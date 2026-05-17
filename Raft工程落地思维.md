# Raft 工程落地思维

> 本文以《Raft 算法：写代码前研究提纲》为骨架，逐一对照 RaftByJava 项目的工程实现，呈现"理论提出了什么问题 → 工程上如何落地"的完整思维链路。

---

## 一、为什么需要分布式？工程上为此做了什么准备？

### 理论说了什么

提纲 1.1—1.2 节讨论了两个基本命题：大规模数据下需要横向扩展来提高读写性能，而横向扩展引入了网络的不确定性，由此带来了数据一致性和系统秩序两大问题。分布式的优势在于数据备份和负载均衡，代价则是需要一套共识机制来驯服网络带来的混乱。

### 工程上怎么做

项目选择 Netty 作为网络基础设施，通过 TCP 长连接 + 长度前缀帧协议来解决网络层的粘包拆包问题。`MessageEncoder` 把每条消息序列化成 JSON 字节数组后先写 4 字节长度头再写载荷，`MessageDecoder` 反过来先读长度再读完整帧，从而在不可靠的字节流上还原出一条一条完整的逻辑消息。这是分布式工程的第一步：在不靠谱的网络上建立一条"可以正常收发完整消息"的通道。

节点间使用 `PeerConnectionManager` 管理所有对端连接，连接建立后第一条消息是 `IdentificationMessage`，让对端知道"这条 TCP 连接属于哪个节点"，从而把匿名的 TCP 通道映射到有名字的集群成员。断线时会自动触发 1 秒退避重连，保证网络短暂抖动不会导致集群通信永久中断。

这套基础设施本身不涉及共识算法，但它是共识算法得以运作的前提——没有可靠的消息通道，一切都是空谈。

---

## 二、CAP 的灰色地带：Raft 的立场

### 理论说了什么

提纲的 CAP 章节用了大量篇幅说明一个核心观点：C 和 A 不是非此即彼的黑白两端，而是一道灰色光谱。强 C（写请求 + 数据同步串行化）保证了即时一致性，但一个节点的故障就能拖垮整个系统的可用性；反之，纯异步同步提高了可用性，却连最终一致性都无法保证。Raft 的目标是在这道光谱上找到一个位置：通过多数派原则把可用性的下限从"全员健康"提升到"半数以上存活"，同时保证至少达到最终一致性，并留出升级到即时一致性的工程接口。

### 工程上怎么做

项目的多数派计算直接体现在 `RaftConfig.getMajorityCount()` 中：`(peers.size() / 2) + 1`。三节点集群需要两票，五节点需要三票。这个数字贯穿了整个系统——选举需要它，日志提交需要它，ReadIndex 心跳确认也需要它。

可用性方面，心跳超时后 Follower 会发起选举，Leader 宕机后集群在一个选举超时周期内就能恢复服务。一致性方面，标准链路保证最终一致性，在此之上项目额外实现了 ReadIndex 机制来支持即时一致性读。这正是提纲所说的"在灰色地带找到合适位置"的工程体现。

---

## 三、多数派原则：从数学期望到代码

### 理论说了什么

提纲独立辟了一节讲多数派原则：系统的决断无需全员参与，多数派达成的共识即可代表整个系统。这将一个"底线思维下的随机性问题进化为数学期望问题"。同时提纲也解释了为什么集群通常用奇数节点——因为同样是允许 2 个节点故障，5 节点比 6 节点更经济。

### 工程上怎么做

多数派在工程中有三个着陆点：

第一，日志提交。`ReplicationManager.advanceCommitIndex()` 从最新的日志往前遍历，对每个索引位置统计 leader 自身加上所有 matchIndex 达到该位置的 Follower 数量，一旦 count 达到 `majorityCount`，就推进 `commitIndex`。注意这里有一个额外约束：只提交当前任期的日志条目，这是为了防止提纲 7.6 节描述的极端 case（后文详述）。

第二，领导者选举。`RaftCore.onRequestVoteResponse()` 统计赞同票数量，达到 `majorityCount` 即调用 `becomeLeader()`。

第三，ReadIndex 确认。`onAppendEntriesResponse()` 中统计心跳 ack 数，达到多数派后调用 `resolvePendingReads()` 释放等待中的读请求。

三个场景，同一个多数派阈值，同一个数学原理。

---

## 四、一主多从与读写分离

### 理论说了什么

Raft 定义了 Leader 和 Follower 两种稳定角色。写操作由 Leader 统一收口，读操作可由任意节点服务。读写分离通过读的负载均衡提高吞吐量，通过写的统一收口降低共识复杂度。但它也衍生了两个问题：Follower 可能返回过时数据（即时一致性问题），以及 Leader 挂了怎么办（选举问题）。

### 工程上怎么做

写请求的收口体现在 `RaftCore.onClientWriteRequest()`：如果当前节点不是 Leader，会自动将写请求转发给已知的 Leader（`forwardWriteToLeader()`），而不是让客户端自行重定向。如果连 Leader 是谁都不知道（集群正在选举），就返回 `NO_LEADER` 错误，客户端需要自行重试。

转发机制的具体实现：Follower 为每个转发的写请求生成一个 `forwardingId`，将原始客户端 Channel 存入 `forwardedWriteChannels` 映射表，然后构造一个携带 `forwardingId` 的新 `ClientWriteRequest` 发送给 Leader。Leader 处理完成后，响应中会携带同一个 `forwardingId`，Follower 在 `onClientWriteResponse()` 中根据 `forwardingId` 从映射表取回原始客户端 Channel，将结果中继回去。这种设计对客户端完全透明——客户端不需要感知集群拓扑，连接任意节点即可完成写操作。`PendingWrite` 也增加了 `forwardingId` 字段，使得 Leader 在 `resolvePendingWrites()` 和 `failPendingWrites()` 中可以正确区分直连请求和转发请求，对转发请求使用 `okForwarded()` / `failForwarded()` 返回带 `forwardingId` 的响应。当 Follower 步下（`stepDown()`）时，`failForwardedWrites()` 会清理所有未完成的转发请求，避免客户端永久等待。

读请求方面，`onClientReadRequest()` 支持两种一致性模式。对于线性一致性读（`linearizable=true`），仍然重定向到 Leader 走 ReadIndex 流程。对于最终一致性读（`linearizable=false`），任意节点都可以直接服务，但会做 `appliedIndex` 校验——如果客户端指定了 `minAppliedIndex`（表示它需要至少读到某个版本的数据），而当前节点的 `lastApplied` 还不够新，则返回 `STALE` 拒绝响应，告知客户端换一个更新的节点重试或等待。响应中也携带当前节点的 `appliedIndex`，方便客户端追踪数据新鲜度。

---

## 五、状态机与预写日志

### 理论说了什么

提纲用了很大篇幅解释预写日志的存在意义：为什么不能直接改状态机，而要先写日志再应用。核心答案是——WAL 提供了一个有序缓冲区，通过日志索引的严格递增和 Follower 对前一条日志的校验，保证即使网络乱序，日志链也能自我修复，从而保证所有节点按相同顺序执行相同操作，最终状态机一致。

### 工程上怎么做

**日志结构。** `LogEntry` 包含三个字段：`term`、`index`、`command`。这正是提纲定义的 `{term, index, command}` 三元组。`term` 和 `index` 构成全局唯一键，保证了提纲 7.2 节的结论：只要 term 和 index 相同，内容必然相同。

**预写日志数组。** `LogManager` 内部用 `List<LogEntry>` 承载日志，采用 1-based 索引，index=0 是一个虚拟哨兵（term=0）。所有写入操作只做追加（`append`），不做就地修改，体现了 WAL 的 append-only 特性。

**前一条日志校验。** `onAppendEntriesRequest()` 中通过 `logManager.hasEntryAt(req.getPrevLogIndex(), req.getPrevLogTerm())` 做前一条日志的校验。如果校验不通过（前一条不存在或 term 不匹配），直接拒绝，返回 `success=false`。这正是提纲所说的"拼图机制"——每一块拼图都必须对上前一块。

**冲突修复。** `LogManager.syncFrom()` 处理三种情况：如果对应位置的日志 term 不一致，先 `truncateFrom()` 截断再追加新日志（对应提纲 case 3：Follower 日志"超前"）；如果对应位置 term 一致，跳过不重复写入（幂等处理）；如果该位置不存在日志，直接追加（对应 case 2：Follower 日志滞后，Leader 会递减 nextIndex 逐步补齐）。

**状态机。** `StateMachine` 是一个简单的内存 KV 存储（`HashMap<String, String>`），支持 `set key=value` 和 `delete key` 两种命令。状态机只存最终结果，不记过程，正如提纲所说的"银行卡余额"。日志才是"流水单"。

---

## 六、两阶段提交

### 理论说了什么

提纲从单机和系统两个维度解读两阶段提交：单机层面，写请求先追加到 WAL 再应用到状态机；系统层面，Leader 先发起提议（proposal），多数派确认后才提交（commit）。正是这种两阶段的设计，让多数派原则得以嵌入——有了第一阶段的广播征询，Leader 才有机会收集民意，进而在多数派达成共识后立即提交。

### 工程上怎么做

**提议阶段。** `onClientWriteRequest()` 中，Leader 先通过 `logManager.append()` 将命令写入本地 WAL，然后调用 `replicationManager.replicateLog(entry)` 广播给所有 Follower。这一步就是 proposal。

**提交阶段。** 当 Follower 返回 `success=true` 的 `AppendEntriesResponse` 后，`onAppendEntriesResponse()` 调用 `replicationManager.advanceCommitIndex()` 检查是否多数派已完成复制。如果是，`commitIndex` 被推进。

**应用阶段。** `commitIndex` 推进后立即调用 `applyCommittedEntries()`，按序将 `(lastApplied, commitIndex]` 区间内的日志逐条应用到状态机。注意应用是严格按索引顺序来的，用 `logManager.getUnappliedEntries()` 的 for 循环从 `lastApplied+1` 遍历到 `commitIndex`，绝不跳过。

**写请求等待。** 提纲说 Leader 提交后才响应客户端。项目用 `PendingWrite` 实现了这一语义：写请求到达时，Leader 不立即返回 ack，而是把请求挂在 `pendingWrites` 这个 map 里，key 是日志索引。当 `commitIndex` 推进后，`resolvePendingWrites()` 遍历 map，把所有 `logIndex <= commitIndex` 的 PendingWrite 逐个响应给客户端。这样客户端拿到响应时，日志已经被多数派确认并提交了。

如果 Leader 在日志提交前就宕机退位，`stepDown()` 中会调用 `failPendingWrites()`，给所有还在等待的客户端返回 `NO_LEADER` 错误。客户端需要自行重试。

---

## 七、领导者选举

### 理论说了什么

提纲讨论了选举的两个基本问题：Follower 如何感知 Leader 已死（心跳超时机制），以及什么样的节点有资格当选（日志至少不滞后于多数派）。选举机制保证了"已提交的日志不会丢失"这一关键性质。

### 工程上怎么做

**心跳与超时检测。** `TimerManager` 管理三个定时器：心跳定时器（Leader 向 Follower 周期性发送心跳）、选举超时定时器（Follower 检测 Leader 是否存活）、以及竞选超时定时器（Candidate 等待投票结果的上限）。心跳间隔由配置文件指定（默认 150ms），选举超时在 `[electionTimeoutMinMs, electionTimeoutMaxMs]` 范围内随机取值。竞选超时使用 `electionTimeoutMaxMs` 作为上限——如果 Candidate 在此时间内未获得多数票，说明本轮竞选已无望，`onCampaignTimeout()` 会让它退回 Follower 并重置选举定时器，等待下一次超时后重新发起。选举超时和竞选超时的分离体现了一个重要的工程决策：选举超时的随机扰动防止了多个节点同时发起竞选（提纲 7.4 节），而竞选超时则防止 Candidate 无限期等待投票结果（比如在网络分区导致收不到足够回复的场景下）。

**选举发起。** 当选举定时器触发时，`onElectionTimeout()` 被调用。但这里项目没有直接发起真正的选举，而是先进入 Pre-Vote 阶段（后文详述）。Pre-Vote 通过后才会调用 `startRealElection()`，在 `ElectionManager.startElection()` 中完成：自增 term、投票给自己、广播 `RequestVoteRequest`。

**投票判断。** `onRequestVoteRequest()` 实现了提纲 5.3 节描述的完整投票逻辑：如果 Candidate 的 term 落后于自己，拒绝；如果自己已经投过票给别人（`votedFor != null && !votedFor.equals(candidateId)`），拒绝；如果 Candidate 的日志不如自己新（通过 `ElectionManager.shouldGrantVote()` 判断），拒绝。只有以上全部条件都通过，才投赞同票。

**日志新旧比较。** `shouldGrantVote()` 的比较规则正是提纲 5.3 节第 IV-VII 条：先比较 `lastLogTerm`，term 大者更新；term 相同时比较 `lastLogIndex`，index 大者更新。这条规则保证了提纲 7.5 节的结论——新任 Leader 一定拥有旧 Leader 已提交的日志。

---

## 八、任期与角色切换

### 理论说了什么

提纲定义了任期 `term` 的语义（朝代），以及三种角色之间的切换路径：Leader 发现更大 term 则退位，Follower 超时则竞选，Candidate 获多数票则上位、被拒绝或发现更高 term 则退回。每个 term 至多一个 Leader。

### 工程上怎么做

**任期单调递增。** `NodeState.currentTerm` 是一个 int 值，只有 `setCurrentTerm()` 能修改它，且该方法会立即通过 `PersistenceManager.saveMeta()` 持久化到磁盘（`meta.json` + fsync）。这保证了即使节点崩溃重启，term 也不会回退。

**角色切换的收敛点——stepDown。** 所有"发现更高 term 后退位"的场景都收敛到同一个方法 `stepDown(int newTerm)`：更新 term，切回 FOLLOWER，清空 votedFor 和 leaderId，停止心跳定时器，重置选举定时器，清空选举和 Pre-Vote 状态，并 fail 掉所有 pending 的写请求。这个方法在 `onAppendEntriesRequest()`、`onAppendEntriesResponse()`、`onRequestVoteRequest()`、`onRequestVoteResponse()`、`onPreVoteResponse()`、`onInstallSnapshotRequest()` 等多处被调用——对应提纲中 Leader 通过同步响应发现新 term、收到新 Leader 心跳、收到更高 term 的拉票请求等所有退位场景。

**becomeLeader。** Candidate 获得多数票后调用 `becomeLeader()`：设角色为 LEADER，取消选举定时器，启动心跳定时器，初始化所有 Follower 的 `nextIndex`（设为 Leader 最后一条日志 +1）和 `matchIndex`（设为 0），然后追加一条 no-op 空日志并复制给所有 Follower。这条 no-op 是提纲 7.6 节的工程落地——新 Leader 必须先提交一条本任期的日志，才能安全地推进旧任期遗留日志的提交。

---

## 九、各角色职责

### 理论说了什么

提纲分三小节详述了 Leader、Follower、Candidate 的职责。Leader 负责接收写请求、发起两阶段提交、发送周期性心跳；Follower 负责同步日志、响应投票、检测心跳超时；Candidate 负责自增 term、给自己投票、广播拉票、根据结果转为 Leader 或退回 Follower。

### 工程上怎么做

**Leader 职责在 `RaftCore` 中的体现：** `onClientWriteRequest()` 处理写请求并发起两阶段提交；`sendHeartbeat()` 通过 `ReplicationManager.sendHeartbeat()` 向所有 Follower 发送心跳（实际上心跳就是空的 AppendEntries，复用了同一个请求结构）；`onAppendEntriesResponse()` 收集多数派确认并推进提交。

**Follower 职责：** `onAppendEntriesRequest()` 处理 Leader 的日志同步请求和心跳——校验 term、校验 prevLog、同步日志、更新 commitIndex、应用到状态机、重置选举定时器；`onRequestVoteRequest()` 处理 Candidate 的拉票请求——执行投票判断逻辑。

**Candidate 职责：** `startRealElection()` 触发 `ElectionManager.startElection()` 执行自增 term、投票给自己、广播拉票，同时启动竞选超时定时器；`onRequestVoteResponse()` 收集投票结果，达到多数派则 `becomeLeader()`，若多数派明确拒绝（`voteRejects >= majorityCount`）则立即退回 Follower 而不等待竞选超时——这避免了在明确落选后的无意义等待；竞选超时则通过 `onCampaignTimeout()` 退回 Follower 并重置选举定时器，等待下一轮选举。

**心跳的单向性。** 提纲特别指出"心跳请求是单向传输，非双向通信，follower 无需回复"。但在项目的工程实现中，心跳复用了 AppendEntries 的请求/响应结构，Follower 实际上会回复 `AppendEntriesResponse`。这是一个工程选择——复用响应通道让 Leader 能及时感知 Follower 的 term 变化和日志同步状态，用微小的额外开销换取了更丰富的反馈信息。同时 ReadIndex 机制也依赖心跳的 ack 来确认 Leader 身份。

---

## 十、外部请求链路

### 理论说了什么

提纲 4 章梳理了写请求的完整主流程（5 步），以及四个补充场景：Leader 任期滞后（case 1）、Follower 日志滞后（case 2）、Follower 日志"超前"（case 3）、如何升级为即时一致性（case 4）。

### 工程上怎么做

**写请求主流程。** 客户端发送 `ClientWriteRequest` → Leader 的 `onClientWriteRequest()` 接收 → `logManager.append()` 追加本地 WAL → `replicationManager.replicateLog()` 广播给 Follower → Follower 的 `onAppendEntriesRequest()` 校验 + 同步 → 返回 `AppendEntriesResponse(success=true)` → Leader 的 `advanceCommitIndex()` 检查多数派 → `commitIndex` 推进 → `applyCommittedEntries()` 应用到状态机 → `resolvePendingWrites()` 响应客户端。完全对应提纲的 5 步主流程。

**case 1 — Leader 任期滞后。** `onAppendEntriesRequest()` 的第一个 if 判断：`req.getTerm() < state.getCurrentTerm()` 时直接拒绝并返回自己的 term。Leader 收到响应后在 `onAppendEntriesResponse()` 中发现 `resp.getTerm() > state.getCurrentTerm()`，调用 `stepDown()` 退位。

**case 2 — Follower 日志滞后。** `hasEntryAt()` 校验失败，Follower 返回 `success=false`。Leader 在 `handleAppendResponse()` 中对 rejected 的情况递减 `nextIndex`（`currentNext - 1`），然后立即重发 `sendAppendEntries(peerId)`。这个递减-重试的过程会一直重复，直到找到 Follower 认可的那个 prevLogIndex 为止，然后从那里开始补齐所有缺失的日志。

**case 3 — Follower 日志"超前"。** `LogManager.syncFrom()` 中检测到相同 index 位置但 term 不一致的日志时，调用 `truncateFrom()` 截掉从该位置开始的所有日志，然后重新追加 Leader 的日志。这保证了 Follower 最终与 Leader 保持一致。

**case 4 — 即时一致性的升级。** 在写流程中，`applyCommittedEntries()` 在 `commitIndex` 推进后立即被调用，保证 Leader 在响应客户端前已经将日志应用到状态机。在读流程中，项目实现了 ReadIndex 机制（后文详述），Leader 先确认自身仍合法再读取状态机。

---

## 十一、内部请求链路

### 理论说了什么

提纲 5 章详细梳理了三类内部 RPC 的请求参数、各角色终点的处理逻辑、响应参数和后处理：日志同步请求（AppendEntries）、心跳请求、竞选拉票请求（RequestVote）。

### 工程上怎么做

**日志同步请求（AppendEntries）。** 请求结构 `AppendEntriesRequest` 包含 `term`、`leaderId`、`prevLogIndex`、`prevLogTerm`、`entries[]`、`leaderCommit`，与提纲 5.1 节的参数完全一一对应。响应结构 `AppendEntriesResponse` 包含 `term`、`success`、`nodeId`（多一个 nodeId 方便 Leader 定位是哪个 Follower 回复的），对应提纲的 `{term, success}` 二元组。

各角色终点的处理：Leader 收到比自己 term 大的 AppendEntries 请求会 stepDown（对应终点 1-II）；Follower 的处理逻辑覆盖了提纲列出的全部情况（term 落后则拒绝、前一条日志不匹配则拒绝、否则同步）；Candidate 收到 term >= 自己的 Leader 请求也会 stepDown 退回 Follower（隐含在 stepDown 的调用中，因为 `onAppendEntriesRequest()` 对所有非 Leader 角色统一处理）。

**心跳请求。** 项目中心跳没有独立的消息类型，而是复用了 AppendEntries，只是 `entries[]` 为空。`ReplicationManager.sendHeartbeat()` 调用的就是 `sendAppendEntries(peerId)`，当 Follower 没有滞后的日志时，`getEntriesFrom(nextIdx)` 返回空列表，自然就是一次心跳。心跳中携带的 `leaderCommit` 字段推动 Follower 更新 commitIndex，对应提纲 5.2 节的功能。

**竞选拉票请求（RequestVote）。** 请求结构 `RequestVoteRequest` 包含 `term`、`candidateId`、`lastLogIndex`、`lastLogTerm`，与提纲 5.3 节完全对应。响应结构 `RequestVoteResponse` 包含 `term`、`voteGranted`。投票判断逻辑在 `onRequestVoteRequest()` 和 `shouldGrantVote()` 中实现，覆盖了提纲列出的 I-VII 全部条件。Candidate 后处理（多数派赞同则上位、发现更高 term 则退回、超时则新一轮竞选）在 `onRequestVoteResponse()` 和 `onElectionTimeout()` 中实现。

---

## 十二、线程模型——工程落地中提纲没有涉及的关键决策

### 这个问题从哪来

提纲作为理论文献，讨论的是算法本身的正确性，不涉及具体的线程模型。但在工程实现中，线程安全是绕不过去的问题：Netty 的 I/O 线程收到消息后，如何安全地修改 Raft 状态？多个 Follower 的响应几乎同时到达时，如何避免竞态条件？

### 工程上怎么做

项目采用了"单线程执行器"模型：所有 Raft 算法逻辑（状态修改、日志操作、定时器回调）都运行在同一个 `ScheduledExecutorService`（命名为 `raft-core-{nodeId}`）上。Netty I/O 线程收到消息后，`RaftMessageHandler` 不直接处理，而是通过 `raftExecutor.execute(() -> dispatch(...))` 把处理任务提交到 Raft 核心线程执行。

这意味着所有 Raft 状态（term、votedFor、commitIndex、日志数组等）都是"线程受限"的——只有 Raft 核心线程能访问它们，天然不需要任何锁。代码中 `NodeState`、`LogManager`、`StateMachine` 等类的注释都明确标注了"Thread-confined to the Raft core thread"。

`TimerManager` 的定时器回调也运行在同一个 executor 上，因此选举超时、心跳发送和消息处理之间不会产生并发冲突。

这种模型的本质思想是：用串行化来消除并发问题。吞吐量的瓶颈不在 Raft 核心逻辑的处理速度（内存操作极快），而在网络 I/O 和磁盘持久化上。单线程处理 Raft 逻辑是一个非常聪明且普遍的工程选择——etcd 的 Raft 模块也是这么做的。

---

## 十三、持久化——掉电安全的保障

### 理论说了什么

提纲提到了 WAL（预写日志）的概念，但更多是从算法角度讨论为什么需要先写日志再应用到状态机，没有深入讨论持久化的工程细节。然而在工程落地中，持久化是不可或缺的——如果节点重启后丢失了 term 和 votedFor，可能导致同一任期内投票给两个不同的 Candidate，从而破坏"一个 term 至多一个 Leader"的安全性。

### 工程上怎么做

`PersistenceManager` 负责所有持久化操作，管理两类文件：

第一类是元数据文件 `meta.json`，存储 `currentTerm` 和 `votedFor`。每次 `NodeState.setCurrentTerm()` 或 `setVotedFor()` 被调用时，都会自动触发 `persistMeta()`，将当前的 term 和 votedFor 序列化成 JSON 写入文件并调用 `fos.getFD().sync()` 强制刷盘。fsync 是掉电安全的关键——只有操作系统内核确认数据已经写入物理磁盘，sync 才会返回。没有这一步，数据可能还停留在操作系统的页缓存里，断电就丢了。

第二类是预写日志文件 `wal.log`，采用每行一条 JSON 的格式。`appendEntry()` 以追加模式打开文件，写入一条 JSON 行后同样 fsync。`loadEntries()` 在启动时逐行读取重建日志数组。`truncateFrom()` 需要截断日志时，先加载全部条目，保留 index 小于截断点的部分，然后重写整个文件。

**启动恢复流程。** `RaftNode.recoverFromDisk()` 按照固定顺序恢复状态：先加载快照（如果有的话，恢复状态机和日志的快照哨兵）、再加载 WAL 条目（重建内存中的日志数组）、最后加载 meta（恢复 term 和 votedFor）。顺序很重要：快照决定了日志数组的起始偏移，WAL 条目要在快照偏移的基础上追加，而 meta 的 term 和 votedFor 要最后恢复以避免触发不必要的重复持久化。

---

## 十四、读一致性的补强——从最终一致到即时一致

### 理论说了什么

提纲在"读请求的一致性补强"一节提出了两种方案：一是 `appliedIndex` 校验（Follower 发现自己的 appliedIndex 落后于客户端携带的 appliedIndex 就拒绝服务），二是强制读主（所有读请求都打到 Leader）。对于强制读主方案，提纲进一步指出了一个关键问题：Leader 可能已经被分区隔离而不自知（"不知大清已亡"），此时它提供的读服务无法保证即时一致性。解决方案是 Leader 在提供读服务前，先向集群发一轮广播确认自己仍被多数派认可。

### 工程上怎么做

项目实现了两层读一致性机制：最终一致性读和线性一致性读（ReadIndex），通过 `ClientReadRequest` 中的 `linearizable` 字段区分。

**最终一致性读（Follower 可服务）。** 当 `linearizable=false` 时，任意节点都可以直接从本地状态机读取数据。但这里有一个关键的 `appliedIndex` 校验：客户端可以在请求中携带 `minAppliedIndex`（例如上一次读响应中返回的 `appliedIndex`），要求服务节点的 `lastApplied` 不低于此值。如果当前节点的 `lastApplied < minAppliedIndex`，说明该节点的状态机还没追赶上客户端预期的版本，返回 `STALE` 拒绝响应，告知客户端需要换一个更新的节点或稍后重试。这个机制实现了提纲所说的 `appliedIndex` 校验方案，为最终一致性读提供了单调读（monotonic-read）保证。每个读响应都会携带当前 `appliedIndex`，让客户端可以逐步追踪数据新鲜度。

**线性一致性读（Leader ReadIndex）。** 当 `linearizable=true` 时，读请求必须由 Leader 处理，非 Leader 节点会返回重定向。Leader 通过 ReadIndex 机制——业界（etcd、TiKV 等）验证过的标准方案——确保读到的数据是最新已提交的。流程如下：Leader 记录当前的 `commitIndex` 作为 `readIndex`。如果 `lastApplied` 已经覆盖了 `readIndex`，直接从状态机读取并返回。否则，将读请求挂入 `pendingReads` 队列，然后发送一轮心跳给所有 Follower。当心跳的 ack 累计达到多数派后（`readIndexHeartbeatAcks + 1 >= majorityCount`），再检查 `lastApplied >= readIndex` 是否满足，满足则从状态机取值返回给客户端。

为什么要等 `lastApplied >= readIndex`？因为 `commitIndex` 的推进只代表日志被多数派确认，但可能还没来得及应用到状态机。必须等到应用完成，才能保证读到的是最新已提交数据。

为什么心跳确认就能证明 Leader 身份合法？因为心跳本质上是 AppendEntries RPC，Follower 只有在确认发送方的 term 不小于自己时才会接受。如果 Leader 已经过时（网络分区期间集群已经选出新 Leader），多数派节点的 term 一定比旧 Leader 高，它们会拒绝心跳，旧 Leader 就收不到多数派 ack，读请求也就不会被错误地处理。

提纲还提到，写请求不需要这个额外校验，因为写流程的 proposal 阶段本身就需要与多数派通信——这个通信过程已经隐含了身份合法性的确认。项目中的实现也印证了这一点：`onClientWriteRequest()` 中没有任何身份校验步骤，因为后续的 `replicateLog()` + `advanceCommitIndex()` 流程自然会完成这个校验。

---

## 十五、新 Leader 上任后的 no-op 空日志

### 理论说了什么

提纲 7.6 节用了一个精密的极端 case 来说明一个问题：并非一条日志只要被多数派同步就可以安全提交。在特定的 Leader 切换序列下，一条旧任期的日志即使被多数派同步，也可能被新 Leader 覆盖。解决方案是：新 Leader 必须先提交一条本任期的日志，才能执行旧日志的提交。工程实践中，通常通过上任后立即广播一条空日志（no-op）来实现。

### 工程上怎么做

`becomeLeader()` 方法中有一行关键代码：`LogEntry noop = logManager.append(state.getCurrentTerm(), "");`。这就是 no-op 日志——command 为空字符串。追加后立即通过 `replicationManager.replicateLog(noop)` 复制给所有 Follower。当这条 no-op 被多数派确认并提交后，Leader 就安全地建立了本任期的"存在感"，此后所有旧任期遗留的日志也可以被安全提交。

在 `applyCommittedEntries()` 中，no-op 日志被特殊处理：`if (entry.isNoOp())` 直接跳过，不应用到状态机。因为它的 command 是空的，本来就不携带任何业务操作，它的存在价值纯粹是安全性方面的。

`advanceCommitIndex()` 中的 `entry.getTerm() != state.getCurrentTerm()` 条件检查也与此相关：Leader 只能通过当前任期的日志来推进 commitIndex，不能直接提交旧任期的日志。这与提纲的规则完全一致——旧日志只是"搭车"被间接提交的，当前任期的新日志（哪怕是 no-op）才是"车"。

---

## 十六、Pre-Vote——解决网络分区的无意义选举

### 理论说了什么

提纲 7.10 节描述了这样一个问题：小分区中的节点由于收不到 Leader 心跳会不断发起选举，反复自增 term。当网络恢复后，这个异常高的 term 会迫使正常的 Leader 退位，触发一次完全没有必要的全集群选举。解决方案是在真正选举前先"试探"：向集群发送请求，只有得到多数派响应才会真正自增 term 发起选举。

### 工程上怎么做

项目实现了完整的 Pre-Vote 机制。当选举定时器超时时，`onElectionTimeout()` 不再直接调用选举，而是调用 `startPreVote()`：计算一个"提议 term"（当前 term + 1，但不真正自增），给自己算一票，然后广播 `PreVoteRequest`。

`PreVoteRequest` 包含 `nextTerm`（提议的 term）、`candidateId`、`lastLogIndex`、`lastLogTerm`。注意它不会修改任何节点的持久化状态——这是 Pre-Vote 与真正选举的本质区别。

收到 `PreVoteRequest` 的节点在 `onPreVoteRequest()` 中判断两个条件：提议的 term 是否 >= 自己的 currentTerm，以及发送方的日志是否不落后于自己。两个条件都满足则赞同。

当 Pre-Vote 的赞同票达到多数派后，`onPreVoteResponse()` 调用 `startRealElection()` 进入真正的选举流程——此时才会自增 term。

这样，一个被网络分区隔离的节点发出的 Pre-Vote 请求不会得到多数派响应（因为它无法与大分区通信），因此永远不会真正自增 term。当网络恢复后，它的 term 仍然是正常的，不会干扰已有的 Leader。

---

## 十七、客户端幂等性——不丢失、不重复

### 理论说了什么

提纲 7.11 节讨论了客户端提交写请求的不丢失和不重复问题。不丢失通过 ack 机制（超时重发）保证。不重复通过序列号 + Leader 端去重保证：客户端为每个写请求分配一个序列号，服务端的 Leader 对相同序列号做幂等处理。

### 工程上怎么做

`ClientSessionTable` 实现了去重表。每个客户端用一个 `clientId` 标识，每个请求用一个递增的 `sequenceNumber` 标识。表内维护每个 clientId 最后一次成功执行的 sequenceNumber 和对应的响应结果。

在 `onClientWriteRequest()` 中，处理写请求的第一步就是幂等检查：`sessionTable.isDuplicate(clientId, sequenceNumber)`。如果 sequenceNumber 小于等于已记录的最新值，说明这个请求是重复的（可能是客户端因为超时而重发），直接返回缓存的响应结果，不再执行任何日志操作。

`isDuplicate()` 的判断逻辑是 `entry.sequenceNumber >= sequenceNumber`，即只要服务端已经处理过不小于此序列号的请求，就判定为重复。这巧妙地处理了"请求乱序到达"的场景——即使序列号 5 比序列号 4 先到，只要 5 已经被处理，4 也会被视为重复。

session 表已经被纳入快照中持久化。`SnapshotManager.takeSnapshot()` 在创建快照时会同时保存 `ClientSessionTable.snapshot()` 返回的去重记录。`installSnapshot()` 和启动恢复（`recoverFromDisk()`）时，也会通过 `sessionTable.restoreFromSnapshot()` 恢复去重表。这确保了节点重启或从快照恢复后不会丢失去重记录，避免客户端的重试请求被错误地重复执行。

---

## 十八、集群成员变更

### 理论说了什么

提纲第 6 章详细讨论了集群变更的安全性问题。配置变更被包装成一条写请求走两阶段提交流程，但变更期间的多数派必须以变更前的老节点名单为准——否则可能出现两个多数派同时存在导致脑裂。提纲用了两个反例来说明：反例 1 是选举期间的脑裂（新老节点分别选出两个 Leader），反例 2 是写请求期间的数据丢失（新多数派确认的日志在 Leader 切换后被覆盖）。

### 工程上怎么做

项目实现了单节点变更（single-node membership change）方案——每次只增删一个节点。这是 Raft 论文推荐的简化方案，它的安全性在于：当只变更一个节点时，新配置的多数派和旧配置的多数派必然存在交集，因此不可能出现两个独立的多数派。

`onMembershipChangeRequest()` 处理变更请求：首先检查是否已有变更在进行（`membershipChangeInProgress`），如果有则拒绝，保证同一时间只有一个变更进行。然后根据变更类型构造 `CONFIG:ADD:nodeId:host:port` 或 `CONFIG:REMOVE:nodeId` 格式的特殊命令，追加到日志中作为一条 `LogEntry` 走正常的两阶段提交流程。

在 Leader 侧，配置变更立即生效（`config.addPeer()` / `config.removePeer()`），同时更新 `majorityCount`。在 Follower 侧，配置变更在日志被应用到状态机时才生效（`applyConfigChange()`）。这种"Leader 立即生效、Follower 提交时生效"的做法是单节点变更方案的标准实现。

添加节点时，Leader 还会立即与新节点建立连接（`peerManager.connectToPeers()`）并重新初始化复制状态（`replicationManager.initialize()`），让新节点能尽快追赶日志进度。

---

## 十九、日志压缩与快照

### 理论说了什么

提纲本身没有专门讨论日志压缩，但这是任何生产级 Raft 实现都必须解决的工程问题：WAL 不断增长，占用越来越多的磁盘空间；节点重启时需要重放全部日志，耗时过长；一个严重滞后的 Follower 需要 Leader 发送大量历史日志来补齐。快照机制一次性解决了这三个问题。

### 工程上怎么做

`SnapshotManager` 负责快照的创建、加载和安装。快照是一个 `snapshot.json` 文件，包含四个字段：`lastIncludedIndex`（快照覆盖的最后一条日志索引）、`lastIncludedTerm`（对应的任期）、`data`（状态机的完整 KV 数据）、`sessions`（`ClientSessionTable` 的完整去重记录）。将 session 表纳入快照是一个关键的工程决策——如果只保存状态机数据而不保存去重表，节点从快照恢复后会丢失所有去重记录，导致客户端的重试请求被错误地重复执行。

**触发时机。** 在每次 `applyCommittedEntries()` 执行完毕后，检查 `snapshotManager.shouldCompact(logManager.size())`——当内存中的日志条目数超过配置的阈值（`snapshotThreshold`，默认 1000）时，触发快照创建。

**创建快照。** `takeSnapshot()` 同时拍下状态机的完整快照（`stateMachine.snapshot()` 返回 HashMap 的深拷贝）和 `ClientSessionTable` 的去重记录（`sessionTable.snapshot()`），两者一起写入 `snapshot.json` 并 fsync，然后调用 `logManager.applySnapshot()` 截断已被快照覆盖的日志条目。截断后，`LogManager` 用 `snapshotLastIndex` 和 `snapshotLastTerm` 记住快照的边界，后续的索引计算都要减去这个偏移量。

**InstallSnapshot RPC。** 当 Leader 发现某个 Follower 需要的日志已经被压缩掉了（`prevLogIndex < logManager.getSnapshotLastIndex()`），就不能再发 AppendEntries 了，改为发送 `InstallSnapshotRequest`，把整个快照一次性传过去。`InstallSnapshotRequest` 中除了状态机数据外，还携带了 `snapshotSessions` 字段——即序列化后的 session 去重表。Follower 收到后，在 `onInstallSnapshotRequest()` 中先反序列化 session 数据，然后调用 `snapshotManager.installSnapshot()` 恢复状态机、恢复 session 去重表、清空旧日志、设置快照哨兵。启动恢复时（`RaftNode.recoverFromDisk()`）也会从快照中加载 session 表。这让严重滞后的节点能跳过大量历史日志，直接从快照追赶到较新的状态，同时保留幂等性保证。

---

## 二十、Q&A 中的理论证明在代码中的体现

### 7.1 — 一个任期至多一个 Leader

提纲的证明基于三点：term 单调递增、一个 Follower 在同一 term 只投一票、需要多数派赞同。对应到代码：`startElection()` 会自增 term；`onRequestVoteRequest()` 中 `state.getVotedFor() != null && !state.getVotedFor().equals(candidateId)` 这个判断保证了一票只投一人；`voteGrants.size() >= majorityCount` 保证了必须多数派同意。三点齐备，一个 term 不可能产生两个 Leader。

### 7.2 — term + index 相同则内容相同

提纲从 WAL 的 append-only 性质和同一 term 只有一个 Leader 推导出这个结论。代码中，`LogEntry` 的 `term`、`index`、`command` 全部是 `final` 的（不可变），日志只通过 `append()` 追加，不存在修改已有条目的方法。同一 term 的 Leader 对同一个 index 只会写入一条确定内容的日志。

### 7.3 — 日志链的归纳法性质

提纲用数学归纳法证明了：如果两个节点在 `{term=x, index=y}` 位置的日志相同，则此前所有日志都相同。代码中的保证来自 `hasEntryAt(prevLogIndex, prevLogTerm)` 校验——每次同步都会验证"前一条日志"的一致性，如果不一致就拒绝。这个链式校验恰好对应归纳法中的"归纳步骤"。

### 7.4 — 选票瓜分的解决

提纲说每个节点在心跳超时和竞选超时上加随机扰动。代码中 `TimerManager` 实现了两个独立的超时机制：选举超时（`resetElectionTimer()`）每次重置时从 `[electionTimeoutMinMs, electionTimeoutMaxMs]` 区间随机取值，防止多个 Follower 同时发起竞选；竞选超时（`startCampaignTimer()`）使用固定的 `electionTimeoutMaxMs` 作为上限，防止 Candidate 无限期等待投票结果。例如配置为 [300ms, 500ms]，选举超时在 300-500ms 之间随机，竞选超时固定为 500ms。两者的分离确保了即使在极端网络条件下（如收不到足够投票回复），Candidate 也能及时退出并让其他节点获得竞选机会。

### 7.5 — 新 Leader 拥有旧 Leader 已提交的日志

提纲的证明基于"两个多数派必有交集"。代码中的保证来自投票判断 `shouldGrantVote()`：只有日志不落后于自己的 Candidate 才能获得选票。由于已提交的日志必然存在于多数派中，任何能获得多数派选票的 Candidate，其日志必然覆盖了已提交的部分。

### 7.7 — 不乱序、不丢失、不重复

不乱序由 `prevLogIndex`/`prevLogTerm` 校验保证（代码中的 `hasEntryAt()`）。不丢失由 Leader 的递减重试保证（`handleAppendResponse()` 中 `nextIndex - 1` 然后重发）。不重复由 `syncFrom()` 中的 term 比对保证——如果相同 index 的日志 term 一致则跳过不重复写入。

### 7.8 — 已提交日志的全局一致性

这是 7.2 和 7.3 的推论。代码中通过 commitIndex 的推进规则保证：只有被多数派复制且属于当前 term 的日志才能被提交（`advanceCommitIndex()` 中的双重检查），而提交意味着多数派拥有这条日志，由 7.3 的链式一致性，这些节点在该日志之前的所有日志也都一致。

### 7.9 — 状态机最终一致性

基于 7.8 的结论加上"只有已提交的日志才能应用到状态机"的规则。代码中 `getUnappliedEntries()` 只返回 `(lastApplied, commitIndex]` 范围内的日志，而 commitIndex 只有在满足多数派 + 当前 term 条件后才会推进。因此所有节点的状态机都会按照相同的顺序执行相同的命令，最终一致性得到保证。

### 7.11 — 客户端不丢失不重复

代码实现见第十七节。不丢失依赖客户端侧的超时重发（项目侧只提供了服务端的幂等保证），不重复依赖 `ClientSessionTable` 的 `isDuplicate()` 检查。

---

## 二十一、工程实现清单 vs 提纲推荐顺序

提纲末尾给出了一份"最小实现清单"，推荐了 10 个落地步骤。下面逐一对照项目的工程实现位置：

**1. 节点状态与三种角色** → `NodeRole` 枚举（LEADER/FOLLOWER/CANDIDATE），`NodeState` 管理角色切换，`RaftCore.stepDown()` 和 `becomeLeader()` 实现角色转换。

**2. 任期、投票记录、超时** → `NodeState.currentTerm` + `votedFor`（持久化到 `meta.json`），`TimerManager` 管理心跳、选举超时、竞选超时三个独立定时器，选举超时带随机扰动，竞选超时防止 Candidate 无限等待。

**3. 日志结构** → `LogEntry{term, index, command}`，不可变对象，Jackson 可序列化。

**4. 日志同步请求参数** → `AppendEntriesRequest{term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit}`，与 Raft 论文参数一一对应。

**5. 提交推进** → `ReplicationManager.advanceCommitIndex()`，从后往前遍历日志，统计多数派复制数，当前 term 条件下推进 `commitIndex`。

**6. 应用状态机** → `RaftCore.applyCommittedEntries()` 按序执行 `(lastApplied, commitIndex]` 区间的日志，`StateMachine.apply()` 解析并执行 KV 命令。

**7. 选举拉票** → `ElectionManager.startElection()` 发起投票，`shouldGrantVote()` 基于 `lastLogTerm`/`lastLogIndex` 判断是否投票。

**8. 日志冲突修复** → `LogManager.syncFrom()` 检测冲突并截断，`ReplicationManager.handleAppendResponse()` 递减 `nextIndex` 补齐滞后。

**9. 读一致性** → 双模式读：最终一致性读（任意节点服务，`appliedIndex` 校验保证单调读），线性一致性读（Leader ReadIndex 机制）。

**10. 工程增强项** → no-op（`becomeLeader()` 中追加空日志），Pre-Vote（`startPreVote()` + `PreVoteRequest/Response`），配置变更（`onMembershipChangeRequest()` + CONFIG 日志），客户端幂等（`ClientSessionTable`，已纳入快照持久化），快照（`SnapshotManager` + `InstallSnapshotRequest/Response`，含 session 去重表），写请求自动转发（Follower 通过 `forwardWriteToLeader()` 透明转发），Candidate 多数派拒绝快速退回（`voteRejects >= majorityCount` 时立即退回 Follower）。

---

## 总结

从提纲到代码，整个思维链路可以浓缩为一句话：**理论定义了"什么是正确的"，工程解决了"如何做到正确的"。**

提纲用 CAP 理论框定了问题空间，用多数派原则给出了核心思路，用两阶段提交打通了执行路径，用选举机制保障了系统在故障时的自愈能力，用 WAL 和日志链式校验保证了数据的顺序一致性，用一系列严谨的证明（7.1-7.11）论证了整套机制的安全性。

工程实现则在此基础上回答了一系列"提纲没说但不得不解决"的问题：用什么线程模型（单线程执行器），怎么序列化消息（JSON + 长度前缀帧），怎么持久化状态（meta.json + wal.log + fsync），怎么处理日志无限增长（快照 + 日志压缩），怎么防止分区节点捣乱（Pre-Vote），怎么保证客户端不重复提交（session 去重表），怎么做到写入后才响应客户端（PendingWrite），以及怎么在不加锁的情况下保证线程安全（线程受限模型）。

理论和工程，缺一不可。理论提供正确性的保证，工程提供落地的手段。Raft 算法之所以被广泛工业化采用，正是因为它在两者之间取得了出色的平衡——理论上足够严谨，工程上足够简洁。