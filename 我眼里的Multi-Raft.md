# 我眼里的 Multi-Raft

> 上一篇写了单个 Raft Group 的核心原理——选举、日志复制、读写一致性。这篇接着往上走一层：当你真正要把 Raft 用在工业级数据库里的时候，单个 Raft Group 根本不够用，必须引入 Multi-Raft。这篇文章以 TiDB/TiKV 为例，用我自己的话把 Multi-Raft 的核心思路捋一遍。

---

## 一、为什么单个 Raft Group 不够

上一篇已经说清楚了，Raft 保证强一致性的代价是：每次写入都必须等多数派确认才能返回。也就是说，一次写入的延迟下限就是一次网络往返（Leader 发日志给 Follower，Follower 回复确认）。对于数据库来说，单次写入几毫秒其实完全可以接受。

但问题在于**吞吐量**。

一个 Raft Group 只有一个 Leader，所有写请求都经过这个 Leader 串行处理。就算你有 100 台机器，如果整个集群只有一个 Raft Group，那 99 台都是 Follower，写入能力完全取决于那一个 Leader 的处理速度。这跟 Redis 的单线程瓶颈本质上是一个道理——不是算法有问题，是架构天花板在那里。

而且还有一个容量问题：一个 Raft Group 的所有副本保存的是完全相同的数据。如果你有 1TB 的数据，每个节点都要存 1TB。加机器只能提高容灾能力，不能扩展存储容量。

所以自然的想法就是：**把数据切成很多小块，每一块单独跑一个 Raft Group。** 这就是 Multi-Raft。

---

## 二、Multi-Raft 的核心思想：数据分片 + 独立共识

### Region：数据的最小管理单位

TiKV 把整个 key 空间按**范围（Range）**切分成一段一段的小块，每一块叫做一个 **Region**。每个 Region 默认管理大约 96MB 的数据，存储的是一段连续的 key 范围，比如 Region-1 管 `[a, m)`，Region-2 管 `[m, z)` 这样。

每个 Region 独立组成一个 Raft Group——有自己的 Leader、自己的 Follower、自己的 term、自己的日志。Region-1 的选举跟 Region-2 完全无关，Region-1 的日志复制跟 Region-2 也完全无关。它们是完全独立的"小集群"，只是恰好运行在同一批物理机器上。

这跟上一篇讲的单 Raft Group 有什么区别？**单 Raft Group 是整个集群共享一个共识流程，Multi-Raft 是把共识流程打散成了成千上万个并行的小流程。** 每个 Region 内部的读写跟上一篇讲的完全一样——Leader 接收写入、复制日志、多数派确认、推进 commitIndex。但不同 Region 之间完全并行，互不阻塞。

### 为什么用 Range 分片而不是 Hash 分片

分片无非两种：Hash 和 Range。Hash 的好处是数据分布均匀，Range 的好处是相邻的 key 在同一个分片里。

TiKV 选了 Range，原因是数据库需要做范围查询（scan）。比如"查询所有以 `user_` 开头的 key"，如果用 Hash 分片，这些 key 会被打散到不同的分片里，范围查询就得广播到所有分片然后合并结果，代价很高。用 Range 分片的话，这些 key 大概率在同一个或者相邻的几个 Region 里，扫描效率高很多。

另外 Range 分片在做 Split 和 Merge 的时候也比 Hash 简单——只需要在某个 key 的位置切一刀，不需要重新 hash 和迁移大量数据。

---

## 三、Region 的生命周期：Split 和 Merge

### Split（分裂）

Region 不是一开始就切好固定不动的。一开始整个集群可能只有一个巨大的 Region 管所有数据。随着数据写入，某个 Region 的大小超过了阈值（默认 96MB），它就会自动**分裂**成两个更小的 Region。

Split 的过程大致是这样的：

1. Region 的 Leader 发现自己管的数据超过阈值了
2. Leader 选择一个中间 key 作为分裂点，把当前 Region 从这个 key 切成左右两半
3. 这个 Split 操作本身也是一条 Raft 日志，需要经过 Raft 共识——所有副本都要执行同样的分裂操作，保证一致
4. 分裂完成后，原来的一个 Raft Group 变成了两个，各自有独立的 Region ID、独立的 Raft 状态

注意一个关键点：**Split 是本地操作。** 分裂出来的两个 Region 一开始都在同一台机器上（因为数据本来就在这里）。如果需要把它们分散到不同机器上实现负载均衡，那是后面调度器（PD）的事情。

### Merge（合并）

跟 Split 相反，如果两个相邻的 Region 数据都很少（比如大量数据被删了），维护两个 Raft Group 反而是浪费——每个 Raft Group 都要维护心跳、选举定时器、日志复制等开销。这时候可以把它们合并回一个 Region。

Merge 比 Split 复杂得多，因为它涉及两个独立 Raft Group 的协调：Source Region 要把自己的数据"交给" Target Region，然后自己消失。这个过程中要保证不丢数据、不出现两个 Region 同时服务同一段 key 范围的情况。具体实现很复杂，但核心思想是一样的——通过 Raft 日志保证所有副本对"合并"这件事达成共识。

---

## 四、PD：全局的大脑

有了成千上万个 Region，谁来管它们？这就是 **PD（Placement Driver）** 的角色。PD 是整个 TiDB 集群的"调度中心"，相当于一个全局的大脑。

### PD 管什么

**元数据。** 每个 Region 的 key 范围是什么、Leader 在哪台机器上、有哪些副本——这些信息都存在 PD 里。客户端想读写某个 key，第一步就是问 PD："这个 key 属于哪个 Region？Region 的 Leader 在哪？" 拿到地址后直接去找那个 TiKV 节点。

**调度。** PD 持续监控所有 TiKV 节点的负载（CPU、磁盘、Region 数量、Leader 数量等），然后做全局调度：

- **负载均衡**：某台机器 Leader 太多了，把一些 Region 的 Leader 转移到空闲的机器上（Transfer Leader）
- **副本均衡**：某台机器的 Region 副本太多了，把一些副本迁移到其他机器上（Add Peer + Remove Peer）
- **热点打散**：某个 Region 被疯狂读写（热点），PD 可以触发 Split 把它切小，然后把切出来的部分调度到不同机器上
- **故障恢复**：某台 TiKV 宕机了，PD 发现那台机器上的 Region 副本数不够了（比如从 3 副本变成了 2 副本），就在其他机器上补一个新副本

**时间戳分配。** TiDB 的事务需要全局唯一递增的时间戳（TSO），这个也是 PD 提供的。

### PD 自己怎么保证不挂

PD 本身也是一个小的 Raft 集群（通常 3 或 5 个节点），用 Raft 保证自己的元数据不丢。所以 PD 挂掉一两个节点没关系，只要多数派还在就能正常服务。

---

## 五、性能优化：怎么在强一致的前提下做到高吞吐

前面说了，Multi-Raft 通过分片并行解决了吞吐量的天花板问题。但光靠分片还不够，单个 Raft Group 内部的性能也需要优化。TiKV 在 Raft 的工程实现上做了很多优化，这些优化不违反 Raft 的正确性，但大幅提升了性能。

### Batch（批量合并）

这是最直观的优化。标准 Raft 里 Leader 每收到一个写请求就发一次 AppendEntries，一次网络往返只处理一个请求。TiKV 的做法是把短时间内到达的多个写请求攒在一起，合并成一个大的 batch，一次性发给 Follower。对 RocksDB 的写入也是用 WriteBatch 一次性 fsync。

这样一次网络往返 + 一次磁盘 fsync 就搞定了几十甚至几百个请求。吞吐量从"一次一个"变成了"一次一批"，提升是数量级的。代价是什么？单个请求的延迟可能会稍微增加一点点（因为要等凑批），但在高并发场景下，凑批几乎是瞬间完成的，感知不到。

### Pipeline（流水线）

标准 Raft 里，Leader 发出一批日志后要等 Follower 回复确认，才能发下一批。中间这段等待时间网络是空闲的。

Pipeline 的思路是：Leader 不等回复就直接发下一批。它维护的 nextIndex 乐观地往前递增，相当于假设 Follower 一定能收到。只要 Follower 没报错，日志就像水管里的水一样连续不断地流过去。万一某一批失败了（Follower 返回了拒绝），再回退 nextIndex 重发就行。

这相当于把网络延迟从"串行等待"变成了"流水线覆盖"。如果网络往返是 2ms，标准模式一秒最多发 500 批，Pipeline 模式可以让网络带宽跑满，不再受往返延迟限制。

### Asynchronous Apply（异步应用）

标准 Raft 的完整流程是：append log → 复制到 Follower → 多数派确认（commit）→ 应用到状态机（apply）。这四步是串行的。

TiKV 的优化是把 apply 这一步拆出来异步做。日志一旦被 commit 了，Raft 层就可以立刻开始处理下一批请求，apply 在另一个线程池里并行执行。对单个客户端来说，它还是要等 apply 完成才能拿到结果（不能提前返回，否则读不到自己刚写的数据）。但对系统整体来说，"正在 apply 上一批"和"正在复制下一批"可以同时进行，流水线更深了，整体吞吐量更高。

### Lease Read（租约读）

这是读优化。上一篇讲过 ReadIndex——Leader 每次处理线性一致性读都要发一轮心跳确认自己还是 Leader。这一轮心跳虽然不需要写磁盘，但还是有网络开销。

Lease Read 的思路是：Leader 发心跳给 Follower 的时候记录一个时间戳。在选举超时时间到来之前，不可能有新 Leader 产生（因为 Follower 要等选举超时后才会发起选举）。所以在这个时间窗口内，当前 Leader 可以确信自己还是 Leader，直接读本地状态机返回就行，不需要任何网络通信。

这样线性一致性读的延迟就从"一次网络往返"降到了"本地内存访问"，几乎零开销。代价是依赖了时钟的准确性——如果机器时钟偏移严重，可能会出现 lease 还没到但实际上已经有新 Leader 了的情况。所以工程上会留一些安全余量。

### Follower Read

更进一步，TiKV 还支持 Follower Read。线性一致性读不一定非要 Leader 自己处理，Follower 也可以：Follower 先问 Leader"你当前的 commitIndex 是多少"，等自己 apply 到那个位置后再读本地状态机返回。这样读请求的负载就可以分散到所有副本上，不再只压在 Leader 身上。

---

## 六、跨 Region 事务怎么办

单 Region 内的读写通过 Raft 保证一致性，这没问题。但如果一个事务要写多个 key，这些 key 恰好分布在不同的 Region 里呢？比如转账——从 A 账户扣钱、给 B 账户加钱，A 和 B 在不同的 Region 里。

这就需要**分布式事务**了。TiDB 用的是 Percolator 模型的两阶段提交（2PC），大致流程：

**Prewrite 阶段**：事务协调者把要写的所有 key 分别发到各自所属的 Region Leader，每个 Leader 先把数据写入本地但不提交（加锁）。如果所有 Region 都 prewrite 成功，进入下一步；任何一个失败就全部回滚。

**Commit 阶段**：协调者向 primary key 所在的 Region 发送 commit 请求。primary 提交成功后，事务就算成功了（持久化了 commit 记录）。然后异步地通知其他 Region 也提交。

注意这里每个 Region 内部的写入本身还是走 Raft 的——prewrite 写的锁、commit 写的提交标记，都要经过 Raft 日志复制和多数派确认。所以 Multi-Raft + 2PC 实现了跨分片的强一致事务。

这跟单 Raft Group 有什么区别？单 Raft Group 不需要 2PC，因为所有数据都在一个地方，一条日志就能原子地修改多个 key。Multi-Raft 打散了数据之后，跨 Region 的原子性就得靠额外的 2PC 机制来保证了。

---

## 七、Raft Learner：异步副本的妙用

标准 Raft 里所有副本都是 Voter——参与投票、参与日志复制的多数派计算。但 TiKV 引入了一种特殊的副本角色：**Learner**。

Learner 跟 Follower 一样从 Leader 那里接收日志，但它**不参与投票，也不算入多数派**。Leader 在复制日志的时候不需要等 Learner 的确认——就算 Learner 挂了或者慢了，完全不影响正常的写入流程。

Learner 有什么用？

**用途一：安全地添加新副本。** 当 PD 决定给某个 Region 添加一个新副本时，不会直接把它变成 Voter。因为新副本数据是空的，需要先从 Leader 那里同步大量历史数据（snapshot）。如果一上来就是 Voter，在它追赶数据的这段时间里，实际可用的 Voter 数量变了，多数派计算可能出问题。所以正确的做法是：先加为 Learner → 等它追赶上进度 → 再 promote 成 Voter。

**用途二：TiFlash 列存副本。** 这是 TiDB 里最精彩的用法。TiFlash 是 TiKV 的列存扩展，用于 OLAP 分析查询。TiFlash 节点作为 Raft Learner 异步地从 Leader 接收日志，把行格式的数据转换成列格式存储。因为 Learner 不参与多数派，所以 TiFlash 再慢也不会拖累 OLTP 的写入性能——完美的读写隔离。而且因为数据是通过 Raft 日志同步过来的，所以 TiFlash 读取的时候可以通过"校对 Raft Index"来确认数据新鲜度，保证一致性。

---

## 八、跟 Redis 对比：为什么一个放弃了一致性，另一个不用放弃

理解了 Multi-Raft 之后，再回头看 Redis 和 TiDB 的选择就很清晰了。

**Redis 的设计哲学是极致低延迟。** 它是内存数据库，单次操作追求微秒级响应。如果每次写入都等多数派确认（几毫秒的网络往返），延迟直接从微秒变毫秒，涨了几百倍，这对 Redis 的缓存场景来说不可接受。所以 Redis 选了异步复制——Master 写完内存立刻返回，异步发给 Slave。Sentinel 的 epoch 机制只管选主，不管数据一致性。代价就是主从切换时可能丢最近没来得及同步的写入。

**TiDB 的设计哲学是强一致的数据库。** 它追求的不是单次操作的极致延迟（几毫秒对数据库来说完全正常），而是在保证强一致的前提下，通过 Multi-Raft 把整体吞吐量水平扩展上去。具体来说：

- **分片并行**解决了"只有一个 Leader 处理写入"的瓶颈——1000 个 Region 分布在 10 台机器上，就有 1000 个 Leader 可以并行处理写入
- **Batch** 解决了"一次网络往返只处理一个请求"的浪费——一次往返搞定一批
- **Pipeline** 解决了"等回复期间网络空闲"的浪费——不等回复连续发
- **Async Apply** 解决了"apply 和复制串行"的瓶颈——并行做
- **Lease Read** 解决了"一致性读也要网络往返"的开销——lease 内直接本地读

所以本质区别不是"Raft 性能差所以不能用"，而是两者优化的目标维度不同。Redis 优化的是**单次延迟**（latency），从毫秒压到微秒；TiDB 优化的是**整体吞吐**（throughput），从单 Leader 扩展到万级并行。单看一次写入，TiKV 肯定比 Redis 慢（要等网络往返+磁盘 fsync），但看集群整体每秒能处理多少个事务，TiDB 的水平扩展能力是 Redis 做不到的。

---

## 九、一些我自己的思考

**关于 Multi-Raft 的本质。** 想清楚之后发现 Multi-Raft 不是什么新算法，它就是"把大问题拆成小问题"这个朴素思想在分布式共识上的应用。每个小问题（Region）内部还是标准的 Raft，没有任何魔改。Multi-Raft 的复杂性不在共识算法本身，而在**管理**——怎么切分、怎么调度、怎么分裂合并、怎么跨分片事务。这些都是工程问题，不是算法问题。

**关于 PD 的角色。** PD 看起来像是一个"中心节点"，那它会不会成为单点瓶颈？实际上 PD 只处理元数据和调度决策，不参与数据的读写路径。客户端拿到 Region 路由信息后会缓存起来，后续读写直接找 TiKV，不经过 PD。只有在 Region 发生迁移或 Split 导致缓存失效时才需要重新问 PD。所以 PD 的负载其实很轻，不是瓶颈。

**关于为什么 Region 默认 96MB。** 太大了不好——一个 Region 太大意味着它包含的 key 范围太广，容易成为热点，而且 Split/迁移时要搬的数据量大，影响正常服务。太小了也不好——Region 太多的话，每个 Region 都要维护心跳、选举定时器、Raft 日志，元数据开销就上去了，PD 的调度压力也大。96MB 是一个经验平衡点。

**关于 Raft 和 Paxos 的选择。** TiKV 选了 Raft 而不是 Paxos，本质原因就是 Raft 好懂好实现好调试。Multi-Raft 的架构里你要管理成千上万个 Raft Group，如果底层共识算法本身就晦涩难懂，出了 bug 几乎不可能排查。Raft 的 Leader 机制让日志流向始终是单向的（Leader → Follower），状态变迁明确，日志冲突的解决规则简单暴力（以 Leader 为准截断 Follower），这在工程上是巨大的优势。
