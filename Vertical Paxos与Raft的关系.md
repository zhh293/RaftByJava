# Vertical Paxos 与 Raft 的关系——读完两篇文章之后的理解

> 这篇文档是在读完《vertical-paxos-raft-teacher-style讲解》和《我眼里的 Raft》之后，把两篇内容放在一起比对后写出来的。目的是搞清楚 Vertical Paxos 到底在说什么、它对理解 Raft 有什么帮助、以及为什么读完它会觉得"被冲击了一下"。

---

## 一、Vertical Paxos 这篇论文到底在做什么

一句话：**它在给"工程界常用但缺乏理论解释的那些复制协议"找一个统一的理论归宿。**

具体来说，它做了三件事：

第一件事是把"重配置"从分布式共识的边缘问题拉到正中央。经典 Paxos 假设副本集合固定不变，但真实系统里节点会宕机、会替换、会扩缩容，需要在不停服的情况下更换副本。这个问题在理论上一直没被体面地解决过——工程上各显神通，但理论框架缺失。Vertical Paxos 就是专门来填这个坑的。

第二件事是把读 quorum 和写 quorum 拆开。经典 Paxos 里大家默认"quorum 就是多数派"，但 Vertical Paxos 指出：读和写的 quorum 可以不一样大，只要它们的交集不为空就行。这个拆分带来了巨大的灵活性——你可以根据系统的读写比例选择不同的 quorum 策略。

第三件事是证明了 primary-backup 协议是 Paxos 家族的一个特例。这是整篇论文最有冲击力的结论——那些看起来"土但好用"的主从复制协议，不是理论之外的民间发明，而是 Paxos 在特定参数配置下的自然退化形式。

---

## 二、Raft 在 Vertical Paxos 的视角里是什么

Raft 本质上是一个 primary-backup 协议。Leader 承担所有写操作，Follower 只负责被动同步——日志流是单向的，从 Leader 到 Follower，永远不会反向。

而 Vertical Paxos 告诉我们，当你把参数这么设的时候：

- **读 quorum = 1**（新 Leader 上任后，只要读自己本地状态就能知道全部历史，因为它必然拥有所有已提交日志——Raft 的选举约束保证了这一点）
- **写 quorum = 多数派**（每条日志必须被多数派确认才算提交）
- **Leader 自身是 acceptor**（Leader 也存日志，不是只做转发）

这套参数组合下，Paxos 的通用协议就自然退化成了 Raft 那套行为模式。换句话说，Raft 不是凭空发明的，它是 Paxos 理论空间中一个精心选择的特定点。

这个认知非常重要：它意味着 Raft 的安全性不是需要从零开始证明的，它继承自 Paxos 家族的统一安全性框架。你学 Raft 时觉得"选举约束保证了 Leader 拥有所有已提交日志"这件事很精巧但不知道为什么可以这么设计——答案就在这里：因为它等价于"读 quorum = 1 + Leader 必须在旧配置里"，在 Vertical Paxos 的框架下这是数学上可证明安全的。

---

## 三、逐点对照：Vertical Paxos 的概念在 Raft 里长什么样

### 1. 外部配置 Master → Raft 的成员变更机制（以及 Multi-Raft 里的 PD）

Vertical Paxos 引入了一个"外部配置主控 master"，负责决定当前副本集合是谁、Leader 是谁、什么时候切配置。

在单集群 Raft 里，这个角色是隐式的——配置变更通过特殊的配置日志来完成，Leader 自己充当了这个"配置 master"的角色。

但在 Multi-Raft 体系（比如 TiKV/TiDB）里，这个角色就显式化了——**PD（Placement Driver）**。PD 负责全局的 Region 调度、副本迁移、Leader 转移、分裂合并。它就是 Vertical Paxos 里那个 configuration master 的直接工程实现。

### 2. 读/写 Quorum 分离 → Raft 的读一致性策略

Vertical Paxos 的核心 trade-off：读 quorum 越小，读越快，但写 quorum 就得越大（写越慢）；反之亦然。

Raft 里这个 trade-off 体现在读一致性的不同策略上：

- **ReadIndex**：Leader 要向多数派发一轮心跳确认自己还是 Leader，然后才能返回读结果。这相当于"读 quorum = 多数派"——读的时候也需要多数派背书。安全但有网络开销。
- **Lease Read**：Leader 在心跳租约期内直接读本地状态机返回，不需要额外确认。这相当于"读 quorum = 1"——只要 Leader 自己有数据就行。更快但依赖时钟精度。
- **Follower Read**：允许 Follower 直接服务读请求，但要带 appliedIndex 水位。这是在多个节点上分摊读压力，相当于在"读 quorum = 1"的基础上进一步把这个"1"从 Leader 扩展到了任意节点。

所以 Vertical Paxos 的读写 quorum 分离，在 Raft 里不是一个显式的参数，而是体现为不同的读策略选择。

### 3. State Transfer（状态迁移）→ Raft 的日志追赶与快照

Vertical Paxos 花了很多篇幅讨论"旧配置的状态怎么安全地交给新配置"，这个问题在 Raft 里就是：

- **新加入的节点怎么追赶上集群当前的状态？** 答案是 Leader 通过 AppendEntries 逐步补发日志（日志追赶），如果落后太多就发送快照（Snapshot/InstallSnapshot）。
- **Leader 切换后新 Leader 怎么知道全部历史？** 答案是选举约束保证了新 Leader 必然拥有所有已提交日志，所以它不需要额外的 state transfer——这恰恰是"读 quorum = 1"带来的好处。

### 4. Vertical Paxos I vs II → Raft 的 Joint Consensus vs Single-Node Change

这是对应关系最直接的一组：

**Vertical Paxos I**（先激活新配置，再慢慢搬状态）对应 **Raft 的 Joint Consensus**——先进入一个"新旧两套配置同时生效"的过渡状态（$C_{old,new}$），在这个过渡期间新旧配置都参与决策，等过渡日志被提交后再正式切到新配置。这样系统不需要停服等待，牺牲的是历史依赖链可能变长。

**Vertical Paxos II**（先搬完状态，再激活新配置）对应 **Raft 的 Single-Node Change**——每次只增删一个节点，等这个变更完全提交后再进行下一个变更。边界清晰，不需要维护复杂的过渡状态，但如果要做大规模变更就得排队一个一个来。

两者没有绝对的优劣——Joint Consensus 更灵活但实现复杂，Single-Node Change 更简单但变更速度慢。就像 Vertical Paxos 论文说的：它们是在"服务连续性"和"历史依赖长度"之间做不同取舍。

---

## 四、这篇论文对理解 Raft 的真正帮助

### 帮助一：从"记住规则"上升到"理解为什么规则是这样的"

学 Raft 的时候，很多规则看起来像是"作者规定的"，比如：

- 为什么 Leader 必须拥有所有已提交日志才能当选？
- 为什么 commitIndex 只能通过当前任期的日志来推进？
- 为什么日志只能从 Leader 流向 Follower？

读完 Vertical Paxos 你会发现，这些不是任意规定，而是"读 quorum = 1 + 写 quorum = 多数派"这组参数选择下的必然推论。如果读 quorum 是 1（意味着新 Leader 只需要读自己就够了），那新 Leader 必须拥有全部已提交日志，否则这个"读 quorum = 1"就不安全。要保证这一点，就必须在选举时做日志新旧比较。一环扣一环，全是 quorum 约束的逻辑推导。

### 帮助二：理解 Raft 的局限性和设计空间

Raft 选择了"读 quorum = 1"，意味着选举约束很强（Leader 必须日志最新）。这带来了简洁性——新 Leader 上来不需要额外询问其他节点就知道全部历史。但代价是选举时对候选人的要求很高，如果集群中日志最新的节点恰好网络不好，选举可能会多轮空转。

如果你理解了 Vertical Paxos 的 quorum 理论，就会知道 Raft 不是唯一的正确设计。你完全可以设计一个"读 quorum = 多数派"的变种——选举约束放宽（不要求候选人日志最新），但新 Leader 上任后需要额外问一圈其他节点来补齐历史。这就是另一种 trade-off，适用于不同的场景。

### 帮助三：看懂工业级系统的架构决策

一旦你拥有了 Vertical Paxos 的视角，很多工业系统的设计就变得可以理解了：

- **TiKV 的 PD** 为什么要作为独立组件存在？因为它是 configuration master。
- **TiKV 的 Raft Learner** 为什么在学习阶段不参与投票？因为它还在做 state transfer，还没被"激活"到新配置里。
- **CockroachDB 的 Range 迁移** 为什么要先加 Learner 再提升为 Voter？这就是 Vertical Paxos II 的思路——先搬状态，再正式激活。
- **etcd 的 Learner 节点** 为什么不计入 quorum？同样的道理。

### 帮助四：给"成员变更"这个难题提供思维框架

成员变更是 Raft 实现中公认最难的部分。很多人读论文时能理解 Joint Consensus 的机制，但不理解"为什么这个机制是安全的"。Vertical Paxos 给出了答案：Joint Consensus 安全，是因为它满足了"新旧配置的 quorum 交集不为空"这个根本约束。只要这个约束被满足，无论你用什么具体机制来实现配置切换，安全性都有保证。

---

## 五、对什么方向有启发

### 对学习方向的启发

如果你只学 Raft 一个协议，你的认知是"一条线"——知道选举怎么做、日志怎么复制，但不知道为什么不是别的做法。读了 Vertical Paxos 之后，你的认知变成了"一个平面"——知道 Raft 是一个更大设计空间里的一个点，知道这个点为什么被选中，也知道其他的点在哪里、适合什么场景。

这意味着接下来你再去看 Multi-Paxos、EPaxos、Flexible Paxos 这些变种时，不会觉得它们是"完全不同的新东西"，而是同一个理论框架下的不同参数选择。学习效率会显著提升。

### 对工程方向的启发

Vertical Paxos 的核心观点——"读写 quorum 分离"和"配置管理外提"——是很多现代分布式系统架构的理论基础：

- 数据库的 quorum 读写策略（Cassandra 的 ONE/QUORUM/ALL 就是在调这个旋钮）
- 云原生存储的多副本调度（Kubernetes 的 etcd 集群管理、TiKV 的 PD）
- 配置中心的独立部署（ZooKeeper 作为外部协调服务的定位）

理解了 Vertical Paxos，你就理解了这些架构决策背后的共同理论根源。

### 对系统设计方向的启发

论文还暗示了一个重要观点：衡量一个共识协议的好坏，不能只看"正常情况下每次操作要几轮消息"。更重要的可能是：

- 出故障后恢复的代价有多大？
- 扩缩容的时候要停多久服？
- 状态迁移的时候能不能继续服务？

这些"非正常路径"的性能，往往才是工业级系统选型的真正决定因素。Raft 之所以比 Multi-Paxos 更受工程欢迎，不是因为正常路径更快（其实差不多），而是因为它的异常处理和成员变更路径更清晰可控。

---

## 六、总结：两篇文章的关系

| 维度 | 《我眼里的 Raft》 | Vertical Paxos 论文 |
|------|-------------------|---------------------|
| 抽象层次 | 具体协议的机制细节 | 协议家族的统一理论框架 |
| 回答的问题 | "怎么做" | "为什么可以这么做" |
| 讲的是 | 一个特定的协议实现 | 一族协议的共同安全性基础 |
| 对成员变更 | 给出 Joint Consensus / Single-Node Change 的具体做法 | 解释为什么这些做法是安全的、它们在理论空间里的位置 |
| 对读写 | 给出 ReadIndex / Lease Read 的具体实现 | 解释这些策略本质上是在调"读 quorum"的大小 |
| 对 primary-backup | 默认使用这种模式（Leader 写，Follower 同步） | 证明这种模式是 Paxos 在极端 quorum 配置下的退化形式 |

如果用一个比喻：**Raft 是"牛顿力学"——告诉你物体怎么运动、力怎么算；Vertical Paxos 是"拉格朗日力学"——告诉你牛顿力学为什么是对的、它在更一般的框架里占什么位置、以及同一个框架还能推出什么别的东西。**

你不需要懂拉格朗日力学也能用牛顿力学解题。但懂了之后，你对整个力学世界的理解深度完全不一样——你知道哪些是本质的、哪些是可以变的、哪些看似不同的系统其实是同一回事。

读完这两篇文章的正确感受就是：Raft 不再是一个"记住规则就行"的算法，而是一个你真正理解了设计动机的系统。
