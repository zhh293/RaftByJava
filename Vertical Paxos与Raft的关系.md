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

---

## 七、从 Vertical Paxos 推导 Multi-Raft：完整推理过程

> Vertical Paxos 论文本身没有写"Multi-Raft"三个字（论文是 2009 年的，TiKV 是 2016 年之后的工程），但 Multi-Raft 的每一个核心架构决策，都可以从 Vertical Paxos 的理论框架里一步一步推导出来。下面就是完整的推理链条。

---

### 架构对比图：单 Raft vs Multi-Raft

先通过两张图直观感受一下，单 Raft 架构和 Multi-Raft 架构到底长什么样、差在哪里：

**单 Raft 架构（一个 Group 管所有数据）：**

```
                         ┌─────────────────────────────────────┐
                         │           客户端请求                  │
                         └──────────────────┬──────────────────┘
                                            │ 所有读写都压到一个 Leader
                                            ▼
                    ┌───────────────────────────────────────────────┐
                    │              唯一的 Raft Group                  │
                    │                                               │
                    │   ┌─────────┐   ┌─────────┐   ┌─────────┐   │
                    │   │ Node A  │   │ Node B  │   │ Node C  │   │
                    │   │(Leader) │──▶│(Follower)│   │(Follower)│   │
                    │   │         │──▶│         │   │         │   │
                    │   │ 全量数据 │   │ 全量数据 │   │ 全量数据 │   │
                    │   │ [a～z)  │   │ [a～z)  │   │ [a～z)  │   │
                    │   └─────────┘   └─────────┘   └─────────┘   │
                    │                                               │
                    └───────────────────────────────────────────────┘

  瓶颈：单 Leader 串行处理所有写入，存储无法水平扩展，加机器只能提高容灾
```

**Multi-Raft 架构（数据分片，多个 Group 并行）：**

```
                         ┌─────────────────────────────────────┐
                         │           客户端请求                  │
                         └───┬──────────────┬──────────────┬───┘
                             │              │              │
                   key∈[a,m) │    key∈[m,t) │    key∈[t,z) │
                             ▼              ▼              ▼
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                                                                         │
 │  ┌─ Region-1 ──────┐   ┌─ Region-2 ──────┐   ┌─ Region-3 ──────┐     │
 │  │  数据范围[a, m)   │   │  数据范围[m, t)   │   │  数据范围[t, z)   │     │
 │  │                  │   │                  │   │                  │     │
 │  │ Node-A (Leader)  │   │ Node-B (Leader)  │   │ Node-C (Leader)  │     │
 │  │ Node-B (Follower)│   │ Node-C (Follower)│   │ Node-A (Follower)│     │
 │  │ Node-D (Follower)│   │ Node-A (Follower)│   │ Node-D (Follower)│     │
 │  └──────────────────┘   └──────────────────┘   └──────────────────┘     │
 │                                                                         │
 └─────────────────────────────────────────────────────────────────────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌────────────┐    ┌────────────┐    ┌────────────┐
    │   Node-A   │    │   Node-B   │    │   Node-C   │    ┌────────────┐
    │ R1-Follower│    │ R1-Leader  │    │ R2-Follower│    │   Node-D   │
    │ R2-Follower│    │ R2-Leader  │    │ R3-Leader  │    │ R1-Follower│
    │ R3-Follower│    │            │    │            │    │ R3-Follower│
    └────────────┘    └────────────┘    └────────────┘    └────────────┘

  优势：3 个 Leader 并行处理写入，数据分散存储可水平扩展
  关键：Leader 打散在不同机器上，负载均衡
```

**加上 PD 的完整 Multi-Raft 全景图：**

```
                    ┌──────────────────────────────────┐
                    │     PD Cluster（3节点 Raft）       │
                    │                                  │
                    │  · 存储全局元数据（哪个Region在哪） │
                    │  · 分配 Region ID / Peer ID       │
                    │  · 收集心跳 → 感知负载和健康       │
                    │  · 下发调度指令：                  │
                    │    - 加副本 / 删副本               │
                    │    - Transfer Leader              │
                    │    - Split / Merge                │
                    └───────────────┬──────────────────┘
                                    │ 调度指令（非数据路径）
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
     ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
     │   TiKV-1    │      │   TiKV-2    │      │   TiKV-3    │
     │             │      │             │      │             │
     │ Region-1(L) │◀────▶│ Region-1(F) │◀────▶│ Region-1(F) │
     │ Region-2(F) │◀────▶│ Region-2(L) │◀────▶│ Region-2(F) │
     │ Region-3(F) │◀────▶│ Region-3(F) │◀────▶│ Region-3(L) │
     │ Region-4(L) │◀────▶│ Region-4(F) │◀────▶│ Region-4(F) │
     │     ...     │      │     ...     │      │     ...     │
     └─────────────┘      └─────────────┘      └─────────────┘
              │                                         │
              │        ┌─────────────┐                 │
              └───────▶│  TiFlash    │◀────────────────┘
                       │ (Learner)   │
                       │ 列存副本     │
                       │ 不参与投票   │
                       │ 服务OLAP查询 │
                       └─────────────┘

  数据路径：客户端 → TiKV 节点（直接读写对应 Region 的 Leader）
  控制路径：PD ← TiKV 心跳上报 / PD → TiKV 调度指令
  两条路径完全分离，PD 不在数据读写的关键路径上
```

有了这三张图的直观印象，下面来看为什么 Multi-Raft 的每个部件都能从 Vertical Paxos 推导出来：

---

### 第一步：从"一个共识实例"到"大量并行实例"——为什么要分片

**出发点：** Vertical Paxos 论文在背景部分有一句非常关键的话——

> "真实的大规模系统，通常由大量的 replica group 组成，每组负责一小片数据。每组内部用某种共识协议保持一致。系统有充裕的空闲服务器用于重配置和部署新副本。"

翻译成大白话就是：没有哪个正经的工业系统会让整个集群只跑一个 Paxos/Raft 实例。数据量一大、请求量一大，你必然要把数据切成很多片，每一片单独跑一个共识组。

**推理过程：**

一个 Raft Group 只有一个 Leader，所有写操作串行通过这一个 Leader。假设你有 1TB 数据、每秒 10 万次写请求，单 Leader 不可能扛住——CPU、网络、磁盘都是瓶颈。而且所有副本存的是完全相同的数据，加机器只能提高容灾能力，不能扩展存储容量。

自然的结论：**把数据切成 N 片，每片独立跑一个 Raft Group，就有 N 个 Leader 可以并行处理写入。** 这就是 Multi-Raft 最朴素的出发点。

到这一步为止，还不需要 Vertical Paxos 的理论——任何人都能想到"分片 + 并行"。

但问题紧接着就来了：**分片之后，每片的节点集合不可能永远不变。** 机器会宕机、需要扩容、需要负载均衡。你必须能在不停服的情况下，动态增减每个分片的成员。这个"动态增减成员"的问题，就是 Vertical Paxos 的核心战场——reconfiguration。

**结论：** 分片本身是朴素的工程决策，但分片之后的"每片怎么安全地换成员"，需要 Vertical Paxos 的理论来支撑。

---

### 第二步：从"Configuration Master"到"PD（Placement Driver）"

**出发点：** Vertical Paxos 的一个核心创新——引入外部配置主控 Master。

论文原话的意思是：不要让每个共识组自己决定怎么重配置，而是引入一个更高层的全局管理者来统一协调。这个 Master 负责：决定每个 ballot 的配置是什么（谁是 Leader、谁是 Acceptor）、分配 ballot number、协调重配置的时机。Master 自身也用复制状态机来实现（保证高可用）。

**推理过程：**

现在假设你有上万个 Raft Group（Region），每个都可能随时需要重配置。问几个问题：

- 某台机器宕机了，上面 100 个 Region 的副本都少了一个。补副本补到哪台机器上？总不能让每个 Region 自己决定吧——万一 100 个 Region 都选了同一台空闲机器，那台机器瞬间过载。
- 某台机器负载太高了，要把一些 Region 的 Leader 转走。转到哪里？转多少个？这需要全局视角——知道每台机器当前负载是多少。
- 某个 Region 数据太大需要分裂。分裂后的两个新 Region 要不要分散到不同机器上？这也需要全局调度。

这些问题都有一个共同特点：**需要全局信息才能做出好的决策，单个 Raft Group 自身不具备全局视角。**

所以你需要一个集中的角色——它掌握全局的机器负载、Region 分布、副本数量等信息，然后统一下发调度指令。

这就是 **PD（Placement Driver）** 的来源。PD 就是 Vertical Paxos 里 Configuration Master 在"管上万个 Raft Group"场景下的工程实现。

**对照表：**

| Vertical Paxos 论文 | Multi-Raft 工程实现（TiKV） |
|---------------------|---------------------------|
| Configuration Master 决定每个 ballot 的配置 | PD 决定每个 Region 的副本分布和 Leader 位置 |
| Master 分配 ballot number | PD 分配 Region ID、Peer ID |
| Master 协调重配置时机 | PD 根据负载和健康状态下发调度指令 |
| Master 自身用复制状态机实现 | PD 自身是一个 3 节点 Raft/etcd 集群 |
| Master 不参与数据面的正常读写 | PD 不在数据读写路径上，只管元数据和调度 |

---

### 第三步：从"VP-II 的状态迁移时序"到"Raft Learner"

**出发点：** Vertical Paxos II 的核心原则——**新配置必须先完成状态迁移（State Transfer），才能被激活参与共识。**

先搬家，再接活。

**推理过程：**

PD 决定给 Region-5 在 Node-D 上加一个新副本。这个新副本一开始数据是空的——它需要从 Leader 那里同步可能几十 MB 甚至几百 MB 的历史数据。

关键问题：**在这个新副本追赶数据的过程中，它能不能算入多数派（quorum）？**

假设原来 Region-5 有 3 个副本（A、B、C），多数派 = 2。现在加了 D，变成 4 副本，多数派 = 3。但 D 还在追赶数据，可能随时卡住或者很慢。

如果 D 已经被算入 quorum 了，那 Leader 要凑齐 3 个确认才能提交。万一 D 很慢，每次写入都被拖住了——本来 A、B 两个确认就够（旧配置下），现在非得等 D 也确认才行（新配置下），性能直接下降。

更极端的情况：如果 D 在追赶过程中直接挂了，你凑不齐 3 票（A、B 给了，C 是 Leader 也给了，但多数派要 3 个 non-Leader？不对——Leader 也算，ABC 三个旧节点投票就够了……但是按 4 节点算多数派是 3，如果 D 挂了、B 也恰好挂了，就只剩 A 和 C，凑不齐 3 票，写入卡死），系统反而因为"加副本"这个操作变得更脆弱了。

这就是为什么新副本**不能一上来就参与投票**。

Vertical Paxos II 的理论告诉我们正确的做法：

1. 新节点先以"非 active 配置成员"的身份存在——只同步数据，不参与决策
2. 等数据追赶完毕后，由 Master（PD）正式将其"激活"——提升为正式的投票成员

翻译成 Raft 的工程术语：

1. 新节点先作为 **Learner** 加入——从 Leader 接收日志，但不参与投票、不计入多数派
2. 等 Learner 的日志追赶到接近 Leader 的进度后
3. PD 下发指令将 Learner **提升为 Voter**——从此它正式参与投票，多数派人数加一

**完整的 VP-II 时序对照：**

```
VP-II 理论流程                              Multi-Raft 工程流程
─────────────────────────────────────────────────────────────────
① Master 决定新配置（但不激活）    →    ① PD 决定加副本到 Node-D
② 新成员开始 State Transfer       →    ② Node-D 作为 Learner 开始同步日志/快照
③ State Transfer 期间：                  ③ Learner 期间：
   - 新成员不参与共识                      - 不参与投票
   - 旧配置继续正常工作                    - 旧的 3 副本继续正常读写
   - Master 不激活新配置                   - PD 不提升 Learner
④ State Transfer 完成              →    ④ Learner 日志追赶到接近 Leader
⑤ 新成员通知 Master："我准备好了"  →    ⑤ PD 检测到 Learner 已 ready
⑥ Master 激活新配置               →    ⑥ PD 下发 ConfChange：Learner → Voter
⑦ 旧配置退役（如果需要）          →    ⑦ PD 下发 RemovePeer 移除旧节点（如果需要）
```

---

### 第四步：从"VP-I 的并行激活"到"Region Split"

**出发点：** Vertical Paxos I 的核心原则——**新配置可以立即激活，状态迁移和服务推进并行进行。**

先接活，再搬家。

**推理过程：**

Region Split 是一个非常特殊的"重配置"操作：一个 Raft Group 变成两个 Raft Group。但它有一个独特的性质——**不需要跨网络的数据迁移。**

想想看：Region-1 管 [a, z)，现在要从 m 这个位置切一刀，变成 Region-1 管 [a, m) 和 Region-2 管 [m, z)。切之前数据就在本机上，切之后数据还在本机上——只是逻辑上分成了两个 Group 各自维护各自的 Raft 状态。物理上不需要搬任何一个字节的数据。

既然不需要 State Transfer，那就没必要用 VP-II 的保守策略（先搬完再激活）。直接用 VP-I 的思路——**新配置立即激活**。

具体流程：

1. Region-1 的 Leader 发现数据量超过阈值（比如 96MB）
2. Leader 选择一个中间 key（比如 m）作为分裂点
3. Leader 把"在 m 处分裂"这个操作写成一条 Raft 日志，通过共识让所有副本一起执行
4. 执行完毕后，同一台机器上就有了两个独立的 Raft Group，**立刻都可以对外服务**

对应到 VP-I：
- "新配置立即 active" = 分裂出来的两个 Region 立刻都能处理读写
- "状态迁移可以后续再做" = 如果 PD 后续要把其中一个 Region 调度到其他机器上，那部分迁移才是真正的 State Transfer（走 Learner 机制，即 VP-II）

所以 **Region Split 是 VP-I 和 VP-II 的组合**：
- Split 操作本身 → VP-I 思路（无需迁移数据，立即生效）
- Split 之后的跨机器调度 → VP-II 思路（先加 Learner 搬数据，搬完提升 Voter）

---

### 第五步：从"Read/Write Quorum 分离"到"Multi-Raft 的读策略菜单"

**出发点：** Vertical Paxos 的核心 trade-off——Read Quorum 越小，读越快；Write Quorum 越大，写越安全但越慢。两者的乘积受约束。

**推理过程：**

在 Multi-Raft 体系里，每个 Region 都是一个独立的 Raft Group。对于每个 Group，你可以独立地选择"读策略"——本质上就是在选择 Read Quorum 的大小。

**选择一：Read Quorum = 多数派 → ReadIndex**

Leader 每次处理读请求前，向多数派发一轮心跳确认自己还是 Leader。只有多数派回复了，才说明"我还是 Leader，我的数据是最新的"。

- 优点：绝对安全，不依赖时钟
- 缺点：每次读都有一次网络往返开销

**选择二：Read Quorum = 1（Leader 自己）→ Lease Read**

Leader 知道"只要选举超时时间没到，就不可能有新 Leader 产生"。所以在 lease 期内，Leader 直接读自己本地状态机返回，不需要任何网络通信。

- 优点：读性能极高，零网络开销
- 缺点：依赖时钟准确性。如果时钟漂移导致 lease 判断错误，可能读到过期数据

**选择三：Read Quorum = 1（任意节点）→ Follower Read**

进一步放宽——如果 Follower 能确认自己的 appliedIndex 不落后于 Leader 的 commitIndex，它自己也能服务读请求。

- 优点：读负载分散到所有副本，不再只压 Leader
- 缺点：Follower 需要先问一下 Leader 当前 commitIndex 是多少

**选择四：Read Quorum 和 Write Quorum 解耦给不同消费者 → TiFlash**

TiFlash 是 TiKV 的列存扩展，作为 Raft Learner 异步接收日志。对 TiFlash 来说：
- 它不参与 Write Quorum（不影响写入性能）
- 它自己就能构成 Read Quorum = 1（OLAP 查询直接读 TiFlash 本地，只要校对 Raft Index 保证数据足够新）

这等于在**同一个 Raft Group 里，对不同的消费者使用了不同的 quorum 配置**——OLTP 走 Voter（标准 quorum），OLAP 走 Learner（独立的宽松 quorum）。这种灵活性正是 Read/Write Quorum 分离理论开辟出来的设计空间。

---

### 第六步：从"Master 全局调度"到"PD 的调度策略"

**出发点：** Vertical Paxos 论文说 Configuration Master "根据系统变化来计算 read 和 write b-quorum"，并且"动态添加元素到 Ballots"。

**推理过程：**

PD 作为 Multi-Raft 体系的 Configuration Master，它的全局调度策略本质上就是在回答 Vertical Paxos 的核心问题——"什么时候重配置、怎么重配置"：

| PD 的调度行为 | VP 理论对应 |
|-------------|-----------|
| 某台机器宕机 → PD 给受影响的 Region 补副本 | Master 检测到配置异常 → 发起重配置 |
| PD 选择一台空闲机器放新副本 | Master 决定新配置的 Acceptor 集合 |
| PD 先加 Learner 再提升 Voter | VP-II：先 State Transfer 再激活 |
| PD 做 Transfer Leader（把 Leader 转到另一个 Voter） | Master 在配置内更换 Leader |
| PD 触发 Region Split | VP-I：新配置立即生效（本地操作无需迁移） |
| PD 触发 Region Merge | 两个独立配置合并为一个配置（逆向 Split） |
| PD 做热点打散（Split + 调度） | VP-I + VP-II 组合 |

PD 的每一种调度操作，都可以被理解为 Vertical Paxos 框架下的某种 reconfiguration 动作。

---

### 第七步：把整条推导链串起来

```
                    Vertical Paxos 理论框架
                           │
          ┌────────────────┼────────────────┐
          │                │                │
    读写Quorum分离     外部Master       重配置安全性
          │                │                │
          ▼                ▼                ▼
  ┌───────────────┐  ┌──────────┐  ┌──────────────────┐
  │ 读策略菜单     │  │   PD     │  │ Learner + Voter  │
  │ ·ReadIndex    │  │ ·元数据   │  │ ·加副本先Learner │
  │ ·Lease Read   │  │ ·调度    │  │ ·追赶完提升Voter │
  │ ·Follower Read│  │ ·故障恢复│  │ ·Split立即生效   │
  │ ·TiFlash      │  │ ·Split   │  │ ·Merge协调退役   │
  └───────────────┘  └──────────┘  └──────────────────┘
          │                │                │
          └────────────────┼────────────────┘
                           │
                           ▼
                    Multi-Raft 完整架构
                   （TiKV / CockroachDB）
```

**用一段话说清楚：**

Vertical Paxos 提供了三个理论工具——读写 Quorum 分离、外部 Configuration Master、以及 VP-I/VP-II 两种重配置时序。Multi-Raft 就是把这三个工具在"大规模分布式数据库"这个场景下全部用上的结果：数据分片产生了上万个独立的 Raft Group；PD 作为外部 Master 统一管理所有 Group 的配置；每个 Group 的成员变更遵循 VP-II 的"先搬状态再激活"时序（Learner 机制）；Split 操作遵循 VP-I 的"先激活再搬状态"时序（本地操作无需迁移）；读策略则是在 Read Quorum 参数空间里根据场景选不同的点。

**Multi-Raft 不是某个工程师"拍脑袋"设计出来的新架构，而是 Vertical Paxos 理论在"大规模 + 高吞吐 + 强一致"这组工程约束下的必然展开形式。** 每一个架构决策——PD 为什么存在、Learner 为什么不投票、Split 为什么能立即生效、TiFlash 为什么不影响写性能——追根到底都是 Vertical Paxos 里某个理论概念的工程兑现。
