# Vertical Paxos and Primary-Backup Replication 论文逐节精讲

> **论文信息**
> - 标题：Vertical Paxos and Primary-Backup Replication
> - 作者：Leslie Lamport, Dahlia Malkhi, Lidong Zhou
> - 发表：PODC 2009（ACM Symposium on Principles of Distributed Computing）
> - 技术报告编号：MSR-TR-2009-63, Microsoft Research, May 2009
> - 全文链接：https://lamport.azurewebsites.net/pubs/vertical-paxos.pdf

---

## 论文结构总览

这篇论文的核心论点可以用一句话概括：**Primary-Backup 复制协议不是"理论之外的工程发明"，而是 Paxos 算法家族在特定参数配置下的自然特例。** 为了证明这一点，作者发明了一类叫做 Vertical Paxos 的新算法，作为连接经典 Paxos 和 Primary-Backup 的桥梁。

论文按以下逻辑展开：

1. 提出问题：经典 Paxos 在真实系统里"不够用"的地方在哪？
2. 引入工具：Quorum 的读写分离 + 外部配置 Master
3. 给出两个算法：Vertical Paxos I（先激活再搬状态）和 Vertical Paxos II（先搬状态再激活）
4. 推导特例：当参数取极端值时，Vertical Paxos 退化为 Primary-Backup

---

## 第一部分：论文要解决的核心问题

### 1.1 经典 Paxos 的假设太干净了

经典 Paxos（也叫 Synod 协议）解决的问题是：**一组固定的进程，如何对一个值达成一致**。

关键词是"固定"。经典 Paxos 的理论模型里，参与共识的 acceptor 集合是事先确定好的，不会在算法运行过程中发生变化。这在理论上很优雅，但在真实系统里几乎不可能：

- 机器会宕机，需要被替换
- 集群需要扩容或缩容
- 硬件维护时需要临时移除节点
- 负载变化时需要调整副本数量

这些操作都涉及一个共同的动作：**在共识算法仍在运行的过程中，更换参与共识的节点集合**——也就是 reconfiguration（重配置）。

### 1.2 真实系统的规模和重配置频率

论文指出，真实的大规模分布式系统通常有如下特征：

- 整个系统包含大量的 replica group（副本组），每组负责一小片数据
- 每个副本组内部用某种共识协议保持一致
- 副本组的成员会频繁变化（机器故障、迁移、维护等）
- 系统有大量空闲服务器可以随时顶上去

在这种环境下，"重配置"不是偶尔发生的异常事件，而是系统日常运营的核心操作。一个不能优雅处理重配置的共识协议，在工程上是残废的。

### 1.3 Primary-Backup 为什么在工程界流行

工程上最常见的副本协议不是 Paxos，而是 Primary-Backup（主从复制）：

- 一个 Primary 接收所有写操作
- Primary 把写操作同步给所有 Backup
- 所有 Backup 确认后，Primary 才回复客户端

这种协议的优点：简单、好实现、性能可预测。但它一直有一个理论上的"合法性危机"——没有人从 Paxos 这个理论框架出发严格证明过它的正确性。大家只是"觉得它对"，然后各自做各自的 ad-hoc 正确性论证。

### 1.4 论文的目标

作者的目标就是搭一座桥：

- 从 Paxos 的理论框架出发
- 通过引入"读写 quorum 分离"和"外部配置 master"两个创新
- 构造出 Vertical Paxos 这一类新算法
- 然后证明：Primary-Backup 是 Vertical Paxos 在特定参数下的退化形式

这样一来，Primary-Backup 的正确性就有了坚实的理论保证——它的安全性直接继承自 Paxos 家族的统一安全性框架。

---

## 第二部分：经典 Paxos 回顾与 Quorum 理论

### 2.1 经典 Paxos 的角色和流程

经典 Paxos 有三类角色：

- **Proposer**（提案者）：提出要达成共识的值
- **Acceptor**（接受者）：投票决定接受哪个值
- **Learner**（学习者）：得知最终被选中的值

共识过程分两阶段：

**Phase 1（Prepare 阶段）**：
- Proposer 选一个全局唯一的 ballot number（提案编号 b），向所有 Acceptor 发送 Prepare(b) 请求
- Acceptor 收到后，如果 b 比自己之前承诺过的 ballot 都大，就承诺"以后不再接受编号小于 b 的提案"，并返回自己之前接受过的最高编号提案的值（如果有的话）
- Proposer 收集到多数派 Acceptor 的回复后，Phase 1 完成

**Phase 2（Accept 阶段）**：
- Proposer 确定要提议的值 v（如果 Phase 1 收到了之前已接受的值，就用那个值；否则可以自由选择）
- 向 Acceptor 发送 Accept(b, v) 请求
- Acceptor 如果没有承诺过更高编号的 ballot，就接受这个提案
- 当多数派 Acceptor 接受了同一个 (b, v)，该值就被"选定"（chosen）了

### 2.2 Ballot 和 Configuration 的关系

在经典 Paxos 里，所有 ballot 使用同一组 Acceptor。也就是说，无论 ballot number 怎么变，"谁来投票"这件事是固定的。

作者将这种模式叫做 **Horizontal Paxos**——可以想象一个二维网格：

- 横轴是不同的 slot（状态机的不同命令位置）
- 纵轴是同一个 slot 内的不同 ballot

在 Horizontal Paxos 里，配置（Configuration）只会在横向（slot 之间）变化。同一个 slot 内部，无论经历多少轮 ballot，用的都是同一组 Acceptor。

### 2.3 Vertical Paxos 的核心创新："纵向"换配置

Vertical Paxos 打破了这个限制：**允许在同一个 slot 内部，不同的 ballot 使用不同的 Acceptor 集合。**

也就是说，配置变化可以发生在"纵向"——同一个共识实例内的不同 ballot 之间。这意味着：

- 你可以在一条命令还没达成共识的过程中，就切换了参与决策的节点
- 新节点可以在旧节点还没完全退出的情况下就加入

这就是"Vertical"这个名字的来源：配置变化方向从"水平的"变成了"垂直的"。

### 2.4 Read Quorum 和 Write Quorum 的分离

这是整篇论文最关键的理论创新之一。

在经典 Paxos 里，Phase 1 和 Phase 2 都使用"多数派"作为 quorum。但 Vertical Paxos 把 quorum 进一步细分为两类：

**b-Read Quorum（ballot b 的读 quorum）**：
- 用于 Phase 1
- 当一个新 Leader 上任（发起新的 ballot b）时，它需要从"上一个 ballot"的 Acceptor 那里读取历史状态
- b-Read Quorum 就是"新 Leader 要问多少个旧 Acceptor 才能确保读到完整历史"

**b-Write Quorum（ballot b 的写 quorum）**：
- 用于 Phase 2
- 当 Leader 在 ballot b 中提议一个值时，需要多少个 Acceptor 确认才算"写入成功"

这两者之间的关键约束是：

> **对于任何两个相邻的 ballot b 和 b+1，ballot b 的 Write Quorum 和 ballot b+1 的 Read Quorum 必须有交集。**

用大白话说：你之前写入时让多少人记住了，我之后读取时要问的那些人里必须包含至少一个记住了的。这样读到的人中至少有一个知道历史，就能把历史状态传递下去。

这个约束的数学表达：
$$\forall \text{ write quorum } W \text{ of ballot } b, \forall \text{ read quorum } R \text{ of ballot } b+1: W \cap R \neq \emptyset$$

### 2.5 Read-Write Quorum 分离的 Trade-off

这个分离带来了一个非常重要的设计旋钮：

- **Read Quorum 越小** → 新 Leader 上任时问的人越少，恢复越快
- 但相应地 → **Write Quorum 就必须越大**，因为要确保读到的人里一定有写过的
- **Write Quorum 越小** → 正常工作时写操作需要的确认越少，吞吐越高
- 但相应地 → **Read Quorum 就必须越大**，恢复时要问更多人

这两者互为代价，不可能同时最小化。在这个 trade-off 空间里，不同的参数选择对应不同的协议：

| Read Quorum | Write Quorum | 对应的协议特征 |
|-------------|--------------|----------------|
| 多数派 | 多数派 | 经典 Paxos / Multi-Paxos |
| 1（单个节点） | 全体 | Primary-Backup |
| 介于两者之间 | 介于两者之间 | 其他 Flexible Paxos 变种 |

### 2.6 Majority 是 Read-Write Quorum 的一个特例

论文明确指出：传统的"多数派"quorum 只是 Read-Write Quorum System 的一个特殊情况——当 Read Quorum = Write Quorum = 多数派时。

这意味着：
- "多数派"不是共识算法正确性的本质要求
- 本质要求只是"读写 quorum 交集不为空"
- 多数派只是满足这个要求的一种（最简单的）方式

这个认知非常重要。它意味着所有基于"多数派"的算法（经典 Paxos、Raft 等）都只是一个更广阔设计空间里的特定点，而不是唯一正确的选择。

---

## 第三部分：外部配置 Master

### 3.1 为什么需要一个外部角色

在经典 Paxos 里，如果要做重配置，通常的做法是"用状态机自己来决定配置变更"——把配置变更当作一条普通的命令，通过共识来决定。

这种做法在理论上没问题，但在工程上有两个痛点：

1. **配置变更和正常命令耦合在一起**，导致实现复杂
2. **配置变更的时机难以精确控制**——配置变更命令被放进哪个 slot，就在那个 slot 之后生效，但不同节点可能在不同时间执行到那个 slot

Vertical Paxos 的做法是：引入一个独立的外部角色——**Configuration Master**（配置主控）。

### 3.2 Configuration Master 的职责

Master 负责以下事情：

1. **决定每个 ballot 的配置**：ballot b 用哪些 Acceptor、谁是 Leader、Read Quorum 和 Write Quorum 分别是什么
2. **分配 ballot number**：确保不同的 Leader 拿到不同的 ballot number
3. **协调重配置时机**：决定什么时候可以激活新配置

Master **不参与**以下事情：

- 不参与正常的数据写入（Phase 2）
- 不存储用户数据
- 不转发客户端请求

### 3.3 Master 自身的可靠性

一个自然的问题：Master 自己挂了怎么办？

论文的回答很直接：**Master 自己也用复制状态机来实现**——比如用一个标准的 Paxos/Raft 集群来保证 Master 的高可用。

注意这里的递归结构：
- 数据面的共识用 Vertical Paxos
- Vertical Paxos 的配置由 Master 管理
- Master 本身用普通 Paxos/Raft 实现

这种"配置面和数据面分离"的设计在现代分布式系统里非常常见：
- TiKV 的 PD（Placement Driver）
- Kubernetes 的 etcd
- Google Spanner 的 Placement Service
- Azure 的 Fabric Controller

### 3.4 Master 与数据面的交互协议

Master 和数据面（Acceptor/Leader）之间通过以下消息交互：

1. **Master → Leader**：告知 Leader 它被分配了哪个 ballot、当前配置是什么、可以开始工作了
2. **Leader → Master**：报告状态迁移完成、请求激活新配置
3. **Master → All**：广播新的配置生效通知

这些交互的具体时序在 Vertical Paxos I 和 II 中有所不同，下面分别详细讲解。

---

## 第四部分：Vertical Paxos I——先激活再搬状态

### 4.1 核心思想

Vertical Paxos I 的核心设计哲学是：**新配置可以立即开始接受新请求，同时在后台从旧配置迁移状态。**

用一句话记忆：**先接活，再搬家。**

这意味着：
- 新配置被 Master 创建后，立刻变为 active（活跃状态）
- 新 Leader 可以马上开始处理新的 Phase 2 请求
- 但它同时需要在后台完成一项工作：从旧配置的 Acceptor 那里读取历史状态（Phase 1）

### 4.2 详细流程

**Step 1：Master 决定新配置**

当 Master 检测到需要重配置（比如某个 Acceptor 宕机了），它：
- 选择一个新的 ballot number b（比之前所有 ballot 都大）
- 确定新的 Acceptor 集合、新的 Leader、新的 Read/Write Quorum
- 将这个配置"激活"——通知新 Leader："你现在是 ballot b 的 Leader，可以开始工作了"

**Step 2：新 Leader 开始 Phase 1（读取旧状态）**

新 Leader 被激活后，做两件并行的事：

（a）**向旧配置的 Acceptor 发起 Phase 1 请求**（读历史）
- 发送 Prepare(b) 给旧配置中 Read Quorum 数量的 Acceptor
- 等待它们回复之前接受过的最高编号提案

这就是所谓的 **State Transfer（状态迁移）**。

（b）**同时开始接受新的客户端请求**（Phase 2）
- 因为新配置已经是 active 的，新 Leader 可以立即处理新请求
- 新请求使用 ballot b 和新配置的 Write Quorum

**Step 3：State Transfer 完成**

当 Read Quorum 数量的旧 Acceptor 回复了 Phase 1 响应后：
- 新 Leader 知道了所有可能已被选定但还没 learn 的值
- 它需要先把这些旧值用新配置重新提交（re-propose），确保它们被选定
- 之后通知 Master："状态迁移完成，旧配置可以释放了"

**Step 4：Master 收缩历史依赖**

Master 收到通知后：
- 记录"ballot b 的状态迁移已完成"
- 之后如果再有新的 ballot b+1 的 Leader 上任，它只需要从 ballot b 的 Acceptor 读状态，不需要追溯到更早的配置

### 4.3 允许多个 Active 配置并存

Vertical Paxos I 的一个关键特征是：**系统中可以同时存在多个 active 配置。**

比如：
- ballot b 的配置已经 active，Leader 正在处理请求
- 同时 ballot b+1 的配置也被激活了（因为 Master 检测到需要再次重配置）
- ballot b+1 的 Leader 正在从 ballot b 的 Acceptor 那里做 State Transfer

这种并行性是 VP-I 的优势——不需要等旧配置的状态全部搬完就能激活新配置，系统不会因为 State Transfer 而停服。

### 4.4 VP-I 的代价

这种并行性的代价是：**历史依赖链可能变长。**

假设连续发生多次重配置：
- ballot b 激活，开始 State Transfer from b-1
- 还没完成，ballot b+1 又激活了
- ballot b+1 的 Leader 需要从 ballot b 的 Acceptor 读状态
- 但 ballot b 自己还没完成从 b-1 的 State Transfer

最坏情况下，如果连续多个 Leader 都在 State Transfer 完成前宕机，后面的新 Leader 需要追溯一整串历史配置。依赖链越长，恢复越慢。

### 4.5 正确性保证

VP-I 的安全性依赖以下不变量：

1. **Quorum 交集保证**：ballot b 的 Write Quorum 和 ballot b+1 的 Read Quorum 必须有交集
2. **只有 Active 配置才能写**：一个配置必须被 Master 显式激活后，其 Leader 才能执行 Phase 2
3. **Phase 1 必须覆盖所有可能已被写入的值**：新 Leader 的 Read Quorum 必须与所有之前可能接受过值的配置的 Write Quorum 有交集

这些不变量加在一起，保证了经典 Paxos 的安全性在配置变更过程中仍然成立：**一旦一个值被选定（多数派/Write Quorum 接受），它就不会被推翻。**

---

## 第五部分：Vertical Paxos II——先搬状态再激活

### 5.1 核心思想

Vertical Paxos II 的设计哲学与 VP-I 相反：**新配置必须先完成状态迁移，才能被激活接受新请求。**

用一句话记忆：**先搬家，再接活。**

### 5.2 详细流程

**Step 1：Master 决定新配置（但不立即激活）**

Master 检测到需要重配置后：
- 选择新的 ballot number b、新的 Acceptor 集合、新的 Leader
- 但**不立即激活**新配置
- 而是通知新 Leader："你被分配了 ballot b，先去做 State Transfer"

**Step 2：新 Leader 进行 State Transfer**

新 Leader 向旧配置（当前唯一 active 的配置）的 Acceptor 发起 Phase 1：
- 读取所有可能已被接受但还没选定的值
- 等待 Read Quorum 数量的响应
- 完成所有未决值的 re-proposal（如果有的话）

在这个阶段：
- 新配置**不接受任何新请求**
- 旧配置仍然是 active 的，仍然在处理客户端请求
- 系统对外表现为"正常工作中"

**Step 3：State Transfer 完成，请求激活**

新 Leader 完成 State Transfer 后：
- 通知 Master："我已经拿到了所有历史状态，可以激活了"
- Master 验证后，正式激活新配置
- 同时**废止旧配置**（旧配置不再 active）

**Step 4：新配置开始正常工作**

从 Master 激活的那一刻起：
- 新 Leader 开始处理客户端请求
- 使用新配置的 Write Quorum
- 旧配置的 Acceptor 可以被释放或回收

### 5.3 系统中只有一个 Active 配置

VP-II 的关键特征是：**任何时刻，系统中最多只有一个 active 配置。**

这带来了一个巨大的简化：
- 新 Leader 做 Phase 1 时，只需要跟"当前唯一 active 的配置"打交道
- 不需要追溯一长串历史配置
- 不会出现 VP-I 那种"依赖链越来越长"的问题

### 5.4 VP-II 的代价

代价也很明显：**如果 State Transfer 很慢（比如要搬大量数据），系统在这段时间内的副本冗余可能降低。**

具体来说：
- 旧配置可能已经有节点宕机了（这才触发了重配置）
- 新配置的 State Transfer 还没完成
- 在这个窗口期，系统的实际冗余度比正常情况低

如果这时候又有节点宕机，可能导致服务中断。

另一个代价是：**如果新 Leader 在 State Transfer 过程中宕机**，一切要从头来。下一个新 Leader 又要重新做 State Transfer。如果连续多个 Leader 在 State Transfer 阶段宕机，系统会经历一段较长的不可用期。

### 5.5 VP-I vs VP-II 对比

| 维度 | Vertical Paxos I | Vertical Paxos II |
|------|------------------|-------------------|
| 新配置何时激活 | 立即激活 | State Transfer 完成后才激活 |
| Active 配置数量 | 可以多个并存 | 始终只有一个 |
| State Transfer 期间能否服务 | 能（新配置直接处理请求） | 能（旧配置继续处理） |
| 历史依赖链长度 | 可能很长（连续失败时） | 固定为 1（只看上一个 active 配置） |
| 恢复复杂度 | 高（可能需追溯多个配置） | 低（只需查一个配置） |
| 适用场景 | State Transfer 快 或 不想停服 | State Transfer 慢 或 想要简单清晰 |
| 工程类比 | Raft Joint Consensus | Raft Single-Node Change + Learner |

### 5.6 论文推荐

论文作者指出：**大多数工程系统中的 Primary-Backup 协议，实际上对应 Vertical Paxos II。** 因为在实际系统中，State Transfer 通常涉及大量数据拷贝（比如同步几百 GB 的数据），在这么长的时间内保持多个 active 配置并发运行的复杂性太高，不如等搬完再切。

---

## 第六部分：Primary-Backup 作为 Vertical Paxos 的特例

### 6.1 关键推导

这是整篇论文最精彩的部分。作者通过一步步调整 quorum 参数，展示 Vertical Paxos 是如何退化为 Primary-Backup 的。

**第一步：把 Write Quorum 设为全体 Acceptor**

如果每次 Phase 2 都要求**所有** Acceptor 确认（而不是多数派），那意味着：
- 写入时，所有副本都会收到数据
- 任何一个副本宕机都会导致写入失败（需要重配置才能继续）

**第二步：把 Read Quorum 设为任意单个 Acceptor**

当 Write Quorum = 全体时，Read Quorum 可以压缩到最小——**任意一个 Acceptor 就能构成 Read Quorum**。

为什么？因为 Write Quorum 是全体，意味着所有 Acceptor 都参与了之前的每次写入。所以随便问一个人，它就知道完整的历史。Quorum 交集约束自然满足：全体 ∩ 任何一个节点 ≠ 空集。

**第三步：让 Leader 自己也是 Acceptor**

如果 Leader 本身就是 Acceptor 集合中的一员，那：
- 新 Leader 做 Phase 1 时，"读一个 Acceptor"可以就是读自己
- 因为它自己参与了之前所有的写入（Write Quorum = 全体，所以它一定收到了所有数据）
- Phase 1 变成了一个本地操作，不需要网络通信！

**第四步：重配置时新 Leader 从当前 Acceptor 中选**

如果进一步要求：新配置的 Leader 必须是旧配置中某个存活的 Acceptor，那：
- 这个新 Leader 自己就拥有完整的历史状态
- State Transfer 变成了"新 Leader 读自己的本地数据"
- 几乎零开销

### 6.2 退化后的协议长什么样

把上面四步的结果组合起来，你得到的协议是：

1. **正常工作时**：Leader（Primary）接收客户端写请求，把数据同步给所有其他 Acceptor（Backup），等全部确认后回复客户端。
2. **某个 Backup 宕机时**：系统无法继续写入。Master 将宕机的 Backup 从配置中移除，系统恢复。
3. **Primary 宕机时**：Master 从存活的 Backup 中选一个作为新 Primary。新 Primary 因为之前参与了所有写入，自身数据就是完整的，直接开始工作。

这不就是 **Primary-Backup 协议**吗？一模一样！

### 6.3 和 Raft 的对应关系

把这套推导映射到 Raft 上：

| Vertical Paxos 概念 | Raft 对应 |
|---------------------|-----------|
| Leader | Leader |
| Acceptor（Write Quorum 中的成员） | 所有 Follower + Leader 自身 |
| Write Quorum = 多数派（Raft 的实际选择） | 多数派确认才算 commit |
| Read Quorum = 1（Leader 自己） | 选举约束保证 Leader 拥有所有已提交日志 |
| Phase 1 = 读自己 | Leader 上任后不需要额外"学习"历史 |
| Phase 2 = 广播给 Acceptor | AppendEntries 日志复制 |
| Master | 隐式（Leader 自己管配置）/ 显式（Multi-Raft 的 PD） |
| State Transfer | 日志追赶 / Snapshot |
| VP-I（先激活再搬状态） | Joint Consensus |
| VP-II（先搬状态再激活） | Single-Node Change / Learner→Voter |

---

## 十二、这篇论文真正教会你的思维方式

### 1. 不要把协议当"固定脚本"来记

很多人学 Paxos 或 Raft 的时候，脑子里就是一套固定流程：Phase 1 干什么、Phase 2 干什么、收到消息怎么回复。这种学法能通过考试，但碰到变种就懵了。

Vertical Paxos 教你的是：**协议是一个参数空间里的点，不是一个固定脚本。** 你调 quorum 大小、调激活时机、调 master 角色，就能在同一个安全性框架里长出完全不同行为的协议。Raft、Multi-Paxos、Primary-Backup、Cheap Paxos——都是这个空间里的不同坐标。

### 2. 安全性和活性分开思考

论文非常明确地把安全性（Safety）和活性（Liveness）分开：

- **安全性**：quorum 交集保证了不会出现两个互相矛盾的值被同时接受。这是数学性质，不依赖任何时序假设。
- **活性**：需要最终有 Leader 能联系到足够多的节点推进协议。这依赖异步系统中的最终可达性假设。

分开思考的好处是：你可以在不影响安全性的前提下，自由调整活性相关的策略（比如 Leader 选举方式、超时策略、心跳频率）。

### 3. 重配置不是"额外功能"，是系统生存的基础

如果你设计的共识系统不支持重配置，那它就是一个"一次性用品"——任何一个节点永久宕机都会让整个系统陷入降级甚至不可用。Vertical Paxos 把重配置拉到和正常共识同等重要的地位，这个视角对任何要在生产环境长期运行的系统都至关重要。

### 4. 理论和工程不是两个世界

这篇论文最大的贡献可能不是任何一个具体算法，而是一种态度：**工程里看似"民间智慧"的做法（比如 primary-backup），其实可以被纳入严格的理论框架来分析和证明。** 这意味着：

- 你可以用理论工具来验证工程设计的正确性
- 你也可以用理论框架来发现工程设计中尚未被探索的改进空间

这正是 Lamport 一辈子在做的事情：让分布式系统从"经验驱动"走向"理论驱动"。

---

## 十三、延伸阅读建议

如果你读完这篇精讲想继续深入，建议按以下顺序：

1. **Paxos Made Simple**（Lamport, 2001）——先确保经典 Paxos 的基础牢固
2. **Raft 论文**（Ongaro & Ousterhout, 2014）——作为 Vertical Paxos 特例的具体实现
3. **Flexible Paxos**（Howard, Malkhi & Spiegelman, 2016）——进一步推广 quorum 交集理论
4. **UPaxos**（Turner, 2016）——Vertical Paxos 的进一步扩展，支持无限流水线重配置
5. **TiKV 源码中的 Raft 实现**——看理论如何落地为工程代码

---

## 十四、最后的总结

《Vertical Paxos and Primary-Backup Replication》这篇论文的核心贡献可以用三句话概括：

**第一句**：它证明了"重配置"可以安全地发生在单个共识实例的内部（纵向变化），不需要等到实例之间才能切配置。

**第二句**：它通过读/写 quorum 分离和外部 configuration master，给出了两种具体的重配置算法（VP-I 和 VP-II），分别适合"追求并行度"和"追求边界清晰"的场景。

**第三句**：它证明了 primary-backup 这种工程界最常见的复制协议，是 Vertical Paxos 在极端 quorum 配置下的数学特例——从而把理论世界和工程世界缝合在了一起。

如果说经典 Paxos 让你知道"分布式共识是可能的"，那 Vertical Paxos 让你知道"分布式共识在真实系统里怎么活下去"。而你学的 Raft，就是这种"活下去的方式"中一个极其优雅的具体实例。