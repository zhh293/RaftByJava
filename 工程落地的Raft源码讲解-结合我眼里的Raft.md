# 工程落地的 Raft 源码讲解 —— 结合《我眼里的 Raft》

> 本文以蚂蚁金服开源的 **SOFAJRaft**（Java 生产级 Raft 实现）为参照物，逐一对应《我眼里的 Raft》中的每个理论点，深入到源码层面讲解工程实现中的完整闭环。理论告诉你"是什么"和"为什么"，工程告诉你"怎么做"以及"还有哪些你没想到的"。

---

## 一、项目架构全景

### 1.1 模块结构

```
sofa-jraft/
├── jraft-core/          ← Raft 算法核心实现（本文重点）
├── jraft-extension/     ← 可插拔扩展（gRPC 通信、BDB 日志存储等）
├── jraft-rheakv/        ← 基于 JRaft 构建的分布式 KV 存储（工程示范）
├── jraft-example/       ← 使用示例（Counter、Election 等）
└── jraft-test/          ← 集成测试
```

### 1.2 分层架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        应用层（Application Layer）                     │
│                                                                     │
│   StateMachine（用户实现）    CliService（集群管理）    RouteTable（路由） │
└───────────────────────────────────┬─────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────┐
│                      核心协议层（Core Protocol Layer）                  │
│                                                                     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐ │
│  │  NodeImpl   │  │  Replicator  │  │ BallotBox  │  │ FSMCaller │ │
│  │ (状态机核心) │  │ (日志复制器)  │  │ (投票箱)    │  │(状态机驱动)│ │
│  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘  └─────┬─────┘ │
│         │                │                 │               │       │
│  ┌──────▼──────┐  ┌──────▼───────┐        │        ┌──────▼─────┐ │
│  │ReadOnlyService│ │ReplicatorGroup│       │        │  Iterator  │ │
│  │(线性一致性读) │  │ (复制器管理)  │        │        │ (日志迭代) │ │
│  └─────────────┘  └──────────────┘        │        └────────────┘ │
└───────────────────────────────────┬───────┬┘────────────────────────┘
                                    │       │
┌───────────────────────────────────▼───────▼─────────────────────────┐
│                       存储层（Storage Layer）                          │
│                                                                     │
│  ┌──────────────┐  ┌────────────────┐  ┌──────────────────────────┐│
│  │ LogManager   │  │RaftMetaStorage │  │   SnapshotExecutor       ││
│  │ (日志管理器)  │  │(term/vote持久化)│  │   (快照管理器)            ││
│  └──────┬───────┘  └────────────────┘  └──────────────────────────┘│
│         │                                                           │
│  ┌──────▼───────┐                                                   │
│  │ LogStorage   │  ← RocksDB / SegmentLog / BDB（可插拔实现）        │
│  └──────────────┘                                                   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────┐
│                       网络层（Network Layer）                          │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────┐│
│  │  RpcServer   │  │  RpcClient   │  │  Protobuf Serialization    ││
│  │ (Bolt/gRPC)  │  │ (Bolt/gRPC)  │  │  (raft.proto / rpc.proto) ││
│  └──────────────┘  └──────────────┘  └────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────┐
│                      基础设施层（Infrastructure）                       │
│                                                                     │
│  RepeatedTimer    Disruptor     HashedWheelTimer    ThreadPool      │
│  (定时器)         (无锁队列)    (时间轮)             (线程池管理)      │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.3 核心类职责一览

| 类 | 对应你笔记中的概念 | 职责 |
|---|---|---|
| `NodeImpl` | 节点状态机（Leader/Follower/Candidate） | 整个 Raft 节点的大脑，处理选举、日志提交、读请求 |
| `Replicator` | 日志同步（AppendEntries RPC） | Leader 向单个 Follower 发送日志的执行器 |
| `BallotBox` | 多数派确认 + commitIndex 推进 | 统计每条日志被多少节点确认，达到多数派后提交 |
| `FSMCallerImpl` | 状态机应用（appliedIndex） | 将已提交的日志按序应用到用户状态机 |
| `LogManagerImpl` | WAL 预写日志 | 管理日志的追加、读取、截断，内存缓存 + 异步刷盘 |
| `ReadOnlyServiceImpl` | 线性一致性读（ReadIndex） | 实现 ReadIndex 协议，确保读到最新已提交数据 |
| `RaftMetaStorage` | term、votedFor 持久化 | 保证重启后不违反"一个 term 只投一票" |
| `RepeatedTimer` | 选举超时、心跳定时器 | 可重复触发的定时器，选举超时带随机抖动 |
| `Configuration` | 集群配置（成员变更） | 描述集群中有哪些节点，支持 Joint Consensus |

---

## 二、节点状态机 —— 工程中的三种角色

### 2.1 你笔记中的理论

> "所有节点刚启动的时候都是 Follower……某个节点的选举定时器先到时间，它就准备竞选了。"

### 2.2 工程实现：State 枚举

```java
// com.alipay.sofa.jraft.core.State
public enum State {
    STATE_LEADER,         // Leader
    STATE_TRANSFERRING,   // 正在转让 Leadership（你笔记未涉及的工程概念）
    STATE_CANDIDATE,      // Candidate
    STATE_FOLLOWER,       // Follower
    STATE_ERROR,          // 节点出错（磁盘故障等）
    STATE_UNINITIALIZED,  // 未初始化
    STATE_SHUTTING,       // 正在关闭
    STATE_SHUTDOWN,       // 已关闭
    STATE_END;            // 结束标记

    public boolean isActive() {
        return this.ordinal() < STATE_ERROR.ordinal();
    }
}
```

**工程中比理论多出来的状态**：

- **STATE_TRANSFERRING**：Leader 主动让位给指定节点（Leadership Transfer），在让位过程中 Leader 暂停接受新的写入，等目标节点日志追上后发送 `TimeoutNowRequest` 让它立即发起选举。
- **STATE_ERROR**：节点遇到不可恢复错误（如磁盘 IO 错误），此时节点不再参与任何 Raft 操作。
- **isActive()** 利用枚举声明顺序做活跃判断——只有 LEADER、TRANSFERRING、CANDIDATE、FOLLOWER 是活跃状态。

### 2.3 节点初始化

节点 `init()` 完成后，最后一步会调用：

```java
stepDown(this.currTerm, false, new Status());
```

这不是"退位"，而是**通过 stepDown 来启动选举定时器**。因为 stepDown 的逻辑里包含了"如果不是 Learner，就启动 electionTimer"这一步。整个系统从 stepDown 开始运转。

如果是**单节点集群**，初始化完成后直接跳过选举：

```java
if (this.conf.isStable() && this.conf.getConf().size() == 1 
    && this.conf.getConf().contains(this.serverId)) {
    electSelf();  // 自己投自己一票，多数派达成，直接成为 Leader
}
```

---

## 三、选举的完整工程实现

### 3.1 选举超时定时器 —— 随机化的工程细节

你笔记中提到：

> "超时时间在一个区间内随机取值（比如 300ms 到 500ms 之间）。随机是关键。"

工程中的具体实现：

```java
// NodeImpl.init() 中创建选举定时器
this.electionTimer = new RepeatedTimer("JRaft-ElectionTimer", 
    this.options.getElectionTimeoutMs(), ...) {
    @Override
    protected int adjustTimeout(final int timeoutMs) {
        // 每次调度时重新随机化超时时间
        return randomTimeout(timeoutMs);
    }
};

private int randomTimeout(final int timeoutMs) {
    // 范围：[electionTimeoutMs, electionTimeoutMs + maxElectionDelayMs)
    // 默认：[1000ms, 2000ms)
    return ThreadLocalRandom.current().nextInt(timeoutMs, 
        timeoutMs + this.raftOptions.getMaxElectionDelayMs());
}
```

**默认参数**：
- `electionTimeoutMs` = 1000ms
- `maxElectionDelayMs` = 1000ms
- 心跳间隔 = `electionTimeoutMs / 10` = 100ms

**设计要点**：每次定时器触发后重新调度时都会重新随机，而不是初始化时随机一次。这保证了即使两个节点某次恰好超时时间相同导致选票瓜分，下一轮它们大概率不同。

### 3.2 handleElectionTimeout —— 选举超时触发

```java
private void handleElectionTimeout() {
    // 1. 只有 Follower 才响应选举超时
    if (this.state != State.STATE_FOLLOWER) return;
    
    // 2. 如果 Leader 的"租约"还没过期，不发起选举
    //   （防止网络抖动导致不必要的选举）
    if (isCurrentLeaderValid()) return;
    
    this.writeLock.lock();
    try {
        // 3. 双重检查（加锁后再确认一次）
        if (this.state != State.STATE_FOLLOWER) return;
        if (isCurrentLeaderValid()) return;
        
        // 4. 清空 leaderId
        resetLeaderId(PeerId.emptyPeer(), ...);
        
        // 5. 优先级选举检查（你笔记未涉及）
        if (!allowLaunchElection()) return;
        
        // 6. 发起 Pre-Vote
        preVote();
    } finally { ... }
}
```

**`isCurrentLeaderValid()`** 的逻辑：

```java
return Utils.monotonicMs() - this.lastLeaderTimestamp < this.options.getElectionTimeoutMs();
```

即：最近一次收到 Leader 心跳的时间距现在是否小于选举超时时间。这是一个**额外的安全检查**——即使选举定时器超时了（可能是系统 GC 导致定时器延迟触发），只要最近确实收到过心跳，就不发起选举。

**`allowLaunchElection()`** —— 优先级选举（你笔记未覆盖的工程特性）：

```java
// priority == 0：永远不参与选举（纯 Follower）
// priority == -1：禁用优先级选举
// priority > 0：只有优先级达到 targetPriority 的节点才能发起选举
```

当高优先级节点宕机时，低优先级节点不会立即发起选举，而是等待一段时间。如果连续多次超时仍无人当选，`targetPriority` 逐步衰减，最终允许低优先级节点参与——**这是一种优雅降级策略**。

### 3.3 Pre-Vote —— 防止无意义选举

你笔记中的理论：

> "Pre-Vote 就是解决这个问题的。节点不直接发起真正的选举，而是先'试探性地问一圈'……它发出的 Pre-Vote 请求根本得不到多数派响应。"

工程实现：

```java
private void preVote() {
    long oldTerm;
    try {
        // 正在安装快照时不发起选举
        if (snapshotExecutor != null && snapshotExecutor.isInstallingSnapshot()) return;
        // 自己不在当前配置中（可能已被移除），不发起选举
        if (!this.conf.contains(this.serverId)) return;
        
        oldTerm = this.currTerm;
    } finally {
        this.writeLock.unlock();  // ★ 释放锁以执行可能耗时的磁盘操作
    }
    
    // 获取最后一条日志的 ID（可能涉及磁盘读取）
    final LogId lastLogId = this.logManager.getLastLogId(true);
    
    this.writeLock.lock();
    try {
        // ★★★ ABA 防御：释放锁期间 term 可能被改变
        if (oldTerm != this.currTerm) return;
        
        // 初始化 Pre-Vote 计票器
        this.prevVoteCtx.init(this.conf.getConf(), 
            this.conf.isStable() ? null : this.conf.getOldConf());
        
        // 向所有 peers 发送 PreVote RPC
        for (final PeerId peer : this.conf.listPeers()) {
            if (peer.equals(this.serverId)) continue;
            
            RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setPreVote(true)                    // ★ 标记为 Pre-Vote
                .setTerm(this.currTerm + 1)          // ★ 用 currTerm+1，但不真正递增自身 term
                .setLastLogIndex(lastLogId.getIndex())
                .setLastLogTerm(lastLogId.getTerm())
                .build();
            
            this.rpcService.preVote(peer.getEndpoint(), request, done);
        }
        
        // 给自己投 Pre-Vote
        this.prevVoteCtx.grant(this.serverId);
        
        // 如果是单节点，Pre-Vote 直接通过
        if (this.prevVoteCtx.isGranted()) {
            electSelf();
        }
    } finally { ... }
}
```

**关键工程细节**：

1. **Pre-Vote 不递增 term**：发送的 term 是 `currTerm + 1`，但自身的 `currTerm` 不变。这样即使 Pre-Vote 失败，也不会影响集群的 term 单调性。

2. **ABA 防御模式**：释放锁 → 执行 IO → 重新加锁 → 检查 term 是否变了。这个模式在 JRaft 中反复出现，因为持有写锁时不能执行磁盘操作（会阻塞整个节点），但释放锁后可能有其他线程改变了状态。

3. **快照和配置检查**：正在安装快照的节点不应该发起选举（它正在追赶数据），不在配置中的节点也不应该发起选举（可能已被集群移除）。

### 3.4 收到 Pre-Vote 请求的处理

```java
public Message handlePreVoteRequest(final RequestVoteRequest request) {
    this.writeLock.lock();
    try {
        boolean granted = false;
        do {
            // ① 候选人必须在当前配置中
            if (!this.conf.contains(candidateId)) break;
            
            // ② ★★★ 如果当前 Leader 仍有效（lease 未过期），拒绝 Pre-Vote
            if (this.leaderId != null && !this.leaderId.isEmpty() 
                && isCurrentLeaderValid()) break;
            
            // ③ 请求 term < 当前 term，拒绝
            if (request.getTerm() < this.currTerm) break;
            
            // ④ 日志完整性检查
            final LogId lastLogId = this.logManager.getLastLogId(true);
            granted = new LogId(request.getLastLogIndex(), request.getLastLogTerm())
                .compareTo(lastLogId) >= 0;
        } while (false);
        
        return RequestVoteResponse.newBuilder()
            .setTerm(this.currTerm)
            .setGranted(granted)
            .build();
    } finally { ... }
}
```

**你笔记没提到的关键点 —— 第②步**：如果 Follower 认为当前 Leader 还活着（最近收到过心跳），它会拒绝 Pre-Vote。这进一步防止了网络抖动导致的不必要选举——即使某个节点超时了想竞选，其他 Follower 如果最近还收到了 Leader 心跳，就不会给它投 Pre-Vote。

### 3.5 正式选举（electSelf）

你笔记中的理论：

> "1. term++ 2. 给自己投一票 3. 角色切换为 Candidate 4. 向所有其他节点广播投票请求"

工程实现：

```java
private void electSelf() {
    try {
        // 状态转换核心
        if (this.state == State.STATE_FOLLOWER) {
            this.electionTimer.stop();            // 停止选举定时器
        }
        resetLeaderId(PeerId.emptyPeer(), ...);
        this.state = State.STATE_CANDIDATE;       // ★ 转为 Candidate
        this.currTerm++;                          // ★ 递增 term
        this.votedId = this.serverId.copy();      // ★ 投票给自己
        this.voteTimer.start();                   // 启动竞选超时定时器
        
        this.voteCtx.init(this.conf.getConf(), 
            this.conf.isStable() ? null : this.conf.getOldConf());
        oldTerm = this.currTerm;
    } finally {
        this.writeLock.unlock();                  // 释放锁以获取 lastLogId
    }
    
    final LogId lastLogId = this.logManager.getLastLogId(true);
    
    this.writeLock.lock();
    try {
        if (oldTerm != this.currTerm) return;     // ABA 防御
        
        // ★★★ 先持久化 (term, votedFor)，再发送 RPC
        if (!this.metaStorage.setTermAndVotedFor(this.currTerm, this.serverId)) {
            stepDown(this.currTerm, false, new Status(RaftError.EIO, ...));
            return;
        }
        
        // 向所有 peers 发送 RequestVote
        for (final PeerId peer : this.conf.listPeers()) {
            if (peer.equals(this.serverId)) continue;
            RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setPreVote(false)
                .setTerm(this.currTerm)
                .setLastLogIndex(lastLogId.getIndex())
                .setLastLogTerm(lastLogId.getTerm())
                .build();
            this.rpcService.requestVote(peer.getEndpoint(), request, done);
        }
        
        // 给自己计票
        this.voteCtx.grant(this.serverId);
        if (this.voteCtx.isGranted()) {
            becomeLeader();  // 单节点直接成为 Leader
        }
    } finally {
        this.writeLock.unlock();
    }
}
```

**工程中的关键安全性保证 —— 先持久化再发 RPC**：

Raft 要求在发送 RequestVote **之前**必须将 `(term, votedFor)` 持久化到磁盘。为什么？如果顺序反过来：发了 RPC → 还没持久化 → 节点崩溃 → 重启后 votedFor 为空 → 可能对同一个 term 投第二票 → 违反了"一个 term 只投一票"的安全性保证 → 可能出现两个 Leader。

这个约束在你的笔记中只是轻描淡写地提到"持久化到磁盘，防止重启后重复投票"，但工程中必须严格保证操作顺序。

### 3.6 投票判断逻辑（handleRequestVoteRequest）

你笔记中的理论：

> "第一步：比 term。第二步：查投票记录。第三步：比日志新旧。"

工程中的完整实现：

```java
public Message handleRequestVoteRequest(final RequestVoteRequest request) {
    this.writeLock.lock();
    try {
        do {
            // ★ 第一步：比 term
            if (request.getTerm() >= this.currTerm) {
                if (request.getTerm() > this.currTerm) {
                    // 对方 term 更高 → 无条件 stepDown
                    // stepDown 会：清空 votedId、更新 currTerm、退为 Follower
                    stepDown(request.getTerm(), false, ...);
                }
            } else {
                // 请求 term < 当前 term → 直接忽略
                break;
            }
            
            // ★ 第二步 + 第三步合并判断
            // 释放锁获取 lastLogId
            this.writeLock.unlock();
            final LogId lastLogId = this.logManager.getLastLogId(true);
            this.writeLock.lock();
            if (request.getTerm() != this.currTerm) break;  // ABA check
            
            // 日志完整性检查
            final boolean logIsOk = new LogId(
                request.getLastLogIndex(), request.getLastLogTerm()
            ).compareTo(lastLogId) >= 0;
            
            // ★ 投票条件：日志足够新 且 尚未投票给其他人
            if (logIsOk && (this.votedId == null || this.votedId.isEmpty())) {
                stepDown(request.getTerm(), false, ...);  // 重置选举定时器
                this.metaStorage.setVotedFor(candidateId);  // 持久化投票记录
                this.votedId = candidateId.copy();
            }
        } while (false);
        
        // 构造响应
        return RequestVoteResponse.newBuilder()
            .setTerm(this.currTerm)
            .setGranted(request.getTerm() == this.currTerm 
                && candidateId.equals(this.votedId))
            .build();
    } finally { ... }
}
```

**LogId.compareTo 的实现——对应你笔记中"先比 lastLogTerm 再比 lastLogIndex"**：

```java
public int compareTo(final LogId o) {
    // 先比 term
    final int c = Long.compare(getTerm(), o.getTerm());
    if (c == 0) {
        // term 相同则比 index
        return Long.compare(getIndex(), o.getIndex());
    } else {
        return c;
    }
}
```

完美对应你笔记中的描述："先比最后一条日志的 lastLogTerm……如果 lastLogTerm 相同，再比最后一条日志的 lastLogIndex。"

### 3.7 投票响应处理

```java
public void handleRequestVoteResponse(final PeerId peerId, final long term, 
                                       final RequestVoteResponse response) {
    this.writeLock.lock();
    try {
        // 必须仍是 CANDIDATE
        if (this.state != State.STATE_CANDIDATE) return;
        // 发送时的 term 必须等于当前 term（防止过期响应）
        if (term != this.currTerm) return;
        // 对方 term 更高 → stepDown
        if (response.getTerm() > this.currTerm) {
            stepDown(response.getTerm(), false, ...);
            return;
        }
        // 对方同意 → 计票
        if (response.getGranted()) {
            this.voteCtx.grant(peerId);
            if (this.voteCtx.isGranted()) {
                becomeLeader();
            }
        }
    } finally { ... }
}
```

你笔记中提到：

> "赞同票达到多数派：竞选成功。拒绝票达到多数派：直接退回 Follower。竞选超时：退回 Follower。"

工程中的微妙差异：代码只检查"赞同票达到多数派"就 becomeLeader，**并没有显式检查"拒绝票达到多数派"**。因为在实践中，拒绝票不需要单独统计——如果拿不到多数派赞同票，最终竞选定时器会超时，同样会退回 Follower。这简化了实现，减少了状态跟踪。

### 3.8 竞选超时处理

```java
private void handleVoteTimeout() {
    this.writeLock.lock();
    if (this.state != State.STATE_CANDIDATE) return;
    
    if (this.raftOptions.isStepDownWhenVoteTimedout()) {
        // 默认策略：退回 Follower → 重新 Pre-Vote
        stepDown(this.currTerm, false, ...);
        preVote();
    } else {
        // 备选策略：直接重新 electSelf（会递增 term）
        electSelf();
    }
}
```

**工程选择**：默认 `stepDownWhenVoteTimedout = true`，即竞选超时后先退回 Follower 再走 Pre-Vote 流程。这比直接 electSelf 更保守——避免在网络不稳定时 term 涨得太快。

---

## 四、Leader 上任 —— becomeLeader 的完整闭环

### 4.1 你笔记中的理论

> "新 Leader 上任后会立即追加一条空日志（no-op）并广播给所有 Follower……让新任期有东西可以提交。"

### 4.2 工程实现

```java
private void becomeLeader() {
    Requires.requireTrue(this.state == State.STATE_CANDIDATE);
    
    // 1. 停止投票定时器
    stopVoteTimer();
    
    // 2. 状态切换
    this.state = State.STATE_LEADER;
    this.leaderId = this.serverId.copy();
    
    // 3. 重置 ReplicatorGroup 的 term
    this.replicatorGroup.resetTerm(this.currTerm);
    
    // 4. ★★★ 为每个 Follower 创建并启动 Replicator
    for (final PeerId peer : this.conf.listPeers()) {
        if (peer.equals(this.serverId)) continue;
        this.replicatorGroup.addReplicator(peer);
    }
    // 为每个 Learner 也创建 Replicator
    for (final PeerId peer : this.conf.listLearners()) {
        this.replicatorGroup.addReplicator(peer, ReplicatorType.Learner);
    }
    
    // 5. ★★★ 初始化 BallotBox 的 pendingIndex
    //    从 lastLogIndex + 1 开始跟踪（即只跟踪新任期的日志）
    this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
    
    // 6. ★★★ 提交一条配置日志（等价于 no-op 的功能）
    this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());
    
    // 7. 启动 stepDown 定时器（周期性检查多数派是否存活）
    this.stepDownTimer.start();
}
```

**工程中 no-op 的变体**：SOFAJRaft 没有写入标准的 `ENTRY_TYPE_NO_OP`，而是写入一条 `ENTRY_TYPE_CONFIGURATION` 日志。效果完全等价——都是一条当前任期的日志，用于推进 commitIndex。选择配置日志而非空日志的好处是，可以同时确认当前集群配置，一举两得。

**Replicator 的初始化**：每个 Replicator 创建时，`nextIndex` 被初始化为 Leader 当前 `lastLogIndex + 1`——这对应你笔记中"把 nextIndex 初始化为尾后位置"的乐观假设。

### 4.3 Replicator 启动后的第一个动作

```java
// Replicator.start()
public static ThreadId start(final ReplicatorOptions opts, final RaftOptions raftOptions) {
    final Replicator r = new Replicator(opts, raftOptions);
    // ...
    r.startHeartbeatTimer(Utils.nowMs());
    r.sendProbeRequest();  // ★ 首先发送一个探测请求
    return r.id;
}
```

启动后第一件事是发送 **Probe 请求**（空的 AppendEntries），目的是验证 Follower 的日志是否与 Leader 在 `nextIndex - 1` 位置一致。如果不一致，走你笔记中描述的"递减 nextIndex、重发"的回退流程。

### 4.4 stepDown —— 所有"退位"场景的统一入口

你笔记中反复提到"看到更高 term 就退位"，工程中 stepDown 被触发的完整场景清单：

| 触发场景 | 代码位置 | 说明 |
|----------|---------|------|
| 收到更高 term 的 RequestVote | handleRequestVoteRequest | 对方 term > 我的 term |
| 投票给某个 Candidate 后 | handleRequestVoteRequest | 重置选举定时器 |
| 收到更高 term 的 AppendEntries | checkStepDown | Leader/Candidate 发现新 Leader |
| 同 term 收到不同 Leader 的 AppendEntries | handleAppendEntriesRequest | 脑裂保护，双方 term+1 |
| 收到投票响应中更高的 term | handleRequestVoteResponse | 对方告知有更高 term |
| 收到 Pre-Vote 响应中更高的 term | handlePreVoteResponse | 同上 |
| Leader 多数派存活检查失败 | handleStepDownTimeout | 超过半数节点不可达 |
| Follower 返回更高 term 的 AppendEntries 响应 | increaseTermTo | Leader 发现自己过期 |
| 节点出错 | onError | 磁盘 IO 错误等 |
| 配置变更后 Leader 被移出集群 | confCtx.nextStage | 自己不在新配置中 |
| electSelf 持久化失败 | electSelf | 磁盘写入 (term, votedFor) 失败 |
| Leadership Transfer | transferLeadershipTo | 主动让位 |
| 收到 TimeoutNow 中更高的 term | handleTimeoutNowRequest | - |
| 节点关闭 | shutdown | 优雅退出 |

stepDown 的核心逻辑：

```java
private void stepDown(final long term, final boolean wakeupCandidate, final Status status) {
    if (!this.state.isActive()) return;
    
    // 1. 根据当前角色做清理
    if (this.state == State.STATE_CANDIDATE) {
        stopVoteTimer();
    } else if (this.state == State.STATE_LEADER || this.state == State.STATE_TRANSFERRING) {
        stopStepDownTimer();
        this.ballotBox.clearPendingTasks();  // 清理待确认的投票
        onLeaderStop(status);                 // 通知状态机
    }
    
    // 2. 清空 Leader 信息
    resetLeaderId(PeerId.emptyPeer(), status);
    
    // 3. 转为 Follower
    this.state = State.STATE_FOLLOWER;
    updateLastLeaderTimestamp(Utils.monotonicMs());
    
    // 4. 如果 term 变了，更新 term 并清空投票
    if (term > this.currTerm) {
        this.currTerm = term;
        this.votedId = PeerId.emptyPeer();  // ★ 新 term 必须清空投票记录
        this.metaStorage.setTermAndVotedFor(term, this.votedId);  // 持久化
    }
    
    // 5. 停止所有 Replicator
    if (wakeupCandidate) {
        // Leadership Transfer：找到目标节点发送 TimeoutNow
        this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
    } else {
        this.replicatorGroup.stopAll();
    }
    
    // 6. 重启选举定时器
    if (!isLearner()) {
        this.electionTimer.restart();
    }
}
```

---

## 五、日志复制的完整工程闭环

### 5.1 写请求的入口 —— apply

你笔记中的理论：

> "Leader 收到写请求后……把命令写入自己的预写日志（WAL），给它分配当前 term 和递增的 index，然后把这条日志通过 AppendEntries RPC 广播给所有 Follower。"

工程中的批量优化：

```java
public void apply(final Task task) {
    final LogEntry entry = new LogEntry();
    entry.setData(task.getData());
    
    // ★ 不直接处理，而是投递到 Disruptor RingBuffer
    final EventTranslator<LogEntryAndClosure> translator = (event, sequence) -> {
        event.done = task.getDone();
        event.entry = entry;
        event.expectedTerm = task.getExpectedTerm();
    };
    
    if (!this.applyQueue.tryPublishEvent(translator)) {
        // ★ 队列满 → 快速失败（过载保护）
        throw new OverloadException("Too many tasks");
    }
}
```

**Disruptor 批处理**：多个 apply 请求先进入 Disruptor 环形缓冲区，然后由消费者线程批量取出处理：

```java
// 消费者端
public void onEvent(LogEntryAndClosure event, long sequence, boolean endOfBatch) {
    this.tasks.add(event);
    // 累积到 applyBatch 大小（默认 32）或者是批次最后一条时，统一处理
    if (this.tasks.size() >= raftOptions.getApplyBatch() || endOfBatch) {
        executeApplyingTasks(this.tasks);
    }
}
```

### 5.2 executeApplyingTasks —— Leader 处理写请求的核心

```java
private void executeApplyingTasks(final List<LogEntryAndClosure> tasks) {
    // 1. 过载检查
    if (!this.logManager.hasAvailableCapacityToAppendEntries(1)) {
        // 回调所有 task 的 done 为 EBUSY
        return;
    }
    
    this.writeLock.lock();
    try {
        // 2. ★ 必须是 Leader
        if (this.state != State.STATE_LEADER) {
            // 回调错误并返回
            return;
        }
        
        final List<LogEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            final LogEntryAndClosure task = tasks.get(i);
            
            // 3. ★ expectedTerm 检查：防止跨任期提交
            if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                continue;  // 这个 task 是旧 term 发来的，拒绝
            }
            
            // 4. ★ 向 BallotBox 注册待确认任务
            this.ballotBox.appendPendingTask(
                this.conf.getConf(), 
                this.conf.isStable() ? null : this.conf.getOldConf(),
                task.done);
            
            // 5. 设置 entry 的 term 和类型
            task.entry.getId().setTerm(this.currTerm);
            task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            entries.add(task.entry);
        }
        
        // 6. ★ 批量追加到 LogManager（异步刷盘）
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
    } finally {
        this.writeLock.unlock();
    }
}
```

**LeaderStableClosure —— 本地日志持久化完成的回调**：

```java
class LeaderStableClosure extends LogManager.StableClosure {
    public void run(final Status status) {
        if (status.isOk()) {
            // ★ Leader 自己也投一票
            NodeImpl.this.ballotBox.commitAt(
                this.firstLogIndex, 
                this.firstLogIndex + this.nEntries - 1,
                NodeImpl.this.serverId);
        }
    }
}
```

这对应了你笔记中"Leader 把自己算作多数派中的一员"的隐含逻辑。

### 5.3 LogManager 的异步刷盘架构

```
apply() → Disruptor(Node) → executeApplyingTasks()
    → logManager.appendEntries(entries, stableClosure)
        → 写入内存缓存(logsInMemory)
        → 唤醒 Replicator（开始向 Follower 发送）
        → 提交到 Disruptor(LogManager) → AppendBatcher 攒批
            → logStorage.appendEntries() 实际磁盘写入
            → stableClosure.run() 回调 BallotBox
```

**关键设计**：日志写入内存后就**立即唤醒 Replicator**，不等磁盘刷盘完成。Replicator 从内存读取日志发送给 Follower。这意味着 Leader 本地刷盘和 Follower 的网络复制是**并行进行**的，大幅降低了写延迟。

### 5.4 Replicator 发送日志 —— Pipeline 模式

你笔记中描述的是简单的请求-响应模式，但工程中使用了 **Pipeline（流水线）**优化：

```java
void sendEntries() {
    while (true) {
        final long nextSendingIndex = getNextSendIndex();
        // Pipeline：不等前一个响应就发下一批
        if (!sendEntries(nextSendingIndex)) break;
    }
}

// getNextSendIndex() 的逻辑：
// 如果 inflights 队列未满，返回上一个请求的结束位置
// 即允许多个 AppendEntries RPC 同时在途
```

默认 `maxReplicatorInflightMsgs = 256`，即最多 256 个 AppendEntries 请求同时在途，不需要等待前一个响应。这对高延迟网络下的吞吐量提升巨大。

### 5.5 Follower 处理 AppendEntries —— 一致性检查

你笔记中的理论（完整的五步校验）：

> "第一步：比请求中的 term。第二步：认可 Leader 权威。第三步：校验前一条日志。第四步：同步日志条目。第五步：更新 commitIndex。"

工程实现（`handleAppendEntriesRequest`）：

```java
public Message handleAppendEntriesRequest(final AppendEntriesRequest request, ...) {
    this.writeLock.lock();
    try {
        // ★ 第一步：Stale term 检查
        if (request.getTerm() < this.currTerm) {
            return response(success=false, term=this.currTerm);
        }
        
        // ★ 第二步：认可 Leader 权威（checkStepDown）
        checkStepDown(request.getTerm(), serverId);
        
        // ★ 脑裂保护（你笔记未提到的工程保护）
        if (!serverId.equals(this.leaderId)) {
            // 同 term 两个不同 Leader！强制双方 stepDown
            stepDown(request.getTerm() + 1, false, ...);
            return response(success=false, term=request.getTerm()+1);
        }
        
        // 更新 Leader 时间戳（续约 lease）
        updateLastLeaderTimestamp(Utils.monotonicMs());
        
        // 正在安装快照时拒绝
        if (entriesCount > 0 && snapshotExecutor.isInstallingSnapshot()) {
            return error(EBUSY);
        }
        
        // ★ 第三步：校验前一条日志
        final long localPrevLogTerm = this.logManager.getTerm(prevLogIndex);
        if (localPrevLogTerm != prevLogTerm) {
            // 不匹配！返回 lastLogIndex 帮助 Leader 快速回退
            return response(success=false, lastLogIndex=logManager.getLastLogIndex());
        }
        
        // 心跳（无日志条目）
        if (entriesCount == 0) {
            // 更新 commitIndex
            this.ballotBox.setLastCommittedIndex(
                Math.min(request.getCommittedIndex(), prevLogIndex));
            return response(success=true);
        }
        
        // ★ 第四步：追加日志（异步）
        this.logManager.appendEntries(entries, new FollowerStableClosure(...));
        
        return null;  // 异步响应，等刷盘完成后回调
    } finally { ... }
}
```

**FollowerStableClosure —— Follower 日志持久化完成后的回调**：

```java
class FollowerStableClosure extends LogManager.StableClosure {
    public void run(final Status status) {
        if (!status.isOk()) { return error; }
        
        // ★★★ 关键安全检查：如果 term 变了，不能回复 success
        node.readLock.lock();
        try {
            if (this.term != node.currTerm) {
                // term 变了说明已经有新 Leader 了，
                // 当前这批日志可能会被新 Leader 截断
                responseBuilder.setSuccess(false).setTerm(node.currTerm);
                return;
            }
        } finally { node.readLock.unlock(); }
        
        // 回复成功
        responseBuilder.setSuccess(true).setTerm(this.term);
        
        // ★ 第五步：更新 commitIndex
        // committedIndex = min(request.committedIndex, 本次追加的最后一条日志)
        node.ballotBox.setLastCommittedIndex(this.committedIndex);
    }
}
```

**你笔记未提到的关键安全检查**：Follower 刷盘完成后，必须再检查一次 term 是否变了。为什么？考虑这个时序：

1. Follower 收到旧 Leader 的日志，开始刷盘
2. 刷盘过程中，新 Leader 当选，Follower 的 term 更新了
3. 刷盘完成，如果此时回复 success 给旧 Leader，旧 Leader 可能把它计入多数派

这会违反安全性。所以刷盘完成后必须检查 term，如果变了就返回失败。

### 5.6 Leader 收到 AppendEntries 响应 —— nextIndex 调整

你笔记中的理论：

> "Leader 收到后会把这个 Follower 的 nextIndex 减一，然后重发……一直重复直到找到一致的点。"

工程中的**快速回退优化**：

```java
static boolean onAppendEntriesReturned(...) {
    if (!response.getSuccess()) {
        // 发现更高 term → Leader 退位
        if (response.getTerm() > r.options.getTerm()) {
            node.increaseTermTo(response.getTerm(), ...);
            return false;
        }
        
        // ★★★ 快速回退优化（你笔记中"一步一步减"的优化版）
        if (response.getLastLogIndex() + 1 < r.nextIndex) {
            // Follower 的最后日志比 Leader 预期的少很多
            // 直接跳到 Follower 的 lastLogIndex + 1
            r.nextIndex = response.getLastLogIndex() + 1;
        } else {
            // Follower 有冲突日志，逐步回退
            if (r.nextIndex > 1) r.nextIndex--;
        }
        
        r.sendProbeRequest();  // 重新探测
        return false;
    }
    
    // ★ 成功：通知 BallotBox 该 peer 已确认
    r.options.getBallotBox().commitAt(
        r.nextIndex, r.nextIndex + entriesSize - 1, r.options.getPeerId());
    r.nextIndex += entriesSize;
    return true;  // 继续发送后续日志
}
```

**工程优化说明**：原始 Raft 论文中 nextIndex 每次只减 1，在 Follower 落后很多时效率极低。SOFAJRaft 的优化是让 Follower 在失败响应中返回自己的 `lastLogIndex`，Leader 可以直接跳到那个位置。这将回退次数从 O(日志差距) 降低到了通常 O(1)。

### 5.7 BallotBox —— 多数派确认与 commitIndex 推进

你笔记中的理论：

> "Leader 收集到多数派确认后，推进 commitIndex，然后按顺序把日志应用到状态机。"
> "只能通过当前任期的日志来推进 commitIndex，不能直接提交旧任期的日志。"

工程实现（`BallotBox.commitAt`）：

```java
public boolean commitAt(long firstLogIndex, long lastLogIndex, PeerId peer) {
    long lastCommittedIndex = 0;
    
    final long startAt = Math.max(this.pendingIndex, firstLogIndex);
    for (long logIndex = startAt; logIndex <= lastLogIndex; logIndex++) {
        final Ballot bl = this.pendingMetaQueue.get((int)(logIndex - this.pendingIndex));
        bl.grant(peer, hint);     // 为该日志投票
        if (bl.isGranted()) {     // ★ 达到多数派
            lastCommittedIndex = logIndex;
        }
    }
    
    if (lastCommittedIndex > 0) {
        // 移除已提交的 Ballot
        this.pendingMetaQueue.removeFromFirst(...);
        this.pendingIndex = lastCommittedIndex + 1;
        this.lastCommittedIndex = lastCommittedIndex;
    }
    
    // 通知 FSMCaller：有新日志可以 apply 了
    this.waiter.onCommitted(lastCommittedIndex);
    return true;
}
```

**"只能提交当前任期日志"的约束如何实现**：

不是在 `commitAt` 中检查 term，而是通过 `resetPendingIndex` 实现的——当新 Leader 上任时，`pendingIndex` 被设为 `lastLogIndex + 1`。这意味着 BallotBox 中只追踪新任期写入的日志，**旧任期的日志根本不在 pendingMetaQueue 中**，不可能被直接提交。旧日志只能在新任期日志被提交时，通过 commitIndex 的推进被"搭车"间接提交。

**Ballot 的多数派计算 —— 支持 Joint Consensus**：

```java
public class Ballot {
    private final UnfoundPeerId[] peers;    // 新配置的节点列表
    private int quorum;                      // 新配置的多数派阈值
    private final UnfoundPeerId[] oldPeers; // 旧配置的节点列表
    private int oldQuorum;                   // 旧配置的多数派阈值
    
    public boolean isGranted() {
        // ★ Joint Consensus：新旧两个配置都要达到多数派
        return this.quorum <= 0 && this.oldQuorum <= 0;
    }
}
```

在成员变更的过渡阶段（Joint Consensus），一条日志必须同时被新配置的多数派和旧配置的多数派确认才算提交。

---

## 六、状态机应用 —— FSMCaller

### 6.1 commitIndex 到 appliedIndex 的完整链路

```
BallotBox.commitAt() → 多数派达成 → waiter.onCommitted(committedIndex)
    → FSMCallerImpl 的 Disruptor 队列
        → 批量合并多个 COMMITTED 事件（取 maxCommittedIndex）
        → doCommitted(maxCommittedIndex)
            → 从 LogManager 读取 [lastApplied+1, committedIndex] 的日志
            → 逐条调用用户 StateMachine.onApply(iterator)
            → 更新 lastAppliedIndex
            → 通知 ReadOnlyService（线性一致性读等待者）
```

### 6.2 doCommitted 的实现

```java
private void doCommitted(final long committedIndex) {
    final long lastAppliedIndex = this.lastAppliedIndex.get();
    if (lastAppliedIndex >= committedIndex) return;  // 已经 apply 过了
    
    // 1. 弹出 closures（客户端回调）
    final List<Closure> closures = new ArrayList<>();
    this.closureQueue.popClosureUntil(committedIndex, closures, taskClosures);
    
    // 2. 创建 Iterator
    final IteratorImpl iterImpl = new IteratorImpl(
        this, this.logManager, closures,
        firstClosureIndex, lastAppliedIndex, committedIndex, this.applyingIndex);
    
    while (iterImpl.isGood()) {
        final LogEntry entry = iterImpl.entry();
        
        if (entry.getType() == ENTRY_TYPE_CONFIGURATION) {
            // 配置变更日志：通知用户状态机
            this.fsm.onConfigurationCommitted(new Configuration(entry.getPeers()));
            iterImpl.next();
            continue;
        }
        
        // 3. ★ 业务日志交给用户状态机
        doApplyTasks(iterImpl);  // 调用 fsm.onApply(iterator)
    }
    
    // 4. 更新 lastAppliedIndex
    setLastApplied(lastIndex, lastTerm);
    // 通知 ReadOnlyService 等待者
    notifyLastAppliedIndexUpdated(lastIndex);
}
```

**批量优化**：FSMCaller 内部会把连续多次 `onCommitted` 调用合并为一次——如果 commit 了 100 条日志触发了 100 次 onCommitted，最终只会调用一次 `doCommitted(最大的commitIndex)`。

---

## 七、线性一致性读 —— ReadIndex 协议

### 7.1 你笔记中的理论

> "Leader 在处理线性一致性读之前，会先做一个 ReadIndex 确认：记下当前的 commitIndex……然后向所有 Follower 发一轮心跳。只有多数派回复了心跳确认……才从状态机读取数据返回给客户端。"

### 7.2 工程中的完整流程

```java
// NodeImpl.readLeader() — Leader 处理 ReadIndex 请求
private void readLeader(final ReadIndexRequest request, ...) {
    final int quorum = getQuorum();
    
    // 单节点快速路径
    if (quorum <= 1) {
        respBuilder.setSuccess(true).setIndex(this.ballotBox.getLastCommittedIndex());
        closure.run(Status.OK());
        return;
    }
    
    final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
    
    // ★★★ 关键约束：Leader 必须在当前 term 提交过日志才能服务 ReadIndex
    if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
        closure.run(new Status(RaftError.EAGAIN, 
            "Leader has not committed any log entry at its term"));
        return;
    }
    
    respBuilder.setIndex(lastCommittedIndex);
    
    // 选择读模式
    ReadOnlyOption readOnlyOpt = ...;
    if (readOnlyOpt == ReadOnlyOption.ReadOnlyLeaseBased && !isLeaderLeaseValid()) {
        readOnlyOpt = ReadOnlyOption.ReadOnlySafe;  // lease 过期则降级
    }
    
    switch (readOnlyOpt) {
        case ReadOnlySafe:
            // ★ 向多数派发送心跳确认自己仍是 Leader
            for (final PeerId peer : targetPeers) {
                this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
            }
            break;
        case ReadOnlyLeaseBased:
            // ★ Lease 有效，直接返回（更快但依赖时钟）
            respBuilder.setSuccess(true);
            closure.run(Status.OK());
            break;
    }
}
```

**心跳确认成功后**：

```java
// ReadIndexHeartbeatResponseClosure
void run(Status status) {
    if (多数派心跳成功) {
        respBuilder.setSuccess(true);
        closure.run(Status.OK());
    }
}
```

**ReadOnlyService 收到确认后的等待逻辑**：

```java
// 收到 ReadIndex 确认后
if (readIndexStatus.isApplied(fsmCaller.getLastAppliedIndex())) {
    // appliedIndex >= commitIndex，可以立即读取
    notifySuccess(readIndexStatus);
} else {
    // ★ 还没 apply 到那个位置，加入等待队列
    pendingNotifyStatus.computeIfAbsent(index, k -> new ArrayList<>())
        .add(readIndexStatus);
}

// FSMCaller apply 日志后回调
public void onApplied(final long appliedIndex) {
    // 唤醒所有 index <= appliedIndex 的等待者
    final Map<Long, List<ReadIndexStatus>> statuses = 
        pendingNotifyStatus.headMap(appliedIndex, true);
    // 通知所有等待者可以读了
}
```

### 7.3 你笔记中的"appliedIndex 水位追踪"在工程中的体现

你笔记中描述的**单调读**保证：

> "每次读响应里会带上当前节点的 appliedIndex……下一次读的时候把上次拿到的 appliedIndex 作为 minAppliedIndex 带在请求里。"

这个逻辑在 SOFAJRaft 中不是由框架强制实现的，而是留给上层应用去做。但框架提供了基础：ReadIndex 响应中会返回 `commitIndex`，应用可以用它来做客户端水位追踪。

### 7.4 两种读模式的对比

| | ReadOnlySafe | ReadOnlyLeaseBased |
|---|---|---|
| 确认方式 | 向多数派发心跳 | 检查 Leader lease 是否在有效期内 |
| 延迟 | 一次心跳 RTT | 零额外延迟 |
| 安全性 | 绝对安全 | 依赖时钟单调性（时钟跳变可能读到过期数据） |
| 适用场景 | 默认模式 | 对延迟极敏感且时钟可靠的环境 |

---

## 八、日志存储的工程设计

### 8.1 LogManager 的内存 + 磁盘两级架构

```java
public class LogManagerImpl implements LogManager {
    private final SegmentList<LogEntry> logsInMemory;  // 内存缓存
    private LogStorage logStorage;                      // 磁盘存储（RocksDB）
    private LogId diskId;                               // 已刷盘的最后日志 ID
    private LogId appliedId;                            // 已应用的最后日志 ID
}
```

日志生命周期：

```
写入 → logsInMemory（内存）→ Disruptor 异步 → logStorage（磁盘 RocksDB）
                                                        ↓
                           apply 后清理内存 ← FSMCaller apply →  状态机
```

### 8.2 冲突日志的检测与截断

你笔记中的理论：

> "如果 Follower 在那个位置上有日志但 term 不一致，截断：从这个位置开始把后面的日志全部删掉。"

工程中的 `checkAndResolveConflict`：

```java
private boolean checkAndResolveConflict(List<LogEntry> entries, ...) {
    if (firstEntry.getId().getIndex() == 0) {
        // Leader 写入：自动分配 index
        for (LogEntry entry : entries) {
            entry.getId().setIndex(++this.lastLogIndex);
        }
    } else {
        // Follower 接收 Leader 日志
        if (firstEntry.getId().getIndex() > this.lastLogIndex + 1) {
            return false;  // 有 gap，拒绝（理论上不应该发生）
        }
        
        // 找冲突点
        int conflictingIndex = 0;
        for (; conflictingIndex < entries.size(); conflictingIndex++) {
            long localTerm = unsafeGetTerm(entries.get(conflictingIndex).getId().getIndex());
            if (localTerm != entries.get(conflictingIndex).getId().getTerm()) {
                break;  // term 不匹配，找到冲突
            }
        }
        
        if (conflictingIndex != entries.size()) {
            // ★ 截断冲突部分
            unsafeTruncateSuffix(entries.get(conflictingIndex).getId().getIndex() - 1);
        }
        
        // 去除重复前缀（已经存在且一致的不需要重复写入）
        entries.subList(0, conflictingIndex).clear();
    }
    return true;
}
```

**安全检查**：`unsafeTruncateSuffix` 有一个硬约束——不能截断已经 apply 到状态机的日志：

```java
if (lastIndexKept < this.appliedId.getIndex()) {
    LOG.error("FATAL: Can't truncate logs before appliedId");
    return;
}
```

这保证了状态机的一致性不会被破坏。

### 8.3 Protobuf 数据结构 —— AppendEntries 的字段设计

```protobuf
message AppendEntriesRequest {
    required string group_id = 1;       // Raft 组 ID（多组复用网络）
    required string server_id = 2;      // Leader ID
    required string peer_id = 3;        // 目标 Follower ID
    required int64 term = 4;            // Leader 当前 term
    required int64 prev_log_term = 5;   // 前一条日志的 term
    required int64 prev_log_index = 6;  // 前一条日志的 index
    repeated EntryMeta entries = 7;     // 日志元数据数组
    required int64 committed_index = 8; // Leader 的 commitIndex
    optional bytes data = 9;            // 所有日志的 data 拼接
}
```

**data 字段的设计优化**：entries 中的日志元数据与实际 data 是分离的。所有日志的 data 被拼接到一个 bytes 字段中，通过每条 entry 的 `data_len` 来切割。这减少了 protobuf 序列化的开销（避免大量小对象）。

---

## 九、快照机制 —— 你笔记未覆盖的核心工程功能

### 9.1 为什么需要快照

你的笔记集中在日志复制上，但没有讨论一个工程中绕不过去的问题：**日志不能无限增长**。如果一个节点宕机很久再恢复，或者新节点加入集群，需要从头重放所有日志来恢复状态——如果有几百万条日志，这是不可接受的。

快照（Snapshot）就是解决方案：定期将状态机的完整状态保存到磁盘，然后安全地丢弃快照之前的所有日志。

### 9.2 快照安装的触发时机

当 Leader 发现某个 Follower 的 `nextIndex` 所指向的日志已经被压缩（被快照替代），就无法通过日志复制来同步了——因为那些日志已经不存在了。此时触发快照安装：

```java
// Replicator.fillCommonFields()
if (!fillCommonFields(rb, this.nextIndex - 1, isHeartbeat)) {
    // prevLogIndex 对应的日志已被压缩
    installSnapshot();  // ★ 需要安装快照
    return;
}
```

### 9.3 快照安装的 RPC

```protobuf
message InstallSnapshotRequest {
    required int64 term = 4;
    required SnapshotMeta meta = 5;  // 快照元数据
    required string uri = 6;         // ★ 快照文件的下载 URI
}
```

SOFAJRaft 的快照传输采用 **URI 引用方式**——Leader 只告诉 Follower 快照在哪里下载，Follower 通过文件服务自行下载。这将快照传输与 Raft 协议解耦，避免了大文件传输阻塞 Raft RPC。

---

## 十、成员变更 —— Joint Consensus 的工程实现

### 10.1 你笔记中的理论

> "解法二：共同一致性（Joint Consensus）——Leader 先下发一个中间过渡状态 $C_{old,new}$。"

### 10.2 工程中的数据结构

```java
public class ConfigurationEntry {
    private Configuration conf;     // 新配置 C_new
    private Configuration oldConf;  // 旧配置 C_old
    
    public boolean isStable() {
        return this.oldConf == null || this.oldConf.isEmpty();
    }
}
```

- `isStable() == true`：稳定状态，只需要 conf 的多数派
- `isStable() == false`：Joint 过渡状态，需要 conf 和 oldConf 的**双重多数派**

### 10.3 变更流程

1. Leader 收到变更请求 → 写入 `ENTRY_TYPE_CONFIGURATION` 日志，`peers` = C_new，`oldPeers` = C_old
2. 此日志在新旧两个配置的多数派都确认后提交
3. 提交后 Leader 再写入一条只有 `peers` = C_new 的配置日志（`oldPeers` 为空），完成切换

### 10.4 BallotBox 中的双重多数派检查

```java
// Ballot.isGranted()
public boolean isGranted() {
    return this.quorum <= 0 && this.oldQuorum <= 0;
    // quorum 和 oldQuorum 分别对应新旧配置的多数派
    // 只有两个都达到才算 granted
}
```

---

## 十一、Leader 存活性检查 —— 你笔记未覆盖的工程保护

### 11.1 问题场景

如果 Leader 被网络分区隔离了但它不知道（比如出方向的网络还通但入方向断了），它会一直以为自己是 Leader，虽然写请求会因为凑不齐多数派而超时失败，但它不会主动退位。这会导致客户端持续重试写失败。

### 11.2 stepDown 定时器

SOFAJRaft 的解决方案是 Leader 主动检测自己是否还能联系到多数派：

```java
// handleStepDownTimeout() — 每 electionTimeoutMs/2 触发一次
private void handleStepDownTimeout() {
    if (!checkDeadNodes(this.conf.getConf(), monotonicNowMs, true)) {
        // 多数派不可达，主动 stepDown
    }
}

// checkDeadNodes：检查在 leaderLeaseTimeoutMs 内是否有多数派节点有 RPC 通信
private boolean checkDeadNodes(Configuration conf, long monotonicNowMs, boolean stepDownOnCheckFail) {
    int aliveCount = 1;  // 自己算一个
    for (PeerId peer : conf.listPeers()) {
        if (peer.equals(this.serverId)) continue;
        long lastRpcSendTimestamp = this.replicatorGroup.getLastRpcSendTimestamp(peer);
        if (monotonicNowMs - lastRpcSendTimestamp <= this.options.getLeaderLeaseTimeoutMs()) {
            aliveCount++;
        }
    }
    if (aliveCount < conf.listPeers().size() / 2 + 1) {
        // 少于多数派存活
        if (stepDownOnCheckFail) {
            stepDown(this.currTerm, false, ...);
        }
        return false;
    }
    return true;
}
```

**Leader Lease**：`leaderLeaseTimeoutMs = electionTimeoutMs * 90%`。Leader 如果在这个时间内没有收到某个 Follower 的任何 RPC 响应，就认为该 Follower 不可达。

---

## 十二、RaftMetaStorage —— term 和 votedFor 的持久化

### 12.1 为什么必须持久化

你笔记中提到："持久化到磁盘，防止重启后重复投票。"

工程中的具体实现——`LocalRaftMetaStorage` 使用本地文件存储：

```java
public class LocalRaftMetaStorage implements RaftMetaStorage {
    private long term;
    private PeerId votedFor = PeerId.emptyPeer();
    private String path;  // 存储路径：{dataPath}/raft_meta
    
    public boolean setTermAndVotedFor(long term, PeerId peer) {
        // 写入本地文件并 fsync
    }
}
```

**何时持久化**：

1. `electSelf()` 中递增 term 并投票给自己后，**发送 RPC 之前**
2. `handleRequestVoteRequest()` 中投票给某个 Candidate 后
3. `stepDown()` 中 term 变更后

---

## 十三、工程中的并发控制与性能优化

### 13.1 全局读写锁

NodeImpl 使用一把 `ReadWriteLock` 保护所有内部状态：

- **WriteLock**：状态修改操作（选举、stepDown、apply、handleAppendEntries 等）
- **ReadLock**：只读查询（handleReadIndex、listPeers、存活性检查的快速路径）

**锁的释放-重获取模式**（用于避免持锁做 IO）：

```java
oldTerm = this.currTerm;
this.writeLock.unlock();
// 执行耗时操作（如 getLastLogId）
final LogId lastLogId = this.logManager.getLastLogId(true);
this.writeLock.lock();
// ABA 检查
if (oldTerm != this.currTerm) return;  // 期间状态被改变，放弃当前操作
```

### 13.2 Disruptor 无锁队列

系统中有**三个 Disruptor**：

| Disruptor | 所在组件 | 作用 |
|-----------|---------|------|
| applyQueue | NodeImpl | 批量收集 apply 请求 |
| diskQueue | LogManagerImpl | 异步刷盘 |
| taskQueue | FSMCallerImpl | 串行驱动状态机 |

默认 Ring Buffer 大小 = 16384。

### 13.3 StampedLock

BallotBox 使用 `StampedLock` 而非普通 ReentrantLock，利用其乐观读模式减少读操作的锁竞争：

```java
// 乐观读
long stamp = stampedLock.tryOptimisticRead();
long value = this.lastCommittedIndex;
if (!stampedLock.validate(stamp)) {
    // 有写操作干扰，升级为悲观读
    stamp = stampedLock.readLock();
    value = this.lastCommittedIndex;
    stampedLock.unlockRead(stamp);
}
```

---

## 十四、默认配置参数一览

| 参数 | 默认值 | 说明 |
|------|--------|------|
| electionTimeoutMs | 1000ms | 选举超时 |
| maxElectionDelayMs | 1000ms | 选举超时随机抖动上限 |
| 心跳间隔 | 100ms | = electionTimeoutMs / 10 |
| Leader Lease | 900ms | = electionTimeoutMs × 90% |
| maxEntriesSize | 1024 | 单次 AppendEntries 最大条目数 |
| maxBodySize | 512KB | 单次 AppendEntries 最大字节数 |
| maxReplicatorInflightMsgs | 256 | Pipeline 在途请求上限 |
| applyBatch | 32 | 批量 apply 大小 |
| disruptorBufferSize | 16384 | Disruptor Ring Buffer 大小 |
| snapshotIntervalSecs | 3600 | 自动快照间隔（1小时） |
| sync | true | 日志写入是否 fsync |
| enableLogEntryChecksum | false | 是否启用日志 CRC 校验 |

---

## 十五、对照检查 —— 你笔记中的每个理论点在工程中的位置

| 你笔记中的概念 | 工程中的实现位置 | 补充说明 |
|---------------|-----------------|---------|
| 随机化选举超时 | `RepeatedTimer.adjustTimeout() + randomTimeout()` | 每次重新随机 |
| Pre-Vote | `NodeImpl.preVote() + handlePreVoteRequest()` | 额外检查 Leader lease |
| term 比较导致退位 | `stepDown()` 被 15+ 个位置调用 | 见第四节场景清单 |
| 一个 term 只投一票 | `metaStorage.setTermAndVotedFor()` 持久化 | 先持久化再发 RPC |
| 先比 lastLogTerm 再比 lastLogIndex | `LogId.compareTo()` | 标准 Comparable 实现 |
| 多数派不会同时两个 | `Ballot.isGranted()` + Joint Consensus | 支持双配置多数派 |
| Leader 追加 no-op | `becomeLeader() → confCtx.flush()` | 用配置日志代替空日志 |
| nextIndex 初始化为尾后 | `Replicator` 创建时 `nextIndex = lastLogIndex+1` | 乐观假设 |
| 惰性修复（递减 nextIndex） | `onAppendEntriesReturned` 快速回退 | 比论文更优化 |
| prevLogIndex/prevLogTerm 校验 | `handleAppendEntriesRequest` 第三步 | 返回 lastLogIndex 加速回退 |
| commitIndex 推进需多数派 | `BallotBox.commitAt()` | 支持 Pipeline 并发确认 |
| 只提交当前 term 日志 | `resetPendingIndex()` 限定跟踪范围 | 旧日志搭车间接提交 |
| 状态机按序应用 | `FSMCallerImpl.doCommitted()` 单线程 Disruptor | 保证串行有序 |
| ReadIndex 线性一致性读 | `ReadOnlyServiceImpl + readLeader()` | 支持 lease 降级 |
| appliedIndex 单调读 | 留给上层应用实现 | 框架提供 commitIndex 基础 |
| 网络分区旧 Leader 退位 | `stepDownTimer + checkDeadNodes()` | 主动检测多数派 |
| 写请求 Follower 转发 | `RouteTable + RheaKV` 层实现 | 核心层只拒绝，上层转发 |

---

## 十六、你笔记中缺失的工程闭环

通过源码分析，以下是你理论文档中没有覆盖但工程中必须解决的关键问题：

### 16.1 Leader 如何检测到自己已经过期

你只提到"旧 Leader 收到更高 term 的消息就退位"，但工程中还有**主动检测**机制——stepDown 定时器每 500ms 检查一次多数派是否可达，不可达则主动退位。这避免了依赖被动收到消息。

### 16.2 Follower 刷盘完成后的 term 校验

异步刷盘完成后，如果发现 term 变了，不能回复 success。这是一个极其微妙的竞态条件保护。

### 16.3 快照与日志压缩

日志不可能无限增长。快照机制是工程必需品——定期保存状态机快照，然后安全丢弃快照之前的日志。落后太多的 Follower 直接安装快照而非逐条重放日志。

### 16.4 过载保护

生产环境必须防止系统雪崩：Disruptor 队列满时快速失败、LogManager 容量检查、正在安装快照时拒绝日志复制请求。

### 16.5 Leadership Transfer

主动让位机制——维护、升级、或负载均衡时需要将 Leader 角色平滑转移到指定节点，而不是依赖 Kill + 重新选举。

### 16.6 Priority Election

指定某些节点优先当选 Leader（比如机房就近），同时保证高优先级节点故障时低优先级节点能接班。

### 16.7 Learner 角色

只接收日志但不参与投票的节点——用于数据同步到异地副本、新节点追赶数据等场景。

### 16.8 Pipeline 复制

论文中的简单请求-响应模式在高延迟网络下吞吐极低。工程中必须支持多个 AppendEntries 请求同时在途。

### 16.9 批量优化

单条日志处理的开销太大。工程中到处都是批量优化——apply 批量收集、LogManager 攒批刷盘、FSMCaller 合并 commit 通知。

### 16.10 CRC 校验

网络和磁盘都可能出错。日志条目支持 CRC64 校验和，读取时验证数据完整性。

---

## 十七、总结

对比理论笔记和工程源码，可以看到：

**理论是骨架**：选主、日志复制、提交规则、线性一致性读——这些核心机制定义了 Raft 的正确性。

**工程是血肉**：在骨架之上，工程必须解决性能（Pipeline、批量、异步 IO）、可靠性（CRC、fsync、快照）、可用性（过载保护、Leadership Transfer、Priority Election）、以及大量的并发安全（锁、ABA 防御、异步回调中的 term 校验）问题。

每一个工程细节都不是"可有可无"的优化，而是生产环境中血淋淋的 bug 教训的沉淀。理论保证了算法在理想环境下正确，工程保证了它在真实世界中可靠地运行。

---

> **源码路径**：`/Users/zhanghonghao/Desktop/RaftByJava/sofa-jraft/jraft-core/src/main/java/com/alipay/sofa/jraft/`
> 
> 核心文件：
> - `core/NodeImpl.java` — 节点状态机（~3500 行）
> - `core/Replicator.java` — 日志复制器（~2000 行）
> - `core/BallotBox.java` — 投票箱
> - `core/FSMCallerImpl.java` — 状态机驱动器
> - `core/ReadOnlyServiceImpl.java` — 线性一致性读
> - `storage/impl/LogManagerImpl.java` — 日志管理器
> - `entity/LogEntry.java` — 日志条目结构