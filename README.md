# RaftByJava

这个仓库包含两大块内容：一是用 Java + Netty 从零实现的 Raft 分布式共识算法，二是围绕分布式系统、网络框架、消息中间件等核心中间件的大量源码阅读笔记。前者是"动手造轮子"，后者是"拆轮子看里面"，两者相辅相成——实现 Raft 时遇到的网络层、线程模型、日志存储等问题，都能在源码笔记里找到工业级的参考答案。

---

## 第一部分：Raft 算法实现

### 是什么

不依赖任何 Raft 框架，用 Java 从零实现一个功能完整的 Raft 共识节点。内置一个内存 KV 状态机，支持 `set key=value` 和 `delete key` 命令，三节点集群可以选出 Leader、复制日志、线性一致读、崩溃恢复。

### 已实现的特性

Leader 选举（含 Pre-Vote 防止网络分区节点干扰）、日志复制与多数派提交（`prevLogIndex`/`prevLogTerm` 一致性校验 + 冲突截断重传）、基于 ReadIndex 的线性一致读、客户端幂等去重（Session Table）、日志压缩与快照（Snapshot + InstallSnapshot RPC）、单节点成员变更（`CONFIG:ADD`/`CONFIG:REMOVE`）、持久化恢复（`currentTerm`/`votedFor`/WAL/快照 落盘，重启按"快照 → WAL → 元数据"顺序恢复）。

### 架构设计

核心设计原则：网络 IO 和算法逻辑运行在不同线程上。Netty NIO 线程池负责连接管理和编解码，一个专用单线程执行器处理所有 Raft 状态变更。网络线程收到消息后通过 `RaftMessageHandler` 投递到算法线程队列。算法层完全无锁，term、role、votedFor、日志等状态始终在同一个线程内读写。

节点启动流程：加载配置 → 创建算法线程 → 创建持久化管理器 → 创建核心状态对象 → 从磁盘恢复 → 启动 Netty 服务端 → 异步连接其他节点（自动重连） → 初始化 Raft 核心（设为 Follower，启动选举定时器） → 选举超时 → 投票 → 产生 Leader。

### 快速开始

环境要求：JDK 8+、Maven 3.6+

编译：

```bash
mvn clean package -DskipTests
```

启动三节点集群（三个终端分别执行，节点可任意顺序启动）：

```bash
# 终端 1
java -jar target/raft-by-java-1.0-SNAPSHOT.jar config/node1.json

# 终端 2
java -jar target/raft-by-java-1.0-SNAPSHOT.jar config/node2.json

# 终端 3
java -jar target/raft-by-java-1.0-SNAPSHOT.jar config/node3.json
```

三个节点全部启动后会在 150~300ms 内选出 Leader，日志中打印 `became leader`。

与集群交互——通过 TCP 连接发送 JSON 请求，协议为"4 字节大端长度前缀 + JSON 载荷"：

写入数据（发任意节点，Follower 自动转发给 Leader）：

```json
{
  "type": "ClientWriteRequest",
  "clientId": "my-client",
  "sequenceNumber": 1,
  "command": "set name=raft"
}
```

线性一致读：

```json
{
  "type": "ClientReadRequest",
  "clientId": "my-client",
  "key": "name",
  "linearizable": true,
  "minAppliedIndex": 0
}
```

删除数据：

```json
{
  "type": "ClientWriteRequest",
  "clientId": "my-client",
  "sequenceNumber": 2,
  "command": "delete name"
}
```

`clientId` + `sequenceNumber` 用于幂等去重，重复请求不会被执行两次。

### 配置文件

每个节点通过 JSON 配置启动，字段如下：

| 字段 | 类型 | 说明 |
|------|------|------|
| `nodeId` | String | 节点唯一标识 |
| `listenHost` | String | 监听地址 |
| `listenPort` | int | 监听端口 |
| `peers` | Array | 集群全部成员（含自己），每项含 `nodeId`/`host`/`port` |
| `electionTimeoutMinMs` | int | 选举超时下限（ms），默认 150 |
| `electionTimeoutMaxMs` | int | 选举超时上限（ms），默认 300 |
| `heartbeatIntervalMs` | int | 心跳间隔（ms），默认 50，须远小于选举超时下限 |
| `dataDir` | String | 持久化目录，存放 WAL、快照、元数据 |
| `snapshotThreshold` | int | 快照触发阈值（日志条目数），默认 1000 |

### 源码结构

```
src/main/java/com/raft/
├── RaftNode.java                  # 入口类，组装并启动所有组件
├── config/                        # 配置加载
│   ├── RaftConfig.java            #   配置对象（含成员变更支持）
│   ├── PeerConfig.java            #   单节点配置
│   └── ConfigLoader.java          #   JSON 配置加载器
├── core/                          # Raft 算法核心
│   ├── RaftCore.java              #   算法大脑，所有状态变更在此发生
│   ├── NodeState.java             #   节点状态（term、votedFor、role、nextIndex、matchIndex）
│   ├── NodeRole.java              #   角色枚举：LEADER / FOLLOWER / CANDIDATE
│   ├── LogEntry.java              #   日志条目 {term, index, command}
│   ├── LogManager.java            #   WAL 日志管理（内存 + 持久化）
│   ├── StateMachine.java          #   KV 状态机（set / delete）
│   ├── ElectionManager.java       #   选举逻辑（Pre-Vote、RequestVote）
│   ├── ReplicationManager.java    #   日志复制（AppendEntries、InstallSnapshot）
│   ├── TimerManager.java          #   选举超时与心跳定时器
│   ├── SnapshotManager.java       #   快照管理
│   ├── PersistenceManager.java    #   持久化（meta / WAL / 快照落盘）
│   ├── ClientSessionTable.java    #   客户端幂等去重
│   └── PendingWrite.java          #   待提交写请求追踪
├── rpc/                           # 网络通信层（Netty）
│   ├── RaftNettyServer.java       #   TCP 服务端
│   ├── RaftNettyClient.java       #   TCP 客户端
│   ├── PeerConnectionManager.java #   节点连接管理（自动重连）
│   ├── RaftMessageHandler.java    #   消息分发（网络线程 → 算法线程）
│   ├── MessageEncoder.java        #   协议编码器
│   ├── MessageDecoder.java        #   协议解码器
│   └── message/                   #   全部 RPC 消息类型定义
│       ├── AppendEntriesRequest / Response
│       ├── RequestVoteRequest / Response
│       ├── PreVoteRequest / Response
│       ├── InstallSnapshotRequest / Response
│       ├── ClientWriteRequest / Response
│       ├── ClientReadRequest / Response
│       ├── MembershipChangeRequest / Response
│       └── ...
└── util/
    └── Threads.java               #   线程工厂
```

### Raft 研究文档

根目录下的中文研究文档是理解实现设计思路的一手资料，按阅读顺序推荐：

`Raft算法-写代码前研究提纲.md` — 最核心的文档。从 CAP 理论出发，全面梳理 Raft 的核心机制（选举、复制、提交、快照）、每条 RPC 的请求链路、各种边界 case 和 Q&A。写代码前先读这篇。

`Raft节点启动流程详解.md` — 从设计思路角度讲解节点从启动到首次选出 Leader 的完整过程，解释了组件创建顺序、网络层与算法线程的分工、自动重连机制等。

`我眼里的Raft.md` — 实现完成后用自己的话重新梳理对 Raft 的理解，不追求学术严谨，追求把每个环节想明白。从"为什么要选主"讲起，覆盖 Pre-Vote、日志复制、提交规则、线性一致读等。

`我眼里的Multi-Raft.md` — 对 Multi-Raft 架构的理解，涉及 Region 分裂、多 Raft 组管理。

`Raft工程落地思维.md` — 工程落地过程中的思考与经验总结。

`工程落地的Raft源码讲解-结合我眼里的Raft.md` — 结合本项目源码的 Raft 讲解。

`Redis与Raft的取舍.md` — Redis Sentinel/Cluster 与 Raft 在数据一致性方案上的对比。

`分布式集群算法选型思维.md` — 分布式共识算法选型的思考框架。

`Vertical Paxos论文逐节精讲.md` / `Vertical Paxos与Raft的关系.md` / `vertical-paxos-raft-teacher-style讲解.md` — Vertical Paxos 论文解读，以及它与 Raft 的关系分析。

`Spec-Coding完整流程指南.md` — 规范化编码流程指南。

`architecture-comparison.html` — 可视化架构对比页面（浏览器打开）。

---

## 第二部分：中间件源码阅读笔记

这部分是对 Java 生态中核心中间件的逐源码分析，覆盖网络框架、RPC 框架、消息中间件、数据同步、诊断工具、响应式编程六大领域。每篇文档都遵循"先给调用链全景，再逐层展开源码"的风格，不跳步、不省略。

### Netty 源码全流程解析（`output/`，18 篇）

从 NioEventLoop 线程模型到 ByteBuf 内存管理，从 ChannelPipeline 责任链到零拷贝实现，系统拆解 Netty 的高性能网络编程设计。第 17 篇从 Netty 视角反观 Dubbo 和 RocketMQ 的网络层设计，第 18 篇扩展到 Disruptor 高性能环形缓冲区。

目录概览：NioEventLoop 线程模型 → Channel 体系与生命周期 → ChannelPipeline 责任链 → ByteBuf 内存管理 → Bootstrap 启动 → 编解码器框架 → 内置 Handler → 写缓冲区与 Flush → 零拷贝与 FileRegion → 内存泄漏检测 → TCP 连接管理 → HTTP 协议支持 → epoll/kqueue 原生传输 → 高性能并发工具 → 线程模型总结 → 整体架构哲学 → Dubbo/RocketMQ 网络层设计 → Disruptor 环形缓冲区。

### Reactor 响应式编程源码解析（`webflux-output/` 和 `webflux-output-easy/`，各 18 篇）

两个版本，`webflux-output/` 是深度版，`webflux-output-easy/` 是精简版。从 Flux/Mono 类体系到操作符实现模式，从订阅机制与背压协议到 Scheduler 调度器体系，完整拆解 Reactor 的响应式编程设计，并从 Reactor 视角理解 WebFlux 和 Spring 响应式架构。

目录概览：实例走读 → Flux/Mono 类体系 → 操作符实现模式 → 订阅机制与背压 → 数据源 Publisher → 常用操作符逐个解析 → Scheduler 调度器 → 线程切换 subscribeOn/publishOn → 熔合优化 Fuseable → Sinks 机制 → Context 上下文传播 → Hooks 与错误处理 → ConnectableFlux 多播 → ParallelFlux 并行处理 → Scannable 诊断 → 装配时与执行时区分 → WebFlux 与 Spring 响应式架构 → 整体设计哲学。

### Dubbo 源码解析（`docs-advanced-config/`，12 篇）

包含 Dubbo 高级配置 8 篇 + 核心机制 4 篇。从 SPI 扩展点机制到集群容错与负载均衡，从 Filter 链到配置中心与链路追踪，覆盖 Dubbo 服务治理的方方面面。核心机制篇包括 Exchange 与 Transport 分层设计、URL 全流程变化形态、服务提供者暴露服务、消费者引用服务的完整源码走读。

目录概览：SPI 扩展点 → 线程池与消息派发 → 启动优化与连接管理 → 集群容错与负载均衡 → RpcContext 与隐式传参 → 服务治理与流量管控 → Filter 链 → 配置中心/元数据/链路追踪/泛化调用 → Exchange 与 Transport 分层 → URL 全流程 → 服务暴露 → 服务引用。

### Sentinel 流控组件源码解析（`docs-advanced-config/`，6 篇）

阿里巴巴 Sentinel 流控框架的源码级解析，覆盖责任链执行、限流规则、热点参数限流（令牌桶 + LRU）、熔断降级状态机、系统自适应保护（BBR 算法）、规则动态配置。

目录概览：核心入口与责任链 → 限流规则 → 热点参数限流 → 熔断降级 CircuitBreaker → 系统自适应保护 BBR → 规则动态配置 DataSource。

### RocketMQ 源码解析（`rocketmq-source-reading/`，8 篇）

从架构总览到 NameServer 路由管理，从 Broker 启动到消息发送/存储/消费全链路，覆盖 Remoting 通信层和高可用 HA 主从同步。

目录概览：架构总览与模块解析 → NameServer 启动与路由管理 → Broker 启动 → 消息发送 → 消息存储 → 消息消费 → Remoting 通信层 → HA 与主从同步。

### Canal 源码解析（`docs-canal-source/`，8 篇）

阿里巴巴 Canal 数据同步中间件的源码级深度剖析。从伪装 MySQL Slave 协议获取 binlog，到 Disruptor 四阶段并行流水线解析，到环形缓冲区存储，再到客户端订阅协议和下游存储同步，完整覆盖数据流转全链路。该目录下有独立的 README 提供阅读指引和设计模式索引。

目录概览：整体架构与启动 → Parse 模块（模拟 Slave + BinlogDump）→ Binlog 事件解析 → Sink 过滤与 Store 环形缓冲区 → CanalInstance 装配与位点持久化 → 客户端订阅协议 → Server 网络层 → ClientAdapter 下游同步。

### Arthas 源码解析（`arthas-source-docs/`，6 篇）

阿里巴巴 Arthas Java 诊断工具的源码级解析。从整体架构到字节码增强与 Instrumentation 机制，从命令系统到 Watch/Trace/Stack 方法监控核心链路，再到 Session 管理和 Tunnel 通信。

目录概览：整体架构与启动 → 字节码增强与 Instrumentation → 命令系统与 CommandExecutor → Watch/Trace/Stack 方法监控 → Jad/MC/Ognl 类操作 → Session/Tunnel/WebUI 交互。

### 开源项目源码（`tmp-source-reading/`）

三个完整开源项目的源码，供阅读时对照：RocketMQ、Canal、Arthas。

### SOFAJRaft 源码（`sofa-jraft/`）

蚂蚁集团开源的生产级 Java Raft 实现，支持 Multi-Raft-Group。是本项目 Raft 实现最重要的参考实现，包含完整源码。通过对照阅读可以理解从教学级实现到工业级实现之间的差距。

---

## 技术栈

Raft 实现部分：Java 8、Netty 4.1（网络通信）、Jackson（JSON 序列化）、SLF4J + Logback（日志）、JUnit 5（测试）、Maven（构建）。

源码笔记涉及的技术栈：Netty、Reactor/WebFlux、Dubbo、Sentinel、RocketMQ、Canal、Arthas、SOFAJRaft、Disruptor。
