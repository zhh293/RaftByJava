# RocketMQ 架构总览与模块深度解析

> 本文档基于 RocketMQ 源码逐行分析，采用 Dubbo 源码阅读文档的风格——架构讲解与源码分析相结合，逐步推进，不跳步。
> 每一个核心类、核心方法都会标注源码路径与方法签名，方便读者对照源码阅读。

---

## 目录

- [一、引言：为什么需要读懂 RocketMQ 源码](#一引言为什么需要读懂-rocketmq-源码)
- [二、RocketMQ 整体架构设计哲学](#二rocketmq-整体架构设计哲学)
  - [2.1 为什么选择 NameServer 而非 ZooKeeper](#21-为什么选择-nameserver-而非-zookeeper)
  - [2.2 为什么 CommitLog + ConsumeQueue 分离](#22-为什么-commitlog--consumequeue-分离)
  - [2.3 为什么 Master-Slave 使用原生 NIO 而非 Netty](#23-为什么-master-slave-使用原生-nio-而非-netty)
  - [2.4 为什么选择长轮询而非 Push 推送](#24-为什么选择长轮询而非-push-推送)
  - [2.5 为什么采用顺序写而非随机写](#25-为什么采用顺序写而非随机写)
- [三、模块全景与依赖关系](#三模块全景与依赖关系)
  - [3.1 模块清单与职责](#31-模块清单与职责)
  - [3.2 模块依赖关系图](#32-模块依赖关系图)
  - [3.3 各模块源码目录结构](#33-各模块源码目录结构)
- [四、NameServer 模块深度解析](#四nameserver-模块深度解析)
- [五、Broker 模块深度解析](#五broker-模块深度解析)
- [六、Store 模块深度解析](#六store-模块深度解析)
- [七、Remoting 模块深度解析](#七remoting-模块深度解析)
- [八、Client 模块深度解析](#八client-模块深度解析)
- [九、HA 模块深度解析](#九ha-模块深度解析)
- [十、消息全链路追踪：从生产到消费](#十消息全链路追踪从生产到消费)
- [十一、关键设计决策与工程考量](#十一关键设计决策与工程考量)

---

## 一、引言：为什么需要读懂 RocketMQ 源码

RocketMQ 是阿里巴巴开源的分布式消息中间件，后捐赠给 Apache 基金会，成为顶级项目。它在阿里内部支撑着双 11 等海量场景，每天处理万亿级消息。读懂 RocketMQ 源码，意义在于：

1. **分布式系统设计的教科书**：从服务发现、主从复制、一致性保证到故障隔离，RocketMQ 的每一层都体现了分布式系统的经典设计模式。
2. **高性能存储引擎的实现**：CommitLog 的顺序写、ConsumeQueue 的索引机制、MappedFile 的内存映射、GroupCommit 的刷盘策略——这些技术组合在一起，构成了一个高性能消息存储引擎的完整实现。
3. **Netty RPC 框架的最佳实践**：RocketMQ 的 remoting 模块是一个完整的、生产级的 Netty RPC 框架，涵盖了编解码、线程模型、超时管理、背压控制等核心话题。
4. **从 Raft 视角理解 Master-Slave**：RocketMQ 的 Controller 模块基于 Raft 实现自动主从切换，理解 RocketMQ 的 HA 机制，有助于将 Raft 理论与工程实践对照学习。

本文将按照"架构哲学 → 模块职责 → 核心类 → 核心方法 → 源码片段"的层次逐步展开。

---

## 二、RocketMQ 整体架构设计哲学

### 2.1 为什么选择 NameServer 而非 ZooKeeper

在分布式消息中间件领域，服务发现是一个核心问题。Kafka 早期依赖 ZooKeeper 进行 Broker 注册与发现，RocketMQ 早期版本也曾依赖 ZooKeeper，但后来转向了自研的 NameServer。这一设计决策背后的考量值得深入分析。

#### 2.1.1 ZooKeeper 的复杂性问题

ZooKeeper 是一个成熟的协调服务框架，基于 ZAB 协议（类 Paxos）实现强一致性。但它存在以下工程痛点：

```
问题 1：部署复杂度高
  - ZooKeeper 集群本身需要 3 个或 5 个节点
  - 运维人员需要同时维护 ZK 集群和 Broker 集群
  - ZK 集群的升级、监控、故障排查都是额外负担

问题 2：强一致性的代价
  - ZAB 协议要求每次写操作都经过 leader，leader 写入后需 majority 确认
  - 对于路由信息这种"读多写少"的场景，强一致性是过度设计
  - ZK 的写性能约为 2-3 万 TPS，而 NameServer 可以轻松达到 10 万+

问题 3：脑裂风险
  - ZK 在网络分区时可能出现 leader 选举超时
  - 消息中间件的元数据不一致会导致消息路由错误，影响面大

问题 4：API 复杂
  - ZK 的 Watcher 机制是一次性的，需要反复注册
  - 客户端需要处理 Session 过期、ConnectionLoss 等复杂场景
```

#### 2.1.2 NameServer 的设计哲学：最终一致性 + 无状态

RocketMQ 的 NameServer 采用了完全不同的设计路线：

```
设计原则 1：NameServer 之间互不通信
  - 多个 NameServer 实例独立运行，彼此不交换数据
  - Broker 同时向所有 NameServer 发送心跳注册
  - 客户端随机选择一个 NameServer 获取路由信息

设计原则 2：最终一致性而非强一致性
  - 不同 NameServer 的路由数据可能短暂不一致
  - 但通过心跳（30s 一次）+ 客户端定时拉取（30s 一次），最终趋于一致
  - 对于消息中间件，短暂的路由不一致是可容忍的——最多导致 30s 的消息延迟

设计原则 3：极简实现
  - NameServer 核心代码不到 2000 行
  - 不需要 ZAB/Paxos 共识算法
  - 不需要事务日志、快照
  - 不需要 leader 选举
```

源码中可以看到这一设计哲学的直接体现。NameServer 的启动类 `NamesrvStartup` 极其简洁：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/NamesrvStartup.java

public class NamesrvStartup {
    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        // 1. 解析命令行参数和配置文件
        CommandLine commandLine = buildOptions(opts, args);
        // 2. 加载配置：NamesrvConfig + NettyServerConfig
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876); // 默认端口 9876
        // 3. 创建并启动 Controller
        NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);
        // ...
        controller.start();
        return controller;
    }
}
```

注意第 3 步：`controller.start()` 就是全部。没有 ZK 的 `LeaderElection`、`ZabProtocol`、`Snapshot` 等复杂流程。NameServer 启动后就是一个 Netty Server，等待 Broker 注册和客户端查询。

#### 2.1.3 心跳注册机制的源码体现

Broker 向所有 NameServer 发送心跳注册的逻辑在 `BrokerController` 中：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java

// start() 方法中的注册逻辑
public void start() throws Exception {
    // ... 启动各项服务 ...
    this.registerBrokerAll(true, false, true);
    
    // 定时注册：每 30 秒向所有 NameServer 发送心跳
    this.scheduledExecutorService.scheduleAtFixedRate(
        new Runnable() {
            @Override
            public void run() {
                BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
            }
        },
        1000 * 10,    // 初始延迟 10 秒
        1000 * 30,    // 间隔 30 秒
        TimeUnit.MILLISECONDS
    );
}

// registerBrokerAll() 的核心逻辑
public synchronized void registerBrokerAll(
    final boolean checkOrderConfig,
    final boolean unRegister,
    final boolean forceRegister) {
    
    // 构建注册信息
    TopicConfigSerializeWrapper topicConfigWrapper = new TopicConfigSerializeWrapper();
    topicConfigWrapper.setTopicConfigTable(this.topicConfigManager.getTopicConfigTable());
    topicConfigWrapper.setDataVersion(this.topicConfigManager.getDataVersion());
    
    // 获取所有 NameServer 地址
    List<RegisterBrokerResult> registerBrokerResultList = 
        this.brokerOuterAPI.registerBrokerAll(
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerClusterName(),
            this.getRegisterBrokerServerList(),  // 所有 NameServer 地址
            topicConfigWrapper,
            checkOrderConfig,
            unRegister,
            forceRegister,
            this.brokerConfig.isRegisterBrokerEnabled() || this.brokerConfig.isEnableSlaveToMaster()
        );
    // ...
}
```

关键点在 `registerBrokerAll` 这个方法名中的 "All"——它向所有 NameServer 地址列表发送注册请求，而不是只发给一个 leader。NameServer 之间不需要通信，因为每个 NameServer 都能独立接收 Broker 的完整注册信息。

### 2.2 为什么 CommitLog + ConsumeQueue 分离

这是 RocketMQ 存储引擎最核心的设计决策，也是它与 Kafka 最大的区别之一。

#### 2.2.1 Kafka 的设计：每个 Partition 一个日志文件

Kafka 中，每个 Partition 对应一个独立的日志目录，消息按 Partition 分别写入。这种设计的优点是：
- 消费者可以直接按 Partition 顺序读取，天然有序
- 不同 Partition 的写入可以并行

但缺点是：
- 当 Partition 数量增多时，磁盘写入从"少量大文件的顺序写"退化为"大量小文件的随机写"
- 在 HDD 场景下，性能急剧下降

#### 2.2.2 RocketMQ 的设计：所有 Topic 共享一个 CommitLog

RocketMQ 将所有 Topic 的所有消息写入同一个 CommitLog（逻辑上是一个连续的日志，物理上按 1GB 切分文件）。然后通过后台线程 ReputMessageService 将 CommitLog 中的消息分发到各 ConsumeQueue。

```
优势：
  - 无论有多少 Topic 和 Queue，磁盘写入始终是单文件顺序写
  - IO 吞吐量最大化，尤其适合 HDD
  - SSD 场景下也有优势——减少文件句柄、减少 page cache 切换

代价：
  - 消费者不能直接从 CommitLog 顺序读取特定 Queue 的消息
  - 需要一个索引层（ConsumeQueue）来间接定位
  - 消费时需要先查 ConsumeQueue 找到物理偏移量，再从 CommitLog 读取——一次额外的随机读
```

这个设计决策体现了 RocketMQ 对"写多读少"场景的优化——在消息中间件中，写入量通常远大于消费量（因为每条消息只被写入一次但可能被消费多次），所以优先保证写入性能。

#### 2.2.3 源码体现：CommitLog 的写入路径

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/CommitLog.java

public PutMessageResult asyncPutMessage(final MessageExtBrokerInner msg) {
    // 1. 获取当前可写的 MappedFile
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
    
    // 2. 加锁（自旋锁或可重入锁，取决于配置）
    putMessageLock.lock();
    try {
        // 3. 检查文件是否已满，满了就创建新文件
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0, true);
        }
        if (null == mappedFile) {
            return new PutMessageResult(PutMessageStatus.CREATE_MAPPEDFILE_FAILED, null);
        }
        
        // 4. 追加消息到 MappedFile（实际字节写入）
        result = mappedFile.appendMessage(msg, this.appendMessageCallback, this.putMessageContext);
    } finally {
        putMessageLock.unlock();
    }
    
    // 5. 如果文件满了，再次获取
    if (result.getStatus() == AppendMessageStatus.END_OF_FILE) {
        mappedFile = this.mappedFileQueue.getLastMappedFile(0, true);
        // 重试...
    }
    
    // 6. 提交刷盘请求
    CommitLog.this.getMessageStore().getFlushManager().handleDiskFlush(result, putMessageResult, msg);
    
    // 7. HA 同步（主从复制）
    CommitLog.this.getMessageStore().getHAService().putMessage(result);
    
    return putMessageResult;
}
```

注意第 2 步的 `putMessageLock.lock()`——这是一个全局锁，意味着所有 Topic 的消息写入是串行的。这正是"所有消息写入同一个 CommitLog"设计所要求的。RocketMQ 提供了三种锁实现：

```java
// CommitLog 中的锁选择逻辑
public CommitLog(final DefaultMessageStore messageStore) {
    // ...
    if (messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage()) {
        putMessageLock = new PutMessageReentrantLock();
    } else {
        putMessageLock = new PutMessageSpinLock(); // 默认使用自旋锁
    }
    // 自适应锁（根据延迟动态切换）也是可选的
}
```

#### 2.2.4 ConsumeQueue：索引层的精巧设计

ConsumeQueue 是每个 `Topic + QueueId` 对应的一个索引文件，每条索引项固定 20 字节：

```
┌──────────────────┬────────────┬──────────────────┐
│ physicalOffset   │ bodySize   │ tagHash          │
│ (8 bytes)        │ (4 bytes)  │ (8 bytes)        │
└──────────────────┴────────────┴──────────────────┘
  CommitLog中的偏移量    消息体大小    标签的哈希值
```

为什么是 20 字节？因为：
- `physicalOffset` 需要 8 字节（long）来覆盖 CommitLog 的 1GB × N 个文件的空间
- `bodySize` 用 4 字节（int）足够表示单条消息体大小（最大 2GB）
- `tagHash` 用 8 字节（long）存储 tag 的 hashcode，用于服务端过滤

源码中的定义：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/consumequeue/ConsumeQueue.java

public static final int CQ_STORE_UNIT_SIZE = 20; // 20 bytes

// ConsumeQueue 的内存布局
// 每个文件固定大小，默认 300000 * 20 = 6000000 bytes ≈ 5.7MB
// 可以存储 30 万条索引

// ConsumeQueue 文件结构：
// ┌─────────────────────────────────────────────────┐
// │ ConsumeQueue (单个文件)                          │
// ├─────────┬─────────┬─────────┬─────────┬────────┤
// │ Entry0  │ Entry1  │ Entry2  │ ...     │EntryN  │
// │ 20B     │ 20B     │ 20B     │         │ 20B    │
// └─────────┴─────────┴─────────┴─────────┴────────┘
// 每个文件满后，创建新文件
```

### 2.3 为什么 Master-Slave 使用原生 NIO 而非 Netty

RocketMQ 的主从同步（HA 模块）使用的是原生 Java NIO，而不是它自己的 remoting 模块（基于 Netty）。这个决策的原因：

```
原因 1：性能优先
  - Netty 的 RemotingCommand 协议有 header 解析、序列化等开销
  - 主从同步传输的是原始的 CommitLog 字节流，不需要协议解析
  - 原生 NIO 直接传输 byte[]，零开销

原因 2：与业务流量隔离
  - Producer/Consumer 的请求走 remoting 端口（默认 10911）
  - HA 同步走单独的 HA 端口（默认 10912）
  - 两套端口、两套线程模型，互不影响

原因 3：简单可控
  - HA 的传输格式极简：physicOffset(8) + bodySize(4) + body
  - Slave 的 ACK 也极简：slaveAckOffset(8)
  - 不需要 RemotingCommand 的完整协议栈
```

### 2.4 为什么选择长轮询而非 Push 推送

RocketMQ 的消费模型是 `Push` 模式，但底层实现实际上是 `Pull`（拉取）。这是一个经典的"推拉结合"设计：

```
纯 Push 的问题：
  - Broker 需要维护每个 Consumer 的推送队列
  - Consumer 处理不过来时，Broker 的内存会堆积
  - Consumer 不同队列的处理速度不同，Broker 难以调度

纯 Pull 的问题：
  - 消费者不知道消息何时到达，需要不断轮询
  - 空轮询浪费网络和 CPU 资源

长轮询的折中：
  - Consumer 发起 Pull 请求
  - 如果 Broker 有消息，立即返回
  - 如果没有消息，Broker 挂起请求（默认挂起 15 秒）
  - 在挂起期间，一旦有新消息到达，立即唤醒并返回
  - 如果超时仍无消息，返回空结果
```

源码中的长轮询实现：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/processor/PullMessageProcessor.java

// 当没有消息时，将请求挂起
if (messageFilter != null
    && !messageFilter.isMatched(subscription, tagsCode)) {
    // tag 不匹配，跳过
} else {
    // 获取消息
    GetMessageResult getMessageResult =
        messageStore.getMessage(
            group, topic, queueId, offset, subMaxOffset, maxMsgNums
        );
    
    if (getMessageResult == null || getMessageResult.getStatus() != GetMessageStatus.FOUND) {
        // 没有找到消息 —— 挂起请求（长轮询核心逻辑）
        if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
            // 将请求加入 PullRequestHoldService 等待
            this.brokerController.getPullRequestHoldService()
                .suspendPullRequest(topic, queueId, opaque, pullRequest);
        } else {
            // 短轮询：直接返回，让 Consumer 稍后重试
            response.setCode(ResponseCode.PULL_NOT_FOUND);
        }
    }
}
```

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/longpolling/PullRequestHoldService.java

// 挂起的请求存储结构
// key: topic@queueId, value: 链表
private final ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
    new ConcurrentHashMap<>(128);

// 定时检查挂起的请求（每 5 秒或 10 秒）
// 如果对应 Queue 有新消息到达，唤醒挂起的请求
public void run() {
    while (!this.isStopped()) {
        try {
            if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                this.waitForRunning(5 * 1000);  // 5 秒检查一次
            } else {
                this.waitForRunning(this.brokerController
                    .getBrokerConfig().getShortPollingTimeMills()); // 默认 1 秒
            }
            this.checkHoldPullRequest();  // 检查是否有新消息可返回
        } catch (Exception e) { /* ... */ }
    }
}
```

### 2.5 为什么采用顺序写而非随机写

这是存储引擎的底层物理原理。磁盘的顺序写入速度远超随机写入：

```
SSD 顺序写:  ~500-1000 MB/s
SSD 随机写:  ~50-100 MB/s  (差距 10 倍)
HDD 顺序写:  ~100-200 MB/s
HDD 随机写:  ~1-2 MB/s     (差距 100 倍)

RocketMQ 的 CommitLog 就是一个纯粹的顺序追加日志
所有 Topic 的消息都追加到同一个文件中
磁盘 IO 模式始终是顺序写——这是性能的基石
```

---

## 三、模块全景与依赖关系

### 3.1 模块清单与职责

RocketMQ 的源码由多个 Maven 模块组成，每个模块承担明确的职责：

| 模块名 | 职责 | 核心类数量 | 重要性 |
|--------|------|-----------|--------|
| **namesrv** | NameServer，服务发现与路由 | ~15 | 核心 |
| **broker** | 消息 Broker，核心消息处理 | ~200 | 核心 |
| **store** | 存储引擎，CommitLog/ConsumeQueue/IndexFile | ~80 | 核心 |
| **client** | Producer 和 Consumer 客户端 | ~150 | 核心 |
| **remoting** | 基于 Netty 的 RPC 框架 | ~40 | 基础 |
| **common** | 共享数据结构与工具类 | ~100 | 基础 |
| **filter** | 消息过滤（服务端 Filter） | ~30 | 可选 |
| **tools** | 管理命令行工具 | ~50 | 运维 |
| **controller** | 基于 Raft 的自动主从切换 | ~40 | 可选 |
| **proxy** | gRPC 代理层 | ~60 | 可选 |
| **tieredstore** | 分级存储（冷热分离） | ~40 | 可选 |
| **auth** | 认证与授权 | ~30 | 可选 |
| **container** | Broker 容器（单进程多 Broker） | ~30 | 可选 |

### 3.2 模块依赖关系图

```
                    ┌──────────┐
                    │  common  │  ← 所有模块都依赖 common
                    └────┬─────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
          ┌───┴───┐ ┌───┴───┐ ┌───┴───┐
          │remoting│ │  tools │ │  auth │
          └───┬───┘ └───┬───┘ └───────┘
              │          │
     ┌────────┼──────────┤
     │        │          │
     ▼        ▼          ▼
┌─────────┐ ┌─────────┐ ┌──────────┐
│ store   │ │ namesrv │ │  client  │
└────┬────┘ └─────────┘ └────┬────┘
     │                         │
     ▼                         │
┌─────────┐                    │
│ broker  │◄───────────────────┘
└────┬────┘
     │
     ├──► broker 依赖 store（存储引擎）
     ├──► broker 依赖 client（内部使用 MQClientAPI）
     ├──► broker 依赖 remoting（RPC 通信）
     ├──► broker 依赖 common（共享模型）
     ├──► broker 依赖 filter（消息过滤）
     └──► broker 依赖 container（可选，容器化部署）

特殊依赖：
  ├──► controller 依赖 remoting + client（与 NameServer 交互）
  ├──► proxy 依赖 remoting + client（gRPC 到 TCP 转换）
  ├──► tieredstore 依赖 store（扩展存储层）
  └──► container 依赖 broker（嵌入 Broker）
```

### 3.3 各模块源码目录结构

#### 3.3.1 namesrv 模块结构

```
namesrv/
└── src/main/java/org/apache/rocketmq/namesrv/
    ├── NamesrvStartup.java           ← 入口类，启动 NameServer
    ├── NamesrvController.java         ← 核心控制器，组装组件
    ├── processor/
    │   ├── DefaultRequestProcessor.java  ← 处理 22 种请求码
    │   └── ClientRequestProcessor.java   ← 处理 GET_ROUTEINFO_BY_TOPIC
    ├── routeinfo/
    │   ├── RouteInfoManager.java     ← 核心路由管理器（5 大数据结构）
    │   └── BatchUnregistrationService.java ← 批量注销服务
    ├── kvconfig/
    │   └── KVConfigManager.java       ← 命名空间 KV 配置存储
    └── NamesrvStatusManager.java      ← 状态管理
```

#### 3.3.2 broker 模块结构

```
broker/
└── src/main/java/org/apache/rocketmq/broker/
    ├── BrokerStartup.java              ← 入口类，启动 Broker
    ├── BrokerController.java           ← 核心控制器（2823 行）
    ├── BrokerPreOnlineService.java     ← 上线前 HA 握手
    ├── client/
    │   └── RebalanceLockManager.java   ← 顺序消费的队列锁管理
    ├── processor/
    │   ├── SendMessageProcessor.java   ← 处理消息发送
    │   ├── PullMessageProcessor.java   ← 处理消息拉取
    │   ├── PopMessageProcessor.java    ← Pop 消费模式
    │   ├── AckMessageProcessor.java    ← 消息确认
    │   ├── QueryMessageProcessor.java  ← 消息查询
    │   ├── ClientManageProcessor.java  ← 客户端管理（心跳等）
    │   ├── EndTransactionProcessor.java← 事务消息结束
    │   └── ... (共 20+ 个 Processor)
    ├── longpolling/
    │   ├── PullRequestHoldService.java     ← 长轮询挂起服务
    │   └── NotifyMessageArrivingListener.java ← 消息到达通知
    ├── topic/
    │   ├── TopicConfigManager.java     ← Topic 配置管理
    │   └── TopicQueueMappingManager.java ← Topic 队列映射
    ├── offset/
    │   └── ConsumerOffsetManager.java  ← 消费进度管理
    ├── subscription/
    │   └── SubscriptionGroupManager.java ← 订阅组管理
    ├── filter/
    │   └── ConsumerFilterManager.java  ← 消费过滤管理
    ├── loadbalance/
    │   └── TopicQueueMappingCleanService.java
    ├── brokerocker/
    │   └── BrokerockMetrics.java
    ├── plugin/
    │   └── AbstractPluginManager.java
    ├── transaction/
    │   └── queue/
    │       ├── TransactionalMessageBridge.java
    │       └── TransactionalMessageService.java
    ├── schedule/
    │   └── ScheduleMessageService.java ← 定时消息
    ├── outapi/
    │   └── BrokerOuterAPI.java         ← Broker 对外 API（注册到 NameServer 等）
    ├── failover/
    │   └── BrokerReadyService.java
    └── metrics/
        └── BrokerMetricsManager.java
```

#### 3.3.3 store 模块结构

```
store/
└── src/main/java/org/apache/rocketmq/store/
    ├── DefaultMessageStore.java        ← 存储引擎主实现
    ├── CommitLog.java                  ← CommitLog（全消息顺序日志）
    ├── ConsumeQueue.java               ← ConsumeQueue（消费队列索引）
    ├── ConsumeQueueExt.java            ← ConsumeQueue 扩展（存储额外属性）
    ├── IndexFile.java                  ← IndexFile（哈希索引文件）
    ├── MappedFileQueue.java            ← MappedFile 队列管理
    ├── DefaultMappedFile.java          ← MappedFile 实现（mmap 封装）
    ├── MappedFile.java                 ← MappedFile 接口
    ├── StoreCheckpoint.java            ← 存储检查点（崩溃恢复）
    ├── MessageExtEncoder.java          ← 消息编码器（二进制格式）
    ├── MessageExtDecoder.java          ← 消息解码器
    ├── AppendMessageCallback.java      ← 追加消息回调接口
    ├── DefaultAppendMessageCallback.java ← 默认追加回调（实际写入）
    ├── AllocateMappedFileService.java  ← MappedFile 预分配服务
    ├── index/
    │   ├── IndexHeader.java            ← 索引文件头
    │   └── IndexService.java           ← 索引服务
    ├── ha/
    │   ├── DefaultHAService.java       ← HA 服务（主从同步）
    │   ├── DefaultHAClient.java        ← HA 客户端（Slave 端）
    │   └── DefaultHAConnection.java    ← HA 连接（Master 端，每 Slave 一个）
    ├── config/
    │   ├── MessageStoreConfig.java     ← 存储配置
    │   └── FlushDiskType.java          ← 刷盘类型（SYNC/ASYNC）
    ├── flush/
    │   └── FlushManager.java           ← 刷盘管理器
    ├── store/
    │   ├── StoreStatsService.java      ← 存储统计
    │   └── StoreUtil.java
    ├── kv/
    │   └── CompactionService.java      ← KV 压缩服务
    ├── timer/
    │   ├── TimerMessageStore.java      ← 定时消息存储
    │   └── TimerMetrics.java
    ├── queue/
    │   ├── ConsumeQueueInterface.java
    │   ├── CqWrapper.java
    │   └── QueueMetadata.java
    ├── plugin/
    │   ├── MessageStoreFactory.java
    │   └── AbstractPluginManager.java
    └── dledger/
        └── DLedgerCommitLog.java       ← 基于 DLedger 的 CommitLog（Raft 模式）
```

#### 3.3.4 client 模块结构

```
client/
└── src/main/java/org/apache/rocketmq/client/
    ├── producer/
    │   ├── DefaultMQProducer.java          ← Producer 入口
    │   ├── DefaultMQProducerImpl.java      ← Producer 实现
    │   ├── MQProducer.java                 ← Producer 接口
    │   ├── TransactionMQProducer.java      ← 事务消息 Producer
    │   ├── TransactionListener.java        ← 事务监听器
    │   ├── LocalTransactionState.java
    │   ├── RequestFutureHolder.java        ← 请求-响应模式
    │   └── RequestCallback.java
    ├── consumer/
    │   ├── DefaultMQPushConsumer.java      ← Push Consumer 入口
    │   ├── DefaultMQPushConsumerImpl.java  ← Push Consumer 实现
    │   ├── DefaultMQPullConsumer.java      ← Pull Consumer 入口
    │   ├── DefaultMQPullConsumerImpl.java  ← Pull Consumer 实现
    │   ├── ConsumeMessageConcurrentlyService.java ← 并发消费
    │   ├── ConsumeMessageOrderlyService.java       ← 顺序消费
    │   ├── PullMessageService.java         ← 拉取消息服务
    │   ├── RebalanceImpl.java              ← 负载均衡实现
    │   ├── ProcessQueue.java               ← 消息处理队列（本地缓存）
    │   ├── MQConsumer.java                 ← Consumer 接口
    │   ├── MessageQueueListener.java
    │   ├── listener/
    │   │   ├── MessageListenerConcurrently.java
    │   │   └── MessageListenerOrderly.java
    │   ├── rebalance/
    │   │   ├── AllocateMessageQueueStrategy.java
    │   │   ├── AllocateMessageQueueAveragely.java
    │   │   ├── AllocateMessageQueueAveragelyByCircle.java
    │   │   └── AllocateMachineRoomNearby.java
    │   └── store/
    │       ├── OffsetStore.java
    │       ├── RemoteBrokerOffsetStore.java   ← 集群模式进度存储
    │       └── LocalFileOffsetStore.java      ← 广播模式进度存储
    ├── impl/
    │   ├── MQClientManager.java            ← MQClientInstance 管理器
    │   ├── MQClientInstance.java           ← 客户端实例（共享工厂）
    │   ├── MQAdminImpl.java                ← 管理 API 实现
    │   ├── factory/
    │   │   └── MQClientInstance.java
    │   └── ConsumerImpl.java
    ├── admin/
    │   ├── DefaultMQAdminExt.java
    │   └── DefaultMQAdminExtImpl.java
    ├── latency/
    │   ├── MQFaultStrategy.java            ← 故障延迟策略
    │   ├── LatencyFaultTolerance.java
    │   ├── LatencyFaultToleranceImpl.java  ← 故障容错实现
    │   └── FaultItem.java                  ← 故障项
    ├── hook/
    │   ├── SendMessageHook.java
    │   ├── CheckForbiddenContext.java
    │   └── ...
    ├── trace/
    │   ├── AsyncTraceDispatcher.java       ← 异步消息追踪
    │   └── TraceDataEncoder.java
    ├── log/
    │   └── ClientLogger.java
    └── stat/
        └── ConsumerStatsManager.java
```

#### 3.3.5 remoting 模块结构

```
remoting/
└── src/main/java/org/apache/rocketmq/remoting/
    ├── RemotingCommand.java               ← 通信协议封装
    ├── RemotingSerializable.java          ← 序列化接口
    ├── CommandCustomHeader.java           ← 命令头接口
    ├── RemotingHelper.java                ← 工具类
    ├── RemotingClient.java                ← 客户端接口
    ├── RemotingServer.java                ← 服务端接口
    ├── exception/
    │   ├── RemotingConnectException.java
    │   ├── RemotingSendRequestException.java
    │   └── RemotingTimeoutException.java
    ├── netty/
    │   ├── NettyRemotingAbstract.java      ← 共享引擎（客户端/服务端基类）
    │   ├── NettyRemotingServer.java        ← Netty 服务端
    │   ├── NettyRemotingClient.java        ← Netty 客户端
    │   ├── NettyRequestProcessor.java     ← 请求处理器接口
    │   ├── NettyEncoder.java              ← 编码器
    │   ├── NettyDecoder.java              ← 解码器
    │   ├── NettyConnectManageHandler.java  ← 连接管理
    │   ├── NettyServerHandler.java         ← 服务端业务处理器
    │   ├── RequestTask.java               ← 请求任务
    │   ├── ResponseFuture.java            ← 响应 Future
    │   ├── InvokeCallback.java            ← 异步回调接口
    │   └── FileRegionEncoder.java
    ├── protocol/
    │   ├── RemotingCommandType.java        ← 请求/响应类型
    │   ├── RemotingSysResponseCode.java   ← 系统响应码
    │   └── SerializeType.java             ← 序列化类型（JSON/ROCKETMQ）
    ├── common/
    │   └── Pair.java
    └── annotation/
        └── ...
```

---

## 四、NameServer 模块深度解析

NameServer 是 RocketMQ 的服务发现与路由中心。它的设计极简——不像 ZooKeeper 那样维护一个复杂的分布式一致性协议，而是采用"心跳 + 最终一致性"的模式。

### 4.1 启动流程：NamesrvStartup

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/NamesrvStartup.java

public class NamesrvStartup {
    private static InternalLogger log;
    
    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        try {
            // 1. 解析命令行参数
            Options options = buildCommandlineOptions(new Options());
            CommandLine commandLine = ServerUtil.parseCmdLine(
                "mqnamesrv", args, buildOptions(options), new PosixParser());
            
            // 2. 加载配置
            final NamesrvConfig namesrvConfig = new NamesrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            nettyServerConfig.setListenPort(9876); // 默认端口
            
            // 3. 支持命令行覆盖配置
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                MixAll.jsonConfigFile2Object(file, NamesrvConfig.class);
            }
            
            // 4. 创建 Controller
            NamesrvController controller = 
                new NamesrvController(namesrvConfig, nettyServerConfig);
            
            // 5. 初始化
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            
            // 6. 注册 JVM 钩子
            Runtime.getRuntime().addShutdownHook(
                new ShutdownHookThread(log, (Callable<Void>) () -> {
                    controller.shutdown();
                    return null;
                })
            );
            
            // 7. 启动
            controller.start();
            return controller;
        } catch (Throwable e) {
            System.exit(-1);
            return null;
        }
    }
}
```

启动流程的 7 个步骤清晰可见。核心在第 5 步 `controller.initialize()` 和第 7 步 `controller.start()`。

### 4.2 核心控制器：NamesrvController

`NamesrvController` 是 NameServer 的中央组装器，负责将各组件连接在一起：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/NamesrvController.java

public class NamesrvController {
    private final NamesrvConfig namesrvConfig;
    private final NettyServerConfig nettyServerConfig;
    
    // 核心路由管理器
    private final RouteInfoManager routeInfoManager;
    
    // KV 配置管理器
    private final KVConfigManager kvConfigManager;
    
    // 批量注销服务
    private BatchUnregistrationService batchUnregistrationService;
    
    // Netty Remoting Server
    private RemotingServer remotingServer;
    
    // Netty 通信层
    private NettyServerConfig nettyServerConfig;
    
    // 线程池配置
    private ExecutorService defaultExecutor;
    private ExecutorService routeInfoExecutor;
    
    // 请求处理器
    private DefaultRequestProcessor defaultRequestProcessor;
    
    // Broker 存活检测服务
    private BrokerHousekeepingService brokerHousekeepingService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        
        // 创建核心组件
        this.kvConfigManager = new KVConfigManager(this);
        this.routeInfoManager = new RouteInfoManager(this, namesrvConfig);
        
        // Broker 存活检测（监听 Channel 事件）
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        
        // 批量注销服务
        this.batchUnregistrationService = new BatchUnregistrationService(this);
    }

    public boolean initialize() {
        // 1. 加载 KV 配置（从磁盘）
        this.kvConfigManager.load();
        
        // 2. 创建 Netty Remoting Server
        this.remotingServer = new NettyRemotingServer(
            this.nettyServerConfig, this.brokerHousekeepingService);
        
        // 3. 创建线程池
        this.defaultExecutor = ... // 默认线程池
        this.routeInfoExecutor = ... // 路由信息专用线程池
        
        // 4. 注册请求处理器
        this.defaultRequestProcessor = new DefaultRequestProcessor(this);
        
        // 注册各种请求码的处理器
        // DefaultRequestProcessor 处理大部分请求码
        remotingServer.registerProcessor(
            RequestCode.GET_ROUTEINFO_BY_TOPIC,
            new ClientRequestProcessor(this),   // 路由查询走专用处理器
            this.routeInfoExecutor
        );
        
        remotingServer.registerDefaultProcessor(
            this.defaultRequestProcessor,       // 其余走默认处理器
            this.defaultExecutor
        );
        
        // 5. 启动批量注销服务
        this.batchUnregistrationService.start();
        
        return true;
    }

    public void start() {
        this.remotingServer.start();
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.batchUnregistrationService.shutdown();
    }
}
```

### 4.3 路由管理器：RouteInfoManager

这是 NameServer 最核心的类，维护着整个集群的路由信息。它管理着 **5 大数据结构**：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java

public class RouteInfoManager {
    // 默认 Broker 心跳超时时间：120 秒
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 120;
    
    // 读锁/写锁
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    // ============== 五大数据结构 ==============
    
    // 1. topic -> (brokerName -> QueueData)
    // QueueData 包含 brokerName、readQueueNums、writeQueueNums、perm、topicSysFlag
    // 这个结构记录了每个 Topic 在哪些 Broker 上有多少个读/写队列
    private final Map<String, Map<String, QueueData>> topicQueueTable =
        new HashMap<>(1024);
    
    // 2. brokerName -> BrokerData
    // BrokerData 包含 brokerName、clusterName、(brokerId -> brokerAddr) 的 map
    // 这个结构记录了每个 Broker 名称下有哪些实例（master/slave）
    private final Map<String, BrokerData> brokerAddrTable =
        new HashMap<>(128);
    
    // 3. clusterName -> Set<brokerName>
    // 这个结构记录了每个集群包含哪些 Broker
    private final Map<String, Set<String>> clusterAddrTable =
        new HashMap<>(32);
    
    // 4. BrokerAddrInfo -> BrokerLiveInfo
    // BrokerAddrInfo = (clusterName, brokerAddr)
    // BrokerLiveInfo 包含 lastUpdateTimestamp、channel、haServerAddr、dataVersion
    // 这个结构记录了每个 Broker 实例的存活状态
    private final Map<BrokerAddrInfo, BrokerLiveInfo> brokerLiveTable =
        new HashMap<>(128);
    
    // 5. BrokerAddrInfo -> List<filterServerAddr>
    // 这个结构记录了每个 Broker 上有哪些 Filter Server
    private final Map<BrokerAddrInfo, List<String>> filterServerTable =
        new HashMap<>(128);
}
```

这 5 大数据结构之间的关系可以通过下图理解：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          RouteInfoManager                               │
│                                                                         │
│  ┌─── topicQueueTable ─────────────────────────────────────────────┐    │
│  │ TopicA -> {                                                      │    │
│  │   broker-a -> QueueData(readQueueNums=8, writeQueueNums=8)      │    │
│  │   broker-b -> QueueData(readQueueNums=8, writeQueueNums=8)      │    │
│  │ }                                                                │    │
│  │ TopicB -> { broker-a -> QueueData(...) }                        │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
│                             │ brokerName 关联                             │
│  ┌──────────────────────────▼───────────────────────────────────────┐    │
│  │ brokerAddrTable                                                 │    │
│  │ broker-a -> BrokerData {                                         │    │
│  │   clusterName = "DefaultCluster"                                 │    │
│  │   brokerAddrs = {                                               │    │
│  │     0 (master) -> "192.168.1.1:10911"                           │    │
│  │     1 (slave)  -> "192.168.1.2:10911"                           │    │
│  │   }                                                              │    │
│  │ }                                                                │    │
│  │ broker-b -> BrokerData { ... }                                  │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
│                             │ clusterName 关联                            │
│  ┌──────────────────────────▼───────────────────────────────────────┐    │
│  │ clusterAddrTable                                                │    │
│  │ "DefaultCluster" -> [broker-a, broker-b]                        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌─── brokerLiveTable ─────────────────────────────────────────────┐    │
│  │ (cluster, addr) -> BrokerLiveInfo {                             │    │
│  │   lastUpdateTimestamp = 1234567890  ← 心跳更新                    │    │
│  │   channel = NettyChannel(...)                                   │    │
│  │   haServerAddr = "192.168.1.1:10912"                           │    │
│  │ }                                                                │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌─── filterServerTable ───────────────────────────────────────────┐    │
│  │ (cluster, addr) -> ["filter1:10912", "filter2:10913"]           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.4 Broker 注册流程

当 Broker 启动或定时心跳时，会向 NameServer 发送 `REGISTER_BROKER` 请求：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java

public RegisterBrokerResult registerBroker(
    final String clusterName,
    final String brokerAddr,
    final String brokerName,
    final long brokerId,
    final String haServerAddr,
    final long timeoutMillis,
    final long themeFutureDeadlineMills,
    final TopicConfigSerializeWrapper topicConfigWrapper,
    final List<String> filterServerList,
    final boolean enableBrokerTopicSets,
    final TopicQueueMappingInfo topicQueueMappingInfo) {

    // 获取写锁——注册操作需要修改路由数据
    try {
        this.lock.writeLock().lockInterruptibly();
        
        // 1. 更新 clusterAddrTable
        Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
        if (null == brokerNames) {
            brokerNames = new HashSet<>();
            this.clusterAddrTable.put(clusterName, brokerNames);
        }
        brokerNames.add(brokerName);

        // 2. 更新 brokerAddrTable
        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (null == brokerData) {
            brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
            this.brokerAddrTable.put(brokerName, brokerData);
        }
        
        // 关键：如果 brokerId == 0 表示 Master，否则是 Slave
        // brokerAddrs 是 brokerId -> addr 的映射
        String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
        boolean isFirstRegister = (oldAddr == null);  // 首次注册

        // 3. 更新 topicQueueTable
        // 只有 Master（brokerId == 0）才会上报 Topic 配置
        if (null != topicConfigWrapper && brokerId == MixAll.MASTER_ID) {
            // 遍历 Topic 配置，更新 topicQueueTable
            TopicConfigAndMappingSerializeWrapper wrapper = ...;
            for (TopicConfig topicConfig : wrapper.getTopicConfigTable().values()) {
                // 创建或更新 QueueData
                QueueData queueData = new QueueData();
                queueData.setBrokerName(brokerName);
                queueData.setReadQueueNums(topicConfig.getReadQueueNums());
                queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
                queueData.setPerm(topicConfig.getPerm());
                queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());
                
                // 如果是首次注册，加入 QueueData
                // 如果已存在，替换
                Map<String, QueueData> queueDataMap = 
                    this.topicQueueTable.get(topicConfig.getTopicName());
                if (queueDataMap == null) {
                    queueDataMap = new HashMap<>();
                    this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
                }
                queueDataMap.put(brokerName, queueData);
            }
        }

        // 4. 更新 brokerLiveTable
        BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.get(brokerAddrInfo);
        if (null == prevBrokerLiveInfo) {
            log.info("new broker registered, {} {}", clusterName, brokerAddrInfo);
        }
        
        // 更新心跳时间和 Channel
        BrokerLiveInfo brokerLiveInfo = new BrokerLiveInfo(
            System.currentTimeMillis(),       // 当前时间作为心跳时间
            haServerAddr,
            channel,                          // Netty Channel
            dataVersion                       // Broker 数据版本
        );
        this.brokerLiveTable.put(brokerAddrInfo, brokerLiveInfo);

        // 5. 更新 filterServerTable
        if (filterServerList != null && !filterServerList.isEmpty()) {
            this.filterServerTable.put(brokerAddrInfo, filterServerList);
        } else {
            this.filterServerTable.remove(brokerAddrInfo);
        }

        // 6. 返回注册结果（包含 Master 的 HA 地址，Slave 需要知道）
        final RegisterBrokerResult result = new RegisterBrokerResult();
        result.setMasterAddr(brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
        result.setHaServerAddr(haServerAddr);
        // ...
        return result;
        
    } finally {
        this.lock.writeLock().unlock();
    }
}
```

注册流程的 6 个步骤环环相扣：先更新集群表，再更新 Broker 表，然后更新 Topic 队列表，接着更新存活表，最后更新 Filter Server 表。注意每次都使用写锁——因为注册操作会修改路由数据，而路由查询（来自客户端）需要读锁来保证一致性。

### 4.5 Broker 存活检测与注销

NameServer 通过两种机制检测 Broker 存活：

#### 4.5.1 BrokerHousekeepingService：Channel 事件监听

当 Broker 与 NameServer 的 TCP 连接断开时，Netty 会触发 Channel 事件。`BrokerHousekeepingService` 监听这些事件并触发注销：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/BrokerHousekeepingService.java

public class BrokerHousekeepingService implements ChannelEventListener {
    private static InternalLogger log = ...;
    private final NamesrvController namesrvController;

    public BrokerHousekeepingService(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelClose(RemotingChannel channel) {
        // 连接关闭 → 注销 Broker
        this.namesrvController.getRouteInfoManager()
            .unregisterBrokerByChannel(channel);
    }

    @Override
    public void onChannelException(RemotingChannel channel) {
        // 连接异常 → 注销 Broker
        this.namesrvController.getRouteInfoManager()
            .unregisterBrokerByChannel(channel);
    }

    @Override
    public void onChannelIdle(RemotingChannel channel) {
        // 连接空闲 → 不处理（由定时扫描处理超时）
    }

    @Override
    public void onChannelActive(RemotingChannel channel) {
        // 连接激活 → 不处理
    }
}
```

#### 4.5.2 定时扫描：清除超时 Broker

除了 Channel 事件，NameServer 还有一个定时任务扫描 `brokerLiveTable`，清除超过 120 秒未发送心跳的 Broker：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java

// 扫描非活跃 Broker
public void scanNotActiveBroker() {
    try {
        this.lock.writeLock().lockInterruptibly();
        
        Iterator<Map.Entry<BrokerAddrInfo, BrokerLiveInfo>> iterator =
            this.brokerLiveTable.entrySet().iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry = iterator.next();
            long last = entry.getValue().getLastUpdateTimestamp();
            long timeoutMillis = entry.getValue().getHeartbeatTimeoutMillis();
            
            if ((last + timeoutMillis) < System.currentTimeMillis()) {
                // 超过 120 秒未心跳 → 关闭 Channel + 移除
                iterator.remove();
                log.warn("broker is expired, {}", entry.getKey());
                
                // 关闭 Channel
                RemotingHelper.closeChannel(entry.getValue().getChannel());
                
                // 同时移除 filterServerTable
                this.filterServerTable.remove(entry.getKey());
                
                // 注销 Broker 的路由信息
                this.batchUnregistrationService.addUnregisterBrokerEvent(
                    entry.getKey().getClusterName(), 
                    entry.getKey().getBrokerAddr()
                );
            }
        }
    } finally {
        this.lock.writeLock().unlock();
    }
}
```

### 4.6 路由查询：客户端如何获取路由信息

客户端通过 `GET_ROUTEINFO_BY_TOPIC` 请求获取 Topic 的路由信息，这个请求由 `ClientRequestProcessor` 处理：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/processor/ClientRequestProcessor.java

public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
    // 解析 Topic 名称
    String topic = request.getExtFields().get(MixAll.TOPIC_ROUTE_INFO);
    
    // 查询路由数据
    TopicRouteData topicRouteData = 
        this.namesrvController.getRouteInfoManager()
            .pickupTopicRouteData(topic);
    
    if (topicRouteData != null) {
        // 序列化为 JSON
        byte[] content = topicRouteData.encode();
        
        response.setBody(content);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    } else {
        // Topic 不存在
        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        return response;
    }
}
```

`pickupTopicRouteData` 方法的实现：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java

public TopicRouteData pickupTopicRouteData(final String topic) {
    TopicRouteData topicRouteData = new TopicRouteData();
    
    try {
        this.lock.readLock().lockInterruptibly();
        
        // 1. 从 topicQueueTable 获取该 Topic 的队列信息
        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
        if (queueDataMap == null || queueDataMap.isEmpty()) {
            return null; // Topic 不存在
        }
        
        // 深拷贝 QueueData 列表
        topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
        
        // 2. 从 brokerAddrTable 获取涉及的 Broker 信息
        Map<String, BrokerData> brokerDataMap = new HashMap<>();
        for (QueueData queueData : queueDataMap.values()) {
            BrokerData brokerData = this.brokerAddrTable.get(queueData.getBrokerName());
            if (brokerData == null) {
                continue;
            }
            brokerDataMap.put(brokerData.getBrokerName(), brokerData.clone());
        }
        topicRouteData.setBrokerDatas(new ArrayList<>(brokerDataMap.values()));
        
        // 3. 获取 Filter Server 列表
        // 4. 获取 Topic 队列映射信息
        // ...
        
        return topicRouteData;
        
    } finally {
        this.lock.readLock().unlock();
    }
}
```

### 4.7 DefaultRequestProcessor：22 种请求码的处理

`DefaultRequestProcessor` 是 NameServer 的默认请求处理器，处理除路由查询外的所有请求：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/processor/DefaultRequestProcessor.java

public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
    switch (request.getCode()) {
        case RequestCode.REGISTER_BROKER:         // 103: Broker 注册
            return this.registerBroker(ctx, request);
        case RequestCode.UNREGISTER_BROKER:       // 104: Broker 注销
            return this.unregisterBroker(ctx, request);
        case RequestCode.GET_ROUTEINFO_BY_TOPIC:  // 105: 查询路由（被 ClientRequestProcessor 拦截）
            return this.getRouteInfoByTopic(ctx, request);
        case RequestCode.GET_ROUTEINFO_BY_DATA:   // 106: 查询 Broker 数据
            return this.getRouteInfoByData(ctx, request);
        case RequestCode.QUERY_BROKER_OFFSET:    // 107: 查询 Broker 偏移量
            return this.queryBrokerOffset(ctx, request);
        case RequestCode.QUERY_TOPIC_OFFSET:      // 108: 查询 Topic 偏移量
            return this.queryTopicOffset(ctx, request);
        case RequestCode.DELETE_KV_CONFIG:         // 109: 删除 KV 配置
            return this.deleteKVConfig(ctx, request);
        case RequestCode.PUT_KV_CONFIG:           // 100: 设置 KV 配置
            return this.putKVConfig(ctx, request);
        case RequestCode.GET_KV_CONFIG:           // 101: 查询 KV 配置
            return this.getKVConfig(ctx, request);
        case RequestCode.GET_KVLIST_BY_NAMESPACE: // 102: 按命名空间查询 KV 列表
            return this.getKVListByNamespace(ctx, request);
        case RequestCode.BROKER_HEARTBEAT:        // 904: Broker 心跳
            return this.brokerHeartbeat(ctx, request);
        case RequestCode.LOCK_BATCH_MQ:           // 313: 批量锁定 MQ（顺序消费）
            return this.lockBatchMQ(ctx, request);
        case RequestCode.UNLOCK_BATCH_MQ:         // 314: 批量解锁 MQ
            return this.unlockBatchMQ(ctx, request);
        case RequestCode.UPDATE_AND_GET_BROKER_REGISTER_INFO: // 318
            return this.updateAndGetBrokerRegisterInfo(ctx, request);
        case RequestCode.QUERY_DATA_VERSION:     // 322: 查询数据版本
            return this.queryBrokerTopicConfig(ctx, request);
        case RequestCode.REGISTER_TOPIC_IN_NAMESRV: // 218: 在 NameServer 注册 Topic
            return this.registerTopicToNameServer(ctx, request);
        case RequestCode.GET_NAMESRV_KV_CONFIG:  // 319
            return this.getNamesrvConfig(ctx, request);
        case RequestCode.PUT_NAMESRV_KV_CONFIG:   // 321
            return this.putNamesrvConfig(ctx, request);
        case RequestCode.QUERY_TOPICS_BY_CLUSTER: // 224: 查询集群下所有 Topic
            return this.queryTopicsByCluster(ctx, request);
        case RequestCode.GET_SYSTEM_TOPIC_LIST:   // 305: 查询系统 Topic 列表
            return this.getSystemTopicList(ctx, request);
        case RequestCode.GET_TOPIC_BY_NAME:       // 312: 按 Topic 名查询
            return this.getTopicByName(ctx, request);
        default:
            return unknownCmd(ctx, request);
    }
}
```

从请求码的列表中可以看出 NameServer 承担的职责范围：
- 路由管理（注册、注销、查询）——核心职责
- KV 配置管理——辅助职责
- 队列锁定（顺序消费）——辅助职责
- Topic 管理——辅助职责

### 4.8 BatchUnregistrationService：批量注销

当多个 Broker 同时断开时（例如网络分区），逐个注销会频繁加锁解锁，影响性能。`BatchUnregistrationService` 将注销请求积攒后批量处理：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/BatchUnregistrationService.java

public class BatchUnregistrationService {
    private final NamesrvController namesrvController;
    
    // 待注销队列（drain pattern）
    private final LinkedBlockingQueue<UnregisterBrokerEvent> queue = 
        new LinkedBlockingQueue<>();
    
    // 工作线程
    private Thread thread;
    
    public void start() {
        thread = new Thread(() -> {
            while (!stopped) {
                try {
                    // 从队列中取出所有待注销事件
                    List<UnregisterBrokerEvent> events = new ArrayList<>();
                    UnregisterBrokerEvent first = queue.take(); // 阻塞等待
                    events.add(first);
                    queue.drainTo(events); // 一次性排空队列
                    
                    // 批量处理
                    for (UnregisterBrokerEvent event : events) {
                        namesrvController.getRouteInfoManager()
                            .unregisterBroker(
                                event.getClusterName(),
                                event.getBrokerAddr()
                            );
                    }
                } catch (Exception e) { /* ... */ }
            }
        }, "batchUnregisterService");
        thread.start();
    }
}
```

### 4.9 KVConfigManager：命名空间 KV 配置

NameServer 还提供了一个简单的 KV 配置存储，按命名空间组织：

```java
// 源码路径: namesrv/src/main/java/org/apache/rocketmq/namesrv/kvconfig/KVConfigManager.java

public class KVConfigManager {
    // 命名空间 -> (key -> value)
    private final HashMap<String/* namespace */, HashMap<String/* key */, String/* value */>> configTable;
    
    public void putKVConfig(final String namespace, final String key, final String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            
            HashMap<String, String> kvTable = this.configTable.get(namespace);
            if (null == kvTable) {
                kvTable = new HashMap<>();
                this.configTable.put(namespace, kvTable);
            }
            
            kvTable.put(key, value);
        } finally {
            this.lock.writeLock().unlock();
        }
        
        // 持久化到磁盘
        this.persist();
    }
    
    // 持久化到 ${user.home}/namesrv/kvConfig.json
    public void persist() {
        // 将 configTable 序列化为 JSON，写入文件
    }
    
    // 启动时从磁盘加载
    public void load() {
        // 读取 kvConfig.json，反序列化为 configTable
    }
}
```

### 4.10 NameServer 小结

NameServer 的设计哲学可以总结为：

```
┌──────────────────────────────────────────────────────────────────────┐
│                    NameServer 设计哲学                                │
│                                                                      │
│  1. 无状态 (Stateless)                                               │
│     - 多个 NameServer 实例之间不通信                                   │
│     - 每个实例独立维护路由数据                                         │
│     - 数据通过 Broker 心跳填充，最终一致                                 │
│                                                                      │
│  2. 最终一致性 (Eventual Consistency)                                 │
│     - 不同 NameServer 的数据可能短暂不一致                               │
│     - 但通过心跳（30s）+ 客户端拉取（30s）最终收敛                       │
│     - 客户端有容错机制：发送失败时更新路由表                              │
│                                                                      │
│  3. 极简实现 (Simplicity)                                            │
│     - 核心代码不到 2000 行                                            │
│     - 不需要共识算法                                                   │
│     - 不需要 leader 选举                                               │
│     - 不需要事务日志/快照                                               │
│                                                                      │
│  4. 读写分离 (ReadWrite Separation)                                   │
│     - 读操作使用读锁（允许多线程并发读）                                  │
│     - 写操作使用写锁（注册/注销时互斥）                                   │
│     - 使用 ReentrantReadWriteLock 保证可见性                            │
│                                                                      │
│  5. 两级检测 (Two-Level Detection)                                    │
│     - 第一级：BrokerHousekeepingService 监听 Channel 事件（毫秒级）       │
│     - 第二级：scanNotActiveBroker 定时扫描（120 秒超时）                 │
│     - 双重保障，不遗漏                                                  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 五、Broker 模块深度解析

Broker 是 RocketMQ 的核心——消息的接收、存储、分发都在这里完成。`BrokerController` 是整个 Broker 的中央编排器，它将配置管理、消息存储、RPC 处理、定时任务等组件组装在一起。

### 5.1 启动流程：BrokerStartup

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerStartup.java

public class BrokerStartup {
    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        if (null == controller) {
            System.exit(-1);
        }
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        
        // 注册 Shutdown 钩子
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> controller.shutdown()));
        
        controller.start();
        return controller;
    }

    public static BrokerController createBrokerController(String[] args) {
        // 1. 解析命令行参数
        // 2. 加载配置：BrokerConfig + MessageStoreConfig + NettyServerConfig + NettyClientConfig
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, ...);
        
        final BrokerConfig brokerConfig = new BrokerConfig();
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        
        // 默认端口 10911
        nettyServerConfig.setListenPort(10911);
        
        // 如果是 Master，使用 ASYNC_FLUSH；如果是 Slave，也用 ASYNC_FLUSH
        // 刷盘方式可通过配置修改
        
        // 3. 创建 BrokerController
        final BrokerController controller = new BrokerController(
            brokerConfig, messageStoreConfig, nettyServerConfig, nettyClientConfig);
        
        controller.getConfiguration().registerConfig(...);
        return controller;
    }
}
```

### 5.2 BrokerController：中央编排器

`BrokerController` 是 Broker 中最大的类（2823+ 行），负责所有组件的创建、初始化、启动和关闭。

#### 5.2.1 构造函数：组件组装

构造函数负责创建所有组件实例：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java

public BrokerController(
    final BrokerConfig brokerConfig,
    final MessageStoreConfig messageStoreConfig,
    final NettyServerConfig nettyServerConfig,
    final NettyClientConfig nettyClientConfig) {
    
    // 配置对象
    this.brokerConfig = brokerConfig;
    this.messageStoreConfig = messageStoreConfig;
    this.nettyServerConfig = nettyServerConfig;
    this.nettyClientConfig = nettyClientConfig;
    
    // 消息存储配置
    this.messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
    
    // ==================== 配置管理器 ====================
    this.topicConfigManager = new TopicConfigManager(this);
    this.topicQueueMappingManager = new TopicQueueMappingManager(this);
    this.consumerOffsetManager = new ConsumerOffsetManager(this);
    this.subscriptionGroupManager = new SubscriptionGroupManager(this);
    this.consumerFilterManager = new ConsumerFilterManager(this);
    this.consumerOrderInfoManager = new ConsumerOrderInfoManager(this);
    
    // ==================== 存储相关 ====================
    // 注意：messageStore 在 initialize() 中创建，因为需要先加载恢复数据
    
    // ==================== 处理器 ====================
    this.sendMessageProcessor = new SendMessageProcessor(this);
    this.pullMessageProcessor = new PullMessageProcessor(this);
    this.popMessageProcessor = new PopMessageProcessor(this);
    this.ackMessageProcessor = new AckMessageProcessor(this);
    this.queryMessageProcessor = new QueryMessageProcessor(this);
    this.clientManageProcessor = new ClientManageProcessor(this);
    this.endTransactionProcessor = new EndTransactionProcessor(this);
    this.replyMessageProcessor = new ReplyMessageProcessor(this);
    this.changeInvisibleTimeProcessor = new ChangeInvisibleTimeProcessor(this);
    
    // ==================== RPC 相关 ====================
    this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
    
    // ==================== 线程池 ====================
    this.sendMessageExecutor = ... // 消息发送线程池
    this.pullMessageExecutor = ...  // 消息拉取线程池
    this.ackMessageExecutor = ...  // 消息确认线程池
    this.queryMessageExecutor = ...// 消息查询线程池
    this.clientManageExecutor = ...// 客户端管理线程池
    this.heartbeatExecutor = ...   // 心跳线程池
    this.endTransactionExecutor = ...// 事务结束线程池
    this.replyMessageExecutor = ...  // 回复消息线程池
    this.adminBrokerExecutor = ...  // 管理操作线程池
    
    // ==================== 长轮询 ====================
    this.pullRequestHoldService = new PullRequestHoldService(this);
    
    // ==================== 锁管理 ====================
    this.rebalanceLockManager = new RebalanceLockManager();
    
    // ==================== 外部 API ====================
    this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
    
    // ==================== 上线前握手 ====================
    this.brokerPreOnlineService = new BrokerPreOnlineService(this);
}
```

从构造函数可以清晰看到 Broker 的组件全景：

```
┌──────────────────────────────────────────────────────────────────┐
│                    BrokerController                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ 配置管理器                                                 │     │
│  │  ├── TopicConfigManager         (Topic 配置)             │     │
│  │  ├── TopicQueueMappingManager   (队列映射)               │     │
│  │  ├── ConsumerOffsetManager      (消费进度)               │     │
│  │  ├── SubscriptionGroupManager   (订阅组)                │     │
│  │  ├── ConsumerFilterManager      (消费过滤)               │     │
│  │  └── ConsumerOrderInfoManager   (顺序消费信息)            │     │
│  └─────────────────────────────────────────────────────────┘     │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ 请求处理器                                                 │     │
│  │  ├── SendMessageProcessor       (消息发送)              │     │
│  │  ├── PullMessageProcessor       (消息拉取)              │     │
│  │  ├── PopMessageProcessor        (Pop 消费)              │     │
│  │  ├── AckMessageProcessor        (消息确认)              │     │
│  │  ├── QueryMessageProcessor      (消息查询)              │     │
│  │  ├── ClientManageProcessor      (客户端管理)            │     │
│  │  ├── EndTransactionProcessor    (事务结束)              │     │
│  │  ├── ReplyMessageProcessor      (回复消息)              │     │
│  │  └── ChangeInvisibleTimeProcessor (修改不可见时间)      │     │
│  └─────────────────────────────────────────────────────────┘     │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ 存储引擎                                                   │     │
│  │  └── DefaultMessageStore (在 initialize() 中创建)        │     │
│  │      ├── CommitLog                                        │     │
│  │      ├── ConsumeQueue                                      │     │
│  │      ├── IndexFile                                         │     │
│  │      └── HAService                                         │     │
│  └─────────────────────────────────────────────────────────┘     │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ 线程池 (每个处理器对应一个线程池)                           │     │
│  │  ├── sendMessageExecutor                                  │     │
│  │  ├── pullMessageExecutor                                   │     │
│  │  ├── ackMessageExecutor                                    │     │
│  │  ├── queryMessageExecutor                                  │     │
│  │  ├── clientManageExecutor                                  │     │
│  │  ├── heartbeatExecutor                                     │     │
│  │  ├── endTransactionExecutor                                │     │
│  │  ├── adminBrokerExecutor                                   │     │
│  │  └── ...                                                   │     │
│  └─────────────────────────────────────────────────────────┘     │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ 其他组件                                                   │     │
│  │  ├── PullRequestHoldService  (长轮询挂起)                 │     │
│  │  ├── BrokerOuterAPI          (对外 RPC)                   │     │
│  │  ├── BrokerPreOnlineService  (上线前握手)                 │     │
│  │  ├── RebalanceLockManager    (顺序消费锁)                 │     │
│  │  └── ScheduledExecutorService (定时任务)                 │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

#### 5.2.2 initialize() 方法

`initialize()` 方法负责加载持久化数据、创建存储引擎、初始化管道：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java

public boolean initialize() throws CloneNotSupportedException {
    // ==================== 1. 加载配置从磁盘 ====================
    boolean result = this.topicConfigManager.load();
    result = result && this.subscriptionGroupManager.load();
    result = result && this.consumerOffsetManager.load();
    result = result && this.consumerFilterManager.load();
    result = result && this.consumerOrderInfoManager.load();
    
    if (!result) {
        return false;
    }
    
    // ==================== 2. 创建消息存储引擎 ====================
    if (messageStoreConfig.isEnableDLedgerCommitLog()) {
        // 使用 DLedger (Raft) 模式
        this.messageStore = new DLedgerCommitLog(...);
    } else if (messageStoreConfig.isEnableControllerMode()) {
        // 使用 Controller 模式（自动主从切换）
        this.messageStore = ...;
    } else {
        // 默认模式
        this.messageStore = new DefaultMessageStore(
            messageStoreConfig,
            this.brokerStatsManager,
            this.messageArrivingListener,
            this.brokerConfig
        );
    }
    
    // 加载存储引擎（恢复 CommitLog、ConsumeQueue 等）
    boolean loadResult = this.messageStore.load();
    if (!loadResult) {
        return false;
    }
    
    // ==================== 3. 创建 Netty Remoting Server ====================
    this.remotingServer = new NettyRemotingServer(
        this.nettyServerConfig, this.clientHousekeepingService);
    
    // Fast Remoting Server（用于快速通道）
    this.fastRemotingServer = new NettyRemotingServer(
        this.nettyServerConfig.copyWithOtherPort(
            this.nettyServerConfig.getListenPort() - 0xFFF),
        this.clientHousekeepingService
    );
    
    // ==================== 4. 注册请求处理器 ====================
    this.registerProcessor();
    
    // ==================== 5. 初始化定时任务 ====================
    this.initialScheduling();
    
    // ==================== 6. 初始化事务相关 ====================
    this.initialTransaction();
    
    // ==================== 7. 初始化 ACL（权限控制） ====================
    this.initialAcl();
    
    // ==================== 8. 初始化 RPC Hook ====================
    this.initialRpcHooks();
    
    // ==================== 9. 初始化插件 ====================
    // ...
    
    return true;
}
```

`initialize()` 方法的 9 个步骤是整个 Broker 的骨架：
1. 配置加载——从磁盘恢复 Topic 配置、订阅组、消费进度等
2. 存储引擎——创建并加载 `DefaultMessageStore`，恢复 CommitLog 和 ConsumeQueue
3. RPC 服务——创建主 RemotingServer 和 Fast RemotingServer
4. 处理器注册——将请求码映射到对应的处理器和线程池
5. 定时任务——注册心跳、消费进度持久化、消费过滤持久化等
6. 事务——初始化事务消息服务
7. ACL——初始化权限控制
8. RPC Hook——初始化请求/响应钩子
9. 插件——初始化扩展插件

#### 5.2.3 registerProcessor() 方法

这是 Broker 中非常重要的方法，它将请求码映射到对应的处理器和线程池：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java

private void registerProcessor() {
    // ========== 1. SendMessageProcessor ==========
    sendMessageProcessor.registerSendMessageHook(...);
    sendMessageProcessor.registerEndTransactionHook(...);
    
    // 在主 RemotingServer 上注册
    this.remotingServer.registerProcessor(
        RequestCode.SEND_MESSAGE,           // 10
        sendMessageProcessor,
        this.sendMessageExecutor
    );
    this.remotingServer.registerProcessor(
        RequestCode.SEND_MESSAGE_V2,        // 310
        sendMessageProcessor,
        this.sendMessageExecutor
    );
    this.remotingServer.registerProcessor(
        RequestCode.SEND_BATCH_MESSAGE,     // 320
        sendMessageProcessor,
        this.sendMessageExecutor
    );
    this.remotingServer.registerProcessor(
        RequestCode.SEND_REPLY_MESSAGE,     // 324
        replyMessageProcessor,
        this.replyMessageExecutor
    );
    this.remotingServer.registerProcessor(
        RequestCode.SEND_REPLY_MESSAGE_V2,  // 325
        replyMessageProcessor,
        this.replyMessageExecutor
    );
    
    // 在 Fast RemotingServer 上也注册（同样的映射）
    this.fastRemotingServer.registerProcessor(
        RequestCode.SEND_MESSAGE,
        sendMessageProcessor,
        this.sendMessageExecutor
    );
    // ... 其他请求码同理
    
    // ========== 2. PullMessageProcessor ==========
    this.remotingServer.registerProcessor(
        RequestCode.PULL_MESSAGE,           // 11
        pullMessageProcessor,
        this.pullMessageExecutor
    );
    this.fastRemotingServer.registerProcessor(
        RequestCode.PULL_MESSAGE,
        pullMessageProcessor,
        this.pullMessageExecutor
    );
    
    // ========== 3. PopMessageProcessor ==========
    this.remotingServer.registerProcessor(
        RequestCode.POP_MESSAGE,            // 200050
        popMessageProcessor,
        this.popMessageExecutor
    );
    this.fastRemotingServer.registerProcessor(
        RequestCode.POP_MESSAGE,
        popMessageProcessor,
        this.popMessageExecutor
    );
    
    // ========== 4. AckMessageProcessor ==========
    this.remotingServer.registerProcessor(
        RequestCode.ACK_MESSAGE,            // 200051
        ackMessageProcessor,
        this.ackMessageExecutor
    );
    
    // ========== 5. QueryMessageProcessor ==========
    this.remotingServer.registerProcessor(
        RequestCode.QUERY_MESSAGE,          // 12
        queryMessageProcessor,
        this.queryMessageExecutor
    );
    this.remotingServer.registerProcessor(
        RequestCode.VIEW_MESSAGE_BY_ID,     // 316
        queryMessageProcessor,
        this.queryMessageExecutor
    );
    
    // ========== 6. ClientManageProcessor ==========
    this.remotingServer.registerProcessor(
        RequestCode.HEART_BEAT,             // 34
        clientManageProcessor,
        this.heartbeatExecutor
    );
    this.remotingServer.registerProcessor(
        RequestCode.UNREGISTER_CLIENT,      // 35
        clientManageProcessor,
        this.clientManageExecutor
    );
    this.remotingServer.registerProcessor(
        RequestCode.CHECK_CLIENT_CONFIG,    // 25
        clientManageProcessor,
        this.clientManageExecutor
    );
    
    // ========== 7. EndTransactionProcessor ==========
    this.remotingServer.registerProcessor(
        RequestCode.END_TRANSACTION,        // 37
        endTransactionProcessor,
        this.endTransactionExecutor
    );
    
    // ========== 8. ChangeInvisibleTimeProcessor ==========
    this.remotingServer.registerProcessor(
        RequestCode.CHANGE_MESSAGE_INVISIBLETIME, // 403
        changeInvisibleTimeProcessor,
        this.ackMessageExecutor
    );
    
    // ========== 9. AdminBrokerProcessor (默认处理器) ==========
    AdminBrokerProcessor adminBrokerProcessor = new AdminBrokerProcessor(this);
    this.remotingServer.registerDefaultProcessor(
        adminBrokerProcessor,
        this.adminBrokerExecutor
    );
}
```

关键设计点：每个请求处理器绑定一个独立的线程池。这意味着消息发送、消息拉取、消息查询等操作在不同的线程池中执行，互不阻塞。这是 Netty 中"线程隔离"模式的经典应用。

#### 5.2.4 start() 方法

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java

public void start() throws Exception {
    // 1. 启动 BrokerOuterAPI（Netty 客户端，用于与 NameServer 通信）
    this.brokerOuterAPI.start();
    
    // 2. 启动核心服务
    this.startBasicService();
    
    // 3. 如果需要上线前握手（Slave 作为 Master 时）
    if (this.brokerConfig.isEnableSlaveActingMaster() 
        && this.brokerConfig.isEnableControllerMode()) {
        this.brokerPreOnlineService.start();
        // 等待握手完成
        this.brokerPreOnlineService.awaitReady();
    }
    
    // 4. 注册到所有 NameServer
    this.registerBrokerAll(true, false, true);
    
    // 5. 启动定时注册任务（每 30 秒）
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.registerBrokerAll(true, false, false),
        1000 * 10,
        Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)),
        TimeUnit.MILLISECONDS
    );
    
    // 6. 如果开启了 Broker Stats
    if (this.brokerConfig.isEnableStatsPlugin()) {
        this.brokerStatsManager.start();
    }
    
    // 7. 如果开启了 Controller 模式
    if (this.brokerConfig.isEnableControllerMode()) {
        this.brokerControllerGroupService.start();
    }
}

private void startBasicService() {
    // 启动消息存储引擎
    this.messageStore.start();
    
    // 启动 Netty Remoting Server（主端口）
    this.remotingServer.start();
    
    // 启动 Fast Remoting Server（VIP 通道端口 = 主端口 - 2）
    this.fastRemotingServer.start();
    
    // 启动长轮询挂起服务
    this.pullRequestHoldService.start();
    
    // 启动存储统计
    if (this.messageStoreConfig.isEnableScheduleStat()) {
        this.scheduleMessageService.start();
    }
    
    // 启动事务消息恢复
    this.transactionalMessageService.start();
    
    // 启动 Broker Pre-online 服务
    if (this.brokerConfig.isEnableSlaveActingMaster()) {
        this.brokerPreOnlineService.start();
    }
}
```

### 5.3 核心处理器详解

#### 5.3.1 SendMessageProcessor：消息发送处理器

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/processor/SendMessageProcessor.java

public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
    SendMessageContext sendMessageContext = null;
    SendMessageRequestHeader requestHeader = decodeRequestHeader(request);
    
    if (requestHeader == null) {
        return errorResponse(ResponseCode.MESSAGE_ILLEGAL);
    }
    
    // 获取 Topic 配置和订阅组配置
    TopicConfig topicConfig = this.brokerController.getTopicConfigManager()
        .selectTopicConfig(requestHeader.getTopic());
    
    // ==================== 前置检查 ====================
    
    // 检查 Topic 是否存在
    if (topicConfig == null) {
        // 如果开启了自动创建 Topic
        if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
            topicConfig = this.brokerController.getTopicConfigManager()
                .createTopicInSendMessageMethod(
                    requestHeader.getTopic(),
                    requestHeader.getDefaultTopic(),
                    this.brokerController.getBrokerConfig().getTopicQueueNums()
                );
        }
    }
    
    // 检查 QueueId 是否合法
    int queueIdInt = requestHeader.getQueueId();
    if (queueIdInt < 0 || queueIdInt >= topicConfig.getWriteQueueNums()) {
        // 自动分配 QueueId
        queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
    }
    
    // 检查队列写权限
    if (!PermName.isWriteable(topicConfig.getPerm())) {
        return errorResponse(ResponseCode.NO_PERMISSION);
    }
    
    // 检查消息体大小
    if (msg.getBody().length > this.brokerController.getMessageStoreConfig()
        .getMaxMessageSize()) {
        return errorResponse(ResponseCode.MESSAGE_ILLEGAL);
    }
    
    // ==================== 处理消息 ====================
    
    // 检查是否是事务消息
    if (msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED) != null) {
        // 事务消息走特殊处理
        return this.handlePutMessageResult(
            this.putTransactionalMessage(topicConfig, queueIdInt, msg),
            request, msg, ...);
    } else {
        // 普通消息
        // 构造 MessageExtBrokerInner
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);
        msgInner.setBody(msg.getBody());
        // ... 设置各种属性
        
        // 调用存储引擎写入消息
        PutMessageResult putMessageResult = 
            this.brokerController.getMessageStore().putMessage(msgInner);
        
        // 处理写入结果
        return handlePutMessageResult(putMessageResult, request, ...);
    }
}
```

消息写入的完整流程：参数校验 → 自动创建 Topic → 队列选择 → 消息封装 → 存储引擎写入 → 结果处理。

#### 5.3.2 PullMessageProcessor：消息拉取处理器

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/processor/PullMessageProcessor.java

public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) {
    PullMessageRequestHeader requestHeader = ...;
    
    // ==================== 前置检查 ====================
    
    // 1. 检查 Broker 是否有读权限
    if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
        return errorResponse(ResponseCode.NO_PERMISSION);
    }
    
    // 2. 检查订阅组配置
    SubscriptionGroupConfig groupConfig = 
        this.brokerController.getSubscriptionGroupManager()
            .findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
    if (groupConfig == null) {
        return errorResponse(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
    }
    
    // 3. 检查是否允许消费
    if (!PermName.isReadable(groupConfig.getPerm())) {
        return errorResponse(ResponseCode.NO_PERMISSION);
    }
    
    // 4. 检查 Topic 配置
    TopicConfig topicConfig = ...;
    
    // 5. 检查消费进度是否合法（不能超过最大偏移量）
    long maxOffset = this.brokerController.getMessageStore()
        .getMaxOffsetInQueue(topic, queueId);
    if (offset > maxOffset) {
        return errorResponse(ResponseCode.PULL_OFFSET_MOVED);
    }
    
    // ==================== 获取消息 ====================
    
    // 消息过滤
    MessageFilter messageFilter = null;
    if (requestHeader.getSubscription() != null) {
        // 按 tag 或表达式过滤
        messageFilter = new ExpressionMessageFilter(
            subscription, consumerFilterData, 
            this.brokerController.getConsumerFilterManager()
        );
    }
    
    // 从存储引擎拉取消息
    GetMessageResult getMessageResult = 
        this.brokerController.getMessageStore().getMessage(
            requestHeader.getConsumerGroup(),
            requestHeader.getTopic(),
            requestHeader.getQueueId(),
            requestHeader.getQueueOffset(),
            requestHeader.getMaxMsgNums(),
            messageFilter
        );
    
    // ==================== 处理结果 ====================
    
    if (getMessageResult.getStatus() == GetMessageStatus.FOUND) {
        // 有消息
        response.setCode(ResponseCode.SUCCESS);
        
        // 设置 nextBeginOffset（消费下一条的偏移量）
        responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
        responseHeader.setMinOffset(getMessageResult.getMinOffset());
        responseHeader.setMaxOffset(getMessageResult.getMaxOffset());
        
        // 构建响应 body（消息列表）
        if (getMessageResult.getMessageBufferList() != null) {
            // 使用 ByteBuf 直接传输，避免拷贝
            response.setBodyList(getMessageResult.getMessageBufferList());
        } else {
            response.setBody(getMessageResult.getMessageBinary());
        }
        
    } else if (getMessageResult.getStatus() == GetMessageStatus.NO_MESSAGE_IN_QUEUE) {
        // 队列中没有消息 —— 长轮询挂起
        if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
            // 挂起 PullRequest
            this.brokerController.getPullRequestHoldService()
                .suspendPullRequest(topic, queueId, opaque, pullRequest);
        } else {
            response.setCode(ResponseCode.PULL_NOT_FOUND);
        }
    }
    
    return response;
}
```

### 5.4 配置管理器

Broker 有多个配置管理器，负责管理不同的持久化配置：

#### 5.4.1 TopicConfigManager

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/topic/TopicConfigManager.java

public class TopicConfigManager extends ConfigManager {
    // topic -> TopicConfig
    private final ConcurrentMap<String, TopicConfig> topicConfigTable =
        new ConcurrentHashMap<>(1024);
    
    // 数据版本（用于增量同步）
    private final DataVersion dataVersion = new DataVersion();
    
    // 默认 Topic 配置（用于 autoCreateTopicEnable）
    private transient TopicConfig defaultTopicQueueNumsConfig;
    
    public TopicConfig createTopicInSendMessageMethod(
        final String topic, final String defaultTopic, final int queueNums) {
        // 自动创建 Topic
        TopicConfig topicConfig = new TopicConfig(topic);
        topicConfig.setReadQueueNums(queueNums);
        topicConfig.setWriteQueueNums(queueNums);
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        
        topicConfigTable.put(topic, topicConfig);
        dataVersion.nextVersion();
        this.persist();
        
        return topicConfig;
    }
    
    // 持久化：序列化为 JSON，写入 ${storeRoot}/config/topics.json
    @Override
    public String encode() {
        return encode(false);
    }
    
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(
            this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }
    
    @Override
    public void decode(String jsonString) {
        TopicConfigSerializeWrapper wrapper = ...;
        topicConfigTable.putAll(wrapper.getTopicConfigTable());
    }
}
```

#### 5.4.2 ConsumerOffsetManager

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/offset/ConsumerOffsetManager.java

public class ConsumerOffsetManager extends ConfigManager {
    // (group@topic) -> (queueId -> offset)
    private ConcurrentMap<String/* group@topic */, 
        ConcurrentMap<Integer/* queueId */, Long/* offset */>> offsetTable =
        new ConcurrentHashMap<>(1024);
    
    public void commitOffset(final String clientHost, final String group, 
            final String topic, final int queueId, final long offset) {
        String key = group + TOPIC_GROUP_SEPARATOR + topic;
        
        ConcurrentMap<Integer, Long> map = offsetTable.get(key);
        if (map == null) {
            map = new ConcurrentHashMap<>();
            ConcurrentMap<Integer, Long> prev = offsetTable.putIfAbsent(key, map);
            if (prev != null) {
                map = prev;
            }
        }
        
        Long old = map.put(queueId, offset);
        if (old != null && old <= offset) {
            // 版本递增
            dataVersion.nextVersion();
        }
    }
    
    public long queryOffset(final String group, final String topic, final int queueId) {
        String key = group + TOPIC_GROUP_SEPARATOR + topic;
        ConcurrentMap<Integer, Long> map = offsetTable.get(key);
        if (map != null) {
            return map.getOrDefault(queueId, 0L);
        }
        return -1;
    }
}
```

### 5.5 定时任务

Broker 启动后会注册多个定时任务，覆盖各种周期性维护工作：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerController.java

private void initialScheduling() {
    // 1. 每 10 秒：持久化消费进度
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.consumerOffsetManager.persist(),
        1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    
    // 2. 每 10 秒：持久化消费过滤
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.consumerFilterManager.persist(),
        1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    
    // 3. 每 10 秒：持久化消费顺序信息
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.consumerOrderInfoManager.persist(),
        1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    
    // 4. 每 60 秒：持久化 Topic 配置
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.topicConfigManager.persist(),
        1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
    
    // 5. 每 5 秒：检查消费进度落后
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.protectBroker(),
        1000, 1000 * 5, TimeUnit.MILLISECONDS);
    
    // 6. 每 3 秒：检查消息发送到 Broker 的频率
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.printWaterMark(),
        10, 1000 * 3, TimeUnit.MILLISECONDS);
    
    // 7. 每天 0 点：统计昨日 TPS
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.brokerStatsManager.record(),
        0, 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    
    // 8. 每 60 秒：向 NameServer 注册
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.registerBrokerAll(true, false, false),
        1000 * 10, Math.max(10000, Math.min(
            brokerConfig.getRegisterNameServerPeriod(), 60000)),
        TimeUnit.MILLISECONDS);
    
    // 9. 每 60 秒：持久化订阅组配置
    this.scheduledExecutorService.scheduleAtFixedRate(
        () -> BrokerController.this.subscriptionGroupManager.persist(),
        1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
    
    // 10. 每 5 秒：检查消息到达，唤醒挂起的 Pull 请求
    if (this.brokerConfig.isLongPollingEnable()) {
        this.scheduledExecutorService.scheduleAtFixedRate(
            () -> BrokerController.this.getPullRequestHoldService()
                .checkHoldPullRequest(),
            0, 1000 * 5, TimeUnit.MILLISECONDS);
    }
}
```

### 5.6 BrokerPreOnlineService：上线前 HA 握手

在 Slave-acting-Master 模式下，当 Slave 即将成为 Master 之前，需要与旧 Master 进行 HA 握手，确保数据已同步：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/BrokerPreOnlineService.java

public class BrokerPreOnlineService extends ServiceThread {
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 1. 等待 HA 同步完成
                if (!brokerController.getMessageStore().getHaService()
                    .isHAOK()) {
                    Thread.sleep(1000);
                    continue;
                }
                
                // 2. 向 Controller 注册为 master
                BrokerController.this.registerBrokerAll(true, false, true);
                
                // 3. 等待 Controller 确认
                if (waitControllerMasterReady()) {
                    log.info("broker pre-online success");
                    break;
                }
            } catch (Exception e) { /* ... */ }
        }
    }
}
```

### 5.7 Broker 小结

Broker 是 RocketMQ 最复杂的模块，其核心设计思想包括：

```
1. 中央编排模式 (Central Orchestrator Pattern)
   - BrokerController 作为中央编排器
   - 所有组件在构造函数中创建
   - initialize() 负责加载和连接
   - start() 负责启动
   - shutdown() 按逆序关闭

2. 处理器-线程池映射 (Processor-Executor Mapping)
   - 每个请求码绑定一个处理器和一个独立线程池
   - 实现线程隔离，避免不同类型的请求互相阻塞

3. 主从双端口 (Dual Port Pattern)
   - 主 RemotingServer：端口 10911
   - Fast RemotingServer：端口 10911 - 2 = 38867
   - 两个 Server 注册相同的处理器映射
   - Fast 通道用于 VIP 客户端

4. 配置持久化 (Config Persistence Pattern)
   - 所有配置管理器继承 ConfigManager
   - 提供 encode()/decode()/persist()/load() 方法
   - 持久化为 JSON 文件
   - 定时任务定期持久化
```


---

## 六、Store 模块深度解析

Store 模块是 RocketMQ 的存储引擎，负责消息的持久化存储、索引构建和崩溃恢复。这是 RocketMQ 最核心的模块之一，也是性能的关键所在。

### 6.1 存储引擎总览：DefaultMessageStore

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java

public class DefaultMessageStore implements MessageStore {
    // ==================== 配置 ====================
    private final MessageStoreConfig messageStoreConfig;
    
    // ==================== 核心存储组件 ====================
    private final CommitLog commitLog;                    // CommitLog（全消息顺序日志）
    private final ConcurrentMap<String/* topic */, 
        ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;  // ConsumeQueue 表
    private final IndexService indexService;             // IndexFile 服务
    private final MappedFileQueue mappedFileQueue;        // (实际在 commitLog 内部)
    
    // ==================== 后台服务 ====================
    private final ReputMessageService reputMessageService;    // CommitLog → ConsumeQueue 分发
    private final FlushManager flushManager;                  // 刷盘管理器
    private final CleanCommitLogService cleanCommitLogService; // CommitLog 清理
    private final CleanConsumeQueueService cleanConsumeQueueService; // ConsumeQueue 清理
    private final AllocateMappedFileService allocateMappedFileService; // MappedFile 预分配
    
    // ==================== HA ====================
    private final HAService haService;                    // 主从同步
    
    // ==================== 事务消息 ====================
    private final TransactionalMessageService transactionalMessageService;
    
    // ==================== 检查点 ====================
    private StoreCheckpoint storeCheckpoint;               // 崩溃恢复检查点
    
    // ==================== 统计 ====================
    private final BrokerStatsManager brokerStatsManager;
    private final StoreStatsService storeStatsService;
    
    // ==================== 其他 ====================
    private final ScheduleMessageService scheduleMessageService;  // 定时消息
    private final TimerMessageStore timerMessageStore;            // 新版定时消息
}
```

DefaultMessageStore 的内部结构可以用下面的图来表示：

```
┌──────────────────────────────────────────────────────────────────────┐
│                    DefaultMessageStore                               │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │ 写入路径 (Write Path)                                       │     │
│  │                                                             │     │
│  │  putMessage(MessageExtBrokerInner)                          │     │
│  │       │                                                     │     │
│  │       ▼                                                     │     │
│  │  CommitLog.asyncPutMessage()                                │     │
│  │       │                                                     │     │
│  │       ├──► putMessageLock.lock()     (串行写入)              │     │
│  │       ├──► MappedFile.appendMessage()  (mmap 写入)          │     │
│  │       │       └──► DefaultAppendMessageCallback.doAppend()  │     │
│  │       │              (实际字节写入)                          │     │
│  │       ├──► FlushManager.handleDiskFlush()  (刷盘)           │     │
│  │       └──► HAService.putMessage()        (主从复制)          │     │
│  │                                                             │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │ 异步分发路径 (Async Dispatch Path)                           │     │
│  │                                                             │     │
│  │  ReputMessageService (后台线程)                              │     │
│  │       │                                                     │     │
│  │       ├──► CommitLog.getMessage()   (从 CommitLog 读取)       │     │
│  │       │                                                     │     │
│  │       ├──► ConsumeQueue.putMessagePositionInfo()           │     │
│  │       │    (写入 ConsumeQueue 索引: offset+size+tagHash)    │     │
│  │       │                                                     │     │
│  │       └──► IndexService.buildIndex()                        │     │
│  │            (写入 IndexFile: hash→offset)                   │     │
│  │                                                             │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │ 读取路径 (Read Path)                                         │     │
│  │                                                             │     │
│  │  getMessage(group, topic, queueId, offset, maxNums)        │     │
│  │       │                                                     │     │
│  │       ├──► ConsumeQueue.getIndicesInBuffer()               │     │
│  │       │    (从 ConsumeQueue 读取索引项)                      │     │
│  │       │                                                     │     │
│  │       └──► CommitLog.getMessage()                           │     │
│  │            (根据索引中的 physicalOffset 从 CommitLog 读取)   │     │
│  │                                                             │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │ 清理路径 (Clean Path)                                        │     │
│  │                                                             │     │
│  │  CleanCommitLogService (定时清理过期 CommitLog 文件)           │     │
│  │  CleanConsumeQueueService (同步清理 ConsumeQueue)             │     │
│  │                                                             │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │ 崩溃恢复路径 (Recovery Path)                                │     │
│  │                                                             │     │
│  │  load()                                                     │     │
│  │       │                                                     │     │
│  │       ├──► CommitLog.recover()                              │     │
│  │       │    (扫描最后一个文件，恢复到最后有效消息)               │     │
│  │       │                                                     │     │
│  │       └──► ConsumeQueue.recover()                           │     │
│  │            (根据 CommitLog 恢复 ConsumeQueue)              │     │
│  │                                                             │     │
│  └─────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────┘
```

### 6.2 CommitLog：全消息顺序日志

CommitLog 是 RocketMQ 存储的核心。所有 Topic 的所有消息都写入同一个 CommitLog。

#### 6.2.1 CommitLog 的文件组织

```
${storePathRootDir}/commitlog/
├── 00000000000000000000  (第 0 个文件，0 ~ 1GB)
├── 00000000001073741824  (第 1 个文件，1GB ~ 2GB)
├── 00000000002147483648  (第 2 个文件，2GB ~ 3GB)
└── ...

每个文件固定大小：1GB (1073741824 bytes)
文件名是该文件起始偏移量（20 位数字补零）
```

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/CommitLog.java

public class CommitLog {
    // 每个 CommitLog 文件固定大小：默认 1GB
    private final int mappedFileSizeQueue;  // = 1024 * 1024 * 1024 = 1073741824
    
    // MappedFile 队列（管理多个顺序排列的 MappedFile）
    private final MappedFileQueue mappedFileQueue;
    
    // 追加消息回调（实际执行字节写入）
    private final AppendMessageCallback appendMessageCallback;
    
    // 写入锁（保证串行写入）
    private final PutMessageLock putMessageLock;
    
    // 消息存储引擎引用
    private final DefaultMessageStore messageStore;
    
    // Magic Code
    public static final int MESSAGE_MAGIC_CODE = 0xDAA320A7;
    public static final int BLANK_MAGIC_CODE = 0xABD43194;
    
    public CommitLog(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
        this.mappedFileSizeQueue = messageStore.getMessageStoreConfig()
            .getMappedFileSizeCommitLog(); // 默认 1GB
        
        this.mappedFileQueue = new MappedFileQueue(
            messageStoreConfig.getStorePathCommitLog(),
            mappedFileSizeQueue,
            allocateMappedFileService
        );
        
        this.appendMessageCallback = new DefaultAppendMessageCallback(
            messageStore.getMessageStoreConfig().getMaxMessageSize()
        );
        
        // 选择锁策略
        if (messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage()) {
            putMessageLock = new PutMessageReentrantLock();
        } else {
            putMessageLock = new PutMessageSpinLock();
        }
    }
}
```

#### 6.2.2 消息写入：asyncPutMessage 详解

这是 RocketMQ 最核心的写入路径，每一条消息都经过这里：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/CommitLog.java

public PutMessageResult asyncPutMessage(final MessageExtBrokerInner msg) {
    // 1. 参数校验
    if (! defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
        // 非副本模式，设置存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());
    }
    
    // 检查 Topic 长度
    if (msg.getTopic().length() > ... ) {
        return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, ...);
    }
    
    // 检查消息体大小
    if (msg.getBody() == null || ...) {
        return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, ...);
    }
    
    // 2. 获取当前可写的 MappedFile
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
    
    // 3. 加锁（串行写入 CommitLog）
    putMessageLock.lock();
    try {
        // 检查文件是否已满
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0, true);
            if (null == mappedFile) {
                log.error("create mapped file error, topic: " + msg.getTopic());
                return new PutMessageResult(
                    PutMessageStatus.CREATE_MAPPEDFILE_FAILED, null);
            }
        }
        
        // 4. 追加消息（核心写入逻辑）
        AppendMessageResult result;
        try {
            result = mappedFile.appendMessage(msg, this.appendMessageCallback, 
                this.putMessageContext);
        } catch (Exception e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, ...);
        }
        
        // 5. 处理写入结果
        switch (result.getStatus()) {
            case PUT_OK:
                // 写入成功
                break;
            case END_OF_FILE:
                // 当前文件已满，创建新文件并重试
                mappedFile = this.mappedFileQueue.getLastMappedFile(0, true);
                result = mappedFile.appendMessage(msg, this.appendMessageCallback, 
                    this.putMessageContext);
                break;
            case MESSAGE_SIZE_EXCEEDED:
            case PROPERTIES_SIZE_EXCEED:
                return new PutMessageResult(
                    PutMessageStatus.MESSAGE_ILLEGAL, result);
            case UNKNOWN_ERROR:
                return new PutMessageResult(
                    PutMessageStatus.UNKNOWN_ERROR, result);
            default:
                return new PutMessageResult(
                    PutMessageStatus.UNKNOWN_ERROR, result);
        }
    } finally {
        putMessageLock.unlock();
    }
    
    // 6. 构造返回结果
    PutMessageResult putMessageResult = new PutMessageResult(
        PutMessageStatus.PUT_OK, result);
    
    // 7. 刷盘
    CommitLog.this.getMessageStore().getFlushManager()
        .handleDiskFlush(result, putMessageResult, msg);
    
    // 8. HA 同步（主从复制）
    if (! defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
        CommitLog.this.getMessageStore().getHAService()
            .putMessage(result);
    }
    
    return putMessageResult;
}
```

#### 6.2.3 DefaultAppendMessageCallback.doAppend：实际字节写入

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultAppendMessageCallback.java

public AppendMessageResult doAppend(
    final long fileFromOffset,       // 文件起始偏移量
    final ByteBuffer byteBuffer,      // MappedFile 的 ByteBuffer
    final int maxBlank,              // 剩余可写空间
    final MessageExtBrokerInner msg, // 消息
    PutMessageContext putMessageContext) {
    
    // ==================== 1. 计算物理偏移量 ====================
    // fileFromOffset + 当前写入位置 = 消息在 CommitLog 中的全局物理偏移量
    long wroteOffset = fileFromOffset + byteBuffer.position();
    
    // ==================== 2. 设置消息的 queueOffset ====================
    // queueOffset 是消息在 ConsumeQueue 中的逻辑偏移量
    // 由消息存储引擎根据 ConsumeQueue 当前最大偏移量 + 1 得到
    msg.setQueueOffset(queueOffset);
    
    // ==================== 3. 计算 SYS_FLAG ====================
    // SYS_FLAG 的 bit 0: 是否有 BODY
    // SYS_FLAG 的 bit 1: 是否有 MAP_CLASS（在旧版本中表示消息类型）
    int sysFlag = msg.getSysFlag();
    
    // ==================== 4. 计算事务相关 ====================
    String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
    int transactionStatus = 0;
    if (tranMsg != null) {
        transactionStatus = ...;
    }
    
    // ==================== 5. 序列化消息为二进制 ====================
    // 调用 MessageExtEncoder 编码
    final byte[] msgData = ...;
    
    // ==================== 6. 写入 ByteBuffer ====================
    // 消息的二进制格式如下：
    //
    // ┌──────────────┬──────────────┬──────────────┬──────────────┐
    // │ TOTALSIZE(4) │ MAGICCODE(4) │ BODYCRC(4)   │ QUEUEID(4)   │
    // ├──────────────┼──────────────┼──────────────┼──────────────┤
    // │ FLAG(4)      │ QUEUEOFFSET(8)│ PHYSICALOFFSET(8)│ SYSFLAG(4)│
    // ├──────────────┼──────────────┼──────────────┼──────────────┤
    // │ BORNTIMESTAMP(8) │ BORNHOST(8/20)│STORETIMESTAMP(8)│STOREHOST(8/20)│
    // ├──────────────┼──────────────┼──────────────┼──────────────┤
    // │ RECONSUMETIMES(4)│PREPAREDTRANOFFSET(8)│BODYLEN(4)│ BODY(variable)│
    // ├──────────────┼──────────────┼──────────────┼──────────────┤
    // │ TOPICLENGTH(1/2) │ TOPIC(variable) │PROPLEN(2)│ PROPERTIES(var)│
    // └──────────────┴──────────────┴──────────────┴──────────────┘
    
    // 检查剩余空间是否足够
    int msgLen = msgData.length;
    if (msgLen + END_FILE_MIN_BLANK_LENGTH > maxBlank) {
        // 空间不足，返回 END_OF_FILE，上层会创建新文件
        return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, ...);
    }
    
    // 写入消息体
    byteBuffer.put(msgData);
    
    // ==================== 7. 返回结果 ====================
    return new AppendMessageResult(
        AppendMessageStatus.PUT_OK,     // 状态
        wroteOffset,                     // 写入偏移量
        msgLen,                          // 消息长度
        msgId,                           // 消息 ID
        msg.getQueueOffset(),            // 队列偏移量
        msg.getStoreTimestamp()          // 存储时间
    );
}
```

#### 6.2.4 消息二进制格式详解

RocketMQ 的消息在 CommitLog 中的二进制格式如下，每一项都精确到字节：

```
偏移量     字段名                    长度(字节)    说明
─────────  ────────────────────────  ──────────    ──────────────────────────────
0          TOTALSIZE                 4             消息总长度（不含本字段和 MAGICCODE）
4          MAGICCODE                 4             魔数：0xDAA320A7 (有效消息) 或 0xABD43194 (空白填充)
8          BODYCRC                   4             消息体的 CRC32 校验码
12         QUEUEID                   4             队列 ID
16         FLAG                      4             消息标志位
20         QUEUEOFFSET               8             ConsumeQueue 中的逻辑偏移量
28         PHYSICALOFFSET            8             CommitLog 中的物理偏移量
36         SYSFLAG                   4             系统标志位
40         BORNTIMESTAMP             8             消息产生时间戳
48         BORNHOST                  8/20          产生消息的地址 (IPv4:8B, IPv6:20B)
48/60      STORETIMESTAMP            8             存储时间戳
56/68      STOREHOSTADDRESS          8/20          存储消息的地址 (IPv4:8B, IPv6:20B)
56/68+8/20 RECONSUMETIMES           4             重试消费次数
...        PreparedTransactionOffset 8             事务消息预备偏移量
...        BODYLENGTH                4             消息体长度
...        BODY                      variable      消息体内容
...        TOPICLENGTH               1/2           Topic 名称长度 (1B 如果 < 127, 2B 如果 >= 128)
...        TOPIC                     variable      Topic 名称
...        PROPERTIESLENGTH          2             属性键值对长度
...        PROPERTIES                variable      属性键值对（序列化为字符串）
```

Magic Code 的含义：
- `MESSAGE_MAGIC_CODE = 0xDAA320A7`：表示这是一条有效消息
- `BLANK_MAGIC_CODE = 0xABD43194`：表示文件末尾的空白填充（用于文件对齐）

### 6.3 ConsumeQueue：消费队列索引

ConsumeQueue 是每个 `Topic + QueueId` 对应的索引文件，存储消息在 CommitLog 中的物理位置。

#### 6.3.1 ConsumeQueue 的内存布局

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/consumequeue/ConsumeQueue.java

public class ConsumeQueue implements ConsumeQueueInterface {
    // 每条索引项固定 20 字节
    public static final int CQ_STORE_UNIT_SIZE = 20;
    
    // 单个 ConsumeQueue 文件大小：默认 300000 * 20 = 6000000 bytes ≈ 5.7MB
    private int fileSize;
    
    // MappedFile 队列（管理多个 ConsumeQueue 文件）
    private final MappedFileQueue mappedFileQueue;
    
    // Topic 和 QueueId
    private final String topic;
    private final int queueId;
    
    // 当前最大偏移量（逻辑偏移量，即消息条数）
    private long maxOffsetInQueue;
    
    // 存储引擎引用
    private final DefaultMessageStore defaultMessageStore;
    
    // ConsumeQueue 存储路径
    // ${storePathRootDir}/consumequeue/${topic}/${queueId}/
    // 文件名是该文件起始偏移量
    
    // ConsumeQueue 文件内部布局：
    //
    // ┌──────────┬──────────┬──────────┬──────────┬────────┐
    // │ Entry 0  │ Entry 1  │ Entry 2  │ ...      │Entry N  │
    // │ 20B      │ 20B      │ 20B      │          │ 20B     │
    // └──────────┴──────────┴──────────┴──────────┴────────┘
    //
    // 每条 Entry 20 字节：
    // ┌──────────────────┬────────────┬──────────────────┐
    // │ physicalOffset   │ bodySize   │ tagHash          │
    // │ (8 bytes)        │ (4 bytes)  │ (8 bytes)        │
    // └──────────────────┴────────────┴──────────────────┘
}
```

#### 6.3.2 索引写入：putMessagePositionInfo

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/consumequeue/ConsumeQueue.java

public void putMessagePositionInfo(
    final long offset,        // CommitLog 物理偏移量
    final int size,           // 消息总大小
    final long tagsCode,     // Tag 哈希码
    final long ConsumeQueueOffset  // ConsumeQueue 逻辑偏移量
) {
    
    // 1. 计算 ByteBuffer
    ByteBuffer byteBuffer = ...;
    
    // 2. 写入 20 字节的索引项
    byteBuffer.putLong(offset);        // 8B: CommitLog 物理偏移量
    byteBuffer.putInt(size);           // 4B: 消息大小
    byteBuffer.putLong(tagsCode);     // 8B: Tag 哈希码
    
    // 3. 获取当前可写的 MappedFile
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(
        ConsumeQueueOffset * CQ_STORE_UNIT_SIZE);
    
    // 如果文件满了，创建新文件
    if (mappedFile == null) {
        mappedFile = this.mappedFileQueue.getLastMappedFile(
            0, true);
    }
    
    // 4. 追加写入
    boolean result = mappedFile.appendMessage(byteBuffer.array());
    
    if (!result) {
        log.error("...");
    }
    
    // 5. 更新 maxOffsetInQueue
    if (ConsumeQueueOffset > maxOffsetInQueue) {
        maxOffsetInQueue = ConsumeQueueOffset;
    }
}
```

#### 6.3.3 索引查询：getIndicesInBuffer

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/consumequeue/ConsumeQueue.java

public SelectMappedBufferResult getIndicesInBuffer(
    final long startIndex,    // 起始逻辑偏移量
    final int size             // 最大返回条数
) {
    // 1. 计算物理偏移量
    int mappedFileSize = this.mappedFileQueue.getMappedFileSize();
    long offset = startIndex * CQ_STORE_UNIT_SIZE;
    
    // 2. 找到对应的 MappedFile
    MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
    if (mappedFile == null || mappedFile.isAvailable() == false) {
        return null;
    }
    
    // 3. 计算在文件内的偏移量
    int pos = (int) (offset % mappedFileSize);
    
    // 4. 从 MappedFile 中读取一个 SelectMappedBufferResult
    // 这实际上是一个 ByteBuffer 的切片，指向 mappedFile 的内存映射区域
    SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
    
    return result;
}
```

### 6.4 IndexFile：哈希索引文件

IndexFile 提供了基于 Message Key 的哈希索引，支持按 key 快速查找消息。

#### 6.4.1 IndexFile 的内存布局

```
┌──────────────────────────────────────────────────────────────────────┐
│                         IndexFile 文件结构                            │
│                                                                      │
│  ┌─── Header (40 bytes) ────────────────────────────────────────┐    │
│  │ beginTimestamp (8B)    : 文件中最旧消息的存储时间               │    │
│  │ endTimestamp   (8B)    : 文件中最新消息的存储时间               │    │
│  │ beginPhyOffset  (8B)   : 文件中最旧消息的 CommitLog 偏移量      │    │
│  │ endPhyOffset    (8B)   : 文件中最新消息的 CommitLog 偏移量      │    │
│  │ hashSlotCount   (4B)   : 已使用的哈希槽数量                     │    │
│  │ indexCount      (4B)   : 已使用的索引条目数量                   │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─── Hash Slots (500000 × 4B = 2000000B) ─────────────────────┐   │
│  │ Slot 0  Slot 1  Slot 2  ...  Slot N                            │   │
│  │ (4B)   (4B)   (4B)        (4B)                                 │   │
│  │ 每个 Slot 存储的是该槽位上最后一条索引的序号                    │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─── Index Entries (20000000 × 20B) ──────────────────────────┐   │
│  │ Entry 0  Entry 1  Entry 2  ...  Entry N                        │   │
│  │ (20B)    (20B)    (20B)         (20B)                          │   │
│  │                                                                │   │
│  │ 每条 Entry 20 字节：                                            │   │
│  │ ┌──────────────┬──────────────────┬─────────────┐              │   │
│  │ │ keyHash (4B) │ phyOffset (8B)   │ timeDiff(4B)│              │   │
│  │ │              │                  │             │              │   │
│  │ │ 消息 key 的  │ CommitLog 物理偏移│ 存储时间与   │              │   │
│  │ │ 哈希值       │ 量              │ header 开始 │              │   │
│  │ │              │                  │ 时间差(秒)  │              │   │
│  │ ├──────────────┤                  │             │              │   │
│  │ │ prevIndex(4B)│                  │             │              │   │
│  │ │              │                  │             │              │   │
│  │ │ 同一槽位上一 │                  │             │              │   │
│  │ │ 条索引的序号  │                  │             │              │   │
│  │ │ (链表实现)   │                  │             │              │   │
│  │ └──────────────┴──────────────────┴─────────────┘              │   │
│  └────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘

总文件大小 = 40 + 500000*4 + 20000000*20 = 401,200,040 bytes ≈ 382MB
```

#### 6.4.2 IndexFile 的写入

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/index/IndexFile.java

public boolean putKey(
    final String key,
    final long phyOffset,
    final long storeTimestamp
) {
    // 1. 计算 key 的 hash 值
    int keyHash = key.hashCode();
    int hashSlot = keyHash % hashSlotNum;  // 定位哈希槽
    if (hashSlot < 0) hashSlot = Math.abs(hashSlot);
    
    // 2. 计算 index 条目位置
    int indexPos = 0;
    int slotValue = this.mappedByteBuffer.getInt(
        INDEX_HEADER_SIZE + hashSlot * HASH_SLOT_SIZE);
    
    // 3. 构建索引条目（20 字节）
    int absIndexPos = INDEX_HEADER_SIZE 
        + hashSlotNum * HASH_SLOT_SIZE 
        + this.indexCount * INDEX_SIZE;
    
    // 写入索引条目
    this.mappedByteBuffer.putInt(absIndexPos, keyHash);           // keyHash
    this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);   // phyOffset
    this.mappedByteBuffer.putInt(absIndexPos + 12, 
        (int)(storeTimestamp - this.beginTimestamp.get()));       // timeDiff
    this.mappedByteBuffer.putInt(absIndexPos + 16, slotValue);   // prevIndex
    
    // 4. 更新哈希槽，指向当前条目
    this.mappedByteBuffer.putInt(
        INDEX_HEADER_SIZE + hashSlot * HASH_SLOT_SIZE, 
        this.indexCount);
    
    // 5. 更新 Header
    if (this.indexCount <= 1) {
        this.beginTimestamp.set(storeTimestamp);
        this.beginPhyOffset.set(phyOffset);
    }
    this.endTimestamp.set(storeTimestamp);
    this.endPhyOffset.set(phyOffset);
    this.indexCount.incrementAndGet();
    
    return true;
}
```

#### 6.4.3 IndexFile 的查询

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/index/IndexFile.java

public void selectPhyOffset(
    final List<Long> phyOffsets,   // 返回结果
    final String key,
    final int maxNum,
    final long begin,
    final long end
) {
    // 1. 计算 hash 和 slot
    int keyHash = key.hashCode();
    int hashSlot = keyHash % hashSlotNum;
    if (hashSlot < 0) hashSlot = Math.abs(hashSlot);
    
    // 2. 读取 slot 值（最后一条索引条目的序号）
    int slotValue = this.mappedByteBuffer.getInt(
        INDEX_HEADER_SIZE + hashSlot * HASH_SLOT_SIZE);
    
    if (slotValue <= 0 || slotValue > this.indexCount) {
        return; // 无数据
    }
    
    // 3. 沿链表遍历
    int nextIndexToRead = slotValue;
    for (int i = 0; i < maxNum; i++) {
        int absIndexPos = INDEX_HEADER_SIZE 
            + hashSlotNum * HASH_SLOT_SIZE 
            + nextIndexToRead * INDEX_SIZE;
        
        // 读取索引条目
        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
        long phyOffset = this.mappedByteBuffer.getLong(absIndexPos + 4);
        int timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 12);
        int prevIndex = this.mappedByteBuffer.getInt(absIndexPos + 16);
        
        // 时间范围检查
        long timeRead = this.beginTimestamp.get() + timeDiff * 1000L;
        if (timeRead < begin || timeRead > end) {
            // 不在时间范围内
        } else {
            // 加入结果
            phyOffsets.add(phyOffset);
        }
        
        if (prevIndex <= 0) {
            break; // 链表结束
        }
        
        nextIndexToRead = prevIndex;
    }
}
```

### 6.5 MappedFile：内存映射文件

MappedFile 是 Java NIO `MappedByteBuffer` 的封装，RocketMQ 的所有文件都通过 mmap 映射到内存。

#### 6.5.1 三位置模型

MappedFile 维护三个位置指针，这是理解 RocketMQ 刷盘机制的关键：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMappedFile.java

public class DefaultMappedFile extends ReferenceResource implements MappedFile {
    // 内存映射的 ByteBuffer
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    
    // ============ 三位置模型 ============
    
    // wrotePosition: 写入位置（消息写入到 ByteBuffer 的位置）
    // 这是最前沿的位置，消息先写到这里
    private volatile int wrotePosition;
    
    // committedPosition: 提交位置（从 ByteBuffer 提交到 FileChannel 的位置）
    // 等于 wrotePosition（在异步提交模式下可能落后）
    private volatile int committedPosition;
    
    // flushedPosition: 刷盘位置（从 FileChannel flush 到磁盘的位置）
    // 这是最慢的位置，表示数据已经持久化到磁盘
    private volatile int flushedPosition;
    
    // 三者的关系：flushedPosition <= committedPosition <= wrotePosition
}
```

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MappedFile 三位置模型                              │
│                                                                     │
│  位置：    0          flushedPos    committedPos    wrotePos    EOF  │
│           │          │              │               │          │    │
│           ▼          ▼              ▼               ▼          ▼    │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │     已刷盘     │  已提交未刷盘  │  已写入未提交  │  未写入   │ │
│  └────────────────────────────────────────────────────────────────┘ │
│           ▲              ▲               ▲                         │
│           │              │               │                         │
│           │              │               └── 消息写入到 mmap 的位置 │
│           │              │                                            │
│           │              └── 从 mmap buffer 提交到 fileChannel 的位置 │
│           │                                                           │
│           └── fileChannel.force() 刷到磁盘的位置                      │
│                                                                     │
│  说明：                                                               │
│  - wrotePosition: 消息写入 mmap 内存后更新                              │
│  - committedPosition: flushManager 定期从 mmap buffer 提交时更新        │
│  - flushedPosition: flushManager 调用 fileChannel.force() 后更新     │
│                                                                     │
│  同步刷盘 (SYNC_FLUSH):                                               │
│    wrotePosition → flush 立即执行 → flushedPosition = wrotePosition   │
│    每条消息都刷盘，可靠性高但性能低                                       │
│                                                                     │
│  异步刷盘 (ASYNC_FLUSH):                                              │
│    wrotePosition → 定期 commit → 定期 flush                           │
│    攒一批再刷盘，性能高但有数据丢失风险                                    │
└─────────────────────────────────────────────────────────────────────┘
```

#### 6.5.2 appendMessage：消息追加

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMappedFile.java

public AppendMessageResult appendMessage(
    final MessageExtBrokerInner msg,
    final AppendMessageCallback cb,
    final PutMessageContext putMessageContext
) {
    // 当前已写入位置
    int currentPos = this.wrotePosition.get();
    
    // 如果文件已满
    if (currentPos >= this.fileSize) {
        return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, ...);
    }
    
    // 调用回调执行实际写入
    return cb.doAppend(
        this.getFileFromOffset(),       // 文件起始偏移量
        this.mappedByteBuffer.slice(),   // ByteBuffer 切片
        this.fileSize - currentPos,     // 剩余可写空间
        msg,                            // 消息
        putMessageContext
    );
}
```

#### 6.5.3 flush：刷盘

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMappedFile.java

public int flush(final int flushLeastPages) {
    // flushLeastPages: 最少刷多少页（每页 4KB）
    // 如果写入量不足一页，可以跳过刷盘（优化）
    
    if (this.isFull() || (this.wrotePosition.get() - this.flushedPosition.get()) 
        >= flushLeastPages * OS_PAGE_SIZE) {
        
        synchronized (this) {
            if (this.isFull() || (this.wrotePosition.get() - this.flushedPosition.get()) 
                >= flushLeastPages * OS_PAGE_SIZE) {
                
                // 从 flushedPosition 刷到 wrotePosition
                int flushOffset = this.wrotePosition.get();
                
                // 获取 ByteBuffer
                ByteBuffer buffer = this.mappedByteBuffer.slice();
                buffer.position(this.flushedPosition.get());
                buffer.limit(flushOffset);
                
                // 调用 fileChannel.force() 刷盘
                this.fileChannel.position(this.flushedPosition.get());
                this.fileChannel.write(buffer);
                this.fileChannel.force(false);
                
                // 更新 flushedPosition
                this.flushedPosition.set(flushOffset);
            }
        }
    }
    
    return this.getFlushedPosition();
}
```

### 6.6 ReputMessageService：异步分发服务

ReputMessageService 是一个后台线程，负责从 CommitLog 读取消息，构建 ConsumeQueue 和 IndexFile：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java

class ReputMessageService extends ServiceThread {
    // 从哪个物理偏移量开始读取
    private volatile long reputFromOffset = 0;
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                Thread.sleep(1); // 每毫秒检查一次
                this.doReput();
            } catch (Exception e) { /* ... */ }
        }
    }
    
    private void doReput() {
        // 1. 从 CommitLog 读取消息
        SelectMappedBufferResult result = 
            CommitLog.this.getMessageStore().getCommitLog()
                .getData(reputFromOffset);
        
        if (result == null) {
            // 没有新消息
            return;
        }
        
        // 2. 从读取位置开始遍历
        ByteBuffer byteBuffer = result.getByteBuffer();
        while (byteBuffer.hasRemaining()) {
            // 读取消息
            DispatchRequest dispatchRequest = 
                CommitLog.this.getMessageStore().getCommitLog()
                    .checkMessageAndReturnSize(byteBuffer, ...);
            
            if (dispatchRequest.isSuccess()) {
                // 3. 分发到 ConsumeQueue
                DispatchRequestCq = dispatchRequest;
                ConsumeQueue consumeQueue = 
                    DefaultMessageStore.this.findConsumeQueue(
                        dispatchRequest.getTopic(), 
                        dispatchRequest.getQueueId()
                    );
                
                consumeQueue.putMessagePositionInfo(
                    dispatchRequest.getCommitLogOffset(),   // 物理偏移量
                    dispatchRequest.getMsgSize(),            // 消息大小
                    dispatchRequest.getTagsCode(),           // Tag 哈希
                    dispatchRequest.getConsumeQueueOffset()  // 队列偏移量
                );
                
                // 4. 分发到 IndexFile
                DefaultMessageStore.this.indexService.buildIndex(
                    dispatchRequest.getTopic(),
                    dispatchRequest.getKeys(),
                    dispatchRequest.getCommitLogOffset(),
                    dispatchRequest.getStoreTimestamp()
                );
                
                // 更新 reputFromOffset
                reputFromOffset += dispatchRequest.getSize();
            }
        }
        
        result.release();
    }
}
```

### 6.7 FlushManager：刷盘管理器

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/flush/FlushManager.java

// 刷盘类型
public enum FlushDiskType {
    SYNC_FLUSH,  // 同步刷盘
    ASYNC_FLUSH  // 异步刷盘
}

// ===== 同步刷盘：GroupCommitService =====
class GroupCommitService extends FlushManager {
    // 同步刷盘请求队列
    private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();
    private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            // 交换读写队列
            this.swapRequests();
            
            // 处理刷盘请求
            for (GroupCommitRequest req : requestsRead) {
                boolean flushOK = false;
                for (int i = 0; i < 2 && !flushOK; i++) {
                    // 刷盘
                    CommitLog.this.mappedFileQueue.getFlushedWhere();
                    flushOK = CommitLog.this.mappedFileQueue.flush(0);
                }
                
                // 唤醒等待的线程
                req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK 
                    : PutMessageStatus.FLUSH_DISK_TIMEOUT);
            }
            
            requestsRead.clear();
            
            // 休息一下（如果没有请求）
            this.waitForRunning(10); // 10ms
        }
    }
}

// ===== 异步刷盘：FlushRealTimeService =====
class FlushRealTimeService extends FlushManager {
    @Override
    public void run() {
        while (!this.isStopped()) {
            // 定期刷盘
            int flushInterval = messageStoreConfig.getFlushIntervalCommitLog();
            int flushLeastPages = messageStoreConfig.getFlushCommitLogLeastPages();
            
            // 如果上一次刷盘后有新写入
            if (flushedWhere < wrotePosition) {
                CommitLog.this.mappedFileQueue.flush(flushLeastPages);
            }
            
            this.waitForRunning(flushInterval); // 默认 500ms
        }
    }
}
```

### 6.8 StoreCheckpoint：崩溃恢复检查点

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/StoreCheckpoint.java

public class StoreCheckpoint {
    // 一个 mmap'd 文件，存储 6 个 long 值
    private final MappedFile mappedFile;
    
    // ============ 6 个检查点值 ============
    
    // 1. CommitLog 的刷盘位置
    //    崩溃恢复时，从 flushPhyOffset 开始扫描 CommitLog
    private volatile long flushPhyOffset;
    
    // 2. CommitLog 的提交位置
    private volatile long commitPhyOffset;
    
    // 3. ConsumeQueue 的刷盘位置
    //    恢复时从 flushCqOffset 开始扫描
    private volatile long flushCqOffset;
    
    // 4. ConsumeQueue 的提交位置
    private volatile long commitCqOffset;
    
    // 5. IndexFile 的刷盘位置
    private volatile long flushIndexOffset;
    
    // 6. 旧版定时消息的刷盘位置
    private volatile long flushScheduleOffset;
    
    public StoreCheckpoint(String path) throws IOException {
        File file = new File(path);
        // 文件固定大小：48 bytes (6 * 8)
        MappedFile mappedFile = new DefaultMappedFile(file, 8 * 6);
        this.mappedFile = mappedFile;
    }
    
    public void setFlushPhyOffset(long phyOffset) {
        this.flushPhyOffset = phyOffset;
        // 写入 mmap buffer
        this.mappedByteBuffer.putLong(0, phyOffset);
    }
    
    public long getFlushPhyOffset() {
        return this.mappedByteBuffer.getLong(0);
    }
    
    // 持久化（调用 force() 刷盘）
    public void flush() {
        // 在 leader 切换或正常关闭时调用
        this.mappedByteBuffer.force();
    }
}
```

### 6.9 崩溃恢复流程

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java

public boolean load() {
    boolean result = true;
    
    // 1. 加载 CommitLog
    result = this.commitLog.load();
    
    // 2. 加载 ConsumeQueue
    result = result && this.loadConsumeQueue();
    
    // 3. 加载检查点
    if (result) {
        this.storeCheckpoint = ...;
    }
    
    // 4. 恢复（核心）
    result = result && this.recover();
    
    return result;
}

private boolean recover() {
    // 1. 恢复 CommitLog
    long maxPhyOffset = this.commitLog.recover();
    
    // 2. 恢复 ConsumeQueue
    this.recoverConsumeQueue(maxPhyOffset);
    
    // 3. 恢复 IndexFile
    this.indexService.recover(maxPhyOffset);
    
    return true;
}

// CommitLog 恢复
public long recover() {
    // 正常恢复模式
    if (messageStoreConfig.isEnableTransientStorePool()) {
        this.recoverCommitLogNormal();
    } else {
        this.recoverCommitLogAbnormal();
    }
}
```

崩溃恢复的核心逻辑：

```
恢复 CommitLog:
  1. 读取 StoreCheckpoint 中的 flushPhyOffset
  2. 从该偏移量开始扫描最后一个 CommitLog 文件
  3. 逐条验证消息的 MagicCode 和 CRC32
  4. 找到最后一条有效消息的位置
  5. 设置 wrotePosition 为该位置

恢复 ConsumeQueue:
  1. 遍历所有 Topic 和 QueueId
  2. 从 StoreCheckpoint 中的 flushCqOffset 开始扫描
  3. 重建 ConsumeQueue 的 maxOffsetInQueue
  4. 如果 ConsumeQueue 损坏，根据 CommitLog 重建

恢复 IndexFile:
  1. 读取 StoreCheckpoint 中的 flushIndexOffset
  2. 修复损坏的 IndexFile
```

### 6.10 MappedFileQueue：文件队列管理

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/MappedFileQueue.java

public class MappedFileQueue {
    // 存储路径
    private final String storePath;
    
    // 每个文件大小
    private final int mappedFileSize;
    
    // 有序的 MappedFile 列表
    private final CopyOnWriteArrayList<MappedFile> mappedFiles;
    
    // 预分配服务
    private final AllocateMappedFileService allocateMappedFileService;
    
    // 刷盘位置（全局）
    private volatile long flushedWhere;
    
    // 提交位置（全局）
    private volatile long committedWhere;
    
    // 获取最后一个 MappedFile（可写的那个）
    public MappedFile getLastMappedFile() {
        MappedFile mappedFile = null;
        if (!this.mappedFiles.isEmpty()) {
            mappedFile = this.mappedFiles.get(this.mappedFiles.size() - 1);
        }
        return mappedFile;
    }
    
    // 获取或创建最后一个 MappedFile
    // doCreate: 如果不存在是否创建新文件
    public MappedFile getLastMappedFile(final long startOffset, boolean doCreate) {
        MappedFile mappedFile = this.getLastMappedFile();
        
        if (mappedFile != null && mappedFile.isFull()) {
            // 当前文件已满，需要创建新文件
            mappedFile = null;
        }
        
        if (mappedFile == null && doCreate) {
            // 计算新文件的起始偏移量
            long createOffset = ...;
            
            // 如果启用了预分配服务
            if (this.allocateMappedFileService != null) {
                // 通过 AllocateMappedFileService 异步预创建
                mappedFile = this.allocateMappedFileService
                    .putRequestAndReturnMappedFile(
                        createOffset, this.mappedFileSize);
            } else {
                // 同步创建
                mappedFile = new DefaultMappedFile(
                    storePath + "/" + fileName, this.mappedFileSize);
            }
            
            // 添加到列表
            this.mappedFiles.add(mappedFile);
        }
        
        return mappedFile;
    }
    
    // 根据偏移量查找 MappedFile
    public MappedFile findMappedFileByOffset(final long offset) {
        long firstFileOffset = ...;
        int fileIndex = (int) ((offset - firstFileOffset) / this.mappedFileSize);
        if (fileIndex >= 0 && fileIndex < this.mappedFiles.size()) {
            return this.mappedFiles.get(fileIndex);
        }
        return null;
    }
    
    // 刷盘（遍历所有需要刷盘的文件）
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        
        MappedFile mappedFile = this.findMappedFileByOffset(committedWhere);
        if (mappedFile != null) {
            // 获取从当前文件开始的所有文件
            int index = this.mappedFiles.indexOf(mappedFile);
            for (int i = index; i < this.mappedFiles.size(); i++) {
                MappedFile mf = this.mappedFiles.get(i);
                result = mf.flush(flushLeastPages);
                if (!result) break;
            }
            // 更新 flushedWhere
            this.flushedWhere = mappedFile.getFlushedPosition() 
                + mappedFile.getFileFromOffset();
        }
        
        return result;
    }
}
```

### 6.11 AllocateMappedFileService：文件预分配

这是一个后台预热服务，提前创建下一个 MappedFile，避免写入时等待文件创建：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/AllocateMappedFileService.java

public class AllocateMappedFileService extends ServiceThread {
    // 请求队列
    private ConcurrentMap<String, AllocateRequest> requestTable =
        new ConcurrentHashMap<>();
    
    // 条件队列
    private ConcurrentMap<String, AllocateRequest> requestQueue =
        new ConcurrentHashMap<>();
    
    public MappedFile putRequestAndReturnMappedFile(
        String nextFilePath, int nextFileSize) {
        
        AllocateRequest req = new AllocateRequest(nextFilePath, nextFileSize);
        
        // 检查是否已有相同请求
        AllocateRequest prev = requestTable.putIfAbsent(nextFilePath, req);
        if (prev != null) {
            req = prev;
        }
        
        if (req.getResponse() == null) {
            // 还没有预创建好，发送信号让后台线程创建
            boolean hasOffered = this.requestQueue.offer(req);
            if (hasOffered) {
                // 等待创建完成（带超时）
                req.getCountDownLatch().await(...);
            }
        }
        
        // 返回已创建的 MappedFile
        return req.getResponse();
    }
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            // 从队列取出请求
            AllocateRequest req = this.requestQueue.take();
            
            // 创建 MappedFile
            if (req.getMappedFile() == null) {
                MappedFile mappedFile = new DefaultMappedFile(
                    req.getFilePath(), req.getFileSize());
                
                // 预热 mmap（touch every page）
                if (messageStoreConfig.isWarmMappedFileEnable()) {
                    mappedFile.warmMappedFile(...);
                }
                
                req.setResponse(mappedFile);
                req.getCountDownLatch().countDown();
            }
        }
    }
}
```

### 6.12 Store 模块小结

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Store 模块设计总结                                  │
│                                                                      │
│  1. 写入路径优化                                                       │
│     - 所有 Topic 共享一个 CommitLog，保证顺序写                          │
│     - 自旋锁（默认）保证串行写入                                        │
│     - mmap 零拷贝写入                                                 │
│     - 预分配 MappedFile，避免运行时创建文件                              │
│                                                                      │
│  2. 读取路径优化                                                       │
│     - ConsumeQueue 提供快速索引                                        │
│     - 20 字节固定大小，一页可容纳 200 条索引                              │
│     - 消费者通过 ConsumeQueue 定位，再从 CommitLog 读取                  │
│                                                                      │
│  3. 索引层设计                                                         │
│     - ConsumeQueue：按 Topic+QueueId 顺序索引                          │
│     - IndexFile：按 Message Key 哈希索引                               │
│     - 两套索引，互为补充                                                 │
│                                                                      │
│  4. 可靠性保证                                                         │
│     - 三位置模型：wrotePosition > committedPosition > flushedPosition │
│     - 同步刷盘：每条消息立即 fsync                                      │
│     - 异步刷盘：攒一批后 fsync                                          │
│     - StoreCheckpoint：崩溃恢复的起点                                   │
│                                                                      │
│  5. 文件管理                                                          │
│     - MappedFileQueue：有序文件列表管理                                  │
│     - 自动创建新文件（预分配优化）                                       │
│     - 过期文件清理                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 七、Remoting 模块深度解析

Remoting 模块是 RocketMQ 的通信基石。它基于 Netty 实现了一个完整的 RPC 框架，包含协议编解码、线程模型、超时管理、背压控制等核心组件。所有 RocketMQ 内部节点之间的通信都经过这个模块。

### 7.1 通信协议：RemotingCommand

RemotingCommand 是 RocketMQ 通信层的核心数据结构，所有请求和响应都封装为 RemotingCommand。

#### 7.1.1 RemotingCommand 的字段定义

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingCommand.java

public class RemotingCommand {
    // 请求码（标识请求类型，如 SEND_MESSAGE=10, PULL_MESSAGE=11）
    private int code;
    
    // 语言类型（JAVA, CPP, PYTHON 等）
    private LanguageCode language = LanguageCode.JAVA;
    
    // RocketMQ 版本
    private int version = 0;
    
    // 不透明 ID（请求/响应匹配的唯一标识）
    // AtomicInteger 全局递增
    private int opaque = RequestIdHolder.getInstance().get();
    
    // 标志位
    // bit 0: 0=REQUEST, 1=RESPONSE
    // bit 1: 0=NORMAL, 1=ONEWAY（单向发送，不需要响应）
    private int flag = 0;
    
    // 自定义请求头（注解驱动的字段映射）
    private transient CommandCustomHeader customHeader;
    
    // 扩展字段（Key-Value）
    private HashMap<String, String> extFields;
    
    // 消息体（二进制负载）
    private transient byte[] body;
    
    // 序列化类型
    // JSON = 0, ROCKETMQ = 1（RocketMQ 自定义的二进制序列化）
    private SerializeType serializeType = SerializeType.ROCKETMQ;
}
```

#### 7.1.2 线上格式

RemotingCommand 在网络传输时的二进制格式：

```
┌──────────────────────────────────────────────────────────────────┐
│                    RemotingCommand 线上格式                       │
│                                                                  │
│  ┌──────────────────┐                                            │
│  │ totalLength (4B) │   整个 RemotingCommand 的总长度               │
│  ├──────────────────┤                                            │
│  │ headerLength     │   header 长度 + 序列化类型                    │
│  │ | serializeType  │   (高 24 位为 header 长度，低 8 位为序列化类型) │
│  │ (4B)             │                                            │
│  ├──────────────────┤                                            │
│  │                  │                                            │
│  │  headerData      │   序列化后的 header（JSON 或 RocketMQ 格式）   │
│  │  (variable)      │                                            │
│  │                  │                                            │
│  ├──────────────────┤                                            │
│  │                  │                                            │
│  │  bodyData        │   消息体（二进制负载）                        │
│  │  (variable)      │                                            │
│  │                  │                                            │
│  └──────────────────┘                                            │
│                                                                  │
│  说明：                                                           │
│  - totalLength = 4 (headerLength字段) + headerData长度 + bodyData长度 │
│  - serializeType 的低 8 位：0=JSON, 1=ROCKETMQ                  │
│  - 序列化类型通过 headerLength 字段的低 8 位传递，节省空间           │
└──────────────────────────────────────────────────────────────────┘
```

#### 7.1.3 编码方法

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingCommand.java

public ByteBuffer encode() {
    // 1. 编码 header
    byte[] headerData;
    switch (this.serializeType) {
        case JSON:
            headerData = RemotingSerializable.encode(this);
            break;
        case ROCKETMQ:
            headerData = this.headerEncode();
            break;
        default:
            headerData = RemotingSerializable.encode(this);
            break;
    }
    
    // 2. 计算长度
    // headerLength: 高 24 位是 header 长度，低 8 位是 serializeType
    int headerLength = headerData.length;
    int totalLength = 4 + headerLength + (body != null ? body.length : 0);
    
    // 3. 分配 ByteBuffer
    ByteBuffer result = ByteBuffer.allocate(4 + totalLength);
    
    // 写入 totalLength
    result.putInt(totalLength);
    
    // 写入 headerLength | serializeType
    result.putInt(markProtocolType(headerLength, serializeType.getCode()));
    
    // 写入 headerData
    result.put(headerData);
    
    // 写入 bodyData
    if (body != null) {
        result.put(body);
    }
    
    result.flip();
    return result;
}

// 将 serializeType 编码到 headerLength 字段中
public static int markProtocolType(int source, SerializeType serializeType) {
    return (source << 8) | (serializeType.getCode() & 0xFF);
}

// 从 headerLength 字段中解析 serializeType
public static SerializeType getProtocolType(int source) {
    return SerializeType.valueOf((byte) (source & 0xFF));
}

// 从 headerLength 字段中解析实际 header 长度
public static int getHeaderLength(int source) {
    return (source) >> 8;
}
```

#### 7.1.4 解码方法

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingCommand.java

public static RemotingCommand decode(final ByteBuffer byteBuffer) {
    // 1. 读取 totalLength
    int totalLength = byteBuffer.limit();
    
    // 2. 读取 headerLength | serializeType
    int headerLength = byteBuffer.getInt();
    int realHeaderLength = getHeaderLength(headerLength);
    SerializeType serializeType = getProtocolType(headerLength);
    
    // 3. 读取 headerData
    byte[] headerData = new byte[realHeaderLength];
    byteBuffer.get(headerData);
    
    // 4. 解码 header
    RemotingCommand cmd;
    switch (serializeType) {
        case JSON:
            cmd = RemotingSerializable.decode(headerData, RemotingCommand.class);
            break;
        case ROCKETMQ:
            cmd = RocketMQSerializable.decode(headerData);
            break;
        default:
            throw new RemotingCommandException("Unknown serialize type");
    }
    
    cmd.setSerializeType(serializeType);
    
    // 5. 读取 bodyData
    int bodyLength = totalLength - 4 - realHeaderLength;
    if (bodyLength > 0) {
        byte[] bodyData = new byte[bodyLength];
        byteBuffer.get(bodyData);
        cmd.setBody(bodyData);
    }
    
    return cmd;
}
```

#### 7.1.5 flag 位的含义

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingCommand.java

// bit 0: 是否为响应
public boolean isResponseType() {
    return (this.flag & 0b0001) == 0b0001;
}

// bit 1: 是否为单向发送
public boolean isOnewayRPC() {
    return (this.flag & 0b0010) == 0b0010;
}

// 标记为响应
public void markResponseType() {
    this.flag |= 0b0001;
}

// 标记为单向发送
public void markOnewayRPC() {
    this.flag |= 0b0010;
}

// bit 2: 是否为 RPC 请求类型（用于 hook）
// bit 3: 是否为 async 请求
```

### 7.2 NettyRemotingAbstract：共享引擎

`NettyRemotingAbstract` 是客户端和服务端的共享基类，提供了三种调用模式（同步、异步、单向）的实现。

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

public abstract class NettyRemotingAbstract {
    // ==================== 处理器表 ====================
    // 请求码 -> (处理器, 线程池)
    protected final ConcurrentMap<Integer/* request code */, 
        Pair<NettyRequestProcessor, ExecutorService>> processorTable;
    
    // 默认处理器
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;
    
    // ==================== 响应表 ====================
    // opaque -> ResponseFuture（用于异步响应匹配）
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable;
    
    // ==================== 信号量（背压） ====================
    // 异步调用的并发限制
    protected final Semaphore semaphoreAsync;
    
    // 单向调用的并发限制
    protected final Semaphore semaphoreOneway;
    
    // ==================== 超时管理 ====================
    // HashedWheelTimer 用于扫描超时的 ResponseFuture
    private final HashedWheelTimer timer;
    
    // ==================== 线程池 ====================
    // 处理器回调使用的线程池
    protected ExecutorService callbackExecutor;
}
```

#### 7.2.1 消息分发：processMessageReceived

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
    final int opaque = msg.getOpaque();
    
    if (msg.isResponseType()) {
        // ===== 响应消息 =====
        processResponseCommand(ctx, msg);
    } else {
        // ===== 请求消息 =====
        processRequestCommand(ctx, msg);
    }
}
```

#### 7.2.2 请求处理：processRequestCommand

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
    // 1. 根据请求码查找处理器
    final Pair<NettyRequestProcessor, ExecutorService> matched = 
        this.processorTable.get(cmd.getCode());
    
    // 如果没有找到，使用默认处理器
    final Pair<NettyRequestProcessor, ExecutorService> pair = 
        (matched == null) ? this.defaultRequestProcessor : matched;
    
    // 2. 获取请求的回调线程池
    ExecutorService executor = pair.getObject2();
    
    // 3. 如果需要限制并发
    if (pair.getObject1().rejectRequest()) {
        // 超过并发限制，拒绝请求
        RemotingCommand response = RemotingCommand.createResponseCommand(
            RemotingSysResponseCode.SYSTEM_BUSY, 
            "[REJECTREQUEST]system busy, start flow control");
        response.setOpaque(opaque);
        ctx.writeAndFlush(response);
        return;
    }
    
    // 4. 创建 RequestTask 并提交到线程池
    RequestTask requestTask = new RequestTask(
        () -> {
            try {
                // 执行处理器
                RemotingCommand response = pair.getObject1()
                    .processRequest(ctx, cmd);
                
                // 设置 opaque
                response.setOpaque(opaque);
                response.markResponseType();
                
                // 执行 RPC Hook
                if (response != null) {
                    for (RPCHook rpcHook : rpcHooks) {
                        rpcHook.doAfterResponse(...);
                    }
                }
                
                // 如果不是单向请求，发送响应
                if (!cmd.isOnewayRPC()) {
                    ctx.writeAndFlush(response);
                }
                
            } catch (Exception e) {
                // 处理异常
                if (!cmd.isOnewayRPC()) {
                    RemotingCommand response = ...createErrorResponse(...);
                    ctx.writeAndFlush(response);
                }
            }
        },
        cmd,    // 请求命令
        ctx,    // ChannelHandlerContext
        pair    // 处理器-线程池对
    );
    
    // 5. 提交到线程池（可能被拒绝）
    try {
        executor.submit(requestTask);
    } catch (RejectedExecutionException e) {
        // 线程池队列已满
        RemotingCommand response = ...createErrorResponse(SYSTEM_BUSY, ...);
        ctx.writeAndFlush(response);
    }
}
```

关键设计点：
- 每个请求码绑定一个独立的线程池，实现线程隔离
- 如果线程池队列已满，返回 `SYSTEM_BUSY` 响应
- 单向请求（Oneway）不发送响应

#### 7.2.3 响应处理：processResponseCommand

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
    final int opaque = cmd.getOpaque();
    
    // 从响应表中移除对应的 ResponseFuture
    final ResponseFuture responseFuture = responseTable.remove(opaque);
    
    if (responseFuture != null) {
        // 设置响应
        responseFuture.setResponseCommand(cmd);
        
        if (responseFuture.getInvokeCallback() != null) {
            // 异步调用：执行回调
            executeInvokeCallback(responseFuture);
        } else {
            // 同步调用：唤醒等待线程
            responseFuture.putResponse(cmd);
            responseFuture.getCountDownLatch().countDown();
        }
    } else {
        // 找不到对应的请求（可能已超时被清理）
        log.warn("receive response, but not matched any request, " + opaque);
    }
}
```

#### 7.2.4 三种调用模式

##### 同步调用：invokeSyncImpl

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

public RemotingCommand invokeSyncImpl(
    final Channel channel, 
    final RemotingCommand request,
    final long timeoutMillis) {
    
    final int opaque = request.getOpaque();
    
    try {
        // 1. 创建 ResponseFuture
        final ResponseFuture responseFuture = new ResponseFuture(
            channel, opaque, timeoutMillis, null, null);
        
        // 2. 注册到响应表
        responseTable.put(opaque, responseFuture);
        
        // 3. 注册超时任务（HashedWheelTimer）
        timer.newTimeout(
            timeout -> {
                // 超时后从响应表移除
                ResponseFuture future = responseTable.remove(opaque);
                if (future != null) {
                    future.setSendRequestOK(false);
                    future.putResponse(null);
                    future.getCountDownLatch().countDown();
                }
            },
            timeoutMillis, TimeUnit.MILLISECONDS
        );
        
        // 4. 发送请求
        channel.writeAndFlush(request).addListener(
            (ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    responseFuture.setSendRequestOK(true);
                } else {
                    // 发送失败
                    responseFuture.setSendRequestOK(false);
                    responseTable.remove(opaque);
                    responseFuture.getCountDownLatch().countDown();
                }
            }
        );
        
        // 5. 等待响应（阻塞）
        RemotingCommand responseCommand = null;
        try {
            responseCommand = responseFuture.waitResponse(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return responseCommand;
        
    } finally {
        // 6. 清理响应表
        responseTable.remove(opaque);
    }
}
```

同步调用的流程图：

```
┌─────────────┐                          ┌─────────────┐
│   Client    │                          │   Server    │
│             │                          │             │
│  invokeSync │                          │             │
│      │      │                          │             │
│      ▼      │   1. 创建 ResponseFuture  │             │
│  注册到      │      并注册到 responseTable│             │
│  responseTable                          │             │
│             │                          │             │
│             │   2. writeAndFlush        │             │
│      │      │   ───────────────────►   │             │
│      │      │      RemotingCommand      │             │
│      │      │                          │  processRequest
│      │      │                          │  Command()
│      │      │                          │      │      │
│      ▼      │                          │      ▼      │
│  CountDownLatch                        │  执行处理器
│  await()    │                          │  返回响应
│  (阻塞)     │                          │      │      │
│             │                          │      ▼      │
│             │   3. writeAndFlush        │             │
│             │   ◄───────────────────   │  RemotingCommand
│             │      RemotingCommand      │  (Response) │
│      │      │                          │             │
│      ▼      │                          │             │
│  processResponse                       │             │
│  Command()  │                          │             │
│  唤醒 CountDownLatch                    │             │
│  返回 response                          │             │
│             │                          │             │
└─────────────┘                          └─────────────┘
```

##### 异步调用：invokeAsyncImpl

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

public void invokeAsyncImpl(
    final Channel channel, 
    final RemotingCommand request,
    final long timeoutMillis,
    final InvokeCallback invokeCallback) {
    
    final int opaque = request.getOpaque();
    
    // 1. 尝试获取信号量（背压控制）
    boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
    
    if (!acquired) {
        // 信号量已满，触发拒绝策略
        if (invokeCallback != null) {
            invokeCallback.operationFail(new RemotingTooMuchRequestException(...));
        }
        return;
    }
    
    // 2. 创建 ResponseFuture（带回调）
    final ResponseFuture responseFuture = new ResponseFuture(
        channel, opaque, timeoutMillis, invokeCallback, null);
    
    responseTable.put(opaque, responseFuture);
    
    // 3. 注册超时
    timer.newTimeout(
        timeout -> {
            ResponseFuture future = responseTable.remove(opaque);
            if (future != null) {
                // 超时处理
                future.setSendRequestOK(false);
                executeInvokeCallback(future, ...);
                // 释放信号量
                semaphoreAsync.release();
            }
        },
        timeoutMillis, TimeUnit.MILLISECONDS
    );
    
    // 4. 发送请求
    channel.writeAndFlush(request).addListener(
        (ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                responseFuture.setSendRequestOK(true);
            } else {
                // 发送失败
                responseTable.remove(opaque);
                executeInvokeCallback(responseFuture, ...);
                semaphoreAsync.release();
            }
        }
    );
}

// 响应到达时执行回调
private void executeInvokeCallback(ResponseFuture responseFuture) {
    // 释放信号量
    responseFuture.release();
    
    // 在回调线程池中执行
    callbackExecutor.submit(() -> {
        if (responseFuture.getInvokeCallback() != null) {
            responseFuture.getInvokeCallback().operationComplete(responseFuture);
        }
    });
}
```

异步调用使用信号量实现背压——当在途（in-flight）的异步请求超过信号量许可数时，新的请求会被拒绝。

##### 单向调用：invokeOnewayImpl

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

public void invokeOnewayImpl(
    final Channel channel, 
    final RemotingCommand request,
    final long timeoutMillis) {
    
    final int opaque = request.getOpaque();
    request.markOnewayRPC();  // 设置 oneway 标志位
    
    // 1. 获取信号量
    boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, ...);
    
    if (!acquired) {
        throw new RemotingTooMuchRequestException("...");
    }
    
    // 2. 注册超时清理（单向请求不注册 ResponseFuture）
    timer.newTimeout(
        timeout -> semaphoreOneway.release(),
        timeoutMillis, TimeUnit.MILLISECONDS
    );
    
    // 3. 发送请求（不等待响应）
    channel.writeAndFlush(request).addListener(
        (ChannelFutureListener) future -> {
            // 无论成功失败，都释放信号量
            semaphoreOneway.release();
        }
    );
}
```

#### 7.2.5 超时清理：scanResponseTable

RocketMQ 使用 HashedWheelTimer（时间轮）管理超时。这是一种高效的超时管理算法：

```
┌──────────────────────────────────────────────────────────────────┐
│                    HashedWheelTimer（时间轮）                      │
│                                                                  │
│  原理：                                                           │
│  - 一个固定大小的数组（轮），每个槽位代表一段时间                      │
│  - 指针按固定速度旋转，每转过一个槽位就处理该槽位的所有超时任务         │
│  - 超时时间 = (轮数 × 轮大小 + 槽位) × tickDuration                │
│                                                                  │
│  ┌────┬────┬────┬────┬────┬────┬────┬────┐                      │
│  │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │ 6  │ 7  │                      │
│  │Task│    │Task│    │    │    │    │    │                      │
│  │  A │    │  B │    │    │    │    │    │                      │
│  └────┴────┴────┴────┴────┴────┴────┴────┘                      │
│    ▲                                                             │
│    │                                                             │
│   指针（每 tickDuration 转过一个槽位）                              │
│                                                                  │
│  优势：                                                           │
│  - O(1) 的注册和取消                                              │
│  - 不需要遍历整个超时列表                                           │
│  - 适合大量短超时任务                                               │
└──────────────────────────────────────────────────────────────────┘
```

### 7.3 NettyRemotingServer：Netty 服务端

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingServer.java

public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    
    private final NettyServerConfig nettyServerConfig;
    
    // 服务端 Netty Bootstrap
    private ServerBootstrap serverBootstrap;
    
    // Boss EventLoopGroup（接收连接）
    private EventLoopGroup eventLoopGroupBoss;
    
    // Worker EventLoopGroup（处理 I/O）
    private EventLoopGroup eventLoopGroupSelector;
    
    // 默认 ChannelHandler（服务端业务处理器）
    private NettyServerHandler nettyServerHandler;
    
    // 多端口支持
    private final ConcurrentMap<Port, RemotingServer> remotingServerTable;
    
    public void start() {
        // 1. 创建 EventLoopGroup
        this.eventLoopGroupBoss = new NioEventLoopGroup(
            nettyServerConfig.getBossGroupThreadNum(), 
            new ThreadFactoryImpl("NettyServerBossThread_"));
        
        this.eventLoopGroupSelector = new NioEventLoopGroup(
            nettyServerConfig.getSelectorGroupThreadNum(),
            new ThreadFactoryImpl("NettyServerWorkerThread_"));
        
        // 2. 配置 Bootstrap
        ServerBootstrap childHandler = this.serverBootstrap
            .group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
            .channel(NioServerSocketChannel.class)
            .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
            .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getServerSocketBacklog())
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
            .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    // 3. 配置 Pipeline
                    ch.pipeline()
                        .addLast(new HandshakeHandler(...))       // TLS 握手
                        .addLast(new NettyEncoder())                // 编码器
                        .addLast(new NettyDecoder())               // 解码器
                        .addLast(new DistributionHandler())        // 分布式处理
                        .addLast(new IdleStateHandler(0, 0, 
                            nettyServerConfig.getServerChannelMaxIdleTimeSeconds())) // 空闲检测
                        .addLast(new ConnectManageHandler())        // 连接管理
                        .addLast(nettyServerHandler);               // 业务处理
                }
            });
        
        // 4. 绑定端口
        ChannelFuture future = this.serverBootstrap.bind().sync();
        this.channel = future.channel();
    }
}
```

#### 7.3.1 Pipeline 解析

```
┌──────────────────────────────────────────────────────────────────┐
│                    Netty Server Pipeline                          │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ HandshakeHandler                                           │  │
│  │   - 处理 TLS 握手                                          │  │
│  │   - 如果配置了 TLS，通过 SNI 协议选择证书                     │  │
│  │   - 将明文连接升级为 TLS 连接                                │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │ NettyEncoder                                                │  │
│  │   - 将 RemotingCommand 编码为 ByteBuf                       │  │
│  │   - 调用 RemotingCommand.encode()                           │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │ NettyDecoder                                                │  │
│  │   - 将 ByteBuf 解码为 RemotingCommand                       │  │
│  │   - 处理粘包/拆包：                                          │  │
│  │     1. 读取 totalLength (4B)                                │  │
│  │     2. 如果不足 totalLength+4B，等待更多数据                  │  │
│  │     3. 读取完整的 RemotingCommand                           │  │
│  │     4. 调用 RemotingCommand.decode()                        │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │ DistributionHandler                                        │  │
│  │   - 分布式处理：根据请求码路由到不同的处理线程池                 │  │
│  │   - 实现 Netty 的 EventLoop 到业务线程池的过渡                 │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │ IdleStateHandler                                            │  │
│  │   - 空闲检测                                                │  │
│  │   - 超过 serverChannelMaxIdleTimeSeconds 触发 IdleEvent     │  │
│  │   - 用于检测 Broker 与 NameServer 的连接是否存活              │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │ ConnectManageHandler                                       │  │
│  │   - 连接管理                                                │  │
│  │   - 处理连接建立、断开、异常事件                               │  │
│  │   - 调用 ChannelEventListener 通知上层                       │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │ NettyServerHandler                                          │  │
│  │   - 业务处理入口                                             │  │
│  │   - 调用 processMessageReceived()                           │  │
│  │   - 分发到 processRequestCommand() 或 processResponseCommand()│  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

#### 7.3.2 NettyDecoder：粘包/拆包处理

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyDecoder.java

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    
    public NettyDecoder() {
        // 参数：
        // maxFrameLength: 最大帧长度
        // lengthFieldOffset: 长度字段偏移量 = 0（长度字段在最前面）
        // lengthFieldLength: 长度字段长度 = 4（4 字节表示长度）
        // lengthAdjustment: 长度调整 = -4（长度值不含自身 4 字节）
        // initialBytesToStrip: 初始跳过字节 = 0（不跳过任何字节）
        super(
            RemotingCommand.MAX_FRAME_LENGTH,  // 最大帧长度
            0,                                 // 长度字段在偏移 0 处
            4,                                 // 长度字段 4 字节
            0,                                 // 不需要调整
            0                                  // 不跳过字节
        );
    }
    
    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        // 先调用父类解码（处理粘包/拆包）
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        
        if (frame == null) {
            return null;  // 数据不完整，等待更多数据
        }
        
        // 将 ByteBuf 转为 ByteBuffer，调用 RemotingCommand.decode()
        ByteBuffer byteBuffer = frame.nioBuffer();
        return RemotingCommand.decode(byteBuffer);
    }
}
```

### 7.4 NettyRemotingClient：Netty 客户端

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingClient.java

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    
    private final NettyClientConfig nettyClientConfig;
    
    // Bootstrap
    private Bootstrap bootstrap;
    
    // EventLoopGroup
    private EventLoopGroup eventLoopGroupWorker;
    
    // Channel 缓存：地址 -> ChannelWrapper
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables =
        new ConcurrentHashMap<>();
    
    // NameServer 地址列表
    private final List<String> namesrvAddrList = new CopyOnWriteArrayList<>();
    
    // 当前选中的 NameServer 地址（轮询选择）
    private final AtomicInteger namesrvIndex = new AtomicInteger(
        initValueIndex().get());
    
    // Channel 事件监听器
    private ChannelEventListener channelEventListener;
    
    @Override
    public void start() {
        // 1. 创建 EventLoopGroup
        this.eventLoopGroupWorker = new NioEventLoopGroup(
            nettyClientConfig.getClientWorkerThreads(),
            new ThreadFactoryImpl("NettyClientWorkerThread_"));
        
        // 2. 配置 Bootstrap
        this.bootstrap = new Bootstrap()
            .group(this.eventLoopGroupWorker)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new NettyEncoder())               // 编码器
                        .addLast(new NettyDecoder())               // 解码器
                        .addLast(new IdleStateHandler(
                            0, 0, 
                            nettyClientConfig.getClientChannelMaxIdleTimeSeconds())) // 空闲检测
                        .addLast(new NettyConnectManageHandler())   // 连接管理
                        .addLast(new NettyClientHandler());          // 业务处理
                }
            });
    }
    
    // 获取或创建 Channel
    private Channel getAndCreateChannel(final String addr) {
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }
        
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        
        // 创建新连接
        return this.createChannel(addr);
    }
    
    private Channel createChannel(final String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        
        synchronized (this.channelTables) {
            cw = this.channelTables.get(addr);
            if (cw != null) {
                // 再次检查
                if (cw.isOK()) {
                    return cw.getChannel();
                }
            }
            
            // 连接
            ChannelFuture future = this.bootstrap.connect(
                RemotingHelper.string2SocketAddress(addr));
            
            cw = new ChannelWrapper(future);
            this.channelTables.put(addr, cw);
        }
        
        return cw.getChannel();
    }
}
```

#### 7.4.1 ChannelWrapper：连接缓存

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingClient.java

class ChannelWrapper {
    private final ChannelFuture channelFuture;
    
    public boolean isOK() {
        return this.channelFuture != null 
            && this.channelFuture.channel() != null
            && this.channelFuture.channel().isActive();
    }
    
    public Channel getChannel() {
        return this.channelFuture.channel();
    }
    
    public boolean isWritable() {
        return this.channelFuture.channel().isWritable();
    }
}
```

#### 7.4.2 NameServer 轮询选择

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingClient.java

private Channel getAndCreateNameserverChannel() {
    // 如果没有配置 NameServer 地址，尝试从 DNS 获取
    if (this.namesrvAddrList.isEmpty()) {
        this.updateNameServerAddressList(...);
        if (this.namesrvAddrList.isEmpty()) {
            return null;
        }
    }
    
    // 轮询选择
    int index = Math.abs(this.namesrvIndex.getAndIncrement()) 
        % this.namesrvAddrList.size();
    
    String addr = this.namesrvAddrList.get(index);
    
    ChannelWrapper cw = this.channelTables.get(addr);
    if (cw != null && cw.isOK()) {
        return cw.getChannel();
    }
    
    return this.createChannel(addr);
}
```

#### 7.4.3 GO_AWAY 处理

RocketMQ 的客户端支持 `GO_AWAY` 机制——服务端可以告诉客户端"我要维护了，请重连其他节点"：

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingClient.java

// 当收到 GO_AWAY 响应时
private void handleGoAway(RemotingCommand cmd, Channel channel) {
    String addr = RemotingHelper.parseChannelRemoteAddr(channel);
    
    // 关闭当前 Channel
    RemotingHelper.closeChannel(channel);
    
    // 从缓存移除
    this.channelTables.remove(addr);
    
    // 下次请求会重新创建连接（到其他节点）
}
```

### 7.5 Remoting 模块小结

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Remoting 模块设计总结                                │
│                                                                      │
│  1. 协议设计                                                           │
│     - RemotingCommand 封装请求/响应，字段简洁                            │
│     - 线上格式：totalLength(4) + headerLen|serializeType(4) + header + body│
│     - 支持 JSON 和 RocketMQ 两种序列化类型                              │
│     - opaque 全局唯一 ID 实现请求/响应匹配                               │
│     - flag 位实现 REQUEST/RESPONSE 和 ONEWAY 标识                       │
│                                                                      │
│  2. 线程模型                                                           │
│     - 每个请求码绑定独立的线程池                                          │
│     - Netty EventLoop → 业务线程池 → 处理器 → 响应                      │
│     - 线程隔离避免不同类型请求互相阻塞                                    │
│                                                                      │
│  3. 三种调用模式                                                       │
│     - 同步：CountDownLatch 阻塞等待                                     │
│     - 异步：InvokeCallback 回调 + 信号量背压                            │
│     - 单向：不等待响应，信号量背压                                        │
│                                                                      │
│  4. 背压控制                                                           │
│     - 信号量限制在途请求数                                               │
│     - 异步调用：semaphoreAsync                                          │
│     - 单向调用：semaphoreOneway                                         │
│     - 超过限制返回 SYSTEM_BUSY                                          │
│                                                                      │
│  5. 超时管理                                                           │
│     - HashedWheelTimer 时间轮，O(1) 注册超时任务                        │
│     - 超时后清理 ResponseFuture，释放信号量                              │
│                                                                      │
│  6. 连接管理                                                           │
│     - ChannelWrapper 缓存连接                                           │
│     - NameServer 轮询选择                                               │
│     - GO_AWAY 优雅重连                                                  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 八、Client 模块深度解析

Client 模块包含 Producer（消息生产者）和 Consumer（消息消费者）两部分。RocketMQ 的 Consumer 虽然名为 "Push" 模式，但底层实现是 Pull 模式加长轮询。

### 8.1 Producer 深度解析

#### 8.1.1 DefaultMQProducer：Producer 入口

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java

public class DefaultMQProducer extends ClientConfig implements MQProducer {
    // Producer 组名（同一逻辑组内的 Producer 共享）
    private String producerGroup;
    
    // 默认 Topic（用于自动创建 Topic）
    private String createTopicKey = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;
    
    // 单条消息最大大小
    private int maxMessageSize = 1024 * 1024 * 4; // 4MB
    
    // 发送超时
    private int sendMsgTimeout = 3000; // 3 秒
    
    // 异步发送消息体大小限制
    private int sendAsyncMsgBodySizeLimit = 1 << 20; // 1MB
    
    // 压缩阈值（超过则压缩）
    private int compressMsgBodyOverHowmuch = 1024 * 4; // 4KB
    
    // 重试次数（同步发送）
    private int retryTimesWhenSendFailed = 2;
    
    // 重试次数（异步发送）
    private int retryTimesWhenSendAsyncFailed = 2;
    
    // 是否重试其他 Broker
    private boolean retryAnotherBrokerWhenNotStoreOK = false;
    
    // 最大消息大小（含属性）
    private int maxMessageBodySize = 0;
    
    // Producer 实现
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;
    
    // 发送消息 Hook
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    
    // 事务监听器
    private TransactionListener transactionListener;
    
    @Override
    public void start() {
        this.defaultMQProducerImpl.start();
    }
    
    @Override
    public SendResult send(Message msg) {
        return this.defaultMQProducerImpl.send(msg);
    }
    
    @Override
    public void send(Message msg, SendCallback sendCallback) {
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }
    
    @Override
    public void sendOneway(Message msg) {
        this.defaultMQProducerImpl.sendOneway(msg);
    }
    
    @Override
    public MessageQueue selectOneMessageQueue(List<MessageQueue> mqs, Message lastMsg) {
        return this.defaultMQProducerImpl.selectOneMessageQueue(mqs, lastMsg);
    }
}
```

#### 8.1.2 DefaultMQProducerImpl：Producer 实现

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java

public class DefaultMQProducerImpl implements MQProducerInner {
    private final DefaultMQProducer defaultMQProducer;
    
    // MQClientInstance（同一 JVM 内共享）
    private MQClientInstance mQClientFactory;
    
    // 发送消息 Hook
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    
    // 故障延迟策略
    private MQFaultStrategy mQFaultStrategy;
    
    // 异步发送信号量（背压）
    private final Semaphore semaphoreAsyncSendNum = ...;
    
    // 启动方法
    public void start() {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                
                // 1. 检查配置
                this.checkConfig();
                
                // 2. 获取 MQClientInstance（单例共享）
                this.mQClientFactory = MQClientManager.getInstance()
                    .getAndCreateMQClientInstance(this.defaultMQProducer, rpcHook);
                
                // 3. 注册 Producer
                boolean registerOK = mQClientFactory.registerProducer(
                    this.defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    throw new MQClientException("...");
                }
                
                // 4. 启动 MQClientInstance
                mQClientFactory.start();
                
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case SHUTDOWN_ALREADY:
            case START_FAILED:
                throw new MQClientException("...");
        }
    }
}
```

#### 8.1.3 sendDefaultImpl：发送核心逻辑

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java

private SendResult sendDefaultImpl(
    Message msg,
    final CommunicationMode communicationMode,
    final SendCallback sendCallback,
    final long timeout) {
    
    // 1. 检查消息合法性
    this.makeSureStateOK();
    Validators.checkMessage(msg, this.defaultMQProducer);
    
    // 2. 获取 Topic 路由信息
    TopicPublishInfo topicPublishInfo = 
        this.tryToFindTopicPublishInfo(msg.getTopic());
    
    if (topicPublishInfo == null || !topicPublishInfo.ok()) {
        // 路由不存在，触发更新
        this.mQClientFactory.updateTopicRouteInfoFromNameServer(msg.getTopic());
        topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
    }
    
    if (topicPublishInfo != null && topicPublishInfo.ok()) {
        // 3. 计算发送超时
        long costTime = System.currentTimeMillis() - beginStartTime;
        long timeOutTotal = timeout + costTime;
        
        // 4. 重试循环
        int timesTotal = communicationMode == CommunicationMode.SYNC 
            ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() 
            : 1;
        
        // 记录上次发送的 Broker
        String[] brokersSent = new String[timesTotal];
        
        for (; times < timesTotal; times++) {
            // 5. 选择 MessageQueue
            MessageQueue mqSelected = this.selectOneMessageQueue(
                topicPublishInfo, lastBrokerName);
            
            if (mqSelected != null) {
                brokersSent[times] = mqSelected.getBrokerName();
                
                try {
                    // 6. 发送消息（核心）
                    sendResult = this.sendKernelImpl(
                        msg,                    // 消息
                        mqSelected,             // 选中的队列
                        communicationMode,      // 通信模式
                        sendCallback,           // 异步回调
                        topicPublishInfo,       // 路由信息
                        timeout - costTime      // 剩余超时
                    );
                    
                } catch (Exception e) {
                    // 发送失败
                    continue;
                }
                
                // 7. 处理结果
                switch (communicationMode) {
                    case ASYNC:
                        // 异步：不等待，回调处理
                        return null;
                    case ONEWAY:
                        // 单向：不等待
                        return null;
                    case SYNC:
                        // 同步：检查结果
                        if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                            return sendResult;
                        }
                        // 发送失败但 Store_OK，可以重试其他 Broker
                        if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                            continue;
                        }
                        return sendResult;
                }
            }
        }
        
        // 重试耗尽
        throw new MQClientException("...");
    }
}
```

#### 8.1.4 sendKernelImpl：发送内核

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java

private SendResult sendKernelImpl(
    final Message msg,
    final MessageQueue mq,
    final CommunicationMode communicationMode,
    final SendCallback sendCallback,
    final TopicPublishInfo topicPublishInfo,
    final long timeout) {
    
    // 1. 获取 Broker 地址
    String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(
        mq.getBrokerName());
    
    if (brokerAddr == null) {
        // 找不到 Broker，更新路由
        this.mQClientFactory.updateTopicRouteInfoFromNameServer(
            topicPublishInfo.getMessageTopic());
        brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(
            mq.getBrokerName());
    }
    
    // 2. 设置消息唯一 ID
    if (!msg.getTags().equals("")) {
        msg.setKeys(msg.getTags());
    }
    
    // 3. 设置消息属性
    SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
    requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
    requestHeader.setTopic(msg.getTopic());
    requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
    requestHeader.setQueueId(mq.getQueueId());
    requestHeader.setSysFlag(sysFlag);
    requestHeader.setBornTimestamp(System.currentTimeMillis());
    requestHeader.setProperties(MessageDecoder.messageProperties2String(
        msg.getProperties()));
    requestHeader.setReconsumeTimes(0);
    
    // 4. 执行 SendMessageHook（前置）
    if (this.hasSendMessageHook()) {
        SendMessageContext context = new SendMessageContext(...);
        this.executeSendMessageHookBefore(context);
    }
    
    // 5. 发送请求
    SendResult sendResult = null;
    
    switch (communicationMode) {
        case ASYNC:
            // 异步发送
            MessageInterceptor messageInterceptor = ...;
            this.sendMessageAsync(
                brokerAddr,           // Broker 地址
                msg.getTopic(),      // Topic
                messageInterceptor,  // 消息拦截器
                msg,                 // 消息
                mq,                  // 队列
                requestHeader,       // 请求头
                sendCallback,        // 回调
                timeout              // 超时
            );
            return null;
            
        case ONEWAY:
            // 单向发送
            this.sendMessageOneway(brokerAddr, msg, requestHeader, timeout);
            return null;
            
        case SYNC:
            // 同步发送
            sendResult = this.sendMessageSync(
                brokerAddr,
                msg.getTopic(),
                msg,
                mq,
                requestHeader,
                timeout
            );
            break;
    }
    
    // 6. 处理发送结果
    if (this.hasSendMessageHook()) {
        SendMessageContext context = new SendMessageContext(...);
        context.setSendResult(sendResult);
        this.executeSendMessageHookAfter(context);
    }
    
    return sendResult;
}
```

#### 8.1.5 MQFaultStrategy：故障延迟策略

RocketMQ 的 Producer 有一个精巧的故障延迟隔离机制——当一个 Broker 响应慢时，在一段时间内不再向它发送消息：

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java

public class MQFaultStrategy {
    private final LatencyFaultTolerance latencyFaultTolerance;
    
    // 消息队列选择器
    public MessageQueue selectOneMessageQueue(
        final TopicPublishInfo tpInfo, 
        final String lastBrokerName,
        final boolean selectBrokerCarryingMessage) {
        
        // 策略 1：如果上次发送失败的 Broker 不是 null，优先选择其他 Broker
        if (lastBrokerName != null) {
            for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                MessageQueue mq = tpInfo.getMessageQueueList().get(i);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    // 检查该 Broker 是否在隔离期
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        return mq;
                    }
                }
            }
        }
        
        // 策略 2：使用 ThreadLocalIndex 递增选择
        int index = tpInfo.getSendWhichQueue().getAndIncrement();
        int pos = Math.abs(index) % tpInfo.getMessageQueueList().size();
        
        // 策略 3：从 pos 开始遍历，找到第一个可用的 Broker
        for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
            MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
            pos = (pos + 1) % tpInfo.getMessageQueueList().size();
            
            if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                return mq;
            }
        }
        
        // 策略 4：所有 Broker 都在隔离期，选一个隔离期最短的
        return tpInfo.getMessageQueueList().get(pos);
    }
    
    // 发送完成后更新延迟状态
    public void updateFaultItem(
        final String brokerName,    // Broker 名称
        final long currentLatency,   // 当前延迟（ms）
        final boolean isolation,     // 是否隔离
        final long maxSendLatencyFaultTolerable) {
        
        // 计算隔离时间
        // 根据延迟查表得到隔离时长
        long[] latencyMax = {50, 100, 550, 1000, 2000, 3000, 5000, 10000};
        long[] notAvailableDuration = {0, 0, 30000, 60000, 180000, 600000, 1200000, 3600000};
        
        for (int i = 0; i < latencyMax.length; i++) {
            if (currentLatency <= latencyMax[i]) {
                this.latencyFaultTolerance.updateFaultItem(
                    brokerName, currentLatency, notAvailableDuration[i]);
                break;
            }
        }
    }
}
```

#### 8.1.6 LatencyFaultToleranceImpl：故障容错实现

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/latency/LatencyFaultToleranceImpl.java

public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    
    // 故障项表：brokerName -> FaultItem
    private final ConcurrentMap<String, FaultItem> faultItemTable =
        new ConcurrentHashMap<>();
    
    @Override
    public void updateFaultItem(
        final String name,            // Broker 名称
        final long currentLatency,    // 当前延迟
        final long notAvailableDuration // 不可用时长
    ) {
        FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem == null) {
            faultItem = new FaultItem(name);
            FaultItem prev = this.faultItemTable.putIfAbsent(name, faultItem);
            if (prev != null) {
                faultItem = prev;
            }
        }
        
        // 更新延迟和不可用时长
        faultItem.setCurrentLatency(currentLatency);
        faultItem.setNotAvailableDuration(notAvailableDuration);
    }
    
    @Override
    public boolean isAvailable(final String name) {
        FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            // 检查是否在隔离期
            return faultItem.isAvailable();
        }
        return true;
    }
    
    // FaultItem：故障项
    class FaultItem implements Comparable<FaultItem> {
        private final String name;                       // Broker 名称
        private volatile long currentLatency;             // 当前延迟
        private volatile long startTimestamp;             // 隔离结束时间
        private volatile long notAvailableDuration;        // 不可用时长
        
        public boolean isAvailable() {
            // 如果当前时间 >= 隔离结束时间，则可用
            return System.currentTimeMillis() >= startTimestamp;
        }
        
        @Override
        public int compareTo(FaultItem other) {
            // 按可用性排序：先可用的排前面
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable()) return -1;
                else return 1;
            }
            
            // 如果都可用或都不可用，按延迟排序
            if (this.currentLatency < other.currentLatency) return -1;
            if (this.currentLatency > other.currentLatency) return 1;
            
            // 延迟相同，按名称排序
            return this.name.compareTo(other.name);
        }
    }
}
```

延迟-隔离时长映射表：

```
延迟范围(ms)          隔离时长(ms)
─────────────────    ──────────────────
0 ~ 50               0 (不隔离)
50 ~ 100             0 (不隔离)
100 ~ 550            30000 (30 秒)
550 ~ 1000           60000 (1 分钟)
1000 ~ 2000          180000 (3 分钟)
2000 ~ 3000          600000 (10 分钟)
3000 ~ 5000          1200000 (20 分钟)
5000 ~ 10000         3600000 (1 小时)
> 10000              不在此表中（应该隔离更久）

这个映射表的设计理念：
  - 延迟 100ms 以下的 Broker 性能正常，不隔离
  - 延迟 100-550ms 的 Broker 可能负载较高，隔离 30 秒后重试
  - 延迟越高的 Broker，隔离时间越长
  - 这样实现了一种"慢者越慢"的反馈机制
```

### 8.2 Consumer 深度解析

#### 8.2.1 DefaultMQPushConsumer：Consumer 入口

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/consumer/DefaultMQPushConsumer.java

public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {
    
    // Consumer 组名
    private String consumerGroup;
    
    // 消息模型：CLUSTERING（集群）或 BROADCASTING（广播）
    private MessageModel messageModel = MessageModel.CLUSTERING;
    
    // 从哪里开始消费：CONSUME_FROM_LAST_OFFSET / CONSUME_FROM_FIRST_OFFSET
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    
    // 消费策略
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    
    // 消费者实现
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    
    // 消息监听器（业务代码注册的消费回调）
    private MessageListener messageListener;
    
    // 并发消费最小线程数
    private int consumeThreadMin = 20;
    
    // 并发消费最大线程数
    private int consumeThreadMax = 20;
    
    // 拉取消息批量大小
    private int pullBatchSize = 32;
    
    // 每次消费批量大小
    private int consumeMessageBatchMaxSize = 1;
    
    // 拉取间隔（默认 0，表示立即拉取下一批）
    private long pullInterval = 0;
    
    // 拉取单条消息超时
    private long consumerPullTimeoutMillis = 1000 * 30;
    
    // 消费超时（顺序消费模式）
    private long consumeTimeout = 15;  // 分钟
    
    // 消费消息的最大重试次数
    private int maxReconsumeTimes = ...;
    
    // offset 存储（集群模式存储在 Broker，广播模式存储在本地）
    private OffsetStore offsetStore;
}
```

#### 8.2.2 DefaultMQPushConsumerImpl：Consumer 实现

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java

public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    
    // MQClientInstance（共享）
    private MQClientInstance mQClientFactory;
    
    // 拉取消息包装器
    private PullAPIWrapper pullAPIWrapper;
    
    // offset 存储
    private OffsetStore offsetStore;
    
    // 消费服务（并发或顺序）
    private ConsumeMessageService consumeMessageService;
    
    // 负载均衡实现
    private RebalanceImpl rebalanceImpl;
    
    // 消息队列分配策略
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    
    public void start() {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                
                // 1. 检查配置
                this.checkConfig();
                
                // 2. 构建 Subject
                this.copySubscription();
                
                // 3. 获取 MQClientInstance
                this.mQClientFactory = MQClientManager.getInstance()
                    .getAndCreateMQClientInstance(this.defaultMQPushConsumer, rpcHook);
                
                // 4. 设置 OffsetStore
                if (this.defaultMQPushConsumer.getOffsetStore() == null) {
                    if (this.defaultMQPushConsumer.getMessageModel() 
                        == MessageModel.BROADCASTING) {
                        // 广播模式：进度存储在本地文件
                        this.offsetStore = new LocalFileOffsetStore(
                            this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                    } else {
                        // 集群模式：进度存储在 Broker
                        this.offsetStore = new RemoteBrokerOffsetStore(
                            this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                    }
                }
                
                // 加载 offset
                this.offsetStore.load();
                
                // 5. 创建消费服务
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    // 顺序消费
                    this.consumeMessageService = new ConsumeMessageOrderlyService(
                        this, ...);
                } else {
                    // 并发消费
                    this.consumeMessageService = new ConsumeMessageConcurrentlyService(
                        this, ...);
                }
                
                // 6. 启动消费服务
                this.consumeMessageService.start();
                
                // 7. 注册 Consumer
                boolean registerOK = mQClientFactory.registerConsumer(
                    this.defaultMQPushConsumer.getConsumerGroup(), this);
                
                // 8. 启动 MQClientInstance
                mQClientFactory.start();
                
                this.serviceState = ServiceState.RUNNING;
                break;
        }
    }
    
    // 核心方法：拉取消息（由 PullMessageService 调用）
    public void pullMessage(PullRequest pullRequest) {
        // 1. 获取 ProcessQueue（本地消息缓存）
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        
        if (processQueue.isDropped()) {
            return;
        }
        
        // 2. 流控检查
        // 检查本地缓存消息数
        if (processQueue.getMsgCount() > defaultMQPushConsumer
            .getPullThresholdForQueue()) {
            // 缓存消息太多，延迟拉取
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            return;
        }
        
        // 检查缓存消息大小
        if (processQueue.getMaxSpan() > defaultMQPushConsumer
            .getPullThresholdSizeForQueue()) {
            // 缓存消息太大，延迟拉取
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            return;
        }
        
        // 检查并发消费数
        if (!this.consumeOrderly && processQueue.getMaxSpan() 
            > defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
            // 顺序不一致，延迟拉取
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            return;
        }
        
        // 3. 从 Broker 拉取消息
        try {
            this.pullAPIWrapper.pullKernelImpl(
                pullRequest,         // 拉取请求
                ...                  // 各种参数
            );
        } catch (Exception e) {
            this.executePullRequestLater(pullRequest, ...);
        }
    }
}
```

#### 8.2.3 ConsumeMessageConcurrentlyService：并发消费

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageConcurrentlyService.java

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    
    // 消费线程池
    private final ThreadPoolExecutor consumeExecutor;
    
    // 消费者监听器
    private final MessageListenerConcurrently messageListener;
    
    @Override
    public void start() {
        // 启动定时清理过期消息的任务
        this.scheduledExecutorService.scheduleAtFixedRate(
            () -> this.cleanExpireMsg(),
            0, 15, TimeUnit.MINUTES);
    }
    
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispathToConsume) {
        
        // 获取每批消费数量
        final int consumeBatchSize = ...;
        
        if (msgs.size() <= consumeBatchSize) {
            // 整批提交
            ConsumeRequest consumeRequest = new ConsumeRequest(
                msgs, processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        } else {
            // 分批提交
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = ...;
                ConsumeRequest consumeRequest = new ConsumeRequest(
                    msgThis, processQueue, messageQueue);
                this.consumeExecutor.submit(consumeRequest);
            }
        }
    }
    
    // 消费任务
    class ConsumeRequest implements Runnable {
        @Override
        public void run() {
            List<MessageExt> msgs = this.msgs;
            ProcessQueue processQueue = this.processQueue;
            
            // 1. 检查 ProcessQueue 是否已丢弃
            if (!processQueue.isDropped()) {
                // 2. 调用业务监听器消费消息
                ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(...);
                ConsumeConcurrentlyStatus status = null;
                
                try {
                    // 执行业务代码
                    status = messageListener.consumeMessage(msgs, context);
                } catch (Exception e) {
                    // 消费异常
                    status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                
                // 3. 处理消费结果
                switch (status) {
                    case CONSUME_SUCCESS:
                        // 消费成功
                        break;
                    case RECONSUME_LATER:
                        // 需要重试
                        // 发送到重试 Topic
                        break;
                }
                
                // 4. 更新消费进度
                long offset = ...;
                DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(
                    messageQueue, offset, true);
            }
        }
    }
}
```

#### 8.2.4 ConsumeMessageOrderlyService：顺序消费

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageOrderlyService.java

public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    
    @Override
    public void start() {
        // 启动定时锁定任务（每 20 秒锁定一次）
        if (MessageModel.CLUSTERING.equals(...)) {
            this.scheduledExecutorService.scheduleAtFixedRate(
                () -> this.lockMQPeriodically(),
                1000 * 10, 1000 * 20, TimeUnit.MILLISECONDS);
        }
    }
    
    class ConsumeRequest implements Runnable {
        @Override
        public void run() {
            // 1. 获取分布式锁
            if (!processQueue.isLocked()) {
                // 还没锁定该队列，稍后重试
                ConsumeMessageOrderlyService.this
                    .submitConsumeRequestLater(this);
                return;
            }
            
            // 2. 获取本地锁（每个 MessageQueue 一把锁）
            final Lock lock = processQueue.getConsumeLock();
            lock.lock();
            try {
                // 3. 检查是否已丢弃
                if (processQueue.isDropped()) {
                    return;
                }
                
                // 4. 循环消费
                while (!processQueue.isDropped() && !processQueue.isEmpty()) {
                    // 从 ProcessQueue 取出一批消息
                    List<MessageExt> msgs = processQueue.takeMessages(consumeBatchSize);
                    
                    // 调用业务监听器
                    ConsumeOrderlyStatus status = messageListener.consumeMessage(msgs, context);
                    
                    switch (status) {
                        case SUCCESS:
                            // 消费成功
                            break;
                        case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                            // 暂停消费，稍后重试
                            // 不会重试发送到重试队列
                            break;
                    }
                    
                    // 更新消费进度
                    processQueue.updateConsumeResult(status, msgs);
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
```

顺序消费的双锁机制：

```
┌──────────────────────────────────────────────────────────────────────┐
│                    顺序消费双锁机制                                     │
│                                                                      │
│  第一层：分布式锁（Broker 上的 RebalanceLockManager）                  │
│    - Consumer 启动时，向 Broker 发送 LOCK_BATCH_MQ 请求                │
│    - Broker 记录 (group@topic@queueId) -> clientId                    │
│    - 锁定成功后才能消费该队列                                            │
│    - 每 20 秒续锁一次                                                   │
│    - 作用：保证同一时刻只有一个 Consumer 消费该队列                       │
│                                                                      │
│  第二层：本地锁（ProcessQueue 上的锁）                                   │
│    - 每个 MessageQueue 对应一个 ProcessQueue                           │
│    - ProcessQueue 内部维护一把 ReentrantLock                            │
│    - 消费前必须获取该锁                                                  │
│    - 作用：保证单个 Consumer 内消费线程串行执行                          │
│                                                                      │
│  流程：                                                               │
│  1. Rebalance 分配 MessageQueue                                       │
│  2. 向 Broker 发送 LOCK_BATCH_MQ 请求                                  │
│  3. 收到 LOCK_OK 后，开始消费                                          │
│  4. 每次消费前获取本地锁                                                 │
│  5. 消费完成，更新 offset                                               │
│  6. 定时续锁（20 秒）                                                   │
│  7. Rebalance 触发队列移除时，先释放锁                                   │
└──────────────────────────────────────────────────────────────────────┘
```

#### 8.2.5 RebalanceImpl：负载均衡

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java

public abstract class RebalanceImpl {
    
    // 分配策略
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    
    // 拉取消息包装器
    protected PullAPIWrapper pullAPIWrapper;
    
    // Consumer 实例
    protected MQConsumerInner consumerConfig;
    
    // 分配的 MessageQueue -> ProcessQueue 映射
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable;
    
    public void doRebalance() {
        // 遍历所有订阅的 Topic
        for (String topic : this.getSubscription().keySet()) {
            this.rebalanceByTopic(topic);
        }
    }
    
    private void rebalanceByTopic(final String topic) {
        // 1. 获取 Topic 路由信息
        TopicRouteData topicRouteData = this.mQClientFactory.getAnexistTopicRouteData(topic);
        
        // 2. 获取所有 MessageQueue
        Set<MessageQueue> mqSet = ...;
        
        // 3. 获取同一 Consumer Group 中的所有 Consumer ID
        List<String> cidAll = this.mQClientFactory.findConsumerIdList(
            topic, this.consumerGroup);
        
        // 4. 分配策略
        List<MessageQueue> allocateResult = this.allocateMessageQueueStrategy.allocate(
            this.consumerGroup,          // Consumer Group
            this.mQClientFactory.getClientId(),  // 当前 Consumer ID
            mqAll,                       // 所有 MessageQueue
            cidAll                       // 所有 Consumer ID
        );
        
        // 5. 更新分配结果
        Set<MessageQueue> allocateResultSet = new HashSet<>(allocateResult);
        
        // 比较当前分配和新分配
        // 新增的队列：创建 PullRequest
        for (MessageQueue mq : allocateResultSet) {
            if (!processQueueTable.containsKey(mq)) {
                // 创建 ProcessQueue
                ProcessQueue pq = new ProcessQueue();
                
                // 计算消费起始偏移量
                long nextOffset = computePullFromWhere(mq);
                
                // 创建 PullRequest
                PullRequest pullRequest = new PullRequest();
                pullRequest.setConsumerGroup(consumerGroup);
                pullRequest.setMessageQueue(mq);
                pullRequest.setProcessQueue(pq);
                pullRequest.setNextOffset(nextOffset);
                
                processQueueTable.put(mq, pq);
                
                // 提交拉取请求
                this.dispatchPullRequest(pullRequest);
            }
        }
        
        // 移除的队列：丢弃 ProcessQueue
        Iterator<Entry<MessageQueue, ProcessQueue>> it = 
            processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> entry = it.next();
            if (!allocateResultSet.contains(entry.getKey())) {
                // 队列不再分配给当前 Consumer
                ProcessQueue pq = entry.getValue();
                pq.setDropped(true);
                // 释放分布式锁（顺序消费）
                this.removeProcessQueue(entry.getKey(), entry.getValue());
                it.remove();
            }
        }
    }
}
```

#### 8.2.6 AllocateMessageQueueAveragely：平均分配策略

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/consumer/rebalance/AllocateMessageQueueAveragely.java

public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    
    @Override
    public List<MessageQueue> allocate(
        final String consumerGroup,    // Consumer Group
        final String currentCID,       // 当前 Consumer ID
        final List<MessageQueue> mqAll,  // 所有队列
        final List<String> cidAll) {    // 所有 Consumer ID
        
        List<MessageQueue> result = new ArrayList<>();
        
        // 排序（保证所有 Consumer 的分配结果一致）
        Collections.sort(mqAll);
        Collections.sort(cidAll);
        
        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize = mqAll.size() <= cidAll.size() 
            ? 1 
            : (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) 
            ? index * (mqAll.size() / cidAll.size() + 1) 
            : index * (mqAll.size() / cidAll.size()) + mod;
        
        // 取出当前 Consumer 分配到的队列
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get(startIndex + i));
        }
        
        return result;
    }
}
```

平均分配的图解：

```
假设有 8 个队列，3 个 Consumer

Consumer 列表（排序后）: [C1, C2, C3]

分配结果：
  C1: [Q0, Q1, Q2]  (3 个)
  C2: [Q3, Q4, Q5]  (3 个)
  C3: [Q6, Q7]       (2 个)

计算过程：
  mod = 8 % 3 = 2
  averageSize = (8/3) + 1 = 3 (对于 index < mod 的 Consumer)
  averageSize = 8/3 = 2       (对于 index >= mod 的 Consumer)
  
  C1 (index=0): startIndex=0, range=3, 取 [Q0, Q1, Q2]
  C2 (index=1): startIndex=3, range=3, 取 [Q3, Q4, Q5]
  C3 (index=2): startIndex=6, range=2, 取 [Q6, Q7]
```

#### 8.2.7 ProcessQueue：本地消息缓存

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/ProcessQueue.java

public class ProcessQueue {
    // 消息缓存：queueOffset -> MessageExt
    // TreeMap 保证按 offset 排序
    private final TreeMap<Long, MessageExt> msgTreeMap;
    
    // 顺序消费时的锁
    private final Lock consumeLock = new ReentrantLock();
    
    // 是否已丢弃（Rebalance 后队列不再属于当前 Consumer）
    private volatile boolean dropped = false;
    
    // 最后一次拉取时间
    private volatile long lastPullTimestamp;
    
    // 最后一次消费时间
    private volatile long lastConsumeTimestamp;
    
    // 从 Broker 拉取的消息加入缓存
    public boolean putMessage(final List<MessageExt> msgs) {
        try {
            this.msgTreeMapWriteLock.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.msgTreeMapWriteLock.unlock();
            }
        } catch (Exception e) { ... }
        return true;
    }
    
    // 消费完成后从缓存移除
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        try {
            this.msgTreeMapWriteLock.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    msgTreeMap.remove(msg.getQueueOffset());
                }
                
                if (!msgTreeMap.isEmpty()) {
                    // 返回下一个要消费的 offset
                    result = msgTreeMap.firstKey();
                }
            } finally {
                this.msgTreeMapWriteLock.unlock();
            }
        } catch (Exception e) { ... }
        return result;
    }
    
    // 获取最大跨度（第一条和最后一条消息的 offset 差）
    public long getMaxSpan() {
        if (msgTreeMap.isEmpty()) return 0;
        return msgTreeMap.lastKey() - msgTreeMap.firstKey();
    }
}
```

#### 8.2.8 OffsetStore：消费进度存储

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/consumer/store/RemoteBrokerOffsetStore.java

public class RemoteBrokerOffsetStore implements OffsetStore {
    
    // MQClientInstance
    private final MQClientInstance mQClientFactory;
    
    // Consumer Group
    private final String groupName;
    
    // offset 缓存：MessageQueue -> offset
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<>();
    
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        AtomicLong offsetOld = this.offsetTable.get(mq);
        if (offsetOld == null) {
            offsetOld = new AtomicLong(offset);
            AtomicLong prev = this.offsetTable.putIfAbsent(mq, offsetOld);
            if (prev != null) {
                offsetOld = prev;
            }
        }
        
        if (increaseOnly) {
            // 只增不减
            MixAll.compareAndIncreaseOnly(offsetOld, offset);
        } else {
            offsetOld.set(offset);
        }
    }
    
    @Override
    public long readOffset(MessageQueue mq, ReadOffsetType type) {
        if (ReadOffsetType.READ_FROM_MEMORY == type) {
            // 从内存读
            AtomicLong offset = this.offsetTable.get(mq);
            return offset != null ? offset.get() : -1;
        } else if (ReadOffsetType.READ_FROM_STORE == type) {
            // 从 Broker 读
            try {
                long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                if (brokerOffset >= 0) {
                    this.updateOffset(mq, brokerOffset, false);
                    return brokerOffset;
                }
            } catch (Exception e) { ... }
        }
        return -1;
    }
    
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        // 将 offset 持久化到 Broker
        for (MessageQueue mq : mqs) {
            AtomicLong offset = this.offsetTable.get(mq);
            if (offset != null) {
                try {
                    // 发送 UPDATE_CONSUMER_OFFSET 请求到 Broker
                    this.mQClientFactory.getMQClientAPIImpl()
                        .updateConsumerOffset(brokerAddr, groupName, 
                            mq.getTopic(), mq.getQueueId(), offset.get(), ...);
                } catch (Exception e) { ... }
            }
        }
    }
}
```

### 8.3 MQClientInstance：共享客户端实例

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java

public class MQClientInstance {
    
    // Client ID（全局唯一）
    private String clientId;
    
    // 配置
    private final ClientConfig clientConfig;
    
    // Producer 表
    private final ConcurrentMap<String, MQProducerInner> producerTable;
    
    // Consumer 表
    private final ConcurrentMap<String, MQConsumerInner> consumerTable;
    
    // Admin 表
    private final ConcurrentMap<String, MQAdminInner> adminExtTable;
    
    // Netty 客户端
    private final NettyRemotingClient remotingClient;
    
    // 拉取消息服务
    private final PullMessageService pullMessageService;
    
    // 负载均衡服务
    private final RebalanceService rebalanceService;
    
    // MQClientAPI（封装 remoting 调用）
    private final MQClientAPIImpl mQClientAPIImpl;
    
    // 路由信息表
    private final MQClientApiRouteInfo mqClientApiRouteInfo;
    
    // 服务状态
    private ServiceState serviceState;
    
    public void start() {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                
                // 1. 启动 Netty 客户端
                this.mQClientAPIImpl.start();
                
                // 2. 启动定时任务
                this.startScheduledTask();
                
                // 3. 启动 Pull 服务
                this.pullMessageService.start();
                
                // 4. 启动 Rebalance 服务
                this.rebalanceService.start();
                
                // 5. 启动默认 Producer（用于内部消息发送）
                this.defaultMQProducer.getDefaultMQProducerImpl().start();
                
                this.serviceState = ServiceState.RUNNING;
                break;
        }
    }
    
    private void startScheduledTask() {
        // 1. 每 2 分钟：从 NameServer 获取路由信息
        this.scheduledExecutorService.scheduleAtFixedRate(
            () -> MQClientInstance.this.updateTopicRouteInfoFromNameServer(),
            10, 60 * 2, TimeUnit.SECONDS);
        
        // 2. 每 30 秒：向 Broker 发送心跳
        this.scheduledExecutorService.scheduleAtFixedRate(
            () -> MQClientInstance.this.cleanOfflineBroker()
                && MQClientInstance.this.sendHeartbeatToAllBrokerWithLock(),
            30 * 1000, 30 * 1000, TimeUnit.MILLISECONDS);
        
        // 3. 每 5 秒：持久化消费进度
        this.scheduledExecutorService.scheduleAtFixedRate(
            () -> MQClientInstance.this.persistAllConsumerOffset(),
            10 * 1000, 5 * 1000, TimeUnit.MILLISECONDS);
        
        // 4. 每 20 秒：触发负载均衡
        this.scheduledExecutorService.scheduleAtFixedRate(
            () -> MQClientInstance.this.adjustThreadPool(),
            1, 20, TimeUnit.MINUTES);
    }
}
```

MQClientInstance 是一个共享工厂——同一 JVM 内的所有 Producer 和 Consumer 共享一个实例。这带来了以下好处：

```
1. 连接共享
   - 所有 Producer 和 Consumer 共享同一个 Netty 客户端
   - 避免重复建连

2. 定时任务共享
   - 路由更新、心跳发送、进度持久化只需执行一次
   - 避免重复调度

3. 服务共享
   - PullMessageService 和 RebalanceService 全局唯一
   - 为所有 Consumer 提供服务

4. 内存共享
   - 路由信息表共享
   - 避免重复缓存
```

#### 8.3.1 PullMessageService：拉取消息服务

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullMessageService.java

public class PullMessageService extends ServiceThread {
    
    private final MQClientInstance mQClientFactory;
    
    // 拉取请求队列
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue =
        new LinkedBlockingQueue<>();
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 从队列取出拉取请求
                PullRequest pullRequest = this.pullRequestQueue.take();
                
                // 找到对应的 Consumer
                MQConsumerInner consumer = mQClientFactory
                    .selectConsumer(pullRequest.getConsumerGroup());
                
                if (consumer != null) {
                    // 执行拉取
                    DefaultMQPushConsumerImpl impl = 
                        (DefaultMQPushConsumerImpl) consumer;
                    impl.pullMessage(pullRequest);
                }
            } catch (Exception e) { ... }
        }
    }
    
    // 将新的 PullRequest 放入队列
    public void executePullRequestLater(PullRequest pullRequest, long timeDelay) {
        if (!this.isStopped()) {
            this.scheduledExecutorService.schedule(
                () -> this.pullRequestQueue.put(pullRequest),
                timeDelay, TimeUnit.MILLISECONDS);
        }
    }
    
    public void executePullRequestImmediately(PullRequest pullRequest) {
        this.pullRequestQueue.put(pullRequest);
    }
}
```

#### 8.3.2 RebalanceService：负载均衡服务

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceService.java

public class RebalanceService extends ServiceThread {
    
    private final MQClientInstance mQClientFactory;
    
    // 负载均衡间隔（默认 20 秒）
    private static final long WAIT_INTERVAL = 
        Long.parseLong(System.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            // 等待间隔
            this.waitForRunning(WAIT_INTERVAL);
            
            // 执行负载均衡
            this.mQClientFactory.doRebalance();
        }
    }
}
```

### 8.4 Client 模块小结

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Client 模块设计总结                                 │
│                                                                      │
│  1. Producer 设计                                                    │
│     - sendDefaultImpl: 重试 + 故障隔离                                │
│     - MQFaultStrategy: 延迟反馈，慢者越慢                              │
│     - LatencyFaultTolerance: FaultItem 机制                          │
│     - 三种发送模式：同步、异步、单向                                    │
│                                                                      │
│  2. Consumer 设计                                                    │
│     - Push 底层是 Pull + 长轮询                                       │
│     - 流控：本地缓存数 + 缓存大小 + 最大跨度                           │
│     - 并发消费 vs 顺序消费：双锁保证顺序                               │
│     - Rebalance：平均分配策略                                         │
│     - ProcessQueue: TreeMap 本地缓存，按 offset 排序                    │
│                                                                      │
│  3. 共享实例模式                                                      │
│     - MQClientInstance: 一 JVM 一实例                                 │
│     - 共享连接、定时任务、服务                                         │
│     - 减少资源占用                                                    │
│                                                                      │
│  4. 负载均衡                                                          │
│     - 20 秒触发一次 Rebalance                                        │
│     - 动态分配/回收 MessageQueue                                       │
│     - 支持多种分配策略                                                │
│                                                                      │
│  5. 消费进度管理                                                      │
│     - CLUSTERING: 存储在 Broker（RemoteBrokerOffsetStore）             │
│     - BROADCASTING: 存储在本地文件（LocalFileOffsetStore）             │
│     - 定时持久化（5 秒一次）                                           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 九、HA 模块深度解析

HA（High Availability）模块负责 RocketMQ 的主从同步。与 remoting 模块使用 Netty 不同，HA 模块使用原生 Java NIO，以追求最高性能。

### 9.1 HA 架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         HA 架构总览                                      │
│                                                                         │
│                     ┌─────────────────┐                                  │
│                     │ DefaultHAService│                                  │
│                     │ (在 Broker 中)  │                                  │
│                     └───────┬─────────┘                                  │
│                             │                                            │
│             ┌───────────────┼───────────────┐                            │
│             │               │               │                            │
│     ┌───────▼──────┐ ┌──────▼───────┐ ┌────▼───────────┐                │
│     │ AcceptSocket  │ │ GroupTransfer│ │ (Slave 模式时)  │                │
│     │ Service       │ │ Service      │ │                │                │
│     │ (Master)      │ │ (Master)     │ │ DefaultHAClient│                │
│     │               │ │              │ │ (Slave)        │                │
│     │ 接收 Slave     │ │ 等待 Slave   │ │                │                │
│     │ 连接           │ │ ACK          │ │ 连接 Master    │                │
│     └───────┬───────┘ └──────┬───────┘ │ 接收数据       │                │
│             │               │          └───────┬────────┘                │
│             │               │                  │                          │
│     ┌───────▼──────┐       │                  │                          │
│     │ DefaultHA    │       │                  │                          │
│     │ Connection   │       │                  │                          │
│     │ (每 Slave    │       │                  │                          │
│     │  一个)       │       │                  │                          │
│     └──┬───────┬───┘       │                  │                          │
│        │       │           │                  │                          │
│  ┌─────▼──┐ ┌──▼────────┐  │                  │                          │
│  │ Read   │ │ Write     │  │                  │                          │
│  │ Socket │ │ Socket    │  │                  │                          │
│  │ Service│ │ Service   │  │                  │                          │
│  │        │ │           │  │                  │                          │
│  │读取    │ │推送      │  │                  │                          │
│  │Slave   │ │CommitLog │  │                  │                          │
│  │ACK     │ │数据       │  │                  │                          │
│  └────────┘ └──────────┘  │                  │                          │
│                            │                  │                          │
│                            ▼                  ▼                          │
│                    ┌──────────────────────────────┐                      │
│                    │    CommitLog (物理日志)        │                      │
│                    │    (Master 和 Slave 各一份)    │                      │
│                    └──────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### 9.2 DefaultHAService：HA 服务

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/ha/DefaultHAService.java

public class DefaultHAService implements HAService {
    
    // Broker 是否为 Master
    private final BrokerRole role;
    
    // HA 端口（默认 10912 = Broker 端口 + 1）
    private int port;
    
    // ==================== Master 组件 ====================
    // 接收 Slave 连接的服务
    private AcceptSocketService acceptSocketService;
    
    // 等待 Slave ACK 的服务（类似 Raft 的 commit 等待）
    private GroupTransferService groupTransferService;
    
    // 所有 Slave 的连接
    private final List<HAConnection> connectionList = new CopyOnWriteArrayList<>();
    
    // ==================== Slave 组件 ====================
    // Slave 连接 Master 的客户端
    private DefaultHAClient haClient;
    
    @Override
    public void init(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.port = defaultMessageStore.getMessageStoreConfig().getHaListenPort();
        this.role = defaultMessageStore.getMessageStoreConfig().getBrokerRole();
        
        if (role == BrokerRole.SYNC_MASTER || role == BrokerRole.ASYNC_MASTER) {
            // Master 模式：启动 AcceptSocketService 和 GroupTransferService
            this.acceptSocketService = new AcceptSocketService(defaultMessageStore);
            this.groupTransferService = new GroupTransferService(this);
        } else if (role == BrokerRole.SLAVE) {
            // Slave 模式：启动 HAClient
            this.haClient = new DefaultHAClient(defaultMessageStore);
        }
    }
    
    @Override
    public void start() {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        
        if (haClient != null) {
            haClient.start();
        }
    }
    
    @Override
    public void putMessage(final AppendMessageResult result) {
        // Master 写入消息后，通知 GroupTransferService
        this.groupTransferService.notifyTransferSome(
            result.getWroteOffset() + result.getWroteBytes());
    }
    
    @Override
    public boolean isSyncMasterEnable() {
        return role == BrokerRole.SYNC_MASTER;
    }
}
```

### 9.3 DefaultHAConnection：Master 端的 Slave 连接

每个 Slave 连接 Master 时，Master 会创建一个 `DefaultHAConnection`，包含两个线程：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/ha/DefaultHAConnection.java

public class DefaultHAConnection {
    // HA Service
    private final DefaultHAService haService;
    
    // 与 Slave 的 SocketChannel
    private final SocketChannel socketChannel;
    
    // Slave 的地址
    private final String clientAddress;
    
    // 读取 Slave ACK 的线程
    private ReadSocketService readSocketService;
    
    // 向 Slave 推送数据的线程
    private WriteSocketService writeSocketService;
    
    // Slave ACK 的偏移量
    private volatile long slaveRequestOffset = -1;
    
    // Slave 已同步到的偏移量
    private volatile long slaveAckOffset = -1;
    
    public DefaultHAConnection(DefaultHAService haService, SocketChannel socketChannel) {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddress = socketChannel.socket().getRemoteSocketAddress().toString();
        
        this.readSocketService = new ReadSocketService(socketChannel);
        this.writeSocketService = new WriteSocketService(socketChannel);
    }
    
    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }
}
```

#### 9.3.1 WriteSocketService：推送数据到 Slave

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/ha/DefaultHAConnection.java

class WriteSocketService extends ServiceThread {
    
    // 下一次发送的物理偏移量
    private long nextTransferFromWhere = -1;
    
    // 上次发送的数据大小
    private int lastWriteSize = -1;
    
    // 上次写入的时间戳
    private long lastWriteTimestamp = System.currentTimeMillis();
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 1. 等待 Slave 发送 ACK（如果还没收到第一个 ACK）
                if (-1 == slaveRequestOffset) {
                    Thread.sleep(10);
                    continue;
                }
                
                // 2. 确定发送起始位置
                if (-1 == nextTransferFromWhere) {
                    if (0 == slaveRequestOffset) {
                        // Slave 是全新的，从头开始
                        long masterOffset = defaultMessageStore.getMaxPhyOffset();
                        nextTransferFromWhere = masterOffset;
                    } else {
                        // 从 Slave 请求的位置开始
                        nextTransferFromWhere = slaveRequestOffset;
                    }
                }
                
                // 3. 检查是否有新数据
                if (nextTransferFromWhere >= defaultMessageStore.getMaxPhyOffset()) {
                    // 没有新数据
                    // 发送心跳（空包）
                    if (System.currentTimeMillis() - lastWriteTimestamp > 5000) {
                        this.transferToSlave(null, 0, 0);  // 心跳
                    }
                    Thread.sleep(10);
                    continue;
                }
                
                // 4. 从 CommitLog 读取数据
                SelectMappedBufferResult selectResult = 
                    defaultMessageStore.getCommitLogData(nextTransferFromWhere);
                
                if (selectResult == null) {
                    // 数据已被清理
                    nextTransferFromWhere = defaultMessageStore.getMaxPhyOffset();
                    continue;
                }
                
                // 5. 计算发送大小
                int size = selectResult.getSize();
                if (size > HA_TRANSFER_SIZE) {
                    size = HA_TRANSFER_SIZE;  // 限制每次发送大小
                }
                
                // 6. 发送到 Slave
                this.transferToSlave(
                    selectResult.getByteBuffer(),    // 数据
                    size,                           // 大小
                    nextTransferFromWhere            // 偏移量
                );
                
                // 7. 更新偏移量
                nextTransferFromWhere += size;
                lastWriteSize = size;
                
                selectResult.release();
                
            } catch (Exception e) { ... }
        }
    }
    
    // 发送数据到 Slave
    private boolean transferToSlave(ByteBuffer byteBuffer, int size, long physicOffset) {
        // 发送格式：
        // ┌──────────────────┬────────────┬──────────────┐
        // │ physicOffset(8B) │ bodySize(4B)│ body(var)  │
        // └──────────────────┴────────────┴──────────────┘
        
        ByteBuffer buffer = ByteBuffer.allocate(size + 8 + 4);
        buffer.putLong(physicOffset);   // 8B: 物理偏移量
        buffer.putInt(size);             // 4B: 数据大小
        if (size > 0) {
            buffer.put(byteBuffer.array(), 0, size);  // 数据
        }
        buffer.flip();
        
        // 发送
        while (buffer.hasRemaining()) {
            int wrote = socketChannel.write(buffer);
            if (wrote <= 0) break;
        }
        
        lastWriteTimestamp = System.currentTimeMillis();
        return true;
    }
}
```

#### 9.3.2 ReadSocketService：读取 Slave ACK

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/ha/DefaultHAConnection.java

class ReadSocketService extends ServiceThread {
    
    // Slave ACK 消息大小：固定 8 字节
    private static final int READ_MAX_BUFFER_SIZE = 8;
    
    // 读取缓冲区
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    
    // 当前处理位置
    private int processPosition = 0;
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 1. 从 SocketChannel 读取数据
                int readSize = this.socketChannel.read(this.byteBufferRead);
                
                if (readSize > 0) {
                    // 2. 处理读取到的数据
                    this.processReadResponse();
                } else if (readSize == -1) {
                    // Slave 断开
                    break;
                }
                
            } catch (Exception e) { ... }
        }
    }
    
    private void processReadResponse() {
        // ACK 格式：
        // ┌────────────────────┐
        // │ slaveAckOffset(8B) │
        // └────────────────────┘
        
        int readPosition = this.byteBufferRead.position();
        
        // 检查是否读完了 8 字节
        if ((readPosition - this.processPosition) >= 8) {
            long offsetRead = this.byteBufferRead.getLong(this.processPosition);
            
            // 更新 slaveAckOffset
            slaveAckOffset = offsetRead;
            slaveRequestOffset = offsetRead;
            
            // 通知 GroupTransferService
            DefaultHAConnection.this.haService
                .getGroupTransferService().notifyTransferSome(offsetRead);
            
            this.processPosition += 8;
        }
    }
}
```

### 9.4 DefaultHAClient：Slave 端同步客户端

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/ha/DefaultHAClient.java

public class DefaultHAClient extends ServiceThread implements HAClient {
    
    // Master 地址
    private String masterAddress;
    
    // 与 Master 的 SocketChannel
    private SocketChannel socketChannel;
    
    // 上次向 Master 请求的偏移量
    private long currentReportedOffset = 0;
    
    // 上次写入时间
    private long lastWriteTimestamp = System.currentTimeMillis();
    
    // 读取缓冲区
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    
    // 处理位置
    private int processPosition = 0;
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 1. 确保连接 Master
                if (!this.connectMaster()) {
                    Thread.sleep(1000);
                    continue;
                }
                
                // 2. 检查是否长时间没有数据交换
                long interval = System.currentTimeMillis() - lastWriteTimestamp;
                if (interval > HA_TIME_GAP) {
                    // 发送心跳（当前 offset）
                    if (!this.reportSlaveMaxOffset()) {
                        continue;
                    }
                    lastWriteTimestamp = System.currentTimeMillis();
                }
                
                // 3. 从 Master 读取数据
                boolean result = this.processReadEvent();
                
                // 4. 定期上报 offset
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastWriteTimestamp > 1000) {
                    if (!this.reportSlaveMaxOffset()) {
                        continue;
                    }
                    lastWriteTimestamp = currentTime;
                }
                
            } catch (Exception e) { ... }
        }
    }
    
    // 连接 Master
    private boolean connectMaster() {
        if (socketChannel == null || !socketChannel.isConnected()) {
            String address = masterAddress;
            if (address != null) {
                SocketAddress socketAddress = 
                    RemotingUtil.string2SocketAddress(address);
                if (socketAddress != null) {
                    this.socketChannel = SocketChannel.open();
                    this.socketChannel.configureBlocking(true);
                    this.socketChannel.socket().setSoLinger(false, 0);
                    this.socketChannel.socket().setTcpNoDelay(true);
                    this.socketChannel.connect(socketAddress);
                    
                    // 重置当前上报 offset
                    currentReportedOffset = defaultMessageStore.getMaxPhyOffset();
                }
            }
        }
        return this.socketChannel != null && this.socketChannel.isConnected();
    }
    
    // 处理从 Master 读取的数据
    private boolean processReadEvent() {
        int readSize = this.socketChannel.read(this.byteBufferRead);
        
        if (readSize > 0) {
            // 处理数据
            int readPosition = this.byteBufferRead.position();
            int i = 0;
            
            while (readPosition - this.processPosition >= 12) {
                // 数据格式：
                // ┌──────────────────┬────────────┬──────────────┐
                // │ physicOffset(8B) │ bodySize(4B)│ body(var)  │
                // └──────────────────┴────────────┴──────────────┘
                
                long masterPhyOffset = this.byteBufferRead.getLong(this.processPosition);
                int bodySize = this.byteBufferRead.getInt(this.processPosition + 8);
                
                // ==================== 安全检查 ====================
                // Slave 的物理偏移量必须等于 Master 报告的物理偏移量
                long slavePhyOffset = defaultMessageStore.getMaxPhyOffset();
                
                if (slavePhyOffset != 0 && slavePhyOffset != masterPhyOffset) {
                    log.error("masterPhyOffset != slavePhyOffset");
                    // 发生偏移不一致，关闭连接
                    return false;
                }
                
                // ==================== 追加到本地 CommitLog ====================
                if (bodySize > 0) {
                    byte[] body = new byte[bodySize];
                    this.byteBufferRead.position(this.processPosition + 12);
                    this.byteBufferRead.get(body, 0, bodySize);
                    
                    // 追加到本地 CommitLog
                    defaultMessageStore.appendToCommitLog(
                        masterPhyOffset, body, 0, bodySize);
                    
                    // 更新本地 offset
                    currentReportedOffset = masterPhyOffset + bodySize;
                    this.processPosition += 12 + bodySize;
                } else {
                    // 心跳包
                    this.processPosition += 12;
                }
            }
        }
        
        // 上报 offset
        if (!this.reportSlaveMaxOffset()) {
            return false;
        }
        
        return true;
    }
    
    // 上报 Slave 的最大 offset
    private boolean reportSlaveMaxOffset() {
        boolean result = true;
        
        long currentPhyOffset = defaultMessageStore.getMaxPhyOffset();
        
        if (currentPhyOffset > currentReportedOffset) {
            currentReportedOffset = currentPhyOffset;
        }
        
        // 发送格式：
        // ┌────────────────────┐
        // │ slaveAckOffset(8B) │
        // └────────────────────┘
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(currentReportedOffset);
        buffer.flip();
        
        // 发送
        while (buffer.hasRemaining()) {
            int wrote = this.socketChannel.write(buffer);
            if (wrote <= 0) break;
            result = result && wrote > 0;
        }
        
        return result;
    }
}
```

### 9.5 GroupTransferService：Quorum ACK 等待

这个服务是 SYNC_MASTER 模式的核心——Master 写入消息后，需要等待至少一个 Slave 确认收到，才能返回成功给 Producer。这与 Raft 的 commit 等待机制非常相似：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/ha/DefaultHAService.java

class GroupTransferService extends ServiceThread {
    
    // 写请求队列
    private volatile WaitMapGroupObject requestsWrite = new WaitMapGroupObject();
    private volatile WaitMapGroupObject requestsRead = new WaitMapGroupObject();
    
    // 通知有新的数据需要同步
    public void notifyTransferSome(final long offset) {
        // Slave ACK 到达时调用
        // 唤醒等待的 GroupCommitRequest
        for (GroupCommitRequest req : requests) {
            if (offset >= req.getNextOffset()) {
                // Slave 已收到该 offset 的数据
                req.wakeupCustomer(true);
            }
        }
    }
    
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 1. 等待请求
                this.waitForRunning(10);
                
                // 2. 交换读写队列（double-buffer swap）
                this.swapRequests();
                
                // 3. 处理请求
                for (GroupCommitRequest req : requestsRead.getRequests()) {
                    // 检查是否已有 Slave ACK
                    boolean transferOK = false;
                    
                    for (int i = 0; i < 2 && !transferOK; i++) {
                        // 检查所有 Slave 的 ACK offset
                        for (HAConnection conn : haService.getConnectionList()) {
                            if (conn.getSlaveAckOffset() >= req.getNextOffset()) {
                                transferOK = true;
                                break;
                            }
                        }
                        
                        if (!transferOK) {
                            // 等待一会
                            Thread.sleep(100);
                        }
                    }
                    
                    // 唤醒等待的 Producer
                    req.wakeupCustomer(transferOK 
                        ? PutMessageStatus.PUT_OK 
                        : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                }
                
                // 4. 清空读队列
                this.requestsRead.clear();
                
            } catch (Exception e) { ... }
        }
    }
}
```

GroupCommitRequest 的结构：

```java
// GroupCommitRequest 简化版
class GroupCommitRequest {
    private final long nextOffset;  // 需要等待的 offset
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private volatile PutMessageStatus status;
    
    public void wakeupCustomer(PutMessageStatus status) {
        this.status = status;
        this.countDownLatch.countDown();
    }
    
    public PutMessageStatus waitForResponse(long timeout) 
        throws InterruptedException {
        this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        return this.status;
    }
}
```

与 Raft 的 commit 等待对比：

```
┌──────────────────────────────────────────────────────────────────────┐
│           GroupTransferService vs Raft Commit 等待                     │
│                                                                      │
│  Raft:                                                               │
│    1. Leader 写入日志条目                                             │
│    2. Leader 向所有 Follower 发送 AppendEntries                      │
│    3. Follower 收到后追加到本地日志，回复 ACK                          │
│    4. Leader 收到 majority ACK 后 commit                              │
│    5. commit 通过 nextIndex 传播给 Follower                           │
│                                                                      │
│  RocketMQ HA:                                                        │
│    1. Master 写入 CommitLog                                          │
│    2. 创建 GroupCommitRequest，加入等待队列                            │
│    3. WriteSocketService 推送 CommitLog 数据到 Slave                  │
│    4. Slave 收到后追加到本地 CommitLog，回复 ACK（8 字节 offset）       │
│    5. ReadSocketService 收到 ACK，通知 GroupTransferService           │
│    6. GroupTransferService 检查 ACK offset >= 需要的 offset          │
│    7. 满足则唤醒等待的 Producer 请求                                   │
│                                                                      │
│  相似点：                                                             │
│    - 都有"写入 → 等待确认 → 通知完成"的三阶段                         │
│    - 都有超时机制                                                      │
│    - 都有 majority 概念（但 RocketMQ 是任一 Slave 确认即可）            │
│                                                                      │
│  不同点：                                                             │
│    - Raft 需要 majority，RocketMQ 只需任一 Slave 确认                  │
│    - Raft 的 commit 通过 AppendEntries 传播                          │
│    - RocketMQ 的 commit 通过 GroupTransferService 唤醒                │
│    - Raft 有 term 防止过期 Leader                                     │
│    - RocketMQ 没有任期概念（通过 Controller 模式补充）                 │
└──────────────────────────────────────────────────────────────────────┘
```

### 9.6 HA 数据传输格式总结

```
Master → Slave（数据推送）：
┌──────────────────┬────────────┬──────────────────┐
│ physicOffset(8B) │ bodySize(4B)│ body(variable) │
│                  │            │                  │
│ CommitLog 中的    │ 数据大小    │ CommitLog 数据   │
│ 起始偏移量         │            │                  │
└──────────────────┴────────────┴──────────────────┘

Slave → Master（ACK）：
┌────────────────────┐
│ slaveAckOffset(8B) │
│                    │
│ Slave 已同步到的    │
│ CommitLog 偏移量    │
└────────────────────┘

安全检查：
  Slave 在追加数据前检查：
    slavePhyOffset == masterPhyOffset
  
  如果不一致，说明数据有断裂，Slave 拒绝追加并关闭连接。
  这类似于 Raft 的 log matching property：
    prevLogIndex 和 prevLogTerm 必须匹配。
  
  但 RocketMQ 的检查更简单——只检查偏移量是否连续，
  不需要检查 term（因为 RocketMQ 没有任期概念）。
```

---

## 十、消息全链路追踪：从生产到消费

本节将追踪一条消息从 Producer 发出到 Consumer 消费完成的完整链路，涵盖所有模块的协作过程。

### 10.1 全局链路 ASCII 图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    消息全链路追踪                                              │
│                                                                              │
│  ┌──────────┐   1.获取路由    ┌──────────┐                                   │
│  │          │ ──────────────► │          │                                   │
│  │ Producer │                 │NameServer│                                   │
│  │          │ ◄────────────── │          │                                   │
│  │          │   2.TopicRouteData          │                                   │
│  │          │                 └──────────┘                                   │
│  │          │   3.选Queue      ┌──────────┐                                   │
│  │          │ ──────────────► │          │   4. SEND_MESSAGE(10)            │
│  │          │                 │  Broker  │                                   │
│  │          │                 │          │   ┌───────────────────────┐       │
│  │          │                 │          │   │ SendMessageProcessor  │       │
│  │          │                 │          │   │   5.参数校验           │       │
│  │          │                 │          │   │   6.自动建Topic        │       │
│  │          │                 │          │   │   7.选QueueId          │       │
│  │          │                 │          │   │   8.封装MessageInner    │       │
│  │          │                 │          │   └───────────┬───────────┘       │
│  │          │                 │          │               │                     │
│  │          │                 │          │               ▼                     │
│  │          │                 │          │   ┌───────────────────────┐       │
│  │          │                 │          │   │ MessageStore          │       │
│  │          │                 │          │   │   .putMessage()       │       │
│  │          │                 │          │   │      │                │       │
│  │          │                 │          │   │      ▼                │       │
│  │          │                 │          │   │ CommitLog             │       │
│  │          │                 │          │   │   .asyncPutMessage() │       │
│  │          │                 │          │   │      │                │       │
│  │          │                 │          │   │      ├──► putMessageLock│    │
│  │          │                 │          │   │      ├──► MappedFile   │     │
│  │          │                 │          │   │      │     .appendMessage()   │
│  │          │                 │          │   │      │     │           │      │
│  │          │                 │          │   │      │     ▼           │      │
│  │          │                 │          │   │      │  doAppend()    │      │
│  │          │                 │          │   │      │  (字节写入)     │      │
│  │          │                 │          │   │      ▼                │       │
│  │          │                 │          │   │ FlushManager           │       │
│  │          │                 │          │   │   .handleDiskFlush()  │       │
│  │          │                 │          │   │      │                │       │
│  │          │                 │          │   │      ├──同步刷盘       │       │
│  │          │                 │          │   │      │  GroupCommit  │       │
│  │          │                 │          │   │      │  Service      │       │
│  │          │                 │          │   │      │  (等待fsync)   │       │
│  │          │                 │          │   │      │                │       │
│  │          │                 │          │   │      └──异步刷盘      │       │
│  │          │                 │          │   │         FlushRealTime │       │
│  │          │                 │          │   │         Service       │       │
│  │          │                 │          │   │         (定期fsync)   │       │
│  │          │                 │          │   │      │                │       │
│  │          │                 │          │   │      ▼                │       │
│  │          │                 │          │   │ HAService             │       │
│  │          │                 │          │   │   .putMessage()      │       │
│  │          │                 │          │   │      │                │       │
│  │          │                 │          │   │      ▼                │       │
│  │          │                 │          │   │ GroupTransferService  │       │
│  │          │                 │          │   │   (等待Slave ACK)     │       │
│  │          │                 │          │   │      │                │       │
│  │          │                 │          │   │      ▼                │       │
│  │          │                 │          │   │   返回PutMessageResult│       │
│  │          │                 │          │   └───────────┬───────────┘       │
│  │          │                 │          │               │                     │
│  │          │ ◄────────────── │          │  9. SendResult│                     │
│  │          │   8. RemotingCommand│      │               │                     │
│  └──────────┘                 └──────────┘               │                     │
│       │                                                  │                     │
│       │  异步分发（后台线程）                               │                     │
│       │                                                  ▼                     │
│  ┌──────┐                                          ┌──────────┐               │
│  │      │  10. ReputMessageService                 │          │               │
│  │Consumer│   从CommitLog读取消息                   │  Broker  │               │
│  │      │   构建 ConsumeQueue 和 IndexFile          │          │               │
│  │      │                                           │          │               │
│  │      │   11. PullMessage                         │          │               │
│  │      │ ──────────────────────────────────────► │          │               │
│  │      │   PULL_MESSAGE(11)                       │          │               │
│  │      │                                           │          │               │
│  │      │                  ┌──────────────────────┤          │               │
│  │      │                  │ PullMessageProcessor    │          │               │
│  │      │                  │   12. 参数校验          │          │               │
│  │      │                  │   13. 检查订阅组        │          │               │
│  │      │                  │   14. 检查Topic        │          │               │
│  │      │                  │   15. 获取消息          │          │               │
│  │      │                  │       │                │          │               │
│  │      │                  │       ▼                │          │               │
│  │      │                  │  MessageStore         │          │               │
│  │      │                  │   .getMessage()      │          │               │
│  │      │                  │       │                │          │               │
│  │      │                  │       ▼                │          │               │
│  │      │                  │  ConsumeQueue         │          │               │
│  │      │                  │   .getIndicesInBuffer│          │               │
│  │      │                  │       │                │          │               │
│  │      │                  │       ▼                │          │               │
│  │      │                  │  CommitLog            │          │               │
│  │      │                  │   .getMessage()      │          │               │
│  │      │                  │   (按physicalOffset) │          │               │
│  │      │                  │       │                │          │               │
│  │      │                  │       ▼                │          │               │
│  │      │                  │  没有消息?            │          │               │
│  │      │                  │    是 → 挂起(长轮询)    │          │               │
│  │      │                  │    否 → 返回消息       │          │               │
│  │      │                  └──────────────────────┤          │               │
│  │      │ ◄──────────────────────────────────── │          │               │
│  │      │   16. PullResult(RemotingCommand)       │          │               │
│  │      │                                           │          │               │
│  │      ▼                                           │          │               │
│  │ ProcessQueue                                     │          │               │
│  │   .putMessage(msgs)                              │          │               │
│  │      │                                           │          │               │
│  │      ▼                                           │          │               │
│  │ ConsumeMessageService                            │          │               │
│  │   .submitConsumeRequest()                        │          │               │
│  │      │                                           │          │               │
│  │      ▼                                           │          │               │
│  │  ConsumeRequest (线程池执行)                       │          │               │
│  │      │                                           │          │               │
│  │      ▼                                           │          │               │
│  │  MessageListener.consumeMessage()                │          │               │
│  │   (业务代码消费)                                  │          │               │
│  │      │                                           │          │               │
│  │      ▼                                           │          │               │
│  │  更新消费进度                                      │          │               │
│  │  OffsetStore.updateOffset()                       │          │               │
│  └──────┘                                           └──────────┘               │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 详细步骤说明

#### 步骤 1-2：Producer 获取路由

```
Producer.start()
  → MQClientInstance.start()
    → 定时任务：每 2 分钟从 NameServer 更新路由
    → updateTopicRouteInfoFromNameServer(topic)
      → NettyRemotingClient.invokeSync()
        → RemotingCommand(code=GET_ROUTEINFO_BY_TOPIC, body=topic)
        → NameServer 处理：
          → ClientRequestProcessor.processRequest()
          → RouteInfoManager.pickupTopicRouteData(topic)
            → 从 topicQueueTable 获取 QueueData
            → 从 brokerAddrTable 获取 BrokerData
            → 组装 TopicRouteData
          → 返回 TopicRouteData (JSON)
    → 解析 TopicRouteData
    → 更新本地路由表
```

#### 步骤 3-9：消息发送到 Broker

```
Producer.send(msg)
  → DefaultMQProducerImpl.sendDefaultImpl(msg, SYNC, null, timeout)
    → 1. tryToFindTopicPublishInfo(topic) — 从本地路由表获取
    → 2. selectOneMessageQueue() — 选择 MessageQueue
         → MQFaultStrategy.selectOneMessageQueue()
         → 考虑故障隔离，选择可用的 Broker
    → 3. sendKernelImpl(msg, mq, SYNC, null, tpInfo, timeout)
         → 3.1 findBrokerAddressInPublish(brokerName) — 获取 Broker 地址
         → 3.2 设置消息属性（ProducerGroup, Topic, QueueId, BornTimestamp, ...）
         → 3.3 执行 SendMessageHook（前置）
         → 3.4 sendMessageSync(brokerAddr, topic, msg, mq, header, timeout)
              → MQClientAPIImpl.sendMessage()
                → RemotingCommand(code=SEND_MESSAGE, header=SendMessageRequestHeader, body=msg.body)
                → NettyRemotingClient.invokeSync(brokerAddr, request, timeout)
                  → 注册 ResponseFuture
                  → channel.writeAndFlush(request)
                  → CountDownLatch.await(timeout) — 阻塞等待响应
    
    Broker 端：
      → NettyRemotingServer Pipeline
        → NettyDecoder → 解码 RemotingCommand
        → NettyServerHandler → processMessageReceived()
        → processRequestCommand()
          → 查找处理器：processorTable.get(10) = (SendMessageProcessor, sendMessageExecutor)
          → 提交到 sendMessageExecutor 线程池
          → SendMessageProcessor.processRequest()
            → 4. 参数校验（Topic 存在？QueueId 合法？消息大小？权限？）
            → 5. 自动创建 Topic（如果开启 autoCreateTopicEnable）
            → 6. 封装 MessageExtBrokerInner
            → 7. messageStore.putMessage(msgInner)
              → CommitLog.asyncPutMessage(msg)
                → 7.1 putMessageLock.lock()
                → 7.2 MappedFile.appendMessage()
                → 7.3 DefaultAppendMessageCallback.doAppend() — 字节写入
                → 7.4 putMessageLock.unlock()
                → 7.5 FlushManager.handleDiskFlush()
                  → 同步刷盘：GroupCommitService 等待 fsync
                  → 异步刷盘：直接返回
                → 7.6 HAService.putMessage()
                  → GroupTransferService 等待 Slave ACK
                → 7.7 返回 PutMessageResult
            → 8. 构造响应 RemotingCommand
              → 设置 SendStatus, msgId, queueOffset, ...
            → 9. ctx.writeAndFlush(response)
    
    Producer 端：
      → processResponseCommand()
      → ResponseFuture.putResponse(cmd)
      → CountDownLatch.countDown() — 唤醒
      → 解析 SendResult
      → 执行 SendMessageHook（后置）
      → 更新 MQFaultStrategy（延迟反馈）
```

#### 步骤 10：异步分发

```
Broker 后台线程：
  ReputMessageService.run()
    → 每毫秒执行 doReput()
    → 从 CommitLog 读取消息（从 reputFromOffset 开始）
    → 遍历消息
      → 构建 DispatchRequest
      → ConsumeQueue.putMessagePositionInfo(offset, size, tagHash, queueOffset)
        → 写入 20 字节索引项到 ConsumeQueue 的 MappedFile
      → IndexService.buildIndex(topic, keys, offset, timestamp)
        → IndexFile.putKey(key, phyOffset, timestamp)
          → 计算 hash → 定位 slot → 写入索引条目
    → 更新 reputFromOffset
    → 如果有新消息到达 PullRequestHoldService 中的挂起请求
      → PullRequestHoldService.notifyMessageArriving()
      → 唤醒挂起的 Consumer Pull 请求
```

#### 步骤 11-16：消息消费

```
Consumer 端：
  RebalanceService.run() — 每 20 秒触发
    → MQClientInstance.doRebalance()
      → 遍历所有 Consumer
      → DefaultMQPushConsumerImpl.doRebalance()
        → RebalanceImpl.doRebalance()
          → rebalanceByTopic(topic)
            → 获取所有 MessageQueue
            → 获取所有 Consumer ID（从 Broker 获取）
            → AllocateMessageQueueAveragely.allocate() — 分配队列
            → 新分配的队列：创建 PullRequest
            → PullMessageService.executePullRequestImmediately(pullRequest)
  
  PullMessageService.run()
    → 从队列取出 PullRequest
    → DefaultMQPushConsumerImpl.pullMessage(pullRequest)
      → 1. 流控检查（ProcessQueue 消息数/大小/跨度）
      → 2. PullAPIWrapper.pullKernelImpl()
        → MQClientAPIImpl.pullMessage()
          → RemotingCommand(code=PULL_MESSAGE, header=PullMessageRequestHeader)
          → NettyRemotingClient.invokeAsync(addr, request, callback)
    
    Broker 端：
      → PullMessageProcessor.processRequest()
        → 2. 参数校验
        → 3. 检查订阅组配置
        → 4. 检查 Topic 配置
        → 5. 构建 MessageFilter
        → 6. MessageStore.getMessage(group, topic, queueId, offset, maxNums, filter)
          → ConsumeQueue.getIndicesInBuffer(offset, maxNums)
            → 找到对应 MappedFile
            → 读取索引项列表
          → 遍历索引项
            → CommitLog.getMessage(physicalOffset, size)
            → 按 tag 过滤
          → 组装 GetMessageResult
        → 7. 处理结果
          → FOUND: 设置消息列表到 body，返回 SUCCESS
          → NO_MESSAGE: 挂起到 PullRequestHoldService（长轮询）
          → OFFSET_MOVED: 返回正确的 offset
    
    Consumer 端（异步回调）：
      → PullCallback.onSuccess(pullResult)
        → 解析 PullResult
        → PullStatus.FOUND:
          → ProcessQueue.putMessage(msgs) — 加入本地缓存
          → ConsumeMessageService.submitConsumeRequest(msgs, pq, mq, true)
            → ConsumeRequest (Runnable)
              → 提交到 consumeExecutor 线程池
              → ConsumeRequest.run()
                → MessageListener.consumeMessage(msgs, context)
                  → 业务代码处理消息
                  → 返回 ConsumeStatus
                → 更新消费进度
                  → OffsetStore.updateOffset(mq, offset, true)
                  → 定时持久化到 Broker（UPDATE_CONSUMER_OFFSET=15）
        → PullStatus.NO_NEW_MSG / NO_MATCHED_MSG:
          → 下次拉取使用 nextBeginOffset
        → 重新提交 PullRequest
          → PullMessageService.executePullRequestImmediately(nextPullRequest)
```

### 10.3 关键性能点

```
┌──────────────────────────────────────────────────────────────────────┐
│                    消息全链路关键性能点                                  │
│                                                                      │
│  1. 写入路径                                                          │
│     - CommitLog 顺序写：磁盘 IO 始终是顺序写                           │
│     - mmap 零拷贝：消息写入通过 MappedByteBuffer，无系统调用            │
│     - 预分配 MappedFile：避免运行时创建文件                             │
│     - 自旋锁：避免线程切换开销（高并发下优于 ReentrantLock）              │
│                                                                      │
│  2. 分发路径                                                          │
│     - ReputMessageService 每毫秒检查一次                               │
│     - ConsumeQueue 也是顺序写（同一 Topic+QueueId 的消息是连续的）      │
│     - 批量构建索引：一次读取多条消息                                   │
│                                                                      │
│  3. 拉取路径                                                          │
│     - ConsumeQueue 索引固定 20B，一页（4KB）可容纳 204 条索引           │
│     - 从 ConsumeQueue 读取索引是 mmap 读，无系统调用                    │
│     - 从 CommitLog 读取消息也是 mmap 读                                │
│     - 消息通过 ByteBuf 传输，避免多次拷贝                              │
│                                                                      │
│  4. 消费路径                                                          │
│     - 消费线程池并行处理                                               │
│     - ProcessQueue 缓存消息，减少 Pull 等待                            │
│     - 批量消费（consumeMessageBatchMaxSize）                           │
│                                                                      │
│  5. 长轮询优化                                                        │
│     - 消息到达后立即唤醒（不是等到定时检查）                            │
│     - PullRequestHoldService + NotifyMessageArrivingListener          │
│     - 挂起请求不占用线程（由 Netty 的 EventLoop 管理）                  │
│                                                                      │
│  6. 网络层                                                            │
│     - Netty 零拷贝（FileRegion）                                      │
│     - 线程隔离避免不同请求互相阻塞                                     │
│     - 信号量背压控制                                                   │
│                                                                      │
│  7. 主从同步                                                          │
│     - 原生 NIO 避免协议开销                                           │
│     - 直接传输 CommitLog 字节流                                       │
│     - 独立端口，不影响业务流量                                         │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 十一、关键设计决策与工程考量

本节对 RocketMQ 源码中体现的关键设计决策进行系统性总结，并分析每个决策背后的工程考量。

### 11.1 NameServer vs ZooKeeper：CAP 取舍

```
┌──────────────────────────────────────────────────────────────────────┐
│              NameServer vs ZooKeeper 的 CAP 取舍                       │
│                                                                      │
│  CAP 定理：                                                           │
│    C (Consistency): 强一致性                                          │
│    A (Availability): 可用性                                           │
│    P (Partition tolerance): 分区容错                                   │
│    分布式系统只能同时满足其中两个                                       │
│                                                                      │
│  ZooKeeper 的选择：CP                                                  │
│    - 使用 ZAB 协议保证强一致性                                         │
│    - Leader 写入后需要 majority 确认                                   │
│    - 在 Leader 选举期间不可写（牺牲 A）                                 │
│    - 适合需要强一致性的场景（如选主、配置同步）                           │
│                                                                      │
│  NameServer 的选择：AP                                                 │
│    - 不保证强一致性，多个实例间数据可能短暂不一致                         │
│    - 但任何 NameServer 都能独立提供路由服务                              │
│    - Broker 向所有 NameServer 注册，最终一致                            │
│    - 适合路由发现这种"读多写少"的场景                                    │
│                                                                      │
│  RocketMQ 为什么选择 AP？                                              │
│    1. 消息中间件对路由一致性的要求不高                                   │
│       - 短暂的路由不一致最多导致消息延迟 30 秒                          │
│       - 消息不会丢失（Broker 上消息已持久化）                           │
│    2. 可用性更重要                                                     │
│       - NameServer 挂了不影响 Broker 运行                               │
│       - 客户端有本地路由缓存，可以继续发送                               │
│    3. 简化运维                                                        │
│       - 无需维护 ZooKeeper 集群                                        │
│       - NameServer 本身就是简单的 Netty Server                         │
│                                                                      │
│  客户端容错机制                                                        │
│    1. 本地路由缓存：NameServer 不可用时使用缓存                         │
│    2. 发送失败时更新路由表：TriggerUpdateTopicRouteInfo               │
│    3. 延迟隔离：MQFaultStrategy 隔离慢 Broker                           │
│    4. 重试机制：retryTimesWhenSendFailed                               │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.2 CommitLog + ConsumeQueue 分离：读写分离思想

```
┌──────────────────────────────────────────────────────────────────────┐
│              CommitLog + ConsumeQueue 分离的工程考量                    │
│                                                                      │
│  问题：如何在保证高写入吞吐量的同时，支持按 Topic+QueueId 消费？        │
│                                                                      │
│  方案 A（Kafka 风格）：每个 Partition 一个日志                          │
│    优点：消费时可直接顺序读                                            │
│    缺点：Topic 数量增多时，写入退化为随机写                              │
│    适用：Topic 数量较少的场景                                          │
│                                                                      │
│  方案 B（RocketMQ 风格）：共享 CommitLog + ConsumeQueue 索引           │
│    优点：                                                              │
│      - 写入始终是顺序写，无论多少 Topic                                 │
│      - 磁盘 IO 模式单一，便于内核优化                                   │
│      - 文件句柄少，page cache 利用率高                                  │
│    缺点：                                                              │
│      - 消费需要先查 ConsumeQueue，再从 CommitLog 读取                    │
│      - 消费时有一次随机读                                              │
│    适用：Topic 数量多、写入量大的场景                                   │
│                                                                      │
│  RocketMQ 的选择：方案 B                                               │
│    理由：                                                              │
│      1. 阿里内部场景：大量 Topic（数万个），每个 Topic 有多个队列         │
│      2. 写入量远大于消费量：一条消息写一次，可能被消费多次               │
│      3. 顺序写 >> 随机写：尤其在 HDD 上差距 100 倍                     │
│      4. ConsumeQueue 的随机读代价可控：                                 │
│         - ConsumeQueue 也是顺序文件， mmap 后在 page cache 中           │
│         - CommitLog 的随机读也在 page cache 中（热点数据）              │
│         - SSD 上随机读性能也足够好                                     │
│                                                                      │
│  性能数据参考：                                                        │
│    写入：10 万 TPS+（单 Broker）                                      │
│    消费：5 万+ TPS（单 Consumer）                                     │
│    ConsumeQueue 查询：<0.1ms（page cache 命中）                         │
│    CommitLog 读取：0.1-1ms（page cache 命中）                          │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.3 锁策略选择：自旋锁 vs 可重入锁

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/CommitLog.java

// 默认使用自旋锁
if (messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage()) {
    putMessageLock = new PutMessageReentrantLock();
} else {
    putMessageLock = new PutMessageSpinLock();
}
```

```
┌──────────────────────────────────────────────────────────────────────┐
│              自旋锁 vs 可重入锁的选择考量                                │
│                                                                      │
│  自旋锁 (PutMessageSpinLock)                                          │
│    实现：CAS + while 循环                                             │
│    优点：                                                              │
│      - 无线程切换开销                                                 │
│      - 在临界区极短（毫秒级以内）的情况下性能更好                        │
│      - 无死锁风险                                                      │
│    缺点：                                                              │
│      - CPU 空转浪费                                                    │
│      - 竞争激烈时退化为忙等待                                          │
│                                                                      │
│  可重入锁 (PutMessageReentrantLock)                                   │
│    实现：AQS + park/unpark                                            │
│    优点：                                                              │
│      - 竞争激烈时不浪费 CPU                                            │
│      - 支持公平/非公平模式                                             │
│      - 支持重入和条件变量                                              │
│    缺点：                                                              │
│      - 线程切换开销                                                    │
│      - park/unpark 涉及系统调用                                       │
│                                                                      │
│  RocketMQ 的选择：默认自旋锁                                           │
│    理由：                                                              │
│      1. CommitLog 写入的临界区极短（mmap 内存写入 + 少量计算）           │
│      2. 在高并发下，自旋锁避免了线程切换开销                           │
│      3. 实际测试表明，自旋锁在高并发写入场景下吞吐量高 10-20%          │
│                                                                      │
│  自适应锁（AdaptiveLock）也是可选的                                    │
│    - 根据延迟动态切换自旋/阻塞                                         │
│    - 但实现复杂度更高，默认不使用                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.4 三位置模型：wrotePosition / committedPosition / flushedPosition

```
┌──────────────────────────────────────────────────────────────────────┐
│              三位置模型的工程考量                                       │
│                                                                      │
│  为什么需要三个位置？                                                   │
│                                                                      │
│  场景 1：同步刷盘 (SYNC_FLUSH)                                         │
│    wrotePosition ──── flush 立即执行 ──── flushedPosition              │
│    flushedPosition = wrotePosition                                   │
│    committedPosition 不使用                                            │
│    每条消息都 fsync，可靠性最高，性能最低                                │
│                                                                      │
│  场景 2：异步刷盘 (ASYNC_FLUSH) - 标准模式                              │
│    wrotePosition ──── 异步 ──── flushedPosition                        │
│    FlushRealTimeService 定期 flush                                     │
│    flushLeastPages = 4 (至少攒 4 页 = 16KB 才刷)                       │
│    性能高，但 crash 可能丢失未刷盘的消息                                 │
│                                                                      │
│  场景 3：异步刷盘 + 堆外内存 (ASYNC_FLUSH + TransientStorePool)        │
│    wrotePosition ──── commit ──── committedPosition ──── flush ──── flushedPosition│
│    消息先写到堆外 ByteBuffer                                           │
│    commit 线程定期从堆外拷贝到 mmap buffer                             │
│    flush 线程定期从 mmap buffer fsync 到磁盘                           │
│    三层流水线，最高性能                                                │
│    但 crash 可能丢失更多数据                                           │
│                                                                      │
│  崩溃恢复时的使用：                                                     │
│    StoreCheckpoint.flushPhyOffset:                                     │
│      - 记录了已刷盘的位置                                              │
│      - 恢复时从此位置开始扫描                                          │
│      - 确保不会重复处理已刷盘的消息                                    │
│                                                                      │
│  与 Raft 的持久化对比：                                                 │
│    Raft: log entry 写入 → fsync → 告知 Follower                       │
│    RocketMQ: CommitLog 写入 → flush → 通知 Producer                   │
│    都是"写入 → 持久化 → 确认"模式                                     │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.5 线程模型设计：处理器-线程池映射

```
┌──────────────────────────────────────────────────────────────────────┐
│              处理器-线程池映射的工程考量                                  │
│                                                                      │
│  设计原则：线程隔离 (Thread Isolation)                                 │
│    - 每个请求码绑定一个独立线程池                                        │
│    - 不同类型的请求互不阻塞                                            │
│    - 类似于 Dubbo 的线程池隔离设计                                      │
│                                                                      │
│  具体实现：                                                            │
│    processorTable:                                                    │
│      SEND_MESSAGE → (SendMessageProcessor, sendMessageExecutor)       │
│      PULL_MESSAGE → (PullMessageProcessor, pullMessageExecutor)      │
│      POP_MESSAGE  → (PopMessageProcessor, popMessageExecutor)         │
│      ACK_MESSAGE  → (AckMessageProcessor, ackMessageExecutor)        │
│      QUERY_MESSAGE → (QueryMessageProcessor, queryMessageExecutor)   │
│      HEART_BEAT    → (ClientManageProcessor, heartbeatExecutor)     │
│      END_TRANSACTION → (EndTransactionProcessor, ...)               │
│      default → (AdminBrokerProcessor, adminBrokerExecutor)           │
│                                                                      │
│  Netty 的两层线程模型：                                                 │
│    第一层：EventLoopGroup（I/O 线程）                                  │
│      - 负责连接接收、读写、分发                                        │
│      - 不执行业务逻辑                                                  │
│    第二层：业务线程池                                                  │
│      - 执行处理器逻辑                                                 │
│      - 可能阻塞（如磁盘 I/O）                                         │
│      - 独立于 I/O 线程                                                 │
│                                                                      │
│  优势：                                                               │
│    1. 避免慢请求阻塞快请求                                             │
│       - 假设 QUERY_MESSAGE 很慢（磁盘 I/O）                            │
│       - SEND_MESSAGE 很快（内存写入）                                  │
│       - 如果共享线程池，慢请求会占满所有线程                              │
│       - 独立线程池后互不影响                                           │
│    2. 独立限流                                                        │
│       - 每个线程池可以独立配置大小                                      │
│       - 消息发送可以配置更多线程                                       │
│       - 管理操作可以配置更少线程                                       │
│    3. 监控和隔离                                                      │
│       - 每个线程池独立监控                                             │
│       - 异常不会扩散到其他类型请求                                     │
│                                                                      │
│  代价：                                                               │
│    - 更多线程 = 更多内存开销                                          │
│    - 线程间切换开销                                                    │
│    - 配置复杂度增加                                                   │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.6 长轮询设计：Push 的本质是 Pull

```
┌──────────────────────────────────────────────────────────────────────┐
│              长轮询设计的工程考量                                       │
│                                                                      │
│  为什么不用真正的 Push？                                               │
│    1. Broker 需要维护每个 Consumer 的推送队列                           │
│       - 消费速度不同的 Consumer 需要不同节奏                            │
│       - 内存管理复杂                                                  │
│    2. Consumer 需要控制消费速率                                        │
│       - 业务处理慢时需要反压                                           │
│       - Push 模式下反压困难                                            │
│    3. 网络效率                                                        │
│       - Push 需要为每个 Consumer 维持推送连接                            │
│       - 连接数随 Consumer 数增长                                       │
│                                                                      │
│  为什么不用纯 Pull？                                                   │
│    1. 空轮询浪费资源                                                   │
│       - Consumer 不断发送 Pull 请求                                    │
│       - 没有消息时 Broker 返回空                                       │
│       - 大量无效请求                                                  │
│    2. 延迟高                                                          │
│       - Consumer 不知道消息何时到达                                     │
│       - 需要平衡轮询间隔和延迟                                         │
│                                                                      │
│  长轮询的折中方案：                                                     │
│    Consumer 发送 Pull 请求                                            │
│    Broker 收到后：                                                    │
│      - 有消息 → 立即返回                                              │
│      - 无消息 → 挂起请求（Hold）                                       │
│    挂起期间：                                                          │
│      - 新消息到达 → PullRequestHoldService 立即唤醒                    │
│      - 超时（15 秒）→ 返回空                                          │
│    挂起请求不占用线程：                                                  │
│      - 存储在 ConcurrentMap 中                                        │
│      - 由 Netty EventLoop 管理 Channel                                 │
│                                                                      │
│  唤醒机制：                                                           │
│    1. ReputMessageService 发现新消息                                   │
│    2. 触发 NotifyMessageArrivingListener                              │
│    3. PullRequestHoldService.notifyMessageArriving(topic, qid, offset)│
│    4. 遍历挂起的请求                                                  │
│    5. 如果有匹配的 offset → 立即处理                                   │
│    6. 发送消息给 Consumer                                             │
│                                                                      │
│  性能：                                                               │
│    - 延迟 < 1ms（新消息到达后立即唤醒）                                │
│    - 无空轮询（挂起期间不消耗 CPU）                                    │
│    - Broker 内存可控（挂起请求只是一个 Map entry）                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.7 主从同步设计：与 Raft 的对比

```
┌──────────────────────────────────────────────────────────────────────┐
│              RocketMQ HA vs Raft 对比                                  │
│                                                                      │
│  ┌──────────────┬────────────────────┬──────────────────────────────┐  │
│  │ 特性         │ RocketMQ HA        │ Raft                        │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 角色         │ Master / Slave     │ Leader / Follower / Candidate│  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 选主         │ 外部指定 /         │ 选举（任期+投票）              │  │
│  │              │ Controller 模式    │                              │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 任期         │ 无                 │ term（单调递增）              │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 日志         │ CommitLog          │ Log entries {term, idx, cmd}│  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 日志格式     │ 消息二进制格式       │ {term, index, command}      │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 复制方式     │ 原生 NIO 推送      │ AppendEntries RPC           │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ ACK 格式     │ slaveAckOffset(8B) │ Follower 返回 matchIndex    │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ Commit 策略  │ 任一 Slave ACK     │ Majority ACK                │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ Commit 等待  │ GroupTransfer     │ nextIndex 推进               │  │
│  │              │ Service           │                              │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 日志匹配     │ slavePhyOffset    │ prevLogIndex + prevLogTerm   │  │
│  │              │ == masterPhyOffset│                              │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 故障检测     │ Channel 事件 +    │ 心跳超时 +                   │  │
│  │              │ 心跳超时           │ 随机化 election timeout      │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 故障恢复     │ Master 手动切换 / │ 自动选主                    │  │
│  │              │ Controller 自动   │                              │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 脑裂风险     │ 无（无选主）      │ 有（通过 term 解决）          │  │
│  ├──────────────┼────────────────────┼──────────────────────────────┤  │
│  │ 数据安全     │ 同步刷盘 +        │ Majority + fsync            │  │
│  │              │ Slave 同步        │                              │  │
│  └──────────────┴────────────────────┴──────────────────────────────┘  │
│                                                                      │
│  关键差异：                                                           │
│    1. RocketMQ HA 是"主从复制"，Raft 是"共识算法"                      │
│    2. RocketMQ 不需要选主（由外部指定），Raft 核心是选主               │
│    3. RocketMQ 的 commit 只需任一 Slave 确认，Raft 需要 majority      │
│    4. RocketMQ 无任期概念，Raft 通过 term 防止过期 Leader              │
│    5. RocketMQ 的 Controller 模式引入 Raft 来解决自动选主问题         │
│                                                                      │
│  RocketMQ Controller 模式：                                            │
│    - 基于 Raft 实现自动选主                                            │
│    - Controller 集群通过 Raft 保证高可用                               │
│    - Controller 决定哪个 Slave 成为新的 Master                         │
│    - Broker 本身的 HA 机制不变                                         │
│    - 相当于在 RocketMQ HA 之上加了一层 Raft 选主                       │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.8 信号量背压设计

```java
// 源码路径: remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java

// 异步调用信号量
protected final Semaphore semaphoreAsync;
// 单向调用信号量
protected final Semaphore semaphoreOneway;
```

```
┌──────────────────────────────────────────────────────────────────────┐
│              信号量背压的工程考量                                       │
│                                                                      │
│  问题：如果不限制在途请求数，会发生什么？                                │
│    - Producer 大量异步发送，响应还没回来就发下一个                      │
│    - 在途请求无限堆积，内存溢出                                        │
│    - 或者 Broker 响应慢，请求队列堆积                                  │
│                                                                      │
│  解决方案：信号量限制在途请求数                                         │
│    - 发送前 tryAcquire 信号量                                         │
│    - 响应到达后 release 信号量                                         │
│    - 超过限制的请求被拒绝（返回 SYSTEM_BUSY）                           │
│                                                                      │
│  配置：                                                               │
│    - semaphoreAsyncSize: 默认 65535                                    │
│    - semaphoreOnewaySize: 默认 65535                                   │
│                                                                      │
│  这是一种"租约"模式：                                                   │
│    - 每个请求持有一个许可                                              │
│    - 请求完成（成功/失败/超时）后归还                                   │
│    - 如果所有许可被占用，拒绝新请求                                     │
│                                                                      │
│  与 Raft 的流控对比：                                                   │
│    Raft:                                                              │
│      - Leader 通过 nextIndex 控制对每个 Follower 的复制速度            │
│      - AppendEntries 是同步的（等待响应后才发下一个）                   │
│      - 天然背压                                                      │
│    RocketMQ:                                                          │
│      - Producer 可以同时发送大量异步请求                                │
│      - 需要信号量显式控制                                              │
│      - 更灵活但更复杂                                                 │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.9 HashedWheelTimer：超时管理

```
┌──────────────────────────────────────────────────────────────────────┐
│              HashedWheelTimer 的工程考量                                │
│                                                                      │
│  问题：如何高效管理大量超时任务？                                       │
│                                                                      │
│  方案 A：PriorityQueue（堆）                                          │
│    - 每次插入 O(log N)                                               │
│    - 每次取出 O(log N)                                               │
│    - 适合少量超时任务                                                  │
│                                                                      │
│  方案 B：每个任务一个 ScheduledFuture                                 │
│    - 精度高                                                          │
│    - 但大量定时任务时线程开销大                                        │
│                                                                      │
│  方案 C：HashedWheelTimer（时间轮）                                    │
│    - 插入 O(1)                                                       │
│    - 取出 O(1)（平均）                                                │
│    - 适合大量短超时任务                                                │
│                                                                      │
│  RocketMQ 的选择：HashedWheelTimer                                    │
│    理由：                                                              │
│      1. 每个请求都需要注册超时（可能数万个在途请求）                     │
│      2. 超时时间通常在秒级（3-30 秒）                                 │
│      3. 时间轮的 tickDuration 为 100ms，精度足够                       │
│      4. O(1) 的插入和取消性能优秀                                     │
│                                                                      │
│  工作原理：                                                            │
│    时间轮 = 一个固定大小的数组 + 一个旋转的指针                        │
│    插入：hash(超时时间) % 槽数 → 放入对应槽位                            │
│    扫描：指针每 tickDuration 转一格，处理该格的所有任务                │
│    取消：从槽位链表中移除（O(1)）                                      │
│                                                                      │
│  对比 Raft 的超时管理：                                                │
│    Raft:                                                              │
│      - election timeout: 随机化 150-300ms                             │
│      - heartbeat interval: 固定 50ms                                 │
│      - 通常使用 ScheduledExecutorService                               │
│      - 因为 Raft 节点数量少（3-7 个），不需要高性能超时管理              │
│    RocketMQ:                                                          │
│      - 可能数万个在途请求，每个都有超时                                │
│      - 需要 O(1) 的超时管理                                          │
│      - 使用 HashedWheelTimer                                          │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.10 预分配与预热：性能优化

```
┌──────────────────────────────────────────────────────────────────────┐
│              预分配与预热的工程考量                                     │
│                                                                      │
│  问题：消息写入时创建 MappedFile 的代价                                │
│    - 创建文件：系统调用 open()                                         │
│    - mmap 映射：系统调用 mmap()                                       │
│    - mlock 锁定内存（可选）                                           │
│    - 预热：touch every page（触发 page fault）                         │
│    这些操作都是毫秒级的，在写入路径上会引入延迟                         │
│                                                                      │
│  RocketMQ 的解决方案：AllocateMappedFileService                        │
│    - 后台线程提前创建下一个 MappedFile                                 │
│    - 当前文件写满时，新文件已经准备好                                   │
│    - 写入路径无延迟                                                   │
│                                                                      │
│  预热 (WarmMappedFile)：                                              │
│    - 新创建的 MappedFile 的 mmap 区域不会立即分配物理页                │
│    - 第一次写入时触发 page fault，分配物理页                            │
│    - 预热：按页（4KB）写入 0，提前触发所有 page fault                  │
│    - 写入时不再有 page fault                                          │
│                                                                      │
│  TransientStorePool（堆外内存池）：                                     │
│    - 预分配一批 DirectByteBuffer                                       │
│    - 消息先写到堆外 ByteBuffer                                         │
│    - commit 线程从堆外拷贝到 mmap buffer                               │
│    - 好处：减少 GC 压力（不使用堆内存）                                │
│    - 好处：多一层流水线，更高吞吐量                                    │
│    - 代价：crash 可能丢失更多数据                                      │
│                                                                      │
│  与 Raft 的对比：                                                      │
│    Raft:                                                              │
│      - 日志通常用内存数据结构或简单文件                                 │
│      - 不需要 mmap 预分配                                             │
│      - 因为 Raft 的日志量通常远小于 RocketMQ                           │
│    RocketMQ:                                                          │
│      - 单 Broker 每天万亿消息                                         │
│      - 存储是性能瓶颈                                                 │
│      - 需要极致优化                                                   │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.11 消息二进制格式的设计考量

```
┌──────────────────────────────────────────────────────────────────────┐
│              消息二进制格式的工程考量                                   │
│                                                                      │
│  设计目标：                                                            │
│    1. 紧凑：减少磁盘占用                                               │
│    2. 可变长：支持不同大小的消息体和属性                                │
│    3. 自描述：从二进制可以解析出所有信息                                │
│    4. 高效解码：固定字段在前，变长字段在后                              │
│                                                                      │
│  格式分析：                                                            │
│    固定头部（约 60-80 字节）：                                         │
│      TOTALSIZE(4) + MAGICCODE(4) + BODYCRC(4) + QUEUEID(4) +         │
│      FLAG(4) + QUEUEOFFSET(8) + PHYSICALOFFSET(8) + SYSFLAG(4) +     │
│      BORNTIMESTAMP(8) + BORNHOST(8/20) + STORETIMESTAMP(8) +         │
│      STOREHOST(8/20) + RECONSUMETIMES(4) + PREPAREDTRANOFFSET(8)     │
│                                                                      │
│    变长部分：                                                          │
│      BODYLENGTH(4) + BODY(variable) +                                 │
│      TOPICLENGTH(1/2) + TOPIC(variable) +                            │
│      PROPERTIESLENGTH(2) + PROPERTIES(variable)                       │
│                                                                      │
│  关键设计点：                                                          │
│    1. TOTALSIZE 在最前面：解码时先读取长度                              │
│    2. MAGICCODE 用于识别有效消息和空白填充                              │
│    3. QUEUEOFFSET 和 PHYSICALOFFSET 用于定位                           │
│    4. BORNHOST 和 STOREHOST 支持 IPv4（8B）和 IPv6（20B）              │
│    5. TOPICLENGTH 用 1 字节（Topic < 127 字节）或 2 字节               │
│    6. PROPERTIES 用 2 字节长度（最大 65535 字节）                      │
│                                                                      │
│  与 Raft 日志格式对比：                                                │
│    Raft: {term(8), index(8), command(variable)}                      │
│      - 极简，只有 3 个字段                                            │
│      - term 用于一致性检查                                            │
│      - index 用于定位                                                 │
│    RocketMQ: 约 20+ 字段                                              │
│      - 丰富，包含完整的消息元数据                                     │
│      - 没有 term（无任期概念）                                         │
│      - QUEUEOFFSET 类似 Raft 的 index                                  │
│      - PHYSICALOFFSET 是 Raft 没有的（因为 Raft 日志按 index 定位）    │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.12 Controller 模式：Raft 在 RocketMQ 中的应用

RocketMQ 4.x 之前，Master-Slave 模式不支持自动故障转移——Master 挂了需要人工介入。RocketMQ 5.x 引入了 Controller 模式，基于 Raft 实现自动选主：

```
┌──────────────────────────────────────────────────────────────────────┐
│              Controller 模式架构                                        │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │                  Controller 集群                           │      │
│  │                                                           │      │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐           │      │
│  │  │Controller│◄──►│Controller│◄──►│Controller│           │      │
│  │  │ Leader  │    │Follower │    │Follower │           │      │
│  │  └────┬─────┘    └──────────┘    └──────────┘           │      │
│  │       │ Raft 共识                                        │      │
│  │       │ 维护: brokerEpoch, masterAddress                  │      │
│  └───────┼───────────────────────────────────────────────────┘      │
│          │                                                          │
│          │ 1. Broker 注册到 Controller                               │
│          │ 2. Controller 决定 Master/Slave                           │
│          │ 3. Master 挂了，Controller 自动选举新 Master              │
│          ▼                                                          │
│  ┌──────────┐          ┌──────────┐                                 │
│  │ Broker A │          │ Broker B │                                 │
│  │ (Master) │◄──HA────►│ (Slave)  │                                 │
│  │          │  复制    │          │                                 │
│  └──────────┘          └──────────┘                                 │
│                                                                      │
│  流程：                                                               │
│    1. Broker 启动时向 Controller 注册                                  │
│    2. Controller 通过 Raft 决定 Master                               │
│    3. Master Broker 开始提供服务                                      │
│    4. Slave Broker 连接 Master 进行 HA 同步                            │
│    5. Master 挂了 → Controller 检测到 → 选出新 Master                 │
│    6. 新 Master 通过 BrokerPreOnlineService 确保数据同步               │
│    7. 新 Master 开始提供服务                                          │
│                                                                      │
│  brokerEpoch 的作用：                                                  │
│    - 类似 Raft 的 term                                               │
│    - 每次选主递增                                                     │
│    - 防止旧 Master 复活导致双主                                       │
│    - Slave 只接受 >= 当前 brokerEpoch 的 Master 的数据                 │
│                                                                      │
│  这体现了 RocketMQ 对 Raft 的借鉴：                                    │
│    - 共识层（Controller）使用 Raft                                    │
│    - 数据层（Broker HA）保持原有机制                                   │
│    - 通过 brokerEpoch 实现类似 term 的安全保证                         │
│    - 分层设计：共识和复制分离                                         │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.13 总结：RocketMQ 架构的全局视角

```
┌──────────────────────────────────────────────────────────────────────┐
│                    RocketMQ 架构全局视角                                │
│                                                                      │
│  层次结构：                                                            │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │ 客户端层 (Client Layer)                                      │     │
│  │  - Producer: 发送消息，故障隔离，重试                         │     │
│  │  - Consumer: 拉取消息，并发/顺序消费，流控                    │     │
│  │  - MQClientInstance: 共享工厂                                │     │
│  └──────────────────────────┬──────────────────────────────────┘     │
│                              │ Remoting (Netty RPC)                    │
│  ┌──────────────────────────▼──────────────────────────────────┐     │
│  │ 路由层 (Routing Layer)                                       │     │
│  │  - NameServer: 无状态路由服务，AP 模式                       │     │
│  │  - Broker 心跳注册，客户端定时拉取路由                         │     │
│  └──────────────────────────┬──────────────────────────────────┘     │
│                              │ Remoting (Netty RPC)                    │
│  ┌──────────────────────────▼──────────────────────────────────┐     │
│  │ 消息处理层 (Broker Layer)                                    │     │
│  │  - BrokerController: 中央编排器                               │     │
│  │  - Processors: 消息发送/拉取/查询/事务处理器                   │     │
│  │  - ConfigManagers: Topic/Offset/Subscription 管理             │     │
│  │  - 长轮询服务                                                │     │
│  │  - 线程隔离                                                  │     │
│  └──────────────────────────┬──────────────────────────────────┘     │
│                              │                                         │
│  ┌──────────────────────────▼──────────────────────────────────┐     │
│  │ 存储层 (Storage Layer)                                       │     │
│  │  - CommitLog: 全消息顺序日志 (1GB/文件)                      │     │
│  │  - ConsumeQueue: 20B/条索引                                   │     │
│  │  - IndexFile: 哈希索引 (按 key 查找)                         │     │
│  │  - MappedFile: mmap 封装，三位置模型                          │     │
│  │  - ReputMessageService: CommitLog → ConsumeQueue 分发        │     │
│  │  - FlushManager: 同步/异步刷盘                                │     │
│  │  - StoreCheckpoint: 崩溃恢复                                  │     │
│  └──────────────────────────┬──────────────────────────────────┘     │
│                              │ 原生 NIO                                │
│  ┌──────────────────────────▼──────────────────────────────────┐     │
│  │ 高可用层 (HA Layer)                                          │     │
│  │  - DefaultHAService: 主从同步                                │     │
│  │  - WriteSocketService: Master 推送 CommitLog 到 Slave        │     │
│  │  - ReadSocketService: 读取 Slave ACK                         │     │
│  │  - DefaultHAClient: Slave 接收数据并追加本地 CommitLog       │     │
│  │  - GroupTransferService: 等待 Slave ACK (类似 Raft commit)   │     │
│  └──────────────────────────┬──────────────────────────────────┘     │
│                              │ Raft (可选)                             │
│  ┌──────────────────────────▼──────────────────────────────────┐     │
│  │ 控制层 (Controller Layer, 可选)                               │     │
│  │  - Controller 集群: 基于 Raft 的自动选主                     │     │
│  │  - brokerEpoch: 类似 Raft term                              │     │
│  │  - BrokerPreOnlineService: 上线前 HA 握手                    │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  设计哲学：                                                           │
│    1. 分层解耦：每层职责清晰，通过接口通信                              │
│    2. 读写分离：CommitLog 写 + ConsumeQueue 读                         │
│    3. 异步流水线：写入 → 分发 → 刷盘 → HA 同步                         │
│    4. 线程隔离：不同类型请求使用不同线程池                              │
│    5. 背压控制：信号量限制在途请求数                                    │
│    6. 故障隔离：MQFaultStrategy 延迟反馈                                │
│    7. 最终一致：NameServer AP 模式                                     │
│    8. 顺序优先：所有写入路径都是顺序 I/O                               │
│    9. 可选一致性：同步刷盘 + 同步复制                                   │
│    10. 简化运维：无外部依赖（无 ZooKeeper）                             │
└──────────────────────────────────────────────────────────────────────┘
```

### 11.14 从 Raft 视角看 RocketMQ 的启示

作为 Raft 研究者，阅读 RocketMQ 源码可以获得以下启示：

```
1. 共识 vs 复制
   - Raft 将共识（选主 + 复制）合为一体
   - RocketMQ 将共识（Controller）和复制（HA）分离
   - 分离的好处：存储层不需要实现复杂的共识算法
   - 分离的代价：需要额外维护 Controller 集群
   - 启示：分层设计是工程实践的常见模式

2. Commit 等待
   - Raft: Leader 等待 majority Follower 的 AppendEntries 响应
   - RocketMQ: Master 等待 Slave 的 ACK
   - 都有超时机制
   - 都有"写入 → 等待确认 → 通知完成"的三阶段
   - 启示：commit 等待是分布式存储的通用模式

3. 日志匹配
   - Raft: prevLogIndex + prevLogTerm 双重检查
   - RocketMQ: slavePhyOffset == masterPhyOffset 偏移量检查
   - Raft 更严格（需要 term 匹配），RocketMQ 更简单
   - 启示：简单的偏移量检查在某些场景下足够

4. 任期的价值
   - Raft 的 term 防止过期 Leader 损坏数据
   - RocketMQ 没有 term，可能出现旧 Master 复活
   - Controller 模式通过 brokerEpoch 弥补
   - 启示：任期是分布式安全的重要机制

5. 性能 vs 可靠性
   - Raft 通常先保证正确性，再优化性能
   - RocketMQ 从设计之初就追求极致性能
   - 自旋锁、mmap、预分配、零拷贝...
   - 启示：在工程中，性能和可靠性需要平衡

6. 状态机应用
   - Raft: 日志条目应用到状态机
   - RocketMQ: 消息被 Consumer 消费（应用到业务状态机）
   - 都有 appliedIndex 的概念
   - RocketMQ 的 ConsumerOffset 类似 Raft 的 lastApplied

7. 快照与清理
   - Raft: 日志压缩 + 快照
   - RocketMQ: CleanCommitLogService 清理过期文件
   - 都需要处理"历史数据清理"问题
   - RocketMQ 没有"快照"概念（因为消息不需要恢复状态）

8. 成员变更
   - Raft: Joint Consensus / 单节点变更
   - RocketMQ: 动态增减 Broker（注册/注销）
   - RocketMQ 更简单（因为无共识约束）
   - 启示：无共识的成员管理更灵活但安全性低
```

---

## 附录：关键源码文件索引

以下列出本文涉及的所有关键源码文件，方便对照阅读：

### NameServer 模块

| 文件 | 作用 |
|------|------|
| `namesrv/.../NamesrvStartup.java` | 启动入口 |
| `namesrv/.../NamesrvController.java` | 核心控制器 |
| `namesrv/.../routeinfo/RouteInfoManager.java` | 路由管理器（5 大数据结构） |
| `namesrv/.../processor/DefaultRequestProcessor.java` | 默认请求处理器 |
| `namesrv/.../processor/ClientRequestProcessor.java` | 客户端路由查询处理器 |
| `namesrv/.../BrokerHousekeepingService.java` | Channel 事件监听 |
| `namesrv/.../kvconfig/KVConfigManager.java` | KV 配置管理 |
| `namesrv/.../routeinfo/BatchUnregistrationService.java` | 批量注销服务 |

### Broker 模块

| 文件 | 作用 |
|------|------|
| `broker/.../BrokerStartup.java` | 启动入口 |
| `broker/.../BrokerController.java` | 中央编排器（2823+ 行） |
| `broker/.../processor/SendMessageProcessor.java` | 消息发送处理器 |
| `broker/.../processor/PullMessageProcessor.java` | 消息拉取处理器 |
| `broker/.../processor/PopMessageProcessor.java` | Pop 消费处理器 |
| `broker/.../processor/AckMessageProcessor.java` | 消息确认处理器 |
| `broker/.../processor/QueryMessageProcessor.java` | 消息查询处理器 |
| `broker/.../processor/ClientManageProcessor.java` | 客户端管理处理器 |
| `broker/.../processor/EndTransactionProcessor.java` | 事务结束处理器 |
| `broker/.../longpolling/PullRequestHoldService.java` | 长轮询挂起服务 |
| `broker/.../topic/TopicConfigManager.java` | Topic 配置管理 |
| `broker/.../offset/ConsumerOffsetManager.java` | 消费进度管理 |
| `broker/.../subscription/SubscriptionGroupManager.java` | 订阅组管理 |
| `broker/.../broker/BrokerPreOnlineService.java` | 上线前 HA 握手 |
| `broker/.../outapi/BrokerOuterAPI.java` | Broker 对外 API |

### Store 模块

| 文件 | 作用 |
|------|------|
| `store/.../DefaultMessageStore.java` | 存储引擎主实现 |
| `store/.../CommitLog.java` | CommitLog（全消息顺序日志） |
| `store/.../consumequeue/ConsumeQueue.java` | ConsumeQueue（消费队列索引） |
| `store/.../index/IndexFile.java` | IndexFile（哈希索引） |
| `store/.../index/IndexService.java` | 索引服务 |
| `store/.../MappedFileQueue.java` | MappedFile 队列管理 |
| `store/.../DefaultMappedFile.java` | MappedFile（mmap 封装） |
| `store/.../DefaultAppendMessageCallback.java` | 消息追加回调 |
| `store/.../MessageExtEncoder.java` | 消息编码器 |
| `store/.../MessageExtDecoder.java` | 消息解码器 |
| `store/.../StoreCheckpoint.java` | 崩溃恢复检查点 |
| `store/.../AllocateMappedFileService.java` | 文件预分配服务 |
| `store/.../flush/FlushManager.java` | 刷盘管理器 |
| `store/.../ha/DefaultHAService.java` | HA 服务 |
| `store/.../ha/DefaultHAClient.java` | HA 客户端（Slave 端） |
| `store/.../ha/DefaultHAConnection.java` | HA 连接（Master 端） |

### Client 模块

| 文件 | 作用 |
|------|------|
| `client/.../producer/DefaultMQProducer.java` | Producer 入口 |
| `client/.../impl/producer/DefaultMQProducerImpl.java` | Producer 实现 |
| `client/.../consumer/DefaultMQPushConsumer.java` | Push Consumer 入口 |
| `client/.../impl/consumer/DefaultMQPushConsumerImpl.java` | Push Consumer 实现 |
| `client/.../impl/consumer/ConsumeMessageConcurrentlyService.java` | 并发消费服务 |
| `client/.../impl/consumer/ConsumeMessageOrderlyService.java` | 顺序消费服务 |
| `client/.../impl/consumer/RebalanceImpl.java` | 负载均衡实现 |
| `client/.../impl/consumer/ProcessQueue.java` | 本地消息缓存 |
| `client/.../impl/consumer/PullMessageService.java` | 拉取消息服务 |
| `client/.../impl/factory/MQClientInstance.java` | 客户端共享实例 |
| `client/.../latency/MQFaultStrategy.java` | 故障延迟策略 |
| `client/.../latency/LatencyFaultToleranceImpl.java` | 故障容错实现 |
| `client/.../consumer/store/RemoteBrokerOffsetStore.java` | 远程消费进度存储 |
| `client/.../consumer/store/LocalFileOffsetStore.java` | 本地消费进度存储 |
| `client/.../consumer/rebalance/AllocateMessageQueueAveragely.java` | 平均分配策略 |

### Remoting 模块

| 文件 | 作用 |
|------|------|
| `remoting/.../RemotingCommand.java` | 通信协议封装 |
| `remoting/.../netty/NettyRemotingAbstract.java` | 共享引擎（客户端/服务端基类） |
| `remoting/.../netty/NettyRemotingServer.java` | Netty 服务端 |
| `remoting/.../netty/NettyRemotingClient.java` | Netty 客户端 |
| `remoting/.../netty/NettyEncoder.java` | 编码器 |
| `remoting/.../netty/NettyDecoder.java` | 解码器 |
| `remoting/.../netty/ResponseFuture.java` | 响应 Future |
| `remoting/.../protocol/RemotingCommandType.java` | 请求/响应类型 |
| `remoting/.../protocol/SerializeType.java` | 序列化类型 |

---

## 附录：关键请求码索引

| 请求码 | 常量名 | 处理者 | 说明 |
|--------|--------|--------|------|
| 10 | SEND_MESSAGE | SendMessageProcessor | 消息发送 |
| 11 | PULL_MESSAGE | PullMessageProcessor | 消息拉取 |
| 12 | QUERY_MESSAGE | QueryMessageProcessor | 消息查询 |
| 14 | QUERY_CONSUMER_OFFSET | - | 查询消费进度 |
| 15 | UPDATE_CONSUMER_OFFSET | - | 更新消费进度 |
| 25 | CHECK_CLIENT_CONFIG | ClientManageProcessor | 客户端配置检查 |
| 34 | HEART_BEAT | ClientManageProcessor | 客户端心跳 |
| 35 | UNREGISTER_CLIENT | ClientManageProcessor | 客户端注销 |
| 37 | END_TRANSACTION | EndTransactionProcessor | 事务结束 |
| 39 | CHECK_TRANSACTION_STATE | - | 事务状态回查 |
| 103 | REGISTER_BROKER | DefaultRequestProcessor (NS) | Broker 注册 |
| 104 | UNREGISTER_BROKER | DefaultRequestProcessor (NS) | Broker 注销 |
| 105 | GET_ROUTEINFO_BY_TOPIC | ClientRequestProcessor (NS) | 查询路由 |
| 310 | SEND_MESSAGE_V2 | SendMessageProcessor | 消息发送 V2 |
| 313 | LOCK_BATCH_MQ | - | 批量锁定队列 |
| 314 | UNLOCK_BATCH_MQ | - | 批量解锁队列 |
| 316 | VIEW_MESSAGE_BY_ID | QueryMessageProcessor | 按 ID 查看消息 |
| 320 | SEND_BATCH_MESSAGE | SendMessageProcessor | 批量消息发送 |
| 324 | SEND_REPLY_MESSAGE | ReplyMessageProcessor | 回复消息 |
| 403 | CHANGE_MESSAGE_INVISIBLETIME | ChangeInvisibleTimeProcessor | 修改不可见时间 |
| 904 | BROKER_HEARTBEAT | DefaultRequestProcessor (NS) | Broker 心跳 |
| 200050 | POP_MESSAGE | PopMessageProcessor | Pop 消费 |
| 200051 | ACK_MESSAGE | AckMessageProcessor | 消息确认 |

---

## 附录：关键配置项索引

### Broker 核心配置 (BrokerConfig)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| brokerName | (配置) | Broker 名称 |
| brokerClusterName | DefaultCluster | 集群名称 |
| brokerId | 0 | 0=Master, 非0=Slave |
| brokerPermission | 6 | 权限（读+写） |
| autoCreateTopicEnable | true | 自动创建 Topic |
| enableControllerMode | false | 启用 Controller 模式 |
| enableSlaveActingMaster | false | Slave 代理 Master |
| registerNameServerPeriod | 30000 | 注册间隔（ms） |
| forceRegister | false | 强制注册 |

### 存储配置 (MessageStoreConfig)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| storePathRootDir | ~/store | 存储根目录 |
| storePathCommitLog | ~/store/commitlog | CommitLog 路径 |
| mappedFileSizeCommitLog | 1073741824 (1GB) | CommitLog 文件大小 |
| mappedFileSizeConsumeQueue | 6000000 | ConsumeQueue 文件大小 |
| mappedFileSizeConsumeQueueExt | 41943040 | ConsumeQueue 扩展大小 |
| mappedFileSizeIndexFile | 401200040 | IndexFile 大小 |
| maxMessageSize | 4194304 (4MB) | 单条消息最大大小 |
| flushDiskType | ASYNC_FLUSH | 刷盘类型 |
| brokerRole | ASYNC_MASTER | Broker 角色 |
| haListenPort | 10912 | HA 端口 |
| haSlaveAddress | (配置) | Slave 地址 |
| transientStorePoolEnable | false | 启用堆外内存池 |
| warmMappedFileEnable | false | 预热 MappedFile |
| useReentrantLockWhenPutMessage | false | 使用可重入锁 |
| enableDLedgerCommitLog | false | 启用 DLedger (Raft) 模式 |

### Netty 配置 (NettyServerConfig/NettyClientConfig)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| listenPort | 10911 | Broker 监听端口 |
| serverSocketBacklog | 1024 | TCP backlog |
| serverChannelMaxIdleTimeSeconds | 120 | 空闲超时 |
| clientWorkerThreads | 4 | 客户端工作线程数 |
| connectTimeoutMillis | 3000 | 连接超时 |
| clientChannelMaxIdleTimeSeconds | 120 | 客户端空闲超时 |
| clientOnewaySemaphoreValue | 65535 | 单向信号量 |
| clientAsyncSemaphoreValue | 65535 | 异步信号量 |

### 客户端配置 (ClientConfig)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| namesrvAddr | (配置) | NameServer 地址 |
| clientIP | (自动) | 客户端 IP |
| instanceName | DEFAULT | 实例名称 |
| clientCallbackExecutorThreads | CPU核心数 | 回调线程数 |
| mqClientApiTimeout | 3000 | API 超时 |

### Producer 配置 (DefaultMQProducer)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| producerGroup | (配置) | Producer 组名 |
| sendMsgTimeout | 3000 | 发送超时（ms） |
| retryTimesWhenSendFailed | 2 | 同步重试次数 |
| retryTimesWhenSendAsyncFailed | 2 | 异步重试次数 |
| maxMessageSize | 4194304 | 最大消息大小 |
| compressMsgBodyOverHowmuch | 4096 | 压缩阈值 |
| retryAnotherBrokerWhenNotStoreOK | false | 重试其他 Broker |

### Consumer 配置 (DefaultMQPushConsumer)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| consumerGroup | (配置) | Consumer 组名 |
| messageModel | CLUSTERING | 消费模式 |
| consumeFromWhere | CONSUME_FROM_LAST_OFFSET | 起始位置 |
| consumeThreadMin | 20 | 最小消费线程 |
| consumeThreadMax | 20 | 最大消费线程 |
| pullBatchSize | 32 | 拉取批量大小 |
| consumeMessageBatchMaxSize | 1 | 消费批量大小 |
| pullInterval | 0 | 拉取间隔 |
| pullThresholdForQueue | 1000 | 队列流控阈值 |

---

> 本文档基于 RocketMQ 源码分析编写，涵盖了架构设计哲学、各模块深度解析、核心数据结构、消息全链路追踪和关键设计决策。
> 所有源码路径、方法签名均基于实际代码，便于读者对照源码阅读。
> 
> **文档总计约 7000+ 行，覆盖 RocketMQ 全部核心模块。**

---

# 第二部分：十二大真实业务场景全链路源码解析

> 以下内容从真实生产场景出发，逐一拆解用户最常使用的十二种消息模式。每个场景包含：业务背景、用户代码示例、底层源码逐行跟踪、核心组件协作时序、设计知识点。不跳步、不省略，从用户 API 调用一路追到磁盘写入和网络发送。

---

## 场景一：普通消息发送与消费 —— 电商下单通知

### 1.1 业务背景

电商平台用户下单后，系统需要发送一条消息通知库存服务扣减库存、通知物流服务生成运单、通知短信服务发送下单成功通知。这是 RocketMQ 最基础也是最常用的场景：一条消息被多个消费者组独立消费。

### 1.2 用户代码

**生产者**：

```java
DefaultMQProducer producer = new DefaultMQProducer("order_producer_group");
producer.setNamesrvAddr("127.0.0.1:9876");
producer.start();

Message msg = new Message("OrderTopic", "create_order",
    ("订单ID: " + orderId + ", 金额: " + amount).getBytes(StandardCharsets.UTF_8));
SendResult sendResult = producer.send(msg);
System.out.println("发送结果: " + sendResult.getSendStatus());
```

**消费者（库存服务）**：

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("inventory_consumer_group");
consumer.setNamesrvAddr("127.0.0.1:9876");
consumer.subscribe("OrderTopic", "create_order");
consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
    for (MessageExt msg : msgs) {
        System.out.println("库存服务收到消息: " + new String(msg.getBody()));
        // 执行扣减库存逻辑...
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
consumer.start();
```

### 1.3 源码全链路追踪

#### 1.3.1 Producer 端发送链路

**入口：`DefaultMQProducer.send(Message msg)`**

```java
// DefaultMQProducer.java
public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    msg.setTopic(withNamespace(msg.getTopic()));
    return this.defaultMQProducerImpl.send(msg);
}
```

这里 `withNamespace` 处理命名空间（多租户隔离），然后委托给 `DefaultMQProducerImpl`。

**`DefaultMQProducerImpl.send(Message msg)`**：

```java
public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
    return send(msg, this.defaultMQProducer.getSendMsgTimeout());
}

public SendResult send(Message msg, long timeout) throws ... {
    return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
}
```

通信模式为 `SYNC`（同步发送），超时时间默认 3000ms。

**`sendDefaultImpl` 核心重试循环**：

```java
private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode,
    final SendCallback sendCallback, final long timeout) throws ... {
    
    // 步骤1：检查Producer是否已启动
    this.makeSureStateOK();
    
    // 步骤2：校验消息合法性
    Validators.checkMessage(msg, this.defaultMQProducer);
    
    // 步骤3：获取Topic发布信息（路由信息）
    TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
    if (topicPublishInfo == null || !topicPublishInfo.ok()) {
        throw new MQClientException("No route info of this topic: " + msg.getTopic());
    }
    
    // 步骤4：计算总重试次数（同步模式下默认 1 + retryTimesWhenSendFailed = 3）
    int timesTotal = communicationMode == CommunicationMode.SYNC
        ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
    
    // 步骤5：重试循环
    int times = 0;
    String[] brokersSent = new String[timesTotal];
    MessageQueue mqSelected = null;
    for (; times < timesTotal; times++) {
        String lastBrokerName = null == mqSelected ? null : mqSelected.getBrokerName();
        boolean resetIndex = times > 0;
        
        // 5a. 选择目标MessageQueue（含故障隔离逻辑）
        mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName, resetIndex);
        if (mqSelected != null) {
            brokersSent[times] = mqSelected.getBrokerName();
            try {
                long beginTimestampPrev = System.currentTimeMillis();
                
                // 5b. 执行实际发送
                SendResult sendResult = this.sendKernelImpl(msg, mqSelected,
                    communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                
                // 5c. 更新故障隔离项（记录延迟）
                endTimestamp = System.currentTimeMillis();
                this.updateFaultItem(mqSelected.getBrokerName(),
                    endTimestamp - beginTimestampPrev, false, true);
                
                // 5d. 同步模式直接返回
                return sendResult;
                
            } catch (MQBrokerException e) {
                // 5e. Broker返回错误，部分错误码可重试
                this.updateFaultItem(mqSelected.getBrokerName(),
                    endTimestamp - beginTimestampPrev, true, false);
                if (this.defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
                    continue;  // 重试
                } else {
                    throw e;  // 不可重试错误，直接抛出
                }
            } catch (RemotingException e) {
                // 5f. 网络异常，隔离Broker并重试
                this.updateFaultItem(mqSelected.getBrokerName(),
                    endTimestamp - beginTimestampPrev, true,
                    !this.mqFaultStrategy.isStartDetectorEnable());
                continue;
            }
        } else {
            break;
        }
    }
    throw new MQClientException("Send message times exceeded");
}
```

**步骤3详解：`tryToFindTopicPublishInfo`**

```java
private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
    TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
    if (null == topicPublishInfo || !topicPublishInfo.ok()) {
        // 本地没有缓存，向NameServer查询
        this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
        topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (topicPublishInfo != null && !topicPublishInfo.ok()) {
            // 发送路由查询请求
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
    }
    
    if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
        if (topicPublishInfo.isHaveTopicRouterInfo() && !topicPublishInfo.ok()) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
        }
        return topicPublishInfo;
    }
    // 如果Topic不存在，使用默认Topic路由
    this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic,
        this.defaultMQProducer, isMixMode);
    topicPublishInfo = this.topicPublishInfoTable.get(topic);
    return topicPublishInfo;
}
```

NameServer 返回的 `TopicRouteData` 包含：
- `queueDatas`：每个 Broker 的队列信息（brokerName、readQueueNums、writeQueueNums、perm）
- `brokerDatas`：每个 Broker 的地址信息（cluster、brokerName、brokerAddrs={brokerId: address}）

Producer 将其转换为 `TopicPublishInfo`，内部维护一个 `List<MessageQueue>` 用于轮询选择。

**步骤5b详解：`sendKernelImpl`**

```java
private SendResult sendKernelImpl(final Message msg, final MessageQueue mq,
    final CommunicationMode communicationMode, final SendCallback sendCallback,
    final TopicPublishInfo topicPublishInfo, final long timeout) throws ... {
    
    long beginStartTime = System.currentTimeMillis();
    // 1. 查找Broker地址
    String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
    if (null == brokerAddr) {
        // 本地缓存未命中，从NameServer重新获取
        tryToFindTopicPublishInfo(mq.getTopic());
        brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
    }
    
    // 2. 如果消息体为空，设置空消息体
    byte[] prevBody = msg.getBody();
    try {
        // 3. 设置消息唯一ID（用于去重和追踪）
        if (!(msg instanceof MessageBatch)) {
            MessageClientIDSetter.setUniqID(msg);
        }
        
        // 4. 计算消息系统标志位
        int sysFlag = 0;
        if (this.tryToCompressMessage(msg)) {
            sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
        }
        final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (Boolean.parseBoolean(tranMsg)) {
            sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        }
        
        // 5. 如果是VIP通道，端口号-2
        if (this.mQClientFactory.getClientConfig().isVipChannelEnabled()) {
            brokerAddr = mixVIPChannel(brokerAddr);
        }
        
        // 6. 构建SendMessageRequestHeader
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTopic(msg.getTopic());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(msg.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
        requestHeader.setReconsumeTimes(0);
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setBornHost(RemotingUtil.socketAddress2String(this.mQClientFactory.getClientConfig().getClientSocketAddress()));
        
        // 7. 根据通信模式选择发送方式
        switch (communicationMode) {
            case SYNC:
                SendResult sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                    brokerAddr, mq.getBrokerName(), msg, requestHeader,
                    timeout - costTime, communicationMode, null, null);
                return sendResult;
            case ASYNC:
                // ... 异步发送 ...
                break;
            case ONEWAY:
                // ... 单向发送 ...
                break;
        }
    } finally {
        msg.setBody(prevBody);
    }
}
```

**知识点：消息压缩**

`tryToCompressMessage` 在消息体超过 4KB 时触发压缩（默认使用 ZIP）。压缩后的消息在 `sysFlag` 中设置 `COMPRESSED_FLAG`，Broker 和 Consumer 在处理时会先解压。

#### 1.3.2 Broker 端处理链路

**`MQClientAPIImpl.sendMessage`** → `RemotingCommand` 创建 → `NettyRemotingClient.invokeSync` → 网络传输 → Broker 的 `NettyRemotingServer` 接收 → `SendMessageProcessor.processRequest`

**`SendMessageProcessor.processRequest`** 核心流程：

```java
// SendMessageProcessor.java
public CompletableFuture<RemotingCommand> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
    SendMessageContext mqtraceContext = null;
    try {
        // 1. 解析请求头
        final SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        
        // 2. 消息追踪上下文初始化
        mqtraceContext = buildMsgContext(ctx, requestHeader);
        this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
    } catch (RemotingCommandException e) { ... }
    
    // 3. 消息校验（Topic名合法性、消息体大小等）
    RemotingCommand response = this.msgCheck(ctx, request, requestHeader, mqtraceContext);
    if (response != null) {
        return CompletableFuture.completedFuture(response);
    }
    
    // 4. 构建内部消息对象
    final MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
    msgInner.setTopic(requestHeader.getTopic());
    msgInner.setQueueId(requestHeader.getQueueId());
    // ... 设置所有字段 ...
    
    // 5. 处理重试消息和死信队列
    if (msgInner.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
        String groupName = KeyBuilder.parseGroup(msgInner.getTopic());
        // ... 处理重试次数判断 ...
    }
    
    // 6. 写入CommitLog（核心存储操作）
    CompletableFuture<PutMessageResult> putResultFuture = null;
    if (msgInner.getDelayTimeLevel() > 0) {
        // 延时消息特殊处理
        // ...
    } else {
        putResultFuture = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
    }
    
    // 7. 处理写入结果
    return putResultFuture.thenApply(putResult -> {
        RemotingCommand resp = handlePutMessageResult(putResult, response, request, msgInner, 
            responseHeader, mqtraceContext, ctx, queueIdInt);
        return resp;
    });
}
```

**步骤6：CommitLog.asyncPutMessage —— 消息存储的核心路径**

这是 RocketMQ 最核心的存储操作。当 `SendMessageProcessor` 调用 `messageStore.asyncPutMessage(msgInner)` 后，执行进入 `CommitLog.asyncPutMessage`。整个写入过程分为两个阶段：**预编码（encode）** 和 **追加写入（doAppend）**，中间穿插锁、偏移量分配、HA 同步等关键逻辑。

**第一阶段：asyncPutMessage 主流程**

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/CommitLog.java
// 行 969-1140
public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
    // 1. 设置存储时间戳（非副本模式）
    if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
        msg.setStoreTimestamp(System.currentTimeMillis());
    }
    // 2. 计算消息体CRC32
    msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
    
    // 3. 根据Topic长度选择消息版本
    msg.setVersion(MessageVersion.MESSAGE_VERSION_V1);
    boolean autoMessageVersionOnTopicLen =
        this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
    if (autoMessageVersionOnTopicLen && topic.length() > Byte.MAX_VALUE) {
        msg.setVersion(MessageVersion.MESSAGE_VERSION_V2);  // V2: topic长度用2字节
    }
    
    // 4. 检测IPv6地址，设置SYS_FLAG中的V6标志位
    InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
    if (bornSocketAddress.getAddress() instanceof Inet6Address) {
        msg.setBornHostV6Flag();   // BORNHOST_V6_FLAG → 20字节
    }
    InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
    if (storeSocketAddress.getAddress() instanceof Inet6Address) {
        msg.setStoreHostV6Flag();  // STOREHOSTADDRESS_V6_FLAG → 20字节
    }
    
    // 5. 获取当前可写的 MappedFile
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
    long currOffset = (mappedFile == null) ? 0 
        : mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
    
    // 6. HA同步前置检查（Controller模式 / SlaveActingMaster模式）
    if (needHandleHA && brokerConfig.isEnableControllerMode()) {
        if (haService.inSyncReplicasNums(currOffset) < minInSyncReplicas) {
            return CompletableFuture.completedFuture(
                new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
        }
    }
    
    // 7. TopicQueueLock — 保证同一 Topic+QueueId 的偏移量分配串行化
    topicQueueLock.lock(topicQueueKey);
    try {
        // 8. 分配 QueueOffset（关键！在编码之前分配）
        if (needAssignOffset) {
            defaultMessageStore.assignOffset(msg);
        }
        
        // 9. ★ 预编码：将消息所有字段写入 thread-local ByteBuf
        PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
        if (encodeResult != null) {
            return CompletableFuture.completedFuture(encodeResult);  // 编码失败
        }
        msg.setEncodedBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());
        
        // 10. ★ putMessageLock — 串行写入 CommitLog（自旋锁或ReentrantLock）
        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;
            
            // 在锁内重新设置存储时间戳，保证全局有序
            if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                msg.setStoreTimestamp(beginLockTimestamp);
            }
            
            // 11. 如果当前MappedFile为空或已满，创建新文件
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            }
            
            // 12. ★ 追加写入：调用 MappedFile.appendMessage
            //     内部委托给 DefaultAppendMessageCallback.doAppend
            result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
            
            switch (result.getStatus()) {
                case PUT_OK:
                    onCommitLogAppend(msg, result, mappedFile);
                    break;
                case END_OF_FILE:
                    // 当前文件已满，写入了空白标记
                    onCommitLogAppend(msg, result, mappedFile);
                    // 创建新文件，重新写入消息
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                    if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                        onCommitLogAppend(msg, result, mappedFile);
                    }
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    return CompletableFuture.completedFuture(
                        new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                default:
                    return CompletableFuture.completedFuture(
                        new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }
        } finally {
            beginTimeInLock = 0;
            putMessageLock.unlock();
        }
        
        // 13. 写入成功，递增 TopicQueue 偏移量
        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
            this.defaultMessageStore.increaseOffset(msg, getMessageNum(msg));
        }
    } finally {
        topicQueueLock.unlock(topicQueueKey);
    }
    
    // 14. 处理刷盘和HA同步（异步）
    return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);
}
```

**第二阶段：MessageExtEncoder.encode() —— 逐字段预编码**

在 `asyncPutMessage` 第9步，`MessageExtEncoder.encode()` 将消息的所有字段按固定顺序写入 thread-local `ByteBuf`。这是消息二进制格式的核心：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/MessageExtEncoder.java
// 行 175-280
public PutMessageResult encode(MessageExtBrokerInner msgInner) {
    this.byteBuf.clear();
    
    // 序列化properties为字节数组
    final byte[] propertiesData =
        msgInner.getPropertiesString() == null ? null 
            : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
    final int propertiesLength = (propertiesData == null ? 0 : propertiesData.length) 
        + (needAppendLastPropertySeparator ? 1 : 0) + crc32ReservedLength;
    
    final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
    final int topicLength = topicData.length;
    final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;
    
    // 计算消息总长度
    final int msgLen = calMsgLength(
        msgInner.getVersion(), msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);
    
    // 1 TOTALSIZE (4字节)
    this.byteBuf.writeInt(msgLen);
    // 2 MAGICCODE (4字节) — MESSAGE_MAGIC_CODE = -626843481
    this.byteBuf.writeInt(msgInner.getVersion().getMagicCode());
    // 3 BODYCRC (4字节)
    this.byteBuf.writeInt(msgInner.getBodyCRC());
    // 4 QUEUEID (4字节)
    this.byteBuf.writeInt(msgInner.getQueueId());
    // 5 FLAG (4字节)
    this.byteBuf.writeInt(msgInner.getFlag());
    // 6 QUEUEOFFSET (8字节) — 占位，doAppend时回填
    this.byteBuf.writeLong(queueOffset);
    // 7 PHYSICALOFFSET (8字节) — 占位，doAppend时回填
    this.byteBuf.writeLong(0);
    // 8 SYSFLAG (4字节) — 含事务类型、V6标志、多TAG标志等
    this.byteBuf.writeInt(msgInner.getSysFlag());
    // 9 BORNTIMESTAMP (8字节)
    this.byteBuf.writeLong(msgInner.getBornTimestamp());
    // 10 BORNHOST (8字节IPv4 / 20字节IPv6)
    ByteBuffer bornHostBytes = msgInner.getBornHostBytes();
    this.byteBuf.writeBytes(bornHostBytes.array());
    // 11 STORETIMESTAMP (8字节) — 占位，doAppend时回填
    this.byteBuf.writeLong(msgInner.getStoreTimestamp());
    // 12 STOREHOSTADDRESS (8字节IPv4 / 20字节IPv6)
    ByteBuffer storeHostBytes = msgInner.getStoreHostBytes();
    this.byteBuf.writeBytes(storeHostBytes.array());
    // 13 RECONSUMETIMES (4字节)
    this.byteBuf.writeInt(msgInner.getReconsumeTimes());
    // 14 PreparedTransactionOffset (8字节)
    this.byteBuf.writeLong(msgInner.getPreparedTransactionOffset());
    // 15 BODYLENGTH (4字节) + BODY (变长)
    this.byteBuf.writeInt(bodyLength);
    if (bodyLength > 0)
        this.byteBuf.writeBytes(msgInner.getBody());
    // 16 TOPICLENGTH (1字节V1 / 2字节V2) + TOPIC (变长)
    if (MessageVersion.MESSAGE_VERSION_V2.equals(msgInner.getVersion())) {
        this.byteBuf.writeShort((short) topicLength);
    } else {
        this.byteBuf.writeByte((byte) topicLength);
    }
    this.byteBuf.writeBytes(topicData);
    // 17 PROPERTIESLENGTH (2字节) + PROPERTIES (变长)
    this.byteBuf.writeShort((short) propertiesLength);
    if (propertiesLength > crc32ReservedLength) {
        this.byteBuf.writeBytes(propertiesData);
    }
    // 18 CRC32 (可选，crc32ReservedLength=17字节)
    this.byteBuf.writerIndex(this.byteBuf.writerIndex() + crc32ReservedLength);
    
    return null;  // null表示编码成功
}
```

**消息二进制格式总表（calMsgLength 计算依据）：**

```
偏移   字段名                    大小(字节)     说明
─────────────────────────────────────────────────────────────────────
0      TOTALSIZE                 4             消息总长度
4      MAGICCODE                 4             -626843481(V1) / -626843477(V2)
8      BODYCRC                   4             消息体CRC32校验
12     QUEUEID                   4             队列ID
16     FLAG                      4             消息标志
20     QUEUEOFFSET               8             队列偏移量（doAppend回填）
28     PHYSICALOFFSET            8             CommitLog物理偏移（doAppend回填）
36     SYSFLAG                   4             系统标志（事务/V6/多TAG）
40     BORNTIMESTAMP             8             产生时间戳
48     BORNHOST                  8或20          IPv4(4+4) / IPv6(16+4)
56/68  STORETIMESTAMP            8             存储时间戳（doAppend回填）
64/76  STOREHOSTADDRESS          8或20          IPv4(4+4) / IPv6(16+4)
72/96  RECONSUMETIMES            4             重试消费次数
76/100 PreparedTransactionOffset 8             事务prepared消息偏移
84/108 BODYLENGTH                4             消息体长度
88/112 BODY                      bodyLength    消息体内容
       TOPICLENGTH               1(V1)/2(V2)   Topic长度
       TOPIC                     topicLength   Topic字符串
       PROPERTIESLENGTH          2             属性长度
       PROPERTIES                propertiesLen 属性键值对
       CRC32                     0或17         CRC32校验（可选）
```

**calMsgLength 的精确计算逻辑：**

```java
// MessageExtEncoder.java 行 60-83
public static int calMsgLength(MessageVersion messageVersion,
    int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
    
    // BORNHOST 和 STOREHOSTADDRESS 的长度取决于 SYSFLAG 中的 V6 标志位
    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
    int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
    
    return 4  // TOTALSIZE
        + 4   // MAGICCODE
        + 4   // BODYCRC
        + 4   // QUEUEID
        + 4   // FLAG
        + 8   // QUEUEOFFSET
        + 8   // PHYSICALOFFSET
        + 4   // SYSFLAG
        + 8   // BORNTIMESTAMP
        + bornhostLength          // BORNHOST
        + 8   // STORETIMESTAMP
        + storehostAddressLength  // STOREHOSTADDRESS
        + 4   // RECONSUMETIMES
        + 8   // PreparedTransactionOffset
        + 4 + Math.max(bodyLength, 0)         // BODYLENGTH + BODY
        + messageVersion.getTopicLengthSize() + topicLength  // TOPICLENGTH + TOPIC
        + 2 + Math.max(propertiesLength, 0);  // PROPERTIESLENGTH + PROPERTIES
}
```

IPv4场景下（无V6标志），固定头部为 `4+4+4+4+4+8+8+4+8+8+8+8+4+8+4 = 88` 字节，加上 topic、body、properties 的变长部分。

**第三阶段：DefaultAppendMessageCallback.doAppend() —— 回填与物理写入**

`encode()` 完成后，`asyncPutMessage` 将预编码的 ByteBuf 设置到消息对象上（`msg.setEncodedBuff()`），然后在 `putMessageLock` 内调用 `MappedFile.appendMessage()`，最终进入 `doAppend()`。`doAppend` 的职责是：**回填3个占位字段**，然后将整个预编码缓冲区拷贝到 MappedFile 的 ByteBuffer 中。

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/CommitLog.java
// 行 1892-2081, DefaultAppendMessageCallback.doAppend
public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
    final int maxBlank, final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
    
    // 获取预编码缓冲区
    ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
    final int msgLen = preEncodeBuffer.getInt(0);  // 读取TOTALSIZE
    preEncodeBuffer.position(0);
    preEncodeBuffer.limit(msgLen);
    
    // 计算物理偏移 = 文件起始偏移 + 当前写入位置
    long wroteOffset = fileFromOffset + byteBuffer.position();
    
    // 懒构造 msgId（storeHost + wroteOffset 的十六进制表示）
    Supplier<String> msgIdSupplier = () -> {
        int msgIdLen = (sysFlag & STOREHOSTADDRESS_V6_FLAG) == 0 ? 16 : 28;
        ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
        MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
        msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
        return UtilAll.bytes2string(msgIdBuffer.array());
    };
    
    Long queueOffset = msgInner.getQueueOffset();
    
    // 事务消息特殊处理：PREPARED和ROLLBACK不进入ConsumeQueue
    final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
    switch (tranType) {
        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
            queueOffset = 0L;  // 这些消息不分配queueOffset
            break;
    }
    
    // ★ 检查剩余空间是否足够（msgLen + 8字节空白标记 > maxBlank）
    if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
        // 空间不足，写入文件结束标记（BLANK_MAGIC_CODE = -875286124）
        this.msgStoreItemMemory.clear();
        this.msgStoreItemMemory.putInt(maxBlank);        // TOTALSIZE = 剩余空间
        this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);  // MAGICCODE
        byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
        return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
            maxBlank, msgIdSupplier, msgInner.getStoreTimestamp(), queueOffset, 0);
    }
    
    // ★★★ 回填3个占位字段（使用绝对位置put，不改变position）★★★
    int pos = 4 + 4 + 4 + 4 + 4;  // 跳过 TOTALSIZE, MAGICCODE, BODYCRC, QUEUEID, FLAG = 20字节
    
    // 回填 QUEUEOFFSET（偏移量20）
    preEncodeBuffer.putLong(pos, queueOffset);
    pos += 8;  // pos = 28
    
    // 回填 PHYSICALOFFSET（偏移量28）
    preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
    pos += 8;  // pos = 36
    
    // 跳过 SYSFLAG(4) + BORNTIMESTAMP(8) + BORNHOST(8或20)
    int ipLen = (sysFlag & BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
    pos += 4 + 8 + ipLen;  // pos = 36 + 4 + 8 + 8 = 56 (IPv4)
    
    // 回填 STORETIMESTAMP（偏移量56 for IPv4）
    preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());
    
    // 可选：计算并回填 CRC32
    if (enabledAppendPropCRC) {
        int checkSize = msgLen - crc32ReservedLength;
        ByteBuffer tmpBuffer = preEncodeBuffer.duplicate();
        tmpBuffer.limit(tmpBuffer.position() + checkSize);
        int crc32 = UtilAll.crc32(tmpBuffer);
        MessageDecoder.createCrc32(tmpBuffer, crc32);
    }
    
    // ★★★ 物理写入：将预编码缓冲区拷贝到 MappedFile 的 ByteBuffer ★★★
    byteBuffer.put(preEncodeBuffer);
    msgInner.setEncodedBuff(null);  // 释放引用
    
    return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen,
        msgIdSupplier, msgInner.getStoreTimestamp(), queueOffset, 0, messageNum);
}
```

**doAppend 的三个关键回填点总结：**

```
回填字段          在preEncodeBuffer中的偏移    值来源
────────────────────────────────────────────────────────────────────
QUEUEOFFSET       20                          msgInner.getQueueOffset()
                                               （asyncPutMessage第8步通过assignOffset分配）
PHYSICALOFFSET    28                          fileFromOffset + byteBuffer.position()
                                               （文件起始偏移 + 当前写入位置）
STORETIMESTAMP    56(IPv4)/68(IPv6)           msgInner.getStoreTimestamp()
                                               （在putMessageLock内重新设置，保证全局有序）
```

为什么 QUEUEOFFSET 和 PHYSICALOFFSET 在 encode 阶段写 0？因为 encode 在 `putMessageLock` 之外执行（为了减少锁持有时间），此时还不知道物理偏移量。STORETIMESTAMP 在锁内重新设置是为了保证消息在 CommitLog 中的存储时间戳单调递增。

**第四阶段：MappedFile 的三位置模型**

消息写入 MappedFile 后，数据流向涉及三个位置指针：`wrotePosition`、`committedPosition`、`flushedPosition`。它们的关系取决于是否启用了 TransientStorePool（堆外内存池）。

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/logfile/DefaultMappedFile.java
// 行 80-82
protected volatile int wrotePosition;       // 数据写入到缓冲区的位置
protected volatile int committedPosition;   // 数据从writeBuffer提交到FileChannel的位置
protected volatile int flushedPosition;     // 数据刷盘到磁盘的位置
```

**场景A：无 TransientStorePool（默认配置，writeBuffer == null）**

数据直接写入 `mappedByteBuffer`（mmap 映射），无需 commit 步骤：

```
写入: wrotePosition → mappedByteBuffer (mmap)
commit(): 直接返回 wrotePosition（no-op）
flush(): mappedByteBuffer.force() → flushedPosition = wrotePosition

关系: wrotePosition == committedPosition >= flushedPosition
```

**场景B：有 TransientStorePool（writeBuffer != null）**

数据先写入堆外 `writeBuffer`，再通过 FileChannel 提交，最后刷盘：

```
写入: wrotePosition → writeBuffer (堆外ByteBuffer)
commit(): writeBuffer[committed..wrote] → FileChannel.write() → committedPosition = wrotePosition
flush(): fileChannel.force(false) → flushedPosition = committedPosition

关系: wrotePosition >= committedPosition >= flushedPosition
```

`appendMessagesInner` 的核心逻辑——获取当前写入位置，切片 ByteBuffer，调用 `doAppend`，然后递增 `wrotePosition`：

```java
// DefaultMappedFile.java 行 351-422
public AppendMessageResult appendMessagesInner(final MessageExt messageExt,
    final AppendMessageCallback cb, PutMessageContext putMessageContext) {
    
    int currentPos = WROTE_POSITION_UPDATER.get(this);  // 当前写入位置
    long fileFromOffset = this.getFileFromOffset();
    
    if (currentPos < this.fileSize) {
        // 切片ByteBuffer，定位到currentPos
        ByteBuffer byteBuffer = appendMessageBuffer().slice();
        byteBuffer.position(currentPos);
        
        // 调用doAppend，maxBlank = fileSize - currentPos
        AppendMessageResult result;
        if (messageExt instanceof MessageExtBrokerInner) {
            result = cb.doAppend(fileFromOffset, byteBuffer, this.fileSize - currentPos,
                (MessageExtBrokerInner) messageExt, putMessageContext);
        }
        
        // 递增 wrotePosition
        WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
        return result;
    }
    return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
}

// appendMessageBuffer: 返回writeBuffer（如果TransientStorePool启用）或mappedByteBuffer
protected ByteBuffer appendMessageBuffer() {
    return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
}
```

**commit0()：从 writeBuffer 到 FileChannel 的数据搬运**

```java
// DefaultMappedFile.java 行 589-605
protected void commit0() {
    int writePos = WROTE_POSITION_UPDATER.get(this);
    int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);
    
    if (writePos - lastCommittedPosition > 0) {
        ByteBuffer byteBuffer = writeBuffer.slice();
        byteBuffer.position(lastCommittedPosition);
        byteBuffer.limit(writePos);
        this.fileChannel.position(lastCommittedPosition);
        this.fileChannel.write(byteBuffer);
        COMMITTED_POSITION_UPDATER.set(this, writePos);
    }
}
```

**flush()：强制刷盘**

```java
// DefaultMappedFile.java 行 526-559
public int flush(final int flushLeastPages) {
    if (this.isAbleToFlush(flushLeastPages)) {
        if (this.hold()) {
            int value = getReadPosition();  // 无TSP: wrotePosition; 有TSP: committedPosition
            
            if (writeWithoutMmap || writeBuffer != null || fileChannel.position() != 0) {
                this.fileChannel.force(false);  // 通过FileChannel刷盘
            } else {
                this.mappedByteBuffer.force();  // 通过mmap刷盘
            }
            
            FLUSHED_POSITION_UPDATER.set(this, value);
        }
    }
    return this.getFlushedPosition();
}
```

`isAbleToFlush` 和 `isAbleToCommit` 都基于页大小（OS_PAGE_SIZE = 4KB）进行阈值判断，只有积攒了足够多的脏页才触发刷盘/提交，避免频繁 I/O。

**MessageExtBrokerInner 的继承链与扩展字段：**

```
Message (topic, flag, properties, body, transactionId)
  └── MessageExt (brokerName, queueId, storeSize, queueOffset, sysFlag,
                  bornTimestamp, bornHost, storeTimestamp, storeHost,
                  msgId, commitLogOffset, bodyCRC, reconsumeTimes,
                  preparedTransactionOffset)
        └── MessageExtBrokerInner (propertiesString, tagsCode, encodedBuff,
                                    encodeCompleted, version)
```

`MessageExtBrokerInner` 相比 `MessageExt` 额外增加了：
- `propertiesString`：properties Map 的序列化字符串形式，直接写入磁盘
- `tagsCode`：tags 的 hashCode，存入 ConsumeQueue 用于快速过滤
- `encodedBuff`：预编码后的 ByteBuffer，由 encoder 生成、doAppend 消费
- `version`：MESSAGE_VERSION_V1 或 V2，控制 magic code 和 topic 长度字段大小

写入成功后，`ReputMessageService` 异步将消息分发到 ConsumeQueue 和 IndexFile。

**步骤7：`handlePutMessageResult`** 根据 `PutMessageStatus` 构建响应：
- `PUT_OK`：返回 `SEND_OK`
- `FLUSH_DISK_TIMEOUT`：返回 `FLUSH_DISK_TIMEOUT`
- `FLUSH_SLAVE_TIMEOUT`：返回 `FLUSH_SLAVE_TIMEOUT`
- `SLAVE_NOT_AVAILABLE`：返回 `SLAVE_NOT_AVAILABLE`

#### 1.3.3 Consumer 端消费链路

Consumer 的启动流程如下：

```
DefaultMQPushConsumer.start()
    → DefaultMQPushConsumerImpl.start()
        → 检查配置
        → 复制订阅关系
        → 初始化OffsetStore (RemoteBrokerOffsetStore for CLUSTERING)
        → 创建ConsumeMessageService (ConsumeMessageConcurrentlyService)
        → MQClientInstance.start()
            → mQClientAPIImpl.start() (Netty客户端)
            → 启动定时任务 (心跳、路由刷新等)
            → PullMessageService.start() (拉取服务)
            → RebalanceService.start() (负载均衡服务)
        → 向所有Broker发送心跳
```

**RebalanceService 触发拉取**：

RebalanceService 每 20 秒执行一次 `doRebalance`，对每个订阅的 Topic 进行队列分配。分配到新的 MessageQueue 后，创建 `PullRequest` 并放入 `PullMessageService` 的队列：

```java
// RebalanceImpl.updateProcessQueueTableInRebalance
// 对每个新分配到的MessageQueue：
ProcessQueue pq = createProcessQueue();
long nextOffset = computePullFromWhere(mq);
PullRequest pullRequest = new PullRequest();
pullRequest.setConsumerGroup(consumerGroup);
pullRequest.setNextOffset(nextOffset);
pullRequest.setMessageQueue(mq);
pullRequest.setProcessQueue(pq);
pullRequestList.add(pullRequest);
```

**PullMessageService 拉取消息**：

```java
// PullMessageService.run()
while (!this.isStopped()) {
    PullRequest pullRequest = this.pullRequestQueue.take();  // 阻塞获取
    this.pullMessage(pullRequest);
}

private void pullMessage(final PullRequest pullRequest) {
    final DefaultMQPushConsumerImpl consumer = this.mQClientFactory
        .getDefaultMQPushConsumerImpl(pullRequest.getConsumerGroup());
    consumer.pullMessage(pullRequest);
}
```

**`DefaultMQPushConsumerImpl.pullMessage` 核心流控逻辑**：

```java
public void pullMessage(final PullRequest pullRequest) {
    final ProcessQueue processQueue = pullRequest.getProcessQueue();
    
    // 流控门控1：ProcessQueue被丢弃
    if (processQueue.isDropped()) return;
    
    // 流控门控2：消息堆积超过阈值（默认1000条）
    if (processQueue.getMsgCount() > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
        // 延迟50ms后重试
        this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
        return;
    }
    
    // 流控门控3：消息大小超过阈值（默认100MB）
    if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
        // 延迟50ms后重试
        this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
        return;
    }
    
    // 构建拉取请求
    final PullCallback pullCallback = new PullCallback() {
        @Override
        public void onSuccess(PullResult pullResult) {
            // 拉取成功后处理
            processPullResult(pullResult, subscriptionData);
            switch (pullResult.getPullStatus()) {
                case FOUND:
                    // 将消息放入ProcessQueue
                    processQueue.putMessage(pullResult.getMsgFoundList());
                    // 提交消费任务
                    consumeMessageService.submitConsumeRequest(
                        pullResult.getMsgFoundList(), processQueue, pullRequest.getMessageQueue(), true);
                    // 如果还有更多消息且队列未满，继续拉取
                    if (pullResult.getNextBeginOffset() != null) {
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                    }
                    if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty()) {
                        executePullRequestImmediately(pullRequest);
                    } else {
                        executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_NO_MESSAGE);
                    }
                    break;
                case NO_NEW_MSG:
                    // 没有新消息，延迟拉取
                    executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_NO_MESSAGE);
                    break;
                // ...
            }
        }
    };
    
    // 发送拉取请求
    this.mQClientFactory.getMQClientAPIImpl().pullMessage(
        brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback);
}
```

**ConsumeMessageConcurrentlyService 消费消息**：

```java
class ConsumeRequest implements Runnable {
    @Override
    public void run() {
        // ...
        try {
            // 调用用户注册的MessageListener
            status = messageListener.consumeMessage(
                Collections.unmodifiableList(msgs), context);
        } catch (Throwable e) {
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        
        // 处理消费结果
        processConsumeResult(status, context, this);
    }
}
```

### 1.3.4 Broker 端拉取处理与长轮询机制（PullMessageProcessor + PullRequestHoldService）

当 Consumer 发送 `PULL_MESSAGE` 请求到 Broker 后，`PullMessageProcessor` 负责处理。这条链路涉及消息查找、过滤、以及当没有消息时的长轮询挂起机制。

**PullMessageProcessor.processRequest：验证与消息查找**

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/processor/PullMessageProcessor.java
// 行 304-610
private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
    boolean brokerAllowSuspend, boolean brokerAllowFlowCtrSuspend) {
    
    final PullMessageRequestHeader requestHeader = ...;
    final ResponseCode subscriptionConfig = ...;
    
    // 1. 验证消费组配置
    SubscriptionGroupConfig subscriptionGroupConfig = 
        this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
    if (null == subscriptionGroupConfig) {
        response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
        return response;
    }
    
    // 2. 获取或构建订阅信息
    // 两种来源：请求中携带(hasSubscriptionFlag) 或 Broker端ConsumerGroupInfo缓存
    if (hasSubscriptionFlag) {
        subscriptionData = FilterAPI.build(topic, subscription, expressionType);
        if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            consumerFilterData = ConsumerFilterManager.build(...);
        }
    } else {
        ConsumerGroupInfo consumerGroupInfo = 
            this.brokerController.getConsumerManager().getConsumerGroupInfo(group);
        subscriptionData = consumerGroupInfo.findSubscriptionData(topic);
        // 版本校验、过滤数据校验...
    }
    
    // 3. 构建消息过滤器（两种实现）
    MessageFilter messageFilter;
    if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
        messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, ...);
    } else {
        messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData, ...);
    }
    
    // 4. ★ 调用 store.getMessageAsync 查找消息
    messageStore.getMessageAsync(group, storeTopic, queueId, requestHeader.getQueueOffset(),
            requestHeader.getMaxMsgNums(), messageFilter)
        .thenApply(result -> {
            // 5. 委托给 pullMessageResultHandler 处理结果
            return pullMessageResultHandler.handle(result, request, requestHeader, channel,
                finalSubscriptionData, subscriptionGroupConfig, brokerAllowSuspend,
                messageFilter, finalResponse, mappingContext, beginTimeMills);
        });
}
```

**DefaultMessageStore.getMessage：从 ConsumeQueue 查找消息并读取 CommitLog**

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java
// 行 865-1062
public GetMessageResult getMessage(final String group, final String topic,
    final int queueId, final long offset, final int maxMsgNums,
    final int maxTotalMsgSize, final MessageFilter messageFilter) {
    
    // 1. 定位 ConsumeQueue
    ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
    minOffset = consumeQueue.getMinOffsetInQueue();
    maxOffset = consumeQueue.getMaxOffsetInQueue();
    
    // 2. 偏移量边界检查
    if (offset < minOffset) {
        status = GetMessageStatus.OFFSET_TOO_SMALL;
        nextBeginOffset = minOffset;
    } else if (offset == maxOffset) {
        status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
        nextBeginOffset = offset;
    } else if (offset > maxOffset) {
        status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
        nextBeginOffset = maxOffset;
    } else {
        // 3. ★ 从 ConsumeQueue 迭代读取 CqUnit
        ReferredIterator<CqUnit> bufferConsumeQueue = 
            consumeQueue.iterateFrom(nextBeginOffset, maxMsgNums);
        
        while (bufferConsumeQueue.hasNext() && nextBeginOffset < maxOffset) {
            CqUnit cqUnit = bufferConsumeQueue.next();
            long offsetPy = cqUnit.getPos();    // CommitLog物理偏移
            int sizePy = cqUnit.getSize();       // 消息大小
            
            // 4. ★ 第一阶段过滤：ConsumeQueue级别（tagsCode快速过滤）
            if (messageFilter != null
                && !messageFilter.isMatchedByConsumeQueue(
                    cqUnit.getValidTagsCodeAsLong(), cqUnit.getCqExtUnit())) {
                continue;  // 不匹配，跳过（不读取CommitLog）
            }
            
            // 5. 从 CommitLog 读取消息
            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
            if (null == selectResult) {
                // 消息已被删除（MappedFile过期），跳到下一个文件
                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                continue;
            }
            
            // 6. ★ 第二阶段过滤：CommitLog级别（表达式精确过滤）
            if (messageFilter != null
                && !messageFilter.isMatchedByCommitLog(
                    selectResult.getByteBuffer().slice(), null)) {
                selectResult.release();
                filterMessageCount++;
                continue;
            }
            
            // 7. 匹配成功，加入结果
            getResult.addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
            status = GetMessageStatus.FOUND;
            
            // 8. 批量大小/总大小限制检查
            if (isTheBatchFull(sizePy, batchNum, maxMsgNums, maxPullSize, ...)) {
                break;
            }
        }
    }
    
    getResult.setStatus(status);
    getResult.setNextBeginOffset(nextBeginOffset);
    getResult.setMaxOffset(maxOffset);
    getResult.setMinOffset(minOffset);
    return getResult;
}
```

**长轮询：当没有消息时挂起请求**

当 `getMessage` 返回 `NO_MESSAGE_IN_QUEUE` 或 `OFFSET_OVERFLOW_ONE` 等状态时，`DefaultPullMessageResultHandler` 将请求挂起到 `PullRequestHoldService`：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/processor/DefaultPullMessageResultHandler.java
// 行 172-188
case ResponseCode.PULL_NOT_FOUND:
    final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
    final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;
    
    if (brokerAllowSuspend && hasSuspendFlag) {
        long pollingTimeMills = suspendTimeoutMillisLong;
        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
            // 长轮询未启用，使用短轮询超时（默认1秒）
            pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
        }
        
        // 创建挂起的PullRequest
        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills,
            this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);
        // ★ 挂起到 PullRequestHoldService
        this.brokerController.getPullRequestHoldService()
            .suspendPullRequest(topic, queueId, pullRequest);
        return null;  // ★ 不发送响应！请求被挂起
    }
```

关键点：`brokerAllowSuspend` 在首次调用时为 `true`，但在 `executeRequestWhenWakeup` 重新执行时为 `false`，确保被唤醒的请求不会被再次挂起。

**PullRequestHoldService：挂起请求的存储与唤醒**

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/longpolling/PullRequestHoldService.java

// 数据结构：topic@queueId → ManyPullRequest（synchronized ArrayList）
protected ConcurrentMap<String, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<>(1024);

// 挂起请求
public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
    String key = this.buildKey(topic, queueId);  // "topic@queueId"
    ManyPullRequest mpr = this.pullRequestTable.get(key);
    if (null == mpr) {
        mpr = new ManyPullRequest();
        ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
        if (prev != null) { mpr = prev; }
    }
    pullRequest.getRequestCommand().setSuspended(true);
    mpr.addPullRequest(pullRequest);
}

// 主循环：每5秒检查一次所有挂起的请求
@Override
public void run() {
    while (!this.isStopped()) {
        if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
            this.waitForRunning(5 * 1000);  // 5秒
        } else {
            this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
        }
        this.checkHoldRequest();
    }
}

// 检查并唤醒
protected void checkHoldRequest() {
    for (String key : this.pullRequestTable.keySet()) {
        String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
        String topic = kArray[0];
        int queueId = Integer.parseInt(kArray[1]);
        final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        this.notifyMessageArriving(topic, queueId, offset);
    }
}

// ★ 核心唤醒逻辑
public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset,
    final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
    
    String key = this.buildKey(topic, queueId);
    ManyPullRequest mpr = this.pullRequestTable.get(key);
    if (mpr != null) {
        List<PullRequest> requestList = mpr.cloneListAndClear();
        if (requestList != null) {
            List<PullRequest> replayList = new ArrayList<>();
            
            for (PullRequest request : requestList) {
                long newestOffset = maxOffset;
                if (newestOffset <= request.getPullFromThisOffset()) {
                    newestOffset = this.brokerController.getMessageStore()
                        .getMaxOffsetInQueue(topic, queueId);
                }
                
                if (newestOffset > request.getPullFromThisOffset()) {
                    // ★ 有新消息！检查过滤器
                    boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode, ...);
                    if (match && properties != null) {
                        match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                    }
                    
                    if (match) {
                        // ★ 唤醒：重新执行拉取请求
                        this.brokerController.getPullMessageProcessor()
                            .executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                        continue;
                    }
                }
                
                // 超时检查
                if (System.currentTimeMillis() >= 
                    (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                    // ★ 超时唤醒：重新执行（会返回PULL_NOT_FOUND给客户端）
                    this.brokerController.getPullMessageProcessor()
                        .executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                    continue;
                }
                
                // 未到期且无新消息：放回继续等待
                replayList.add(request);
            }
            
            if (!replayList.isEmpty()) {
                mpr.addPullRequest(replayList);
            }
        }
    }
}
```

**ReputMessageService：实时触发长轮询唤醒**

除了 `PullRequestHoldService` 每5秒的定时检查外，`ReputMessageService` 每1毫秒扫描 CommitLog，当新消息被分发到 ConsumeQueue 时立即触发唤醒：

```java
// 源码路径: store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java
// ReputMessageService.doReput(), 行 2711-2789
public void doReput() {
    for (boolean doNext = true; isCommitLogAvailable() && doNext; ) {
        SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
        
        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
            DispatchRequest dispatchRequest =
                DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), ...);
            
            if (dispatchRequest.isSuccess() && size > 0) {
                // 1. 分发到 ConsumeQueue 和 IndexFile
                DefaultMessageStore.this.doDispatch(dispatchRequest);
                
                // 2. ★ 如果启用长轮询，立即通知
                if (isNotifyMessageArriveWhenReput()) {
                    notifyMessageArriveIfNecessary(dispatchRequest);
                }
                
                this.reputFromOffset += size;
                readSize += size;
            }
        }
    }
}

// notifyMessageArriveIfNecessary: 通过监听器触发 PullRequestHoldService
@Override
public void notifyMessageArriveIfNecessary(DispatchRequest dispatchRequest) {
    if (DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
        && DefaultMessageStore.this.messageArrivingListener != null) {
        DefaultMessageStore.this.messageArrivingListener.arriving(
            dispatchRequest.getTopic(),
            dispatchRequest.getQueueId(),
            dispatchRequest.getConsumeQueueOffset() + 1,  // 新消息的offset
            dispatchRequest.getTagsCode(),
            dispatchRequest.getStoreTimestamp(),
            dispatchRequest.getBitMap(),
            dispatchRequest.getPropertiesMap());
    }
}

// NotifyMessageArrivingListener: 桥接到 PullRequestHoldService
public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
    long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
    this.pullRequestHoldService.notifyMessageArriving(
        topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
}
```

**长轮询的完整生命周期：**

```
1. Consumer 发送 PULL_MESSAGE → PullMessageProcessor
2. PullMessageProcessor 调用 getMessage → 无消息 → PULL_NOT_FOUND
3. DefaultPullMessageResultHandler 挂起请求到 PullRequestHoldService
   → 返回 null（不发送响应）
   → Consumer 的 Netty 客户端在等待响应

4. 【唤醒路径A — 实时（1ms延迟）】
   ReputMessageService.doReput() 发现新消息
   → notifyMessageArriveIfNecessary
   → NotifyMessageArrivingListener.arriving()
   → PullRequestHoldService.notifyMessageArriving()
   → 检查 offset 和过滤器
   → executeRequestWhenWakeup() 重新执行 processRequest
   → brokerAllowSuspend=false → 有消息返回 FOUND，无消息返回 PULL_NOT_FOUND

5. 【唤醒路径B — 定时（5秒）】
   PullRequestHoldService.run() 每5秒
   → checkHoldRequest() 遍历所有挂起请求
   → notifyMessageArriving() 检查 offset 和超时
   → 超时的请求执行 executeRequestWhenWakeup()

6. executeRequestWhenWakeup:
   processRequest(channel, request, false, brokerAllowFlowCtrSuspend)
   → brokerAllowSuspend=false，不会再挂起
   → 返回响应给 Consumer
```

这种设计使得 Consumer 在没有新消息时不会频繁空轮询（减少网络开销），同时在新消息到达时能在1ms内被唤醒（保证低延迟）。默认挂起超时为15秒（由客户端 `suspendTimeoutMillis` 控制），超时后返回 `PULL_NOT_FOUND`，Consumer 会立即重新发起拉取。

### 1.4 完整时序图

```
Producer                     NameServer              Broker                  Consumer
    │                            │                      │                        │
    │── 路由查询 ──────────────>│                      │                        │
    │<── TopicRouteData ────────│                      │                        │
    │                            │                      │                        │
    │── SEND_MESSAGE ─────────────────────────────────>│                        │
    │   (topic, queueId, body)  │                      │                        │
    │                            │                      │── 写入CommitLog        │
    │                            │                      │── 异步分发ConsumeQueue  │
    │<── SEND_OK ──────────────────────────────────────│                        │
    │                            │                      │                        │
    │                            │                      │<── PULL_MESSAGE ──────│
    │                            │                      │   (group, topic,      │
    │                            │                      │    queueId, offset)   │
    │                            │                      │── 查ConsumeQueue       │
    │                            │                      │── 读CommitLog          │
    │                            │                      │── 返回消息列表 ────────>│
    │                            │                      │                        │── 调用Listener
    │                            │                      │<── UPDATE_OFFSET ─────│
    │                            │                      │                        │
```

### 1.5 知识点：消息 ID 的生成

```java
// MessageClientIDSetter.setUniqID(msg)
public static void setUnuniqID(final Message msg) {
    if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
        msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            createUniqID());
    }
}

public static String createUniqID() {
    StringBuilder sb = new StringBuilder(LEN * 2);
    sb.append(hexString(START_TIMESTAMP));   // 8字节：启动时间戳
    sb.append(hexString(COUNTER.getAndIncrement())); // 4字节：计数器
    sb.append(hexString(LEN));               // 4字节：进程PID
    sb.append(hexString(InstanceUtil.getUniqInstanceIndex())); // 4字节：实例索引
    return sb.toString();
}
```

消息 ID 由 Producer 端生成，包含启动时间戳、计数器、PID 和实例索引。这个 ID 在整个消息生命周期中不变，用于幂等去重和消息追踪。

---

## 场景二：顺序消息 —— 订单状态变更链路

### 2.1 业务背景

订单状态需要按创建→支付→发货→完成 的顺序处理。如果使用普通消息，不同状态的消息可能被不同队列消费，导致顺序错乱（如先收到发货再收到支付）。顺序消息保证同一订单的所有状态变更消息按发送顺序被消费。

### 2.2 用户代码

**生产者（使用 MessageQueueSelector 实现分区顺序）**：

```java
DefaultMQProducer producer = new DefaultMQProducer("order_status_producer");
producer.start();

String[] orderStatuses = {"CREATED", "PAID", "SHIPPED", "DELIVERED", "COMPLETED"};
for (String status : orderStatuses) {
    Message msg = new Message("OrderStatusTopic", "status_change",
        ("订单" + orderId + "状态变更为: " + status).getBytes());
    
    // 关键：使用orderId作为分区键，保证同一订单的消息进入同一队列
    SendResult result = producer.send(msg, new MessageQueueSelector() {
        @Override
        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
            String orderId = (String) arg;
            int index = Math.abs(orderId.hashCode()) % mqs.size();
            return mqs.get(index);
        }
    }, orderId);
    
    System.out.println("发送: " + status + ", 队列: " + result.getMessageQueue().getQueueId());
}
```

**消费者（顺序消费）**：

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_status_consumer");
consumer.subscribe("OrderStatusTopic", "status_change");

// 关键：注册顺序消费的MessageListener
consumer.registerMessageListener(new MessageListenerOrderly() {
    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        for (MessageExt msg : msgs) {
            System.out.println("消费: " + new String(msg.getBody()));
        }
        return ConsumeOrderlyStatus.SUCCESS;
    }
});
consumer.start();
```

### 2.3 源码全链路追踪

#### 2.3.1 生产者端：MessageQueueSelector 选择队列

```java
// DefaultMQProducerImpl.send(Message msg, MessageQueueSelector selector, Object arg)
public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws ... {
    msg.setTopic(withNamespace(msg.getTopic()));
    return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null,
        this.defaultMQProducer.getSendMsgTimeout());
}

private SendResult sendSelectImpl(Message msg, MessageQueueSelector selector, Object arg,
    CommunicationMode communicationMode, SendCallback sendCallback, final long timeout) throws ... {
    
    this.makeSureStateOK();
    Validators.checkMessage(msg, this.defaultMQProducer);
    
    TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
    if (topicPublishInfo == null || !topicPublishInfo.ok()) {
        throw new MQClientException("No route info");
    }
    
    // 关键：调用用户提供的selector选择队列
    MessageQueue mq = topicPublishInfo.selectOneMessageQueue(selector, msg, arg);
    if (mq != null) {
        // 设置Namespace后发送
        mq = queueWithNamespace(mq);
        return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout);
    }
    throw new MQClientException("select message queue return null.");
}
```

`topicPublishInfo.selectOneMessageQueue` 最终调用用户提供的 `selector.select(mqs, msg, arg)`，使用 `orderId.hashCode() % mqs.size()` 选择队列。这保证了同一 `orderId` 的所有消息进入同一 MessageQueue，从而保证顺序。

#### 2.3.2 消费者端：顺序消费的三级锁机制

顺序消费的核心在于确保同一队列同一时刻只有一个线程在消费，且在消费过程中不被 Rebalance 打断。RocketMQ 使用三级锁实现：

**第一级：Broker 端分布式锁**

在 CLUSTERING 模式下，消费者启动时会定期向 Broker 发送 `LOCK_BATCH_MQ` 请求锁定分配到的队列：

```java
// ConsumeMessageOrderlyService.start()
public void start() {
    if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
        // 每20秒续锁一次
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                ConsumeMessageOrderlyService.this.lockMQPeriodically();
            } catch (Throwable e) { ... }
        }, 1000, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);  // 20000ms
    }
    // ...
}
```

```java
// RebalanceImpl.lock(MessageQueue mq)
public boolean lock(final MessageQueue mq) {
    FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(
        this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
    if (findBrokerResult != null) {
        LockBatchRequestBody requestBody = new LockBatchRequestBody();
        requestBody.setConsumerGroup(this.consumerGroup);
        requestBody.setClientId(this.mQClientFactory.getClientId());
        requestBody.getMqSet().add(mq);
        try {
            Set<MessageQueue> lockedMq = this.mQClientFactory.getMQClientAPIImpl()
                .lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
            for (MessageQueue mmqq : lockedMq) {
                ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                if (processQueue != null) {
                    processQueue.setLocked(true);
                    processQueue.setLastLockTimestamp(System.currentTimeMillis());
                }
            }
            return lockedMq.contains(mq);
        } catch (Exception e) { ... }
    }
    return false;
}
```

Broker 端处理 `LOCK_BATCH_MQ`：

```java
// LockBatchMQProcessor.processRequest
// Broker维护一个 ConcurrentHashMap<String/*group*/, ConcurrentHashMap<MessageQueue, LockEntry>>
// LockEntry 包含 clientId 和 timestamp
// 只有同一clientId可以重入锁，不同clientId需要等锁过期(30秒)
```

**知识点：锁过期机制**

如果消费者崩溃，Broker 端的锁会在 `REBALANCE_LOCK_MAX_LIVE_TIME`（默认30秒）后自动过期，允许其他消费者接管该队列。这避免了因为消费者崩溃导致队列永久被锁定。

**第二级：客户端每队列锁（MessageQueueLock）**

```java
// ConsumeMessageOrderlyService.ConsumeRequest.run()
class ConsumeRequest implements Runnable {
    @Override
    public void run() {
        if (this.processQueue.isDropped()) return;
        
        // 获取该MessageQueue的独占锁对象
        final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
        synchronized (objLock) {  // ← 第二级锁：确保同一队列同一时刻只有一个线程消费
            if (MessageModel.BROADCASTING.equals(messageModel)
                || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                
                // 消费循环
                for (boolean continueConsume = true; continueConsume; ) {
                    if (this.processQueue.isDropped()) break;
                    
                    // 检查Broker锁状态
                    if (MessageModel.CLUSTERING.equals(messageModel)
                        && !this.processQueue.isLocked()) {
                        tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                        break;
                    }
                    if (MessageModel.CLUSTERING.equals(messageModel)
                        && this.processQueue.isLockExpired()) {
                        tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                        break;
                    }
                    
                    // 防止无限消费（默认60秒）
                    long interval = System.currentTimeMillis() - beginTime;
                    if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                        submitConsumeRequestLater(processQueue, messageQueue, 10);
                        break;
                    }
                    
                    // 从ProcessQueue取出消息
                    final int consumeBatchSize = ...getConsumeMessageBatchMaxSize();
                    List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
                    
                    if (!msgs.isEmpty()) {
                        // ← 第三级锁：防止Rebalance在消费过程中移除队列
                        this.processQueue.getConsumeLock().readLock().lock();
                        try {
                            if (this.processQueue.isDropped()) break;
                            status = messageListener.consumeMessage(msgs, context);
                        } finally {
                            this.processQueue.getConsumeLock().readLock().unlock();
                        }
                        
                        continueConsume = processConsumeResult(msgs, status, context, this);
                    } else {
                        continueConsume = false;
                    }
                }
            } else {

#### 2.3.2b processConsumeResult：顺序消费结果处理（真实源码）

`processConsumeResult` 是顺序消费中处理消费结果的核心方法，根据 `autoCommit` 和 `ConsumeOrderlyStatus` 的不同组合，走不同的分支：

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageOrderlyService.java
// 行 274-345
public boolean processConsumeResult(
    final List<MessageExt> msgs,
    final ConsumeOrderlyStatus status,
    final ConsumeOrderlyContext context,
    final ConsumeRequest consumeRequest) {
    
    boolean continueConsume = true;
    long commitOffset = -1L;
    
    if (context.isAutoCommit()) {
        // ========== autoCommit == true 分支 ==========
        switch (status) {
            case COMMIT:
            case ROLLBACK:
                // ★ 警告：autoCommit模式下COMMIT和ROLLBACK是非法的，当作SUCCESS处理
                log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                    consumeRequest.getMessageQueue());
            case SUCCESS:
                // 消费成功，提交 ProcessQueue 中的消息
                commitOffset = consumeRequest.getProcessQueue().commit();
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, 
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                // ★ 消费失败，暂停当前队列
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                if (checkReconsumeTimes(msgs)) {
                    // 重试次数未超限：将消息放回 ProcessQueue 待消费
                    consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                    // 延迟后重新提交消费请求
                    this.submitConsumeRequestLater(
                        consumeRequest.getProcessQueue(),
                        consumeRequest.getMessageQueue(),
                        context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;  // 暂停消费循环
                } else {
                    // ★ 重试次数超限：直接提交（相当于ACK，消息不再重试）
                    commitOffset = consumeRequest.getProcessQueue().commit();
                }
                break;
            default:
                break;
        }
    } else {
        // ========== autoCommit == false 分支（手动提交） ==========
        switch (status) {
            case SUCCESS:
                // 不自动提交，由用户通过 context.commit() 手动提交
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, 
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            case COMMIT:
                // 用户显式提交
                commitOffset = consumeRequest.getProcessQueue().commit();
                break;
            case ROLLBACK:
                // 回滚 ProcessQueue
                consumeRequest.getProcessQueue().rollback();
                this.submitConsumeRequestLater(
                    consumeRequest.getProcessQueue(),
                    consumeRequest.getMessageQueue(),
                    context.getSuspendCurrentQueueTimeMillis());
                continueConsume = false;
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                if (checkReconsumeTimes(msgs)) {
                    consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                    this.submitConsumeRequestLater(
                        consumeRequest.getProcessQueue(),
                        consumeRequest.getMessageQueue(),
                        context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                }
                // ★ 手动模式下重试超限：什么都不做（消息既不提交也不回滚）
                // 这意味着消息会留在 consumingMsgOrderlyTreeMap 中
                break;
            default:
                break;
        }
    }
    
    // 更新消费 offset
    if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
        this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(
            consumeRequest.getMessageQueue(), commitOffset, false);
    }
    
    return continueConsume;
}
```

**checkReconsumeTimes：顺序消费的重试次数控制**

```java
// 行 360-377
private boolean checkReconsumeTimes(List<MessageExt> msgs) {
    boolean suspend = false;
    if (msgs != null && !msgs.isEmpty()) {
        for (MessageExt msg : msgs) {
            if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                // ★ 超过最大重试次数 → 发送到重试Topic（%RETRY%group）
                MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                if (!sendMessageBack(msg)) {
                    // 发回失败 → 继续本地暂停重试
                    suspend = true;
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
                // 发回成功 → suspend=false，消息被ACK（不再本地重试）
            } else {
                // 未超限 → 本地暂停重试
                suspend = true;
                msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
            }
        }
    }
    return suspend;
}

// 默认最大重试次数：Integer.MAX_VALUE（无限重试）
private int getMaxReconsumeTimes() {
    if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
        return Integer.MAX_VALUE;  // ★ 顺序消费默认无限重试！
    }
    return this.defaultMQPushConsumer.getMaxReconsumeTimes();
}
```

**顺序消费与并发消费的关键区别：**
- 顺序消费默认无限重试（`Integer.MAX_VALUE`），而并发消费默认16次
- 顺序消费的 `sendMessageBack` 将消息发到 `%RETRY%group` 并设置递增延时级别（`3 + reconsumeTimes`），与并发消费的重试机制殊途同归
- 顺序消费在 `SUSPEND_CURRENT_QUEUE_A_MOMENT` 时，消息被放回 `consumingMsgOrderlyTreeMap`（通过 `makeMessageToConsumeAgain`），而不是发回 Broker
- 只有当重试次数超限且 `sendMessageBack` 成功时，消息才被 ACK 并离开本地队列
                tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
            }
        }
    }
}
```

**第三级：ProcessQueue.consumeLock（读写锁）**

ProcessQueue 内部持有一个 `ReadWriteLock`：

```java
// ProcessQueue.java
private final ReadWriteLock lockConsume = new ReentrantReadWriteLock();
```

消费线程持有**读锁**，Rebalance 移除队列时需要获取**写锁**：

```java
// RebalancePushImpl.removeUnnecessaryMessageQueue (顺序消费模式)
private boolean tryRemoveOrderMessageQueue(final MessageQueue mq, final ProcessQueue pq) {
    try {
        boolean forceUnlock = pq.isDropped()
            && System.currentTimeMillis() > pq.getLastLockTimestamp() + UNLOCK_DELAY_TIME_MILLS;
        
        // 尝试获取写锁（最多等待500ms）
        if (forceUnlock || pq.getConsumeLock().writeLock().tryLock(500, TimeUnit.MILLISECONDS)) {
            try {
                this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
                this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
                pq.setLocked(false);
                this.unlock(mq, true);  // 释放Broker端锁
                return true;
            } finally {
                if (!forceUnlock) {
                    pq.getConsumeLock().writeLock().unlock();
                }
            }
        } else {
            pq.incTryUnlockTimes();
        }
    } catch (Exception e) { ... }
    return false;
}
```

**知识点：为什么需要读写锁？**

如果 Rebalance 在消费线程执行 `consumeMessage` 的过程中移除了 ProcessQueue 并更新了 offset，会导致消费了但没提交 offset，消息丢失。读写锁确保：
- 消费线程持有读锁时，Rebalance 无法获取写锁，必须等待消费完成
- 消费完成后释放读锁，Rebalance 才能获取写锁移除队列并提交 offset

#### 2.3.3 ProcessQueue 的消息追踪机制

顺序消费使用两个 TreeMap 追踪消息：

```java
// ProcessQueue.java
private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<>();           // 待消费消息
private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<>(); // 消费中消息

// 取出消息：从msgTreeMap移动到consumingMsgOrderlyTreeMap
public List<MessageExt> takeMessages(final int batchSize) {
    List<MessageExt> result = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; i++) {
        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
        if (entry != null) {
            result.add(entry.getValue());
            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
        } else break;
    }
    return result;
}

// 消费成功：移除consumingMsgOrderlyTreeMap中的消息，返回下一个offset
public long commit() {
    Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
    this.consumingMsgOrderlyTreeMap.clear();
    if (offset != null) return offset + 1;
    return -1;
}

// 消费失败回滚：将消息从consumingMsgOrderlyTreeMap移回msgTreeMap
public void rollback() {
    this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
    this.consumingMsgOrderlyTreeMap.clear();
}
```

**知识点：与并发消费的区别**

并发消费直接从 `msgTreeMap` 中 `removeMessage` 并立即更新 offset。顺序消费先 `takeMessages` 到 `consumingMsgOrderlyTreeMap`，消费成功后 `commit` 更新 offset，失败则 `rollback` 回滚到 `msgTreeMap`。这保证了消息在消费过程中不被其他线程消费，且失败时可以重新消费。

---

## 场景三：延时消息 —— 超时自动取消订单

### 3.1 业务背景

用户下单后30分钟未支付，系统需要自动取消订单并释放库存。传统方案使用定时任务轮询数据库，性能差且有延迟。RocketMQ 的延时消息可以在消息发送时指定延迟级别，到期后自动投递给消费者。

### 3.2 用户代码

```java
// 发送延时消息
Message msg = new Message("OrderTimeoutTopic", "timeout_check",
    ("订单" + orderId + "30分钟未支付，自动取消").getBytes());
// 设置延时级别：level 14 = 10分钟，level 16 = 30分钟
msg.setDelayTimeLevel(16);  // 30分钟
SendResult result = producer.send(msg);

// 消费者
consumer.subscribe("OrderTimeoutTopic", "timeout_check");
consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
    for (MessageExt msg : msgs) {
        // 检查订单状态，如果未支付则取消
        String orderId = parseOrderId(new String(msg.getBody()));
        Order order = orderService.getById(orderId);
        if (order.getStatus() == OrderStatus.UNPAID) {
            orderService.cancel(orderId, "超时未支付自动取消");
            inventoryService.release(order.getItems());
        }
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
```

### 3.3 源码全链路追踪

#### 3.3.1 Producer 端设置延时级别

```java
// Message.java
public void setDelayTimeLevel(int level) {
    this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
}
```

这只是在消息 Properties 中设置 `DELAY` 属性，延时逻辑完全在 Broker 端实现。

#### 3.3.2 Broker 端：消息存储时的特殊处理

当 `SendMessageProcessor` 处理消息时，检测到 `DELAY` 属性，将消息 Topic 改为 `SCHEDULE_TOPIC_XXXX`：

```java
// CommitLog.asyncPutMessage 中（在DefaultAppendMessageCallback.doAppend之前）
// 消息存储时，如果发现延迟级别，修改topic和queueId
if (delayLevel > 0) {
    topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
    queueId = ScheduleMessageService.delayLevel2QueueId(delayLevel);  // delayLevel - 1
}
```

**延时级别映射表**（`MessageStoreConfig.messageDelayLevel`）：

```
"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
```

| 级别 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 |
|------|---|---|---|---|---|---|---|---|---|----|----|----|----|----|----|----|----|----|
| 延迟 | 1s | 5s | 10s | 30s | 1m | 2m | 3m | 4m | 5m | 6m | 7m | 8m | 9m | 10m | 20m | 30m | 1h | 2h |

**关键：tagsCode 被替换为投递时间戳**

```java
// CommitLog.java (doAppend方法中)
{
    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
        int delayLevel = Integer.parseInt(t);
        if (delayLevel > this.defaultMessageStore.getMaxDelayLevel()) {
            delayLevel = this.defaultMessageStore.getMaxDelayLevel();
        }
        if (delayLevel > 0) {
            // 将tagsCode替换为投递时间戳 = 存储时间 + 延迟时间
            tagsCode = this.defaultMessageStore.computeDeliverTimestamp(
                delayLevel, storeTimestamp);
        }
    }
}
```

```java
// ScheduleMessageService.computeDeliverTimestamp
public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
    Long time = this.delayLevelTable.get(delayLevel);  // 如 level 16 → 30*60*1000 = 1800000ms
    if (time != null) return time + storeTimestamp;
    return storeTimestamp + 1000;  // 默认1秒
}
```

**知识点：为什么把 tagsCode 替换为时间戳？**

ConsumeQueue 中的每个条目有一个 `tagsCode` 字段（8字节），正常情况下存储的是 Tag 的 hashCode。延时消息将这个字段复用为"投递时间戳"（storeTimestamp + delayMillis）。这样 `DeliverDelayedMessageTimerTask` 扫描 ConsumeQueue 时可以直接比较时间戳判断是否到期，无需读取完整的 CommitLog 消息。

#### 3.3.3 Broker 端：定时投递机制

`ScheduleMessageService` 为每个延时级别创建一个 `DeliverDelayedMessageTimerTask`：

```java
// ScheduleMessageService.start()
for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
    Integer level = entry.getKey();
    Long timeDelay = entry.getValue();
    Long offset = this.offsetTable.get(level);
    if (null == offset) {
        offset = 0L;
    }
    if (delayTime < 0) {
        delayTime = timeDelay;
    }
    // 为每个level创建定时任务
    this.timer.schedule(
        new DeliverDelayedMessageTimerTask(this, level, offset),
        delayTime);
}
```

**`DeliverDelayedMessageTimerTask.executeOnTimeUp`** 核心逻辑：

```java
public void executeOnTimeUp() {
    // 1. 获取该级别的ConsumeQueue
    ConsumeQueueInterface cq = scheduleMessageService.getDefaultMessageStore()
        .getConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel2QueueId(delayLevel));
    
    if (cq == null) {
        // 队列不存在，延迟重试
        scheduleNextTimerTask(...);
        return;
    }
    
    // 2. 从上次处理位置开始扫描ConsumeQueue
    SelectMappedBufferResult bufferCQ = cq.iterateFrom(this.offset);
    if (bufferCQ == null) {
        scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
        return;
    }
    
    // 3. 逐条检查是否到期
    while (bufferCQ.hasNext()) {
        CqUnit cqUnit = bufferCQ.next();
        long offsetPy = cqUnit.getPos();       // CommitLog物理偏移
        int sizePy = cqUnit.getSize();          // 消息大小
        long tagsCode = cqUnit.getTagsCode();   // 实际是投递时间戳
        
        long now = System.currentTimeMillis();
        long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
        long countdown = deliverTimestamp - now;
        
        if (countdown > 0) {
            // 未到期，重新调度定时任务
            scheduleNextTimerTask(currOffset, countdown);
            scheduleMessageService.updateOffset(this.delayLevel, currOffset);
            return;
        }
        
        // 4. 已到期，从CommitLog读取原始消息
        MessageExt msgExt = scheduleMessageService.getDefaultMessageStore()
            .lookMessageByOffset(offsetPy, sizePy);
        
        // 5. 恢复消息原始Topic和QueueId
        MessageExtBrokerInner msgInner = scheduleMessageService.messageTimeUp(msgExt);
        
        // 6. 重新写入CommitLog（投递到原始Topic）
        boolean deliverSuc = scheduleMessageService.getBrokerController()
            .getEscapeBridge().asyncPutMessage(msgInner).get();
        
        if (deliverSuc) {
            // 投递成功，更新进度
            scheduleMessageService.updateOffset(this.delayLevel, currOffset);
        } else {
            // 投递失败，延迟重试
            scheduleNextTimerTask(currOffset, DELAY_FOR_A_WHILE);
            return;
        }
    }
    
    // 7. 当前队列扫描完毕，延迟后继续
    scheduleNextTimerTask(currOffset, DELAY_FOR_A_WHILE);
}
```

**`messageTimeUp`：恢复消息原始信息**

```java
public MessageExtBrokerInner messageTimeUp(MessageExt msgExt) {
    MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
    msgInner.setBody(msgExt.getBody());
    msgInner.setFlag(msgExt.getFlag());
    
    // 恢复原始Topic
    msgInner.setTopic(msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
    
    // 恢复原始QueueId
    int queueId = Integer.parseInt(msgExt.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID));
    msgInner.setQueueId(queueId);
    
    // 清除延时属性
    MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
    
    // 保留其他属性
    msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
    
    return msgInner;
}
```

### 3.4 延时消息全流程图

```
Producer                    Broker                              Consumer
   │                          │                                    │
   │── SEND_MSG(delayLevel=16)─>│                                    │
   │                          │                                    │
   │                          │── 改Topic=SCHEDULE_TOPIC_XXXX       │
   │                          │── 改QueueId=15 (level-1)            │
   │                          │── tagsCode=storeTime+30min          │
   │                          │── 写入CommitLog                     │
   │                          │── 异步分发到SCHEDULE_TOPIC的CQ      │
   │<── SEND_OK ──────────────│                                    │
   │                          │                                    │
   │                          │── [30分钟后]                        │
   │                          │   DeliverDelayedMessageTimerTask:   │
   │                          │   1. 扫描SCHEDULE_TOPIC的CQ         │
   │                          │   2. tagsCode <= now → 已到期       │
   │                          │   3. 读取CommitLog原始消息          │
   │                          │   4. 恢复Topic=OrderTimeoutTopic    │
   │                          │   5. 清除DELAY属性                  │
   │                          │   6. 重新写入CommitLog              │
   │                          │   7. 分发到OrderTimeoutTopic的CQ    │
   │                          │                                    │
   │                          │<── PULL_MESSAGE ──────────────────│
   │                          │── 返回消息 ────────────────────────>│
   │                          │                                    │── 检查订单状态
   │                          │                                    │── 取消未支付订单
```

### 3.5 知识点：延时消息的进度持久化

```java
// ScheduleMessageService.persist()
public synchronized void persist() {
    // 将offsetTable序列化为JSON
    DelayOffsetSerializeWrapper wrapper = new DelayOffsetSerializeWrapper();
    for (Map.Entry<Integer, Long> entry : this.offsetTable.entrySet()) {
        wrapper.getOffsetTable().put(entry.getKey(), entry.getValue());
    }
    String jsonString = wrapper.toJson(false);
    // 写入文件：~/store/config/delayOffset.json
    MixAll.string2File(jsonString, this.storePath);
}
```

**Broker 重启恢复：ScheduleMessageService 的 load / start / correctDelayOffset 全流程**

`ScheduleMessageService` 继承 `ConfigManager`，其持久化文件为 `delayOffset.json`。Broker 重启时的完整恢复链路如下：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/schedule/ScheduleMessageService.java
// 行 135-166, start()方法
public void start() {
    // 1. ★ 从 delayOffset.json 加载持久化的 offsetTable
    this.load();  
    
    // 2. 为每个延时级别创建 DeliverDelayedMessageTimerTask
    for (int i = 1; i <= this.maxDelayLevel; i++) {
        Long offset = this.offsetTable.get(i);
        if (offset == null) {
            this.offsetTable.put(i, 0L);
            offset = 0L;
        }
        
        if (isSyncDeliver) {
            // 同步投递模式
            this.deliverExecutorService.scheduleAtFixedRate(
                new DeliverDelayedMessageTimerTask(i, offset),
                FIRST_DELAY_TIME,  // 1000ms 初始延迟
                ...);
        } else {
            // 异步投递模式（默认）
            this.deliverExecutorService.scheduleAtFixedRate(
                new DeliverDelayedMessageTimerTask(i, offset),
                FIRST_DELAY_TIME,
                ...);
            // 额外启动 HandlePutResultTask 处理异步投递结果
            this.handleExecutorService.scheduleAtFixedRate(
                new HandlePutResultTask(i),
                FIRST_DELAY_TIME,
                ...);
        }
    }
    
    // 3. 启动定时持久化任务
    this.scheduledPersistService = new ScheduledExecutorService...
    this.scheduledPersistService.scheduleAtFixedRate(() -> {
        ScheduleMessageService.this.persist();
    }, 10000, this.flushDelayOffsetInterval, TimeUnit.MILLISECONDS);
}
```

**load()：三步恢复链路**

```java
// 行 222-227
public boolean load() {
    boolean result = super.load();          // ConfigManager: 读取 delayOffset.json
    result = result && this.parseDelayLevel();   // 解析 messageDelayLevel 配置
    result = result && this.correctDelayOffset(); // 修正偏移量到合法范围
    return result;
}
```

**ConfigManager.load()** 读取 `delayOffset.json` 文件内容（若不存在则尝试 `.bak` 文件），然后调用 `decode(jsonString)` 反序列化：

```java
// 行 277-290
public void decode(String jsonString) {
    if (jsonString != null) {
        DelayOffsetSerializeWrapper wrapper = 
            DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
        if (wrapper != null) {
            // ★ 恢复 level → consume offset 映射
            this.offsetTable.putAll(wrapper.getOffsetTable());
            if (wrapper.getDataVersion() != null) {
                this.dataVersion.assignNewOne(wrapper.getDataVersion());
            }
        }
    }
}
```

**correctDelayOffset()：修正偏移量到合法范围**

Broker 重启后，CommitLog 可能已被清理过期的 MappedFile，导致 ConsumeQueue 的 `[minOffset, maxOffset]` 范围变化。`correctDelayOffset` 将每个级别的 offset 钳制到实际范围内：

```java
// 行 235-269
private boolean correctDelayOffset() {
    for (int delayLevel : this.delayLevelTable.keySet()) {
        ConsumeQueueInterface cq = defaultMessageStore.getConsumeQueue(
            TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1);
        
        if (cq != null) {
            Long currOffset = this.offsetTable.get(delayLevel);
            if (currOffset < cq.getMinOffsetInQueue()) {
                // offset 小于最小值（消息已被清理），修正到最小值
                this.offsetTable.put(delayLevel, cq.getMinOffsetInQueue());
            } else if (currOffset > cq.getMaxOffsetInQueue()) {
                // offset 大于最大值（异常），修正到最大值
                this.offsetTable.put(delayLevel, cq.getMaxOffsetInQueue());
            }
        }
    }
    return true;
}
```

**shutdown / stop()：关闭时的 at-least-once 语义保证**

```java
// 行 173-198
public boolean stop() {
    if (this.started.compareAndSet(true, false) && null != this.deliverExecutorService) {
        // 1. 关闭线程池，等待最多5秒
        this.deliverExecutorService.shutdown();
        this.deliverExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS); // 5000ms
        
        if (this.handleExecutorService != null) {
            this.handleExecutorService.shutdown();
            this.handleExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
        }
        
        // 2. 记录未完成的异步投递任务数量（仅日志，不持久化）
        for (int i = 1; i <= this.deliverPendingTable.size(); i++) {
            log.warn("deliverPendingTable level: {}, size: {}", i, this.deliverPendingTable.get(i).size());
        }
        
        // 3. ★ 持久化 offsetTable 到 delayOffset.json
        this.persist();
    }
    return true;
}
```

**关键边界条件分析：**

投递失败处理：当 `DeliverDelayedMessageTimerTask.executeOnTimeUp` 中 `escapeBridge.asyncPutMessage` 投递失败时，会调用 `scheduleNextTimerTask(currOffset, DELAY_FOR_A_WHILE)`（延迟100ms后重试），不会更新 offset，因此消息不会丢失。

Broker 宕机重启：由于 offset 仅在投递成功后才通过 `updateOffset` 推进，重启后 `load()` 恢复的 offset 指向最后一条已确认投递的消息的下一个位置。未确认的消息会被重新投递，实现 at-least-once 语义。对于异步投递模式，已提交到 `escapeBridge.asyncPutMessage` 但 offset 未及时持久化的消息，重启后会被重复投递。

消息过期跳过：如果延时消息的投递时间远早于当前时间（例如 Broker 长时间宕机后恢复），`correctDeliverTimestamp` 方法会立即投递这些过期消息，而不是按原始延时等待。`deliverTimestamp = now + delayLevel` 的修正逻辑确保不会因为时钟偏差导致无限等待。

---

## 场景四：事务消息 —— 跨系统最终一致性转账

### 4.1 业务背景

银行 A 向银行 B 转账，需要同时更新 A 的扣款记录和 B 的入账记录。由于跨系统无法使用本地事务，使用 RocketMQ 事务消息保证：本地事务执行成功则消息一定被投递，本地事务失败则消息不被投递。

### 4.2 用户代码

```java
// 1. 创建事务消息生产者
TransactionMQProducer producer = new TransactionMQProducer("transfer_producer_group");
producer.setNamesrvAddr("127.0.0.1:9876");

// 2. 设置事务监听器（执行本地事务 + 事务回查）
producer.setTransactionListener(new TransactionListener() {
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        // 执行本地事务：扣减A账户余额
        String accountId = msg.getUserProperty("accountId");
        BigDecimal amount = new BigDecimal(msg.getUserProperty("amount"));
        try {
            accountService.debit(accountId, amount);
            return LocalTransactionState.COMMIT_MESSAGE;  // 本地事务成功 → 提交消息
        } catch (Exception e) {
            return LocalTransactionState.ROLLBACK_MESSAGE;  // 本地事务失败 → 回滚消息
        }
    }
    
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        // 事务回查：检查本地事务是否执行成功
        String accountId = msg.getUserProperty("accountId");
        String txnId = msg.getUserProperty("txnId");
        boolean debited = txnService.checkDebited(txnId);
        return debited ? LocalTransactionState.COMMIT_MESSAGE 
                       : LocalTransactionState.ROLLBACK_MESSAGE;
    }
});

producer.start();

// 3. 发送事务消息
Message msg = new Message("TransferTopic", "transfer",
    ("转账 txnId=" + txnId + " 金额=" + amount).getBytes());
msg.putUserProperty("accountId", accountId);
msg.putUserProperty("amount", amount.toString());
msg.putUserProperty("txnId", txnId);

TransactionSendResult result = producer.sendMessageInTransaction(msg, null);
```

### 4.3 源码全链路追踪

#### 4.3.1 Producer 端：半消息发送 + 本地事务 + 二次确认

```java
// DefaultMQProducerImpl.sendMessageInTransaction
public TransactionSendResult sendMessageInTransaction(final Message msg, Object arg) throws ... {
    
    // 步骤1：设置事务准备标记
    MessageListener listener = ...getTransactionListener();
    if (listener == null) throw new MQClientException("TransactionListener is null");
    
    msg.putProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
    msg.putProperty(MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
    
    // 步骤2：发送半消息（和普通消息走相同的发送链路）
    try {
        SendResult sendResult = this.send(msg);
        if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
            // 半消息发送失败，直接返回
            TransactionSendResult transactionSendResult = new TransactionSendResult();
            transactionSendResult.setSendStatus(sendResult.getSendStatus());
            return transactionSendResult;
        }
        
        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        
        // 步骤3：半消息发送成功，执行本地事务
        try {
            localTransactionState = listener.executeLocalTransaction(msg, arg);
        } catch (Throwable e) {
            localException = e;
        }
        
        if (localTransactionState == null) {
            localTransactionState = LocalTransactionState.UNKNOW;
        }
        
        // 步骤4：根据本地事务结果发送二次确认
        if (localTransactionState == LocalTransactionState.COMMIT_MESSAGE) {
            this.endTransaction(sendResult, localTransactionState, null);
        } else if (localTransactionState == LocalTransactionState.ROLLBACK_MESSAGE) {
            this.endTransaction(sendResult, localTransactionState, null);
        } else {
            // UNKNOW：不发送二次确认，等待Broker回查
        }
        
        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
        
    } catch (Exception e) {
        // 异常处理...
    }
}
```

**步骤4：`endTransaction` 发送二次确认**

```java
// DefaultMQProducerImpl.endTransaction
private void endTransaction(final SendResult sendResult, 
    final LocalTransactionState localTransactionState, final Throwable localException) throws ... {
    
    final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
    
    EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
    requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
    requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());  // 半消息在half topic的offset
    requestHeader.setCommitLogOffset(sendResult.getOffsetMsgId());       // 半消息在CommitLog的offset
    requestHeader.setFromTransactionCheck(false);
    
    // 映射本地状态到Broker操作类型
    switch (localTransactionState) {
        case COMMIT_MESSAGE:
            requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
            break;
        case ROLLBACK_MESSAGE:
            requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
            break;
        case UNKNOW:
            requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
            break;
    }
    
    // 发送END_TRANSACTION请求（oneway方式）
    this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(
        brokerAddr, requestHeader, "endTransaction", 5000);
}
```

#### 4.3.2 Broker 端：半消息存储

`SendMessageProcessor` 检测到 `TRANSACTION_PREPARED_TYPE` 标志后，委托给 `TransactionalMessageService`：

```java
// SendMessageProcessor.asyncSendMessage
if (msgInner.getSysFlag() == (MessageSysFlag.TRANSACTION_PREPARED_TYPE)) {
    // 事务消息
    putMessageResult = transactionalMessageService.asyncPrepareMessage(msgInner);
}
```

```java
// TransactionalMessageServiceImpl.asyncPrepareMessage
public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
    return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
}
```

```java
// TransactionalMessageBridge.asyncPutHalfMessage
public CompletableFuture<PutMessageResult> asyncPutHalfMessage(MessageExtBrokerInner messageInner) {
    // 关键：将消息转换为半消息
    MessageExtBrokerInner msgInner = parseHalfMessageInner(messageInner);
    // 存储到CommitLog
    return store.asyncPutMessage(msgInner);
}

public static MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
    // 保存原始Topic和QueueId到Properties
    MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
    MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msgInner.getQueueId()));
    
    // 清除事务标记
    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
    
    // 修改Topic为半消息Topic
    msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());  // RMQ_SYS_TRANS_HALF_TOPIC
    msgInner.setQueueId(0);
    
    return msgInner;
}
```

**知识点：半消息对消费者不可见**

半消息的 Topic 被改为 `RMQ_SYS_TRANS_HALF_TOPIC`，消费者不会订阅这个 Topic，因此半消息在事务未确认前对消费者完全不可见。

#### 4.3.3 Broker 端：二次确认处理（EndTransactionProcessor 真实源码）

`EndTransactionProcessor` 处理 `END_TRANSACTION` 请求。以下是真实源码的完整分支逻辑：

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/processor/EndTransactionProcessor.java
// 行 58-190
public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
    final EndTransactionRequestHeader requestHeader = ...;
    
    // SLAVE节点拒绝处理
    if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
        response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
        return response;
    }
    
    // fromTransactionCheck=true 表示这是事务回查的响应
    // fromTransactionCheck=false 表示这是Producer主动提交的二次确认
    if (requestHeader.getFromTransactionCheck()) {
        // 回查响应分支
        switch (requestHeader.getCommitOrRollback()) {
            case MessageSysFlag.TRANSACTION_NOT_TYPE:
                LOGGER.warn("check producer transaction state, but it's not commit or rollback");
                return null;  // Producer未决定，不处理
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                LOGGER.warn("check producer transaction state, the producer commit one message");
                break;  // 继续走COMMIT逻辑
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                LOGGER.warn("check producer transaction state, the producer rollback one message");
                break;  // 继续走ROLLBACK逻辑
            default:
                return null;
        }
    } else {
        // Producer主动提交分支
        switch (requestHeader.getCommitOrRollback()) {
            case MessageSysFlag.TRANSACTION_NOT_TYPE:
                LOGGER.warn("producer end transaction, but not commit/rollback");
                return null;
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                break;  // 继续走COMMIT逻辑
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                LOGGER.warn("producer end transaction rollback");
                break;  // 继续走ROLLBACK逻辑
            default:
                return null;
        }
    }
    
    // ★ 核心处理逻辑
    OperationResult result = new OperationResult();
    if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
        // ========== COMMIT 分支 ==========
        result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);
        if (result.getResponseCode() == ResponseCode.SUCCESS) {
            // 检查是否超时（超过checkImmunityTime的COMMIT被拒绝，改由回查处理）
            if (rejectCommitOrRollback(requestHeader, result.getPrepareMessage())) {
                response.setCode(ResponseCode.ILLEGAL_OPERATION);
                return response;
            }
            // 验证半消息完整性
            RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
            if (res.getCode() == ResponseCode.SUCCESS) {
                // ★ 恢复原始Topic和QueueId
                MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                // 重置事务类型为COMMIT
                msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(
                    msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
                msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED);
                
                // ★ 写入真实消息到CommitLog（投递到原始Topic）
                RemotingCommand sendResult = sendFinalMessage(msgInner);
                if (sendResult.getCode() == ResponseCode.SUCCESS) {
                    // ★ 投递成功后，删除半消息（写op记录）
                    deletePrepareMessage(result);
                }
                return sendResult;
            }
            return res;
        }
    } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
        // ========== ROLLBACK 分支 ==========
        result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
        if (result.getResponseCode() == ResponseCode.SUCCESS) {
            if (rejectCommitOrRollback(requestHeader, result.getPrepareMessage())) {
                response.setCode(ResponseCode.ILLEGAL_OPERATION);
                return response;
            }
            RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
            if (res.getCode() == ResponseCode.SUCCESS) {
                // ★ ROLLBACK只删除半消息，不投递（不调用sendFinalMessage）
                deletePrepareMessage(result);
            }
            return res;
        }
    }
    
    response.setCode(result.getResponseCode());
    response.setRemark(result.getResponseRemark());
    return response;
}
```

**COMMIT 和 ROLLBACK 的关键区别：**
- COMMIT：读取半消息 → `endMessageTransaction` 恢复原始Topic → `sendFinalMessage` 写入CommitLog → `deletePrepareMessage` 写op记录
- ROLLBACK：读取半消息 → `deletePrepareMessage` 写op记录（不恢复、不投递）

**commitMessage / rollbackMessage：从 CommitLog 读取半消息**

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/transaction/queue/TransactionalMessageServiceImpl.java
// 行 633-641, 583-594
public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
    return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
}
public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
    return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
}

private OperationResult getHalfMessageByOffset(long commitLogOffset) {
    OperationResult response = new OperationResult();
    // 通过物理偏移量直接从CommitLog读取半消息
    MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
    if (messageExt != null) {
        response.setPrepareMessage(messageExt);
        response.setResponseCode(ResponseCode.SUCCESS);
        response.setResponseRemark(null);
    } else {
        response.setResponseCode(ResponseCode.SYSTEM_ERROR);
        response.setResponseRemark("Find prepared transaction message failed");
    }
    return response;
}
```

**endMessageTransaction：恢复原始Topic和QueueId**

```java
// EndTransactionProcessor.java 行 266-296
private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
    MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
    // ★ 从properties中恢复原始Topic和QueueId
    msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
    msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
    msgInner.setBody(msgExt.getBody());
    msgInner.setFlag(msgExt.getFlag());
    msgInner.setBornTimestamp(msgExt.getBornTimestamp());
    msgInner.setBornHost(msgExt.getBornHost());
    msgInner.setStoreHost(msgExt.getStoreHost());
    msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
    msgInner.setWaitStoreMsgOK(false);
    msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
    msgInner.setSysFlag(msgExt.getSysFlag());
    
    TopicFilterType topicFilterType =
        (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG
            ? TopicFilterType.MULTI_TAG : TopicFilterType.SINGLE_TAG;
    long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
    msgInner.setTagsCode(tagsCodeValue);
    
    // 复制properties并清除REAL_TOPIC/REAL_QUEUE_ID
    MessageAccessor.setProperties(msgInner,
        MessageDecoder.string2messageProperties(
            MessageDecoder.messageProperties2String(msgExt.getProperties())));
    MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
    MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
    msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
    return msgInner;
}
```

**sendFinalMessage：写入真实消息到 CommitLog**

```java
// EndTransactionProcessor.java 行 298-371
private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
    final RemotingCommand response = RemotingCommand.createResponseCommand(null);
    // 调用store.putMessage写入CommitLog（走正常的asyncPutMessage流程）
    final PutMessageResult putMessageResult = 
        this.brokerController.getMessageStore().putMessage(msgInner);
    if (putMessageResult != null) {
        switch (putMessageResult.getPutMessageStatus()) {
            case PUT_OK:
                // 统计...
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                // ★ 注意：这三种状态也视为SUCCESS（消息已写入，只是刷盘/复制未完成）
                response.setCode(ResponseCode.SUCCESS);
                break;
            // 其他失败状态映射到对应错误码...
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                break;
        }
    }
    return response;
}
```

**deletePrepareMessage：批量写入 op 记录（不是物理删除）**

`deletePrepareMessage` 并非物理删除半消息，而是在 `RMQ_SYS_TRANS_OP_HALF_TOPIC` 写入一条标记消息（tag="d"，body=半消息的queueOffset）。后续事务回查服务通过检查 op topic 来判断哪些半消息已处理。

```java
// 源码路径: broker/src/main/java/org/apache/rocketmq/broker/transaction/queue/TransactionalMessageServiceImpl.java
// 行 596-631
public boolean deletePrepareMessage(MessageExt messageExt) {
    Integer queueId = messageExt.getQueueId();
    // 获取或创建该queueId对应的批量缓冲上下文
    MessageQueueOpContext mqContext = deleteContext.get(queueId);
    if (mqContext == null) {
        mqContext = new MessageQueueOpContext(System.currentTimeMillis(), 20000);
        MessageQueueOpContext old = deleteContext.putIfAbsent(queueId, mqContext);
        if (old != null) { mqContext = old; }
    }
    
    // 构建op数据：半消息的queueOffset + ","
    String data = messageExt.getQueueOffset() + TransactionalMessageUtil.OFFSET_SEPARATOR;
    try {
        // 放入批量缓冲队列（容量20000）
        boolean res = mqContext.getContextQueue().offer(data, 100, TimeUnit.MILLISECONDS);
        if (res) {
            int totalSize = mqContext.getTotalSize().addAndGet(data.length());
            // 如果缓冲数据超过最大大小，立即触发批量发送
            if (totalSize > transactionOpMsgMaxSize) {
                this.transactionalOpBatchService.wakeup();
            }
            return true;
        } else {
            // 队列满，触发批量发送
            this.transactionalOpBatchService.wakeup();
        }
    } catch (InterruptedException ignore) { }
    
    // fallback：同步写入单条op消息
    Message msg = getOpMessage(queueId, data);
    if (this.transactionalMessageBridge.writeOp(queueId, msg)) {
        log.warn("Force add remove op data. queueId={}", queueId);
        return true;
    }
    return false;
}
```

**op 消息格式与批量发送机制：**

```java
// TransactionalMessageUtil.java 行 32-39
public static final String REMOVE_TAG = "d";           // op消息tag
public static final Charset CHARSET = StandardCharsets.UTF_8;
public static final String OFFSET_SEPARATOR = ",";      // body中offset之间的分隔符
public static String buildOpTopic() {
    return TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC;  // op topic
}

// getOpMessage: 将多个offset合并为一条op消息
public Message getOpMessage(int queueId, String moreData) {
    StringBuilder sb = new StringBuilder();
    if (moreData != null) { sb.append(moreData); }
    // 从缓冲队列中取出所有待发送的offset
    while (!mqContext.getContextQueue().isEmpty()) {
        if (sb.length() >= maxSize) { break; }
        String data = mqContext.getContextQueue().poll();
        if (data != null) { sb.append(data); }
    }
    if (sb.length() == 0) { return null; }
    // op消息: topic=RMQ_SYS_TRANS_OP_HALF_TOPIC, tag="d", body="offset1,offset2,offset3,"
    return new Message(opTopic, TransactionalMessageUtil.REMOVE_TAG,
            sb.toString().getBytes(TransactionalMessageUtil.CHARSET));
}
```

`TransactionalOpBatchService` 是一个定时线程，每 `transactionOpBatchInterval` 毫秒（默认1000ms）或缓冲区满时触发一次批量发送，将积攒的 op offset 合并为少量消息写入 `RMQ_SYS_TRANS_OP_HALF_TOPIC`，与 half topic 的 queueId 一一对应。这种批量设计大幅减少了 op 消息的数量和 I/O 开销。

#### 4.3.4 Broker 端：事务回查服务

`TransactionalMessageCheckService` 每60秒执行一次回查：

```java
// TransactionalMessageServiceImpl.check
public void check(long transactionTimeout, int transactionCheckMax,
    AbstractTransactionalMessageCheckListener listener) {
    
    // 1. 获取half topic的ConsumeQueue
    ConsumeQueueInterface cq = defaultMessageStore.getConsumeQueue(
        TransactionalMessageUtil.buildHalfTopic(), 0);
    
    // 2. 获取op topic的ConsumeQueue（记录已处理的半消息offset）
    ConsumeQueueInterface opCq = defaultMessageStore.getConsumeQueue(
        TransactionalMessageUtil.buildOpHalfTopic(), 0);
    
    // 3. 从op CQ构建已处理offset集合
    long currentOpOffset = 0;
    HashMap<Long, Long> removeMap = new HashMap<>();
    fillOpRemoveMap(removeMap, opCq, 0, currentOpOffset, doneNum);
    
    // 4. 遍历half CQ，对每个未处理的半消息进行回查
    while (bufferCQ.hasNext()) {
        long offsetPy = cqUnit.getPos();
        int sizePy = cqUnit.getSize();
        long msgOffset = cqUnit.getQueueOffset();  // 半消息的queueOffset
        
        // 检查是否已在op中标记（已提交或已回滚）
        if (removeMap.containsKey(msgOffset)) {
            // 已处理，跳过
            continue;
        }
        
        // 从CommitLog读取半消息
        MessageExt msgExt = defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
        
        // 检查回查次数
        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
            // 超过最大回查次数（默认15次），丢弃
            continue;
        }
        
        // 检查是否到了回查时间
        long checkImmunityTime = ...;  // 默认6秒
        if (checkImmunityTime > (now - msgExt.getStoreTimestamp())) {
            // 还未到回查时间
            scheduleNextCheckTask();
            return;
        }
        
        // 5. 重新写入半消息（增加回查次数）
        msgInner = renewHalfMessageInner(msgExt);
        // PROPERTY_TRANSACTION_CHECK_TIMES + 1
        
        // 6. 触发回查
        listener.resolveHalfMsg(msgExt);
    }
}
```

**`listener.resolveHalfMsg`** 最终向 Producer 发送 `CHECK_TRANSACTION_STATE` 请求：

```java
// AbstractTransactionalMessageCheckListener.resolveHalfMsg
public void resolveHalfMsg(final MessageExt msgExt) {
    executorService.execute(() -> {
        // 向Producer发送回查请求
        // Producer收到后调用 checkLocalTransaction 方法
        // 返回 COMMIT/ROLLBACK/UNKNOW
    });
}
```

### 4.4 事务消息全流程图

```
Producer                    Broker                          Consumer(B银行)
   │                          │                                  │
   │── 半消息(PREPARED) ──────>│                                  │
   │   topic=RMQ_SYS_TRANS_   │                                  │
   │   HALF_TOPIC             │── 存储半消息到CommitLog           │
   │<── SEND_OK ──────────────│                                  │
   │                          │                                  │
   │── 执行本地事务            │                                  │
   │   (扣减A账户余额)         │                                  │
   │                          │                                  │
   │── END_TRANSACTION ──────>│                                  │
   │   (COMMIT_MESSAGE)       │                                  │
   │                          │── 从CommitLog读取半消息           │
   │                          │── 恢复原始Topic=TransferTopic     │
   │                          │── 写入CommitLog(投递到原始Topic)  │
   │                          │── 写op标记(标记半消息已处理)       │
   │<── SUCCESS ──────────────│                                  │
   │                          │                                  │
   │                          │── ReputMessageService分发         │
   │                          │   到TransferTopic的ConsumeQueue   │
   │                          │                                  │
   │                          │<── PULL_MESSAGE ────────────────│
   │                          │── 返回消息 ──────────────────────>│
   │                          │                                  │── B银行入账
```

### 4.5 知识点：事务消息的一致性保证

1. **半消息存储成功后，即使Producer崩溃，Broker的回查服务也会定期检查未确认的事务**
2. **回查次数有限**（默认15次），超过后消息被丢弃，需要人工干预
3. **回查间隔递增**：第一次回查在存储后6秒，之后每60秒一次
4. **COMMIT/ROLLBACK 操作是幂等的**：Producer可能重复发送，Broker通过op topic去重

---

## 场景五：批量消息 —— 日志批量上报

### 5.1 业务背景

微服务系统每秒产生数千条操作日志，如果逐条发送到RocketMQ，网络开销巨大。批量消息允许将多条消息合并为一次网络请求发送，大幅提升吞吐量。

### 5.2 用户代码

```java
DefaultMQProducer producer = new DefaultMQProducer("log_producer_group");
producer.start();

// 收集日志
List<Message> logs = new ArrayList<>();
for (LogEntry entry : logBuffer) {
    Message msg = new Message("OperationLogTopic", "log",
        entry.toJson().getBytes());
    logs.add(msg);
}

// 批量发送
SendResult result = producer.send(logs);
```

### 5.3 源码全链路追踪

#### 5.3.1 消息合并：MessageBatch

```java
// DefaultMQProducer.send(Collection<Message> msgs)
public SendResult send(Collection<Message> msgs, long timeout) throws ... {
    return this.defaultMQProducerImpl.send(batch(msgs), timeout);
}

private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
    MessageBatch msgBatch = MessageBatch.generateFromList(msgs);
    // 为批次中每条消息设置唯一ID
    MessageClientIDSetter.setUniqID(msgBatch);
    msgBatch.setBody(msgBatch.encode());
    return msgBatch;
}
```

**`MessageBatch.generateFromList` 校验约束**：

```java
// MessageBatch.java
public static MessageBatch generateFromList(Collection<Message> messages) {
    assert messages != null;
    assert !messages.isEmpty();
    
    MessageBatch msgBatch = new MessageBatch();
    
    // 约束1：转换为List
    if (messages instanceof List) {
        msgBatch.messages = (List<Message>) messages;
    } else {
        msgBatch.messages = new ArrayList<>(messages);
    }
    
    // 约束2：不能包含延时消息
    for (Message message : msgBatch.messages) {
        if (message.getDelayTimeLevel() > 0) {
            throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
        }
    }
    
    // 约束3：不能包含重试Topic
    for (Message message : msgBatch.messages) {
        if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            throw new UnsupportedOperationException("Retry group is not supported for batching");
        }
    }
    
    // 约束4：所有消息必须是同一个Topic
    for (Message message : msgBatch.messages) {
        // 检查topic一致性
    }
    
    // 约束5：所有消息的waitStoreMsgOK必须一致
    // ...
    
    return msgBatch;
}
```

#### 5.3.2 消息编码：MessageDecoder

```java
// MessageBatch.encode()
public byte[] encode() {
    return MessageDecoder.encodeMessages(messages);
}

// MessageDecoder.encodeMessages
public static byte[] encodeMessages(List<Message> messages) {
    // 1. 先逐条编码
    List<byte[]> encodedMessages = new ArrayList<>(messages.size());
    int allSize = 0;
    for (Message message : messages) {
        byte[] tmp = encodeMessage(message);
        encodedMessages.add(tmp);
        allSize += tmp.length;
    }
    
    // 2. 拼接成一个字节数组
    byte[] allBytes = new byte[allSize];
    int pos = 0;
    for (byte[] bytes : encodedMessages) {
        System.arraycopy(bytes, 0, allBytes, pos, bytes.length);
        pos += bytes.length;
    }
    return allBytes;
}

// MessageDecoder.encodeMessage (单条消息编码)
public static byte[] encodeMessage(Message message) {
    // 格式：TOTALSIZE(4) + MAGICCODE(4) + BODYCRC(4) + FLAG(4) + BODY(4+len) + PROPERTIES(2+len)
    byte[] body = message.getBody();
    byte[] properties = messageProperties2String(message.getProperties()).getBytes();
    
    int storeSize = 4 + 4 + 4 + 4 + 4 + body.length + 2 + properties.length;
    
    ByteBuffer byteBuffer = ByteBuffer.allocate(storeSize);
    byteBuffer.putInt(storeSize);                        // TOTALSIZE
    byteBuffer.putInt(MESSAGE_MAGIC_CODE);               // MAGICCODE
    byteBuffer.putInt(0);                                // BODYCRC (暂不计算)
    byteBuffer.putInt(message.getFlag());                // FLAG
    byteBuffer.putInt(body.length);                      // BODY length
    byteBuffer.put(body);                                // BODY
    byteBuffer.putShort((short) properties.length);      // PROPERTIES length
    byteBuffer.put(properties);                          // PROPERTIES
    
    return byteBuffer.array();
}
```

**知识点：批量消息的编码格式**

批量消息将多条消息编码为一个连续的字节数组，每条消息内部有 `TOTALSIZE` 字段标识自身长度。Broker 端接收后通过 `MessageExtBatch` 逐条解析，但只写入一次 CommitLog（作为一条逻辑消息）。

#### 5.3.3 Broker 端处理

```java
// CommitLog.asyncPutMessages (批量消息专用方法)
public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
    // ... 初始化 ...
    
    // 校验：批量消息不支持事务
    if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
        return CompletableFuture.completedFuture(
            new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, ...));
    }
    
    // 校验：批量消息不支持延时
    if (messageExtBatch.getDelayTimeLevel() > 0) {
        return CompletableFuture.completedFuture(
            new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, ...));
    }
    
    // 获取或创建MappedFile
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
    
    // 写入（批量写入，一次加锁）
    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
    // appendMessageCallback会逐条解析批量消息体中的每条消息
    // 为每条消息分配queueOffset并写入
    
    // ... 刷盘和HA ...
}
```

### 5.4 批量消息 vs 逐条消息性能对比

```
逐条发送1000条消息:
  - 1000次网络往返
  - 1000次CommitLog写入
  - 1000次锁获取/释放
  - 总耗时: ~10秒

批量发送1000条消息:
  - 1次网络往返
  - 1次CommitLog写入(批量)
  - 1次锁获取/释放
  - 总耗时: ~100毫秒
```

### 5.5 知识点：批量消息的限制

1. **同一个Topic**：批量消息中所有消息必须是同一个Topic
2. **不支持延时**：延时消息需要改写Topic，与批量消息冲突
3. **不支持事务**：事务消息需要两阶段提交，与批量消息冲突
4. **总大小限制**：批量消息总大小不能超过 `maxMessageSize`（默认4MB）
5. **消费者感知**：消费者收到的每条消息都是独立的 `MessageExt`，但它们的 `queueOffset` 是连续的

---

## 场景六：消息重试与死信队列 —— 消费失败容错

### 6.1 业务背景

消费者处理消息时可能因为数据库异常、第三方服务不可用等原因导致消费失败。RocketMQ 提供自动重试机制，失败的消息会延迟后重新投递。超过最大重试次数后，消息进入死信队列（DLQ），供人工处理。

### 6.2 用户代码

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("payment_consumer_group");
consumer.subscribe("PaymentTopic", "*");

// 设置最大重试次数（默认16次）
consumer.setMaxReconsumeTimes(5);

consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
    try {
        for (MessageExt msg : msgs) {
            processPayment(msg);
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    } catch (Exception e) {
        // 消费失败，返回RECONSUME_LATER触发重试
        context.setAckIndex(0);  // 只重试失败的消息
        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
    }
});
consumer.start();
```

### 6.3 源码全链路追踪

#### 6.3.1 消费失败后的处理：processConsumeResult

```java
// ConsumeMessageConcurrentlyService.processConsumeResult
public void processConsumeResult(final ConsumeConcurrentlyStatus status,
    final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {
    
    int ackIndex = context.getAckIndex();
    
    if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
        ackIndex = consumeRequest.getMsgs().size();  // 全部ACK
    } else if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
        ackIndex = -1;  // 全部需要重试
    }
    
    switch (this.defaultMQPushConsumer.getMessageModel()) {
        case BROADCASTING:
            // 广播模式：直接丢弃失败消息
            for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                log.warn("BROADCASTING, message consume failed, drop it");
            }
            break;
            
        case CLUSTERING:
            // 集群模式：将失败消息发回Broker
            List<MessageExt> msgBackFailed = new ArrayList<>();
            for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                MessageExt msg = consumeRequest.getMsgs().get(i);
                
                // 发送回Broker
                boolean result = this.sendMessageBack(msg, context);
                if (!result) {
                    // 发回失败，本地重试
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    msgBackFailed.add(msg);
                }
            }
            
            if (!msgBackFailed.isEmpty()) {
                // 本地延迟5秒后重新提交消费
                consumeRequest.getMsgs().clear();
                consumeRequest.getMsgs().addAll(msgBackFailed);
                this.submitConsumeRequestLater(
                    msgBackFailed, consumeRequest.getProcessQueue(),
                    consumeRequest.getMessageQueue(), 5000);
            }
            break;
    }
    
    // 移除已处理的消息并更新offset
    long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
    if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
        this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(
            consumeRequest.getMessageQueue(), offset, true);
    }
}
```

#### 6.3.2 sendMessageBack：发回 Broker

```java
// DefaultMQPushConsumerImpl.sendMessageBack
public void sendMessageBack(MessageExt msg, ConsumeConcurrentlyContext context) {
    try {
        // 发送CONSUMER_SEND_MSG_BACK请求到Broker
        this.consumerSendMessageBack(msg, context.getDelayLevelWhenNextConsume());
    } catch (Exception e) {
        // 发回失败，作为普通消息发送到%RETRY%group
        this.sendMessageBackAsNormalMessage(msg);
    }
}

private void sendMessageBackAsNormalMessage(MessageExt msg) {
    // 构建重试消息
    Message newMsg = new Message(
        MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()),  // %RETRY%group
        msg.getBody());
    
    // 保存原始Topic
    MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
    
    // 设置重试次数
    MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
    MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
    
    // 设置递增的延时级别：3 + reconsumeTimes
    // 第1次重试：level 3 = 10秒
    // 第2次重试：level 4 = 30秒
    // 第3次重试：level 5 = 1分钟
    // ...逐步递增
    newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
    
    this.mQClientFactory.getDefaultMQProducer().send(newMsg);
}
```

**知识点：递增延时级别**

重试消息使用递增的延时级别（`3 + reconsumeTimes`），这意味着：
- 第1次重试：10秒后
- 第2次重试：30秒后
- 第3次重试：1分钟后
- ...
- 第14次重试：2小时后

递增延时的设计既给了系统恢复的时间，又避免了频繁重试导致的雪崩。

**★ 重试消息与延时消息的交叉关系：共享 SCHEDULE_TOPIC 基础设施**

这是一个容易被忽略但至关重要的设计：重试消息本质上就是延时消息的一种特殊应用。当 `sendMessageBackAsNormalMessage` 构建重试消息时，设置了 `newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes())`，这使得重试消息在到达 Broker 后走与普通延时消息完全相同的路径。

```
重试消息的完整流转路径：

1. Consumer 消费失败 → sendMessageBackAsNormalMessage
2. 构建消息: topic=%RETRY%group, delayTimeLevel=3+reconsumeTimes
3. 发送到 Broker → SendMessageProcessor
4. ★ Broker 检测到 delayTimeLevel > 0，走延时消息路径:
   - topic 改为 SCHEDULE_TOPIC_XXXX
   - queueId = delayLevel - 1
   - tagsCode = storeTime + delayMs
   - 写入 CommitLog
5. ★ ScheduleMessageService 的 DeliverDelayedMessageTimerTask 扫描到该消息
   - 计算投递时间
   - 到期后调用 messageTimeUp 恢复原始 topic (%RETRY%group)
   - 重新写入 CommitLog
6. Consumer 从 %RETRY%group 拉取并消费
```

关键交叉点在于步骤4-5：重试消息和用户主动发送的延时消息共用同一套 `SCHEDULE_TOPIC_XXXX` → `ScheduleMessageService` → `DeliverDelayedMessageTimerTask` → `messageTimeUp` 的基础设施。区别仅在于：

延时消息的 `messageTimeUp` 恢复的是用户原始 Topic（如 `OrderTimeoutTopic`），而重试消息的 `messageTimeUp` 恢复的是 `%RETRY%group`。两者的 `PROPERTY_REAL_TOPIC` 不同，但走的是完全相同的代码路径。

这意味着延时级别 `3 + reconsumeTimes` 必须在 `messageDelayLevel` 定义的18个级别范围内。默认配置 `"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"` 中：
- level 3 = 10秒（第1次重试）
- level 4 = 30秒（第2次重试）
- level 5 = 1分钟（第3次重试）
- ...
- level 16 = 2小时（第14次重试，最后一次）

如果 `reconsumeTimes` 超过13（即 delayLevel > 16），由于 maxDelayLevel=18，消息仍能被正确投递，但延时不再递增。

#### 6.3.3 Broker 端：重试 vs 死信队列决策

```java
// SendMessageProcessor.handleRetryAndDLQ
private RemotingCommand handleRetryAndDLQ(SendMessageRequestHeader requestHeader,
    RemotingCommand response, SendMessageContext sendMessageContext,
    MessageExt msg, TopicConfig topicConfig) {
    
    String newTopic = requestHeader.getTopic();
    if (newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
        // 这是一个重试消息
        String groupName = KeyBuilder.parseGroup(newTopic);
        SubscriptionGroupConfig subscriptionGroupConfig = 
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);
        
        // 获取最大重试次数
        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();  // 默认16
        if (requestHeader.getMaxReconsumeTimes() != null) {
            maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();  // 客户端覆盖
        }
        
        int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 
            : requestHeader.getReconsumeTimes();
        
        if (reconsumeTimes > maxReconsumeTimes) {
            // ★ 超过最大重试次数 → 进入死信队列
            newTopic = MixAll.getDLQTopic(groupName);  // %DLQ%group
            int queueIdInt = randomQueueId(DLQ_NUMS_PER_GROUP);  // 随机选择队列
            topicConfig = this.brokerController.getTopicConfigManager()
                .createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP,
                    PermName.PERM_WRITE | PermName.PERM_READ, 0);
            msg.setTopic(newTopic);
            msg.setQueueId(queueIdInt);
            msg.setDelayTimeLevel(0);  // 死信消息不再延迟
        }
        // 否则继续写入%RETRY%group（带延时级别）
    }
    
    return null;  // 继续正常写入流程
}
```

### 6.4 重试与死信队列全流程

```
Consumer                    Broker
   │                          │
   │── 消费消息(第1次) ──────>│ (原始Topic)
   │<── 返回RECONSUME_LATER   │
   │                          │
   │── CONSUMER_SEND_MSG_BACK>│
   │   reconsumeTimes=0       │
   │                          │── reconsumeTimes(0) <= max(5)?
   │                          │   YES → 写入%RETRY%group
   │                          │        delayLevel=3(10秒)
   │                          │
   │── [10秒后] 消费消息(第2次)│ (从%RETRY%group拉取)
   │<── 返回RECONSUME_LATER   │
   │                          │
   │── CONSUMER_SEND_MSG_BACK>│
   │   reconsumeTimes=1       │
   │                          │── delayLevel=4(30秒)
   │                          │
   │ ... 重复5次 ...          │
   │                          │
   │── CONSUMER_SEND_MSG_BACK>│
   │   reconsumeTimes=5       │
   │                          │── reconsumeTimes(5) > max(5)?
   │                          │   YES → 写入%DLQ%group ★
   │                          │        delayLevel=0(立即)
   │                          │        → 死信队列，等待人工处理
```

### 6.5 知识点：重试消息的 Topic 切换

```java
// DefaultMQPushConsumerImpl.resetRetryAndNamespace
// 在消息交给Listener之前，将%RETRY%group改回原始Topic
private void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
    final String groupTopic = MixAll.getRetryTopic(consumerGroup);
    for (MessageExt msg : msgs) {
        if (groupTopic.equals(msg.getTopic())) {
            // 从Properties中恢复原始Topic
            String retryTopic = msg.getUserProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null) {
                msg.setTopic(retryTopic);
            }
        }
    }
}
```

这意味着消费者的 `MessageListener` 看到的始终是原始 Topic，不需要感知重试机制的存在。重试消息的 `%RETRY%group` Topic 对消费者透明。

---

## 场景七：消息轨迹追踪 —— 全链路追踪

### 7.1 业务背景

生产环境中消息丢失或消费异常时，需要追踪消息从发送到消费的完整链路。RocketMQ 的消息轨迹功能记录每条消息的 Pub（发送）、SubBefore（消费前）、SubAfter（消费后）三个阶段的详细信息。

### 7.2 用户代码

```java
// Producer 端开启轨迹
DefaultMQProducer producer = new DefaultMQProducer("trace_producer_group", true);  // true=开启轨迹
producer.start();

// Consumer 端开启轨迹
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("trace_consumer_group", true);  // true=开启轨迹
consumer.subscribe("TraceTestTopic", "*");
consumer.registerMessageListener(...);
consumer.start();
```

### 7.3 源码全链路追踪

#### 7.3.1 轨迹数据模型

```java
// TraceContext.java
public class TraceContext implements Comparable<TraceContext> {
    private TraceType traceType;        // PUB, SUB_BEFORE, SUB_AFTER, ENDTRANSACTION
    private long timeStamp;             // 时间戳
    private String regionId;            // 区域ID
    private String groupName;           // 生产者/消费者组名
    private long costTime;              // 耗时(ms)
    private boolean isSuccess;          // 是否成功
    private String requestId;           // 请求ID
    private List<TraceBean> traceBeans; // 消息详情列表
}

// TraceBean.java
public class TraceBean {
    private String topic;               // 消息Topic
    private String msgId;               // 消息唯一ID
    private String offsetMsgId;         // 物理偏移消息ID
    private String tags;                // 消息Tag
    private String keys;                // 消息Keys
    private String storeHost;           // 存储Broker地址
    private String clientHost;          // 客户端地址
    private long storeTime;             // 存储时间
    private int retryTimes;             // 重试次数
    private int bodyLength;             // 消息体长度
    private MessageType msgType;        // 消息类型
}
```

#### 7.3.2 异步轨迹收集：AsyncTraceDispatcher

```java
// AsyncTraceDispatcher.java
// 1. Hook回调：消息发送成功后触发
public void append(TraceContext context) {
    // 将TraceContext放入有界队列（容量2048）
    boolean offer = traceContextQueue.offer(context);
    if (!offer) {
        discardCount.incrementAndGet();  // 队列满时丢弃
    }
}

// 2. 后台线程：批量收集
class AsyncRunnable implements Runnable {
    @Override
    public void run() {
        while (!isStopped()) {
            try {
                flushTraceContext();
                Thread.sleep(flushTraceInterval);  // 5秒或队列满时触发
            } catch (Exception e) { ... }
        }
    }
}

// 3. 批量刷新
private void flushTraceContext() {
    // 每次最多收集batchNum(20)个TraceContext
    List<TraceContext> contexts = new ArrayList<>(batchNum);
    while (contexts.size() < batchNum) {
        TraceContext ctx = traceContextQueue.poll(100, TimeUnit.MILLISECONDS);
        if (ctx != null) {
            contexts.add(ctx);
        } else break;
    }
    
    if (!contexts.isEmpty()) {
        // 提交异步发送任务
        AsyncDataSendTask task = new AsyncDataSendTask(contexts);
        traceExecutorService.submit(task);
    }
}

// 4. 异步发送到Trace Topic
class AsyncDataSendTask {
    public void sendTraceData() {
        // 按topic分组
        Map<String, List<TraceContext>> groupedContexts = ...;
        
        for (Map.Entry<String, List<TraceContext>> entry : groupedContexts.entrySet()) {
            // ★ 编码为轨迹数据字符串（见下方 encoderFromContextBean 详解）
            TraceTransferBean transferBean = TraceDataEncoder.encoderFromContextBean(context);
            StringBuilder data = new StringBuilder();
            data.append(transferBean.getTransData());
            
            // 构建轨迹消息
            Message traceMsg = new Message(traceTopic, data.toString().getBytes());
            // 设置keys为原始消息ID（便于通过消息ID查询轨迹）
            traceMsg.setKeys(keySet);
            
            // 发送（使用专用的非轨迹Producer，避免递归）
            traceProducer.send(traceMsg);
        }
    }
}
```

**知识点：轨迹 Producer 是独立的**

```java
// AsyncTraceDispatcher.getAndCreateTraceProducer
private DefaultMQProducer getAndCreateTraceProducer() {
    DefaultMQProducer traceProducer = new DefaultMQProducer(
        traceGroupName,  // "_INNER_TRACE_PRODUCER_group_type_N"
        false);          // ★ false = 不开启轨迹（避免递归追踪）
    traceProducer.setSendMsgTimeout(5000);
    // ...
    return traceProducer;
}
```

轨迹 Producer 自身关闭了轨迹功能，否则会形成无限递归（轨迹消息的发送轨迹又产生轨迹消息...）。

**TraceDataEncoder.encoderFromContextBean 逐字段展开**

`encoderFromContextBean` 是轨迹数据序列化的核心方法。它根据 `TraceType` 将 `TraceContext` 编码为特定格式的字符串，字段之间用 `TraceConstants.CONTENT_SPLITOR`（`\u0001`）分隔，记录之间用 `TraceConstants.FIELD_SPLITOR`（`\u0002`）终止。

```java
// 源码路径: client/src/main/java/org/apache/rocketmq/client/trace/TraceDataEncoder.java
// 行 159-256
public static TraceTransferBean encoderFromContextBean(TraceContext ctx) {
    TraceTransferBean transferBean = new TraceTransferBean();
    StringBuilder sb = new StringBuilder(256);
    
    switch (ctx.getTraceType()) {
        case Pub:
            // ★ Pub记录格式（14个字段）:
            // TraceType | timeStamp | regionId | groupName | topic | msgId | tags | keys
            // | storeHost | bodyLength | costTime | msgType.ordinal() | offsetMsgId | isSuccess
            sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTopic()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getMsgId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getTags()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getKeys()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getStoreHost()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getBodyLength()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getCostTime()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getMsgType().ordinal()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getOffsetMsgId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.isSuccess()).append(TraceConstants.FIELD_SPLITOR);
            break;
            
        case SubBefore:
            // ★ SubBefore记录格式（每个TraceBean一条，8个字段）:
            // TraceType | timeStamp | regionId | groupName | requestId | msgId | retryTimes | keys
            for (TraceBean bean : ctx.getTraceBeans()) {
                sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.getRequestId()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)
                  .append((short) bean.getRetryTimes()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(bean.getKeys()).append(TraceConstants.FIELD_SPLITOR);
            }
            break;
            
        case SubAfter:
            // ★ SubAfter记录格式（每个TraceBean一条，6+2个字段）:
            // TraceType | requestId | msgId | costTime | isSuccess | keys | contextCode
            // [ | timeStamp | groupName ]  ← 仅非CLOUD通道才有后两个字段
            for (TraceBean bean : ctx.getTraceBeans()) {
                sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.getRequestId()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.getCostTime()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.isSuccess()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(bean.getKeys()).append(TraceConstants.CONTENT_SPLITOR)
                  .append(ctx.getContextCode());
                
                if (ctx.getAccessChannel() != AccessChannel.CLOUD) {
                    // 非CLOUD通道追加timeStamp和groupName
                    sb.append(TraceConstants.CONTENT_SPLITOR)
                      .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)
                      .append(ctx.getGroupName());
                }
                sb.append(TraceConstants.FIELD_SPLITOR);
            }
            break;
            
        case EndTransaction:
            // ★ EndTransaction记录格式（13个字段）:
            // TraceType | timeStamp | regionId | groupName | topic | msgId | tags | keys
            // | storeHost | msgType.ordinal() | transactionId | transactionState.name() | isFromTransactionCheck
            sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTopic()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getMsgId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getTags()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getKeys()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getStoreHost()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getMsgType().ordinal()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getTransactionId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getTransactionState().name()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).isFromTransactionCheck()).append(TraceConstants.FIELD_SPLITOR);
            break;
            
        case Recall:
            // ★ Recall记录格式（7个字段）:
            // TraceType | timeStamp | regionId | groupName | topic | msgId | isSuccess
            sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTopic()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.getTraceBeans().get(0).getMsgId()).append(TraceConstants.CONTENT_SPLITOR)
              .append(ctx.isSuccess()).append(TraceConstants.FIELD_SPLITOR);
            break;
            
        default:
            break;
    }
    
    transferBean.setTransData(sb.toString());
    
    // ★ 构建transKey：用于索引（msgId + 业务keys）
    for (TraceBean bean : ctx.getTraceBeans()) {
        transferBean.getTransKey().add(bean.getMsgId());
        if (bean.getKeys() != null && bean.getKeys().length() > 0) {
            String[] keys = bean.getKeys().split(MessageConst.KEY_SEPARATOR);
            transferBean.getTransKey().addAll(Arrays.asList(keys));
        }
    }
    
    return transferBean;
}
```

**五种轨迹类型的编码格式总结：**

```
类型            字段数   记录分隔                说明
──────────────────────────────────────────────────────────────────────────
Pub             14      FIELD_SPLITOR           消息发送轨迹，含costTime和offsetMsgId
SubBefore       8       每个bean一条            消费前轨迹，含requestId和retryTimes
SubAfter        6或8    每个bean一条            消费后轨迹，非CLOUD追加timeStamp+groupName
EndTransaction  13      FIELD_SPLITOR           事务消息轨迹，含transactionState和isFromTransactionCheck
Recall          7       FIELD_SPLITOR           消息撤回轨迹
```

`transKey` 集合包含原始消息的 `msgId` 和用户设置的 `keys`，用作轨迹消息的索引键，便于通过消息 ID 或业务 Key 反查轨迹。

#### 7.3.3 三阶段轨迹采集

**阶段1：Pub（发送）**

```java
// 在SendMessageProcessor中，通过SendMessageHook触发
public void executeSendMessageHookBefore(ChannelHandlerContext ctx, RemotingCommand request,
    SendMessageContext context) {
    if (hasSendMessageHook()) {
        for (SendMessageHook hook : sendMessageHookList) {
            hook.sendMessageBefore(context);
        }
    }
}

// 发送完成后
public void executeSendMessageHookAfter(SendMessageContext context) {
    if (hasSendMessageHook()) {
        for (SendMessageHook hook : sendMessageHookList) {
            hook.sendMessageAfter(context);
        }
    }
}
```

**阶段2：SubBefore（消费前）**

```java
// ConsumeMessageConcurrentlyService.ConsumeRequest.run()
// 在调用MessageListener之前
if (hasConsumeMessageHook()) {
    ConsumeMessageContext context = new ConsumeMessageContext();
    context.setTraceType(TraceType.SubBefore);
    // ... 记录消费前信息 ...
    executeConsumeMessageHookBefore(context);
}
```

**阶段3：SubAfter（消费后）**

```java
// 在调用MessageListener之后
if (hasConsumeMessageHook()) {
    context.setTraceType(TraceType.SubAfter);
    context.setSuccess(success);
    context.setCostTime(costTime);
    executeConsumeMessageHookAfter(context);
}
```

### 7.4 轨迹数据格式

```
Pub,Timestamp,RegionId,GroupName,Topic,MsgId,OffsetMsgId,Tags,Keys,StoreHost,ClientHost,StoreTime,BodyLength
SubBefore,Timestamp,RegionId,GroupName,Topic,MsgId,OffsetMsgId,Tags,Keys,StoreHost,ClientHost,RetryTimes
SubAfter,Timestamp,RegionId,GroupName,Topic,MsgId,Tags,Keys,Success,CostTime
```

一条消息的完整轨迹包含3条Trace记录，通过 `MsgId` 关联。用户可以通过 `mqadmin queryTraceById -i msgId` 查询完整轨迹。

---

## 场景八：消费者负载均衡与扩缩容 —— 消费者动态伸缩

### 8.1 业务背景

大促期间消息量激增，运维临时扩容消费者实例从4台到8台。扩容后，新的消费者需要自动接管部分消息队列，实现负载的自动再均衡。缩容时同理，下线的消费者的队列需要被其他消费者接管。

### 8.2 源码全链路追踪

#### 8.2.1 RebalanceService：定时触发

```java
// RebalanceService.run()
public void run() {
    long realWaitInterval = waitInterval;  // 默认20秒
    while (!this.isStopped()) {
        this.waitForRunning(realWaitInterval);
        long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
        if (interval < minInterval) {
            // 最小间隔保护（默认1秒）
            realWaitInterval = minInterval - interval;
        } else {
            // 执行Rebalance
            boolean balanced = this.mqClientFactory.doRebalance();
            // 如果不均衡，缩短下次检查间隔
            realWaitInterval = balanced ? waitInterval : minInterval;
            lastRebalanceTimestamp = System.currentTimeMillis();
        }
    }
}
```

**知识点：自适应间隔**

Rebalance 默认每20秒执行一次。但如果发现不均衡（如刚加入新消费者），间隔会缩短到1秒，加速收敛。一旦均衡，恢复到20秒。

#### 8.2.2 doRebalance：遍历所有订阅Topic

```java
// MQClientInstance.doRebalance
public boolean doRebalance() {
    boolean balanced = true;
    for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
        MQConsumerInner impl = entry.getValue();
        if (impl != null) {
            try {
                if (!impl.tryRebalance()) {
                    balanced = false;
                }
            } catch (Throwable e) { ... }
        }
    }
    return balanced;
}
```

#### 8.2.3 rebalanceByTopic：队列分配核心

```java
// RebalanceImpl.rebalanceByTopic
private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
    boolean balanced = true;
    
    switch (messageModel) {
        case CLUSTERING: {
            // 1. 获取Topic的所有MessageQueue
            Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
            
            // 2. 获取消费者组中所有消费者ID（通过Broker查询）
            List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
            
            if (mqSet == null || cidAll == null) {
                balanced = false;
                break;
            }
            
            // 3. 排序（保证所有消费者看到相同的顺序）
            List<MessageQueue> mqAll = new ArrayList<>(mqSet);
            Collections.sort(mqAll);
            Collections.sort(cidAll);
            
            // 4. 执行分配策略
            AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;
            List<MessageQueue> allocateResult = strategy.allocate(
                this.consumerGroup,       // 消费者组名
                this.mQClientFactory.getClientId(),  // 当前消费者ID
                mqAll,                    // 所有队列
                cidAll);                  // 所有消费者ID
            
            Set<MessageQueue> allocateResultSet = new HashSet<>(allocateResult);
            
            // 5. 更新ProcessQueueTable（核心diff逻辑）
            boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
            
            if (changed) {
                balanced = false;
                // 触发messageQueueChanged回调
                this.messageQueueChanged(topic, mqAll, allocateResultSet);
            }
            
            break;
        }
        case BROADCASTING: {
            // 广播模式：每个消费者获取所有队列
            Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
            if (mqSet != null) {
                boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, false);
                if (changed) {
                    this.messageQueueChanged(topic, mqSet, mqSet);
                }
            }
            break;
        }
    }
    return balanced;
}
```

#### 8.2.4 分配策略详解

**策略1：AllocateMessageQueueAveragely（默认，连续分配）**

```java
// 8个队列，4个消费者 → 每人2个连续队列
// Consumer 0: [Q0, Q1]
// Consumer 1: [Q2, Q3]
// Consumer 2: [Q4, Q5]
// Consumer 3: [Q6, Q7]

// 8个队列，3个消费者 → 余数分配给前几个消费者
// Consumer 0: [Q0, Q1, Q2]  (多1个)
// Consumer 1: [Q3, Q4, Q5]  (多1个)
// Consumer 2: [Q6, Q7]      (正常)

public List<MessageQueue> allocate(String consumerGroup, String currentCID,
    List<MessageQueue> mqAll, List<String> cidAll) {
    
    int index = cidAll.indexOf(currentCID);
    int mod = mqAll.size() % cidAll.size();  // 余数
    int averageSize = mqAll.size() <= cidAll.size() ? 1
        : (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 
                                   : mqAll.size() / cidAll.size());
    int startIndex = (mod > 0 && index < mod) 
        ? index * averageSize 
        : index * averageSize + mod;
    int range = Math.min(averageSize, mqAll.size() - startIndex);
    
    List<MessageQueue> result = new ArrayList<>();
    for (int i = 0; i < range; i++) {
        result.add(mqAll.get(startIndex + i));
    }
    return result;
}
```

**策略2：AllocateMessageQueueAveragelyByCircle（轮询分配）**

```java
// 8个队列，3个消费者 → 交叉分配
// Consumer 0: [Q0, Q3, Q6]
// Consumer 1: [Q1, Q4, Q7]
// Consumer 2: [Q2, Q5]

// 优点：队列分散在不同Broker上，负载更均匀
// 缺点：顺序消费时可能跨Broker

for (int i = index; i < mqAll.size(); i++) {
    if (i % cidAll.size() == index) {
        result.add(mqAll.get(i));
    }
}
```

**策略3：AllocateMessageQueueConsistentHash（一致性哈希）**

```java
// 使用一致性哈希环，每个消费者在环上有10个虚拟节点
// 优点：消费者扩缩容时只影响相邻节点的队列
// 缺点：分配不均匀

Collection<ClientNode> cidNodes = new ArrayList<>();
for (String cid : cidAll) {
    cidNodes.add(new ClientNode(cid));
}
ConsistentHashRouter<ClientNode> router = 
    new ConsistentHashRouter<>(cidNodes, virtualNodeCnt);  // virtualNodeCnt=10

for (MessageQueue mq : mqAll) {
    ClientNode clientNode = router.routeNode(mq.toString());
    if (clientNode != null && currentCID.equals(clientNode.getKey())) {
        result.add(mq);
    }
}
```

**三种策略对比**：

| 策略 | 均匀性 | 扩缩容影响范围 | 适用场景 |
|------|--------|---------------|----------|
| Averagely | 最好 | 影响所有消费者 | 默认场景 |
| AveragelyByCircle | 好 | 影响所有消费者 | 需要跨Broker分散 |
| ConsistentHash | 一般 | 只影响相邻节点 | 消费者频繁扩缩容 |

#### 8.2.5 updateProcessQueueTableInRebalance：增量更新

```java
private boolean updateProcessQueueTableInRebalance(final String topic,
    final Set<MessageQueue> mqSet, final boolean needLockMq) {
    boolean changed = false;
    
    // 步骤1：标记不再属于当前消费者的队列
    HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>();
    Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
    while (it.hasNext()) {
        Entry<MessageQueue, ProcessQueue> next = it.next();
        MessageQueue mq = next.getKey();
        ProcessQueue pq = next.getValue();
        if (mq.getTopic().equals(topic)) {
            if (!mqSet.contains(mq)) {
                // 不在新分配列表中 → 标记为dropped
                pq.setDropped(true);
                removeQueueMap.put(mq, pq);
            } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                // 拉取超时 → 标记为dropped
                pq.setDropped(true);
                removeQueueMap.put(mq, pq);
            }
        }
    }
    
    // 步骤2：移除不需要的队列（先持久化offset）
    for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
        if (this.removeUnnecessaryMessageQueue(entry.getKey(), entry.getValue())) {
            this.processQueueTable.remove(entry.getKey());
            changed = true;
        }
    }
    
    // 步骤3：为新分配的队列创建ProcessQueue和PullRequest
    List<PullRequest> pullRequestList = new ArrayList<>();
    for (MessageQueue mq : mqSet) {
        if (!this.processQueueTable.containsKey(mq)) {
            ProcessQueue pq = createProcessQueue();
            pq.setLocked(true);
            
            // 计算初始拉取位置
            long nextOffset = this.computePullFromWhere(mq);
            if (nextOffset >= 0) {
                ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                if (pre == null) {
                    PullRequest pullRequest = new PullRequest();
                    pullRequest.setConsumerGroup(consumerGroup);
                    pullRequest.setNextOffset(nextOffset);
                    pullRequest.setMessageQueue(mq);
                    pullRequest.setProcessQueue(pq);
                    pullRequestList.add(pullRequest);
                    changed = true;
                }
            }
        }
    }
    
    // 步骤4：分发PullRequest
    this.dispatchPullRequest(pullRequestList, 500);
    
    return changed;
}
```

### 8.3 扩容场景演示

```
初始状态：4个消费者，8个队列
  Consumer-0: [Q0, Q1]   Consumer-1: [Q2, Q3]
  Consumer-2: [Q4, Q5]   Consumer-3: [Q6, Q7]

扩容：新增 Consumer-4

Rebalance触发后（所有消费者同时执行）：
  cidAll = [C0, C1, C2, C3, C4]  (5个消费者)
  
  Consumer-0: [Q0, Q1]  → [Q0, Q1]     (不变)
  Consumer-1: [Q2, Q3]  → [Q2, Q3]     (不变)
  Consumer-2: [Q4, Q5]  → [Q4]          (减少1个)
  Consumer-3: [Q6, Q7]  → [Q5, Q6]      (变化)
  Consumer-4: []        → [Q7]          (新增)
  
  Consumer-2的Q5被移除：
    - ProcessQueue.setDropped(true)
    - 持久化Q5的offset到Broker
    - 移除ProcessQueue
  
  Consumer-4获得Q7：
    - 创建ProcessQueue
    - 从Broker读取Q7的lastOffset
    - 创建PullRequest
    - 开始拉取消息
```

**知识点：消费者发现机制**

`findConsumerIdList` 通过向 Broker 发送 `GET_CONSUMER_LIST_BY_GROUP` 请求获取消费者列表。Broker 的 `ConsumerManager` 维护了每个消费者组的心跳信息，30秒未收到心跳的消费者会被移除。因此消费者上下线后，最多30秒（心跳超时）+ 20秒（Rebalance间隔）= 50秒后，其他消费者会感知到变化并重新分配队列。

---

## 场景九：消息过滤 —— Tag 与 SQL92 表达式精准消费

### 9.1 业务背景

同一个 Topic 下可能包含多种类型的消息（如订单创建、订单支付、订单退款）。不同消费者组只关心特定类型的消息。Tag 过滤适用于简单场景，SQL92 表达式适用于复杂条件过滤。

### 9.2 用户代码

**Tag 过滤**：

```java
// 生产者：设置不同的Tag
producer.send(new Message("OrderTopic", "CREATE", body1));
producer.send(new Message("OrderTopic", "PAY", body2));
producer.send(new Message("OrderTopic", "REFUND", body3));

// 消费者：只消费CREATE和PAY
consumer.subscribe("OrderTopic", "CREATE || PAY");

// 消费者：消费所有Tag
consumer.subscribe("OrderTopic", "*");
```

**SQL92 表达式过滤**：

```java
// 生产者：设置自定义属性
Message msg = new Message("OrderTopic", "CREATE", body);
msg.putUserProperty("amount", "500");
msg.putUserProperty("region", "beijing");
msg.putUserProperty("vip", "true");
producer.send(msg);

// 消费者：使用SQL92表达式过滤
consumer.subscribe("OrderTopic", 
    MessageSelector.bySql("amount > 100 AND region = 'beijing' AND vip = 'true'"));
```

### 9.3 源码全链路追踪

#### 9.3.1 两阶段过滤架构

RocketMQ 使用两阶段过滤策略避免读取完整的 CommitLog 消息：

**阶段1（ConsumeQueue级别）**：通过 ConsumeQueue 中存储的 `tagsCode` 或 Bloom Filter 快速判断是否可能匹配。

**阶段2（CommitLog级别）**：只有阶段1判断为"可能匹配"的消息，才从 CommitLog 读取完整内容并解析属性，执行精确过滤。

#### 9.3.2 Tag 过滤源码

```java
// ExpressionMessageFilter.isMatchedByConsumeQueue (阶段1)
@Override
public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
    if (null == subscriptionData) return true;
    
    if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
        // Tag过滤
        if (tagsCode == null) return true;
        if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) return true;  // "*"订阅全部
        
        // 检查tagsCode是否在订阅的CodeSet中
        return subscriptionData.getCodeSet().contains(tagsCode.intValue());
    }
    // ... SQL92分支 ...
}
```

**知识点：Tag 的 hashcode**

ConsumeQueue 中存储的不是 Tag 字符串，而是 Tag 的 hashcode（`tagsCode = tag.hashCode()`）。Producer 发送 `subscribe("OrderTopic", "CREATE || PAY")` 时，客户端会将 `CREATE` 和 `PAY` 的 hashcode 放入 `CodeSet`。Broker 在 ConsumeQueue 扫描时只需比较 8 字节的 hashcode，非常高效。

**Tag hashcode 冲突风险**：两个不同的 Tag 可能 hashcode 相同。因此阶段1判断为匹配的消息，在阶段2还需要验证完整 Tag（但对于 Tag 类型，阶段2直接返回 true，不验证）。这意味着理论上存在 Tag hashcode 冲突导致消费者收到不匹配消息的可能，但概率极低。

#### 9.3.3 SQL92 表达式过滤源码

```java
// ExpressionMessageFilter.isMatchedByConsumeQueue (阶段1 - SQL92分支)
@Override
public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
    // ... Tag分支 ...
    else {
        // SQL92过滤：使用Bloom Filter
        if (consumerFilterData == null || consumerFilterData.getExpression() == null
            || consumerFilterData.getCompiledExpression() == null
            || consumerFilterData.getBloomFilterData() == null) {
            return true;  // 没有过滤器，放行到阶段2
        }
        
        // 检查消息是否在消费者订阅之前产生
        if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
            return true;  // 旧消息直接放行
        }
        
        // Bloom Filter检查
        byte[] filterBitMap = cqExtUnit.getFilterBitMap();
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        if (filterBitMap == null || !this.bloomDataValid
            || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
            return true;  // BitMap无效，放行
        }
        
        BitsArray bitsArray = BitsArray.create(filterBitMap);
        boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
        return ret;  // Bloom Filter判断是否可能匹配
    }
}

// ExpressionMessageFilter.isMatchedByCommitLog (阶段2 - SQL92)
@Override
public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
    if (subscriptionData == null) return true;
    if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
        return true;  // Tag类型在阶段1已完成
    }
    
    // SQL92表达式求值
    ConsumerFilterData realFilterData = this.consumerFilterData;
    if (realFilterData == null || realFilterData.getExpression() == null
        || realFilterData.getCompiledExpression() == null) {
        return true;
    }
    
    // 解码消息属性
    if (tempProperties == null && msgBuffer != null) {
        tempProperties = MessageDecoder.decodeProperties(msgBuffer);
    }
    
    // 执行编译后的表达式
    try {
        MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);
        Object ret = realFilterData.getCompiledExpression().evaluate(context);
        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }
        return (Boolean) ret;
    } catch (Throwable e) {
        log.error("Message Filter error", e);
        return false;
    }
}
```

**知识点：SQL92 表达式编译**

```java
// ConsumerFilterManager.build
public static ConsumerFilterData build(String topic, String consumerGroup,
    String expression, String expressionType, long subVersion) {
    
    ConsumerFilterData filterData = new ConsumerFilterData();
    filterData.setTopic(topic);
    filterData.setConsumerGroup(consumerGroup);
    filterData.setBornTime(System.currentTimeMillis());
    filterData.setExpression(expression);
    filterData.setExpressionType(expressionType);
    filterData.setSubVersion(subVersion);
    
    // 编译表达式
    try {
        Expression expressionObj = FilterFactory.INSTANCE.get(expressionType).compile(expression);
        filterData.setCompiledExpression(expressionObj);
    } catch (Throwable e) {
        return null;
    }
    
    // 创建Bloom Filter
    BloomFilterData bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);
    filterData.setBloomFilterData(bloomFilterData);
    
    return filterData;
}
```

表达式在订阅时编译一次，后续每次过滤只需执行编译后的表达式对象，避免重复解析。

### 9.4 过滤流程图

```
ConsumeQueue扫描
    │
    ├─► 读取CQ条目 (tagsCode + cqExtUnit)
    │
    ├─► 阶段1: isMatchedByConsumeQueue(tagsCode, cqExtUnit)
    │   ├─ Tag类型: CodeSet.contains(tagsCode) → O(1)
    │   └─ SQL92: BloomFilter.isHit(filterBitMap) → 可能匹配?
    │
    ├─► 阶段1通过 → 从CommitLog读取消息
    │   阶段1不通过 → 跳过此消息
    │
    └─► 阶段2: isMatchedByCommitLog(msgBuffer, properties)
        ├─ Tag类型: 直接返回true (阶段1已足够)
        └─ SQL92: compiledExpression.evaluate(properties) → 精确匹配?
        
        阶段2通过 → 返回给消费者
        阶段2不通过 → 跳过（Bloom Filter误判）
```

---

## 场景十：集群消费 vs 广播消费 —— 不同消费模式对比

### 10.1 业务背景

集群消费：同一消费者组的多个实例分担消费，每条消息只被一个实例消费。适用于高吞吐场景。

广播消费：同一消费者组的每个实例都消费全量消息，每条消息被所有实例消费。适用于本地缓存刷新、配置同步等场景。

### 10.2 用户代码

```java
// 集群消费（默认）
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cluster_group");
consumer.setMessageModel(MessageModel.CLUSTERING);
consumer.subscribe("ConfigTopic", "*");

// 广播消费
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast_group");
consumer.setMessageModel(MessageModel.BROADCASTING);
consumer.subscribe("ConfigTopic", "*");
```

### 10.3 源码差异对比

#### 10.3.1 Rebalance 差异

```java
// RebalanceImpl.rebalanceByTopic
case CLUSTERING: {
    // 获取所有队列和所有消费者ID
    Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
    List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
    
    // 执行分配策略，每个消费者只获得部分队列
    List<MessageQueue> allocateResult = strategy.allocate(
        consumerGroup, clientId, mqAll, cidAll);
    // ...
}
case BROADCASTING: {
    // 每个消费者获得所有队列
    Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
    // 不执行分配策略，直接使用全部队列
    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, false);
    // ...
}
```

#### 10.3.2 OffsetStore 差异

```java
// DefaultMQPushConsumerImpl.start()
switch (this.defaultMQPushConsumer.getMessageModel()) {
    case BROADCASTING:
        // 广播模式：offset存储在本地文件
        this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory,
            this.defaultMQPushConsumer.getConsumerGroup());
        break;
    case CLUSTERING:
        // 集群模式：offset存储在Broker
        this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory,
            this.defaultMQPushConsumer.getConsumerGroup());
        break;
}
this.offsetStore.load();
```

**LocalFileOffsetStore**：
- 存储路径：`~/.rocketmq_offsets/{clientId}/{groupName}/offsets.json`
- `persist`：将offset写入本地JSON文件
- `readOffset`：从本地文件读取
- `updateConsumeOffsetToBroker`：空实现（不同步到Broker）

**RemoteBrokerOffsetStore**：
- `readOffset`：先查本地缓存，缓存未命中则发送 `QUERY_CONSUMER_OFFSET` 到Broker
- `persist`：发送 `UPDATE_CONSUMER_OFFSET` 到Broker
- Broker 端持久化到 `consumerOffset.json`

#### 10.3.3 消费失败处理差异

```java
// ConsumeMessageConcurrentlyService.processConsumeResult
case BROADCASTING:
    // 广播模式：直接丢弃失败消息
    for (int i = ackIndex + 1; i < msgs.size(); i++) {
        log.warn("BROADCASTING, message consume failed, drop it");
    }
    break;
    
case CLUSTERING:
    // 集群模式：发回Broker重试
    for (int i = ackIndex + 1; i < msgs.size(); i++) {
        boolean result = this.sendMessageBack(msg, context);
        // ...
    }
    break;
```

**知识点：广播模式不支持重试**

广播模式下，消费失败的消息直接丢弃，不进行重试。因为每个消费者独立消费全量消息，如果重试会导致该消息被重复投递。如果需要保证消费可靠性，应使用集群模式。

### 10.4 完整对比表

| 维度 | CLUSTERING | BROADCASTING |
|------|-----------|--------------|
| 队列分配 | 每个消费者获得部分队列 | 每个消费者获得全部队列 |
| Offset存储 | Broker端 | 本地文件 |
| 消费失败 | 发回Broker重试 | 直接丢弃 |
| 消息投递 | 每条消息被一个消费者消费 | 每条消息被所有消费者消费 |
| 支持重试 | 是（%RETRY%group） | 否 |
| 支持死信队列 | 是（%DLQ%group） | 否 |
| 顺序消费锁 | Broker端分布式锁 | 不需要（无竞争） |
| 适用场景 | 高吞吐业务消费 | 配置同步、缓存刷新 |

---

## 场景十一：消息回溯 —— 重新消费历史消息

### 11.1 业务背景

消费者代码存在Bug，导致最近2小时的消息处理有误。修复Bug后需要重新消费这2小时的历史消息。RocketMQ 支持通过时间戳重置消费位点，实现消息回溯。

### 11.2 用户代码

```java
// 方法1：通过管理命令重置
// mqadmin resetOffsetByTime -g consumer_group -t OrderTopic -s "2024-01-15#14:00:00:000"

// 方法2：通过代码重置（PushConsumer）
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");
consumer.subscribe("OrderTopic", "*");
consumer.start();

// 回溯到2小时前
long twoHoursAgo = System.currentTimeMillis() - 2 * 60 * 60 * 1000;
consumer.resetOffsetByTimeStamp(twoHoursAgo);
```

### 11.3 源码全链路追踪

#### 11.3.1 resetOffsetByTimeStamp

```java
// DefaultMQPushConsumerImpl.resetOffsetByTimeStamp
public void resetOffsetByTimeStamp(long timeStamp) throws MQClientException {
    for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
        Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (CollectionUtils.isNotEmpty(mqs)) {
            Map<MessageQueue, Long> offsetTable = new HashMap<>(mqs.size(), 1);
            
            // 对每个队列，根据时间戳查找对应的offset
            for (MessageQueue mq : mqs) {
                long offset = searchOffset(mq, timeStamp);
                offsetTable.put(mq, offset);
            }
            
            // 执行重置
            this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
        }
    }
}

// MQAdminImpl.searchOffset
// 向Broker发送SEARCH_OFFSET_BY_TIMESTAMP请求
// Broker在ConsumeQueue中二分查找，找到第一个storeTimestamp >= timeStamp的条目
public long searchOffset(MessageQueue mq, long timestamp) throws ... {
    return this.mQClientFactory.getMQClientAPIImpl().searchOffset(
        brokerAddr, requestHeader, 5000);
}
```

#### 11.3.2 MQClientInstance.resetOffset：核心重置逻辑

```java
public synchronized void resetOffset(String topic, String group,
    Map<MessageQueue, Long> offsetTable) {
    
    DefaultMQPushConsumerImpl consumer = ...;
    
    // 步骤1：暂停消费者（停止新的拉取）
    consumer.suspend();  // 设置 pause = true
    
    // 步骤2：清空所有ProcessQueue
    ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = 
        consumer.getRebalanceImpl().getProcessQueueTable();
    for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
        MessageQueue mq = entry.getKey();
        if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
            ProcessQueue pq = entry.getValue();
            pq.setDropped(true);  // 标记为已丢弃
            pq.clear();           // 清空缓存的消息
        }
    }
    
    // 步骤3：等待正在进行的消费完成（非顺序消费等待10秒）
    if (!consumer.isConsumeOrderly()) {
        try {
            TimeUnit.SECONDS.sleep(RESET_OFFSET_MAX_WAIT);  // 10秒
        } catch (InterruptedException ignored) { }
    }
    
    // 步骤4：更新每个队列的offset
    Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
    while (iterator.hasNext()) {
        MessageQueue mq = iterator.next();
        Long offset = offsetTable.get(mq);
        if (topic.equals(mq.getTopic()) && offset != null) {
            ProcessQueue pq = processQueueTable.get(mq);
            waitResetOffsetReady(consumer, pq);
            
            // 更新offset到OffsetStore
            consumer.updateConsumeOffset(mq, offset);
            
            // 移除旧ProcessQueue
            consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, pq);
            iterator.remove();
        }
    }
    
    // 步骤5：恢复消费者（触发doRebalance）
    consumer.resume();  // 设置 pause = false，触发 doRebalance()
}
```

**步骤5：resume 后的 Rebalance**

```java
public void resume() {
    this.pause = false;
    doRebalance();  // 立即触发一次Rebalance
}
```

Rebalance 会发现 ProcessQueue 被清空了，为每个 MessageQueue 重新创建 ProcessQueue 和 PullRequest。创建时调用 `computePullFromWhere` 读取 OffsetStore 中的 offset（此时已被重置为目标值）：

```java
// RebalancePushImpl.computePullFromWhereWithException
long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
if (lastOffset >= 0) {
    result = lastOffset;  // 使用重置后的offset
}
```

### 11.3 消息回溯全流程

```
时间线:
  T-2h: 有Bug的消费开始
  T-0:  发现Bug，停止消费者
  T+1:  修复Bug，重启消费者
  T+2:  调用resetOffsetByTimeStamp(T-2h)
  
  1. consumer.suspend() → 暂停拉取
  2. 清空所有ProcessQueue (丢弃缓存消息)
  3. 等待10秒 (让正在执行的消费线程完成)
  4. 向Broker查询每个队列在T-2h时刻的offset
  5. 更新OffsetStore中的offset为T-2h的值
  6. consumer.resume() → 触发Rebalance
  7. Rebalance创建新的ProcessQueue和PullRequest
  8. computePullFromWhere读取到重置后的offset
  9. 从T-2h的offset开始重新拉取消息
```

### 11.4 知识点：消息保留时间

消息回溯的前提是消息在 Broker 上仍然存在。Broker 的 `fileReservedTime`（默认72小时）控制 CommitLog 文件的保留时间。超过保留时间的消息文件会被 `CleanCommitLogService` 定时删除。因此回溯只能回溯到消息保留时间范围内。

---

## 场景十二：Producer 故障容错与延迟隔离 —— 高可用发送

### 12.1 业务背景

多机房部署的 RocketMQ 集群中，某个机房的 Broker 出现网络抖动导致发送延迟升高。Producer 需要自动隔离有问题的 Broker，将消息发送到健康的 Broker，待故障 Broker 恢复后再重新使用。

### 12.2 源码全链路追踪

#### 12.2.1 MQFaultStrategy：延迟隔离机制

```java
// MQFaultStrategy
private long[] latencyMax = {50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L};
private long[] notAvailableDuration = {0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L};

// 延迟→隔离时间映射:
// < 50ms → 不隔离
// 50-100ms → 不隔离
// 100-550ms → 隔离2秒
// 550-1800ms → 隔离5秒
// 1800-3000ms → 隔离6秒
// 3000-5000ms → 隔离10秒
// 5000-15000ms → 隔离30秒

private long computeNotAvailableDuration(final long currentLatency) {
    for (int i = latencyMax.length - 1; i >= 0; i--) {
        if (currentLatency >= latencyMax[i]) {
            return this.notAvailableDuration[i];
        }
    }
    return 0;
}

public void updateFaultItem(final String brokerName, final long currentLatency,
    boolean isolation, final boolean reachable) {
    if (this.sendLatencyFaultEnable) {
        // isolation=true时，使用10000ms作为延迟（对应隔离30秒）
        long duration = computeNotAvailableDuration(isolation ? 10000 : currentLatency);
        this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
    }
}
```

#### 12.2.2 LatencyFaultToleranceImpl：FaultItem 管理

```java
// LatencyFaultToleranceImpl.FaultItem
public class FaultItem implements Comparable<FaultItem> {
    private final String name;                    // Broker名称
    private volatile long currentLatency;         // 最近一次发送延迟
    private volatile long startTimestamp;         // 何时恢复可用
    private volatile boolean reachableFlag;       // 网络是否可达
    
    public void updateNotAvailableDuration(long notAvailableDuration) {
        if (notAvailableDuration > 0 
            && System.currentTimeMillis() + notAvailableDuration > this.startTimestamp) {
            this.startTimestamp = System.currentTimeMillis() + notAvailableDuration;
        }
    }
    
    public boolean isAvailable() {
        return System.currentTimeMillis() >= startTimestamp;
    }
    
    // 排序：可用 > 不可用，低延迟 > 高延迟
    @Override
    public int compareTo(final FaultItem other) {
        if (this.isAvailable() != other.isAvailable()) {
            if (this.isAvailable()) return -1;  // 可用的排前面
            if (other.isAvailable()) return 1;
        }
        if (this.currentLatency < other.currentLatency) return -1;
        else if (this.currentLatency > other.currentLatency) return 1;
        if (this.startTimestamp < other.startTimestamp) return -1;
        else if (this.startTimestamp > other.startTimestamp) return 1;
        return 0;
    }
}
```

#### 12.2.3 三级降级选择策略

```java
// MQFaultStrategy.selectOneMessageQueue
public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo,
    final String lastBrokerName, final boolean resetIndex) {
    
    BrokerFilter brokerFilter = threadBrokerFilter.get();
    brokerFilter.setLastBrokerName(lastBrokerName);
    
    if (this.sendLatencyFaultEnable) {
        if (resetIndex) {
            tpInfo.resetIndex();  // 重置轮询索引
        }
        
        // 优先级1：可用 + 不是上次失败的Broker
        MessageQueue mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter);
        if (mq != null) return mq;
        
        // 优先级2：网络可达 + 不是上次失败的Broker
        mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter);
        if (mq != null) return mq;
        
        // 优先级3：任意队列（所有Broker都被隔离时的兜底）
        return tpInfo.selectOneMessageQueue();
    }
    
    // 故障隔离未开启：只跳过上次失败的Broker
    MessageQueue mq = tpInfo.selectOneMessageQueue(brokerFilter);
    if (mq != null) return mq;
    return tpInfo.selectOneMessageQueue();
}
```

**三级降级场景演示**：

```
集群: Broker-A(正常), Broker-B(延迟高), Broker-C(网络不通)

正常状态:
  FaultItem-A: available=true,  latency=30ms,  reachable=true
  FaultItem-B: available=true,  latency=800ms, reachable=true
  FaultItem-C: available=false, latency=0,     reachable=false  (隔离中)

第一次选择:
  优先级1: 可用+不是上次失败 → Broker-A (延迟最低)
  
发送到Broker-A成功，latency=30ms → 不隔离

第二次选择:
  优先级1: 可用+不是A → Broker-B (可用但延迟高)
  
发送到Broker-B，latency=800ms → 隔离5秒

第三次选择:
  优先级1: 可用+不是B → Broker-A (又是A)
  
发送到Broker-A成功，latency=30ms → 不隔离

5秒后:
  Broker-B隔离到期，重新可用
  FaultItem-B: available=true,  latency=800ms, reachable=true
  
下次选择会优先选择Broker-A(延迟更低)，但Broker-A不可用时可以选择Broker-B
```

#### 12.2.4 Broker 健康检测

```java
// LatencyFaultToleranceImpl.startDetector()
// 每3秒检测一次所有Broker的网络可达性
public void startDetector() {
    this.scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
            if (startDetectorEnable) {
                detectByOneRound();
            }
        } catch (Exception e) { ... }
    }, 3, 3, TimeUnit.SECONDS);
}

public void detectByOneRound() {
    for (Map.Entry<String, FaultItem> item : this.faultItemTable.entrySet()) {
        FaultItem brokerItem = item.getValue();
        if (System.currentTimeMillis() - brokerItem.checkStamp >= 0) {
            brokerItem.checkStamp = System.currentTimeMillis() + this.detectInterval;
            
            String brokerAddr = resolver.resolve(brokerItem.getName());
            boolean serviceOK = serviceDetector.detect(brokerAddr, detectTimeout);
            
            if (serviceOK && !brokerItem.reachableFlag) {
                log.info(brokerItem.name + " is reachable now");
                brokerItem.reachableFlag = true;
            } else if (!serviceOK && brokerItem.reachableFlag) {
                log.info(brokerItem.name + " is unreachable now");
                brokerItem.reachableFlag = false;
            }
        }
    }
}
```

**知识点：检测器的作用**

没有检测器时，`reachableFlag` 只在发送失败时设为 false，恢复需要等下次发送成功。有了检测器，每3秒主动检测一次，即使不发送消息也能感知 Broker 恢复。这在某些 Broker 从不可达恢复时，能更快地重新使用该 Broker。

### 12.3 sendDefaultImpl 中的故障更新

```java
// DefaultMQProducerImpl.sendDefaultImpl
try {
    beginTimestampPrev = System.currentTimeMillis();
    sendResult = this.sendKernelImpl(msg, mq, communicationMode, ...);
    endTimestamp = System.currentTimeMillis();
    
    // 发送成功：更新延迟，标记可达
    this.updateFaultItem(mq.getBrokerName(),
        endTimestamp - beginTimestampPrev, false, true);
    
} catch (RemotingException e) {
    endTimestamp = System.currentTimeMillis();
    // 网络异常：强制隔离，标记不可达（如果检测器关闭）
    this.updateFaultItem(mq.getBrokerName(),
        endTimestamp - beginTimestampPrev, true,
        !this.mqFaultStrategy.isStartDetectorEnable());
    exception = e;
    continue;  // 重试
    
} catch (MQBrokerException e) {
    endTimestamp = System.currentTimeMillis();
    // Broker错误：强制隔离
    this.updateFaultItem(mq.getBrokerName(),
        endTimestamp - beginTimestampPrev, true, false);
    if (this.defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
        continue;  // 可重试错误码
    } else {
        throw e;  // 不可重试
    }
}
```

**可重试的响应码**（`retryResponseCodes`）：
- `ResponseCode.TOPIC_NOT_EXIST` (17)：Topic不存在
- `ResponseCode.SERVICE_NOT_AVAILABLE` (223)：服务不可用
- `ResponseCode.SYSTEM_ERROR` (1)：系统错误
- `ResponseCode.NO_PERMISSION` (206)：无权限
- `ResponseCode.NO_BUYER_ID` (13)：无Buyer ID
- `ResponseCode.FLUSH_DISK_TIMEOUT` (210)：刷盘超时
- `ResponseCode.FLUSH_SLAVE_TIMEOUT` (211)：Slave同步超时
- `ResponseCode.SLAVE_NOT_AVAILABLE` (212)：Slave不可用

---

## 十二场景总结：RocketMQ 核心能力矩阵

| 场景 | 核心机制 | 关键组件 | 设计知识点 |
|------|---------|---------|-----------|
| 普通消息 | 同步发送 + 拉取消费 | DefaultMQProducer, DefaultMQPushConsumer | 重试循环, 流控门控 |
| 顺序消息 | MessageQueueSelector + 三级锁 | MessageQueueLock, ProcessQueue.consumeLock | 分区顺序, Broker分布式锁 |
| 延时消息 | SCHEDULE_TOPIC + 定时投递 | ScheduleMessageService, DeliverDelayedMessageTimerTask | tagsCode复用, 18级延迟 |
| 事务消息 | 半消息 + 回查 | TransactionalMessageService, EndTransactionProcessor | 两阶段提交, op topic去重 |
| 批量消息 | 消息编码合并 | MessageBatch, MessageDecoder.encodeMessages | 一次网络往返, 约束限制 |
| 重试与DLQ | %RETRY% + %DLQ% + 递增延迟 | processConsumeResult, handleRetryAndDLQ | 递增delayLevel, maxReconsumeTimes |
| 消息轨迹 | 异步采集 + 专用Producer | AsyncTraceDispatcher, TraceContext | 三阶段采集, 避免递归 |
| 负载均衡 | 定时Rebalance + 分配策略 | RebalanceService, AllocateMessageQueueStrategy | 自适应间隔, 三种策略 |
| 消息过滤 | 两阶段过滤 | ExpressionMessageFilter, ConsumerFilterManager | Bloom Filter, SQL92编译 |
| 集群vs广播 | 队列分配 + Offset存储 | RemoteBrokerOffsetStore, LocalFileOffsetStore | 消费失败处理差异 |
| 消息回溯 | 时间戳查offset + 重置 | resetOffsetByTimeStamp, searchOffset | 暂停-清空-重置-恢复 |
| 故障容错 | 延迟隔离 + 三级降级 | MQFaultStrategy, LatencyFaultToleranceImpl | latencyMax映射, 健康检测 |

> 以上十二个场景覆盖了 RocketMQ 在生产环境中最常用的功能特性。每个场景都从用户代码出发，深入到 Broker 端的存储和处理逻辑，完整展现了消息从产生到消费的全生命周期。读者可以对照源码，逐行验证每个步骤的实现细节。
