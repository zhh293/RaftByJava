# 我眼里的 Canal 设计

## 一、核心问题域：MySQL 增量数据的统一捕获与消费

在一个多 MySQL 集群的异构数据架构中，业务方对增量数据的消费需求各不相同：有些需要实时同步到搜索引擎（ES）、有些需要构建数据仓库宽表、有些要驱动缓存失效。如果每个业务方各自去监听 Binlog，会带来三个问题：

1. **重复造轮子**：每个业务都得实现一套 Binlog 解析逻辑。
2. **对线上 MySQL 的侵入**：每个消费者都以 Slave 身份连接主库，Slave 连接数膨胀。
3. **消费能力参差不齐**：慢消费者拖慢 Binlog 位点推进，但 MySQL 主库并不关心某个 Slave 是否消费及时——它只需要把 Binlog 发给所有注册的 Slave。

因此需要一个**中间层**：它对外伪装成 MySQL Slave，统一从各个线上 MySQL 集群拉取 Binlog，解析、暂存，然后通过统一的消费接口暴露给所有下游业务方。这套中间层就是 Canal。

---

## 二、整体架构：三层分离

从职责上，Canal 可以拆成三个独立的层：

### 2.1 第一层：Binlog 监听与解析层（`CanalInstance`）

> **一句话**：伪装成 MySQL Slave，拉取 Binlog 并解析为结构化事件，写入内存队列。

这一层是 Canal 的**数据入口**。一个 `CanalInstance` 绑定一个 MySQL **Destination**（一个具体的 MySQL 实例或集群），它内部包含三个核心子模块：

- **Parser（解析模块）**：实现 MySQL Replication Protocol 的 Slave 端握手流程——`COM_REGISTER_SLAVE` 注册身份、`COM_BINLOG_DUMP` 发起 Binlog 订阅。它直接复用 MySQL 官方的 `mysql-connector-java` 或者 open-replicator 类库来解析网络流中的 Binlog Event，将其转化为 Canal 内部通用的 `Entry` 数据结构。


---

## 全局数据流全景

先给你一张完整的数据流路图，后面逐步展开每一层：

```
MySQL Master (Binlog)
    |
    | ① TCP连接 + MySQL握手认证
    | ② COM_REGISTER_SLAVE (0x15) 注册为Slave
    | ③ COM_BINLOG_DUMP (0x12) 请求Binlog数据流
    |
    v
MysqlConnection (模拟Slave)
    |
    | ④ 逐个MySQL网络包读取 [3B长度+1B序号][包体]
    |
    v
DirectLogFetcher (读字节流)
    |
    | ⑤ 检查标志位(OK/Error/EOF) + semi-sync处理 + 大包拼接
    |
    v
LogDecoder (初步解码)
    |
    | ⑥ 从字节流中解出 LogEvent 对象
    |
    +-------------- 并行模式(parallel=true) ---------------+
    |                                                       |
    v                                                       v
MysqlMultiStageCoprocessor                           串行模式(parallel=false)
(Disruptor 四阶段流水线)                              LogEventConvert 直接解析
    |                                                       |
    | Stage1: SimpleParserStage (单线程解码+TableMap)        |
    | Stage2: DmlParserStage   (多线程并行DML解析)           |
    | Stage3: SinkStoreStage   (单线程保序投递)              |
    |                                                       |
    +-------------------------------------------------------+
    |
    v
LogEventConvert (深度解析)
    |
    | ⑦ LogEvent → CanalEntry.Entry (Protobuf结构化数据)
    | ⑧ 表结构元数据查询(TableMetaCache)
    | ⑨ 字段级变更提取(before/after image)
    |
    v
EventTransactionBuffer (事务攒批)
    |
    | ⑩ TRANSACTIONBEGIN → 开始攒批
    |    ROWDATA → 持续积累
    |    TRANSACTIONEND → flush整个事务
    |
    v
CanalEventSink (投递Store)
    |
    | ⑪ 过滤 + 路由
    |
    v
CanalEventStore (MemoryEventStoreWithBuffer / RingBuffer)
    |
    | ⑫ 客户端通过 getWithoutAck() 消费
    |
    v
Client (消费者)
```

---

## 类继承关系全景图

理解 Parse 模块需要先厘清类继承关系，这是整个模块的骨架：

```
CanalEventParser (接口)
  │   定义: start() / stop() / getEventSink() / getLogPositionManager()
  │
  └── AbstractEventParser (抽象类) ★★★ 核心引擎 ★★★
      │   持有: parseThread, transactionBuffer, eventSink, logPositionManager
      │   核心: start() 中创建 parseThread, run() 主循环是整个模块的心跳
      │   回调: buildSinkHandler() → consumeTheEventAndProfilingIfNecessary()
      │
      └── AbstractMysqlEventParser (抽象类)
          │   增加: binlogParser(LogEventConvert), connection, metaConnection
          │   增加: 位点查找(findStartPosition), 表结构管理(tableMetaTSDB)
          │   增加: 并行解析(MysqlMultiStageCoprocessor)
          │
          └── MysqlEventParser (实现类) ★★★ MySQL专用 ★★★
              │   增加: masterInfo, standbyInfo (主备数据源)
              │   增加: HA切换(doSwitch), 心跳(MysqlDetectingTimeTask)
              │   增加: slaveId生成, GTID支持, 多种位点查找策略
              │
              └── LocalBinlogEventParser (本地文件模式)
                    解析本地binlog文件，不走网络，用于离线分析

ErosaConnection (接口)
  │   定义: connect() / reconnect() / disconnect() / dump() / seek()
  │
  └── MysqlConnection (实现类) ★★★ 模拟Slave连接 ★★★
        持有: MysqlConnector(TCP连接器), DirectLogFetcher(字节流读取器)
        核心: dump() = updateSettings → loadBinlogChecksum → sendRegisterSlave
              → sendBinlogDump → 循环fetch → 回调SinkFunction

BinlogParser (接口)
  │   定义: parse(byte[]) / parse(LogEvent)
  │
  └── LogEventConvert (实现类) ★★★ 深度解析器 ★★★
        将底层 LogEvent 转为结构化的 CanalEntry.Entry(Protobuf)
        处理: QueryEvent, RowsEvent, TableMapEvent, XidEvent 等

EventTransactionBuffer (事务缓冲区)
  │   环形数组实现，按事务边界攒批
  │   flush 回调 → consumeTheEventAndProfilingIfNecessary() → eventSink.sink()

MysqlMultiStageCoprocessor (Disruptor并行协处理器)
  │   四阶段流水线: publish → SimpleParserStage → DmlParserStage → SinkStoreStage
  │   基于 LMAX Disruptor RingBuffer 实现

DirectLogFetcher (底层网络读取器)
  │   从Socket读取MySQL网络包，处理分包拼接、semi-sync应答
  │   继承自 LogFetcher，管理内部 byte[] buffer

LogFetcher (抽象类)
  │   管理 buffer、position、limit 指针
  │   提供 ensureCapacity() 动态扩容
  │
  └── DirectLogFetcher (网络Socket读取)
  └── FileLogFetcher   (本地文件读取)
```

---

# 上面就是Parse模块具体实现思路


- **Sink（过滤与链路模块）**：对解析出来的 Event 做过滤（比如按库/表白名单过滤 DDL/DML），并将过滤后的事件打上链路追踪标识，送往下一环节。

- **Store（存储模块）**：基于 **Disruptor**（无锁环形缓冲区）实现的高性能内存队列。Store 将 `Entry` 按 RingBuffer 的序列号有序写入，同时提供基于序列号的随机读取能力（Get）和 ACK 推进能力（Ack/Rollback）。

> **为什么选 Disruptor？** 在高 QPS（万级 TPS 的 DML 场景）下，传统 `BlockingQueue` 的生产者-消费者锁竞争会显著降低吞吐。Disruptor 通过 CAS + 预填充缓存行消除了锁开销，并且天然支持单生产者多消费者的场景——Canal 的 Store 恰好是单生产者（Parser 线程写入）多消费者（多个下游消费线程读取）。

### 2.2 第二层：消费调度层（`CanalController` / `CanalServer`）

> **一句话**：从 `CanalInstance` 的 Store 中拉取事件，并以不同模式交付给下游。

这一层是 Canal 的**数据出口**，核心职责是**消费模式的选择与调度**。一个 `CanalController` 可以管理多个 `CanalInstance`，负责为每个 `CanalInstance` 创建消费链路。它提供两种消费模式：

#### 模式一：TCP 直连模式（开放端口）

`CanalController` 内置一个 Netty Server，监听指定端口，等待客户端主动建立 TCP 长连接。客户端连接后走 Canal 自定义的应用层协议（基于 Protobuf 序列化），可以执行三步操作：

1. **Subscribe**（订阅）：声明要消费的库、表。
2. **Get**（拉取）：按 `batchSize` 批量读取 Store 中指定 `sequence` 之后的事件。
3. **Ack**（确认）：消费完毕后回传确认位点，Store 据此推进 GC，释放已被消费的 RingBuffer 空间。

这种模式下，客户端自己管理消费位点，适合对消费进度有精细控制需求的场景。

#### 模式二：MQ 投递模式（发往消息队列）

为了避免客户端消费过慢导致 Store 爆满，另一种设计是：Canal 自身变成**主动推送方**。针对每一个 Destination，`CanalController` 启动一个**专用的工作线程**，以 `while(true)` 循环定时从 Store 拉取事件，序列化后投递到外部消息队列（RocketMQ / Kafka / RabbitMQ），下游业务方不再直连 Canal，而是从 MQ 消费。

MQ 模式下有一个关键设计问题：**分区路由策略**。同一张表的 DML 事件必须路由到 MQ 的同一个 Partition，才能保证消费的顺序性（RocketMQ 的 MessageQueue、Kafka 的 Partition 都是局部有序的）。Canal 的默认策略是 `hash(pk) % partitionNum`，对于无主键的表则轮询或使用 `schema.table` 的哈希。

### 2.3 第三层：统一消费接口（`CanalEmbeddedServer`）

> **一句话**：为上层屏蔽 TCP 模式和 MQ 模式的差异，让消费端代码零感知。

不管底层是 Netty Server 直连还是后台线程投递 MQ，`CanalEmbeddedServer` 暴露出一致的 API：

```text
subscribe(clientIdentity, filter)
get(clientIdentity, batchSize)
ack(clientIdentity, batchId)
rollback(clientIdentity, batchId)
```

**实现原理**：`CanalEmbeddedServer` 维护一个 `Map<String, CanalInstance>`，按客户端的 `destination`（即目标 MySQL 实例标识）路由到对应的 `CanalInstance`。它不关心事件最终是怎么到客户端的——它只管从 Store 取数据和推进位点。

这个设计本质上是一个**适配器模式（Adapter Pattern）**：
- TCP 模式下，Netty Handler 在解码客户端请求后，直接调用 `CanalEmbeddedServer` 的同名方法。
- MQ 模式下，后台工作线程在 `while` 循环中调用 `get()` 拉取数据，然后序列化投递到 MQ。

两者共享同一套核心调用链路，差异仅在上层触发方式。

---

## 三、高可用（HA）设计：分布式锁 + 主备切换

在实际生产环境中，单个 Canal 进程挂掉会导致增量数据断流。因此需要对同一个 Destination 部署**多个 Canal 实例**（通常部署在不同的物理机或容器上）。

问题来了：如果两个 Canal 实例同时对一个 MySQL 实例执行 `COM_BINLOG_DUMP`，MySQL 会认为有两个 Slave 在拉取同一份 Binlog——这本身并不会出错，但会导致 Store 中有两份完全重复的事件，下游可能重复消费。更严重的是，两个实例分别向同一个 MQ Topic 投递事件，分区内的顺序会被彻底打乱。

因此需要对每个 Destination 引入**互斥（Mutual Exclusion）**：

1. **选主机制**：Canal 不自己实现 Leader Election，而是依赖外部分布式协调服务（ZooKeeper 或 etcd）。每个 Canal 进程在启动时为每个 Destination 尝试创建一个**临时顺序节点**，序号最小的那个节点即为当前 Active 节点。

2. **分布式锁**：利用 ZooKeeper 的临时节点（EPHEMERAL）特性实现分布式锁——获得锁的实例成为 Active Instance，开始 `COM_BINLOG_DUMP` 并对外提供服务；未获得锁的实例进入 Standby 状态，不启动 Parser 和消费链路，仅心跳上报自身存活。

3. **故障切换（Failover）**：Active 实例宕机后，它与 ZooKeeper 的 Session 超时断开，临时节点自动删除。下一个序号的 Standby 实例通过 Watcher 感知到节点删除事件，重新竞争分布式锁，抢到锁后立即从**持久化的 Meta 位点**（上一次 Active 实例最后 ACK 的 Binlog 位点）开始 `COM_BINLOG_DUMP`，实现断点续传。

4. **优雅切换**：当 Active 实例正常收到停机信号时，先停止 Binlog 拉取，等待所有积压事件被消费端消费完毕（或达到超时），然后主动释放分布式锁，触发 Standby 实例接管。

---

## 四、启动流程：配置拉取 → SPI 决策 → 组件装配

Canal 的启动遵循严格的**分层初始化**顺序：

### 第一步：从 Admin 控制台拉取配置

Canal 的 Admin 模块是一个独立的 Web 管理平台，负责集中管理所有 Canal 集群的配置。启动时，Canal Server 首先通过 HTTP 向 Admin 拉取当**前节点应承担的所有配置项**，包括：

| 配置项 | 说明 |
|--------|------|
| `destination` | 要监听的 MySQL 实例逻辑名称 |
| `canal.ip` / `canal.port` | Canal Server 的 IP 和端口 |
| `canal.mode` | 消费模式：`tcp` / `rocketmq` / `kafka` |
| `canal.mq.topic` | MQ 模式下的 Topic 名称 |
| `canal.mq.servers` | MQ 集群地址 |
| `canal.instance.filter.regex` | 库表过滤正则 |
| `canal.instance.master.address` | 要监听的 MySQL Master 地址 |
| `canal.instance.master.journal.name` | 从哪个 Binlog 文件开始消费 |
| `canal.instance.master.position` | 从文件中的哪个偏移量开始 |
| `zk.servers` | ZooKeeper 集群地址（用于 HA 分布式锁） |

### 第二步：启动 Netty Server（暴露端口）

无论最终消费模式是 TCP 还是 MQ，Canal Server 都先启动 Netty Server 端口。因为在 TCP 模式下客户端需要直接连接；在 MQ 模式下，也需要暴露运维接口（如 JMX 监控端口、HTTP 管理接口）供 Admin 探活与查看状态。

### 第三步：SPI 决策——决定消费链路启动方式

**SPI（Service Provider Interface）** 是 Java 的一种服务发现机制：在 `META-INF/services/` 下声明接口实现类，JVM 在运行时通过 `ServiceLoader` 动态加载。

Canal 用 SPI 来解决**启动哪种消费模式**的决策问题：

```text
// 伪代码：SPI 决策
CanalMQProducer mqProducer = ServiceLoader.load(CanalMQProducer.class)
    .stream()
    .filter(p -> p.accept(canalMode))  // 按 canal.mode 匹配
    .findFirst()
    .orElse(null);

if (mqProducer != null) {
    // MQ 模式
    for (String destination : allDestinations) {
        // 为每个 destination 创建后台工作线程
        startMqWorkerThread(destination, mqProducer);
    }
} else {
    // TCP 模式
    // Netty Server 已启动，等待客户端直连即可
    // 客户端连接后由 CanalEmbeddedServer 直接服务
}
```

核心逻辑：
- 如果存在可用的 `CanalMQProducer` SPI 实现（如 `RocketMQProducer`、`KafkaProducer`），则为**每一个 Destination 创建工作线程**，该线程 `while(true)` 循环拉取事件 → 投递 MQ。
- 如果不存在任何 SPI 实现，则默认走 TCP 直连模式，Canal 仅作为一个 TCP Server 等待客户端连接消费。

这保证了**消费模式的插拔式扩展**——增加一种新的 MQ 类型时，只需新增一个实现了 `CanalMQProducer` 接口的 SPI 类并打入 JAR 即可，无需修改核心代码。

---

## 五、端到端的数据流（一笔 DML 的全生命周期）

以业务方在 MySQL 中执行 `UPDATE t_user SET name = 'Bob' WHERE id = 1001` 为例，完整数据流如下：

```text
MySQL Master (写入 Redo Log + Binlog)
        │
        │  MySQL Replication Protocol (Binlog Stream)
        ▼
┌─────────────────────────────────────────┐
│ CanalInstance (per Destination)           │
│                                           │
│  ① Parser: COM_BINLOG_DUMP 接收 Binlog   │
│     → 解析 UpdateRowsEvent               │
│     → 构造 Canal Entry (Header + RowData) │
│                                           │
│  ② Sink: 过滤（白名单匹配通过）           │
│     → 添加链路追踪标识                    │
│     → 送入 Store                          │
│                                           │
│  ③ Store (Disruptor RingBuffer)           │
│     → Entry 按 sequence 写入              │
│     → 等待消费端 Get                      │
└──────────────────┬──────────────────────┘
                   │
                   │ CanalEmbeddedServer.get(batchSize)
                   ▼
┌─────────────────────────────────────────┐
│ 消费链路 (二选一)                        │
│                                           │
│  [TCP 直连模式]                           │
│    客户端 Netty Channel → Subscribe/Get   │
│    → 解析 Protobuf Message               │
│    → 业务处理 → Ack(sequence)             │
│                                           │
│  [MQ 投递模式]                            │
│    后台工作线程 while(true) → Get        │
│    → 序列化为 JSON / Protobuf            │
│    → RocketMQ/Kafka Producer.send()      │
│    → 下游消费者从 MQ 拉取                 │
└──────────────────────────────────────────┘
```

---

## 六、关键技术决策总结

| 决策点 | 选型 | 原因 |
|--------|------|------|
| Binlog 协议实现 | MySQL Replication Protocol（伪装 Slave） | 官方协议的兼容性最好，无需对 MySQL 做任何改造 |
| 内存队列 | Disruptor RingBuffer | 无锁 CAS，避免高 TPS 下 `BlockingQueue` 的锁竞争 |
| 消费模式隔离 | SPI 插件化 | TCP / MQ 模式之间零耦合，新增 MQ 类型只需扩展 SPI |
| 统一消费接口 | `CanalEmbeddedServer`（适配器模式） | 上层调用方无需感知底层是 TCP 还是 MQ |
| 高可用 | 外部分布式锁（ZooKeeper 临时节点） | Canal 自身不造轮子，依赖成熟的分布式协调服务 |
| HA 模式 | 主备（Active-Standby），同 Destination 仅一个 Active | 避免重复 DUMP + 重复投递 |
| 配置管理 | Admin 控制台集中下发 | 多集群场景下避免各节点配置不一致 |
| 位点持久化 | ZooKeeper 或本地文件系统 | 故障恢复时可从上次 ACK 位点继续，不丢不重 |

---

## 七、与官方 Canal 源码的对应关系

本文描述的设计思路与 Alibaba Canal 官方源码高度吻合：

| 本文概念 | 官方模块 / 类 | 说明 |
|----------|--------------|------|
| CanalInstance | `CanalInstance` 接口 + `DefaultCanalInstance` | 一个 MySQL Destination 对应一个实例 |
| Parser | `CanalEventParser` → `MysqlEventParser` | 基于 open-replicator 解析 Binlog |
| Sink | `CanalEventSink` → `GroupEventSink` | 过滤 + 链路后写入 Store |
| Store | `CanalEventStore` → `MemoryEventStoreWithBuffer` | 基于 Disruptor 的环形缓冲区 |
| CanalEmbeddedServer | `CanalServerWithEmbedded` | 内嵌式统一消费接口 |
| CanalController | `CanalController` | 管理 Instance 生命周期 + 消费链路 |
| SPI MQ Producer | `CanalMQProducer` SPI | 插拔式 MQ 投递 |
| 分布式锁 | `RunningMonitor` → ZK 临时节点 | 基于 ZooKeeper 的 HA 主备 |
| Admin 配置拉取 | `CanalConfigClient` → Admin HTTP API | 启动时拉取集中配置 |

---

## 八、设计思想提炼

Canal 的设计本质上是**关注点分离**原则在数据集成领域的实践：

1. **数据捕获** 与 **数据消费** 解耦——`CanalInstance` 只负责从 MySQL 拿到数据并缓存，不关心下游怎么消费。
2. **消费接口** 与 **消费交付方式** 解耦——`CanalEmbeddedServer` 提供统一的 Get/Ack 语义，TCP 还是 MQ 只是上层的两种"触发方式"。
3. **实例生命周期** 与 **业务配置** 解耦——Admin 控制台集中管理配置，Canal 进程无状态化（状态外挂到 ZK）。

这种分层解耦使得 Canal 既能以轻量级进程嵌入到 Java 应用中使用（Embedded 模式），也能以独立 Server 集群模式对外服务（Standalone / Cluster 模式），适应了从开发调试到生产高可用的全场景需求。


## 九 canal真实架构和启动流程

```
JVM 启动
  |
  +-- 1. CanalLauncher.main()
  |     -> 加载 canal.properties
  |     -> [可选] 连接 Canal Admin，从远程拉取配置 + 启动定时轮询配置变更
  |     -> 创建 CanalStarter，调用 start()
  |
  +-- 2. CanalStarter.start()
  |     -> [MQ模式] SPI加载CanalMQProducer并初始化
  |     -> new CanalController(properties)  <-- 核心初始化发生在构造函数中
  |     -> controller.start()
  |     -> [MQ模式] 启动CanalMQStarter(1)
  |     -> [有admin.port] 启动CanalAdminWithNetty
  |
  +-- 3. CanalController 构造函数（核心初始化）
  |     +-- initGlobalConfig()
  |     |     -> 创建 instanceGenerator Lambda（决定如何创建CanalInstance）
  |     +-- initInstanceConfig()
  |     |     -> 解析 canal.destinations（如 "example,order,user"）
  |     +-- 创建 CanalServerWithEmbedded 单例
  |     +-- 创建 CanalServerWithNetty 单例
  |     +-- [有ZK] 初始化 ZkClientx
  |     +-- 为每个 destination 配置 ServerRunningMonitor（HA竞争机制）
  |     |     -> processActiveEnter 回调: embeddedServer.start(destination)
  |     |     -> processActiveExit  回调: embeddedServer.stop(destination)
  |     +-- [autoScan] 创建 InstanceConfigMonitor（配置变更扫描器）
  |
  +-- 4. CanalController.start()
  |     +-- ZK注册当前server节点（临时节点）
  |     +-- embeddedCanalServer.start()
  |     |     -> 创建 LazyMap<destination, CanalInstance>
  |     +-- 遍历所有 destination：
  |     |     +-- ServerRunningMonitor.start()
  |     |     |     -> ZK抢占运行权
  |     |     |     -> 成功后回调 processActiveEnter
  |     |     |         -> embeddedCanalServer.start(destination)
  |     |     |             -> canalInstances.get(destination)  <-- 触发LazyMap
  |     |     |                 -> instanceGenerator.generate(destination)
  |     |     |                     +-- [SPRING模式] SpringCanalInstanceGenerator
  |     |     |                     |     -> 加载 Spring XML → 获取 instance bean
  |     |     |                     +-- [MANAGER模式] PlainCanalInstanceGenerator
  |     |     |                           -> 远程拉取配置 → new CanalInstanceWithManager
  |     |     |             -> canalInstance.start()
  |     |     |                 +-- metaManager.start()     -- 消费位点管理
  |     |     |                 +-- alarmHandler.start()    -- 报警
  |     |     |                 +-- eventStore.start()      -- RingBuffer存储
  |     |     |                 +-- eventSink.start()       -- Sink桥接器
  |     |     |                 +-- eventParser.start()     -- binlog解析器（开始dump!）
  |     |     +-- instanceConfigMonitor.register(destination)
  |     +-- instanceConfigMonitor.start()  -- 启动配置变更扫描
  |     +-- canalServer.start()  -- 启动Netty TCP服务
  |           -> ServerBootstrap 绑定端口（默认11111）
  |           -> Pipeline: FixedHeaderFrameDecoder → Handshake → Auth → Session
  |
  +-- 5. 主线程 runningLatch.await() 阻塞，直到 ShutdownHook 触发
```

## 补充一下(1)的过程

```
 +-- [MQ模式] CanalMQStarter.start()
          |
          +-- 遍历所有 destination：
          |     |
          |     +-- 为每个 destination 创建 CanalMQRunnable(destination, mqProducer)
          |     |     |
          |     |     +-- Runnable.run() 内部核心逻辑:
          |     |           while (running) {
          |     |             // ① 从 EmbeddedServer 拉取（不走TCP协议栈，直接调内部API）
          |     |             messages = embeddedServer.getWithoutAck(destination, batchSize, timeout, unit)
          |     |             if (messages.isEmpty()) continue
          |     |
          |     |             // ② 过滤 + 格式转换（Entry → MQ Message）
          |     |             flatMessages = flatMessageCallback.filterAndConvert(messages)
          |     |             //    - 过滤事务 Begin/End 事件
          |     |             //    - 将 RowChange 展平为 JSON/KV 格式
          |     |             //    - 拼装库名、表名、事件类型、变更前后数据
          |     |
          |     |             // ③ 分区路由（保证同一行数据的变更有序）
          |     |             partition = hash(flatMessage.pk) % partitionNum
          |     |             //    无主键时用 schema.table.hashCode()
          |     |
          |     |             // ④ 投递到 MQ
          |     |             mqProducer.send(topic, partition, flatMessage)
          |     |             //    RocketMQ: 指定 MessageQueue
          |     |             //    Kafka:    指定 Partition
          |     |
          |     |             // ⑤ ACK 推进 Store 位点（释放 RingBuffer 空间）
          |     |             embeddedServer.ack(destination, batchId)
          |     |           }
          |     |
          |     +-- 提交到 executorService 执行（每个 destination 一个常驻线程）
          |
          +-- 注册 ShutdownHook：
                -> running = false（通知所有工作线程退出循环）
                -> executorService.shutdown()（等待最后一批消息投递完成）
                -> mqProducer.shutdown()（关闭与 MQ 集群的连接）
                -> 等待 CanalController.stop() 完成 Store/Instance 清理


```


---

# 即使 MQ 模式下不走 TCP 消费数据，Netty 端口仍然要承担其他职责——比如 Admin 探活、运维指令下发、JMX 监控数据拉取。

## Canal 整体架构概览

在深入源码之前，先理解 Canal 的整体架构。Canal 的核心设计理念是**伪装成 MySQL slave，通过 MySQL 的主从复制协议获取 binlog，然后解析成结构化的数据变更事件，提供给下游消费**。

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                    CanalController (调度中枢)                 │
                    │                                                             │
                    │  ┌───────────────────┐    ┌───────────────────────────┐     │
                    │  │  CanalServerWith  │    │  CanalServerWithNetty    │     │
                    │  │     Embedded      │    │  (Netty TCP 11111端口)    │     │
                    │  │  (数据服务层)       │◄───│  (网络协议层)             │     │
                    │  │  subscribe/get/   │    │  Protobuf编解码          │     │
                    │  │  ack/rollback     │    │  4字节Length帧协议        │     │
                    │  └────────┬──────────┘    └───────────────────────────┘     │
                    │           │                                                  │
                    │           │ canalInstances.get(destination)                  │
                    │           ▼                                                  │
                    │  ┌──────────────────────────────────────────────────────┐   │
                    │  │           CanalInstance (per destination)             │   │
                    │  │                                                      │   │
                    │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │   │
                    │  │  │ Parser   │→│  Sink    │→│  Store   │→│  Meta  │ │   │
                    │  │  │模拟slave │ │过滤/路由  │ │RingBuffer│ │位点管理│ │   │
                    │  │  │解析binlog│ │投递store │ │内存存储   │ │cursor  │ │   │
                    │  │  └──────────┘ └──────────┘ └──────────┘ └────────┘ │   │
                    │  └──────────────────────────────────────────────────────┘   │
                    └─────────────────────────────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │        HA 机制 (ServerRunningMonitor)  │
                    │  通过 ZooKeeper 临时节点竞争运行权      │
                    │  多个 Canal Server 只有一个active      │
                    └──────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │     配置监控 (InstanceConfigMonitor)   │
                    │  SPRING模式: 扫描conf目录文件变更       │
                    │  MANAGER模式: 轮询远程Admin API        │
                    └──────────────────────────────────────┘
```

### 核心类继承关系

```
CanalLifeCycle (接口)
  +-- CanalServer (接口)
  |     +-- CanalServerWithEmbedded (implements CanalServer, CanalService) [单例]
  |     +-- CanalServerWithNetty (implements CanalServer) [单例]
  +-- CanalInstance (接口)
  |     +-- AbstractCanalInstance (抽象类)
  |           +-- CanalInstanceWithManager (Manager模式)
  |           +-- CanalInstanceWithSpring  (Spring模式)
  +-- InstanceConfigMonitor (接口)
        +-- SpringInstanceConfigMonitor  (监听文件系统目录变化)
        +-- ManagerInstanceConfigMonitor (轮询远程Admin配置变化)

CanalService (接口) -- 定义 subscribe/get/ack/rollback 方法
CanalInstanceGenerator (接口) -- 工厂接口，通过 destination 生成 CanalInstance
```

### 模块职责划分

| 模块 | 核心类 | 职责 |
|------|--------|------|
| deployer | CanalLauncher / CanalStarter / CanalController | JVM 入口、生命周期编排、Instance 调度 |
| server | CanalServerWithEmbedded / CanalServerWithNetty | 数据服务(subscribe/get/ack)、网络协议层 |
| instance | CanalInstance / AbstractCanalInstance | 四大组件(parser/sink/store/meta)的容器 |
| parse | AbstractEventParser / MysqlEventParser | 模拟 slave 协议、binlog dump 与解析 |
| sink | EntryEventSink / GroupEventSink | 数据过滤、路由、多源归并 |
| store | MemoryEventStoreWithBuffer | RingBuffer 内存存储 |
| meta | MemoryMetaManager / FileMixed / ZooKeeper | 消费位点、batch 管理 |
| protocol | CanalEntry / CanalPacket | Protobuf 协议定义 |
| filter | AviaterRegexFilter | 表名正则过滤 |
| client | CanalConnector / SimpleCanalConnector | 客户端 SDK |
| client-adapter | OuterAdapter / ES / RDB / HBase | 数据同步到下游存储 |

---

## 十 canal在此架构上做的一些性能优化

### 1. 懒加载 + LazyMap

`CanalServerWithEmbedded` 中的 `canalInstances` 是 `ComputingMap`（Guava），首次通过 `get(destination)` 访问时才调用 `instanceGenerator.generate()` 创建 Instance。这意味着：配置了 100 个 destination 但只有 10 个客户端在消费时，只会创建 10 个 Instance，其余 90 个不占用任何资源（不连接 MySQL、不开线程）。

### 2. HA 机制 —— ZK 临时节点竞争

通过 ZooKeeper 的临时节点实现 destination 级别的主备竞争。同一时刻同一个 destination 只有一个 Canal Server 在运行，其他 Server 作为 standby 监听节点变化。当 active 节点宕机，临时节点自动删除，standby 节点感知到后立即抢占，实现秒级故障转移。

### 3. 双模式 Instance 创建

SPRING 模式通过 Spring XML 灵活装配组件（适合开发者定制），MANAGER 模式通过远程 API 拉取配置编程式构建（适合运维管理大量 Instance）。两种模式最终都产出相同的 `CanalInstance` 抽象，上层代码无感知。

### 4. Netty Pipeline 动态重构

认证完成后动态移除握手和认证 Handler，添加空闲检测 Handler。精确控制连接生命周期各阶段的处理逻辑，避免不必要的 Handler 处理开销。

### 5. 协议设计极简 + 高性能序列化

4 字节长度 + Protobuf body 的极简帧协议。GET 响应中对 raw 模式数据绕过 Protobuf Builder，直接用 `CodedOutputStream` 手工序列化，显著减少 GC 压力。

### 6. 三层配置热更新

| 层级 | 范围 | 机制 |
|------|------|------|
| CanalLauncher | 整个 canal.properties | 远程轮询 Admin API，MD5 变化时全量重启 |
| ManagerInstanceConfigMonitor | 单个 instance 配置 | 远程轮询 Admin API，发现增删改 |
| SpringInstanceConfigMonitor | 单个 instance 配置 | 本地文件系统扫描，检测文件修改时间变化 |

### 7. 组件启动顺序保证

AbstractCanalInstance 严格按"下游先启动、上游后启动"的顺序初始化组件：MetaManager → AlarmHandler → EventStore → EventSink → EventParser。停止时反序。这个设计保证了 Parser 开始产出数据时，整个数据管道已经就绪，不会丢数据。

