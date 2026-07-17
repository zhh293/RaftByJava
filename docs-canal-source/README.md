# Canal 源码深度分析文档

> 基于 Alibaba Canal 源码逐层分析，涵盖从 MySQL binlog dump 协议伪装到下游存储同步的全链路，每篇均结合完整源码、不跳步、不省略。

---

## 项目简介

Canal（[kə'næl]，水道/管道）是阿里巴巴开源的 MySQL 数据库增量日志解析工具，基于 MySQL 主从复制协议，伪装成 MySQL slave 获取 binlog，解析成结构化的数据变更事件，提供给下游消费。

本系列文档从源码级别深度剖析 Canal 的每一个核心模块，遵循"先给调用链全景、再逐层展开源码"的风格，适合需要深入理解 Canal 内部实现机制的开发者。

---

## 文档目录

| 编号 | 文档 | 核心内容 |
|------|------|---------|
| 01 | [Canal 整体架构与启动流程](./01-Canal整体架构与启动流程-源码全流程解析.md) | CanalLauncher → CanalStarter → CanalController 构造函数初始化 → start() 启动 → Embedded/Netty Server → CanalInstance 五大组件装配与启动顺序 → HA 竞争机制（ZK 临时节点）→ 配置热更新 |
| 02 | [Parse 模块 — 模拟 Slave 协议与 Binlog Dump](./02-Parse模块-模拟Slave协议与BinlogDump全流程源码解析.md) | AbstractEventParser.run() 核心引擎主循环 → MysqlConnection 伪装 slave 协议（COM_REGISTER_SLAVE/COM_BINLOG_DUMP 字节级构造）→ DirectLogFetcher 网络包拆解 → 位点查找多级决策树 → Disruptor 四阶段并行流水线 → EventTransactionBuffer 事务攒批 → 心跳与 HA 切换 |
| 03 | [Binlog 事件解析与 LogEvent 转换](./03-Binlog事件解析与LogEvent转换-源码全流程解析.md) | MySQL binlog 事件格式基础 → LogDecoder 解码分发 → LogEventConvert.parse() 事件类型分发 → parseQueryEvent() DDL/事务/XA → parseRowsEvent() DML 行数据解析 → parseOneRow() 列对齐的灵魂（unsigned 修正 / BINARY vs TEXT / 字段过滤）→ TableMetaCache 表结构缓存与 TSDB → CanalEntry.Entry 协议结构 |
| 04 | [Sink 过滤与 Store 环形缓冲区](./04-Sink过滤与Store环形缓冲区-源码全流程解析.md) | AviaterRegexFilter 正则过滤引擎 → EntryEventSink 空事务双级过滤 → doSink() Handler 责任链与渐进式退避 → GroupEventSink 多源归并 → TimelineBarrier/TimelineTransactionBarrier 时间线归并排序与事务感知 → MemoryEventStoreWithBuffer 三指针 RingBuffer（put/get/ack/rollback）→ DDL 隔离 → ITEMSIZE/MEMSIZE 批量模式 |
| 05 | [CanalInstance 装配与 Meta 位点持久化](./05-CanalInstance装配与Meta位点持久化-源码全流程解析.md) | CanalInstance 接口与五大组件 → AbstractCanalInstance 严格有序启动/停止 → CanalInstanceWithManager 编程式装配 → CanalInstanceWithSpring Spring XML 装配 → 四种 MetaManager（Memory/File/ZK/Mixed）→ batch 管理的顺序约束 → LogPositionManager 五种实现 → 位点安全设计（事务边界持久化）|
| 06 | [客户端订阅协议与 Get/Ack/Rollback 全流程](./06-客户端订阅协议与GetAckRollback全流程-源码全流程解析.md) | TCP 连接→握手→认证→订阅→消费 完整时序 → CanalConnector 接口设计 → SimpleCanalConnector 单机直连 → ClusterCanalConnector 集群 HA → 密码 Scramble 算法 → CanalMessageDeserializer 反序列化 → CanalServerWithEmbedded 数据服务 → 客户端最佳实践 |
| 07 | [Server 网络层与 Netty 协议编解码](./07-Server网络层与Netty协议编解码-源码全流程解析.md) | CanalServer 双 Server 架构 → Netty 3.x Pipeline 构建 → FixedHeaderFrameDecoder 帧解码 → HandshakeInitializationHandler 握手 → ClientAuthenticationHandler 认证与 Pipeline 动态重构 → SessionHandler 五种请求处理（含 raw 模式高性能序列化）→ SecurityUtil 密码安全 → CanalMQStarter MQ 投递模式 → Kafka/RocketMQ Connector |
| 08 | [Client-Adapter 数据同步到下游存储](./08-ClientAdapter数据同步到下游存储-源码全流程解析.md) | Adapter 框架层 SPI 设计 → OuterAdapter 接口 → Dml 数据结构 → ExtensionLoader 动态加载 → Launcher 启动与消费循环 → RDB 适配器（SQL 构建/批量执行）→ ES 适配器（BulkRequest/文档映射）→ HBase 适配器（RowKey 构建/Put/Delete）→ 全量+增量同步模型 |

---

## 整体架构图

```
                    MySQL Master
                        │
                        │ MySQL Replication Protocol
                        │ COM_REGISTER_SLAVE + COM_BINLOG_DUMP
                        ▼
    ┌───────────────────────────────────────────────────────────────┐
    │                     Canal Server                              │
    │                                                               │
    │  ┌─────────────────────────────────────────────────────────┐ │
    │  │              CanalInstance (per destination)              │ │
    │  │                                                          │ │
    │  │  ┌────────┐   ┌────────┐   ┌─────────┐   ┌──────────┐ │ │
    │  │  │ Parser │──→│  Sink  │──→│  Store   │   │   Meta   │ │ │
    │  │  │        │   │        │   │RingBuffer│   │ 位点管理  │ │ │
    │  │  │模拟slave│   │过滤投递 │   │ 内存存储  │   │cursor/   │ │ │
    │  │  │解析binlog│  │空事务滤 │   │put/get/  │   │ batch    │ │ │
    │  │  │Disruptor│  │DDL隔离  │   │ack/roll  │   │          │ │ │
    │  │  └────────┘   └────────┘   └─────────┘   └──────────┘ │ │
    │  └─────────────────────────────────────────────────────────┘ │
    │                                                               │
    │  ┌──────────────────┐     ┌──────────────────────────────┐  │
    │  │  Embedded Server  │◄────│  Netty Server (TCP 11111)    │  │
    │  │  subscribe/get/   │     │  Protobuf + 4字节Length帧    │  │
    │  │  ack/rollback     │     │  Pipeline动态重构             │  │
    │  └──────────────────┘     └──────────────────────────────┘  │
    └───────────────────────────────────────────────────────────────┘
                │                              │
                │ (MQ模式)                     │ (TCP模式)
                ▼                              ▼
    ┌────────────────────┐       ┌────────────────────────────┐
    │  Kafka / RocketMQ   │       │  Canal Client (SDK)         │
    │  RabbitMQ / Pulsar  │       │  SimpleCanalConnector       │
    └────────────────────┘       │  ClusterCanalConnector      │
                │                 └────────────────────────────┘
                ▼                              │
    ┌────────────────────┐                     ▼
    │  Client-Adapter     │       ┌────────────────────────────┐
    │  RDB/ES/HBase/      │       │  业务消费应用                │
    │  ClickHouse/Kudu    │       └────────────────────────────┘
    └────────────────────┘
```

---

## 数据流全链路

```
MySQL binlog 字节流
    │
    ▼
DirectLogFetcher.fetch()        -- 网络包拆解（去包头/OK标志/分包拼接）
    │
    ▼
MysqlMultiStageCoprocessor      -- Disruptor 四阶段流水线
    ├─ SimpleParserStage         -- 单线程：LogDecoder 解码 + TableMeta 建立
    ├─ DmlParserStage            -- 多线程：DML 行数据深度解析 → CanalEntry.Entry
    └─ SinkStoreStage            -- 单线程：按序投递 EventTransactionBuffer
    │
    ▼
EventTransactionBuffer          -- 按事务边界攒批（TRANSACTIONEND 时 flush）
    │
    ▼
EntryEventSink.sinkData()       -- 过滤（AviaterRegexFilter）+ 空事务双级过滤
    │
    ▼
MemoryEventStoreWithBuffer      -- 三指针 RingBuffer（putSequence/getSequence/ackSequence）
    │
    ▼
CanalServerWithEmbedded         -- subscribe/getWithoutAck/ack/rollback
    │
    ├─ (TCP) SessionHandler      -- Protobuf 编解码 → Netty 响应
    └─ (MQ)  CanalMQStarter     -- getWithoutAck → MQProducer.send → ack
```

---

## 推荐阅读顺序

1. **先读第 01 篇**：理解整体架构和启动流程，建立全局视角
2. **读第 02 + 03 篇**：深入理解数据从 MySQL 到 Canal 的获取和解析过程
3. **读第 04 篇**：理解数据在 Canal 内部的流转（Sink 过滤 + Store 存储）
4. **读第 05 篇**：理解组件装配和位点持久化机制
5. **读第 06 + 07 篇**：理解客户端如何消费数据（协议层和网络层）
6. **读第 08 篇**：理解如何将数据同步到下游存储

---

## 按使用场景查找

| 场景 | 推荐文档 |
|------|---------|
| 想了解 Canal 整体架构 | 01 - 整体架构与启动流程 |
| 想了解 Canal 如何伪装成 MySQL slave | 02 - Parse 模块 |
| 想了解 binlog 二进制格式如何解析 | 03 - Binlog 事件解析 |
| 想了解数据在 Canal 内部如何缓存 | 04 - Store 环形缓冲区 |
| 想了解 Canal 如何保证不丢数据 | 05 - 位点持久化 |
| 想了解客户端 SDK 如何使用 | 06 - 客户端订阅协议 |
| 想了解 Canal 的网络协议设计 | 07 - Server 网络层 |
| 想了解如何同步到 ES/HBase/RDB | 08 - Client-Adapter |
| 想了解 Canal HA 机制 | 01 (ZK竞争) + 02 (心跳切换) |
| 想了解 Canal MQ 投递模式 | 07 - CanalMQStarter |
| 想了解 Disruptor 在 Canal 中的应用 | 02 - MysqlMultiStageCoprocessor |
| 想了解 Canal 的表结构追踪（TSDB） | 03 - TableMetaCache |

---

## 核心设计模式索引

| 设计模式 | 应用位置 | 文档 |
|---------|---------|------|
| LazyMap（懒加载） | CanalServerWithEmbedded.canalInstances | 01 |
| ZK 临时节点竞争 | ServerRunningMonitor（HA机制） | 01 |
| 工厂方法 | instanceGenerator Lambda | 01 |
| Disruptor RingBuffer | MysqlMultiStageCoprocessor（四阶段流水线） | 02 |
| 模板方法 | AbstractEventParser.run() | 02 |
| 策略模式 | AviaterRegexFilter / AviaterSimpleFilter | 04 |
| 责任链 | EntryEventSink.doSink() Handler before/after/retry | 04 |
| 三指针 RingBuffer | MemoryEventStoreWithBuffer (put/get/ack) | 04 |
| 归并排序 + 屏障 | TimelineBarrier (PriorityBlockingQueue) | 04 |
| 装饰器 | FileMixedMetaManager → MemoryMetaManager | 05 |
| SPI 插件化 | OuterAdapter / CanalMQProducer | 07, 08 |
| Pipeline 动态重构 | ClientAuthenticationHandler | 07 |
| 单例 | CanalServerWithEmbedded / CanalServerWithNetty | 01, 07 |
