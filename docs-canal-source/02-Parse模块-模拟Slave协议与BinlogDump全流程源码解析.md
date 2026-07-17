# Canal Parse模块 —— 模拟MySQL Slave协议与Binlog Dump全流程源码解析

> 基于源码项目 `canal` 逐步分析，从 `AbstractEventParser.start()` 启动 parseThread，到模拟 MySQL Slave 握手认证、发送 COM_BINLOG_DUMP 命令、逐字节读取 Binlog 网络包、Disruptor 并行解析、事务攒批投递，不跳步、不省略。

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

## 第一阶段：AbstractEventParser.start() —— 核心引擎

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/AbstractEventParser.java`

这是整个 Parse 模块的心脏。`start()` 方法创建一个 `parseThread` 线程，该线程的 `run()` 方法包含了从连接 MySQL 到 dump binlog 到解析投递的全部逻辑。可以说，理解了这个 `run()` 方法，就理解了 Parse 模块的 80%。

### 1.1 start() 方法——启动引擎

```java
// AbstractEventParser.java
public void start() {
    super.start();
    // ① 初始化事务缓冲区（环形数组 + flush回调）
    transactionBuffer = new EventTransactionBuffer(new TransactionFlushCallback() {
        public void flush(List<CanalEntry.Entry> transaction) throws InterruptedException {
            // 事务完成后的回调：消费并持久化位点
            boolean successed = consumeTheEventAndProfilingIfNecessary(transaction);
            if (!successed) {
                throw new CanalParseException("consume failed!");
            }
            // 构建最后一条事务的位点信息
            LogPosition position = buildLastTransactionPosition(transaction);
            if (position != null) {
                logPositionManager.persistLogPosition(
                    AbstractEventParser.this.destination, position);
            }
        }
    });
    transactionBuffer.setBufferSize(transactionSize);
    transactionBuffer.start();

    // ② 创建并启动 parseThread（整个 Parse 模块的引擎线程）
    parseThread = new Thread(new Runnable() {
        public void run() {
            // ===== parseThread 的 run() 主循环 =====
            // 见 1.2 节详细展开
        }
    });
    parseThread.setUncaughtExceptionHandler(handler);
    parseThread.setName(String.format("destination = %s , binlog parser", destination));
    parseThread.start();
}
```

> **这一步在干什么？**
>
> `start()` 做了两件事：（1）初始化 `EventTransactionBuffer` 事务缓冲区，设置 flush 回调——当一个完整事务积累完毕后，回调函数负责将事务投递到 Sink 并持久化位点；（2）创建 `parseThread` 线程并启动它。这个线程就是整个 Parse 模块的"引擎"——它在 `run()` 方法中执行一个 **while(running) 无限循环**，不断连接 MySQL、dump binlog、解析、投递，直到被 `stop()` 停止。

### 1.2 parseThread.run() 主循环 —— 12个关键步骤完整源码

这是整个 Parse 模块最核心的方法，约 200 行代码。我将完整展示并逐步解读：

```java
// AbstractEventParser.java - parseThread 的 run() 方法
public void run() {
    MDC.put("destination", String.valueOf(destination));
    while (running) {
        try {
            // ============ 步骤①：构造数据库连接 ============
            // 构造主连接（用于dump binlog）和探测连接（用于心跳探测）
            erosaConnection = buildErosaConnection();
            erosaConnection.connect();

            long queryServerId = erosaConnection.queryServerId();
            if (queryServerId != 0) {
                serverId = queryServerId;
            }

            // ============ 步骤②：启动心跳机制 ============
            startHeartBeat();

            // ============ 步骤③：执行 preDump 准备操作 ============
            // 由子类实现，MysqlEventParser 在这里做：
            //   - 构建metaConnection
            //   - 调用 preDump(erosaConnection) 做会话设置
            preDump(erosaConnection);

            // ============ 步骤④：重新连接以应用设置 ============
            erosaConnection.reconnect();

            // ============ 步骤⑤：查询 serverId ============
            // 重新连接后再次查询（设置可能影响结果）
            queryServerId = erosaConnection.queryServerId();
            if (queryServerId != 0) {
                serverId = queryServerId;
            }

            // ============ 步骤⑥：查找起始位点 ============
            // 这是非常关键的一步——决定从 binlog 的哪个位置开始 dump
            EntryPosition position = findStartPosition(erosaConnection);
            final EntryPosition startPosition = position;
            if (startPosition == null) {
                throw new CanalParseException(
                    "find start position error, destination: " + destination);
            }

            // 如果连接支持GTID并且配置了GTID模式
            // 需要额外处理GTID集合
            // ... GTID处理逻辑 ...

            // ============ 步骤⑦：处理表结构元数据 ============
            // 在正式dump之前，需要回滚表结构到起始位点对应的时间
            // 这样解析binlog时才能正确映射字段
            processTableMeta(startPosition);

            // ============ 步骤⑧：重新连接（应用所有设置后的干净连接）============
            erosaConnection.reconnect();

            // ============ 步骤⑨：构建 SinkHandler ============
            // SinkHandler 是 dump 过程中每收到一个 event 的回调处理器
            // 并行模式和串行模式的 handler 不同
            MultiStageCoprocessor coprocessor = null;
            if (parallel) {
                // 并行模式：使用 Disruptor 流水线
                coprocessor = buildMultiStageCoprocessor();
                // 设置起始位点信息，用于后续的位点追踪
                if (isGTIDMode()) {
                    ((MysqlMultiStageCoprocessor) coprocessor)
                        .setStartPosition(startPosition);
                }
                coprocessor.start();
            } else {
                // 串行模式：构建同步 SinkHandler
                // handler 在 dump 循环中被同步调用
            }

            // ============ 步骤⑩：执行 dump ============
            // 这是整个Parse模块真正开始工作的地方
            // dump() 方法会阻塞在 while 循环中不断读取 binlog
            if (parallel) {
                // 并行dump：数据通过Disruptor流水线异步处理
                erosaConnection.dump(startPosition.getJournalName(),
                    startPosition.getPosition(), coprocessor);
            } else {
                // 串行dump：数据通过SinkFunction同步处理
                erosaConnection.dump(startPosition.getJournalName(),
                    startPosition.getPosition(), sinkHandler);
            }
        } catch (TableIdNotFoundException e) {
            // ============ 步骤⑪：TableIdNotFoundException 特殊处理 ============
            // 当遇到 TableMap 缺失（通常是位点跳过了 TableMap 事件）
            // 需要回退位点，从更早的位置重新开始dump
            exception = null;  // 不记为致命异常
            needTransactionPosition.compareAndSet(false, true);
            logger.error("encounter TableIdNotFound, will retry with transaction position");
        } catch (Throwable e) {
            // ============ 步骤⑫：异常处理与随机退避重试 ============
            // 通用异常处理：记录异常，报警，等待后重试
            exception = ExceptionUtils.getRootCause(e);
            if (exception instanceof CanalParseException
                && exception.getMessage().contains("errno = 1236")) {
                // binlog 被删除的特殊情况
                dumpErrorCount.incrementAndGet();
            }

            if (isNeedContinue()) {
                logger.error("parse error, will retry after {} ms",
                    RECONNECT_DELAY, e);
                // 报警
                if (alarmHandler != null) {
                    alarmHandler.sendAlarm(destination,
                        ExceptionUtils.getFullStackTrace(e));
                }
            } else {
                // running = false, 不再重试
                logger.info("parse thread exit, destination: {}", destination);
            }
        } finally {
            // 清理资源
            try {
                if (erosaConnection != null) {
                    erosaConnection.disconnect();
                }
            } catch (IOException e) {
                logger.error("disconnect error", e);
            }
            // 停止并行协处理器
            if (coprocessor != null && coprocessor.isStart()) {
                coprocessor.stop();
            }

            // 随机退避重试
            if (running) {
                // 随机等待 1000~3000ms 后重试
                long sleepTime = RECONNECT_DELAY
                    + (long) (RECONNECT_DELAY * Math.random());
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    MDC.remove("destination");
}
```

> **这一步在干什么？**
>
> `run()` 方法是一个 `while(running)` 无限循环，每轮循环都会尝试完整执行一次 dump 流程。如果中途出现任何异常（网络断开、MySQL 宕机、binlog 被删除等），catch 块会记录异常并在 finally 块中随机退避后重试。这种设计保证了 Parse 模块具有**自愈能力**——无论遇到什么异常，只要 `running=true`，它就会不断重试直到恢复正常。

让我们逐步解读 12 个关键步骤：

#### 步骤①：构造数据库连接 `buildErosaConnection()`

```java
// MysqlEventParser.java
protected ErosaConnection buildErosaConnection() {
    return buildMysqlConnection(this.runningInfo);
}

private MysqlConnection buildMysqlConnection(AuthenticationInfo runningInfo) {
    MysqlConnection connection = new MysqlConnection(
        runningInfo.getAddress(),                    // MySQL地址
        runningInfo.getUsername(),                    // 用户名
        runningInfo.getPassword(),                   // 密码
        connectionCharsetNumber,                     // 字符集编号(默认33=utf8)
        runningInfo.getDefaultDatabaseName()         // 默认数据库
    );
    connection.getConnector().setReceiveBufferSize(receiveBufferSize);  // 接收缓冲区
    connection.getConnector().setSendBufferSize(sendBufferSize);        // 发送缓冲区
    connection.getConnector().setSoTimeout(defaultConnectionTimeoutInSeconds * 1000); // Socket超时
    connection.setCharset(connectionCharset);        // 字符集名称(默认UTF-8)
    connection.setReceivedBinlogBytes(receivedBinlogBytes);  // 统计接收字节数
    // 设置 slaveId
    connection.setSlaveId(this.slaveId);
    return connection;
}
```

> **关键设计分析：** `buildErosaConnection()` 是模板方法——`AbstractEventParser` 定义接口，`MysqlEventParser` 提供 MySQL 实现。如果未来要支持其他数据库（如 MariaDB、PostgreSQL），只需新增子类并重写此方法。

#### 步骤②：启动心跳机制 `startHeartBeat()`

心跳机制是 Canal HA 的基础，详见第七阶段。这里只需知道它启动了一个 Timer 定时任务，周期性检测 MySQL 连接是否存活。

#### 步骤③：preDump 准备操作

```java
// MysqlEventParser.java
protected void preDump(ErosaConnection connection) {
    // 1. 构建 metaConnection（用于执行 show 命令查询表结构）
    if (metaConnection != null) {
        metaConnection.disconnect();
    }
    metaConnection = buildMysqlConnection(this.runningInfo);
    try {
        metaConnection.connect();
    } catch (IOException e) {
        throw new CanalParseException(e);
    }

    // 2. 如果有 TableMetaTSDB（时间序列数据库，存储表结构历史变更）
    if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
        ((DatabaseTableMeta) tableMetaTSDB).setConnection(metaConnection);
        ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
        ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
        ((DatabaseTableMeta) tableMetaTSDB).setSnapshotInterval(
            tsdbSnapshotInterval);
        ((DatabaseTableMeta) tableMetaTSDB).setSnapshotExpire(
            tsdbSnapshotExpire);
        ((DatabaseTableMeta) tableMetaTSDB).init(destination);
    }

    // 3. 构建 TableMetaCache
    if (tableMetaCache == null) {
        tableMetaCache = new TableMetaCache(metaConnection, tableMetaTSDB);
    }
    ((TableMetaCache) tableMetaCache).setFilter(eventFilter);
    ((TableMetaCache) tableMetaCache).setBlackFilter(eventBlackFilter);

    // 4. 将 TableMetaCache 设置到 binlogParser（LogEventConvert）中
    if (binlogParser != null && binlogParser instanceof LogEventConvert) {
        ((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
    }
}
```

> **这一步在干什么？**
>
> `preDump()` 在正式 dump 之前做三件准备工作：（1）创建 `metaConnection`，这是一个独立的 MySQL 连接，专门用于执行 `SHOW CREATE TABLE` 等 DDL 查询来获取表结构元数据——不能复用 dump 连接，因为 dump 连接在 dump 期间被 binlog 数据流占满；（2）初始化 `TableMetaTSDB`（如果启用），它是一个时间序列数据库，记录每张表的结构变更历史，用于在任意位点准确还原当时的表结构；（3）构建 `TableMetaCache`，它缓存当前的表结构信息供 `LogEventConvert` 在解析行数据时使用。

#### 步骤⑥：查找起始位点 `findStartPosition()`

这是位点决策的入口，详见第四阶段的完整决策树分析。

#### 步骤⑦：处理表结构元数据 `processTableMeta()`

```java
// AbstractMysqlEventParser.java
protected void processTableMeta(EntryPosition position) {
    if (tableMetaTSDB != null) {
        // 将 TSDB 中的表结构回滚到 startPosition 对应的时间点
        // 这样在解析从该位点开始的 binlog 时，表结构是正确的
        if (position.getTimestamp() != null && position.getTimestamp() > 0) {
            tableMetaTSDB.rollback(position);
        } else {
            // 没有时间戳信息时，使用全量快照
            tableMetaTSDB.snapshot();
        }
    }
}
```

> **这一步在干什么？**
>
> 表结构元数据处理是 binlog 解析的前置依赖。binlog 中的 RowsEvent 只包含字段的二进制值，不包含字段名和类型——这些信息需要通过 `SHOW CREATE TABLE` 获取。但问题是：如果 binlog 回溯了（比如 Canal 重启从昨天的位点开始），而昨天到今天之间表结构发生了 DDL 变更，那当前的 `SHOW CREATE TABLE` 结果就和 binlog 中的行数据不匹配了。`TableMetaTSDB` 解决了这个问题——它记录了表结构的每一次变更，可以精确回滚到任意时间点的表结构。

#### 步骤⑨：构建 SinkHandler（串行模式）

```java
// AbstractEventParser.java
// 串行模式下的 SinkHandler
final SinkFunction sinkHandler = new SinkFunction<EVENT>() {
    private LogPosition lastPosition;

    public boolean sink(EVENT event) {
        try {
            CanalEntry.Entry entry = parseAndProfilingIfNecessary(event,
                false /*非并行*/);
            if (!running) {
                return false; // 停止信号
            }
            if (entry != null) {
                // 异常字段置空（表示心跳正常，连接存活）
                exception = null;
                // 投递到事务缓冲区
                transactionBuffer.add(entry);
            }
            return running;
        } catch (TableIdNotFoundException e) {
            throw e;
        } catch (Throwable e) {
            if (e.getCause() instanceof TableIdNotFoundException) {
                throw (TableIdNotFoundException) e.getCause();
            }
            // 重新抛出以触发外层的异常处理
            throw new CanalParseException(e);
        }
    }
};
```

> **这一步在干什么？**
>
> 串行模式的 SinkHandler 是一个简单的回调函数：每收到一个 binlog event，先调用 `parseAndProfilingIfNecessary()` 解析成 `CanalEntry.Entry`，然后投递到 `EventTransactionBuffer` 事务缓冲区。注意 `exception = null` 这一行——它不只是清除异常，还承担着**心跳存活探针**的作用（见下文分析）。

### 1.3 事务缓冲区 flush 回调的深度解读

当 `EventTransactionBuffer` 积累完一个完整事务后，会调用 `TransactionFlushCallback.flush()`。让我们深入解读这个回调中的两个关键方法：

#### consumeTheEventAndProfilingIfNecessary()

```java
// AbstractEventParser.java
protected boolean consumeTheEventAndProfilingIfNecessary(
        List<CanalEntry.Entry> entrys) throws CanalSinkException, InterruptedException {
    long startTs = -1;
    boolean enabled = getProfilingEnabled();
    if (enabled) {
        startTs = System.nanoTime();
    }

    // ★ 核心：将事务投递到 EventSink
    boolean result = eventSink.sink(entrys,
        (runningInfo == null) ? null : runningInfo.getAddress(),
        destination);

    if (enabled) {
        // 性能统计
        long endTs = System.nanoTime();
        profilingCount.incrementAndGet();
        profilingSumTime.addAndGet(endTs - startTs);
    }
    return result;
}
```

> **这一步在干什么？**
>
> 这个方法将完整的事务（一组 `CanalEntry.Entry`）投递给 `CanalEventSink`（通常是 `EntryEventSink`），Sink 会将数据写入 `CanalEventStore`（通常是 `MemoryEventStoreWithBuffer` RingBuffer）。如果 RingBuffer 满了，`sink()` 方法会阻塞等待——这就是背压机制的体现。同时方法支持性能 profiling，统计投递耗时。

#### buildLastTransactionPosition()

```java
// AbstractEventParser.java
protected LogPosition buildLastTransactionPosition(List<CanalEntry.Entry> entrys) {
    // 从事务的 Entry 列表中，找到最后一条（TRANSACTIONEND）的位点
    for (int i = entrys.size() - 1; i >= 0; i--) {
        CanalEntry.Entry entry = entrys.get(i);
        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND
            || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
            // 构建 LogPosition
            LogPosition logPosition = new LogPosition();
            EntryPosition position = new EntryPosition();
            position.setJournalName(entry.getHeader().getLogfileName());
            position.setPosition(entry.getHeader().getLogfileOffset());
            position.setTimestamp(entry.getHeader().getExecuteTime());
            // serverId 用于 HA 场景的源标识
            position.setServerId(entry.getHeader().getServerId());
            logPosition.setPostion(position);

            // 记录来源身份信息（IP:port）
            LogIdentity identity = new LogIdentity(
                runningInfo.getAddress(), -1L);
            logPosition.setIdentity(identity);
            return logPosition;
        }
    }
    return null;
}
```

> **这一步在干什么？**
>
> 位点构建的核心逻辑——从事务的 Entry 列表中提取最后一条 `TRANSACTIONEND`（或 `TRANSACTIONBEGIN`）的位点信息，包括 binlog 文件名、偏移量、时间戳、serverId。这个位点随后会通过 `logPositionManager.persistLogPosition()` 持久化。

### 1.4 位点安全设计：为什么只在事务 END 后才持久化？

```
                      位点持久化时机
                      ═══════════════

binlog事件序列:  BEGIN → DML1 → DML2 → DML3 → END → BEGIN → DML4 → END
                   │                              │                    │
                   │                              ▼                    ▼
                   │                        持久化位点A           持久化位点B
                   │
                   X 不在这里持久化

假设在 DML2 之后持久化了位点，然后 Canal 崩溃：
  - 重启后从 DML2 之后开始 dump
  - DML1 丢失！事务不完整！下游只收到半个事务！

所以必须在 TRANSACTIONEND 之后才持久化：
  - 即使 Canal 在 DML2 之后崩溃
  - 重启后从上一个 END（或BEGIN）的位点开始
  - 整个事务从头重新解析和投递
  - 下游收到完整事务（幂等消费即可）
```

**关键设计分析：** 这是 Canal 保证**事务原子性投递**的核心设计。位点只在事务边界（END/BEGIN）后持久化，保证 Canal 崩溃重启后，要么完整重放一个事务，要么完全跳过一个事务——永远不会出现半个事务的情况。这要求下游消费者实现**幂等消费**（同一个事务可能被重复投递），但保证了不丢数据。

### 1.5 exception 字段的双重作用

```java
// AbstractEventParser.java
protected volatile Throwable exception = null;

// 作用1：记录最近一次异常（用于外部监控）
catch (Throwable e) {
    exception = ExceptionUtils.getRootCause(e);
    // ...
}

// 作用2：心跳存活探针（SinkHandler 中）
public boolean sink(EVENT event) {
    CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, false);
    if (entry != null) {
        exception = null;  // ★ 收到有效event，清除异常 → 证明连接存活
        transactionBuffer.add(entry);
    }
    return running;
}

// 心跳探测时检查 exception 字段
// HeartBeatHAController.java
public void onFailed(Throwable e) {
    failedTimes.incrementAndGet();
    // 如果 exception != null 且 failedTimes 超阈值，触发 HA 切换
}
```

> **这一步在干什么？**
>
> `exception` 字段身兼二职：（1）作为**异常记录器**，保存最近一次解析异常，供外部监控系统查询；（2）作为**心跳存活探针**——每次成功解析一个 event 时都会将 `exception` 置为 `null`。心跳检测机制通过检查 `exception` 是否为 `null` 来判断 binlog dump 是否正常进行。如果 `exception` 长时间不为 `null`（连续多次心跳检测都发现有异常），说明 dump 卡住了或者连接已断，就会触发 HA 切换。

### 1.6 TableIdNotFoundException 特殊处理

```java
catch (TableIdNotFoundException e) {
    exception = null;  // ★ 不记为致命异常
    needTransactionPosition.compareAndSet(false, true);
    logger.error("encounter TableIdNotFound, will retry with transaction position");
}
```

> **这一步在干什么？**
>
> MySQL binlog 中的 RowsEvent 通过 `tableId`（一个数字）引用表结构，表结构定义在前面的 TableMapEvent 中。如果 Canal 从一个 binlog 中间位置开始 dump，可能跳过了 TableMapEvent 但遇到了引用它的 RowsEvent，导致 `TableIdNotFoundException`。
>
> 处理策略：设置 `needTransactionPosition = true`，下一轮重试时，`findStartPosition()` 会返回上一个完整事务的**起始位置**（TRANSACTIONBEGIN）而不是上次持久化的位置——这样就能完整地重放事务，包括其中的 TableMapEvent。
>
> 注意 `exception = null`——这个异常不算"致命异常"，不应该触发心跳报警或 HA 切换，只是一个需要重试的临时状态。

### 1.7 随机退避重试机制

```java
finally {
    // ... 清理资源 ...

    if (running) {
        // RECONNECT_DELAY = 1000ms (默认)
        // 随机等待 1000 ~ 2000ms
        long sleepTime = RECONNECT_DELAY
            + (long) (RECONNECT_DELAY * Math.random());
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
```

> **关键设计分析：** 退避时间引入了随机因子 `Math.random()`，这是**防止惊群效应**的经典设计。假设有 10 个 Canal 实例同时失去和 MySQL Master 的连接，如果它们全用固定间隔重试，会在同一时刻同时重连，造成 MySQL 的连接风暴。随机退避让它们错开重连时间，减轻 MySQL 的瞬时压力。

---

## 第二阶段：MysqlConnection —— 伪装成MySQL Slave的完整协议

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/driver/mysql/MysqlConnection.java` 及 `dbsync/src/main/java/com/alibaba/otter/canal/parse/driver/mysql/MysqlConnector.java`

MysqlConnection 是 Canal 伪装成 MySQL Slave 的核心类。它封装了从 TCP 连接建立、MySQL 握手认证、到发送 COM_BINLOG_DUMP 命令开始接收 binlog 的全部协议细节。

### 2.1 connect() —— TCP连接建立 + MySQL握手认证

```java
// MysqlConnector.java
public void connect() throws IOException {
    // 1. 建立 TCP 连接
    SocketChannel channel = SocketChannel.open();
    channel.socket().setKeepAlive(true);
    channel.socket().setReuseAddress(true);
    channel.socket().setSoTimeout(soTimeout);
    channel.socket().setTcpNoDelay(true);
    channel.socket().setReceiveBufferSize(receiveBufferSize);
    channel.socket().setSendBufferSize(sendBufferSize);
    channel.socket().connect(address, connTimeout);

    // 2. 读取 MySQL Server 的 Handshake 包
    // MySQL 协议规定：连接建立后，Server 先发送一个 Handshake 包
    // 包含：协议版本、server版本号、连接ID、auth-plugin-data（盐值）、能力标志等
    negotiatePacket = MysqlPacketManager.readPacket(channel);
    HandshakeInitializationPacket handshake =
        new HandshakeInitializationPacket();
    handshake.fromBytes(negotiatePacket);

    // 3. 客户端认证
    // 构建 ClientAuthenticationPacket
    // 根据 Handshake 包中的能力标志和 auth-plugin-data，
    // 使用 MySQL 协议规定的密码加密算法加密密码
    ClientAuthenticationPacket clientAuth = new ClientAuthenticationPacket();
    clientAuth.setCharsetNumber(charsetNumber);
    clientAuth.setUsername(username);
    // MySQL 4.1+ 使用 SHA1 双重加密
    clientAuth.setPassword(password);
    clientAuth.setServerCapabilities(handshake.serverCapabilities);
    clientAuth.setAuthPluginData(
        joinAndCreateScrumbleBuff(handshake)); // 拼接两段 auth-plugin-data
    clientAuth.setDatabaseName(defaultSchema);
    clientAuth.setSeed(joinAndCreateScrumbleBuff(handshake));

    // 4. 发送认证包
    byte[] clientAuthPkg = clientAuth.toBytes();
    MysqlPacketManager.writePacket(channel, clientAuthPkg);

    // 5. 读取认证响应
    byte[] authResult = MysqlPacketManager.readPacket(channel);
    // 检查是否认证成功（首字节 0x00 = OK, 0xFF = Error）
    if (authResult[0] == (byte) 0xFF) {
        ErrorPacket error = new ErrorPacket();
        error.fromBytes(authResult);
        throw new IOException("Authentication failed: "
            + error.message + ", errno=" + error.errorNumber);
    }

    // 认证成功，保存连接
    this.channel = channel;
    this.connected = true;
}
```

> **这一步在干什么？**
>
> `connect()` 完成了 MySQL 客户端连接的全部握手流程。MySQL 协议的握手分三步：（1）Server 发送 Handshake 包，包含协议版本、服务器版本、连接 ID、认证盐值（auth-plugin-data）；（2）Client 发送 Authentication 包，包含用户名、加密后的密码、字符集、数据库名；（3）Server 返回 OK 或 Error。Canal 在这里完全模拟了一个标准的 MySQL 客户端行为。

### 2.2 updateSettings() —— 10条SQL会话设置的逐条解读

```java
// MysqlConnection.java
private void updateSettings() throws IOException {
    try {
        // ① 设置等待超时：28800秒（8小时）
        // 防止 MySQL 因为空闲超时断开 dump 连接
        update("set wait_timeout=28800");

        // ② 设置交互等待超时
        // 与 wait_timeout 类似，但用于交互式连接
        update("set interactive_timeout=28800");

        // ③ 设置网络读超时
        // MySQL Server 从 dump 连接读数据的超时
        // 设置为极大值避免超时
        update("set net_read_timeout=28800");

        // ④ 设置网络写超时
        // MySQL Server 向 dump 连接写数据的超时
        update("set net_write_timeout=28800");

        // ⑤ 设置字符集为 binary
        // 重要！binlog dump 必须用 binary 字符集
        // 避免 MySQL 做字符集转换导致数据损坏
        update("set names 'binary'");

        // ⑥ 设置 binlog checksum
        // 告诉 MySQL Server，Canal（作为slave）支持处理 checksum
        // MySQL 5.6.6+ 默认启用 binlog checksum（CRC32）
        update("set @master_binlog_checksum= @@global.binlog_checksum");

        // ⑦ 设置 slave UUID
        // 每个 slave 需要一个唯一的 UUID
        // 避免 MySQL Master 因为 UUID 冲突拒绝连接
        update("set @slave_uuid=uuid()");

        // ⑧ 设置 heartbeat 周期
        // MySQL 主从复制协议的心跳间隔
        // 30秒发一次心跳，确保 dump 连接活跃
        update("SET @master_heartbeat_period="
            + MASTER_HEARTBEAT_PERIOD_SECONDS * 1000000000L);

        // ⑨ 设置 mariadb slave capability（如果是 MariaDB）
        // MariaDB 和 MySQL 的协议有差异
        if (isMariaDB()) {
            update("SET @mariadb_slave_capability='"
                + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
        }

        // ⑩ 设置 SQL 模式
        // 清除可能影响解析的 SQL 模式
        update("SET @@session.sql_mode = ''");

    } catch (Exception e) {
        logger.warn("update settings error", e);
    }
}
```

> **这一步在干什么？**
>
> 这 10 条 SQL 设置了 dump 连接的会话参数，每一条都有明确的目的：
>
> | 序号 | SQL | 目的 |
> |-----|-----|------|
> | ① | `set wait_timeout=28800` | 防止 MySQL 因空闲断连（dump 期间可能长时间无写入） |
> | ② | `set interactive_timeout=28800` | 同上，交互连接版本 |
> | ③ | `set net_read_timeout=28800` | 防止 MySQL 读超时断连 |
> | ④ | `set net_write_timeout=28800` | 防止 MySQL 写超时断连 |
> | ⑤ | `set names 'binary'` | **关键！** 避免字符集转换破坏 binlog 二进制数据 |
> | ⑥ | `set @master_binlog_checksum` | 声明支持 binlog checksum 校验（MySQL 5.6.6+） |
> | ⑦ | `set @slave_uuid=uuid()` | 避免 slave UUID 冲突被 Master 拒绝 |
> | ⑧ | `SET @master_heartbeat_period` | MySQL 级心跳，30 秒无 event 时 Master 发心跳包 |
> | ⑨ | MariaDB slave capability | 兼容 MariaDB 协议差异 |
> | ⑩ | `SET @@session.sql_mode = ''` | 清除可能影响 SQL 解析的 mode 设置 |

### 2.3 loadBinlogChecksum() —— checksum 协商

```java
// MysqlConnection.java
private void loadBinlogChecksum() {
    // 查询 MySQL Server 配置的 binlog checksum 算法
    ResultSetPacket rs = query("select @@global.binlog_checksum");
    List<String> columnValues = rs.getFieldValues();
    if (columnValues != null && columnValues.size() >= 1
        && columnValues.get(0) != null
        && columnValues.get(0).toUpperCase().equals("CRC32")) {
        binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_CRC32;
    } else {
        binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_OFF;
    }
}
```

> **这一步在干什么？**
>
> MySQL 5.6.6+ 引入了 binlog checksum 机制——在每个 binlog event 末尾附加 4 字节 CRC32 校验和。Canal 作为 slave，需要知道 Master 是否启用了 checksum，以便在解析 event 时正确处理尾部的 4 字节（是数据还是校验和？）。如果 checksum 启用，LogDecoder 在解析时会自动跳过尾部的 4 字节校验和。

### 2.4 sendRegisterSlave() —— COM_REGISTER_SLAVE (0x15) 包体字节级构造

```java
// MysqlConnection.java
private void sendRegisterSlave() throws IOException {
    // 构造 COM_REGISTER_SLAVE 命令包
    RegisterSlaveCommandPacket cmd = new RegisterSlaveCommandPacket();
    cmd.reportHost = address.getHostString();
    cmd.reportPort = address.getPort();
    cmd.reportUser = username;
    cmd.reportPasswd = password;
    cmd.serverId = this.slaveId;

    byte[] cmdBody = cmd.toBytes();
    // 发送注册命令
    MysqlPacketManager.writePacket(channel, cmdBody);
    // 读取响应（期望 OK 包）
    byte[] response = MysqlPacketManager.readPacket(channel);
    // 检查响应状态
    // ...
}
```

COM_REGISTER_SLAVE 的包体格式如下（字节级别）：

```
COM_REGISTER_SLAVE 包体结构
════════════════════════════════════════════════════

偏移  长度    字段              说明
────  ────    ────              ────
 0    1B      command           0x15 (COM_REGISTER_SLAVE 命令标识)
 1    4B      server-id         Slave 的 server-id（小端序 uint32）
 5    1B      hostname_length   hostname 字符串长度
 6    nB      hostname          主机名（ASCII）
6+n   1B      username_length   用户名字符串长度
7+n   mB      username          用户名（ASCII）
7+n+m 1B      password_length   密码字符串长度
8+n+m pB      password          密码（ASCII）
8+n+m+p 2B    port              端口号（小端序 uint16）
10+..  4B     replication_rank  复制排名（固定为 0）
14+..  4B     master-id         Master server-id（固定为 0）

示例（十六进制）:
  15                          -- command = COM_REGISTER_SLAVE
  64 00 00 00                 -- server-id = 100 (小端)
  09                          -- hostname长度 = 9
  6C 6F 63 61 6C 68 6F 73 74 -- hostname = "localhost"
  05                          -- username长度 = 5
  63 61 6E 61 6C              -- username = "canal"
  06                          -- password长度 = 6
  63 61 6E 61 6C 21           -- password = "canal!"
  0E 27                       -- port = 10000 (小端)
  00 00 00 00                 -- replication_rank = 0
  00 00 00 00                 -- master-id = 0
```

```java
// RegisterSlaveCommandPacket.java
public byte[] toBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // command
    out.write(new byte[] { (byte) 0x15 });
    // server-id (小端 uint32)
    ByteHelper.writeUnsignedIntLittleEndian(serverId, out);
    // hostname
    out.write((byte) reportHost.getBytes().length);
    out.write(reportHost.getBytes());
    // username
    out.write((byte) reportUser.getBytes().length);
    out.write(reportUser.getBytes());
    // password
    out.write((byte) reportPasswd.getBytes().length);
    out.write(reportPasswd.getBytes());
    // port (小端 uint16)
    ByteHelper.writeUnsignedShortLittleEndian(reportPort, out);
    // replication rank (0)
    ByteHelper.writeUnsignedIntLittleEndian(0, out);
    // master-id (0)
    ByteHelper.writeUnsignedIntLittleEndian(0, out);
    return out.toByteArray();
}
```

> **这一步在干什么？**
>
> `COM_REGISTER_SLAVE` 是 MySQL 主从复制协议的一部分——Slave 在开始 dump binlog 之前，需要先向 Master "注册"自己。这个命令告诉 Master："我是一个 Slave，我的 server-id 是 X，我的地址是 host:port"。Master 收到后会记录这个 Slave 信息，可以通过 `SHOW SLAVE HOSTS` 命令查看已注册的 Slave 列表。Canal 在这里完全按照 MySQL 协议规范构造了二进制包体，字段顺序和编码方式严格遵循协议定义。

### 2.5 sendBinlogDump() —— COM_BINLOG_DUMP (0x12) 包体字节级构造

```java
// MysqlConnection.java
private void sendBinlogDump(String binlogFileName, long binlogPosition)
        throws IOException {
    BinlogDumpCommandPacket cmd = new BinlogDumpCommandPacket();
    cmd.binlogFileName = binlogFileName;
    cmd.binlogPosition = binlogPosition;
    cmd.slaveServerId = this.slaveId;

    byte[] cmdBody = cmd.toBytes();
    // 发送 dump 命令
    MysqlPacketManager.writePacket(channel, cmdBody);
}
```

COM_BINLOG_DUMP 的包体格式如下（字节级别）：

```
COM_BINLOG_DUMP 包体结构
════════════════════════════════════════════════════

偏移  长度    字段              说明
────  ────    ────              ────
 0    1B      command           0x12 (COM_BINLOG_DUMP 命令标识)
 1    4B      binlog_position   开始dump的位点（小端序 uint32）
 5    2B      flags             标志位：
                                  0x00 = 正常模式
                                  0x01 = BINLOG_DUMP_NON_BLOCK
                                         （无数据时立即返回EOF，不阻塞等待）
 7    4B      server-id         Slave 的 server-id（小端序 uint32）
11    nB      binlog_filename   Binlog 文件名（不含长度前缀，直到包尾）

示例（十六进制）:
  12                          -- command = COM_BINLOG_DUMP
  6E 00 00 00                 -- position = 110 (小端)
  00 00                       -- flags = 0 (阻塞模式)
  64 00 00 00                 -- server-id = 100 (小端)
  6D 79 73 71 6C 2D 62 69    -- "mysql-bi"
  6E 2E 30 30 30 30 30 31    -- "n.000001"
```

```java
// BinlogDumpCommandPacket.java
public byte[] toBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // command
    out.write(new byte[] { (byte) 0x12 });
    // binlog position (小端 uint32)
    ByteHelper.writeUnsignedIntLittleEndian(binlogPosition, out);
    // flags (2字节, 通常为0)
    out.write(new byte[] { 0x00, 0x00 });
    // slave server-id (小端 uint32)
    ByteHelper.writeUnsignedIntLittleEndian(slaveServerId, out);
    // binlog filename (直到包尾)
    out.write(binlogFileName.getBytes());
    return out.toByteArray();
}
```

> **关键设计分析：**
>
> `flags` 字段通常设为 `0x00`（阻塞模式）——这意味着当 Master 没有新的 binlog 事件时，连接不会断开，而是会阻塞等待直到有新事件产生。这正是实时 binlog 订阅的关键：Canal 发送 `COM_BINLOG_DUMP` 后，就像打开了一个"水龙头"，binlog 事件源源不断地流过来，直到连接断开。

### 2.6 COM_BINLOG_DUMP_GTID (0x1e) 的字节格式

Canal 还支持基于 GTID 的 binlog dump，使用 `COM_BINLOG_DUMP_GTID` 命令：

```
COM_BINLOG_DUMP_GTID 包体结构
════════════════════════════════════════════════════

偏移  长度    字段                说明
────  ────    ────                ────
 0    1B      command             0x1e (COM_BINLOG_DUMP_GTID)
 1    2B      flags               标志位（小端 uint16）
                                    0x00 = 正常
                                    0x04 = USING_GTID（使用GTID模式）
 3    4B      server-id           Slave server-id（小端 uint32）
 7    4B      filename_length     Binlog文件名长度（小端 uint32）
11    nB      filename            Binlog文件名
11+n  8B      binlog_position     起始位点（小端 uint64）
19+n  4B      gtid_data_length    GTID数据长度（小端 uint32）
23+n  mB      gtid_data           GTID集合序列化数据
                                    包含: n_sids(8B) + 每个SID条目:
                                      uuid(16B) + n_intervals(8B) +
                                      每个interval: start(8B) + end(8B)
```

```java
// BinlogDumpGTIDCommandPacket.java
public byte[] toBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // command
    out.write(0x1e);
    // flags: 0x04 = BINLOG_THROUGH_GTID
    ByteHelper.writeUnsignedShortLittleEndian(0x04, out);
    // server-id
    ByteHelper.writeUnsignedIntLittleEndian(slaveServerId, out);
    // binlog filename length + filename
    ByteHelper.writeUnsignedIntLittleEndian(
        binlogFileName != null ? binlogFileName.length() : 0, out);
    if (binlogFileName != null) {
        out.write(binlogFileName.getBytes());
    }
    // binlog position (uint64)
    ByteHelper.writeUnsignedInt64LittleEndian(binlogPosition, out);
    // GTID data
    byte[] gtidData = gtidSet.encode();
    ByteHelper.writeUnsignedIntLittleEndian(gtidData.length, out);
    out.write(gtidData);
    return out.toByteArray();
}
```

> **GTID 模式 vs 文件位点模式：**
>
> | 特性 | 文件位点模式 (0x12) | GTID 模式 (0x1e) |
> |-----|---------------------|-------------------|
> | 定位方式 | binlog文件名 + 偏移量 | GTID 集合 |
> | HA 切换 | 需要手动计算新 Master 的对应位点 | 自动定位，GTID 全局唯一 |
> | 复杂度 | 简单，但不支持自动故障转移 | 复杂，但天然支持 Master 切换 |
> | MySQL版本要求 | 所有版本 | MySQL 5.6.5+ |

### 2.7 dump() 主方法 —— 完整流程

```java
// MysqlConnection.java
public void dump(String binlogFileName, Long binlogPosition,
                 SinkFunction func) throws IOException {
    // ① 会话设置（10条SQL）
    updateSettings();

    // ② 加载 checksum 配置
    loadBinlogChecksum();

    // ③ 注册为 Slave
    sendRegisterSlave();

    // ④ 发送 COM_BINLOG_DUMP 命令
    sendBinlogDump(binlogFileName, binlogPosition);

    // ⑤ 创建 DirectLogFetcher（底层字节流读取器）
    DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
    fetcher.start(connector.getChannel());

    // ⑥ 创建 LogDecoder（二进制解码器）
    LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT,
        LogEvent.ENUM_END_EVENT);

    // ⑦ 创建 LogContext（解码上下文，持有 FormatDescriptionLogEvent）
    LogContext context = new LogContext();
    context.setLogPosition(new LogPosition(binlogFileName, binlogPosition));
    // 设置 FormatDescriptionLogEvent（binlog 文件头的第一个事件）
    context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));

    // ⑧ 循环 fetch + decode + callback
    while (fetcher.fetch()) {
        // 解码字节流为 LogEvent
        LogEvent event = decoder.decode(fetcher, context);

        if (event == null) {
            throw new CanalParseException("parse event error, not found event");
        }

        // 更新统计：接收字节数
        accumulateReceivedBytes(event);

        // 回调 SinkFunction
        if (!func.sink(event)) {
            break; // SinkFunction 返回 false，停止 dump
        }
    }
}
```

并行模式的 dump 方法签名不同，使用 `MultiStageCoprocessor` 替代 `SinkFunction`：

```java
// MysqlConnection.java
public void dump(String binlogFileName, Long binlogPosition,
                 MultiStageCoprocessor coprocessor) throws IOException {
    // ①②③④ 同上

    // ⑤⑥⑦ 同上

    // ⑧ 循环 fetch + publish 到 Disruptor
    while (fetcher.fetch()) {
        LogEvent event = decoder.decode(fetcher, context);
        if (event == null) {
            throw new CanalParseException("parse event error");
        }
        accumulateReceivedBytes(event);

        // 发布到 Disruptor RingBuffer（而不是同步回调）
        if (!coprocessor.publish(event)) {
            break;
        }
    }
}
```

> **这一步在干什么？**
>
> `dump()` 是整个数据链路的起点。它按顺序完成 5 个准备步骤后，进入一个 `while(fetcher.fetch())` 无限循环——每次循环从 Socket 读取一个 MySQL 网络包，解码成 `LogEvent`，然后通过回调（串行模式）或 Disruptor（并行模式）交给下游处理。这个循环只有在以下情况才会退出：（1）网络断开（`fetch()` 抛异常）；（2）收到 EOF 包（`fetch()` 返回 false）；（3）SinkFunction 返回 false（`coprocessor.publish()` 返回 false）；（4）Canal 停止（通过 `running` 标志）。

---

## 第三阶段：DirectLogFetcher —— 从Socket读取Binlog字节流

**源码位置**: `dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/DirectLogFetcher.java`

DirectLogFetcher 是连接 MySQL 网络层和 binlog 解码层的桥梁。它负责从 TCP Socket 读取 MySQL 网络包，处理分包、拼接、标志位检查等底层细节，向上层提供一个简洁的 `fetch()` 接口。

### 3.1 MySQL 网络包格式

```
MySQL 网络包（MySQL Protocol Packet）格式
═══════════════════════════════════════════════════════════

┌─────────────────┬───────────┬──────────────────────────┐
│ payload_length  │ sequence  │        payload           │
│   3 bytes       │  1 byte   │    N bytes               │
│  (小端序)        │  (包序号)  │   (实际数据)              │
└─────────────────┴───────────┴──────────────────────────┘

│◄── NET_HEADER_SIZE = 4 ──►│◄── payload_length ──────►│

- payload_length: 3字节小端序无符号整数，表示 payload 的长度
  最大值 = 0xFFFFFF = 16777215 = 16MB - 1
- sequence: 1字节包序号，用于检测丢包和乱序
- payload: 实际数据内容

超大包分片机制：
  当 payload > 16MB-1 时，MySQL 会将数据分成多个 16MB-1 的包发送
  每个分片的 payload_length = 0xFFFFFF
  最后一个分片的 payload_length < 0xFFFFFF
  接收方需要将所有分片拼接起来

Binlog 事件包的 payload 首字节标志位：
  0x00 = OK（正常数据包，后续是 binlog event 数据）
  0xFF = Error（MySQL 返回错误）
  0xFE = EOF（binlog dump 结束）
```

### 3.2 关键常量

```java
public class DirectLogFetcher {
    // MySQL 网络包头大小：3字节长度 + 1字节序号 = 4字节
    public static final int NET_HEADER_SIZE = 4;

    // MySQL 单个网络包的最大 payload 长度
    // 超过此长度的数据会被拆分为多个包
    public static final int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1); // 0xFFFFFF = 16MB-1

    // 首字节标志位
    private static final int PACKET_RESULT_OK = 0x00;
    private static final int PACKET_RESULT_EOF = 0xFE;
    private static final int PACKET_RESULT_ERROR = 0xFF;

    // 底层缓冲区
    private SocketChannel channel;
    private byte[] buffer;
    private int origin;   // 有效数据起始位置
    private int limit;    // 有效数据结束位置
}
```

### 3.3 fetch() 方法的完整解读

```java
// DirectLogFetcher.java
public boolean fetch() throws IOException {
    try {
        // =============== 第一步：读取包头（4字节）===============
        // 确保 buffer 中至少有 NET_HEADER_SIZE 字节
        ensureCapacity(channel, NET_HEADER_SIZE);

        // 解析包头
        // 前3字节 = payload_length（小端序）
        int netlen = getUint24(buffer, origin);
        // 第4字节 = 包序号
        int netnum = getUint8(buffer, origin + 3);

        // =============== 第二步：读取包体 ===============
        // 确保 buffer 中有完整的包（包头 + 包体）
        ensureCapacity(channel, NET_HEADER_SIZE + netlen);

        // 跳过包头
        origin += NET_HEADER_SIZE;
        // limit 指向包体末尾
        limit = origin + netlen;

        // =============== 第三步：检查标志位 ===============
        // payload 的首字节是状态标志
        int mark = getUint8(buffer, origin);

        if (mark == PACKET_RESULT_ERROR) {
            // 0xFF = Error 包
            // 解析错误码和错误消息
            error = true;
            int errno = getUint16(buffer, origin + 1);
            String sqlstate = new String(buffer, origin + 3, 5);
            String errmsg = new String(buffer, origin + 8, limit - origin - 8);
            throw new IOException("MySQL Error: errno=" + errno
                + ", sqlstate=" + sqlstate + ", errmsg=" + errmsg);
        }

        if (mark == PACKET_RESULT_EOF) {
            // 0xFE = EOF 包
            // binlog dump 正常结束（通常不会到这里，因为 dump 是阻塞的）
            return false;
        }

        // =============== 第四步：Semi-Sync 处理 ===============
        // MySQL 半同步复制（Semi-Synchronous Replication）
        // 在 binlog event 前面会多一个字节的 semi-sync 标志
        if (semiSyncEnabled) {
            // 跳过 semi-sync header（1字节标志 + 1字节ack请求）
            origin += 2;
        }

        // 跳过 OK 标志位（0x00）
        origin += 1;

        // =============== 第五步：处理超大包分片 ===============
        // 如果 payload_length == MAX_PACKET_LENGTH，说明数据被分片了
        // 需要继续读取后续分片并拼接
        if (netlen == MAX_PACKET_LENGTH) {
            // 递归或循环读取后续分片
            fetchMorePackets(netlen);
        }

        return true;

    } catch (SocketTimeoutException e) {
        // 读取超时（默认 25000ms）
        // 对于 binlog dump，超时通常意味着：
        // 1. MySQL Master 长时间没有写入（正常情况，等待即可）
        // 2. 网络问题（需要重连）
        close();
        throw e;
    }
}
```

> **这一步在干什么？**
>
> `fetch()` 方法的核心逻辑分为 5 步：
>
> 1. **读包头**：从 Socket 读取 4 字节，解析出 payload 长度和序号
> 2. **读包体**：根据长度读取完整的 payload
> 3. **检查标志位**：payload 首字节决定这个包的类型
>    - `0x00` = 正常数据（binlog event）
>    - `0xFF` = MySQL 返回错误
>    - `0xFE` = dump 结束
> 4. **Semi-Sync 处理**：如果启用了半同步复制，跳过额外的 2 字节头
> 5. **分片拼接**：如果包长达到 16MB-1 上限，说明数据被分片了，需要继续读取后续分片

### 3.4 ensureCapacity() —— 缓冲区扩容和网络读取

```java
private void ensureCapacity(SocketChannel channel, int needSize)
        throws IOException {
    // buffer 中剩余可用字节
    int available = limit - origin;

    if (available >= needSize) {
        return; // 已有足够数据，无需网络读取
    }

    // 数据不足，需要从网络读取
    // 如果 buffer 尾部空间不够，先压缩（将有效数据移到 buffer 头部）
    if (origin > 0 && (buffer.length - limit) < (needSize - available)) {
        System.arraycopy(buffer, origin, buffer, 0, available);
        limit = available;
        origin = 0;
    }

    // 如果 buffer 本身太小，扩容
    if (buffer.length < needSize) {
        byte[] newBuffer = new byte[needSize];
        System.arraycopy(buffer, origin, newBuffer, 0, available);
        buffer = newBuffer;
        limit = available;
        origin = 0;
    }

    // 从 Socket 读取数据直到满足 needSize
    int totalRead = available;
    while (totalRead < needSize) {
        int bytesRead = SocketChannelUtils.read(channel, buffer,
            limit, buffer.length - limit);
        if (bytesRead <= 0) {
            throw new IOException("Unexpected end of stream");
        }
        limit += bytesRead;
        totalRead += bytesRead;
    }
}
```

> **关键设计分析：**
>
> `ensureCapacity()` 实现了一个**自适应缓冲区**：当缓冲区中的数据不足时，先尝试"压缩"（将有效数据移到头部释放尾部空间），如果还不够再扩容。这种设计避免了频繁的数组分配和复制，同时能处理任意大小的 MySQL 网络包。压缩操作使用 `System.arraycopy` 实现内存移动，性能开销可控。

### 3.5 数据流转全景

```
MySQL Master
     │
     │  TCP Socket 字节流
     │  ┌──────┬──────┬──────┬──────┐
     │  │ pkt1 │ pkt2 │ pkt3 │ ...  │
     ▼  └──────┴──────┴──────┴──────┘
DirectLogFetcher.fetch()
     │  ┌─────────────────────────────────────┐
     │  │           buffer (字节数组)           │
     │  │  ┌──────────────────────────┐       │
     │  │  │ origin          limit    │       │
     │  │  │   ↓               ↓      │       │
     │  │  │ [binlog event 原始字节]    │       │
     │  │  └──────────────────────────┘       │
     │  └─────────────────────────────────────┘
     │
     │  每次 fetch() 返回一个完整的 binlog event
     ▼
LogDecoder.decode(fetcher, context)
     │
     │  将字节流解码为结构化的 LogEvent 对象
     │  （QueryEvent, RowsEvent, TableMapEvent, ...）
     ▼
SinkFunction / Disruptor
```

---

## 第四阶段：位点查找 findStartPosition —— 多级决策树

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlEventParser.java`

位点查找是 Canal Parse 模块中逻辑最复杂的部分之一。每次 Canal 启动或重连时，都需要决定"从 binlog 的哪个位置开始 dump"。这个决策涉及多种场景：首次启动、正常重启、HA 主备切换、binlog 被删除等。

### 4.1 findStartPosition() 入口逻辑

```java
// MysqlEventParser.java
protected EntryPosition findStartPosition(ErosaConnection connection)
        throws IOException {
    EntryPosition startPosition = findStartPositionInternal(connection);
    if (needTransactionPosition.get()) {
        // 如果上一轮 dump 遇到了 TableIdNotFoundException
        // 需要回退到事务的起始位置
        // 从当前位点往前查找最近的 TRANSACTIONBEGIN
        Long preTransactionStartPosition =
            findTransactionBeginPosition(connection, startPosition);
        if (preTransactionStartPosition != null && !preTransactionStartPosition.equals(
                startPosition.getPosition())) {
            logger.info("find transaction start position: {}",
                preTransactionStartPosition);
            startPosition = new EntryPosition(
                startPosition.getJournalName(),
                preTransactionStartPosition,
                startPosition.getTimestamp(),
                startPosition.getServerId());
        }
        needTransactionPosition.compareAndSet(true, false);
    }
    return startPosition;
}
```

> **这一步在干什么？**
>
> 入口方法做了两件事：（1）调用 `findStartPositionInternal()` 执行核心决策逻辑；（2）如果 `needTransactionPosition` 标记为 `true`（因为上一轮遇到了 `TableIdNotFoundException`），则将位点回退到最近的事务起始位置。这保证了从一个完整的事务开始 dump，避免缺少 TableMapEvent。

### 4.2 findStartPositionInternal() 的完整决策树

```java
// MysqlEventParser.java
private EntryPosition findStartPositionInternal(ErosaConnection connection) {
    // ★ 从 LogPositionManager 获取历史位点
    LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);

    if (logPosition == null) {
        // ════════════════════════════════════════════════
        // 情况1：无历史位点（首次启动）
        // ════════════════════════════════════════════════
        EntryPosition entryPosition = null;

        if (masterPosition != null && masterPosition.getJournalName() != null) {
            // 1a. 使用配置的位点
            entryPosition = masterPosition;
        } else if (masterPosition != null && masterPosition.getTimestamp() != null
                   && masterPosition.getTimestamp() > 0L) {
            // 1b. 使用配置的时间戳查找
            entryPosition = findByStartTimeStamp(connection,
                masterPosition.getTimestamp());
        } else {
            // 1c. 都没配置，使用 show master status 获取当前最新位点
            entryPosition = findEndPosition(connection);
        }
        return entryPosition;

    } else {
        // ════════════════════════════════════════════════
        // 情况2：有历史位点
        // ════════════════════════════════════════════════
        // 检查源地址是否匹配（用于判断是否发生了 HA 切换）
        if (logPosition.getIdentity().getSourceAddress().equals(
                runningInfo.getAddress())) {
            // ========================================
            // 情况2a：源地址匹配（没有发生 HA 切换）
            // ========================================
            if (dumpErrorCountThreshold >= 0
                && dumpErrorCount.get() >= dumpErrorCountThreshold) {
                // 2a-1. dump 错误次数超阈值
                // 位点对应的 binlog 可能已被 MySQL purge 删除
                // 回退到更早的时间戳重新查找
                long newStartTimestamp = logPosition.getPostion().getTimestamp()
                    - fallbackIntervalInSeconds * 1000;
                logger.warn("dump error count:{} >= threshold:{}, try to find "
                    + "position by timestamp:{}", dumpErrorCount.get(),
                    dumpErrorCountThreshold, newStartTimestamp);
                return findByStartTimeStamp(connection, newStartTimestamp);
            } else {
                // 2a-2. 正常情况：直接使用历史位点
                return logPosition.getPostion();
            }

        } else {
            // ========================================
            // 情况2b：源地址不匹配（发生了 HA 切换）
            // ========================================
            // Master 换了！需要根据时间戳在新 Master 上重新定位
            long newStartTimestamp = logPosition.getPostion().getTimestamp()
                - fallbackIntervalInSeconds * 1000; // 默认回退60秒
            logger.info("HA switch detected, old address:{}, new address:{}, "
                + "fallback {} seconds", logPosition.getIdentity().getSourceAddress(),
                runningInfo.getAddress(), fallbackIntervalInSeconds);
            return findByStartTimeStamp(connection, newStartTimestamp);
        }
    }
}
```

完整决策树如下：

```
findStartPositionInternal()
  │
  ├── logPosition == null?  （是否有历史位点）
  │   │
  │   ├── YES（首次启动）
  │   │   ├── 配置了 binlog 文件名？
  │   │   │   └── YES → 使用配置的位点
  │   │   ├── 配置了时间戳？
  │   │   │   └── YES → findByStartTimeStamp() 按时间戳查找
  │   │   └── 都没配置
  │   │       └── findEndPosition() → show master status 获取最新位点
  │   │
  │   └── NO（有历史位点）
  │       ├── 源地址匹配？（是否 HA 切换）
  │       │   │
  │       │   ├── YES（正常重启）
  │       │   │   ├── dumpErrorCount >= 阈值？
  │       │   │   │   ├── YES → 回退时间戳 → findByStartTimeStamp()
  │       │   │   │   └── NO → 直接返回历史位点 ✓
  │       │   │   │
  │       │   │
  │       │   └── NO（HA 切换）
  │       │       └── 回退 fallbackIntervalInSeconds(60s)
  │       │           → findByStartTimeStamp() 在新 Master 上查找
  │       │
```

### 4.3 findByStartTimeStamp() —— 从最新binlog倒序逐文件查找

```java
// MysqlEventParser.java
private EntryPosition findByStartTimeStamp(MysqlConnection connection,
        Long startTimestamp) {
    // 1. 获取 MySQL 上的 binlog 文件列表（通过 SHOW BINARY LOGS）
    // 返回按时间升序排列的文件列表
    // 如 [mysql-bin.000001, mysql-bin.000002, ..., mysql-bin.000010]

    // 2. 从最新的文件开始，倒序逐个搜索
    // 原因：通常我们要找的时间点在最新的几个文件中
    // 倒序搜索可以更快找到目标
    EntryPosition endPosition = findEndPosition(connection);
    String startSearchBinlogFile = endPosition.getJournalName();

    while (startSearchBinlogFile != null) {
        EntryPosition entryPosition =
            findAsPerTimestampInSpecificLogFile(
                connection, startTimestamp, endPosition,
                startSearchBinlogFile, true /*需要精确搜索*/);

        if (entryPosition == null) {
            // 当前文件没找到，尝试前一个文件
            // show binary logs，找到当前文件的前一个
            startSearchBinlogFile = getPreBinlogFile(connection,
                startSearchBinlogFile);
        } else {
            logger.info("found start position by timestamp:{}, position:{}",
                startTimestamp, entryPosition);
            return entryPosition;
        }
    }

    // 所有文件都没找到，返回最早的 binlog 文件的起始位置
    return findStartPosition(connection);
}
```

> **这一步在干什么？**
>
> 按时间戳查找位点的策略是"倒序逐文件搜索"：
> 1. 通过 `SHOW BINARY LOGS` 获取所有 binlog 文件列表
> 2. 从最新的文件开始，逐个文件查找第一个时间戳 >= `startTimestamp` 的事件
> 3. 找到则返回该事件的位点；没找到则继续查前一个文件
> 4. 所有文件都没找到（`startTimestamp` 太早，对应的 binlog 已被 purge），返回最早可用的位点

### 4.4 findAsPerTimestampInSpecificLogFile() —— 单文件内遍历事务边界

```java
// MysqlEventParser.java
private EntryPosition findAsPerTimestampInSpecificLogFile(
        MysqlConnection connection, Long startTimestamp,
        EntryPosition endPosition, String searchBinlogFile,
        Boolean needTransactionEnd) {
    try {
        // 创建一个临时的 dump 连接，从指定文件的起始位置(4)开始
        // 位置4是因为每个binlog文件的前4字节是magic number（\xfe\x62\x69\x6e）
        EntryPosition startPosition = new EntryPosition(
            searchBinlogFile, 4L, startTimestamp);

        // 构建连接并开始 dump
        MysqlConnection tempConnection = buildMysqlConnection(runningInfo);
        try {
            tempConnection.connect();
            tempConnection.reconnect();

            final AtomicBoolean found = new AtomicBoolean(false);
            final EntryPosition result = new EntryPosition();

            // dump 整个文件，逐事件检查时间戳
            tempConnection.seek(searchBinlogFile, 4L, new SinkFunction() {
                private LogPosition lastPosition;

                public boolean sink(LogEvent event) {
                    // 对于每个事件，检查其时间戳
                    EntryPosition position = buildEntryPosition(event);

                    // 找到第一个 timestamp >= startTimestamp 的事件
                    if (event.getWhen() * 1000 >= startTimestamp) {
                        // 但不能直接返回这个事件的位点
                        // 需要返回上一个事务的 END 位置
                        // （确保从完整事务开始）
                        if (lastPosition != null) {
                            result.setJournalName(lastPosition.getFileName());
                            result.setPosition(lastPosition.getPosition());
                            result.setTimestamp(lastPosition.getTimestamp());
                        }
                        found.set(true);
                        return false; // 停止 dump
                    }

                    // 记录事务边界位点
                    if (isTransactionEnd(event)) {
                        lastPosition = buildLogPosition(event);
                    }
                    return true; // 继续
                }
            });

            if (found.get()) {
                return result;
            }
        } finally {
            tempConnection.disconnect();
        }
    } catch (Exception e) {
        logger.error("find timestamp in specific log file error", e);
    }
    return null;
}
```

> **关键设计分析：**
>
> 按时间戳定位时，不能简单地返回"第一个时间戳匹配的 event"的位点。因为这个 event 可能在一个事务的中间——从这里开始 dump 会导致事务不完整。所以实际返回的是**匹配 event 之前最近的一个事务 END 的位点**。这样从该位点开始 dump，可以保证事务的完整性。

### 4.5 findEndPosition() —— show master status 的多版本兼容

```java
// MysqlEventParser.java
private EntryPosition findEndPosition(MysqlConnection connection) {
    try {
        // MySQL 8.4+ 废弃了 SHOW MASTER STATUS
        // 改用 SHOW BINARY LOG STATUS
        String showCmd;
        if (connection.getMySQLVersion().isAtLeast(8, 4, 0)) {
            showCmd = "SHOW BINARY LOG STATUS";
        } else {
            showCmd = "SHOW MASTER STATUS";
        }

        ResultSetPacket resultSet = connection.query(showCmd);
        // 结果集列：File | Position | Binlog_Do_DB | Binlog_Ignore_DB | ...
        List<String> fields = resultSet.getFieldValues();
        if (fields != null && fields.size() > 0) {
            EntryPosition endPosition = new EntryPosition(
                fields.get(0),                    // File (如 mysql-bin.000010)
                Long.valueOf(fields.get(1)),       // Position (如 154)
                0L, 0L);
            // 如果有 GTID 列（MySQL 5.6+）
            if (fields.size() > 4) {
                endPosition.setGtid(fields.get(4)); // Executed_Gtid_Set
            }
            return endPosition;
        }
    } catch (IOException e) {
        logger.error("find end position error", e);
    }
    return null;
}
```

> **这一步在干什么？**
>
> `findEndPosition()` 通过 `SHOW MASTER STATUS`（或 MySQL 8.4+ 的 `SHOW BINARY LOG STATUS`）获取 MySQL Master 当前正在写入的 binlog 文件名和位置。这是"最新位点"——如果 Canal 首次启动且没有配置起始位点，就从这个位置开始 dump（只获取增量数据）。注意 MySQL 8.4 的版本兼容处理，这是 Canal 保持多版本 MySQL 支持的典型做法。

### 4.6 slaveId 生成策略

```java
// MysqlEventParser.java
private long generateUniqueServerId() {
    try {
        // 使用 IP 后三段 + 端口的 hashCode 生成 slaveId
        // 目的：同一网络中多个 Canal 实例的 slaveId 不冲突
        InetAddress localHost = InetAddress.getLocalHost();
        byte[] addr = localHost.getAddress();
        int salt = (destination != null) ? destination.hashCode() : 0;

        // IP 后三段作为基础
        // 例如 192.168.1.100 → addr[1]=168, addr[2]=1, addr[3]=100
        return ((0x7f & addr[1]) << 24)  // 第二段左移24位
             + ((0xff & addr[2]) << 16)  // 第三段左移16位
             + ((0xff & addr[3]) << 8)   // 第四段左移8位
             + (0xff & salt);             // hashCode 取低8位
    } catch (UnknownHostException e) {
        // fallback: 使用随机数
        return (long)(Math.random() * Integer.MAX_VALUE);
    }
}
```

> **关键设计分析：**
>
> MySQL 要求每个 Slave 有唯一的 `server-id`，重复的 `server-id` 会导致复制冲突。Canal 使用 IP 地址后三段加 destination 的 hashCode 来生成 `server-id`，在大多数场景下可以保证唯一性。但在某些极端情况下（如多个 destination 的 hashCode 碰撞），仍可能冲突——所以 Canal 也支持手动配置 `canal.instance.mysql.slaveId` 来指定。

---

## 第五阶段：MysqlMultiStageCoprocessor —— Disruptor并行流水线

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`

MysqlMultiStageCoprocessor 是 Canal Parse 模块的性能核心。它使用 LMAX Disruptor 框架实现了一个多阶段并行流水线，将 binlog 事件的解析工作拆分成多个阶段，不同阶段可以并行执行，显著提升解析吞吐量。

### 5.1 四阶段流水线设计图

```
                     MysqlMultiStageCoprocessor 并行流水线
═══════════════════════════════════════════════════════════════════════════

                         ┌──────────────────────┐
                         │   RingBuffer         │
                         │   (Disruptor)        │
                         │   bufferSize=1024    │
                         │   单生产者            │
                         └──────────┬───────────┘
                                    │
    ┌───────────────────────────────┼──────────────────────────────────┐
    │                               │                                  │
    │  Stage 1: publish()      Stage 2:                Stage 3:       │
    │  ┌──────────────┐     SimpleParserStage        DmlParserStage   │
    │  │ 生产者线程    │     ┌─────────────┐        ┌─────────────┐   │
    │  │ (dump线程)   │     │ 单线程       │        │ 多线程       │   │
    │  │              │     │ LogEvent     │        │ WorkerPool  │   │
    │  │ tryNext()    │────▶│ 二进制解码    │───────▶│ DML深度解析  │   │
    │  │ publish()    │     │ TableMap构建 │        │ 行数据提取   │   │
    │  └──────────────┘     └─────────────┘        └──────┬──────┘   │
    │                                                      │          │
    │                                              Stage 4:│          │
    │                                           SinkStoreStage       │
    │                                           ┌─────────────┐      │
    │                                           │ 单线程       │      │
    │                                           │ 按序投递     │      │
    │                                           │ → EventSink  │      │
    │                                           └─────────────┘      │
    │                                                                  │
    └──────────────────────────────────────────────────────────────────┘

为什么这样设计？
═══════════════
 - Stage 2 必须单线程：TableMap event 建立 tableId→表结构 的映射
   如果多线程处理，映射顺序无法保证，导致后续的 RowsEvent 找到错误的表结构

 - Stage 3 可以多线程：DML 行数据的解析是 CPU 密集且互相独立的
   每行数据只依赖自己的 TableMap，不依赖其他行。并行化提升吞吐

 - Stage 4 必须单线程：投递到 EventSink/EventStore 必须保证顺序
   binlog 的顺序就是事务提交顺序，乱序投递会破坏数据一致性
```

### 5.2 start() 中 Disruptor 的精确装配代码

```java
// MysqlMultiStageCoprocessor.java
public void start() {
    super.start();

    // ① 创建 Disruptor RingBuffer
    // 使用单生产者模式（SingleProducerSequencer）
    // 因为只有 dump 线程一个生产者
    this.disruptorMsgBuffer = RingBuffer.createSingleProducer(
        new MessageEventFactory(),  // 事件工厂
        ringBufferSize,              // 缓冲区大小（默认1024，必须是2的幂）
        new BlockingWaitStrategy()   // 等待策略：阻塞等待
    );

    // ② 配置 Stage 2: SimpleParserStage（单线程）
    // 创建 SequenceBarrier：Stage 2 等待生产者发布
    SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
    // 创建单线程处理器
    this.simpleParserStage = new BatchEventProcessor<MessageEvent>(
        disruptorMsgBuffer,
        sequenceBarrier,
        new SimpleParserStage(logContext));  // 事件处理器
    // 设置异常处理器：绝不吞异常
    this.simpleParserStage.setExceptionHandler(
        new SimpleFatalExceptionHandler());

    // ③ 配置 Stage 3: DmlParserStage（多线程 WorkerPool）
    // 创建 SequenceBarrier：Stage 3 等待 Stage 2 完成
    SequenceBarrier dmlParserSequenceBarrier = disruptorMsgBuffer.newBarrier(
        simpleParserStage.getSequence());
    // 创建多个 Worker（默认等于 CPU 核心数的一半）
    WorkHandler<MessageEvent>[] workHandlers =
        new DmlParserStage[parserThreadCount];
    for (int i = 0; i < parserThreadCount; i++) {
        workHandlers[i] = new DmlParserStage();
    }
    // 创建 WorkerPool
    this.workerPool = new WorkerPool<>(
        disruptorMsgBuffer,
        dmlParserSequenceBarrier,
        new SimpleFatalExceptionHandler(),
        workHandlers);

    // ④ 配置 Stage 4: SinkStoreStage（单线程）
    // 创建 SequenceBarrier：Stage 4 等待 Stage 3 的所有 Worker 完成
    SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(
        workerPool.getWorkerSequences());
    this.sinkStoreStage = new BatchEventProcessor<MessageEvent>(
        disruptorMsgBuffer,
        sinkSequenceBarrier,
        new SinkStoreStage());
    this.sinkStoreStage.setExceptionHandler(
        new SimpleFatalExceptionHandler());

    // ⑤ 设置 Gating Sequence
    // 告诉 RingBuffer：最慢的消费者是 sinkStoreStage
    // 生产者不能覆盖 sinkStoreStage 还没消费的数据
    disruptorMsgBuffer.addGatingSequences(
        sinkStoreStage.getSequence());

    // ⑥ 启动所有线程
    // Stage 2: 单线程
    this.simpleParserStageExecutor = Executors.newFixedThreadPool(1,
        new NamedThreadFactory("canal-simple-parser-" + destination));
    this.simpleParserStageExecutor.submit(simpleParserStage);

    // Stage 3: 多线程
    this.parallelParserStageExecutor = Executors.newFixedThreadPool(
        parserThreadCount,
        new NamedThreadFactory("canal-parallel-parser-" + destination));
    workerPool.start(parallelParserStageExecutor);

    // Stage 4: 单线程
    this.sinkStoreStageExecutor = Executors.newFixedThreadPool(1,
        new NamedThreadFactory("canal-sink-" + destination));
    this.sinkStoreStageExecutor.submit(sinkStoreStage);
}
```

> **这一步在干什么？**
>
> Disruptor 的装配是整个并行流水线的核心。让我们理解其中的依赖关系：
>
> ```
> 生产者(dump线程) → [RingBuffer] → Stage2(单线程解码)
>                                        ↓ (SequenceBarrier)
>                                    Stage3(多线程DML解析)
>                                        ↓ (SequenceBarrier)
>                                    Stage4(单线程投递)
>                                        ↓ (GatingSequence)
>                                    反压生产者
> ```
>
> 每个 Stage 通过 `SequenceBarrier` 等待上游 Stage 完成，保证处理顺序。`GatingSequence` 设置为 Stage4 的 sequence，确保生产者（dump 线程）不会覆盖 Stage4 还未消费的数据——这是背压机制。

### 5.3 为什么单生产者？为什么各 Stage 的线程模型这样选择？

| Stage | 线程模型 | 原因 |
|-------|---------|------|
| 生产者 | 单线程 | dump 连接只有一个 Socket，数据天然串行 |
| Stage 2 (SimpleParser) | 单线程 | TableMapEvent 必须有序处理，建立 tableId→表结构映射 |
| Stage 3 (DmlParser) | **多线程** | 行数据解析是 CPU 密集的纯计算，互相独立，天然可并行 |
| Stage 4 (SinkStore) | 单线程 | 投递到 EventStore 必须保序，保证事务顺序 |

**关键设计分析：** Stage 2 必须单线程的原因值得深入理解。MySQL binlog 中，每个 `RowsEvent` 之前一定有对应的 `TableMapEvent`，`RowsEvent` 通过 `tableId` 引用 `TableMapEvent` 中的表结构信息。如果 `TableMapEvent` 被多线程并行处理，可能出现 `RowsEvent` 先于其对应的 `TableMapEvent` 被处理的情况，导致找不到表结构。所以 `TableMapEvent` 的处理（Stage 2）必须严格有序。

### 5.4 publish() 的背压机制

```java
// MysqlMultiStageCoprocessor.java
public boolean publish(LogEvent event) {
    if (!isStart()) {
        return false;
    }

    boolean interupted = false;
    long blockingStart = 0L;
    int applyCount = 0;

    do {
        try {
            // ★ 尝试获取 RingBuffer 中的下一个可用槽位
            // tryNext() 非阻塞：如果 RingBuffer 已满，立即抛异常
            long next = disruptorMsgBuffer.tryNext();
            // 获取到槽位后，填充数据
            MessageEvent data = disruptorMsgBuffer.get(next);
            data.setEvent(event);
            data.setNeedDmlParse(false);  // 初始标记为不需要DML解析
            // 发布到 RingBuffer
            disruptorMsgBuffer.publish(next);

            // 如果之前有背压等待，记录等待时间
            if (applyCount > 0) {
                eventsPublishBlockingTime.addAndGet(
                    System.nanoTime() - blockingStart);
            }
            break; // 发布成功，退出循环

        } catch (InsufficientCapacityException e) {
            // RingBuffer 已满！
            // 背压策略：退避等待
            applyCount++;
            if (applyCount == 1) {
                blockingStart = System.nanoTime();
            }

            // 使用 LockSupport.parkNanos 短暂让出 CPU
            // 比 Thread.sleep 更精确，比 busy-wait 更省 CPU
            LockSupport.parkNanos(1000 * 1000L); // 1ms

            if (Thread.interrupted()) {
                interupted = true;
            }
        }
    } while (isStart() && !interupted);

    return isStart() && !interupted;
}
```

> **关键设计分析：**
>
> `publish()` 使用 `tryNext()` 而非 `next()` 获取槽位。`next()` 会无限阻塞直到有可用槽位，而 `tryNext()` 在 RingBuffer 满时立即返回 `InsufficientCapacityException`。这样做的好处是生产者可以在等待期间检查 `isStart()` 状态——如果 Canal 正在停止，可以及时退出，避免死锁。等待策略使用 `LockSupport.parkNanos(1ms)` 实现短暂退避，平衡了响应延迟和 CPU 开销。

### 5.5 SimpleParserStage —— 单线程解码

```java
// MysqlMultiStageCoprocessor.java 内部类
private class SimpleParserStage implements EventHandler<MessageEvent> {
    private LogDecoder decoder;
    private LogContext context;

    public SimpleParserStage(LogContext context) {
        this.decoder = new LogDecoder();
        this.context = context;
    }

    @Override
    public void onEvent(MessageEvent event, long sequence,
            boolean endOfBatch) throws Exception {
        try {
            LogEvent logEvent = event.getEvent();
            if (logEvent == null) {
                return;
            }

            int eventType = logEvent.getHeader().getType();

            // ① 所有事件：构建基础的 CanalEntry.Entry
            CanalEntry.Entry entry = logEventConvert.parse(logEvent, false);
            // parse() 的 isSeek=false 表示"非查找模式"，进行完整解析

            // ② 标记是否需要 DML 深度解析
            // 只有 WRITE_ROWS / UPDATE_ROWS / DELETE_ROWS 需要
            if (eventType == LogEvent.WRITE_ROWS_EVENT_V1
                || eventType == LogEvent.WRITE_ROWS_EVENT
                || eventType == LogEvent.UPDATE_ROWS_EVENT_V1
                || eventType == LogEvent.UPDATE_ROWS_EVENT
                || eventType == LogEvent.DELETE_ROWS_EVENT_V1
                || eventType == LogEvent.DELETE_ROWS_EVENT) {
                event.setNeedDmlParse(true);  // 标记给 Stage 3
            } else {
                event.setNeedDmlParse(false);
            }

            // ③ 保存解析结果
            event.setEntry(entry);

        } catch (Exception e) {
            exception = new CanalParseException(e);
            throw exception;
        }
    }
}
```

> **这一步在干什么？**
>
> SimpleParserStage（Stage 2）是流水线中的"快速解码"阶段。它在单线程中对每个 LogEvent 做轻量级解析——提取 header 信息（时间戳、事件类型、文件名、偏移量等），处理 TableMapEvent（建立 tableId→表结构映射），但**不解析 DML 行数据的具体字段值**。对于 DML 事件（INSERT/UPDATE/DELETE），它只标记 `needDmlParse=true`，交给下一阶段的多线程并行解析。

### 5.6 DmlParserStage —— 多线程并行深度解析

```java
private class DmlParserStage implements WorkHandler<MessageEvent> {

    @Override
    public void onEvent(MessageEvent event) throws Exception {
        try {
            // 只处理需要 DML 深度解析的事件
            if (event.isNeedDmlParse()) {
                // ★ 深度解析行数据
                // 将 RowsEvent 的二进制数据解析为结构化的字段值列表
                // 包括：解码每列的值、处理 NULL bitmap、
                //       对比 before/after image (UPDATE)
                int eventType = event.getEvent().getHeader().getType();
                CanalEntry.Entry entry = event.getEntry();
                LogEvent logEvent = event.getEvent();

                // 调用 LogEventConvert 的深度解析方法
                // 这个方法会解析每一行的每一列数据
                // CPU 密集操作：字符集转换、数字格式化、日期解析等
                CanalEntry.Entry parsedEntry =
                    logEventConvert.parseRowsEvent(
                        (RowsLogEvent) logEvent, entry);

                event.setEntry(parsedEntry);
            }
        } catch (Exception e) {
            exception = new CanalParseException(e);
            throw exception;
        }
    }
}
```

> **这一步在干什么？**
>
> DmlParserStage（Stage 3）是计算密集型阶段。它将 RowsEvent 的原始二进制数据解析为结构化的行数据，包括：（1）解码每列的值（根据列类型：INT、VARCHAR、DATETIME、DECIMAL 等）；（2）处理 NULL bitmap；（3）对于 UPDATE 事件，对比 before-image 和 after-image。这些操作涉及大量的字节操作和类型转换，是 CPU 密集型任务。因此使用多线程 WorkerPool 并行处理不同行的事件，充分利用多核 CPU。

### 5.7 SinkStoreStage —— 单线程按序投递

```java
private class SinkStoreStage implements EventHandler<MessageEvent> {

    @Override
    public void onEvent(MessageEvent event, long sequence,
            boolean endOfBatch) throws Exception {
        try {
            if (event.getEntry() != null) {
                // ★ 投递到事务缓冲区（保证顺序！）
                // transactionBuffer 会按事务边界缓冲，
                // 完整事务才会 flush 到 EventSink
                transactionBuffer.add(event.getEntry());
            }

            // 清除事件引用，帮助 GC
            // 防止 RingBuffer 中长期持有大对象引用
            LogEvent logEvent = event.getEvent();
            if (logEvent != null) {
                // 清理已解析的数据引用
                event.setEvent(null);
                event.setEntry(null);
            }

            // 如果之前有异常（非本 Stage 抛出），检查并重新抛出
            if (exception != null) {
                throw exception;
            }
        } catch (Exception e) {
            exception = new CanalParseException(e);
            throw exception;
        }
    }
}
```

> **这一步在干什么？**
>
> SinkStoreStage（Stage 4）是流水线的最后一个阶段，负责将解析完成的 `CanalEntry.Entry` 投递到 `EventTransactionBuffer`。虽然 Stage 3 是多线程并行处理的，但 Stage 4 通过 Disruptor 的 SequenceBarrier 保证了**按序消费**——即使 Stage 3 的 Worker A 比 Worker B 先完成了 Sequence 5 的处理，Stage 4 也会按 Sequence 1, 2, 3, 4, 5 的顺序依次消费。这实现了"并行解析但保序投递"的核心设计目标。

### 5.8 异常处理：SimpleFatalExceptionHandler

```java
private static class SimpleFatalExceptionHandler
        implements ExceptionHandler {

    @Override
    public void handleEventException(Throwable ex, long sequence,
            Object event) {
        // ★ 绝不吞异常——直接包装为 RuntimeException 抛出
        // 这会导致对应的 Stage 线程终止
        // 外部通过 exception 字段感知到异常，触发整个 dump 循环重试
        throw new CanalParseException("uncaught exception in stage", ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        throw new CanalParseException("start exception", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        // 关闭时的异常只记日志
        logger.error("shutdown exception", ex);
    }
}
```

> **关键设计分析：** Disruptor 默认的异常处理器会"吞掉"异常——记录日志后继续处理下一个事件。这在 Canal 的场景下是不可接受的：一个 binlog event 解析失败意味着数据可能损坏或丢失，必须停止 dump 并重试。`SimpleFatalExceptionHandler` 通过将异常重新抛出为 `RuntimeException`，使得 Stage 线程崩溃，进而触发外层的异常捕获和重连逻辑。

### 5.9 停止流程

```java
// MysqlMultiStageCoprocessor.java
public void stop() {
    super.stop();

    // 1. 停止所有 Stage
    // Stage 2
    simpleParserStage.halt();
    // Stage 3
    workerPool.halt();
    // Stage 4
    sinkStoreStage.halt();

    // 2. 关闭线程池
    simpleParserStageExecutor.shutdownNow();
    parallelParserStageExecutor.shutdownNow();
    sinkStoreStageExecutor.shutdownNow();

    // 3. 等待线程终止
    try {
        simpleParserStageExecutor.awaitTermination(5, TimeUnit.SECONDS);
        parallelParserStageExecutor.awaitTermination(5, TimeUnit.SECONDS);
        sinkStoreStageExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
        // ignore
    }
}
```

---

## 第六阶段：EventTransactionBuffer —— 事务缓冲区

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/EventTransactionBuffer.java`

EventTransactionBuffer 是连接 Parser 和 Sink 的桥梁。它的核心职责是将散碎的 binlog event 按事务边界聚合，保证向 Sink 投递的数据始终是完整的事务。

### 6.1 环形数组设计

```java
public class EventTransactionBuffer extends AbstractCanalLifeCycle {

    // ★ 核心数据结构：环形数组
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private int               bufferSize;       // 缓冲区大小（必须是2的幂）
    private int               indexMask;         // = bufferSize - 1，用位运算取模
    private CanalEntry.Entry[] entries;           // 环形数组

    // ★ 两个关键指针
    private AtomicLong        putSequence  = new AtomicLong(-1L);  // 写入位置
    private AtomicLong        flushSequence = new AtomicLong(-1L); // 已flush位置

    // ★ Flush 回调（由 AbstractEventParser 设置）
    private TransactionFlushCallback flushCallback;

    public void start() {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            // bufferSize 必须是2的幂
            bufferSize = 1 << (Integer.highestOneBit(bufferSize - 1) + 1);
        }
        indexMask = bufferSize - 1;
        entries = new CanalEntry.Entry[bufferSize];
    }
}
```

> **关键设计分析：**
>
> 使用环形数组（Ring Buffer）而非 ArrayList 的原因：
> 1. **固定内存**：预分配固定大小的数组，避免动态扩容带来的 GC 压力
> 2. **取模优化**：`bufferSize` 是 2 的幂，`index % bufferSize` 可以用 `index & indexMask` 位运算替代，性能更高
> 3. **空间复用**：环形结构天然支持覆盖写入，无需数据搬移

### 6.2 add() 的事务边界逻辑

```java
public void add(CanalEntry.Entry entry) throws InterruptedException {
    switch (entry.getEntryType()) {
        case TRANSACTIONBEGIN:
            // ═══════════════════════════════════════════
            // 事务开始：先 flush 之前缓冲的数据，再放入新事务
            // ═══════════════════════════════════════════
            flush();   // ① flush 上一个事务（如果有）
            put(entry); // ② 放入 TRANSACTIONBEGIN
            break;

        case TRANSACTIONEND:
            // ═══════════════════════════════════════════
            // 事务结束：先放入 END，再 flush 整个事务
            // ═══════════════════════════════════════════
            put(entry);  // ① 放入 TRANSACTIONEND
            flush();     // ② flush 完整事务（BEGIN → DML... → END）
            break;

        case ROWDATA:
            // ═══════════════════════════════════════════
            // 行数据：只放入缓冲区，不 flush
            // ═══════════════════════════════════════════
            put(entry);  // 攒在缓冲区里
            break;

        case HEARTBEAT:
            // ═══════════════════════════════════════════
            // 心跳：放入后立即 flush（心跳不属于任何事务）
            // ═══════════════════════════════════════════
            put(entry);
            flush();
            break;

        default:
            break;
    }
}
```

> **这一步在干什么？**
>
> `add()` 方法实现了事务边界检测和缓冲逻辑，其核心设计意图是：
>
> ```
> 事件流：  BEGIN → DML1 → DML2 → DML3 → END
> add()：  flush↑    put    put    put   put + flush↑
>                                               │
>                                               ▼
>                                     flushCallback.flush(
>                                       [BEGIN, DML1, DML2, DML3, END])
>                                               │
>                                               ▼
>                                     consumeTheEventAndProfilingIfNecessary()
>                                               │
>                                               ▼
>                                     eventSink.sink(entries)
> ```
>
> DML 数据只是 `put` 到缓冲区，只有遇到 `TRANSACTIONEND` 时才 `flush`，将完整的事务一次性投递给 Sink。这保证了下游收到的永远是完整事务。

### 6.3 put() / flush() / checkFreeSlotAt() 的完整实现

```java
// EventTransactionBuffer.java
private void put(CanalEntry.Entry data) throws InterruptedException {
    // 检查缓冲区是否有空闲槽位
    // 如果满了，先 flush 再 put
    if (checkFreeSlotAt(putSequence.get() + 1)) {
        long current = putSequence.get();
        long next = current + 1;

        // 写入环形数组
        entries[getIndex(next)] = data;
        // 更新写入指针
        putSequence.set(next);
    } else {
        // 缓冲区满，强制 flush
        flush();
        // flush 后重试 put
        put(data);
    }
}

private void flush() throws InterruptedException {
    long start = flushSequence.get() + 1;
    long end = putSequence.get();

    if (start <= end) {
        // 收集 [start, end] 范围内的所有 Entry
        List<CanalEntry.Entry> transaction = new ArrayList<>();
        for (long next = start; next <= end; next++) {
            transaction.add(entries[getIndex(next)]);
        }

        // ★ 调用 flush 回调，将完整事务投递给 Sink
        flushCallback.flush(transaction);

        // 更新 flush 指针
        flushSequence.set(end);
    }
}

private boolean checkFreeSlotAt(long sequence) {
    // 检查 sequence 对应的槽位是否已经被消费（可以覆盖写入）
    // 环形数组的核心判断：写入位置不能追上 flush 位置超过一圈
    long wrapPoint = sequence - bufferSize;
    if (wrapPoint > flushSequence.get()) {
        // 缓冲区满！
        return false;
    }
    return true;
}

private int getIndex(long sequence) {
    // 位运算取模（比 % 运算快）
    return (int) sequence & indexMask;
}
```

> **关键设计分析：**
>
> 环形数组的空间管理通过 `putSequence` 和 `flushSequence` 两个指针实现：
>
> ```
> entries[]:  [0] [1] [2] [3] [4] [5] [6] [7]  (bufferSize=8, indexMask=7)
>                      ↑               ↑
>              flushSequence=2   putSequence=5
>              (已flush到这里)    (已写入到这里)
>              
>              可用空间 = bufferSize - (put - flush) = 8 - 3 = 5
>              
>              当 putSequence - flushSequence >= bufferSize 时，缓冲区满
> ```
>
> `checkFreeSlotAt()` 检查写入指针是否已经"绕了一圈追上"了 flush 指针——如果追上了说明缓冲区满，需要先 flush 才能继续写入。

### 6.4 设计意图：事务原子投递

```
          EventTransactionBuffer 的设计意图
═══════════════════════════════════════════════════

问题：binlog event 是逐个到达的
     BEGIN → DML1 → DML2 → DML3 → END → BEGIN → ...

如果每个 event 都直接投递到 Sink/Store：
  - 客户端 getWithoutAck(batchSize=2) 可能只拿到 [BEGIN, DML1]
  - 事务不完整！下游无法正确处理半个事务

EventTransactionBuffer 的解决方案：
  - DML event 先攒在缓冲区
  - 遇到 TRANSACTIONEND 时，一次性 flush 整个事务
  - Sink/Store 收到的永远是 [BEGIN, DML1, DML2, DML3, END]
  - 保证事务的原子性

特殊情况：超大事务
  - 如果一个事务包含 10 万条 DML，缓冲区满了怎么办？
  - 缓冲区满时会调用 flush()，强制将当前积累的数据投递
  - 这意味着超大事务可能被拆成多次投递
  - 但位点只在 TRANSACTIONEND 时持久化
  - 所以即使 Canal 中途崩溃，重启后会从事务开头重新投递
```

---

## 第七阶段：心跳机制

**源码位置**: 
- `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/AbstractEventParser.java`
- `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlEventParser.java`
- `parse/src/main/java/com/alibaba/otter/canal/parse/ha/HeartBeatHAController.java`

心跳机制是 Canal HA 的基础设施，通过周期性检测 MySQL 连接状态来判断是否需要执行主备切换。

### 7.1 startHeartBeat() —— Timer创建和双检锁

```java
// AbstractEventParser.java
protected void startHeartBeat() {
    // 心跳间隔 <= 0 表示禁用心跳
    long heartBeatInterval = detectingIntervalInSeconds * 1000;
    if (heartBeatInterval <= 0) {
        return;
    }

    // 双检锁确保 Timer 只创建一次
    if (timer == null) {
        synchronized (AbstractEventParser.class) {
            if (timer == null) {
                timer = new Timer("canal-heartbeat-" + destination, true);
            }
        }
    }

    // 调度心跳任务
    // 首次延迟 heartBeatInterval 后执行，之后周期执行
    timer.schedule(buildHeartBeatTimeTask(), heartBeatInterval, heartBeatInterval);
}
```

### 7.2 buildHeartBeatTimeTask() 默认实现

```java
// AbstractEventParser.java
protected TimerTask buildHeartBeatTimeTask() {
    return new TimerTask() {
        public void run() {
            try {
                // 默认实现：构造一个 HEARTBEAT 假 Entry
                // 投递到 transactionBuffer，让下游知道"我还活着"
                if (exception == null || lastEntryTime > 0) {
                    // 上次有数据活动 or 没有异常 → 心跳正常
                    long now = System.currentTimeMillis();
                    long intval = now - lastEntryTime;
                    if (intval >= detectingIntervalInSeconds * 1000) {
                        // 超过心跳间隔没有新 event 了
                        // 构造心跳 Entry 投递
                        Header.Builder headerBuilder = Header.newBuilder();
                        headerBuilder.setExecuteTime(now);
                        Entry.Builder entryBuilder = Entry.newBuilder();
                        entryBuilder.setHeader(headerBuilder.build());
                        entryBuilder.setEntryType(EntryType.HEARTBEAT);
                        Entry entry = entryBuilder.build();

                        // 通过 transactionBuffer 投递心跳
                        // EventTransactionBuffer 对 HEARTBEAT 类型会立即 flush
                        transactionBuffer.add(entry);
                    }
                }
            } catch (Throwable e) {
                logger.warn("heartbeat error", e);
            }
        }
    };
}
```

> **这一步在干什么？**
>
> 默认心跳实现是构造一个假的 HEARTBEAT Entry 投递给下游。这有两个作用：（1）推动位点前进——即使 MySQL 长时间没有写入操作，心跳 Entry 仍然会触发 `flush()` → `persistLogPosition()`，保持位点更新；（2）让 EventStore 中有"新数据"可以被消费者感知到（触发 get 的 timeout 唤醒）。

### 7.3 MysqlEventParser 重写 —— 真实的 MySQL 心跳

```java
// MysqlEventParser.java
@Override
protected TimerTask buildHeartBeatTimeTask() {
    return new MysqlDetectingTimeTask(destination) {
        @Override
        protected void doDetecting() {
            try {
                // ★ 使用独立的 metaConnection 执行心跳 SQL
                // 不能复用 dump 连接，因为 dump 连接被 binlog 流占满
                if (metaConnection != null && metaConnection.isConnected()) {
                    // 执行心跳 SQL（默认 "SELECT 1"）
                    metaConnection.query(detectingSQL);
                    // 查询成功 → 通知 HAController 心跳正常
                    if (haController != null
                        && haController instanceof HeartBeatHAController) {
                        ((HeartBeatHAController) haController).onSuccess();
                    }
                }
            } catch (Throwable e) {
                // 查询失败 → 通知 HAController 心跳异常
                if (haController != null
                    && haController instanceof HeartBeatHAController) {
                    ((HeartBeatHAController) haController).onFailed(e);
                }
                logger.warn("detecting failed, retrying...", e);
            }
        }
    };
}
```

> **这一步在干什么？**
>
> MysqlEventParser 重写了默认的心跳实现，改为使用独立的 `metaConnection`（非 dump 连接）执行 `SELECT 1` 心跳 SQL。这样做的好处是：dump 连接在 dump 期间是单向的（只有 MySQL → Canal 的数据流），无法在 dump 连接上执行查询。使用独立连接可以准确检测 MySQL 的可用性——如果 `SELECT 1` 执行失败，说明 MySQL 可能宕机或网络不通，需要触发 HA 切换。

### 7.4 HeartBeatHAController —— HA 切换触发器

```java
// HeartBeatHAController.java
public class HeartBeatHAController extends AbstractCanalLifeCycle
        implements CanalHAController {

    private int detectingRetryTimes = 3;  // 连续失败多少次触发切换
    private AtomicInteger failedTimes = new AtomicInteger(0);
    private CanalHASwitchable switchable;  // MysqlEventParser

    public void onSuccess() {
        // 心跳成功，重置失败计数
        failedTimes.set(0);
    }

    public void onFailed(Throwable e) {
        // 心跳失败，累加计数
        int times = failedTimes.incrementAndGet();
        if (times >= detectingRetryTimes) {
            // ★ 连续失败次数达到阈值，触发 HA 切换
            try {
                switchable.doSwitch();
            } catch (Exception ex) {
                logger.error("HA switch failed", ex);
            }
            // 重置计数
            failedTimes.set(0);
        }
    }
}
```

> **关键设计分析：**
>
> HeartBeatHAController 实现了一个简单的"N 次连续失败则切换"策略。`detectingRetryTimes` 默认为 3——即连续 3 次心跳失败（默认心跳间隔 3 秒，即 9 秒内都无法连接 MySQL）才触发切换。这个设计避免了瞬时网络抖动导致的误切换。每次心跳成功都会将 `failedTimes` 重置为 0，所以只有**连续**失败才会触发。

---

## 第八阶段：HA切换

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlEventParser.java`

### 8.1 MysqlEventParser.doSwitch() —— Master/Standby 互换

```java
// MysqlEventParser.java
public void doSwitch() {
    // ① 检查是否配置了 standby（备库）
    if (standbyInfo == null || standbyInfo.getAddress() == null) {
        logger.warn("no standby info, can not switch");
        return;
    }

    // ② Master ↔ Standby 互换
    AuthenticationInfo tmp = this.runningInfo;
    this.runningInfo = this.standbyInfo;
    this.standbyInfo = tmp;

    logger.warn("HA switch: {} -> {}",
        standbyInfo.getAddress(),  // 旧 master（现在变成 standby）
        runningInfo.getAddress()); // 新 master（原来的 standby）

    // ③ 停止当前 Parser
    stop();

    // ④ 重新启动（使用新的 runningInfo 连接）
    start();
}
```

> **这一步在干什么？**
>
> HA 切换的逻辑非常直接：将 `runningInfo`（当前活跃连接信息）和 `standbyInfo`（备用连接信息）互换，然后执行 `stop() → start()` 重启。重启后，`buildErosaConnection()` 会使用新的 `runningInfo`（即原来的 standby）构建连接，`findStartPosition()` 检测到源地址变化后会回退 60 秒按时间戳重新定位。

### 8.2 切换后位点查找流程

```
HA 切换后的位点定位流程
════════════════════════════════════════════════════

原来的连接：Master A (192.168.1.1:3306)
  - 最后位点：mysql-bin.000010, pos=4096, timestamp=T
  - 位点记录中：sourceAddress = 192.168.1.1:3306

切换后的连接：Master B (192.168.1.2:3306)（原来的 Standby）

findStartPositionInternal() 执行：
  1. logPosition = {sourceAddress=192.168.1.1:3306, pos=..., timestamp=T}
  2. logPosition.sourceAddress (192.168.1.1) != runningInfo.address (192.168.1.2)
     → 检测到 HA 切换！
  3. newStartTimestamp = T - fallbackIntervalInSeconds * 1000
                       = T - 60000  (回退60秒)
  4. findByStartTimeStamp(connection, newStartTimestamp)
     → 在 Master B 上，按时间戳 T-60s 查找对应的位点
     → 返回 Master B 的 binlog 位点

为什么要回退60秒？
  - Master A 和 Master B 的复制可能有延迟
  - 回退一定时间确保不丢数据
  - 代价是可能产生少量重复数据（需要下游幂等消费）
```

### 8.3 dumpErrorCount 机制

```java
// MysqlEventParser.java
private AtomicInteger dumpErrorCount = new AtomicInteger(0);
private int dumpErrorCountThreshold = 2; // 默认阈值

// 在 catch 块中累计：
catch (Throwable e) {
    exception = ExceptionUtils.getRootCause(e);
    // 检查是否是 errno=1236（binlog 被删除）
    if (exception instanceof CanalParseException
        && exception.getMessage() != null
        && exception.getMessage().contains("errno = 1236")) {
        // errno 1236: "Could not find first log file name in
        //              binary log index file"
        // 或 "Client requested master to start replication from
        //     position > file size"
        dumpErrorCount.incrementAndGet();
    }
}

// 在位点查找中使用：
if (dumpErrorCount.get() >= dumpErrorCountThreshold) {
    // dump 连续失败次数超阈值
    // 说明 binlog 位点已经无效（binlog 被 purge 删除了）
    // 回退时间戳重新查找
    long newStartTimestamp = logPosition.getPostion().getTimestamp()
        - fallbackIntervalInSeconds * 1000;
    return findByStartTimeStamp(connection, newStartTimestamp);
}
```

> **关键设计分析：**
>
> `errno=1236` 是 MySQL 返回的"binlog 不存在"错误，通常发生在 Canal 长时间停止后重启——期间 MySQL 执行了 `PURGE BINARY LOGS`，删除了 Canal 上次记录的位点所在的 binlog 文件。Canal 的处理策略是：连续 2 次遇到此错误后，放弃历史位点，改用"回退 60 秒的时间戳"重新查找最近可用的位点。这个设计容忍了少量数据丢失（被 purge 的部分），但保证了 Canal 能够自动恢复运行。

---

## 类继承关系全景图

```
                         Canal Parse 模块类继承关系全景
═══════════════════════════════════════════════════════════════════════

CanalEventParser (接口)
  │
  └── AbstractEventParser (抽象类)
        │  - parseThread (核心线程)
        │  - transactionBuffer (事务缓冲区)
        │  - eventSink / logPositionManager
        │  - start() / stop() / run() 核心循环
        │
        └── AbstractMysqlEventParser (抽象类)
              │  - binlogParser (LogEventConvert)
              │  - tableMetaCache / tableMetaTSDB
              │  - preDump() / processTableMeta()
              │
              ├── MysqlEventParser (MySQL实现)
              │     - runningInfo / standbyInfo (HA双地址)
              │     - haController (心跳HA切换)
              │     - buildMysqlConnection()
              │     - findStartPosition() / findEndPosition()
              │     - doSwitch()
              │
              └── LocalBinlogEventParser (本地binlog解析)
                    - 用于回放本地binlog文件


ErosaConnection (接口)
  │
  └── MysqlConnection
        │  - MysqlConnector (底层TCP/认证)
        │  - connect() / disconnect() / reconnect()
        │  - dump() / seek() (binlog dump)
        │  - query() / update() (SQL执行)
        │  - sendRegisterSlave() / sendBinlogDump()
        │  - updateSettings() / loadBinlogChecksum()
        │
        └── MysqlConnector
              - SocketChannel
              - connect() / disconnect()
              - TCP握手 / MySQL认证协议


BinlogParser (接口)
  │
  └── LogEventConvert
        - parse(LogEvent) → CanalEntry.Entry
        - 将MySQL二进制binlog事件转换为Canal的Protobuf结构
        - TableMetaCache 表结构缓存


MultiStageCoprocessor (接口)
  │
  └── MysqlMultiStageCoprocessor
        - Disruptor RingBuffer
        - SimpleParserStage (Stage 2)
        - DmlParserStage (Stage 3, WorkerPool)
        - SinkStoreStage (Stage 4)
        - publish() / start() / stop()
```

---

## 全局数据流全景

```
              Canal Parse 模块 —— 全局数据流全景图
═══════════════════════════════════════════════════════════════════════

┌──────────────────┐
│  MySQL Master    │
│  (binlog 文件)    │
└────────┬─────────┘
         │
         │ ① TCP 连接 + MySQL 握手认证
         │ ② COM_REGISTER_SLAVE (0x15)
         │ ③ COM_BINLOG_DUMP (0x12) / COM_BINLOG_DUMP_GTID (0x1e)
         │ ④ MySQL 持续推送 binlog event 字节流
         │
         ▼
┌──────────────────┐
│ MysqlConnection  │  → 模拟 MySQL Slave，管理连接和协议
│ (ErosaConnection)│  → updateSettings() / loadBinlogChecksum()
│                  │  → sendRegisterSlave() / sendBinlogDump()
└────────┬─────────┘
         │
         │ MySQL 网络包（3B长度 + 1B序号 + payload）
         ▼
┌──────────────────┐
│ DirectLogFetcher │  → 从 Socket 读取字节流
│                  │  → 处理分包、标志位、Semi-Sync
│                  │  → 输出：原始 binlog event 字节
└────────┬─────────┘
         │
         │ 原始字节
         ▼
┌──────────────────┐
│  LogDecoder      │  → 字节 → LogEvent 对象
│                  │  → 根据 event type 分发
│                  │  → 处理 checksum 校验
└────────┬─────────┘
         │
         │ LogEvent 对象
         ├──────────────────────────────┐
         │                              │
    ┌────▼─────┐                 ┌──────▼───────┐
    │ 串行模式  │                 │   并行模式    │
    │          │                 │              │
    │ SinkFunc │                 │  Disruptor   │
    │ 同步回调  │                 │  RingBuffer  │
    │          │                 │              │
    │ parse → │                 │  Stage2      │ → 单线程解码
    │ add()   │                 │  Stage3      │ → 多线程DML解析
    │          │                 │  Stage4      │ → 单线程投递
    └────┬─────┘                 └──────┬───────┘
         │                              │
         └──────────────┬───────────────┘
                        │
                        │ CanalEntry.Entry
                        ▼
         ┌──────────────────────────┐
         │ EventTransactionBuffer   │  → 事务缓冲区
         │ (环形数组)                │  → 按 BEGIN/END 边界攒批
         │                          │  → flush 完整事务
         └─────────────┬────────────┘
                       │
                       │ List<CanalEntry.Entry> (完整事务)
                       ▼
         ┌──────────────────────────┐
         │  CanalEventSink          │  → 数据过滤 / 路由
         │  (EntryEventSink)        │  → 表名匹配 filter
         │                          │  → 投递到 EventStore
         └─────────────┬────────────┘
                       │
                       ▼
         ┌──────────────────────────┐
         │  CanalEventStore         │  → RingBuffer 内存存储
         │  (MemoryEventStore       │  → 供客户端 get/ack
         │   WithBuffer)            │  → 背压：满时阻塞 Sink
         └──────────────────────────┘
```

---

## 关键设计总结

### 设计决策总表

| 设计点 | 方案 | 原因 |
|--------|------|------|
| 连接伪装 | 完整实现 MySQL Slave 协议 | MySQL 只向注册的 Slave 推送 binlog |
| 会话设置 | 10 条 SQL 预设置 | 确保字符集正确(binary)、连接不超时、支持checksum |
| 字节读取 | DirectLogFetcher + 自适应缓冲区 | 高效处理任意大小的 MySQL 网络包 |
| 并行解析 | Disruptor 4 阶段流水线 | Stage2 有序建映射 → Stage3 并行解析 → Stage4 保序投递 |
| 背压控制 | tryNext + parkNanos 退避 | RingBuffer 满时不死锁，可检查停止信号 |
| 事务缓冲 | 环形数组 + BEGIN/END 边界检测 | 保证向 Sink 投递完整事务 |
| 位点持久化 | 仅在 TRANSACTIONEND 后 | 崩溃重启后可完整重放事务，不丢不重 |
| 位点查找 | 多级决策树 | 覆盖首次启动/正常重启/HA切换/binlog被删4种场景 |
| HA 切换 | 心跳检测 + N 次连续失败 | 避免网络抖动误触发切换 |
| 时间戳定位 | 倒序逐文件 + 事务边界对齐 | 高效查找 + 保证事务完整性 |
| 退避重试 | 随机退避(1~2秒) | 防止多实例同时重连造成惊群 |
| slaveId 生成 | IP后三段 + hashCode | 大多数场景唯一，支持手动覆盖 |
| checksum 处理 | 动态协商 | 兼容 MySQL 5.5(无checksum) 和 5.6+(CRC32) |
| GTID 支持 | COM_BINLOG_DUMP_GTID (0x1e) | 支持 MySQL 5.6.5+ 的 GTID 复制 |
| 异常处理 | SimpleFatalExceptionHandler | Disruptor 阶段异常不吞掉，触发 dump 重试 |
| 心跳设计 | 独立连接 + SELECT 1 | 不干扰 dump 连接的单向数据流 |
| exception 双重作用 | 异常记录 + 存活探针 | 一个字段同时服务于监控和心跳两个需求 |

### 性能关键路径

```
性能关键路径分析
═══════════════════════════════════════════

1. 网络 I/O 层
   DirectLogFetcher.fetch()
   → Socket 读取，受网络带宽和 MySQL binlog 产出速度制约
   → 优化：大缓冲区 + 减少 syscall 次数

2. 解码层
   LogDecoder.decode()
   → 字节 → LogEvent，CPU 密集但不是瓶颈
   → 优化：单次 decode，不重复解码

3. ★ 行解析层（性能瓶颈！）
   LogEventConvert.parseRowsEvent()
   → 每行每列的类型解析、字符集转换、值格式化
   → 优化：Disruptor Stage3 多线程并行

4. 投递层
   EventTransactionBuffer → EventSink → EventStore
   → EventStore(RingBuffer) 满时会背压整个管道
   → 优化：增大 EventStore bufferSize，加快消费速度

最常见的性能瓶颈：
  - 大事务：单个事务包含数十万行变更
    → Stage3 的并行度不足以消化
    → 解决：增加 parserThreadCount

  - EventStore 满：下游消费慢
    → 背压到 EventTransactionBuffer → Disruptor → DirectLogFetcher
    → 解决：加大 canal.instance.memory.buffer.size

  - 网络延迟：跨机房部署 Canal
    → DirectLogFetcher 读取慢
    → 解决：同机房部署
```

### 异常恢复矩阵

| 异常场景 | 检测方式 | 恢复策略 |
|---------|---------|---------|  
| MySQL 连接断开 | dump 循环 catch IOException | 随机退避 1~2 秒后重连重试 |
| MySQL 主库宕机 | 心跳 SELECT 1 失败 3 次 | doSwitch() HA 切换到 Standby |
| binlog 被 purge | errno=1236 连续 2 次 | 回退 60s 时间戳重新查找位点 |
| TableId 找不到 | TableIdNotFoundException | 回退到事务起始位置重新 dump |
| Disruptor 阶段异常 | SimpleFatalExceptionHandler | 异常冒泡，触发 dump 循环重试 |
| EventStore 满 | eventSink.sink() 阻塞 | 背压等待，下游消费后自动恢复 |
| Canal 进程崩溃 | 位点已持久化到事务边界 | 重启后从最后完整事务位点恢复 |
| HA 切换后位点不匹配 | 源地址不匹配检测 | 回退 60s，按时间戳在新 Master 查找 |

---

## 附录：核心配置参数速查

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `canal.instance.mysql.slaveId` | 自动生成 | Slave server-id，同一 MySQL 不能重复 |
| `canal.instance.master.address` | - | MySQL Master 地址 (host:port) |
| `canal.instance.master.journal.name` | - | 起始 binlog 文件名 |
| `canal.instance.master.position` | - | 起始 binlog 偏移量 |
| `canal.instance.master.timestamp` | - | 起始时间戳（毫秒） |
| `canal.instance.standby.address` | - | Standby 地址（HA 切换用） |
| `canal.instance.detecting.sql` | `SELECT 1` | 心跳 SQL |
| `canal.instance.detecting.interval.time` | 3 | 心跳间隔（秒） |
| `canal.instance.detecting.retry.threshold` | 3 | 心跳连续失败切换阈值 |
| `canal.instance.fallbackIntervalInSeconds` | 60 | HA 切换时时间回退（秒） |
| `canal.instance.network.receiveBufferSize` | 16384 | Socket 接收缓冲区 |
| `canal.instance.network.sendBufferSize` | 16384 | Socket 发送缓冲区 |
| `canal.instance.connectionCharset` | UTF-8 | 连接字符集 |
| `canal.instance.parser.parallel` | true | 是否启用并行解析 |
| `canal.instance.parser.parallelThreadSize` | Runtime.availableProcessors() * 60% | 并行解析线程数 |
| `canal.instance.parser.parallelBufferSize` | 256 | Disruptor RingBuffer 大小 |
| `canal.instance.transaction.size` | 1024 | EventTransactionBuffer 大小 |
| `canal.instance.memory.buffer.size` | 16384 | EventStore RingBuffer 大小 |
| `canal.instance.tsdb.enable` | true | 是否启用表结构时间序列 |
| `canal.instance.gtidon` | false | 是否启用 GTID 模式 |