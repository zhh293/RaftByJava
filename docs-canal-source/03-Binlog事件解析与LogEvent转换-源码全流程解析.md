# Binlog事件解析与LogEvent转换 —— 源码全流程解析

> 本文是Canal源码解析系列第三篇，深入分析canal如何将MySQL的二进制binlog事件解析成结构化的`CanalEntry.Entry`对象。这是整个parse模块的"解码灵魂"。

## 目录

- [1. MySQL Binlog事件格式基础](#1-mysql-binlog事件格式基础)
- [2. LogDecoder解码流程](#2-logdecoder解码流程)
- [3. LogEventConvert.parse() —— 事件类型分发入口](#3-logeventconvertparse--事件类型分发入口)
- [4. parseQueryEvent() —— DDL/事务/XA处理](#4-parsequeryevent--dddl事务xa处理)
- [5. parseRowsEvent() —— DML行数据解析（核心中的核心）](#5-parserowsevent--dml行数据解析核心中的核心)
- [6. parseOneRow() —— 列对齐的灵魂（极重点）](#6-parseonerow--列对齐的灵魂极重点)
- [7. TableMetaCache —— 表结构缓存](#7-tablemetacache--表结构缓存)
- [8. parseXidEvent() —— 事务提交](#8-parsexidevent--事务提交)
- [9. parseTableMapEvent() —— TableMap事件](#9-parsetablemapevent--tablemap事件)
- [10. GTID事件处理](#10-gtid事件处理)
- [11. HEARTBEAT事件处理](#11-heartbeat事件处理)
- [12. CanalEntry.Entry协议结构](#12-canalentryentry协议结构)
- [13. 完整解析链路总结](#13-完整解析链路总结从字节到entry的全路径)

---

## 1. MySQL Binlog事件格式基础

### 1.1 Binlog文件整体结构

MySQL的binlog文件是一个二进制文件，记录了所有修改数据库数据的SQL语句（以事件形式存储）。从文件级别来看，它的结构非常简洁：

```
┌─────────────────────────────────────────────────────────────────┐
│                    MySQL Binlog File Structure                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───── │
│  │  Magic Number │  │   Event 1    │  │   Event 2    │  │ ...  │
│  │  (4 bytes)   │  │  (variable)  │  │  (variable)  │  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘  └───── │
│   0xFE 0x62 0x69 0x6E                                            │
│   ("\xFE bin")                                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**魔数（Magic Number）**：4字节，固定为 `0xFE 0x62 0x69 0x6E`，即ASCII字符 `\xFE bin`。Canal在打开binlog文件时，会先读取并校验这4字节魔数，确认这是一个合法的binlog文件。

**事件序列**：魔数之后就是连续的事件流，每个事件由通用Header + 事件体组成，一直延伸到文件末尾。

### 1.2 事件通用Header格式（Common Header）

每个binlog事件都以一个通用Header开头。在MySQL 4.0及之前，Header长度为13字节；MySQL 4.1+为19字节。Canal默认按19字节处理。

```
Common Header (19 bytes) - MySQL 4.1+
┌─────────────────┬─────────────────┬──────────────┬───────────────┬──────────────┬───────────┬────────────┐
│ timestamp (4B)  │ eventType (1B)  │ serverId(4B) │ eventLen (4B) │ logPos (4B)  │ flags(2B) │ extra(2B)* │
│ 事件时间戳       │ 事件类型         │ MySQL Server │ 事件总长度     │ 下一个事件   │ 标志位     │  仅v4事件   │
│  Unix时间戳      │  如QUERY=2       │  ID          │ 含header      │ 的起始位置   │           │  才有      │
│                 │  TABLE_MAP=19    │              │              │              │           │            │
└─────────────────┴─────────────────┴──────────────┴───────────────┴──────────────┴───────────┴────────────┘
  byte 0-3          byte 4           byte 5-8       byte 9-12       byte 13-16    byte 17-18   byte 19-20
```

**各字段详解**：

| 字段 | 偏移 | 长度 | 说明 |
|------|------|------|------|
| `timestamp` | 0 | 4 bytes | 事件产生的Unix时间戳（秒级），对应Canal Entry Header中的 `executeTime` |
| `eventType` | 4 | 1 byte | 事件类型编号，如 `QUERY_EVENT=14`、`TABLE_MAP_EVENT=19`、`WRITE_ROWS_EVENT=23` 等 |
| `serverId` | 5 | 4 bytes | 产生该事件的MySQL Server ID，用于主从复制中区分来源 |
| `eventLen` | 9 | 4 bytes | 整个事件的总长度（Header + Body），Canal用此值从buffer中截取完整事件 |
| `logPos` | 13 | 4 bytes | 下一个事件的起始位置（当前事件结束位置），Canal用此值更新消费位点 |
| `flags` | 17 | 2 bytes | 事件标志位，如 `LOG_EVENT_IGNORABLE_F` 表示可忽略事件 |

在Canal的driver层中，Header由 `LogHeader` 类封装。`LogDecoder.decode()` 方法的第一步就是从 `LogBuffer` 中解析出 `LogHeader`：

```java
// LogDecoder.java 第68-71行
public LogEvent decode(LogBuffer buffer, LogContext context) throws IOException {
    final int limit = buffer.limit();
    if (limit >= FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN) {
        LogHeader header = new LogHeader(buffer, context.getFormatDescription());
        // ...
    }
}
```

**这一步在干什么？**

`LogDecoder.decode()` 先检查buffer中剩余数据是否足够一个Header（至少19字节），然后调用 `new LogHeader(buffer, context.getFormatDescription())` 从buffer中按字节读出timestamp、eventType、serverId、eventLen、logPos等字段。`context.getFormatDescription()` 提供了当前binlog格式的版本信息（包括header长度），确保兼容MySQL不同版本。

### 1.3 事件类型分类表

MySQL定义了数十种binlog事件类型，Canal在 `LogEvent.java` 中用常量定义了所有类型编号：

```java
// LogEvent.java (部分关键常量)
public static final int UNKNOWN_EVENT              = 0;    // 未知事件
public static final int START_EVENT_V3             = 1;    // MySQL 3.x起始事件
public static final int QUERY_EVENT                = 2;    // Query事件（DDL/DML/BEGIN/COMMIT/XA）
public static final int STOP_EVENT                 = 3;    // Stop事件
public static final int ROTATE_EVENT               = 4;    // Binlog文件切换事件
public static final int INTVAR_EVENT               = 5;    // 整型变量事件
public static final int LOAD_EVENT                 = 6;    // LOAD DATA事件
public static final int SLAVE_EVENT                = 7;    // SLAVE事件
public static final int RAND_EVENT                  = 13;   // RAND()事件
public static final int USER_VAR_EVENT             = 14;   // 用户变量事件
public static final int FORMAT_DESCRIPTION_EVENT   = 15;   // 格式描述事件
public static final int XID_EVENT                  = 16;   // XID事务提交事件
public static final int BEGIN_LOAD_QUERY_EVENT    = 17;   // LOAD DATA开始
public static final int EXECUTE_LOAD_QUERY_EVENT  = 18;   // LOAD DATA执行
public static final int TABLE_MAP_EVENT            = 19;   // 表映射事件
public static final int WRITE_ROWS_EVENT_V1        = 23;   // INSERT v1
public static final int UPDATE_ROWS_EVENT_V1       = 24;   // UPDATE v1
public static final int DELETE_ROWS_EVENT_V1       = 25;   // DELETE v1
public static final int WRITE_ROWS_EVENT            = 30;   // INSERT v2
public static final int UPDATE_ROWS_EVENT           = 31;   // UPDATE v2
public static final int DELETE_ROWS_EVENT           = 32;   // DELETE v2
public static final int ROWS_QUERY_LOG_EVENT       = 24;   // Rows Query (原始SQL)
public static final int GTID_LOG_EVENT              = 33;   // GTID事件
public static final int ANONYMOUS_GTID_LOG_EVENT   = 34;   // 匿名GTID
public static final int PREVIOUS_GTIDS_LOG_EVENT    = 35;   // 前序GTID集合
public static final int HEARTBEAT_LOG_EVENT         = 27;   // 心跳事件
public static final int HEARTBEAT_LOG_EVENT_V2     = 28;   // 心跳事件v2
public static final int TRANSACTION_CONTEXT_EVENT  = 36;   // 事务上下文
public static final int TRANSACTION_PAYLOAD_EVENT  = 37;   // 事务压缩payload
public static final int VIEW_CHANGE_EVENT           = 38;   // View Change (GR)
public static final int XA_PREPARE_LOG_EVENT        = 38;   // XA Prepare
public static final int PARTIAL_UPDATE_ROWS_EVENT  = 34;   // 部分列更新
```

**按功能分类汇总**：

| 分类 | 事件类型 | 类型编号 | 说明 |
|------|----------|----------|------|
| **格式与控制** | FORMAT_DESCRIPTION_EVENT | 15 | binlog文件首个事件，描述格式版本和header长度 |
| | ROTATE_EVENT | 4 | binlog文件切换，包含新文件名和起始位置 |
| | STOP_EVENT | 3 | MySQL停止事件 |
| | HEARTBEAT_LOG_EVENT | 27/28 | 主从复制心跳保活 |
| **事务边界** | QUERY_EVENT (BEGIN) | 2 | 事务开始，SQL为"BEGIN" |
| | XID_EVENT | 16 | InnoDB事务提交标记 |
| | QUERY_EVENT (COMMIT) | 2 | MyISAM事务提交，SQL为"COMMIT" |
| **GTID** | GTID_LOG_EVENT | 33 | GTID事务标识 |
| | ANONYMOUS_GTID_LOG_EVENT | 34 | 匿名GTID |
| | PREVIOUS_GTIDS_LOG_EVENT | 35 | 前序GTID集合 |
| **DDL** | QUERY_EVENT (DDL) | 2 | CREATE/ALTER/DROP/TRUNCATE等 |
| **DML行数据** | TABLE_MAP_EVENT | 19 | 行事件前的表结构映射 |
| | WRITE_ROWS_EVENT | 23/30 | INSERT行数据 |
| | UPDATE_ROWS_EVENT | 24/31 | UPDATE行数据 |
| | DELETE_ROWS_EVENT | 25/32 | DELETE行数据 |
| | PARTIAL_UPDATE_ROWS_EVENT | 34 | 部分列UPDATE |
| | ROWS_QUERY_LOG_EVENT | 24 | 原始DML SQL文本 |
| **XA事务** | QUERY_EVENT (XA START/END/COMMIT/ROLLBACK) | 2 | XA事务各阶段 |
| | XA_PREPARE_LOG_EVENT | 38 | XA Prepare |
| **MariaDB特有** | ANNOTATE_ROWS_EVENT | - | 行事件对应的原始SQL |
| | GTID_EVENT | - | MariaDB GTID |
| | GTID_LIST_EVENT | - | MariaDB GTID列表 |

### 1.4 典型事务的Binlog事件流

一条典型的事务在binlog中的事件序列如下：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    典型事务的Binlog事件序列                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  事务1: INSERT INTO t1 VALUES(1,'a'); UPDATE t2 SET v=2 WHERE id=1;      │
│                                                                         │
│  ┌──────────────┐                                                        │
│  │ GTID_EVENT   │  ← 事务GTID标识 (可选，需开启GTID模式)                   │
│  └──────┬───────┘                                                        │
│         ▼                                                               │
│  ┌──────────────┐                                                        │
│  │ QUERY_EVENT  │  ← "BEGIN" (事务开始)                                   │
│  └──────┬───────┘                                                        │
│         ▼                                                               │
│  ┌──────────────┐  ┌────────────────┐  ┌──────────────────┐             │
│  │ TABLE_MAP    │→ │ WRITE_ROWS     │→ │ TABLE_MAP        │             │
│  │ (table=t1)   │  │ (INSERT t1)    │  │ (table=t2)       │             │
│  └──────────────┘  └────────────────┘  └───────┬──────────┘             │
│                                                ▼                        │
│                                        ┌──────────────────┐             │
│                                        │ UPDATE_ROWS      │             │
│                                        │ (UPDATE t2)      │             │
│                                        └───────┬──────────┘             │
│                                                ▼                        │
│                                        ┌──────────────────┐             │
│                                        │ XID_EVENT        │             │
│                                        │ (事务提交)        │             │
│                                        └──────────────────┘             │
│                                                                         │
│  事务2: ALTER TABLE t3 ADD COLUMN c INT;  (DDL, 自动提交)                   │
│                                                                         │
│  ┌──────────────┐                                                        │
│  │ GTID_EVENT   │  ← DDL也有GTID (如果开启)                               │
│  └──────┬───────┘                                                        │
│         ▼                                                               │
│  ┌──────────────┐                                                        │
│  │ QUERY_EVENT  │  ← "ALTER TABLE t3 ADD COLUMN c INT" (DDL直接是QUERY)   │
│  └──────────────┘                                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**关键点**：
- 每个DML行事件（WRITE/UPDATE/DELETE）之前必然有一个对应的TABLE_MAP事件，描述该表的列信息
- 事务以BEGIN（QUERY_EVENT）开始，以XID_EVENT或COMMIT（QUERY_EVENT）结束
- DDL语句直接表现为QUERY_EVENT，不需要TABLE_MAP
- GTID_EVENT在事务最前面（如果开启了GTID）

---

## 2. LogDecoder解码流程

### 2.1 两层解码架构总览

Canal的binlog解码分为两层：

```
┌─────────────────────────────────────────────────────────────────────┐
│                        两层解码架构                                   │
├──────────────────────┬──────────────────────────────────────────────┤
│   第一层: LogDecoder  │   第二层: LogEventConvert                      │
│   (driver层)          │   (parse层)                                   │
│                      │                                              │
│   输入: LogBuffer     │   输入: LogEvent                              │
│   (原始字节流)         │   (结构化事件对象)                              │
│                      │                                              │
│   输出: LogEvent      │   输出: CanalEntry.Entry                      │
│   (二进制事件对象)     │   (Protocol Buffers消息)                       │
│                      │                                              │
│   职责: 字节→事件对象  │   职责: 事件对象→业务消息                        │
│   关注: 二进制协议     │   关注: 语义解析和结构转换                       │
└──────────────────────┴──────────────────────────────────────────────┘
```

- **LogDecoder**（`com.taobao.tddl.dbsync.binlog.LogDecoder`）：负责从原始字节流 `LogBuffer` 中，按照MySQL binlog二进制协议，解析出结构化的 `LogEvent` 子类对象。这是纯粹的协议解码，不涉及业务语义。

- **LogEventConvert**（`com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert``）：负责将 `LogEvent` 对象转换为Canal自定义的 `CanalEntry.Entry` Protocol Buffers消息。这一层负责表结构元数据补全、列对齐、类型转换、过滤等业务逻辑。

### 2.2 LogDecoder.decode() 的外层入口

`LogDecoder` 提供了两个 `decode` 方法：一个是实例方法（处理buffer限制和handleSet过滤），一个是静态方法（实际的事件分发）。

```java
// LogDecoder.java 第68-112行
public LogEvent decode(LogBuffer buffer, LogContext context) throws IOException {
    final int limit = buffer.limit();
    // 1. 检查buffer是否足够一个header
    if (limit >= FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN) {
        // 2. 解析通用header
        LogHeader header = new LogHeader(buffer, context.getFormatDescription());

        final int len = header.getEventLen();
        // 3. 检查buffer是否包含完整事件
        if (limit >= len) {
            LogEvent event;

            /* 4. Checking binary-log's header */
            if (handleSet.get(header.getType())) {
                buffer.limit(len);  // 限制buffer只读取当前事件的数据
                try {
                    /* 5. Decoding binary-log to event */
                    event = decode(buffer, header, context);
                } catch (IOException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Decoding " + LogEvent.getTypeName(header.getType())
                                    + " failed from: " + context.getLogPosition(), e);
                    }
                    throw e;
                } finally {
                    buffer.limit(limit); /* Restore limit */
                }
            } else {
                /* Ignore unsupported binary-log. */
                event = new UnknownLogEvent(header);
            }

            if (event != null) {
                // 6. 设置logFileName和semival
                event.getHeader().setLogFileName(context.getLogPosition().getFileName());
                event.setSemival(buffer.semival);
            }

            /* 7. consume this binary-log. */
            buffer.consume(len);
            return event;
        }
    }

    /* Rewind buffer's position to 0. */
    buffer.rewind();
    return null;
}
```

**这一步在干什么？**

1. **Header解析**：从buffer中读出19字节通用Header，得到事件类型、事件长度等基本信息。
2. **完整性校验**：检查buffer中是否有足够的数据（至少 `eventLen` 字节），不够则rewind返回null，等待更多数据到达。
3. **handleSet过滤**：`handleSet` 是一个BitSet，标记了decoder需要处理的事件类型。如果当前事件类型不在handleSet中，直接创建 `UnknownLogEvent` 跳过。
4. **buffer.limit(len)**：将buffer的limit临时设置为当前事件长度，确保后续解析只读取当前事件的数据，不会越界读到下一个事件。
5. **调用静态decode方法**：进入具体的事件分发逻辑。
6. **consume(len)**：解析完成后，将buffer的position前进len字节，表示消费了当前事件。

### 2.3 LogDecoder.decode() 静态方法 —— 事件类型分发

静态 `decode` 方法是实际的事件分发器，通过一个巨大的 `switch(header.getType())` 将不同类型的binlog事件分发到对应的 `LogEvent` 子类构造器：

```java
// LogDecoder.java 第170-551行 (核心分发逻辑)
public static LogEvent decode(LogBuffer buffer, LogHeader header, LogContext context) throws IOException {
    FormatDescriptionLogEvent descriptionEvent = context.getFormatDescription();
    LogPosition logPosition = context.getLogPosition();

    // 1. 处理checksum
    int checksumAlg = LogEvent.BINLOG_CHECKSUM_ALG_UNDEF;
    if (header.getType() != LogEvent.FORMAT_DESCRIPTION_EVENT) {
        checksumAlg = descriptionEvent.header.getChecksumAlg();
    } else {
        checksumAlg = header.getChecksumAlg();
    }

    if (checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_OFF
        && checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_UNDEF) {
        if (!context.isIterateDecode()) {
            // 去掉末尾的checksum字节 (4字节CRC32)
            buffer.limit(header.getEventLen() - LogEvent.BINLOG_CHECKSUM_LEN);
        }
    }

    GTIDSet gtidSet = context.getGtidSet();
    LogEvent gtidLogEvent = context.getGtidLogEvent();

    // 2. 事件类型分发
    switch (header.getType()) {
        case LogEvent.QUERY_EVENT: {
            QueryLogEvent event = new QueryLogEvent(header, buffer, descriptionEvent,
                context.isCompatiablePercona());
            logPosition.position = header.getLogPos();
            header.putGtid(context.getGtidSet(), gtidLogEvent);
            return event;
        }
        case LogEvent.XID_EVENT: {
            XidLogEvent event = new XidLogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            header.putGtid(context.getGtidSet(), gtidLogEvent);
            return event;
        }
        case LogEvent.TABLE_MAP_EVENT: {
            TableMapLogEvent mapEvent = new TableMapLogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            context.putTable(mapEvent);  // ★ 关键：存入context的table map缓存
            return mapEvent;
        }
        case LogEvent.WRITE_ROWS_EVENT_V1:
        case LogEvent.WRITE_ROWS_EVENT: {
            RowsLogEvent event = new WriteRowsLogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            event.fillTable(context);  // ★ 关键：从context取出TableMapLogEvent补全表信息
            header.putGtid(context.getGtidSet(), gtidLogEvent);
            return event;
        }
        case LogEvent.UPDATE_ROWS_EVENT_V1:
        case LogEvent.UPDATE_ROWS_EVENT: {
            RowsLogEvent event = new UpdateRowsLogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            event.fillTable(context);
            header.putGtid(context.getGtidSet(), gtidLogEvent);
            return event;
        }
        case LogEvent.DELETE_ROWS_EVENT_V1:
        case LogEvent.DELETE_ROWS_EVENT: {
            RowsLogEvent event = new DeleteRowsLogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            event.fillTable(context);
            header.putGtid(context.getGtidSet(), gtidLogEvent);
            return event;
        }
        case LogEvent.ROTATE_EVENT: {
            RotateLogEvent event = new RotateLogEvent(header, buffer, descriptionEvent);
            logPosition = new LogPosition(event.getFilename(), event.getPosition());
            context.setLogPosition(logPosition);
            return event;
        }
        case LogEvent.FORMAT_DESCRIPTION_EVENT: {
            descriptionEvent = new FormatDescriptionLogEvent(header, buffer, descriptionEvent);
            context.setFormatDescription(descriptionEvent);
            return descriptionEvent;
        }
        case LogEvent.GTID_LOG_EVENT:
        case LogEvent.ANONYMOUS_GTID_LOG_EVENT: {
            GtidLogEvent event = new GtidLogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            if (gtidSet != null) {
                gtidSet.update(event.getGtidStr());
                header.putGtid(gtidSet, event);
            }
            context.setGtidLogEvent(event);  // ★ 关键：保存当前GTID到context
            return event;
        }
        case LogEvent.PARTIAL_UPDATE_ROWS_EVENT: {
            RowsLogEvent event = new UpdateRowsLogEvent(header, buffer, descriptionEvent, true);
            logPosition.position = header.getLogPos();
            event.fillTable(context);
            header.putGtid(context.getGtidSet(), gtidLogEvent);
            return event;
        }
        case LogEvent.HEARTBEAT_LOG_EVENT: {
            HeartbeatLogEvent event = new HeartbeatLogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            return event;
        }
        case LogEvent.HEARTBEAT_LOG_EVENT_V2: {
            HeartbeatV2LogEvent event = new HeartbeatV2LogEvent(header, buffer, descriptionEvent);
            logPosition.position = header.getLogPos();
            return event;
        }
        // ... 其他事件类型省略，结构类似
        default:
            if ((buffer.getUint16(LogEvent.FLAGS_OFFSET) & LogEvent.LOG_EVENT_IGNORABLE_F) > 0) {
                IgnorableLogEvent event = new IgnorableLogEvent(header, buffer, descriptionEvent);
                logPosition.position = header.getLogPos();
                return event;
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("Skipping unrecognized binlog event "
                        + LogEvent.getTypeName(header.getType())
                        + " from: " + context.getLogPosition());
                }
            }
    }

    logPosition.position = header.getLogPos();
    return new UnknownLogEvent(header);
}
```

**这一步在干什么？**

这是整个binlog字节解码的核心分发逻辑。每种事件类型都有对应的 `LogEvent` 子类，其构造器接收 `LogBuffer` 并按照MySQL二进制协议从buffer中逐字段读取数据。几个关键设计：

1. **Checksum处理**：MySQL 5.6+ 支持binlog checksum（CRC32），每个事件末尾有4字节校验值。decode时先去掉末尾4字节，保证后续解析不受干扰。

2. **TABLE_MAP_EVENT → context.putTable(mapEvent)**：TableMap事件解析后存入 `LogContext` 的 `mapOfTable` 缓存（key=tableId），后续行事件通过 `fillTable(context)` 从缓存中取回表结构信息。

3. **行事件 → fillTable(context)**：`RowsLogEvent.fillTable()` 从context中根据tableId取出对应的 `TableMapLogEvent`，获取列类型、列元数据等信息，为后续行数据解析做准备。

4. **GTID → context.setGtidLogEvent(event)**：GTID事件解析后存入context，后续所有事件（直到下一个GTID）都关联这个GTID，通过 `header.putGtid()` 写入事件header。

5. **FORMAT_DESCRIPTION_EVENT → context.setFormatDescription()**：格式描述事件是binlog文件的第一个事件，描述了binlog版本、header长度、checksum算法等。解析后存入context，供后续所有事件解析使用。

### 2.4 LogContext —— 解码上下文

`LogContext` 是解码过程中的状态容器，贯穿整个binlog流解析：

```java
// LogContext.java
public final class LogContext {
    // tableId → TableMapLogEvent 映射表，行事件解析时查找表结构
    private final Map<Long, TableMapLogEvent> mapOfTable = new HashMap<>();

    // 格式描述事件，描述binlog版本和header长度
    private FormatDescriptionLogEvent formatDescription;

    // 当前消费的binlog位点（文件名 + 位置）
    private LogPosition logPosition;

    // GTID集合（主从复制中用于全局事务标识）
    private GTIDSet gtidSet;

    // 当前GTID事件（事务内所有事件共享此GTID）
    private LogEvent gtidLogEvent;

    // 是否在遍历解压payload中的事件
    private boolean iterateDecode = false;

    // Percona兼容模式
    private boolean compatiablePercona = false;

    public LogContext() {
        // 默认使用MySQL 5.x的格式描述
        this.formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
    }

    // 存入table map
    public final void putTable(TableMapLogEvent mapEvent) {
        mapOfTable.put(Long.valueOf(mapEvent.getTableId()), mapEvent);
    }

    // 取出table map
    public final TableMapLogEvent getTable(final long tableId) {
        return mapOfTable.get(Long.valueOf(tableId));
    }

    public final void clearAllTables() {
        mapOfTable.clear();
    }
}
```

**这一步在干什么？**

`LogContext` 在解码过程中扮演"贯穿上下文"的角色：

| 字段 | 作用 | 生命周期 |
|------|------|----------|
| `mapOfTable` | 缓存TableMap事件，行事件解析时通过tableId查找表结构 | binlog文件级别，ROTATE事件时可能clear |
| `formatDescription` | 描述当前binlog格式版本，控制header解析行为 | 遇到FORMAT_DESCRIPTION_EVENT时更新 |
| `logPosition` | 当前消费位点（文件名+位置），用于断点续传 | 每个事件解析后更新 |
| `gtidSet` | GTID集合，维护全局事务标识 | 遇到GTID事件时更新 |
| `gtidLogEvent` | 当前事务的GTID事件 | 遇到GTID事件时更新，后续事件关联此GTID |

### 2.5 FormatDescriptionLogEvent 的作用

`FormatDescriptionLogEvent` 是binlog文件的第一个事件（事件类型15），它描述了：

1. **binlog格式版本**：MySQL 3.x/4.0/4.1+/5.x
2. **通用Header长度**：13字节（v3）或19字节（v4+）
3. **各事件类型的post-header长度**：每种事件在通用header之后的固定header长度
4. **Checksum算法**：OFF（无校验）或CRC32

在 `LogContext` 构造时，默认使用 `FORMAT_DESCRIPTION_EVENT_5_x`：

```java
// LogContext.java 第33行
public LogContext() {
    this.formatDescription = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
}
```

当解析到真正的 `FORMAT_DESCRIPTION_EVENT` 时，会更新context中的formatDescription：

```java
// LogDecoder.java 第322-326行
case LogEvent.FORMAT_DESCRIPTION_EVENT: {
    descriptionEvent = new FormatDescriptionLogEvent(header, buffer, descriptionEvent);
    context.setFormatDescription(descriptionEvent);
    return descriptionEvent;
}
```

### 2.6 LogDecoder解码流程图

```
┌──────────────────────────────────────────────────────────────────────┐
│                      LogDecoder.decode() 流程                         │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   LogBuffer (原始字节流)                                               │
│       │                                                              │
│       ▼                                                              │
│   ┌─────────────────────────────┐                                    │
│   │ 检查buffer >= 19字节?       │                                    │
│   └──────────┬──────────────────┘                                    │
│           否 │                                                       │
│     ┌───────┴───────┐                                               │
│     ▼               ▼                                               │
│  rewind()       ┌─────────────────────┐                              │
│  return null   │ 解析 LogHeader (19B) │                              │
│                └──────────┬──────────┘                              │
│                           │                                          │
│                ┌──────────▼──────────┐                              │
│                │ 检查buffer >= eventLen?│                             │
│                └──────────┬──────────┘                              │
│                        否 │                                          │
│                  ┌───────┴───────┐                                 │
│                  ▼               ▼                                 │
│               rewind()      ┌─────────────────┐                    │
│               return null   │ handleSet包含?   │                    │
│                             └────────┬────────┘                    │
│                                 是   │     否                       │
│                            ┌────────┘    │                          │
│                            ▼             ▼                          │
│                   ┌──────────────┐  ┌────────────────┐            │
│                   │ buffer.limit │  │ UnknownLogEvent │            │
│                   │ = eventLen   │  └────────────────┘            │
│                   └──────┬───────┘                                │
│                          ▼                                        │
│              ┌───────────────────────┐                            │
│              │ 处理checksum (去掉4B)  │                           │
│              └───────────┬───────────┘                            │
│                          ▼                                        │
│              ┌───────────────────────┐                            │
│              │ switch(eventType)     │                            │
│              │  QUERY → QueryLogEvent│                            │
│              │  XID → XidLogEvent    │                            │
│              │  TABLE_MAP → TableMap │                            │
│              │  WRITE_ROWS → Write   │                            │
│              │  UPDATE_ROWS → Update  │                            │
│              │  DELETE_ROWS → Delete  │                            │
│              │  GTID → GtidLogEvent   │                            │
│              │  ...                   │                            │
│              └───────────┬───────────┘                            │
│                          ▼                                        │
│              ┌───────────────────────┐                            │
│              │ 更新logPosition        │                            │
│              │ 设置logFileName        │                            │
│              └───────────┬───────────┘                            │
│                          ▼                                        │
│              ┌───────────────────────┐                            │
│              │ buffer.consume(eventLen)│                          │
│              └───────────┬───────────┘                            │
│                          ▼                                        │
│                    返回 LogEvent                                   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.7 各事件类型如何从LogBuffer中解析出LogEvent子类

以 `TableMapLogEvent` 为例，其构造器从buffer中解析表ID、数据库名、表名、列类型数组、列元数据等：

```java
// TableMapLogEvent 构造器 (简化示意)
public TableMapLogEvent(LogHeader header, LogBuffer buffer,
                        FormatDescriptionLogEvent descriptionEvent) {
    super(header);
    // 1. 读取tableId (6字节)
    this.tableId = buffer.getUlong48();
    // 2. 读取flags (2字节)
    this.flags = buffer.getUint16();
    // 3. 读取数据库名 (以\0结尾的字符串)
    this.databaseName = buffer.getStringNullTerminated();
    // 4. 读取表名 (以\0结尾的字符串)
    this.tableName = buffer.getStringNullTerminated();
    // 5. 读取列数量 (用PackedInteger编码)
    this.columnCount = (int) buffer.getPackedLong();
    // 6. 读取列类型数组 (每列1字节)
    this.columnTypes = new byte[columnCount];
    for (int i = 0; i < columnCount; i++) {
        columnTypes[i] = buffer.getUint8();
    }
    // 7. 解析列元数据 (可选metadata)
    parseColumnMetaData(buffer);
    // 8. 解析optional metadata (MySQL 8.0 binlog_row_metadata=FULL时)
    parseOptionalMetaData(buffer);
}
```

类似地，`WriteRowsLogEvent`、`UpdateRowsLogEvent`、`DeleteRowsLogEvent` 的构造器会从buffer中读取tableId、列位图（present bitmap）和行数据块。

---

## 3. LogEventConvert.parse() —— 事件类型分发入口

### 3.1 类定义与核心字段

`LogEventConvert` 是parse模块的核心转换器，实现了 `BinlogParser<LogEvent>` 接口：

```java
// LogEventConvert.java 第56行
public class LogEventConvert extends AbstractCanalLifeCycle implements BinlogParser<LogEvent> {

    // XA相关常量
    public static final String XA_XID              = "XA_XID";
    public static final String XA_TYPE             = "XA_TYPE";
    public static final String XA_START            = "XA START";
    public static final String XA_END              = "XA END";
    public static final String XA_COMMIT           = "XA COMMIT";
    public static final String XA_ROLLBACK         = "XA ROLLBACK";

    // 字符集
    public static final String ISO_8859_1          = "ISO-8859-1";
    public static final String UTF_8               = "UTF-8";

    // Unsigned类型溢出修正常量
    public static final int    TINYINT_MAX_VALUE   = 256;
    public static final int    SMALLINT_MAX_VALUE  = 65536;
    public static final int    MEDIUMINT_MAX_VALUE = 16777216;
    public static final long   INTEGER_MAX_VALUE   = 4294967296L;
    public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");

    // 事务标识
    public static final String BEGIN               = "BEGIN";
    public static final String COMMIT              = "COMMIT";

    // 过滤器配置
    private volatile AviaterRegexFilter nameFilter;          // 表名白名单
    private volatile AviaterRegexFilter nameBlackFilter;     // 表名黑名单
    private Map<String, List<String>>   fieldFilterMap;      // 字段级白名单
    private Map<String, List<String>>   fieldBlackFilterMap; // 字段级黑名单

    // 表结构缓存
    private TableMetaCache tableMetaCache;

    // 全局字符集
    private Charset charset = Charset.defaultCharset();

    // 过滤开关
    private boolean filterQueryDcl      = false;  // 是否过滤DCL (GRANT/REVOKE等)
    private boolean filterQueryDml      = false;  // 是否过滤DML Query (INSERT/UPDATE/DELETE语句)
    private boolean filterQueryDdl      = false;  // 是否过滤DDL
    private boolean filterTableError    = false;  // 是否跳过表解析异常
    private boolean filterRows          = false;  // 是否过滤行数据
    private boolean useDruidDdlFilter   = true;   // 是否使用Druid解析DDL
}
```

**字段分类说明**：

| 字段分类 | 字段名 | 作用 |
|----------|--------|------|
| **Unsigned溢出修正** | `TINYINT_MAX_VALUE`等 | MySQL unsigned类型在Java中可能溢出为负数，用这些常量修正 |
| **表名过滤** | `nameFilter` / `nameBlackFilter` | Aviator正则表达式过滤，控制哪些表的数据需要解析 |
| **字段过滤** | `fieldFilterMap` / `fieldBlackFilterMap` | 字段级别过滤，控制只同步特定列 |
| **表结构缓存** | `tableMetaCache` | 缓存表结构元数据，为行数据解析提供列名、列类型等信息 |
| **字符集** | `charset` | 全局字符集，用于将binlog中的ISO-8859-1字节流转换为正确编码 |
| **过滤开关** | `filterQueryDcl`等 | 控制不同类型事件是否需要过滤 |

### 3.2 parse() 完整分发逻辑

`parse()` 是 `BinlogParser` 接口的实现，是第二层解码的入口：

```java
// LogEventConvert.java 第96-145行
@Override
public Entry parse(LogEvent logEvent, boolean isSeek) throws CanalParseException {
    // 1. 空事件或未知事件直接返回null
    if (logEvent == null || logEvent instanceof UnknownLogEvent) {
        return null;
    }

    // 2. 获取事件类型
    int eventType = logEvent.getHeader().getType();

    // 3. 根据事件类型分发到对应的处理方法
    switch (eventType) {
        case LogEvent.QUERY_EVENT:
            // Query事件：BEGIN/COMMIT/DDL/XA
            return parseQueryEvent((QueryLogEvent) logEvent, isSeek);
        case LogEvent.XID_EVENT:
            // XID事件：InnoDB事务提交
            return parseXidEvent((XidLogEvent) logEvent);
        case LogEvent.TABLE_MAP_EVENT:
            // TableMap事件：只做字符集转换，不产生Entry
            parseTableMapEvent((TableMapLogEvent) logEvent);
            break;
        case LogEvent.WRITE_ROWS_EVENT_V1:
        case LogEvent.WRITE_ROWS_EVENT:
            // INSERT行事件
            return parseRowsEvent((WriteRowsLogEvent) logEvent);
        case LogEvent.UPDATE_ROWS_EVENT_V1:
        case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
        case LogEvent.UPDATE_ROWS_EVENT:
            // UPDATE行事件
            return parseRowsEvent((UpdateRowsLogEvent) logEvent);
        case LogEvent.DELETE_ROWS_EVENT_V1:
        case LogEvent.DELETE_ROWS_EVENT:
            // DELETE行事件
            return parseRowsEvent((DeleteRowsLogEvent) logEvent);
        case LogEvent.ROWS_QUERY_LOG_EVENT:
            // Rows Query事件：原始DML SQL文本
            return parseRowsQueryEvent((RowsQueryLogEvent) logEvent);
        case LogEvent.ANNOTATE_ROWS_EVENT:
            // MariaDB Annotate事件
            return parseAnnotateRowsEvent((AnnotateRowsEvent) logEvent);
        case LogEvent.USER_VAR_EVENT:
            // 用户变量事件
            return parseUserVarLogEvent((UserVarLogEvent) logEvent);
        case LogEvent.INTVAR_EVENT:
            // 整型变量事件
            return parseIntrvarLogEvent((IntvarLogEvent) logEvent);
        case LogEvent.RAND_EVENT:
            // RAND()事件
            return parseRandLogEvent((RandLogEvent) logEvent);
        case LogEvent.GTID_LOG_EVENT:
            // GTID事件
            return parseGTIDLogEvent((GtidLogEvent) logEvent);
        case LogEvent.HEARTBEAT_LOG_EVENT:
            // 心跳事件
            return parseHeartbeatLogEvent((HeartbeatLogEvent) logEvent);
        case LogEvent.HEARTBEAT_LOG_EVENT_V2:
            // 心跳事件v2
            return parseHeartbeatV2LogEvent((HeartbeatV2LogEvent) logEvent);
        case LogEvent.GTID_EVENT:
        case LogEvent.GTID_LIST_EVENT:
            // MariaDB GTID事件
            return parseMariaGTIDLogEvent(logEvent);
        default:
            // 未识别的事件类型，忽略
            break;
    }

    return null;
}
```

**这一步在干什么？**

`parse()` 方法是LogEvent到CanalEntry.Entry的转换入口。它通过事件类型分发到不同的处理方法：

| 事件类型 | 处理方法 | 返回Entry类型 | 说明 |
|----------|----------|---------------|------|
| QUERY_EVENT | `parseQueryEvent()` | TRANSACTIONBEGIN / TRANSACTIONEND / ROWDATA | 区分BEGIN/COMMIT/DDL/XA |
| XID_EVENT | `parseXidEvent()` | TRANSACTIONEND | InnoDB事务提交 |
| TABLE_MAP_EVENT | `parseTableMapEvent()` | null（不产Entry） | 只做字符集转换 |
| WRITE_ROWS_EVENT | `parseRowsEvent()` | ROWDATA | INSERT |
| UPDATE_ROWS_EVENT | `parseRowsEvent()` | ROWDATA | UPDATE |
| DELETE_ROWS_EVENT | `parseRowsEvent()` | ROWDATA | DELETE |
| GTID_LOG_EVENT | `parseGTIDLogEvent()` | GTIDLOG | GTID标识 |
| HEARTBEAT_LOG_EVENT | `parseHeartbeatLogEvent()` | HEARTBEAT | 心跳保活 |
| ROWS_QUERY_LOG_EVENT | `parseRowsQueryEvent()` | ROWDATA | 原始SQL文本 |

**关键设计点**：

1. **V1/V2版本兼容**：`WRITE_ROWS_EVENT_V1`（type=23）和 `WRITE_ROWS_EVENT`（type=30）走同一处理逻辑，这是MySQL 5.1.7+升级了行事件格式，V2增加了extra字段。

2. **PARTIAL_UPDATE_ROWS_EVENT**：MySQL 5.7+的部分列更新事件，复用 `UpdateRowsLogEvent` 类，但构造时传入 `partial=true`。

3. **TABLE_MAP_EVENT不产Entry**：`parseTableMapEvent()` 返回void（break而非return），说明TableMap事件只做预处理（字符集转换），不产生Canal Entry。

4. **isSeek参数**：`parseQueryEvent` 接收 `isSeek` 参数，当处于seek模式（位点定位）时，跳过DDL的tableMetaCache.apply()调用，避免在位点定位阶段修改表结构缓存。

### 3.3 parse() 分发流程图

```
┌──────────────────────────────────────────────────────────────────────┐
│                  LogEventConvert.parse() 分发流程                      │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│                    LogEvent (二进制事件对象)                           │
│                          │                                           │
│                          ▼                                           │
│              ┌───────────────────────┐                              │
│              │ null or Unknown?      │                              │
│              └───────────┬───────────┘                              │
│                     是    │     否                                    │
│               ┌──────────┘    │                                      │
│               ▼               ▼                                      │
│           return null   ┌────────────────┐                         │
│                          │ switch(eventType)│                        │
│                          └───────┬────────┘                         │
│                                  │                                  │
│         ┌──────────┬─────────┬───┴───┬──────────┬────────┐         │
│         ▼          ▼         ▼       ▼          ▼        ▼         │
│    ┌─────────┐┌─────────┐┌──────┐┌──────┐┌──────┐┌──────┐       │
│    │QUERY    ││XID      ││TABLE ││WRITE ││UPDATE││DELETE│       │
│    │_EVENT   ││_EVENT   ││_MAP  ││_ROWS ││_ROWS ││_ROWS │       │
│    └────┬────┘└────┬────┘└──┬───┘└──┬───┘└──┬───┘└──┬───┘       │
│         │        │       │      │      │      │             │
│         ▼        ▼       ▼      ▼      ▼      ▼             │
│   parseQuery  parseXid parse  parse   parse   parse         │
│   Event()    Event()  Table  Rows    Rows    Rows          │
│              ()       Map()  Event() Event() Event()        │
│                                  │      │      │           │
│                                  ▼      ▼      ▼           │
│                              ┌────────────────────┐        │
│                              │  parseOneRow()     │        │
│                              │  (列对齐核心)      │        │
│                              └────────────────────┘        │
│                                                           │
│    ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐     │
│    │GTID  │  │HEART │  │ROWS  │  │USER  │  │INTVAR│     │
│    │_LOG  │  │BEAT  │  │_QUERY│  │_VAR  │  │_EVENT│     │
│    └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘     │
│       ▼         ▼         ▼         ▼         ▼           │
│   parseGTID parseHeart parseRows parseUser parseInt        │
│   LogEvent  beatLog   QueryEvent VarLog   varLog          │
│             Event()   ()        Event()   Event()         │
│                                                           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. parseQueryEvent() —— DDL/事务/XA处理

### 4.1 Query事件的多重身份

`QUERY_EVENT`（事件类型2）是binlog中最复杂的事件类型之一，因为它承载了多种语义：

```
┌─────────────────────────────────────────────────────────────────────┐
│              QUERY_EVENT 的多重身份与处理策略                          │
├──────────────────────┬─────────────────────────────────────────────┤
│   SQL前缀            │   处理方式                                   │
├──────────────────────┼─────────────────────────────────────────────┤
│ "XA START"           │ → EntryType.TRANSACTIONBEGIN + XA props     │
│ "XA END"             │ → EntryType.TRANSACTIONEND + XA props       │
│ "XA COMMIT"          │ → EntryType.ROWDATA + EventType.XACOMMIT    │
│ "XA ROLLBACK"        │ → EntryType.ROWDATA + EventType.XAROLLBACK  │
│ "...BEGIN"           │ → EntryType.TRANSACTIONBEGIN               │
│ "...COMMIT"          │ → EntryType.TRANSACTIONEND                 │
│ CREATE/ALTER/DROP... │ → EntryType.ROWDATA + EventType.QUERY(DDL) │
│ INSERT/UPDATE/DELETE │ → EntryType.ROWDATA + EventType.QUERY(DML)  │
│ GRANT/REVOKE...      │ → EntryType.ROWDATA + EventType.QUERY(DCL)  │
└──────────────────────┴─────────────────────────────────────────────┘
```

### 4.2 parseQueryEvent() 完整源码

```java
// LogEventConvert.java 第202-307行
private Entry parseQueryEvent(QueryLogEvent event, boolean isSeek) {
    String queryString = event.getQuery();

    // ============ 1. XA事务处理 ============
    if (StringUtils.startsWithIgnoreCase(queryString, XA_START)) {
        // XA START → TransactionBegin
        TransactionBegin.Builder beginBuilder = TransactionBegin.newBuilder();
        beginBuilder.setThreadId(event.getSessionId());
        beginBuilder.addProps(createSpecialPair(XA_TYPE, XA_START));
        beginBuilder.addProps(createSpecialPair(XA_XID, getXaXid(queryString, XA_START)));
        TransactionBegin transactionBegin = beginBuilder.build();
        Header header = createHeader(event.getHeader(), "", "", null);
        return createEntry(header, EntryType.TRANSACTIONBEGIN, transactionBegin.toByteString());

    } else if (StringUtils.startsWithIgnoreCase(queryString, XA_END)) {
        // XA END → TransactionEnd
        TransactionEnd.Builder endBuilder = TransactionEnd.newBuilder();
        endBuilder.setTransactionId(String.valueOf(0L));
        endBuilder.addProps(createSpecialPair(XA_TYPE, XA_END));
        endBuilder.addProps(createSpecialPair(XA_XID, getXaXid(queryString, XA_END)));
        TransactionEnd transactionEnd = endBuilder.build();
        Header header = createHeader(event.getHeader(), "", "", null);
        return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());

    } else if (StringUtils.startsWithIgnoreCase(queryString, XA_COMMIT)) {
        // XA COMMIT → RowData with XACOMMIT
        Header header = createHeader(event.getHeader(), "", "", EventType.XACOMMIT);
        RowChange.Builder rowChangeBuilder = RowChange.newBuilder();
        rowChangeBuilder.setSql(queryString);
        rowChangeBuilder.addProps(createSpecialPair(XA_TYPE, XA_COMMIT));
        rowChangeBuilder.addProps(createSpecialPair(XA_XID, getXaXid(queryString, XA_COMMIT)));
        rowChangeBuilder.setEventType(EventType.XACOMMIT);
        return createEntry(header, EntryType.ROWDATA, rowChangeBuilder.build().toByteString());

    } else if (StringUtils.startsWithIgnoreCase(queryString, XA_ROLLBACK)) {
        // XA ROLLBACK → RowData with XAROLLBACK
        Header header = createHeader(event.getHeader(), "", "", EventType.XAROLLBACK);
        RowChange.Builder rowChangeBuilder = RowChange.newBuilder();
        rowChangeBuilder.setSql(queryString);
        rowChangeBuilder.addProps(createSpecialPair(XA_TYPE, XA_ROLLBACK));
        rowChangeBuilder.addProps(createSpecialPair(XA_XID, getXaXid(queryString, XA_ROLLBACK)));
        rowChangeBuilder.setEventType(EventType.XAROLLBACK);
        return createEntry(header, EntryType.ROWDATA, rowChangeBuilder.build().toByteString());

    // ============ 2. 普通事务BEGIN ============
    } else if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
        TransactionBegin transactionBegin = createTransactionBegin(event.getSessionId());
        Header header = createHeader(event.getHeader(), "", "", null);
        return createEntry(header, EntryType.TRANSACTIONBEGIN, transactionBegin.toByteString());

    // ============ 3. 普通事务COMMIT ============
    } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
        TransactionEnd transactionEnd = createTransactionEnd(0L);
        // MyISAM可能不会有xid事件
        Header header = createHeader(event.getHeader(), "", "", null);
        return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());

    // ============ 4. DDL/DML/DCL处理 ============
    } else {
        boolean notFilter = false;
        EventType type = EventType.QUERY;
        String tableName = null;
        String schemaName = null;

        // 4a. 使用Druid解析DDL语句
        if (useDruidDdlFilter) {
            List<DdlResult> results = DruidDdlParser.parse(queryString, event.getDbName());
            for (DdlResult result : results) {
                if (!processFilter(queryString, result)) {
                    notFilter = true;
                }
            }
            if (results.size() > 0) {
                // 多条DDL只取第一条的schema和table
                type = results.get(0).getType();
                schemaName = results.get(0).getSchemaName();
                tableName = results.get(0).getTableName();
            }
        } else {
            // 降级使用SimpleDdlParser
            DdlResult result = SimpleDdlParser.parse(queryString, event.getDbName());
            if (!processFilter(queryString, result)) {
                notFilter = true;
            }
            type = result.getType();
            schemaName = result.getSchemaName();
            tableName = result.getTableName();
        }

        // 4b. 被过滤则返回null
        if (!notFilter) {
            return null;
        }

        // 4c. 判断是否DML
        boolean isDml = (type == EventType.INSERT
                      || type == EventType.UPDATE
                      || type == EventType.DELETE);

        // 4d. DDL需要更新tableMetaCache
        if (!isSeek && !isDml) {
            EntryPosition position = createPosition(event.getHeader());
            tableMetaCache.apply(position, event.getDbName(), queryString, null);
        }

        // 4e. filterQueryDdl=true时过滤DDL
        if (filterQueryDdl) {
            return null;
        }

        // 4f. 构建RowChange Entry
        Header header = createHeader(event.getHeader(), schemaName, tableName, type);
        RowChange.Builder rowChangeBuilder = RowChange.newBuilder();
        rowChangeBuilder.setIsDdl(!isDml);
        rowChangeBuilder.setSql(queryString);
        if (StringUtils.isNotEmpty(event.getDbName())) {
            rowChangeBuilder.setDdlSchemaName(event.getDbName());
        }
        rowChangeBuilder.setEventType(type);
        return createEntry(header, EntryType.ROWDATA, rowChangeBuilder.build().toByteString());
    }
}
```

### 4.3 parseQueryEvent() 处理流程图

```
┌──────────────────────────────────────────────────────────────────────┐
│                   parseQueryEvent() 处理流程                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   QueryLogEvent (包含SQL文本)                                         │
│           │                                                          │
│           ▼                                                          │
│   ┌───────────────────────┐                                        │
│   │ queryString = getQuery()│                                        │
│   └───────────┬───────────┘                                        │
│               │                                                      │
│   ┌───────────▼───────────┐                                        │
│   │ 以什么开头/结尾?        │                                        │
│   └───────────┬───────────┘                                        │
│               │                                                      │
│   ┌───────────┼───────────┬───────────┬──────────┐               │
│   ▼           ▼           ▼           ▼          ▼              │
│ XA START    XA END     XA COMMIT   XA ROLLBACK  其他                │
│   │           │           │           │          │                 │
│   ▼           ▼           ▼           ▼          ▼                 │
│ Trans-      Trans-      RowData      RowData   ┌──────────┐        │
│ Begin       End         XACOMMIT     XAROLL    │BEGIN?    │        │
│ +XA props  +XA props   +XA props   +XA props  │COMMIT?   │        │
│                                   │          │DDL/DML?  │        │
│                                   │          └────┬─────┘        │
│                                   │               │               │
│                                   │      ┌────────┼────────┐    │
│                                   │      ▼        ▼        ▼    │
│                                   │   BEGIN    COMMIT    DDL/DML│
│                                   │      │        │        │    │
│                                   │      ▼        ▼        ▼    │
│                                   │  Trans-   Trans-   DruidDdl │
│                                   │  Begin    End     Parser   │
│                                   │                    解析     │
│                                   │                      │     │
│                                   │                      ▼     │
│                                   │              ┌──────────┐  │
│                                   │              │process   │  │
│                                   │              │Filter()  │  │
│                                   │              │表名过滤  │  │
│                                   │              └────┬─────┘  │
│                                   │                   │        │
│                                   │            ┌──────┴──────┐ │
│                                   │            ▼             ▼ │
│                                   │         过滤?          不过滤│
│                                   │         return null      │  │
│                                   │                     ┌────┘ │
│                                   │                     ▼      │
│                                   │              ┌───────────┐  │
│                                   │              │ isDml?    │  │
│                                   │              └─────┬─────┘  │
│                                   │              否    │   是   │
│                                   │         ┌──────┘   └──────┘│
│                                   │         ▼                 ▼ │
│                                   │  tableMetaCache    (跳过)   │
│                                   │  .apply()                   │
│                                   │  (更新表结构)                │
│                                   │         │                   │
│                                   │         ▼                   │
│                                   │  ┌────────────────┐         │
│                                   │  │ filterQueryDdl?│         │
│                                   │  └───────┬────────┘         │
│                                   │     是   │   否              │
│                                   │  return  │    │              │
│                                   │  null    ▼    │              │
│                                   │  ┌────────────────┐         │
│                                   │  │ 构建RowChange   │         │
│                                   │  │ Entry          │         │
│                                   │  │ isDdl=true     │         │
│                                   │  │ sql=queryString│         │
│                                   │  └────────────────┘         │
│                                   │                              │
│   └──────────────────────────────┴──────────────────────────────┘│
│                                                                  │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.4 DDL的DruidDdlParser解析

当QUERY_EVENT不是BEGIN/COMMIT/XA时，使用Druid解析SQL语句类型。Druid是阿里开源的SQL解析器，支持MySQL方言：

```java
// 使用Druid解析DDL
List<DdlResult> results = DruidDdlParser.parse(queryString, event.getDbName());
```

`DruidDdlParser.parse()` 返回 `DdlResult` 列表（一条SQL可能包含多个DDL操作，如 `RENAME TABLE a TO b, c TO d`），每个 `DdlResult` 包含：

| 字段 | 说明 | 示例 |
|------|------|------|
| `type` | DDL/DML类型 | `EventType.ALTER`、`EventType.CREATE`、`EventType.INSERT` |
| `schemaName` | 数据库名 | "test_db" |
| `tableName` | 表名 | "t_user" |
| `oriSchemaName` | RENAME源库名 | "test_db" |
| `oriTableName` | RENAME源表名 | "old_table" |
| `renameTableResult` | 多表RENAME的链表 | 下一个DdlResult |

### 4.5 processFilter() —— 表名过滤与表结构缓存清理

`processFilter()` 方法在DDL处理中起到双重作用：表名过滤 + 表结构缓存清理：

```java
// LogEventConvert.java 第313-386行
private boolean processFilter(String queryString, DdlResult result) {
    String schemaName = result.getSchemaName();
    String tableName = result.getTableName();

    // 1. ALTER/ERASE/RENAME需要清理tableMetaCache
    if (tableMetaCache != null
        && (result.getType() == EventType.ALTER
            || result.getType() == EventType.ERASE
            || result.getType() == EventType.RENAME)) {
        for (DdlResult renameResult = result; renameResult != null;
             renameResult = renameResult.getRenameTableResult()) {
            String schemaName0 = renameResult.getSchemaName();
            String tableName0 = renameResult.getTableName();
            if (StringUtils.isNotEmpty(tableName0)) {
                // 精确清理：清理特定表的缓存
                tableMetaCache.clearTableMeta(schemaName0, tableName0);
            } else {
                // 粗粒度清理：清理整个schema的缓存
                tableMetaCache.clearTableMetaWithSchemaName(schemaName0);
            }
        }
    }

    // 2. DDL类型判断与过滤
    if (result.getType() == EventType.ALTER || result.getType() == EventType.ERASE
        || result.getType() == EventType.CREATE || result.getType() == EventType.TRUNCATE
        || result.getType() == EventType.RENAME || result.getType() == EventType.CINDEX
        || result.getType() == EventType.DINDEX) {
        // DDL处理
        if (!filterQueryDdl && (StringUtils.isEmpty(tableName)
            || (result.getType() == EventType.RENAME
                && StringUtils.isEmpty(result.getOriTableName())))) {
            // 解析不出表名，抛异常
            throw new CanalParseException("SimpleDdlParser process query failed...");
        } else {
            // check name filter
            String name = schemaName + "." + tableName;
            if (nameFilter != null && !nameFilter.filter(name)) {
                // 白名单不匹配
                if (result.getType() == EventType.RENAME) {
                    // RENAME：源表或目标表满足一个即可
                    if (nameFilter != null
                        && !nameFilter.filter(result.getOriSchemaName() + "." + result.getOriTableName())) {
                        return true; // 过滤
                    }
                } else {
                    return true; // 过滤
                }
            }
            if (nameBlackFilter != null && nameBlackFilter.filter(name)) {
                // 黑名单匹配
                if (result.getType() == EventType.RENAME) {
                    if (nameBlackFilter != null
                        && nameBlackFilter.filter(result.getOriSchemaName() + "." + result.getOriTableName())) {
                        return true; // 过滤
                    }
                } else {
                    return true; // 过滤
                }
            }
        }
    } else if (result.getType() == EventType.INSERT || result.getType() == EventType.UPDATE
               || result.getType() == EventType.DELETE) {
        // DML处理
        if (filterQueryDml) {
            return true; // 过滤
        }
    } else if (filterQueryDcl) {
        // DCL处理
        return true; // 过滤
    }

    return false; // 不过滤
}
```

**这一步在干什么？**

1. **表结构缓存清理**：当遇到ALTER/DROP/RENAME等DDL时，需要清除对应表的缓存。因为表结构已经变了，下次查询行数据时需要重新获取最新的 `SHOW CREATE TABLE`。

2. **RENAME特殊处理**：`RENAME TABLE a TO b` 涉及两个表名，白名单/黑名单检查时只要源表或目标表满足条件就不过滤。`renameTableResult` 是链表结构，处理多表RENAME。

3. **三类过滤**：
   - `nameFilter`（白名单）：表名不匹配白名单规则 → 过滤
   - `nameBlackFilter`（黑名单）：表名匹配黑名单规则 → 过滤
   - `filterQueryDml`/`filterQueryDdl`/`filterQueryDcl`：按SQL类型过滤

### 4.6 字符集处理

在 `parseQueryEvent` 中，`queryString` 来自 `event.getQuery()`。`QueryLogEvent` 的SQL文本在MySQL中以二进制形式存储，Canal driver层在解析时使用了ISO-8859-1编码读取字节：

```java
// QueryLogEvent 中的SQL获取 (driver层)
// bytes → new String(bytes, ISO-8859-1)
```

在 `parseRowsQueryEvent` 中可以更明显地看到字符集转换：

```java
// LogEventConvert.java 第388-400行
private Entry parseRowsQueryEvent(RowsQueryLogEvent event) {
    if (filterQueryDml) {
        return null;
    }
    String queryString = null;
    try {
        // ISO-8859-1 → 目标charset转换
        queryString = new String(event.getRowsQuery().getBytes(ISO_8859_1), charset);
        // ...
    }
}
```

**为什么用ISO-8859-1？** 因为ISO-8859-1是单字节编码，每个字节1:1映射为字符，不会丢失任何数据。driver层统一用ISO-8859-1读取，convert层再转换为实际的charset（如UTF-8），保证字符集正确。

### 4.7 tableMetaCache.apply() 更新表结构

在DDL处理中，除了清理缓存外，还需要将DDL应用到TSDB（如果启用了TSDB模式）：

```java
// LogEventConvert.java 第286-290行
if (!isSeek && !isDml) {
    EntryPosition position = createPosition(event.getHeader());
    tableMetaCache.apply(position, event.getDbName(), queryString, null);
}
```

`isSeek` 参数的作用：当Canal在位点定位（seek）阶段时，会快速回放binlog到目标位点，此时不应执行DDL的表结构更新，只有正常消费阶段才需要。

`tableMetaCache.apply()` 会将DDL语句和位点信息存入TSDB，后续行数据解析时可以按位点回溯历史表结构。详见第7节。

---

## 5. parseRowsEvent() —— DML行数据解析（核心中的核心）

### 5.1 整体流程概述

`parseRowsEvent()` 是整个Canal binlog解析中最核心的方法，负责将DML行事件（INSERT/UPDATE/DELETE）解析成包含完整列信息的 `CanalEntry.RowChange`：

```
┌──────────────────────────────────────────────────────────────────────┐
│                    parseRowsEvent() 整体流程                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  RowsLogEvent (Write/Update/Delete)                                  │
│      │                                                              │
│      ▼                                                              │
│  ┌─────────────────────┐                                            │
│  │ 1. 获取表结构元数据    │  ← TableMetaCache.getTableMeta()           │
│  │    TableMeta          │    (SHOW CREATE TABLE / TSDB回溯)           │
│  └──────────┬──────────┘                                            │
│             ▼                                                        │
│  ┌─────────────────────┐                                            │
│  │ 2. 确定事件类型        │  ← INSERT / UPDATE / DELETE               │
│  └──────────┬──────────┘                                            │
│             ▼                                                        │
│  ┌─────────────────────┐                                            │
│  │ 3. 表名过滤检查        │  ← nameFilter / nameBlackFilter            │
│  └──────────┬──────────┘                                            │
│             ▼                                                        │
│  ┌─────────────────────┐                                            │
│  │ 4. 创建RowsLogBuffer │  ← 解封装行数据                              │
│  └──────────┬──────────┘                                            │
│             ▼                                                        │
│  ┌─────────────────────┐                                            │
│  │ 5. 逐行迭代           │  ← buffer.nextOneRow()                    │
│  │    while循环          │                                            │
│  │  ┌───────────────┐   │                                            │
│  │  │ parseOneRow() │   │  ← 列对齐 + 类型转换 + 过滤                  │
│  │  └───────────────┘   │                                            │
│  └──────────┬──────────┘                                            │
│             ▼                                                        │
│  ┌─────────────────────┐                                            │
│  │ 6. 构建RowChange      │  ← protobuf序列化                          │
│  │    +Entry返回         │                                            │
│  └─────────────────────┘                                            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 5.2 parseRowsEvent() 完整源码

```java
// LogEventConvert.java (parseRowsEvent核心方法)
private Entry parseRowsEvent(RowsLogEvent event) throws CanalParseException {
    // ============ 1. 获取表结构元数据 ============
    TableMapLogEvent table = event.getTable();
    if (table == null) {
        // 没有TableMap事件，无法解析
        throw new CanalParseException("not found table map event!");
    }

    String dbname = table.getDatabaseName();
    String tbname = event.getTable().getTableName();
    // 获取字符集
    Charset charset = getCharset(dbname);

    // 获取表结构元数据
    TableMeta tableMeta = null;
    try {
        tableMeta = tableMetaCache.getTableMeta(dbname, tbname, event.getTable().getDatabaseName());
    } catch (Throwable e) {
        if (!filterTableError) {
            throw e;
        }
    }

    // ============ 2. 确定事件类型 ============
    EventType eventType = null;
    EventType logEventType = event.getHeader().getType();
    if (logEventType == LogEvent.WRITE_ROWS_EVENT_V1
        || logEventType == LogEvent.WRITE_ROWS_EVENT) {
        eventType = EventType.INSERT;
    } else if (logEventType == LogEvent.UPDATE_ROWS_EVENT_V1
        || logEventType == LogEvent.UPDATE_ROWS_EVENT
        || logEventType == LogEvent.PARTIAL_UPDATE_ROWS_EVENT) {
        eventType = EventType.UPDATE;
    } else if (logEventType == LogEvent.DELETE_ROWS_EVENT_V1
        || logEventType == LogEvent.DELETE_ROWS_EVENT) {
        eventType = EventType.DELETE;
    }

    // ============ 3. 表名过滤检查 ============
    if (filterRows) {
        // 仅订阅非rows数据
        return null;
    }
    if (tableMeta != null) {
        // 检查表名白名单和黑名单
        if (nameFilter != null && !nameFilter.filter(tableMeta.getFullName())) {
            return null;
        }
        if (nameBlackFilter != null && nameBlackFilter.filter(tableMeta.getFullName())) {
            return null;
        }
    }

    // ============ 4. 创建RowsLogBuffer ============
    int columnCount = event.getTable().getColumnCount();
    BitSet columns = event.getColumns();          // present列位图
    BitSet changeColumns = event.getChangeColumns(); // change列位图（UPDATE用）

    RowsLogBuffer buffer = event.getRowsBuf(charset);
    // buffer中包含列类型、列元数据等信息，用于逐字段解封装

    // ============ 5. 逐行迭代解析 ============
    while (buffer.nextOneRow()) {
        // 处理单行数据
        boolean isAfter = false;
        if (eventType == EventType.DELETE) {
            // DELETE只有beforeColumns
            parseOneRow(buffer, tableMeta, eventType, false, rowBuilder, isAfter);
        } else if (eventType == EventType.INSERT) {
            // INSERT只有afterColumns
            parseOneRow(buffer, tableMeta, eventType, true, rowBuilder, isAfter);
        } else if (eventType == EventType.UPDATE) {
            // UPDATE有before + after
            parseOneRow(buffer, tableMeta, eventType, false, rowBuilder, isAfter); // before
            parseOneRow(buffer, tableMeta, eventType, true, rowBuilder, isAfter);  // after
        }
    }

    // ============ 6. 构建RowChange Entry ============
    RowChange.Builder rowChangeBuilder = RowChange.newBuilder();
    rowChangeBuilder.setTableId(table.getTableId());
    rowChangeBuilder.setEventType(eventType);
    // 设置schema和table
    Header header = createHeader(event.getHeader(), dbname, tbname, eventType);
    return createEntry(header, EntryType.ROWDATA, rowChangeBuilder.build().toByteString());
}
```

> **注意**：以上是 `parseRowsEvent` 的逻辑骨架，实际源码中还有更多细节（如partial update处理、charset获取、异常处理等）。核心流程一致。

### 5.3 BitSet columns / changeColumns 的含义

在binlog的行事件中，有两个关键的BitSet（位图）：

| 位图名 | 来源 | 含义 |
|--------|------|------|
| `columns` (present bitmap) | `event.getColumns()` | 标记哪些列在当前事件中存在（非LOB列或minimal模式下只包含部分列） |
| `changeColumns` (change bitmap) | `event.getChangeColumns()` | UPDATE事件中标记哪些列发生了变化 |

**为什么需要present bitmap？**

MySQL的binlog有两种行模式：
1. **FULL模式**（`binlog_row_image=FULL`）：记录所有列，包括没有变化的列
2. **MINIMAL模式**（`binlog_row_image=MINIMAL`）：只记录变化的列（UPDATE）或主键列（DELETE）

```
假设表有4列：id, name, age, city
FULL UPDATE (name='Bob' WHERE id=1):
  columns    = [1,1,1,1]  全部列都在
  changeColumns = [0,1,0,0] 只有name变了

MINIMAL UPDATE (name='Bob' WHERE id=1):
  columns    = [1,1,0,0]  只有id和name
  changeColumns = [0,1,0,0] 只有name变了
```

`parseOneRow()` 需要根据 `columns` 位图跳过不存在的列，保证与表结构元数据的列对齐。

### 5.4 RowsLogBuffer.nextOneRow() 逐行迭代

`RowsLogBuffer` 是行数据的解封装器。`nextOneRow()` 方法每次从packed buffer中提取一行数据：

```java
// RowsLogBuffer.java 第74-100行
public final boolean nextOneRow(BitSet columns, boolean after) {
    final boolean hasOneRow = buffer.hasRemaining();

    if (hasOneRow) {
        // 1. 计算当前行包含多少列
        int column = 0;
        for (int i = 0; i < columnLen; i++) {
            if (columns.get(i)) {
                column++;
            }
        }

        // 2. Partial update特殊处理
        if (after && partial) {
            partialBits.clear();
            long valueOptions = buffer.getPackedLong();
            int PARTIAL_JSON_UPDATES = 1;
            if ((valueOptions & PARTIAL_JSON_UPDATES) != 0) {
                partialBits.set(1);
                buffer.forward((jsonColumnCount + 7) / 8);
            }
        }

        // 3. 读取null bitmap
        nullBitIndex = 0;
        nullBits.clear();
        buffer.fillBitmap(nullBits, column);
    }
    return hasOneRow;
}
```

**这一步在干什么？**

1. **统计列数**：根据 `columns` 位图计算当前行实际包含多少列值（跳过了不存在的列）。
2. **null bitmap**：MySQL行格式中，每行数据开头有一个null位图，标记哪些列是NULL。`fillBitmap(nullBits, column)` 从buffer中读取null位图。
3. **Partial update处理**：MySQL 8.0的部分JSON列更新有特殊的 `value_options` 字段，需要额外处理。

### 5.5 INSERT / DELETE / UPDATE 的列差异

| 事件类型 | beforeColumns | afterColumns | 说明 |
|----------|---------------|--------------|------|
| INSERT | 无 | 有 | 只需要after（新值） |
| DELETE | 有 | 无 | 只需要before（旧值） |
| UPDATE | 有 | 有 | before（旧值）+ after（新值） |

在 `parseRowsEvent` 中，UPDATE事件会连续调用两次 `parseOneRow()`：

```java
if (eventType == EventType.UPDATE) {
    // 第一次：解析before（旧值，用于changed标记比较）
    parseOneRow(buffer, tableMeta, eventType, false, rowBuilder, false);
    // 第二次：解析after（新值）
    parseOneRow(buffer, tableMeta, eventType, true, rowBuilder, true);
}
```

而INSERT和DELETE各只调用一次：

```java
if (eventType == EventType.DELETE) {
    parseOneRow(buffer, tableMeta, eventType, false, rowBuilder, false);
} else if (eventType == EventType.INSERT) {
    parseOneRow(buffer, tableMeta, eventType, true, rowBuilder, true);
}
```

---

## 6. parseOneRow() —— 列对齐的灵魂（极重点）

### 6.1 为什么parseOneRow是"灵魂"？

`parseOneRow()` 是整个Canal binlog解析中最复杂、最关键的方法。它的核心挑战在于：

> **binlog行数据只包含列的位置和二进制值，不包含列名、不包含完整的列类型信息。**
> **列名、主键标记、MySQL类型、是否unsigned等元数据，全部需要从TableMeta补全。**

而且，binlog中的列顺序可能与当前表结构不一致（由于在线DDL加列、RDS隐藏主键等原因），需要对齐处理。

### 6.2 parseOneRow() 的核心逻辑

```java
// LogEventConvert.java (parseOneRow核心逻辑)
private void parseOneRow(RowsLogBuffer buffer, TableMeta tableMeta,
                         EventType eventType, boolean isAfter,
                         RowData.Builder rowBuilder, boolean isAfterRow) {
    // ============ 1. 获取列信息 ============
    BitSet cols = isAfter ? buffer.getAfterColumns() : buffer.getBeforeColumns();

    // 列数对齐：binlog列数 vs TableMeta列数
    int columnCount = tableMeta != null ? tableMeta.getFields().size() : 0;
    int binlogColumnCount = event.getTable().getColumnCount();

    // ============ 2. 逐列遍历与对齐 ============
    for (int i = 0; i < columnCount; i++) {
        FieldMeta fieldMeta = tableMeta.getFields().get(i);
        String columnName = fieldMeta.getColumnName();
        String columnType = fieldMeta.getType();
        boolean isKey = fieldMeta.isKey();

        // 2a. 检查binlog中该列是否存在（present bitmap）
        if (i < binlogColumnCount && !cols.get(i)) {
            // 该列在binlog中不存在（MINIMAL模式或被过滤）
            Column.Builder columnBuilder = Column.newBuilder();
            columnBuilder.setIndex(i);
            columnBuilder.setName(columnName);
            columnBuilder.setIsKey(isKey);
            columnBuilder.setIsNull(true);
            columnBuilder.setSqlType(getSqlType(fieldMeta));
            columnBuilder.setMysqlType(columnType);
            // INSERT: 未提供的列标记为未更新
            columnBuilder.setUpdated(false);
            rowBuilder.addAfterColumns(columnBuilder.build());  // or before
            continue;
        }

        // 2b. 从buffer中读取列值
        int type = event.getTable().getColumnTypes()[i];
        int meta = event.getTable().getColumnMeta()[i];
        Serializable value = buffer.nextValue(columnName, i, type, meta, isBinary);

        // 2c. 类型转换与unsigned修正
        Column.Builder columnBuilder = Column.newBuilder();
        columnBuilder.setIndex(i);
        columnBuilder.setName(columnName);
        columnBuilder.setIsKey(isKey);
        columnBuilder.setSqlType(getSqlType(fieldMeta));
        columnBuilder.setMysqlType(columnType);

        if (value == null) {
            columnBuilder.setIsNull(true);
        } else {
            columnBuilder.setIsNull(false);
            // unsigned修正
            value = fixUnsignedValue(value, type, fieldMeta);
            // 字符集转换
            if (isText(fieldMeta.getType())) {
                value = new String(value.toString().getBytes(ISO_8859_1), charset);
            }
            columnBuilder.setValue(value.toString());
        }

        // 2d. UPDATE的changed标记
        if (eventType == EventType.UPDATE && isAfter) {
            // 比较before和after的值
            boolean changed = isUpdate(beforeValues, value, i);
            columnBuilder.setUpdated(changed);
        } else {
            columnBuilder.setUpdated(true);
        }

        // 2e. 字段级过滤
        if (!needField(columnName, eventType)) {
            continue;
        }

        rowBuilder.addAfterColumns(columnBuilder.build());  // or before
    }
}
```

> **注意**：以上为逻辑骨架，实际源码包含更多细节处理。

### 6.3 列数不匹配处理

这是 `parseOneRow` 中最棘手的问题之一：

```
场景1: RDS隐藏主键列
  表结构(MySQL SHOW CREATE): id, name, age  (3列)
  binlog列数:                id, __hidden_pk, name, age  (4列)
  → binlog比表结构多一列

场景2: 在线DDL加字段
  表结构(当前): id, name, age, city  (4列)
  binlog(旧): id, name, age  (3列)
  → binlog比表结构少一列

场景3: 字段顺序变化
  表结构(旧): id, name, age
  表结构(新): id, age, name  (ALTER后字段顺序可能变化)
  binlog按新顺序: id, age, name
  但TableMeta是旧的: id, name, age
  → 需要按位置对齐
```

处理逻辑的关键代码：

```java
for (int i = 0; i < columnCount; i++) {
    FieldMeta fieldMeta = tableMeta.getFields().get(i);
    // ...
    if (i < binlogColumnCount && !cols.get(i)) {
        // binlog中该列不存在 → 填null
        // ...
        continue;
    }
    // ...
}
```

当 `i >= binlogColumnCount` 时，说明表结构比binlog多列（在线DDL加了字段），这些新增列在旧的binlog事件中不存在，直接填充null。

当 `!cols.get(i)` 时，说明binlog中该列不在present bitmap中（MINIMAL模式跳过了未变化的列），也填充null。

### 6.4 字段元数据对齐

**binlog中有什么？** TableMapLogEvent提供了：
- `columnTypes[]`：每列的二进制类型（如 `MYSQL_TYPE_INT24=9`，`MYSQL_TYPE_VARCHAR=15`）
- `columnMeta[]`：每列的元数据（如VARCHAR的长度、DECIMAL的精度等）

**binlog中没有什么？** 以下信息完全依赖TableMeta补全：

| 元数据 | 来源 | 用途 |
|--------|------|------|
| 列名 | TableMeta | CanalEntry.Column.name |
| 主键标记 | TableMeta | CanalEntry.Column.isKey |
| MySQL类型字符串 | TableMeta | CanalEntry.Column.mysqlType (如"VARCHAR(255)") |
| SQL/JDBC类型 | TableMeta + LogEvent类型映射 | CanalEntry.Column.sqlType |
| 是否unsigned | TableMeta | unsigned修正 |
| 是否NOT NULL | TableMeta | 空值判断 |

```java
// 从TableMapLogEvent获取binlog层信息
int type = event.getTable().getColumnTypes()[i];   // 如 MYSQL_TYPE_LONG=3
int meta = event.getTable().getColumnMeta()[i];     // 如 0

// 从TableMeta补全业务层信息
FieldMeta fieldMeta = tableMeta.getFields().get(i);
String columnName = fieldMeta.getColumnName();  // "user_id"
boolean isKey = fieldMeta.isKey();              // true
String columnType = fieldMeta.getType();        // "int(10) unsigned"
```

### 6.5 MySQL 8.0 binlog_row_metadata=FULL 的交叉校验

MySQL 8.0引入了 `binlog_row_metadata=FULL` 选项，开启后TableMap事件会包含完整的列信息（列名、主键标记等），不再需要从TableMeta补全。

`TableMapLogEvent` 中有 `ColumnInfo` 类来存储这些可选元数据：

```java
// TableMapLogEvent.java - ColumnInfo 内部类
public static class ColumnInfo {
    public String       name;           // 列名
    public int          type;           // MySQL类型
    public String       typeName;       // 类型名称字符串
    public int          meta;           // 元数据
    public boolean      unsigned;       // 是否unsigned
    public boolean      nullable;       // 是否可空
    public String       charsetName;    // 字符集名
    public String       collationName;  // 排序规则名
    public String       defaultValue;   // 默认值
    public boolean      isPrimaryKey;   // 是否主键
    public boolean      isUniqueKey;     // 是否唯一键
    public boolean      isMultipleKey;   // 是否普通索引
}
```

当 `isExistOptionalMetaData()` 返回true时，Canal可以直接使用binlog中的列信息进行交叉校验，确保与TableMeta一致。

### 6.6 Unsigned类型负数溢出修正

这是 `parseOneRow` 中一个非常精妙的处理。MySQL的unsigned整数类型在Java中会溢出为负数，需要修正：

```java
// LogEventConvert.java 中的unsigned修正常量
public static final int    TINYINT_MAX_VALUE   = 256;
public static final int    SMALLINT_MAX_VALUE  = 65536;
public static final int    MEDIUMINT_MAX_VALUE = 16777216;
public static final long   INTEGER_MAX_VALUE   = 4294967296L;
public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");
```

修正逻辑示例：

| MySQL类型 | binlog解析的Java值 | 是否unsigned | 修正后值 | 修正方法 |
|-----------|-------------------|-------------|---------|---------|
| TINYINT | 200 | NO | 200 | 无需修正 |
| TINYINT UNSIGNED | 200 | YES | 200 | 无需修正（200 < 128） |
| TINYINT UNSIGNED | 250（Java解析为-6） | YES | 250 | -6 + 256 = 250 |
| INT UNSIGNED | 4000000000（Java解析为-294967296） | YES | 4000000000 | -294967296 + 4294967296 = 4000000000 |
| BIGINT UNSIGNED | 18000000000000000000（Java解析为负数） | YES | 18000000000000000000 | BigInteger修正 |

```java
// unsigned修正逻辑 (简化)
if (isUnsigned && value instanceof Integer) {
    int intVal = (Integer) value;
    if (intVal < 0) {
        value = intVal + INTEGER_MAX_VALUE;  // 4294967296L
    }
} else if (isUnsigned && value instanceof Long) {
    long longVal = (Long) value;
    if (longVal < 0) {
        value = BigInteger.valueOf(longVal).add(BIGINT_MAX_VALUE);
    }
}
```

### 6.7 BINARY vs TEXT 判断

MySQL中有些类型既可以是二进制也可以是文本（如VARCHAR、BLOB），需要判断后进行不同的处理：

```java
// 判断列是否为文本类型
boolean isText = isText(fieldMeta.getType());
// 如果是文本类型，需要字符集转换
if (isText && value != null) {
    value = new String(value.toString().getBytes(ISO_8859_1), charset);
}
```

`isText()` 方法判断MySQL类型是否为文本：

| MySQL类型 | isText | 说明 |
|-----------|--------|------|
| VARCHAR | true | 需要字符集转换 |
| TEXT | true | 需要字符集转换 |
| CHAR | true | 需要字符集转换 |
| BLOB | false | 二进制，不需要转换 |
| VARBINARY | false | 二进制 |
| BINARY | false | 二进制 |
| JSON | false | 特殊处理（JsonConversion） |

### 6.8 DECIMAL与时间类型处理

**DECIMAL类型**：MySQL的DECIMAL在binlog中以二进制定点数存储，RowsLogBuffer解析为BigDecimal。为了保证精度不丢失，使用 `toPlainString()` 转为字符串：

```java
if (value instanceof BigDecimal) {
    value = ((BigDecimal) value).toPlainString();
    // 不用toString()因为BigDecimal.toString()可能使用科学计数法
}
```

**时间类型**处理：

| MySQL类型 | Java类型 | 转换方式 |
|-----------|----------|---------|
| DATETIME | Timestamp | toString() → "2024-01-15 10:30:00.0" |
| DATE | Date | toString() → "2024-01-15" |
| TIME | Time | toString() → "10:30:00" |
| TIMESTAMP | Timestamp | toString() → "2024-01-15 10:30:00.0" |
| YEAR | Integer | toString() → "2024" |

### 6.9 UPDATE的changed标记

对于UPDATE事件，Canal需要标记哪些列发生了变化。这通过比较before和after的值实现：

```java
if (eventType == EventType.UPDATE && isAfter) {
    // 比较before值和after值
    Serializable beforeValue = beforeRow.get(i);
    boolean changed = !Objects.equals(beforeValue, value);
    columnBuilder.setUpdated(changed);
}
```

对于INSERT，所有after列都标记为 `updated=true`（因为是新插入的值）；对于DELETE，所有before列都标记为 `updated=false`（因为是删除的旧值）。

### 6.10 needField() —— 字段级白/黑名单过滤

`needField()` 方法实现了字段级别的过滤控制：

```java
private boolean needField(String columnName, EventType eventType) {
    // 字段白名单检查
    if (fieldFilterMap != null) {
        List<String> fields = fieldFilterMap.get(fullName);
        if (fields != null && !fields.contains(columnName)) {
            return false; // 该字段不在白名单中
        }
    }
    // 字段黑名单检查
    if (fieldBlackFilterMap != null) {
        List<String> fields = fieldBlackFilterMap.get(fullName);
        if (fields != null && fields.contains(columnName)) {
            return false; // 该字段在黑名单中
        }
    }
    return true; // 不过滤
}
```

### 6.11 parseOneRow() 完整流程图

```
┌──────────────────────────────────────────────────────────────────────┐
│                     parseOneRow() 列对齐流程                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   RowsLogBuffer (行数据)  +  TableMeta (表结构)                       │
│           │                    │                                     │
│           └────────┬───────────┘                                     │
│                    ▼                                                 │
│   ┌───────────────────────────────┐                                 │
│   │ for (i = 0; i < columnCount; i++) │                            │
│   └───────────────┬───────────────┘                                 │
│                   │                                                 │
│         ┌─────────▼─────────┐                                     │
│         │ cols.get(i)?       │                                     │
│         │ (列在binlog中存在?) │                                     │
│         └─────┬───────┬───────┘                                     │
│          否   │       │ 是                                          │
│               ▼       ▼                                             │
│    ┌──────────────┐  ┌──────────────────┐                         │
│    │ 填null       │  │ buffer.nextValue()│                         │
│    │ setIsNull    │  │ 获取二进制值       │                         │
│    │ setUpdated   │  │ 获取javaType      │                         │
│    │ (false)      │  └────────┬─────────┘                         │
│    │ continue     │           ▼                                     │
│    └──────────────┘  ┌──────────────────┐                         │
│                      │ unsigned修正     │                         │
│                      │ (TINYINT+256等)  │                         │
│                      └────────┬─────────┘                         │
│                               ▼                                     │
│                      ┌──────────────────┐                         │
│                      │ isText()?        │                         │
│                      │ 字符集转换       │                         │
│                      │ ISO-8859-1→UTF-8│                         │
│                      └────────┬─────────┘                         │
│                               ▼                                     │
│                      ┌──────────────────┐                         │
│                      │ DECIMAL→         │                         │
│                      │ toPlainString()  │                         │
│                      │ 时间→toString()  │                         │
│                      └────────┬─────────┘                         │
│                               ▼                                     │
│                      ┌──────────────────┐                         │
│                      │ UPDATE:          │                         │
│                      │ isUpdate(before, │                         │
│                      │ value, i)?      │                         │
│                      │ setUpdated()     │                         │
│                      └────────┬─────────┘                         │
│                               ▼                                     │
│                      ┌──────────────────┐                         │
│                      │ needField()?     │                         │
│                      │ 字段级过滤        │                         │
│                      └────────┬─────────┘                         │
│                          过滤  │  不过滤                            │
│                         skip  ▼                                    │
│                      ┌──────────────────┐                         │
│                      │ Column.newBuilder│                         │
│                      │ .setIndex(i)    │                         │
│                      │ .setName(name)  │                         │
│                      │ .setValue(value)│                         │
│                      │ .setSqlType()   │                         │
│                      │ .setMysqlType() │                         │
│                      │ .setIsKey(key)   │                         │
│                      │ .setIsNull(null)│                         │
│                      │ .setUpdated(chg)│                         │
│                      │ .build()        │                         │
│                      └──────────────────┘                         │
│                                                                      │
│   → 添加到 RowData.beforeColumns[] 或 afterColumns[]                 │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 6.12 Column构建结果汇总表

经过 `parseOneRow()` 处理后，每列生成一个 `CanalEntry.Column` 对象：

| Column字段 | 来源 | 说明 |
|-----------|------|------|
| `index` | 循环变量i | 列在表中的位置（0-based） |
| `name` | TableMeta.fieldMeta.columnName | 列名 |
| `value` | buffer.nextValue() + 类型转换 | 列值的字符串表示 |
| `sqlType` | TableMeta + 类型映射 | JDBC类型编号（java.sql.Types） |
| `mysqlType` | TableMeta.fieldMeta.type | MySQL类型字符串（如"int(10) unsigned"） |
| `isKey` | TableMeta.fieldMeta.isKey | 是否主键 |
| `isNull` | buffer null bitmap | 值是否为NULL |
| `updated` | INSERT=true, DELETE=false, UPDATE=比较结果 | 值是否发生变化 |

---

## 7. TableMetaCache —— 表结构缓存

### 7.1 两种工作模式

TableMetaCache 有两种工作模式：

```
┌──────────────────────────────────────────────────────────────────────┐
│                  TableMetaCache 两种工作模式                          │
├──────────────────────┬─────────────────────────────────────────────┤
│   TSDB模式            │   本地缓存模式                                │
│   (推荐)              │   (降级方案)                                  │
├──────────────────────┼─────────────────────────────────────────────┤
│ - 基于嵌入式数据库    │ - 基于Guava LoadingCache                     │
│ - 记录DDL时间序列     │ - 只缓存最新表结构                             │
│ - 按binlog位点回溯    │ - 无法回溯历史表结构                           │
│   历史表结构          │                                              │
│ - 支持: ALTER前后     │ - 场景: 不需要TSDB的简单部署                   │
│   不同表结构          │                                              │
│ - 场景: 生产环境      │                                              │
└──────────────────────┴─────────────────────────────────────────────┘
```

### 7.2 TableMetaCache 类结构

```java
// TableMetaCache.java
public class TableMetaCache {
    // Guava LoadingCache 缓存表结构
    private LoadingCache<String, TableMeta> tableMetaCache;

    // TSDB (可选)
    private TableMetaTSDB tableMetaTSDB;

    // 数据库连接
    private MysqlConnection connection;

    // 是否在RDS上
    private boolean isOnRDS = false;

    // 是否在PolarDB-X上
    private boolean isOnPolarX = false;

    /**
     * 获取表结构元数据
     */
    public TableMeta getTableMeta(String schema, String table, String dbName) {
        // 1. 构造缓存key
        String fullName = getFullName(schema, table);

        // 2. 先查Guava缓存
        TableMeta tableMeta = tableMetaCache.getIfPresent(fullName);

        if (tableMeta == null) {
            // 3. 缓存未命中，从TSDB或MySQL获取
            if (tableMetaTSDB != null) {
                // TSDB模式：按当前位点回溯历史表结构
                tableMeta = tableMetaTSDB.getTableMeta(schema, table, binlogPosition);
            } else {
                // 本地缓存模式：执行SHOW CREATE TABLE获取最新表结构
                tableMeta = getTableMetaFromDB(schema, table);
            }

            // 4. 存入缓存
            if (tableMeta != null) {
                tableMetaCache.put(fullName, tableMeta);
            }
        }

        return tableMeta;
    }

    /**
     * 从MySQL获取表结构 (SHOW CREATE TABLE)
     */
    private TableMeta getTableMetaFromDB(String schema, String table) {
        String createTableSQL = connection.queryCreateTable(schema, table);

        if (createTableSQL != null) {
            // 解析SHOW CREATE TABLE结果
            return parseCreateTable(createTableSQL);
        } else {
            // 降级：使用DESC命令
            return parseDescTable(schema, table);
        }
    }

    /**
     * 应用DDL到TSDB
     */
    public void apply(EntryPosition position, String schema, String ddl, String extra) {
        if (tableMetaTSDB != null) {
            // TSDB模式：记录DDL和位点，构建时间序列
            tableMetaTSDB.apply(position, schema, ddl, extra);
        }
        // 本地缓存模式不需要apply
    }

    /**
     * 清理指定表的缓存
     */
    public void clearTableMeta(String schema, String table) {
        tableMetaCache.invalidate(getFullName(schema, table));
    }

    /**
     * 清理整个schema的缓存
     */
    public void clearTableMetaWithSchemaName(String schema) {
        // 遍历清理该schema下所有表
        for (String key : tableMetaCache.asMap().keySet()) {
            if (key.startsWith(schema + ".")) {
                tableMetaCache.invalidate(key);
            }
        }
    }

    /**
     * 清理所有缓存
     */
    public void clearTableMeta() {
        tableMetaCache.invalidateAll();
    }
}
```

### 7.3 getTableMeta() 缓存策略

```
┌──────────────────────────────────────────────────────────────────────┐
│                   getTableMeta() 缓存策略                             │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   getTableMeta(schema, table)                                        │
│           │                                                          │
│           ▼                                                          │
│   ┌───────────────────────┐                                        │
│   │ Guava LoadingCache     │                                        │
│   │ .getIfPresent(key)     │                                        │
│   └───────────┬───────────┘                                        │
│          命中 │       未命中                                        │
│               ▼           ▼                                        │
│          返回缓存     ┌─────────────┐                              │
│                       │ TSDB模式?   │                              │
│                       └──────┬──────┘                              │
│                         是   │    否                                │
│                    ┌─────────┘    └────────┐                       │
│                    ▼                       ▼                       │
│           ┌────────────────┐     ┌─────────────────┐             │
│           │ TSDB回溯       │     │ SHOW CREATE TABLE│             │
│           │ 按binlog位点   │     │ 从MySQL实时获取   │             │
│           │ 查历史表结构   │     │                   │             │
│           └───────┬────────┘     └────────┬────────┘             │
│                   │                       │                       │
│                   └───────┬───────────────┘                       │
│                           ▼                                        │
│                   ┌─────────────────┐                             │
│                   │ 存入LoadingCache  │                             │
│                   └────────┬────────┘                             │
│                            ▼                                        │
│                        返回TableMeta                               │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 7.4 SHOW CREATE TABLE 解析 + 降级DESC解析

Canal获取表结构有两种方式：

**方式1：SHOW CREATE TABLE（首选）**

```sql
SHOW CREATE TABLE `test_db`.`t_user`;
-- 返回:
-- CREATE TABLE `t_user` (
--   `id` int(11) NOT NULL AUTO_INCREMENT,
--   `name` varchar(255) DEFAULT NULL,
--   `age` tinyint(4) DEFAULT '0',
--   PRIMARY KEY (`id`)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

Canal解析这个DDL语句，提取出列名、列类型、主键信息，构建 `TableMeta` 对象。Druid的SQL Parser用于解析CREATE TABLE语句。

**方式2：DESC（降级）**

当SHOW CREATE TABLE失败（权限不足或表不存在）时，降级使用DESC命令：

```sql
DESC `test_db`.`t_user`;
-- 返回:
-- +-------+--------------+------+-----+---------+----------------+
-- | Field | Type         | Null | Key | Default | Extra          |
-- +-------+--------------+------+-----+---------+----------------+
-- | id    | int(11)      | NO   | PRI | NULL    | auto_increment |
-- | name  | varchar(255) | YES  |     | NULL    |                |
-- | age   | tinyint(4)   | YES  |     | 0       |                |
-- +-------+--------------+------+-----+---------+----------------+
```

DESC的结果更简单，但不包含完整的CREATE TABLE信息（如charset、engine等），仅作为降级方案。

### 7.5 TSDB的DDL时间序列功能

TSDB（Table Structure Database）是Canal的特色功能，它记录了所有DDL语句及其位点，可以按binlog位点回溯任意历史时刻的表结构。

```
时间线:
  T1: CREATE TABLE t_user (id INT, name VARCHAR(100));  位点=1000
  T2: ALTER TABLE t_user ADD COLUMN age INT;            位点=2000
  T3: ALTER TABLE t_user MODIFY COLUMN name VARCHAR(255);  位点=3000

当解析到位点=1500的行事件时:
  → TSDB回溯到T1之后、T2之前的表结构
  → 表结构: (id INT, name VARCHAR(100))

当解析到位点=2500的行事件时:
  → TSDB回溯到T2之后、T3之前的表结构
  → 表结构: (id INT, name VARCHAR(100), age INT)
```

**为什么需要时间序列？** 因为Canal支持断点续传。如果消费到一半暂停，之后表结构发生了ALTER，重新消费时旧的binlog行事件的列数与新表结构不一致，必须用历史表结构才能正确解析。

`apply()` 方法将DDL按位点顺序写入TSDB：

```java
public void apply(EntryPosition position, String schema, String ddl, String extra) {
    // 将DDL语句和位点信息持久化到TSDB
    // 后续getTableMeta()可以按位点回溯
    tableMetaTSDB.apply(position, schema, ddl, extra);
}
```

### 7.6 isOnRDS / isOnPolarX 探测

TableMetaCache在初始化时会探测当前MySQL实例是否为RDS（阿里云RDS）或PolarDB-X：

```java
// 探测RDS
// RDS有些特殊行为:
// 1. 可能有隐藏主键列（如RDS的隐藏_tongo_id）
// 2. SHOW CREATE TABLE可能有额外信息
// 3. 某些系统表的结构不同

// 探测PolarDB-X
// PolarDB-X的binlog格式有差异:
// 1. 可能有额外的分布式事务字段
// 2. GTID格式不同
```

探测方式通常是通过检查MySQL版本字符串中的特征标识：

```java
// 简化逻辑
String version = connection.queryServerVersion();
if (version.contains("rds")) {
    isOnRDS = true;
}
if (version.contains("polardb-x") || version.contains("tddl")) {
    isOnPolarX = true;
}
```

### 7.7 TableMeta和FieldMeta数据结构

```java
// TableMeta.java
public class TableMeta {
    private String schema;           // 数据库名
    private String table;            // 表名
    private List<FieldMeta> fields;  // 字段列表

    public String getFullName() {
        return schema + "." + table;
    }
}

// FieldMeta.java
public class FieldMeta {
    private String columnName;    // 列名
    private String type;         // MySQL类型字符串 (如 "int(10) unsigned")
    private boolean isKey;      // 是否主键
    private boolean isNullable; // 是否可空
    private String defaultValue; // 默认值
    private boolean unsigned;   // 是否unsigned
    private int sqlType;         // JDBC类型 (java.sql.Types)
}
```

**TableMeta与CanalEntry.Column的映射关系**：

```
TableMeta → RowData
  └── FieldMeta → Column
        ├── columnName  → Column.name
        ├── type        → Column.mysqlType
        ├── isKey       → Column.isKey
        ├── sqlType     → Column.sqlType
        └── unsigned    → (用于unsigned修正)
```

---

## 8. parseXidEvent() —— 事务提交

### 8.1 XID事件说明

XID事件（事件类型16）是InnoDB引擎在事务提交时写入binlog的标记。它包含事务的XID（InnoDB内部事务ID），用于保证binlog和InnoDB redo log的一致性（两阶段提交）。

### 8.2 parseXidEvent() 源码

```java
// LogEventConvert.java
private Entry parseXidEvent(XidLogEvent event) {
    // XID事件直接映射为TRANSACTIONEND
    TransactionEnd transactionEnd = createTransactionEnd(event.getXid());
    Header header = createHeader(event.getHeader(), "", "", null);
    return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());
}
```

**这一步在干什么？**

XID事件的处理非常简单，直接转换为 `TRANSACTIONEND` 类型的Entry。`event.getXid()` 是InnoDB的事务ID，作为transactionId存入TransactionEnd。

**XID vs COMMIT (QUERY_EVENT)**：
- InnoDB事务提交：先写COMMIT(QUERY_EVENT)，再写XID_EVENT
- MyISAM事务提交：只写COMMIT(QUERY_EVENT)，没有XID_EVENT
- Canal对MyISAM的COMMIT也生成TRANSACTIONEND（通过parseQueryEvent处理）

---

## 9. parseTableMapEvent() —— TableMap事件

### 9.1 TableMap事件说明

TABLE_MAP_EVENT（事件类型19）出现在每个行事件（WRITE/UPDATE/DELETE）之前，描述了该表的列信息。但在LogDecoder阶段，TableMap事件已经解析并存入LogContext。

在LogEventConvert阶段，`parseTableMapEvent()` 的作用仅限于字符集转换，不产生CanalEntry.Entry：

```java
// LogEventConvert.java
private void parseTableMapEvent(TableMapLogEvent ev) {
    // 只做字符集转换，不产生Entry
    // 将数据库名和表名从ISO-8859-1转为目标charset
    if (ev.getDatabaseName() != null) {
        ev.setDatabaseName(new String(ev.getDatabaseName().getBytes(ISO_8859_1), charset));
    }
    if (ev.getTableName() != null) {
        ev.setTableName(new String(ev.getTableName().getBytes(ISO_8859_1), charset));
    }
}
```

### 9.2 为什么TableMap不产生Entry？

因为TableMap事件只是一个"元数据事件"，它不包含实际的数据变更。下游消费者不需要单独处理TableMap事件，行事件（WRITE/UPDATE/DELETE）中已经包含了所有必要信息（通过LogContext关联了TableMap的表结构）。

在 `parse()` 方法中，TableMap的处理是 `break` 而非 `return`：

```java
case LogEvent.TABLE_MAP_EVENT:
    parseTableMapEvent((TableMapLogEvent) logEvent);
    break;  // 不返回Entry
```

### 9.3 TableMapLogEvent 的关键数据

TableMapLogEvent 提供了行事件解析所需的核心元数据：

| 字段 | 类型 | 说明 |
|------|------|------|
| `tableId` | long | 表的唯一ID（MySQL内部编号） |
| `databaseName` | String | 数据库名 |
| `tableName` | String | 表名 |
| `columnCount` | int | 列总数 |
| `columnTypes[]` | byte[] | 每列的MySQL二进制类型 |
| `columnMeta[]` | int[] | 每列的元数据（如VARCHAR长度） |
| `nullBitmap[]` | BitSet | 列是否可NULL的位图 |
| `columnInfo[]` | ColumnInfo[] | MySQL 8.0可选完整元数据 |

---

## 10. GTID事件处理

### 10.1 GTID事件说明

GTID（Global Transaction Identifier）是MySQL 5.6+引入的全局事务标识，格式为 `uuid:sequence_number`，如 `3E11FA47-71CA-11E1-9E33-C80AA9429562:1`。

### 10.2 parseGTIDLogEvent() 源码

```java
// LogEventConvert.java 第172-187行
private Entry parseGTIDLogEvent(GtidLogEvent logEvent) {
    LogHeader logHeader = logEvent.getHeader();

    // 构建GTID Pair (key-value对)
    Pair.Builder builder = Pair.newBuilder();
    builder.setKey("gtid");
    builder.setValue(logEvent.getGtidStr());

    // MySQL 8.0的GTID可能包含lastCommitted和sequenceNumber
    // 用于乐观并发控制
    if (logEvent.getLastCommitted() != -1) {
        builder.setKey("lastCommitted");
        builder.setValue(String.valueOf(logEvent.getLastCommitted()));
        builder.setKey("sequenceNumber");
        builder.setValue(String.valueOf(logEvent.getSequenceNumber()));
    }

    // 构建Entry
    Header header = createHeader(logHeader, "", "", EventType.GTID);
    return createEntry(header, EntryType.GTIDLOG, builder.build().toByteString());
}
```

### 10.3 GTID的lastCommitted和sequenceNumber

MySQL 8.0引入了基于GTID的乐观并发控制。每个GTID事件包含：

| 字段 | 说明 |
|------|------|
| `gtidStr` | GTID字符串，如 "uuid:5" |
| `lastCommitted` | 该事务开始时，最后一个已提交事务的sequenceNumber |
| `sequenceNumber` | 该事务的sequenceNumber（全局递增） |

```
事务依赖关系:
  T1: lastCommitted=0,  sequenceNumber=1  (可并行执行)
  T2: lastCommitted=0,  sequenceNumber=2  (可与T1并行，因为没有依赖)
  T3: lastCommitted=2,  sequenceNumber=3  (必须等T2提交后才能执行)
```

### 10.4 MariaDB GTID处理

MariaDB的GTID格式与MySQL不同，使用 `domain-server-sequence` 格式：

```java
// LogEventConvert.java 第189-200行
private Entry parseMariaGTIDLogEvent(LogEvent logEvent) {
    LogHeader logHeader = logEvent.getHeader();
    Pair.Builder builder = Pair.newBuilder();
    builder.setKey("gtid");

    if (logEvent instanceof MariaGtidLogEvent) {
        builder.setValue(((MariaGtidLogEvent) logEvent).getGtidStr());
    } else if (logEvent instanceof MariaGtidListLogEvent) {
        builder.setValue(((MariaGtidListLogEvent) logEvent).getGtidStr());
    }

    Header header = createHeader(logHeader, "", "", EventType.GTID);
    return createEntry(header, EntryType.GTIDLOG, builder.build().toByteString());
}
```

---

## 11. HEARTBEAT事件处理

### 11.1 心跳事件说明

MySQL主从复制中，当主库长时间没有数据变更时，从库的IO Thread会定期收到心跳事件（HEARTBEAT_LOG_EVENT），用于保活连接和更新复制位点。

### 11.2 parseHeartbeatLogEvent() 源码

```java
// LogEventConvert.java 第154-161行
private Entry parseHeartbeatLogEvent(HeartbeatLogEvent logEvent) {
    Header.Builder headerBuilder = Header.newBuilder();
    headerBuilder.setEventType(EventType.MHEARTBEAT);
    Entry.Builder entryBuilder = Entry.newBuilder();
    entryBuilder.setHeader(headerBuilder.build());
    entryBuilder.setEntryType(EntryType.HEARTBEAT);
    return entryBuilder.build();
}

// V2版本处理相同
private Entry parseHeartbeatV2LogEvent(HeartbeatV2LogEvent logEvent) {
    Header.Builder headerBuilder = Header.newBuilder();
    headerBuilder.setEventType(EventType.MHEARTBEAT);
    Entry.Builder entryBuilder = Entry.newBuilder();
    entryBuilder.setHeader(headerBuilder.build());
    entryBuilder.setEntryType(EntryType.HEARTBEAT);
    return entryBuilder.build();
}
```

**这一步在干什么？**

心跳事件的处理非常简单：构建一个EntryType为HEARTBEAT的Entry，EventType为MHEARTBEAT。心跳Entry不包含任何数据，仅用于保活和位点更新。

---

## 12. CanalEntry.Entry协议结构

### 12.1 Protocol Buffers协议定义

CanalEntry是Canal自定义的Protocol Buffers消息，定义了从binlog到消费者的数据传输协议。完整结构如下：

```
CanalEntry.Entry (顶层消息)
├── Header
│   ├── logfileOffset (long)      → binlog文件偏移量
│   ├── logfileName (string)      → binlog文件名 (如 "mysql-bin.000001")
│   ├── serverId (uint32)         → MySQL Server ID
│   ├── serverenCode (string)     → 服务端编码
│   ├── executeTime (uint32)      → 事件执行时间戳
│   ├── sourceType [enum]         → 数据源类型 (MYSQL/ORACLE等)
│   ├── schemaName (string)       → 数据库名
│   ├── tableName (string)        → 表名
│   ├── eventLength (uint32)      → 事件长度
│   ├── eventType [enum]          → 事件类型 (INSERT/UPDATE/DELETE/CREATE等)
│   ├── props [] Pair             → 扩展属性 (如gtid)
│   ├── gtid (string)             → GTID字符串
│   └── eventTypeStr (string)     → 事件类型字符串表示
├── entryType [enum]               → Entry类型
│   ├── TRANSACTIONBEGIN          → 事务开始
│   ├── TRANSACTIONEND            → 事务结束
│   ├── ROWDATA                   → 行数据(DML或DDL)
│   ├── HEARTBEAT                 → 心跳
│   └── GTIDLOG                   → GTID日志
└── storeValue (bytes)            → 序列化的消息体
    ├── TransactionBegin           → (entryType=TRANSACTIONBEGIN时)
    │   ├── transactionId (uint32)
    │   └── props [] Pair
    ├── TransactionEnd             → (entryType=TRANSACTIONEND时)
    │   ├── transactionId (string)
    │   └── props [] Pair
    ├── RowChange                  → (entryType=ROWDATA时)
    │   ├── tableId (uint64)       → 表ID
    │   ├── isDdl (bool)           → 是否DDL
    │   ├── eventType [enum]       → INSERT/UPDATE/DELETE/CREATE等
    │   ├── rowDatas [] RowData    → 行数据列表
    │   ├── sql (string)           → DDL的SQL文本
    │   ├── ddlSchemaName (string) → DDL的数据库名
    │   └── props [] Pair          → 扩展属性(如XA类型)
    └── Pair                       → (entryType=GTIDLOG时)
        ├── key (string)
        └── value (string)
```

### 12.2 RowData结构

```
CanalEntry.RowData
├── beforeColumns [] Column        → 变更前的列值
│   ├── index (uint32)             → 列索引
│   ├── sqlType (int32)             → JDBC类型 (java.sql.Types)
│   ├── name (string)               → 列名
│   ├── value (string)              → 列值(字符串)
│   ├── updated (bool)              → 是否变化
│   ├── isKey (bool)                → 是否主键
│   ├── isNull (bool)               → 是否NULL
│   └── mysqlType (string)          → MySQL类型字符串
└── afterColumns [] Column         → 变更后的列值
    └── (同上)
```

### 12.3 各种事件类型的Entry映射汇总

| binlog事件类型 | EntryType | EventType | storeValue包含 | 说明 |
|---------------|-----------|-----------|---------------|------|
| GTID_LOG_EVENT | GTIDLOG | GTID | Pair{gtid, lastCommitted, sequenceNumber} | GTID标识 |
| QUERY_EVENT (BEGIN) | TRANSACTIONBEGIN | (null) | TransactionBegin{threadId} | 事务开始 |
| QUERY_EVENT (XA START) | TRANSACTIONBEGIN | (null) | TransactionBegin{threadId, xaType, xaXid} | XA开始 |
| TABLE_MAP_EVENT | (不产生Entry) | - | - | 只做字符集转换 |
| WRITE_ROWS_EVENT | ROWDATA | INSERT | RowChange{tableId, rowDatas[]} | INSERT行数据 |
| UPDATE_ROWS_EVENT | ROWDATA | UPDATE | RowChange{tableId, rowDatas[]} | UPDATE行数据 |
| DELETE_ROWS_EVENT | ROWDATA | DELETE | RowChange{tableId, rowDatas[]} | DELETE行数据 |
| QUERY_EVENT (DDL) | ROWDATA | CREATE/ALTER/... | RowChange{isDdl=true, sql, ddlSchemaName} | DDL语句 |
| QUERY_EVENT (DML) | ROWDATA | INSERT/UPDATE/DELETE | RowChange{isDdl=false, sql} | DML语句(Query形式) |
| XID_EVENT | TRANSACTIONEND | (null) | TransactionEnd{xid} | 事务提交 |
| QUERY_EVENT (COMMIT) | TRANSACTIONEND | (null) | TransactionEnd{0} | MyISAM提交 |
| QUERY_EVENT (XA END) | TRANSACTIONEND | (null) | TransactionEnd{xaType, xaXid} | XA结束 |
| QUERY_EVENT (XA COMMIT) | ROWDATA | XACOMMIT | RowChange{sql, xaType, xaXid} | XA提交 |
| QUERY_EVENT (XA ROLLBACK) | ROWDATA | XAROLLBACK | RowChange{sql, xaType, xaXid} | XA回滚 |
| HEARTBEAT_LOG_EVENT | HEARTBEAT | MHEARTBEAT | (空) | 心跳保活 |
| ROWS_QUERY_LOG_EVENT | ROWDATA | QUERY | RowChange{sql=原始SQL} | 原始DML文本 |

### 12.4 Header的关键字段说明

```protobuf
message Header {
    optional string  logfileName    = 1;   // binlog文件名
    optional uint64  logfileOffset   = 2;   // binlog偏移量
    optional uint32  serverId        = 3;   // MySQL Server ID
    optional string  serverenCode    = 4;   // 编码
    optional uint32  executeTime     = 5;   // 执行时间 (Unix时间戳)
    optional Type    sourceType      = 6;   // 数据源类型
    optional string  schemaName      = 7;   // 数据库名
    optional string  tableName       = 8;   // 表名
    optional uint32  eventLength     = 9;   // 事件长度
    optional EventType eventType     = 10;  // 事件类型
    repeated Pair    props           = 11;  // 扩展属性
    optional string  gtid            = 12;  // GTID
}
```

**关键字段来源**：

| Header字段 | 来源 | 说明 |
|-----------|------|------|
| `logfileName` | `logPosition.getFileName()` | 来自LogContext中维护的当前binlog文件名 |
| `logfileOffset` | `header.getLogPos()` | 来自LogHeader中的logPos字段 |
| `serverId` | `header.getServerId()` | 来自LogHeader中的serverId字段 |
| `executeTime` | `header.getWhen()` | 来自LogHeader中的timestamp字段 |
| `schemaName` | `table.getDatabaseName()` 或DDL解析结果 | 来自TableMap或DDL解析 |
| `tableName` | `table.getTableName()` 或DDL解析结果 | 来自TableMap或DDL解析 |
| `eventType` | 事件类型映射 | INSERT/UPDATE/DELETE/CREATE等 |
| `gtid` | `header.getGtid()` | 来自GTID事件（通过LogContext关联） |

---

## 13. 完整解析链路总结（从字节到Entry的全路径）

### 13.1 全链路架构图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│              Canal Binlog解析全链路: 从MySQL字节到CanalEntry.Entry               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  MySQL Binlog File                                                             │
│  ┌──┬──────────────────────────────────────────────────────────────────────┐  │
│  │MB│ Event1 │ Event2 │ Event3 │ Event4 │ Event5 │ Event6 │ ...           │  │
│  └──┴────────┴────────┴────────┴────────┴────────┴────────┴──────────────┘  │
│      │        │        │        │        │        │                           │
│      ▼        │        │        │        │        │                           │
│  ┌────────┐   │        │        │        │        │                           │
│  │FORMAT  │   │        │        │        │        │   第一层: LogDecoder       │
│  │_DESC   │   │        │        │        │        │   (字节→LogEvent)          │
│  └───┬────┘   │        │        │        │        │                           │
│      ▼        ▼        ▼        ▼        ▼        ▼                           │
│  ┌────────┐┌───────┐┌───────┐┌───────┐┌───────┐┌───────┐                │
│  │Format  ││GTID   ││QUERY  ││TABLE  ││WRITE  ││XID    │                │
│  │Desc    ││Event  ││Event  ││_MAP   ││_ROWS  ││Event  │                │
│  │Event   ││       ││BEGIN  ││Event  ││Event  ││       │                │
│  └───┬────┘└───┬───┘└───┬───┘└───┬───┘└───┬───┘└───┬───┘                │
│      │        │        │        │        │        │                          │
│      ▼        ▼        │        ▼        │        │                          │
│  context.   context. │   context.     │        │                          │
│  setFormat  setGtid  │   putTable    │        │                          │
│  Desc()     LogEvent │               │        │                          │
│             ()       │               │        │                          │
│                      ▼               ▼        ▼                          │
│               ┌────────────────────────────────────────┐                  │
│               │     LogContext (解码上下文)             │                  │
│               │  - formatDescription                    │                  │
│               │  - mapOfTable{tableId→TableMap}        │                  │
│               │  - gtidLogEvent                         │                  │
│               │  - logPosition                          │                  │
│               └────────────────────────────────────────┘                  │
│                                                                               │
│  ═══════════════════════════════════════════════════════════════════════    │
│                                                                               │
│                    第二层: LogEventConvert.parse()                            │
│                    (LogEvent → CanalEntry.Entry)                             │
│                                                                               │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │GTID Log  │    │QUERY     │    │TABLE_MAP │    │WRITE_ROWS│              │
│  │Event     │    │Event     │    │Event     │    │Event     │              │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘              │
│       ▼              ▼              │              ▼                       │
│  parseGTID     parseQuery    parseTableMap  parseRowsEvent                 │
│  LogEvent()    Event()       Event()        ()                              │
│       │              │              │              │                       │
│       │         ┌────┴────┐         │         ┌────┴────┐                  │
│       │         ▼         ▼         │         ▼         ▼                  │
│       │      BEGIN     DDL/XA        │    getTableMeta  parseOneRow         │
│       │         │         │         │    (TableMeta    ()                  │
│       │         ▼         ▼         │     Cache)  ───┐                      │
│       │      Trans-    RowChange    │              │                          │
│       │      Begin     Builder     │    ┌──────────▼──────┐                 │
│       │         │         │         │    │ TableMetaCache  │                 │
│       │         ▼         ▼         │    │ - LoadingCache  │                 │
│       │      Entry     Entry        │    │ - TSDB (时间序列)│                 │
│       │               (ROWDATA)     │    │ - SHOW CREATE   │                 │
│       │                            │    │   TABLE / DESC  │                 │
│       ▼                            ▼    └─────────────────┘                │
│  ┌──────────┐              (无Entry)          ▼                              │
│  │ Entry    │                           ┌──────────┐                        │
│  │ (GTIDLOG)│                           │ RowData  │                        │
│  └──────────┘                           │ Builder  │                        │
│                                         └────┬─────┘                       │
│                                              ▼                              │
│                                         ┌──────────┐                        │
│                                         │ Entry    │                        │
│                                         │ (ROWDATA)│                        │
│                                         └──────────┘                        │
│                                                                               │
│  ═══════════════════════════════════════════════════════════════════════    │
│                                                                               │
│                    最终输出: List<CanalEntry.Entry>                            │
│                                                                               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│  │ Entry    │ │ Entry    │ │ Entry    │ │ Entry    │ │ Entry    │        │
│  │(GTIDLOG) │ │(TRANS    │ │(ROWDATA  │ │(ROWDATA  │ │(TRANS    │        │
│  │          │ │ BEGIN)   │ │ INSERT)  │ │ UPDATE)  │ │ END)     │        │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘        │
│                                                                               │
│  → 传递给sink模块 → store模块 → 消费者                                       │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 13.2 一条INSERT语句的完整解析路径

以 `INSERT INTO test_db.t_user (id, name) VALUES (1, 'Alice')` 为例：

```
步骤1: MySQL写入binlog
  binlog文件中写入事件序列:
  [GTID_EVENT] [QUERY_EVENT "BEGIN"] [TABLE_MAP_EVENT] [WRITE_ROWS_EVENT] [XID_EVENT]

步骤2: LogDecoder第一层解码 (字节→LogEvent)
  GTID_EVENT     → GtidLogEvent { gtidStr="uuid:1" }
  QUERY_EVENT    → QueryLogEvent { query="BEGIN", sessionId=12345 }
  TABLE_MAP_EVENT→ TableMapLogEvent { tableId=100, dbName="test_db", tableName="t_user",
                                       columnTypes=[MYSQL_TYPE_LONG, MYSQL_TYPE_VARCHAR],
                                       columnMeta=[0, 768] }
  WRITE_ROWS_EVENT → WriteRowsLogEvent { tableId=100,
                                         columns=BitSet{0,1},  // 两列都存在
                                         rowsBuffer=<packed binary data> }
  XID_EVENT      → XidLogEvent { xid=987654 }

  LogContext状态:
    formatDescription = FORMAT_DESCRIPTION_EVENT_5_x
    mapOfTable = { 100 → TableMapLogEvent }
    gtidLogEvent = GtidLogEvent { gtidStr="uuid:1" }
    logPosition = { fileName="mysql-bin.000001", position=1234 }

步骤3: LogEventConvert.parse() 第二层转换 (LogEvent→Entry)
  3a. GtidLogEvent → parseGTIDLogEvent()
      → Entry { header{gtid="uuid:1"}, entryType=GTIDLOG, storeValue=Pair{gtid="uuid:1"} }

  3b. QueryLogEvent "BEGIN" → parseQueryEvent()
      → endsWith("BEGIN") == true
      → Entry { entryType=TRANSACTIONBEGIN,
               storeValue=TransactionBegin{threadId=12345} }

  3c. TableMapLogEvent → parseTableMapEvent()
      → 只做字符集转换，不产生Entry

  3d. WriteRowsLogEvent → parseRowsEvent()
      → tableMetaCache.getTableMeta("test_db", "t_user")
        → SHOW CREATE TABLE → TableMeta{
            fields=[FieldMeta{name="id", type="int(11)", isKey=true},
                    FieldMeta{name="name", type="varchar(255)", isKey=false}] }
      → eventType = INSERT
      → buffer.nextOneRow() → true (有一行)
      → parseOneRow(buffer, tableMeta, INSERT, after=true)
        → i=0: cols.get(0)=true
              → buffer.nextValue("id", 0, MYSQL_TYPE_LONG, 0)
                → value=1 (Integer)
              → isKey=true, sqlType=Types.INTEGER, mysqlType="int(11)"
              → updated=true (INSERT)
              → Column{index=0, name="id", value="1", isKey=true,
                       sqlType=4, mysqlType="int(11)", isNull=false, updated=true}

        → i=1: cols.get(1)=true
              → buffer.nextValue("name", 1, MYSQL_TYPE_VARCHAR, 768)
                → value="Alice" (byte[] → String via charset)
              → isText=true → ISO-8859-1→UTF-8转换
              → isKey=false, sqlType=Types.VARCHAR, mysqlType="varchar(255)"
              → updated=true
              → Column{index=1, name="name", value="Alice", isKey=false,
                       sqlType=12, mysqlType="varchar(255)", isNull=false, updated=true}
      → RowData{ afterColumns=[Column{id}, Column{name}] }
      → RowChange{ tableId=100, eventType=INSERT, rowDatas=[RowData] }
      → Entry{ header{schemaName="test_db", tableName="t_user", eventType=INSERT},
               entryType=ROWDATA,
               storeValue=RowChange }

  3e. XidLogEvent → parseXidEvent()
      → Entry { entryType=TRANSACTIONEND,
               storeValue=TransactionEnd{transactionId="987654"} }

步骤4: 最终输出5个Entry
  1. Entry{GTIDLOG, gtid="uuid:1"}
  2. Entry{TRANSACTIONBEGIN, threadId=12345}
  3. Entry{ROWDATA, INSERT, afterColumns=[id=1, name=Alice]}
  4. Entry{TRANSACTIONEND, transactionId=987654}
  (TableMap不产生Entry)
```

### 13.3 关键设计决策总结

| 设计决策 | 原因 | 影响 |
|----------|------|------|
| 两层解码架构（LogDecoder + LogEventConvert） | 关注点分离：协议解码 vs 业务转换 | 代码清晰，driver层可复用 |
| ISO-8859-1中转字符集 | 单字节编码不丢失数据 | 保证多字节字符集正确转换 |
| TableMeta缓存 + TSDB时间序列 | DDL变更后旧binlog需要历史表结构 | 支持断点续传和在线DDL |
| Unsigned溢出修正 | Java没有unsigned类型 | 保证unsigned TINYINT/INT/BIGINT正确 |
| present bitmap列对齐 | MINIMAL模式只记录部分列 | 支持binlog_row_image=MINIMAL |
| DDL清理缓存 + apply到TSDB | 表结构变更后需要重新获取 | 保证行数据解析使用正确表结构 |
| RDS/PolarX探测 | 云数据库有特殊行为 | 增强云数据库兼容性 |
| filterTableError开关 | 某些表可能已删除或无权限 | 控制是否因表错误中断解析 |

### 13.4 异常处理策略

| 异常场景 | 处理方式 | 配置开关 |
|----------|----------|----------|
| TableMap事件缺失 | 抛出CanalParseException | 无法跳过 |
| 获取表结构失败（表不存在/无权限） | filterTableError=true时跳过，否则抛异常 | `filterTableError` |
| 列数不匹配（binlog vs TableMeta） | 尽力对齐，多出的列填null | - |
| DDL解析失败 | 抛出CanalParseException | - |
| 字符集转换失败 | 使用ISO-8859-1兜底 | - |

### 13.5 性能优化要点

1. **Guava LoadingCache缓存表结构**：避免每次行事件都执行SHOW CREATE TABLE
2. **Long/Integer缓存**：RowsLogBuffer中预分配了128K个Long和Integer对象缓存，减少GC压力
3. **BitSet位图而非数组**：使用BitSet表示列存在性，内存占用更少
4. **Protocol Buffers序列化**：Entry使用protobuf，序列化/反序列化效率高
5. **Druid DDL解析**：Druid解析器比正则表达式更准确，支持复杂DDL

---

## 附录：核心源码文件索引

| 文件路径 | 行数 | 核心职责 |
|----------|------|----------|
| `parse/.../dbsync/LogEventConvert.java` | ~1123 | LogEvent→Entry转换的核心类 |
| `parse/.../dbsync/TableMetaCache.java` | ~313 | 表结构缓存与TSDB管理 |
| `dbsync/.../binlog/LogDecoder.java` | ~553 | 字节流→LogEvent的解码器 |
| `dbsync/.../binlog/LogContext.java` | ~105 | 解码上下文状态容器 |
| `dbsync/.../binlog/LogEvent.java` | ~800+ | 事件类型常量定义基类 |
| `dbsync/.../binlog/event/LogHeader.java` | ~150 | 通用Header(19字节)解析 |
| `dbsync/.../binlog/event/RowsLogBuffer.java` | ~600+ | 行数据解封装器 |
| `dbsync/.../binlog/event/RowsLogEvent.java` | ~300+ | 行事件基类(WRITE/UPDATE/DELETE) |
| `dbsync/.../binlog/event/TableMapLogEvent.java` | ~858 | 表映射事件(列类型/元数据) |
| `dbsync/.../binlog/event/FormatDescriptionLogEvent.java` | - | 格式描述事件(binlog版本) |
| `parse/.../ddl/DruidDdlParser.java` | - | Druid SQL解析器封装 |
| `parse/.../ddl/SimpleDdlParser.java` | - | 简易DDL解析器(降级) |

---

> 本文档基于Canal源码撰写，覆盖了从MySQL binlog字节流到CanalEntry.Entry的完整解析链路。核心关注点在 `LogEventConvert.parseOneRow()` 的列对齐逻辑和 `TableMetaCache` 的表结构管理策略。如需了解更多细节，建议直接阅读上述源码文件。
