# RocketMQ Remoting 通信层源码解析

> 本文档基于 RocketMQ 源码，深入剖析 RocketMQ 自研的基于 Netty 的 RPC 通信框架（remoting 模块），涵盖通信协议设计、编解码、请求-响应匹配、三种调用模式、超时清理、连接管理、GO_AWAY 优雅重连等核心机制。

---

## 目录

1. [全局架构总览](#1-全局架构总览)
2. [RemotingCommand 通信协议详解](#2-remotingcommand-通信协议详解)
3. [NettyEncoder/NettyDecoder 编解码](#3-nettyencodernettydecoder-编解码)
4. [NettyRemotingAbstract 共享引擎](#4-nettyremotingabstract-共享引擎)
5. [三种调用模式](#5-三种调用模式)
6. [ResponseFuture 请求-响应匹配](#6-responsefuture-请求-响应匹配)
7. [scanResponseTable 超时清理](#7-scanresponsetable-超时清理)
8. [failFast 连接关闭处理](#8-failfast-连接关闭处理)
9. [NettyEventExecutor 通道事件机制](#9-nettyeventexecutor-通道事件机制)
10. [NettyRemotingServer 服务端启动](#10-nettyremotingserver-服务端启动)
11. [Pipeline 管道详解](#11-pipeline-管道详解)
12. [NettyRemotingClient 客户端启动](#12-nettyremotingclient-客户端启动)
13. [ChannelWrapper 连接管理](#13-channelwrapper-连接管理)
14. [NameServer 地址轮询](#14-nameserver-地址轮询)
15. [GO_AWAY 优雅重连](#15-go_away-优雅重连)
16. [ServiceThread 基础线程类](#16-servicethread-基础线程类)
17. [RequestCode/ResponseCode 全表](#17-requestcoderesponsecode-全表)
18. [配置参数详解](#18-配置参数详解)
19. [知识点总结](#19-知识点总结)

---

## 1. 全局架构总览

### 1.1 Remoting 模块架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           RocketMQ Remoting 通信框架                                 │
│                                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                        RemotingCommand (通信协议)                              │   │
│  │  code | language | version | opaque(请求ID) | flag | remark | extFields |     │   │
│  │  customHeader | body | serializeType(JSON/ROCKETMQ)                           │   │
│  │  Wire Format: [totalLen(4)] [headerLen|type(4)] [headerData] [bodyData]       │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│  ┌──────────────────────────┐         ┌───────────────────────────────────────┐    │
│  │   NettyRemotingClient     │         │   NettyRemotingServer                  │    │
│  │   (客户端)                 │         │   (服务端)                             │    │
│  │                           │         │                                       │    │
│  │  Bootstrap                │         │  ServerBootstrap                      │    │
│  │  channelTables            │◄───────►│  remotingServerTable                  │    │
│  │  namesrvAddrList          │  Netty  │  bossGroup (1 thread)                 │    │
│  │  eventLoopGroupWorker     │  Channel│  selectorGroup (3 threads)            │    │
│  │                           │         │                                       │    │
│  │  Pipeline:                │         │  Pipeline:                            │    │
│  │  SSL?→Encoder→Decoder→    │         │  Handshake→Encoder→Decoder→           │    │
│  │  IdleState→ConnectMgr→    │         │  Distribution→IdleState→              │    │
│  │  ClientHandler            │         │  ConnMgr→ServerHandler                │    │
│  └───────────┬───────────────┘         └───────────────┬───────────────────────┘    │
│              │                                          │                            │
│              │     ┌────────────────────────────────────┘                            │
│              │     │                                                                  │
│              ▼     ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                    NettyRemotingAbstract (共享引擎)                            │   │
│  │                                                                               │   │
│  │  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────────┐     │   │
│  │  │ semaphoreOneway  │  │ semaphoreAsync    │  │ responseTable           │     │   │
│  │  │ (oneway信号量)    │  │ (async信号量)     │  │ <opaque, ResponseFuture>│     │   │
│  │  └─────────────────┘  └──────────────────┘  └─────────────────────────┘     │   │
│  │                                                                               │   │
│  │  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────────┐     │   │
│  │  │ processorTable   │  │ defaultProcessor  │  │ nettyEventExecutor      │     │   │
│  │  │ <code, Pair>     │  │ (兜底处理器)       │  │ (通道事件)              │     │   │
│  │  └─────────────────┘  └──────────────────┘  └─────────────────────────┘     │   │
│  │                                                                               │   │
│  │  processMessageReceived() → processRequestCommand() / processResponseCommand()│  │
│  │                                                                               │   │
│  │  invokeSyncImpl()  |  invokeAsyncImpl()  |  invokeOnewayImpl()               │   │
│  │  scanResponseTable()  |  failFast()                                        │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│  ┌─────────────────────┐  ┌───────────────────┐  ┌──────────────────────────┐     │
│  │  ResponseFuture      │  │  NettyEvent       │  │  ServiceThread           │     │
│  │  (请求-响应匹配)      │  │  Executor         │  │  (线程基类)              │     │
│  │  opaque | latch |    │  │  (事件分发)        │  │  start/stop/waitFor     │     │
│  │  callback | timeout  │  │                   │  │  Running/wakeup          │     │
│  └─────────────────────┘  └───────────────────┘  └──────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 请求处理流程

```
                          Client                                          Server
                            │                                                │
    invokeSync/Async/Oneway │                                                │
    ┌───────────────────────┘                                                │
    │                                                                        │
    ▼                                                                        │
┌─────────────────────┐                                                      │
│ acquire semaphore    │  (async/oneway 才需要)                               │
│ (信号量限流)          │                                                      │
└─────────┬───────────┘                                                      │
          │                                                                   │
          ▼                                                                   │
┌─────────────────────┐                                                      │
│ build ResponseFuture │  (async/sync 才需要)                                 │
│ put to responseTable │  key = opaque                                       │
└─────────┬───────────┘                                                      │
          │                                                                   │
          ▼                                                                   │
┌─────────────────────┐         Netty          ┌──────────────────────────┐  │
│ channel.writeAndFlush│──────────────────────▶│ NettyDecoder              │  │
│ (RemotingCommand)    │                       │ (解码帧)                   │  │
└─────────────────────┘                       └──────────┬───────────────┘  │
                                                         │                   │
                                                         ▼                   │
                                              ┌──────────────────────────┐  │
                                              │ processMessageReceived() │  │
                                              │ → processRequestCommand()│  │
                                              └──────────┬───────────────┘  │
                                                         │                   │
                                                         ▼                   │
                                              ┌──────────────────────────┐  │
                                              │ lookup processor by code │  │
                                              │ submit to ExecutorService│  │
                                              └──────────┬───────────────┘  │
                                                         │                   │
                                                         ▼                   │
                                              ┌──────────────────────────┐  │
                                              │ processor.processRequest()│ │
                                              │ (业务逻辑)                │  │
                                              └──────────┬───────────────┘  │
                                                         │                   │
                                                         ▼                   │
                                              ┌──────────────────────────┐  │
                                              │ writeResponse()           │  │
                                              │ channel.writeAndFlush()   │  │
                                              └──────────┬───────────────┘  │
                                                         │                   │
    ┌───────────────────────────────────────────────────┘                   │
    │                                                                       │
    ▼                                                                       │
┌─────────────────────────┐                                                │
│ NettyDecoder (解码响应)   │                                                │
└────────┬────────────────┘                                                │
         │                                                                  │
         ▼                                                                  │
┌─────────────────────────┐                                                │
│ processResponseCommand() │                                                │
│ → responseTable.get(opaque)│                                               │
│ → responseFuture.setResponse│                                              │
│ → executeInvokeCallback() │                                                │
│ → remove from responseTable│                                               │
└─────────────────────────┘                                                │
```

### 1.3 核心类索引

| 类名 | 包 | 职责 |
|------|-----|------|
| `RemotingCommand` | `org.apache.rocketmq.remoting.protocol` | 通信协议封装 |
| `NettyEncoder` | `org.apache.rocketmq.remoting.netty` | 编码器 |
| `NettyDecoder` | `org.apache.rocketmq.remoting.netty` | 解码器 |
| `NettyRemotingAbstract` | `org.apache.rocketmq.remoting.netty` | 客户端/服务端共享引擎 |
| `NettyRemotingServer` | `org.apache.rocketmq.remoting.netty` | 服务端实现 |
| `NettyRemotingClient` | `org.apache.rocketmq.remoting.netty` | 客户端实现 |
| `ResponseFuture` | `org.apache.rocketmq.remoting.netty` | 请求-响应匹配 |
| `NettyEventExecutor` | `org.apache.rocketmq.remoting.netty` | 通道事件分发 |
| `ServiceThread` | `org.apache.rocketmq.remoting.common` | 后台线程基类 |
| `NettyServerConfig` | `org.apache.rocketmq.remoting.netty` | 服务端配置 |
| `NettyClientConfig` | `org.apache.rocketmq.remoting.netty` | 客户端配置 |

---

## 2. RemotingCommand 通信协议详解

### 2.1 RemotingCommand 字段定义

`RemotingCommand` 是 RocketMQ 通信层的核心协议类，所有网络请求和响应都以 `RemotingCommand` 为载体：

```java
// RemotingCommand.java
public class RemotingCommand {
    // 请求/响应码（标识请求类型）
    private int code;

    // 语言标识（JAVA, CPP, GO, PYTHON 等）
    private LanguageCode language = LanguageCode.JAVA;

    // 协议版本号
    private int version = 0;

    // 请求 ID（用于匹配请求和响应）
    // 使用 AtomicInteger 全局递增
    private int opaque = requestId.getAndIncrement();

    // 标志位：
    // bit 0: 0=REQUEST, 1=RESPONSE
    // bit 1: 0=非oneway, 1=oneway
    private int flag = 0;

    // 备注（错误描述等）
    private String remark;

    // 扩展字段（HashMap<String, String>）
    // 用于传递键值对形式的参数
    private HashMap<String, String> extFields;

    // 自定义请求头（POJO，通过反射转换为 extFields）
    private transient CommandCustomHeader customHeader;

    // 请求体（byte[]）
    private transient byte[] body;

    // 序列化类型（JSON 或 ROCKETMQ）
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    // 全局请求 ID 生成器
    private static final AtomicInteger requestId = new AtomicInteger(0);

    // 序列化类型配置（全局）
    public static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;
}
```

### 2.2 Wire Format 线格式

RocketMQ 的网络传输格式如下：

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           网络帧格式                                       │
│                                                                          │
│  ┌───────────────┬─────────────────────────────┬──────────────┬────────┐ │
│  │ totalLength   │ headerLength | serializeType │  headerData │  body  │ │
│  │ (4 bytes)     │ (4 bytes)                    │ (变长)        │ (变长)  │ │
│  └───────────────┴─────────────────────────────┴──────────────┴────────┘ │
│                                                                          │
│  totalLength:    整个帧的总长度（不含自身4字节）                            │
│  headerLength:   headerData 的长度（低24位）                               │
│  serializeType:  序列化类型（高8位）                                       │
│                  0x00 = JSON, 0x01 = ROCKETMQ                            │
│  headerData:     序列化后的 RemotingCommand 头部（JSON 或 RocketMQ 二进制）│
│  body:           消息体（byte[]，如消息内容）                               │
└──────────────────────────────────────────────────────────────────────────┘
```

详细分解：

```
Byte:  0  1  2  3  | 4        5        6  7  | 8 ... 8+headerLen-1 | 8+headerLen ... 8+headerLen+bodyLen-1
       ┌──────────┐ ┌──────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────────────┐
       │totalLen  │ │serializeType│hdrLen   │ │    headerData       │ │           bodyData              │
       │(4 bytes) │ │  (1 byte)  │(3 bytes) │ │  (headerLen bytes)  │ │         (bodyLen bytes)         │
       └──────────┘ └──────────────────────┘ └─────────────────────┘ └─────────────────────────────────┘
                      高8位=type     低24位=len
```

### 2.3 encode 编码

```java
// RemotingCommand.java
public ByteBuffer encode() {
    // 1. 序列化头部
    byte[] headerData = this.headerEncode();

    // 2. 计算总长度
    // totalLength = 4(headerLength字段) + headerData.length + body.length
    int totalLength = 4 + headerData.length + (body != null ? body.length : 0);

    // 3. 分配 ByteBuf
    ByteBuffer result = ByteBuffer.allocate(4 + totalLength);

    // 4. 写入 totalLength
    result.putInt(totalLength);

    // 5. 写入 headerLength | serializeType
    // 高8位为序列化类型，低24位为 header 长度
    int headerLength = headerData.length;
    result.put(markProtocolType(headerLength, serializeTypeCurrentRPC));

    // 6. 写入 headerData
    result.put(headerData);

    // 7. 写入 body
    if (body != null) {
        result.put(body);
    }

    result.flip();
    return result;
}

/**
 * 将 headerLength 和 serializeType 组合为一个 int
 */
public static int markProtocolType(int source, SerializeType type) {
    return (type.getCode() << 24) | (source & 0x00FFFFFF);
}

/**
 * 从 int 中提取序列化类型
 */
public static SerializeType getProtocolType(int source) {
    return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
}

/**
 * 从 int 中提取 header 长度
 */
public static int getHeaderLength(int source) {
    return source & 0x00FFFFFF;
}
```

### 2.4 fastEncodeHeader 快速编码

```java
// RemotingCommand.java
public ByteBuffer fastEncodeHeader(ByteBuf out) {
    // 1. 序列化头部
    byte[] headerData = this.headerEncode();

    // 2. 计算 header 长度
    int headerLength = headerData.length;

    // 3. 计算 body 长度
    int bodyLength = this.body != null ? this.body.length : 0;

    // 4. 总长度 = 4 + headerLength + bodyLength
    int totalLength = 4 + headerLength + bodyLength;

    // 5. 写入 totalLength
    out.writeInt(totalLength);

    // 6. 写入 headerLength | serializeType
    out.writeInt(markProtocolType(headerLength, serializeTypeCurrentRPC));

    // 7. 写入 headerData
    out.writeBytes(headerData);

    // 返回 header 的 ByteBuffer（用于 body 写入）
    return null;
}

private byte[] headerEncode() {
    this.makeCustomHeaderToNet();
    if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
        return RocketMQSerializable.rocketMQProtocolEncode(this);
    } else {
        return JsonUtil.toJson(this, false).getBytes(StandardCharsets.UTF_8);
    }
}
```

### 2.5 decode 解码

```java
// RemotingCommand.java
public static RemotingCommand decode(final byte[] array) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(array);
    return decode(byteBuffer);
}

public static RemotingCommand decode(final ByteBuffer buf) {
    // 1. 读取总长度（4字节）
    int length = buf.limit();
    int oriHeaderLen = buf.getInt();

    // 2. 提取 header 长度（低24位）
    int headerLength = getHeaderLength(oriHeaderLen);

    // 3. 提取序列化类型（高8位）
    byte[] headerData;
    if (oriHeaderLen > 0) {
        SerializeType type = getProtocolType(oriHeaderLen);
        headerData = new byte[headerLength];
        buf.get(headerData);

        // 4. 根据序列化类型解码 header
        RemotingCommand cmd;
        switch (type) {
            case JSON:
                cmd = JsonUtil.fromJson(headerData, RemotingCommand.class);
                break;
            case ROCKETMQ:
                cmd = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                break;
            default:
                cmd = null;
                break;
        }

        if (cmd != null) {
            cmd.setSerializeTypeCurrentRPC(type);

            // 5. 解码自定义头部
            if (cmd.customHeader != null) {
                // 反射解码
            }

            // 6. 提取 body
            if (buf.hasRemaining()) {
                int bodyLength = length - 4 - headerLength;
                byte[] bodyData = new byte[bodyLength];
                buf.get(bodyData);
                cmd.body = bodyData;
            }

            return cmd;
        }
    }

    return null;
}

/**
 * 从 ByteBuf 解码（Netty 使用）
 */
public static RemotingCommand decode(final ByteBuf buf) {
    int length = buf.readableBytes();
    int oriHeaderLen = buf.readInt();

    int headerLength = getHeaderLength(oriHeaderLen);
    SerializeType protocolType = getProtocolType(oriHeaderLen);

    byte[] headerData = new byte[headerLength];
    buf.readBytes(headerData);

    RemotingCommand cmd;
    switch (protocolType) {
        case JSON:
            cmd = JsonUtil.fromJson(headerData, RemotingCommand.class);
            break;
        case ROCKETMQ:
            cmd = RocketMQSerializable.rocketMQProtocolDecode(headerData);
            break;
        default:
            throw new RemotingCommandException("Unknown protocol type");
    }

    cmd.setSerializeTypeCurrentRPC(protocolType);

    // body
    int bodyLength = length - 4 - headerLength;
    if (bodyLength > 0) {
        byte[] bodyData = new byte[bodyLength];
        buf.readBytes(bodyData);
        cmd.body = bodyData;
    }

    return cmd;
}
```

### 2.6 标志位 Flag

```java
// RemotingCommand.java
// Flag 位定义
public static final int RPC_TYPE = 0;  // bit 0: 0=REQUEST, 1=RESPONSE
public static final int RPC_ONEWAY = 1; // bit 1: 0=非oneway, 1=oneway

// 判断是否为请求
public boolean isResponseType() {
    int bits = 1 << RPC_TYPE;
    return (this.flag & bits) == bits;
}

// 标记为响应
public void markResponseType() {
    int bits = 1 << RPC_TYPE;
    this.flag |= bits;
}

// 判断是否为 oneway
public boolean isOnewayRPC() {
    int bits = 1 << RPC_ONEWAY;
    return (this.flag & bits) == bits;
}

// 标记为 oneway
public void markOnewayRPC() {
    int bits = 1 << RPC_ONEWAY;
    this.flag |= bits;
}
```

### 2.7 工厂方法

```java
// RemotingCommand.java

/**
 * 创建请求命令
 */
public static RemotingCommand createRequestCommand(int code,
        CommandCustomHeader customHeader) {
    RemotingCommand cmd = new RemotingCommand();
    cmd.setCode(code);
    cmd.setCustomHeader(customHeader);
    cmd.setLanguage(CodeclipseHelper.getLanguageCode());
    // opaque 已在构造函数中通过 AtomicInteger 自增设置
    return cmd;
}

/**
 * 创建响应命令
 */
public static RemotingCommand createResponseCommand(int code, String remark) {
    RemotingCommand cmd = new RemotingCommand();
    cmd.setCode(code);
    cmd.setRemark(remark);
    cmd.markResponseType();
    return cmd;
}

/**
 * 创建错误响应
 */
public static RemotingCommand buildErrorResponse(int code, String remark) {
    final RemotingCommand response = RemotingCommand.createResponseCommand(
        code, remark);
    return response;
}

/**
 * 创建带自定义头的响应
 */
public static RemotingCommand createResponseCommandWithHeader(
        int code, CommandCustomHeader customHeader) {
    RemotingCommand cmd = new RemotingCommand();
    cmd.setCode(code);
    cmd.setCustomHeader(customHeader);
    cmd.markResponseType();
    return cmd;
}
```

### 2.8 CustomHeader 自定义头部

RocketMQ 使用 POJO 作为请求/响应的自定义头部，通过反射机制与 `extFields` 互相转换：

```java
// CommandCustomHeader.java
public interface CommandCustomHeader {
    // 标记接口
    void checkFields() throws RemotingCommandException;
}
```

#### 反射转换机制

```java
// RemotingCommand.java

// 反射缓存表
private static final ConcurrentMap<Class<?>, Field[]
    CLASS_HASH_MAP = new ConcurrentHashMap<>();
private static final ConcurrentMap<Class<?>, Boolean>
    NULLABLE_FIELD_CACHE = new ConcurrentHashMap<>();

/**
 * 将 CustomHeader 的字段值写入 extFields
 */
public void makeCustomHeaderToNet() {
    if (this.customHeader != null) {
        // 获取 CustomHeader 的所有字段
        Field[] fields = getClazzFields(customHeader.getClass());

        if (null == this.extFields) {
            this.extFields = new HashMap<>();
        }

        // 通过反射读取字段值并写入 extFields
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String fieldName = field.getName();
                if (!fieldName.startsWith("this$")) {
                    try {
                        field.setAccessible(true);
                        Object value = field.get(this.customHeader);
                        if (value != null) {
                            this.extFields.put(fieldName, value.toString());
                        }
                    } catch (Exception e) {
                        log.error("Failed to access field", e);
                    }
                }
            }
        }
    }
}

/**
 * 从 extFields 反射构建 CustomHeader
 */
public <T extends CommandCustomHeader> T decodeCommandCustomHeader(
        Class<T> classHeader) throws RemotingCommandException {
    T objectHeader;
    try {
        objectHeader = classHeader.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
        return null;
    }

    if (this.extFields != null) {
        Field[] fields = getClazzFields(classHeader);
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                if (!field.getName().startsWith("this$")) {
                    String fieldName = field.getName();
                    String value = this.extFields.get(fieldName);
                    if (value != null) {
                        try {
                            field.setAccessible(true);
                            // 类型转换
                            Object convertedValue = convertValue(value, field.getType());
                            field.set(objectHeader, convertedValue);
                        } catch (Exception e) {
                            log.error("Failed to set field value", e);
                        }
                    }
                }
            }
        }

        // 调用字段检查方法
        objectHeader.checkFields();
    }

    return objectHeader;
}

/**
 * 获取类的所有字段（带缓存）
 */
private static Field[] getClazzFields(Class<?> clazz) {
    Field[] fields = CLASS_HASH_MAP.get(clazz);
    if (fields == null) {
        fields = clazz.getDeclaredFields();
        CLASS_HASH_MAP.putIfAbsent(clazz, fields);
        fields = CLASS_HASH_MAP.get(clazz);
    }
    return fields;
}
```

#### FastCodesHeader 反射优化

对于性能敏感的头部，RocketMQ 提供了 `FastCodesHeader` 接口避免反射开销：

```java
// FastCodesHeader.java
public interface FastCodesHeader extends CommandCustomHeader {
    /**
     * 直接写入 extFields，避免反射
     */
    void encode(Map<String, String> extFields);

    /**
     * 从 extFields 直接读取，避免反射
     */
    void decode(HashMap<String, String> extFields);
}
```

使用 `FastCodesHeader` 的类（如 `PullMessageRequestHeader`）通过直接编码/解码跳过反射，提升性能：

```java
// PullMessageRequestHeader.java (示例)
public class PullMessageRequestHeader extends FastCodesHeader {
    private String consumerGroup;
    private String topic;
    private int queueId;
    private long queueOffset;
    private int maxMsgNums;
    private int sysFlag;
    private long commitOffset;
    private long suspendTimeoutMillis;
    private String subscription;
    private long subVersion;
    private String expressionType;

    @Override
    public void encode(Map<String, String> extFields) {
        // 直接写入，避免反射开销
        extFields.put("consumerGroup", this.consumerGroup);
        extFields.put("topic", this.topic);
        extFields.put("queueId", String.valueOf(this.queueId));
        extFields.put("queueOffset", String.valueOf(this.queueOffset));
        extFields.put("maxMsgNums", String.valueOf(this.maxMsgNums));
        extFields.put("sysFlag", String.valueOf(this.sysFlag));
        extFields.put("commitOffset", String.valueOf(this.commitOffset));
        extFields.put("suspendTimeoutMillis",
            String.valueOf(this.suspendTimeoutMillis));
        extFields.put("subscription", this.subscription);
        extFields.put("subVersion", String.valueOf(this.subVersion));
        extFields.put("expressionType", this.expressionType);
    }

    @Override
    public void decode(HashMap<String, String> extFields) {
        // 直接读取，避免反射开销
        this.consumerGroup = extFields.get("consumerGroup");
        this.topic = extFields.get("topic");
        this.queueId = Integer.parseInt(extFields.get("queueId"));
        this.queueOffset = Long.parseLong(extFields.get("queueOffset"));
        this.maxMsgNums = Integer.parseInt(extFields.get("maxMsgNums"));
        this.sysFlag = Integer.parseInt(extFields.get("sysFlag"));
        this.commitOffset = Long.parseLong(extFields.get("commitOffset"));
        this.suspendTimeoutMillis =
            Long.parseLong(extFields.get("suspendTimeoutMillis"));
        this.subscription = extFields.get("subscription");
        this.subVersion = Long.parseLong(extFields.get("subVersion"));
        this.expressionType = extFields.get("expressionType");
    }
}
```

### 2.9 SerializeType 序列化类型

```java
// SerializeType.java
public enum SerializeType {
    JSON((byte) 0),       // JSON 序列化
    ROCKETMQ((byte) 1);   // RocketMQ 二进制序列化

    private final byte code;

    SerializeType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static SerializeType valueOf(byte code) {
        for (SerializeType type : SerializeType.values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        return null;
    }
}
```

**JSON vs ROCKETMQ 序列化对比**：

| 维度 | JSON | ROCKETMQ |
|------|------|----------|
| 可读性 | 好（文本格式） | 差（二进制格式） |
| 性能 | 较低 | 较高 |
| 体积 | 较大 | 较小 |
| 默认 | 是 | 否 |

默认使用 JSON 序列化，可通过系统属性 `rocketmq.serialize.type` 配置为 ROCKETMQ。

---

## 3. NettyEncoder/NettyDecoder 编解码

### 3.1 NettyEncoder 编码器

```java
// NettyEncoder.java
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    private static final InternalLogger log =
        InternalLoggerFactory.getLogger(NettyEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand,
            ByteBuf out) throws Exception {
        try {
            // 调用 RemotingCommand 的快速编码方法
            remotingCommand.fastEncodeHeader(out);

            // 写入 body
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            log.error("encode exception, addr={}",
                NettyUtil.parseRemoteAddress(ctx.channel()), e);
            if (remotingCommand != null) {
                log.error("encode exception, command={}", remotingCommand);
            }
            // 发生异常时关闭连接
            ctx.close();
        }
    }
}
```

`NettyEncoder` 继承自 Netty 的 `MessageToByteEncoder<RemotingCommand>`，将 `RemotingCommand` 对象编码为字节流：

```
RemotingCommand 对象
       │
       ▼
  fastEncodeHeader(out)
       │
       ├── out.writeInt(totalLength)      // 4 bytes: 总长度
       ├── out.writeInt(headerLen|type)   // 4 bytes: header长度|序列化类型
       └── out.writeBytes(headerData)     // N bytes: header数据(JSON/ROCKETMQ)
       │
       ▼
  out.writeBytes(body)                    // M bytes: body数据
       │
       ▼
  ByteBuf (网络传输)
```

### 3.2 NettyDecoder 解码器

```java
// NettyDecoder.java
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    private static final InternalLogger log =
        InternalLoggerFactory.getLogger(NettyDecoder.class);

    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty(
            "com.rocketmq.remoting.frameMaxLength",
            String.valueOf(16 * 1024 * 1024)));  // 默认 16MB

    public NettyDecoder() {
        super(
            FRAME_MAX_LENGTH,  // maxFrameLength: 最大帧长度
            0,                  // lengthFieldOffset: 长度字段偏移量（从0开始）
            4,                  // lengthFieldLength: 长度字段占4字节
            0,                  // lengthAdjustment: 不需要调整
            4                   // initialBytesToStrip: 跳过4字节长度字段
        );
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        // 1. LengthFieldBasedFrameDecoder 先读取4字节长度，
        //    然后读取对应长度的数据，返回一个完整的帧 ByteBuf
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        // 2. 将帧解码为 RemotingCommand
        byte[] bytes = new byte[frame.readableBytes()];
        frame.readBytes(bytes);

        try {
            RemotingCommand cmd = RemotingCommand.decode(bytes);
            return cmd;
        } catch (Exception e) {
            log.error("decode exception, addr={}",
                NettyUtil.parseRemoteAddress(ctx.channel()), e);
            // 发生异常时关闭连接
            ctx.close();
            return null;
        } finally {
            // 释放 ByteBuf
            frame.release();
        }
    }

    @Override
    public void decodeLast(ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) throws Exception {
        // 处理 Channel 关闭时的剩余数据
        super.decodeLast(ctx, in, out);
    }
}
```

### 3.3 LengthFieldBasedFrameDecoder 参数说明

```
LengthFieldBasedFrameDecoder 参数:
┌─────────────────────┬───────────────────────────────────────────────┐
│ maxFrameLength      │ 16MB - 单个帧最大长度                           │
│ lengthFieldOffset   │ 0 - 长度字段在帧起始位置                        │
│ lengthFieldLength   │ 4 - 长度字段占4字节(int)                        │
│ lengthAdjustment    │ 0 - 不需要调整                                 │
│ initialBytesToStrip │ 4 - 读取后跳过4字节长度字段                     │
└─────────────────────┴───────────────────────────────────────────────┘

帧结构:
┌──────────┬──────────────────────────────────────┐
│ 4 bytes  │     totalLength bytes                 │
│ (length) │  [headerLen|type(4)] [header] [body]  │
└──────────┴──────────────────────────────────────┘
     │
     ▼ LengthFieldBasedFrameDecoder 读取后跳过4字节
┌──────────────────────────────────────┐
│  totalLength bytes                    │
│  [headerLen|type(4)] [header] [body]  │ ← 传给 RemotingCommand.decode()
└──────────────────────────────────────┘
```

### 3.4 编解码完整流程

```
发送端 (编码):
  RemotingCommand 对象
      │
      ▼ NettyEncoder.encode()
  ┌──────────┬──────────────┬────────────┬────────┐
  │totalLen  │hdrLen|type   │ headerData │  body  │
  │(4 bytes) │  (4 bytes)   │  (变长)     │ (变长) │
  └──────────┴──────────────┴────────────┴────────┘
      │
      ▼ TCP 传输
      │
      ▼

接收端 (解码):
  ┌──────────┬──────────────┬────────────┬────────┐
  │totalLen  │hdrLen|type   │ headerData │  body  │
  │(4 bytes) │  (4 bytes)   │  (变长)     │ (变长) │
  └──────────┴──────────────┴────────────┴────────┘
      │
      ▼ LengthFieldBasedFrameDecoder
  ┌──────────────┬────────────┬────────┐
  │hdrLen|type   │ headerData │  body  │   (跳过 totalLen)
  │  (4 bytes)   │            │        │
  └──────────────┴────────────┴────────┘
      │
      ▼ NettyDecoder.decode() → RemotingCommand.decode()
  ┌──────────────────────────┐
  │ 1. 提取 headerLen (低24位) │
  │ 2. 提取 serializeType (高8位) │
  │ 3. 读取 headerData       │
  │ 4. JSON/ROCKETMQ 反序列化  │
  │ 5. decodeCommandCustomHeader (反射构建自定义头) │
  │ 6. 读取 body             │
  └──────────┬───────────────┘
             │
             ▼
  RemotingCommand 对象
```

---

## 4. NettyRemotingAbstract 共享引擎

### 4.1 类结构

`NettyRemotingAbstract` 是客户端和服务端的共享基类，提供了请求处理、响应处理、超时清理等核心逻辑：

```java
// NettyRemotingAbstract.java
public abstract class NettyRemotingAbstract {
    // ========== 限流信号量 ==========
    // Oneway 请求并发信号量
    protected final Semaphore semaphoreOneway;
    // Async/Sync 请求并发信号量
    protected final Semaphore semaphoreAsync;

    // ========== 请求-响应匹配表 ==========
    // key = opaque (请求ID), value = ResponseFuture
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture>
        responseTable = new ConcurrentHashMap<>(256);

    // ========== 请求处理器表 ==========
    // key = requestCode, value = Pair<处理器, 线程池>
    protected final HashMap<Integer/*requestCode*/,
        Pair<NettyRequestProcessor, ExecutorService>> processorTable =
        new HashMap<>(64);

    // 默认请求处理器
    protected Pair<NettyRequestProcessor, ExecutorService>
        defaultRequestProcessorPair;

    // ========== 通道事件 ==========
    protected NettyEventExecutor nettyEventExecutor;

    // ========== 信号量参数 ==========
    protected int invokeOnewayTimeoutMills = 30000;

    protected final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    // HashedWheelTimer 用于超时扫描
    protected final Timer timer = new HashedWheelTimer(
        new ThreadFactoryImpl("NettyRemotingTimerThread"));
}
```

### 4.2 processMessageReceived 消息接收分发

所有入站消息（无论是请求还是响应）都通过 `processMessageReceived` 分发：

```java
// NettyRemotingAbstract.java
public void processMessageReceived(ChannelHandlerContext ctx,
        RemotingCommand msg) throws Exception {
    final RemotingCommand cmd = msg;
    if (cmd != null) {
        switch (cmd.getType()) {
            case REQUEST_COMMAND:
                // 收到请求 → 处理请求
                processRequestCommand(ctx, cmd);
                break;
            case RESPONSE_COMMAND:
                // 收到响应 → 处理响应
                processResponseCommand(ctx, cmd);
                break;
            default:
                break;
        }
    }
}
```

`RemotingCommand.getType()` 根据 flag 判断：

```java
// RemotingCommand.java
public RemotingCommandType getType() {
    if (this.isResponseType()) {
        return RemotingCommandType.RESPONSE_COMMAND;
    }
    return RemotingCommandType.REQUEST_COMMAND;
}
```

### 4.3 processRequestCommand 请求处理

当收到请求时，根据 requestCode 查找对应的处理器并执行：

```java
// NettyRemotingAbstract.java
public void processRequestCommand(final ChannelHandlerContext ctx,
        final RemotingCommand cmd) {

    // 1. 查找处理器
    final Pair<NettyRequestProcessor, ExecutorService> matched =
        this.processorTable.get(cmd.getCode());
    final Pair<NettyRequestProcessor, ExecutorService> pair =
        null == matched ? this.defaultRequestProcessorPair : matched;

    // 2. 获取请求唯一标识
    final int opaque = cmd.getOpaque();

    // 3. 如果没有找到处理器
    if (pair != null) {
        // 获取处理器绑定的拒绝策略
        Runnable run = buildProcessRequestHandler(ctx, cmd, pair, opaque);

        // 4. 检查是否正在关闭
        if (this.isShuttingDown.get()) {
            // 检查请求方版本
            if (cmd.getVersion() > MQVersion.Version.V4_7_0.ordinal()) {
                // 版本较新，返回 GO_AWAY
                RemotingCommand response = RemotingCommand
                    .createResponseCommand(ResponseCode.GO_AWAY, "");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                return;
            }
        }

        // 5. 检查处理器是否拒绝请求
        try {
            if (pair.getObject1().rejectRequest()) {
                // 处理器拒绝（系统繁忙）
                RemotingCommand response = RemotingCommand
                    .createResponseCommand(
                        ResponseCode.SYSTEM_BUSY,
                        "[REJECTREQUEST]system busy, start flow control");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                return;
            }
        } catch (Exception e) {
            log.error("processRequestCommand rejectRequest exception", e);
        }

        // 6. 提交到处理器绑定的线程池
        try {
            // 包装为 RequestTask
            final RequestTask requestTask = new RequestTask(
                run, ctx.channel(), cmd);
            pair.getObject2().submit(requestTask);
        } catch (RejectedExecutionException e) {
            // 线程池拒绝
            // ...
            if (!shouldDiscard(cmd)) {
                // 非丢弃请求，返回 SYSTEM_BUSY
                RemotingCommand response = RemotingCommand
                    .createResponseCommand(
                        ResponseCode.SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
            }
        }
    } else {
        // 没有找到处理器
        String error = " request type " + cmd.getCode() + " not supported";
        final RemotingCommand response = RemotingCommand
            .createResponseCommand(
                ResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
        response.setOpaque(opaque);
        ctx.writeAndFlush(response);
    }
}
```

### 4.4 buildProcessRequestHandler 构建请求处理Runnable

```java
// NettyRemotingAbstract.java
private Runnable buildProcessRequestHandler(
        ChannelHandlerContext ctx,
        final RemotingCommand cmd,
        final Pair<NettyRequestProcessor, ExecutorService> pair,
        final int opaque) {

    return new Runnable() {
        @Override
        public void run() {
            try {
                // 1. RPC 前置钩子（ACL 认证、追踪等）
                doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddress(ctx),
                    cmd);

                // 2. 请求管道（可选）
                // ...

                // 3. 执行处理器（实际业务逻辑）
                RemotingCommand response =
                    pair.getObject1().processRequest(ctx, cmd);

                // 4. RPC 后置钩子
                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddress(ctx),
                    cmd, response);

                // 5. 写回响应（非 oneway 请求）
                if (response != null) {
                    response.setOpaque(opaque);
                    response.markResponseType();
                    try {
                        ctx.writeAndFlush(response);
                    } catch (Throwable e) {
                        log.error("process request over, but response failed", e);
                    }
                }
            } catch (Exception e) {
                // 异常处理
                log.error("process request exception", e);
                RemotingCommand response = RemotingCommand
                    .createResponseCommand(
                        ResponseCode.SYSTEM_ERROR,
                        "process request exception: " + e.getMessage());
                response.setOpaque(opaque);
                response.markResponseType();
                ctx.writeAndFlush(response);
            }
        }
    };
}
```

### 4.5 writeResponse 写回响应

```java
// NettyRemotingAbstract.java
protected void writeResponse(ChannelHandlerContext ctx, RemotingCommand cmd,
        RemotingCommand response) {
    // 如果是 oneway 请求，不需要响应
    if (cmd.isOnewayRPC()) {
        return;
    }

    // 设置 opaque 与请求匹配
    response.setOpaque(cmd.getOpaque());

    // 标记为响应类型
    response.markResponseType();

    try {
        ctx.writeAndFlush(response);
    } catch (Throwable e) {
        log.error("writeResponse error", e);
    }
}
```

### 4.6 processResponseCommand 响应处理

当收到响应时，通过 `opaque` 在 `responseTable` 中找到对应的 `ResponseFuture` 并完成它：

```java
// NettyRemotingAbstract.java
public void processResponseCommand(ChannelHandlerContext ctx,
        RemotingCommand cmd) {
    // 获取请求 ID（opaque）
    final int opaque = cmd.getOpaque();

    // 从 responseTable 中查找对应的 ResponseFuture
    final ResponseFuture responseFuture = responseTable.get(opaque);
    if (responseFuture != null) {
        // 设置响应命令
        responseFuture.setResponseCommand(cmd);
        // 从表中移除
        responseTable.remove(opaque);

        if (responseFuture.getInvokeCallback() != null) {
            // 异步调用：执行回调
            executeInvokeCallback(responseFuture);
        } else {
            // 同步调用：设置响应并释放锁
            responseFuture.putResponse(cmd);
            responseFuture.release();
        }
    } else {
        // 找不到对应的 ResponseFuture
        // 可能是超时已被清理，或者响应重复到达
        log.warn("receive response, but not matched any request, {}",
            RemotingHelper.parseChannelRemoteAddress(ctx));
        log.warn(cmd.toString());
    }
}

/**
 * 执行异步回调
 */
protected void executeInvokeCallback(final ResponseFuture responseFuture) {
    // 确保回调只执行一次
    if (responseFuture.getInvokeCallback() != null
        && responseFuture.getExecuteCallbackOnlyOnce().compareAndSet(false, true)) {

        // 在回调线程池中执行
        if (responseFuture.getCallbackExecutor() != null) {
            responseFuture.getCallbackExecutor().execute(() -> {
                try {
                    responseFuture.getInvokeCallback().operationComplete(
                        responseFuture);
                } catch (Exception e) {
                    log.error("executeInvokeCallback exception", e);
                }
            });
        } else {
            // 直接在当前线程执行
            responseFuture.getInvokeCallback().operationComplete(responseFuture);
        }
    }
}
```

### 4.7 opaque 请求-响应匹配机制

```
Client (发起请求)                         Server (处理请求)

┌─────────────────┐                      ┌─────────────────┐
│ RemotingCommand │                      │ RemotingCommand │
│ opaque = 100    │                      │ opaque = 100    │
│ code = 11       │ ────── 请求 ──────▶  │ code = 11       │
│ (REQUEST)       │                      │ (REQUEST)       │
└─────────────────┘                      └────────┬────────┘
                                                  │
                                          processor.processRequest()
                                                  │
                                          ┌───────▼────────┐
                                          │ RemotingCommand │
                                          │ opaque = 100    │
                                          │ code = 0        │
                                          │ (RESPONSE)      │
                                          └───────┬────────┘
                                                  │
┌─────────────────┐                      ┌─────────────────┐
│ ResponseFuture  │ ◀────── 响应 ─────── │ opaque = 100    │
│ opaque = 100    │                      │ (RESPONSE)      │
│ in responseTable│                      └─────────────────┘
└─────────────────┘
        │
        ▼
processResponseCommand()
  → responseTable.get(100) → ResponseFuture
  → responseFuture.setResponseCommand(cmd)
  → executeInvokeCallback() or putResponse()
```

**关键点**：
- 请求和响应共享同一个 `opaque` 值
- `opaque` 使用 `AtomicInteger` 全局递增，确保唯一性
- 客户端发起请求时，将 `ResponseFuture` 以 `opaque` 为 key 存入 `responseTable`
- 服务端处理完请求后，将响应的 `opaque` 设为与请求相同的值
- 客户端收到响应后，通过 `opaque` 在 `responseTable` 中找到对应的 `ResponseFuture` 并完成

---

## 5. 三种调用模式

### 5.1 调用模式概览

RocketMQ 支持三种 RPC 调用模式：

| 模式 | 说明 | 是否等待响应 | 信号量 |
|------|------|------------|--------|
| `SYNC` | 同步调用 | 是（阻塞等待） | `semaphoreAsync` |
| `ASYNC` | 异步调用 | 否（回调通知） | `semaphoreAsync` |
| `ONEWAY` | 单向调用 | 否 | `semaphoreOneway` |

### 5.2 invokeSyncImpl 同步调用

```java
// NettyRemotingAbstract.java
public RemotingCommand invokeSyncImpl(final Channel channel,
        final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingSendException,
        RemotingTimeoutException {

    // 获取请求 ID
    final int opaque = request.getOpaque();

    try {
        // 使用 CompletableFuture 进行异步编排
        final ResponseFuture responseFuture = new ResponseFuture(
            channel, opaque, request, timeoutMillis, null, null);
        // 注册到 responseTable
        responseTable.put(opaque, responseFuture);

        // 发送请求（带 listener 处理发送失败）
        channel.writeAndFlush(request).addListener(
            new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    // 发送失败
                    responseFuture.setSendRequestOK(false);
                    requestFail(opaque);
                }
            });

        // 阻塞等待响应
        RemotingCommand responseCommand = responseFuture.waitResponse(
            timeoutMillis);
        if (null == responseCommand) {
            if (responseFuture.isSendRequestOK()) {
                throw new RemotingTimeoutException(
                    RemotingHelper.parseChannelRemoteAddress(channel),
                    timeoutMillis, responseFuture.getCause());
            } else {
                throw new RemotingSendException(
                    RemotingHelper.parseChannelRemoteAddress(channel),
                    responseFuture.getCause());
            }
        }

        return responseCommand;
    } finally {
        // 清理 responseTable
        responseTable.remove(opaque);
    }
}
```

### 5.3 invokeAsyncImpl 异步调用

```java
// NettyRemotingAbstract.java
public void invokeAsyncImpl(final Channel channel,
        final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendException {

    // 1. 获取信号量许可（限制并发异步请求数）
    boolean acquired = this.semaphoreAsync.tryAcquire(
        timeoutMillis, TimeUnit.MILLISECONDS);
    if (acquired) {
        // 获取许可成功
        final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(
            this.semaphoreAsync);
        // ...
    } else {
        // 获取许可失败（并发请求过多）
        if (timeoutMillis <= 0) {
            throw new RemotingTooMuchRequestException("invokeAsyncImpl " +
                "too much request");
        } else {
            throw new RemotingTimeoutException("wait semaphore timeout");
        }
    }

    final int opaque = request.getOpaque();

    // 2. 构建 ResponseFuture（带回调）
    final ResponseFuture responseFuture = new ResponseFuture(
        channel, opaque, request, timeoutMillis, invokeCallback, once);
    responseTable.put(opaque, responseFuture);

    try {
        // 3. 发送请求
        channel.writeAndFlush(request).addListener(
            new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    // 发送失败
                    responseFuture.setSendRequestOK(false);
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    requestFail(opaque);
                }
            });
    } catch (Exception e) {
        // 异常处理
        responseFuture.release();
        responseTable.remove(opaque);
        throw new RemotingSendException("invokeAsyncImpl send exception", e);
    }
}
```

### 5.4 invokeOnewayImpl 单向调用

```java
// NettyRemotingAbstract.java
public void invokeOnewayImpl(final Channel channel,
        final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendException {

    // 1. 标记为 oneway 请求
    request.markOnewayRPC();

    // 2. 获取 oneway 信号量许可
    boolean acquired = this.semaphoreOneway.tryAcquire(
        timeoutMillis, TimeUnit.MILLISECONDS);
    if (acquired) {
        final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(
            this.semaphoreOneway);
        try {
            // 3. 发送请求（不注册 ResponseFuture）
            channel.writeAndFlush(request).addListener(
                new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        // 无论成功失败，都释放信号量
                        once.release();
                        if (!f.isSuccess()) {
                            log.warn("send a oneway message failed.");
                        }
                    }
                });
        } catch (Exception e) {
            once.release();
            throw new RemotingSendException("invokeOnewayImpl send exception", e);
        }
    } else {
        // 获取信号量失败
        throw new RemotingTooMuchRequestException("invokeOnewayImpl " +
            "too much request");
    }
}
```

### 5.5 invokeImpl 通用调用（客户端）

客户端的 `invokeImpl` 是统一入口，处理信号量获取和 GO_AWAY 重连：

```java
// NettyRemotingClient.java (继承 NettyRemotingAbstract)
@Override
public RemotingCommand invokeSync(String addr, final RemotingCommand request,
        long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingSendException, RemotingTimeoutException {

    // 获取或创建 Channel
    final Channel channel = this.getAndCreateChannel(addr);
    if (channel != null && channel.isActive()) {
        try {
            // 调用父类的同步实现
            RemotingCommand response = this.invokeSyncImpl(channel, request,
                timeoutMillis);
            // 处理 GO_AWAY 响应
            if (response != null &&
                response.getCode() == ResponseCode.GO_AWAY) {
                // 重连并重试
                this.closeChannel(addr, channel);
                // ... 重新连接
            }
            return response;
        } catch (RemotingSendException e) {
            // ...
        }
    } else {
        // 通道不可用
        this.closeChannel(addr, channel);
        throw new RemotingConnectException(addr);
    }
}
```

### 5.6 三种调用模式对比

```
SYNC (同步):
  Client                              Server
    │                                   │
    │ Request (opaque=100)              │
    │──────────────────────────────────▶│
    │                                   │
    │   (阻塞等待)                       │ processRequest()
    │   waitResponse(timeout)           │
    │                                   │
    │ Response (opaque=100)             │
    │◀──────────────────────────────────│
    │                                   │
    ▼                                   ▼
  返回 responseCommand

ASYNC (异步):
  Client                              Server
    │                                   │
    │ Request (opaque=100)              │
    │──────────────────────────────────▶│
    │                                   │
    │   (不阻塞，继续执行其他逻辑)        │ processRequest()
    │                                   │
    │ Response (opaque=100)             │
    │◀──────────────────────────────────│
    │                                   │
    ▼                                   ▼
  executeInvokeCallback()
  → invokeCallback.operationComplete()

ONEWAY (单向):
  Client                              Server
    │                                   │
    │ Request (opaque=100, oneway=true) │
    │──────────────────────────────────▶│
    │                                   │
    │   (立即返回，不等响应)              │ processRequest()
    │                                   │
    │                                   │ (不返回响应)
    ▼                                   ▼
  方法结束
```

### 5.7 信号量限流

```java
// NettyRemotingAbstract.java 构造函数
protected NettyRemotingAbstract(int semaphoreOnewaySize, int semaphoreAsyncSize) {
    this.semaphoreOneway = new Semaphore(semaphoreOnewaySize);
    this.semaphoreAsync = new Semaphore(semaphoreAsyncSize);
}
```

| 信号量 | 客户端默认值 | 服务端默认值 | 说明 |
|--------|------------|------------|------|
| `semaphoreOneway` | `clientOnewaySemaphoreValue` | `serverOnewaySemaphoreValue` (256) | 限制并发 oneway 请求 |
| `semaphoreAsync` | `clientAsyncSemaphoreValue` | `serverAsyncSemaphoreValue` (64) | 限制并发 async/sync 请求 |

---

## 6. ResponseFuture 请求-响应匹配

### 6.1 ResponseFuture 类定义

```java
// ResponseFuture.java
public class ResponseFuture {
    // 关联的 Channel
    private final Channel channel;

    // 请求 ID（opaque）
    private final int opaque;

    // 原始请求
    private final RemotingCommand request;

    // 超时时间
    private final long timeoutMillis;

    // 异步回调
    private final InvokeCallback invokeCallback;

    // 请求开始时间
    private final long beginTimestamp = System.currentTimeMillis();

    // 同步等待用的 CountDownLatch
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    // 信号量释放器（确保只释放一次）
    private final SemaphoreReleaseOnlyOnce once;

    // 确保回调只执行一次
    private final AtomicBoolean executeCallbackOnlyOnce =
        new AtomicBoolean(false);

    // 响应命令
    private volatile RemotingCommand responseCommand;

    // 请求是否发送成功
    private volatile boolean sendRequestOK = false;

    // 异常原因
    private volatile Throwable cause;

    // 回调线程池
    private ExecutorService callbackExecutor;

    public ResponseFuture(Channel channel, int opaque,
            RemotingCommand request, long timeoutMillis,
            InvokeCallback invokeCallback,
            SemaphoreReleaseOnlyOnce once) {
        this.channel = channel;
        this.opaque = opaque;
        this.request = request;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }
}
```

### 6.2 waitResponse 同步等待

```java
// ResponseFuture.java
public RemotingCommand waitResponse(final long timeoutMillis)
        throws InterruptedException {
    // 在 CountDownLatch 上阻塞等待
    this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);

    if (this.responseCommand == null) {
        // 超时或被中断，返回 null
        return null;
    }

    return this.responseCommand;
}
```

### 6.3 putResponse 设置响应

```java
// ResponseFuture.java
public void putResponse(final RemotingCommand responseCommand) {
    this.responseCommand = responseCommand;
    // 倒计数，唤醒 waitResponse 的线程
    this.countDownLatch.countDown();
}
```

### 6.4 executeInvokeCallback 执行回调

```java
// ResponseFuture.java
public void executeInvokeCallback() {
    // 确保回调只执行一次
    if (this.invokeCallback != null) {
        if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
            // 执行回调
            this.invokeCallback.operationComplete(this);
        }
    }
}
```

### 6.5 release 释放信号量

```java
// ResponseFuture.java
public void release() {
    if (this.once != null) {
        this.once.release();
    }
}
```

### 6.6 isTimeout 判断超时

```java
// ResponseFuture.java
public boolean isTimeout() {
    long diff = System.currentTimeMillis() - this.beginTimestamp;
    return diff > this.timeoutMillis;
}
```

### 6.7 SemaphoreReleaseOnlyOnce 信号量释放器

```java
// SemaphoreReleaseOnlyOnce.java
public class SemaphoreReleaseOnlyOnce {
    private final AtomicBoolean released = new AtomicBoolean(false);
    private final Semaphore semaphore;

    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public void release() {
        if (this.semaphore != null) {
            // 确保只释放一次
            if (this.released.compareAndSet(false, true)) {
                this.semaphore.release();
            }
        }
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}
```

### 6.8 ResponseFuture 生命周期

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ResponseFuture 生命周期                           │
│                                                                         │
│  创建阶段:                                                               │
│  ┌─────────────────────────────────────────────────┐                    │
│  │ new ResponseFuture(channel, opaque, request,    │                    │
│  │   timeout, callback, semaphoreRelease)          │                    │
│  │ → 记录 beginTimestamp                            │                    │
│  └────────────────────┬────────────────────────────┘                    │
│                       │                                                 │
│                       ▼                                                 │
│  注册阶段:                                                               │
│  ┌─────────────────────────────────────────────────┐                    │
│  │ responseTable.put(opaque, responseFuture)        │                    │
│  └────────────────────┬────────────────────────────┘                    │
│                       │                                                 │
│                       ▼                                                 │
│  发送阶段:                                                               │
│  ┌─────────────────────────────────────────────────┐                    │
│  │ channel.writeAndFlush(request)                   │                    │
│  │ → 成功: sendRequestOK = true                     │                    │
│  │ → 失败: requestFail(opaque)                      │                    │
│  └────────────────────┬────────────────────────────┘                    │
│                       │                                                 │
│         ┌─────────────┴─────────────┐                                   │
│         ▼                           ▼                                   │
│  同步模式                         异步模式                               │
│  ┌──────────┐                   ┌──────────────┐                        │
│  │waitResponse│                  │ (不阻塞)      │                        │
│  │(阻塞等待)  │                   │              │                        │
│  └─────┬────┘                   └──────┬───────┘                        │
│        │                               │                                 │
│        ▼                               ▼                                 │
│  响应到达阶段:                                                           │
│  ┌─────────────────────────────────────────────────┐                    │
│  │ processResponseCommand(ctx, responseCmd)         │                    │
│  │ → responseTable.get(opaque) → ResponseFuture     │                    │
│  │ → responseFuture.setResponseCommand(cmd)         │                    │
│  │ → responseTable.remove(opaque)                  │                    │
│  └────────────────────┬────────────────────────────┘                    │
│                       │                                                 │
│         ┌─────────────┴─────────────┐                                   │
│         ▼                           ▼                                   │
│  同步模式                         异步模式                               │
│  ┌──────────────┐                ┌──────────────────────┐               │
│  │putResponse() │                │executeInvokeCallback()│               │
│  │→ countDown() │                │→ callback.operation   │               │
│  │→ waitResponse│                │   Complete()         │               │
│  │   返回        │                │→ release()           │               │
│  └──────────────┘                └──────────────────────┘               │
│                                                                         │
│  清理阶段:                                                               │
│  ┌─────────────────────────────────────────────────┐                    │
│  │ responseTable.remove(opaque)                     │                    │
│  │ responseFuture.release()  (释放信号量)            │                    │
│  └─────────────────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 7. scanResponseTable 超时清理

### 7.1 超时扫描机制

RocketMQ 使用 `HashedWheelTimer` 定时扫描 `responseTable`，清理超时的请求：

```java
// NettyRemotingAbstract.java
public void scanResponseTable() {
    // 遍历所有 ResponseFuture
    List<ResponseFuture> rfList = new LinkedList<>();
    Iterator<Map.Entry<Integer, ResponseFuture>> it =
        this.responseTable.entrySet().iterator();
    while (it.hasNext()) {
        Map.Entry<Integer, ResponseFuture> next = it.next();
        ResponseFuture rep = next.getValue();

        // 检查是否超时
        if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000)
                <= System.currentTimeMillis()) {
            // 超时，移除
            it.remove();
            rfList.add(rep);
            log.warn("remove timeout request, " + rep);
        }
    }

    // 处理超时的 ResponseFuture
    for (ResponseFuture rf : rfList) {
        try {
            if (rf.getInvokeCallback() != null) {
                // 异步调用：执行超时回调
                executeInvokeCallback(rf);
            } else {
                // 同步调用：设置 null 响应，唤醒等待线程
                rf.putResponse(null);
            }
        } catch (Throwable e) {
            log.error("scanResponseTable, execute callback in timeout", e);
        } finally {
            // 释放信号量
            rf.release();
        }
    }
}
```

### 7.2 定时调度

```java
// NettyRemotingServer.java / NettyRemotingClient.java start()
this.timer.scheduleAtFixedRate(new TimerTask() {
    @Override
    public void run(Timeout t) {
        try {
            NettyRemotingServer.this.scanResponseTable();
        } catch (Throwable e) {
            log.error("scanResponseTable exception", e);
        }
    }
}, 1000 * 3, 1000);  // 每 1 秒执行一次
```

### 7.3 超时处理流程

```
scanResponseTable() 每 1 秒执行
       │
       ▼
┌──────────────────────────────────────┐
│ 遍历 responseTable                   │
│ 对每个 ResponseFuture:                │
│   if (beginTime + timeout + 1000     │
│       <= now) → 超时                  │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│ 从 responseTable 移除                 │
│ 加入超时列表                          │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│ 对超时的 ResponseFuture:              │
│                                      │
│ 有 callback?                         │
│   Yes → executeInvokeCallback()      │
│          (回调中通知超时)              │
│   No  → putResponse(null)            │
│          (唤醒同步等待线程)             │
│                                      │
│ release() (释放信号量)                │
└──────────────────────────────────────┘
```

### 7.4 HashedWheelTimer 时间轮

RocketMQ 使用 Netty 的 `HashedWheelTimer` 进行超时检测。时间轮是一种高效的定时器实现：

```java
// NettyRemotingAbstract.java
protected final Timer timer = new HashedWheelTimer(
    new ThreadFactoryImpl("NettyRemotingTimerThread"));
```

时间轮原理：
```
时间轮 (HashedWheelTimer):
┌────┬────┬────┬────┬────┬────┬────┬────┐
│ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │ 6  │ 7  │  ← 槽位
└─┬──┴────┴────┴────┴────┴────┴────┴────┘
  │
  ▼ 指针每 tickDuration 旋转一个槽位

每个槽位维护一个超时任务链表
指针扫到某个槽位时，执行该槽位所有任务
```

优势：O(1) 添加任务，不需要像 `ScheduledThreadPoolExecutor` 那样维护优先队列。

---

## 8. failFast 连接关闭处理

### 8.1 failFast 机制

当 Channel 关闭时，需要快速失败所有关联的未完成请求，避免请求无限等待：

```java
// NettyRemotingAbstract.java
public void failFast(final Channel channel) {
    // 扫描 responseTable 中所有与该 Channel 关联的 ResponseFuture
    Iterator<Map.Entry<Integer, ResponseFuture>> it =
        responseTable.entrySet().iterator();
    while (it.hasNext()) {
        Map.Entry<Integer, ResponseFuture> entry = it.next();
        Integer opaque = entry.getKey();
        ResponseFuture responseFuture = entry.getValue();

        if (responseFuture.getChannel() == channel) {
            // 找到关联的 ResponseFuture
            it.remove();

            // 标记为发送失败
            responseFuture.setSendRequestOK(false);
            responseFuture.setCause(
                new RemotingConnectException(
                    "channel closed, " + channel.remoteAddress()));

            // 处理失败
            if (responseFuture.getInvokeCallback() != null) {
                // 异步：执行回调
                executeInvokeCallback(responseFuture);
            } else {
                // 同步：设置 null，唤醒等待
                responseFuture.putResponse(null);
            }

            // 释放信号量
            responseFuture.release();

            log.warn("failFast: remove response future, opaque={}", opaque);
        }
    }
}
```

### 8.2 failFast 触发时机

```
Channel 关闭事件
       │
       ├── NettyConnectManageHandler (客户端)
       │   → onChannelClose → closeChannel(addr, channel)
       │                    → failFast(channel)
       │
       └── NettyConnetManageHandler (服务端)
           → onChannelClose → doChannelCloseEvent
                            → failFast(channel)
```

### 8.3 requestFail 请求失败处理

```java
// NettyRemotingAbstract.java
protected void requestFail(final int opaque) {
    ResponseFuture responseFuture = responseTable.remove(opaque);
    if (responseFuture != null) {
        responseFuture.setSendRequestOK(false);
        responseFuture.setCause(
            new RemotingSendException("request fail"));

        if (responseFuture.getInvokeCallback() != null) {
            // 异步：执行回调
            executeInvokeCallback(responseFuture);
        } else {
            // 同步：设置 null，唤醒等待
            responseFuture.putResponse(null);
        }

        // 释放信号量
        responseFuture.release();
    }
}
```

---

## 9. NettyEventExecutor 通道事件机制

### 9.1 NettyEvent 事件定义

```java
// NettyEvent.java
public class NettyEvent {
    private final NettyEventType type;    // 事件类型
    private final String remoteAddr;      // 远程地址
    private final Channel channel;        // 关联的 Channel

    public NettyEvent(NettyEventType type, String remoteAddr, Channel channel) {
        this.type = type;
        this.remoteAddr = remoteAddr;
        this.channel = channel;
    }
}

// NettyEventType.java
public enum NettyEventType {
    CONNECT,     // 连接建立
    CLOSE,       // 连接关闭
    IDLE,        // 空闲
    EXCEPTION,   // 异常
    ACTIVE       // 活跃
}
```

### 9.2 NettyEventExecutor 事件执行器

```java
// NettyEventExecutor.java
public class NettyEventExecutor extends ServiceThread {
    private final InternalLogger log = InternalLoggerFactory
        .getLogger(NettyEventExecutor.class);

    // 事件队列（容量 10000）
    private final LinkedBlockingQueue<NettyEvent> eventQueue =
        new LinkedBlockingQueue<>(10000);

    // 通道事件监听器
    private final ChannelEventListener channelEventListener;

    public NettyEventExecutor(ChannelEventListener channelEventListener) {
        this.channelEventListener = channelEventListener;
    }

    /**
     * 将事件放入队列
     */
    public void putNettyEvent(final NettyEvent event) {
        // offer 非阻塞，队列满时丢弃
        boolean result = this.eventQueue.offer(event);
        if (!result) {
            log.warn("event queue is full, drop event: {}", event);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                if (event != null) {
                    // 处理事件
                    this.processEvent(event);
                }
            } catch (Exception e) {
                log.error("processEvent exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    private void processEvent(NettyEvent event) {
        switch (event.getType()) {
            case CONNECT:
                this.channelEventListener.onChannelConnect(
                    event.getRemoteAddr(), event.getChannel());
                break;
            case CLOSE:
                this.channelEventListener.onChannelClose(
                    event.getRemoteAddr(), event.getChannel());
                break;
            case IDLE:
                this.channelEventListener.onChannelIdle(
                    event.getRemoteAddr(), event.getChannel());
                break;
            case EXCEPTION:
                this.channelEventListener.onChannelException(
                    event.getRemoteAddr(), event.getChannel());
                break;
            case ACTIVE:
                this.channelEventListener.onChannelActive(
                    event.getRemoteAddr(), event.getChannel());
                break;
            default:
                break;
        }
    }

    @Override
    public String getServiceName() {
        return NettyEventExecutor.class.getSimpleName();
    }
}
```

### 9.3 ChannelEventListener 接口

```java
// ChannelEventListener.java
public interface ChannelEventListener {
    void onChannelConnect(final String remoteAddr, final Channel channel);
    void onChannelClose(final String remoteAddr, final Channel channel);
    void onChannelException(final String remoteAddr, final Channel channel);
    void onChannelIdle(final String remoteAddr, final Channel channel);
    void onChannelActive(final String remoteAddr, final Channel channel);
}
```

### 9.4 事件产生源

事件由 Pipeline 中的 `connectionManageHandler` 产生：

```java
// NettyRemotingClient.NettyConnectManageHandler (示例)
class NettyConnectManageHandler extends ChannelDuplexHandler {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 连接活跃
        String remoteAddress = RemotingHelper.parseChannelRemoteAddress(ctx.channel());
        log.info("NETTY CLIENT CONNECT: {}", remoteAddress);

        // 产生 CONNECT 事件
        if (channelEventListener != null) {
            nettyEventExecutor.putNettyEvent(new NettyEvent(
                NettyEventType.CONNECT, remoteAddress, ctx.channel()));
        }

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 连接不活跃（关闭）
        String remoteAddress = RemotingHelper.parseChannelRemoteAddress(ctx.channel());
        log.info("NETTY CLIENT DISCONNECT: {}", remoteAddress);

        // 产生 CLOSE 事件
        if (channelEventListener != null) {
            nettyEventExecutor.putNettyEvent(new NettyEvent(
                NettyEventType.CLOSE, remoteAddress, ctx.channel()));
        }

        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
            throws Exception {
        if (evt instanceof IdleStateEvent) {
            // 空闲事件
            IdleStateEvent event = (IdleStateEvent) evt;
            switch (event.state()) {
                case READER_IDLE:
                    // 读空闲
                    break;
                case WRITER_IDLE:
                    // 写空闲
                    break;
                case ALL_IDLE:
                    // 读写空闲
                    String remoteAddress = RemotingHelper
                        .parseChannelRemoteAddress(ctx.channel());
                    log.warn("NETTY CLIENT IDLE: {}", remoteAddress);

                    if (channelEventListener != null) {
                        nettyEventExecutor.putNettyEvent(new NettyEvent(
                            NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                    break;
                default:
                    break;
            }
        }

        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        // 异常事件
        String remoteAddress = RemotingHelper.parseChannelRemoteAddress(ctx.channel());
        log.warn("NETTY CLIENT EXCEPTION: {}", remoteAddress, cause);

        if (channelEventListener != null) {
            nettyEventExecutor.putNettyEvent(new NettyEvent(
                NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
        }

        ctx.close();
    }
}
```

### 9.5 事件处理架构

```
Netty Pipeline
       │
       ▼
┌─────────────────────────┐
│ ConnectManageHandler     │
│ (通道事件捕获)            │
└────────────┬─────────────┘
             │ putNettyEvent
             ▼
┌─────────────────────────┐
│ LinkedBlockingQueue      │
│ <NettyEvent>             │
│ (cap=10000)              │
└────────────┬─────────────┘
             │ poll
             ▼
┌─────────────────────────┐
│ NettyEventExecutor       │
│ .run() (单线程)           │
│ processEvent()           │
└────────────┬─────────────┘
             │ dispatch
             ▼
┌─────────────────────────┐
│ ChannelEventListener     │
│ onChannelConnect/Close/  │
│ Idle/Exception/Active    │
└─────────────────────────┘
```

---

## 10. NettyRemotingServer 服务端启动

### 10.1 类结构

```java
// NettyRemotingServer.java
public class NettyRemotingServer extends NettyRemotingAbstract
        implements RemotingServer {
    private static final InternalLogger log =
        InternalLoggerFactory.getLogger(NettyRemotingServer.class);

    // Netty ServerBootstrap
    private final ServerBootstrap serverBootstrap;

    // Boss EventLoopGroup（1 个线程，接受连接）
    private final EventLoopGroup bossEventLoopGroup;

    // Selector EventLoopGroup（处理 IO 读写）
    private final EventLoopGroup selectorEventLoopGroup;

    // 服务端配置
    private final NettyServerConfig nettyServerConfig;

    // 公共线程池（用于回调等）
    private final ExecutorService publicExecutor;

    // 通道事件监听器
    private ChannelEventListener channelEventListener;

    // 默认事件执行器组（用于 Pipeline 中的业务处理）
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    // HashedWheelTimer（超时扫描）
    private final Timer timer = new HashedWheelTimer(
        new ThreadFactoryImpl("NettyRemotingServerTimerThread"));

    // 多端口服务端表（支持监听多个端口）
    private final ConcurrentMap<Integer /* port */, SubRemotingServer>
        remotingServerTable = new ConcurrentHashMap<>();
}
```

### 10.2 start 启动方法

```java
// NettyRemotingServer.java
@Override
public void start() {
    // 1. 创建默认事件执行器组
    this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
        nettyServerConfig.getServerWorkerThreads(),     // 默认 8 个线程
        new ThreadFactoryImpl("NettyServerWorkerThread_"));

    // 2. 初始化 ServerBootstrap
    initServerBootstrap(this.serverBootstrap,
        this.bossEventLoopGroup, this.selectorEventLoopGroup);

    // 3. 绑定端口并启动
    try {
        ChannelFuture channelFuture = this.serverBootstrap
            .bind(this.nettyServerConfig.getListenPort()).sync();
        if (channelFuture.isSuccess()) {
            log.info("RemotingServer started on port: {}",
                this.nettyServerConfig.getListenPort());
        }
    } catch (Exception e) {
        throw new RuntimeException("this.serverBootstrap.bind().sync() error", e);
    }

    // 4. 启动通道事件执行器
    if (this.channelEventListener != null) {
        this.nettyEventExecutor.start();
    }

    // 5. 定时扫描 responseTable（超时清理）
    this.timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run(Timeout t) {
            try {
                NettyRemotingServer.this.scanResponseTable();
            } catch (Throwable e) {
                log.error("scanResponseTable exception", e);
            }
        }
    }, 1000 * 3, 1000);  // 初始延迟 3 秒，每 1 秒执行
}
```

### 10.3 initServerBootstrap 初始化

```java
// NettyRemotingServer.java
private void initServerBootstrap(ServerBootstrap serverBootstrap,
        EventLoopGroup bossGroup, EventLoopGroup selectorGroup) {

    // 设置 Boss 和 Selector 线程组
    serverBootstrap.group(bossGroup, selectorGroup)
        .channel(useEpoll() ? EpollServerSocketChannel.class
                            : NioServerSocketChannel.class)
        // 配置 TCP 参数
        .option(ChannelOption.SO_BACKLOG, 1024)          // 连接队列大小
        .option(ChannelOption.SO_REUSEADDR, true)        // 地址重用
        .option(ChannelOption.TCP_NODELAY, true)         // 禁用 Nagle 算法
        .childOption(ChannelOption.SO_KEEPALIVE, false)  // 不使用 TCP KeepAlive
        .childOption(ChannelOption.TCP_NODELAY, true)    // 子通道也禁用 Nagle
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        // 设置 ChannelInitializer
        .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline()
                    .addLast(defaultEventExecutorGroup,
                        new HandshakeHandler(...))        // 握手处理（HAProxy 检测）
                    .addLast(defaultEventExecutorGroup,
                        new NettyEncoder())               // 编码器
                    .addLast(defaultEventExecutorGroup,
                        new NettyDecoder())               // 解码器
                    .addLast(defaultEventExecutorGroup,
                        new IdleStateHandler(0, 0,
                            nettyServerConfig
                                .getServerChannelMaxIdleTimeSeconds())) // 空闲检测
                    .addLast(defaultEventExecutorGroup,
                        new NettyConnectManageHandler())  // 连接管理
                    .addLast(defaultEventExecutorGroup,
                        new NettyServerHandler());         // 服务端处理器
            }
        });
}

/**
 * 是否使用 Epoll（Linux 下使用 Native 支持）
 */
private boolean useEpoll() {
    return RemotingUtil.isUseEpollNativeSelector() && Epoll.isAvailable();
}
```

### 10.4 Boss 和 Selector 线程组

```java
// NettyRemotingServer.java 构造函数
public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
        final ChannelEventListener channelEventListener) {
    super(nettyServerConfig.getServerOnewaySemaphoreValue(),   // 256
          nettyServerConfig.getServerAsyncSemaphoreValue());   // 64

    this.serverBootstrap = new ServerBootstrap();
    this.nettyServerConfig = nettyServerConfig;
    this.channelEventListener = channelEventListener;

    // 创建公共线程池
    this.publicExecutor = Executors.newFixedThreadPool(
        nettyServerConfig.getServerCallbackExecutorThreads(),
        new ThreadFactoryImpl("NettyServerPublicExecutor_"));

    int serverSelectorThreads = nettyServerConfig.getServerSelectorThreads();  // 默认 3
    if (useEpoll()) {
        // Linux 下使用 EpollEventLoopGroup
        this.bossEventLoopGroup = new EpollEventLoopGroup(1,
            new ThreadFactoryImpl("NettyEPOLLBoss_"));
        this.selectorEventLoopGroup = new EpollEventLoopGroup(
            serverSelectorThreads,
            new ThreadFactoryImpl("NettyServerEPOLLSelector_"));
    } else {
        // 其他平台使用 NioEventLoopGroup
        this.bossEventLoopGroup = new NioEventLoopGroup(1,
            new ThreadFactoryImpl("NettyNIOBoss_"));
        this.selectorEventLoopGroup = new NioEventLoopGroup(
            serverSelectorThreads,
            new ThreadFactoryImpl("NettyServerNIOSelector_"));
    }
}
```

### 10.5 线程模型

```
                    ┌─────────────────────────┐
                    │    BossEventLoopGroup     │
                    │    (1 个线程)              │
                    │    接受新连接              │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  SelectorEventLoopGroup   │
                    │  (3 个线程，默认)          │
                    │  处理 IO 读写             │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  DefaultEventExecutorGroup│
                    │  (8 个线程，默认)          │
                    │  执行 Pipeline Handler    │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  Processor ExecutorService│
                    │  (每个 requestCode 绑定)   │
                    │  执行业务逻辑             │
                    └─────────────────────────┘
```

### 10.6 多端口支持

RocketMQ 支持在同一个 Server 上监听多个端口，通过 `remotingServerTable` 管理：

```java
// NettyRemotingServer.java
public void addRemotingServer(int port) {
    // 创建子 Server
    SubRemotingServer subServer = new SubRemotingServer(port);
    remotingServerTable.put(port, subServer);
    // 启动子 Server
    subServer.start();
}

public void removeRemotingServer(int port) {
    SubRemotingServer subServer = remotingServerTable.remove(port);
    if (subServer != null) {
        subServer.shutdown();
    }
}
```

---

## 11. Pipeline 管道详解

### 11.1 服务端 Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│                      服务端 Pipeline                              │
│                                                                  │
│  ┌──────────────┐                                                │
│  │HandshakeHandler│ ← HAProxy 协议检测，识别真实客户端 IP          │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐                                                │
│  │ NettyEncoder │ ← RemotingCommand → ByteBuf (出站编码)          │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐                                                │
│  │ NettyDecoder │ ← ByteBuf → RemotingCommand (入站解码)          │
│  │              │   LengthFieldBasedFrameDecoder (帧拆包)          │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────┐                                           │
│  │ distributionHandler│ ← 请求分发（多端口路由）                    │
│  └──────┬───────────┘                                           │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────┐                                           │
│  │IdleStateHandler  │ ← 空闲检测 (0,0,120s)                      │
│  │                  │   120秒无读写则触发 IDLE 事件               │
│  └──────┬───────────┘                                           │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────────┐                                       │
│  │connectionManageHandler│ ← 通道事件捕获 → NettyEventExecutor    │
│  │                      │   onConnect/Close/Idle/Exception       │
│  └──────┬───────────────┘                                       │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────┐                                           │
│  │  NettyServerHandler│ ← 最终处理器                               │
│  │                   │   channelRead0 → processMessageReceived   │
│  └───────────────────┘                                           │
└──────────────────────────────────────────────────────────────────┘
```

### 11.2 NettyServerHandler

```java
// NettyRemotingServer.java
@ChannelHandler.Sharable
class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx,
            RemotingCommand msg) throws Exception {
        // 获取本地端口
        int localPort = RemotingHelper.parseSocketAddressPort(
            ctx.channel().localAddress());

        // 从 remotingServerTable 查找对应的 SubRemotingServer
        RemotingAbstract remotingAbstract = NettyRemotingServer.this;

        // 处理消息
        remotingAbstract.processMessageReceived(ctx, msg);
    }
}
```

### 11.3 HandshakeHandler 握手处理

```java
// HandshakeHandler.java
@ChannelHandler.Sharable
class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final HaProtocol haProtocol;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
            throws Exception {
        // 检测是否为 HAProxy 协议（用于获取真实客户端 IP）
        if (haProtocol != null && haProtocol.isHAProxyProtocol(msg)) {
            // 解析 HAProxy 头部，获取真实客户端地址
            HAProxyMessage haMessage = HAProxyMessageDecoder.decode(msg);
            // 设置真实客户端地址
            // ...
            ctx.pipeline().remove(this);  // 移除自身
            ctx.fireChannelActive();
        } else {
            // 非 HAProxy 协议，移除自身并继续传递
            ctx.pipeline().remove(this);
            ctx.fireChannelRead(msg.retain());
        }
    }
}
```

### 11.4 客户端 Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│                      客户端 Pipeline                              │
│                                                                  │
│  ┌──────────────┐                                                │
│  │   SSLHandler  │ ← 可选 SSL/TLS 加密                            │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐                                                │
│  │ NettyEncoder │ ← RemotingCommand → ByteBuf                    │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐                                                │
│  │ NettyDecoder │ ← ByteBuf → RemotingCommand                    │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────┐                                           │
│  │IdleStateHandler  │ ← 空闲检测                                 │
│  └──────┬───────────┘                                           │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────────┐                                       │
│  │NettyConnectManage    │ ← 通道事件捕获                          │
│  │Handler (客户端)       │   onConnect/Close → closeChannel+failFast│
│  └──────┬───────────────┘                                       │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────┐                                           │
│  │NettyClientHandler │ ← 最终处理器                               │
│  │                   │   channelRead0 → processMessageReceived   │
│  └───────────────────┘                                           │
└──────────────────────────────────────────────────────────────────┘
```

### 11.5 客户端 NettyClientHandler

```java
// NettyRemotingClient.java
@ChannelHandler.Sharable
class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx,
            final RemotingCommand msg) throws Exception {
        // 处理接收到的消息（可能是请求或响应）
        processMessageReceived(ctx, msg);
    }
}
```

---

## 12. NettyRemotingClient 客户端启动

### 12.1 类结构

```java
// NettyRemotingClient.java
public class NettyRemotingClient extends NettyRemotingAbstract
        implements RemotingClient {
    private static final InternalLogger log =
        InternalLoggerFactory.getLogger(NettyRemotingClient.class);

    // Netty Bootstrap
    private final Bootstrap bootstrap = new Bootstrap();

    // 事件循环组（单线程）
    private final EventLoopGroup eventLoopGroupWorker;

    // 客户端配置
    private final NettyClientConfig nettyClientConfig;

    // 通道表：key = "host:port", value = ChannelWrapper
    private final ConcurrentMap<String /* addr */, ChannelWrapper>
        channelTables = new ConcurrentHashMap<>();

    // NameServer 地址列表
    private final List<String> namesrvAddrList = new CopyOnWriteArrayList<>();

    // 当前选择的 NameServer 地址
    private volatile String namesrvAddrChoosed;

    // NameServer 轮询索引
    private volatile int namesrvIndex = 0;

    // 通道事件监听器
    private ChannelEventListener channelEventListener;

    // 定时器
    private final Timer timer = new HashedWheelTimer(
        new ThreadFactoryImpl("NettyRemotingClientTimerThread"));

    // 是否启用 SSL
    private boolean useTLS;
}
```

### 12.2 start 启动方法

```java
// NettyRemotingClient.java
@Override
public void start() {
    // 1. 配置 Bootstrap
    this.bootstrap.group(this.eventLoopGroupWorker)  // 单线程 NIO
        .channel(useEpoll() ? EpollSocketChannel.class : NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, false)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
            nettyClientConfig.getConnectTimeoutMillis())  // 默认 3000ms
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                // SSL 处理（可选）
                if (nettyClientConfig.isUseTLS()) {
                    SSLEngine sslEngine = sslContext.createSSLEngine();
                    sslEngine.setUseClientMode(true);
                    pipeline.addLast(defaultEventExecutorGroup,
                        new SslHandler(sslEngine));
                }

                // 编码器
                pipeline.addLast(defaultEventExecutorGroup,
                    new NettyEncoder());
                // 解码器
                pipeline.addLast(defaultEventExecutorGroup,
                    new NettyDecoder());
                // 空闲检测
                pipeline.addLast(defaultEventExecutorGroup,
                    new IdleStateHandler(
                        0, 0,
                        nettyClientConfig.getClientChannelMaxIdleTimeSeconds()));
                // 连接管理
                pipeline.addLast(defaultEventExecutorGroup,
                    new NettyConnectManageHandler());
                // 客户端处理器
                pipeline.addLast(defaultEventExecutorGroup,
                    new NettyClientHandler());
            }
        });

    // 2. 启动通道事件执行器
    if (this.channelEventListener != null) {
        this.nettyEventExecutor.start();
    }

    // 3. 定时扫描 responseTable（超时清理）
    this.timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run(Timeout t) {
            try {
                NettyRemotingClient.this.scanResponseTable();
            } catch (Throwable e) {
                log.error("scanResponseTable exception", e);
            }
        }
    }, 1000 * 3, 1000);  // 每 1 秒执行

    // 4. 定时扫描可用的 NameServer
    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
            try {
                NettyRemotingClient.this.scanAvailableNameSrv();
            } catch (Exception e) {
                log.error("scanAvailableNameSrv exception", e);
            }
        }
    }, 1000 * 10, 1000 * 5, TimeUnit.MILLISECONDS);  // 每 5 秒执行
}
```

### 12.3 事件循环组

```java
// NettyRemotingClient.java 构造函数
public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
    super(nettyClientConfig.getClientOnewaySemaphoreValue(),
          nettyClientConfig.getClientAsyncSemaphoreValue());

    this.nettyClientConfig = nettyClientConfig;
    this.channelEventListener = channelEventListener;

    // 单线程 NIO 事件循环组
    int workerThreadNum = nettyClientConfig.getClientWorkerThreads();  // 默认 1
    if (workerThreadNum <= 0) {
        workerThreadNum = 1;
    }

    if (useEpoll()) {
        this.eventLoopGroupWorker = new EpollEventLoopGroup(workerThreadNum,
            new ThreadFactoryImpl("NettyClientEPOLLWorker_"));
    } else {
        this.eventLoopGroupWorker = new NioEventLoopGroup(workerThreadNum,
            new ThreadFactoryImpl("NettyClientNIOWorker_"));
    }

    // 默认事件执行器组
    this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
        nettyClientConfig.getClientCallbackExecutorThreads(),
        new ThreadFactoryImpl("NettyClientWorker_"));
}
```

---

## 13. ChannelWrapper 连接管理

### 13.1 ChannelWrapper 类

`ChannelWrapper` 是对 Netty `ChannelFuture` 的包装，提供连接管理和重连功能：

```java
// NettyRemotingClient.java
class ChannelWrapper {
    private ChannelFuture channelFuture;
    // 最后响应时间（用于 GO_AWAY 判断）
    private long lastResponseTime;
    // 是否需要重连
    private volatile boolean reconnectable = true;

    public ChannelWrapper(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
        this.lastResponseTime = System.currentTimeMillis();
    }

    public boolean isOK() {
        return this.channelFuture.channel() != null
            && this.channelFuture.channel().isActive();
    }

    public boolean isWritable() {
        return this.channelFuture.channel().isWritable();
    }

    public Channel getChannel() {
        return this.channelFuture.channel();
    }

    public ChannelFuture getChannelFuture() {
        return channelFuture;
    }

    public long getLastResponseTime() {
        return lastResponseTime;
    }

    public void setLastResponseTime(long lastResponseTime) {
        this.lastResponseTime = lastResponseTime;
    }

    public boolean isReconnectable() {
        return reconnectable;
    }

    public void setReconnectable(boolean reconnectable) {
        this.reconnectable = reconnectable;
    }

    /**
     * 重连
     */
    public void reconnect() {
        // 关闭旧连接
        Channel oldChannel = this.channelFuture.channel();
        if (oldChannel != null) {
            oldChannel.close();
        }

        // 创建新连接
        // ... bootstrap.connect()
    }
}
```

### 13.2 getAndCreateChannelAsync 异步获取通道

```java
// NettyRemotingClient.java
public Channel getAndCreateChannelAsync(final String addr)
        throws InterruptedException, RemotingConnectException {
    if (null == addr) {
        return getAndCreateNameserverChannel();
    }

    // 从缓存中获取
    ChannelWrapper cw = this.channelTables.get(addr);
    if (cw != null && cw.isOK()) {
        return cw.getChannel();
    }

    // 异步创建新连接
    return this.createChannel(addr);
}

private Channel createChannel(final String addr) throws InterruptedException {
    ChannelWrapper cw = this.channelTables.get(addr);
    if (cw != null && cw.isOK()) {
        return cw.getChannel();
    }

    // 加锁创建
    synchronized (this) {
        cw = this.channelTables.get(addr);
        if (cw != null) {
            if (cw.isOK()) {
                return cw.getChannel();
            }
        } else {
            cw = new ChannelWrapper(this.bootstrap.connect(
                RemotingHelper.string2SocketAddress(addr)));
            this.channelTables.put(addr, cw);
        }
    }

    // 等待连接完成
    ChannelFuture future = cw.getChannelFuture();
    if (future.awaitUninterruptibly(
            this.nettyClientConfig.getConnectTimeoutMillis())) {
        if (cw.isOK()) {
            log.info("connect to {} success", addr);
            return cw.getChannel();
        } else {
            log.warn("connect to {} failed", addr);
        }
    } else {
        log.warn("connect to {} timeout", addr);
    }

    return null;
}
```

### 13.3 closeChannel 关闭通道

```java
// NettyRemotingClient.java
public void closeChannel(final String addr, final Channel channel) {
    if (null == channel) {
        return;
    }

    final String addrFinal = addr;
    // 异步关闭，避免在 IO 线程中执行
    this.eventLoopGroupWorker.execute(new Runnable() {
        @Override
        public void run() {
            try {
                ChannelWrapper cw = channelTables.get(addrFinal);
                if (cw != null && cw.getChannel() == channel) {
                    channelTables.remove(addrFinal);
                }
            } catch (Exception e) {
                log.warn("closeChannel exception, addr={}", addrFinal, e);
            }

            // 关闭通道
            RemotingHelper.closeChannel(channel);
        }
    });
}
```

---

## 14. NameServer 地址轮询

### 14.1 NameServer 地址管理

```java
// NettyRemotingClient.java
// NameServer 地址列表
private final List<String> namesrvAddrList = new CopyOnWriteArrayList<>();
// 当前选择的 NameServer 地址
private volatile String namesrvAddrChoosed;
// 轮询索引
private volatile int namesrvIndex = 0;
```

### 14.2 getAndCreateNameserverChannelAsync

```java
// NettyRemotingClient.java
private Channel getAndCreateNameserverChannel() throws InterruptedException,
        RemotingConnectException {
    String addr = this.namesrvAddrChoosed;
    if (addr != null) {
        // 使用上次选择的 NameServer 地址
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
    }

    // 需要重新选择 NameServer
    final List<String> addrList = this.namesrvAddrList;
    if (this.namesrvAddrChoosed != null && !addrList.isEmpty()) {
        // 轮询选择下一个 NameServer
        int index = this.namesrvIndex++ % addrList.size();
        if (index < 0) {
            index = -index;
        }
        addr = addrList.get(index);
        this.namesrvAddrChoosed = addr;
    }

    if (addr != null) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        // 创建新连接
        Channel channel = createChannel(addr);
        if (channel != null) {
            return channel;
        }
    }

    return null;
}
```

### 14.3 轮询策略

```
假设 namesrvAddrList = ["10.0.0.1:9876", "10.0.0.2:9876", "10.0.0.3:9876"]

第1次请求: index = 0 → 选择 "10.0.0.1:9876"
第2次请求: index = 1 → 选择 "10.0.0.2:9876"
第3次请求: index = 2 → 选择 "10.0.0.3:9876"
第4次请求: index = 0 → 选择 "10.0.0.1:9876"
...

如果当前选择的 NameServer 连接失败，下次会轮询选择下一个。
```

### 14.4 updateNameServerAddressList 更新地址列表

```java
// NettyRemotingClient.java
@Override
public void updateNameServerAddressList(List<String> addrs) {
    if (addrs != null && !addrs.isEmpty()) {
        // 更新地址列表
        this.namesrvAddrList.clear();
        this.namesrvAddrList.addAll(addrs);
        log.info("name server address updated: {}", addrs);
    }
}
```

### 14.5 scanAvailableNameSrv 扫描可用NameServer

```java
// NettyRemotingClient.java
public void scanAvailableNameSrv() {
    if (this.namesrvAddrList.isEmpty()) {
        return;
    }

    for (String addr : this.namesrvAddrList) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && !cw.isOK()) {
            // 通道不可用，移除
            this.channelTables.remove(addr);
            log.info("scanAvailableNameSrv: remove unavailable name server, addr={}",
                addr);
        }
    }
}
```

---

## 15. GO_AWAY 优雅重连

### 15.1 GO_AWAY 场景

当服务端正在进行优雅停机（shutdown）时，会向客户端返回 `GO_AWAY` 响应码（1500）。客户端收到后进行重连操作。

### 15.2 服务端 GO_AWAY 逻辑

```java
// NettyRemotingAbstract.java (processRequestCommand 中)
if (this.isShuttingDown.get()) {
    // 服务端正在关闭
    if (cmd.getVersion() > MQVersion.Version.V4_7_0.ordinal()) {
        // 客户端版本较新，返回 GO_AWAY
        RemotingCommand response = RemotingCommand
            .createResponseCommand(ResponseCode.GO_AWAY, "");
        response.setOpaque(opaque);
        ctx.writeAndFlush(response);
        return;
    }
}
```

### 15.3 客户端 GO_AWAY 处理

```java
// NettyRemotingClient.java (invokeImpl override)
@Override
public RemotingCommand invokeSync(String addr, final RemotingCommand request,
        long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingSendException, RemotingTimeoutException {

    final Channel channel = this.getAndCreateChannel(addr);
    if (channel != null && channel.isActive()) {
        try {
            RemotingCommand response = this.invokeSyncImpl(channel, request,
                timeoutMillis);

            // 检查 GO_AWAY 响应
            if (response != null &&
                response.getCode() == ResponseCode.GO_AWAY &&
                this.nettyClientConfig.isEnableReconnectForGoAway()) {

                // GO_AWAY: 重连
                ChannelWrapper cw = this.channelTables.get(addr);
                if (cw != null) {
                    // 标记需要重连
                    cw.setReconnectable(true);

                    // 关闭旧通道
                    this.closeChannel(addr, channel);

                    // 等待重连间隔
                    Thread.sleep(this.nettyClientConfig
                        .getReconnectIntervalMillis());

                    // 重连并重试一次
                    Channel newChannel = this.getAndCreateChannel(addr);
                    if (newChannel != null && newChannel.isActive()) {
                        // 重试请求
                        response = this.invokeSyncImpl(newChannel, request,
                            timeoutMillis);
                    }
                }
            }

            return response;
        } catch (RemotingSendException e) {
            this.closeChannel(addr, channel);
            throw e;
        }
    } else {
        this.closeChannel(addr, channel);
        throw new RemotingConnectException(addr);
    }
}
```

### 15.4 GO_AWAY 重连流程

```
Client                              Server (正在关闭)
  │                                    │
  │ Request                            │
  │───────────────────────────────────▶│
  │                                    │ isShuttingDown = true
  │                                    │ → GO_AWAY (code=1500)
  │                                    │
  │ Response (GO_AWAY)                 │
  │◀───────────────────────────────────│
  │                                    │
  │ 检测到 GO_AWAY                     │
  │ → closeChannel(addr, channel)      │
  │ → 等待 reconnectInterval           │
  │                                    │
  │ 重新连接到同一地址                   │
  │ → getAndCreateChannel(addr)        │
  │ → 新的 Channel                      │
  │                                    │
  │ Request (重试)                      │
  │───────────────────────────────────▶│ (如果服务端恢复了)
  │                                    │
  │ Response                           │
  │◀───────────────────────────────────│
```

### 15.5 GO_AWAY 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `enableReconnectForGoAway` | true | 是否启用 GO_AWAY 重连 |
| `reconnectIntervalMillis` | 3000 | 重连间隔 |
| `maxReconnectIntervalTimeSeconds` | 60 | 最大重连间隔 |

---

## 16. ServiceThread 基础线程类

### 16.1 ServiceThread 类定义

`ServiceThread` 是 RocketMQ 中所有后台线程的基类：

```java
// ServiceThread.java
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log =
        InternalLoggerFactory.getLogger(ServiceThread.class);

    // 线程
    protected volatile Thread thread;
    // 线程工厂
    protected final CountDownLatch2 waitPoint = new CountDownLatch2();
    // 线程名
    private static final long JOIN_TIME = 90 * 1000;
    // 停止标志
    protected volatile boolean stopped = false;
    // 是否已通知
    protected volatile boolean hasNotified = false;

    // 监听器
    protected final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {
    }

    public abstract String getServiceName();

    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}",
            getServiceName(), this.started.get(), thread);

        if (!started.compareAndSet(false, true)) {
            return;
        }

        this.stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(true);
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        this.stopped = true;
        log.info("shutdown thread {} interrupt {}", getServiceName(), interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            // 唤醒可能在等待的线程
            waitPoint.countDown();
        }

        if (interrupt) {
            // 中断线程
            if (thread != null) {
                thread.interrupt();
            }
        }

        try {
            // 等待线程结束
            if (thread != null) {
                thread.join(JOINT_TIME);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    /**
     * 等待运行（用于定时触发场景）
     */
    public void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            // 已被通知，直接返回
            return;
        }

        // 等待指定时间或被唤醒
        try {
            waitPoint.reset();
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
        }
    }

    /**
     * 唤醒等待的线程
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown();
        }
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isStarted() {
        return started.get();
    }
}
```

### 16.2 ServiceThread 使用模式

```java
// 典型使用方式
public class MyBackgroundService extends ServiceThread {
    @Override
    public void run() {
        log.info("{} service started", getServiceName());

        while (!this.isStopped()) {
            // 等待 5 秒或被唤醒
            this.waitForRunning(5000);

            // 执行业务逻辑
            doWork();
        }

        log.info("{} service end", getServiceName());
    }

    @Override
    public String getServiceName() {
        return "MyBackgroundService";
    }

    private void doWork() {
        // ...
    }
}

// 启动
MyBackgroundService service = new MyBackgroundService();
service.start();

// 停止
service.shutdown();
```

### 16.3 ServiceThread 在 RocketMQ 中的应用

| 使用类 | 用途 |
|--------|------|
| `RebalanceService` | 消费者定时 Rebalance |
| `PullMessageService` | 消费者拉取消息 |
| `NettyEventExecutor` | 通道事件分发 |
| `PullRequestHoldService` | 长轮询挂起检查 |
| `ReputMessageService` | 消息分发（构建 ConsumeQueue/Index） |
| `FlushConsumeQueueService` | 刷盘 ConsumeQueue |
| `HAService` | 主从同步 |

### 16.4 waitForRunning 和 wakeup 的协作

```
                     ServiceThread.run()
                     │
                     ▼
              ┌──────────────┐
              │ while (!stopped)│
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │ waitForRunning│─── 被唤醒 ──▶ 执行 doWork()
              │ (interval)    │              │
              └──────┬───────┘              │
                     │                       │
                     │ 超时                  │
                     ▼                       │
              ┌──────────────┐              │
              │ 执行 doWork() │◀─────────────┘
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │ 继续循环      │
              └──────────────┘

wakeup() 调用时机:
  - 外部需要立即触发执行时
  - 例如: 新消息到达时唤醒 PullRequestHoldService
  - 例如: 配置变更时唤醒 RebalanceService
```

---

## 17. RequestCode/ResponseCode 全表

### 17.1 RequestCode 请求码

#### 消息相关

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `SEND_MESSAGE` | 发送消息 | 10 | 普通消息发送 |
| `PULL_MESSAGE` | 拉取消息 | 11 | 消费者拉取消息 |
| `QUERY_MESSAGE` | 查询消息 | 12 | 按条件查询消息 |
| `QUERY_BROKER_OFFSET` | 查询 Broker Offset | 13 | 查询 Broker 最大 offset |
| `QUERY_CONSUMER_OFFSET` | 查询消费 Offset | 14 | 查询消费者消费进度 |
| `UPDATE_CONSUMER_OFFSET` | 更新消费 Offset | 15 | 更新消费者消费进度 |
| `UPDATE_BROKER_OFFSET` | 更新 Broker Offset | 16 | 更新 Broker offset |
| `SEND_MESSAGE_V2` | 发送消息 V2 | 310 | 优化版消息发送 |
| `SEND_BATCH_MESSAGE` | 批量发送消息 | 320 | 批量消息发送 |
| `VIEW_BROKER_STATS_DATA` | 查看 Broker 统计 | 321 | 查看统计数据 |

#### 消费者相关

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `GET_CONSUMER_LIST_BY_GROUP` | 获取消费者列表 | 38 | 查询消费者组下的消费者列表 |
| `GET_CONSUMER_STATUS_FROM_CLIENT` | 获取消费状态 | 39 | 查询消费者消费状态 |
| `GET_CONSUMER_RUNNING_INFO` | 获取消费者运行信息 | 307 | 获取消费者运行时信息 |
| `CONSUME_MESSAGE_DIRECTLY` | 直接消费消息 | 309 | 直接消费消息（用于排查） |
| `NOTIFY_CONSUMER_IDS_CHANGED` | 通知消费者变化 | 40 | 通知消费者列表变化 |

#### POP 消费相关

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `POP_MESSAGE` | Pop 消息 | 200050 | 轻量级拉取消息 |
| `ACK_MESSAGE` | ACK 消息 | 200051 | 确认消费 |
| `BATCH_ACK_MESSAGE` | 批量 ACK | 200151 | 批量确认消费 |
| `PEEK_MESSAGE` | Peek 消息 | 200052 | 查看消息不消费 |
| `CHANGE_MESSAGE_INVISIBLETIME` | 修改消息不可见时间 | 200053 | 修改 Pop 消息不可见时间 |

#### Broker 注册与路由

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `REGISTER_BROKER` | 注册 Broker | 103 | Broker 向 NameServer 注册 |
| `UNREGISTER_BROKER` | 注销 Broker | 104 | Broker 从 NameServer 注销 |
| `GET_ROUTEINFO_BY_TOPIC` | 按主题查路由 | 105 | 查询 Topic 路由信息 |
| `GET_BROKER_CLUSTER_INFO` | 查询集群信息 | 106 | 查询 Broker 集群信息 |
| `GET_HAS_UNIT_SUB_UNUNIT_TOPIC` | 查询单元化 Topic | 307 | 查询单元化部署的 Topic |
| `GET_HAS_UNIT_SUB_UNIT_TOPIC` | 查询单元子 Topic | 308 | 查询单元化子 Topic |

#### 心跳

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `HEART_BEAT` | 心跳 | 34 | 客户端向 Broker 发送心跳 |
| `UNREGISTER_CLIENT` | 注销客户端 | 35 | 客户端注销 |
| `CHECK_CLIENT_CONFIG` | 检查客户端配置 | 41 | 检查客户端配置一致性 |

#### 事务

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `END_TRANSACTION` | 结束事务 | 37 | 提交或回滚事务 |
| `CHECK_TRANSACTION_STATE` | 检查事务状态 | 39 | Broker 检查事务状态 |

#### HA 高可用

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `GET_BROKER_MEMBER_GROUP` | 获取 Broker 组成员 | 901 | 查询 Broker 组信息 |
| `BROKER_HEARTBEAT` | Broker 心跳 | 904 | Broker 向 Controller 发送心跳 |
| `NOTIFY_MIN_BROKER_ID_CHANGE` | 通知最小 Broker ID 变化 | 905 | 通知主 Broker 变化 |

#### Controller

| 请求码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `CONTROLLER_ELECT_MASTER` | Controller 选举 Master | 1002 | Controller 选举新的 Master |
| `CONTROLLER_REGISTER_BROKER` | Controller 注册 Broker | 1003 | Broker 向 Controller 注册 |
| `CONTROLLER_GET_REPLICA_INFO` | 获取副本信息 | 1004 | 查询副本信息 |
| `CONTROLLER_GET_METADATA_INFO` | 获取元数据 | 1005 | 查询 Controller 元数据 |
| `CONTROLLER_GET_SYNC_STATE_DATA` | 获取同步状态数据 | 1006 | 查询同步状态数据 |
| `CONTROLLER_ALTER_SYNC_STATE_SET` | 修改同步状态集 | 1007 | 修改 SyncStateSet |
| `CONTROLLER_BROKER_CLOSE` | Broker 关闭 | 1008 | 通知 Controller Broker 关闭 |

### 17.2 ResponseCode 响应码

| 响应码 | 名称 | 值 | 说明 |
|--------|------|-----|------|
| `SUCCESS` | 成功 | 0 | 请求处理成功 |
| `SYSTEM_ERROR` | 系统错误 | 1 | 系统内部错误 |
| `SYSTEM_BUSY` | 系统繁忙 | 2 | 系统繁忙，触发流控 |
| `REQUEST_CODE_NOT_SUPPORTED` | 请求码不支持 | 3 | 不支持的请求码 |
| `TRANSACTION_FAILED` | 事务失败 | 4 | 事务处理失败 |
| `FLUSH_DISK_TIMEOUT` | 刷盘超时 | 10 | 刷盘超时 |
| `SLAVE_NOT_AVAILABLE` | Slave 不可用 | 11 | Slave 不可用 |
| `FLUSH_SLAVE_TIMEOUT` | 同步 Slave 超时 | 12 | 同步 Slave 超时 |
| `ACCESS_CHANNEL_NOT_ALLOWED` | 访问通道不允许 | 13 | 不允许的访问 |
| `SUBSCRIPTION_GROUP_NOT_EXIST` | 订阅组不存在 | 200 | 消费者组不存在 |
| `SUBSCRIPTION_NOT_EXIST` | 订阅不存在 | 201 | 订阅数据不存在 |
| `SUBSCRIPTION_NOT_LATEST` | 订阅不是最新 | 202 | 订阅数据不是最新 |
| `SUBSCRIPTION_GROUP_NOT_ONLINE` | 订阅组不在线 | 203 | 消费者组不在线 |
| `NO_PERMISSION` | 无权限 | 206 | 权限不足 |
| `PULL_NOT_FOUND` | 拉取未找到 | 19 | 没有新消息 |
| `PULL_RETRY_IMMEDIATELY` | 立即重试拉取 | 20 | 没有匹配消息，立即重试 |
| `PULL_OFFSET_MOVED` | 拉取偏移量移动 | 21 | 偏移量不合法，需要调整 |
| `QUERY_NOT_FOUND` | 查询未找到 | 22 | 查询结果不存在 |
| `GO_AWAY` | GO_AWAY | 1500 | 服务端要求客户端重连 |
| `CONTROLLER_NOT_LEADER` | Controller 非主 | 2007 | Controller 不是 Leader |
| `CONTROLLER_FENCED_MASTER_EPOCH` | Master Epoch 被隔离 | 2000 | Master Epoch 无效 |
| `CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH` | SyncStateSet Epoch 被隔离 | 2001 | SyncStateSet Epoch 无效 |
| `CONTROLLER_INVALID_MASTER_EPOCH` | 无效 Master Epoch | 2002 | Master Epoch 无效 |
| `CONTROLLER_INVALID_SYNC_STATE_SET_EPOCH` | 无效 SyncStateSet Epoch | 2003 | SyncStateSet Epoch 无效 |
| `CONTROLLER_BROKER_NOT_ALIVE` | Broker 不活跃 | 2004 | Broker 未活跃 |
| `CONTROLLER_BROKER_METADATA_NOT_EXIST` | Broker 元数据不存在 | 2005 | Broker 元数据不存在 |
| `CONTROLLER_NOT_MASTER_BROKER` | 非 Master Broker | 2006 | 非 Master 角色 |

### 17.3 请求码与处理器注册

Broker 端在启动时注册处理器：

```java
// BrokerController.java (注册处理器示例)
// 发送消息处理器
remotingServer.registerProcessor(RequestCode.SEND_MESSAGE,
    sendMessageProcessor, sendMessageExecutor);
remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2,
    sendMessageProcessor, sendMessageExecutor);
remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE,
    sendMessageProcessor, sendMessageExecutor);

// 拉取消息处理器
remotingServer.registerProcessor(RequestCode.PULL_MESSAGE,
    pullMessageProcessor, pullMessageExecutor);

// 消费者管理处理器
remotingServer.registerProcessor(RequestCode.HEART_BEAT,
    consumerManageProcessor, heartbeatExecutor);
remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT,
    consumerManageProcessor, heartbeatExecutor);
remotingServer.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT,
    consumerManageProcessor, heartbeatExecutor);

// 偏移量处理器
remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET,
    consumerManageProcessor, consumerManageExecutor);
remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET,
    consumerManageProcessor, consumerManageExecutor);

// 事务处理器
remotingServer.registerProcessor(RequestCode.END_TRANSACTION,
    endTransactionProcessor, endTransactionExecutor);
remotingServer.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE,
    endTransactionProcessor, endTransactionExecutor);

// POP 消息处理器
remotingServer.registerProcessor(RequestCode.POP_MESSAGE,
    popMessageProcessor, pullMessageExecutor);
remotingServer.registerProcessor(RequestCode.ACK_MESSAGE,
    ackMessageProcessor, ackMessageExecutor);
remotingServer.registerProcessor(RequestCode.BATCH_ACK_MESSAGE,
    ackMessageProcessor, ackMessageExecutor);
```

### 17.4 registerProcessor 注册方法

```java
// NettyRemotingServer.java
@Override
public void registerProcessor(int requestCode, NettyRequestProcessor processor,
        ExecutorService executor) {
    // 注册到 processorTable
    this.processorTable.put(requestCode,
        new Pair<>(processor, executor));
}

@Override
public void registerDefaultProcessor(NettyRequestProcessor processor,
        ExecutorService executor) {
    // 注册默认处理器
    this.defaultRequestProcessorPair = new Pair<>(processor, executor);
}
```

---

## 18. 配置参数详解

### 18.1 NettyServerConfig 服务端配置

```java
// NettyServerConfig.java
public class NettyServerConfig {
    // 绑定地址
    private String bindAddress = "0.0.0.0";
    // 监听端口
    private int listenPort = 0;
    // 工作线程数（用于 Pipeline Handler 执行）
    private int serverWorkerThreads = 8;
    // Selector 线程数（NIO 事件循环）
    private int serverSelectorThreads = 3;
    // Oneway 信号量大小
    private int serverOnewaySemaphoreValue = 256;
    // Async/Sync 信号量大小
    private int serverAsyncSemaphoreValue = 64;
    // 通道最大空闲时间（秒）
    private int serverChannelMaxIdleTimeSeconds = 120;
    // 通道最大连接数
    private int serverChannelMaxConnections = 65536;
    // 回调线程数
    private int serverCallbackExecutorThreads = NettyRemotingServer
        .class.getName().length();
    // 帧最大长度
    private int serverNettyFrameMaxLength = 16777216;  // 16MB
    // 是否启用 Epoll
    private boolean serverUseEpollNativeSelector = true;
    // 是否启用 HAProxy 协议
    private boolean serverHaProxyProtocolEnable = false;
    // 是否启用 SSL
    private boolean serverUseTLS = false;
}
```

### 18.2 NettyServerConfig 参数详解

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `bindAddress` | 0.0.0.0 | 绑定地址，0.0.0.0 表示所有网卡 |
| `listenPort` | 0 | 监听端口，0 表示自动分配 |
| `serverWorkerThreads` | 8 | Pipeline Handler 执行线程数 |
| `serverSelectorThreads` | 3 | NIO Selector 线程数 |
| `serverOnewaySemaphoreValue` | 256 | 并发 Oneway 请求限制 |
| `serverAsyncSemaphoreValue` | 64 | 并发 Async 请求限制 |
| `serverChannelMaxIdleTimeSeconds` | 120 | 通道空闲超时（秒） |
| `serverNettyFrameMaxLength` | 16MB | 单帧最大长度 |
| `serverUseEpollNativeSelector` | true | Linux 下是否使用 Epoll |

### 18.3 NettyClientConfig 客户端配置

```java
// NettyClientConfig.java
public class NettyClientConfig {
    // 回调线程数（默认 CPU 核数）
    private int clientCallbackExecutorThreads =
        Runtime.getRuntime().availableProcessors();
    // 工作线程数（NIO 事件循环）
    private int clientWorkerThreads = 1;
    // Oneway 信号量大小
    private int clientOnewaySemaphoreValue = 65535;
    // Async/Sync 信号量大小
    private int clientAsyncSemaphoreValue = 65535;
    // 通道最大空闲时间（秒）
    private int clientChannelMaxIdleTimeSeconds = 60;
    // 连接超时（毫秒）
    private int connectTimeoutMillis = 3000;
    // 通道不活跃检查间隔（毫秒）
    private long channelNotActiveInterval = 60000;
    // 最大重连间隔（秒）
    private int maxReconnectIntervalTimeSeconds = 60;
    // 是否启用 GO_AWAY 重连
    private boolean enableReconnectForGoAway = true;
    // 是否启用 Epoll
    private boolean clientUseEpollNativeSelector = true;
    // 是否启用 SSL
    private boolean useTLS = false;
    // 帧最大长度
    private int clientNettyFrameMaxLength = 16777216;  // 16MB
    // 空闲超时关闭
    private boolean clientCloseSocketIfTimeout = true;
    // 重连间隔（毫秒）
    private long reconnectIntervalMillis = 3000;
}
```

### 18.4 NettyClientConfig 参数详解

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `clientCallbackExecutorThreads` | CPU 核数 | 回调线程数 |
| `clientWorkerThreads` | 1 | NIO 工作线程数 |
| `clientOnewaySemaphoreValue` | 65535 | 并发 Oneway 请求限制 |
| `clientAsyncSemaphoreValue` | 65535 | 并发 Async 请求限制 |
| `clientChannelMaxIdleTimeSeconds` | 60 | 通道空闲超时 |
| `connectTimeoutMillis` | 3000 | 连接超时 |
| `channelNotActiveInterval` | 60000 | 通道不活跃检查间隔 |
| `maxReconnectIntervalTimeSeconds` | 60 | 最大重连间隔 |
| `enableReconnectForGoAway` | true | 是否启用 GO_AWAY 重连 |
| `reconnectIntervalMillis` | 3000 | 重连间隔 |

### 18.5 客户端 vs 服务端配置对比

| 参数 | 客户端默认值 | 服务端默认值 | 说明 |
|------|------------|------------|------|
| Worker 线程数 | 1 | 8 | 客户端单线程足够，服务端需多线程 |
| Selector 线程数 | 1 | 3 | 客户端连接少，服务端连接多 |
| Oneway 信号量 | 65535 | 256 | 客户端限制宽，服务端限制严 |
| Async 信号量 | 65535 | 64 | 同上 |
| 空闲超时 | 60s | 120s | 客户端更主动检测 |
| Epoll | true | true | Linux 下均使用 Epoll |

---

## 19. 知识点总结

### 19.1 核心设计思想

1. **自定义二进制协议**：RocketMQ 设计了精简的 `RemotingCommand` 协议，`[totalLen][headerLen|type][header][body]` 格式，支持 JSON 和 ROCKETMQ 两种序列化方式，兼顾可读性和性能。

2. **opaque 请求-响应匹配**：使用全局递增的 `opaque` 作为请求 ID，请求和响应共享同一个 `opaque`，通过 `responseTable` 实现异步请求-响应匹配。

3. **三种调用模式**：
   - `SYNC`：阻塞等待响应，适合需要立即获取结果的场景
   - `ASYNC`：回调通知，适合高吞吐量场景
   - `ONEWAY`：不等待响应，适合日志、监控等可丢失场景

4. **信号量限流**：使用 `semaphoreAsync` 和 `semaphoreOneway` 分别限制并发异步/同步请求和 oneway 请求，保护系统不被压垮。

5. **HashedWheelTimer 超时清理**：使用时间轮算法高效扫描 `responseTable`，清理超时的 `ResponseFuture`，避免内存泄漏。

6. **failFast 快速失败**：通道关闭时快速失败所有关联请求，避免请求无限等待。

7. **GO_AWAY 优雅重连**：服务端优雅停机时返回 `GO_AWAY`，客户端自动重连，实现无缝迁移。

8. **ChannelWrapper 连接管理**：封装 `ChannelFuture`，支持重连和连接状态管理。

9. **NettyEventExecutor 事件机制**：通过队列异步处理通道事件，避免阻塞 IO 线程。

10. **ServiceThread 基础线程类**：提供统一的线程生命周期管理（start/shutdown/waitForRunning/wakeup），所有后台线程继承此类。

### 19.2 通信全链路总结

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          RocketMQ Remoting 通信全链路                        │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 1. 协议层: RemotingCommand                                          │   │
│  │    [totalLen(4)] [headerLen|type(4)] [headerData] [bodyData]        │   │
│  │    JSON / ROCKETMQ 序列化                                           │   │
│  │    customHeader ←反射→ extFields                                    │   │
│  │    FastCodesHeader → 跳过反射                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 2. 编解码层: NettyEncoder / NettyDecoder                            │   │
│  │    LengthFieldBasedFrameDecoder (4字节长度字段, 16MB最大帧)           │   │
│  │    Encoder: RemotingCommand → ByteBuf                               │   │
│  │    Decoder: ByteBuf → RemotingCommand                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 3. 调用层: NettyRemotingAbstract                                    │   │
│  │    invokeSync:  信号量 → ResponseFuture → writeAndFlush → wait      │   │
│  │    invokeAsync: 信号量 → ResponseFuture → writeAndFlush → callback   │   │
│  │    invokeOneway: 信号量 → markOneway → writeAndFlush → done          │   │
│  │    processRequestCommand: processorTable → submit to executor       │   │
│  │    processResponseCommand: responseTable.get(opaque) → match        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 4. 连接层: NettyRemotingServer / NettyRemotingClient                │   │
│  │    Server: ServerBootstrap (boss=1, selector=3, worker=8)           │   │
│  │    Client: Bootstrap (worker=1), ChannelWrapper, NameServer 轮询     │   │
│  │    Pipeline: Encoder → Decoder → IdleState → ConnectMgr → Handler   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 5. 管理层: 超时/失败/重连/事件                                       │   │
│  │    scanResponseTable: HashedWheelTimer 每1s扫描超时                  │   │
│  │    failFast: 通道关闭快速失败所有请求                                │   │
│  │    GO_AWAY: 服务端停机 → 客户端重连重试                              │   │
│  │    NettyEventExecutor: 通道事件异步分发                              │   │
│  │    ServiceThread: 后台线程统一管理                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 19.3 常见问题分析

#### Q1: 为什么 RocketMQ 不直接使用 gRPC 或其他现成 RPC 框架？

A: RocketMQ 的 Remoting 框架针对消息场景做了大量优化：
1. 支持长轮询（Broker 端挂起 Pull 请求），gRPC 难以实现
2. 自定义协议更轻量，减少不必要的 HTTP/2 开销
3. 支持灵活的请求路由（processorTable），按 requestCode 分配处理器
4. 内置信号量限流、超时清理等机制，更适合高吞吐场景

#### Q2: opaque 会溢出吗？

A: `opaque` 是 `int` 类型（通过 `AtomicInteger` 自增），最大值 2^31-1 ≈ 21 亿。每秒即使 10 万请求，也需要约 6 小时才溢出。溢出后会从负数继续递增，不影响功能（key 是唯一的）。但如果极端情况下不放心，可以定期重置。

#### Q3: 信号量大小如何选择？

A: 服务端 `serverAsyncSemaphoreValue=64` 意味着最多同时处理 64 个异步请求。这个值需要根据服务端处理能力调整。太小会限制吞吐，太大会导致系统过载。客户端默认 65535 基本不限制，因为客户端请求量通常远小于服务端。

#### Q4: GO_AWAY 重连可能导致请求重复吗？

A: 可能的。如果请求已经到达服务端但响应返回 GO_AWAY，客户端重连后会重试同一个请求。因此业务逻辑需要幂等性。RocketMQ 内部对此有处理，例如发送消息时 Broker 会进行去重。

#### Q5: 为什么客户端 Worker 线程数只有 1？

A: 客户端使用单个 NIO 线程处理所有 IO 事件。由于请求是异步的，单个 IO 线程足以处理高并发请求。业务逻辑处理在回调线程池中执行，不会阻塞 IO 线程。这是 Netty 推荐的线程模型。

#### Q6: responseTable 会不会内存泄漏？

A: 不会。有三重保障：
1. `scanResponseTable` 每 1 秒扫描超时的 ResponseFuture 并清理
2. `failFast` 在通道关闭时清理所有关联的 ResponseFuture
3. 正常响应到达时 `processResponseCommand` 会从 `responseTable` 移除

#### Q7: NettyDecoder 为什么继承 LengthFieldBasedFrameDecoder？

A: `LengthFieldBasedFrameDecoder` 处理 TCP 粘包/半包问题。它读取帧头 4 字节长度字段，确保读取完整的帧后才传递给后续 Handler。如果数据不完整，会缓存等待更多数据到达。

#### Q8: 序列化类型可以动态切换吗？

A: 可以，但需要全局一致。通过系统属性 `rocketmq.serialize.type=ROCKETMQ` 设置。序列化类型编码在每帧的 `headerLength|serializeType` 字段高 8 位，接收端动态识别并使用对应解码器。但生产环境中通常保持 JSON 序列化，除非有明确的性能需求。

### 19.4 性能优化要点

1. **FastCodesHeader 避免反射**：对性能敏感的请求头（如 PullMessageRequestHeader）实现 `FastCodesHeader` 接口，直接编码/解码，跳过反射开销。

2. **PooledByteBufAllocator**：使用 Netty 的池化 ByteBuf 分配器，减少 GC 压力。

3. **Epoll Native**：Linux 下使用 Epoll 替代 NIO，性能更高。

4. **TCP_NODELAY**：禁用 Nagle 算法，减少小包延迟。

5. **信号量限流**：防止过多并发请求压垮系统，服务端信号量（64/256）远小于客户端（65535）。

6. **HashedWheelTimer**：时间轮算法 O(1) 复杂度添加超时任务，比优先队列更高效。

7. **异步回调**：核心路径全部异步，IO 线程不做业务逻辑，业务在独立线程池执行。

8. **连接复用**：`channelTables` 缓存 Channel，避免重复建连。

### 19.5 Remoting 在 RocketMQ 中的应用

| 使用场景 | 客户端 | 服务端 | 请求码 |
|---------|--------|--------|--------|
| Producer 发送消息 | Producer | Broker | SEND_MESSAGE (10) |
| Consumer 拉取消息 | Consumer | Broker | PULL_MESSAGE (11) |
| Consumer Pop 消息 | Consumer | Broker | POP_MESSAGE (200050) |
| 心跳 | Client | Broker | HEART_BEAT (34) |
| 路由查询 | Client | NameServer | GET_ROUTEINFO_BY_TOPIC (105) |
| Broker 注册 | Broker | NameServer | REGISTER_BROKER (103) |
| 消费偏移量 | Consumer | Broker | UPDATE_CONSUMER_OFFSET (15) |
| 事务检查 | Broker | Producer | CHECK_TRANSACTION_STATE (39) |
| 主从同步 | Slave | Master | (HA 协议) |

---

> **文档版本**: v1.0
> **基于 RocketMQ 源码版本**: 5.x
> **覆盖内容**: RocketMQ Remoting 通信层全链路，从协议设计到连接管理到 GO_AWAY 重连
