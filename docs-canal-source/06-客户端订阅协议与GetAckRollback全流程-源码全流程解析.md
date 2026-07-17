# 客户端订阅协议与 Get/Ack/Rollback 全流程 —— 源码全流程解析

> 本文聚焦 Canal 数据面最靠近使用者的一层：**客户端与服务端之间的 TCP 交互协议**。
> 从 `CanalConnector` 接口的抽象设计出发，逐字节拆解 Canal 自定义的"4 字节长度 + Protobuf Body"帧格式，
> 完整走读一次客户端 `connect → handshake → auth → subscribe → getWithoutAck → ack → rollback → disconnect`
> 的全生命周期，再深入服务端 Netty Pipeline 的三段式处理器（握手/认证/会话），
> 最后落到 `CanalServerWithEmbedded` 的数据服务实现，看清 batchId、cursor、EventStore 三者在
> "至少一次投递（at-least-once）"语义下是如何协同工作的。
>
> 阅读完本文，你将能够回答这些问题：
> - 为什么 Canal 要设计一个 `getWithoutAck` 而不是只提供自动确认的 `get`？
> - 客户端明文密码是怎样经过 SHA-1 scramble 变换后安全地传给服务端的？服务端又是怎么校验的？
> - 集群模式（ClusterCanalConnector）是如何通过 ZooKeeper 感知 active server 切换、并自动 failover 的？
> - 同一个 destination 被多个客户端同时消费时，Canal 用什么机制保证"同一时刻只有一个客户端在拉数据"？
> - 服务端处理 GET 请求时，那段绕过 Protobuf Builder、手写 `CodedOutputStream` 的"高性能 raw 模式"到底优化了什么？

---

## 全局定位：客户端协议在整体架构中的位置

在前几篇文档里，我们从 MySQL 端一路走到了 Canal 内部：

- 第 02/03 篇：Parser 模拟 Slave 拉取并解析 Binlog，产出 `CanalEntry.Entry`；
- 第 04 篇：Sink 过滤后写入 Store 环形缓冲区（`MemoryEventStoreWithBuffer`）；
- 第 05 篇：CanalInstance 把各组件装配起来，MetaManager 负责记录"客户端消费到哪里了"。

这些工作全部发生在**服务端进程内部**。但 Canal 的价值在于把 Binlog 变更"投递"给下游业务系统，而下游系统运行在**另一个进程、另一台机器**上。这就需要一层网络协议，把 Store 里的数据搬运到客户端。本文讲的就是这一层：

```
   ┌────────────────────────────────────────────────────────────────────────┐
   │                        业务方进程（Client）                              │
   │   ┌────────────────────────────────────────────────────────────┐        │
   │   │  CanalConnector（SimpleCanalConnector / ClusterCanalConnector）│      │
   │   │      connect / subscribe / getWithoutAck / ack / rollback     │      │
   │   └───────────────────────────┬───────────────────────────────────┘      │
   └───────────────────────────────┼──────────────────────────────────────────┘
                                   │  TCP（4 字节 length + Protobuf Body）
                                   │
   ┌───────────────────────────────┼──────────────────────────────────────────┐
   │            canal-deployer 进程（Server）                                  │
   │   ┌───────────────────────────┴────────────────────────────────┐          │
   │   │      CanalServerWithNetty（Netty 服务端）                     │          │
   │   │  Pipeline: Handshake → ClientAuthentication → Session         │          │
   │   └───────────────────────────┬────────────────────────────────┘          │
   │                               │  方法调用                                  │
   │   ┌───────────────────────────┴────────────────────────────────┐          │
   │   │      CanalServerWithEmbedded（数据服务，单例）                 │          │
   │   │  subscribe / getWithoutAck / ack / rollback / auth            │          │
   │   └───────────────────────────┬────────────────────────────────┘          │
   │                               │                                            │
   │              ┌────────────────┴─────────────────┐                          │
   │              │  MetaManager（batch/cursor）        │                         │
   │              │  EventStore（环形缓冲区）            │                         │
   │              └──────────────────────────────────┘                          │
   └────────────────────────────────────────────────────────────────────────┘
```

值得强调的一个架构事实：**Canal 服务端有两个 Server 抽象**。

- `CanalServerWithEmbedded`：真正干活的"数据服务"，它直接持有 `canalInstances` 这张 Map，所有 `subscribe/get/ack/rollback` 的业务逻辑都在这里。它是一个进程内单例（`SingletonHolder`），既可以被 Netty Server 包一层对外提供 TCP 服务，也可以被业务方直接嵌入到自己的进程里"零网络开销"地使用（这就是 embedded 的含义）。
- `CanalServerWithNetty`：网络门面。它内部持有一个 `CanalServerWithEmbedded`，把 TCP 上收到的 Protobuf 请求解析出来，翻译成对 embedded server 的方法调用，再把返回值序列化写回 TCP。

本文的主线是 TCP 模式（`SimpleCanalConnector` ↔ `CanalServerWithNetty` ↔ `CanalServerWithEmbedded`），因为这是绝大多数生产环境的部署形态。

---

## 第一章：全局交互时序总览

在钻进任何一行源码之前，先建立一张"上帝视角"的时序图。整个客户端会话可以拆成五个阶段：**建连握手、认证、订阅、循环消费、关闭**。

```
Client(SimpleCanalConnector)                                   Server(Netty Pipeline + Embedded)
        │                                                                    │
        │───────── TCP 三次握手（SocketChannel.connect）────────────────────▶│  channelOpen 触发
        │                                                                    │
        │◀──────── Packet{ type=HANDSHAKE, body=Handshake{ seeds=8字节随机 }} ─│  HandshakeInitializationHandler
        │                                                                    │
        │  scramble411(password, seed) 生成加密密码                            │
        │───────── Packet{ type=CLIENTAUTHENTICATION, body=ClientAuth } ─────▶│  ClientAuthenticationHandler
        │                                                                    │    embeddedServer.auth() 校验
        │                                                                    │    （可选）embeddedServer.subscribe()
        │◀──────── Packet{ type=ACK, body=Ack{ errorCode=0 } } ──────────────│    动态重构 Pipeline（移除握手/认证 Handler）
        │                                                                    │
        │  ============ 认证完成，进入 SessionHandler 处理阶段 ============      │
        │                                                                    │
        │───────── Packet{ type=SUBSCRIPTION, body=Sub{ dest,clientId,filter }}▶│  embeddedServer.subscribe()
        │◀──────── Packet{ type=ACK } ───────────────────────────────────────│
        │                                                                    │
        │───────── Packet{ type=CLIENTROLLBACK, body=ClientRollback{ 0 } } ──▶│  rollbackOnConnect：清空未 ack 批次
        │        （无 ACK 响应，fire-and-forget）                              │
        │                                                                    │
        │  ┌──────────────────── 循环消费（业务主循环）──────────────────┐    │
        │  │                                                            │    │
        │  │──── Packet{ type=GET, body=Get{ fetchSize,timeout,autoAck=false }}▶│ embeddedServer.getWithoutAck()
        │  │◀─── Packet{ type=MESSAGES, body=Messages{ batchId, entries[] } } ─│                        │
        │  │                                                            │    │
        │  │  业务处理 entries...                                        │    │
        │  │                                                            │    │
        │  │──── Packet{ type=CLIENTACK, body=ClientAck{ batchId } } ────▶│ embeddedServer.ack()（无 ACK 响应）
        │  │                                                            │    │
        │  │  （异常时）── Packet{ type=CLIENTROLLBACK } ────────────────▶│ embeddedServer.rollback()
        │  │                                                            │    │
        │  └────────────────────────────────────────────────────────────┘    │
        │                                                                    │
        │───────── TCP 关闭（SocketChannel.close）──────────────────────────▶│  channelClosed
        │                                                                    │
```

这张图里有几个反直觉但至关重要的细节，先点出来，后文逐一展开：

1. **握手是服务端主动发起的**。客户端 `connect()` 里 TCP 建连成功后，第一件事不是发数据，而是**读**一个包——这个包是服务端在 `channelOpen` 回调里主动推过来的 Handshake，携带了认证用的随机 seed。
2. **认证包同时可以携带订阅信息**。`ClientAuth` 里带了 `destination/clientId/filter` 字段，服务端认证通过后会顺手做一次 subscribe。但 `SimpleCanalConnector` 并没有用这个"顺手订阅"的能力，而是在认证完成后**单独再发一个 SUBSCRIPTION 包**。这是历史演进留下的冗余设计，两条路径都能工作。
3. **ack 和 rollback 是"只写不等"的**。观察 `SimpleCanalConnector.ack()`：它只 `writeWithHeader(...)`，**不 `readNextPacket()`**。服务端的 `SessionHandler` 处理 CLIENTACK/CLIENTROLLBACK 时也不回写 ACK 包（正常路径下只做 metrics 聚合）。这是一种 fire-and-forget 的性能优化，但也意味着 ack 失败客户端感知不到——这正是 at-least-once 语义的根源之一。
4. **连接建立后会自动 rollback 一次**。`rollbackOnConnect` 默认为 `true`，这保证了断线重连后，上一次没来得及 ack 的批次会被"退回"，下次 get 从断点重新拉取，避免丢数据。

---

## 第二章：CanalConnector 接口设计

### 2.1 接口全貌

客户端所有能力都收敛在 `CanalConnector` 这个接口上（`client/.../CanalConnector.java`）。它是业务方唯一需要面对的 API 抽象，`SimpleCanalConnector`（单机）和 `ClusterCanalConnector`（集群）是它的两个实现。

```java
public interface CanalConnector {

    void connect() throws CanalClientException;                 // 建立链接（含握手+认证）
    void disconnect() throws CanalClientException;              // 释放链接
    boolean checkValid() throws CanalClientException;           // 检查链接是否处于"工作节点"状态

    void subscribe(String filter) throws CanalClientException;  // 订阅（带 filter）
    void subscribe() throws CanalClientException;               // 订阅（用服务端配置的 filter）
    void unsubscribe() throws CanalClientException;             // 取消订阅

    Message get(int batchSize) throws CanalClientException;                       // 获取+自动确认
    Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException;

    Message getWithoutAck(int batchSize) throws CanalClientException;             // 获取但不确认
    Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException;

    void ack(long batchId) throws CanalClientException;         // 确认（<= batchId 的都确认）
    void rollback(long batchId) throws CanalClientException;    // 回滚指定批次
    void rollback() throws CanalClientException;                // 回滚所有未 ack 批次
}
```

把这 13 个方法按语义分组，脉络就清晰了：

| 分组 | 方法 | 作用 |
| --- | --- | --- |
| 连接生命周期 | `connect` / `disconnect` / `checkValid` | 建连、断连、判活 |
| 订阅关系 | `subscribe` / `unsubscribe` | 声明本客户端要消费哪个 destination、按什么 filter 过滤 |
| 数据获取 | `get` / `getWithoutAck` | 从服务端拉取一批变更数据 |
| 消费确认 | `ack` / `rollback` | 告诉服务端"这批我处理成功了/失败了" |

### 2.2 为什么要设计 `getWithoutAck`？—— at-least-once 语义的基石

这是 Canal 客户端 API 设计里最值得玩味的一点。乍看之下，`get(batchSize)` 已经很好用了：拉一批、自动确认、返回。为什么还要暴露一个"拉了但不确认"的 `getWithoutAck`？

答案藏在**消费的可靠性语义**里。考虑这样一个业务流程：

```
拉取数据 → 写入下游 DB/MQ → 确认
```

如果用 `get(batchSize)`（自动确认），确认动作发生在**数据刚拉到客户端内存、业务代码还没来得及处理**的时刻。此时一旦业务处理失败（写下游 DB 抛异常、进程崩溃），这批数据已经被服务端标记为"已确认"，cursor 已经前移，**这批数据就永久丢失了**——服务端不会再投递第二次。这是 **at-most-once（至多一次）** 语义，会丢数据。

而 `getWithoutAck` 把"确认"这个动作的控制权交还给了业务方：

```java
Message message = connector.getWithoutAck(batchSize);   // 拉取，但服务端仍认为"你还没确认"
long batchId = message.getId();
try {
    doBusiness(message);          // 业务处理（写下游）
    connector.ack(batchId);       // 处理成功后才确认
} catch (Exception e) {
    connector.rollback(batchId);  // 处理失败则回滚，下次会重新拉到这批
}
```

在这种模式下：

- 如果 `doBusiness` 成功、`ack` 也成功：数据被消费一次，cursor 前移。理想情况。
- 如果 `doBusiness` 失败：`rollback` 让服务端"退回"这批，下次 `getWithoutAck` 会**重新拉到同一批数据**。数据没丢，但可能重复。
- 如果 `doBusiness` 成功、但 `ack` 之前进程崩溃了：重启重连后，`rollbackOnConnect` 自动回滚，下次会**重新拉到已经处理过的那批**。数据没丢，但会重复处理。

可以看到，`getWithoutAck` + 手动 `ack`/`rollback` 提供的是 **at-least-once（至少一次）** 语义：**保证不丢，但可能重复**。这就是为什么 Canal 官方示例和几乎所有生产实践都推荐用 `getWithoutAck` 而非 `get`。至于"可能重复"的问题，需要业务方通过**幂等设计**（比如按主键 upsert、按唯一业务号去重）来兜底。

> 小结：`get` = `getWithoutAck` + 立即 `ack`（源码中确实就是这么实现的，见后文 2.3）。Canal 没有提供 exactly-once，因为在"服务端投递 + 客户端处理 + 确认"这个分布式流程里，exactly-once 需要下游存储配合两阶段提交，超出了 Canal 的职责边界。Canal 的定位是"可靠地把 Binlog 搬过来，不丢；重复交给你去幂等"。

### 2.3 `get` 就是 `getWithoutAck` + 立即 `ack`

翻开 `SimpleCanalConnector` 的实现，这一点一目了然：

```java
@Override
public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
    Message message = getWithoutAck(batchSize, timeout, unit);
    ack(message.getId());   // 拉完立刻确认
    return message;
}
```

`get` 只是把 `getWithoutAck` 和 `ack` 组合起来的语法糖。因为 `ack` 紧跟在拉取之后、在业务处理之前，所以它天然是 at-most-once 的。理解了这一层，就能理解为什么框架层面同时保留了两套 API：`get` 给"不在乎丢一点数据、图省事"的场景，`getWithoutAck` 给"绝不能丢数据"的场景。

### 2.4 `checkValid` 与集群语义

`checkValid` 这个方法在单机模式下永远返回 `true`（"默认都放过"），但在集群模式下有实际意义。它的 JavaDoc 说明了两种"链接不合法"的情形：

1. 一直连不上任何 canal server；
2. 当前客户端在 running 抢占中处于**备份节点**（非工作节点）。

第二点是集群消费的核心约束：**同一个 destination 在同一时刻只允许一个客户端在消费**（后文第七章展开）。当你的客户端是备份节点时，所有 `CanalConnector` 操作都会阻塞，直到它抢到工作权。所以官方建议业务方定时调 `checkValid()`，据此决定是否退出当前消费线程、释放资源。

---

## 第三章：Protobuf 协议定义

Canal 客户端与服务端之间跑的是自定义应用层协议，序列化用 Protobuf 3。协议定义在 `protocol/.../CanalProtocol.proto`，编译后生成 `CanalPacket.java`（一个上万行的巨型文件，我们只需理解 `.proto` 源即可）。

### 3.1 帧格式：4 字节大端 Length + Protobuf Body

Canal 没有用任何现成的 RPC 框架，而是自己定义了极简的分帧规则。看客户端的读写代码（`SimpleCanalConnector`）：

```java
private void writeWithHeader(WritableByteChannel channel, byte[] body) throws IOException {
    synchronized (writeDataLock) {
        writeHeader.clear();
        writeHeader.putInt(body.length);   // 4 字节，大端序，写入 body 长度
        writeHeader.flip();
        channel.write(writeHeader);        // 先写 4 字节头
        channel.write(ByteBuffer.wrap(body)); // 再写 body
    }
}

private byte[] readNextPacket(ReadableByteChannel channel) throws IOException {
    synchronized (readDataLock) {
        readHeader.clear();
        read(channel, readHeader);         // 先读满 4 字节头
        int bodyLen = readHeader.getInt(0);// 解析出 body 长度
        ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen).order(ByteOrder.BIG_ENDIAN);
        read(channel, bodyBuf);            // 按长度读满 body
        return bodyBuf.array();
    }
}
```

其中 `readHeader` 和 `writeHeader` 在构造时就固定为大端序：

```java
private final ByteBuffer readHeader  = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
private final ByteBuffer writeHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
```

帧结构示意：

```
   ┌───────────────┬──────────────────────────────────────────────┐
   │  Length (4B)  │              Protobuf Body (Length 字节)        │
   │  big-endian   │              CanalPacket.Packet                │
   └───────────────┴──────────────────────────────────────────────┘
```

这个设计的要点：

- **定长头解决 TCP 粘包/拆包**：TCP 是字节流，一次 `read` 未必对应一个完整逻辑包。先读固定 4 字节拿到长度，再"读满"整个 body（`read` 方法里用 `while (buffer.hasRemaining())` 循环，直到读够或流结束），这就把字节流精确切分成了一个个逻辑包。
- **读写各自加锁**：`readDataLock` 和 `writeDataLock` 是两把独立的锁。注释解释得很直白——"读也需要排他锁，并发度容易造成数据包混乱，反序列化失败"。因为一个包的读取分了"读头 + 读体"两步，如果两个线程交错读，就会把 A 包的头和 B 包的体拼一起，直接反序列化崩溃。写同理。用两把锁而非一把，是为了让"一个线程在读、另一个线程在写"能并行（全双工），减小锁粒度。

服务端 Netty 侧用的是 `LengthFieldBasedFrameDecoder`（在 `CanalServerWithNetty` 里配置，本文未展开该文件），解码规则与客户端的手写分帧严格对应：4 字节长度字段、大端。

### 3.2 Packet 结构

所有请求/响应都包在同一个 `Packet` 里：

```protobuf
message Packet {
     oneof magic_number_present { int32 magic_number = 1; }
     oneof version_present     { int32 version = 2; }
     PacketType type = 3;                              // 包类型（核心区分字段）
     oneof compression_present { Compression compression = 4; }
     bytes body = 5;                                   // 真正的负载，按 type 决定怎么解析
}
```

`Packet` 是个"信封"：`type` 字段决定了 `body` 里装的是什么消息。收到一个 Packet 后，先看 `type`，再用对应的 `parseFrom` 去解析 `body`。这是一种典型的"标签联合（tagged union）"设计。

`version` 字段用于协议版本协商。客户端 `doConnect` 里会校验：

```java
if (p.getVersion() != 1) {
    throw new CanalClientException("unsupported version at this client.");
}
```

而服务端 Handshake 包发的是 `NettyUtils.VERSION`，认证 Handler 里支持 `SUPPORTED_VERSION = 3`。这种"发一个版本、支持另一个版本"的写法是 Canal 长期演进中兼容性妥协的结果，实际运行中 default 分支兜底处理了各种版本。

### 3.3 PacketType 枚举

```protobuf
enum PacketType {
    PACKAGETYPECOMPATIBLEPROTO2 = 0;   // proto2 兼容占位
    HANDSHAKE = 1;                      // 服务端 → 客户端：握手（含 seed）
    CLIENTAUTHENTICATION = 2;             // 客户端 → 服务端：认证
    ACK = 3;                            // 服务端 → 客户端：通用应答
    SUBSCRIPTION = 4;                   // 客户端 → 服务端：订阅
    UNSUBSCRIPTION = 5;                 // 客户端 → 服务端：取消订阅
    GET = 6;                            // 客户端 → 服务端：拉取数据
    MESSAGES = 7;                       // 服务端 → 客户端：数据批次
    CLIENTACK = 8;                      // 客户端 → 服务端：确认
    SHUTDOWN = 9;                       // 管理：关闭
    DUMP = 10;                          // 集成：全量 dump
    HEARTBEAT = 11;                     // 心跳
    CLIENTROLLBACK = 12;                // 客户端 → 服务端：回滚
}
```

把这些类型按"谁发给谁"归类：

| 方向 | 类型 | Body 消息 | 触发时机 |
| --- | --- | --- | --- |
| Server → Client | HANDSHAKE | `Handshake` | TCP 建连后服务端主动推 |
| Client → Server | CLIENTAUTHENTICATION | `ClientAuth` | 客户端收到握手后回应 |
| Server → Client | ACK | `Ack` | 认证/订阅成功或失败的应答 |
| Client → Server | SUBSCRIPTION | `Sub` | 订阅 |
| Client → Server | UNSUBSCRIPTION | `Unsub` | 取消订阅 |
| Client → Server | GET | `Get` | 拉数据 |
| Server → Client | MESSAGES | `Messages` | 拉数据的响应（含 batchId + entries） |
| Client → Server | CLIENTACK | `ClientAck` | 确认批次 |
| Client → Server | CLIENTROLLBACK | `ClientRollback` | 回滚批次 |

### 3.4 各 Body 消息定义

**Handshake（握手）**

```protobuf
message Handshake {
    oneof communication_encoding_present { string communication_encoding = 1; }
    bytes seeds = 2;                          // 认证用的随机种子（8 字节）
    Compression supported_compressions = 3;   // 服务端支持的压缩算法
}
```

`seeds` 是握手的核心，它是服务端每次连接随机生成的 8 字节盐值，用于密码 scramble（第五章详解）。

**ClientAuth（认证）**

```protobuf
message ClientAuth {
    string username = 1;
    bytes password = 2;                       // 用 seed scramble 后的密码，不是明文
    oneof net_read_timeout_present  { int32 net_read_timeout = 3; }
    oneof net_write_timeout_present { int32 net_write_timeout = 4; }
    string destination = 5;                   // 可选：认证时顺带订阅
    string client_id = 6;
    string filter = 7;
    int64 start_timestamp = 8;
}
```

注意 `password` 是 `bytes` 且注释明确写着 "hashed password with seeds from Handshake message"——明文密码从不上网。

**Ack（通用应答）**

```protobuf
message Ack {
    oneof error_code_present { int32 error_code = 1; }  // 0 表示成功，>0 表示错误
    string error_message = 2;
}
```

**Sub / Unsub（订阅/取消订阅）**

```protobuf
message Sub {
    string destination = 1;
    string client_id = 2;
    string filter = 7;
}
message Unsub {
    string destination = 1;
    string client_id = 2;
    string filter = 7;
}
```

**Get（拉取请求）**

```protobuf
message Get {
    string destination = 1;
    string client_id = 2;
    int32 fetch_size = 3;                     // 一批最多拉多少条
    oneof timeout_present { int64 timeout = 4; } // -1 代表不做超时控制
    oneof unit_present    { int32 unit = 5; }  // 时间单位：0纳秒/1微秒/2毫秒/3秒/4分/5时/6天
    oneof auto_ack_present { bool auto_ack = 6; } // 是否自动 ack
}
```

**Messages（数据响应）**

```protobuf
message Messages {
    int64 batch_id = 1;                       // 本批次的唯一 id（后续 ack/rollback 用它）
    repeated bytes messages = 2;              // 每个元素是一个序列化后的 CanalEntry.Entry
}
```

这里 `messages` 是 `repeated bytes` 而非 `repeated Entry`，是刻意为之的性能优化：服务端可以直接把 Store 里"已经序列化好的 Entry 字节"塞进来，不必反序列化再序列化；客户端也可以选择"懒解析"（raw 模式），拿到字节先不解析，用到时再解。这一点在第六章 CanalMessageDeserializer 里详细讲。

**ClientAck / ClientRollback（确认/回滚）**

```protobuf
message ClientAck {
    string destination = 1;
    string client_id = 2;
    int64 batch_id = 3;
}
message ClientRollback {
    string destination = 1;
    string client_id = 2;
    int64 batch_id = 3;
}
```

二者结构完全一致，只是 `type` 不同。`batch_id = 0` 有特殊含义：ack 时 0 是非法值（会返回 402 错误），rollback 时 0 代表"回滚所有未 ack 批次"。

### 3.5 Compression 枚举

```protobuf
enum Compression {
    COMPRESSIONCOMPATIBLEPROTO2 = 0;
    NONE = 1;
    ZLIB = 2;
    GZIP = 3;
    LZF = 4;
}
```

协议预留了多种压缩算法，但客户端 `CanalMessageDeserializer` 目前只接受 `NONE` 和 `COMPRESSIONCOMPATIBLEPROTO2`，其余会抛 "compression is not supported in this connector"。也就是说 TCP 直连模式下数据是不压缩的（压缩通常在 MQ 投递等其它路径上处理）。

---

## 第四章：SimpleCanalConnector —— 单机直连客户端

这是最核心的一章。`SimpleCanalConnector` 是所有客户端行为的"真身"，连 `ClusterCanalConnector` 内部也是委托给一个 `SimpleCanalConnector` 实例干活的。

### 4.1 关键字段与状态

```java
public class SimpleCanalConnector implements CanalConnector {
    private SocketAddress        address;                 // 目标 server 地址
    private String               username;
    private String               password;
    private int                  soTimeout   = 60000;     // socket 读超时
    private int                  idleTimeout = 60*60*1000;// 空闲超时，默认 1 小时
    private String               filter;                  // 记录上次 filter，便于自动重连时重放

    private final ByteBuffer     readHeader  = ByteBuffer.allocate(4).order(BIG_ENDIAN);
    private final ByteBuffer     writeHeader = ByteBuffer.allocate(4).order(BIG_ENDIAN);
    private SocketChannel        channel;
    private ReadableByteChannel  readableChannel;
    private WritableByteChannel  writableChannel;
    private ClientIdentity       clientIdentity;          // destination + clientId + filter
    private ClientRunningMonitor runningMonitor;          // 集群下的 running 抢占控制
    private ZkClientx            zkClientx;
    private BooleanMutex         mutex       = new BooleanMutex(false); // 工作/备份状态门闩
    private volatile boolean     connected   = false;     // connect() 是否已执行（不代表在工作）
    private boolean              rollbackOnConnect    = true;  // 连上后自动 rollback
    private boolean              rollbackOnDisConnect = false; // 断开前自动 rollback
    private boolean              lazyParseEntry       = false; // 懒解析 Entry（raw 模式）
    private Object               readDataLock  = new Object();
    private Object               writeDataLock = new Object();
    private volatile boolean     running       = false;
}
```

构造函数里，`clientIdentity` 的 `clientId` 被**硬编码为 1001**：

```java
this.clientIdentity = new ClientIdentity(destination, (short) 1001);
```

这是 Canal 的一个约定：一个 destination 下的客户端默认 clientId 都是 1001。这个 id 会参与 ZooKeeper 路径构造（集群 running 抢占）和服务端的 batch/cursor 元数据 key。

### 4.2 connect() 全流程

```java
@Override
public void connect() throws CanalClientException {
    if (connected) {
        return;                             // 幂等：已连接直接返回
    }

    if (runningMonitor != null) {           // 集群模式：走 ZK running 抢占
        if (!runningMonitor.isStart()) {
            runningMonitor.start();         // 抢占成功后回调里会 doConnect
        }
    } else {                                // 单机模式：直接连
        waitClientRunning();
        if (!running) {
            return;
        }
        doConnect();
        if (filter != null) {               // 自动重连场景，重放上次的 filter
            subscribe(filter);
        }
        if (rollbackOnConnect) {
            rollback();                     // 连上后自动回滚，保证从断点续拉
        }
    }

    connected = true;
}
```

单机模式下 `runningMonitor == null`，走 else 分支：`waitClientRunning()` 在单机模式里只是把 `running` 置 true，然后 `doConnect()` 完成真正的握手认证。之后如果 `filter` 非空（说明这是一次自动重连，之前订阅过），重新订阅一次；`rollbackOnConnect` 默认 true，所以还会自动回滚一次。

集群模式下 `runningMonitor != null`，`connect()` 本身不直接建连，而是启动 `runningMonitor` 去 ZK 抢工作权，抢到后由回调 `processActiveEnter()` 触发 `doConnect()`（见第七章）。

### 4.3 doConnect() —— 握手与认证的核心

这是整个客户端最精华的一段代码，完成了"TCP 建连 → 读握手 → 加密密码 → 发认证 → 读 ACK"的完整流程：

```java
private InetSocketAddress doConnect() throws CanalClientException {
    try {
        channel = SocketChannel.open();
        channel.socket().setSoTimeout(soTimeout);
        SocketAddress address = getAddress();
        if (address == null) {
            address = getNextAddress();       // 集群模式下由子类重写，从 ZK 选一个 active server
        }
        channel.connect(address);             // ① TCP 三次握手
        readableChannel = Channels.newChannel(channel.socket().getInputStream());
        writableChannel = Channels.newChannel(channel.socket().getOutputStream());

        Packet p = Packet.parseFrom(readNextPacket());  // ② 读服务端主动推来的握手包
        if (p.getVersion() != 1) {
            throw new CanalClientException("unsupported version at this client.");
        }
        if (p.getType() != PacketType.HANDSHAKE) {
            throw new CanalClientException("expect handshake but found other type.");
        }

        Handshake handshake = Handshake.parseFrom(p.getBody());
        supportedCompressions.add(handshake.getSupportedCompressions());

        ByteString seed = handshake.getSeeds();         // ③ 取出认证 seed
        String newPasswd = password;
        if (password != null) {
            // ④ 用 seed 对密码做 scramble411 加密
            newPasswd = SecurityUtil.byte2HexStr(
                SecurityUtil.scramble411(password.getBytes(), seed.toByteArray()));
        }

        ClientAuth ca = ClientAuth.newBuilder()          // ⑤ 构造认证包
            .setUsername(username != null ? username : "")
            .setPassword(ByteString.copyFromUtf8(newPasswd != null ? newPasswd : ""))
            .setNetReadTimeout(idleTimeout)
            .setNetWriteTimeout(idleTimeout)
            .build();
        writeWithHeader(Packet.newBuilder()              // ⑥ 发送认证包
            .setType(PacketType.CLIENTAUTHENTICATION)
            .setBody(ca.toByteString())
            .build()
            .toByteArray());

        Packet ack = Packet.parseFrom(readNextPacket()); // ⑦ 读认证结果
        if (ack.getType() != PacketType.ACK) {
            throw new CanalClientException("unexpected packet type when ack is expected");
        }
        Ack ackBody = Ack.parseFrom(ack.getBody());
        if (ackBody.getErrorCode() > 0) {                // ⑧ errorCode > 0 表示认证失败
            throw new CanalClientException("something goes wrong when doing authentication: "
                                           + ackBody.getErrorMessage());
        }

        connected = true;
        return new InetSocketAddress(channel.socket().getLocalAddress(),
                                     channel.socket().getLocalPort());
    } catch (IOException | NoSuchAlgorithmException e) {
        throw new CanalClientException(e);
    }
}
```

逐步拆解：

1. **①TCP 建连**：`SocketChannel.open()` + `connect(address)`。注意用的是**阻塞式** SocketChannel（没有 configureBlocking(false)），配合 `soTimeout` 控制读超时。这跟服务端的 Netty NIO 是两种模型——客户端用阻塞 IO 更简单，因为它的交互是严格请求-响应的。
2. **②读握手包**：`readNextPacket()` 会阻塞直到读到一个完整包。这里体现了"服务端主动推握手"的设计——客户端 connect 后第一个动作是 read 而非 write。
3. **③④加密密码**：从握手包取出 `seed`，调 `SecurityUtil.scramble411(password.getBytes(), seed)` 做单向散列变换，再 `byte2HexStr` 转成十六进制字符串。明文密码经此变换后才上网。
4. **⑤⑥发认证**：把加密后的密码、用户名、超时参数打进 `ClientAuth`，包成 `CLIENTAUTHENTICATION` 类型的 Packet 发出。注意这里**没有填 destination/clientId/filter**，所以服务端的"顺带订阅"不会触发，订阅是后面单独发 SUBSCRIPTION 包完成的。
5. **⑦⑧读结果**：期望收到一个 `ACK` 包，`errorCode == 0` 表示认证通过，否则抛异常携带服务端返回的错误信息。

### 4.4 subscribe() —— 订阅

```java
@Override
public void subscribe(String filter) throws CanalClientException {
    waitClientRunning();
    if (!running) {
        return;
    }
    try {
        writeWithHeader(Packet.newBuilder()
            .setType(PacketType.SUBSCRIPTION)
            .setBody(Sub.newBuilder()
                .setDestination(clientIdentity.getDestination())
                .setClientId(String.valueOf(clientIdentity.getClientId()))
                .setFilter(filter != null ? filter : "")
                .build()
                .toByteString())
            .build()
            .toByteArray());

        Packet p = Packet.parseFrom(readNextPacket());  // 订阅需要等 ACK
        Ack ack = Ack.parseFrom(p.getBody());
        if (ack.getErrorCode() > 0) {
            throw new CanalClientException("failed to subscribe with reason: " + ack.getErrorMessage());
        }
        clientIdentity.setFilter(filter);               // 记住 filter
    } catch (IOException e) {
        throw new CanalClientException(e);
    }
}
```

订阅是一个**同步请求-响应**：发出 SUBSCRIPTION 包后，`readNextPacket()` 阻塞等待服务端的 ACK。这与后面 ack/rollback 的"只写不等"形成鲜明对比——订阅是低频、需要确认成功的操作，值得同步等待。

无参 `subscribe()` 直接传空串：

```java
@Override
public void subscribe() throws CanalClientException {
    subscribe(""); // 传递空字符即可，服务端会用配置文件里的 filter
}
```

按接口 JavaDoc，filter 为空时服务端用自己配置的 filter；非空时以客户端提交的为准（直接替换）。

### 4.5 unsubscribe() —— 取消订阅

结构与 subscribe 几乎一样，只是包类型换成 `UNSUBSCRIPTION`，Body 换成 `Unsub`，同样同步等 ACK：

```java
@Override
public void unsubscribe() throws CanalClientException {
    waitClientRunning();
    if (!running) return;
    try {
        writeWithHeader(Packet.newBuilder()
            .setType(PacketType.UNSUBSCRIPTION)
            .setBody(Unsub.newBuilder()
                .setDestination(clientIdentity.getDestination())
                .setClientId(String.valueOf(clientIdentity.getClientId()))
                .build().toByteString())
            .build().toByteArray());
        Packet p = Packet.parseFrom(readNextPacket());
        Ack ack = Ack.parseFrom(p.getBody());
        if (ack.getErrorCode() > 0) {
            throw new CanalClientException("failed to unSubscribe with reason: " + ack.getErrorMessage());
        }
    } catch (IOException e) {
        throw new CanalClientException(e);
    }
}
```

### 4.6 getWithoutAck() —— 拉取数据

```java
@Override
public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
    waitClientRunning();
    if (!running) {
        return null;
    }
    try {
        int size = (batchSize <= 0) ? 1000 : batchSize;             // 默认 1000
        long time = (timeout == null || timeout < 0) ? -1 : timeout; // -1 = 不做超时
        if (unit == null) {
            unit = TimeUnit.MILLISECONDS;
        }

        writeWithHeader(Packet.newBuilder()
            .setType(PacketType.GET)
            .setBody(Get.newBuilder()
                .setAutoAck(false)                                   // 关键：不自动 ack
                .setDestination(clientIdentity.getDestination())
                .setClientId(String.valueOf(clientIdentity.getClientId()))
                .setFetchSize(size)
                .setTimeout(time)
                .setUnit(unit.ordinal())                             // 时间单位序号
                .build()
                .toByteString())
            .build()
            .toByteArray());
        return receiveMessages();                                    // 读 MESSAGES 响应
    } catch (IOException e) {
        throw new CanalClientException(e);
    }
}

private Message receiveMessages() throws IOException {
    byte[] data = readNextPacket();
    return CanalMessageDeserializer.deserializer(data, lazyParseEntry);
}
```

重点：

- `setAutoAck(false)` 是 `getWithoutAck` 与 `get` 在协议层唯一的区别。但实际上服务端 `SessionHandler` 里 autoAck 分支被注释掉了（见 6.3），服务端**永远走 getWithoutAck 逻辑**，是否自动 ack 完全由客户端 `get()` 里的"拉完立即调 ack"来决定。
- `unit.ordinal()` 把 Java 的 `TimeUnit` 枚举序号（NANOSECONDS=0…DAYS=6）塞进协议，服务端 `convertTimeUnit` 再反向映射回来。
- `timeout = -1` 是"不阻塞、有多少拿多少"，`timeout = 0` 是"阻塞直到拿够 batchSize"，`timeout > 0` 是"最多等这么久"。这三态语义在服务端 `getEvents` 里落地（见 6.6）。

### 4.7 ack() —— 确认（fire-and-forget）

```java
@Override
public void ack(long batchId) throws CanalClientException {
    waitClientRunning();
    if (!running) return;
    ClientAck ca = ClientAck.newBuilder()
        .setDestination(clientIdentity.getDestination())
        .setClientId(String.valueOf(clientIdentity.getClientId()))
        .setBatchId(batchId)
        .build();
    try {
        writeWithHeader(Packet.newBuilder()
            .setType(PacketType.CLIENTACK)
            .setBody(ca.toByteString())
            .build()
            .toByteArray());
        // 注意：没有 readNextPacket()！发完就返回
    } catch (IOException e) {
        throw new CanalClientException(e);
    }
}
```

**ack 只写不读**。发完 CLIENTACK 包立即返回，不等待服务端应答。这是性能优化（省一次 RTT），但代价是客户端无法确认服务端是否真的 ack 成功。结合前面 at-least-once 的讨论：即便 ack 包在网络上丢了，服务端没收到，也只是导致下次重连时重复投递这批数据——不丢数据，符合 at-least-once。

### 4.8 rollback() —— 回滚

```java
@Override
public void rollback(long batchId) throws CanalClientException {
    waitClientRunning();
    ClientRollback ca = ClientRollback.newBuilder()
        .setDestination(clientIdentity.getDestination())
        .setClientId(String.valueOf(clientIdentity.getClientId()))
        .setBatchId(batchId)
        .build();
    try {
        writeWithHeader(Packet.newBuilder()
            .setType(PacketType.CLIENTROLLBACK)
            .setBody(ca.toByteString())
            .build()
            .toByteArray());
        // 同样 fire-and-forget
    } catch (IOException e) {
        throw new CanalClientException(e);
    }
}

@Override
public void rollback() throws CanalClientException {
    waitClientRunning();
    rollback(0); // 0 代表回滚所有未 ack 批次
}
```

`rollback()` 无参版本传 `batchId = 0`，服务端据此判断为"回滚全部未 ack 批次"。`rollback(batchId)` 则只回滚指定批次。二者都是 fire-and-forget。

### 4.9 disconnect() 与资源清理

```java
@Override
public void disconnect() throws CanalClientException {
    if (rollbackOnDisConnect && channel.isConnected()) {
        rollback();                     // 可选：断开前回滚
    }
    connected = false;
    if (runningMonitor != null) {       // 集群：停止 running 抢占
        if (runningMonitor.isStart()) {
            runningMonitor.stop();
        }
    } else {                            // 单机：直接关 socket
        doDisconnect();
    }
}

private void doDisconnect() throws CanalClientException {
    if (readableChannel != null) { quietlyClose(readableChannel); readableChannel = null; }
    if (writableChannel != null) { quietlyClose(writableChannel); writableChannel = null; }
    if (channel != null)         { quietlyClose(channel);         channel = null; }
}
```

`rollbackOnDisConnect` 默认为 false（与 `rollbackOnConnect` 默认 true 相反）。因为断开时通常已经 ack 过了，没必要再回滚；而连接时回滚是为了兜住"上次没 ack 的"。

### 4.10 waitClientRunning() —— 单机与集群的分水岭

几乎所有业务方法开头都调了 `waitClientRunning()`，它是单机/集群行为差异的总开关：

```java
private void waitClientRunning() {
    try {
        if (zkClientx != null) {           // 集群模式
            if (!connected) {
                throw new CanalClientException("should connect first");
            }
            running = true;
            mutex.get();                   // 阻塞等待，直到自己成为工作节点
        } else {                           // 单机模式
            running = true;                // 直接放行
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CanalClientException(e);
    }
}
```

- **单机**：`zkClientx == null`，直接把 `running` 置 true 放行，没有任何阻塞。
- **集群**：`mutex.get()` 会阻塞，直到这个客户端在 ZK 上抢到"工作节点"身份（`mutex` 被 `processActiveEnter` 置 true）。如果它是备份节点，这里会一直阻塞——这就是 `checkValid()` 语义"备份节点所有操作都阻塞"的实现。

`stopRunning()` 提供了打断这个阻塞的手段：

```java
public void stopRunning() {
    if (running) {
        running = false;
        if (!mutex.state()) {
            mutex.set(true);   // 强行放行，让阻塞的 mutex.get() 返回
        }
    }
}
```

---

## 第五章：密码 Scramble 算法详解

Canal 的认证借鉴了 MySQL 的 challenge-response 机制：服务端发随机 seed（挑战），客户端用 seed 对密码做单向变换（响应），服务端用同样的 seed 和自己保存的密码做校验。全过程**明文密码从不上网，且每次 seed 不同，抓包重放也无效**。

算法实现在 `protocol/.../SecurityUtil.java`，类注释精炼地给出了数学定义：

```
1、client
   stage1_hash = SHA1(明文密码)
   token = SHA1(scramble + SHA1(stage1_hash)) XOR stage1_hash
2. server
   token = SHA1(token XOR SHA1(scramble + password))
3. checktoken vs password
```

### 5.1 客户端侧：scramble411

```java
public static final byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-1");
    byte[] pass1 = md.digest(pass);        // stage1 = SHA1(明文密码)
    md.reset();
    byte[] pass2 = md.digest(pass1);       // stage2 = SHA1(stage1)
    md.reset();
    md.update(seed);
    byte[] pass3 = md.digest(pass2);       // stage3 = SHA1(seed + stage2)
    for (int i = 0; i < pass3.length; i++) {
        pass3[i] = (byte) (pass3[i] ^ pass1[i]);  // token = stage3 XOR stage1
    }
    return pass3;
}
```

三次 SHA-1 加一次逐字节异或：

```
   明文密码 ──SHA1──▶ pass1 ──SHA1──▶ pass2 ──SHA1(seed+·)──▶ pass3
                       │                                        │
                       └──────────────── XOR ───────────────────┘
                                          │
                                          ▼
                                        token（上网传输的密码）
```

客户端把这个 `token`（20 字节）再经 `byte2HexStr` 转成 40 位十六进制字符串，塞进 `ClientAuth.password` 发给服务端。

### 5.2 服务端侧：scrambleServerAuth

服务端保存的是 `SHA1(SHA1(明文密码))`（即 stage2 的十六进制，称为 `scrambleGenPass` 的产物），而不是明文。校验逻辑：

```java
public static final boolean scrambleServerAuth(byte[] token, byte[] pass, byte[] seed)
                                                                    throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-1");
    md.update(seed);
    byte[] pass1 = md.digest(pass);         // pass1 = SHA1(seed + 服务端存的stage2)
    for (int i = 0; i < pass1.length; i++) {
        pass1[i] = (byte) (token[i] ^ pass1[i]);  // 还原出客户端的 stage1
    }
    md = MessageDigest.getInstance("SHA-1");
    byte[] pass2 = md.digest(pass1);        // pass2 = SHA1(还原出的stage1)
    return Arrays.equals(pass, pass2);      // 与服务端存的 stage2 比较
}
```

推导一下为什么能校验成功。设明文密码为 P：

- 客户端算出 `token = SHA1(seed + SHA1(SHA1(P))) XOR SHA1(P)`
- 服务端存的 `pass = SHA1(SHA1(P))`
- 服务端算 `pass1 = SHA1(seed + pass) = SHA1(seed + SHA1(SHA1(P)))`
- 于是 `token XOR pass1 = SHA1(P)`（异或抵消，还原出客户端的 stage1）
- 再 `pass2 = SHA1(SHA1(P))`
- 若密码正确，`pass2 == pass`，校验通过。

这套设计的安全性在于：服务端从不存明文、seed 每次随机（抗重放）、SHA-1 单向不可逆（抓包拿到 token 也反推不出密码）。

### 5.3 服务端 auth() 的分支处理

服务端 `CanalServerWithEmbedded.auth()` 在调用 scramble 校验前，还有一层"空密码放行"的逻辑：

```java
public boolean auth(String user, String passwd, byte[] seed) {
    // 用户名匹配（server 未配置 user 则任意用户名放行）
    if ((StringUtils.isEmpty(this.user) || StringUtils.equals(this.user, user))) {
        if (StringUtils.isEmpty(this.passwd)) {
            return true;                        // server 没配密码，直接放行
        } else if (StringUtils.isEmpty(passwd)) {
            return false;                       // server 配了密码但客户端没传，拒绝
        }
        try {
            byte[] passForClient = SecurityUtil.hexStr2Bytes(passwd);   // 客户端传的 token（hex→bytes）
            return SecurityUtil.scrambleServerAuth(passForClient,
                SecurityUtil.hexStr2Bytes(this.passwd), seed);          // 服务端存的密码同样是 hex
        } catch (NoSuchAlgorithmException e) {
            return false;
        }
    }
    return false;
}
```

注意 `this.passwd` 也是十六进制字符串（`scrambleGenPass` 的产物存进配置），`hexStr2Bytes` 把它转回字节再喂给 `scrambleServerAuth`。这解释了 `SecurityUtil` 里为什么要有 `hexStr2Bytes` 这个反向转换工具。

---

## 第六章：CanalMessageDeserializer —— raw 模式与懒解析

客户端收到 MESSAGES 响应后，用 `CanalMessageDeserializer.deserializer` 把字节反序列化成 `Message` 对象。这里藏着一个重要的性能优化开关：`lazyParseEntry`。

### 6.1 反序列化实现

```java
public static Message deserializer(byte[] data, boolean lazyParseEntry) {
    try {
        if (data == null) return null;
        CanalPacket.Packet p = CanalPacket.Packet.parseFrom(data);
        switch (p.getType()) {
            case MESSAGES: {
                if (!p.getCompression().equals(Compression.NONE)
                    && !p.getCompression().equals(Compression.COMPRESSIONCOMPATIBLEPROTO2)) {
                    throw new CanalClientException("compression is not supported in this connector");
                }
                CanalPacket.Messages messages = CanalPacket.Messages.parseFrom(p.getBody());
                Message result = new Message(messages.getBatchId());
                if (lazyParseEntry) {
                    result.setRawEntries(messages.getMessagesList()); // 只存字节，不解析
                    result.setRaw(true);
                } else {
                    for (ByteString byteString : messages.getMessagesList()) {
                        result.addEntry(CanalEntry.Entry.parseFrom(byteString)); // 逐条解析成 Entry
                    }
                    result.setRaw(false);
                }
                return result;
            }
            case ACK: {   // 服务端返回错误时，MESSAGES 位置会是 ACK 包
                Ack ack = Ack.parseFrom(p.getBody());
                throw new CanalClientException("something goes wrong with reason: " + ack.getErrorMessage());
            }
            default:
                throw new CanalClientException("unexpected packet type: " + p.getType());
        }
    } catch (Exception e) {
        throw new CanalClientException("deserializer failed by " + e.getMessage(), e);
    }
}
```

### 6.2 raw 模式 vs 非 raw 模式

`Message` 内部有两套存储（`protocol/.../Message.java`）：

```java
public class Message implements Serializable {
    private long                   id;         // batchId
    private List<CanalEntry.Entry> entries    = new ArrayList<>();   // 解析后的 Entry
    private boolean                raw        = true;
    private List<ByteString>       rawEntries = new ArrayList<>();   // 未解析的原始字节
}
```

- **非 raw 模式（lazyParseEntry=false，默认）**：反序列化时就把每个 `ByteString` 解析成 `CanalEntry.Entry`，存进 `entries`。业务方拿到 Message 直接 `getEntries()` 即可。代价是拉取时就要付出全部解析成本，哪怕业务只关心其中几条。
- **raw 模式（lazyParseEntry=true）**：只把原始字节存进 `rawEntries`，`raw=true`。业务方需要时自己调 `Entry.parseFrom(byteString)` 解析。适合"先快速拿到批次、按需解析"或"只做转发不解析"的场景，能显著降低客户端 CPU 和 GC 压力。

这个优化对应 GitHub issue #726，核心思想是**把序列化/反序列化的成本尽量后移或消除**。服务端侧也有对应的 raw 优化（见 6.4）。

---

## 第七章：集群 HA 客户端

### 7.1 ClusterCanalConnector —— failover 外壳

`ClusterCanalConnector` 本身不做任何网络 IO，它是一个"带重试和故障转移的外壳"，内部委托给 `currentConnector`（一个 `SimpleCanalConnector`）：

```java
public class ClusterCanalConnector implements CanalConnector {
    private int                     retryTimes    = 3;      // 每个操作最多重试次数
    private int                     retryInterval = 5000;   // 重试间隔 5 秒
    private CanalNodeAccessStrategy accessStrategy;         // 节点选择策略
    private SimpleCanalConnector    currentConnector;       // 真正干活的内部连接
}
```

**connect()：内层重试 + 外层选点**

```java
@Override
public void connect() throws CanalClientException {
    while (currentConnector == null) {
        int times = 0;
        while (true) {
            try {
                currentConnector = new SimpleCanalConnector(null, username, password, destination) {
                    @Override
                    public SocketAddress getNextAddress() {
                        return accessStrategy.nextNode();   // 由策略动态选一个 active server
                    }
                };
                currentConnector.setSoTimeout(soTimeout);
                currentConnector.setIdleTimeout(idleTimeout);
                if (filter != null) currentConnector.setFilter(filter);
                if (accessStrategy instanceof ClusterNodeAccessStrategy) {
                    currentConnector.setZkClientx(((ClusterNodeAccessStrategy) accessStrategy).getZkClient());
                }
                currentConnector.connect();
                break;
            } catch (Exception e) {
                logger.warn("failed to connect to canal server after retry {} times", times);
                currentConnector.disconnect();
                currentConnector = null;
                times = times + 1;
                if (times >= retryTimes) {
                    throw new CanalClientException(e);
                } else {
                    try {
                        Thread.sleep(retryInterval);   // issue #55：避免 CPU 打满
                    } catch (InterruptedException e1) {
                        throw new CanalClientException(e1);
                    }
                }
            }
        }
    }
}
```

关键点：

- 内部 `SimpleCanalConnector` 用匿名子类重写了 `getNextAddress()`，把"选哪个 server"委托给 `accessStrategy`。回顾 `SimpleCanalConnector.doConnect()` 里 `address == null` 时会调 `getNextAddress()`——集群模式下 address 传的是 null，正是走这条路。
- 如果 accessStrategy 是基于 ZK 的 `ClusterNodeAccessStrategy`，还会把 zkClient 注入 SimpleCanalConnector，从而激活其 `runningMonitor`（running 抢占）。
- `retryTimes` 默认 3，`retryInterval` 默认 5 秒，重试间隔里 sleep 是为了避免"疯狂重连打满 CPU"（issue #55）。

**业务方法：统一的"重试 + restart"模板**

所有业务方法（subscribe/get/getWithoutAck/ack/rollback）都套用同一个模板：

```java
@Override
public Message getWithoutAck(int batchSize) throws CanalClientException {
    int times = 0;
    while (times < retryTimes) {
        try {
            return currentConnector.getWithoutAck(batchSize);
        } catch (Throwable t) {
            logger.warn("something goes wrong when getWithoutAck data from server:{}", ...);
            times++;
            restart();     // 断开重连（可能连到新的 active server）
            logger.info("restart the connector for next round retry.");
        }
    }
    throw new CanalClientException("failed to fetch the data after " + times + " times retry");
}

private void restart() throws CanalClientException {
    disconnect();
    try {
        Thread.sleep(retryInterval);
    } catch (InterruptedException e) {
        throw new CanalClientException(e);
    }
    connect();             // 重连时 getNextAddress 会重新从 ZK 选点
}
```

这就是 failover 的本质：任何操作抛异常，就 `restart()`（断开 + sleep + 重连），重连时 `getNextAddress()` 会从 ZK 拿到**当前最新的 active server**。如果原 server 挂了、ZK 上 running 节点已切换，重连自然就连到新 leader 了。`connect()` 内部还会自动 `subscribe(filter)` + `rollback()`，所以重连后订阅关系和消费位点都能无缝恢复。

`subscribe` 里还有一个优雅停机的细节：

```java
if (retryTimes == -1 && t.getCause() instanceof InterruptedException) {
    logger.info("block waiting interrupted by other thread.");
    return;   // retryTimes=-1 时，允许被中断打断阻塞，用于优雅停机
}
```

设置 `retryTimes = -1` 可以让 `while (times < retryTimes)` 变成"无限重试"，同时允许外部通过 interrupt 打断，实现优雅停机。

这里还有一个容易踩坑的细节值得展开：**`ClusterCanalConnector` 的重试是"操作级"的，而非"批次级"的**。以 `getWithoutAck` 为例，如果第一次 `currentConnector.getWithoutAck(batchSize)` 成功拿到了数据、但客户端在处理响应时抛了异常（比如反序列化失败），`ClusterCanalConnector` 会 `restart()` 后**重新拉一次**。由于服务端 `getWithoutAck` 对"未 ack 批次"的幂等重发语义，重连后从 cursor/lastestBatch 起点重取，通常能拿到"同一批数据"，不会丢。但如果异常发生在 `ack` 之后、`restart` 之前，就可能出现"这批已 ack、下一批还没拉"的边界，需要业务侧幂等兜底。

另一个细节是 `restart()` 里固定 `Thread.sleep(retryInterval)`（默认 5 秒）。这意味着 failover 不是"瞬时"的——从原 server 挂掉到客户端连上新 server，最坏情况要经历"操作超时 + 5 秒重试间隔 + ZK 感知新 running + 重新握手认证订阅"这一整套流程，通常在秒级到十几秒不等。对延迟敏感的业务需要把这个恢复窗口纳入 SLA 考量，必要时调小 `retryInterval` 和 socket `soTimeout`。

`ClusterCanalConnector` 各方法与内部 `SimpleCanalConnector` 的委托关系可以归纳为下表：

| ClusterCanalConnector 方法 | 委托给 | 失败处理 |
| --- | --- | --- |
| `connect()` | 新建 `SimpleCanalConnector` + `connect()` | 每节点重试 `retryTimes` 次，间隔 `retryInterval` |
| `subscribe(filter)` | `currentConnector.subscribe(filter)` | 重试 + `restart()`；`retryTimes=-1` 时可被中断优雅退出 |
| `unsubscribe()` | `currentConnector.unsubscribe()` | 重试 + `restart()` |
| `get / getWithoutAck` | `currentConnector` 对应方法 | 重试 + `restart()` |
| `ack / rollback` | `currentConnector` 对应方法 | 重试 + `restart()` |
| `disconnect()` | `currentConnector.disconnect()` + 置 null | — |
| `checkValid()` | `currentConnector.checkValid()` | 空连接返回 false |

### 7.2 ClusterNodeAccessStrategy —— 基于 ZK 的节点选择

`ClusterNodeAccessStrategy` 负责回答"现在应该连哪个 server"。它通过监听两个 ZK 路径来维护 server 列表和当前 active 节点：

```java
public ClusterNodeAccessStrategy(String destination, ZkClientx zkClient) {
    this.destination = destination;
    this.zkClient = zkClient;

    // 监听器 1：所有 server 列表变化
    childListener = (parentPath, currentChilds) -> initClusters(currentChilds);

    // 监听器 2：当前工作节点（running）变化
    dataListener = new IZkDataListener() {
        public void handleDataDeleted(String dataPath) { runningAddress = null; }
        public void handleDataChange(String dataPath, Object data) { initRunning(data); }
    };

    // 订阅 /otter/canal/destinations/{destination}/cluster 下的子节点
    String clusterPath = ZookeeperPathUtils.getDestinationClusterRoot(destination);
    this.zkClient.subscribeChildChanges(clusterPath, childListener);
    initClusters(this.zkClient.getChildren(clusterPath));

    // 订阅 /otter/canal/destinations/{destination}/running 节点数据
    String runningPath = ZookeeperPathUtils.getDestinationServerRunning(destination);
    this.zkClient.subscribeDataChanges(runningPath, dataListener);
    initRunning(this.zkClient.readData(runningPath, true));
}
```

选点逻辑 `nextNode()`：

```java
@Override
public SocketAddress nextNode() {
    if (runningAddress != null) {
        return runningAddress;              // 优先返回当前 active server
    } else if (!currentAddress.isEmpty()) {
        return currentAddress.get(0);       // 没有 active，随机挑一个触发 lazy 启动
    } else {
        throw new ServerNotFoundException("no alive canal server for " + destination);
    }
}
```

- `runningAddress`：ZK `running` 节点里记录的当前工作 server。它由 `initRunning` 从 `ServerRunningData` 里解析出来。一旦 server 发生 HA 切换，ZK 的 running 节点数据变化，`handleDataChange` 回调更新 `runningAddress`，下次 `nextNode()` 就返回新 server。
- `currentAddress`：所有存活 server 列表，`initClusters` 里做了 `Collections.shuffle`（打散），避免所有客户端都往第一个节点挤。当没有 active server 时（比如 server 集群是 lazy 启动的），随机挑一个去"触发"它启动。

这套机制让客户端**无需感知具体 server IP**，只要连 ZK 就能自动追踪 active server 的漂移。

### 7.3 ClientRunningMonitor —— 客户端消费权的串行化

这是集群消费里最容易被忽略、却最关键的一环。**问题**：如果你为了高可用，部署了两个相同的消费客户端（同一个 destination、同一个 clientId=1001），它们会不会同时拉数据、导致重复消费和乱序？

**答案**：不会。Canal 用 `ClientRunningMonitor` 在 ZK 上做了"客户端消费权抢占"——同一时刻只有一个客户端能真正消费，另一个作为热备阻塞等待。

抢占逻辑 `initRunning()`：

```java
public synchronized void initRunning() {
    if (!isStart()) return;
    String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, clientData.getClientId());
    byte[] bytes = JsonUtils.marshalToByte(clientData);
    try {
        mutex.set(false);
        zkClient.create(path, bytes, CreateMode.EPHEMERAL);   // 抢占：创建临时节点
        processActiveEnter();                                 // 抢到了！回调建连
        activeData = clientData;
        mutex.set(true);                                      // 放行业务操作
    } catch (ZkNodeExistsException e) {
        // 节点已存在，说明别人抢到了
        bytes = zkClient.readData(path, true);
        if (bytes == null) {
            initRunning();                                    // 刚好被删了，重试
        } else {
            activeData = JsonUtils.unmarshalFromByte(bytes, ClientRunningData.class);
            if (activeData.getAddress().contains(":") && isMine(activeData.getAddress())) {
                mutex.set(true);                              // 是自己（重复抢占），放行
            }
            // 否则：我是备份节点，mutex 保持 false，业务操作阻塞
        }
    } catch (ZkNoNodeException e) {
        // 父节点不存在，先建父节点再重试
        zkClient.createPersistent(
            ZookeeperPathUtils.getClientIdNodePath(this.destination, clientData.getClientId()), true);
        initRunning();
    } catch (Throwable t) {
        ...
        releaseRunning();
        throw new CanalClientException("something goes wrong in initRunning method. ", t);
    }
}
```

利用 ZK 临时节点（EPHEMERAL）的"排他创建"特性：谁成功创建 `/otter/canal/destinations/{dest}/{clientId}/running` 谁就是工作节点，`mutex` 置 true 放行业务操作；创建失败的作为备份，`mutex` 保持 false，其 `waitClientRunning()` 里的 `mutex.get()` 一直阻塞。

**故障切换**由 `dataListener` 驱动：

```java
public void handleDataDeleted(String dataPath) throws Exception {
    MDC.put("destination", destination);
    mutex.set(false);
    processActiveExit();          // 触发退出
    if (!release && activeData != null && isMine(activeData.getAddress())) {
        initRunning();            // 上次就是自己，立即重新抢占
    } else {
        // 否则延迟 delayTime 秒再抢，避免网络抖动引起的频繁切换
        delayExector.schedule(() -> initRunning(), delayTime, TimeUnit.SECONDS);
    }
}
```

当工作节点的客户端进程崩溃或断网，它在 ZK 上的临时 running 节点会自动消失，触发所有备份客户端的 `handleDataDeleted`。备份客户端随即调 `initRunning()` 重新抢占——谁先抢到谁上位。这里的 `delayTime`（默认 5 秒）延迟重抢是为了避免网络瞬断/ZK session 抖动导致的"抢占抖动"。

抢占成功后的建连回调 `processActiveEnter`（在 `SimpleCanalConnector.initClientRunningMonitor` 里定义）：

```java
runningMonitor.setListener(new ClientRunningListener() {
    public InetSocketAddress processActiveEnter() {
        InetSocketAddress address = doConnect();   // 真正建连
        mutex.set(true);
        if (filter != null) subscribe(filter);     // 恢复订阅
        if (rollbackOnConnect) rollback();          // 回滚未 ack
        return address;
    }
    public void processActiveExit() {
        mutex.set(false);
        doDisconnect();                             // 让出工作权时断开连接
    }
});
```

这样就形成了完整闭环：**ZK 抢占成功 → 建立 TCP 连接 → 恢复订阅 → 回滚未 ack → 开始消费；节点故障 → 临时节点消失 → 备份抢占上位 → 从断点继续消费**。整个过程业务方无感知，只要循环调用 `getWithoutAck` 即可（备份期间会阻塞在 `mutex.get()`）。

---

## 第八章：服务端处理全链路

服务端用 Netty 承接 TCP 连接，Pipeline 采用"三段式"设计，且**认证完成后会动态重构 Pipeline**，把用不到的 Handler 移除，只留下会话处理器。

### 8.1 握手阶段：HandshakeInitializationHandler

这个 Handler 的特别之处在于：它在 `channelOpen`（连接刚建立）时**主动**推送握手包，而不是等客户端先说话。

```java
public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    if (childGroups != null) {
        childGroups.add(ctx.getChannel());     // 把 channel 登记进 group，便于统一管理
    }

    final byte[] seed = org.apache.commons.lang3.RandomUtils.nextBytes(8);  // 生成 8 字节随机 seed
    byte[] body = Packet.newBuilder()
        .setType(CanalPacket.PacketType.HANDSHAKE)
        .setVersion(NettyUtils.VERSION)
        .setBody(Handshake.newBuilder().setSeeds(ByteString.copyFrom(seed)).build().toByteString())
        .build()
        .toByteArray();

    NettyUtils.write(ctx.getChannel(), body, future -> {
        // 写完握手包后，把 seed 交给认证 Handler，供后续校验
        ClientAuthenticationHandler handler = (ClientAuthenticationHandler) ctx.getPipeline()
            .get(ClientAuthenticationHandler.class.getName());
        handler.setSeed(seed);
    });
    logger.info("send handshake initialization packet to : {}", ctx.getChannel());
}
```

要点：

- **seed 生成**：`RandomUtils.nextBytes(8)`，每个连接独立随机，是 scramble 认证抗重放的关键。
- **seed 的传递**：seed 生成在握手 Handler，但校验发生在认证 Handler。所以写完握手包的回调里，通过 pipeline 拿到 `ClientAuthenticationHandler` 并 `setSeed(seed)`，把 seed 交接过去。这是同一条 channel 上两个 Handler 之间的状态传递。

### 8.2 认证阶段：ClientAuthenticationHandler

```java
public void messageReceived(final ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
    final Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
    switch (packet.getVersion()) {
        case SUPPORTED_VERSION:
        default:
            final ClientAuth clientAuth = ClientAuth.parseFrom(packet.getBody());
            if (seed == null) {                    // 没 seed 无法校验，报错
                byte[] errorBytes = NettyUtils.errorPacket(400, "auth failed for seed is null");
                NettyUtils.write(ctx.getChannel(), errorBytes, null);
                break;
            }
            // ① 校验密码
            if (!embeddedServer.auth(clientAuth.getUsername(),
                                     clientAuth.getPassword().toStringUtf8(), seed)) {
                byte[] errorBytes = NettyUtils.errorPacket(400,
                    MessageFormatter.format("auth failed for user:{}", clientAuth.getUsername()).getMessage());
                NettyUtils.write(ctx.getChannel(), errorBytes, null);
                break;
            }
            // ② 认证包若携带订阅信息，顺带订阅并启动 instance
            if (StringUtils.isNotEmpty(clientAuth.getDestination())
                && StringUtils.isNotEmpty(clientAuth.getClientId())) {
                ClientIdentity clientIdentity = new ClientIdentity(clientAuth.getDestination(),
                    Short.valueOf(clientAuth.getClientId()), clientAuth.getFilter());
                try {
                    MDC.put("destination", clientIdentity.getDestination());
                    embeddedServer.subscribe(clientIdentity);
                    if (!embeddedServer.isStart(clientIdentity.getDestination())) {
                        ServerRunningMonitor runningMonitor =
                            ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
                        if (!runningMonitor.isStart()) {
                            runningMonitor.start();
                        }
                    }
                } finally {
                    MDC.remove("destination");
                }
            }
            // ③ 认证成功，回 ACK，并在回调里动态重构 Pipeline
            NettyUtils.ack(ctx.getChannel(), future -> {
                logger.info("remove unused channel handlers after authentication is done successfully.");
                ctx.getPipeline().remove(HandshakeInitializationHandler.class.getName());
                ctx.getPipeline().remove(ClientAuthenticationHandler.class.getName());

                // 装配空闲检测 Handler：超时自动关连接，节省服务端资源
                int readTimeout  = defaultSubscriptorDisconnectIdleTimeout;
                int writeTimeout = defaultSubscriptorDisconnectIdleTimeout;
                if (clientAuth.getNetReadTimeout() > 0)  readTimeout  = clientAuth.getNetReadTimeout();
                if (clientAuth.getNetWriteTimeout() > 0) writeTimeout = clientAuth.getNetWriteTimeout();
                IdleStateHandler idleStateHandler = new IdleStateHandler(
                    NettyUtils.hashedWheelTimer, readTimeout, writeTimeout, 0, TimeUnit.MILLISECONDS);
                ctx.getPipeline().addBefore(SessionHandler.class.getName(),
                    IdleStateHandler.class.getName(), idleStateHandler);

                IdleStateAwareChannelHandler idleStateAwareChannelHandler = new IdleStateAwareChannelHandler() {
                    public void channelIdle(ChannelHandlerContext ctx1, IdleStateEvent e1) {
                        logger.warn("channel:{} idle timeout exceeds, close channel...", ctx1.getChannel());
                        ctx1.getChannel().close();
                    }
                };
                ctx.getPipeline().addBefore(SessionHandler.class.getName(),
                    IdleStateAwareChannelHandler.class.getName(), idleStateAwareChannelHandler);
            });
            break;
    }
}
```

三个关键动作：

1. **①密码校验**：调 `embeddedServer.auth(username, password, seed)`，内部走第五章的 `scrambleServerAuth`。失败则返回 400 错误包。
2. **②顺带订阅**：如果认证包里带了 destination/clientId，服务端会顺手 subscribe 并按需启动 CanalInstance。但如前所述，`SimpleCanalConnector` 的认证包没填这些字段，所以这条路径在标准客户端下不触发。
3. **③动态重构 Pipeline**：这是最精彩的设计。认证成功后：
   - 从 pipeline 里**移除** HandshakeInitializationHandler 和 ClientAuthenticationHandler——它们的使命（一次性握手认证）已完成，留着浪费内存且可能被后续包错误命中。
   - **添加** `IdleStateHandler` + `IdleStateAwareChannelHandler`：做空闲连接检测，客户端长时间不读不写就自动关连接，防止资源泄漏。超时时间取客户端在 `ClientAuth` 里声明的 `netReadTimeout/netWriteTimeout`（对应客户端的 `idleTimeout`，默认 1 小时）。
   - 重构后，pipeline 里只剩 SessionHandler（加上刚加的空闲检测），后续所有请求都由 SessionHandler 处理。

重构前后 Pipeline 对比：

```
认证前：  FrameDecoder → HandshakeInitializationHandler → ClientAuthenticationHandler → SessionHandler
认证后：  FrameDecoder → IdleStateHandler → IdleStateAwareChannelHandler → SessionHandler
```

### 8.3 会话阶段：SessionHandler

认证完成后，所有 SUBSCRIPTION/UNSUBSCRIPTION/GET/CLIENTACK/CLIENTROLLBACK 请求都进入 `SessionHandler.messageReceived`，它是一个大 `switch`，按 `packet.getType()` 分发。

**SUBSCRIPTION 处理**

```java
case SUBSCRIPTION:
    Sub sub = Sub.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(sub.getDestination()) && StringUtils.isNotEmpty(sub.getClientId())) {
        ClientIdentity clientIdentity = new ClientIdentity(sub.getDestination(),
            Short.parseShort(sub.getClientId()), sub.getFilter());
        MDC.put("destination", clientIdentity.getDestination());
        // 按需启动 CanalInstance（lazy）
        if (!embeddedServer.isStart(clientIdentity.getDestination())) {
            ServerRunningMonitor runningMonitor =
                ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
            if (!runningMonitor.isStart()) runningMonitor.start();
        }
        embeddedServer.subscribe(clientIdentity);         // 核心：订阅
        byte[] ackBytes = NettyUtils.ackPacket();
        NettyUtils.write(ctx.getChannel(), ackBytes, ...); // 回 ACK
    } else {
        byte[] errorBytes = NettyUtils.errorPacket(401, "destination or clientId is null. Sub: " + sub);
        NettyUtils.write(ctx.getChannel(), errorBytes, ...);
    }
    break;
```

订阅时会"按需启动"对应 destination 的 CanalInstance——这就是为什么 Canal server 可以配置很多 destination 但只启动被订阅的那些（lazy 启动，节省资源）。订阅成功回 ACK。

**GET 处理（含高性能 raw 序列化）**

```java
case GET:
    Get get = CanalPacket.Get.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(get.getDestination()) && StringUtils.isNotEmpty(get.getClientId())) {
        ClientIdentity clientIdentity = new ClientIdentity(get.getDestination(),
            Short.parseShort(get.getClientId()));
        MDC.put("destination", clientIdentity.getDestination());
        Message message = null;
        // 注意：autoAck 分支被整体注释掉了，服务端一律走 getWithoutAck
        if (get.getTimeout() == -1) {
            message = embeddedServer.getWithoutAck(clientIdentity, get.getFetchSize());
        } else {
            TimeUnit unit = convertTimeUnit(get.getUnit());
            message = embeddedServer.getWithoutAck(clientIdentity, get.getFetchSize(), get.getTimeout(), unit);
        }

        if (message.getId() != -1 && message.isRaw()) {
            // ===== 高性能 raw 模式：手写 CodedOutputStream，跳过 Protobuf Builder =====
            List<ByteString> rowEntries = message.getRawEntries();
            int messageSize = 0;
            messageSize += CodedOutputStream.computeInt64Size(1, message.getId());
            int dataSize = 0;
            for (ByteString rowEntry : rowEntries) {
                dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);
            }
            messageSize += dataSize;
            messageSize += rowEntries.size();   // 每个 entry 的 tag 占 1 字节

            int size = 0;
            size += CodedOutputStream.computeEnumSize(3, PacketType.MESSAGES.getNumber());
            size += CodedOutputStream.computeTagSize(5)
                  + CodedOutputStream.computeRawVarint32Size(messageSize)
                  + messageSize;

            byte[] body = new byte[size];
            CodedOutputStream output = CodedOutputStream.newInstance(body);
            output.writeEnum(3, PacketType.MESSAGES.getNumber());          // Packet.type
            output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);      // Packet.body 的 tag
            output.writeRawVarint32(messageSize);                          // body 长度
            output.writeInt64(1, message.getId());                         // Messages.batch_id
            for (ByteString rowEntry : rowEntries) {
                output.writeBytes(2, rowEntry);                            // Messages.messages（直接写字节）
            }
            output.checkNoSpaceLeft();
            NettyUtils.write(ctx.getChannel(), body, ...);
        } else {
            // ===== 普通模式：用 Protobuf Builder 拼装 =====
            Messages.Builder messageBuilder = CanalPacket.Messages.newBuilder();
            messageBuilder.setBatchId(message.getId());
            if (message.getId() != -1) {
                if (message.isRaw() && !CollectionUtils.isEmpty(message.getRawEntries())) {
                    messageBuilder.addAllMessages(message.getRawEntries());
                } else if (!CollectionUtils.isEmpty(message.getEntries())) {
                    for (Entry entry : message.getEntries()) {
                        messageBuilder.addMessages(entry.toByteString());
                    }
                }
            }
            Packet.Builder packetBuilder = CanalPacket.Packet.newBuilder();
            packetBuilder.setType(PacketType.MESSAGES).setVersion(NettyUtils.VERSION);
            byte[] body = packetBuilder.setBody(messageBuilder.build().toByteString()).build().toByteArray();
            NettyUtils.write(ctx.getChannel(), body, ...);
        }
    } else {
        byte[] errorBytes = NettyUtils.errorPacket(401, "destination or clientId is null. Get: " + get);
        NettyUtils.write(ctx.getChannel(), errorBytes, ...);
    }
    break;
```

这里的 **raw 模式高性能序列化**值得细品。常规做法是用 `Messages.newBuilder().addAllMessages(...).build().toByteString()`，这会经历"Builder 累积 → 计算大小 → 分配缓冲 → 序列化"多步，且 Builder 内部有防御性拷贝。而 raw 模式直接：

1. **手工计算**整个 Packet 的精确字节大小（`computeInt64Size`/`computeBytesSizeNoTag`/`computeEnumSize`/`computeTagSize`/`computeRawVarint32Size`）；
2. **一次性分配** `byte[size]`；
3. **手写** Protobuf wire format：先写 Packet 的 type 字段（field 3, enum）、再写 body 的 tag（field 5, length-delimited）+ body 长度 + body 内容（Messages 的 batch_id 和每个 entry 字节）。

关键收益：`output.writeBytes(2, rowEntry)` 里的 `rowEntry` 是**从 Store 里原样取出的、已经序列化好的 Entry 字节**（`Event::getRawEntry`）。整条路径上，Entry **从未被反序列化再序列化**——它在 Parser 阶段序列化一次后，一路以字节形式流经 Store、Message，直到写回 TCP。这就是 issue #726 优化的服务端一侧：**zero re-serialization**。对于高吞吐场景，这能省下大量 CPU 和临时对象（减少 GC）。

**CLIENTACK 处理**

```java
case CLIENTACK:
    ClientAck ack = CanalPacket.ClientAck.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(ack.getDestination()) && StringUtils.isNotEmpty(ack.getClientId())) {
        MDC.put("destination", ack.getDestination());
        if (ack.getBatchId() == 0L) {
            byte[] errorBytes = NettyUtils.errorPacket(402, "batchId should assign value. Ack: " + ack);
            NettyUtils.write(ctx.getChannel(), errorBytes, ...);       // batchId=0 非法
        } else if (ack.getBatchId() == -1L) {
            // -1 代表上一次 get 没有数据，直接忽略
        } else {
            ClientIdentity clientIdentity = new ClientIdentity(ack.getDestination(),
                Short.parseShort(ack.getClientId()));
            embeddedServer.ack(clientIdentity, ack.getBatchId());     // 核心：确认
            // 只做 metrics 聚合，不回写 ACK 包（与客户端 fire-and-forget 对应）
            new ChannelFutureAggregator(ack.getDestination(), ack, packet.getType(), 0,
                System.nanoTime() - start).operationComplete(null);
        }
    } else {
        ... errorPacket(401, ...)
    }
    break;
```

注意 ack 正常路径下**不回写任何响应包**（只有非法 batchId 才回 error）。这与客户端 `ack()` 的"只写不读"严丝合缝——两边都不等对方。`batchId == -1` 是"上次 get 空批次"的哨兵值，直接忽略；`batchId == 0` 是非法的（0 是 protobuf int64 的默认值，无法区分"未设置"），返回 402。

**CLIENTROLLBACK 处理**

```java
case CLIENTROLLBACK:
    ClientRollback rollback = CanalPacket.ClientRollback.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(rollback.getDestination()) && StringUtils.isNotEmpty(rollback.getClientId())) {
        ClientIdentity clientIdentity = new ClientIdentity(rollback.getDestination(),
            Short.parseShort(rollback.getClientId()));
        MDC.put("destination", rollback.getDestination());
        if (rollback.getBatchId() == 0L) {
            embeddedServer.rollback(clientIdentity);            // 0：回滚所有批次
        } else {
            embeddedServer.rollback(clientIdentity, rollback.getBatchId());  // 回滚单个批次
        }
        // 同样只做 metrics，不回写
        new ChannelFutureAggregator(rollback.getDestination(), rollback, packet.getType(), 0,
            System.nanoTime() - start).operationComplete(null);
    } else {
        ... errorPacket(401, ...)
    }
    break;
```

`batchId == 0` → 全部回滚（对应客户端无参 `rollback()`）；否则回滚指定批次。同样 fire-and-forget。

**convertTimeUnit —— 协议整数还原为 TimeUnit**

```java
private TimeUnit convertTimeUnit(int unit) {
    switch (unit) {
        case 0: return TimeUnit.NANOSECONDS;
        case 1: return TimeUnit.MICROSECONDS;
        case 2: return TimeUnit.MILLISECONDS;
        case 3: return TimeUnit.SECONDS;
        case 4: return TimeUnit.MINUTES;
        case 5: return TimeUnit.HOURS;
        case 6: return TimeUnit.DAYS;
        default: return TimeUnit.MILLISECONDS;
    }
}
```

这与客户端 `getWithoutAck` 里的 `unit.ordinal()` 正好互为逆映射（`TimeUnit` 枚举的声明顺序就是 NANOSECONDS…DAYS）。

---

## 第九章：CanalServerWithEmbedded 数据服务

服务端 Netty 层只是"翻译官"，真正的数据逻辑全在 `CanalServerWithEmbedded`。它持有 `Map<String, CanalInstance> canalInstances`，每个方法都先根据 destination 找到对应 instance，再操作其 MetaManager 和 EventStore。

理解这一章的关键，是记住三个概念的关系（第 05 篇已详述，这里复习）：

- **cursor（消费位点）**：MetaManager 记录的"这个客户端已经确认消费到哪个 Binlog 位置"。ack 时前移。
- **batch（未确认批次）**：每次 get 会生成一个 batchId，记录"这批数据的位置范围"。ack 时移除，rollback 时清除。
- **EventStore（环形缓冲区）**：真正存数据的地方，有 put/get/ack 三个指针（第 04 篇）。

### 9.1 subscribe() 完整流程

```java
@Override
public void subscribe(ClientIdentity clientIdentity) throws CanalServerException {
    checkStart(clientIdentity.getDestination());

    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
    if (!canalInstance.getMetaManager().isStart()) {
        canalInstance.getMetaManager().start();
    }
    canalInstance.getMetaManager().subscribe(clientIdentity);        // ① meta 层登记订阅

    Position position = canalInstance.getMetaManager().getCursor(clientIdentity);
    if (position == null) {                                          // ② 没有历史 cursor
        position = canalInstance.getEventStore().getFirstPosition(); // 取 store 里最老一条
        if (position != null) {
            canalInstance.getMetaManager().updateCursor(clientIdentity, position); // 初始化 cursor
        }
        logger.info("subscribe successfully, {} with first position:{} ", clientIdentity, position);
    } else {
        logger.info("subscribe successfully, {} use last cursor position:{} ", clientIdentity, position);
    }

    canalInstance.subscribeChange(clientIdentity);                   // ③ 通知订阅关系变化（可能触发 filter 更新）
}
```

三步：

1. **meta 登记**：MetaManager 记录"这个 clientIdentity 订阅了"。
2. **cursor 初始化**：如果这个客户端从没消费过（cursor 为 null），就把消费起点设为 Store 里现存最老的数据（`getFirstPosition`）。这解释了接口 JavaDoc 里"第一次 fetch 从 canal 保存的最老一条数据开始"。如果有历史 cursor（重连场景），就沿用它——断点续传。
3. **通知变化**：`subscribeChange` 让 instance 感知订阅（比如 filter 变了需要通知 Parser）。

### 9.2 getWithoutAck() 完整流程

```java
@Override
public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                        throws CanalServerException {
    checkStart(clientIdentity.getDestination());
    checkSubscribe(clientIdentity);                        // 必须先订阅

    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
    synchronized (canalInstance) {                         // ★ 对 instance 加锁，保证 meta 与数据的顺序一致性
        PositionRange<LogPosition> positionRanges =
            canalInstance.getMetaManager().getLastestBatch(clientIdentity);  // 最近一批的位置

        Events<Event> events = null;
        if (positionRanges != null) {
            // 存在未处理完的流式批次：从上一批的起点重新取（幂等重发）
            events = getEvents(canalInstance.getEventStore(), positionRanges.getStart(), batchSize, timeout, unit);
        } else {
            // ack 后的第一次获取：从 cursor 取
            Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
            if (start == null) {
                start = canalInstance.getEventStore().getFirstPosition();  // 还没 ack 过，取最老
            }
            events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);
        }

        if (CollectionUtils.isEmpty(events.getEvents())) {
            return new Message(-1, true, new ArrayList()); // 空批次：batchId=-1，不生成新 batch
        } else {
            Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange()); // 生成 batchId
            boolean raw = isRaw(canalInstance.getEventStore());
            List entrys;
            if (raw) {
                entrys = events.getEvents().stream().map(Event::getRawEntry).collect(Collectors.toList());
            } else {
                entrys = events.getEvents().stream().map(Event::getEntry).collect(Collectors.toList());
            }
            return new Message(batchId, raw, entrys);
        }
    }
}
```

几个精髓：

- **`synchronized (canalInstance)`**：注释解释得很清楚——"meta 获取和数据的获取需要保证顺序性，优先拿到 meta 的一定也优先拿到数据"。如果不加锁，两个 get 并发可能出现"线程 A 拿到 meta 位置、线程 B 抢先拿走了数据"，导致顺序错乱。加锁保证了同一个 instance 的 get 严格串行。
- **两种起点**：如果 `getLastestBatch` 非空（上一批还没 ack/rollback），就从上一批**起点**重取——这实现了"没 ack 的批次会被重复投递"的幂等语义。如果为空（已 ack 或首次），从 cursor 取；cursor 也为空则从最老数据取。
- **空批次返回 -1**：没数据时返回 `Message(-1, ...)`，不调用 `addBatch`，避免"生成一个空 batchId 浪费性能"。客户端 `receiveMessages` 拿到 batchId=-1 就知道没数据。
- **raw 决定取字节还是取 Entry**：`isRaw` 判断 EventStore 是否是 raw 模式（`MemoryEventStoreWithBuffer.isRaw()`）。raw 时取 `getRawEntry()`（字节），非 raw 取 `getEntry()`（对象）。这与前面服务端 GET 序列化的 raw 分支呼应。
- **注意**：与 `get()` 不同，`getWithoutAck` 里**没有**在返回前调 `ack`。而 `get()` 里有一行 `ack(clientIdentity, batchId)`——这就是自动确认与手动确认在服务端的唯一区别。

### 9.3 ack() 完整流程

```java
@Override
public void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException {
    checkStart(clientIdentity.getDestination());
    checkSubscribe(clientIdentity);

    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
    PositionRange<LogPosition> positionRanges;
    positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity, batchId); // ① 移除该批次
    if (positionRanges == null) {
        throw new CanalServerException(String.format(
            "ack error , clientId:%s batchId:%d is not exist , please check",
            clientIdentity.getClientId(), batchId));   // 重复 ack 或非法 batchId
    }

    if (positionRanges.getAck() != null) {
        canalInstance.getMetaManager().updateCursor(clientIdentity, positionRanges.getAck()); // ② 前移 cursor
        logger.info("ack successfully, clientId:{} batchId:{} position:{}",
            clientIdentity.getClientId(), batchId, positionRanges);
    }

    canalInstance.getEventStore().ack(positionRanges.getEnd(), positionRanges.getEndSeq()); // ③ 释放 store 空间
}
```

三步，对应"确认"的完整语义：

1. **removeBatch**：从 MetaManager 移除这个 batchId 的记录。返回它的位置范围。如果返回 null，说明这个 batch 不存在（重复 ack、或 rollback 过了），抛异常。
2. **updateCursor**：把消费位点 cursor 前移到这批的**结束位置**（`getAck()`）。cursor 前移意味着"这批之前的数据都算确认了"——这也是接口 JavaDoc 说"小于等于此 batchId 的 Message 都会被确认"的实现。
3. **eventStore.ack**：通知环形缓冲区，这批数据占的空间可以回收了（ack 指针前移，腾出空间给 Parser 继续写入）。

### 9.4 rollback() 完整流程

**全量回滚**：

```java
@Override
public void rollback(ClientIdentity clientIdentity) throws CanalServerException {
    checkStart(clientIdentity.getDestination());
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
    // 首次连接会自动 rollback，此时可能还没订阅，忽略之
    boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
    if (!hasSubscribe) {
        return;
    }
    synchronized (canalInstance) {
        canalInstance.getMetaManager().clearAllBatchs(clientIdentity);  // ① 清除所有未 ack 批次
        canalInstance.getEventStore().rollback();                       // ② store get 指针回退到 ack 指针
        logger.info("rollback successfully, clientId:{}", clientIdentity.getClientId());
    }
}
```

**单批次回滚**：

```java
@Override
public void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException {
    checkStart(clientIdentity.getDestination());
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
    boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
    if (!hasSubscribe) return;
    synchronized (canalInstance) {
        PositionRange<LogPosition> positionRanges =
            canalInstance.getMetaManager().removeBatch(clientIdentity, batchId);  // 移除指定批次
        if (positionRanges == null) {
            throw new CanalServerException(String.format(
                "rollback error, clientId:%s batchId:%d is not exist , please check",
                clientIdentity.getClientId(), batchId));
        }
        canalInstance.getEventStore().rollback();  // store 状态回退
        logger.info("rollback successfully, clientId:{} batchId:{} position:{}",
            clientIdentity.getClientId(), batchId, positionRanges);
    }
}
```

对比：

- 全量 rollback 调 `clearAllBatchs`（清空该客户端所有未 ack 批次），单批次调 `removeBatch`（只移一个）。
- 二者都调 `eventStore.rollback()`，把 Store 的 get 指针回退到 ack 指针位置——这样下次 get 会从"最后一次 ack 之后"重新读，重发未确认的数据。
- **`hasSubscribe` 判断很关键**：`rollbackOnConnect=true` 导致 connect 时会自动 rollback，但那一刻客户端可能还没发 SUBSCRIPTION 包（回顾 `SimpleCanalConnector.connect`：先 doConnect，再 subscribe，最后 rollback——实际顺序是 doConnect→subscribe→rollback，但认证阶段的顺带订阅路径可能让顺序不同）。用 `hasSubscribe` 兜底，未订阅时直接返回，避免异常。

### 9.5 get() 的自动 ack

最后看一眼 `get()`（自动确认版）与 `getWithoutAck` 的差异，就在返回前多了一行：

```java
@Override
public Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                        throws CanalServerException {
    checkStart(clientIdentity.getDestination());
    checkSubscribe(clientIdentity);
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
    synchronized (canalInstance) {
        PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLastestBatch(clientIdentity);
        if (positionRanges != null) {
            // get 模式下，如果上一批还没 ack，直接报错（可能丢数据）
            throw new CanalServerException(String.format(
                "clientId:%s has last batch:[%s] isn't ack , maybe loss data",
                clientIdentity.getClientId(), positionRanges));
        }
        ...
        Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
        ...
        ack(clientIdentity, batchId);   // ★ 拉完立即 ack
        return new Message(batchId, raw, entrys);
    }
}
```

注意 `get` 与 `getWithoutAck` 在"存在未 ack 批次"时的处理截然不同：

- `getWithoutAck`：存在未 ack 批次时，**重发**这批（幂等，配合 rollback 使用）。
- `get`：存在未 ack 批次时，**直接抛异常** "maybe loss data"。因为 get 是自动 ack 的，正常情况下不该有未 ack 批次；出现了说明状态异常，宁可报错也不冒险。

这再次印证：`get`（at-most-once）追求简单直接，`getWithoutAck`（at-least-once）追求不丢数据。

---

## 第十章：客户端最佳实践示例

Canal 官方在 example 模块给出了标准消费范式。核心在 `AbstractCanalClientTest.process()`：

```java
protected void process() {
    int batchSize = 5 * 1024;                    // 一批最多 5120 条
    while (running) {
        try {
            MDC.put("destination", destination);
            connector.connect();                 // ① 建连（握手+认证）
            connector.subscribe();               // ② 订阅（空 filter，用服务端配置）
            while (running) {
                Message message = connector.getWithoutAck(batchSize);  // ③ 拉取但不确认
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    // 空批次：没数据，继续下一轮（此处可 sleep 降频）
                } else {
                    printSummary(message, batchId, size);
                    printEntry(message.getEntries());  // ④ 业务处理
                }
                if (batchId != -1) {
                    connector.ack(batchId);      // ⑤ 处理成功后确认
                }
            }
        } catch (Throwable e) {
            logger.error("process error!", e);
            try { Thread.sleep(1000L); } catch (InterruptedException e1) { }
            connector.rollback();                // ⑥ 出错回滚，下次重拉
        } finally {
            connector.disconnect();              // ⑦ 断连
            MDC.remove("destination");
        }
    }
}
```

这段代码是 Canal 消费的"标准姿势"，蕴含几个最佳实践：

1. **双层循环**：外层 `while(running)` 负责"连接生命周期 + 异常恢复"，内层 `while(running)` 负责"持续拉取"。任何异常都会跳出内层，进入 catch 做 rollback，然后外层 finally 断连、下一轮外层重连。这是一个自愈的循环。
2. **getWithoutAck + ack/rollback**：严格遵循 at-least-once。拉取后处理，成功 ack、失败 rollback。
3. **空批次处理**：`batchId == -1` 表示没数据，此时应该 sleep 一小段时间再拉（示例里注释掉了 sleep，实际生产建议加上，避免空转打满 CPU）。注意空批次**不 ack**（`if (batchId != -1)` 才 ack）。
4. **异常兜底 rollback**：任何异常（业务处理失败、网络断）都触发 `rollback()`，把未 ack 的批次退回，保证下次重拉不丢。
5. **finally 断连**：即使正常也要在退出时 disconnect，释放服务端资源。

单机模式的入口（`SimpleCanalClientTest`）：

```java
public static void main(String args[]) {
    String destination = "example";
    String ip = AddressUtils.getHostIp();
    CanalConnector connector = CanalConnectors
        .newSingleConnector(new InetSocketAddress(ip, 11111), destination, "canal", "canal");

    final SimpleCanalClientTest clientTest = new SimpleCanalClientTest(destination);
    clientTest.setConnector(connector);
    clientTest.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
            logger.info("## stop the canal client");
            clientTest.stop();
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping canal:", e);
        } finally {
            logger.info("## canal client is down.");
        }
    }));
}
```

要点：

- **端口 11111**：Canal server 的默认 TCP 端口。
- **用户名/密码 "canal"/"canal"**：与 server 端 `canal.user`/`canal.passwd` 配置对应，触发第五章的 scramble 认证。
- **ShutdownHook**：JVM 退出时优雅停机，调 `disconnect()` 释放连接。

集群模式只需换一行创建方式：

```java
// 用 ZK 自动发现 server 列表 + HA failover
CanalConnector connector = CanalConnectors.newClusterConnector(
    "10.20.30.1:2181,10.20.30.2:2181", destination, "canal", "canal");
```

业务消费代码（`process()` 那套双层循环）完全不变——这就是接口抽象的价值：单机/集群对业务透明。

---

## 第十一章：关键设计总结

通读整条链路，Canal 客户端订阅协议与 Get/Ack/Rollback 全流程体现了以下几个核心设计：

### 11.1 分离 get 与 ack —— at-least-once 的基石

最核心的设计决策是把"拉取"和"确认"拆成两个独立操作（`getWithoutAck` + `ack`/`rollback`）。这让消费者可以在"数据落地成功"之后才 ack，从而保证 at-least-once：任何时刻进程崩溃，未 ack 的数据都会在重连后被重发。代价是消费者必须自己处理"重复投递"（幂等），但对大多数 CDC 场景这是可接受的权衡——宁可重复，不可丢失。

而 `get`（自动 ack）作为 at-most-once 的便捷入口保留，适用于"丢一点无所谓"的场景，且在检测到未 ack 批次时直接报错，绝不冒进。

### 11.2 batchId 作为消费进度的唯一凭证

服务端不为客户端维护复杂的会话状态，只用一个单调递增的 `batchId` 串联 get/ack/rollback：

- get 生成 batchId，记录该批次的 Binlog 位置范围（addBatch）。
- ack(batchId) 移除该批次、前移 cursor、释放 Store。
- rollback 清除批次、回退 Store get 指针。

这种"轻状态"设计让服务端易于水平扩展和故障恢复——所有进度都落在 MetaManager（可持久化到 ZK/File），server 重启后从 cursor 恢复即可。

### 11.3 raw 模式的 zero re-serialization

Entry 在 Parser 阶段序列化为字节后，一路以 `ByteString` 形式流经 Store → Message → TCP，中途从不反序列化再序列化。服务端 GET 响应甚至手写 `CodedOutputStream` 拼装 Protobuf wire format，绕开 Builder 的防御性拷贝。客户端则通过 `lazyParseEntry` 把反序列化延迟到业务真正访问时。这套"字节直通"设计（issue #726）在高吞吐场景显著降低 CPU 和 GC 压力。

### 11.4 scramble 认证 —— 密码不上网

借鉴 MySQL 的 scramble411 挑战应答机制：server 每连接发随机 seed，client 用 `SHA1` 多轮混淆 + XOR 生成 token，明文密码永不在网络传输，且每次 seed 不同可抗重放。server 侧用 `scrambleServerAuth` 逆向校验。这是一个轻量但有效的认证方案。

### 11.5 双 ZK 抢占 —— server HA 与 client 串行化

集群模式下有两套独立的 ZK 抢占：

- **server 侧**（`running` 节点）：多个 canal server 抢同一 destination 的工作权，保证同一 destination 只有一个 server 在解析 Binlog（避免重复解析、位点冲突）。客户端通过 `ClusterNodeAccessStrategy` 监听 running 节点，自动追踪 active server。
- **client 侧**（`{clientId}/running` 节点）：多个消费客户端抢同一 clientId 的消费权，保证同一消费者身份同一时刻只有一个实例在拉数据（避免重复消费、乱序）。由 `ClientRunningMonitor` 实现，备份客户端阻塞在 `mutex.get()`。

两套机制叠加，让 Canal 在 server 和 client 两端都具备 HA 能力，且对业务代码透明。

### 11.6 动态 Pipeline 重构 —— 用完即弃

服务端 Netty Pipeline 在认证完成后动态移除握手/认证 Handler，换上空闲检测 Handler。这体现了"阶段性 Handler 用完即弃"的思想：一次性的握手认证逻辑不该常驻内存、也不该被后续业务包错误命中。重构后 Pipeline 精简，长连接只保留会话处理和空闲检测。

### 11.7 fire-and-forget 的 ack/rollback

`ack` 和 `rollback` 在客户端"只写不读"、在服务端"只处理不回包"（正常路径）。这种单向设计降低了网络往返，提升吞吐——毕竟确认/回滚是高频操作，若每次都等 ACK 会拖慢消费。代价是客户端无法立即感知 ack 是否成功，但由 batchId 的幂等语义兜底（重复 ack 会被服务端识别为"batch not exist"并报错，从而暴露问题）。

---

## 第十二章：常见问题答疑（源码视角）

结合前面拆解的源码，这里集中回答几个使用 Canal 客户端时高频遇到、且答案就藏在源码细节里的问题。

### 12.1 为什么第一次消费拿不到"历史全量"数据？

因为 `subscribe()` 里 cursor 初始化用的是 `eventStore.getFirstPosition()`——Store 环形缓冲区里**当前存在的最老一条**，而不是 MySQL Binlog 的最开始。Store 是有容量上限的内存缓冲区（第 04 篇），早于它的历史变更早已被覆盖/丢弃。所以 Canal 客户端第一次连上只能拿到"从现在往前、Store 里还留着的那部分增量"，不是全量。要做全量初始化，需要业务方自己先 dump 一次表快照，再从 Canal 接增量。

### 12.2 为什么 batchId=-1 时不能 ack？

服务端 `SessionHandler` 对 CLIENTACK 有 `ack.getBatchId() == -1L` 的分支——直接忽略。因为 batchId=-1 是"空批次"的哨兵值（`getWithoutAck` 无数据时返回 `Message(-1, ...)`，且没有调 `addBatch`）。既然没生成真实批次，ack 它就是无意义的。示例代码里 `if (batchId != -1) connector.ack(batchId)` 正是遵循这一约定。而 batchId=0 更特殊——它是 protobuf int64 的默认值，服务端无法区分"未设置"和"真的是 0"，所以 ack 时 0 被判为 402 非法，rollback 时 0 被赋予"回滚全部"的语义。

### 12.3 多个客户端用相同 clientId 会怎样？

在**单机模式**下（`newSingleConnector`，无 ZK），没有 `ClientRunningMonitor`，多个相同 clientId 的客户端会**同时**向服务端拉数据。由于服务端 `getWithoutAck` 里 `synchronized(canalInstance)` + "未 ack 批次报错/重发"的逻辑，两个客户端会互相干扰：一个 get 生成了 batch，另一个 get 发现 `getLastestBatch != null`（在 `get` 自动 ack 模式下）直接抛 "maybe loss data"，或（在 `getWithoutAck` 模式下）重复拿到同一批。结果是消费混乱。

在**集群模式**下（`newClusterConnector(zkServers,...)`），`ClientRunningMonitor` 通过 ZK 临时节点抢占，保证同一 clientId 同一时刻只有一个客户端在消费，另一个阻塞在 `mutex.get()` 做热备。这才是"部署多个消费实例做高可用"的正确姿势。

### 12.4 filter 修改后为什么要重新 subscribe？

`subscribe(filter)` 会把 filter 一路传到服务端 `embeddedServer.subscribe` → `subscribeChange`，通知 instance 更新过滤规则。客户端侧 `SimpleCanalConnector.subscribe` 还会 `clientIdentity.setFilter(filter)` 并被 `ClusterCanalConnector` 记进 `this.filter`——后者的意义在于**自动重连时能重放上次的 filter**（回顾 `connect()` 里 `if (filter != null) subscribe(filter)`）。所以修改 filter 必须通过重新 subscribe 生效，直接改客户端字段不会同步到服务端。

### 12.5 idleTimeout 和 soTimeout 有什么区别？

- **`soTimeout`（默认 60 秒）**：客户端 socket 的**读超时**。`channel.socket().setSoTimeout(soTimeout)`。它控制单次阻塞读最多等多久。若一次 `getWithoutAck(batchSize, 0, ...)` 用了阻塞语义（timeout=0）且长时间无数据，读操作会受 soTimeout 约束。
- **`idleTimeout`（默认 1 小时）**：客户端在 `ClientAuth` 里声明的 `netReadTimeout/netWriteTimeout`，传给服务端。服务端据此装配 `IdleStateHandler`——**连接空闲超过这个时间，服务端主动关连接**以回收资源。它是"连接级"的保活/回收阈值，比 soTimeout 大得多。

二者配合：soTimeout 防止单次读永久阻塞，idleTimeout 防止空闲连接长期占用服务端资源。

### 12.6 为什么 ack 之后再 rollback 会报错？

因为 ack 里调了 `metaManager.removeBatch(clientIdentity, batchId)`，把这个 batch 从元数据里移除了。之后 rollback 同一个 batchId 时，`removeBatch` 返回 null，服务端抛 "rollback error, batchId is not exist"。同理，重复 ack 也会因 `removeBatch` 返回 null 抛 "ack error, batchId is not exist"。这套"移除即消费"的设计天然防止了重复 ack/rollback 造成的状态错乱——batchId 是"一次性消费"的凭证。

### 12.7 raw 模式（lazyParseEntry）什么时候该开？

`lazyParseEntry=true`（默认 false）时，客户端拿到 Message 只存原始字节，不解析成 Entry。适合：

- **纯转发场景**：把 Canal 数据原样投递到下游 MQ，不需要在客户端解析字段。
- **高吞吐 + 部分解析**：一批几千条，业务只关心特定表/特定操作，可先按需解析，跳过不关心的。

不适合：客户端需要立即访问所有 Entry 字段的场景（此时懒解析只是把成本推后，还多了一层 `getRawEntries()` 手动解析的心智负担）。开启后要记得业务代码里改用 `message.getRawEntries()` + `Entry.parseFrom(byteString)`，而不是 `message.getEntries()`（后者在 raw 模式下是空的）。

### 12.8 服务端的两个 Server 类到底怎么配合？

- `CanalServerWithEmbedded`：进程内单例（`SingletonHolder`），持有所有 CanalInstance，实现全部业务逻辑。可被直接嵌入业务进程使用（零网络）。
- `CanalServerWithNetty`：网络门面，内部持有一个 `CanalServerWithEmbedded`，把 TCP 请求翻译成对 embedded 的方法调用。本文的 SessionHandler/认证 Handler 都是它 Pipeline 里的处理器，最终都调 `embeddedServer.xxx()`。

一句话：Netty 负责"网络 IO + 协议编解码"，Embedded 负责"数据逻辑"，职责清晰分离。

---

## 附录：完整交互时序（文字版）

```
客户端                                          服务端(Netty + Embedded)
  │                                                │
  │──── TCP connect ──────────────────────────────▶│  channelOpen
  │                                                │  生成 8字节 seed
  │◀──── HANDSHAKE{seeds, supportedCompressions} ──│  推送握手包，seed 交给认证Handler
  │                                                │
  │  scramble411(password, seed)                   │
  │──── CLIENTAUTHENTICATION{user, token, ...} ───▶│  ClientAuthenticationHandler
  │                                                │  auth() 校验(scrambleServerAuth)
  │◀──── ACK{errorCode=0} ─────────────────────────│  认证成功，动态重构Pipeline
  │                                                │  (移除握手/认证Handler，加空闲检测)
  │                                                │
  │──── SUBSCRIPTION{dest, clientId, filter} ─────▶│  SessionHandler
  │                                                │  embeddedServer.subscribe()
  │                                                │  (meta登记 + cursor初始化 + lazy启动instance)
  │◀──── ACK ──────────────────────────────────────│
  │                                                │
  │──── CLIENTROLLBACK{batchId=0} ────────────────▶│  rollback(全部)  (rollbackOnConnect)
  │       (fire-and-forget, 无响应)                 │
  │                                                │
  │ ┌──────────── 消费循环 ────────────┐            │
  │ │                                 │            │
  │──── GET{fetchSize, timeout, autoAck=false} ───▶│  getWithoutAck()
  │                                                │  synchronized(instance)
  │                                                │  addBatch → 生成batchId
  │◀──── MESSAGES{batchId, [rawEntries]} ──────────│  (raw模式手写CodedOutputStream)
  │                                                │
  │  CanalMessageDeserializer.deserializer()       │
  │  业务处理...                                     │
  │                                                │
  │──── CLIENTACK{batchId} ───────────────────────▶│  ack()
  │       (fire-and-forget, 无响应)                 │  removeBatch + updateCursor + store.ack
  │ │                                 │            │
  │ └─────────────────────────────────┘            │
  │        (若处理失败)                              │
  │──── CLIENTROLLBACK{batchId} ──────────────────▶│  rollback(单批次)
  │                                                │  removeBatch + store.rollback
  │                                                │
  │──── TCP disconnect ───────────────────────────▶│  channelClosed
  │                                                │
```

至此，客户端订阅协议与 Get/Ack/Rollback 的完整链路——从 TCP 建连、握手认证、订阅、循环消费、确认/回滚，到集群 HA failover——已全部拆解完毕。理解这条链路，就理解了 Canal 作为一个"可靠的 Binlog 订阅分发系统"最核心的对外契约。
