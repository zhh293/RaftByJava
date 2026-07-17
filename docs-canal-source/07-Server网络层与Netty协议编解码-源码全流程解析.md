# Server网络层与Netty协议编解码 —— 源码全流程解析

> 基于源码项目 `canal` 的 server 模块，从 CanalServer 接口定义到 Netty Pipeline 构建、Handshake/Auth/Session 三阶段协议交互、Protobuf 编解码、性能监控采集、MQ 投递模式，逐行拆解，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的 Server 网络层调用链路图，后面逐步展开每一层：

```
客户端TCP连接
  |
  +-- 1. CanalServerWithNetty.start()
  |     -> 启动 CanalServerWithEmbedded（如果未启动）
  |     -> 创建 ServerBootstrap（Netty 3.x）
  |     -> 配置 NioServerSocketChannelFactory（Boss/Worker 线程池）
  |     -> 设置 TCP 选项：keepAlive、tcpNoDelay
  |     -> 构建 Pipeline：
  |     |   +-- FixedHeaderFrameDecoder     -- 帧解码器
  |     |   +-- HandshakeInitializationHandler -- 握手处理器
  |     |   +-- ClientAuthenticationHandler -- 认证处理器
  |     |   +-- SessionHandler             -- 业务会话处理器
  |     -> bind(ip:port)
  |
  +-- 2. 客户端连接建立（channelOpen）
  |     -> HandshakeInitializationHandler.channelOpen()
  |     |   -> 将 Channel 加入 ChannelGroup（用于统一管理）
  |     |   -> 生成 8 字节随机 seed
  |     |   -> 构建 Handshake Packet（Protobuf 序列化）
  |     |   -> NettyUtils.write() 发送给客户端
  |     |   -> ChannelFuture 回调：将 seed 传递给 ClientAuthenticationHandler
  |
  +-- 3. 客户端发送认证请求（messageReceived）
  |     -> FixedHeaderFrameDecoder.decode()
  |     |   -> 读取 4 字节长度 + 对应长度的 Body
  |     -> ClientAuthenticationHandler.messageReceived()
  |     |   -> 解析 ClientAuth Packet
  |     |   -> 校验 seed 非空
  |     |   -> 调用 embeddedServer.auth() 校验用户名密码
  |     |   -> [可选] 如果 ClientAuth 携带 destination/clientId：
  |     |   |   -> embeddedServer.subscribe(clientIdentity)
  |     |   |   -> 启动 ServerRunningMonitor（如果未启动）
  |     |   -> 发送 ACK 响应
  |     |   -> ChannelFuture 回调：动态重构 Pipeline
  |     |       -> 移除 HandshakeInitializationHandler
  |     |       -> 移除 ClientAuthenticationHandler
  |     |       -> 添加 IdleStateHandler（超时检测）
  |     |       -> 添加 IdleStateAwareChannelHandler（超时关闭连接）
  |
  +-- 4. 客户端发送业务请求（messageReceived）
  |     -> SessionHandler.messageReceived()
  |     |   +-- SUBSCRIPTION：订阅 destination
  |     |   +-- UNSUBSCRIPTION：取消订阅
  |     |   +-- GET：拉取变更数据（高性能序列化）
  |     |   +-- CLIENTACK：确认消费
  |     |   +-- CLIENTROLLBACK：回滚消费
  |     |   +-- default：返回 400 错误
  |
  +-- 5. 连接断开
        -> CanalServerWithNetty.stop()
        |   -> 关闭 serverChannel
        |   -> 关闭所有 childGroups
        |   -> 释放 Bootstrap 资源
        |   -> 停止 embeddedServer
```

---

## 一、Server模块整体架构

### 1.1 两大核心接口

Canal Server 模块的顶层设计围绕两个接口展开：`CanalServer` 和 `CanalService`。

#### CanalServer 接口

```java
// 文件：com.alibaba.otter.canal.server.CanalServer
package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * 对应canal整个服务实例，一个jvm实例只有一份server
 */
public interface CanalServer extends CanalLifeCycle {

    void start() throws CanalServerException;

    void stop() throws CanalServerException;
}
```

**逐行解读：**

| 行 | 说明 |
|---|------|
| `extends CanalLifeCycle` | 继承生命周期接口，提供 `isStart()` 等方法。Canal 中几乎所有核心组件都实现此接口 |
| `void start()` | 启动服务。不同实现（Embedded / Netty）的启动逻辑完全不同 |
| `void stop()` | 停止服务。需要释放网络资源、停止 Instance 等 |

**设计意图**：`CanalServer` 是一个极简的生命周期管理接口，它不关心数据操作。数据操作由 `CanalService` 定义。

---

#### CanalService 接口

```java
// 文件：com.alibaba.otter.canal.server.CanalService
package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

public interface CanalService {

    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                      throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                      throws CanalServerException;

    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
```

**逐行解读：**

| 方法 | 说明 |
|------|------|
| `subscribe` | 客户端订阅指定 destination。会在 MetaManager 中记录订阅关系 |
| `unsubscribe` | 取消订阅。如果是最后一个订阅者，可能会触发 Instance 释放 |
| `get` | 获取数据并自动 ACK（获取即确认） |
| `getWithoutAck` | 获取数据但不自动 ACK（需要客户端手动确认或回滚） |
| `ack` | 确认消费，推进消费位点 |
| `rollback` | 回滚消费，数据会被重新投递 |

**核心设计**：`CanalService` 定义了 Canal 客户端交互的完整数据操作语义。注意 `get` 和 `getWithoutAck` 两种模式的区别 —— 生产环境几乎都使用 `getWithoutAck`，因为它允许消费失败后重试。

---

### 1.2 双Server设计模式

Canal 的 Server 层采用了一种经典的 **分层委托模式**，核心是两个实现类的职责分工：

```
                    ┌─────────────────────────────┐
                    │        CanalServer          │
                    │       (接口定义)              │
                    └──────────┬──────────────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                  │
   ┌──────────▼──────────┐         ┌─────────────▼────────────┐
   │ CanalServerWithNetty │         │ CanalServerWithEmbedded  │
   │                      │         │                          │
   │  职责：网络壳          │ ──────> │  职责：数据服务核心         │
   │  - 监听TCP端口        │  委托    │  - Instance管理           │
   │  - 协议编解码          │         │  - subscribe/get/ack     │
   │  - 连接管理            │         │  - 数据操作               │
   │  - 认证握手            │         │  - 认证校验               │
   └──────────────────────┘         └──────────────────────────┘
```

**关键设计点：**

1. **CanalServerWithEmbedded** 同时实现了 `CanalServer` 和 `CanalService`，它是真正的数据服务核心。所有的 subscribe、get、ack、rollback 操作都由它执行。

2. **CanalServerWithNetty** 只实现了 `CanalServer`，它是一个纯粹的网络外壳。它的唯一职责是将 TCP 字节流解码为 Protobuf 消息，然后委托给 `CanalServerWithEmbedded` 处理。

3. **两者都是单例**，通过静态内部类持有者模式实现：

```java
// CanalServerWithEmbedded 的单例
private static class SingletonHolder {
    private static final CanalServerWithEmbedded CANAL_SERVER_WITH_EMBEDDED = new CanalServerWithEmbedded();
}

// CanalServerWithNetty 的单例
private static class SingletonHolder {
    private static final CanalServerWithNetty CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty();
}
```

4. **Netty 持有 Embedded 的引用**：

```java
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {
    private CanalServerWithEmbedded embeddedServer;  // 嵌入式server

    private CanalServerWithNetty(){
        this.embeddedServer = CanalServerWithEmbedded.instance();
        // ...
    }
}
```

**为什么这样设计？**

这种分层设计有三个重要好处：

- **可独立使用**：`CanalServerWithEmbedded` 可以不经过 Netty 直接在同一 JVM 内使用，适用于嵌入式场景
- **职责清晰**：网络协议与业务逻辑完全解耦。更换网络框架（比如从 Netty 换成 gRPC）只需要替换外壳，不影响数据层
- **MQ 模式复用**：MQ 模式（Kafka/RocketMQ）绕过 Netty 直接使用 Embedded Server，无需网络开销

---

### 1.3 两种运行模式的选择

Canal 有两种运行模式，决定了是否使用 Netty：

| 模式 | 说明 | 是否启动 Netty | 数据交付方式 |
|------|------|--------------|------------|
| **TCP 模式** | 默认模式 | 是 | 客户端通过 TCP 连接，使用 Canal 自定义协议拉取数据 |
| **MQ 模式** | serverMode=kafka/rocketMQ/rabbitMQ | 否 | Canal 主动推送数据到 MQ，客户端消费 MQ |

在 `CanalStarter.start()` 中的选择逻辑（简化）：

```
if (serverMode == tcp) {
    启动 CanalServerWithNetty   // 监听 TCP 端口
} else {
    启动 CanalMQStarter         // 启动 MQ 投递线程
    // 不启动 Netty
}
```

这两种模式最终都依赖 `CanalServerWithEmbedded` 来完成数据操作。

---

## 二、Netty 3.x 的使用方式

### 2.1 为什么 Canal 使用 Netty 3.x

Canal 的 Netty 网络层使用的是 **Netty 3.x**（`org.jboss.netty` 包），而不是当前主流的 Netty 4.x（`io.netty` 包）。这从 import 语句中可以明确看出：

```java
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
```

**历史原因分析：**

1. Canal 项目始于 2012 年（从源码注释 `@author jianghang 2012-7-12` 可以看出），当时 Netty 4.x 尚未发布稳定版
2. Canal 的网络层非常轻量（只有 4 个 Handler），升级到 Netty 4.x 的收益有限
3. Netty 3.x 和 4.x 的 API 差异巨大（包名、线程模型、Buffer API 都不同），迁移成本较高
4. Canal 的性能瓶颈不在网络层，而在 binlog 解析和 Store 读写

**Netty 3.x 与 4.x 的关键差异：**

| 特性 | Netty 3.x | Netty 4.x |
|------|-----------|-----------|
| 包名 | `org.jboss.netty` | `io.netty` |
| 线程模型 | Boss/Worker 由 `Executors` 手动创建 | `EventLoopGroup` 封装 |
| Buffer | `ChannelBuffer` | `ByteBuf`（引用计数） |
| Pipeline 创建 | `ChannelPipelineFactory` | `ChannelInitializer` |
| Handler 基类 | `SimpleChannelHandler` | `ChannelInboundHandlerAdapter` |
| 事件模型 | Upstream/Downstream | Inbound/Outbound |
| 内存管理 | 无引用计数 | 有引用计数（池化 Buffer） |

---

### 2.2 CanalServerWithNetty 完整源码解读

```java
// 文件：com.alibaba.otter.canal.server.netty.CanalServerWithNetty
package com.alibaba.otter.canal.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.handler.ClientAuthenticationHandler;
import com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder;
import com.alibaba.otter.canal.server.netty.handler.HandshakeInitializationHandler;
import com.alibaba.otter.canal.server.netty.handler.SessionHandler;

/**
 * 基于netty网络服务的server实现
 */
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {

    private CanalServerWithEmbedded embeddedServer;      // 嵌入式server
    private String                  ip;
    private int                     port;
    private Channel                 serverChannel = null;
    private ServerBootstrap         bootstrap     = null;
    private ChannelGroup            childGroups   = null; // socket channel container

    private static class SingletonHolder {
        private static final CanalServerWithNetty CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty();
    }

    private CanalServerWithNetty(){
        this.embeddedServer = CanalServerWithEmbedded.instance();
        this.childGroups = new DefaultChannelGroup();
    }

    public static CanalServerWithNetty instance() {
        return SingletonHolder.CANAL_SERVER_WITH_NETTY;
    }

    // ... (start/stop 方法见下文分析)
}
```

**字段逐个解读：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `embeddedServer` | `CanalServerWithEmbedded` | 核心数据服务引用，所有业务操作都委托给它 |
| `ip` | `String` | 绑定的 IP 地址，为空时绑定所有网卡 |
| `port` | `int` | 监听端口，默认 11111 |
| `serverChannel` | `Channel` | 服务端监听 Channel，用于关闭时释放 |
| `bootstrap` | `ServerBootstrap` | Netty 3.x 的服务端引导类 |
| `childGroups` | `ChannelGroup` | 所有客户端连接的集合，用于统一关闭 |

---

### 2.3 start() 方法：ServerBootstrap 配置与 Pipeline 构建

```java
public void start() {
    super.start();

    if (!embeddedServer.isStart()) {
        embeddedServer.start();
    }

    this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()));

    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setOption("child.tcpNoDelay", true);

    // 构造对应的pipeline
    bootstrap.setPipelineFactory(() -> {
        ChannelPipeline pipelines = Channels.pipeline();
        pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
        pipelines.addLast(HandshakeInitializationHandler.class.getName(),
            new HandshakeInitializationHandler(childGroups));
        pipelines.addLast(ClientAuthenticationHandler.class.getName(),
            new ClientAuthenticationHandler(embeddedServer));

        SessionHandler sessionHandler = new SessionHandler(embeddedServer);
        pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
        return pipelines;
    });

    // 启动
    if (StringUtils.isNotEmpty(ip)) {
        this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
    } else {
        this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
    }
}
```

**逐段深度解析：**

#### 第一段：确保 Embedded Server 已启动

```java
super.start();

if (!embeddedServer.isStart()) {
    embeddedServer.start();
}
```

- `super.start()` 调用 `AbstractCanalLifeCycle.start()`，设置 `running = true` 标志
- 确保 `CanalServerWithEmbedded` 已经启动。因为 Netty 是网络壳，所有业务操作都要委托给 Embedded Server
- 通常情况下，`CanalController.start()` 会先启动 Embedded Server，这里的检查是一种防御性编程

#### 第二段：创建 ServerBootstrap

```java
this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
    Executors.newCachedThreadPool(),    // Boss 线程池
    Executors.newCachedThreadPool()));  // Worker 线程池
```

- `NioServerSocketChannelFactory` 是 Netty 3.x 的 NIO Server 工厂
- **Boss 线程池**：负责接受新连接（accept），通常只需要 1 个线程
- **Worker 线程池**：负责已建立连接的 I/O 读写，线程数默认为 CPU 核数 * 2
- 使用 `Executors.newCachedThreadPool()` 创建无上限的线程池。在 Canal 场景下，客户端连接数通常很少（几个到几十个），所以不会有线程爆炸问题

> **对比 Netty 4.x**：在 4.x 中，等价代码为：
> ```java
> EventLoopGroup bossGroup = new NioEventLoopGroup(1);
> EventLoopGroup workerGroup = new NioEventLoopGroup();
> new ServerBootstrap().group(bossGroup, workerGroup)...
> ```

#### 第三段：TCP 选项配置

```java
bootstrap.setOption("child.keepAlive", true);
bootstrap.setOption("child.tcpNoDelay", true);
```

| 选项 | 值 | 说明 |
|------|----|------|
| `child.keepAlive` | `true` | 启用 TCP KeepAlive 机制。操作系统会定期发送探测包，检测死连接。参数依赖于 OS 配置：`tcp_keepalive_time=300`, `tcp_keepalive_probes=2`, `tcp_keepalive_intvl=30` |
| `child.tcpNoDelay` | `true` | 禁用 Nagle 算法。Nagle 会将小包合并发送以减少网络开销，但会增加延迟。Canal 需要低延迟，所以禁用 |

> `child.` 前缀表示这些选项应用于子连接（accepted connections），而不是 server socket 本身。

#### 第四段：Pipeline 构建

```java
bootstrap.setPipelineFactory(() -> {
    ChannelPipeline pipelines = Channels.pipeline();
    pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
    pipelines.addLast(HandshakeInitializationHandler.class.getName(),
        new HandshakeInitializationHandler(childGroups));
    pipelines.addLast(ClientAuthenticationHandler.class.getName(),
        new ClientAuthenticationHandler(embeddedServer));
    SessionHandler sessionHandler = new SessionHandler(embeddedServer);
    pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
    return pipelines;
});
```

**Pipeline 初始结构图：**

```
入站数据流（Inbound / Upstream）
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  TCP字节流                                                            │
│    │                                                                  │
│    ▼                                                                  │
│  ┌─────────────────────────┐                                         │
│  │ FixedHeaderFrameDecoder │  帧解码：4字节长度头 + Body               │
│  │ (ReplayingDecoder)      │  将 TCP 字节流切分为完整的协议帧           │
│  └───────────┬─────────────┘                                         │
│              │ ChannelBuffer                                         │
│              ▼                                                       │
│  ┌─────────────────────────────────┐                                 │
│  │ HandshakeInitializationHandler  │  握手：连接建立时发送 Handshake    │
│  │ (SimpleChannelHandler)          │  包含随机 seed                    │
│  └───────────┬─────────────────────┘                                 │
│              │                                                       │
│              ▼                                                       │
│  ┌─────────────────────────────────┐                                 │
│  │ ClientAuthenticationHandler     │  认证：校验用户名密码              │
│  │ (SimpleChannelHandler)          │  认证成功后动态移除自身和握手Handler│
│  └───────────┬─────────────────────┘                                 │
│              │                                                       │
│              ▼                                                       │
│  ┌─────────────────────────────────┐                                 │
│  │ SessionHandler                  │  业务处理：订阅/获取/确认/回滚     │
│  │ (SimpleChannelHandler)          │  这是最终的业务处理器              │
│  └─────────────────────────────────┘                                 │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

**关键设计点：**

1. **Handler 命名**：使用 `类名.getName()` 作为 Handler 的 key，方便后续通过名称动态添加/移除 Handler
2. **每个连接一个 Pipeline 实例**：`setPipelineFactory` 的 Lambda 每次新连接建立时都会被调用，确保 Handler 不被共享
3. **HandshakeInitializationHandler 共享 childGroups**：所有连接的 Channel 都加入同一个 ChannelGroup，方便统一关闭
4. **ClientAuthenticationHandler 和 SessionHandler 共享 embeddedServer**：业务操作都委托给 Embedded Server

#### 第五段：绑定端口

```java
if (StringUtils.isNotEmpty(ip)) {
    this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
} else {
    this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
}
```

- 如果指定了 IP，绑定到特定网卡
- 如果未指定 IP，绑定到所有网卡（`0.0.0.0`）
- 默认端口为 `11111`

---

### 2.4 stop() 方法：优雅关闭

```java
public void stop() {
    super.stop();

    if (this.serverChannel != null) {
        this.serverChannel.close().awaitUninterruptibly(1000);
    }

    if (this.childGroups != null) {
        this.childGroups.close().awaitUninterruptibly(5000);
    }

    if (this.bootstrap != null) {
        this.bootstrap.releaseExternalResources();
    }

    if (embeddedServer.isStart()) {
        embeddedServer.stop();
    }
}
```

**关闭顺序解读：**

| 步骤 | 操作 | 超时 | 说明 |
|------|------|------|------|
| 1 | 关闭 serverChannel | 1秒 | 停止接受新连接 |
| 2 | 关闭所有 childGroups | 5秒 | 显式关闭所有已建立的客户端连接 |
| 3 | 释放 bootstrap 资源 | - | 释放 Boss/Worker 线程池 |
| 4 | 停止 embeddedServer | - | 停止数据服务核心 |

**为什么 childGroups 关闭超时是 5 秒而 serverChannel 是 1 秒？**

- `serverChannel` 只是一个监听 Socket，关闭很快
- `childGroups` 包含所有客户端连接，可能有正在进行的 I/O 操作，需要更长的等待时间
- 注释中说明："close sockets explicitly to reduce socket channel hung in complicated network environment"

---

## 三、Pipeline 完整构建与生命周期变化

### 3.1 Pipeline 在连接生命周期各阶段的变化

Canal 的 Pipeline 是 **动态变化** 的，不同阶段的 Pipeline 结构不同：

#### 阶段一：连接建立（初始状态）

```
FixedHeaderFrameDecoder -> HandshakeInitializationHandler -> ClientAuthenticationHandler -> SessionHandler
```

此时 `HandshakeInitializationHandler` 会立即发送 Handshake 包。

#### 阶段二：认证成功后

```
FixedHeaderFrameDecoder -> IdleStateHandler -> IdleStateAwareChannelHandler -> SessionHandler
```

`ClientAuthenticationHandler.messageReceived()` 的回调中，动态重构 Pipeline：
- 移除 `HandshakeInitializationHandler`（已完成使命）
- 移除 `ClientAuthenticationHandler`（已完成使命）
- 在 `SessionHandler` 前面添加 `IdleStateHandler`（超时检测）
- 在 `SessionHandler` 前面添加 `IdleStateAwareChannelHandler`（超时时关闭连接）

#### 阶段三：正常工作状态

```
FixedHeaderFrameDecoder -> IdleStateHandler -> IdleStateAwareChannelHandler -> SessionHandler
```

此时只有 `SessionHandler` 处理业务消息，`IdleStateHandler` 负责超时检测。

**这种动态 Pipeline 设计的好处：**

1. **安全性**：未认证的连接只能执行握手和认证操作，不能访问业务数据
2. **性能**：认证完成后，数据包不再经过握手/认证 Handler 的处理，减少了不必要的检查
3. **职责单一**：每个 Handler 只负责连接生命周期的一个阶段

---

### 3.2 四个 Handler 的类继承关系

```
                    ┌─────────────────────────────┐
                    │ org.jboss.netty.channel      │
                    │    .ChannelHandler           │
                    └──────────┬──────────────────┘
                               │
          ┌────────────────────┴───────────────────┐
          │                                        │
┌─────────▼──────────┐             ┌───────────────▼──────────┐
│ ReplayingDecoder    │             │ SimpleChannelHandler     │
│ <VoidEnum>          │             │                          │
├─────────────────────┤             ├──────────────────────────┤
│ FixedHeaderFrame    │             │ HandshakeInitialization  │
│ Decoder             │             │ Handler                  │
│                     │             │                          │
│ decode() 方法       │             │ ClientAuthentication     │
│                     │             │ Handler                  │
└─────────────────────┘             │                          │
                                    │ SessionHandler           │
                                    └──────────────────────────┘
```

- `FixedHeaderFrameDecoder` 继承 `ReplayingDecoder`：一个特殊的解码器，它会自动处理不完整的数据包
- 其他三个 Handler 都继承 `SimpleChannelHandler`：Netty 3.x 的通用 Handler 基类，可以处理入站和出站事件

---

## 四、FixedHeaderFrameDecoder 详解

### 4.1 完整源码

```java
// 文件：com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder
package com.alibaba.otter.canal.server.netty.handler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;

/**
 * 解析对应的header信息
 */
public class FixedHeaderFrameDecoder extends ReplayingDecoder<VoidEnum> {

    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, VoidEnum state)
                                                                                                             throws Exception {
        return buffer.readBytes(buffer.readInt());
    }
}
```

### 4.2 逐行深度解读

这个 Handler 只有一行核心代码，却蕴含了相当多的设计思考。

#### 继承 `ReplayingDecoder<VoidEnum>`

```java
public class FixedHeaderFrameDecoder extends ReplayingDecoder<VoidEnum>
```

- `ReplayingDecoder` 是 Netty 3.x 提供的一个特殊解码器。它的核心特性是：**当数据不足时，自动抛出 `ReplayError`，等待更多数据到达后重试**
- 泛型参数 `VoidEnum` 表示"无状态"，即这个解码器不需要状态机来管理解码阶段
- 开发者写的代码看起来像是"数据总是充足的"，但实际上 `ReplayingDecoder` 在背后处理了所有不完整数据的情况

#### decode() 方法

```java
protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, VoidEnum state)
                                                                                                         throws Exception {
    return buffer.readBytes(buffer.readInt());
}
```

这一行代码做了两件事：

1. `buffer.readInt()` —— 读取 4 个字节（int），解释为消息体的长度
2. `buffer.readBytes(length)` —— 根据长度读取对应的消息体

**在 `ReplayingDecoder` 的保护下，如果：**
- 可用数据不足 4 字节 → `readInt()` 抛出 `ReplayError`，等待更多数据
- 可用数据不足 `length` 字节 → `readBytes()` 抛出 `ReplayError`，等待更多数据
- 数据充足 → 正常返回完整的 `ChannelBuffer`

### 4.3 协议帧格式

Canal 使用了一种极其简洁的帧协议：

```
┌─────────────────┬─────────────────────────────────┐
│   Header (4B)   │           Body (N bytes)         │
│                 │                                   │
│  int32 length   │   Protobuf serialized Packet     │
│  (Big-Endian)   │                                   │
└─────────────────┴─────────────────────────────────┘

Total = 4 + N bytes
```

| 字段 | 大小 | 说明 |
|------|------|------|
| Header | 4 字节 | 消息体长度，Big-Endian 的 int32 |
| Body | N 字节 | Protobuf 序列化的 `CanalPacket.Packet` |

**发送端的编码逻辑在 `NettyUtils.write()` 中：**

```java
byte[] header = ByteBuffer.allocate(HEADER_LENGTH)
    .order(ByteOrder.BIG_ENDIAN)
    .putInt(body.length)
    .array();
```

发送时将 header 和 body 合并为一个 `CompositeChannelBuffer`，一次写出。

### 4.4 为什么不用 LengthFieldBasedFrameDecoder

Netty 内置了 `LengthFieldBasedFrameDecoder`，可以更灵活地处理各种长度字段的帧协议。Canal 不用它的原因：

| 对比维度 | `FixedHeaderFrameDecoder` | `LengthFieldBasedFrameDecoder` |
|---------|-------------------------|------------------------------|
| 代码量 | 1 行核心代码 | 需要配置多个参数 |
| 灵活性 | 只支持固定的 4 字节长度头 | 支持任意偏移、长度、调整量 |
| 可读性 | 极其直观 | 参数含义需要查文档 |
| 适用场景 | 私有协议，帧格式固定 | 需要兼容各种已有协议 |
| 解码后输出 | 只包含 Body（不含长度头） | 可配置是否包含长度头 |

Canal 的帧格式极其简单（固定 4 字节长度头），使用 `ReplayingDecoder` 一行代码就解决了，没有必要引入 `LengthFieldBasedFrameDecoder` 的复杂参数。

### 4.5 ReplayingDecoder 的内部原理

```
                    ┌─────────────────────┐
                    │   TCP 字节流         │
                    │   [部分数据到达]      │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  ReplayingDecoder   │
                    │                     │
                    │  1. 包装原始 Buffer  │
                    │     为 Replaying-   │
                    │     DecoderBuffer   │
                    │                     │
                    │  2. 调用 decode()   │
                    │                     │
                    │  3. readInt()       │──── 数据不足？
                    │     readBytes()     │     │
                    │                     │     ▼
                    │                     │  抛出 ReplayError
                    │                     │  (Signal，非真正异常)
                    │                     │     │
                    │  4. 捕获 Replay-    │ <───┘
                    │     Error，重置     │
                    │     readerIndex    │
                    │                     │
                    │  5. 等待更多数据     │
                    │     到达后重试       │
                    └─────────────────────┘
```

`ReplayingDecoder` 通过一个代理 Buffer（`ReplayingDecoderBuffer`）包装原始 Buffer。当调用 `readInt()`/`readBytes()` 时，如果数据不足，代理 Buffer 会抛出一个 `ReplayError`（这是一个 Error，不是 Exception，不会被 catch(Exception) 捕获）。`ReplayingDecoder` 捕获这个 Error，重置 Buffer 的读位置，等待更多数据到达后再次调用 `decode()`。

---

## 五、HandshakeInitializationHandler 详解

### 5.1 完整源码

```java
// 文件：com.alibaba.otter.canal.server.netty.handler.HandshakeInitializationHandler
package com.alibaba.otter.canal.server.netty.handler;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.CanalPacket.Handshake;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.server.netty.NettyUtils;
import com.google.protobuf.ByteString;

/**
 * handshake交互
 */
public class HandshakeInitializationHandler extends SimpleChannelHandler {

    // support to maintain socket channel.
    private ChannelGroup childGroups;

    public HandshakeInitializationHandler(ChannelGroup childGroups){
        this.childGroups = childGroups;
    }

    private static final Logger logger = LoggerFactory.getLogger(HandshakeInitializationHandler.class);

    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // add new socket channel in channel container, used to manage sockets.
        if (childGroups != null) {
            childGroups.add(ctx.getChannel());
        }

        final byte[] seed = org.apache.commons.lang3.RandomUtils.nextBytes(8);
        byte[] body = Packet.newBuilder()
            .setType(CanalPacket.PacketType.HANDSHAKE)
            .setVersion(NettyUtils.VERSION)
            .setBody(Handshake.newBuilder().setSeeds(ByteString.copyFrom(seed)).build().toByteString())
            .build()
            .toByteArray();

        NettyUtils.write(ctx.getChannel(), body, future -> {
            ctx.getPipeline().get(HandshakeInitializationHandler.class.getName());
            ClientAuthenticationHandler handler = (ClientAuthenticationHandler) ctx.getPipeline()
                .get(ClientAuthenticationHandler.class.getName());
            handler.setSeed(seed);
        });
        logger.info("send handshake initialization packet to : {}", ctx.getChannel());
    }
}
```

### 5.2 逐行深度解读

#### 构造函数：接收 ChannelGroup

```java
private ChannelGroup childGroups;

public HandshakeInitializationHandler(ChannelGroup childGroups){
    this.childGroups = childGroups;
}
```

- `ChannelGroup` 是 Netty 提供的 Channel 集合，线程安全
- 在 `CanalServerWithNetty` 中创建：`new DefaultChannelGroup()`
- 所有连接的 Channel 都会加入这个 Group，方便在 `stop()` 时统一关闭

#### channelOpen 事件

```java
public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
```

- `channelOpen` 是 Netty 3.x 中的事件方法，在 TCP 连接建立完成时触发
- 这是连接生命周期中最早的事件（在 `channelConnected` 之前）
- 等价于 Netty 4.x 的 `channelActive`

#### 注册 Channel 到 Group

```java
if (childGroups != null) {
    childGroups.add(ctx.getChannel());
}
```

- 将新连接加入 `ChannelGroup`
- `ChannelGroup.close()` 时会自动关闭所有已加入的 Channel
- null 检查是防御性编程

#### 生成随机 seed

```java
final byte[] seed = org.apache.commons.lang3.RandomUtils.nextBytes(8);
```

- 生成 8 字节的随机种子
- 这个 seed 用于密码认证的挑战-响应协议（类似 MySQL 的握手协议）
- 每个连接都有独立的 seed，确保安全性
- 使用 Apache Commons Lang 3 的 `RandomUtils`，生成安全随机数

#### 构建 Handshake Packet

```java
byte[] body = Packet.newBuilder()
    .setType(CanalPacket.PacketType.HANDSHAKE)     // PacketType = 1
    .setVersion(NettyUtils.VERSION)                 // version = 1
    .setBody(Handshake.newBuilder()
        .setSeeds(ByteString.copyFrom(seed))        // 将 seed 放入 Handshake 消息
        .build()
        .toByteString())
    .build()
    .toByteArray();
```

**Packet 结构（Protobuf）：**

```
Packet {
    PacketType type = HANDSHAKE (1)
    int32 version = 1
    bytes body = Handshake {
        bytes seeds = [8 随机字节]
    }
}
```

- 使用 Protobuf 序列化，紧凑且跨语言
- `Handshake` 消息只包含一个字段：`seeds`
- 注意序列化的嵌套结构：`Packet.body` 是 `Handshake` 消息序列化后的 `ByteString`

#### 发送 Handshake 并注册回调

```java
NettyUtils.write(ctx.getChannel(), body, future -> {
    ctx.getPipeline().get(HandshakeInitializationHandler.class.getName());
    ClientAuthenticationHandler handler = (ClientAuthenticationHandler) ctx.getPipeline()
        .get(ClientAuthenticationHandler.class.getName());
    handler.setSeed(seed);
});
```

**ChannelFuture 回调链分析：**

1. `NettyUtils.write()` 将 `[4字节长度头 + body]` 写入 Channel
2. 写操作完成后，ChannelFuture 的回调被触发
3. 在回调中，通过 Pipeline 获取 `ClientAuthenticationHandler` 实例
4. 将 `seed` 传递给 `ClientAuthenticationHandler`

**为什么要通过回调传递 seed，而不是在构造函数中传递？**

因为 Pipeline 中每个 Handler 实例在连接建立时就创建了。`seed` 是在 `channelOpen` 时动态生成的，必须在握手包发送成功后才传递给认证 Handler。这确保了 seed 的传递是时序安全的。

> 注意：第一行 `ctx.getPipeline().get(HandshakeInitializationHandler.class.getName())` 获取了 HandshakeInitializationHandler 但没有使用返回值。这可能是一个无意义的遗留代码，或者最初有某种用途后来被移除了。

#### 日志记录

```java
logger.info("send handshake initialization packet to : {}", ctx.getChannel());
```

记录每个新连接的握手信息，包含客户端的 IP 和端口。

---

### 5.3 Handshake 交互时序图

```
    Client                                          Server
      │                                                │
      │──────── TCP SYN ──────────────────────────────>│
      │<─────── TCP SYN+ACK ─────────────────────────-│
      │──────── TCP ACK ──────────────────────────────>│
      │                                                │
      │              [连接建立, channelOpen 触发]         │
      │                                                │
      │                                  生成 seed[8]   │
      │                                  构建 Handshake │
      │                                  Packet        │
      │                                                │
      │<────── [4B长度头 + Handshake Packet] ──────────-│
      │                                                │
      │        解析 seed                                │
      │        构建 ClientAuth                          │
      │        使用 seed 加密密码                        │
      │                                                │
      │──────── [4B长度头 + ClientAuth Packet] ────────>│
      │                                                │
```

---

## 六、ClientAuthenticationHandler 详解

### 6.1 完整源码

```java
// 文件：com.alibaba.otter.canal.server.netty.handler.ClientAuthenticationHandler
package com.alibaba.otter.canal.server.netty.handler;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.helpers.MessageFormatter;

import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAuth;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.NettyUtils;

/**
 * 客户端身份认证处理
 */
public class ClientAuthenticationHandler extends SimpleChannelHandler {

    private static final Logger     logger                                  =
        LoggerFactory.getLogger(ClientAuthenticationHandler.class);
    private final int               SUPPORTED_VERSION                       = 3;
    private final int               defaultSubscriptorDisconnectIdleTimeout = 60 * 60 * 1000;
    private CanalServerWithEmbedded embeddedServer;
    private byte[]                  seed;

    public ClientAuthenticationHandler(){
    }

    public ClientAuthenticationHandler(CanalServerWithEmbedded embeddedServer){
        this.embeddedServer = embeddedServer;
    }

    // ... (messageReceived 方法见下文)

    public void setSeed(byte[] seed) {
        this.seed = seed;
    }
}
```

### 6.2 字段解读

| 字段 | 值 | 说明 |
|------|-----|------|
| `SUPPORTED_VERSION` | 3 | 支持的协议版本号 |
| `defaultSubscriptorDisconnectIdleTimeout` | 3600000 (1小时) | 默认的空闲断开超时时间，单位毫秒 |
| `embeddedServer` | 单例引用 | 实际执行认证和订阅操作的 Embedded Server |
| `seed` | 8字节数组 | 由 HandshakeInitializationHandler 在回调中设置的随机种子 |

### 6.3 messageReceived 方法逐段解读

#### 解析 ClientAuth 包

```java
public void messageReceived(final ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
    final Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
    switch (packet.getVersion()) {
        case SUPPORTED_VERSION:
        default:
            final ClientAuth clientAuth = ClientAuth.parseFrom(packet.getBody());
```

- `e.getMessage()` 返回的是经过 `FixedHeaderFrameDecoder` 解码后的 `ChannelBuffer`（已经去掉了 4 字节长度头）
- `Packet.parseFrom()` 使用 Protobuf 反序列化
- `switch` 的 `case SUPPORTED_VERSION` 和 `default` 合并在一起，意味着任何版本号都按相同逻辑处理
- `ClientAuth.parseFrom(packet.getBody())` 从 Packet 的 body 中提取 ClientAuth 消息

**ClientAuth 消息结构：**

```
ClientAuth {
    string username      -- 用户名
    bytes password        -- 加密后的密码（使用 seed 加密）
    int32 netReadTimeout  -- 客户端读超时（毫秒）
    int32 netWriteTimeout -- 客户端写超时（毫秒）
    string destination    -- [可选] 要订阅的 destination
    string clientId       -- [可选] 客户端ID
    string filter         -- [可选] 订阅过滤器
}
```

#### Seed 校验

```java
if (seed == null) {
    byte[] errorBytes = NettyUtils.errorPacket(400,
        MessageFormatter.format("auth failed for seed is null", clientAuth.getUsername()).getMessage());
    NettyUtils.write(ctx.getChannel(), errorBytes, null);
    break;
}
```

如果 seed 为 null，说明 Handshake 包的 ChannelFuture 回调还没有执行完，或者 `setSeed()` 还没有被调用。这是一种防御性检查。

#### 密码校验

```java
if (!embeddedServer.auth(clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed)) {
    byte[] errorBytes = NettyUtils.errorPacket(400,
        MessageFormatter.format("auth failed for user:{}", clientAuth.getUsername()).getMessage());
    NettyUtils.write(ctx.getChannel(), errorBytes, null);
    break;
}
```

调用 `CanalServerWithEmbedded.auth()` 执行实际的认证逻辑。

**auth() 方法的实现（位于 CanalServerWithEmbedded）：**

```java
public boolean auth(String user, String passwd, byte[] seed) {
    // 如果user/passwd密码为空,则任何用户账户都能登录
    if ((StringUtils.isEmpty(this.user) || StringUtils.equals(this.user, user))) {
        if (StringUtils.isEmpty(this.passwd)) {
            return true;
        } else if (StringUtils.isEmpty(passwd)) {
            // 如果server密码有配置,客户端密码为空,则拒绝
            return false;
        }

        try {
            byte[] passForClient = SecurityUtil.hexStr2Bytes(passwd);
            return SecurityUtil.scrambleServerAuth(passForClient,
                SecurityUtil.hexStr2Bytes(this.passwd), seed);
        } catch (NoSuchAlgorithmException e) {
            return false;
        }
    }
    return false;
}
```

**认证逻辑的三层判断：**

```
                    ┌──────────────────────────┐
                    │ Server 配置了 user/passwd？│
                    └─────────┬────────────────┘
                              │
                    ┌─────────▼────────────────┐
              否    │ this.user 为空？           │
           ┌────────┤ 或 user 匹配？            │
           │        └─────────┬────────────────┘
           │                  │ 是
           ▼                  ▼
       返回 false    ┌────────────────────┐
                    │ this.passwd 为空？  │
                    └────┬──────────┬────┘
                     是  │          │ 否
                         ▼          ▼
                    返回 true   ┌───────────────────┐
                    (免密)     │ 客户端 passwd 为空？│
                              └────┬──────────┬────┘
                               是  │          │ 否
                                   ▼          ▼
                              返回 false   SecurityUtil
                              (拒绝空密码)  .scrambleServerAuth()
```

#### 可选的自动订阅

```java
// 如果存在订阅信息
if (StringUtils.isNotEmpty(clientAuth.getDestination())
    && StringUtils.isNotEmpty(clientAuth.getClientId())) {
    ClientIdentity clientIdentity = new ClientIdentity(clientAuth.getDestination(),
        Short.valueOf(clientAuth.getClientId()),
        clientAuth.getFilter());
    try {
        MDC.put("destination", clientIdentity.getDestination());
        embeddedServer.subscribe(clientIdentity);
        // 尝试启动，如果已经启动，忽略
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
```

**这段逻辑的作用：**

如果客户端在 `ClientAuth` 包中携带了 `destination` 和 `clientId`，认证成功后会立即执行订阅。这是一种"连接时自动订阅"的优化，避免了客户端需要发送额外的 `SUBSCRIPTION` 请求。

**执行步骤：**

1. 构建 `ClientIdentity`（destination + clientId + filter）
2. 调用 `embeddedServer.subscribe(clientIdentity)` 注册订阅
3. 检查对应的 Instance 是否已启动，如果未启动则通过 `ServerRunningMonitor` 启动
4. `MDC.put/remove` 用于日志上下文，让日志能区分不同 destination

#### 认证成功的 ACK 和 Pipeline 重构

```java
// 鉴权一次性，暂不统计
NettyUtils.ack(ctx.getChannel(), future -> {
    logger.info("remove unused channel handlers after authentication is done successfully.");
    ctx.getPipeline().remove(HandshakeInitializationHandler.class.getName());
    ctx.getPipeline().remove(ClientAuthenticationHandler.class.getName());

    int readTimeout = defaultSubscriptorDisconnectIdleTimeout;
    int writeTimeout = defaultSubscriptorDisconnectIdleTimeout;
    if (clientAuth.getNetReadTimeout() > 0) {
        readTimeout = clientAuth.getNetReadTimeout();
    }
    if (clientAuth.getNetWriteTimeout() > 0) {
        writeTimeout = clientAuth.getNetWriteTimeout();
    }

    // fix bug: soTimeout parameter's unit from connector is milliseconds.
    IdleStateHandler idleStateHandler = new IdleStateHandler(NettyUtils.hashedWheelTimer,
        readTimeout,
        writeTimeout,
        0,
        TimeUnit.MILLISECONDS);
    ctx.getPipeline().addBefore(SessionHandler.class.getName(),
        IdleStateHandler.class.getName(),
        idleStateHandler);

    IdleStateAwareChannelHandler idleStateAwareChannelHandler = new IdleStateAwareChannelHandler() {
        public void channelIdle(ChannelHandlerContext ctx1, IdleStateEvent e1) throws Exception {
            logger.warn("channel:{} idle timeout exceeds, close channel to save server resources...",
                ctx1.getChannel());
            ctx1.getChannel().close();
        }
    };
    ctx.getPipeline().addBefore(SessionHandler.class.getName(),
        IdleStateAwareChannelHandler.class.getName(),
        idleStateAwareChannelHandler);
});
```

**这是整个认证处理中最复杂的部分，Pipeline 动态重构的详细过程：**

##### 步骤一：发送 ACK

```java
NettyUtils.ack(ctx.getChannel(), future -> { ... });
```

向客户端发送一个 ACK 包（空 body 的 ACK Packet），ACK 发送成功后触发 ChannelFuture 回调。

##### 步骤二：移除已完成使命的 Handler

```java
ctx.getPipeline().remove(HandshakeInitializationHandler.class.getName());
ctx.getPipeline().remove(ClientAuthenticationHandler.class.getName());
```

- 握手和认证都是一次性的，认证完成后这两个 Handler 就没有用了
- 移除它们可以减少每个消息的处理链长度，提升性能
- 使用类名字符串定位 Handler（与 Pipeline 构建时的命名一致）

##### 步骤三：添加空闲检测 Handler

```java
IdleStateHandler idleStateHandler = new IdleStateHandler(NettyUtils.hashedWheelTimer,
    readTimeout,    // 读超时，默认 1 小时
    writeTimeout,   // 写超时，默认 1 小时
    0,              // allIdleTime，不使用
    TimeUnit.MILLISECONDS);
ctx.getPipeline().addBefore(SessionHandler.class.getName(),
    IdleStateHandler.class.getName(),
    idleStateHandler);
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hashedWheelTimer` | 全局共享 | 时间轮定时器，所有连接共享一个实例 |
| `readTimeout` | 3600000ms (1小时) | 读空闲超时，可由客户端 `netReadTimeout` 覆盖 |
| `writeTimeout` | 3600000ms (1小时) | 写空闲超时，可由客户端 `netWriteTimeout` 覆盖 |
| `allIdleTime` | 0 | 不使用 |

##### 步骤四：添加空闲事件处理 Handler

```java
IdleStateAwareChannelHandler idleStateAwareChannelHandler = new IdleStateAwareChannelHandler() {
    public void channelIdle(ChannelHandlerContext ctx1, IdleStateEvent e1) throws Exception {
        logger.warn("channel:{} idle timeout exceeds, close channel to save server resources...",
            ctx1.getChannel());
        ctx1.getChannel().close();
    }
};
ctx.getPipeline().addBefore(SessionHandler.class.getName(),
    IdleStateAwareChannelHandler.class.getName(),
    idleStateAwareChannelHandler);
```

- `IdleStateHandler` 检测到空闲后，会触发 `IdleStateEvent`
- `IdleStateAwareChannelHandler` 接收这个事件，直接关闭连接
- 这样可以释放长时间不活跃的客户端连接，节省服务端资源

**Pipeline 变化前后对比：**

```
认证前：
FixedHeaderFrameDecoder -> HandshakeInitializationHandler -> ClientAuthenticationHandler -> SessionHandler

认证后：
FixedHeaderFrameDecoder -> IdleStateHandler -> IdleStateAwareChannelHandler -> SessionHandler
```

---

### 6.4 认证交互时序图

```
    Client                                          Server
      │                                                │
      │<────── Handshake [seed] ──────────────────────-│
      │                                                │
      │   使用 seed 加密密码                             │
      │   token = scramble411(password, seed)           │
      │                                                │
      │──────── ClientAuth [user, token,               │
      │         destination?, clientId?] ──────────────>│
      │                                                │
      │                     auth(user, token, seed)     │
      │                     验证: scrambleServerAuth()  │
      │                                                │
      │                     [认证成功]                   │
      │                     移除 Handshake Handler     │
      │                     移除 Auth Handler          │
      │                     添加 IdleState Handler     │
      │                     [可选] subscribe()          │
      │                                                │
      │<────── ACK ────────────────────────────────────│
      │                                                │
      │   [连接进入正常工作状态]                          │
      │                                                │
      │──────── SUBSCRIPTION / GET / ACK / ... ───────>│
      │                                                │
```

---

## 七、SessionHandler 详解（核心业务处理器）

### 7.1 完整源码

```java
// 文件：com.alibaba.otter.canal.server.netty.handler.SessionHandler
package com.alibaba.otter.canal.server.netty.handler;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalPacket.*;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.NettyUtils;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

public class SessionHandler extends SimpleChannelHandler {

    private static final Logger     logger = LoggerFactory.getLogger(SessionHandler.class);
    private CanalServerWithEmbedded embeddedServer;

    public SessionHandler(){
    }

    public SessionHandler(CanalServerWithEmbedded embeddedServer){
        this.embeddedServer = embeddedServer;
    }

    // ... (messageReceived 方法见下文分段解析)
}
```

### 7.2 messageReceived 总体结构

```java
public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    long start = System.nanoTime();
    ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
    Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
    try {
        switch (packet.getType()) {
            case SUBSCRIPTION:      // 订阅请求
                // ...
                break;
            case UNSUBSCRIPTION:    // 取消订阅请求
                // ...
                break;
            case GET:               // 获取数据请求
                // ...
                break;
            case CLIENTACK:         // 客户端确认请求
                // ...
                break;
            case CLIENTROLLBACK:    // 客户端回滚请求
                // ...
                break;
            default:                // 不支持的请求类型
                // ...
                break;
        }
    } catch (Throwable exception) {
        // 全局异常处理
    } finally {
        MDC.remove("destination");
    }
}
```

**关键特点：**

1. `System.nanoTime()` 用于精确计时，配合 `ChannelFutureAggregator` 做性能监控
2. 使用 `Packet.parseFrom()` 反序列化 Protobuf 消息
3. 通过 `packet.getType()` 分发到不同的处理分支
4. 所有分支都会创建 `ChannelFutureAggregator` 用于性能统计

---

### 7.3 SUBSCRIPTION 请求处理

```java
case SUBSCRIPTION:
    Sub sub = Sub.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(sub.getDestination()) && StringUtils.isNotEmpty(sub.getClientId())) {
        ClientIdentity clientIdentity = new ClientIdentity(
            sub.getDestination(),
            Short.parseShort(sub.getClientId()),
            sub.getFilter());
        MDC.put("destination", clientIdentity.getDestination());

        // 尝试启动，如果已经启动，忽略
        if (!embeddedServer.isStart(clientIdentity.getDestination())) {
            ServerRunningMonitor runningMonitor =
                ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
            if (!runningMonitor.isStart()) {
                runningMonitor.start();
            }
        }

        embeddedServer.subscribe(clientIdentity);

        byte[] ackBytes = NettyUtils.ackPacket();
        NettyUtils.write(ctx.getChannel(), ackBytes, new ChannelFutureAggregator(
            sub.getDestination(), sub, packet.getType(),
            ackBytes.length, System.nanoTime() - start));
    } else {
        byte[] errorBytes = NettyUtils.errorPacket(401,
            "destination or clientId is null. Sub: " + sub);
        NettyUtils.write(ctx.getChannel(), errorBytes, new ChannelFutureAggregator(
            sub.getDestination(), sub, packet.getType(),
            errorBytes.length, System.nanoTime() - start, (short) 401));
    }
    break;
```

**处理流程：**

```
SUBSCRIPTION 请求
      │
      ▼
┌─────────────────────────────┐
│ 参数校验                      │
│ destination 和 clientId 非空？│
└──────┬──────────────┬───────┘
   是  │              │ 否
       ▼              ▼
┌──────────────┐  返回 401 错误
│ 构建           │
│ ClientIdentity │
└──────┬───────┘
       ▼
┌──────────────────────┐
│ Instance 已启动？      │
└──────┬──────────┬────┘
   否  │          │ 是
       ▼          │
  启动 Instance    │
  (RunningMonitor) │
       │          │
       ▼          ▼
┌──────────────────────┐
│ embeddedServer       │
│   .subscribe()       │
└──────┬───────────────┘
       ▼
  返回 ACK
```

**Lazy 启动模式：**

这里体现了 Canal 的 lazy 启动模式 —— Instance 不是在 Server 启动时就全部启动，而是在客户端首次订阅时才启动。这种设计的好处：

1. 节省资源：未被订阅的 Instance 不会占用 MySQL 连接和内存
2. 按需启动：客户端连接时自动启动对应的 Instance
3. HA 支持：通过 `ServerRunningMonitor` 实现主备切换

---

### 7.4 UNSUBSCRIPTION 请求处理

```java
case UNSUBSCRIPTION:
    Unsub unsub = Unsub.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(unsub.getDestination()) && StringUtils.isNotEmpty(unsub.getClientId())) {
        ClientIdentity clientIdentity = new ClientIdentity(
            unsub.getDestination(),
            Short.parseShort(unsub.getClientId()),
            unsub.getFilter());
        MDC.put("destination", clientIdentity.getDestination());

        embeddedServer.unsubscribe(clientIdentity);
        stopCanalInstanceIfNecessary(clientIdentity); // 尝试关闭

        byte[] ackBytes = NettyUtils.ackPacket();
        NettyUtils.write(ctx.getChannel(), ackBytes, new ChannelFutureAggregator(
            unsub.getDestination(), unsub, packet.getType(),
            ackBytes.length, System.nanoTime() - start));
    } else {
        // ... 返回 401 错误
    }
    break;
```

**stopCanalInstanceIfNecessary 方法：**

```java
private void stopCanalInstanceIfNecessary(ClientIdentity clientIdentity) {
    List<ClientIdentity> clientIdentitys =
        embeddedServer.listAllSubscribe(clientIdentity.getDestination());
    if (clientIdentitys != null && clientIdentitys.size() == 1
        && clientIdentitys.contains(clientIdentity)) {
        ServerRunningMonitor runningMonitor =
            ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
        if (runningMonitor.isStart()) {
            runningMonitor.release();
        }
    }
}
```

**逻辑解读：**

当最后一个订阅者取消订阅时，释放 HA 运行权（而不是直接停止 Instance）。

- `listAllSubscribe()` 获取当前 destination 的所有订阅者
- 只有当订阅者列表中仅剩自己一个时，才释放运行权
- `runningMonitor.release()` 释放 ZK 上的运行权，让其他节点可以接管

---

### 7.5 GET 请求处理（高性能序列化核心）

GET 请求是 Canal 中最复杂、性能最关键的处理分支。

```java
case GET:
    Get get = CanalPacket.Get.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(get.getDestination()) && StringUtils.isNotEmpty(get.getClientId())) {
        ClientIdentity clientIdentity = new ClientIdentity(
            get.getDestination(), Short.parseShort(get.getClientId()));
        MDC.put("destination", clientIdentity.getDestination());

        Message message = null;

        if (get.getTimeout() == -1) { // 是否是初始值
            message = embeddedServer.getWithoutAck(clientIdentity, get.getFetchSize());
        } else {
            TimeUnit unit = convertTimeUnit(get.getUnit());
            message = embeddedServer.getWithoutAck(clientIdentity,
                get.getFetchSize(), get.getTimeout(), unit);
        }
```

**数据获取：**

- `getWithoutAck()` 从 Store 中获取数据，但不立即确认
- 支持两种模式：无超时（一次性获取）和有超时（阻塞等待）
- `timeout == -1` 是初始值标记，表示客户端没有设置超时

#### Raw 模式高性能序列化

```java
if (message.getId() != -1 && message.isRaw()) {
    List<ByteString> rowEntries = message.getRawEntries();
    // message size
    int messageSize = 0;
    messageSize += com.google.protobuf.CodedOutputStream.computeInt64Size(1, message.getId());

    int dataSize = 0;
    for (ByteString rowEntry : rowEntries) {
        dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);
    }
    messageSize += dataSize;
    messageSize += rowEntries.size();

    // packet size
    int size = 0;
    size += com.google.protobuf.CodedOutputStream.computeEnumSize(3,
        PacketType.MESSAGES.getNumber());
    size += com.google.protobuf.CodedOutputStream.computeTagSize(5)
            + com.google.protobuf.CodedOutputStream.computeRawVarint32Size(messageSize)
            + messageSize;

    byte[] body = new byte[size];
    CodedOutputStream output = CodedOutputStream.newInstance(body);
    output.writeEnum(3, PacketType.MESSAGES.getNumber());

    output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    output.writeRawVarint32(messageSize);
    // message
    output.writeInt64(1, message.getId());
    for (ByteString rowEntry : rowEntries) {
        output.writeBytes(2, rowEntry);
    }
    output.checkNoSpaceLeft();
    NettyUtils.write(ctx.getChannel(), body, new ChannelFutureAggregator(
        get.getDestination(), get, packet.getType(),
        body.length, System.nanoTime() - start, message.getId() == -1));
```

**这段代码是 Canal 网络层的性能核心。让我们逐步解析：**

##### 为什么使用 Raw 模式

当 `message.isRaw() == true` 时，消息中存储的是已经序列化好的 `ByteString`（原始字节），而不是反序列化后的 `Entry` 对象。这意味着：

1. **避免反序列化 + 再序列化**：数据从 Store 取出时就是原始字节，直接写入网络，省去了序列化开销
2. **减少内存分配**：不需要创建中间的 Java 对象（`Entry` 等 Protobuf 对象）
3. **减少 GC 压力**：大批量数据传输时，减少临时对象的创建

##### CodedOutputStream 手写序列化

不使用 Protobuf 的 Builder 模式（`Packet.newBuilder().setBody(...).build()`），而是直接使用 `CodedOutputStream` 手写字节流。这样做的好处：

1. **精确控制内存分配**：先计算出总大小，一次性分配 `byte[]`，避免 Builder 内部的多次 copy
2. **零拷贝**：raw entries 直接写入输出流，不需要额外拷贝
3. **避免 Builder 开销**：Builder 模式内部有字段验证、默认值设置等开销

**手写序列化对应的 Protobuf 结构：**

```
Packet {                               // 手写
    field 3 (enum): type = MESSAGES    // output.writeEnum(3, ...)
    field 5 (bytes): body = Messages { // output.writeTag(5, ...) + writeRawVarint32
        field 1 (int64): batchId       // output.writeInt64(1, id)
        field 2 (bytes): messages[]    // output.writeBytes(2, entry) * N
    }
}
```

##### Size 计算的精确推导

```java
// 计算 Messages 内部的大小
int messageSize = 0;
messageSize += CodedOutputStream.computeInt64Size(1, message.getId()); // batchId 字段

int dataSize = 0;
for (ByteString rowEntry : rowEntries) {
    dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);  // 每个 entry 的数据大小
}
messageSize += dataSize;
messageSize += rowEntries.size();  // 每个 entry 的 tag 大小（1字节）

// 计算 Packet 外层的大小
int size = 0;
size += CodedOutputStream.computeEnumSize(3, PacketType.MESSAGES.getNumber()); // type 字段
size += CodedOutputStream.computeTagSize(5)                    // body 字段的 tag
     + CodedOutputStream.computeRawVarint32Size(messageSize)   // body 字段的长度前缀
     + messageSize;                                            // body 字段的数据
```

这种精确的 size 计算确保了 `byte[]` 一次分配到位，`output.checkNoSpaceLeft()` 在最后验证计算的正确性。

##### 非 Raw 模式（回退路径）

```java
} else {
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
    byte[] body = packetBuilder.setBody(messageBuilder.build().toByteString())
        .build().toByteArray();
    NettyUtils.write(ctx.getChannel(), body, new ChannelFutureAggregator(...));
}
```

当 `message.getId() == -1`（没有数据）或 `message.isRaw() == false` 时，使用标准的 Protobuf Builder 模式。这是一个回退路径，性能不如 Raw 模式但代码更简洁。

---

### 7.6 CLIENTACK 请求处理

```java
case CLIENTACK:
    ClientAck ack = CanalPacket.ClientAck.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(ack.getDestination()) && StringUtils.isNotEmpty(ack.getClientId())) {
        MDC.put("destination", ack.getDestination());
        if (ack.getBatchId() == 0L) {
            byte[] errorBytes = NettyUtils.errorPacket(402,
                "batchId should assign value. Ack: " + ack);
            NettyUtils.write(ctx.getChannel(), errorBytes, ...);
        } else if (ack.getBatchId() == -1L) {
            // -1代表上一次get没有数据，直接忽略之
            // donothing
        } else {
            ClientIdentity clientIdentity = new ClientIdentity(
                ack.getDestination(), Short.parseShort(ack.getClientId()));
            embeddedServer.ack(clientIdentity, ack.getBatchId());
            new ChannelFutureAggregator(...).operationComplete(null);
        }
    } else {
        // ... 返回 401 错误
    }
    break;
```

**三种 batchId 值的处理策略：**

| batchId | 处理 | 说明 |
|---------|------|------|
| `0` | 返回 402 错误 | batchId 不能为 0，这是无效值 |
| `-1` | 忽略（donothing） | 上一次 GET 没有数据时返回 -1，客户端 ACK -1 是正常行为 |
| `> 0` | 调用 `embeddedServer.ack()` | 正常确认，释放已消费的数据 |

**注意：ACK 操作不需要返回响应给客户端。** `ChannelFutureAggregator` 直接调用 `operationComplete(null)` 仅用于性能统计，不涉及网络 I/O。

---

### 7.7 CLIENTROLLBACK 请求处理

```java
case CLIENTROLLBACK:
    ClientRollback rollback = CanalPacket.ClientRollback.parseFrom(packet.getBody());
    if (StringUtils.isNotEmpty(rollback.getDestination())
        && StringUtils.isNotEmpty(rollback.getClientId())) {
        ClientIdentity clientIdentity = new ClientIdentity(
            rollback.getDestination(), Short.parseShort(rollback.getClientId()));
        MDC.put("destination", rollback.getDestination());

        if (rollback.getBatchId() == 0L) {
            embeddedServer.rollback(clientIdentity);          // 回滚所有批次
        } else {
            embeddedServer.rollback(clientIdentity, rollback.getBatchId()); // 只回滚单个批次
        }

        new ChannelFutureAggregator(...).operationComplete(null);
    } else {
        // ... 返回 401 错误
    }
    break;
```

**两种回滚模式：**

| batchId | 操作 | 说明 |
|---------|------|------|
| `0` | `rollback(clientIdentity)` | 回滚该客户端的所有未确认批次 |
| `> 0` | `rollback(clientIdentity, batchId)` | 只回滚指定的批次 |

**与 ACK 的对比：**

```
          batchId = 0    batchId = -1    batchId > 0
  ACK:    报错 402        忽略            确认
  ROLLBACK: 全部回滚      N/A             部分回滚
```

---

### 7.8 异常处理

#### 业务异常：catch (Throwable exception)

```java
catch (Throwable exception) {
    String error = "something goes wrong with channel: " + ctx.getChannel()
        + ", exception: " + ExceptionUtils.getStackTrace(exception);
    byte[] errorBytes = NettyUtils.errorPacket(400, error);
    NettyUtils.write(ctx.getChannel(), errorBytes, new ChannelFutureAggregator(...));
} finally {
    MDC.remove("destination");
}
```

- 捕获所有 `Throwable`（包括 Error），将完整堆栈信息发送给客户端
- 错误码统一使用 400
- finally 块确保 MDC 被清理

#### 连接异常：exceptionCaught

```java
public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    logger.error("something goes wrong with channel:{}, exception={}",
        ctx.getChannel(),
        ExceptionUtils.getStackTrace(e.getCause()));

    ctx.getChannel().close();
}
```

- 当网络层出现异常时，直接关闭连接
- 记录完整的异常堆栈
- 不尝试恢复，让客户端重新连接

#### channelClosed 事件（已注释）

```java
public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // 已注释掉的代码：
    // ClientIdentity clientIdentity = (ClientIdentity) ctx.getAttachment();
    // if (clientIdentity != null) {
    //     stopCanalInstanceIfNecessary(clientIdentity);
    // }
}
```

原本的设计是：当客户端断开连接时，自动释放 Instance 的运行权。但这段代码被注释掉了，可能的原因：
- 在 HA 环境下，客户端断开不应该导致 Instance 停止（可能只是网络抖动）
- Instance 的生命周期应该由 UNSUBSCRIPTION 显式控制

---

### 7.9 时间单位转换

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

将 Protobuf 中的 int 枚举转换为 Java 的 `TimeUnit`。默认值为 `MILLISECONDS`。

---

## 八、NettyUtils 工具类详解

### 8.1 完整源码

```java
// 文件：com.alibaba.otter.canal.server.netty.NettyUtils
package com.alibaba.otter.canal.server.netty;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.CompositeChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.CanalPacket.Ack;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;

public class NettyUtils {

    private static final Logger logger           = LoggerFactory.getLogger(NettyUtils.class);
    public static int           HEADER_LENGTH    = 4;
    public static Timer         hashedWheelTimer = new HashedWheelTimer();
    public static int           VERSION          = 1;

    // ... (方法见下文)
}
```

### 8.2 常量字段

| 字段 | 值 | 说明 |
|------|-----|------|
| `HEADER_LENGTH` | 4 | 帧头长度（4 字节 int） |
| `hashedWheelTimer` | 全局单例 | 时间轮定时器，用于 IdleStateHandler |
| `VERSION` | 1 | 协议版本号 |

### 8.3 write() 方法（byte[] 版本）

```java
public static void write(Channel channel, byte[] body, ChannelFutureListener channelFutureListner) {
    byte[] header = ByteBuffer.allocate(HEADER_LENGTH)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(body.length)
        .array();
    if (channelFutureListner == null) {
        Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body));
    } else {
        Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body))
            .addListener(channelFutureListner);
    }
}
```

**逐步解析：**

1. **构造 header**：创建 4 字节的 ByteBuffer，Big-Endian 字节序，写入 body 的长度
2. **合并 header 和 body**：使用 `ChannelBuffers.wrappedBuffer(header, body)` 将两个 byte[] 包装为一个 ChannelBuffer（零拷贝）
3. **写入 Channel**：使用 `Channels.write()` 发起异步写操作
4. **回调注册**：如果有 `channelFutureListner`，注册到 ChannelFuture 上

### 8.4 write() 方法（ByteBuffer 版本）

```java
public static void write(Channel channel, ByteBuffer body, ChannelFutureListener channelFutureListner) {
    byte[] header = ByteBuffer.allocate(HEADER_LENGTH)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(body.limit())
        .array();
    List<ChannelBuffer> components = new ArrayList<>(2);
    components.add(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, header));
    components.add(ChannelBuffers.wrappedBuffer(body));

    if (channelFutureListner == null) {
        Channels.write(channel, new CompositeChannelBuffer(ByteOrder.BIG_ENDIAN, components));
    } else {
        Channels.write(channel, new CompositeChannelBuffer(ByteOrder.BIG_ENDIAN, components))
            .addListener(channelFutureListner);
    }
}
```

与 byte[] 版本的区别：
- 使用 `CompositeChannelBuffer` 合并多个 ChannelBuffer
- 使用 `body.limit()` 获取 ByteBuffer 的长度（而不是 `body.length`）
- 这个版本目前在代码中被注释掉的 ByteBuffer 回收逻辑使用（SessionHandler 的 GET 处理中）

### 8.5 ack() 方法

```java
public static void ack(Channel channel, ChannelFutureListener channelFutureListner) {
    write(channel,
        Packet.newBuilder()
            .setType(CanalPacket.PacketType.ACK)
            .setVersion(VERSION)
            .setBody(Ack.newBuilder().build().toByteString())
            .build()
            .toByteArray(),
        channelFutureListner);
}
```

发送一个空的 ACK 包。ACK 包的 Protobuf 结构：

```
Packet {
    type = ACK (3)
    version = 1
    body = Ack {
        // 空，表示成功
    }
}
```

### 8.6 error() 方法

```java
public static void error(int errorCode, String errorMessage, Channel channel,
                         ChannelFutureListener channelFutureListener) {
    if (channelFutureListener == null) {
        channelFutureListener = ChannelFutureListener.CLOSE;
    }

    logger.error("ErrotCode:{} , Caused by : \n{}", errorCode, errorMessage);
    write(channel,
        Packet.newBuilder()
            .setType(CanalPacket.PacketType.ACK)
            .setVersion(VERSION)
            .setBody(Ack.newBuilder()
                .setErrorCode(errorCode)
                .setErrorMessage(errorMessage)
                .build().toByteString())
            .build()
            .toByteArray(),
        channelFutureListener);
}
```

**注意：**

- 错误响应也是 `PacketType.ACK`，通过 `Ack.errorCode` 和 `Ack.errorMessage` 区分成功和失败
- 如果没有指定回调，默认使用 `ChannelFutureListener.CLOSE`，即发送错误后关闭连接
- 代码中有个 typo：`ErrotCode` 应该是 `ErrorCode`

### 8.7 ackPacket() 和 errorPacket() 方法

```java
public static byte[] ackPacket() {
    return Packet.newBuilder()
        .setType(CanalPacket.PacketType.ACK)
        .setVersion(VERSION)
        .setBody(Ack.newBuilder().build().toByteString())
        .build()
        .toByteArray();
}

public static byte[] errorPacket(int errorCode, String errorMessage) {
    return Packet.newBuilder()
        .setType(CanalPacket.PacketType.ACK)
        .setVersion(VERSION)
        .setBody(Ack.newBuilder()
            .setErrorCode(errorCode)
            .setErrorMessage(errorMessage)
            .build().toByteString())
        .build()
        .toByteArray();
}
```

这两个方法与 `ack()`/`error()` 的区别：只生成字节数组，不执行写操作。供 `SessionHandler` 使用，因为 `SessionHandler` 需要先计算字节长度用于性能统计。

### 8.8 HashedWheelTimer

```java
public static Timer hashedWheelTimer = new HashedWheelTimer();
```

`HashedWheelTimer` 是 Netty 提供的时间轮定时器，用于高效管理大量超时任务。

**时间轮原理：**

```
        ┌─────┐
    ┌───┤  0  ├───┐
    │   └─────┘   │
 ┌──┴──┐       ┌──┴──┐
 │  7  │       │  1  │
 └──┬──┘       └──┬──┘
 ┌──┴──┐       ┌──┴──┐
 │  6  │       │  2  │
 └──┬──┘       └──┬──┘
    │   ┌─────┐   │
    ├───┤  5  ├───┤
    │   └─────┘   │
 ┌──┴──┐       ┌──┴──┐
 │  4  │       │  3  │
 └─────┘       └─────┘
    指针按 tick 旋转
```

- 每个格子（bucket）存储一组定时任务
- 指针每 tick（默认 100ms）前进一格
- 到达的格子中的任务被检查和执行
- 时间复杂度 O(1) 的任务添加和取消
- 适合管理大量粗粒度的超时任务（如连接超时）

全局共享一个 `HashedWheelTimer` 实例，所有连接的 `IdleStateHandler` 都使用它，避免创建大量线程。

---

## 九、Profiler 性能监控体系

### 9.1 架构概览

```
┌─────────────────────────────────────────────────────┐
│                  SessionHandler                     │
│                                                     │
│   每个请求创建 ChannelFutureAggregator               │
│   记录: destination, type, amount, latency          │
└──────────────────────┬──────────────────────────────┘
                       │ I/O 完成后回调
                       ▼
┌──────────────────────────────────────────────────────┐
│              ChannelFutureAggregator                 │
│                                                      │
│  operationComplete(ChannelFuture) {                  │
│      if (future.getCause() != null)                  │
│          result.channelError = cause;                │
│      profiler().profiling(result);                   │
│  }                                                   │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│           CanalServerWithNettyProfiler               │
│           (单例，策略模式)                              │
│                                                      │
│  默认实现: DefaultClientInstanceProfiler (NOP)        │
│  可替换为: 自定义的 ClientInstanceProfiler 实现         │
│                                                      │
│  profiling(ClientRequestResult result) {             │
│      instanceProfiler.profiling(result);             │
│  }                                                   │
└──────────────────────────────────────────────────────┘
```

### 9.2 ClientInstanceProfiler 接口

```java
// 文件：com.alibaba.otter.canal.server.netty.ClientInstanceProfiler
package com.alibaba.otter.canal.server.netty;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator.ClientRequestResult;

public interface ClientInstanceProfiler extends CanalLifeCycle {
    void profiling(ClientRequestResult result);
}
```

- 继承 `CanalLifeCycle`，拥有 `start()` 和 `stop()` 生命周期方法
- 只有一个核心方法 `profiling()`，接收请求结果
- 实现类可以做任何统计：吞吐量、延迟分布、错误率等

### 9.3 CanalServerWithNettyProfiler 单例

```java
// 文件：com.alibaba.otter.canal.server.netty.CanalServerWithNettyProfiler
public class CanalServerWithNettyProfiler {

    public static final ClientInstanceProfiler NOP = new DefaultClientInstanceProfiler();
    private ClientInstanceProfiler instanceProfiler;

    private static class SingletonHolder {
        private static CanalServerWithNettyProfiler SINGLETON = new CanalServerWithNettyProfiler();
    }

    private CanalServerWithNettyProfiler() {
        this.instanceProfiler = NOP;
    }

    public static CanalServerWithNettyProfiler profiler() {
        return SingletonHolder.SINGLETON;
    }

    public void profiling(ClientRequestResult result) {
        instanceProfiler.profiling(result);
    }

    public void setInstanceProfiler(ClientInstanceProfiler instanceProfiler) {
        this.instanceProfiler = instanceProfiler;
    }

    private static class DefaultClientInstanceProfiler
        extends AbstractCanalLifeCycle implements ClientInstanceProfiler {
        @Override
        public void profiling(ClientRequestResult result) {
            // NOP - 什么都不做
        }
    }
}
```

**设计模式分析：**

1. **单例模式**：`SingletonHolder` 延迟初始化，线程安全
2. **策略模式**：`instanceProfiler` 是可替换的策略对象
3. **空对象模式**：`NOP` 是默认的"什么都不做"的实现，避免 null 检查
4. **开闭原则**：通过 `setInstanceProfiler()` 可以在不修改代码的情况下替换监控实现

### 9.4 ChannelFutureAggregator 详解

```java
// 文件：com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator
public class ChannelFutureAggregator implements ChannelFutureListener {

    private ClientRequestResult result;

    public ChannelFutureAggregator(String destination, GeneratedMessageV3 request,
        CanalPacket.PacketType type, int amount, long latency, boolean empty) {
        this(destination, request, type, amount, latency, empty, (short) 0);
    }

    // ... 其他重载构造函数

    private ChannelFutureAggregator(String destination, GeneratedMessageV3 request,
        CanalPacket.PacketType type, int amount, long latency,
        boolean empty, short errorCode) {
        this.result = new ClientRequestResult.Builder()
                .destination(destination)
                .type(type)
                .request(request)
                .amount(amount + HEADER_LENGTH)  // 加上 4 字节头
                .latency(latency)
                .errorCode(errorCode)
                .empty(empty)
                .build();
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        // profiling after I/O operation
        if (future != null && future.getCause() != null) {
            result.channelError = future.getCause();
        }
        profiler().profiling(result);
    }
}
```

**关键细节：**

- `amount + HEADER_LENGTH`：统计的数据量包含 4 字节的帧头
- `operationComplete()` 在 I/O 完成后被调用，此时可以知道是否有网络错误
- 对于 ACK 和 ROLLBACK 请求（不需要返回数据），直接调用 `operationComplete(null)`

### 9.5 ClientRequestResult POJO

```java
public static class ClientRequestResult {
    private String                 destination;    // 目标 Instance
    private CanalPacket.PacketType type;           // 请求类型
    private GeneratedMessageV3     request;        // 原始请求
    private int                    amount;         // 响应数据量（字节）
    private long                   latency;        // 处理延迟（纳秒）
    private short                  errorCode;      // 错误码（0=成功）
    private boolean                empty;          // 是否空响应
    private Throwable              channelError;   // 网络层错误

    // Builder 模式构造
    public static class Builder {
        // ... 链式设置方法
    }
}
```

这个 POJO 记录了一次请求的完整上下文，包括：
- **谁**发的请求（destination）
- **什么**类型的请求（type）
- 响应**多大**（amount）
- **花了多久**（latency）
- **是否成功**（errorCode, channelError）
- 响应**是否为空**（empty）

---

## 十、SecurityUtil 密码安全机制

### 10.1 完整源码

```java
// 文件：com.alibaba.otter.canal.protocol.SecurityUtil
package com.alibaba.otter.canal.protocol;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class SecurityUtil {

    /**
     * 客户端加密算法
     * token = SHA1(scramble + SHA1(stage1_hash)) XOR stage1_hash
     */
    public static final byte[] scramble411(byte[] pass, byte[] seed)
        throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);     // stage1_hash = SHA1(明文密码)
        md.reset();
        byte[] pass2 = md.digest(pass1);    // stage2_hash = SHA1(stage1_hash)
        md.reset();
        md.update(seed);                    // SHA1(seed + stage2_hash)
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (pass3[i] ^ pass1[i]);  // XOR stage1_hash
        }
        return pass3;
    }

    /**
     * 服务端验证算法
     */
    public static final boolean scrambleServerAuth(byte[] token, byte[] pass, byte[] seed)
        throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.update(seed);
        byte[] pass1 = md.digest(pass);     // SHA1(seed + password_hash)
        for (int i = 0; i < pass1.length; i++) {
            pass1[i] = (byte) (token[i] ^ pass1[i]);  // token XOR SHA1(seed + password_hash)
        }

        md = MessageDigest.getInstance("SHA-1");
        byte[] pass2 = md.digest(pass1);    // SHA1(result)
        return Arrays.equals(pass, pass2);  // 对比 password_hash
    }

    // ... (md5String, byte2HexStr, hexStr2Bytes 等辅助方法)
}
```

### 10.2 密码校验协议详解

Canal 的密码校验协议与 MySQL 的 `mysql_native_password` 认证协议完全一致。

#### 客户端加密流程（scramble411）

```
输入：
  pass = 明文密码的字节数组
  seed = 服务端发送的 8 字节随机种子

计算过程：
  step1: pass1 = SHA1(pass)              // 对明文密码做 SHA1
  step2: pass2 = SHA1(pass1)             // 对 SHA1(密码) 再做 SHA1
  step3: pass3 = SHA1(seed + pass2)      // 将 seed 和 二次哈希拼接后做 SHA1
  step4: token = pass3 XOR pass1         // 异或第一次哈希

输出：
  token（20字节）
```

#### 服务端验证流程（scrambleServerAuth）

```
输入：
  token  = 客户端发送的加密令牌
  pass   = 服务端存储的密码哈希（SHA1(SHA1(明文密码))）
  seed   = 服务端当初生成的随机种子

计算过程：
  step1: pass1 = SHA1(seed + pass)       // 将 seed 和存储的密码哈希拼接后 SHA1
  step2: result = token XOR pass1        // 异或还原出 SHA1(明文密码)
  step3: pass2 = SHA1(result)            // 对还原结果做 SHA1
  step4: 比较 pass2 == pass              // 如果相等，认证通过

输出：
  true/false
```

#### 数学证明

```
客户端计算的 token:
  token = SHA1(seed + SHA1(SHA1(pass))) XOR SHA1(pass)

服务端验证:
  pass1 = SHA1(seed + stored_hash)      // stored_hash = SHA1(SHA1(pass))
        = SHA1(seed + SHA1(SHA1(pass)))  // 与客户端 step3 结果相同

  result = token XOR pass1
         = [SHA1(seed + SHA1(SHA1(pass))) XOR SHA1(pass)] XOR SHA1(seed + SHA1(SHA1(pass)))
         = SHA1(pass)                    // XOR 两次抵消

  pass2 = SHA1(result) = SHA1(SHA1(pass))

  比较: pass2 == stored_hash
      = SHA1(SHA1(pass)) == SHA1(SHA1(pass))  // 相等！
```

### 10.3 安全性分析

**这种挑战-响应协议的安全特性：**

| 特性 | 说明 |
|------|------|
| 明文密码不传输 | 网络上传输的是 token，不是明文密码 |
| 重放攻击防护 | 每次连接的 seed 不同，相同密码产生不同的 token |
| 服务端不存明文 | 服务端只存储 `SHA1(SHA1(password))`，即二次哈希 |
| 中间人攻击 | 不防护。攻击者获取 seed 和 token 后可以离线暴力破解 |
| 密码强度 | 依赖于用户密码的强度，SHA1 本身已不被认为是安全的哈希算法 |

### 10.4 与 MySQL 密码协议的关系

```
┌──────────────────────────────────────────────────────┐
│               MySQL 协议握手流程                      │
│                                                      │
│  1. Server -> Client: Handshake Packet              │
│     包含: protocol_version, server_version,          │
│           salt_part1 (8B) + salt_part2 (12B)        │
│           auth_plugin_name = mysql_native_password   │
│                                                      │
│  2. Client -> Server: Handshake Response            │
│     包含: username, auth_response (20B)             │
│           auth_response = scramble411(pass, salt)    │
│                                                      │
│  3. Server: 验证 auth_response                      │
│     使用 mysql.user 表中存储的密码哈希验证             │
│                                                      │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│               Canal 协议握手流程                      │
│                                                      │
│  1. Server -> Client: Handshake Packet              │
│     包含: seed (8B)                                  │
│                                                      │
│  2. Client -> Server: ClientAuth                    │
│     包含: username, password (token的hex编码)        │
│           password = hex(scramble411(pass, seed))    │
│                                                      │
│  3. Server: 验证                                    │
│     使用配置文件中的密码哈希验证                       │
│     scrambleServerAuth(token, stored_hash, seed)    │
│                                                      │
└──────────────────────────────────────────────────────┘
```

Canal 直接复用了 MySQL 的 `mysql_native_password` 认证算法，只是简化了协议包的格式（使用 Protobuf 而不是 MySQL 的二进制协议）。

### 10.5 辅助方法

#### byte2HexStr

```java
public static String byte2HexStr(byte[] b) {
    StringBuilder hs = new StringBuilder();
    for (byte value : b) {
        String hex = (Integer.toHexString(value & 0XFF));
        if (hex.length() == 1) {
            hs.append("0" + hex);
        } else {
            hs.append(hex);
        }
    }
    return hs.toString();
}
```

将字节数组转换为十六进制字符串。例如 `[0x1a, 0x2b]` -> `"1a2b"`。

#### hexStr2Bytes

将十六进制字符串转换回字节数组。这个方法的实现比较冗长（使用 switch-case 逐个字符转换），但性能很好，避免了 `Integer.parseInt()` 的开销。

#### scrambleGenPass

```java
public static final String scrambleGenPass(byte[] pass) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-1");
    byte[] pass1 = md.digest(pass);
    md.reset();
    byte[] pass2 = md.digest(pass1);
    return SecurityUtil.byte2HexStr(pass2);
}
```

生成存储在服务端的密码哈希：`SHA1(SHA1(明文密码))` 的十六进制表示。这个方法用于生成 `canal.properties` 中 `canal.user.passwd` 的值。

---

## 十一、CanalMQStarter —— MQ 投递模式

### 11.1 概述

Canal 支持两种数据输出模式：

1. **TCP 模式**（Netty）：客户端通过 TCP 连接拉取数据
2. **MQ 模式**（Kafka/RocketMQ/RabbitMQ/PulsarMQ）：服务端主动推送数据到消息队列

当 Canal 运行在 MQ 模式下时，**Netty 服务端不需要启动**。数据通过 `CanalMQStarter` 从 `CanalServerWithEmbedded` 拉取，然后投递到 MQ。

```
┌──────────────────────────────────────────────────────┐
│                 TCP 模式                              │
│                                                      │
│  Client ──TCP──> CanalServerWithNetty                │
│                      │                               │
│                      ▼                               │
│                  CanalServerWithEmbedded              │
│                      │                               │
│                      ▼                               │
│                  CanalInstance (Parser/Sink/Store)    │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│                 MQ 模式                               │
│                                                      │
│  CanalMQStarter (worker 线程)                        │
│      │                                               │
│      ▼                                               │
│  CanalServerWithEmbedded                             │
│      │          │                                    │
│      ▼          ▼                                    │
│  CanalInstance  CanalMQProducer ──> Kafka/RocketMQ   │
│  (Parser/Sink/Store)                                 │
└──────────────────────────────────────────────────────┘
```

### 11.2 完整源码分析

#### 类结构

```java
public class CanalMQStarter {

    private volatile boolean             running        = false;
    private ExecutorService              executorService;
    private CanalMQProducer              canalMQProducer;
    private MQProperties                 mqProperties;
    private CanalServerWithEmbedded      canalServer;
    private Map<String, CanalMQRunnable> canalMQWorks   = new ConcurrentHashMap<>();
    private static Thread                shutdownThread = null;

    public CanalMQStarter(CanalMQProducer canalMQProducer){
        this.canalMQProducer = canalMQProducer;
    }
}
```

| 字段 | 说明 |
|------|------|
| `running` | 全局运行标志，volatile 保证可见性 |
| `executorService` | 线程池，每个 destination 一个工作线程 |
| `canalMQProducer` | MQ 生产者（SPI 加载） |
| `mqProperties` | MQ 配置属性 |
| `canalServer` | Embedded Server 引用 |
| `canalMQWorks` | destination -> 工作线程 的映射 |
| `shutdownThread` | JVM 关闭钩子线程 |

#### start() 方法

```java
public synchronized void start(String destinations) {
    try {
        if (running) {
            return;
        }
        mqProperties = canalMQProducer.getMqProperties();
        if (mqProperties.isFilterTransactionEntry()) {
            System.setProperty("canal.instance.filter.transaction.entry", "true");
        }

        canalServer = CanalServerWithEmbedded.instance();

        executorService = Executors.newCachedThreadPool();
        logger.info("## start the MQ workers.");

        String[] dsts = StringUtils.split(destinations, ",");
        for (String destination : dsts) {
            destination = destination.trim();
            CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
            canalMQWorks.put(destination, canalMQRunnable);
            executorService.execute(canalMQRunnable);
        }

        running = true;
        // ... JVM shutdown hook
    } catch (Throwable e) {
        logger.error("## Something goes wrong when starting up the canal MQ workers:", e);
    }
}
```

**启动流程：**

1. 获取 MQ 配置属性
2. 如果配置了事务条目过滤，设置系统属性
3. 获取 `CanalServerWithEmbedded` 单例
4. 创建线程池
5. 为每个 destination 创建一个 `CanalMQRunnable` 工作线程
6. 注册 JVM 关闭钩子

#### worker() 方法：消费循环核心

```java
private void worker(String destination, AtomicBoolean destinationRunning, CountDownLatch latch) {
    // 等待全局 running 标志
    while (!running || !destinationRunning.get()) {
        Thread.sleep(100);
    }

    logger.info("## start the MQ producer: {}.", destination);
    MDC.put("destination", destination);
    final ClientIdentity clientIdentity = new ClientIdentity(destination, (short) 1001, "");

    while (running && destinationRunning.get()) {
        try {
            CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
            if (canalInstance == null) {
                Thread.sleep(3000);
                continue;
            }

            MQDestination canalDestination = new MQDestination();
            // ... 从 canalInstance.getMqConfig() 获取 MQ 配置

            canalServer.subscribe(clientIdentity);

            Integer getTimeout = mqProperties.getFetchTimeout();
            Integer getBatchSize = mqProperties.getBatchSize();

            while (running && destinationRunning.get()) {
                Message message;
                if (getTimeout != null && getTimeout > 0) {
                    message = canalServer.getWithoutAck(clientIdentity,
                        getBatchSize, getTimeout.longValue(), TimeUnit.MILLISECONDS);
                } else {
                    message = canalServer.getWithoutAck(clientIdentity, getBatchSize);
                }

                final long batchId = message.getId();
                try {
                    int size = message.isRaw()
                        ? message.getRawEntries().size()
                        : message.getEntries().size();

                    if (batchId != -1 && size != 0) {
                        canalMQProducer.send(canalDestination, message, new Callback() {
                            @Override
                            public void commit() {
                                canalServer.ack(clientIdentity, batchId);
                            }

                            @Override
                            public void rollback() {
                                canalServer.rollback(clientIdentity, batchId);
                            }
                        });
                    } else {
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("process error!", e);
        }
    }

    latch.countDown();
}
```

**消费循环流程图：**

```
┌───────────────────────────┐
│  等待 running = true       │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  获取 CanalInstance        │
│  Instance 不存在？          │──── 是 ──> sleep(3s) 重试
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  构建 MQDestination       │
│  (topic, partition, etc)  │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  subscribe(clientIdentity) │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  getWithoutAck()          │<─────────────────────┐
│  从 Store 拉取一批数据     │                       │
└──────────┬────────────────┘                       │
           ▼                                        │
┌───────────────────────────┐                       │
│  数据为空？                │                       │
│  (batchId == -1 or size==0)│                       │
└────┬──────────────┬───────┘                       │
  是 │              │ 否                             │
     ▼              ▼                               │
  sleep(100ms)   send() 投递到 MQ                    │
     │              │                               │
     │              ├── 成功 → callback.commit()     │
     │              │          → ack(batchId)        │
     │              │                               │
     │              └── 失败 → callback.rollback()   │
     │                         → rollback(batchId)  │
     │                                              │
     └──────────────────────────────────────────────┘
```

**关键设计点：**

1. **clientId = 1001**：MQ 模式使用固定的 clientId `1001`，与 TCP 模式的客户端区分
2. **getWithoutAck + ack/rollback**：与 TCP 模式相同的消费语义，保证数据不丢失
3. **Callback 模式**：`CanalMQProducer.send()` 接受一个 `Callback`，由 Producer 在发送成功/失败后调用
4. **空数据 sleep**：没有数据时 sleep 100ms，避免 CPU 空转
5. **Instance 延迟可用**：如果 Instance 还没有启动，sleep 3 秒后重试

#### CanalMQRunnable 内部类

```java
private class CanalMQRunnable implements Runnable {
    private String destination;
    private AtomicBoolean running = new AtomicBoolean(true);
    private CountDownLatch latch = new CountDownLatch(1);
    private Future future;

    CanalMQRunnable(String destination){
        this.destination = destination;
    }

    @Override
    public void run() {
        worker(destination, running, latch);
    }

    public void stop(boolean wait) {
        running.set(false);
        if (wait) {
            try {
                future.cancel(true);   // 中断线程
                latch.await();         // 等待线程正常退出
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
```

**优雅停止机制：**

1. `running.set(false)` —— 设置停止标志
2. `future.cancel(true)` —— 中断线程（唤醒 sleep/阻塞等待）
3. `latch.await()` —— 等待 worker 线程正常退出（latch 在 worker 结尾 countDown）

#### MQ 模式下为什么禁用 Netty

MQ 模式下不需要 Netty 的原因：

1. **数据消费方式不同**：TCP 模式是客户端主动拉取（pull），MQ 模式是服务端主动推送（push）
2. **不需要网络协议**：MQ 模式直接在 JVM 内部调用 `embeddedServer` 的方法，不需要 TCP 协议
3. **不需要客户端管理**：MQ 模式的"客户端"是 MQ Producer，是 JVM 内部对象
4. **减少资源占用**：不需要 Netty 的 Boss/Worker 线程池和网络端口

---

## 十二、CanalServer 与 CanalService 接口定义

### 12.1 CanalServer 接口

```java
// 文件：com.alibaba.otter.canal.server.CanalServer
public interface CanalServer extends CanalLifeCycle {
    void start() throws CanalServerException;
    void stop() throws CanalServerException;
}
```

`CanalServer` 是最顶层的 Server 抽象，只定义了生命周期方法。两个实现类：

- `CanalServerWithEmbedded`：嵌入式实现，包含所有数据操作逻辑
- `CanalServerWithNetty`：网络层实现，将操作委托给 Embedded

### 12.2 CanalService 接口

```java
// 文件：com.alibaba.otter.canal.server.CanalService
public interface CanalService {
    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;
    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;
    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;
    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;
    void rollback(ClientIdentity clientIdentity) throws CanalServerException;
    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
```

`CanalService` 定义了所有数据操作的接口，是 Canal 消费协议的 Java 抽象。

**接口方法与 Netty PacketType 的对应关系：**

| CanalService 方法 | PacketType | 说明 |
|-------------------|-----------|------|
| `subscribe()` | SUBSCRIPTION | 注册订阅 |
| `unsubscribe()` | UNSUBSCRIPTION | 取消订阅 |
| `get()` | GET (autoAck=true) | 获取并自动确认 |
| `getWithoutAck()` | GET (autoAck=false) | 获取但不自动确认 |
| `ack()` | CLIENTACK | 确认消费 |
| `rollback()` | CLIENTROLLBACK | 回滚消费 |

只有 `CanalServerWithEmbedded` 实现了 `CanalService`。`CanalServerWithNetty` 不实现它，因为 Netty 层只负责网络传输，业务逻辑委托给 Embedded。

---

## 十三、Kafka/RocketMQ Connector 概述

### 13.1 CanalMQProducer SPI 接口

```java
// 文件：com.alibaba.otter.canal.connector.core.spi.CanalMQProducer
@SPI("kafka")
public interface CanalMQProducer {
    void init(Properties properties);
    MQProperties getMqProperties();
    void send(MQDestination canalDestination, Message message, Callback callback);
    void stop();
}
```

**SPI 加载机制：**

- `@SPI("kafka")` 注解指定默认实现为 kafka
- Canal 使用自己的 `ExtensionLoader`（类似 Dubbo 的 SPI 机制）加载实现类
- 实现类通过 `@SPI("kafka")` 或 `@SPI("rocketmq")` 注解标识
- 在 `CanalStarter` 中通过配置 `canal.serverMode` 决定加载哪个实现

**SPI 接口方法说明：**

| 方法 | 说明 |
|------|------|
| `init(Properties)` | 初始化 Producer，读取配置，创建底层 MQ 客户端 |
| `getMqProperties()` | 获取 MQ 配置属性，供 CanalMQStarter 使用 |
| `send()` | 发送消息到 MQ，成功调用 `callback.commit()`，失败调用 `callback.rollback()` |
| `stop()` | 关闭 Producer，释放资源 |

### 13.2 CanalKafkaProducer 实现详解

```java
// 文件：com.alibaba.otter.canal.connector.kafka.producer.CanalKafkaProducer
@SPI("kafka")
public class CanalKafkaProducer extends AbstractMQProducer implements CanalMQProducer {

    private Producer<String, byte[]> producer;

    @Override
    public void init(Properties properties) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
        this.mqProperties = kafkaProducerConfig;
        super.init(properties);
        this.loadKafkaProperties(properties);

        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(kafkaProducerConfig.getKafkaProperties());
        kafkaProperties.put("max.in.flight.requests.per.connection", 1);
        kafkaProperties.put("key.serializer", StringSerializer.class);
        // ... Kerberos 配置
        kafkaProperties.put("value.serializer", KafkaMessageSerializer.class);
        producer = new KafkaProducer<>(kafkaProperties);
    }
}
```

**关键配置：**

| 配置项 | 值 | 说明 |
|--------|-----|------|
| `max.in.flight.requests.per.connection` | 1 | **关键！** 确保在网络重试时的消息顺序性 |
| `key.serializer` | StringSerializer | Key 使用 String 序列化 |
| `value.serializer` | KafkaMessageSerializer | Value 使用自定义序列化器 |

> `max.in.flight.requests.per.connection = 1` 的含义：每个连接在同一时刻最多只有一个未确认的请求。如果某个消息发送失败需要重试，后续消息会被阻塞，从而保证分区内的消息顺序性。

### 13.3 send() 方法解析

```java
@Override
public void send(MQDestination mqDestination, Message message, Callback callback) {
    ExecutorTemplate template = new ExecutorTemplate(sendExecutor);

    try {
        List result;
        if (!StringUtils.isEmpty(mqDestination.getDynamicTopic())) {
            // 动态 topic 路由
            Map<String, Message> messageMap = MQMessageUtils.messageTopics(
                message, mqDestination.getTopic(), mqDestination.getDynamicTopic());

            for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
                final String topicName = entry.getKey().replace('.', '_');
                final Message messageSub = entry.getValue();
                template.submit((Callable) () -> {
                    return send(mqDestination, topicName, messageSub, mqProperties.isFlatMessage());
                });
            }
            result = template.waitForResult();
        } else {
            // 静态 topic
            result = new ArrayList();
            List<Future> futures = send(mqDestination, mqDestination.getTopic(),
                message, mqProperties.isFlatMessage());
            result.add(futures);
        }

        // 批量 flush + 检查发送结果
        producer.flush();
        for (Object obj : result) {
            List<Future> futures = (List<Future>) obj;
            for (Future future : futures) {
                future.get();  // 检查是否有异常
            }
        }

        callback.commit();  // 全部成功，提交确认
    } catch (Throwable e) {
        logger.error(e.getMessage(), e);
        callback.rollback();  // 有失败，回滚
    } finally {
        template.clear();
    }
}
```

**发送流程：**

```
              Message（一批 binlog 事件）
                      │
          ┌───────────┴───────────┐
          │                       │
     动态 Topic               静态 Topic
          │                       │
     ┌────▼─────┐            ┌────▼─────┐
     │ 按 schema │            │ 直接发送  │
     │ /table   │            │          │
     │ 路由到    │            │          │
     │ 不同topic │            │          │
     └────┬─────┘            └────┬─────┘
          │                       │
          ▼                       ▼
     ┌──────────────────────────────┐
     │  send(destination, topic,    │
     │       message, flat)         │
     │                              │
     │  flat=true:  JSON 序列化     │
     │  flat=false: Protobuf 序列化 │
     │                              │
     │  有 partitionHash:           │
     │    按 hash 路由到分区         │
     │  无 partitionHash:           │
     │    使用固定分区               │
     └──────────┬───────────────────┘
                │
                ▼
     ┌──────────────────────┐
     │  producer.send()     │  异步发送
     │  (ProducerRecord)    │
     └──────────┬───────────┘
                │
                ▼
     ┌──────────────────────┐
     │  producer.flush()    │  等待所有消息写出
     └──────────┬───────────┘
                │
                ▼
     ┌──────────────────────┐
     │  future.get()        │  检查发送结果
     └──────────┬───────────┘
                │
         ┌──────┴──────┐
         │             │
      全部成功       有失败
         │             │
   callback.commit() callback.rollback()
```

### 13.4 消息序列化方式

| 模式 | 配置 | 格式 | 适用场景 |
|------|------|------|----------|
| Protobuf 原始模式 | `flatMessage=false` | `CanalMessageSerializerUtil.serializer()` | 高性能，跨语言需要 proto 文件 |
| JSON 扁平模式 | `flatMessage=true` | `JSON.toJSONBytes(FlatMessage)` | 易读，不需要 proto 文件 |

**FlatMessage JSON 示例：**

```json
{
    "id": 1,
    "database": "test",
    "table": "user",
    "type": "INSERT",
    "ts": 1638888888000,
    "sql": "",
    "data": [
        {
            "id": "1",
            "name": "张三",
            "age": "25"
        }
    ],
    "old": null
}
```

### 13.5 分区策略

```java
if (mqDestination.getPartitionHash() != null && !mqDestination.getPartitionHash().isEmpty()) {
    // Hash 分区
    EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
    Message[] messages = MQMessageUtils.messagePartition(datas,
        message.getId(), partitionNum,
        mqDestination.getPartitionHash(),
        this.mqProperties.isDatabaseHash());
    // 每个分区一个 ProducerRecord
} else {
    // 固定分区
    final int partition = mqDestination.getPartition() != null ? mqDestination.getPartition() : 0;
    records.add(new ProducerRecord<>(topicName, partition, null, serializedData));
}
```

**两种分区策略：**

| 策略 | 配置 | 说明 |
|------|------|------|
| 固定分区 | `canal.mq.partition` | 所有消息发送到同一个分区，保证全局顺序 |
| Hash 分区 | `canal.mq.partitionHash` | 按 database/table/pk 做 hash，同一行的变更保证顺序 |

Hash 分区的配置格式：

```properties
# 按 database.table 做 hash
canal.mq.partitionHash=test\..*

# 按主键做 hash
canal.mq.partitionHash=test\.user:id

# 按 database 做 hash
canal.mq.databaseHash=true
```

---

## 十四、CanalPacket Protobuf 协议定义

### 14.1 PacketType 枚举

```protobuf
enum PacketType {
    PACKAGETYPECOMPATIBLEPROTO2 = 0;  // 兼容 proto2
    HANDSHAKE = 1;                     // 握手
    CLIENTAUTHENTICATION = 2;          // 客户端认证
    ACK = 3;                           // 确认/错误响应
    SUBSCRIPTION = 4;                  // 订阅
    UNSUBSCRIPTION = 5;                // 取消订阅
    GET = 6;                           // 获取数据
    MESSAGES = 7;                      // 数据响应
    CLIENTACK = 8;                     // 客户端确认
    SHUTDOWN = 9;                      // 关闭
    DUMP = 10;                         // dump
    HEARTBEAT = 11;                    // 心跳
    CLIENTROLLBACK = 12;               // 客户端回滚
}
```

### 14.2 Compression 枚举

```protobuf
enum Compression {
    COMPRESSIONCOMPATIBLEPROTO2 = 0;
    NONE = 1;          // 不压缩
    ZLIB = 2;          // ZLIB 压缩
    GZIP = 3;          // GZIP 压缩
    LZF = 4;           // LZF 压缩
}
```

### 14.3 消息类型与使用场景

```
┌───────────────────────────────────────────────────────────────────────┐
│                    Canal 协议完整交互流程                              │
│                                                                       │
│  Server ──HANDSHAKE──> Client                                        │
│  Client ──CLIENTAUTHENTICATION──> Server                             │
│  Server ──ACK──> Client (认证结果)                                    │
│                                                                       │
│  ┌─ 循环 ─────────────────────────────────────────────────────────┐  │
│  │                                                                 │  │
│  │  Client ──SUBSCRIPTION──> Server                               │  │
│  │  Server ──ACK──> Client                                        │  │
│  │                                                                 │  │
│  │  Client ──GET──> Server                                        │  │
│  │  Server ──MESSAGES──> Client  (或 ACK 表示无数据)               │  │
│  │                                                                 │  │
│  │  Client ──CLIENTACK──> Server  (确认消费)                       │  │
│  │                                                                 │  │
│  │  Client ──CLIENTROLLBACK──> Server  (回滚，重新消费)             │  │
│  │                                                                 │  │
│  │  Client ──UNSUBSCRIPTION──> Server                             │  │
│  │  Server ──ACK──> Client                                        │  │
│  │                                                                 │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

---

## 十五、关键设计总结

### 15.1 双 Server 架构设计

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│                  CanalServerWithNetty                              │
│                  (网络壳 / 协议层)                                  │
│                                                                    │
│   职责：                                                           │
│   - TCP 连接管理（accept / close / idle timeout）                  │
│   - 帧编解码（4B 长度头 + Protobuf body）                          │
│   - 握手和认证（seed 挑战 + 密码校验）                              │
│   - Pipeline 动态管理                                              │
│   - 请求分发（PacketType switch）                                  │
│   - 响应序列化（高性能 raw 模式）                                   │
│                                                                    │
│   不做：                                                           │
│   - 任何数据存取逻辑                                               │
│   - Instance 管理                                                  │
│   - 消费位点管理                                                   │
│                                                                    │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│                  CanalServerWithEmbedded                           │
│                  (数据服务核心 / 业务层)                             │
│                                                                    │
│   职责：                                                           │
│   - Instance 的创建、启动、停止                                     │
│   - 订阅管理（subscribe / unsubscribe）                            │
│   - 数据获取（get / getWithoutAck）                                │
│   - 消费确认（ack / rollback）                                     │
│   - 用户认证（auth）                                               │
│   - Metrics 监控                                                   │
│                                                                    │
│   不做：                                                           │
│   - 任何网络通信                                                   │
│   - 协议编解码                                                     │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

**这种分层设计的好处：**

1. **复用性**：`CanalServerWithEmbedded` 可以被 Netty 模式和 MQ 模式共同使用
2. **可测试性**：可以直接通过 Java API 调用 Embedded Server 进行单元测试，不需要建立 TCP 连接
3. **灵活性**：可以轻松替换网络层实现（如从 Netty 3 升级到 Netty 4，或使用其他网络框架）
4. **简洁性**：每一层只关注自己的职责，代码逻辑清晰

### 15.2 Netty 3.x 的使用特点

#### 为什么 Canal 使用 Netty 3.x 而不是 4.x

| 原因 | 说明 |
|------|------|
| 历史原因 | Canal 始于 2012 年，当时 Netty 4.x 还没有稳定发布 |
| 稳定性 | Netty 3.x 经过多年生产验证，非常稳定 |
| 够用 | Canal 的网络层需求很简单（私有协议、连接数少），3.x 完全能满足 |
| 升级成本 | Netty 3.x 和 4.x 的 API 差异很大，升级需要重写整个网络层 |
| 性能影响小 | Canal 的性能瓶颈在 binlog 解析和存储，不在网络层 |

#### Netty 3.x vs 4.x API 对比

| 概念 | Netty 3.x | Netty 4.x |
|------|-----------|----------|
| 引导类 | `ServerBootstrap` | `ServerBootstrap` |
| Channel 工厂 | `NioServerSocketChannelFactory` | `NioServerSocketChannel.class` |
| 线程模型 | Boss/Worker 线程池 | `EventLoopGroup` |
| Pipeline 工厂 | `setPipelineFactory(Lambda)` | `childHandler(ChannelInitializer)` |
| Handler 基类 | `SimpleChannelHandler` | `ChannelInboundHandlerAdapter` |
| 帧解码器 | `ReplayingDecoder` | `ReplayingDecoder` (API变化) |
| 事件方法 | `channelOpen` | `channelActive` |
| 消息事件 | `messageReceived(ctx, MessageEvent)` | `channelRead(ctx, Object)` |
| 异常事件 | `exceptionCaught(ctx, ExceptionEvent)` | `exceptionCaught(ctx, Throwable)` |
| Buffer | `ChannelBuffer` | `ByteBuf` |
| 静态帮助 | `Channels.write()` | `ctx.writeAndFlush()` |

### 15.3 连接生命周期完整时序

```
时间轴
  │
  ▼
  T1: TCP 三次握手建立连接
      │
      ├── channelOpen 事件
      │   ├── 加入 ChannelGroup
      │   ├── 生成 seed[8]
      │   ├── 发送 Handshake Packet
      │   └── 回调: 传递 seed 给 AuthHandler
      │
  T2: 客户端发送 ClientAuth
      │
      ├── messageReceived (AuthHandler)
      │   ├── 解析 ClientAuth
      │   ├── 校验 seed != null
      │   ├── 校验密码 (SecurityUtil)
      │   ├── [可选] subscribe + 启动 Instance
      │   ├── 发送 ACK
      │   └── 回调: Pipeline 重构
      │       ├── 移除 HandshakeHandler
      │       ├── 移除 AuthHandler
      │       ├── 添加 IdleStateHandler
      │       └── 添加 IdleStateAwareHandler
      │
  T3: 客户端发送 SUBSCRIPTION
      │
      ├── messageReceived (SessionHandler)
      │   ├── 启动 Instance (如需)
      │   ├── embeddedServer.subscribe()
      │   └── 返回 ACK
      │
  T4: 客户端发送 GET
      │
      ├── messageReceived (SessionHandler)
      │   ├── embeddedServer.getWithoutAck()
      │   ├── Raw 模式: CodedOutputStream 手写序列化
      │   ├── 非 Raw: Protobuf Builder 序列化
      │   └── 返回 MESSAGES Packet
      │
  T5: 客户端发送 CLIENTACK
      │
      ├── messageReceived (SessionHandler)
      │   ├── batchId == 0: 报错 402
      │   ├── batchId == -1: 忽略
      │   └── batchId > 0: embeddedServer.ack()
      │
  T4-T5 循环 ...
      │
  T6: 空闲超时
      │
      ├── IdleStateHandler 检测到空闲
      │   └── 触发 IdleStateEvent
      │       └── IdleStateAwareHandler.channelIdle()
      │           └── channel.close()
      │
  T7: 连接关闭
      │
      ├── channelClosed 事件 (当前为空实现)
      └── Channel 从 ChannelGroup 中自动移除
```

### 15.4 错误码体系

| 错误码 | 使用位置 | 含义 |
|--------|----------|------|
| 400 | AuthHandler, SessionHandler | 通用错误（认证失败、内部异常、不支持的请求类型） |
| 401 | SessionHandler | 参数校验失败（destination 或 clientId 为空） |
| 402 | SessionHandler | 业务逻辑错误（batchId=0 无效） |

### 15.5 线程模型总览

```
┌──────────────────────────────────────────────────────────────────┐
│                     Canal Server 线程模型                         │
│                                                                  │
│  Netty Boss Thread (1)                                          │
│    └── 接受新连接                                                │
│                                                                  │
│  Netty Worker Threads (CPU * 2)                                 │
│    ├── 帧解码 (FixedHeaderFrameDecoder)                          │
│    ├── 握手/认证处理                                              │
│    └── 业务请求处理 (SessionHandler)                              │
│                                                                  │
│  HashedWheelTimer Thread (1)                                    │
│    └── 空闲连接超时检测                                           │
│                                                                  │
│  MQ Worker Threads (每 destination 1 个)                        │
│    └── getWithoutAck → send → ack 消费循环                      │
│                                                                  │
│  MQ Send Executor (线程池)                                      │
│    └── 多 topic 并发发送                                         │
│                                                                  │
│  MQ Build Executor (线程池)                                     │
│    └── 并发构建 EntryRowData                                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 15.6 性能优化手段汇总

| 优化手段 | 位置 | 效果 |
|----------|------|------|
| Raw 模式序列化 | SessionHandler GET | 避免反序列化+再序列化，直接透传原始字节 |
| CodedOutputStream 手写 | SessionHandler GET | 精确控制内存分配，避免 Builder 开销 |
| ChannelBuffers.wrappedBuffer | NettyUtils write | 零拷贝合并 header 和 body |
| Pipeline 动态裁剪 | AuthHandler | 认证后移除无用 Handler，减少处理链 |
| HashedWheelTimer 共享 | NettyUtils | 全局一个定时器，避免线程浪费 |
| ChannelGroup 统一管理 | CanalServerWithNetty | 快速关闭所有连接 |
| CachedThreadPool | ServerBootstrap | 按需创建线程，Canal 连接数少时节省资源 |
| TCP KeepAlive | ServerBootstrap | OS 层面检测死连接 |
| TCP NoDelay | ServerBootstrap | 禁用 Nagle，降低延迟 |
| Kafka max.in.flight=1 | CanalKafkaProducer | 保证分区内消息顺序 |
| 批量 flush | CanalKafkaProducer | 异步发送，集中 flush，提升吞吐 |

### 15.7 设计模式总结

| 设计模式 | 使用位置 | 说明 |
|----------|----------|------|
| 单例模式 | CanalServerWithNetty, CanalServerWithEmbedded, CanalServerWithNettyProfiler | 全局唯一实例，LazyHolder 实现 |
| 代理/委托模式 | CanalServerWithNetty -> CanalServerWithEmbedded | Netty 层委托 Embedded 层处理业务 |
| 策略模式 | CanalServerWithNettyProfiler + ClientInstanceProfiler | 可替换的性能监控策略 |
| 空对象模式 | DefaultClientInstanceProfiler (NOP) | 默认不做任何事的监控实现 |
| Builder 模式 | ClientRequestResult.Builder, Protobuf Builder | 构建复杂对象 |
| SPI 模式 | CanalMQProducer + @SPI 注解 | 可插拔的 MQ 实现 |
| 责任链模式 | Netty Pipeline (Handler 链) | 请求在 Handler 链中逐级处理 |
| 观察者模式 | ChannelFutureListener | I/O 完成后回调通知 |
| 工厂模式 | PipelineFactory (Lambda) | 为每个连接创建独立的 Pipeline |
| 模板方法模式 | AbstractMQProducer | 定义发送流程骨架，子类实现具体细节 |

---

## 十六、常见问题与排查

### 16.1 连接建立后立即断开

**可能原因：**

1. 认证失败（用户名/密码错误）
2. seed 为 null（Handshake 回调未执行完就收到了 Auth 包）
3. 网络层异常被 `exceptionCaught` 捕获，触发连接关闭

**排查方法：**

查看服务端日志中的错误信息，特别是 `ClientAuthenticationHandler` 中的 errorPacket 内容。

### 16.2 连接空闲超时断开

**原因：**

`IdleStateHandler` 检测到连接在 `readTimeout` 或 `writeTimeout` 时间内没有任何 I/O 操作，触发 `IdleStateEvent`，`IdleStateAwareChannelHandler` 关闭连接。

**解决方案：**

1. 客户端在 `ClientAuth` 中设置更大的 `netReadTimeout` 和 `netWriteTimeout`
2. 客户端定期发送请求保活
3. 默认超时时间为 1 小时（3600000ms）

### 16.3 GET 请求返回空数据

**判断依据：**

`message.getId() == -1` 表示没有数据。客户端 ACK batchId=-1 会被服务端忽略（donothing）。

### 16.4 MQ 消息乱序

**排查方向：**

1. 检查 Kafka 的 `max.in.flight.requests.per.connection` 是否为 1
2. 检查是否使用了 Hash 分区（相同行的变更需要路由到同一个分区）
3. 检查消费者是否使用了多线程消费（需要保证分区内单线程消费）

### 16.5 服务端停止时连接挂起

**解决方案：**

`CanalServerWithNetty.stop()` 中先关闭 serverChannel（停止接受新连接），再通过 `childGroups.close()` 显式关闭所有已建立的连接。注释中说明："close sockets explicitly to reduce socket channel hung in complicated network environment"。

---

## 十七、附录

### 附录 A：核心类关系图

```
                                ┌─────────────┐
                                │ CanalServer │ (interface)
                                │             │
                                │ + start()   │
                                │ + stop()    │
                                └──────┬──────┘
                                       │ implements
                          ┌────────────┴────────────┐
                          │                         │
              ┌───────────▼──────────┐  ┌──────────▼──────────────┐
              │CanalServerWithNetty  │  │CanalServerWithEmbedded  │
              │                      │  │                          │
              │ - embeddedServer ─────┼─>│ + auth()                │
              │ - bootstrap          │  │ + subscribe()           │
              │ - childGroups        │  │ + getWithoutAck()       │
              │ - serverChannel      │  │ + ack()                 │
              └──────────────────────┘  │ + rollback()            │
                                        └──────────────────────────┘
                                                    ▲
                                                    │ 调用
                          ┌─────────────────────────┤
                          │                         │
              ┌───────────┴──────────┐  ┌──────────┴──────────────┐
              │   SessionHandler     │  │   CanalMQStarter        │
              │   (Netty Handler)    │  │   (MQ Worker)           │
              │                      │  │                          │
              │  处理: SUB/UNSUB/    │  │  循环: getWithoutAck    │
              │  GET/ACK/ROLLBACK   │  │  → send → ack/rollback  │
              └──────────────────────┘  └──────────────────────────┘
```

### 附录 B：源码文件索引

| 文件 | 包路径 | 职责 |
|------|--------|------|
| `CanalServer.java` | `com.alibaba.otter.canal.server` | Server 生命周期接口 |
| `CanalService.java` | `com.alibaba.otter.canal.server` | 数据操作接口 |
| `CanalServerWithNetty.java` | `com.alibaba.otter.canal.server.netty` | Netty 网络层实现 |
| `FixedHeaderFrameDecoder.java` | `com.alibaba.otter.canal.server.netty.handler` | 帧解码器 |
| `HandshakeInitializationHandler.java` | `com.alibaba.otter.canal.server.netty.handler` | 握手处理器 |
| `ClientAuthenticationHandler.java` | `com.alibaba.otter.canal.server.netty.handler` | 认证处理器 |
| `SessionHandler.java` | `com.alibaba.otter.canal.server.netty.handler` | 业务处理器 |
| `NettyUtils.java` | `com.alibaba.otter.canal.server.netty` | 网络工具类 |
| `CanalServerWithNettyProfiler.java` | `com.alibaba.otter.canal.server.netty` | 性能监控单例 |
| `ClientInstanceProfiler.java` | `com.alibaba.otter.canal.server.netty` | 性能监控接口 |
| `ChannelFutureAggregator.java` | `com.alibaba.otter.canal.server.netty.listener` | I/O 回调聚合器 |
| `SecurityUtil.java` | `com.alibaba.otter.canal.protocol` | 密码安全工具 |
| `CanalPacket.java` | `com.alibaba.otter.canal.protocol` | Protobuf 协议定义 |
| `CanalMQStarter.java` | `com.alibaba.otter.canal.server` | MQ 模式启动器 |
| `CanalMQProducer.java` | `com.alibaba.otter.canal.connector.core.spi` | MQ Producer SPI 接口 |
| `CanalKafkaProducer.java` | `com.alibaba.otter.canal.connector.kafka.producer` | Kafka Producer 实现 |

### 附录 C：配置项参考

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `canal.ip` | (空) | 绑定 IP，空表示所有网卡 |
| `canal.port` | 11111 | Netty 监听端口 |
| `canal.user` | (空) | 认证用户名，空表示免认证 |
| `canal.passwd` | (空) | 认证密码哈希（SHA1(SHA1(明文))的hex） |
| `canal.serverMode` | tcp | 运行模式：tcp/kafka/rocketmq/rabbitmq/pulsarmq |
| `canal.mq.servers` | (空) | MQ 服务器地址 |
| `canal.mq.batchSize` | 50 | MQ 每次拉取的批量大小 |
| `canal.mq.fetchTimeout` | 100 | MQ 拉取超时（毫秒） |
| `canal.mq.flatMessage` | true | 是否使用 JSON 扁平格式 |
| `canal.mq.partitionHash` | (空) | Hash 分区表达式 |
| `canal.mq.partitionsNum` | 1 | 分区数 |

---

> 本文基于 Canal 1.1.x 版本源码分析，完整覆盖了 Server 网络层的 Netty 协议编解码、Handler 链、Pipeline 动态重构、安全认证、性能监控、MQ 投递等核心机制。所有分析均基于源码逐行解读，不跳步、不省略。
