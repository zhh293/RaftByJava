# Arthas Session管理 / Tunnel通信 / WebUI交互源码全流程解析

> 本文基于 Arthas 开源项目源码进行分析，源码根目录位于：
> `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/`
> 涉及的核心模块包括 `core`（主体引擎）、`tunnel-client`（隧道客户端）、`tunnel-server`（隧道服务端）、`tunnel-common`（隧道公共协议）、`web-ui`（Web前端控制台）。
> 本文力求"不跳步、不省略"，从最顶层的 TermServer 启动到最底层的 WebSocket 帧转发，每一步方法调用都追踪到底。

---

## 全局调用链路总览

在深入代码之前，先给出 Arthas Session/Tunnel/WebUI 交互的完整架构俯瞰图：

```
                                  ┌─────────────────────────────────────────────────────┐
                                  │                   Arthas Core (目标JVM内)              │
                                  │                                                     │
                                  │  ┌───────────┐    ┌────────────┐    ┌────────────┐   │
 Telnet Client ────────────────▶  │  │ProtocolDe-│───▶│TelnetChann-│───▶│TermImpl    │   │
 (arthas telnet 127.0.0.1 3658)   │  │tectHandler │    │elHandler   │    │  ┌──────┐  │   │
                                  │  └───────────┘    └────────────┘    │  │Tty   │  │   │
                                  │        │                            │  │Connec│  │   │
 Browser (WebSocket) ──────────▶  │        │──────▶HttpServerCodec ──▶  │  │tion  │  │   │
 (http://127.0.0.1:3658/ws)       │        │       BasicHttpAuth   ──▶  │  └──────┘  │   │
                                  │        │       HttpRequestHandler   │             │   │
                                  │        │       WebSocketProtocol    │             │   │
                                  │        │       TtyWebSocketFrame    │             │   │
                                  │        │       Handler              │             │   │
                                  │  ┌─────┴────────────────────────┐   │             │   │
                                  │  │  HttpTelnetTermServer        │   │             │   │
                                  │  │  (同一端口,协议自动检测)       │   │             │   │
                                  │  └─────────────────────────────┘   │             │   │
                                  │                                     │             │   │
 Browser (HTTP API) ───────────▶  │  ┌─────────────────────────────┐   │             │   │
 (POST /api)                      │  │  HttpTermServer              │   │             │   │
                                  │  │  (独立HTTP端口,无Telnet)     │   │             │   │
                                  │  │  ┌─────────────────────┐     │   │             │   │
                                  │  │  │ HttpApiHandler       │     │   │             │   │
                                  │  │  │ (REST API处理)       │     │   │             │   │
                                  │  │  └─────────────────────┘     │   │             │   │
                                  │  └─────────────────────────────┘   │             │   │
                                  │                                     │             │   │
                                  │  ┌─────────────────────────────┐   │             │   │
                                  │  │ShellServerImpl               │   │  ┌───────┐  │   │
                                  │  │ ┌──────────────────┐         │   │  │ShellI-│  │   │
                                  │  │ │TermServerTermHan │         │───┼─▶│mpl    │  │   │
                                  │  │ │dler (回调)       │         │   │  │       │  │   │
                                  │  │ └──────────────────┘         │   │  └───┬───┘  │   │
                                  │  │                               │   │      │      │   │
                                  │  │  sessions: Map<id,ShellImpl> │   │      ▼      │   │
                                  │  │  evictSessions() 定时清理     │   │ SessionImpl │   │
                                  │  └─────────────────────────────┘   │             │   │
                                  │                                     │             │   │
                                  │  ┌─────────────────────────────┐   │             │   │
                                  │  │SessionManagerImpl            │   │             │   │
                                  │  │ (HTTP API专用Session管理)    │   │             │   │
                                  │  │ sessions: Map<id,Session>    │   │             │   │
                                  │  │ 定时evict + tryLock          │   │             │   │
                                  │  └─────────────────────────────┘   │             │   │
                                  │                                     │             │   │
                                  │  ┌─────────────────────────────┐   │             │   │
                                  │  │SecurityAuthenticatorImpl     │   │             │   │
                                  │  │ (认证: 本地连接/用户名密码)  │   │             │   │
                                  │  └─────────────────────────────┘   │             │   │
                                  │                                     │             │   │
                                  │  ┌─────────────────────────────┐   │             │   │
                                  │  │HistoryManagerImpl            │   │             │   │
                                  │  │ (命令历史 最多500条)          │   │             │   │
                                  │  └─────────────────────────────┘   │             │   │
                                  │                                     │             │   │
                                  │  ┌──────────────────┐               │             │   │
                                  │  │TunnelClient       │◀─────────────┼─────────┐   │   │
                                  │  │ (WebSocket连接    │               │         │   │   │
                                  │  │  到TunnelServer)  │               │         │   │   │
                                  │  └──────┬───────────┘               │         │   │   │
                                  └─────────┼───────────────────────────┘         │   │   │
                                            │                                     │   │   │
                                            │ WebSocket (agentRegister)           │   │   │
                                            ▼                                     │   │   │
                                  ┌─────────────────────────────────────────────┐  │   │   │
                                  │            TunnelServer (独立部署)           │  │   │   │
                                  │                                             │  │   │   │
                                  │  ┌────────────────────────────┐             │  │   │   │
                                  │  │TunnelSocketFrameHandler    │             │  │   │   │
                                  │  │  agentRegister()           │             │  │   │   │
                                  │  │  connectArthas()           │             │  │   │   │
                                  │  │  openTunnel()              │             │  │   │   │
                                  │  └────────────────────────────┘             │  │   │   │
                                  │                                             │  │   │   │
                                  │  ┌────────────────────────────┐             │  │   │   │
                                  │  │agentInfoMap                │             │  │   │   │
                                  │  │  agentId -> AgentInfo      │             │  │   │   │
                                  │  │clientConnectionInfoMap     │             │  │   │   │
                                  │  │  connId -> ClientConnInfo  │             │  │   │   │
                                  │  └────────────────────────────┘             │  │   │   │
                                  │                                             │  │   │   │
                                  │  ┌────────────────────────────┐             │  │   │   │
                                  │  │TunnelClusterStore          │             │  │   │   │
                                  │  │  (Redis/InMemory集群存储)  │             │  │   │   │
                                  │  └────────────────────────────┘             │  │   │   │
                                  │                                             │  │   │   │
                                  │  ┌────────────────────────────┐             │  │   │   │
 Browser (WebUI) ────────────────▶│  │StatController/ProxyCtrl   │             │  │   │   │
 (tunnel-server提供的WebUI)       │  │  /api/tunnelAgents         │             │  │   │   │
                                  │  │  /proxy/{agentId}          │             │  │   │   │
                                  │  └────────────────────────────┘             │  │   │   │
                                  └─────────────────────────────────────────────┘  │   │   │
                                            ▲                                     │   │   │
                                            │ connectArthas (WebSocket)           │   │   │
                                            │                                     │   │   │
                                  ┌─────────┴──────────────┐                      │   │   │
                                  │  Web UI (Vue.js前端)    │                      │   │   │
                                  │  Console.vue            │                      │   │   │
                                  │  xterm.js终端模拟       │──────────────────────┘   │   │
                                  │  consoleMachine状态机   │                          │   │
                                  └────────────────────────┘                          │   │
                                                                                      │   │
                                  ┌────────────────────────────────────────────────────┘   │
                                  │  ForwardClient (Tunnel Client内部)                     │
                                  │  收到startTunnel指令后,建立第二条WebSocket连接           │
                                  │  openTunnel -> RelayHandler双向转发                     │
                                  └────────────────────────────────────────────────────────┘
```

**数据流总结**：

| 连接方式 | 入口 | 协议 | Session类型 | 管理方式 |
|---------|------|------|------------|---------|
| Telnet直连 | HttpTelnetTermServer:3658 | Telnet协议 | ShellImpl内的SessionImpl | ShellServerImpl.sessions |
| WebSocket直连 | HttpTelnetTermServer:3658 | HTTP→WS升级 | ShellImpl内的SessionImpl | ShellServerImpl.sessions |
| HTTP API直连 | HttpTermServer:8563 | HTTP POST /api | SessionManagerImpl管理 | SessionManagerImpl.sessions |
| Tunnel隧道 | TunnelServer→TunnelClient→本地WS | WebSocket转发 | 透明转发,无独立Session | TunnelServer.agentInfoMap |
| WebUI | TunnelServer HTTP页面 | HTTP + WS | 依赖Tunnel转发 | TunnelServer管理 |

---

## 第一阶段：TermServer体系 —— 终端服务器的启动与监听

TermServer 是 Arthas 接受外部连接的入口。Arthas 支持两种 TermServer：一种同时支持 Telnet 和 HTTP/WebSocket（`HttpTelnetTermServer`），另一种仅支持 HTTP（`HttpTermServer`）。两者在同一个 `ShellServer` 中注册，由 `ArthasBootstrap` 在启动时创建。

### 1.1 TermServer —— 终端服务器抽象基类

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/TermServer.java`

```java
public abstract class TermServer {

    protected Handler<Term> termHandler;

    public abstract TermServer listen(Handler<Future<TermServer>> listenHandler);

    public abstract void close();

    public abstract void close(Handler<Future<Void>> completionHandler);

    public TermServer termHandler(Handler<Term> handler) {
        this.termHandler = handler;
        return this;
    }

    public static TermServer createHttpTermServer(String hostIp, int port,
            long connectionTimeout) {
        return new HttpTermServer(hostIp, port, connectionTimeout);
    }

    public static TermServer createHttpTelnetTermServer(String hostIp,
            int port, long connectionTimeout,
            EventExecutorGroup workerGroup,
            HttpSessionManager httpSessionManager) {
        return new HttpTelnetTermServer(hostIp, port, connectionTimeout,
                workerGroup, httpSessionManager);
    }
}
```

**逐行分析**：

1. `TermServer` 是一个抽象类，不是接口。它定义了终端服务器的骨架。
2. `termHandler` 字段保存一个回调——当有新的终端连接建立时，调用这个 Handler 把 `Term` 传递给上层（即 `ShellServerImpl`）。
3. `listen()` 是抽象方法，子类负责启动 Netty 服务器并绑定端口。
4. `close()` 关闭服务器。
5. 两个静态工厂方法 `createHttpTermServer` 和 `createHttpTelnetTermServer` 是创建具体实例的入口。

**Q: 它为什么存在？**
A: TermServer 的抽象层使得 Arthas 可以在同一套 Shell 体系下接入不同协议的终端。上层的 `ShellServerImpl` 只需要调用 `registerTermServer()` 注册不同的 TermServer 实例，就能同时监听 Telnet 和 HTTP——这是一种经典的**策略模式**。

### 1.2 HttpTelnetTermServer —— 同端口复用的Telnet+HTTP服务器

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/httptelnet/HttpTelnetTermServer.java`

```java
public class HttpTelnetTermServer extends TermServer {
    private static final Logger logger = LoggerFactory
            .getLogger(HttpTelnetTermServer.class);

    private NettyHttpTelnetTtyBootstrap bootstrap;
    private String hostIp;
    private int port;
    private long connectionTimeout;
    private EventExecutorGroup workerGroup;
    private HttpSessionManager httpSessionManager;

    public HttpTelnetTermServer(String hostIp, int port,
            long connectionTimeout,
            EventExecutorGroup workerGroup,
            HttpSessionManager httpSessionManager) {
        this.hostIp = hostIp;
        this.port = port;
        this.connectionTimeout = connectionTimeout;
        this.workerGroup = workerGroup;
        this.httpSessionManager = httpSessionManager;
    }
```

**构造函数分析**：

- `hostIp`：绑定的 IP 地址，如 `127.0.0.1` 或 `0.0.0.0`。
- `port`：监听端口，默认 3658。
- `connectionTimeout`：连接超时时间（毫秒）。
- `workerGroup`：Netty 的 EventExecutorGroup，用于处理 HTTP 请求的工作线程组。
- `httpSessionManager`：HTTP Session 管理器，用于管理 HTTP 认证状态。

```java
    @Override
    public TermServer listen(Handler<Future<TermServer>> listenHandler) {
        // TODO: Charset and set binary
        bootstrap = new NettyHttpTelnetTtyBootstrap(
                workerGroup, httpSessionManager);
        bootstrap.setHost(hostIp);
        bootstrap.setPort(port);
        bootstrap.start(new Consumer<TtyConnection>() {
            @Override
            public void accept(final TtyConnection conn) {
                termHandler.handle(new TermImpl(conn));
            }
        }).get();
        listenHandler.handle(Future
                .<TermServer>succeededFuture(HttpTelnetTermServer.this));
        return this;
    }
```

**逐行分析**：

1. 创建 `NettyHttpTelnetTtyBootstrap` 实例——这是 Netty 服务器的启动器。
2. 设置主机和端口。
3. 调用 `bootstrap.start()` 启动服务器，传入一个 `Consumer<TtyConnection>` 回调。
4. 当有新连接到来时（无论是 Telnet 还是 HTTP/WebSocket），底层会创建一个 `TtyConnection`，然后回调这个 Consumer。
5. 在回调中，把 `TtyConnection` 包装成 `TermImpl`，再通过 `termHandler.handle()` 传递给 `ShellServerImpl`。
6. `listenHandler` 通知上层监听成功。

**类比理解**：`HttpTelnetTermServer` 就像一个"前台接待员"。无论来的是打电话（Telnet）还是上门（HTTP）的客户，它都接待，然后把客户引导到对应的"服务窗口"（ShellImpl）。

```java
    @Override
    public void close(Handler<Future<Void>> completionHandler) {
        if (bootstrap != null) {
            bootstrap.stop().get();
        }
        completionHandler.handle(Future.<Void>succeededFuture());
    }
```

关闭逻辑很简单：停止 Netty Bootstrap，通知上层关闭完成。

### 1.3 NettyHttpTelnetTtyBootstrap —— Netty启动引导器

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/httptelnet/NettyHttpTelnetTtyBootstrap.java`

```java
public class NettyHttpTelnetTtyBootstrap {

    private final NettyHttpTelnetBootstrap httpTelnetTtyBootstrap;
    private boolean outBinary;
    private boolean inBinary;
    private Charset charset = Charset.forName("UTF-8");

    public NettyHttpTelnetTtyBootstrap(
            EventExecutorGroup workerGroup,
            HttpSessionManager httpSessionManager) {
        this.httpTelnetTtyBootstrap = new NettyHttpTelnetBootstrap(
                workerGroup, httpSessionManager);
    }
```

这是一个中间桥接层，负责：
1. 设置 Telnet 协议的二进制模式（`outBinary` / `inBinary`）和字符编码。
2. 将 `Consumer<TtyConnection>` 回调传递给底层的 `NettyHttpTelnetBootstrap`。

```java
    public void start(final Consumer<TtyConnection> factory,
                      Consumer<Throwable> doneHandler) {
        httpTelnetTtyBootstrap.start(new Supplier<TelnetHandler>() {
            @Override
            public TelnetHandler get() {
                return new TelnetTtyConnection(
                        inBinary, outBinary, charset, factory);
            }
        }, factory, doneHandler);
    }
```

**关键点**：这里传了两个回调给 `NettyHttpTelnetBootstrap`：
1. `Supplier<TelnetHandler>`：为每个 Telnet 连接创建一个 `TelnetTtyConnection`（处理 Telnet 协议细节，最终回调 `factory`）。
2. `factory`（即 `Consumer<TtyConnection>`）：为 HTTP/WebSocket 连接直接使用。

这种双回调设计是因为 Telnet 和 HTTP 连接走不同的 Netty Pipeline，但最终都要产生 `TtyConnection`。

### 1.4 ProtocolDetectHandler —— 协议自动检测（核心设计亮点）

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/httptelnet/ProtocolDetectHandler.java`

```java
public class ProtocolDetectHandler
        extends ChannelInboundHandlerAdapter {
    private ChannelGroup channelGroup;
    private Supplier<TelnetHandler> handlerFactory;
    private Consumer<TtyConnection> ttyConnectionFactory;
    private EventExecutorGroup workerGroup;
    private HttpSessionManager httpSessionManager;
```

**Q: 为什么 Arthas 能在同一个端口同时支持 Telnet 和 HTTP？**

A: 答案就在 `ProtocolDetectHandler` 中。它通过**嗅探连接的前几个字节**来判断协议类型。

```java
    @Override
    public void channelActive(final ChannelHandlerContext ctx)
            throws Exception {
        detectTelnetFuture = ctx.channel().eventLoop()
                .schedule(new Runnable() {
            @Override
            public void run() {
                channelGroup.add(ctx.channel());
                TelnetChannelHandler handler =
                        new TelnetChannelHandler(handlerFactory);
                ChannelPipeline pipeline = ctx.pipeline();
                pipeline.addLast(handler);
                pipeline.remove(ProtocolDetectHandler.this);
                ctx.fireChannelActive();
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }
```

**channelActive 的策略**：
1. 当连接建立时，启动一个 **1秒延时任务**。
2. 如果 1 秒内没有收到任何数据，就**默认认为是 Telnet 连接**（因为 Telnet 客户端连接后通常等待服务端发送 IAC 协商命令）。
3. 添加 `TelnetChannelHandler` 到 Pipeline，移除自身。

```java
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        ByteBuf in = (ByteBuf) msg;
        if (in.readableBytes() < 3) {
            return;
        }

        if (detectTelnetFuture != null
                && detectTelnetFuture.isCancellable()) {
            detectTelnetFuture.cancel(false);
        }

        byte[] bytes = new byte[3];
        in.getBytes(0, bytes);
        String httpHeader = new String(bytes);

        ChannelPipeline pipeline = ctx.pipeline();
        if (!"GET".equalsIgnoreCase(httpHeader)) { // telnet
            channelGroup.add(ctx.channel());
            TelnetChannelHandler handler =
                    new TelnetChannelHandler(handlerFactory);
            pipeline.addLast(handler);
            ctx.fireChannelActive();
        } else {
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new ChunkedWriteHandler());
            pipeline.addLast(new HttpObjectAggregator(
                    ArthasConstants.MAX_HTTP_CONTENT_LENGTH));
            pipeline.addLast(new BasicHttpAuthenticatorHandler(
                    httpSessionManager));
            pipeline.addLast(workerGroup, "HttpRequestHandler",
                    new HttpRequestHandler(
                            ArthasConstants.DEFAULT_WEBSOCKET_PATH));
            pipeline.addLast(new WebSocketServerProtocolHandler(
                    ArthasConstants.DEFAULT_WEBSOCKET_PATH,
                    null, false,
                    ArthasConstants.MAX_HTTP_CONTENT_LENGTH,
                    false, true));
            pipeline.addLast(new IdleStateHandler(
                    0, ArthasConstants.WEBSOCKET_IDLE_SECONDS, 0));
            pipeline.addLast(new TtyWebSocketFrameHandler(
                    channelGroup, ttyConnectionFactory));
            ctx.fireChannelActive();
        }
        pipeline.remove(this);
        ctx.fireChannelRead(in);
    }
```

**channelRead 的协议检测算法**：

1. 等待至少 3 个字节可读。
2. 取消之前的 Telnet 延时任务（因为已经收到数据了）。
3. 读取前 3 个字节，判断是否为 `"GET"`。
4. **如果不是 `"GET"`**：认为是 Telnet 协议，添加 `TelnetChannelHandler`。
5. **如果是 `"GET"`**：认为是 HTTP 协议，构建完整的 HTTP Pipeline：

| Pipeline 层次 | Handler | 作用 |
|-------------|---------|------|
| 第1层 | HttpServerCodec | HTTP 编解码器 |
| 第2层 | ChunkedWriteHandler | 支持分块传输 |
| 第3层 | HttpObjectAggregator | 聚合 HTTP 消息 |
| 第4层 | BasicHttpAuthenticatorHandler | HTTP Basic 认证 |
| 第5层 | HttpRequestHandler | 静态资源 + API 路由 |
| 第6层 | WebSocketServerProtocolHandler | WebSocket 协议升级 |
| 第7层 | IdleStateHandler | 空闲检测 |
| 第8层 | TtyWebSocketFrameHandler | WebSocket 终端帧处理 |

**这一步做了什么？** 通过检查网络流量的前几个字节，Arthas 实现了在单一端口上自动区分 Telnet 和 HTTP/WebSocket 协议。这是一种非常巧妙的设计——用户只需要记住一个端口号。

### 1.5 HttpTermServer —— 独立HTTP服务器

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/HttpTermServer.java`

```java
public class HttpTermServer extends TermServer {
    private static final Logger logger =
            LoggerFactory.getLogger(HttpTermServer.class);

    private String hostIp;
    private int port;
    private long connectionTimeout;
    private NettyWebsocketTtyBootstrap bootstrap;

    public HttpTermServer(String hostIp, int port,
            long connectionTimeout) {
        this.hostIp = hostIp;
        this.port = port;
        this.connectionTimeout = connectionTimeout;
    }
```

与 `HttpTelnetTermServer` 不同的是，`HttpTermServer` **只支持 HTTP/WebSocket**，不支持 Telnet。它通常监听在另一个端口（默认 8563），专门提供 HTTP API 服务。

```java
    @Override
    public TermServer listen(
            Handler<Future<TermServer>> listenHandler) {
        bootstrap = new NettyWebsocketTtyBootstrap(workerGroup);
        bootstrap.setHost(hostIp);
        bootstrap.setPort(port);
        bootstrap.start(new Consumer<TtyConnection>() {
            @Override
            public void accept(final TtyConnection conn) {
                termHandler.handle(new TermImpl(conn));
            }
        }).get();
        listenHandler.handle(Future
                .<TermServer>succeededFuture(HttpTermServer.this));
        return this;
    }
```

**Q: 既然 HttpTelnetTermServer 已经支持 HTTP 了，为什么还需要独立的 HttpTermServer？**

A: 这是为了不同的使用场景。`HttpTelnetTermServer`（默认端口 3658）是面向交互式终端的，支持 Telnet 回退。`HttpTermServer`（默认端口 8563）是面向程序化调用的 HTTP API 服务，两者可以绑定不同的 IP/端口，有不同的安全策略。在某些场景下，用户可能只想暴露 HTTP API 而不想暴露 Telnet。

---

## 第二阶段：Term —— 终端抽象层

Term 是 Arthas 对"终端"这个概念的抽象。无论用户通过 Telnet、WebSocket 还是 HTTP API 连接，上层的 Shell 都通过 `Term` 接口来读写数据。

### 2.1 Tty —— 最底层的TTY接口

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/Tty.java`

```java
public interface Tty {

    String type();

    int width();

    int height();

    Term stdinHandler(Handler<String> handler);

    Term stdoutHandler(Function<String, String> handler);

    Term write(String data);
}
```

`Tty` 定义了终端的最基本能力：
- `type()`：终端类型，如 `"vt100"`、`"xterm"` 等。
- `width()` / `height()`：终端窗口尺寸（字符数）。
- `stdinHandler()`：注册标准输入处理器。
- `stdoutHandler()`：注册标准输出过滤器。
- `write()`：向终端写入数据。

### 2.2 SignalHandler —— 信号处理器

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/SignalHandler.java`

```java
public interface SignalHandler {
    void handle(int signal);
}
```

极其简洁的信号处理器接口。信号值的含义在 Term 接口的上下文中定义（如 Ctrl+C 中断、Ctrl+Z 挂起）。

### 2.3 Term —— 终端接口（核心）

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/Term.java`

```java
public interface Term extends Tty {

    Term resizehandler(Handler<Void> handler);

    Term stdinHandler(Handler<String> handler);

    Term stdoutHandler(Function<String, String> handler);

    Term write(String data);

    long lastAccessedTime();

    Term echo(String text);

    Term setSession(Session session);

    Term interruptHandler(SignalHandler handler);

    Term suspendHandler(SignalHandler handler);

    void readline(String prompt, Handler<String> lineHandler);

    void readline(String prompt, Handler<String> lineHandler,
            Handler<Completion> completionHandler);

    Term closeHandler(Handler<Void> handler);

    void close();
}
```

`Term` 继承 `Tty`，并增加了以下能力：

| 方法 | 作用 |
|-----|------|
| `resizehandler()` | 注册窗口大小变化回调 |
| `lastAccessedTime()` | 获取最后访问时间（用于Session超时判断） |
| `echo()` | 回显文本 |
| `setSession()` | 关联Session |
| `interruptHandler()` | 注册中断信号处理（Ctrl+C） |
| `suspendHandler()` | 注册挂起信号处理（Ctrl+Z） |
| `readline()` | 行读取（带提示符，支持Tab补全） |
| `closeHandler()` | 注册关闭回调 |
| `close()` | 关闭终端 |

**Q: 为什么 Term 接口如此重要？**

A: Term 是整个 Arthas 交互体系的"契约"。ShellImpl 通过 Term 来：
1. 显示提示符并读取命令（`readline`）。
2. 输出命令结果（`write`）。
3. 处理 Ctrl+C 中断（`interruptHandler`）。
4. 判断会话是否超时（`lastAccessedTime`）。

不同的连接方式（Telnet/WebSocket/HTTP API）提供不同的 Term 实现，但上层 Shell 代码完全相同。

### 2.4 TermImpl —— Term的通用实现

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/TermImpl.java`

```java
public class TermImpl implements Term {
    private static final Logger logger =
            LoggerFactory.getLogger(TermImpl.class);

    private Readline readline;
    private Consumer<int[]> echoHandler;
    private TtyConnection conn;
    private volatile long lastAccessedTime =
            System.currentTimeMillis();
    private Session session;
```

**核心字段**：

- `readline`：行编辑器，支持方向键导航、历史命令、Tab 补全。
- `conn`：底层的 `TtyConnection`——可能是 `TelnetTtyConnection`（Telnet连接）或 `ExtHttpTtyConnection`（WebSocket连接）。
- `lastAccessedTime`：最后访问时间戳，使用 `volatile` 保证多线程可见性。
- `session`：关联的 Arthas Session。

```java
    public TermImpl(TtyConnection conn) {
        this.conn = conn;
        readline = new Readline(getKeymap());
        echoHandler = new DefaultTermStdinHandler(this);
        conn.setStdinHandler(echoHandler);
        conn.setSizeHandler(new SizeHandlerWrapper(this));
    }
```

**构造函数做了什么**：
1. 保存底层连接引用。
2. 创建 `Readline` 实例——这是一个功能强大的行编辑器，支持 Emacs 风格的快捷键。
3. 设置默认的 stdin 处理器 `DefaultTermStdinHandler`，它负责回显输入字符。
4. 设置窗口大小变化处理器。

```java
    @Override
    public void readline(String prompt,
            final Handler<String> lineHandler,
            final Handler<Completion> completionHandler) {
        if (conn.getStdinHandler() != echoHandler) {
            conn.setStdinHandler(echoHandler);
        }
        readline.readline(conn, prompt,
                new RequestHandler(this, lineHandler),
                new CompletionHandler(completionHandler, session));
    }
```

`readline` 方法是用户交互的核心入口。它：
1. 确保 stdin 处理器恢复为默认的回显处理器。
2. 调用 `Readline.readline()`，在终端上显示提示符并等待用户输入。
3. 用户按回车后，`RequestHandler` 被调用，将输入传递给 `lineHandler`。
4. 用户按 Tab 键时，`CompletionHandler` 提供命令补全。

```java
    @Override
    public Term write(String data) {
        try {
            conn.write(data);
        } catch (Throwable t) {
            logger.debug("Write data error, msg: {}", t.getMessage());
        }
        lastAccessedTime = System.currentTimeMillis();
        return this;
    }
```

**write 方法**：将数据写入底层连接，并更新 `lastAccessedTime`。这个时间戳被 Session 超时机制使用——只要有数据输出，会话就不会被认为是"空闲"的。

```java
    @Override
    public void close() {
        conn.close();
    }
```

关闭终端就是关闭底层的 `TtyConnection`。

```java
    public TtyConnection getConn() {
        return conn;
    }

    public Readline getReadline() {
        return readline;
    }
```

这两个 getter 方法暴露了内部细节，被 `ShellImpl` 用来：
- 获取 `conn` 判断连接类型（Telnet 还是 WebSocket），以决定认证方式。
- 获取 `readline` 来保存命令历史。

### 2.5 ExtHttpTtyConnection —— WebSocket终端连接

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/ExtHttpTtyConnection.java`

```java
public class ExtHttpTtyConnection extends HttpTtyConnection {
    private ChannelHandlerContext context;
    private final boolean quiet;

    public ExtHttpTtyConnection(ChannelHandlerContext context) {
        this(context, false);
    }

    public ExtHttpTtyConnection(ChannelHandlerContext context,
            boolean quiet) {
        this.context = context;
        this.quiet = quiet;
    }
```

`ExtHttpTtyConnection` 是 Arthas 对 termd 库中 `HttpTtyConnection` 的扩展。"Ext"代表"Extended"（扩展）。

```java
    @Override
    protected void write(byte[] buffer) {
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(buffer);
        if (context != null) {
            context.writeAndFlush(
                    new TextWebSocketFrame(byteBuf));
        }
    }
```

**核心方法 write**：将字节数组包装成 `TextWebSocketFrame`（WebSocket 文本帧），通过 Netty 的 `ChannelHandlerContext` 发送给浏览器。

```java
    public Map<String, Object> extSessions() {
        Map<String, Object> result =
                new HashMap<String, Object>();
        if (quiet) {
            result.put(Session.QUIET, Boolean.TRUE);
        }
        if (context != null) {
            HttpSession httpSession =
                    HttpSessionManager.getHttpSessionFromContext(
                            context);
            if (httpSession != null) {
                Object subject = httpSession.getAttribute(
                        ArthasConstants.SUBJECT_KEY);
                if (subject != null) {
                    result.put(ArthasConstants.SUBJECT_KEY,
                            subject);
                }
                Object userId = httpSession.getAttribute(
                        ArthasConstants.USER_ID_KEY);
                if (userId != null) {
                    result.put(ArthasConstants.USER_ID_KEY,
                            userId);
                }
            }
        }
        if (!result.isEmpty()) {
            return result;
        }
        return Collections.emptyMap();
    }
```

**extSessions 方法**：从 HTTP Session 中提取认证信息（`Subject` 和 `userId`），传递给 Arthas 的 Shell Session。这是 HTTP 认证信息从 Netty Pipeline 传递到 Arthas Session 的桥梁。

### 2.6 TtyWebSocketFrameHandler —— WebSocket帧处理器

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/TtyWebSocketFrameHandler.java`

这个处理器是 WebSocket 连接的入口，它在 Netty Pipeline 中位于 `WebSocketServerProtocolHandler` 之后。

当 WebSocket 握手完成后，它创建一个 `ExtHttpTtyConnection`，并调用 `ttyConnectionFactory`（即 `Consumer<TtyConnection>` 回调），最终创建 `TermImpl` 并传递给 `ShellServerImpl`。

当收到 WebSocket 帧时，它将文本内容传递给 `ExtHttpTtyConnection` 的 stdin 处理器，实现用户输入到 Arthas 命令的转换。

---

## 第三阶段：ShellServer体系 —— Shell 服务器与会话管理

### 3.1 ShellServer —— Shell 服务器抽象

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/ShellServer.java`

```java
public abstract class ShellServer {

    public abstract ShellServer registerTermServer(
            TermServer termServer);

    public abstract ShellServer registerCommandResolver(
            CommandResolver resolver);

    public abstract ShellServer listen(
            Handler<Future<Void>> listenHandler);

    public abstract void close(
            Handler<Future<Void>> completionHandler);

    public abstract Shell createShell();

    public abstract Shell createShell(Term term);

    public static ShellServer create() {
        return new ShellServerImpl(
                new ShellServerOptions());
    }

    public static ShellServer create(
            ShellServerOptions options) {
        return new ShellServerImpl(options);
    }
}
```

`ShellServer` 定义了 Shell 服务器的生命周期：注册终端服务器 → 注册命令 → 开始监听 → 创建 Shell → 关闭。

### 3.2 ShellServerOptions —— 服务器配置项

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/ShellServerOptions.java`

```java
public class ShellServerOptions {
    public static final long DEFAULT_SESSION_TIMEOUT = 1000 * 60 * 30;
    public static final long DEFAULT_REAPER_INTERVAL = 1000 * 60;

    private String welcomeMessage;
    private long sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    private long reaperInterval = DEFAULT_REAPER_INTERVAL;
    private Instrumentation instrumentation;
    private long pid;
```

关键配置项说明：

| 配置项 | 默认值 | 说明 |
|-------|--------|------|
| `sessionTimeout` | 30分钟 | 会话空闲超时时间 |
| `reaperInterval` | 1分钟 | 会话清理定时任务的执行间隔 |
| `welcomeMessage` | Arthas Banner | 连接时显示的欢迎信息 |
| `instrumentation` | - | JVM Instrumentation 实例 |
| `pid` | - | 目标 JVM 进程 ID |

### 3.3 ShellServerImpl —— Shell服务器实现（核心）

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/impl/ShellServerImpl.java`

```java
public class ShellServerImpl extends ShellServer {

    private static final Logger logger =
            LoggerFactory.getLogger(ShellServerImpl.class);

    private final CopyOnWriteArrayList<CommandResolver> resolvers;
    private final InternalCommandManager commandManager;
    private final List<TermServer> termServers;
    private final long timeoutMillis;
    private final long reaperInterval;
    private String welcomeMessage;
    private Instrumentation instrumentation;
    private long pid;
    private boolean closed = true;
    private final Map<String, ShellImpl> sessions;
    private final Future<Void> sessionsClosed = Future.future();
    private ScheduledExecutorService scheduledExecutorService;
    private JobControllerImpl jobController =
            new GlobalJobControllerImpl();
```

**核心字段解析**：

- `resolvers`：命令解析器列表，使用 `CopyOnWriteArrayList` 保证线程安全。
- `termServers`：注册的终端服务器列表。
- `sessions`：所有活跃的 Shell 会话，key 是 UUID。使用 `ConcurrentHashMap` 保证线程安全。
- `jobController`：全局 Job 控制器，管理所有会话的任务。
- `scheduledExecutorService`：定时任务执行器，用于清理超时会话。

```java
    @Override
    public ShellServer listen(
            final Handler<Future<Void>> listenHandler) {
        final List<TermServer> toStart;
        synchronized (this) {
            if (!closed) {
                throw new IllegalStateException(
                        "Server listening");
            }
            toStart = termServers;
        }
        final AtomicInteger count =
                new AtomicInteger(toStart.size());
        if (count.get() == 0) {
            setClosed(false);
            listenHandler.handle(
                    Future.<Void>succeededFuture());
            return this;
        }
        Handler<Future<TermServer>> handler =
                new TermServerListenHandler(
                        this, listenHandler, toStart);
        for (TermServer termServer : toStart) {
            termServer.termHandler(
                    new TermServerTermHandler(this));
            termServer.listen(handler);
        }
        return this;
    }
```

**listen 方法的启动流程**：

1. 检查服务器未在运行中。
2. 遍历所有注册的 TermServer。
3. 为每个 TermServer 设置 `termHandler`——即 `TermServerTermHandler`。
4. 调用每个 TermServer 的 `listen()` 方法启动监听。
5. 当所有 TermServer 都启动成功后，`TermServerListenHandler` 通知上层。

### 3.4 TermServerTermHandler —— 新连接处理器

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/handlers/server/TermServerTermHandler.java`

```java
public class TermServerTermHandler implements Handler<Term> {
    private ShellServerImpl shellServer;

    public TermServerTermHandler(ShellServerImpl shellServer) {
        this.shellServer = shellServer;
    }

    @Override
    public void handle(Term term) {
        shellServer.handleTerm(term);
    }
}
```

极其简洁——当新的终端连接建立时，将 `Term` 传递给 `ShellServerImpl.handleTerm()`。

### 3.5 ShellServerImpl.handleTerm —— 新会话创建流程

```java
    public void handleTerm(Term term) {
        synchronized (this) {
            if (closed) {
                term.close();
                return;
            }
        }

        ShellImpl session = createShell(term);
        tryUpdateWelcomeMessage();
        session.setWelcome(welcomeMessage);
        session.closedFuture.setHandler(
                new SessionClosedHandler(this, session));
        session.init();
        sessions.put(session.id, session);
        session.readline();
    }
```

**逐步分析**：

1. **检查服务器状态**：如果服务器已关闭，直接关闭终端连接。
2. **创建 ShellImpl**：这是一个新的会话实例，包含 Term、Session、命令管理器等。
3. **更新欢迎信息**：如果配置了 TunnelClient，把 agentId 加入欢迎信息。
4. **注册关闭回调**：当 Shell 关闭时，`SessionClosedHandler` 负责清理。
5. **初始化 Shell**：设置中断处理器、挂起处理器、关闭处理器，显示欢迎信息。
6. **存入 sessions Map**：注意是在 init 之后才 put，确保关闭处理器已经设置好。
7. **开始读取命令**：调用 `session.readline()` 显示提示符，等待用户输入。

```java
    private void tryUpdateWelcomeMessage() {
        TunnelClient tunnelClient =
                ArthasBootstrap.getInstance().getTunnelClient();
        if (tunnelClient != null) {
            String id = tunnelClient.getId();
            if (id != null) {
                Map<String, String> welcomeInfos =
                        new HashMap<String, String>();
                welcomeInfos.put("id", id);
                this.welcomeMessage =
                        ArthasBanner.welcome(welcomeInfos);
            }
        }
    }
```

如果 Arthas 配置了 Tunnel Server，欢迎信息中会包含 agentId，方便用户通过 Tunnel 连接。

### 3.6 会话超时清理机制

```java
    private void evictSessions() {
        long now = System.currentTimeMillis();
        Set<ShellImpl> toClose = new HashSet<ShellImpl>();
        for (ShellImpl session : sessions.values()) {
            if (now - session.lastAccessedTime() > timeoutMillis
                    && session.jobs().size() == 0) {
                toClose.add(session);
            }
        }
        for (ShellImpl session : toClose) {
            long timeOutInMinutes = timeoutMillis / 1000 / 60;
            String reason = "session is inactive for "
                    + timeOutInMinutes + " min(s).";
            session.close(reason);
        }
    }
```

**超时清理策略**：

1. 每隔 `reaperInterval`（默认1分钟）执行一次。
2. 遍历所有会话，检查 `lastAccessedTime` 是否超过 `timeoutMillis`（默认30分钟）。
3. **额外条件**：只有当会话没有运行中的 Job 时才会被清理。这个设计非常重要——如果用户执行了 `trace` 命令在等待匹配条件，这个会话不会被清理。
4. 关闭时会向终端写入原因信息，让用户知道会话为什么被关闭。

```java
    public synchronized void setTimer() {
        if (!closed && reaperInterval > 0) {
            scheduledExecutorService =
                    Executors.newSingleThreadScheduledExecutor(
                            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    final Thread t = new Thread(r,
                            "arthas-shell-server");
                    t.setDaemon(true);
                    return t;
                }
            });
            scheduledExecutorService
                    .scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    evictSessions();
                }
            }, 0, reaperInterval, TimeUnit.MILLISECONDS);
        }
    }
```

定时器使用**守护线程**，不会阻止 JVM 关闭。

---

## 第四阶段：ShellImpl与Session —— 会话的内部结构

### 4.1 Shell —— Shell接口

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/Shell.java`

```java
public interface Shell {

    Job createJob(List<CliToken> args);

    Job createJob(String line);

    Session session();

    void close(String reason);
}
```

Shell 接口定义了命令执行（`createJob`）、会话获取（`session`）和关闭（`close`）三个核心操作。

### 4.2 ShellImpl —— Shell实现（会话核心）

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/impl/ShellImpl.java`

```java
public class ShellImpl implements Shell {
    private static final String ARTHAS_AGENT_TERMINAL_TYPE =
            "arthas-agent";

    private JobControllerImpl jobController;
    final String id;
    final Future<Void> closedFuture;
    private InternalCommandManager commandManager;
    private Session session = new SessionImpl();
    private Term term;
    private String welcome;
    private Job currentForegroundJob;
    private String prompt;
```

**核心字段**：

- `id`：会话 UUID，全局唯一。
- `session`：`SessionImpl` 实例，存储会话级别的数据。
- `term`：终端连接。
- `currentForegroundJob`：当前前台运行的 Job。
- `prompt`：命令提示符，格式为 `[arthas@<pid>]$ `。

```java
    public ShellImpl(ShellServer server, Term term,
            InternalCommandManager commandManager,
            Instrumentation instrumentation, long pid,
            JobControllerImpl jobController) {
        if (term instanceof TermImpl) {
            TermImpl termImpl = (TermImpl) term;
            TtyConnection conn = termImpl.getConn();
            // 处理telnet本地连接鉴权
            if (conn instanceof TelnetTtyConnection) {
                TelnetConnection telnetConnection =
                        ((TelnetTtyConnection) conn)
                                .getTelnetConnection();
                if (telnetConnection
                        instanceof NettyTelnetConnection) {
                    ChannelHandlerContext handlerContext =
                            ((NettyTelnetConnection)
                                    telnetConnection)
                                    .channelHandlerContext();
                    Principal principal =
                            AuthUtils.localPrincipal(
                                    handlerContext);
                    if (principal != null) {
                        try {
                            SecurityAuthenticator auth =
                                    ArthasBootstrap
                                            .getInstance()
                                            .getSecurityAuthenticator();
                            Subject subject =
                                    auth.login(principal);
                            if (subject != null) {
                                session.put(
                                        ArthasConstants
                                                .SUBJECT_KEY,
                                        subject);
                            }
                        } catch (LoginException e) {
                            logger.error(
                                    "local connection "
                                    + "auth error", e);
                        }
                    }
                }
            }

            if (conn instanceof ExtHttpTtyConnection) {
                ExtHttpTtyConnection extConn =
                        (ExtHttpTtyConnection) conn;
                Map<String, Object> extSessions =
                        extConn.extSessions();
                for (Entry<String, Object> entry
                        : extSessions.entrySet()) {
                    session.put(entry.getKey(),
                            entry.getValue());
                }
            }
        }
        if (term != null && ARTHAS_AGENT_TERMINAL_TYPE
                .equalsIgnoreCase(term.type())) {
            session.put(Session.QUIET, Boolean.TRUE);
        }
        session.put(Session.COMMAND_MANAGER, commandManager);
        session.put(Session.INSTRUMENTATION, instrumentation);
        session.put(Session.PID, pid);
        session.put(Session.SERVER, server);
        session.put(Session.TTY, term);
        this.id = UUID.randomUUID().toString();
        session.put(Session.ID, id);
        this.commandManager = commandManager;
        this.closedFuture = Future.future();
        this.term = term;
        this.jobController = jobController;

        if (term != null) {
            term.setSession(session);
        }
        this.setPrompt();
    }
```

**构造函数做了大量工作**：

1. **Telnet 本地连接鉴权**：通过层层解包 (`TermImpl` → `TelnetTtyConnection` → `NettyTelnetConnection` → `ChannelHandlerContext`)，获取连接的来源 IP，判断是否是本地连接。如果是，自动创建一个本地 Principal 并登录。

2. **WebSocket 认证信息传递**：如果是 HTTP/WebSocket 连接，从 `ExtHttpTtyConnection` 的 `extSessions()` 中获取 HTTP Session 中的认证信息，传递到 Arthas Session。

3. **静默模式**：如果终端类型是 `"arthas-agent"`，设置 `QUIET` 模式，不显示欢迎信息和提示符。

4. **Session 初始化**：将各种全局对象（CommandManager、Instrumentation、PID 等）放入 Session。

```java
    public ShellImpl init() {
        term.interruptHandler(new InterruptHandler(this));
        term.suspendHandler(new SuspendHandler(this));
        term.closeHandler(new CloseHandler(this));

        if (!isQuietSession()
                && welcome != null
                && welcome.length() > 0) {
            term.write(welcome + "\n");
        }
        return this;
    }
```

**init 方法**：
1. 注册 Ctrl+C 中断处理器（`InterruptHandler`）。
2. 注册 Ctrl+Z 挂起处理器（`SuspendHandler`）。
3. 注册连接关闭处理器（`CloseHandler`）。
4. 如果不是静默会话，显示欢迎信息。

```java
    public void readline() {
        term.readline(prompt,
                new ShellLineHandler(this),
                new CommandManagerCompletionHandler(
                        commandManager));
    }
```

**readline 方法**：显示提示符 `[arthas@<pid>]$ `，等待用户输入。`ShellLineHandler` 处理用户输入的命令行，`CommandManagerCompletionHandler` 提供 Tab 补全。

### 4.3 Session —— 会话接口

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/session/Session.java`

```java
public interface Session {
    String ID = "id";
    String SERVER = "server";
    String TTY = "tty";
    String PID = "pid";
    String INSTRUMENTATION = "instrumentation";
    String COMMAND_MANAGER = "commandManager";
    String QUIET = "quiet";

    Session put(String key, Object obj);
    <T> T get(String key);
    <T> T remove(String key);

    String getSessionId();
    boolean tryLock();
    void unLock();
    int getLock();
    long getPid();
    long getCreateTime();
    long getLastAccessTime();
    void setLastAccessTime(long lastAccessTime);
    String getUserId();
    void setUserId(String userId);

    Job getForegroundJob();
    void setForegroundJob(Job job);
    SharingResultDistributor getResultDistributor();
    void setResultDistributor(
            SharingResultDistributor resultDistributor);
}
```

Session 是一个 **键值对容器**（类似 Servlet 的 HttpSession），同时提供了：

| 能力 | 方法 | 说明 |
|------|------|------|
| 键值存储 | `put()` / `get()` / `remove()` | 存储任意会话数据 |
| 并发控制 | `tryLock()` / `unLock()` / `getLock()` | 防止同一会话并发执行命令 |
| 时间管理 | `getCreateTime()` / `getLastAccessTime()` | 用于超时判断 |
| Job管理 | `getForegroundJob()` / `setForegroundJob()` | 管理前台任务 |
| 结果分发 | `getResultDistributor()` | HTTP API 模式的结果分发器 |

### 4.4 SessionImpl —— Session实现

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/session/impl/SessionImpl.java`

```java
public class SessionImpl implements Session {
    private Map<String, Object> data =
            new HashMap<String, Object>();
    private final String sessionId =
            UUID.randomUUID().toString();
    private final long createTime = System.currentTimeMillis();
    private volatile long lastAccessTime =
            System.currentTimeMillis();
    private volatile int lock = 0;
    private volatile Job foregroundJob;
    private volatile SharingResultDistributor
            resultDistributor;
    private volatile String userId;
```

**关键设计点**：

1. `sessionId` 使用 `UUID.randomUUID()` 生成，保证全局唯一。
2. `lock` 是一个简单的整数锁，使用 `volatile` 保证可见性。
3. `foregroundJob` 记录当前正在前台运行的 Job，防止并发执行。

```java
    @Override
    public boolean tryLock() {
        lock++;
        return true;
    }

    @Override
    public void unLock() {
        lock--;
    }

    @Override
    public int getLock() {
        return lock;
    }
```

**Q: 这个 tryLock 真的能防止并发吗？**

A: 严格来说，这个 `tryLock` 实现并不是真正的互斥锁。`lock++` 不是原子操作，在多线程环境下可能有问题。但在 Arthas 的实际使用中，HTTP API 请求是串行处理的（同一个 Session 同一时间只应有一个请求），所以这个简单实现在实践中是够用的。这更多是一个"逻辑锁"而非"并发锁"——它的主要目的是防止用户在一个命令还没执行完时提交另一个命令。

```java
    @Override
    public Session put(String key, Object obj) {
        if (obj == null) {
            data.remove(key);
        } else {
            data.put(key, obj);
        }
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) data.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T remove(String key) {
        return (T) data.remove(key);
    }
```

键值对存储的实现简单直接。注意 `put(key, null)` 等同于 `remove(key)`。

### 4.5 SessionManager —— HTTP API 的会话管理

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/session/SessionManager.java`

```java
public interface SessionManager {

    Session createSession();

    Session getSession(String sessionId);

    Session removeSession(String sessionId);

    void updateAccessTime(Session session);

    void close();

    InternalCommandManager getCommandManager();

    JobController getJobController();
}
```

**Q: 为什么 ShellServerImpl 和 SessionManager 都管理 Session？**

A: 这是两套不同的 Session 管理体系：
- `ShellServerImpl` 管理的是 **交互式 Shell 会话**（Telnet/WebSocket连接），每个连接对应一个 `ShellImpl`，Shell 关闭时会话结束。
- `SessionManager` 管理的是 **HTTP API 会话**，通过 `init_session` 创建，通过 `close_session` 销毁，没有持久连接。

| 对比维度 | ShellServerImpl | SessionManager |
|---------|----------------|----------------|
| 适用场景 | Telnet/WebSocket 交互 | HTTP API 调用 |
| 连接模型 | 长连接 | 无连接（请求-响应） |
| 生命周期 | 连接断开即结束 | 需要显式关闭 |
| 输出方式 | 直接写终端 | 结果放入队列，客户端拉取 |

### 4.6 SessionManagerImpl —— SessionManager实现

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/session/impl/SessionManagerImpl.java`

```java
public class SessionManagerImpl implements SessionManager {
    private static final Logger logger =
            LoggerFactory.getLogger(SessionManagerImpl.class);

    private InternalCommandManager commandManager;
    private JobController jobController;
    private Map<String, Session> sessions =
            new ConcurrentHashMap<String, Session>();
    private long sessionTimeout;
    private Instrumentation instrumentation;
    private long pid;
    private ScheduledExecutorService executorService;
```

使用 `ConcurrentHashMap` 存储所有 HTTP API 会话，保证线程安全。

```java
    public SessionManagerImpl(
            ShellServerOptions options,
            InternalCommandManager commandManager,
            JobController jobController) {
        this.commandManager = commandManager;
        this.sessionTimeout = options.getSessionTimeout();
        this.instrumentation = options.getInstrumentation();
        this.pid = options.getPid();
        this.jobController = jobController;

        executorService = Executors
                .newSingleThreadScheduledExecutor(
                        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r,
                        "arthas-session-manager");
                t.setDaemon(true);
                return t;
            }
        });
        executorService.scheduleAtFixedRate(
                new Runnable() {
            @Override
            public void run() {
                cleanExpiredSessions();
            }
        }, sessionTimeout, sessionTimeout,
                TimeUnit.MILLISECONDS);
    }
```

**初始化时启动超时清理定时器**。与 `ShellServerImpl` 的 `evictSessions` 类似，但这里管理的是 HTTP API 会话。

```java
    @Override
    public Session createSession() {
        Session session = new SessionImpl();
        session.put(Session.COMMAND_MANAGER, commandManager);
        session.put(Session.INSTRUMENTATION, instrumentation);
        session.put(Session.PID, pid);
        sessions.put(session.getSessionId(), session);
        return session;
    }

    @Override
    public Session getSession(String sessionId) {
        return sessions.get(sessionId);
    }

    @Override
    public Session removeSession(String sessionId) {
        Session session = sessions.remove(sessionId);
        if (session != null) {
            session.setForegroundJob(null);
            SharingResultDistributor resultDistributor =
                    session.getResultDistributor();
            if (resultDistributor != null) {
                resultDistributor.close();
            }
        }
        return session;
    }
```

`removeSession` 的清理工作包括：
1. 从 Map 中移除。
2. 清空前台 Job。
3. 关闭结果分发器（释放内存中的命令结果缓存）。

```java
    private void cleanExpiredSessions() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Session> entry
                : sessions.entrySet()) {
            Session session = entry.getValue();
            if (now - session.getLastAccessTime()
                    > sessionTimeout) {
                removeSession(entry.getKey());
                logger.info(
                        "%.s session expired, sessionId: {}",
                        entry.getKey());
            }
        }
    }
```

**超时清理逻辑**：检查每个 Session 的最后访问时间，超时则移除。注意这里**没有**像 `ShellServerImpl.evictSessions` 那样检查运行中的 Job，因为 HTTP API 会话的 Job 是通过轮询获取结果的，超时就直接清除。

---

## 第五阶段：HttpApiHandler —— HTTP REST API 处理

### 5.1 API 请求/响应模型

#### 5.1.1 ApiAction —— API 操作枚举

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/api/ApiAction.java`

```java
public enum ApiAction {
    EXEC,
    ASYNC_EXEC,
    INTERRUPT_JOB,
    PULL_RESULTS,
    INIT_SESSION,
    JOIN_SESSION,
    CLOSE_SESSION,
    SESSION_INFO
}
```

支持的 API 操作：

| Action | 说明 |
|--------|------|
| `EXEC` | 同步执行命令，等待结果返回 |
| `ASYNC_EXEC` | 异步执行命令，立即返回 |
| `INTERRUPT_JOB` | 中断正在执行的 Job |
| `PULL_RESULTS` | 拉取异步执行的结果 |
| `INIT_SESSION` | 创建新会话 |
| `JOIN_SESSION` | 加入已有会话（观察模式） |
| `CLOSE_SESSION` | 关闭会话 |
| `SESSION_INFO` | 获取会话信息 |

#### 5.1.2 ApiRequest —— API 请求体

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/api/ApiRequest.java`

```java
public class ApiRequest {
    private String action;
    private String command;
    private String sessionId;
    private String consumerId;
    private String requestId;
    private Integer execTimeout;
    private String userId;
    // getter/setter ...
}
```

请求体字段说明：

| 字段 | 必填 | 说明 |
|------|------|------|
| `action` | 是 | 操作类型（见 ApiAction） |
| `command` | EXEC/ASYNC_EXEC 时必填 | 要执行的命令 |
| `sessionId` | 除 INIT_SESSION/EXEC 外必填 | 会话 ID |
| `consumerId` | PULL_RESULTS 时必填 | 结果消费者 ID |
| `requestId` | 否 | 请求 ID（原样返回，用于请求追踪） |
| `execTimeout` | 否 | EXEC 超时时间（ms），默认 30000 |
| `userId` | 否 | 用户 ID |

#### 5.1.3 ApiResponse —— API 响应体

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/api/ApiResponse.java`

```java
public class ApiResponse {
    private ApiState state;
    private String message;
    private String sessionId;
    private String consumerId;
    private String requestId;
    private Map<String, Object> body;
    // getter/setter ...
}
```

#### 5.1.4 ApiState —— API 状态枚举

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/api/ApiState.java`

```java
public enum ApiState {
    SCHEDULED,
    SUCCEEDED,
    FAILED,
    REFUSED,
    INTERRUPTED
}
```

| 状态 | 含义 |
|------|------|
| `SCHEDULED` | 异步任务已调度（ASYNC_EXEC 的正常返回） |
| `SUCCEEDED` | 操作成功 |
| `FAILED` | 操作失败 |
| `REFUSED` | 操作被拒绝（如另一个命令正在执行） |
| `INTERRUPTED` | 命令被中断（超时或手动中断） |

### 5.2 HttpApiHandler —— API 核心处理器

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/api/HttpApiHandler.java`

```java
public class HttpApiHandler {

    private static final ValueFilter[] JSON_FILTERS =
            new ValueFilter[] { new ObjectVOFilter() };
    private static final String ONETIME_SESSION_KEY =
            "oneTimeSession";
    public static final int DEFAULT_EXEC_TIMEOUT = 30000;
    private final SessionManager sessionManager;
    private final InternalCommandManager commandManager;
    private final JobController jobController;
    private final HistoryManager historyManager;

    public HttpApiHandler(
            HistoryManager historyManager,
            SessionManager sessionManager) {
        this.historyManager = historyManager;
        this.sessionManager = sessionManager;
        commandManager =
                this.sessionManager.getCommandManager();
        jobController =
                this.sessionManager.getJobController();
    }
```

**Q: 为什么 HttpApiHandler 需要 HistoryManager？**

A: 即使是通过 HTTP API 执行的命令，也会被记录到命令历史中。这样用户通过 Telnet/WebSocket 连接时，可以用上下方向键查看之前通过 API 执行的命令。

### 5.3 请求处理主流程

```java
    public HttpResponse handle(
            ChannelHandlerContext ctx,
            FullHttpRequest request) throws Exception {

        ApiResponse result;
        String requestBody = null;
        String requestId = null;
        try {
            HttpMethod method = request.method();
            if (HttpMethod.POST.equals(method)) {
                requestBody = getBody(request);
                ApiRequest apiRequest =
                        parseRequest(requestBody);
                requestId = apiRequest.getRequestId();
                result = processRequest(ctx, apiRequest);
            } else {
                result = createResponse(ApiState.REFUSED,
                        "Unsupported http method: "
                        + method.name());
            }
        } catch (Throwable e) {
            result = createResponse(ApiState.FAILED,
                    "Process request error: "
                    + e.getMessage());
        }
        if (result == null) {
            result = createResponse(ApiState.FAILED,
                    "The request was not processed");
        }
        result.setRequestId(requestId);

        byte[] jsonBytes =
                JSON.toJSONBytes(result, JSON_FILTERS);

        DefaultFullHttpResponse response =
                new DefaultFullHttpResponse(
                        request.protocolVersion(),
                        HttpResponseStatus.OK,
                        Unpooled.wrappedBuffer(jsonBytes));
        response.headers().set(
                HttpHeaderNames.CONTENT_TYPE,
                "application/json; charset=utf-8");
        return response;
    }
```

**主流程**：

1. **只接受 POST 请求**，GET 等其他方法直接拒绝。
2. 解析请求体为 `ApiRequest` 对象。
3. 调用 `processRequest` 处理请求。
4. 将结果序列化为 JSON，使用 `ObjectVOFilter` 过滤敏感数据。
5. 返回 HTTP 200 响应（注意：即使业务失败也是 HTTP 200，错误信息在 JSON 中）。

### 5.4 请求分发与处理

```java
    private ApiResponse processRequest(
            ChannelHandlerContext ctx,
            ApiRequest apiRequest) {

        String actionStr = apiRequest.getAction();
        try {
            if (StringUtils.isBlank(actionStr)) {
                throw new ApiException(
                        "'action' is required");
            }
            ApiAction action;
            try {
                action = ApiAction.valueOf(
                        actionStr.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ApiException(
                        "unknown action: " + actionStr);
            }

            // INIT_SESSION 不需要 sessionId
            if (ApiAction.INIT_SESSION.equals(action)) {
                return processInitSessionRequest(
                        apiRequest);
            }

            // 其他操作需要 sessionId
            Session session = null;
            boolean allowNullSession =
                    ApiAction.EXEC.equals(action);
            String sessionId = apiRequest.getSessionId();
            if (StringUtils.isBlank(sessionId)) {
                if (!allowNullSession) {
                    throw new ApiException(
                            "'sessionId' is required");
                }
            } else {
                session = sessionManager.getSession(
                        sessionId);
                if (session == null) {
                    throw new ApiException(
                            "session not found: "
                            + sessionId);
                }
                sessionManager.updateAccessTime(session);
            }

            // EXEC 允许无 session，自动创建一次性 session
            if (session == null) {
                session = sessionManager.createSession();
                session.put(ONETIME_SESSION_KEY,
                        new Object());
            }
```

**关键设计**：`EXEC` 操作支持"一次性 Session"。如果客户端不传 `sessionId`，会自动创建一个临时 Session，命令执行完毕后自动销毁。这简化了简单场景下的 API 使用——不需要先 INIT_SESSION 再 EXEC 再 CLOSE_SESSION。

### 5.5 同步执行（EXEC）

```java
    private ApiResponse processExecRequest(
            ApiRequest apiRequest, Session session) {
        boolean oneTimeAccess = false;
        if (session.get(ONETIME_SESSION_KEY) != null) {
            oneTimeAccess = true;
        }

        try {
            String commandLine = apiRequest.getCommand();

            if (!session.tryLock()) {
                // 另一个命令正在执行
                response.setState(ApiState.REFUSED)
                        .setMessage(
                                "Another command "
                                + "is executing.");
                return response;
            }

            // 检查是否有前台 Job
            Job foregroundJob =
                    session.getForegroundJob();
            if (foregroundJob != null) {
                response.setState(ApiState.REFUSED)
                        .setMessage(
                                "Another job "
                                + "is running.");
                return response;
            }

            // 创建 Job 并运行
            packingResultDistributor =
                    new PackingResultDistributorImpl(
                            session);
            job = this.createJob(commandLine, session,
                    packingResultDistributor);
            session.setForegroundJob(job);
            job.run();

            // 等待完成或超时
            Integer timeout =
                    apiRequest.getExecTimeout();
            if (timeout == null || timeout <= 0) {
                timeout = DEFAULT_EXEC_TIMEOUT;
            }
            boolean timeExpired =
                    !waitForJob(job, timeout);
            if (timeExpired) {
                job.interrupt();
            }

            // 打包结果
            body.put("results",
                    packingResultDistributor
                            .getResults());
            return response;
        } finally {
            if (oneTimeAccess) {
                sessionManager.removeSession(
                        session.getSessionId());
            }
        }
    }
```

**同步执行的流程**：

1. 获取 Session 锁，防止并发执行。
2. 检查是否有正在运行的前台 Job。
3. 创建 `PackingResultDistributor`——它将命令输出收集到列表中。
4. 创建并运行 Job。
5. **阻塞等待** Job 完成或超时（默认 30 秒）。
6. 超时则强制中断 Job。
7. 将收集到的结果返回。
8. 如果是一次性 Session，自动清理。

### 5.6 异步执行（ASYNC_EXEC）

```java
    private ApiResponse processAsyncExecRequest(
            ApiRequest apiRequest, Session session) {
        String commandLine = apiRequest.getCommand();

        if (!session.tryLock()) {
            response.setState(ApiState.REFUSED)
                    .setMessage(
                            "Another command "
                            + "is executing.");
            return response;
        }

        // 创建 Job 并运行
        Job job = this.createJob(commandLine, session,
                session.getResultDistributor());
        session.setForegroundJob(job);
        job.run();

        // 立即返回，不等待结果
        response.setState(ApiState.SCHEDULED);
        return response;
    }
```

**异步执行与同步执行的区别**：

| 对比维度 | EXEC（同步） | ASYNC_EXEC（异步） |
|---------|------------|------------------|
| 结果分发器 | PackingResultDistributor | SharingResultDistributor |
| 是否等待 | 等待完成或超时 | 立即返回 |
| 结果获取 | 响应体中直接包含 | 需要 PULL_RESULTS 拉取 |
| 适用场景 | 快速命令（version, sc等） | 长时间命令（trace, watch等） |

### 5.7 ApiTerm —— API 虚拟终端

```java
    private static class ApiTerm implements Term {

        private Session session;

        public ApiTerm(Session session) {
            this.session = session;
        }

        @Override
        public String type() {
            return "web";
        }

        @Override
        public int width() {
            return 1000;
        }

        @Override
        public int height() {
            return 200;
        }

        @Override
        public Term write(String data) {
            return this;
        }

        @Override
        public void close() {
        }
        // ... 其他方法都是空实现
    }
```

**Q: 为什么 ApiTerm 的 write 方法什么都不做？**

A: HTTP API 模式下，命令输出通过 `ResultDistributor` 收集，而不是直接写入终端。`ApiTerm` 是一个"虚拟终端"，它的宽度设为 1000 字符（避免输出被截断），高度设为 200 行。所有的写操作都被忽略，因为真正的输出通过结果分发器传递给客户端。

---

## 第六阶段：HttpRequestHandler —— HTTP 请求路由

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/HttpRequestHandler.java`

`HttpRequestHandler` 是 Netty Pipeline 中处理 HTTP 请求的关键处理器。它负责路由不同的 HTTP 路径到不同的处理逻辑：

1. **`/api` 路径**：转发给 `HttpApiHandler` 处理 REST API 请求。
2. **WebSocket 升级请求**：传递给下一个 Pipeline 处理器（`WebSocketServerProtocolHandler`）。
3. **静态资源请求**：直接返回前端文件（HTML/CSS/JS）。

这个设计让 Arthas 的 HTTP 服务器能够同时服务 API 请求、WebSocket 连接和 Web UI 静态资源。

---

## 第七阶段：安全认证机制

### 7.1 SecurityAuthenticator —— 认证器接口

**源码位置**: `core/src/main/java/com/taobao/arthas/core/security/SecurityAuthenticator.java`

```java
public interface SecurityAuthenticator {

    boolean needLogin();

    Subject login(Principal principal)
            throws LoginException;

    String getUsername();

    String getPassword();
}
```

接口定义了四个方法：

| 方法 | 说明 |
|------|------|
| `needLogin()` | 是否需要登录 |
| `login()` | 执行登录验证 |
| `getUsername()` | 获取用户名 |
| `getPassword()` | 获取密码（用于 HTTP Basic Auth） |

### 7.2 SecurityAuthenticatorImpl —— 认证器实现

**源码位置**: `core/src/main/java/com/taobao/arthas/core/security/SecurityAuthenticatorImpl.java`

```java
public class SecurityAuthenticatorImpl
        implements SecurityAuthenticator {
    private static final Logger logger =
            LoggerFactory.getLogger(
                    SecurityAuthenticatorImpl.class);

    private boolean needLogin;
    private String username;
    private String password;
```

**构造函数**（从 ArthasBootstrap 中初始化）：

认证器的初始化逻辑如下：

1. 如果用户配置了 `username` 和 `password`，启用认证。
2. 如果 Arthas 监听在 `0.0.0.0`（所有网络接口），**强制启用认证**，自动生成随机密码并打印到日志中。
3. 如果只监听 `127.0.0.1`（本地），默认不需要认证。

```java
    @Override
    public boolean needLogin() {
        return needLogin;
    }

    @Override
    public Subject login(Principal principal)
            throws LoginException {
        if (!needLogin) {
            Subject subject = new Subject();
            subject.getPrincipals().add(principal);
            return subject;
        }

        if (principal instanceof UsernamePasswordPrincipal) {
            UsernamePasswordPrincipal up =
                    (UsernamePasswordPrincipal) principal;
            if (username.equals(up.getUsername())
                    && password.equals(up.getPassword())) {
                Subject subject = new Subject();
                subject.getPrincipals().add(principal);
                return subject;
            }
            throw new LoginException(
                    "username or password error");
        }

        if (principal instanceof LocalConnectionPrincipal) {
            Subject subject = new Subject();
            subject.getPrincipals().add(principal);
            return subject;
        }

        throw new LoginException(
                "unsupported principal type: "
                + principal.getClass());
    }
```

**login 方法的认证策略**：

1. **不需要登录**：直接创建 Subject，放行。
2. **用户名密码认证**：匹配配置的 username/password。
3. **本地连接认证**：`LocalConnectionPrincipal` 表示来自 127.0.0.1 的连接，直接放行。

**Q: 安全认证在什么情况下会强制启用？**

A: 当 Arthas 监听在 `0.0.0.0` 时会强制启用。因为此时 Arthas 可以被网络中的任何机器访问，没有认证是非常危险的——攻击者可以远程执行任意 Java 代码。

### 7.3 BasicHttpAuthenticatorHandler —— HTTP Basic 认证

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/BasicHttpAuthenticatorHandler.java`

这个 Netty Handler 在 HTTP Pipeline 中实现 HTTP Basic Authentication。它的工作流程：

1. 检查 `SecurityAuthenticator.needLogin()` 是否返回 true。
2. 如果需要登录，检查请求头中的 `Authorization` 字段。
3. 解码 Base64 编码的用户名:密码。
4. 调用 `SecurityAuthenticator.login()` 验证。
5. 验证成功后，将认证信息存入 `HttpSession`。
6. 验证失败返回 HTTP 401 响应。

### 7.4 HttpSession 管理

#### 7.4.1 HttpSession —— HTTP 会话接口

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/session/HttpSession.java`

```java
public interface HttpSession {
    String getId();
    Object getAttribute(String name);
    void setAttribute(String name, Object value);
    long getCreationTime();
    long getLastAccessedTime();
    void setLastAccessedTime(long lastAccessedTime);
}
```

注意，这个 `HttpSession` 是 Arthas 自己实现的，不是 Servlet 的 `javax.servlet.http.HttpSession`。它用于在 HTTP 请求之间保持认证状态。

#### 7.4.2 HttpSessionManager —— HTTP 会话管理

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/session/HttpSessionManager.java`

```java
public class HttpSessionManager {
    private static final String HTTP_SESSION_ATTR_KEY =
            "HTTP_SESSION";
    private LRUCache<String, HttpSession> sessions;
    private int maxSessionCount;
    private long sessionTimeout;
```

使用 `LRUCache` 管理 HTTP Session，当 Session 数量超过上限时，最久未使用的 Session 会被淘汰。

#### 7.4.3 LRUCache —— LRU缓存

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/session/LRUCache.java`

```java
public class LRUCache<K, V>
        extends LinkedHashMap<K, V> {
    private int maxCapacity;

    public LRUCache(int maxCapacity) {
        super(16, 0.75f, true);
        this.maxCapacity = maxCapacity;
    }

    @Override
    protected boolean removeEldestEntry(
            Map.Entry<K, V> eldest) {
        return size() > maxCapacity;
    }
}
```

利用 `LinkedHashMap` 的 `accessOrder=true` 特性实现 LRU 缓存。当缓存大小超过 `maxCapacity` 时，自动移除最久未访问的条目。

---

## 第八阶段：Tunnel通信体系 —— 远程管理

### 8.1 为什么需要 Tunnel？

在传统部署模式下，Arthas 监听在目标 JVM 所在机器的端口上。但在以下场景中，直接连接是不可行的：

- **容器环境**：Docker/K8s 中的 JVM 无法直接被外部访问。
- **安全策略**：生产环境通常不允许开放额外端口。
- **大规模管理**：需要管理成百上千个 JVM 实例。

Tunnel 通过**反向代理**模式解决了这个问题：

```
┌─────────────┐              ┌──────────────┐
│  浏览器/CLI  │──WebSocket──>│ Tunnel Server│
│  (Web UI)   │              │ (独立部署)    │
└─────────────┘              └──────┬───────┘
                                    │
                         WebSocket  │ (反向连接)
                                    │
                             ┌──────┴───────┐
                             │ Tunnel Client│
                             │ (目标JVM内)  │
                             └──────┬───────┘
                                    │
                              本地连接│
                                    │
                             ┌──────┴───────┐
                             │  Arthas Core │
                             │  (目标JVM)   │
                             └──────────────┘
```

**关键点**：Tunnel Client 主动连接 Tunnel Server（出站连接），不需要在目标机器上开放任何入站端口。

### 8.2 tunnel-common —— 公共协议定义

#### 8.2.1 URIConstans —— URI 常量

**源码位置**: `tunnel-common/src/main/java/com/alibaba/arthas/tunnel/common/URIConstans.java`

```java
public class URIConstans {
    public static final String RESPONSE = "response";
    public static final String METHOD = "method";
    public static final String ID = "id";
    public static final String CLIENT_CONNECTION_ID =
            "clientConnectionId";
    public static final String APP_NAME = "appName";
    public static final String ARTHAS_VERSION =
            "arthasVersion";
    public static final String TARGET_URL = "targetUrl";
    public static final String PROXY_REQUEST_ID =
            "proxyRequestId";
    public static final String PROXY_RESPONSE_DATA =
            "proxyResponseData";
}
```

这些常量定义了 Tunnel 协议中 URL 参数的 key。所有 Tunnel 通信都通过 WebSocket 文本帧传输，参数以 URL query string 格式编码。

#### 8.2.2 MethodConstants —— 方法常量

**源码位置**: `tunnel-common/src/main/java/com/alibaba/arthas/tunnel/common/MethodConstants.java`

```java
public class MethodConstants {
    public static final String AGENT_REGISTER =
            "agentRegister";
    public static final String CONNECT_ARTHAS =
            "connectArthas";
    public static final String START_TUNNEL =
            "startTunnel";
    public static final String OPEN_TUNNEL =
            "openTunnel";
    public static final String HTTP_PROXY =
            "httpProxy";
}
```

| 方法 | 方向 | 说明 |
|------|------|------|
| `agentRegister` | Client → Server | Agent 向 Server 注册 |
| `connectArthas` | Browser → Server | 浏览器请求连接某个 Agent |
| `startTunnel` | Server → Client | Server 通知 Client 建立隧道 |
| `openTunnel` | Client → Server | Client 建立数据转发隧道 |
| `httpProxy` | 双向 | HTTP 代理请求/响应 |

#### 8.2.3 SimpleHttpResponse —— HTTP 响应封装

**源码位置**: `tunnel-common/src/main/java/com/alibaba/arthas/tunnel/common/SimpleHttpResponse.java`

```java
public class SimpleHttpResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    private int status;
    private Map<String, String> headers =
            new HashMap<String, String>();
    private byte[] body;
```

`SimpleHttpResponse` 用于在 Tunnel 中传输 HTTP 代理的响应。它实现了 `Serializable`，可以序列化为字节数组，再通过 Base64 编码在 WebSocket 文本帧中传输。

```java
    public static byte[] toBytes(
            SimpleHttpResponse response)
            throws IOException {
        ByteArrayOutputStream bos =
                new ByteArrayOutputStream();
        ObjectOutputStream oos =
                new ObjectOutputStream(bos);
        oos.writeObject(response);
        oos.flush();
        return bos.toByteArray();
    }

    public static SimpleHttpResponse fromBytes(
            byte[] bytes)
            throws IOException,
            ClassNotFoundException {
        ByteArrayInputStream bis =
                new ByteArrayInputStream(bytes);
        ObjectInputStream ois =
                new ObjectInputStream(bis);
        return (SimpleHttpResponse) ois.readObject();
    }
```

使用 Java 原生序列化。虽然不是最高效的序列化方式，但对于代理 HTTP 响应这种低频操作来说足够了。

### 8.3 TunnelClient —— 隧道客户端

**源码位置**: `tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/TunnelClient.java`

```java
public class TunnelClient {
    private String tunnelServerUrl;
    private int reconnectDelay = 5;

    // 两个线程：一个用于连接，一个用于重连。#1284
    private EventLoopGroup eventLoopGroup =
            new NioEventLoopGroup(2,
                    new DefaultThreadFactory(
                            "arthas-TunnelClient", true));

    private String appName;
    volatile private String id;
    private String version = "unknown";
    private volatile boolean connected = false;
```

**核心字段**：

- `tunnelServerUrl`：Tunnel Server 的 WebSocket 地址，如 `ws://tunnel.example.com:7777/ws`。
- `reconnectDelay`：重连延迟，默认 5 秒。
- `eventLoopGroup`：Netty 事件循环组，2 个线程——一个用于当前连接，一个用于重连。
- `id`：Agent ID，由 Tunnel Server 分配或 Agent 指定。使用 `volatile` 保证多线程可见性。
- `appName`：应用名称，用于在 Tunnel Server 上分组管理。

### 8.4 TunnelClient.connect —— 连接建立

```java
    public ChannelFuture connect(boolean reconnect)
            throws SSLException, URISyntaxException,
            InterruptedException {
        QueryStringEncoder queryEncoder =
                new QueryStringEncoder(
                        this.tunnelServerUrl);
        queryEncoder.addParam(
                URIConstans.METHOD,
                MethodConstants.AGENT_REGISTER);
        queryEncoder.addParam(
                URIConstans.ARTHAS_VERSION,
                this.version);
        if (appName != null) {
            queryEncoder.addParam(
                    URIConstans.APP_NAME, appName);
        }
        if (id != null) {
            queryEncoder.addParam(
                    URIConstans.ID, id);
        }
        final URI agentRegisterURI =
                queryEncoder.toUri();
```

**连接 URI 构建**：

注册请求的 URI 格式如下：
```
ws://127.0.0.1:7777/ws?method=agentRegister&arthasVersion=3.x.x&appName=myapp&id=xxx
```

参数说明：
- `method=agentRegister`：表明这是一个 Agent 注册请求。
- `arthasVersion`：Arthas 版本号。
- `appName`：应用名（可选）。
- `id`：Agent ID（重连时复用之前的 ID）。

```java
        final boolean ssl =
                "wss".equalsIgnoreCase(scheme);
        final SslContext sslCtx;
        if (ssl) {
            sslCtx = SslContextBuilder.forClient()
                    .trustManager(
                            InsecureTrustManagerFactory
                                    .INSTANCE)
                    .build();
        } else {
            sslCtx = null;
        }
```

**SSL 支持**：
- `ws://` → 不加密。
- `wss://` → TLS 加密，使用 `InsecureTrustManagerFactory`（信任所有证书）。

```java
        Bootstrap bs = new Bootstrap();
        bs.group(eventLoopGroup)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                5000)
        .option(ChannelOption.TCP_NODELAY, true)
        .channel(NioSocketChannel.class)
        .remoteAddress(host, port)
        .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(
                            ch.alloc(), host, port));
                }
                p.addLast(
                        new HttpClientCodec(),
                        new HttpObjectAggregator(
                                ArthasConstants
                                        .MAX_HTTP_CONTENT_LENGTH),
                        websocketClientHandler,
                        new IdleStateHandler(
                                0, 0,
                                ArthasConstants
                                        .WEBSOCKET_IDLE_SECONDS),
                        handler);
            }
        });
```

**Netty Pipeline 结构**：

| 层次 | Handler | 作用 |
|------|---------|------|
| 1 | SslHandler（可选） | TLS 加密 |
| 2 | HttpClientCodec | HTTP 编解码 |
| 3 | HttpObjectAggregator | HTTP 消息聚合 |
| 4 | WebSocketClientProtocolHandler | WebSocket 协议处理 |
| 5 | IdleStateHandler | 空闲检测 |
| 6 | TunnelClientSocketClientHandler | Tunnel 业务逻辑 |

### 8.5 TunnelClientSocketClientHandler —— 客户端消息处理

**源码位置**: `tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/TunnelClientSocketClientHandler.java`

```java
public class TunnelClientSocketClientHandler
        extends SimpleChannelInboundHandler<WebSocketFrame> {

    private final TunnelClient tunnelClient;
    private ChannelPromise registerPromise;

    public TunnelClientSocketClientHandler(
            TunnelClient tunnelClient) {
        this.tunnelClient = tunnelClient;
    }
```

#### 消息处理 —— channelRead0

```java
    @Override
    public void channelRead0(
            ChannelHandlerContext ctx,
            WebSocketFrame frame) throws Exception {
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame =
                    (TextWebSocketFrame) frame;
            String text = textFrame.text();

            QueryStringDecoder queryDecoder =
                    new QueryStringDecoder(text);
            Map<String, List<String>> parameters =
                    queryDecoder.parameters();
            String method = null;
            List<String> methodList =
                    parameters.get(URIConstans.METHOD);
            if (methodList != null
                    && !methodList.isEmpty()) {
                method = methodList.get(0);
            }
```

消息格式是 URL query string，通过 `QueryStringDecoder` 解析。

**处理 agentRegister 响应**：

```java
            if (MethodConstants.AGENT_REGISTER
                    .equals(method)) {
                List<String> idList =
                        parameters.get(URIConstans.ID);
                if (idList != null
                        && !idList.isEmpty()) {
                    this.tunnelClient.setId(
                            idList.get(0));
                }
                tunnelClient.setConnected(true);
                registerPromise.setSuccess();
            }
```

注册成功后：
1. 保存 Server 分配的 Agent ID。
2. 标记连接状态为已连接。
3. 设置注册 Promise 为成功——通知调用者注册完成。

**处理 startTunnel 请求**：

```java
            if (MethodConstants.START_TUNNEL
                    .equals(method)) {
                QueryStringEncoder queryEncoder =
                        new QueryStringEncoder(
                                this.tunnelClient
                                        .getTunnelServerUrl());
                queryEncoder.addParam(
                        URIConstans.METHOD,
                        MethodConstants.OPEN_TUNNEL);
                queryEncoder.addParam(
                        URIConstans.CLIENT_CONNECTION_ID,
                        parameters.get(
                                URIConstans
                                        .CLIENT_CONNECTION_ID)
                                .get(0));
                queryEncoder.addParam(
                        URIConstans.ID,
                        parameters.get(URIConstans.ID)
                                .get(0));

                final URI forwardUri =
                        queryEncoder.toUri();

                ForwardClient forwardClient =
                        new ForwardClient(forwardUri);
                forwardClient.start();
            }
```

当 Tunnel Server 通知"有浏览器要连接你"时，Client 启动一个 `ForwardClient`，建立第二条 WebSocket 连接到 Tunnel Server，用于数据转发。

**处理 httpProxy 请求**：

```java
            if (MethodConstants.HTTP_PROXY
                    .equals(method)) {
                ProxyClient proxyClient =
                        new ProxyClient();
                List<String> targetUrls =
                        parameters.get(
                                URIConstans.TARGET_URL);
                // ... 解析 requestId

                String targetUrl = targetUrls.get(0);
                SimpleHttpResponse simpleHttpResponse =
                        proxyClient.query(targetUrl);

                ByteBuf byteBuf = Base64.encode(
                        Unpooled.wrappedBuffer(
                                SimpleHttpResponse.toBytes(
                                        simpleHttpResponse)));
                String requestData =
                        byteBuf.toString(CharsetUtil.UTF_8);

                QueryStringEncoder queryEncoder =
                        new QueryStringEncoder("");
                queryEncoder.addParam(
                        URIConstans.METHOD,
                        MethodConstants.HTTP_PROXY);
                queryEncoder.addParam(
                        URIConstans.PROXY_REQUEST_ID, id);
                queryEncoder.addParam(
                        URIConstans.PROXY_RESPONSE_DATA,
                        requestData);

                ctx.writeAndFlush(
                        new TextWebSocketFrame(
                                queryEncoder.toString()));
            }
```

HTTP 代理的工作流程：
1. 收到代理请求，包含目标 URL。
2. 使用 `ProxyClient` 本地发起 HTTP 请求（访问 Arthas 本地 API）。
3. 将响应序列化 → Base64 编码 → 通过 WebSocket 发回 Tunnel Server。

#### 断线重连机制

```java
    @Override
    public void channelUnregistered(
            final ChannelHandlerContext ctx)
            throws Exception {
        tunnelClient.setConnected(false);
        ctx.channel().eventLoop().schedule(
                new Runnable() {
            @Override
            public void run() {
                try {
                    tunnelClient.connect(true);
                } catch (Throwable e) {
                    logger.error(
                            "reconnect error", e);
                }
            }
        }, tunnelClient.getReconnectDelay(),
                TimeUnit.SECONDS);
    }
```

**自动重连**：当连接断开时，延迟 `reconnectDelay` 秒（默认5秒）后尝试重连。使用 Netty 的 EventLoop 调度，避免阻塞。

#### 心跳机制

```java
    @Override
    public void userEventTriggered(
            ChannelHandlerContext ctx, Object evt)
            throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.writeAndFlush(
                    new PingWebSocketFrame());
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
```

当连接空闲超过 `WEBSOCKET_IDLE_SECONDS` 时，发送 WebSocket Ping 帧保持连接活跃。

### 8.6 ForwardClient —— 数据转发客户端

**源码位置**: `tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/ForwardClient.java`

```java
public class ForwardClient {
    private URI tunnelServerURI;
    private EventLoopGroup eventLoopGroup =
            new NioEventLoopGroup(1,
                    new DefaultThreadFactory(
                            "arthas-ForwardClient", true));
```

`ForwardClient` 建立到 Tunnel Server 的**第二条 WebSocket 连接**，专门用于数据转发。它同时也建立到 Arthas 本地 WebSocket 服务的连接（通过 `LocalFrameHandler`），实现双向数据桥接。

### 8.7 LocalFrameHandler —— 本地帧处理器

**源码位置**: `tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/LocalFrameHandler.java`

这个处理器连接 Arthas 本地的 WebSocket 端口，接收 Arthas 的命令输出并转发给 Tunnel Server，同时将 Tunnel Server 转来的用户输入转发给 Arthas。

### 8.8 RelayHandler —— 数据中继器

**源码位置**: `tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/RelayHandler.java`

```java
public class RelayHandler
        extends ChannelInboundHandlerAdapter {
    private final Channel relayChannel;

    public RelayHandler(Channel relayChannel) {
        this.relayChannel = relayChannel;
    }

    @Override
    public void channelRead(
            ChannelHandlerContext ctx, Object msg)
            throws Exception {
        if (relayChannel.isActive()) {
            relayChannel.writeAndFlush(msg);
        } else {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelInactive(
            ChannelHandlerContext ctx)
            throws Exception {
        ChannelUtils.closeOnFlush(relayChannel);
    }
```

`RelayHandler` 是最简单也最优雅的数据转发器。它把从一个 Channel 收到的数据，原封不动地写入另一个 Channel。当一个 Channel 断开时，关闭另一个 Channel。这是一个标准的"双向管道"模式。

### 8.9 TunnelServer —— 隧道服务端

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/TunnelServer.java`

```java
public class TunnelServer {
    private boolean ssl;
    private String host;
    private int port;
    private String path =
            ArthasConstants.DEFAULT_WEBSOCKET_PATH;

    private Map<String, AgentInfo> agentInfoMap =
            new ConcurrentHashMap<>();
    private Map<String, ClientConnectionInfo>
            clientConnectionInfoMap =
                    new ConcurrentHashMap<>();
    private Map<String, Promise<SimpleHttpResponse>>
            proxyRequestPromiseMap =
                    new ConcurrentHashMap<>();

    private EventLoopGroup bossGroup =
            new NioEventLoopGroup(1,
                    new DefaultThreadFactory(
                            "arthas-TunnelServer-boss",
                            true));
    private EventLoopGroup workerGroup =
            new NioEventLoopGroup(
                    new DefaultThreadFactory(
                            "arthas-TunnelServer-worker",
                            true));

    private TunnelClusterStore tunnelClusterStore;
    private String clientConnectHost;
```

**核心数据结构**：

| Map | Key | Value | 说明 |
|-----|-----|-------|------|
| `agentInfoMap` | agentId | AgentInfo | 所有已注册的 Agent |
| `clientConnectionInfoMap` | clientConnectionId | ClientConnectionInfo | 等待隧道打通的浏览器连接 |
| `proxyRequestPromiseMap` | requestId | Promise | HTTP 代理请求的异步结果 |

### 8.10 TunnelServer.start —— 服务启动与清理任务

```java
    public void start() throws Exception {
        final SslContext sslCtx;
        if (ssl) {
            SelfSignedCertificate ssc =
                    new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(
                    ssc.certificate(),
                    ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(
                        new TunnelSocketServerInitializer(
                                this, sslCtx));

        if (StringUtils.isBlank(host)) {
            channel = b.bind(port).sync().channel();
        } else {
            channel = b.bind(host, port)
                    .sync().channel();
        }
```

标准的 Netty ServerBootstrap 启动。注意 SSL 使用自签名证书，这在生产环境中通常会通过 Nginx 代理来提供真正的 TLS。

```java
        workerGroup.scheduleWithFixedDelay(
                new Runnable() {
            @Override
            public void run() {
                agentInfoMap.entrySet().removeIf(
                        e -> !e.getValue()
                                .getChannelHandlerContext()
                                .channel().isActive());
                clientConnectionInfoMap.entrySet()
                        .removeIf(
                                e -> !e.getValue()
                                        .getChannelHandlerContext()
                                        .channel().isActive());

                // 更新集群 key 信息
                if (tunnelClusterStore != null
                        && clientConnectHost != null) {
                    for (Entry<String, AgentInfo> entry
                            : agentInfoMap.entrySet()) {
                        tunnelClusterStore.addAgent(
                                entry.getKey(),
                                new AgentClusterInfo(
                                        entry.getValue(),
                                        clientConnectHost,
                                        port),
                                60 * 60,
                                TimeUnit.SECONDS);
                    }
                }
            }
        }, 60, 60, TimeUnit.SECONDS);
    }
```

**定时清理任务**（每60秒执行）：
1. 移除已断开连接的 Agent。
2. 移除已断开连接的浏览器。
3. 如果配置了集群存储，更新 Agent 信息到 Redis（TTL 1小时）。

### 8.11 AgentInfo —— Agent 信息

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/AgentInfo.java`

```java
public class AgentInfo {
    private ChannelHandlerContext channelHandlerContext;
    private String host;
    private int port;
    private String arthasVersion;
    // getter/setter ...
}
```

存储 Agent 的连接信息，包括 Netty 的 `ChannelHandlerContext`（用于向 Agent 发送消息）。

### 8.12 ClientConnectionInfo —— 浏览器连接信息

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/ClientConnectionInfo.java`

```java
public class ClientConnectionInfo {
    private ChannelHandlerContext channelHandlerContext;
    private String host;
    private int port;
    private Promise<Channel> promise;
    // getter/setter ...
}
```

关键字段是 `promise`——这是一个 `Promise<Channel>`，当 Agent 建立数据隧道后，通过这个 Promise 通知浏览器连接。

### 8.13 TunnelSocketFrameHandler —— 服务端消息处理（核心）

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/TunnelSocketFrameHandler.java`

```java
public class TunnelSocketFrameHandler
        extends SimpleChannelInboundHandler<WebSocketFrame> {

    private TunnelServer tunnelServer;

    @Override
    public void userEventTriggered(
            ChannelHandlerContext ctx, Object evt)
            throws Exception {
        if (evt instanceof HandshakeComplete) {
            HandshakeComplete handshake =
                    (HandshakeComplete) evt;
            String uri = handshake.requestUri();

            MultiValueMap<String, String> parameters =
                    UriComponentsBuilder.fromUriString(uri)
                            .build().getQueryParams();
            String method = parameters.getFirst(
                    URIConstans.METHOD);

            if (MethodConstants.CONNECT_ARTHAS
                    .equals(method)) {
                connectArthas(ctx, parameters);
            } else if (MethodConstants.AGENT_REGISTER
                    .equals(method)) {
                agentRegister(ctx, handshake, uri);
            }
            if (MethodConstants.OPEN_TUNNEL
                    .equals(method)) {
                String clientConnectionId =
                        parameters.getFirst(
                                URIConstans
                                        .CLIENT_CONNECTION_ID);
                openTunnel(ctx, clientConnectionId);
            }
        }
    }
```

**三种 WebSocket 连接类型**：

1. **CONNECT_ARTHAS**（来自浏览器）：用户想连接某个 Agent。
2. **AGENT_REGISTER**（来自 Arthas Agent）：Agent 注册自己。
3. **OPEN_TUNNEL**（来自 Agent 的 ForwardClient）：打开数据隧道。

### 8.14 agentRegister —— Agent 注册流程

```java
    private void agentRegister(
            ChannelHandlerContext ctx,
            HandshakeComplete handshake,
            String requestUri)
            throws URISyntaxException {
        QueryStringDecoder queryDecoder =
                new QueryStringDecoder(requestUri);
        Map<String, List<String>> parameters =
                queryDecoder.parameters();

        String appName = null;
        List<String> appNameList =
                parameters.get(URIConstans.APP_NAME);
        if (appNameList != null
                && !appNameList.isEmpty()) {
            appName = appNameList.get(0);
        }

        // 生成 Agent ID
        String id = null;
        if (appName != null) {
            id = appName + "_"
                    + RandomStringUtils.random(
                            20, true, true).toUpperCase();
        } else {
            id = RandomStringUtils.random(
                    20, true, true).toUpperCase();
        }
        // Agent 传过来的 id 优先
        List<String> idList =
                parameters.get(URIConstans.ID);
        if (idList != null && !idList.isEmpty()) {
            id = idList.get(0);
        }
```

**Agent ID 生成策略**：
1. 如果 Agent 提供了 `appName`，ID 格式为 `appName_RANDOM20`，如 `myapp_ABCDEFGHIJ1234567890`。
2. 如果没有 `appName`，ID 为纯随机字符串。
3. 如果 Agent 重连时携带了之前的 ID，优先使用旧 ID。

```java
        AgentInfo info = new AgentInfo();
        HttpHeaders headers = handshake.requestHeaders();
        String host = HttpUtils.findClientIP(headers);
        if (host == null) {
            SocketAddress remoteAddress =
                    ctx.channel().remoteAddress();
            if (remoteAddress
                    instanceof InetSocketAddress) {
                InetSocketAddress addr =
                        (InetSocketAddress) remoteAddress;
                info.setHost(
                        addr.getHostString());
                info.setPort(addr.getPort());
            }
        } else {
            info.setHost(host);
        }
        info.setChannelHandlerContext(ctx);
        tunnelServer.addAgent(id, info);

        // 注册 Channel 关闭监听器
        ctx.channel().closeFuture().addListener(
                new GenericFutureListener<
                        Future<? super Void>>() {
            @Override
            public void operationComplete(
                    Future<? super Void> future)
                    throws Exception {
                tunnelServer.removeAgent(finalId);
            }
        });

        // 发送注册响应
        URI responseUri = UriComponentsBuilder
                .newInstance()
                .scheme(URIConstans.RESPONSE)
                .path("/")
                .queryParam(URIConstans.METHOD,
                        MethodConstants.AGENT_REGISTER)
                .queryParam(URIConstans.ID, id)
                .build().encode().toUri();

        ctx.channel().writeAndFlush(
                new TextWebSocketFrame(
                        responseUri.toString()));
    }
```

**注册流程**：
1. 提取 Agent 的 IP 地址（优先从 `X-Forwarded-For` 头获取，支持 Nginx 代理）。
2. 创建 `AgentInfo` 并存入 `agentInfoMap`。
3. 如果配置了 `TunnelClusterStore`，同步到 Redis。
4. 注册 Channel 关闭监听器，当 Agent 断开时自动移除。
5. 发送注册成功响应。

### 8.15 connectArthas —— 浏览器连接 Agent（关键交互流程）

```java
    private void connectArthas(
            ChannelHandlerContext tunnelSocketCtx,
            MultiValueMap<String, String> parameters)
            throws URISyntaxException {

        List<String> agentId = parameters.getOrDefault(
                "id", Collections.emptyList());

        Optional<AgentInfo> findAgent =
                tunnelServer.findAgent(agentId.get(0));

        if (findAgent.isPresent()) {
            ChannelHandlerContext agentCtx =
                    findAgent.get()
                            .getChannelHandlerContext();

            String clientConnectionId =
                    RandomStringUtils.random(
                            20, true, true).toUpperCase();
```

找到 Agent 后，生成一个随机的 `clientConnectionId` 用于关联浏览器和即将建立的数据隧道。

```java
            // 创建一个 Promise，等待 Agent 建立数据隧道
            Promise<Channel> promise =
                    GlobalEventExecutor.INSTANCE
                            .newPromise();
            promise.addListener(
                    new FutureListener<Channel>() {
                @Override
                public void operationComplete(
                        final Future<Channel> future)
                        throws Exception {
                    final Channel outboundChannel =
                            future.getNow();
                    if (future.isSuccess()) {
                        // 移除当前 handler
                        tunnelSocketCtx.pipeline()
                                .remove(
                                        TunnelSocketFrameHandler
                                                .this);
                        // 移除 Agent 端的 handler
                        outboundChannel.pipeline()
                                .removeLast();
                        // 添加双向中继
                        outboundChannel.pipeline()
                                .addLast(
                                        new RelayHandler(
                                                tunnelSocketCtx
                                                        .channel()));
                        tunnelSocketCtx.pipeline()
                                .addLast(
                                        new RelayHandler(
                                                outboundChannel));
                    }
                }
            });
```

**关键设计**：当数据隧道建立成功后，浏览器和 Agent 之间的 Pipeline 被替换为 `RelayHandler`，实现直接的双向数据转发。此时 `TunnelSocketFrameHandler` 已从 Pipeline 中移除。

```java
            // 通知 Agent 建立隧道
            agentCtx.channel().writeAndFlush(
                    new TextWebSocketFrame(
                            uri.toString()));

            // 等待 Agent 建立隧道（超时 20 秒）
            boolean watiResult =
                    promise.awaitUninterruptibly(
                            20, TimeUnit.SECONDS);
```

**等待隧道建立**：Server 向 Agent 发送 `startTunnel` 命令后，阻塞等待最多 20 秒。Agent 收到后建立 ForwardClient 连接，Server 端的 `openTunnel` 方法设置 Promise 成功。

### 8.16 openTunnel —— 打开数据隧道

```java
    private void openTunnel(
            ChannelHandlerContext ctx,
            String clientConnectionId) {
        Optional<ClientConnectionInfo> infoOptional =
                this.tunnelServer.findClientConnection(
                        clientConnectionId);

        if (infoOptional.isPresent()) {
            ClientConnectionInfo info =
                    infoOptional.get();
            Promise<Channel> promise =
                    info.getPromise();
            promise.setSuccess(ctx.channel());
        }
    }
```

当 Agent 的 ForwardClient 连接到 Tunnel Server 并请求 `openTunnel` 时，Server 通过 `clientConnectionId` 找到之前等待的 Promise，将新的 Channel 设置为 Promise 的结果。这触发了 `connectArthas` 中注册的监听器，完成双向中继的建立。

### 8.17 Tunnel 建立完整时序图

```
浏览器                    Tunnel Server                  Tunnel Client/Agent
  |                            |                              |
  |                            |<---WebSocket (agentRegister)--|  ① Agent注册
  |                            |---响应(id=xxx)--------------->|  ② 分配ID
  |                            |                              |
  |--WebSocket (connectArthas)->|                              |  ③ 浏览器连接
  |                            |--startTunnel (connId=abc)---->|  ④ 通知Agent
  |                            |                              |
  |                            |<---WebSocket (openTunnel,     |  ⑤ Agent建新连接
  |                            |    connId=abc)                |
  |                            |                              |
  |                            | [替换Pipeline为RelayHandler] |  ⑥ 建立中继
  |                            |                              |
  |<===== 双向数据隧道 ============================>|  ⑦ 数据直传
  |                            |                              |
```

### 8.18 集群支持

#### 8.18.1 TunnelClusterStore —— 集群存储接口

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/cluster/TunnelClusterStore.java`

```java
public interface TunnelClusterStore {
    void addAgent(String agentId,
            AgentClusterInfo info,
            long timeout, TimeUnit timeUnit);
    Optional<AgentClusterInfo> findAgent(
            String agentId);
    void removeAgent(String agentId);
    Collection<String> allAgentIds();
}
```

在集群部署时，多个 Tunnel Server 实例通过共享存储（Redis）来同步 Agent 信息。

#### 8.18.2 InMemoryClusterStore —— 内存存储（单机模式）

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/cluster/InMemoryClusterStore.java`

单机模式下使用内存 Map 存储，适用于开发和测试。

#### 8.18.3 AgentClusterInfo —— 集群Agent信息

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/AgentClusterInfo.java`

```java
public class AgentClusterInfo {
    private String host;
    private int port;
    private String arthasVersion;
    private String clientConnectHost;
    private int clientConnectPort;
```

除了 Agent 自身信息外，还包含 `clientConnectHost` 和 `clientConnectPort`——这是浏览器应该连接的 Tunnel Server 地址。在集群模式下，浏览器可能需要被重定向到 Agent 实际注册的那台 Tunnel Server。

---

## 第九阶段：HistoryManager —— 命令历史管理

### 9.1 HistoryManager —— 历史管理接口

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/history/HistoryManager.java`

```java
public interface HistoryManager {
    void saveHistory();
    void loadHistory();
    void clearHistory();
    void addHistory(String commandLine);
    List<String> getHistory();
    void setHistory(List<String> history);
}
```

### 9.2 HistoryManagerImpl —— 历史管理实现

**源码位置**: `core/src/main/java/com/taobao/arthas/core/shell/history/impl/HistoryManagerImpl.java`

```java
public class HistoryManagerImpl
        implements HistoryManager {
    private static final int MAX_HISTORY_SIZE = 500;

    private List<String> history =
            new ArrayList<String>();

    @Override
    public synchronized void saveHistory() {
        try {
            FileUtils.saveCommandHistoryString(
                    history,
                    new File(
                            Constants.CMD_HISTORY_FILE));
        } catch (Throwable e) {
            logger.error(
                    "save command history failed", e);
        }
    }

    @Override
    public synchronized void loadHistory() {
        try {
            history = FileUtils
                    .loadCommandHistoryString(
                            new File(
                                    Constants
                                            .CMD_HISTORY_FILE));
        } catch (Throwable e) {
            logger.error(
                    "load command history failed", e);
        }
    }

    @Override
    public synchronized void addHistory(
            String commandLine) {
        while (history.size() >= MAX_HISTORY_SIZE) {
            history.remove(0);
        }
        history.add(commandLine);
    }

    @Override
    public synchronized List<String> getHistory() {
        return new ArrayList<String>(history);
    }
}
```

**设计分析**：

1. **线程安全**：所有方法都使用 `synchronized` 同步，因为命令历史会被 Telnet 和 HTTP API 两个线程同时访问。

2. **容量限制**：最多保存 500 条命令历史。当超过上限时，移除最旧的命令（`history.remove(0)`）——这是一个简单的 FIFO 策略。

3. **持久化**：通过 `FileUtils.saveCommandHistoryString` 保存到文件（默认路径为 `~/.arthas/history`），下次启动时通过 `loadHistory` 加载。

4. **防御性拷贝**：`getHistory()` 返回的是一个新的 ArrayList，避免外部修改影响内部状态。

**Q: 为什么 addHistory 使用 while 而不是 if？**

A: 理论上 `if` 就够了，因为每次只添加一条。但使用 `while` 更安全——如果因为某种原因（比如直接 `setHistory` 了一个超大列表）导致 size 远大于 MAX_HISTORY_SIZE，`while` 能确保最终回到限制以内。

---

## 第十阶段：WebUI 前端架构

### 10.1 Web UI 项目结构

Arthas 的 Web UI 是一个现代的 Vue.js 单页应用，位于 `web-ui/arthasWebConsole/` 目录下。

```
web-ui/arthasWebConsole/
├── all/
│   ├── ui/           # 主 Web Console
│   │   └── ui/src/
│   │       ├── views/          # 页面组件
│   │       │   ├── Console.vue # 交互式控制台
│   │       │   ├── DashBoard.vue
│   │       │   ├── Synchronize.vue
│   │       │   └── Asynchronize.vue
│   │       ├── machines/       # 状态机
│   │       │   ├── consoleMachine.ts
│   │       │   └── perRequestMachine.ts
│   │       ├── stores/         # 状态管理
│   │       │   └── fetch.ts
│   │       └── components/     # 共享组件
│   ├── tunnel/       # Tunnel 管理 UI
│   │   └── tunnel/src/
│   │       └── views/
│   │           ├── Apps.vue    # 应用列表
│   │           └── Agent.vue   # Agent 列表
│   ├── native-agent/ # 原生 Agent UI
│   └── share/        # 共享资源
│       └── component/
│           └── Console.vue     # 共享控制台组件
```

### 10.2 通信方式

Web UI 与后端有两种通信方式：

1. **WebSocket**（交互式模式）：通过 WebSocket 连接到 Arthas 的 HTTP 端口，建立 `ExtHttpTtyConnection`，实现实时的终端交互。

2. **HTTP API**（异步模式）：通过 `fetch` 调用 `/api` 端点，使用 INIT_SESSION → ASYNC_EXEC → PULL_RESULTS 的模式执行命令和获取结果。

### 10.3 状态机模式

Web UI 使用状态机（`consoleMachine.ts`、`perRequestMachine.ts`）管理复杂的交互状态：

- **IDLE**：等待用户输入。
- **SENDING**：发送命令中。
- **WAITING**：等待结果。
- **POLLING**：轮询异步结果。
- **COMPLETED**：命令执行完成。

这种设计避免了复杂的 if-else 嵌套，使状态转换清晰可追踪。

### 10.4 Tunnel UI

`tunnel/` 目录下的 UI 是 Tunnel Server 的管理界面，提供：

- **应用列表**（Apps.vue）：显示所有注册的应用及其 Agent 数量。
- **Agent 列表**（Agent.vue）：显示某个应用下的所有 Agent，支持点击直接连接。

---

## 第十一阶段：关键设计问题深入分析

### 11.1 为什么 Arthas 同时支持 Telnet 和 HTTP 两种协议？

| 维度 | Telnet | HTTP/WebSocket |
|------|--------|----------------|
| 客户端 | 系统自带 telnet 命令 | 浏览器/curl/程序化调用 |
| 交互方式 | 字符流式 | 请求-响应 / 双工 |
| 功能 | 完整的终端交互 | 终端交互 + REST API |
| 适用场景 | 开发环境快速调试 | 生产环境自动化 |
| 安全 | 明文传输 | 支持 HTTPS |

设计哲学：**覆盖不同用户群体**。运维人员习惯 Telnet，前端/全栈工程师习惯 Web UI，自动化脚本需要 REST API。

### 11.2 Tunnel 通信解决了什么问题？

Tunnel 解决的核心问题是**网络可达性**。在以下场景中，传统的直连方式行不通：

1. **容器化部署**：Pod 没有固定 IP，端口映射复杂。
2. **防火墙限制**：只允许出站连接，不允许入站连接。
3. **多集群管理**：需要从一个入口访问多个集群的 JVM。

Tunnel 通过反向连接（Agent 主动连接 Server）巧妙地绕过了这些限制。

### 11.3 Session 超时机制如何防止资源泄漏？

Arthas 有两层超时保护：

1. **Shell Session 超时**（`ShellServerImpl.evictSessions`）：
   - 检查 `lastAccessedTime`（来自 `TermImpl.write` 的更新）。
   - 额外检查是否有运行中的 Job（避免误关正在执行 trace 的会话）。
   - 默认 30 分钟超时。

2. **HTTP API Session 超时**（`SessionManagerImpl.cleanExpiredSessions`）：
   - 检查 `lastAccessTime`（来自 `updateAccessTime` 的更新）。
   - 不检查运行中的 Job（HTTP API 会话的 Job 是通过轮询获取的）。
   - 默认 30 分钟超时。

### 11.4 HTTP API 与 Telnet 的区别和联系

**联系**：
- 都通过 `TermServer` 接入。
- 共享同一个 `ShellServer` 实例。
- 使用相同的命令体系和 Job 系统。

**区别**：
- Telnet 使用 `ShellServerImpl` 管理的 `ShellImpl` 会话（长连接模式）。
- HTTP API 使用 `SessionManager` 管理的独立 Session（无连接模式）。
- HTTP API 通过 `ApiTerm`（虚拟终端）执行命令，输出收集到 `ResultDistributor`。
- Telnet 通过 `TermImpl`（真实终端）交互，输出直接写入连接。

### 11.5 安全认证在什么情况下会强制启用？

| 条件 | 是否强制认证 |
|------|------------|
| 监听 127.0.0.1 | 不强制 |
| 监听 0.0.0.0 | **强制**（自动生成随机密码） |
| 用户配置了 username/password | 强制 |
| 本地 Telnet 连接 | 自动通过（LocalConnectionPrincipal） |
| 远程连接无认证信息 | 返回 401 |

### 11.6 WebUI 相比 Telnet 有什么优势？

| 维度 | Telnet | WebUI |
|------|--------|-------|
| 视觉效果 | 纯文本 | 富文本 + 语法高亮 |
| 访问方式 | 需要 telnet 客户端 | 浏览器即可 |
| 远程访问 | 需要目标机器可达 | 通过 Tunnel 访问 |
| 多人协作 | 不支持 | 支持 JOIN_SESSION |
| 自动化 | 需要 expect 脚本 | REST API |
| 安全性 | 明文 | 支持 HTTPS |
| 状态管理 | 简单文本流 | 状态机驱动 |

---

## 第十二阶段：TunnelServer Web 控制器

### 12.1 StatController —— 统计接口

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/app/web/StatController.java`

StatController 提供 RESTful API 供 Web UI 查询 Agent 信息：

- `GET /api/tunnelAgents`：获取所有已注册的 Agent 列表。
- 返回 Agent ID、主机、端口、版本等信息。

Web UI 的 Apps.vue 和 Agent.vue 页面通过这些 API 获取数据并展示。

### 12.2 ProxyController —— 代理控制器

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/app/web/ProxyController.java`

ProxyController 实现了通过 Tunnel Server 代理访问 Arthas 的 HTTP API：

```
浏览器 → POST /proxy/{agentId}/api → TunnelServer
    → WebSocket(httpProxy) → TunnelClient
    → 本地 HTTP 请求 → Arthas API
    → 响应原路返回
```

这使得浏览器可以不直接连接 Agent，而是通过 Tunnel Server 代理所有的 HTTP API 请求。

### 12.3 ArthasProperties —— 配置属性

**源码位置**: `tunnel-server/src/main/java/com/alibaba/arthas/tunnel/server/app/configuration/ArthasProperties.java`

```java
@ConfigurationProperties(prefix = "arthas")
public class ArthasProperties {
    /**
     * tunnel server listen host
     */
    private String server_host;
    /**
     * tunnel server listen port
     */
    private int server_port = 7777;
    /**
     * 是否启用 ssl
     */
    private boolean server_ssl = false;
    /**
     * tunnel server的websocket path
     */
    private String server_path = "/ws";
    /**
     * 连接到tunnel server的host
     */
    private String server_clientConnectHost;
    /**
     * enable detail pages
     */
    private boolean enableDetailPages = false;
}
```

Spring Boot 配置属性，支持通过 `application.properties` 或环境变量配置 Tunnel Server。

---

## 第十三阶段：ArthasBootstrap 中的初始化流程

### 13.1 TermServer 注册

在 `ArthasBootstrap` 的 `bind` 方法中，两种 TermServer 被创建并注册到 ShellServer：

```java
// 创建 HttpTelnetTermServer（同端口 Telnet + HTTP）
shellServer.registerTermServer(
    TermServer.createHttpTelnetTermServer(
        configure.getIp(),
        configure.getTelnetPort(),
        options.getConnectionTimeout(),
        workerGroup,
        httpSessionManager));

// 创建 HttpTermServer（独立 HTTP 端口）
shellServer.registerTermServer(
    TermServer.createHttpTermServer(
        configure.getIp(),
        configure.getHttpPort(),
        options.getConnectionTimeout()));
```

### 13.2 TunnelClient 初始化

```java
if (configure.getTunnelServer() != null) {
    tunnelClient = new TunnelClient();
    tunnelClient.setTunnelServerUrl(
        configure.getTunnelServer());
    tunnelClient.setAppName(configure.getAppName());
    tunnelClient.setId(configure.getAgentId());
    tunnelClient.setVersion(ArthasBanner.version());
    tunnelClient.start();
}
```

只有当用户配置了 `tunnelServer` 参数时，才会创建 TunnelClient。

### 13.3 SessionManager 初始化

```java
sessionManager = new SessionManagerImpl(
    options, commandManager, jobController);
httpApiHandler = new HttpApiHandler(
    historyManager, sessionManager);
```

SessionManager 与 HttpApiHandler 在 ArthasBootstrap 中创建，作为 HTTP API 的核心组件。

---

## 第十四阶段：完整的数据流追踪

### 14.1 Telnet 命令执行全链路

```
1. 用户输入: telnet 127.0.0.1 3658
2. ProtocolDetectHandler: 1秒无数据 → 判定为Telnet
3. TelnetChannelHandler → TelnetTtyConnection 创建
4. Consumer<TtyConnection>.accept() 回调
5. new TermImpl(conn) 包装为 Term
6. termHandler.handle(term) → ShellServerImpl.handleTerm()
7. createShell(term) → new ShellImpl(...)
8. ShellImpl 构造: 检测到 TelnetTtyConnection
   → AuthUtils.localPrincipal() 判断是否本地连接
   → SecurityAuthenticator.login() 认证
9. session.init() → 注册中断/挂起/关闭处理器
10. term.write(welcome) → 显示欢迎信息
11. session.readline() → 显示 [arthas@pid]$ 提示符
12. 用户输入命令 → ShellLineHandler.handle()
13. createJob(commandLine) → Job.run()
14. 命令执行结果 → term.write(result)
15. 执行完成 → ShellJobHandler.onTerminated()
    → session.readline() 再次显示提示符
```

### 14.2 HTTP API 命令执行全链路

```
1. 客户端: POST /api {"action":"exec","command":"version"}
2. ProtocolDetectHandler: 前3字节是"POS" → 判定为HTTP
3. HttpServerCodec → HttpObjectAggregator
4. BasicHttpAuthenticatorHandler: 检查认证
5. HttpRequestHandler: 路径为 /api
   → httpApiHandler.handle()
6. parseRequest(body) → ApiRequest
7. processRequest() → processExecRequest()
8. sessionManager.createSession() → 创建一次性Session
9. createJob(command, session, packingDistributor)
10. job.run() → 命令执行
11. waitForJob(job, 30000) → 阻塞等待
12. packingDistributor.getResults() → 收集结果
13. JSON.toJSONBytes(response) → 序列化
14. 返回 HTTP 200 + JSON Body
15. 一次性Session自动清理
```

### 14.3 Tunnel 交互全链路

```
1. Agent 启动: TunnelClient.start()
   → connect(false) → WebSocket 连接 TunnelServer
2. TunnelSocketFrameHandler.agentRegister()
   → 生成/确认 agentId
   → agentInfoMap.put(id, info)
   → 响应 agentRegister 成功
3. TunnelClientSocketClientHandler.channelRead0()
   → 收到 agentRegister 响应
   → tunnelClient.setId(id)
   → registerPromise.setSuccess()

4. 浏览器连接: WebSocket 连接 TunnelServer
   → method=connectArthas&id=xxx
5. TunnelSocketFrameHandler.connectArthas()
   → findAgent(agentId) → 找到Agent
   → 创建 Promise<Channel>
   → 通知Agent: startTunnel(clientConnectionId)
   → promise.await(20秒)

6. TunnelClientSocketClientHandler.channelRead0()
   → 收到 startTunnel 请求
   → new ForwardClient(openTunnelUri).start()
   → ForwardClient 建立第二条 WebSocket 到 TunnelServer

7. TunnelSocketFrameHandler.openTunnel()
   → findClientConnection(clientConnectionId)
   → promise.setSuccess(channel)

8. Promise 回调触发:
   → 移除 TunnelSocketFrameHandler
   → 添加 RelayHandler（双向）
   → 浏览器 ←→ TunnelServer ←→ Agent 数据直传

9. ForwardClient 同时连接 Arthas 本地 WebSocket
   → LocalFrameHandler 桥接
   → TunnelServer数据 ←→ 本地Arthas终端
```

---

## 第十五阶段：设计模式与架构总结

### 15.1 使用的设计模式

| 模式 | 应用位置 | 说明 |
|------|---------|------|
| 策略模式 | TermServer 体系 | 不同协议的终端服务器 |
| 工厂方法 | TermServer.create*() | 创建具体实现 |
| 观察者模式 | Handler/Future | 事件通知机制 |
| 中介者模式 | TunnelServer | 协调Agent和浏览器 |
| 代理模式 | ProxyClient/ProxyController | HTTP 代理 |
| 管道模式 | Netty Pipeline | 请求处理链 |
| 桥接模式 | TermImpl/TtyConnection | 分离抽象和实现 |
| 空对象模式 | ApiTerm | 虚拟终端 |

### 15.2 线程模型

```
┌─────────────────────────────────────────────────────┐
│ Arthas Core 线程模型                                 │
│                                                     │
│ NioEventLoopGroup (boss)     ── 接受新连接           │
│ NioEventLoopGroup (worker)   ── IO 读写              │
│ EventExecutorGroup (worker)  ── HTTP 请求处理         │
│ arthas-shell-server          ── 会话超时清理          │
│ arthas-session-manager       ── API Session 清理     │
│ arthas-TunnelClient          ── Tunnel 通信          │
│ arthas-command-execute       ── 命令执行              │
└─────────────────────────────────────────────────────┘
```

### 15.3 核心类关系图

```
ArthasBootstrap
  ├── ShellServerImpl
  │     ├── List<TermServer>
  │     │     ├── HttpTelnetTermServer
  │     │     │     └── NettyHttpTelnetTtyBootstrap
  │     │     │           └── NettyHttpTelnetBootstrap
  │     │     │                 └── ProtocolDetectHandler
  │     │     └── HttpTermServer
  │     │           └── NettyWebsocketTtyBootstrap
  │     ├── Map<String, ShellImpl> sessions
  │     │     └── ShellImpl
  │     │           ├── SessionImpl (implements Session)
  │     │           ├── TermImpl (implements Term)
  │     │           │     └── TtyConnection
  │     │           │           ├── TelnetTtyConnection
  │     │           │           └── ExtHttpTtyConnection
  │     │           └── JobControllerImpl
  │     └── BuiltinCommandResolver
  ├── SessionManagerImpl
  │     └── Map<String, Session> sessions
  ├── HttpApiHandler
  │     └── ApiTerm (虚拟终端)
  ├── SecurityAuthenticatorImpl
  ├── HistoryManagerImpl
  └── TunnelClient (可选)
        └── TunnelClientSocketClientHandler
              └── ForwardClient
                    ├── ForwardClientSocketClientHandler
                    └── LocalFrameHandler
```

---

## 总结

本文从源码层面深入分析了 Arthas 的 Session 管理、Tunnel 通信和 WebUI 交互三大体系。核心发现如下：

1. **协议检测设计**：Arthas 通过 `ProtocolDetectHandler` 在同一端口自动区分 Telnet 和 HTTP 协议，只需读取前 3 个字节即可判定。这是一种极其巧妙的设计，大大简化了用户的使用体验。

2. **分层架构**：从底层的 `TtyConnection` 到中层的 `Term` 再到上层的 `Shell`，每一层都有清晰的职责边界。不同的连接方式（Telnet/WebSocket/HTTP API）在底层有不同的实现，但在上层完全统一。

3. **双 Session 体系**：`ShellServerImpl` 管理交互式会话，`SessionManager` 管理 HTTP API 会话。两者各有适用场景，互不干扰。

4. **Tunnel 反向代理**：通过 Agent 主动连接 Server 的方式，解决了容器环境和防火墙限制下的远程管理问题。隧道建立后，Pipeline 被替换为简单的 `RelayHandler`，实现高效的双向数据转发。

5. **安全认证**：Arthas 在监听 `0.0.0.0` 时强制启用认证，自动生成随机密码，有效防止了未授权访问。本地连接通过 `LocalConnectionPrincipal` 自动认证。

6. **命令历史持久化**：HistoryManager 将命令历史保存到文件，跨会话共享，上限 500 条，使用 synchronized 保证线程安全。

整个体系的设计体现了 Arthas 团队对**易用性**和**安全性**的平衡追求：用户只需要一个端口就能访问所有功能，同时在安全敏感的场景下自动启用保护措施。