# Dubbo Exchange 与 Transport 分层设计详解

> 本文专门解答一个问题：从 `DubboProtocol` 到 Netty 真正的 `connect()`/`bind()`，中间为什么要经过 Exchangers -> HeaderExchanger -> Transporters -> NettyTransporter 这么多层嵌套？每一层的"真面目"是什么？它存在的意义是什么？去掉它会怎样？

---

## 先看完整调用链（Consumer 建连为例）

```
DubboProtocol.initClient(url)
  |
  +-- Exchangers.connect(url, requestHandler)          [静态工厂入口]
        |
        +-- HeaderExchanger.connect(url, handler)      [Exchange 层实现]
              |
              +-- 包装 Handler 链:
              |     handler -> HeaderExchangeHandler -> DecodeHandler
              |
              +-- Transporters.connect(url, wrappedHandler)  [静态工厂入口]
              |       |
              |       +-- NettyTransporter.connect(url, handler)  [Transport 层实现]
              |             |
              |             +-- new NettyClient(url, handler)
              |                   |
              |                   +-- doConnect()
              |                         |
              |                         +-- bootstrap.connect(address)  <-- TCP 连接!
              |
              +-- new HeaderExchangeClient(transportClient, needHeartbeat)
                    [包装成 Exchange 语义的 Client]
```

看起来绕了好多层，但每一层做的事情完全不同。下面逐层拆解。

---

## 第一层：Exchangers —— 静态工厂入口（门面模式）

### 它是什么？

`Exchangers` 是一个纯粹的**静态工具类**（门面 / Facade），不包含任何逻辑。它的全部代码就是：

```java
public class Exchangers {

    public static ExchangeClient connect(URL url, ExchangeHandler handler) {
        url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
        // 通过 SPI 获取 Exchanger 实现（默认 "header"）
        return getExchanger(url).connect(url, handler);
    }

    public static ExchangeServer bind(URL url, ExchangeHandler handler) {
        url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
        return getExchanger(url).bind(url, handler);
    }

    private static Exchanger getExchanger(URL url) {
        String type = url.getParameter(EXCHANGER_KEY, "header");
        return ExtensionLoader.getExtensionLoader(Exchanger.class).getExtension(type);
    }
}
```

### 它为什么存在？

你可能会问：直接调 `HeaderExchanger.connect()` 不就行了，为什么要多一个 Exchangers？

原因是 **SPI 可替换性**。`Exchanger` 是一个 SPI 接口：

```java
@SPI("header")
public interface Exchanger {
    ExchangeServer bind(URL url, ExchangeHandler handler);
    ExchangeClient connect(URL url, ExchangeHandler handler);
}
```

默认实现是 `HeaderExchanger`，但理论上你可以实现自己的 Exchanger（比如用 AMQP 消息队列做请求-响应模式）。`Exchangers` 工具类负责根据 URL 参数动态加载对应的 SPI 实现。调用方（DubboProtocol）不需要知道具体用的是哪个 Exchanger。

**如果去掉它？** DubboProtocol 就得直接 `new HeaderExchanger().connect(...)`，丧失了通过配置切换 Exchanger 实现的能力。

---

## 第二层：HeaderExchanger —— Exchange 层的核心（请求-响应语义）

### 它是什么？

`HeaderExchanger` 是 `Exchanger` SPI 的默认实现。它做两件事：

1. **包装 Handler 链**（给 Handler 加功能）
2. **创建 Exchange 语义的 Client/Server**（在 Transport 之上叠加请求-响应模型）

```java
public class HeaderExchanger implements Exchanger {

    @Override
    public ExchangeClient connect(URL url, ExchangeHandler handler) {
        // 1. 包装 Handler 链
        //    handler = DubboProtocol 的 requestHandler（处理回调请求等）
        //    -> HeaderExchangeHandler（管理请求-响应模型）
        //    -> DecodeHandler（延迟解码，在业务线程中反序列化）
        DecodeHandler wrappedHandler = new DecodeHandler(new HeaderExchangeHandler(handler));

        // 2. 通过 Transport 层建立底层连接
        Client transportClient = Transporters.connect(url, wrappedHandler);

        // 3. 包装成 Exchange 语义的 Client
        return new HeaderExchangeClient(transportClient, true);
    }

    @Override
    public ExchangeServer bind(URL url, ExchangeHandler handler) {
        // 同理，Server 端也是：包装 Handler + 绑定端口 + 包装成 ExchangeServer
        return new HeaderExchangeServer(
            Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler)))
        );
    }
}
```

### 它为什么存在？解决了什么问题？

**核心问题：Transport 层只管"发消息"和"收消息"，它不知道"请求"和"响应"是什么东西。**

想象一下，如果只有 Transport 层（NettyClient/NettyServer），你能做什么？

- 你能 `channel.send(bytes)` 发一坨字节
- 你能收到对方发来的一坨字节

但你没法回答以下问题：
- 我发了 3 个请求，现在收到 1 个响应，这个响应对应的是第几个请求？
- 我发了一个请求，3 秒还没收到回复，要超时报错
- 收到的消息是一个请求还是一个响应？如果是请求我该怎么处理？

**这些"请求-响应"的语义就是 Exchange 层的职责。**

Exchange 层叠加了以下能力：

| 能力 | 实现类 | 具体做什么 |
|------|--------|-----------|
| 请求-响应匹配 | HeaderExchangeChannel + DefaultFuture | 每个请求生成唯一 requestId，存入 FUTURES map；收到响应按 ID 匹配并 complete Future |
| 超时检测 | DefaultFuture + HashedWheelTimer | 请求发出后启动定时任务，到时间没响应就 completeExceptionally |
| 心跳保活 | HeaderExchangeClient / HeaderExchangeServer | 定时发心跳包检测连接活性，连续 N 次心跳无响应则判定连接断开 |
| 消息类型识别 | HeaderExchangeHandler | 区分 Request/Response/Heartbeat 三种消息类型，分发到不同处理逻辑 |
| 延迟解码 | DecodeHandler | Body 的反序列化延迟到业务线程执行，不阻塞 IO 线程 |

**类比**：Transport 层相当于邮局（只管寄信收信），Exchange 层相当于信封上的"挂号编号"（你寄了 3 封挂号信，回执按编号匹配哪封信已送达）。没有 Exchange 层，你寄了信但永远不知道对方收没收到、回复的是哪封。

**如果去掉它？** 你的 DubboProtocol 就得自己管理 requestId、自己维护 FUTURES map、自己做超时检测、自己发心跳包... 所有这些跟具体业务协议无关的"请求-响应"公共逻辑都得每个 Protocol 实现重写一遍。

---

## Handler 包装链详解

HeaderExchanger 做的第一件事是包装 Handler 链。包了两层，每层的职责如下：

```
你传入的 handler（DubboProtocol.requestHandler）
    |
    | 被包装为
    v
HeaderExchangeHandler
    |   职责：
    |   - 收到 Request 类型消息 -> 调用 handler.reply() 处理请求 -> 拿到 Future -> Future 完成后写回 Response
    |   - 收到 Response 类型消息 -> 调 DefaultFuture.received() 匹配请求
    |   - 收到心跳请求 -> 直接返回心跳响应
    |   - 简单来说就是"请求-响应模型的状态机"
    |
    | 被包装为
    v
DecodeHandler
    |   职责：
    |   - 如果消息还没反序列化（Decodeable 状态），在当前线程（业务线程）完成反序列化
    |   - 这样可以避免在 IO 线程做耗时的反序列化操作
    |
    | 传给 Transport 层
    v
Transport 层（NettyClient / NettyServer）内部还会再包装一层：
    AllChannelHandler（或其他 Dispatcher 实现）
        职责：把消息从 IO 线程 dispatch 到业务线程池
```

**完整的消息处理顺序（收到消息时，从外到内）：**

```
Netty IO 线程:
  NettyServerHandler.channelRead(msg)
    -> MultiMessageHandler.received(msg)       // 处理批量消息
      -> HeartbeatHandler.received(msg)        // 快速过滤心跳
        -> AllChannelHandler.received(msg)     // === 线程切换点 ===
                                               // 提交到业务线程池

业务线程:
  ChannelEventRunnable.run()
    -> DecodeHandler.received(msg)             // 延迟解码（反序列化）
      -> HeaderExchangeHandler.received(msg)   // 识别消息类型并分发
        -> 如果是 Request:
             handler.reply(channel, msg)       // 调用 DubboProtocol.requestHandler
        -> 如果是 Response:
             DefaultFuture.received(response)  // 匹配 Future 并 complete
```

---

## 第三层：Transporters —— 又一个静态工厂入口

### 它是什么？

和 `Exchangers` 完全同理，`Transporters` 也是一个静态工具类门面：

```java
public class Transporters {

    public static Client connect(URL url, ChannelHandler... handlers) {
        ChannelHandler handler;
        if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            handler = new ChannelHandlerDispatcher(handlers);
        }
        return getTransporter(url).connect(url, handler);
    }

    public static RemotingServer bind(URL url, ChannelHandler... handlers) {
        // 同上
        return getTransporter(url).bind(url, handler);
    }

    private static Transporter getTransporter(URL url) {
        return ExtensionLoader.getExtensionLoader(Transporter.class)
                .getAdaptiveExtension();  // 根据 URL 的 transporter 参数选择实现
    }
}
```

### 它为什么存在？

`Transporter` 是 SPI 接口：

```java
@SPI("netty")
public interface Transporter {
    @Adaptive({Constants.SERVER_KEY, Constants.TRANSPORTER_KEY})
    RemotingServer bind(URL url, ChannelHandler handler);

    @Adaptive({Constants.CLIENT_KEY, Constants.TRANSPORTER_KEY})
    Client connect(URL url, ChannelHandler handler);
}
```

默认实现是 `NettyTransporter`（基于 Netty 4）。但你可以通过配置 `transporter=mina` 切换到 Mina 实现（Dubbo 历史上确实支持过 Mina）。`Transporters` 工具类就是那个根据 URL 参数动态选择具体 Transporter 实现的"路由器"。

**如果去掉它？** HeaderExchanger 就得直接 `new NettyTransporter().connect(...)`，写死了网络框架，丧失了通过配置切换底层网络实现的能力。

---

## 第四层：NettyTransporter —— Transport 层实现（网络传输）

### 它是什么？

`NettyTransporter` 是 `Transporter` SPI 的默认实现，极其简单：

```java
public class NettyTransporter implements Transporter {

    @Override
    public RemotingServer bind(URL url, ChannelHandler handler) {
        return new NettyServer(url, handler);  // 直接 new，构造函数里完成端口绑定
    }

    @Override
    public Client connect(URL url, ChannelHandler handler) {
        return new NettyClient(url, handler);  // 直接 new，构造函数里完成 TCP 连接
    }
}
```

### 它为什么存在？

它就是"具体的网络框架适配器"。它的职责是：

- **Client 端**：创建 NettyClient，配置 Netty Bootstrap（EventLoopGroup、Pipeline、ChannelHandler），调用 `bootstrap.connect()` 建立 TCP 连接
- **Server 端**：创建 NettyServer，配置 ServerBootstrap，调用 `bootstrap.bind()` 监听端口

**核心能力**：连接管理（建连/断连/重连）、心跳发送、编解码 Pipeline 配置、IO 线程模型。

**如果去掉它？** 那就没有网络了。它是真正干活的那个人。

---

## 为什么分两层（Exchange + Transport）而不是一层？

这是整个设计最关键的问题。先看它们各自的抽象层次：

| 维度 | Transport 层 | Exchange 层 |
|------|-------------|-------------|
| 抽象级别 | 消息级（Message） | 请求-响应级（Request-Response） |
| 核心接口 | `Channel.send(Object msg)` | `ExchangeChannel.request(Object req, int timeout)` |
| 返回值 | void（发了就完事，不管对方收没收到） | `CompletableFuture<Object>`（等对方回复） |
| 关心的事 | 连接建立/断开/重连、编解码、IO线程模型 | 请求和响应的对应关系、超时检测、心跳 |
| 可替换的是什么 | 底层网络框架（Netty/Mina/Grizzly） | 请求-响应的实现方式（Header协议/AMQP/...） |

**一个形象的比喻：**

- **Transport 层 = 快递公司**：负责把包裹从 A 寄到 B。它不关心包裹里是什么、是不是需要回执、有没有时效要求。你调 `send()` 它就帮你寄。
- **Exchange 层 = 挂号信系统**：在快递公司之上，它给每个包裹编了号（requestId），要求收件人回一个带同样编号的回执（Response）。如果超过 3 天没收到回执（timeout），就标记为丢失（TimeoutException）。

两者的关注点完全不同，所以分层。这样带来的好处：

1. **替换 Transport 不影响 Exchange**：你把 Netty 换成 Mina，Exchange 层的请求-响应匹配逻辑一行不用改
2. **替换 Exchange 不影响 Transport**：你要支持一种新的请求-响应协议（比如 AMQP 的 correlationId 匹配），只需实现新的 Exchanger，底层还是用 Netty
3. **职责单一、可测试**：Transport 层的单元测试只关心"消息能不能发出去/收到"；Exchange 层的单元测试只关心"requestId 匹配对不对、超时触发准不准"

---

## Exchange 层的核心类详解

### HeaderExchangeClient

```java
public class HeaderExchangeClient implements ExchangeClient {

    private final Client client;          // 底层 Transport Client（NettyClient）
    private final ExchangeChannel channel; // HeaderExchangeChannel，真正干活的
    private HashedWheelTimer timer;        // 心跳定时器

    public HeaderExchangeClient(Client client, boolean startTimer) {
        this.client = client;
        this.channel = new HeaderExchangeChannel(client);

        if (startTimer) {
            // 启动心跳定时任务
            startHeartBeatTask();
            // 启动重连定时任务
            startReconnectTask();
        }
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) {
        // 委托给 HeaderExchangeChannel
        return channel.request(request, timeout, executor);
    }
}
```

**它的真面目**：在 Transport Client 之上加了心跳保活和重连机制。它是你"拿在手里用"的那个 Client 对象。

### HeaderExchangeChannel

```java
public class HeaderExchangeChannel implements ExchangeChannel {

    private final Channel channel;  // 底层 Transport Channel（NettyChannel）

    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) {
        // 1. 构造 Request 对象，分配 requestId
        Request req = new Request();
        req.setVersion(Version.getProtocolVersion());
        req.setTwoWay(true);
        req.setData(request);  // 你的 RpcInvocation

        // 2. 创建 DefaultFuture（以 requestId 为 key 存入全局 map）
        DefaultFuture future = DefaultFuture.newFuture(channel, req, timeout, executor);

        // 3. 通过底层 Transport Channel 发送
        try {
            channel.send(req);  // -> NettyChannel.send() -> Netty writeAndFlush
        } catch (RemotingException e) {
            future.cancel();
            throw e;
        }

        // 4. 返回 Future
        return future;
    }
}
```

**它的真面目**：Exchange 层最核心的类。`request()` 方法就是"请求-响应"语义的实现——分配 ID、存 Future、发消息、返回 Future。这是 Transport 层的 `send()` 所不具备的能力。

### HeaderExchangeHandler

```java
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    private final ExchangeHandler handler;  // DubboProtocol 的 requestHandler

    @Override
    public void received(Channel channel, Object message) {
        if (message instanceof Request) {
            Request request = (Request) message;
            if (request.isEvent()) {
                handlerEvent(channel, request);
            } else if (request.isTwoWay()) {
                // 双向请求：调用业务 handler 处理，拿到结果后写回 Response
                handleRequest(channel, request);
            } else {
                // 单向请求：只处理不回复
                handler.received(channel, request.getData());
            }
        } else if (message instanceof Response) {
            // 响应消息：匹配到对应的 DefaultFuture 并 complete
            handleResponse(channel, (Response) message);
        }
    }

    private void handleRequest(ExchangeChannel channel, Request req) {
        Response res = new Response(req.getId(), req.getVersion());
        Object msg = req.getData();

        // 调用业务逻辑（DubboProtocol.requestHandler.reply）
        CompletableFuture<Object> future = handler.reply(channel, msg);

        // 异步等结果，完成后写回 Response
        future.whenComplete((appResult, t) -> {
            if (t == null) {
                res.setStatus(Response.OK);
                res.setResult(appResult);
            } else {
                res.setStatus(Response.SERVICE_ERROR);
                res.setErrorMessage(t.getMessage());
            }
            channel.send(res);  // 写回响应
        });
    }

    private void handleResponse(Channel channel, Response response) {
        // 核心：通过 requestId 找到 Future 并 complete
        DefaultFuture.received(channel, response);
    }
}
```

**它的真面目**：消息类型的"分发器"。收到消息后判断是 Request 还是 Response，然后走不同的处理路径。对于 Request，它负责调用业务逻辑并把结果封装成 Response 写回；对于 Response，它负责匹配到对应的 Future。

### DefaultFuture

```java
public class DefaultFuture extends CompletableFuture<Object> {

    // 全局映射：requestId -> Future
    private static final Map<Long, DefaultFuture> FUTURES = new ConcurrentHashMap<>();

    private final long id;        // requestId
    private final int timeout;    // 超时时间
    private Timeout timeoutTask;  // HashedWheelTimer 的超时任务句柄

    public static DefaultFuture newFuture(Channel channel, Request request, int timeout, ExecutorService executor) {
        DefaultFuture future = new DefaultFuture(channel, request, timeout);
        // 存入全局 map
        FUTURES.put(request.getId(), future);
        // 注册超时检查
        timeoutCheck(future);
        return future;
    }

    // 收到响应时调用
    public static void received(Channel channel, Response response) {
        DefaultFuture future = FUTURES.remove(response.getId());
        if (future != null) {
            future.cancelTimeoutCheckTask();
            future.doReceived(response);
        }
    }

    private void doReceived(Response res) {
        if (res.getStatus() == Response.OK) {
            this.complete(res.getResult());
        } else {
            this.completeExceptionally(new RemotingException(res.getErrorMessage()));
        }
    }

    // HashedWheelTimer 超时触发
    private static void timeoutCheck(DefaultFuture future) {
        TimeoutCheckTask task = new TimeoutCheckTask(future.getId());
        future.timeoutTask = TIME_OUT_TIMER.newTimeout(task, future.getTimeout(), MILLISECONDS);
    }

    // 超时任务
    private static class TimeoutCheckTask implements TimerTask {
        private final long requestId;

        @Override
        public void run(Timeout timeout) {
            DefaultFuture future = FUTURES.remove(requestId);
            if (future != null && !future.isDone()) {
                future.completeExceptionally(new TimeoutException("Timeout waiting for response"));
            }
        }
    }
}
```

**它的真面目**：整个 Exchange 层的"灵魂"。请求-响应匹配 + 超时管理全靠它。一个 `ConcurrentHashMap<Long, DefaultFuture>` 就是多路复用的全部秘密。

---

## Transport 层的核心类详解

### NettyClient

```java
public class NettyClient extends AbstractClient {

    private Bootstrap bootstrap;
    private volatile Channel channel;  // 当前活跃的 Netty Channel

    @Override
    protected void doOpen() {
        // 配置 Netty Bootstrap
        bootstrap = new Bootstrap();
        bootstrap.group(NIO_EVENT_LOOP_GROUP)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, getConnectTimeout())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    // Pipeline 配置
                    ch.pipeline()
                        .addLast("decoder", adapter.getDecoder())    // DubboCodec 解码
                        .addLast("encoder", adapter.getEncoder())    // DubboCodec 编码
                        .addLast("client-idle-handler",
                            new IdleStateHandler(heartbeatInterval, 0, 0, MILLISECONDS))
                        .addLast("handler", new NettyClientHandler(getUrl(), this));
                    // NettyClientHandler 桥接 Netty 事件到 Dubbo Handler 链
                }
            });
    }

    @Override
    protected void doConnect() {
        // 真正的 TCP 连接
        ChannelFuture future = bootstrap.connect(getConnectAddress());
        boolean ret = future.awaitUninterruptibly(getConnectTimeout(), MILLISECONDS);
        if (ret && future.isSuccess()) {
            this.channel = future.channel();
        } else {
            throw new RemotingException("Connect failed");
        }
    }

    @Override
    public void send(Object message, boolean sent) {
        Channel ch = getChannel();
        ch.writeAndFlush(message);  // Netty 发送
    }
}
```

**它的真面目**：Netty 的封装。管理 Bootstrap、Channel、编解码 Pipeline、连接生命周期（建连/断连/重连）。它只负责"把消息写进 TCP 连接"和"从 TCP 连接读出消息"，不关心消息的语义。

### NettyServer

```java
public class NettyServer extends AbstractServer {

    private ServerBootstrap bootstrap;
    private Channel channel;

    @Override
    protected void doOpen() {
        bootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(getUrl().getPositiveParameter(IO_THREADS_KEY, CPU_COUNT + 1));

        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast("decoder", adapter.getDecoder())
                        .addLast("encoder", adapter.getEncoder())
                        .addLast("server-idle-handler",
                            new IdleStateHandler(0, 0, heartbeatInterval * 3, MILLISECONDS))
                        .addLast("handler", new NettyServerHandler(getUrl(), NettyServer.this));
                }
            });

        // 绑定端口
        ChannelFuture channelFuture = bootstrap.bind(getBindAddress());
        channelFuture.syncUninterruptibly();
        channel = channelFuture.channel();
    }
}
```

---

## 把所有层串起来：一次完整的请求发送

当你调用 `userService.getUserById(1)` 时，消息从上到下穿过每一层：

```
你的代码: userService.getUserById(1)
    |
    v
[Proxy 层] InvokerInvocationHandler
    封装为 RpcInvocation{methodName="getUserById", args=[1]}
    |
    v
[Cluster 层] FailoverClusterInvoker
    负载均衡选出一个 DubboInvoker
    |
    v
[Protocol 层] DubboInvoker.doInvoke()
    选一条 ExchangeClient，调用 client.request(invocation, timeout)
    |
    v
[Exchange 层] HeaderExchangeChannel.request()
    1. new Request(自增ID=42)，req.setData(invocation)
    2. DefaultFuture.newFuture() -> FUTURES.put(42, future)
    3. 启动 HashedWheelTimer 超时检查（3000ms 后触发）
    4. channel.send(req)  --> 往下走
    5. return future
    |
    v
[Transport 层] NettyChannel.send(req)
    -> NioSocketChannel.writeAndFlush(req)
    |
    v
[Netty Pipeline - 出站]
    -> DubboCodec.encode(req)
       写 16 字节 Header（magic=0xdabb, requestId=42, ...）
       写 Body（Hessian2 序列化 RpcInvocation）
    -> TCP 发送字节流
```

响应回来时，从下到上穿过每一层：

```
TCP 收到字节流
    |
    v
[Netty Pipeline - 入站]
    -> DubboCodec.decode()
       读 Header -> 发现 requestId=42
       读 Body -> Response 对象
    |
    v
[Transport 层] NettyClientHandler.channelRead(response)
    -> handler.received(channel, response)  // 触发 Dubbo Handler 链
    |
    v
[线程切换] AllChannelHandler.received()
    把消息从 IO 线程 dispatch 到业务线程池
    |
    v (业务线程)
[Exchange 层 - DecodeHandler]
    完成延迟解码（反序列化 AppResponse）
    |
    v
[Exchange 层 - HeaderExchangeHandler]
    识别为 Response 类型
    -> DefaultFuture.received(channel, response)
       -> FUTURES.remove(42) 取出 Future
       -> future.complete(appResponse)
    |
    v
[回到调用线程]
    DefaultFuture 被 complete
    -> DubboInvoker 里的 appResponseFuture 完成
    -> AsyncRpcResult.recreate() 里 future.get() 返回
    -> InvokerInvocationHandler 返回业务结果
    |
    v
你的代码收到: User 对象
```

---

## 总结：每一层的一句话定位

| 层 | 核心类 | 一句话职责 | 没有它会怎样 |
|----|--------|-----------|-------------|
| Exchangers | 静态工具类 | SPI 路由入口，根据配置选择 Exchanger 实现 | 写死 HeaderExchanger，无法切换 |
| HeaderExchanger | Exchanger SPI 实现 | 组装 Handler 链 + 创建 Exchange Client/Server | Handler 链没人组装，心跳/解码/请求分发没人做 |
| HeaderExchangeClient | ExchangeClient | 在 Transport Client 上加心跳保活和重连 | 连接断了没人发现，没人自动重连 |
| HeaderExchangeChannel | ExchangeChannel | request() 方法：分配 ID + 存 Future + 发消息 + 返回 Future | 只能 send 不能 request，没有请求-响应语义 |
| HeaderExchangeHandler | ChannelHandler | 消息分发器：Request 走 reply，Response 走 Future 匹配 | 收到消息不知道是请求还是响应 |
| DefaultFuture | CompletableFuture | requestId-Future 映射 + 超时检测 | 多路复用没法做，超时没法检测 |
| DecodeHandler | ChannelHandler | 延迟解码，在业务线程做反序列化 | 反序列化阻塞 IO 线程 |
| Transporters | 静态工具类 | SPI 路由入口，根据配置选择 Transporter 实现 | 写死 NettyTransporter，无法切换网络框架 |
| NettyTransporter | Transporter SPI 实现 | 创建 NettyClient/NettyServer | 没有具体的网络实现 |
| NettyClient | Client | Netty Bootstrap + TCP connect + Channel 管理 | 没有网络连接 |
| NettyServer | Server | Netty ServerBootstrap + bind + 端口监听 | 没有端口监听 |

---

## 设计哲学

整个分层设计体现了三个原则：

**1. 关注点分离（Separation of Concerns）**

Transport 只管"字节怎么在网络上跑"，Exchange 只管"请求和响应怎么配对"。两者互不干扰。

**2. SPI 可扩展（Open for Extension）**

每一层都有 SPI 接口（Exchanger、Transporter），可以通过配置切换实现。你甚至可以写一个基于 Unix Domain Socket 的 Transporter，Exchange 层代码一行不用改。

**3. 装饰器模式（Handler 链）**

Handler 的层层包装（DecodeHandler -> HeaderExchangeHandler -> AllChannelHandler -> ...）让每一层只关注自己的职责，通过组合而非继承来增加功能。新增一个功能（比如加密）只需要新写一个 Handler 插进去。
