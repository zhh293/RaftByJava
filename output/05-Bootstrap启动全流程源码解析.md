# Bootstrap 启动全流程源码解析（Server 端 + Client 端）

> 基于 Netty 源码，完整解析 ServerBootstrap.bind() 和 Bootstrap.connect() 的全流程。用时序图展示 bind/connect 过程中线程的切换，标注每一步在哪个线程执行。

---

## 一、类继承体系与设计模式

```
AbstractBootstrap<B, C>          ← 抽象基类（模板方法模式 + Builder 模式）
├── ServerBootstrap              ← 服务端启动器
└── Bootstrap                    ← 客户端启动器
```

Bootstrap 的设计融合了两种经典设计模式：

**Builder 模式**：所有配置方法（`group()`、`channel()`、`option()`、`handler()` 等）都返回 `self()`（即 `(B) this`），支持链式调用。这使得"配置阶段"和"执行阶段"被清晰地分为两个步骤——先通过链式调用完成所有配置，最后调用 `bind()` 或 `connect()` 一键启动。

**模板方法模式**：`AbstractBootstrap` 定义了 `initAndRegister()` 的骨架算法，其中 `init(Channel)` 是抽象方法，由 `ServerBootstrap` 和 `Bootstrap` 各自实现具体的初始化逻辑。

---

## 二、AbstractBootstrap 核心字段

```java
public abstract class AbstractBootstrap<B extends AbstractBootstrap<B, C>, C extends Channel> {
    volatile EventLoopGroup group;                          // EventLoop 组
    private volatile ChannelFactory<? extends C> channelFactory;  // Channel 工厂
    private volatile SocketAddress localAddress;                   // 本地地址
    private final Map<ChannelOption<?>, Object> options = new LinkedHashMap<>();  // Channel 选项
    private final Map<AttributeKey<?>, Object> attrs = new ConcurrentHashMap<>(); // 自定义属性
    private volatile ChannelHandler handler;                      // Handler
}
```

ServerBootstrap 额外增加了 child 系列字段：

```java
public class ServerBootstrap extends AbstractBootstrap<ServerBootstrap, ServerChannel> {
    private volatile EventLoopGroup childGroup;        // worker 线程组
    private volatile ChannelHandler childHandler;       // 子 Channel 的 Handler
    private final Map<ChannelOption<?>, Object> childOptions = new LinkedHashMap<>();
    private final Map<AttributeKey<?>, Object> childAttrs = new ConcurrentHashMap<>();
}
```

---

## 三、channel() 方法 —— 反射工厂注册

```java
public B channel(Class<? extends C> channelClass) {
    return channelFactory(new ReflectiveChannelFactory<C>(channelClass));
}
```

`channel(NioServerSocketChannel.class)` 实际上注册了一个 `ReflectiveChannelFactory`，它通过反射获取无参构造器，在需要时创建 Channel 实例：

```java
public class ReflectiveChannelFactory<T extends Channel> implements ChannelFactory<T> {
    private final Constructor<? extends T> constructor;

    public ReflectiveChannelFactory(Class<? extends T> clazz) {
        this.constructor = clazz.getConstructor();  // 获取无参构造器
    }

    @Override
    public T newChannel() {
        return constructor.newInstance();  // 反射实例化
    }
}
```

这是工厂模式的典型应用——将 Channel 的具体类型与创建逻辑解耦。切换传输实现（NIO → Epoll → KQueue）只需要换一个 Class 参数。

---

## 四、Server 端 —— ServerBootstrap.bind(port) 全流程

### 4.1 完整调用链总览

```
ServerBootstrap.bind(port)                                          【用户线程】
  └→ AbstractBootstrap.doBind(localAddress)
       └→ initAndRegister()                                         【用户线程】
       │    ├→ channelFactory.newChannel()                          创建 NioServerSocketChannel
       │    ├→ init(channel)                                        配置 Pipeline + 添加 ChannelInitializer
       │    └→ config().group().register(channel)                   提交到 EventLoop
       │         └→ AbstractChannel.register()        ─────────→    【EventLoop 线程】
       │              ├→ doRegister()                               注册到 Selector（不监听任何事件）
       │              ├→ pipeline.invokeHandlerAddedIfNeeded()      触发 ChannelInitializer.initChannel()
       │              │    ├→ 添加用户 handler
       │              │    └→ eventLoop.execute(添加 ServerBootstrapAcceptor)
       │              ├→ pipeline.fireChannelRegistered()
       │              └→ pipeline.fireChannelActive()
       │
       └→ doBind0(regFuture, channel, localAddress, promise)        【用户线程提交 task】
            └→ channel.eventLoop().execute(...)       ─────────→    【EventLoop 线程】
                 └→ channel.bind(localAddress, promise)
                      └→ Pipeline 出站传播到 HeadContext
                           └→ unsafe.bind()
                                └→ doBind()
                                     └→ javaChannel().bind(localAddress, backlog)  JDK 绑定
```

### 4.2 入口：bind()

**【执行线程：用户线程】**

```java
public ChannelFuture bind(int inetPort) {
    return bind(new InetSocketAddress(inetPort));
}

public ChannelFuture bind(SocketAddress localAddress) {
    validate();  // 验证 group/channelFactory 都已设置
    return doBind(localAddress);
}
```

### 4.3 doBind() —— 编排 initAndRegister + doBind0

**【执行线程：用户线程开始，后续可能切换到 EventLoop 线程】**

```java
private ChannelFuture doBind(final SocketAddress localAddress) {
    // 步骤1：创建 Channel + 初始化 + 注册到 EventLoop
    final ChannelFuture regFuture = initAndRegister();
    final Channel channel = regFuture.channel();

    if (regFuture.cause() != null) {
        return regFuture;  // 创建/初始化失败，直接返回
    }

    if (regFuture.isDone()) {
        // 情况 A：注册已完成（同步完成或已经完成）
        ChannelPromise promise = channel.newPromise();
        doBind0(regFuture, channel, localAddress, promise);
        return promise;
    } else {
        // 情况 B：注册尚未完成，添加监听器等待完成后再 bind
        final PendingRegistrationPromise promise = new PendingRegistrationPromise(channel);
        regFuture.addListener(future -> {
            Throwable cause = future.cause();
            if (cause != null) {
                promise.setFailure(cause);
            } else {
                promise.registered();
                doBind0(regFuture, channel, localAddress, promise);
            }
        });
        return promise;
    }
}
```

这里有一个重要的设计考量：`regFuture` 可能在 `initAndRegister()` 返回时已经完成（如果 `register()` 是同步执行的），也可能尚未完成（如果 `register()` 被提交为异步 task）。两种情况都被正确处理。

### 4.4 initAndRegister() —— 核心骨架方法

**【执行线程：用户线程】**

```java
final ChannelFuture initAndRegister() {
    Channel channel = null;
    try {
        // 步骤 1：通过工厂反射创建 Channel 实例
        channel = channelFactory.newChannel();
        // 步骤 2：初始化 Channel（模板方法，子类实现）
        init(channel);
    } catch (Throwable t) {
        if (channel != null) {
            channel.unsafe().closeForcibly();
            return new DefaultChannelPromise(channel, GlobalEventExecutor.INSTANCE).setFailure(t);
        }
        return new DefaultChannelPromise(new FailedChannel(), GlobalEventExecutor.INSTANCE).setFailure(t);
    }

    // 步骤 3：将 Channel 注册到 EventLoopGroup
    final ChannelFuture regFuture = config().group().register(channel);
    if (regFuture.cause() != null) {
        if (channel.isRegistered()) {
            channel.close();
        } else {
            channel.unsafe().closeForcibly();
        }
    }
    return regFuture;
}

// 子类必须实现的模板方法
abstract void init(Channel channel) throws Throwable;
```

关键要点：

1. `channelFactory.newChannel()` 在**用户线程**中执行，通过反射创建 Channel
2. `init(channel)` 在**用户线程**中执行，配置 Pipeline 和选项
3. `group().register(channel)` 将注册操作提交到 **EventLoop 线程**执行

### 4.5 ServerBootstrap.init(Channel) —— 初始化 ServerChannel

**【执行线程：用户线程】**

```java
@Override
void init(Channel channel) throws Throwable {
    // 1. 设置 ServerChannel 的 Options 和 Attributes
    setChannelOptions(channel, newOptionsArray(), logger);
    setAttributes(channel, newAttributesArray());

    ChannelPipeline p = channel.pipeline();

    // 2. 捕获 child 相关配置（闭包变量）
    final EventLoopGroup currentChildGroup = childGroup;
    final ChannelHandler currentChildHandler = childHandler;
    final Entry<ChannelOption<?>, Object>[] currentChildOptions = newOptionsArray(childOptions);
    final Entry<AttributeKey<?>, Object>[] currentChildAttrs = newAttributesArray(childAttrs);
    final Collection<ChannelInitializerExtension> extensions = getInitializerExtensions();

    // 3. 向 ServerChannel 的 Pipeline 添加 ChannelInitializer
    p.addLast(new ChannelInitializer<Channel>() {
        @Override
        public void initChannel(final Channel ch) {
            final ChannelPipeline pipeline = ch.pipeline();

            // 添加用户通过 ServerBootstrap.handler() 设置的 handler
            ChannelHandler handler = config.handler();
            if (handler != null) {
                pipeline.addLast(handler);
            }

            // 【关键！】通过 eventLoop 异步添加 ServerBootstrapAcceptor
            // 确保在 channelRegistered() 之后添加
            ch.eventLoop().execute(new Runnable() {
                @Override
                public void run() {  // 【EventLoop 线程执行】
                    pipeline.addLast(new ServerBootstrapAcceptor(
                            ch, currentChildGroup, currentChildHandler,
                            currentChildOptions, currentChildAttrs, extensions));
                }
            });
        }
    });

    // 4. 执行扩展点
    if (!extensions.isEmpty() && channel instanceof ServerChannel) {
        for (ChannelInitializerExtension extension : extensions) {
            extension.postInitializeServerListenerChannel((ServerChannel) channel);
        }
    }
}
```

**为什么要通过 `eventLoop.execute()` 异步添加 ServerBootstrapAcceptor？**

因为 `ChannelInitializer.initChannel()` 是在 `register0()` 的 `invokeHandlerAddedIfNeeded()` 中被调用的，此时 Channel 刚刚注册完成，还没有触发 `channelRegistered()` 事件。如果在这里直接添加 ServerBootstrapAcceptor，那么 Acceptor 的 `channelRegistered()` 会在 `initChannel()` 内部被立即触发，时序不对。通过提交到 eventLoop 的任务队列，保证 Acceptor 是在当前所有注册回调（`channelRegistered` → `channelActive`）执行完毕后才被添加到 Pipeline 中。

### 4.6 doBind0() —— 实际绑定端口

**【切换到 EventLoop 线程执行】**

```java
private static void doBind0(
        final ChannelFuture regFuture, final Channel channel,
        final SocketAddress localAddress, final ChannelPromise promise) {

    // 提交到 EventLoop 线程执行
    channel.eventLoop().execute(new Runnable() {
        @Override
        public void run() {  // 【EventLoop 线程】
            if (regFuture.isSuccess()) {
                channel.bind(localAddress, promise)
                       .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            } else {
                promise.setFailure(regFuture.cause());
            }
        }
    });
}
```

`channel.bind(localAddress, promise)` 触发出站事件，沿 Pipeline 从 tail 到 head 传播，最终到达 `HeadContext`，由 `HeadContext` 调用 `unsafe.bind()`，最终执行 JDK 的 `ServerSocketChannel.bind(localAddress, backlog)`。

### 4.7 bind() 完整时序图

```
用户线程                                    EventLoop 线程
─────────                                  ──────────────
bind(port)
  └→ validate()
  └→ doBind(localAddress)
       ├→ initAndRegister()
       │    ├→ newChannel()        创建 NioServerSocketChannel
       │    ├→ init(channel)       添加 ChannelInitializer
       │    └→ group.register()  ──────────→  register0()
       │                                       ├→ doRegister()           注册到 Selector
       │                                       ├→ invokeHandlerAdded()   触发 initChannel()
       │                                       │    ├→ 添加用户 handler
       │                                       │    └→ execute(添加 Acceptor)  入队
       │                                       ├→ fireChannelRegistered()
       │                                       └→ fireChannelActive()
       │                                       └→ [任务队列] 添加 ServerBootstrapAcceptor
       │
       └→ doBind0()               ──────────→  channel.bind(localAddress)
                                                 └→ Pipeline 传播到 HeadContext
                                                      └→ unsafe.bind()
                                                           └→ javaChannel().bind()  JDK 绑定端口
                                                      └→ fireChannelActive()  开始监听 OP_ACCEPT
```

---

## 五、ServerBootstrapAcceptor —— 新连接接入处理

ServerBootstrapAcceptor 是 Netty Reactor 模型中**主从 Reactor 的关键桥梁**——它在 bossGroup 的 EventLoop 中处理 Accept 事件，将新连接转交给 workerGroup。

**【执行线程：bossGroup 的 EventLoop 线程】**

```java
private static class ServerBootstrapAcceptor extends ChannelInboundHandlerAdapter {

    private final EventLoopGroup childGroup;       // workerGroup
    private final ChannelHandler childHandler;     // 用户配置的 childHandler
    private final Entry<ChannelOption<?>, Object>[] childOptions;
    private final Entry<AttributeKey<?>, Object>[] childAttrs;
    private final Runnable enableAutoReadTask;
    private final Collection<ChannelInitializerExtension> extensions;

    ServerBootstrapAcceptor(final Channel channel, EventLoopGroup childGroup,
            ChannelHandler childHandler, ...) {
        this.childGroup = childGroup;
        this.childHandler = childHandler;
        this.childOptions = childOptions;
        this.childAttrs = childAttrs;
        this.extensions = extensions;

        // 预创建 enableAutoReadTask（避免 lambda 导致的类加载器问题）
        enableAutoReadTask = () -> channel.config().setAutoRead(true);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // msg 就是新接入的子 Channel（NioSocketChannel）
        final Channel child = (Channel) msg;

        // 步骤 1：向子 Channel 的 Pipeline 添加 childHandler
        child.pipeline().addLast(childHandler);

        // 步骤 2：设置子 Channel 的 Options
        try {
            setChannelOptions(child, childOptions, logger);
        } catch (Throwable cause) {
            forceClose(child, cause);
            return;
        }

        // 步骤 3：设置子 Channel 的 Attributes
        setAttributes(child, childAttrs);

        // 步骤 4：执行扩展点
        if (!extensions.isEmpty()) {
            for (ChannelInitializerExtension extension : extensions) {
                extension.postInitializeServerChildChannel(child);
            }
        }

        // 步骤 5：【关键！】将子 Channel 注册到 workerGroup
        try {
            childGroup.register(child).addListener(future -> {
                if (!future.isSuccess()) {
                    forceClose(child, future.cause());
                }
            });
        } catch (Throwable t) {
            forceClose(child, t);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        final ChannelConfig config = ctx.channel().config();
        if (config.isAutoRead()) {
            // 异常时暂停接收新连接 1 秒，防止 accept 风暴
            config.setAutoRead(false);
            ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
        }
        ctx.fireExceptionCaught(cause);
    }
}
```

### 新连接接入的完整时序

```
bossGroup EventLoop 线程                          workerGroup EventLoop 线程
──────────────────────                           ───────────────────────────
Selector 检测到 OP_ACCEPT 事件
  └→ ServerSocketChannel.accept()
       获得 SocketChannel（JDK 原生）
  └→ 包装为 NioSocketChannel
  └→ Pipeline.fireChannelRead(child)
       └→ ServerBootstrapAcceptor.channelRead()
            ├→ child.pipeline().addLast(childHandler)
            ├→ setChannelOptions(child, childOptions)
            ├→ setAttributes(child, childAttrs)
            └→ childGroup.register(child)   ─────────→  注册到 workerGroup 的某个 EventLoop
                                                          ├→ 绑定到 Selector
                                                          ├→ 触发 channelRegistered()
                                                          ├→ 触发 channelActive()
                                                          └→ 开始监听 OP_READ 事件
```

### accept 风暴防护

`exceptionCaught` 中的防护逻辑值得注意：当 accept 过程中发生异常时（例如文件描述符耗尽），如果不暂停 accept，Selector 会持续返回 OP_ACCEPT 就绪事件，导致无限循环的异常（accept 风暴）。通过暂时关闭 `autoRead` 并延迟 1 秒恢复，给系统一个喘息的机会来释放资源。

---

## 六、Client 端 —— Bootstrap.connect(host, port) 全流程

### 6.1 完整调用链总览

```
Bootstrap.connect("example.com", 8080)                               【用户线程】
  └→ doResolveAndConnect(unresolved addr)
       └→ initAndRegister()                                           【用户线程】
       │    ├→ channelFactory.newChannel()                            创建 NioSocketChannel
       │    ├→ init(channel)                                          添加 handler + options + attrs
       │    └→ config().group().register(channel)     ─────────→      【EventLoop 线程】注册到 Selector
       │
       └→ doResolveAndConnect0(channel, remoteAddress, localAddress)
            ├→ resolver.resolve(addr)                                  DNS 解析
            └→ doConnect(resolvedAddr, localAddress, promise)
                 └→ channel.eventLoop().execute(...)  ─────────→      【EventLoop 线程】
                      └→ channel.connect(remoteAddress, promise)
                           └→ Pipeline 传播到 HeadContext
                                └→ unsafe.connect()
                                     └→ doConnect()
                                          └→ javaChannel().connect()   JDK 连接
                                          └→ 如果未立即连接成功，注册 OP_CONNECT
```

### 6.2 入口：connect()

**【执行线程：用户线程】**

```java
public ChannelFuture connect(String inetHost, int inetPort) {
    return connect(InetSocketAddress.createUnresolved(inetHost, inetPort));
    // 注意：createUnresolved 不进行 DNS 解析，仅封装 host 字符串
}

public ChannelFuture connect(SocketAddress remoteAddress) {
    validate();
    return doResolveAndConnect(remoteAddress, config.localAddress());
}
```

### 6.3 doResolveAndConnect() —— 编排 initAndRegister + DNS 解析 + connect

```java
private ChannelFuture doResolveAndConnect(final SocketAddress remoteAddress,
                                          final SocketAddress localAddress) {
    final ChannelFuture regFuture = initAndRegister();
    final Channel channel = regFuture.channel();

    if (regFuture.isDone()) {
        if (!regFuture.isSuccess()) {
            return regFuture;
        }
        return doResolveAndConnect0(channel, remoteAddress, localAddress, channel.newPromise());
    } else {
        final PendingRegistrationPromise promise = new PendingRegistrationPromise(channel);
        regFuture.addListener(future -> {  // 【监听器在 EventLoop 线程执行】
            Throwable cause = future.cause();
            if (cause != null) {
                promise.setFailure(cause);
            } else {
                promise.registered();
                doResolveAndConnect0(channel, remoteAddress, localAddress, promise);
            }
        });
        return promise;
    }
}
```

### 6.4 doResolveAndConnect0() —— DNS 解析

```java
private ChannelFuture doResolveAndConnect0(final Channel channel, SocketAddress remoteAddress,
                                           final SocketAddress localAddress, final ChannelPromise promise) {
    try {
        if (disableResolver) {
            doConnect(remoteAddress, localAddress, promise);
            return promise;
        }

        final EventLoop eventLoop = channel.eventLoop();
        AddressResolver<SocketAddress> resolver;
        try {
            resolver = ExternalAddressResolver.getOrDefault(externalResolver).getResolver(eventLoop);
        } catch (Throwable cause) {
            channel.close();
            return promise.setFailure(cause);
        }

        if (!resolver.isSupported(remoteAddress) || resolver.isResolved(remoteAddress)) {
            // 地址已解析或不需要解析
            doConnect(remoteAddress, localAddress, promise);
            return promise;
        }

        // 执行异步 DNS 解析
        final Future<SocketAddress> resolveFuture = resolver.resolve(remoteAddress);

        if (resolveFuture.isDone()) {
            final Throwable resolveFailureCause = resolveFuture.cause();
            if (resolveFailureCause != null) {
                channel.close();
                promise.setFailure(resolveFailureCause);
            } else {
                doConnect(resolveFuture.getNow(), localAddress, promise);
            }
            return promise;
        }

        // DNS 解析尚未完成，等待异步回调
        resolveFuture.addListener((FutureListener<SocketAddress>) future -> {
            if (future.cause() != null) {
                channel.close();
                promise.setFailure(future.cause());
            } else {
                doConnect(future.getNow(), localAddress, promise);
            }
        });
    } catch (Throwable cause) {
        promise.tryFailure(cause);
    }
    return promise;
}
```

### 6.5 DNS 解析器体系

```
AddressResolverGroup<T>             ← 按 EventExecutor 缓存 Resolver 实例
└── DefaultAddressResolverGroup     ← 默认实现（单例 INSTANCE）
       └── 创建 DefaultNameResolver

DefaultNameResolver extends InetNameResolver
       └── doResolve(): InetAddress.getByName(host)  // JDK 阻塞 DNS
```

`AddressResolverGroup.getResolver()` 使用 `IdentityHashMap` 按 EventExecutor 对象引用缓存 Resolver 实例，保证每个 EventLoop 共享同一个 Resolver：

```java
public AddressResolver<T> getResolver(final EventExecutor executor) {
    AddressResolver<T> r;
    synchronized (resolvers) {
        r = resolvers.get(executor);
        if (r == null) {
            final AddressResolver<T> newResolver = newResolver(executor);
            resolvers.put(executor, newResolver);
            executor.terminationFuture().addListener(future -> {
                resolvers.remove(executor);
                newResolver.close();
            });
            r = newResolver;
        }
    }
    return r;
}
```

注意：默认的 `DefaultNameResolver` 使用 JDK 阻塞式 DNS 解析（`InetAddress.getByName()`）。在生产环境中通常会替换为 Netty 的 `DnsNameResolverGroup`——基于 Netty 自身的异步 DNS 客户端，使用 UDP 协议异步查询，不会阻塞 EventLoop 线程。

### 6.6 doConnect() —— 实际发起 TCP 连接

**【切换到 EventLoop 线程执行】**

```java
private static void doConnect(final SocketAddress remoteAddress,
                              final SocketAddress localAddress,
                              final ChannelPromise connectPromise) {
    final Channel channel = connectPromise.channel();
    channel.eventLoop().execute(new Runnable() {
        @Override
        public void run() {  // 【EventLoop 线程】
            if (localAddress == null) {
                channel.connect(remoteAddress, connectPromise);
            } else {
                channel.connect(remoteAddress, localAddress, connectPromise);
            }
            connectPromise.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        }
    });
}
```

### 6.7 Bootstrap.init(Channel) —— 客户端 Channel 初始化

**【执行线程：用户线程】**

```java
@Override
void init(Channel channel) throws Throwable {
    ChannelPipeline p = channel.pipeline();
    p.addLast(config.handler());
    setChannelOptions(channel, newOptionsArray(), logger);
    setAttributes(channel, newAttributesArray());
    for (ChannelInitializerExtension extension : getInitializerExtensions()) {
        extension.postInitializeClientChannel(channel);
    }
}
```

对比 `ServerBootstrap.init()`，客户端的 init 简洁很多——无需添加 Acceptor，直接设置 handler、options、attributes。

### 6.8 connect() 完整时序图

```
用户线程                                    EventLoop 线程
─────────                                  ──────────────
connect("example.com", 8080)
  └→ validate()
  └→ doResolveAndConnect(unresolved addr)
       ├→ initAndRegister()
       │    ├→ newChannel()        创建 NioSocketChannel
       │    ├→ init(channel)       添加 handler + options + attrs
       │    └→ group.register()  ──────→  register0()
       │                                    ├→ doRegister()        注册到 Selector
       │                                    ├→ fireChannelRegistered()
       │                                    └→ （不触发 channelActive，等连接成功后触发）
       │
       └→ doResolveAndConnect0()
            ├→ resolver.resolve(addr) ────→  DNS 解析（默认 JDK 阻塞）
            └→ doConnect(resolvedAddr)
                 └→ eventLoop.execute()  ──→  channel.connect(remoteAddress)
                                               └→ Pipeline 传播到 HeadContext
                                                    └→ unsafe.connect()
                                                         └→ doConnect()
                                                              ├→ javaChannel().connect()
                                                              ├→ 如果非阻塞未立即完成
                                                              │    └→ 注册 OP_CONNECT
                                                              │    └→ 设置连接超时定时任务
                                                              └→ Selector 检测到 OP_CONNECT
                                                                   └→ finishConnect()
                                                                        └→ fireChannelActive()
                                                                             └→ 开始监听 OP_READ
```

---

## 七、ChannelFuture / ChannelPromise / DefaultChannelPromise

### 7.1 接口层次

```
Future<V>                        ← Netty 通用异步结果
├── ChannelFuture                ← 只读的 Channel I/O 操作结果
│     └── channel()             ← 关联的 Channel
│
Promise<V> extends Future<V>     ← 可写的 Future
└── ChannelPromise extends ChannelFuture, Promise<Void>
      └── setSuccess() / setFailure()  ← 设置结果
```

### 7.2 ChannelFuture 状态机

```
                                      +---------------------------+
                                      | Completed successfully    |
                                      +---------------------------+
                                 +---->      isDone() = true      |
 +--------------------------+    |    |   isSuccess() = true      |
 |        Uncompleted       |    |    +===========================+
 +--------------------------+    |    | Completed with failure    |
 |      isDone() = false    |    |    +---------------------------+
 |   isSuccess() = false    |----+---->      isDone() = true      |
 | isCancelled() = false    |    |    |       cause() = non-null  |
 |       cause() = null     |    |    +===========================+
 +--------------------------+    |    | Completed by cancellation |
                                 |    +---------------------------+
                                 +---->      isDone() = true      |
                                      | isCancelled() = true      |
                                      +---------------------------+
```

### 7.3 PendingRegistrationPromise —— 注册未完成时的特殊 Promise

```java
static final class PendingRegistrationPromise extends DefaultChannelPromise {
    private volatile boolean registered;

    PendingRegistrationPromise(Channel channel) {
        super(channel);
    }

    void registered() {
        registered = true;
    }

    @Override
    protected EventExecutor executor() {
        if (registered) {
            return super.executor();  // 注册成功后使用 Channel 的 EventLoop
        }
        // 注册失败时使用 GlobalEventExecutor 通知监听器
        return GlobalEventExecutor.INSTANCE;
    }
}
```

**设计意图**：Channel 注册到 EventLoop 之前，没有绑定的线程来执行监听器通知。此时使用 `GlobalEventExecutor`（一个全局的单线程执行器）作为兜底，保证即使注册失败，promise 的监听器也能被正确执行。

---

## 八、线程模型总结

| 操作步骤 | 执行线程 | 说明 |
|---------|---------|------|
| `bootstrap.bind(port)` / `connect()` | 用户线程 | 入口调用 |
| `channelFactory.newChannel()` | 用户线程 | 反射创建 Channel |
| `init(channel)` | 用户线程 | 初始化 Pipeline/Options |
| `group().register(channel)` | 提交到 EventLoop | 注册到 Selector |
| `ChannelInitializer.initChannel()` | EventLoop 线程 | 注册完成后回调 |
| 添加 ServerBootstrapAcceptor | EventLoop 线程 | 通过 eventLoop.execute() |
| `doBind0()` → `channel.bind()` | EventLoop 线程 | 通过 eventLoop.execute() |
| `ServerBootstrapAcceptor.channelRead()` | bossGroup EventLoop | 处理新连接 |
| `childGroup.register(child)` | 转到 workerGroup EventLoop | 子 Channel 注册 |
| DNS 解析 | 可能在用户线程/EventLoop 线程 | 取决于 regFuture 状态 |
| `doConnect()` → `channel.connect()` | EventLoop 线程 | 通过 eventLoop.execute() |

**核心设计精髓**：`channel.eventLoop().execute()` 是 Netty 线程模型的核心。它将所有对 Channel 的 I/O 操作都串行化到同一个 EventLoop 线程，避免了同步锁的开销，同时保证了 Pipeline 中 handler 的执行顺序。

---

## 九、设计哲学：Builder 模式 + 模板方法模式的组合

### 为什么要把"配置"和"执行"分成两个阶段？

**阶段一：配置阶段**（用户线程，无并发问题）

```java
ServerBootstrap b = new ServerBootstrap();
b.group(bossGroup, workerGroup)
 .channel(NioServerSocketChannel.class)
 .option(ChannelOption.SO_BACKLOG, 1024)
 .childOption(ChannelOption.TCP_NODELAY, true)
 .childHandler(new ChannelInitializer<SocketChannel>() { ... });
```

所有配置方法都是简单的赋值操作，在用户线程中顺序执行，不涉及任何 I/O 操作或线程安全问题。Builder 模式的链式调用让配置过程流畅自然。

**阶段二：执行阶段**（跨线程，精心编排）

```java
ChannelFuture f = b.bind(8080).sync();
```

`bind()` 触发一系列精心编排的操作：创建 Channel → 初始化 → 注册到 EventLoop → 绑定端口。这些操作涉及用户线程和 EventLoop 线程的切换，通过 `ChannelFuture` 和 `eventLoop.execute()` 实现线程安全的异步编排。

**两阶段分离的好处**：

1. **配置阶段的简单性**：所有配置在用户线程中完成，不需要考虑线程安全
2. **执行阶段的确定性**：所有 I/O 操作都在 EventLoop 线程中执行，通过线程封闭保证安全
3. **复用性**：同一个 Bootstrap 配置可以用于创建多个 Channel（例如重连场景）
4. **可测试性**：配置和执行分离使得单元测试更容易

---

## 十、与 Dubbo 的关联

### Dubbo NettyServer.doOpen() 对应 bind() 的哪一步

Dubbo 的 `NettyServer.doOpen()` 方法中：

```java
// Dubbo 的 NettyServer.doOpen()
bootstrap.group(bossGroup, workerGroup)
    .channel(NioServerSocketChannel.class)
    .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
            ch.pipeline()
                .addLast("decoder", adapter.getDecoder())  // DubboCodec 解码器
                .addLast("encoder", adapter.getEncoder())  // DubboCodec 编码器
                .addLast("server-idle-handler",
                    new IdleStateHandler(0, 0, idleTimeout, MILLISECONDS))  // 心跳检测
                .addLast("handler", nettyServerHandler);   // 业务处理器
        }
    });
ChannelFuture channelFuture = bootstrap.bind(getBindAddress());
```

- `bootstrap.bind()` 直接调用了本文分析的 `ServerBootstrap.bind()` 全流程
- Pipeline 配置中的 `DubboCodec` 对应提示词 06（编解码器框架）
- `IdleStateHandler` 对应提示词 07（内置 Handler）

### Dubbo 为什么在 Pipeline 中加入 IdleStateHandler

Dubbo 使用 `IdleStateHandler` 进行心跳检测，而不是使用 TCP 的 `SO_KEEPALIVE`。原因是 `SO_KEEPALIVE` 的默认探测间隔是 2 小时（由操作系统控制），对于 RPC 框架来说太慢了——Dubbo 需要在秒级别发现连接异常。`IdleStateHandler` 通过应用层定时任务（EventLoop.schedule）实现更灵活、更快速的心跳检测。

---

## 十一、本篇涉及的设计模式

1. **Builder 模式（建造者模式）**：`AbstractBootstrap` 的所有配置方法（`group()`、`channel()`、`option()`、`handler()`）返回 `self()`，支持链式调用。将复杂对象（网络服务）的构建过程分解为独立的配置步骤，使得创建过程灵活可控。

2. **模板方法模式（Template Method）**：`AbstractBootstrap.initAndRegister()` 定义了"创建 → 初始化 → 注册"的骨架算法，`init(Channel)` 作为抽象方法由 `ServerBootstrap`（添加 Acceptor）和 `Bootstrap`（添加 handler）各自实现。保证了流程的统一性，同时允许子类定制关键步骤。

3. **工厂模式（Factory）**：`ReflectiveChannelFactory` 通过反射创建 Channel 实例，将 Channel 的具体类型（NIO/Epoll/KQueue）与创建逻辑解耦。切换传输实现只需要替换传入的 Class 参数。

4. **观察者模式（Observer）**：`ChannelFuture.addListener()` 注册异步操作的完成回调。`doBind()` 和 `doResolveAndConnect()` 中通过 listener 实现异步编排——注册完成后触发 bind，DNS 解析完成后触发 connect。

5. **Promise 模式**：`ChannelPromise` 是可写的 `ChannelFuture`，作为异步操作的结果容器。操作发起方创建 Promise 并传递给执行方，执行方在操作完成时设置结果（`setSuccess()`/`setFailure()`），监听方通过 `addListener()` 消费结果。

6. **Reactor 模式（主从多线程）**：`ServerBootstrapAcceptor` 实现了主从 Reactor 的关键转换——bossGroup（主 Reactor）接收新连接，通过 `childGroup.register(child)` 将子 Channel 交给 workerGroup（从 Reactor）处理 I/O 读写。

---

## 十二、本篇涉及的高性能并发技术

1. **线程封闭（Thread Confinement）**：所有 Channel I/O 操作都通过 `channel.eventLoop().execute()` 提交到绑定的 EventLoop 线程执行。`doBind0()` 和 `doConnect()` 都将实际操作封装为 Runnable 提交到 EventLoop，保证 Channel 的所有操作在同一个线程中串行执行，完全避免了锁竞争。

2. **异步非阻塞编排**：`ChannelFuture` + `addListener()` 实现了无阻塞的异步操作编排。`doBind()` 中的 `regFuture.isDone()` 检查和 `regFuture.addListener()` 回调保证了"注册完成后再绑定"的时序约束，同时不阻塞用户线程。

3. **延迟初始化与异步任务调度**：`ServerBootstrap.init()` 中通过 `ch.eventLoop().execute()` 延迟添加 `ServerBootstrapAcceptor`，保证了"在所有注册回调完成后再添加 Acceptor"的时序正确性。这种通过任务队列控制执行顺序的技巧在 Netty 中被大量使用。

4. **PendingRegistrationPromise 的 Executor 切换**：巧妙地解决了"Channel 未注册时没有绑定 EventLoop"的问题。未注册时使用 `GlobalEventExecutor` 通知监听器，注册成功后切换到 Channel 的 EventLoop。这避免了空指针异常，同时保证了监听器始终有执行线程。

5. **DNS 解析器缓存**：`AddressResolverGroup` 通过 `IdentityHashMap` 按 EventExecutor 实例缓存 Resolver，避免了重复创建 Resolver 的开销。同时注册了 EventExecutor 的终止监听器，在 EventExecutor 关闭时自动清理缓存，防止内存泄漏。

6. **Accept 风暴防护**：`ServerBootstrapAcceptor.exceptionCaught()` 在 accept 异常时暂时关闭 `autoRead` 并延迟 1 秒恢复，防止 Selector 持续返回 OP_ACCEPT 就绪事件导致的无限循环异常。这是一种典型的退避策略（backoff），在高压力下保护系统稳定性。