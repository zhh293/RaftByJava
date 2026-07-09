# Channel 体系与生命周期全流程源码解析

> **Netty 源码深度研究系列 · 第 02 篇**
>
> 基于 Netty 主分支源码，从 Channel 接口的设计出发，完整梳理 Channel 的类型体系、创建过程和生命周期状态机。

---

## 一、Channel 类继承体系图

```
Channel (接口)
│  定义所有 I/O 操作的契约：read / write / connect / bind / close
│  继承 AttributeMap + ChannelOutboundInvoker + Comparable<Channel>
│
└── AbstractChannel (抽象类)
    │  骨架实现：持有 parent / ChannelId / Unsafe / Pipeline / EventLoop
    │  构造时创建 Pipeline（newChannelPipeline()）
    │  实现 register / bind / close / deregister 的状态机
    │
    └── AbstractNioChannel (抽象类)
        │  引入 JDK SelectableChannel，设置非阻塞模式
        │  实现 doRegister()（向 IoEventLoop 注册）
        │  实现 doBeginRead()（添加 interest ops）
        │  定义 NioUnsafe 子接口（含 handle() 事件分发）
        │
        ├── AbstractNioByteChannel (抽象类)
        │   │  面向字节流的读写
        │   │  NioByteUnsafe.read()：循环读字节 → fireChannelRead
        │   │  doWrite()：spin write，支持 gathering write
        │   │  filterOutboundMessage()：确保写出的是 direct buffer
        │   │
        │   └── NioSocketChannel (具体类)
        │       绑定 JDK SocketChannel
        │       doReadBytes() / doWriteBytes() / doWrite()
        │       isActive() = isOpen() && isConnected()
        │
        └── AbstractNioMessageChannel (抽象类)
            │  面向消息的读写
            │  NioMessageUnsafe.read()：循环调 doReadMessages()
            │  批量 fireChannelRead()
            │
            └── NioServerSocketChannel (具体类)
                绑定 JDK ServerSocketChannel
                doReadMessages()：accept 新连接 → 包装为 NioSocketChannel
                isActive() = isOpen() && isBound()
```

**每一层增加的能力**：

| 层级 | 新增核心能力 |
|------|------------|
| Channel 接口 | 定义 IO 操作契约、Unsafe 内部接口、Pipeline 访问 |
| AbstractChannel | 持有 parent/id/unsafe/pipeline；实现生命周期状态机（register/bind/close/deregister）|
| AbstractNioChannel | 引入 JDK SelectableChannel + 非阻塞模式；向 Selector 注册；connect 超时机制 |
| AbstractNioByteChannel | 字节流读写循环；spin write；heap→direct 转换 |
| NioSocketChannel | 绑定 JDK SocketChannel；实现 doReadBytes/doWrite；gathering write 优化 |
| AbstractNioMessageChannel | 消息级读写循环（accept 循环）；批量事件传播 |
| NioServerSocketChannel | 绑定 JDK ServerSocketChannel；accept 新连接并包装为 NioSocketChannel |

---

## 二、Channel 的创建过程

### 2.1 ServerBootstrap.bind() 触发 NioServerSocketChannel 创建

整个创建过程从 `ServerBootstrap.bind(port)` 开始：

```java
// AbstractBootstrap.java
private ChannelFuture doBind(final SocketAddress localAddress) {
    final ChannelFuture regFuture = initAndRegister();  // Step 1: 创建 + 初始化 + 注册
    // ...
    doBind0(regFuture, channel, localAddress, promise);  // Step 2: 绑定端口
    return promise;
}

final ChannelFuture initAndRegister() {
    Channel channel = null;
    try {
        channel = channelFactory.newChannel();  // ★ 反射创建 NioServerSocketChannel
    } catch (Throwable t) {
        return new DefaultChannelPromise(
            new FailedChannel(), GlobalEventExecutor.INSTANCE).setFailure(t);
    }

    try {
        init(channel);  // ★ 初始化：配置 options/attrs，添加 ServerBootstrapAcceptor
    } catch (Throwable t) {
        channel.unsafe().closeForcibly();
        return new DefaultChannelPromise(channel, GlobalEventExecutor.INSTANCE).setFailure(t);
    }

    ChannelFuture regFuture = config().group().register(channel);  // ★ 注册到 bossGroup
    // ...
    return regFuture;
}
```

### 2.2 ReflectiveChannelFactory 的作用

当用户调用 `bootstrap.channel(NioServerSocketChannel.class)` 时：

```java
// AbstractBootstrap.java
public B channel(Class<? extends C> channelClass) {
    return channelFactory(new ReflectiveChannelFactory<C>(channelClass));
}
```

```java
// ReflectiveChannelFactory.java
public class ReflectiveChannelFactory<T extends Channel> implements ChannelFactory<T> {
    private final Constructor<? extends T> constructor;

    public ReflectiveChannelFactory(Class<? extends T> clazz) {
        this.constructor = clazz.getConstructor();  // 获取无参构造器
    }

    @Override
    public T newChannel() {
        try {
            return constructor.newInstance();  // ★ 反射调用无参构造器
        } catch (Throwable t) {
            throw new ChannelException("Unable to create Channel from class " + constructor.getDeclaringClass(), t);
        }
    }
}
```

**它是什么**：通过反射调用无参构造器创建 Channel 实例的工厂。

**没有它会怎样**：用户每次创建 Channel 都需要手动 `new NioServerSocketChannel()`，无法通过配置切换 Channel 类型（比如从 NIO 切换到 Epoll 只需要改一个 class 参数）。

**为什么用反射而不是 lambda/supplier**：`ReflectiveChannelFactory` 提供了基于类名的统一创建方式，配合 `channel(Class)` API 简洁直观。用户也可以通过 `channelFactory(ChannelFactory)` 传入自定义工厂实现。

### 2.3 新连接接入时 NioSocketChannel 的创建

当 Boss EventLoop 检测到 `OP_ACCEPT` 事件时：

```java
// NioServerSocketChannel.java
@Override
protected int doReadMessages(List<Object> buf) throws Exception {
    SocketChannel ch = SocketUtils.accept(javaChannel());  // ★ JDK accept()
    try {
        if (ch != null) {
            buf.add(new NioSocketChannel(this, ch));  // ★ 创建子 Channel，parent = this
            return 1;
        }
    } catch (Throwable t) {
        logger.warn("Failed to create a new channel from an accepted socket.", t);
        try {
            ch.close();
        } catch (Throwable t2) {
            logger.warn("Failed to close a socket.", t2);
        }
    }
    return 0;
}
```

子 Channel 创建后，通过 `ServerBootstrapAcceptor` 注册到 Worker EventLoopGroup：

```java
// ServerBootstrap.ServerBootstrapAcceptor
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    final Channel child = (Channel) msg;

    child.pipeline().addLast(childHandler);           // 添加用户配置的 childHandler
    setChannelOptions(child, childOptions, logger);   // 设置 TCP 参数
    setAttributes(child, childAttrs);                 // 设置属性

    // ★ 核心：向 childGroup（worker）注册子 Channel
    childGroup.register(child).addListener((ChannelFutureListener) future -> {
        if (!future.isSuccess()) {
            forceClose(child, future.cause());
        }
    });
}
```

**Boss → Worker 的完整转移过程**：

```
Boss EventLoop 线程：
  NioMessageUnsafe.read()
    → doReadMessages()      → JDK accept()
    → new NioSocketChannel(this, jdkChannel)  → 创建子 Channel（Pipeline 随之创建）
    → pipeline.fireChannelRead(childChannel)
    → ServerBootstrapAcceptor.channelRead()
      → child.pipeline().addLast(childHandler)
      → childGroup.register(child)            → 选择一个 Worker EventLoop
        → child.unsafe().register(workerEventLoop, promise)
                                              ↓ 此后子 Channel 的所有操作都在 Worker 线程
Worker EventLoop 线程：
  register0()
    → doRegister()                            → 向 Worker 的 Selector 注册
    → pipeline.fireChannelRegistered()
    → isActive()? → true (已 connected)
    → pipeline.fireChannelActive()
    → beginRead() → 添加 OP_READ             → 开始接收数据
```

---

## 三、Channel 的核心组件拆解

### 3.1 Unsafe 接口的真面目

```java
// Channel.java 内部接口
/**
 * <em>Unsafe</em> operations that should <em>never</em> be called from user-code.
 * These methods are only provided to implement the actual transport, and must be
 * invoked from an I/O thread except for the following methods:
 * localAddress(), remoteAddress(), closeForcibly(), register(), deregister(), voidPromise()
 */
interface Unsafe {
    RecvByteBufAllocator.Handle recvBufAllocHandle();
    SocketAddress localAddress();
    SocketAddress remoteAddress();
    void register(EventLoop eventLoop, ChannelPromise promise);
    void bind(SocketAddress localAddress, ChannelPromise promise);
    void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise);
    void disconnect(ChannelPromise promise);
    void close(ChannelPromise promise);
    void closeForcibly();
    void deregister(ChannelPromise promise);
    void beginRead();
    void write(Object msg, ChannelPromise promise);
    void flush();
    ChannelPromise voidPromise();
    ChannelOutboundBuffer outboundBuffer();
}
```

**为什么叫 Unsafe**：这里的 Unsafe 和 `sun.misc.Unsafe` 完全没有关系。它叫 Unsafe 是因为这些方法是 Channel 的"内部 API"，对用户代码来说是"不安全的"——如果用户直接调用 `channel.unsafe().write(msg, promise)`，会绕过整个 Pipeline Handler 链，丧失编解码、日志、安全校验等所有中间处理。正确的做法是调用 `channel.write(msg)` 或 `ctx.write(msg)`，让消息经过 Pipeline 传播。

**它封装了什么**：Unsafe 封装了所有真正触达底层 IO 的操作入口。Pipeline 中事件传播的终点（HeadContext）就是调用 `unsafe.xxx()` 方法：

```
用户代码: channel.write(msg)
  → pipeline.write(msg)          // Pipeline 入口（从 tail 开始）
  → ... Handler 链处理 ...
  → HeadContext.write(ctx, msg, promise)
  → unsafe.write(msg, promise)    // ★ Unsafe 是 Pipeline 出站事件的终点
```

**没有 Unsafe 会怎样**：Pipeline 的出站事件需要一个"执行者"来真正执行 IO 操作。如果把这些方法直接暴露在 Channel 接口上，用户可能绕过 Pipeline 直接调用，导致编解码等 Handler 被跳过。Unsafe 通过访问控制（命名警告）和文档约束，引导用户通过 Pipeline 进行操作。

### 3.2 Pipeline 的挂载时机

```java
// AbstractChannel.java
protected AbstractChannel(Channel parent) {
    this.parent = parent;
    id = newId();
    unsafe = newUnsafe();
    pipeline = newChannelPipeline();  // ★ Channel 构造时就创建 Pipeline
}

protected DefaultChannelPipeline newChannelPipeline() {
    return new DefaultChannelPipeline(this);
}
```

Pipeline 在 Channel 构造函数中创建，时机早于 register、bind 或 connect。这意味着在 `channelFactory.newChannel()` 完成后，Channel 就已经持有了完整的 Pipeline（包含 HeadContext 和 TailContext 两个哨兵节点）。

### 3.3 EventLoop 的绑定时机

EventLoop 在 `register` 阶段绑定：

```java
// AbstractChannel.AbstractUnsafe
@Override
public final void register(EventLoop eventLoop, final ChannelPromise promise) {
    ObjectUtil.checkNotNull(eventLoop, "eventLoop");
    if (isRegistered()) {
        promise.setFailure(new IllegalStateException("registered to an event loop already"));
        return;
    }
    if (!isCompatible(eventLoop)) {
        promise.setFailure(new IllegalStateException("incompatible event loop type"));
        return;
    }

    // ★ 核心：将 EventLoop 绑定到 Channel
    AbstractChannel.this.eventLoop = eventLoop;

    // 确保后续操作在 EventLoop 线程中执行
    if (eventLoop.inEventLoop()) {
        register0(promise);
    } else {
        try {
            eventLoop.execute(() -> register0(promise));
        } catch (Throwable t) {
            closeForcibly();
            closeFuture.setClosed();
            safeSetFailure(promise, t);
        }
    }
}
```

**关键点**：`AbstractChannel.this.eventLoop = eventLoop` 是一次性赋值——Channel 一旦绑定了 EventLoop，就永远不会更换。这保证了 Channel 的所有操作都在同一个线程中执行（线程封闭），从而避免了并发安全问题。

---

## 四、Channel 生命周期状态机

```
创建（new）
  │
  ▼
channelRegistered    ← Channel 成功注册到 EventLoop
  │
  ▼
channelActive        ← Channel 变为可用状态（Server: bind 完成; Client: connect 完成）
  │
  ├─── channelRead         ← 每收到一个消息触发一次
  │      │
  │      ▼
  ├─── channelReadComplete ← 一次读循环结束
  │      │
  │      ▼
  │    （可重复多次 channelRead → channelReadComplete）
  │
  ▼
channelInactive      ← Channel 从 active 变为 inactive（close/disconnect）
  │
  ▼
channelUnregistered  ← Channel 从 EventLoop 取消注册
  │
  ▼
销毁
```

### 4.1 channelRegistered 的触发

```java
// AbstractChannel.AbstractUnsafe
private void register0(ChannelPromise promise) {
    try {
        if (!promise.setUncancellable() || !ensureOpen(promise)) {
            return;
        }
        boolean firstRegistration = neverRegistered;

        doRegister(registerPromise);  // 子类实现：向 Selector 注册

        // 注册成功后的回调：
        neverRegistered = false;
        registered = true;

        pipeline.invokeHandlerAddedIfNeeded();  // ★ 确保 handlerAdded 在 channelRegistered 之前

        pipeline.fireChannelRegistered();       // ★ 触发 channelRegistered

        if (isActive()) {
            if (firstRegistration) {
                pipeline.fireChannelActive();   // ★ 如果已经 active，触发 channelActive
            } else if (config().isAutoRead()) {
                beginRead();
            }
        }
    } catch (Throwable t) {
        closeForcibly();
        closeFuture.setClosed();
        safeSetFailure(promise, t);
    }
}
```

### 4.2 channelActive 的触发

channelActive 有三个触发场景：

**场景一**：register0 中 isActive() 已经为 true（子 Channel 被 accept 时，JDK SocketChannel 已经 connected）

**场景二**：bind 成功后，Server Channel 变为 active

```java
// AbstractChannel.AbstractUnsafe
@Override
public final void bind(final SocketAddress localAddress, final ChannelPromise promise) {
    // ...
    boolean wasActive = isActive();
    try {
        doBind(localAddress);  // JDK ServerSocketChannel.bind()
    } catch (Throwable t) {
        safeSetFailure(promise, t);
        closeIfClosed();
        return;
    }

    if (!wasActive && isActive()) {
        // ★ 绑定前不是 active，绑定后变 active → 触发 channelActive
        invokeLater(() -> pipeline.fireChannelActive());
    }
    safeSetSuccess(promise);
}
```

**场景三**：connect 完成后，Client Channel 变为 active（在 `finishConnect()` 中触发）

### 4.3 channelRead 和 channelReadComplete 的触发

**对于 NioSocketChannel（字节流读取）**：

```java
// AbstractNioByteChannel.NioByteUnsafe
@Override
public final void read() {
    final ChannelConfig config = config();
    final ChannelPipeline pipeline = pipeline();
    final ByteBufAllocator allocator = config.getAllocator();
    final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
    allocHandle.reset(config);

    ByteBuf byteBuf = null;
    boolean close = false;
    try {
        do {
            byteBuf = allocHandle.allocate(allocator);         // 分配 ByteBuf
            allocHandle.lastBytesRead(doReadBytes(byteBuf));   // 从 JDK Channel 读数据
            if (allocHandle.lastBytesRead() <= 0) {
                byteBuf.release();
                byteBuf = null;
                close = allocHandle.lastBytesRead() < 0;       // -1 表示 EOF（对端关闭）
                break;
            }
            allocHandle.incMessagesRead(1);
            readPending = false;
            pipeline.fireChannelRead(byteBuf);                 // ★ 触发 channelRead
            byteBuf = null;
        } while (allocHandle.continueReading());               // 自适应决定是否继续读

        allocHandle.readComplete();
        pipeline.fireChannelReadComplete();                    // ★ 触发 channelReadComplete

        if (close) {
            closeOnRead(pipeline);                             // EOF → 关闭连接
        }
    } catch (Throwable t) {
        handleReadException(pipeline, byteBuf, t, close, allocHandle);
    } finally {
        if (!readPending && !config.isAutoRead()) {
            removeReadOp();
        }
    }
}
```

**对于 NioServerSocketChannel（新连接接收）**：

```java
// AbstractNioMessageChannel.NioMessageUnsafe
@Override
public void read() {
    // ...
    try {
        do {
            int localRead = doReadMessages(readBuf);  // accept 新连接
            if (localRead == 0) break;
            allocHandle.incMessagesRead(localRead);
        } while (continueReading(allocHandle));

        int size = readBuf.size();
        for (int i = 0; i < size; i++) {
            readPending = false;
            pipeline.fireChannelRead(readBuf.get(i));  // ★ 每个子 Channel 触发一次 channelRead
        }
        readBuf.clear();
        allocHandle.readComplete();
        pipeline.fireChannelReadComplete();            // ★ 触发 channelReadComplete
    } catch (Throwable t) {
        // ...
    }
}
```

### 4.4 channelInactive 和 channelUnregistered 的触发

```java
// AbstractChannel.AbstractUnsafe
private void deregister(final ChannelPromise promise, final boolean fireChannelInactive) {
    if (!promise.setUncancellable()) {
        return;
    }

    if (!registered) {
        safeSetSuccess(promise);
        return;
    }

    invokeLater(() -> {
        try {
            doDeregister();  // 从 Selector 取消注册
        } catch (Throwable t) {
            logger.warn("Unexpected exception occurred while deregistering a channel.", t);
        } finally {
            if (fireChannelInactive) {
                pipeline.fireChannelInactive();     // ★ 先触发 channelInactive
            }
            if (registered) {
                registered = false;
                pipeline.fireChannelUnregistered();  // ★ 再触发 channelUnregistered
            }
            safeSetSuccess(promise);
        }
    });
}
```

**触发顺序**：channelInactive 一定在 channelUnregistered 之前。这是有意义的——inactive 表示"连接断开了"，unregistered 表示"从 EventLoop 解绑了"。业务层通常关心 inactive（处理断连逻辑），而 unregistered 是更底层的清理。

---

## 五、NioSocketChannel 与 JDK NIO 的交互

### 5.1 doReadBytes() — 读取数据

```java
// NioSocketChannel.java
@Override
protected int doReadBytes(ByteBuf byteBuf) throws Exception {
    final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
    allocHandle.attemptedBytesRead(byteBuf.writableBytes());
    return byteBuf.writeBytes(javaChannel(), allocHandle.attemptedBytesRead());
    // ★ 最终调用 JDK: javaChannel().read(ByteBuffer)
}
```

`byteBuf.writeBytes(ScatteringByteChannel, int)` 的底层实现会获取 ByteBuf 底层的 `java.nio.ByteBuffer`，然后调用 `javaChannel().read(nioBuffer)`。如果是 DirectByteBuf，直接使用堆外内存；如果是 HeapByteBuf，需要一次到 DirectBuffer 的临时拷贝（这也是 Netty 默认使用 Direct 的原因之一）。

### 5.2 doWrite() — 写入数据

```java
// NioSocketChannel.java
@Override
protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    SocketChannel ch = javaChannel();
    int writeSpinCount = config().getWriteSpinCount();  // 默认 16

    do {
        if (in.isEmpty()) {
            clearOpWrite();  // 数据写完，取消 OP_WRITE
            return;
        }

        int maxBytesPerGatheringWrite =
            ((NioSocketChannelConfig) config).getMaxBytesPerGatheringWrite();
        ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
        int nioBufferCnt = in.nioBufferCount();

        switch (nioBufferCnt) {
            case 0:
                // 非 ByteBuf 类型（如 FileRegion）
                writeSpinCount -= doWrite0(in);
                break;

            case 1: {
                // 单个 ByteBuffer，使用普通写入
                ByteBuffer buffer = nioBuffers[0];
                int attemptedBytes = buffer.remaining();
                final int localWrittenBytes = ch.write(buffer);  // ★ JDK SocketChannel.write()

                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);  // Socket 缓冲区满，注册 OP_WRITE
                    return;
                }
                adjustMaxBytesPerGatheringWrite(
                    attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
                break;
            }

            default: {
                // 多个 ByteBuffer，使用 Gathering Write（批量写入）
                long attemptedBytes = in.nioBufferSize();
                final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);
                // ★ JDK SocketChannel.write(ByteBuffer[]) — Gathering Write

                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite(
                    (int) attemptedBytes, (int) localWrittenBytes, maxBytesPerGatheringWrite);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
                break;
            }
        }
    } while (writeSpinCount > 0);

    // spin 用尽但还有数据，延迟到下一轮再写
    incompleteWrite(writeSpinCount < 0);
}
```

**writeSpinCount 的作用**：默认 16 次。防止一个 Channel 的写操作独占 EventLoop 线程——如果 Socket 缓冲区很大且数据很多，可能连续写入很长时间，导致同一 EventLoop 上的其他 Channel 得不到处理。spin 16 次后即使还有数据，也要让出线程去处理其他 Channel 的事件。

**Gathering Write 优化**：当 ChannelOutboundBuffer 中有多个 ByteBuf 待发送时，Netty 将它们转为 `ByteBuffer[]` 调用 JDK 的 `SocketChannel.write(ByteBuffer[])`。这利用了 OS 的 scatter/gather IO，内核可以一次性将多个 buffer 写入 Socket，减少系统调用次数。

---

## 六、Channel 的配置体系

### 6.1 继承关系

```
ChannelConfig (接口)
  └── DefaultChannelConfig (通用配置)
      │  allocator / autoRead / writeSpinCount / WriteBufferWaterMark / connectTimeoutMillis
      │
      └── DefaultSocketChannelConfig (TCP 配置)
          │  TCP_NODELAY / SO_KEEPALIVE / SO_SNDBUF / SO_RCVBUF / SO_LINGER / SO_REUSEADDR
          │
          └── NioSocketChannelConfig (NIO 特定配置，NioSocketChannel 内部类)
              maxBytesPerGatheringWrite / 支持 NioChannelOption
```

### 6.2 TCP 参数如何生效

`DefaultSocketChannelConfig` 持有 JDK `java.net.Socket` 引用，所有 TCP 参数的 get/set 直接代理到 JDK：

```java
// DefaultSocketChannelConfig.java
public class DefaultSocketChannelConfig extends DefaultChannelConfig
    implements SocketChannelConfig {

    protected final Socket javaSocket;

    public DefaultSocketChannelConfig(SocketChannel channel, Socket javaSocket) {
        super(channel);
        this.javaSocket = requireNonNull(javaSocket, "javaSocket");

        // ★ Netty 默认开启 TCP_NODELAY（禁用 Nagle 算法）
        if (PlatformDependent.canEnableTcpNoDelayByDefault()) {
            try {
                setTcpNoDelay(true);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public SocketChannelConfig setTcpNoDelay(boolean tcpNoDelay) {
        try {
            javaSocket.setTcpNoDelay(tcpNoDelay);  // ★ 直接调用 JDK API
        } catch (SocketException e) {
            throw new ChannelException(e);
        }
        return this;
    }

    @Override
    public SocketChannelConfig setSendBufferSize(int sendBufferSize) {
        try {
            javaSocket.setSendBufferSize(sendBufferSize);  // ★ JDK API
        } catch (SocketException e) {
            throw new ChannelException(e);
        }
        return this;
    }

    @Override
    public SocketChannelConfig setKeepAlive(boolean keepAlive) {
        try {
            javaSocket.setKeepAlive(keepAlive);  // ★ JDK API
        } catch (SocketException e) {
            throw new ChannelException(e);
        }
        return this;
    }
    // ... SO_LINGER, SO_REUSEADDR, SO_RCVBUF, IP_TOS 同理
}
```

**Netty 默认开启 TCP_NODELAY 的原因**：JDK 默认关闭 TCP_NODELAY（即启用 Nagle 算法），Nagle 算法会将小数据包缓冲合并后发送，减少网络小包数量。但对于 RPC/实时通信场景，延迟比带宽利用率更重要，Nagle 算法引入的延迟（最多 40ms）是不可接受的。因此 Netty 默认禁用 Nagle。

### 6.3 通过 ChannelOption 设置参数

用户在 Bootstrap 中通过 `option()` 和 `childOption()` 设置参数：

```java
ServerBootstrap b = new ServerBootstrap();
b.option(ChannelOption.SO_BACKLOG, 128)          // Server Channel 的参数
 .childOption(ChannelOption.TCP_NODELAY, true)    // 子 Channel 的参数
 .childOption(ChannelOption.SO_KEEPALIVE, true);
```

这些参数在 Channel 初始化时（`init(channel)` 和 `ServerBootstrapAcceptor.channelRead()`）通过 `setChannelOptions()` 批量设置到 ChannelConfig 中。

---

## 七、设计哲学总结

### Channel 作为"通信管道"抽象

Netty 的 Channel 是对"网络连接"的高层抽象，它与 JDK 原生 `java.nio.channels.Channel` 的本质区别在于：

| 对比维度 | JDK Channel | Netty Channel |
|---------|------------|---------------|
| 操作模型 | 同步阻塞/非阻塞，需自己管理 Selector | 完全异步，所有操作返回 ChannelFuture |
| 线程安全 | 需要用户自己保证 | 通过 EventLoop 绑定实现线程封闭 |
| 生命周期 | 没有事件通知 | 完整的状态机 + 事件回调 |
| 可扩展性 | 需要自己实现拦截逻辑 | 通过 Pipeline Handler 链灵活扩展 |
| 内存管理 | 使用 JDK ByteBuffer | 使用池化 ByteBuf，引用计数 |
| 配置管理 | 散落在各处的 Socket API | 统一的 ChannelConfig 体系 |

Netty Channel 的设计将"网络编程"从"操作系统 API 的调用"提升到了"事件驱动的管道处理"。用户不再关心 Selector 注册、NIO 缓冲区管理、线程同步等底层细节，只需要在 Pipeline 中插入自己的 Handler 来处理业务逻辑。

---

## 八、本篇涉及的设计模式

**模板方法模式（Template Method）**：`AbstractChannel` 定义了 `register0()`、`bind()`、`close()` 等操作的流程框架（先检查状态 → 调用 doXxx() → 触发事件），具体的 `doRegister()`、`doBind()`、`doClose()` 由子类 `AbstractNioChannel`、`NioServerSocketChannel` 等实现。这是整个 Channel 体系最核心的设计模式。

**工厂模式（Factory）**：`ReflectiveChannelFactory` 通过反射创建 Channel 实例，用户只需指定 Class 即可。`ChannelFactory` 接口解耦了 Channel 类型的选择和创建过程，使得从 NIO 切换到 Epoll 只需改一个配置参数。

**责任链模式（Chain of Responsibility）**：Channel 的所有出站操作（write/flush/bind/connect/close）都通过 Pipeline 传播，经过一系列 Handler 处理后最终到达 HeadContext → Unsafe。Pipeline 就是一条责任链。

**观察者模式（Observer）**：Channel 的生命周期事件（channelRegistered/Active/Read/Inactive/Unregistered）通过 Pipeline 的 `fireXxx()` 方法传播给所有注册的 Handler。每个 Handler 就是一个观察者。

**外观模式（Facade）**：Channel 接口对外提供统一的 IO 操作门面（read/write/bind/connect/close），内部将操作委托给 Pipeline → HeadContext → Unsafe → JDK Channel。用户不需要感知内部的多层委托关系。

**适配器模式（Adapter）**：Netty Channel 适配了 JDK NIO Channel，将非阻塞 IO 的复杂操作（Selector 注册、interest ops 管理、非阻塞 read/write）封装为事件驱动的简洁接口。

---

## 九、本篇涉及的高性能并发技术

**线程封闭（Thread Confinement）**：每个 Channel 在 register 阶段绑定唯一的 EventLoop，此后所有 IO 操作和事件传播都在该 EventLoop 线程中执行。这从根本上消除了对 Channel 状态的并发竞争，Unsafe 的各种操作方法（write/flush/read/close）都不需要加锁。解决的瓶颈：多线程并发操作同一个连接的同步开销。

**inEventLoop() 线程判断**：`register()` 等方法通过 `eventLoop.inEventLoop()` 判断当前线程是否是 EventLoop 线程。如果不是，则将操作封装为 task 提交到 EventLoop 的 taskQueue，由 EventLoop 线程串行执行。这保证了 Channel 操作的线程安全，同时允许用户从任意线程安全地调用 Channel 方法。解决的瓶颈：用户线程与 IO 线程之间的安全交互。

**Gathering Write（批量写入）**：`NioSocketChannel.doWrite()` 将多个待发送的 ByteBuf 转为 `ByteBuffer[]`，调用 JDK 的 `SocketChannel.write(ByteBuffer[])` 一次性写入。这利用了 OS 的 scatter/gather IO 机制，减少系统调用次数。解决的瓶颈：多次 write 系统调用的上下文切换开销。

**Write Spin Count（写旋转次数）**：`doWrite()` 最多尝试 16 次写操作后让出线程，防止一个 Channel 的大量写操作独占 EventLoop，确保同一 EventLoop 上的其他 Channel 能及时得到处理。解决的瓶颈：单个 Channel 写操作的饥饿问题。

**非阻塞 IO + 事件驱动**：`AbstractNioChannel` 在构造时将 JDK Channel 设为非阻塞模式（`ch.configureBlocking(false)`），配合 Selector 的 interest ops 机制（OP_ACCEPT/OP_READ/OP_WRITE/OP_CONNECT），实现了高效的 IO 多路复用。一个 EventLoop 线程可以管理数千个连接。解决的瓶颈：传统 BIO 的"一个连接一个线程"模型的线程资源浪费。

**自适应缓冲区分配（RecvByteBufAllocator）**：`NioByteUnsafe.read()` 中通过 `allocHandle.continueReading()` 自适应决定是否继续读取、分配多大的缓冲区。根据上一次实际读取的数据量动态调整，避免分配过大（浪费内存）或过小（需要多次读取）的缓冲区。解决的瓶颈：静态缓冲区大小无法适应流量波动的问题。
