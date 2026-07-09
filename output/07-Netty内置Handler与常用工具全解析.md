# Netty 内置 Handler 与常用工具全解析

> **Netty 源码深度研究系列 · 第 07 篇**
>
> 基于 Netty 主分支源码，系统讲解 IdleStateHandler、HashedWheelTimer、FlowControlHandler、ChunkedWriteHandler、LoggingHandler、SslHandler 六大核心组件的源码实现，并给出生产级 Pipeline 的 Handler 编排最佳实践。

---

## 一、总览：Netty 内置 Handler 全景图

```
ChannelPipeline
 │
 ├─ [入站方向 Head → Tail]
 │   ├─ SslHandler              ← SSL/TLS 解密（必须放最前）
 │   ├─ IdleStateHandler        ← 空闲检测（读/写/全）
 │   ├─ LoggingHandler          ← 调试日志（可选）
 │   ├─ LengthFieldBasedFrameDecoder ← 拆包
 │   ├─ MessageDecoder          ← 业务解码
 │   ├─ FlowControlHandler      ← 流量控制（可选）
 │   └─ BusinessHandler         ← 业务处理
 │
 ├─ [出站方向 Tail → Head]
 │   ├─ BusinessHandler          ← 业务编码
 │   ├─ ChunkedWriteHandler      ← 大文件分块写入
 │   ├─ IdleStateHandler         ← 空闲检测（出站回调）
 │   ├─ LoggingHandler           ← 调试日志（可选）
 │   └─ SslHandler               ← SSL/TLS 加密（必须放最后）
 │
 └─ [独立工具]
     ├─ HashedWheelTimer         ← 时间轮定时器（不依赖 Pipeline）
     └─ WriteBufferWaterMark     ← 写缓冲区水位线（ChannelConfig 级别）
```

每个 Handler 按"**解决什么问题 → 核心源码 → 使用示例 → 坑点警告**"的结构组织。

---

## 二、IdleStateHandler —— 空闲检测的心脏

### 2.1 解决什么问题

TCP 连接建立后，如果对端因为断电、网线断开等原因非正常退出，操作系统层面的 TCP keep-alive 默认需要 **2 小时**才能检测到。对于 RPC 框架来说，这个延迟不可接受。

IdleStateHandler 提供应用层空闲检测：在指定时间内如果没有读/写操作发生，就触发一个 `IdleStateEvent` 用户事件，业务层可以据此发送心跳包或关闭连接。Dubbo、gRPC、RocketMQ 都依赖它做心跳保活。

### 2.2 三种空闲类型

| 类型 | 触发条件 | 典型用途 |
|------|----------|----------|
| `READER_IDLE` | 在 `readerIdleTime` 内没有收到任何数据 | 对端可能已断开，关闭连接 |
| `WRITER_IDLE` | 在 `writerIdleTime` 内没有发送任何数据 | 主动发送心跳包 |
| `ALL_IDLE` | 在 `allIdleTime` 内既没有读也没有写 | 通用空闲检测 |

三个参数可以任意组合，传 0 表示禁用对应的空闲检测。

### 2.3 核心源码解析

#### 2.3.1 类定义与字段

```java
// IdleStateHandler.java
public class IdleStateHandler extends ChannelDuplexHandler {
    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private final boolean observeOutput;
    private final long readerIdleTimeNanos;
    private final long writerIdleTimeNanos;
    private final long allIdleTimeNanos;

    // EventLoop 的 Ticker，用于获取精确的 nanoTime
    private Ticker ticker = Ticker.systemTicker();

    // 三种定时任务的 Future 引用
    private Future<?> readerIdleTimeout;
    private Future<?> writerIdleTimeout;
    private Future<?> allIdleTimeout;

    // 上次读/写时间戳（纳秒）
    private long lastReadTime;
    private long lastWriteTime;

    // 首次事件标志（区分首次触发和后续触发）
    private boolean firstReaderIdleEvent = true;
    private boolean firstWriterIdleEvent = true;
    private boolean firstAllIdleEvent = true;

    // 是否正在读取（channelRead → channelReadComplete 之间）
    private boolean reading;

    // 状态机
    private byte state;
    private static final byte ST_INITIALIZED = 1;
    private static final byte ST_DESTROYED = 2;

    // 写完成监听器（复用，减少 GC）
    private final ChannelFutureListener writeListener = future -> {
        lastWriteTime = ticker.nanoTime();
        firstWriterIdleEvent = firstAllIdleEvent = true;
    };
}
```

**它是什么**：`IdleStateHandler` 继承 `ChannelDuplexHandler`，同时处理入站和出站事件。入站用于跟踪读时间戳，出站用于跟踪写时间戳。

**为什么用 `Ticker` 而不是直接 `System.nanoTime()`**：Netty 的 EventLoop 有自己的 Ticker 实现，可以在测试中 mock 时间，也保证了所有时间操作都在同一个时钟域内。

#### 2.3.2 初始化：定时任务的注册

```java
// IdleStateHandler.java

@Override
public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    this.ticker = ctx.executor().ticker();
    if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
        // channelActive() 已经触发过了，这里手动初始化
        initialize(ctx);
    }
    // 否则等 channelActive() 回调中初始化
}

@Override
public void channelActive(ChannelHandlerContext ctx) throws Exception {
    initialize(ctx);
    super.channelActive(ctx);
}

private void initialize(ChannelHandlerContext ctx) {
    switch (state) {
    case ST_INITIALIZED:
    case ST_DESTROYED:
        return;  // 防止重复初始化
    default:
        break;
    }

    state = ST_INITIALIZED;
    initOutputChanged(ctx);

    lastReadTime = lastWriteTime = ticker.nanoTime();

    if (readerIdleTimeNanos > 0) {
        readerIdleTimeout = schedule(ctx, new ReaderIdleTimeoutTask(ctx),
                readerIdleTimeNanos, TimeUnit.NANOSECONDS);
    }
    if (writerIdleTimeNanos > 0) {
        writerIdleTimeout = schedule(ctx, new WriterIdleTimeoutTask(ctx),
                writerIdleTimeNanos, TimeUnit.NANOSECONDS);
    }
    if (allIdleTimeNanos > 0) {
        allIdleTimeout = schedule(ctx, new AllIdleTimeoutTask(ctx),
                allIdleTimeNanos, TimeUnit.NANOSECONDS);
    }
}

Future<?> schedule(ChannelHandlerContext ctx, Runnable task, long delay, TimeUnit unit) {
    return ctx.executor().schedule(task, delay, unit);
}
```

**关键设计**：定时任务通过 `ctx.executor().schedule()` 注册到 Channel 绑定的 EventLoop 上，而不是用独立的 `ScheduledExecutorService`。这意味着空闲检测任务和 IO 操作在同一个线程中执行，无需同步。

**初始化时机**：`handlerAdded` 和 `channelActive` 都可能触发初始化。如果 Handler 在 Channel 激活后添加（热加载），则 `handlerAdded` 中初始化；如果在激活前添加，则等 `channelActive` 回调初始化。两种情况都覆盖。

#### 2.3.3 读写时间戳的更新

```java
// IdleStateHandler.java

@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (readerIdleTimeNanos > 0 || allIdleTimeNanos > 0) {
        reading = true;
        firstReaderIdleEvent = firstAllIdleEvent = true;
    }
    ctx.fireChannelRead(msg);
}

@Override
public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    if ((readerIdleTimeNanos > 0 || allIdleTimeNanos > 0) && reading) {
        lastReadTime = ticker.nanoTime();
        reading = false;
    }
    ctx.fireChannelReadComplete();
}

@Override
public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (writerIdleTimeNanos > 0 || allIdleTimeNanos > 0) {
        ctx.write(msg, promise.unvoid()).addListener(writeListener);
    } else {
        ctx.write(msg, promise);
    }
}
```

**为什么 `lastReadTime` 在 `channelReadComplete` 而不是 `channelRead` 中更新**：一次读事件可能触发多次 `channelRead`（TCP 粘包），只有在 `channelReadComplete` 时才表示这一轮读操作完成。用 `reading` 标志位区分：`channelRead` 标记 `reading=true`，`channelReadComplete` 更新时间戳并重置 `reading=false`。

**为什么 `writeListener` 是复用的**：注释明确写了"Not create a new ChannelFutureListener per write operation to reduce GC pressure"。每次 write 都创建一个 lambda 对象在高频写场景下会产生大量短命对象，复用同一个 listener 避免这个问题。

#### 2.3.4 ReaderIdleTimeoutTask —— 读空闲检测

```java
// IdleStateHandler.java
private final class ReaderIdleTimeoutTask extends AbstractIdleTask {

    @Override
    protected void run(ChannelHandlerContext ctx) {
        long nextDelay = readerIdleTimeNanos;
        if (!reading) {
            // 不在读取中，计算自上次读完成以来的空闲时间
            nextDelay -= ticker.nanoTime() - lastReadTime;
        }

        if (nextDelay <= 0) {
            // 已超时 → 空闲！
            // 1. 重新调度一个完整的周期
            readerIdleTimeout = schedule(ctx, this, readerIdleTimeNanos, TimeUnit.NANOSECONDS);

            // 2. 获取 first 标志并重置
            boolean first = firstReaderIdleEvent;
            firstReaderIdleEvent = false;

            // 3. 触发 IdleStateEvent
            try {
                IdleStateEvent event = newIdleStateEvent(IdleState.READER_IDLE, first);
                channelIdle(ctx, event);
            } catch (Throwable t) {
                ctx.fireExceptionCaught(t);
            }
        } else {
            // 还没超时 → 用更短的延迟重新调度
            readerIdleTimeout = schedule(ctx, this, nextDelay, TimeUnit.NANOSECONDS);
        }
    }
}
```

**核心算法**：`nextDelay = readerIdleTimeNanos - (now - lastReadTime)`。如果 `nextDelay <= 0`，说明距离上次读操作已经超过了配置的空闲时间，触发事件；否则用剩余的 `nextDelay` 重新调度。

**为什么不固定间隔检查而是动态计算**：假设 `readerIdleTime=60s`，如果在第 55 秒来了数据，`lastReadTime` 被更新。此时如果固定 60 秒检查，下一次检查在第 115 秒，比实际的 115-55=60 秒晚了 55 秒。而动态计算会在第 55 秒重新调度 60 秒后检查（即第 115 秒），精确度更高。更关键的是，如果没有数据来，动态计算会精确地在超时那一刻触发。

#### 2.3.5 WriterIdleTimeoutTask —— 写空闲检测

```java
// IdleStateHandler.java
private final class WriterIdleTimeoutTask extends AbstractIdleTask {

    @Override
    protected void run(ChannelHandlerContext ctx) {
        long lastWriteTime = IdleStateHandler.this.lastWriteTime;
        long nextDelay = writerIdleTimeNanos - (ticker.nanoTime() - lastWriteTime);
        if (nextDelay <= 0) {
            // Writer is idle
            writerIdleTimeout = schedule(ctx, this, writerIdleTimeNanos, TimeUnit.NANOSECONDS);

            boolean first = firstWriterIdleEvent;
            firstWriterIdleEvent = false;

            try {
                // 额外检查：出站缓冲区是否有变化
                if (hasOutputChanged(ctx, first)) {
                    return;  // 缓冲区在变化，不算空闲
                }

                IdleStateEvent event = newIdleStateEvent(IdleState.WRITER_IDLE, first);
                channelIdle(ctx, event);
            } catch (Throwable t) {
                ctx.fireExceptionCaught(t);
            }
        } else {
            writerIdleTimeout = schedule(ctx, this, nextDelay, TimeUnit.NANOSECONDS);
        }
    }
}
```

**`hasOutputChanged()` 的作用**：当 `observeOutput=true` 时，即使应用层没有调用 `write()`，但如果出站缓冲区中还有数据在慢慢发送（比如大文件传输），Writer 空闲检测不会误触发。它通过检查三个指标判断缓冲区是否有变化：`messageHashCode`（当前消息对象 identity hash code）、`pendingWriteBytes`（待写字节数）、`flushProgress`（flush 进度）。只要任何一个变化，就认为"有输出活动"，不算空闲。

**坑点警告**：`observeOutput` 默认为 `false`，这意味着如果应用层调用了 `write()` 但数据还没真正 flush 到 Socket，Writer 空闲事件仍然会触发。大多数场景下不需要开启 `observeOutput`，只有在写空闲误报频繁时才考虑。

#### 2.3.6 AllIdleTimeoutTask —— 全空闲检测

```java
// IdleStateHandler.java
private final class AllIdleTimeoutTask extends AbstractIdleTask {

    @Override
    protected void run(ChannelHandlerContext ctx) {
        long nextDelay = allIdleTimeNanos;
        if (!reading) {
            // 取读和写中较晚的时间作为"上次活跃时间"
            nextDelay -= ticker.nanoTime() - Math.max(lastReadTime, lastWriteTime);
        }
        if (nextDelay <= 0) {
            allIdleTimeout = schedule(ctx, this, allIdleTimeNanos, TimeUnit.NANOSECONDS);

            boolean first = firstAllIdleEvent;
            firstAllIdleEvent = false;

            try {
                if (hasOutputChanged(ctx, first)) {
                    return;
                }
                IdleStateEvent event = newIdleStateEvent(IdleState.ALL_IDLE, first);
                channelIdle(ctx, event);
            } catch (Throwable t) {
                ctx.fireExceptionCaught(t);
            }
        } else {
            allIdleTimeout = schedule(ctx, this, nextDelay, TimeUnit.NANOSECONDS);
        }
    }
}
```

**核心区别**：`Math.max(lastReadTime, lastWriteTime)` —— 只要读或写任一活动发生，就更新活跃时间。只有在读和写都空闲的情况下才触发 ALL_IDLE 事件。

#### 2.3.7 IdleStateEvent 与事件传播

```java
// IdleStateEvent.java
public class IdleStateEvent {
    // 6 个预定义常量，避免重复创建对象
    public static final IdleStateEvent FIRST_READER_IDLE_STATE_EVENT =
            new DefaultIdleStateEvent(IdleState.READER_IDLE, true);
    public static final IdleStateEvent READER_IDLE_STATE_EVENT =
            new DefaultIdleStateEvent(IdleState.READER_IDLE, false);
    public static final IdleStateEvent FIRST_WRITER_IDLE_STATE_EVENT =
            new DefaultIdleStateEvent(IdleState.WRITER_IDLE, true);
    public static final IdleStateEvent WRITER_IDLE_STATE_EVENT =
            new DefaultIdleStateEvent(IdleState.WRITER_IDLE, false);
    public static final IdleStateEvent FIRST_ALL_IDLE_STATE_EVENT =
            new DefaultIdleStateEvent(IdleState.ALL_IDLE, true);
    public static final IdleStateEvent ALL_IDLE_STATE_EVENT =
            new DefaultIdleStateEvent(IdleState.ALL_IDLE, false);

    private final IdleState state;
    private final boolean first;
}
```

```java
// IdleStateHandler.java
protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
    ctx.fireUserEventTriggered(evt);
}

protected IdleStateEvent newIdleStateEvent(IdleState state, boolean first) {
    switch (state) {
        case ALL_IDLE:
            return first ? IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT : IdleStateEvent.ALL_IDLE_STATE_EVENT;
        case READER_IDLE:
            return first ? IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT : IdleStateEvent.READER_IDLE_STATE_EVENT;
        case WRITER_IDLE:
            return first ? IdleStateEvent.FIRST_WRITER_IDLE_STATE_EVENT : IdleStateEvent.WRITER_IDLE_STATE_EVENT;
        default:
            throw new IllegalArgumentException("Unhandled: state=" + state + ", first=" + first);
    }
}
```

**为什么用 `fireUserEventTriggered` 而不是 `fireChannelRead`**：空闲事件不是网络数据，不应该走 `channelRead` 链路。Netty 的 `userEventTriggered` 是专门为这类非 IO 事件设计的传播通道，不会干扰正常的数据处理流程。

**`first` 标志的设计意图**：首次空闲事件和后续空闲事件的业务处理可能不同。比如首次 Writer 空闲时发送心跳，后续空闲时可能检查心跳是否回复。6 个预定义常量避免了每次触发都创建新对象。

#### 2.3.8 销毁逻辑

```java
// IdleStateHandler.java
private void destroy() {
    state = ST_DESTROYED;

    if (readerIdleTimeout != null) {
        readerIdleTimeout.cancel(false);
        readerIdleTimeout = null;
    }
    if (writerIdleTimeout != null) {
        writerIdleTimeout.cancel(false);
        writerIdleTimeout = null;
    }
    if (allIdleTimeout != null) {
        allIdleTimeout.cancel(false);
        allIdleTimeout = null;
    }
}
```

在 `channelInactive` 和 `handlerRemoved` 时调用，取消所有定时任务，防止连接关闭后任务还在执行。

### 2.4 使用示例

```java
// 通用空闲检测：60 秒无读 → 关连接，30 秒无写 → 发心跳
pipeline.addLast(new IdleStateHandler(60, 30, 0, TimeUnit.SECONDS));
pipeline.addLast(new ChannelDuplexHandler() {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            switch (e.state()) {
                case READER_IDLE:
                    log.warn("60 秒未收到数据，关闭连接: {}", ctx.channel().remoteAddress());
                    ctx.close();
                    break;
                case WRITER_IDLE:
                    log.debug("30 秒未发送数据，发送心跳");
                    ctx.writeAndFlush(HEARTBEAT_PACKET);
                    break;
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
});
```

### 2.5 Dubbo 如何使用 IdleStateHandler 做心跳检测

Dubbo 在 `NettyCodecAdapter` 构建 Pipeline 时加入 `IdleStateHandler`：

```java
// Dubbo NettyClient.java（简化）
bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
    @Override
    protected void initChannel(NioSocketChannel ch) {
        ch.pipeline()
            .addLast("decoder", adapter.getDecoder())
            .addLast("encoder", adapter.getEncoder())
            // Client 端：60 秒无读 → 重连，30 秒无写 → 发心跳
            .addLast("client-idle-handler",
                new IdleStateHandler(60, 30, 0, TimeUnit.SECONDS))
            .addLast("handler", new NettyClientHandler());
    }
});

// Dubbo NettyServer.java（简化）
.childHandler(new ChannelInitializer<NioSocketChannel>() {
    @Override
    protected void initChannel(NioSocketChannel ch) {
        ch.pipeline()
            .addLast("decoder", adapter.getDecoder())
            .addLast("encoder", adapter.getEncoder())
            // Server 端：60 秒无读 → 关连接（Client 不发心跳就断）
            .addLast("server-idle-handler",
                new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS))
            .addLast("handler", new NettyServerHandler());
    }
});
```

Dubbo 的心跳策略是**非对称**的：Client 端配置 Writer 空闲发送心跳，Server 端只配置 Reader 空闲关闭连接。Client 的心跳回复由 Server 的正常 `channelRead` 隐式完成，不需要额外的 Writer 空闲检测。

**为什么 Dubbo 不用 TCP keep-alive**：TCP keep-alive 的检测周期默认是 2 小时，即使调小也受限于操作系统内核参数（`tcp_keepalive_time`、`tcp_keepalive_intvl`、`tcp_keepalive_probes`），不够灵活。而且 TCP keep-alive 只能检测连接是否存活，无法在应用层做心跳响应逻辑（如统计延迟、触发重连）。

### 2.6 坑点警告

**坑 1：`channelRead` 和 `channelReadComplete` 之间的间隙**。`lastReadTime` 在 `channelReadComplete` 时更新，不是 `channelRead`。如果业务 Handler 在 `channelRead` 中就做耗时操作，导致 `channelReadComplete` 延迟触发，读空闲检测的时间会比实际晚。

**坑 2：不要在 `userEventTriggered` 中做耗时操作**。空闲事件在 EventLoop 线程中触发，如果在此方法中做阻塞操作（如查数据库），会阻塞所有该 EventLoop 管理的 Channel。

**坑 3：`first` 标志在首次触发后不会自动恢复**。只有在下次有实际读/写活动时才会重置为 `true`。如果业务依赖首次事件做特殊逻辑，注意连续空闲时只会收到一次 `first=true` 的事件。

---

## 三、HashedWheelTimer —— 时间轮定时器

### 3.1 解决什么问题

当你需要管理**大量**的定时任务（比如几万个连接的超时检测），使用 JDK 的 `ScheduledThreadPoolExecutor` 会有性能问题：每个任务都是一个 `ScheduledFutureTask` 对象，放在一个 `DelayedWorkQueue`（基于二叉堆）中，插入和删除的时间复杂度都是 O(log n)。当任务数量达到万级以上时，堆的维护开销显著。

HashedWheelTimer 基于 George Varghese 和 Tony Lauck 的论文《Hashed and Hierarchical Timing Wheels》，用**时间轮**数据结构将添加任务的时间复杂度降为 O(1)，代价是牺牲精度——任务的执行时间最多有 `tickDuration` 的误差。

### 3.2 数据结构

```
时间轮结构示意（wheel.length = 8，实际默认 512）：

wheel[] 数组（每个元素是一个 HashedWheelBucket 双向链表）
 ┌─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┐
 │ Bucket0 │ Bucket1 │ Bucket2 │ Bucket3 │ Bucket4 │ Bucket5 │ Bucket6 │ Bucket7 │
 │  ↓      │  ↓      │  ↓      │  ↓      │  ↓      │  ↓      │  ↓      │  ↓      │
 │ Timeout │ Timeout │ (empty) │ Timeout │ (empty) │ (empty) │ Timeout │ (empty) │
 │  ↓      │  ↓      │         │         │         │         │         │         │
 │ Timeout │ (end)   │         │         │         │         │         │         │
 │  ↓      │         │         │         │         │         │         │         │
 │ (end)   │         │         │         │         │         │         │         │
 └─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
      ↑
   tick 指针（每 tickDuration ms 前进一步，对 mask 做位运算取模）
```

核心数据结构：

| 组件 | 类型 | 职责 |
|------|------|------|
| `wheel` | `HashedWheelBucket[]` | 轮盘数组，每个元素是一个双向链表头 |
| `mask` | `int` | `wheel.length - 1`，用于位运算取模 |
| `tick` | `long` | 当前 tick 数（Worker 线程维护） |
| `timeouts` | `Queue<HashedWheelTimeout>` | MPSC 队列，暂存新提交的任务 |
| `cancelledTimeouts` | `Queue<HashedWheelTimeout>` | MPSC 队列，暂存取消的任务 |

### 3.3 核心源码解析

#### 3.3.1 构造与 createWheel()

```java
// HashedWheelTimer.java
public HashedWheelTimer(
        ThreadFactory threadFactory,
        long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
        long maxPendingTimeouts, Executor taskExecutor) {

    checkNotNull(threadFactory, "threadFactory");
    checkNotNull(unit, "unit");
    checkPositive(tickDuration, "tickDuration");
    checkPositive(ticksPerWheel, "ticksPerWheel");
    this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor");

    // 1. 规范化轮盘大小为 2 的幂
    wheel = createWheel(ticksPerWheel);
    mask = wheel.length - 1;

    // 2. 转换 tickDuration 为纳秒
    long duration = unit.toNanos(tickDuration);
    if (duration >= Long.MAX_VALUE / wheel.length) {
        throw new IllegalArgumentException(...);
    }
    // 最小精度 1ms
    if (duration < MILLISECOND_NANOS) {
        logger.warn("Configured tickDuration {} smaller than {}, using 1ms.",
                    tickDuration, MILLISECOND_NANOS);
        this.tickDuration = MILLISECOND_NANOS;
    } else {
        this.tickDuration = duration;
    }

    // 3. 创建 Worker 线程
    workerThread = threadFactory.newThread(worker);

    // 4. 泄漏检测
    leak = leakDetection || !workerThread.isDaemon() ? leakDetector.track(this) : null;

    this.maxPendingTimeouts = maxPendingTimeouts;

    // 5. 实例数超过 64 个时发出警告
    if (INSTANCE_COUNTER.incrementAndGet() > INSTANCE_COUNT_LIMIT &&
        WARNED_TOO_MANY_INSTANCES.compareAndSet(false, true)) {
        reportTooManyInstances();
    }
}

private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
    // 规范化为 2 的幂
    ticksPerWheel = MathUtil.findNextPositivePowerOfTwo(ticksPerWheel);

    HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
    for (int i = 0; i < wheel.length; i++) {
        wheel[i] = new HashedWheelBucket();
    }
    return wheel;
}
```

**为什么轮盘大小必须为 2 的幂**：取模运算 `tick % wheel.length` 可以用位运算 `tick & mask` 替代，性能更高。`MathUtil.findNextPositivePowerOfTwo(512) = 512`（已经是 2 的幂），`findNextPositivePowerOfTwo(513) = 1024`。

**为什么限制实例数**：每个 `HashedWheelTimer` 会创建一个独立线程。如果为每个连接创建一个时间轮，线程数会爆炸。注释明确警告："Do not create many instances."。Netty 建议全应用共享一个实例。

**默认配置**：`tickDuration=100ms`，`ticksPerWheel=512`。这意味着一轮耗时 `512 × 100ms = 51.2s`。

#### 3.3.2 newTimeout() —— 提交任务

```java
// HashedWheelTimer.java
@Override
public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
    checkNotNull(task, "task");
    checkNotNull(unit, "unit");

    long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();

    if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
        pendingTimeouts.decrementAndGet();
        throw new RejectedExecutionException("Number of pending timeouts ("
            + pendingTimeoutsCount + ") is greater than or equal to maximum allowed pending "
            + "timeouts (" + maxPendingTimeouts + ")");
    }

    // 懒启动 Worker 线程
    start();

    // 计算相对于 startTime 的 deadline
    long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

    // 防止溢出
    if (delay > 0 && deadline < 0) {
        deadline = Long.MAX_VALUE;
    }

    // 创建 Timeout 节点，放入 MPSC 队列
    HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
    timeouts.add(timeout);
    return timeout;
}
```

**关键设计**：`newTimeout()` 不直接将任务放入轮盘，而是放入 `timeouts` MPSC 队列。这是因为 `newTimeout()` 可能被多个线程调用（多生产者），但轮盘操作只能在 Worker 线程中进行（单消费者）。MPSC 队列保证了多线程提交的安全性。

**`deadline` 的计算**：`deadline = System.nanoTime() + delay - startTime`。`startTime` 是 Worker 线程启动时记录的 `System.nanoTime()`。所有 deadline 都是相对于 `startTime` 的偏移量，这样 Worker 线程只需计算 `currentTime - startTime` 即可判断是否超时。

#### 3.3.3 Worker.run() —— 核心调度循环

```java
// HashedWheelTimer.java
private final class Worker implements Runnable {
    private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();
    private long tick;

    @Override
    public void run() {
        // 初始化 startTime
        startTime = System.nanoTime();
        if (startTime == 0) {
            startTime = 1;  // 0 用作未初始化标志
        }

        // 通知 start() 方法中等待的线程
        startTimeInitialized.countDown();

        do {
            // 1. 等待下一个 tick
            final long deadline = waitForNextTick();
            if (deadline > 0) {
                // 2. 计算当前 tick 对应的 bucket 索引
                int idx = (int) (tick & mask);

                // 3. 处理已取消的任务
                processCancelledTasks();

                // 4. 将 timeouts 队列中的任务转移到对应的 bucket 中
                transferTimeoutsToBuckets();

                // 5. 过期当前 bucket 中的任务
                HashedWheelBucket bucket = wheel[idx];
                bucket.expireTimeouts(deadline);

                tick++;
            }
        } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

        // 清理：收集未处理的任务
        for (HashedWheelBucket bucket: wheel) {
            bucket.clearTimeouts(unprocessedTimeouts);
        }
        for (;;) {
            HashedWheelTimeout timeout = timeouts.poll();
            if (timeout == null) break;
            if (!timeout.isCancelled()) {
                unprocessedTimeouts.add(timeout);
            }
        }
        processCancelledTasks();
    }
}
```

**每 tick 的四步操作**：

1. `waitForNextTick()` —— 阻塞等待到下一个 tick 时间点
2. `processCancelledTasks()` —— 从 `cancelledTimeouts` 队列取出已取消的任务，从 bucket 中移除
3. `transferTimeoutsToBuckets()` —— 从 `timeouts` 队列取出新任务，计算它们应该放入哪个 bucket
4. `bucket.expireTimeouts(deadline)` —— 遍历当前 bucket 的链表，过期到期的任务

#### 3.3.4 transferTimeoutsToBuckets() —— 任务入轮

```java
// HashedWheelTimer.java
private void transferTimeoutsToBuckets() {
    // 每 tick 最多转移 100000 个，防止提交过快导致 Worker 饿死
    for (int i = 0; i < 100000; i++) {
        HashedWheelTimeout timeout = timeouts.poll();
        if (timeout == null) break;
        if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) continue;

        // 计算任务应该在哪个 tick 触发
        long calculated = timeout.deadline / tickDuration;

        // 计算剩余圈数（如果任务在很久以后执行，需要转多圈）
        timeout.remainingRounds = (calculated - tick) / wheel.length;

        // 确保不调度到过去的 tick
        final long ticks = Math.max(calculated, tick);
        int stopIndex = (int) (ticks & mask);

        HashedWheelBucket bucket = wheel[stopIndex];
        bucket.addTimeout(timeout);
    }
}
```

**`remainingRounds` 的含义**：假设 `tickDuration=100ms`，`wheel.length=512`，一轮 = 51.2s。如果任务延迟 120s，`calculated = 120000 / 100 = 1200`，当前 `tick = 0`，`remainingRounds = (1200 - 0) / 512 = 2`。意味着这个任务在轮盘上需要转 2 圈才会在第 1200 个 tick 时触发。Worker 每经过一个 bucket 时，如果 `remainingRounds > 0`，就减 1；当 `remainingRounds == 0` 时才真正过期。

#### 3.3.5 waitForNextTick() —— 精确等待

```java
// HashedWheelTimer.java
private long waitForNextTick() {
    long deadline = tickDuration * (tick + 1);

    for (;;) {
        final long currentTime = System.nanoTime() - startTime;
        // 计算需要 sleep 的毫秒数
        long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

        if (sleepTimeMs <= 0) {
            if (currentTime == Long.MIN_VALUE) {
                return -Long.MAX_VALUE;
            } else {
                return currentTime;
            }
        }

        // Windows 平台特殊处理：Thread.sleep 精度问题
        if (PlatformDependent.isWindows()) {
            sleepTimeMs = sleepTimeMs / 10 * 10;
            if (sleepTimeMs == 0) {
                sleepTimeMs = 1;
            }
        }

        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException ignored) {
            if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                return Long.MIN_VALUE;
            }
        }
    }
}
```

**Windows bug workaround**：Windows 上 `Thread.sleep()` 的精度是 10ms 的倍数，如果传入 5ms 实际可能 sleep 15ms。Netty 将 `sleepTimeMs` 向下取整到 10 的倍数，避免精度问题导致 tick 延迟累积。

#### 3.3.6 HashedWheelBucket.expireTimeouts() —— 过期任务

```java
// HashedWheelTimer.java
private static final class HashedWheelBucket {
    private HashedWheelTimeout head;
    private HashedWheelTimeout tail;

    public void expireTimeouts(long deadline) {
        HashedWheelTimeout timeout = head;
        while (timeout != null) {
            HashedWheelTimeout next = timeout.next;
            if (timeout.remainingRounds <= 0) {
                if (timeout.deadline <= deadline) {
                    // 到期了！
                    timeout.expire();
                } else {
                    // 不应该发生
                    throw new IllegalStateException(String.format(
                        "timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline));
                }
            } else if (!timeout.isCancelled()) {
                // 还没到圈数，减一圈
                timeout.remainingRounds--;
            }
            timeout = next;
        }
    }

    public void addTimeout(HashedWheelTimeout timeout) {
        assert timeout.bucket == null;
        timeout.bucket = this;
        if (head == null) {
            head = tail = timeout;
        } else {
            tail.next = timeout;
            timeout.prev = tail;
            tail = timeout;
        }
    }
}
```

**双向链表的设计**：`HashedWheelTimeout` 本身充当链表节点（`next`/`prev`），不需要额外创建包装对象。取消操作时可以 O(1) 从链表中移除。

#### 3.3.7 HashedWheelTimeout —— 任务节点

```java
// HashedWheelTimer.java
private static final class HashedWheelTimeout implements Timeout, Runnable {
    private static final int ST_INIT = 0;
    private static final int ST_CANCELLED = 1;
    private static final int ST_EXPIRED = 2;
    private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");

    private final HashedWheelTimer timer;
    private final TimerTask task;
    private final long deadline;
    private volatile int state = ST_INIT;

    long remainingRounds;
    HashedWheelTimeout next;
    HashedWheelTimeout prev;
    HashedWheelBucket bucket;

    @Override
    public boolean cancel() {
        if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
            return false;
        }
        // 不直接从 bucket 移除，而是放入 cancelledTimeouts 队列
        // 下一个 tick 时由 processCancelledTasks() 统一处理
        timer.cancelledTimeouts.add(this);
        return true;
    }

    public void expire() {
        if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
            return;
        }
        try {
            remove();
            // 通过 taskExecutor 执行，避免阻塞 Worker 线程
            timer.taskExecutor.execute(this);
        } catch (Throwable t) {
            logger.warn("An exception was thrown while submit " + TimerTask.class.getSimpleName()
                    + " for execution.", t);
        }
    }

    @Override
    public void run() {
        try {
            task.run(this);
        } catch (Throwable t) {
            logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
        }
    }
}
```

**取消操作为什么不直接从 bucket 移除**：`cancel()` 可能被任何线程调用，但 bucket 链表只在 Worker 线程中操作。直接修改链表需要同步，会破坏单线程模型。放入 `cancelledTimeouts` MPSC 队列后，Worker 线程在下个 tick 的 `processCancelledTasks()` 中统一移除，最多有一个 tick（默认 100ms）的延迟。

**任务执行通过 `taskExecutor`**：过期任务不直接在 Worker 线程中执行 `task.run()`，而是提交到 `taskExecutor`。默认 `taskExecutor` 是 `ImmediateExecutor`（在当前线程执行），但可以配置为线程池，避免耗时任务阻塞时间轮推进。

### 3.4 使用示例

```java
// 全局共享一个 HashedWheelTimer 实例
Timer timer = new HashedWheelTimer(
    new DefaultThreadFactory("wheel-timer"),
    100, TimeUnit.MILLISECONDS,  // tickDuration = 100ms
    512,                          // wheel size = 512
    true,                         // leak detection
    -1,                           // no max pending limit
    ImmediateExecutor.INSTANCE    // 在 Worker 线程中执行
);

// 提交一个 5 秒后执行的任务
Timeout timeout = timer.newTimeout(task -> {
    System.out.println("5 秒后执行");
}, 5, TimeUnit.SECONDS);

// 取消任务
timeout.cancel();

// 关闭时间轮（返回未处理的任务）
Set<Timeout> unprocessed = timer.stop();
```

### 3.5 与 JDK ScheduledThreadPoolExecutor 对比

| 维度 | HashedWheelTimer | ScheduledThreadPoolExecutor |
|------|------------------|-----------------------------|
| 数据结构 | 轮盘数组 + 链表 | DelayedWorkQueue（二叉堆） |
| 添加任务 | O(1) 放入 MPSC 队列 | O(log n) 堆插入 |
| 取消任务 | O(1) CAS + 延迟移除 | O(log n) 堆删除 |
| 执行精度 | tickDuration 精度（默认 100ms 误差） | 精确到纳秒 |
| 线程数 | 1 个 Worker 线程 | 可配置线程池 |
| 适用场景 | 大量低精度定时任务（连接超时、心跳超时） | 少量高精度定时任务 |
| 内存占用 | 每个任务一个 HashedWheelTimeout 对象 | 每个任务一个 ScheduledFutureTask + RunnableScheduledFuture 包装 |

**Dubbo 的使用选择**：Dubbo 的 `DefaultFuture` 超时检测原本用 `HashedWheelTimer`（在 `TimeScheduledCheckService` 中），而 Netty 自身的 `IdleStateHandler` 用 `EventLoop.schedule()`（底层是 `ScheduledThreadPoolExecutor`）。原因在于：`IdleStateHandler` 每个 Channel 只有 3 个定时任务，任务量小，用 EventLoop 自带的调度器即可，无需引入额外的时间轮线程。

**坑点警告**：

**坑 1**：不要为每个连接创建一个 `HashedWheelTimer`。每个实例一个线程，1000 个连接就是 1000 个线程。应该全局共享一个。

**坑 2**：`HashedWheelTimer` 的精度是 `tickDuration`。如果你设置 100ms 的 tick，一个 350ms 的任务实际可能在 300~400ms 之间执行。不能用于高精度定时场景。

**坑 3**：`stop()` 不能在 Worker 线程内部调用（即在 TimerTask 的 `run` 方法中），会抛 `IllegalStateException`。如果需要在任务中关闭时间轮，应该通过其他线程调用。

---

## 四、FlowControlHandler —— 流量控制

### 4.1 解决什么问题

Netty 的 `autoRead` 机制可以让 Channel 自动读取数据并传播 `channelRead` 事件。但当 `autoRead=false` 时，业务需要手动调用 `channel.read()` 来触发数据读取。

问题在于：像 `ByteToMessageDecoder` 这样的解码器，一次 `channelRead` 可能产出**多条**业务消息（比如粘包解码出 3 个请求），但下游 Handler 可能只想一次处理一个。`FlowControlHandler` 解决的就是这个问题——**确保每次 `read()` 只向下游投递一条消息**。

典型场景：HTTP 解码器一次性产出 `HttpRequest` + `LastHttpContent` 两个事件，但业务 Handler 在处理 `HttpRequest` 时可能需要暂停处理，等异步操作完成后再处理 `LastHttpContent`。

### 4.2 核心源码解析

#### 4.2.1 类定义与核心字段

```java
// FlowControlHandler.java
public class FlowControlHandler extends ChannelDuplexHandler {

    private final boolean releaseMessages;

    // 缓存队列：暂存还未投递给下游的消息
    private RecyclableArrayDeque queue;

    private ChannelConfig config;

    // 未满足的 read() 调用计数
    private int unsatisfiedReads;

    // 是否正在出队（防止重入）
    private boolean dequeuing;
}
```

**`unsatisfiedReads` 的含义**：当 `autoRead=false` 时，每次下游调用 `ctx.read()` 都代表"我要一条消息"。如果队列里暂时没有消息，这个 `read()` 就是"未满足"的。当消息到来时，`unsatisfiedReads` 个消息会被投递给下游。

#### 4.2.2 read() —— 拦截下游读请求

```java
// FlowControlHandler.java
@Override
public void read(ChannelHandlerContext ctx) throws Exception {
    if (!config.isAutoRead()) {
        unsatisfiedReads++;
    }

    boolean didSatisfyARead = dequeue(ctx);
    boolean isAutoRead = config.isAutoRead();
    if (!didSatisfyARead || isAutoRead) {
        // 没有满足这次 read，或者 autoRead 开启 → 继续向上游传递 read 请求
        ctx.read();
    } else if (unsatisfiedReads == 0 && !dequeuing) {
        // autoRead 关闭，且所有 read 都已满足 → 完成当前读周期
        ctx.fireChannelReadComplete();
    }
    // 否则等待后续的 channelRead 或 channelReadComplete
}
```

**三种分支**：

1. 队列为空（`didSatisfyARead=false`）或 `autoRead=true`：调用 `ctx.read()` 向上游传递读请求，让数据从 Socket 读入。
2. `autoRead=false` 且所有 read 已满足（`unsatisfiedReads==0`）：触发 `channelReadComplete` 结束当前读周期，不再从 Socket 读新数据。
3. 还有未满足的 read 但队列暂时为空：什么都不做，等待上游 `channelRead` 带来新消息。

#### 4.2.3 channelRead() —— 入队并尝试出队

```java
// FlowControlHandler.java
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (queue == null) {
        queue = RecyclableArrayDeque.newInstance();
    }

    queue.offer(msg);

    if (dequeue(ctx)) {
        if (!config.isAutoRead() && unsatisfiedReads == 0 && !dequeuing) {
            ctx.fireChannelReadComplete();
        }
    }
}
```

消息到来时先入队，然后尝试出队。`dequeue()` 会根据 `autoRead` 和 `unsatisfiedReads` 决定投递多少条消息。

#### 4.2.4 dequeue() —— 精确控制投递数量

```java
// FlowControlHandler.java
private boolean dequeue(ChannelHandlerContext ctx) {
    boolean didSatisfyARead = false;

    boolean wasDequeuing = dequeuing;
    dequeuing = true;
    try {
        // autoRead 开启 → 全部取出
        // autoRead 关闭 → 只取 unsatisfiedReads 个
        while (queue != null && (config.isAutoRead() || unsatisfiedReads > 0)) {
            Object msg = queue.poll();
            if (msg == null) break;

            if (unsatisfiedReads > 0) {
                unsatisfiedReads--;
            }
            ctx.fireChannelRead(msg);

            didSatisfyARead = true;
        }

        // 队列空了 → 回收
        if (queue != null && queue.isEmpty()) {
            queue.recycle();
            queue = null;
        }

        return didSatisfyARead;
    } finally {
        dequeuing = wasDequeuing;
    }
}
```

**核心逻辑**：`while` 循环的条件是 `config.isAutoRead() || unsatisfiedReads > 0`。当 `autoRead=true` 时，无条件投递所有消息（不做流量控制）；当 `autoRead=false` 时，只投递 `unsatisfiedReads` 个消息，实现精确的 1:1 消息投递。

**`dequeuing` 防重入**：`fireChannelRead` 可能触发下游 Handler 调用 `ctx.read()`，进而重入 `dequeue()`。`dequeuing` 标志确保只有最外层的 `dequeue()` 在完成后才触发 `channelReadComplete`。

#### 4.2.5 RecyclableArrayDeque —— 可回收队列

```java
// FlowControlHandler.java
private static final class RecyclableArrayDeque extends ArrayDeque<Object> {
    private static final int DEFAULT_NUM_ELEMENTS = 2;
    private static final Recycler<RecyclableArrayDeque> RECYCLER =
            new Recycler<RecyclableArrayDeque>() {
                @Override
                protected RecyclableArrayDeque newObject(Handle<RecyclableArrayDeque> handle) {
                    return new RecyclableArrayDeque(DEFAULT_NUM_ELEMENTS, handle);
                }
            };

    public static RecyclableArrayDeque newInstance() {
        return RECYCLER.get();
    }

    private final Handle<RecyclableArrayDeque> handle;

    private RecyclableArrayDeque(int numElements, Handle<RecyclableArrayDeque> handle) {
        super(numElements);
        this.handle = handle;
    }

    public void recycle() {
        clear();
        handle.recycle(this);
    }
}
```

队列使用 Netty 的 `Recycler` 对象池管理，避免频繁创建 `ArrayDeque` 对象。默认初始容量 2（大多数情况下队列中只有 1~2 条消息）。

### 4.3 使用示例

```java
pipeline.addLast(new HttpServerCodec());
pipeline.addLast(new FlowControlHandler());  // 确保一次只投递一个 HTTP 消息
pipeline.addLast(new MyBusinessHandler());

class MyBusinessHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            ctx.channel().config().setAutoRead(false);  // 暂停读取

            // 异步处理请求
            asyncProcess((HttpRequest) msg).thenAccept(response -> {
                ctx.writeAndFlush(response);
                ctx.channel().config().setAutoRead(true);  // 恢复读取
            });
        }
        // LastHttpContent 会被 FlowControlHandler 缓存，直到 autoRead 恢复
    }
}
```

### 4.4 坑点警告

**坑 1**：`FlowControlHandler` 只在 `autoRead=false` 时生效。如果 `autoRead=true`（默认），它就是一个透明透传的 Handler，不做任何流量控制。

**坑 2**：如果 Handler 被移除时队列中还有未投递的消息，`handlerRemoved` 会尝试一次性 `dequeue` 全部消息。如果 `releaseMessages=true`（默认），未投递的消息会被 `ReferenceCountUtil.safeRelease()` 释放，避免内存泄漏。

---

## 五、ChunkedWriteHandler —— 大文件分块写入

### 5.1 解决什么问题

直接写一个 1GB 的文件到 Channel，需要先将整个文件读入内存（ByteBuf），然后一次性 `writeAndFlush`。这会导致 OOM。

`ChunkedWriteHandler` 配合 `ChunkedInput` 接口，将大文件分成固定大小的块（默认 8KB），逐块读取、逐块写入。通过 `WriteBufferWaterMark` 水位线机制，当 Channel 写缓冲区满时暂停读取，水位恢复时继续——实现**背压（backpressure）**。

### 5.2 核心源码解析

#### 5.2.1 write() —— 拦截 ChunkedInput 类型消息

```java
// ChunkedWriteHandler.java
public class ChunkedWriteHandler extends ChannelDuplexHandler {

    private Queue<PendingWrite> queue;
    private volatile ChannelHandlerContext ctx;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!queueIsEmpty() || msg instanceof ChunkedInput) {
            // 有排队的 ChunkedInput，或者当前消息是 ChunkedInput → 入队
            allocateQueue();
            queue.add(new PendingWrite(msg, promise));
        } else {
            // 非 ChunkedInput 消息直接透传
            ctx.write(msg, promise);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        doFlush(ctx);
    }
}
```

**设计要点**：只有 `ChunkedInput` 类型的消息才会被拦截入队。普通消息（如业务响应 ByteBuf）直接透传，不经过分块逻辑。队列保证多个 `ChunkedInput` 按顺序发送。

#### 5.2.2 doFlush() —— 分块写入核心

```java
// ChunkedWriteHandler.java
private void doFlush(final ChannelHandlerContext ctx) {
    final Channel channel = ctx.channel();
    if (!channel.isActive()) {
        discard(null);
        ctx.flush();
        return;
    }

    if (queueIsEmpty()) {
        ctx.flush();
        return;
    }

    boolean requiresFlush = true;
    ByteBufAllocator allocator = ctx.alloc();
    // 关键：channel.isWritable() 控制背压
    while (channel.isWritable()) {
        final PendingWrite currentWrite = queue.peek();
        if (currentWrite == null) break;

        if (currentWrite.promise.isDone()) {
            queue.remove();
            continue;
        }

        final Object pendingMessage = currentWrite.msg;

        if (pendingMessage instanceof ChunkedInput) {
            final ChunkedInput<?> chunks = (ChunkedInput<?>) pendingMessage;
            boolean endOfInput;
            boolean suspend;
            Object message = null;
            try {
                // 读取一块数据
                message = chunks.readChunk(allocator);
                endOfInput = chunks.isEndOfInput();
                suspend = message == null && !endOfInput;
            } catch (final Throwable t) {
                queue.remove();
                if (message != null) ReferenceCountUtil.release(message);
                closeInput(chunks);
                currentWrite.fail(t);
                break;
            }

            if (suspend) {
                // ChunkedInput 还没到末尾，但暂时没有数据 → 等待 resumeTransfer()
                break;
            }

            if (message == null) {
                message = Unpooled.EMPTY_BUFFER;
            }

            if (endOfInput) {
                queue.remove();
            }

            // 写入并 flush 这一块
            ChannelFuture f = ctx.writeAndFlush(message);
            if (endOfInput) {
                // 最后一块 → 完成时回调
                if (f.isDone()) {
                    handleEndOfInputFuture(f, chunks, currentWrite);
                } else {
                    f.addListener((ChannelFutureListener) future ->
                            handleEndOfInputFuture(future, chunks, currentWrite));
                }
            } else {
                // 非最后一块 → 完成时检查是否需要继续
                final boolean resume = !channel.isWritable();
                if (f.isDone()) {
                    handleFuture(f, chunks, currentWrite, resume);
                } else {
                    f.addListener((ChannelFutureListener) future ->
                            handleFuture(future, chunks, currentWrite, resume));
                }
            }
            requiresFlush = false;
        } else {
            // 非 ChunkedInput 消息直接写出
            queue.remove();
            ctx.write(pendingMessage, currentWrite.promise);
            requiresFlush = true;
        }

        if (!channel.isActive()) {
            discard(new ClosedChannelException());
            break;
        }
    }

    if (requiresFlush) {
        ctx.flush();
    }
}
```

**背压机制**：`while (channel.isWritable())` 是核心。`channel.isWritable()` 检查 ChannelOutboundBuffer 中的待发送字节数是否超过高水位线（`WriteBufferWaterMark.high`，默认 64KB）。如果超过，`isWritable()` 返回 `false`，循环退出，不再读取新的 chunk。当待发送字节数降到低水位线（默认 32KB）以下时，触发 `channelWritabilityChanged` 事件，恢复 `doFlush`。

#### 5.2.3 channelWritabilityChanged() —— 背压恢复

```java
// ChunkedWriteHandler.java
@Override
public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().isWritable()) {
        // 水位恢复 → 继续分块写入
        doFlush(ctx);
    }
    ctx.fireChannelWritabilityChanged();
}
```

**联动流程**：

```
doFlush() 写入 chunk → ChannelOutboundBuffer 积压 → 超过高水位线 64KB
    → channel.isWritable() = false → while 循环退出 → 停止读取新 chunk
    → Socket 发送数据 → 积压降到低水位线 32KB
    → channelWritabilityChanged 事件触发
    → doFlush() 恢复 → 继续读取下一个 chunk → 循环
```

#### 5.2.4 WriteBufferWaterMark —— 水位线

```java
// WriteBufferWaterMark.java
public final class WriteBufferWaterMark {
    private static final int DEFAULT_LOW_WATER_MARK = 32 * 1024;   // 32KB
    private static final int DEFAULT_HIGH_WATER_MARK = 64 * 1024;  // 64KB

    public static final WriteBufferWaterMark DEFAULT =
            new WriteBufferWaterMark(DEFAULT_LOW_WATER_MARK, DEFAULT_HIGH_WATER_MARK, false);

    private final int low;
    private final int high;
}
```

**为什么需要高低两个水位线而不是一个**：如果只有一个阈值，当积压量在阈值附近波动时，`isWritable()` 会频繁切换 true/false，导致 `channelWritabilityChanged` 事件频繁触发（抖动）。高/低两个水位线形成**滞后区间（hysteresis）**：超过高水位线才变为不可写，降到低水位线才恢复可写，中间区间保持状态不变。

#### 5.2.5 ChunkedFile —— 文件分块读取

```java
// ChunkedFile.java
public class ChunkedFile implements ChunkedInput<ByteBuf> {
    private final RandomAccessFile file;
    private final long startOffset;
    private final long endOffset;
    private final int chunkSize;
    private long offset;

    public ChunkedFile(RandomAccessFile file, long offset, long length, int chunkSize) throws IOException {
        this.file = file;
        this.offset = startOffset = offset;
        this.endOffset = offset + length;
        this.chunkSize = chunkSize;
        file.seek(offset);
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        long offset = this.offset;
        if (offset >= endOffset) {
            return null;
        }

        int chunkSize = (int) Math.min(this.chunkSize, endOffset - offset);
        ByteBuf buf = allocator.heapBuffer(chunkSize);
        boolean release = true;
        try {
            // 从文件读取一块数据到 ByteBuf
            file.readFully(buf.array(), buf.arrayOffset(), chunkSize);
            buf.writerIndex(chunkSize);
            this.offset = offset + chunkSize;
            release = false;
            return buf;
        } finally {
            if (release) buf.release();
        }
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return !(offset < endOffset && file.getChannel().isOpen());
    }
}
```

**为什么用 `heapBuffer` 而不是 `directBuffer`**：文件数据先读到堆内 ByteBuf，再由 Netty 的 Socket 写入逻辑自动转换为 Direct Buffer。对于文件传输场景，如果 OS 支持 `sendfile` 零拷贝，应该用 `FileRegion` 而不是 `ChunkedFile`。`ChunkedFile` 适合需要加密或修改文件内容的场景（如 SSL + 文件传输）。

### 5.3 使用示例

```java
// 配置水位线
bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
    new WriteBufferWaterMark(32 * 1024, 64 * 1024));

// Pipeline 中添加 ChunkedWriteHandler
pipeline.addLast(new ChunkedWriteHandler());
pipeline.addLast(new MyFileServerHandler());

// 业务 Handler 中发送大文件
class MyFileServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // 收到文件请求
        File file = new File("/data/large_file.bin");
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        ChunkedFile chunkedFile = new ChunkedFile(raf, 0, raf.length(), 8192);

        // 写入 ChunkedInput，ChunkedWriteHandler 会自动分块发送
        ctx.writeAndFlush(chunkedFile).addListener(future -> {
            if (future.isSuccess()) {
                log.info("文件发送完成");
            } else {
                log.error("文件发送失败", future.cause());
            }
        });
    }
}
```

### 5.4 坑点警告

**坑 1**：`ChunkedWriteHandler` 和 `SslHandler` 一起使用时，`ChunkedWriteHandler` 必须放在 `SslHandler` 之后（出站方向更靠近 Head），否则加密后的数据会被分块，可能导致 SSL 记录被截断。

**坑 2**：`resumeTransfer()` 的使用。有些 `ChunkedInput` 实现（如流式数据源）会在 `readChunk()` 返回 `null` 但 `isEndOfInput()` 返回 `false`，表示暂时没有数据。当新数据到来时需要手动调用 `resumeTransfer()` 恢复传输。

**坑 3**：默认 `chunkSize` 是 8KB（`ChunkedStream.DEFAULT_CHUNK_SIZE`）。太小会导致频繁系统调用，太大会占用过多内存。生产环境建议根据文件大小和内存情况调整为 64KB~256KB。

---

## 六、LoggingHandler —— 调试利器

### 6.1 解决什么问题

开发阶段需要看到 Pipeline 中事件的流转过程：哪个事件先触发、ByteBuf 里到底是什么字节、出站数据是什么格式。手动在每个 Handler 里加日志既繁琐又容易遗漏。`LoggingHandler` 作为一个全量拦截器，记录所有入站和出站事件，是调试 Pipeline 编排问题的第一工具。

### 6.2 核心源码解析

```java
// LoggingHandler.java
@Sharable
public class LoggingHandler extends ChannelDuplexHandler {

    private static final LogLevel DEFAULT_LEVEL = LogLevel.DEBUG;

    protected final InternalLogger logger;
    protected final InternalLogLevel internalLevel;

    private final LogLevel level;
    private final ByteBufFormat byteBufFormat;

    // 拦截所有入站事件
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            logger.log(internalLevel, format(ctx, "REGISTERED"));
        }
        ctx.fireChannelRegistered();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            logger.log(internalLevel, format(ctx, "ACTIVE"));
        }
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            logger.log(internalLevel, format(ctx, "READ", msg));
        }
        ctx.fireChannelRead(msg);
    }

    // 拦截所有出站事件
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            logger.log(internalLevel, format(ctx, "WRITE", msg));
        }
        ctx.write(msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            logger.log(internalLevel, format(ctx, "FLUSH"));
        }
        ctx.flush();
    }

    // ByteBuf 的 HEX_DUMP 格式化
    private String formatByteBuf(ChannelHandlerContext ctx, String eventName, ByteBuf msg) {
        String chStr = ctx.channel().toString();
        int length = msg.readableBytes();
        if (length == 0) {
            return chStr + ' ' + eventName + ": 0B";
        } else {
            StringBuilder buf = new StringBuilder(...);
            buf.append(chStr).append(' ').append(eventName).append(": ").append(length).append('B');
            if (byteBufFormat == ByteBufFormat.HEX_DUMP) {
                buf.append(NEWLINE);
                appendPrettyHexDump(buf, msg);  // 十六进制 + ASCII 格式化
            }
            return buf.toString();
        }
    }
}
```

**`@Sharable` 的含义**：`LoggingHandler` 没有实例状态（logger 和 level 在构造时确定且不可变），可以被多个 Pipeline 共享。非 `@Sharable` 的 Handler 如果被添加到多个 Pipeline 会抛异常。

**`logger.isEnabled(internalLevel)` 的作用**：先检查日志级别是否开启，避免在日志关闭时仍然执行字符串拼接（`format()` 方法涉及 `StringBuilder` 和 `HexDump`，开销不小）。

**HEX_DUMP 输出格式**：

```
[id: 0x12345678, L:/127.0.0.1:8080 - R:/127.0.0.1:54321] READ: 32B
         +-------------------------------------------------+
         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |
+--------+-------------------------------------------------+----------------+
|00000000| 47 45 54 20 2f 69 6e 64 65 78 20 48 54 54 50 2f |GET /index HTTP/|
|00000010| 31 2e 31 0d 0a 48 6f 73 74 3a 20 6c 6f 63 61 6c |1.1..Host: local|
|00000020| 68 6f 73 74 0d 0a                                |host..          |
+--------+-------------------------------------------------+----------------+
```

### 6.3 使用示例

```java
// 放在 Pipeline 最前面，能看到所有原始字节
pipeline.addLast(new LoggingHandler(LogLevel.INFO));

// 或者只看特定阶段的日志
pipeline.addLast(new HttpServerCodec());
pipeline.addLast(new LoggingHandler(LogLevel.DEBUG));  // 看解码后的 HTTP 对象
pipeline.addLast(new MyBusinessHandler());
```

### 6.4 坑点警告

**坑 1**：生产环境必须移除或降级 `LoggingHandler`。`HEX_DUMP` 会对每个 ByteBuf 做十六进制格式化，在高 QPS 场景下会产生大量日志和 CPU 开销。

**坑 2**：`LoggingHandler` 放在不同位置看到的日志内容不同。放在最前面（Head 附近）看到的是原始字节流；放在解码器后面看到的是解码后的业务对象。根据调试目的选择位置。

---

## 七、SslHandler —— SSL/TLS 加密

### 7.1 解决什么问题

在不可信的网络（如互联网）上传输数据需要加密。SSL/TLS 协议在 TCP 之上提供加密、认证和完整性保护。`SslHandler` 将 SSL/TLS 协议的实现集成到 Netty Pipeline 中，对上层 Handler 透明：上层看到的是明文 ByteBuf，SslHandler 在底层自动完成加密/解密。

### 7.2 核心源码解析

#### 7.2.1 类定义与继承关系

```java
// SslHandler.java
public class SslHandler extends ByteToMessageDecoder implements ChannelOutboundHandler {
    // ...
}
```

**它是什么**：`SslHandler` 同时继承 `ByteToMessageDecoder`（入站解密）和实现 `ChannelOutboundHandler`（出站加密）。入站时，加密的 SSL 记录进入 `ByteToMessageDecoder` 的 `decode()` 方法，被解密为明文；出站时，明文数据经过 `write()` 方法被加密为 SSL 记录。

**为什么继承 ByteToMessageDecoder**：SSL 数据也是流式的，一条 SSL 记录可能跨多个 TCP 包，需要累积。`ByteToMessageDecoder` 的累积器机制正好满足这个需求。

#### 7.2.2 SslEngineType —— 三引擎适配

```java
// SslHandler.java
private enum SslEngineType {
    TCNATIVE(true, COMPOSITE_CUMULATOR) {
        // 基于 OpenSSL 的 native 引擎，性能最高
        // 支持 CompositeByteBuf 零拷贝 unwrap
    },
    CONSCRYPT(true, COMPOSITE_CUMULATOR) {
        // Google 的 Conscrypt 引擎，基于 BoringSSL
    },
    JDK(false, MERGE_CUMULATOR) {
        // JDK 自带的 SSLEngine，性能最低但兼容性最好
        // 只能操作单个 ByteBuffer，必须用 MERGE_CUMULATOR
    };

    static SslEngineType forEngine(SSLEngine engine) {
        return engine instanceof ReferenceCountedOpenSslEngine ? TCNATIVE :
               engine instanceof ConscryptAlpnSslEngine ? CONSCRYPT : JDK;
    }
}
```

**三种引擎的差异**：

| 引擎 | 缓冲区类型 | Cumulator | 性能 | 依赖 |
|------|-----------|-----------|------|------|
| TCNATIVE (OpenSSL) | Direct | COMPOSITE | 最高 | netty-tcnative |
| CONSCRYPT (BoringSSL) | Direct | COMPOSITE | 高 | conscrypt |
| JDK | Heap | MERGE | 最低 | JDK 自带 |

**为什么 JDK 引擎用 Heap Buffer**：JDK 的 `SSLEngine` 内部在 `byte[]` 上操作，如果传入 Direct Buffer 会多做一次拷贝。而 OpenSSL 的 native 引擎直接在 Direct Buffer 上操作，零拷贝。

#### 7.2.3 握手流程

```java
// SslHandler.java

@Override
public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    this.ctx = ctx;
    // 初始化待加密写队列
    pendingUnencryptedWrites = new SslHandlerCoalescingBufferQueue(channel, 16, engineType.wantsDirectBuffer) { ... };

    boolean active = channel.isActive();
    if (active || fastOpen) {
        startHandshakeProcessing(active);
    }
}

@Override
public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    if (!startTls) {
        startHandshakeProcessing(true);
    }
    ctx.fireChannelActive();
}

private void startHandshakeProcessing(boolean flushAtEnd) {
    if (!isStateSet(STATE_HANDSHAKE_STARTED)) {
        setState(STATE_HANDSHAKE_STARTED);
        if (engine.getUseClientMode()) {
            // 客户端模式：主动发起握手
            handshake(flushAtEnd);
        }
        // 服务端模式：等客户端的 ClientHello 到来时被动响应
        applyHandshakeTimeout();
    } else if (isStateSet(STATE_NEEDS_FLUSH)) {
        forceFlush(ctx);
    }
}

private void handshake(boolean flushAtEnd) {
    if (engine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
        return;  // 已经在握手中了
    }
    if (handshakePromise.isDone()) {
        return;  // 握手已完成
    }

    final ChannelHandlerContext ctx = this.ctx;
    try {
        // 触发 SSLEngine 开始握手
        engine.beginHandshake();
        // 产生握手数据（如 ClientHello）并写入出站
        wrapNonAppData(ctx, false);
    } catch (Throwable e) {
        setHandshakeFailure(ctx, e);
    }
}
```

**握手触发时机**：

- **客户端模式**：`channelActive` → `startHandshakeProcessing` → `handshake()` → `engine.beginHandshake()` → `wrapNonAppData()` 发送 `ClientHello`
- **服务端模式**：`channelActive` → `startHandshakeProcessing` → 只设置超时，等数据到来时在 `decode()` → `unwrap()` 中被动响应

#### 7.2.4 wrapNonAppData() —— 握手数据包装

```java
// SslHandler.java
private boolean wrapNonAppData(final ChannelHandlerContext ctx, boolean inUnwrap) throws SSLException {
    ByteBuf out = null;
    ByteBufAllocator alloc = ctx.alloc();
    try {
        outer: while (!ctx.isRemoved()) {
            if (out == null) {
                out = allocateOutNetBuf(ctx, 2048, 1);
            }
            // 用空数据调用 wrap，产生握手数据
            SSLEngineResult result = wrap(alloc, engine, Unpooled.EMPTY_BUFFER, out);
            if (result.bytesProduced() > 0) {
                // 写入握手数据到出站
                ctx.write(out).addListener(future -> {
                    Throwable cause = future.cause();
                    if (cause != null) {
                        setHandshakeFailureTransportFailure(ctx, cause);
                    }
                });
                if (inUnwrap) {
                    setState(STATE_NEEDS_FLUSH);
                }
                out = null;
            }

            HandshakeStatus status = result.getHandshakeStatus();
            switch (status) {
                case FINISHED:
                    // 握手完成！
                    // ...
                    break;
                case NEED_TASK:
                    // 有委托任务需要执行（如证书验证）
                    // ...
                    break;
                case NEED_UNWRAP:
                    // 需要读取对端数据继续握手
                    if (inUnwrap || unwrapNonAppData(ctx) <= 0) {
                        return false;
                    }
                    break;
                case NEED_WRAP:
                    // 继续包装握手数据
                    break;
                case NOT_HANDSHAKING:
                    // 握手结束
                    if (setHandshakeSuccess() && inUnwrap && !pendingUnencryptedWrites.isEmpty()) {
                        wrap(ctx, true);
                    }
                    return true;
                default:
                    throw new IllegalStateException("Unknown handshake status: " + status);
            }

            if (result.bytesProduced() == 0 && status != HandshakeStatus.NEED_TASK) {
                break;
            }
        }
    } finally {
        if (out != null) {
            out.release();
        }
    }
    return false;
}
```

**握手状态机**：SSL 握手是一个多轮交互过程，`SSLEngine` 通过 `HandshakeStatus` 告诉调用者下一步应该做什么。`wrapNonAppData` 是一个循环，根据状态分支处理：

- `NEED_WRAP`：引擎需要产生握手数据，继续调用 `wrap()`
- `NEED_UNWRAP`：引擎需要读取对端数据，调用 `unwrapNonAppData()`
- `NEED_TASK`：引擎有委托任务（如证书验证的 `Runnable`），需要在 `delegatedTaskExecutor` 中执行
- `FINISHED`：握手完成，触发 `handshakePromise`
- `NOT_HANDSHAKING`：非握手状态，握手已结束

#### 7.2.5 applyHandshakeTimeout() —— 握手超时

```java
// SslHandler.java
private void applyHandshakeTimeout() {
    final Promise<Channel> localHandshakePromise = this.handshakePromise;

    final long handshakeTimeoutMillis = this.handshakeTimeoutMillis;
    if (handshakeTimeoutMillis <= 0 || localHandshakePromise.isDone()) {
        return;
    }

    // 默认 10 秒超时
    final Future<?> timeoutFuture = ctx.executor().schedule(() -> {
        if (localHandshakePromise.isDone()) {
            return;
        }
        SSLException exception =
            new SslHandshakeTimeoutException("handshake timed out after " + handshakeTimeoutMillis + "ms");
        try {
            if (localHandshakePromise.tryFailure(exception)) {
                SslUtils.handleHandshakeFailure(ctx, exception, true);
            }
        } finally {
            releaseAndFailAll(ctx, exception);
        }
    }, handshakeTimeoutMillis, TimeUnit.MILLISECONDS);

    // 握手成功时取消超时任务
    localHandshakePromise.addListener(future -> timeoutFuture.cancel(false));
}
```

**默认握手超时 10 秒**：如果 10 秒内握手未完成，`handshakePromise` 被 `tryFailure` 标记为失败，所有待发送的明文数据被释放，连接被关闭。

#### 7.2.6 decode() → unwrap() —— 入站数据解密

`SslHandler` 继承 `ByteToMessageDecoder`，入站的加密数据进入 `decode()` 方法，内部调用 `unwrap()` 进行解密：

```java
// SslHandler.java（简化）
private int unwrap(ChannelHandlerContext ctx, ByteBuf packet, int length) throws SSLException {
    ByteBuf decodeOut = allocate(ctx, length);
    try {
        do {
            // 调用 SSLEngine.unwrap() 解密
            final SSLEngineResult result = engineType.unwrap(this, packet, length, decodeOut);
            final Status status = result.getStatus();
            final HandshakeStatus handshakeStatus = result.getHandshakeStatus();

            // 根据状态处理
            switch (status) {
                case OK:
                    // 解密成功，明文在 decodeOut 中
                    break;
                case BUFFER_OVERFLOW:
                    // 输出缓冲区不够大，扩容后重试
                    break;
                case BUFFER_UNDERFLOW:
                    // 输入数据不够，需要更多数据
                    break;
                case CLOSED:
                    // SSL 连接关闭
                    break;
            }

            // 如果在握手过程中，继续处理握手状态
            if (handshakeStatus == HandshakeStatus.NEED_WRAP) {
                wrapNonAppData(ctx, true);
            }
        } while (...);

        // 将解密后的明文传播给下游
        if (decodeOut.isReadable()) {
            ctx.fireChannelRead(decodeOut);
        }
    } finally {
        decodeOut.release();
    }
}
```

### 7.3 Pipeline 中的位置

```
Pipeline（入站方向）：
  Head → SslHandler → FrameDecoder → BusinessDecoder → BusinessHandler

Pipeline（出站方向）：
  BusinessHandler → BusinessEncoder → SslHandler → Head → Socket
```

**SslHandler 必须放在最前面（入站方向最靠近 Head）**：因为入站数据是加密的，必须先经过 SslHandler 解密，后续的 FrameDecoder 才能正确解析明文帧。同样，出站方向 SslHandler 必须在最后（最靠近 Head），确保所有业务编码完成后再加密。

### 7.4 使用示例

```java
// 服务端 SSL 配置
SslContext sslCtx = SslContextBuilder
    .forServer(certFile, keyFile)
    .sslProvider(SslProvider.OPENSSL)  // 使用 OpenSSL，性能最高
    .build();

pipeline.addLast(sslCtx.newHandler(ch.alloc()));  // 必须放第一个
pipeline.addLast(new LengthFieldBasedFrameDecoder(...));
pipeline.addLast(new MyBusinessHandler());

// 监听握手完成
sslHandler.handshakeFuture().addListener(future -> {
    if (future.isSuccess()) {
        log.info("SSL 握手完成");
    } else {
        log.error("SSL 握手失败", future.cause());
    }
});
```

### 7.5 坑点警告

**坑 1**：`SslHandler` 和 `FileRegion`（零拷贝）不兼容。`FileRegion` 使用 `sendfile` 系统调用直接从文件描述符传输到 Socket，数据不经过用户空间。但 SSL 加密必须在用户空间完成，所以如果 Pipeline 中有 `SslHandler`，`FileRegion` 会退化为普通写入。大文件传输场景应该用 `ChunkedFile` + `ChunkedWriteHandler`。

**坑 2**：握手超时默认 10 秒，在网络延迟高的环境（如跨地域）可能不够。通过 `sslHandler.setHandshakeTimeout(30, TimeUnit.SECONDS)` 调整。

**坑 3**：每个 `SslHandler` 实例绑定一个 `SSLEngine`，不可复用。连接关闭后必须移除旧的 `SslHandler`，创建新的。

---

## 八、生产级 Server 端 Pipeline 编排最佳实践

### 8.1 完整编排示例

```java
public class ProductionServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslContext;
    private final Timer timeoutTimer;  // 共享的 HashedWheelTimer

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        // ① SSL/TLS 解密（如果有）
        if (sslContext != null) {
            SslHandler sslHandler = sslContext.newHandler(ch.alloc());
            sslHandler.setHandshakeTimeout(10, TimeUnit.SECONDS);
            pipeline.addLast("ssl", sslHandler);
        }

        // ② 空闲检测
        // Reader 90 秒 → 关连接，Writer 30 秒 → 发心跳
        pipeline.addLast("idleStateHandler",
            new IdleStateHandler(90, 30, 0, TimeUnit.SECONDS));

        // ③ 连接超时控制（可选，用于限流或安全）
        // pipeline.addLast("readTimeoutHandler", new ReadTimeoutHandler(120));

        // ④ 拆包：长度字段解码器
        pipeline.addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(
                1024 * 1024,  // maxFrameLength = 1MB
                0,             // lengthFieldOffset = 0
                4,             // lengthFieldLength = 4 字节
                0,             // lengthAdjustment = 0
                4              // initialBytesToStrip = 跳过长度字段
            ));

        // ⑤ 业务解码器：字节 → 请求对象
        pipeline.addLast("protocolDecoder", new MyProtocolDecoder());

        // ⑥ 流量控制（如果需要精确控制读节奏）
        // pipeline.addLast("flowControl", new FlowControlHandler());

        // ⑦ 调试日志（仅开发环境）
        // pipeline.addLast("logging", new LoggingHandler(LogLevel.DEBUG));

        // ⑧ 业务处理器
        pipeline.addLast("heartbeatHandler", new HeartbeatServerHandler());
        pipeline.addLast("businessHandler", new BusinessServerHandler());

        // ⑨ 大文件分块写入（如果支持文件传输）
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());

        // ⑩ 业务编码器：响应对象 → 字节
        pipeline.addLast("protocolEncoder", new MyProtocolEncoder());

        // 注意：SslHandler 在出站方向自动加密，不需要再添加
    }
}
```

### 8.2 编排顺序及原因

| 顺序 | Handler | 方向 | 为什么在这个位置 |
|------|---------|------|------------------|
| ① SslHandler | 双向 | 最靠近 Head，入站第一个处理 | 入站数据是加密的，必须先解密才能做后续处理；出站最后加密，确保所有数据编码完成 |
| ② IdleStateHandler | 双向 | 紧跟 SslHandler | 需要检测的是应用层读写空闲，应该在解密之后、业务处理之前；如果放在 SslHandler 前面，SSL 握手数据会干扰空闲判断 |
| ③ FrameDecoder | 入站 | 在 IdleStateHandler 之后 | 需要解密后的明文才能正确拆包；放在 IdleStateHandler 之后是因为 IdleStateHandler 透传 ByteBuf 不做修改 |
| ④ ProtocolDecoder | 入站 | 在 FrameDecoder 之后 | 接收拆好的完整帧，反序列化为业务对象 |
| ⑤ FlowControlHandler | 双向 | 在 Decoder 之后、业务之前 | 控制的是业务消息的投递节奏，必须在解码之后（才有业务消息可控制） |
| ⑥ BusinessHandler | 入站+出站 | Pipeline 中段 | 核心业务逻辑，接收解码后的对象，产出响应对象 |
| ⑦ ChunkedWriteHandler | 出站 | 在 BusinessHandler 之后 | 拦截 BusinessHandler 产出的 ChunkedInput 消息，分块写入 |
| ⑧ ProtocolEncoder | 出站 | 在 ChunkedWriteHandler 之后 | 将业务响应对象编码为字节，然后经过 SslHandler 加密 |
| ⑨ SslHandler（出站） | 出站 | 最靠近 Head | 加密所有出站数据，确保发送到网络的是密文 |

### 8.3 编排原则总结

原则一：**SSL 最内（最靠近 Head）**。加密/解密是数据进出网络的最后一道/第一道工序，必须在所有业务处理之前完成。

原则二：**空闲检测紧跟 SSL**。空闲检测需要跟踪应用层读写活动，应该在解密之后跟踪，否则 SSL 握手过程会产生额外的读写活动，干扰空闲判断。

原则三：**拆包在解码之前**。先拆出完整的帧（FrameDecoder），再反序列化为业务对象（ProtocolDecoder）。这是"先断句再理解"的自然顺序。

原则四：**流量控制在业务之前**。FlowControlHandler 控制的是业务消息的投递节奏，必须在解码之后（才有消息可控制）、业务之前（才能控制投递）。

原则五：**ChunkedWriteHandler 在编码之后**。它拦截的是 `ChunkedInput` 类型消息，需要业务 Handler 先产出这种消息。

原则六：**一个 Channel 一个 SslHandler 实例，一个 IdleStateHandler 实例**。这些 Handler 有状态（SSL 引擎、定时任务），不能跨 Channel 共享。而 `LoggingHandler` 标注了 `@Sharable`，可以共享。

---

## 本篇涉及的设计模式

本篇源码中体现了以下设计模式：

**模板方法模式（Template Method）**：`IdleStateHandler` 中的 `AbstractIdleTask` 定义了 `run()` 的骨架（检查 Channel 是否 open → 调用子类 `run(ctx)`），三个子类 `ReaderIdleTimeoutTask`、`WriterIdleTimeoutTask`、`AllIdleTimeoutTask` 各自实现空闲检测的具体逻辑。`SslHandler` 的 `SslEngineType` 枚举也是模板方法的变体——定义了 `unwrap()`、`allocateWrapBuffer()` 等抽象方法，三个具体实现（TCNATIVE、CONSCRYPT、JDK）各自适配不同的 SSL 引擎。

**策略模式（Strategy）**：`SslHandler` 的 `SslEngineType` 枚举是一组策略，根据 `SSLEngine` 的实际类型（OpenSsl / Conscrypt / JDK）选择不同的缓冲区分配策略、累积器类型和 unwrap 实现。`LoggingHandler` 的 `ByteBufFormat`（HEX_DUMP vs SIMPLE）也是策略模式，决定 ByteBuf 的格式化方式。

**观察者模式（Observer）**：`IdleStateHandler` 通过 `fireUserEventTriggered` 传播 `IdleStateEvent`，下游 Handler 通过 `userEventTriggered` 方法接收——这是经典的观察者模式。`SslHandler` 的 `handshakeFuture()` 返回一个 `Promise<Channel>`，业务代码通过 `addListener` 注册回调，握手完成时通知所有监听者。

**享元模式（Flyweight）/ 对象池模式（Object Pool）**：`IdleStateEvent` 的 6 个预定义常量是享元模式的体现——所有相同类型的空闲事件共享同一个对象，避免重复创建。`FlowControlHandler` 中的 `RecyclableArrayDeque` 使用 Netty 的 `Recycler` 对象池管理队列对象，用完后回收到池中供下次使用，减少 GC 压力。`HashedWheelTimeout` 节点自身充当双向链表节点（`next`/`prev`），不需要额外创建包装对象。

**状态模式（State）**：`HashedWheelTimer` 的 `workerState`（INIT / STARTED / SHUTDOWN）控制行为：INIT 状态可以 start，STARTED 状态可以 stop，SHUTDOWN 状态不可逆。`HashedWheelTimeout` 的 `state`（INIT / CANCELLED / EXPIRED）通过 CAS 状态机管理生命周期。`IdleStateHandler` 的 `state`（INITIALIZED / DESTROYED）防止重复初始化。

**适配器模式（Adapter）**：`SslHandler` 继承 `ByteToMessageDecoder` 同时实现 `ChannelOutboundHandler`，将 JDK 的 `SSLEngine`（面向 ByteBuffer 的同步 API）适配为 Netty 的 Pipeline 事件模型（面向 ByteBuf 的异步事件）。`SslEngineType` 枚举适配了三种底层 SSL 实现的差异，为 `SslHandler` 提供统一的接口。

**责任链模式（Chain of Responsibility）**：所有 Handler 在 Pipeline 中形成责任链，`IdleStateEvent` 通过 `fireUserEventTriggered` 沿链传播，直到被某个 Handler 处理。`SslHandler` 解密后的明文通过 `fireChannelRead` 传播给下游 FrameDecoder。

**建造者模式（Builder）**：`WriteBufferWaterMark` 封装了高/低水位线两个参数，作为一个整体配置传递给 `ChannelOption.WRITE_BUFFER_WATER_MARK`，是 Builder 模式的简化形式——将多个相关参数组合为一个不可变对象。

## 本篇涉及的高性能并发技术

本篇源码中使用了以下高性能并发和性能优化技术：

**MPSC 无锁队列（Multi-Producer Single-Consumer）**：`HashedWheelTimer` 使用两个 MPSC 队列（`timeouts` 和 `cancelledTimeouts`）实现多线程提交任务、单 Worker 线程消费的无锁模型。多生产者通过 CAS 入队，单消费者直接 poll 出队，无需任何锁。这比 `ConcurrentLinkedQueue` 更高效，因为后者需要处理多消费者场景的更复杂协调。

**CAS 无锁状态转换**：`HashedWheelTimeout` 的状态转换（INIT → CANCELLED / EXPIRED）使用 `AtomicIntegerFieldUpdater.compareAndSet`，无需 `synchronized`。`HashedWheelTimer` 的 `workerState` 转换（INIT → STARTED）也使用 CAS，保证 `start()` 方法在多线程环境下只启动一次 Worker 线程。`AtomicIntegerFieldUpdater` 相比 `AtomicInteger` 节省了 16 字节对象头，在百万级 Timeout 对象中差异可观。

**线程封闭（Thread Confinement）**：`HashedWheelTimer` 的轮盘数组（`wheel`）和链表操作只在 Worker 线程中执行，无需同步。`FlowControlHandler` 的 `queue` 和 `unsatisfiedReads` 只在 EventLoop 线程中操作（Handler 的线程亲和性保证）。`IdleStateHandler` 的 `lastReadTime`、`lastWriteTime` 等字段也只在 EventLoop 线程中修改。通过线程封闭避免了锁竞争，这是 Netty 并发设计的核心理念。

**延迟处理（Deferred Processing）**：`HashedWheelTimer` 的 `cancel()` 不直接从 bucket 链表中移除节点（这需要同步），而是放入 `cancelledTimeouts` MPSC 队列，由 Worker 线程在下一个 tick 统一处理。这种"标记 + 延迟清理"的模式将并发操作降为单线程操作，代价是最多一个 tick（100ms）的 GC 延迟。

**位运算优化**：`HashedWheelTimer` 将轮盘大小规范化为 2 的幂，使得取模运算 `tick % length` 可以用 `tick & mask` 替代。位运算比除法/取模快 3~5 倍，在每 tick 都要计算 bucket 索引的场景下收益显著。`IdleStateHandler` 的 `newIdleStateEvent` 中通过 `switch` 状态枚举选择预定义常量，避免创建新对象。

**对象复用**：`IdleStateHandler` 的 `writeListener` 是一个复用的 `ChannelFutureListener` 实例，避免了每次 write 操作都创建新的 lambda 对象。`IdleStateEvent` 的 6 个预定义常量是单例，所有空闲事件共享这些对象，零分配。`FlowControlHandler` 的 `RecyclableArrayDeque` 通过 `Recycler` 对象池管理，避免频繁创建 `ArrayDeque`。

**背压机制（Backpressure）**：`ChunkedWriteHandler` 通过 `channel.isWritable()` 检查 `WriteBufferWaterMark` 水位线，实现自动背压：写缓冲区满时暂停读取文件块，水位恢复时继续。这防止了生产者（文件读取）速度超过消费者（网络发送）导致 OOM。高低两个水位线形成滞后区间，避免在阈值附近频繁切换状态。

**批量处理**：`HashedWheelTimer` 的 `transferTimeoutsToBuckets()` 每个 tick 最多转移 100000 个任务，而不是一次只转一个。这种批量操作减少了 MPSC 队列的 poll 次数，同时设置了上限防止任务提交过快导致 Worker 线程饿死。`HashedWheelBucket.expireTimeouts()` 一次遍历整个链表，批量过期所有到期任务。

**精度换效率的权衡**：`HashedWheelTimer` 的核心设计是用时间精度（默认 100ms 误差）换取 O(1) 的任务添加复杂度。对于 IO 超时检测这类不需要毫秒级精度的场景，这个权衡是非常划算的——10000 个连接的超时管理，用时间轮只需要 1 个线程，而用 `ScheduledThreadPoolExecutor` 需要维护一个 10000 元素的堆，每次插入 O(log 10000) ≈ 13 次比较。

**懒启动（Lazy Initialization）**：`HashedWheelTimer` 的 Worker 线程在第一次 `newTimeout()` 调用时才启动（`start()` 方法），而不是在构造时就启动。如果创建了时间轮但最终没有使用，不会浪费线程资源。`IdleStateHandler` 的定时任务也在 `channelActive` 或 `handlerAdded` 时才注册，而不是构造时。

**共享资源检测**：`HashedWheelTimer` 通过 `INSTANCE_COUNTER` 全局原子计数器追踪实例数，超过 64 个时发出警告。这是一种防御性编程手段，防止开发者误用（为每个连接创建时间轮导致线程爆炸）。同时使用 `ResourceLeakDetector` 检测时间轮实例是否被正确 `stop()`，防止线程泄漏。
