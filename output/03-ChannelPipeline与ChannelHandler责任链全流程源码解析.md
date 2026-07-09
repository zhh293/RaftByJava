# ChannelPipeline 与 ChannelHandler 责任链全流程源码解析

> **Netty 源码深度研究系列 · 第 03 篇**
>
> 基于 Netty 主分支源码，完整解析 ChannelPipeline 的双向链表结构和事件传播机制。Pipeline 就像一条流水线，入站消息从左到右经过每道工序（解码→业务处理→日志），出站消息从右到左（业务结果→编码→发送）。每道工序可以独立开发、自由组合。

---

## 一、完整调用链总览

一个入站消息从 Socket 读取到 Handler 处理的全链路：

```
网卡收到数据 → 内核通知 epoll → Selector 唤醒
    │
    ▼
NioByteUnsafe.read()
    ├─ allocHandle.allocate(allocator)          → 分配 ByteBuf
    ├─ doReadBytes(byteBuf)                     → JDK SocketChannel.read()
    └─ pipeline.fireChannelRead(byteBuf)        → 触发入站事件
         │
         ▼  入站方向：head ──────────────────▶ tail
    ┌─────────┐    ┌──────────┐    ┌───────────────┐    ┌─────────┐
    │ HeadCtx  │ ─▶ │ Decoder   │ ─▶ │ BusinessHandler│ ─▶ │ TailCtx  │
    │ 转发      │    │ 字节→对象  │    │ 业务处理       │    │ 兜底释放  │
    └─────────┘    └──────────┘    └───────┬───────┘    └─────────┘
                                          │
                                    ctx.writeAndFlush(response)
                                          │
         ▼  出站方向：head ◀──────────────────── 当前 ctx
    ┌─────────┐    ┌──────────┐
    │ HeadCtx  │ ◀─ │ Encoder   │ ◀─── 出站起点（ctx.write 从当前位置开始）
    │ Unsafe写入│    │ 对象→字节  │
    └────┬────┘    └──────────┘
         │
    unsafe.write() → ChannelOutboundBuffer
    unsafe.flush() → JDK SocketChannel.write()
         │
    数据发出
```

---

## 二、Pipeline 的数据结构

### 2.1 DefaultChannelPipeline 的双向链表

```java
// DefaultChannelPipeline.java
public class DefaultChannelPipeline implements ChannelPipeline {

    final HeadContext head;   // 头哨兵节点
    final TailContext tail;   // 尾哨兵节点

    private final Channel channel;

    protected DefaultChannelPipeline(Channel channel) {
        this.channel = ObjectUtil.checkNotNull(channel, "channel");
        succeededFuture = new SucceededChannelFuture(channel, null);
        voidPromise = new VoidChannelPromise(channel, true);

        tail = new TailContext(this);
        head = new HeadContext(this);

        // 初始化空链表：head ↔ tail
        head.next = tail;
        tail.prev = head;
    }
}
```

### 2.2 ChannelHandlerContext 的双向链表结构

每个 `AbstractChannelHandlerContext` 节点包含 prev/next 指针，构成双向链表：

```java
// AbstractChannelHandlerContext.java
abstract class AbstractChannelHandlerContext implements ChannelHandlerContext, ResourceLeakHint {

    volatile AbstractChannelHandlerContext next;  // 下一个节点（toward tail）
    volatile AbstractChannelHandlerContext prev;  // 上一个节点（toward head）

    private final DefaultChannelPipeline pipeline;
    private final String name;
    private final int executionMask;  // 位掩码，标记该 Handler 处理哪些事件

    // Handler 状态机
    private static final int INIT = 0;
    private static final int ADD_PENDING = 1;
    private static final int ADD_COMPLETE = 2;
    private static final int REMOVE_COMPLETE = 3;
    private volatile int handlerState = INIT;
}
```

### 2.3 Handler 与 HandlerContext 的一对一关系

```java
// DefaultChannelHandlerContext.java
final class DefaultChannelHandlerContext extends AbstractChannelHandlerContext {
    private final ChannelHandler handler;  // 一对一持有 Handler 引用

    DefaultChannelHandlerContext(
            DefaultChannelPipeline pipeline, EventExecutor executor,
            String name, ChannelHandler handler) {
        super(pipeline, executor, name, handler.getClass());
        this.handler = handler;
    }

    @Override
    public ChannelHandler handler() {
        return handler;
    }
}
```

HeadContext 和 TailContext 比较特殊——它们本身既是 Context 也是 Handler：

```java
// HeadContext
public ChannelHandler handler() { return this; }  // Context 自身就是 Handler

// TailContext
public ChannelHandler handler() { return this; }
```

**Pipeline 结构示意图**：

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                     DefaultChannelPipeline                      │
  │                                                                  │
  │  ┌──────┐ next ┌──────┐ next ┌──────┐ next ┌──────┐ next ┌────┐ │
  │  │ Head │────▶│ Ctx1 │────▶│ Ctx2 │────▶│ Ctx3 │────▶│Tail│ │
  │  │Context│◀────│      │◀────│      │◀────│      │◀────│Ctx │ │
  │  └──┬───┘ prev └──┬───┘ prev└──┬───┘ prev└──┬───┘ prev└──┬─┘ │
  │     │             │            │            │            │    │
  │  HeadCtx      Handler1     Handler2     Handler3     TailCtx  │
  │  (IO操作)     (解码器)      (业务)       (编码器)     (兜底)   │
  └──────────────────────────────────────────────────────────────────┘
```

---

## 三、Handler 的添加过程

### 3.1 addLast 源码

所有的 addFirst/addLast/addBefore/addAfter 都路由到统一的 `internalAdd` 方法：

```java
// DefaultChannelPipeline.java
private ChannelPipeline internalAdd(EventExecutorGroup group, String name,
                                    ChannelHandler handler, String baseName,
                                    AddStrategy addStrategy) {
    final AbstractChannelHandlerContext newCtx;
    synchronized (this) {                             // ① 全局锁保证线程安全
        checkMultiplicity(handler);                    // ② 检查 @Sharable
        name = filterName(name, handler);              // ③ 生成/校验名称

        newCtx = newContext(group, name, handler);     // ④ 创建 Context

        switch (addStrategy) {                         // ⑤ 修改链表指针
            case ADD_FIRST:  addFirst0(newCtx); break;
            case ADD_LAST:   addLast0(newCtx); break;
            case ADD_BEFORE: addBefore0(getContextOrDie(baseName), newCtx); break;
            case ADD_AFTER:  addAfter0(getContextOrDie(baseName), newCtx); break;
        }

        if (!registered) {
            // ⑥ Channel 还没注册，延迟 handlerAdded 回调
            newCtx.setAddPending();
            callHandlerCallbackLater(newCtx, true);
            return this;
        }

        EventExecutor executor = newCtx.executor();
        if (!executor.inEventLoop()) {
            // ⑦ 不在 EventLoop 线程，异步执行 handlerAdded
            callHandlerAddedInEventLoop(newCtx, executor);
            return this;
        }
    }
    // ⑧ 在 EventLoop 线程内，直接调用 handlerAdded（锁外执行）
    callHandlerAdded0(newCtx);
    return this;
}
```

链表尾部插入的实现非常简洁：

```java
// DefaultChannelPipeline.java
private void addLast0(AbstractChannelHandlerContext newCtx) {
    AbstractChannelHandlerContext prev = tail.prev;
    newCtx.prev = prev;
    newCtx.next = tail;
    prev.next = newCtx;
    tail.prev = newCtx;
}
```

### 3.2 checkMultiplicity — 防止非 @Sharable Handler 重复添加

```java
// DefaultChannelPipeline.java
private static void checkMultiplicity(ChannelHandler handler) {
    if (handler instanceof ChannelHandlerAdapter) {
        ChannelHandlerAdapter h = (ChannelHandlerAdapter) handler;
        if (!h.isSharable() && h.added) {
            throw new ChannelPipelineException(
                h.getClass().getName() +
                " is not a @Sharable handler, so can't be added or removed multiple times.");
        }
        h.added = true;
    }
}
```

没有标注 `@Sharable` 的 Handler 可能包含成员变量状态，不能被多个 Channel 的 Pipeline 共用。这个检查在添加时就拦截，避免运行时出现诡异的状态共享 bug。

### 3.3 线程安全：如何在 EventLoop 线程外安全添加 Handler

`internalAdd` 使用 `synchronized(this)` 保护链表指针的修改。但 `handlerAdded()` 回调在锁外执行——这是为了避免用户在 `handlerAdded()` 中调用其他 Pipeline 操作（如 addLast）时产生死锁。

当 Channel 尚未注册到 EventLoop 时，`handlerAdded()` 的执行被延迟到 `register0()` 中：

```java
// DefaultChannelPipeline.java
private void callHandlerAddedForAllHandlers() {
    final PendingHandlerCallback pendingHandlerCallbackHead;
    synchronized (this) {
        assert !registered;
        registered = true;
        pendingHandlerCallbackHead = this.pendingHandlerCallbackHead;
        this.pendingHandlerCallbackHead = null;
    }
    // 在 synchronized 块外执行所有待处理的回调
    PendingHandlerCallback task = pendingHandlerCallbackHead;
    while (task != null) {
        task.execute();
        task = task.next;
    }
}
```

### 3.4 ChannelInitializer 的工作原理（一次性 Handler）

`ChannelInitializer` 是 `@Sharable` 的特殊 Handler，用于在 Channel 注册时初始化 Pipeline：

```java
// ChannelInitializer.java
@Sharable
public abstract class ChannelInitializer<C extends Channel> extends ChannelInboundHandlerAdapter {

    private final Set<ChannelHandlerContext> initMap = ConcurrentHashMap.newKeySet();

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isRegistered()) {
            // Channel 已注册时，立即执行初始化
            if (initChannel(ctx)) {
                removeState(ctx);
            }
        }
    }

    private boolean initChannel(ChannelHandlerContext ctx) throws Exception {
        if (initMap.add(ctx)) {  // 防止重入（用 ConcurrentHashMap.newKeySet()）
            try {
                initChannel((C) ctx.channel());  // ★ 用户实现的抽象方法
            } catch (Throwable cause) {
                exceptionCaught(ctx, cause);
            } finally {
                if (!ctx.isRemoved()) {
                    ctx.pipeline().remove(this);  // ★ 初始化完毕，将自己从 Pipeline 移除
                }
            }
            return true;
        }
        return false;
    }

    // 用户需要实现这个方法
    protected abstract void initChannel(C ch) throws Exception;
}
```

**工作流程**：

1. 用户在 Bootstrap 中配置 `ChannelInitializer`
2. Channel 创建后，`ChannelInitializer` 被加入 Pipeline
3. Channel 注册到 EventLoop 时，触发 `handlerAdded()`
4. `initChannel()` 被调用，用户在其中添加真正的业务 Handler
5. 初始化完成后，`ChannelInitializer` 将自己从 Pipeline 中移除

**为什么要自己移除**：`ChannelInitializer` 的使命是"初始化 Pipeline"，这是一次性工作。如果不移除，它会一直占据链表中的一个位置，虽然没有实际功能（所有方法都被 `@Skip`），但增加了链表长度。

---

## 四、事件传播机制

### 4.1 入站事件传播（head → tail）

#### Pipeline 入口

```java
// DefaultChannelPipeline.java
@Override
public final ChannelPipeline fireChannelRead(Object msg) {
    if (head.executor().inEventLoop()) {
        if (head.invokeHandler()) {
            head.channelRead(head, msg);   // 直接调用 HeadContext
        } else {
            head.fireChannelRead(msg);     // 跳过 HeadContext
        }
    } else {
        head.executor().execute(() -> fireChannelRead(msg));  // 异步提交
    }
    return this;
}
```

#### HeadContext 的 channelRead — 转发给下一个

```java
// HeadContext
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ctx.fireChannelRead(msg);  // 简单转发，不做任何处理
}
```

#### ctx.fireChannelRead() 的核心实现

```java
// AbstractChannelHandlerContext.java
@Override
public ChannelHandlerContext fireChannelRead(final Object msg) {
    // ★ 关键：找到下一个处理 channelRead 的入站 Handler
    AbstractChannelHandlerContext next = findContextInbound(MASK_CHANNEL_READ);

    if (next.executor().inEventLoop()) {
        final Object m = pipeline.touch(msg, next);  // 内存泄漏检测
        if (next.invokeHandler()) {
            try {
                final ChannelHandler handler = next.handler();
                final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
                if (handler == headContext) {
                    headContext.channelRead(next, m);
                } else if (handler instanceof ChannelDuplexHandler) {
                    ((ChannelDuplexHandler) handler).channelRead(next, m);
                } else {
                    ((ChannelInboundHandler) handler).channelRead(next, m);
                }
            } catch (Throwable t) {
                next.invokeExceptionCaught(t);  // 异常转入异常传播链
            }
        } else {
            next.fireChannelRead(m);  // Handler 未就绪，跳过
        }
    } else {
        // 跨 Executor 线程，异步提交
        next.executor().execute(() -> fireChannelRead(msg));
    }
    return this;
}
```

#### findContextInbound — 向 tail 方向查找

```java
// AbstractChannelHandlerContext.java
private AbstractChannelHandlerContext findContextInbound(int mask) {
    AbstractChannelHandlerContext ctx = this;
    EventExecutor currentExecutor = executor();
    do {
        ctx = ctx.next;  // 向 tail 方向移动
    } while (skipContext(ctx, currentExecutor, mask, MASK_ONLY_INBOUND));
    return ctx;
}
```

#### skipContext — 跳过逻辑

```java
// AbstractChannelHandlerContext.java
private static boolean skipContext(
        AbstractChannelHandlerContext ctx, EventExecutor currentExecutor,
        int mask, int onlyMask) {
    return (ctx.executionMask & (onlyMask | mask)) == 0 ||
           (ctx.executor() == currentExecutor && (ctx.executionMask & mask) == 0);
}
```

跳过的两种情况：

1. **类型不匹配**：该 Context 既不是入站类型（`onlyMask`），也不处理当前事件（`mask`）
2. **方法被 @Skip**：executor 相同时，该事件对应的方法被标记为 `@Skip`（即用户没有覆写 Adapter 的默认实现）

#### @Skip 优化的精妙设计

`ChannelInboundHandlerAdapter` 的所有方法都标注了 `@Skip`：

```java
// ChannelInboundHandlerAdapter.java
@Skip
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ctx.fireChannelRead(msg);  // 默认实现：直接转发
}
```

Netty 通过 `ChannelHandlerMask.mask()` 在 Handler 首次添加到 Pipeline 时计算 `executionMask`：

```java
// ChannelHandlerMask.java
private static int mask0(Class<? extends ChannelHandler> handlerType) {
    int mask = MASK_EXCEPTION_CAUGHT;
    if (ChannelInboundHandler.class.isAssignableFrom(handlerType)) {
        mask |= MASK_ALL_INBOUND;
        // 如果 channelRead 方法标注了 @Skip，清除对应位
        if (isSkippable(handlerType, "channelRead",
                ChannelHandlerContext.class, Object.class)) {
            mask &= ~MASK_CHANNEL_READ;
        }
        // ... 其他入站方法同理
    }
    // ... 出站方法同理
    return mask;
}
```

当用户继承 Adapter **覆写**了 `channelRead()` 时，子类方法没有 `@Skip`（注解不继承），`executionMask` 的 `MASK_CHANNEL_READ` 位被保留，事件会传播到该 Handler。如果没有覆写，该位被清除，`skipContext()` 返回 true，事件直接跳过这个 Handler，减少不必要的方法调用。

### 4.2 出站事件传播（tail → head）

#### Pipeline 出站入口 — 从 tail 开始

```java
// DefaultChannelPipeline.java
@Override
public final ChannelFuture write(Object msg) {
    return tail.write(msg);  // ★ 从 tail 开始
}

@Override
public final ChannelFuture write(Object msg, ChannelPromise promise) {
    return tail.write(msg, promise);
}

@Override
public final ChannelPipeline flush() {
    tail.flush();
    return this;
}
```

#### ctx.write() 的核心实现

```java
// AbstractChannelHandlerContext.java
void write(Object msg, boolean flush, ChannelPromise promise) {
    if (validateWrite(msg, promise)) {
        // ★ 关键：向 head 方向查找下一个出站 Handler
        final AbstractChannelHandlerContext next = findContextOutbound(
            flush ? MASK_WRITE | MASK_FLUSH : MASK_WRITE);

        final Object m = pipeline.touch(msg, next);
        EventExecutor executor = next.executor();

        if (executor.inEventLoop()) {
            if (next.invokeHandler()) {
                promise = ensurePromiseUseCorrectExecutor(promise);
                try {
                    final ChannelHandler handler = next.handler();
                    final DefaultChannelPipeline.HeadContext headContext = pipeline.head;
                    if (handler == headContext) {
                        headContext.write(next, msg, promise);
                    } else if (handler instanceof ChannelDuplexHandler) {
                        ((ChannelDuplexHandler) handler).write(next, msg, promise);
                    } else {
                        ((ChannelOutboundHandler) handler).write(next, msg, promise);
                    }
                } catch (Throwable t) {
                    notifyOutboundHandlerException(t, promise);  // 出站异常→通知Promise
                }
                if (flush) {
                    // ... 调用 handler.flush()
                }
            } else {
                next.write(msg, flush, promise);  // Handler 未就绪，跳过
            }
        } else {
            // 跨线程：封装为 WriteTask 异步提交
            final WriteTask task = WriteTask.newInstance(this, m, promise, flush);
            if (!safeExecute(executor, task, promise, m, !flush)) {
                task.cancel();
            }
        }
    }
}
```

#### findContextOutbound — 向 head 方向查找

```java
// AbstractChannelHandlerContext.java
private AbstractChannelHandlerContext findContextOutbound(int mask) {
    AbstractChannelHandlerContext ctx = this;
    EventExecutor currentExecutor = executor();
    do {
        ctx = ctx.prev;  // ★ 向 head 方向移动
    } while (skipContext(ctx, currentExecutor, mask, MASK_ONLY_OUTBOUND));
    return ctx;
}
```

### 4.3 channel.write() vs ctx.write() 的本质区别

```
Pipeline: head ↔ Encoder ↔ BusinessHandler ↔ tail

channel.write(msg)：
  → pipeline.write(msg)
  → tail.write(msg)                    从 tail 开始
  → findContextOutbound() 找到 BusinessHandler（如果它是出站的）或 Encoder
  → ... 经过所有出站 Handler
  → HeadContext.write()
  → unsafe.write()

ctx.write(msg)（在 BusinessHandler 中调用）：
  → findContextOutbound() 从 BusinessHandler 的位置向 head 查找
  → 找到 Encoder
  → ... 经过 BusinessHandler 之前的出站 Handler
  → HeadContext.write()
  → unsafe.write()
```

**本质区别**：`channel.write()` 从 `tail` 开始，经过**所有**出站 Handler；`ctx.write()` 从**当前 Context** 开始，只经过当前位置**之前**的出站 Handler。

**实际影响**：如果在 BusinessHandler 中用 `ctx.write(response)`，消息会经过 Encoder 后到达 HeadContext，这是正确的。但如果用 `channel.write(response)`，消息从 tail 出发，**可能会再次经过 BusinessHandler 本身**（如果它同时实现了出站接口），导致死循环或逻辑错误。

---

## 五、HeadContext 和 TailContext 的源码

### 5.1 HeadContext — IO 操作的执行者 + 入站事件的起点

```java
// DefaultChannelPipeline.java 内部类
final class HeadContext extends AbstractChannelHandlerContext
        implements ChannelOutboundHandler, ChannelInboundHandler {

    private final Unsafe unsafe;

    HeadContext(DefaultChannelPipeline pipeline) {
        super(pipeline, null, HEAD_NAME, HeadContext.class);
        unsafe = pipeline.channel().unsafe();
        setAddComplete();
    }

    @Override
    public ChannelHandler handler() { return this; }

    // ═══ 出站操作：所有出站事件的终点，委托给 Unsafe ═══

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress,
                     ChannelPromise promise) {
        unsafe.bind(localAddress, promise);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                        SocketAddress localAddress, ChannelPromise promise) {
        unsafe.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) {
        unsafe.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        unsafe.close(promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) {
        unsafe.deregister(promise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) {
        unsafe.beginRead();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        unsafe.write(msg, promise);  // ★ 写入 ChannelOutboundBuffer
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        unsafe.flush();  // ★ 触发真正的 Socket 写入
    }

    // ═══ 入站事件：大部分只是转发给下一个 Handler ═══

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        invokeHandlerAddedIfNeeded();      // ★ 触发延迟的 handlerAdded 回调
        ctx.fireChannelRegistered();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
        ctx.fireChannelUnregistered();
        if (!channel.isOpen()) {
            destroy();  // ★ Channel 关闭时，移除所有 Handler
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.fireChannelActive();
        readIfIsAutoRead();  // ★ 如果 autoRead=true，触发一次 read
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ctx.fireChannelRead(msg);  // 简单转发
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.fireChannelReadComplete();
        readIfIsAutoRead();  // ★ 读完一批数据后，再次注册 OP_READ
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.fireExceptionCaught(cause);  // 继续传播异常
    }

    private void readIfIsAutoRead() {
        if (channel.config().isAutoRead()) {
            channel.read();  // 触发 beginRead() → 注册 OP_READ
        }
    }
}
```

HeadContext 的**双重角色**：

1. **出站终点**：所有出站事件（write/flush/bind/connect/close）传播到 HeadContext 后，委托给 `Unsafe` 执行真正的 IO 操作
2. **入站起点**：HeadContext 是入站事件传播链的第一个节点，它负责一些框架级的初始化工作（invokeHandlerAddedIfNeeded、readIfIsAutoRead），然后将事件转发给后续的用户 Handler

### 5.2 TailContext — 入站事件的兜底处理

```java
// DefaultChannelPipeline.java 内部类
final class TailContext extends AbstractChannelHandlerContext
        implements ChannelInboundHandler {

    TailContext(DefaultChannelPipeline pipeline) {
        super(pipeline, null, TAIL_NAME, TailContext.class);
        setAddComplete();
    }

    @Override
    public ChannelHandler handler() { return this; }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        onUnhandledInboundMessage(ctx, msg);  // ★ 未处理的消息
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        onUnhandledInboundException(cause);  // ★ 未处理的异常
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        onUnhandledInboundUserEventTriggered(evt);
    }

    // channelRegistered / channelUnregistered / channelActive / channelInactive
    // channelReadComplete / channelWritabilityChanged → 空实现
}
```

兜底处理的实现：

```java
// DefaultChannelPipeline.java
protected void onUnhandledInboundMessage(Object msg) {
    try {
        logger.debug(
            "Discarded inbound message {} that reached at the tail of the pipeline. " +
            "Please check your pipeline configuration.", msg);
    } finally {
        ReferenceCountUtil.release(msg);  // ★ 释放引用计数，防止内存泄漏
    }
}

protected void onUnhandledInboundException(Throwable cause) {
    try {
        logger.warn(
            "An exceptionCaught() event was fired, and it reached at the tail of the pipeline. " +
            "It usually means the last handler in the pipeline did not handle the exception.",
            cause);
    } finally {
        ReferenceCountUtil.release(cause);
    }
}
```

TailContext 的**核心职责**就是兜底——如果入站消息一路传播到 tail 都没有被任何 Handler 消费，TailContext 负责释放它的引用计数（防止 ByteBuf 内存泄漏），并打印警告日志提醒开发者检查 Pipeline 配置。

---

## 六、异常传播

### 6.1 入站异常的传播

当任何 Handler 的入站方法抛出异常时：

```java
// AbstractChannelHandlerContext.fireChannelRead() 中
try {
    ((ChannelInboundHandler) handler).channelRead(next, m);
} catch (Throwable t) {
    next.invokeExceptionCaught(t);  // ★ 异常转入 exceptionCaught 传播链
}
```

`invokeExceptionCaught` 的实现：

```java
// AbstractChannelHandlerContext.java
private void invokeExceptionCaught(final Throwable cause) {
    if (invokeHandler()) {
        try {
            handler().exceptionCaught(this, cause);
        } catch (Throwable error) {
            // ★ 如果 exceptionCaught 本身也抛异常，只记录日志，不再传播
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "An exception was thrown by a user handler's exceptionCaught() " +
                    "method while handling the following exception:", cause);
            } else if (logger.isWarnEnabled()) {
                logger.warn(
                    "An exception '{}' was thrown by a user handler's exceptionCaught() " +
                    "method while handling the following exception:", error, cause);
            }
        }
    } else {
        fireExceptionCaught(cause);  // Handler 未就绪，继续传播
    }
}
```

`ctx.fireExceptionCaught()` 向 tail 方向传播：

```java
// AbstractChannelHandlerContext.java
@Override
public ChannelHandlerContext fireExceptionCaught(final Throwable cause) {
    AbstractChannelHandlerContext next = findContextInbound(MASK_EXCEPTION_CAUGHT);
    // ... 调用 next.invokeExceptionCaught(cause)
    return this;
}
```

**异常传播规则总结**：

```
Handler1.channelRead() 抛异常
    │
    ▼
Handler1.exceptionCaught(cause)    ← 先交给抛异常的 Handler 自己处理
    │
    │  如果调用 ctx.fireExceptionCaught(cause)
    ▼
Handler2.exceptionCaught(cause)    ← 沿入站方向继续传播
    │
    │  如果继续 fire
    ▼
...
    │
    ▼
TailContext.exceptionCaught(cause) ← 兜底：打印警告日志
```

### 6.2 出站异常的处理

出站异常（write/bind/connect 等）有不同的处理路径——通过 `ChannelPromise` 通知：

```java
// AbstractChannelHandlerContext.java
private static void notifyOutboundHandlerException(Throwable cause, ChannelPromise promise) {
    PromiseNotificationUtil.tryFailure(promise, cause,
        promise instanceof VoidChannelPromise ? null : logger);
}
```

这意味着出站操作的异常会设置到对应的 Promise 上，用户可以通过 `future.addListener()` 或 `promise.cause()` 获取异常信息。

### 6.3 最佳实践

```java
// 在 Pipeline 最后面加一个异常捕获 Handler
pipeline.addLast(new ChannelInboundHandlerAdapter() {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Unhandled exception in pipeline", cause);
        ctx.close();  // 出现未处理异常，关闭连接
    }
});
```

---

## 七、与 Dubbo Filter 链的对比

| 对比维度 | Netty Pipeline | Dubbo Filter 链 |
|---------|---------------|-----------------|
| 数据结构 | 双向链表（prev/next） | 单向链表（next） |
| 方向 | 双向：入站 head→tail，出站 tail→head | 单向：request 正向传播，response 反向传播 |
| 入站/出站分离 | 入站 Handler 和出站 Handler 独立编程 | Filter 同时处理 request 和 response |
| 动态修改 | 运行时可 add/remove Handler | 启动时通过 SPI 加载，运行时不可修改 |
| 异常处理 | exceptionCaught 沿链传播 | try-catch 在每个 Filter 中 |
| 线程模型 | 支持不同 Handler 绑定不同 Executor | Filter 链在同一线程执行 |
| 跳过优化 | @Skip + executionMask 位掩码跳过 | 无类似优化 |

**相同点**：两者都是责任链模式的经典应用——将请求处理分解为多个独立的处理步骤（Handler/Filter），每个步骤只关心自己的逻辑，通过链式调用串联。这提供了极高的可扩展性。

**不同点**：Netty Pipeline 是双向的——入站和出站是两条独立的传播链，共享同一个双向链表。这比 Dubbo 的单向 Filter 链更复杂，但也更灵活：解码器只需要实现入站接口，编码器只需要实现出站接口，互不干扰。

---

## 八、设计哲学总结

Pipeline 作为"拦截器模式"的经典实现，**为什么要双向而非单向**？

网络通信天然是双向的——有请求就有响应，有读就有写。如果用单向链表，处理入站事件后发送响应时，消息需要"绕一圈"重新从链表起点开始传播。双向链表让入站和出站各自有独立的传播方向，在同一个数据结构上实现了两条逻辑上独立的处理流水线：

- 入站方向（head → tail）：收到网络数据 → 解码 → 业务处理
- 出站方向（tail → head）：业务响应 → 编码 → 写入网络

这种设计的核心优势是**关注点分离**——解码器只关心"字节怎么变成对象"，编码器只关心"对象怎么变成字节"，业务 Handler 只关心"收到请求后如何处理"。它们可以独立开发、独立测试、自由组合，像乐高积木一样搭建出各种协议处理管道。

---

## 九、本篇涉及的设计模式

**责任链模式（Chain of Responsibility）**：Pipeline 的核心设计——`head ↔ ctx1 ↔ ctx2 ↔ ... ↔ tail` 双向链表，每个 Context/Handler 决定是否处理当前事件，或者通过 `ctx.fireXxx()` / `ctx.write()` 传递给下一个。体现在 `AbstractChannelHandlerContext.fireChannelRead()` 和 `AbstractChannelHandlerContext.write()` 中。

**模板方法模式（Template Method）**：`ChannelInboundHandlerAdapter` 和 `ChannelOutboundHandlerAdapter` 定义了所有方法的默认实现（直接转发），用户只需覆写感兴趣的方法。这是模板方法模式的变体——基类提供"什么都不做就转发"的骨架，子类覆写需要处理的特定事件。

**观察者模式（Observer）**：Pipeline 的事件传播机制——当 IO 事件发生时（如数据到达），通过 `fireChannelRead()` 通知所有注册的 Handler。每个 Handler 就是一个观察者，可以选择处理事件或继续传播。

**适配器模式（Adapter）**：`ChannelInboundHandlerAdapter` 和 `ChannelOutboundHandlerAdapter` 为 Handler 接口的每个方法提供了空/转发实现，用户继承 Adapter 只需覆写需要的方法，避免了实现接口的所有方法。这是经典的接口适配器模式。

**装饰器模式（Decorator）**：每个 Handler 都可以在不修改消息的情况下"装饰"它（如 LoggingHandler 打印日志后转发），或者转换消息（如 Decoder 将 ByteBuf 转为业务对象）。Handler 链本质上是装饰器的链式组合。

**策略模式（Strategy）**：通过替换 Pipeline 中的 Handler，可以改变事件处理策略。例如用不同的 Decoder 处理不同的协议，用不同的 Encoder 实现不同的序列化方式。Pipeline 的动态 add/remove 能力使得策略切换可以在运行时完成。

---

## 十、本篇涉及的高性能并发技术

**executionMask 位掩码跳过（@Skip 优化）**：通过 `int` 类型的 `executionMask` 位掩码记录 Handler 关心哪些事件，`findContextInbound/Outbound` 方法在遍历链表时通过位运算快速跳过不需要的节点，避免了不必要的方法调用。在长 Pipeline 中（如 10+ 个 Handler），这个优化减少了每次事件传播的调用栈深度。解决的瓶颈：Pipeline 中大量 Adapter Handler 的方法调用开销。

**线程封闭与 inEventLoop 判断**：所有事件传播方法都先检查 `executor.inEventLoop()`，如果不在目标线程则异步提交。这保证了 Handler 的方法始终在正确的线程中执行，无需任何锁。解决的瓶颈：多线程并发访问 Handler 状态的同步开销。

**synchronized + 锁外回调**：`internalAdd` 使用 `synchronized(this)` 保护链表修改，但 `callHandlerAdded0()` 在锁外执行。这避免了用户在 `handlerAdded()` 回调中调用 `addLast()` 等方法时的死锁。解决的瓶颈：链表修改的线程安全与回调灵活性的平衡。

**WriteTask 对象池化**：跨线程的 write 操作被封装为 `WriteTask`，通过 `Recycler` 对象池复用，减少了高频 write 场景下的 GC 压力。解决的瓶颈：大量短生命周期 Task 对象的 GC 开销。

**volatile + 状态机**：Handler 的状态（INIT → ADD_PENDING → ADD_COMPLETE → REMOVE_COMPLETE）通过 volatile int 管理，`invokeHandler()` 方法通过简单的读取和比较判断 Handler 是否就绪，无需锁。解决的瓶颈：Handler 生命周期状态的线程安全检查。

**延迟回调机制**：Channel 未注册时添加的 Handler，其 `handlerAdded()` 回调被收集到 `PendingHandlerCallback` 链表中，在 register 完成后批量执行。这避免了在 Channel 尚未绑定 EventLoop 时执行回调可能导致的线程安全问题。解决的瓶颈：Channel 创建阶段的时序安全。
