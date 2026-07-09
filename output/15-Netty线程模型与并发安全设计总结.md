# Netty 线程模型与并发安全设计总结

> **Netty 源码深度研究系列 · 第 15 篇**
>
> 基于 Netty 主分支源码，从竞态条件的视角出发，系统性地剖析 Netty 如何通过 EventLoop 线程亲和性、inEventLoop 判断、synchronized 保护链表、无锁原子操作等手段，在高并发环境下实现"无锁化"的极致性能与绝对的线程安全。

---

## 一、核心原则：EventLoop 线程亲和性（Thread Affinity）

### 1.1 一个 Channel 只绑定一个 EventLoop

Netty 线程模型的根基是一个极其简单的约束：**每个 Channel 在其整个生命周期中只绑定一个 EventLoop 线程，该 Channel 上的所有 IO 操作和事件回调都在这个唯一的线程中串行执行**。这意味着同一个 Channel 上的 `channelRead`、`write`、`flush`、`close` 等操作之间不存在竞态条件，Handler 的开发者无需为同一个 Channel 的并发访问加锁。

这一约束的实现锚定在 `SingleThreadEventExecutor.inEventLoop(Thread)` 方法上：

```java
// SingleThreadEventExecutor.java
private volatile Thread thread;

@Override
public boolean inEventLoop(Thread thread) {
    return thread == this.thread;
}
```

`thread` 字段在 `doStartThread()` 中被赋值为执行 `run()` 方法的那个线程（`thread = Thread.currentThread()`），之后通过 `volatile` 保证对其他线程可见。整个判断只是一次引用比较，开销极低，但它是 Netty 所有线程安全策略的分水岭——几乎每一个可能被外部线程调用的方法，都会先执行 `inEventLoop()` 判断，并据此选择"直接执行"还是"封装为 task 提交"。

### 1.2 OrderedEventExecutor 标记接口

`SingleThreadEventExecutor` 实现了 `OrderedEventExecutor` 接口：

```java
// OrderedEventExecutor.java
/**
 * Marker interface for EventExecutors that will process all submitted tasks
 * in an ordered / serial fashion.
 */
public interface OrderedEventExecutor extends EventExecutor {
}
```

这是一个纯标记接口，没有定义任何方法。它的作用在于向 Netty 框架的其他组件声明：提交到这个 Executor 的所有 task 会按提交顺序串行执行。`AbstractChannelHandlerContext` 的构造方法中会检查这一点：

```java
// AbstractChannelHandlerContext.java
ordered = executor == null || executor instanceof OrderedEventExecutor;
```

当 `ordered` 为 `true` 时，`invokeHandler()` 方法允许在 `ADD_PENDING` 状态下执行 Handler，因为串行执行保证了 `handlerAdded()` 回调一定在后续事件之前完成。这个看似微小的标记，避免了在 Handler 添加流程中引入额外的同步屏障。

---

## 二、线程安全的关键实现

### 2.1 非 EventLoop 线程操作自动封装为 task

设想一个典型的竞态场景：业务线程池中的某个线程执行完数据库查询后，想通过某个 Channel 把结果写回客户端。如果直接调用 `ctx.write(result)`，write 操作会沿着 Pipeline 向前传播，最终到达 `HeadContext` 调用 `unsafe.write()` 来操作 `ChannelOutboundBuffer`。但此时当前线程是业务线程，而 `ChannelOutboundBuffer` 的链表操作没有加锁。如果 EventLoop 线程同时在执行 flush 遍历同一个链表，就会出现链表指针被并发修改的灾难性后果。

Netty 通过 `AbstractChannelHandlerContext.write()` 中的 `inEventLoop()` 判断彻底消除了这种可能：

```java
// AbstractChannelHandlerContext.java
void write(Object msg, boolean flush, ChannelPromise promise) {
    if (validateWrite(msg, promise)) {
        final AbstractChannelHandlerContext next = findContextOutbound(flush ?
                MASK_WRITE | MASK_FLUSH : MASK_WRITE);
        final Object m = pipeline.touch(msg, next);
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            // 当前就是 EventLoop 线程 → 直接同步调用 handler.write()
            if (next.invokeHandler()) {
                promise = ensurePromiseUseCorrectExecutor(promise);
                // ... 直接调用 handler.write(ctx, msg, promise)
            }
        } else {
            // 非 EventLoop 线程 → 包装为 WriteTask 提交到 EventLoop 的 taskQueue
            final WriteTask task = WriteTask.newInstance(this, m, promise, flush);
            if (!safeExecute(executor, task, promise, m, !flush)) {
                task.cancel();
            }
        }
    }
}
```

这段代码的关键决策路径是：先通过 `findContextOutbound` 找到下一个目标 Handler 的 Context，取出其 Executor，再判断当前线程是否就是该 Executor 的线程。如果是，一切同步进行，零额外开销；如果不是，创建一个 `WriteTask` 对象提交到目标 Executor 的队列中去。

### 2.2 WriteTask 的 Recycler 对象池复用

`WriteTask` 的创建使用了 `Recycler` 对象池，避免了每次跨线程写入都产生新的对象分配和 GC 压力：

```java
// AbstractChannelHandlerContext.WriteTask
static final class WriteTask implements Runnable {
    private static final Recycler<WriteTask> RECYCLER = new Recycler<WriteTask>() {
        @Override
        protected WriteTask newObject(Handle<WriteTask> handle) {
            return new WriteTask(handle);
        }
    };

    static WriteTask newInstance(AbstractChannelHandlerContext ctx,
            Object msg, ChannelPromise promise, boolean flush) {
        WriteTask task = RECYCLER.get();
        init(task, ctx, msg, promise, flush);
        return task;
    }
}
```

`init` 方法中还会在提交时立即更新 `pendingOutboundBytes`（通过 `ctx.pipeline.incrementPendingOutboundBytes(task.size)`），因为这一计数需要跨线程可见以触发写水位控制。`WriteTask` 执行完毕后在 `recycle()` 中清空所有引用并归还对象池。

### 2.3 ensurePromiseUseCorrectExecutor 保证 Listener 线程安全

在直接调用路径中，`write` 会调用 `ensurePromiseUseCorrectExecutor(promise)`：

```java
private ChannelPromise ensurePromiseUseCorrectExecutor(ChannelPromise promise) {
    if (promise instanceof DefaultChannelPromise &&
            !((DefaultChannelPromise) promise).executor().inEventLoop()) {
        ChannelPromise newPromise = newPromise();
        PromiseNotifier.cascade(newPromise, promise);
        return newPromise;
    }
    return promise;
}
```

这处理的是另一个微妙的竞态场景：用户可能在 Handler 中创建了一个 Promise，然后在其上注册了 Listener，该 Listener 会访问 Handler 中的非线程安全字段。如果 Promise 的 Executor 不是当前 Handler 所在的 EventLoop，Listener 就可能在错误的线程上执行。此方法通过创建一个新的 Promise（绑定到正确的 Executor）并级联通知来保证 Listener 一定在 Handler 的 EventLoop 线程中被回调。

### 2.4 Pipeline 添加 Handler 的线程安全

Pipeline 的链表结构会被多个线程并发修改。比如在 `ServerBootstrap` 的 `childHandler` 中添加 Handler 发生在 boss EventLoop 线程，而后续的动态 `pipeline.addLast()` 可能在 worker EventLoop 线程甚至业务线程中调用。Netty 在 `DefaultChannelPipeline.internalAdd()` 中使用了 `synchronized(this)` 来保护链表修改：

```java
// DefaultChannelPipeline.java
private ChannelPipeline internalAdd(EventExecutorGroup group, String name,
                                    ChannelHandler handler, String baseName,
                                    AddStrategy addStrategy) {
    final AbstractChannelHandlerContext newCtx;
    synchronized (this) {
        checkMultiplicity(handler);
        name = filterName(name, handler);
        newCtx = newContext(group, name, handler);

        // 修改双向链表
        switch (addStrategy) {
            case ADD_FIRST:  addFirst0(newCtx);  break;
            case ADD_LAST:   addLast0(newCtx);   break;
            case ADD_BEFORE: addBefore0(getContextOrDie(baseName), newCtx); break;
            case ADD_AFTER:  addAfter0(getContextOrDie(baseName), newCtx);  break;
        }

        if (!registered) {
            // Channel 尚未注册 → 延迟回调
            newCtx.setAddPending();
            callHandlerCallbackLater(newCtx, true);
            return this;
        }

        EventExecutor executor = newCtx.executor();
        if (!executor.inEventLoop()) {
            // 非 EventLoop 线程 → 提交 task 到 EventLoop 中回调
            callHandlerAddedInEventLoop(newCtx, executor);
            return this;
        }
    }
    // 在 EventLoop 线程中 → 退出 synchronized 后直接回调
    callHandlerAdded0(newCtx);
    return this;
}
```

这里的三分支回调调度极为精妙。第一种情况，Channel 尚未注册到任何 EventLoop（例如在 `ChannelInitializer` 中添加 Handler），此时将回调封装为 `PendingHandlerCallback` 挂入一个链表，待注册完成后在 `invokeHandlerAddedIfNeeded()` 中统一触发。第二种情况，Channel 已注册但当前不在 EventLoop 线程，则通过 `callHandlerAddedInEventLoop` 将回调提交到 EventLoop 的 task 队列。第三种情况，当前就在 EventLoop 线程中，退出 `synchronized` 块后直接调用 `callHandlerAdded0`，避免在持锁状态下执行用户代码（用户的 `handlerAdded` 可能尝试再次添加 Handler 从而导致死锁）。

值得注意的是，`AbstractChannelHandlerContext` 的 `next` 和 `prev` 字段声明为 `volatile`：

```java
volatile AbstractChannelHandlerContext next;
volatile AbstractChannelHandlerContext prev;
```

这意味着事件传播时沿链表读取下一个节点不需要获取锁——`volatile` 读保证看到最新的链表结构。写操作（链表修改）由 `synchronized(this)` 保护，读操作（事件传播）通过 `volatile` 保证可见性，二者配合实现了写时加锁、读时无锁的高效方案。

### 2.5 ChannelOutboundBuffer 无锁设计的原因

`ChannelOutboundBuffer` 是管理待发送数据的核心数据结构，它内部维护了一个由 `flushedEntry`、`unflushedEntry`、`tailEntry` 三个指针组成的链表。打开其 Javadoc 可以看到一句关键声明：

```
All methods must be called by a transport implementation from an I/O thread,
except the following ones: isWritable(), getUserDefinedWritability(int)
and setUserDefinedWritability(int, boolean)
```

这就是说，链表操作（`addMessage`、`addFlush`、`remove`、`nioBuffers` 等）全部只在 EventLoop 线程中调用。`AbstractChannel.AbstractUnsafe` 中的 `assertEventLoop()` 断言确保了这一点：

```java
// AbstractChannel.java
private void assertEventLoop() {
    assert !registered || eventLoop.inEventLoop();
}

@Override
public final void write(Object msg, ChannelPromise promise) {
    assertEventLoop();
    // ... 操作 outboundBuffer
}

@Override
public final void flush() {
    assertEventLoop();
    // ... 遍历 outboundBuffer
}
```

既然链表操作被约束在单线程中，自然不需要 `synchronized`。但 `ChannelOutboundBuffer` 中有两个字段使用了原子操作——它们恰恰是需要跨线程访问的例外：

```java
private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER =
        AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");
private volatile long totalPendingSize;

private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> UNWRITABLE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "unwritable");
private volatile int unwritable;
```

`totalPendingSize` 使用 `AtomicLongFieldUpdater` 是因为 `WriteTask` 在提交时（可能从非 EventLoop 线程）会通过 `incrementPendingOutboundBytes` 增加待写字节数。`unwritable` 使用 `AtomicIntegerFieldUpdater` 是因为 `isWritable()` 可以从任意线程调用（用户可能在业务线程中检查 Channel 是否可写），而水位变化时对 `unwritable` 的 CAS 操作可能与读操作并发。

当水位状态发生翻转（从可写变为不可写或反之）时，`fireChannelWritabilityChanged` 通过 `channel.eventLoop().execute(task)` 将事件通知回调回 EventLoop 线程：

```java
private void fireChannelWritabilityChanged(boolean invokeLater) {
    final ChannelPipeline pipeline = channel.pipeline();
    if (invokeLater) {
        Runnable task = fireChannelWritabilityChangedTask;
        if (task == null) {
            fireChannelWritabilityChangedTask = task = () -> pipeline.fireChannelWritabilityChanged();
        }
        channel.eventLoop().execute(task);
    } else {
        pipeline.fireChannelWritabilityChanged();
    }
}
```

这是一个典型的设计思路：大部分操作约束在单线程中以避免锁，少数需要跨线程可见的状态使用原子变量，状态变化的通知回调则通过 task 投递回正确的线程。

---

## 三、EventLoop 的 task 机制

### 3.1 普通 task：MpscQueue

EventLoop 的 task 队列默认使用 JCTools 提供的 `MpscQueue`（Multi-Producer Single-Consumer Queue），这是一种无锁队列，允许多个线程并发提交 task（Multi-Producer），但只有一个 EventLoop 线程消费 task（Single-Consumer）。`NioEventLoop` 重写了 `newTaskQueue()` 来使用这种更高性能的实现，而非 `SingleThreadEventExecutor` 基类默认的 `LinkedBlockingQueue`。

task 的提交路径如下：

```java
// SingleThreadEventExecutor.java
private void execute(Runnable task, boolean immediate) {
    boolean inEventLoop = inEventLoop();
    addTask(task);           // → taskQueue.offer(task)
    if (!inEventLoop) {
        startThread();       // 确保 EventLoop 线程已启动
        // ...
    }
    if (!addTaskWakesUp && immediate) {
        wakeup(inEventLoop); // 唤醒正在 select 的 EventLoop
    }
}
```

当非 EventLoop 线程提交 task 时，除了入队之外还会调用 `startThread()` 确保 EventLoop 已经在运行（懒启动），并调用 `wakeup()` 唤醒可能正阻塞在 `Selector.select()` 上的 EventLoop 线程，使其尽快消费新 task。

### 3.2 定时 task：PriorityQueue

定时任务存储在 `AbstractScheduledEventExecutor` 维护的 `PriorityQueue<ScheduledFutureTask>` 中，按截止时间排序。与普通 task 队列不同，定时任务队列不是线程安全的——因为它只应该在 EventLoop 线程中被访问。当从外部线程调用 `schedule()` 时，Netty 将"添加到定时队列"这个操作本身包装成一个普通 task 提交到线程安全的 MpscQueue 中：

```java
// AbstractScheduledEventExecutor.java
private <V> ScheduledFuture<V> schedule(final ScheduledFutureTask<V> task) {
    if (inEventLoop()) {
        scheduleFromEventLoop(task);   // 直接添加到 scheduledTaskQueue
    } else {
        // ... 检查延迟时间
        execute(task);                 // 包装成普通 task 提交
    }
    return task;
}
```

### 3.3 执行顺序：先定时后普通，合并执行

`SingleThreadEventExecutor.runAllTasks()` 的执行逻辑是：先将所有已到期的定时 task 从 `scheduledTaskQueue` 移入 `taskQueue`，然后统一从 `taskQueue` 中批量执行：

```java
protected boolean runAllTasks() {
    assert inEventLoop();
    boolean fetchedAll;
    boolean ranAtLeastOne = false;

    do {
        fetchedAll = fetchFromScheduledTaskQueue(taskQueue);
        if (runAllTasksFrom(taskQueue)) {
            ranAtLeastOne = true;
        }
    } while (!fetchedAll);

    if (ranAtLeastOne) {
        lastExecutionTime = getCurrentTimeNanos();
    }
    afterRunningAllTasks();
    return ranAtLeastOne;
}
```

`fetchFromScheduledTaskQueue` 从定时队列中逐个取出到期的 `ScheduledFutureTask` 并 `offer` 到 `taskQueue`。如果 `taskQueue` 满了（`offer` 返回 `false`），则将该任务放回定时队列并返回 `false`，外层 `do-while` 会在处理完一批后再次尝试。

带超时版本的 `runAllTasks(long timeoutNanos)` 则会每执行 64 个 task 检查一次是否超时（`if ((runTasks & 0x3F) == 0)`），在 IO 处理和 task 执行之间保持平衡。这个 64 的间隔是为了避免频繁调用 `nanoTime()` 带来的开销。

---

## 四、典型并发陷阱与解决方案

### 4.1 Handler 中启线程

一个常见的错误模式是在 `channelRead` 中启动新线程执行耗时操作，然后在新线程中直接操作 `ctx` 或 `channel`：

```java
// ❌ 错误示例
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    new Thread(() -> {
        String result = queryDatabase(msg);
        ctx.writeAndFlush(result);  // 跨线程调用！
    }).start();
}
```

表面上这段代码看起来"能工作"，因为 `ctx.writeAndFlush()` 内部会通过 `inEventLoop()` 判断发现当前不在 EventLoop 线程中，进而包装为 `WriteTask` 提交。但问题在于 `msg` 可能是一个 `ByteBuf`，此时 `ByteBuf` 的引用计数和内容都没有保护——EventLoop 线程可能在新线程使用 `msg` 之前就释放了它。正确的做法是先 `retain()` 再传递，或者使用 EventLoop 的 `execute()` 提交整个任务：

```java
// ✅ 正确示例
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf buf = (ByteBuf) msg;
    buf.retain();  // 增加引用计数，防止被释放
    executor.submit(() -> {
        try {
            String result = queryDatabase(buf);
            ctx.writeAndFlush(result);
        } finally {
            buf.release();  // 在使用完毕后释放
        }
    });
}
```

### 4.2 @Sharable 注解语义

`@ChannelHandler.Sharable` 注解标识一个 Handler 可以安全地添加到多个 Pipeline 中。`DefaultChannelPipeline.checkMultiplicity()` 在每次添加 Handler 时进行检查：

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

逻辑很清晰：如果 Handler 没有标注 `@Sharable` 且 `added` 字段已经为 `true`（说明已被添加过），就抛出异常阻止重复添加。值得注意的是，`added` 字段**没有声明为 `volatile`**：

```java
// ChannelHandlerAdapter.java
// Not using volatile because it's used only for a sanity check.
boolean added;
```

源码注释明确说明这只是一个"sanity check"——最坏情况下，两个线程同时添加同一个非 `@Sharable` Handler，都没看到对方设置的 `added = true`，导致检查漏过。但这只是一个边缘场景，不会导致数据损坏，只是用户犯错时可能收不到异常提醒。在正确使用的情况下（同一个 Handler 只添加一次），这个检查完全不需要 `volatile` 的内存可见性保证。

`isSharable()` 的实现使用了 `ThreadLocal` + `WeakHashMap` 来缓存反射结果：

```java
// ChannelHandlerAdapter.java
public boolean isSharable() {
    Class<?> clazz = getClass();
    Map<Class<?>, Boolean> cache = InternalThreadLocalMap.get().handlerSharableCache();
    Boolean sharable = cache.get(clazz);
    if (sharable == null) {
        sharable = clazz.isAnnotationPresent(Sharable.class);
        cache.put(clazz, sharable);
    }
    return sharable;
}
```

每个线程有自己的缓存 Map，避免了缓存本身的并发访问问题。使用 `WeakHashMap` 是为了让类加载器卸载 Handler 类时能正确回收缓存条目。

### 4.3 ByteBuf 跨线程传递

ByteBuf 的引用计数操作本身是线程安全的（基于 `AtomicIntegerFieldUpdater`），但 ByteBuf 的内容读写不是。设想一个场景：EventLoop 线程 A 上的 Handler 收到一个 `ByteBuf`，想传递给 EventLoop 线程 B 上注册的另一个 Channel 进行转发。如果不 `retain()` 就直接传递，原 Pipeline 的后续 Handler 可能会 `release()` 掉这个 ByteBuf，导致线程 B 读到已回收的内存。

正确的做法是使用 `ByteBuf.retainedDuplicate()` 或 `copy()` 创建一个独立的副本，或者在传递前 `retain()` 并在目标线程使用完毕后 `release()`。Netty 的 `ResourceLeakDetector` 会在开发阶段帮助检测这类问题。

---

## 五、ChannelHandlerMask：事件传播的跳过优化

`ChannelHandlerMask` 通过反射和 `@Skip` 注解为每个 Handler 类构建一个 `executionMask` 位图，标记该 Handler 关心哪些事件：

```java
// ChannelHandlerMask.java
private static int mask0(Class<? extends ChannelHandler> handlerType) {
    int mask = MASK_EXCEPTION_CAUGHT;
    if (ChannelInboundHandler.class.isAssignableFrom(handlerType)) {
        mask |= MASK_ALL_INBOUND;
        if (isSkippable(handlerType, "channelRead", ChannelHandlerContext.class, Object.class)) {
            mask &= ~MASK_CHANNEL_READ;
        }
        // ... 对每个事件方法检查 @Skip
    }
    // ...
    return mask;
}
```

`isSkippable` 通过反射查找方法上是否标注了 `@Skip` 注解。例如 `ChannelHandlerAdapter.exceptionCaught()` 默认标注了 `@Skip`，表示它只是简单转发，不做实际处理。事件传播时，`findContextInbound` 和 `findContextOutbound` 会利用 `executionMask` 跳过那些不关心该事件的 Handler：

```java
private AbstractChannelHandlerContext findContextInbound(int mask) {
    AbstractChannelHandlerContext ctx = this;
    EventExecutor currentExecutor = executor();
    do {
        ctx = ctx.next;
    } while (skipContext(ctx, currentExecutor, mask, MASK_ONLY_INBOUND));
    return ctx;
}
```

这个优化的并发安全性由 `volatile next/prev` 保证——跳过判断在读取链表节点时总能看到最新的链表结构。`executionMask` 本身在 `AbstractChannelHandlerContext` 构造时计算一次，之后不再改变，因此也不存在并发修改的风险。

mask 的计算结果通过 `FastThreadLocal<WeakHashMap>` 缓存，与 `isSharable()` 的缓存策略一致：每线程独立缓存，无需跨线程同步。

---

## 六、与其他框架线程模型对比

### 6.1 Tomcat

Tomcat 的 NIO 模式使用 Acceptor 线程接受连接后交给 Poller 线程监听 IO 就绪事件，就绪后将请求分发到工作线程池中处理。一个请求的处理在一个工作线程中完成，但同一个连接的多次请求可能分配到不同的工作线程。这意味着如果使用 keep-alive 长连接，连接级别的状态需要开发者自己保证线程安全。Tomcat 的模型更适合短生命周期的请求-响应模式，在这种场景下线程池的利用率高于 Netty 的固定绑定模式。

### 6.2 Go

Go 使用 goroutine + 运行时调度器（GMP 模型），每个连接通常用一个 goroutine 以同步阻塞的风格编写代码。Go 的运行时负责在 goroutine 阻塞时挂起并调度其他 goroutine，开发者无需关心 IO 多路复用的细节。goroutine 的内存开销极小（初始栈只有几 KB），因此可以轻松创建数十万个。但同一个连接的并发读写仍然需要开发者自己加锁，Go 推荐使用 Channel（语言层面的通信原语）来协调 goroutine 之间的数据交换。

### 6.3 Node.js

Node.js 使用单线程事件循环，所有 JavaScript 代码在同一个线程中执行，IO 操作委托给 libuv 的线程池。这种模型天然避免了并发问题，但代价是 CPU 密集型任务会阻塞整个事件循环。Node.js 可以通过 `worker_threads` 模块或 `cluster` 模块扩展到多核，但每个 worker 是一个独立的事件循环，不共享内存。

### 6.4 Netty

Netty 的模型可以看作是 Node.js 单线程事件循环的多实例版本：`NioEventLoopGroup` 包含 N 个 `NioEventLoop`（默认 N = CPU 核数 × 2），每个 `NioEventLoop` 是一个独立的单线程事件循环，拥有自己的 `Selector` 和 task 队列。Channel 在注册时被分配到某个 `NioEventLoop` 并终身绑定。这种设计既保留了单线程模型的简单性（同一个 Channel 的操作无需加锁），又通过多个 EventLoop 实例充分利用多核。相比 Tomcat 的工作线程池模型，Netty 避免了线程上下文切换的开销和锁竞争；相比 Go 的 goroutine 模型，Netty 的线程数量是固定的和可控的，不存在因 goroutine 泄漏导致的资源耗尽风险；相比 Node.js 的单线程模型，Netty 天然支持多核而无需额外的进程管理。

| 维度 | Tomcat NIO | Go (GMP) | Node.js | Netty |
|------|-----------|----------|---------|-------|
| 并发单元 | 线程池中的工作线程 | goroutine | 单线程事件循环 | EventLoop（固定数量） |
| 连接绑定 | 无绑定，每次请求随机分配 | 一个 goroutine 一个连接 | 所有连接共享一个线程 | 一个 Channel 终身绑定一个 EventLoop |
| 同连接并发安全 | 需自行加锁 | 需自行加锁 | 天然安全（单线程） | 天然安全（单线程串行） |
| 多核利用 | 工作线程池 | 运行时调度 | cluster/worker_threads | 多 EventLoop 实例 |
| CPU 密集任务 | 可以，占用工作线程 | 可以，调度器自动处理 | 阻塞事件循环 | 阻塞 EventLoop，需卸载 |

---

## 七、本篇涉及的设计模式

- **线程亲和性模式（Thread Affinity）**：将特定对象（Channel）与特定线程（EventLoop）绑定，通过约束所有操作在同一线程中执行来消除并发问题。这是 Netty 线程安全策略的基石。

- **生产者-消费者模式（Producer-Consumer）**：MpscQueue 实现了多生产者单消费者的无锁队列。外部线程作为生产者提交 task，EventLoop 线程作为唯一消费者处理 task。

- **对象池模式（Object Pool / Recycler）**：`WriteTask` 使用 `Recycler` 对象池避免高频创建短生命周期对象。池化的 `WriteTask` 在执行完毕后清空状态并归还池中，下次需要时直接复用。

- **标记接口模式（Marker Interface）**：`OrderedEventExecutor` 是一个无方法的标记接口，仅通过 `instanceof` 检查来传达语义信息（"此 Executor 保证有序执行"），影响 `invokeHandler()` 的行为。

- **位掩码模式（Bitmask）**：`ChannelHandlerMask` 使用整数位图标记 Handler 关心的事件类型，在事件传播时通过位运算快速跳过不相关的 Handler，将 O(N) 的链表遍历优化为只访问真正需要处理事件的节点。

- **延迟初始化模式（Lazy Initialization）**：EventLoop 线程采用懒启动策略——直到第一个 task 被提交时才通过 `startThread()` 创建并启动线程。

---

## 八、本篇涉及的高性能并发技术

- **MPSC 无锁队列（JCTools MpscQueue）**：基于 CAS 操作实现的多生产者单消费者队列，在 EventLoop 的 task 提交路径上完全避免了 `synchronized` 和 `ReentrantLock` 的开销。相比 `LinkedBlockingQueue`，在高并发提交场景下吞吐量提升数倍。

- **AtomicFieldUpdater 替代 AtomicXxx 包装类**：`ChannelOutboundBuffer` 的 `totalPendingSize` 使用 `AtomicLongFieldUpdater`，`unwritable` 使用 `AtomicIntegerFieldUpdater`。相比直接使用 `AtomicLong` / `AtomicInteger`，省去了一个包装对象的内存开销（16 字节对象头 + 填充），在大量 Channel 并存时节省可观的内存。

- **volatile 读替代锁的读路径优化**：Pipeline 链表的 `next/prev` 字段声明为 `volatile`，事件传播时的链表遍历只需要 `volatile` 读，不需要获取任何锁。写操作通过 `synchronized(pipeline)` 保护，实现了写时加锁、读时无锁的分离策略。

- **ThreadLocal 缓存避免反射开销**：`ChannelHandlerMask.mask()` 和 `ChannelHandlerAdapter.isSharable()` 都使用 `FastThreadLocal<WeakHashMap>` 缓存反射计算结果。每线程独立的缓存实例消除了缓存本身的并发访问问题，`WeakHashMap` 的弱引用键确保类卸载时不会发生内存泄漏。

- **CAS 自旋替代互斥锁**：`ChannelOutboundBuffer` 的 `setWritable()` / `setUnwritable()` 使用 CAS 循环来原子地翻转可写状态位，避免了在水位变化这种高频路径上使用互斥锁。`SingleThreadEventExecutor` 的状态转换（`ST_NOT_STARTED → ST_STARTED → ST_SHUTTING_DOWN`）同样使用 `AtomicIntegerFieldUpdater.compareAndSet` 实现无锁状态机。

- **位运算优化事件分发**：`ChannelHandlerMask` 将每种事件类型映射为一个位（`MASK_CHANNEL_READ = 1 << 5`），Handler 的 executionMask 是这些位的组合。事件传播时通过 `(ctx.executionMask & mask) == 0` 一次位与运算即可判断是否需要跳过，比传统的 `instanceof` 检查或方法分派快一个数量级。

- **Recycler 对象池减少 GC 压力**：`WriteTask` 和 `ChannelOutboundBuffer.Entry` 都使用 Recycler 对象池。每个线程维护自己的回收栈，归还操作无需跨线程同步。在高吞吐场景下，这一优化将短生命周期对象的分配开销降至接近零。
