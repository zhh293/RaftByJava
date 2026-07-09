# NioEventLoop 线程模型全流程源码解析

> **Netty 源码深度研究系列 · 第 01 篇**
>
> 基于 Netty 主分支源码（含 IoHandler 抽象层重构），从 `new NioEventLoopGroup()` 到 `NioEventLoop.run()` 的无限循环，逐层展开 Netty 的线程模型实现。

---

## 一、调用链总览图

```
new NioEventLoopGroup()
 │
 ├─ super(nThreads=0, executor=null, NioIoHandler.newFactory(), ...)
 │   │
 │   └─ MultithreadEventLoopGroup(nThreads, executor, args)
 │       │
 │       ├─ nThreads == 0 → DEFAULT_EVENT_LOOP_THREADS = CPU核数 × 2
 │       │
 │       └─ MultithreadEventExecutorGroup(nThreads, executor, chooserFactory, args)
 │           │
 │           ├─ executor == null → new ThreadPerTaskExecutor(newDefaultThreadFactory())
 │           │
 │           ├─ children = new EventExecutor[nThreads]
 │           │   └─ for each: newChild(executor, args)
 │           │       └─ new NioEventLoop(group, executor, ioHandlerFactory, ...)
 │           │           ├─ openSelector() → 创建 Selector + SelectedSelectionKeySet 优化
 │           │           ├─ taskQueue = newMpscQueue() → MPSC 无锁队列
 │           │           └─ tailTasks = newMpscQueue()
 │           │
 │           └─ chooser = chooserFactory.newChooser(children)
 │               ├─ isPowerOfTwo? → PowerOfTwoEventExecutorChooser（位运算取模）
 │               └─ otherwise   → GenericEventExecutorChooser（普通取模）
 │
 └─ NioEventLoop 启动（lazy，第一次提交 task 时触发 thread.start()）
     │
     └─ SingleThreadIoEventLoop.run()    ← 无限循环
         │
         do {
         │   ├─ runIo() → NioIoHandler.run(context)
         │   │   ├─ selectStrategy.calculateStrategy()  ← 决定是 select 还是直接处理
         │   │   ├─ select(context, wakenUp)             ← 阻塞等待 IO 事件
         │   │   │   └─ 空轮询 Bug 检测 → rebuildSelector()
         │   │   └─ processSelectedKeys()               ← 处理就绪的 Channel
         │   │       └─ processSelectedKeysOptimized()   ← 数组遍历（替代 HashSet）
         │   │
         │   └─ runAllTasks(maxTaskProcessingQuantumNs)
         │       ├─ fetchFromScheduledTaskQueue()        ← 定时任务 → taskQueue
         │       ├─ 循环执行 taskQueue（每 64 个检查超时）
         │       └─ afterRunningAllTasks()               ← 执行 tailTasks
         } while (!confirmShutdown());
```

---

## 二、逐层展开源码分析

### 2.1 MultithreadEventLoopGroup 的构造过程

#### 2.1.1 默认线程数的确定

当你写 `new NioEventLoopGroup()` 时，传入的 `nThreads` 为 0。Netty 会用默认值替代：

```java
// MultithreadEventLoopGroup.java
public abstract class MultithreadEventLoopGroup
    extends MultithreadEventExecutorGroup implements EventLoopGroup {

    private static final int DEFAULT_EVENT_LOOP_THREADS;

    static {
        DEFAULT_EVENT_LOOP_THREADS = Math.max(1, SystemPropertyUtil.getInt(
                "io.netty.eventLoopThreads",
                NettyRuntime.availableProcessors() * 2));
    }

    protected MultithreadEventLoopGroup(int nThreads, Executor executor, Object... args) {
        super(nThreads == 0 ? DEFAULT_EVENT_LOOP_THREADS : nThreads, executor, args);
    }
}
```

**它是什么**：默认线程数 = `Math.max(1, CPU核数 × 2)`，可通过 JVM 参数 `-Dio.netty.eventLoopThreads=N` 覆盖。

**为什么是 CPU × 2**：Netty 的 EventLoop 是 IO 密集型线程，大部分时间阻塞在 `selector.select()` 上。CPU × 2 是 IO 密集型程序的经验值——当一个线程阻塞在 IO 时，另一个线程可以利用 CPU 处理任务。如果 CPU 核数为 4，创建 8 个 EventLoop 可以充分利用硬件。

**去掉会怎样**：如果不设默认值，用户必须手动计算线程数，容易因设置过大（线程切换开销）或过小（IO 处理不及时）而影响性能。

#### 2.1.2 Executor 的创建

```java
// MultithreadEventExecutorGroup.java
protected MultithreadEventExecutorGroup(int nThreads, Executor executor,
                                        EventExecutorChooserFactory chooserFactory, Object... args) {
    // 如果没有传入 Executor，创建 ThreadPerTaskExecutor
    if (executor == null) {
        executor = new ThreadPerTaskExecutor(newDefaultThreadFactory());
    }
    
    // ... 后续创建 children 数组
}
```

```java
// ThreadPerTaskExecutor.java
public final class ThreadPerTaskExecutor implements Executor {
    private final ThreadFactory threadFactory;

    public ThreadPerTaskExecutor(ThreadFactory threadFactory) {
        this.threadFactory = requireNonNull(threadFactory, "threadFactory");
    }

    @Override
    public void execute(Runnable command) {
        threadFactory.newThread(command).start();
    }
}
```

**它是什么**：`ThreadPerTaskExecutor` 每收到一个任务就创建一个新线程执行。但这不意味着 Netty 会疯狂创建线程——每个 EventLoop 只会调用一次 `executor.execute()`（在首次提交任务时启动自己的线程），此后所有任务都在这个线程中串行执行。

**为什么不用 ThreadPoolExecutor**：EventLoop 的设计理念是"一个 EventLoop = 一个固定线程"。ThreadPoolExecutor 的核心价值是线程复用和池化，但 EventLoop 的线程需要永续运行，不存在"归还到池中"的场景。`ThreadPerTaskExecutor` 简单直接，没有多余的池化开销。

#### 2.1.3 children 数组的创建

```java
// MultithreadEventExecutorGroup.java
children = new EventExecutor[nThreads];

for (int i = 0; i < nThreads; i++) {
    boolean success = false;
    try {
        children[i] = newChild(executor, args);  // 由子类实现
        success = true;
    } catch (Exception e) {
        throw new IllegalStateException("failed to create a child event loop", e);
    } finally {
        if (!success) {
            // 如果有一个创建失败，优雅关闭已创建的所有 EventLoop
            for (int j = 0; j < i; j++) {
                children[j].shutdownGracefully();
            }
        }
    }
}
```

NioEventLoopGroup 的 `newChild()` 实现：

```java
// NioEventLoopGroup.java
@Override
protected IoEventLoop newChild(Executor executor, IoHandlerFactory ioHandlerFactory, Object... args) {
    RejectedExecutionHandler rejectedExecutionHandler = (RejectedExecutionHandler) args[0];
    EventLoopTaskQueueFactory taskQueueFactory = null;
    EventLoopTaskQueueFactory tailTaskQueueFactory = null;
    // ...
    return new NioEventLoop(
            this, executor, ioHandlerFactory,
            taskQueueFactory, tailTaskQueueFactory, rejectedExecutionHandler);
}
```

---

### 2.2 ChooserFactory 如何分配 EventLoop

当一个新的 Channel 需要绑定 EventLoop 时，`EventLoopGroup.next()` 会通过 chooser 轮询选择：

```java
// DefaultEventExecutorChooserFactory.java
public final class DefaultEventExecutorChooserFactory implements EventExecutorChooserFactory {

    public static final DefaultEventExecutorChooserFactory INSTANCE =
        new DefaultEventExecutorChooserFactory();

    @Override
    public EventExecutorChooser newChooser(EventExecutor[] executors) {
        if (isPowerOfTwo(executors.length)) {
            return new PowerOfTwoEventExecutorChooser(executors);
        } else {
            return new GenericEventExecutorChooser(executors);
        }
    }

    private static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }
}
```

**策略一：PowerOfTwoEventExecutorChooser（线程数是 2 的幂次）**

```java
private static final class PowerOfTwoEventExecutorChooser implements EventExecutorChooser {
    private final AtomicInteger idx = new AtomicInteger();
    private final EventExecutor[] executors;

    @Override
    public EventExecutor next() {
        return executors[idx.getAndIncrement() & executors.length - 1];
    }
}
```

**策略二：GenericEventExecutorChooser（线程数不是 2 的幂次）**

```java
private static final class GenericEventExecutorChooser implements EventExecutorChooser {
    private final AtomicLong idx = new AtomicLong();
    private final EventExecutor[] executors;

    @Override
    public EventExecutor next() {
        return executors[(int) Math.abs(idx.getAndIncrement() % executors.length)];
    }
}
```

**为什么要区分两种策略**：当线程数是 2 的幂次时，`idx & (length - 1)` 等价于 `idx % length`，但位运算比取模运算快得多（单个 CPU 指令 vs 除法指令）。在每个新连接都要调用 `next()` 的高并发场景下，这个微优化累积起来很可观。

**为什么 Generic 用 AtomicLong 而 PowerOfTwo 用 AtomicInteger**：`int` 溢出后变为负数，`& (length-1)` 的结果仍然正确（位运算不受符号影响）；但 `% length` 对负数的结果也是负数，会导致数组越界。用 `long` 可以极大延迟溢出的时间点，`Math.abs()` 则处理溢出后的情况。

---

### 2.3 NioEventLoop 的构造

#### 2.3.1 Selector 的创建与优化

NioEventLoop 的 IO 处理委托给 `NioIoHandler`，Selector 在其中创建：

```java
// NioIoHandler.java
private SelectorTuple openSelector() {
    final Selector unwrappedSelector;
    try {
        unwrappedSelector = provider.openSelector();
    } catch (IOException e) {
        throw new ChannelException("failed to open a new selector", e);
    }

    if (DISABLE_KEY_SET_OPTIMIZATION) {
        return new SelectorTuple(unwrappedSelector);
    }

    // ★ 核心优化：用数组替代 Selector 内部的 HashSet
    final SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

    // 通过反射或 Unsafe 替换 SelectorImpl 内部的 selectedKeys 字段
    Object maybeSelectorImplClass = AccessController.doPrivileged(
        new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    return Class.forName("sun.nio.ch.SelectorImpl", false,
                            PlatformDependent.getSystemClassLoader());
                } catch (Throwable cause) {
                    return cause;
                }
            }
        });

    // ... 反射替换逻辑
    // Java 9+ 使用 PlatformDependent.putObject() (Unsafe)
    if (PlatformDependent.javaVersion() >= 9 && PlatformDependent.hasUnsafe()) {
        long selectedKeysFieldOffset =
            PlatformDependent.objectFieldOffset(selectedKeysField);
        long publicSelectedKeysFieldOffset =
            PlatformDependent.objectFieldOffset(publicSelectedKeysField);
        PlatformDependent.putObject(unwrappedSelector,
            selectedKeysFieldOffset, selectedKeySet);
        PlatformDependent.putObject(unwrappedSelector,
            publicSelectedKeysFieldOffset, selectedKeySet);
    }
    // ...
}
```

#### 2.3.2 SelectedSelectionKeySet — 数组替代 HashSet

```java
// SelectedSelectionKeySet.java
final class SelectedSelectionKeySet extends AbstractSet<SelectionKey> {

    SelectionKey[] keys;
    int size;

    SelectedSelectionKeySet() {
        keys = new SelectionKey[1024];
    }

    @Override
    public boolean add(SelectionKey o) {
        if (o == null) {
            return false;
        }
        if (size == keys.length) {
            increaseCapacity();
        }
        keys[size++] = o;  // O(1) 数组尾追加
        return true;
    }

    @Override
    public boolean remove(Object o) {
        return false;  // 不支持单个删除
    }

    void reset() {
        reset(0);
    }

    void reset(int start) {
        Arrays.fill(keys, start, size, null);  // 清空引用，帮助 GC
        size = 0;
    }

    private void increaseCapacity() {
        SelectionKey[] newKeys = new SelectionKey[keys.length << 1];  // 2倍扩容
        System.arraycopy(keys, 0, newKeys, 0, size);
        keys = newKeys;
    }
}
```

**它是什么**：JDK `SelectorImpl` 内部用 `HashSet<SelectionKey>` 存放就绪的 key。Netty 通过反射将其替换为基于数组的 `SelectedSelectionKeySet`。

**为什么存在**：`HashSet.add()` 需要计算哈希值、处理冲突、可能触发 rehash；遍历时需要创建 Iterator 对象。而数组的 `add()` 是 O(1) 的尾追加，遍历是连续内存访问（CPU cache 友好）。在每次 `selector.select()` 返回后都要遍历所有就绪 key 的高频场景下，这个优化显著减少了开销。

**去掉会怎样**：功能不受影响，但在高并发场景下（每秒数万次 select），HashSet 的哈希计算和内存跳转会成为可测量的性能瓶颈。

#### 2.3.3 taskQueue 的选型 — MPSC 无锁队列

```java
// SingleThreadIoEventLoop.java
@Override
protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
    return newTaskQueue0(maxPendingTasks);
}

protected static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
    return maxPendingTasks == Integer.MAX_VALUE
        ? PlatformDependent.<Runnable>newMpscQueue()
        : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
}
```

底层使用 JCTools 的 `MpscChunkedArrayQueue` 或 `MpscUnboundedArrayQueue`：

```java
// PlatformDependent.java → Mpsc 内部类
static <T> Queue<T> newChunkedMpscQueue(final int chunkSize, final int capacity) {
    return USE_MPSC_CHUNKED_ARRAY_QUEUE
        ? new MpscChunkedArrayQueue<T>(chunkSize, capacity)
        : new MpscChunkedAtomicArrayQueue<T>(chunkSize, capacity);
}
```

**它是什么**：MPSC = Multiple Producer, Single Consumer（多生产者单消费者）无锁队列。

**为什么选 MPSC**：这完美匹配 EventLoop 的使用场景：

- 多个外部线程可以提交任务（如用户线程调用 `channel.write()`，Netty 会封装为 task 提交到 EventLoop）→ Multiple Producer
- 只有 EventLoop 自己的线程消费任务 → Single Consumer

MPSC 队列利用这个"只有一个消费者"的约束，消除了消费端的 CAS 竞争，比通用的 `ConcurrentLinkedQueue`（MPMC）更高效。

**去掉会怎样**：替换为 `LinkedBlockingQueue` 或 `ConcurrentLinkedQueue` 功能上可行，但前者每次 `put/take` 都需要加锁，后者在消费端有不必要的 CAS 操作。在百万级消息吞吐的场景下，队列操作的效率直接影响整体性能。

---

### 2.4 NioEventLoop.run() 主循环的三件事

主循环位于 `SingleThreadIoEventLoop.run()`，它做三件事：**select → processSelectedKeys → runAllTasks**。

```java
// SingleThreadIoEventLoop.java
@Override
protected void run() {
    assert inEventLoop();
    ioHandler.initialize();
    do {
        // 第 1+2 件事：执行 IO（select + processSelectedKeys）
        runIo();

        if (isShuttingDown()) {
            ioHandler.prepareToDestroy();
        }

        // 第 3 件事：执行任务队列中的所有任务
        runAllTasks(maxTaskProcessingQuantumNs);

    } while (!confirmShutdown() && !canSuspend());
}
```

#### 2.4.1 第一件事：select() — 等待 IO 事件

```java
// NioIoHandler.java
private void select(IoHandlerContext runner, boolean oldWakenUp) throws IOException {
    Selector selector = this.selector;
    try {
        int selectCnt = 0;
        long currentTimeNanos = System.nanoTime();
        final long delayNanos = runner.delayNanos(currentTimeNanos);

        for (;;) {
            // 计算 select 超时时间（基于最近的定时任务到期时间）
            final long timeoutMillis;
            if (delayNanos != Long.MAX_VALUE) {
                long millisBeforeDeadline = millisBeforeDeadline(
                    selectDeadLineNanos, currentTimeNanos);
                if (millisBeforeDeadline <= 0) {
                    // 已有定时任务到期，不阻塞
                    if (selectCnt == 0) {
                        selector.selectNow();
                        selectCnt = 1;
                    }
                    break;
                }
                timeoutMillis = millisBeforeDeadline;
            } else {
                timeoutMillis = 0;  // 无定时任务时可以无限期阻塞
            }

            // 在 select 前检查是否有新任务到来
            if (!runner.canBlock() && wakenUp.compareAndSet(false, true)) {
                selector.selectNow();
                selectCnt = 1;
                break;
            }

            // ★ 核心：阻塞等待 IO 事件
            int selectedKeys = selector.select(timeoutMillis);
            selectCnt++;

            // 有事件、被唤醒、或有新任务到达 → 退出 select 循环
            if (selectedKeys != 0 || oldWakenUp || wakenUp.get()
                || !runner.canBlock()) {
                break;
            }

            // ★ 空轮询 Bug 检测（下文详述）
            long time = System.nanoTime();
            if (time - TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
                >= currentTimeNanos) {
                selectCnt = 1;  // 正常超时返回，重置计数
            } else if (SELECTOR_AUTO_REBUILD_THRESHOLD > 0 &&
                       selectCnt >= SELECTOR_AUTO_REBUILD_THRESHOLD) {
                // 连续空轮询达到阈值 → 重建 Selector
                selector = selectRebuildSelector(selectCnt);
                selectCnt = 1;
                break;
            }

            currentTimeNanos = time;
        }
    } catch (CancelledKeyException e) {
        // 忽略
    }
}
```

**canBlock() 的判断逻辑**：

```java
// SingleThreadIoEventLoop.java 中的 IoHandlerContext
@Override
public boolean canBlock() {
    assert inEventLoop();
    return !hasTasks() && !hasScheduledTasks();
}
```

只有当没有待执行的普通任务、也没有到期的定时任务时，才允许 `select()` 阻塞等待。否则用 `selectNow()` 非阻塞立即返回，以便尽快处理任务。

#### 2.4.2 空轮询 Bug 检测与 Rebuild Selector

**Bug 背景**：JDK NIO 在 Linux 上有一个臭名昭著的 epoll bug（[JDK-6670302](https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6670302)）——`Selector.select(timeout)` 在没有任何就绪事件的情况下提前返回（不等到超时），导致 EventLoop 的 `for(;;)` 循环空转，CPU 飙升到 100%。

**Netty 的检测策略**：

```java
// 默认阈值：512
private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;

static {
    int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt(
            "io.netty.selectorAutoRebuildThreshold", 512);
    if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
        selectorAutoRebuildThreshold = 0;
    }
    SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;
}
```

检测逻辑：如果 `select(timeout)` 在未超时的情况下返回 0（无就绪事件），连续累计达到 512 次，就判定为 epoll bug，触发 Selector 重建。

**重建过程**：

```java
// NioIoHandler.java
void rebuildSelector0() {
    final Selector oldSelector = selector;
    final SelectorTuple newSelectorTuple;

    try {
        newSelectorTuple = openSelector();  // 创建全新的 Selector
    } catch (Exception e) {
        logger.warn("Failed to create a new Selector.", e);
        return;
    }

    // 将所有 Channel 从旧 Selector 迁移到新 Selector
    int nChannels = 0;
    for (SelectionKey key : oldSelector.keys()) {
        DefaultNioRegistration handle = (DefaultNioRegistration) key.attachment();
        try {
            if (!key.isValid() ||
                key.channel().keyFor(newSelectorTuple.unwrappedSelector) != null) {
                continue;
            }
            handle.register(newSelectorTuple.unwrappedSelector);
            nChannels++;
        } catch (Exception e) {
            logger.warn("Failed to re-register a NioHandle to the new Selector.", e);
            handle.cancel();
        }
    }

    selector = newSelectorTuple.selector;
    unwrappedSelector = newSelectorTuple.unwrappedSelector;

    try {
        oldSelector.close();  // 关闭有 bug 的旧 Selector
    } catch (Throwable t) {
        logger.warn("Failed to close the old Selector.", t);
    }

    logger.info("Migrated " + nChannels + " channel(s) to the new Selector.");
}
```

**为什么 512 次**：太小容易误判（正常情况下偶尔的早返回是允许的），太大则检测太慢。512 是一个经验值，意味着在没有任何事件的情况下连续空转 512 次才判定为 bug。

#### 2.4.3 第二件事：processSelectedKeys() — 处理就绪的 Channel

```java
// NioIoHandler.java
private int processSelectedKeys() {
    if (selectedKeys != null) {
        return processSelectedKeysOptimized();  // ★ 优化版：数组遍历
    } else {
        return processSelectedKeysPlain(selector.selectedKeys());  // 降级版：HashSet 遍历
    }
}

private int processSelectedKeysOptimized() {
    int handled = 0;
    for (int i = 0; i < selectedKeys.size; ++i) {
        final SelectionKey k = selectedKeys.keys[i];
        selectedKeys.keys[i] = null;  // 置空引用，帮助 GC

        processSelectedKey(k);
        ++handled;

        if (needsToSelectAgain) {
            // 有 Channel 注销，需要重新 select 以清理无效 key
            selectedKeys.reset(i + 1);
            selectAgain();
            i = -1;  // 从头开始遍历
        }
    }
    return handled;
}

private void processSelectedKey(SelectionKey k) {
    final DefaultNioRegistration registration =
        (DefaultNioRegistration) k.attachment();
    if (!registration.isValid()) {
        try {
            registration.handle.close();
        } catch (Exception e) {
            // log
        }
        return;
    }
    // 将就绪操作分发给具体的 NioHandle（如 NioServerSocketChannel、NioSocketChannel）
    registration.handle(k.readyOps());
}
```

**数组遍历 vs HashSet 遍历的优势**：

| 对比项 | HashSet (JDK 原生) | 数组 (Netty 优化) |
|--------|-------------------|------------------|
| add() 复杂度 | O(1) 均摊，需哈希计算 | O(1)，直接数组尾追加 |
| 遍历方式 | Iterator，涉及 modCount 检查 | 简单 for 循环，连续内存 |
| CPU Cache | 哈希桶跳转，cache miss 多 | 连续数组访问，cache 友好 |
| 对象分配 | 每次遍历创建 Iterator | 无额外对象 |

#### 2.4.4 ioRatio 的作用

在经典的 Netty 4.1.x 版本中，`ioRatio` 控制 IO 处理时间与任务执行时间的配比：

```java
// 经典 NioEventLoop.run() 中的 ioRatio 逻辑（4.1.x 版本）
final int ioRatio = this.ioRatio;  // 默认 50
if (ioRatio == 100) {
    processSelectedKeys();
    runAllTasks();
} else {
    final long ioStartTime = System.nanoTime();
    processSelectedKeys();
    final long ioTime = System.nanoTime() - ioStartTime;
    // 任务执行时间 = IO 时间 × (100 - ioRatio) / ioRatio
    runAllTasks(ioTime * (100 - ioRatio) / ioRatio);
}
```

`ioRatio = 50` 表示 IO 和任务各占 50% 的时间。如果 IO 花了 10ms，那任务最多也执行 10ms。

**在新版 Netty 中，ioRatio 已被移除**，替换为 `maxTaskProcessingQuantumNs`（默认 1000ms），直接限制任务执行的绝对时间上限：

```java
// NioEventLoop.java (新版)
public int getIoRatio() {
    return 0;  // Always return 0
}

@Deprecated
public void setIoRatio(int ioRatio) {
    logger.debug("NioEventLoop.setIoRatio(int) logic was removed, this is a no-op");
}
```

#### 2.4.5 第三件事：runAllTasks(timeout) — 任务调度逻辑

```java
// SingleThreadEventExecutor.java
protected boolean runAllTasks(long timeoutNanos) {
    // 1. 将已到期的定时任务从 scheduledTaskQueue 转移到 taskQueue
    fetchFromScheduledTaskQueue(taskQueue);

    // 2. 取出第一个任务
    Runnable task = pollTask();
    if (task == null) {
        afterRunningAllTasks();  // 执行 tailTasks
        return false;
    }

    // 3. 计算截止时间
    final long deadline = timeoutNanos > 0
        ? getCurrentTimeNanos() + timeoutNanos : 0;
    long runTasks = 0;
    long lastExecutionTime;

    for (;;) {
        safeExecute(task);  // 安全执行：捕获所有异常，防止一个 task 的异常影响后续 task

        runTasks++;

        // ★ 每执行 64 个任务检查一次是否超时
        // nanoTime() 调用有一定开销（系统调用），不宜每个 task 都检查
        if ((runTasks & 0x3F) == 0) {
            lastExecutionTime = getCurrentTimeNanos();
            if (lastExecutionTime >= deadline) {
                break;  // 超时退出，让出时间给 IO
            }
        }

        task = pollTask();
        if (task == null) {
            lastExecutionTime = getCurrentTimeNanos();
            break;  // 任务队列空了
        }
    }

    afterRunningAllTasks();  // 执行 tailTasks
    this.lastExecutionTime = lastExecutionTime;
    return true;
}
```

**三级任务队列**：

```
scheduledTaskQueue (PriorityQueue<ScheduledFutureTask>)
      │ fetchFromScheduledTaskQueue()
      ▼
taskQueue (MpscQueue)  ← 外部线程和 EventLoop 都往这里提交
      │ pollTask()
      ▼
执行任务
      │
      ▼
tailTasks (MpscQueue)  ← afterRunningAllTasks() 中执行，用于收尾工作
```

**为什么每 64 个任务才检查一次超时**：`System.nanoTime()` 需要发起系统调用，开销约 100-200ns。如果每个 task 都检查，当 task 本身执行时间很短（如几十纳秒的 CAS 操作）时，检查超时的开销反而比任务本身还大。64 是一个平衡值——`0x3F` 的位运算判断也是零开销的。

---

## 三、为什么 Netty 自己实现线程模型，不用 JDK ThreadPoolExecutor

JDK 的 `ThreadPoolExecutor` 是一个通用的线程池，它的设计目标是"提交任务 → 任意线程执行"。但 Netty 的需求完全不同：

| 对比维度 | JDK ThreadPoolExecutor | Netty EventLoop |
|---------|----------------------|-----------------|
| 线程与任务的关系 | 任意线程可以执行任意任务 | 每个 Channel 绑定固定线程 |
| 线程安全模型 | 需要锁保护共享状态 | 线程封闭，无需锁 |
| 任务队列 | BlockingQueue（通用） | MpscQueue（专用无锁） |
| IO 集成 | 不支持 | 深度集成 Selector |
| 定时任务 | 需要 ScheduledThreadPoolExecutor | 内置 scheduledTaskQueue |
| 生命周期 | 线程可回收/创建 | 线程永续运行 |

核心矛盾在于：ThreadPoolExecutor 的"任意线程执行任务"特性恰恰是 Netty 要避免的——它会引入线程安全问题。Netty 通过"Channel 绑定 EventLoop"实现了线程封闭（Thread Confinement），从根本上消除了对 Channel 操作的并发竞争。

---

## 四、Reactor 模式的三种变体

### 4.1 单线程 Reactor

```
┌──────────────────────────────────┐
│         EventLoop (1个线程)        │
│  accept + read + decode +        │
│  process + encode + write        │
└──────────────────────────────────┘
```

所有操作在一个线程中完成。适合连接数少、业务逻辑简单的场景。

### 4.2 多线程 Reactor

```
┌──────────────────────────────────┐
│         EventLoop (1个线程)        │
│  accept + read + write           │
├──────────────────────────────────┤
│      Worker ThreadPool           │
│  decode + process + encode       │
└──────────────────────────────────┘
```

一个线程处理 IO，耗时的业务逻辑交给工作线程池。

### 4.3 主从多线程 Reactor（Netty 的选择）

```
┌─────────────────────┐    ┌─────────────────────────────────┐
│  Boss EventLoopGroup │    │     Worker EventLoopGroup        │
│  (1-N 个线程)         │    │     (M 个线程)                    │
│                     │    │                                 │
│  accept 新连接        │───▶│  read + decode + process +      │
│                     │    │  encode + write                 │
└─────────────────────┘    └─────────────────────────────────┘
```

**Netty 属于主从多线程 Reactor**：

- **Boss EventLoopGroup**：负责接收新连接（accept），通常 1 个线程就够（因为 accept 操作很轻量）
- **Worker EventLoopGroup**：负责已建立连接的 IO 读写和业务处理，通常 CPU × 2 个线程
- 新连接被 accept 后，Boss 将其注册到 Worker 的某个 EventLoop 上，此后该连接的所有 IO 操作都在这个 EventLoop 线程中执行

**为什么选择主从模式**：

- 单线程模式：accept 和 IO 处理在同一线程，高并发时 accept 会被 IO 处理阻塞，新连接排队等待
- 多线程模式：accept 和 IO 分离，但 accept 仍是单线程瓶颈
- 主从模式：accept 由专门的线程组处理，IO 由另一组线程处理，职责清晰，可独立调优

---

## 五、一次 IO 事件从 Selector 唤醒到 Handler 处理的完整时序图

```
                        EventLoop 线程
                            │
                            ▼
                   ┌─────────────────┐
                   │  selector.select()  │  ← 阻塞等待，直到有 IO 事件
                   └────────┬────────┘
                            │ 网卡收到数据，内核唤醒 epoll
                            ▼
                   ┌─────────────────┐
                   │ processSelectedKeys │
                   └────────┬────────┘
                            │ 遍历 selectedKeys 数组
                            ▼
                   ┌─────────────────┐
                   │ processSelectedKey(k) │
                   │ k.readyOps() → OP_READ │
                   └────────┬────────┘
                            │ attachment 是 NioSocketChannel
                            ▼
                   ┌─────────────────────┐
                   │ NioSocketChannel      │
                   │   .unsafe()           │
                   │   .read()             │
                   └────────┬─────────────┘
                            │
                            ▼
                   ┌─────────────────────┐
                   │ allocate ByteBuf       │  ← 从池中分配内存
                   │ doReadBytes(byteBuf)   │  ← JDK SocketChannel.read(buffer)
                   └────────┬─────────────┘
                            │ 数据读入 ByteBuf
                            ▼
                   ┌──────────────────────┐
                   │ pipeline.fireChannelRead │  ← 触发入站事件
                   │         (byteBuf)        │
                   └────────┬─────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   ┌─────────┐      ┌──────────────┐    ┌──────────────┐
   │ HeadCtx  │ ──▶  │ DecoderHandler │ ──▶ │ BusinessHandler│
   │ (入站起点) │      │ (解码: bytes→obj)│    │ (业务处理)      │
   └─────────┘      └──────────────┘    └──────────────┘
        │                                       │
        │              Pipeline 入站方向           │
        │           head ──────────▶ tail        │
        │                                       │
        │                                       ▼
        │                               ctx.writeAndFlush(resp)
        │                                       │
        │              Pipeline 出站方向           │
        │           head ◀──────────── tail      │
        │                                       │
        ▼                   ▼                   ▼
   ┌─────────┐      ┌──────────────┐    ┌──────────────┐
   │ HeadCtx  │ ◀──  │ EncoderHandler │ ◀── │ TailCtx      │
   │ (出站终点) │      │ (编码: obj→bytes)│    │ (出站起点)    │
   └────┬────┘      └──────────────┘    └──────────────┘
        │
        ▼
   ┌──────────────────────┐
   │ unsafe.write()         │  ← 数据写入 ChannelOutboundBuffer
   │ unsafe.flush()         │  ← 触发 JDK SocketChannel.write()
   └──────────────────────┘
        │
        ▼
    数据通过网卡发出
```

---

## 六、本篇涉及的设计模式

**Reactor 模式**：整个 EventLoop 的设计核心——`Selector` 多路复用检测 IO 事件，`EventLoop` 线程分发处理。`NioIoHandler.run()` 中的 select → process → dispatch 就是经典的 Reactor 循环。Netty 的 Boss/Worker 分离是主从 Reactor 的实现。

**策略模式（Strategy）**：`DefaultEventExecutorChooserFactory` 根据线程数是否为 2 的幂次，选择 `PowerOfTwoEventExecutorChooser` 或 `GenericEventExecutorChooser` 两种轮询策略。`SelectStrategy` 接口也是策略模式，决定是阻塞 select 还是直接处理。

**工厂模式（Factory）**：`NioIoHandler.newFactory()` 返回 `IoHandlerFactory`，用于创建 `NioIoHandler` 实例；`DefaultThreadFactory` 创建 `FastThreadLocalThread`；`EventExecutorChooserFactory` 创建 Chooser。工厂模式贯穿整个构造过程。

**模板方法模式（Template Method）**：`MultithreadEventExecutorGroup` 定义了创建 EventLoop 的流程框架（创建 Executor → 循环调用 newChild() → 创建 Chooser），`newChild()` 是抽象方法，由 `NioEventLoopGroup` 等子类实现具体的 EventLoop 创建逻辑。

**单例模式（Singleton）**：`DefaultEventExecutorChooserFactory.INSTANCE` 是典型的饿汉式单例。

**观察者模式（Observer）**：`terminationListener` 注册在每个 child EventLoop 的 `terminationFuture()` 上，当所有 EventLoop 都终止时，通知 Group 级别的 `terminationFuture` 完成。

---

## 七、本篇涉及的高性能并发技术

**无锁 CAS（Compare-And-Swap）**：`PowerOfTwoEventExecutorChooser` 的 `AtomicInteger.getAndIncrement()` 使用 CAS 实现无锁轮询计数，避免了锁竞争。`wakenUp` 字段（`AtomicBoolean`）也通过 CAS 控制 Selector 的唤醒状态。解决的瓶颈：高并发注册 Channel 时的轮询分配。

**线程封闭（Thread Confinement）**：每个 Channel 绑定一个 EventLoop 线程，Channel 的所有 IO 操作和 Pipeline 事件传播都在该线程中执行。这从根本上消除了对 Channel 状态的并发竞争，无需任何锁。解决的瓶颈：传统多线程模型中 Channel 状态的同步开销。

**MPSC 无锁队列**：EventLoop 的 taskQueue 使用 JCTools 的 `MpscChunkedArrayQueue`，利用"只有一个消费者"的约束消除消费端 CAS。解决的瓶颈：多线程向 EventLoop 提交任务时的队列竞争。

**数组替代哈希集合**：`SelectedSelectionKeySet` 用数组替代 `HashSet`，将 `add()` 操作从哈希计算降为数组尾追加，遍历从哈希桶跳转变为连续内存访问（CPU cache 友好）。解决的瓶颈：每次 `select()` 后遍历就绪 key 的开销。

**位运算优化**：`PowerOfTwoEventExecutorChooser` 使用 `idx & (length - 1)` 替代 `idx % length`，将取模运算降为单个 AND 指令。`runAllTasks()` 中 `(runTasks & 0x3F) == 0` 用位运算替代 `runTasks % 64 == 0` 来判断是否检查超时。解决的瓶颈：高频调用路径上的除法运算开销。

**空轮询检测与自愈**：通过计数器检测 JDK NIO 的 epoll 空轮询 bug，达到 512 次阈值后自动重建 Selector。这是一种"检测 → 恢复"的容错机制，避免了 CPU 100% 空转。解决的瓶颈：JDK NIO 的已知 bug 导致的 CPU 浪费。

**延迟超时检查**：`runAllTasks()` 每执行 64 个任务才调用一次 `System.nanoTime()` 检查超时，减少系统调用频率。解决的瓶颈：`nanoTime()` 的系统调用开销（100-200ns/次）在密集小任务场景下成为瓶颈。

**空间换时间（FastThreadLocal 预备）**：Netty 创建的线程默认是 `FastThreadLocalThread`，内置 `InternalThreadLocalMap`（数组直接下标访问），为后续所有 `FastThreadLocal` 的 O(1) 访问做准备。解决的瓶颈：JDK `ThreadLocal` 的线性探测哈希冲突。
