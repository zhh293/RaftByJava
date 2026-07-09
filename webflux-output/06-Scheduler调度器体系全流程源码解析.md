# Scheduler 调度器体系全流程源码解析

> **Reactor Core 源码深度研究系列 · 第 06 篇**
> 从接口抽象到四种内置实现，拆解 Reactor 调度器的线程模型、池化策略与生命周期管理。

---

## 一、全局架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Schedulers (工厂 + 缓存)                        │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │  immediate()  │  │   single()   │  │  parallel()  │  │boundedElast │ │
│  │              │  │              │  │              │  │   ic()      │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬──────┘ │
│         │                 │                 │                  │        │
│  ┌──────▼───────┐  ┌──────▼───────┐  ┌──────▼───────┐  ┌─────▼──────┐ │
│  │Immediate     │  │Single        │  │Parallel      │  │BoundedElast│ │
│  │Scheduler     │  │Scheduler     │  │Scheduler     │  │icScheduler │ │
│  │(无状态单例)   │  │(1线程)       │  │(N线程固定池) │  │(动态线程池)│ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘ │
│         │                 │                 │                 │        │
│         │                 │                 │                 │        │
│         ▼                 ▼                 ▼                 ▼        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                  Scheduler 接口                                  │  │
│  │  schedule(Runnable)                                              │  │
│  │  schedule(Runnable, delay, unit)                                 │  │
│  │  schedulePeriodically(Runnable, initialDelay, period, unit)      │  │
│  │  createWorker() → Worker                                         │  │
│  │  init() / dispose() / disposeGracefully()                       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                               │                                        │
│                               ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                  Worker 接口                                     │  │
│  │  schedule(Runnable)                                              │  │
│  │  schedule(Runnable, delay, unit)                                 │  │
│  │  schedulePeriodically(Runnable, initialDelay, period, unit)      │  │
│  │  dispose()                                                       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  辅助类:                                                                │
│  ┌────────────────┐ ┌──────────────────┐ ┌──────────────────────────┐  │
│  │ReactorThread   │ │SchedulerState<T> │ │ExecutorServiceWorker     │  │
│  │Factory         │ │(CAS 状态管理)    │ │(通用 Worker 实现)         │  │
│  │(含NonBlocking  │ │                  │ │                          │  │
│  │ Thread)        │ │                  │ │                          │  │
│  └────────────────┘ └──────────────────┘ └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 二、Scheduler 接口：调度边界的抽象

### 2.1 接口定义

Reactor 的调度器体系建立在 `Scheduler` 接口之上。这个接口定义在 `reactor/core/scheduler/Scheduler.java`，它提供了向操作符注入异步边界的能力。

源码文件：`reactor/core/scheduler/Scheduler.java`
```java
public interface Scheduler extends Disposable {

    Disposable schedule(Runnable task);

    default Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        throw Exceptions.failWithRejectedNotTimeCapable();
    }

    default Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        throw Exceptions.failWithRejectedNotTimeCapable();
    }

    Worker createWorker();

    default void init() {
        start();
    }

    default void dispose() {
    }

    default Mono<Void> disposeGracefully() {
        return Mono.fromRunnable(this::dispose);
    }
}
```

这里有几个设计选择值得深入分析：

**为什么 `schedule(Runnable, long, TimeUnit)` 是 default 方法且默认抛异常？** 因为并非所有调度器都是时间感知的（time-capable）。`ImmediateScheduler` 和 `ExecutorScheduler` 就不支持延迟调度。如果把延迟调度设计为抽象方法，这些调度器就必须实现一个无意义的方法体。用 default + 抛异常的方式，调用者在使用前就能知道该调度器不支持此能力。去掉这个设计会怎样？如果改成抽象方法，`ImmediateScheduler` 就必须实现一个永远抛异常的方法，代码表达力反而更差。

**为什么 `createWorker()` 是必须实现的抽象方法？** 因为 Worker 是操作符（如 `publishOn`、`subscribeOn`）执行任务的核心单元。每个 Worker 代表一个独立的异步边界，保证提交到同一 Worker 的任务按 FIFO 顺序执行。如果去掉 Worker 抽象，操作符就只能直接调 `schedule()`，而 `schedule()` 不保证顺序性——这对数据流的有序投递是灾难性的。

### 2.2 Worker 内部接口

源码文件：`reactor/core/scheduler/Scheduler.java`
```java
interface Worker extends Disposable {
    Disposable schedule(Runnable task);

    default Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        throw Exceptions.failWithRejectedNotTimeCapable();
    }

    default Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        throw Exceptions.failWithRejectedNotTimeCapable();
    }
}
```

Worker 与 Scheduler 的关系是一对多：一个 Scheduler 可以创建多个 Worker，每个 Worker 绑定到一个执行资源。Worker 的 `dispose()` 释放它占用的资源，而 Scheduler 的 `dispose()` 释放所有底层资源。

---

## 三、Schedulers 工厂：缓存、常量与全局配置

### 3.1 三大默认常量

`Schedulers` 类是 Reactor 调度器体系的入口。它定义了三个关键的默认值常量。

源码文件：`reactor/core/scheduler/Schedulers.java`
```java
public static final int DEFAULT_POOL_SIZE =
    Optional.ofNullable(System.getProperty("reactor.schedulers.defaultPoolSize"))
            .map(Integer::parseInt)
            .orElseGet(() -> Runtime.getRuntime().availableProcessors());

public static final int DEFAULT_BOUNDED_ELASTIC_SIZE =
    Optional.ofNullable(System.getProperty("reactor.schedulers.defaultBoundedElasticSize"))
            .map(Integer::parseInt)
            .orElseGet(() -> 10 * Runtime.getRuntime().availableProcessors());

public static final int DEFAULT_BOUNDED_ELASTIC_QUEUESIZE =
    Optional.ofNullable(System.getProperty("reactor.schedulers.defaultBoundedElasticQueueSize"))
            .map(Integer::parseInt)
            .orElse(100000);
```

#### DEFAULT_POOL_SIZE = Runtime.availableProcessors()

**为什么等于 CPU 核数？** 这源于 CPU 密集型工作的经典理论：对于不涉及阻塞的纯计算任务，线程数等于 CPU 核数时吞吐量最优，因为没有线程上下文切换的浪费。`parallel()` 调度器正是为这类场景设计的。

**去掉系统属性覆盖会怎样？** 在容器化环境中，`Runtime.availableProcessors()` 可能返回宿主机的核数而不是容器的 CPU 配额。此时需要通过 `-Dreactor.schedulers.defaultPoolSize=4` 手动覆盖。如果去掉这个能力，容器中的 Reactor 应用可能创建远超实际可用 CPU 的线程池，导致过度竞争。

#### DEFAULT_BOUNDED_ELASTIC_SIZE = 10 * availableProcessors()

**为什么是 10 倍？** `boundedElastic()` 用于阻塞式 I/O 操作（如数据库查询、文件读写）。阻塞操作的特点是线程大部分时间在等待 I/O，CPU 利用率很低。10 倍的系数允许有足够的并发度来覆盖 I/O 等待。这个值是经验值——足够大以处理常见的阻塞场景，又不会无限制地创建线程。

**如果设成 `Integer.MAX_VALUE` 会怎样？** 那就退化为无界的线程池，失去了"bounded"的保护。当上游持续以高速率产生阻塞任务时，线程数会无限增长，直到 OOM。这正是 Reactor 3.x 弃用旧的 `elastic()` 调度器的原因。

#### DEFAULT_BOUNDED_ELASTIC_QUEUESIZE = 100000

**为什么每个线程的排队上限是 10 万？** 这是背压安全的最后一道防线。当所有线程都繁忙时，新任务会排入队列。如果队列无界，内存会持续增长。10 万是一个折衷值：对大多数应用来说足够大，不会因为瞬时的任务尖峰而拒绝执行；又小到能在内存泄漏场景下提供及时的 `RejectedExecutionException` 报警。

### 3.2 CachedScheduler：共享实例的线程安全缓存

源码文件：`reactor/core/scheduler/Schedulers.java`
```java
static AtomicReference<@Nullable CachedScheduler> CACHED_BOUNDED_ELASTIC = new AtomicReference<>();
static AtomicReference<@Nullable CachedScheduler> CACHED_PARALLEL = new AtomicReference<>();
static AtomicReference<@Nullable CachedScheduler> CACHED_SINGLE = new AtomicReference<>();

static CachedScheduler cache(AtomicReference<@Nullable CachedScheduler> reference,
        String key, Supplier<Scheduler> supplier) {
    CachedScheduler s = reference.get();
    if (s != null) {
        return s;
    }
    s = new CachedScheduler(key, supplier.get());
    if (reference.compareAndSet(null, s)) {
        return s;
    }
    CachedScheduler other = reference.get();
    assert other != null : "CachedScheduler should not be null after failed CAS";
    s._dispose();
    return other;
}
```

`CachedScheduler` 包装了真实的 `Scheduler`，它的 `dispose()` 方法被重写为空操作：

```java
@Override
public void dispose() {
}

void _dispose() {
    cached.dispose();
}
```

**为什么 `dispose()` 是空操作？** 因为 `CachedScheduler` 是全局共享的。如果任何一个调用者能 dispose 它，其他依赖该调度器的操作符就会收到 `RejectedExecutionException`。只有通过 `Schedulers.shutdownNow()` 或 `setFactory()` 才能触发真正的 `_dispose()`。这是典型的"受保护的单例"模式。

### 3.3 Factory 接口与虚拟线程支持

源码文件：`reactor/core/scheduler/Schedulers.java`
```java
public static final boolean DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS =
    Optional.ofNullable(System.getProperty("reactor.schedulers.defaultBoundedElasticOnVirtualThreads"))
            .map(Boolean::parseBoolean)
            .orElse(false);
```

`Factory` 接口是 Scheduler 创建的扩展点：

```java
public interface Factory {
    default Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
        return new BoundedElasticScheduler(threadCap, queuedTaskCap, threadFactory, ttlSeconds);
    }

    default Scheduler newThreadPerTaskBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory) {
        return new BoundedElasticThreadPerTaskScheduler(threadCap, queuedTaskCap, threadFactory);
    }

    default Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
        return new ParallelScheduler(parallelism, threadFactory);
    }

    default Scheduler newSingle(ThreadFactory threadFactory) {
        return new SingleScheduler(threadFactory);
    }
}
```

当 `DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS` 为 `true` 且运行在 Java 21+ 上时，`BoundedElasticSchedulerSupplier`（JDK 21 变体）会调用 `Factory.newThreadPerTaskBoundedElastic()`，创建 `BoundedElasticThreadPerTaskScheduler`，它为每个任务分配一个虚拟线程。在 JDK 8/11/17 环境下，`VirtualThreadFactory` 的构造器直接抛出 `UnsupportedOperationException`。

---

## 四、四种内置调度器深度剖析

### 4.1 ImmediateScheduler：零开销的"空对象"

源码文件：`reactor/core/scheduler/ImmediateScheduler.java`
```java
final class ImmediateScheduler implements Scheduler, Scannable {

    private static final ImmediateScheduler INSTANCE;

    static {
        INSTANCE = new ImmediateScheduler();
        INSTANCE.init();
    }

    public static Scheduler instance() {
        return INSTANCE;
    }

    private ImmediateScheduler() {
    }

    static final Disposable FINISHED = Disposables.disposed();

    @Override
    public Disposable schedule(Runnable task) {
        task.run();
        return FINISHED;
    }

    @Override
    public void dispose() {
        //NO-OP
    }

    @Override
    public Worker createWorker() {
        return new ImmediateSchedulerWorker();
    }
}
```

**核心特征：** 调用 `schedule(task)` 时直接在当前线程执行 `task.run()`，不做任何线程切换。返回的 `Disposable` 是预创建的 `FINISHED` 单例（`Disposables.disposed()`），因为任务已经执行完毕，没有什么可取消的。

**为什么不支持延迟调度？** `ImmediateScheduler` 没有重写 `schedule(Runnable, long, TimeUnit)`，因此调用该方法会抛出 `Exceptions.failWithRejectedNotTimeCapable()`。原因是：在当前线程上实现延迟调度意味着 `Thread.sleep()`，这会阻塞调用线程。对于一个标榜"非阻塞"的框架来说，这是不可接受的。

**Worker 的 `shutdown` 字段：**

```java
static final class ImmediateSchedulerWorker implements Scheduler.Worker, Scannable {
    volatile boolean shutdown;

    @Override
    public Disposable schedule(Runnable task) {
        if (shutdown) {
            throw Exceptions.failWithRejected();
        }
        task.run();
        return FINISHED;
    }

    @Override
    public void dispose() {
        shutdown = true;
    }
}
```

Worker 有状态（`shutdown`），而 Scheduler 本身无状态。这是因为 Worker 的生命周期由使用它的操作符控制（如 `publishOn` 在完成后调用 `worker.dispose()`），而 `ImmediateScheduler` 作为全局单例永远存活。

### 4.2 SingleScheduler：事件派发线程

源码文件：`reactor/core/scheduler/SingleScheduler.java`
```java
final class SingleScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                       Scannable, SchedulerState.DisposeAwaiter<ScheduledExecutorService> {

    static final AtomicLong COUNTER = new AtomicLong();
    static final ScheduledExecutorService TERMINATED;

    static {
        TERMINATED = Executors.newSingleThreadScheduledExecutor();
        TERMINATED.shutdownNow();
    }

    final ThreadFactory factory;

    volatile SchedulerState<ScheduledExecutorService> state;
    static final AtomicReferenceFieldUpdater<SingleScheduler, SchedulerState> STATE = ...;

    private static final SchedulerState<ScheduledExecutorService> INIT =
            SchedulerState.init(TERMINATED);
}
```

**核心模型：** 一个 `ScheduledThreadPoolExecutor`（corePoolSize=1, maxPoolSize=1），由一个线程独占。这等价于 UI 框架中的"事件派发线程"模型——所有任务串行执行，保证顺序性。

**`get()` 方法创建执行器：**

```java
@Override
public ScheduledExecutorService get() {
    ScheduledThreadPoolExecutor e = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, this.factory);
    e.setRemoveOnCancelPolicy(true);
    e.setMaximumPoolSize(1);
    return e;
}
```

`setRemoveOnCancelPolicy(true)` 确保被取消的任务立即从队列中移除，避免内存泄漏。如果不设置这个策略，取消的 `ScheduledFuture` 会一直留在队列中直到到期被执行（然后发现已取消），在高频 cancel 的场景（如 `timeout` 操作符）中会导致队列无限增长。

**SchedulerState CAS 状态管理：**

```java
@Override
public void init() {
    SchedulerState<ScheduledExecutorService> a = this.state;
    if (a != INIT) {
        if (a.currentResource == TERMINATED) {
            throw new IllegalStateException("Initializing a disposed scheduler is not permitted");
        }
        return;
    }

    SchedulerState<ScheduledExecutorService> b = SchedulerState.init(
            Schedulers.decorateExecutorService(this, this.get())
    );

    if (!STATE.compareAndSet(this, INIT, b)) {
        b.currentResource.shutdownNow();
        if (isDisposed()) {
            throw new IllegalStateException("Initializing a disposed scheduler is not permitted");
        }
    }
}
```

通过 `AtomicReferenceFieldUpdater` 对 `state` 字段做 CAS 更新，确保多线程环境下只有一个线程能成功初始化。失败的线程（CAS 失败）会将多余创建的 `ScheduledExecutorService` 立即 `shutdownNow()`，避免资源泄漏。

**为什么所有 Worker 共享同一个 executor？**

```java
@Override
public Worker createWorker() {
    return new ExecutorServiceWorker(state.currentResource);
}
```

`SingleScheduler` 的所有 Worker 都指向同一个 `ScheduledExecutorService`。这意味着来自不同 Worker 的任务会在同一个线程上串行执行。如果需要多个独立的串行执行器，应该使用 `Schedulers.newSingle()` 创建多个独立实例。

### 4.3 ParallelScheduler：固定池与 Round-Robin 分配

源码文件：`reactor/core/scheduler/ParallelScheduler.java`
```java
final class ParallelScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                         SchedulerState.DisposeAwaiter<ScheduledExecutorService[]>,
                                         Scannable {

    static final ScheduledExecutorService TERMINATED;
    static final ScheduledExecutorService[] SHUTDOWN = new ScheduledExecutorService[0];
    static final AtomicLong COUNTER = new AtomicLong();

    final int n;
    final ThreadFactory factory;

    volatile @Nullable SchedulerState<ScheduledExecutorService[]> state;
    int roundRobin;
}
```

**核心模型：** 一个包含 N 个单线程 `ScheduledExecutorService` 的固定数组。N 默认等于 `DEFAULT_POOL_SIZE`（即 CPU 核数）。

**初始化过程：**

```java
@Override
public void init() {
    SchedulerState<ScheduledExecutorService[]> a = this.state;
    if (a != null) {
        if (a.currentResource == SHUTDOWN) {
            throw new IllegalStateException("Initializing a disposed scheduler is not permitted");
        }
        return;
    }

    SchedulerState<ScheduledExecutorService[]> b =
            SchedulerState.init(new ScheduledExecutorService[n]);

    for (int i = 0; i < n; i++) {
        b.currentResource[i] = Schedulers.decorateExecutorService(this, this.get());
    }

    if (!STATE.compareAndSet(this, null, b)) {
        for (ScheduledExecutorService exec : b.currentResource) {
            exec.shutdownNow();
        }
        if (isDisposed()) {
            throw new IllegalStateException("Initializing a disposed scheduler is not permitted");
        }
    }
}
```

每个 executor 都是 `ScheduledThreadPoolExecutor(1, factory)`，即每个线程一个池。`Schedulers.decorateExecutorService(this, this.get())` 会应用所有通过 `addExecutorServiceDecorator()` 注册的装饰器（如 Micrometer 指标收集）。

**Round-Robin 分配机制：**

```java
ScheduledExecutorService pick() {
    SchedulerState<ScheduledExecutorService[]> a = state;
    if (a == null) {
        init();
        a = state;
        if (a == null) {
            throw new IllegalStateException("executors uninitialized after implicit init()");
        }
    }
    if (a.currentResource != SHUTDOWN) {
        // ignoring the race condition here, its already random who gets which executor
        int idx = roundRobin;
        if (idx == n) {
            idx = 0;
            roundRobin = 1;
        }
        else {
            roundRobin = idx + 1;
        }
        return a.currentResource[idx];
    }
    return TERMINATED;
}
```

注意 `roundRobin` **不是** `volatile` 的，也没有用 CAS 保护。源码注释明确说明了原因：`"ignoring the race condition here, its already random who gets which executor"`。竞态条件最坏的结果是两个 Worker 拿到同一个 executor，这对正确性没有影响（任务仍然会被执行），只是均匀性稍差。用 CAS 保护 round-robin 会引入不必要的开销。

**`createWorker()` 与 `schedule()` 的区别：**

```java
@Override
public Worker createWorker() {
    return new ExecutorServiceWorker(pick());
}

@Override
public Disposable schedule(Runnable task) {
    return Schedulers.directSchedule(pick(), task, null, 0L, TimeUnit.MILLISECONDS);
}
```

`createWorker()` 创建一个绑定到某个 executor 的 Worker，后续该 Worker 的所有任务都在同一个线程上串行执行。而 `schedule()` 每次调用都重新 `pick()`，任务可能在不同线程上执行。操作符（如 `publishOn`）使用 Worker 来保证数据流的有序性。

### 4.4 BoundedElasticScheduler：动态线程池的精密设计

`BoundedElasticScheduler` 是四种调度器中最复杂的，它实现了动态线程创建、空闲回收、任务队列限制和最小忙碌度选择等特性。

源码文件：`reactor/core/scheduler/BoundedElasticScheduler.java`
```java
final class BoundedElasticScheduler implements Scheduler,
                                               SchedulerState.DisposeAwaiter<BoundedElasticScheduler.BoundedServices>,
                                               Scannable {

    static final int DEFAULT_TTL_SECONDS = 60;
    static final AtomicLong COUNTER = new AtomicLong();

    final int maxThreads;
    final int maxTaskQueuedPerThread;
    final Clock clock;
    final ThreadFactory factory;
    final long ttlMillis;

    volatile SchedulerState<BoundedServices> state;
}
```

#### 4.4.1 BoundedServices：线程池的核心容器

```java
static final class BoundedServices extends AtomicInteger {
    final BoundedElasticScheduler parent;
    final Clock clock;
    final ScheduledExecutorService evictor;
    final Deque<BoundedState> idleQueue;
    volatile BusyStates busyStates;
}
```

`BoundedServices` 继承 `AtomicInteger`，这个整数值记录的是**已创建的线程总数**（包括忙碌和空闲的）。`idleQueue` 是一个 `ConcurrentLinkedDeque<BoundedState>`，存放空闲的执行器状态。`busyStates` 是一个不可变数组的包装：

```java
static final class BusyStates {
    final BoundedState[] array;
    final boolean shutdown;
}
```

用不可变数组 + CAS 更新的方式替代并发集合，这避免了锁的开销，但代价是每次添加/移除 busy 状态都需要数组拷贝。

#### 4.4.2 pick() 方法：三级选择策略

```java
BoundedState pick() {
    for (;;) {
        if (busyStates == ALL_SHUTDOWN) {
            return CREATING;
        }

        int a = get();
        if (!idleQueue.isEmpty()) {
            // 第一优先级：从空闲池中取
            BoundedState bs = idleQueue.pollLast();
            if (bs != null && bs.markPicked()) {
                boolean accepted = setBusy(bs);
                if (!accepted) {
                    bs.shutdown(true);
                    return CREATING;
                }
                return bs;
            }
        }
        else if (a < parent.maxThreads) {
            // 第二优先级：创建新线程
            if (compareAndSet(a, a + 1)) {
                ScheduledExecutorService s = Schedulers.decorateExecutorService(
                    parent, parent.createBoundedExecutorService());
                BoundedState newState = new BoundedState(this, s);
                if (newState.markPicked()) {
                    boolean accepted = setBusy(newState);
                    if (!accepted) {
                        newState.shutdown(true);
                        return CREATING;
                    }
                    return newState;
                }
            }
        }
        else {
            // 第三优先级：选择最不忙的现有线程
            BoundedState s = choseOneBusy();
            if (s != null && s.markPicked()) {
                return s;
            }
        }
    }
}
```

三级选择策略体现了资源利用的优先级：
1. **复用空闲线程**：避免创建新线程的开销
2. **创建新线程**：当空闲池为空且未达上限时
3. **共享忙碌线程**：当已达上限时，选择 `markCount` 最小（即被最少 Worker 共享）的线程

**`choseOneBusy()` 的最小负载选择：**

```java
private @Nullable BoundedState choseOneBusy() {
    BoundedState[] arr = busyStates.array;
    int len = arr.length;
    if (len == 0) return null;
    if (len == 1) return arr[0];

    BoundedState choice = arr[0];
    int leastBusy = Integer.MAX_VALUE;

    for (int i = 0; i < arr.length; i++) {
        BoundedState state = arr[i];
        int busy = state.markCount;
        if (busy < leastBusy) {
            leastBusy = busy;
            choice = state;
        }
    }
    return choice;
}
```

#### 4.4.3 BoundedState：执行器状态的原子管理

```java
static class BoundedState implements Disposable, Scannable {
    static final int EVICTED = -1;

    final BoundedServices parent;
    final ScheduledExecutorService executor;

    long idleSinceTimestamp = -1L;

    volatile int markCount;
    static final AtomicIntegerFieldUpdater<BoundedState> MARK_COUNT = ...;
}
```

`markCount` 是该执行器被 Worker 引用的计数。每次 `pick()` 成功时 `markPicked()` 将其加 1，每次 Worker 被 `dispose()` 时 `release()` 将其减 1。当减到 0 时，执行器被放回空闲队列。

**TTL 驱逐机制：**

```java
boolean tryEvict(long evictionTimestamp, long ttlMillis) {
    long idleSince = this.idleSinceTimestamp;
    if (idleSince < 0) return false;
    long elapsed = evictionTimestamp - idleSince;
    if (elapsed >= ttlMillis) {
        if (MARK_COUNT.compareAndSet(this, 0, EVICTED)) {
            executor.shutdownNow();
            return true;
        }
    }
    return false;
}
```

`idleSinceTimestamp` 在 `release()` 中设置，在 `markPicked()` 中被隐式清除。驱逐线程（evictor）每隔 `ttlMillis`（默认 60 秒）扫描一次空闲队列，将超过 TTL 的执行器通过 CAS `(0, EVICTED)` 标记为已驱逐并关闭。CAS 保证了驱逐和 pick 之间不会冲突。

#### 4.4.4 BoundedScheduledExecutorService：任务队列限制

```java
static final class BoundedScheduledExecutorService extends ScheduledThreadPoolExecutor
        implements Scannable {

    final int queueCapacity;

    BoundedScheduledExecutorService(int queueCapacity, ThreadFactory factory) {
        super(1, factory);
        setMaximumPoolSize(1);
        setRemoveOnCancelPolicy(true);
        this.queueCapacity = queueCapacity;
    }

    void ensureQueueCapacity(int taskCount) {
        if (queueCapacity == Integer.MAX_VALUE) return;
        int queueSize = super.getQueue().size();
        if ((queueSize + taskCount) > queueCapacity) {
            throw Exceptions.failWithRejected(
                "Task capacity of bounded elastic scheduler reached while scheduling "
                + taskCount + " tasks (" + (queueSize + taskCount) + "/" + queueCapacity + ")");
        }
    }

    @Override
    public synchronized <T> Future<T> submit(Callable<T> task) {
        ensureQueueCapacity(1);
        return super.submit(task);
    }
    // ... 所有 submit/schedule 方法都加了 synchronized 和 ensureQueueCapacity
}
```

**为什么需要 `synchronized`？** `ScheduledThreadPoolExecutor` 的 `getQueue().size()` 和 `submit()` 之间不是原子的。如果不加锁，两个线程可能同时检查队列大小（都认为还有空间），然后都提交任务，导致超过上限。`synchronized` 保证了 check-then-act 的原子性。

**为什么不用 `RejectedExecutionHandler`？** 源码注释解释了：`"RejectedExecutionHandlers are not supported since they expect a ThreadPoolExecutor in their arguments"`。由于 `BoundedScheduledExecutorService` 继承的是 `ScheduledThreadPoolExecutor`，使用 Doug Lea 建议的通过 `getQueue()` 检查的方式更直接。

---

## 五、NonBlocking 标记接口与线程安全守卫

源码文件：`reactor/core/scheduler/NonBlocking.java`
```java
public interface NonBlocking { }
```

这是一个空的标记接口，但它的作用极为关键。

源码文件：`reactor/core/scheduler/ReactorThreadFactory.java`
```java
@Override
public final Thread newThread(@NonNull Runnable runnable) {
    String newThreadName = name + "-" + counterReference.incrementAndGet();
    Thread t = rejectBlocking
            ? new NonBlockingThread(runnable, newThreadName)
            : new Thread(runnable, newThreadName);
    if (daemon) {
        t.setDaemon(true);
    }
    return t;
}

static final class NonBlockingThread extends Thread implements NonBlocking {
    public NonBlockingThread(Runnable target, String name) {
        super(target, name);
    }
}
```

当 `ReactorThreadFactory` 的 `rejectBlocking` 参数为 `true` 时（`ParallelScheduler` 和 `SingleScheduler` 的默认值），创建的线程是 `NonBlockingThread`——它实现了 `NonBlocking` 接口。

源码文件：`reactor/core/scheduler/Schedulers.java`
```java
public static boolean isNonBlockingThread(Thread t) {
    return t instanceof NonBlocking || nonBlockingThreadPredicate.test(t);
}
```

Reactor 的阻塞 API（如 `Mono.block()`、`Flux.blockFirst()`）在执行前会检查当前线程是否实现了 `NonBlocking`。如果是，则抛出异常，防止开发者在非阻塞线程上执行阻塞操作。

**为什么 `BoundedElasticScheduler` 的线程不标记 `NonBlocking`？** 因为它本身就是为阻塞操作设计的。在 `newBoundedElastic()` 中：

```java
new ReactorThreadFactory(name, BoundedElasticScheduler.COUNTER, daemon, false,
        Schedulers::defaultUncaughtException)
```

第四个参数 `rejectBlocking` 为 `false`。

---

## 六、SchedulerState：状态机的 CAS 管理

源码文件：`reactor/core/scheduler/SchedulerState.java`
```java
final class SchedulerState<T> {

    final @Nullable T initialResource;
    final T currentResource;
    final Mono<Void> onDispose;

    private SchedulerState(@Nullable T initialResource, T currentResource, Mono<Void> onDispose) {
        this.initialResource = initialResource;
        this.currentResource = currentResource;
        this.onDispose = onDispose;
    }

    static <T> SchedulerState<T> init(final T resource) {
        return new SchedulerState<>(resource, resource, Mono.empty());
    }

    static <T> SchedulerState<T> transition(@Nullable T initial, T next, DisposeAwaiter<T> awaiter) {
        return new SchedulerState<T>(
            initial,
            next,
            initial == null ? Mono.empty() :
                Flux.<Void>create(sink -> awaitInPool(awaiter, initial, sink, 100))
                    .replay()
                    .refCount()
                    .next());
    }
}
```

`SchedulerState` 是不可变的三元组：
- `initialResource`：转换前的原始资源（用于 dispose 时关闭）
- `currentResource`：当前活跃的资源（可能是 `TERMINATED` 哨兵）
- `onDispose`：一个 `Mono<Void>`，在优雅关闭时用于等待资源释放

`transition()` 方法创建一个新的状态，其中 `onDispose` 是通过 `Flux.create()` + `replay().refCount()` 构造的冷-热混合信号：多个订阅者可以共享同一个等待过程。

**`DisposeAwaiterRunnable` 的轮询等待：**

```java
static class DisposeAwaiterRunnable<T> implements Runnable {
    static final ScheduledExecutorService TRANSITION_AWAIT_POOL;
    static {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(0);
        executor.setKeepAliveTime(10, TimeUnit.SECONDS);
        executor.allowCoreThreadTimeOut(true);
        executor.setMaximumPoolSize(Schedulers.DEFAULT_POOL_SIZE);
        TRANSITION_AWAIT_POOL = executor;
    }

    @Override
    public void run() {
        if (cancelled) return;
        try {
            if (awaiter.await(initial, awaitMs, TimeUnit.MILLISECONDS)) {
                sink.complete();
            }
            else {
                if (cancelled) return;
                TRANSITION_AWAIT_POOL.submit(this); // trampoline
            }
        }
        catch (InterruptedException e) {
            //NO-OP
        }
    }
}
```

优雅关闭不是阻塞等待，而是每 100ms 轮询一次底层资源是否已终止。这个轮询跑在专门的 `TRANSITION_AWAIT_POOL` 上，不占用业务线程。

---

## 七、ExecutorServiceWorker：通用的 Worker 实现

源码文件：`reactor/core/scheduler/ExecutorServiceWorker.java`
```java
final class ExecutorServiceWorker implements Scheduler.Worker, Disposable, Scannable {

    final ScheduledExecutorService exec;
    final Composite disposables;

    ExecutorServiceWorker(ScheduledExecutorService exec) {
        this.exec = exec;
        this.disposables = Disposables.composite();
    }

    @Override
    public Disposable schedule(Runnable task) {
        return Schedulers.workerSchedule(exec, disposables, task, 0L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void dispose() {
        disposables.dispose();
    }
}
```

`ExecutorServiceWorker` 被 `ParallelScheduler`、`SingleScheduler` 和 `DelegateServiceScheduler` 共同使用。它维护一个 `Composite disposables`，所有提交的任务都会被包装为 `WorkerTask` 并加入 composite。当 Worker 被 dispose 时，所有未完成的任务都会被取消。

在 `BoundedElasticScheduler.createWorker()` 中有一个额外的操作：

```java
@Override
public Worker createWorker() {
    BoundedState picked = state.currentResource.pick();
    ExecutorServiceWorker worker = new ExecutorServiceWorker(picked.executor);
    worker.disposables.add(picked); // 确保 Worker dispose 时释放 BoundedState
    return worker;
}
```

将 `picked`（一个实现了 `Disposable` 的 `BoundedState`）加入 Worker 的 disposables，确保 Worker 被 dispose 时会调用 `BoundedState.dispose()`（即 `release()`），将引用计数减 1。

---

## 八、Java 21 虚拟线程支持

Reactor 3.6.0 引入了对 Java 21 虚拟线程的支持，通过 `BoundedElasticThreadPerTaskScheduler` 实现。

从 `Schedulers.java` 可以看到：

```java
public static final boolean DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS =
    Optional.ofNullable(System.getProperty("reactor.schedulers.defaultBoundedElasticOnVirtualThreads"))
            .map(Boolean::parseBoolean)
            .orElse(false);
```

启用方式是设置系统属性 `-Dreactor.schedulers.defaultBoundedElasticOnVirtualThreads=true`。

`VirtualThreadFactory` 在低于 JDK 21 的运行时中是一个桩实现：

```java
class VirtualThreadFactory implements ThreadFactory, Thread.UncaughtExceptionHandler {
    VirtualThreadFactory(String name, boolean inheritThreadLocals,
            @Nullable BiConsumer<Thread, Throwable> uncaughtExceptionHandler) {
        throw new UnsupportedOperationException("Virtual Threads are not supported in JVM lower than 21");
    }
}
```

在 JDK 21+ 的构建变体中，`VirtualThreadFactory` 会使用 `Thread.ofVirtual()` API 创建虚拟线程。虚拟线程的核心区别在于：它们不绑定操作系统线程，阻塞操作不会浪费操作系统线程资源，因此 `maxThreads` 可以设置得更大。

`BoundedElasticSchedulerSupplier`（JDK 8 变体）在检测到虚拟线程开关打开时会打印警告：

```java
@Override
public Scheduler get() {
    if (DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS) {
        logger.warn(
            "Virtual Threads support is not available on the given JVM. " +
            "Falling back to default BoundedElastic setup");
    }
    return newBoundedElastic(DEFAULT_BOUNDED_ELASTIC_SIZE,
            DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
            BOUNDED_ELASTIC,
            BoundedElasticScheduler.DEFAULT_TTL_SECONDS,
            true);
}
```

---

## 九、DelegateServiceScheduler 与 ExecutorScheduler

### 9.1 DelegateServiceScheduler：包装外部 ExecutorService

源码文件：`reactor/core/scheduler/DelegateServiceScheduler.java`

```java
final class DelegateServiceScheduler implements Scheduler,
        SchedulerState.DisposeAwaiter<ScheduledExecutorService>, Scannable {

    final String executorName;
    final ScheduledExecutorService original;
    volatile @Nullable SchedulerState<ScheduledExecutorService> state;
}
```

通过 `Schedulers.fromExecutorService(ExecutorService)` 创建。如果传入的不是 `ScheduledExecutorService`，会被包装为 `UnsupportedScheduledExecutorService`，其 `schedule(delay)` 等方法直接抛异常。

### 9.2 ExecutorScheduler：包装外部 Executor

源码文件：`reactor/core/scheduler/ExecutorScheduler.java`

`ExecutorScheduler` 包装任意 `Executor`，提供两种 Worker：
- **非 trampoline**（`ExecutorSchedulerWorker`）：任务直接提交给 executor，不保证 FIFO
- **trampoline**（`ExecutorSchedulerTrampolineWorker`）：任务入队，由 WIP 计数器控制排空

trampoline Worker 中的 WIP 模式：

```java
@Override
public Disposable schedule(Runnable task) {
    // ...
    queue.offer(r);

    if (WIP.getAndIncrement(this) == 0) {
        try {
            executor.execute(this);
        }
        catch (Throwable ex) { ... }
    }
    return r;
}

@Override
public void run() {
    final Queue<ExecutorTrackedRunnable> q = queue;
    for (; ; ) {
        int e = 0;
        int r = wip;
        while (e != r) {
            ExecutorTrackedRunnable task = q.poll();
            if (task == null) break;
            task.run();
            e++;
        }
        if (WIP.addAndGet(this, -e) == 0) break;
    }
}
```

这就是经典的"drain loop"模式——第一个提交任务的线程成为"drain owner"，负责执行队列中的所有任务。后续提交的线程只增加 WIP 计数，不重复提交 executor。这保证了所有任务在同一个 executor 调度中串行执行。

---

## 十、归纳总表

| 维度 | `immediate()` | `single()` | `parallel()` | `boundedElastic()` |
|---|---|---|---|---|
| **实现类** | `ImmediateScheduler` | `SingleScheduler` | `ParallelScheduler` | `BoundedElasticScheduler` |
| **线程数** | 0（当前线程） | 1 | `DEFAULT_POOL_SIZE` (CPU 核数) | 0 ~ `DEFAULT_BOUNDED_ELASTIC_SIZE` (10 * CPU 核数) |
| **线程类型** | 无 | `NonBlockingThread`（daemon） | `NonBlockingThread`（daemon） | 普通 `Thread`（daemon） |
| **NonBlocking** | N/A | 是 | 是 | 否 |
| **时间感知** | 否 | 是 | 是 | 是 |
| **任务队列限制** | 无 | 无（JDK 默认） | 无（JDK 默认） | `DEFAULT_BOUNDED_ELASTIC_QUEUESIZE` (100000) |
| **线程复用** | N/A | 永久存活 | 永久存活 | TTL 60 秒空闲回收 |
| **Worker 分配** | 每次创建新 Worker | 所有 Worker 共享同一线程 | Round-Robin 分配到 N 个线程 | 空闲优先 → 新建 → 最少共享 |
| **适用场景** | 测试、空对象模式 | 顺序任务、事件派发 | CPU 密集型并行计算 | 阻塞 I/O（数据库、文件、HTTP） |
| **虚拟线程** | N/A | 不支持 | 不支持 | 3.6.0+ 可选支持 |
| **底层执行器** | 无 | `ScheduledThreadPoolExecutor(1)` | `ScheduledThreadPoolExecutor(1)` x N | `BoundedScheduledExecutorService(1)` x M（动态） |
| **状态管理** | 无状态单例 | `SchedulerState` + CAS | `SchedulerState` + CAS | `SchedulerState` + CAS + BoundedServices |
| **`dispose()` 行为** | 空操作 | `shutdownNow()` executor | `shutdownNow()` 所有 executor | 关闭 evictor + 关闭所有 BoundedState |
