# ParallelFlux 并行处理源码解析

> **Reactor Core 源码深度研究系列 · 第 13 篇**
> 本文深入解析 Reactor 的并行处理抽象 `ParallelFlux`，从数据分发、线程调度、并行操作符到结果合并，完整呈现"一流多轨"的并行计算引擎实现。

---

## 一、全局架构总览

```
                    ┌───────────────────────────────────┐
                    │         Source Publisher           │
                    │     (普通 Flux<T> 数据源)          │
                    └──────────────┬────────────────────┘
                                   │ subscribe()
                                   ▼
                    ┌───────────────────────────────────┐
                    │      ParallelSource<T>            │
                    │   (round-robin 分配到 N 个 rail)   │
                    │                                   │
                    │   parallelism: int                │
                    │   prefetch: int                   │
                    │   queue: Queue<T>                 │
                    │   index: int (轮转指针)            │
                    └──┬────────┬────────┬──────────────┘
                       │        │        │
              rail[0]  │ rail[1]│ rail[2]│ ...rail[N-1]
                       ▼        ▼        ▼
                    ┌──────┐ ┌──────┐ ┌──────┐
                    │Sub-0 │ │Sub-1 │ │Sub-2 │   CoreSubscriber[]
                    └──┬───┘ └──┬───┘ └──┬───┘
                       │        │        │
                       ▼        ▼        ▼
                    ┌───────────────────────────────────┐
                    │      ParallelRunOn<T>             │
                    │   (为每个 rail 分配 Worker 线程)    │
                    │                                   │
                    │   scheduler: Scheduler            │
                    │   per rail: PublishOnSubscriber   │
                    └──┬────────┬────────┬──────────────┘
                       │        │        │
                       ▼        ▼        ▼
                    ┌───────────────────────────────────┐
                    │    ParallelMap / ParallelFilter    │
                    │   (每个 rail 独立执行操作)          │
                    └──┬────────┬────────┬──────────────┘
                       │        │        │
                       ▼        ▼        ▼
                    ┌───────────────────────────────────┐
                    │   ParallelMergeSequential<T>      │
                    │  (合并 N 个 rail 为单一 Flux)      │
                    │                                   │
                    │   MergeSequentialMain              │
                    │   MergeSequentialInner[]           │
                    └───────────────┬───────────────────┘
                                    │
                                    ▼
                         ┌─────────────────┐
                         │   Subscriber    │
                         │   (最终消费者)   │
                         └─────────────────┘
```

**并行轨道数据流图**：

```
Source: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                    │
                    │ parallel(3) — round-robin 分配
                    ▼
    rail[0]: [1, 4, 7, 10]     ─── Worker-0 ──▶ map/filter ──┐
    rail[1]: [2, 5, 8]         ─── Worker-1 ──▶ map/filter ──┤ sequential()
    rail[2]: [3, 6, 9]         ─── Worker-2 ──▶ map/filter ──┘
                                                               │
                                                               ▼
                                              Output: [1, 2, 3, 4, 5, 6, ...]
                                              (round-robin 交错合并，不保证原顺序)
```

---

## 二、ParallelFlux 抽象基类

### 2.1 设计定位

`ParallelFlux<T>` 是 Reactor 并行处理的核心抽象，它代表一个分成 N 条"轨道"（rail）的并行数据流。与普通 `Flux` 不同，它不是 `Publisher<T>` 的直接子类，而是实现了 `CorePublisher<T>` 接口。

源码位置：`reactor/core/publisher/ParallelFlux.java`

```java
public abstract class ParallelFlux<T> implements CorePublisher<T> {
    // ...
}
```

**两个核心抽象方法**：

- `parallelism()`：返回并行轨道数。
- `subscribe(CoreSubscriber<? super T>[] subscribers)`：接受一个 Subscriber 数组，每个 Subscriber 消费一个 rail。

注意 `subscribe(CoreSubscriber[])` 的签名——它接受的是**数组**而不是单个 Subscriber。这与普通 `Publisher.subscribe(Subscriber)` 截然不同，体现了 ParallelFlux 的"多轨"本质。

### 2.2 创建方式

```java
public static <T> ParallelFlux<T> from(Publisher<? extends T> source,
        int parallelism, int prefetch, Supplier<Queue<T>> queueSupplier) {
    Objects.requireNonNull(queueSupplier, "queueSupplier");
    Objects.requireNonNull(source, "source");
    return onAssembly(new ParallelSource<>(source, parallelism, prefetch, queueSupplier));
}
```

无参 `from(source)` 使用默认的并行度 `Schedulers.DEFAULT_POOL_SIZE`（通常等于 CPU 核心数）和默认 prefetch `Queues.SMALL_BUFFER_SIZE`（256）。

**为什么默认并行度等于 CPU 核心数？** 因为 ParallelFlux 的典型场景是 CPU 密集型计算。如果并行度大于核心数，线程竞争反而会降低性能。对于 IO 密集型任务，通常直接用 `flatMap` + `subscribeOn` 更合适。

### 2.3 sequential() — 从并行回到串行

```java
public final Flux<T> sequential() {
    return sequential(Queues.SMALL_BUFFER_SIZE);
}

public final Flux<T> sequential(int prefetch) {
    return Flux.onAssembly(new ParallelMergeSequential<>(this, prefetch, Queues.small()));
}
```

`sequential()` 通过 `ParallelMergeSequential` 将 N 个 rail 合并为单一的 `Flux`。

### 2.4 reduce() — 并行归约

```java
public final Mono<T> reduce(BiFunction<T, T, T> reducer) {
    return Mono.onAssembly(new ParallelMergeReduce<>(this, reducer));
}

public final <R> ParallelFlux<R> reduce(Supplier<R> initialSupplier,
        BiFunction<R, ? super T, R> reducer) {
    return onAssembly(new ParallelReduceSeed<>(this, initialSupplier, reducer));
}
```

两个版本：不带初始值的版本返回 `Mono<T>`（将 N 个 rail 的结果再归约为一个值）；带初始值的版本返回 `ParallelFlux<R>`（每个 rail 各自归约为一个值，结果还是 N 个 rail）。

---

## 三、ParallelSource：Round-Robin 数据分发引擎

### 3.1 类结构

`ParallelSource<T>` 是 `ParallelFlux.from()` 的核心实现，负责将上游 Publisher 的数据以 round-robin 方式分发到 N 个下游 rail。

源码位置：`reactor/core/publisher/ParallelSource.java`

```java
final class ParallelSource<T> extends ParallelFlux<T> implements Scannable {
    final Publisher<? extends T> source;
    final int parallelism;
    final int prefetch;
    final Supplier<Queue<T>> queueSupplier;
}
```

### 3.2 subscribe() 的桥接

```java
@Override
public void subscribe(CoreSubscriber<? super T>[] subscribers) {
    if (!validate(subscribers)) {
        return;
    }
    source.subscribe(new ParallelSourceMain<>(subscribers, prefetch, queueSupplier));
}
```

`ParallelSourceMain` 订阅上游 source，然后通过 round-robin 将数据分发到各个 rail。

### 3.3 ParallelSourceMain 的核心字段

```java
static final class ParallelSourceMain<T> implements InnerConsumer<T> {
    final CoreSubscriber<? super T>[] subscribers;  // N 个 rail 的 Subscriber
    final AtomicLongArray requests;                  // 每个 rail 的 request 计数
    final long[] emissions;                          // 每个 rail 的已发送计数
    final int prefetch;
    final int limit;
    Subscription s;
    Queue<T> queue;
    int index;            // 当前 round-robin 指针
    volatile boolean done;
    volatile boolean cancelled;
    volatile int wip;     // 工作计数器 (drain 串行化)
    int produced;         // 向上游请求的批次计数
    int sourceMode;       // fusion 模式
}
```

关键设计：

- **`requests`**：使用 `AtomicLongArray` 而不是 `AtomicLong[]`，因为 `AtomicLongArray` 使用连续内存布局，对缓存更友好，且避免了数组元素的 volatile 语义问题。
- **`emissions`**：普通 long 数组，只在 drain 循环内访问（WIP 保证了串行），不需要原子操作。
- **`index`**：round-robin 指针，同样只在 drain 循环内更新。

### 3.4 drainAsync() — 异步模式的 Round-Robin 核心

```java
void drainAsync() {
    int missed = 1;
    Queue<T> q = queue;
    CoreSubscriber<? super T>[] a = this.subscribers;
    AtomicLongArray r = this.requests;
    long[] e = this.emissions;
    int n = e.length;
    int idx = index;
    int consumed = produced;

    for (;;) {
        int notReady = 0;
        for (;;) {
            if (cancelled) { q.clear(); return; }
            boolean d = done;
            // ... error/complete 检查 ...
            boolean empty = q.isEmpty();
            if (d && empty) {
                for (Subscriber<? super T> s : a) { s.onComplete(); }
                return;
            }
            if (empty) { break; }

            long ridx = r.get(idx);    // rail[idx] 的当前 request
            long eidx = e[idx];        // rail[idx] 的已发送计数
            if (ridx != eidx) {        // 还有剩余 request
                T v = q.poll();
                if (v == null) { break; }
                a[idx].onNext(v);      // 发送到 rail[idx]
                e[idx] = eidx + 1;

                int c = ++consumed;
                if (c == limit) {
                    consumed = 0;
                    s.request(c);      // 向上游请求补充
                }
                notReady = 0;
            } else {
                notReady++;            // rail[idx] 暂时没有 request
            }

            idx++;
            if (idx == n) { idx = 0; } // round-robin 回绕

            if (notReady == n) { break; } // 所有 rail 都没有 request，退出
        }
        // WIP drain loop 退出逻辑
        index = idx;
        produced = consumed;
        missed = WIP.addAndGet(this, -missed);
        if (missed == 0) { break; }
    }
}
```

**Round-Robin 算法详解**：

1. 维护一个 `index` 指针，从 0 到 N-1 循环。
2. 每次尝试向 `index` 对应的 rail 发送数据，前提是该 rail 的 `requests[idx] != emissions[idx]`（即还有未满足的 request）。
3. 如果当前 rail 没有 request，跳过它（`notReady++`），继续下一个 rail。
4. 如果连续 N 个 rail 都没有 request（`notReady == n`），退出内层循环。

**为什么不用取模（%）而是 if + 重置？**

```java
idx++;
if (idx == n) { idx = 0; }
```

这比 `idx = (idx + 1) % n` 更高效。取模运算在大多数 CPU 上需要除法指令，而条件判断+重置只需要一次比较和一次赋值。在每秒可能执行数百万次的热路径上，这个优化是有意义的。

### 3.5 drainSync() — 同步 Fusion 模式

当上游支持 `Fuseable.SYNC` 融合时，`drainSync()` 被使用。与 `drainAsync()` 的区别是不需要 `s.request()` 向上游请求（SYNC 模式下 queue 直接从上游 pull），且空 queue 直接意味着完成：

```java
void drainSync() {
    // ...
    for (;;) {
        if (q.isEmpty()) {
            for (Subscriber<? super T> s : a) { s.onComplete(); }
            return;
        }
        // ... round-robin 分发，同 drainAsync 但没有 s.request()
    }
}
```

### 3.6 ParallelSourceInner 的 request 路由

每个 rail 的 Subscriber 通过 `ParallelSourceInner` 与 `ParallelSourceMain` 通信：

```java
static final class ParallelSourceInner<T> implements InnerProducer<T> {
    final ParallelSourceMain<T> parent;
    final int index;
    final int length;

    @Override
    public void request(long n) {
        if (Operators.validate(n)) {
            AtomicLongArray ra = parent.requests;
            for (;;) {
                long r = ra.get(index);
                if (r == Long.MAX_VALUE) { return; }
                long u = Operators.addCap(r, n);
                if (ra.compareAndSet(index, r, u)) { break; }
            }
            if (parent.subscriberCount == length) {
                parent.drain();
            }
        }
    }
}
```

注意 `parent.subscriberCount == length` 这个条件：**只有当所有 rail 的 Subscriber 都已设置完毕后，才触发 drain**。这避免了部分 rail 尚未 subscribe 时就开始分发数据。

---

## 四、ParallelRunOn：为每个 Rail 分配线程

### 4.1 设计原理

`ParallelRunOn<T>` 是 `runOn(Scheduler)` 操作符的实现，它为每个 rail 创建一个独立的 `Worker`，使得后续操作在各自的线程上执行。

源码位置：`reactor/core/publisher/ParallelRunOn.java`

```java
final class ParallelRunOn<T> extends ParallelFlux<T> implements Scannable {
    final ParallelFlux<? extends T> source;
    final Scheduler scheduler;
    final int prefetch;
    final Supplier<Queue<T>> queueSupplier;
}
```

### 4.2 subscribe() — Worker 分配

```java
@Override
public void subscribe(CoreSubscriber<? super T>[] subscribers) {
    if (!validate(subscribers)) { return; }
    int n = subscribers.length;
    CoreSubscriber<T>[] parents = new CoreSubscriber[n];

    boolean conditional = subscribers[0] instanceof Fuseable.ConditionalSubscriber;

    for (int i = 0; i < n; i++) {
        Worker w = scheduler.createWorker();
        if (conditional) {
            parents[i] = new FluxPublishOn.PublishOnConditionalSubscriber<>(
                    (Fuseable.ConditionalSubscriber<T>)subscribers[i],
                    scheduler, w, true, prefetch, prefetch, queueSupplier);
        } else {
            parents[i] = new FluxPublishOn.PublishOnSubscriber<>(subscribers[i],
                    scheduler, w, true, prefetch, prefetch, queueSupplier);
        }
    }
    source.subscribe(parents);
}
```

**关键洞察**：`ParallelRunOn` 复用了 `FluxPublishOn.PublishOnSubscriber`——也就是说，每个 rail 的线程切换机制与普通 `publishOn()` 完全相同。这是一种优雅的代码复用。

**每个 rail 一个 Worker** 意味着 `Scheduler` 需要能创建足够多的 Worker。如果使用 `Schedulers.single()`（只有一个线程的 Scheduler），所有 rail 会共享同一个线程，失去并行的意义。推荐使用 `Schedulers.parallel()` 或 `Schedulers.boundedElastic()`。

### 4.3 RUN_STYLE 属性

```java
@Override
public @Nullable Object scanUnsafe(Attr key) {
    if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;
    // ...
}
```

`ParallelRunOn` 将 `RUN_STYLE` 报告为 `ASYNC`，因为它引入了线程切换。这与 `ParallelSource`、`ParallelMap` 等报告 `SYNC` 不同。

---

## 五、ParallelMap / ParallelFilter：并行操作符

### 5.1 ParallelMap — 每个 Rail 独立映射

源码位置：`reactor/core/publisher/ParallelMap.java`

```java
final class ParallelMap<T, R> extends ParallelFlux<R> implements Scannable {
    final ParallelFlux<T> source;
    final Function<? super T, ? extends R> mapper;

    @Override
    public void subscribe(CoreSubscriber<? super R>[] subscribers) {
        if (!validate(subscribers)) { return; }
        int n = subscribers.length;
        CoreSubscriber<? super T>[] parents = new CoreSubscriber[n];

        for (int i = 0; i < n; i++) {
            if (conditional) {
                parents[i] = new FluxMap.MapConditionalSubscriber<>(
                        (Fuseable.ConditionalSubscriber<R>) subscribers[i], mapper);
            } else {
                parents[i] = new FluxMap.MapSubscriber<>(subscribers[i], mapper);
            }
        }
        source.subscribe(parents);
    }
}
```

**模式总结**：ParallelMap 的 `subscribe()` 遵循统一的"包装-转发"模式：
1. 为每个 rail 的 Subscriber 创建对应的 Operator Subscriber（复用 `FluxMap.MapSubscriber`）。
2. 将包装后的 Subscriber 数组传给上游的 `source.subscribe()`。

这意味着 **ParallelFlux 的操作符与 Flux 的操作符共享相同的内部实现**。`ParallelMap` 不需要重新实现映射逻辑，只需要为每个 rail 创建一个 `FluxMap.MapSubscriber`。

### 5.2 ParallelFilter — 每个 Rail 独立过滤

源码位置：`reactor/core/publisher/ParallelFilter.java`

```java
final class ParallelFilter<T> extends ParallelFlux<T> implements Scannable {
    final ParallelFlux<T> source;
    final Predicate<? super T> predicate;

    @Override
    public void subscribe(CoreSubscriber<? super T>[] subscribers) {
        int n = subscribers.length;
        CoreSubscriber<? super T>[] parents = new CoreSubscriber[n];
        for (int i = 0; i < n; i++) {
            if (conditional) {
                parents[i] = new FluxFilter.FilterConditionalSubscriber<>(
                        (Fuseable.ConditionalSubscriber<T>)subscribers[i], predicate);
            } else {
                parents[i] = new FluxFilter.FilterSubscriber<>(subscribers[i], predicate);
            }
        }
        source.subscribe(parents);
    }
}
```

同样复用了 `FluxFilter.FilterSubscriber`。

### 5.3 ConditionalSubscriber 优化

注意两个操作符都检查了 `subscribers[0] instanceof Fuseable.ConditionalSubscriber`。这是一个融合优化：`ConditionalSubscriber` 的 `tryOnNext(T)` 方法可以原地判断是否接受元素，避免了不必要的 `request(1)` 回调。对于 `filter()` 等操作符，这可以显著减少上下游之间的往返次数。

---

## 六、ParallelFlatMap：每个 Rail 内部的扁平映射

源码位置：`reactor/core/publisher/ParallelFlatMap.java`

```java
final class ParallelFlatMap<T, R> extends ParallelFlux<R> implements Scannable {
    final ParallelFlux<T> source;
    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    final boolean delayError;
    final int maxConcurrency;
    final Supplier<? extends Queue<R>> mainQueueSupplier;
    final int prefetch;
    final Supplier<? extends Queue<R>> innerQueueSupplier;

    @Override
    public void subscribe(CoreSubscriber<? super R>[] subscribers) {
        int n = subscribers.length;
        CoreSubscriber<T>[] parents = new CoreSubscriber[n];
        for (int i = 0; i < n; i++) {
            parents[i] = new FluxFlatMap.FlatMapMain<>(subscribers[i],
                    mapper, delayError, maxConcurrency,
                    mainQueueSupplier, prefetch, innerQueueSupplier);
        }
        source.subscribe(parents);
    }
}
```

每个 rail 内部创建一个独立的 `FluxFlatMap.FlatMapMain`。这意味着每个 rail 有自己的 `maxConcurrency` 限制。如果 `parallelism = 4` 且 `maxConcurrency = 16`，总的并发内部订阅数最多为 `4 * 16 = 64`。

---

## 七、ParallelReduceSeed：每个 Rail 的独立归约

源码位置：`reactor/core/publisher/ParallelReduceSeed.java`

```java
final class ParallelReduceSeed<T, R> extends ParallelFlux<R> implements Scannable, Fuseable {
    final ParallelFlux<? extends T> source;
    final Supplier<R> initialSupplier;
    final BiFunction<R, ? super T, R> reducer;
}
```

### 7.1 subscribe() 中的初始值创建

```java
@Override
public void subscribe(CoreSubscriber<? super R>[] subscribers) {
    int n = subscribers.length;
    CoreSubscriber<T>[] parents = new CoreSubscriber[n];
    for (int i = 0; i < n; i++) {
        R initialValue;
        try {
            initialValue = Objects.requireNonNull(initialSupplier.get(),
                    "The initialSupplier returned a null value");
        } catch (Throwable ex) {
            reportError(subscribers, Operators.onOperatorError(ex, subscribers[i].currentContext()));
            return;
        }
        parents[i] = new ParallelReduceSeedSubscriber<>(subscribers[i], initialValue, reducer);
    }
    source.subscribe(parents);
}
```

**每个 rail 独立调用 `initialSupplier.get()`**。这很重要——如果初始值是可变对象（如 `ArrayList`），必须为每个 rail 创建独立实例，否则多个线程会并发修改同一个对象。

### 7.2 ParallelReduceSeedSubscriber

```java
static final class ParallelReduceSeedSubscriber<T, R> extends Operators.BaseFluxToMonoOperator<T, R> {
    final BiFunction<R, ? super T, R> reducer;
    @Nullable R accumulator;
    boolean done;

    @Override
    public void onNext(T t) {
        if (done) { return; }
        synchronized (this) {
            R v;
            try {
                v = Objects.requireNonNull(reducer.apply(accumulator, t), "...");
            } catch (Throwable ex) {
                onError(Operators.onOperatorError(this.s, ex, t, actual.currentContext()));
                return;
            }
            accumulator = v;
        }
    }
}
```

注意 `onNext()` 使用了 `synchronized`。**为什么并行 rail 还需要同步？** 因为虽然每个 rail 在 `runOn()` 之后通常运行在独立线程上，但 Reactive Streams 规范允许 Subscription 的方法在任意线程调用，且某些场景下 Subscriber 可能被多个线程调用（如上游使用了 merge 或类似操作符）。`synchronized` 提供了安全保障。

`cancel()` 时会清理 `accumulator` 并调用 `Operators.onDiscard()`，防止内存泄漏：

```java
@Override
public void cancel() {
    s.cancel();
    final R a;
    synchronized (this) {
        a = accumulator;
        if (a != null) { accumulator = null; }
    }
    if (a == null) { return; }
    Operators.onDiscard(a, currentContext());
}
```

---

## 八、ParallelMergeSequential：合并 N 轨为单一 Flux

### 8.1 设计目标

`ParallelMergeSequential<T>` 是 `sequential()` 操作符的实现。它将 N 个 rail 的数据合并为单一的 `Flux<T>`。

源码位置：`reactor/core/publisher/ParallelMergeSequential.java`

```java
final class ParallelMergeSequential<T> extends Flux<T> implements Scannable {
    final ParallelFlux<? extends T> source;
    final int prefetch;
    final Supplier<Queue<T>> queueSupplier;
}
```

### 8.2 subscribe() 的初始化

```java
@Override
public void subscribe(CoreSubscriber<? super T> actual) {
    MergeSequentialMain<T> parent = new MergeSequentialMain<>(actual,
            source.parallelism(), prefetch, queueSupplier);
    actual.onSubscribe(parent);
    source.subscribe(parent.subscribers);
}
```

`MergeSequentialMain` 创建 N 个 `MergeSequentialInner`，每个 inner 消费一个 rail：

```java
MergeSequentialMain(CoreSubscriber<? super T> actual, int n, int prefetch,
        Supplier<Queue<T>> queueSupplier) {
    this.actual = actual;
    this.queueSupplier = queueSupplier;
    MergeSequentialInner<T>[] a = new MergeSequentialInner[n];
    for (int i = 0; i < n; i++) {
        a[i] = new MergeSequentialInner<>(this, prefetch);
    }
    this.subscribers = a;
    DONE.lazySet(this, n);
}
```

`DONE` 初始化为 `n`，每个 rail 完成时递减。当减到 0 时表示所有 rail 都已完成。

### 8.3 onNext() 的快速路径

```java
void onNext(MergeSequentialInner<T> inner, T value) {
    if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
        if (requested != 0) {
            actual.onNext(value);
            if (requested != Long.MAX_VALUE) {
                REQUESTED.decrementAndGet(this);
            }
            inner.requestOne();
        } else {
            Queue<T> q = inner.getQueue(queueSupplier);
            if(!q.offer(value)){ /* overflow error */ }
        }
        if (WIP.decrementAndGet(this) == 0) {
            return;  // 快速路径退出
        }
    } else {
        Queue<T> q = inner.getQueue(queueSupplier);
        if(!q.offer(value)){ /* overflow error */ }
        if (WIP.getAndIncrement(this) != 0) {
            return;  // 已有其他线程在 drain
        }
    }
    drainLoop();
}
```

**快速路径优化**：当 WIP 为 0 且下游有 request 时，直接 `actual.onNext(value)` 而不需要经过 queue 缓冲。这避免了 queue 的 offer/poll 开销。只有在并发竞争或没有 request 时才走 queue 缓冲路径。

### 8.4 drainLoop() 的 Round-Robin 合并

```java
void drainLoop() {
    int missed = 1;
    MergeSequentialInner<T>[] s = this.subscribers;
    int n = s.length;
    Subscriber<? super T> a = this.actual;

    for (;;) {
        long r = requested;
        long e = 0;

        middle:
        while (e != r) {
            // ... cancelled/error 检查 ...
            boolean d = done == 0;
            boolean empty = true;

            for (int i = 0; i < n; i++) {
                MergeSequentialInner<T> inner = s[i];
                Queue<T> q = inner.queue;
                if (q != null) {
                    T v = q.poll();
                    if (v != null) {
                        empty = false;
                        a.onNext(v);
                        inner.requestOne();
                        if (++e == r) { break middle; }
                    }
                }
            }

            if (d && empty) {
                a.onComplete();
                return;
            }
            if (empty) { break; }
        }
        // ... WIP drain loop 退出 ...
    }
}
```

合并策略是遍历所有 inner 的 queue，依次 poll 数据发送给下游。**这不是严格的 round-robin**——而是"谁有数据就发谁的"。如果某个 rail 特别慢，其他 rail 的数据会先输出。这意味着 `sequential()` 不保证输出顺序与输入顺序一致。

### 8.5 requestOne() 的批量请求优化

```java
void requestOne() {
    long p = produced + 1;
    if (p == limit) {
        produced = 0;
        s.request(p);
    } else {
        produced = p;
    }
}
```

不是每消费一个元素就 `request(1)`，而是累积到 `limit` 时批量请求。`limit = Operators.unboundedOrLimit(prefetch)`，对于默认 prefetch 256，limit 为 192（prefetch 的 75%）。这减少了上下游之间的请求往返次数。

---

## 九、ParallelFlux vs flatMap：两种并行模型的对比

### 9.1 架构区别

**flatMap 的并行**：创建多个内部 Publisher，每个内部 Publisher 可以运行在不同线程上。

```
Source ──▶ flatMap(mapper, concurrency=4)
                │
                ├──▶ Inner Publisher 0 (异步执行)
                ├──▶ Inner Publisher 1 (异步执行)
                ├──▶ Inner Publisher 2 (异步执行)
                └──▶ Inner Publisher 3 (异步执行)
                         │
                         ▼ merge 输出
```

**ParallelFlux 的并行**：将源数据分成固定的 N 条轨道，每条轨道有自己的线程和操作符链。

```
Source ──▶ parallel(4) ──▶ runOn(scheduler)
                │
                ├── rail[0]: Worker-0 上的独立操作符链
                ├── rail[1]: Worker-1 上的独立操作符链
                ├── rail[2]: Worker-2 上的独立操作符链
                └── rail[3]: Worker-3 上的独立操作符链
                         │
                         ▼ sequential() 合并输出
```

### 9.2 为什么 ParallelFlux 在某些场景更高效

1. **订阅开销**：flatMap 每个元素都可能创建一个新的内部 Publisher 和 Subscriber，产生大量的短生命周期对象。ParallelFlux 的 rail 数是固定的，Subscriber 在整个生命周期内只创建一次。

2. **线程固定**：ParallelFlux 的每个 rail 绑定一个 Worker（线程），数据在同一个线程上流过整个操作符链，减少了上下文切换。flatMap 的内部 Publisher 可能在不同线程上执行，merge 时需要更多的同步操作。

3. **背压简单**：ParallelFlux 的每个 rail 独立背压，不需要 flatMap 那样复杂的 WIP 协调。`MergeSequentialMain` 只需要简单地遍历各 rail 的 queue。

4. **代码复用**：`ParallelMap` 复用 `FluxMap.MapSubscriber`，`ParallelFilter` 复用 `FluxFilter.FilterSubscriber`，`ParallelRunOn` 复用 `FluxPublishOn.PublishOnSubscriber`。零额外实现成本。

### 9.3 什么时候不应该用 ParallelFlux

- **IO 密集型且延迟差异大的任务**：round-robin 分配可能导致某些 rail 积压而其他 rail 空闲。flatMap 的"谁快谁先处理"模型更适合。
- **元素数量少于 parallelism**：如果只有 3 个元素但 parallelism=8，大部分 rail 会空转。
- **需要保持元素顺序**：`sequential()` 不保证顺序。如果需要排序合并，需要用 `sorted(Comparator)` 或 `ParallelMergeOrdered`，但这有额外开销。

---

## 十、归纳表格

### ParallelFlux 操作符对照表

| 操作符类 | 对应方法 | 作用 | 复用的 Flux 操作符 | 状态 |
|---------|---------|------|-------------------|------|
| `ParallelSource` | `parallel(n)` / `from()` | 将 Flux 拆分为 N 个 rail (round-robin) | 无（独立实现） | 有 queue, WIP, index |
| `ParallelRunOn` | `runOn(Scheduler)` | 为每个 rail 分配独立线程 | `FluxPublishOn.PublishOnSubscriber` | 有 queue, Worker |
| `ParallelMap` | `map(Function)` | 每个 rail 独立映射 | `FluxMap.MapSubscriber` | 无状态 |
| `ParallelFilter` | `filter(Predicate)` | 每个 rail 独立过滤 | `FluxFilter.FilterSubscriber` | 无状态 |
| `ParallelFlatMap` | `flatMap(Function)` | 每个 rail 内部扁平映射 | `FluxFlatMap.FlatMapMain` | 有 queue, maxConcurrency |
| `ParallelReduceSeed` | `reduce(Supplier, BiFunction)` | 每个 rail 独立归约 | 独立实现 | 有 accumulator |
| `ParallelMergeSequential` | `sequential()` | 合并 N 个 rail 为单一 Flux | 无（独立实现） | 有 queue[], WIP, DONE |
| `ParallelMergeReduce` | `reduce(BiFunction)` | 合并 N 个 rail 的归约结果为 Mono | 独立实现 | 有 accumulator |
| `ParallelCollect` | `collect(Supplier, BiConsumer)` | 每个 rail 收集到容器 | 独立实现 | 有 container |
| `ParallelConcatMap` | `concatMap(Function)` | 每个 rail 按序扁平映射 | `FluxConcatMap` | 有 queue |
| `ParallelGroup` | `groups()` | 将 N 个 rail 暴露为 GroupedFlux | 独立实现 | 有 UnicastProcessor |
| `ParallelPeek` | `doOnNext()` 等 | 每个 rail 的副作用回调 | 独立实现 | 无状态 |
| `ParallelLog` | `log()` | 每个 rail 的日志输出 | 独立实现 | 无状态 |

### 关键设计决策对照表

| 设计决策 | 选择 | 原因 |
|---------|------|------|
| 数据分发策略 | Round-Robin | 简单高效，均匀分配，O(1) 分发 |
| 并行度默认值 | `Schedulers.DEFAULT_POOL_SIZE` (CPU 核心数) | CPU 密集型场景最优 |
| request 数组类型 | `AtomicLongArray` | 连续内存，缓存友好 |
| drain 串行化 | WIP (AtomicIntegerFieldUpdater) | 标准的无锁串行化模式 |
| 操作符实现 | 复用 Flux 的 Subscriber | 零代码重复，一致的行为 |
| 合并策略 | 遍历 poll（不保序） | 简单、低延迟 |
| 上游请求策略 | 批量请求 (limit = 75% of prefetch) | 减少请求往返 |

### ParallelFlux 生命周期状态表

| 阶段 | ParallelSource | ParallelRunOn | ParallelMergeSequential |
|------|---------------|---------------|------------------------|
| 创建 | 构造 queue, requests[], emissions[] | 创建 N 个 Worker | 创建 N 个 MergeSequentialInner |
| 订阅 | source.subscribe(ParallelSourceMain) | source.subscribe(PublishOnSubscriber[]) | source.subscribe(MergeSequentialInner[]) |
| 数据流 | round-robin drain → 各 rail | 各 Worker 异步消费上游 rail | 各 inner 的 queue → drainLoop → actual |
| 完成 | done=true, 广播 onComplete 给所有 rail | 各 Worker 独立完成 | DONE 递减至 0, actual.onComplete() |
| 取消 | cancelled=true, queue.clear() | 各 Worker 独立取消 | cancelAll(), cleanup() |
