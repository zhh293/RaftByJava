# ParallelFlux 并行处理（易懂版）

> **Reactor Core 源码解析系列 · 第 13 篇 · 易懂版**
> 用"多车道并行"的类比，把 ParallelFlux 的并行机制从头到尾讲清楚。

---

## 一、从一个真实的性能问题说起

假设你有一个图像处理服务，需要批量处理 1000 张图片——每张图片要做缩放、加水印、压缩。单张处理耗时约 50ms，1000 张串行处理就是 50 秒。用户等不了这么久。

你可能会这样写：

```java
Flux.range(1, 1000)
    .flatMap(imageId -> 
        Mono.fromCallable(() -> processImage(imageId))
            .subscribeOn(Schedulers.boundedElastic())
    , 16)  // 并发 16 个
    .collectList()
    .block();
```

这能工作，但有没有更高效的方式？Reactor 提供了另一个选择：

```java
Flux.range(1, 1000)
    .parallel(4)                    // 分成 4 条轨道
    .runOn(Schedulers.parallel())   // 每条轨道用独立线程
    .map(this::processImage)        // 每条轨道独立处理
    .sequential()                   // 合并回单一 Flux
    .collectList()
    .block();
```

两种写法都能并行处理，但底层机制完全不同。哪一种更适合你的场景？要回答这个问题，我们需要深入理解 `ParallelFlux` 的工作原理。

---

## 二、单车道公路 vs 多车道并行

### 2.1 普通 Flux 是"单车道公路"

普通 `Flux` 的数据流就像一条单车道公路——数据一辆接一辆地通过，前一辆没走完后一辆就不能走。即使你在中间加了 `publishOn` 切换线程，同一时刻也只有一个数据在处理。

```
普通 Flux 的数据流：
Source: [1, 2, 3, 4, 5, 6, 7, 8]
              │
              ▼
         [处理 1] → [处理 2] → [处理 3] → ... → [处理 8]
         
同一时刻只有一个在处理
```

如果你用 `flatMap` + `subscribeOn`，相当于在单车道上开了多个"岔路口"，每个岔路口可以并行处理，但数据分发和结果合并都有额外开销。

### 2.2 ParallelFlux 是"多车道公路"

`ParallelFlux` 把一条路直接分成 N 条车道，数据按 round-robin（轮转）方式分配到各车道，每条车道有自己的线程和完整的操作符链，N 辆车同时跑。

```
ParallelFlux 的数据流：
Source: [1, 2, 3, 4, 5, 6, 7, 8]
              │
         parallel(4) — 轮转分配
              │
    ┌─────────┼─────────┬─────────┐
    ▼         ▼         ▼         ▼
 rail[0]: [1, 5]  rail[1]: [2, 6]  rail[2]: [3, 7]  rail[3]: [4, 8]
    │         │         │         │
 Worker-0  Worker-1  Worker-2  Worker-3
    │         │         │         │
 [处理1]   [处理2]   [处理3]   [处理4]    ← 4 个同时处理！
 [处理5]   [处理6]   [处理7]   [处理8]
    │         │         │         │
    └─────────┴─────────┴─────────┘
              │
         sequential() — 合并输出
```

关键区别：**ParallelFlux 的每条轨道有自己独立的操作符链和线程**，数据在同一条轨道内顺序处理，但多条轨道之间是真正并行的。

### 2.3 创建 ParallelFlux

```java
// 方式一：从普通 Flux 转换
ParallelFlux<Integer> parallel = Flux.range(1, 1000).parallel(4);

// 方式二：直接创建
ParallelFlux<Integer> parallel = ParallelFlux.from(
    Flux.range(1, 1000), 
    4,                                    // 并行度（轨道数）
    Queues.SMALL_BUFFER_SIZE,             // 预取量
    Queues::small                         // 队列工厂
);
```

⚠️ **踩坑提醒**：`parallel(4)` 只是"分车道"，还没有分配线程。如果你不加 `runOn()`，所有轨道还是在同一个线程上执行，完全失去了并行的意义。一定要配合 `runOn()` 使用：

```java
// 正确写法
Flux.range(1, 1000)
    .parallel(4)
    .runOn(Schedulers.parallel())  // 必须加这行！
    .map(this::processImage)
    .sequential();

// 错误写法（没有 runOn，不会并行）
Flux.range(1, 1000)
    .parallel(4)
    .map(this::processImage)  // 还是在调用线程上执行
    .sequential();
```

---

## 三、ParallelFlux 的核心概念：rail（轨道）

### 3.1 什么是 rail

`ParallelFlux` 不像普通 `Flux` 那样只有一个 `Subscriber` 消费数据，而是有 N 个 `Subscriber`，每个消费一条"轨道"上的数据。这个 N 就是 `parallelism()` 的值。

```java
public abstract class ParallelFlux<T> implements CorePublisher<T> {
    public abstract int parallelism();
    
    // 注意：接受的是 Subscriber 数组，不是单个 Subscriber！
    public abstract void subscribe(CoreSubscriber<? super T>[] subscribers);
}
```

注意 `subscribe()` 的签名——它接受的是**数组**而不是单个 Subscriber。每个 Subscriber 消费一个 rail，这就是"多轨"的本质。

### 3.2 默认并行度为什么等于 CPU 核心数？

```java
public static <T> ParallelFlux<T> from(Publisher<? extends T> source) {
    // 默认并行度 = Schedulers.DEFAULT_POOL_SIZE（通常等于 CPU 核心数）
    return from(source, 
        Schedulers.DEFAULT_POOL_SIZE, 
        Queues.SMALL_BUFFER_SIZE, 
        Queues.small());
}
```

因为 `ParallelFlux` 的典型场景是 **CPU 密集型计算**。如果并行度大于核心数，线程之间会竞争 CPU 时间片，上下文切换的开销反而降低性能。

⚠️ **踩坑提醒**：如果你的任务是 IO 密集型（如 HTTP 调用、数据库查询），不要用 `ParallelFlux`。IO 密集型任务的瓶颈不是 CPU，而是 IO 等待。此时用 `flatMap` + `subscribeOn(boundedElastic)` 更合适，因为 `boundedElastic` 线程池可以创建远多于 CPU 核心数的线程来并发等待 IO。

### 3.3 从并行回到串行：sequential()

`ParallelFlux` 最终通常需要合并回单一的 `Flux`，让下游消费者正常订阅：

```java
public final Flux<T> sequential() {
    return sequential(Queues.SMALL_BUFFER_SIZE);
}

public final Flux<T> sequential(int prefetch) {
    return Flux.onAssembly(new ParallelMergeSequential<>(this, prefetch, Queues.small()));
}
```

`sequential()` 通过 `ParallelMergeSequential` 将 N 个轨道的数据合并为一个 `Flux`。

⚠️ **踩坑提醒**：`sequential()` **不保证输出顺序与输入顺序一致**。它是"谁有数据就先输出谁"的合并策略。如果你需要保持原始顺序，需要额外用 `sorted()` 或考虑其他方案。

---

## 四、ParallelSource：Round-Robin 数据分发引擎

### 4.1 数据怎么分配到各轨道

`ParallelSource` 是 `parallel(n)` 的核心实现。它订阅上游 source，然后通过 round-robin（轮转）方式将数据分发到 N 个轨道。

```
数据: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
轨道数: 3

轮转分配结果：
rail[0]: 1, 4, 7, 10
rail[1]: 2, 5, 8, 11
rail[2]: 3, 6, 9, 12
```

第一个数据给 rail[0]，第二个给 rail[1]，第三个给 rail[2]，第四个又给 rail[0]……如此循环。

### 4.2 核心字段

```java
static final class ParallelSourceMain<T> implements InnerConsumer<T> {
    final CoreSubscriber<? super T>[] subscribers;  // N 个轨道的 Subscriber
    final AtomicLongArray requests;                  // 每个轨道的 request 计数
    final long[] emissions;                          // 每个轨道的已发送计数
    int index;            // 当前 round-robin 指针
    Queue<T> queue;       // 从上游收到的数据缓冲队列
    volatile int wip;     // 工作计数器（drain 串行化）
}
```

关键设计决策：

- **`requests` 用 `AtomicLongArray` 而不是 `AtomicLong[]`**：`AtomicLongArray` 使用连续内存布局，对 CPU 缓存更友好。想象 N 个轨道的 request 计数排成一排放在内存中，CPU 一次就能把整排加载到缓存行。
- **`emissions` 用普通 long 数组**：因为只在 `drain()` 循环内访问，WIP 机制保证了同一时刻只有一个线程在 drain，不需要原子操作。
- **`index` 是 round-robin 指针**：同样只在 drain 循环内更新。

### 4.3 drainAsync() —— 轮转分发的核心

```java
void drainAsync() {
    int missed = 1;
    Queue<T> q = queue;
    CoreSubscriber<? super T>[] a = this.subscribers;
    AtomicLongArray r = this.requests;
    long[] e = this.emissions;
    int n = e.length;
    int idx = index;

    for (;;) {
        int notReady = 0;
        for (;;) {
            // ... 取消/错误/完成检查 ...
            
            if (q.isEmpty()) break;

            long ridx = r.get(idx);    // rail[idx] 的 request
            long eidx = e[idx];        // rail[idx] 的已发送数
            if (ridx != eidx) {        // 这个轨道还有剩余 request
                T v = q.poll();
                a[idx].onNext(v);      // 发送到 rail[idx]
                e[idx] = eidx + 1;
                
                // 累积消费到 limit 时批量请求
                int c = ++consumed;
                if (c == limit) {
                    consumed = 0;
                    s.request(c);      // 向上游补充
                }
                notReady = 0;
            } else {
                notReady++;            // 这个轨道暂时没 request
            }

            idx++;
            if (idx == n) { idx = 0; } // round-robin 回绕

            if (notReady == n) break;  // 所有轨道都没 request，退出
        }
        // WIP 退出逻辑
        index = idx;
        missed = WIP.addAndGet(this, -missed);
        if (missed == 0) break;
    }
}
```

Round-Robin 算法详解：

1. 维护一个 `index` 指针，从 0 到 N-1 循环。
2. 每次尝试向 `index` 对应的轨道发送数据，前提是该轨道还有未满足的 request（`requests[idx] != emissions[idx]`）。
3. 如果当前轨道没有 request，跳过它（`notReady++`），继续下一个。
4. 如果连续 N 个轨道都没有 request（`notReady == n`），退出等待。

**为什么不用取模（%）而是 if + 重置？**

```java
idx++;
if (idx == n) { idx = 0; }
```

这比 `idx = (idx + 1) % n` 更高效。取模运算在大多数 CPU 上需要除法指令，而条件判断+重置只需要一次比较和一次赋值。在每秒可能执行数百万次的热路径上，这个优化是有意义的。

### 4.4 只有所有轨道都就绪才开始分发

```java
// ParallelSourceInner 的 request 方法
public void request(long n) {
    // ... 更新 requests[index] ...
    if (parent.subscriberCount == length) {
        parent.drain();  // 只有所有轨道都 subscribe 后才 drain
    }
}
```

`parent.subscriberCount == length` 这个条件确保：**只有当所有轨道的 Subscriber 都已设置完毕后，才开始分发数据**。这避免了部分轨道还没 subscribe 时就开始分发，导致数据丢失。

---

## 五、ParallelRunOn：给每条轨道分配线程

### 5.1 工作原理

`runOn(Scheduler)` 为每条轨道创建一个独立的 `Worker`，使得后续操作在各自的线程上执行。

```java
public void subscribe(CoreSubscriber<? super T>[] subscribers) {
    int n = subscribers.length;
    CoreSubscriber<T>[] parents = new CoreSubscriber[n];
    
    for (int i = 0; i < n; i++) {
        Worker w = scheduler.createWorker();  // 每个轨道一个 Worker
        parents[i] = new FluxPublishOn.PublishOnSubscriber<>(
            subscribers[i], scheduler, w, true, prefetch, prefetch, queueSupplier);
    }
    source.subscribe(parents);
}
```

**关键洞察**：`ParallelRunOn` 复用了 `FluxPublishOn.PublishOnSubscriber`——也就是说，每条轨道的线程切换机制与普通 `publishOn()` 完全相同。这是一种优雅的代码复用。

⚠️ **踩坑提醒**：`ParallelRunOn` 需要为每条轨道创建一个 `Worker`。如果你用 `Schedulers.single()`（只有一个线程的 Scheduler），所有轨道会共享同一个线程，完全失去并行效果。推荐使用 `Schedulers.parallel()`（CPU 核心数个线程）或 `Schedulers.boundedElastic()`（弹性线程池）。

```java
// 正确：每个轨道有独立线程
.parallel(4).runOn(Schedulers.parallel())

// 错误：所有轨道共享一个线程，等于没并行
.parallel(4).runOn(Schedulers.single())

// IO 密集型任务可以用 boundedElastic
.parallel(4).runOn(Schedulers.boundedElastic())
```

---

## 六、并行操作符：每条轨道独立执行

### 6.1 ParallelMap —— 每条轨道独立映射

```java
final class ParallelMap<T, R> extends ParallelFlux<R> {
    public void subscribe(CoreSubscriber<? super R>[] subscribers) {
        int n = subscribers.length;
        CoreSubscriber<? super T>[] parents = new CoreSubscriber[n];
        for (int i = 0; i < n; i++) {
            parents[i] = new FluxMap.MapSubscriber<>(subscribers[i], mapper);
        }
        source.subscribe(parents);
    }
}
```

**模式总结**：`ParallelMap` 不需要重新实现映射逻辑，只需要为每条轨道创建一个 `FluxMap.MapSubscriber`。这是 ParallelFlux 操作符的通用模式——**包装-转发**：

1. 为每条轨道的 Subscriber 创建对应的 Flux 操作符 Subscriber。
2. 将包装后的 Subscriber 数组传给上游。

这意味着 `ParallelMap`、`ParallelFilter` 等并行操作符与对应的 Flux 操作符**共享相同的内部实现**，零额外实现成本。

### 6.2 ParallelFilter —— 每条轨道独立过滤

```java
final class ParallelFilter<T> extends ParallelFlux<T> {
    public void subscribe(CoreSubscriber<? super T>[] subscribers) {
        int n = subscribers.length;
        CoreSubscriber<? super T>[] parents = new CoreSubscriber[n];
        for (int i = 0; i < n; i++) {
            parents[i] = new FluxFilter.FilterSubscriber<>(subscribers[i], predicate);
        }
        source.subscribe(parents);
    }
}
```

同样复用了 `FluxFilter.FilterSubscriber`。

### 6.3 ConditionalSubscriber 优化

两个操作符都检查了 `subscribers[0] instanceof Fuseable.ConditionalSubscriber`。这是一个融合优化：`ConditionalSubscriber` 的 `tryOnNext(T)` 方法可以原地判断是否接受元素，避免了不必要的 `request(1)` 回调。

对于 `filter()` 操作符，如果过滤掉了 90% 的元素，传统方式需要 10 次 `request` 才能得到 1 个通过的元素。用 `ConditionalSubscriber` 可以原地判断，不通过的元素直接跳过，大幅减少上下游之间的往返次数。

---

## 七、ParallelReduceSeed：每条轨道独立归约

### 7.1 并行归约的思想

归约（reduce）是把多个数据合并为一个的过程。并行归约的思想是：先把数据分成 N 组，每组独立归约，再把 N 个结果合并。

```
数据: [1, 2, 3, 4, 5, 6, 7, 8]
parallel(4) 分配:
  rail[0]: [1, 5] → reduce → 6
  rail[1]: [2, 6] → reduce → 8
  rail[2]: [3, 7] → reduce → 10
  rail[3]: [4, 8] → reduce → 12
合并: [6, 8, 10, 12]
```

### 7.2 每条轨道独立创建初始值

```java
public void subscribe(CoreSubscriber<? super R>[] subscribers) {
    int n = subscribers.length;
    CoreSubscriber<T>[] parents = new CoreSubscriber[n];
    for (int i = 0; i < n; i++) {
        R initialValue = initialSupplier.get();  // 每条轨道独立调用
        parents[i] = new ParallelReduceSeedSubscriber<>(subscribers[i], initialValue, reducer);
    }
    source.subscribe(parents);
}
```

⚠️ **踩坑提醒**：**每条轨道独立调用 `initialSupplier.get()`**。如果你的初始值是可变对象（如 `ArrayList`），必须为每条轨道创建独立实例。如果所有轨道共享同一个 `ArrayList`，多个线程会并发修改它，导致数据损坏。

```java
// 错误：所有轨道共享同一个 list
List<Integer> sharedList = new ArrayList<>();
parallel.reduce(() -> sharedList, (list, v) -> { list.add(v); return list; });

// 正确：每条轨道创建独立的 list
parallel.reduce(ArrayList::new, (list, v) -> { list.add(v); return list; });
```

### 7.3 为什么 onNext 需要 synchronized

```java
public void onNext(T t) {
    if (done) return;
    synchronized (this) {
        R v = reducer.apply(accumulator, t);
        accumulator = v;
    }
}
```

虽然每条轨道通常运行在独立线程上，但 Reactive Streams 规范允许 Subscription 的方法在任意线程调用。某些场景下 Subscriber 可能被多个线程调用（如上游使用了 merge 操作符）。`synchronized` 提供了安全保障，即使开销很小（无竞争时 JVM 会优化为偏向锁）。

---

## 八、ParallelMergeSequential：合并 N 轨为单一 Flux

### 8.1 合并策略

`sequential()` 通过 `ParallelMergeSequential` 将 N 个轨道合并为单一 `Flux`。合并策略是遍历所有轨道的队列，"谁有数据就先输出谁"：

```java
void drainLoop() {
    for (;;) {
        while (e != r) {
            boolean empty = true;
            for (int i = 0; i < n; i++) {
                T v = s[i].queue.poll();
                if (v != null) {
                    empty = false;
                    a.onNext(v);
                    s[i].requestOne();
                    if (++e == r) break;
                }
            }
            if (empty) break;
        }
    }
}
```

⚠️ **踩坑提醒**：这不是严格的 round-robin——而是"谁有数据就发谁的"。如果某条轨道特别慢（比如处理耗时很长），其他轨道的数据会先输出。这意味着 `sequential()` **不保证输出顺序与输入顺序一致**。

### 8.2 快速路径优化

```java
void onNext(MergeSequentialInner<T> inner, T value) {
    if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
        if (requested != 0) {
            actual.onNext(value);  // 直接发送，不经队列
            inner.requestOne();
        } else {
            inner.getQueue(queueSupplier).offer(value);  // 有 request 时才入队
        }
        if (WIP.decrementAndGet(this) == 0) return;
    } else {
        inner.getQueue(queueSupplier).offer(value);
        if (WIP.getAndIncrement(this) != 0) return;
    }
    drainLoop();
}
```

**快速路径**：当 WIP 为 0 且下游有 request 时，直接 `actual.onNext(value)` 而不需要经过队列缓冲。这避免了 queue 的 offer/poll 开销。只有在并发竞争或没有 request 时才走队列缓冲路径。

### 8.3 批量请求优化

```java
void requestOne() {
    long p = produced + 1;
    if (p == limit) {
        produced = 0;
        s.request(p);  // 累积到 limit 才批量请求
    } else {
        produced = p;
    }
}
```

不是每消费一个元素就 `request(1)`，而是累积到 `limit`（prefetch 的 75%）时批量请求。对于默认 prefetch 256，limit 为 192。这减少了上下游之间的请求往返次数。

---

## 九、ParallelFlux vs flatMap：两种并行模型对比

### 9.1 架构区别

**flatMap 的并行**：每个元素创建一个新的内部 Publisher，每个内部 Publisher 可以运行在不同线程上。

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

**ParallelFlux 的并行**：源数据分成固定的 N 条轨道，每条轨道有自己的线程和完整的操作符链。

```
Source ──▶ parallel(4) ──▶ runOn(scheduler)
                │
                ├── rail[0]: Worker-0 上的完整操作符链
                ├── rail[1]: Worker-1 上的完整操作符链
                ├── rail[2]: Worker-2 上的完整操作符链
                └── rail[3]: Worker-3 上的完整操作符链
                         │
                         ▼ sequential() 合并输出
```

### 9.2 ParallelFlux 在哪些场景更高效

1. **订阅开销**：flatMap 每个元素都可能创建一个新的内部 Publisher 和 Subscriber，产生大量短生命周期对象，增加 GC 压力。ParallelFlux 的轨道数固定，Subscriber 在整个生命周期内只创建一次。

2. **线程固定**：ParallelFlux 的每条轨道绑定一个 Worker，数据在同一个线程上流过整个操作符链，减少了上下文切换。flatMap 的内部 Publisher 可能在线程间漂移，merge 时需要更多同步操作。

3. **背压简单**：ParallelFlux 的每条轨道独立背压，不需要 flatMap 那样复杂的 WIP 协调。

4. **代码复用**：`ParallelMap` 复用 `FluxMap.MapSubscriber`，`ParallelFilter` 复用 `FluxFilter.FilterSubscriber`，`ParallelRunOn` 复用 `FluxPublishOn.PublishOnSubscriber`。零额外实现成本。

### 9.3 什么时候不应该用 ParallelFlux

- **IO 密集型且延迟差异大的任务**：round-robin 分配是静态的，如果某些元素处理特别慢，会导致某条轨道积压而其他轨道空闲。flatMap 的"谁快谁先处理"模型更适合这种场景。

- **元素数量少于 parallelism**：如果只有 3 个元素但 `parallelism=8`，大部分轨道会空转，白白创建了 Worker。

- **需要保持元素顺序**：`sequential()` 不保证顺序。如果需要排序合并，需要用 `sorted(Comparator)`，但这有额外开销。

### 9.4 实际选择建议

```java
// 场景一：CPU 密集型，数据量大，处理时间均匀
// → 用 ParallelFlux
Flux.range(1, 10000)
    .parallel(8)
    .runOn(Schedulers.parallel())
    .map(this::cpuIntensiveTask)
    .sequential();

// 场景二：IO 密集型，延迟差异大
// → 用 flatMap + subscribeOn
Flux.range(1, 100)
    .flatMap(id -> 
        Mono.fromCallable(() -> httpCall(id))
            .subscribeOn(Schedulers.boundedElastic())
    , 16);

// 场景三：数据量少，不值得并行
// → 直接 map
Flux.range(1, 10)
    .map(this::process);
```

---

## 十、归纳表格

### ParallelFlux 操作符对照表

| 操作符类 | 对应方法 | 作用 | 复用的 Flux 操作符 | 类比 |
|---------|---------|------|-------------------|------|
| `ParallelSource` | `parallel(n)` | 将 Flux 拆分为 N 条轨道 | 无（独立实现） | 分车道 |
| `ParallelRunOn` | `runOn(Scheduler)` | 为每条轨道分配独立线程 | `FluxPublishOn.PublishOnSubscriber` | 给每条车道配一个司机 |
| `ParallelMap` | `map(Function)` | 每条轨道独立映射 | `FluxMap.MapSubscriber` | 每条车道各自加工 |
| `ParallelFilter` | `filter(Predicate)` | 每条轨道独立过滤 | `FluxFilter.FilterSubscriber` | 每条车道各自安检 |
| `ParallelFlatMap` | `flatMap(Function)` | 每条轨道内部扁平映射 | `FluxFlatMap.FlatMapMain` | 每条车道再分岔 |
| `ParallelReduceSeed` | `reduce(Supplier, BiFunction)` | 每条轨道独立归约 | 独立实现 | 每条车道各自汇总 |
| `ParallelMergeSequential` | `sequential()` | 合并 N 条轨道为单一 Flux | 无（独立实现） | 多车道汇合为一条 |
| `ParallelMergeReduce` | `reduce(BiFunction)` | 合并 N 条轨道的归约结果为 Mono | 独立实现 | 各车道汇总后再总汇 |

### ParallelFlux vs flatMap 对比表

| 特性 | ParallelFlux | flatMap |
|------|-------------|---------|
| **并行模型** | 固定 N 条轨道 | 每个元素一个内部 Publisher |
| **线程绑定** | 每条轨道绑定一个 Worker | 内部 Publisher 可能在线程间漂移 |
| **对象创建** | 固定 N 个 Subscriber | 每个元素创建新的 Publisher/Subscriber |
| **背压** | 每条轨道独立 | 复杂的 WIP 协调 |
| **数据分配** | Round-robin（静态） | 谁快谁先处理（动态） |
| **顺序保证** | 不保证 | 不保证 |
| **适用场景** | CPU 密集型、数据量大、处理均匀 | IO 密集型、延迟差异大 |
| **GC 压力** | 低 | 高（大量短生命周期对象） |

### 关键设计决策对照表

| 设计决策 | 选择 | 原因 |
|---------|------|------|
| 数据分发策略 | Round-Robin | 简单高效，均匀分配，O(1) 分发 |
| 并行度默认值 | CPU 核心数 | CPU 密集型场景最优 |
| request 数组类型 | `AtomicLongArray` | 连续内存，缓存友好 |
| drain 串行化 | WIP (AtomicIntegerFieldUpdater) | 标准的无锁串行化模式 |
| 操作符实现 | 复用 Flux 的 Subscriber | 零代码重复，一致的行为 |
| 合并策略 | 遍历 poll（不保序） | 简单、低延迟 |
| 上游请求策略 | 批量请求 (limit = 75% of prefetch) | 减少请求往返 |
| round-robin 回绕 | if + 重置 | 比取模运算更快 |

### ParallelFlux 生命周期状态表

| 阶段 | ParallelSource | ParallelRunOn | ParallelMergeSequential |
|------|---------------|---------------|------------------------|
| **创建** | 构造 queue, requests[], emissions[] | 创建 N 个 Worker | 创建 N 个 MergeSequentialInner |
| **订阅** | source.subscribe(ParallelSourceMain) | source.subscribe(PublishOnSubscriber[]) | source.subscribe(MergeSequentialInner[]) |
| **数据流** | round-robin drain → 各轨道 | 各 Worker 异步消费上游轨道 | 各 inner 的 queue → drainLoop → actual |
| **完成** | done=true, 广播 onComplete 给所有轨道 | 各 Worker 独立完成 | DONE 递减至 0, actual.onComplete() |
| **取消** | cancelled=true, queue.clear() | 各 Worker 独立取消 | cancelAll(), cleanup() |
