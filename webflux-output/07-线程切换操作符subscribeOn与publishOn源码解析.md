# 线程切换操作符 subscribeOn 与 publishOn 源码解析

> **Reactor Core 源码深度研究系列 · 第 07 篇**
> 从订阅线程到发射线程，拆解 Reactor 两大线程切换操作符的内部机制：队列缓冲、WIP 排空、prefetch 补货与线程感知 request 转发。

---

## 一、线程切换时机对比总览图

```
                    subscribeOn 的作用域
                    ┌──────────────────────────────────────────────────┐
                    │         影响订阅(subscribe)发生在哪个线程          │
                    │                                                   │
  Flux.just(1,2,3)  │  .map(x -> x*2)  .filter(x -> x>2)             │  .subscribe()
       ▲            │       ▲                  ▲                       │      │
       │            │       │                  │                       │      │
       │   source产生数据   │       操作符处理数据                      │      ▼
       │ (在subscribeOn     │    (在subscribeOn                        │  subscriber
       │  指定的线程上)      │     指定的线程上)                        │  接收数据
       └────────────┴───────┴──────────────────┘                       │
                                                                       │
  ═══════════════════════════════════════════════════════════════════════

  Flux.just(1,2,3)  .map(x -> x*2)  .publishOn(scheduler)  .filter(x -> x>2)  .subscribe()
       ▲                  ▲                   │                    ▲                  │
       │                  │                   │                    │                  │
  source产生数据    操作符处理数据       ─────┼──────────    操作符处理数据        subscriber
  (在订阅线程上)   (在订阅线程上)       线程  │  切换点      (在publishOn          接收数据
                                       边界  │              指定的线程上)       (在publishOn
                                             │                                  指定的线程上)
                                             │
                                    publishOn 的作用域
                                    ┌──────────────────────────────────┐
                                    │  影响下游接收(onNext)发生在哪个线程  │
                                    └──────────────────────────────────┘


  时序图 —— subscribeOn:

  调用线程                  Worker 线程
     │                          │
     │  worker.schedule(parent) │
     │ ─────────────────────►  │
     │                         │ source.subscribe(this)
     │                         │ ────► source 开始产生数据
     │                         │ ◄──── onNext/onComplete 也在此线程
     │                         │ ────► actual.onNext(t)  (直接转发,不切线程)
     ▼                         ▼

  时序图 —— publishOn:

  上游线程(source)           Worker 线程
     │                          │
     │  onNext(t)               │
     │  ──► queue.offer(t)      │
     │  ──► trySchedule()       │
     │      WIP++ → 0→1         │
     │      worker.schedule()   │
     │  ─────────────────────►  │
     │                          │  run() → drain()
     │                          │  queue.poll() → actual.onNext(v)
     │                          │  (下游在Worker线程收到数据)
     ▼                          ▼
```

---

## 二、FluxPublishOn 深度解析

### 2.1 类结构总览

源码文件：`reactor/core/publisher/FluxPublishOn.java`
```java
final class FluxPublishOn<T> extends InternalFluxOperator<T, T> implements Fuseable {

    final Scheduler scheduler;
    final boolean delayError;
    final Supplier<? extends Queue<T>> queueSupplier;
    final int prefetch;
    final int lowTide;
}
```

`FluxPublishOn` 实现了 `Fuseable` 接口，表示它可以参与操作符熔合。五个核心字段：
- `scheduler`：目标调度器，用于创建 Worker
- `delayError`：是否延迟错误传递（先排空队列中的正常数据）
- `queueSupplier`：队列工厂，通常提供 `SpscArrayQueue`（单生产者单消费者数组队列）
- `prefetch`：预取量，决定初始向上游请求多少元素
- `lowTide`：低水位标记，决定何时触发补货

### 2.2 subscribeOrReturn：订阅链的组装

```java
@Override
public @Nullable CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
    Worker worker = Objects.requireNonNull(scheduler.createWorker(),
            "The scheduler returned a null worker");

    if (actual instanceof ConditionalSubscriber) {
        ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) actual;
        source.subscribe(new PublishOnConditionalSubscriber<>(cs,
                scheduler, worker, delayError, prefetch, lowTide, queueSupplier));
        return null;
    }
    return new PublishOnSubscriber<>(actual,
            scheduler, worker, delayError, prefetch, lowTide, queueSupplier);
}
```

注意两个分支：
1. 如果下游是 `ConditionalSubscriber`，使用 `PublishOnConditionalSubscriber`，它支持 `tryOnNext()` 优化
2. 否则使用普通的 `PublishOnSubscriber`

Worker 在这里被创建并绑定到 Subscriber 的生命周期中——当 Subscriber 完成或取消时，Worker 会被 dispose。

### 2.3 PublishOnSubscriber 核心字段

```java
static final class PublishOnSubscriber<T>
        implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {

    final CoreSubscriber<? super T> actual;
    final Scheduler scheduler;
    final Worker worker;
    final boolean delayError;
    final int prefetch;
    final int limit;
    final Supplier<? extends Queue<T>> queueSupplier;

    Subscription s;
    Queue<T> queue;

    volatile boolean cancelled;
    volatile boolean done;
    @Nullable Throwable error;

    volatile int wip;
    static final AtomicIntegerFieldUpdater<PublishOnSubscriber> WIP = ...;

    volatile long requested;
    static final AtomicLongFieldUpdater<PublishOnSubscriber> REQUESTED = ...;

    int sourceMode;
    long produced;
    boolean outputFused;
}
```

**`limit` 字段的计算：**

```java
this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
```

`limit` 是补货阈值。当已消费的元素数达到 `limit` 时，向上游再次发起 `request(limit)` 进行补货。`lowTide` 是一个比例参数，默认情况下 `limit = prefetch - (prefetch >> 2)` 即 prefetch 的 75%。这意味着当队列消费了 75% 的预取量后，就开始向上游补货，避免队列完全耗尽。

**为什么不在队列空时才补货？** 如果等队列空了再补货，会引入一段"空窗期"——下游在等数据、上游还没收到新的 request。这段空窗期会降低吞吐量。提前在 75% 处补货，让上游的生产和下游的消费形成流水线，使队列始终保持部分填充。

### 2.4 onSubscribe：熔合协商

```java
@Override
public void onSubscribe(Subscription s) {
    if (Operators.validate(this.s, s)) {
        this.s = s;

        if (s instanceof QueueSubscription) {
            @SuppressWarnings("unchecked") QueueSubscription<T> f =
                    (QueueSubscription<T>) s;

            int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

            if (m == Fuseable.SYNC) {
                sourceMode = Fuseable.SYNC;
                queue = f;
                done = true;
                actual.onSubscribe(this);
                return;
            }
            if (m == Fuseable.ASYNC) {
                sourceMode = Fuseable.ASYNC;
                queue = f;
                actual.onSubscribe(this);
                s.request(Operators.unboundedOrPrefetch(prefetch));
                return;
            }
        }

        queue = queueSupplier.get();
        actual.onSubscribe(this);
        s.request(Operators.unboundedOrPrefetch(prefetch));
    }
}
```

这里的熔合协商传入了 `Fuseable.ANY | Fuseable.THREAD_BARRIER`。`THREAD_BARRIER` 标志告诉上游"我会在不同线程上消费你的数据"——这是 `publishOn` 的核心语义。

三种结果：
1. **SYNC 熔合**：上游是同步源（如 `FluxArray`），直接把上游的 `QueueSubscription` 作为队列使用。设置 `done = true` 因为 SYNC 源的所有数据已经在队列中了。不需要调 `request()`。
2. **ASYNC 熔合**：上游支持异步熔合，上游的 `QueueSubscription` 既是队列又是 Subscription。需要调 `request(prefetch)` 启动数据流。
3. **无熔合**：创建独立的队列（`queueSupplier.get()`），通过 `onNext()` 入队。需要调 `request(prefetch)` 启动数据流。

### 2.5 onNext：入队与调度

```java
@Override
public void onNext(T t) {
    if (sourceMode == ASYNC) {
        trySchedule(this, null, null /* t always null */);
        return;
    }

    if (done) {
        Operators.onNextDropped(t, actual.currentContext());
        return;
    }

    if (cancelled) {
        Operators.onDiscard(t, actual.currentContext());
        return;
    }

    if (!queue.offer(t)) {
        Operators.onDiscard(t, actual.currentContext());
        error = Operators.onOperatorError(s,
                Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
                t, actual.currentContext());
        done = true;
    }
    trySchedule(this, null, t);
}
```

**ASYNC 模式下的 `onNext(null)`：** 当 `sourceMode == ASYNC` 时，上游调用 `onNext(t)` 实际上是一个通知信号（t 被忽略），告诉 publishOn"队列里有新数据了"。数据已经在共享的 `QueueSubscription` 队列中，不需要再 `offer()`。

**非熔合模式的入队：** `queue.offer(t)` 将数据放入队列。如果队列满了（`offer` 返回 `false`），设置 `error` 并标记 `done = true`，这是一个背压溢出错误。

### 2.6 trySchedule 与 WIP 门控

```java
void trySchedule(
        @Nullable Subscription subscription,
        @Nullable Throwable suppressed,
        @Nullable Object dataSignal) {
    if (WIP.getAndIncrement(this) != 0) {
        if (cancelled) {
            if (sourceMode == ASYNC) {
                queue.clear();
            }
            else {
                Operators.onDiscard(dataSignal, actual.currentContext());
            }
        }
        return;
    }

    try {
        worker.schedule(this);
    }
    catch (RejectedExecutionException ree) {
        // 清理队列 + 报错
    }
}
```

**WIP（Work-In-Progress）门控是 publishOn 的核心并发控制机制。** `WIP` 是一个原子整数计数器。`getAndIncrement()` 返回旧值：
- 如果旧值为 0，说明当前没有 drain 正在执行，需要调度 `worker.schedule(this)` 启动一轮新的 drain
- 如果旧值不为 0，说明已经有一轮 drain 正在执行（或已被调度），只需要增加计数即可——正在执行的 drain 循环会检测到新的 work 并继续处理

**为什么不是每次 `onNext` 都调度一次 `worker.schedule`？** 因为 Worker 的 `schedule()` 意味着向执行器提交一个 `Runnable`。如果每个 `onNext` 都提交一次，当上游高速发射时，会产生大量的任务提交开销。WIP 门控确保同一时刻只有一个 drain 任务在 Worker 上运行，后续的 `onNext` 只是增加计数，drain 循环会自动处理所有积压的数据。

### 2.7 run() 方法：三种 drain 模式

```java
@Override
public void run() {
    if (outputFused) {
        runBackfused();
    }
    else if (sourceMode == Fuseable.SYNC) {
        runSync();
    }
    else {
        runAsync();
    }
}
```

`PublishOnSubscriber` 实现了 `Runnable`，`run()` 方法就是 Worker 执行的任务体。根据 `sourceMode` 和 `outputFused` 选择不同的 drain 策略。

#### 2.7.1 runAsync：标准异步排空

```java
void runAsync() {
    int missed = 1;

    final Subscriber<? super T> a = actual;
    final Queue<T> q = queue;

    long e = produced;

    for (; ; ) {
        long r = requested;

        while (e != r) {
            boolean d = done;
            T v;

            try {
                v = q.poll();
            }
            catch (Throwable ex) { ... }

            boolean empty = v == null;

            if (checkTerminated(d, empty, a, v)) {
                return;
            }

            if (empty) {
                break;
            }

            a.onNext(v);

            e++;
            if (e == limit) {
                if (r != Long.MAX_VALUE) {
                    r = REQUESTED.addAndGet(this, -e);
                }
                s.request(e);
                e = 0L;
            }
        }

        if (e == r && checkTerminated(done, q.isEmpty(), a, null)) {
            return;
        }

        int w = wip;
        if (missed == w) {
            produced = e;
            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                break;
            }
        }
        else {
            missed = w;
        }
    }
}
```

这段代码的核心逻辑是**双层循环**：

**内层循环**（`while (e != r)`）：
1. 从队列 `poll()` 取元素
2. 如果队列空了或已满足下游的 `requested` 需求，退出内层循环
3. 调用 `actual.onNext(v)` 将元素推送给下游（此时已在 Worker 线程上）
4. 计数器 `e` 递增。当 `e == limit` 时，向上游发起补货 `s.request(e)` 并重置 `e = 0`

**外层循环**（`for (;;)`）：
1. 检查是否有新的 work（`missed == w` 判断）
2. 如果 `WIP.addAndGet(this, -missed)` 返回 0，说明没有新的 work 产生，退出整个 drain

**补货机制的细节：**

```java
if (e == limit) {
    if (r != Long.MAX_VALUE) {
        r = REQUESTED.addAndGet(this, -e);
    }
    s.request(e);
    e = 0L;
}
```

当消费了 `limit` 个元素后：
1. 先从 `REQUESTED` 中减去已消费的量（除非是无界请求 `Long.MAX_VALUE`）
2. 向上游请求同样数量的新元素
3. 重置消费计数器

这实现了一个滑动窗口式的背压控制：下游请求 N 个 → publishOn 向上游预取 prefetch 个 → 消费 limit 个后再向上游补 limit 个。

#### 2.7.2 runSync：同步源的简化排空

```java
void runSync() {
    int missed = 1;
    final Subscriber<? super T> a = actual;
    final Queue<T> q = queue;
    long e = produced;

    for (; ; ) {
        long r = requested;

        while (e != r) {
            T v;
            try {
                v = q.poll();
            }
            catch (Throwable ex) { ... }

            if (cancelled) { ... return; }
            if (v == null) {
                doComplete(a);
                return;
            }

            a.onNext(v);
            e++;
        }

        if (cancelled) { ... return; }

        if (q.isEmpty()) {
            doComplete(a);
            return;
        }

        int w = wip;
        if (missed == w) {
            produced = e;
            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                break;
            }
        }
        else {
            missed = w;
        }
    }
}
```

与 `runAsync` 的区别：
1. **不需要补货**：SYNC 模式下，所有数据已经在队列中（`done = true`），不需要向上游 `request()`
2. **`v == null` 意味着完成**：SYNC 队列返回 `null` 表示数据耗尽，直接调 `doComplete()`
3. **不检查 `done` 标志**：因为在 `onSubscribe` 时已经设置了 `done = true`

### 2.8 request 的传递

```java
@Override
public void request(long n) {
    if (Operators.validate(n)) {
        Operators.addCap(REQUESTED, this, n);
        trySchedule(this, null, null);
    }
}
```

下游调用 `request(n)` 时：
1. 将 n 累加到 `REQUESTED` 字段（`addCap` 是有上限的加法，防止溢出到负数）
2. 调用 `trySchedule()` 触发一轮 drain

**`request` 不直接传递给上游！** publishOn 自己管理与上游的 request 协议：初始 `request(prefetch)` + 后续的 `request(limit)` 补货。下游的 `request(n)` 只是告诉 publishOn "我能接收 n 个元素"，publishOn 根据这个信息决定从队列中 drain 多少个。

### 2.9 cancel 与资源清理

```java
@Override
public void cancel() {
    if (cancelled) {
        return;
    }

    cancelled = true;
    s.cancel();
    worker.dispose();

    if (WIP.getAndIncrement(this) == 0) {
        if (sourceMode == ASYNC) {
            queue.clear();
        }
        else if (!outputFused) {
            Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
        }
    }
}
```

取消操作：
1. 标记 `cancelled = true`
2. 取消上游订阅 `s.cancel()`
3. 释放 Worker `worker.dispose()`
4. 通过 WIP 门控安全地清理队列（避免与正在进行的 drain 竞争）

### 2.10 requestFusion：输出端熔合

```java
@Override
public int requestFusion(int requestedMode) {
    if ((requestedMode & ASYNC) != 0) {
        outputFused = true;
        return ASYNC;
    }
    return NONE;
}
```

publishOn 只支持输出端的 ASYNC 熔合。当下游请求熔合时，publishOn 不再通过 `onNext()` 推送数据，而是让下游直接 `poll()` 队列。此时 drain 循环使用 `runBackfused()`——它只发出 `onNext(null)` 通知信号，下游从队列 poll 实际数据。

---

## 三、FluxSubscribeOn 深度解析

### 3.1 类结构

源码文件：`reactor/core/publisher/FluxSubscribeOn.java`
```java
final class FluxSubscribeOn<T> extends InternalFluxOperator<T, T> {

    final Scheduler scheduler;
    final boolean requestOnSeparateThread;
}
```

`requestOnSeparateThread` 是一个关键参数。当为 `true` 时，`request()` 调用也会被调度到 Worker 线程执行，而不是在调用 request 的线程上执行。

### 3.2 subscribeOrReturn：调度订阅动作

```java
@Override
public @Nullable CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
    Worker worker = Objects.requireNonNull(scheduler.createWorker(),
            "The scheduler returned a null Function");

    SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(source,
            actual, worker, requestOnSeparateThread);
    actual.onSubscribe(parent);

    try {
        worker.schedule(parent);
    }
    catch (RejectedExecutionException ree) {
        if (parent.s != Operators.cancelledSubscription()) {
            actual.onError(Operators.onRejectedExecution(ree, parent, null, null,
                    actual.currentContext()));
        }
    }
    return null;
}
```

关键序列：
1. 创建 `SubscribeOnSubscriber`（它实现了 `Runnable`）
2. **先调用 `actual.onSubscribe(parent)`**——在当前线程上让下游拿到 Subscription
3. **再调用 `worker.schedule(parent)`**——将 `source.subscribe(this)` 调度到 Worker 线程

**返回 `null` 而不是返回 subscriber**——这意味着 `subscribeOrReturn` 已经自行完成了 `source.subscribe()` 的调度，父类不需要再做额外的订阅操作。

### 3.3 SubscribeOnSubscriber 核心

```java
static final class SubscribeOnSubscriber<T> implements InnerOperator<T, T>, Runnable {

    final CoreSubscriber<? super T> actual;
    final CorePublisher<? extends T> source;
    final Worker worker;
    final boolean requestOnSeparateThread;

    volatile @Nullable Subscription s;
    static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, @Nullable Subscription> S = ...;

    volatile long requested;
    static final AtomicLongFieldUpdater<SubscribeOnSubscriber> REQUESTED = ...;

    volatile Thread thread;
    static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, Thread> THREAD = ...;
}
```

三个 volatile 字段和对应的原子更新器：
- `s`：上游的 Subscription，通过 `Operators.setOnce()` 保证只设置一次
- `requested`：在上游 Subscription 到达前累积的 request 量
- `thread`：记录 Worker 线程的引用，用于线程感知的 request 转发

### 3.4 run()：订阅动作的执行

```java
@Override
public void run() {
    THREAD.lazySet(this, Thread.currentThread());
    source.subscribe(this);
}
```

这两行代码包含了 `subscribeOn` 最核心的语义：
1. 记录当前的 Worker 线程到 `THREAD` 字段
2. 在 Worker 线程上调用 `source.subscribe(this)`

**这就是为什么 `subscribeOn` 影响的是"上游"而不是"下游"。** `source.subscribe(this)` 会触发整个上游链的订阅过程（`onSubscribe` → `request` → `onNext`），而这些过程都发生在 Worker 线程上。

### 3.5 onSubscribe：延迟 request 的处理

```java
@Override
public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
        long r = REQUESTED.getAndSet(this, 0L);
        if (r != 0L) {
            requestUpstream(r, s);
        }
    }
}
```

当上游的 `Subscription` 到达时：
1. 通过 `Operators.setOnce()` 原子地设置 `s`
2. 取出在 `s` 到达前已经累积的 `requested` 量
3. 如果有累积的请求，调用 `requestUpstream()` 转发给上游

**为什么需要累积？** 因为 `actual.onSubscribe(parent)` 在当前线程调用，下游可能立即调用 `parent.request(n)`，但此时上游的 `Subscription` 可能还没有到达（`source.subscribe()` 还在 Worker 线程上等待调度）。所以需要用 `REQUESTED` 字段缓存这些提前到达的 request。

### 3.6 request 与 requestUpstream：线程感知的转发

```java
@Override
public void request(long n) {
    if (Operators.validate(n)) {
        Subscription s = S.get(this);
        if (s != null) {
            requestUpstream(n, s);
        }
        else {
            Operators.addCap(REQUESTED, this, n);
            s = S.get(this);
            if (s != null) {
                long r = REQUESTED.getAndSet(this, 0L);
                if (r != 0L) {
                    requestUpstream(r, s);
                }
            }
        }
    }
}
```

这里有一个精妙的双重检查：
1. 先检查 `s` 是否已设置，如果是，直接转发
2. 如果不是，累积到 `REQUESTED`
3. 再次检查 `s`——因为在步骤 2 和步骤 3 之间，`onSubscribe` 可能恰好到达并设置了 `s`，但此时 `onSubscribe` 中读到的 `REQUESTED` 为 0（因为步骤 2 还没执行完），所以需要再次检查

```java
void requestUpstream(final long n, final Subscription s) {
    if (!requestOnSeparateThread || Thread.currentThread() == THREAD.get(this)) {
        s.request(n);
    }
    else {
        try {
            worker.schedule(() -> s.request(n));
        }
        catch (RejectedExecutionException ree) {
            if(!worker.isDisposed()) {
                throw Operators.onRejectedExecution(ree, this, null, null,
                        actual.currentContext());
            }
        }
    }
}
```

**线程感知的 request 转发：** 
- 如果 `requestOnSeparateThread` 为 `false`，或者当前线程就是 Worker 线程，直接调用 `s.request(n)`
- 否则，将 `s.request(n)` 调度到 Worker 线程执行

`Thread.currentThread() == THREAD.get(this)` 这个比较是引用比较（`==`），不是 `equals`，因为这里关心的是"是否是同一个线程对象"。

**为什么需要线程感知？** 如果 `request()` 总是在调用它的线程上执行，那么当下游在 main 线程调用 `request()` 时，上游数据源可能在 main 线程上被触发产生数据，绕过了 subscribeOn 指定的线程。通过将 request 转发到 Worker 线程，确保整个上游链——包括因 request 触发的数据生产——都在 Worker 线程上执行。

### 3.7 onNext/onError/onComplete：直通转发

```java
@Override
public void onNext(T t) {
    actual.onNext(t);
}

@Override
public void onError(Throwable t) {
    try {
        actual.onError(t);
    }
    finally {
        worker.dispose();
    }
}

@Override
public void onComplete() {
    actual.onComplete();
    worker.dispose();
}
```

**`subscribeOn` 不切换数据传递的线程！** `onNext`、`onError`、`onComplete` 都是直通调用——在哪个线程被上游调用，就在哪个线程传递给下游。这意味着如果上游在 Worker 线程上发射数据（这是 subscribeOn 的典型效果），那么下游也在 Worker 线程上接收数据。但如果上游内部有自己的线程切换（比如上游本身包含一个 `publishOn`），那么 `subscribeOn` 的线程不影响那之后的数据传递线程。

---

## 四、MonoPublishOn 与 MonoSubscribeOn

### 4.1 MonoPublishOn：简化的单值线程切换

源码文件：`reactor/core/publisher/MonoPublishOn.java`
```java
final class MonoPublishOn<T> extends InternalMonoOperator<T, T> {

    final Scheduler scheduler;

    static final class PublishOnSubscriber<T>
            implements InnerOperator<T, T>, Runnable {

        final CoreSubscriber<? super T> actual;
        final Scheduler scheduler;
        Subscription s;
        volatile @Nullable Disposable future;
        volatile @Nullable T value;
        volatile @Nullable Throwable error;
    }
}
```

与 `FluxPublishOn` 的巨大区别在于：Mono 最多只有一个值，不需要队列和 prefetch/limit 机制。

```java
@Override
public void onNext(T t) {
    value = t;
    trySchedule(this, null, t);
}

@Override
public void onComplete() {
    if (value == null) {
        trySchedule(null, null, null);
    }
}

void trySchedule(...) {
    if(future != null){
        return;
    }
    try {
        future = this.scheduler.schedule(this);
    }
    catch (RejectedExecutionException ree) { ... }
}
```

注意 `MonoPublishOn` 使用 `scheduler.schedule()` 而不是 `worker.schedule()`——因为 Mono 只有一个值，不需要 Worker 的 FIFO 保证。直接用 Scheduler 调度更轻量。

`onComplete()` 有一个关键判断：`if (value == null)`。如果 Mono 发射了值，`onNext` 已经调度了 run 任务（在 run 中会调 onNext + onComplete）。如果 Mono 是空的（直接 onComplete），需要单独调度。

### 4.2 MonoSubscribeOn：Mono 变体

源码文件：`reactor/core/publisher/MonoSubscribeOn.java`
```java
final class MonoSubscribeOn<T> extends InternalMonoOperator<T, T> {

    final Scheduler scheduler;

    static final class SubscribeOnSubscriber<T>
            implements InnerOperator<T, T>, Runnable {

        final Publisher<? extends T> parent;
        final Scheduler.Worker worker;
        volatile @Nullable Subscription s;
        volatile long requested;
        volatile @Nullable Thread thread;
    }
}
```

与 `FluxSubscribeOn` 的 `SubscribeOnSubscriber` 结构基本相同，但有一个区别：

```java
void trySchedule(long n, Subscription s) {
    if (Thread.currentThread() == THREAD.get(this)) {
        s.request(n);
    }
    else {
        try {
            worker.schedule(() -> s.request(n));
        }
        catch (RejectedExecutionException ree) { ... }
    }
}
```

MonoSubscribeOn 总是将 request 调度到 Worker 线程（当不在 Worker 线程上时），而 FluxSubscribeOn 有 `requestOnSeparateThread` 开关可以控制这个行为。

---

## 五、反例分析：为什么多个 subscribeOn 只有最上游的生效

考虑以下代码：

```java
Flux.range(1, 10)
    .subscribeOn(schedulerA)   // subscribeOn-1
    .map(x -> x * 2)
    .subscribeOn(schedulerB)   // subscribeOn-2
    .subscribe(System.out::println);
```

订阅过程从下游向上游传播：

```
subscribe() 触发：

1. subscribeOn-2 的 subscribeOrReturn():
   - 创建 WorkerB
   - actual.onSubscribe(parentB)  ← 在调用线程上
   - WorkerB.schedule(parentB)    ← 调度到 schedulerB 的线程

2. WorkerB 线程执行 parentB.run():
   - source.subscribe(parentB)    ← 触发上游链的订阅
   - 进入 subscribeOn-1 的 subscribeOrReturn():
     - 创建 WorkerA
     - parentB.onSubscribe(parentA) ← 在 schedulerB 线程上
     - WorkerA.schedule(parentA)    ← 调度到 schedulerA 的线程

3. WorkerA 线程执行 parentA.run():
   - source.subscribe(parentA)     ← 在 schedulerA 线程上
   - Flux.range 开始在 schedulerA 线程上产生数据
```

结果：
- **Flux.range 的数据在 schedulerA 线程上产生**（subscribeOn-1 的效果）
- subscribeOn-2 只影响了 "subscribeOn-1 的 `subscribeOrReturn()` 方法在哪个线程被调用"，但 subscribeOn-1 又把实际的 `source.subscribe()` 调度到了 schedulerA

所以最终效果是 **只有最靠近源头（最上游）的 `subscribeOn` 决定了数据的产生线程**。下游的 `subscribeOn` 只是增加了一层无意义的线程跳转。

**如果把 `subscribeOn` 换成 `publishOn` 呢？**

```java
Flux.range(1, 10)
    .publishOn(schedulerA)    // publishOn-1
    .map(x -> x * 2)
    .publishOn(schedulerB)    // publishOn-2
    .subscribe(System.out::println);
```

两个 `publishOn` 都会生效：
- `publishOn-1` 将 `map` 的执行切换到 schedulerA
- `publishOn-2` 将 `subscribe` 的接收切换到 schedulerB

这是因为每个 `publishOn` 都引入一个独立的"队列 + drain 循环"，每个都在自己的 Worker 线程上消费数据。

---

## 六、publishOn vs subscribeOn 对比总表

| 维度 | `publishOn` | `subscribeOn` |
|---|---|---|
| **实现类** | `FluxPublishOn` / `MonoPublishOn` | `FluxSubscribeOn` / `MonoSubscribeOn` |
| **切换方向** | 切换下游接收线程 | 切换上游订阅线程 |
| **影响范围** | 从 publishOn 开始，到下一个 publishOn 或链末尾 | 整个上游链（直到源头） |
| **多次使用** | 每次都生效，形成多段线程切换 | 只有最靠近源头的生效 |
| **数据传递** | 通过队列中转，异步投递 | 直通转发 `actual.onNext(t)`，不经过队列 |
| **队列** | 有（SpscArrayQueue 或熔合队列） | 无 |
| **prefetch** | 有，默认 256 | 无 |
| **补货机制** | `limit` 阈值触发 `s.request(limit)` | 无，直接转发 request |
| **WIP 门控** | 有，控制 drain 循环的并发 | 无 |
| **Worker 用途** | 执行 drain 循环（从队列取数据推给下游） | 执行 `source.subscribe(this)` |
| **Worker 生命周期** | 绑定到 Subscriber，完成/取消时 dispose | 绑定到 Subscriber，完成/取消时 dispose |
| **Fuseable** | 是（支持输入端 ANY+THREAD_BARRIER，输出端 ASYNC） | 否 |
| **request 处理** | 累积到 `REQUESTED`，由 drain 循环控制消费速率 | 可选地调度到 Worker 线程转发给上游 |
| **onNext 线程** | Worker 线程（线程切换在 onNext 路径上） | 上游 subscribe 时的线程（线程切换在 subscribe 路径上） |
| **典型场景** | I/O 回调线程切到计算线程、限制下游处理线程 | 将阻塞 source 的订阅从 main 线程转移到 boundedElastic |
| **Mono 变体差异** | 无队列/prefetch，直接用 Scheduler.schedule() | 结构与 Flux 版本相同 |
| **背压策略** | 队列满时 BACKPRESSURE_ERROR_QUEUE_FULL | 透传上游背压 |
