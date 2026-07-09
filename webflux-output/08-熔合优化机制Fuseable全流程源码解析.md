# 熔合优化机制 Fuseable 全流程源码解析

> **Reactor Core 源码深度研究系列 · 第 08 篇**
> 从 Fuseable 接口的常量定义到 SYNC/ASYNC 熔合的完整协商流程，揭示 Reactor 如何通过队列共享和 poll-拉取模式消除操作符之间的 request 开销。

---

## 一、熔合协商流程总览图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        熔合协商的全局流程                                    │
│                                                                              │
│  组装阶段(Assembly Time):                                                   │
│  source ──► operatorA ──► operatorB ──► subscriber                          │
│  (FluxArray) (FluxMapFuseable) (FluxFilterFuseable) (最终订阅者)            │
│                                                                              │
│  订阅阶段(Subscription Time):                                               │
│  subscriber.onSubscribe() ← operatorB.onSubscribe() ← operatorA.onSubscribe│
│            ← source.subscribe()                                              │
│                                                                              │
│  熔合协商在 onSubscribe() 中发生:                                            │
│                                                                              │
│  ┌─────────────┐     requestFusion(SYNC)     ┌──────────────────┐           │
│  │ FluxArray    │  ◄──────────────────────── │ FluxMapFuseable  │           │
│  │ ArraySub-    │                             │ MapFuseable-     │           │
│  │ scription    │  ────────────────────────►  │ Subscriber       │           │
│  │ (implements  │     return SYNC              │ (sourceMode=SYNC)│           │
│  │ Synchronous  │                             │                  │           │
│  │ Subscription)│                             │ poll() {         │           │
│  │              │                             │   T v = s.poll() │           │
│  │ poll() {     │  ◄─── s.poll() ────────── │   return mapper   │           │
│  │   return     │                             │     .apply(v);   │           │
│  │   array[i++];│                             │ }                │           │
│  │ }            │                             └────────┬─────────┘           │
│  └──────────────┘                                      │                     │
│                                                         │ requestFusion(SYNC)│
│                                               ┌────────▼─────────┐           │
│                                               │FluxFilterFuseable│           │
│                                               │FilterFuseable-   │           │
│                                               │Subscriber        │           │
│                                               │(sourceMode=SYNC) │           │
│                                               │                  │           │
│                                               │poll() {          │           │
│                                               │  for(;;) {       │           │
│                                               │    T v = s.poll()│           │
│                                               │    if(predicate  │           │
│                                               │       .test(v))  │           │
│                                               │      return v;   │           │
│                                               │  }               │           │
│                                               │}                 │           │
│                                               └──────────────────┘           │
│                                                                              │
│  最终效果: subscriber 调用一次 poll() →                                      │
│    FilterFuseable.poll() → MapFuseable.poll() → ArraySubscription.poll()    │
│    数据沿 poll 链同步拉取, 无 request/onNext 开销                            │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                        ASYNC 熔合 (publishOn 场景)                          │
│                                                                              │
│  asyncSource ──► publishOn ──► subscriber                                   │
│                                                                              │
│  asyncSource 的 QueueSubscription:                                          │
│    requestFusion(ANY|THREAD_BARRIER)                                        │
│    如果 asyncSource 支持 ASYNC → return ASYNC                               │
│                                                                              │
│  publishOn 的 PublishOnSubscriber:                                          │
│    sourceMode = ASYNC                                                        │
│    queue = asyncSource 的 QueueSubscription (共享队列)                       │
│                                                                              │
│  数据流:                                                                     │
│    asyncSource 将数据放入自己的队列                                           │
│    asyncSource.onNext(null) → publishOn 知道有新数据                        │
│    publishOn.run() → queue.poll() → actual.onNext(v)                        │
│                                                                              │
│  省去了: 独立的中间队列 + offer/poll 的双重拷贝                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、Fuseable 接口：熔合的基石

### 2.1 常量定义

源码文件：`reactor/core/Fuseable.java`
```java
public interface Fuseable {

    /** Indicates the QueueSubscription can't support the requested mode. */
    int NONE = 0;
    /** Indicates the QueueSubscription can perform sync-fusion. */
    int SYNC = 1;
    /** Indicates the QueueSubscription can perform only async-fusion. */
    int ASYNC = 2;
    /** Indicates the QueueSubscription should decide what fusion it performs (input only). */
    int ANY = 3;
    /**
     * Indicates that the queue will be drained from another thread
     * thus any queue-exit computation may be invalid at that point.
     */
    int THREAD_BARRIER = 0b100; //4
}
```

这五个常量构成了熔合协商的"词汇表"：

| 常量 | 值 | 位表示 | 含义 |
|---|---|---|---|
| `NONE` | 0 | `000` | 不支持熔合 / 熔合被拒绝 |
| `SYNC` | 1 | `001` | 同步熔合：数据已在队列中，可直接 poll |
| `ASYNC` | 2 | `010` | 异步熔合：数据异步到达，但共享同一个队列 |
| `ANY` | 3 | `011` | 两者都行，由上游决定（仅用于请求方） |
| `THREAD_BARRIER` | 4 | `100` | 线程边界标志，与其他模式组合使用 |

**为什么 `ANY = 3` 等于 `SYNC | ASYNC`？** 这不是巧合。`ANY` 在位运算上就是 SYNC 和 ASYNC 的或——请求方通过 `requestFusion(ANY)` 表示"我两种都能接受"，上游通过检查 `(requestedMode & SYNC) != 0` 或 `(requestedMode & ASYNC) != 0` 来决定返回哪种模式。

**`THREAD_BARRIER` 为什么是独立的 bit 位（`0b100`）？** 因为它不是一种熔合模式，而是一个附加标志。`publishOn` 使用 `requestFusion(ANY | THREAD_BARRIER)` 表示"我支持任何熔合模式，但请注意我会在不同线程上消费数据"。上游操作符可以根据这个标志决定是否接受熔合。

### 2.2 fusionModeName 辅助方法

```java
static String fusionModeName(int mode, boolean ignoreThreadBarrier) {
    int evaluated = mode;
    String threadBarrierSuffix = "";
    if (mode >= 0) {
        evaluated = mode & ~THREAD_BARRIER; // 擦除 THREAD_BARRIER bit
        if (!ignoreThreadBarrier && (mode & THREAD_BARRIER) == THREAD_BARRIER) {
            threadBarrierSuffix = "+THREAD_BARRIER";
        }
    }

    switch (evaluated) {
        case -1: return "Disabled";
        case Fuseable.NONE: return "NONE" + threadBarrierSuffix;
        case Fuseable.SYNC: return "SYNC" + threadBarrierSuffix;
        case Fuseable.ASYNC: return "ASYNC" + threadBarrierSuffix;
        default: return "Unknown(" + evaluated + ")" + threadBarrierSuffix;
    }
}
```

通过 `mode & ~THREAD_BARRIER` 擦除第三个 bit，只保留低两位来判断基本模式。这是位运算设计的优雅之处——标志位和模式位互不干扰。

---

## 三、QueueSubscription：队列与订阅的统一

### 3.1 接口定义

源码文件：`reactor/core/Fuseable.java`
```java
interface QueueSubscription<T> extends Queue<T>, Subscription {

    String NOT_SUPPORTED_MESSAGE = "Although QueueSubscription extends Queue it is purely internal" +
            " and only guarantees support for poll/clear/size/isEmpty." +
            " Instances shouldn't be used/exposed as Queue outside of Reactor operators.";

    int requestFusion(int requestedMode);

    @Override
    default @Nullable T peek() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    default boolean add(@Nullable T t) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    default boolean offer(@Nullable T t) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }
    // ... 所有非必要的 Queue 方法都抛 UnsupportedOperationException
}
```

`QueueSubscription` 同时继承了 `Queue<T>` 和 `Subscription`，但实际上只支持 `Queue` 接口的一个子集：`poll()`、`clear()`、`size()`、`isEmpty()`。其他方法（如 `offer`、`add`、`peek`、`iterator` 等）都抛出异常。

**为什么继承完整的 `Queue` 接口而不是定义自己的精简接口？** 源码注释给出了答案：`"it is purely internal"`。这样做的好处是下游操作符可以直接把 `QueueSubscription` 赋值给 `Queue<T>` 类型的字段（如 `FluxPublishOn.PublishOnSubscriber.queue`），与非熔合场景下使用的普通 `Queue` 保持类型兼容。如果定义新的接口，`publishOn` 就需要两套 drain 代码分别处理 `Queue` 和 `FusionQueue`。

**核心方法 `requestFusion(int requestedMode)`：**
- 请求方调用此方法，传入希望的模式（`SYNC`、`ASYNC` 或 `ANY`，可能附带 `THREAD_BARRIER`）
- 被请求方返回实际支持的模式（`NONE`、`SYNC` 或 `ASYNC`，永远不会返回 `ANY`）

### 3.2 SynchronousSubscription：同步源的默认实现

```java
interface SynchronousSubscription<T> extends QueueSubscription<T> {

    @Override
    default int requestFusion(int requestedMode) {
        if ((requestedMode & Fuseable.SYNC) != 0) {
            return Fuseable.SYNC;
        }
        return NONE;
    }
}
```

`SynchronousSubscription` 的默认 `requestFusion` 实现非常简单：如果请求包含 `SYNC` 位（即 `requestedMode` 是 `SYNC` 或 `ANY`），就返回 `SYNC`；否则返回 `NONE`。

**它完全忽略了 `THREAD_BARRIER` 标志。** 这是因为 SYNC 熔合下，数据由消费者主动 `poll()`，poll 动作和数据消费在同一个线程上。消费者已经知道自己在哪个线程调用 poll，所以 THREAD_BARRIER 对 SYNC 模式没有意义。

---

## 四、FluxArray 的 ArraySubscription：SYNC 熔合的源头

### 4.1 类定义

源码文件：`reactor/core/publisher/FluxArray.java`
```java
final class FluxArray<T> extends Flux<T> implements Fuseable, SourceProducer<T> {

    final T[] array;

    static final class ArraySubscription<T>
            implements InnerProducer<T>, SynchronousSubscription<T> {

        final CoreSubscriber<? super T> actual;
        final T[] array;
        int index;
        volatile boolean cancelled;
        volatile long requested;
    }
}
```

`ArraySubscription` 实现了 `SynchronousSubscription`，因此它的 `requestFusion()` 默认返回 `SYNC`（来自 `SynchronousSubscription` 的 default 方法）。

### 4.2 poll() 方法

```java
@Override
public @Nullable T poll() {
    int i = index;
    T[] a = array;
    if (i != a.length) {
        T t = a[i];
        Objects.requireNonNull(t);
        index = i + 1;
        return t;
    }
    return null;
}
```

`poll()` 的语义等价于一个数组迭代器：每次调用返回下一个元素并推进索引。当索引到达数组末尾时返回 `null`，表示数据耗尽。

**注意 `index` 不是 `volatile` 的。** 这是安全的，因为在 SYNC 熔合模式下，`poll()` 总是被同一个线程调用（消费者的 drain 循环运行在单个线程上）。非熔合模式下，`poll()` 不会被调用——数据通过 `onNext()` 推送。

### 4.3 完整的 subscribe 流程（非熔合 vs SYNC 熔合）

**非熔合模式下的数据流：**

```java
// ArraySubscription.request(n) → slowPath(n) 或 fastPath()
void fastPath() {
    final T[] a = array;
    final int len = a.length;
    final Subscriber<? super T> s = actual;

    for (int i = index; i != len; i++) {
        if (cancelled) return;
        T t = a[i];
        if (t == null) {
            s.onError(new NullPointerException(...));
            return;
        }
        s.onNext(t);
    }
    if (cancelled) return;
    s.onComplete();
}
```

非熔合模式：数组通过 `for` 循环主动调用 `subscriber.onNext(t)` 推送每个元素。每个元素都经过 `onNext` 调用链——`ArraySubscription → MapSubscriber.onNext → FilterSubscriber.onNext → actual.onNext`。

**SYNC 熔合模式下的数据流：**

消费者（如 `publishOn` 的 drain 循环）直接调用：

```java
// publishOn.runSync() 中
T v = queue.poll(); // queue 就是 ArraySubscription 本身
// 或者 fusedOperator.poll() → 内部调 s.poll() → ArraySubscription.poll()
```

不需要 `onNext` 调用链，不需要 `request()` 记账。数据沿 `poll()` 链同步拉取。

---

## 五、FluxMapFuseable：中间操作符的熔合透传

### 5.1 requestFusion 的透传与拦截

源码文件：`reactor/core/publisher/FluxMapFuseable.java`
```java
static final class MapFuseableSubscriber<T, R>
        implements InnerOperator<T, R>, QueueSubscription<R> {

    final CoreSubscriber<? super R> actual;
    final Function<? super T, ? extends @Nullable R> mapper;
    QueueSubscription<T> s;
    int sourceMode;

    @SuppressWarnings("unchecked")
    @Override
    public void onSubscribe(Subscription s) {
        if (Operators.validate(this.s, s)) {
            this.s = (QueueSubscription<T>) s;
            actual.onSubscribe(this);
        }
    }

    @Override
    public int requestFusion(int requestedMode) {
        int m;
        if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
            return Fuseable.NONE;
        }
        else {
            m = s.requestFusion(requestedMode);
        }
        sourceMode = m;
        return m;
    }
}
```

`MapFuseableSubscriber.requestFusion()` 的逻辑：

1. **如果请求包含 `THREAD_BARRIER`，立即返回 `NONE`。** 这是安全性保护：`mapper.apply()` 函数可能在错误的线程上执行。假设 `asyncSource.map(expensiveComputation).publishOn(scheduler)`，如果允许熔合，publishOn 的 drain 循环会在 scheduler 的线程上调用 `MapFuseableSubscriber.poll()`，进而执行 `mapper.apply()`。但 mapper 可能期望在 asyncSource 的线程上执行。`THREAD_BARRIER` 正是用来表达这种线程边界关切。

2. **如果没有 `THREAD_BARRIER`，将请求透传给上游 `s.requestFusion(requestedMode)`。** map 操作符本身不引入异步边界，所以它可以完全透传上游的熔合能力。

3. **记录返回的模式到 `sourceMode`。** 后续的 `onNext()` 和 `poll()` 根据 `sourceMode` 选择不同的代码路径。

### 5.2 poll() 方法：拉取 + 转换

```java
@Override
public @Nullable R poll() {
    for(;;) {
        T v = s.poll();
        if (v != null) {
            try {
                return Objects.requireNonNull(mapper.apply(v));
            }
            catch (Throwable t) {
                RuntimeException e_ = Operators.onNextPollError(v, t, currentContext());
                if (e_ != null) {
                    throw e_;
                }
                else {
                    continue;
                }
            }
        }
        return null;
    }
}
```

SYNC 或 ASYNC 熔合模式下，下游不通过 `onNext()` 接收数据，而是直接调 `poll()`。`MapFuseableSubscriber.poll()` 做了两件事：
1. 从上游 `s.poll()` 拉取原始值
2. 应用 `mapper.apply(v)` 并返回转换后的值

这消除了 `onNext` 调用的开销：不需要虚方法调用、不需要 null 检查、不需要 done/cancelled 状态检查。

**`for(;;)` 循环的作用：** 当 `mapper.apply(v)` 抛出异常且 `onNextPollError` 返回 `null`（表示错误被吞掉、应该跳过此元素）时，`continue` 会重新 poll 下一个元素。这对应了 `onErrorContinue` 模式。

### 5.3 onNext 在不同 sourceMode 下的行为

```java
@Override
public void onNext(T t) {
    if (sourceMode == ASYNC) {
        actual.onNext(null);
    }
    else {
        if (done) {
            Operators.onNextDropped(t, actual.currentContext());
            return;
        }
        R v;
        try {
            v = mapper.apply(t);
            if (v == null) {
                throw new NullPointerException("The mapper [" + mapper.getClass().getName() + "] returned a null value.");
            }
        }
        catch (Throwable e) {
            Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
            if (e_ != null) {
                onError(e_);
            }
            else {
                s.request(1);
            }
            return;
        }

        actual.onNext(v);
    }
}
```

- **ASYNC 模式**：`onNext(t)` 被忽略，只是转发一个 `onNext(null)` 通知给下游，告诉它"队列里有新数据了"。实际数据通过 `poll()` 拉取。
- **非熔合模式**：正常的 push 模式，执行 `mapper.apply(t)` 并调用 `actual.onNext(v)`。

---

## 六、FluxFilterFuseable：条件过滤的熔合支持

### 6.1 requestFusion：与 map 完全相同的策略

源码文件：`reactor/core/publisher/FluxFilterFuseable.java`
```java
@Override
public int requestFusion(int requestedMode) {
    int m;
    if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
        return Fuseable.NONE;
    }
    else {
        m = s.requestFusion(requestedMode);
    }
    sourceMode = m;
    return m;
}
```

与 `FluxMapFuseable` 完全相同的策略：拒绝 `THREAD_BARRIER`，否则透传。

### 6.2 poll()：循环跳过不匹配元素

```java
@Override
public @Nullable T poll() {
    if (sourceMode == ASYNC) {
        long dropped = 0;
        for (; ; ) {
            T v = s.poll();

            try {
                if (v == null || predicate.test(v)) {
                    if (dropped != 0) {
                        request(dropped);
                    }
                    return v;
                }
                Operators.onDiscard(v, this.ctx);
                dropped++;
            }
            catch (Throwable e) {
                RuntimeException e_ = Operators.onNextPollError(v, e, currentContext());
                Operators.onDiscard(v, this.ctx);
                if (e_ != null) {
                    throw e_;
                }
            }
        }
    }
    else {
        for (; ; ) {
            T v = s.poll();

            try {
                if (v == null || predicate.test(v)) {
                    return v;
                }
                Operators.onDiscard(v, this.ctx);
            }
            catch (Throwable e) {
                RuntimeException e_ = Operators.onNextPollError(v, e, currentContext());
                Operators.onDiscard(v, this.ctx);
                if (e_ != null) {
                    throw e_;
                }
            }
        }
    }
}
```

过滤操作符的 `poll()` 有一个关键差异：它在循环中持续 poll 直到找到匹配的元素或数据耗尽。

**ASYNC 模式下的 `dropped` 计数器：** 在 ASYNC 熔合中，被过滤掉的元素需要"补偿 request"。每过滤一个元素，`dropped` 加 1。当找到匹配元素时，调用 `request(dropped)` 向上游请求被丢弃的量，确保上游能够继续产生足够的数据。

**SYNC 模式下不需要 `dropped`：** SYNC 源不需要 request（数据已全部在队列中），所以过滤掉的元素不需要补偿。

### 6.3 ConditionalSubscriber：tryOnNext 优化

源码文件：`reactor/core/Fuseable.java`
```java
interface ConditionalSubscriber<T> extends CoreSubscriber<T> {
    /**
     * Try consuming the value and return true if successful.
     * @param t the value to consume, not null
     * @return true if consumed, false if dropped and a new value can be immediately sent
     */
    boolean tryOnNext(T t);
}
```

`ConditionalSubscriber` 解决了一个特定的性能问题：filter 操作符在非熔合模式下，如果一个元素不匹配，需要调 `s.request(1)` 向上游再要一个元素。这个 `request(1)` 调用有开销（原子操作 + 可能的线程调度）。

`tryOnNext(T t)` 的语义是"尝试消费，如果拒绝返回 false，调用者可以立即推送下一个元素而不需要额外的 request"。

以 `FilterFuseableSubscriber` 为例：

```java
@Override
public boolean tryOnNext(T t) {
    if (done) {
        Operators.onNextDropped(t, this.ctx);
        return false;
    }

    boolean b;
    try {
        b = predicate.test(t);
    }
    catch (Throwable e) {
        Throwable e_ = Operators.onNextError(t, e, this.ctx, s);
        if (e_ != null) {
            onError(e_);
        }
        Operators.onDiscard(t, this.ctx);
        return false;
    }
    if (b) {
        actual.onNext(t);
        return true;
    }
    Operators.onDiscard(t, this.ctx);
    return false;
}
```

当 `predicate.test(t)` 返回 `false` 时，`tryOnNext` 返回 `false`。上游看到 `false` 后，可以立即推送下一个元素，而不需要等待 `request(1)` 的往返。

在非熔合模式的 `onNext` 中对比：

```java
@Override
public void onNext(T t) {
    // ...
    if (b) {
        actual.onNext(t);
    }
    else {
        s.request(1);  // ← 需要额外的 request(1) 往返
        Operators.onDiscard(t, this.ctx);
    }
}
```

`tryOnNext` 省去了 `s.request(1)` 调用。在高过滤率的场景中（例如 90% 的元素被过滤），这意味着节省了 90% 的 request 原子操作。

---

## 七、publishOn 中的熔合协商：THREAD_BARRIER 的实际应用

### 7.1 输入端熔合

源码文件：`reactor/core/publisher/FluxPublishOn.java`
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

publishOn 请求熔合时传入 `Fuseable.ANY | Fuseable.THREAD_BARRIER = 3 | 4 = 7（二进制 111）`。

**为什么传 `THREAD_BARRIER`？** 因为 publishOn 会在不同的线程上消费数据（Worker 线程）。上游操作符看到 THREAD_BARRIER 后有权拒绝熔合。例如：

- `FluxMapFuseable` 看到 THREAD_BARRIER 后返回 `NONE`——拒绝熔合，因为 mapper 函数可能不是线程安全的
- `FluxArray.ArraySubscription`（`SynchronousSubscription`）不检查 THREAD_BARRIER，直接返回 `SYNC`——因为 SYNC 模式下，数据通过 poll 拉取，mapper 在消费者线程执行，这是消费者自己的决定

### 7.2 输出端熔合

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

publishOn 只支持输出端的 ASYNC 熔合（不支持 SYNC，因为 publishOn 本身就是异步边界）。当输出端熔合时，`outputFused = true`，drain 循环使用 `runBackfused()`：

```java
void runBackfused() {
    int missed = 1;

    for (; ; ) {
        if (cancelled) {
            this.clear();
            return;
        }

        boolean d = done;

        actual.onNext(null); // 通知下游有新数据

        if (d) {
            Throwable e = error;
            if (e != null) {
                doError(actual, e);
            }
            else {
                doComplete(actual);
            }
            return;
        }

        missed = WIP.addAndGet(this, -missed);
        if (missed == 0) {
            break;
        }
    }
}
```

`onNext(null)` 只是通知信号，下游通过 `poll()` 从 publishOn 的队列中拉取数据。

---

## 八、端到端分析：FluxArray.map().filter() 的熔合效果

### 8.1 不做熔合的情况

```java
Flux.just(1, 2, 3, 4, 5)  // FluxArray
    .map(x -> x * 2)        // FluxMap (非 Fuseable 版本，假设)
    .filter(x -> x > 4)     // FluxFilter
    .subscribe(System.out::println);
```

每个元素的处理路径：

```
元素 1:
  ArraySubscription.onNext(1) 调用 → MapSubscriber.onNext(1)
    mapper.apply(1) = 2
    MapSubscriber 调用 → FilterSubscriber.onNext(2)
      predicate.test(2) = false (2 <= 4)
      FilterSubscriber 调用 → s.request(1)  ← 往返 request
        request 传播回 ArraySubscription

元素 2:
  ArraySubscription.onNext(2) 调用 → MapSubscriber.onNext(2)
    mapper.apply(2) = 4
    MapSubscriber 调用 → FilterSubscriber.onNext(4)
      predicate.test(4) = false (4 <= 4)
      FilterSubscriber 调用 → s.request(1)  ← 又一次往返 request

元素 3:
  ArraySubscription.onNext(3) 调用 → MapSubscriber.onNext(3)
    mapper.apply(3) = 6
    MapSubscriber 调用 → FilterSubscriber.onNext(6)
      predicate.test(6) = true
      FilterSubscriber 调用 → actual.onNext(6)
      println(6)
```

对于 5 个元素：
- 5 次 `onNext` 虚方法调用 × 2 层 = 10 次
- 3 次被 filter 拒绝 → 3 次 `request(1)` 原子操作往返
- 总计：10 次 onNext + 3 次 request 原子操作

### 8.2 有 SYNC 熔合的情况

```java
Flux.just(1, 2, 3, 4, 5)  // FluxArray (implements Fuseable)
    .map(x -> x * 2)        // FluxMapFuseable (implements Fuseable)
    .filter(x -> x > 4)     // FluxFilterFuseable (implements Fuseable)
    .subscribe(subscriber);
```

订阅阶段的熔合协商：

```
1. FluxFilterFuseable.onSubscribe(MapFuseableSubscriber 的 QueueSubscription)
   → requestFusion(SYNC)  // subscriber 请求 SYNC
   → MapFuseableSubscriber.requestFusion(SYNC)
     → (THREAD_BARRIER bit 不存在) 透传给上游
     → ArraySubscription.requestFusion(SYNC)
     → return SYNC (from SynchronousSubscription default)
   → MapFuseableSubscriber.sourceMode = SYNC
   → return SYNC
   → FilterFuseableSubscriber.sourceMode = SYNC
```

数据阶段——subscriber 调用 `poll()`：

```
subscriber.poll()
  → FilterFuseableSubscriber.poll()    // SYNC 模式
    → for(;;) {
        T v = s.poll();                 // s = MapFuseableSubscriber
        → MapFuseableSubscriber.poll()
          → T v2 = s.poll();            // s = ArraySubscription
          → ArraySubscription.poll()    // return array[index++] = 1
          → return mapper.apply(1) = 2;
        if (predicate.test(2)) → false  // 2 <= 4, 不匹配
        Operators.onDiscard(2, ctx);
        continue;                       // 继续 poll 下一个

        T v = s.poll();
        → MapFuseableSubscriber.poll()
          → ArraySubscription.poll()    // return 2
          → return mapper.apply(2) = 4;
        if (predicate.test(4)) → false  // 4 <= 4, 不匹配
        continue;

        T v = s.poll();
        → MapFuseableSubscriber.poll()
          → ArraySubscription.poll()    // return 3
          → return mapper.apply(3) = 6;
        if (predicate.test(6)) → true   // 6 > 4, 匹配！
        return 6;
      }
```

优化效果：
- **0 次 `onNext` 调用**——数据通过 `poll()` 同步拉取
- **0 次 `request()` 原子操作**——SYNC 模式不需要 request，数据已在数组中
- 每次 `poll()` 是普通的方法调用（虽然是虚方法，但 JIT 可以内联）
- Filter 跳过不匹配元素时只是 `continue`，不需要跨操作符的 request 往返

### 8.3 ASYNC 熔合的场景

当 `publishOn` 介入时：

```java
Flux.just(1, 2, 3, 4, 5)  // FluxArray
    .map(x -> x * 2)        // FluxMapFuseable
    .publishOn(scheduler)    // FluxPublishOn
    .subscribe(subscriber);
```

协商过程：

```
publishOn.onSubscribe(MapFuseableSubscriber)
  → MapFuseableSubscriber 是 QueueSubscription
  → requestFusion(ANY | THREAD_BARRIER) = requestFusion(7)
  → MapFuseableSubscriber.requestFusion(7)
    → (requestedMode & THREAD_BARRIER) != 0  → return NONE !
```

**MapFuseable 拒绝了熔合！** 因为 publishOn 传入了 `THREAD_BARRIER`，而 map 操作符不允许 mapper 函数在不同线程上执行。

那如果没有 map，直接 `FluxArray.publishOn(scheduler)`：

```
publishOn.onSubscribe(ArraySubscription)
  → ArraySubscription 是 QueueSubscription (SynchronousSubscription)
  → requestFusion(ANY | THREAD_BARRIER) = requestFusion(7)
  → SynchronousSubscription.requestFusion(7)
    → (7 & SYNC) != 0 → return SYNC
  → publishOn.sourceMode = SYNC
  → publishOn.queue = ArraySubscription (直接作为队列使用)
  → publishOn.done = true
```

此时 publishOn 直接从 `ArraySubscription.poll()` 拉取数据，不需要中间队列。

---

## 九、反例分析：如果不做熔合会损失什么

### 9.1 性能角度

考虑一个典型的管道 `Flux.range(1, 1_000_000).map(i -> i * 2).filter(i -> i % 3 == 0)`：

**不熔合：**
- 1,000,000 次 `onNext` × 2 层 = 2,000,000 次虚方法调用
- 约 666,667 次 `request(1)` 原子操作（2/3 的元素被 filter 拒绝）
- 每次 request 至少涉及一次 `AtomicLong.getAndAdd` 或 `compareAndSet`

**SYNC 熔合：**
- 数据通过 `poll()` 链拉取
- Filter 内部循环跳过不匹配元素，不产生 request
- 只有满足条件的 ~333,333 个元素需要从 subscriber 到 source 的 poll 调用栈

在微基准测试中，SYNC 熔合对于纯内存操作可以带来 2-5 倍的吞吐量提升。

### 9.2 内存角度

不熔合时，`publishOn` 需要分配一个独立的 `SpscArrayQueue`（通常大小为 prefetch = 256 个槽位）来中转数据。SYNC 或 ASYNC 熔合可以复用上游的 `QueueSubscription` 作为队列，省去了这个分配。

### 9.3 如果所有操作符都强制熔合会怎样

并非所有操作符都适合熔合。`flatMap` 不支持 SYNC 熔合，因为它的输出是异步的、交错的多个内部流。`publishOn` 不支持 SYNC 输出熔合，因为它本身就是异步边界。强制熔合会破坏这些操作符的语义正确性。

---

## 十、THREAD_BARRIER 的工程动机：为什么需要线程边界标记

考虑这个链路：

```java
asyncSource.map(expensiveMapper).publishOn(scheduler)
```

如果 asyncSource 支持 ASYNC 熔合，且 map 也允许透传 ASYNC 熔合，那么 publishOn 可以直接从 asyncSource 的队列中 poll 数据，并在 poll 时执行 `expensiveMapper.apply()`。

问题在于：`expensiveMapper` 原本应该在 asyncSource 的线程上执行（因为没有 publishOn 时，map 的 `onNext` 在 asyncSource 的线程上被调用）。但熔合后，mapper 在 publishOn 的 Worker 线程上的 `poll()` 调用中执行——线程边界被悄无声息地突破了。

如果 mapper 是线程不安全的（例如访问了 ThreadLocal 或非线程安全的缓存），这就是一个并发 bug。

`THREAD_BARRIER` 标志就是这个问题的解决方案。`FluxMapFuseable.requestFusion()` 检查到 `THREAD_BARRIER` 后返回 `NONE`，强制 publishOn 创建独立的中间队列，mapper 继续在 asyncSource 的线程上通过 `onNext` 执行。

**什么时候可以忽略 `THREAD_BARRIER`？** `SynchronousSubscription`（如 `FluxArray`）忽略它，因为 SYNC 源的数据始终由消费者 poll，不存在"原始线程"的概念。`FluxArray` 的数据在内存数组中，在哪个线程 poll 都一样。

---

## 十一、熔合模式对照总表

| 维度 | NONE（无熔合） | SYNC 熔合 | ASYNC 熔合 |
|---|---|---|---|
| **常量值** | 0 | 1 | 2 |
| **数据传递方式** | `onNext(t)` 推送 | `poll()` 拉取 | `onNext(null)` 通知 + `poll()` 拉取 |
| **数据来源** | 上游调用 subscriber.onNext | 消费者主动 poll 队列 | 上游填充队列 + 消费者 poll |
| **队列** | 独立创建（如 SpscArrayQueue） | 复用上游的 QueueSubscription | 复用上游的 QueueSubscription |
| **request 记账** | 需要 | 不需要 | 需要（部分） |
| **典型源** | 任意 Publisher | `FluxArray`、`FluxRange`、`MonoJust` | `UnicastProcessor`、异步操作符 |
| **适用操作符** | 所有操作符 | `map`、`filter`、`publishOn`（输入端） | `publishOn`（输入端） |
| **THREAD_BARRIER 影响** | 无 | 无（SYNC 本身不涉及原始线程） | 中间操作符可能拒绝熔合 |
| **完成信号** | `onComplete()` | `poll()` 返回 null | `onComplete()` |
| **性能收益** | 基线 | 消除 onNext/request 开销 | 消除中间队列分配 |
| **ConditionalSubscriber** | 无关 | 无关（poll 模式不需要） | 无关（poll 模式不需要） |

| 接口/类 | requestFusion 行为 | poll() 行为 |
|---|---|---|
| `SynchronousSubscription` | `(mode & SYNC) != 0` → `SYNC`，否则 `NONE` | 直接从底层数据源拉取 |
| `MapFuseableSubscriber` | 有 THREAD_BARRIER → `NONE`；否则透传上游 | `s.poll()` + `mapper.apply(v)` |
| `FilterFuseableSubscriber` | 有 THREAD_BARRIER → `NONE`；否则透传上游 | 循环 `s.poll()` 直到匹配 |
| `PublishOnSubscriber`（输入端） | 向上游请求 `ANY + THREAD_BARRIER` | 取决于 sourceMode |
| `PublishOnSubscriber`（输出端） | `(mode & ASYNC) != 0` → `ASYNC`，否则 `NONE` | 从内部队列 poll + 补货 request |
| `FluxArray.ArraySubscription` | 继承 SynchronousSubscription 默认行为 | `return array[index++]` |
