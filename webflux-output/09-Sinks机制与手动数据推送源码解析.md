# Sinks 机制与手动数据推送源码解析

> **Reactor Core 源码深度研究系列 · 第 09 篇**

本文深入剖析 Reactor Core 中 Sinks 机制的整体架构、核心实现类、序列化策略以及与旧版 Processor 的演进关系。所有分析基于真实源码，引用真实类名、字段名和方法名。

---

## 一、全局架构总览

```
                         Sinks API 层次图
                         ================

  用户入口:
  Sinks.many()  ──→  ManySpec
       │                ├── unicast()  → UnicastSpec
       │                │     ├── onBackpressureBuffer()        → SinkManySerialized<SinkManyUnicast>
       │                │     ├── onBackpressureBuffer(Queue)   → SinkManySerialized<SinkManyUnicast>
       │                │     └── onBackpressureError()          → SinkManySerialized<SinkManyUnicastNoBackpressure>
       │                ├── multicast() → MulticastSpec
       │                │     ├── onBackpressureBuffer()         → SinkManySerialized<SinkManyEmitterProcessor>
       │                │     ├── directAllOrNothing()           → SinkManySerialized<SinkManyBestEffort>
       │                │     └── directBestEffort()             → SinkManySerialized<SinkManyBestEffort>
       │                └── replay()    → MulticastReplaySpec
       │                      ├── all()                           → SinkManySerialized<SinkManyReplayProcessor>
       │                      ├── latest()                        → SinkManySerialized<SinkManyReplayProcessor>
       │                      └── limit(historySize)              → SinkManySerialized<SinkManyReplayProcessor>
       │
  Sinks.one()   ──→  One<T>  (SinkOneSerialized<SinkOneMulticast>)
  Sinks.empty() ──→  Empty<T>(SinkEmptySerialized<SinkEmptyMulticast>)
  Sinks.unsafe()──→  RootSpec (直接创建, 无序列化包装)
```

```
  序列化层与底层实现的关系:

  用户调用 tryEmitNext()
        │
        ▼
  ┌──────────────────────┐
  │  SinkManySerialized   │  ← 序列化包装层 (WIP + LOCKED_AT)
  │  tryAcquire(thread)   │
  │  └→ delegate.sink     │
  └──────────┬───────────┘
             │
             ▼
  ┌──────────────────────┐
  │  SinkManyUnicast      │  ← 真正的 Sink 实现
  │  queue.offer(t)       │
  │  drain()              │
  └──────────────────────┘
```

Sinks 是 Reactor 3.4 引入的全新 API，用于替代旧版 Processor（如 `EmitterProcessor`、`ReplayProcessor`、`UnicastProcessor`）。其核心设计理念是：**将"数据推送"与"订阅消费"彻底分离**，通过 `tryEmitNext()` / `tryEmitError()` / `tryEmitComplete()` 返回 `EmitResult` 枚举，让调用者明确知道推送是否成功，而不是像旧版 Processor 那样默默丢弃或抛异常。

---

## 二、Sinks API 接口层次

### 2.1 核心接口定义

源码文件：`reactor/core/publisher/Sinks.java`

`Sinks` 类是整个 API 的入口，通过三个静态工厂方法暴露不同的 Sink 类型：

```java
// Sinks.java
public static <T> Sinks.Empty<T> empty() {
    return SinksSpecs.DEFAULT_SINKS.empty();
}

public static <T> Sinks.One<T> one() {
    return SinksSpecs.DEFAULT_SINKS.one();
}

public static ManySpec many() {
    return SinksSpecs.DEFAULT_SINKS.many();
}

public static RootSpec unsafe() {
    return SinksSpecs.UNSAFE_ROOT_SPEC;
}
```

接口继承关系如下：
- `Sinks.Many<T>` extends `Scannable` — Flux 语义，可多次推送
- `Sinks.One<T>` extends `Sinks.Empty<T>` — Mono 语义，可推送一个值或终止信号
- `Sinks.Empty<T>` extends `Scannable` — 只能推送终止信号（complete/error）

**为什么 `One` 继承 `Empty` 而不是反过来？** 从语义上讲，`One` 比 `Empty` 多了一个能力——可以携带一个值完成。`Empty` 只能 complete 或 error，`One` 在此基础上增加了 `tryEmitValue(T)`。这遵循了接口隔离原则：需要 `Empty` 语义的地方不应该被迫接受 `tryEmitValue` 的存在。

### 2.2 Spec 模式构建器

Sinks 使用 Builder Spec 模式分层暴露不同配置选项：

```java
// Sinks.java
public interface ManySpec {
    UnicastSpec unicast();         // 单订阅者
    MulticastSpec multicast();     // 多订阅者
    MulticastReplaySpec replay();  // 多订阅者 + 历史回放
}
```

每一层 Spec 对应不同的实现类，这种分层不是冗余设计，而是因为不同订阅模型需要完全不同的背压策略和队列管理逻辑。

---

## 三、EmitResult 枚举：推送结果的原子化表达

源码文件：`reactor/core/publisher/Sinks.java`（第118-192行）

```java
public enum EmitResult {
    OK,                     // 成功推送
    FAIL_TERMINATED,        // Sink 已终止（complete 或 error）
    FAIL_OVERFLOW,          // 缓冲队列已满
    FAIL_CANCELLED,         // 订阅者已取消
    FAIL_NON_SERIALIZED,    // 并发访问未序列化
    FAIL_ZERO_SUBSCRIBER;   // 无订阅者且无缓冲能力

    public boolean isSuccess() { return this == OK; }
    public boolean isFailure() { return this != OK; }

    public void orThrow() {
        if (this == OK) return;
        throw new EmissionException(this);
    }
}
```

**为什么用枚举返回值而不是抛异常？** 这是 Sinks 相对于旧版 Processor 最核心的设计改进。旧版 `Processor.onNext()` 在遇到错误状态时只能抛异常或默默丢弃，调用者无法区分"推送成功但下游尚未消费"和"推送因队列满而失败"。`EmitResult` 枚举让每次推送的结果都变成一个可检查的值，调用者可以根据不同的失败类型采取不同的恢复策略。

从不同视角分析每种失败码的含义：

| 视角 | OK | FAIL_OVERFLOW | FAIL_CANCELLED | FAIL_TERMINATED | FAIL_NON_SERIALIZED | FAIL_ZERO_SUBSCRIBER |
|------|-----|---------------|----------------|-----------------|---------------------|---------------------|
| **生产者** | 数据已入队 | 队列满，需降速 | 消费者已离开 | 序列已结束 | 有并发竞争 | 无人监听 |
| **消费者** | 将通过onNext收到 | 不受影响 | 主动取消的结果 | 后续无数据 | 不感知 | 等待订阅 |
| **系统** | 正常流转 | 背压告警 | 资源释放 | 生命周期终结 | 线程安全告警 | 冷启动期 |

---

## 四、EmitFailureHandler：失败重试策略

源码文件：`reactor/core/publisher/Sinks.java`（第256-292行）

`EmitFailureHandler` 是 `emitNext()` / `emitError()` / `emitComplete()` 等"便捷 API"的核心组件。与 `tryEmitNext()` 返回 `EmitResult` 不同，`emitNext()` 接受一个 `EmitFailureHandler`，在失败时询问 handler 是否重试。

```java
public interface EmitFailureHandler {
    // 快速失败：不重试任何失败
    EmitFailureHandler FAIL_FAST = (signalType, emission) -> false;

    // 忙等待重试：仅对 FAIL_NON_SERIALIZED 重试，持续到 deadline
    static EmitFailureHandler busyLooping(Duration duration) {
        return new OptimisticEmitFailureHandler(duration);
    }

    boolean onEmitFailure(SignalType signalType, EmitResult emitResult);
}
```

`OptimisticEmitFailureHandler` 的实现：

```java
// Sinks.java
static class OptimisticEmitFailureHandler implements EmitFailureHandler {
    private final long deadline;

    OptimisticEmitFailureHandler(Duration duration) {
        this.deadline = System.nanoTime() + duration.toNanos();
    }

    @Override
    public boolean onEmitFailure(SignalType signalType, EmitResult emitResult) {
        return emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)
                && deadline - System.nanoTime() > 0;
    }
}
```

**为什么 `busyLooping` 只重试 `FAIL_NON_SERIALIZED`？** 因为 `FAIL_OVERFLOW` 意味着队列已满，忙等不会让消费者更快地消费；`FAIL_CANCELLED` 和 `FAIL_TERMINATED` 是不可逆状态，重试无意义。只有 `FAIL_NON_SERIALIZED` 表示并发竞争是暂时的——另一个线程正在使用 Sink，等它用完就可以继续。这是一个乐观锁的自旋等待策略。

**反例：如果不做 `FAIL_NON_SERIALIZED` 重试会怎样？** 在高并发场景下，两个线程同时调用 `sink.next()`，一个成功，另一个得到 `FAIL_NON_SERIALIZED`。如果直接抛 `EmissionException`，用户代码会被一个瞬态的并发竞争打断，即使两次推送在语义上都是合法的。`busyLooping` 让被竞争掉的线程短暂自旋等待，大幅降低了误报率。

---

## 五、SinkManyUnicast 深度解析

源码文件：`reactor/core/publisher/SinkManyUnicast.java`

### 5.1 类定义与核心字段

```java
// SinkManyUnicast.java
final class SinkManyUnicast<T> extends Flux<T> 
    implements InternalManySink<T>, Disposable, 
               Fuseable.QueueSubscription<T>, Fuseable {

    final Queue<T>            queue;          // 内部队列，缓冲未消费数据
    volatile boolean          done;           // 是否已终止
    volatile boolean          cancelled;      // 是否已取消
    volatile boolean          subscriptionDelivered;
    @Nullable Throwable       error;          // 终止异常
    boolean                   hasDownstream;  // 是否有活跃订阅者
    
    volatile CoreSubscriber<? super T> actual; // 唯一订阅者
    volatile long             requested;       // 订阅者请求量
    volatile int              wip;             // drain 循环的进入标志
    volatile int              once;            // 确保单订阅
    volatile int              discardGuard;    // discard 操作的互斥标志
}
```

### 5.2 tryEmitNext 的核心逻辑

```java
// SinkManyUnicast.java
@Override
public EmitResult tryEmitNext(T t) {
    if (done) {
        return EmitResult.FAIL_TERMINATED;
    }
    if (cancelled) {
        return EmitResult.FAIL_CANCELLED;
    }

    if (!queue.offer(t)) {
        return (once > 0) ? EmitResult.FAIL_OVERFLOW : EmitResult.FAIL_ZERO_SUBSCRIBER;
    }
    drain(t);
    return EmitResult.OK;
}
```

这段代码的逻辑非常清晰：先检查终态和取消态，然后尝试入队，入队失败时区分"有订阅者但队列满"（`FAIL_OVERFLOW`）和"无订阅者"（`FAIL_ZERO_SUBSCRIBER`）两种情况。

**为什么区分 `FAIL_OVERFLOW` 和 `FAIL_ZERO_SUBSCRIBER`？** 因为这两种情况的恢复策略完全不同。`FAIL_OVERFLOW` 意味着消费者太慢，生产者应该降速或增加缓冲；`FAIL_ZERO_SUBSCRIBER` 意味着还没有消费者订阅，生产者可以选择等待订阅或直接丢弃。如果将两者合并，调用者就无法做出正确的恢复决策。

### 5.3 drain 机制：队列到订阅者的数据搬运

```java
// SinkManyUnicast.java
void drain(@Nullable T dataSignalOfferedBeforeDrain) {
    if (WIP.getAndIncrement(this) != 0) {
        // 已有线程在 drain，处理刚 offer 的数据
        if (dataSignalOfferedBeforeDrain != null) {
            if (cancelled) {
                Operators.onDiscard(dataSignalOfferedBeforeDrain, actual.currentContext());
            }
            else if (done) {
                Operators.onNextDropped(dataSignalOfferedBeforeDrain, currentContext());
            }
        }
        return;
    }

    int missed = 1;
    for (;;) {
        if (subscriptionDelivered) {
            CoreSubscriber<? super T> a = actual;
            if (outputFused) {
                drainFused(a);   // ASYNC 融合模式
            } else {
                drainRegular(a); // 常规模式
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

`drain` 方法使用 `WIP`（work-in-progress）计数器实现了一个典型的"遗漏检查"（missed check）模式：
1. 如果 `WIP` 从 0 变为 1，当前线程获得 drain 权限
2. 如果 `WIP` 已经大于 0，说明已有线程在 drain，当前线程直接返回
3. 获得权限的线程在完成一轮 drain 后检查 `WIP` 是否又增加了（有新数据入队），如果是则继续 drain

**为什么不用 `synchronized`？** 因为 `synchronized` 在竞争时会挂起线程（内核态切换），而 `WIP` 的 CAS 操作是用户态的自旋。在响应式流中，drain 操作通常很快（就是 poll + onNext），自旋等待的代价远小于线程切换。

### 5.4 drainRegular：常规模式下的背压控制

```java
// SinkManyUnicast.java
void drainRegular(CoreSubscriber<? super T> a) {
    int missed = 1;
    final Queue<T> q = queue;

    for (;;) {
        long r = requested;    // 读取当前需求量
        long e = 0L;          // 已发射计数

        while (r != e) {       // 在需求范围内消费
            boolean d = done;
            T t = q.poll();
            boolean empty = t == null;

            if (checkTerminated(d, empty, a, q, t)) {
                return;
            }
            if (empty) {
                break;
            }
            a.onNext(t);       // 推送给订阅者
            e++;
        }

        if (r == e) {          // 需求用完，检查是否终止
            if (checkTerminated(done, q.isEmpty(), a, q, null)) {
                return;
            }
        }

        if (e != 0 && r != Long.MAX_VALUE) {
            REQUESTED.addAndGet(this, -e); // 扣减已消费的需求
        }

        missed = WIP.addAndGet(this, -missed);
        if (missed == 0) {
            break;
        }
    }
}
```

这里实现了标准的 Reactive Streams 背压协议：只在 `requested` 范围内调用 `a.onNext(t)`，消费后用 `REQUESTED.addAndGet(this, -e)` 扣减需求量。如果 `r == Long.MAX_VALUE`（无界需求），则不扣减，这是性能优化。

### 5.5 checkTerminated：终止与取消的清理

```java
// SinkManyUnicast.java
boolean checkTerminated(boolean d, boolean empty, CoreSubscriber<? super T> a, 
                        Queue<T> q, @Nullable T t) {
    if (cancelled) {
        Operators.onDiscard(t, a.currentContext());
        Operators.onDiscardQueueWithClear(q, a.currentContext(), null);
        hasDownstream = false;
        return true;
    }
    if (d && empty) {
        Throwable e = error;
        hasDownstream = false;
        if (e != null) {
            a.onError(e);
        } else {
            a.onComplete();
        }
        return true;
    }
    return false;
}
```

**关键设计：取消时为什么要调用 `onDiscard`？** 如果队列中还有数据库连接、文件句柄等资源，直接清空队列会导致资源泄漏。`Operators.onDiscard` 会查找 Context 中注册的 `reactor.onDiscard.local` 钩子，让用户有机会执行清理逻辑（如关闭连接）。

---

## 六、SinkManyUnicastNoBackpressure：无队列直推模式

源码文件：`reactor/core/publisher/SinkManyUnicastNoBackpressure.java`

```java
final class SinkManyUnicastNoBackpressure<T> extends Flux<T> 
    implements InternalManySink<T>, Subscription, ContextHolder {

    enum State { INITIAL, SUBSCRIBED, TERMINATED, CANCELLED }
    
    volatile State state;
    volatile long  requested;
    private volatile @Nullable CoreSubscriber<? super T> actual = null;
}
```

这个实现完全去掉了队列，`tryEmitNext` 直接调用订阅者的 `onNext`：

```java
// SinkManyUnicastNoBackpressure.java
@Override
public Sinks.EmitResult tryEmitNext(T t) {
    switch (state) {
        case INITIAL:
            return Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER;
        case SUBSCRIBED:
            if (requested == 0L) {
                return Sinks.EmitResult.FAIL_OVERFLOW;  // 无需求，立即失败
            }
            CoreSubscriber<? super T> actualSubscriber = this.actual;
            actualSubscriber.onNext(t);                  // 直接推送
            Operators.produced(REQUESTED, this, 1);      // 扣减需求
            return Sinks.EmitResult.OK;
        case TERMINATED:
            return Sinks.EmitResult.FAIL_TERMINATED;
        case CANCELLED:
            return Sinks.EmitResult.FAIL_CANCELLED;
    }
}
```

**为什么需要这个实现？** 在"热点数据流"场景中，数据要么立即被消费，要么直接丢弃——不需要缓冲。`SinkManyUnicast` 的队列引入了内存开销和延迟（数据先入队再 drain），而 `NoBackpressure` 版本消除了这些开销。代价是：如果 `requested == 0`，推送立即失败返回 `FAIL_OVERFLOW`，数据不会被保留。

---

## 七、序列化机制：SinkManySerialized 与 AbstractSerializedSink

源码文件：`reactor/core/publisher/SinkManySerialized.java` 和 `reactor/core/publisher/SinksSpecs.java`

### 7.1 序列化包装层

当通过 `Sinks.many().unicast().onBackpressureBuffer()` 创建 Sink 时，实际返回的是 `SinkManySerialized` 包装了 `SinkManyUnicast`：

```java
// SinksSpecs.java
static final class UnicastSpecImpl implements Sinks.UnicastSpec {
    final boolean serialized;

    <T, MANY extends Many<T> & ContextHolder> Many<T> wrapMany(MANY original) {
        if (serialized) {
            return new SinkManySerialized<>(original, original);
        }
        return original;  // unsafe 模式不包装
    }

    @Override
    public <T> Many<T> onBackpressureBuffer() {
        final SinkManyUnicast<T> original = SinkManyUnicast.create();
        return wrapMany(original);
    }
}
```

### 7.2 AbstractSerializedSink 的 tryAcquire 机制

```java
// SinksSpecs.java
abstract static class AbstractSerializedSink {
    volatile int wip;
    static final AtomicIntegerFieldUpdater<AbstractSerializedSink> WIP =
            AtomicIntegerFieldUpdater.newUpdater(AbstractSerializedSink.class, "wip");

    volatile @Nullable Thread lockedAt;
    static final AtomicReferenceFieldUpdater<AbstractSerializedSink, @Nullable Thread> LOCKED_AT =
            AtomicReferenceFieldUpdater.newUpdater(AbstractSerializedSink.class, Thread.class, "lockedAt");

    boolean tryAcquire(Thread currentThread) {
        if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
            // 首次获取：CAS WIP 从 0 到 1，记录当前线程
            LOCKED_AT.lazySet(this, currentThread);
        }
        else {
            // 已有线程持有：检查是否是同一线程（可重入）
            if (LOCKED_AT.get(this) != currentThread) {
                return false;  // 不同线程，返回 FAIL_NON_SERIALIZED
            }
            WIP.incrementAndGet(this);  // 同线程重入，WIP +1
        }
        return true;
    }
}
```

### 7.3 SinkManySerialized 的 tryEmitNext

```java
// SinkManySerialized.java
@Override
public Sinks.EmitResult tryEmitNext(T t) {
    Objects.requireNonNull(t, "t is null in sink.next(t)");

    Thread currentThread = Thread.currentThread();
    if (!tryAcquire(currentThread)) {
        return Sinks.EmitResult.FAIL_NON_SERIALIZED;
    }

    try {
        return sink.tryEmitNext(t);  // 委托给底层实现
    } finally {
        if (WIP.decrementAndGet(this) == 0) {
            LOCKED_AT.compareAndSet(this, currentThread, null);  // 释放锁
        }
    }
}
```

**这个序列化机制不是传统的互斥锁，而是一个可重入的"线程亲和性"锁。** 它的核心特征是：
1. 第一个获取的线程获得"锁"，`LOCKED_AT` 记录该线程
2. 同一线程可以重入（`WIP` 递增）
3. 不同线程直接返回 `FAIL_NON_SERIALIZED`，而不是阻塞等待

**为什么不直接用 `synchronized` 或 `ReentrantLock`？** 因为响应式流规范要求 `onNext` 不能阻塞。如果用 `synchronized`，一个慢速生产者会阻塞另一个生产者的线程，违反了 Reactive Streams 的非阻塞约束。`FAIL_NON_SERIALIZED` 让调用者自己决定如何处理（重试、丢弃、抛异常），而不是被强制阻塞。

**反例：如果不做序列化，多线程同时调用 `sink.next()` 会怎样？** 以 `SinkManyUnicast` 为例，`queue.offer(t)` 和 `drain(t)` 都不是原子操作。两个线程同时 `offer` 可能破坏队列的内部结构（特别是非并发安全的队列），同时 `drain` 会导致同一个元素被推送两次或 `WIP` 计数混乱。更严重的是，Reactive Streams 规范规则 1.3 要求 `onNext` 调用必须串行化——并发调用 `onNext` 是规范违规。

---

## 八、SinkOneMulticast 与 SinkEmptyMulticast

源码文件：`reactor/core/publisher/SinkOneMulticast.java` 和 `reactor/core/publisher/SinkEmptyMulticast.java`

### 8.1 SinkEmptyMulticast：多订阅终止信号

`SinkEmptyMulticast` 是 `Sinks.empty()` 的底层实现，使用数组管理多个订阅者：

```java
// SinkEmptyMulticast.java
class SinkEmptyMulticast<T> extends Mono<T> implements InternalEmptySink<T> {
    volatile Inner<T>[] subscribers;
    
    static final Inner[] EMPTY = new Inner[0];
    static final Inner[] TERMINATED_EMPTY = new Inner[0];
    static final Inner[] TERMINATED_ERROR = new Inner[0];
    
    static final int STATE_ADDED = 0;
    static final int STATE_ERROR = -1;
    static final int STATE_EMPTY = -2;
}
```

注意 `EMPTY`、`TERMINATED_EMPTY`、`TERMINATED_ERROR` 虽然都是长度为 0 的数组，但它们是不同的对象引用，通过引用比较来区分状态。这是一种常见的无锁状态机设计模式。

### 8.2 SinkOneMulticast：带值的 Promise

`SinkOneMulticast` 继承自 `SinkEmptyMulticast`，增加了值存储能力：

```java
// SinkOneMulticast.java
final class SinkOneMulticast<O> extends SinkEmptyMulticast<O> implements InternalOneSink<O> {
    static final Inner[] TERMINATED_VALUE = new Inner[0];
    static final int STATE_VALUE = 1;

    @Nullable O value;

    @Override
    public EmitResult tryEmitValue(@Nullable O value) {
        Inner<O>[] prevSubscribers = this.subscribers;
        if (isTerminated(prevSubscribers)) {
            return EmitResult.FAIL_TERMINATED;
        }
        if (value == null) {
            return tryEmitEmpty();  // null 值降级为 empty
        }

        this.value = value;

        // CAS 将 subscribers 替换为 TERMINATED_VALUE
        for (;;) {
            if (SUBSCRIBERS.compareAndSet(this, prevSubscribers, TERMINATED_VALUE)) {
                break;
            }
            prevSubscribers = this.subscribers;
            if (isTerminated(prevSubscribers)) {
                return EmitResult.FAIL_TERMINATED;
            }
        }

        // 通知所有已订阅的 Inner
        for (Inner<O> as : prevSubscribers) {
            as.complete(value);
        }
        return EmitResult.OK;
    }
}
```

**为什么 `tryEmitValue(null)` 降级为 `tryEmitEmpty`？** 因为 `null` 在 Reactor 中有特殊语义——`Mono` 中的 `null` 表示"无值完成"。这避免了 `NullPointerException` 在订阅者端传播，同时保持了 API 的一致性：`One` 可以有值也可以无值，但不会有一个"null 值"。

### 8.3 订阅时的状态检查

```java
// SinkOneMulticast.java
@Override
public void subscribe(final CoreSubscriber<? super O> actual) {
    CoreSubscriber<? super O> wrapped =
            Operators.restoreContextOnSubscriberIfAutoCPEnabled(this, actual);
    NextInner<O> as = new NextInner<>(wrapped, this);
    wrapped.onSubscribe(as);
    final int addState = add(as);
    
    if (addState == STATE_ADDED) {
        if (as.isCancelled()) {
            remove(as);
        }
    }
    else if (addState == STATE_ERROR) {
        wrapped.onError(error);
    }
    else if (addState == STATE_EMPTY) {
        as.complete();           // 已 complete，无值
    }
    else {
        assert value != null;
        as.complete(value);      // 已 complete，有值
    }
}
```

这个 `add` 方法返回不同的状态码，让 `subscribe` 知道在订阅发生时 Sink 已经处于什么终态。如果 Sink 已经有值（`STATE_VALUE`），新订阅者立即收到该值——这就是 `One` 的"replay"语义。

---

## 九、SinkManyEmitterProcessor：多播背压处理器

源码文件：`reactor/core/publisher/SinkManyEmitterProcessor.java`

这是 `Sinks.many().multicast().onBackpressureBuffer()` 的底层实现，也是旧版 `EmitterProcessor` 的 Sinks 适配。

```java
// SinkManyEmitterProcessor.java
final class SinkManyEmitterProcessor<T> extends Flux<T> 
    implements InternalManySink<T>, Sinks.ManyWithUpstream<T>, 
               CoreSubscriber<T>, Scannable, Disposable, ContextHolder {

    final int prefetch;
    final boolean autoCancel;
    volatile Subscription s;
    volatile FluxPublish.PubSubInner<T>[] subscribers;
    volatile @Nullable Queue<T> queue;
    volatile boolean done;
    volatile @Nullable Throwable error;
    volatile int wip;
}
```

### 9.1 tryEmitNext 的队列延迟初始化

```java
// SinkManyEmitterProcessor.java
@Override
public EmitResult tryEmitNext(T t) {
    if (done) {
        return Sinks.EmitResult.FAIL_TERMINATED;
    }
    Queue<T> q = queue;
    if (q == null) {
        // 延迟初始化队列
        if (Operators.setOnce(S, this, Operators.emptySubscription())) {
            q = Queues.<T>get(prefetch).get();
            queue = q;
        }
        else {
            // 等待队列初始化完成
            for (; ; ) {
                if (isCancelled()) {
                    return EmitResult.FAIL_CANCELLED;
                }
                q = queue;
                if (q != null) {
                    break;
                }
            }
        }
    }
    if (!q.offer(t)) {
        return subscribers == EMPTY ? EmitResult.FAIL_ZERO_SUBSCRIBER : EmitResult.FAIL_OVERFLOW;
    }
    drain();
    return EmitResult.OK;
}
```

**为什么延迟初始化队列？** 因为 `SinkManyEmitterProcessor` 同时支持作为 `Sinks.Many`（手动推送）和 `Sinks.ManyWithUpstream`（订阅上游 Publisher）使用。在手动推送模式下，队列在第一次 `tryEmitNext` 时创建；在订阅上游模式下，队列在 `onSubscribe` 中根据上游的融合模式创建。延迟初始化避免了在构造时就分配队列内存，也解决了两种模式的队列配置冲突。

### 9.2 多订阅者背压：最小需求对齐

```java
// SinkManyEmitterProcessor.java (drain 方法片段)
long maxRequested = Long.MAX_VALUE;
int cancel = 0;

for (FluxPublish.PubSubInner<T> inner : a) {
    long r = inner.requested;
    if (r >= 0L) {
        maxRequested = Math.min(maxRequested, r);  // 取所有订阅者的最小需求
    }
    else { // Long.MIN_VALUE == CANCEL_REQUEST
        cancel++;
    }
}
```

多播模式下，一个元素要么发给所有订阅者，要么不发。因此 drain 循环以"最小需求"为限进行推送。如果某个订阅者很慢（需求为 0），其他订阅者也会被阻塞。这是"all-or-nothing"背压策略。

**反例：如果不做最小需求对齐会怎样？** 假设有两个订阅者，一个快一个慢。如果不做对齐，快订阅者会消费大量数据，而慢订阅者的队列溢出。更糟糕的是，由于是同一个 `onNext` 调用，快订阅者收到的数据和慢订阅者收到的数据会产生分歧，违反了多播的语义一致性保证。

---

## 十、SinkManyReplayProcessor：回放处理器

源码文件：`reactor/core/publisher/SinkManyReplayProcessor.java`

这是 `Sinks.many().replay()` 的底层实现，是旧版 `ReplayProcessor` 的 Sinks 适配。

```java
// SinkManyReplayProcessor.java
final class SinkManyReplayProcessor<T> extends Flux<T> 
    implements InternalManySink<T>, CoreSubscriber<T>, ContextHolder, 
               Disposable, Fuseable, Scannable {

    final FluxReplay.ReplayBuffer<T> buffer;
    volatile FluxReplay.ReplaySubscription<T>[] subscribers;
}
```

### 10.1 tryEmitNext：总是成功入缓冲区

```java
// SinkManyReplayProcessor.java
@Override
public Sinks.EmitResult tryEmitNext(T t) {
    FluxReplay.ReplayBuffer<T> b = buffer;
    if (b.isDone()) {
        return Sinks.EmitResult.FAIL_TERMINATED;
    }
    // 注意：ReplayProcessor 总是能缓冲元素，不存在 FAIL_ZERO_SUBSCRIBER
    b.add(t);
    for (FluxReplay.ReplaySubscription<T> rs : subscribers) {
        b.replay(rs);
    }
    return Sinks.EmitResult.OK;
}
```

**为什么 ReplayProcessor 不返回 `FAIL_ZERO_SUBSCRIBER`？** 因为回放语义要求"所有元素都被记住"。即使没有订阅者，元素也会被存入 `ReplayBuffer`，等订阅者到来时回放。这与 `SinkManyEmitterProcessor` 的"warm up"行为不同——EmitterProcessor 的缓冲区有大小限制，无订阅者时入队失败返回 `FAIL_ZERO_SUBSCRIBER`；而 ReplayBuffer 的设计目标就是无限（或大容量）存储。

### 10.2 多种 ReplayBuffer 策略

```java
// SinkManyReplayProcessor.java
static <E> SinkManyReplayProcessor<E> create(int historySize, boolean unbounded) {
    FluxReplay.ReplayBuffer<E> buffer;
    if (unbounded) {
        buffer = new FluxReplay.UnboundedReplayBuffer<>(historySize);
    }
    else {
        buffer = new FluxReplay.SizeBoundReplayBuffer<>(historySize);
    }
    return new SinkManyReplayProcessor<>(buffer);
}

static <T> SinkManyReplayProcessor<T> createSizeAndTimeout(int size, Duration maxAge, Scheduler scheduler) {
    return new SinkManyReplayProcessor<>(new FluxReplay.SizeAndTimeBoundReplayBuffer<>(
            size, maxAge.toNanos(), scheduler));
}
```

三种 ReplayBuffer 实现：
- `UnboundedReplayBuffer`：无界，回放所有历史（`Sinks.many().replay().all()`）
- `SizeBoundReplayBuffer`：有界，保留最近 N 个（`Sinks.many().replay().limit(N)`）
- `SizeAndTimeBoundReplayBuffer`：有界 + TTL，保留最近 N 个且在 maxAge 内（`Sinks.many().replay().limit(N, maxAge)`）

---

## 十一、Sinks 与旧版 Processor 的演进关系

源码文件：`reactor/core/publisher/SinksSpecs.java`

从 `SinksSpecs` 可以清晰看到 Sinks 和旧版 Processor 的映射关系：

```java
// SinksSpecs.java (UnsafeSpecImpl - 无序列化版本)
@Override
public <T> Sinks.Many<T> onBackpressureBuffer() {
    return new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);  // 旧 EmitterProcessor
}

@Override
public <T> Many<T> all() {
    return SinkManyReplayProcessor.create();  // 旧 ReplayProcessor
}
```

| Sinks API | 底层实现类 | 旧版 Processor 等价物 |
|-----------|-----------|---------------------|
| `Sinks.many().unicast().onBackpressureBuffer()` | `SinkManyUnicast` | `UnicastProcessor` |
| `Sinks.many().unicast().onBackpressureError()` | `SinkManyUnicastNoBackpressure` | 无直接等价（新增） |
| `Sinks.many().multicast().onBackpressureBuffer()` | `SinkManyEmitterProcessor` | `EmitterProcessor` |
| `Sinks.many().multicast().directAllOrNothing()` | `SinkManyBestEffort` | `DirectProcessor`（变体） |
| `Sinks.many().replay().all()` | `SinkManyReplayProcessor` | `ReplayProcessor` |
| `Sinks.one()` | `SinkOneMulticast` | `MonoProcessor`（已废弃） |
| `Sinks.empty()` | `SinkEmptyMulticast` | 无直接等价（新增） |

**为什么用 Sinks 替代 Processor？** 旧版 Processor 同时实现了 `Subscriber` 和 `Publisher` 接口，这导致两个问题：
1. 用户可以通过 `Processor.onSubscribe()` / `Processor.request()` / `Processor.cancel()` 干扰内部状态
2. 旧版 `Processor.onNext()` 在错误状态下只能抛异常或默默丢弃，调用者无法区分失败原因

Sinks 将"推送 API"（`tryEmitNext` 等）与"订阅 API"（`asFlux()` / `asMono()` 返回的 `Flux` / `Mono`）彻底分离，推送操作返回 `EmitResult` 枚举，让失败变得可检查、可恢复。

---

## 十二、unicast vs multicast 对比分析

### 12.1 订阅者数量

| 特性 | `Sinks.many().unicast()` | `Sinks.many().multicast()` |
|------|--------------------------|---------------------------|
| 最大订阅者数 | 1（第二个抛 `IllegalStateException`） | 无限制 |
| 订阅者管理 | 单个 `actual` 字段 | `PubSubInner[]` 数组 + CAS 增删 |
| 背压模型 | PUSH-PULL（队列 + requested） | 最小需求对齐 |
| 无订阅者时 | 数据入队等待（`onBackpressureBuffer`） | warm up（`onBackpressureBuffer`）或 fail fast |

### 12.2 `SinkManyUnicast` 的单订阅保证

```java
// SinkManyUnicast.java
@Override
public void subscribe(CoreSubscriber<? super T> actual) {
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        this.hasDownstream = true;
        this.actual = wrapped;
        wrapped.onSubscribe(this);
        subscriptionDelivered = true;
        if (cancelled) {
            this.hasDownstream = false;
        } else {
            drain(null);
        }
    } else {
        Operators.error(wrapped, new IllegalStateException(
            "Sinks.many().unicast() sinks only allow a single Subscriber"));
    }
}
```

`ONCE` 字段使用 CAS 保证只有一个订阅者能成功订阅。第二个订阅者会收到 `onSubscribe` 后立即收到 `onError(IllegalStateException)`。

### 12.3 `SinkManyEmitterProcessor` 的多订阅者管理

```java
// SinkManyEmitterProcessor.java
boolean add(EmitterInner<T> inner) {
    for (; ; ) {
        FluxPublish.PubSubInner<T>[] a = subscribers;
        if (a == TERMINATED) {
            return false;
        }
        int n = a.length;
        FluxPublish.PubSubInner<?>[] b = new FluxPublish.PubSubInner[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = inner;
        if (SUBSCRIBERS.compareAndSet(this, a, b)) {
            return true;
        }
    }
}
```

每次添加/移除订阅者都创建新数组并 CAS 替换，这是一种 copy-on-write 模式。虽然每次操作有 O(n) 的数组复制开销，但读取（drain 中的 `subscribers` 访问）是无锁的，适合"读多写少"的场景。

---

## 十三、多角度交叉验证：Sinks 的设计哲学

### 13.1 从 Reactive Streams 规范视角

Reactive Streams 规范规则 1.3 要求：`onNext` 调用必须串行化。Sinks 通过 `AbstractSerializedSink` 的 `tryAcquire` 机制在安全模式下保证了这一点，而 `unsafe()` 模式则将责任转移给调用者——如果调用者已经在 Reactive Streams 合规的上下文中（如操作符内部），则不需要额外的序列化开销。

### 13.2 从性能视角

| 操作 | SinkManyUnicast | SinkManyEmitterProcessor | SinkManyReplayProcessor |
|------|----------------|--------------------------|------------------------|
| tryEmitNext | queue.offer + drain | queue.offer + drain | buffer.add + replay all |
| 订阅者管理 | 单字段赋值 | 数组 CAS copy | 数组 CAS copy |
| 内存开销 | 1个队列 | 1个队列 + 订阅者数组 | 1个buffer + 订阅者数组 |
| 融合支持 | ASYNC fusion | ASYNC/SYNC fusion | ASYNC fusion |

### 13.3 从错误处理视角

Sinks 的 `EmitResult` 让错误处理从"异常驱动"变为"值驱动"。在响应式编程中，异常驱动的问题是：抛异常会打断响应式流的异步管线，而值驱动让调用者在当前上下文中做出决策，不打断管线的连续性。

---

## 十四、归纳表格：Sinks 类型对照表

| Sinks 类型 | 底层实现类 | 语义 | 订阅者数 | 背压策略 | 无订阅者时行为 | 回放 | 序列化包装 | 旧版等价 |
|-----------|-----------|------|---------|---------|--------------|------|-----------|---------|
| `many().unicast().onBackpressureBuffer()` | `SinkManyUnicast` | Flux | 1 | PUSH-PULL（队列+requested） | 入队等待 | 否 | `SinkManySerialized` | `UnicastProcessor` |
| `many().unicast().onBackpressureError()` | `SinkManyUnicastNoBackpressure` | Flux | 1 | 立即失败（无队列） | `FAIL_ZERO_SUBSCRIBER` | 否 | `SinkManySerialized` | 无 |
| `many().multicast().onBackpressureBuffer()` | `SinkManyEmitterProcessor` | Flux | N | 最小需求对齐 | warm up（缓冲到 prefetch） | 否 | `SinkManySerialized` | `EmitterProcessor` |
| `many().multicast().directAllOrNothing()` | `SinkManyBestEffort` | Flux | N | 全有或全无 | `FAIL_ZERO_SUBSCRIBER` | 否 | `SinkManySerialized` | `DirectProcessor`(变体) |
| `many().multicast().directBestEffort()` | `SinkManyBestEffort` | Flux | N | 最佳努力 | `FAIL_ZERO_SUBSCRIBER` | 否 | `SinkManySerialized` | `DirectProcessor`(变体) |
| `many().replay().all()` | `SinkManyReplayProcessor` | Flux | N | 独立背压 | 全部缓冲 | 全部 | `SinkManySerialized` | `ReplayProcessor` |
| `many().replay().limit(N)` | `SinkManyReplayProcessor` | Flux | N | 独立背压 | 缓冲最近N个 | 最近N个 | `SinkManySerialized` | `ReplayProcessor` |
| `many().replay().limit(N, maxAge)` | `SinkManyReplayProcessor` | Flux | N | 独立背压 | 缓冲N个且在TTL内 | N个+TTL | `SinkManySerialized` | `ReplayProcessor` |
| `one()` | `SinkOneMulticast` | Mono | N | 无需（单值） | 缓冲值，等待订阅 | 单值 | `SinkOneSerialized` | `MonoProcessor` |
| `empty()` | `SinkEmptyMulticast` | Mono | N | 无需（仅终止） | 缓冲终止信号 | 终止信号 | `SinkEmptySerialized` | 无 |
| `unsafe().many().*` | 同上各种 | Flux | 同上 | 同上 | 同上 | 同上 | **无**（外部同步） | 同上 |
