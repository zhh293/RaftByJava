# 数据源 Publisher 全流程源码解析

> **Reactor Core 源码深度研究系列 · 第 04 篇**
>
> 本篇系统性地解析 Reactor Core 中所有核心数据源 Publisher 的实现，从同步源到异步源、从空源到延迟源，逐一剖析其 subscribe 链路、Subscription 设计、背压策略与融合机制。

---

## 一、全局架构总览

在 Reactor Core 中，"数据源"（Source）是指处于响应式链最上游的 Publisher。它们没有上游 source，直接产生数据。所有数据源都实现 `SourceProducer<O>` 接口——一个同时继承 `Scannable` 和 `Publisher<O>` 的标记接口。

```
                    ┌──────────────────────────────────────────────────┐
                    │              SourceProducer<O>                   │
                    │   (extends Scannable, Publisher<O>)              │
                    │   scanUnsafe: PARENT=null, ACTUAL=null           │
                    │   stepName: "source(ClassName)"                  │
                    └──────────────────┬───────────────────────────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────────┐
          │                            │                                │
     ┌────▼─────┐               ┌─────▼──────┐                  ┌──────▼──────┐
     │ 同步源   │               │  异步源    │                  │  特殊源     │
     │ SYNC     │               │  ASYNC     │                  │             │
     └────┬─────┘               └─────┬──────┘                  └──────┬──────┘
          │                           │                                │
    ┌─────┼───────┐            ┌──────┼──────┐              ┌──────────┼──────────┐
    │     │       │            │      │       │              │          │          │
 Flux  Flux   Mono/        Flux  Flux   Flux            Flux      Mono/      Flux
Array  Range  FluxJust   Create Interval Never          Empty     MonoEmpty  Error
    │     │       │            │      │                   │          │          │
    │     │       │            │      │                   │          │          │
  Fuseable  Fuseable  Fuseable  非     非                  Fuseable   Fuseable   Fuseable
  ScalarCallable              Fuseable Fuseable           ScalarCallable
                                                   FluxDefer(延迟源, 懒加载)
                                                   FluxIterable(迭代器源)
                                                   FluxGenerate(生成器源)
```

### 数据源分类图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         数据源四大分类                                   │
├─────────────┬───────────────┬───────────────┬───────────────────────────┤
│   同步源    │   异步源      │    空源       │   延迟源                  │
│   SYNC      │   ASYNC       │               │                           │
├─────────────┼───────────────┼───────────────┼───────────────────────────┤
│ FluxArray   │ FluxCreate    │ FluxEmpty     │ FluxDefer                 │
│ FluxRange   │ FluxInterval  │ MonoEmpty     │                           │
│ MonoJust    │               │ FluxNever     │                           │
│ FluxJust    │               │ FluxError     │                           │
│ FluxIterable│               │               │                           │
│ FluxGenerate│               │               │                           │
└─────────────┴───────────────┴───────────────┴───────────────────────────┘
```

---

## 二、SourceProducer 接口——数据源的统一契约

### 2.1 接口定义

源码文件：`reactor/core/publisher/SourceProducer.java`

```java
interface SourceProducer<O> extends Scannable, Publisher<O> {

    @Override
    default @Nullable Object scanUnsafe(Attr key) {
        if (key == Attr.PARENT) return null;
        if (key == Attr.ACTUAL) return null;
        if (key == InternalProducerAttr.INSTANCE) return true;
        return null;
    }

    @Override
    default String stepName() {
        return "source(" + getClass().getSimpleName() + ")";
    }
}
```

### 2.2 为什么需要 SourceProducer？

**去掉会怎样？** 如果没有 `SourceProducer` 接口，所有数据源类将直接实现 `Publisher`，那么：

1. **Scannable 不可用**：操作符链的调试和监控（如 `Flux.dump()`）需要通过 `scanUnsafe` 向上游遍历。数据源没有上游，`PARENT` 和 `ACTUAL` 都应为 `null`。`SourceProducer` 在接口层固化了这一语义。
2. **stepName 无法统一**：操作符链的字符串表示（用于日志和监控）需要知道链的起点。`stepName()` 默认返回 `"source(FluxRange)"` 这样的格式，让链可视化工具能区分数据源和操作符。
3. **InternalProducerAttr 标记**：`InternalProducerAttr.INSTANCE` 标记当前对象是链的源头，下游操作符遍历时遇到此标记即知到达链头。

从不同视角来看：
- **调试视角**：`SourceProducer` 让 `Scannable` 链有了明确的终止点。
- **性能视角**：接口的 `default` 方法避免了每个数据源类重复实现相同的 `scanUnsafe` 逻辑。
- **架构视角**：它建立了"数据源 vs 操作符"的二分法——操作符有 `source`（上游），数据源没有。

---

## 三、同步源详解

### 3.1 FluxArray——数组数据源

源码文件：`reactor/core/publisher/FluxArray.java`

`FluxArray` 是最典型的同步数据源。它持有一个 `T[] array`，在 subscribe 时将数组内容逐个发射给订阅者。

#### 3.1.1 类声明与核心字段

```java
final class FluxArray<T> extends Flux<T> implements Fuseable, SourceProducer<T> {
    final T[] array;
}
```

`FluxArray` 同时实现了 `Fuseable` 和 `SourceProducer`。`Fuseable` 表示它支持队列融合优化（SYNC 模式），`SourceProducer` 表示它是链的源头。

#### 3.1.2 subscribe 方法的 ConditionalSubscriber 优化

```java
@SuppressWarnings("unchecked")
public static <T> void subscribe(CoreSubscriber<? super T> s, T[] array) {
    if (array.length == 0) {
        Operators.complete(s);
        return;
    }
    if (s instanceof ConditionalSubscriber) {
        s.onSubscribe(new ArrayConditionalSubscription<>((ConditionalSubscriber<? super T>) s, array));
    }
    else {
        s.onSubscribe(new ArraySubscription<>(s, array));
    }
}
```

**为什么需要 ConditionalSubscriber 分支？** `ConditionalSubscriber` 提供了 `tryOnNext(T)` 方法，返回 boolean 表示是否消费了该值。在 `filter` 等操作符下游，如果数据源的 `onNext` 被过滤掉，传统方式需要额外的 `request(1)` 补偿。`ConditionalSubscriber` 让数据源直接知道值是否被消费，避免了"发出去再要回来"的浪费。`ArrayConditionalSubscription` 在 slowPath 中，只有 `tryOnNext` 返回 `true` 时才递增 `e`（已发射计数）：

```java
// ArrayConditionalSubscription.slowPath 中
boolean b = s.tryOnNext(t);
if (cancelled) return;
i++;
if (b) {
    e++;
}
```

#### 3.1.3 ArraySubscription 的 fastPath 与 slowPath

`ArraySubscription` 实现了 `InnerProducer<T>` 和 `SynchronousSubscription<T>`。后者继承 `QueueSubscription<T>`，使其可以被用于 SYNC 融合。

```java
static final class ArraySubscription<T>
        implements InnerProducer<T>, SynchronousSubscription<T> {
    final CoreSubscriber<? super T> actual;
    final T[] array;
    int index;
    volatile boolean cancelled;
    volatile long requested;
    static final AtomicLongFieldUpdater<ArraySubscription> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(ArraySubscription.class, "requested");
}
```

**request 方法的核心逻辑：**

```java
public void request(long n) {
    if (Operators.validate(n)) {
        if (Operators.addCap(REQUESTED, this, n) == 0) {
            if (n == Long.MAX_VALUE) {
                fastPath();
            }
            else {
                slowPath(n);
            }
        }
    }
}
```

`Operators.addCap` 返回累加前的旧值。如果旧值为 0，说明当前线程是第一个触发 request 的线程，可以进入发射流程；如果旧值非 0，说明已有其他线程在发射，避免重复进入。

**fastPath** 用于 `request(Long.MAX_VALUE)`（无界请求），直接遍历数组全部发射：

```java
void fastPath() {
    final T[] a = array;
    final int len = a.length;
    final Subscriber<? super T> s = actual;
    for (int i = index; i != len; i++) {
        if (cancelled) return;
        T t = a[i];
        if (t == null) {
            s.onError(new NullPointerException("The " + i + "th array element was null"));
            return;
        }
        s.onNext(t);
    }
    if (cancelled) return;
    s.onComplete();
}
```

**slowPath** 处理有限请求，通过 `requested` 的 CAS 实现背压：

```java
void slowPath(long n) {
    // ... 循环逻辑：发射 n 个后，检查 requested 是否有新增
    // 通过 REQUESTED.addAndGet(this, -e) 扣减已发射数量
    // 如果扣减后为 0，退出循环等待下一次 request
}
```

**去掉 fastPath 会怎样？** 如果只用 slowPath，即使请求 `Long.MAX_VALUE`，每次循环都要做 CAS 操作（`REQUESTED.addAndGet`），在大数组场景下性能显著下降。fastPath 通过跳过 CAS，实现了"零开销全量发射"。

#### 3.1.4 SYNC 融合支持

`ArraySubscription` 实现了 `SynchronousSubscription<T>`，意味着它支持 SYNC 融合。下游可以通过 `requestFusion(SYNC)` 获取一个 `QueueSubscription`，直接通过 `poll()` 拉取数据：

```java
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

**多角度验证：** 从下游操作符的视角看，如果上游是 `FluxArray`，`filter` 操作符可以通过融合直接 `poll()`，对每个值执行 predicate，避免了 `onNext` → `request(1)` 的往返开销。从 `FluxArray` 自身视角看，`poll()` 只是简单的数组索引递增，无锁无竞争。

### 3.2 FluxRange——整数范围数据源

源码文件：`reactor/core/publisher/FluxRange.java`

```java
final class FluxRange extends Flux<Integer>
        implements Fuseable, SourceProducer<Integer> {
    final long start;
    final long end;
}
```

`FluxRange` 与 `FluxArray` 的结构高度相似，但数据源是 `[start, end)` 的整数序列，而非数组。它的 `subscribe` 方法有三条路径：

```java
public void subscribe(CoreSubscriber<? super Integer> actual) {
    long st = start;
    long en = end;
    if (st == en) {
        Operators.complete(actual);  // 空范围直接完成
        return;
    }
    if (st + 1 == en) {
        actual.onSubscribe(Operators.scalarSubscription(actual, (int)st));  // 单元素优化
        return;
    }
    if (actual instanceof ConditionalSubscriber) {
        actual.onSubscribe(new RangeSubscriptionConditional(...));
        return;
    }
    actual.onSubscribe(new RangeSubscription(actual, st, en));
}
```

**单元素优化**：当范围只有一个值时（`st + 1 == en`），直接使用 `Operators.scalarSubscription`——一个预定义的标量 Subscription，避免了创建完整的 `RangeSubscription` 对象。去掉这个优化会怎样？每次 `Flux.range(5, 1)` 都会创建一个 `RangeSubscription`，但该 Subscription 只会发射一次就完成，对象开销与功能不匹配。

### 3.3 MonoJust 与 FluxJust——标量数据源

源码文件：`reactor/core/publisher/MonoJust.java` 和 `reactor/core/publisher/FluxJust.java`

#### 3.3.1 MonoJust

```java
final class MonoJust<T> extends Mono<T>
        implements Fuseable.ScalarCallable<T>, Fuseable, SourceProducer<T> {
    final T value;

    MonoJust(T value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    @Override
    public T call() throws Exception {
        return value;
    }

    @Override
    public T block(Duration m) { return value; }

    @Override
    public T block() { return value; }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        actual.onSubscribe(Operators.scalarSubscription(actual, value));
    }
}
```

#### 3.3.2 ScalarCallable 接口的关键作用

`Fuseable.ScalarCallable<T>` 是一个继承自 `Callable<T>` 的接口。它标记该 Publisher "可以在不订阅的情况下直接获取值"。

**为什么这很重要？** 考虑 `Mono.just(1).map(x -> x + 1).filter(x -> x > 0)` 这样的链。如果 `map` 操作符在 subscribe 时发现上游是 `ScalarCallable`，它可以直接调用 `call()` 获取值，同步执行 mapper 函数，然后把自己也变成一个标量源——整个过程不需要创建任何 Subscription 对象。这就是 `FluxFlatMap.trySubscribeScalarMap` 等优化的基础：

```java
// FluxFlatMap.trySubscribeScalarMap 中的优化逻辑
if (source instanceof Callable) {
    T t = ((Callable<? extends T>) source).call();
    if (t == null) {
        Operators.complete(s);
        return true;
    }
    Publisher<? extends R> p = mapper.apply(t);
    if (p instanceof Callable) {
        R v = ((Callable<R>) p).call();
        if (v != null) {
            s.onSubscribe(Operators.scalarSubscription(s, v));
        } else {
            Operators.complete(s);
        }
    }
    return true;
}
```

**去掉 ScalarCallable 会怎样？** 每个 `Mono.just(x).map(f)` 都会走完整的订阅链路：创建 `MapSubscriber`、调用 `onSubscribe`、`request(1)`、`onNext`、`onComplete`。对于单值源，这些对象创建和方法调用的开销完全是浪费。

#### 3.3.3 FluxJust 与 MonoJust 的对比

```java
final class FluxJust<T> extends Flux<T>
        implements Fuseable.ScalarCallable<T>, Fuseable, SourceProducer<T> {
    final T value;

    @Override
    public void subscribe(final CoreSubscriber<? super T> actual) {
        actual.onSubscribe(Operators.scalarSubscription(actual, value, "just"));
    }
}
```

两者几乎完全一致，区别仅在于：`MonoJust` 继承 `Mono<T>`（语义上只发射一个值），`FluxJust` 继承 `Flux<T>`（语义上可以发射零到多个值但此处只发射一个）。`MonoJust` 额外重写了 `block()` 和 `block(Duration)` 直接返回值，而 `FluxJust` 没有。

### 3.4 FluxIterable——迭代器数据源

源码文件：`reactor/core/publisher/FluxIterable.java`

`FluxIterable` 将一个 `Iterable` 转换为 Flux。它通过 `Spliterator` 进行遍历，支持有限性检测和关闭回调。

```java
final class FluxIterable<T> extends Flux<T> implements Fuseable, SourceProducer<T> {
    final Iterable<? extends T> iterable;
    private final @Nullable Runnable onClose;
}
```

#### 3.4.1 有限性检测

```java
static <T> boolean checkFinite(Spliterator<? extends T> spliterator) {
    return spliterator.hasCharacteristics(Spliterator.SIZED);
}
```

**为什么需要检测有限性？** `FluxIterable` 在 cancel 或 error 时需要丢弃（discard）剩余元素。如果 Iterable 是无限的（如 `Stream.iterate`），直接遍历丢弃会导致死循环。`checkFinite` 通过检查 `Spliterator.SIZED` 特性来判断是否可以安全遍历。

#### 3.4.2 IterableSubscription 的状态机

`IterableSubscription` 使用一个四状态机管理迭代：

```java
static final int STATE_HAS_NEXT_NO_VALUE  = 0;  // hasNext 返回 true 但值未取
static final int STATE_HAS_NEXT_HAS_VALUE = 1;  // 有值在 current 中
static final int STATE_NO_NEXT            = 2;  // 没有更多值
static final int STATE_CALL_HAS_NEXT      = 3;  // 值已消费，需调 hasNext
```

`hasNext()` 方法使用 `spliterator.tryAdvance(this)` 实现，其中 `this` 是 `Consumer<T>`，`accept` 方法将值存入 `nextElement`：

```java
boolean hasNext() {
    if (!valueReady)
        spliterator.tryAdvance(this);
    return valueReady;
}

@Override
public void accept(T t) {
    valueReady = true;
    nextElement = t;
}
```

**为什么用 tryAdvance 而非 Iterator？** `Spliterator` 比 `Iterator` 更灵活：支持并行分割、支持特性查询（SIZED、ORDERED 等）、且 `tryAdvance` 是一次性的消费操作，避免了 `Iterator` 的 `hasNext` + `next` 两步操作可能的不一致问题。

### 3.5 FluxGenerate——同步生成器数据源

源码文件：`reactor/core/publisher/FluxGenerate.java`

```java
final class FluxGenerate<T, S extends @Nullable Object> extends Flux<T>
        implements Fuseable, SourceProducer<T> {
    final Callable<S> stateSupplier;
    final BiFunction<S, SynchronousSink<T>, S> generator;
    final Consumer<? super S> stateConsumer;
}
```

`FluxGenerate` 是一个带状态的同步生成器。用户通过 `BiFunction<S, SynchronousSink<T>, S>` 在每次调用中生成一个值并通过 `SynchronousSink.next()` 发射，返回新的状态。

#### 3.5.1 GenerateSubscription 的 "one call to onNext" 约束

```java
@Override
public void next(T t) {
    if (terminate) {
        Operators.onNextDropped(t, actual.currentContext());
        return;
    }
    if (hasValue) {
        error(new IllegalStateException("More than one call to onNext"));
        return;
    }
    if (t == null) {
        error(new NullPointerException("The generator produced a null value"));
        return;
    }
    hasValue = true;
    if (outputFused) {
        generatedValue = t;
    } else {
        actual.onNext(t);
    }
}
```

**为什么限制每次只调一次 next？** `FluxGenerate` 是同步生成器，设计为"一次调用产生一个值"。如果允许在一次 generator 调用中多次 `next()`，则无法实现背压——generator 一次产生了多个值，但下游只请求了 1 个。去掉这个限制会导致背压协议被破坏。

#### 3.5.2 状态清理

```java
void cleanup(@Nullable S s) {
    try {
        state = null;
        stateConsumer.accept(s);
    } catch (Throwable e) {
        Operators.onErrorDropped(e, actual.currentContext());
    }
}
```

每次 generator 结束（cancel/error/complete）时都会调用 `cleanup`，让用户清理状态（如关闭文件、释放资源）。`state` 被置 null 以帮助 GC。

---

## 四、异步源详解

### 4.1 FluxCreate——编程式数据源

源码文件：`reactor/core/publisher/FluxCreate.java`

`FluxCreate` 是 Reactor 中最灵活的数据源——它暴露一个 `FluxSink<T>` 给用户代码，用户可以在任意线程、任意时机调用 `sink.next()` 发射数据。

```java
final class FluxCreate<T> extends Flux<T> implements SourceProducer<T> {
    enum CreateMode { PUSH_ONLY, PUSH_PULL }

    final Consumer<? super FluxSink<T>> source;
    final OverflowStrategy backpressure;
    final CreateMode createMode;
}
```

#### 4.1.1 subscribe 流程

```java
public void subscribe(CoreSubscriber<? super T> actual) {
    CoreSubscriber<? super T> wrapped =
            Operators.restoreContextOnSubscriberIfAutoCPEnabled(this, actual);
    BaseSink<T> sink = createSink(wrapped, backpressure);
    wrapped.onSubscribe(sink);
    try {
        source.accept(
                createMode == CreateMode.PUSH_PULL ? new SerializedFluxSink<>(sink) : sink);
    }
    catch (Throwable ex) {
        Exceptions.throwIfFatal(ex);
        sink.error(Operators.onOperatorError(ex, wrapped.currentContext()));
    }
}
```

关键步骤：
1. 创建对应 OverflowStrategy 的 Sink
2. 调用 `onSubscribe` 将 Sink 作为 Subscription 传给下游
3. 调用用户的 `source.accept(sink)` 开始数据生产
4. 如果用户代码抛异常，通过 `sink.error()` 传递

**为什么 PUSH_PULL 模式需要 SerializedFluxSink？** 在 PUSH_ONLY 模式下，用户承诺只在单个线程调用 sink。但在 PUSH_PULL 模式下，`onRequest` 回调可能在 request 线程执行，而 `next()` 在生产者线程执行——两个线程可能并发调用 sink。`SerializedFluxSink` 通过 WIP（work-in-progress）CAS 和 MPSC 队列实现线程安全。

#### 4.1.2 五种 OverflowStrategy 对应的 Sink 类型

```java
static <T> BaseSink<T> createSink(CoreSubscriber<? super T> t,
        OverflowStrategy backpressure) {
    switch (backpressure) {
        case IGNORE: return new IgnoreSink<>(t);
        case ERROR:  return new ErrorAsyncSink<>(t);
        case DROP:   return new DropAsyncSink<>(t);
        case LATEST: return new LatestAsyncSink<>(t);
        default:     return new BufferAsyncSink<>(t, Queues.SMALL_BUFFER_SIZE);
    }
}
```

| OverflowStrategy | Sink 类型 | 行为 | 溢出处理 |
|---|---|---|---|
| BUFFER (default) | BufferAsyncSink | 无界队列缓冲 | 队列无界，OOM 风险 |
| LATEST | LatestAsyncSink | 只保留最新值 | 旧值被 discard |
| DROP | DropAsyncSink | 丢弃新值 | 直接 discard |
| ERROR | ErrorAsyncSink | 抛出 OverflowException | 调用 `error()` |
| IGNORE | IgnoreSink | 忽略背压，直接发 | 无背压控制 |

#### 4.1.3 BaseSink——所有 Sink 的基类

```java
static abstract class BaseSink<T> extends AtomicBoolean
        implements FluxSink<T>, InnerProducer<T> {

    static final Disposable TERMINATED = OperatorDisposables.DISPOSED;
    static final Disposable CANCELLED  = Disposables.disposed();

    final CoreSubscriber<? super T> actual;
    final Context ctx;
    volatile @Nullable Disposable disposable;
    volatile long requested;

    BaseSink(CoreSubscriber<? super T> actual) {
        this.actual = actual;
        this.ctx = actual.currentContext();
        REQUESTED.lazySet(this, Long.MIN_VALUE);
    }
}
```

**requested 的初始值 Long.MIN_VALUE 设计：** 这是一个巧妙的位标记。`Long.MIN_VALUE` 的最高位为 1，其余为 0。`BaseSink.hasRequestConsumer` 方法检查最高位：

```java
static boolean hasRequestConsumer(long requestedState) {
    return (requestedState & Long.MIN_VALUE) == 0;
}
```

初始状态 `Long.MIN_VALUE` 表示"requestConsumer 尚未设置"。当 `onPushPullRequest` 或 `onPushRequest` 被调用时，`markRequestConsumerSet` 会清除最高位：

```java
static <T> long markRequestConsumerSet(BaseSink<T> instance) {
    long u, s;
    for (;;) {
        s = instance.requested;
        if (hasRequestConsumer(s)) return s;
        u = s & Long.MAX_VALUE;  // 清除最高位
        if (REQUESTED.compareAndSet(instance, s, u)) return u;
    }
}
```

**去掉这个设计会怎样？** 如果用一个单独的 `volatile boolean requestConsumerSet` 字段，需要额外的内存屏障和字段访问。将标记编码到 `requested` 的高位中，节省了一个 volatile 字段的开销，且保证了 `requestConsumer` 设置与 `requested` 累加的原子性——它们通过同一个 CAS 操作完成。

#### 4.1.4 IgnoreSink——IGNORE 策略

```java
static final class IgnoreSink<T> extends BaseSink<T> {
    @Override
    public FluxSink<T> next(T t) {
        if (isTerminated()) {
            Operators.onNextDropped(t, ctx);
            return this;
        }
        if (isCancelled()) {
            Operators.onDiscard(t, ctx);
            return this;
        }
        actual.onNext(t);
        for (;;) {
            long s = requested;
            long r = s & Long.MAX_VALUE;
            if (r == 0L || REQUESTED.compareAndSet(this, s, (r - 1) | (s & Long.MIN_VALUE))) {
                return this;
            }
        }
    }
}
```

IgnoreSink **直接调用 `actual.onNext(t)`**，完全无视下游的请求量。但它仍然递减 `requested`（如果非零），这是一种"尽力而为"的记账——让 `requestedFromDownstream()` 返回值不至于完全不准确。

**为什么说 IGNORE 危险？** 如果下游用了 `limit()` 或 `take()` 等基于 request 的操作符，IGNORE 会导致下游收到远超请求量的数据。虽然 `take()` 会 cancel 上游，但在 cancel 生效前已经 `onNext` 出去的值无法收回。

#### 4.1.5 NoOverflowBaseAsyncSink——DROP 和 ERROR 的基类

```java
static abstract class NoOverflowBaseAsyncSink<T> extends BaseSink<T> {
    @Override
    public final FluxSink<T> next(T t) {
        if (isTerminated()) {
            Operators.onNextDropped(t, ctx);
            return this;
        }
        if (requestedFromDownstream() != 0) {
            actual.onNext(t);
            produced(this, 1);
        }
        else {
            onOverflow();  // 子类决定溢出策略
            Operators.onDiscard(t, ctx);
        }
        return this;
    }
    abstract void onOverflow();
}
```

- `DropAsyncSink.onOverflow()`：空实现，直接丢弃。
- `ErrorAsyncSink.onOverflow()`：调用 `error(Exceptions.failWithOverflow())`。

#### 4.1.6 BufferAsyncSink——BUFFER 策略

```java
static final class BufferAsyncSink<T> extends BaseSink<T> {
    final Queue<T> queue;
    @Nullable Throwable error;
    volatile boolean done;
    volatile int wip;

    BufferAsyncSink(CoreSubscriber<? super T> actual, int capacityHint) {
        super(actual);
        this.queue = Queues.<T>unbounded(capacityHint).get();
    }

    @Override
    public FluxSink<T> next(T t) {
        queue.offer(t);
        drain();
        return this;
    }
}
```

`BufferAsyncSink` 使用一个无界队列缓冲溢出的值。`drain()` 方法通过 WIP CAS 保证只有一个线程在排空队列，按下游请求量逐个发射。

#### 4.1.7 LatestAsyncSink——LATEST 策略

```java
static final class LatestAsyncSink<T> extends BaseSink<T> {
    final AtomicReference<@Nullable T> queue;

    @Override
    public FluxSink<T> next(T t) {
        T old = queue.getAndSet(t);
        Operators.onDiscard(old, ctx);
        drain();
        return this;
    }
}
```

`LatestAsyncSink` 使用 `AtomicReference` 只保留最新值。每次 `next()` 用 `getAndSet` 替换旧值，旧值被 discard。这相当于一个容量为 1 的有界队列。

---

## 五、空源与特殊源

### 5.1 FluxEmpty 与 MonoEmpty

源码文件：`reactor/core/publisher/FluxEmpty.java` 和 `reactor/core/publisher/MonoEmpty.java`

两者都是单例模式，subscribe 时只调用 `Operators.complete(actual)`：

```java
// FluxEmpty
final class FluxEmpty extends Flux<Object>
        implements Fuseable.ScalarCallable<Object>, SourceProducer<Object> {
    private static final Flux<Object> INSTANCE = new FluxEmpty();

    @Override
    public void subscribe(CoreSubscriber<? super Object> actual) {
        Operators.complete(actual);
    }

    @Override
    public @Nullable Object call() throws Exception {
        return null; /* Scalar optimizations on empty */
    }
}
```

**为什么是单例？** 空源没有状态，不需要每个订阅创建新实例。单例模式节省了对象分配开销。通过 `instance()` 方法做泛型强转：

```java
@SuppressWarnings("unchecked")
public static <T> Flux<T> instance() {
    return (Flux<T>) INSTANCE;
}
```

**ScalarCallable 的 call() 返回 null：** 当 `Flux.empty().map(f)` 这样的链遇到 ScalarCallable 优化时，`call()` 返回 null 表示"没有值"，直接走 `Operators.complete(s)` 路径，跳过整个订阅链。

### 5.2 FluxNever——永不完成的源

源码文件：`reactor/core/publisher/FluxNever.java`

```java
final class FluxNever extends Flux<Object> implements SourceProducer<Object> {
    static final Publisher<Object> INSTANCE = new FluxNever();

    @Override
    public void subscribe(CoreSubscriber<? super Object> actual) {
        actual.onSubscribe(Operators.emptySubscription());
    }
}
```

`FluxNever` 只调用 `onSubscribe`，永不调用 `onNext`、`onComplete` 或 `onError`。用于测试和特殊场景（如占位符）。

### 5.3 FluxError——立即错误的源

源码文件：`reactor/core/publisher/FluxError.java`

```java
final class FluxError<T> extends Flux<T> implements Fuseable.ScalarCallable, SourceProducer<T> {
    final Throwable error;

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        Operators.error(actual, error);
    }

    @Override
    public Object call() throws Exception {
        if (error instanceof Exception) {
            throw ((Exception) error);
        }
        throw Exceptions.propagate(error);
    }
}
```

`FluxError` 在 subscribe 时立即通过 `Operators.error()` 传递错误。作为 `ScalarCallable`，它的 `call()` 方法直接抛出异常——这使得 `Flux.error(e).map(f)` 能在 ScalarCallable 优化路径中直接走 error 分支。

---

## 六、延迟源：FluxDefer

源码文件：`reactor/core/publisher/FluxDefer.java`

```java
final class FluxDefer<T> extends Flux<T> implements SourceProducer<T> {
    final Supplier<? extends Publisher<? extends T>> supplier;

    @Override
    @SuppressWarnings("unchecked")
    public void subscribe(CoreSubscriber<? super T> actual) {
        Publisher<? extends T> p;
        try {
            p = Objects.requireNonNull(supplier.get(),
                    "The Publisher returned by the supplier is null");
        }
        catch (Throwable e) {
            Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
            return;
        }
        from(p).subscribe(actual);
    }
}
```

### 6.1 懒加载设计动机

**为什么需要 Defer？** 考虑以下场景：

```java
Mono<User> user = userRepository.findById(userId);  // 在这一行就执行了查询
return user.flatMap(u -> renderPage(u));
```

如果 `userRepository.findById` 返回的是一个 Hot Publisher（立即执行），那么查询在创建 Mono 时就已经发出，而不是在订阅时。`Defer` 解决了这个问题：

```java
Mono<User> user = Mono.defer(() -> userRepository.findById(userId));  // 订阅时才执行
```

**去掉 Defer 会怎样？** 每次 `Flux.defer(() -> ...)` 都变成直接调用，Supplier 中的代码在构造时执行而非订阅时。对于需要"每次订阅都重新创建 Publisher"的场景（如 HTTP 请求重试），没有 Defer 就无法实现正确的重订阅。

### 6.2 subscribe 的 from(p) 转换

`from(p)` 是 `Flux.from` 方法，它检查 `p` 是否已经是 `Flux` 或 `Mono`，如果是则直接返回，否则包装为 `FluxFromPublisher`。这确保了无论 Supplier 返回什么类型的 Publisher，最终都能以 Flux 的形式订阅。

---

## 七、异步定时源：FluxInterval

源码文件：`reactor/core/publisher/FluxInterval.java`

```java
final class FluxInterval extends Flux<Long> implements SourceProducer<Long> {
    final Scheduler timedScheduler;
    final long initialDelay;
    final long period;
    final TimeUnit unit;
}
```

### 7.1 IntervalRunnable——定时发射的核心

```java
static final class IntervalRunnable implements Runnable, Subscription,
                                               InnerProducer<Long> {
    final CoreSubscriber<? super Long> actual;
    final Worker worker;
    volatile long requested;
    long count;
    volatile boolean cancelled;

    @Override
    public void run() {
        if (!cancelled) {
            if (requested != 0L) {
                actual.onNext(count++);
                if (requested != Long.MAX_VALUE) {
                    REQUESTED.decrementAndGet(this);
                }
            } else {
                cancel();
                actual.onError(Exceptions.failWithOverflow(
                    "Could not emit tick " + count + " due to lack of requests" +
                    " (interval doesn't support small downstream requests that replenish slower than the ticks)"));
            }
        }
    }
}
```

**FluxInterval 的溢出处理是 ERROR 策略：** 当下游请求不足时，`IntervalRunnable` 直接 cancel 并发出 `OverflowException`。这与其他数据源不同——FluxInterval 没有可配置的 OverflowStrategy。

**为什么 FluxInterval 不支持 BUFFER 策略？** Interval 是基于定时器的，如果下游消费慢而缓冲，定时器会持续产生 tick，队列无限增长。更关键的是，Interval 的语义是"周期性发射递增的 long 值"——如果缓冲了过期的 tick 值，发射它们没有实际意义（用户需要的是"当前时间"而非"过去的时间"）。所以直接报错比缓冲更合理。

### 7.2 subscribe 流程

```java
public void subscribe(CoreSubscriber<? super Long> actual) {
    Worker w = timedScheduler.createWorker();
    IntervalRunnable r = new IntervalRunnable(actual, w);
    actual.onSubscribe(r);
    try {
        w.schedulePeriodically(r, initialDelay, period, unit);
    }
    catch (RejectedExecutionException ree) {
        if (!r.cancelled) {
            actual.onError(Operators.onRejectedExecution(ree, r, null, null,
                    actual.currentContext()));
        }
    }
}
```

注意 `schedulePeriodically` 可能抛出 `RejectedExecutionException`（如 Scheduler 已关闭）。此时如果订阅者尚未 cancel，则传递错误。

---

## 八、SerializedFluxSink 的线程安全机制

在 `FluxCreate` 的 `PUSH_PULL` 模式下，`SerializedFluxSink` 包装了 `BaseSink`，提供线程安全的 `next()` 调用：

```java
@Override
public FluxSink<T> next(T t) {
    Objects.requireNonNull(t, "t is null in sink.next(t)");
    if (sink.isTerminated() || done) {
        Operators.onNextDropped(t, sink.currentContext());
        return this;
    }
    if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
        try {
            sink.next(t);
        }
        catch (Throwable ex) {
            Operators.onOperatorError(sink, ex, t, sink.currentContext());
        }
        if (WIP.decrementAndGet(this) == 0) {
            return this;
        }
    }
    else {
        this.mpscQueue.offer(t);
        if (WIP.getAndIncrement(this) != 0) {
            return this;
        }
    }
    drainLoop();
    return this;
}
```

**设计原理：** 这是一个典型的 "try-lock or enqueue" 模式：
1. 尝试 CAS 获取锁（WIP 0→1）
2. 如果成功，直接调用 `sink.next(t)`，然后释放锁
3. 如果释放后 WIP 非 0，说明有其他线程在排队，进入 `drainLoop`
4. 如果 CAS 失败，将值放入 MPSC 队列，递增 WIP
5. 如果递增后 WIP 非 0，说明已有线程在 drain，直接返回

**去掉 MPSC 队列会怎样？** 如果只用 CAS 锁，当多个线程并发调用 `next()` 时，未获取锁的线程需要自旋等待。在高并发场景下，自旋浪费 CPU 且可能导致延迟突增。MPSC 队列让未获取锁的线程快速入队后返回，由持有锁的线程在 `drainLoop` 中统一排空。

---

## 九、数据源类型对照表

| 源类名 | 发射数量 | 是否 Fuseable | RunStyle | 是否异步 | 典型场景 |
|---|---|---|---|---|---|
| `FluxArray` | 0~N | 是 (SYNC) | SYNC | 否 | `Flux.just(1,2,3)` |
| `FluxRange` | 0~N | 是 (SYNC) | SYNC | 否 | `Flux.range(1, 10)` |
| `MonoJust` | 恰好 1 | 是 (ScalarCallable) | SYNC | 否 | `Mono.just("hello")` |
| `FluxJust` | 恰好 1 | 是 (ScalarCallable) | SYNC | 否 | `Flux.just("hello")` |
| `FluxEmpty` | 0 | 是 (ScalarCallable) | SYNC | 否 | `Flux.empty()` |
| `MonoEmpty` | 0 | 是 (ScalarCallable) | SYNC | 否 | `Mono.empty()` |
| `FluxNever` | 0 (永不完成) | 否 | SYNC | 否 | 测试占位 |
| `FluxError` | 0 (立即错误) | 是 (ScalarCallable) | SYNC | 否 | `Flux.error(ex)` |
| `FluxIterable` | 0~N | 是 (SYNC) | SYNC | 否 | `Flux.fromIterable(list)` |
| `FluxGenerate` | 0~N | 是 (SYNC) | SYNC | 否 | `Flux.generate(sink -> ...)` |
| `FluxCreate` | 0~N | 否 | ASYNC | 是 | `Flux.create(sink -> ...)` |
| `FluxInterval` | 0~∞ | 否 | ASYNC | 是 | `Flux.interval(Duration.ofSeconds(1))` |
| `FluxDefer` | 取决于内部源 | 否 | SYNC | 取决于内部源 | `Flux.defer(() -> ...)` |

### 关键设计决策总结

| 设计决策 | 动机 | 去掉的后果 |
|---|---|---|
| ConditionalSubscriber 分支 | 避免 filter 等操作符的 request 补偿开销 | 每次过滤掉的值都需要额外 request(1) 往返 |
| fastPath / slowPath 分离 | 无界请求时跳过 CAS | 大数组/大范围的发射性能下降 |
| ScalarCallable 接口 | 单值源跳过整个订阅链 | `Mono.just(x).map(f)` 创建不必要的 Subscription 对象 |
| BaseSink 的 requested 高位标记 | 节省一个 volatile 字段 | 需要额外字段和内存屏障 |
| FluxGenerate 的 "one next" 约束 | 保证背压正确性 | 一次 generator 调用可能产生多个值，背压失效 |
| FluxDefer 的懒加载 | 每次订阅创建新 Publisher | 无法实现正确的重试/重订阅 |
| FluxInterval 的 ERROR 策略 | 过期 tick 无意义 | 缓冲无用的时间戳值，队列无限增长 |
| SerializedFluxSink 的 MPSC 队列 | 避免自旋等待 | 高并发下 CPU 浪费和延迟突增 |
