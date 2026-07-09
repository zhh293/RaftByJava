# Flux 与 Mono 的类体系与抽象层次源码解析

> **Reactor Core 源码深度研究系列 · 第 01 篇**
>
> 本篇从 `org.reactivestreams.Publisher` 出发，逐层剖析 Reactor Core 的类继承体系。以真实源码为证据，揭示每一层抽象的设计动机——不仅告诉你"是什么"，还要回答"为什么这样设计"以及"去掉这一层会怎样"。

---

## 一、全局类层次总览

在进入细节之前，先建立一张完整的 ASCII 继承图。后续每一节都会回到这张图上定位讨论的焦点。

```
                          «interface»
                     org.reactivestreams.Publisher<T>
                               │
                               ▼
                       «interface»
                    CorePublisher<T>              ← 增加 subscribe(CoreSubscriber) 方法
                     ┌─────┴───────────────────────────┐
                     │                                 │
              «abstract class»                  «abstract class»
                 Flux<T>                           Mono<T>                ← 0..N  vs  0..1
                     │                                 │
            ┌────────┼──────────┐             ┌────────┴────────┐
            │        │          │             │                 │
   ConnectableFlux<T>│   «abstract class»  «abstract class»    │
            │        │  FluxOperator<I,O>  MonoOperator<I,O>    │
            │        │          │             │                 │
            │        │   «abstract class»  «abstract class»    │
            │        │  InternalFluxOp<I,O> InternalMonoOp<I,O> │
            │        │      │                   │               │
            │        │  FluxMap, FluxFilter  MonoMap, MonoFilter │
            │        │  FluxFlatMap, ...     MonoFlatMap, ...    │
            │        │                                          │
            │   ParallelFlux<T> (implements CorePublisher<T>)   │
            │                                                   │
            └───────────────────────────────────────────────────┘

   «interface»                          «interface»
   SourceProducer<O>                    OptimizableOperator<IN, OUT>
   (Scannable + Publisher)              (CorePublisher + 优化循环契约)
        │                                    ▲
        │                                    │
  FluxArray, FluxRange,             InternalFluxOperator, InternalMonoOperator
  FluxEmpty, FluxJust ...           (同时继承 FluxOperator/MonoOperator)
```

这张图的核心信息：

1. Reactor 在 Reactive Streams 标准的 `Publisher` / `Subscriber` 之上分别增加了 `CorePublisher` / `CoreSubscriber` 扩展层。
2. `Flux` 和 `Mono` 是两条平行的抽象主干，分别代表 0..N 和 0..1 语义。
3. 操作符分为"公开基类"（`FluxOperator` / `MonoOperator`）和"内部优化基类"（`InternalFluxOperator` / `InternalMonoOperator`）两层。
4. 源头 Publisher（如 `FluxArray`、`FluxRange`）通过 `SourceProducer` 接口标记自身为链的起点。

---

## 二、从 Publisher 到 CorePublisher：第一层扩展

### 2.1 org.reactivestreams.Publisher 回顾

Reactive Streams 规范只定义了一个方法：

```java
public interface Publisher<T> {
    void subscribe(Subscriber<? super T> s);
}
```

这是跨框架互操作的最小公约数。RxJava、Reactor、Akka Streams 都实现这个接口，因此任何一个框架的 `Publisher` 可以被另一个框架消费。

### 2.2 CorePublisher 多了什么

源码位置：`reactor/core/CorePublisher.java`

```java
public interface CorePublisher<T> extends Publisher<T> {

    /**
     * An internal {@link Publisher#subscribe(Subscriber)} that will bypass
     * {@link Hooks#onLastOperator(Function)} pointcut.
     * <p>
     * In addition to behave as expected by {@link Publisher#subscribe(Subscriber)}
     * in a controlled manner, it supports direct subscribe-time {@link Context} passing.
     */
    void subscribe(CoreSubscriber<? super T> subscriber);
}
```

**关键差异：参数类型从 `Subscriber` 变成了 `CoreSubscriber`。**

这意味着什么？

1. **绕过 `Hooks.onLastOperator` 拦截点**。公开的 `subscribe(Subscriber)` 会经过 `Hooks.onLastOperator` 注册的全局钩子，而内部的 `subscribe(CoreSubscriber)` 不走这条路——这是"快车道"。
2. **支持 Context 传递**。`CoreSubscriber` 自带 `currentContext()` 方法（下一节详述），而原始 `Subscriber` 没有。

**为什么不直接在 `Publisher` 上加这个方法？** 因为 `Publisher` 是 Reactive Streams 规范定义的，属于跨框架契约，不能随意修改。Reactor 需要在不破坏互操作性的前提下增加自己的扩展能力，所以通过继承加了一层。

**去掉 `CorePublisher` 会怎样？** 所有内部操作符在订阅时都必须走公开的 `subscribe(Subscriber)` 路径，每次都经过 `Hooks.onLastOperator` 拦截和 `Operators.toCoreSubscriber()` 转换。对于一条 10 个操作符的链，意味着 10 次不必要的拦截检查——这在高性能场景下是不可接受的开销。

---

## 三、从 Subscriber 到 CoreSubscriber：规则放宽与 Context 注入

### 3.1 CoreSubscriber 的完整定义

源码位置：`reactor/core/CoreSubscriber.java`

```java
/**
 * A {@link Context} aware subscriber which has relaxed rules for §1.3 and §3.9
 * compared to the original {@link org.reactivestreams.Subscriber} from Reactive Streams.
 * If an invalid request {@code <= 0} is done on the received subscription, the request
 * will not produce an onError and will simply be ignored.
 */
public interface CoreSubscriber<T> extends Subscriber<T> {

    default Context currentContext(){
        return Context.empty();
    }

    @Override
    void onSubscribe(Subscription s);
}
```

### 3.2 多了什么

**第一，`currentContext()` 方法。** 这是 Reactor 的 Context 传播机制的入口。Context 是一个不可变的键值对容器，沿着订阅链从下游向上游传播。每个操作符的内部 Subscriber 实现会覆盖 `currentContext()` 方法，将自己持有的 Context 与下游的 Context 合并。

**第二，放宽了 Reactive Streams 规范的两条规则：**

| 规则编号 | 原始规范要求 | CoreSubscriber 的放宽 |
|---------|------------|---------------------|
| §1.3 | `onSubscribe` 调用 `request(n)` 时 n <= 0 必须触发 `onError(IllegalArgumentException)` | 无效请求 (n <= 0) 被静默忽略或通过 Logger 报告，不触发 `onError` |
| §3.9 | `Subscriber.onSubscribe` 必须在接收到 `Subscription` 后才调用 `request` | 内部操作符之间信任彼此的调用顺序，不做严格检查 |

**为什么要放宽？** 这两条规则是为了保护不信任的跨框架交互。但在 Reactor 内部，操作符之间是同一个框架的代码，天然可信。如果每个中间操作符都严格执行这些检查，会增加大量分支判断。`CoreSubscriber` 的存在让内部路径免于这些开销，而外部用户传入的 `Subscriber` 仍然通过 `Operators.toCoreSubscriber()` 方法被包装成严格合规的版本。

**去掉 `CoreSubscriber` 会怎样？** 

1. Context 传播无处安放。Reactive Streams 的 `Subscriber` 没有任何扩展点可以承载 Context，必须通过额外的包装类或 ThreadLocal 来传递——前者增加内存分配，后者在异步场景下不可靠。
2. 所有操作符都必须执行严格的规范检查，即使已知上游是 Reactor 内部的操作符。

### 3.3 多角度对比：CoreSubscriber vs Subscriber

从**安全性**角度：`CoreSubscriber` 牺牲了边界检查，换取了内部性能。这种 trade-off 是合理的，因为外部入口点（`Flux.subscribe(Subscriber)`）仍然通过 `Operators.toCoreSubscriber` 做了严格包装。

从**功能性**角度：`currentContext()` 让 Reactor 实现了无需 ThreadLocal 的上下文传播，这是 Reactor 对比 RxJava 2.x 的一个重要优势。RxJava 2.x 没有类似机制，必须通过 `subscribeWith` 手动传递状态。

从**兼容性**角度：`CoreSubscriber` 继承自 `Subscriber`，所以一个 `CoreSubscriber` 可以被任何遵循 Reactive Streams 规范的 `Publisher` 消费。反过来，一个普通的 `Subscriber` 可以通过 `Operators.toCoreSubscriber()` 被安全地提升为 `CoreSubscriber`。

---

## 四、Flux 与 Mono：两条平行的主干

### 4.1 Flux 的声明

源码位置：`reactor/core/publisher/Flux.java`（第 126 行）

```java
public abstract class Flux<T> implements CorePublisher<T> {
    // ... 数千行的操作符方法
}
```

Javadoc 明确指出：

> A Reactive Streams Publisher with rx operators that emits **0 to N** elements, and then completes (successfully or with an error).

### 4.2 Mono 的声明

源码位置：`reactor/core/publisher/Mono.java`（第 121 行）

```java
public abstract class Mono<T> implements CorePublisher<T> {
    // ... 数千行的操作符方法
}
```

Javadoc 明确指出：

> A Reactive Streams Publisher with basic rx operators that emits **at most one item** via the onNext signal then terminates with an onComplete signal (successful Mono, with or without value), or only emits a single onError signal (failed Mono).

### 4.3 本质区别：0..N vs 0..1

Flux 和 Mono **在运行时并没有强制限制元素个数**——Mono 不会在发出第一个元素后自动取消订阅。区别是**语义契约**：

- `Mono` 告诉调用方"你最多会收到一个值"，这让许多优化成为可能。例如 `Mono.flatMap` 返回的仍是 `Mono`（而不是 `Flux`），因为 1 -> 1 的映射最多产生 1 个值。
- `Flux` 没有这个约束，所以 `Flux.flatMap` 返回的是 `Flux`。

**为什么需要两个独立的类而不是一个通用的 Publisher？**

这是一个类型系统层面的设计决策。如果只有 `Flux`，那么一个返回"最多一个用户"的方法签名是 `Flux<User>`，调用方无法通过类型信息推断出这个流的基数。有了 `Mono<User>`，类型签名本身就是文档——它传达了"这是一个异步的 Optional"这一语义。

**从操作符组合的角度看**，Mono 提供了不同于 Flux 的操作符集合。例如：
- `Mono` 有 `zipWith`（两个 Mono 的值组合成 Tuple2），而 `Flux.zipWith` 是按元素逐对匹配。
- `Mono` 有 `flatMapMany`（Mono 转 Flux），这在 Flux 上不存在。
- `Mono` 没有 `buffer`、`window` 这些流分片操作符，因为对单个值做分片没有意义。

### 4.4 ConnectableFlux 和 ParallelFlux：特化的 Flux 变体

**ConnectableFlux** 继承自 `Flux`，增加了"连接"语义——订阅者先堆积，直到显式调用 `connect()` 才开始数据流。

源码位置：`reactor/core/publisher/ConnectableFlux.java`（第 35 行）

```java
public abstract class ConnectableFlux<T> extends Flux<T> {
    public abstract void connect(Consumer<? super Disposable> cancelSupport);
    public final Flux<T> autoConnect(int minSubscribers, Consumer<? super Disposable> cancelSupport) { ... }
    public final Flux<T> refCount(int minSubscribers) { ... }
}
```

ConnectableFlux 的关键方法是 `connect(Consumer)` 和 `autoConnect`/`refCount`。`autoConnect` 在达到指定订阅者数量时自动连接；`refCount` 则额外跟踪引用计数，在所有订阅者取消后断开连接。

**ParallelFlux** 没有继承 `Flux`，而是直接实现 `CorePublisher`：

源码位置：`reactor/core/publisher/ParallelFlux.java`（第 77 行）

```java
public abstract class ParallelFlux<T> implements CorePublisher<T> {
    // 并行"轨道"上的操作符
}
```

**为什么 ParallelFlux 不继承 Flux？** 因为 `Flux` 的操作符假设数据是在单一流上的线性序列，而 `ParallelFlux` 将数据分到多条"轨道"上并行处理。如果继承 `Flux`，用户会误以为 `Flux` 上的所有操作符都适用于并行场景，而实际上 `buffer`、`window` 等操作符在并行轨道上的语义完全不同。

---

## 五、FluxOperator 与 MonoOperator：公开的操作符基类

### 5.1 FluxOperator

源码位置：`reactor/core/publisher/FluxOperator.java`

```java
public abstract class FluxOperator<I, O> extends Flux<O> implements Scannable {

    protected final Flux<? extends I> source;

    protected FluxOperator(Flux<? extends I> source) {
        this.source = Objects.requireNonNull(source);
    }

    @Override
    public @Nullable Object scanUnsafe(Attr key) {
        if (key == Attr.PREFETCH) return getPrefetch();
        if (key == Attr.PARENT) return source;
        if (key == InternalProducerAttr.INSTANCE) return false; // public class!
        return null;
    }
}
```

### 5.2 MonoOperator

源码位置：`reactor/core/publisher/MonoOperator.java`

```java
public abstract class MonoOperator<I, O> extends Mono<O> implements Scannable {

    protected final Mono<? extends I> source;

    protected MonoOperator(Mono<? extends I> source) {
        this.source = Objects.requireNonNull(source);
    }

    @Override
    public @Nullable Object scanUnsafe(Attr key) {
        if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
        if (key == Attr.PARENT) return source;
        if (key == InternalProducerAttr.INSTANCE) return false; // public class!
        return null;
    }
}
```

### 5.3 source 字段的设计意义

两个类的核心都是一个 `source` 字段。这个字段保存着上游 Publisher 的引用，构成了装饰器模式的链条。

当用户写 `flux.map(f).filter(p).take(10)` 时，实际上创建了一条由 `source` 指针串起来的链：

```
FluxTake.source -> FluxFilter.source -> FluxMap.source -> 原始 Flux
```

**注意 `source` 的类型约束：**
- `FluxOperator.source` 的类型是 `Flux<? extends I>`——上游必须是 `Flux`。
- `MonoOperator.source` 的类型是 `Mono<? extends I>`——上游必须是 `Mono`。

这个类型约束保证了 `Flux` 操作符链上不会混入 `Mono`，反之亦然。如果需要跨越边界（例如 `Mono.flux()` 或 `Flux.single()`），会通过专门的桥接操作符处理。

### 5.4 InternalProducerAttr.INSTANCE 标记

两个类的 `scanUnsafe` 方法都对 `InternalProducerAttr.INSTANCE` 返回 `false`。这个标记用于区分"内部类"和"公开类"。`FluxOperator` 和 `MonoOperator` 是 `public` 的，第三方库可以继承它们来创建自定义操作符，因此标记为非内部。

**这个标记在哪里用？** 在 `Operators.restoreContextOnSubscriberIfPublisherNonInternal()` 方法中。当订阅链遇到非内部的 Publisher 时，需要额外的 Context 恢复逻辑，因为外部 Publisher 可能没有正确传播 Context。

---

## 六、InternalFluxOperator 与 InternalMonoOperator：优化循环的核心

### 6.1 InternalFluxOperator

源码位置：`reactor/core/publisher/InternalFluxOperator.java`

```java
abstract class InternalFluxOperator<I, O> extends FluxOperator<I, O> implements Scannable,
                                                                                OptimizableOperator<O, I> {

    final @Nullable OptimizableOperator<?, I> optimizableOperator;

    protected InternalFluxOperator(Flux<? extends I> source) {
        super(source);
        if (source instanceof OptimizableOperator) {
            @SuppressWarnings("unchecked")
            OptimizableOperator<?, I> optimSource = (OptimizableOperator<?, I>) source;
            this.optimizableOperator = optimSource;
        }
        else {
            this.optimizableOperator = null;
        }
    }
```

注意构造器中的 `instanceof` 检查：如果上游 `source` 也实现了 `OptimizableOperator` 接口，就把它缓存到 `optimizableOperator` 字段。这为后续的优化循环提供了"链表指针"。

### 6.2 subscribe 方法中的 while(true) 优化循环

这是整个 Reactor 订阅路径中最关键的优化，值得逐行分析：

源码位置：`reactor/core/publisher/InternalFluxOperator.java`（第 48-77 行）

```java
@Override
@SuppressWarnings("unchecked")
public final void subscribe(CoreSubscriber<? super O> subscriber) {
    OptimizableOperator operator = this;
    try {
        while (true) {
            subscriber = operator.subscribeOrReturn(subscriber);
            if (subscriber == null) {
                // null means "I will subscribe myself", returning...
                return;
            }
            OptimizableOperator newSource = operator.nextOptimizableSource();
            if (newSource == null) {
                CorePublisher operatorSource = operator.source();
                subscriber = Operators.restoreContextOnSubscriberIfPublisherNonInternal(
                    operatorSource, subscriber);
                operatorSource.subscribe(subscriber);
                return;
            }
            operator = newSource;
        }
    }
    catch (Throwable e) {
        Operators.reportThrowInSubscribe(subscriber, e);
        return;
    }
}
```

**这段代码在做什么？** 它把原本递归的订阅过程转换成了迭代。

**如果没有这个优化会怎样？** 考虑一条 100 个操作符的链。传统递归方式下，`subscribe` 调用会产生 100 层的方法栈：

```
op100.subscribe(subscriber)
  -> op99.subscribe(wrappedSubscriber)
    -> op98.subscribe(wrappedSubscriber)
      -> ... (100 层递归)
```

对于深度嵌套的操作符链，这可能导致 `StackOverflowError`。while(true) 循环将递归展开为迭代：

```
while(true) {
  subscriber = op100.subscribeOrReturn(subscriber);  // 创建包装 Subscriber，但不递归
  operator = op99;
  subscriber = op99.subscribeOrReturn(subscriber);
  operator = op98;
  // ... 在同一个栈帧里完成所有操作符的订阅
}
```

**循环的退出条件有两个：**

1. `subscribeOrReturn` 返回 `null`——操作符自己处理了订阅（例如某些操作符需要特殊的订阅逻辑），循环退出。
2. `nextOptimizableSource()` 返回 `null`——已经到达链的非优化源头（如 `FluxArray`），直接调用 `source.subscribe(subscriber)` 退出。

### 6.3 InternalMonoOperator 的几乎相同实现

源码位置：`reactor/core/publisher/InternalMonoOperator.java`

```java
abstract class InternalMonoOperator<I, O> extends MonoOperator<I, O> implements Scannable,
                                                                                OptimizableOperator<O, I> {

    final @Nullable OptimizableOperator<?, I> optimizableOperator;

    protected InternalMonoOperator(Mono<? extends I> source) {
        super(source);
        if (source instanceof OptimizableOperator) {
            @SuppressWarnings("unchecked")
            OptimizableOperator<?, I> optimSource = (OptimizableOperator<?, I>) source;
            this.optimizableOperator = optimSource;
        }
        else {
            this.optimizableOperator = null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void subscribe(CoreSubscriber<? super O> subscriber) {
        OptimizableOperator operator = this;
        try {
            while (true) {
                subscriber = operator.subscribeOrReturn(subscriber);
                if (subscriber == null) {
                    return;
                }
                OptimizableOperator newSource = operator.nextOptimizableSource();
                if (newSource == null) {
                    CorePublisher operatorSource = operator.source();
                    subscriber = Operators.restoreContextOnSubscriberIfPublisherNonInternal(
                        operatorSource, subscriber);
                    operatorSource.subscribe(subscriber);
                    return;
                }
                operator = newSource;
            }
        }
        catch (Throwable e) {
            Operators.reportThrowInSubscribe(subscriber, e);
            return;
        }
    }
}
```

`InternalMonoOperator` 和 `InternalFluxOperator` 的 `subscribe` 方法完全相同。这是代码复制而非提取到公共基类的结果——因为 Java 的单继承限制，`InternalFluxOperator` 必须继承 `FluxOperator`，`InternalMonoOperator` 必须继承 `MonoOperator`，无法再共享一个公共父类。

### 6.4 InternalProducerAttr.INSTANCE = true

与 `FluxOperator` 不同，`InternalFluxOperator` 的 `scanUnsafe` 对 `InternalProducerAttr.INSTANCE` 返回 `true`：

```java
@Override
public @Nullable Object scanUnsafe(Attr key) {
    if (key == Attr.PREFETCH) return getPrefetch();
    if (key == Attr.PARENT) return source;
    if (key == InternalProducerAttr.INSTANCE) return true;
    return super.scanUnsafe(key);
}
```

这意味着 Reactor 知道这是一个内部操作符，可以信任它正确传播了 Context，不需要额外的 Context 恢复逻辑。

---

## 七、SourceProducer：链的起点标记

源码位置：`reactor/core/publisher/SourceProducer.java`

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

`SourceProducer` 是一个标记接口，实现了 `Scannable` 和 `Publisher`。典型的实现者包括 `FluxArray`、`FluxRange`、`FluxJust`、`FluxEmpty` 等——它们是数据的源头，没有上游 `source`，所以 `Attr.PARENT` 返回 `null`。

**设计意义：** `SourceProducer` 让扫描工具（如调试时的操作符链打印）能够识别出链的起点，不再往上追溯。`stepName()` 的默认实现返回 `"source(FluxArray)"` 这样的可读字符串，用于调试输出。

**为什么不让 FluxArray 等直接实现 Scannable？** 因为 `SourceProducer` 提供了统一的 `scanUnsafe` 默认实现——`PARENT` 返回 `null`，`ACTUAL` 返回 `null`，`InternalProducerAttr` 返回 `true`。如果每个源头操作符都自己写这些逻辑，会导致大量重复代码。

---

## 八、OptimizableOperator 接口：优化循环的契约

源码位置：`reactor/core/publisher/OptimizableOperator.java`

```java
interface OptimizableOperator<IN, OUT> extends CorePublisher<IN> {

    @Nullable CoreSubscriber<? super OUT> subscribeOrReturn(
        CoreSubscriber<? super IN> actual) throws Throwable;

    CorePublisher<? extends OUT> source();

    @Nullable OptimizableOperator<?, ? extends OUT> nextOptimizableSource();
}
```

这个接口定义了三个方法，它们的协作关系如下：

| 方法 | 职责 | 返回值含义 |
|-----|------|---------|
| `subscribeOrReturn(actual)` | 创建内部 Subscriber 包装 `actual` | 非 null = 返回上游的 Subscriber 让循环继续；null = 已自行订阅 |
| `source()` | 获取原始上游 Publisher | 当 `nextOptimizableSource` 返回 null 时，用这个源头做最终订阅 |
| `nextOptimizableSource()` | 获取上游中最近的 OptimizableOperator | 非 null = 循环继续；null = 到达非优化边界 |

`nextOptimizableSource()` 的实现就是返回构造器中缓存的 `optimizableOperator` 字段。这意味着优化循环只能沿着连续的 `OptimizableOperator` 链前进。一旦遇到非 `OptimizableOperator` 的源头（如第三方实现的 `Publisher`），循环就停止并回退到传统的递归订阅。

---

## 九、Flux.subscribe(Subscriber) 中的优化循环入口

除了 `InternalFluxOperator.subscribe(CoreSubscriber)` 之外，`Flux` 的公开 `subscribe(Subscriber)` 方法中也包含了一个类似的优化循环：

源码位置：`reactor/core/publisher/Flux.java`（第 8861-8894 行）

```java
@Override
@SuppressWarnings("unchecked")
public final void subscribe(Subscriber<? super T> actual) {
    CorePublisher publisher = Operators.onLastAssembly(this);
    CoreSubscriber subscriber = Operators.toCoreSubscriber(actual);

    // Fuseable 兼容性检查 ...

    try {
        if (publisher instanceof OptimizableOperator) {
            OptimizableOperator operator = (OptimizableOperator) publisher;
            while (true) {
                subscriber = operator.subscribeOrReturn(subscriber);
                if (subscriber == null) {
                    return;
                }
                OptimizableOperator newSource = operator.nextOptimizableSource();
                if (newSource == null) {
                    publisher = operator.source();
                    break;
                }
                operator = newSource;
            }
        }

        subscriber = Operators.restoreContextOnSubscriberIfPublisherNonInternal(
            publisher, subscriber);
        publisher.subscribe(subscriber);
    }
    catch (Throwable e) {
        Operators.reportThrowInSubscribe(subscriber, e);
        return;
    }
}
```

**为什么公开方法中也需要这个循环？** 因为用户调用 `flux.subscribe(mySubscriber)` 时，入口是公开的 `subscribe(Subscriber)` 方法。如果这里不做优化循环，那么第一次从公开 `subscribe` 调用到 `InternalFluxOperator.subscribe(CoreSubscriber)` 时才开始优化，但公开方法中的 `Hooks.onLastOperator` 等处理会导致这次调用的入口 Publisher 已经被 `Operators.onLastAssembly` 替换过——优化循环的起点可能不同。

**两处循环的区别：**
- 公开 `subscribe` 会先调用 `Operators.onLastAssembly(this)` 和 `Operators.toCoreSubscriber(actual)`，然后再进入循环。
- 内部 `subscribe(CoreSubscriber)` 直接进入循环，跳过了这些步骤。

---

## 十、从 FluxOperator 到 InternalFluxOperator：为什么要分两层？

这是一个重要的架构决策，值得从多个角度分析。

**从 API 可见性角度：**
- `FluxOperator` 是 `public` 的，第三方库（如 `reactor-extra`、`reactor-netty`）可以继承它来创建自定义操作符。
- `InternalFluxOperator` 是包私有的（`abstract class`，无 `public` 修饰符），只能被 Reactor Core 内部的操作符使用。

**从优化参与度角度：**
- 继承 `FluxOperator` 的第三方操作符不参与优化循环，它们的 `subscribe` 方法走传统递归路径。
- 继承 `InternalFluxOperator` 的内部操作符实现了 `OptimizableOperator` 接口，参与优化循环。

**从安全性角度：**
- 第三方操作符不能保证正确实现 `subscribeOrReturn` 的契约（返回 null 或非 null 的语义），如果允许它们参与优化循环，可能导致不可预期的行为。
- 内部操作符由 Reactor 团队维护，可以保证契约的正确性。

**去掉 FluxOperator 只保留 InternalFluxOperator 会怎样？** 第三方库无法以标准方式创建自定义操作符——它们必须直接继承 `Flux`，失去了 `source` 字段和 `Scannable` 的默认实现。

**去掉 InternalFluxOperator 只保留 FluxOperator 会怎样？** 所有操作符都走传统递归路径，deep 操作符链可能爆栈，每次订阅都有不必要的栈帧开销。

---

## 十一、归纳表格：类层次对照表

| 类/接口 | 包可见性 | 继承自 | 关键字段 | 核心方法 | 设计目的 |
|---------|---------|-------|---------|---------|---------|
| `Publisher<T>` | 公开 (RS 规范) | - | - | `subscribe(Subscriber)` | 跨框架互操作的最小契约 |
| `CorePublisher<T>` | 公开 | `Publisher` | - | `subscribe(CoreSubscriber)` | 绕过 Hooks 拦截点，支持 Context 传递 |
| `CoreSubscriber<T>` | 公开 | `Subscriber` | - | `currentContext()` | Context 传播，放宽 RS 规则 §1.3 和 §3.9 |
| `Flux<T>` | 公开抽象类 | `CorePublisher` | - | 数千个操作符方法 | 0..N 元素序列的主干类 |
| `Mono<T>` | 公开抽象类 | `CorePublisher` | - | 数千个操作符方法 | 0..1 元素序列的主干类 |
| `ConnectableFlux<T>` | 公开抽象类 | `Flux` | - | `connect(Consumer)` | 热源：订阅者堆积后连接 |
| `ParallelFlux<T>` | 公开抽象类 | `CorePublisher` | - | `runOn(Scheduler)`, `sequential()` | 多轨道并行处理 |
| `FluxOperator<I,O>` | 公开抽象类 | `Flux<O>`, `Scannable` | `source: Flux<I>` | `scanUnsafe(Attr)` | 公开的操作符基类，第三方可继承 |
| `MonoOperator<I,O>` | 公开抽象类 | `Mono<O>`, `Scannable` | `source: Mono<I>` | `scanUnsafe(Attr)` | 公开的操作符基类，第三方可继承 |
| `InternalFluxOperator<I,O>` | 包私有 | `FluxOperator`, `OptimizableOperator` | `optimizableOperator` | `subscribe(CoreSubscriber)` (while 循环), `subscribeOrReturn(CoreSubscriber)` | 内部操作符基类，参与优化循环 |
| `InternalMonoOperator<I,O>` | 包私有 | `MonoOperator`, `OptimizableOperator` | `optimizableOperator` | `subscribe(CoreSubscriber)` (while 循环), `subscribeOrReturn(CoreSubscriber)` | 内部操作符基类，参与优化循环 |
| `OptimizableOperator<IN,OUT>` | 包私有接口 | `CorePublisher` | - | `subscribeOrReturn`, `source`, `nextOptimizableSource` | 定义优化循环的三方法契约 |
| `SourceProducer<O>` | 包私有接口 | `Scannable`, `Publisher` | - | `scanUnsafe(Attr)`, `stepName()` | 标记链的源头，提供默认扫描实现 |

---

## 十二、总结

Reactor Core 的类体系设计遵循了几个一以贯之的原则：

1. **在规范之上做扩展，不修改规范。** `CorePublisher` 和 `CoreSubscriber` 是 Reactive Streams 规范之上的扩展层，保持了向后兼容性。

2. **通过类型系统表达语义。** `Flux` 和 `Mono` 的分离不是运行时强制的，而是类型系统层面的契约。这让编译器帮助开发者避免语义错误。

3. **内部优化与外部契约分离。** `FluxOperator`（公开）和 `InternalFluxOperator`（内部）的分层让第三方可以安全地扩展框架，同时让框架内部可以执行激进的优化。

4. **用循环替代递归。** `OptimizableOperator` 的三方法契约是将递归订阅展开为迭代的关键抽象，它解决了深度操作符链的栈溢出风险和性能问题。

5. **标记接口辅助运行时决策。** `SourceProducer`、`InternalProducerAttr` 等标记让框架能在运行时区分内部/外部、源头/中间操作符，从而做出不同的优化决策。
