# Reactor 整体架构与响应式编程设计哲学

> **Reactor Core 源码深度研究系列 · 第 17 篇**
>
> 作为系列的总结篇，本文从前 16 篇源码分析中提炼 Reactor Core 的全景架构，深入探讨其四大设计哲学、五大设计模式、六大性能优化策略，并与 RxJava、Mutiny 等响应式框架进行横向对比，最终以两张归纳表格收束全篇。

---

## 一、Reactor Core 全景架构图

```
                    Reactor Core 全景架构
═══════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                      用户 API 层 (reactor.core.publisher)            │
  │                                                                     │
  │   Flux<T>                        Mono<T>                            │
  │   ├── 静态工厂方法                  ├── 静态工厂方法                    │
  │   │   (just, range, fromIterable    │   (just, fromCallable,          │
  │   │    empty, error, create...)     │    empty, error, create...)     │
  │   ├── 操作符方法                    ├── 操作符方法                      │
  │   │   (map, filter, flatMap,         │   (map, flatMap, then,          │
  │   │    concat, merge, zip...)        │    zip, when, timeout...)       │
  │   └── 装配时入口                     └── 装配时入口                     │
  │       onAssembly()                     onAssembly()                 │
  │                                                                     │
  │   ParallelFlux<T>               ConnectableFlux<T>                 │
  │   (并行处理)                      (热发布者/多播)                      │
  └───────────────────────────┬─────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    操作符层 (reactor.core.publisher)                  │
  │                                                                     │
  │  ┌─────────────────────────────────────────────────────────────┐   │
  │  │              操作符基类                                       │   │
  │  │  FluxOperator<I,O>  ────►  InternalFluxOperator<I,O>        │   │
  │  │  MonoOperator<I,O>  ────►  InternalMonoOperator<I,O>        │   │
  │  │                    实现 OptimizableOperator 接口              │   │
  │  └─────────────────────────────────────────────────────────────┘   │
  │                                                                     │
  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────────┐  │
  │  │ FluxMap    │ │ FluxFilter │ │ FluxFlatMap│ │ FluxOnAssembly │  │
  │  │ FluxMapFuseable│ FluxFilterFuseable│       │ │ MonoOnAssembly │  │
  │  ├────────────┤ ├────────────┤ ├────────────┤ ├────────────────┤  │
  │  │ FluxLift   │ │ FluxPeek   │ │ FluxPublish│ │ FluxSubscribeOn│  │
  │  │ MonoLift   │ │ MonoPeek   │ │ FluxBuffer │ │ FluxPublishOn  │  │
  │  ├────────────┤ ├────────────┤ ├────────────┤ ├────────────────┤  │
  │  │ FluxRange  │ │ FluxJust   │ │ FluxEmpty  │ │ FluxError      │  │
  │  │ MonoJust   │ │ MonoEmpty  │ │ FluxCreate │ │ FluxDefer      │  │
  │  └────────────┘ └────────────┘ └────────────┘ └────────────────┘  │
  └───────────────────────────┬─────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                  核心契约层 (reactor.core)                            │
  │                                                                     │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
  │  │ CorePublisher│  │ CoreSubscriber│  │ Fuseable                 │  │
  │  │  extends     │  │  extends     │  │  (队列融合接口)            │  │
  │  │  Publisher   │  │  Subscriber  │  │  SYNC/ASYNC/NONE 模式     │  │
  │  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
  │                                                                     │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
  │  │ Scannable    │  | Disposable   │  │ ConditionalSubscriber    │  │
  │  │ (元数据查询)  │  │ (资源释放)    │  │ (tryOnNext 优化)         │  │
  │  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
  │                                                                     │
  │  ┌──────────────────────────────────────────────────────────────┐  │
  │  │ Operators (工具类)                                            │  │
  │  │  ├── lift() / LiftFunction (自定义操作符)                      │  │
  │  │  ├── restoreContextOnSubscriberIfPublisherNonInternal()       │  │
  │  │  ├── reportThrowInSubscribe() / onOperatorError()             │  │
  │  │  ├── addCap() / produced() (CAS 需求量管理)                   │  │
  │  │  ├── complete() / cancelledSubscription()                     │  │
  │  │  └── onNextError() (错误策略)                                 │  │
  │  └──────────────────────────────────────────────────────────────┘  │
  └───────────────────────────┬─────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │              调度与上下文层 (reactor.core.scheduler / reactor.util)  │
  │                                                                     │
  │  ┌─────────────────────────────────────────────────────────────┐   │
  │  │                    Schedulers                                │   │
  │  │  ├── parallel()    (CPU 密集型, 线程数 = CPU核心数)             │   │
  │  │  ├── boundedElastic() (I/O 密集型, 有界弹性线程池)              │   │
  │  │  ├── single()      (单线程)                                   │   │
  │  │  ├── immediate()   (当前线程)                                 │   │
  │  │  └── fromExecutor() (自定义线程池)                             │   │
  │  └─────────────────────────────────────────────────────────────┘   │
  │                                                                     │
  │  ┌─────────────────────────────────────────────────────────────┐   │
  │  │                    Context / ContextView                     │   │
  │  │  Context0 (空, 单例) → Context1 → Context2 → ContextN        │   │
  │  │  不可变, 从下游向上游传播                                       │   │
  │  └─────────────────────────────────────────────────────────────┘   │
  │                                                                     │
  │  ┌─────────────────────────────────────────────────────────────┐   │
  │  │                    Hooks (全局钩子)                           │   │
  │  │  ├── onEachOperator (每个操作符装配时)                         │   │
  │  │  ├── onLastOperator (最后一个操作符装配时)                     │   │
  │  │  ├── onOperatorError (操作符错误映射)                          │   │
  │  │  ├── onNextError (onNext 错误策略)                            │   │
  │  │  └── onOperatorDebug (全局堆栈跟踪)                           │   │
  │  └─────────────────────────────────────────────────────────────┘   │
  └───────────────────────────┬─────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │              基础设施层 (reactor.util / reactor.core.publisher)      │
  │                                                                     │
  │  Exceptions        (复合异常、传播异常工具)                            │
  │  Queues            (无锁队列: MpscArrayQueue, SpscArrayQueue...)    │
  │  Traces            (堆栈清理、调用点提取)                              │
  │  Loggers           (SLF4J 适配)                                     │
  │  ContextPropagation (ThreadLocal 桥接)                               │
  └─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │              Reactive Streams 规范 (org.reactivestreams)             │
  │                                                                     │
  │  Publisher<T>  ←→  Subscriber<T>  ←→  Subscription                  │
  │                                                                     │
  │  规范规则:                                                           │
  │  §1.0-1.9  Publisher.subscribe 语义                                  │
  │  §2.0-2.13 Subscriber.onSubscribe/onNext/onError/onComplete 语义     │
  │  §3.0-3.4  Subscription.request/cancel 语义                          │
  │  §4.0      Publisher/Subscriber/Subscription 契约可被静态检查         │
  └─────────────────────────────────────────────────────────────────────┘
```

这张全景图展示了 Reactor Core 从底层 Reactive Streams 规范到上层用户 API 的完整分层结构。每一层都严格定义了职责边界：规范层定义契约，核心层定义抽象，操作符层提供具体实现，API 层提供用户友好的接口。

---

## 二、Reactor 的四大设计哲学

### 2.1 声明式编程：先组装管道，再触发执行

**核心思想：** 代码描述的是"要做什么"（What），而非"怎么做"（How）。操作符链的构建（装配时）与执行（执行时）严格分离。

在第 15 篇中我们深入分析了这一机制：

```java
// 装配时：只构建操作符链，不执行任何用户代码
Flux<String> pipeline = Flux.range(1, 100)
    .map(i -> "item-" + i)
    .filter(s -> s.length() > 5)
    .flatMap(s -> Mono.fromCallable(() -> queryDatabase(s)));

// 执行时：subscribe 触发数据流动
pipeline.subscribe(System.out::println);
```

装配时，每个操作符的构造器只做两件事——存 source 引用、存操作参数。`Flux.onAssembly()` 方法在此阶段触发 `Hooks.onEachOperatorHook`，但不执行任何数据处理。执行时，`InternalFluxOperator.subscribe()` 通过 `OptimizableOperator` 的优化循环创建 Subscriber 链，最终订阅数据源。

**为什么选择声明式而非命令式？** 如果在 `map(fn)` 调用时就执行 `fn`，会导致副作用过早执行、无法处理异步源、背压失效、多次订阅结果不一致等问题（详见第 15 篇反例分析）。声明式设计确保了操作符链是一个纯粹的"数据管道蓝图"，可以被多次订阅、在不同线程上执行、支持背压控制。

### 2.2 背压驱动：消费者控制生产者速率

**核心思想：** 数据流动的方向是从上游到下游，但控制信号的方向是从下游到上游。消费者通过 `request(n)` 告诉生产者"我能处理多少数据"。

这是 Reactive Streams 规范的核心贡献。在 Reactor 中，`request(n)` 从下游 Subscriber 向上游 Publisher 传播，经过每一层操作符：

```
用户 Subscriber: request(10)
    → FluxFilterSubscriber: request(10) (可能调大以预取)
    → FluxMapSubscriber: request(10)
    → FluxRange: 开始产出 1..10
```

在源码层面，`Operators.addCap()` 使用 `AtomicLongFieldUpdater` 进行 CAS 操作来累加需求量：

```java
// Operators.java 中的 addCap (概念)
static long addCap(long expected, long n) {
    long r = expected + n;
    if (r < 0) {  // 溢出
        return Long.MAX_VALUE;  // 表示无限需求
    }
    return r;
}
```

**如果去掉背压会怎样？** 生产者以最大速度产出数据，消费者来不及处理，数据在中间缓冲区堆积，最终导致 OOM。这正是传统 `Observable`（RxJava 1.x 的非背压版本）和 Java Stream 的局限——它们要么全量缓冲，要么丢弃数据。Reactor 的背压机制确保了内存使用的可预测性。

### 2.3 不可变组合：操作符链不可变，每次操作返回新 Publisher

**核心思想：** 每个 `Flux`/`Mono` 实例一旦创建就不可修改。调用任何操作符方法都会返回一个新的 `Flux`/`Mono` 实例，原实例不受影响。

源码文件：`reactor/core/publisher/FluxOperator.java`

```java
public abstract class FluxOperator<I, O> extends Flux<O> implements Scannable {
    protected final Flux<? extends I> source;

    protected FluxOperator(Flux<? extends I> source) {
        this.source = Objects.requireNonNull(source);
    }
}
```

每个操作符持有上游 `source` 的 `final` 引用，构建完成后不可修改。`map` 操作符创建 `FluxMap` 并通过 `onAssembly()` 返回，原 `Flux` 实例不受任何影响。

**为什么选择不可变？** 
1. **线程安全**：不可变对象天然线程安全，无需同步
2. **可重用**：同一个 `Flux` 实例可以被多次 subscribe，每次创建独立的 Subscriber 链
3. **可组合**：操作符链可以通过变量引用自由组合、分支、合并
4. **可推理**：代码的行为不受执行顺序的副作用影响

### 2.4 函数式风格：操作符借鉴 Stream API

**核心思想：** Reactor 的操作符设计大量借鉴了 Java 8 Stream API 和函数式编程的概念——`map`、`filter`、`reduce`、`flatMap`、`zip` 等。

```java
// Stream API
stream.map(fn).filter(pred).collect(Collectors.toList());

// Reactor (几乎相同的 API)
flux.map(fn).filter(pred).collectList();

// 区别：Stream 是同步拉取，Reactor 是异步推送 + 背压
```

但 Reactor 的函数式风格有两个关键扩展：
1. **异步性**：`flatMap` 中的函数返回 `Publisher`，内部订阅和等待是异步的
2. **背压**：每个操作符都实现了 `request` 传递，确保背压沿链传播

---

## 三、Reactor 与 Reactive Streams 规范的关系

### 3.1 规范的实现

Reactor Core 是 Reactive Streams 规范（`org.reactivestreams` 包）的参考实现之一。规范定义了四个核心接口：

- `Publisher<T>`：数据生产者，提供 `subscribe(Subscriber)` 方法
- `Subscriber<T>`：数据消费者，提供 `onSubscribe`/`onNext`/`onError`/`onComplete` 方法
- `Subscription`：订阅契约，提供 `request(long)`/`cancel()` 方法
- `Processor<T, R>`：既是 Publisher 又是 Subscriber 的中间组件

Reactor 在此基础上扩展了三个核心接口：

- `CorePublisher<T> extends Publisher<T>`：增加了 `subscribe(CoreSubscriber)` 方法，避免装箱
- `CoreSubscriber<T> extends Subscriber<T>`：增加了 `currentContext()` 方法，支持 Context 传播
- `Fuseable`：标记支持队列融合的 Publisher，提供 `requestFusion()` 协商机制

### 3.2 Reactor 超越规范的扩展

Reactive Streams 规范只定义了最基本的契约，Reactor 在此基础上添加了大量工程化能力：

| 规范定义 | Reactor 扩展 |
|---------|-------------|
| `Publisher.subscribe(Subscriber)` | `CorePublisher.subscribe(CoreSubscriber)` 避免装箱 |
| `Subscriber` 无 Context | `CoreSubscriber.currentContext()` 支持 Context 传播 |
| 无融合机制 | `Fuseable` 接口 + `QueueSubscription` 实现队列融合 |
| 无条件订阅者优化 | `ConditionalSubscriber.tryOnNext()` 避免 request 往返 |
| 无可观测性 | `Scannable` 接口支持运行时元数据查询 |
| 无装配跟踪 | `FluxOnAssembly` + `AssemblySnapshot` 装配时堆栈捕获 |
| 无 Hooks | `Hooks.onEachOperator` / `onLastOperator` 全局拦截 |
| 无 Scheduler | `Schedulers` 提供线程调度能力 |
| 无背压策略选择 | `OverflowStrategy` / `BufferOverflowStrategy` 策略模式 |

---

## 四、Reactor 对比其他响应式框架

### 4.1 Reactor vs RxJava

| 维度 | Reactor | RxJava |
|------|---------|--------|
| **规范兼容** | Reactive Streams (原生) | RxJava 2+ 兼容 Reactive Streams |
| **类型系统** | Flux (0..N) + Mono (0..1) | Observable (非背压) + Flowable (背压) + Single/Maybe/Completable |
| **背压默认** | 所有 Publisher 都支持背压 | 分为背压(Flowable)和非背压(Observable)两类 |
| **Context** | 原生 Context 不可变传播 | 无原生 Context (需借助外部机制) |
| **Scheduler** | parallel/boundedElastic/single/immediate | computation/io/newThread/trampoline/single |
| **装配时/执行时分离** | OptimizableOperator 循环优化 | 传统递归 subscribe |
| **融合** | Fuseable 队列融合 | 类似的微融合机制 |
| **生态** | Spring WebFlux 原生集成 | 通用响应式库，Android 广泛使用 |

Reactor 相对于 RxJava 的核心优势在于 `Mono` 类型的引入——RxJava 用 `Flowable` 处理 0..N 和 0..1 两种场景，而 Reactor 专设 `Mono` 类型，使得单值异步操作（如 HTTP 请求）的类型签名更精确，且可以在编译时获得更强的类型保证。

### 4.2 Reactor vs Mutiny

| 维度 | Reactor | Mutiny |
|------|---------|--------|
| **发起方** | VMware/Pivotal (Spring 生态) | Red Hat (Quarkus 生态) |
| **类型系统** | Flux + Mono | Uni + Multi |
| **API 风格** | 函数式链式调用 | 事件驱动链式调用 (onItem, onFailure, onCompletion) |
| **背压** | Reactive Streams 原生 | Reactive Streams 兼容 |
| **Context** | 原生 Context | 无原生 Context |
| **装配优化** | OptimizableOperator 循环 | 传统递归 |

Mutiny 的 API 风格更接近事件驱动（`uni.onItem().transform(...)`），而 Reactor 更接近函数式（`mono.map(...)`）。两者在概念上等价，但 Reactor 的函数式风格更简洁，Mutiny 的事件驱动风格更明确地表达了"什么时候做什么"。

---

## 五、Reactor 的核心设计模式

### 5.1 装饰器模式（操作符包装 source）

Reactor 的每个操作符都是对上游 `source` 的装饰。`FluxOperator` 基类持有 `source` 引用，操作符在不修改 source 的前提下添加新的处理逻辑：

```java
// FluxOperator.java
public abstract class FluxOperator<I, O> extends Flux<O> {
    protected final Flux<? extends I> source;
}
```

`FluxMap` 装饰了 `source`，在数据通过时应用 `mapper` 函数；`FluxFilter` 装饰了 `source`，在数据通过时应用 `predicate` 过滤。这种装饰器链就是操作符链。

**去掉装饰器模式会怎样？** 如果不用装饰器，每个操作符需要直接修改上游 Publisher 的行为，这破坏了不可变性。装饰器模式确保了每个操作符是一个独立的、可组合的单元。

### 5.2 观察者模式（Subscriber 监听 Publisher）

`Subscriber` 监听 `Publisher` 的信号——`onSubscribe`、`onNext`、`onError`、`onComplete`。这是经典的观察者模式，但增加了背压控制（`Subscription.request`）和取消能力（`Subscription.cancel`）。

### 5.3 建造者模式（操作符链构建）

`Flux` 的操作符链构建是一种隐式的建造者模式。每次调用 `map()`、`filter()` 等方法都返回新的 `Flux` 实例（带上了新的操作符装饰），逐步构建出完整的数据处理管道。与传统建造者模式不同，Reactor 的"建造者"返回的是不可变对象，每次操作产生新实例而非修改内部状态。

### 5.4 策略模式（OverflowStrategy、OnNextFailureStrategy）

Reactor 在多个地方使用策略模式允许用户选择不同的行为策略：

源码文件：`reactor/core/publisher/OnNextFailureStrategy.java`

```java
interface OnNextFailureStrategy extends BiFunction<Throwable, Object, Throwable>,
        BiPredicate<Throwable, Object> {

    String KEY_ON_NEXT_ERROR_STRATEGY = "reactor.onNextError.localStrategy";
    // ...
}
```

`OnNextFailureStrategy` 允许用户选择 `STOP`（终止序列）、`RESUME_DROP`（丢弃错误元素继续）或自定义策略。类似地，`BufferOverflowStrategy` 枚举提供了 `ERROR`、`DROP_OLDEST`、`DROP_LATEST` 等缓冲溢出策略。

源码文件：`reactor/core/publisher/BufferOverflowStrategy.java`

```java
public enum BufferOverflowStrategy {
    ERROR,
    DROP_OLDEST,
    DROP_LATEST,
}
```

### 5.5 享元模式（FluxEmpty 单例、Context0 单例）

Reactor 对无状态的对象使用享元模式，通过单例复用减少对象创建开销：

源码文件：`reactor/core/publisher/FluxEmpty.java`

```java
final class FluxEmpty extends Flux<Object>
        implements Fuseable.ScalarCallable<Object>, SourceProducer<Object> {

    private static final Flux<Object> INSTANCE = new FluxEmpty();

    private FluxEmpty() {
        // deliberately no op
    }

    @SuppressWarnings("unchecked")
    public static <T> Flux<T> instance() {
        return (Flux<T>) INSTANCE;
    }
}
```

`FluxEmpty` 是一个全局唯一的空 Publisher 单例，通过泛型擦除安全地复用于所有类型的 `Flux.empty()` 调用。`subscribe()` 方法直接调用 `Operators.complete(actual)`，不创建任何中间对象。

源码文件：`reactor/util/context/Context0.java`

```java
final class Context0 implements CoreContext {

    static final Context0 INSTANCE = new Context0();

    @Override
    public Context put(Object key, Object value) {
        return new Context1(key, value);
    }
    // ...
}
```

`Context0` 是空 Context 的单例。当 `Context.put()` 被调用时，返回 `Context1`（一个键值对）；再次 `put` 返回 `Context2`；更多键值对返回 `ContextN`。这种渐进式结构避免了在小规模 Context 中使用 Map 的开销。

---

## 六、Reactor 的性能优化策略

### 6.1 操作符融合（Fuseable）

源码文件：`reactor/core/Fuseable.java`

```java
public interface Fuseable {
    int NONE = 0;
    int SYNC = 1;
    int ASYNC = 2;
    // ...
}
```

Fuseable 机制允许相邻的操作符之间通过共享队列直接传递数据，避免每次 `onNext` 的方法调用开销。融合有两种模式：
- **SYNC**：上游是同步的（如 `FluxRange`），数据可以通过 `poll()` 直接获取，不需要 `request`
- **ASYNC**：上游是异步的（如 `FluxCreate`），数据在队列中，通过 `poll()` 取出

融合协商在 `onSubscribe` 时通过 `requestFusion()` 方法完成，如果协商成功，下游操作符直接调用 `poll()` 获取数据，跳过 `onNext` 信号传递。

### 6.2 条件订阅者优化（ConditionalSubscriber）

源码文件：`reactor/core/Fuseable.java`

```java
interface ConditionalSubscriber<T> extends CoreSubscriber<T> {
    /**
     * Try consuming the value and return true if successful.
     * @return true if consumed, false if dropped and a new value can be immediately sent
     */
    boolean tryOnNext(T t);
}
```

`ConditionalSubscriber` 提供了 `tryOnNext()` 方法，返回 boolean 表示是否消费了数据。这主要用于 `filter` 操作符——传统方式下，`filter` 需要先 `onNext` 传入下游，下游再决定是否处理；`tryOnNext` 允许 `filter` 在被拒绝时立即处理下一个元素，避免了不必要的 `request(1)` 往返。

### 6.3 优化循环替代递归（OptimizableOperator）

在第 15 篇中我们详细分析了 `OptimizableOperator` 接口和 `InternalFluxOperator.subscribe()` 的优化循环。这个循环用 `while(true)` 替代递归 `source.subscribe()`，将 O(N) 的栈深度降为 O(1)。

源码文件：`reactor/core/publisher/OptimizableOperator.java`

```java
interface OptimizableOperator<IN, OUT> extends CorePublisher<IN> {
    @Nullable CoreSubscriber<? super OUT> subscribeOrReturn(CoreSubscriber<? super IN> actual) throws Throwable;
    CorePublisher<? extends OUT> source();
    @Nullable OptimizableOperator<?, ? extends OUT> nextOptimizableSource();
}
```

### 6.4 CAS 无锁并发（AtomicLongFieldUpdater）

Reactor 大量使用 `AtomicLongFieldUpdater` 和 `AtomicReferenceFieldUpdater` 进行无锁并发控制。需求量管理、错误状态、取消状态等都通过 CAS 操作实现：

```java
// 典型模式（概念）
volatile long requested;
static final AtomicLongFieldUpdater<SomeSubscriber> REQUESTED =
    AtomicLongFieldUpdater.newUpdater(SomeSubscriber.class, "requested");

void request(long n) {
    long current, next;
    do {
        current = requested;
        next = Operators.addCap(current, n);
    } while (!REQUESTED.compareAndSet(this, current, next));
}
```

CAS 相比 `synchronized` 的优势在于不阻塞线程——在竞争不激烈时（大多数场景），CAS 只需要一次原子操作即可完成。

### 6.5 对象池和单例

除了 `FluxEmpty` 和 `Context0` 的单例模式，Reactor 还使用了：
- `EmptySubscription.INSTANCE`：空订阅单例
- `Operators.cancelledSubscription()`：已取消订阅单例
- `Queues.get(int)`：根据容量返回预配置的队列实例
- `Context1`/`Context2`：小规模 Context 使用固定字段而非 Map

这些优化减少了 GC 压力，在高吞吐场景下尤为重要。

### 6.6 Callable 优化

对于同步数据源（如 `Mono.just(T)`、`Mono.fromCallable(() -> value)`），Reactor 使用 `Callable` 接口标记。当操作符链中所有操作符都是同步的时，可以通过 `call()` 方法直接获取值，跳过整个 subscribe/onSubscribe/onNext 信号链。

源码文件：`reactor/core/publisher/FluxEmpty.java`

```java
final class FluxEmpty extends Flux<Object>
        implements Fuseable.ScalarCallable<Object>, SourceProducer<Object> {
    // ...
    @Override
    public @Nullable Object call() throws Exception {
        return null; /* Scalar optimizations on empty */
    }
}
```

`ScalarCallable` 是 `Callable` 的子接口，标记数据源是"标量"的——即可以直接同步获取值。`FluxMap` 等操作符在检测到上游是 `ScalarCallable` 时，可以直接同步执行 map 并返回结果，无需构建 Subscriber 链。

---

## 七、多角度交叉验证

### 7.1 从 API 设计角度

Reactor 的 API 设计遵循了"渐进式复杂度"原则：初学者可以用 `flux.map().filter().subscribe()` 快速上手，高级用户可以使用 `Operators.lift()` 自定义操作符、`Hooks.onEachOperator()` 全局拦截、`Fuseable` 队列融合等底层机制。这种设计使得 Reactor 既是易用的响应式库，也是高度可定制的底层框架。

### 7.2 从工程实践角度

Reactor 在生产环境中的典型使用模式包括：
- **Spring WebFlux**：HTTP 请求处理 = `Mono<Void>`，背压从客户端传递到数据库
- **R2DBC**：数据库查询结果 = `Flux<Row>`，流式读取，背压控制查询速率
- **WebClient**：HTTP 客户端请求 = `Mono<Response>`，非阻塞 I/O
- **Reactor Kafka**：消息消费 = `Flux<Record>`，背压控制消费速率
- **Reactor Netty**：TCP/UDP 通信 = `Flux<ByteBuf>`，Netty EventLoop 线程处理

### 7.3 从性能角度

Reactor 的性能优化层层叠加：
1. 装配时：`onAssembly` 只执行一次（操作符链构建完成后不再重复）
2. 执行时：`OptimizableOperator` 循环避免递归栈开销
3. 数据传递：`Fuseable` 融合避免 `onNext` 方法调用开销
4. 过滤场景：`ConditionalSubscriber.tryOnNext` 避免 `request` 往返
5. 同步场景：`ScalarCallable` 跳过信号链直接获取值
6. 并发控制：CAS 无锁操作避免线程阻塞
7. 内存管理：单例和对象池减少 GC 压力

### 7.4 从调试角度

Reactor 的调试支持是工程化的：
- `checkpoint()` 在装配时插入 `FluxOnAssembly`，在 `onError` 时附加调用链
- `Hooks.onOperatorDebug()` 全局启用堆栈跟踪
- `FluxOnAssembly.OnAssemblyException` 构建操作符调用树，格式化为可读的 "Error has been observed at the following site(s)" 输出
- `Scannable` 接口支持运行时查询操作符链状态（parent、actual、prefetch、cancelled 等）

---

## 八、归纳表格

### 表一：Reactor 核心设计模式对照表

| 设计模式 | Reactor 中的体现 | 核心类/接口 | 设计动机 |
|---------|-----------------|------------|---------|
| **装饰器模式** | 操作符包装上游 source | `FluxOperator`、`MonoOperator`、`InternalFluxOperator` | 不修改原 Publisher 的前提下添加处理逻辑，保证不可变性 |
| **观察者模式** | Subscriber 监听 Publisher 信号 | `CoreSubscriber`、`CorePublisher`、`Subscription` | 异步事件驱动的数据传递，支持背压控制 |
| **建造者模式** | 操作符链逐步构建 | `Flux.map().filter().flatMap()` | 渐进式构建数据处理管道，每次操作返回新实例 |
| **策略模式** | 背压策略、错误处理策略 | `BufferOverflowStrategy`、`OnNextFailureStrategy`、`OverflowStrategy` | 允许用户选择不同场景下的行为策略 |
| **享元模式** | 无状态对象单例复用 | `FluxEmpty.INSTANCE`、`Context0.INSTANCE`、`EmptySubscription.INSTANCE` | 减少重复对象的创建开销，降低 GC 压力 |
| **模板方法模式** | subscribe() 固定流程，subscribeOrReturn 可变 | `InternalFluxOperator.subscribe()`、`subscribeOrReturn()` | 统一订阅流程，具体操作符只实现创建 Subscriber 的逻辑 |
| **适配器模式** | Lift 适配不同 Publisher 类型 | `FluxLift`、`MonoLift`、`Operators.LiftFunction` | 将用户自定义的 Subscriber 装饰器适配到操作符链中 |

### 表二：Reactor 性能优化技术对照表

| 优化技术 | 机制 | 核心类/接口 | 效果 |
|---------|------|------------|------|
| **操作符融合 (Fuseable)** | 相邻操作符共享队列，poll() 替代 onNext() | `Fuseable`、`QueueSubscription`、`SYNC`/`ASYNC` 模式 | 减少 N 次方法调用为 N 次 poll()，跳过信号传递 |
| **条件订阅者 (ConditionalSubscriber)** | tryOnNext() 返回 boolean，避免 request 往返 | `ConditionalSubscriber`、`FluxFilter` | filter 场景下减少 50% 的 request 调用 |
| **优化循环 (OptimizableOperator)** | while 循环替代递归 subscribe | `OptimizableOperator`、`InternalFluxOperator.subscribe()` | 栈深度从 O(N) 降为 O(1) |
| **CAS 无锁并发** | AtomicLongFieldUpdater/AtomicReferenceFieldUpdater | `Operators.addCap()`、各 Subscriber 的 requested 字段 | 避免线程阻塞，高并发下性能稳定 |
| **ScalarCallable 优化** | 同步数据源直接 call() 获取值 | `ScalarCallable`、`FluxJust`、`FluxEmpty` | 跳过 subscribe/onSubscribe/onNext 信号链 |
| **单例复用** | 无状态对象全局唯一实例 | `FluxEmpty.INSTANCE`、`Context0.INSTANCE`、`EmptySubscription.INSTANCE` | 零分配，减少 GC |
| **渐进式 Context** | 根据键值对数量选择不同实现 | `Context0`→`Context1`→`Context2`→`ContextN` | 小规模 Context 用固定字段，避免 Map 开销 |
| **装配时预计算** | nextOptimizableSource() 在构造时计算 | `InternalFluxOperator.optimizableOperator` (final 字段) | 执行时无需类型检查，直接读取 |
| **延迟堆栈格式化** | AssemblySnapshot 在 onError 时才格式化 | `FluxOnAssembly.OnAssemblySubscriber.fail()` | 正常路径零开销，错误路径才付出代价 |
| **Hooks 复合函数** | 多个 hook 编译为单个 andThen 复合函数 | `Hooks.createOrUpdateOpHook()` | 装配时只调用一次复合函数，非多次遍历 |

---

## 九、总结：Reactor 的设计哲学回顾

经过 17 篇源码深度分析，我们可以将 Reactor Core 的设计哲学总结为以下五句话：

1. **声明优先于执行**：操作符链是数据管道的蓝图，而非立即执行的命令。装配时构建不可变链，执行时通过 subscribe 触发。这一分离使得异步管道可以被组合、重用、延迟执行。

2. **消费者驱动生产者**：数据流动方向和控制信号方向相反——数据从上游到下游，request 从下游到上游。背压是整个系统的安全阀，确保内存使用可预测。

3. **组合优先于继承**：每个操作符是一个独立的装饰器，通过持有 source 引用构建链。操作符之间通过标准接口（CoreSubscriber、Subscription）通信，无需了解彼此的具体实现。

4. **优化不可见但无处不在**：从 Fuseable 队列融合到 OptimizableOperator 循环优化，从 ConditionalSubscriber 到 ScalarCallable，Reactor 在保持 API 简洁的同时，在底层注入了大量性能优化。用户无需了解这些机制即可受益。

5. **工程化而非学术化**：Reactor 不仅是 Reactive Streams 规范的实现，更是一个面向生产环境的工程框架——Hooks 全局拦截、checkpoint 调试支持、Context 跨线程传播、Scheduler 线程管理，这些工程能力使得 Reactor 能够胜任从 Web 服务器到数据管道的各种生产场景。

Reactor Core 的源码是响应式编程在 Java 平台上的集大成之作。理解其源码不仅有助于更好地使用 Reactor 和 WebFlux，更能够深入理解异步编程、并发控制、背压机制等分布式系统核心概念的工程实现。这正是本系列源码研究的终极目标——**从源码中学习设计，从设计中理解哲学**。
