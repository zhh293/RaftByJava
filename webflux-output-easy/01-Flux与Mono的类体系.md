# Flux 与 Mono 的类体系：为什么 Reactor 要设计这么多层？

> **Reactor Core 源码解析 · 易懂版 01**
>
> 本篇用"为什么"驱动的方式，带你理解 Reactor 从 `Publisher` 到 `InternalFluxOperator` 这整棵类继承树。每一层的存在都有明确的理由——我们会逐层回答"它是什么"、"为什么需要它"、"去掉它会怎样"。

---

## 开场：从一个简单的类比开始

在讲 Reactor 的类体系之前，先用两个你一定用过的东西来建立直觉：

**Mono 就像快递单号追踪**——你查了一个快递单号，结果要么是"包裹已到达"（有一个值），要么是"查无此单"（空）。不可能一个单号对应好几个包裹。

**Flux 就像订阅 Newsletter**——你订阅了一个技术周刊，它会持续给你发内容，可能发 10 期、可能发 100 期，也可能哪天停刊了（onComplete）或者邮件系统崩了（onError）。

这就是 `Mono`（0 或 1 个元素）和 `Flux`（0 到 N 个元素）的本质区别。

**你可能会问：为什么要分成两个类？用一个 `Flux` 不行吗？我把 Flux 限制成最多发一个元素，不就是 Mono 了吗？**

当然可以这样做——但你会失去很多好处。后面我们会详细讨论这个设计决策。

---

## 先看全景：这棵继承树长什么样

在深入每一层之前，先建立一个全局视角。Reactor 的核心类继承关系大致如下：

```
                  Publisher<T>              ← Reactive Streams 规范定义的接口
                       │
                       ▼
                CorePublisher<T>            ← Reactor 自己加的扩展层
                 ┌─────┴──────┐
                 │            │
              Flux<T>      Mono<T>          ← 两条平行主干：0..N vs 0..1
                 │            │
          FluxOperator    MonoOperator      ← 公开的操作符基类（第三方可继承）
                 │            │
      InternalFluxOp    InternalMonoOp      ← 内部操作符基类（参与优化循环）
           │                  │
     FluxMap, FluxFilter   MonoMap...       ← 具体的操作符实现
```

除此之外，还有几个"辅助角色"：
- `CoreSubscriber` —— 对应 `CorePublisher`，是 `Subscriber` 的扩展
- `SourceProducer` —— 标记"我是数据源头"
- `OptimizableOperator` —— 优化循环的契约接口
- `ConnectableFlux` —— 热源变体
- `ParallelFlux` —— 并行处理变体

看起来层级不少，对吧？别担心，每一层都有存在的理由。我们一层层来看。

---

## 第一层：Publisher —— 所有响应式类型的老祖宗

### 它是什么？

`Publisher` 是 Reactive Streams 规范定义的接口，只有一个方法：

```java
public interface Publisher<T> {
    void subscribe(Subscriber<? super T> s);
}
```

就这么简单——"你可以订阅我，我会给你发数据"。

### 为什么需要它？

`Publisher` 是**跨框架互操作的最小公约数**。不管你用 Reactor、RxJava 还是 Akka Streams，只要大家都实现这个接口，就能互相消费对方的数据流。

比如你可以把一个 RxJava 的 `Flowable` 当作 `Publisher` 传给 Reactor 的 `Flux.from()`，Reactor 能直接消费它。

### 对应的 Subscriber

```java
public interface Subscriber<T> {
    void onSubscribe(Subscription s);
    void onNext(T t);
    void onError(Throwable t);
    void onComplete();
}
```

四个方法，分别对应订阅建立、数据到达、错误发生、流结束。这就是 Reactive Streams 规范的全部核心接口。

---

## 第二层：CorePublisher 和 CoreSubscriber —— Reactor 为什么要多加一层？

### 你可能会问：Publisher 已经够用了，为什么 Reactor 还要在上面加一层？

让我们看 `CorePublisher` 多了什么：

```java
public interface CorePublisher<T> extends Publisher<T> {
    void subscribe(CoreSubscriber<? super T> subscriber);
}
```

表面上看，只是参数类型从 `Subscriber` 变成了 `CoreSubscriber`。但这一个改变带来了两个重要能力：

**能力一：跳过全局拦截点**

Reactor 有一个叫 `Hooks.onLastOperator` 的全局钩子——每次调用公开的 `subscribe(Subscriber)` 都会经过这个拦截点（用于调试、监控等）。但 Reactor 内部的操作符之间互相订阅时，不需要这个拦截——它只会浪费性能。

`subscribe(CoreSubscriber)` 就是内部使用的"快车道"，绕过这些不必要的检查。

**能力二：支持 Context 传递**

来看 `CoreSubscriber`：

```java
public interface CoreSubscriber<T> extends Subscriber<T> {
    default Context currentContext() {
        return Context.empty();
    }
    
    @Override
    void onSubscribe(Subscription s);
}
```

多了一个 `currentContext()` 方法。这是 Reactor 的 Context 传播机制的入口——一个不可变的键值对容器，**从下游往上游传播**（和数据流的方向相反）。

> 生活类比：你去饭店点菜，服务员记下你的桌号写在小纸条上。这个小纸条（Context）随着订单从前台（下游）传到厨房（上游），厨房看到纸条就知道菜做好了端到几号桌。
>
> 在 WebFlux 中，Context 常用来传递 Trace ID、用户认证信息等——这些信息需要跨越整条异步链，但不能用 ThreadLocal（因为响应式编程会切换线程）。

### CoreSubscriber 还放宽了什么规则？

Reactive Streams 规范有些严格的规则是为"不信任的跨框架交互"设计的。比如：

| 规范规则 | 原始要求 | CoreSubscriber 的放宽 |
|---------|---------|---------------------|
| 1.3 | 请求量 n <= 0 时必须触发 onError | 无效请求被静默忽略，不触发 onError |
| 3.9 | 必须在 onSubscribe 之后才能 request | 内部操作符之间信任调用顺序，不做严格检查 |

**为什么要放宽？** 在 Reactor 内部，操作符之间是"自家人"，天然可信。如果每个中间操作符都执行严格的边界检查，一条 10 个操作符的链就要做 10 次不必要的检查。`CoreSubscriber` 让内部路径更高效，而外部用户传入的 `Subscriber` 仍然通过 `Operators.toCoreSubscriber()` 被包装成严格合规的版本。

### 去掉 CorePublisher / CoreSubscriber 会怎样？

1. **Context 无处安放**。Reactive Streams 的 `Subscriber` 没有扩展点可以承载 Context，要么用额外包装类（增加对象分配），要么用 ThreadLocal（异步场景不可靠）。
2. **所有内部订阅都要经过 Hooks 拦截**。10 个操作符的链 = 10 次不必要的拦截检查。
3. **所有操作符都要执行严格的规范检查**，即使上游是已知安全的 Reactor 内部组件。

---

## 第三层：Flux 和 Mono —— 为什么需要两条平行主干？

### 类型即文档

```java
public abstract class Flux<T> implements CorePublisher<T> { ... }
public abstract class Mono<T> implements CorePublisher<T> { ... }
```

`Flux` 和 `Mono` 都实现了 `CorePublisher`，它们在**运行时并没有强制限制元素个数**——Mono 不会在发出第一个元素后自动取消订阅。区别是**类型系统层面的语义契约**。

**你可能会问：那这个区分有什么实际意义呢？**

考虑两个方法签名：

```java
// 方法 A
Flux<User> findUsers(String keyword);

// 方法 B
Mono<User> findUserById(int id);
```

看到 `Mono<User>` 你立刻知道"这个方法最多返回一个用户"——它就像一个异步的 `Optional<User>`。看到 `Flux<User>` 你知道"可能返回多个结果"。**类型签名本身就是文档**，编译器也能帮你避免错误。

### 操作符集合不同

因为基数约束不同，Mono 和 Flux 提供了不同的操作符：

| 操作符 | Flux 的行为 | Mono 的行为或替代 |
|--------|-----------|-----------------|
| `flatMap` | 返回 `Flux`（N -> M 映射，可能交错） | 返回 `Mono`（1 -> 1 映射） |
| `zipWith` | 按元素逐对匹配 | 两个 Mono 的值组合成 `Tuple2` |
| `flatMapMany` | 不存在 | `Mono` 独有：从 Mono 转 Flux |
| `buffer`、`window` | 有 | 没有（对单个值做分片没意义） |

### ConnectableFlux 和 ParallelFlux：特化的变体

**ConnectableFlux** 继承自 `Flux`，增加了"连接"语义——多个订阅者先堆积，直到显式调用 `connect()` 才开始数据流。典型用法是 `Flux.publish()`。

```java
public abstract class ConnectableFlux<T> extends Flux<T> {
    public abstract void connect(Consumer<? super Disposable> cancelSupport);
    public final Flux<T> autoConnect(int minSubscribers, ...) { ... }
    public final Flux<T> refCount(int minSubscribers) { ... }
}
```

- `autoConnect(n)`：当订阅者数量达到 n 时自动连接
- `refCount(n)`：额外跟踪引用计数，所有订阅者取消后断开

> 类比：ConnectableFlux 就像直播间——观众（订阅者）先进来等着，主播（connect）开播后大家同时看到相同的内容。

**ParallelFlux** 没有继承 `Flux`，而是直接实现 `CorePublisher`：

```java
public abstract class ParallelFlux<T> implements CorePublisher<T> { ... }
```

**为什么不继承 Flux？** 因为 `Flux` 的操作符假设数据是单线串行的，而 `ParallelFlux` 将数据分到多条"轨道"上并行处理。如果继承了 `Flux`，用户会误以为 `buffer`、`window` 等操作符在并行场景下也能正常工作。不继承就从类型系统层面杜绝了这种误用。

> 类比：`Flux` 是单车道公路，`ParallelFlux` 是多车道高速公路。多车道上的交通规则（操作符）和单车道是不一样的——你不能在高速公路上随意停车（buffer），但可以在出口汇合（sequential）。

---

## 第四层：FluxOperator 和 MonoOperator —— 操作符的公开基类

### 它们长什么样？

```java
public abstract class FluxOperator<I, O> extends Flux<O> implements Scannable {
    protected final Flux<? extends I> source;

    protected FluxOperator(Flux<? extends I> source) {
        this.source = Objects.requireNonNull(source);
    }
}

public abstract class MonoOperator<I, O> extends Mono<O> implements Scannable {
    protected final Mono<? extends I> source;

    protected MonoOperator(Mono<? extends I> source) {
        this.source = Objects.requireNonNull(source);
    }
}
```

### 核心：source 字段

每个操作符都有一个 `source` 字段，指向它的上游。当你写 `flux.map(f).filter(p).take(10)` 时，实际上创建了一条由 `source` 指针串起来的链：

```
FluxTake.source → FluxFilter.source → FluxMap.source → 原始 Flux
```

这就是经典的装饰器模式——每个操作符"包装"了上一个操作符，在不修改原始对象的情况下增加新功能。

**注意 source 的类型约束**：
- `FluxOperator.source` 的类型是 `Flux<? extends I>`——上游必须是 `Flux`
- `MonoOperator.source` 的类型是 `Mono<? extends I>`——上游必须是 `Mono`

这保证了 Flux 的操作符链上不会混入 Mono，反之亦然。要跨越边界（比如 `Mono.flux()` 或 `Flux.single()`），需要专门的桥接操作符。

### 谁会继承它们？

`FluxOperator` 和 `MonoOperator` 是 `public` 的——**第三方库可以继承它们来创建自定义操作符**。比如 `reactor-extra` 和 `reactor-netty` 就用它来扩展 Reactor 的能力。

### Scannable 和 InternalProducerAttr

这两个类都实现了 `Scannable` 接口，用于运行时的"链扫描"——调试时打印操作符链、获取 prefetch 值等。

一个重要的细节：`scanUnsafe` 方法对 `InternalProducerAttr.INSTANCE` 返回 `false`：

```java
// FluxOperator.scanUnsafe
if (key == InternalProducerAttr.INSTANCE) return false; // 公开类！
```

这个标记告诉 Reactor："我是公开的外部类，不是 Reactor 内部的"。这在 Context 传播时有用——遇到外部 Publisher 时，Reactor 需要额外的 Context 恢复逻辑，因为外部代码可能没有正确传播 Context。

---

## 第五层：InternalFluxOperator 和 InternalMonoOperator —— 性能优化的核心

### 这才是重头戏

```java
// 注意：没有 public 修饰符，是包私有的
abstract class InternalFluxOperator<I, O> extends FluxOperator<I, O>
        implements Scannable, OptimizableOperator<O, I> {

    final @Nullable OptimizableOperator<?, I> optimizableOperator;

    protected InternalFluxOperator(Flux<? extends I> source) {
        super(source);
        if (source instanceof OptimizableOperator) {
            this.optimizableOperator = (OptimizableOperator<?, I>) source;
        } else {
            this.optimizableOperator = null;
        }
    }
}
```

### 构造器里的 instanceof 检查

注意构造器里的逻辑：如果上游 `source` 也实现了 `OptimizableOperator`（即也是内部操作符），就把它缓存到 `optimizableOperator` 字段。这构成了一条"优化链表"——每个内部操作符都有一个指针指向上游最近的内部操作符。

### subscribe 方法中的 while(true) 优化循环

这是整个 Reactor 订阅路径中最关键的优化：

```java
@Override
public final void subscribe(CoreSubscriber<? super O> subscriber) {
    OptimizableOperator operator = this;
    try {
        while (true) {
            subscriber = operator.subscribeOrReturn(subscriber);
            if (subscriber == null) {
                return;  // 操作符自己处理了订阅
            }
            OptimizableOperator newSource = operator.nextOptimizableSource();
            if (newSource == null) {
                // 到达非优化边界，用传统方式订阅
                CorePublisher operatorSource = operator.source();
                operatorSource.subscribe(subscriber);
                return;
            }
            operator = newSource;
        }
    } catch (Throwable e) {
        Operators.reportThrowInSubscribe(subscriber, e);
    }
}
```

**这段代码把原本递归的订阅过程转换成了迭代。**

传统递归方式下，100 个操作符的链在订阅时需要 100 层栈帧——极端场景可能导致 `StackOverflowError`。while(true) 循环让所有操作符的订阅都在同一个栈帧内完成，栈深度恒定为 O(1)。

> 类比：递归就像"传话游戏"——A 告诉 B，B 告诉 C，每个人都得等下一个人回话。while 循环就像"依次点名"——老师从名单的第一个念到最后一个，一个人完成所有工作。

### 为什么要分 FluxOperator 和 InternalFluxOperator 两层？

| 维度 | FluxOperator | InternalFluxOperator |
|------|-------------|---------------------|
| 可见性 | `public`，第三方可继承 | 包私有，仅 Reactor 内部使用 |
| 优化循环 | 不参与 | 参与（实现 OptimizableOperator） |
| InternalProducerAttr | `false` | `true` |
| 典型继承者 | reactor-extra、reactor-netty 的自定义操作符 | FluxMap、FluxFilter 等 Reactor 核心操作符 |

**去掉 FluxOperator 只保留 InternalFluxOperator 会怎样？** 第三方库无法以标准方式创建自定义操作符——它们必须直接继承 `Flux`，失去了 `source` 字段和 `Scannable` 的默认实现。

**去掉 InternalFluxOperator 只保留 FluxOperator 会怎样？** 所有操作符都走传统递归路径，深度操作符链可能爆栈。

**为什么不让第三方操作符也参与优化循环？** 因为第三方代码不能保证正确实现 `subscribeOrReturn` 的契约（返回 null 表示"我自己处理"，非 null 表示"继续循环"）。如果第三方代码实现有 bug，可能导致不可预期的行为。

---

## 辅助角色一：SourceProducer —— "我是源头"

```java
interface SourceProducer<O> extends Scannable, Publisher<O> {
    @Override
    default Object scanUnsafe(Attr key) {
        if (key == Attr.PARENT) return null;     // 没有上游
        if (key == Attr.ACTUAL) return null;     // 不关心下游
        if (key == InternalProducerAttr.INSTANCE) return true;
        return null;
    }

    @Override
    default String stepName() {
        return "source(" + getClass().getSimpleName() + ")";
    }
}
```

`FluxArray`、`FluxRange`、`FluxJust`、`FluxEmpty` 等源头操作符都实现了这个接口。它做了两件事：
1. 通过 `Attr.PARENT` 返回 `null` 告诉扫描工具"链到我这里就结束了"。
2. 提供统一的 `stepName()` 实现（用于调试输出，比如 `"source(FluxArray)"`）。

**为什么不让 FluxArray 等直接实现 Scannable？** 因为 `SourceProducer` 提供了统一的默认实现。如果每个源头类都自己写 `PARENT` 返回 null、`ACTUAL` 返回 null 的逻辑，就会有大量重复代码。

---

## 辅助角色二：OptimizableOperator —— 优化循环的契约

```java
interface OptimizableOperator<IN, OUT> extends CorePublisher<IN> {

    @Nullable CoreSubscriber<? super OUT> subscribeOrReturn(
        CoreSubscriber<? super IN> actual) throws Throwable;

    CorePublisher<? extends OUT> source();

    @Nullable OptimizableOperator<?, ? extends OUT> nextOptimizableSource();
}
```

三个方法的协作关系：

| 方法 | 做什么 | 返回值含义 |
|------|--------|----------|
| `subscribeOrReturn(actual)` | 创建内部 Subscriber 包装下游 | 非 null = "这是上游需要的 Subscriber，循环继续"；null = "我自己搞定了，循环停止" |
| `source()` | 获取原始上游 Publisher | 当循环到达尽头时，用它做最终订阅 |
| `nextOptimizableSource()` | 获取上游最近的 OptimizableOperator | 非 null = "继续循环"；null = "我到头了" |

`nextOptimizableSource()` 的实现就是返回构造器中缓存的 `optimizableOperator` 字段。优化循环只能沿着连续的内部操作符链前进。一旦遇到非内部的 Publisher（如第三方实现），循环停止并回退到传统递归。

---

## Flux.subscribe(Subscriber) 的入口逻辑

你可能注意到了，`Flux` 本身的公开 `subscribe(Subscriber)` 方法里也有一个类似的优化循环：

```java
// Flux.java
public final void subscribe(Subscriber<? super T> actual) {
    CorePublisher publisher = Operators.onLastAssembly(this);  // 经过 Hooks
    CoreSubscriber subscriber = Operators.toCoreSubscriber(actual);  // 包装成 CoreSubscriber

    if (publisher instanceof OptimizableOperator) {
        OptimizableOperator operator = (OptimizableOperator) publisher;
        while (true) {
            subscriber = operator.subscribeOrReturn(subscriber);
            if (subscriber == null) return;
            OptimizableOperator newSource = operator.nextOptimizableSource();
            if (newSource == null) {
                publisher = operator.source();
                break;
            }
            operator = newSource;
        }
    }

    publisher.subscribe(subscriber);
}
```

**为什么公开方法中也需要这个循环？** 因为用户直接调用 `flux.subscribe(mySubscriber)` 时，入口是公开的 `subscribe(Subscriber)`。如果这里不做优化，第一层操作符的 `subscribe` 就走不到优化循环——白白浪费了一次优化机会。

**两处循环的区别**：
- 公开 `subscribe` 先走 `Operators.onLastAssembly` 和 `Operators.toCoreSubscriber`（Hooks + 包装）
- 内部 `subscribe(CoreSubscriber)` 直接进循环（"快车道"）

---

## 为什么 InternalMonoOperator 和 InternalFluxOperator 的代码几乎一模一样？

对，`InternalMonoOperator` 的 `subscribe` 方法与 `InternalFluxOperator` 完全相同——逐字符复制。

**你可能会问：为什么不提取到一个公共基类？**

因为 Java 是**单继承**的。`InternalFluxOperator` 必须继承 `FluxOperator`（→ `Flux`），`InternalMonoOperator` 必须继承 `MonoOperator`（→ `Mono`）。它们已经用掉了唯一的继承名额，无法再共享一个公共父类。

这是 Java 类型系统的一个固有限制。Kotlin 的接口默认方法或 Scala 的 trait 可以更优雅地解决这个问题，但 Reactor 是纯 Java 项目，只能接受这种代码重复。

---

## 实际开发中，这些知识有什么用？

**场景一：调试复杂的操作符链**

当你的 WebFlux 接口报了一个莫名其妙的错误，堆栈里全是 Reactor 的内部类名。理解了类体系后，你至少能读懂：
- `FluxMap$MapSubscriber.onNext` —— 啊，是在 `map` 操作符的数据处理中出了问题
- `InternalFluxOperator.subscribe` —— 啊，是在订阅阶段的优化循环里出了问题

⚠️ **踩坑提醒**：如果你在开发中看到堆栈里有 `Operators.restoreContextOnSubscriberIfPublisherNonInternal`，说明 Reactor 在处理一个"非内部"Publisher（可能是第三方的或你自己写的自定义操作符）。Context 传播可能在这里出了问题。

**场景二：写自定义操作符**

如果你需要扩展 Reactor 的能力，比如写一个"自动重试带退避"的操作符：
- 继承 `FluxOperator`（不是 `InternalFluxOperator`——后者是包私有的）
- 提供 `source` 字段存储上游
- 实现 `subscribe(CoreSubscriber)` 方法

**场景三：理解性能特征**

知道优化循环的存在后，你就理解了为什么 Reactor 建议用内置操作符组合而不是自定义操作符——内置操作符参与优化循环（O(1) 栈深度），自定义操作符不参与（增加一层栈帧）。对于绝大多数场景这无所谓，但在极深的操作符链中可能会成为问题。

---

## 归纳表格：类层次全景对照

| 类/接口 | 可见性 | 继承自 | 核心字段 | 核心方法 | 存在的理由 |
|---------|--------|-------|---------|---------|-----------|
| `Publisher<T>` | 公开（RS 规范） | - | - | `subscribe(Subscriber)` | 跨框架互操作的最小契约 |
| `CorePublisher<T>` | 公开 | `Publisher` | - | `subscribe(CoreSubscriber)` | 内部快车道：绕过 Hooks，支持 Context |
| `CoreSubscriber<T>` | 公开 | `Subscriber` | - | `currentContext()` | Context 传播 + 放宽内部规范检查 |
| `Flux<T>` | 公开抽象类 | `CorePublisher` | - | 几千个操作符方法 | 0..N 语义的主干类 |
| `Mono<T>` | 公开抽象类 | `CorePublisher` | - | 几千个操作符方法 | 0..1 语义的主干类 |
| `ConnectableFlux<T>` | 公开抽象类 | `Flux` | - | `connect()`, `autoConnect()`, `refCount()` | 热源：订阅者堆积后统一连接 |
| `ParallelFlux<T>` | 公开抽象类 | `CorePublisher` | - | `runOn()`, `sequential()` | 多轨道并行处理 |
| `FluxOperator<I,O>` | 公开抽象类 | `Flux<O>` + `Scannable` | `source: Flux<I>` | `scanUnsafe()` | 第三方可继承的操作符基类 |
| `MonoOperator<I,O>` | 公开抽象类 | `Mono<O>` + `Scannable` | `source: Mono<I>` | `scanUnsafe()` | 第三方可继承的操作符基类 |
| `InternalFluxOperator<I,O>` | 包私有 | `FluxOperator` + `OptimizableOperator` | `optimizableOperator` | `subscribe()`（while 循环）, `subscribeOrReturn()` | 内部操作符基类，参与优化循环 |
| `InternalMonoOperator<I,O>` | 包私有 | `MonoOperator` + `OptimizableOperator` | `optimizableOperator` | `subscribe()`（while 循环）, `subscribeOrReturn()` | 内部操作符基类，参与优化循环 |
| `OptimizableOperator<IN,OUT>` | 包私有接口 | `CorePublisher` | - | `subscribeOrReturn`, `source`, `nextOptimizableSource` | 定义优化循环的三方法契约 |
| `SourceProducer<O>` | 包私有接口 | `Scannable` + `Publisher` | - | `scanUnsafe()`, `stepName()` | 标记链的源头，提供默认扫描实现 |

## 设计原则总结

回顾整棵类继承树，Reactor 遵循了几个一以贯之的设计原则：

| 原则 | 体现 |
|------|------|
| **在规范之上扩展，不修改规范** | `CorePublisher`/`CoreSubscriber` 是对 RS 规范的扩展，不是替换 |
| **用类型系统表达语义** | `Flux` vs `Mono` 的分离让编译器帮你检查基数约束 |
| **内部优化与外部契约分离** | `FluxOperator`（公开）vs `InternalFluxOperator`（内部）的分层 |
| **用循环替代递归** | `OptimizableOperator` 的三方法契约，解决深度链的栈溢出风险 |
| **标记接口辅助运行时决策** | `SourceProducer`、`InternalProducerAttr` 让框架在运行时区分内外 |

理解了这棵继承树，你就拥有了阅读 Reactor 任何操作符源码的"地图"。下一篇我们会深入具体的操作符实现——看看 `FluxMap`、`FluxFilter` 这些类内部到底长什么样。
