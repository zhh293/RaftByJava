# 数据源 Publisher——你的数据到底是从哪儿来的

> Reactor Core 源码解析·易懂版 第 04 篇
>
> 配套硬核版：`04-数据源Publisher全流程源码解析.md`

---

## 一、先问一个问题：数据从哪来

你每天写的响应式代码，无非是这几种写法：

```java
Flux.just(1, 2, 3)
Flux.range(1, 10)
Flux.fromIterable(userList)
Flux.create(sink -> { ... })
Flux.empty()
Flux.error(new RuntimeException("挂了"))
Flux.defer(() -> queryFromDb())
Flux.interval(Duration.ofSeconds(1))
```

这些都是"数据源"（Source）——响应式链条最上游、不依赖任何上游 Publisher、直接生产数据的那个起点。但你有没有想过：**这些数据源底层的实现方式完全不一样？** 有的像是"提前做好放在保温柜里的快餐"（同步源），有的像是"现点现做的现炒菜"（异步源），有的干脆是"空盘子"（空源），还有的是"到了才决定做什么"（延迟源）。

这一篇我们就把 Reactor Core 里所有核心数据源掰开揉碎，看看它们各自的脾气秉性。

在源码层面，所有数据源都实现了一个叫 `SourceProducer<O>` 的接口，这是一个标记接口，主要作用是告诉框架"我是链条的起点，我没有上游"：

```java
interface SourceProducer<O> extends Scannable, Publisher<O> {
    @Override
    default @Nullable Object scanUnsafe(Attr key) {
        if (key == Attr.PARENT) return null;   // 我没有上游
        if (key == Attr.ACTUAL) return null;
        if (key == InternalProducerAttr.INSTANCE) return true;  // 我是链的起点
        return null;
    }

    @Override
    default String stepName() {
        return "source(" + getClass().getSimpleName() + ")";  // 方便调试工具识别
    }
}
```

**为什么需要专门搞一个接口来标记"我是源头"？** 想象你在用 Reactor 的调试工具（比如 `Flux.dump()` 或链路追踪）排查一个复杂链条的问题，工具需要顺着操作符链一直往上游遍历，直到走到头。如果没有一个明确的"到头了"标记，遍历逻辑就得东猜西猜。`SourceProducer` 相当于给整条链的起点插了一面旗子：**往上不用再找了，我就是源头**。

数据源大致可以分成四类：

```
┌─────────────┬───────────────┬───────────────┬───────────────────────────┐
│   同步源    │   异步源      │    空源       │   延迟源                  │
├─────────────┼───────────────┼───────────────┼───────────────────────────┤
│ FluxArray   │ FluxCreate    │ FluxEmpty     │ FluxDefer                 │
│ FluxRange   │ FluxInterval  │ MonoEmpty     │                           │
│ MonoJust    │               │ FluxNever     │                           │
│ FluxJust    │               │ FluxError     │                           │
│ FluxIterable│               │               │                           │
│ FluxGenerate│               │               │                           │
└─────────────┴───────────────┴───────────────┴───────────────────────────┘
```

下面按这四类逐一拆解。

---

## 二、同步源：数据都是提前备好的"快餐"

同步源的共同特点是：**数据在 subscribe 那一刻就已经完全确定了，发射过程不涉及任何等待（没有网络 IO、没有异步回调）**，就像快餐店的食材早就切好焯好放在保温柜里，你一点单，师傅立刻就能给你打包。

### 2.1 FluxArray——最朴素的"一份份打包好的菜"

`FluxArray` 就是把一个 Java 数组包装成 Flux，对应 `Flux.just(1, 2, 3)` 这种写法（元素超过 1 个时走的就是这条路）。它的 subscribe 逻辑：

```java
public static <T> void subscribe(CoreSubscriber<? super T> s, T[] array) {
    if (array.length == 0) {
        Operators.complete(s);   // 空数组，直接说完事了
        return;
    }
    if (s instanceof ConditionalSubscriber) {
        s.onSubscribe(new ArrayConditionalSubscription<>(
            (ConditionalSubscriber<? super T>) s, array));
    }
    else {
        s.onSubscribe(new ArraySubscription<>(s, array));
    }
}
```

这里有个有意思的分支：`ConditionalSubscriber`。这是什么？

**举个场景**：假如你写了 `Flux.just(1,2,3,4,5).filter(x -> x % 2 == 0)`，`filter` 会把不满足条件的值丢掉，然后必须回头跟上游说"再给我一个，刚才那个我没用"（多补一次 `request(1)`）。如果每次过滤掉一个值都要走一次完整的"发送-拒绝-补请求"往返，开销就浪费在了这些"没用的沟通"上。

`ConditionalSubscriber` 提供了一个 `tryOnNext(T)` 方法，直接返回一个 boolean 告诉上游"这个值我到底用没用"，上游据此判断该不该把这次发送计入配额。这就好比后厨直接问一句"这道菜你要不要"，顾客一句话回答"不要"，比起把菜端上桌又端回去再补一份新的，效率高得多。

⚠️ **踩坑提醒**：这个优化是自动生效的，你不需要写任何代码来"开启"它——只要你的下游操作符（比如 `filter`）恰好实现了 `ConditionalSubscriber`，Reactor 就会自动切换到更高效的路径。这也是为什么很多人觉得"Reactor 的 filter 性能出奇的好"，秘密就在这里。

`ArraySubscription` 内部有两条发射路径——**fastPath**（管够模式）和 **slowPath**（按需计量模式），这部分内容跟上一篇《订阅机制与背压协议》讲的完全一致，这里不重复，你可以理解为：**"管够"时直接从头发到尾，不做任何计数；"按需"时，一边发一边扣配额，配额扣光了就暂停，等下次 `request` 才继续。**

`FluxArray` 还支持一种叫 **SYNC 融合** 的优化，简单说就是下游可以绕开 `onNext` 的推送机制，直接用 `poll()` 从数组里"拽"数据：

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

打个比方：正常的 `onNext` 推送模式，好比服务员一道一道端菜上桌，顾客被动接收；而融合模式下的 `poll()`，好比顾客直接走进厨房自己拿盘子——省去了"喊服务员"这个中间环节，对于像 `filter`、`map` 这种简单的转换操作符来说，直接融合能省下大量方法调用开销。

### 2.2 FluxRange——"一份份编号菜"

`FluxRange`（对应 `Flux.range(1, 10)`）跟 `FluxArray` 结构几乎一样，只是数据源从"数组"变成了"一个连续的整数区间"。它的 subscribe 有三条分支：

```java
public void subscribe(CoreSubscriber<? super Integer> actual) {
    long st = start;
    long en = end;
    if (st == en) {
        Operators.complete(actual);  // 空范围，直接完事
        return;
    }
    if (st + 1 == en) {
        actual.onSubscribe(Operators.scalarSubscription(actual, (int)st));  // 只有一个数，走极简通道
        return;
    }
    // ... 正常创建 RangeSubscription
    actual.onSubscribe(new RangeSubscription(actual, st, en));
}
```

**范围为空**（比如 `range(5, 0)`）直接完事，**范围里只有一个数**（比如 `range(5, 1)`）走一个专门的单值极简通道，跳过了创建完整 Subscription 所需要的 CAS 字段、原子变量等重型基础设施。

这体现了 Reactor 一个非常一致的设计哲学：**为最常见的边界情况（0 个、1 个元素）单独优化，不要让"通用逻辑"为这些简单场景背锅**。

### 2.3 MonoJust / FluxJust——"单人套餐"，以及一个叫 ScalarCallable 的黑科技

`Mono.just("hello")` 和 `Flux.just("hello")`（只传一个参数时）背后分别对应 `MonoJust` 和 `FluxJust`，它们的实现极其简单：

```java
final class MonoJust<T> extends Mono<T>
        implements Fuseable.ScalarCallable<T>, Fuseable, SourceProducer<T> {
    final T value;

    @Override
    public T call() throws Exception {
        return value;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        actual.onSubscribe(Operators.scalarSubscription(actual, value));
    }
}
```

这里最值得关注的是 `Fuseable.ScalarCallable<T>` 这个接口。它标记的意思是：**"我这个 Publisher 里只有一个值，你甚至不需要走订阅这一整套流程，直接调用我的 `call()` 方法就能拿到值。"**

**这有什么用？** 想象一下 `Mono.just(1).map(x -> x + 1).filter(x -> x > 0)` 这条链。正常情况下，每加一个操作符就要多一层 Subscriber 包装、多一次 `onSubscribe`、`request`、`onNext`、`onComplete` 的完整往返。但既然 `Mono.just(1)` 从一开始就知道自己只有一个值，`map` 操作符完全可以在订阅之前就直接调用 `call()` 拿到这个值，同步执行完 `mapper` 函数，把结果包装成一个新的标量值——**全程不需要创建任何 Subscription 对象**。这是 `FluxFlatMap` 里"标量优化"的理论基础，我们会在第 05 篇讲 flatMap 时再看到它的实际应用。

打个比方：正常流程好比你去餐厅点了一道菜，厨房要走完整的"接单-确认-下厨-上菜"流程；而 `ScalarCallable` 相当于你问服务员"今天有啤酒吗"，服务员直接从口袋里掏出一瓶递给你——不用启动整套厨房流程。

⚠️ **踩坑提醒**：`MonoJust` 的 `value` 字段是不允许为 null 的（构造函数里有 `Objects.requireNonNull`）。如果你写 `Mono.just(getValueThatMightBeNull())`，一旦这个值是 null，代码会在构造阶段就直接抛 `NullPointerException`，而不是等到订阅时才报错。这是新手很容易踩的坑——**Reactor 从设计上完全不允许 null 值流经管道**，如果你需要表达"可能没有值"，应该用 `Mono.justOrEmpty(...)` 或者干脆 `Mono.empty()`。

`MonoJust` 和 `FluxJust` 的唯一区别：`MonoJust` 额外实现了 `block()` 方法可以直接同步拿到值（因为 Mono 语义上就是"最多一个值"），`FluxJust` 没有这个方法（因为 Flux 语义上可以有多个值，没有"直接拿到唯一结果"这种说法）。

### 2.4 FluxIterable——把普通集合接进响应式世界

`Flux.fromIterable(userList)` 背后是 `FluxIterable`，它把一个普通的 `Iterable`（比如 `List`）转换成 Flux。这里有个很实用的细节：**怎么判断这个 Iterable 是不是"有限的"？**

```java
static <T> boolean checkFinite(Spliterator<? extends T> spliterator) {
    return spliterator.hasCharacteristics(Spliterator.SIZED);
}
```

**为什么要关心"有限还是无限"？** 因为当流被取消或者出错时，Reactor 需要把队列里剩下没处理的元素做"丢弃"（discard）处理——这通常意味着要遍历一遍剩余元素。但如果这个 Iterable 本身是无限的（比如你传进来一个用 `Stream.iterate` 生成的无限流），遍历丢弃这个动作会直接死循环卡死。`checkFinite` 通过检查 `Spliterator.SIZED` 这个特性位，提前判断这个数据源到底能不能安全地"清盘"。

**为什么用 `Spliterator` 而不是我们更熟悉的 `Iterator`？** `Spliterator` 是 Java 8 之后引入的更现代的遍历接口，比传统 `Iterator` 多了几个优势：支持并行分割、支持通过特性位（如 `SIZED`、`ORDERED`）查询集合的性质、并且 `tryAdvance` 方法是"一步到位"的操作（取值和判断是否还有更多元素合二为一），避免了传统 `Iterator` 里 `hasNext()` 和 `next()` 分开调用可能出现的状态不一致问题（比如两次调用之间集合被并发修改了）。

### 2.5 FluxGenerate——"厨师亲自控制每道菜怎么做"

`Flux.generate(sink -> { ... })` 是一种更精细的数据生成方式，用户可以维护一个状态，在每次调用时基于状态生成一个值：

```java
Flux.generate(
    () -> 0,                                   // 初始状态
    (state, sink) -> {
        sink.next("第" + state + "个值");
        if (state == 9) sink.complete();
        return state + 1;                      // 返回新状态
    }
);
```

这里有个很严格的约束：**每次调用只能调一次 `sink.next()`**，源码里是这么强制的：

```java
public void next(T t) {
    if (hasValue) {
        error(new IllegalStateException("More than one call to onNext"));
        return;
    }
    ...
}
```

**为什么这么严格？** `FluxGenerate` 是同步生成器，它的设计前提是"每调一次生成一个值"，这样才能跟背压协议对应上——下游请求几个，就调几次 generator 函数。**如果允许一次调用产生多个值，背压协议就直接失效了**——下游明明只要了 1 个，你却硬塞给它 3 个，这就跟前面讲的"顾客点一份菜，后厨直接端上三份"是一个道理，会破坏整个协议的信任基础。

⚠️ **踩坑提醒**：新手写 `Flux.generate` 时最容易犯的错误就是在一次回调里连续调用两次 `sink.next()`，这会直接触发 `IllegalStateException`。如果你需要一次产生多个值，应该用 `Flux.create` 而不是 `Flux.generate`。

---

## 三、异步源：数据是"现点现做"的

异步源跟同步源最大的区别是：**数据的产生时机不由 Reactor 的发射循环决定，而是由外部事件（用户代码、定时器、网络回调等）触发的**。这就像现炒菜——你点单之后，厨师什么时候把菜炒好端出来，取决于炉火、备料等各种外部因素，不是你说要就立刻有。

### 3.1 FluxCreate——最灵活的"开放式厨房"

`Flux.create(sink -> { ... })` 是 Reactor 里最灵活的数据源。它会把一个 `FluxSink<T>` 对象交给用户代码，用户可以在**任意线程、任意时机**调用 `sink.next()` 发射数据——就像顾客直接拿着一个对讲机，随时可以喊"上菜"。

```java
Flux.create(sink -> {
    someAsyncApiWithCallback(result -> {
        sink.next(result);     // 回调触发时，随时喊"上菜"
        sink.complete();
    });
});
```

`subscribe` 的核心流程：

```java
public void subscribe(CoreSubscriber<? super T> actual) {
    BaseSink<T> sink = createSink(wrapped, backpressure);
    wrapped.onSubscribe(sink);
    try {
        source.accept(sink);   // 把 sink 交给用户代码，用户开始"随时喊上菜"
    }
    catch (Throwable ex) {
        sink.error(...);
    }
}
```

**这里最关键的问题来了：既然用户可以在任意时机、甚至任意线程调用 `sink.next()`，那如果生产速度远超消费速度，多出来的数据该怎么办？** 这就是 `FluxCreate` 提供 **5 种溢出策略（OverflowStrategy）** 的原因。

#### 用水龙头类比理解 5 种溢出策略

想象背压是一个水盆接水龙头的水。下游消费的速度好比水盆排水的速度，上游生产的速度好比水龙头的出水速度。如果水龙头出水比排水快，多余的水该怎么处理？五种策略给出五种截然不同的答案：

```java
static <T> BaseSink<T> createSink(CoreSubscriber<T> t, OverflowStrategy backpressure) {
    switch (backpressure) {
        case IGNORE: return new IgnoreSink<>(t);
        case ERROR:  return new ErrorAsyncSink<>(t);
        case DROP:   return new DropAsyncSink<>(t);
        case LATEST: return new LatestAsyncSink<>(t);
        default:     return new BufferAsyncSink<>(t, Queues.SMALL_BUFFER_SIZE);
    }
}
```

| 策略 | 水龙头类比 | 实际行为 | 适用场景 |
|---|---|---|---|
| **IGNORE** | 水龙头全开，压根没接水盆，水漫金山也不管 | 完全无视下游的请求量，直接硬推数据给下游 | 极少用，几乎总是有风险 |
| **ERROR** | 水盆装满了直接报警，不许再接了 | 请求配额用完时，直接抛 `OverflowException` 终止流 | 需要"宁可报错也不要数据错乱"的严格场景 |
| **DROP** | 水盆满了，多出来的水直接从旁边流走，不管了 | 请求配额用完时，新数据直接丢弃，流继续正常运行 | 类似"实时行情推送"，丢几条无所谓，能拿到最新的就行 |
| **LATEST** | 只在盆边放一个杯子，永远只留最后接的那一口水 | 只保留最新的一个值，旧值被丢弃并覆盖 | 需要"只关心当前最新状态"的场景，比如进度条更新 |
| **BUFFER（默认）**| 发现水盆要满了，赶紧换一个更大的盆接着 | 用无界队列把溢出的数据全部缓存起来，等下游腾出配额慢慢发 | 默认策略，但要小心——盆可以无限换大，最终可能内存爆掉 |

我们逐个看它们的实现细节：

**IGNORE——最危险的策略**

```java
static final class IgnoreSink<T> extends BaseSink<T> {
    public FluxSink<T> next(T t) {
        ...
        actual.onNext(t);   // 直接推，完全不管下游有没有请求
        // 之后象征性地扣一下配额，但已经晚了
        ...
    }
}
```

它直接调用 `actual.onNext(t)`，完全无视下游到底请求了多少。虽然后面它也会"扣一下"已知的请求量做记账，但这只是"事后补账"，实际上下游已经被硬塞了数据。

⚠️ **踩坑提醒**：如果下游用了 `take(3)` 这种基于请求量的限流操作符，`IGNORE` 策略会导致下游收到远超 3 条的数据。虽然 `take` 最终会 cancel 上游，但在 cancel 生效之前，已经被 `onNext` 推送出去的数据是收不回来的——就像水已经泼出去了。**除非你能百分之百确定下游一定能跟上生产速度，否则不要用 IGNORE。**

**DROP 和 ERROR——共享同一套逻辑骨架**

```java
static abstract class NoOverflowBaseAsyncSink<T> extends BaseSink<T> {
    public final FluxSink<T> next(T t) {
        if (requestedFromDownstream() != 0) {
            actual.onNext(t);       // 还有配额，正常发
            produced(this, 1);
        }
        else {
            onOverflow();            // 没配额了，交给子类决定怎么办
            Operators.onDiscard(t, ctx);
        }
        return this;
    }
    abstract void onOverflow();
}
```

- `DropAsyncSink.onOverflow()` 是空实现——没配额就直接丢弃，什么都不做。
- `ErrorAsyncSink.onOverflow()` 会调用 `error(Exceptions.failWithOverflow())`——没配额就直接把整条流判"死刑"。

**BUFFER——默认策略，也是最容易踩坑的策略**

```java
static final class BufferAsyncSink<T> extends BaseSink<T> {
    final Queue<T> queue;

    BufferAsyncSink(CoreSubscriber<? super T> actual, int capacityHint) {
        super(actual);
        this.queue = Queues.<T>unbounded(capacityHint).get();  // 无界队列！
    }

    public FluxSink<T> next(T t) {
        queue.offer(t);
        drain();
        return this;
    }
}
```

⚠️ **踩坑提醒**：这是新手最容易掉进去的坑——`Flux.create` 不指定 `OverflowStrategy` 时，默认用的就是 `BUFFER`，而这个缓冲队列是**无界的**！如果你的生产速度长期、持续地超过消费速度（比如上游是一个飞快的消息队列，下游是一个很慢的数据库写入），这个队列会无限增长，最终导致 OOM。**生产代码里如果用 `Flux.create`，强烈建议显式指定一个有界的溢出策略（比如 `DROP` 或 `LATEST`），而不是依赖默认的无界 `BUFFER`。**

**LATEST——只要最新的一口水**

```java
static final class LatestAsyncSink<T> extends BaseSink<T> {
    final AtomicReference<T> queue;

    public FluxSink<T> next(T t) {
        T old = queue.getAndSet(t);   // 换新水，把旧水倒掉
        Operators.onDiscard(old, ctx);
        drain();
        return this;
    }
}
```

只保留一个"容量为 1"的容器，每次新值来了就把旧值顶替掉。适合"我只关心最新状态，中间过程无所谓"的场景，比如实时进度、传感器最新读数。

#### PUSH_ONLY vs PUSH_PULL：为什么有的场景需要 SerializedFluxSink

`FluxCreate` 有两种模式：

```java
enum CreateMode { PUSH_ONLY, PUSH_PULL }
```

`PUSH_ONLY` 模式假设用户只在单个线程调用 `sink`；但 `PUSH_PULL` 模式下，`onRequest` 回调可能在请求线程执行，而 `next()` 又在生产者线程执行——**这就是两个线程同时在摸同一个 sink**，必须加线程安全保护，这就是 `SerializedFluxSink` 的用途：

```java
public FluxSink<T> next(T t) {
    if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
        // 抢到了"发言权"，直接发
        sink.next(t);
        ...
    }
    else {
        // 没抢到，先排队，别人发完了会帮你处理
        this.mpscQueue.offer(t);
        ...
    }
    drainLoop();
    return this;
}
```

这是一个经典的"抢锁或排队"模式：能抢到就直接发，抢不到就把数据丢进一个多生产者单消费者（MPSC）队列排队，由正在发送的那个线程顺便帮你处理掉，而不是让抢不到的线程傻等自旋——避免了高并发下的 CPU 空转浪费。

### 3.2 FluxInterval——按时打卡的"闹钟厨房"

`Flux.interval(Duration.ofSeconds(1))` 每隔一段时间发射一个递增的 long 值，常用来做定时任务或者心跳。它的核心逻辑：

```java
static final class IntervalRunnable implements Runnable, Subscription, InnerProducer<Long> {
    public void run() {
        if (!cancelled) {
            if (requested != 0L) {
                actual.onNext(count++);
                ...
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

**注意：`FluxInterval` 只支持一种"溢出策略"——直接报错，没有可选项。**

⚠️ **踩坑提醒**：这跟 `FluxCreate` 不一样，`FluxInterval` 是"定时闹钟"模式，你没法给它配置 `OverflowStrategy`。如果下游消费跟不上节奏（比如你在 `Flux.interval(Duration.ofMillis(10))` 后面接了一个耗时的数据库操作，又没有做任何异步调度），流会直接报 `OverflowException` 崩掉。

**为什么 `FluxInterval` 不支持缓冲策略？** 因为 interval 的语义是"周期性地告诉你现在是第几个时间点"，如果允许缓冲，就意味着队列里堆积着一堆"过时的时间点"——这些堆积的旧 tick 对业务毫无意义（你真正想要的是"现在的时间"，而不是"5 秒前排队没处理完的时间"）。与其缓冲一堆没用的数据把内存撑爆，不如直接报错让你意识到"消费能力跟不上，需要调整设计"。

---

## 四、空源与特殊源：什么都不产生，或者只产生一件事

### 4.1 FluxEmpty / MonoEmpty——空盘子

```java
final class FluxEmpty extends Flux<Object>
        implements Fuseable.ScalarCallable<Object>, SourceProducer<Object> {
    private static final Flux<Object> INSTANCE = new FluxEmpty();

    public void subscribe(CoreSubscriber<? super Object> actual) {
        Operators.complete(actual);
    }

    public @Nullable Object call() throws Exception {
        return null;   // 标量优化：没有值
    }
}
```

`Flux.empty()` 和 `Mono.empty()` 都是**单例**——因为空源没有任何状态需要区分不同实例，没必要每次调用都 new 一个新对象出来，直接复用一个全局单例即可，省掉了不必要的内存分配。

### 4.2 FluxNever——一份永远不会上桌的菜

```java
final class FluxNever extends Flux<Object> implements SourceProducer<Object> {
    public void subscribe(CoreSubscriber<? super Object> actual) {
        actual.onSubscribe(Operators.emptySubscription());
        // 仅此而已，onNext、onComplete、onError 永远不会被调用
    }
}
```

这是个特殊用途的源——只完成"点菜"这个动作，但永远不上菜、永远不说结束、也永远不报错。主要用在测试场景，或者需要一个"占位符"表示"这里故意什么都不发生"的地方。

### 4.3 FluxError——开门就说"今天不营业"

```java
final class FluxError<T> extends Flux<T> implements Fuseable.ScalarCallable, SourceProducer<T> {
    final Throwable error;

    public void subscribe(CoreSubscriber<? super T> actual) {
        Operators.error(actual, error);   // 订阅了就立刻报错
    }
}
```

`Flux.error(exception)` 在被订阅的那一刻就立即报错，不会有任何 `onNext`。它同样实现了 `ScalarCallable`，只不过 `call()` 方法是直接抛异常——这让 `Flux.error(e).map(f)` 这样的链条也能走前面提到的"标量优化"路径，直接在订阅之前就判定这条链一定会报错，省去构建整条 Subscriber 链的开销。

---

## 五、延迟源：FluxDefer——"等你真点单了我才决定做什么"

这是一个非常实用、也非常容易被误用的数据源。先看一个真实的生产环境常见 bug：

```java
// 错误写法：查询在这一行就已经执行了！
Mono<User> user = userRepository.findById(userId);
return user.flatMap(u -> renderPage(u));
```

如果 `userRepository.findById` 底层是某种"立即执行"的实现（Hot Publisher），这行代码在**创建 Mono 的时候**查询就已经发出去了，而不是等到真正被订阅（也就是被消费）的时候才执行。这在某些场景下会导致完全出乎意料的行为——比如你原本期望"每次订阅都查一次最新数据"，结果查询只在创建时执行了一次，后续多次订阅拿到的都是同一份缓存结果。

`FluxDefer` 就是用来解决这个问题的：

```java
final class FluxDefer<T> extends Flux<T> implements SourceProducer<T> {
    final Supplier<? extends Publisher<? extends T>> supplier;

    public void subscribe(CoreSubscriber<? super T> actual) {
        Publisher<? extends T> p;
        try {
            p = Objects.requireNonNull(supplier.get(), "...");
        }
        catch (Throwable e) {
            Operators.error(actual, ...);
            return;
        }
        from(p).subscribe(actual);
    }
}
```

正确的写法应该是：

```java
Mono<User> user = Mono.defer(() -> userRepository.findById(userId));  // 订阅时才真正执行
return user.flatMap(u -> renderPage(u));
```

**`FluxDefer` 做的事情很简单：把"生成 Publisher 的逻辑"包在一个 `Supplier` 里，直到真正被订阅（`subscribe` 被调用）的那一刻，才去执行这个 `Supplier` 拿到真正的 Publisher，然后转手把订阅请求交给它。**

打个比方：正常的 `Mono.just(x)` 就像是提前把菜做好放在保温柜里；而 `Mono.defer(() -> ...)` 就像是"你不点单，厨房绝对不会提前动手"——保证了每次点单（订阅）都是一次全新的、独立的下厨过程。

⚠️ **踩坑提醒**：这在**重试（retry）场景**下特别重要。如果你的 Publisher 不是通过 `defer` 包装的，重试时复用的其实还是"第一次执行时创建的那个 Publisher"，而不是重新执行一遍创建逻辑——对于一个 HTTP 请求，这意味着重试根本没有发起新的请求，而是在检查一个早就过期的结果。正确的写法永远是 `Mono.defer(() -> httpClient.get(url))`，而不是 `Mono.just(httpClient.get(url))` 或者直接拿一个已经创建好的 Mono 反复 retry。

---

## 六、归纳总结表

### 6.1 数据源类型对照表

| 源类名 | 对应 API | 发射数量 | 支持融合 | 是否异步 | 典型场景 |
|---|---|---|---|---|---|
| `FluxArray` | `Flux.just(1,2,3)` | 0~N | 是（SYNC） | 否 | 固定的几个值 |
| `FluxRange` | `Flux.range(1, 10)` | 0~N | 是（SYNC） | 否 | 连续整数序列 |
| `MonoJust` | `Mono.just("x")` | 恰好 1 | 是（标量优化） | 否 | 单值场景 |
| `FluxJust` | `Flux.just("x")` | 恰好 1 | 是（标量优化） | 否 | 单值场景 |
| `FluxEmpty` / `MonoEmpty` | `Flux.empty()` | 0 | 是（标量优化） | 否 | 表示"没有数据" |
| `FluxNever` | `Flux.never()` | 0（永不完成） | 否 | 否 | 测试占位符 |
| `FluxError` | `Flux.error(ex)` | 0（立即报错） | 是（标量优化） | 否 | 表示"已知会失败" |
| `FluxIterable` | `Flux.fromIterable(list)` | 0~N | 是（SYNC） | 否 | 包装已有集合 |
| `FluxGenerate` | `Flux.generate(sink -> ...)` | 0~N | 是（SYNC） | 否 | 精细控制的同步生成 |
| `FluxCreate` | `Flux.create(sink -> ...)` | 0~N | 否 | 是 | 对接回调式 API |
| `FluxInterval` | `Flux.interval(Duration...)` | 0~∞ | 否 | 是 | 定时任务、心跳 |
| `FluxDefer` | `Flux.defer(() -> ...)` | 取决于内部源 | 取决于内部源 | 取决于内部源 | 延迟执行、重试场景 |

### 6.2 FluxCreate 五种溢出策略速查

| 策略 | 水龙头类比 | 会不会丢数据 | 会不会内存暴涨 | 会不会报错终止 | 推荐场景 |
|---|---|---|---|---|---|
| IGNORE | 水漫金山，完全不接 | 不丢，但下游可能被硬灌爆 | 视下游而定 | 不会 | 几乎不推荐使用 |
| ERROR | 水满就报警 | 会丢并终止 | 不会 | 会 | 严格场景，宁可失败不要错乱 |
| DROP | 满了就往外流失 | 会丢 | 不会 | 不会 | 高频、可容忍丢弃的场景（如实时行情）|
| LATEST | 只留最后一口 | 旧值全丢 | 不会 | 不会 | 只关心最新状态（如进度、传感器）|
| BUFFER（默认） | 换更大的盆接着 | 不丢 | **会**（无界队列） | 不会 | 谨慎使用，建议显式换成有界策略 |

### 6.3 关键设计决策总结

| 设计决策 | 解决了什么问题 | 如果没有这个设计会怎样 |
|---|---|---|
| ConditionalSubscriber 分支 | 避免 filter 等操作符的"发了又要回来"往返开销 | 每次过滤掉的值都要多一次 request 往返 |
| fastPath / slowPath 分离 | 无界请求时跳过 CAS 计数 | 大数组/大范围场景下性能明显下降 |
| ScalarCallable 接口 | 单值源可以跳过整条订阅链直接拿值 | `Mono.just(x).map(f)` 白白创建不必要的对象 |
| FluxGenerate 的"一次一值"约束 | 保证背压协议不被破坏 | 一次生成多个值会导致下游收到超量数据 |
| FluxDefer 的懒加载 | 保证每次订阅都重新执行创建逻辑 | 重试、多次订阅拿到的都是同一份过期结果 |
| FluxInterval 的强制报错策略 | 避免堆积一堆没有意义的过期 tick | 队列无限增长，OOM 风险 |
| SerializedFluxSink 的排队机制 | 避免多线程并发写 sink 时的自旋等待 | 高并发下 CPU 空转、延迟飙升 |

一句话总结这一篇：**选对数据源，就是选对"数据到底该怎么来"这件事的成本和风险。** 同步源几乎零成本，但只适合数据已知的场景；异步源灵活但需要你自己想清楚溢出策略，尤其 `Flux.create` 默认的无界缓冲是生产环境最容易埋雷的地方；延迟源解决的是"什么时候执行"这个经常被忽视但代价高昂的问题。下一篇我们会讲操作符——数据从源头出来之后，是怎么被 map、filter、flatMap 这些操作符一步步加工的。
