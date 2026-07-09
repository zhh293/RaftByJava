# Scannable 诊断与可观测性（易懂版）

> **Reactor Core 源码解析系列 · 第 14 篇 · 易懂版**
> 用"给操作符链拍 X 光片"的类比，把 Reactor 的诊断与可观测性机制从头到尾讲清楚。

---

## 一、从一个调试噩梦说起

你上线了一个 Reactor 应用，突然收到告警——某个接口报了 `NullPointerException`。你打开错误日志，看到的是这样的堆栈：

```
java.lang.NullPointerException
    at reactor.core.publisher.FluxMap$MapSubscriber.onNext(FluxMap.java:120)
    at reactor.core.publisher.FluxFilter$FilterSubscriber.onNext(FluxFilter.java:113)
    at reactor.core.publisher.FluxFlatMap$FlatMapMain.onNext(FluxFlatMap.java:425)
    at reactor.core.publisher.FluxPublishOn$PublishOnSubscriber.onNext(FluxPublishOn.java:180)
    ...
```

你的第一反应是去 `FluxMap.java:120` 看代码——但那是 Reactor 的源码，不是你的业务代码。你根本不知道是**你的哪一行代码**装配了这个 `map` 操作符，也不知道数据流经了哪些操作符才到这里。

你的整个 Reactor 操作符链在运行时是一串嵌套的 `Subscriber` 对象，正常情况下你完全看不到内部状态——它就像一个黑盒。

你需要一台 **X 光机**，能穿透黑盒，看到操作符链的内部结构：这个操作符缓存了多少元素？是否已取消？上游是谁？运行在哪个线程上？

`Scannable` 就是这台 X 光机。

---

## 二、Scannable 是什么：Reactor 的 X 光机

### 2.1 一句话理解

`Scannable` 是 Reactor 的内省（introspection）接口。每个操作符都实现了它，你可以通过它查询操作符的内部状态——就像给操作符链拍 X 光片，看到骨骼结构。

### 2.2 核心方法：scanUnsafe

```java
@FunctionalInterface
public interface Scannable {
    
    @Nullable Object scanUnsafe(Attr key);
    
    // ... 大量 default 方法 ...
}
```

它只有一个抽象方法 `scanUnsafe(Attr key)`。你传入一个 `Attr`（属性键），它返回对应的属性值。

**为什么叫 `scanUnsafe`？** 因为它返回 `@Nullable Object`，调用者需要自己处理类型转换。安全的替代方案是 `scan(Attr<T>)` 方法：

```java
default <T> @Nullable T scan(Attr<T> key) {
    T value = key.tryConvert(scanUnsafe(key));
    if (value == null) return key.defaultValue();
    return value;
}
```

`scan()` 方法会自动处理 null 值（返回默认值）和类型转换，推荐日常使用。

### 2.3 Attr：X 光片上的检查项目

`Attr<T>` 是类型化的属性键，每个 `Attr` 代表 X 光片上的一个"检查项目"。以下是 Reactor 预定义的所有检查项目：

```java
// 上游是谁？
Scannable parent = subscriber.scan(Attr.PARENT);

// 下游是谁？
Scannable actual = subscriber.scan(Attr.ACTUAL);

// 当前缓存了多少元素？
int buffered = subscriber.scan(Attr.BUFFERED);

// 最大容量是多少？
int capacity = subscriber.scan(Attr.CAPACITY);

// 是否已取消？
boolean cancelled = subscriber.scan(Attr.CANCELLED);

// 是否已终止？
boolean terminated = subscriber.scan(Attr.TERMINATED);

// 当前错误是什么？
Throwable error = subscriber.scan(Attr.ERROR);

// 预取量是多少？
int prefetch = subscriber.scan(Attr.PREFETCH);

// 下游还有多少未满足的 request？
long requested = subscriber.scan(Attr.REQUESTED_FROM_DOWNSTREAM);

// 运行在哪个 Scheduler 上？
Scannable runOn = subscriber.scan(Attr.RUN_ON);

// 运行模式是什么？
Attr.RunStyle runStyle = subscriber.scan(Attr.RUN_STYLE);

// 用户定义的名称是什么？
String name = subscriber.scan(Attr.NAME);

// 是否延迟错误？
boolean delayError = subscriber.scan(Attr.DELAY_ERROR);
```

用一个表格整理：

| Attr 名称 | 类型 | 含义 | X 光类比 |
|-----------|------|------|---------|
| `PARENT` | Scannable | 上游组件引用 | 看上一节脊椎骨 |
| `ACTUAL` | Scannable | 下游组件引用 | 看下一节脊椎骨 |
| `BUFFERED` | Integer | 当前缓冲区大小 | 看胃里有多少食物 |
| `CAPACITY` | Integer | 最大缓冲容量 | 看胃有多大 |
| `CANCELLED` | Boolean | 是否已取消 | 看是否已经"断电" |
| `TERMINATED` | Boolean | 是否已终止 | 看是否已经"停机" |
| `ERROR` | Throwable | 当前错误 | 看哪里"发炎"了 |
| `PREFETCH` | Integer | 预取数量 | 看一次能吃多少 |
| `REQUESTED_FROM_DOWNSTREAM` | Long | 下游未满足的 request | 看下游"点了多少菜" |
| `RUN_ON` | Scannable | 运行的 Scheduler | 看在哪个"手术室" |
| `RUN_STYLE` | RunStyle | 运行模式 | 看是同步还是异步 |
| `NAME` | String | 用户定义名称 | 看病人"叫什么名字" |
| `TAGS` | Stream | 标签集合 | 看"病历标签" |
| `DELAY_ERROR` | Boolean | 是否延迟错误 | 看是否"带病坚持工作" |

### 2.4 RunStyle：三种运行模式

```java
public enum RunStyle {
    UNKNOWN,   // 没有保证（默认值，最弱保证级别）
    ASYNC,     // 操作符可能切换线程
    SYNC;      // 操作符保证不切换线程（最强保证级别）
}
```

`RunStyle` 的 `ordinal()` 反映了保证级别：`UNKNOWN(0) < ASYNC(1) < SYNC(2)`。这个设计允许下游根据上游的 RunStyle 进行优化决策——如果整个链路都是 `SYNC`，可以避免不必要的线程安全措施。

大多数操作符返回 `SYNC`（如 `FluxMap`、`FluxFilter`），表示它们不引入线程切换。`FluxPublishOn`、`ParallelRunOn` 返回 `ASYNC`，表示它们会切换线程。

---

## 三、导航方法：沿着操作符链上下走

### 3.1 操作符链的结构

Reactor 的操作符链在运行时是一串嵌套的 `Subscriber` 对象，通过 `PARENT`（上游）和 `ACTUAL`（下游）互相引用：

```
Source(Flux.just)  ←──PARENT──  map()  ←──PARENT──  filter()  ←──PARENT──  Subscriber
     │                           │                     │                      │
     │──ACTUAL──▶               │──ACTUAL──▶          │──ACTUAL──▶           │
```

### 3.2 parents() —— 向上游走

```java
default Stream<? extends Scannable> parents() {
    return Attr.recurse(this, Attr.PARENT);
}
```

沿着 `PARENT` 链向上游遍历。这是一个惰性求值的 Stream，每次 `next()` 时跟随 `PARENT` 前进一步：

```java
subscriber.parents().forEach(s -> 
    System.out.println(s.stepName()));
// 输出：filter, map, just
```

### 3.3 actuals() —— 向下游走

```java
default Stream<? extends Scannable> actuals() {
    return Attr.recurse(this, Attr.ACTUAL);
}
```

与 `parents()` 完全对称，沿 `ACTUAL` 链向下游遍历。

### 3.4 steps() —— 拍一张完整的 X 光片

`steps()` 是最常用的诊断方法，它把整条操作符链的操作符名称按顺序列出来：

```java
subscriber.steps().forEach(System.out::println);
// 输出：just, map, filter, subscribe
```

内部实现是：先收集所有 parent（向上），反转使其成为从源到当前的顺序，加上自己，再收集所有 actual（向下）。

### 3.5 stepName() —— 从类名提取操作符名称

```java
default String stepName() {
    String name = getClass().getName();        // reactor.core.publisher.FluxMap$MapSubscriber
    int innerClassIndex = name.indexOf('$');
    if (innerClassIndex != -1) {
        name = name.substring(0, innerClassIndex);  // reactor.core.publisher.FluxMap
    }
    int stripPackageIndex = name.lastIndexOf('.');
    if (stripPackageIndex != -1) {
        name = name.substring(stripPackageIndex + 1);  // FluxMap
    }
    // 用正则去掉噪声词
    String stripped = OPERATOR_NAME_UNRELATED_WORDS_PATTERN
        .matcher(name).replaceAll("");  // Map
    return stripped.substring(0, 1).toLowerCase() + stripped.substring(1);  // map
}
```

`OPERATOR_NAME_UNRELATED_WORDS_PATTERN` 的定义：

```java
Pattern OPERATOR_NAME_UNRELATED_WORDS_PATTERN =
    Pattern.compile("Parallel|Flux|Mono|Publisher|Subscriber|Fuseable|Operator|Conditional");
```

转换示例：
- `FluxMap` → 去掉 `Flux` → `Map` → `map`
- `FluxPublishOn` → 去掉 `Flux` → `PublishOn` → `publishOn`
- `ParallelMap` → 去掉 `Parallel` → `Map` → `map`
- `FluxFlatMap` → 去掉 `Flux` → `FlatMap` → `flatMap`

### 3.6 name() —— 查找用户定义的名称

```java
default String name() {
    String thisName = this.scan(Attr.NAME);  // 先看自己有没有名字
    if (thisName != null) return thisName;
    return parents()                         // 向上找第一个有名字的
        .map(s -> s.scan(Attr.NAME))
        .filter(Objects::nonNull)
        .findFirst()
        .orElseGet(this::stepName);          // 都没有就用类名
}
```

优先返回通过 `Flux.name("xxx")` 设置的名称，如果没有就向上遍历找，都没有就回退到 `stepName()`。

### 3.7 两个哨兵值

```java
static final Scannable UNAVAILABLE_SCAN = new Scannable() {
    public Object scanUnsafe(Attr key) { return null; }
    public boolean isScanAvailable() { return false; }
    public String stepName() { return "UNAVAILABLE_SCAN"; }
};

static final Scannable NULL_SCAN = new Scannable() {
    public Object scanUnsafe(Attr key) { return null; }
    public boolean isScanAvailable() { return false; }
    public String stepName() { return "NULL_SCAN"; }
};
```

- **`UNAVAILABLE_SCAN`**：对象存在但不是 Scannable（比如第三方库的操作符）。X 光片上显示"这里有东西，但我看不懂"。
- **`NULL_SCAN`**：对象不存在，链路到头了。X 光片上显示"这里是末端"。

两者都返回 `isScanAvailable() == false`，终止链路遍历。

**为什么需要两个不同的哨兵？** 虽然行为相同，但语义不同。在调试时，`UNAVAILABLE_SCAN` 告诉你"这里有一个第三方组件但我看不懂"，而 `NULL_SCAN` 告诉你"这里没有组件，链路到头了"。

---

## 四、操作符怎么实现 Scannable：if-chain 模式

每个操作符都实现了 `scanUnsafe()`，暴露自己的内部状态。以 `PublishSubscriber` 为例：

```java
@Override
public @Nullable Object scanUnsafe(Attr key) {
    if (key == Attr.PARENT) return s;                    // 上游 Subscription
    if (key == Attr.PREFETCH) return prefetch;            // 预取量
    if (key == Attr.ERROR) return error;                  // 当前错误
    if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;  // 缓冲区大小
    if (key == Attr.TERMINATED) return isTerminated();    // 是否终止
    if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();  // 是否取消
    if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;  // 运行模式
    return null;  // 不认识的属性返回 null
}
```

**为什么不用 `Map<Attr, Object>` 而用 if-chain？**

1. `Attr` 的种类有限（约 15 个），if-chain 的性能与 HashMap 相当。
2. 很多属性的值是**动态计算**的（如 `queue.size()`），不能预存在 Map 里。
3. 避免了 HashMap 的内存开销——每个操作符都会创建大量实例，如果每个都带一个 HashMap，内存浪费严重。

---

## 五、checkpoint()：给操作符链贴标签

### 5.1 为什么需要 checkpoint

回到开头的调试噩梦——异常堆栈只显示 Reactor 内部类名，看不到你的业务代码位置。`checkpoint()` 就是在操作符链上"贴标签"，当错误发生时，标签会出现在错误信息中，帮你快速定位。

### 5.2 两种模式

**轻量级模式**（只传描述文本）：

```java
flux.map(this::transform)
    .checkpoint("after-transform")  // 轻量级：只记录描述
    .filter(this::validate)
    .checkpoint("after-filter")
    .subscribe();
```

错误发生时输出：
```
Error has been observed at the following site(s):
    *__checkpoint ⇢ after-transform
    |_ filter ⇢ ...
    *__checkpoint ⇢ after-filter
```

轻量级模式**不捕获堆栈**，成本极低，但需要你提供足够具体的描述。

**重量级模式**（捕获堆栈）：

```java
flux.map(this::transform)
    .checkpoint()  // 重量级：捕获完整堆栈
    .subscribe();
```

错误发生时输出：
```
Error has been observed at the following site(s):
    *__checkpoint ⇢ com.example.MyService.process(MyService.java:42)
```

重量级模式**捕获堆栈**，提供精确的调用位置，但成本较高（创建异常对象来获取堆栈）。

**混合模式**（描述 + 堆栈）：

```java
flux.map(this::transform)
    .checkpoint("after-transform", true)  // 描述 + 强制堆栈
    .subscribe();
```

### 5.3 底层实现：AssemblySnapshot

`checkpoint()` 的核心是 `AssemblySnapshot`，它有三个子类代表不同的快照模式：

| 子类 | isCheckpoint | 堆栈捕获 | 用途 |
|------|-------------|---------|------|
| `CheckpointLightSnapshot` | true | 否 | `checkpoint("desc")` |
| `CheckpointHeavySnapshot` | true | 是（延迟） | `checkpoint()` / `checkpoint("desc", true)` |
| `MethodReturnSnapshot` | false | 否 | 方法返回标记 |

**延迟求值是关键设计**：`CheckpointHeavySnapshot` 的堆栈信息通过 `Supplier<String>` 延迟获取，只在错误实际发生时才调用。在正常执行路径上，`checkpoint()` 的开销接近于零。

### 5.4 OnAssemblyException：错误增强

当错误发生时，`OnAssemblySubscriber.fail()` 方法将装配信息注入到异常中：

```java
final Throwable fail(Throwable t) {
    // 在异常的 suppressed exceptions 中查找已有的 OnAssemblyException
    OnAssemblyException onAssemblyException = null;
    for (Throwable e : t.getSuppressed()) {
        if (e instanceof OnAssemblyException) {
            onAssemblyException = (OnAssemblyException) e;
            break;
        }
    }
    
    if (onAssemblyException == null) {
        // 没有就创建一个新的，附加到原始异常
        onAssemblyException = new OnAssemblyException(description);
        t = Exceptions.addSuppressed(t, onAssemblyException);
    }
    
    // 将当前操作符的装配信息添加到树形结构中
    onAssemblyException.add(parent, current, snapshotStack);
    return t;
}
```

核心流程：
1. 在异常的 suppressed exceptions 中查找已有的 `OnAssemblyException`。
2. 如果没有，创建一个新的并附加到原始异常。
3. 调用 `onAssemblyException.add()` 将当前操作符的装配信息添加到树形结构中。

**为什么用树形结构而不是列表？** 因为 `flatMap` 等分支操作符会产生多条并行子流，错误可能在任一子流中发生。树形结构能准确表达错误传播的分叉路径。

### 5.5 为什么 OnAssemblyException 不填充堆栈？

```java
@Override
public Throwable fillInStackTrace() {
    return this;  // 不填充堆栈
}
```

因为它的"堆栈"信息来自装配时捕获的快照，而不是运行时调用栈。`fillInStackTrace()` 是 Java 异常创建时最昂贵的操作之一（需要遍历整个调用栈），覆盖它使其返回 `this` 可以避免这个开销。

⚠️ **踩坑提醒**：不要在生产环境全局开启 `Hooks.onOperatorDebug()`——它会对**所有**操作符捕获堆栈，性能开销巨大。只在开发/测试环境使用，或者用 `checkpoint()` 精确地在关键位置标记。

```java
// 开发环境可以用（全局开启）
Hooks.onOperatorDebug();

// 生产环境用这个（精确标记）
flux.checkpoint("critical-section");
```

---

## 六、tap() 操作符：不修改流的旁观者

### 6.1 什么是 tap

`tap()` 是一个特殊的操作符，它让你在不修改数据流的情况下观测所有信号。你可以把它想象成在管道上装了一个"透明观察窗"——数据正常流过，但你能看到里面发生了什么。

```java
flux.tap(SignalListenerFactory -> {
    // 可以观测：onSubscribe, onNext, onComplete, onError, 
    //          onRequest, onCancel, doFinally...
    // 但不修改任何数据
});
```

### 6.2 SignalListener：全信号监听

`SignalListener` 是 `doOnXxx` 系列副作用操作符的统一抽象。它覆盖了所有可能的信号类型：

```java
public interface SignalListener<T> {
    void doFirst() throws Throwable;                           // subscribe 时（最先）
    void doOnSubscription() throws Throwable;                  // 收到 Subscription 时
    void doOnFusion(int negotiatedFusion) throws Throwable;    // 融合协商后
    void doOnRequest(long requested) throws Throwable;         // 收到 request 时
    void doOnCancel() throws Throwable;                        // 收到 cancel 时
    void doOnNext(T value) throws Throwable;                   // 收到数据时（传播前）
    void doOnComplete() throws Throwable;                      // 收到完成时（传播前）
    void doOnError(Throwable error) throws Throwable;          // 收到错误时（传播前）
    void doAfterComplete() throws Throwable;                   // 完成传播后
    void doAfterError(Throwable error) throws Throwable;       // 错误传播后
    void doFinally(SignalType terminationType) throws Throwable; // 最终清理（传播后）
    void doOnMalformedOnNext(T value) throws Throwable;        // 终止后还收到 onNext
    void doOnMalformedOnError(Throwable error) throws Throwable; // 终止后还收到 onError
    void doOnMalformedOnComplete() throws Throwable;           // 终止后还收到 onComplete
    void handleListenerError(Throwable listenerError);         // 上述任何方法抛异常时
}
```

用一个表格整理信号生命周期：

| 阶段 | 方法 | 调用时机 | 对应 doOnXxx |
|------|------|---------|-------------|
| 订阅启动 | `doFirst()` | subscribe 时（最先） | `doFirst()` |
| 收到 Subscription | `doOnSubscription()` | onSubscribe 后 | `doOnSubscribe()` |
| 融合协商 | `doOnFusion(int)` | onSubscribe 期间 | 无对应操作符 |
| 数据推送 | `doOnNext(T)` | 传播前 | `doOnNext()` |
| 请求 | `doOnRequest(long)` | 传播前 | `doOnRequest()` |
| 取消 | `doOnCancel()` | 传播前 | `doOnCancel()` |
| 正常完成 | `doOnComplete()` | 传播前 | `doOnComplete()` |
| 完成后 | `doAfterComplete()` | 传播后 | `doAfterTerminate()` |
| 错误终止 | `doOnError(Throwable)` | 传播前 | `doOnError()` |
| 错误后 | `doAfterError(Throwable)` | 传播后 | `doAfterTerminate()` |
| 最终清理 | `doFinally(SignalType)` | 最后调用 | `doFinally()` |
| 畸形信号 | `doOnMalformedOnNext/OnError/OnComplete` | 终止后收到信号 | 自动 drop |

**为什么 `handleListenerError` 必须不能抛异常？** 因为它是错误处理的最后一道防线。如果它也抛异常，就会形成无限递归。它的存在是为了让监听器在 `doFinally` 不会被调用的情况下（handler 自身出错时）仍然有机会清理资源。

### 6.3 DefaultSignalListener：空实现基类

```java
public abstract class DefaultSignalListener<T> implements SignalListener<T> {
    int fusionMode = Fuseable.NONE;

    @Override public void doFirst() throws Throwable {}
    @Override public void doFinally(SignalType terminationType) throws Throwable {}
    @Override public void doOnSubscription() throws Throwable {}
    // ... 所有其他方法都是空实现 ...

    @Override
    public void doOnFusion(int negotiatedFusion) throws Throwable {
        this.fusionMode = negotiatedFusion;  // 捕获融合模式
    }
    
    protected int getFusionMode() { return fusionMode; }
}
```

注意 `doOnFusion()` 不是空实现——它捕获了融合模式并通过 `getFusionMode()` 暴露给子类。这让自定义监听器可以根据融合模式调整行为。

### 6.4 SignalListenerFactory：两阶段工厂

```java
public interface SignalListenerFactory<T, STATE> {
    // 第一阶段：Publisher 级别，装配时调用一次
    STATE initializePublisherState(Publisher<? extends T> source);
    
    // 第二阶段：Subscriber 级别，每次 subscribe 调用
    SignalListener<T> createListener(Publisher<? extends T> source,
            ContextView listenerContext, STATE publisherContext);
}
```

**为什么分两个阶段？** 因为某些初始化操作只需要做一次（如解析操作符名称、创建 metric meter），不需要每次 subscribe 都重复。`STATE` 类型参数允许工厂定义自己的共享状态类型。

这就像医院的体检流程：医院整体只需要准备一次体检设备（Publisher 级别），但每个病人来体检时需要创建一份独立的体检报告（Subscriber 级别）。

### 6.5 tap() 的三个重载版本

```java
// 最简单：每次 subscribe 创建新的 SignalListener
public final Flux<T> tap(Supplier<SignalListener<T>> simpleListenerGenerator)

// 带 ContextView：可以访问下游 Context
public final Flux<T> tap(Function<ContextView, SignalListener<T>> listenerGenerator)

// 完整版：使用 SignalListenerFactory
public final Flux<T> tap(SignalListenerFactory<T, ?> listenerFactory)
```

最简单的 `Supplier` 版本内部被适配为 `SignalListenerFactory`：

```java
public final Flux<T> tap(Supplier<SignalListener<T>> simpleListenerGenerator) {
    return tap(new SignalListenerFactory<T, Void>() {
        @Override
        public Void initializePublisherState(Publisher<? extends T> ignored) {
            return null;
        }
        @Override
        public SignalListener<T> createListener(Publisher<? extends T> ignored1,
                ContextView ignored2, Void ignored3) {
            return simpleListenerGenerator.get();
        }
    });
}
```

### 6.6 实际应用：用 tap 实现 Metrics

```java
flux.name("http-requests")
    .tag("uri", "/api/users")
    .tap(new MicrometerMeterListenerFactory<>(meterRegistry))
    .subscribe();
```

`tap()` 是 Micrometer 集成的基础——它让 metrics 采集器能在不修改数据流的情况下观测所有信号（请求量、成功率、延迟等）。

---

## 七、如果没有 Scannable 会怎样

想象一下，如果 Reactor 没有 `Scannable` 机制：

1. **调试噩梦**：错误堆栈只显示 `onError` 的执行路径，不知道操作符链是在哪里装配的。10 个 `map()` 操作符在堆栈中看起来完全一样——都是 `FluxMap$MapSubscriber.onNext`。

2. **无法监控**：不知道每个操作符的缓冲区有多大、请求了多少、是否已取消。Micrometer 集成无从做起。

3. **黑盒操作**：无法遍历操作符链，无法知道数据流经了哪些操作符。`steps()` 的输出在生产问题排查中是无价的。

4. **标签传递断裂**：`Flux.name("http-request").tag("uri", "/api/users")` 无法将标签传递给下游的 metrics 采集器。

`Scannable` 本质上是 Reactor 的**反射 API**——它让响应式流从"只进不出"的黑盒变成了可检查、可监控、可调试的透明管道。

---

## 八、实际使用场景

### 8.1 调试时查看操作符链

```java
// 在 subscribe 之前插入 checkpoint
flux.map(this::transform)
    .filter(this::validate)
    .checkpoint("before-subscribe")
    .subscribe();

// 错误发生时，错误信息中会显示 checkpoint 标签
// 帮你快速定位是哪段操作符链出了问题
```

### 8.2 运行时检查操作符状态

```java
// 通过 Scannable.from() 包装 Subscriber，查询内部状态
Scannable scannable = Scannable.from(subscriber);
if (scannable.isScanAvailable()) {
    int buffered = scannable.scan(Attr.BUFFERED);
    boolean cancelled = scannable.scan(Attr.CANCELLED);
    System.out.println("Buffered: " + buffered + ", Cancelled: " + cancelled);
    
    // 遍历操作符链
    scannable.steps().forEach(step -> 
        System.out.println("  step: " + step));
}
```

### 8.3 用 tap 实现 custom metrics

```java
flux.tap(() -> new DefaultSignalListener<>() {
    private long startTime;
    private int count;
    
    @Override
    public void doOnSubscription() {
        startTime = System.nanoTime();
    }
    
    @Override
    public void doOnNext(T value) {
        count++;
    }
    
    @Override
    public void doFinally(SignalType type) {
        long duration = System.nanoTime() - startTime;
        System.out.printf("Processed %d items in %d ms%n", 
            count, duration / 1_000_000);
    }
});
```

### 8.4 Context 传播

`Scannable` 还支持多播场景下的 Context 合并：

```java
// PublishSubscriber 中
@Override
public Context currentContext() {
    return Operators.multiSubscribersContext(subscribers);
}
```

这确保了多播场景下，上游能看到所有下游 Subscriber 的 Context 的合并视图。

---

## 九、归纳表格

### Attr 属性对照表

| Attr 名称 | 类型 | 默认值 | 含义 | 典型提供者 |
|-----------|------|--------|------|-----------|
| `PARENT` | Scannable | null | 上游组件引用 | 所有操作符 |
| `ACTUAL` | Scannable | null | 下游组件引用 | 所有 InnerOperator |
| `BUFFERED` | Integer | 0 | 当前缓冲区大小 | PublishSubscriber, MergeSequentialInner |
| `CAPACITY` | Integer | 0 | 最大容量 | ReplaySubscriber |
| `CANCELLED` | Boolean | false | 下游是否已取消 | 大多数 Subscriber |
| `TERMINATED` | Boolean | false | 上游是否已终止 | 大多数 Subscriber |
| `ERROR` | Throwable | null | 当前错误 | PublishSubscriber, ReplaySubscriber |
| `PREFETCH` | Integer | 0 | 预取数量 | FluxPublish, ParallelSource |
| `REQUESTED_FROM_DOWNSTREAM` | Long | 0L | 下游未满足的 request | PubSubInner, ReplayInner |
| `RUN_ON` | Scannable | null | 运行的 Scheduler | FluxPublishOn, ReplayInner |
| `RUN_STYLE` | RunStyle | UNKNOWN | 运行模式 | 所有操作符 |
| `NAME` | String | null | 用户定义名称 | FluxName |
| `TAGS` | Stream | null | 标签集合 | FluxName |
| `DELAY_ERROR` | Boolean | false | 是否延迟错误 | FluxFlatMap, ParallelFlatMap |
| `LARGE_BUFFERED` | Long | null | 超大缓冲区大小 | FluxFlatMap, FluxWindow |
| `ACTUAL_METADATA` | Boolean | false | 是否为元数据源 | FluxOnAssembly |
| `LIFTER` | String | null | lift 函数名称 | Operators.lift() |

### 诊断工具对照表

| 工具 | 时机 | 成本 | 信息量 | 适用场景 |
|------|------|------|--------|---------|
| `Scannable.scan(Attr)` | 运行时 | 极低 | 单一属性值 | 监控、调试 |
| `Scannable.steps()` | 运行时 | 低 | 操作符链全貌 | 链路追踪 |
| `Scannable.parents()` | 运行时 | 低 | 上游链路 | 根因分析 |
| `checkpoint("desc")` | 装配时 + 错误时 | 极低 | 用户描述文本 | 已知位置的标记 |
| `checkpoint()` | 装配时 + 错误时 | 中（堆栈捕获） | 精确调用位置 | 未知错误的定位 |
| `checkpoint(desc, true)` | 装配时 + 错误时 | 中 | 描述 + 调用位置 | 精确 + 语义化 |
| `tap(SignalListener)` | 运行时 | 低 | 全部信号 | Metrics, Tracing |
| `Hooks.onOperatorDebug()` | 全局装配时 | 高（所有操作符） | 所有操作符的装配位置 | 开发环境全量调试 |

### AssemblySnapshot 子类对照表

| 类名 | isCheckpoint | isLight | 堆栈捕获 | 用途 |
|------|-------------|---------|---------|------|
| `AssemblySnapshot` | false | false | 是（延迟） | 全局 debug 模式 |
| `CheckpointLightSnapshot` | true | true | 否 | `checkpoint("desc")` |
| `CheckpointHeavySnapshot` | true | false | 是（延迟） | `checkpoint()` / `checkpoint("desc", true)` |
| `MethodReturnSnapshot` | false | true | 否 | 方法返回标记 |

### SignalListener 方法与信号生命周期对照表

| 阶段 | 方法 | 调用顺序 | 异常处理 |
|------|------|---------|---------|
| 订阅启动 | `doFirst()` | 最先调用 | handleListenerError → 下游 onError |
| 收到 Subscription | `doOnSubscription()` | doFirst 之后 | handleListenerError → 下游 onError |
| 融合协商 | `doOnFusion(int)` | onSubscribe 期间 | handleListenerError → 下游 onError |
| 数据推送 | `doOnNext(T)` | 传播前 | handleListenerError → 下游 onError |
| 请求 | `doOnRequest(long)` | 传播前 | handleListenerError → 忽略 |
| 取消 | `doOnCancel()` | 传播前 | handleListenerError → drop |
| 正常完成 | `doOnComplete()` | 传播前 | handleListenerError → 下游 onError |
| 完成后 | `doAfterComplete()` | 传播后 | handleListenerError → drop |
| 错误终止 | `doOnError(Throwable)` | 传播前 | handleListenerError → 下游 onError |
| 错误后 | `doAfterError(Throwable)` | 传播后 | handleListenerError → drop |
| 最终清理 | `doFinally(SignalType)` | 最后调用 | handleListenerError → drop |
| 畸形 onNext | `doOnMalformedOnNext(T)` | 自动 drop 前 | drop 异常 → drop 值 |
| 畸形 onError | `doOnMalformedOnError(Throwable)` | 自动 drop 前 | drop 异常 → drop 原错 |
| 畸形 onComplete | `doOnMalformedOnComplete()` | 忽略前 | drop 异常 |

---

## 十、设计启示与最佳实践

1. **`@FunctionalInterface` + 大量 default 方法**：`Scannable` 只要求实现 `scanUnsafe()`，但通过 default 方法提供了丰富的导航能力（`parents()`、`actuals()`、`steps()`、`name()` 等）。这种"单方法接口 + 丰富默认行为"的模式在 Java 8+ API 设计中非常实用。

2. **延迟求值避免不必要开销**：`AssemblySnapshot` 的堆栈信息通过 `Supplier<String>` 延迟获取，只在错误实际发生时才调用。在正常执行路径上，`checkpoint()` 的开销接近于零。这是性能优化的经典模式——"只在需要时才付出代价"。

3. **哨兵对象模式**：`UNAVAILABLE_SCAN` 和 `NULL_SCAN` 避免了 null 检查泛滥。调用者可以安全地调用 `isScanAvailable()` 而不用担心 NPE。两种哨兵语义不同但行为相同，在调试时提供更精确的信息。

4. **工厂的两阶段初始化**：`SignalListenerFactory` 将 Publisher 级别的初始化（只做一次）与 Subscriber 级别的创建（每次 subscribe）分开，避免了重复解析 name/tags 等元数据。这种"预计算 + 按需创建"的模式在资源密集型工厂中很常见。

5. **树形错误跟踪**：`OnAssemblyException` 内部维护一棵 `ObservedAtInformationNode` 树，而不是简单的列表。这支持了 `flatMap` 等分支操作符的错误跟踪——错误传播路径可能有分叉，只有树形结构能准确表达。

6. **实际开发建议**：
   - 开发环境：`Hooks.onOperatorDebug()` 全局开启，所有错误都带装配位置。
   - 生产环境：用 `checkpoint("description")` 在关键位置标记，成本极低。
   - 排查问题：用 `Scannable.steps()` 查看操作符链全貌。
   - 监控：用 `tap()` + `SignalListener` 实现自定义 metrics。
   - 调试状态：用 `Scannable.scan(Attr.BUFFERED)` 等查询内部状态。
