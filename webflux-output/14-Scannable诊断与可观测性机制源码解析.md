# Scannable 诊断与可观测性机制源码解析

> **Reactor Core 源码深度研究系列 · 第 14 篇**
> 本文深入分析 Reactor 的诊断与可观测性基础设施，包括 `Scannable` 接口的内省机制、`FluxOnAssembly` 的装配跟踪、`SignalListener` / `SignalListenerFactory` 的运行时信号观测，以及 `checkpoint()` 的轻量级/重量级两种模式。

---

## 一、全局架构总览

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                     Scannable 接口                          │
                    │              @FunctionalInterface                           │
                    │                                                             │
                    │  核心方法:  scanUnsafe(Attr key) → @Nullable Object         │
                    │                                                             │
                    │  导航方法:  parents()  → Stream<Scannable>  (向上遍历)       │
                    │             actuals()  → Stream<Scannable>  (向下遍历)       │
                    │             inners()   → Stream<Scannable>  (内部子流)       │
                    │             steps()    → Stream<String>     (全链路名称)     │
                    │                                                             │
                    │  名称方法:  name()     → 用户定义名称 / stepName()           │
                    │             stepName() → 从类名提取的操作符名称              │
                    │                                                             │
                    │  标签方法:  tags()     → Stream<Tuple2<String,String>>       │
                    └─────────────────┬───────────────────────────────────────────┘
                                      │
              ┌───────────────────────┼─────────────────────────┐
              │                       │                         │
              ▼                       ▼                         ▼
    ┌─────────────────┐    ┌──────────────────┐    ┌───────────────────────┐
    │    Attr<T>       │    │ FluxOnAssembly   │    │  SignalListener<T>   │
    │                  │    │                  │    │  SignalListenerFactory│
    │  PARENT          │    │ AssemblySnapshot │    │  DefaultSignalListener│
    │  ACTUAL          │    │  ├─ Checkpoint   │    │                       │
    │  BUFFERED        │    │  │  LightSnapshot│    │  tap() 操作符          │
    │  CAPACITY        │    │  ├─ Checkpoint   │    │  Micrometer 集成      │
    │  CANCELLED       │    │  │  HeavySnapshot│    └───────────────────────┘
    │  TERMINATED      │    │  └─ MethodReturn │
    │  ERROR           │    │     Snapshot     │
    │  PREFETCH        │    │                  │
    │  REQUESTED_FROM_ │    │ OnAssembly       │
    │   DOWNSTREAM     │    │  Exception       │
    │  RUN_ON          │    │  (traceback)     │
    │  RUN_STYLE       │    └──────────────────┘
    │  NAME            │
    │  TAGS            │
    │  DELAY_ERROR     │
    │  LARGE_BUFFERED  │
    │  LIFTER          │
    │  ACTUAL_METADATA │
    └─────────────────┘

    操作符链导航图:
    
    Source(Flux.just)  ←──PARENT──  map()  ←──PARENT──  filter()  ←──PARENT──  Subscriber
         │                           │                     │                      │
         │──ACTUAL──▶               │──ACTUAL──▶          │──ACTUAL──▶           │
         │                           │                     │                      │
    parents() ◀════════════════════════════════════════════════════════════════════│
    actuals() │════════════════════════════════════════════════════════════════════▶
```

---

## 二、Scannable 接口：Reactor 的内省之眼

### 2.1 @FunctionalInterface 的设计选择

源码位置：`reactor/core/Scannable.java`

```java
@FunctionalInterface
public interface Scannable {

    @Nullable Object scanUnsafe(Attr key);

    // ... 大量 default 方法 ...
}
```

`Scannable` 被标记为 `@FunctionalInterface`——它只有一个抽象方法 `scanUnsafe(Attr key)`。这意味着任何 lambda 都可以充当 `Scannable`，虽然在实践中主要由操作符类实现。

**为什么叫 `scanUnsafe`？** 因为它返回 `@Nullable Object`，调用者需要自己处理类型转换。安全的替代方案是 `scan(Attr<T>)` 方法，它通过 `Attr` 的类型参数和转换器提供类型安全：

```java
default <T> @Nullable T scan(Attr<T> key) {
    T value = key.tryConvert(scanUnsafe(key));
    if (value == null)
        return key.defaultValue();
    return value;
}
```

`key.tryConvert()` 对于大多数 `Attr` 就是简单的强制转换（`(T) o`），但对于 `Attr<Scannable>` 类型（如 `PARENT`、`ACTUAL`），它会通过 `Scannable::from` 进行安全转换，将非 Scannable 对象转为 `UNAVAILABLE_SCAN` 哨兵值。

### 2.2 Attr：类型化的属性键

`Attr<T>` 是 `Scannable` 属性的类型化键。每个 `Attr` 实例定义了属性名称、默认值和可选的安全转换器。

```java
class Attr<T> {
    final @Nullable T defaultValue;
    final @Nullable Function<Object, ? extends T> safeConverter;

    protected Attr(@Nullable T defaultValue) {
        this(defaultValue, null);
    }

    protected Attr(@Nullable T defaultValue,
            @Nullable Function<Object, ? extends T> safeConverter) {
        this.defaultValue = defaultValue;
        this.safeConverter = safeConverter;
    }
}
```

### 2.3 所有预定义 Attr 完整列表

以下是 Reactor Core 定义的全部 `Attr` 常量：

**ACTUAL** — 下游组件引用：
```java
public static final Attr<Scannable> ACTUAL = new Attr<>(null, Scannable::from);
```
指向操作符链的下游 Subscriber。注意 `safeConverter` 是 `Scannable::from`，将非 Scannable 的 Subscriber 转为 `UNAVAILABLE_SCAN`。

**ACTUAL_METADATA** — 元数据标记：
```java
public static final Attr<Boolean> ACTUAL_METADATA = new Attr<>(false);
```
标记某个 Scannable 应该作为前一个操作符的元数据来源（如装配跟踪信息）。`FluxOnAssembly` 在这里返回 `!snapshotStack.isCheckpoint`。

**BUFFERED** — 当前缓冲区大小：
```java
public static final Attr<Integer> BUFFERED = new Attr<>(0);
```
暴露组件当前持有的待处理数据量。例如 `PublishSubscriber` 返回 `queue.size()`。

**CAPACITY** — 最大容量：
```java
public static final Attr<Integer> CAPACITY = new Attr<>(0);
```
组件的最大缓冲容量。`Integer.MAX_VALUE` 表示无限。

**CANCELLED** — 是否已取消：
```java
public static final Attr<Boolean> CANCELLED = new Attr<>(false);
```
下游是否已经取消了订阅。

**DELAY_ERROR** — 是否延迟错误：
```java
public static final Attr<Boolean> DELAY_ERROR = new Attr<>(false);
```
组件是否会延迟传播错误（先处理完缓冲区再传播）。

**ERROR** — 错误状态：
```java
public static final Attr<Throwable> ERROR = new Attr<>(null);
```
如果组件处于错误状态，暴露该错误。

**LARGE_BUFFERED** — 大容量缓冲区大小：
```java
public static final Attr<Long> LARGE_BUFFERED = new Attr<>(null);
```
当缓冲区超过 `Integer.MAX_VALUE` 时使用。例如 `Flux.flatMap` 的 buffer。

**NAME** — 用户定义的名称：
```java
public static final Attr<String> NAME = new Attr<>(null);
```
通过 `Flux.name("myFlux")` 设置的自定义名称。

**PARENT** — 上游组件引用：
```java
public static final Attr<Scannable> PARENT = new Attr<>(null, Scannable::from);
```
指向操作符链的上游。操作符通常返回其 source 或 Subscription。

**RUN_ON** — 运行所在的 Scheduler/Worker：
```java
public static final Attr<Scannable> RUN_ON = new Attr<>(null, Scannable::from);
```
暴露组件运行在哪个 `Scheduler` 或 `Worker` 上。

**PREFETCH** — 预取数量：
```java
public static final Attr<Integer> PREFETCH = new Attr<>(0);
```
组件一次向上游请求的数据量。

**REQUESTED_FROM_DOWNSTREAM** — 下游待处理的 request：
```java
public static final Attr<Long> REQUESTED_FROM_DOWNSTREAM = new Attr<>(0L);
```
下游当前未满足的 request 数量。

**TERMINATED** — 是否已终止：
```java
public static final Attr<Boolean> TERMINATED = new Attr<>(false);
```
上游是否已经发送了 onComplete 或 onError。

**TAGS** — 标签集合：
```java
public static final Attr<Stream<Tuple2<String, String>>> TAGS = new Attr<>(null);
```
通过 `Flux.tag("key", "value")` 设置的键值对标签。

**RUN_STYLE** — 运行模式：
```java
public static final Attr<RunStyle> RUN_STYLE = new Attr<>(RunStyle.UNKNOWN);
```

**LIFTER** — 提升函数名称：
```java
public static final Attr<String> LIFTER = new Attr<>(null);
```

### 2.4 RunStyle 枚举

```java
public enum RunStyle {
    UNKNOWN,   // 没有保证（默认值，最弱保证级别）
    ASYNC,     // 操作符可能切换线程
    SYNC;      // 操作符保证不切换线程（最强保证级别）
}
```

RunStyle 的 `ordinal()` 反映了保证级别：`UNKNOWN(0) < ASYNC(1) < SYNC(2)`。这个设计允许下游根据上游的 RunStyle 进行优化决策。例如，如果整个链路都是 `SYNC`，可以避免不必要的线程安全措施。

在源码中，大多数操作符返回 `SYNC`（如 `FluxMap`、`FluxFilter`），表示它们不引入线程切换。`FluxPublishOn`、`ParallelRunOn` 返回 `ASYNC`，表示它们会切换线程。

---

## 三、导航方法：遍历操作符链

### 3.1 parents() — 向上游遍历

```java
default Stream<? extends Scannable> parents() {
    return Attr.recurse(this, Attr.PARENT);
}
```

内部调用 `Attr.recurse()`，沿着 `PARENT` 属性链构建一个 `Stream`：

```java
static Stream<? extends Scannable> recurse(Scannable _s, Attr<Scannable> key) {
    Scannable s = Scannable.from(_s.scan(key));
    if (!s.isScanAvailable()) {
        return Stream.empty();
    }
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Scannable>() {
        Scannable c = s;

        @Override
        public boolean hasNext() {
            return c != null && c.isScanAvailable();
        }

        @Override
        public Scannable next() {
            Scannable _c = c;
            c = Scannable.from(c.scan(key));
            return _c;
        }
    }, 0), false);
}
```

这是一个惰性求值的迭代器，每次 `next()` 时跟随 `PARENT` 链前进一步。如果遇到非 Scannable 的组件（如第三方操作符），`Scannable.from()` 返回 `UNAVAILABLE_SCAN`（`isScanAvailable() == false`），遍历终止。

### 3.2 actuals() — 向下游遍历

```java
default Stream<? extends Scannable> actuals() {
    return Attr.recurse(this, Attr.ACTUAL);
}
```

与 `parents()` 完全对称，沿 `ACTUAL` 链向下游遍历。

### 3.3 inners() — 内部子流

```java
default Stream<? extends Scannable> inners() {
    return Stream.empty();
}
```

默认返回空。由多播或 flatMap 等操作符覆盖，返回内部的订阅者流。例如 `FluxPublish.PublishSubscriber`：

```java
@Override
public Stream<? extends Scannable> inners() {
    return Stream.of(subscribers);
}
```

### 3.4 steps() — 全链路步骤名称

```java
default Stream<String> steps() {
    List<Scannable> chain = new ArrayList<>();
    chain.addAll(parents().collect(Collectors.toList()));
    Collections.reverse(chain);
    chain.add(this);
    chain.addAll(actuals().collect(Collectors.toList()));

    List<String> chainNames = new ArrayList<>(chain.size());
    for (int i = 0; i < chain.size(); i++) {
        Scannable step = chain.get(i);
        Scannable stepAfter = null;
        if (i < chain.size() - 1) {
            stepAfter = chain.get(i + 1);
        }
        if (stepAfter != null && Boolean.TRUE.equals(stepAfter.scan(Attr.ACTUAL_METADATA))) {
            chainNames.add(stepAfter.stepName());
            i++;
        } else {
            chainNames.add(step.stepName());
        }
    }
    return chainNames.stream();
}
```

`steps()` 做了以下事情：

1. 收集所有 parent（向上），反转使其成为从源到当前的顺序。
2. 加上自己。
3. 收集所有 actual（向下）。
4. 对每个步骤取 `stepName()`，但如果下一个步骤标记了 `ACTUAL_METADATA == true`，则用它的 `stepName()` 替代当前步骤的名称（并跳过它）。

`ACTUAL_METADATA` 机制是装配跟踪的关键——`FluxOnAssembly` 的 `ACTUAL_METADATA` 返回 `!snapshotStack.isCheckpoint`，意味着非 checkpoint 的装配跟踪会将自己的 stepName 附加到前一个操作符上。

### 3.5 stepName() — 从类名提取操作符名称

```java
default String stepName() {
    String name = getClass().getName();
    int innerClassIndex = name.indexOf('$');
    if (innerClassIndex != -1) {
        name = name.substring(0, innerClassIndex);
    }
    int stripPackageIndex = name.lastIndexOf('.');
    if (stripPackageIndex != -1) {
        name = name.substring(stripPackageIndex + 1);
    }
    String stripped = OPERATOR_NAME_UNRELATED_WORDS_PATTERN
        .matcher(name)
        .replaceAll("");

    if (!stripped.isEmpty()) {
        return stripped.substring(0, 1).toLowerCase() + stripped.substring(1);
    }
    return stripped;
}
```

处理流程：
1. 取完整类名（如 `reactor.core.publisher.FluxMap$MapSubscriber`）。
2. 去掉内部类后缀 `$MapSubscriber` → `reactor.core.publisher.FluxMap`。
3. 去掉包名 → `FluxMap`。
4. 用正则 `OPERATOR_NAME_UNRELATED_WORDS_PATTERN` 去掉噪声词。
5. 首字母小写。

`OPERATOR_NAME_UNRELATED_WORDS_PATTERN` 的定义：

```java
Pattern OPERATOR_NAME_UNRELATED_WORDS_PATTERN =
    Pattern.compile("Parallel|Flux|Mono|Publisher|Subscriber|Fuseable|Operator|Conditional");
```

例如：
- `FluxMap` → 去掉 `Flux` → `Map` → `map`
- `FluxPublishOn` → 去掉 `Flux` → `PublishOn` → `publishOn`
- `ParallelMap` → 去掉 `Parallel` → `Map` → `map`
- `FluxFlatMap` → 去掉 `Flux` → `FlatMap` → `flatMap`

### 3.6 name() — 查找用户定义名称

```java
default String name() {
    String thisName = this.scan(Attr.NAME);
    if (thisName != null) {
        return thisName;
    }
    return parents()
            .map(s -> s.scan(Attr.NAME))
            .filter(Objects::nonNull)
            .findFirst()
            .orElseGet(this::stepName);
}
```

优先返回当前组件的 `NAME` 属性（通过 `Flux.name("xxx")` 设置）。如果没有，向上遍历 parent 链找第一个有名字的。如果整条链都没有名字，回退到 `stepName()`。

### 3.7 tags() — 标签聚合

```java
default Stream<Tuple2<String, String>> tags() {
    List<Scannable> sources = new LinkedList<>();
    Scannable aSource = this;
    while (aSource != null && aSource.isScanAvailable()) {
        sources.add(0, aSource);
        aSource = aSource.scan(Attr.PARENT);
    }
    return sources.stream()
        .flatMap(source -> source.scanOrDefault(Attr.TAGS, Stream.empty()));
}
```

从最远的祖先开始收集标签，保持声明顺序。这意味着如果多个操作符设置了同 key 的标签，最终输出中会包含所有值（不去重），顺序是 grandparent → parent → current。

### 3.8 两个哨兵 Scannable

```java
static final Scannable UNAVAILABLE_SCAN = new Scannable() {
    @Override public @Nullable Object scanUnsafe(Attr key) { return null; }
    @Override public boolean isScanAvailable() { return false; }
    @Override public String stepName() { return "UNAVAILABLE_SCAN"; }
};

static final Scannable NULL_SCAN = new Scannable() {
    @Override public @Nullable Object scanUnsafe(Attr key) { return null; }
    @Override public boolean isScanAvailable() { return false; }
    @Override public String stepName() { return "NULL_SCAN"; }
};
```

- **`UNAVAILABLE_SCAN`**：当 `from(Object)` 接收到一个非 null 但非 Scannable 的对象时返回。表示"对象存在但不可扫描"。
- **`NULL_SCAN`**：当 `from(Object)` 接收到 null 时返回。表示"对象不存在"。

两者都返回 `isScanAvailable() == false`，终止链路遍历。

**为什么需要两个不同的哨兵？** 虽然行为相同，但语义不同。在调试时，`UNAVAILABLE_SCAN` 告诉你"这里有一个第三方组件但我看不懂"，而 `NULL_SCAN` 告诉你"这里没有组件，链路到头了"。

---

## 四、FluxOnAssembly：装配时堆栈跟踪

### 4.1 设计目标

在普通的 Java 异步编程中，异常堆栈只显示执行时的调用栈，不包含操作符链的装配位置。这让调试 Reactor 应用变得困难。`FluxOnAssembly` 通过在装配时（而非执行时）捕获堆栈跟踪来解决这个问题。

源码位置：`reactor/core/publisher/FluxOnAssembly.java`

```java
final class FluxOnAssembly<T> extends InternalFluxOperator<T, T>
        implements Fuseable, AssemblyOp {
    final AssemblySnapshot snapshotStack;

    FluxOnAssembly(Flux<? extends T> source, AssemblySnapshot snapshotStack) {
        super(source);
        this.snapshotStack = snapshotStack;
    }
}
```

### 4.2 AssemblySnapshot 层次结构

```java
static class AssemblySnapshot {
    final boolean isCheckpoint;
    final @Nullable String description;
    final @Nullable Supplier<String> assemblyInformationSupplier;
    @Nullable String cached;
}
```

- **`isCheckpoint`**：区分 `checkpoint()` 和全局 debug 模式。
- **`description`**：用户提供的描述（用于 checkpoint 的轻量级模式）。
- **`assemblyInformationSupplier`**：延迟计算的堆栈信息供应商。第一次调用 `toAssemblyInformation()` 时触发并缓存结果。
- **`cached`**：缓存的装配信息字符串。

**三个子类**代表不同的快照模式：

#### CheckpointLightSnapshot — 轻量级 checkpoint

```java
static final class CheckpointLightSnapshot extends AssemblySnapshot {
    CheckpointLightSnapshot(@Nullable String description) {
        super(true, description, null);
        this.cached = "checkpoint(\"" + (description == null ? "" : description) + "\")";
    }

    @Override public boolean isLight() { return true; }

    @Override
    String operatorAssemblyInformation() {
        return this.cached;
    }
}
```

**不捕获堆栈**，只记录描述文本。成本极低，但需要用户提供足够具体的描述来定位问题。

#### CheckpointHeavySnapshot — 重量级 checkpoint

```java
static final class CheckpointHeavySnapshot extends AssemblySnapshot {
    CheckpointHeavySnapshot(@Nullable String description, Supplier<String> assemblyInformationSupplier) {
        super(true, description, assemblyInformationSupplier);
    }
}
```

**捕获堆栈**，通过 `Supplier<String>` 延迟获取。成本较高（创建异常对象来获取堆栈），但提供精确的调用位置。

#### MethodReturnSnapshot — 方法返回快照

```java
static final class MethodReturnSnapshot extends AssemblySnapshot {
    MethodReturnSnapshot(String method) {
        super(false, method, null);
        cached = method;
    }
    @Override public boolean isLight() { return true; }
}
```

用于标记操作符来自哪个方法返回，不涉及堆栈捕获。

### 4.3 checkpoint() 的两种使用模式

在 `Flux` 或 `ParallelFlux` 中：

**轻量级模式**（只传描述）：
```java
public final Flux<T> checkpoint(String description) {
    return checkpoint(description, false);
}
// 内部使用 CheckpointLightSnapshot
```

**重量级模式**（捕获堆栈）：
```java
public final Flux<T> checkpoint() {
    // 内部使用 CheckpointHeavySnapshot + Traces.callSiteSupplierFactory
}
```

**混合模式**（描述 + 堆栈）：
```java
public final Flux<T> checkpoint(String description, boolean forceStackTrace) {
    // forceStackTrace=true 时使用 CheckpointHeavySnapshot
    // forceStackTrace=false 时使用 CheckpointLightSnapshot
}
```

### 4.4 OnAssemblyException — 错误增强的核心

当错误发生时，`OnAssemblySubscriber.fail()` 方法将装配信息注入到异常中：

```java
final Throwable fail(Throwable t) {
    boolean lightCheckpoint = snapshotStack.isLight();

    OnAssemblyException onAssemblyException = null;
    for (Throwable e : t.getSuppressed()) {
        if (e instanceof OnAssemblyException) {
            onAssemblyException = (OnAssemblyException) e;
            break;
        }
    }

    if (onAssemblyException == null) {
        if (lightCheckpoint) {
            onAssemblyException = new OnAssemblyException("");
        } else {
            StringBuilder sb = new StringBuilder();
            fillStacktraceHeader(sb, parent.getClass(), snapshotStack.getDescription());
            sb.append(snapshotStack.toAssemblyInformation().replaceFirst("\\n$", ""));
            onAssemblyException = new OnAssemblyException(description);
        }
        t = Exceptions.addSuppressed(t, onAssemblyException);
        // 清理堆栈中的 OnAssembly 相关帧
        // ...
    }

    onAssemblyException.add(parent, current, snapshotStack);
    return t;
}
```

核心流程：
1. 在异常的 suppressed exceptions 中查找已有的 `OnAssemblyException`。
2. 如果没有，创建一个新的并附加到原始异常。
3. 调用 `onAssemblyException.add()` 将当前操作符的装配信息添加到树形结构中。

`OnAssemblyException` 内部维护一棵 `ObservedAtInformationNode` 树，`getMessage()` 方法将其格式化为可读的错误跟踪信息：

```java
@Override
public @Nullable String getMessage() {
    synchronized (nodesPerId) {
        if (root.children.isEmpty()) {
            return super.getMessage();
        }
        StringBuilder sb = new StringBuilder(super.getMessage())
            .append(System.lineSeparator())
            .append("Error has been observed at the following site(s):")
            .append(System.lineSeparator());

        List<List<ObservedAtInformationNode>> rootPaths = new ArrayList<>();
        root.children.forEach(actualRoot -> findPathToLeaves(actualRoot, rootPaths));

        rootPaths.forEach(path -> path.forEach(node -> {
            boolean isRoot = node.parent == null || node.parent == root;
            sb.append("\t");
            String connector = isRoot ? "*_" : "|_";
            sb.append(connector);
            // ... 对齐和格式化 ...
            sb.append(node.operator);
            sb.append(Traces.CALL_SITE_GLUE);
            sb.append(node.message);
            sb.append(System.lineSeparator());
        }));
        sb.append("Original Stack Trace:");
        return sb.toString();
    }
}
```

输出示例：
```
Error has been observed at the following site(s):
	*__checkpoint ⇢ after map
	|_ map ⇢ com.example.MyService.process(MyService.java:42)
Original Stack Trace:
```

### 4.5 为什么不用 fillInStackTrace？

```java
@Override
public Throwable fillInStackTrace() {
    return this;  // 不填充堆栈
}
```

`OnAssemblyException` 覆盖了 `fillInStackTrace()` 使其不产生堆栈。因为它的"堆栈"信息来自装配时捕获的快照，而不是运行时。这避免了创建 `OnAssemblyException` 时的额外开销。

---

## 五、SignalListener 与 tap() 操作符：运行时信号观测

### 5.1 SignalListener 接口

源码位置：`reactor/core/observability/SignalListener.java`

```java
public interface SignalListener<T> {
    void doFirst() throws Throwable;
    void doFinally(SignalType terminationType) throws Throwable;
    void doOnSubscription() throws Throwable;
    void doOnFusion(int negotiatedFusion) throws Throwable;
    void doOnRequest(long requested) throws Throwable;
    void doOnCancel() throws Throwable;
    void doOnNext(T value) throws Throwable;
    void doOnComplete() throws Throwable;
    void doOnError(Throwable error) throws Throwable;
    void doAfterComplete() throws Throwable;
    void doAfterError(Throwable error) throws Throwable;
    void doOnMalformedOnNext(T value) throws Throwable;
    void doOnMalformedOnError(Throwable error) throws Throwable;
    void doOnMalformedOnComplete() throws Throwable;
    void handleListenerError(Throwable listenerError);

    default Context addToContext(Context originalContext) {
        return originalContext;
    }
}
```

`SignalListener` 是 `doOnXxx` 系列副作用操作符的统一抽象。它覆盖了所有可能的信号类型：

| 方法 | 触发时机 | 对应 doOnXxx |
|------|---------|-------------|
| `doFirst()` | subscribe 时（最先） | doFirst() |
| `doOnSubscription()` | 收到 Subscription 时 | doOnSubscribe() |
| `doOnNext(T)` | 收到数据时（传播前） | doOnNext() |
| `doOnComplete()` | 收到完成时（传播前） | doOnComplete() |
| `doOnError(Throwable)` | 收到错误时（传播前） | doOnError() |
| `doAfterComplete()` | 完成传播后 | doAfterTerminate() |
| `doAfterError(Throwable)` | 错误传播后 | doAfterTerminate() |
| `doOnRequest(long)` | 收到 request 时 | doOnRequest() |
| `doOnCancel()` | 收到 cancel 时（传播前） | doOnCancel() |
| `doFinally(SignalType)` | 最终清理（传播后） | doFinally() |
| `doOnFusion(int)` | 融合协商后 | 无对应操作符 |
| `doOnMalformedOnNext(T)` | 终止后还收到 onNext | 自动 drop |
| `doOnMalformedOnError(Throwable)` | 终止后还收到 onError | 自动 drop |
| `doOnMalformedOnComplete()` | 终止后还收到 onComplete | 忽略 |
| `handleListenerError(Throwable)` | 上述任何方法抛异常时 | 最后的错误处理 |

**为什么 `handleListenerError` 必须不能抛异常？** 因为它是错误处理的最后一道防线。如果它也抛异常，就会形成无限递归。它的存在是为了让监听器在 `doFinally` 不会被调用的情况下（handler 自身出错时）仍然有机会清理资源。

### 5.2 DefaultSignalListener — 空实现基类

源码位置：`reactor/core/observability/DefaultSignalListener.java`

```java
public abstract class DefaultSignalListener<T> implements SignalListener<T> {
    int fusionMode = Fuseable.NONE;

    @Override public void doFirst() throws Throwable {}
    @Override public void doFinally(SignalType terminationType) throws Throwable {}
    @Override public void doOnSubscription() throws Throwable {}

    @Override
    public void doOnFusion(int negotiatedFusion) throws Throwable {
        this.fusionMode = negotiatedFusion;
    }

    protected int getFusionMode() { return fusionMode; }

    // ... 所有其他方法都是空实现 ...
}
```

注意 `doOnFusion()` 不是空实现——它捕获了融合模式并通过 `getFusionMode()` 暴露给子类。这让自定义监听器可以根据融合模式调整行为（例如在 SYNC 融合模式下跳过某些计数）。

### 5.3 SignalListenerFactory — 工厂模式

源码位置：`reactor/core/observability/SignalListenerFactory.java`

```java
public interface SignalListenerFactory<T, STATE> {
    STATE initializePublisherState(Publisher<? extends T> source);

    SignalListener<T> createListener(Publisher<? extends T> source,
            ContextView listenerContext, STATE publisherContext);
}
```

这是一个两阶段工厂：

1. **Publisher 级别**：`initializePublisherState()` 在装配时调用一次，创建共享状态（如从 source 提取 NAME 和 TAGS 用于 metrics 前缀）。
2. **Subscriber 级别**：`createListener()` 在每次 subscribe 时调用，创建独立的 `SignalListener`。

**为什么分两个阶段？** 因为某些初始化操作（如解析操作符名称、创建 metric meter）只需要做一次，而不需要每次 subscribe 都重复。`STATE` 类型参数允许工厂定义自己的共享状态类型。

### 5.4 tap() 操作符

`tap()` 是将 `SignalListenerFactory` 应用到 `Flux` 的操作符。它有三个重载版本：

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

---

## 六、反例：如果没有 Scannable 会怎样

想象一下，如果 Reactor 没有 `Scannable` 机制：

1. **调试噩梦**：错误堆栈只显示 `onError` 的执行路径，不知道操作符链是在哪里装配的。10 个 `map()` 操作符在堆栈中看起来完全一样。

2. **无法监控**：不知道每个操作符的缓冲区有多大、请求了多少、是否已取消。Micrometer 集成无从做起。

3. **黑盒操作**：无法遍历操作符链，无法知道数据流经了哪些操作符。`steps()` 的输出在生产问题排查中是无价的。

4. **标签传递断裂**：`Flux.name("http-request").tag("uri", "/api/users")` 无法将标签传递给下游的 metrics 采集器。

`Scannable` 本质上是 Reactor 的**反射 API**——它让响应式流从"只进不出"的黑盒变成了可检查、可监控、可调试的透明管道。

---

## 七、实际应用：Scannable 在 Reactor 内部的使用

### 7.1 操作符链中的 scanUnsafe 实现模式

每个操作符都实现了 `scanUnsafe()`，暴露自己的内部状态。以 `FluxPublish.PublishSubscriber` 为例：

```java
@Override
public @Nullable Object scanUnsafe(Attr key) {
    if (key == Attr.PARENT) return s;
    if (key == Attr.PREFETCH) return prefetch;
    if (key == Attr.ERROR) return error;
    if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
    if (key == Attr.TERMINATED) return isTerminated();
    if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
    if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
    return null;
}
```

这种 if-chain 模式在整个 Reactor 代码库中重复出现。为什么不用 `Map<Attr, Object>`？因为：
- `Attr` 的种类有限（约 15 个），if-chain 的性能与 HashMap 相当。
- 很多属性的值是动态计算的（如 `queue.size()`），不能预存。
- 避免了 HashMap 的内存开销（每个操作符都会创建大量实例）。

### 7.2 Context 传播中的 Scannable

`Operators.multiSubscribersContext()` 利用 Scannable 的 inners 来合并多个下游的 Context：

```java
// PublishSubscriber 和 ReplaySubscriber 中
@Override
public Context currentContext() {
    return Operators.multiSubscribersContext(subscribers);
}
```

这确保了多播场景下，上游能看到所有下游 Subscriber 的 Context 的合并视图。

---

## 八、归纳表格

### Attr 属性对照表

| Attr 名称 | 类型 | 默认值 | 安全转换 | 含义 | 典型提供者 |
|-----------|------|--------|---------|------|-----------|
| `ACTUAL` | `Scannable` | null | `Scannable::from` | 下游组件引用 | 所有 InnerOperator |
| `ACTUAL_METADATA` | `Boolean` | false | 无 | 是否为元数据源 | FluxOnAssembly |
| `BUFFERED` | `Integer` | 0 | 无 | 当前缓冲区大小 | PublishSubscriber, MergeSequentialInner |
| `CAPACITY` | `Integer` | 0 | 无 | 最大容量 | ReplaySubscriber (buffer.capacity()) |
| `CANCELLED` | `Boolean` | false | 无 | 下游是否已取消 | 大多数 Subscriber |
| `DELAY_ERROR` | `Boolean` | false | 无 | 是否延迟错误 | FluxFlatMap, ParallelFlatMap |
| `ERROR` | `Throwable` | null | 无 | 当前错误 | PublishSubscriber, ReplaySubscriber |
| `LARGE_BUFFERED` | `Long` | null | 无 | 超大缓冲区大小 | FluxFlatMap, FluxWindow |
| `LIFTER` | `String` | null | 无 | lift 函数名称 | Operators.lift() |
| `NAME` | `String` | null | 无 | 用户定义名称 | FluxName |
| `PARENT` | `Scannable` | null | `Scannable::from` | 上游组件引用 | 所有操作符 |
| `PREFETCH` | `Integer` | 0 | 无 | 预取数量 | FluxPublish, ParallelSource |
| `REQUESTED_FROM_DOWNSTREAM` | `Long` | 0L | 无 | 下游未满足的 request | PubSubInner, ReplayInner |
| `RUN_ON` | `Scannable` | null | `Scannable::from` | 运行的 Scheduler | FluxPublishOn, ReplayInner |
| `RUN_STYLE` | `RunStyle` | UNKNOWN | 无 | 运行模式 | 所有操作符 |
| `TAGS` | `Stream<Tuple2>` | null | 无 | 标签集合 | FluxName |
| `TERMINATED` | `Boolean` | false | 无 | 上游是否已终止 | 大多数 Subscriber |

### 诊断工具对照表

| 工具 | 时机 | 成本 | 信息量 | 适用场景 |
|------|------|------|--------|---------|
| `Scannable.scan(Attr)` | 运行时 | 极低 | 单一属性值 | 监控、调试 |
| `Scannable.steps()` | 运行时 | 低 | 操作符链全貌 | 链路追踪 |
| `Scannable.parents()` | 运行时 | 低 | 上游链路 | 根因分析 |
| `checkpoint()` (轻量级) | 装配时 + 错误时 | 极低 | 用户描述文本 | 已知位置的标记 |
| `checkpoint()` (重量级) | 装配时 + 错误时 | 中（堆栈捕获） | 精确调用位置 | 未知错误的定位 |
| `checkpoint(desc, true)` | 装配时 + 错误时 | 中 | 描述 + 调用位置 | 精确+语义化 |
| `tap(SignalListener)` | 运行时 | 低 | 全部信号 | Metrics, Tracing |
| `Hooks.onOperatorDebug()` | 全局装配时 | 高（所有操作符） | 所有操作符的装配位置 | 开发环境全量调试 |

### AssemblySnapshot 子类对照表

| 类名 | isCheckpoint | isLight | 堆栈捕获 | 用途 |
|------|-------------|---------|---------|------|
| `AssemblySnapshot` | false | false | 是 (lazy) | 全局 debug 模式 |
| `CheckpointLightSnapshot` | true | true | 否 | checkpoint("desc") |
| `CheckpointHeavySnapshot` | true | false | 是 (lazy) | checkpoint() / checkpoint("desc", true) |
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

## 九、设计启示

1. **`@FunctionalInterface` + 大量 default 方法**：`Scannable` 只要求实现 `scanUnsafe()`，但通过 default 方法提供了丰富的导航能力。这种"单方法接口 + 丰富默认行为"的模式在 Java 8+ API 设计中非常实用。

2. **位段编码的状态机**：`OnAssemblyException.ObservedAtInformationNode` 用树形结构记录错误传播路径，而不是简单的列表。这支持了分支操作符（如 flatMap）的错误跟踪。

3. **延迟求值避免不必要开销**：`AssemblySnapshot` 的堆栈信息通过 `Supplier<String>` 延迟获取，只在错误实际发生时才调用。在正常执行路径上，`checkpoint()` 的开销接近于零。

4. **哨兵对象模式**：`UNAVAILABLE_SCAN` 和 `NULL_SCAN` 避免了 null 检查泛滥。调用者可以安全地调用 `isScanAvailable()` 而不用担心 NPE。

5. **工厂的两阶段初始化**：`SignalListenerFactory` 将 Publisher 级别的初始化与 Subscriber 级别的创建分开，避免了每次 subscribe 都重复解析 name/tags 等元数据。这种"预计算 + 按需创建"的模式在资源密集型工厂中很常见。
