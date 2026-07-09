# Context 上下文传播机制源码解析

> **Reactor Core 源码深度研究系列 · 第 10 篇**

本文深入剖析 Reactor Core 中 Context 上下文传播机制的设计理念、实现细节和操作符链中的传播路径。所有分析基于真实源码，引用真实类名、字段名和方法名。

---

## 一、全局架构总览

```
                    Context 传播方向图
                    ==================

  数据流方向 (下游 → 上游):
  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
  │ Subscriber│◄────│  map()   │◄────│ filter() │◄────│  Source  │
  │ (终端消费者)│     │Operator  │     │Operator  │     │(数据源)   │
  └──────────┘     └──────────┘     └──────────┘     └──────────┘
       │                │                │                │
       │ currentContext()│  currentContext()│  currentContext()│
       ▼                ▼                ▼                ▼
  ┌──────────────────────────────────────────────────────────────┐
  │                    Context 实例                               │
  │  Context0 → Context1 → Context2 → ... → ContextN             │
  │  (从下游向上游传播，每经过 contextWrite() 操作符可修改)          │
  └──────────────────────────────────────────────────────────────┘

  Context 传播方向: 下游 → 上游 (与数据流方向相反!)
  数据流方向: 上游 → 下游
```

```
  Context 类层次结构:

  ContextView (只读接口)
      │
      └── Context (可写接口, extends ContextView)
              │
              └── CoreContext (内部接口, 提供 putAllInto/unsafePutAllInto)
                      │
                      ├── Context0   (0个键值对, 单例)
                      ├── Context1   (1个键值对, final字段)
                      ├── Context2   (2个键值对, final字段)
                      ├── Context3   (3个键值对, final字段)
                      ├── Context4   (4个键值对, final字段)
                      ├── Context5   (5个键值对, final字段)
                      └── ContextN   (6+个键值对, LinkedHashMap)
```

Reactor 的 Context 是一个不可变的键值对容器，沿着操作符链从下游（Subscriber）向上游（Source）传播。这个设计是 Reactor 区别于传统命令式编程的核心特性之一——它解决了响应式编程中线程切换导致 ThreadLocal 丢失的问题。

---

## 二、ContextView 与 Context 的接口分离

### 2.1 ContextView：只读接口

源码文件：`reactor/util/context/ContextView.java`

```java
// ContextView.java
public interface ContextView {
    <T> T get(Object key);                                    // 获取值，不存在则抛异常
    default <T> T get(Class<T> key) { ... }                   // 按类型键获取
    default <T> @Nullable T getOrDefault(Object key, @Nullable T defaultValue) { ... }
    default <T> Optional<T> getOrEmpty(Object key) { ... }
    boolean hasKey(Object key);                               // 是否包含键
    default boolean isEmpty() { return size() == 0; }
    int size();                                               // 键值对数量
    Stream<Map.Entry<Object, Object>> stream();               // 流式遍历
    default void forEach(BiConsumer<Object, Object> action) { ... }
}
```

### 2.2 Context：可写接口

源码文件：`reactor/util/context/Context.java`

```java
// Context.java
public interface Context extends ContextView {
    static Context empty() { return Context0.INSTANCE; }
    static Context of(Object key, Object value) { return new Context1(key, value); }
    static Context of(Object key1, Object value1, Object key2, Object value2) { ... }
    // ... up to 5 key-value pairs
    
    Context put(Object key, Object value);        // 返回新 Context（不可变）
    Context delete(Object key);                    // 返回新 Context（不可变）
    default Context putAll(ContextView other) { ... }
    default ContextView readOnly() { return this; }
}
```

**为什么将读和写分离到两个接口？** 这是一个经典的"接口隔离"设计。在 Reactor 中，`CoreSubscriber.currentContext()` 返回的是 `Context`，但下游操作符只需要读取 Context 中的值（如 tracing ID），不需要修改。如果只暴露 `ContextView`，编译器就能防止下游意外修改 Context。`Context.put()` 返回的是新实例而不是修改当前实例——这是不可变设计的核心约束。

**反例：如果 Context 是可变的会怎样？** 假设操作符 A 修改了 Context 添加了 key="traceId"，然后数据流继续向上游传播。如果操作符 B 在另一个线程中也修改了同一个 Context 实例，就会产生竞态条件。更严重的是，由于 Context 沿操作符链传播，一个可变的 Context 会被多个操作符共享，任何修改都会影响整个链——这完全违背了"每个操作符独立管理自己 Context"的设计意图。

---

## 三、Context0-Context5：字段级优化

### 3.1 Context0：空 Context 单例

源码文件：`reactor/util/context/Context0.java`

```java
// Context0.java
final class Context0 implements CoreContext {
    static final Context0 INSTANCE = new Context0();

    @Override
    public Context put(Object key, Object value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return new Context1(key, value);  // 0→1 升级
    }

    @Override
    public Context delete(Object key) {
        return this;  // 空 Context 删除任何 key 都是自身
    }

    @Override
    public <T> T get(Object key) {
        throw new NoSuchElementException("Context is empty");
    }

    @Override
    public boolean hasKey(Object key) { return false; }

    @Override
    public int size() { return 0; }

    @Override
    public boolean isEmpty() { return true; }
}
```

`Context0` 是一个无状态的单例，所有空 Context 共享同一个实例。`put` 操作返回一个新的 `Context1`，体现了"不可变 + 升级"模式。

### 3.2 Context1：单键值对

源码文件：`reactor/util/context/Context1.java`

```java
// Context1.java
final class Context1 implements CoreContext {
    final Object key;
    final Object value;

    Context1(Object key, Object value) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = Objects.requireNonNull(value, "value");
    }

    @Override
    public Context put(Object key, Object value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (this.key.equals(key)) {
            return new Context1(key, value);  // 同 key 替换值，仍然是 Context1
        }
        return new Context2(this.key, this.value, key, value);  // 不同 key，升级为 Context2
    }

    @Override
    public Context delete(Object key) {
        if (this.key.equals(key)) {
            return Context.empty();  // 删除唯一 key，降级为 Context0
        }
        return this;  // key 不存在，返回自身
    }

    @Override
    public boolean hasKey(Object key) {
        return this.key.equals(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key) {
        if (hasKey(key)) {
            return (T) this.value;
        }
        throw new NoSuchElementException("Context does not contain key: " + key);
    }
}
```

### 3.3 升级与降级策略

Context1-5 的 `put` 和 `delete` 方法实现了自动升级/降级：

- **put 时升级**：`Context1.put(新key)` → `Context2`；`Context5.put(新key)` → `ContextN`
- **delete 时降级**：`Context1.delete(唯一key)` → `Context0`；`ContextN.delete(某个key)` 且剩余 5 个 → `Context5`

**为什么在 5 个键值对时切换到 ContextN？** 这是一个基于实测的性能权衡。1-5 个键值对用 `final` 字段存储有三个优势：
1. **零内存开销**：不需要 Map 的内部结构（Node 数组、负载因子等）
2. **CPU 缓存友好**：final 字段在对象内存中连续排列，cache line 利用率高
3. **JIT 优化友好**：final 字段可以被 JIT 优化为常量折叠

超过 5 个时，`LinkedHashMap` 的 O(1) 查找优势开始超过字段遍历的 O(n) 开销，且 Map 的内存开销在 6+ 个条目时被均摊到可接受范围。

### 3.4 从多视角验证字段优化

| 视角 | Context1 (1个字段) | ContextN (LinkedHashMap) |
|------|-------------------|------------------------|
| **内存** | 2个对象引用 (key, value) + 对象头 | LinkedHashMap 内部结构 + 6+个Node |
| **查找** | 1次 equals 比较 | 1次 hashCode + 可能的链表遍历 |
| **修改** | 创建新对象 (2个引用赋值) | 创建新Map + copy所有条目 |
| **GC** | 年轻代快速回收 | 可能进入老年代 |

---

## 四、ContextN：Map 支持的实现

源码文件：`reactor/util/context/ContextN.java`

```java
// ContextN.java
final class ContextN extends LinkedHashMap<Object, Object>
        implements CoreContext, BiConsumer<Object, Object>, Consumer<Entry<Object, Object>> {

    ContextN(Object key1, Object value1, /* ... */ Object key6, Object value6) {
        super(6, 1f);
        accept(key1, value1);
        accept(key2, value2);
        // ... 
    }

    ContextN(Map<Object, Object> originalToCopy) {
        super(Objects.requireNonNull(originalToCopy, "originalToCopy"));
    }

    ContextN(int initialCapacity) {
        super(initialCapacity, 1.0f);
    }

    // 内部 put（不创建新实例）
    @Override
    public void accept(Object key, Object value) {
        super.put(Objects.requireNonNull(key, "key"),
                Objects.requireNonNull(value, "value"));
    }

    // 外部 put（创建新实例，不可变语义）
    @Override
    public Context put(Object key, Object value) {
        ContextN newContext = new ContextN(this);  // 复制当前 Map
        newContext.accept(key, value);              // 添加新条目
        return newContext;
    }

    @Override
    public Context delete(Object key) {
        if (!hasKey(key)) {
            return this;
        }
        int s = size() - 1;
        if (s == 5) {
            // 降级为 Context5
            Entry<Object, Object>[] arr = new Entry[s];
            int idx = 0;
            for (Entry<Object, Object> entry : entrySet()) {
                if (!entry.getKey().equals(key)) {
                    arr[idx] = entry;
                    idx++;
                }
            }
            return new Context5(
                    arr[0].getKey(), arr[0].getValue(),
                    arr[1].getKey(), arr[1].getValue(),
                    arr[2].getKey(), arr[2].getValue(),
                    arr[3].getKey(), arr[3].getValue(),
                    arr[4].getKey(), arr[4].getValue());
        }
        ContextN newInstance = new ContextN(this);
        newInstance.remove(key);
        return newInstance;
    }
}
```

### 4.1 put 的 copy-on-write 语义

`ContextN.put()` 不是修改当前 Map，而是创建一个新的 `ContextN`（通过 `new ContextN(this)` 复制当前 Map），然后在新实例上添加条目。这保证了不可变语义——原有 Context 实例不受影响。

### 4.2 delete 的降级优化

当 `ContextN` 删除一个 key 后只剩 5 个条目时，会降级为 `Context5`，享受字段级优化的好处。这种"自适应容器"设计让 Context 在不同规模下都能保持最优性能。

### 4.3 ContextN 同时实现 BiConsumer 和 Consumer

```java
final class ContextN extends LinkedHashMap<Object, Object>
        implements CoreContext, BiConsumer<Object, Object>, Consumer<Entry<Object, Object>> {
```

`ContextN` 实现了 `BiConsumer<Object, Object>` 和 `Consumer<Entry<Object, Object>>`，这样它可以直接作为 `Map.forEach` 和 `Stream.forEach` 的回调使用，避免了额外的 lambda 分配：

```java
// Context.putAll 中的使用
default Context putAll(ContextView other) {
    if (other.isEmpty()) return this;
    ContextN newContext = new ContextN(this.size() + other.size());
    this.stream().sequential().forEach(newContext);      // ContextN 作为 Consumer
    other.stream().sequential().forEach(newContext);      // ContextN 作为 Consumer
    if (newContext.size() <= 5) {
        return Context.of((Map<?, ?>) newContext);        // 可能降级回 Context1-5
    }
    return newContext;
}
```

---

## 五、CoreContext：内部优化接口

源码文件：`reactor/util/context/CoreContext.java`（通过 `Context0` 等实现）

`CoreContext` 是 Reactor 内部使用的接口，提供了 `putAllInto` 和 `unsafePutAllInto` 两个方法，用于高效的 Context 合并：

```java
// Context0.java
@Override
public Context putAllInto(Context base) {
    return base;  // 空 Context 不需要合并
}

@Override
public void unsafePutAllInto(ContextN other) {
    // 什么都不做
}

// Context1.java
@Override
public Context putAllInto(Context base) {
    return base.put(key, value);  // 将唯一的键值对 put 到目标 Context
}

@Override
public void unsafePutAllInto(ContextN other) {
    other.accept(key, value);  // 直接写入目标 Map，无创建新实例
}
```

**为什么需要 `unsafePutAllInto`？** 在 `putAll` 操作中，如果两个 Context 都是 `CoreContext`，可以直接将源 Context 的条目写入目标 `ContextN` 的内部 Map，避免逐个 `put` 创建中间 Context 实例。`unsafe` 前缀表示这个方法绕过了不可变约束——它直接修改目标 Map，但调用者保证目标 Map 是新创建的、不被外部引用的。

---

## 六、currentContext() 在操作符链中的传播

### 6.1 CoreSubscriber 的默认实现

源码文件：`reactor/core/CoreSubscriber.java`

```java
// CoreSubscriber.java
public interface CoreSubscriber<T> extends Subscriber<T> {
    default Context currentContext() {
        return Context.empty();  // 默认返回空 Context
    }
    
    @Override
    void onSubscribe(Subscription s);
}
```

终端订阅者（如 `LambdaSubscriber`）默认返回 `Context.empty()`，除非用户通过 `subscribe(..., Context)` 显式提供初始 Context。

### 6.2 InnerOperator 的委托传播

源码文件：`reactor/core/publisher/InnerOperator.java`

```java
// InnerOperator.java
interface InnerOperator<I, O> extends InnerConsumer<I>, InnerProducer<O> {
    @Override
    default Context currentContext() {
        CoreSubscriber<? super O> actual = actual();
        assert actual != null : "actual subscriber can not be null in inner operator";
        return actual.currentContext();  // 委托给下游订阅者
    }
}
```

这是 Context 传播的核心机制：每个操作符的 `currentContext()` 方法不维护自己的 Context，而是委托给下游订阅者（`actual`）。这形成了一个从终端订阅者到数据源的委托链：

```
LambdaSubscriber.currentContext()           → Context.empty() 或用户提供的 Context
    ↑ 委托
MapSubscriber.currentContext()              → actual.currentContext()
    ↑ 委托
FilterSubscriber.currentContext()           → actual.currentContext()
    ↑ 委托
FluxRange.RangeSubscription.currentContext() → Context.empty() (InnerConsumer 默认)
```

**为什么 Context 从下游向上游传播，而不是从上游向下游？** 因为 Context 携带的是"下游消费者想要的上下文信息"。例如，用户在终端订阅时设置了 tracing ID，这个 ID 需要被上游所有操作符感知（用于在日志中关联请求）。如果从上游向下游传播，上游操作符无法知道下游消费者需要什么上下文。这种"逆向传播"设计让每个操作符在订阅阶段就能获取完整的 Context，无需在数据流阶段额外传递。

### 6.3 contextWrite() 操作符：修改 Context

源码文件：`reactor/core/publisher/FluxContextWrite.java`

```java
// FluxContextWrite.java
final class FluxContextWrite<T> extends InternalFluxOperator<T, T> implements Fuseable {
    final Function<Context, Context> doOnContext;

    FluxContextWrite(Flux<? extends T> source, Function<Context, Context> doOnContext) {
        super(source);
        this.doOnContext = Objects.requireNonNull(doOnContext, "doOnContext");
    }

    @Override
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
        // 1. 从下游获取当前 Context
        Context c = doOnContext.apply(actual.currentContext());
        // 2. 创建带有新 Context 的订阅者
        return new ContextWriteSubscriber<>(actual, c);
    }
}
```

`FluxContextWrite` 在订阅时做了两件事：
1. 调用 `actual.currentContext()` 获取下游的 Context
2. 对该 Context 应用用户提供的修改函数，得到新 Context
3. 用新 Context 包装下游订阅者，形成一个新的 `ContextWriteSubscriber`

```java
// FluxContextWrite.java
static final class ContextWriteSubscriber<T>
        implements ConditionalSubscriber<T>, InnerOperator<T, T>, QueueSubscription<T> {
    final CoreSubscriber<? super T> actual;
    final Context context;  // 修改后的 Context

    ContextWriteSubscriber(CoreSubscriber<? super T> actual, Context context) {
        this.actual = actual;
        this.context = context;
    }

    @Override
    public Context currentContext() {
        return this.context;  // 返回修改后的 Context，而不是委托给 actual
    }
    
    // ... 其他方法直接委托给 actual
}
```

**关键点：`ContextWriteSubscriber.currentContext()` 返回自己的 `context` 字段，而不是委托给 `actual.currentContext()`。** 这是 `contextWrite()` 修改 Context 传播的根本机制——它打破了委托链，插入了一个新的 Context 值。

### 6.4 传播路径完整示例

```
用户代码:
flux
    .map(fn1)                    // MapSubscriber
    .contextWrite(ctx -> ctx.put("traceId", "abc"))  // ContextWriteSubscriber
    .filter(fn2)                 // FilterSubscriber
    .subscribe(subscriber);      // LambdaSubscriber (带初始 Context: {userId: "user1"})

订阅时的 Context 传播:
1. LambdaSubscriber.currentContext() → {userId: "user1"}
2. FilterSubscriber.currentContext() → actual.currentContext() → {userId: "user1"}
3. ContextWriteSubscriber.currentContext() → 
     doOnContext.apply({userId: "user1"}) → {userId: "user1", traceId: "abc"}
4. MapSubscriber.currentContext() → actual.currentContext() → {userId: "user1", traceId: "abc"}
5. Source.currentContext() → {userId: "user1", traceId: "abc"}
```

上游操作符（map、source）看到的是经过 `contextWrite()` 修改后的 Context，而 `contextWrite()` 之前的操作符（filter）看到的是原始 Context。这就是"Context 从下游向上游传播"的含义。

---

## 七、对比 ThreadLocal：为什么 Reactor 不用 ThreadLocal

### 7.1 线程切换导致 ThreadLocal 丢失

Reactor 的操作符可能在不同的线程上执行，特别是 `publishOn()` 和 `subscribeOn()`：

```
场景: publishOn 切换线程

Thread-1:                          Thread-2 (scheduler):
  flux.map(fn)                       ↓
    .publishOn(scheduler)           mapSubscriber.onNext(item)
    .subscribe(sub)                 ← ThreadLocal 中没有 Thread-1 设置的值!

如果用 ThreadLocal:
  Thread-1 设置 ThreadLocal("traceId", "abc")
  → publishOn 切换到 Thread-2
  → Thread-2 读取 ThreadLocal("traceId") → null! (丢失!)

如果用 Context:
  Subscriber 设置 Context({traceId: "abc"})
  → publishOn 切换到 Thread-2
  → Thread-2 上的操作符调用 currentContext() → {traceId: "abc"} (不丢失!)
```

**反例：如果用 ThreadLocal 存储 tracing ID 会怎样？** 假设有一个 WebFlux 应用，请求处理链为 `Controller → Service → Repository`，中间经过 `publishOn(boundedElastic)`。如果 tracing ID 存在 ThreadLocal 中：
1. Controller 在 Netty 线程上设置 `ThreadLocal.set("traceId", "req-123")`
2. `publishOn(boundedElastic)` 切换到工作线程
3. Service 在工作线程上读取 `ThreadLocal.get("traceId")` → `null`！
4. 日志中的 traceId 丢失，无法关联请求

用 Context 则不存在这个问题，因为 Context 存储在订阅者对象中，不依赖线程。无论 `onNext` 在哪个线程执行，`currentContext()` 返回的都是同一个 Context 实例。

### 7.2 多视角对比

| 维度 | ThreadLocal | Reactor Context |
|------|------------|----------------|
| **存储位置** | 线程的 ThreadLocalMap | 订阅者对象的字段 |
| **线程切换** | 值丢失 | 值保留 |
| **传播方向** | 线程内隐式传播 | 操作符链显式传播（下游→上游） |
| **修改语义** | 可变（直接修改） | 不可变（put 返回新实例） |
| **线程安全** | 线程隔离，天然安全 | 不可变，天然安全 |
| **生命周期** | 线程生命周期 | 订阅生命周期 |
| **内存开销** | 每线程一个 ThreadLocalMap 条目 | 每订阅一个 Context 实例 |
| **清理** | 需要 remove() 防止内存泄漏 | 随订阅销毁自动回收 |

### 7.3 自动 ThreadLocal 传播（3.5.3+）

Reactor 3.5.3 引入了 `Hooks.enableAutomaticContextPropagation()`，可以将 Context 中的值自动同步到 ThreadLocal：

```java
// Hooks.java
public static void enableAutomaticContextPropagation() {
    if (ContextPropagationSupport.isContextPropagationOnClasspath) {
        Schedulers.onScheduleHook(CONTEXT_IN_THREAD_LOCALS_KEY,
                ContextPropagation.scopePassingOnScheduleHook());
        ContextPropagationSupport.propagateContextToThreadLocals = true;
        ContextPropagation.configureContextSnapshotFactory(true);
    }
}
```

这个功能需要 [context-propagation](https://github.com/micrometer-metrics/context-propagation) 库支持。它的原理是在 Scheduler 提交任务时，将当前 Context 中的值写入 ThreadLocal，在任务执行完毕后恢复。这为需要 ThreadLocal 的遗留库（如 MDC 日志）提供了桥接机制。

---

## 八、Context 在多订阅者场景中的处理

### 8.1 Sinks 中的多订阅者 Context 合并

源码文件：`reactor/core/publisher/SinkManyEmitterProcessor.java`

```java
// SinkManyEmitterProcessor.java
@Override
public Context currentContext() {
    return Operators.multiSubscribersContext(subscribers);
}
```

当 Sink 有多个订阅者时，`currentContext()` 调用 `Operators.multiSubscribersContext()` 合并所有订阅者的 Context。这个方法遍历所有订阅者的 Context 并执行 `putAll` 合并。

### 8.2 为什么需要合并？

在多播场景中，不同订阅者可能携带不同的 Context。例如：
- 订阅者 A 的 Context: `{traceId: "req-1"}`
- 订阅者 B 的 Context: `{userId: "user-2"}`

Sink 作为数据源需要为所有订阅者服务，它的 `currentContext()` 应该能看到所有订阅者的上下文信息。合并后的 Context: `{traceId: "req-1", userId: "user-2"}`。

**如果两个订阅者的 Context 有相同的 key 但不同的 value 怎么办？** `putAll` 的语义是"后者覆盖前者"，所以合并结果取决于遍历顺序。这种情况下，Sink 的 `currentContext()` 结果可能不确定。但实际上，多播场景中不同订阅者通常不应该设置相同的 Context key——如果需要，应该通过 `contextWrite()` 在 Sink 上游统一设置。

---

## 九、Context 不可变设计的深层原因

### 9.1 不可变保证线程安全

Context 的 `put()` 返回新实例，不修改当前实例。这意味着：
- 多个操作符可以同时读取同一个 Context 实例，无需同步
- 一个操作符修改 Context 不会影响其他操作符看到的 Context
- Context 实例可以被安全地缓存和共享（如 `Context0.INSTANCE`）

### 9.2 不可变保证传播一致性

```
contextWrite(ctx -> ctx.put("k", "v1"))
    .contextWrite(ctx -> ctx.put("k", "v2"))
    .subscribe(sub);

传播过程:
1. sub.currentContext() → Context0
2. 第二个 contextWrite: Context0.put("k","v2") → Context1{k=v2}
3. 第一个 contextWrite: Context1{k=v2}.put("k","v1") → Context1{k=v1}
4. 上游看到 Context1{k=v1}
```

下游的 `contextWrite` 先执行，上游的 `contextWrite` 后执行。由于不可变性，每次 `put` 都创建新实例，最终上游看到的是最后一个 `contextWrite` 的结果。如果 Context 是可变的，后执行的 `put` 会覆盖先执行的 `put`，导致不可预期的行为。

### 9.3 从不同视角看不可变设计

| 视角 | 不可变的好处 | 不可变的代价 |
|------|------------|------------|
| **线程安全** | 天然安全，无需锁 | 每次修改创建新对象 |
| **GC 压力** | 短生命周期对象在年轻代快速回收 | 频繁修改时产生大量临时对象 |
| **可追踪性** | 每次 put 产生新实例，可以追踪 Context 演化 | 调试时需要跟踪多个实例 |
| **API 安全** | 调用者不可能意外修改共享 Context | 调用者需要理解"put 返回新实例"的模式 |

---

## 十、Context 的工厂方法优化

源码文件：`reactor/util/context/Context.java`

```java
// Context.java
static Context of(Map<?, ?> map) {
    int size = Objects.requireNonNull(map, "map").size();
    if (size == 0) return Context.empty();
    if (size <= 5) {
        Map.Entry[] entries = map.entrySet().toArray(new Map.Entry[size]);
        switch (size) {
            case 1: return new Context1(entries[0].getKey(), entries[0].getValue());
            case 2: return new Context2(/* ... */);
            // ... up to 5
        }
    }
    // 6+ entries: validate nulls then create ContextN
    map.forEach((key, value) -> {
        Objects.requireNonNull(key, "null key found");
        if (value == null) {
            throw new NullPointerException("null value for key " + key);
        }
    });
    return new ContextN((Map<Object, Object>) map);
}
```

**为什么 `Context.of(Map)` 在 size <= 5 时不用 `ContextN`？** 因为从 Map 创建 `ContextN` 会保留 Map 的内部结构（Node 数组等），而 `Context1-5` 只需要提取键值对到 final 字段。对于小 Map，字段级实现的内存和访问性能都更优。

`Context.of(Map)` 也是一个"压缩"方法——如果传入的 Map 只有 2 个条目，返回的是 `Context2` 而不是 `ContextN`，这确保了无论来源如何，Context 始终使用最优的存储方式。

---

## 十一、归纳表格：Context 实现类对照表

| 实现类 | 键值对数 | 存储方式 | 接口 | put 行为 | delete 行为 | 特殊优化 |
|--------|---------|---------|------|---------|------------|---------|
| `Context0` | 0 | 无字段（单例） | `CoreContext` | 返回 `Context1` | 返回 `this` | 全局单例 `INSTANCE`，零内存 |
| `Context1` | 1 | `final Object key, value` | `CoreContext` | 同key→`Context1`；新key→`Context2` | 删唯一key→`Context0` | 单次 equals 查找 |
| `Context2` | 2 | `final Object key1,value1,key2,value2` | `CoreContext` | 同key→`Context2`；新key→`Context3` | 降至→`Context1` | 2次 equals 查找 |
| `Context3` | 3 | 3对 final 字段 | `CoreContext` | 同key→`Context3`；新key→`Context4` | 降至→`Context2` | 3次 equals 查找 |
| `Context4` | 4 | 4对 final 字段 | `CoreContext` | 同key→`Context4`；新key→`Context5` | 降至→`Context3` | 4次 equals 查找 |
| `Context5` | 5 | 5对 final 字段 | `CoreContext` | 同key→`Context5`；新key→`ContextN` | 降至→`Context4` | 5次 equals 查找 |
| `ContextN` | 6+ | `LinkedHashMap<Object,Object>` | `CoreContext` | copy-on-write 新 Map | 降至5个→`Context5`；否则新 Map | O(1) hashCode 查找；实现 `BiConsumer`/`Consumer` 避免 lambda 分配 |
| `ContextView` | N/A | 只读接口 | — | N/A | N/A | 定义 get/hasKey/size/stream |
| `Context` | N/A | 可写接口(extends ContextView) | — | `put` 返回新实例 | `delete` 返回新实例 | 定义 put/delete/putAll |
| `CoreContext` | N/A | 内部优化接口(extends Context) | — | N/A | N/A | `putAllInto`/`unsafePutAllInto` 高效合并 |
