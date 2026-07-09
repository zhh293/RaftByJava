# Hooks 钩子机制与错误处理全流程源码解析

> **Reactor Core 源码深度研究系列 · 第 11 篇**

本文深入剖析 Reactor Core 中 Hooks 全局钩子机制的设计架构、触发时机、错误处理全流程以及装配跟踪机制。所有分析基于真实源码，引用真实类名、字段名和方法名。

---

## 一、全局架构总览

```
                    Hooks 钩子插入点全景图
                    =====================

  装配阶段 (Assembly Time):
  ┌────────────────────────────────────────────────────────────────┐
  │  flux.map(fn1).filter(fn2).map(fn3).subscribe()                │
  │       │           │           │                                │
  │       ▼           ▼           ▼                                │
  │  [onEachOperator] [onEachOperator] [onEachOperator + onLastOperator] │
  │       │           │           │                                │
  │       ▼           ▼           ▼                                │
  │  每个操作符创建后  每个操作符创建后  最后一个操作符创建后             │
  │  都经过 hook 包装  都经过 hook 包装  额外经过 onLastOperator 包装  │
  └────────────────────────────────────────────────────────────────┘

  运行时错误处理:
  ┌──────────────────────────────────────────────────────────────┐
  │  Source.onNext(item)                                          │
  │       │                                                       │
  │       ▼                                                       │
  │  操作符处理 item 时出错?                                        │
  │       │                                                       │
  │       ├── 是 → OnNextFailureStrategy 处理                     │
  │       │         ├── STOP (默认): cancel + onError              │
  │       │         ├── RESUME_DROP: drop value + drop error      │
  │       │         └── RESUME: 调用用户 consumer，继续序列         │
  │       │                                                       │
  │       ├── 操作符已终止后收到数据 → onNextDropped                 │
  │       ├── 操作符已终止后收到错误 → onErrorDropped                │
  │       └── 取消时队列中的元素 → onDiscard                         │
  └──────────────────────────────────────────────────────────────┘
```

```
  Hooks 分类:
  ┌─────────────────────────────────────────────────────┐
  │  转换型钩子 (Transformative)                          │
  │  ├── onEachOperator: 每个操作符创建后包装              │
  │  ├── onLastOperator: 最后一个操作符创建后包装          │
  │  └── onOperatorError: 操作符错误映射                   │
  ├─────────────────────────────────────────────────────┤
  │  回调型钩子 (Callback)                                │
  │  ├── onNextDropped: 丢弃数据时的回调                  │
  │  └── onErrorDropped: 丢弃错误时的回调                 │
  ├─────────────────────────────────────────────────────┤
  │  策略型钩子 (Strategy)                                │
  │  ├── onNextError: onNext 错误恢复策略                 │
  │  └── onDiscard: 取消时元素清理 (通过 Context 传递)     │
  └─────────────────────────────────────────────────────┘
```

Hooks 是 Reactor 提供的全局拦截机制，允许在不修改操作符代码的情况下，对装配过程和运行时错误处理进行横向切面增强。典型应用场景包括：调试跟踪（`onOperatorDebug`）、指标收集（`FluxMetrics`）、错误恢复策略定制等。

---

## 二、Hooks 的命名子钩子机制

源码文件：`reactor/core/publisher/Hooks.java`

### 2.1 LinkedHashMap 存储与按 key 管理

```java
// Hooks.java
// 转换型钩子使用 LinkedHashMap 存储命名子钩子
private static final LinkedHashMap<String, Function<? super Publisher<Object>, ? extends Publisher<Object>>> onEachOperatorHooks;
private static final LinkedHashMap<String, Function<? super Publisher<Object>, ? extends Publisher<Object>>> onLastOperatorHooks;
private static final LinkedHashMap<String, BiFunction<? super Throwable, @Nullable Object, ? extends Throwable>> onOperatorErrorHooks;

static {
    onEachOperatorHooks = new LinkedHashMap<>(1);
    onLastOperatorHooks = new LinkedHashMap<>(1);
    onOperatorErrorHooks = new LinkedHashMap<>(1);
}
```

### 2.2 onEachOperator 的添加与替换

```java
// Hooks.java
public static void onEachOperator(String key, 
        Function<? super Publisher<Object>, ? extends Publisher<Object>> onEachOperator) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(onEachOperator, "onEachOperator");
    log.debug("Hooking onEachOperator: {}", key);

    synchronized (log) {
        onEachOperatorHooks.put(key, onEachOperator);  // 同 key 替换，不同 key 添加
        onEachOperatorHook = createOrUpdateOpHook(onEachOperatorHooks.values());
    }
}
```

### 2.3 子钩子组合机制

```java
// Hooks.java
@SuppressWarnings({"unchecked", "rawtypes"})
static @Nullable Function<Publisher, Publisher> createOrUpdateOpHook(
        Collection<Function<? super Publisher<Object>, ? extends Publisher<Object>>> hooks) {
    Function<Publisher, Publisher> composite = null;
    for (Function<? super Publisher<Object>, ? extends Publisher<Object>> function : hooks) {
        Function<? super Publisher, ? extends Publisher> op = 
            (Function<? super Publisher, ? extends Publisher>) function;
        if (composite != null) {
            composite = composite.andThen(op);  // 链式组合
        }
        else {
            composite = (Function<Publisher, Publisher>) op;
        }
    }
    return composite;
}
```

**为什么用 `LinkedHashMap` 而不是 `HashMap`？** 因为 `LinkedHashMap` 保持插入顺序，`createOrUpdateOpHook` 按迭代顺序组合子钩子，保证了子钩子的执行顺序是确定的。如果用 `HashMap`，不同 JVM 版本的迭代顺序可能不同，导致钩子执行顺序不确定。

**命名子钩子的设计动机：** 假设有两个库 A 和 B 都需要注册 `onEachOperator` 钩子。如果用简单的 `List`，移除 A 的钩子时无法精确定位。用命名 `LinkedHashMap`，A 注册为 `onEachOperator("libA", fnA)`，B 注册为 `onEachOperator("libB", fnB)`，移除 A 只需 `resetOnEachOperator("libA")`，不影响 B。

### 2.4 替换时保持执行顺序

```java
// Hooks.java javadoc:
// Note that sub-hooks are cumulative. Invoking this method twice with the same key will
// replace the old sub-hook with that name, but keep the execution order (eg. A-h1, B-h2,
// A-h3 will keep A-B execution order, leading to hooks h3 then h2 being executed).
```

这意味着：先用 key="A" 注册 h1，再用 key="B" 注册 h2，再用 key="A" 替换为 h3。执行顺序是 h3 → h2（A 的位置不变，只是替换了内容）。这种设计让库可以安全地更新自己的钩子而不影响其他库的执行顺序。

---

## 三、onEachOperator vs onLastOperator：触发时机

### 3.1 onEachOperator：每个操作符后触发

`onEachOperator` 在每个操作符创建时被调用。例如 `flux.map(fn1).filter(fn2)` 会触发两次：`map` 创建后一次，`filter` 创建后一次。

```java
// Hooks.java
static @Nullable Function<Publisher, Publisher> onEachOperatorHook;
```

这个字段缓存了组合后的钩子函数。每次操作符创建时，Reactor 会检查 `onEachOperatorHook` 是否为 null，如果不为 null，就将操作符 Publisher 传入钩子函数进行包装。

### 3.2 onLastOperator：最终订阅前触发

`onLastOperator` 只在最后一个操作符（即直接连接到 `subscribe()` 的操作符）创建时触发。

```java
// Hooks.java
static volatile @Nullable Function<Publisher, Publisher> onLastOperatorHook;
```

### 3.3 两者的区别与联系

```
flux.map(fn).filter(fn2).subscribe(sub)

onEachOperator 触发点:
  map 创建后 → onEachOperatorHook.apply(mapPublisher)
  filter 创建后 → onEachOperatorHook.apply(filterPublisher)

onLastOperator 触发点:
  filter 创建后 (因为它是最后一个) → onLastOperatorHook.apply(filterPublisher)
```

**为什么需要两个不同的钩子？** `onEachOperator` 适用于需要对每个操作符都进行包装的场景，如全局调试跟踪（记录每个操作符的创建位置）。`onLastOperator` 适用于只需要在订阅入口处进行拦截的场景，如全局指标收集（只需要知道最终订阅了什么，不需要每个中间操作符都包装）。

从性能角度看，`onEachOperator` 的开销与操作符链长度成正比——每个操作符都会被包装一层。如果操作符链有 20 个操作符，就会包装 20 层。`onLastOperator` 只包装一次，开销固定。

---

## 四、onNextDropped：终止后的数据丢弃

源码文件：`reactor/core/publisher/Operators.java`（第689-702行）

```java
// Operators.java
public static <T> void onNextDropped(T t, Context context) {
    Objects.requireNonNull(t, "onNext");
    Objects.requireNonNull(context, "context");
    Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_NEXT_DROPPED, null);
    if (hook == null) {
        hook = Hooks.onNextDroppedHook;  // 回退到全局钩子
    }
    if (hook != null) {
        hook.accept(t);
    }
    else if (log.isDebugEnabled()) {
        log.debug("onNextDropped: " + t);  // 默认: DEBUG 级别日志
    }
}
```

### 4.1 什么时候触发 onNextDropped？

`onNextDropped` 在以下场景触发：
1. **操作符已终止后收到新数据**：例如 `Flux` 已经 `onComplete()`，但上游仍调用 `onNext()`
2. **操作符已取消后收到新数据**：例如下游已 `cancel()`，但上游未及时停止仍推送数据
3. **Sink 已终止后调用 `tryEmitNext` 的 `emitNext` 便捷 API**：`EmitResult.FAIL_TERMINATED` 时，`emitNext` 会调用 `Operators.onNextDropped`

### 4.2 两级查找策略

`onNextDropped` 使用两级查找：
1. 先查找 Context 中的 `reactor.onNextDropped.local`（序列级钩子）
2. 如果没有，查找全局 `Hooks.onNextDroppedHook`
3. 如果都没有，默认 DEBUG 级别日志记录

**为什么优先查找 Context？** 因为 Context 级钩子是序列隔离的——不同序列可以有不同的 drop 策略。例如，一个序列可能需要将 drop 的数据写入死信队列，另一个序列可能只需要日志记录。全局钩子是"兜底"策略，只在序列没有定义自己的钩子时生效。

### 4.3 Hooks.onNextDropped 的注册

```java
// Hooks.java
public static void onNextDropped(Consumer<Object> c) {
    Objects.requireNonNull(c, "onNextDroppedHook");
    log.debug("Hooking new default : onNextDropped");
    synchronized(log) {
        if (onNextDroppedHook != null) {
            onNextDroppedHook = onNextDroppedHook.andThen(c);  // 累加
        }
        else {
            onNextDroppedHook = c;
        }
    }
}

public static void onNextDroppedFail() {
    log.debug("Enabling failure mode for onNextDropped");
    synchronized(log) {
        onNextDroppedHook = n -> {throw Exceptions.failWithCancel();};  // 抛异常模式
    }
}
```

`onNextDropped` 是累加的——多次调用会通过 `andThen` 串联多个 Consumer。`onNextDroppedFail()` 是一个特殊模式，将 drop 行为改为抛出 `CancelException`，用于在测试或严格模式下快速发现数据丢失问题。

---

## 五、onErrorDropped：终止后的错误丢弃

源码文件：`reactor/core/publisher/Operators.java`（第667-677行）

```java
// Operators.java
public static void onErrorDropped(Throwable e, Context context) {
    Consumer<? super Throwable> hook = context.getOrDefault(Hooks.KEY_ON_ERROR_DROPPED, null);
    if (hook == null) {
        hook = Hooks.onErrorDroppedHook;
    }
    if (hook == null) {
        log.error("Operator called default onErrorDropped", e);  // 默认: ERROR 级别日志
        return;
    }
    hook.accept(e);
}
```

**注意默认日志级别的差异：** `onNextDropped` 默认是 DEBUG 级别（因为数据丢弃在响应式流中比较常见），而 `onErrorDropped` 默认是 ERROR 级别（因为错误丢弃通常意味着有未处理的异常，需要引起注意）。

**什么时候触发 onErrorDropped？**
- 操作符已经 `onComplete()` 或 `onError()` 后，上游又发来 `onError()`
- `Sinks.Many.emitError()` 在 `tryEmitError` 返回 `FAIL_TERMINATED` 时，通过 `Operators.onErrorDropped` 丢弃重复的错误信号

---

## 六、onDiscard：取消时的元素清理

源码文件：`reactor/core/publisher/Operators.java`（第437-447行）

### 6.1 onDiscard 的核心实现

```java
// Operators.java
public static <T> void onDiscard(@Nullable T element, Context context) {
    Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
    if (element != null && hook != null) {
        try {
            hook.accept(element);
        }
        catch (Throwable t) {
            log.warn("Error in discard hook", t);
        }
    }
}
```

`onDiscard` 只通过 Context 中的 `reactor.onDiscard.local` 钩子触发，**没有全局钩子回退**。这意味着用户必须通过 `contextWrite(ctx -> ctx.put("reactor.onDiscard.local", cleanupConsumer))` 注册清理逻辑。

### 6.2 onDiscardQueueWithClear：队列清理

```java
// Operators.java
public static <T> void onDiscardQueueWithClear(
        @Nullable Queue<T> queue, Context context,
        @Nullable Function<T, Stream<?>> extract) {
    if (queue == null) {
        return;
    }
    Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
    if (hook == null) {
        queue.clear();  // 没有钩子时直接 clear
        return;
    }
    // 有钩子时逐个 poll 并调用 hook
    try {
        for(;;) {
            T toDiscard = queue.poll();
            if (toDiscard == null) {
                break;
            }
            if (extract != null) {
                // 提取嵌套元素再 discard
                extract.apply(toDiscard).forEach(elementToDiscard -> {
                    try { hook.accept(elementToDiscard); }
                    catch (Throwable t) { log.warn("...", t); }
                });
            }
            else {
                try { hook.accept(toDiscard); }
                catch (Throwable t) { log.warn("...", t); }
            }
        }
    }
    catch (Throwable t) {
        log.warn("Cannot further apply discard hook...", t);
    }
}
```

**关键设计：没有钩子时直接 `queue.clear()`，有钩子时逐个 `poll` + `hook.accept`。** 因为 `queue.clear()` 是 O(1) 操作（直接重置 head/tail 指针），而逐个 poll 是 O(n)。只有在用户注册了清理钩子时才付出 O(n) 的代价，这是"按需付费"设计。

### 6.3 onDiscard 在 SinkManyUnicast 中的调用

```java
// SinkManyUnicast.java
boolean checkTerminated(boolean d, boolean empty, CoreSubscriber<? super T> a, 
                        Queue<T> q, @Nullable T t) {
    if (cancelled) {
        Operators.onDiscard(t, a.currentContext());              // 丢弃当前元素
        Operators.onDiscardQueueWithClear(q, a.currentContext(), null);  // 清空队列
        hasDownstream = false;
        return true;
    }
    // ...
}

@Override
public void cancel() {
    if (cancelled) return;
    cancelled = true;
    doTerminate();
    if (WIP.getAndIncrement(this) == 0) {
        if (!outputFused) {
            Operators.onDiscardQueueWithClear(queue, currentContext(), null);  // 取消时清理
        }
        hasDownstream = false;
    }
}
```

**反例：如果不做 onDiscard，数据库连接在取消时可能泄漏。** 假设有一个响应式流从数据库读取数据：

```java
flux.flatMap(item -> {
    Connection conn = dataSource.getConnection();  // 获取连接
    return queryWithConn(conn)
        .doFinally(signal -> conn.close());          // 正常完成或错误时关闭
})
.subscribe();
```

如果下游在 `flatMap` 内部队列中已经缓冲了 10 个带连接的 item，此时用户取消订阅。没有 `onDiscard` 时，这些 item 被 `queue.clear()` 直接丢弃，`doFinally` 不会触发（因为 item 从未到达下游），连接永远不会被关闭。

通过 `onDiscard` 注册清理钩子：

```java
flux.contextWrite(ctx -> ctx.put("reactor.onDiscard.local", (Object obj) -> {
    if (obj instanceof ConnectionHolder) {
        ((ConnectionHolder) obj).close();
    }
}))
```

取消时，`onDiscardQueueWithClear` 会逐个 poll 队列中的元素并调用清理钩子，确保所有连接都被正确关闭。

---

## 七、OnNextFailureStrategy：onNext 错误恢复策略

源码文件：`reactor/core/publisher/OnNextFailureStrategy.java`

### 7.1 策略接口

```java
// OnNextFailureStrategy.java
interface OnNextFailureStrategy extends BiFunction<Throwable, Object, Throwable>,
                                        BiPredicate<Throwable, Object> {
    String KEY_ON_NEXT_ERROR_STRATEGY = "reactor.onNextError.localStrategy";

    // 判断是否可以恢复此错误
    boolean test(Throwable throwable, @Nullable Object o);

    // 处理错误：返回 null 表示已恢复，返回 Throwable 表示需要传播
    @Nullable Throwable process(Throwable error, @Nullable Object value, Context context);
}
```

### 7.2 内置策略

```java
// OnNextFailureStrategy.java
// 1. STOP 策略（默认）：不恢复任何错误
OnNextFailureStrategy STOP = new OnNextFailureStrategy() {
    @Override
    public boolean test(Throwable error, @Nullable Object value) {
        return false;  // 不恢复任何错误
    }

    @Override
    public Throwable process(Throwable error, @Nullable Object value, Context context) {
        Exceptions.throwIfFatal(error);
        Throwable iee = new IllegalStateException("STOP strategy cannot process errors");
        iee.addSuppressed(error);
        return iee;
    }
};

// 2. RESUME_DROP 策略：恢复所有错误，丢弃值和错误
static OnNextFailureStrategy resumeDrop() { return RESUME_DROP; }

OnNextFailureStrategy RESUME_DROP = new ResumeDropStrategy(null);

// 3. RESUME 策略：恢复所有错误，调用用户 consumer
static OnNextFailureStrategy resume(BiConsumer<Throwable, Object> errorConsumer) {
    return new ResumeStrategy(null, errorConsumer);
}

// 4. RESUME_IF 策略：按谓词恢复
static OnNextFailureStrategy resumeIf(Predicate<Throwable> causePredicate,
                                       BiConsumer<Throwable, Object> errorConsumer) {
    return new ResumeStrategy(causePredicate, errorConsumer);
}
```

### 7.3 ResumeDropStrategy 的处理逻辑

```java
// OnNextFailureStrategy.java
final class ResumeDropStrategy implements OnNextFailureStrategy {
    final @Nullable Predicate<Throwable> errorPredicate;

    @Override
    public @Nullable Throwable process(Throwable error, @Nullable Object value, Context context) {
        if (errorPredicate == null) {
            Exceptions.throwIfFatal(error);  // 致命错误仍然抛出
        }
        else if (!errorPredicate.test(error)) {
            Exceptions.throwIfFatal(error);
            return error;  // 不匹配谓词，返回错误让操作符传播
        }
        try {
            if (value != null) {
                Operators.onNextDropped(value, context);  // 丢弃值
            }
            Operators.onErrorDropped(error, context);      // 丢弃错误
            return null;  // 返回 null 表示已恢复
        }
        catch (Throwable e) {
            return Exceptions.addSuppressed(e, error);
        }
    }
}
```

### 7.4 Operators.onNextError 的策略查找与执行

```java
// Operators.java
static final OnNextFailureStrategy onNextErrorStrategy(Context context) {
    OnNextFailureStrategy strategy = null;
    // 1. 先查 Context 中的本地策略
    BiFunction<? super Throwable, Object, ? extends Throwable> fn = context.getOrDefault(
            OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY, null);
    if (fn instanceof OnNextFailureStrategy) {
        strategy = (OnNextFailureStrategy) fn;
    } else if (fn != null) {
        strategy = new OnNextFailureStrategy.LambdaOnNextErrorStrategy(fn);
    }
    // 2. 再查全局 Hook
    if (strategy == null) strategy = Hooks.onNextErrorHook;
    // 3. 默认 STOP
    if (strategy == null) strategy = OnNextFailureStrategy.STOP;
    return strategy;
}

public static <T> @Nullable Throwable onNextError(@Nullable T value, Throwable error, 
        Context context, Subscription subscriptionForCancel) {
    error = unwrapOnNextError(error);
    OnNextFailureStrategy strategy = onNextErrorStrategy(context);
    if (strategy.test(error, value)) {
        // 策略可以处理此错误
        Throwable t = strategy.process(error, value, context);
        if (t != null) {
            subscriptionForCancel.cancel();  // 有返回值 → 取消上游
        }
        return t;  // null → 恢复，继续序列；非 null → 传播错误
    }
    else {
        // 策略不处理 → 回退到 onOperatorError
        return onOperatorError(subscriptionForCancel, error, value, context);
    }
}
```

**策略查找的三级回退机制：**
1. Context 中的 `reactor.onNextError.localStrategy`（序列级，最高优先级）
2. 全局 `Hooks.onNextErrorHook`
3. 默认 `OnNextFailureStrategy.STOP`

**为什么需要三级回退？** 默认的 STOP 策略保证了 Reactive Streams 的标准行为——任何 `onNext` 中的错误都会终止序列。但某些场景需要"继续处理后续数据"（如日志分析管道中单条记录解析失败不应中断整个管道）。通过 Context 级策略，不同序列可以使用不同的恢复策略，而全局钩子提供了默认的恢复行为。

---

## 八、Exceptions：异常工具类与致命错误处理

源码文件：`reactor/core/Exceptions.java`

### 8.1 throwIfFatal：致命错误直接抛出

```java
// Exceptions.java
public static void throwIfFatal(@Nullable Throwable t) {
    if (t == null) {
        return;
    }
    if (isFatalButNotJvmFatal(t)) {
        LOGGER.warn("throwIfFatal detected a fatal exception...", t);
        throw (RuntimeException) t;  // BubblingException, ErrorCallbackNotImplemented
    }
    if (isJvmFatal(t)) {
        LOGGER.warn("throwIfFatal detected a jvm fatal exception...", t);
        throw (Error) t;  // VirtualMachineError, ThreadDeath, LinkageError
    }
}

public static boolean isFatal(@Nullable Throwable t) {
    return isFatalButNotJvmFatal(t) || isJvmFatal(t);
}

static boolean isFatalButNotJvmFatal(@Nullable Throwable t) {
    return t instanceof BubblingException || t instanceof ErrorCallbackNotImplemented;
}

public static boolean isJvmFatal(@Nullable Throwable t) {
    return t instanceof VirtualMachineError ||
        t instanceof ThreadDeath ||
        t instanceof LinkageError;
}
```

**为什么某些异常被认为是"致命"的？** Reactive Streams 规范允许通过 `onError` 传播任何 `Throwable`。但有些异常表示 JVM 或 Reactor 本身处于不可恢复的状态：
- `VirtualMachineError`：JVM 内部错误（如 `OutOfMemoryError`），继续执行可能导致数据损坏
- `LinkageError`：类加载失败，后续操作可能依赖该类
- `BubblingException`：Reactor 内部的"向上冒泡"信号，不应该被 `onError` 捕获
- `ErrorCallbackNotImplemented`：下游没有实现 `onError` 回调，继续传播无意义

这些异常直接 `throw` 而不是通过 `onError` 传播，是为了让调用者立即感知到不可恢复的故障。

### 8.2 异常包装机制

```java
// Exceptions.java
public static RuntimeException propagate(Throwable t) {
    throwIfFatal(t);
    if (t instanceof RuntimeException) {
        return (RuntimeException) t;  // RuntimeException 直接返回
    }
    return new ReactiveException(t);  // Checked exception 包装为 RuntimeException
}

public static RuntimeException bubble(Throwable t) {
    throwIfFatal(t);
    return new BubblingException(t);  // 包装为 BubblingException（致命，会向上抛）
}
```

**`propagate` vs `bubble` 的区别：**
- `propagate`：将 checked exception 包装为 `ReactiveException`，通过 `onError` 传播
- `bubble`：包装为 `BubblingException`，标记为"致命"，直接 `throw` 绕过 `onError`

### 8.3 addThrowable：多异常合并

```java
// Exceptions.java
public static <T> boolean addThrowable(
        AtomicReferenceFieldUpdater<T, @Nullable Throwable> field,
        T instance, Throwable exception) {
    for (; ; ) {
        Throwable current = field.get(instance);
        if (current == TERMINATED) {
            return false;  // 已终止，拒绝添加
        }
        if (current instanceof CompositeException) {
            current.addSuppressed(exception);  // 已是复合异常，追加
            return true;
        }
        Throwable update;
        if (current == null) {
            update = exception;  // 首个异常
        } else {
            update = multiple(current, exception);  // 合并为复合异常
        }
        if (field.compareAndSet(instance, current, update)) {
            return true;
        }
    }
}
```

这个方法使用 CAS 循环安全地向 `AtomicReferenceFieldUpdater` 添加异常。如果已有异常，创建 `CompositeException`（"Multiple exceptions"）将两个异常合并。如果已有 `CompositeException`，直接 `addSuppressed` 追加。`TERMINATED` 单例用于标记"已终止，不再接受异常"。

### 8.4 异常类型体系

```java
// Exceptions.java 中的异常类型层次:
// 
// Error
//   └── StaticThrowable           (静态字段安全使用，不填充堆栈)
//
// RuntimeException
//   └── ReactiveException         (基础响应式异常)
//        ├── SourceException       (上游 onError 的包装)
//        └── CompositeException    (多异常合并)
//   └── BubblingException          (致命，向上冒泡)
//        └── CancelException       (取消信号)
//   └── ErrorCallbackNotImplemented (致命，onError 未实现)
//
// IllegalStateException
//   └── OverflowException          (背压溢出)
//   └── RetryExhaustedException    (重试耗尽)
//
// RejectedExecutionException
//   └── ReactorRejectedExecutionException (调度器拒绝)
//   └── StaticRejectedExecutionException  (静态字段安全使用)
```

每种异常类型都有特定的用途和检测方法（`isOverflow`、`isCancel`、`isBubbling`、`isMultiple`、`isTraceback`、`isRetryExhausted`），让操作符和用户代码可以按异常类型做不同的恢复决策。

---

## 九、onOperatorError：操作符错误映射

源码文件：`reactor/core/publisher/Operators.java`（第752-779行）

```java
// Operators.java
public static Throwable onOperatorError(@Nullable Subscription subscription,
        Throwable error, @Nullable Object dataSignal, Context context) {
    Exceptions.throwIfFatal(error);       // 致命错误直接抛
    if (subscription != null) {
        subscription.cancel();            // 取消上游
    }

    Throwable t = Exceptions.unwrap(error);  // 解包 ReactiveException
    BiFunction<? super Throwable, @Nullable Object, ? extends Throwable> hook =
            context.getOrDefault(Hooks.KEY_ON_OPERATOR_ERROR, null);
    if (hook == null) {
        hook = Hooks.onOperatorErrorHook;  // 回退到全局钩子
    }
    if (hook == null) {
        // 默认行为：如果 dataSignal 是异常，作为 suppressed 添加
        if (dataSignal != null) {
            if (dataSignal != t && dataSignal instanceof Throwable) {
                t = Exceptions.addSuppressed(t, (Throwable) dataSignal);
            }
        }
        return t;
    }
    return hook.apply(error, dataSignal);  // 应用用户钩子
}
```

**`onOperatorError` 与 `onNextError` 的区别：**
- `onNextError`：专门处理 `onNext` 调用中的错误，有恢复策略（可以继续序列）
- `onOperatorError`：处理操作符内部的一般性错误（如 `map` 函数抛异常），没有恢复策略（总是取消 + 传播）

`onOperatorError` 的默认行为是将 `dataSignal`（触发错误的数据或异常）作为 `suppressed` 添加到错误中。这让调试时可以看到是什么数据导致了错误。

---

## 十、FluxOnAssembly：装配跟踪与 checkpoint

源码文件：`reactor/core/publisher/FluxOnAssembly.java`

### 10.1 AssemblySnapshot：装配快照

```java
// FluxOnAssembly.java
static class AssemblySnapshot {
    final           boolean          isCheckpoint;
    final @Nullable String           description;
    final @Nullable Supplier<String> assemblyInformationSupplier;
    @Nullable String cached;

    AssemblySnapshot(@Nullable String description, Supplier<String> assemblyInformationSupplier) {
        this(description != null, description, assemblyInformationSupplier);
    }

    AssemblySnapshot(String assemblyInformation) {
        this.isCheckpoint = false;
        this.description = null;
        this.assemblyInformationSupplier = null;
        this.cached = assemblyInformation;
    }

    String toAssemblyInformation() {
        if (cached == null) {
            if (assemblyInformationSupplier == null) {
                throw new IllegalStateException("...");
            }
            cached = assemblyInformationSupplier.get();  // 延迟计算堆栈
        }
        return cached;
    }
}
```

**为什么 `assemblyInformationSupplier` 是 `Supplier<String>` 而不是直接 `String`？** 因为获取堆栈跟踪（`new Throwable().getStackTrace()`）是一个昂贵的操作。使用 `Supplier` 延迟计算，只有在真正需要查看装配信息时（如出错时）才执行堆栈捕获。如果序列正常完成，装配信息的计算开销被完全避免。

### 10.2 FluxOnAssembly 的装配包装

```java
// FluxOnAssembly.java
final class FluxOnAssembly<T> extends InternalFluxOperator<T, T> implements Fuseable, AssemblyOp {
    final AssemblySnapshot snapshotStack;

    FluxOnAssembly(Flux<? extends T> source, AssemblySnapshot snapshotStack) {
        super(source);
        this.snapshotStack = snapshotStack;
    }

    @Override
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
        return wrapSubscriber(actual, source, this, snapshotStack);
    }

    static <T> CoreSubscriber<? super T> wrapSubscriber(CoreSubscriber<? super T> actual,
            Flux<? extends T> source, Publisher<?> current,
            @Nullable AssemblySnapshot snapshotStack) {
        if (snapshotStack != null) {
            if (actual instanceof ConditionalSubscriber) {
                return new OnAssemblyConditionalSubscriber<>(...);
            }
            else {
                return new OnAssemblySubscriber<>(actual, snapshotStack, source, current);
            }
        }
        return actual;
    }
}
```

### 10.3 Hooks.onOperatorDebug：全局调试模式

```java
// Hooks.java
public static void onOperatorDebug() {
    log.debug("Enabling stacktrace debugging via onOperatorDebug");
    GLOBAL_TRACE = true;
}

static boolean GLOBAL_TRACE = initStaticGlobalTrace();

static boolean initStaticGlobalTrace() {
    return Boolean.parseBoolean(System.getProperty("reactor.trace.operatorStacktrace", "false"));
}
```

`GLOBAL_TRACE` 为 `true` 时，每个操作符创建时都会捕获堆栈并包装为 `FluxOnAssembly`。这是一个全局开关，也可以通过系统属性 `reactor.trace.operatorStacktrace=true` 启用。

**为什么默认关闭？** 因为每个操作符都捕获堆栈的性能开销极大——一次堆栈捕获可能需要 1-10ms，操作符链有 20 个操作符就需要 20-200ms 的额外装配时间。在生产环境中这是不可接受的，所以只在调试时启用。

### 10.4 Hooks.addAssemblyInfo 的装配注入

```java
// Hooks.java
@SuppressWarnings("unchecked")
static <T, P extends Publisher<T>> Publisher<T> addAssemblyInfo(P publisher, AssemblySnapshot stacktrace) {
    if (publisher instanceof Callable) {
        if (publisher instanceof Mono) {
            return new MonoCallableOnAssembly<>((Mono<T>) publisher, stacktrace);
        }
        return new FluxCallableOnAssembly<>((Flux<T>) publisher, stacktrace);
    }
    if (publisher instanceof Mono) {
        return new MonoOnAssembly<>((Mono<T>) publisher, stacktrace);
    }
    if (publisher instanceof ParallelFlux) {
        return new ParallelFluxOnAssembly<>((ParallelFlux<T>) publisher, stacktrace);
    }
    if (publisher instanceof ConnectableFlux) {
        return new ConnectableFluxOnAssembly<>((ConnectableFlux<T>) publisher, stacktrace);
    }
    return new FluxOnAssembly<>((Flux<T>) publisher, stacktrace);
}
```

这个方法根据 Publisher 的具体类型（`Mono`、`Flux`、`ParallelFlux`、`ConnectableFlux`、`Callable`）选择对应的 `OnAssembly` 包装类。这种类型分派确保了装配跟踪不会改变 Publisher 的语义类型。

### 10.5 checkpoint() 操作符

`checkpoint()` 是 `FluxOnAssembly` 的用户级 API。与 `Hooks.onOperatorDebug()` 的全局跟踪不同，`checkpoint()` 只在指定位置插入装配跟踪：

```java
// Flux.java 中的 checkpoint 实现（简化）
// checkpoint(description) 创建一个 AssemblySnapshot(isCheckpoint=true, description, ...)
// 然后包装为 FluxOnAssembly
```

`AssemblySnapshot.isCheckpoint` 字段区分了 `checkpoint()` 和 `onOperatorDebug()` 产生的跟踪——`checkpoint` 的跟踪信息更简洁，通常只包含描述字符串和调用位置，而不是完整堆栈。

---

## 十一、错误处理全流程：从 Source 到 Subscriber

### 11.1 完整的错误传播路径

```
Source.onError(exception)
    │
    ▼
操作符 (如 FluxMap.MapSubscriber)
    │
    ├── 1. Exceptions.throwIfFatal(error) → 致命异常直接 throw
    │
    ├── 2. 是否在 onNext 中出错?
    │       ├── 是 → Operators.onNextError(value, error, context, subscription)
    │       │         ├── strategy.test(error, value)?
    │       │         │     ├── 是 → strategy.process() → null(恢复) 或 Throwable(传播)
    │       │         │     └── 否 → onOperatorError(subscription, error, value, context)
    │       │         └── 返回值: null → 恢复(继续序列) / Throwable → 传播
    │       │
    │       └── 否 → onOperatorError(subscription, error, context)
    │                    ├── throwIfFatal
    │                    ├── subscription.cancel()
    │                    ├── unwrap(error)
    │                    ├── 查找 Context/全局 hook
    │                    └── 返回映射后的 Throwable
    │
    ├── 3. actual.onError(mappedError) → 传播给下游
    │
    └── 下游继续处理...
```

### 11.2 从不同视角看错误处理

| 视角 | 关注点 | 机制 |
|------|-------|------|
| **Source 视角** | 如何通知下游出错 | `onError(throwable)` |
| **操作符视角** | 如何处理内部错误 | `onNextError` 策略 + `onOperatorError` 映射 |
| **订阅者视角** | 如何感知和处理错误 | `onError` 回调 |
| **全局视角** | 如何拦截所有错误 | `Hooks.onOperatorError` + `Hooks.onNextError` |
| **序列视角** | 如何自定义错误处理 | Context 中存储本地钩子 |
| **清理视角** | 如何在错误/取消时清理资源 | `onDiscard` + `doFinally` |

### 11.3 错误处理中的 Context 传播

所有错误处理方法都接受 `Context` 参数，这保证了序列级的错误处理钩子可以在任何操作符中使用。Context 的传播路径是：`Subscriber.currentContext()` → `InnerOperator.currentContext()` → 委托给 `actual.currentContext()`。

这意味着即使错误发生在 `publishOn` 切换线程后，操作符仍然能获取到正确的 Context，因为 Context 存储在订阅者对象中而非线程中。

---

## 十二、Hooks 的队列包装机制

源码文件：`reactor/core/publisher/Hooks.java`

```java
// Hooks.java
private static final LinkedHashMap<String, Function<Queue<?>, Queue<?>>> QUEUE_WRAPPERS = new LinkedHashMap<>(1);
private static Function<Queue<?>, Queue<?>> QUEUE_WRAPPER = Function.identity();

public static void addQueueWrapper(String key, Function<Queue<?>, Queue<?>> decorator) {
    synchronized (QUEUE_WRAPPERS) {
        QUEUE_WRAPPERS.put(key, decorator);
        Function<Queue<?>, Queue<?>> newHook = null;
        for (Function<Queue<?>, Queue<?>> function : QUEUE_WRAPPERS.values()) {
            if (newHook == null) {
                newHook = function;
            }
            else {
                newHook = newHook.andThen(function);
            }
        }
        QUEUE_WRAPPER = newHook;
    }
}

public static <T> Queue<T> wrapQueue(Queue<T> queue) {
    return (Queue) QUEUE_WRAPPER.apply(queue);
}
```

**为什么需要队列包装？** Reactor 的很多操作符内部使用 `Queue` 来缓冲数据。`wrapQueue` 允许第三方库在不修改操作符代码的情况下，拦截队列操作。典型应用场景是 Micrometer 的指标收集——包装队列以监控队列深度、出入队速率等指标。

`SinkManyUnicast.create(Queue queue)` 中就调用了 `Hooks.wrapQueue(queue)`：

```java
// SinkManyUnicast.java
static <E> SinkManyUnicast<E> create(Queue<E> queue) {
    return new SinkManyUnicast<>(Hooks.wrapQueue(queue));
}
```

---

## 十三、归纳表格：Hooks 类型与触发时机对照表

| Hook 类型 | 注册方法 | 存储方式 | 触发时机 | 默认行为 | 级别 | Context 支持 | 典型用途 |
|-----------|---------|---------|---------|---------|------|-------------|---------|
| `onEachOperator` | `Hooks.onEachOperator(key, fn)` | `LinkedHashMap<String, Function>` | 每个操作符创建后 | 无操作（不包装） | 全局 | 否 | 装配跟踪、指标收集 |
| `onLastOperator` | `Hooks.onLastOperator(key, fn)` | `LinkedHashMap<String, Function>` | 最后一个操作符创建后 | 无操作 | 全局 | 否 | 订阅入口拦截 |
| `onOperatorError` | `Hooks.onOperatorError(key, fn)` | `LinkedHashMap<String, BiFunction>` | 操作符内部出错时 | unwrap + addSuppressed | 全局+Context | `reactor.onOperatorError.local` | 错误映射、日志增强 |
| `onNextDropped` | `Hooks.onNextDropped(c)` | `Consumer<Object>` (累加) | 已终止/取消后收到数据 | DEBUG 日志 | 全局+Context | `reactor.onNextDropped.local` | 死信队列、数据丢失告警 |
| `onErrorDropped` | `Hooks.onErrorDropped(c)` | `Consumer<Throwable>` (累加) | 已终止后收到错误 | ERROR 日志 | 全局+Context | `reactor.onErrorDropped.local` | 错误监控、告警 |
| `onNextError` | `Hooks.onNextError(fn)` | `OnNextFailureStrategy` | `onNext` 中的错误 | `STOP`（取消+传播） | 全局+Context | `reactor.onNextError.localStrategy` | 错误恢复、继续处理 |
| `onDiscard` | 通过 `contextWrite` 注册 | Context 中的 `Consumer<Object>` | 取消/错误时清理队列元素 | `queue.clear()`（无钩子时） | 仅Context | `reactor.onDiscard.local` | 资源清理（连接、文件句柄） |
| `onOperatorDebug` | `Hooks.onOperatorDebug()` | `GLOBAL_TRACE` 布尔值 | 每个操作符创建时 | 关闭 | 全局 | 否 | 全局堆栈跟踪 |
| `addQueueWrapper` | `Hooks.addQueueWrapper(key, fn)` | `LinkedHashMap<String, Function>` | 创建队列时 | `Function.identity()` | 全局 | 否 | 队列监控、指标收集 |
| `enableAutomaticContextPropagation` | `Hooks.enableAutomaticContextPropagation()` | `ContextPropagationSupport` 标志 | Scheduler 调度任务时 | 关闭 | 全局 | 否 | ThreadLocal 桥接 |
