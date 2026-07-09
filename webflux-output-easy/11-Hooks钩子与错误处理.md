# Hooks 钩子与错误处理（易懂版）

> **Reactor Core 源码解析系列 · 第 11 篇 · 易懂版**

---

## 一、从一个真实场景说起：你想在所有操作符上"埋点"

你负责一个大型 WebFlux 项目，领导说："我想知道每个响应式管道里有多少操作符、执行了多久，给我加个全局监控。"

你一看代码，项目里有上百条 Flux/Mono 链，不可能一个一个去改。你需要的是一个**全局钩子**——不修改任何业务代码，就能拦截所有操作符的创建和执行。

这就是 Hooks 的用途。

**类比：Hooks 就像工厂的质检体系——**
- `onEachOperator` = 每个工位（操作符）都安排一个质检员
- `onLastOperator` = 只在出厂前（最终订阅前）做一道质检
- `onOperatorError` = 产品出了质量问题（异常）时的处理流程
- `onNextDropped` = 产品掉地上（数据丢弃）时的记录
- `onDiscard` = 生产线停了（取消订阅），传送带上半成品（队列中的元素）的清理流程

---

## 二、onEachOperator vs onLastOperator：埋在哪里？

### onEachOperator：每个工位都安排质检员

```java
// 在应用启动时注册
Hooks.onEachOperator("metrics", publisher -> {
    log.info("发现操作符: {}", publisher.getClass().getSimpleName());
    return publisher;  // 可以包装成带监控的版本
});
```

注册后，项目里的每一条 Flux/Mono 链在创建操作符时都会被拦截：

```java
flux.map(fn1)          // 触发一次: publisher = FluxMap
    .filter(fn2)       // 触发一次: publisher = FluxFilter
    .flatMap(fn3)      // 触发一次: publisher = FluxFlatMap
    .subscribe();
```

### onLastOperator：只在出厂前做质检

```java
Hooks.onLastOperator("entry-point", publisher -> {
    log.info("最终订阅的流是: {}", publisher.getClass().getSimpleName());
    return publisher;
});
```

同样的链，`onLastOperator` 只会在最后一个操作符（直接连接 `subscribe()` 的那个）上触发一次：

```java
flux.map(fn1)          // 不触发
    .filter(fn2)       // 不触发
    .flatMap(fn3)      // 触发一次（因为它是最后一个操作符）
    .subscribe();
```

**Q：什么时候用 `onEachOperator`，什么时候用 `onLastOperator`？**

| 场景 | 选择 | 原因 |
|------|------|------|
| 全局调试跟踪（记录每个操作符的创建位置） | `onEachOperator` | 需要覆盖每一个操作符 |
| 全局指标收集（统计管道数量） | `onLastOperator` | 只需要在订阅入口统计 |
| Micrometer metrics 集成 | `onEachOperator` | 需要包装每个操作符来计时 |
| 日志 Context 注入 | `onLastOperator` | 只需要在最外层注入 |

**性能差异：** `onEachOperator` 的开销和操作符数量成正比——20个操作符就要包装20次。`onLastOperator` 只包装1次。所以除非真的需要拦截每一个操作符，否则优先用 `onLastOperator`。

---

## 三、命名子钩子：多个库注册钩子不打架

**Q：如果两个库都要注册 `onEachOperator`，会不会互相覆盖？**

不会。Hooks 支持**命名子钩子**，通过一个 `String key` 来区分：

```java
// 库 A 注册自己的钩子
Hooks.onEachOperator("libraryA", publisher -> {
    // A 的监控逻辑
    return wrapWithMetricsA(publisher);
});

// 库 B 注册自己的钩子
Hooks.onEachOperator("libraryB", publisher -> {
    // B 的监控逻辑
    return wrapWithMetricsB(publisher);
});

// 两个钩子都会生效！执行顺序是先 A 再 B（按注册顺序）

// 只移除 A 的钩子，B 不受影响
Hooks.resetOnEachOperator("libraryA");
```

底层用的是 `LinkedHashMap<String, Function>`：
- 用 `LinkedHashMap` 而不是 `HashMap`，是为了**保持注册顺序**（钩子的执行顺序就是注册顺序）
- 同一个 key 再次注册会**替换**内容，但保持原来的位置

```java
// 先注册 A，再注册 B，再更新 A
Hooks.onEachOperator("A", hookA1);   // 顺序: A
Hooks.onEachOperator("B", hookB);    // 顺序: A → B
Hooks.onEachOperator("A", hookA2);   // 顺序: A(更新为hookA2) → B
// 执行顺序仍然是 hookA2 → hookB，A 的位置没变
```

### 钩子的组合：andThen 链式调用

多个子钩子通过 `Function.andThen()` 组合成一个函数链：

```java
// Hooks 内部实现
Function<Publisher, Publisher> composite = null;
for (Function<...> fn : hooks.values()) {
    if (composite != null) {
        composite = composite.andThen(fn);  // 链式组合
    } else {
        composite = fn;
    }
}
```

最终效果：`composite.apply(publisher)` 等于先执行 hookA 再执行 hookB。

---

## 四、错误处理全景图：快递丢了怎么办？

Reactor 中的错误处理机制有好几层，初学者容易搞混。用"快递系统"类比一下就清楚了。

**场景：你在网上下单买了一本书**

```
卖家发货（Source.onNext）
  → 快递公司揽收（操作符处理）
    → 中途出问题了！
       ├── 快递丢了（onNext 中出错）
       │    ├── 赔个默认的 → onErrorReturn
       │    ├── 换一家快递重新发 → onErrorResume
       │    └── 让原快递再找一次 → retry
       │
       ├── 快递到了但你不在家，又退回去了（终止后收到数据）
       │    → onNextDropped（记录"有个快递没人收"）
       │
       ├── 快递公司说"这个快递有问题"，但你已经拒签了（终止后收到错误）
       │    → onErrorDropped（记录"有个问题没人处理"）
       │
       └── 你取消了订单，但快递已经在路上（取消时队列有数据）
            → onDiscard（把在路上的快递退回仓库）
```

下面逐个深入。

---

## 五、操作符级错误处理：onErrorReturn / onErrorResume / retry

这三个是最常用的错误处理操作符，先搞懂它们。

### onErrorReturn：丢了就赔个默认的

```java
Flux.just(1, 2, 0, 4)
    .map(i -> 10 / i)                    // 除以0会抛异常
    .onErrorReturn(-1)                   // 出错就返回 -1
    .subscribe(System.out::println);
// 输出: 10, 5, -1  （到0时出错，返回-1，序列结束）
```

**注意：`onErrorReturn` 会终止序列！** 遇到错误后，它返回默认值然后发 complete 信号，后面的4不会被处理。

### onErrorResume：丢了换一家快递重新发

```java
Flux.just(1, 2, 0, 4)
    .map(i -> 10 / i)
    .onErrorResume(e -> {
        log.warn("出错了: {}", e.getMessage());
        return Flux.just(-1, -2);          // 切换到备用流
    })
    .subscribe(System.out::println);
// 输出: 10, 5, -1, -2  （出错后切换到备用流）
```

`onErrorResume` 比 `onErrorReturn` 更灵活——你可以根据错误类型返回不同的备用流。

### retry：让原快递再找一次

```java
Flux.just(1, 2, 0, 4)
    .map(i -> 10 / i)
    .retry(2)                             // 最多重试2次
    .subscribe(System.out::println);
// 输出: 10, 5, 10, 5, 10, 5, （然后报错）
// 每次重试都从头开始！
```

⚠️ **踩坑提醒：`retry` 是从头重新订阅！** 它不是"从出错的地方继续"，而是把整个 Flux 从第一个元素重新执行一遍。如果你的数据源有副作用（比如写数据库），重试可能导致重复操作。

---

## 六、OnNextFailureStrategy：onNext 出错时的恢复策略

**Q：`onErrorReturn` 和 `onErrorResume` 都会终止原序列。如果我想"跳过出错的元素，继续处理后面的"呢？**

这就需要 `OnNextFailureStrategy`。它是一个更底层的机制，控制的是"在 `onNext` 处理过程中出错时怎么办"。

### 三种内置策略

```
1. STOP（默认）：出错就停 → 取消上游，把错误传给下游
2. RESUME_DROP：跳过出错元素 → 丢弃出错的值和错误，继续处理下一个
3. RESUME：跳过 + 自定义回调 → 调用用户函数记录后继续
```

### 实战：日志分析管道

假设你在解析日志文件，某些行格式不对会解析失败。你不想因为一行坏数据就中断整个管道：

```java
Flux.fromIterable(logLines)
    .map(line -> parseLogEntry(line))  // 某些行可能解析失败
    .onErrorContinue((error, value) -> {
        // 记录出错的行，然后继续处理后续行
        log.warn("解析失败，跳过此行: {}，错误: {}", value, error.getMessage());
    })
    .subscribe(entry -> processEntry(entry));
```

`onErrorContinue` 的底层就是通过 Context 注入了一个 `RESUME` 策略：

```java
// 大致等价于
.contextWrite(ctx -> ctx.put(
    "reactor.onNextError.localStrategy",
    OnNextFailureStrategy.resume((error, value) -> {
        log.warn("跳过: {}", value);
    })
));
```

### 策略查找的三级回退

```
1. 先查 Context 中的本地策略（序列级别，最高优先级）
2. 没有就查全局 Hooks.onNextErrorHook
3. 都没有就用默认的 STOP（出错就停）
```

**为什么需要三级？**
- **Context 级**：不同的管道可以用不同策略。日志分析管道用 RESUME，支付管道用 STOP。
- **全局级**：项目统一的默认策略。
- **STOP 兜底**：保证 Reactive Streams 规范的标准行为。

⚠️ **踩坑提醒：`onErrorContinue` 不是万能的！** 它只对支持这个机制的操作符有效。如果操作符内部没有调用 `Operators.onNextError()` 来检查策略，`onErrorContinue` 就不生效。具体哪些操作符支持，需要查看源码或文档。另外，`flatMap` 内部的错误默认不会被外层的 `onErrorContinue` 捕获——需要在 `flatMap` 内部处理。

---

## 七、onNextDropped：数据掉地上了

**Q：什么情况下数据会被"丢弃"？**

三种典型场景：

**场景一：操作符已经 complete 了，上游还在发数据**

```java
// take(3) 只要3个，但上游发了5个
Flux.range(1, 5)
    .take(3)           // 收到第3个后发 cancel
    .subscribe();      // 上游可能还来得及发第4个 → onNextDropped
```

**场景二：操作符已经 cancel 了，上游还在发数据**

```java
flux.subscribe(new BaseSubscriber<Integer>() {
    protected void hookOnNext(Integer value) {
        if (value > 3) cancel();  // 手动取消
    }
});
// cancel 后上游可能还来不及停止，继续发数据 → onNextDropped
```

**场景三：Sink 已终止后仍然推送数据**

```java
sink.tryEmitComplete();
sink.emitNext("too late", FAIL_FAST);  // FAIL_TERMINATED → onNextDropped
```

### 处理 onNextDropped 的两级查找

```java
// Operators.java
public static <T> void onNextDropped(T t, Context context) {
    // 1. 先查 Context 中的本地钩子
    Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_NEXT_DROPPED, null);
    // 2. 没有就查全局钩子
    if (hook == null) {
        hook = Hooks.onNextDroppedHook;
    }
    // 3. 都没有就 DEBUG 日志
    if (hook != null) {
        hook.accept(t);
    } else {
        log.debug("onNextDropped: " + t);
    }
}
```

**注册全局钩子：**

```java
// 方式一：自定义处理（累加的，可以注册多个）
Hooks.onNextDropped(value -> {
    log.warn("数据被丢弃: {}", value);
    deadLetterQueue.add(value);  // 送入死信队列
});

// 方式二：严格模式——丢弃就抛异常（测试时用）
Hooks.onNextDroppedFail();
```

⚠️ **踩坑提醒：默认情况下数据丢弃只会打 DEBUG 日志，很容易被忽略。如果你的系统对数据丢失敏感（如金融系统），强烈建议注册全局 `onNextDropped` 钩子把丢弃记录到监控系统。**

---

## 八、onErrorDropped：错误没人处理了

**Q：什么情况下错误会被"丢弃"？**

当一个操作符已经终止（已经发了 `onError` 或 `onComplete`），又收到一个新的 `onError` 信号时，这个新错误无处可去——下游已经不再接受任何信号了。

```java
// 假设有两个异步操作，都可能失败
Flux.merge(
    asyncOp1(),  // 失败了 → onError 传给下游
    asyncOp2()   // 也失败了 → 但下游已经收到第一个错误了！
)
.subscribe();
// asyncOp2 的错误被 onErrorDropped
```

**默认行为是 ERROR 级别日志**（注意不是 DEBUG，因为错误丢弃通常意味着有未处理的异常）：

```java
log.error("Operator called default onErrorDropped", e);
```

**注册钩子：**

```java
Hooks.onErrorDropped(error -> {
    log.warn("错误被丢弃: {}", error.getMessage());
    alertService.sendAlert("Unhandled error: " + error.getMessage());
});
```

---

## 九、onDiscard：取消时的资源清理

**这是最容易被忽视、但最容易造成线上事故的钩子。**

### 问题场景：数据库连接泄漏

```java
Flux.range(1, 100)
    .flatMap(id -> {
        Connection conn = dataSource.getConnection();  // 获取连接
        return queryWithConn(conn, id)
            .doFinally(signal -> conn.close());  // 正常完成/出错时关闭
    }, 10)  // 最大并发10
    .take(5)  // 只要前5个结果
    .subscribe();
```

`take(5)` 收到5个结果后会 `cancel` 上游。此时 `flatMap` 内部队列里可能已经缓冲了一些正在进行或已完成的结果。**这些缓冲的结果不会经过 `doFinally`**（因为它们从未到达下游），所以连接就泄漏了。

### onDiscard 的工作原理

```java
// Operators.java
public static <T> void onDiscard(@Nullable T element, Context context) {
    Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
    if (element != null && hook != null) {
        hook.accept(element);
    }
}
```

**关键特征：**
1. **只通过 Context 传递**，没有全局钩子。你必须用 `contextWrite` 注册。
2. **元素为 null 或没有钩子时，什么都不做**——队列直接 `clear()`。
3. **钩子抛异常时只打 warn 日志**，不会影响主流程。

### 注册 onDiscard 钩子

```java
Flux.range(1, 100)
    .flatMap(id -> {
        Connection conn = dataSource.getConnection();
        return queryWithConn(conn, id)
            .doFinally(signal -> conn.close());
    }, 10)
    .take(5)
    .contextWrite(ctx -> ctx.put(
        Hooks.KEY_ON_DISCARD,           // "reactor.onDiscard.local"
        (Consumer<Object>) obj -> {
            if (obj instanceof ConnectionHolder) {
                ((ConnectionHolder) obj).close();  // 清理连接
            }
        }
    ))
    .subscribe();
```

### onDiscardQueueWithClear：队列的批量清理

当操作符取消时，不仅要处理当前元素，还要清理内部队列中所有缓冲的元素：

```java
// Operators.java
public static <T> void onDiscardQueueWithClear(Queue<T> queue, Context context, ...) {
    Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
    if (hook == null) {
        queue.clear();  // 没有钩子？直接清空（O(1)）
        return;
    }
    // 有钩子？逐个 poll 出来调用钩子（O(n)）
    for (;;) {
        T element = queue.poll();
        if (element == null) break;
        hook.accept(element);
    }
}
```

**设计亮点：** 没有注册钩子时用 `queue.clear()`（O(1)，直接重置指针），注册了钩子才逐个 poll（O(n)）。这叫**按需付费**——你不需要清理功能就不付性能代价。

⚠️ **踩坑提醒：以下场景必须注册 `onDiscard` 钩子：**
1. Flux/Mono 中流转的元素包含需要关闭的资源（数据库连接、文件句柄、HTTP 连接）
2. 有 `take(n)`、`takeUntil`、`timeout` 等可能取消上游的操作符
3. 有 `flatMap` 等内部有缓冲队列的操作符
4. 使用 Sinks 且队列中可能缓冲资源对象

---

## 十、onOperatorError：操作符内部出错时的全局拦截

**Q：`map` 的转换函数抛了异常，Reactor 怎么处理？**

```java
Flux.just(1, 2, 0, 4)
    .map(i -> 10 / i)  // i=0 时抛 ArithmeticException
    .subscribe();
```

处理流程：

```
1. map 操作符捕获到 ArithmeticException
2. 调用 Operators.onOperatorError(subscription, error, value, context)
   ├── a. Exceptions.throwIfFatal(error)  → 致命异常直接 throw，不走 onError
   ├── b. subscription.cancel()           → 取消上游
   ├── c. 查找 Context/全局的 onOperatorError 钩子
   ├── d. 有钩子？调钩子映射错误
   └── e. 没钩子？把 value 作为 suppressed 加到错误上
3. 把处理后的 Throwable 通过 actual.onError() 传给下游
```

### 注册全局钩子

```java
Hooks.onOperatorError("error-enricher", (error, data) -> {
    // 给所有错误附加额外信息
    log.error("操作符出错，触发数据: {}", data);
    if (error instanceof BusinessException) {
        return error;  // 业务异常不包装
    }
    return new EnrichedError("操作符内部错误", error);  // 其他异常包装
});
```

**onOperatorError vs onNextError 的区别：**

| | onOperatorError | onNextError (OnNextFailureStrategy) |
|---|---|---|
| 触发时机 | 操作符内部的任何错误 | 专门针对 `onNext` 中的错误 |
| 默认行为 | 取消上游 + 传播错误 | STOP（同左） |
| 能否恢复 | 不能（总是传播错误） | 能（RESUME 策略可以跳过继续） |
| 用途 | 全局错误映射/增强 | 跳过坏数据继续处理 |

---

## 十一、致命异常：有些错误不能 catch

**Q：是不是所有异常都会走 `onError`？**

不是。有一类"致命异常"会直接 `throw`，绕过整个 Reactive Streams 的错误处理管线：

```java
// Exceptions.java
public static void throwIfFatal(Throwable t) {
    if (isJvmFatal(t)) throw (Error) t;           // JVM 级致命异常
    if (isFatalButNotJvmFatal(t)) throw (RuntimeException) t;  // Reactor 级致命异常
}
```

哪些是致命异常？

| 异常类型 | 为什么是致命的 |
|---------|-------------|
| `OutOfMemoryError` | JVM 内存耗尽，继续执行可能数据损坏 |
| `StackOverflowError` | 栈溢出，执行状态已经不可靠 |
| `VirtualMachineError` | JVM 内部错误 |
| `ThreadDeath` | 线程被强制终止 |
| `LinkageError` | 类加载失败，后续操作依赖的类可能找不到 |
| `BubblingException` | Reactor 内部的"向上冒泡"信号 |
| `ErrorCallbackNotImplemented` | 没有 onError 回调，传播无意义 |

**类比：普通异常像是"快递破损"——可以退款、重发、换货（`onError` 处理）。致命异常像是"快递站着火了"——不是处理包裹的问题了，得赶紧撤退（直接 throw）。**

### 异常工具类

```java
// 将 checked exception 包装成 RuntimeException（通过 onError 传播）
RuntimeException wrapped = Exceptions.propagate(checkedEx);

// 包装成 BubblingException（致命，直接 throw 绕过 onError）
RuntimeException bubbled = Exceptions.bubble(error);

// 多个异常合并
Exceptions.addThrowable(ERROR_FIELD, this, newException);
// 第一个异常直接存，第二个开始合并成 CompositeException
```

---

## 十二、FluxOnAssembly：出错了怎么知道是哪行代码的锅？

**Q：Reactor 的错误堆栈为什么那么难读？出错了只看到一堆 `FluxMap$MapSubscriber.onNext`，根本找不到业务代码的位置。**

这是因为 Reactor 的操作符链在"装配时"（创建 Flux/Mono 的时候）和"运行时"（数据流动的时候）是分开的：

```java
// 装配时（在你写代码的线程上执行）
Flux<Integer> flux = Flux.range(1, 10)
    .map(i -> 10 / i)        // 这里只是"创建"了一个 FluxMap 对象
    .filter(i -> i > 0);     // 这里只是"创建"了一个 FluxFilter 对象

// 运行时（可能在完全不同的线程上执行）
flux.subscribe();             // 这时候才真正开始执行
```

当 `map` 里除以0出错时，堆栈里只有运行时的调用链（`FluxMap$MapSubscriber.onNext` → `FluxFilter$FilterSubscriber.onNext` → ...），**没有装配时的信息**（即你在哪一行写的 `.map(i -> 10/i)`）。

### 解决方案一：`Hooks.onOperatorDebug()`（全局，开发用）

```java
// 应用启动时
Hooks.onOperatorDebug();
```

开启后，每个操作符创建时都会捕获当前堆栈：

```
Error has been observed at the following site(s):
    *__Flux.map ⇢ at com.example.MyService.process(MyService.java:42)
    |_ Flux.filter ⇢ at com.example.MyService.process(MyService.java:43)
Original Stack Trace:
    at com.example.MyService.lambda$process$0(MyService.java:42)
    ...
```

现在能看到是 `MyService.java:42` 那行的 `map` 出了问题。

⚠️ **踩坑提醒：`Hooks.onOperatorDebug()` 对性能影响巨大！** 每个操作符创建时都要捕获堆栈（`new Throwable().getStackTrace()`），一次可能花 1-10ms。如果项目里有成百上千条响应式链，启动时间会显著增加。**绝对不要在生产环境开启！**

### 解决方案二：`checkpoint()`（局部，可用于生产）

```java
Flux.range(1, 10)
    .map(i -> 10 / i)
    .checkpoint("除法操作")      // 在这里打一个检查点
    .filter(i -> i > 0)
    .checkpoint("过滤操作")
    .subscribe();
```

`checkpoint()` 只在指定位置记录信息，不是全局的，性能开销小得多。出错时会显示：

```
Error has been observed at the following site(s):
    *__checkpoint ⇢ 除法操作
    |_ checkpoint ⇢ 过滤操作
```

### 装配信息的延迟计算

`checkpoint()` 的堆栈信息使用 `Supplier<String>` 延迟计算：

```java
class AssemblySnapshot {
    final Supplier<String> assemblyInformationSupplier;
    @Nullable String cached;  // 缓存，避免重复计算
    
    String toAssemblyInformation() {
        if (cached == null) {
            cached = assemblyInformationSupplier.get();  // 第一次调用才计算
        }
        return cached;
    }
}
```

**如果流正常完成，堆栈信息永远不会被计算。** 只有出错时才会调用 `get()` 获取堆栈。这就是为什么 `checkpoint()` 的性能开销远小于 `Hooks.onOperatorDebug()`。

---

## 十三、Hooks 的队列包装：给内部队列加监控

**Q：操作符内部的队列有多深？有没有即将溢出的风险？**

Reactor 的很多操作符（如 `flatMap`、`unicast Sink`）内部都有 `Queue`。`Hooks.addQueueWrapper` 允许你在不修改操作符代码的情况下包装这些队列：

```java
Hooks.addQueueWrapper("monitor", queue -> {
    return new MonitoredQueue<>(queue, metrics);  // 包装队列，加入监控
});
```

**典型用途：** Micrometer 指标收集——监控队列深度、入队速率、出队速率，在队列即将溢出时触发告警。

---

## 十四、错误处理最佳实践

### 原则一：尽量在靠近错误发生点的地方处理

```java
// 好：在 map 操作符附近处理
Flux.range(1, 10)
    .map(i -> riskyOperation(i))
    .onErrorResume(e -> {
        log.error("操作失败", e);
        return Flux.empty();  // 优雅降级
    })
    .subscribe();

// 不太好：依赖全局 Hooks 处理
Hooks.onOperatorError((error, data) -> {
    log.error("某处出错了", error);  // 不知道是哪里的错误
    return error;
});
```

### 原则二：区分可恢复错误和不可恢复错误

```java
Flux.fromIterable(orders)
    .flatMap(order ->
        processOrder(order)
            .onErrorResume(BusinessException.class, e -> {
                // 业务异常：可恢复，跳过这个订单
                log.warn("订单处理失败，跳过: {}", order.getId());
                return Mono.empty();
            })
            // 系统异常（如数据库挂了）：不恢复，让它传播
    )
    .subscribe();
```

### 原则三：别忘了 onDiscard

```java
// 凡是管道中流转资源对象的，都要注册 onDiscard
flux
    .contextWrite(ctx -> ctx.put(
        Hooks.KEY_ON_DISCARD,
        (Consumer<Object>) obj -> cleanup(obj)
    ))
    .subscribe();
```

### 原则四：生产环境的错误监控

```java
// 注册全局钩子，捕获所有"漏网之鱼"
Hooks.onErrorDropped(error -> {
    alertSystem.critical("错误被丢弃（无处理器）: " + error.getMessage());
});

Hooks.onNextDropped(value -> {
    metrics.increment("reactor.data.dropped");
});
```

---

## 十五、归纳总结表格

### 表1：错误处理操作符速查

| 操作符 | 行为 | 序列是否继续 | 适用场景 |
|--------|------|-------------|---------|
| `onErrorReturn(value)` | 返回默认值 + complete | 否（序列终止） | 简单降级 |
| `onErrorResume(fn)` | 切换到备用流 | 否（切换新序列） | 灵活降级 |
| `onErrorMap(fn)` | 转换异常类型 | 否（继续传播） | 异常包装 |
| `retry(n)` | 从头重新订阅 | 是（重新开始） | 瞬态故障重试 |
| `retryWhen(spec)` | 可配置的重试 | 是（重新开始） | 带退避的重试 |
| `onErrorContinue(fn)` | 跳过出错元素 | 是（继续后续元素） | 容错管道 |
| `doOnError(fn)` | 副作用（不改变流） | 否（继续传播） | 日志/监控 |
| `doFinally(fn)` | 终止时执行 | - | 资源清理 |

### 表2：Hooks 钩子全景对照

| 钩子 | 注册方式 | 触发时机 | 默认行为 | 支持 Context？ | 典型用途 |
|------|---------|---------|---------|-------------|---------|
| `onEachOperator` | `Hooks.onEachOperator(key, fn)` | 每个操作符创建后 | 不包装 | 否 | 全局监控、调试跟踪 |
| `onLastOperator` | `Hooks.onLastOperator(key, fn)` | 最后一个操作符创建后 | 不包装 | 否 | 订阅入口拦截 |
| `onOperatorError` | `Hooks.onOperatorError(key, fn)` | 操作符内部出错 | 加 suppressed | 是 | 错误映射/增强 |
| `onNextDropped` | `Hooks.onNextDropped(consumer)` | 终止/取消后收到数据 | DEBUG 日志 | 是 | 死信队列、数据丢失告警 |
| `onErrorDropped` | `Hooks.onErrorDropped(consumer)` | 终止后收到错误 | ERROR 日志 | 是 | 错误监控 |
| `onNextError` | `Hooks.onNextError(fn)` | onNext 中出错 | STOP（停止） | 是 | 跳过坏数据继续处理 |
| `onDiscard` | `contextWrite()` 注册 | 取消/错误时清理队列 | `queue.clear()` | 仅 Context | 资源清理（连接、句柄） |
| `onOperatorDebug` | `Hooks.onOperatorDebug()` | 每个操作符创建时 | 关闭 | 否 | 开发调试 |
| `addQueueWrapper` | `Hooks.addQueueWrapper(key, fn)` | 内部队列创建时 | 不包装 | 否 | 队列监控 |

### 表3：异常分类

| 异常类型 | 是否致命 | 处理方式 | 典型例子 |
|---------|---------|---------|---------|
| 普通 `RuntimeException` | 否 | 通过 `onError` 传播 | `NullPointerException`, `ArithmeticException` |
| 普通 `Exception` (checked) | 否 | `Exceptions.propagate()` 包装后传播 | `IOException`, `SQLException` |
| `VirtualMachineError` | 是 | 直接 throw | `OutOfMemoryError`, `StackOverflowError` |
| `LinkageError` | 是 | 直接 throw | `NoClassDefFoundError` |
| `BubblingException` | 是 | 直接 throw | `Exceptions.bubble()` 创建的 |
| `ErrorCallbackNotImplemented` | 是 | 直接 throw | 没有 onError 回调时 |
| `CompositeException` | 否 | 通过 `onError` 传播 | `Exceptions.addThrowable()` 合并的 |
| `OverflowException` | 否 | 通过 `onError` 传播 | 背压溢出 |
| `RetryExhaustedException` | 否 | 通过 `onError` 传播 | 重试次数用尽 |

### 表4：Context 中的钩子 Key

| Key 常量 | 值 | 存储类型 | 查找优先级 |
|---------|-----|---------|----------|
| `Hooks.KEY_ON_DISCARD` | `"reactor.onDiscard.local"` | `Consumer<Object>` | 仅 Context |
| `Hooks.KEY_ON_NEXT_DROPPED` | `"reactor.onNextDropped.local"` | `Consumer<Object>` | Context > 全局 |
| `Hooks.KEY_ON_ERROR_DROPPED` | `"reactor.onErrorDropped.local"` | `Consumer<Throwable>` | Context > 全局 |
| `Hooks.KEY_ON_OPERATOR_ERROR` | `"reactor.onOperatorError.local"` | `BiFunction<Throwable,Object,Throwable>` | Context > 全局 |
| `OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY` | `"reactor.onNextError.localStrategy"` | `OnNextFailureStrategy` | Context > 全局 > STOP |

### 表5：核心源码类对照

| 源码类 | 作用 | 关键方法 |
|--------|------|---------|
| `Hooks` | 全局钩子注册中心 | `onEachOperator()`, `onLastOperator()`, `onOperatorDebug()` |
| `Operators` | 操作符工具类 | `onNextDropped()`, `onErrorDropped()`, `onDiscard()`, `onOperatorError()`, `onNextError()` |
| `OnNextFailureStrategy` | onNext 错误恢复策略接口 | `STOP`, `RESUME_DROP`, `resume()`, `test()`, `process()` |
| `Exceptions` | 异常工具类 | `throwIfFatal()`, `propagate()`, `bubble()`, `addThrowable()` |
| `FluxOnAssembly` | 装配跟踪包装 | `wrapSubscriber()`, `AssemblySnapshot` |
| `AssemblySnapshot` | 装配快照（堆栈信息） | `toAssemblyInformation()` 延迟计算 |
