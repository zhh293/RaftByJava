# Arthas Watch/Trace/Stack 方法监控核心链路源码全流程解析

> 本文基于 Arthas 源码项目 `arthas` 进行分析，源码路径位于 `tmp-source-reading/arthas`。
> 分析范围涵盖 `watch`、`trace`、`stack` 三个核心方法监控命令的完整调用链路，
> 从命令输入、类匹配、字节码增强、Advice 回调、表达式求值到结果输出，
> 逐行追踪每一步方法调用，力求做到"不跳步、不省略"。

---

## 全局调用链总览

下面是一张从用户输入命令到结果输出的完整 ASCII 调用链路总览图，涵盖 watch、trace、stack 三个命令的完整流程：

```
用户输入命令 (watch / trace / stack)
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  AnnotatedCommand.process(CommandProcess process)               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  WatchCommand.process()    / TraceCommand(继承)          │    │
│  │  StackCommand.process()    / MonitorCommand(继承)        │    │
│  │                                                         │    │
│  │  1. validateSizeLimit() (WatchCommand)                  │    │
│  │  2. super.process() → EnhancerCommand.process()         │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  EnhancerCommand.process()                                      │
│  1. process.interruptHandler(new CommandInterruptHandler)       │
│  2. process.stdinHandler(new QExitHandler)                      │
│  3. enhance(process)                                            │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  EnhancerCommand.enhance()                                      │
│  1. session.tryLock() —— 防止并发增强                           │
│  2. getAdviceListenerWithId(process) —— 获取监听器              │
│     └─ getAdviceListener(process) —— 子类实现                   │
│        ├─ WatchCommand  → new WatchAdviceListener               │
│        ├─ TraceCommand  → new TraceAdviceListener               │
│        └─ StackCommand  → new StackAdviceListener               │
│  3. new Enhancer(listener, isTracing, skipJDKTrace,             │
│        classNameMatcher, methodNameMatcher, ...)                │
│  4. process.register(listener, enhancer)                        │
│  5. enhancer.enhance(inst, maxNumOfMatchedClass)               │
│     └─ ClassFileTransformer.transform() —— ASM字节码织入        │
│        └─ SpyAPI.atBefore / atAfterReturning / atAfterThrowing  │
│  6. scheduleTimeoutTask(process) —— 超时自动退出                 │
│  7. session.unLock() —— 释放锁                                  │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼  (目标方法被调用时，由增强后的字节码触发回调)
┌─────────────────────────────────────────────────────────────────┐
│  AdviceWeaver (SpyAPI 回调入口)                                  │
│  ├─ before()       → AdviceListenerAdapter.before()             │
│  ├─ afterReturning() → AdviceListenerAdapter.afterReturning()   │
│  ├─ afterThrowing()  → AdviceListenerAdapter.afterThrowing()    │
│  └─ (trace only) invokeBeforeTracing / invokeAfterTracing       │
└─────────────────────────────────────────────────────────────────┘
        │
        ├──────────────────┬──────────────────┬──────────────────┐
        ▼                  ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Watch        │  │ Trace        │  │ Stack        │  │ Monitor      │
│ AdviceListener│ │ AdviceListener│ │ AdviceListener│ │ AdviceListener│
│              │  │ (Abstract)   │  │              │  │              │
│ 1.start计时  │  │ 1.begin树节点│  │ 1.start计时  │  │ 1.按时间窗口 │
│ 2.watching() │  │ 2.deep++     │  │ 2.finishing()│  │   统计       │
│   条件判断   │  │ 3.tree.end() │  │   条件判断   │  │ 2.定时输出   │
│   OGNL求值   │  │ 4.deep--     │  │   获取栈帧   │  │              │
│   WatchModel │  │ 5.finishing()│  │   StackModel │  │              │
│   appendResult│ │   TraceModel │  │   appendResult│  │              │
│   abortProcess│ │   appendResult│ │   abortProcess│  │              │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
        │                  │                  │
        ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  CommandProcess.appendResult(model)                             │
│  └─ ResultView 渲染 → 终端输出                                   │
└─────────────────────────────────────────────────────────────────┘
```

上图展示了三个命令从输入到输出的完整链路。可以看到，它们共享 `EnhancerCommand` 基类提供的增强流程，
差异主要体现在各自的 `AdviceListener` 实现上。

### 三个命令的继承体系

```
AnnotatedCommand
    └── EnhancerCommand (抽象基类)
            ├── WatchCommand      → WatchAdviceListener
            ├── TraceCommand      → TraceAdviceListener (extends AbstractTraceAdviceListener)
            ├── StackCommand      → StackAdviceListener
            ├── MonitorCommand    → MonitorAdviceListener
            └── TimeTunnelCommand → TimeTunnelAdviceListener
```

所有监控命令都继承自 `EnhancerCommand`，它定义了通用的增强流程模板。
每个子类只需要实现三个抽象方法：`getClassNameMatcher()`、`getMethodNameMatcher()`、`getAdviceListener()`。

### 类比理解

可以将 Arthas 的监控机制类比为"在高速公路上安装摄像头"：

- **EnhancerCommand** 是"摄像头安装队"的总调度，负责在指定路段（类/方法）安装摄像头
- **Enhancer** 是具体的"安装工"，用 ASM 字节码技术在方法入口/出口织入回调代码
- **AdviceListener** 是"摄像头控制器"，决定在什么时机拍照（before/afterReturning/afterThrowing）
- **WatchAdviceListener** 是"高清摄像头"，拍下方法的参数、返回值、异常等详细信息
- **TraceAdviceListener** 是"路径追踪器"，记录方法调用链路的每一跳
- **StackAdviceListener** 是"全景摄像头"，在方法入口拍下完整的调用栈

---

## 第1阶段：EnhancerCommand —— 增强命令基类

### 1.1 EnhancerCommand 类 —— 所有监控命令的模板基类

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/EnhancerCommand.java`

`EnhancerCommand` 是所有需要字节码增强的命令的抽象基类。它定义了通用的增强流程模板，包括：
- 会话锁管理（防止并发增强）
- 监听器获取
- Enhancer 创建与执行
- 超时任务调度

```java
public abstract class EnhancerCommand extends AnnotatedCommand {

    private static final Logger logger = LoggerFactory.getLogger(EnhancerCommand.class);
    protected static final List<String> EMPTY = Collections.emptyList();

    protected Matcher classNameMatcher;
    protected Matcher classNameExcludeMatcher;
    protected Matcher methodNameMatcher;

    protected long listenerId;
    protected boolean verbose;
    protected int maxNumOfMatchedClass;
    protected Long timeout;
    protected boolean lazy = false;
    protected String hashCode;
```

这段代码定义了 `EnhancerCommand` 的核心字段：

- `classNameMatcher` / `classNameExcludeMatcher` / `methodNameMatcher`：三个匹配器，分别用于匹配类名、排除类名和方法名。采用懒初始化模式，子类在 `getClassNameMatcher()` 等方法中首次访问时创建。
- `listenerId`：监听器 ID，用于支持多个命令复用同一个监听器的场景。
- `verbose`：是否输出详细日志。
- `maxNumOfMatchedClass`：最大匹配类数量，默认 50，防止增强过多类导致性能问题。
- `timeout`：超时时间（秒），超时后自动结束命令。
- `lazy`：懒加载模式，当目标类尚未加载时，等待类加载后再增强。
- `hashCode`：指定 ClassLoader 的 hash 值，只增强该 ClassLoader 加载的类。

#### 1.1.1 为什么需要 maxNumOfMatchedClass 限制？

想象一下，如果用户输入 `watch * *` 这样的通配符，可能会匹配到 JVM 中成千上万个类。
对这么多类进行字节码增强不仅耗时极长，还会严重影响应用性能。
因此 `maxNumOfMatchedClass` 默认限制为 50，是一个安全阀。

### 1.2 CLI 参数定义 —— EnhancerCommand 的通用选项

```java
@Option(longName = "exclude-class-pattern")
@Description("exclude class name pattern, use either '.' or '/' as separator")
public void setExcludeClassPattern(String excludeClassPattern) {
    this.excludeClassPattern = excludeClassPattern;
}

@Option(longName = "classloader")
@Description("The hash code of the special class's classLoader")
public void setHashCode(String hashCode) {
    this.hashCode = hashCode;
}

@Option(longName = "listenerId")
@Description("The special listenerId")
public void setListenerId(long listenerId) {
    this.listenerId = listenerId;
}

@Option(shortName = "v", longName = "verbose", flag = true)
@Description("Enables print verbose information, default value false.")
public void setVerbosee(boolean verbose) {
    this.verbose = verbose;
}

@Option(shortName = "m", longName = "maxMatch")
@DefaultValue("50")
@Description("The maximum of matched class.")
public void setMaxNumOfMatchedClass(int maxNumOfMatchedClass) {
    this.maxNumOfMatchedClass = maxNumOfMatchedClass;
}

@Option(longName = "timeout")
@Description("Timeout value in seconds for the command to exit automatically.")
public void setTimeout(Long timeout) {
    this.timeout = timeout;
}

@Option(shortName = "L", longName = "lazy", flag = true)
@Description("Enable lazy mode to enhance classes when they are loaded.")
public void setLazy(boolean lazy) {
    this.lazy = lazy;
}
```

这些是所有继承 `EnhancerCommand` 的命令共享的 CLI 选项：

| 选项 | 短名 | 说明 | 默认值 |
|------|------|------|--------|
| `--exclude-class-pattern` | 无 | 排除类名模式 | 无 |
| `--classloader` | `-c` | 指定 ClassLoader hash | 无 |
| `--listenerId` | 无 | 复用已有监听器 ID | 0（新建） |
| `--verbose` | `-v` | 输出详细信息 | false |
| `--maxMatch` | `-m` | 最大匹配类数 | 50 |
| `--timeout` | 无 | 超时秒数 | 无 |
| `--lazy` | `-L` | 懒加载增强 | false |

### 1.3 抽象方法定义 —— 子类必须实现的契约

```java
protected abstract Matcher getClassNameMatcher();
protected abstract Matcher getClassNameExcludeMatcher();
protected abstract Matcher getMethodNameMatcher();
protected abstract AdviceListener getAdviceListener(CommandProcess process);
```

`EnhancerCommand` 定义了四个抽象方法，构成子类必须实现的契约：

- `getClassNameMatcher()`：返回类名匹配器，决定哪些类会被增强
- `getClassNameExcludeMatcher()`：返回排除类名匹配器，排除不需要增强的类
- `getMethodNameMatcher()`：返回方法名匹配器，决定哪些方法会被增强
- `getAdviceListener(process)`：返回通知监听器，决定增强后如何处理回调

这种设计是典型的**模板方法模式**（Template Method Pattern）：父类定义流程骨架，子类填充具体实现。

### 1.4 getAdviceListenerWithId() —— 监听器获取（支持复用）

```java
AdviceListener getAdviceListenerWithId(CommandProcess process) {
    if (listenerId != 0) {
        AdviceListener listener = AdviceWeaver.listener(listenerId);
        if (listener != null) {
            return listener;
        }
    }
    return getAdviceListener(process);
}
```

这个方法实现了监听器的复用机制：

1. 如果用户指定了 `--listenerId`（即 `listenerId != 0`），则尝试从 `AdviceWeaver` 中根据 ID 查找已有的监听器
2. 如果找到了，直接复用，避免重复创建
3. 如果没找到，调用子类实现的 `getAdviceListener(process)` 创建新的监听器

**它为什么存在？** 在某些场景下，用户可能希望多个命令共享同一个监听器。例如，先启动一个 `watch` 命令，
然后用 `--listenerId` 复用该监听器来执行其他操作。这减少了重复增强的开销。

### 1.5 process() 方法 —— 命令执行入口

```java
@Override
public void process(final CommandProcess process) {
    // ctrl-C support
    process.interruptHandler(new CommandInterruptHandler(process));
    // q exit support
    process.stdinHandler(new QExitHandler(process));

    // start to enhance
    enhance(process);
}
```

这是 `EnhancerCommand` 对 `AnnotatedCommand.process()` 的实现，是命令执行的入口点。它只做了三件事：

1. **注册中断处理器**：`CommandInterruptHandler` 处理 Ctrl+C 中断信号，让用户可以通过 Ctrl+C 终止监控命令
2. **注册标准输入处理器**：`QExitHandler` 处理键盘输入 'q' 字符，让用户可以通过按 'q' 键退出监控命令
3. **调用 enhance() 方法**：开始执行字节码增强流程

**这一步做了什么？** 这是命令生命周期的起点。在正式开始增强之前，先做好"退路"的准备——确保用户随时可以安全退出。
这是一种防御性编程的体现。

### 1.6 enhance() 方法 —— 核心增强流程（重点）

这是整个 Arthas 监控机制最核心的方法，所有监控命令的字节码增强都从这里开始。

```java
protected void enhance(CommandProcess process) {
    Session session = process.session();
    if (!session.tryLock()) {
        String msg = "someone else is enhancing classes, pls. wait.";
        process.appendResult(EnhancerModelFactory.create(null, false, msg));
        process.end(-1, msg);
        return;
    }
    EnhancerAffect effect = null;
    int lock = session.getLock();
```

**第一阶段：获取会话锁**

- `session.tryLock()` 尝试获取会话锁。这是一个非阻塞的锁操作，如果锁已被占用则立即返回 false。
- 如果获取锁失败，说明有其他命令正在执行增强操作，直接返回错误信息。

**为什么需要加锁？** 字节码增强是通过 `Instrumentation.retransformClasses()` 实现的，
这是一个重量级操作。如果多个命令同时 retransform 同一个类，可能会导致类文件冲突或 JVM 内部状态不一致。
因此 Arthas 通过会话级别的锁来保证同一时间只有一个增强操作在执行。

`int lock = session.getLock()` 记录当前锁的值（类似于乐观锁的版本号），用于后续的补偿性检查。

```java
    try {
        Instrumentation inst = session.getInstrumentation();
        AdviceListener listener = getAdviceListenerWithId(process);
        if (listener == null) {
            logger.error("advice listener is null");
            String msg = "advice listener is null, check arthas log";
            process.appendResult(EnhancerModelFactory.create(effect, false, msg));
            process.end(-1, msg);
            return;
        }
```

**第二阶段：获取监听器和 Instrumentation**

- `session.getInstrumentation()` 获取 JVM 的 `Instrumentation` 实例，这是 Java Agent API 的核心接口，提供了字节码操作能力。
- `getAdviceListenerWithId(process)` 获取通知监听器，如果返回 null 则直接报错退出。

```java
        boolean skipJDKTrace = false;
        if(listener instanceof AbstractTraceAdviceListener) {
            skipJDKTrace = ((AbstractTraceAdviceListener) listener).getCommand().isSkipJDKTrace();
        }

        Enhancer enhancer = new Enhancer(listener, listener instanceof InvokeTraceable, skipJDKTrace,
                getClassNameMatcher(), getClassNameExcludeMatcher(), getMethodNameMatcher(), this.lazy, this.hashCode);
        enhancer.setLineEnhanceOptions(getLineEnhanceOptions());
```

**第三阶段：创建 Enhancer 对象**

这里创建 `Enhancer` 对象，传入以下参数：

| 参数 | 说明 |
|------|------|
| `listener` | 通知监听器，回调时使用 |
| `listener instanceof InvokeTraceable` | 是否需要方法调用级别的跟踪（trace 命令需要） |
| `skipJDKTrace` | 是否跳过 JDK 方法的跟踪 |
| `getClassNameMatcher()` | 类名匹配器 |
| `getClassNameExcludeMatcher()` | 排除类名匹配器 |
| `getMethodNameMatcher()` | 方法名匹配器 |
| `this.lazy` | 是否启用懒加载模式 |
| `this.hashCode` | 指定 ClassLoader hash |

`listener instanceof InvokeTraceable` 是一个关键判断：
- `TraceAdviceListener` 实现了 `InvokeTraceable` 接口，返回 true
- `WatchAdviceListener` 和 `StackAdviceListener` 没有实现该接口，返回 false

这决定了 Enhancer 是否会在方法体内部的每次方法调用前后都插入跟踪代码。trace 命令需要这样做来构建调用树，
而 watch 和 stack 只需要在方法入口/出口插入代码即可。

```java
        // 注册通知监听器
        process.register(listener, enhancer);
        effect = enhancer.enhance(inst, this.maxNumOfMatchedClass);
```

**第四阶段：注册监听器并执行增强**

- `process.register(listener, enhancer)`：将监听器和 Enhancer 注册到 CommandProcess 中。这一步建立了监听器与增强器之间的关联，确保在命令结束时可以正确清理。
- `enhancer.enhance(inst, this.maxNumOfMatchedClass)`：执行实际的字节码增强操作。Enhancer 作为 `ClassFileTransformer` 的实现，通过 `Instrumentation` API 对匹配的类进行 retransform。

```java
        if (effect.getThrowable() != null) {
            String msg = "error happens when enhancing class: "+effect.getThrowable().getMessage();
            process.appendResult(EnhancerModelFactory.create(effect, false, msg));
            process.end(1, msg + ", check arthas log: " + LogUtil.loggingFile());
            return;
        }
```

**第五阶段：错误处理**

如果增强过程中抛出异常，直接返回错误信息，命令结束。

```java
        if (effect.cCnt() == 0 || effect.mCnt() == 0) {
            // no class effected
            if (!StringUtils.isEmpty(effect.getOverLimitMsg())) {
                process.appendResult(EnhancerModelFactory.create(effect, false));
                process.end(-1);
                return;
            }
            
            // 懒加载模式：即使没有匹配的类也不立即结束，等待类加载
            if (this.lazy) {
                String lazyMsg = "Lazy mode is enabled, waiting for class to be loaded. "
                    + "Press Q or Ctrl+C to abort.\n"
                    + "When the target class is loaded, it will be automatically enhanced.";
                process.write(lazyMsg + "\n");
            } else {
                process.appendResult(EnhancerModelFactory.create(effect, false, "No class or method is affected"));
                // ... 输出排查建议 ...
                process.end(-1, msg);
                return;
            }
        }
```

**第六阶段：匹配结果检查**

- `effect.cCnt()` 返回被增强的类数量
- `effect.mCnt()` 返回被增强的方法数量
- 如果两者任一为 0，说明没有匹配到任何类或方法

这里有一个重要的分支：懒加载模式。如果启用了 `--lazy`，即使没有匹配到类也不会立即结束，
而是等待目标类被加载后再自动增强。这在目标类尚未被 JVM 加载的场景下非常有用。

```java
        // 这里做个补偿,如果在enhance期间,unLock被调用了,则补偿性放弃
        if (session.getLock() == lock) {
            if (process.isForeground()) {
                process.echoTips(Constants.Q_OR_CTRL_C_ABORT_MSG + "\n");
            }
        }

        process.appendResult(EnhancerModelFactory.create(effect, true));

        // 设置超时任务
        scheduleTimeoutTask(process);

        //异步执行，在AdviceListener中结束
    } catch (Throwable e) {
        String msg = "error happens when enhancing class: "+e.getMessage();
        logger.error(msg, e);
        process.appendResult(EnhancerModelFactory.create(effect, false, msg));
        process.end(-1, msg);
    } finally {
        if (session.getLock() == lock) {
            // enhance结束后解锁
            process.session().unLock();
        }
    }
}
```

**第七阶段：补偿检查与清理**

- `session.getLock() == lock`：补偿性检查。在 enhance 执行期间，如果会话锁被其他操作释放了（比如用户按了 Ctrl+C），则不再执行后续操作。
- `scheduleTimeoutTask(process)`：设置超时任务，超时后自动结束命令。
- `finally` 块中释放会话锁，同样通过版本号检查确保只释放自己加的锁。

**关键设计：异步执行模式**

注释 `异步执行，在AdviceListener中结束` 非常重要。`enhance()` 方法执行完毕后，命令并没有结束。
字节码增强已经完成，现在等待目标方法被调用。当目标方法被调用时，增强的字节码会触发 `AdviceListener` 的回调方法，
在回调中决定何时输出结果、何时终止命令。这就是 Arthas 监控命令的异步特性。

### 1.7 scheduleTimeoutTask() —— 超时自动退出

```java
private void scheduleTimeoutTask(final CommandProcess process) {
    if (timeout == null || timeout <= 0) {
        return;
    }

    final ScheduledFuture<?> timeoutFuture = ArthasBootstrap.getInstance()
            .getScheduledExecutorService()
            .schedule(new Runnable() {
                @Override
                public void run() {
                    if (process.isRunning()) {
                        process.write("Command execution timeout after " + timeout + " seconds.\n");
                        process.end();
                    }
                }
            }, timeout, TimeUnit.SECONDS);

    // Cancel the timeout task if the process ends normally
    process.endHandler(new com.taobao.arthas.core.shell.handlers.Handler<Void>() {
        @Override
        public void handle(Void event) {
            timeoutFuture.cancel(false);
        }
    });
}
```

这个方法实现了一个优雅的超时机制：

1. 如果没有设置 `--timeout`，直接返回，不启动超时任务
2. 使用 Arthas 的调度线程池在指定秒数后执行超时回调
3. 超时回调检查进程是否仍在运行，如果是则写入超时信息并结束进程
4. 注册 `endHandler`，在进程正常结束时取消超时任务，避免资源泄漏

**它为什么存在？** 防止用户启动监控命令后忘记退出，导致目标应用长期承受增强带来的性能开销。
这是一种自我保护机制。

### 1.8 complete() 方法 —— Tab 自动补全

```java
@Override
public void complete(Completion completion) {
    int argumentIndex = CompletionUtils.detectArgumentIndex(completion);

    if (argumentIndex == 1) { // class name
        if (!CompletionUtils.completeClassName(completion)) {
            super.complete(completion);
        }
        return;
    } else if (argumentIndex == 2) { // method name
        if (!CompletionUtils.completeMethodName(completion)) {
            super.complete(completion);
        }
        return;
    } else if (argumentIndex == 3) { // watch express
        completeArgument3(completion);
        return;
    }

    super.complete(completion);
}
```

这个方法实现了命令行 Tab 自动补全功能：

- 第 1 个参数：补全类名，从已加载的类列表中匹配
- 第 2 个参数：补全方法名，从指定类的方法列表中匹配
- 第 3 个参数：由子类决定如何补全（如 WatchCommand 补全表达式示例）

`completeArgument3()` 是一个 protected 方法，子类可以覆盖。例如 `WatchCommand` 覆盖了它来提供表达式补全。

### 1.9 EnhancerCommand 设计总结

| 设计决策 | 原因 |
|---------|------|
| 模板方法模式 | 统一增强流程，子类只需实现匹配器和监听器 |
| 会话级锁 | 防止并发 retransform 导致状态不一致 |
| 补偿性锁检查 | 处理增强期间被中断的边缘情况 |
| 懒加载模式 | 处理目标类尚未加载的场景 |
| 超时任务 | 防止长期运行导致性能损耗 |
| 异步执行 | 增强完成后等待回调，不阻塞命令线程 |

---

## 第2阶段：WatchCommand —— 方法执行数据观测

### 2.1 WatchCommand 类 —— watch 命令定义

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/WatchCommand.java`

`WatchCommand` 是 Arthas 中使用频率最高的命令之一，用于观察方法的输入输出参数、返回值和异常。

```java
@Name("watch")
@Summary("Display the input/output parameter, return object, and thrown exception of specified method invocation")
@Description(Constants.EXPRESS_DESCRIPTION + "\nExamples:\n" +
        "  watch org.apache.commons.lang.StringUtils isBlank\n" +
        "  watch org.apache.commons.lang.StringUtils isBlank '{params, target, returnObj, throwExp}' -x 2\n" +
        "  watch *StringUtils isBlank params[0] params[0].length==1\n" +
        "  watch *StringUtils isBlank params '#cost>100'\n" +
        "  watch -f *StringUtils isBlank params\n" +
        Constants.WIKI + Constants.WIKI_HOME + "watch")
public class WatchCommand extends EnhancerCommand {
```

`@Name("watch")` 注册命令名称，`@Summary` 提供简要描述，`@Description` 提供详细说明和使用示例。

### 2.2 WatchCommand 字段定义 —— 监控参数

```java
private String classPattern;
private String methodPattern;
private String express;
private String conditionExpress;
private boolean isBefore = false;
private boolean isFinish = false;
private boolean isException = false;
private boolean isSuccess = false;
private Integer expand = 1;
private Integer sizeLimit;
private boolean isRegEx = false;
private int numberOfLimit = 100;
```

这些字段对应 watch 命令的各种参数：

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `classPattern` | String | 类名匹配模式 | 必填 |
| `methodPattern` | String | 方法名匹配模式 | 必填 |
| `express` | String | 观察表达式（OGNL） | `{params, target, returnObj}` |
| `conditionExpress` | String | 条件表达式 | 无 |
| `isBefore` | boolean | 方法调用前观察 | false |
| `isFinish` | boolean | 方法调用后观察 | false |
| `isException` | boolean | 抛出异常时观察 | false |
| `isSuccess` | boolean | 成功返回时观察 | false |
| `expand` | Integer | 对象展开深度 | 1 |
| `sizeLimit` | Integer | 结果大小限制 | 全局配置 |
| `isRegEx` | boolean | 是否使用正则匹配 | false |
| `numberOfLimit` | int | 执行次数上限 | 100 |

### 2.3 CLI 参数绑定 —— @Argument 和 @Option

```java
@Argument(index = 0, argName = "class-pattern")
@Description("The full qualified class name you want to watch")
public void setClassPattern(String classPattern) {
    this.classPattern = StringUtils.normalizeClassName(classPattern);
}

@Argument(index = 1, argName = "method-pattern")
@Description("The method name you want to watch")
public void setMethodPattern(String methodPattern) {
    this.methodPattern = methodPattern;
}

@Argument(index = 2, argName = "express", required = false)
@DefaultValue("{params, target, returnObj}")
@Description("The content you want to watch, written by ognl.")
public void setExpress(String express) {
    this.express = express;
}

@Argument(index = 3, argName = "condition-express", required = false)
@Description(Constants.CONDITION_EXPRESS)
public void setConditionExpress(String conditionExpress) {
    this.conditionExpress = conditionExpress;
}
```

注意 `setClassPattern` 中调用了 `StringUtils.normalizeClassName(classPattern)`，这个方法将类名中的 `/` 替换为 `.`，
因为 Java 内部类名使用 `/` 作为分隔符（如 `java/lang/String`），而用户通常使用 `.` 分隔符（如 `java.lang.String`）。

`express` 参数的默认值是 `{params, target, returnObj}`，这意味着如果不指定观察表达式，watch 默认会展示方法参数、目标对象和返回值。

### 2.4 观察点选项 —— -b/-f/-e/-s

```java
@Option(shortName = "b", longName = "before", flag = true)
@Description("Watch before invocation")
public void setBefore(boolean before) {
    isBefore = before;
}

@Option(shortName = "f", longName = "finish", flag = true)
@Description("Watch after invocation, enable by default")
public void setFinish(boolean finish) {
    isFinish = finish;
}

@Option(shortName = "e", longName = "exception", flag = true)
@Description("Watch after throw exception")
public void setException(boolean exception) {
    isException = exception;
}

@Option(shortName = "s", longName = "success", flag = true)
@Description("Watch after successful invocation")
public void setSuccess(boolean success) {
    isSuccess = success;
}
```

watch 命令支持四个观察点：

| 选项 | 短名 | 观察点 | 说明 |
|------|------|--------|------|
| `--before` | `-b` | 方法调用前 | 可以看到方法参数，但还没有返回值 |
| `--finish` | `-f` | 方法调用后（无论成功/异常） | 默认启用 |
| `--exception` | `-e` | 抛出异常时 | 只在方法抛出异常时触发 |
| `--success` | `-s` | 成功返回时 | 只在方法正常返回时触发 |

如果不指定任何观察点选项，`WatchAdviceListener.isFinish()` 方法会返回 true（默认在方法结束后观察）。

### 2.5 展开深度与大小限制 —— -x/-M/-n

```java
@Option(shortName = "x", longName = "expand")
@Description("Expand level of object (1 by default), the max value is " + ObjectView.MAX_DEEP)
public void setExpand(Integer expand) {
    this.expand = expand;
}

@Option(shortName = "M", longName = "sizeLimit")
@Description("Upper size limit in bytes for the result")
public void setSizeLimit(Integer sizeLimit) {
    this.sizeLimit = sizeLimit;
}

@Option(shortName = "n", longName = "limits")
@Description("Threshold of execution times")
public void setNumberOfLimit(int numberOfLimit) {
    this.numberOfLimit = numberOfLimit;
}
```

- `-x` 控制对象展开深度。例如，如果一个方法的返回值是一个嵌套的 Map，`-x 1` 只展示第一层，`-x 2` 会展开到第二层。
- `-M` 控制结果的大小上限（字节数），防止输出过大的对象导致终端卡死。
- `-n` 控制执行次数上限，达到后自动退出。默认 100 次。

### 2.6 匹配器构建 —— getClassNameMatcher / getMethodNameMatcher

```java
@Override
protected Matcher getClassNameMatcher() {
    if (classNameMatcher == null) {
        classNameMatcher = SearchUtils.classNameMatcher(getClassPattern(), isRegEx());
    }
    return classNameMatcher;
}

@Override
protected Matcher getClassNameExcludeMatcher() {
    if (classNameExcludeMatcher == null && getExcludeClassPattern() != null) {
        classNameExcludeMatcher = SearchUtils.classNameMatcher(getExcludeClassPattern(), isRegEx());
    }
    return classNameExcludeMatcher;
}

@Override
protected Matcher getMethodNameMatcher() {
    if (methodNameMatcher == null) {
        methodNameMatcher = SearchUtils.classNameMatcher(getMethodPattern(), isRegEx());
    }
    return methodNameMatcher;
}
```

三个匹配器都采用懒初始化模式（Lazy Initialization）：
- 首次调用时创建匹配器对象
- 后续调用直接返回缓存的实例

`SearchUtils.classNameMatcher()` 根据 `isRegEx` 的值决定创建 `WildcardMatcher`（通配符匹配）还是 `RegexMatcher`（正则匹配）。

### 2.7 process() 方法 —— watch 命令的入口

```java
@Override
public void process(CommandProcess process) {
    String validateError = validateSizeLimit(sizeLimit);
    if (validateError != null) {
        process.end(-1, validateError);
        return;
    }
    super.process(process);
}

static String validateSizeLimit(Integer sizeLimit) {
    if (sizeLimit != null && sizeLimit.intValue() <= 0) {
        return "sizeLimit must be greater than 0.";
    }
    return null;
}
```

`WatchCommand` 覆盖了 `process()` 方法，在调用 `super.process()` 之前先验证 `sizeLimit` 参数。
如果 `sizeLimit` 小于等于 0，直接返回错误。验证通过后，调用父类 `EnhancerCommand.process()` 进入标准增强流程。

### 2.8 getAdviceListener() —— 创建 WatchAdviceListener

```java
@Override
protected AdviceListener getAdviceListener(CommandProcess process) {
    return new WatchAdviceListener(this, process, GlobalOptions.verbose || this.verbose);
}
```

这是 `WatchCommand` 对 `EnhancerCommand` 抽象方法的实现，创建并返回一个 `WatchAdviceListener` 实例。
传入三个参数：
- `this`：WatchCommand 实例，监听器通过它获取命令参数
- `process`：CommandProcess 实例，用于输出结果和控制命令生命周期
- `GlobalOptions.verbose || this.verbose`：是否输出详细日志，全局 verbose 或命令级 verbose 任一为 true 即可

### 2.9 completeArgument3() —— 表达式补全

```java
@Override
protected void completeArgument3(Completion completion) {
    CompletionUtils.complete(completion, Arrays.asList(EXPRESS_EXAMPLES));
}
```

`EXPRESS_EXAMPLES` 是 `EnhancerCommand` 中定义的数组，包含常用的表达式示例：
`{ "params", "returnObj", "throwExp", "target", "clazz", "method", "{params,returnObj}", "params[0]" }`。

当用户在输入 watch 命令的第三个参数（观察表达式）时按 Tab，会显示这些候选值。

---

## 第3阶段：WatchAdviceListener —— Watch 回调实现

### 3.1 WatchAdviceListener 类 —— watch 命令的回调核心

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/WatchAdviceListener.java`

`WatchAdviceListener` 继承自 `AdviceListenerAdapter`，是 watch 命令的回调实现。
当被增强的方法被调用时，ASM 织入的代码会通过 SpyAPI 触发 `AdviceWeaver`，最终调用到这里。

```java
class WatchAdviceListener extends AdviceListenerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(WatchAdviceListener.class);
    private final ThreadLocalWatch threadLocalWatch = new ThreadLocalWatch();
    private WatchCommand command;
    private CommandProcess process;

    public WatchAdviceListener(WatchCommand command, CommandProcess process, boolean verbose) {
        this.command = command;
        this.process = process;
        super.setVerbose(verbose);
    }
```

### 3.2 ThreadLocalWatch —— 线程本地计时器

`WatchAdviceListener` 持有一个 `ThreadLocalWatch` 实例，用于记录方法调用的耗时。
`ThreadLocalWatch` 使用 `ThreadLocal<long[]>` 来存储时间戳，确保多线程环境下各线程的计时互不干扰。

### 3.3 isFinish() —— 判断是否在方法结束后观察

```java
private boolean isFinish() {
    return command.isFinish() || !command.isBefore() && !command.isException() && !command.isSuccess();
}
```

这个方法判断是否需要在方法结束时（无论成功还是异常）进行观察。逻辑如下：
- 如果用户显式指定了 `-f`，返回 true
- 如果用户没有指定任何观察点选项（`-b`、`-e`、`-s` 都没有），也返回 true（默认行为）

这行代码的短路逻辑很巧妙：`!isBefore && !isException && !isSuccess` 表示用户没有指定任何观察点，
此时默认在方法结束后观察。

### 3.4 before() —— 方法调用前回调

```java
@Override
public void before(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args)
        throws Throwable {
    // 开始计算本次方法调用耗时
    threadLocalWatch.start();
    if (command.isBefore()) {
        watching(Advice.newForBefore(loader, clazz, method, target, args));
    }
}
```

当被增强的方法即将被调用时，`AdviceWeaver` 会回调此方法：

1. `threadLocalWatch.start()`：记录开始时间戳。使用 `System.nanoTime()` 获取高精度时间。
2. 如果用户指定了 `-b` 选项，创建一个 `Advice` 对象（标记为 before 访问点），调用 `watching()` 方法进行观察。

**这一步做了什么？** 在方法执行前启动计时器，并根据用户配置决定是否在方法入口处进行观察。

### 3.5 afterReturning() —— 方法正常返回回调

```java
@Override
public void afterReturning(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                           Object returnObject) throws Throwable {
    Advice advice = Advice.newForAfterReturning(loader, clazz, method, target, args, returnObject);
    if (command.isSuccess()) {
        watching(advice);
    }

    finishing(advice);
}
```

当被增强的方法正常返回时，`AdviceWeaver` 会回调此方法：

1. 创建 `Advice` 对象，标记为 afterReturning 访问点，包含返回值 `returnObject`
2. 如果用户指定了 `-s` 选项，调用 `watching()` 观察成功返回的数据
3. 调用 `finishing()` 处理默认的 finish 观察点

注意：`-s`（success）和 `-f`（finish）是不同的。`-s` 只在方法成功返回时触发，
而 `-f` 在方法结束（无论成功还是异常）时触发。

### 3.6 afterThrowing() —— 方法抛出异常回调

```java
@Override
public void afterThrowing(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                          Throwable throwable) {
    Advice advice = Advice.newForAfterThrowing(loader, clazz, method, target, args, throwable);
    if (command.isException()) {
        watching(advice);
    }

    finishing(advice);
}
```

当被增强的方法抛出异常时，`AdviceWeaver` 会回调此方法：

1. 创建 `Advice` 对象，标记为 afterThrowing 访问点，包含异常 `throwable`
2. 如果用户指定了 `-e` 选项，调用 `watching()` 观察异常信息
3. 调用 `finishing()` 处理默认的 finish 观察点

### 3.7 finishing() —— 默认结束观察

```java
private void finishing(Advice advice) {
    if (isFinish()) {
        watching(advice);
    }
}
```

这个方法处理默认的 finish 观察点。如果 `isFinish()` 返回 true，调用 `watching()` 方法。

**调用链路总结**：对于一次正常返回的方法调用，如果用户没有指定任何观察点选项：
1. `before()` 被调用 → 启动计时器，不调用 `watching()`
2. 方法执行
3. `afterReturning()` 被调用 → 创建 Advice → `finishing()` → `isFinish()` 返回 true → `watching()`

### 3.8 watching() —— 核心观察逻辑（重点）

这是 `WatchAdviceListener` 最核心的方法，所有观察点的数据都通过这里处理和输出。

```java
private void watching(Advice advice) {
    try {
        // 本次调用的耗时
        double cost = threadLocalWatch.costInMillis();
        boolean conditionResult = isConditionMet(command.getConditionExpress(), advice, cost);
        if (this.isVerbose()) {
            process.write("Condition express: " + command.getConditionExpress()
                + " , result: " + conditionResult + "\n");
        }
        if (conditionResult) {
            // TODO: concurrency issues for process.write

            Object value = getExpressionResult(command.getExpress(), advice, cost);

            WatchModel model = new WatchModel();
            model.setTs(LocalDateTime.now());
            model.setCost(cost);
            model.setValue(new ObjectVO(value, command.getExpand()));
            model.setSizeLimit(command.getSizeLimit());
            model.setClassName(advice.getClazz().getName());
            model.setMethodName(advice.getMethod().getName());
```

逐步分析：

1. **计算耗时**：`threadLocalWatch.costInMillis()` 从 ThreadLocal 栈中弹出开始时间戳，计算耗时（毫秒）
2. **条件判断**：`isConditionMet()` 检查条件表达式是否满足。如果用户没有指定条件表达式，默认返回 true
3. **详细日志**：如果开启了 verbose 模式，输出条件表达式的求值结果
4. **表达式求值**：`getExpressionResult()` 使用 OGNL 对观察表达式求值，得到要展示的数据
5. **构造 WatchModel**：组装结果模型

```java
            if (advice.isBefore()) {
                model.setAccessPoint(AccessPoint.ACCESS_BEFORE.getKey());
            } else if (advice.isAfterReturning()) {
                model.setAccessPoint(AccessPoint.ACCESS_AFTER_RETUNING.getKey());
            } else if (advice.isAfterThrowing()) {
                model.setAccessPoint(AccessPoint.ACCESS_AFTER_THROWING.getKey());
            }

            process.appendResult(model);
            process.times().incrementAndGet();
            if (isLimitExceeded(command.getNumberOfLimit(), process.times().get())) {
                abortProcess(process, command.getNumberOfLimit());
            }
        }
    } catch (Throwable e) {
        logger.warn("watch failed.", e);
        process.end(-1, "watch failed, condition is: " + command.getConditionExpress()
            + ", express is: " + command.getExpress() + ", " + e.getMessage()
            + ", visit " + LogUtil.loggingFile() + " for more details.");
    }
}
```

6. **设置访问点**：根据 Advice 的类型设置 AccessPoint（AtEnter / AtExit / AtExceptionExit）
7. **输出结果**：`process.appendResult(model)` 将结果模型发送到终端
8. **计数**：`process.times().incrementAndGet()` 增加执行次数计数
9. **检查限制**：`isLimitExceeded()` 检查是否达到执行次数上限，如果是则调用 `abortProcess()` 终止命令

**abortProcess 如何工作？** 参见 `AdviceListenerAdapter.abortProcess()`：

```java
protected void abortProcess(CommandProcess process, int limit) {
    process.write("Command execution times exceed limit: " + limit
            + ", so command will exit. You can set it with -n option.\n");
    process.end();
}
```

它写入一条提示信息，然后调用 `process.end()` 结束命令。`process.end()` 会触发清理流程，
包括移除字节码增强、销毁监听器等。

### 3.9 WatchAdviceListener 回调时序图

```
目标方法被调用
    │
    ▼
AdviceWeaver.before()
    │
    ├─ threadLocalWatch.start()  ← 记录开始时间
    ├─ if (isBefore) watching(advice_before)
    │       └─ cost = 0 (刚开始)
    │       └─ OGNL 求值 → WatchModel → appendResult
    │
    ▼
方法执行中...
    │
    ▼
方法正常返回
    │
    ▼
AdviceWeaver.afterReturning()
    │
    ├─ Advice.newForAfterReturning(...)
    ├─ if (isSuccess) watching(advice_return)
    │       └─ cost = 实际耗时
    │       └─ OGNL 求值 → WatchModel → appendResult
    ├─ finishing(advice_return)
    │       └─ if (isFinish) watching(advice_return)
    │               └─ cost = 实际耗时
    │               └─ OGNL 求值 → WatchModel → appendResult
    │
    ▼
或方法抛出异常
    │
    ▼
AdviceWeaver.afterThrowing()
    │
    ├─ Advice.newForAfterThrowing(...)
    ├─ if (isException) watching(advice_throw)
    │       └─ cost = 实际耗时
    │       └─ OGNL 求值 → WatchModel → appendResult
    ├─ finishing(advice_throw)
    │       └─ if (isFinish) watching(advice_throw)
    │               └─ cost = 实际耗时
    │               └─ OGNL 求值 → WatchModel → appendResult
```

---

## 第4阶段：Advice 类 —— 方法调用上下文

### 4.1 Advice 类 —— 方法调用的"快照"

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/advisor/Advice.java`

`Advice` 对象是 Arthas 监控体系中最核心的数据结构之一。它封装了方法调用的完整上下文信息，
是表达式求值的数据源。可以把 `Advice` 理解为方法调用的"快照"——它在某个特定的访问点（before/afterReturning/afterThrowing）
捕获方法调用的所有相关信息。

```java
public class Advice {

    private final ClassLoader loader;
    private final Class<?> clazz;
    private final ArthasMethod method;
    private final Object target;
    private final Object[] params;
    private final Object returnObj;
    private final Throwable throwExp;
    private final int lineNumber;
    private final String[] argNames;
    private final Object[] localVars;
    private final String[] localVarNames;
    private final Map<String, Object> localVarMap;
    private final boolean isBefore;
    private final boolean isThrow;
    private final boolean isReturn;
    private final boolean isLine;
```

### 4.2 Advice 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `loader` | ClassLoader | 加载目标类的类加载器 |
| `clazz` | Class<?> | 目标类 |
| `method` | ArthasMethod | 目标方法 |
| `target` | Object | 方法调用的 this 对象（静态方法为 null） |
| `params` | Object[] | 方法参数数组 |
| `returnObj` | Object | 方法返回值（before/throwing 时为 null） |
| `throwExp` | Throwable | 方法抛出的异常（before/returning 时为 null） |
| `lineNumber` | int | 当前行号（仅 watch -L 模式有效） |
| `argNames` | String[] | 参数名数组（依赖 debug info） |
| `localVars` | Object[] | 局部变量值数组 |
| `localVarNames` | String[] | 局部变量名数组 |
| `localVarMap` | Map<String, Object> | 局部变量名到值的映射 |
| `isBefore` | boolean | 是否为方法入口访问点 |
| `isThrow` | boolean | 是否为异常退出访问点 |
| `isReturn` | boolean | 是否为正常返回访问点 |
| `isLine` | boolean | 是否为行号访问点 |

### 4.3 Advice 的工厂方法 —— 不同访问点创建不同实例

```java
public static Advice newForBefore(ClassLoader loader, Class<?> clazz, ArthasMethod method,
                                  Object target, Object[] params) {
    return new Advice(loader, clazz, method, target, params,
            null,   // returnObj
            null,   // throwExp
            AccessPoint.ACCESS_BEFORE.getValue());
}

public static Advice newForAfterReturning(ClassLoader loader, Class<?> clazz, ArthasMethod method,
                                          Object target, Object[] params, Object returnObj) {
    return new Advice(loader, clazz, method, target, params,
            returnObj,
            null,   // throwExp
            AccessPoint.ACCESS_AFTER_RETUNING.getValue());
}

public static Advice newForAfterThrowing(ClassLoader loader, Class<?> clazz, ArthasMethod method,
                                         Object target, Object[] params, Throwable throwExp) {
    return new Advice(loader, clazz, method, target, params,
            null,   // returnObj
            throwExp,
            AccessPoint.ACCESS_AFTER_THROWING.getValue());
}
```

三个工厂方法分别对应三个访问点：
- `newForBefore`：方法入口，`returnObj` 和 `throwExp` 都为 null
- `newForAfterReturning`：正常返回，只有 `returnObj` 有值
- `newForAfterThrowing`：异常退出，只有 `throwExp` 有值

### 4.4 AccessPoint —— 访问点枚举

```java
public enum AccessPoint {
    ACCESS_BEFORE(1, "AtEnter"),
    ACCESS_AFTER_RETUNING(1 << 1, "AtExit"),
    ACCESS_AFTER_THROWING(1 << 2, "AtExceptionExit"),
    ACCESS_LINE(1 << 3, "AtLine");
```

`AccessPoint` 使用位掩码（Bitmask）设计，每个访问点对应一个 bit：

| 枚举值 | 十进制 | 二进制 | key |
|--------|--------|--------|-----|
| ACCESS_BEFORE | 1 | 0001 | AtEnter |
| ACCESS_AFTER_RETUNING | 2 | 0010 | AtExit |
| ACCESS_AFTER_THROWING | 4 | 0100 | AtExceptionExit |
| ACCESS_LINE | 8 | 1000 | AtLine |

使用位掩码的好处是可以用一个 int 值同时表示多个访问点。在 Advice 构造函数中：

```java
isBefore = (access & AccessPoint.ACCESS_BEFORE.getValue()) == AccessPoint.ACCESS_BEFORE.getValue();
isThrow = (access & AccessPoint.ACCESS_AFTER_THROWING.getValue()) == AccessPoint.ACCESS_AFTER_THROWING.getValue();
isReturn = (access & AccessPoint.ACCESS_AFTER_RETUNING.getValue()) == AccessPoint.ACCESS_AFTER_RETUNING.getValue();
isLine = (access & AccessPoint.ACCESS_LINE.getValue()) == AccessPoint.ACCESS_LINE.getValue();
```

通过按位与运算判断当前 Advice 对应哪个访问点。

### 4.5 Advice 构造函数 —— 局部变量处理

```java
private Advice(ClassLoader loader, Class<?> clazz, ArthasMethod method,
               Object target, Object[] params, Object returnObj, Throwable throwExp,
               int access, int lineNumber, String[] argNames,
               Object[] localVars, String[] localVarNames) {
    this.loader = loader;
    this.clazz = clazz;
    this.method = method;
    this.target = target;
    this.params = params;
    this.returnObj = returnObj;
    this.throwExp = throwExp;
    this.lineNumber = lineNumber;
    this.argNames = argNames;
    LocalVariableSnapshot snapshot = normalizeLocalVariables(localVarNames, localVars);
    this.localVars = snapshot.values;
    this.localVarNames = snapshot.names;
    this.localVarMap = buildLocalVarMap(this.localVarNames, this.localVars);
    // ... 位掩码判断 ...
}
```

构造函数中有一个重要的步骤：`normalizeLocalVariables()`，它处理局部变量中的 `this` 引用。

```java
private static LocalVariableSnapshot normalizeLocalVariables(String[] localVarNames, Object[] localVars) {
    if (localVarNames == null || localVars == null) {
        return new LocalVariableSnapshot(localVarNames, localVars);
    }
    int length = Math.min(localVarNames.length, localVars.length);
    int thisIndex = -1;
    for (int i = 0; i < length; i++) {
        if ("this".equals(localVarNames[i])) {
            thisIndex = i;
            break;
        }
    }
    if (thisIndex < 0) {
        return new LocalVariableSnapshot(localVarNames, localVars);
    }
    // 过滤掉 "this" 变量
    String[] filteredNames = new String[length - 1];
    Object[] filteredValues = new Object[length - 1];
    int index = 0;
    for (int i = 0; i < length; i++) {
        if (i == thisIndex) {
            continue;
        }
        filteredNames[index] = localVarNames[i];
        filteredValues[index] = localVars[i];
        index++;
    }
    return new LocalVariableSnapshot(filteredNames, filteredValues);
}
```

**为什么需要过滤 "this"？** 在 JVM 字节码中，实例方法的第一个局部变量总是 `this`。
但是 Arthas 已经通过 `target` 字段提供了对 this 对象的访问，如果局部变量列表中也保留 this，会导致重复和混淆。
因此在构建 Advice 时，会将局部变量中的 "this" 过滤掉。

### 4.6 buildLocalVarMap() —— 局部变量映射构建

```java
private static Map<String, Object> buildLocalVarMap(String[] localVarNames, Object[] localVars) {
    if (localVarNames == null || localVars == null) {
        return Collections.emptyMap();
    }
    Map<String, Object> result = new LinkedHashMap<String, Object>();
    int length = Math.min(localVarNames.length, localVars.length);
    for (int i = 0; i < length; i++) {
        String name = localVarNames[i];
        if (name != null && name.length() > 0) {
            result.put(name, localVars[i]);
        }
    }
    return result;
}
```

这个方法将局部变量名和值组装成一个 Map，方便通过名称访问。使用 `LinkedHashMap` 保持插入顺序。
在 OGNL 表达式中，用户可以通过局部变量名直接访问对应的值。

### 4.7 Advice 与 OGNL 表达式的关系

Advice 对象是 OGNL 表达式求值时的"根对象"（root object）。当用户写 `params[0]` 这样的表达式时，
OGNL 会从 Advice 对象中获取 `params` 属性，然后取第一个元素。

下表列出了 OGNL 表达式中可用的变量及其对应的 Advice 字段：

| OGNL 变量 | Advice 字段 | 说明 |
|-----------|-------------|------|
| `params` | `params` | 方法参数数组 |
| `target` | `target` | this 对象 |
| `returnObj` | `returnObj` | 返回值 |
| `throwExp` | `throwExp` | 异常对象 |
| `clazz` | `clazz` | 目标类 |
| `method` | `method` | 目标方法 |
| `#cost` | (外部绑定) | 方法耗时（毫秒） |

---

## 第5阶段：AdviceListener 接口与 AdviceListenerAdapter 适配器

### 5.1 AdviceListener 接口 —— 通知监听器契约

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/advisor/AdviceListener.java`

```java
public interface AdviceListener {

    long id();

    void create();

    void destroy();

    void before(Class<?> clazz, String methodName, String methodDesc,
            Object target, Object[] args) throws Throwable;

    void afterReturning(Class<?> clazz, String methodName, String methodDesc,
            Object target, Object[] args, Object returnObject) throws Throwable;

    void afterThrowing(Class<?> clazz, String methodName, String methodDesc,
            Object target, Object[] args, Throwable throwable) throws Throwable;

    default void atLine(Class<?> clazz, String methodName, String methodDesc,
            Object target, Object[] args,
            int lineNumber, String[] argNames, Object[] localVars,
            String[] localVarNames) throws Throwable {
    }
}
```

`AdviceListener` 接口定义了通知监听器的完整契约：

- `id()`：返回监听器的唯一 ID
- `create()`：监听器创建时的回调
- `destroy()`：监听器销毁时的回调
- `before()`：方法调用前通知
- `afterReturning()`：方法正常返回通知
- `afterThrowing()`：方法抛出异常通知
- `atLine()`：行号通知（用于 watch 命令的行号模式）

注意接口中的方法参数使用的是原始类型（`Class<?>`、`String`），而不是 `ArthasMethod`。
这是因为这些方法会被 `AdviceWeaver`（通过 SpyAPI）调用，而 SpyAPI 运行在目标 JVM 的 bootstrap classloader 中，
无法直接引用 Arthas 的类。

### 5.2 AdviceListenerAdapter —— 监听器适配器（重点）

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/advisor/AdviceListenerAdapter.java`

`AdviceListenerAdapter` 是一个抽象适配器类，实现了 `AdviceListener` 接口，提供了大量通用的模板方法。

```java
public abstract class AdviceListenerAdapter implements AdviceListener, ProcessAware {
    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
    private Process process;
    private long id = ID_GENERATOR.addAndGet(1);
    private boolean verbose;
```

- `ID_GENERATOR`：原子递增的 ID 生成器，确保每个监听器有唯一 ID
- `process`：关联的命令进程
- `id`：监听器的唯一标识符

### 5.3 AdviceListenerAdapter 的方法适配

```java
@Override
final public void before(Class<?> clazz, String methodName, String methodDesc,
        Object target, Object[] args) throws Throwable {
    before(clazz.getClassLoader(), clazz, new ArthasMethod(clazz, methodName, methodDesc), target, args);
}

@Override
final public void afterReturning(Class<?> clazz, String methodName, String methodDesc,
        Object target, Object[] args, Object returnObject) throws Throwable {
    afterReturning(clazz.getClassLoader(), clazz, new ArthasMethod(clazz, methodName, methodDesc),
            target, args, returnObject);
}

@Override
final public void afterThrowing(Class<?> clazz, String methodName, String methodDesc,
        Object target, Object[] args, Throwable throwable) throws Throwable {
    afterThrowing(clazz.getClassLoader(), clazz, new ArthasMethod(clazz, methodName, methodDesc),
            target, args, throwable);
}
```

这些 `final` 方法是接口方法到抽象方法的适配器。它们将接口层面的原始参数（`Class<?>`、`String methodName`）
转换为更高层的参数（`ClassLoader`、`ArthasMethod`），然后调用子类实现的抽象方法。

**为什么用 final？** 防止子类覆盖适配逻辑，确保参数转换的一致性。

### 5.4 isConditionMet() —— 条件表达式判断

```java
protected boolean isConditionMet(String conditionExpress, Advice advice, double cost) throws ExpressException {
    return StringUtils.isEmpty(conditionExpress)
            || ExpressFactory.threadLocalExpress(advice).bind(Constants.COST_VARIABLE, cost).is(conditionExpress);
}
```

这个方法判断条件表达式是否满足，逻辑如下：

1. 如果条件表达式为空（用户没有指定条件），返回 true（无条件通过）
2. 否则，使用 `ExpressFactory.threadLocalExpress(advice)` 获取线程本地的 Express 对象
3. `.bind(Constants.COST_VARIABLE, cost)` 将 `#cost` 变量绑定到表达式中
4. `.is(conditionExpress)` 对条件表达式求值，返回布尔结果

`Constants.COST_VARIABLE` 的值是 `"cost"`，所以在条件表达式中使用 `#cost` 来引用方法耗时。
例如：`#cost>100` 表示只观察耗时超过 100ms 的方法调用。

### 5.5 getExpressionResult() —— 观察表达式求值

```java
protected Object getExpressionResult(String express, Advice advice, double cost) throws ExpressException {
    return ExpressFactory.threadLocalExpress(advice).bind(Constants.COST_VARIABLE, cost).get(express);
}
```

与 `isConditionMet()` 类似，但使用 `.get()` 而不是 `.is()`。`.get()` 返回表达式的求值结果（任意类型），
而 `.is()` 返回布尔值。

例如，对于表达式 `{params, target, returnObj}`，`.get()` 会返回一个包含三个元素的数组。

### 5.6 isLimitExceeded() 与 abortProcess() —— 次数限制

```java
protected boolean isLimitExceeded(int limit, int currentTimes) {
    return currentTimes >= limit;
}

protected void abortProcess(CommandProcess process, int limit) {
    process.write("Command execution times exceed limit: " + limit
            + ", so command will exit. You can set it with -n option.\n");
    process.end();
}
```

- `isLimitExceeded()`：检查当前执行次数是否达到上限
- `abortProcess()`：终止命令执行，写入提示信息并调用 `process.end()`

**abortProcess 如何保证只中断一次？** 在单线程场景下不会有问题。但在多线程场景下，
如果多个线程同时触发方法调用，可能会同时调用 `abortProcess()`。
`AbstractTraceAdviceListener` 通过 `AtomicBoolean` 解决了这个问题（详见第8阶段分析）。

### 5.7 AdviceListenerAdapter 继承体系

```
AdviceListener (接口)
    │
    └── AdviceListenerAdapter (抽象适配器)
            ├── WatchAdviceListener
            ├── StackAdviceListener
            ├── AbstractTraceAdviceListener
            │       └── TraceAdviceListener (implements InvokeTraceable)
            │       └── PathTraceAdviceListener
            ├── MonitorAdviceListener
            └── TimeTunnelAdviceListener
```

所有具体的监听器都继承自 `AdviceListenerAdapter`，复用了条件判断、表达式求值、次数限制等通用逻辑。

---

## 第6阶段：ExpressFactory 与 OGNL 表达式

### 6.1 ExpressFactory 类 —— 表达式工厂

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/express/ExpressFactory.java`

```java
public class ExpressFactory {

    /**
     * 这里不能直接在 ThreadLocalMap 里强引用 Express（它由 ArthasClassLoader 加载），
     * 否则 stop/detach 后会被业务线程持有，导致 ArthasClassLoader 无法被 GC 回收。
     *
     * 用 WeakReference 打断强引用链：Thread -> ThreadLocalMap -> value(WeakReference) -X-> Express。
     */
    private static final ThreadLocal<WeakReference<Express>> expressRef = ThreadLocal
            .withInitial(() -> new WeakReference<Express>(new OgnlExpress()));

    public static Express threadLocalExpress(Object object) {
        WeakReference<Express> reference = expressRef.get();
        Express express = reference == null ? null : reference.get();
        if (express == null) {
            express = new OgnlExpress();
            expressRef.set(new WeakReference<Express>(express));
        }
        return express.reset().bind(object);
    }

    public static Express unpooledExpress(ClassLoader classloader) {
        if (classloader == null) {
            classloader = ClassLoader.getSystemClassLoader();
        }
        return new OgnlExpress(new ClassLoaderClassResolver(classloader));
    }
}
```

### 6.2 ThreadLocal + WeakReference 的精妙设计

`ExpressFactory` 使用 `ThreadLocal<WeakReference<Express>>` 来缓存 Express 对象，这个设计非常精妙：

**问题背景**：Arthas 的 Express 对象（`OgnlExpress`）由 `ArthasClassLoader` 加载。
如果直接在 ThreadLocal 中强引用 Express 对象，会导致：
```
业务线程 → ThreadLocalMap → value(Express) → ArthasClassLoader
```
这条强引用链会阻止 `ArthasClassLoader` 被 GC 回收。当用户执行 `stop` 或 `detach` 命令时，
ArthasClassLoader 应该被卸载，但因为业务线程的 ThreadLocal 持有强引用，导致内存泄漏。

**解决方案**：使用 `WeakReference` 打断强引用链：
```
业务线程 → ThreadLocalMap → value(WeakReference) -X-> Express → ArthasClassLoader
```
WeakReference 的引用不会阻止 GC，所以当 ArthasClassLoader 没有其他强引用时，Express 对象可以被回收。

**类比理解**：就像你借了朋友的一本书，如果直接放在书架上（强引用），朋友永远拿不回去。
但如果用一根橡皮筋挂在书架上（弱引用），当朋友要拿走时，橡皮筋会断开，书就可以被拿走了。

### 6.3 threadLocalExpress() —— 获取线程本地 Express

```java
public static Express threadLocalExpress(Object object) {
    WeakReference<Express> reference = expressRef.get();
    Express express = reference == null ? null : reference.get();
    if (express == null) {
        express = new OgnlExpress();
        expressRef.set(new WeakReference<Express>(express));
    }
    return express.reset().bind(object);
}
```

方法流程：
1. 从 ThreadLocal 获取 WeakReference
2. 从 WeakReference 获取 Express 对象（可能已被 GC 回收，返回 null）
3. 如果 Express 为 null（被 GC 回收或首次使用），创建新的 OgnlExpress
4. 将新的 Express 重新放入 ThreadLocal（修复被 GC 回收的情况）
5. `reset()` 清除上下文中的旧数据
6. `bind(object)` 绑定根对象（通常是 Advice 对象）

### 6.4 unpooledExpress() —— 无缓存版本

```java
public static Express unpooledExpress(ClassLoader classloader) {
    if (classloader == null) {
        classloader = ClassLoader.getSystemClassLoader();
    }
    return new OgnlExpress(new ClassLoaderClassResolver(classloader));
}
```

`unpooledExpress` 每次都创建新的 Express 对象，不使用 ThreadLocal 缓存。
它接受一个 ClassLoader 参数，使用 `ClassLoaderClassResolver` 来解析类名。

**什么时候用 unpooledExpress？** 在 `TimeTunnelCommand.processWatch()` 中使用：
```java
Object value = ExpressFactory.unpooledExpress(advice.getLoader()).bind(advice).get(watchExpress);
```
因为 tt 命令的回放可能需要使用与原始方法调用时相同的 ClassLoader 来解析类名，
而 threadLocalExpress 使用的是 `CustomClassResolver`，可能无法正确解析业务类。

### 6.5 OgnlExpress —— OGNL 表达式实现

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/express/OgnlExpress.java`

```java
public class OgnlExpress implements Express {
    private static final MemberAccess MEMBER_ACCESS = new DefaultMemberAccess(true);
    private static final ArthasObjectPropertyAccessor OBJECT_PROPERTY_ACCESSOR = new ArthasObjectPropertyAccessor();

    private Object bindObject;
    private final OgnlContext context;

    public OgnlExpress() {
        this(CustomClassResolver.customClassResolver);
    }

    public OgnlExpress(ClassResolver classResolver) {
        OgnlRuntime.setPropertyAccessor(Object.class, OBJECT_PROPERTY_ACCESSOR);
        context = new OgnlContext(MEMBER_ACCESS, classResolver, null, null);
    }
```

- `MEMBER_ACCESS`：成员访问器，`DefaultMemberAccess(true)` 允许访问 private 成员
- `OBJECT_PROPERTY_ACCESSOR`：自定义的属性访问器，用于优化属性访问逻辑
- `bindObject`：绑定的根对象（通常是 Advice 对象）
- `context`：OGNL 上下文，存储变量和配置

### 6.6 OgnlExpress 的核心方法

```java
@Override
public Object get(String express) throws ExpressException {
    try {
        return Ognl.getValue(express, context, bindObject);
    } catch (Exception e) {
        logger.error("Error during evaluating the expression:", e);
        throw new ExpressException(express, e);
    }
}

@Override
public boolean is(String express) throws ExpressException {
    final Object ret = get(express);
    return ret instanceof Boolean && (Boolean) ret;
}

@Override
public Express bind(Object object) {
    this.bindObject = object;
    return this;
}

@Override
public Express bind(String name, Object value) {
    context.put(name, value);
    return this;
}

@Override
public Express reset() {
    context.clear();
    return this;
}
```

| 方法 | 说明 |
|------|------|
| `get(express)` | 对表达式求值，返回任意类型的结果 |
| `is(express)` | 对表达式求值，返回布尔值 |
| `bind(object)` | 绑定根对象（作为 OGNL 的 root） |
| `bind(name, value)` | 绑定命名变量到上下文 |
| `reset()` | 清除上下文中的所有变量 |

**bind(object) vs bind(name, value)** 的区别：
- `bind(object)` 设置 OGNL 的 root 对象，表达式中可以直接访问其属性（如 `params`、`returnObj`）
- `bind(name, value)` 在 OGNL 上下文中设置命名变量，表达式中需要用 `#name` 来引用（如 `#cost`）

### 6.7 OGNL 表达式求值流程

以 `watch demo.MathGame run '{params, target, returnObj}' '#cost>100'` 为例：

```
1. Advice 对象创建（包含 params, target, returnObj 等信息）
2. isConditionMet("#cost>100", advice, cost=120.5)
   └─ ExpressFactory.threadLocalExpress(advice)
      └─ bindObject = advice
      └─ bind("cost", 120.5)
         └─ context.put("cost", 120.5)
      └─ is("#cost>100")
         └─ Ognl.getValue("#cost>100", context, advice)
            └─ 从 context 获取 #cost = 120.5
            └─ 120.5 > 100 → true
   → 条件满足，继续执行

3. getExpressionResult("{params, target, returnObj}", advice, cost=120.5)
   └─ ExpressFactory.threadLocalExpress(advice)  (复用同一个 Express)
      └─ reset()  清除之前的上下文
      └─ bind(advice)  重新绑定根对象
      └─ bind("cost", 120.5)  重新绑定 #cost
      └─ get("{params, target, returnObj}")
         └─ Ognl.getValue("{params, target, returnObj}", context, advice)
            └─ 从 advice 获取 params → Object[]
            └─ 从 advice 获取 target → MathGame 实例
            └─ 从 advice 获取 returnObj → 返回值
            └─ 返回 Object[] {params, target, returnObj}
   → 得到观察结果
```

---

## 第7阶段：WatchModel 与结果输出

### 7.1 WatchModel 类 —— watch 命令结果模型

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/model/WatchModel.java`

```java
public class WatchModel extends ResultModel {

    private LocalDateTime ts;
    private double cost;
    private ObjectVO value;

    private Integer sizeLimit;
    private String className;
    private String methodName;
    private String accessPoint;

    @Override
    public String getType() {
        return "watch";
    }
```

`WatchModel` 继承自 `ResultModel`，是 watch 命令输出的数据模型：

| 字段 | 类型 | 说明 |
|------|------|------|
| `ts` | LocalDateTime | 时间戳 |
| `cost` | double | 方法耗时（毫秒） |
| `value` | ObjectVO | 观察结果（OGNL 求值结果） |
| `sizeLimit` | Integer | 结果大小限制 |
| `className` | String | 类名 |
| `methodName` | String | 方法名 |
| `accessPoint` | String | 访问点（AtEnter/AtExit/AtExceptionExit） |

`getType()` 返回 `"watch"`，用于标识结果类型，前端根据类型选择对应的渲染器。

### 7.2 ObjectVO —— 值对象与展开深度

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/arthas-model/src/main/java/com/taobao/arthas/core/command/model/ObjectVO.java`

```java
public class ObjectVO {
    private Object object;
    private Integer expand;

    public ObjectVO(Object object, Integer expand) {
        this.object = object;
        this.expand = expand;
    }

    public static ObjectVO[] array(Object[] objects, Integer expand) {
        if (objects == null) {
            return new ObjectVO[0];
        }
        ObjectVO[] result = new ObjectVO[objects.length];
        for (int i = 0; i < objects.length; ++i) {
            result[i] = new ObjectVO(objects[i], expand);
        }
        return result;
    }

    public int expandOrDefault() {
        if (expand != null) {
            return expand;
        }
        return 1;
    }

    public boolean needExpand() {
        return null != expand && expand > 0;
    }
```

`ObjectVO` 是一个包装类，将原始值与展开深度绑定在一起。

**为什么需要 ObjectVO？** 注释说明：`包装一层，解决json输出问题`。
直接序列化复杂对象可能遇到循环引用、不可序列化等问题。ObjectVO 将原始对象和展开深度一起传递给渲染器，
渲染器根据展开深度决定展示多少层。

| 方法 | 说明 |
|------|------|
| `array(objects, expand)` | 将 Object 数组转换为 ObjectVO 数组 |
| `expandOrDefault()` | 返回展开深度，默认为 1 |
| `needExpand()` | 是否需要展开（expand > 0） |

### 7.3 WatchModel 的构建过程

回顾 `WatchAdviceListener.watching()` 中的 WatchModel 构建：

```java
WatchModel model = new WatchModel();
model.setTs(LocalDateTime.now());          // 当前时间
model.setCost(cost);                        // 方法耗时
model.setValue(new ObjectVO(value, command.getExpand()));  // OGNL 结果 + 展开深度
model.setSizeLimit(command.getSizeLimit()); // 大小限制
model.setClassName(advice.getClazz().getName());    // 类名
model.setMethodName(advice.getMethod().getName());  // 方法名
if (advice.isBefore()) {
    model.setAccessPoint(AccessPoint.ACCESS_BEFORE.getKey());         // "AtEnter"
} else if (advice.isAfterReturning()) {
    model.setAccessPoint(AccessPoint.ACCESS_AFTER_RETUNING.getKey());  // "AtExit"
} else if (advice.isAfterThrowing()) {
    model.setAccessPoint(AccessPoint.ACCESS_AFTER_THROWING.getKey());  // "AtExceptionExit"
}

process.appendResult(model);
```

每一步都清晰对应：时间戳、耗时、观察值、类名方法名、访问点。最终通过 `process.appendResult(model)` 输出到终端。

### 7.4 结果渲染流程

```
WatchModel
    │
    ▼
process.appendResult(model)
    │
    ▼
ResultView (根据 model.getType() = "watch" 选择渲染器)
    │
    ▼
WatchResultView.render(model)
    │
    ├─ 输出时间戳 ts
    ├─ 输出类名.方法名
    ├─ 输出访问点 (@A[AtEnter] / @A[AtExit] / @A[AtExceptionExit])
    ├─ 输出耗时 cost(ms)
    └─ 输出观察值 (按 expand 深度展开 ObjectVO)
```

终端输出示例：
```
ts=2024-01-15 10:30:45; [cost=120.534ms] result=ArrayList(
    @ArrayList[接收方法的参数列表],
    @MathGame[demo.MathGame@45ee12],
    @Integer[42],
)
```

---

## 第8阶段：TraceCommand 与 AbstractTraceAdviceListener

### 8.1 TraceCommand 类 —— trace 命令定义

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TraceCommand.java`

```java
@Name("trace")
@Summary("Trace the execution time of specified method invocation.")
@Description(value = Constants.EXPRESS_DESCRIPTION + Constants.EXAMPLE +
        "  trace org.apache.commons.lang.StringUtils isBlank\n" +
        "  trace *StringUtils isBlank\n" +
        "  trace *StringUtils isBlank params[0].length==1\n" +
        "  trace *StringUtils isBlank '#cost>100'\n" +
        "  trace -E org\\\\.apache\\\\.commons\\\\.lang\\\\.StringUtils isBlank\n" +
        "  trace -E com.test.ClassA|org.test.ClassB method1|method2|method3\n" +
        "  trace demo.MathGame run -n 5\n" +
        "  trace demo.MathGame run --skipJDKMethod false\n" +
        Constants.WIKI + Constants.WIKI_HOME + "trace")
public class TraceCommand extends EnhancerCommand {
```

### 8.2 TraceCommand 字段定义

```java
private String classPattern;
private String methodPattern;
private String conditionExpress;
private boolean isRegEx = false;
private int numberOfLimit = 100;
private List<String> pathPatterns;
private boolean skipJDKTrace;
```

与 `WatchCommand` 相比，`TraceCommand` 有两个特有字段：

| 字段 | 说明 |
|------|------|
| `pathPatterns` | 路径追踪模式（`-p` 选项），支持追踪特定调用路径 |
| `skipJDKTrace` | 是否跳过 JDK 方法追踪（默认 true） |

`trace` 命令没有 `express`（观察表达式）字段，因为 trace 的输出是调用树结构，不需要额外的表达式来决定观察什么。

### 8.3 trace 命令的特有选项

```java
@Option(shortName = "p", longName = "path", acceptMultipleValues = true)
@Description("path tracing pattern")
public void setPathPatterns(List<String> pathPatterns) {
    this.pathPatterns = pathPatterns;
}

@Option(longName = "skipJDKMethod")
@DefaultValue("true")
@Description("skip jdk method trace, default value true.")
public void setSkipJDKTrace(boolean skipJDKTrace) {
    this.skipJDKTrace = skipJDKTrace;
}
```

- `-p` 选项支持路径追踪，可以指定要追踪的调用路径上的类。例如：
  `trace ClassA methodA -p ClassB -p ClassC` 会追踪从 ClassA.methodA 开始，经过 ClassB 和 ClassC 的调用路径。
- `--skipJDKMethod` 默认为 true，跳过 JDK 方法的追踪。这是因为 JDK 方法调用非常频繁，
  如果全部追踪会导致输出爆炸且性能严重下降。

### 8.4 路径追踪的匹配器构建

```java
@Override
protected Matcher getClassNameMatcher() {
    if (classNameMatcher == null) {
        if (pathPatterns == null || pathPatterns.isEmpty()) {
            classNameMatcher = SearchUtils.classNameMatcher(getClassPattern(), isRegEx());
        } else {
            classNameMatcher = getPathTracingClassMatcher();
        }
    }
    return classNameMatcher;
}

private Matcher<String> getPathTracingClassMatcher() {
    List<Matcher<String>> matcherList = new ArrayList<Matcher<String>>();
    matcherList.add(SearchUtils.classNameMatcher(getClassPattern(), isRegEx()));

    if (null != getPathPatterns()) {
        for (String pathPattern : getPathPatterns()) {
            if (isRegEx()) {
                matcherList.add(new RegexMatcher(pathPattern));
            } else {
                matcherList.add(new WildcardMatcher(pathPattern));
            }
        }
    }

    return new GroupMatcher.Or<String>(matcherList);
}

private Matcher<String> getPathTracingMethodMatcher() {
    return new TrueMatcher<String>();
}
```

路径追踪模式下的匹配器构建逻辑：
1. 创建一个 `Or` 组合匹配器
2. 将原始类名匹配器加入
3. 将每个路径模式也作为类名匹配器加入
4. 方法名匹配器使用 `TrueMatcher`，即匹配所有方法

**为什么要用 Or 匹配？** 路径追踪需要同时增强多个类：原始目标类 + 路径上的类。
Or 匹配器表示"匹配任意一个即可"，确保所有相关类都会被增强。

### 8.5 getAdviceListener() —— 创建 TraceAdviceListener

```java
@Override
protected AdviceListener getAdviceListener(CommandProcess process) {
    if (pathPatterns == null || pathPatterns.isEmpty()) {
        return new TraceAdviceListener(this, process, GlobalOptions.verbose || this.verbose);
    } else {
        return new PathTraceAdviceListener(this, process);
    }
}
```

根据是否指定了路径模式，创建不同的监听器：
- 没有路径模式：创建 `TraceAdviceListener`（标准 trace）
- 有路径模式：创建 `PathTraceAdviceListener`（路径 trace）

### 8.6 AbstractTraceAdviceListener —— trace 回调基类（重点）

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/AbstractTraceAdviceListener.java`

```java
public abstract class AbstractTraceAdviceListener extends AdviceListenerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTraceAdviceListener.class);
    protected final ThreadLocalWatch threadLocalWatch = new ThreadLocalWatch();
    protected TraceCommand command;
    protected CommandProcess process;
    private final AtomicBoolean processAborted = new AtomicBoolean(false);

    protected final ThreadLocal<TraceEntity> threadBoundEntity = new ThreadLocal<TraceEntity>();
```

关键字段：
- `threadLocalWatch`：线程本地计时器，与 WatchAdviceListener 中的相同
- `processAborted`：原子布尔值，确保 `abortProcess` 只执行一次
- `threadBoundEntity`：ThreadLocal 持有的 TraceEntity，每个线程一个独立的调用树

### 8.7 threadLocalTraceEntity() —— 获取线程本地调用树

```java
protected TraceEntity threadLocalTraceEntity(ClassLoader loader) {
    TraceEntity traceEntity = threadBoundEntity.get();
    if (traceEntity == null) {
        traceEntity = new TraceEntity(loader);
        threadBoundEntity.set(traceEntity);
    }
    return traceEntity;
}
```

这个方法实现了"每线程一个调用树"的模式：
1. 从 ThreadLocal 获取当前线程的 TraceEntity
2. 如果不存在（首次调用），创建新的 TraceEntity
3. 将 TraceEntity 放入 ThreadLocal
4. 返回 TraceEntity

**为什么用 ThreadLocal？** 因为一个被 trace 的方法可能被多个线程同时调用。每个线程的调用栈是独立的，
如果不使用 ThreadLocal，不同线程的调用树会混在一起，导致输出混乱。ThreadLocal 确保每个线程有自己独立的调用树。

### 8.8 before() —— 方法调用前回调

```java
@Override
public void before(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args)
        throws Throwable {
    TraceEntity traceEntity = threadLocalTraceEntity(loader);
    traceEntity.tree.begin(clazz.getName(), method.getName(), -1, false);
    traceEntity.deep++;
    // 开始计算本次方法调用耗时
    threadLocalWatch.start();
}
```

before 回调做了三件事：
1. 获取线程本地的 TraceEntity
2. 在调用树中开始一个新节点：`tree.begin(className, methodName, -1, false)`
   - lineNumber 为 -1 表示这是方法入口的 begin（不是方法内部调用的 begin）
   - isInvoking 为 false 表示这是方法体入口的 onBefore，不是 invoke 调用
3. `deep++`：调用深度递增
4. 启动计时器

### 8.9 afterReturning() —— 方法正常返回回调

```java
@Override
public void afterReturning(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                           Object returnObject) throws Throwable {
    threadLocalTraceEntity(loader).tree.end();
    final Advice advice = Advice.newForAfterReturning(loader, clazz, method, target, args, returnObject);
    finishing(loader, advice);
}
```

1. `tree.end()`：结束当前调用树节点，记录结束时间戳
2. 创建 Advice 对象
3. 调用 `finishing()` 处理深度计数和结果输出

### 8.10 afterThrowing() —— 方法抛出异常回调

```java
@Override
public void afterThrowing(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                          Throwable throwable) throws Throwable {
    int lineNumber = -1;
    StackTraceElement[] stackTrace = throwable.getStackTrace();
    if (stackTrace.length != 0) {
        lineNumber = stackTrace[0].getLineNumber();
    }

    threadLocalTraceEntity(loader).tree.end(throwable, lineNumber);
    final Advice advice = Advice.newForAfterThrowing(loader, clazz, method, target, args, throwable);
    finishing(loader, advice);
}
```

与 afterReturning 的区别：
1. 从异常的堆栈中提取行号信息
2. 调用 `tree.end(throwable, lineNumber)` 而不是 `tree.end()`，在调用树中添加一个 ThrowNode

### 8.11 finishing() —— 深度计数与结果输出（重点）

```java
private void finishing(ClassLoader loader, Advice advice) {
    // 本次调用的耗时
    TraceEntity traceEntity = threadLocalTraceEntity(loader);
    if (traceEntity.deep >= 1) { // #1817 防止deep为负数
        traceEntity.deep--;
    }
    if (traceEntity.deep == 0) {
        double cost = threadLocalWatch.costInMillis();
        try {
            boolean conditionResult = isConditionMet(command.getConditionExpress(), advice, cost);
            if (this.isVerbose()) {
                process.write("Condition express: " + command.getConditionExpress()
                    + " , result: " + conditionResult + "\n");
            }
            if (conditionResult) {
                // 满足输出条件
                process.times().incrementAndGet();
                process.appendResult(traceEntity.getModel());

                // 是否到达数量限制
                if (isLimitExceeded(command.getNumberOfLimit(), process.times().get())) {
                    abortProcess(process, command.getNumberOfLimit());
                }
            }
        } catch (Throwable e) {
            logger.warn("trace failed.", e);
            process.end(1, "trace failed, condition is: " + command.getConditionExpress()
                          + ", " + e.getMessage() + ", visit " + LogUtil.loggingFile()
                          + " for more details.");
        } finally {
            threadBoundEntity.remove();
        }
    }
}
```

这是 trace 命令最核心的方法，逻辑如下：

1. **深度递减**：`traceEntity.deep--`，表示一层方法调用结束
2. **深度为 0 时输出**：只有当 `deep == 0` 时，表示整个调用链路（从被 trace 的方法开始到结束）已经完成
3. **计算耗时**：`threadLocalWatch.costInMillis()` 获取整个调用链路的总耗时
4. **条件判断**：检查条件表达式是否满足
5. **输出调用树**：`process.appendResult(traceEntity.getModel())` 输出整棵调用树
6. **次数限制检查**：达到上限则终止
7. **清理 ThreadLocal**：`threadBoundEntity.remove()` 在 finally 块中移除 ThreadLocal，防止内存泄漏

**deep 计数器的原理**：
- `before()` 中 `deep++`
- `finishing()` 中 `deep--`
- 当 `deep == 0` 时，表示回到了最外层方法

例如，调用链路 A → B → C：
```
before(A): deep=1, tree.begin(A)
  before(B): deep=2, tree.begin(B)
    before(C): deep=3, tree.begin(C)
    afterReturning(C): tree.end(), finishing(C): deep=2 (不为0，不输出)
  afterReturning(B): tree.end(), finishing(B): deep=1 (不为0，不输出)
afterReturning(A): tree.end(), finishing(A): deep=0 (输出整棵树!)
```

### 8.12 abortProcess() 的线程安全保证

```java
@Override
protected void abortProcess(CommandProcess process, int limit) {
    // Only proceed if this thread is the first one to set the flag to true
    if (processAborted.compareAndSet(false, true)) {
        super.abortProcess(process, limit);
    }
}
```

**为什么需要 AtomicBoolean？** 在多线程场景下，如果被 trace 的方法被多个线程同时调用，
可能有两个线程同时到达次数上限并同时调用 `abortProcess()`。`process.end()` 如果被调用多次会导致错误。

`compareAndSet(false, true)` 是一个原子操作：只有当当前值为 false 时才设置为 true 并返回 true。
这确保只有一个线程能通过这个检查，其他线程会被跳过。

### 8.13 destroy() —— 清理 ThreadLocal

```java
@Override
public void destroy() {
    threadBoundEntity.remove();
}
```

在监听器被销毁时，清理当前线程的 ThreadLocal。**注意**：这里只能清理当前线程的 ThreadLocal，
其他线程的 ThreadLocal 需要等待那些线程自然结束时才会被清理。

---

## 第9阶段：TraceAdviceListener 与 InvokeTraceable 接口

### 9.1 TraceAdviceListener 类 —— trace 的具体实现

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TraceAdviceListener.java`

```java
public class TraceAdviceListener extends AbstractTraceAdviceListener implements InvokeTraceable {

    public TraceAdviceListener(TraceCommand command, CommandProcess process, boolean verbose) {
        super(command, process);
        super.setVerbose(verbose);
    }

    @Override
    public void invokeBeforeTracing(ClassLoader classLoader, String tracingClassName,
            String tracingMethodName, String tracingMethodDesc, int tracingLineNumber) throws Throwable {
        threadLocalTraceEntity(classLoader).tree.begin(tracingClassName, tracingMethodName,
                tracingLineNumber, true);
    }

    @Override
    public void invokeAfterTracing(ClassLoader classLoader, String tracingClassName,
            String tracingMethodName, String tracingMethodDesc, int tracingLineNumber) throws Throwable {
        threadLocalTraceEntity(classLoader).tree.end();
    }

    @Override
    public void invokeThrowTracing(ClassLoader classLoader, String tracingClassName,
            String tracingMethodName, String tracingMethodDesc, int tracingLineNumber) throws Throwable {
        threadLocalTraceEntity(classLoader).tree.end(true);
    }
}
```

### 9.2 InvokeTraceable 接口 —— 方法调用跟踪契约

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/advisor/InvokeTraceable.java`

```java
public interface InvokeTraceable {

    void invokeBeforeTracing(ClassLoader classLoader, String tracingClassName,
            String tracingMethodName, String tracingMethodDesc, int tracingLineNumber) throws Throwable;

    void invokeThrowTracing(ClassLoader classLoader, String tracingClassName,
            String tracingMethodName, String tracingMethodDesc, int tracingLineNumber) throws Throwable;

    void invokeAfterTracing(ClassLoader classLoader, String tracingClassName,
            String tracingMethodName, String tracingMethodDesc, int tracingLineNumber) throws Throwable;
}
```

`InvokeTraceable` 接口定义了方法调用级别的跟踪回调：

| 方法 | 说明 |
|------|------|
| `invokeBeforeTracing` | 在方法内部调用其他方法之前触发 |
| `invokeAfterTracing` | 在方法内部调用其他方法之后触发 |
| `invokeThrowTracing` | 在方法内部调用其他方法抛出异常时触发 |

### 9.3 trace 与 watch 的字节码增强差异

这是理解 trace 命令的关键：

| 特性 | watch (非 InvokeTraceable) | trace (InvokeTraceable) |
|------|---------------------------|-------------------------|
| 方法入口 | 织入 `SpyAPI.atBefore()` | 织入 `SpyAPI.atBefore()` |
| 方法返回 | 织入 `SpyAPI.atAfterReturning()` | 织入 `SpyAPI.atAfterReturning()` |
| 方法异常 | 织入 `SpyAPI.atAfterThrowing()` | 织入 `SpyAPI.atAfterThrowing()` |
| **方法内部每次调用** | **不织入** | **织入 `SpyTraceInterceptor`** |

在 `EnhancerCommand.enhance()` 中：
```java
Enhancer enhancer = new Enhancer(listener, listener instanceof InvokeTraceable, skipJDKTrace, ...);
```

`listener instanceof InvokeTraceable` 决定了 Enhancer 是否在方法体内部的每个方法调用前后插入跟踪代码。
`TraceAdviceListener` 实现了 `InvokeTraceable`，所以返回 true；
`WatchAdviceListener` 没有实现该接口，返回 false。

**类比理解**：
- watch 像一个"门卫"，只在方法入口和出口检查一次
- trace 像一个"全程跟踪器"，在方法内部的每一次方法调用前后都记录

trace 命令的字节码增强粒度更细，它不仅在目标方法的入口/出口插入代码，
还在方法体内部的每一条方法调用指令前后插入跟踪代码。这就是为什么 trace 能构建出完整的调用树。

### 9.4 invokeBeforeTracing / invokeAfterTracing / invokeThrowTracing 的调用时机

这三个方法不是由 `AdviceWeaver` 的 before/afterReturning/afterThrowing 调用的，
而是由 `SpyTraceInterceptor` 在方法体内部的方法调用指令前后调用的。

以以下代码为例：
```java
public void methodA() {
    methodB();    // 这里的 invoke 指令前后会插入跟踪代码
    methodC();    // 这里的 invoke 指令前后也会插入跟踪代码
}
```

调用时序：
```
1. before(methodA)                    → tree.begin("ClassA", "methodA")
2.   invokeBeforeTracing("ClassB", "methodB")  → tree.begin("ClassB", "methodB")
3.   methodB() 实际执行
4.   invokeAfterTracing("ClassB", "methodB")   → tree.end()
5.   invokeBeforeTracing("ClassC", "methodC")  → tree.begin("ClassC", "methodC")
6.   methodC() 实际执行
7.   invokeAfterTracing("ClassC", "methodC")   → tree.end()
8. afterReturning(methodA)            → tree.end(), finishing()
```

注意区别：
- `before/afterReturning/afterThrowing` 是方法级别的通知，由 `SpyInterceptor1/2/3` 触发
- `invokeBeforeTracing/invokeAfterTracing/invokeThrowTracing` 是方法调用级别的跟踪，由 `SpyTraceInterceptor1/2/3` 触发

### 9.5 skipJDKTrace 的作用

在 `EnhancerCommand.enhance()` 中：
```java
boolean skipJDKTrace = false;
if(listener instanceof AbstractTraceAdviceListener) {
    skipJDKTrace = ((AbstractTraceAdviceListener) listener).getCommand().isSkipJDKTrace();
}
```

如果 `skipJDKTrace` 为 true，Enhancer 会使用 `SpyTraceExcludeJDKInterceptor` 而不是 `SpyTraceInterceptor`。
前者会跳过 `java.*`、`javax.*`、`sun.*` 等 JDK 包的方法调用，不对其进行跟踪。

**为什么要跳过 JDK 方法？**
1. JDK 方法调用极其频繁，全部跟踪会导致性能严重下降
2. JDK 方法通常不是用户关注的重点
3. JDK 方法的跟踪输出会淹没用户真正关心的业务调用

---

## 第10阶段：TraceEntity —— 调用树节点

### 10.1 TraceEntity 类 —— 线程本地的调用树容器

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TraceEntity.java`

```java
public class TraceEntity {

    protected TraceTree tree;
    protected int deep;

    public TraceEntity(ClassLoader loader) {
        this.tree = createTraceTree(loader);
        this.deep = 0;
    }

    private TraceTree createTraceTree(ClassLoader loader) {
        return new TraceTree(ThreadUtil.getThreadNode(loader, Thread.currentThread()));
    }

    public TraceModel getModel() {
        tree.trim();
        return new TraceModel(tree.getRoot(), tree.getNodeCount());
    }
}
```

`TraceEntity` 是一个简单的容器，包含两个核心字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `tree` | TraceTree | 调用树 |
| `deep` | int | 当前调用深度 |

构造函数创建一棵新的 `TraceTree`，根节点是一个 `ThreadNode`（包含线程信息）。

`getModel()` 方法在输出前调用 `tree.trim()` 修整树结构（标准化类名），然后构造 `TraceModel` 返回。

### 10.2 TraceTree 类 —— 调用树数据结构

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/model/TraceTree.java`

```java
public class TraceTree {
    private TraceNode root;
    private TraceNode current;
    private int nodeCount = 0;

    public TraceTree(ThreadNode root) {
        this.root = root;
        this.current = root;
    }
```

`TraceTree` 维护三个关键字段：
- `root`：树的根节点（ThreadNode）
- `current`：当前活跃节点（类似指针，指向正在执行的节点）
- `nodeCount`：节点总数

### 10.3 TraceTree.begin() —— 开始一个新的方法调用节点

```java
public void begin(String className, String methodName, int lineNumber, boolean isInvoking) {
    TraceNode child = findChild(current, className, methodName, lineNumber);
    if (child == null) {
        child = new MethodNode(className, methodName, lineNumber, isInvoking);
        current.addChild(child);
    }
    child.begin();
    current = child;
    nodeCount += 1;
}
```

`begin()` 方法的逻辑：

1. **查找已存在的子节点**：`findChild()` 在当前节点的子节点中查找是否已有相同的调用
2. **如果不存在，创建新节点**：创建 `MethodNode` 并添加为当前节点的子节点
3. **开始计时**：`child.begin()` 记录开始时间戳
4. **移动当前指针**：`current = child`，将当前指针移动到新节点
5. **计数**：`nodeCount++`

**为什么要 findChild？** 如果同一个方法在循环中被调用多次，不需要每次都创建新节点。
通过 findChild 找到已存在的节点，复用它并累加统计信息（times、minCost、maxCost、totalCost）。

### 10.4 TraceTree.end() —— 结束当前方法调用节点

```java
public void end() {
    current.end();
    if (current.parent() != null) {
        current = current.parent();
    }
}

public void end(Throwable throwable, int lineNumber) {
    ThrowNode throwNode = new ThrowNode();
    throwNode.setException(throwable.getClass().getName());
    throwNode.setMessage(throwable.getMessage());
    throwNode.setLineNumber(lineNumber);
    current.addChild(throwNode);
    this.end(true);
}

public void end(boolean isThrow) {
    if (isThrow) {
        current.setMark("throws Exception");
        if (current instanceof MethodNode) {
            MethodNode methodNode = (MethodNode) current;
            methodNode.setThrow(true);
        }
    }
    this.end();
}
```

`end()` 方法的三个版本：
- `end()`：正常结束，记录结束时间戳，将当前指针移回父节点
- `end(throwable, lineNumber)`：异常结束，创建 ThrowNode 子节点，然后调用 `end(true)`
- `end(boolean isThrow)`：标记当前节点为抛出异常，然后调用 `end()`

### 10.5 TraceTree.findChild() —— 查找匹配的子节点

```java
private TraceNode findChild(TraceNode node, String className, String methodName, int lineNumber) {
    List<TraceNode> childList = node.getChildren();
    if (childList != null) {
        for (int i = 0; i < childList.size(); i++) {
            TraceNode child = childList.get(i);
            if (matchNode(child, className, methodName, lineNumber)) {
                return child;
            }
        }
    }
    return null;
}

private boolean matchNode(TraceNode node, String className, String methodName, int lineNumber) {
    if (node instanceof MethodNode) {
        MethodNode methodNode = (MethodNode) node;
        if (lineNumber != methodNode.getLineNumber()) return false;
        if (className != null ? !className.equals(methodNode.getClassName()) : methodNode.getClassName() != null) return false;
        return methodName != null ? methodName.equals(methodNode.getMethodName()) : methodNode.getMethodName() == null;
    }
    return false;
}
```

`matchNode` 通过三个条件判断节点是否匹配：
1. 行号相同
2. 类名相同
3. 方法名相同

只有三者全部匹配才认为是同一个调用。这确保了同一行代码对不同方法的调用不会被合并。

### 10.6 TraceTree.trim() —— 树修整

```java
public void trim() {
    this.normalizeClassName(root);
}

private void normalizeClassName(TraceNode node) {
    if (node instanceof MethodNode) {
        MethodNode methodNode = (MethodNode) node;
        String nodeClassName = methodNode.getClassName();
        String normalizeClassName = StringUtils.normalizeClassName(nodeClassName);
        methodNode.setClassName(normalizeClassName);
    }
    List<TraceNode> children = node.getChildren();
    if (children != null) {
        for (int i = 0; i < children.size(); i++) {
            TraceNode child = children.get(i);
            normalizeClassName(child);
        }
    }
}
```

`trim()` 递归遍历整棵树，将所有节点的类名标准化（`/` 替换为 `.`）。
这样做是为了"延迟标准化"——在构建树的过程中使用 JVM 内部类名（`/` 分隔），
在输出前统一转换，减少重复操作。

### 10.7 TraceNode —— 调用树节点基类

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/model/TraceNode.java`

```java
public abstract class TraceNode {

    protected TraceNode parent;
    protected List<TraceNode> children;
    private String type;
    private String mark;
    private int marks = 0;

    public TraceNode(String type) {
        this.type = type;
    }

    public void addChild(TraceNode child) {
        if (children == null) {
            children = new ArrayList<TraceNode>();
        }
        this.children.add(child);
        child.setParent(this);
    }
```

`TraceNode` 是抽象基类，定义了树节点的基本结构：
- `parent`：父节点引用
- `children`：子节点列表（懒初始化）
- `type`：节点类型（"method"、"throw" 等）
- `mark`：备注标记（如 "throws Exception"）

### 10.8 MethodNode —— 方法调用节点

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/model/MethodNode.java`

```java
public class MethodNode extends TraceNode {

    private String className;
    private String methodName;
    private int lineNumber;
    private Boolean isThrow;
    private String throwExp;
    private boolean isInvoking;

    private long beginTimestamp;
    private long endTimestamp;

    // 合并统计相同调用
    private long minCost = Long.MAX_VALUE;
    private long maxCost = Long.MIN_VALUE;
    private long totalCost = 0;
    private long times = 0;
```

`MethodNode` 是 trace 输出中最常见的节点类型，包含丰富的信息：

| 字段 | 说明 |
|------|------|
| `className` | 类名 |
| `methodName` | 方法名 |
| `lineNumber` | 调用行号 |
| `isThrow` | 是否抛出异常 |
| `isInvoking` | 是否为方法调用点（true）还是方法入口（false） |
| `beginTimestamp` | 开始时间戳 |
| `endTimestamp` | 结束时间戳 |
| `minCost` | 最小耗时（多次调用合并统计） |
| `maxCost` | 最大耗时 |
| `totalCost` | 总耗时 |
| `times` | 调用次数 |

### 10.9 MethodNode.begin() 和 end() —— 计时与统计

```java
public void begin() {
    beginTimestamp = System.nanoTime();
}

public void end() {
    endTimestamp = System.nanoTime();

    long cost = getCost();
    if (cost < minCost) {
        minCost = cost;
    }
    if (cost > maxCost) {
        maxCost = cost;
    }
    times++;
    totalCost += cost;
}

public long getCost() {
    return endTimestamp - beginTimestamp;
}
```

`end()` 方法不仅记录结束时间，还更新统计信息：
- 更新最小/最大耗时
- 调用次数 +1
- 累加总耗时

这使得 trace 可以对循环中的重复调用进行合并统计。输出时，用户可以看到同一调用的次数、最小/最大/平均耗时。

### 10.10 调用树构建完整示例

以以下代码为例：
```java
// ClassA.methodA()
public void methodA() {
    methodB();  // line 10
    methodB();  // line 11 (循环中重复调用)
}
```

调用树构建过程：
```
ThreadNode (root)
└── MethodNode(ClassA, methodA, -1)
    ├── MethodNode(ClassA, methodB, 10)  times=1, cost=5ms
    │   └── MethodNode(ClassB, methodC, 5)  times=1, cost=2ms
    └── MethodNode(ClassA, methodB, 11)  times=1, cost=3ms
        └── MethodNode(ClassB, methodC, 5)  times=1, cost=1ms
```

如果 methodB 在循环中调用 100 次（同一行），则：
```
ThreadNode (root)
└── MethodNode(ClassA, methodA, -1)
    └── MethodNode(ClassA, methodB, 10)  times=100, minCost=1ms, maxCost=10ms, totalCost=350ms
        └── MethodNode(ClassB, methodC, 5)  times=100, minCost=0.5ms, maxCost=5ms, totalCost=200ms
```

节点合并后，trace 输出会显示调用次数和耗时统计，而不是重复输出 100 次。

---

## 第11阶段：StackCommand 与 StackAdviceListener

### 11.1 StackCommand 类 —— stack 命令定义

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/StackCommand.java`

```java
@Name("stack")
@Summary("Display the stack trace for the specified class and method")
@Description(Constants.EXPRESS_DESCRIPTION + Constants.EXAMPLE +
        "  stack org.apache.commons.lang.StringUtils isBlank\n" +
        "  stack *StringUtils isBlank\n" +
        "  stack *StringUtils isBlank params[0].length==1\n" +
        "  stack *StringUtils isBlank '#cost>100'\n" +
        "  stack -E org\\\\.apache\\\\.commons\\\\.lang\\\\.StringUtils isBlank\n" +
        Constants.WIKI + Constants.WIKI_HOME + "stack")
public class StackCommand extends EnhancerCommand {
```

`stack` 命令的参数比 `watch` 简单很多：

```java
private String classPattern;
private String methodPattern;
private String conditionExpress;
private boolean isRegEx = false;
private int numberOfLimit = 100;
```

注意 `stack` 命令没有 `express`（观察表达式）参数，因为它的输出固定是调用栈，不需要通过表达式来决定观察什么。

### 11.2 StackCommand 的匹配器和监听器

```java
@Override
protected Matcher getClassNameMatcher() {
    if (classNameMatcher == null) {
        classNameMatcher = SearchUtils.classNameMatcher(getClassPattern(), isRegEx());
    }
    return classNameMatcher;
}

@Override
protected Matcher getMethodNameMatcher() {
    if (methodNameMatcher == null) {
        methodNameMatcher = SearchUtils.classNameMatcher(getMethodPattern(), isRegEx());
    }
    return methodNameMatcher;
}

@Override
protected AdviceListener getAdviceListener(CommandProcess process) {
    return new StackAdviceListener(this, process, GlobalOptions.verbose || this.verbose);
}
```

`StackCommand` 的匹配器构建与 `WatchCommand` 完全一致。区别在于 `getAdviceListener()` 返回 `StackAdviceListener`。

### 11.3 StackAdviceListener —— stack 回调实现

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/StackAdviceListener.java`

```java
public class StackAdviceListener extends AdviceListenerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(StackAdviceListener.class);

    private final ThreadLocalWatch threadLocalWatch = new ThreadLocalWatch();
    private StackCommand command;
    private CommandProcess process;

    public StackAdviceListener(StackCommand command, CommandProcess process, boolean verbose) {
        this.command = command;
        this.process = process;
        super.setVerbose(verbose);
    }
```

### 11.4 StackAdviceListener.before() —— 只在方法入口启动计时

```java
@Override
public void before(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args)
        throws Throwable {
    // 开始计算本次方法调用耗时
    threadLocalWatch.start();
}
```

`stack` 命令的 `before()` 方法只做一件事：启动计时器。注意它没有像 `WatchAdviceListener` 那样在 before 时调用任何观察方法，
因为 stack 只关心方法被调用时的栈帧信息，不需要在方法入口输出数据。

### 11.5 StackAdviceListener.afterReturning() 和 afterThrowing()

```java
@Override
public void afterThrowing(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                          Throwable throwable) throws Throwable {
    Advice advice = Advice.newForAfterThrowing(loader, clazz, method, target, args, throwable);
    finishing(advice);
}

@Override
public void afterReturning(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                           Object returnObject) throws Throwable {
    Advice advice = Advice.newForAfterReturning(loader, clazz, method, target, args, returnObject);
    finishing(advice);
}
```

无论是正常返回还是抛出异常，都会调用 `finishing()` 方法。这与 `WatchAdviceListener` 不同——
`WatchAdviceListener` 在 afterReturning/afterThrowing 中会根据 `-s`/`-e` 选项决定是否调用 watching，
而 `StackAdviceListener` 总是调用 finishing。

### 11.6 StackAdviceListener.finishing() —— 核心逻辑

```java
private void finishing(Advice advice) {
    try {
        double cost = threadLocalWatch.costInMillis();
        boolean conditionResult = isConditionMet(command.getConditionExpress(), advice, cost);
        if (this.isVerbose()) {
            process.write("Condition express: " + command.getConditionExpress()
                + " , result: " + conditionResult + "\n");
        }
        if (conditionResult) {
            StackModel stackModel = ThreadUtil.getThreadStackModel(advice.getLoader(), Thread.currentThread());
            stackModel.setTs(LocalDateTime.now());
            process.appendResult(stackModel);
            process.times().incrementAndGet();
            if (isLimitExceeded(command.getNumberOfLimit(), process.times().get())) {
                abortProcess(process, command.getNumberOfLimit());
            }
        }
    } catch (Throwable e) {
        logger.warn("stack failed.", e);
        process.end(-1, "stack failed, condition is: " + command.getConditionExpress()
                      + ", " + e.getMessage() + ", visit " + LogUtil.loggingFile()
                      + " for more details.");
    }
}
```

`finishing()` 方法的流程与 `WatchAdviceListener.watching()` 类似，但有关键区别：

1. **计算耗时**：`threadLocalWatch.costInMillis()`
2. **条件判断**：`isConditionMet()` 检查条件表达式
3. **获取线程栈**：`ThreadUtil.getThreadStackModel(advice.getLoader(), Thread.currentThread())`
   - 这是 stack 命令的核心——获取当前线程的完整调用栈
4. **设置时间戳**：`stackModel.setTs(LocalDateTime.now())`
5. **输出结果**：`process.appendResult(stackModel)`
6. **次数限制**：与 watch 相同

### 11.7 StackModel —— stack 结果模型

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/model/StackModel.java`

```java
public class StackModel extends ResultModel {

    private LocalDateTime ts;
    private double cost;
    private String traceId;
    private String rpcId;
    private String threadName;
    private String threadId;
    private boolean daemon;
    private int priority;
    private String classloader;
    private StackTraceElement[] stackTrace;

    @Override
    public String getType() {
        return "stack";
    }
```

`StackModel` 包含丰富的线程信息：

| 字段 | 说明 |
|------|------|
| `ts` | 时间戳 |
| `cost` | 方法耗时 |
| `traceId` | 链路追踪 ID |
| `rpcId` | RPC ID |
| `threadName` | 线程名 |
| `threadId` | 线程 ID |
| `daemon` | 是否为守护线程 |
| `priority` | 线程优先级 |
| `classloader` | 当前线程的 ClassLoader |
| `stackTrace` | 完整的调用栈帧数组 |

### 11.8 stack 命令的特点

与 watch/trace 相比，stack 命令有以下特点：

1. **只关注方法入口/出口**：不需要 InvokeTraceable 接口，不在方法内部插入跟踪代码
2. **输出完整调用栈**：通过 `Thread.currentThread().getStackTrace()` 获取完整的调用栈
3. **不输出方法参数/返回值**：没有 OGNL 表达式求值
4. **适合定位"谁调用了这个方法"**：当你想知道某个方法被谁调用时，stack 是最佳选择

---

## 第12阶段：MonitorCommand —— 方法执行统计

### 12.1 MonitorCommand 类 —— monitor 命令定义

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/MonitorCommand.java`

```java
@Name("monitor")
@Summary("Monitor method execution statistics, e.g. total/success/failure count, "
    + "average rt, fail rate, etc.")
public class MonitorCommand extends EnhancerCommand {

    private String classPattern;
    private String methodPattern;
    private String conditionExpress;
    private int cycle = 60;
    private boolean isRegEx = false;
    private int numberOfLimit = 100;
    private boolean isBefore = false;
```

`monitor` 命令的核心参数是 `cycle`（统计周期，默认 60 秒）和 `numberOfLimit`（输出次数上限）。

### 12.2 monitor 的特有选项

```java
@Option(shortName = "c", longName = "cycle")
@Description("The monitor interval (in seconds), 60 seconds by default")
public void setCycle(int cycle) {
    this.cycle = cycle;
}

@Option(shortName = "b", longName = "before", flag = true)
@Description("Evaluate the condition-express before method invoke")
public void setBefore(boolean before) {
    this.isBefore = before;
}
```

- `-c` 指定统计周期（秒），每隔这么长时间输出一次统计结果
- `-b` 指定在方法调用前评估条件表达式

### 12.3 MonitorCommand 的监听器创建

```java
@Override
protected AdviceListener getAdviceListener(CommandProcess process) {
    final AdviceListener listener = new MonitorAdviceListener(this, process,
            GlobalOptions.verbose || this.verbose);
    process.suspendHandler(new Handler<Void>() {
        @Override
        public void handle(Void event) {
            listener.destroy();
        }
    });
    process.resumeHandler(new Handler<Void>() {
        @Override
        public void handle(Void event) {
            listener.create();
        }
    });
    return listener;
}
```

`MonitorCommand` 在创建监听器时额外注册了 suspend/resume 处理器：
- **suspend 时**：调用 `listener.destroy()` 停止定时统计任务
- **resume 时**：调用 `listener.create()` 重新启动定时统计任务

这确保了当命令被挂起（如用户按 Ctrl+Z）时，统计任务也会暂停，恢复时重新开始。

### 12.4 monitor 的统计机制

`MonitorAdviceListener`（未在本文中展开源码）通过定时任务按周期统计：
- 方法调用总次数
- 成功次数
- 失败次数
- 平均耗时
- 失败率

每个周期结束时输出一次统计表格，然后重置计数器开始下一个周期。

---

## 第13阶段：TimeTunnelCommand —— 时间隧道

### 13.1 TimeTunnelCommand 类 —— tt 命令定义

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TimeTunnelCommand.java`

```java
@Name("tt")
@Summary("Time Tunnel")
public class TimeTunnelCommand extends EnhancerCommand {

    // 时间隧道(时间碎片的集合)
    private static final Map<Integer, TimeFragment> timeFragmentMap = new LinkedHashMap<Integer, TimeFragment>();
    // 时间碎片序列生成器
    private static final AtomicInteger sequence = new AtomicInteger(1000);
```

`tt` 命令使用静态的 `timeFragmentMap` 存储所有记录的方法调用片段，索引从 1000 开始递增。

### 13.2 tt 命令的多种操作模式

```java
@Override
public void process(final CommandProcess process) {
    checkArguments();

    process.interruptHandler(new CommandInterruptHandler(process));
    process.stdinHandler(new QExitHandler(process));

    if (isTimeTunnel) {
        enhance(process);
    } else if (isPlay) {
        processPlay(process);
    } else if (isList) {
        processList(process);
    } else if (isDeleteAll) {
        processDeleteAll(process);
    } else if (isDelete) {
        processDelete(process);
    } else if (hasSearchExpress()) {
        processSearch(process);
    } else if (index != null) {
        if (hasWatchExpress()) {
            processWatch(process);
        } else {
            processShow(process);
        }
    }
}
```

`tt` 命令支持多种操作模式：

| 模式 | 选项 | 说明 |
|------|------|------|
| 记录 | `-t` | 记录方法调用（需要字节码增强） |
| 回放 | `-p -i INDEX` | 重新执行记录的方法调用 |
| 列表 | `-l` | 列出所有记录 |
| 删除全部 | `--delete-all` | 删除所有记录 |
| 删除指定 | `-d -i INDEX` | 删除指定记录 |
| 搜索 | `-s EXPRESS` | 按表达式搜索记录 |
| 查看 | `-i INDEX` | 查看指定记录详情 |
| 观察 | `-i INDEX -w EXPRESS` | 对指定记录执行 OGNL 表达式 |

### 13.3 tt 的记录模式

当使用 `-t` 选项时，`tt` 命令走标准的 `enhance()` 流程，通过 `TimeTunnelAdviceListener` 记录方法调用的完整上下文。

### 13.4 tt 的回放模式

```java
private void processPlay(CommandProcess process) {
    TimeFragment tf = timeFragmentMap.get(index);
    if (null == tf) {
        process.end(1, format("Time fragment[%d] does not exist.", index));
        return;
    }
    Advice advice = tf.getAdvice();
    ArthasMethod method = advice.getMethod();
    boolean accessible = advice.getMethod().isAccessible();
    try {
        if (!accessible) {
            method.setAccessible(true);
        }
        for (int i = 0; i < getReplayTimes(); i++) {
            if (i > 0) {
                Thread.sleep(getReplayInterval());
                if (!process.isRunning()) {
                    return;
                }
            }
            long beginTime = System.nanoTime();

            TimeFragmentVO replayResult = createTimeFragmentVO(index, tf, expand);
            replayResult.setTimestamp(LocalDateTime.now())
                    .setCost(0)
                    .setReturn(false)
                    .setReturnObj(null)
                    .setThrow(false)
                    .setThrowExp(null);

            try {
                Object returnObj = method.invoke(advice.getTarget(), advice.getParams());
                double cost = (System.nanoTime() - beginTime) / 1000000.0;
                replayResult.setCost(cost)
                        .setReturn(true)
                        .setReturnObj(new ObjectVO(returnObj, expand));
            } catch (Throwable t) {
                double cost = (System.nanoTime() - beginTime) / 1000000.0;
                replayResult.setCost(cost)
                        .setThrow(true)
                        .setThrowExp(new ObjectVO(t, expand));
            }

            TimeTunnelModel timeTunnelModel = new TimeTunnelModel()
                    .setReplayResult(replayResult)
                    .setReplayNo(i + 1)
                    .setExpand(expand)
                    .setSizeLimit(sizeLimit);
            process.appendResult(timeTunnelModel);
        }
        process.end();
    } catch (Throwable t) {
        logger.warn("tt replay failed.", t);
        process.end(-1, "tt replay failed");
    } finally {
        method.setAccessible(accessible);
    }
}
```

回放模式的流程：
1. 根据索引获取之前记录的 `TimeFragment`
2. 从 TimeFragment 中提取 Advice（包含 target、method、params）
3. 支持多次回放（`replayTimes`），每次之间有间隔（`replayInterval`）
4. 使用反射调用原始方法：`method.invoke(advice.getTarget(), advice.getParams())`
5. 记录回放结果（返回值或异常）

### 13.5 tt 的观察模式

```java
private void processWatch(CommandProcess process) {
    RowAffect affect = new RowAffect();
    try {
        final TimeFragment tf = timeFragmentMap.get(index);
        if (null == tf) {
            process.end(1, format("Time fragment[%d] does not exist.", index));
            return;
        }

        Advice advice = tf.getAdvice();

        Object value = ExpressFactory.unpooledExpress(advice.getLoader())
                .bind(advice).get(watchExpress);
        TimeTunnelModel timeTunnelModel = new TimeTunnelModel()
                .setWatchValue(new ObjectVO(value, expand))
                .setExpand(expand)
                .setSizeLimit(sizeLimit);
        process.appendResult(timeTunnelModel);

        affect.rCnt(1);
        process.appendResult(new RowAffectModel(affect));
        process.end();
    } catch (ExpressException e) {
        logger.warn("tt failed.", e);
        process.end(1, e.getMessage() + ", visit " + LogUtil.loggingFile() + " for more detail");
    }
}
```

观察模式使用 `ExpressFactory.unpooledExpress()` 对记录的 Advice 执行 OGNL 表达式求值。
注意这里使用的是 `unpooledExpress` 而不是 `threadLocalExpress`，
因为需要使用原始方法调用时的 ClassLoader 来解析类名。

---

## 第14阶段：三个命令对比分析

### 14.1 watch/trace/stack 全面对比

| 维度 | watch | trace | stack |
|------|-------|-------|-------|
| **监控粒度** | 单个方法 | 方法调用链路 | 方法调用栈 |
| **增强类型** | 方法入口/出口 | 方法入口/出口 + 方法内部调用 | 方法入口/出口 |
| **InvokeTraceable** | 否 | 是 | 否 |
| **输出形式** | 方法参数/返回值/异常 | 调用树（缩进格式） | 完整调用栈帧 |
| **OGNL 表达式** | 支持 | 不支持（仅条件表达式） | 不支持（仅条件表达式） |
| **观察点** | -b/-f/-e/-s | 仅结束时 | 仅结束时 |
| **展开深度** | -x | N/A | N/A |
| **条件过滤** | 支持 | 支持 | 支持 |
| **次数限制** | -n (默认100) | -n (默认100) | -n (默认100) |
| **ThreadLocal** | ThreadLocalWatch | ThreadLocalWatch + TraceEntity | ThreadLocalWatch |
| **结果模型** | WatchModel | TraceModel | StackModel |
| **适用场景** | 观察方法输入输出 | 分析方法调用链路耗时 | 定位方法调用来源 |
| **性能开销** | 低 | 高（方法内部每次调用都跟踪） | 低 |

### 14.2 监控粒度对比

```
watch  →  方法A 入口 → ... → 方法A 出口
                 ↑ 只在入口和出口观察

trace  →  方法A 入口 → 方法B 入口 → 方法C 入口 → 方法C 出口 → 方法B 出口 → 方法A 出口
                 ↑                ↑                ↑              ↑              ↑
                 每一层方法调用都跟踪，构建完整的调用树

stack  →  方法A 入口 → [抓取当前线程完整栈帧] → 方法A 出口
                 ↑ 在方法入口/出口获取调用栈
```

### 14.3 输出示例对比

**watch 输出示例**：
```
ts=2024-01-15 10:30:45; [cost=120.534ms] result=@ArrayList[
    @Object[][isEmpty=false; size=2],
    @MathGame[demo.MathGame@45ee12],
    @Integer[42],
]
```

**trace 输出示例**：
```
`---[120.534ms] demo.MathGame:run()
    +---[0.012ms] demo.MathGame:print()
    +---[100.234ms] demo.MathGame:calculate()
    |   `---[99.987ms] java.util.Random:nextInt()
    `---[0.003ms] demo.MathGame:log()
```

**stack 输出示例**：
```
ts=2024-01-15 10:30:45; [cost=120.534ms] thread_name=main
    demo.MathGame.run(MathGame.java:46)
    demo.MathGame.main(MathGame.java:16)
```

### 14.4 性能开销对比

| 因素 | watch | trace | stack |
|------|-------|-------|-------|
| 字节码增强量 | 方法入口/出口各一段 | 方法入口/出口 + 每个invoke指令前后 | 方法入口/出口各一段 |
| 运行时开销 | OGNL求值 + 对象序列化 | 调用树构建 + ThreadLocal操作 | 获取栈帧 |
| 内存消耗 | 较低 | 较高（调用树存储） | 较低 |
| 对热点方法影响 | 小 | 大（每次调用都记录） | 小 |

---

## 第15阶段：关键设计问题深入分析

### 15.1 watch 为什么用 AdviceListener 而不是 TraceListener？

Arthas 的监听器体系中没有 TraceListener 这个概念。watch 使用 `AdviceListener`（具体是 `AdviceListenerAdapter` 的子类），
这是因为：

1. **统一的监听器接口**：所有监控命令（watch/trace/stack/monitor/tt）都使用 `AdviceListener` 接口，
   保证了增强流程的一致性。`EnhancerCommand.enhance()` 只需要调用 `getAdviceListener()` 获取监听器，
   不需要关心具体是什么类型的命令。

2. **回调时机的统一性**：`AdviceListener` 定义了 `before`、`afterReturning`、`afterThrowing` 三个回调方法，
   这三个方法覆盖了方法调用的所有关键时间点。watch 只需要在对应的时间点观察数据即可。

3. **差异通过实现体现**：watch、trace、stack 的差异不在接口层面，而在实现层面。
   - watch 在 `before/afterReturning/afterThrowing` 中调用 `watching()` 进行 OGNL 求值
   - trace 在 `before/afterReturning/afterThrowing` 中更新调用树
   - stack 在 `afterReturning/afterThrowing` 中获取线程栈

### 15.2 trace 为什么需要 InvokeTraceable 接口？

`InvokeTraceable` 接口定义了方法调用级别的跟踪回调，这是 trace 命令独有的需求：

1. **watch/stack 只关心方法级别**：它们只需要知道方法何时被调用（before）、何时返回（afterReturning）、何时抛异常（afterThrowing）。
   不需要知道方法内部调用了哪些其他方法。

2. **trace 需要构建调用树**：trace 的核心价值在于展示方法内部的调用链路。要做到这一点，
   必须在方法体内部的每一条 invoke 指令前后插入跟踪代码。`InvokeTraceable` 接口就是这些跟踪代码的回调入口。

3. **Enhancer 的判断依据**：`Enhancer` 在创建时通过 `listener instanceof InvokeTraceable` 判断是否需要插入方法调用级别的跟踪代码。
   如果返回 true，Enhancer 会使用 `SpyTraceInterceptor`（而不是 `SpyInterceptor`）来织入字节码。

4. **性能考量**：方法调用级别的跟踪代码会增加显著的性能开销。通过 `InvokeTraceable` 接口，
   Arthas 确保只有 trace 命令才承受这个开销，watch 和 stack 不会。

### 15.3 ThreadLocal 在调用树中的作用——为什么不用全局变量？

`AbstractTraceAdviceListener` 使用 `ThreadLocal<TraceEntity>` 来存储每个线程的调用树。
不能用全局变量的原因：

1. **多线程并发**：被 trace 的方法可能被多个线程同时调用。每个线程的调用栈是独立的，
   如果用全局变量存储调用树，不同线程的调用会混在一起，导致调用树结构混乱。

2. **线程隔离**：ThreadLocal 确保每个线程有自己独立的 TraceEntity 和调用树。
   线程 A 的调用树不会影响线程 B 的调用树。

3. **深度计数的正确性**：`deep` 字段也存储在 ThreadLocal 中的 TraceEntity 里。
   如果用全局变量，线程 A 的方法调用会递增全局的 deep，导致线程 B 的 deep 判断错误。

4. **性能优势**：ThreadLocal 的访问是无锁的，每个线程直接访问自己的副本，
   不需要同步开销。这对于高频调用的方法尤为重要。

**类比理解**：想象一栋办公楼，每个员工（线程）都有自己的笔记本（ThreadLocal）记录今天的工作日志。
如果用公共黑板（全局变量），大家同时写会互相干扰。每人用自己的笔记本就不会冲突。

### 15.4 深度计数 deep 为什么用 ThreadLocal？

`deep` 字段存储在 `TraceEntity` 中，而 `TraceEntity` 存储在 `ThreadLocal` 中。

原因与上一问题相同：每个线程的调用深度是独立的。线程 A 可能在调用链路的第 3 层，
而线程 B 可能在第 1 层。如果用全局的 deep 计数器，两个线程的计数会互相干扰。

deep 的工作原理：
```
线程A: before(A) deep=1 → before(B) deep=2 → after(B) deep=1 → after(A) deep=0 → 输出
线程B: before(C) deep=1 → after(C) deep=0 → 输出
```

两个线程的 deep 计数完全独立，互不影响。

### 15.5 条件表达式如何过滤不需要的结果？

条件表达式的工作流程：

```
1. 方法调用触发回调 (before/afterReturning/afterThrowing)
2. 创建 Advice 对象（包含方法上下文信息）
3. 计算耗时 cost
4. 调用 isConditionMet(conditionExpress, advice, cost)
   ├─ 如果 conditionExpress 为空 → 返回 true（不过滤）
   └─ 如果 conditionExpress 不为空
      ├─ ExpressFactory.threadLocalExpress(advice) 获取 Express
      ├─ bind("#cost", cost) 绑定耗时变量
      └─ is(conditionExpress) 对表达式求值
         ├─ true → 继续处理（输出结果）
         └─ false → 跳过（不输出）
```

条件表达式中可以使用的变量：
- `params[0]`：第一个参数
- `params[0].length==1`：参数属性判断
- `#cost>100`：耗时判断
- `throwExp != null`：异常判断
- `returnObj == null`：返回值判断
- `target.field == 'xxx'`：目标对象字段判断

### 15.6 abortProcess 如何保证只中断一次？

在单线程场景下，`abortProcess` 的多次调用不是问题，因为 `process.end()` 内部有状态检查。
但在多线程场景下，如果多个线程同时达到次数上限：

**WatchAdviceListener 的情况**：
`WatchAdviceListener` 继承自 `AdviceListenerAdapter`，使用的是 `AdviceListenerAdapter.abortProcess()`，
没有额外的线程安全保护。这是因为 `process.end()` 内部通常已经有线程安全的实现。

**AbstractTraceAdviceListener 的情况**：
`AbstractTraceAdviceListener` 覆盖了 `abortProcess()`，使用 `AtomicBoolean.compareAndSet` 保证只执行一次：

```java
private final AtomicBoolean processAborted = new AtomicBoolean(false);

@Override
protected void abortProcess(CommandProcess process, int limit) {
    if (processAborted.compareAndSet(false, true)) {
        super.abortProcess(process, limit);
    }
}
```

`compareAndSet(false, true)` 是一个原子操作：
- 如果当前值为 false，设为 true，返回 true → 执行 abortProcess
- 如果当前值为 true，返回 false → 跳过

这确保了即使 100 个线程同时达到限制，也只有第一个线程能执行 `abortProcess`，其余 99 个线程会被跳过。

**为什么 trace 需要而 watch 不需要？**
trace 的调用树构建涉及更多的共享状态（如 nodeCount），并发冲突的概率更高。
watch 的输出是独立的，每次方法调用的结果互不影响，所以对 abortProcess 的线程安全要求相对较低。
但从设计角度看，`WatchAdviceListener` 没有添加 `AtomicBoolean` 保护是一个潜在的问题。

### 15.7 多个 watch 命令同时监控同一个类会怎样？

当多个 watch 命令同时监控同一个类时：

1. **会话锁的影响**：`EnhancerCommand.enhance()` 使用 `session.tryLock()` 保证同一时间只有一个增强操作在执行。
   第二个 watch 命令必须等第一个增强完成后才能开始。

2. **字节码增强的叠加**：每次增强都会通过 `Instrumentation.retransformClasses()` 重新变换类文件。
   如果类已经被第一个 watch 增强过，第二个 watch 的增强会在已增强的字节码基础上再次增强。
   但 Arthas 通过 SpyAPI 的 `listenerId` 机制避免了重复增强——每个监听器有唯一的 ID，
   增强代码会根据 listenerId 路由到正确的监听器。

3. **listenerId 机制**：`AdviceListenerAdapter` 中的 `ID_GENERATOR` 为每个监听器分配唯一 ID。
   增强后的字节码中会嵌入 listenerId，当方法被调用时，SpyAPI 根据 listenerId 找到对应的监听器进行回调。

4. **独立的结果输出**：每个 watch 命令有自己的 `CommandProcess`，结果分别输出到各自的终端会话。

5. **性能叠加**：多个 watch 同时运行会增加性能开销，因为每次方法调用都会触发多个监听器的回调。

6. **命令退出时的清理**：当某个 watch 命令退出时（通过 `-n` 限制或用户手动退出），
   会移除对应的监听器，并 retransform 类移除该监听器对应的增强代码。
   其他 watch 命令的增强代码不受影响。

---

## 第16阶段：ThreadLocalWatch 深度分析

### 16.1 ThreadLocalWatch 类 —— 线程本地计时器

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/util/ThreadLocalWatch.java`

```java
public class ThreadLocalWatch {

    /**
     * 用 long[] 做一个固定大小的 ring stack，避免把 ArthasClassLoader 加载的对象塞到
     * 业务线程的 ThreadLocalMap 里，从而在 stop/detach 后导致 ArthasClassLoader 无法被 GC 回收。
     */
    private static final int DEFAULT_STACK_SIZE = 1024 * 4;
    private final ThreadLocal<long[]> timestampRef = ThreadLocal.withInitial(() -> new long[DEFAULT_STACK_SIZE + 1]);
```

`ThreadLocalWatch` 使用 `ThreadLocal<long[]>` 存储时间戳，这是一个非常巧妙的设计：

1. **为什么用 long[] 而不是 Long 或自定义对象？**
   `long[]` 是原生类型数组，由 bootstrap classloader 加载，不会被 ArthasClassLoader 引用。
   这样在 Arthas stop/detach 时，业务线程的 ThreadLocal 不会阻止 ArthasClassLoader 被 GC 回收。

2. **为什么用固定大小的环形栈？**
   - push/pop 不一定成对调用（如方法抛出异常时可能 pop 没被执行）
   - 如果用动态扩容的栈，极端情况下可能无限增长导致内存问题
   - 固定大小 + 环形覆写确保内存使用可控

### 16.2 环形栈的实现

```java
static void push(long[] stack, long value) {
    int cap = stack.length - 1;
    int pos = (int) stack[0];
    if (pos < cap) {
        pos++;
    } else {
        // if stack is full, reset pos
        pos = 1;
    }
    stack[pos] = value;
    stack[0] = pos;
}

static long pop(long[] stack) {
    int cap = stack.length - 1;
    int pos = (int) stack[0];
    if (pos > 0) {
        long value = stack[pos];
        stack[0] = pos - 1;
        return value;
    }

    pos = cap;
    long value = stack[pos];
    stack[0] = pos - 1;
    return value;
}
```

环形栈的约定：
- `stack[0]` 存储当前栈顶位置（0 到 cap）
- `stack[1..cap]` 存储数据
- 栈满时，pos 重置为 1，覆写最旧的数据

**push 逻辑**：
1. 读取当前位置 pos
2. 如果 pos < cap，pos++（正常递增）
3. 如果 pos >= cap，pos = 1（环形回到起点）
4. 在 pos 位置写入值
5. 更新 stack[0] 为 pos

**pop 逻辑**：
1. 读取当前位置 pos
2. 如果 pos > 0，返回 stack[pos]，pos--
3. 如果 pos == 0，跳到 cap 位置（环形），返回 stack[cap]，pos = cap-1

### 16.3 start() 和 costInMillis() 方法

```java
public long start() {
    final long timestamp = System.nanoTime();
    push(timestampRef.get(), timestamp);
    return timestamp;
}

public double costInMillis() {
    return (System.nanoTime() - pop(timestampRef.get())) / 1000000.0;
}

public double costInMillisWithoutPop() {
    long timestamp = peek(timestampRef.get());
    if (timestamp == 0) {
        return 0.0;
    }
    return (System.nanoTime() - timestamp) / 1000000.0;
}
```

- `start()`：获取当前纳秒时间戳，push 到栈中
- `costInMillis()`：pop 出之前的时间戳，计算差值，转换为毫秒
- `costInMillisWithoutPop()`：peek（不弹出）时间戳，计算当前耗时

**为什么需要 costInMillisWithoutPop？** 在某些场景下，需要在不消费时间戳的情况下查看当前耗时（如条件表达式中使用 `#cost`）。

### 16.4 ThreadLocalWatch 在 watch/trace/stack 中的使用差异

| 命令 | start() 调用点 | costInMillis() 调用点 |
|------|---------------|----------------------|
| watch | before() | watching() |
| trace | before() | finishing()（deep==0时） |
| stack | before() | finishing() |

在 trace 中，start() 在方法入口调用，costInMillis() 在最外层方法返回时（deep==0）调用，
所以 trace 的 cost 是整个调用链路的总耗时。

---

## 第17阶段：Enhancer 类与字节码增强机制

### 17.1 Enhancer 类概览

**源码位置**: `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/advisor/Enhancer.java`

```java
public class Enhancer implements ClassFileTransformer {

    private final AdviceListener listener;
    private final boolean isTracing;
    private final boolean skipJDKTrace;
    private final Matcher classNameMatcher;
    private final Matcher classNameExcludeMatcher;
    private final Matcher methodNameMatcher;
    private final boolean lazy;
    private final String hashCode;
```

`Enhancer` 实现了 `ClassFileTransformer` 接口，是 Java Agent 字节码变换的核心类。

### 17.2 Enhancer 的关键参数

| 参数 | 说明 |
|------|------|
| `listener` | 通知监听器，回调时使用 |
| `isTracing` | 是否需要方法调用级别的跟踪（trace 命令为 true） |
| `skipJDKTrace` | 是否跳过 JDK 方法的跟踪 |
| `classNameMatcher` | 类名匹配器 |
| `classNameExcludeMatcher` | 排除类名匹配器 |
| `methodNameMatcher` | 方法名匹配器 |
| `lazy` | 是否启用懒加载模式 |
| `hashCode` | 指定 ClassLoader hash |

### 17.3 Enhancer 与 SpyAPI 的协作

Enhancer 使用 ASM 字节码操作框架在目标方法中织入对 `SpyAPI` 的调用。
`SpyAPI` 是 Arthas 注入到 bootstrap classloader 的间谍 API，它是增强代码与监听器之间的桥梁。

增强后的方法调用流程：
```
目标方法被调用
    │
    ▼
SpyAPI.atEnter(clazz, methodName, methodDesc, target, args)
    │
    ▼
AdviceWeaver.before(clazz, methodName, methodDesc, target, args)
    │
    ▼
AdviceListenerAdapter.before(...)  →  子类实现的 before(...)
    │
    ▼
方法体执行
    │
    ▼
SpyAPI.atExit(clazz, methodName, methodDesc, target, args, returnObject)
    │
    ▼
AdviceWeaver.afterReturning(...)
    │
    ▼
AdviceListenerAdapter.afterReturning(...)  →  子类实现的 afterReturning(...)
```

### 17.4 isTracing 对增强策略的影响

当 `isTracing` 为 true 时，Enhancer 会使用不同的 SpyInterceptor：

| isTracing | 方法入口/出口 | 方法内部调用 |
|-----------|-------------|-------------|
| false (watch/stack) | SpyInterceptor1/2/3 | 不增强 |
| true (trace, skipJDKTrace=false) | SpyTraceInterceptor1/2/3 | SpyTraceInterceptor1/2/3 |
| true (trace, skipJDKTrace=true) | SpyTraceExcludeJDKInterceptor1/2/3 | SpyTraceExcludeJDKInterceptor1/2/3 |

- `SpyInterceptor`：只触发 before/afterReturning/afterThrowing 回调
- `SpyTraceInterceptor`：除了触发 before/afterReturning/afterThrowing 外，还触发 invokeBeforeTracing/invokeAfterTracing/invokeThrowTracing
- `SpyTraceExcludeJDKInterceptor`：同上，但跳过 JDK 方法

---

## 第18阶段：AdviceWeaver —— 通知织入器

### 18.1 AdviceWeaver 的角色

`AdviceWeaver` 是 SpyAPI 与 AdviceListener 之间的桥梁。当增强后的方法被调用时，
SpyAPI 会调用 AdviceWeaver 的静态方法，AdviceWeaver 再根据 listenerId 找到对应的 AdviceListener 进行回调。

### 18.2 AdviceWeaver 的关键方法

AdviceWeaver 维护了一个 `listenerId -> AdviceListener` 的映射：

```java
// 简化的逻辑
public static AdviceListener listener(long listenerId) {
    return listeners.get(listenerId);
}

public static void before(Class<?> clazz, String methodName, String methodDesc,
        Object target, Object[] args) {
    AdviceListener listener = listener(listenerId);
    if (listener != null) {
        listener.before(clazz, methodName, methodDesc, target, args);
    }
}
```

AdviceWeaver 的 before/afterReturning/afterThrowing 方法会：
1. 根据 listenerId 查找对应的 AdviceListener
2. 如果找到，调用对应的回调方法
3. 如果没找到（监听器已被移除），直接返回（no-op）

这种设计确保了即使增强代码仍然存在于类中，当监听器被移除后，方法调用也不会触发任何回调。

---

## 第19阶段：完整的端到端调用链路

### 19.1 watch 命令完整调用链

以 `watch demo.MathGame run '{params, returnObj}' -x 2 -n 3` 为例：

```
1. 用户输入命令
   watch demo.MathGame run '{params, returnObj}' -x 2 -n 3

2. CLI 解析 → WatchCommand 对象创建
   classPattern = "demo.MathGame"
   methodPattern = "run"
   express = "{params, returnObj}"
   expand = 2
   numberOfLimit = 3

3. WatchCommand.process()
   ├─ validateSizeLimit(null) → 通过
   └─ super.process() → EnhancerCommand.process()

4. EnhancerCommand.process()
   ├─ process.interruptHandler(CommandInterruptHandler)
   ├─ process.stdinHandler(QExitHandler)
   └─ enhance(process)

5. EnhancerCommand.enhance()
   ├─ session.tryLock() → 成功
   ├─ getAdviceListenerWithId(process)
   │   └─ getAdviceListener(process)
   │       └─ new WatchAdviceListener(this, process, false)
   ├─ new Enhancer(listener, false, false, classNameMatcher, ..., ...)
   ├─ process.register(listener, enhancer)
   ├─ enhancer.enhance(inst, 50)
   │   └─ 遍历所有已加载的类
   │       └─ 匹配到 demo.MathGame
   │           └─ retransform → ASM织入SpyAPI调用
   │               cCnt=1, mCnt=1
   ├─ process.appendResult(EnhancerModel) → 输出增强结果
   └─ session.unLock()

6. 等待目标方法被调用...
   (命令进入异步等待状态)

7. demo.MathGame.run() 被调用
   ├─ SpyAPI.atEnter(MathGame.class, "run", "...", this, args)
   │   └─ AdviceWeaver.before(...)
   │       └─ WatchAdviceListener.before(loader, clazz, method, target, args)
   │           ├─ threadLocalWatch.start() → 记录开始时间
   │           └─ isBefore=false → 不调用 watching()
   │
   ├─ 方法体执行...
   │
   └─ SpyAPI.atExit(MathGame.class, "run", "...", this, args, returnObj)
       └─ AdviceWeaver.afterReturning(...)
           └─ WatchAdviceListener.afterReturning(loader, clazz, method, target, args, returnObj)
               ├─ Advice.newForAfterReturning(...)
               ├─ isSuccess=false → 不调用 watching()
               └─ finishing(advice)
                   └─ isFinish()=true → watching(advice)
                       ├─ cost = threadLocalWatch.costInMillis() = 120.5
                       ├─ isConditionMet(null, advice, 120.5) = true
                       ├─ getExpressionResult("{params, returnObj}", advice, 120.5)
                       │   └─ ExpressFactory.threadLocalExpress(advice)
                       │       └─ OgnlExpress.get("{params, returnObj}")
                       │           └─ 返回 Object[] {params, returnObj}
                       ├─ new WatchModel()
                       │   ts = now
                       │   cost = 120.5
                       │   value = ObjectVO(Object[], 2)
                       │   className = "demo.MathGame"
                       │   methodName = "run"
                       │   accessPoint = "AtExit"
                       ├─ process.appendResult(model) → 终端输出
                       ├─ process.times() = 1
                       └─ isLimitExceeded(3, 1) = false → 继续

8. 第2次调用 → times=2 → 继续
9. 第3次调用 → times=3 → isLimitExceeded(3, 3) = true
   └─ abortProcess(process, 3)
       ├─ process.write("Command execution times exceed limit: 3...")
       └─ process.end()
           └─ 清理增强代码、销毁监听器
```

### 19.2 trace 命令完整调用链

以 `trace demo.MathGame run -n 1` 为例：

```
1-5. 与 watch 类似，但创建 TraceAdviceListener
    Enhancer(listener, true, true, ...) ← isTracing=true, skipJDKTrace=true

6. 等待目标方法被调用...

7. demo.MathGame.run() 被调用
   ├─ SpyAPI.atEnter → AdviceWeaver.before → TraceAdviceListener.before()
   │   ├─ threadLocalTraceEntity(loader) → 获取/创建 TraceEntity
   │   ├─ tree.begin("demo.MathGame", "run", -1, false)
   │   │   └─ 创建 MethodNode(MathGame, run, -1)
   │   ├─ deep++ → deep=1
   │   └─ threadLocalWatch.start()
   │
   ├─ 方法体中调用 methodB()
   │   ├─ SpyTraceExcludeJDKInterceptor → invokeBeforeTracing("ClassB", "methodB", ...)
   │   │   └─ tree.begin("ClassB", "methodB", lineNumber, true)
   │   │       └─ 创建 MethodNode(ClassB, methodB, lineNumber)
   │   ├─ methodB() 实际执行
   │   └─ invokeAfterTracing("ClassB", "methodB", ...)
   │       └─ tree.end() → 记录耗时，current 回到 methodA 节点
   │
   ├─ 方法体中调用 methodC()
   │   ├─ invokeBeforeTracing("ClassC", "methodC", ...)
   │   │   └─ tree.begin("ClassC", "methodC", lineNumber, true)
   │   ├─ methodC() 实际执行
   │   └─ invokeAfterTracing("ClassC", "methodC", ...)
   │       └─ tree.end()
   │
   └─ SpyAPI.atExit → AdviceWeaver.afterReturning → TraceAdviceListener.afterReturning()
       ├─ tree.end() → 结束 MathGame.run 节点
       ├─ Advice.newForAfterReturning(...)
       └─ finishing(loader, advice)
           ├─ deep-- → deep=0
           ├─ deep==0 → 输出整棵树
           ├─ cost = threadLocalWatch.costInMillis()
           ├─ isConditionMet(null, advice, cost) = true
           ├─ process.times()++ → 1
           ├─ traceEntity.getModel()
           │   ├─ tree.trim() → 标准化类名
           │   └─ new TraceModel(root, nodeCount)
           ├─ process.appendResult(traceModel) → 终端输出调用树
           ├─ isLimitExceeded(1, 1) = true
           └─ abortProcess(process, 1)
               └─ process.end()
           └─ finally: threadBoundEntity.remove()
```

### 19.3 stack 命令完整调用链

以 `stack demo.MathGame run -n 1` 为例：

```
1-5. 与 watch 类似，但创建 StackAdviceListener
    Enhancer(listener, false, false, ...) ← isTracing=false

6. 等待目标方法被调用...

7. demo.MathGame.run() 被调用
   ├─ SpyAPI.atEnter → AdviceWeaver.before → StackAdviceListener.before()
   │   └─ threadLocalWatch.start() → 只启动计时器
   │
   ├─ 方法体执行...
   │
   └─ SpyAPI.atExit → AdviceWeaver.afterReturning → StackAdviceListener.afterReturning()
       ├─ Advice.newForAfterReturning(...)
       └─ finishing(advice)
           ├─ cost = threadLocalWatch.costInMillis()
           ├─ isConditionMet(null, advice, cost) = true
           ├─ ThreadUtil.getThreadStackModel(loader, Thread.currentThread())
           │   └─ 获取线程名、ID、ClassLoader、完整栈帧
           │   └─ 创建 StackModel
           ├─ stackModel.setTs(now)
           ├─ process.appendResult(stackModel) → 终端输出调用栈
           ├─ process.times()++ → 1
           ├─ isLimitExceeded(1, 1) = true
           └─ abortProcess(process, 1)
               └─ process.end()
```

---

## 第20阶段：总结与设计哲学

### 20.1 Arthas 监控体系的设计哲学

1. **模板方法模式**：`EnhancerCommand` 定义增强流程模板，子类只需实现匹配器和监听器。
   这保证了所有监控命令遵循相同的增强流程，减少了重复代码。

2. **适配器模式**：`AdviceListenerAdapter` 将 SpyAPI 的低级回调适配为高级的监听器接口，
   提供了通用的条件判断、表达式求值、次数限制等方法。

3. **策略模式**：通过不同的 `AdviceListener` 实现（Watch/Trace/Stack/Monitor/TimeTunnel），
   在相同的增强框架下实现不同的监控策略。

4. **线程隔离**：广泛使用 ThreadLocal 确保多线程环境下的数据隔离，
   同时使用 WeakReference 和原生类型数组避免 ClassLoader 泄漏。

5. **防御性编程**：补偿性锁检查、AtomicBoolean 保证单次中断、固定大小环形栈防止内存溢出。

6. **延迟初始化**：匹配器懒创建、类名延迟标准化、TraceEntity 懒创建，减少不必要的开销。

### 20.2 核心类之间的关系图

```
                    ┌──────────────┐
                    │  Annotated    │
                    │  Command      │
                    └──────┬───────┘
                           │ extends
                    ┌──────┴───────┐
                    │  Enhancer     │
                    │  Command      │
                    │  (abstract)   │
                    └──────┬───────┘
                           │ extends
           ┌───────────────┼───────────────┐
           │               │               │
    ┌──────┴──────┐ ┌──────┴──────┐ ┌──────┴──────┐
    │  Watch      │ │  Trace      │ │  Stack      │
    │  Command    │ │  Command    │ │  Command    │
    └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
           │               │               │
           │ getAdviceListener()           │
           │               │               │
    ┌──────┴──────┐ ┌──────┴──────┐ ┌──────┴──────┐
    │  Watch      │ │  Trace      │ │  Stack      │
    │  Advice     │ │  Advice     │ │  Advice     │
    │  Listener   │ │  Listener   │ │  Listener   │
    └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
           │               │               │
           │ extends       │ extends       │ extends
           │               │               │
    ┌──────┴───────────────┴───────────────┴──────┐
    │         AdviceListenerAdapter               │
    │  (isConditionMet, getExpressionResult,      │
    │   isLimitExceeded, abortProcess)            │
    └──────────────────────┬──────────────────────┘
                           │ implements
                    ┌──────┴───────┐
                    │  Advice      │
                    │  Listener    │
                    │  (interface) │
                    └──────────────┘
                           │
                           │ called by
                    ┌──────┴───────┐
                    │  Advice      │
                    │  Weaver      │
                    └──────┬───────┘
                           │ called by
                    ┌──────┴───────┐
                    │  SpyAPI      │
                    │  (bootstrap) │
                    └──────────────┘
                           │
                           │ triggered by
                    ┌──────┴───────┐
                    │  Enhancer    │
                    │  (ASM字节码)  │
                    └──────────────┘
```

### 20.3 数据流总结

```
用户输入 → CLI解析 → Command对象 → EnhancerCommand.process() → enhance()
    → Enhancer.enhance() → ASM字节码织入 → SpyAPI调用
    → AdviceWeaver → AdviceListenerAdapter → 具体Listener
    → Advice(上下文) → ExpressFactory/OGNL → Model
    → process.appendResult() → ResultView → 终端输出
```

### 20.4 关键源码文件索引

| 文件 | 路径 | 说明 |
|------|------|------|
| EnhancerCommand | `core/src/main/java/.../command/monitor200/EnhancerCommand.java` | 增强命令基类 |
| WatchCommand | `core/src/main/java/.../command/monitor200/WatchCommand.java` | watch 命令定义 |
| WatchAdviceListener | `core/src/main/java/.../command/monitor200/WatchAdviceListener.java` | watch 回调实现 |
| TraceCommand | `core/src/main/java/.../command/monitor200/TraceCommand.java` | trace 命令定义 |
| AbstractTraceAdviceListener | `core/src/main/java/.../command/monitor200/AbstractTraceAdviceListener.java` | trace 回调基类 |
| TraceAdviceListener | `core/src/main/java/.../command/monitor200/TraceAdviceListener.java` | trace 具体实现 |
| TraceEntity | `core/src/main/java/.../command/monitor200/TraceEntity.java` | 调用树容器 |
| TraceTree | `core/src/main/java/.../command/model/TraceTree.java` | 调用树数据结构 |
| TraceNode | `core/src/main/java/.../command/model/TraceNode.java` | 树节点基类 |
| MethodNode | `core/src/main/java/.../command/model/MethodNode.java` | 方法调用节点 |
| StackCommand | `core/src/main/java/.../command/monitor200/StackCommand.java` | stack 命令定义 |
| StackAdviceListener | `core/src/main/java/.../command/monitor200/StackAdviceListener.java` | stack 回调实现 |
| StackModel | `core/src/main/java/.../command/model/StackModel.java` | stack 结果模型 |
| Advice | `core/src/main/java/.../advisor/Advice.java` | 方法调用上下文 |
| AdviceListener | `core/src/main/java/.../advisor/AdviceListener.java` | 监听器接口 |
| AdviceListenerAdapter | `core/src/main/java/.../advisor/AdviceListenerAdapter.java` | 监听器适配器 |
| AccessPoint | `core/src/main/java/.../advisor/AccessPoint.java` | 访问点枚举 |
| InvokeTraceable | `core/src/main/java/.../advisor/InvokeTraceable.java` | 方法调用跟踪接口 |
| Enhancer | `core/src/main/java/.../advisor/Enhancer.java` | 字节码增强器 |
| ExpressFactory | `core/src/main/java/.../command/express/ExpressFactory.java` | 表达式工厂 |
| OgnlExpress | `core/src/main/java/.../command/express/OgnlExpress.java` | OGNL 表达式实现 |
| ThreadLocalWatch | `core/src/main/java/.../util/ThreadLocalWatch.java` | 线程本地计时器 |
| WatchModel | `core/src/main/java/.../command/model/WatchModel.java` | watch 结果模型 |
| ObjectVO | `arthas-model/src/main/java/.../command/model/ObjectVO.java` | 值对象包装 |
| MonitorCommand | `core/src/main/java/.../command/monitor200/MonitorCommand.java` | monitor 命令定义 |
| TimeTunnelCommand | `core/src/main/java/.../command/monitor200/TimeTunnelCommand.java` | tt 命令定义 |

---

## 附录：Arthas 监控命令速查表

| 命令 | 核心类 | 监听器 | 输出模型 | 增强类型 | 适用场景 |
|------|--------|--------|----------|---------|---------|
| watch | WatchCommand | WatchAdviceListener | WatchModel | 方法级 | 观察方法参数/返回值/异常 |
| trace | TraceCommand | TraceAdviceListener | TraceModel | 方法级+调用级 | 分析调用链路耗时 |
| stack | StackCommand | StackAdviceListener | StackModel | 方法级 | 定位方法调用来源 |
| monitor | MonitorCommand | MonitorAdviceListener | MonitorModel | 方法级 | 统计方法执行指标 |
| tt | TimeTunnelCommand | TimeTunnelAdviceListener | TimeTunnelModel | 方法级 | 记录/回放方法调用 |

---

*本文档基于 Arthas 源码逐行分析编写，覆盖了 watch/trace/stack 三个核心监控命令从命令输入到结果输出的完整调用链路。
所有代码片段均来自实际源码文件，确保准确无误。*
