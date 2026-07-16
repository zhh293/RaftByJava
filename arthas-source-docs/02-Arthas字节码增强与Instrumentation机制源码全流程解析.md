# Arthas 字节码增强与 Instrumentation 机制源码全流程解析

> 本文基于 Arthas 开源项目源码进行分析，源码根目录位于 `/arthas`。分析涵盖从用户输入 `watch` 命令触发字节码增强，到运行时回调输出结果的完整链路。本文将逐类、逐方法、逐行地追踪核心逻辑，不跳步、不省略，力求还原 Arthas 字节码增强机制的全貌。

---

## 全局调用链总览

在深入每一个类的源码之前，我们先用一张完整的 ASCII 调用链路图，建立起从用户输入命令到运行时回调的全局视角。这张图将贯穿本文始终，后续每一节都是在展开这张图中的某一个节点。

```
用户输入: watch com.example.MyService myMethod

    |
    v
+-------------------------------------------+
|  WatchCommand.process(CommandProcess)      |
|  |                                         |
|  +-> EnhancerCommand.enhance(process)      |
|      |                                     |
|      +-> getAdviceListenerWithId(process)   |
|      |   |                                 |
|      |   +-> new WatchAdviceListener(...)   |
|      |                                     |
|      +-> new Enhancer(listener, ...)       |
|      |                                     |
|      +-> process.register(listener,        |
|      |                    enhancer)         |
|      |                                     |
|      +-> enhancer.enhance(inst,            |
|                           maxMatch)        |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
|  Enhancer.enhance(Instrumentation, int)    |
|  |                                         |
|  +-> SearchUtils.searchClass(inst,         |
|  |                  classNameMatcher)       |
|  +-> filter(matchingClasses)               |
|  +-> TransformerManager                    |
|  |     .addTransformer(this, isTracing)    |
|  +-> inst.retransformClasses(classArray)   |
+-------------------------------------------+
    |
    v  (JVM 触发 retransform 回调)
+-------------------------------------------+
|  TransformerManager                        |
|  (复合 classFileTransformer)               |
|  |                                         |
|  +-> reTransformers 遍历 transform()      |
|  +-> watchTransformers 遍历 transform()   |
|  +-> traceTransformers 遍历 transform()   |
|      |                                     |
|      +-> Enhancer.transform(...)           |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
|  Enhancer.transform(ClassLoader,           |
|        className, classBeingRedefined,     |
|        protectionDomain, classfileBuffer)  |
|  |                                         |
|  +-> 检查 ClassLoader 能否加载 SpyAPI     |
|  +-> matchingClasses 过滤                  |
|  +-> ASM ClassReader/ClassNode 解析        |
|  +-> 移除 JSR 指令                         |
|  +-> 解析拦截器模板:                        |
|  |   SpyInterceptor1 (@AtEnter)            |
|  |   SpyInterceptor2 (@AtExit)             |
|  |   SpyInterceptor3 (@AtExceptionExit)    |
|  |   + 可选 SpyTraceInterceptor1/2/3      |
|  +-> 方法匹配 (methodNameMatcher)          |
|  +-> GroupLocationFilter 防重复织入         |
|  +-> 逐方法织入:                            |
|  |   MethodProcessor + InterceptorProcessor|
|  +-> AdviceListenerManager                 |
|  |     .registerAdviceListener(...)        |
|  +-> AsmUtils.toBytes() 生成最终字节码     |
|  +-> dump class (可选)                     |
+-------------------------------------------+
    |
    v  (增强后的字节码被 JVM 加载)
+-------------------------------------------+
|  目标方法被调用时的运行时回调               |
|                                             |
|  目标方法 myMethod() 执行 -->               |
|  |                                         |
|  +-> SpyAPI.atEnter(clazz, methodInfo,     |
|  |                  target, args)           |
|  |   |                                     |
|  |   +-> SpyImpl.atEnter(...)              |
|  |       |                                 |
|  |       +-> StringUtils.splitMethodInfo() |
|  |       +-> AdviceListenerManager         |
|  |       |     .queryAdviceListeners(...)  |
|  |       +-> listener.before(...)          |
|  |           |                             |
|  |           +-> AdviceListenerAdapter     |
|  |           |     .before(...)            |
|  |           +-> WatchAdviceListener       |
|  |                 .before(loader, ...)    |
|  |                 |                       |
|  |                 +-> threadLocalWatch    |
|  |                       .start()          |
|  |                                         |
|  +-> [目标方法体执行]                       |
|  |                                         |
|  +-> SpyAPI.atExit(clazz, methodInfo,      |
|  |                 target, args, returnObj) |
|  |   |                                     |
|  |   +-> SpyImpl.atExit(...)               |
|  |       |                                 |
|  |       +-> listener.afterReturning(...)  |
|  |           |                             |
|  |           +-> WatchAdviceListener       |
|  |                 .afterReturning(...)     |
|  |                 |                       |
|  |                 +-> watching(advice)    |
|  |                     |                   |
|  |                     +-> 计算耗时        |
|  |                     +-> 条件表达式判断   |
|  |                     +-> OGNL 求值       |
|  |                     +-> process         |
|  |                          .appendResult  |
|  |                          (WatchModel)   |
|  |                                         |
|  +-> (异常时) SpyAPI.atExceptionExit(...)  |
|      |                                     |
|      +-> SpyImpl.atExceptionExit(...)      |
|          +-> listener.afterThrowing(...)   |
+-------------------------------------------+
    |
    v
+-------------------------------------------+
|  结果通过 CommandProcess 输出到终端         |
+-------------------------------------------+
```

上面这张图清晰地展示了整个流程的五大阶段：

| 阶段 | 核心动作 | 关键类 |
|------|---------|--------|
| 第一阶段 | 命令解析与增强入口 | WatchCommand, EnhancerCommand |
| 第二阶段 | 类搜索与增强触发 | Enhancer.enhance(), SearchUtils |
| 第三阶段 | 字节码织入 | Enhancer.transform(), TransformerManager |
| 第四阶段 | 运行时回调 | SpyAPI, SpyImpl, AdviceListenerManager |
| 第五阶段 | 结果输出 | WatchAdviceListener, CommandProcess |

接下来，我们将按照这五大阶段逐一展开。

---

## 第一阶段：Java Instrumentation API 基础知识

在深入 Arthas 源码之前，我们必须先理解 Arthas 所依赖的底层 JVM 机制 —— Java Instrumentation API。这是整个字节码增强的地基。

### 1.1 ClassFileTransformer 接口 —— 字节码转换的核心契约

`java.lang.instrument.ClassFileTransformer` 是 JDK 提供的标准接口，它只有一个方法：

```java
public interface ClassFileTransformer {
    byte[] transform(ClassLoader loader,
                     String className,
                     Class<?> classBeingRedefined,
                     ProtectionDomain protectionDomain,
                     byte[] classfileBuffer)
            throws IllegalClassFormatException;
}
```

这个接口的语义是：当 JVM 加载或重新定义一个类时，会调用已注册的 `ClassFileTransformer` 的 `transform` 方法，给你一次修改类字节码的机会。

**参数解释：**

| 参数 | 含义 |
|------|------|
| `loader` | 加载该类的 ClassLoader，如果是 Bootstrap ClassLoader 则为 `null` |
| `className` | 类的内部名称（用 `/` 分隔，如 `com/example/MyService`） |
| `classBeingRedefined` | 如果是 retransform/redefine 触发的，指向已加载的 Class 对象；如果是类首次加载，则为 `null` |
| `protectionDomain` | 类的保护域 |
| `classfileBuffer` | 原始的类字节码（或前一个 transformer 修改后的字节码） |

**返回值：**
- 返回修改后的字节码数组：JVM 将使用这个新的字节码
- 返回 `null`：表示不做任何修改，使用传入的 `classfileBuffer`

这就像一条流水线：每个 `ClassFileTransformer` 是流水线上的一个工位，原始字节码从第一个工位流入，每个工位可以对字节码做修改，最终修改后的字节码被 JVM 加载。

### 1.2 Instrumentation.addTransformer() —— 注册转换器

JVM 提供了两种注册 transformer 的方式：

```java
// 方式一：非 retransform-capable
void addTransformer(ClassFileTransformer transformer);

// 方式二：retransform-capable
void addTransformer(ClassFileTransformer transformer, boolean canRetransform);
```

**这两种方式的关键区别：**

| 特性 | `addTransformer(t)` 或 `addTransformer(t, false)` | `addTransformer(t, true)` |
|------|---------------------------------------------------|---------------------------|
| 类首次加载时被调用 | 是 | 是 |
| retransformClasses 时被调用 | **否** | **是** |
| 用途 | 只想在类首次加载时拦截 | 需要对已加载的类进行修改 |

> 这一点至关重要！Arthas 的 `TransformerManager` 正是利用了这个区别，同时注册了两种 transformer：一个 retransform-capable 的（处理 watch/trace 等），一个非 retransform-capable 的（处理懒加载模式）。

### 1.3 retransformClasses() —— 重新转换已加载的类

```java
void retransformClasses(Class<?>... classes) throws UnmodifiableClassException;
```

这个方法的作用是：告诉 JVM "请重新对这些已经加载的类执行一次 transform 流程"。

调用后，JVM 会：
1. 获取这些类的原始字节码（不是当前在用的字节码，而是最初 ClassLoader 加载时的原始字节码）
2. 依次调用所有 retransform-capable 的 `ClassFileTransformer`
3. 用最终的字节码替换 JVM 中该类的定义

**为什么需要 retransformClasses？**

当用户执行 `watch com.example.MyService myMethod` 时，`MyService` 这个类很可能已经被 JVM 加载了（应用启动时就加载了）。我们不能等到类"下次加载"才去增强——因为这个类可能永远不会被重新加载。所以必须通过 `retransformClasses()` 主动触发一次 transform 流程，让我们的 `Enhancer`（实现了 `ClassFileTransformer`）有机会修改这个已加载类的字节码。

### 1.4 premain vs agentmain —— Agent 的两种启动方式

| 启动方式 | 时机 | JVM 参数 | 典型场景 |
|---------|------|---------|---------|
| `premain` | JVM 启动时，main 方法之前 | `-javaagent:agent.jar` | 应用启动前就需要增强的场景 |
| `agentmain` | JVM 运行中，通过 Attach API 动态加载 | 无需 JVM 参数 | Arthas 的核心启动方式 |

Arthas 主要使用 `agentmain` 方式。用户运行 `java -jar arthas-boot.jar` 后，Arthas 通过 JDK 的 Attach API 动态连接到目标 JVM，并加载 agent。这就是为什么 Arthas 能够在不重启应用的情况下进行诊断。

### 1.5 retransform-capable vs not —— 为什么这个区别如此重要

让我们用一个类比来理解：

- **retransform-capable 的 transformer** 就像一个"常驻安检员"：无论行人（类）是第一次通过安检口，还是被要求"请再走一遍安检"（retransformClasses），这个安检员都会检查。
- **非 retransform-capable 的 transformer** 就像一个"新客迎宾员"：只在客人第一次进门时打招呼，如果客人被要求重新走一遍流程，迎宾员不会再理会。

Arthas 的 `TransformerManager` 精妙地利用了这个区别：

- 主 transformer（retransform-capable=true）：处理 watch、trace 等命令的增强，能对已加载的类通过 `retransformClasses` 进行修改
- 懒加载 transformer（retransform-capable=false）：只在类首次被 ClassLoader 加载时触发，用于实现"等类加载时自动增强"的懒加载模式

---

## 第二阶段：命令入口与增强触发

### 2.1 WatchCommand —— 用户命令的入口

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/WatchCommand.java`

`WatchCommand` 继承自 `EnhancerCommand`，是用户输入 `watch` 命令后第一个被触发的类。

```java
@Name("watch")
@Summary("Display the input/output parameter, return object, "
       + "and thrown exception of specified method invocation")
public class WatchCommand extends EnhancerCommand {
    private String classPattern;
    private String methodPattern;
    private String express;
    private String conditionExpress;
    private boolean isBefore = false;
    private boolean isFinish = false;
    private boolean isException = false;
    private boolean isSuccess = false;
    private Integer expand = 1;
    private int numberOfLimit = 100;
    // ...
}
```

当用户输入 `watch com.example.MyService myMethod` 时，CLI 框架会：

1. 将 `com.example.MyService` 设置到 `classPattern`
2. 将 `myMethod` 设置到 `methodPattern`
3. 将默认表达式 `{params, target, returnObj}` 设置到 `express`

**关键方法 `getAdviceListener`：**

```java
@Override
protected AdviceListener getAdviceListener(CommandProcess process) {
    return new WatchAdviceListener(this, process,
            GlobalOptions.verbose || this.verbose);
}
```

这个方法创建了一个 `WatchAdviceListener` 实例。这个 listener 就是最终在运行时接收回调、输出 watch 结果的对象。

**问：WatchCommand 本身做了多少事情？**

答：WatchCommand 本身做的事情很少——它只负责两件事：(1) 解析用户输入的参数；(2) 创建对应的 `WatchAdviceListener`。真正的增强逻辑全部委托给了父类 `EnhancerCommand`。

### 2.2 WatchCommand.process() —— 命令执行入口

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
```

这个方法首先验证 `sizeLimit` 参数（如果用户指定了 `-M` 选项），然后调用父类 `EnhancerCommand.process()`。

### 2.3 EnhancerCommand —— 所有增强命令的公共基类

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/EnhancerCommand.java`

`EnhancerCommand` 是 `WatchCommand`、`TraceCommand`、`StackCommand`、`MonitorCommand` 等所有需要字节码增强的命令的共同父类。

```java
public abstract class EnhancerCommand extends AnnotatedCommand {
    protected static final List<String> EMPTY = Collections.emptyList();
    private String excludeClassPattern;
    protected Matcher classNameMatcher;
    protected Matcher classNameExcludeMatcher;
    protected Matcher methodNameMatcher;
    protected long listenerId;
    protected boolean verbose;
    protected int maxNumOfMatchedClass;
    protected Long timeout;
    protected boolean lazy = false;
    protected String hashCode;
    // ...
}
```

**四个抽象方法定义了扩展点：**

```java
protected abstract Matcher getClassNameMatcher();
protected abstract Matcher getClassNameExcludeMatcher();
protected abstract Matcher getMethodNameMatcher();
protected abstract AdviceListener getAdviceListener(CommandProcess process);
```

每个子命令（watch、trace 等）通过实现这四个方法来定制自己的行为：匹配哪些类、匹配哪些方法、使用什么 listener 来处理回调。

### 2.4 EnhancerCommand.process() —— 设置中断处理后进入增强

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

这一步做了两件事：

1. **注册中断处理器**：当用户按 `Ctrl+C` 时，`CommandInterruptHandler` 会被触发，负责清理增强状态
2. **注册输入处理器**：当用户输入 `q` 时，`QExitHandler` 会被触发，同样负责退出命令
3. **调用 `enhance(process)`**：进入真正的增强流程

### 2.5 EnhancerCommand.enhance() —— 增强流程的总控方法

这是整个增强流程的"总指挥"，我们逐段分析：

**第一段：获取会话锁**

```java
protected void enhance(CommandProcess process) {
    Session session = process.session();
    if (!session.tryLock()) {
        String msg = "someone else is enhancing classes, pls. wait.";
        process.appendResult(
            EnhancerModelFactory.create(null, false, msg));
        process.end(-1, msg);
        return;
    }
    // ...
}
```

Arthas 通过会话锁保证同一时刻只有一个命令在进行字节码增强。这是因为 `retransformClasses` 是一个全局性操作，多个命令同时增强可能导致不可预测的问题。

**这一步做了什么？** 尝试获取增强锁。如果有其他命令正在增强，则直接返回错误信息。

**第二段：创建 Listener 和 Enhancer**

```java
Instrumentation inst = session.getInstrumentation();
AdviceListener listener = getAdviceListenerWithId(process);
if (listener == null) {
    logger.error("advice listener is null");
    String msg = "advice listener is null, check arthas log";
    process.appendResult(
        EnhancerModelFactory.create(effect, false, msg));
    process.end(-1, msg);
    return;
}
boolean skipJDKTrace = false;
if (listener instanceof AbstractTraceAdviceListener) {
    skipJDKTrace = ((AbstractTraceAdviceListener) listener)
            .getCommand().isSkipJDKTrace();
}
```

这段代码从 session 中获取 `Instrumentation` 实例（这是 JVM Agent 启动时传入的），然后通过 `getAdviceListenerWithId` 获取 listener。

**`getAdviceListenerWithId` 方法的逻辑：**

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

如果用户指定了 `--listenerId`（复用已有的 listener），就从 `AdviceWeaver` 中查找；否则调用子类的 `getAdviceListener()` 创建一个新的 listener。对于 `WatchCommand`，这会创建一个新的 `WatchAdviceListener`。

**第三段：创建 Enhancer 并执行增强**

```java
Enhancer enhancer = new Enhancer(listener,
        listener instanceof InvokeTraceable,
        skipJDKTrace,
        getClassNameMatcher(),
        getClassNameExcludeMatcher(),
        getMethodNameMatcher(),
        this.lazy,
        this.hashCode);
enhancer.setLineEnhanceOptions(getLineEnhanceOptions());

// 注册通知监听器
process.register(listener, enhancer);

effect = enhancer.enhance(inst, this.maxNumOfMatchedClass);
```

这里有几个关键点：

1. **`listener instanceof InvokeTraceable`**：判断 listener 是否实现了 `InvokeTraceable` 接口。对于 `WatchAdviceListener`，它没有实现这个接口（因为 watch 不需要跟踪方法内部调用），所以 `isTracing` 为 `false`。而 `TraceAdviceListener` 实现了 `InvokeTraceable`，所以 trace 命令的 `isTracing` 为 `true`。
2. **`process.register(listener, enhancer)`**：将 listener 和 enhancer 注册到进程中，以便后续清理（当命令结束时，需要移除 transformer 并还原字节码）。
3. **`enhancer.enhance(inst, maxNumOfMatchedClass)`**：这是真正触发增强的调用，下一阶段我们将深入分析。

**第四段：处理增强结果**

```java
if (effect.getThrowable() != null) {
    String msg = "error happens when enhancing class: "
        + effect.getThrowable().getMessage();
    process.appendResult(
        EnhancerModelFactory.create(effect, false, msg));
    process.end(1, msg + ", check arthas log: "
        + LogUtil.loggingFile());
    return;
}

if (effect.cCnt() == 0 || effect.mCnt() == 0) {
    if (!StringUtils.isEmpty(effect.getOverLimitMsg())) {
        process.appendResult(
            EnhancerModelFactory.create(effect, false));
        process.end(-1);
        return;
    }
    
    if (this.lazy) {
        String lazyMsg = "Lazy mode is enabled, "
            + "waiting for class to be loaded. "
            + "Press Q or Ctrl+C to abort.\n"
            + "When the target class is loaded, "
            + "it will be automatically enhanced.";
        process.write(lazyMsg + "\n");
    } else {
        // 提示用户各种排查建议
        process.end(-1, msg);
        return;
    }
}
```

增强完成后，检查结果：

1. **有异常**：直接输出错误信息并结束
2. **没有匹配到任何类或方法**：
   - 如果超过数量限制，输出限制信息
   - 如果是懒加载模式，不结束命令，继续等待类加载
   - 否则，输出详细的排查建议（使用 sm 命令确认方法存在、设置 unsafe 等）

**第五段：超时任务**

```java
// 设置超时任务
scheduleTimeoutTask(process);
```

```java
private void scheduleTimeoutTask(final CommandProcess process) {
    if (timeout == null || timeout <= 0) {
        return;
    }
    final ScheduledFuture<?> timeoutFuture = ArthasBootstrap
        .getInstance().getScheduledExecutorService()
        .schedule(new Runnable() {
            @Override
            public void run() {
                if (process.isRunning()) {
                    process.write("Command execution timeout "
                        + "after " + timeout + " seconds.\n");
                    process.end();
                }
            }
        }, timeout, TimeUnit.SECONDS);

    process.endHandler(
        new com.taobao.arthas.core.shell.handlers.Handler<Void>() {
            @Override
            public void handle(Void event) {
                timeoutFuture.cancel(false);
            }
        });
}
```

如果用户指定了 `--timeout` 参数，则通过 `ScheduledExecutorService` 安排一个定时任务。超时后自动结束命令。同时注册了 `endHandler`，如果命令正常结束了，就取消超时任务。

---

## 第三阶段：TransformerManager —— 增强器的统一管理中枢

### 3.1 TransformerManager 类 —— 架构概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/TransformerManager.java`

`TransformerManager` 是整个字节码增强体系的"中央调度器"。它管理着所有的 `ClassFileTransformer`，并且向 JVM 的 `Instrumentation` 注册了两个复合 transformer。

```java
public class TransformerManager {

    private Instrumentation instrumentation;

    private List<ClassFileTransformer> watchTransformers =
        new CopyOnWriteArrayList<ClassFileTransformer>();

    private List<ClassFileTransformer> traceTransformers =
        new CopyOnWriteArrayList<ClassFileTransformer>();

    private List<ClassFileTransformer> reTransformers =
        new CopyOnWriteArrayList<ClassFileTransformer>();

    private List<ClassFileTransformer> lazyTransformers =
        new CopyOnWriteArrayList<ClassFileTransformer>();

    private ClassFileTransformer classFileTransformer;
    private ClassFileTransformer lazyClassFileTransformer;

    // ...
}
```

### 3.2 四类 Transformer —— 为什么需要分四类？

Arthas 将 transformer 分为四类，每一类承担不同的职责：

| 类别 | 存储字段 | 执行顺序 | 注册方式 | 用途 |
|------|---------|---------|---------|------|
| reTransformers | `reTransformers` | 最先执行 | `addRetransformer()` | 先于 watch/trace 的通用 transformer |
| watchTransformers | `watchTransformers` | 第二执行 | `addTransformer(t, false)` | watch/monitor/stack 等命令 |
| traceTransformers | `traceTransformers` | 第三执行 | `addTransformer(t, true)` | trace 命令 |
| lazyTransformers | `lazyTransformers` | 独立通道 | `addLazyTransformer()` | 懒加载模式 |

**为什么顺序是 re -> watch -> trace？**

这个顺序的设计考量是：

1. **reTransformers 最先执行**：这些 transformer 通常执行一些预处理或修复操作，需要在其他增强之前完成。源码注释中写道 `先于 watch/trace 的 Transformer`，并且留了一个 TODO：`改进为全部用 order 排序？`，说明目前是通过分类来隐式排序的。

2. **watch 先于 trace**：watch 命令只在方法的入口/出口插桩，而 trace 命令需要在方法内部的每个子调用处插桩。如果 trace 先执行，它插入的大量字节码可能影响后续 watch 的位置判断。让 watch 先执行，确保基础的 enter/exit 插桩位置稳定，然后 trace 在此基础上添加 invoke 级别的插桩。

3. **CopyOnWriteArrayList 的选择**：四个列表都使用了 `CopyOnWriteArrayList`，这是因为 transformer 的注册/移除是低频操作（只在命令执行/结束时发生），而遍历是高频操作（每次 retransform 都要遍历），`CopyOnWriteArrayList` 适合读多写少的场景。

### 3.3 构造方法 —— 注册两个复合 Transformer

构造方法是 `TransformerManager` 最核心的逻辑所在。让我们逐行分析。

**第一个复合 transformer（retransform-capable）：**

```java
public TransformerManager(Instrumentation instrumentation) {
    this.instrumentation = instrumentation;

    classFileTransformer = new ClassFileTransformer() {
        @Override
        public byte[] transform(ClassLoader loader,
                String className,
                Class<?> classBeingRedefined,
                ProtectionDomain protectionDomain,
                byte[] classfileBuffer)
                throws IllegalClassFormatException {

            for (ClassFileTransformer classFileTransformer
                    : reTransformers) {
                byte[] transformResult =
                    classFileTransformer.transform(loader,
                        className, classBeingRedefined,
                        protectionDomain, classfileBuffer);
                if (transformResult != null) {
                    classfileBuffer = transformResult;
                }
            }

            for (ClassFileTransformer classFileTransformer
                    : watchTransformers) {
                byte[] transformResult =
                    classFileTransformer.transform(loader,
                        className, classBeingRedefined,
                        protectionDomain, classfileBuffer);
                if (transformResult != null) {
                    classfileBuffer = transformResult;
                }
            }

            for (ClassFileTransformer classFileTransformer
                    : traceTransformers) {
                byte[] transformResult =
                    classFileTransformer.transform(loader,
                        className, classBeingRedefined,
                        protectionDomain, classfileBuffer);
                if (transformResult != null) {
                    classfileBuffer = transformResult;
                }
            }

            return classfileBuffer;
        }
    };

    instrumentation.addTransformer(classFileTransformer, true);
```

这个匿名内部类实现的 `ClassFileTransformer` 做了三件事，严格按 re -> watch -> trace 的顺序：

1. 遍历所有 `reTransformers`，依次对字节码进行变换
2. 遍历所有 `watchTransformers`，依次对字节码进行变换
3. 遍历所有 `traceTransformers`，依次对字节码进行变换

每一步中，如果某个 transformer 返回了非 `null` 的结果，就用这个结果替换 `classfileBuffer`，作为下一个 transformer 的输入。这就是"流水线"模式：每个 transformer 基于前一个 transformer 的输出继续加工。

注意最后一行 `instrumentation.addTransformer(classFileTransformer, true)`，第二个参数是 `true`，表示这是一个 retransform-capable 的 transformer。这意味着当调用 `inst.retransformClasses()` 时，这个 transformer 会被触发。

**第二个复合 transformer（非 retransform-capable，懒加载专用）：**

```java
    lazyClassFileTransformer = new ClassFileTransformer() {
        @Override
        public byte[] transform(ClassLoader loader,
                String className,
                Class<?> classBeingRedefined,
                ProtectionDomain protectionDomain,
                byte[] classfileBuffer)
                throws IllegalClassFormatException {
            // 只处理类首次加载的情况
            if (classBeingRedefined != null) {
                return null;
            }

            for (ClassFileTransformer transformer
                    : lazyTransformers) {
                byte[] transformResult =
                    transformer.transform(loader, className,
                        classBeingRedefined,
                        protectionDomain, classfileBuffer);
                if (transformResult != null) {
                    classfileBuffer = transformResult;
                }
            }

            return classfileBuffer;
        }
    };

    // 使用 false 参数
    instrumentation.addTransformer(lazyClassFileTransformer, false);
}
```

这个 transformer 与第一个有两个关键区别：

1. **`classBeingRedefined != null` 时直接返回 `null`**：这意味着只处理类首次加载的情况。当 `classBeingRedefined` 不为 `null` 时，说明这是一次 retransform（类已经加载过了），懒加载 transformer 不需要处理这种情况。

2. **`addTransformer(lazyClassFileTransformer, false)`**：第二个参数是 `false`，表示这不是 retransform-capable 的。这实际上是双重保险：即使 `retransformClasses()` 被调用，这个 transformer 也不会被触发；而即使意外被触发，`classBeingRedefined != null` 的检查也会让它直接返回 `null`。

**为什么懒加载模式需要单独的 transformer？**

这是一个精妙的设计。考虑这样的场景：用户执行 `watch com.example.FutureService process --lazy`，而 `FutureService` 还没有被加载。

- 如果只用 retransform-capable 的 transformer，那么 `retransformClasses()` 只能对已加载的类生效。对于未加载的类，`retransformClasses()` 无法处理（因为你甚至拿不到 Class 对象来传给它）。
- 使用非 retransform-capable 的 transformer（`addTransformer(t, false)`），当类首次被 ClassLoader 加载时，JVM 会自动调用这个 transformer，从而有机会在类加载的那一刻对其进行增强。

这就是为什么需要两个独立的复合 transformer：一个负责"修改已加载的类"，一个负责"拦截首次加载的类"。

### 3.4 addTransformer() —— 添加 Transformer

```java
public void addTransformer(ClassFileTransformer transformer,
                           boolean isTracing) {
    if (isTracing) {
        traceTransformers.add(transformer);
    } else {
        watchTransformers.add(transformer);
    }
}
```

这个方法根据 `isTracing` 标志决定将 transformer 放入 `watchTransformers` 还是 `traceTransformers`。对于 `watch` 命令，`isTracing` 为 `false`，所以 `Enhancer` 会被放入 `watchTransformers` 列表。

### 3.5 addLazyTransformer() —— 添加懒加载 Transformer

```java
public void addLazyTransformer(ClassFileTransformer transformer) {
    lazyTransformers.add(transformer);
}
```

当使用 `--lazy` 或 `-L` 选项时，`Enhancer` 同时会被添加到 `lazyTransformers` 列表，使其能够在类首次加载时被触发。

### 3.6 addRetransformer() —— 添加优先级最高的 Transformer

```java
public void addRetransformer(ClassFileTransformer transformer) {
    reTransformers.add(transformer);
}
```

这些 transformer 会在 watch 和 trace 之前执行。

### 3.7 removeTransformer() —— 移除 Transformer

```java
public void removeTransformer(ClassFileTransformer transformer) {
    reTransformers.remove(transformer);
    watchTransformers.remove(transformer);
    traceTransformers.remove(transformer);
    lazyTransformers.remove(transformer);
}
```

移除时从所有四个列表中尝试删除，保证不遗漏。因为一个 `Enhancer` 可能同时存在于多个列表中（例如懒加载模式下，同时在 `watchTransformers` 和 `lazyTransformers` 中）。

### 3.8 destroy() —— 完全销毁

```java
public void destroy() {
    reTransformers.clear();
    watchTransformers.clear();
    traceTransformers.clear();
    lazyTransformers.clear();
    instrumentation.removeTransformer(classFileTransformer);
    instrumentation.removeTransformer(lazyClassFileTransformer);
}
```

销毁时不仅清空所有列表，还从 `Instrumentation` 中移除两个复合 transformer。这通常在 Arthas 退出时调用。

---

## 第四阶段：Enhancer —— 字节码织入的核心引擎

`Enhancer` 是整个字节码增强体系中最核心、最复杂的类。它实现了 `ClassFileTransformer` 接口，同时提供了 `enhance()` 方法来触发增强流程。

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/Enhancer.java`

### 4.1 类定义与核心字段

```java
public class Enhancer implements ClassFileTransformer {

    private static final Logger logger =
        LoggerFactory.getLogger(Enhancer.class);

    private final AdviceListener listener;
    private final boolean isTracing;
    private final boolean skipJDKTrace;
    private final Matcher classNameMatcher;
    private final Matcher classNameExcludeMatcher;
    private final Matcher methodNameMatcher;
    private final String targetClassLoaderHash;
    private LineEnhanceOptions lineEnhanceOptions;
    private final EnhancerAffect affect;
    private Set<Class<?>> matchingClasses = null;
    private boolean isLazy = false;

    private static final ClassLoader selfClassLoader =
        Enhancer.class.getClassLoader();

    private final static Map<Class<?>, Object> classBytesCache =
        new WeakHashMap<Class<?>, Object>();

    private static SpyImpl spyImpl = new SpyImpl();
    // ...
}
```

让我们逐一理解这些字段：

| 字段 | 类型 | 含义 |
|------|------|------|
| `listener` | `AdviceListener` | 运行时回调监听器（如 WatchAdviceListener） |
| `isTracing` | `boolean` | 是否需要跟踪方法内部调用（trace 命令为 true） |
| `skipJDKTrace` | `boolean` | 跟踪时是否跳过 JDK 内部方法 |
| `classNameMatcher` | `Matcher` | 类名匹配器（支持通配符或正则） |
| `classNameExcludeMatcher` | `Matcher` | 类名排除匹配器 |
| `methodNameMatcher` | `Matcher` | 方法名匹配器 |
| `targetClassLoaderHash` | `String` | 指定 ClassLoader 的 hashCode（可选） |
| `lineEnhanceOptions` | `LineEnhanceOptions` | 行号增强选项（用于行级别调试） |
| `affect` | `EnhancerAffect` | 增强影响统计（修改了多少类、多少方法） |
| `matchingClasses` | `Set<Class<?>>` | 匹配到的类集合 |
| `isLazy` | `boolean` | 是否懒加载模式 |
| `selfClassLoader` | `ClassLoader` | Enhancer 自身的 ClassLoader |
| `classBytesCache` | `Map<Class<?>, Object>` | 已增强类的缓存（WeakHashMap，允许 GC） |
| `spyImpl` | `SpyImpl` | SpyAPI 的实现类实例 |

### 4.2 静态块 —— 设置 SpyAPI 的实现

```java
private static SpyImpl spyImpl = new SpyImpl();

static {
    SpyAPI.setSpy(spyImpl);
}
```

**这一步做了什么？**

在 `Enhancer` 类首次被加载时，静态块会执行 `SpyAPI.setSpy(spyImpl)`，将 `SpyImpl` 实例设置为 `SpyAPI` 的委托实现。

**它为什么存在？**

`SpyAPI` 位于 `java.arthas` 包下，由 BootstrapClassLoader 加载，而 `SpyImpl` 位于 Arthas 的核心模块中，由 Arthas 自己的 ClassLoader 加载。通过 `SpyAPI.setSpy()` 这个静态方法，将 Arthas ClassLoader 中的 `SpyImpl` 实例注入到 BootstrapClassLoader 可见的 `SpyAPI` 中，实现了跨 ClassLoader 的桥接。

### 4.3 构造方法 —— 三个重载版本

```java
public Enhancer(AdviceListener listener,
        boolean isTracing,
        boolean skipJDKTrace,
        Matcher classNameMatcher,
        Matcher classNameExcludeMatcher,
        Matcher methodNameMatcher) {
    this(listener, isTracing, skipJDKTrace, classNameMatcher,
         classNameExcludeMatcher, methodNameMatcher, false, null);
}

public Enhancer(AdviceListener listener,
        boolean isTracing,
        boolean skipJDKTrace,
        Matcher classNameMatcher,
        Matcher classNameExcludeMatcher,
        Matcher methodNameMatcher,
        boolean isLazy) {
    this(listener, isTracing, skipJDKTrace, classNameMatcher,
         classNameExcludeMatcher, methodNameMatcher, isLazy, null);
}

public Enhancer(AdviceListener listener,
        boolean isTracing,
        boolean skipJDKTrace,
        Matcher classNameMatcher,
        Matcher classNameExcludeMatcher,
        Matcher methodNameMatcher,
        boolean isLazy,
        String targetClassLoaderHash) {
    this.listener = listener;
    this.isTracing = isTracing;
    this.skipJDKTrace = skipJDKTrace;
    this.classNameMatcher = classNameMatcher;
    this.classNameExcludeMatcher = classNameExcludeMatcher;
    this.methodNameMatcher = methodNameMatcher;
    this.targetClassLoaderHash = targetClassLoaderHash;
    this.affect = new EnhancerAffect();
    affect.setListenerId(listener.id());
    this.isLazy = isLazy;
}
```

三个构造方法形成一个调用链：简化版本委托给完整版本。最终版本保存所有参数，并创建 `EnhancerAffect` 来记录增强的影响范围。

### 4.4 enhance() 方法 —— 搜索匹配类并触发 retransform

`enhance()` 方法是增强流程的入口，它负责"找到要增强的类"和"触发 JVM 的 retransform"。

```java
public synchronized EnhancerAffect enhance(
        final Instrumentation inst,
        int maxNumOfMatchedClass)
        throws UnmodifiableClassException {
```

注意 `synchronized` 关键字——同一个 Enhancer 实例的 enhance 方法是同步的。

**第一步：搜索匹配的类**

```java
this.matchingClasses = GlobalOptions.isDisableSubClass
    ? SearchUtils.searchClass(inst, classNameMatcher)
    : SearchUtils.searchSubClass(inst,
          SearchUtils.searchClass(inst, classNameMatcher));
```

这里有两种搜索策略：
1. **禁用子类搜索（`isDisableSubClass = true`）**：只搜索精确匹配类名的类
2. **启用子类搜索（默认）**：先搜索精确匹配的类，然后搜索这些类的所有子类

`SearchUtils.searchClass()` 的实现是遍历 `inst.getAllLoadedClasses()` 所有已加载的类，用 `classNameMatcher` 进行匹配：

```java
public static Set<Class<?>> searchClass(
        Instrumentation inst,
        Matcher<String> classNameMatcher) {
    return searchClass(inst, classNameMatcher, Integer.MAX_VALUE);
}

public static Set<Class<?>> searchClass(
        Instrumentation inst,
        Matcher<String> classNameMatcher,
        int limit) {
    if (classNameMatcher == null) {
        return Collections.emptySet();
    }
    final Set<Class<?>> matches = new HashSet<Class<?>>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        if (clazz == null) {
            continue;
        }
        if (classNameMatcher.matching(clazz.getName())) {
            matches.add(clazz);
        }
        if (matches.size() >= limit) {
            break;
        }
    }
    return matches;
}
```

`searchSubClass()` 则再次遍历所有已加载的类，通过 `isAssignableFrom()` 判断继承关系：

```java
public static Set<Class<?>> searchSubClass(
        Instrumentation inst,
        Set<Class<?>> classSet) {
    final Set<Class<?>> matches = new HashSet<Class<?>>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        if (clazz == null) {
            continue;
        }
        for (Class<?> superClass : classSet) {
            if (superClass.isAssignableFrom(clazz)) {
                matches.add(clazz);
                break;
            }
        }
    }
    return matches;
}
```

**第二步：过滤不能被增强的类**

```java
List<Pair<Class<?>, String>> filtedList = filter(matchingClasses);
if (!filtedList.isEmpty()) {
    for (Pair<Class<?>, String> filted : filtedList) {
        logger.info("ignore class: {}, reason: {}",
            filted.getFirst().getName(),
            filted.getSecond());
    }
}
```

`filter()` 方法会检查并移除以下几类不能增强的类：

```java
private List<Pair<Class<?>, String>> filter(Set<Class<?>> classes) {
    List<Pair<Class<?>, String>> filteredClasses =
        new ArrayList<Pair<Class<?>, String>>();
    final Iterator<Class<?>> it = classes.iterator();
    while (it.hasNext()) {
        final Class<?> clazz = it.next();
        boolean removeFlag = false;

        if (null == clazz) {
            removeFlag = true;
        } else if (!isTargetClassLoader(clazz.getClassLoader())) {
            filteredClasses.add(new Pair<>(clazz,
                "classloader is not matched"));
            removeFlag = true;
        } else if (isSelf(clazz)) {
            filteredClasses.add(new Pair<>(clazz,
                "class loaded by arthas itself"));
            removeFlag = true;
        } else if (isUnsafeClass(clazz)) {
            filteredClasses.add(new Pair<>(clazz,
                "class loaded by Bootstrap Classloader, "
                + "try to execute `options unsafe true`"));
            removeFlag = true;
        } else if (isExclude(clazz)) {
            filteredClasses.add(new Pair<>(clazz,
                "class is excluded"));
            removeFlag = true;
        } else {
            Pair<Boolean, String> unsupportedResult =
                isUnsupportedClass(clazz);
            if (unsupportedResult.getFirst()) {
                filteredClasses.add(new Pair<>(clazz,
                    unsupportedResult.getSecond()));
                removeFlag = true;
            }
        }

        if (removeFlag) {
            it.remove();
        }
    }
    return filteredClasses;
}
```

过滤规则汇总：

| 检查 | 条件 | 原因 |
|------|------|------|
| null 类 | `clazz == null` | 防止 NPE |
| ClassLoader 不匹配 | 指定了 `targetClassLoaderHash` 但不匹配 | 只增强指定 ClassLoader 加载的类 |
| Arthas 自身的类 | `clazz.getClassLoader() == selfClassLoader` | 增强自己会导致死循环 |
| Bootstrap 类（unsafe） | `clazz.getClassLoader() == null && !isUnsafe` | 默认不增强 JDK 核心类，需要 `options unsafe true` |
| 排除匹配 | `classNameExcludeMatcher.matching()` | 用户通过 `--exclude-class-pattern` 排除 |
| Lambda 类 | `ClassUtils.isLambdaClass(clazz)` | Lambda 类不支持增强 |
| 接口 | `clazz.isInterface()` | 默认不增强接口 |
| Integer/Class/Method | 特定系统类 | 增强这些类会导致 JVM 不稳定 |
| 数组 | `clazz.isArray()` | 数组类不支持增强 |

**第三步：注册 Transformer 并触发 retransform**

```java
affect.setTransformer(this);

try {
    ArthasBootstrap.getInstance()
        .getTransformerManager()
        .addTransformer(this, isTracing);

    if (isLazy) {
        ArthasBootstrap.getInstance()
            .getTransformerManager()
            .addLazyTransformer(this);
        logger.info("Lazy mode enabled, "
            + "transformer added to lazy transformer list");
    }

    if (GlobalOptions.isBatchReTransform) {
        final int size = matchingClasses.size();
        final Class<?>[] classArray = new Class<?>[size];
        arraycopy(matchingClasses.toArray(), 0,
                  classArray, 0, size);
        if (classArray.length > 0) {
            inst.retransformClasses(classArray);
        }
    } else {
        for (Class<?> clazz : matchingClasses) {
            try {
                inst.retransformClasses(clazz);
            } catch (Throwable t) {
                logger.warn("retransform {} failed.", clazz, t);
                // 异常处理...
            }
        }
    }
} catch (Throwable e) {
    logger.error("Enhancer error, matchingClasses: {}",
                 matchingClasses, e);
    affect.setThrowable(e);
}

return affect;
```

这段代码做了三件关键的事：

1. **将 this（Enhancer）添加到 TransformerManager**：根据 `isTracing` 标志，放入 `watchTransformers` 或 `traceTransformers` 列表。如果是懒加载模式，还同时放入 `lazyTransformers`。

2. **调用 `inst.retransformClasses()`**：这是触发 JVM 回调的关键一步。JVM 收到这个请求后，会取出这些类的原始字节码，然后依次调用所有 retransform-capable 的 `ClassFileTransformer`——这就会触发 `TransformerManager` 中注册的复合 transformer，进而调用到我们的 `Enhancer.transform()` 方法。

3. **批量 vs 逐个 retransform**：根据 `GlobalOptions.isBatchReTransform` 决定是一次性 retransform 所有类，还是逐个进行。逐个进行可以更好地隔离错误：如果某个类 retransform 失败，不会影响其他类。

### 4.5 transform() 方法 —— 字节码织入的核心逻辑

这是整个 Enhancer 最核心的方法。当 JVM 执行 retransform 时，会通过 `TransformerManager` 的复合 transformer 调用到这个方法。让我们逐段分析。

**方法签名：**

```java
@Override
public byte[] transform(
        final ClassLoader inClassLoader,
        String className,
        Class<?> classBeingRedefined,
        ProtectionDomain protectionDomain,
        byte[] classfileBuffer)
        throws IllegalClassFormatException {
```

**第一步：检查 ClassLoader 能否加载 SpyAPI**

```java
try {
    if (inClassLoader != null) {
        inClassLoader.loadClass(SpyAPI.class.getName());
    }
} catch (Throwable e) {
    logger.error(
        "the classloader can not load SpyAPI, ignore it. "
        + "classloader: {}, className: {}",
        inClassLoader.getClass().getName(), className, e);
    return null;
}
```

**这一步做了什么？** 尝试用目标类的 ClassLoader 加载 `SpyAPI` 类。如果加载失败，则放弃增强。

**为什么需要这个检查？**

增强后的字节码中会插入对 `SpyAPI.atEnter()` 等方法的调用。这些调用是静态方法调用，运行时需要由目标类的 ClassLoader 能够找到 `SpyAPI` 类。`SpyAPI` 被设计为由 BootstrapClassLoader 加载（包名为 `java.arthas`），理论上所有 ClassLoader 都能看到它。但在某些特殊的 ClassLoader 隔离场景下（例如 OSGi 或某些自定义 ClassLoader），可能无法加载到 `SpyAPI`，此时增强是无意义的。

**第二步：matchingClasses 过滤（含懒加载逻辑）**

```java
if (matchingClasses != null
        && !matchingClasses.contains(classBeingRedefined)) {
    if (isLazy && classBeingRedefined == null
            && className != null) {
        String classNameDot = className.replace('/', '.');
        if (!classNameMatcher.matching(classNameDot)) {
            return null;
        }
        if (classNameExcludeMatcher != null
                && classNameExcludeMatcher.matching(classNameDot)) {
            return null;
        }
        if (!isTargetClassLoader(inClassLoader)) {
            return null;
        }
        if (inClassLoader != null
                && isEquals(inClassLoader, selfClassLoader)) {
            return null;
        }
        if (!GlobalOptions.isUnsafe && inClassLoader == null) {
            return null;
        }
        logger.info("Lazy mode: enhancing newly loaded class: {}",
                     classNameDot);
    } else {
        return null;
    }
}
```

源码中有一段重要的注释：`这里要再次过滤一次，为啥？因为在transform的过程中，有可能还会再诞生新的类，所以需要将之前需要转换的类集合传递下来，再次进行判断`。

这段逻辑的核心意思是：

1. 如果 `matchingClasses` 已经确定了（通过 `enhance()` 方法搜索出来的），但当前被 transform 的类不在这个集合中，通常应该跳过（返回 `null`）。
2. **但是**，如果是懒加载模式（`isLazy = true`），并且是类首次加载（`classBeingRedefined == null`），那就不能简单跳过——这可能正是用户等待的那个类。此时需要重新进行一系列匹配检查：类名匹配、排除匹配、ClassLoader 匹配、是否是 Arthas 自身的类、是否是 unsafe 类。

**第三步：ASM 解析与 JSR 指令移除**

```java
ClassNode classNode = new ClassNode(Opcodes.ASM9);
ClassReader classReader =
    AsmUtils.toClassNode(classfileBuffer, classNode);
classNode = AsmUtils.removeJSRInstructions(classNode);
```

- `ClassReader`：ASM 的字节码读取器，将原始字节码解析为 ASM 的内部表示
- `ClassNode`：ASM 的树形 API 表示，将整个类结构建模为一个对象树，方便操作
- `removeJSRInstructions()`：移除 JSR（Jump SubRoutine）指令。JSR 是 Java 早期版本使用的指令，在新版本中已经不推荐使用。Arthas 移除它是为了简化后续的字节码操作，避免复杂的控制流分析。这解决了 GitHub issue #1304。

**第四步：解析拦截器模板**

```java
DefaultInterceptorClassParser defaultInterceptorClassParser =
    new DefaultInterceptorClassParser();

final List<InterceptorProcessor> interceptorProcessors =
    new ArrayList<InterceptorProcessor>();

interceptorProcessors.addAll(
    defaultInterceptorClassParser.parse(SpyInterceptor1.class));
interceptorProcessors.addAll(
    defaultInterceptorClassParser.parse(SpyInterceptor2.class));
interceptorProcessors.addAll(
    defaultInterceptorClassParser.parse(SpyInterceptor3.class));
```

`DefaultInterceptorClassParser` 来自 `bytekit` 库（阿里的字节码增强工具库），它解析拦截器类上的注解（如 `@AtEnter`、`@AtExit`、`@AtExceptionExit`），生成对应的 `InterceptorProcessor`。每个 processor 知道如何在字节码的特定位置（方法入口、方法出口、异常出口）插入代码。

这三个拦截器是基础拦截器，所有增强命令都需要它们：

| 拦截器 | 注解 | 作用 |
|--------|------|------|
| `SpyInterceptor1` | `@AtEnter` | 方法入口处插入 `SpyAPI.atEnter()` 调用 |
| `SpyInterceptor2` | `@AtExit` | 方法正常返回处插入 `SpyAPI.atExit()` 调用 |
| `SpyInterceptor3` | `@AtExceptionExit` | 方法异常退出处插入 `SpyAPI.atExceptionExit()` 调用 |

**第五步：解析行号拦截器（如果需要）**

```java
final List<InterceptorProcessor> lineInterceptorProcessors =
    new ArrayList<InterceptorProcessor>();
if (isLineEnhance()) {
    lineInterceptorProcessors.addAll(
        defaultInterceptorClassParser.parse(
            SpyLineInterceptor.class));
    for (InterceptorProcessor interceptorProcessor
            : lineInterceptorProcessors) {
        interceptorProcessor.setLocationMatcher(
            new LineLocationMatcher(
                lineEnhanceOptions.getMode(),
                lineEnhanceOptions.getDuplicatePolicy(),
                lineEnhanceOptions.getLineList()));
    }
}
```

如果启用了行号增强（`lineEnhanceOptions` 不为空且包含要增强的行号），则解析 `SpyLineInterceptor`，并为每个 processor 设置行号匹配器。

**第六步：解析 Trace 拦截器（如果需要）**

```java
if (this.isTracing) {
    if (!this.skipJDKTrace) {
        interceptorProcessors.addAll(
            defaultInterceptorClassParser.parse(
                SpyTraceInterceptor1.class));
        interceptorProcessors.addAll(
            defaultInterceptorClassParser.parse(
                SpyTraceInterceptor2.class));
        interceptorProcessors.addAll(
            defaultInterceptorClassParser.parse(
                SpyTraceInterceptor3.class));
    } else {
        interceptorProcessors.addAll(
            defaultInterceptorClassParser.parse(
                SpyTraceExcludeJDKInterceptor1.class));
        interceptorProcessors.addAll(
            defaultInterceptorClassParser.parse(
                SpyTraceExcludeJDKInterceptor2.class));
        interceptorProcessors.addAll(
            defaultInterceptorClassParser.parse(
                SpyTraceExcludeJDKInterceptor3.class));
    }
}
```

只有当 `isTracing` 为 `true`（即 `AdviceListener` 实现了 `InvokeTraceable` 接口，典型如 trace 命令）时，才添加 trace 相关的拦截器。

根据 `skipJDKTrace` 选择不同版本的 trace 拦截器：

| skipJDKTrace | 拦截器版本 | 排除规则 |
|-------------|-----------|----------|
| `false` | `SpyTraceInterceptor1/2/3` | 只排除 `SpyAPI` 和装箱类型 |
| `true` | `SpyTraceExcludeJDKInterceptor1/2/3` | 排除所有 `java.**` 包 |

**第七步：匹配方法**

```java
List<MethodNode> matchedMethods = new ArrayList<MethodNode>();
for (MethodNode methodNode : classNode.methods) {
    if (isLineEnhance()) {
        if (isLineMethodMatched(methodNode)) {
            matchedMethods.add(methodNode);
        }
    } else if (!isIgnore(methodNode, methodNameMatcher)) {
        matchedMethods.add(methodNode);
    }
}
```

遍历类中的所有方法，通过匹配器筛选出需要增强的方法。

`isIgnore()` 方法的逻辑：

```java
private boolean isIgnore(MethodNode methodNode,
                         Matcher methodNameMatcher) {
    return null == methodNode
        || isAbstract(methodNode.access)
        || !methodNameMatcher.matching(methodNode.name)
        || ArthasCheckUtils.isEquals(
               methodNode.name, "<clinit>");
}
```

忽略以下方法：
- `null` 方法节点
- 抽象方法（没有方法体，无法插桩）
- 方法名不匹配
- `<clinit>` 类初始化方法（增强可能导致类加载问题）

注意：`<init>` 构造方法是**不会被忽略**的，Arthas 支持对构造方法进行增强。

**第八步：修复 CGLIB 代理类的构造方法异常表**

```java
if (AsmUtils.isEnhancerByCGLIB(className)) {
    for (MethodNode methodNode : matchedMethods) {
        if (AsmUtils.isConstructor(methodNode)) {
            AsmUtils.fixConstructorExceptionTable(methodNode);
        }
    }
}
```

CGLIB 生成的代理类的构造方法可能有异常表的问题（GitHub issue #1690），需要修复后才能正确增强。

**第九步：创建 GroupLocationFilter 防止重复织入**

```java
GroupLocationFilter groupLocationFilter =
    new GroupLocationFilter();

LocationFilter enterFilter =
    new InvokeContainLocationFilter(
        Type.getInternalName(SpyAPI.class),
        "atEnter", LocationType.ENTER);
LocationFilter existFilter =
    new InvokeContainLocationFilter(
        Type.getInternalName(SpyAPI.class),
        "atExit", LocationType.EXIT);
LocationFilter exceptionFilter =
    new InvokeContainLocationFilter(
        Type.getInternalName(SpyAPI.class),
        "atExceptionExit", LocationType.EXCEPTION_EXIT);

groupLocationFilter.addFilter(enterFilter);
groupLocationFilter.addFilter(existFilter);
groupLocationFilter.addFilter(exceptionFilter);

LocationFilter invokeBeforeFilter =
    new InvokeCheckLocationFilter(
        Type.getInternalName(SpyAPI.class),
        "atBeforeInvoke", LocationType.INVOKE);
LocationFilter invokeAfterFilter =
    new InvokeCheckLocationFilter(
        Type.getInternalName(SpyAPI.class),
        "atInvokeException", LocationType.INVOKE_COMPLETED);
LocationFilter invokeExceptionFilter =
    new InvokeCheckLocationFilter(
        Type.getInternalName(SpyAPI.class),
        "atInvokeException", LocationType.INVOKE_EXCEPTION_EXIT);
groupLocationFilter.addFilter(invokeBeforeFilter);
groupLocationFilter.addFilter(invokeAfterFilter);
groupLocationFilter.addFilter(invokeExceptionFilter);
```

**这一步做了什么？** 创建了一组位置过滤器，用于检查字节码中是否已经包含了 SpyAPI 的调用。

**为什么需要防止重复织入？**

考虑以下场景：用户先执行了 `watch com.example.MyService process`，然后又执行了 `trace com.example.MyService process`。两个命令都会对同一个方法进行增强。如果不检查重复，方法入口处会被插入两次 `SpyAPI.atEnter()` 调用，这不仅浪费性能，还可能导致回调逻辑混乱。

`InvokeContainLocationFilter` 会检查目标位置（如方法入口）的字节码中是否已经包含了对 `SpyAPI.atEnter()` 的调用。如果已经有了，就跳过这个位置。

`InvokeCheckLocationFilter` 用于检查子调用级别的 trace 插桩是否已存在。

**第十步：逐方法处理——字节码织入**

```java
for (MethodNode methodNode : matchedMethods) {
    if (AsmUtils.isNative(methodNode)) {
        logger.info("ignore native method: {}",
            AsmUtils.methodDeclaration(
                Type.getObjectType(classNode.name),
                methodNode));
        continue;
    }
```

首先跳过 native 方法（没有 Java 字节码，无法插桩）。

```java
    // 先查找是否有 atBeforeInvoke 函数，
    // 如果有，则说明已经有trace了，
    // 则直接不再尝试增强，直接插入 listener
    if (AsmUtils.containsMethodInsnNode(methodNode,
            Type.getInternalName(SpyAPI.class),
            "atBeforeInvoke")) {
        for (AbstractInsnNode insnNode =
                methodNode.instructions.getFirst();
                insnNode != null;
                insnNode = insnNode.getNext()) {
            if (insnNode instanceof MethodInsnNode) {
                final MethodInsnNode methodInsnNode =
                    (MethodInsnNode) insnNode;
                if (this.skipJDKTrace) {
                    if (methodInsnNode.owner
                            .startsWith("java/")) {
                        continue;
                    }
                }
                if (AsmOpUtils.isBoxType(
                        Type.getObjectType(
                            methodInsnNode.owner))) {
                    continue;
                }
                AdviceListenerManager
                    .registerTraceAdviceListener(
                        inClassLoader, className,
                        methodInsnNode.owner,
                        methodInsnNode.name,
                        methodInsnNode.desc, listener);
            }
        }
    } else {
        MethodProcessor methodProcessor =
            new MethodProcessor(classNode, methodNode,
                                groupLocationFilter);
        for (InterceptorProcessor interceptor
                : interceptorProcessors) {
            try {
                List<Location> locations =
                    interceptor.process(methodProcessor);
                for (Location location : locations) {
                    if (location
                            instanceof MethodInsnNodeWare) {
                        MethodInsnNodeWare ware =
                            (MethodInsnNodeWare) location;
                        MethodInsnNode methodInsnNode =
                            ware.methodInsnNode();
                        AdviceListenerManager
                            .registerTraceAdviceListener(
                                inClassLoader, className,
                                methodInsnNode.owner,
                                methodInsnNode.name,
                                methodInsnNode.desc,
                                listener);
                    }
                }
            } catch (Throwable e) {
                logger.error(
                    "enhancer error, class: {}, "
                    + "method: {}, interceptor: {}",
                    classNode.name, methodNode.name,
                    interceptor.getClass().getName(), e);
            }
        }
    }
```

这段逻辑分为两个分支：

**分支一：已经有 trace 插桩**

如果方法中已经包含了 `SpyAPI.atBeforeInvoke()` 的调用，说明之前已经有 trace 命令增强过了。此时不再重复插入 trace 代码，只需要为每个子调用注册当前的 listener 即可。这样多个 trace/watch 命令可以共享同一套 trace 插桩代码，通过 `AdviceListenerManager` 注册多个 listener 来实现多路复用。

**分支二：全新增强**

如果方法中没有 trace 插桩，则使用 `MethodProcessor` 和 `InterceptorProcessor` 进行完整的字节码织入：

1. `MethodProcessor` 封装了对方法字节码的操作能力
2. `interceptor.process(methodProcessor)` 在字节码的适当位置插入拦截代码
3. 对于 trace 相关的位置（`MethodInsnNodeWare`），注册 trace 监听器

**第十一步：注册 enter/exit 监听器和行号监听器**

```java
    // enter/exit 总是要插入 listener
    AdviceListenerManager.registerAdviceListener(
        inClassLoader, className,
        methodNode.name, methodNode.desc, listener);

    if (isLineEnhance()) {
        for (Integer lineNumber : lineNumbers) {
            AdviceListenerManager.registerLineAdviceListener(
                inClassLoader, className,
                methodNode.name, methodNode.desc,
                lineNumber, listener);
        }
    }

    affect.addMethodAndCount(inClassLoader, className,
        methodNode.name, methodNode.desc);
}
```

无论是否有 trace，enter/exit 的 listener 都必须注册。这确保了方法被调用时，`SpyImpl.atEnter()` 能通过 `AdviceListenerManager.queryAdviceListeners()` 找到正确的 listener。

**第十二步：版本升级与字节码生成**

```java
// V1_5 的 major version 是 49
if (AsmUtils.getMajorVersion(classNode.version) < 49) {
    classNode.version =
        AsmUtils.setMajorVersion(classNode.version, 49);
}

byte[] enhanceClassByteArray =
    AsmUtils.toBytes(classNode, inClassLoader, classReader);
```

如果类的字节码版本低于 Java 1.5（major version 49），需要升级到 49。这是因为 Arthas 插入的代码可能使用了 Java 1.5 以上的特性（GitHub issue #1223）。

`AsmUtils.toBytes()` 将修改后的 `ClassNode` 树转换回字节码数组。

**第十三步：缓存与 dump**

```java
// 增强成功，记录类
classBytesCache.put(classBeingRedefined, new Object());

// dump the class
dumpClassIfNecessary(className, enhanceClassByteArray, affect);

// 成功计数
affect.cCnt(1);

return enhanceClassByteArray;
```

- `classBytesCache` 记录哪些类被增强过（值是一个空对象，只关心键是否存在），供 `reset()` 方法使用
- `dumpClassIfNecessary()` 如果启用了 dump 选项（`GlobalOptions.isDump`），将增强后的字节码写入文件，方便调试
- `affect.cCnt(1)` 增加成功计数
- 返回增强后的字节码数组，JVM 会用这个字节码替换原来的字节码

### 4.6 reset() 方法 —— 还原字节码

```java
public static synchronized EnhancerAffect reset(
        final Instrumentation inst,
        final Matcher classNameMatcher)
        throws UnmodifiableClassException {

    final EnhancerAffect affect = new EnhancerAffect();
    final Set<Class<?>> enhanceClassSet =
        new HashSet<Class<?>>();

    for (Class<?> classInCache : classBytesCache.keySet()) {
        if (classNameMatcher.matching(classInCache.getName())) {
            enhanceClassSet.add(classInCache);
        }
    }

    try {
        enhance(inst, enhanceClassSet);
        logger.info("Success to reset classes: "
            + enhanceClassSet);
    } finally {
        for (Class<?> resetClass : enhanceClassSet) {
            classBytesCache.remove(resetClass);
            affect.cCnt(1);
        }
    }

    return affect;
}
```

`reset()` 方法通过以下步骤还原字节码：

1. 从 `classBytesCache` 中找到所有匹配的已增强类
2. 调用 `enhance(inst, enhanceClassSet)` 即 `inst.retransformClasses(classArray)` 触发 retransform
3. 由于此时 `TransformerManager` 中对应的 `Enhancer` 已经被移除了（命令结束时会调用 `removeTransformer`），所以没有任何 transformer 会修改字节码，JVM 会使用原始字节码，从而实现还原
4. 从缓存中移除这些类的记录

这个设计非常巧妙：不需要保存原始字节码，只需要移除 transformer 后重新 retransform，JVM 就会自动使用原始字节码。

### 4.7 dumpClassIfNecessary() —— 导出增强后的类文件

```java
private static void dumpClassIfNecessary(
        String className, byte[] data,
        EnhancerAffect affect) {
    if (!GlobalOptions.isDump) {
        return;
    }
    final File dumpClassFile =
        new File("./arthas-class-dump/"
                 + className + ".class");
    final File classPath =
        new File(dumpClassFile.getParent());

    if (!classPath.mkdirs() && !classPath.exists()) {
        logger.warn(
            "create dump classpath:{} failed.", classPath);
        return;
    }

    try {
        FileUtils.writeByteArrayToFile(dumpClassFile, data);
        affect.addClassDumpFile(dumpClassFile);
    } catch (IOException e) {
        logger.warn(
            "dump class:{} to file {} failed.",
            className, dumpClassFile, e);
    }
}
```

当用户执行 `options dump true` 后，增强后的类文件会被导出到 `./arthas-class-dump/` 目录。这对于调试字节码增强问题非常有用——你可以用 `javap -c` 反编译查看插入的代码。

---

## 第五阶段：SpyInterceptors —— 拦截器模板定义

### 5.1 SpyInterceptors 类 —— 架构概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/SpyInterceptors.java`

`SpyInterceptors` 是一个纯粹的模板定义类，它包含了多个静态内部类，每个内部类定义了一种拦截行为的模板。这些模板通过 bytekit 的注解来声明"在什么位置"插入"什么代码"。

bytekit 的工作原理类似于 AspectJ，但在字节码层面工作：

1. 开发者用注解声明拦截意图（@AtEnter, @AtExit 等）
2. bytekit 解析这些注解，生成 `InterceptorProcessor`
3. `InterceptorProcessor` 在目标方法的字节码中插入对应的调用

### 5.2 SpyInterceptor1 —— 方法入口拦截

```java
public static class SpyInterceptor1 {
    @AtEnter(inline = true)
    public static void atEnter(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.MethodInfo String methodInfo,
            @Binding.Args Object[] args) {
        SpyAPI.atEnter(clazz, methodInfo, target, args);
    }
}
```

**注解解析：**

- `@AtEnter`：在方法入口处插入代码
- `inline = true`：将这个方法的字节码**内联**到目标方法中，而不是生成一个方法调用。内联的好处是避免了额外的方法调用开销，也避免了在栈帧上留下 Arthas 相关的信息

**@Binding 注解详解：**

| Binding 注解 | 含义 | 运行时绑定的值 |
|-------------|------|---------------|
| `@Binding.This` | 方法所属对象实例 | `this` 指针，静态方法为 `null` |
| `@Binding.Class` | 方法所属类 | 目标类的 `Class` 对象 |
| `@Binding.MethodInfo` | 方法信息 | 格式为 `methodName|methodDesc` 的字符串 |
| `@Binding.Args` | 方法参数 | 所有参数打包成 `Object[]` |

**inline = true 的含义深入解释：**

假设目标方法是：
```java
public String process(String input) {
    return input.toUpperCase();
}
```

增强后（概念上）等价于：
```java
public String process(String input) {
    // 内联的 SpyInterceptor1.atEnter() 代码
    SpyAPI.atEnter(MyService.class, "process|(Ljava/lang/String;)Ljava/lang/String;", this, new Object[]{input});
    
    return input.toUpperCase();
}
```

注意：由于 `inline = true`，字节码中不会出现对 `SpyInterceptor1.atEnter()` 的调用，而是直接出现对 `SpyAPI.atEnter()` 的调用。

### 5.3 SpyInterceptor2 —— 方法正常返回拦截

```java
public static class SpyInterceptor2 {
    @AtExit(inline = true)
    public static void atExit(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.MethodInfo String methodInfo,
            @Binding.Args Object[] args,
            @Binding.Return Object returnObj) {
        SpyAPI.atExit(clazz, methodInfo,
                      target, args, returnObj);
    }
}
```

与 `SpyInterceptor1` 类似，但：
- `@AtExit`：在方法正常返回前插入代码（即 return 语句之前）
- 多了 `@Binding.Return Object returnObj`：绑定方法的返回值

### 5.4 SpyInterceptor3 —— 方法异常退出拦截

```java
public static class SpyInterceptor3 {
    @AtExceptionExit(inline = true)
    public static void atExceptionExit(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.MethodInfo String methodInfo,
            @Binding.Args Object[] args,
            @Binding.Throwable Throwable throwable) {
        SpyAPI.atExceptionExit(clazz, methodInfo,
                               target, args, throwable);
    }
}
```

- `@AtExceptionExit`：在方法因异常退出时插入代码（即异常从方法中抛出之前）
- `@Binding.Throwable Throwable throwable`：绑定抛出的异常对象

### 5.5 SpyLineInterceptor —— 行号拦截

```java
public static class SpyLineInterceptor {
    @AtLine(lines = { -1 }, inline = true)
    public static void atLine(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.MethodInfo String methodInfo,
            @Binding.Line int lineNumber,
            @Binding.Args Object[] args,
            @Binding.ArgNames(optional = true)
                String[] argNames,
            @Binding.LocalVars(ignoreThis = true,
                optional = true) Object[] localVars,
            @Binding.LocalVarNames(ignoreThis = true,
                optional = true) String[] localVarNames) {
        SpyAPI.atLine(clazz, methodInfo, lineNumber,
                      target, args, argNames,
                      localVars, localVarNames);
    }
}
```

- `@AtLine(lines = { -1 })`：在指定行号处插入代码。`-1` 是占位符，实际行号在运行时通过 `LineLocationMatcher` 动态确定
- `@Binding.Line`：绑定当前执行的源码行号
- `@Binding.ArgNames`：绑定参数名称（需要 debug info）
- `@Binding.LocalVars`：绑定当前行可见的局部变量值
- `@Binding.LocalVarNames`：绑定当前行可见的局部变量名
- `optional = true`：这些绑定是可选的，如果字节码中没有调试信息，不会导致增强失败
- `ignoreThis = true`：局部变量中忽略 `this`（因为 `this` 已经通过 `@Binding.This` 单独绑定了）

### 5.6 SpyTraceInterceptor1 —— 子调用前拦截（不排除 JDK）

```java
public static class SpyTraceInterceptor1 {
    @AtInvoke(name = "", inline = true,
              whenComplete = false,
              excludes = {"java.arthas.SpyAPI",
                          "java.lang.Byte",
                          "java.lang.Boolean",
                          "java.lang.Short",
                          "java.lang.Character",
                          "java.lang.Integer",
                          "java.lang.Float",
                          "java.lang.Long",
                          "java.lang.Double"})
    public static void onInvoke(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.InvokeInfo String invokeInfo) {
        SpyAPI.atBeforeInvoke(clazz, invokeInfo, target);
    }
}
```

**注解解析：**

- `@AtInvoke`：在方法内部的子调用处插入代码
- `name = ""`：匹配所有方法调用（空字符串表示不过滤）
- `whenComplete = false`：在调用**之前**插入（而非调用完成后）
- `excludes`：排除对这些类的方法调用的拦截

**为什么要排除装箱类型（Byte, Boolean, Short, Character, Integer, Float, Long, Double）？**

装箱类型的方法调用（如 `Integer.valueOf()`）在 Java 中极其频繁。当方法参数是基本类型时，编译器会自动插入自动装箱代码。如果不排除这些调用，trace 的输出会充斥大量无意义的装箱调用，严重影响可读性和性能。

**为什么要排除 `java.arthas.SpyAPI`？**

因为增强后的代码已经包含了对 `SpyAPI` 的调用。如果不排除，trace 会把这些 Arthas 自己插入的调用也记录下来，产生无限递归。

- `@Binding.InvokeInfo String invokeInfo`：绑定子调用的信息，格式为 `owner|methodName|methodDesc|lineNumber`

### 5.7 SpyTraceInterceptor2 —— 子调用后拦截

```java
public static class SpyTraceInterceptor2 {
    @AtInvoke(name = "", inline = true,
              whenComplete = true,
              excludes = {"java.arthas.SpyAPI",
                          "java.lang.Byte",
                          /* ... 其他装箱类型 ... */})
    public static void onInvokeAfter(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.InvokeInfo String invokeInfo) {
        SpyAPI.atAfterInvoke(clazz, invokeInfo, target);
    }
}
```

与 `SpyTraceInterceptor1` 的区别：`whenComplete = true`，表示在子调用**完成后**插入代码。

### 5.8 SpyTraceInterceptor3 —— 子调用异常拦截

```java
public static class SpyTraceInterceptor3 {
    @AtInvokeException(name = "", inline = true,
              excludes = {"java.arthas.SpyAPI",
                          "java.lang.Byte",
                          /* ... 其他装箱类型 ... */})
    public static void onInvokeException(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.InvokeInfo String invokeInfo,
            @Binding.Throwable Throwable throwable) {
        SpyAPI.atInvokeException(
            clazz, invokeInfo, target, throwable);
    }
}
```

- `@AtInvokeException`：在子调用抛出异常时插入代码

### 5.9 SpyTraceExcludeJDKInterceptor1/2/3 —— 排除 JDK 的版本

```java
public static class SpyTraceExcludeJDKInterceptor1 {
    @AtInvoke(name = "", inline = true,
              whenComplete = false,
              excludes = "java.**")
    public static void onInvoke(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.InvokeInfo String invokeInfo) {
        SpyAPI.atBeforeInvoke(clazz, invokeInfo, target);
    }
}

public static class SpyTraceExcludeJDKInterceptor2 {
    @AtInvoke(name = "", inline = true,
              whenComplete = true,
              excludes = "java.**")
    public static void onInvokeAfter(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.InvokeInfo String invokeInfo) {
        SpyAPI.atAfterInvoke(clazz, invokeInfo, target);
    }
}

public static class SpyTraceExcludeJDKInterceptor3 {
    @AtInvokeException(name = "", inline = true,
              excludes = "java.**")
    public static void onInvokeException(
            @Binding.This Object target,
            @Binding.Class Class<?> clazz,
            @Binding.InvokeInfo String invokeInfo,
            @Binding.Throwable Throwable throwable) {
        SpyAPI.atInvokeException(
            clazz, invokeInfo, target, throwable);
    }
}
```

与非 ExcludeJDK 版本的唯一区别：`excludes = "java.**"`。这会排除所有 `java` 包下的方法调用，包括 `java.util.*`、`java.io.*` 等。当用户执行 `trace --skipJDKTrace` 时会使用这个版本。

### 5.10 拦截器对比总表

| 拦截器类 | 注解 | 位置 | 调用的 SpyAPI 方法 | 排除规则 |
|---------|------|------|-------------------|----------|
| SpyInterceptor1 | @AtEnter | 方法入口 | atEnter() | 无 |
| SpyInterceptor2 | @AtExit | 方法正常返回 | atExit() | 无 |
| SpyInterceptor3 | @AtExceptionExit | 方法异常退出 | atExceptionExit() | 无 |
| SpyLineInterceptor | @AtLine | 指定行号 | atLine() | 无 |
| SpyTraceInterceptor1 | @AtInvoke(complete=false) | 子调用前 | atBeforeInvoke() | SpyAPI + 装箱类 |
| SpyTraceInterceptor2 | @AtInvoke(complete=true) | 子调用后 | atAfterInvoke() | SpyAPI + 装箱类 |
| SpyTraceInterceptor3 | @AtInvokeException | 子调用异常 | atInvokeException() | SpyAPI + 装箱类 |
| SpyTraceExcludeJDK1 | @AtInvoke(complete=false) | 子调用前 | atBeforeInvoke() | java.** |
| SpyTraceExcludeJDK2 | @AtInvoke(complete=true) | 子调用后 | atAfterInvoke() | java.** |
| SpyTraceExcludeJDK3 | @AtInvokeException | 子调用异常 | atInvokeException() | java.** |

---

## 第六阶段：SpyAPI 与 SpyImpl —— 跨 ClassLoader 的桥梁

### 6.1 SpyAPI —— 位于 BootstrapClassLoader 的间谍入口

**源码位置**: `arthas/spy/src/main/java/java/arthas/SpyAPI.java`

这是整个 Arthas 字节码增强体系中最精妙的设计之一。让我们从包名开始分析。

**包名为什么是 `java.arthas`？**

```java
package java.arthas;
```

这个包名的选择绝非随意。以 `java.` 开头的包由 BootstrapClassLoader 加载。Arthas 将 `spy.jar`（包含 `SpyAPI` 类）追加到 BootstrapClassLoader 的搜索路径中（通过 `Instrumentation.appendToBootstrapClassLoaderSearch()`），使得 `SpyAPI` 类对所有 ClassLoader 都可见。

用一个类比来理解：`SpyAPI` 就像是插在"操作系统层"的窃听器，而不是插在某个"应用程序"中。因为"操作系统"（BootstrapClassLoader）加载的类对所有"应用程序"（其他 ClassLoader）都可见，所以任何被增强的类都能找到并调用 `SpyAPI` 的方法。

### 6.2 SpyAPI 核心结构

```java
public class SpyAPI {
    public static final AbstractSpy NOPSPY = new NopSpy();
    private static volatile AbstractSpy spyInstance = NOPSPY;
    public static volatile boolean INITED;
```

**关键字段解析：**

| 字段 | 类型 | 含义 |
|------|------|------|
| `NOPSPY` | `AbstractSpy` | 空操作实现，所有方法都是空方法体 |
| `spyInstance` | `AbstractSpy` | 当前生效的 Spy 实现（volatile 保证可见性） |
| `INITED` | `boolean` | 是否已初始化 |

**为什么需要 NOPSPY？**

`NOPSPY` 的存在是一个安全网。当 Arthas 还没有完全启动时，或者在 Arthas 退出（`destroy()`）后，`spyInstance` 会回到 `NOPSPY`。此时即使有增强过的字节码还在运行，调用 `SpyAPI.atEnter()` 也只是调用到空方法，不会产生任何副作用。

### 6.3 SpyAPI 的委托方法

```java
public static void atEnter(Class<?> clazz,
        String methodInfo, Object target, Object[] args) {
    spyInstance.atEnter(clazz, methodInfo, target, args);
}

public static void atExit(Class<?> clazz,
        String methodInfo, Object target,
        Object[] args, Object returnObject) {
    spyInstance.atExit(clazz, methodInfo, target,
                       args, returnObject);
}

public static void atExceptionExit(Class<?> clazz,
        String methodInfo, Object target,
        Object[] args, Throwable throwable) {
    spyInstance.atExceptionExit(clazz, methodInfo, target,
                                args, throwable);
}

public static void atBeforeInvoke(Class<?> clazz,
        String invokeInfo, Object target) {
    spyInstance.atBeforeInvoke(clazz, invokeInfo, target);
}

public static void atAfterInvoke(Class<?> clazz,
        String invokeInfo, Object target) {
    spyInstance.atAfterInvoke(clazz, invokeInfo, target);
}

public static void atInvokeException(Class<?> clazz,
        String invokeInfo, Object target,
        Throwable throwable) {
    spyInstance.atInvokeException(clazz, invokeInfo,
                                  target, throwable);
}

public static void atLine(Class<?> clazz,
        String methodInfo, int lineNumber,
        Object target, Object[] args,
        String[] argNames, Object[] localVars,
        String[] localVarNames) {
    spyInstance.atLine(clazz, methodInfo, lineNumber,
                       target, args, argNames,
                       localVars, localVarNames);
}
```

所有方法都是简单的委托：调用 `spyInstance` 对应的方法。`SpyAPI` 本身不包含任何业务逻辑。

### 6.4 AbstractSpy —— Spy 的抽象基类

```java
public static abstract class AbstractSpy {
    public abstract void atEnter(
            Class<?> clazz, String methodInfo,
            Object target, Object[] args);

    public abstract void atExit(
            Class<?> clazz, String methodInfo,
            Object target, Object[] args,
            Object returnObject);

    public abstract void atExceptionExit(
            Class<?> clazz, String methodInfo,
            Object target, Object[] args,
            Throwable throwable);

    public abstract void atBeforeInvoke(
            Class<?> clazz, String invokeInfo,
            Object target);

    public abstract void atAfterInvoke(
            Class<?> clazz, String invokeInfo,
            Object target);

    public abstract void atInvokeException(
            Class<?> clazz, String invokeInfo,
            Object target, Throwable throwable);

    public abstract void atLine(
            Class<?> clazz, String methodInfo,
            int lineNumber, Object target,
            Object[] args, String[] argNames,
            Object[] localVars, String[] localVarNames);
}
```

定义了七种拦截点的抽象方法。

### 6.5 NopSpy —— 空操作实现

```java
static class NopSpy extends AbstractSpy {
    @Override
    public void atEnter(Class<?> clazz, String methodInfo,
            Object target, Object[] args) {
    }

    @Override
    public void atExit(Class<?> clazz, String methodInfo,
            Object target, Object[] args,
            Object returnObject) {
    }

    // ... 其他方法同样是空方法体 ...
}
```

所有方法都是空的。这是 SpyAPI 的默认状态和安全降级状态。

### 6.6 SpyAPI 的生命周期管理

```java
public static AbstractSpy getSpy() {
    return spyInstance;
}

public static void setSpy(AbstractSpy spy) {
    spyInstance = spy;
}

public static void setNopSpy() {
    setSpy(NOPSPY);
}

public static boolean isNopSpy() {
    return NOPSPY == spyInstance;
}

public static void init() {
    INITED = true;
}

public static boolean isInited() {
    return INITED;
}

public static void destroy() {
    setNopSpy();
    INITED = false;
}
```

生命周期：
1. **启动**：`Enhancer` 的静态块调用 `SpyAPI.setSpy(new SpyImpl())`
2. **初始化**：某处调用 `SpyAPI.init()` 设置 `INITED = true`
3. **运行中**：所有回调通过 `SpyImpl` 处理
4. **销毁**：调用 `SpyAPI.destroy()` 将 `spyInstance` 重置为 `NOPSPY`

### 6.7 SpyImpl —— Spy 的真正实现

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/SpyImpl.java`

`SpyImpl` 是 `AbstractSpy` 的唯一业务实现，它将 SpyAPI 的回调转发给真正的业务监听器。

```java
public class SpyImpl extends AbstractSpy {
    private static final Logger logger =
        LoggerFactory.getLogger(SpyImpl.class);
    // ...
}
```

### 6.8 SpyImpl.atEnter() —— 方法入口回调

```java
@Override
public void atEnter(Class<?> clazz, String methodInfo,
                    Object target, Object[] args) {
    ClassLoader classLoader = clazz.getClassLoader();

    String[] info = StringUtils.splitMethodInfo(methodInfo);
    String methodName = info[0];
    String methodDesc = info[1];

    List<AdviceListener> listeners =
        AdviceListenerManager.queryAdviceListeners(
            classLoader, clazz.getName(),
            methodName, methodDesc);

    if (listeners != null) {
        for (AdviceListener adviceListener : listeners) {
            try {
                if (skipAdviceListener(adviceListener)) {
                    continue;
                }
                adviceListener.before(
                    clazz, methodName, methodDesc,
                    target, args);
            } catch (Throwable e) {
                logger.error(
                    "class: {}, methodInfo: {}",
                    clazz.getName(), methodInfo, e);
            }
        }
    }
}
```

**逐步解析：**

1. **获取 ClassLoader**：从 Class 对象获取 ClassLoader，用作后续查询的 key
2. **解析 methodInfo**：将 `"methodName|methodDesc"` 格式的字符串拆分为方法名和方法描述符
3. **查询监听器**：通过 `AdviceListenerManager.queryAdviceListeners()` 查找注册在这个 `classLoader + className + methodName + methodDesc` 上的所有监听器
4. **遍历调用**：对每个监听器调用 `before()` 方法
5. **跳过检查**：通过 `skipAdviceListener()` 检查监听器是否应该跳过
6. **异常隔离**：每个监听器的调用都被 try-catch 包裹，确保一个监听器的异常不会影响其他监听器

### 6.9 SpyImpl.atExit() —— 方法正常返回回调

```java
@Override
public void atExit(Class<?> clazz, String methodInfo,
                   Object target, Object[] args,
                   Object returnObject) {
    ClassLoader classLoader = clazz.getClassLoader();

    String[] info = StringUtils.splitMethodInfo(methodInfo);
    String methodName = info[0];
    String methodDesc = info[1];

    List<AdviceListener> listeners =
        AdviceListenerManager.queryAdviceListeners(
            classLoader, clazz.getName(),
            methodName, methodDesc);

    if (listeners != null) {
        for (AdviceListener adviceListener : listeners) {
            try {
                if (skipAdviceListener(adviceListener)) {
                    continue;
                }
                adviceListener.afterReturning(
                    clazz, methodName, methodDesc,
                    target, args, returnObject);
            } catch (Throwable e) {
                logger.error(
                    "class: {}, methodInfo: {}",
                    clazz.getName(), methodInfo, e);
            }
        }
    }
}
```

结构与 `atEnter()` 完全一致，区别是调用监听器的 `afterReturning()` 方法，并多传递了 `returnObject`。

### 6.10 SpyImpl.atExceptionExit() —— 方法异常退出回调

```java
@Override
public void atExceptionExit(Class<?> clazz,
                            String methodInfo,
                            Object target, Object[] args,
                            Throwable throwable) {
    ClassLoader classLoader = clazz.getClassLoader();

    String[] info = StringUtils.splitMethodInfo(methodInfo);
    String methodName = info[0];
    String methodDesc = info[1];

    List<AdviceListener> listeners =
        AdviceListenerManager.queryAdviceListeners(
            classLoader, clazz.getName(),
            methodName, methodDesc);

    if (listeners != null) {
        for (AdviceListener adviceListener : listeners) {
            try {
                if (skipAdviceListener(adviceListener)) {
                    continue;
                }
                adviceListener.afterThrowing(
                    clazz, methodName, methodDesc,
                    target, args, throwable);
            } catch (Throwable e) {
                logger.error(
                    "class: {}, methodInfo: {}",
                    clazz.getName(), methodInfo, e);
            }
        }
    }
}
```

调用监听器的 `afterThrowing()` 方法，传递异常对象。

### 6.11 SpyImpl.atBeforeInvoke() —— 子调用前回调（Trace 专用）

```java
@Override
public void atBeforeInvoke(Class<?> clazz,
                           String invokeInfo,
                           Object target) {
    ClassLoader classLoader = clazz.getClassLoader();
    String[] info = StringUtils.splitInvokeInfo(invokeInfo);
    String owner = info[0];
    String methodName = info[1];
    String methodDesc = info[2];

    List<AdviceListener> listeners =
        AdviceListenerManager.queryTraceAdviceListeners(
            classLoader, clazz.getName(),
            owner, methodName, methodDesc);

    if (listeners != null) {
        for (AdviceListener adviceListener : listeners) {
            try {
                if (skipAdviceListener(adviceListener)) {
                    continue;
                }
                final InvokeTraceable listener =
                    (InvokeTraceable) adviceListener;
                listener.invokeBeforeTracing(
                    classLoader, owner, methodName,
                    methodDesc, Integer.parseInt(info[3]));
            } catch (Throwable e) {
                logger.error(
                    "class: {}, invokeInfo: {}",
                    clazz.getName(), invokeInfo, e);
            }
        }
    }
}
```

与 enter/exit 不同的是：

1. **invokeInfo 的格式不同**：`splitInvokeInfo()` 解析 `"owner|methodName|methodDesc|lineNumber"` 四段信息
2. **使用 `queryTraceAdviceListeners()`**：查询的是 trace 类型的监听器（key 的格式包含 `owner`）
3. **强转为 `InvokeTraceable`**：调用 `invokeBeforeTracing()` 方法。只有实现了 `InvokeTraceable` 接口的 listener（如 `TraceAdviceListener`）才能处理 trace 回调

### 6.12 SpyImpl.atAfterInvoke() —— 子调用后回调

```java
@Override
public void atAfterInvoke(Class<?> clazz,
                          String invokeInfo,
                          Object target) {
    ClassLoader classLoader = clazz.getClassLoader();
    String[] info = StringUtils.splitInvokeInfo(invokeInfo);
    String owner = info[0];
    String methodName = info[1];
    String methodDesc = info[2];

    List<AdviceListener> listeners =
        AdviceListenerManager.queryTraceAdviceListeners(
            classLoader, clazz.getName(),
            owner, methodName, methodDesc);

    if (listeners != null) {
        for (AdviceListener adviceListener : listeners) {
            try {
                if (skipAdviceListener(adviceListener)) {
                    continue;
                }
                final InvokeTraceable listener =
                    (InvokeTraceable) adviceListener;
                listener.invokeAfterTracing(
                    classLoader, owner, methodName,
                    methodDesc, Integer.parseInt(info[3]));
            } catch (Throwable e) {
                logger.error(
                    "class: {}, invokeInfo: {}",
                    clazz.getName(), invokeInfo, e);
            }
        }
    }
}
```

调用 `InvokeTraceable.invokeAfterTracing()`。

### 6.13 SpyImpl.atInvokeException() —— 子调用异常回调

```java
@Override
public void atInvokeException(Class<?> clazz,
                              String invokeInfo,
                              Object target,
                              Throwable throwable) {
    ClassLoader classLoader = clazz.getClassLoader();
    String[] info = StringUtils.splitInvokeInfo(invokeInfo);
    String owner = info[0];
    String methodName = info[1];
    String methodDesc = info[2];

    List<AdviceListener> listeners =
        AdviceListenerManager.queryTraceAdviceListeners(
            classLoader, clazz.getName(),
            owner, methodName, methodDesc);

    if (listeners != null) {
        for (AdviceListener adviceListener : listeners) {
            try {
                if (skipAdviceListener(adviceListener)) {
                    continue;
                }
                final InvokeTraceable listener =
                    (InvokeTraceable) adviceListener;
                listener.invokeThrowTracing(
                    classLoader, owner, methodName,
                    methodDesc, Integer.parseInt(info[3]));
            } catch (Throwable e) {
                logger.error(
                    "class: {}, invokeInfo: {}",
                    clazz.getName(), invokeInfo, e);
            }
        }
    }
}
```

调用 `InvokeTraceable.invokeThrowTracing()`。

### 6.14 SpyImpl.atLine() —— 行号回调

```java
@Override
public void atLine(Class<?> clazz, String methodInfo,
                   int lineNumber, Object target,
                   Object[] args, String[] argNames,
                   Object[] localVars,
                   String[] localVarNames) {
    ClassLoader classLoader = clazz.getClassLoader();

    String[] info = StringUtils.splitMethodInfo(methodInfo);
    String methodName = info[0];
    String methodDesc = info[1];

    List<AdviceListener> listeners =
        AdviceListenerManager.queryLineAdviceListeners(
            classLoader, clazz.getName(),
            methodName, methodDesc, lineNumber);

    if (listeners != null) {
        for (AdviceListener adviceListener : listeners) {
            try {
                if (skipAdviceListener(adviceListener)) {
                    continue;
                }
                adviceListener.atLine(
                    clazz, methodName, methodDesc,
                    target, args, lineNumber, argNames,
                    localVars, localVarNames);
            } catch (Throwable e) {
                logger.error(
                    "class: {}, methodInfo: {}, "
                    + "lineNumber: {}",
                    clazz.getName(), methodInfo,
                    lineNumber, e);
            }
        }
    }
}
```

行号回调使用 `queryLineAdviceListeners()`，其 key 包含行号信息。

### 6.15 skipAdviceListener() —— 进程状态检查

```java
private static boolean skipAdviceListener(
        AdviceListener adviceListener) {
    if (adviceListener instanceof ProcessAware) {
        ProcessAware processAware =
            (ProcessAware) adviceListener;
        Process process = processAware.getProcess();
        if (process == null) {
            return true;
        }
        ExecStatus status = process.status();
        if (status.equals(ExecStatus.TERMINATED)
                || status.equals(ExecStatus.STOPPED)) {
            return true;
        }
    }
    return false;
}
```

**这一步做了什么？** 检查监听器关联的命令进程是否还在运行。

**它为什么存在？**

当用户按 `q` 或 `Ctrl+C` 结束命令后，命令进程的状态会变为 `TERMINATED` 或 `STOPPED`。但此时增强的字节码还在 JVM 中运行（字节码的还原是异步的），`SpyAPI.atEnter()` 等方法仍然会被调用。`skipAdviceListener()` 通过检查进程状态，避免在命令已经结束后还继续处理回调。

注意：这里检查的是 `ProcessAware` 接口。`AdviceListenerAdapter`（所有具体监听器的基类）实现了 `ProcessAware`，所以所有 Arthas 的监听器都会被这个检查覆盖。

---

## 第七阶段：AdviceListenerManager —— 监听器注册表

### 7.1 AdviceListenerManager 类 —— 设计思想

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/AdviceListenerManager.java`

`AdviceListenerManager` 是一个静态工具类，充当全局的"监听器注册表"。当 `Enhancer.transform()` 增强字节码时，它会把监听器注册到这个表中；当运行时 `SpyImpl` 收到回调时，它从这个表中查找对应的监听器。

### 7.2 核心数据结构 —— 按 ClassLoader 分组

```java
private static final ConcurrentWeakKeyHashMap
    <ClassLoader, ClassLoaderAdviceListenerManager>
    adviceListenerMap =
        new ConcurrentWeakKeyHashMap
            <ClassLoader,
             ClassLoaderAdviceListenerManager>();
```

**为什么要按 ClassLoader 分组？**

在 Java 中，相同全限定名的类可以被不同的 ClassLoader 加载，形成完全独立的类。例如，在 OSGi 或类似的模块化环境中，`com.example.MyService` 可能存在于多个 ClassLoader 中。Arthas 需要精确地区分这些"同名不同类"，所以用 ClassLoader 作为第一级 key。

**为什么用 `ConcurrentWeakKeyHashMap`？**

Weak Key（弱引用键）意味着：当 ClassLoader 被 GC 回收时（例如 Web 应用被卸载），对应的注册表条目也会自动被清理，防止内存泄漏。这是一个非常重要的设计——想象一下，在一个应用服务器中频繁部署/卸载 Web 应用，如果用强引用持有 ClassLoader，被卸载的 ClassLoader 永远无法被 GC 回收，导致严重的内存泄漏。

### 7.3 FakeBootstrapClassLoader —— 处理 null ClassLoader

```java
private static final FakeBootstrapClassLoader
    FAKEBOOTSTRAPCLASSLOADER = new FakeBootstrapClassLoader();

private static ClassLoader wrap(ClassLoader classLoader) {
    if (classLoader != null) {
        return classLoader;
    }
    return FAKEBOOTSTRAPCLASSLOADER;
}

private static class FakeBootstrapClassLoader
        extends ClassLoader {
}
```

**这个"假"ClassLoader 为什么存在？**

由 BootstrapClassLoader 加载的类，其 `getClassLoader()` 返回 `null`。但 `ConcurrentWeakKeyHashMap` 不允许 `null` 作为 key（WeakReference 不能包裹 null）。所以 Arthas 创建了一个 `FakeBootstrapClassLoader` 实例来替代 `null`，作为 BootstrapClassLoader 加载的类的 key。

### 7.4 ClassLoaderAdviceListenerManager —— 二级管理器

```java
static class ClassLoaderAdviceListenerManager {
    private ConcurrentHashMap<String, List<AdviceListener>>
        map = new ConcurrentHashMap
            <String, List<AdviceListener>>();
```

每个 ClassLoader 对应一个 `ClassLoaderAdviceListenerManager`，其内部用 `ConcurrentHashMap` 存储具体的监听器列表。

### 7.5 Key 的设计 —— 三种 Key 格式

```java
private String key(String className,
                   String methodName,
                   String methodDesc) {
    return className + methodName + methodDesc;
}

private String keyForTrace(String className,
                           String owner,
                           String methodName,
                           String methodDesc) {
    return className + owner + methodName + methodDesc;
}

private String keyForLine(String className,
                          String methodName,
                          String methodDesc,
                          int lineNumber) {
    return className + methodName + methodDesc
           + "#" + lineNumber;
}
```

| Key 类型 | 格式 | 用途 | 示例 |
|---------|------|------|------|
| 普通 key | `className + methodName + methodDesc` | watch/monitor/stack 等 | `com.example.MyServiceprocess(Ljava/lang/String;)V` |
| trace key | `className + owner + methodName + methodDesc` | trace 命令的子调用 | `com.example.MyServicecom/example/DaogetById(I)Ljava/lang/Object;` |
| line key | `className + methodName + methodDesc + "#" + lineNumber` | 行号增强 | `com.example.MyServiceprocess(Ljava/lang/String;)V#42` |

注意：key 中没有使用分隔符（除了 line key 用了 `#`）。这是因为 className、methodName、methodDesc 的组合在 Java 中是唯一的，简单的字符串拼接就足够区分了。

### 7.6 registerAdviceListener() —— 注册普通监听器

```java
public static void registerAdviceListener(
        ClassLoader classLoader,
        String className, String methodName,
        String methodDesc, AdviceListener listener) {
    classLoader = wrap(classLoader);
    className = className.replace('/', '.');

    logger.info(
        "registerAdviceListener: classLoader={}, "
        + "className={}, methodName={}, "
        + "methodDesc={}, listener={}",
        classLoader, className, methodName,
        methodDesc, listener.id());

    ClassLoaderAdviceListenerManager manager =
        adviceListenerMap.get(classLoader);

    if (manager == null) {
        manager = new ClassLoaderAdviceListenerManager();
        adviceListenerMap.put(classLoader, manager);
    }
    manager.registerAdviceListener(
        className, methodName, methodDesc, listener);
}
```

**逐步解析：**

1. **`wrap(classLoader)`**：将 null ClassLoader 替换为 FakeBootstrapClassLoader
2. **`className.replace('/', '.')`**：将 JVM 内部格式（`/` 分隔）转为 Java 格式（`.` 分隔）
3. **获取或创建二级管理器**：如果这个 ClassLoader 第一次出现，创建一个新的 `ClassLoaderAdviceListenerManager`
4. **委托注册**：调用二级管理器的 `registerAdviceListener()`

### 7.7 registerListener() —— 内部注册逻辑

```java
private void registerListener(String key,
                              AdviceListener listener) {
    List<AdviceListener> listeners = map.get(key);
    if (listeners != null && listeners.contains(listener)) {
        return;
    }

    List<AdviceListener> newListeners = listeners == null
        ? new ArrayList<AdviceListener>()
        : new ArrayList<AdviceListener>(listeners);
    newListeners.add(listener);
    map.put(key, newListeners);
}
```

关键设计点：

1. **去重**：如果 listener 已经在列表中，直接返回
2. **Copy-on-Write 策略**：创建新的 ArrayList 而不是在原列表上修改。这避免了在遍历列表时出现 `ConcurrentModificationException`（因为 `SpyImpl` 中的遍历和这里的注册可能并发执行）
3. **synchronized 保护**：调用方 `registerAdviceListener` 在 `synchronized(this)` 块中调用此方法

### 7.8 queryAdviceListeners() —— 查询监听器

```java
public static List<AdviceListener> queryAdviceListeners(
        ClassLoader classLoader, String className,
        String methodName, String methodDesc) {
    classLoader = wrap(classLoader);
    className = className.replace('/', '.');
    ClassLoaderAdviceListenerManager manager =
        adviceListenerMap.get(classLoader);

    if (manager != null) {
        return manager.queryAdviceListeners(
            className, methodName, methodDesc);
    }

    return null;
}
```

查询逻辑是注册的逆操作：通过 ClassLoader 找到二级管理器，再通过 key 找到 listener 列表。

### 7.9 registerTraceAdviceListener() 和 registerLineAdviceListener()

```java
public static void registerTraceAdviceListener(
        ClassLoader classLoader, String className,
        String owner, String methodName,
        String methodDesc, AdviceListener listener) {
    classLoader = wrap(classLoader);
    className = className.replace('/', '.');

    ClassLoaderAdviceListenerManager manager =
        adviceListenerMap.get(classLoader);

    if (manager == null) {
        manager = new ClassLoaderAdviceListenerManager();
        adviceListenerMap.put(classLoader, manager);
    }
    manager.registerTraceAdviceListener(
        className, owner, methodName, methodDesc, listener);
}

public static void registerLineAdviceListener(
        ClassLoader classLoader, String className,
        String methodName, String methodDesc,
        int lineNumber, AdviceListener listener) {
    classLoader = wrap(classLoader);
    className = className.replace('/', '.');

    ClassLoaderAdviceListenerManager manager =
        adviceListenerMap.get(classLoader);

    if (manager == null) {
        manager = new ClassLoaderAdviceListenerManager();
        adviceListenerMap.put(classLoader, manager);
    }
    manager.registerLineAdviceListener(
        className, methodName, methodDesc,
        lineNumber, listener);
}
```

与 `registerAdviceListener()` 结构一致，只是使用不同的 key 格式。

### 7.10 定时清理 TERMINATED 监听器

```java
static {
    ArthasBootstrap.getInstance()
        .getScheduledExecutorService()
        .scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Entry<ClassLoader,
                            ClassLoaderAdviceListenerManager>
                            entry
                            : adviceListenerMap.entrySet()) {
                        ClassLoaderAdviceListenerManager
                            adviceListenerManager =
                                entry.getValue();
                        synchronized (adviceListenerManager) {
                            for (Entry<String,
                                    List<AdviceListener>>
                                    eee
                                    : adviceListenerManager
                                        .map.entrySet()) {
                                List<AdviceListener> listeners
                                    = eee.getValue();
                                List<AdviceListener> newResult
                                    = new ArrayList
                                        <AdviceListener>();
                                for (AdviceListener listener
                                        : listeners) {
                                    if (listener
                                            instanceof
                                            ProcessAware) {
                                        ProcessAware
                                            processAware =
                                                (ProcessAware)
                                                    listener;
                                        Process process =
                                            processAware
                                                .getProcess();
                                        if (process == null) {
                                            continue;
                                        }
                                        ExecStatus status =
                                            process.status();
                                        if (!status.equals(
                                                ExecStatus
                                                .TERMINATED))
                                        {
                                            newResult.add(
                                                listener);
                                        }
                                    }
                                }

                                if (newResult.size()
                                        != listeners.size()) {
                                    adviceListenerManager
                                        .map.put(
                                            eee.getKey(),
                                            newResult);
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    try {
                        logger.error(
                            "clean AdviceListener error", e);
                    } catch (Throwable t) {
                        // ignore
                    }
                }
            }
        }, 3, 3, TimeUnit.SECONDS);
}
```

**这一步做了什么？** 每 3 秒清理一次已经终止的监听器。

**为什么需要定时清理？**

当用户结束一个 watch/trace 命令后，命令的进程状态变为 `TERMINATED`。但此时注册在 `AdviceListenerManager` 中的监听器并不会自动移除。虽然 `SpyImpl.skipAdviceListener()` 会跳过已终止的监听器，但这些无用的监听器仍然占用内存，并且每次方法调用时都需要遍历它们。

定时清理器会扫描所有注册的监听器，移除那些进程状态为 `TERMINATED` 的。清理间隔为 3 秒，在实时性和性能之间取得平衡。

注意：清理逻辑只检查 `TERMINATED` 状态，不检查 `STOPPED` 状态。这是因为 `STOPPED` 是暂停状态，监听器可能还会被恢复。

---

## 第八阶段：AdviceWeaver —— 监听器编织者

### 8.1 AdviceWeaver 类 —— 简洁的全局管理器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/AdviceWeaver.java`

`AdviceWeaver` 是一个更简单的监听器管理器，它通过 `listenerId`（一个 long 类型的 ID）来管理监听器。与 `AdviceListenerManager`（通过 ClassLoader + className + methodName + methodDesc 来索引）不同，`AdviceWeaver` 提供了一种基于 ID 的直接索引方式。

```java
public class AdviceWeaver {

    private static final Logger logger =
        LoggerFactory.getLogger(AdviceWeaver.class);

    // 通知监听器集合
    private final static Map<Long, AdviceListener> advices
        = new ConcurrentHashMap<Long, AdviceListener>();
```

### 8.2 reg() —— 注册监听器

```java
public static void reg(AdviceListener listener) {
    // 触发监听器创建
    listener.create();

    // 注册监听器
    advices.put(listener.id(), listener);
}
```

注册分两步：
1. **调用 `listener.create()`**：触发监听器的初始化逻辑（在 `AdviceListenerAdapter` 中默认为空操作）
2. **存入 Map**：以 listener 的 ID 为 key 存储

### 8.3 unReg() —— 注销监听器

```java
public static void unReg(AdviceListener listener) {
    if (null != listener) {
        // 注销监听器
        advices.remove(listener.id());

        // 触发监听器销毁
        listener.destroy();
    }
}
```

注销也是两步：先从 Map 中移除，再调用 `listener.destroy()` 进行清理。

### 8.4 listener() —— 查询监听器

```java
public static AdviceListener listener(long id) {
    return advices.get(id);
}
```

通过 ID 直接查询。这在 `EnhancerCommand.getAdviceListenerWithId()` 中被使用，用于支持 `--listenerId` 参数（复用已有的 listener）。

### 8.5 resume() 和 suspend() —— 暂停/恢复

```java
public static void resume(AdviceListener listener) {
    advices.put(listener.id(), listener);
}

public static AdviceListener suspend(long adviceId) {
    return advices.remove(adviceId);
}
```

`suspend()` 暂停监听（从 Map 中移除但不调用 `destroy()`），`resume()` 恢复监听（重新加入 Map 但不调用 `create()`）。

---

## 第九阶段：AdviceListener 体系 —— 从接口到实现

### 9.1 AdviceListener 接口 —— 通知监听器契约

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/AdviceListener.java`

```java
public interface AdviceListener {
    long id();
    void create();
    void destroy();

    void before(Class<?> clazz, String methodName,
                String methodDesc, Object target,
                Object[] args) throws Throwable;

    void afterReturning(Class<?> clazz, String methodName,
                        String methodDesc, Object target,
                        Object[] args, Object returnObject)
                        throws Throwable;

    void afterThrowing(Class<?> clazz, String methodName,
                       String methodDesc, Object target,
                       Object[] args, Throwable throwable)
                       throws Throwable;

    default void atLine(Class<?> clazz, String methodName,
                        String methodDesc, Object target,
                        Object[] args, int lineNumber,
                        String[] argNames, Object[] localVars,
                        String[] localVarNames)
                        throws Throwable {
    }
}
```

接口定义了监听器的完整生命周期和四种回调方法。

### 9.2 InvokeTraceable 接口 —— 方法调用跟踪

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/InvokeTraceable.java`

```java
public interface InvokeTraceable {
    void invokeBeforeTracing(
            ClassLoader classLoader,
            String tracingClassName,
            String tracingMethodName,
            String tracingMethodDesc,
            int tracingLineNumber) throws Throwable;

    void invokeThrowTracing(
            ClassLoader classLoader,
            String tracingClassName,
            String tracingMethodName,
            String tracingMethodDesc,
            int tracingLineNumber) throws Throwable;

    void invokeAfterTracing(
            ClassLoader classLoader,
            String tracingClassName,
            String tracingMethodName,
            String tracingMethodDesc,
            int tracingLineNumber) throws Throwable;
}
```

这个接口定义了三种子调用级别的回调。只有 trace 命令的 listener 需要实现这个接口。

### 9.3 AdviceListenerAdapter —— 监听器适配器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/advisor/AdviceListenerAdapter.java`

```java
public abstract class AdviceListenerAdapter
        implements AdviceListener, ProcessAware {
    private static final AtomicLong ID_GENERATOR =
        new AtomicLong(0);
    private Process process;
    private long id = ID_GENERATOR.addAndGet(1);
    private boolean verbose;
```

**ID 生成策略：**

每个 `AdviceListenerAdapter` 实例在创建时自动获得一个全局唯一的 ID。使用 `AtomicLong` 保证线程安全和唯一性。

**方法桥接：**

```java
@Override
final public void before(Class<?> clazz,
        String methodName, String methodDesc,
        Object target, Object[] args)
        throws Throwable {
    before(clazz.getClassLoader(), clazz,
           new ArthasMethod(clazz, methodName, methodDesc),
           target, args);
}

@Override
final public void afterReturning(Class<?> clazz,
        String methodName, String methodDesc,
        Object target, Object[] args,
        Object returnObject) throws Throwable {
    afterReturning(clazz.getClassLoader(), clazz,
           new ArthasMethod(clazz, methodName, methodDesc),
           target, args, returnObject);
}

@Override
final public void afterThrowing(Class<?> clazz,
        String methodName, String methodDesc,
        Object target, Object[] args,
        Throwable throwable) throws Throwable {
    afterThrowing(clazz.getClassLoader(), clazz,
           new ArthasMethod(clazz, methodName, methodDesc),
           target, args, throwable);
}
```

`AdviceListenerAdapter` 在 `AdviceListener` 接口和子类之间做了一层适配：

1. 从 `Class` 对象提取 `ClassLoader`
2. 将 `methodName + methodDesc` 封装为 `ArthasMethod` 对象
3. 调用子类实现的抽象方法（参数更加丰富和友好）

这些桥接方法是 `final` 的，子类不能覆盖，确保了适配逻辑的一致性。

**条件表达式求值：**

```java
protected boolean isConditionMet(
        String conditionExpress,
        Advice advice, double cost)
        throws ExpressException {
    return StringUtils.isEmpty(conditionExpress)
        || ExpressFactory.threadLocalExpress(advice)
               .bind(Constants.COST_VARIABLE, cost)
               .is(conditionExpress);
}
```

支持条件表达式过滤（如 `watch MyService process params '#cost>100'`），使用 OGNL 表达式引擎求值。

### 9.4 WatchAdviceListener —— Watch 命令的监听器实现

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/WatchAdviceListener.java`

```java
class WatchAdviceListener extends AdviceListenerAdapter {
    private final ThreadLocalWatch threadLocalWatch =
        new ThreadLocalWatch();
    private WatchCommand command;
    private CommandProcess process;

    public WatchAdviceListener(WatchCommand command,
            CommandProcess process, boolean verbose) {
        this.command = command;
        this.process = process;
        super.setVerbose(verbose);
    }
```

**before() —— 方法入口回调**

```java
@Override
public void before(ClassLoader loader, Class<?> clazz,
        ArthasMethod method, Object target, Object[] args)
        throws Throwable {
    threadLocalWatch.start();
    if (command.isBefore()) {
        watching(Advice.newForBefore(
            loader, clazz, method, target, args));
    }
}
```

1. **`threadLocalWatch.start()`**：开始计时。使用 `ThreadLocal` 存储开始时间，确保线程安全
2. 如果用户指定了 `-b`（before），则在方法入口处就输出 watch 结果

**afterReturning() —— 方法正常返回回调**

```java
@Override
public void afterReturning(ClassLoader loader,
        Class<?> clazz, ArthasMethod method,
        Object target, Object[] args,
        Object returnObject) throws Throwable {
    Advice advice = Advice.newForAfterReturning(
        loader, clazz, method, target, args, returnObject);
    if (command.isSuccess()) {
        watching(advice);
    }
    finishing(advice);
}
```

如果用户指定了 `-s`（success），在成功返回时输出。然后调用 `finishing()`。

**afterThrowing() —— 方法异常退出回调**

```java
@Override
public void afterThrowing(ClassLoader loader,
        Class<?> clazz, ArthasMethod method,
        Object target, Object[] args,
        Throwable throwable) {
    Advice advice = Advice.newForAfterThrowing(
        loader, clazz, method, target, args, throwable);
    if (command.isException()) {
        watching(advice);
    }
    finishing(advice);
}
```

如果用户指定了 `-e`（exception），在异常退出时输出。

**watching() —— 核心输出逻辑**

```java
private void watching(Advice advice) {
    try {
        double cost = threadLocalWatch.costInMillis();
        boolean conditionResult = isConditionMet(
            command.getConditionExpress(), advice, cost);

        if (this.isVerbose()) {
            process.write("Condition express: "
                + command.getConditionExpress()
                + " , result: " + conditionResult + "\n");
        }

        if (conditionResult) {
            Object value = getExpressionResult(
                command.getExpress(), advice, cost);

            WatchModel model = new WatchModel();
            model.setTs(LocalDateTime.now());
            model.setCost(cost);
            model.setValue(new ObjectVO(
                value, command.getExpand()));
            model.setSizeLimit(command.getSizeLimit());
            model.setClassName(
                advice.getClazz().getName());
            model.setMethodName(
                advice.getMethod().getName());

            if (advice.isBefore()) {
                model.setAccessPoint(
                    AccessPoint.ACCESS_BEFORE.getKey());
            } else if (advice.isAfterReturning()) {
                model.setAccessPoint(
                    AccessPoint.ACCESS_AFTER_RETUNING
                        .getKey());
            } else if (advice.isAfterThrowing()) {
                model.setAccessPoint(
                    AccessPoint.ACCESS_AFTER_THROWING
                        .getKey());
            }

            process.appendResult(model);
            process.times().incrementAndGet();
            if (isLimitExceeded(
                    command.getNumberOfLimit(),
                    process.times().get())) {
                abortProcess(process,
                    command.getNumberOfLimit());
            }
        }
    } catch (Throwable e) {
        logger.warn("watch failed.", e);
        process.end(-1,
            "watch failed, condition is: "
            + command.getConditionExpress()
            + ", express is: " + command.getExpress()
            + ", " + e.getMessage());
    }
}
```

`watching()` 方法是 watch 命令最终输出结果的地方：

1. **计算耗时**：从 `threadLocalWatch` 获取方法执行时间
2. **条件判断**：通过 OGNL 表达式判断是否满足用户指定的条件
3. **求值表达式**：通过 OGNL 表达式求值用户想要观察的内容
4. **构建输出模型**：创建 `WatchModel`，包含时间戳、耗时、观察值等信息
5. **输出结果**：通过 `process.appendResult(model)` 将结果发送给用户
6. **次数限制**：如果达到了 `-n` 指定的次数限制，自动终止命令

---

## 第十阶段：运行时回调完整链路

### 10.1 从目标方法调用到 watch 输出的完整链路图

下面是从用户执行 `watch com.example.MyService process` 到看到输出结果的完整运行时调用链路：

```
目标方法被调用: MyService.process("hello")
       |
       v
[增强后的字节码] SpyAPI.atEnter(MyService.class,
                    "process|(Ljava/lang/String;)Ljava/lang/String;",
                    this, new Object[]{"hello"})
       |
       v
[SpyAPI] spyInstance.atEnter(clazz, methodInfo, target, args)
       |  (spyInstance = SpyImpl 实例)
       v
[SpyImpl.atEnter()]
  1. classLoader = clazz.getClassLoader()
  2. splitMethodInfo("process|...") -> ["process", "(L...)L..."]
  3. AdviceListenerManager.queryAdviceListeners(
         classLoader, "com.example.MyService",
         "process", "(L...)L...")
  4. 遍历 listeners:
       |
       v
[skipAdviceListener()] 检查进程状态
  -> ExecStatus != TERMINATED && != STOPPED
  -> 不跳过
       |
       v
[adviceListener.before()]
  -> AdviceListenerAdapter.before()
       |
       v
[AdviceListenerAdapter.before()]
  -> before(classLoader, clazz,
            new ArthasMethod(...), target, args)
       |
       v
[WatchAdviceListener.before()]
  1. threadLocalWatch.start()  // 开始计时
  2. if (command.isBefore()) {
         watching(Advice.newForBefore(...))
     }
       |
       v (如果用户指定了 -b)
[WatchAdviceListener.watching()]
  1. cost = threadLocalWatch.costInMillis()
  2. conditionResult = isConditionMet(...)
  3. if (conditionResult) {
         value = getExpressionResult(
             "{params, target, returnObj}", advice, cost)
         model = new WatchModel()
         process.appendResult(model)
     }
       |
       v
[用户看到 watch 输出]
```

### 10.2 方法正常返回时的链路

```
目标方法正常返回: return "HELLO"
       |
       v
[增强后的字节码] SpyAPI.atExit(MyService.class,
                    "process|...", this,
                    new Object[]{"hello"}, "HELLO")
       |
       v
[SpyAPI] -> [SpyImpl.atExit()]
  -> queryAdviceListeners() -> listeners
  -> listener.afterReturning()
       |
       v
[AdviceListenerAdapter.afterReturning()]
  -> afterReturning(loader, clazz,
                    new ArthasMethod(...),
                    target, args, returnObject)
       |
       v
[WatchAdviceListener.afterReturning()]
  1. advice = Advice.newForAfterReturning(...)
  2. if (command.isSuccess()) watching(advice)
  3. finishing(advice)
       |
       v
[WatchAdviceListener.finishing()]
  -> if (isFinish()) watching(advice)
  -> isFinish() = isFinish || (!isBefore
                   && !isException && !isSuccess)
  -> 默认情况下 isFinish() 返回 true
       |
       v
[WatchAdviceListener.watching()]
  -> process.appendResult(model)
       |
       v
[用户看到 watch 输出，包含返回值 "HELLO"]
```

### 10.3 方法抛出异常时的链路

```
目标方法抛出异常: throw new RuntimeException("error")
       |
       v
[增强后的字节码] SpyAPI.atExceptionExit(MyService.class,
                    "process|...", this,
                    new Object[]{"hello"},
                    new RuntimeException("error"))
       |
       v
[SpyAPI] -> [SpyImpl.atExceptionExit()]
  -> queryAdviceListeners() -> listeners
  -> listener.afterThrowing()
       |
       v
[WatchAdviceListener.afterThrowing()]
  1. advice = Advice.newForAfterThrowing(...)
  2. if (command.isException()) watching(advice)
  3. finishing(advice)
       |
       v
[用户看到 watch 输出，包含异常信息]
```

---

## 第十一阶段：关键设计问题深入分析

### 11.1 为什么 SpyAPI 要放在 BootstrapClassLoader？

这是 Arthas 字节码增强体系中最核心的设计决策之一。让我们用一个完整的分析来理解它。

**问题背景：**

Arthas 需要在被增强的目标方法中插入对 SpyAPI 的调用（如 `SpyAPI.atEnter()`）。在 JVM 中，一个类只能调用其 ClassLoader 能够加载到的类。

**方案对比：**

| 方案 | SpyAPI 位置 | 优点 | 缺点 |
|------|-----------|------|------|
| 方案A：放在 Arthas ClassLoader | Arthas 自己的 JAR | 实现简单 | 其他 ClassLoader 加载的类无法调用 |
| 方案B：注入到每个 ClassLoader | 每个 ClassLoader 都有一份 | 都能访问 | 维护困难，状态不同步 |
| 方案C：放在 BootstrapClassLoader | `java.arthas` 包 | 所有 ClassLoader 都能访问 | 需要特殊的包名 |

Arthas 选择了方案C。`BootstrapClassLoader` 是 JVM 类加载层次的根，所有其他 ClassLoader 都能委托给它。因此，放在 BootstrapClassLoader 搜索路径中的类对所有 ClassLoader 都可见。

**包名 `java.arthas` 的巧妙之处：**

以 `java.` 开头的包被 JVM 安全策略保护，普通代码不能声明这个包下的类（否则会抛出 SecurityException）。但 Arthas 通过 `Instrumentation.appendToBootstrapClassLoaderSearch()` 将 spy.jar 添加到 BootstrapClassLoader 的搜索路径中，绕过了这个限制。这确保了 SpyAPI 确实由 BootstrapClassLoader 加载。

### 11.2 为什么 transform 时要检查 ClassLoader 能否加载 SpyAPI？

虽然 SpyAPI 放在了 BootstrapClassLoader 中，但在极少数情况下，某些特殊的 ClassLoader 可能无法访问到它：

1. **自定义 ClassLoader 覆盖了委托机制**：某些框架的 ClassLoader（如 OSGi 的 BundleClassLoader）不遵循标准的双亲委派模型，可能不会委托给 BootstrapClassLoader
2. **安全策略限制**：某些安全管理器可能阻止对 `java.arthas` 包的访问
3. **ClassLoader 隔离**：在高度隔离的容器环境中，ClassLoader 之间可能存在不可逾越的边界

如果不做这个检查，增强后的字节码在运行时会抛出 `NoClassDefFoundError`，导致目标方法完全无法执行——这比不增强要糟糕得多。

### 11.3 为什么要有 GroupLocationFilter 防止重复织入？

**场景分析：**

用户先后执行：
```
watch com.example.MyService process
trace com.example.MyService process
```

两个命令都会触发 `Enhancer.transform()` 对同一个类的同一个方法进行增强。如果没有 `GroupLocationFilter`：

1. 第一次增强：在方法入口插入 `SpyAPI.atEnter()`
2. 第二次增强：又在方法入口插入一次 `SpyAPI.atEnter()`

结果是方法每次执行时 `SpyAPI.atEnter()` 被调用两次，导致：
- `WatchAdviceListener.before()` 被调用两次
- 输出内容重复
- 计时可能出现偏差

`GroupLocationFilter` 通过检查字节码中是否已经存在对 `SpyAPI.atEnter/atExit/atExceptionExit` 的调用来防止重复插入。这是一种"幂等性"保障。

### 11.4 trace 和 watch 的拦截器有什么区别？

**watch 的拦截器（SpyInterceptor1/2/3）：**

只在方法的三个"边界点"插桩：
- `@AtEnter`：方法入口
- `@AtExit`：方法正常返回
- `@AtExceptionExit`：方法异常退出

这三个拦截器足以捕获方法的输入参数、返回值和异常，满足 watch 命令的需求。

**trace 的拦截器（SpyTraceInterceptor1/2/3）：**

在方法内部的**每个子调用**处插桩：
- `@AtInvoke(whenComplete=false)`：在子调用之前
- `@AtInvoke(whenComplete=true)`：在子调用完成后
- `@AtInvokeException`：在子调用抛出异常后

trace 命令需要知道方法内部调用了哪些其他方法，以及每个子调用的耗时。这要求在每个 `INVOKEVIRTUAL`、`INVOKEINTERFACE`、`INVOKESTATIC`、`INVOKESPECIAL` 指令前后都插入代码。

**性能影响对比：**

| 命令 | 插桩数量 | 性能影响 |
|------|---------|---------|
| watch | 3个点/方法（入口+正常出口+异常出口） | 较小 |
| trace | 3 + 3*N个点/方法（入口/出口/异常 + 每个子调用*3） | 较大 |

这就是为什么 `TransformerManager` 将 watch 和 trace 分开管理，并且 Arthas 文档中建议谨慎使用 trace，因为它的性能开销远大于 watch。

### 11.5 为什么 AdviceListenerManager 用 WeakKeyHashMap？

**内存泄漏风险分析：**

在 Java Web 应用服务器（如 Tomcat）中，每个 Web 应用有自己的 `WebappClassLoader`。当 Web 应用被热部署（undeploy 后 redeploy）时：

1. 旧的 `WebappClassLoader` 应该被 GC 回收
2. 如果 `AdviceListenerManager` 用强引用持有这个 ClassLoader 作为 Map 的 key，那么：
   - ClassLoader 无法被 GC
   - ClassLoader 加载的所有类都无法被 GC
   - 这些类持有的所有静态变量都无法被 GC
   - 最终导致 PermGen/Metaspace OOM

使用 `ConcurrentWeakKeyHashMap`，当 ClassLoader 只被 Map 的 WeakReference 引用时，GC 可以回收 ClassLoader，Map 的对应条目也会自动被清理。

**类比理解：**

想象一个图书馆的借书系统。如果用"借阅者的强引用"来管理借书记录，即使借阅者已经注销了账号，借书记录仍然持有对借阅者的引用，借阅者的信息永远无法从系统中清除。使用弱引用，当借阅者注销后（没有其他强引用），借书记录会自动被清理。

### 11.6 为什么需要定时清理 TERMINATED 监听器？

**问题场景：**

1. 用户执行 `watch com.example.MyService process`
2. 监听器被注册到 `AdviceListenerManager` 中
3. 用户按 `q` 退出 watch 命令
4. 命令进程状态变为 `TERMINATED`
5. 但注册表中的监听器**不会自动移除**

**为什么不在命令结束时立即移除？**

因为移除操作涉及遍历整个注册表（所有 ClassLoader、所有 key），这是一个比较重的操作。而且命令结束的上下文中可能有并发问题需要处理。因此 Arthas 采用了"懒清理"策略：

1. **运行时跳过**：`SpyImpl.skipAdviceListener()` 检查进程状态，跳过已终止的监听器。这保证了功能正确性。
2. **定时清理**：每 3 秒清理一次，将已终止的监听器从注册表中移除。这保证了内存不会持续增长。

这种"及时跳过 + 延迟清理"的策略在很多系统中都有应用，比如 Java 的 `WeakHashMap` 也是在每次操作时顺便清理，而不是实时清理。

### 11.7 懒加载模式如何工作？

**应用场景：**

用户想 watch 一个还没有被加载的类：

```
watch com.example.LazyService process --lazy
```

此时 `LazyService` 还没有被任何 ClassLoader 加载（可能是一个按需创建的类，或者应用的某个模块还没有启动）。

**工作流程：**

```
1. EnhancerCommand.enhance() 执行:
   |
   +-> SearchUtils.searchClass() -> 空集合（类未加载）
   |
   +-> Enhancer.enhance():
       |
       +-> matchingClasses = {} (空)
       +-> filter() -> 无事可做
       +-> TransformerManager.addTransformer(this, false)
       |   (注册到 watchTransformers)
       +-> TransformerManager.addLazyTransformer(this)
       |   (注册到 lazyTransformers)
       +-> retransformClasses() -> 无事可做（没有类要 retransform）
       +-> affect.cCnt() == 0, affect.mCnt() == 0
   |
   +-> "Lazy mode is enabled, waiting for class..."

2. 某个时刻，ClassLoader 加载 LazyService:
   |
   +-> JVM 调用 TransformerManager 的
   |   lazyClassFileTransformer.transform()
   |   (因为是类首次加载，classBeingRedefined == null)
   |
   +-> 遍历 lazyTransformers -> Enhancer.transform()
       |
       +-> matchingClasses 不包含 classBeingRedefined
       |   (classBeingRedefined == null)
       +-> isLazy == true && classBeingRedefined == null
       +-> classNameMatcher.matching("com.example.LazyService")
       |   -> true!
       +-> 各项检查通过
       +-> 执行完整的字节码织入
       +-> 注册监听器
       +-> 返回增强后的字节码
   |
   +-> JVM 使用增强后的字节码定义类

3. LazyService.process() 被调用:
   |
   +-> SpyAPI.atEnter() -> SpyImpl -> WatchAdviceListener
   +-> 用户看到 watch 输出
```

**关键设计点：**

1. **两个 Transformer 列表**：`Enhancer` 同时被注册到 `watchTransformers`（处理已加载类的 retransform）和 `lazyTransformers`（处理首次加载的类）
2. **两个复合 Transformer**：`classFileTransformer`（retransform-capable=true）和 `lazyClassFileTransformer`（retransform-capable=false）分别处理这两种场景
3. **双重检查**：`lazyClassFileTransformer` 检查 `classBeingRedefined != null` 只处理首次加载；`Enhancer.transform()` 中在 `isLazy` 分支内重新检查类名匹配、ClassLoader 匹配等条件

---

## 第十二阶段：核心类关系图总览

为了帮助读者建立起所有类之间的关系，我们最后用一张完整的类关系图来做总结。

```
+------------------+
|  WatchCommand    |     用户命令入口
+--------+---------+
         |
         | extends
         v
+------------------+
| EnhancerCommand  |     所有增强命令的基类
+--------+---------+
         |
         | creates
         v
+------------------+        +-----------------------+
|    Enhancer      |------->| TransformerManager    |
| (implements      |  add   |                       |
|  ClassFile-      |  to    | watchTransformers[]   |
|  Transformer)    |        | traceTransformers[]   |
+--------+---------+        | reTransformers[]      |
         |                  | lazyTransformers[]    |
         | creates          |                       |
         v                  | classFileTransformer  |
+------------------+        |  (retransform-capable)|
| WatchAdvice-     |        | lazyClassFileTransfmr |
|   Listener       |        |  (non-retransformable)|
| (extends         |        +-----------------------+
|  AdviceListener- |                    |
|  Adapter)        |                    | registered with
+--------+---------+                    v
         |                  +-----------------------+
         |                  | Instrumentation       |
         |                  | (JVM API)             |
         |                  +-----------+-----------+
         |                              |
  registered                 retransformClasses()
  with                              |
         |                          v
         v               Enhancer.transform() called
+------------------+              |
| AdviceListener-  |              v
|   Manager        |   +----------------------+
|                  |   | SpyInterceptors      |
| WeakKeyHashMap   |   |  SpyInterceptor1     |
|  <ClassLoader,   |   |  SpyInterceptor2     |
|   CLAdvice-      |   |  SpyInterceptor3     |
|   ListenerMgr>   |   |  SpyTraceIntcpt1/2/3 |
+--------+---------+   +----------+-----------+
         ^                         |
         |                   inline into
         | query                   |
         |                         v
+------------------+   [增强后的目标方法字节码]
|    SpyImpl       |              |
| (extends         |    运行时调用
|  AbstractSpy)    |<-------------+
+--------+---------+
         ^
         |  setSpy()
+------------------+
|    SpyAPI        |     位于 java.arthas 包
| (Bootstrap       |     BootstrapClassLoader 加载
|  ClassLoader)    |     所有 ClassLoader 可见
+------------------+

+------------------+
|  AdviceWeaver    |     按 listenerId 管理
|  Map<Long,       |     reg() / unReg() / listener()
|   AdviceListener>|
+------------------+
```

---

## 第十三阶段：增强/还原生命周期完整时序

最后，让我们用一个完整的时序图来总结 watch 命令从开始到结束的完整生命周期。

```
时间 -->

用户        WatchCmd    EnhancerCmd   Enhancer      TransformerMgr   JVM           SpyImpl      WatchListener
 |            |            |            |               |              |              |              |
 |--watch---->|            |            |               |              |              |              |
 |            |--process-->|            |               |              |              |              |
 |            |            |--enhance-->|               |              |              |              |
 |            |            |            |--searchClass->|              |              |              |
 |            |            |            |               |--getAllLoaded>|              |              |
 |            |            |            |<-matchClasses-|              |              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |--filter------>|              |              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |--addTransfmr->|              |              |              |
 |            |            |            |               |--addTransfmr>|              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |--retransform->|              |              |              |
 |            |            |            |               |--retransfmr->|              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |<---transform--|              |              |              |
 |            |            |            |  (classfile)  |              |              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |  [ASM解析]     |              |              |              |
 |            |            |            |  [拦截器解析]   |              |              |              |
 |            |            |            |  [方法匹配]    |              |              |              |
 |            |            |            |  [字节码织入]   |              |              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |--registerAdvc>|              |              |              |
 |            |            |            |  (listener)   |              |              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |---enhanced--->|              |              |              |
 |            |            |            |   bytes       |--loadClass-->|              |              |
 |            |            |            |               |              |              |              |
 |            |            |<--affect---|               |              |              |              |
 |            |<--result---|            |               |              |              |              |
 |<--output---|            |            |               |              |              |              |
 |            |            |            |               |              |              |              |
 |  [等待目标方法被调用]     |               |              |              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |               |    [目标方法被调用]           |              |
 |            |            |            |               |              |--atEnter---->|              |
 |            |            |            |               |              |              |--before----->|
 |            |            |            |               |              |              |              |--start()
 |            |            |            |               |              |              |              |
 |            |            |            |               |    [方法体执行]              |              |
 |            |            |            |               |              |              |              |
 |            |            |            |               |              |--atExit----->|              |
 |            |            |            |               |              |              |--afterRet--->|
 |            |            |            |               |              |              |              |--watching()
 |            |            |            |               |              |              |              |--cost()
 |            |            |            |               |              |              |              |--condition
 |            |            |            |               |              |              |              |--express
 |<----result-|------------|------------|---------------|--------------|--------------|--------------|--appendResult
 |            |            |            |               |              |              |              |
 |            |            |            |               |              |              |              |
 |--q/Ctrl+C->|            |            |               |              |              |              |
 |            |--cleanup-->|            |               |              |              |              |
 |            |            |--removeTfm>|               |              |              |              |
 |            |            |            |--removeTransf>|              |              |              |
 |            |            |            |               |              |              |              |
 |            |            |--reset---->|               |              |              |              |
 |            |            |            |--retransfmr-->|              |              |              |
 |            |            |            |               |--retransfmr->|              |              |
 |            |            |            |               |  [无transformer修改]         |              |
 |            |            |            |               |  [使用原始字节码]            |              |
 |            |            |            |               |              |              |              |
 |            |<--done-----|            |               |              |              |              |
 |<--exit-----|            |            |               |              |              |              |
```

---

## 第十四阶段：设计模式与架构总结

### 14.1 使用的设计模式

| 设计模式 | 应用场景 | 具体实现 |
|---------|---------|---------|
| **策略模式** | 不同命令使用不同的 AdviceListener | WatchAdviceListener, TraceAdviceListener 等 |
| **模板方法模式** | EnhancerCommand 定义增强流程骨架 | enhance() 方法是模板，子类实现 getAdviceListener() |
| **委托模式** | SpyAPI 委托给 SpyImpl | SpyAPI 持有 AbstractSpy 引用 |
| **空对象模式** | NopSpy 作为默认的空实现 | 避免 null 检查 |
| **组合模式** | TransformerManager 组合多个 transformer | 复合 classFileTransformer |
| **责任链模式** | 多个 transformer 按顺序处理字节码 | re -> watch -> trace 顺序 |
| **观察者模式** | AdviceListener 监听方法执行事件 | before/afterReturning/afterThrowing |
| **适配器模式** | AdviceListenerAdapter 适配接口 | 将简单的 AdviceListener 接口适配为更丰富的参数 |

### 14.2 核心架构决策总结

| 决策 | 选择 | 理由 |
|------|------|------|
| SpyAPI 的 ClassLoader | BootstrapClassLoader | 确保所有 ClassLoader 可见 |
| Transformer 管理 | 集中式 TransformerManager | 统一管理、避免冲突 |
| 字节码操作库 | ASM + bytekit | ASM 轻量高效，bytekit 提供高级抽象 |
| 监听器注册表 | WeakKeyHashMap | 防止 ClassLoader 内存泄漏 |
| 拦截代码注入方式 | inline = true | 避免额外的方法调用开销 |
| 重复织入防护 | GroupLocationFilter | 检查已有的 SpyAPI 调用 |
| 监听器清理 | 定时清理（3秒） | 平衡实时性和性能 |
| 类搜索 | inst.getAllLoadedClasses() | JVM 标准 API |
| retransform 策略 | 支持批量和逐个 | 灵活应对不同场景 |

### 14.3 性能影响分析

| 阶段 | 性能影响 | 原因 |
|------|---------|------|
| 增强时 | 一次性开销 | retransformClasses 会导致短暂的 STW |
| 运行时（无匹配调用） | 几乎为零 | 增强后的代码只是多了几个静态方法调用 |
| 运行时（有匹配调用） | 微秒级 | SpyAPI 调用 + HashMap 查找 + listener 回调 |
| 还原时 | 一次性开销 | 同增强时 |

### 14.4 线程安全分析

| 组件 | 线程安全机制 |
|------|-------------|
| SpyAPI.spyInstance | volatile 关键字 |
| TransformerManager 的四个列表 | CopyOnWriteArrayList |
| AdviceListenerManager | ConcurrentWeakKeyHashMap + synchronized |
| AdviceWeaver.advices | ConcurrentHashMap |
| Enhancer.enhance() | synchronized 方法 |
| WatchAdviceListener.threadLocalWatch | ThreadLocal |

---

## 总结

通过对 Arthas 字节码增强与 Instrumentation 机制的全流程源码分析，我们可以看到这是一个设计精良、考虑周全的系统。它巧妙地利用了 JVM 提供的 Instrumentation API，通过 ASM + bytekit 进行字节码操作，通过 SpyAPI 的 BootstrapClassLoader 放置实现跨 ClassLoader 通信，通过 AdviceListenerManager 的 WeakKeyHashMap 防止内存泄漏，通过 GroupLocationFilter 防止重复织入，通过定时清理器维护注册表的健康。

整个系统的核心思路可以用一句话概括：**在不修改源代码的情况下，通过 JVM Agent 机制在目标方法的关键位置（入口、出口、异常出口、子调用处）插入对 SpyAPI 静态方法的调用，运行时通过 SpyAPI -> SpyImpl -> AdviceListenerManager -> AdviceListener 的调用链将方法执行信息传递给命令处理器，最终输出给用户。**

这种设计的优雅之处在于：

1. **非侵入性**：不需要修改目标应用的任何代码
2. **可逆性**：通过移除 transformer 并 retransform，可以完全还原字节码
3. **多路复用**：多个命令可以共享同一套插桩代码，通过注册不同的 listener 实现不同的功能
4. **安全降级**：SpyAPI 的 NopSpy 确保了在 Arthas 未启动或已退出时不会产生副作用
5. **内存安全**：WeakKeyHashMap + 定时清理确保了不会因为诊断工具本身导致内存泄漏
