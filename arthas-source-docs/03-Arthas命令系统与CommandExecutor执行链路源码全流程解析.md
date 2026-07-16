# Arthas 命令系统与 CommandExecutor 执行链路源码全流程解析

> 本文基于 [Arthas](https://github.com/alibaba/arthas) 开源项目源码进行分析，源码阅读目录位于
> `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas`。
> Arthas 是阿里巴巴开源的 Java 诊断工具，它通过 Java Agent 技术动态 attach 到目标 JVM 进程，
> 提供了丰富的诊断命令（如 watch、trace、jad、thread 等），帮助开发者在不重启应用的情况下排查线上问题。
>
> 本文将从用户在终端输入一条命令开始，逐层追踪到命令被执行并将结果输出到终端的完整链路。
> 我们将以"外科手术式"的精确度，深入到每一个方法调用、每一行关键代码背后的设计意图。
> 这不仅仅是一篇"代码导读"，更是一次对 **Arthas 命令系统架构设计哲学** 的深度探索。

---

## 全局调用链总览

在深入源码之前，让我们先获得一个全景视角。下面这张 ASCII 图展示了从用户敲入命令到命令执行完毕的完整调用链路：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        用户在终端输入命令（如 "watch Demo test"）                   │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第一阶段：ShellServerImpl                                                        │
│                                                                                 │
│  TermServer (Netty)                                                             │
│    │                                                                            │
│    ├──→ TermServerTermHandler.handle(Term)                                      │
│    │        │                                                                   │
│    │        └──→ ShellServerImpl.handleTerm(Term)                               │
│    │                │                                                           │
│    │                ├── createShell(term) → new ShellImpl(...)                   │
│    │                ├── session.init()    → 注册 Interrupt/Suspend/Close Handler │
│    │                ├── sessions.put(id, session)                                │
│    │                └── session.readline() → 开始读取用户输入                      │
│    │                                                                            │
└────┼────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第二阶段：ShellImpl                                                              │
│                                                                                 │
│  readline()                                                                     │
│    │                                                                            │
│    └──→ term.readline(prompt, ShellLineHandler, CompletionHandler)              │
│              │                                                                  │
│              │  用户输入一行命令后回车                                              │
│              ▼                                                                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第三阶段：ShellLineHandler                                                       │
│                                                                                 │
│  handle(String line)                                                            │
│    │                                                                            │
│    ├── CliTokens.tokenize(line) → 分词                                          │
│    ├── TokenUtils.findFirstTextToken(tokens) → 获取命令名                        │
│    │                                                                            │
│    ├── 内建命令分支:                                                              │
│    │   ├── "exit"/"logout"/"q"/"quit" → handleExit()                            │
│    │   ├── "jobs"                     → handleJobs()                             │
│    │   ├── "fg"                       → handleForeground(tokens)                 │
│    │   ├── "bg"                       → handleBackground(tokens)                 │
│    │   └── "kill"                     → handleKill(tokens)                       │
│    │                                                                            │
│    └── 普通命令分支:                                                              │
│        ├── createJob(tokens) → shell.createJob(tokens)                          │
│        └── job.run()                                                            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第四阶段：Job 创建链路                                                            │
│                                                                                 │
│  ShellImpl.createJob(tokens)                                                    │
│    │                                                                            │
│    └──→ JobControllerImpl.createJob(commandManager, tokens, session, ...)       │
│              │                                                                  │
│              ├── checkPermission(session, token) → 鉴权检查                      │
│              ├── idGenerator.incrementAndGet()   → 分配 Job ID                   │
│              ├── runInBackground(tokens)         → 检查是否后台运行(&)             │
│              ├── createProcess(...)               → 创建 Process                 │
│              │      │                                                           │
│              │      ├── commandManager.getCommand(name) → 查找命令               │
│              │      └── createCommandProcess(command, tokens, ...) → 构建进程    │
│              │             │                                                    │
│              │             ├── 解析管道符 | → injectHandler(stdoutHandlerChain)  │
│              │             ├── 解析重定向 > / >> → RedirectHandler               │
│              │             ├── 构建 ProcessOutput(stdoutHandlerChain)            │
│              │             └── new ProcessImpl(command, remaining, handler, ...) │
│              │                                                                  │
│              └── new JobImpl(jobId, controller, process, line, ...)             │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第五阶段：Job 运行 & Process 执行                                                 │
│                                                                                 │
│  JobImpl.run(foreground)                                                        │
│    │                                                                            │
│    ├── process.setSession(session)                                              │
│    ├── process.run(foreground)                                                  │
│    │      │                                                                     │
│    │      ├── processStatus = RUNNING                                           │
│    │      ├── new CommandProcessImpl(this, tty)                                 │
│    │      ├── new TermResultDistributorImpl(process, resultViewResolver)        │
│    │      ├── 提取文本参数 args2                                                 │
│    │      ├── commandContext.cli().parse(args2) → 解析命令行参数                  │
│    │      │      │                                                              │
│    │      │      └── 若 --help → 输出帮助并 terminate                            │
│    │      │                                                                     │
│    │      └── ArthasBootstrap.execute(new CommandProcessTask(process))          │
│    │             │                                                              │
│    │             │  ┌─── 线程池异步执行 ───┐                                     │
│    │             ▼  │                      │                                    │
│    │      CommandProcessTask.run()         │                                    │
│    │        │                              │                                    │
│    │        └── handler.handle(process)    │                                    │
│    │              │                        │                                    │
│    │              ▼                        │                                    │
│    │                                       │                                    │
│    └── jobHandler.onForeground(this) / onBackground(this)                      │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第六阶段：AnnotatedCommandImpl —— 注解命令执行                                    │
│                                                                                 │
│  ProcessHandler.handle(process)                                                 │
│    │                                                                            │
│    └──→ AnnotatedCommandImpl.process(process)                                  │
│              │                                                                  │
│              ├── clazz.newInstance()                → 反射创建命令实例              │
│              ├── CLIConfigurator.inject(cl, instance) → 注入命令行参数            │
│              └── instance.process(process)          → 执行具体命令逻辑            │
│                     │                                                           │
│                     ├── process.appendResult(model)  → 输出结果                  │
│                     └── process.end()                → 结束命令                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第七阶段：结果渲染与输出                                                          │
│                                                                                 │
│  process.appendResult(ResultModel)                                              │
│    │                                                                            │
│    └──→ ProcessImpl.appendResult(result)                                       │
│              │                                                                  │
│              └──→ resultDistributor.appendResult(result)                        │
│                        │                                                        │
│                        └──→ TermResultDistributorImpl.appendResult(result)      │
│                                  │                                              │
│                                  ├── resultViewResolver.getResultView(model)    │
│                                  │      │                                       │
│                                  │      └── resultViewMap.get(model.getClass()) │
│                                  │                                              │
│                                  └── resultView.draw(commandProcess, model)     │
│                                           │                                     │
│                                           └── process.write(data)              │
│                                                  │                              │
│                                                  └── processOutput.write(data) │
│                                                         │                       │
│                                                         └── stdoutHandlerChain │
│                                                              │  │  │           │
│                                                              ▼  ▼  ▼           │
│                                                         GrepHandler            │
│                                                         PlainTextHandler       │
│                                                         TermHandler → 终端输出  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 第八阶段：命令结束 & 资源回收                                                     │
│                                                                                 │
│  process.end(statusCode, message)                                               │
│    │                                                                            │
│    └──→ ProcessImpl.terminate(statusCode, null, message)                        │
│              │                                                                  │
│              ├── appendResult(new StatusModel(exitCode, message))               │
│              ├── processOutput.close()     → 关闭输出流                          │
│              ├── updateStatus(TERMINATED)  → 状态更新                            │
│              │      │                                                           │
│              │      └── terminatedHandler.handle(exitCode)                      │
│              │             │                                                    │
│              │             ├── jobHandler.onTerminated(job) → 通知 Shell         │
│              │             ├── controller.removeJob(id)      → 移除 Job          │
│              │             └── terminateFuture.complete()     → Future 完成      │
│              │                                                                  │
│              └── process.unregister()      → 卸载字节码增强                       │
│                     │                                                           │
│                     ├── TransformerManager.removeTransformer(transformer)        │
│                     └── AdviceWeaver.unReg(listener)                            │
│                                                                                 │
│  ShellJobHandler.onTerminated(job)                                              │
│    │                                                                            │
│    ├── shell.setForegroundJob(null) → 清除前台 Job                               │
│    ├── saveCommandHistory()         → 保存命令历史                               │
│    └── shell.readline()             → 重新开始读取下一条命令                       │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

> **类比理解**：整个命令执行链路就像一家"餐厅"的运转流程。ShellServer 是**餐厅大门**（接待客人入场），
> ShellImpl 是**服务员**（为每个客人分配座位、记录点单），ShellLineHandler 是**点菜系统**
> （解析菜名、分发给后厨），JobControllerImpl 是**厨房调度**（分配灶位、管理多个订单），
> ProcessImpl 是**厨师**（真正执行炒菜操作），ResultDistributor 是**传菜员**（把做好的菜端给客人），
> 而 ResultViewResolver 是**摆盘师**（把菜品摆出漂亮的造型呈现给客人）。

---

## 第一阶段：ShellServerImpl —— Shell 服务端的心脏

### 1.1 ShellServer 抽象类 —— 定义 Shell 服务端的契约

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/ShellServer.java`

在分析 ShellServerImpl 之前，我们先看它的父类 ShellServer。这是一个抽象类，定义了 Shell 服务端的核心能力：

```java
public abstract class ShellServer {

    public abstract ShellServer registerCommandResolver(CommandResolver resolver);

    public abstract ShellServer registerTermServer(TermServer termServer);

    public abstract ShellServer listen(Handler<Future<Void>> listenHandler);

    public abstract Shell createShell();

    public abstract Shell createShell(Term term);

    public abstract void close(Handler<Future<Void>> completionHandler);
}
```

这个抽象类非常简洁，但它清晰地表达了 Shell 服务端需要具备的几个核心能力：

| 方法 | 职责 | 类比 |
|------|------|------|
| `registerCommandResolver` | 注册命令解析器 | 餐厅添加新菜单 |
| `registerTermServer` | 注册终端服务器 | 开设新的入口大门 |
| `listen` | 启动监听 | 餐厅正式开业 |
| `createShell` | 创建 Shell 会话 | 为客人安排座位 |
| `close` | 关闭服务 | 餐厅打烊 |

**这一步做了什么？** ShellServer 是一个典型的 **模板方法模式** 的体现。它定义了 Shell 服务端的行为骨架，
具体的实现则交给子类 ShellServerImpl。这种设计允许未来替换不同的实现，比如用于测试的 Mock 实现。

---

### 1.2 ShellServerImpl —— Shell 服务端的实际实现

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/impl/ShellServerImpl.java`

ShellServerImpl 是整个 Arthas 命令系统的**枢纽中心**。让我们先来看它持有的核心字段：

```java
public class ShellServerImpl extends ShellServer {

    private static final Logger logger = LoggerFactory.getLogger(ShellServerImpl.class);

    private final CopyOnWriteArrayList<CommandResolver> resolvers;
    private final InternalCommandManager commandManager;
    private final List<TermServer> termServers;
    private final long timeoutMillis;
    private final long reaperInterval;
    private String welcomeMessage;
    private Instrumentation instrumentation;
    private long pid;
    private boolean closed = true;
    private final Map<String, ShellImpl> sessions;
    private final Future<Void> sessionsClosed = Future.future();
    private ScheduledExecutorService scheduledExecutorService;
    private JobControllerImpl jobController = new GlobalJobControllerImpl();
```

这里有几个字段值得特别关注：

#### 1.2.1 resolvers —— 命令解析器列表

```java
private final CopyOnWriteArrayList<CommandResolver> resolvers;
```

**它为什么存在？** `resolvers` 用 `CopyOnWriteArrayList` 存储所有已注册的命令解析器。
使用 `CopyOnWriteArrayList` 而非普通的 `ArrayList`，是因为命令注册可能在运行时动态发生
（比如加载新的命令插件），而命令查找会在每次用户输入时频繁执行。
`CopyOnWriteArrayList` 的"读多写少"特性非常适合这个场景 —— 命令注册（写操作）极其稀少，
但命令查找（读操作）每次用户输入命令都会触发。

#### 1.2.2 sessions —— 会话管理

```java
private final Map<String, ShellImpl> sessions;
```

在构造函数中被初始化为 `ConcurrentHashMap`：

```java
this.sessions = new ConcurrentHashMap<String, ShellImpl>();
```

**它为什么存在？** 一个 Arthas 实例可能同时被多个终端连接（telnet、WebSocket），
每个连接对应一个独立的 Shell 会话。`sessions` 就是管理这些并发会话的容器。
使用 `ConcurrentHashMap` 确保了多线程安全 —— 当一个用户正在连接时，另一个用户可能正在断开。

#### 1.2.3 jobController —— 全局 Job 控制器

```java
private JobControllerImpl jobController = new GlobalJobControllerImpl();
```

**它为什么存在？** 这里使用的是 `GlobalJobControllerImpl`（而非普通的 `JobControllerImpl`），
这意味着所有会话共享同一个 Job 控制器。这种设计使得不同 telnet 会话之间可以看到彼此的后台任务。
比如用户 A 启动了一个 `trace` 命令并放入后台，用户 B 通过 `jobs` 命令也能看到这个后台任务。

---

### 1.3 构造函数 —— 初始化 Shell 服务端

```java
public ShellServerImpl(ShellServerOptions options) {
    this.welcomeMessage = options.getWelcomeMessage();
    this.termServers = new ArrayList<TermServer>();
    this.timeoutMillis = options.getSessionTimeout();
    this.sessions = new ConcurrentHashMap<String, ShellImpl>();
    this.reaperInterval = options.getReaperInterval();
    this.resolvers = new CopyOnWriteArrayList<CommandResolver>();
    this.commandManager = new InternalCommandManager(resolvers);
    this.instrumentation = options.getInstrumentation();
    this.pid = options.getPid();

    // Register builtin commands so they are listed in help
    resolvers.add(new BuiltinCommandResolver());
}
```

逐行解读：

1. **`this.welcomeMessage = options.getWelcomeMessage()`**：从配置中获取欢迎消息。
   用户连接后会看到那个经典的 Arthas Logo 和版本信息，就是这个 welcomeMessage。

2. **`this.timeoutMillis = options.getSessionTimeout()`**：会话超时时间，默认 3 小时。
   超过这个时间没有活动的会话会被自动清理。

3. **`this.commandManager = new InternalCommandManager(resolvers)`**：创建命令管理器。
   注意，这里直接把 `resolvers` 引用传给了 `InternalCommandManager`，这意味着后续对 `resolvers`
   的修改会立即被 commandManager 感知。这是一个巧妙的"共享引用"设计。

4. **`resolvers.add(new BuiltinCommandResolver())`**：注册内建命令解析器。
   `BuiltinCommandResolver` 只包含 `exit` 和 `help` 等最基础的内建命令，
   它和 `BuiltinCommandPack`（包含 40+ 命令）是不同的。

---

### 1.4 ShellServerOptions —— 服务端配置

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/ShellServerOptions.java`

```java
public class ShellServerOptions {

    public static final long DEFAULT_REAPER_INTERVAL = 60 * 1000; // 60 seconds
    public static final long DEFAULT_SESSION_TIMEOUT = 3 * 60 * 60 * 1000; // 3 hours
    public static final long DEFAULT_CONNECTION_TIMEOUT = 6000; // 6 seconds

    public static final String DEFAULT_WELCOME_MESSAGE = ArthasBanner.welcome();
    public static final String DEFAULT_INPUTRC = "com/taobao/arthas/core/shell/term/readline/inputrc";

    private String welcomeMessage;
    private long sessionTimeout;
    private long reaperInterval;
    private long connectionTimeout;
    private long pid;
    private Instrumentation instrumentation;
}
```

| 配置项 | 默认值 | 含义 |
|--------|--------|------|
| `DEFAULT_REAPER_INTERVAL` | 60 秒 | 会话回收器的检查间隔 |
| `DEFAULT_SESSION_TIMEOUT` | 3 小时 | 会话的最大空闲时间 |
| `DEFAULT_CONNECTION_TIMEOUT` | 6 秒 | 客户端连接超时时间 |
| `DEFAULT_WELCOME_MESSAGE` | Arthas Banner | 连接时的欢迎信息 |

**这一步做了什么？** ShellServerOptions 是一个典型的 **Builder/Options 模式** 的配置类。
所有 setter 方法都返回 `this`，支持链式调用：

```java
public ShellServerOptions setSessionTimeout(long sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
    return this;
}
```

---

### 1.5 listen() 方法 —— 启动监听

```java
@Override
public ShellServer listen(final Handler<Future<Void>> listenHandler) {
    final List<TermServer> toStart;
    synchronized (this) {
        if (!closed) {
            throw new IllegalStateException("Server listening");
        }
        toStart = termServers;
    }
    final AtomicInteger count = new AtomicInteger(toStart.size());
    if (count.get() == 0) {
        setClosed(false);
        listenHandler.handle(Future.<Void>succeededFuture());
        return this;
    }
    Handler<Future<TermServer>> handler = new TermServerListenHandler(this, listenHandler, toStart);
    for (TermServer termServer : toStart) {
        termServer.termHandler(new TermServerTermHandler(this));
        termServer.listen(handler);
    }
    return this;
}
```

逐段解读：

**第一段：状态检查**

```java
synchronized (this) {
    if (!closed) {
        throw new IllegalStateException("Server listening");
    }
    toStart = termServers;
}
```

用 `synchronized` 保护 `closed` 状态的读取，确保不会重复启动。如果 `closed` 为 `false`，
说明服务已经在运行中，抛出异常防止重入。

**第二段：无 TermServer 的退化处理**

```java
if (count.get() == 0) {
    setClosed(false);
    listenHandler.handle(Future.<Void>succeededFuture());
    return this;
}
```

如果没有注册任何 TermServer，直接标记为已启动，并通知监听处理器"成功了"。
这是一个 **防御性编程** 的典范 —— 处理了边界情况。

**第三段：启动所有 TermServer**

```java
for (TermServer termServer : toStart) {
    termServer.termHandler(new TermServerTermHandler(this));
    termServer.listen(handler);
}
```

遍历所有注册的 TermServer（可能是 Telnet 服务器、HTTP/WebSocket 服务器），
为每个服务器设置连接处理器 `TermServerTermHandler`，然后启动监听。

**关键设计点**：`termServer.termHandler(new TermServerTermHandler(this))` 这行代码把
`this`（即 ShellServerImpl 自己）传给了 `TermServerTermHandler`。这意味着当有新连接到来时，
`TermServerTermHandler` 会回调 `ShellServerImpl.handleTerm(term)` 方法。
这是一个经典的 **回调模式** 的应用。

---

### 1.6 TermServerTermHandler —— 连接事件的桥梁

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/handlers/server/TermServerTermHandler.java`

```java
public class TermServerTermHandler implements Handler<Term> {

    private ShellServerImpl shellServer;

    public TermServerTermHandler(ShellServerImpl shellServer) {
        this.shellServer = shellServer;
    }

    @Override
    public void handle(Term term) {
        shellServer.handleTerm(term);
    }
}
```

**这一步做了什么？** `TermServerTermHandler` 是一个极其简单的**适配器/桥梁**。它的唯一职责就是把
TermServer 的连接事件（`Handler<Term>`）转发给 `ShellServerImpl.handleTerm(term)`。

**它为什么存在？** 为什么不让 ShellServerImpl 直接实现 `Handler<Term>` 接口？因为：
1. ShellServerImpl 已经继承了 ShellServer，Java 不支持多重继承。
2. 将连接处理逻辑封装为独立的 Handler，符合**单一职责原则**。
3. 将来如果需要在连接处理前添加拦截逻辑（如连接数限制、IP 白名单），只需要替换这个 Handler。

---

### 1.7 handleTerm(Term term) —— 新连接的入口

```java
public void handleTerm(Term term) {
    synchronized (this) {
        // That might happen with multiple ser
        if (closed) {
            term.close();
            return;
        }
    }

    ShellImpl session = createShell(term);
    tryUpdateWelcomeMessage();
    session.setWelcome(welcomeMessage);
    session.closedFuture.setHandler(new SessionClosedHandler(this, session));
    session.init();
    sessions.put(session.id, session);
    session.readline(); // Now readline
}
```

这是当有新终端连接时的核心处理流程，让我们逐行分析：

**第一步：状态检查**

```java
synchronized (this) {
    if (closed) {
        term.close();
        return;
    }
}
```

如果服务已经关闭，直接关闭这个新连接。注意 `synchronized` 保护了对 `closed` 的读取，
避免了在关闭过程中有新连接"漏进来"。

**第二步：创建 Shell 会话**

```java
ShellImpl session = createShell(term);
```

调用 `createShell(term)` 方法：

```java
@Override
public synchronized ShellImpl createShell(Term term) {
    if (closed) {
        throw new IllegalStateException("Closed");
    }
    return new ShellImpl(this, term, commandManager, instrumentation, pid, jobController);
}
```

注意这里传递了六个参数给 ShellImpl：
- `this`：ShellServer 本身（用于会话关闭时回调）
- `term`：终端连接（用于读写数据）
- `commandManager`：命令管理器（用于查找和执行命令）
- `instrumentation`：Java Instrumentation 实例（用于字节码增强）
- `pid`：目标 JVM 进程 ID
- `jobController`：全局 Job 控制器

**第三步：更新欢迎信息**

```java
tryUpdateWelcomeMessage();
session.setWelcome(welcomeMessage);
```

```java
private void tryUpdateWelcomeMessage() {
    TunnelClient tunnelClient = ArthasBootstrap.getInstance().getTunnelClient();
    if (tunnelClient != null) {
        String id = tunnelClient.getId();
        if (id != null) {
            Map<String, String> welcomeInfos = new HashMap<String, String>();
            welcomeInfos.put("id", id);
            this.welcomeMessage = ArthasBanner.welcome(welcomeInfos);
        }
    }
}
```

如果配置了 Arthas Tunnel（用于远程连接），欢迎信息中会包含 Tunnel 的连接 ID。

**第四步：注册关闭处理器**

```java
session.closedFuture.setHandler(new SessionClosedHandler(this, session));
```

当会话关闭时，`SessionClosedHandler` 会被触发，它会调用 `ShellServerImpl.removeSession(shell)`
来清理会话资源。

**第五步：初始化并开始读取**

```java
session.init();
sessions.put(session.id, session); // Put after init
session.readline(); // Now readline
```

注意代码注释 "Put after init so the close handler on the connection is set"。
这是一个重要的顺序保证 —— 必须先调用 `init()`（注册了连接关闭处理器），然后才把会话放入 sessions Map。
如果顺序反过来，可能出现会话还没注册关闭处理器就断开了连接，导致资源泄漏。

最后调用 `session.readline()` 开始读取用户的第一条命令。

---

### 1.8 evictSessions() —— 会话回收器

```java
private void evictSessions() {
    long now = System.currentTimeMillis();
    Set<ShellImpl> toClose = new HashSet<ShellImpl>();
    for (ShellImpl session : sessions.values()) {
        // do not close if there is still job running,
        // e.g. trace command might wait for a long time before condition is met
        if (now - session.lastAccessedTime() > timeoutMillis && session.jobs().size() == 0) {
            toClose.add(session);
        }
        logger.debug(session.id + ":" + session.lastAccessedTime());
    }
    for (ShellImpl session : toClose) {
        long timeOutInMinutes = timeoutMillis / 1000 / 60;
        String reason = "session is inactive for " + timeOutInMinutes + " min(s).";
        session.close(reason);
    }
}
```

**它为什么存在？** 防止僵尸会话占用资源。但这里有一个非常重要的判断条件：
`session.jobs().size() == 0`。即使会话超时了，只要还有正在运行的 Job（比如 `trace` 命令正在等待触发条件），
就不会关闭。这体现了 Arthas 的实用主义设计 —— 用户可能启动了一个长时间运行的追踪任务后离开终端，
不应该因为超时而中断这个有价值的追踪。

---

### 1.9 removeSession() —— 会话移除

```java
public void removeSession(ShellImpl shell) {
    boolean completeSessionClosed;

    Job job = shell.getForegroundJob();
    if (job != null) {
        job.terminate();
        logger.info("Session {} closed, so terminate foreground job, id: {}, line: {}",
                    shell.session().getSessionId(), job.id(), job.line());
    }

    synchronized (ShellServerImpl.this) {
        sessions.remove(shell.id);
        shell.close("network error");
        completeSessionClosed = sessions.isEmpty() && closed;
    }
    if (completeSessionClosed) {
        sessionsClosed.complete();
    }
}
```

**关键逻辑**：当一个会话被移除时：
1. 先终止该会话的前台 Job（如果有的话）
2. 从 sessions Map 中移除
3. 关闭 Shell
4. 如果这是最后一个会话且服务正在关闭，标记 `sessionsClosed` Future 完成

**它为什么存在？** 这是资源清理的核心方法。当用户断开连接或会话超时时，必须确保所有相关的 Job 被终止，
否则可能导致被增强的类一直保持增强状态，影响目标应用的性能。

---

## 第二阶段：ShellImpl —— Shell 会话

### 2.1 ShellImpl 类概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/impl/ShellImpl.java`

ShellImpl 是每个终端连接对应的 Shell 会话实体。它是用户与 Arthas 交互的"中间人"。

```java
public class ShellImpl implements Shell {
    private static final Logger logger = LoggerFactory.getLogger(ShellImpl.class);
    private static final String ARTHAS_AGENT_TERMINAL_TYPE = "arthas-agent";

    private JobControllerImpl jobController;
    final String id;
    final Future<Void> closedFuture;
    private InternalCommandManager commandManager;
    private Session session = new SessionImpl();
    private Term term;
    private String welcome;
    private Job currentForegroundJob;
    private String prompt;
}
```

核心字段解读：

| 字段 | 类型 | 职责 |
|------|------|------|
| `id` | String | 会话唯一标识（UUID） |
| `jobController` | JobControllerImpl | Job 控制器（全局共享） |
| `commandManager` | InternalCommandManager | 命令管理器 |
| `session` | Session | 会话上下文（存储 key-value 数据） |
| `term` | Term | 终端连接抽象 |
| `currentForegroundJob` | Job | 当前前台运行的 Job |
| `prompt` | String | 命令提示符（如 `[arthas@12345]$`） |
| `closedFuture` | Future | 会话关闭的 Future |

---

### 2.2 构造函数 —— 会话初始化与鉴权

ShellImpl 的构造函数非常丰富，它不仅完成了字段初始化，还处理了鉴权逻辑：

```java
public ShellImpl(ShellServer server, Term term, InternalCommandManager commandManager,
        Instrumentation instrumentation, long pid, JobControllerImpl jobController) {
    if (term instanceof TermImpl) {
        TermImpl termImpl = (TermImpl) term;
        TtyConnection conn = termImpl.getConn();
        // 处理telnet本地连接鉴权
        if (conn instanceof TelnetTtyConnection) {
            TelnetConnection telnetConnection = ((TelnetTtyConnection) conn).getTelnetConnection();
            if (telnetConnection instanceof NettyTelnetConnection) {
                ChannelHandlerContext handlerContext = ((NettyTelnetConnection) telnetConnection)
                        .channelHandlerContext();
                Principal principal = AuthUtils.localPrincipal(handlerContext);
                if (principal != null) {
                    try {
                        SecurityAuthenticator securityAuthenticator =
                            ArthasBootstrap.getInstance().getSecurityAuthenticator();
                        Subject subject = securityAuthenticator.login(principal);
                        if (subject != null) {
                            session.put(ArthasConstants.SUBJECT_KEY, subject);
                        }
                    } catch (LoginException e) {
                        logger.error("local connection auth error", e);
                    }
                }
            }
        }
```

这段代码层层递进地获取底层连接信息：
`Term` → `TermImpl` → `TtyConnection` → `TelnetTtyConnection` → `TelnetConnection`
→ `NettyTelnetConnection` → `ChannelHandlerContext`。

最终通过 `AuthUtils.localPrincipal(handlerContext)` 获取本地连接的身份信息，
然后通过 `SecurityAuthenticator` 进行登录鉴权。如果鉴权成功，将 `Subject`
存入 Session 中，后续命令执行时会检查这个 Subject。

接下来是 HTTP 连接的鉴权处理：

```java
        if (conn instanceof ExtHttpTtyConnection) {
            // 传递http cookie 里的鉴权信息到新建立的session中
            ExtHttpTtyConnection extConn = (ExtHttpTtyConnection) conn;
            Map<String, Object> extSessions = extConn.extSessions();
            for (Entry<String, Object> entry : extSessions.entrySet()) {
                session.put(entry.getKey(), entry.getValue());
            }
        }
    }
```

对于 HTTP/WebSocket 连接，将 Cookie 中的鉴权信息传递到 Session 中。

然后是核心数据的初始化：

```java
    if (term != null && ARTHAS_AGENT_TERMINAL_TYPE.equalsIgnoreCase(term.type())) {
        session.put(Session.QUIET, Boolean.TRUE);
    }
    session.put(Session.COMMAND_MANAGER, commandManager);
    session.put(Session.INSTRUMENTATION, instrumentation);
    session.put(Session.PID, pid);
    session.put(Session.SERVER, server);
    session.put(Session.TTY, term);
    this.id = UUID.randomUUID().toString();
    session.put(Session.ID, id);
    this.commandManager = commandManager;
    this.closedFuture = Future.future();
    this.term = term;
    this.jobController = jobController;

    if (term != null) {
        term.setSession(session);
    }

    this.setPrompt();
}
```

Session 本质上是一个 `ConcurrentHashMap`，这里把各种关键对象存入其中，
使得后续命令执行时可以通过 `session.get(key)` 获取到这些对象。
这是一种 **上下文传递** 的设计模式 —— 将全局或会话级别的对象通过 Session 容器进行传递，
避免了方法参数的层层传递。

**关于 QUIET 模式**：如果终端类型是 `"arthas-agent"`（即通过 Arthas Agent API 连接），
则设置 `QUIET` 标志。在 quiet 模式下不会输出欢迎信息，适合程序化调用的场景。

---

### 2.3 init() 方法 —— 注册信号处理器

```java
public ShellImpl init() {
    term.interruptHandler(new InterruptHandler(this));
    term.suspendHandler(new SuspendHandler(this));
    term.closeHandler(new CloseHandler(this));

    if (!isQuietSession() && welcome != null && welcome.length() > 0) {
        term.write(welcome + "\n");
    }
    return this;
}
```

`init()` 方法做了两件事：
1. 注册三个信号处理器（Ctrl+C、Ctrl+Z、连接关闭）
2. 向终端写入欢迎信息

这三个信号处理器对应了 Unix 终端的标准信号：

| 信号 | 快捷键 | Handler | 行为 |
|------|--------|---------|------|
| SIGINT | Ctrl+C | InterruptHandler | 中断当前前台 Job |
| SIGTSTP | Ctrl+Z | SuspendHandler | 挂起当前前台 Job |
| 连接关闭 | - | CloseHandler | 清理会话资源 |

---

### 2.4 setPrompt() —— 设置命令提示符

```java
private void setPrompt(){
    this.prompt = "[arthas@" +
            session.getPid() +
            "]$ ";
}
```

生成类似 `[arthas@12345]$` 的提示符，其中 12345 是目标 JVM 的进程 ID。
这让用户一眼就知道当前正在诊断哪个进程。

---

### 2.5 readline() 方法 —— 读取用户输入

```java
public void readline() {
    term.readline(prompt, new ShellLineHandler(this),
            new CommandManagerCompletionHandler(commandManager));
}
```

这是 Shell 交互循环的核心。`term.readline()` 接收三个参数：
1. `prompt`：命令提示符（`[arthas@12345]$`）
2. `ShellLineHandler`：用户输入一行后的处理器
3. `CommandManagerCompletionHandler`：Tab 补全处理器

**它为什么存在？** `readline()` 实现了一个 **异步事件驱动** 的交互模型。它不会阻塞当前线程等待用户输入，
而是注册回调处理器后立即返回。当用户真正输入一行并按下回车时，`ShellLineHandler.handle(line)` 才会被调用。

这与传统的同步 `Scanner.nextLine()` 模型完全不同。异步模型的优势在于一个线程可以服务多个终端连接。

---

### 2.6 createJob() 方法 —— 创建 Job

```java
@Override
public synchronized Job createJob(List<CliToken> args) {
    Job job = jobController.createJob(commandManager, args, session,
            new ShellJobHandler(this), term, null);
    return job;
}

@Override
public Job createJob(String line) {
    return createJob(CliTokens.tokenize(line));
}
```

`createJob` 有两个重载版本：
- 一个接受已分词的 `List<CliToken>`
- 一个接受原始字符串 `line`（会先调用 `CliTokens.tokenize(line)` 进行分词）

注意 `synchronized` 关键字 —— 同一个 Shell 实例不能同时创建多个 Job，
这保证了 Job ID 的唯一性和会话状态的一致性。

传给 `jobController.createJob()` 的 `new ShellJobHandler(this)` 是一个重要的回调对象：

---

### 2.7 ShellJobHandler —— Shell 级别的 Job 生命周期回调

```java
private static class ShellJobHandler implements JobListener {
    ShellImpl shell;

    public ShellJobHandler(ShellImpl shell) {
        this.shell = shell;
    }

    @Override
    public void onForeground(Job job) {
        shell.setForegroundJob(job);
    }

    @Override
    public void onBackground(Job job) {
        resetAndReadLine();
    }

    @Override
    public void onTerminated(Job job) {
        if (!job.isRunInBackground()){
            resetAndReadLine();
        }
        // save command history
        Term term = shell.term();
        if (term instanceof TermImpl) {
            List<int[]> history = ((TermImpl) term).getReadline().getHistory();
            FileUtils.saveCommandHistory(history, new File(Constants.CMD_HISTORY_FILE));
        }
    }

    @Override
    public void onSuspend(Job job) {
        if (!job.isRunInBackground()){
            resetAndReadLine();
        }
    }

    private void resetAndReadLine() {
        shell.setForegroundJob(null);
        shell.readline();
    }
}
```

这是 Shell 与 Job 之间的"联络员"。当 Job 状态发生变化时，ShellJobHandler 负责更新 Shell 的状态：

| 事件 | 行为 |
|------|------|
| `onForeground` | 设置当前前台 Job |
| `onBackground` | 清除前台 Job，开始读取下一条命令 |
| `onTerminated` | 如果是前台 Job，清除并读取下一条命令；保存命令历史 |
| `onSuspend` | 如果是前台 Job，清除并读取下一条命令 |

**关键设计**：`resetAndReadLine()` 做了两件事：
1. `shell.setForegroundJob(null)` —— 清除前台 Job
2. `shell.readline()` —— 重新显示提示符，开始读取下一条命令

这就是为什么当你执行完一条命令后，会重新看到 `[arthas@12345]$` 提示符的原因。
`readline()` 的调用形成了一个 **隐式的事件循环**：readline → 用户输入 → handler 处理 →
job 终止 → onTerminated → readline → ...

---

### 2.8 close() 方法 —— 关闭会话

```java
public void close(String reason) {
    if (term != null) {
        try {
            term.write("session (" + session.getSessionId() + ") is closed because " + reason + "\n");
        } catch (Throwable t) {
            logger.error("Error writing data:", t);
        }
        term.close();
    } else {
        jobController.close(closedFutureHandler());
    }
}
```

关闭会话时，先向终端写入关闭原因（如 "session is inactive for 180 min(s)."），
然后关闭终端连接。注意这里用了 try-catch 包裹写入操作，因为在 WebSocket 断开的场景下，
写入可能会抛出 NPE（见代码注释中引用的 GitHub Issue #320）。

---

## 第三阶段：ShellLineHandler —— 命令行处理器

### 3.1 ShellLineHandler 类概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/handlers/shell/ShellLineHandler.java`

ShellLineHandler 是连接"用户输入"和"命令执行"的桥梁。当用户输入一行命令并按下回车后，
这个 Handler 负责解析和分发命令。

```java
public class ShellLineHandler implements Handler<String> {

    private ShellImpl shell;
    private Term term;

    public ShellLineHandler(ShellImpl shell) {
        this.shell = shell;
        this.term = shell.term();
    }
}
```

构造函数很简单，保存了 Shell 实例和 Term 终端引用。

---

### 3.2 handle(String line) —— 核心处理方法

```java
@Override
public void handle(String line) {
    if (line == null) {
        // EOF
        handleExit();
        return;
    }

    List<CliToken> tokens = CliTokens.tokenize(line);
    CliToken first = TokenUtils.findFirstTextToken(tokens);
    if (first == null) {
        // For now do like this
        shell.readline();
        return;
    }

    String name = first.value();
    if (name.equals("exit") || name.equals("logout") || name.equals("q") || name.equals("quit")) {
        handleExit();
        return;
    } else if (name.equals("jobs")) {
        handleJobs();
        return;
    } else if (name.equals("fg")) {
        handleForeground(tokens);
        return;
    } else if (name.equals("bg")) {
        handleBackground(tokens);
        return;
    } else if (name.equals("kill")) {
        handleKill(tokens);
        return;
    }

    Job job = createJob(tokens);
    if (job != null) {
        job.run();
    }
}
```

让我们逐步分析这个方法的执行流程：

**第一步：EOF 处理**

```java
if (line == null) {
    handleExit();
    return;
}
```

当 `line` 为 `null` 时，说明终端发送了 EOF 信号（Ctrl+D），此时退出会话。

**第二步：分词**

```java
List<CliToken> tokens = CliTokens.tokenize(line);
```

将用户输入的命令行字符串拆分为 Token 列表。例如 `watch Demo test` 会被拆分为：
- Token(" ") —— 空白 Token（blank）
- Token("watch") —— 文本 Token
- Token(" ") —— 空白 Token
- Token("Demo") —— 文本 Token
- Token(" ") —— 空白 Token
- Token("test") —— 文本 Token

**第三步：提取命令名**

```java
CliToken first = TokenUtils.findFirstTextToken(tokens);
if (first == null) {
    shell.readline();
    return;
}
String name = first.value();
```

`TokenUtils.findFirstTextToken(tokens)` 跳过空白 Token，找到第一个文本 Token（即命令名）。
如果用户只输入了空白（按了回车），则直接重新读取下一行。

**第四步：内建命令处理**

内建命令（exit/jobs/fg/bg/kill）由 ShellLineHandler 直接处理，不经过 Job 系统：

```java
if (name.equals("exit") || name.equals("logout") || name.equals("q") || name.equals("quit")) {
    handleExit();
    return;
} else if (name.equals("jobs")) {
    handleJobs();
    return;
} else if (name.equals("fg")) {
    handleForeground(tokens);
    return;
} else if (name.equals("bg")) {
    handleBackground(tokens);
    return;
} else if (name.equals("kill")) {
    handleKill(tokens);
    return;
}
```

**为什么这些命令要在 ShellLineHandler 中直接处理，而不是像其他命令一样走 Job 系统？**

因为这些命令本身就是用来 **管理 Job** 的。如果 `kill` 命令本身也是一个 Job，
那么当你要 kill 一个阻塞的 Job 时，kill 命令的 Job 会被阻塞在等待队列中，
形成"死锁"（不是真正的线程死锁，但逻辑上陷入了循环等待）。
这就像"厨房调度员"不能自己变成一个"菜品订单"去排队等待处理。

**第五步：普通命令处理**

```java
Job job = createJob(tokens);
if (job != null) {
    job.run();
}
```

对于所有非内建命令，创建一个 Job 并运行它。

---

### 3.3 handleExit() —— 退出处理

```java
private void handleExit() {
    String msg = Ansi.ansi().fg(Ansi.Color.GREEN).a("Session has been terminated.\n"
            + "Arthas is still running in the background.\n"
            + "To completely shutdown arthas, please execute the 'stop' command.\n").reset().toString();
    term.write(msg);
    term.close();
}
```

退出会话时输出一段绿色提示信息，告诉用户：
- Session 已终止
- Arthas 仍在后台运行（不会影响目标 JVM）
- 如果想完全停止 Arthas，使用 `stop` 命令

这里使用了 ANSI 转义码 `Ansi.ansi().fg(Ansi.Color.GREEN)` 来设置文本颜色。

---

### 3.4 handleJobs() —— 查看所有 Job

```java
private void handleJobs() {
    for (Job job : shell.jobController().jobs()) {
        String statusLine = shell.statusLine(job, job.status());
        term.write(statusLine);
    }
    shell.readline();
}
```

遍历所有 Job，输出状态信息后重新开始读取命令。

`shell.statusLine(job, status)` 的实现值得一看：

```java
public String statusLine(Job job, ExecStatus status) {
    StringBuilder sb = new StringBuilder("[").append(job.id()).append("]");
    if (this.session().equals(job.getSession())) {
        sb.append("*");
    }
    sb.append("\n");
    sb.append("       ").append(Character.toUpperCase(status.name().charAt(0)))
            .append(status.name().substring(1).toLowerCase());
    sb.append("           ").append(job.line()).append("\n");
    sb.append("       execution count : ").append(job.process().times()).append("\n");
    sb.append("       start time      : ").append(job.process().startTime()).append("\n");
    String cacheLocation = job.process().cacheLocation();
    if (cacheLocation != null) {
        sb.append("       cache location  : ").append(cacheLocation).append("\n");
    }
    Date timeoutDate = job.timeoutDate();
    if (timeoutDate != null) {
        sb.append("       timeout date    : ").append(timeoutDate).append("\n");
    }
    sb.append("       session         : ").append(job.getSession().getSessionId()).append(
            session.equals(job.getSession()) ? " (current)" : "").append("\n");
    return sb.toString();
}
```

输出格式类似：
```
[1]*
       Running           trace Demo test
       execution count : 5
       start time      : Mon Jan 01 10:00:00 CST 2024
       timeout date    : Tue Jan 02 10:00:00 CST 2024
       session         : abc-123 (current)
```

其中 `*` 表示该 Job 属于当前会话。

---

### 3.5 handleForeground(tokens) —— 前台切换

```java
private void handleForeground(List<CliToken> tokens) {
    String arg = TokenUtils.findSecondTokenText(tokens);
    Job job;
    if (arg == null) {
        job = shell.getForegroundJob();
    } else {
        job = shell.jobController().getJob(getJobId(arg));
    }
    if (job == null) {
        term.write(arg + " : no such job\n");
        shell.readline();
    } else {
        if (job.getSession() != shell.session()) {
            term.write("job " + job.id() + " doesn't belong to this session, so can not fg it\n");
            shell.readline();
        } else if (job.status() == ExecStatus.STOPPED) {
            job.resume(true);
        } else if (job.status() == ExecStatus.RUNNING) {
            job.toForeground();
        } else {
            term.write("job " + job.id() + " is already terminated, so can not fg it\n");
            shell.readline();
        }
    }
}
```

`fg` 命令的逻辑：
1. 如果没有指定 Job ID，默认使用最近挂起的前台 Job
2. 检查 Job 是否存在
3. 检查 Job 是否属于当前会话（不能 fg 其他会话的 Job）
4. 如果 Job 是 STOPPED 状态，恢复并切换到前台
5. 如果 Job 是 RUNNING 状态（在后台运行），切换到前台

**关键安全检查**：`job.getSession() != shell.session()` 确保了会话隔离。
用户 A 不能把用户 B 的后台 Job 切到自己的前台。

---

### 3.6 handleBackground(tokens) —— 后台恢复

```java
private void handleBackground(List<CliToken> tokens) {
    String arg = TokenUtils.findSecondTokenText(tokens);
    Job job;
    if (arg == null) {
        job = shell.getForegroundJob();
    } else {
        job = shell.jobController().getJob(getJobId(arg));
    }
    if (job == null) {
        term.write(arg + " : no such job\n");
        shell.readline();
    } else {
        if (job.status() == ExecStatus.STOPPED) {
            job.resume(false);
            term.echo(shell.statusLine(job, ExecStatus.RUNNING));
            shell.readline();
        } else {
            term.write("job " + job.id() + " is already running\n");
            shell.readline();
        }
    }
}
```

`bg` 命令的逻辑更简单：只接受 STOPPED 状态的 Job，将其恢复为后台运行。

---

### 3.7 handleKill(tokens) —— 终止 Job

```java
private void handleKill(List<CliToken> tokens) {
    String arg = TokenUtils.findSecondTokenText(tokens);
    if (arg == null) {
        term.write("kill: usage: kill job_id\n");
        shell.readline();
        return;
    }
    Job job = shell.jobController().getJob(getJobId(arg));
    if (job == null) {
        term.write(arg + " : no such job\n");
        shell.readline();
    } else {
        job.terminate();
        term.write("kill job " + job.id() + " success\n");
        shell.readline();
    }
}
```

`kill` 命令直接调用 `job.terminate()` 终止 Job，触发完整的资源清理流程。

---

### 3.8 createJob(tokens) —— 创建 Job 的包装方法

```java
private Job createJob(List<CliToken> tokens) {
    Job job;
    try {
        job = shell.createJob(tokens);
    } catch (Exception e) {
        term.echo(e.getMessage() + "\n");
        shell.readline();
        return null;
    }
    return job;
}
```

这个方法是 `shell.createJob(tokens)` 的异常安全包装。如果命令不存在或参数错误，
会捕获异常并输出错误信息，然后重新开始读取下一条命令，而不是让异常向上传播导致会话崩溃。

---

### 3.9 getJobId(String arg) —— 解析 Job ID

```java
private int getJobId(String arg) {
    int result = -1;
    try {
        if (arg.startsWith("%")) {
            result = Integer.parseInt(arg.substring(1));
        } else {
            result = Integer.parseInt(arg);
        }
    } catch (Exception e) {
    }
    return result;
}
```

支持两种格式的 Job ID：
- `%1` —— Unix 风格（带百分号前缀）
- `1` —— 直接数字

这与 Unix 的 `fg %1` 命令保持了一致的使用体验。

---

## 第四阶段：TokenUtils 与 CliTokens —— 命令行分词

### 4.1 CliToken 接口 —— Token 的抽象

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/cli/CliToken.java`

CliToken 是命令行 Token 的抽象接口，每个 Token 有两个核心属性：
- `value()` —— Token 的文本值
- `isText()` / `isBlank()` —— Token 的类型
- `raw()` —— 原始文本（包含引号等）

分词时，命令行字符串会被拆分为交替的"文本 Token"和"空白 Token"序列。

---

### 4.2 CliTokens.tokenize(line) —— 分词入口

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/cli/CliTokens.java`

`CliTokens.tokenize(line)` 是分词的入口方法，它将一行命令文本拆分为 Token 列表。
例如：

| 输入 | Token 列表 |
|------|------------|
| `watch Demo test` | [blank(" "), text("watch"), blank(" "), text("Demo"), blank(" "), text("test")] |
| `trace Demo test \| grep hello` | [text("trace"), blank(" "), text("Demo"), blank(" "), text("test"), blank(" "), text("\|"), blank(" "), text("grep"), blank(" "), text("hello")] |

管道符 `|` 和重定向符 `>` / `>>` 都会被作为独立的文本 Token。

---

### 4.3 TokenUtils —— Token 工具类

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/util/TokenUtils.java`

```java
public class TokenUtils {

    public static CliToken findFirstTextToken(List<CliToken> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            return null;
        }
        CliToken first = null;
        for (CliToken token : tokens) {
            if (token != null && token.isText()) {
                first = token;
                break;
            }
        }
        return first;
    }

    public static CliToken findLastTextToken(List<CliToken> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            return null;
        }
        for (int i = tokens.size() - 1; i >= 0; i--) {
            CliToken token = tokens.get(i);
            if (token != null && token.isText()) {
                return token;
            }
        }
        return null;
    }

    public static String findSecondTokenText(List<CliToken> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            return null;
        }
        boolean first = true;
        for (CliToken token : tokens) {
            if (token != null && token.isText()) {
                if (first) {
                    first = false;
                } else {
                    return token.value();
                }
            }
        }
        return null;
    }
}
```

TokenUtils 提供了三个常用的 Token 查找方法：

| 方法 | 职责 | 使用场景 |
|------|------|----------|
| `findFirstTextToken` | 找第一个文本 Token | 获取命令名 |
| `findLastTextToken` | 找最后一个文本 Token | 判断是否有 `&` 后台标记 |
| `findSecondTokenText` | 找第二个文本 Token 的值 | 获取 fg/bg/kill 的参数 |

**为什么这些方法要跳过空白 Token？** 因为用户可能在命令前后输入了多余的空格，
或者在参数之间使用了多个空格。跳过空白 Token 可以容错地处理这些情况。

---

## 第五阶段：Job 创建与 JobControllerImpl

### 5.1 JobController 接口 —— Job 控制器的契约

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/system/JobController.java`

JobController 定义了 Job 管理的核心接口：

```java
public interface JobController {
    Set<Job> jobs();
    Job getJob(int id);
    Job createJob(InternalCommandManager commandManager, List<CliToken> tokens,
                  Session session, JobListener jobHandler, Term term,
                  ResultDistributor resultDistributor);
    void close(Handler<Void> completionHandler);
    void close();
}
```

---

### 5.2 JobControllerImpl —— Job 控制器的核心实现

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/system/impl/JobControllerImpl.java`

```java
public class JobControllerImpl implements JobController {

    private final SortedMap<Integer, JobImpl> jobs = new TreeMap<Integer, JobImpl>();
    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private boolean closed = false;
```

核心字段解读：

- **`jobs`**：使用 `TreeMap` 存储所有 Job，按 ID 排序。这使得 `jobs` 命令输出的 Job 列表是有序的。
- **`idGenerator`**：使用 `AtomicInteger` 生成唯一的 Job ID，保证了多线程安全。

---

### 5.3 createJob() —— Job 创建的核心方法

```java
@Override
public Job createJob(InternalCommandManager commandManager, List<CliToken> tokens,
                     Session session, JobListener jobHandler, Term term,
                     ResultDistributor resultDistributor) {
    checkPermission(session, tokens.get(0));
    int jobId = idGenerator.incrementAndGet();
    StringBuilder line = new StringBuilder();
    for (CliToken arg : tokens) {
        line.append(arg.raw());
    }
    boolean runInBackground = runInBackground(tokens);
    Process process = createProcess(session, tokens, commandManager, jobId, term, resultDistributor);
    process.setJobId(jobId);
    JobImpl job = new JobImpl(jobId, this, process, line.toString(), runInBackground, session, jobHandler);
    jobs.put(jobId, job);
    return job;
}
```

逐步分析：

**第一步：权限检查**

```java
checkPermission(session, tokens.get(0));
```

```java
private void checkPermission(Session session, CliToken token) {
    if (ArthasBootstrap.getInstance().getSecurityAuthenticator().needLogin()) {
        Object subject = session.get(ArthasConstants.SUBJECT_KEY);
        if (subject == null) {
            if (token != null && token.isText() && token.value().trim().equals(ArthasConstants.AUTH)) {
                return;
            }
            throw new IllegalArgumentException(
                "Error! command not permitted, try to use 'auth' command to authenticates.");
        }
    }
}
```

如果开启了安全认证，用户在执行任何命令前必须先通过 `auth` 命令认证。
未认证的会话只允许执行 `auth` 命令本身 —— 这是一个"鸡生蛋"问题的巧妙解决方案。

**第二步：判断后台运行**

```java
private boolean runInBackground(List<CliToken> tokens) {
    boolean runInBackground = false;
    CliToken last = TokenUtils.findLastTextToken(tokens);
    if (last != null && "&".equals(last.value())) {
        runInBackground = true;
        tokens.remove(last);
    }
    return runInBackground;
}
```

检查命令行末尾是否有 `&` 符号。如果有，标记为后台运行并移除 `&` Token。
这与 Unix Shell 的后台运行语法 `command &` 完全一致。

**第三步：创建 Process**

```java
Process process = createProcess(session, tokens, commandManager, jobId, term, resultDistributor);
```

这是最复杂的一步，我们在下一节详细分析。

**第四步：创建 JobImpl**

```java
JobImpl job = new JobImpl(jobId, this, process, line.toString(), runInBackground, session, jobHandler);
jobs.put(jobId, job);
return job;
```

将 Job 放入 `jobs` Map 中管理。

---

### 5.4 createProcess() —— 命令查找

```java
private Process createProcess(Session session, List<CliToken> line,
                              InternalCommandManager commandManager, int jobId,
                              Term term, ResultDistributor resultDistributor) {
    try {
        ListIterator<CliToken> tokens = line.listIterator();
        while (tokens.hasNext()) {
            CliToken token = tokens.next();
            if (token.isText()) {
                checkPermission(session, token);
                Command command = commandManager.getCommand(token.value());
                if (command != null) {
                    return createCommandProcess(command, tokens, jobId, term, resultDistributor);
                } else {
                    throw new IllegalArgumentException(token.value() + ": command not found");
                }
            }
        }
        throw new IllegalArgumentException();
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}
```

这个方法的逻辑是：遍历 Token 列表，找到第一个文本 Token，然后通过 `commandManager.getCommand(name)` 查找对应的 Command。如果找到了，调用 `createCommandProcess` 创建进程；如果找不到，抛出 "command not found" 异常。

**`commandManager.getCommand(name)` 的实现：**

```java
public Command getCommand(String commandName) {
    for (CommandResolver resolver : resolvers) {
        if (resolver instanceof ShellInternalCommandResolver) {
            continue;
        }
        Command command = getCommand(resolver, commandName);
        if (command != null) {
            return command;
        }
    }
    return null;
}

private static Command getCommand(CommandResolver commandResolver, String name) {
    List<Command> commands = commandResolver.commands();
    if (commands == null || commands.isEmpty()) {
        return null;
    }
    for (Command command : commands) {
        if (name.equals(command.name())) {
            return command;
        }
    }
    return null;
}
```

命令查找的过程是遍历所有注册的 `CommandResolver`，对每个 Resolver 中的命令列表做线性查找。
注意这里跳过了 `ShellInternalCommandResolver`（内建命令），因为内建命令在 ShellLineHandler 中已经处理过了。

---

### 5.5 createCommandProcess() —— 构建管道链和进程

这是 JobControllerImpl 中最复杂的方法，它负责解析管道和重定向，构建完整的输出处理链：

```java
private Process createCommandProcess(Command command, ListIterator<CliToken> tokens,
                                     int jobId, Term term,
                                     ResultDistributor resultDistributor) throws IOException {
    List<CliToken> remaining = new ArrayList<CliToken>();
    List<CliToken> pipelineTokens = new ArrayList<CliToken>();
    boolean isPipeline = false;
    RedirectHandler redirectHandler = null;
    List<Function<String, String>> stdoutHandlerChain = new ArrayList<Function<String, String>>();
    String cacheLocation = null;
    while (tokens.hasNext()) {
        CliToken remainingToken = tokens.next();
        if (remainingToken.isText()) {
            String tokenValue = remainingToken.value();
            if ("|".equals(tokenValue)) {
                isPipeline = true;
                injectHandler(stdoutHandlerChain, pipelineTokens);
                continue;
            } else if (">>".equals(tokenValue) || ">".equals(tokenValue)) {
                String name = getRedirectFileName(tokens);
                if (name == null) {
                    name = LogUtil.cacheDir() + File.separator + Constants.PID
                           + File.separator + jobId;
                    cacheLocation = name;
                    if (getRedirectJobCount() == 8) {
                        throw new IllegalStateException(
                            "The amount of async command that saving result to file can't > 8");
                    }
                }
                redirectHandler = new RedirectHandler(name, ">>".equals(tokenValue));
                break;
            }
        }
        if (isPipeline) {
            pipelineTokens.add(remainingToken);
        } else {
            remaining.add(remainingToken);
        }
    }
    injectHandler(stdoutHandlerChain, pipelineTokens);
    if (redirectHandler != null) {
        stdoutHandlerChain.add(redirectHandler);
        term.write("redirect output file will be: " + redirectHandler.getFilePath() + "\n");
    } else {
        stdoutHandlerChain.add(new TermHandler(term));
        if (GlobalOptions.isSaveResult) {
            stdoutHandlerChain.add(new RedirectHandler());
        }
    }
    ProcessOutput processOutput = new ProcessOutput(stdoutHandlerChain, cacheLocation, term);
    ProcessImpl process = new ProcessImpl(command, remaining, command.processHandler(),
                                          processOutput, resultDistributor);
    process.setTty(term);
    return process;
}
```

这个方法做了以下几件事：

**1. 解析管道和重定向**

遍历命令名之后的 Token，根据 `|`、`>`、`>>` 将 Token 分为不同的组：
- `remaining`：命令本身的参数
- `pipelineTokens`：管道之后的参数（如 `grep hello` 中的 `grep` 和 `hello`）

**2. 构建输出处理链 stdoutHandlerChain**

```java
List<Function<String, String>> stdoutHandlerChain = new ArrayList<Function<String, String>>();
```

输出处理链是一个 `Function<String, String>` 列表，每个函数接收一个字符串，返回处理后的字符串。
数据会依次流过链中的每个处理器。

**3. 管道处理器注入**

```java
private void injectHandler(List<Function<String, String>> stdoutHandlerChain,
                           List<CliToken> pipelineTokens) {
    if (!pipelineTokens.isEmpty()) {
        StdoutHandler handler = StdoutHandler.inject(pipelineTokens);
        if (handler != null) {
            stdoutHandlerChain.add(handler);
        }
        pipelineTokens.clear();
    }
}
```

`StdoutHandler.inject(pipelineTokens)` 根据管道命令名创建对应的处理器：

```java
public static StdoutHandler inject(List<CliToken> tokens) {
    CliToken firstTextToken = null;
    for (CliToken token : tokens) {
        if (token.isText()) {
            firstTextToken = token;
            break;
        }
    }
    if (firstTextToken == null) {
        return null;
    }
    if (firstTextToken.value().equals(GrepHandler.NAME)) {
        return GrepHandler.inject(tokens);
    } else if (firstTextToken.value().equals(PlainTextHandler.NAME)) {
        return PlainTextHandler.inject(tokens);
    } else if (firstTextToken.value().equals(WordCountHandler.NAME)) {
        return WordCountHandler.inject(tokens);
    } else if (firstTextToken.value().equals(TeeHandler.NAME)){
        return TeeHandler.inject(tokens);
    } else{
        return null;
    }
}
```

支持的管道命令：

| 管道命令 | Handler | 功能 |
|----------|---------|------|
| `grep` | GrepHandler | 过滤输出中匹配的行 |
| `plaintext` | PlainTextHandler | 去除 ANSI 颜色码 |
| `wc` | WordCountHandler | 统计行数/字数 |
| `tee` | TeeHandler | 将输出同时写入文件和终端 |

**4. 末尾处理器**

如果有重定向，最后添加 `RedirectHandler`；否则添加 `TermHandler`（写入终端）。

```java
if (redirectHandler != null) {
    stdoutHandlerChain.add(redirectHandler);
} else {
    stdoutHandlerChain.add(new TermHandler(term));
    if (GlobalOptions.isSaveResult) {
        stdoutHandlerChain.add(new RedirectHandler());
    }
}
```

`TermHandler` 是最终将字符串写入终端的处理器：

```java
public class TermHandler implements Function<String, String> {
    private Tty term;
    public TermHandler(Tty term) {
        this.term = term;
    }
    @Override
    public String apply(String data) {
        term.write(data);
        return data;
    }
}
```

**完整的输出处理链示例**：

假设用户输入 `watch Demo test | grep hello > /tmp/output.log`，输出处理链为：

```
GrepHandler("hello") → RedirectHandler("/tmp/output.log")
```

数据流向：
```
命令输出字符串 → GrepHandler 过滤 → RedirectHandler 写文件
```

如果用户输入 `watch Demo test | grep hello`（无重定向），输出处理链为：
```
GrepHandler("hello") → TermHandler(终端输出)
```

**5. 创建 ProcessImpl**

```java
ProcessOutput processOutput = new ProcessOutput(stdoutHandlerChain, cacheLocation, term);
ProcessImpl process = new ProcessImpl(command, remaining, command.processHandler(),
                                      processOutput, resultDistributor);
process.setTty(term);
return process;
```

将构建好的输出处理链封装为 `ProcessOutput`，然后创建 `ProcessImpl` 实例。

---

### 5.6 重定向任务数量限制

```java
if (getRedirectJobCount() == 8) {
    throw new IllegalStateException(
        "The amount of async command that saving result to file can't > 8");
}

private int getRedirectJobCount() {
    int count = 0;
    for (Job job : jobs.values()) {
        if (job.process() != null && job.process().cacheLocation() != null) {
            count++;
        }
    }
    return count;
}
```

**它为什么存在？** 重定向到文件的 Job 会持续写入磁盘，如果不限制数量，
可能导致磁盘 I/O 过高影响目标应用。Arthas 限制最多同时有 8 个带重定向的异步任务，
这是一个合理的**资源保护**措施。

---

## 第六阶段：JobImpl —— Job 的生命周期

### 6.1 JobImpl 类概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/system/impl/JobImpl.java`

```java
public class JobImpl implements Job {

    final int id;
    final JobControllerImpl controller;
    final Process process;
    final String line;
    private volatile Session session;
    private volatile ExecStatus actualStatus;
    volatile long lastStopped;
    volatile JobListener jobHandler;
    volatile Handler<ExecStatus> statusUpdateHandler;
    volatile Date timeoutDate;
    final Future<Void> terminateFuture;
    final AtomicBoolean runInBackground;
}
```

核心字段解读：

| 字段 | 类型 | 含义 |
|------|------|------|
| `id` | int | Job 唯一标识 |
| `controller` | JobControllerImpl | 所属的 Job 控制器 |
| `process` | Process | 底层进程（ProcessImpl） |
| `line` | String | 原始命令行字符串 |
| `session` | Session | 所属的会话 |
| `actualStatus` | ExecStatus | 实际状态 |
| `jobHandler` | JobListener | Job 生命周期回调 |
| `timeoutDate` | Date | 超时时间 |
| `terminateFuture` | Future | 终止完成的 Future |
| `runInBackground` | AtomicBoolean | 是否在后台运行 |

---

### 6.2 Job 状态机

Job 的状态由底层 Process 管理，通过 `ExecStatus` 枚举表示：

```java
public enum ExecStatus {
    READY,
    RUNNING,
    STOPPED,
    TERMINATED
}
```

状态转换图：

```
┌─────────┐    run()     ┌─────────┐   suspend()   ┌─────────┐
│  READY  │────────────→│ RUNNING │──────────────→│ STOPPED │
└─────────┘             └─────────┘               └─────────┘
                             │                         │
                             │  terminate()            │ resume()
                             │                         │
                             ▼                         ▼
                        ┌────────────┐            ┌─────────┐
                        │ TERMINATED │←───────────│ RUNNING │
                        └────────────┘ terminate()└─────────┘
```

| 起始状态 | 触发动作 | 目标状态 | 说明 |
|----------|----------|----------|------|
| READY | run() | RUNNING | 开始执行 |
| RUNNING | suspend() | STOPPED | Ctrl+Z 挂起 |
| RUNNING | terminate() | TERMINATED | 命令完成或被 kill |
| STOPPED | resume(true) | RUNNING(前台) | fg 恢复到前台 |
| STOPPED | resume(false) | RUNNING(后台) | bg 恢复到后台 |
| STOPPED | terminate() | TERMINATED | kill 挂起的 Job |

---

### 6.3 run() 方法 —— 启动 Job

```java
@Override
public Job run() {
    return run(!runInBackground.get());
}

@Override
public Job run(boolean foreground) {
    actualStatus = ExecStatus.RUNNING;
    if (statusUpdateHandler != null) {
        statusUpdateHandler.handle(ExecStatus.RUNNING);
    }
    process.setSession(this.session);
    process.run(foreground);

    if (this.status() == ExecStatus.RUNNING) {
        if (foreground) {
            jobHandler.onForeground(this);
        } else {
            jobHandler.onBackground(this);
        }
    }
    return this;
}
```

`run()` 方法的逻辑：
1. 设置状态为 RUNNING
2. 将 Session 设置到 Process 上
3. 调用 `process.run(foreground)` 启动底层进程
4. 根据前台/后台模式，通知 jobHandler

**关键设计**：`process.run(foreground)` 调用后，如果是前台命令，控制权不会立即返回
（因为前台命令会占据终端直到完成）。只有当命令执行完成或被挂起/中断时，才会通过回调链返回。

---

### 6.4 构造函数中的 terminatedHandler

```java
JobImpl(int id, final JobControllerImpl controller, Process process, String line,
        boolean runInBackground, Session session, JobListener jobHandler) {
    // ... 字段初始化 ...
    process.terminatedHandler(new TerminatedHandler(controller));
}
```

在 Job 创建时就注册了 `TerminatedHandler`，它会在进程终止时被回调：

```java
private class TerminatedHandler implements Handler<Integer> {
    private final JobControllerImpl controller;

    public TerminatedHandler(JobControllerImpl controller) {
        this.controller = controller;
    }

    @Override
    public void handle(Integer exitCode) {
        jobHandler.onTerminated(JobImpl.this);
        controller.removeJob(JobImpl.this.id);
        if (statusUpdateHandler != null) {
            statusUpdateHandler.handle(ExecStatus.TERMINATED);
        }
        terminateFuture.complete();
    }
}
```

当进程终止时，TerminatedHandler 会：
1. 通知 Shell 层的 jobHandler（`ShellJobHandler.onTerminated`）
2. 从 JobController 中移除该 Job
3. 标记 `terminateFuture` 完成

---

### 6.5 terminate() 方法 —— 终止 Job

```java
@Override
public void terminate() {
    try {
        process.terminate();
    } catch (IllegalStateException ignore) {
        // Process already terminated, likely by itself
    } finally {
        controller.removeJob(this.id);
    }
}
```

`terminate()` 先调用 `process.terminate()` 终止底层进程，然后从 controller 中移除。
注意 `catch (IllegalStateException ignore)` —— 如果进程已经自行终止了（比如命令执行完毕），
再次调用 terminate 会抛出 IllegalStateException，这里选择忽略它。
`finally` 块确保无论如何，Job 都会从 controller 中被移除。

---

### 6.6 suspend() 方法 —— 挂起 Job

```java
@Override
public Job suspend() {
    try {
        process.suspend(new SuspendHandler());
    } catch (IllegalStateException ignore) {
        return this;
    }
    if (statusUpdateHandler != null) {
        statusUpdateHandler.handle(process.status());
    }
    jobHandler.onSuspend(this);
    return this;
}

private class SuspendHandler implements Handler<Void> {
    @Override
    public void handle(Void event) {
        actualStatus = ExecStatus.STOPPED;
    }
}
```

挂起 Job 时：
1. 调用 `process.suspend()` 挂起底层进程
2. 通知 jobHandler（`ShellJobHandler.onSuspend`）
3. ShellJobHandler 会清除前台 Job 并重新开始 readline

---

### 6.7 resume() 方法 —— 恢复 Job

```java
@Override
public Job resume(boolean foreground) {
    try {
        process.resume(foreground, new ResumeHandler());
    } catch (IllegalStateException ignore) {
    }
    runInBackground.set(!foreground);

    if (statusUpdateHandler != null) {
        statusUpdateHandler.handle(process.status());
    }

    if (this.status() == ExecStatus.RUNNING) {
        if (foreground) {
            jobHandler.onForeground(this);
        } else {
            jobHandler.onBackground(this);
        }
    }
    return this;
}
```

恢复 Job 时根据 `foreground` 参数决定是恢复到前台还是后台，然后通知 jobHandler。

---

### 6.8 toBackground() 和 toForeground() —— 前后台切换

```java
@Override
public Job toBackground() {
    if (!this.runInBackground.get()) {
        if (runInBackground.compareAndSet(false, true)) {
            process.toBackground();
            if (statusUpdateHandler != null) {
                statusUpdateHandler.handle(process.status());
            }
            jobHandler.onBackground(this);
        }
    }
    return this;
}

@Override
public Job toForeground() {
    if (this.runInBackground.get()) {
        if (runInBackground.compareAndSet(true, false)) {
            process.toForeground();
            if (statusUpdateHandler != null) {
                statusUpdateHandler.handle(process.status());
            }
            jobHandler.onForeground(this);
        }
    }
    return this;
}
```

这两个方法用 `AtomicBoolean.compareAndSet` 实现了原子性的前后台切换，
防止并发切换导致状态不一致。

---

## 第七阶段：GlobalJobControllerImpl —— 全局 Job 控制器

### 7.1 GlobalJobControllerImpl 概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/system/impl/GlobalJobControllerImpl.java`

GlobalJobControllerImpl 继承自 JobControllerImpl，增加了 Job 超时管理功能。

```java
public class GlobalJobControllerImpl extends JobControllerImpl {
    private Map<Integer, JobTimeoutTask> jobTimeoutTaskMap =
        new ConcurrentHashMap<Integer, JobTimeoutTask>();
}
```

**它为什么存在？** 普通的 JobControllerImpl 会在 Shell 会话关闭时一起关闭所有 Job。
但 GlobalJobControllerImpl 是全局的，不会因为某个会话的断开而关闭。它的 `close(Handler)` 方法被重写为空操作：

```java
@Override
public void close(final Handler<Void> completionHandler) {
    if (completionHandler != null) {
        completionHandler.handle(null);
    }
}
```

这意味着当用户断开 telnet 连接时，后台运行的 Job 不会被终止。
只有当 Arthas 整体停止时（调用无参的 `close()` 方法），才会终止所有 Job。

---

### 7.2 Job 超时管理

```java
@Override
public Job createJob(InternalCommandManager commandManager, List<CliToken> tokens,
                     Session session, JobListener jobHandler, Term term,
                     ResultDistributor resultDistributor) {
    final Job job = super.createJob(commandManager, tokens, session, jobHandler, term, resultDistributor);

    JobTimeoutTask jobTimeoutTask = new JobTimeoutTask(job);
    long jobTimeoutInSecond = getJobTimeoutInSecond();
    Date timeoutDate = new Date(System.currentTimeMillis() + (jobTimeoutInSecond * 1000));
    ArthasBootstrap.getInstance().getScheduledExecutorService()
        .schedule(jobTimeoutTask, jobTimeoutInSecond, TimeUnit.SECONDS);
    jobTimeoutTaskMap.put(job.id(), jobTimeoutTask);
    job.setTimeoutDate(timeoutDate);

    return job;
}
```

每创建一个 Job，都会同时创建一个超时定时任务。到达超时时间后自动终止 Job。

超时时间的解析：

```java
private long getJobTimeoutInSecond() {
    long result = -1;
    String jobTimeoutConfig = GlobalOptions.jobTimeout.trim();
    try {
        char unit = jobTimeoutConfig.charAt(jobTimeoutConfig.length() - 1);
        String duration = jobTimeoutConfig.substring(0, jobTimeoutConfig.length() - 1);
        switch (unit) {
        case 'h':
            result = TimeUnit.HOURS.toSeconds(Long.parseLong(duration));
            break;
        case 'd':
            result = TimeUnit.DAYS.toSeconds(Long.parseLong(duration));
            break;
        case 'm':
            result = TimeUnit.MINUTES.toSeconds(Long.parseLong(duration));
            break;
        case 's':
            result = Long.parseLong(duration);
            break;
        default:
            result = Long.parseLong(jobTimeoutConfig);
            break;
        }
    } catch (Throwable e) {
        logger.error("parse jobTimeoutConfig: {} error!", jobTimeoutConfig, e);
    }
    if (result < 0) {
        result = TimeUnit.DAYS.toSeconds(1);
        logger.warn("Configuration with job timeout " + jobTimeoutConfig + " is error, use 1d in default.");
    }
    return result;
}
```

支持的超时格式：

| 格式 | 含义 | 示例 |
|------|------|------|
| `Xs` | X 秒 | `300s` = 5 分钟 |
| `Xm` | X 分钟 | `30m` = 30 分钟 |
| `Xh` | X 小时 | `2h` = 2 小时 |
| `Xd` | X 天 | `1d` = 1 天（默认值） |

---

### 7.3 JobTimeoutTask —— 超时清理任务

```java
private static class JobTimeoutTask implements Runnable {
    private Job job;

    public JobTimeoutTask(Job job) {
        this.job = job;
    }

    @Override
    public void run() {
        try {
            if (job != null) {
                Job temp = job;
                job = null;
                temp.terminate();
            }
        } catch (Throwable e) {
            // 日志记录
        }
    }

    public void cancel() {
        job = null;
    }
}
```

**关键设计**：`cancel()` 方法只是将 `job` 引用设为 `null`，而不是取消定时任务本身。
当定时任务触发时，`run()` 方法检查 `job` 是否为 `null`，如果是则什么都不做。
这是一种 **轻量级取消** 的设计模式，避免了与 `ScheduledExecutorService` 的取消机制交互的复杂性。

---

## 第八阶段：ProcessImpl —— 命令进程的核心

### 8.1 ProcessImpl 类概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/system/impl/ProcessImpl.java`

ProcessImpl 是整个命令执行链路中最核心的类。它封装了一个命令的完整生命周期：
从参数解析到命令执行，从结果输出到资源清理。

```java
public class ProcessImpl implements Process {

    private static final Logger logger = LoggerFactory.getLogger(ProcessImpl.class);

    private Command commandContext;
    private Handler<CommandProcess> handler;
    private List<CliToken> args;
    private Tty tty;
    private Session session;
    private Handler<Void> interruptHandler;
    private Handler<Void> suspendHandler;
    private Handler<Void> resumeHandler;
    private Handler<Void> endHandler;
    private Handler<Void> backgroundHandler;
    private Handler<Void> foregroundHandler;
    private Handler<Integer> terminatedHandler;
    private boolean foreground;
    private volatile ExecStatus processStatus;
    private boolean processForeground;
    private Handler<String> stdinHandler;
    private Handler<Void> resizeHandler;
    private Integer exitCode;
    private CommandProcessImpl process;
    private Date startTime;
    private ProcessOutput processOutput;
    private int jobId;
    private ResultDistributor resultDistributor;
}
```

这个类持有大量的 Handler 引用，形成了一个完整的**事件驱动模型**：

| Handler | 触发时机 | 典型用途 |
|---------|----------|----------|
| `interruptHandler` | Ctrl+C | 停止命令执行 |
| `suspendHandler` | Ctrl+Z | 挂起命令 |
| `resumeHandler` | fg/bg | 恢复命令 |
| `endHandler` | 命令完成 | 清理资源 |
| `backgroundHandler` | 切换到后台 | 解绑终端 |
| `foregroundHandler` | 切换到前台 | 绑定终端 |
| `terminatedHandler` | 进程终止 | 通知 Job |
| `stdinHandler` | 用户输入 | 交互式命令 |
| `resizeHandler` | 终端大小变化 | 调整输出格式 |

---

### 8.2 构造函数

```java
public ProcessImpl(Command commandContext, List<CliToken> args,
                   Handler<CommandProcess> handler,
                   ProcessOutput processOutput,
                   ResultDistributor resultDistributor) {
    this.commandContext = commandContext;
    this.handler = handler;
    this.args = args;
    this.resultDistributor = resultDistributor;
    this.processStatus = ExecStatus.READY;
    this.processOutput = processOutput;
}
```

注意初始状态是 `ExecStatus.READY`。Process 在创建后不会立即执行，
必须显式调用 `run()` 方法才会开始。

---

### 8.3 run(boolean fg) —— 进程启动的核心

```java
@Override
public synchronized void run(boolean fg) {
    if (processStatus != ExecStatus.READY) {
        throw new IllegalStateException("Cannot run proces in " + processStatus + " state");
    }

    processStatus = ExecStatus.RUNNING;
    processForeground = fg;
    foreground = fg;
    startTime = new Date();

    // Make a local copy
    final Tty tty = this.tty;
    if (tty == null) {
        throw new IllegalStateException("Cannot execute process without a TTY set");
    }

    process = new CommandProcessImpl(this, tty);
    if (resultDistributor == null) {
        resultDistributor = new TermResultDistributorImpl(process,
            ArthasBootstrap.getInstance().getResultViewResolver());
    }

    final List<String> args2 = new LinkedList<String>();
    for (CliToken arg : args) {
        if (arg.isText()) {
            args2.add(arg.value());
        }
    }

    CommandLine cl = null;
    try {
        if (commandContext.cli() != null) {
            if (commandContext.cli().parse(args2, false).isAskingForHelp()) {
                appendResult(new HelpCommand().createHelpDetailModel(commandContext));
                terminate();
                return;
            }
            cl = commandContext.cli().parse(args2);
            process.setArgs2(args2);
            process.setCommandLine(cl);
        }
    } catch (CLIException e) {
        terminate(-10, null, e.getMessage());
        return;
    }

    if (cacheLocation() != null) {
        process.echoTips("job id  : " + this.jobId + "\n");
        process.echoTips("cache location  : " + cacheLocation() + "\n");
    }
    Runnable task = new CommandProcessTask(process);
    ArthasBootstrap.getInstance().execute(task);
}
```

这个方法是整个执行链路的关键节点，让我们逐段分析：

**第一段：状态检查与初始化**

```java
if (processStatus != ExecStatus.READY) {
    throw new IllegalStateException("Cannot run proces in " + processStatus + " state");
}
processStatus = ExecStatus.RUNNING;
processForeground = fg;
foreground = fg;
startTime = new Date();
```

确保进程只能从 READY 状态启动。`synchronized` 关键字保证了状态转换的原子性。

**第二段：创建 CommandProcessImpl**

```java
process = new CommandProcessImpl(this, tty);
if (resultDistributor == null) {
    resultDistributor = new TermResultDistributorImpl(process,
        ArthasBootstrap.getInstance().getResultViewResolver());
}
```

创建命令进程实现对象。如果没有传入 ResultDistributor（通常是 null），
则创建默认的 `TermResultDistributorImpl`。

**第三段：参数提取**

```java
final List<String> args2 = new LinkedList<String>();
for (CliToken arg : args) {
    if (arg.isText()) {
        args2.add(arg.value());
    }
}
```

从 Token 列表中提取所有文本 Token 的值，忽略空白 Token。
例如 Token 列表 `[text("watch"), blank(" "), text("Demo"), blank(" "), text("test")]`
会被提取为 `["watch", "Demo", "test"]`。

**第四段：帮助信息处理**

```java
if (commandContext.cli() != null) {
    if (commandContext.cli().parse(args2, false).isAskingForHelp()) {
        appendResult(new HelpCommand().createHelpDetailModel(commandContext));
        terminate();
        return;
    }
    cl = commandContext.cli().parse(args2);
    process.setArgs2(args2);
    process.setCommandLine(cl);
}
```

这里先用 `parse(args2, false)` 做一次"宽松解析"（不抛出异常），
检查用户是否输入了 `-h` 或 `--help`。如果是，直接输出帮助信息并终止。
然后用 `parse(args2)` 做一次"严格解析"，如果参数有误会抛出 `CLIException`。

**第五段：异步执行**

```java
Runnable task = new CommandProcessTask(process);
ArthasBootstrap.getInstance().execute(task);
```

将命令包装为 `CommandProcessTask`，提交到 Arthas 的全局线程池执行。

**为什么命令执行要放在线程池里而不是当前线程？**

1. **避免阻塞 I/O 线程**：当前线程是 Netty 的 I/O 线程（EventLoop），
   如果在 I/O 线程上执行耗时命令（如 `jad` 反编译大型类），会阻塞整个 Netty 的事件循环，
   导致其他 telnet 会话无法响应。

2. **支持并发执行**：多个会话可以同时执行不同的命令，互不干扰。

3. **超时控制**：在独立线程上执行，可以通过 interrupt 机制实现命令超时中断。

---

### 8.4 CommandProcessTask —— 命令执行的线程任务

```java
private class CommandProcessTask implements Runnable {

    private CommandProcess process;

    public CommandProcessTask(CommandProcess process) {
        this.process = process;
    }

    @Override
    public void run() {
        try {
            handler.handle(process);
        } catch (Throwable t) {
            logger.error("Error during processing the command:", t);
            process.end(1, "Error during processing the command: "
                + t.getClass().getName() + ", message:" + t.getMessage()
                + ", please check $HOME/logs/arthas/arthas.log for more details.");
        }
    }
}
```

**这一步做了什么？** `handler.handle(process)` 是命令实际执行的入口。
`handler` 就是 `command.processHandler()` 返回的处理器，对于注解命令来说，
它是 `AnnotatedCommandImpl.ProcessHandler`。

异常处理非常重要 —— 用 `catch (Throwable t)` 捕获所有异常（包括 Error），
确保即使命令执行中出现任何异常，都会：
1. 记录日志
2. 调用 `process.end(1, msg)` 正常结束进程
3. 输出友好的错误信息给用户

这避免了命令异常导致"僵尸进程" —— 即进程既不是 RUNNING 也不是 TERMINATED 的状态。

---

### 8.5 CommandProcessImpl —— 命令进程的内部实现

CommandProcessImpl 是 ProcessImpl 的内部类，实现了 `CommandProcess` 接口。
它是命令代码与框架交互的"桥梁"。

```java
private class CommandProcessImpl implements CommandProcess {

    private final Process process;
    private final Tty tty;
    private List<String> args2;
    private CommandLine commandLine;
    private AtomicInteger times = new AtomicInteger();
    private AdviceListener listener = null;
    private ClassFileTransformer transformer;

    public CommandProcessImpl(Process process, Tty tty) {
        this.process = process;
        this.tty = tty;
    }
}
```

---

### 8.6 register() —— 注册增强监听器

```java
@Override
public void register(AdviceListener adviceListener, ClassFileTransformer transformer) {
    if (adviceListener instanceof ProcessAware) {
        ProcessAware processAware = (ProcessAware) adviceListener;
        if(processAware.getProcess() == null) {
            processAware.setProcess(this.process);
        }
    }
    this.listener = adviceListener;
    AdviceWeaver.reg(listener);
    
    this.transformer = transformer;
}
```

**这一步做了什么？** 当增强类命令（如 `watch`、`trace`、`stack`）执行时，会调用 `register()` 方法：

1. 如果 listener 实现了 `ProcessAware` 接口，将 Process 引用注入给它。
   这使得 listener 在回调中可以操作 Process（如输出结果、结束命令）。

2. 调用 `AdviceWeaver.reg(listener)` 将监听器注册到全局的 AdviceWeaver 中。
   这样当被增强的方法被调用时，AdviceWeaver 会触发这个 listener。

3. 保存 `transformer` 引用，以便后续 `unregister()` 时移除。

**CommandProcessImpl 为什么要持有 AdviceListener 和 ClassFileTransformer？**

因为命令结束时需要清理增强。`AdviceListener` 是增强的"监听端"，
`ClassFileTransformer` 是增强的"织入端"。两者都需要在命令结束时被清理，
否则：
- listener 会继续接收回调但没有输出目标，导致内存泄漏
- transformer 会继续对后续加载的类进行增强，影响目标应用性能

---

### 8.7 unregister() —— 卸载增强

```java
@Override
public void unregister() {
    if (transformer != null) {
        ArthasBootstrap.getInstance().getTransformerManager().removeTransformer(transformer);
    }
    
    if (listener instanceof ProcessAware) {
        if (this.process.equals(((ProcessAware) listener).getProcess())) {
            AdviceWeaver.unReg(listener);
        }
    } else {
        AdviceWeaver.unReg(listener);
    }
}
```

`unregister()` 在 `terminate()` 时被调用，它做了两件事：

1. **移除 ClassFileTransformer**：调用 `TransformerManager.removeTransformer(transformer)`，
   这会使得后续加载的类不再被增强。

2. **注销 AdviceListener**：调用 `AdviceWeaver.unReg(listener)`。
   但这里有一个特殊判断：如果 listener 是 `ProcessAware`，
   需要检查它是否属于当前 Process。这是因为某些场景下，
   一个 listener 可能被多个命令共享（代码注释："listener有可能其它 command 创建的"），
   不能误删别人的 listener。

**unregister() 为什么要在 terminate 时调用？**

因为增强是一种"侵入性"操作，如果不及时卸载，会有以下问题：
1. 增强代码会持续执行，消耗目标应用的 CPU
2. 增强的 listener 持有各种引用，可能导致内存泄漏
3. 如果用户意外断开连接，增强不会自动清理，
   只能通过 `reset` 命令手动清理

Arthas 通过在 `terminate()` 中调用 `unregister()`，
确保了命令结束时自动清理增强，是一种 **RAII（资源获取即初始化）** 的设计思想。

---

### 8.8 write() —— 输出到终端

```java
@Override
public CommandProcess write(String data) {
    if (processStatus != ExecStatus.RUNNING) {
        throw new IllegalStateException(
                "Cannot write to standard output when " + status().name().toLowerCase());
    }
    processOutput.write(data);
    return this;
}
```

`write()` 方法将数据写入 `processOutput`（即前面构建的输出处理链）。
注意状态检查 —— 只有 RUNNING 状态的进程才能输出数据。
如果命令已经终止还试图输出，会抛出异常。

---

### 8.9 appendResult() —— 输出结构化结果

```java
@Override
public void appendResult(ResultModel result) {
    if (processStatus != ExecStatus.RUNNING) {
        throw new IllegalStateException(
                "Cannot write to standard output when " + status().name().toLowerCase());
    }
    ProcessImpl.this.appendResult(result);
}
```

而 ProcessImpl 的 `appendResult()`：

```java
private void appendResult(ResultModel result) {
    result.setJobId(jobId);
    if (resultDistributor != null) {
        resultDistributor.appendResult(result);
    }
}
```

这里先给 result 设置 jobId，然后交给 `resultDistributor` 分发。
`write()` 是低级的字符串输出，`appendResult()` 是高级的结构化结果输出。

---

### 8.10 end() 方法族 —— 结束进程

```java
@Override
public void end() {
    end(0);
}

@Override
public void end(int statusCode) {
    end(statusCode, null);
}

@Override
public void end(int statusCode, String message) {
    terminate(statusCode, null, message);
}
```

三个重载版本形成了一个调用链，最终都调用 `ProcessImpl.terminate()`：

```java
private synchronized boolean terminate(int exitCode, Handler<Void> completionHandler, String message) {
    if (processStatus != ExecStatus.TERMINATED) {
        this.appendResult(new StatusModel(exitCode, message));
        if (process != null) {
            processOutput.close();
        }
        updateStatus(ExecStatus.TERMINATED, exitCode, false, endHandler, terminatedHandler, completionHandler);
        if (process != null) {
            process.unregister();
        }
        return true;
    } else {
        return false;
    }
}
```

terminate 方法的执行步骤：

1. **输出状态信息**：`appendResult(new StatusModel(exitCode, message))`
2. **关闭输出链**：`processOutput.close()`
3. **更新状态**：`updateStatus(ExecStatus.TERMINATED, ...)`
4. **卸载增强**：`process.unregister()`

**关键设计**：`synchronized` 和状态检查 `processStatus != ExecStatus.TERMINATED`
确保了 terminate 只会执行一次。即使多个线程同时调用（比如命令自行完成的同时用户按了 Ctrl+C），
也不会重复清理资源。

---

### 8.11 ProcessOutput —— 输出处理链

```java
static class ProcessOutput {

    private List<Function<String, String>> stdoutHandlerChain;
    private StatisticsFunction statisticsHandler = null;
    private List<Function<String, String>> flushHandlerChain = null;
    private String cacheLocation;
    private Tty term;

    public ProcessOutput(List<Function<String, String>> stdoutHandlerChain,
                         String cacheLocation, Tty term) {
        int i = 0;
        for (; i < stdoutHandlerChain.size(); i++) {
            if (stdoutHandlerChain.get(i) instanceof StatisticsFunction) {
                break;
            }
        }
        if (i < stdoutHandlerChain.size()) {
            this.stdoutHandlerChain = stdoutHandlerChain.subList(0, i + 1);
            this.statisticsHandler = (StatisticsFunction) stdoutHandlerChain.get(i);
            if (i < stdoutHandlerChain.size() - 1) {
                flushHandlerChain = stdoutHandlerChain.subList(i + 1, stdoutHandlerChain.size());
            }
        } else {
            this.stdoutHandlerChain = stdoutHandlerChain;
        }
        this.cacheLocation = cacheLocation;
        this.term = term;
    }
```

ProcessOutput 的构造函数做了一件有趣的事：
它在 stdoutHandlerChain 中查找 `StatisticsFunction`（如 `wc` 命令的处理器），
将链拆分为两部分：
- 前半部分（包含 StatisticsFunction 在内）：在每次 `write()` 时执行
- 后半部分（flushHandlerChain）：在 `close()` 时执行

**为什么要这样拆分？** 因为统计类函数（如 `wc`）需要先收集所有数据，
然后在命令结束时才能输出统计结果。就像 Unix 的 `wc` 命令一样，
它会在 EOF 时才输出总行数。

**write() 方法**：

```java
private void write(String data) {
    if (stdoutHandlerChain != null) {
        int size = stdoutHandlerChain.size();
        for (int i = 0; i < size; i++) {
            Function<String, String> function = stdoutHandlerChain.get(i);
            data = function.apply(data);
        }
    }
}
```

数据依次流过链中的每个处理器。注意使用了普通 `for` 循环而非 `foreach`/迭代器，
代码注释明确说明这是为了 "reduce memory fragment (foreach/iterator)"。
在热路径上避免创建 Iterator 对象，减少 GC 压力 —— 这是 Arthas 对性能的极致追求。

**close() 方法**：

```java
private void close() {
    if (statisticsHandler != null && flushHandlerChain != null) {
        String data = statisticsHandler.result();
        for (Function<String, String> function : flushHandlerChain) {
            data = function.apply(data);
            if (function instanceof StatisticsFunction) {
                data = ((StatisticsFunction) function).result();
            }
        }
    }
    if (stdoutHandlerChain != null) {
        for (Function<String, String> function : stdoutHandlerChain) {
            if (function instanceof CloseFunction) {
                ((CloseFunction) function).close();
            }
        }
    }
}
```

关闭时：
1. 将 StatisticsFunction 的统计结果通过 flushHandlerChain 输出
2. 关闭所有实现了 `CloseFunction` 接口的处理器（如释放文件句柄）

---

### 8.12 updateStatus() —— 状态更新与前后台切换

```java
private void updateStatus(ExecStatus statusUpdate, Integer exitCodeUpdate,
                          boolean foregroundUpdate,
                          Handler<Void> handler, Handler<Integer> terminatedHandler,
                          Handler<Void> completionHandler) {
    processStatus = statusUpdate;
    exitCode = exitCodeUpdate;
    if (!foregroundUpdate) {
        if (processForeground) {
            processForeground = false;
            if (stdinHandler != null) {
                tty.stdinHandler(null);
            }
            if (resizeHandler != null) {
                tty.resizehandler(null);
            }
        }
    } else {
        if (!processForeground) {
            processForeground = true;
            if (stdinHandler != null) {
                tty.stdinHandler(stdinHandler);
            }
            if (resizeHandler != null) {
                tty.resizehandler(resizeHandler);
            }
        }
    }

    foreground = foregroundUpdate;
    try {
        if (handler != null) {
            handler.handle(null);
        }
    } finally {
        if (completionHandler != null) {
            completionHandler.handle(null);
        }
        if (terminatedHandler != null && statusUpdate == ExecStatus.TERMINATED) {
            terminatedHandler.handle(exitCodeUpdate);
        }
    }
}
```

这个方法处理了前后台切换时的 I/O 绑定：
- 切换到**前台**：将 `stdinHandler` 和 `resizeHandler` 绑定到 TTY
- 切换到**后台**：将这些 handler 从 TTY 解绑（设为 null）

**为什么切换到后台要解绑 handler？** 因为后台进程不应该接收用户输入。
如果用户在一个命令运行时按了 Ctrl+Z 挂起它，然后输入新命令，
新输入不应该被挂起的命令捕获。

---

### 8.13 interrupt() —— 中断进程

```java
@Override
public boolean interrupt(final Handler<Void> completionHandler) {
    if (processStatus == ExecStatus.RUNNING || processStatus == ExecStatus.STOPPED
        || processStatus == ExecStatus.TERMINATED) {
        final Handler<Void> handler = interruptHandler;
        try {
            if (handler != null) {
                handler.handle(null);
            }
        } finally {
            if (completionHandler != null) {
                completionHandler.handle(null);
            }
        }
        return handler != null;
    } else {
        throw new IllegalStateException("Cannot interrupt process in " + processStatus + " state");
    }
}
```

注意 `interrupt()` 只是调用 `interruptHandler`，而不是直接终止进程。
具体的中断行为由命令自己通过 `process.interruptHandler(handler)` 注册。

对于增强类命令（如 `watch`），中断处理器通常会调用 `process.end()` 来结束命令。
对于 `dashboard` 这样的持续输出命令，中断处理器会停止定时器并调用 `process.end()`。

---

## 第九阶段：AnnotatedCommandImpl —— 注解命令执行

### 9.1 AnnotatedCommandImpl 类概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/command/impl/AnnotatedCommandImpl.java`

AnnotatedCommandImpl 是 Arthas 命令体系的核心骨架。所有通过注解定义的命令
（即 Arthas 的全部 40+ 个命令）都通过这个类包装和执行。

```java
public class AnnotatedCommandImpl extends Command {

    private CLI cli;
    private Class<? extends AnnotatedCommand> clazz;
    private Handler<CommandProcess> processHandler = new ProcessHandler();

    public AnnotatedCommandImpl(Class<? extends AnnotatedCommand> clazz) {
        this.clazz = clazz;
        cli = CLIConfigurator.define(clazz, true);
        cli.addOption(new Option().setArgName("help").setFlag(true)
            .setShortName("h").setLongName("help")
            .setDescription("this help").setHelp(true));
    }
}
```

构造函数做了两件关键的事：

1. **`CLIConfigurator.define(clazz, true)`**：通过反射扫描命令类上的注解，
   生成 CLI（命令行接口）定义。这个 CLI 包含了命令的所有参数定义：
   - `@Name` 注解定义命令名
   - `@Summary` 注解定义命令简述
   - `@Description` 注解定义命令详细描述
   - `@Option` 注解定义可选参数
   - `@Argument` 注解定义位置参数

2. **手动添加 `-h` / `--help` 选项**：所有命令都自动支持 `-h` 显示帮助信息。

---

### 9.2 name() 和 cli() —— 命令元信息

```java
@Override
public String name() {
    if (shouldOverridesName(clazz)) {
        try {
            return clazz.newInstance().name();
        } catch (Exception ignore) {
        }
    }
    return cli.getName();
}

@Override
public CLI cli() {
    if (shouldOverrideCli(clazz)) {
        try {
            return clazz.newInstance().cli();
        } catch (Exception ignore) {
        }
    }
    return cli;
}

private boolean shouldOverridesName(Class<? extends AnnotatedCommand> clazz) {
    try {
        clazz.getDeclaredMethod("name");
        return true;
    } catch (NoSuchMethodException ignore) {
        return false;
    }
}
```

这里有一个精巧的设计：通过 `getDeclaredMethod("name")` 检查命令类是否重写了 `name()` 方法。
如果重写了，调用命令实例的方法获取名字；否则使用 CLI 注解中定义的名字。

**为什么不直接创建实例调用 name()？** 因为反射创建实例有开销，
如果命令类没有重写 `name()` 方法，就不需要创建实例，直接用注解信息即可。
这是一个性能优化。

---

### 9.3 process() —— 命令执行的入口

```java
private void process(CommandProcess process) {
    AnnotatedCommand instance;
    try {
        instance = clazz.newInstance();
    } catch (Exception e) {
        process.end();
        return;
    }
    CLIConfigurator.inject(process.commandLine(), instance);
    instance.process(process);
    String userId = process.session() != null ? process.session().getUserId() : null;
    UserStatUtil.arthasUsageSuccess(name(), process.args(), userId);
}
```

这是命令执行的核心流程，三步走：

**第一步：创建命令实例**

```java
instance = clazz.newInstance();
```

每次命令执行都会创建一个**全新的命令实例**。这意味着命令类是无状态的 —— 
不同的执行之间不会共享任何状态。这是一个重要的设计选择，
保证了命令的线程安全性。

**第二步：注入参数**

```java
CLIConfigurator.inject(process.commandLine(), instance);
```

`CLIConfigurator.inject()` 通过反射将解析后的命令行参数注入到命令实例的字段中。
例如，如果用户输入了 `watch Demo test -n 5`，那么命令实例的 `numberOfLimit` 字段
会被自动设置为 5。

**第三步：执行命令**

```java
instance.process(process);
```

调用命令实例的 `process()` 方法，传入 `CommandProcess` 对象。
命令的具体逻辑就在各个命令类的 `process()` 方法中实现。

**第四步：统计上报**

```java
UserStatUtil.arthasUsageSuccess(name(), process.args(), userId);
```

统计命令的使用情况。这是一个可选的用户行为统计，帮助 Arthas 团队了解各命令的使用频率。

---

### 9.4 ProcessHandler —— 命令执行的 Handler 包装

```java
private class ProcessHandler implements Handler<CommandProcess> {
    @Override
    public void handle(CommandProcess process) {
        process(process);
    }
}
```

`ProcessHandler` 是 `process()` 方法的简单包装，实现了 `Handler<CommandProcess>` 接口。
它是 ProcessImpl 中 `handler.handle(process)` 调用的最终目标。

调用链回顾：
```
ProcessImpl.run()
  → new CommandProcessTask(process)
  → ArthasBootstrap.execute(task)
  → CommandProcessTask.run()
  → handler.handle(process)        // handler = AnnotatedCommandImpl.ProcessHandler
  → AnnotatedCommandImpl.process(process)
  → instance.process(process)      // 具体命令类的 process 方法
```

---

### 9.5 complete() —— Tab 补全

```java
@Override
public void complete(final Completion completion) {
    final AnnotatedCommand instance;
    try {
        instance = clazz.newInstance();
    } catch (Exception e) {
        super.complete(completion);
        return;
    }

    try {
        instance.complete(completion);
    } catch (Throwable t) {
        completion.complete(Collections.<String>emptyList());
    }
}
```

Tab 补全也是通过创建命令实例来完成的。命令类可以重写 `complete()` 方法
提供自定义的补全逻辑。例如，`SearchClassCommand` 的 `complete()` 方法
会搜索 JVM 中的类名进行补全。

---

## 第十阶段：BuiltinCommandPack —— 内置命令注册

### 10.1 BuiltinCommandPack 类概览

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/BuiltinCommandPack.java`

BuiltinCommandPack 是 Arthas 所有内置命令的"大本营"。它实现了 `CommandResolver` 接口，
管理着 40 多个命令的注册。

```java
public class BuiltinCommandPack implements CommandResolver {
    private static final Logger logger = LoggerFactory.getLogger(BuiltinCommandPack.class);
    private List<Command> commands = new ArrayList<Command>();

    public BuiltinCommandPack(List<String> disabledCommands) {
        initCommands(disabledCommands);
    }

    @Override
    public List<Command> commands() {
        return commands;
    }
}
```

---

### 10.2 initCommands() —— 命令初始化

```java
private void initCommands(List<String> disabledCommands) {
    List<Class<? extends AnnotatedCommand>> commandClassList =
        new ArrayList<Class<? extends AnnotatedCommand>>(33);
    commandClassList.add(HelpCommand.class);
    commandClassList.add(AuthCommand.class);
    commandClassList.add(KeymapCommand.class);
    commandClassList.add(SearchClassCommand.class);
    commandClassList.add(SearchMethodCommand.class);
    commandClassList.add(ClassLoaderCommand.class);
    commandClassList.add(JadCommand.class);
    commandClassList.add(GetStaticCommand.class);
    commandClassList.add(MonitorCommand.class);
    commandClassList.add(StackCommand.class);
    commandClassList.add(ThreadCommand.class);
    commandClassList.add(TraceCommand.class);
    commandClassList.add(WatchCommand.class);
    commandClassList.add(LineCommand.class);
    commandClassList.add(TimeTunnelCommand.class);
    commandClassList.add(JvmCommand.class);
    commandClassList.add(MemoryCommand.class);
    commandClassList.add(PerfCounterCommand.class);
    commandClassList.add(OgnlCommand.class);
    commandClassList.add(MemoryCompilerCommand.class);
    commandClassList.add(RedefineCommand.class);
    commandClassList.add(RetransformCommand.class);
    commandClassList.add(DashboardCommand.class);
    commandClassList.add(DumpClassCommand.class);
    commandClassList.add(HeapDumpCommand.class);
    commandClassList.add(JulyCommand.class);
    commandClassList.add(ThanksCommand.class);
    commandClassList.add(OptionsCommand.class);
    commandClassList.add(ClsCommand.class);
    commandClassList.add(ResetCommand.class);
    commandClassList.add(VersionCommand.class);
    commandClassList.add(SessionCommand.class);
    commandClassList.add(SystemPropertyCommand.class);
    commandClassList.add(SystemEnvCommand.class);
    commandClassList.add(VMOptionCommand.class);
    commandClassList.add(LoggerCommand.class);
    commandClassList.add(HistoryCommand.class);
    commandClassList.add(CatCommand.class);
    commandClassList.add(Base64Command.class);
    commandClassList.add(EchoCommand.class);
    commandClassList.add(PwdCommand.class);
    commandClassList.add(MBeanCommand.class);
    commandClassList.add(GrepCommand.class);
    commandClassList.add(TeeCommand.class);
    commandClassList.add(ProfilerCommand.class);
    commandClassList.add(VmToolCommand.class);
    commandClassList.add(StopCommand.class);
```

这里列出了 Arthas 的所有内置命令，可以按功能分类：

| 分类 | 命令 | 说明 |
|------|------|------|
| **基础命令** | help, version, cls, session, history, echo, pwd, cat, base64 | 基础信息和工具 |
| **系统信息** | jvm, memory, thread, dashboard, sysprop, sysenv, vmoption, mbean, perfcounter | 系统和 JVM 信息 |
| **类操作** | sc (SearchClass), sm (SearchMethod), classloader, jad, dump, redefine, retransform, mc | 类的搜索、反编译、热替换 |
| **增强命令** | watch, trace, stack, monitor, tt (TimeTunnel), line | 字节码增强诊断 |
| **OGNL** | ognl, getstatic | 表达式求值 |
| **日志** | logger | 日志级别动态修改 |
| **性能** | profiler | 性能火焰图 |
| **VM工具** | vmtool, heapdump | JVM 原生工具 |
| **安全** | auth, keymap, options | 认证和配置 |
| **管理** | reset, stop | 清理和停止 |
| **隐藏命令** | july, thanks | 彩蛋命令 |

---

### 10.3 JFR 命令的条件注册

```java
try {
    if (ClassLoader.getSystemClassLoader().getResource("jdk/jfr/Recording.class") != null) {
        commandClassList.add(ClassLoaderMetaspaceCommand.class);
        commandClassList.add(JFRCommand.class);
    }
} catch (Throwable e) {
    logger.error("This jdk version not support jfr command");
}
```

**它为什么存在？** JFR（Java Flight Recorder）是 JDK 11+ 才有的特性。
在 JDK 8 上运行 Arthas 时，`jdk/jfr/Recording.class` 不存在，
这两个命令就不会被注册。这是一种 **按能力注册** 的设计 ——
只注册当前 JVM 支持的命令，避免用户调用不支持的命令时产生困惑。

---

### 10.4 禁用命令机制

```java
for (Class<? extends AnnotatedCommand> clazz : commandClassList) {
    Name name = clazz.getAnnotation(Name.class);
    if (name != null && name.value() != null) {
        if (disabledCommands.contains(name.value())) {
            continue;
        }
    }
    commands.add(Command.create(clazz));
}
```

遍历所有命令类，检查是否在 `disabledCommands` 列表中。
如果被禁用，跳过注册。否则调用 `Command.create(clazz)` 创建命令实例。

`Command.create(clazz)` 的实现：

```java
public static Command create(final Class<? extends AnnotatedCommand> clazz) {
    return new AnnotatedCommandImpl(clazz);
}
```

非常简洁 —— 只是创建一个 `AnnotatedCommandImpl` 包装。

---

## 第十一阶段：CommandResolver 体系

### 11.1 CommandResolver 接口

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/command/CommandResolver.java`

```java
public interface CommandResolver {
    List<Command> commands();
}
```

这是一个极其简单的接口，只有一个方法：返回该解析器提供的所有命令列表。

**命令注册为什么要用 CommandResolver 而不是直接 Map？**

使用 Map 的方案（`Map<String, Command>`）虽然查找快，但有以下限制：

1. **不支持多源注册**：一个 Map 只能表示一个命令源，而 Arthas 需要支持多个命令源
  （BuiltinCommandResolver、BuiltinCommandPack、未来的插件命令）。

2. **不支持动态更新**：如果命令源是动态的（比如从远程加载命令），
   每次变化都需要更新 Map，而 CommandResolver 模式下只需要让
   `commands()` 方法返回最新的列表即可。

3. **不支持优先级**：多个 Map 合并时可能有命名冲突，而 `List<CommandResolver>` 的遍历顺序自然形成了优先级
  （先注册的 resolver 优先）。

| 方案 | 查找效率 | 多源支持 | 动态更新 | 优先级 |
|------|----------|----------|----------|--------|
| Map | O(1) | 差 | 差 | 差 |
| CommandResolver | O(n) | 好 | 好 | 好 |

在 Arthas 的场景下，命令数量只有 40+，O(n) 查找的性能完全可以接受。

---

### 11.2 BuiltinCommandResolver —— 内建命令解析器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/impl/BuiltinCommandResolver.java`

BuiltinCommandResolver 提供了最基础的内建命令（如 help），
它与 BuiltinCommandPack（提供所有 40+ 命令）是不同的。

它实现了 `ShellInternalCommandResolver` 标记接口，
这使得 `InternalCommandManager.getCommand()` 方法在查找命令时会跳过它：

```java
public Command getCommand(String commandName) {
    for (CommandResolver resolver : resolvers) {
        if (resolver instanceof ShellInternalCommandResolver) {
            continue;  // 跳过内建命令解析器
        }
        Command command = getCommand(resolver, commandName);
        if (command != null) {
            return command;
        }
    }
    return null;
}
```

**为什么要跳过 ShellInternalCommandResolver？** 因为内建命令（exit/jobs/fg/bg/kill）
已经在 ShellLineHandler 中被直接处理了，不需要再通过 Job 系统执行。
如果不跳过，这些命令会被创建为 Job，导致不必要的资源消耗和复杂性。

---

### 11.3 InternalCommandManager —— 命令管理器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/system/impl/InternalCommandManager.java`

InternalCommandManager 是连接 CommandResolver 和命令查找的核心管理器。

```java
public class InternalCommandManager {

    private final List<CommandResolver> resolvers;

    public InternalCommandManager(List<CommandResolver> resolvers) {
        this.resolvers = resolvers;
    }

    public Command getCommand(String commandName) {
        for (CommandResolver resolver : resolvers) {
            if (resolver instanceof ShellInternalCommandResolver) {
                continue;
            }
            Command command = getCommand(resolver, commandName);
            if (command != null) {
                return command;
            }
        }
        return null;
    }

    private static Command getCommand(CommandResolver commandResolver, String name) {
        List<Command> commands = commandResolver.commands();
        if (commands == null || commands.isEmpty()) {
            return null;
        }
        for (Command command : commands) {
            if (name.equals(command.name())) {
                return command;
            }
        }
        return null;
    }
}
```

命令查找的完整流程：

```
InternalCommandManager.getCommand("watch")
  → 遍历 resolvers 列表
    → 跳过 ShellInternalCommandResolver
    → 对 BuiltinCommandPack
      → 遍历 commands 列表
        → 比较 command.name() == "watch"
        → 找到 → 返回 WatchCommand 对应的 AnnotatedCommandImpl
```

---

### 11.4 Tab 补全的实现

InternalCommandManager 还负责 Tab 补全功能：

```java
public void complete(final Completion completion) {
    List<CliToken> lineTokens = completion.lineTokens();
    int index = findLastPipe(lineTokens);
    LinkedList<CliToken> tokens = new LinkedList<CliToken>(
        lineTokens.subList(index + 1, lineTokens.size()));

    while (tokens.size() > 0 && tokens.getFirst().isBlank()) {
        tokens.removeFirst();
    }

    if (tokens.size() > 1) {
        completeSingleCommand(completion, tokens);
    } else {
        completeCommands(completion, tokens);
    }
}
```

补全分两种情况：

1. **命令名补全**（用户输入了部分命令名，如 `wa` + Tab）：
   遍历所有命令，找到以输入前缀开头的命令名，提供补全列表。

2. **命令参数补全**（用户已经输入了完整命令名，如 `watch De` + Tab）：
   交给具体命令的 `complete()` 方法处理。

---

## 第十二阶段：ResultDistributor —— 结果分发

### 12.1 ResultDistributor 接口

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/distribution/ResultDistributor.java`

```java
public interface ResultDistributor {
    void appendResult(ResultModel result);
    void close();
}
```

**它为什么存在？** 命令的结果需要分发到不同的目标：
- Telnet 终端
- WebSocket 客户端
- HTTP API 调用者

ResultDistributor 抽象了结果分发的过程，使得命令代码不需要关心结果最终输出到哪里。

---

### 12.2 TermResultDistributorImpl —— 终端结果分发器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/distribution/impl/TermResultDistributorImpl.java`

```java
public class TermResultDistributorImpl implements ResultDistributor {

    private final CommandProcess commandProcess;
    private final ResultViewResolver resultViewResolver;
    private final Object outputLock = new Object();

    public TermResultDistributorImpl(CommandProcess commandProcess,
                                     ResultViewResolver resultViewResolver) {
        this.commandProcess = commandProcess;
        this.resultViewResolver = resultViewResolver;
    }

    @Override
    public void appendResult(ResultModel model) {
        ResultView resultView = resultViewResolver.getResultView(model);
        if (resultView != null) {
            synchronized (outputLock) {
                resultView.draw(commandProcess, model);
            }
        }
    }

    @Override
    public void close() {
    }
}
```

`appendResult()` 的执行流程：

1. 通过 `resultViewResolver.getResultView(model)` 找到与 ResultModel 对应的 ResultView
2. 用 `synchronized (outputLock)` 保证输出的原子性（避免多线程并发输出导致内容交错）
3. 调用 `resultView.draw(commandProcess, model)` 渲染结果到终端

**为什么需要 outputLock？** 因为某些命令（如 `watch`）的回调可能在多个线程上同时触发
（当被监控的方法被多个线程同时调用时）。没有锁的话，不同线程的输出可能会交叉混乱。

---

### 12.3 ResultViewResolver —— 视图解析器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/view/ResultViewResolver.java`

ResultViewResolver 维护了一个 ResultModel 类型到 ResultView 的映射：

```java
public class ResultViewResolver {

    private Map<Class, ResultView> resultViewMap = new ConcurrentHashMap<Class, ResultView>();

    public ResultViewResolver() {
        initResultViews();
    }

    public ResultView getResultView(ResultModel model) {
        return resultViewMap.get(model.getClass());
    }
}
```

查找逻辑非常简单 —— 根据 `model.getClass()` 在 Map 中查找对应的 View。
这是一种经典的 **策略模式** 的应用：不同类型的 ResultModel 有不同的渲染策略。

---

### 12.4 initResultViews() —— 视图注册

```java
private void initResultViews() {
    try {
        registerView(RowAffectView.class);

        // basic1000
        registerView(StatusView.class);
        registerView(VersionView.class);
        registerView(MessageView.class);
        registerView(HelpView.class);
        registerView(EchoView.class);
        registerView(CatView.class);
        // ... 其他 View

        // klass100
        registerView(ClassLoaderView.class);
        registerView(JadView.class);
        // ... 其他 View

        // monitor2000
        registerView(DashboardView.class);
        registerView(TraceView.class);
        registerView(WatchView.class);
        // ... 其他 View
    } catch (Throwable e) {
        logger.error("register result view failed", e);
    }
}
```

每个 View 在注册时，会通过反射自动关联到它对应的 ResultModel 类型：

```java
public void registerView(Class<? extends ResultView> viewClass) {
    ResultView view = null;
    try {
        view = viewClass.newInstance();
    } catch (Throwable e) {
        throw new RuntimeException("create view instance failure, viewClass:" + viewClass, e);
    }
    this.registerView(view);
}

public ResultViewResolver registerView(ResultView view) {
    Class modelClass = getModelClass(view);
    if (modelClass == null) {
        throw new NullPointerException("model class is null");
    }
    return this.registerView(modelClass, view);
}
```

`getModelClass()` 方法通过反射获取 View 的 `draw()` 方法的第二个参数类型：

```java
public static <V extends ResultView> Class getModelClass(V view) {
    Class<? extends ResultView> viewClass = view.getClass();
    Method[] declaredMethods = viewClass.getDeclaredMethods();
    for (int i = 0; i < declaredMethods.length; i++) {
        Method method = declaredMethods[i];
        if (method.getName().equals("draw")) {
            Class<?>[] parameterTypes = method.getParameterTypes();
            if (parameterTypes.length == 2
                    && parameterTypes[0] == CommandProcess.class
                    && parameterTypes[1] != ResultModel.class
                    && ResultModel.class.isAssignableFrom(parameterTypes[1])) {
                return parameterTypes[1];
            }
        }
    }
    return null;
}
```

这是一个非常巧妙的设计 —— 通过反射 `draw(CommandProcess, XxxModel)` 方法的签名，
自动推断出 View 对应的 Model 类型。开发者只需要继承 `ResultView<XxxModel>` 并实现 `draw()` 方法，
就会自动完成 View 和 Model 的关联，无需手动配置映射关系。

---

### 12.5 ResultView 抽象类

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/view/ResultView.java`

```java
public abstract class ResultView<T extends ResultModel> {

    public abstract void draw(CommandProcess process, T result);

    protected void writeln(CommandProcess process, String str) {
        process.write(str).write("\n");
    }
}
```

ResultView 是所有视图的抽象基类。注意代码注释 "Result view is a reusable and stateless instance"，
说明 View 实例是可重用的、无状态的。这意味着所有渲染逻辑不能依赖 View 实例的字段，
必须通过方法参数传入。

---

## 第十三阶段：信号处理 —— Ctrl+C 和 Ctrl+Z

### 13.1 InterruptHandler —— Ctrl+C 处理

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/handlers/shell/InterruptHandler.java`

```java
public class InterruptHandler implements SignalHandler {

    private ShellImpl shell;

    public InterruptHandler(ShellImpl shell) {
        this.shell = shell;
    }

    @Override
    public boolean deliver(int key) {
        if (shell.getForegroundJob() != null) {
            return shell.getForegroundJob().interrupt();
        }
        return true;
    }
}
```

当用户按下 Ctrl+C 时，InterruptHandler 被触发：
1. 获取当前前台 Job
2. 如果有前台 Job，调用 `job.interrupt()` 中断它
3. 返回值 `true` 表示信号已被消费，终端不需要做额外处理

调用链：
```
Ctrl+C → TermImpl → InterruptHandler.deliver()
  → shell.getForegroundJob().interrupt()
  → process.interrupt()
  → interruptHandler.handle(null)   // 命令注册的中断处理器
  → process.end()                   // 通常命令会在中断处理器中调用 end()
  → ProcessImpl.terminate()
```

---

### 13.2 SuspendHandler —— Ctrl+Z 处理

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/handlers/shell/SuspendHandler.java`

```java
public class SuspendHandler implements SignalHandler {

    private ShellImpl shell;

    public SuspendHandler(ShellImpl shell) {
        this.shell = shell;
    }

    @Override
    public boolean deliver(int key) {
        Term term = shell.term();

        Job job = shell.getForegroundJob();
        if (job != null) {
            term.echo(shell.statusLine(job, ExecStatus.STOPPED));
            job.suspend();
        }

        return true;
    }
}
```

当用户按下 Ctrl+Z 时：
1. 获取当前前台 Job
2. 向终端输出 Job 的状态信息（如 "[1]* Stopped trace Demo test"）
3. 调用 `job.suspend()` 挂起 Job

调用链：
```
Ctrl+Z → TermImpl → SuspendHandler.deliver()
  → term.echo(statusLine)           // 输出停止状态信息
  → job.suspend()
  → process.suspend(new SuspendHandler())
  → ProcessImpl.updateStatus(STOPPED, ...)
  → processForeground = false       // 解绑终端 I/O
  → suspendHandler.handle(null)     // 命令注册的挂起处理器
  → JobImpl.SuspendHandler.handle() // actualStatus = STOPPED
  → jobHandler.onSuspend(this)      // ShellJobHandler.onSuspend
  → resetAndReadLine()              // 重新显示提示符
```

---

## 第十四阶段：Session 锁机制

### 14.1 Session 接口中的锁

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/session/Session.java`

Session 接口定义了一组锁相关的方法：

```java
public interface Session {
    boolean isLocked();
    void unLock();
    boolean tryLock();
    int getLock();
}
```

---

### 14.2 SessionImpl 中的锁实现

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/shell/session/impl/SessionImpl.java`

```java
public class SessionImpl implements Session {
    private final static AtomicInteger lockSequence = new AtomicInteger();
    private final static int LOCK_TX_EMPTY = -1;
    private final AtomicInteger lock = new AtomicInteger(LOCK_TX_EMPTY);

    @Override
    public boolean tryLock() {
        return lock.compareAndSet(LOCK_TX_EMPTY, lockSequence.getAndIncrement());
    }

    @Override
    public void unLock() {
        int currentLockTx = lock.get();
        if (!lock.compareAndSet(currentLockTx, LOCK_TX_EMPTY)) {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean isLocked() {
        return lock.get() != LOCK_TX_EMPTY;
    }

    @Override
    public int getLock() {
        return lock.get();
    }
}
```

**Session.tryLock() 的作用 —— 为什么增强命令需要加锁？**

当用户执行 `watch Demo test` 这样的增强命令时，Arthas 需要修改目标类的字节码。
如果同时有两个增强命令试图增强同一个类（比如一个 `watch` 和一个 `trace`），
它们的字节码修改可能会互相覆盖或冲突。

Session 锁的设计解决了这个问题：
1. 增强命令执行前调用 `session.tryLock()` 尝试获取锁
2. 如果获取成功，执行增强操作
3. 增强操作完成后调用 `session.unLock()` 释放锁
4. 如果获取失败（其他增强命令正在执行），返回错误提示

**为什么用 AtomicInteger 而不是 ReentrantLock？** 因为这个锁不是线程锁，
而是一个"业务锁" —— 它要锁的不是线程，而是"增强操作"。
同一个线程可能在不同时间执行不同的增强命令，
这时候需要的是"操作级别"的互斥而非"线程级别"的互斥。

`lockSequence` 是一个全局递增的序列号，每次加锁都分配一个新的序列号。
这使得 `getLock()` 可以返回当前锁的"事务 ID"，用于调试和追踪。

---

## 第十五阶段：关键设计问题深入分析

### 15.1 为什么命令执行要放在线程池里而不是当前线程？

回顾 ProcessImpl.run() 中的这行代码：

```java
Runnable task = new CommandProcessTask(process);
ArthasBootstrap.getInstance().execute(task);
```

**深层原因分析**：

1. **Netty EventLoop 保护**：Arthas 使用 Netty 处理 Telnet/WebSocket 连接。
   Netty 的 EventLoop 是单线程的，如果在 EventLoop 中执行耗时命令
   （如 `jad` 反编译一个有几千行的类），会阻塞整个 EventLoop，
   导致所有连接无法响应。这就像在高速公路收费站让一辆车停下来修发动机 ——
   所有后面的车都会被堵住。

2. **支持长时间运行的命令**：像 `trace`、`watch` 这样的增强命令可能会持续运行数小时。
   如果在 Netty 线程上运行，Netty 的超时检测机制会认为这是一个异常连接。

3. **中断机制**：只有在独立线程上运行的任务，才能通过 `Thread.interrupt()` 机制中断。
   如果在 EventLoop 上运行，中断 EventLoop 会影响所有连接。

4. **线程堆栈独立**：命令执行的线程堆栈与 Netty I/O 线程分离，
   使得 `thread` 命令查看线程时，不会看到大量的 Netty I/O 线程噪声。

---

### 15.2 CommandProcessImpl 为什么要持有 AdviceListener 和 ClassFileTransformer？

这个设计决策的核心考量是 **生命周期绑定**。

| 方案 | 优势 | 劣势 |
|------|------|------|
| 全局管理 | 集中控制 | 难以关联到具体命令，清理困难 |
| 命令实例持有 | 命令结束即清理 | 命令实例生命周期短暂 |
| **Process 持有** | **与进程生命周期绑定** | **需要在 terminate 时清理** |

选择让 CommandProcessImpl 持有 listener 和 transformer，
是因为 Process 的生命周期恰好与增强的生命周期一致：
- 进程创建 → 可以注册增强
- 进程运行 → 增强生效
- 进程终止 → 增强需要清理

这是一种 **RAII**（Resource Acquisition Is Initialization）的设计思想 ——
资源的获取和释放与对象的生命周期绑定。

---

### 15.3 unregister() 为什么要在 terminate 时调用？

回顾 terminate 方法中的调用：

```java
private synchronized boolean terminate(int exitCode,
                                       Handler<Void> completionHandler,
                                       String message) {
    if (processStatus != ExecStatus.TERMINATED) {
        // ... 其他操作 ...
        if (process != null) {
            process.unregister();
        }
        return true;
    }
}
```

**如果不在 terminate 时调用 unregister() 会怎样？**

1. **内存泄漏**：AdviceWeaver 中注册的 listener 持有 Process 的引用，
   Process 持有 Session 的引用，Session 持有 Term 的引用...
   形成一条引用链，GC 无法回收。

2. **持续的性能开销**：被增强的方法每次调用都会触发 listener 的回调，
   即使这些回调的结果已经没有人接收了（因为命令进程已经终止）。

3. **字节码膨胀**：ClassFileTransformer 不移除的话，
   后续重新加载同一个类时（如热部署），增强代码会持续叠加。

4. **不可预期的行为**：如果用户再次执行 `watch` 命令增强同一个方法，
   旧的 listener 和新的 listener 同时生效，导致重复输出。

---

### 15.4 命令注册为什么要用 CommandResolver 而不是直接 Map？

| 考量因素 | Map 方案 | CommandResolver 方案 |
|----------|----------|---------------------|
| **可扩展性** | 差，添加新命令源需修改 Map 管理代码 | 好，只需添加新的 Resolver |
| **动态性** | 差，Map 内容在启动时固定 | 好，Resolver 可以动态返回不同的命令列表 |
| **优先级** | 需要额外的优先级管理 | 自然按 List 顺序形成优先级 |
| **隔离性** | 所有命令混在一个 Map 中 | 不同 Resolver 管理不同来源的命令 |
| **测试性** | 需要 mock 整个 Map | 可以注入 mock 的 Resolver |

Arthas 选择 CommandResolver 方案，体现了 **开闭原则** ——
对扩展开放（可以添加新的 Resolver），对修改关闭（不需要修改已有代码）。

---

### 15.5 前台/后台 Job 模型的设计思路

Arthas 的 Job 模型借鉴了 Unix Shell 的 Job Control 机制。
让我们对比两者：

| 特性 | Unix Shell | Arthas |
|------|------------|--------|
| 后台运行 | `command &` | `command &` |
| 查看 Job | `jobs` | `jobs` |
| 切换前台 | `fg %1` | `fg 1` 或 `fg %1` |
| 后台恢复 | `bg %1` | `bg 1` 或 `bg %1` |
| 终止 Job | `kill %1` | `kill 1` |
| 挂起 | Ctrl+Z | Ctrl+Z |
| 中断 | Ctrl+C | Ctrl+C |

**为什么 Arthas 要实现一个完整的 Job Control 系统？**

因为 Arthas 的很多命令（如 `trace`、`watch`、`dashboard`）是**长时间运行**的。
用户可能需要：
1. 同时运行多个 `watch` 命令监控不同的方法
2. 暂时挂起一个 `trace` 命令去执行其他诊断
3. 将一个 `dashboard` 放到后台持续收集数据

没有 Job Control，用户只能一次运行一个命令，极大地限制了诊断效率。

**前台 Job 与后台 Job 的核心区别**：

```
前台 Job：
  - 占据终端的 stdin（接收用户输入）
  - 输出直接显示在终端
  - 阻止新命令的输入
  - 可以被 Ctrl+C 中断、Ctrl+Z 挂起

后台 Job：
  - 不接收终端 stdin
  - 输出仍然显示在终端（与新命令的输出交错）
  - 不阻止新命令的输入
  - 只能通过 kill 命令终止
```

---

### 15.6 Session.tryLock() 的深入分析

让我们看一个具体的使用场景。当用户执行 `watch Demo test` 时，
EnhancerCommand（WatchCommand 的基类）的执行流程大致如下：

```
WatchCommand.process(process)
  → EnhancerCommand.enhance(process)
    → 1. session.tryLock()               // 尝试获取锁
    → 2. if (!locked) → 返回错误          // 已经有增强命令在执行
    → 3. Enhancer.enhance(...)            // 执行字节码增强
    → 4. process.register(listener, transformer)  // 注册监听器和转换器
    → 5. 命令持续运行，等待方法调用触发
    → ...
    → 6. process.end()                    // 命令结束
    → 7. ProcessImpl.terminate()
       → process.unregister()            // 卸载增强
       → session.unLock()                // 释放锁
```

**竞态条件分析**：如果两个终端同时执行增强命令：

```
终端 A: watch Demo test
终端 B: trace Demo test

时序：
  t1: 终端 A → session.tryLock() → 成功（lock = 0）
  t2: 终端 B → session.tryLock() → 失败（lock != -1）
  t3: 终端 B → 输出 "Other command is executing, please wait or use 'reset' to cancel"
  t4: 终端 A → 增强完成，命令开始运行
  t5: 终端 A → Ctrl+C → process.end() → unregister() → session.unLock()
  t6: 终端 B → 用户重新执行 → session.tryLock() → 成功
```

`AtomicInteger.compareAndSet` 保证了 `tryLock()` 的原子性，
避免了两个命令同时获取锁的竞态条件。

---

## 总结：完整执行链路的数据流图

最后，让我们用一张数据流图总结整个命令执行链路中数据的流向：

```
用户输入: "watch Demo test -n 5 | grep hello"
         │
         ▼
    ┌────────────┐
    │   CliTokens  │  tokenize
    │  .tokenize() │─────────────→ [text("watch"), blank(" "), text("Demo"),
    └────────────┘                  blank(" "), text("test"), blank(" "),
                                    text("-n"), blank(" "), text("5"),
                                    blank(" "), text("|"), blank(" "),
                                    text("grep"), blank(" "), text("hello")]
         │
         ▼
    ┌────────────────────┐
    │  ShellLineHandler   │  提取命令名: "watch"
    │  .handle(line)      │  非内建命令 → createJob
    └────────────────────┘
         │
         ▼
    ┌────────────────────┐
    │  JobControllerImpl  │  命令查找: "watch" → WatchCommand
    │  .createJob(...)    │  管道解析: | grep hello → GrepHandler
    │                     │  构建输出链: [GrepHandler, TermHandler]
    │                     │  创建 ProcessImpl + JobImpl
    └────────────────────┘
         │
         ▼
    ┌────────────────────┐
    │  JobImpl.run()      │  设置 Session, 启动 Process
    │                     │
    │  ProcessImpl.run()  │  解析参数: -n 5
    │                     │  提交到线程池
    └────────────────────┘
         │
         ▼  (线程池中执行)
    ┌────────────────────┐
    │  AnnotatedCommandImpl │  clazz.newInstance() → 创建 WatchCommand
    │  .process(process)    │  CLIConfigurator.inject() → 注入 numberOfLimit=5
    │                       │  instance.process(process) → WatchCommand.process()
    └────────────────────┘
         │
         ▼
    ┌────────────────────┐
    │  WatchCommand       │  增强字节码
    │  .process(process)  │  注册 AdviceListener
    │                     │  等待目标方法调用...
    └────────────────────┘
         │
         │  目标方法被调用
         ▼
    ┌────────────────────┐
    │  AdviceListener     │  创建 WatchModel (ResultModel)
    │  .afterReturning()  │  调用 process.appendResult(model)
    └────────────────────┘
         │
         ▼
    ┌────────────────────────────┐
    │  ResultDistributor          │
    │  .appendResult(model)       │
    │                             │
    │  ResultViewResolver         │  WatchModel → WatchView
    │  .getResultView(model)      │
    │                             │
    │  WatchView.draw(process,    │  渲染结果字符串
    │              model)         │
    │                             │
    │  process.write(data)        │  写入输出链
    └────────────────────────────┘
         │
         ▼
    ┌────────────────────────────┐
    │  ProcessOutput.write(data)  │
    │                             │
    │  stdoutHandlerChain:        │
    │  [0] GrepHandler("hello")  │──→ 过滤包含 "hello" 的行
    │  [1] TermHandler(term)     │──→ 写入终端显示给用户
    └────────────────────────────┘
         │
         ▼
    ┌────────────────────────────┐
    │  用户终端                    │
    │                             │
    │  显示匹配 "hello" 的         │
    │  watch 结果                  │
    └────────────────────────────┘
```

---

## 附录 A：核心类关系图

```
                    ┌───────────────┐
                    │  ShellServer  │ (abstract)
                    │   (抽象类)     │
                    └───────┬───────┘
                            │ extends
                    ┌───────┴───────┐
                    │ShellServerImpl│
                    │               │
                    │ - resolvers   │
                    │ - sessions    │
                    │ - jobController│
                    └───────┬───────┘
                            │ creates
                    ┌───────┴───────┐
                    │   ShellImpl   │
                    │               │
                    │ - session     │
                    │ - term        │
                    │ - commandManager│
                    │ - jobController│
                    └───────┬───────┘
                            │ uses
              ┌─────────────┼─────────────┐
              │             │             │
    ┌─────────┴──────┐ ┌───┴────┐ ┌──────┴──────┐
    │ShellLineHandler│ │SessionImpl│ │JobControllerImpl│
    │                │ │         │ │             │
    │ handle(line)   │ │tryLock()│ │createJob()  │
    │ → tokenize     │ │unLock() │ │createProcess│
    │ → createJob    │ │put/get  │ │close()      │
    └────────────────┘ └─────────┘ └──────┬──────┘
                                          │ creates
                            ┌─────────────┼─────────────┐
                            │                           │
                    ┌───────┴───────┐           ┌───────┴───────┐
                    │   JobImpl     │           │  ProcessImpl  │
                    │               │           │               │
                    │ - id          │           │ - command     │
                    │ - process     │           │ - handler     │
                    │ - line        │           │ - processOutput│
                    │ - runInBackground│        │ - resultDistributor│
                    │ run()/suspend()│           │               │
                    │ resume()/terminate()│      │ run()         │
                    └───────────────┘           │ terminate()   │
                                               └───────┬───────┘
                                                       │ creates
                                               ┌───────┴───────┐
                                               │CommandProcessImpl│
                                               │ (inner class)  │
                                               │                │
                                               │ - listener     │
                                               │ - transformer  │
                                               │ write()        │
                                               │ appendResult() │
                                               │ register()     │
                                               │ unregister()   │
                                               │ end()          │
                                               └────────────────┘
```

---

## 附录 B：命令执行链路中的线程切换

```
[Netty EventLoop Thread]
  │
  │ 接收 Telnet 数据
  │ 解析一行命令
  │ ShellLineHandler.handle(line)
  │ ShellImpl.createJob(tokens)
  │ JobControllerImpl.createJob(...)
  │ JobImpl.run()
  │ ProcessImpl.run()
  │   ├── 状态检查、参数解析（同步执行）
  │   └── ArthasBootstrap.execute(task) ────→ 提交到线程池
  │
  │ 返回 EventLoop（不阻塞）
  │
  ├─────────────────────────────────────────────────┐
  │                                                 │
  │                                    [Arthas Command Thread Pool]
  │                                                 │
  │                                    CommandProcessTask.run()
  │                                      │
  │                                      └── handler.handle(process)
  │                                            │
  │                                            └── WatchCommand.process(process)
  │                                                  │
  │                                                  ├── 字节码增强
  │                                                  └── 等待目标方法调用...
  │
  │                                    [Target Application Thread]
  │                                                 │
  │                                    被增强方法被调用
  │                                      │
  │                                      └── AdviceListener 回调
  │                                            │
  │                                            └── process.appendResult(model)
  │                                                  │
  │                                                  └── resultView.draw()
  │                                                        │
  │                                                        └── process.write(data)
  │                                                              │
  │◄─────────────────────────────────────────────────────────────┘
  │                                                  数据通过 TTY
  │ Netty 发送数据到终端                               写回 Netty Channel
  │
```

**三个关键线程**：

1. **Netty EventLoop Thread**：处理网络 I/O，解析命令，创建 Job
2. **Arthas Command Thread Pool**：执行命令逻辑，字节码增强
3. **Target Application Thread**：被增强的目标应用线程，触发回调

---

## 附录 C：设计模式在命令系统中的应用

| 设计模式 | 应用位置 | 说明 |
|----------|----------|------|
| **模板方法** | ShellServer 抽象类 | 定义骨架，子类提供实现 |
| **策略模式** | ResultView 体系 | 不同 Model 使用不同的渲染策略 |
| **责任链** | ProcessOutput.stdoutHandlerChain | 输出数据依次经过多个处理器 |
| **观察者** | JobListener | Job 状态变化时通知 Shell |
| **工厂方法** | Command.create(clazz) | 通过静态方法创建命令实例 |
| **适配器** | TermServerTermHandler | 适配 TermServer 和 ShellServerImpl |
| **代理** | AnnotatedCommandImpl | 代理执行注解命令的 process 方法 |
| **命令模式** | Job/Process/Command | 将请求封装为对象 |
| **状态机** | ExecStatus | READY → RUNNING → STOPPED → TERMINATED |
| **RAII** | register/unregister | 资源与 Process 生命周期绑定 |

---

## 附录 D：关键配置参数一览

| 配置项 | 默认值 | 配置方式 | 含义 |
|--------|--------|----------|------|
| sessionTimeout | 3 小时 | ShellServerOptions | 会话空闲超时 |
| reaperInterval | 60 秒 | ShellServerOptions | 会话回收检查间隔 |
| connectionTimeout | 6 秒 | ShellServerOptions | 连接超时 |
| jobTimeout | 1 天 | GlobalOptions | Job 最大运行时间 |
| maxRedirectJobs | 8 | 硬编码 | 最大重定向 Job 数 |
| isSaveResult | false | GlobalOptions | 是否自动保存结果 |

---

## 结语

通过这篇文章，我们从用户在终端输入命令的第一个字符开始，
一路追踪到命令被解析、创建为 Job、提交到线程池、反射创建命令实例、
执行字节码增强、接收回调输出结果、经过管道过滤、最终写入终端 ——
完整地还原了 Arthas 命令系统的每一个细节。

Arthas 的命令系统设计体现了几个核心原则：

1. **事件驱动**：整个系统基于回调和 Handler，没有阻塞等待
2. **生命周期管理**：通过 Process/Job 的状态机精确控制资源的获取和释放
3. **可扩展性**：CommandResolver 体系使得添加新命令非常简单
4. **健壮性**：异常处理、状态检查、锁机制确保了系统在各种极端情况下都能正常工作
5. **Unix 兼容**：Job Control 机制与 Unix Shell 保持一致，降低了用户的学习成本

理解了这套命令系统的设计，你就掌握了 Arthas 的"中枢神经系统"。
后续无论是分析具体命令的实现（如 watch、trace），
还是理解字节码增强机制（AdviceWeaver、Enhancer），
都会以本文的知识为基础。
