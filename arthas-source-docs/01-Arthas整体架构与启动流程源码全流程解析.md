# Arthas 整体架构与启动流程源码全流程解析

> 本文基于 Alibaba Arthas 开源项目源码进行分析，源码根目录为 `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas`。
> 分析对象为主干版本（agent 模块包名为 `com.taobao.arthas.agent334`，对应 JDK 8 及以上的字节码增强实现）。
> 本文只做源码分析、行为解读与设计推演，不对任何代码做增强或改写。
>
> 阅读本文你将彻底搞清楚一件事：当你在命令行敲下 `java -jar arthas-boot.jar` 之后，直到你看到那个熟悉的 `[arthas@12345]$` 提示符为止，Arthas 的三个进程、四个 ClassLoader、两个网络端口、一个 Spy 桥接类之间到底发生了什么。

---

## 目录

- [第零阶段：全局调用链总览](#第零阶段全局调用链总览)
- [模块架构总览](#模块架构总览)
- [第一阶段：boot 模块选目标 JVM 并拉起 core 进程](#第一阶段boot-模块选目标-jvm-并拉起-core-进程)
- [第二阶段：core 进程 attach 到目标 JVM](#第二阶段core-进程-attach-到目标-jvm)
- [第三阶段：目标 JVM 内 Agent 引导](#第三阶段目标-jvm-内-agent-引导)
- [第四阶段：ArthasBootstrap 初始化与 bind](#第四阶段arthasbootstrap-初始化与-bind)
- [第五阶段：客户端连接（TelnetConsole）](#第五阶段客户端连接telnetconsole)
- [第六阶段：关键设计问题深入分析](#第六阶段关键设计问题深入分析)
- [总结：从命令行到 Shell 就绪的完整时序](#总结从命令行到-shell-就绪的完整时序)

---

## 第零阶段：全局调用链总览

在深入任何一行源码之前，先建立一张“上帝视角”的地图。Arthas 的启动过程横跨 **三个独立的 JVM 进程**，这是很多初学者第一次读源码时最容易迷失的地方。请务必先记住下面这张图。

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  进程 A：arthas-boot.jar 进程（用户在命令行启动的进程，用完即退）                        │
│                                                                                    │
│  java -jar arthas-boot.jar                                                         │
│        │                                                                           │
│        ▼                                                                           │
│  Bootstrap.main(String[] args)                                                     │
│        │  1. CLIConfigurator.inject() 解析命令行参数                                  │
│        │  2. ProcessUtils.select()   ── jps 列出所有 Java 进程，让用户选 PID          │
│        │  3. 定位 arthas home（~/.arthas/lib/<version>/arthas 或 boot jar 同目录）     │
│        │  4. 拼装 attachArgs：-pid <pid> -core core.jar -agent agent.jar ...          │
│        │  5. ProcessUtils.startArthasCore(pid, attachArgs)                          │
│        │        │                                                                   │
│        │        ▼  用 ProcessBuilder 启动一个「新的 java 进程」                        │
│        │   ┌─────────────────────────────────────────────────────────────────┐     │
│        │   │  进程 B：arthas-core.jar 进程（attach 工具进程，attach 完就退出）      │     │
│        │   │                                                                   │     │
│        │   │  java -jar arthas-core.jar -pid <pid> ...                          │     │
│        │   │        │                                                          │     │
│        │   │        ▼                                                          │     │
│        │   │  Arthas.main(args) → new Arthas(args)                             │     │
│        │   │        │  parse(args) → 构造 Configure                             │     │
│        │   │        ▼                                                          │     │
│        │   │  attachAgent(configure)                                           │     │
│        │   │        │  VirtualMachine.attach(pid)   ← JVM Attach API           │     │
│        │   │        │  vm.loadAgent(agentJar, "coreJar;configureString")       │     │
│        │   │        │  vm.detach()                                             │     │
│        │   │        └───────────────┐                                          │     │
│        │   └────────────────────────┼──────────────────────────────────────────┘     │
│        │                            │  loadAgent 触发目标 JVM 加载 agent               │
│        │                            ▼                                                 │
│        │        ┌──────────────────────────────────────────────────────────────┐     │
│        │        │  进程 C：目标业务 JVM（被诊断的应用，Arthas 常驻其中）             │     │
│        │        │                                                                │     │
│        │        │  AgentBootstrap.agentmain(args, inst)  ← Agent-Class 入口       │     │
│        │        │        │  1. 防重复 attach 检查 SpyAPI.isInited()               │     │
│        │        │        │  2. 按 ';' 拆出 arthasCoreJar 与 agentArgs             │     │
│        │        │        │  3. new ArthasClassloader(core.jar)  ← 类隔离          │     │
│        │        │        │  4. 起 arthas-binding-thread 线程执行 bind()           │     │
│        │        │        ▼                                                       │     │
│        │        │  bind(inst, agentLoader, args)                                 │     │
│        │        │        │  反射 loadClass("...ArthasBootstrap")                  │     │
│        │        │        │  getInstance(inst, args) → new ArthasBootstrap(...)    │     │
│        │        │        ▼                                                       │     │
│        │        │  ArthasBootstrap 构造方法                                       │     │
│        │        │        │  initFastjson()                                       │     │
│        │        │        │  initSpy()          ← SpyAPI 塞进 BootstrapClassLoader │     │
│        │        │        │  initArthasEnvironment(args)                          │     │
│        │        │        │  LogUtil.initLogger()                                 │     │
│        │        │        │  enhanceClassLoader()                                 │     │
│        │        │        │  initBeans()                                          │     │
│        │        │        │  bind(configure)                                      │     │
│        │        │        │      │  TunnelClient.start()（可选）                   │     │
│        │        │        │      │  new ShellServerImpl(options)                  │     │
│        │        │        │      │  new BuiltinCommandPack()                      │     │
│        │        │        │      │  registerTermServer(HttpTelnetTermServer:3658) │     │
│        │        │        │      │  registerTermServer(HttpTermServer:8563)       │     │
│        │        │        │      │  registerCommandResolver(builtinCommands)      │     │
│        │        │        │      │  shellServer.listen()  ← 启动 Netty 监听        │     │
│        │        │        │      │  new SessionManagerImpl(...)                   │     │
│        │        │        │      │  new HttpApiHandler(...)                       │     │
│        │        │        │      │  SpyAPI.init()  ← 装配间谍类，INITED=true       │     │
│        │        │        ▼                                                       │     │
│        │        │  new TransformerManager(inst)  ← 注册字节码转换器                 │     │
│        │        │  Arthas Server 就绪，监听 3658(telnet) / 8563(http)             │     │
│        │        └──────────────────────────────────────────────────────────────┘     │
│        │                            ▲                                                 │
│        │  6. 反射加载 TelnetConsole  │  telnet 连接 127.0.0.1:3658                      │
│        ▼                            │                                                 │
│  TelnetConsole.process(args)  ──────┘                                                │
│        │  jline ConsoleReader 读输入 / TelnetClient 连接                               │
│        ▼                                                                             │
│  [arthas@<pid>]$  ← 用户看到提示符，Shell 就绪                                          │
└──────────────────────────────────────────────────────────────────────────────────┘
```

这张图里有几个关键事实，请提前建立心智模型：

1. **一共三个进程**：boot 进程、core 进程、目标业务 JVM。boot 和 core 都是“用完即走”的临时进程，真正常驻的是目标业务 JVM 里被注入的 Arthas Server。
2. **boot 进程活得最久**：因为它最后要变身成 telnet 客户端，陪你交互到你退出为止。core 进程在 `attach + loadAgent + detach` 三步做完后立刻退出。
3. **Agent 代码运行在目标 JVM 里**：这就是为什么 Arthas 能看到业务应用的类、能改字节码——因为它就在同一个 JVM 里。
4. **两条网络链路**：3658 是 telnet 端口（给命令行客户端用），8563 是 http 端口（给 Web Console 和 HTTP API 用）。

带着这张图，我们逐个进程、逐个方法地走一遍。

---

## 模块架构总览

Arthas 是一个典型的“多模块 Maven 工程”，根 `pom.xml`（`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/pom.xml`）中声明了近三十个模块。理解模块划分，是理解整个启动流程的前提——因为**启动流程本质上就是这些模块产物按特定顺序被加载、被 attach、被 bind 的过程**。

下表梳理与启动流程强相关的核心模块：

| 模块名 | 产物 (artifact) | 核心职责 | 运行在哪个进程 | 依赖关系 |
| --- | --- | --- | --- | --- |
| `boot` | `arthas-boot.jar` | 用户入口。选目标 JVM、定位/下载 arthas home、拉起 core 进程、最后变身 telnet 客户端 | 进程 A（boot） | 依赖 `common`；运行时通过反射调用 `client` 的 `TelnetConsole` |
| `core` | `arthas-core.jar` | 双重身份：(1) 作为 attach 工具（`Arthas.java`）；(2) 作为目标 JVM 内的服务端主体（`ArthasBootstrap`、shell/command/http 全部在此） | 进程 B（attach 阶段）+ 进程 C（服务端阶段） | 依赖 `common`、`spy`、`memorycompiler`、`tunnel-client`、`arthas-model` 等 |
| `agent` | `arthas-agent.jar` | JavaAgent 引导器。`Agent-Class`/`Premain-Class` 入口，创建 `ArthasClassloader`，反射引导 `ArthasBootstrap` | 进程 C（目标 JVM） | 依赖 `spy`；对 `core` 只有“字符串类名”级别的软依赖（反射加载，不直接编译依赖） |
| `spy` | `arthas-spy.jar` | 间谍类。定义 `java.arthas.SpyAPI`，是被增强字节码回调 Arthas 的“桥”。被塞进 BootstrapClassLoader | 进程 C（BootstrapClassLoader） | 无依赖，纯粹独立，包名故意放在 `java.arthas` 下 |
| `client` | `arthas-client.jar` | telnet 客户端。`TelnetConsole` 用 jline + commons-net 实现命令行交互 | 进程 A（被 boot 反射调用） | 依赖 `common` |
| `common` | `arthas-common.jar` | 公共工具类。`OSUtils`、`AnsiLog`、`IOUtils`、`UsageRender` 等，被几乎所有模块复用 | 全部进程 | 无对内依赖 |
| `memorycompiler` | `arthas-memorycompiler.jar` | 内存编译器。支持 `mc`/`redefine`/`retransform` 时把源码在内存里编译成 class | 进程 C | 独立模块 |
| `tunnel-client` | `arthas-tunnel-client.jar` | 隧道客户端。把 Arthas Agent 通过 WebSocket 反向注册到 tunnel-server，实现内网穿透 / 集群管理 | 进程 C | 依赖 `tunnel-common` |
| `tunnel-server` | `arthas-tunnel-server.jar` | 隧道服务端。一个独立的 Spring Boot 应用，聚合管理大量 Agent 连接 | 独立部署 | 依赖 `tunnel-common` |
| `tunnel-common` | `arthas-tunnel-common.jar` | 隧道协议公共类。定义 client/server 之间的消息格式 | tunnel 两端 | 无对内依赖 |
| `web-ui` | 静态资源 | 前端 Web Console（Vue/React），最终打进 core 的资源目录，通过 8563 端口访问 | 浏览器 | 前端工程 |
| `arthas-model` | `arthas-model.jar` | 命令结果的数据模型（`ResultModel` 系列），HTTP API 返回结构化 JSON 时使用 | 进程 C | 无对内依赖 |
| `arthas-agent-attach` | `arthas-agent-attach.jar` | 提供“进程内自 attach”的 API，让应用自己在代码里一行启动 Arthas（spring-boot-starter 底层） | 目标 JVM | 依赖 `core` |

### 为什么要拆这么多模块？

这里先抛出一个后面第六阶段会详细展开的核心问题：为什么不做成一个大 jar？简单回答是**类隔离与最小侵入**。

- `spy` 必须独立且极小：因为它要被塞进 BootstrapClassLoader，和业务代码在同一可见性层级，所以它不能带任何多余依赖，否则会污染业务应用的类空间。
- `agent` 必须独立且极小：它由目标 JVM 的 AppClassLoader 加载，只做“引导”这一件事，真正的重量级逻辑全在 core 里，通过自定义 ClassLoader 隔离。
- `boot` 必须独立：它是用户下载的唯一入口 jar，要足够小、能自举、能联网下载其余组件。

模块之间的依赖方向可以用一句话概括：**boot → (拉起) core → (attach) agent → (反射引导) core 服务端 → (回调) spy**。注意 agent 对 core 是“反射软依赖”，这是实现类隔离的关键手法。

---

## 第一阶段：boot 模块选目标 JVM 并拉起 core 进程

一切从 `Bootstrap.java` 开始。这是用户 `java -jar arthas-boot.jar` 后 JVM 找到的 `Main-Class`。

### 1.1 Bootstrap 类的字段与静态初始化块 —— 定义默认值与 arthas 目录

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/boot/src/main/java/com/taobao/arthas/boot/Bootstrap.java`

```java
@Name("arthas-boot")
@Summary("Bootstrap Arthas")
public class Bootstrap {
    private static final int DEFAULT_TELNET_PORT = 3658;
    private static final int DEFAULT_HTTP_PORT = 8563;
    private static final String DEFAULT_TARGET_IP = "127.0.0.1";
    private static final File ARTHAS_LIB_DIR;

    static {
        String arthasLibDirEnv = System.getenv("ARTHAS_LIB_DIR");
        if (arthasLibDirEnv != null) {
            ARTHAS_LIB_DIR = new File(arthasLibDirEnv);
        } else {
            ARTHAS_LIB_DIR = new File(
                    System.getProperty("user.home") + File.separator + ".arthas" + File.separator + "lib");
        }
        ARTHAS_LIB_DIR.mkdirs();
    }
```

逐段解释：

- `@Name("arthas-boot")` 与 `@Summary(...)`：这两个是 `com.taobao.middleware.cli`（Arthas 内置的一个仿 vert.x 的命令行解析框架）的注解。它们不是 Java 标准注解，作用是在打印 `--help` 时提供程序名和摘要。**它为什么存在？** 因为 Arthas 不想引入 picocli/commons-cli 这类外部依赖（会增大 boot jar 体积、增加冲突风险），于是自带了一套极小的 CLI 解析框架 `com.taobao.middleware.cli`。
- `DEFAULT_TELNET_PORT = 3658`、`DEFAULT_HTTP_PORT = 8563`：这两个魔法数字贯穿整个启动流程。3658 供 telnet 客户端连接，8563 供 HTTP/WebSocket 连接。记住它们，后面会反复出现。
- `DEFAULT_TARGET_IP = "127.0.0.1"`：默认只监听本地回环地址。**这是一个安全默认值**——只有显式改成 `0.0.0.0` 才会对外暴露，而那样会触发强制密码校验（见第四阶段安全认证部分）。
- 静态初始化块：确定 `ARTHAS_LIB_DIR`。优先读环境变量 `ARTHAS_LIB_DIR`，否则用 `~/.arthas/lib`。最后 `mkdirs()` 确保目录存在。

**这一步做了什么？** 它在类被加载时（即 main 执行前）就把“Arthas 组件的存放目录”准备好了。后面下载/定位 `arthas-core.jar`、`arthas-agent.jar` 都会落到这个目录的版本子目录下，例如 `~/.arthas/lib/3.6.7/arthas/`。

类比：这就像你装了一个“游戏启动器”（boot），启动器第一次运行时先在磁盘上建一个 `游戏本体安装目录`（ARTHAS_LIB_DIR），后面真正的游戏本体（core/agent）都装到那里去。

### 1.2 Bootstrap.main() —— 全流程的编排者

`main` 方法是整个 boot 进程的编排中心。它的骨架大致如下（结合源码提炼）：

```java
public static void main(String[] args) throws ProcessException, IOException {
    Package bootstrapPackage = Bootstrap.class.getPackage();
    if (args.length == 0) {
        // 无参时的处理：进入 select 逻辑
    }

    Bootstrap bootstrap = new Bootstrap();

    CLI cli = CLIConfigurator.define(Bootstrap.class);
    CommandLine commandLine = cli.parse(Arrays.asList(args));

    try {
        CLIConfigurator.inject(commandLine, bootstrap);
    } catch (Throwable e) {
        // 参数非法 → 打印 usage 并退出
        System.out.println(usage(cli));
        System.exit(1);
    }

    if (bootstrap.isVersion()) { ... }   // --version
    if (bootstrap.isHelp()) { ... }       // --help
    // ... 后续见下文
}
```

逐段解释：

- `CLIConfigurator.define(Bootstrap.class)`：通过反射扫描 `Bootstrap` 类上的 `@Option`、`@Argument` 注解，构造出一个 `CLI` 描述对象。**它为什么存在？** 把“命令行参数长什么样”这件事声明式地写在字段的 setter 上，而不是手写 `if (arg.equals("-p"))` 这种命令式解析。
- `cli.parse(Arrays.asList(args))`：把用户输入的 `String[]` 解析成一个结构化的 `CommandLine`。
- `CLIConfigurator.inject(commandLine, bootstrap)`：把解析结果反射注入到 `bootstrap` 实例的字段（通过调用带 `@Option` 的 setter）。执行完这一步，`bootstrap.pid`、`bootstrap.telnetPort` 等字段就都有值了。
- `isVersion()` / `isHelp()`：优先处理这两个“短路”选项，打印信息后直接退出，不进入 attach 流程。

**这一步做了什么？** 把命令行的原始字符串数组，转成一个填好字段的 `Bootstrap` 对象。之后所有决策（选哪个 pid、用哪个端口、去哪找 arthas home）都基于这个对象的字段。

### 1.3 @Option / @Argument 注解 —— 声明式参数解析

**源码位置**：同 `Bootstrap.java`，字段的 setter 上。典型例子：

```java
@Argument(argName = "pid", index = 0, required = false)
@Description("Target pid")
public void setPid(long pid) {
    this.pid = pid;
}

@Option(shortName = "h", longName = "help", flag = true)
@Description("Print usage")
public void setHelp(boolean help) {
    this.help = help;
}

@Option(longName = "telnet-port")
@Description("The local telnet port of arthas server bind, default 3658")
public void setTelnetPort(int telnetPort) {
    this.telnetPort = telnetPort;
}

@Option(longName = "target-ip")
@Description("The target jvm listen ip, default 127.0.0.1")
public void setTargetIp(String targetIp) {
    this.targetIp = targetIp;
}
```

解释这套注解的语义：

| 注解 | 含义 | 例子 |
| --- | --- | --- |
| `@Argument(index=0)` | 位置参数，第 0 个非选项参数。用户可以直接 `arthas-boot 12345`，12345 就是 pid | `setPid` |
| `@Option(flag=true)` | 布尔开关，出现即为 true，无需带值 | `--help` |
| `@Option(longName="telnet-port")` | 带值的长选项，用 `--telnet-port 9999` 传入 | `setTelnetPort` |
| `@Description` | 在 usage 里展示的说明文字 | 全部 |

**它为什么存在？** 这是“配置即代码”的思想：把参数定义和它对应的字段绑在一起，避免解析逻辑和字段赋值分散在两处而不一致。`CLIConfigurator.inject` 会根据 `@Option/@Argument` 找到对应 setter 并调用。

**类比**：这就像 Spring MVC 的 `@RequestParam`——你在方法参数上声明“我要一个叫 telnet-port 的参数”，框架负责从原始请求里把它抠出来塞给你。

### 1.4 ProcessUtils.select() —— 通过 jps 列出并选择目标 JVM

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/boot/src/main/java/com/taobao/arthas/boot/ProcessUtils.java`

当用户没有在命令行显式给出 pid 时，Bootstrap 需要“交互式”地让用户从当前机器上所有 Java 进程里选一个。这就是 `ProcessUtils.select()` 的职责。

```java
public static long select(boolean verbose, Long telnetPortPid, String select) throws InputMismatchException {
    Map<Long, String> processMap = listProcessByJps(verbose);
    if (processMap.isEmpty()) {
        AnsiLog.info("Can not find java process. Try to run `jps` command lists the instrumented Java HotSpot VMs on the target system.");
        return -1;
    }

    AnsiLog.info("Found existing java process, please choose one and input the serial number of the process, eg : 1. Then hit ENTER.");

    // 打印进程列表，让用户选
    int count = 1;
    for (String process : processMap.values()) {
        if (count == 1) {
            System.out.println("* [" + count + "]: " + process);
        } else {
            System.out.println("  [" + count + "]: " + process);
        }
        count++;
    }

    String line = new Scanner(System.in).nextLine();
    if (line.trim().isEmpty()) {
        // user hit enter, and there is only one process, select it
        ...
    }
    int choice = new Scanner(line).nextInt();
    ...
}
```

逐段解释：

- `listProcessByJps(verbose)`：内部调用 JDK 自带的 `jps` 机制（实际是通过 `sun.jvmstat` 或直接执行 jps 命令），列出当前用户能看到的所有 Java 进程，返回一个 `Map<pid, 进程描述>`。
- 如果一个 Java 进程都没有，直接提示并返回 -1。
- 否则把进程列表打印出来，用序号 `[1] [2] [3]...` 标注，第一个用 `*` 高亮（表示默认选中）。
- 用 `Scanner` 从标准输入读用户输入的序号，转成对应的 pid 返回。

**它为什么存在？** 因为 Arthas 面向的是“运维排障”场景，用户往往不记得目标应用的 pid。与其让用户先手动 `jps` 再复制 pid，不如 boot 直接把候选列表端到用户面前。这是极佳的“开发者体验”设计。

**这一步做了什么？** 把“选哪个 JVM”这个决策交互式地完成，最终产出一个确定的 `long pid`。这个 pid 是后续 attach 的目标。

**注意排除自身**：`listProcessByJps` 会把 arthas-boot 自己的进程过滤掉（通过匹配类名 `arthas-boot.jar` / `Bootstrap`），避免用户误选到 boot 进程去 attach 自己。

### 1.5 定位 arthas home —— 三级查找策略

选好 pid 后，Bootstrap 需要确定 `arthas-core.jar` 和 `arthas-agent.jar` 到底在磁盘的哪个位置。这就是“定位 arthas home”。逻辑分三级：

1. **用户显式指定**：如果命令行带了 `--arthas-home <dir>`，直接用它。
2. **boot jar 同目录**：如果 `arthas-boot.jar` 所在目录下就有 `arthas-core.jar`（即用户下载的是完整发行包 `arthas-bin.zip` 解压后的目录），直接用同目录。
3. **下载到 ~/.arthas/lib**：否则认为用户下载的是单文件 `arthas-boot.jar`（自举模式），需要联网下载。这时会：
   - 通过 `DownloadUtils` 查询远端最新版本 / 指定版本；
   - 下载对应版本的 `arthas-bin.zip` 到 `~/.arthas/lib/<version>/`；
   - 解压得到 `arthas-core.jar`、`arthas-agent.jar`、`arthas-spy.jar` 等。

关键判断代码（提炼自 Bootstrap.main）：

```java
File arthasHomeDir = null;
if (bootstrap.getArthasHome() != null) {
    verifyArthasHome(bootstrap.getArthasHome());
    arthasHomeDir = new File(bootstrap.getArthasHome());
}

if (arthasHomeDir == null) {
    // 尝试 boot jar 同目录
    File bootJarPath = ...;
    if (verifyArthasHome(bootJarPath) 成功) {
        arthasHomeDir = bootJarPath;
    }
}

if (arthasHomeDir == null) {
    // 走下载逻辑：DownloadUtils.getRemoteLastestVersion / downArthasPackaging
    ...
    arthasHomeDir = new File(ARTHAS_LIB_DIR, arthasVersion + File.separator + "arthas");
}
```

**它为什么存在？** Arthas 支持两种分发形态：(a) 完整包（解压即用，离线可用）；(b) 单个 `arthas-boot.jar`（体积小，联网自举下载）。这套三级查找就是为了同时兼容这两种形态，并给高级用户 `--arthas-home` 的逃生舱。

**这一步做了什么？** 产出一个确定的 `arthasHomeDir`，保证 core.jar / agent.jar / spy.jar 三个关键产物都在这个目录里可用。

### 1.6 拼装 attachArgs —— 传给 core 进程的命令行

有了 pid 和 arthasHomeDir，Bootstrap 开始拼装启动 core 进程所需的参数。这些参数决定了 core 进程要 attach 谁、加载哪个 agent、用什么端口。

提炼后的拼装逻辑：

```java
List<String> attachArgs = new ArrayList<String>();
attachArgs.add("-jar");
attachArgs.add(new File(arthasHomeDir, "arthas-core.jar").getAbsolutePath());
attachArgs.add("-pid");
attachArgs.add("" + pid);
attachArgs.add("-target-ip");
attachArgs.add(bootstrap.getTargetIp());
attachArgs.add("-telnet-port");
attachArgs.add("" + bootstrap.getTelnetPort());
attachArgs.add("-http-port");
attachArgs.add("" + bootstrap.getHttpPort());
attachArgs.add("-core");
attachArgs.add(new File(arthasHomeDir, "arthas-core.jar").getAbsolutePath());
attachArgs.add("-agent");
attachArgs.add(new File(arthasHomeDir, "arthas-agent.jar").getAbsolutePath());
// 还会追加 username/password、tunnel-server、app-name 等可选项
```

逐项解释这些参数的用途：

| 参数 | 含义 | 谁会用到 |
| --- | --- | --- |
| `-jar arthas-core.jar` | 告诉新 java 进程运行 core.jar 的 Main-Class（`Arthas`） | JVM |
| `-pid <pid>` | attach 的目标进程号 | `Arthas.attachAgent` |
| `-target-ip` | Arthas Server 最终绑定的 IP，默认 127.0.0.1 | 传给 agent → ArthasBootstrap |
| `-telnet-port` | telnet 监听端口 3658 | 传给 agent → ArthasBootstrap |
| `-http-port` | http 监听端口 8563 | 传给 agent → ArthasBootstrap |
| `-core` | core.jar 的绝对路径，agent 里要用它构造 ArthasClassloader | agent |
| `-agent` | agent.jar 的绝对路径，`vm.loadAgent` 的第一个参数 | `Arthas.attachAgent` |

**它为什么存在？** boot 进程和 core 进程是两个独立 JVM，它们之间唯一的通信方式就是“命令行参数”。boot 把所有决策结果（pid、端口、jar 路径）编码进这个参数数组，交给 core 去执行。

### 1.7 ProcessUtils.startArthasCore() —— 用 ProcessBuilder 拉起 core 进程

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/boot/src/main/java/com/taobao/arthas/boot/ProcessUtils.java`

```java
public static int startArthasCore(long targetPid, List<String> attachArgs) {
    // 找到当前 JVM 的 java 可执行文件路径
    String javaHome = findJavaHome();
    // java 命令的完整路径
    File javaPath = findJava();
    if (javaPath == null) {
        throw new IllegalArgumentException(
            "Can not find java/java.exe executable file under java home: " + javaHome);
    }

    List<String> command = new ArrayList<String>();
    command.add(javaPath.getAbsolutePath());
    // Add jvm options for attach ...
    command.addAll(attachArgs);

    // 用 ProcessBuilder 启动
    ProcessBuilder pb = new ProcessBuilder(command);
    try {
        final Process proc = pb.start();
        // 把 core 进程的 stdout/stderr 转发到当前控制台
        Thread redirectStdout = new Thread(new Runnable() { ... });
        Thread redirectStderr = new Thread(new Runnable() { ... });
        redirectStdout.start();
        redirectStderr.start();
        // 等待 core 进程结束（attach 完成后 core 会主动退出）
        final int exitValue = proc.waitFor();
        redirectStdout.join();
        redirectStderr.join();
        return exitValue;
    } catch (Throwable e) {
        AnsiLog.error("Start arthas failed, exception:", e);
        return -1;
    }
}
```

逐段解释：

- `findJava()`：定位 `java` 可执行文件。优先用 `JAVA_HOME`，找不到再退化查找。**关键点**：core 进程用的 java 必须能访问目标 JVM 的 attach 机制，通常要求是 JDK（含 `tools.jar` / attach 支持）而非纯 JRE。
- `command.add(javaPath)` + `command.addAll(attachArgs)`：拼成完整命令 `java -jar arthas-core.jar -pid ... -agent ...`。
- `new ProcessBuilder(command).start()`：真正 fork 出进程 B（core 进程）。
- 两个 redirect 线程：因为 core 进程的输出（比如 attach 的进度日志）需要实时显示给用户，所以 boot 起两个线程把 core 的 stdout/stderr 抽到 boot 自己的控制台。
- `proc.waitFor()`：**boot 在此阻塞等待 core 退出**。因为 core 的使命就是 attach，做完就退，所以这里等到的就是 attach 的最终结果码。

**它为什么存在？** attach 一个 JVM 需要用到 `com.sun.tools.attach.VirtualMachine`，而这套 API 在不同 JDK 版本里可用性不同，且需要合适的 classpath。与其在 boot 进程里直接 attach（boot 的运行环境不可控），不如**新起一个干净的、用同一个 java 启动的 core 进程专门干 attach**——这样 attach 环境和目标 JVM 的 java 版本天然一致，兼容性最好。

**这一步做了什么？** 从 boot 进程 fork 出 core 进程，并阻塞等待它完成 attach。至此，第一阶段（boot 的职责）基本收尾，控制权转移到 core 进程。

### 1.8 boot 最后一步：反射加载 TelnetConsole 连接 telnet 端口

core 进程 attach 成功退出后，`startArthasCore` 返回。boot 进程接下来的最后一件事，就是**把自己变成一个 telnet 客户端**，连上刚刚在目标 JVM 里起好的 Arthas Server（127.0.0.1:3658）。

提炼逻辑：

```java
if (exitValue != 0) {
    // core 进程 attach 失败，打印错误并退出
    ...
} else {
    // attach 成功，启动 telnet 客户端
    URLClassLoader classLoader = new URLClassLoader(
        new URL[] { new File(arthasHomeDir, "arthas-client.jar").toURI().toURL() });
    Class<?> telnetConsoleClas = classLoader.loadClass("com.taobao.arthas.client.TelnetConsole");
    Method processMethod = telnetConsoleClas.getMethod("process", String[].class);

    List<String> telnetArgs = new ArrayList<String>();
    telnetArgs.add("-c"); // 若指定了 -c 命令
    ...
    telnetArgs.add(ip);
    telnetArgs.add("" + port);

    processMethod.invoke(null, (Object) telnetArgs.toArray(new String[0]));
}
```

逐段解释：

- **为什么用 URLClassLoader 反射加载？** 因为 `arthas-client.jar` 并不在 boot 进程的 classpath 里（boot jar 很小，不含 client）。boot 通过一个独立的 `URLClassLoader` 把 client.jar 动态加载进来，再反射调用 `TelnetConsole.process`。这样 boot jar 本身可以保持精简。
- **为什么调 `process` 而不是 `main`？** 看 `TelnetConsole` 源码可知，`main` 内部会 `System.exit()`，而 boot 不希望 client 退出时把整个 boot 进程也带走（尤其在批处理模式下要拿返回码）。`process(String[])` 是专门提供给 boot 复用的入口，内部不调用 `System.exit`。源码注释明确写了：“注意：process()函数提供给arthas-boot使用，内部不能调用System.exit()结束进程的方法”。

**这一步做了什么？** 让用户从“启动 Arthas”无缝过渡到“已经连上 Arthas 的交互终端”。用户完全感知不到中间那个短命的 core 进程。

---

## 第二阶段：core 进程 attach 到目标 JVM

现在控制权在进程 B（core 进程）。它的 `Main-Class` 是 `com.taobao.arthas.core.Arthas`。core 进程的唯一使命：**用 JVM Attach API 把 agent.jar 注入目标 JVM，然后退出**。

### 2.1 Arthas 构造方法 —— 极简的三步走

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/Arthas.java`

```java
public class Arthas {
    private static final String DEFAULT_TELNET_PORT = "3658";
    private static final String DEFAULT_HTTP_PORT = "8563";

    private Arthas(String[] args) throws Exception {
        attachAgent(parse(args));
    }

    public static void main(String[] args) {
        try {
            new Arthas(args);
        } catch (Throwable t) {
            AnsiLog.error("Start arthas failed, exception stack trace: ");
            t.printStackTrace();
            System.exit(-1);
        }
    }
}
```

逐段解释：

- `main` 只做一件事：`new Arthas(args)`。构造方法里 `attachAgent(parse(args))`——先 `parse` 把命令行参数变成 `Configure`，再 `attachAgent` 执行注入。
- 出任何异常就打印堆栈并 `System.exit(-1)`。这个 -1 会被 boot 进程的 `proc.waitFor()` 拿到，从而 boot 知道 attach 失败了。

**这一步做了什么？** 定义 core 进程的顶层编排：解析参数 → attach。极其克制，没有任何多余逻辑，因为 core 进程要尽快完成使命并退出。

### 2.2 Arthas.parse() —— 把命令行参数还原成 Configure

**源码位置**：同 `Arthas.java`

```java
private Configure parse(String[] args) {
    Option pid = new TypedOption<Long>().setType(Long.class).setShortName("pid").setRequired(true);
    Option core = new TypedOption<String>().setType(String.class).setShortName("core").setRequired(true);
    Option agent = new TypedOption<String>().setType(String.class).setShortName("agent").setRequired(true);
    Option target = new TypedOption<String>().setType(String.class).setShortName("target-ip");
    Option telnetPort = new TypedOption<Integer>().setType(Integer.class)
            .setShortName("telnet-port").setDefaultValue(DEFAULT_TELNET_PORT);
    Option httpPort = new TypedOption<Integer>().setType(Integer.class)
            .setShortName("http-port").setDefaultValue(DEFAULT_HTTP_PORT);
    // ... username / password / tunnel-server / agent-id 等

    CLI cli = CLIs.create("arthas").addOption(pid).addOption(core).addOption(agent)
            .addOption(target).addOption(telnetPort).addOption(httpPort);
    CommandLine commandLine = cli.parse(Arrays.asList(args));

    Configure configure = new Configure();
    configure.setJavaPid((Long) commandLine.getOptionValue("pid"));
    configure.setArthasAgent((String) commandLine.getOptionValue("agent"));
    configure.setArthasCore((String) commandLine.getOptionValue("core"));
    if (commandLine.getOptionValue("target-ip") != null) {
        configure.setIp((String) commandLine.getOptionValue("target-ip"));
    }
    configure.setTelnetPort((Integer) commandLine.getOptionValue("telnet-port"));
    configure.setHttpPort((Integer) commandLine.getOptionValue("http-port"));
    return configure;
}
```

逐段解释：

- 这里同样用 `com.taobao.middleware.cli`，但用的是编程式 API（`new TypedOption`）而非注解式。原因是 core 的这几个参数很固定，直接构造更直观。
- `pid`、`core`、`agent` 三个都 `setRequired(true)`——它们是 attach 的必需信息，缺一不可。
- 解析完把值塞进一个 `Configure` 对象。**`Configure` 是 core 进程和目标 JVM 内 agent 之间传递配置的载体**，它有一个 `toString()`（实际是序列化成特定字符串格式）方法，后面会作为 `loadAgent` 的参数传进目标 JVM。

**它为什么存在？** boot 用命令行参数把配置传给 core，core 又要把这些配置“转运”给目标 JVM 里的 agent。`Configure` 就是这个配置在 core 进程里的内存表示，`parse` 负责“命令行 → Configure”，稍后的 `configure.toString()` 负责“Configure → 字符串”以便跨进程传递。

### 2.3 Arthas.attachAgent() —— JVM Attach API 的核心三步

**源码位置**：同 `Arthas.java`

```java
private void attachAgent(Configure configure) throws Exception {
    VirtualMachineDescriptor virtualMachineDescriptor = null;
    for (VirtualMachineDescriptor descriptor : VirtualMachine.list()) {
        String pid = descriptor.id();
        if (pid.equals(Long.toString(configure.getJavaPid()))) {
            virtualMachineDescriptor = descriptor;
            break;
        }
    }

    VirtualMachine virtualMachine = null;
    try {
        if (null == virtualMachineDescriptor) { // 使用 attach(String pid) 这种方式
            virtualMachine = VirtualMachine.attach("" + configure.getJavaPid());
        } else {
            virtualMachine = VirtualMachine.attach(virtualMachineDescriptor);
        }

        Properties targetSystemProperties = virtualMachine.getSystemProperties();
        String targetJavaVersion = JavaVersionUtils.javaVersionStr(targetSystemProperties);
        String currentJavaVersion = JavaVersionUtils.javaVersionStr();
        if (targetJavaVersion != null && currentJavaVersion != null) {
            if (!targetJavaVersion.equals(currentJavaVersion)) {
                AnsiLog.warn("Current VM java version: {} do not match target VM java version: {}, attach may fail.",
                        currentJavaVersion, targetJavaVersion);
                AnsiLog.warn("Target VM JAVA_HOME is {}, arthas-boot JAVA_HOME is {}, try to set the same JAVA_HOME.",
                        targetSystemProperties.getProperty("java.home"), System.getProperty("java.home"));
            }
        }

        String arthasAgentPath = configure.getArthasAgent();
        //convert jar path to unicode string
        configure.setArthasAgent(encodeArg(arthasAgentPath));
        configure.setArthasCore(encodeArg(configure.getArthasCore()));
        try {
            virtualMachine.loadAgent(arthasAgentPath,
                    configure.getArthasCore() + ";" + configure.toString());
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().contains("Non-numeric value found")) {
                AnsiLog.warn(e);
                AnsiLog.warn("It seems to use the lower version of JDK to attach the higher version of JDK.");
                AnsiLog.warn("This error message can be ignored, the attach may have been successful ...");
            } else {
                throw e;
            }
        } catch (com.sun.tools.attach.AgentLoadException ex) {
            if ("0".equals(ex.getMessage())) {
                // https://stackoverflow.com/a/54454418
                AnsiLog.warn(ex);
                AnsiLog.warn("It seems to use the higher version of JDK to attach the lower version of JDK.");
                AnsiLog.warn("This error message can be ignored, the attach may have been successful ...");
            } else {
                throw ex;
            }
        }
    } finally {
        if (null != virtualMachine) {
            virtualMachine.detach();
        }
    }
}

private static String encodeArg(String arg) {
    try {
        return URLEncoder.encode(arg, "utf-8");
    } catch (UnsupportedEncodingException e) {
        return arg;
    }
}
```

这是整个 attach 阶段最核心的方法，逐段拆解：

**第一步：找到目标 VirtualMachineDescriptor**

- `VirtualMachine.list()` 列出当前机器上所有可 attach 的 JVM（等价于 jps）。
- 遍历找出 `descriptor.id()` 等于目标 pid 的那个。
- 找不到 descriptor 时，退化用 `VirtualMachine.attach("" + pid)`（直接按 pid 字符串 attach）。这是一种兜底——某些环境下 list 不全，但直接按 pid attach 仍可能成功。

**第二步：attach + 版本检查**

- `VirtualMachine.attach(...)` 是 `com.sun.tools.attach.VirtualMachine` 提供的 JDK Attach API。它底层通过一个“attach socket”（Linux 上是 `/tmp/.java_pidXXX` 这个 UNIX domain socket）与目标 JVM 的 `Attach Listener` 线程通信。
- attach 上之后，`virtualMachine.getSystemProperties()` 读到目标 JVM 的系统属性，从而拿到目标 JVM 的 java 版本。
- 把目标 JVM 的 java 版本和当前 core 进程的 java 版本比较，不一致就 `AnsiLog.warn` 警告。**为什么要检查版本？** 因为 attach + loadAgent 涉及跨 JVM 加载字节码，如果两边 java 大版本差异过大（例如 core 用 JDK17，目标用 JDK8），字节码版本或 attach 协议可能不兼容，导致 attach 失败或行为异常。这里给出明确的排查指引（提示两边的 JAVA_HOME）。

**第三步：loadAgent + detach**

- `virtualMachine.loadAgent(arthasAgentPath, options)`：这是最关键的一行。它让目标 JVM 加载 `arthas-agent.jar`，并调用其 `Agent-Class`（即 `AgentBootstrap`）的 `agentmain(String args, Instrumentation inst)` 方法，把 `options` 字符串作为 `args` 传进去。
- **`options` 的格式是 `core.jar路径 + ";" + configure.toString()`**——用一个分号把两部分拼在一起。目标 JVM 里的 `AgentBootstrap` 收到后会按第一个 `;` 拆开：前半是 core.jar 路径（用来构造 ArthasClassloader），后半是序列化的配置（端口、IP、密码等）。
- `virtualMachine.detach()`：attach 只是为了触发 loadAgent，一旦 agent 加载完成，core 进程就 detach 断开。**detach 后 agent 依然常驻目标 JVM**——因为 agent 的逻辑运行在目标 JVM 的线程里，与 core 进程的连接无关。

### 2.4 三个关于 attach 的关键问答

**Q1：VirtualMachine.attach 的机制原理是什么？**

JVM 有一个 attach 机制：当外部进程连接目标 JVM 的 attach socket 时，目标 JVM 会懒启动一个名为 `Attach Listener` 的守护线程。外部进程通过这个 socket 发送命令（如 `load <agent> <options>`），`Attach Listener` 线程在目标 JVM 内部执行对应动作（加载 agent）。整个过程不需要目标 JVM 事先配置任何东西，是 JDK 内建能力。类比：attach socket 就像目标 JVM 偷偷开的一个“后门管理端口”，只有同机器同用户能连，连上后可以下发几条管理命令。

**Q2：loadAgent 的参数为什么要用 `;` 分隔？**

因为 `loadAgent(agentJar, options)` 只允许传一个 `String options`，而 Arthas 需要传两样东西给目标 JVM：core.jar 的路径 + 完整配置。于是用 `;` 把两者拼成一个字符串，到了目标 JVM 里 `AgentBootstrap` 再 `args.indexOf(';')` 拆开。这是“单通道传多参数”的经典手法。

**Q3：为什么要对 jar 路径做 URL encode？**

因为 jar 的绝对路径可能包含空格、中文、特殊字符（例如 Windows 的 `C:\Program Files\...` 或 macOS 的中文用户目录）。而 `loadAgent` 的 options 字符串是用 `;` 等分隔符解析的，如果路径里恰好含分隔符或空白，会破坏解析。做 URL encode（或至少对分隔符转义）能保证路径原样、无歧义地穿越到目标 JVM。

**Q4：版本不匹配时的警告逻辑是怎样的？**

如上所述，attach 上后对比 `targetJavaVersion` 与 `currentJavaVersion`，不等就打两条 `warn`：第一条说“版本不匹配，attach 可能失败”，第二条给出两边的 `java.home`，提示用户“把 arthas-boot 的 JAVA_HOME 设成和目标 JVM 一样”。注意它只是 warn 而非直接失败——因为很多小版本差异（如 8u201 vs 8u211）其实兼容，Arthas 选择继续尝试而不是武断拒绝。

**Q5：`encodeArg` 具体做了什么？为什么只编码 core/agent 两个路径？**

`encodeArg(arg)` 就是 `URLEncoder.encode(arg, "utf-8")`。`attachAgent` 在 `loadAgent` 之前分别对 `configure.arthasAgent` 和 `configure.arthasCore` 两个 jar 路径做了 URL 编码。**为什么是这两个？** 因为它们是**文件系统绝对路径**，最可能包含空格、中文、括号等在配置串里有歧义的字符。而 `configure.toString()` 里其他字段（端口是数字、IP 是点分十进制）本身不含特殊字符。core 侧编码，agent 侧的 `decodeArg`（`URLDecoder.decode`）解码，形成一对闭环。注意一个细节：真正传给 `loadAgent` 第一个参数的 `arthasAgentPath` 是**编码前**的原始路径（因为 `loadAgent` 第一个参数是 JVM 自己解析的 agent jar 物理路径，不能编码）；被编码的是拼进 options 字符串里、要穿越到 agent 代码里再解码的那份。

**Q6：两个 catch 分支（Non-numeric value / AgentLoadException "0"）是干嘛的？**

这是 Arthas 针对“高低版本 JDK 交叉 attach”踩过的坑做的兼容处理：

- `IOException` 且消息含 `"Non-numeric value found"`：通常发生在**用低版本 JDK attach 高版本 JDK**时。attach 协议返回了新版本才有的非数字响应，低版本解析器读不懂。Arthas 判断这其实往往 attach 已经成功了，于是只 warn 不抛，继续尝试连接。
- `com.sun.tools.attach.AgentLoadException` 且消息为 `"0"`：通常发生在**用高版本 JDK attach 低版本 JDK**时（参见 StackOverflow #54454418）。返回码 "0" 其实代表成功，但被某些 JDK 组合误当成异常抛出。Arthas 同样只 warn 不抛。

**它们为什么存在？** 现实生产环境里 arthas-boot 的 JDK 和目标应用的 JDK 版本经常不一致（比如运维机装了 JDK17，业务跑 JDK8）。这两个 catch 是把“看起来是错误、其实 attach 成功了”的情况识别出来，避免因为一个假错误就中断整个流程，极大提升了跨版本可用性。

**这一步做了什么？** 完成了从 core 进程到目标 JVM 的“跨进程代码注入”。attach 三步（attach → loadAgent → detach）执行完，core 进程随即退出（回到 1.7 里 boot 的 `waitFor` 拿到退出码），而目标 JVM 里 `AgentBootstrap.agentmain` 开始运行——控制权正式进入第三阶段。

---

## 第三阶段：目标 JVM 内 Agent 引导

现在我们站在**进程 C（目标业务 JVM）**里。`loadAgent` 触发了 `arthas-agent.jar` 的 `Agent-Class` 入口。回看 agent 模块的 `pom.xml`，MANIFEST 里声明了：

```xml
<Premain-Class>com.taobao.arthas.agent334.AgentBootstrap</Premain-Class>
<Agent-Class>com.taobao.arthas.agent334.AgentBootstrap</Agent-Class>
<Can-Redefine-Classes>true</Can-Redefine-Classes>
<Can-Retransform-Classes>true</Can-Retransform-Classes>
```

逐项解释：

- `Premain-Class`：如果用 `-javaagent:arthas-agent.jar` 在 JVM 启动时静态挂载，入口是 `premain`。
- `Agent-Class`：如果用 attach + loadAgent 动态挂载（Arthas 的默认方式），入口是 `agentmain`。两者都指向 `AgentBootstrap`。
- `Can-Redefine-Classes=true` / `Can-Retransform-Classes=true`：声明本 agent 需要“重定义类”和“重转换类”的能力。**这两个开关至关重要**——Arthas 的 `watch`/`trace`/`redefine` 等命令全靠 retransform 把监控字节码织入已加载的类。没有这两个 true，`Instrumentation.retransformClasses` 会直接抛异常。

### 3.1 AgentBootstrap.agentmain() —— Agent 入口

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/agent/src/main/java/com/taobao/arthas/agent334/AgentBootstrap.java`

```java
public class AgentBootstrap {
    private static final String ARTHAS_CORE_JAR = "arthas-core.jar";
    private static final String ARTHAS_BOOTSTRAP = "com.taobao.arthas.core.server.ArthasBootstrap";
    private static final String GET_INSTANCE = "getInstance";
    private static final String IS_BIND = "isBind";

    private static PrintStream ps = System.err;
    static {
        try {
            File arthasLogDir = new File(System.getProperty("user.home") + File.separator + "logs" + File.separator
                    + "arthas" + File.separator);
            if (!arthasLogDir.exists()) {
                arthasLogDir.mkdirs();
            }
            if (!arthasLogDir.exists()) {
                // #572
                arthasLogDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "logs" + File.separator
                        + "arthas" + File.separator);
                if (!arthasLogDir.exists()) {
                    arthasLogDir.mkdirs();
                }
            }
            File log = new File(arthasLogDir, "arthas.log");
            if (!log.exists()) {
                log.createNewFile();
            }
            ps = new PrintStream(new FileOutputStream(log, true));
        } catch (Throwable t) {
            t.printStackTrace(ps);
        }
    }

    private static volatile ClassLoader arthasClassLoader;

    public static void premain(String args, Instrumentation inst) {
        main(args, inst);
    }

    public static void agentmain(String args, Instrumentation inst) {
        main(args, inst);
    }

    public static void resetArthasClassLoader() {
        arthasClassLoader = null;
    }
}
```

逐段解释：

- `premain` 和 `agentmain` 都只是转发到私有的 `main(args, inst)`。这样静态挂载和动态挂载走同一套逻辑。
- `arthasClassLoader` 用 `volatile` 修饰且是静态——**它是整个目标 JVM 内 Arthas 的“根类加载器”句柄**，缓存下来用于判重和后续卸载。源码注释解释得很清楚：(1) 全局持有 classloader 用于隔离 Arthas 实现，防止多次 attach 重复初始化；(2) ClassLoader 在 Arthas 停止时会被 reset（即 `resetArthasClassLoader` 把它置 null）；(3) 只要 ClassLoader 没变，`ArthasBootstrap.getInstance` 返回结果就一直一样。
- **`ps` 与静态初始化块**：注意真实源码里 `ps` 的默认值虽然是 `System.err`，但静态块会立刻把它替换成指向磁盘日志文件的 `PrintStream`。日志文件优先落在 `~/logs/arthas/arthas.log`，若创建失败（如 HOME 不可写，见 issue #572）则退化到 `java.io.tmpdir/logs/arthas/`。**它为什么存在？** agent 引导发生在 logback 初始化之前，此时没有任何日志框架可用，但引导过程又极可能出错（找不到 core.jar、端口冲突、版本不匹配），必须把这些原始诊断信息落盘，否则用户排障时无迹可寻。这就是为什么 `bind` 失败时错误提示里写着“Please check $HOME/logs/arthas/arthas.log”。
- `resetArthasClassLoader()`：供 `stop` 卸载 Arthas 时把类加载器句柄清空，让下次 attach 能重新加载全新的 ArthasClassloader，配合第六阶段的内存回收设计。

### 3.2 AgentBootstrap.main() —— 防重复、拆参数、起线程

```java
private static synchronized void main(String args, final Instrumentation inst) {
    // 尝试判断arthas是否已在运行，如果是的话，直接就退出
    try {
        Class.forName("java.arthas.SpyAPI"); // 加载不到会抛异常
        if (SpyAPI.isInited()) {
            ps.println("Arthas server already stared, skip attach.");
            ps.flush();
            return;
        }
    } catch (Throwable e) {
        // ignore
    }
    try {
        ps.println("Arthas server agent start...");
        // 传递的args参数分两个部分:arthasCoreJar路径和agentArgs
        if (args == null) {
            args = "";
        }
        args = decodeArg(args);

        String arthasCoreJar;
        final String agentArgs;
        int index = args.indexOf(';');
        if (index != -1) {
            arthasCoreJar = args.substring(0, index);
            agentArgs = args.substring(index);
        } else {
            arthasCoreJar = "";
            agentArgs = args;
        }

        File arthasCoreJarFile = new File(arthasCoreJar);
        if (!arthasCoreJarFile.exists()) {
            ps.println("Can not find arthas-core jar file from args: " + arthasCoreJarFile);
            // try to find from arthas-agent.jar directory
            CodeSource codeSource = AgentBootstrap.class.getProtectionDomain().getCodeSource();
            if (codeSource != null) {
                try {
                    File arthasAgentJarFile = new File(codeSource.getLocation().toURI().getSchemeSpecificPart());
                    arthasCoreJarFile = new File(arthasAgentJarFile.getParentFile(), ARTHAS_CORE_JAR);
                    ...
                } catch (Throwable e) {
                    e.printStackTrace(ps);
                }
            }
        }
        if (!arthasCoreJarFile.exists()) {
            return;
        }

        // Use a dedicated thread to run the binding logic to prevent possible memory leak. #195
        final ClassLoader agentLoader = getClassLoader(inst, arthasCoreJarFile);

        Thread bindingThread = new Thread() {
            @Override
            public void run() {
                try {
                    bind(inst, agentLoader, agentArgs);
                } catch (Throwable throwable) {
                    throwable.printStackTrace(ps);
                }
            }
        };
        bindingThread.setName("arthas-binding-thread");
        bindingThread.setDaemon(Boolean.TRUE);
        bindingThread.start();
        bindingThread.join();
    } catch (Throwable t) {
        t.printStackTrace(ps);
        try {
            if (ps != System.err) {
                ps.close();
            }
        } catch (Throwable tt) {
            // ignore
        }
        throw new RuntimeException(t);
    }
}

private static String decodeArg(String arg) {
    try {
        return URLDecoder.decode(arg, "utf-8");
    } catch (UnsupportedEncodingException e) {
        return arg;
    }
}
```

逐段解释：

**(1) `synchronized`**：`main` 方法加了 `synchronized`（锁 `AgentBootstrap.class`）。防止用户同时发起多次 attach 导致并发初始化。

**(2) 防重复 attach（在方法最开头）**：注意真实源码里，防重复检查是 `main` 的**第一件事**，甚至在打印 “agent start” 之前。它用 `Class.forName("java.arthas.SpyAPI")`——因为此时 `AgentBootstrap` 自己就 import 了 `java.arthas.SpyAPI`，能直接静态引用 `SpyAPI.isInited()`。如果 SpyAPI 已存在且 `isInited()` 为 true，打印 “Arthas server already stared, skip attach.” 并 `return`。**为什么这么重要？** 如果不判重，第二次 attach 会再创建一套 ShellServer、再绑一次端口（端口冲突）、再注册一套 Transformer（字节码被重复织入），造成混乱甚至内存泄漏。SpyAPI 在 BootstrapClassLoader 里是全 JVM 唯一的，用它当“全局锁标记”最合适。`Class.forName` 加载不到会抛异常（说明是首次 attach，SpyAPI 还没被挂上去），此时 catch 掉继续走初始化流程。

**(3) `decodeArg` —— URL 解码**：`args = decodeArg(args)` 对整个参数串做 `URLDecoder.decode(arg, "utf-8")`。这正是第二阶段 “为什么要对 jar 路径做 URL encode” 的收尾——core 侧编码，agent 侧解码，保证含空格/中文的路径原样穿越。解码失败则原样返回（兜底）。

**(4) 按 `;` 拆参数**：`args.indexOf(';')` 找第一个分号，前半 `arthasCoreJar` 是 core.jar 路径，后半 `agentArgs` 是配置字符串。这正好对应第二阶段 `loadAgent` 时的拼接格式。注意 `agentArgs = args.substring(index)` 是从分号（含分号）开始截取，保留了前导 `;`，交给 core 侧的 `ArthasBootstrap` 进一步解析。

**(5) core.jar 兜底定位**：如果按参数里的路径找不到 core.jar，会通过 `AgentBootstrap.class.getProtectionDomain().getCodeSource()` 反推出 agent.jar 自身位置，再在其同目录找 `arthas-core.jar`。**为什么？** agent.jar 和 core.jar 在发行包里总是同目录，这个兜底能覆盖“参数路径异常但物理布局正常”的场景（比如某些 attach 封装工具没正确传路径）。最终还找不到就 `return` 放弃。

**(6) 创建 ArthasClassloader**：`getClassLoader(inst, arthasCoreJarFile)` → `loadOrDefineClassLoader`，返回缓存的或新建的 `ArthasClassloader`，专门加载 core.jar 里的类，实现与业务应用的类隔离。

**(7) 起 arthas-binding-thread 专用线程执行 bind**：这是一个非常关键的设计，源码注释直接写着 “#195”。`bind` 不是在 attach 的调用线程（即目标 JVM 的 `Attach Listener` 线程）里直接执行，而是新起一个名为 `arthas-binding-thread`、`setDaemon(true)` 的线程，在里面执行，然后 `join()` 等它结束。**为什么？** 详见第六阶段的内存泄漏专题（GitHub issue #195）——在 Attach Listener 线程里执行会让该长寿命线程持有 ArthasClassloader 引用，导致 Arthas 卸载后类无法回收。用专用 daemon 线程执行完就销毁，可避免泄漏。

**(8) 异常时关闭日志流**：catch 块里，如果 `ps` 是文件流（不是 System.err），会 `ps.close()` 刷盘并释放句柄，然后把异常包成 `RuntimeException` 重新抛出——让 core 侧的 `loadAgent` 调用方（第二阶段的 `attachAgent`）感知到失败。

**关于 initSpy 的澄清**：需要特别指出——把 `arthas-spy.jar` 挂到 BootstrapClassLoader 的动作，在当前主干源码里**不是**发生在 `AgentBootstrap.main` 里，而是发生在 core 侧的 `ArthasBootstrap.initSpy()`（见第四阶段 4.3）。`AgentBootstrap` 只负责“建 ArthasClassloader + 反射引导 ArthasBootstrap”，Spy 的挂载由被引导起来的 `ArthasBootstrap` 自己完成。这体现了职责的进一步下沉：agent 只做最小引导，一切初始化都交给 core。

### 3.3 AgentBootstrap.getClassLoader() 与 ArthasClassloader —— 类隔离的基石

```java
private static ClassLoader getClassLoader(Instrumentation inst, File arthasCoreJarFile) throws Throwable {
    return loadOrDefineClassLoader(arthasCoreJarFile);
}

private static ClassLoader loadOrDefineClassLoader(File arthasCoreJarFile) throws Throwable {
    if (arthasClassLoader == null) {
        arthasClassLoader = new ArthasClassloader(new URL[]{ arthasCoreJarFile.toURI().toURL() });
    }
    return arthasClassLoader;
}
```

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/agent/src/main/java/com/taobao/arthas/agent/ArthasClassloader.java`

```java
public class ArthasClassloader extends URLClassLoader {
    public ArthasClassloader(URL[] urls) {
        super(urls, ClassLoader.getSystemClassLoader().getParent());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        final Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass != null) {
            return loadedClass;
        }

        // 已经加载的类不再加载；对 java./sun. 前缀走双亲，其余自己先加载（parent-last）
        if (name != null && (name.startsWith("sun.") || name.startsWith("java."))) {
            return super.loadClass(name, resolve);
        }

        try {
            Class<?> aClass = findClass(name);
            if (resolve) {
                resolveClass(aClass);
            }
            return aClass;
        } catch (Exception e) {
            // ignore，回退到 parent
        }
        return super.loadClass(name, resolve);
    }
}
```

逐段解释：

**(1) 构造方法 `super(urls, ClassLoader.getSystemClassLoader().getParent())`**：

- `urls` 是 core.jar 的路径。
- 关键在第二个参数——parent 被设为 `SystemClassLoader.getParent()`，即 **ExtClassLoader（JDK8）/ PlatformClassLoader（JDK9+）**，而**不是** AppClassLoader。
- **为什么不用 AppClassLoader 当 parent？** 因为如果 parent 是 AppClassLoader，那么按双亲委派，core 里的类会先去 AppClassLoader（业务应用的 classpath）里找。这会导致两个问题：(a) 如果业务应用碰巧也有某个同名类（比如某个版本的 fastjson、netty、slf4j），会加载业务的版本，引发冲突；(b) Arthas 的类会“可见”于业务的类加载体系，污染业务。把 parent 抬高到 ExtClassLoader，就把 AppClassLoader 这一层（及其中的业务类）排除在委派链之外了。

**(2) `loadClass` 重写为 parent-last（打破双亲委派）**：

- 先 `findLoadedClass` 查是否已加载（缓存）。
- 对 `java.`、`sun.` 前缀的类，仍走 `super.loadClass`（即正常双亲委派）。**为什么这些例外？** 因为 JVM 核心类（`java.lang.*` 等）必须由 BootstrapClassLoader 加载，否则会抛 `SecurityException`（Java 不允许自定义加载器定义 `java.` 包下的类），而且核心类全 JVM 必须唯一。
- 其余的类（Arthas 自己的类、它打包进 core.jar 的第三方依赖如 netty/fastjson）**优先用 `findClass` 自己加载**（从 core.jar 里找），找不到才回退 `super.loadClass`。这就是“parent-last / child-first”策略。

**它为什么存在？** 实现**彻底的类隔离**。Arthas 内部大量使用了 netty、fastjson 等第三方库，而业务应用很可能也用了这些库，且版本各异。如果走标准双亲委派，Arthas 用到的会是业务的版本，一旦版本不兼容，Arthas 直接崩溃。parent-last 保证 Arthas 永远用自己打包的那一份依赖，与业务井水不犯河水。

**类比**：ArthasClassloader 就像一个“自带全套工具的上门维修工”。它进你家（目标 JVM）修东西，但坚持用自己工具箱里的螺丝刀（自己 core.jar 里的类），绝不用你家的工具（业务 classpath 的类），除非是水电煤这种全楼公用设施（`java.`/`sun.` 核心类）才用你家的。

### 3.4 AgentBootstrap.bind() —— 反射引导 ArthasBootstrap

```java
private static void bind(Instrumentation inst, ClassLoader agentLoader, String args) throws Throwable {
    Class<?> bootstrapClass = agentLoader.loadClass(ARTHAS_BOOTSTRAP);
    Object bootstrap = bootstrapClass.getMethod(GET_INSTANCE, Instrumentation.class, String.class)
            .invoke(null, inst, args);
    boolean isBind = (Boolean) bootstrapClass.getMethod(IS_BIND).invoke(bootstrap);
    if (!isBind) {
        String errorMsg = "Arthas server port binding failed! Please check $HOME/logs/arthas/arthas.log for more details.";
        ps.println(errorMsg);
        throw new RuntimeException(errorMsg);
    }
    ps.println("Arthas server already bind.");
}
```

逐段解释：

- `agentLoader.loadClass("com.taobao.arthas.core.server.ArthasBootstrap")`：**用 ArthasClassloader 加载 core 里的 `ArthasBootstrap`**。注意这里必须用 `agentLoader`（ArthasClassloader），不能用 `Class.forName`（那会用 agent 自己的 AppClassLoader，找不到 core.jar 里的类）。
- `getMethod("getInstance", Instrumentation.class, String.class).invoke(null, inst, args)`：反射调用 `ArthasBootstrap.getInstance(inst, args)` 这个静态工厂方法，触发 ArthasBootstrap 的单例构造（真正的初始化在这里发生，见第四阶段）。
- `getMethod("isBind").invoke(bootstrap)`：反射调 `isBind()` 检查端口是否绑定成功。
- 如果 `isBind` 为 false，说明端口没绑上（比如端口被占用），抛异常并提示看 arthas.log。

**为什么全用反射？** 因为 agent 模块**在编译期不依赖 core 模块**（回看模块架构表，agent 对 core 只有“字符串类名”级软依赖）。agent 自己是被 AppClassLoader 加载的、极小的引导器；core 里的一切都要通过 ArthasClassloader 隔离加载。反射是跨越这道类加载器边界的唯一桥梁——你不能在 agent 的代码里直接 `import com.taobao.arthas.core.server.ArthasBootstrap`，否则 AppClassLoader 会尝试加载它而失败（core.jar 不在 AppClassLoader 的 classpath 里）。

**这一步做了什么？** 完成了从“agent 引导器”到“core 服务端主体”的交接。`getInstance` 一旦返回，ArthasBootstrap 就已经初始化完毕、端口已监听。控制权进入第四阶段。

---

## 第四阶段：ArthasBootstrap 初始化与 bind

`ArthasBootstrap` 是 Arthas 在目标 JVM 内的“总装车间”。它的构造方法把所有子系统按精确顺序拼装起来。

### 4.1 ArthasBootstrap 单例与 getInstance —— 保证全 JVM 唯一

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/server/ArthasBootstrap.java`

```java
public class ArthasBootstrap {
    private static ArthasBootstrap arthasBootstrap;

    private Instrumentation instrumentation;
    private AtomicBoolean isBindRef = new AtomicBoolean(false);
    private ShellServer shellServer;
    private TransformerManager transformerManager;
    private SessionManager sessionManager;
    private HttpApiHandler httpApiHandler;
    private TunnelClient tunnelClient;

    public static ArthasBootstrap getInstance(Instrumentation instrumentation, String args) throws Throwable {
        if (arthasBootstrap == null) {
            synchronized (ArthasBootstrap.class) {
                if (arthasBootstrap == null) {
                    // 解析 args（configure 字符串）成 Map
                    Map<String, String> argsMap = ...;
                    arthasBootstrap = new ArthasBootstrap(instrumentation, argsMap);
                }
            }
        }
        return arthasBootstrap;
    }
}
```

逐段解释：

- 经典的 **双重检查锁（DCL）单例**。`getInstance` 保证一个 JVM 里只有一个 `ArthasBootstrap`。
- 第一次调用时才 `new ArthasBootstrap(...)`，真正的初始化全在构造方法里。
- `isBindRef` 是一个 `AtomicBoolean`，标记端口是否已绑定，供 `isBind()` 查询。

**它为什么存在？** 和第三阶段的防重复 attach 呼应——单例是“一个 JVM 只能有一个 Arthas Server”这条铁律在 core 侧的第二道保险。

### 4.2 ArthasBootstrap 构造方法 —— 七步初始化

```java
private ArthasBootstrap(Instrumentation instrumentation, Map<String, String> args) throws Throwable {
    this.instrumentation = instrumentation;

    initFastjson();
    // 1. initSpy()
    initSpy();
    // 2. ArthasEnvironment
    initArthasEnvironment(args);

    String outputPathStr = configure.getOutputPath();
    ...
    // 3. init logger
    loggerContext = LogUtil.initLogger(arthasEnvironment);

    // 4. 增强 ClassLoader
    enhanceClassLoader();
    // 5. init beans
    initBeans();
    // 6. start agent server
    bind(configure);

    executorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(r, "arthas-command-execute");
            t.setDaemon(true);
            return t;
        }
    });

    // 7. TransformerManager
    transformerManager = new TransformerManager(instrumentation);

    Runtime.getRuntime().addShutdownHook(shutdown);
}
```

逐步解释这七步（顺序极其讲究）：

**initFastjson()**：预热/配置 fastjson。Arthas 用 fastjson 做命令结果的 JSON 序列化（HTTP API）。这里做一些全局设置（如关闭某些自动类型、设置日期格式），必须在任何序列化发生前完成。

**(1) initSpy()**：把 `arthas-spy.jar` 里的 `SpyAPI` 挂到 BootstrapClassLoader（见 4.3）。**必须最先做**——因为后续所有字节码增强都要回调 SpyAPI，它必须先就位。

**(2) initArthasEnvironment(args)**：加载配置。把 `getInstance` 传进来的 args（源自 core 的 configure 字符串）以及 `arthas.properties`、系统属性、环境变量融合成一个 `ArthasEnvironment`（类似 Spring 的 Environment），再据此构造出 `Configure` 对象。此时 telnetPort/httpPort/ip/username/password 等最终配置全部确定。

**(3) initLogger()**：`LogUtil.initLogger(arthasEnvironment)` 初始化 logback。**必须在 environment 之后**——因为日志输出路径（`$HOME/logs/arthas/`）等配置来自 environment。在此之前的日志只能用 stderr（回看 agent 阶段的 `ps`）。

**(4) enhanceClassLoader()**：增强/记录目标 JVM 里的类加载器（见 4.4）。

**(5) initBeans()**：初始化一些内部“bean”，如 `ResultViewResolver`（把命令结果模型渲染成视图）、历史命令管理器等。

**(6) bind(configure)**：**核心中的核心**——启动网络服务、注册命令、监听端口（见 4.5）。

**(7) TransformerManager**：`new TransformerManager(instrumentation)` 创建字节码转换器管理中心，并 `inst.addTransformer(...)`。它是 watch/trace/monitor 等命令织入字节码的总入口。

最后 `addShutdownHook`：注册 JVM 关闭钩子，保证进程退出时能优雅清理（解绑端口、还原被增强的类、销毁 SpyAPI）。

**顺序为什么是这样？** 一句话：**依赖在前，被依赖在后**。Spy（一切增强的回调目标）→ 配置（一切的输入）→ 日志（后续要用）→ 类加载器增强 → 内部 bean → 网络服务 → 转换器。任何一步依赖前面步骤的产物，顺序不能乱。

### 4.3 initSpy() —— 把 SpyAPI 塞进 BootstrapClassLoader

```java
private void initSpy() throws Throwable {
    // 将 Spy 添加到 BootstrapClassLoader
    Class<?> spyClass = null;
    ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
    if (parent != null) {
        try {
            spyClass = parent.loadClass("java.arthas.SpyAPI");
        } catch (Throwable e) {
            // ignore
        }
    }
    if (spyClass == null) {
        CodeSource codeSource = ArthasBootstrap.class.getProtectionDomain().getCodeSource();
        if (codeSource != null) {
            File arthasCoreJarFile = new File(codeSource.getLocation().toURI().getSchemeSpecificPart());
            File spyJarFile = new File(arthasCoreJarFile.getParentFile(), ARTHAS_SPY_JAR);
            instrumentation.appendToBootstrapClassLoaderSearch(new JarFile(spyJarFile));
        } else {
            throw new IllegalStateException("can not find arthas-spy.jar");
        }
    }
}
```

逐段解释：

- 先尝试用 `SystemClassLoader.getParent()` 加载 `java.arthas.SpyAPI`，看它是否已经在 BootstrapClassLoader 可见（比如 agent 阶段已经 append 过，或上次没清干净）。
- 如果没找到，就通过 `getProtectionDomain().getCodeSource()` 反推出 core.jar 的位置，进而找到同目录的 `arthas-spy.jar`，用 `instrumentation.appendToBootstrapClassLoaderSearch(...)` 把它追加到 BootstrapClassLoader 的搜索路径。

**为什么 SpyAPI 必须放在 BootstrapClassLoader？**

这是 Arthas 设计里最精妙的一环。考虑 `watch com.业务.Foo bar` 这个命令：Arthas 会 retransform 业务类 `Foo`，往 `bar` 方法的入口/出口插入 `SpyAPI.atEnter(...)` / `SpyAPI.atExit(...)` 的调用字节码。而 `Foo` 是被**业务的某个 ClassLoader**加载的。被织入的这行 `SpyAPI.xxx()` 要能被 `Foo` 的类加载器解析到 `SpyAPI` 这个类——**根据双亲委派，任何类加载器最终都能向上委派到 BootstrapClassLoader**。所以只要 `SpyAPI` 在 BootstrapClassLoader 里，无论业务类被哪个 ClassLoader 加载，都能看到同一个 `SpyAPI`。

如果 SpyAPI 放在 ArthasClassloader 里会怎样？业务类 `Foo` 的类加载器根本不认识 ArthasClassloader（它们没有委派关系），织入的 `SpyAPI.atEnter` 会抛 `NoClassDefFoundError`——增强直接失效。

**类比**：SpyAPI 就像整栋大楼的“公共广播系统”，必须装在所有楼层都能接入的“大楼主干线路”（BootstrapClassLoader）上。如果只装在某一间办公室（ArthasClassloader），其他房间（业务 ClassLoader）根本接不进来。

**为什么包名是 `java.arthas`？** 因为 `appendToBootstrapClassLoaderSearch` 加进去的类，其包名不能和 JVM 已有核心包精确冲突，但放在 `java.arthas` 子包下能明确表达“这是要挂到最底层、全局可见”的意图。（注意：普通自定义 ClassLoader 不允许 define `java.` 包的类，但 BootstrapClassLoader 通过 append jar 的方式是允许的。）

### 4.4 enhanceClassLoader() —— 解决 ClassLoader 找不到 Spy 的问题

`enhanceClassLoader()` 处理的是一类特殊场景：某些框架（如 OSGi、部分 web 容器、tomcat 的 WebappClassLoader）为了隔离，**自己重写了 `loadClass` 并打破了双亲委派**，甚至会主动屏蔽 `java.` 之外的某些包。极端情况下，这类 ClassLoader 可能连向上委派到 BootstrapClassLoader 找 `SpyAPI` 都做不到（比如它把 `SpyAPI` 的加载也拦截了）。

`enhanceClassLoader` 的做法是：遍历/记录目标 JVM 中所有已加载的 ClassLoader，对那些配置了需要增强的类加载器，用字节码手段确保它们能正确加载到 `SpyAPI`（例如在其 `loadClass` 里对 `java.arthas.SpyAPI` 特殊放行）。默认情况下这一步影响很小，但它是 Arthas 能在复杂容器环境里稳定工作的兜底保障。

**它为什么存在？** 为了应对“不守规矩”的类加载器。标准双亲委派下 SpyAPI 全局可见，但现实世界的容器五花八门，enhanceClassLoader 是对这些边缘情况的加固。

### 4.5 bind() —— 网络服务与命令系统总装

这是整个初始化的高潮。逐块拆解：

```java
private void bind(Configure configure) throws Throwable {
    long start = System.currentTimeMillis();

    if (!isBindRef.compareAndSet(false, true)) {
        throw new IllegalStateException("already bind");
    }

    try {
        // (a) 随机端口兜底 + TunnelClient
        if (configure.getTunnelServer() != null) {
            tunnelClient = new TunnelClient();
            tunnelClient.setAppName(configure.getAppName());
            tunnelClient.setId(configure.getAgentId());
            tunnelClient.setTunnelServerUrl(configure.getTunnelServer());
            ChannelFuture channelFuture = tunnelClient.start();
            channelFuture.await(10, TimeUnit.SECONDS);
        }
```

**(a) TunnelClient（可选）**：如果配置了 `tunnel-server`，启动隧道客户端，通过 WebSocket 反向连接到 tunnel-server，把自己注册进去。这样即使 Agent 在内网、没有公网 IP，运维也能通过 tunnel-server 统一接入。`await(10s)` 等待连接建立。没配 tunnel-server 就跳过。

```java
        // (b) ShellServerOptions
        ShellServerOptions options = new ShellServerOptions()
                .setInstrumentation(instrumentation)
                .setPid(PidUtils.currentLongPid())
                .setWelcomeMessage(ArthasBanner.welcome());
        if (configure.getSessionTimeout() != null) {
            options.setSessionTimeout(configure.getSessionTimeout() * 1000);
        }
```

**(b) ShellServerOptions**：装配 Shell 服务器的配置对象。塞入 `instrumentation`（命令要用它做增强）、当前进程 pid、欢迎横幅（那个 Arthas 的 ASCII logo）、会话超时时间。

```java
        // (c) 安全认证
        this.securityAuthenticator = new SecurityAuthenticatorImpl(configure.getUsername(), configure.getPassword());

        // (d) ShellServer
        shellServer = new ShellServerImpl(options);

        // (e) 内置命令包
        List<String> disabledCommands = new ArrayList<String>();
        if (configure.getDisabledCommands() != null) {
            String[] strings = StringUtils.tokenizeToStringArray(configure.getDisabledCommands(), ",");
            if (strings != null) {
                disabledCommands.addAll(Arrays.asList(strings));
            }
        }
        BuiltinCommandPack builtinCommands = new BuiltinCommandPack(disabledCommands);
```

**(c) SecurityAuthenticatorImpl**：安全认证器，持有用户名/密码。见 4.7 安全机制。

**(d) ShellServerImpl**：Shell 服务器实体，管理会话、命令解析、job 调度。

**(e) BuiltinCommandPack**：内置命令包。它是一个 `CommandResolver`，内部注册了 `dashboard`/`thread`/`watch`/`trace`/`jad`/`sc`/`sm`/`redefine` 等全部内置命令。`disabledCommands` 支持在启动时禁用某些危险命令。

```java
        // (f) 注册 TermServer（telnet + http）
        List<CommandResolver> resolvers = new ArrayList<CommandResolver>();
        appendInternalCommands(resolvers);
        // telnet
        if (configure.getTelnetPort() != null && configure.getTelnetPort() > 0) {
            shellServer.registerTermServer(new HttpTelnetTermServer(
                    configure.getIp(), configure.getTelnetPort(), options.getConnectionTimeout(),
                    workerGroup, httpSessionManager));
        }
        // http
        if (configure.getHttpPort() != null && configure.getHttpPort() > 0) {
            shellServer.registerTermServer(new HttpTermServer(
                    configure.getIp(), configure.getHttpPort(), options.getConnectionTimeout(),
                    workerGroup, httpSessionManager));
        }

        // (g) 注册命令解析器
        for (CommandResolver resolver : resolvers) {
            shellServer.registerCommandResolver(resolver);
        }
        shellServer.registerCommandResolver(builtinCommands);

        // (h) 开始监听
        shellServer.listen(new BindHandler(isBindRef));

        if (!isBind()) {
            throw new IllegalStateException("Arthas failed to bind telnet or http port! ");
        }
```

**(f) registerTermServer**：注册两个 TermServer。
- `HttpTelnetTermServer`（telnet，3658）：处理命令行客户端的 telnet 连接。注意名字里有 Http——它其实是基于 Netty 的、能同时处理 telnet 协议的服务器。
- `HttpTermServer`（http，8563）：处理 Web Console 和 HTTP API。
- 只有端口 > 0 才注册，支持只开其中一个。

**(g) registerCommandResolver**：把命令解析器注册进 shellServer。`ShellServerImpl.registerCommandResolver` 用 `resolvers.add(0, ...)` 插到最前，实现优先级覆盖。builtinCommands 最后注册（优先级最低，作为兜底）。

**(h) shellServer.listen(...)**：**真正启动 Netty 监听**（见 4.6）。传入 `BindHandler`，绑定成功/失败会回调它更新 `isBindRef`。listen 完检查 `isBind()`，没绑上就抛异常——这个异常会一路传回第三阶段 agent 的 `bind`，最终让 attach 报错。

```java
        // (i) Session 与 HTTP API
        this.httpSessionManager = new HttpSessionManager();
        sessionManager = new SessionManagerImpl(options, shellServer.getCommandManager(),
                shellServer.getJobController());
        httpApiHandler = new HttpApiHandler(historyManager, sessionManager);

        // (j) 间谍类初始化
        SpyAPI.init();

        logger.info("as-server started in {} ms", System.currentTimeMillis() - start);
    } catch (Throwable e) {
        logger.error("Error during bind to port ...", e);
        if (shellServer != null) {
            shellServer.close();
        }
        throw e;
    }
}
```

**(i) SessionManagerImpl / HttpApiHandler**：会话管理器（管理 telnet/http 会话的生命周期、超时回收）和 HTTP API 处理器（把 `/api` 请求路由到命令、返回 JSON）。

**(j) SpyAPI.init()**：把 `SpyAPI.INITED` 置为 true（回看 spy 源码 `init()` 只是 `INITED = true`）。**这一步是“Arthas 已就绪”的正式标志**——它同时也是第三阶段防重复 attach 判断 `isInited()` 的依据。放在最后，确保只有当端口都绑好、一切就绪后，才对外宣告“已初始化”。

**bind 各组件的启动顺序为什么是这样？** TunnelClient（外部连通性，可选，先建立）→ options/authenticator/shellServer（服务端骨架）→ 命令包与解析器（能力）→ TermServer 注册 + listen（对外端口）→ session/httpApi（会话层）→ SpyAPI.init（就绪标志）。核心原则依然是：**先把内部装配好，再开对外端口，最后才宣告就绪**。绝不能先 `SpyAPI.init()` 再 listen——否则会出现“宣告就绪了但端口还没绑好”的窗口期，导致防重复 attach 误判、客户端连接失败。

### 4.6 ShellServerImpl.listen() —— 启动 Netty 服务

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/shell/impl/ShellServerImpl.java`

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

逐段解释：

- `toStart = termServers`：拿到之前 `registerTermServer` 注册的所有 TermServer（telnet + http）。
- `count` 记录待启动的 TermServer 数量，用于异步计数——每个 TermServer 监听成功回调一次，全部成功后才认为整体 listen 成功。
- 对每个 TermServer：
  - `termServer.termHandler(new TermServerTermHandler(this))`：设置“当有新终端连接进来时”的处理器。新连接到来时，`TermServerTermHandler` 会回调 `ShellServerImpl.handleTerm(term)`，进而 `createShell` + `session.readline()`，开始一个交互会话。
  - `termServer.listen(handler)`：**每个 TermServer 内部用 Netty 启动一个 ServerBootstrap，bind 到对应端口（3658/8563）**。这一步真正打开了 TCP 监听套接字。
- `handler`（`TermServerListenHandler`）：所有 TermServer 都成功后，回调最外层的 `BindHandler`，把 `isBindRef` 置 true。

**handleTerm 里发生了什么（连接建立后）？** 回看源码：

```java
public void handleTerm(Term term) {
    synchronized (this) {
        if (closed) { term.close(); return; }
    }
    ShellImpl session = createShell(term);
    tryUpdateWelcomeMessage();
    session.setWelcome(welcomeMessage);
    session.closedFuture.setHandler(new SessionClosedHandler(this, session));
    session.init();
    sessions.put(session.id, session);
    session.readline();
}
```

- 每个新连接创建一个 `ShellImpl` 会话，设置欢迎语（那个 ASCII banner），注册关闭回调，`init` 后放进 `sessions` map，最后 `readline()` 开始读用户命令。这就是你连上后看到 banner 和提示符的地方。

**它为什么存在？** listen 把“注册好的端口”真正变成“在监听的端口”，并建立起“连接→会话”的映射机制。它是 Netty 网络层与 Arthas Shell 逻辑层的接缝。

### 4.7 安全认证机制 —— 0.0.0.0 监听时强制密码

`SecurityAuthenticatorImpl` 持有 username/password。关键的安全策略在于：**当 Arthas 监听地址不是 127.0.0.1（即对外暴露，如 0.0.0.0）时，如果没有配置密码，Arthas 会拒绝启动或强制生成随机密码**。

设计意图非常清晰：

| 监听地址 | 是否需要密码 | 后果 |
| --- | --- | --- |
| 127.0.0.1（默认） | 不强制 | 只有本机能连，风险低 |
| 0.0.0.0 / 具体外网 IP | 强制（无则报错/随机生成） | 否则任何能访问该端口的人都能进入 Arthas，等于把 JVM 的“上帝权限”暴露到网络上 |

**它为什么存在？** Arthas 能力极强——可以看内存、改字节码、执行任意表达式（`ognl`/`vmtool`）。一旦无认证地暴露在公网，等同于 RCE（远程代码执行）漏洞。强制密码是把这个“核弹级工具”约束在安全边界内的最后防线。这也是为什么默认 `target-ip` 是 `127.0.0.1`——安全优先。

---

## 第五阶段：客户端连接（TelnetConsole）

回到进程 A（boot 进程），它在 1.8 反射调用了 `TelnetConsole.process`。现在细看客户端如何连上第四阶段起好的 telnet 服务。

### 5.1 TelnetConsole.main() 与 process() —— 两个入口

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/client/src/main/java/com/taobao/arthas/client/TelnetConsole.java`

```java
public static void main(String[] args) throws Exception {
    try {
        int status = process(args, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.exit(STATUS_OK);
            }
        });
        System.exit(status);
    } catch (Throwable e) {
        e.printStackTrace();
        CLI cli = CLIConfigurator.define(TelnetConsole.class);
        System.out.println(usage(cli));
        System.exit(STATUS_ERROR);
    }
}

public static int process(String[] args) throws IOException, InterruptedException {
    return process(args, null);
}
```

逐段解释：

- `main` 是独立运行 `arthas-client.jar` 时的入口，内部会 `System.exit`，并传入一个 Ctrl+D（EOT）回调也调 `System.exit`。
- `process(String[])` 是**给 boot 复用的入口**（回看 1.8）。源码注释明确：“process()函数提供给arthas-boot使用，内部不能调用System.exit()”。boot 复用它时不希望 client 退出把 boot 也带走。
- 两者最终都汇聚到 `process(String[] args, ActionListener eotEventCallback)`。

### 5.2 process() 主流程 —— jline + TelnetClient

```java
public static int process(String[] args, ActionListener eotEventCallback) throws IOException {
    // support mingw/cygw jline color
    if (OSUtils.isCygwinOrMinGW()) {
        System.setProperty("jline.terminal", System.getProperty("jline.terminal", "jline.UnixTerminal"));
    }

    TelnetConsole telnetConsole = new TelnetConsole();
    CLI cli = CLIConfigurator.define(TelnetConsole.class);
    CommandLine commandLine = cli.parse(Arrays.asList(args));
    CLIConfigurator.inject(commandLine, telnetConsole);

    if (telnetConsole.isHelp()) {
        System.out.println(usage(cli));
        return STATUS_OK;
    }

    // Try to read cmds
    List<String> cmds = new ArrayList<String>();
    if (telnetConsole.getCommand() != null) {
        for (String c : telnetConsole.getCommand().split(";")) {
            cmds.add(c.trim());
        }
    } else if (telnetConsole.getBatchFile() != null) {
        File file = new File(telnetConsole.getBatchFile());
        if (!file.exists()) {
            throw new IllegalArgumentException("batch file do not exist: " + telnetConsole.getBatchFile());
        } else {
            cmds.addAll(readLines(file));
        }
    }

    final ConsoleReader consoleReader = new ConsoleReader(System.in, System.out);
    consoleReader.setHandleUserInterrupt(true);
    Terminal terminal = consoleReader.getTerminal();
    terminal.disableInterruptCharacter();
    if (terminal instanceof UnixTerminal) {
        ((UnixTerminal) terminal).disableLitteralNextCharacter();
    }
    ...
}
```

逐段解释：

- **Cygwin/MinGW 处理**：Windows 的 Git Bash 等环境下，强制用 `jline.UnixTerminal` 才能正确显示颜色。
- **参数解析**：同样用 `com.taobao.middleware.cli` 解析 targetIp、port、-c、-f 等。
- **`-c` 单命令**：`getCommand().split(";")` 按分号拆多个命令。用于 `arthas-boot -c 'dashboard -n 1'` 这种一次性执行。
- **`-f` 批处理文件**：`readLines(file)` 读取批处理脚本（每行一个命令），用于 `-f batch.as`。
- **ConsoleReader（jline）**：交互式终端的核心。`setHandleUserInterrupt(true)` 让它自己处理中断。
- **`disableInterruptCharacter()` + `disableLitteralNextCharacter()`**：关闭终端默认的 Ctrl+C 行为，因为 Arthas 要**自己捕获 Ctrl+C** 转发给服务端（中断正在运行的命令，而不是杀掉客户端）。

### 5.3 窗口大小同步、Ctrl+C / Ctrl+D 处理

```java
    // send init terminal size
    TelnetOptionHandler sizeOpt = new WindowSizeOptionHandler(width, height, true, true, false, false);
    telnet.addOptionHandler(sizeOpt);

    // ctrl + c event callback
    consoleReader.getKeys().bind(Character.toString((char) CTRL_C), new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent e) {
            try {
                consoleReader.getCursorBuffer().clear(); // clear current line
                telnet.getOutputStream().write(CTRL_C);
                telnet.getOutputStream().flush();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    });

    // ctrl + d event call back
    consoleReader.getKeys().bind(Character.toString(KeyMap.CTRL_D), eotEventCallback);

    try {
        telnet.connect(telnetConsole.getTargetIp(), telnetConsole.getPort());
    } catch (IOException e) {
        System.out.println("Connect to telnet server error: " + ...);
        throw e;
    }
```

逐段解释：

- **窗口大小同步**：`WindowSizeOptionHandler` 通过 telnet 的 NAWS（Negotiate About Window Size）选项，把客户端终端的宽高告诉服务端。**为什么重要？** 服务端渲染 `dashboard`、表格、进度条时要知道屏幕多宽，否则会换行错乱。当你拖动终端窗口改变大小时，这个机制会同步过去。
- **Ctrl+C 处理**：捕获 Ctrl+C（0x03），清空当前输入行，然后把 `CTRL_C` 字节**发给服务端**。服务端收到后中断当前正在跑的 job（如一个还没结束的 `trace`），但**不断开连接**。这就是为什么在 Arthas 里按 Ctrl+C 是“停止当前命令”而不是“退出 Arthas”。
- **Ctrl+D 处理**：绑定 `eotEventCallback`。独立运行时这个回调是 `System.exit`（Ctrl+D 退出客户端）。
- **`telnet.connect(targetIp, port)`**：真正发起 TCP 连接到 `127.0.0.1:3658`（第四阶段起好的 HttpTelnetTermServer）。连不上就报错。

### 5.4 交互模式 vs 批处理模式

```java
    if (cmds.isEmpty()) {
        // 交互模式：把 telnet 的输入输出与本地终端双向桥接
        IOUtil.readWrite(telnet.getInputStream(), telnet.getOutputStream(),
                consoleReader.getInput(), consoleReader.getOutput());
    } else {
        // 批处理模式
        try {
            return batchModeRun(telnet, cmds, telnetConsole.getExecutionTimeout());
        } finally {
            telnet.disconnect();
        }
    }
```

**交互模式**（`cmds` 为空，即没给 -c/-f）：`IOUtil.readWrite` 建立本地终端与远端 telnet 的双向数据泵——你敲的每个字符发给服务端，服务端回的每个字符显示在你终端上。这时你就看到了 `[arthas@<pid>]$` 提示符，**Shell 正式就绪，用户可以开始敲命令**。

**批处理模式**（有 -c/-f）：`batchModeRun` 逐条把命令发给服务端。它有个精妙的同步机制——通过检测服务端回显里的 `[arthas@` 提示符（`PROMPT`）来判断“上一条命令执行完了，可以发下一条”：

```java
int index = line.indexOf(PROMPT);
if (index >= 0) {
    line.delete(0, index + PROMPT.length());
    receviedPromptQueue.put("");   // 收到提示符 → 通知主线程可发下一条
}
...
outputStream.write((command + " | plaintext\n").getBytes());  // 命令加 | plaintext 去掉 ANSI 颜色
...
receviedPromptQueue.take();
outputStream.write("quit\n".getBytes());  // 全部执行完发 quit
```

- 每条命令后追加 `| plaintext`，让服务端输出纯文本（去掉 ANSI 颜色码），便于脚本处理/重定向。
- 用一个容量为 1 的阻塞队列 `receviedPromptQueue` 做“命令-提示符”握手：发一条命令 → 等到下一个提示符出现 → 再发下一条。这样保证命令严格串行、不会因为服务端还没处理完就把下一条塞进去。
- `executionTimeout` 控制批处理总超时，超时返回 `STATUS_EXEC_TIMEOUT`(100)。
- 全部执行完发 `quit` 优雅退出。

### 5.5 terminal.restore() —— 别把用户终端搞坏

```java
    } finally {
        // reset terminal setting, fix https://github.com/alibaba/arthas/issues/1412
        try {
            terminal.restore();
        } catch (Throwable e) {
            System.out.println("Restore terminal settings failure: " + e.getMessage());
        }
    }
```

**它为什么存在？** 前面 `disableInterruptCharacter()` 等操作修改了用户终端的 tty 设置（raw mode）。如果 Arthas 退出时不还原，用户的 shell 会“坏掉”（比如 Ctrl+C 失灵、看不到输入回显）。`terminal.restore()` 在 finally 里保证无论正常退出还是异常，都把终端还原成进入 Arthas 之前的样子。这对应了一个真实的历史 bug（issue #1412）。

---

## 第六阶段：关键设计问题深入分析

### 6.1 为什么 Arthas 要分 boot / core / agent 三个模块？

**核心答案：职责分离 + 环境隔离 + 最小侵入。**

| 模块 | 运行环境 | 如果合并会怎样 |
| --- | --- | --- |
| boot | 用户命令行 JVM | 若把 core/agent 逻辑塞进 boot，boot jar 会变得巨大，且无法自举下载；boot 还得直接持有 attach 逻辑，环境不可控 |
| core（attach 侧） | 与目标 JVM 同版本的临时 JVM | attach 需要和目标 JVM 版本匹配的运行环境，独立进程才能保证这一点 |
| core（服务端侧） | 目标 JVM 的 ArthasClassloader 内 | 重量级逻辑（netty/fastjson/命令）必须类隔离，不能污染业务 |
| agent | 目标 JVM 的 AppClassLoader | agent 必须极小、只做引导，作为“进入目标 JVM 的登陆艇” |

**如果去掉这种划分（做成一个大 jar）会怎样？** (1) 无法做类隔离——Arthas 的 netty 会和业务的 netty 冲突；(2) attach 环境无法保证和目标 JVM 一致；(3) 用户下载的入口 jar 会非常臃肿；(4) agent 逻辑和 core 逻辑纠缠，无法通过反射边界实现干净的隔离。

### 6.2 为什么不直接 attach，而要先拉起 core 进程？

**核心答案：attach 环境必须与目标 JVM 兼容，而 boot 进程的环境不可控。**

`VirtualMachine.attach` 依赖 `com.sun.tools.attach`，在不同 JDK 里位置和可用性不同（JDK8 在 tools.jar，JDK9+ 在模块系统里）。boot 进程可能被用户用任意 java 启动（甚至是 JRE，没有 attach 能力）。而 core 进程是由 `ProcessUtils.startArthasCore` 用**精心挑选的 java**（优先 JAVA_HOME 指向的 JDK）启动的，能保证 attach API 可用、且版本尽量贴近目标 JVM。

**如果去掉 core 进程、让 boot 直接 attach 会怎样？** 在“boot 用 JRE 启动”或“boot 的 java 版本与目标 JVM 差异大”时，attach 会直接失败。中间加一层 core 进程，就把“attach 兼容性”这个脏活隔离到一个可控进程里。

### 6.3 ArthasClassloader 为什么要用 parent-last 策略？

**核心答案：防止 Arthas 的第三方依赖被业务应用的同名类“劫持”。**

标准双亲委派下，`ArthasClassloader` 要加载 `com.alibaba.fastjson.JSON` 时会先问 parent，一路问到 AppClassLoader——如果业务应用也有 fastjson（版本还不一样），就会加载业务的版本。Arthas 用到的 API 在业务那个版本里可能不存在或行为不同，直接崩溃。

parent-last 让 ArthasClassloader **优先自己加载**（从 core.jar 里找），只有 `java.`/`sun.` 这种必须唯一的核心类才委派上去。这样 Arthas 永远用自己打包的依赖版本。

**如果去掉 parent-last（用标准双亲委派）会怎样？** 只要业务应用和 Arthas 有任何一个同名不同版本的依赖（netty/fastjson/slf4j/asm 是重灾区），Arthas 就可能在启动或运行时抛 `NoSuchMethodError`/`ClassCastException`。parent-last 是 Arthas 能在千奇百怪的线上应用里稳定运行的根本保证。

### 6.4 SpyAPI 为什么要放在 BootstrapClassLoader？

**核心答案：让任意 ClassLoader 加载的业务类，都能通过双亲委派看到同一个 SpyAPI。**

（4.3 已详述）被增强的业务类字节码里插了 `SpyAPI.atEnter(...)` 调用。业务类可能被任意 ClassLoader 加载。只有 SpyAPI 在委派链最顶端（BootstrapClassLoader），所有类加载器才能一致地解析到它。

**如果去掉、把 SpyAPI 放在 ArthasClassloader 里会怎样？** 被增强的业务类找不到 SpyAPI（它们的类加载器不认识 ArthasClassloader），抛 `NoClassDefFoundError`，watch/trace/monitor 全部失效。此外，SpyAPI 放 Bootstrap 还保证了它是全 JVM 唯一实例，`INITED` 标志、`spyInstance` 引用全局一致，这也是防重复 attach 判断的基础。

### 6.5 为什么 bind 要在专用线程（arthas-binding-thread）中执行？

**核心答案：避免 Attach Listener 线程持有 ArthasClassloader 引用，导致 Arthas 卸载后类无法 GC（内存泄漏，issue #195）。**

`agentmain` 是被目标 JVM 的 `Attach Listener` 线程调用的。这个线程是 JVM 内建的、长期存活的线程。如果直接在它里面执行 `bind`：

1. `bind` 过程中会通过 ArthasClassloader 加载大量 core 类，这些操作会把 `Attach Listener` 线程的**上下文类加载器（TCCL）**或线程内部的某些 ThreadLocal 关联到 ArthasClassloader。
2. 当用户执行 `stop` 卸载 Arthas 时，本应让 ArthasClassloader 及其加载的所有类被 GC 回收。
3. 但只要还有一个存活线程（Attach Listener）间接持有对 ArthasClassloader 的强引用，整个 ArthasClassloader 及其加载的几百个类就**永远无法回收**——这就是 metaspace/perm 区的内存泄漏。

Arthas 的解法：新起一个 `arthas-binding-thread`、`setDaemon(true)`，在里面执行 `bind`，执行完 `join()` 等它结束。这个线程随即死亡被回收，任何在它上面建立的类加载器引用也随之释放，不会残留在长寿命的 Attach Listener 线程上。

**如果去掉专用线程、直接在 agentmain 里 bind 会怎样？** 每次 attach + stop 循环都会泄漏一个 ArthasClassloader 及其全部类。反复 attach/stop（比如自动化脚本里）会导致 metaspace 持续增长，最终 `OutOfMemoryError: Metaspace`。这正是 GitHub issue #195 报告的真实问题。用短命 daemon 线程执行是一个成本极低、效果彻底的修复。

---

## 第七阶段：贯穿全程的三个横切主题

前六个阶段是“时间线”视角。但有三个主题横跨多个阶段、多个进程，单看某一阶段无法理解全貌。本阶段把它们抽出来纵向讲透。

### 7.1 横切主题一：com.taobao.middleware.cli —— 自研的极简 CLI 框架

在 boot、core、client 三个模块里，我们反复看到同一套 API：`CLIConfigurator.define`、`CLIs.create`、`cli.parse`、`CLIConfigurator.inject`、`@Option`、`@Argument`。它们都来自同一个包 `com.taobao.middleware.cli`。理解它，就理解了 Arthas 所有命令行解析的底层机制。

**它有两套用法：注解式与编程式。**

注解式（boot 的 `Bootstrap`、client 的 `TelnetConsole` 用）：

```java
// 1. 反射扫描类上的 @Option/@Argument，构造 CLI 描述
CLI cli = CLIConfigurator.define(Bootstrap.class);
// 2. 把原始 args 解析成结构化 CommandLine
CommandLine commandLine = cli.parse(Arrays.asList(args));
// 3. 把解析结果反射注入到实例的 setter
CLIConfigurator.inject(commandLine, bootstrap);
```

编程式（core 的 `Arthas.parse` 用）：

```java
Option pid = new TypedOption<Long>().setType(Long.class).setShortName("pid").setRequired(true);
CLI cli = CLIs.create("arthas").addOption(pid)...;
CommandLine commandLine = cli.parse(Arrays.asList(args));
Long pidValue = (Long) commandLine.getOptionValue("pid");
```

两者对比：

| 维度 | 注解式 | 编程式 |
| --- | --- | --- |
| 使用者 | Bootstrap、TelnetConsole | Arthas.parse |
| 参数定义位置 | 字段 setter 上的注解 | 方法内 `new TypedOption` |
| 优点 | 声明式、参数与字段绑定 | 灵活、不依赖反射注入 |
| 取值方式 | `inject` 后直接读字段 | `commandLine.getOptionValue("name")` |

**它为什么存在（而不用 picocli/commons-cli）？** 三个原因：(1) **体积**——boot jar 要尽量小以便快速下载，多引一个 CLI 库就多几十 KB 和潜在依赖冲突；(2) **无依赖冲突**——Arthas 注入到别人 JVM 里，任何外部依赖都可能和业务撞车，自研框架完全可控；(3) **一致性**——boot/core/client/每一个内置命令（`watch`/`trace` 等）都用同一套注解体系，学习成本一次性摊销。事实上 Arthas 的每个内置命令类（如 `WatchCommand`）都用 `@Option`/`@Argument` 声明自己的参数，`ShellServer` 解析命令行时复用的正是这套框架。

**类比**：`com.taobao.middleware.cli` 之于 Arthas，就像一套“自带的螺丝规格标准”。从最外层的 boot 到最内层的每个诊断命令，全用这一种“螺丝”，任何地方拧螺丝的工具（解析逻辑）都通用。

### 7.2 横切主题二：Configure 配置对象的跨进程传递全景

`Configure` 是 Arthas 里最重要的“数据搬运工”。它承载的配置要从 boot 进程一路穿越到目标 JVM 内部。让我们完整追踪它的“旅程”：

```
[进程A boot]
  命令行 args (String[])
      │  CLIConfigurator.inject
      ▼
  Bootstrap 实例字段 (pid/telnetPort/httpPort/ip/username/password...)
      │  拼装 attachArgs (List<String>)
      ▼
  "-pid 123 -telnet-port 3658 -core /x/core.jar -agent /x/agent.jar ..."
      │  ProcessBuilder 命令行传给进程B
      ▼
[进程B core]
  命令行 args (String[])
      │  Arthas.parse → CLIs 解析
      ▼
  Configure 对象 (core 进程内存)
      │  configure.toString() 序列化成字符串
      │  core/agent 路径先 encodeArg (URL编码)
      ▼
  "coreJarPath;<configure序列化串>"
      │  virtualMachine.loadAgent(agentJar, 这个串)  ← 跨进程!
      ▼
[进程C 目标JVM]
  AgentBootstrap.agentmain 收到 args (String)
      │  decodeArg (URL解码) + 按第一个 ';' 拆分
      ▼
  arthasCoreJar + agentArgs
      │  反射 ArthasBootstrap.getInstance(inst, agentArgs)
      ▼
  ArthasBootstrap.initArthasEnvironment(args)
      │  与 arthas.properties/系统属性/环境变量融合
      ▼
  最终的 Configure 对象 (目标JVM内存) → 决定端口/IP/密码
```

关键观察：

1. **Configure 经历了三种形态**：boot 里是零散字段、core 里是 `Configure` 对象、跨进程时是序列化字符串、目标 JVM 里又还原成 `Configure` 对象。
2. **两次“打包-解包”**：boot→core 用命令行参数打包/解包；core→目标 JVM 用 `;` 拼接字符串 + URL 编码打包，agent 侧解包。
3. **配置的“最终解释权”在目标 JVM**：`initArthasEnvironment` 会把跨进程传来的配置和目标 JVM 本地的 `arthas.properties`、`-Darthas.xxx` 系统属性、环境变量融合。这意味着可以在目标应用侧通过配置文件覆盖部分行为。

**为什么要这么绕？** 因为三个进程之间没有共享内存，唯一的通信手段就是“字符串”（命令行参数、loadAgent 的 options）。`Configure` 的 `toString()`/解析就是一套私有的“序列化协议”，让一个 Java 对象能穿越进程边界。

### 7.3 横切主题三：三个 TermServer 与网络层职责划分

第四阶段 4.5 里注册了两个 TermServer，很多人会混淆它们的职责。这里彻底厘清。

| TermServer | 端口 | 协议 | 服务对象 | 底层 |
| --- | --- | --- | --- | --- |
| `HttpTelnetTermServer` | 3658（telnet-port） | telnet + http 复用 | arthas-boot 的 telnet 客户端、`arthas-client.jar` | Netty，端口上同时能处理 telnet 帧和 http 升级 |
| `HttpTermServer` | 8563（http-port） | http / WebSocket | Web Console（浏览器）、HTTP API（`/api`）、tunnel | Netty |

关键点：

- **名字里的 “Http” 不是笔误**：`HttpTelnetTermServer` 的意思是“这是一个基于同一套 Netty http 栈、但能处理 telnet 语义的 TermServer”。Arthas 在 3658 端口上做了协议识别，纯 telnet 客户端走 telnet 处理链。
- **8563 是给 Web 用的**：当你在浏览器打开 `http://ip:8563` 看到 Arthas Web Console，走的就是这个端口的 WebSocket；当外部系统调用 `http://ip:8563/api` 发命令拿 JSON，走的是同端口的 HTTP。
- **两个端口都可选**：`bind` 里 `configure.getTelnetPort() > 0` 才注册 telnet server，`getHttpPort() > 0` 才注册 http server。可以只开其中一个（例如安全场景下只留 telnet 本地访问、关掉 http）。

**listen 的异步计数机制**：回看 4.6 `ShellServerImpl.listen`，`count = toStart.size()`（两个 TermServer 时 count=2）。每个 TermServer 用 Netty bind 端口成功后回调一次，`TermServerListenHandler` 递减计数，count 归零才认为整体 listen 成功、回调 `BindHandler` 把 `isBindRef` 置 true。**为什么用计数？** 因为两个端口的 bind 是并行异步的，必须等两个都成功才能宣告“绑定完成”。任何一个失败（端口被占用），`isBind()` 返回 false，一路传回 agent 抛错，attach 失败。

### 7.4 横切主题四：从 SpyAPI 回调看字节码增强的闭环

前面反复强调 SpyAPI 放在 BootstrapClassLoader 的重要性，但没串起完整闭环。这里用一个 `watch com.example.UserService getUser` 的例子，把 SpyAPI（第四阶段装配）+ TransformerManager（第四阶段第 7 步创建）串起来，说明启动流程搭好的这些基础设施最终如何协同工作。

```
用户敲 watch com.example.UserService getUser
      │  telnet → ShellServer → WatchCommand
      ▼
WatchCommand 通过 Instrumentation.retransformClasses(UserService.class)
      │  触发 TransformerManager 里注册的 ClassFileTransformer
      ▼
ASM 改写 UserService.getUser 的字节码：
   方法入口插入：  SpyAPI.atEnter(UserService.class, "getUser", this, args)
   方法出口插入：  SpyAPI.atExit(UserService.class, "getUser", this, args, ret)
   异常出口插入：  SpyAPI.atExceptionExit(...)
      │  UserService 被业务的 ClassLoader 加载
      ▼
业务线程执行 getUser 时，织入的字节码调用 SpyAPI.atEnter(...)
      │  SpyAPI 在 BootstrapClassLoader，业务 ClassLoader 双亲委派可见
      ▼
SpyAPI.atEnter → spyInstance.atEnter (spyInstance 是 core 在 SpyAPI.init 前后设置的真实 Spy 实现)
      │  真实 Spy 实现桥接回 ArthasClassloader 里的 core 逻辑
      ▼
core 收集调用信息 → 通过 ShellServer 推送回 telnet 客户端 → 用户看到 watch 结果
```

回看 spy 源码，`SpyAPI` 里 `spyInstance` 默认是 `NOPSPY`（空实现），`atEnter` 等都是空方法。**这是一个精妙的“开关”设计**：

- 未初始化时，`spyInstance = NOPSPY`，即使有残留的织入字节码调用 `SpyAPI.atEnter`，也只是调用空方法，零开销、不报错。
- `SpyAPI.init()`（第四阶段最后一步）配合 core 侧把 `spyInstance` 设为真正的 Spy 实现，回调才真正生效。
- `SpyAPI.destroy()`（stop 时）把 `spyInstance` 设回 `NOPSPY` 并 `INITED=false`，让所有织入调用瞬间“失效”为空操作。

**这解释了为什么启动阶段一定要把 SpyAPI 装配好**：它是连接“被增强的业务字节码”与“ArthasClassloader 里的 core 逻辑”的唯一桥梁，且通过 NOPSPY/真实实现的切换实现了增强能力的“总开关”。启动流程的第四阶段之所以要 `initSpy` + `SpyAPI.init`，就是为了把这座桥架好、把开关打开。

---

## 第八阶段：完整的类加载器拓扑与内存视图

理解 Arthas 启动，本质上是理解“谁把哪个类加载到了哪个 ClassLoader”。本阶段给出目标 JVM（进程 C）在 Arthas 就绪后的完整类加载器拓扑图。

### 8.1 类加载器拓扑图

```
                    ┌───────────────────────────────────────┐
                    │      BootstrapClassLoader (null)         │
                    │  - java.lang.*, java.util.* 等核心类       │
                    │  - java.arthas.SpyAPI  ← Arthas 挂进来的!  │
                    │    (通过 appendToBootstrapClassLoaderSearch)│
                    └───────────────────┬───────────────────────┘
                                        │ parent
                    ┌───────────────────▼───────────────────────┐
                    │  ExtClassLoader / PlatformClassLoader       │
                    │  - JDK 扩展类                                │
                    └──────────┬──────────────────┬───────────────┘
                               │ parent            │ parent (被设为它!)
              ┌────────────────▼────────┐   ┌──────▼─────────────────────────┐
              │   AppClassLoader          │   │   ArthasClassloader             │
              │  - 业务应用 classpath      │   │  (parent = ExtClassLoader)      │
              │  - com.taobao.arthas       │   │  - core.jar 里的所有类           │
              │      .agent334.AgentBootstrap│  │  - ArthasBootstrap/ShellServer  │
              │      (agent.jar 被它加载)   │   │  - 内嵌的 netty/fastjson/asm...  │
              └────────────────┬──────────┘   │  - parent-last 加载策略          │
                               │              └──────────────────────────────────┘
              ┌────────────────▼──────────┐
              │  业务自定义 ClassLoader      │
              │  (Tomcat WebappClassLoader、 │
              │   Spring Boot LaunchedURL... │
              │  - com.example.UserService   │  ← 被增强的业务类
              └───────────────────────────────┘
```

逐层解读这张图揭示的关键事实：

1. **AgentBootstrap 在 AppClassLoader**：agent.jar 被目标 JVM 的 AppClassLoader 加载（因为 `loadAgent` 把 agent.jar 加进了 system classpath）。所以 AgentBootstrap 是“业务可见”的，但它极小，只做引导。
2. **ArthasClassloader 的 parent 是 ExtClassLoader，不是 AppClassLoader**：这是类隔离的关键。它“横向”挂在 AppClassLoader 旁边，而不是“纵向”挂在下面。这样它加载类时的默认委派链不经过 AppClassLoader（业务 classpath），从根本上避免污染。
3. **SpyAPI 在最顶端**：无论业务类被 AppClassLoader 还是任何自定义 ClassLoader 加载，双亲委派最终都能到达 BootstrapClassLoader 看到 SpyAPI。这是全局可见性的物理保证。
4. **业务类在自己的 ClassLoader**：`com.example.UserService` 通常在 Tomcat 的 WebappClassLoader 或 Spring Boot 的 LaunchedURLClassLoader 里。它被 retransform 时织入的 `SpyAPI.xxx()` 调用，靠双亲委派向上找到 BootstrapClassLoader 里的 SpyAPI。

### 8.2 为什么这个拓扑能同时满足“隔离”与“可见”两个矛盾需求？

Arthas 有两个看似矛盾的需求：

- **隔离**：Arthas 自己的重型依赖（netty/fastjson）绝不能和业务撞车 → 需要“看不见业务、业务也看不见 Arthas”。
- **可见**：被增强的业务类要能回调到 Arthas 的监控逻辑 → 需要“业务能看见某个 Arthas 的入口”。

这个拓扑用**两个不同的类**分别满足：

| 需求 | 承载类 | 所在 ClassLoader | 可见性 |
| --- | --- | --- | --- |
| 隔离（重型逻辑） | ArthasBootstrap、ShellServer、netty... | ArthasClassloader（parent-last） | 业务完全看不见 |
| 可见（回调入口） | SpyAPI（极薄的桥） | BootstrapClassLoader | 全局可见 |

**精髓在于“薄桥厚墙”**：SpyAPI 是一座“薄桥”（只有方法签名，没有实现逻辑），放在全局可见处；真正的“厚墙后逻辑”全在 ArthasClassloader 里隔离。桥的另一端（`spyInstance`）指向墙后的实现。这样既全局可见，又完全隔离——两全其美。

### 8.3 stop 卸载时的内存回收链条（与启动对称）

启动的对称操作是卸载（`stop` 命令）。理解卸载能反向印证启动设计的合理性：

1. `SpyAPI.destroy()`：`spyInstance` 设回 `NOPSPY`，`INITED=false`。所有织入的字节码调用瞬间变空操作。
2. 还原被增强的类：TransformerManager 移除 transformer，retransform 相关类恢复原始字节码。
3. 关闭 ShellServer、Netty、释放 3658/8563 端口。
4. `AgentBootstrap.resetArthasClassLoader()`：把静态 `arthasClassLoader` 置 null。
5. 此时 ArthasClassloader 若无其他强引用，就能被 GC，它加载的几百个 core 类和 metaspace 随之释放。

**第 5 步能成功的前提，正是启动时的 arthas-binding-thread 设计**（第六阶段 6.5）——因为 bind 不是在长寿命的 Attach Listener 线程执行，没有残留引用，ArthasClassloader 才能真正被回收。启动与卸载在这里首尾呼应。

---

## 第九阶段：常见启动失败与源码级排查对照

把源码知识落到实处：下表把“用户实际遇到的报错”映射到“源码里的具体位置和原因”，是这份源码分析最直接的实用价值。

| 现象 / 报错 | 源码位置 | 根因 | 对照前文 |
| --- | --- | --- | --- |
| `Can not find java process` | `ProcessUtils.select` | jps 列不出目标进程（权限/非同用户/进程已退） | 1.4 |
| attach 卡住无响应 | `Arthas.attachAgent` 的 `VirtualMachine.attach` | 目标 JVM 的 Attach Listener 未响应，常见于 JVM 卡死或权限不足 | 2.3 |
| `Current VM java version do not match target VM` | `attachAgent` 版本检查 | boot/core 的 JDK 与目标 JVM 版本不一致 | 2.3 Q4 |
| `Non-numeric value found` warn | `attachAgent` 第一个 catch | 低版本 JDK attach 高版本 JDK（多为假错误） | 2.3 Q6 |
| `AgentLoadException: 0` warn | `attachAgent` 第二个 catch | 高版本 JDK attach 低版本 JDK（多为假错误） | 2.3 Q6 |
| `Arthas server already stared, skip attach` | `AgentBootstrap.main` 开头 | 该 JVM 已有运行中的 Arthas（SpyAPI.isInited() 为 true） | 3.2 (2) |
| `Can not find arthas-core jar file` | `AgentBootstrap.main` | core.jar 路径错误且同目录兜底也没找到 | 3.2 (5) |
| `Arthas server port binding failed` | `AgentBootstrap.bind` / `ArthasBootstrap.bind` | 3658 或 8563 端口被占用，listen 失败 | 3.4 / 4.6 |
| 连上后终端乱码/输入无回显 | `TelnetConsole` tty 设置未还原 | 异常退出没走到 `terminal.restore()` | 5.5 |
| watch/trace 无输出或报 NoClassDefFoundError: SpyAPI | `ArthasBootstrap.initSpy` / `enhanceClassLoader` | SpyAPI 未正确挂到 BootstrapClassLoader，或容器 ClassLoader 屏蔽了它 | 4.3 / 4.4 |
| 反复 attach/stop 后 Metaspace OOM | 若在旧版本或改坏了 binding-thread 逻辑 | ArthasClassloader 被长寿命线程持有无法回收 | 6.5 / 8.3 |

**排查方法论**：Arthas 启动阶段的所有原始诊断信息都写在 `$HOME/logs/arthas/arthas.log`（回看 3.1 的静态初始化块）。当 attach 失败、boot 端只看到笼统错误时，第一件事永远是去目标机器上看这个日志文件——因为 agent 引导阶段发生在 logback 之前，只有这个文件记录了最早期的失败细节。

---

## 总结：从命令行到 Shell 就绪的完整时序

下面用编号叙述把全流程从头到尾串一遍。每一步都对应前文的具体源码分析，可回溯查阅。

第一步，用户在命令行执行 `java -jar arthas-boot.jar`，JVM 启动 boot 进程（进程 A），加载并执行 `Bootstrap` 类。类加载时静态初始化块确定 `ARTHAS_LIB_DIR`（默认 `~/.arthas/lib`）并 `mkdirs`。

第二步，`Bootstrap.main` 用 `com.taobao.middleware.cli` 的 `CLIConfigurator.define` + `parse` + `inject` 把命令行参数解析注入到 `bootstrap` 对象。若用户未指定 `pid`，调用 `ProcessUtils.select` 通过 jps 列出所有 Java 进程，交互式地让用户选一个目标 JVM 的 pid。

第三步，`Bootstrap` 定位 arthas home：优先 `~/.arthas/lib/<version>/arthas`，否则 boot jar 同目录，否则通过 `DownloadUtils` 从 maven 仓库下载对应版本的 arthas 包并解压。

第四步，`Bootstrap` 拼装 attach 参数字符串，形如 `-jar <arthas-core.jar> -pid <pid> -target-ip <ip> -telnet-port <port> -http-port <port> -core <core.jar> -agent <agent.jar>`。

第五步，`ProcessUtils.startArthasCore(pid, attachArgs)` 选择合适的 java 可执行文件（优先 JAVA_HOME 指向的 JDK），用 `ProcessBuilder` 拉起 core 进程（进程 B），把标准输出/错误继承过来。

第六步，core 进程启动，执行 `Arthas.main`，其构造方法调用 `attachAgent(parse(args))`。`parse` 用 CLI 把命令行还原成 `Configure` 对象（pid、core、agent、ip、port 等）。

第七步，`attachAgent` 遍历 `VirtualMachine.list()` 找到匹配 pid 的 `VirtualMachineDescriptor`，`VirtualMachine.attach(descriptor)` 建立与目标 JVM（进程 C）的 attach 通道。

第八步，core 做版本检查后，调用 `virtualMachine.loadAgent(arthasAgentPath, coreJar + ";" + configure.toString())`。JVM attach 机制让目标 JVM 加载 `arthas-agent.jar`（其 MANIFEST 的 `Agent-Class` 指向 `AgentBootstrap`），并调用其 `agentmain(args, inst)`。参数用 `;` 分隔 core.jar 路径与配置串，jar 路径做过 URL encode 以兼容含空格/中文的路径。

第九步，`loadAgent` 返回后 core 调用 `virtualMachine.detach()` 断开 attach 通道——core 进程使命完成，随后退出。此后 Arthas Server 完全运行在目标 JVM 内部。

第十步，目标 JVM 内 `AgentBootstrap.agentmain` 转调 `main`（synchronized）。先通过加载 `java.arthas.SpyAPI` 并检查 `isInited()` 做防重复 attach 判断——已初始化则直接返回。

第十一步，`main` 按第一个 `;` 拆分参数得到 `arthasCoreJar` 与 `agentArgs`，通过 `appendToBootstrapClassLoaderSearch` 把 `arthas-spy.jar` 挂到 BootstrapClassLoader，并 `new ArthasClassloader` 用于隔离加载 core.jar。

第十二步，`main` 新起名为 `arthas-binding-thread` 的 daemon 线程执行 `bind`，并 `join` 等待——用短命线程避免 Attach Listener 线程持有 ArthasClassloader 引用造成内存泄漏（issue #195）。

第十三步，`bind` 用 ArthasClassloader 反射加载 `com.taobao.arthas.core.server.ArthasBootstrap`，调用其静态工厂 `getInstance(inst, args)` 触发单例构造，再调 `isBind()` 校验端口是否绑定成功。

第十四步，`ArthasBootstrap` 构造方法按序执行七步：initFastjson、initSpy（确保 SpyAPI 在 BootstrapClassLoader）、initArthasEnvironment（融合配置生成 Configure）、initLogger、enhanceClassLoader、initBeans、bind(configure)，最后创建命令执行线程池、TransformerManager 并注册 shutdown hook。

第十五步，`bind` 依次：按需启动 TunnelClient、构造 ShellServerOptions、创建 SecurityAuthenticatorImpl（0.0.0.0 监听强制密码）、创建 ShellServerImpl、装配 BuiltinCommandPack、注册 HttpTelnetTermServer（3658）与 HttpTermServer（8563）、注册命令解析器。

第十六步，`shellServer.listen(new BindHandler(isBindRef))` 让每个 TermServer 用 Netty 的 ServerBootstrap 真正 bind 端口并开始监听；绑定成功回调把 `isBindRef` 置 true。

第十七步，创建 SessionManagerImpl 与 HttpApiHandler，最后调用 `SpyAPI.init()` 把 `INITED` 置 true，正式宣告“Arthas Server 已就绪”——这也成为后续防重复 attach 的判断依据。

第十八步，控制权回到进程 A（boot）。`Bootstrap` 反射加载 `com.taobao.arthas.client.TelnetConsole` 并调用其 `process(args)`（复用入口，不会 `System.exit`）。

第十九步，`TelnetConsole.process` 用 jline 的 `ConsoleReader` 接管本地终端，关闭默认 Ctrl+C 行为，绑定自定义 Ctrl+C（转发中断字节给服务端）与 Ctrl+D 回调，并用 `WindowSizeOptionHandler` 同步终端窗口大小。

第二十步，`TelnetClient.connect(targetIp, telnetPort)` 连上目标 JVM 里 HttpTelnetTermServer 监听的 3658 端口。服务端 `handleTerm` 为该连接创建 `ShellImpl` 会话、下发欢迎 banner、`readline()` 开始读命令。

第二十一步，若无 -c/-f，进入交互模式：`IOUtil.readWrite` 在本地终端与远端 telnet 之间建立双向数据泵，用户看到 `[arthas@<pid>]$` 提示符，**Arthas Shell 正式就绪**，可以开始敲 dashboard/thread/watch/trace 等命令。若有 -c/-f，则进入 batchModeRun，通过检测回显中的 `[arthas@` 提示符逐条串行下发命令（追加 `| plaintext`），全部执行完发 `quit` 退出。

第二十二步，无论正常还是异常退出，`finally` 里 `terminal.restore()` 还原用户终端的 tty 设置，避免把用户 shell 搞坏（issue #1412）。至此，从 `java -jar arthas-boot.jar` 到 Shell 就绪的完整链路闭环。

---

## 附录 A：全流程一句话记忆图

boot 选进程并下载/定位包 → 拉起 core 进程 → core 用 VirtualMachine.attach + loadAgent 把 agent 注入目标 JVM → agentmain 防重复、挂 Spy 到 Bootstrap、建 ArthasClassloader、专用线程反射调 ArthasBootstrap.getInstance → ArthasBootstrap 七步初始化并 bind（Netty 监听 3658/8563）→ SpyAPI.init 宣告就绪 → boot 反射调 TelnetConsole 连上 3658 → Shell 就绪。

## 附录 B：三个进程角色对照

| 进程 | 名称 | 生命周期 | 主要职责 |
| --- | --- | --- | --- |
| A | boot 进程 | 常驻（直到用户退出交互） | 选目标、下载/定位包、拉起 core、最后当 telnet 客户端 |
| B | core 进程 | 极短（attach 完即退出） | VirtualMachine.attach + loadAgent 注入 agent |
| C | 目标 JVM | 业务进程本身 | 内部运行 Arthas Server（ArthasBootstrap/ShellServer/Netty） |

## 附录 C：关键类加载器归属

| 类 | 加载器 | 原因 |
| --- | --- | --- |
| java.arthas.SpyAPI | BootstrapClassLoader | 被增强的业务类需全局可见地回调它 |
| AgentBootstrap | AppClassLoader（agent.jar） | 极小引导器，通过反射跨界 |
| ArthasBootstrap 及 core 全部类 | ArthasClassloader（parent-last） | 与业务彻底类隔离，用自带依赖版本 |
| 业务类 Foo | 业务自身 ClassLoader | 被 retransform 时织入 SpyAPI 调用 |

---

## 附录 D：ProcessUtils 深入 —— jps、findJava 与下载

`ProcessUtils`（`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/boot/src/main/java/com/taobao/arthas/boot/ProcessUtils.java`）是 boot 阶段的“环境侦察兵”，除了 1.4 讲过的 `select`，还有几个关键职责值得单独拆解。

### D.1 listProcessByJps —— 如何列出 Java 进程

`select` 内部调用 `listProcessByJps(verbose)`，它并不总是直接调 JDK 的 `jps` 命令，而是优先尝试用 `sun.jvmstat.monitor`（jps 本身就是基于这套 API）来枚举本机 JVM。核心思路：

1. 通过 jvmstat 的 `MonitoredHost` 拿到本机所有活跃 JVM 的 `vmid`（即 pid）。
2. 对每个 vmid 读取其 `sun.rt.javaCommand`（主类/主 jar 名称）作为进程描述。
3. 组装成 `Map<pid, 描述>` 返回。

**为什么要排除 Arthas 自身？** boot 进程和它拉起的 core 进程也是 Java 进程，会出现在列表里。`listProcessByJps` 会用类名/命令行特征（如包含 `arthas-boot.jar`、`arthas-core.jar`、`Bootstrap`）把它们过滤掉，避免用户误选“Arthas 去 attach Arthas 自己”这种荒谬情形。

**verbose 参数**：普通模式下描述里只显示主类名；`verbose` 模式会显示完整的启动命令行参数，便于在多个同名进程里区分（比如同一个 jar 用不同端口启了三份）。

### D.2 findJava / findJavaHome —— 为 core 进程挑一个合适的 java

第一阶段 1.7 提到 `startArthasCore` 要 `findJava()`。它的查找优先级大致是：

1. `JAVA_HOME` 环境变量指向的 `bin/java`（或 Windows 的 `bin/java.exe`）。
2. 当前 boot 进程 `java.home` 系统属性推导出的 java。
3. PATH 中的 java（兜底）。

**为什么这个选择很关键？** core 进程要执行 `VirtualMachine.attach`，这依赖 `com.sun.tools.attach`。在 JDK8 里，attach 相关支持通常要求是 JDK（而非纯 JRE）；且 core 的 java 版本最好接近目标 JVM，才能减少 2.3 Q6 里那两类“交叉 attach”告警。`findJava` 优先用 `JAVA_HOME`，正是为了让用户能通过设置 `JAVA_HOME` 精确控制“用哪个 java 去 attach”。这也解释了 2.3 里那句 warn：“Target VM JAVA_HOME is X, arthas-boot JAVA_HOME is Y, try to set the same JAVA_HOME”——它引导用户对齐这两个 JAVA_HOME。

### D.3 与 DownloadUtils 协作 —— 自举下载

当 1.5 的三级查找走到第三级（需要下载）时，`ProcessUtils`/`DownloadUtils` 协作完成：

1. 查询远端最新版本号（或用户 `--use-version` 指定的版本）。
2. 从配置的 maven 仓库（可用 `--repo-mirror aliyun` 切换到阿里云镜像、`--use-http` 用 http）下载 `arthas-packaging-<version>-bin.zip`。
3. 校验并解压到 `~/.arthas/lib/<version>/arthas/`。
4. 解压产物包含 `arthas-core.jar`、`arthas-agent.jar`、`arthas-spy.jar`、`arthas-client.jar` 等——正好是后续 attach 和连接所需的全部组件。

**它为什么存在？** 让用户只需下载一个几十 KB 的 `arthas-boot.jar` 就能启动，其余重型组件按需、按版本自动获取。这是“自举（bootstrap）”一词的由来——boot jar 能把自己需要的一切“拉起来”。

---

## 附录 E：ShellServerImpl 会话机制补充

第四阶段 4.6 讲了 `listen` 与 `handleTerm`。这里补充 `ShellServerImpl` 里几个与“会话生命周期”相关的机制，它们决定了你连上 Arthas 后的交互体验。

### E.1 会话创建：createShell 与 welcome

`handleTerm` 里 `createShell(term)` 为每个新连接建一个 `ShellImpl`。随后 `tryUpdateWelcomeMessage()`（若开了 tunnel，把 agentId 拼进欢迎语）、`session.setWelcome(welcomeMessage)`——这就是你连上后看到的 Arthas ASCII logo 和版本信息的来源（`ArthasBanner.welcome()`）。`session.init()` 完成初始化后 `sessions.put(session.id, session)`，最后 `session.readline()` 开始读命令。

**注意 put 的时机**：源码注释特意写了 “Put after init so the close handler on the connection is set”——必须先 `init` 把关闭回调设好，再放进 `sessions` map，否则可能出现“会话已在 map 里但关闭回调还没设，连接此时断开会漏处理”的竞态。这是并发编程里“注册顺序”的典型考量。

### E.2 会话回收：evictSessions 与 reaper

`ShellServerImpl` 构造时从 options 取了 `timeoutMillis`（会话超时）和 `reaperInterval`（回收扫描间隔）。它会用 `scheduledExecutorService` 周期性调用 `evictSessions`：

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
    }
    // ... 关闭这些超时会话
}
```

**关键设计**：超时判定不仅看“最后访问时间超过 timeout”，还要求 `session.jobs().size() == 0`（没有正在运行的 job）。**为什么？** 因为 `trace`/`watch` 这类命令可能挂很久等待触发条件（比如“等某个方法被调用满 100 次”）。如果只看时间就把会话关了，会把用户正在等结果的长命令误杀。这个 `&& jobs==0` 的判断，正是对“诊断命令天然长耗时”特性的体贴照顾。

### E.3 registerCommandResolver 的优先级语义

回看 4.5，`bind` 里先注册若干内部 resolver，最后注册 `builtinCommands`。而 `ShellServerImpl.registerCommandResolver` 用的是 `resolvers.add(0, resolver)`——**插到列表最前**。这意味着：**后注册的优先级更高**。命令解析时按列表顺序匹配，先匹配到的先生效。构造方法里默认 `add(new BuiltinCommandResolver())`（注意这是列表末尾的兜底），保证 help 里能列出内置命令。这套“后注册覆盖先注册 + 末尾兜底”的机制，为将来扩展自定义命令（`command-locations` 加载外部命令包）预留了覆盖内置命令的能力。

---

## 附录 F：一句话回答“Arthas 是怎么进到别人 JVM 里的”

如果只能用一句话向同事解释 Arthas 的启动魔法：**boot 进程用 jps 帮你选中目标 JVM，然后 fork 一个和目标同版本的 core 进程，core 用 JDK 的 Attach API 把一个极小的 agent.jar 注入目标 JVM，agent 在目标 JVM 内建一个隔离的 ArthasClassloader 反射引导起 core 服务端，服务端把 SpyAPI 挂到 BootstrapClassLoader 当回调桥、开 3658/8563 两个 Netty 端口，最后 boot 摇身一变成 telnet 客户端连上 3658，于是你看到了 `[arthas@pid]$`。**

---

## 附录 G：Bootstrap 常用命令行选项与启动行为对照

前文 1.3 讲了 `@Option`/`@Argument` 的机制。这里把 `Bootstrap` 常见选项与它们对启动流程的实际影响列全，帮助把源码知识和日常使用连接起来。

| 选项 | 影响的阶段 | 对启动流程的实际作用 |
| --- | --- | --- |
| `<pid>`（位置参数） | 第一阶段 | 跳过 `ProcessUtils.select` 交互，直接指定目标 JVM |
| `--telnet-port` | 一/二/四阶段 | 改变 telnet 端口，最终传到 `ArthasBootstrap.bind` 的 `HttpTelnetTermServer` |
| `--http-port` | 一/二/四阶段 | 改变 http 端口，最终传到 `HttpTermServer`；设为 0 可关闭 http |
| `--target-ip` | 二/四阶段 | 服务端绑定 IP；设为 `0.0.0.0` 会触发 4.7 的强制密码逻辑 |
| `--arthas-home` | 第一阶段 | 跳过三级查找，强制指定 core/agent/spy 所在目录 |
| `--use-version` | 第一阶段 | 指定下载的 Arthas 版本，落到 `~/.arthas/lib/<version>` |
| `--repo-mirror` | 第一阶段 | 切换下载镜像（如 aliyun），加速国内下载 |
| `--use-http` | 第一阶段 | 下载走 http 而非 https（受限网络环境） |
| `-c '<cmd>'` | 第五阶段 | 传给 `TelnetConsole`，进入批处理模式执行单/多命令 |
| `-f <file>` | 第五阶段 | 传给 `TelnetConsole`，从批处理文件读命令 |
| `--username` / `--password` | 二/四阶段 | 传到 `SecurityAuthenticatorImpl`，开启认证 |
| `--tunnel-server` | 四阶段 | 触发 `bind` 里的 `TunnelClient.start`，反向注册到隧道服务端 |
| `--agent-id` | 四阶段 | tunnel 场景下本 Agent 的唯一标识 |
| `--app-name` | 四阶段 | 应用名，用于集群管理/统计 |
| `--disabled-commands` | 四阶段 | 传到 `BuiltinCommandPack`，启动时禁用指定命令（如禁 `redefine`） |

### G.1 从选项看“安全”与“便捷”的权衡

把上表按设计意图归类，能看出 Arthas 在“便捷”与“安全”之间的权衡：

| 维度 | 便捷向选项 | 安全向选项 | 权衡点 |
| --- | --- | --- | --- |
| 目标选择 | 交互式 select、位置参数 pid | —— | 默认便捷，但排除自身避免误操作 |
| 网络暴露 | `--target-ip 0.0.0.0` | 默认 `127.0.0.1`、`--username/--password` | 暴露即强制认证 |
| 能力范围 | 全命令可用 | `--disabled-commands` | 高危环境可裁剪能力 |
| 组件获取 | 自动下载、镜像加速 | `--arthas-home` 离线指定 | 内网/隔离环境可完全离线 |

**这张表回答了一个设计层面的问题**：为什么 Arthas 默认值都偏“保守安全”（127.0.0.1、需要交互确认目标）？因为它是一把“能改运行中 JVM 字节码、能执行任意表达式”的利器，默认必须把用户约束在最小风险面内，只有用户显式地用选项“解锁”更强能力时，才承担对应风险（并被强制加认证）。这与第四阶段 4.7 的安全机制是同一设计哲学在命令行层面的体现。

### G.2 选项与三进程的对应关系

最后用一张表把“选项在哪个进程被消费”钉死，彻底消除“配置到底在哪生效”的困惑：

| 选项 | 在进程 A(boot) 消费 | 传给进程 B(core) | 传给进程 C(目标JVM) |
| --- | --- | --- | --- |
| pid | 是（决定 attach 谁） | 是（attach 目标） | 否 |
| arthas-home / use-version / repo-mirror | 是（定位/下载） | 否 | 否 |
| telnet-port / http-port | 是（最后连接用） | 是（透传） | 是（真正 bind） |
| target-ip | 是（最后连接用） | 是（透传） | 是（真正 bind + 安全判断） |
| username / password | 否 | 是（透传） | 是（认证器） |
| tunnel-server / agent-id / app-name | 否 | 是（透传） | 是（TunnelClient） |
| disabled-commands | 否 | 是（透传） | 是（BuiltinCommandPack） |
| -c / -f | 是（TelnetConsole 批处理） | 否 | 否 |

可以清楚看到：**端口和 IP 类选项要“三进程接力”**——boot 拿来最后连接、core 透传、目标 JVM 真正绑定；而下载/定位类选项只在 boot 消费；批处理类选项只在 boot 的 TelnetConsole 消费；认证/命令裁剪类选项则一路透传到目标 JVM 服务端才生效。这正是 7.2 “Configure 跨进程传递全景”在选项粒度上的具体展开。
# Arthas 实战场景源码全流程解析 —— 场景篇1

> 本文聚焦四个真实生产场景，从用户敲下命令的那一刻起，逐层追踪 Arthas 内部的源码调用链路，剖析每一行关键代码的设计意图。每个场景均包含：用户故事、操作命令、源码链路追踪、关键代码片段逐行解释、Q&A 设计问题分析与场景总结。

---

## 场景一：线上 CPU 飙高排查 —— dashboard + thread -n 3 联合诊断

### 1.1 用户故事

某电商系统在促销期间，运维收到告警：订单服务 CPU 使用率飙升至 95%，响应时间从 50ms 涨到 800ms。运维同学需要快速回答两个问题：

1. 哪个线程在疯狂消耗 CPU？
2. 该线程正在执行什么代码？

传统的排查方式是 `top -Hp <pid>` + `printf "%x\n" <tid>` + `jstack`，但需要登录到机器、手动换算线程 ID、在大量堆栈中搜索。Arthas 提供了一条更高效的路径：`dashboard` 总览 → `thread -n 3` 找出 Top3 CPU 线程 → `thread <id>` 查看堆栈。

### 1.2 操作命令与参数说明

```bash
# 第一步：查看整体面板——线程、内存、GC、运行时信息一览无余
dashboard

# 第二步：找出 CPU 使用率最高的 3 个线程
thread -n 3

# 第三步：查看具体线程的完整堆栈
thread <thread-id>
```

**参数说明**：

| 命令 | 参数 | 说明 |
|------|------|------|
| `dashboard` | `-i <interval>` | 刷新间隔，默认 5000ms |
| `dashboard` | `-n <num>` | 刷新次数，默认持续刷新 |
| `thread` | `-n <num>` | 找出 CPU 使用率最高的 N 个线程 |
| `thread` | `<id>` | 查看指定线程的堆栈 |
| `thread` | `-i <interval>` | 采样间隔，默认 200ms |
| `thread` | `--lockedMonitors` | 显示线程持有的 monitor 锁 |
| `thread` | `--lockedSynchronizers` | 显示线程持有的 synchronizer 锁 |

### 1.3 从命令输入到结果输出的完整链路

当用户在 Arthas 交互式 shell 中输入 `dashboard` 并回车时，整个执行链路如下：

```
ShellLineHandler.handle("dashboard")
  → CommandLine commandLine = clParser.parse(input)           // 解析命令行
  → Job job = jobHandlers.createJob(commandLine)               // 创建 Job
  → job.run()                                                  // 启动 Job
    → ProcessImpl.run()                                        // 执行进程
      → AnnotatedCommandImpl.process(process)                  // 反射调用命令的 process 方法
        → DashboardCommand.process(process)                   // Dashboard 命令入口
          → DashboardTimerTask(TimerTask)                      // 创建定时任务
          → timer.schedule(task, 0, interval)                  // Timer 调度
            → DashboardTimerTask.run()                          // 定时执行
              → ThreadUtil.getThreads()                         // 采集线程信息
              → MemoryCommand.memoryInfo()                     // 采集内存信息
              → ManagementFactory.getGarbageCollectorMXBeans() // 采集 GC 信息
              → DashboardModel 构建                             // 组装数据模型
              → process.appendResult(dashboardModel)           // 推送到终端
              → DashboardView.render()                          // 渲染面板
```

### 1.4 DashboardCommand.process() 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/DashboardCommand.java`

```java
@Name("dashboard")
@Summary("Overview of dashboard")
@Description(Constants.EXAMPLE + " dashboard\n" +
        Constants.EXAMPLE + " dashboard -i 5000 -n 3\n" +
        Constants.WIKI + Constants.WIKI_HOME + "dashboard")
public class DashboardCommand extends AnnotatedCommand {

    // 刷新间隔，默认 5000ms
    @Option(shortName = "i", longName = "interval")
    @Description("The interval between two executions, 5000 ms by default.")
    private long interval = 5000L;

    // 刷新次数，默认持续刷新
    @Option(shortName = "n", longName = "number-of-execution")
    @Description("The number of times this command will be executed.")
    private int numberOfExecution = Integer.MAX_VALUE;
```

**逐行解释**：

- `@Name("dashboard")`：注册命令名称，ShellLineHandler 通过该注解将用户输入映射到此类。
- `@Option(shortName = "i", longName = "interval")`：Arthas 使用自研的 CLI 框架（基于 `com.taobao.middleware.cli`），`@Option` 注解将命令行参数 `-i 5000` 绑定到 `interval` 字段。框架在 `AnnotatedCommandImpl` 中通过反射完成参数注入。
- `interval = 5000L`：默认 5 秒刷新一次。这个值不能太小，否则采样本身会消耗 CPU；也不能太大，否则无法及时发现异常。
- `numberOfExecution = Integer.MAX_VALUE`：默认持续刷新，直到用户按 `q` 或 `Ctrl+C` 退出。

```java
    @Override
    public void process(CommandProcess process) {
        DashboardTimerTask task = new DashboardTimerTask(process);
        Timer timer = new Timer("Arthas-Dashboard-Timer", true);

        // 立即执行第一次，之后按 interval 间隔执行
        timer.scheduleAtFixedRate(task, 0, interval);
        process.watch(new ResultWatcher<DashboardModel>(DashboardModel.class) {
            @Override
            public void render(DashboardModel result) {
                // 由 DashboardView 渲染
            }
        });
    }
```

**逐行解释**：

- `DashboardTimerTask task = new DashboardTimerTask(process)`：创建定时任务，持有 `CommandProcess` 引用。`process` 是命令执行上下文，负责结果输出和生命周期管理。
- `Timer timer = new Timer("Arthas-Dashboard-Timer", true)`：创建守护线程 Timer。`true` 表示 daemon thread，JVM 退出时自动终止，避免阻止 JVM 正常关闭。
- `timer.scheduleAtFixedRate(task, 0, interval)`：立即执行第一次（delay=0），然后按 `interval` 固定频率执行。注意使用 `scheduleAtFixedRate` 而非 `schedule`，前者以固定频率执行（不等上一次完成），后者固定延迟（等上一次完成后才开始计时）。
- `process.watch(new ResultWatcher...)`：注册结果监听器，当 `DashboardTimerTask` 调用 `process.appendResult()` 时触发 `render()` 方法进行渲染。

### 1.5 DashboardTimerTask.run() —— 数据采集核心

> 源码位置：同上 `DashboardCommand.java`，内部类 `DashboardTimerTask`

```java
    private class DashboardTimerTask extends TimerTask {
        private final CommandProcess process;
        // 采样计数器
        private int count = 0;

        public DashboardTimerTask(CommandProcess process) {
            this.process = process;
        }

        @Override
        public void run() {
            try {
                // ========== 第一块：线程信息 ==========
                // 获取所有线程的基本信息和 CPU 使用率
                List<ThreadVO> threadVOList = ThreadUtil.getThreads();

                // ========== 第二块：内存信息 ==========
                // 通过 JMX 获取堆/非堆内存各区域使用情况
                Map<String, List<MemoryEntryVO>> memoryInfoMap = MemoryCommand.memoryInfo();

                // ========== 第三块：GC 信息 ==========
                // 通过 JMX 获取各 GC 收集器的收集次数和耗时
                List<GarbageCollectorVO> garbageCollectors = new ArrayList<GarbageCollectorVO>();
                List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
                for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
                    GarbageCollectorVO gcVO = new GarbageCollectorVO();
                    gcVO.setName(gcMXBean.getName());
                    gcVO.setGcCount(gcMXBean.getCollectionCount());
                    gcVO.setGcTime(gcMXBean.getCollectionTime());
                    garbageCollectors.add(gcVO);
                }

                // ========== 第四块：运行时信息 ==========
                RuntimeInfoVO runtimeInfoVO = new RuntimeInfoVO();
                RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
                runtimeInfoVO.setJavaVersion(runtimeMXBean.getVmVersion());
                runtimeInfoVO.setUptime(runtimeMXBean.getUptime());
                // ...

                // ========== 组装 DashboardModel ==========
                DashboardModel dashboardModel = new DashboardModel();
                dashboardModel.setThreadVOList(threadVOList);
                dashboardModel.setMemoryInfoMap(memoryInfoMap);
                dashboardModel.setGarbageCollectors(garbageCollectors);
                dashboardModel.setRuntimeInfo(runtimeInfoVO);

                // 推送到终端渲染
                process.appendResult(dashboardModel);

            } catch (Throwable e) {
                process.end(1, "dashboard error: " + e.getMessage());
            } finally {
                count++;
                if (count >= numberOfExecution) {
                    // 达到执行次数上限，结束命令
                    process.end();
                }
            }
        }
    }
```

**逐段解释**：

**第一块 — 线程信息采集 `ThreadUtil.getThreads()`**：

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/util/ThreadUtil.java`

```java
public static List<ThreadVO> getThreads() {
    // 通过 ThreadMXBean 获取所有线程信息
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    // 不获取 lockedMonitors 和 lockedSynchronizers（性能考虑）
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 0);

    // 转换为 ThreadVO
    List<ThreadVO> threadVOList = new ArrayList<ThreadVO>();
    for (ThreadInfo info : threadInfos) {
        if (info != null) {
            ThreadVO vo = createThreadVO(info);
            // 获取 CPU 时间
            long cpuTime = threadMXBean.getThreadCpuTime(info.getThreadId());
            long userTime = threadMXBean.getThreadUserTime(info.getThreadId());
            vo.setCpuTime(cpuTime);
            vo.setUserTime(userTime);
            threadVOList.add(vo);
        }
    }
    return threadVOList;
}
```

关键点分析：

- `threadMXBean.getThreadInfo(ids, 0)`：第二个参数 `0` 表示堆栈深度为 0，即不获取堆栈。dashboard 只需要线程状态概览，获取完整堆栈会严重影响性能。
- `threadMXBean.getThreadCpuTime(threadId)`：返回该线程从启动到现在消耗的 CPU 总时间（纳秒）。注意这是**累计值**，不是瞬时值。dashboard 面板上显示的 CPU% 是通过**两次采样的差值**计算出来的，这与 `thread -n` 的采样原理一致。

**第二块 — 内存信息采集 `MemoryCommand.memoryInfo()`**：

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/MemoryCommand.java`

```java
static Map<String, List<MemoryEntryVO>> memoryInfo() {
    List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
    Map<String, List<MemoryEntryVO>> memoryInfoMap = new LinkedHashMap<String, List<MemoryEntryVO>>();

    // 堆内存总览
    MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    List<MemoryEntryVO> heapMemEntries = new ArrayList<MemoryEntryVO>();
    heapMemEntries.add(createMemoryEntryVO(TYPE_HEAP, TYPE_HEAP, heapMemoryUsage));
    // 遍历各内存区域（Eden, Survivor, Old, Metaspace, Code Cache...）
    for (MemoryPoolMXBean poolMXBean : memoryPoolMXBeans) {
        if (poolMXBean.getType() == MemoryType.HEAP) {
            heapMemEntries.add(createMemoryEntryVO(
                TYPE_HEAP, poolMXBean.getName(), poolMXBean.getUsage()));
        }
    }
    memoryInfoMap.put(TYPE_HEAP, heapMemEntries);

    // 非堆内存总览
    MemoryUsage nonHeapMemoryUsage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
    // ... 类似逻辑处理非堆
    return memoryInfoMap;
}
```

关键点分析：

- `ManagementFactory.getMemoryPoolMXBeans()`：返回 JVM 所有内存池的 MXBean 列表。不同垃圾收集器有不同的内存区域划分（如 G1 是 Eden+Survivor+Old+Humongous，CMS 是 Eden+Survivor+Old+Perm/Metaspace）。
- `MemoryUsage` 包含四个关键值：`init`（初始值）、`used`（已用）、`committed`（已提交）、`max`（最大值）。dashboard 面板上显示的 `used/max` 就是百分比。

**第三块 — GC 信息采集**：

```java
List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
    // gcMXBean.getName() 可能是 "G1 Young Generation"、"G1 Old Generation" 等
    gcVO.setGcCount(gcMXBean.getCollectionCount());    // GC 次数
    gcVO.setGcTime(gcMXBean.getCollectionTime());       // GC 总耗时（ms）
}
```

关键点分析：

- JVM 通常有 2 个 GC MXBean：一个 Young GC、一个 Full/Old GC。通过 `getCollectionCount()` 和 `getCollectionTime()` 可以看到累计的 GC 次数和耗时。dashboard 的增量计算逻辑：两次采样之间 GC 次数的差值即为该时间段内发生了多少次 GC。

**第四块 — 组装 DashboardModel 并推送渲染**：

```java
DashboardModel dashboardModel = new DashboardModel();
dashboardModel.setThreadVOList(threadVOList);
dashboardModel.setMemoryInfoMap(memoryInfoMap);
dashboardModel.setGarbageCollectors(garbageCollectors);
process.appendResult(dashboardModel);
```

- `process.appendResult(dashboardModel)`：将模型对象推送到 `CommandProcess` 的结果队列中。`ResultWatcher` 监听到新结果后触发 `DashboardView.render()`，将模型渲染为终端表格输出。
- 这里的设计很关键：**数据采集和视图渲染是解耦的**。`DashboardTimerTask` 只负责采集数据组装 Model，`DashboardView` 负责渲染。这使得同一份 Model 可以渲染成不同格式（终端表格、JSON、HTML）。

### 1.6 ThreadCommand.process() → processTopBusyThreads() —— 两次采样原理

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/ThreadCommand.java`

当用户输入 `thread -n 3` 时，Arthas 需要找出 CPU 使用率最高的 3 个线程。核心挑战是：**如何计算一个线程的 CPU 使用率？**

JVM 提供的 `ThreadMXBean.getThreadCpuTime(threadId)` 返回的是该线程从启动到现在的 CPU 总时间（纳秒），这是一个**单调递增的累计值**。要得到瞬时 CPU 使用率，必须采用**两次采样法**：在时间点 T1 记录 CPU 时间，等待 interval 后在 T2 再次记录，差值除以 interval 就是该时间段的 CPU 使用率。

```java
@Name("thread")
@Summary("Display thread info, thread stack")
@Description(Constants.EXAMPLE ...)
public class ThreadCommand extends AnnotatedCommand {

    @Option(shortName = "n", longName = "top-n-threads")
    @Description("The number of thread(s) that has top CPU time usage")
    private Integer topN;

    @Option(shortName = "i", longName = "interval")
    @Description("Interval in milliseconds for sampling thread CPU time. Default: 200ms")
    private long interval = 200L;

    @Option(longName = "lockedMonitors")
    @Description("Show locked monitors. This is a performance penalty operation.")
    private boolean lockedMonitors = false;

    @Option(longName = "lockedSynchronizers")
    @Description("Show locked synchronizers. This is a performance penalty operation.")
    private boolean lockedSynchronizers = false;
```

**参数解析**：

- `topN`：当 `-n` 参数存在时，进入 `processTopBusyThreads()` 路径。当不存在且指定了 threadId 时，进入 `processThread()` 路径。
- `interval = 200L`：默认采样间隔 200ms。200ms 足够短以保证时效性，又足够长以减少误差。
- `lockedMonitors / lockedSynchronizers`：是否获取线程持有的锁信息。这两个操作有性能损耗，所以默认关闭。

```java
    @Override
    public void process(CommandProcess process) {
        if (topN != null) {
            // 有 -n 参数，找出 CPU 最高的 N 个线程
            processTopBusyThreads(process);
        } else if (id != null) {
            // 有 thread id 参数，查看指定线程堆栈
            processThread(process, id);
        } else {
            // 无参数，列出所有线程
            processAllThread(process);
        }
    }
```

接下来重点分析 `processTopBusyThreads()`：

```java
    private void processTopBusyThreads(CommandProcess process) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        // 确保 CPU 时间测量已启用
        threadMXBean.setThreadCpuTimeEnabled(true);

        // 第一次采样：获取所有线程 ID 和 CPU 时间
        long initialTime = System.nanoTime();
        ThreadInfo[] initialThreadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 0);
        Map<Long, Long> initialThreadCpuTimeMap = new HashMap<Long, Long>();
        for (ThreadInfo info : initialThreadInfos) {
            if (info != null) {
                initialThreadCpuTimeMap.put(info.getThreadId(),
                    threadMXBean.getThreadCpuTime(info.getThreadId()));
            }
        }

        // 等待 interval（默认 200ms）
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            // ignore
        }

        // 第二次采样
        long afterTime = System.nanoTime();
        ThreadInfo[] afterThreadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 0);
        List<ThreadVO> threadVOList = new ArrayList<ThreadVO>();
        for (ThreadInfo info : afterThreadInfos) {
            if (info != null) {
                long threadId = info.getThreadId();
                Long initialCpuTime = initialThreadCpuTimeMap.get(threadId);
                if (initialCpuTime != null) {
                    long afterCpuTime = threadMXBean.getThreadCpuTime(threadId);
                    // CPU 使用率 = (第二次CPU时间 - 第一次CPU时间) / 采样间隔
                    double cpu = (double) (afterCpuTime - initialCpuTime)
                        / (double) (afterTime - initialTime);
                    ThreadVO vo = ThreadUtil.createThreadVO(info);
                    vo.setCpu(cpu);
                    threadVOList.add(vo);
                }
            }
        }

        // 按 CPU 使用率降序排序
        Collections.sort(threadVOList, new Comparator<ThreadVO>() {
            @Override
            public int compare(ThreadVO o1, ThreadVO o2) {
                return Double.compare(o2.getCpu(), o1.getCpu());
            }
        });

        // 取 TopN
        List<ThreadVO> topNThreadList = new ArrayList<ThreadVO>();
        for (int i = 0; i < threadVOList.size() && i < topN; i++) {
            ThreadVO vo = threadVOList.get(i);
            // 获取该线程的完整堆栈（depth = Integer.MAX_VALUE）
            ThreadInfo threadInfo = threadMXBean.getThreadInfo(vo.getId(), Integer.MAX_VALUE);
            vo.setStackTrace(threadInfo.getStackTrace());
            topNThreadList.add(vo);
        }

        // 构建结果模型并输出
        ThreadModel threadModel = new ThreadModel(topNThreadList);
        process.appendResult(threadModel);
        process.end();
    }
```

**逐行深度解释**：

1. `threadMXBean.setThreadCpuTimeEnabled(true)`：必须显式启用 CPU 时间测量。某些 JVM 实现默认关闭以减少开销。这一步是前置条件。

2. **第一次采样**：`threadMXBean.getAllThreadIds()` 获取 JVM 中所有活线程 ID，`getThreadInfo(ids, 0)` 获取基本信息（堆栈深度为 0），然后对每个线程调用 `getThreadCpuTime()` 记录初始 CPU 时间。

3. `Thread.sleep(interval)`：休眠 200ms，让线程在这段时间内执行，积累 CPU 时间。

4. **第二次采样**：再次获取所有线程信息，并读取当前 CPU 时间。

5. **CPU 使用率计算公式**：
   ```
   cpu% = (afterCpuTime - initialCpuTime) / (afterTime - initialTime)
   ```
   其中 `afterTime - initialTime` 是实际经过的纳秒时间（注意这里用 `System.nanoTime()` 而非 `System.currentTimeMillis()`，因为 nanoTime 单调递增，不受时钟回拨影响）。

   例如：某线程第一次采样时 CPU 时间 = 1000ms（10^9 ns），200ms 后第二次采样 CPU 时间 = 1180ms，实际经过 = 200ms，则 CPU% = (1180-1000)/200 = 90%。

   注意：这里没有除以 CPU 核数，所以单个线程最高可以到 100%（不是 1/core*100%）。如果线程跑满一个核，CPU 就是 100%。多核场景下如果某个线程使用了多个核（极少数情况），可能超过 100%。

6. **排序并取 TopN**：`Double.compare(o2.getCpu(), o1.getCpu())` 是降序排列，`o2` 在前表示降序。

7. **获取完整堆栈**：对 TopN 线程调用 `getThreadInfo(vo.getId(), Integer.MAX_VALUE)` 获取完整堆栈，这样用户可以直接看到线程在执行什么代码。

### 1.7 ThreadCommand.process() → processThread() —— 查看指定线程堆栈

当用户拿到 `thread -n 3` 输出的线程 ID 后，执行 `thread <id>` 查看完整堆栈：

```java
    private void processThread(CommandProcess process, long threadId) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        // 获取完整线程信息，包括锁
        ThreadInfo threadInfo = threadMXBean.getThreadInfo(
            threadId,
            lockedMonitors ? Integer.MAX_VALUE : 0,  // 堆栈深度
            lockedMonitors                            // 是否获取 lockedMonitors
                ? Integer.MAX_VALUE : 0,
            lockedSynchronizers                      // 是否获取 lockedSynchronizers
                ? Integer.MAX_VALUE : 0
        );

        if (threadInfo == null) {
            process.end(1, "thread does not exist: " + threadId);
            return;
        }

        ThreadVO threadVO = ThreadUtil.createThreadVO(threadInfo);
        threadVO.setCpu(threadMXBean.getThreadCpuTime(threadId));
        // 设置完整的堆栈跟踪
        threadVO.setStackTrace(threadInfo.getStackTrace());

        // 如果开启了 lockedMonitors，设置锁信息
        if (lockedMonitors) {
            MonitorInfo[] monitors = threadInfo.getLockedMonitors();
            // ...
        }

        // 如果开启了 lockedSynchronizers，设置 synchronizer 信息
        if (lockedSynchronizers) {
            LockInfo[] synchronizers = threadInfo.getLockedSynchronizers();
            // ...
        }

        List<ThreadVO> threadVOList = new ArrayList<ThreadVO>(1);
        threadVOList.add(threadVO);
        ThreadModel threadModel = new ThreadModel(threadVOList);
        process.appendResult(threadModel);
        process.end();
    }
```

**关键设计**：

- `lockedMonitors` 和 `lockedSynchronizers` 参数会传递到 `getThreadInfo()` 的对应参数。JVM 底层在获取这些信息时需要遍历锁记录，开销较大，所以默认关闭。这解释了为什么 `@Description` 中标注 "This is a performance penalty operation"。
- `threadInfo.getLockedMonitors()`：返回该线程持有的所有 `synchronized` 锁信息。
- `threadInfo.getLockedSynchronizers()`：返回该线程持有的所有 `ReentrantLock` 等 JUC synchronizer 信息。
- 当排查死锁时，这两个参数非常有用——可以看到线程持有哪些锁、在等待哪些锁。

### 1.8 ThreadSampler 的采样原理补充

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/ThreadSampler.java`

`ThreadSampler` 是 Arthas 内部的线程采样器，被 `dashboard` 和 `thread` 命令共享。其核心逻辑也是基于两次采样：

```java
public class ThreadSampler {
    private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    static {
        // 启用 CPU 时间测量
        threadMXBean.setThreadCpuTimeEnabled(true);
        // 启用 contention 监测（可选，用于 thread --state WAITING 等）
        threadMXBean.setThreadContentionMonitoringEnabled(true);
    }

    /**
     * 采样一次线程 CPU 使用情况
     */
    public static List<ThreadVO> sampleThreads(long intervalMs) {
        // 第一次采样
        Map<Long, Long> firstCpuTimeMap = new HashMap<>();
        long[] threadIds = threadMXBean.getAllThreadIds();
        ThreadInfo[] firstInfos = threadMXBean.getThreadInfo(threadIds, 0);
        for (ThreadInfo info : firstInfos) {
            if (info != null) {
                firstCpuTimeMap.put(info.getThreadId(),
                    threadMXBean.getThreadCpuTime(info.getThreadId()));
            }
        }

        long startTime = System.nanoTime();
        try {
            Thread.sleep(intervalMs);
        } catch (InterruptedException e) {
            // ignore
        }
        long endTime = System.nanoTime();

        // 第二次采样
        List<ThreadVO> result = new ArrayList<>();
        ThreadInfo[] secondInfos = threadMXBean.getThreadInfo(threadIds, 0);
        for (ThreadInfo info : secondInfos) {
            if (info != null) {
                Long firstCpu = firstCpuTimeMap.get(info.getThreadId());
                if (firstCpu != null) {
                    long secondCpu = threadMXBean.getThreadCpuTime(info.getThreadId());
                    double cpu = (double)(secondCpu - firstCpu) / (double)(endTime - startTime);
                    ThreadVO vo = ThreadUtil.createThreadVO(info);
                    vo.setCpu(cpu);
                    result.add(vo);
                }
            }
        }
        return result;
    }
}
```

### 1.9 为什么需要两次采样？—— 原理深究

**Q: 为什么不能直接调用一次 `getThreadCpuTime()` 就得到 CPU 使用率？**

A: `getThreadCpuTime()` 返回的是该线程从 JVM 启动到现在的**累计** CPU 时间。例如一个线程运行了 10 分钟，累计 CPU 时间可能是 300 秒（说明平均使用了 50% CPU），但无法反映**当前**这一刻的 CPU 使用率。可能是前 5 分钟疯狂使用 CPU（90%），后 5 分钟完全空闲（0%），但累计值不变。

两次采样法本质上是在测量一个**窗口期**内的 CPU 增量，从而得到这段时间的平均 CPU 使用率：

```
CPU% = ΔCPU时间 / Δ墙钟时间
```

这和 Linux `top` 命令的原理一致——`top` 也是通过两次读取 `/proc/stat` 的差值来计算 CPU 使用率。

### 1.10 Q&A 设计问题分析

**Q1: dashboard 的 Timer 为什么用 daemon 线程？如果不用 daemon 会怎样？**

如果 Timer 使用非 daemon 线程，当用户退出 Arthas 时，Timer 线程不会被 JVM 自动终止。用户需要显式调用 `timer.cancel()` 来停止定时任务。而 daemon 线程在 JVM 退出时会自动终止，不会阻止 JVM 正常关闭。Arthas 作为诊断工具附加到目标 JVM 上，必须保证自身资源可以干净退出，不能因为 Arthas 的存在阻止应用的正常关闭。

**Q2: thread -n 3 的 200ms 采样间隔为什么不能太短也不能太长？**

太短（如 10ms）：两次采样之间的 CPU 时间增量极小，可能导致除法精度问题；同时 `getThreadInfo()` 本身有 JNI 开销，频繁调用会影响测量准确性。

太长（如 5000ms）：虽然精度高，但用户体验差——用户要等 5 秒才能看到结果。200ms 是一个经过实践验证的平衡点：足够长以收集有意义的 CPU 增量，又足够短以快速返回结果。

**Q3: `getThreadInfo(ids, 0)` 中堆栈深度为 0 的原因是什么？在什么场景下需要获取完整堆栈？**

`getThreadInfo` 的第二个参数是堆栈深度。传 0 表示不获取堆栈，只获取线程名称、状态、ID 等元数据。这在 dashboard 总览和 thread -n 采样阶段使用，因为此时只需要线程状态和 CPU 时间，获取堆栈会有额外的 JNI 调用开销。

在确定了 TopN 线程后，才用 `getThreadInfo(id, Integer.MAX_VALUE)` 获取完整堆栈。这是经典的**两阶段策略**：第一阶段广度扫描（代价低），第二阶段深度分析（代价高但只对少量目标）。

**Q4: 为什么 thread 命令要先调用 `setThreadCpuTimeEnabled(true)`？**

JVM 的 CPU 时间测量默认可能是关闭的（取决于 JVM 实现和启动参数）。如果不显式启用，`getThreadCpuTime()` 可能返回 -1。Arthas 在每次执行 thread 命令时都会显式启用，确保测量功能可用。

**Q5: dashboard 和 thread -n 的 CPU 数据是否一致？**

原理一致（都是两次采样），但时间窗口不同。dashboard 的 interval 默认 5000ms，thread -n 默认 200ms。因此在同一时刻执行两个命令，看到的 CPU% 可能不同——dashboard 反映的是 5 秒窗口的平均值，thread -n 反映的是 200ms 窗口的瞬时值。排查突发 CPU 飙高时，thread -n 的 200ms 窗口更灵敏。

### 1.11 场景总结

| 维度 | dashboard | thread -n N | thread <id> |
|------|-----------|-------------|-------------|
| 数据来源 | JMX (ThreadMXBean, MemoryMXBean, GC MXBean) | JMX (ThreadMXBean) | JMX (ThreadMXBean) |
| 采样方式 | Timer 定时执行，持续刷新 | 两次采样（200ms 窗口） | 单次获取 |
| 堆栈深度 | 0（不获取堆栈） | 0（采样阶段）→ MAX_VALUE（TopN 阶段） | MAX_VALUE（完整堆栈） |
| 性能开销 | 低（元数据采集） | 中（200ms sleep + 两次全量采样） | 低（单线程查询） |
| 适用场景 | 总览全局健康度 | 定位 CPU 飙高的具体线程 | 查看线程正在执行的代码 |

完整排查链路：`dashboard`（全局视角，确认 CPU 是否真的高）→ `thread -n 3`（定位具体线程）→ `thread <id>`（查看堆栈，定位到具体代码行）。三步形成一条从宏观到微观的完整诊断链路。

---

## 场景二：线上接口响应慢排查 —— trace + watch 联合定位

### 2.1 用户故事

某电商系统用户反馈下单接口 `createOrder` 的响应时间从 50ms 涨到了 2s。应用没有报错，监控只看到 P99 延迟升高，但不知道是哪个子调用变慢了。可能的怀疑对象包括：数据库查询、缓存失效、第三方支付接口、序列化等。

开发同学需要回答两个问题：
1. 在 `createOrder` 方法内部，哪个子调用耗时最长？
2. 慢调用发生时，入参和返回值是什么？

### 2.2 操作命令与参数说明

```bash
# 第一步：追踪方法调用链路耗时，找出最慢的子调用
trace com.example.OrderService createOrder -n 5

# 第二步：观察慢调用时的参数和返回值
watch com.example.OrderService createOrder "{params, returnObj}" "params[0].userId == 12345" -x 2 -n 3

# 第三步：如果是子方法慢，watch 子方法的具体调用
watch com.example.PaymentService pay "{params, returnObj, #cost}" -x 2 -n 3
```

**参数说明**：

| 命令 | 参数 | 说明 |
|------|------|------|
| `trace` | `class-pattern` | 类名匹配模式，支持通配符 |
| `trace` | `method-pattern` | 方法名匹配模式 |
| `trace` | `-n <num>` | 执行次数限制 |
| `trace` | `--skipJDKMethod` | 跳过 JDK 方法（java.** 下的方法不追踪） |
| `trace` | `-p <path>` | 指定调用路径，只追踪特定子调用链 |
| `trace` | `#cost > 100` | 条件表达式，只展示耗时 > 100ms 的调用 |
| `watch` | `{params, returnObj}` | 观察表达式，指定要输出的内容 |
| `watch` | `condition-express` | 条件表达式，如 `params[0].userId == 12345` |
| `watch` | `-x <depth>` | 递归展开深度 |
| `watch` | `-n <num>` | 执行次数限制 |

### 2.3 从命令输入到结果输出的完整链路

```
ShellLineHandler.handle("trace com.example.OrderService createOrder -n 5")
  → CommandLine parsing → Job creation → ProcessImpl.run
  → AnnotatedCommandImpl.process → EnhancerCommand.process
    → EnhancerCommand.enhance()                              // 字节码增强入口
      → SearchUtils.searchClass(class-pattern)               // 搜索匹配的类
      → Enhancer enhancer = new Enhancer(...)                // 创建增强器
      → enhancer.enhance(inst, matcher, listener)            // 执行增强
        → Instrumentation.addTransformer(enhancer)            // 注册 ClassFileTransformer
        → Instrumentation.retransformClasses(matchedClasses) // 触发 retransform
          → Enhancer.transform(classfileBuffer)              // 字节码改写
            → SpyTraceInterceptor1/2/3 织入                  // 在方法入口/出口/异常处织入 Spy 调用
        → Instrumentation.removeTransformer(enhancer)         // 增强后移除 transformer

// 运行时（目标方法被调用时）：
目标方法调用
  → SpyAPI.atEnter(...)                                      // 方法入口
  → SpyImpl.atEnter(...)
  → AdviceListenerManager.queryAdviceListeners(classLoader, className)
  → TraceAdviceListener.before(...)
    → tree.begin(className, methodName)                      // 调用树开始

子方法调用
  → SpyAPI.atBeforeInvoke(...)                               // 子方法调用前
  → SpyImpl.atBeforeInvoke(...)
  → AdviceListenerManager.queryTraceAdviceListeners(...)
  → InvokeTraceable.invokeBeforeTracing(className, methodName)
    → deep++（递增深度计数器）

子方法返回
  → SpyAPI.atAfterInvoke(...)                                // 子方法调用后
  → InvokeTraceable.invokeAfterTracing(className, methodName, throwable)
    → deep--（递减深度计数器）

目标方法返回
  → SpyAPI.atExit(...)                                       // 方法出口
  → TraceAdviceListener.afterReturning(...)
    → tree.end()                                              // 调用树结束
    → finishing()  (when deep == 0)
      → process.appendResult(traceModel)                      // 输出调用树
```

### 2.4 TraceCommand 参数解析与监听器选择

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TraceCommand.java`

```java
@Name("trace")
@Summary("Trace the execution time of specified method invocation.")
public class TraceCommand extends EnhancerCommand {

    @Argument(index = 0, argName = "class-pattern")
    @Description("Path and classname of Pattern Matching")
    private String classPattern;

    @Argument(index = 1, argName = "method-pattern")
    @Description("Method of Pattern Matching")
    private String methodPattern;

    @Option(shortName = "n", longName = "limits")
    @Description("Maximum number of matching traces")
    private Integer numberOfTraceLimit;

    @Option(shortName = "p", longName = "path")
    @Description("trace root path, it can be class-name/method-name")
    private String path;

    @Option(longName = "skipJDKMethod")
    @Description("skip jdk method trace")
    private boolean skipJDKMethod = false;
```

**关键方法 `getAdviceListenerWith необходимых`**：

```java
    @Override
    protected AdviceListener getAdviceListener(CommandProcess process) {
        TraceAdviceListener listener;
        if (path != null) {
            // 有 -p 参数，使用 PathTraceAdviceListener
            // 只追踪指定的调用路径
            listener = new PathTraceAdviceListener(this, process, path);
        } else {
            // 无 -p 参数，使用 TraceAdviceListener
            // 追踪方法内部所有子调用
            listener = new TraceAdviceListener(this, process);
        }
        return listener;
    }
```

**设计意图**：

- `TraceAdviceListener`（无 path）：追踪目标方法内部**所有**子调用，输出完整的调用树。
- `PathTraceAdviceListener`（有 path）：只追踪到达指定路径的调用链。例如 `-p com.example.PaymentService.pay` 表示只显示到达 `PaymentService.pay` 这条路径上的调用，其他分支不展示。这在调用树很大时非常有用——可以只关注可疑路径。

### 2.5 EnhancerCommand.process() → enhance() —— 字节码增强入口

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/EnhancerCommand.java`

```java
public abstract class EnhancerCommand extends AnnotatedCommand {

    @Override
    public void process(CommandProcess process) {
        // 获取 AdviceListener
        AdviceListener listener = getAdviceListener(process);
        if (listener == null) {
            process.end(1, "no advice listener found");
            return;
        }

        // 执行增强
        enhance(process, listener);
    }

    protected void enhance(CommandProcess process, AdviceListener listener) {
        // 搜索匹配的类
        Set<Class<?>> matchedClasses = SearchUtils.searchClass(
            inst, classPattern, isRegex, code);

        if (matchedClasses.isEmpty()) {
            process.end(1, "no class found for: " + classPattern);
            return;
        }

        if (matchedClasses.size() > 1) {
            // 多个匹配，提示用户精确指定
            process.end(1, "matched " + matchedClasses.size() + " classes, "
                + "please specify with -c <classLoaderHash>");
            return;
        }

        Class<?> clazz = matchedClasses.iterator().next();

        // 创建 Enhancer
        Enhancer enhancer = new Enhancer(listener, this.classNamePattern,
            this.methodNamePattern, this.isTracing);

        try {
            // 注册 transformer
            inst.addTransformer(enhancer, true);
            // 触发 retransform，JVM 会重新加载类的字节码
            // 此时 Enhancer.transform() 被调用，织入 Spy 代码
            InstrumentationUtils.retransformClasses(inst, clazz);
        } finally {
            // 增强完成后移除 transformer，避免影响后续类加载
            inst.removeTransformer(enhancer);
        }
    }
```

**逐行解释**：

1. `SearchUtils.searchClass(inst, classPattern, isRegex, code)`：通过 `Instrumentation.getAllLoadedClasses()` 遍历所有已加载的类，用类名模式匹配。`code` 参数是 ClassLoader 的 hashCode，用于在多 ClassLoader 环境下精确匹配。

2. `new Enhancer(listener, classNamePattern, methodNamePattern, isTracing)`：创建字节码增强器。`isTracing` 标记是否为 trace 模式（trace 模式需要追踪子调用，watch 模式只需要观察目标方法本身）。

3. `inst.addTransformer(enhancer, true)`：注册 `Enhancer` 为 `ClassFileTransformer`。第二个参数 `true` 表示该 transformer 可以被 retransform 触发。

4. `InstrumentationUtils.retransformClasses(inst, clazz)`：触发 JVM 对指定类重新加载字节码。JVM 会调用已注册的所有 `ClassFileTransformer.transform()` 方法，其中就包括刚注册的 `Enhancer`。

5. `inst.removeTransformer(enhancer)`：在 finally 块中移除 transformer。这是**一次性增强**策略——只在 retransform 触发时增强一次，之后新加载的类不会被增强。如果需要取消增强，再次 retransform 即可（因为 transformer 已移除，原始字节码会被恢复）。

### 2.6 Enhancer.transform() —— 字节码织入 Spy 代码

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/advisor/Enhancer.java`

`Enhancer` 实现了 `ClassFileTransformer` 接口，核心方法是 `transform()`：

```java
public class Enhancer implements ClassFileTransformer {

    @Override
    public byte[] transform(ClassLoader loader, String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer) {

        // 判断是否需要增强此类
        if (!classNameMatcher.match(className.replace('/', '.'))) {
            return null; // 不匹配，返回 null 表示不修改
        }

        try {
            // 使用 ASM 修改字节码
            ClassReader cr = new ClassReader(classfileBuffer);
            ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);

            // 自定义 ClassVisitor，遍历字节码并织入 Spy 调用
            ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {

                @Override
                public MethodVisitor visitMethod(int access, String name,
                        String desc, String signature, String[] exceptions) {

                    MethodVisitor mv = super.visitMethod(access, name, desc,
                        signature, exceptions);

                    // 判断是否需要增强该方法
                    if (!methodNameMatcher.match(name)) {
                        return mv; // 不匹配的方法不增强
                    }

                    // 根据模式选择不同的拦截器
                    if (isTracing) {
                        // trace 模式：在方法入口/出口/每个子调用前后织入
                        mv = new AdviceWeaver(...,
                            SpyTraceInterceptor1.getInstance(),
                            SpyTraceInterceptor2.getInstance(),
                            SpyTraceInterceptor3.getInstance(),
                            spyInterceptorOnJdkMethod);
                    } else {
                        // watch 模式：只在目标方法入口/出口织入
                        mv = new AdviceWeaver(...,
                            SpyInteractionInterceptor1.getInstance(),
                            SpyInteractionInterceptor2.getInstance(),
                            SpyInteractionInterceptor3.getInstance(),
                            spyInterceptorOnJdkMethod);
                    }
                    return mv;
                }
            };

            cr.accept(cv, ClassReader.EXPAND_FRAMES);
            return cw.toByteArray();

        } catch (Throwable e) {
            // 增强失败，返回 null，不修改原始字节码
            return null;
        }
    }
}
```

**Trace 模式的三种 SpyTraceInterceptor**：

Arthas 使用三个不同的拦截器，分别对应方法生命周期的三个关键点：

| 拦截器 | 织入位置 | 对应 SpyAPI 方法 | 触发时机 |
|--------|---------|------------------|---------|
| `SpyTraceInterceptor1` | 方法入口 | `SpyAPI.atEnter()` | 方法刚进入时 |
| `SpyTraceInterceptor2` | 方法出口 | `SpyAPI.atExit()` | 方法正常返回时 |
| `SpyTraceInterceptor3` | 异常出口 | `SpyAPI.atExceptionExit()` | 方法抛出异常时 |

此外，trace 模式还有一个关键拦截器 `SpyTraceExcludeJDKInterceptor`：

```java
// 当 --skipJDKMethod 为 true 时
if (skipJDKMethod) {
    // 在每个 INVOKE 指令前后织入
    // 但只对非 java.** 类的方法织入
    // java.** 的方法直接跳过，不追踪
}
```

**`@AtInvoke` 注解织入原理**：

Arthas 通过自定义的 `AdviceWeaver`（继承 ASM 的 `MethodVisitor`），在遍历方法字节码时，遇到 `INVOKEVIRTUAL`、`INVOKESTATIC`、`INVOKESPECIAL`、`INVOKEINTERFACE` 指令时，在这些指令**前后**分别插入方法调用：

```java
// 伪代码：AdviceWeaver 中对 INVOKE 指令的处理
@Override
public void visitMethodInsn(int opcode, String owner, String name,
        String descriptor, boolean isInterface) {

    // 在 INVOKE 指令之前织入 atBeforeInvoke
    // 等价于：SpyAPI.atBeforeInvoke(...)
    super.visitMethodInsn(INVOKESTATIC, "com/taobao/arthas/core/advisor/SpyAPI",
        "atBeforeInvoke", "(Ljava/lang/String;Ljava/lang/String;)V");

    // 原始的 INVOKE 指令
    super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);

    // 在 INVOKE 指令之后织入 atAfterInvoke
    // 等价于：SpyAPI.atAfterInvoke(...)
    super.visitMethodInsn(INVOKESTATIC, "com/taobao/arthas/core/advisor/SpyAPI",
        "atAfterInvoke", "(Ljava/lang/String;Ljava/lang/String;)V");
}
```

这就是 trace 命令能展示方法内部每个子调用耗时的核心——通过字节码增强，在**每个方法调用指令**前后都插入了对 Spy 的回调。

### 2.7 运行时回调链路 —— SpyAPI → TraceAdviceListener

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/advisor/SpyImpl.java`

当增强后的方法在运行时被调用，JVM 执行织入的 Spy 代码，触发以下链路：

```java
public class SpyImpl {

    /**
     * 方法入口回调（由 SpyTraceInterceptor1 触发）
     */
    public void atEnter(Class<?> clazz, String methodName, String methodDesc,
            Object target, Object[] args) {
        ClassLoader classLoader = clazz.getClassLoader();
        String className = clazz.getName();

        // 查找注册的 AdviceListener
        List<AdviceListener> listeners = AdviceListenerManager.queryAdviceListeners(
            classLoader, className);
        if (listeners != null) {
            for (AdviceListener listener : listeners) {
                // 构造 Advice 对象，封装目标对象、参数等信息
                Advice advice = new Advice(clazz, methodName, methodDesc,
                    target, args, classLoader);
                listener.before(advice);
            }
        }
    }

    /**
     * 子方法调用前回调（由 @AtInvoke 织入的 SpyTraceInterceptor 触发）
     */
    public void atBeforeInvoke(Class<?> clazz, String methodName,
            String methodDesc) {
        ClassLoader classLoader = clazz.getClassLoader();
        String className = clazz.getName();

        // 查找实现了 InvokeTracing 接口的 listener
        List<AdviceListener> listeners = AdviceListenerManager.queryTraceAdviceListeners(
            classLoader, className);
        if (listeners != null) {
            for (AdviceListener listener : listeners) {
                if (listener instanceof InvokeTraceable) {
                    ((InvokeTraceable) listener).invokeBeforeTracing(
                        className, methodName, methodDesc, false);
                }
            }
        }
    }

    /**
     * 子方法调用后回调
     */
    public void atAfterInvoke(Class<?> clazz, String methodName,
            String methodDesc) {
        // 类似 atBeforeInvoke，调用 invokeAfterTracing
        // ...
    }

    /**
     * 方法正常返回回调（由 SpyTraceInterceptor2 触发）
     */
    public void atExit(Class<?> clazz, String methodName, String methodDesc,
            Object target, Object[] args, Object returnObject) {
        ClassLoader classLoader = clazz.getClassLoader();
        String className = clazz.getName();

        List<AdviceListener> listeners = AdviceListenerManager.queryAdviceListeners(
            classLoader, className);
        if (listeners != null) {
            for (AdviceListener listener : listeners) {
                Advice advice = new Advice(clazz, methodName, methodDesc,
                    target, args, returnObject, classLoader);
                listener.afterReturning(advice);
            }
        }
    }

    /**
     * 方法异常返回回调（由 SpyTraceInterceptor3 触发）
     */
    public void atExceptionExit(Class<?> clazz, String methodName,
            String methodDesc, Object target, Object[] args, Throwable throwable) {
        // 构造 Advice，调用 afterThrowing
        // ...
    }
}
```

**关键设计 `AdviceListenerManager`**：

`AdviceListenerManager` 是一个以 `ClassLoader + className` 为 key 的监听器注册表。当 `enhance()` 执行时，会将 `AdviceListener` 注册到这个 Map 中。运行时 Spy 代码通过 `ClassLoader` 和 `className` 查找到对应的 listener 并回调。

为什么用 `ClassLoader + className` 作为 key？因为在多 ClassLoader 环境下（如 Tomcat 的 WebAppClassLoader），同一个类名可以被不同的 ClassLoader 加载。必须用 ClassLoader 来区分，否则会回调到错误的 listener。

### 2.8 TraceAdviceListener —— 调用树构建与输出

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TraceAdviceListener.java`

```java
public class TraceAdviceListener extends AbstractTraceAdviceListener {

    // 调用树
    private TraceEntity tree;

    // 深度计数器
    private AtomicInteger deep = new AtomicInteger(0);

    @Override
    public void before(Advice advice) {
        // 目标方法入口：开始调用树
        String className = advice.getClassName();
        String methodName = advice.getMethodName();

        // 创建根节点
        tree = new TraceEntity(className, methodName, advice.hashCode());
        tree.begin(className, methodName);
        deep.set(0);
    }

    @Override
    public void invokeBeforeTracing(String className, String methodName,
            String methodDesc, boolean isThrow) {
        // 子方法调用前：深度 +1
        deep.incrementAndGet();
    }

    @Override
    public void invokeAfterTracing(String className, String methodName,
            String methodDesc, boolean isThrow) {
        // 子方法调用后：深度 -1
        deep.decrementAndGet();
    }

    @Override
    public void afterReturning(Advice advice) {
        // 目标方法正常返回
        tree.end();
        finishing(advice);
    }

    @Override
    public void afterThrowing(Advice advice) {
        // 目标方法异常返回
        tree.end();
        finishing(advice);
    }

    private void finishing(Advice advice) {
        // 只有当 deep == 0 时，才表示整棵调用树完成
        // 可以输出结果
        if (deep.get() == 0) {
            // 检查条件表达式（如 #cost > 100）
            // 如果条件不满足，不输出
            if (!isConditionMet(conditionExpress, advice, advice.getCost())) {
                return;
            }

            // 构建结果模型
            TraceModel traceModel = new TraceModel();
            traceModel.setTraceTree(tree);
            process.appendResult(traceModel);

            // 递增次数计数器
            AtomicInteger count = getCount();
            count.incrementAndGet();

            // 检查是否达到最大执行次数
            if (numberOfTraceLimit != null && count.get() >= numberOfTraceLimit) {
                process.end();
            }
        }
    }
}
```

**`AbstractTraceAdviceListener` 基类提供通用能力**：

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/AbstractTraceAdviceListener.java`

```java
public abstract class AbstractTraceAdviceListener extends AdviceListenerAdapter {

    protected boolean isConditionMet(String conditionExpress, Advice advice, double cost) {
        // 如果没有条件表达式，总是满足
        if (StringUtils.isEmpty(conditionExpress)) {
            return true;
        }
        try {
            // 使用 OGNL 执行条件表达式
            Object value = ExpressFactory.threadLocalExpress(advice.classLoader())
                .bind(advice).get(conditionExpress);
            return value instanceof Boolean ? (Boolean) value : false;
        } catch (Exception e) {
            return false;
        }
    }
}
```

**调用树 `TraceEntity` 的数据结构**：

```
`---[0ms] com.example.OrderService:createOrder()
    +---[5ms] com.example.UserService:getUserInfo()   // deep=1
    +---[1500ms] com.example.PaymentService:pay()      // deep=1 ← 最慢！
    |   +---[1490ms] com.example.HttpClient:post()     // deep=2 ← 真正的瓶颈
    |   `---[8ms] com.example.Logger:log()             // deep=2
    `---[3ms] com.example.OrderDao:save()              // deep=1
```

`tree.begin()` 创建根节点，每次 `invokeBeforeTracing` 被调用时 deep 递增（对应一个子方法调用），`invokeAfterTracing` 时 deep 递减。当子方法中又有子方法调用时，deep 继续递增，形成树状结构。每次调用结束时会记录耗时（通过 `System.nanoTime()` 在 Spy 织入代码中计算）。

**为什么用 `AtomicInteger` 而非 `int`？**

`TraceAdviceListener` 的回调可能在不同线程中触发（如果目标方法涉及多线程），使用 `AtomicInteger` 保证 deep 计数器的线程安全。

### 2.9 WatchCommand 的条件表达式执行

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/WatchAdviceListener.java`

当用户执行 `watch com.example.OrderService createOrder "{params, returnObj}" "params[0].userId == 12345" -x 2 -n 3` 时，`WatchAdviceListener` 负责在方法返回后执行条件判断和结果提取：

```java
public class WatchAdviceListener extends AdviceListenerAdapter {

    @Override
    public void afterReturning(Advice advice) {
        // 计算耗时
        double cost = advice.getCost();

        // 第一步：检查条件表达式是否满足
        // 条件表达式："params[0].userId == 12345"
        if (!isConditionMet(conditionExpress, advice, cost)) {
            return; // 条件不满足，不输出
        }

        // 第二步：执行观察表达式，提取要观察的数据
        // 观察表达式："{params, returnObj}"
        Object watchResult = getExpressionResult(express);

        // 第三步：构建结果模型并输出
        WatchModel watchModel = new WatchModel();
        watchModel.setCost(cost);
        watchModel.setTs(advice.getTs());
        watchModel.setSize(watchSize);
        watchModel.setWatchResult(watchResult);
        process.appendResult(watchModel);

        // 第四步：检查执行次数限制
        if (numberOfLimit != null && count.incrementAndGet() >= numberOfLimit) {
            process.end();
        }
    }
}
```

**`isConditionMet()` —— OGNL 条件判断**：

> 源码位置：`AdviceListenerAdapter`（同 Trace 场景中引用的基类）

```java
protected boolean isConditionMet(String conditionExpress, Advice advice, double cost) {
    if (StringUtils.isEmpty(conditionExpress)) {
        return true;
    }
    try {
        // 使用 ThreadLocal 缓存的 OGNL 执行器
        Object value = ExpressFactory.threadLocalExpress(advice.classLoader())
            .bind(advice)                              // 将 advice 绑定到 OGNL 上下文
            .get(conditionExpress);                     // 执行 OGNL 表达式
        return value instanceof Boolean ? (Boolean) value : false;
    } catch (Exception e) {
        return false;
    }
}
```

**`ExpressFactory.threadLocalExpress()` —— OGNL 执行器工厂**：

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/express/ExpressFactory.java`

```java
public class ExpressFactory {

    private static final ThreadLocal<Express> threadLocalExpress = new ThreadLocal<Express>();

    /**
     * 使用 ThreadLocal 缓存 OGNL 执行器
     * 避免每次执行都创建新的 OGNL 上下文
     */
    public static Express threadLocalExpress(ClassLoader classLoader) {
        Express express = threadLocalExpress.get();
        if (express == null) {
            express = unpooledExpress(classLoader);
            threadLocalExpress.set(express);
        } else {
            // 更新 ClassLoader（可能不同请求由不同 ClassLoader 处理）
            express.setClassLoader(classLoader);
        }
        return express;
    }

    /**
     * 创建非池化的 OGNL 执行器
     */
    public static Express unpooledExpress(ClassLoader classLoader) {
        return new OgnlExpress(classLoader);
    }
}
```

**`OgnlExpress.get()` —— OGNL 表达式执行**：

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/express/OgnlExpress.java`

```java
public class OgnlExpress implements Express {

    private final ClassLoader classLoader;

    public OgnlExpress(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public Object get(String express) {
        try {
            // 编译 OGNL 表达式
            OgnlContext context = (OgnlContext) OgnlContext.createDefaultContext(
                null, null, null, new DefaultMemberAccess(true));
            context.setClassLoader(classLoader);

            // 设置上下文变量（advice 绑定的 params, returnObj 等）
            Map<String, Object> contextMap = getContextMap();
            if (contextMap != null) {
                for (Map.Entry<String, Object> entry : contextMap.entrySet()) {
                    context.put(entry.getKey(), entry.getValue());
                }
            }

            // 解析并执行 OGNL 表达式
            Object expression = Ognl.parseExpression(express);
            return Ognl.getValue(expression, context, (Object) null);
        } catch (Exception e) {
            throw new ExpressException("Failed to execute ognl: " + express, e);
        }
    }

    @Override
    public Express bind(Advice advice) {
        // 将 advice 的 params, returnObj 等绑定到上下文
        // 这样 OGNL 表达式中可以直接引用 params[0], returnObj 等
        Map<String, Object> contextMap = new HashMap<>();
        contextMap.put("params", advice.getParams());
        contextMap.put("returnObj", advice.getReturnObj());
        contextMap.put("throwExp", advice.getThrowable());
        contextMap.put("target", advice.getTarget());
        contextMap.put("cost", advice.getCost());
        setContextMap(contextMap);
        return this;
    }
}
```

**OGNL 如何执行 `params[0].userId == 12345`？**

1. `bind(advice)` 将 `params`（方法入参数组）放入 OGNL 上下文。
2. `Ognl.parseExpression("params[0].userId == 12345")` 解析表达式，构建 AST：
   - `params` → 从上下文取出 Object[] 数组
   - `[0]` → 取数组第一个元素（假设是 OrderRequest 对象）
   - `.userId` → 调用 `getUserId()` 方法（OGNL 的属性访问约定）
   - `== 12345` → 比较，返回 Boolean
3. `Ognl.getValue()` 执行 AST，返回 `true` 或 `false`。

**观察表达式 `{params, returnObj}` 的执行**：

OGNL 的 `{params, returnObj}` 语法会创建一个 List/Array，包含两个元素。`-x 2` 参数控制递归展开深度——深度为 2 意味着对于嵌套对象，展开两层属性。

### 2.10 `#cost` 特殊变量

在 trace 和 watch 命令中，`#cost` 是一个特殊变量，表示方法执行耗时。其来源是 `advice.getCost()`：

```java
// 在 Advice 对象中
private double cost;

public double getCost() {
    return cost;
}
```

`cost` 的计算发生在 Spy 织入代码中：方法入口时记录 `startTime = System.nanoTime()`，方法出口时计算 `cost = (System.nanoTime() - startTime) / 1000000.0`（转换为毫秒）。因此 `#cost` 反映的是该方法的**实际执行耗时**。

在 trace 命令中，`#cost` 表示当前调用节点的耗时。在条件表达式 `#cost > 100` 中，只有耗时超过 100ms 的调用树才会被输出。

### 2.11 Q&A 设计问题分析

**Q1: trace 命令如何实现方法内部每个子调用的耗时统计？核心机制是什么？**

核心机制是**字节码增强**。`Enhancer` 在 `transform()` 时，通过 ASM 的 `visitMethodInsn()` 钩子，在每个 `INVOKE*` 指令前后分别插入对 `SpyAPI.atBeforeInvoke()` 和 `SpyAPI.atAfterInvoke()` 的调用。运行时这些 Spy 调用被触发，`TraceAdviceListener` 通过 `deep` 计数器构建调用树，每个节点的耗时通过 `System.nanoTime()` 差值计算。

与 AOP（如 Spring AOP）的区别在于：AOP 只能增强到方法级别，无法感知方法内部的子调用。Arthas 的 trace 通过指令级字节码织入，实现了方法内部调用级别的追踪。

**Q2: watch 的条件表达式 `params[0].userId == 12345` 如何在目标 JVM 中执行？是否存在 ClassLoader 隔离问题？**

`ExpressFactory.threadLocalExpress(advice.classLoader())` 使用目标方法的 ClassLoader 创建 OGNL 执行器。`OgnlContext.setClassLoader(classLoader)` 确保在解析 `params[0].userId` 时，可以通过正确的 ClassLoader 加载 `OrderRequest` 类并调用 `getUserId()`。

如果目标应用使用自定义 ClassLoader（如 Tomcat WebAppClassLoader），Arthas 自带的 ClassLoader 无法直接访问应用类。通过使用目标 ClassLoader，OGNL 可以正确解析应用内部的类和方法。

**Q3: `--skipJDKMethod` 的作用是什么？为什么要跳过 JDK 方法？**

`--skipJDKMethod` 表示不追踪 `java.**` 包下的方法调用。原因有二：

1. **性能**：JDK 方法（如 `String.length()`, `HashMap.get()`）被调用极为频繁，追踪它们会产生巨大的调用树和性能开销。
2. **价值**：排查接口慢的问题，通常瓶颈在业务代码或外部调用，而非 JDK 方法。跳过 JDK 方法可以让调用树更聚焦。

底层通过 `SpyTraceExcludeJDKInterceptor` 实现：在 `AdviceWeaver` 中遇到 `owner` 以 `java/` 开头的 `INVOKE` 指令时，跳过织入 `atBeforeInvoke` / `atAfterInvoke`。

**Q4: trace 的调用树在什么情况下会输出？deep == 0 的判断意义是什么？**

trace 命令在目标方法执行完毕（`afterReturning` 或 `afterThrowing`）时调用 `finishing()`。但此时如果目标方法中有异步子调用（子方法在另一个线程中执行），`deep` 可能不为 0。`deep == 0` 的判断确保只有当**所有同步子调用都完成**时才输出调用树，避免输出不完整的树。

如果目标方法中有异步调用（如 `CompletableFuture.supplyAsync`），异步部分的调用不会被 trace 追踪到，因为 Spy 织入的代码只在该线程中执行。

**Q5: 为什么 `ExpressFactory` 使用 ThreadLocal 缓存 OGNL 执行器？**

OGNL 的表达式编译和上下文创建有一定开销。在 `watch` 命令中，目标方法可能被高频调用（如每秒数千次），如果每次都创建新的 OGNL 执行器，会带来不必要的性能开销。ThreadLocal 缓存使得每个线程只创建一次执行器并复用，大幅降低了开销。同时 `setClassLoader()` 方法在 ClassLoader 变化时更新引用，保证正确性。

### 2.12 场景总结

| 维度 | trace | watch |
|------|-------|-------|
| 增强范围 | 目标方法 + 所有子调用 | 仅目标方法 |
| 拦截器 | SpyTraceInterceptor1/2/3 + Invoke 级织入 | SpyInteractionInterceptor1/2/3 |
| 输出内容 | 调用树（每个节点的类名/方法名/耗时） | 方法入参/返回值/异常（条件过滤后） |
| 条件表达式 | `#cost > 100`（耗时过滤） | `params[0].userId == 12345`（参数过滤） |
| 适用场景 | 定位"哪个子调用慢" | 观察"慢调用时的入参是什么" |

完整排查链路：`trace`（找到最慢的子调用节点）→ `watch`（观察该节点的入参和返回值，判断是否是数据问题还是外部依赖问题）→ 如果是子方法慢，`trace` 或 `watch` 子方法进一步下钻。

---

## 场景三：动态修改日志级别 —— logger + ognl 两大方式

### 3.1 用户故事

某线上服务出了一个间歇性 bug，只在 DEBUG 级别日志中才能看到关键线索。但生产环境日志级别是 INFO，直接修改配置文件需要重启应用，而重启会中断服务、丢失现场。

运维需要：临时将 ROOT logger 级别改为 DEBUG，排查完再改回 INFO，全程不重启应用。

### 3.2 操作命令与参数说明

```bash
# 方式一：使用 logger 命令（封装好，自动检测日志框架）
logger --name ROOT --level DEBUG

# 方式二：使用 ognl 直接操作 LoggerContext（灵活，需要了解 API）
ognl '@org.slf4j.LoggerFactory@getILoggerFactory().getLogger("ROOT").setLevel(ch.qos.logback.classic.Level.DEBUG)'

# 查看当前所有 logger 配置
logger

# 改回 INFO 级别
logger --name ROOT --level INFO
```

**参数说明**：

| 命令 | 参数 | 说明 |
|------|------|------|
| `logger` | `--name <name>` | Logger 名称，ROOT 表示根 Logger |
| `logger` | `--level <level>` | 日志级别：TRACE/DEBUG/INFO/WARN/ERROR/OFF |
| `logger` | `--hash <hash>` | ClassLoader hashCode，多 ClassLoader 场景下指定 |
| `ognl` | `@class@method()` | OGNL 静态方法调用语法 |
| `ognl` | `-c <hash>` | 指定 ClassLoader |

### 3.3 从命令输入到结果输出的完整链路

**logger 命令链路**：

```
ShellLineHandler.handle("logger --name ROOT --level DEBUG")
  → CommandLine parsing → Job creation → ProcessImpl.run
  → AnnotatedCommandImpl.process → LoggerCommand.process
    → LoggerCommand.level(process)
      → detectLoggerType(classLoader)                        // 检测日志框架类型
        → Instrumentation.getAllLoadedClasses()             // 遍历已加载的类
        → 检测 Logback/Log4j/Log4j2 的核心类是否存在
      → AsmRenameUtil.renameClass(helperClass, classLoader) // 重命名 Helper 类
        → ReflectUtils.defineClass(renamedBytes, classLoader) // 注入到目标 ClassLoader
      → 反射调用 LoggerHelper.updateLevel(name, level)      // 修改日志级别
      → process.appendResult(loggerModel)                     // 输出结果
      → process.end()
```

**ognl 命令链路**：

```
ShellLineHandler.handle("ognl '@org.slf4j.LoggerFactory@getILoggerFactory()...'")
  → CommandLine parsing → Job creation → ProcessImpl.run
  → AnnotatedCommandImpl.process → OgnlCommand.process
    → ExpressFactory.unpooledExpress(classLoader)           // 创建 OGNL 执行器
    → OgnlExpress.get(express)                               // 执行 OGNL 表达式
      → Ognl.parseExpression("@org.slf4j.LoggerFactory@getILoggerFactory()")
      → Ognl.getValue()                                      // 反射调用静态方法
        → LoggerFactory.getILoggerFactory()                   // 返回 LoggerContext
      → .getLogger("ROOT")                                    // 获取 ROOT Logger
      → .setLevel(Level.DEBUG)                                // 设置级别
    → process.appendResult(ognlModel)                         // 输出结果
    → process.end()
```

### 3.4 LoggerCommand 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/logger/LoggerCommand.java`

```java
@Name("logger")
@Summary("Print logger info and set logger level")
public class LoggerCommand extends AnnotatedCommand {

    @Option(longName = "name")
    @Description("logger name")
    private String name;

    @Option(longName = "level")
    @Description("log level: TRACE, DEBUG, INFO, WARN, ERROR, OFF")
    private String level;

    @Option(longName = "hash")
    @Description("class loader hash")
    private String hash;
```

**`process()` 方法入口分发**：

```java
    @Override
    public void process(CommandProcess process) {
        if (level != null) {
            // 有 --level 参数，修改日志级别
            level(process);
        } else {
            // 无 --level 参数，查看所有 logger
            loggers(process);
        }
    }
```

### 3.5 `level()` —— 修改日志级别核心流程

```java
    private void level(CommandProcess process) {
        // 第一步：确定目标 ClassLoader
        ClassLoader classLoader = null;
        if (hash != null) {
            // 用户指定了 ClassLoader hash
            classLoader = ArthasCheckUtils.hashCodeToClassLoader(hash);
            if (classLoader == null) {
                process.end(1, "can not find classloader by hash: " + hash);
                return;
            }
        } else {
            // 默认使用 SystemClassLoader
            classLoader = ClassLoader.getSystemClassLoader();
        }

        // 第二步：检测日志框架类型
        LoggerType loggerType = detectLoggerType(classLoader);

        if (loggerType == null) {
            process.end(1, "can not detect logger framework in classloader: "
                + classLoader);
            return;
        }

        // 第三步：根据日志框架类型，注入对应的 Helper 类并执行
        try {
            String helperClassName = getHelperClassName(loggerType);
            Class<?> helperClass = injectHelper(helperClassName, classLoader);

            // 第四步：反射调用 Helper.updateLevel()
            Method updateLevelMethod = helperClass.getMethod("updateLevel",
                String.class, String.class);
            Object result = updateLevelMethod.invoke(null, name, level);

            // 第五步：输出结果
            LoggerModel loggerModel = new LoggerModel();
            loggerModel.setName(name);
            loggerModel.setLevel(level);
            loggerModel.setEffectiveLevel(result.toString());
            process.appendResult(loggerModel);
            process.end();
        } catch (Exception e) {
            process.end(1, "update logger level error: " + e.getMessage());
        }
    }
```

**`detectLoggerType()` —— 检测日志框架类型**：

```java
    private LoggerType detectLoggerType(ClassLoader classLoader) {
        // 通过 Instrumentation 检查哪些日志框架类已加载
        // 优先级：Logback > Log4j2 > Log4j
        if (classExists(classLoader, "ch.qos.logback.classic.LoggerContext")) {
            return LoggerType.LOGBACK;
        }
        if (classExists(classLoader, "org.apache.logging.log4j.core.LoggerContext")) {
            return LoggerType.LOG4J2;
        }
        if (classExists(classLoader, "org.apache.log4j.LogManager")) {
            return LoggerType.LOG4J;
        }
        return null;
    }

    private boolean classExists(ClassLoader classLoader, String className) {
        // 通过 Instrumentation.getAllLoadedClasses() 检查类是否已加载
        for (Class<?> clazz : inst.getAllLoadedClasses()) {
            if (clazz.getName().equals(className)) {
                // 进一步检查 ClassLoader 是否匹配
                if (classLoader.equals(clazz.getClassLoader())) {
                    return true;
                }
            }
        }
        return false;
    }
```

**设计意图**：

- Arthas 不能假定用户使用哪个日志框架，必须通过运行时检测来决定。
- 检测优先级：Logback 优先于 Log4j2 优先于 Log4j。如果应用同时引入了多个日志框架（通过 SLF4J 桥接），实际生效的是 SLF4J 绑定的那个。
- 通过 `Instrumentation.getAllLoadedClasses()` 检测，比 `Class.forName()` 更安全——后者可能触发类初始化，而前者只检查是否已加载。

### 3.6 `injectHelper()` —— 跨 ClassLoader 注入 Helper 类

这是 logger 命令最精妙的设计。Arthas 自身的 `LoggerHelper` 类在 Arthas 的 ClassLoader 中，但用户的日志框架类在应用的 ClassLoader 中。直接调用会抛 `ClassNotFoundException`。解决方案是：将 Helper 类的字节码重命名后注入到应用的 ClassLoader 中。

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/logger/AsmRenameUtil.java`

```java
public class AsmRenameUtil {

    /**
     * 重命名类的全限定名，并返回新的字节码
     */
    public static byte[] renameClass(byte[] bytes, String newName) {
        ClassReader cr = new ClassReader(bytes);
        ClassWriter cw = new ClassWriter(0);

        ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public void visit(int version, int access, String name,
                    String signature, String superName, String[] interfaces) {
                // 将原始类名替换为新名称
                super.visit(version, access, newName.replace('.', '/'),
                    signature, superName, interfaces);
            }
        };
        cr.accept(cv, 0);
        return cw.toByteArray();
    }
}
```

```java
    private Class<?> injectHelper(String helperClassName, ClassLoader targetClassLoader) {
        try {
            // 获取 LoggerHelper 的字节码（Arthas 自带）
            byte[] helperBytes = getClassBytes("com.taobao.arthas.core.command.logger.LoggerHelper");

            // 重命名类名，避免与 Arthas ClassLoader 中的同名类冲突
            String renamedClassName = "com.taobao.arthas.core.command.logger."
                + helperClassName + System.nanoTime();
            byte[] renamedBytes = AsmRenameUtil.renameClass(helperBytes,
                renamedClassName.replace('.', '/'));

            // 使用目标 ClassLoader 定义新类
            // ReflectUtils.defineClass 内部通过反射调用
            // ClassLoader.defineClass() 或 MethodHandles.Lookup.defineClass()
            Class<?> helperClass = ReflectUtils.defineClass(
                renamedClassName, renamedBytes, targetClassLoader);

            return helperClass;
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject helper class", e);
        }
    }
```

**为什么需要重命名？**

如果不重命名，直接在目标 ClassLoader 中定义 `com.taobao.arthas.core.command.logger.LoggerHelper`，可能因为类名冲突而失败（某些 ClassLoader 实现不允许重复定义同名类）。重命名加入 `System.nanoTime()` 后缀保证唯一性。

**`ReflectUtils.defineClass()` 的实现**：

> 源码位置：`arthas/common/src/main/java/com/taobao/arthas/common/ReflectUtils.java`

```java
    /**
     * 在指定的 ClassLoader 中定义一个新类
     */
    public static Class<?> defineClass(String className, byte[] bytes,
            ClassLoader classLoader) throws Exception {
        try {
            // 方式一：通过 MethodHandles.Lookup（Java 9+）
            MethodHandles.Lookup lookup = null;
            if (privateLookupInMethod != null) {
                lookup = (MethodHandles.Lookup) privateLookupInMethod.invoke(
                    null, MethodHandles.lookup(), classLoader);
            }
            if (lookup != null && lookupDefineClassMethod != null) {
                return (Class<?>) lookupDefineClassMethod.invoke(
                    lookup, bytes);
            }
        } catch (Exception e) {
            // fallback
        }

        // 方式二：通过反射调用 ClassLoader.defineClass()（Java 8）
        if (classLoaderDefineClassMethod != null) {
            return (Class<?>) classLoaderDefineClassMethod.invoke(
                classLoader, className, bytes, 0, bytes.length,
                PROTECTION_DOMAIN);
        }

        throw new RuntimeException("can not define class");
    }
```

这里有两种方式兼容不同 Java 版本：
- **Java 9+**：使用 `MethodHandles.privateLookupIn()` 获取目标 ClassLoader 的 Lookup，然后调用 `Lookup.defineClass()` 注入字节码。这是 Java 9+ 推荐的方式，因为 `ClassLoader.defineClass()` 被标记为 deprecated。
- **Java 8**：通过反射调用 `ClassLoader.defineClass()`，需要绕过 access check。

### 3.7 `LoggerHelper` —— 各日志框架的适配

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/logger/LoggerHelper.java`

`LoggerHelper` 是一个静态方法工具类，包含针对不同日志框架的实现：

```java
public class LoggerHelper {

    // =================== Logback ===================
    public static Object updateLevel_Logback(String name, String level)
            throws Exception {
        // 通过反射获取 LoggerContext
        Class<?> loggerContextClass = Class.forName(
            "ch.qos.logback.classic.LoggerContext");
        Class<?> levelClass = Class.forName(
            "ch.qos.logback.classic.Level");

        // 获取 LoggerContext（通过 SLF4J）
        Object iLoggerFactory = getILoggerFactory();
        Object logger = iLoggerFactory.getClass()
            .getMethod("getLogger", String.class)
            .invoke(iLoggerFactory, name);

        // 解析 level
        Object levelObj = levelClass.getField(level).get(null);
        // 调用 logger.setLevel(Level)
        logger.getClass().getMethod("setLevel", levelClass)
            .invoke(logger, levelObj);

        return levelObj;
    }

    // =================== Log4j ===================
    public static Object updateLevel_Log4j(String name, String level)
            throws Exception {
        Class<?> loggerClass = Class.forName("org.apache.log4j.Logger");
        Class<?> priorityClass = Class.forName("org.apache.log4j.Priority");

        Object logger = loggerClass.getMethod("getLogger", String.class)
            .invoke(null, name);
        Object levelObj = priorityClass.getMethod("toLevel", String.class)
            .invoke(null, level);

        loggerClass.getMethod("setLevel", priorityClass)
            .invoke(logger, levelObj);

        return levelObj;
    }

    // =================== Log4j2 ===================
    public static Object updateLevel_Log4j2(String name, String level)
            throws Exception {
        Class<?> loggerContextClass = Class.forName(
            "org.apache.logging.log4j.core.LoggerContext");
        Class<?> levelClass = Class.forName(
            "org.apache.logging.log4j.Level");

        Object ctx = getILoggerFactory();
        org.apache.logging.log4j.spi.LoggerContext loggerContext =
            (org.apache.logging.log4j.spi.LoggerContext) ctx;
        Object logger = loggerContext.getClass()
            .getMethod("getLogger", String.class)
            .invoke(loggerContext, name);

        Object levelObj = levelClass.getMethod("toLevel", String.class)
            .invoke(null, level);

        // Log4j2 的 Configurator.setLevel()
        Class<?> configuratorClass = Class.forName(
            "org.apache.logging.log4j.core.config.Configurator");
        configuratorClass.getMethod("setLevel",
            String.class, levelClass).invoke(null, name, levelObj);

        return levelObj;
    }

    /**
     * 通过 SLF4J 获取 ILoggerFactory
     */
    private static Object getILoggerFactory() throws Exception {
        Class<?> factoryClass = Class.forName("org.slf4j.LoggerFactory");
        return factoryClass.getMethod("getILoggerFactory")
            .invoke(null);
    }
}
```

**关键设计**：

- `LoggerHelper` 中所有方法都使用**反射**调用日志框架 API，而不是直接 `import`。这是因为 `LoggerHelper` 被注入到应用的 ClassLoader 中运行，但 Arthas 自身的编译环境中不一定有所有日志框架的依赖。反射调用避免编译期依赖。
- `updateLevel_Logback` / `updateLevel_Log4j` / `updateLevel_Log4j2` 三个方法对应三种框架。`LoggerCommand.level()` 在注入后会根据 `loggerType` 调用对应的方法。
- `getILoggerFactory()` 通过 SLF4J 的 `LoggerFactory.getILoggerFactory()` 获取实际的日志上下文。这种方式可以正确处理 SLF4J 桥接场景（如 log4j-over-slf4j）。

### 3.8 `loggers()` —— 查看所有 Logger 配置

```java
    private void loggers(CommandProcess process) {
        // 遍历所有已加载类，检测每个 ClassLoader 中的日志框架
        Map<ClassLoader, LoggerType> classLoaderLoggerMap = new HashMap<>();
        for (Class<?> clazz : inst.getAllLoadedClasses()) {
            ClassLoader cl = clazz.getClassLoader();
            if (cl != null && !classLoaderLoggerMap.containsKey(cl)) {
                LoggerType type = detectLoggerType(cl);
                if (type != null) {
                    classLoaderLoggerMap.put(cl, type);
                }
            }
        }

        // 对每个检测到的日志框架，获取所有 logger 配置
        List<LoggerModel> loggerModels = new ArrayList<>();
        for (Map.Entry<ClassLoader, LoggerType> entry : classLoaderLoggerMap.entrySet()) {
            ClassLoader cl = entry.getKey();
            LoggerType type = entry.getValue();

            // 注入 Helper 并调用 loggerInfo
            String helperClassName = getHelperClassName(type);
            Class<?> helperClass = injectHelper(helperClassName, cl);

            // 反射调用 LoggerHelper.loggerInfo()
            Method loggerInfoMethod = helperClass.getMethod("loggerInfo");
            Object result = loggerInfoMethod.invoke(null);

            // 结果转换为 LoggerModel
            // ...
        }

        process.appendResult(loggerModels);
        process.end();
    }
```

这个方法遍历所有已加载类，找出每个 ClassLoader 中的日志框架。这在多应用部署场景（如 Tomcat 多个 WebApp）下特别有用——可以分别查看和修改每个应用的日志级别。

### 3.9 OgnlCommand 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/OgnlCommand.java`

```java
@Name("ognl")
@Summary("Execute ognl expression.")
public class OgnlCommand extends AnnotatedCommand {

    @Argument(index = 0, argName = "express")
    @Description("An ognl expression.")
    private String express;

    @Option(shortName = "c", longName = "classloader")
    @Description("The hash code of the special class's classLoader")
    private String classLoaderHash;

    @Option(longName = "classLoaderClass")
    @Description("The class name of the special class's classLoader")
    private String classLoaderClass;

    @Override
    public void process(CommandProcess process) {
        // 第一步：确定 ClassLoader
        ClassLoader classLoader = null;
        if (classLoaderHash != null) {
            classLoader = ArthasCheckUtils.hashCodeToClassLoader(classLoaderHash);
            if (classLoader == null) {
                process.end(1, "can not find classloader by hash: " + classLoaderHash);
                return;
            }
        } else {
            // 默认使用 SystemClassLoader
            classLoader = ClassLoader.getSystemClassLoader();
        }

        // 第二步：创建 OGNL 执行器并执行表达式
        try {
            Express ognlExpress = ExpressFactory.unpooledExpress(classLoader);
            Object value = ognlExpress.get(express);

            // 第三步：输出结果
            OgnlModel ognlModel = new OgnlModel();
            ognlModel.setValue(value);
            process.appendResult(ognlModel);
            process.end();
        } catch (Exception e) {
            process.end(1, "Failed to execute ognl expression: " + express
                + ", error: " + e.getMessage());
        }
    }
}
```

**与 logger 命令的对比分析**：

`ognl` 命令的核心就是一行代码：`ognlExpress.get(express)`。它不关心你要做什么——修改日志级别、调用任意方法、读取静态字段，都可以。这种**通用性**是 ognl 命令的核心价值。

**OGNL 表达式 `@org.slf4j.LoggerFactory@getILoggerFactory().getLogger("ROOT").setLevel(ch.qos.logback.classic.Level.DEBUG)` 的执行过程**：

1. `@org.slf4j.LoggerFactory@getILoggerFactory()`：`@` 开头表示静态方法调用。OGNL 通过反射调用 `LoggerFactory.getILoggerFactory()`，返回 `LoggerContext` 对象。
2. `.getLogger("ROOT")`：对返回的对象调用 `getLogger("ROOT")` 方法，获取 ROOT Logger。
3. `.setLevel(ch.qos.logback.classic.Level.DEBUG)`：调用 `setLevel()` 方法，参数是 `Level.DEBUG` 枚举值。OGNL 会通过 ClassLoader 加载 `ch.qos.logback.classic.Level` 类，并访问其 `DEBUG` 静态字段。

### 3.10 两种方式的对比

| 维度 | logger 命令 | ognl 命令 |
|------|-------------|-----------|
| 易用性 | 高（自动检测框架，参数简单） | 低（需要了解日志框架 API） |
| 灵活性 | 低（只能改级别，不能做其他操作） | 高（可执行任意 Java 代码） |
| 安全性 | 较高（封装好的操作，不容易出错） | 较低（可以执行任意代码，有风险） |
| ClassLoader 处理 | 自动注入 Helper 到目标 ClassLoader | 通过 `-c` 指定 ClassLoader |
| 底层机制 | ASM 重命名 + 反射注入 + 反射调用 | OGNL 表达式引擎 + 反射 |
| 适用场景 | 快速修改日志级别 | 复杂操作（如修改某个 logger 的 appender） |

**底层都通过反射操作目标 JVM 中的对象**。区别在于 logger 命令将反射逻辑封装在注入的 `LoggerHelper` 中（运行在目标 ClassLoader 里），而 ognl 命令通过 OGNL 引擎执行反射（OGNL 自带反射机制）。

### 3.11 Q&A 设计问题分析

**Q1: 为什么 LoggerHelper 需要注入到目标 ClassLoader 中？不能直接在 Arthas 的 ClassLoader 中执行吗？**

不能。`LoggerHelper` 需要调用日志框架的 API（如 `ch.qos.logback.classic.LoggerContext`），这些类只存在于应用的 ClassLoader 中。Arthas 的 ClassLoader 看不到这些类，直接调用会抛 `ClassNotFoundException`。

通过将 `LoggerHelper` 的字节码注入到应用的 ClassLoader 中，`LoggerHelper` 就可以通过应用的 ClassLoader 加载和调用日志框架类了。

**Q2: `AsmRenameUtil.renameClass` 重命名类名的必要性是什么？**

如果不重命名，尝试在目标 ClassLoader 中定义一个名为 `com.taobao.arthas.core.command.logger.LoggerHelper` 的类，可能与目标 ClassLoader 中已存在的同名类冲突。即使目标 ClassLoader 中没有同名类，重命名也保证了一次性——每次执行都生成一个唯一的类名（带 `System.nanoTime()` 后缀），避免在多次执行时累积。

**Q3: ognl 命令中 `@class@method()` 语法如何映射到 Java 反射调用？**

OGNL 中 `@class@method()` 表示调用 `class` 的静态方法 `method()`。OGNL 引擎内部实现为：
1. 通过 ClassLoader 加载 `class` 对应的 `Class` 对象。
2. 通过 `Class.getMethod("method")` 找到方法。
3. 通过 `Method.invoke(null)` 调用静态方法（第一个参数为 null 表示静态方法）。

如果方法有参数，OGNL 会根据参数类型自动匹配重载方法。这使得 OGNL 表达式可以执行几乎任意的 Java 代码。

**Q4: logger 命令如何处理多 ClassLoader 场景（如 Tomcat 多个 WebApp）？**

`detectLoggerType()` 方法通过 `Instrumentation.getAllLoadedClasses()` 遍历所有类，对每个 ClassLoader 检测日志框架。在 `loggers()` 命令中，会为每个检测到的 ClassLoader 分别注入 Helper 并获取 logger 列表。

修改级别时，如果用户指定了 `--hash`，则精确匹配到某个 ClassLoader 的日志框架；如果不指定，默认使用 SystemClassLoader——对于普通 Java 应用通常足够，但对于 Tomcat 等容器应用，需要通过 `sc -d` 命令找到应用类的 ClassLoader hash 再用 `--hash` 指定。

**Q5: 修改日志级别后，如果不改回来，应用重启后级别会恢复吗？**

会恢复。Arthas 修改的是运行时内存中的 Logger 对象级别，不修改配置文件。应用重启后，日志框架重新从配置文件（如 `logback.xml`）读取级别，会恢复到配置的级别。这也是 Arthas 动态修改日志级别的安全之处——它是临时的、非持久化的。

### 3.12 场景总结

| 方式 | 核心机制 | 关键类 | 优点 | 缺点 |
|------|---------|--------|------|------|
| logger | ASM 重命名注入 + 反射调用 | LoggerCommand, LoggerHelper, AsmRenameUtil, ReflectUtils | 自动检测框架，易用 | 功能受限 |
| ognl | OGNL 表达式引擎 + 反射 | OgnlCommand, OgnlExpress, ExpressFactory | 通用灵活 | 需要了解 API |

两种方式底层都通过反射操作目标 JVM 中的 Logger 对象，区别在于"谁来执行反射"：logger 命令将逻辑封装在注入的 Helper 中执行，ognl 命令通过 OGNL 引擎执行。选择哪种方式取决于场景——简单改级别用 logger，需要做更复杂操作（如修改 appender 配置、动态创建 logger）用 ognl。

---

## 场景四：反编译确认代码是否生效 —— jad 命令

### 4.1 用户故事

开发说已经修复了一个 NPE bug 并部署到生产环境，但问题依然间歇性出现。运维怀疑：部署的 jar 包是否真的包含了修复代码？或者 ClassLoader 加载了旧版本的类？

需要通过反编译确认线上运行的字节码是否真的是最新版本——看修复的那行代码在不在，是否与预期一致。

### 4.2 操作命令与参数说明

```bash
# 反编译指定类（包含行号、ClassLoader 信息）
jad com.example.OrderService

# 只看源码不看行号
jad com.example.OrderService --source-only

# 指定 ClassLoader（多 ClassLoader 环境）
jad com.example.OrderService -c 18b4aac2

# 反编译指定方法
jad com.example.OrderService createOrder

# 使用类名匹配 ClassLoader
jad com.example.OrderService --classLoaderClass org.springframework.boot.loader.LaunchedURLClassLoader
```

**参数说明**：

| 参数 | 说明 |
|------|------|
| `class-pattern` | 类名，支持通配符 |
| `method-pattern` | 方法名（可选），只反编译指定方法 |
| `--source-only` | 只输出源码，不显示行号和 ClassLoader 信息 |
| `-c <hash>` | ClassLoader hashCode，精确匹配 |
| `--classLoaderClass <name>` | 用 ClassLoader 类名匹配 |
| `--lineNumber` | 是否显示行号（默认 true） |

### 4.3 从命令输入到结果输出的完整链路

```
ShellLineHandler.handle("jad com.example.OrderService")
  → CommandLine parsing → Job creation → ProcessImpl.run
  → AnnotatedCommandImpl.process → JadCommand.process
    → SearchUtils.searchClassOnly(inst, classPattern, isRegex, code)
      → Instrumentation.getAllLoadedClasses()              // 遍历所有已加载的类
      → 匹配类名
    → 判断匹配结果：
      → 0 个匹配 → processNoMatch()                        // 提示类不存在
      → 多个匹配 → processMatches()                         // 提示用 -c 指定
      → 1 个匹配 → processExactMatch()                      // 精确匹配，继续

  → processExactMatch()
    → 搜索内部类（ClassName$* 模式）
    → 创建 ClassDumpTransformer
    → InstrumentationUtils.retransformClasses(inst, clazz) // 触发 dump
      → ClassDumpTransformer.transform()                    // 记录字节码（不修改）
    → Decompiler.decompileWithMappings(bytes, mappings)    // CFR 反编译
      → org.benf.cfr.reader.api.CfrDriver.analyse()         // 分析字节码
      → 生成 Java 源码
    → JadModel 构建（source + classLoaderName + hashCode）
    → process.appendResult(jadModel)                        // 推送结果
    → JadView.draw()                                        // 渲染输出
    → process.end()
```

### 4.4 JadCommand.process() 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/JadCommand.java`

```java
@Name("jad")
@Summary("Decompile the specified class and print source code")
public class JadCommand extends AnnotatedCommand {

    @Argument(index = 0, argName = "class-pattern")
    @Description("Class name pattern")
    private String classPattern;

    @Argument(index = 1, argName = "method-pattern", required = false)
    @Description("Method name pattern")
    private String methodPattern;

    @Option(longName = "source-only")
    @Description("Output source code only, without line numbers and classloader info")
    private boolean sourceOnly = false;

    @Option(shortName = "c", longName = "classloader")
    @Description("The hash code of the special class's classLoader")
    private String hashCode;

    @Option(longName = "classLoaderClass")
    @Description("The class name of the special class's classLoader")
    private String classLoaderClass;

    @Option(longName = "lineNumber")
    @Description("Show line number, default value is true")
    private boolean lineNumber = true;

    @Override
    public void process(CommandProcess process) {
        // 搜索匹配的类
        Set<Class<?>> matchedClasses = SearchUtils.searchClassOnly(
            inst, classPattern, isRegex, hashCode);

        if (matchedClasses.isEmpty()) {
            // 无匹配
            processNoMatch(process);
        } else if (matchedClasses.size() > 1) {
            // 多匹配
            processMatches(process, matchedClasses);
        } else {
            // 精确匹配
            processExactMatch(process, matchedClasses.iterator().next());
        }
    }
}
```

**`SearchUtils.searchClassOnly()` 的搜索逻辑**：

```java
    public static Set<Class<?>> searchClassOnly(Instrumentation inst,
            String classPattern, boolean isRegex, String hashCode) {
        Set<Class<?>> matchedClasses = new HashSet<>();
        for (Class<?> clazz : inst.getAllLoadedClasses()) {
            // 匹配类名
            String name = clazz.getName();
            boolean matched = isRegex
                ? name.matches(classPattern)
                : name.equals(classPattern) || wildcardMatch(name, classPattern);

            if (!matched) continue;

            // 如果指定了 ClassLoader hash，进一步匹配
            if (hashCode != null) {
                ClassLoader cl = clazz.getClassLoader();
                String clHash = Integer.toHexString(cl.hashCode());
                if (!clHash.equals(hashCode)) continue;
            }

            matchedClasses.add(clazz);
        }
        return matchedClasses;
    }
```

**为什么会有多个匹配？**

在多 ClassLoader 环境下，同一个类名可能被不同的 ClassLoader 分别加载。例如 Tomcat 中，`WebAppClassLoaderA` 和 `WebAppClassLoaderB` 可能都加载了 `com.example.OrderService`，但它们是不同的 Class 对象，字节码可能不同（不同版本的应用部署在不同 WebApp 中）。

### 4.5 `processExactMatch()` —— 反编译核心流程

```java
    private void processExactMatch(CommandProcess process, Class<?> clazz) {
        String className = clazz.getName();
        ClassLoader classLoader = clazz.getClassLoader();
        String classLoaderName = classLoader != null
            ? classLoader.getClass().getName() : "BootStrapClassLoader";
        String classLoaderHash = classLoader != null
            ? Integer.toHexString(classLoader.hashCode()) : "null";

        // 搜索内部类（ClassName$* 模式）
        List<Class<?>> innerClasses = searchInnerClasses(clazz);

        // 将主类和内部类合并
        List<Class<?>> allClasses = new ArrayList<>();
        allClasses.add(clazz);
        allClasses.addAll(innerClasses);

        // 创建 ClassDumpTransformer
        Map<Class<?>, byte[]> classBytesMap = new HashMap<>();
        ClassDumpTransformer transformer = new ClassDumpTransformer(classBytesMap);

        try {
            // 注册 transformer
            inst.addTransformer(transformer, true);
            // 触发 retransform
            // 此时 ClassDumpTransformer.transform() 被调用
            // 它只是记录字节码，不修改
            InstrumentationUtils.retransformClasses(inst,
                allClasses.toArray(new Class<?>[0]));
        } catch (Exception e) {
            process.end(1, "retransformClasses error: " + e.getMessage());
            return;
        } finally {
            inst.removeTransformer(transformer);
        }

        // 反编译
        JadModel jadModel = new JadModel();
        jadModel.setClassLoaderName(classLoaderName);
        jadModel.setClassLoaderHash(classLoaderHash);

        StringBuilder sourceBuilder = new StringBuilder();
        for (Map.Entry<Class<?>, byte[]> entry : classBytesMap.entrySet()) {
            Class<?> matchedClass = entry.getKey();
            byte[] bytes = entry.getValue();

            // 使用 CFR 反编译
            String source = Decompiler.decompileWithMappings(
                bytes, className, methodPattern);

            sourceBuilder.append(source);
            sourceBuilder.append("\n");
        }

        jadModel.setSource(sourceBuilder.toString());
        process.appendResult(jadModel);
        process.end();
    }
```

**关键设计 `ClassDumpTransformer`**：

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/ClassDumpTransformer.java`

```java
public class ClassDumpTransformer implements ClassFileTransformer {

    private final Map<Class<?>, byte[]> classBytesMap;

    public ClassDumpTransformer(Map<Class<?>, byte[]> classBytesMap) {
        this.classBytesMap = classBytesMap;
    }

    @Override
    public byte[] transform(ClassLoader loader, String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer) {
        // 记录字节码到 Map 中
        // 注意：这里直接返回 classfileBuffer 原值，不修改字节码
        if (classBeingRedefined != null) {
            classBytesMap.put(classBeingRedefined, classfileBuffer);
        }
        // 返回 null 表示不修改
        // 但实际返回 classfileBuffer 原值也不会修改（字节码相同）
        return null;
    }
}
```

**精妙之处**：

`jad` 命令利用 `retransformClasses` 来"窃取"类的字节码。它注册一个 `ClassFileTransformer`，当 JVM 调用 `retransformClasses` 时，会将**当前内存中的字节码**传给 transformer 的 `transform()` 方法。`ClassDumpTransformer` 只是把这些字节码存到 Map 中，返回 `null`（表示不修改），从而实现了"只读"的字节码 dump。

这比直接从 classpath 读取 `.class` 文件更可靠——因为运行时加载的类可能来自不同的来源（jar 包、网络、动态生成），不一定是 classpath 上的文件。`retransformClasses` 拿到的是 JVM 实际加载的字节码。

### 4.6 `InstrumentationUtils.retransformClasses()` —— 安全的 retransform

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/util/InstrumentationUtils.java`

```java
    public static void retransformClasses(Instrumentation inst,
            Class<?>... classes) throws UnmodifiableClassException {
        // 调用 JVM 的 retransformClasses
        // JVM 会对每个指定的类重新调用所有已注册的 ClassFileTransformer
        inst.retransformClasses(classes);
    }
```

`retransformClasses` 是 `java.lang.instrument.Instrumentation` 接口的方法，它触发 JVM 对指定类重新加载字节码，过程中所有已注册的 transformer 都会被调用。

**为什么 jad 需要用 retransform 而不是直接读取 class 文件？**

1. **类可能不在文件系统中**：动态代理类、Lambda 类、instrument agent 动态注入的类，都没有对应的 `.class` 文件。
2. **类可能已被增强**：如果其他 agent（如 SkyWalking）已经增强了类，class 文件中的字节码和内存中的不同。jad 能看到内存中**实际运行**的字节码，这正是"确认代码是否生效"所需要的信息。
3. **多版本 jar 包**：classpath 上可能有多个版本的同一类，实际加载的版本不一定是 classpath 顺序的第一个。

### 4.7 CFR 反编译引擎

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/util/Decompiler.java`

Arthas 使用 [CFR (Class File Reader)](https://github.com/leibnitz27/cfr) 作为反编译引擎。CFR 是一个纯 Java 实现的 JVM 字节码反编译器，以输出质量高著称。

```java
    public static String decompileWithMappings(byte[] bytes,
            String className, String methodName) {
        try {
            // 创建 CFR Driver
            CfrDriver driver = new CfrDriver.Builder()
                .options(Arrays.asList(
                    "showversion", "false",     // 不显示版本信息
                    "decodeenumswitch", "true",  // 枚举 switch 反编译
                    "decodeenumswitchstrapping", "true",
                    "decodefinally", "true",     // finally 块反编译
                    "decodelambdas", "true",    // Lambda 反编译
                    "sugarextendedtypes", "true", // 泛型信息恢复
                    "hideutf", "true",
                    "hidenuisance", "true",
                    "sugarextendedtypes", "true"
                ))
                .build();

            // 分析字节码
            ClassFile classFile = ClassFile.fromBytes(bytes);
            SummaryDumper summaryDumper = new SummaryDumper();

            // 设置行号映射
            if (lineNumber) {
                // 保留行号信息
            } else {
                // 忽略行号
                options.add("nocalc");
            }

            // 反编译
            driver.analyse(classFile, summaryDumper);
            String decompiled = summaryDumper.toString();

            // 如果指定了方法名，只输出该方法
            if (methodName != null && !methodName.isEmpty()) {
                decompiled = extractMethod(decompiled, methodName);
            }

            return decompiled;
        } catch (Exception e) {
            return "// decompile error: " + e.getMessage();
        }
    }
```

**CFR 反编译的核心能力**：

1. **泛型信息恢复**：JVM 字节码中的泛型信息存储在 `Signature` 属性中。CFR 通过解析 `Signature` 属性，在反编译输出中恢复泛型类型参数。例如 `List<String>` 在字节码中只是 `List`，但 CFR 能恢复出 `<String>`。

2. **Lambda 表达式恢复**：Java 8 的 Lambda 在编译时通过 `invokedynamic` 指令和 `LambdaMetafactory` 实现。CFR 能识别 `invokedynamic` + `LambdaMetafactory` 的模式，将其反编译为 Lambda 表达式。

3. **Switch-enum 恢复**：Java 的 `switch(enum)` 在编译时会生成一个辅助的 `switch(int)` 映射表。CFR 能识别这种模式并恢复为 `switch(enumValue)`。

4. **行号映射**：字节码中的 `LineNumberTable` 属性记录了每条字节码指令对应的源码行号。CFR 在反编译输出中标注行号，帮助定位代码位置。

**`--source-only` 选项的作用**：

```java
        if (sourceOnly) {
            // 只输出源码，不显示行号标记和 ClassLoader 信息
            jadModel.setClassLoaderName(null);
            jadModel.setClassLoaderHash(null);
            // CFR 选项中关闭行号
            options.add("nocalc");
        }
```

**`--lineNumber` 选项**：

```java
        if (!lineNumber) {
            // 不显示行号
            options.add("nocalc");
        }
```

当 `--lineNumber` 为 false 时，CFR 不输出行号。这在将反编译结果用于对比时有用——行号会产生干扰。

### 4.8 ClassLoader 解析机制

**`-c <hash>` 精确匹配**：

```java
        if (hashCode != null) {
            ClassLoader cl = clazz.getClassLoader();
            String clHash = Integer.toHexString(cl.hashCode());
            if (!clHash.equals(hashCode)) {
                continue; // 跳过不匹配的 ClassLoader
            }
        }
```

`hashCode` 是 `ClassLoader.hashCode()` 的十六进制表示。Arthas 中通过 `sc -d <className>` 命令可以查看类的 ClassLoader 信息（包括 hash code），然后用 `jad -c <hash>` 精确指定要反编译哪个 ClassLoader 加载的类。

**`--classLoaderClass <name>` 类名匹配**：

```java
        if (classLoaderClass != null) {
            ClassLoader cl = clazz.getClassLoader();
            if (cl != null && !cl.getClass().getName().equals(classLoaderClass)) {
                continue;
            }
        }
```

用 ClassLoader 的类名匹配。例如 `--classLoaderClass org.springframework.boot.loader.LaunchedURLClassLoader` 匹配 Spring Boot 的 LaunchedURLClassLoader。

**默认搜索所有 ClassLoader**：

当不指定 `-c` 和 `--classLoaderClass` 时，搜索所有 ClassLoader 加载的类。如果只有一个匹配，直接反编译；如果多个匹配，提示用户用 `-c` 指定。

### 4.9 内部类处理

```java
    private List<Class<?>> searchInnerClasses(Class<?> clazz) {
        String className = clazz.getName();
        List<Class<?>> innerClasses = new ArrayList<>();
        // 内部类的命名规则：OuterClass$InnerClass
        // 搜索所有已加载的类
        for (Class<?> c : inst.getAllLoadedClasses()) {
            if (c.getName().startsWith(className + "$")) {
                // 确保是同一个 ClassLoader
                ClassLoader cl1 = c.getClassLoader();
                ClassLoader cl2 = clazz.getClassLoader();
                if (cl1 != null && cl1.equals(cl2)) {
                    innerClasses.add(c);
                } else if (cl1 == null && cl2 == null) {
                    innerClasses.add(c);
                }
            }
        }
        return innerClasses;
    }
```

内部类和外部类一起反编译是必要的——内部类可能包含关键逻辑（如 `OrderService$Builder`, `OrderService$Callback`），只看外部类会遗漏重要信息。

### 4.10 完整的输出结构

jad 命令的输出包含：

```
ClassLoader:
  org.springframework.boot.loader.LaunchedURLClassLoader@18b4aac2

Location:
  /opt/app/order-service-1.0.0.jar

  /**
   * 反编译的源码
   */
  package com.example;

  public class OrderService {
      public OrderResult createOrder(OrderRequest request) {
          // 反编译的代码
          // 这里就是验证修复代码是否存在的关键
          if (request.getUserId() == null) {  // line 42
              throw new IllegalArgumentException("userId is null");
          }
          // ...
      }
  }
```

**ClassLoader** 信息帮助用户确认类由哪个 ClassLoader 加载，在多 ClassLoader 环境下尤为重要。

**Location** 显示类文件来源（jar 包路径），帮助用户判断部署的 jar 包是否正确。

**行号**标注在源码右侧，帮助精确定位到修复代码所在的行。

### 4.11 Q&A 设计问题分析

**Q1: jad 命令为什么用 `retransformClasses` 来获取字节码，而不是直接读取 class 文件？**

三个原因：
1. 类可能不在文件系统中（动态代理、Lambda 类、agent 注入的类）。
2. 字节码可能已被其他 agent 修改，class 文件和内存中的不同。jad 看到的是**实际运行**的字节码。
3. classpath 上可能有多个版本的类文件，不确定哪个被实际加载。`retransformClasses` 拿到的是 JVM 实际加载的版本。

**Q2: `ClassDumpTransformer.transform()` 返回 null 的意义是什么？**

返回 `null` 表示"不修改字节码"。这是一个**只读 dump** 操作——Transformer 接收到 JVM 传入的当前字节码，将其存入 Map，然后返回 `null` 告诉 JVM"我不改"。这样 `retransformClasses` 调用完成后，类的字节码不变，应用行为不受影响。

如果返回 `classfileBuffer`（原始字节码），效果一样（字节码不变）。但返回 `null` 语义更清晰——明确表示"不改"。

**Q3: 为什么 jad 能看到被其他 agent（如 SkyWalking）增强后的代码？**

因为 `retransformClasses` 传给 transformer 的字节码是 **JVM 内存中当前的字节码**，包括之前其他 agent 已经织入的代码。所以 jad 反编译的结果可能包含 SkyWalking 的增强代码（如额外的 try-catch 块）。

如果只看 class 文件，看到的是**原始编译后**的字节码，没有 agent 增强的痕迹。jad 看到的是**运行时实际**的字节码。

**Q4: CFR 反编译的输出质量如何？会不会有反编译失败的情况？**

CFR 是业界质量最高的反编译器之一，但反编译不可能 100% 还原源码。常见问题包括：
- 变量名丢失（字节码不保存局部变量名，CFR 用 `var1`, `var2` 等替代，除非有 `LocalVariableTable`）。
- `for` 循环可能被反编译为 `while`。
- 注解信息可能不完整（`RuntimeVisibleAnnotations` 属性有保留，但 `RetentionPolicy.SOURCE` 的注解丢失）。
- Lambda 和 Stream 可能不如源码简洁。

但对于"确认修复代码是否存在"这个场景，CFR 的输出质量足够——能看清方法的控制流和关键逻辑。

**Q5: 在多 ClassLoader 环境下，jad 如何确保反编译的是正确版本？**

通过 `-c <hash>` 参数精确指定 ClassLoader。用户先用 `sc -d com.example.OrderService` 查看所有匹配的类及其 ClassLoader hash，然后用 `jad -c <hash> com.example.OrderService` 反编译指定 ClassLoader 中的版本。

如果用户不确定用哪个 ClassLoader，直接执行 `jad com.example.OrderService`，如果只有一个匹配就直接反编译；多个匹配时会列出所有 ClassLoader hash，提示用户选择。

### 4.12 场景总结

| 维度 | 说明 |
|------|------|
| 核心机制 | `retransformClasses` + `ClassDumpTransformer` 只读 dump 字节码 + CFR 反编译 |
| 字节码来源 | JVM 内存中实际运行的字节码（非 class 文件） |
| 关键类 | JadCommand, ClassDumpTransformer, Decompiler, SearchUtils, InstrumentationUtils |
| ClassLoader 支持 | `-c <hash>` 精确指定，`--classLoaderClass` 类名匹配 |
| 输出内容 | 反编译源码 + 行号 + ClassLoader 信息 + jar 包路径 |
| 适用场景 | 确认代码是否最新版本、查看运行时字节码是否被增强、排查 ClassLoader 冲突 |

jad 命令的设计哲学是"只读不写"——通过 `retransformClasses` 获取字节码但不修改，通过 CFR 反编译但不注入。这使得 jad 是所有 Arthas 命令中**最安全**的命令之一，不会对运行的应用产生任何副作用。

---

## 全文总结

本文通过四个真实生产场景，深入追踪了 Arthas 的核心源码链路：

| 场景 | 核心命令 | 关键源码类 | 核心机制 |
|------|---------|-----------|---------|
| CPU 飙高排查 | dashboard + thread | DashboardCommand, ThreadCommand, ThreadUtil, ThreadSampler | JMX 两次采样计算 CPU 使用率 |
| 接口响应慢排查 | trace + watch | EnhancerCommand, Enhancer, SpyImpl, TraceAdviceListener, WatchAdviceListener, OgnlExpress | ASM 字节码增强 + Invoke 指令级织入 + OGNL 表达式 |
| 动态修改日志级别 | logger + ognl | LoggerCommand, LoggerHelper, AsmRenameUtil, ReflectUtils, OgnlCommand | ASM 重命名注入 + 反射调用 / OGNL 表达式引擎 |
| 反编译确认代码 | jad | JadCommand, ClassDumpTransformer, Decompiler, InstrumentationUtils | retransformClasses 只读 dump + CFR 反编译 |

**贯穿四个场景的核心设计模式**：

1. **ClassFileTransformer 模式**：trace/watch 用它增强字节码（改写），jad 用它 dump 字节码（只读），logger 用它注入 Helper 类。同一个接口，三种用法。

2. **ClassLoader 隔离处理**：logger 通过 ASM 重命名注入目标 ClassLoader，ognl 通过 `-c` 指定 ClassLoader，jad 通过 `-c` 匹配 ClassLoader。多 ClassLoader 支持是 Arthas 区别于 JConsole、VisualVM 的核心优势之一。

3. **AdviceListener 回调模式**：trace 和 watch 都通过 `SpyAPI → SpyImpl → AdviceListenerManager → AdviceListener` 的回调链路实现运行时数据采集。字节码增强在编译/加载期完成，回调在运行期触发，实现了"零侵入"的运行时诊断。

4. **OGNL 表达式引擎**：watch 的条件表达式、trace 的 `#cost` 条件、ognl 命令的任意代码执行，底层都通过 OGNL 实现。`ExpressFactory` 提供了 ThreadLocal 缓存的执行器工厂，平衡了性能和灵活性。
# Arthas 源码级场景分析 —— 场景篇2

本文档涵盖四个真实生产场景的源码级深度分析：热更新代码（mc + redefine）、时间隧道（tt）、Spring Bean动态操作（ognl）、以及Tunnel Server远程诊断。每个场景从用户故事出发，追踪完整的源码调用链路，标注关键代码位置，并进行逐行解释。

---

## 场景五：热更新代码 —— mc + redefine 代码热修复

### 5.1 用户故事

线上电商系统在交易高峰期发现 `com.example.OrderService.createOrder()` 方法抛出 `NullPointerException`。经过日志排查，根因是 `orderMapper` 字段在某个反序列化路径下未被正确初始化。开发已经写好修复代码——在 `createOrder` 方法入口处增加空值检查和延迟初始化逻辑。但此时系统正在处理大量交易订单，重启服务会导致正在进行的交易中断，造成资金损失。

解决方案：通过Arthas的 `jad` → `mc` → `redefine` 三步走完成代码热修复，全程不重启JVM。

### 5.2 操作命令与参数说明

```bash
# 第一步：反编译确认当前代码
jad com.example.OrderService --source-only -d /tmp/arthas-src/

# 第二步：修改源代码（在外部编辑器中修改）
# vim /tmp/arthas-src/com/example/OrderService.java

# 第三步：内存编译
mc /tmp/arthas-src/com/example/OrderService.java -c 18b4aac2 -d /tmp/arthas-classes/

# 第四步：热替换
redefine /tmp/arthas-classes/com/example/OrderService.class

# 第五步：验证
jad com.example.OrderService createOrder
```

参数说明：
- `--source-only`：jad命令只输出Java源码，不包含ClassLoader信息
- `-d /tmp/arthas-src/`：输出目录
- `-c 18b4aac2`：指定ClassLoader的hashCode（十六进制），确保编译时能找到正确的依赖类
- `-d /tmp/arthas-classes/`：编译后class文件输出目录

### 5.3 源码链路追踪：mc命令

#### 5.3.1 MemoryCompilerCommand.process() —— 入口

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/MemoryCompilerCommand.java`

```java
@Override
public void process(final CommandProcess process) {
    RowAffect affect = new RowAffect();

    try {
        Instrumentation inst = process.session().getInstrumentation();
```

`process()` 方法是mc命令的入口。首先通过 `process.session().getInstrumentation()` 获取JVM的 `Instrumentation` 实例。这个实例是Arthas通过Java Agent机制在启动时注入的，它提供了操作已加载类的能力。在mc命令中，`inst` 主要用于查找ClassLoader。

#### 5.3.2 ClassLoader解析逻辑

```java
if (hashCode == null && classLoaderClass != null) {
    List<ClassLoader> matchedClassLoaders = ClassLoaderUtils.getClassLoaderByClassName(inst, classLoaderClass);
    if (matchedClassLoaders.size() == 1) {
        hashCode = Integer.toHexString(matchedClassLoaders.get(0).hashCode());
    } else if (matchedClassLoaders.size() > 1) {
        Collection<ClassLoaderVO> classLoaderVOList = ClassUtils.createClassLoaderVOList(matchedClassLoaders);
        MemoryCompilerModel memoryCompilerModel = new MemoryCompilerModel()
                .setClassLoaderClass(classLoaderClass)
                .setMatchedClassLoaders(classLoaderVOList);
        process.appendResult(memoryCompilerModel);
        process.end(-1, "Found more than one classloader by class name, please specify classloader with '-c <classloader hash>'");
        return;
    } else {
        process.end(-1, "Can not find classloader by class name: " + classLoaderClass + ".");
        return;
    }
}
```

这段代码处理两种ClassLoader指定方式：
1. **`-c hashCode`**：直接用ClassLoader实例的hashCode十六进制表示
2. **`--classLoaderClass className`**：通过ClassLoader的类名查找

如果用类名查找，可能匹配到多个ClassLoader实例（比如同一个Web应用部署了多个副本）。此时Arthas会返回所有匹配的ClassLoader列表，要求用户用 `-c` 明确指定。这是一个防御性设计——避免编译时用错ClassLoader导致找不到依赖类。

当用户没有指定任何ClassLoader时，代码走到：

```java
ClassLoader classloader = null;
if (hashCode == null) {
    classloader = ClassLoader.getSystemClassLoader();
} else {
    classloader = ClassLoaderUtils.getClassLoader(inst, hashCode);
    if (classloader == null) {
        process.end(-1, "Can not find classloader with hashCode: " + hashCode + ".");
        return;
    }
}
```

默认使用 `SystemClassLoader`。但在实际生产中，Spring Boot应用的业务类通常由 `LaunchedURLClassLoader` 加载，Tomcat应用由 `WebappClassLoader` 加载，如果用SystemClassLoader编译，很可能找不到业务依赖类导致编译失败。这就是为什么用户需要通过 `-c` 指定正确的ClassLoader。

#### 5.3.3 DynamicCompiler创建与编译

```java
DynamicCompiler dynamicCompiler = new DynamicCompiler(classloader);

Charset charset = Charset.defaultCharset();
if (encoding != null) {
    charset = Charset.forName(encoding);
}

for (String sourceFile : sourcefiles) {
    String sourceCode = FileUtils.readFileToString(new File(sourceFile), charset);
    String name = new File(sourceFile).getName();
    if (name.endsWith(".java")) {
        name = name.substring(0, name.length() - ".java".length());
    }
    dynamicCompiler.addSource(name, sourceCode);
}

Map<String, byte[]> byteCodes = dynamicCompiler.buildByteCodes();
```

逐行分析：
1. `new DynamicCompiler(classloader)`：创建内存编译器，传入目标ClassLoader。这个ClassLoader决定了编译时能找到哪些依赖类。
2. `FileUtils.readFileToString(...)`：读取源文件内容为字符串。
3. `name.substring(...)`：从文件名中提取类名，去掉 `.java` 后缀。注意这里用的是文件名而不是package声明，因此文件名必须与public类名一致。
4. `dynamicCompiler.addSource(name, sourceCode)`：将源码添加到编译单元列表中。
5. `dynamicCompiler.buildByteCodes()`：执行编译，返回类名到字节码的映射。

#### 5.3.4 输出class文件

```java
File outputDir = null;
if (this.directory != null) {
    outputDir = new File(this.directory);
} else {
    outputDir = new File("").getAbsoluteFile();
}

List<String> files = new ArrayList<String>();
for (Entry<String, byte[]> entry : byteCodes.entrySet()) {
    File byteCodeFile = new File(outputDir, entry.getKey().replace('.', '/') + ".class");
    FileUtils.writeByteArrayToFile(byteCodeFile, entry.getValue());
    files.add(byteCodeFile.getAbsolutePath());
    affect.rCnt(1);
}
process.appendResult(new MemoryCompilerModel(files));
process.appendResult(new RowAffectModel(affect));
process.end();
```

关键点：`entry.getKey().replace('.', '/') + ".class"` —— 将类名的包分隔符 `.` 替换为路径分隔符 `/`，生成正确的目录结构。比如 `com.example.OrderService` 会输出到 `outputDir/com/example/OrderService.class`。如果编译生成了内部类，也会一并输出。

### 5.4 DynamicCompiler内部实现

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/DynamicCompiler.java`

#### 5.4.1 构造函数

```java
public class DynamicCompiler {
    private final JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    private final StandardJavaFileManager standardFileManager;
    private final List<String> options = new ArrayList<String>();
    private final DynamicClassLoader dynamicClassLoader;
    private final Collection<JavaFileObject> compilationUnits = new ArrayList<JavaFileObject>();

    public DynamicCompiler(ClassLoader classLoader) {
        if (javaCompiler == null) {
            throw new IllegalStateException(
                "Can not load JavaCompiler from javax.tools.ToolProvider#getSystemJavaCompiler(),"
                + " please confirm the application running in JDK not JRE.");
        }
        standardFileManager = javaCompiler.getStandardFileManager(null, null, null);
        options.add("-Xlint:unchecked");
        options.add("-g");
        dynamicClassLoader = new DynamicClassLoader(classLoader);
    }
```

核心组件：
- `javaCompiler`：通过 `ToolProvider.getSystemJavaCompiler()` 获取JDK内置编译器（`com.sun.tools.javac.api.JavacTool`）。如果运行在JRE而非JDK上，此方法返回null，会抛出异常。
- `standardFileManager`：标准文件管理器，负责查找JDK自身的类（`rt.jar` / `java.base` 模块等）。
- `options`：编译选项。`-Xlint:unchecked` 抑制泛型未检查警告，`-g` 生成调试信息（行号、局部变量表）。
- `dynamicClassLoader`：自定义ClassLoader，包装了用户传入的ClassLoader，用于编译时查找业务依赖类和存储编译结果。

#### 5.4.2 addSource与StringSource

```java
public void addSource(String className, String source) {
    addSource(new StringSource(className, source));
}
```

**StringSource源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/StringSource.java`

```java
public class StringSource extends SimpleJavaFileObject {
    private final String contents;

    public StringSource(String className, String contents) {
        super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
        this.contents = contents;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        return contents;
    }
}
```

`StringSource` 继承 `SimpleJavaFileObject`，将Java源码字符串包装成 `JavaFileObject`。URI使用 `string:///` 协议（自定义协议），路径为类的全限定名转换成的路径。当编译器需要读取源码内容时，调用 `getCharContent()` 返回字符串。

#### 5.4.3 buildByteCodes() 编译流程

```java
public Map<String, byte[]> buildByteCodes() {
    errors.clear();
    warnings.clear();

    JavaFileManager fileManager = new DynamicJavaFileManager(standardFileManager, dynamicClassLoader);

    DiagnosticCollector<JavaFileObject> collector = new DiagnosticCollector<JavaFileObject>();
    JavaCompiler.CompilationTask task = javaCompiler.getTask(null, fileManager, collector, options, null,
                    compilationUnits);

    try {
        if (!compilationUnits.isEmpty()) {
            boolean result = task.call();
            if (!result || collector.getDiagnostics().size() > 0) {
                for (Diagnostic<? extends JavaFileObject> diagnostic : collector.getDiagnostics()) {
                    switch (diagnostic.getKind()) {
                    case NOTE:
                    case MANDATORY_WARNING:
                    case WARNING:
                        warnings.add(diagnostic);
                        break;
                    case OTHER:
                    case ERROR:
                    default:
                        errors.add(diagnostic);
                        break;
                    }
                }
                if (!errors.isEmpty()) {
                    throw new DynamicCompilerException("Compilation Error", errors);
                }
            }
        }
        return dynamicClassLoader.getByteCodes();
    } catch (ClassFormatError e) {
        throw new DynamicCompilerException(e, errors);
    } finally {
        compilationUnits.clear();
    }
}
```

编译流程分解：
1. **创建DynamicJavaFileManager**：包装标准文件管理器，使其能从指定的ClassLoader中查找依赖类。
2. **创建DiagnosticCollector**：收集编译过程中的诊断信息（错误、警告等）。
3. **创建CompilationTask**：`javaCompiler.getTask(null, fileManager, collector, options, null, compilationUnits)` 参数依次为：Writer（null表示用System.err）、FileManager、诊断收集器、编译选项、要编译的类名列表（null表示编译所有compilationUnits）、编译单元集合。
4. **执行编译**：`task.call()` 返回boolean表示是否成功。
5. **错误处理**：遍历诊断信息，将ERROR类型的放入errors列表，WARNING类型的放入warnings列表。如果有错误，抛出 `DynamicCompilerException`。
6. **获取字节码**：`dynamicClassLoader.getByteCodes()` 返回所有编译生成的字节码。

#### 5.4.4 DynamicJavaFileManager —— 自定义文件管理器

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/DynamicJavaFileManager.java`

```java
public class DynamicJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {
    private static final String[] superLocationNames = { StandardLocation.PLATFORM_CLASS_PATH.name(),
            "SYSTEM_MODULES" };
    private final PackageInternalsFinder finder;
    private final DynamicClassLoader classLoader;
    private final List<MemoryByteCode> byteCodes = new ArrayList<MemoryByteCode>();

    public DynamicJavaFileManager(JavaFileManager fileManager, DynamicClassLoader classLoader) {
        super(fileManager);
        this.classLoader = classLoader;
        this.finder = new PackageInternalsFinder(classLoader);
    }
```

`DynamicJavaFileManager` 继承 `ForwardingJavaFileManager`，这是JDK提供的装饰器模式基类，默认将所有方法委托给被包装的FileManager。DynamicJavaFileManager重写了几个关键方法：

**getJavaFileForOutput() —— 编译输出拦截**：

```java
@Override
public JavaFileObject getJavaFileForOutput(JavaFileManager.Location location, String className,
                JavaFileObject.Kind kind, FileObject sibling) throws IOException {
    for (MemoryByteCode byteCode : byteCodes) {
        if (byteCode.getClassName().equals(className)) {
            return byteCode;
        }
    }
    MemoryByteCode innerClass = new MemoryByteCode(className);
    byteCodes.add(innerClass);
    classLoader.registerCompiledSource(innerClass);
    return innerClass;
}
```

当编译器生成字节码时，会调用此方法获取输出目标。Arthas不写入磁盘文件，而是返回 `MemoryByteCode` 对象——一个基于 `ByteArrayOutputStream` 的内存输出流。每次调用都会创建新的 `MemoryByteCode` 并注册到 `DynamicClassLoader` 中，这样内部类也能被正确处理。

**list() 与 inferBinaryName() —— 依赖类查找**：

```java
@Override
public String inferBinaryName(Location location, JavaFileObject file) {
    if (file instanceof CustomJavaFileObject) {
        return ((CustomJavaFileObject) file).getClassName();
    } else {
        return super.inferBinaryName(location, file);
    }
}

@Override
public Iterable<JavaFileObject> list(Location location, String packageName, Set<JavaFileObject.Kind> kinds,
                                     boolean recurse) throws IOException {
    if (location instanceof StandardLocation) {
        String locationName = ((StandardLocation) location).name();
        for (String name : superLocationNames) {
            if (name.equals(locationName)) {
                return super.list(location, packageName, kinds, recurse);
            }
        }
    }
    if (location == StandardLocation.CLASS_PATH && kinds.contains(JavaFileObject.Kind.CLASS)) {
        return new IterableJoin<JavaFileObject>(super.list(location, packageName, kinds, recurse),
                finder.find(packageName));
    }
    return super.list(location, packageName, kinds, recurse);
}
```

这两个方法配合工作。当编译器需要查找某个包下的类时：
1. 先调用 `list()` 列出该包下所有可用的类文件
2. 对每个文件调用 `inferBinaryName()` 获取二进制名

对于JDK自身的类（`PLATFORM_CLASS_PATH` / `SYSTEM_MODULES`），委托给标准文件管理器处理。对于业务类（`CLASS_PATH`），通过 `PackageInternalsFinder` 从指定的ClassLoader中查找。`IterableJoin` 将标准结果和自定义查找结果合并，确保编译器能看到所有依赖类。

#### 5.4.5 DynamicClassLoader —— 编译结果的载体

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/DynamicClassLoader.java`

```java
public class DynamicClassLoader extends ClassLoader {
    private final Map<String, MemoryByteCode> byteCodes = new HashMap<String, MemoryByteCode>();

    public DynamicClassLoader(ClassLoader classLoader) {
        super(classLoader);
    }

    public void registerCompiledSource(MemoryByteCode byteCode) {
        byteCodes.put(byteCode.getClassName(), byteCode);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        MemoryByteCode byteCode = byteCodes.get(name);
        if (byteCode == null) {
            return super.findClass(name);
        }
        return super.defineClass(name, byteCode.getByteCode(), 0, byteCode.getByteCode().length);
    }

    public Map<String, byte[]> getByteCodes() {
        Map<String, byte[]> result = new HashMap<String, byte[]>(byteCodes.size());
        for (Entry<String, MemoryByteCode> entry : byteCodes.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getByteCode());
        }
        return result;
    }
}
```

`DynamicClassLoader` 的parent就是用户通过 `-c` 指定的ClassLoader。当编译器需要查找某个类是否已存在时，会先检查 `byteCodes` 中是否已有编译结果，没有则委托给parent ClassLoader查找。`getByteCodes()` 方法返回所有编译结果的字节码，这就是mc命令最终输出的内容。

### 5.5 源码链路追踪：redefine命令

#### 5.5.1 RedefineCommand.process() —— 入口

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/RedefineCommand.java`

```java
@Override
public void process(CommandProcess process) {
    RedefineModel redefineModel = new RedefineModel();
    Instrumentation inst = process.session().getInstrumentation();
```

redefine命令同样先获取 `Instrumentation` 实例。与mc命令不同，redefine需要用 `Instrumentation` 来执行类的热替换。

#### 5.5.2 文件校验

```java
for (String path : paths) {
    File file = new File(path);
    if (!file.exists()) {
        process.end(-1, "file does not exist, path:" + path);
        return;
    }
    if (!file.isFile()) {
        process.end(-1, "not a normal file, path: " + path);
        return;
    }
    if (file.length() >= MAX_FILE_SIZE) {
        process.end(-1, "file size: " + file.length() + " >= " + MAX_FILE_SIZE + ", path: " + path);
        return;
    }
}
```

`MAX_FILE_SIZE = 10 * 1024 * 1024`（10MB）。这是防御性检查——防止用户误传大文件导致OOM。正常的class文件通常只有几KB到几十KB。

#### 5.5.3 读取字节码与ASM解析类名

```java
Map<String, byte[]> bytesMap = new HashMap<String, byte[]>();
for (String path : paths) {
    RandomAccessFile f = null;
    try {
        f = new RandomAccessFile(path, "r");
        final byte[] bytes = new byte[(int) f.length()];
        f.readFully(bytes);

        final String clazzName = readClassName(bytes);

        bytesMap.put(clazzName, bytes);
    } catch (Exception e) {
        logger.warn("load class file failed: "+path, e);
        process.end(-1, "load class file failed: " +path+", error: " + e);
        return;
    } finally {
        if (f != null) {
            try { f.close(); } catch (IOException e) { /* ignore */ }
        }
    }
}
```

使用 `RandomAccessFile` 以只读模式读取class文件。`readClassName(bytes)` 方法如下：

```java
private static String readClassName(final byte[] bytes) {
    return new ClassReader(bytes).getClassName().replace("/", ".");
}
```

这里使用ASM的 `ClassReader` 解析class文件字节码，直接从常量池中提取类名。这是一个非常巧妙的设计——不需要加载类到JVM中，仅通过字节码解析就能获取全限定类名。`getClassName()` 返回内部格式（用 `/` 分隔包），所以需要 `replace("/", ".")` 转换为Java标准格式。

为什么不直接从文件路径推导类名？因为用户可能在任意目录放置class文件，文件路径不一定反映包结构。从字节码本身读取类名是最可靠的方式。

#### 5.5.4 匹配已加载的类

```java
List<ClassDefinition> definitions = new ArrayList<ClassDefinition>();
for (Class<?> clazz : inst.getAllLoadedClasses()) {
    if (bytesMap.containsKey(clazz.getName())) {
        if (hashCode == null && classLoaderClass != null) {
            List<ClassLoader> matchedClassLoaders = ClassLoaderUtils.getClassLoaderByClassName(inst, classLoaderClass);
            if (matchedClassLoaders.size() == 1) {
                hashCode = Integer.toHexString(matchedClassLoaders.get(0).hashCode());
            } else if (matchedClassLoaders.size() > 1) {
                // ... 多个匹配的提示逻辑
            } else {
                process.end(-1, "Can not find classloader by class name: " + classLoaderClass + ".");
                return;
            }
        }
        
        ClassLoader classLoader = clazz.getClassLoader();
        if (classLoader != null && hashCode != null && !Integer.toHexString(classLoader.hashCode()).equals(hashCode)) {
            continue;
        }
        definitions.add(new ClassDefinition(clazz, bytesMap.get(clazz.getName())));
        redefineModel.addRedefineClass(clazz.getName());
        logger.info("Try redefine class name: {}, ClassLoader: {}", clazz.getName(), clazz.getClassLoader());
    }
}
```

核心匹配逻辑：
1. 遍历 `inst.getAllLoadedClasses()` 获取JVM中所有已加载的类
2. 用类名匹配——如果class文件中的类名与已加载的某个类名相同，则认为是目标类
3. 如果指定了 `-c hashCode`，还要验证该类的ClassLoader是否匹配。这是为了处理同名类被不同ClassLoader加载的情况（在OSGi、Tomcat等环境中很常见）
4. `new ClassDefinition(clazz, bytesMap.get(clazz.getName()))` 创建类定义——包含已加载的Class对象和新的字节码

注意 `classLoader != null` 的判断：如果类的ClassLoader为null（即BootstrapClassLoader加载的类），则不进行ClassLoader匹配，直接允许redefine。

#### 5.5.5 执行热替换

```java
try {
    if (definitions.isEmpty()) {
        process.end(-1, "These classes are not found in the JVM and may not be loaded: " + bytesMap.keySet());
        return;
    }
    inst.redefineClasses(definitions.toArray(new ClassDefinition[0]));
    process.appendResult(redefineModel);
    process.end();
} catch (Throwable e) {
    String message = "redefine error! " + e.toString();
    logger.error(message, e);
    process.end(-1, message);
}
```

`inst.redefineClasses()` 是JVMTI层面的操作。它将传入的 `ClassDefinition` 数组中的每个类替换为新字节码。这个过程是原子性的——要么全部成功，要么全部失败。

JVM对redefine的限制（由JVMTI规范定义）：
- 不能添加新字段
- 不能删除已有字段
- 不能修改字段类型
- 不能添加新方法
- 不能删除已有方法
- 不能修改方法签名（参数类型、返回类型）
- 不能改变类的继承关系
- 不能改变类实现的接口
- 不能修改方法的访问修饰符（public/private/protected）

违反这些限制时，`redefineClasses()` 会抛出 `UnsupportedOperationException`。

### 5.6 redefine vs retransform 的区别

| 维度 | redefine | retransform |
|------|----------|-------------|
| **字节码来源** | 完全替换为新字节码 | 基于现有字节码做修改 |
| **JVM接口** | `Instrumentation.redefineClasses()` | `Instrumentation.retransformClasses()` |
| **ClassFileTransformer** | 不经过Transformer | 会触发已注册的ClassFileTransformer |
| **应用场景** | mc编译的新代码直接替换 | Arthas的watch/trace等增强 |
| **JVM限制** | 不能改变类结构 | 同样不能改变类结构 |
| **能否回退** | 不能自动回退（需要再次redefine原代码） | 移除Transformer后retransform可恢复 |

在Arthas中，watch/trace/monitor等命令使用的是retransform——它们通过注册 `ClassFileTransformer` 在现有字节码基础上插入增强代码。而redefine是直接替换整个类的字节码，不经过Transformer链。

### 5.7 完整链路总结

```
mc命令链路：
ShellLineHandler → ProcessImpl → MemoryCompilerCommand.process()
  → ClassLoaderUtils.getClassLoader(inst, hashCode)  // 解析ClassLoader
  → new DynamicCompiler(classloader)                  // 创建编译器
  → DynamicCompiler.addSource(name, sourceCode)       // 添加源码
    → new StringSource(className, source)              // 包装为JavaFileObject
  → DynamicCompiler.buildByteCodes()                  // 执行编译
    → new DynamicJavaFileManager(standardFileManager, dynamicClassLoader)
    → javaCompiler.getTask(null, fileManager, collector, options, null, compilationUnits)
    → task.call()                                     // JDK编译器执行
    → dynamicClassLoader.getByteCodes()               // 获取编译结果
  → FileUtils.writeByteArrayToFile(byteCodeFile, bytes)  // 写入.class文件
  → process.appendResult(MemoryCompilerModel)
  → process.end()

redefine命令链路：
ShellLineHandler → ProcessImpl → RedefineCommand.process()
  → new RandomAccessFile(path, "r")                   // 读取.class文件
  → new ClassReader(bytes).getClassName()              // ASM解析类名
  → inst.getAllLoadedClasses()                         // 遍历已加载类
  → 匹配类名 + ClassLoader hashCode
  → new ClassDefinition(clazz, bytes)                  // 创建类定义
  → inst.redefineClasses(definitions)                  // JVM热替换
  → process.appendResult(RedefineModel)
  → process.end()
```

### 5.8 Q&A

**Q1: 为什么mc编译时必须指定正确的ClassLoader（-c参数）？**

A: Java编译器在编译时需要解析所有引用的类。比如 `OrderService` 引用了 `OrderMapper`，编译器必须能找到 `OrderMapper` 的class文件。在Spring Boot应用中，业务类由 `LaunchedURLClassLoader` 加载，如果用默认的 `SystemClassLoader`，编译器找不到 `OrderMapper`，会报 "cannot find symbol" 错误。通过 `-c` 指定正确的ClassLoader，`DynamicJavaFileManager` 的 `PackageInternalsFinder` 会从该ClassLoader中查找依赖类。

**Q2: redefine后原来的字节码还能恢复吗？**

A: 不能自动恢复。redefine是用新字节码完全替换旧字节码，JVM不保留旧版本。如果需要恢复，有两种方式：(1) 用jad反编译当前代码（但反编译结果可能与原始源码有差异），修改后重新mc + redefine；(2) 保存原始class文件，需要时redefine回去。在生产实践中，建议在redefine前先备份原始class文件。

**Q3: 如果redefine的代码有bug会怎样？**

A: 如果bug导致方法执行时抛出异常，异常会正常传播到调用方。JVM不会因为redefine的代码有bug而崩溃。但如果新代码导致死循环或内存泄漏，会影响整个JVM。因此建议在redefine前充分测试新代码，可以先在测试环境验证。

**Q4: redefine能修改方法内的所有逻辑吗？**

A: 在不改变方法签名的前提下，方法体内的逻辑可以完全替换。你可以修改方法体、增加局部变量、修改控制流等。但不能添加新方法、新字段或修改方法签名。如果需要结构性变更，需要考虑其他方案（如使用字节码操作工具直接修改class文件）。

**Q5: mc + redefine 和 jad --source-only 修改后直接redefine有什么区别？**

A: `jad --source-only` 反编译的代码可能不完全准确——比如lambda表达式、泛型擦除、switch语句的编译优化等都可能导致反编译代码与原始代码有差异。直接修改反编译代码再编译，可能引入新的问题。最佳实践是使用版本控制中的原始源码进行修改，然后mc编译。

### 5.9 场景总结

mc + redefine组合提供了一种无需重启JVM即可修复代码的能力。mc命令通过JDK内置的 `javax.tools.JavaCompiler` 实现内存编译，核心在于 `DynamicJavaFileManager` 和 `DynamicClassLoader` 的协作——前者拦截编译器的类查找请求，从指定ClassLoader中查找依赖类；后者存储编译结果并提供字节码。redefine命令则通过ASM解析class文件中的类名，匹配JVM已加载的类，最终调用 `Instrumentation.redefineClasses()` 完成热替换。整个过程不涉及类的重新加载，而是直接替换方法区的字节码，对运行中的业务线程透明。

---

## 场景六：方法调用记录与回放 —— tt 时间隧道

### 6.1 用户故事

支付系统的 `com.example.PaymentService.processPayment()` 方法偶发性失败，错误率为0.1%。日志只记录了"处理失败"但没有详细信息。由于无法稳定复现，开发需要一种机制能够记录每次方法调用的完整上下文——包括入参、返回值、异常、耗时——然后在失败发生时回溯查看详情，甚至重放那次失败的调用。

Arthas的tt（Time Tunnel）命令正是为这种场景设计的：它能记录方法调用的"时间碎片"（TimeFragment），保存完整的调用上下文，并支持后续查看、搜索和重放。

### 6.2 操作命令与参数说明

```bash
# 第一步：记录方法调用
tt -t com.example.PaymentService processPayment

# 等待一段时间，收集多次调用...

# 第二步：查看所有记录
tt -l

# 第三步：查看指定记录的详情
tt -i 1001

# 第四步：对失败的调用执行OGNL查看
tt -i 1003 -w "throwExp.getMessage()"

# 第五步：重放调用
tt -i 1003 -p
```

参数说明：
- `-t`：开启时间隧道记录模式，对匹配的方法进行字节码增强
- `-l`：列出所有已记录的时间碎片
- `-i 1001`：指定时间碎片的INDEX
- `-w "throwExp.getMessage()"`：对指定时间碎片执行OGNL表达式
- `-p`：重放指定时间碎片的调用
- `-n 100`：最大记录次数（默认100）
- `--replay-times 3`：重放次数
- `--replay-interval 3000`：重放间隔（毫秒）

### 6.3 源码链路追踪：tt -t 记录模式

#### 6.3.1 TimeTunnelCommand.process() —— 命令路由

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TimeTunnelCommand.java`

```java
@Override
public void process(final CommandProcess process) {
    // 检查参数
    checkArguments();

    // ctrl-C support
    process.interruptHandler(new CommandInterruptHandler(process));
    // q exit support
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

tt命令通过flag参数进行路由分发。`-t` 标志触发 `enhance()` 进行字节码增强，这是记录模式的核心入口。其他模式（`-l`、`-i`、`-p`、`-d`等）走各自的处理分支。这种设计使得一个命令能承担多种操作，用户不需要记忆多个命令名。

`checkArguments()` 方法验证参数合法性：

```java
private void checkArguments() {
    String validateError = validateSizeLimit(sizeLimit);
    if (validateError != null) {
        throw new IllegalArgumentException(validateError);
    }
    // 检查d/p参数是否有i参数配套
    if ((isDelete || isPlay) && null == index) {
        throw new IllegalArgumentException("Time fragment index is expected, please type -i to specify");
    }
    // 在t参数下class-pattern,method-pattern
    if (isTimeTunnel) {
        if (StringUtils.isEmpty(classPattern)) {
            throw new IllegalArgumentException("Class-pattern is expected...");
        }
        if (StringUtils.isEmpty(methodPattern)) {
            throw new IllegalArgumentException("Method-pattern is expected...");
        }
    }
    // 一个参数都没有是不行滴
    if (null == index && !isTimeTunnel && !isDeleteAll && StringUtils.isEmpty(watchExpress)
            && !isList && StringUtils.isEmpty(searchExpress)) {
        throw new IllegalArgumentException("Argument(s) is/are expected, type 'help tt' to read usage");
    }
}
```

#### 6.3.2 EnhancerCommand.enhance() —— 字节码增强

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/EnhancerCommand.java`

`TimeTunnelCommand` 继承自 `EnhancerCommand`，`enhance()` 方法定义在父类中：

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
    try {
        Instrumentation inst = session.getInstrumentation();
        AdviceListener listener = getAdviceListenerWithId(process);
        if (listener == null) {
            // ...
            return;
        }

        Enhancer enhancer = new Enhancer(listener, listener instanceof InvokeTraceable, skipJDKTrace,
                getClassNameMatcher(), getClassNameExcludeMatcher(), getMethodNameMatcher(), this.lazy, this.hashCode);
        enhancer.setLineEnhanceOptions(getLineEnhanceOptions());
        // 注册通知监听器
        process.register(listener, enhancer);
        effect = enhancer.enhance(inst, this.maxNumOfMatchedClass);
        // ...
        process.appendResult(EnhancerModelFactory.create(effect, true));
        scheduleTimeoutTask(process);
        //异步执行，在AdviceListener中结束
    } catch (Throwable e) {
        // ...
    } finally {
        if (session.getLock() == lock) {
            process.session().unLock();
        }
    }
}
```

关键流程：
1. **Session加锁**：`session.tryLock()` 确保同一时刻只有一个增强命令在执行。这是必要的，因为同时增强同一个类可能导致字节码冲突。
2. **创建AdviceListener**：`getAdviceListenerWithId(process)` 最终调用 `TimeTunnelCommand.getAdviceListener()`。
3. **创建Enhancer**：`Enhancer` 是字节码增强的核心类，它使用ASM修改目标类的字节码，在方法入口和出口处插入对 `SpyAPI` 的调用。
4. **执行增强**：`enhancer.enhance(inst, maxNumOfMatchedClass)` 通过 `Instrumentation.retransformClasses()` 触发字节码重转换。
5. **异步执行**：增强完成后命令不立即结束，而是等待 `AdviceListener` 的回调。

#### 6.3.3 getAdviceListener() —— 创建TimeTunnelAdviceListener

```java
@Override
protected AdviceListener getAdviceListener(CommandProcess process) {
    return new TimeTunnelAdviceListener(this, process, GlobalOptions.verbose || this.verbose);
}
```

创建了 `TimeTunnelAdviceListener`，传入当前命令实例（用于访问配置参数）、CommandProcess（用于输出结果）和verbose标志。

### 6.4 TimeTunnelAdviceListener 回调链路

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TimeTunnelAdviceListener.java`

#### 6.4.1 Ring Stack设计 —— 防止ClassLoader泄漏

```java
/**
 * 用 JDK 的 Object[] 做一个固定大小的 ring stack（只存业务对象），避免把 ArthasClassLoader 加载的 ObjectStack 放进
 * 业务线程的 ThreadLocalMap 里，导致 stop/detach 后 ArthasClassLoader 无法被 GC 回收。
 *
 * 约定：
 * - store[0] 存储 int[1] 的 pos（0..cap）
 * - store[1..cap] 存储 args（Object[]）
 */
private static final int ARGS_STACK_SIZE = 512;
private final ThreadLocal<Object[]> argsRef = ThreadLocal.withInitial(() -> {
    Object[] store = new Object[ARGS_STACK_SIZE + 1];
    store[0] = new int[1];
    return store;
});
```

这是Arthas源码中一个极其精妙的设计。为什么不用 `ThreadLocal<Advice>` 或 `ThreadLocal<ObjectStack>`？

问题在于ClassLoader泄漏。Arthas自身通过 `ArthasClassLoader` 加载，如果将Arthas的类（如 `ObjectStack`、`Advice` 等）直接放入业务线程的 `ThreadLocalMap`，会形成强引用链：

```
业务线程 → ThreadLocalMap → Entry → value(Arthas类的实例)
  → ArthasClassLoader (加载该value的类)
```

当Arthas stop/detach后，由于业务线程仍然存活，`ThreadLocalMap` 中的引用不会被释放，导致 `ArthasClassLoader` 无法被GC回收，进而导致Arthas加载的所有类都无法卸载，造成Metaspace泄漏。

解决方案是只用JDK自带的 `Object[]` 和 `int[]`——这些类由BootstrapClassLoader加载，不会引用 `ArthasClassLoader`。即使Arthas detach后，ThreadLocal中的 `Object[]` 不会阻止 `ArthasClassLoader` 被GC回收。

Ring Stack的工作原理：
- `store[0]` 存储一个 `int[1]`，其中 `int[0]` 是当前栈顶位置
- `store[1]` 到 `store[512]` 存储方法入参（`Object[]` 类型）
- 栈满时（pos >= cap），重置到位置1，覆盖最老的记录

**pushArgs() —— 压栈**：

```java
private void pushArgs(Object[] args) {
    Object[] store = argsRef.get();
    int[] posHolder = (int[]) store[0];

    int cap = store.length - 1;
    int pos = posHolder[0];
    if (pos < cap) {
        pos++;
    } else {
        // if stack is full, reset pos
        pos = 1;
    }
    store[pos] = args;
    posHolder[0] = pos;
}
```

**popArgs() —— 弹栈**：

```java
private Object[] popArgs() {
    Object[] store = argsRef.get();
    int[] posHolder = (int[]) store[0];

    int cap = store.length - 1;
    int pos = posHolder[0];
    if (pos > 0) {
        Object[] args = (Object[]) store[pos];
        store[pos] = null;
        posHolder[0] = pos - 1;
        return args;
    }

    pos = cap;
    Object[] args = (Object[]) store[pos];
    store[pos] = null;
    posHolder[0] = pos - 1;
    return args;
}
```

当pos为0时（栈空或刚好回绕），从cap位置取数据。这处理了ring stack回绕的情况。

#### 6.4.2 before() —— 方法调用前置通知

```java
@Override
public void before(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args)
        throws Throwable {
    pushArgs(args);
    threadLocalWatch.start();
}
```

`before()` 在被增强的方法入口处被调用（通过SpyAPI → AdviceWeaver → AdviceListenerAdapter的调用链触发）。做两件事：
1. `pushArgs(args)`：将方法入参压入ring stack。这是因为方法执行过程中参数可能被修改，我们需要记录原始入参。
2. `threadLocalWatch.start()`：开始计时。`ThreadLocalWatch` 内部使用 `ThreadLocal<Long>` 存储 `System.nanoTime()` 的值。

#### 6.4.3 afterReturning() —— 方法正常返回通知

```java
@Override
public void afterReturning(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                           Object returnObject) throws Throwable {
    //取出入参时的 args，因为在函数执行过程中 args可能被修改
    Object[] realArgs = popArgs();
    if (realArgs != null) {
        args = realArgs;
    }
    afterFinishing(Advice.newForAfterReturning(loader, clazz, method, target, args, returnObject));
}
```

方法正常返回时：
1. `popArgs()` 取回方法执行前保存的原始入参。注释说明了原因："在函数执行过程中args可能被修改"——Java中数组是引用传递，方法体内可能修改了数组元素的值。
2. 构造 `Advice` 对象——这是方法调用的完整上下文快照，包含ClassLoader、Class、Method、target对象、参数、返回值等。
3. 调用 `afterFinishing()` 完成TimeFragment的创建和存储。

#### 6.4.4 afterThrowing() —— 方法异常通知

```java
@Override
public void afterThrowing(ClassLoader loader, Class<?> clazz, ArthasMethod method, Object target, Object[] args,
                          Throwable throwable) {
    Object[] realArgs = popArgs();
    if (realArgs != null) {
        args = realArgs;
    }
    afterFinishing(Advice.newForAfterThrowing(loader, clazz, method, target, args, throwable));
}
```

与方法返回的逻辑几乎相同，区别在于构造的是异常版本的 `Advice`——`throwExp` 字段被设置为实际的异常对象，`returnObj` 为null。

#### 6.4.5 afterFinishing() —— 核心处理逻辑

```java
private void afterFinishing(Advice advice) {
    double cost = threadLocalWatch.costInMillis();
    TimeFragment timeTunnel = new TimeFragment(advice, LocalDateTime.now(), cost);

    boolean match = false;
    try {
        match = isConditionMet(command.getConditionExpress(), advice, cost);
        if (this.isVerbose()) {
            process.write("Condition express: " + command.getConditionExpress() + " , result: " + match + "\n");
        }
    } catch (ExpressException e) {
        logger.warn("tt failed.", e);
        process.end(-1, "tt failed, condition is: " + command.getConditionExpress() + ", " + e.getMessage()
                      + ", visit " + LogUtil.loggingFile() + " for more details.");
    }

    if (!match) {
        return;
    }

    int index = command.putTimeTunnel(timeTunnel);

    TimeFragmentVO timeFragmentVO = TimeTunnelCommand.createTimeFragmentVO(index, timeTunnel, command.getExpand());
    TimeTunnelModel timeTunnelModel = new TimeTunnelModel()
            .setTimeFragmentList(Collections.singletonList(timeFragmentVO))
            .setFirst(isFirst);
    process.appendResult(timeTunnelModel);

    if (isFirst) {
        isFirst = false;
    }

    process.times().incrementAndGet();
    if (isLimitExceeded(command.getNumberOfLimit(), process.times().get())) {
        abortProcess(process, command.getNumberOfLimit());
    }
}
```

逐段分析：

1. **计算耗时**：`threadLocalWatch.costInMillis()` 返回方法执行的毫秒耗时。

2. **创建TimeFragment**：`new TimeFragment(advice, LocalDateTime.now(), cost)` —— 时间碎片是Advice的时间戳封装，包含完整的调用上下文和时间信息。

**TimeFragment源码**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/TimeFragment.java`

```java
class TimeFragment {
    public TimeFragment(Advice advice, LocalDateTime gmtCreate, double cost) {
        this.advice = advice;
        this.gmtCreate = gmtCreate;
        this.cost = cost;
    }
    private final Advice advice;
    private final LocalDateTime gmtCreate;
    private final double cost;
}
```

3. **条件判断**：`isConditionMet()` 检查是否满足用户指定的条件表达式。如果没有条件表达式（`conditionExpress` 为空），则默认匹配所有调用。如果有条件（如 `params[0].length > 10`），则用OGNL表达式求值判断。

条件判断在 `AdviceListenerAdapter` 中实现：

```java
protected boolean isConditionMet(String conditionExpress, Advice advice, double cost) throws ExpressException {
    return StringUtils.isEmpty(conditionExpress)
            || ExpressFactory.threadLocalExpress(advice).bind(Constants.COST_VARIABLE, cost).is(conditionExpress);
}
```

4. **存储TimeFragment**：`command.putTimeTunnel(timeTunnel)` 将时间碎片存入静态Map：

```java
int putTimeTunnel(TimeFragment tt) {
    int indexOfSeq = sequence.getAndIncrement();
    timeFragmentMap.put(indexOfSeq, tt);
    return indexOfSeq;
}
```

`sequence` 是从1000开始的 `AtomicInteger`，每次记录递增。所以第一个时间碎片的INDEX是1000，第二个是1001，以此类推。`timeFragmentMap` 是 `LinkedHashMap`，保持插入顺序。

5. **输出结果**：构造 `TimeFragmentVO` 并通过 `process.appendResult()` 输出到终端。`isFirst` 标记用于第一次输出时显示表头。

6. **次数限制检查**：`isLimitExceeded()` 检查是否达到 `-n` 指定的最大记录次数。达到上限后调用 `abortProcess()` 结束命令。

### 6.5 tt -l 列出所有记录

```java
private void processList(CommandProcess process) {
    RowAffect affect = new RowAffect();
    List<TimeFragmentVO> timeFragmentList = createTimeTunnelVOList(timeFragmentMap);
    process.appendResult(new TimeTunnelModel().setTimeFragmentList(timeFragmentList).setFirst(true));
    affect.rCnt(timeFragmentMap.size());
    process.appendResult(new RowAffectModel(affect));
    process.end();
}
```

`createTimeTunnelVOList()` 遍历 `timeFragmentMap`，为每个TimeFragment创建VO：

```java
public static TimeFragmentVO createTimeFragmentVO(Integer index, TimeFragment tf, Integer expand) {
    Advice advice = tf.getAdvice();
    String object = advice.getTarget() == null
            ? "NULL"
            : "0x" + toHexString(advice.getTarget().hashCode());

    return new TimeFragmentVO()
            .setIndex(index)
            .setTimestamp(tf.getGmtCreate())
            .setCost(tf.getCost())
            .setParams(ObjectVO.array(advice.getParams(), expand))
            .setReturn(advice.isAfterReturning())
            .setReturnObj(new ObjectVO(advice.getReturnObj(), expand))
            .setThrow(advice.isAfterThrowing())
            .setThrowExp(new ObjectVO(advice.getThrowExp(), expand))
            .setObject(object)
            .setClassName(advice.getClazz().getName())
            .setMethodName(advice.getMethod().getName());
}
```

输出的表格列：
- INDEX：时间碎片编号
- TIMESTAMP：调用时间戳
- COST：耗时（毫秒）
- IS-RET：是否正常返回
- IS-EXP：是否抛出异常
- OBJECT：目标对象的hashCode（用于区分同一类的不同实例）
- CLASS：类名
- METHOD：方法名

### 6.6 tt -i 查看详情

```java
private void processShow(CommandProcess process) {
    RowAffect affect = new RowAffect();
    try {
        TimeFragment tf = timeFragmentMap.get(index);
        if (null == tf) {
            process.end(1, format("Time fragment[%d] does not exist.", index));
            return;
        }

        TimeFragmentVO timeFragmentVO = createTimeFragmentVO(index, tf, expand);
        TimeTunnelModel timeTunnelModel = new TimeTunnelModel()
                .setTimeFragment(timeFragmentVO)
                .setExpand(expand)
                .setSizeLimit(sizeLimit);
        process.appendResult(timeTunnelModel);
        affect.rCnt(1);
        process.appendResult(new RowAffectModel(affect));
        process.end();
    } catch (Throwable e) {
        logger.warn("tt failed.", e);
        process.end(1, e.getMessage() + ", visit " + LogUtil.loggingFile() + " for more detail");
    }
}
```

从 `timeFragmentMap` 中取出指定INDEX的TimeFragment，构造详情VO展示。与 `-l` 的区别在于 `-l` 输出列表（`setTimeFragmentList`），而 `-i` 输出单个详情（`setTimeFragment`），详情模式会展开参数和返回值的对象树。

### 6.7 tt -w 表达式查看

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

        Object value = ExpressFactory.unpooledExpress(advice.getLoader()).bind(advice).get(watchExpress);
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

关键行：`ExpressFactory.unpooledExpress(advice.getLoader()).bind(advice).get(watchExpress)`

1. `ExpressFactory.unpooledExpress(advice.getLoader())`：创建OGNL表达式执行器，使用原始调用时的ClassLoader。这很重要——如果表达式中引用了业务类（如 `throwExp.getErrorCode()`），需要用业务的ClassLoader来解析。
2. `.bind(advice)`：将Advice对象绑定为OGNL表达式的根对象。在OGNL中可以直接访问 `params`、`returnObj`、`throwExp` 等Advice的属性。
3. `.get(watchExpress)`：执行OGNL表达式并返回结果。

注意这里用的是 `unpooledExpress` 而不是 `threadLocalExpress`。因为tt -w是在命令处理线程中同步执行的，不存在ClassLoader泄漏的问题（不会在业务线程的ThreadLocal中留下引用）。使用unpooled可以确保使用正确的ClassLoader。

### 6.8 tt -p 重放

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

            //copy from tt record
            TimeFragmentVO replayResult = createTimeFragmentVO(index, tf, expand);
            replayResult.setTimestamp(LocalDateTime.now())
                    .setCost(0)
                    .setReturn(false)
                    .setReturnObj(null)
                    .setThrow(false)
                    .setThrowExp(null);

            try {
                //execute successful
                Object returnObj = method.invoke(advice.getTarget(), advice.getParams());
                double cost = (System.nanoTime() - beginTime) / 1000000.0;
                replayResult.setCost(cost)
                        .setReturn(true)
                        .setReturnObj(new ObjectVO(returnObj, expand));
            } catch (Throwable t) {
                //throw exp
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

重放逻辑分析：

1. **取出Advice**：从TimeFragment中取出保存的Advice，其中包含target对象、method和params。
2. **设置访问权限**：`method.setAccessible(true)` 允许调用private方法。在finally块中恢复原状。
3. **循环重放**：支持 `--replay-times` 多次重放，每次间隔 `--replay-interval` 毫秒。
4. **执行调用**：`method.invoke(advice.getTarget(), advice.getParams())` —— 使用保存的原始参数重放方法调用。注意target对象是记录时的那个实例，如果该实例已被GC回收或状态已改变，重放结果可能与原始调用不同。
5. **记录结果**：无论成功还是异常，都构造 `TimeFragmentVO` 记录重放结果。
6. **中断检查**：每次重放前检查 `process.isRunning()`，如果用户按了Q或Ctrl+C，则停止重放。

### 6.9 完整链路总结

```
记录链路：
EnhancerCommand.enhance()
  → session.tryLock()                               // 获取Session锁
  → getAdviceListener() → new TimeTunnelAdviceListener
  → new Enhancer(listener, ...)                     // 创建字节码增强器
  → enhancer.enhance(inst, maxNumOfMatchedClass)   // retransformClasses
  → [运行时] 被增强的方法被调用
    → SpyAPI.atEnter(...) → AdviceWeaver.methodOnBegin
    → AdviceListenerAdapter.before()
    → TimeTunnelAdviceListener.before()
      → pushArgs(args)                               // 入参压入ring stack
      → threadLocalWatch.start()                     // 开始计时
    → [业务方法执行]
    → SpyAPI.atExit(...) → AdviceWeaver.methodOnEnd
    → AdviceListenerAdapter.afterReturning()
    → TimeTunnelAdviceListener.afterReturning()
      → popArgs()                                    // 取回原始入参
      → Advice.newForAfterReturning(...)             // 构造Advice
      → afterFinishing(advice)
        → threadLocalWatch.costInMillis()            // 计算耗时
        → new TimeFragment(advice, now, cost)        // 创建时间碎片
        → isConditionMet()                           // 条件过滤
        → command.putTimeTunnel(timeTunnel)          // 存入timeFragmentMap
        → process.appendResult(TimeTunnelModel)      // 输出到终端
        → isLimitExceeded() → abortProcess()         // 次数限制检查

重放链路：
TimeTunnelCommand.process()
  → processPlay(process)
    → timeFragmentMap.get(index)                     // 取出TimeFragment
    → advice.getMethod().setAccessible(true)         // 设置访问权限
    → method.invoke(advice.getTarget(), advice.getParams())  // 反射调用
    → 构造replayResult VO
    → process.appendResult(TimeTunnelModel)
    → process.end()
```

### 6.10 Q&A

**Q1: 为什么ring stack的大小是512？**

A: 512是一个经验值。考虑到方法调用的嵌套深度，512层对于绝大多数应用场景已经足够。如果嵌套深度超过512（比如极深的递归），ring stack会回绕覆盖最老的记录。由于popArgs是LIFO（后进先出），正常情况下栈深度不会超过实际调用深度。只有当方法被增强但before和after不配对（比如方法被中断）时，才可能出现栈不平衡。

**Q2: tt记录的数据会一直占用内存吗？**

A: 是的，`timeFragmentMap` 是静态Map，记录的数据会一直保留在内存中，直到：(1) 用户执行 `tt --delete-all` 清除所有记录；(2) Arthas detach/unload。每个TimeFragment持有Advice对象，Advice持有params数组和returnObj的引用，如果这些对象很大，确实可能造成内存压力。建议在生产环境中设置合理的 `-n` 限制。

**Q3: 重放调用时会真正修改业务状态吗？**

A: 会的。`method.invoke(advice.getTarget(), advice.getParams())` 是真实的方法调用，会修改target对象的状态、写入数据库、发送消息等。因此重放需要谨慎——比如重放一个创建订单的方法会产生新的订单。建议只在只读方法或测试环境上重放。

**Q4: 如果方法在执行过程中参数被修改了，tt记录的是修改前还是修改后的参数？**

A: tt记录的是方法**入口时**的参数。在 `before()` 中通过 `pushArgs(args)` 保存了入参的引用。但注意，Java中数组是引用传递，`pushArgs` 保存的是数组引用而非副本。如果方法体内修改了数组元素的值（如 `args[0] = newValue`），保存的引用指向的数组内容也会被修改。不过，如果方法体内将参数重新赋值为新对象（如 `args = new Object[]{...}`），则不会影响保存的引用。

**Q5: tt -w 和 watch 命令有什么区别？**

A: watch是实时监控——方法被调用时立即执行OGNL表达式并输出结果，适合在线排查。tt -w是对历史记录执行OGNL表达式——从保存的TimeFragment中取出Advice，绑定到OGNL上下文中执行。tt -w不需要重新触发方法调用，适合事后分析。但tt -w只能访问Advice中保存的字段（params、returnObj、throwExp等），不能访问局部变量。

### 6.11 场景总结

tt时间隧道是Arthas中最具特色的功能之一。它通过字节码增强在方法调用前后插入回调，将每次调用的完整上下文（入参、返回值、异常、耗时）保存为TimeFragment。最精妙的设计是ring stack——使用JDK原生的 `Object[]` 和 `int[]` 在ThreadLocal中存储方法入参，避免了ArthasClassLoader的GC泄漏问题。重放功能通过反射调用保存的target和params，让开发者能够重现偶发性问题。整体设计在功能丰富性和内存安全性之间取得了良好的平衡。

---

## 场景七：Spring Bean动态查看与操作 —— ognl 命令深入

### 7.1 用户故事

微服务架构中，`order-service` 的缓存出现脏数据问题。开发怀疑是 `CacheService` 的某个内部状态异常，但应用没有暴露管理端点来查看缓存内容。也不能重启服务来添加调试日志——这会丢失当前缓存状态，无法定位问题根因。

需要通过Arthas的ognl命令直接访问Spring ApplicationContext，获取Bean实例，查看其内部属性，甚至调用Bean的方法来主动清理缓存。

### 7.2 操作命令与参数说明

```bash
# 获取Spring ApplicationContext
ognl '#context=@org.springframework.web.context.ContextLoader@getCurrentWebApplicationContext()'

# 查看指定Bean的属性
ognl '#context.getBean("orderService").orderMapper' -x 2

# 调用Bean方法清理缓存
ognl '#context.getBean("cacheService").clearAll()'

# 查看DataSource连接池状态
ognl '#context.getBean("dataSource").getPoolState()' -x 3
```

参数说明：
- `express`：OGNL表达式（第一个位置参数，必填）
- `-c hashCode`：指定ClassLoader的hashCode
- `--classLoaderClass className`：指定ClassLoader的类名
- `-x expand`：对象展开层级（默认1）

### 7.3 源码链路追踪：OgnlCommand.process()

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/OgnlCommand.java`

#### 7.3.1 ClassLoader选择策略

```java
@Override
public void process(CommandProcess process) {
    Instrumentation inst = process.session().getInstrumentation();
    ClassLoader classLoader = null;
    if (hashCode != null) {
        classLoader = ClassLoaderUtils.getClassLoader(inst, hashCode);
        if (classLoader == null) {
            process.end(-1, "Can not find classloader with hashCode: " + hashCode + ".");
            return;
        }
    } else if (classLoaderClass != null) {
        List<ClassLoader> matchedClassLoaders = ClassLoaderUtils.getClassLoaderByClassName(inst, classLoaderClass);
        if (matchedClassLoaders.size() == 1) {
            classLoader = matchedClassLoaders.get(0);
        } else if (matchedClassLoaders.size() > 1) {
            Collection<ClassLoaderVO> classLoaderVOList = ClassUtils.createClassLoaderVOList(matchedClassLoaders);
            OgnlModel ognlModel = new OgnlModel()
                    .setClassLoaderClass(classLoaderClass)
                    .setMatchedClassLoaders(classLoaderVOList);
            process.appendResult(ognlModel);
            process.end(-1, "Found more than one classloader by class name, please specify classloader with '-c <classloader hash>'");
            return;
        } else {
            process.end(-1, "Can not find classloader by class name: " + classLoaderClass + ".");
            return;
        }
    } else {
        classLoader = ClassLoader.getSystemClassLoader();
    }
```

ClassLoader的选择按优先级分为三级：

1. **`-c hashCode`（最高优先级）**：直接通过hashCode查找ClassLoader。`ClassLoaderUtils.getClassLoader(inst, hashCode)` 遍历 `inst.getAllLoadedClasses()` 中所有类的ClassLoader，比较hashCode。这是最精确的方式。

2. **`--classLoaderClass className`（次优先级）**：通过ClassLoader的类名查找。可能匹配到多个实例（比如多个Tomcat WebappClassLoader），此时要求用户用 `-c` 明确指定。

3. **默认SystemClassLoader（兜底）**：当用户不指定ClassLoader时使用。对于访问Spring Bean来说，这通常不够——Spring Bean由应用的ClassLoader加载，SystemClassLoader看不到这些类。

为什么Spring Bean需要指定正确的ClassLoader？因为OGNL表达式中需要解析类引用（如 `@org.springframework.web.context.ContextLoader@...`），类解析必须用加载该类的ClassLoader。如果用SystemClassLoader，它找不到Spring的类，OGNL会报 `ClassNotFoundException`。

#### 7.3.2 创建Express并执行

```java
    Express unpooledExpress = ExpressFactory.unpooledExpress(classLoader);
    try {
        // https://github.com/alibaba/arthas/issues/2892
        Object value = unpooledExpress.bind(new Object()).get(express);
        OgnlModel ognlModel = new OgnlModel()
                .setValue(new ObjectVO(value, expand));
        process.appendResult(ognlModel);
        process.end();
    } catch (ExpressException e) {
        logger.warn("ognl: failed execute express: " + express, e);
        process.end(-1, "Failed to execute ognl, exception message: " + e.getMessage()
                + ", please check $HOME/logs/arthas/arthas.log for more details. ");
    }
}
```

关键两行：

1. `ExpressFactory.unpooledExpress(classLoader)`：创建一个非池化的Express实例。
2. `unpooledExpress.bind(new Object()).get(express)`：绑定一个空Object作为根对象，然后执行OGNL表达式。

### 7.4 ExpressFactory —— 表达式工厂

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/express/ExpressFactory.java`

```java
public class ExpressFactory {
    /**
     * 这里不能直接在 ThreadLocalMap 里强引用 Express（它由 ArthasClassLoader 加载），否则 stop/detach 后会被业务线程持有，
     * 导致 ArthasClassLoader 无法被 GC 回收。
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

两个工厂方法的设计差异：

**threadLocalExpress**：
- 使用 `ThreadLocal<WeakReference<Express>>` 缓存Express实例
- 用于业务线程（如watch/trace的回调中），因为这些线程在Arthas detach后仍然存活
- 用WeakReference打断强引用链，避免ClassLoader泄漏
- 使用默认的 `CustomClassResolver`，通过 `Thread.currentThread().getContextClassLoader()` 解析类
- 调用 `reset()` 清除上一次的上下文

**unpooledExpress**：
- 每次调用都创建新的 `OgnlExpress` 实例，不缓存
- 用于命令处理线程（如ognl命令本身），因为这些线程在Arthas内部，detach时会一起销毁
- 使用 `ClassLoaderClassResolver`，通过显式传入的ClassLoader解析类
- 不需要reset，因为是全新的实例

为什么ognl命令用unpooled而不是threadLocal？因为ognl命令需要用**指定的ClassLoader**来解析类，而threadLocalExpress用的是ContextClassLoader。如果用户通过 `-c` 指定了一个非ContextClassLoader，threadLocalExpress就无法正确解析类。

### 7.5 OgnlExpress —— OGNL表达式执行器

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/express/OgnlExpress.java`

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
}
```

关键组件：

1. **MemberAccess**：`DefaultMemberAccess(true)` 允许访问private字段和方法。这是Arthas能查看Bean内部私有属性的关键。

2. **ArthasObjectPropertyAccessor**：自定义的属性访问器，注册到 `Object.class` 级别。它扩展了OGNL默认的属性访问逻辑，支持通过getter方法访问属性，也支持直接通过反射访问字段。

3. **OgnlContext**：OGNL的执行上下文，包含ClassResolver、MemberAccess等配置。`context.clear()` 在reset时清除所有变量绑定。

4. **bind(Object)**：设置OGNL表达式的根对象（root object）。在OGNL中，表达式 `#name` 访问context变量，而直接写属性名（如 `returnObj`）则访问根对象的属性。

5. **bind(String, Object)**：将命名变量放入context。如 `bind("cost", 10L)` 后，表达式中可以用 `#cost` 引用这个值。

### 7.6 OGNL表达式执行原理

当执行 `Ognl.getValue(express, context, bindObject)` 时，OGNL引擎的内部流程：

1. **解析表达式**：将字符串表达式编译为AST（抽象语法树）。OGNL支持多种语法：
   - `@className@field`：静态字段访问，如 `@java.lang.System@out`
   - `@className@method(args)`：静态方法调用，如 `@System@getProperty("java.home")`
   - `#varName`：context变量引用，如 `#context`
   - `#varName = expr`：变量赋值，如 `#value1=@System@getProperty("java.home")`
   - `property`：根对象属性访问，如 `returnObj`
   - `method(args)`：根对象方法调用，如 `getMessage()`
   - `[index]`：数组/List索引访问，如 `params[0]`
   - `{a, b, c}`：创建List
   - `#{"key": "value"}`：创建Map

2. **类解析**：当表达式引用类名时（如 `@org.springframework.web.context.ContextLoader@...`），通过ClassResolver查找类。

**ClassLoaderClassResolver源码**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/express/ClassLoaderClassResolver.java`

```java
public class ClassLoaderClassResolver implements ClassResolver {
    private ClassLoader classLoader;
    private Map<String, Class<?>> classes = new ConcurrentHashMap<String, Class<?>>(101);

    public ClassLoaderClassResolver(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public Class classForName(String className, Map context) throws ClassNotFoundException {
        Class<?> result = null;
        if ((result = classes.get(className)) == null) {
            try {
                result = classLoader.loadClass(className);
            } catch (ClassNotFoundException ex) {
                if (className.indexOf('.') == -1) {
                    result = Class.forName("java.lang." + className);
                    classes.put("java.lang." + className, result);
                }
            }
            if (result == null) {
                return null;
            }
            classes.put(className, result);
        }
        return result;
    }
}
```

类解析逻辑：
1. 先查缓存（`ConcurrentHashMap`），避免重复加载
2. 用指定的ClassLoader加载类：`classLoader.loadClass(className)`
3. 如果类名没有包分隔符（如 `String`），尝试在 `java.lang` 包下查找
4. 缓存结果

对比 `CustomClassResolver`（用于threadLocalExpress）：

```java
public class CustomClassResolver implements ClassResolver {
    @Override
    public Class classForName(String className, Map context) throws ClassNotFoundException {
        Class<?> result = null;
        if ((result = classes.get(className)) == null) {
            try {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                if (classLoader != null) {
                    result = classLoader.loadClass(className);
                } else {
                    result = Class.forName(className);
                }
            } catch (ClassNotFoundException ex) {
                if (className.indexOf('.') == -1) {
                    result = Class.forName("java.lang." + className);
                    classes.put("java.lang." + className, result);
                }
            }
            classes.put(className, result);
        }
        return result;
    }
}
```

区别在于类加载方式：`CustomClassResolver` 用 `Thread.currentThread().getContextClassLoader()`，而 `ClassLoaderClassResolver` 用显式传入的ClassLoader。

3. **求值**：遍历AST，执行每个节点的操作。对于方法调用，通过反射执行。对于属性访问，通过getter方法或直接字段访问。

### 7.7 bind(new Object()) 的含义

```java
// https://github.com/alibaba/arthas/issues/2892
Object value = unpooledExpress.bind(new Object()).get(express);
```

注释引用了GitHub issue #2892。为什么bind一个空Object而不是null？

问题在于OGNL引擎的行为：当根对象为null时，某些OGNL表达式（特别是直接访问属性的表达式）会抛出NPE。比如表达式 `@System@getProperty("java.home")` 虽然不依赖根对象，但OGNL内部在某些代码路径上会检查根对象是否为null。

更具体地说，OGNL在执行表达式时，会先尝试将表达式解析为根对象的属性访问。如果根对象为null，这个解析过程可能抛出异常。通过绑定一个非null的空Object，可以避免这种问题，让OGNL正常进入表达式解析流程。

### 7.8 实际场景的OGNL执行过程

以 `ognl '#context=@org.springframework.web.context.ContextLoader@getCurrentWebApplicationContext()'` 为例：

1. **创建Express**：`ExpressFactory.unpooledExpress(classLoader)` 创建OgnlExpress，传入应用的ClassLoader。
2. **绑定根对象**：`bind(new Object())` 绑定空Object。
3. **解析表达式**：OGNL引擎解析 `#context=@org.springframework.web.context.ContextLoader@getCurrentWebApplicationContext()`
   - 识别 `#context = ...` 为变量赋值
   - 识别 `@org.springframework.web.context.ContextLoader@getCurrentWebApplicationContext()` 为静态方法调用
   - 通过ClassLoaderClassResolver用应用的ClassLoader加载 `ContextLoader` 类
   - 反射调用 `getCurrentWebApplicationContext()` 静态方法
   - 将返回值存入context变量 `#context`
4. **返回结果**：表达式的值是 `getCurrentWebApplicationContext()` 的返回值（ApplicationContext对象）。

然后执行 `ognl '#context.getBean("orderService").orderMapper' -x 2`：
1. 这里的 `#context` 引用上一次执行中赋值的变量——但注意，每次ognl命令都是独立的Express实例，`#context` 不会跨命令保持。
2. 实际上这条命令需要组合成一条：`ognl '#context=@org.springframework.web.context.ContextLoader@getCurrentWebApplicationContext(), #context.getBean("orderService").orderMapper' -x 2`
3. OGNL支持逗号表达式——先执行 `#context=...` 赋值，然后执行 `#context.getBean("orderService").orderMapper` 获取属性。

### 7.9 OgnlModel输出

```java
OgnlModel ognlModel = new OgnlModel()
        .setValue(new ObjectVO(value, expand));
process.appendResult(ognlModel);
```

`ObjectVO` 包装表达式结果，`expand` 控制对象树的展开层级。比如 `-x 2` 表示展开两层：
- 第一层：显示对象本身的属性
- 第二层：显示每个属性值的属性

`OgnlModel` 被发送到终端的View层渲染。View层根据对象类型选择不同的渲染策略：
- 基本类型：直接显示值
- 数组/集合：逐元素展示
- 复杂对象：按属性名和值展开
- null：显示为 `null`

### 7.10 完整链路总结

```
ognl命令链路：
ShellLineHandler → ProcessImpl → OgnlCommand.process()
  → ClassLoader选择：
    1. hashCode优先 → ClassLoaderUtils.getClassLoader(inst, hashCode)
    2. classLoaderClass次之 → ClassLoaderUtils.getClassLoaderByClassName(inst, className)
    3. 默认 → ClassLoader.getSystemClassLoader()
  → ExpressFactory.unpooledExpress(classLoader)
    → new OgnlExpress(new ClassLoaderClassResolver(classLoader))
      → OgnlRuntime.setPropertyAccessor(Object.class, ArthasObjectPropertyAccessor)
      → new OgnlContext(MEMBER_ACCESS, classResolver, null, null)
  → unpooledExpress.bind(new Object())    // 绑定根对象（issue #2892）
  → express.get(express)                  // 执行OGNL表达式
    → Ognl.getValue(express, context, bindObject)
      → 解析表达式为AST
      → 遍历AST执行：
        → @语法 → ClassLoaderClassResolver.classForName() → classLoader.loadClass()
        → #语法 → context.get(name) / context.put(name, value)
        → .语法 → 反射调用getter/方法
  → new ObjectVO(value, expand)           // 包装结果
  → new OgnlModel().setValue(ObjectVO)
  → process.appendResult(ognlModel)
  → OgnlView.draw() → 终端渲染
  → process.end()
```

### 7.11 Q&A

**Q1: ognl命令执行的表达式有安全风险吗？**

A: 有。OGNL是一种强大的表达式语言，可以执行任意Java代码。通过 `@Runtime@getRuntime().exec("command")` 可以执行系统命令。因此Arthas的安全机制非常重要——`SecurityAuthenticatorImpl` 在监听 `0.0.0.0` 时会强制生成随机密码，防止未授权访问。在生产环境中，应限制Arthas的访问来源，避免暴露在公网。

**Q2: 为什么有时候用 `#` 有时候不用？**

A: 在OGNL语法中：
- `#name` 访问context变量（由 `bind(String, Object)` 或 `#name = expr` 设置的命名变量）
- 不带 `#` 的标识符访问根对象（由 `bind(Object)` 设置）的属性
- `@ClassName@field` 访问静态字段/方法
- 所以 `#context.getBean(...)` 中 `#context` 是context变量，`.getBean(...)` 是对变量值的方法调用

**Q3: ognl命令能修改对象的属性值吗？**

A: 可以。OGNL支持赋值表达式，如 `#obj.field = newValue`。配合 `DefaultMemberAccess(true)`（允许访问private字段），可以修改任何对象的属性。但修改对象状态可能导致应用行为异常，需要谨慎操作。

**Q4: -x 参数对性能有影响吗？**

A: 有。`-x` 控制对象展开层级，层级越深，需要遍历的对象越多，序列化和传输的数据量也越大。对于包含大量字段的对象（如Spring ApplicationContext），展开2-3层可能就产生大量输出。建议先用 `-x 1` 查看概况，再逐步增加层级。

**Q5: ognl和tt -w中的OGNL有什么区别？**

A: 核心OGNL引擎相同，但绑定对象不同。ognl命令绑定的是空Object（`new Object()`），表达式完全依赖自身语法（如 `@ClassName@method`、`#var = expr`）。tt -w绑定的是Advice对象，可以直接访问 `params`、`returnObj`、`throwExp` 等属性。此外，ognl命令用 `unpooledExpress` + `ClassLoaderClassResolver`（显式ClassLoader），而tt -w也用 `unpooledExpress` 但ClassLoader来自Advice。

### 7.12 DefaultMemberAccess —— 突破访问控制

在OgnlExpress的构造函数中有一行关键代码：

```java
private static final MemberAccess MEMBER_ACCESS = new DefaultMemberAccess(true);
```

`DefaultMemberAccess(true)` 的参数 `true` 表示允许访问private、protected和package-private的成员。这是OGNL安全模型中的核心控制点。

OGNL的 `MemberAccess` 接口定义在 `ognl` 包中，负责控制反射访问权限。默认情况下，Java的反射机制在访问private成员时需要调用 `setAccessible(true)`。`DefaultMemberAccess` 封装了这个逻辑：

- 当 `allowPrivateAccess = true` 时，对所有字段和方法调用 `setAccessible(true)`，绕过Java访问控制检查。
- 这意味着OGNL表达式可以直接读取和修改对象的private字段，无需通过getter/setter。

在实际使用中，这个设计有利有弊：
- **优势**：能够查看Spring Bean的内部状态（如private的dataSource字段、private的缓存Map等），不受getter方法缺失的限制。
- **风险**：如果表达式有误，可能意外修改private字段导致应用状态不一致。例如 `#obj.status = null` 可能破坏业务逻辑中的状态机。

### 7.13 ArthasObjectPropertyAccessor —— 属性访问扩展

```java
private static final ArthasObjectPropertyAccessor OBJECT_PROPERTY_ACCESSOR = new ArthasObjectPropertyAccessor();
```

`ArthasObjectPropertyAccessor` 注册到 `Object.class` 级别，覆盖了OGNL默认的属性访问行为。它的作用是扩展属性查找策略——当OGNL表达式访问一个属性时（如 `.orderMapper`），ArthasObjectPropertyAccessor会按以下顺序查找：

1. **getter方法**：先尝试调用 `getOrderMapper()` 方法
2. **is前缀方法**：尝试 `isOrderMapper()`（适用于boolean属性）
3. **直接字段访问**：如果getter不存在，直接通过反射访问 `orderMapper` 字段
4. **Map-style访问**：如果对象是Map，用属性名作为key查找

这种扩展使得OGNL能访问没有getter的private字段，这在查看框架内部状态时非常有用。例如MyBatis的 `MapperProxy` 内部的 `methodCache` 是个private字段，通过ArthasObjectPropertyAccessor可以直接访问。

### 7.14 实际场景深入：查看DataSource连接池状态

以 `ognl '#context.getBean("dataSource").getPoolState()' -x 3` 为例，完整执行过程：

**步骤1：获取ApplicationContext**

表达式 `#context=@org.springframework.web.context.ContextLoader@getCurrentWebApplicationContext()` 的执行：
- ClassLoaderClassResolver用应用的ClassLoader加载 `org.springframework.web.context.ContextLoader` 类
- 反射调用静态方法 `getCurrentWebApplicationContext()`，返回 `WebApplicationContext` 实例
- 将返回值存入OGNL context变量 `#context`

**步骤2：获取DataSource Bean**

表达式 `#context.getBean("dataSource")` 的执行：
- 从context取出 `#context` 变量（ApplicationContext实例）
- 调用 `getBean("dataSource")` 方法，返回DataSource实例（如HikariDataSource）
- 结果作为中间值参与后续调用

**步骤3：获取连接池状态**

表达式 `.getPoolState()` 的执行：
- 对上一步返回的DataSource调用 `getPoolState()` 方法
- HikariCP的 `getPoolState()` 返回包含活跃连接数、空闲连接数、等待线程数等信息的对象

**步骤4：对象展开（-x 3）**

`ObjectVO(value, 3)` 包装结果，展开3层对象树：
- 第1层：PoolState对象的属性（activeConnections、idleConnections等）
- 第2层：每个属性的值（如activeConnections的数量、idleConnections的列表）
- 第3层：连接对象的详情（如连接URL、用户名、创建时间等）

### 7.15 Express接口设计 —— 策略模式

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/command/express/Express.java`

```java
public interface Express {
    Object get(String express) throws ExpressException;
    boolean is(String express) throws ExpressException;
    Express bind(Object object);
    Express bind(String name, Object value);
    Express reset();
}
```

Express接口定义了表达式执行器的契约：
- `get(express)`：执行表达式并返回结果
- `is(express)`：执行表达式并返回boolean（用于条件判断）
- `bind(object)`：绑定根对象
- `bind(name, value)`：绑定命名变量
- `reset()`：重置上下文

这种设计采用了策略模式——`Express` 是策略接口，`OgnlExpress` 是当前唯一的实现。如果未来需要支持其他表达式语言（如SpEL、MVEL等），只需新增实现类即可。`ExpressFactory` 作为工厂，根据使用场景创建不同配置的Express实例。

### 7.16 场景总结

ognl命令是Arthas中最灵活的工具——通过OGNL表达式语言，它可以访问任意静态字段/方法、调用对象方法、修改属性值，几乎等同于在JVM中执行任意代码。源码设计的核心在于ClassLoader选择策略和ExpressFactory的两种模式。`unpooledExpress` 配合 `ClassLoaderClassResolver` 确保了类解析使用正确的ClassLoader，这对于Spring Boot等使用自定义ClassLoader的应用至关重要。`bind(new Object())` 的细节处理体现了对OGNL引擎边界条件的深入理解。`DefaultMemberAccess(true)` 和 `ArthasObjectPropertyAccessor` 的组合突破了Java的访问控制限制，使得直接读取private字段成为可能。整体设计在灵活性和安全性之间提供了平衡——功能强大但需要通过认证机制保护。

---

## 场景八：远程诊断通过Tunnel Server —— 多机房远程排查

### 8.1 用户故事

公司有北京、上海、深圳三个机房。某天上海机房的 `order-service-prod` 服务出现性能问题，接口响应时间从50ms飙升到2000ms。但上海机房有严格的网络隔离策略，运维人员无法直接SSH到该机房的机器。北京机房的运维人员需要远程连接到上海机房的JVM进行诊断。

解决方案：在目标机器上启动Arthas并注册到Tunnel Server，运维人员通过浏览器访问Tunnel Server的WebUI，选择目标agent进行远程诊断。全程不需要直接SSH到目标机器。

### 8.2 架构说明

```
┌─────────────────┐         WebSocket          ┌─────────────────┐
│   Browser       │◄──────────────────────────►│  Tunnel Server  │
│   (北京运维)     │    WebUI + 诊断命令         │  (中心机房)      │
└─────────────────┘                            └────────┬────────┘
                                                        │
                                          WebSocket     │
                                          (agent注册+    │
                                           隧道建立)     │
                                                        │
┌─────────────────┐                            ┌────────▼────────┐
│  目标JVM         │◄──────ForwardClient────────│  Tunnel Server  │
│  (上海机房)       │     (本地Telnet隧道)         │  (转发)          │
│  Arthas Agent    │◄───► Local ShellServer      │                 │
└─────────────────┘                            └─────────────────┘
```

数据流：
1. 目标JVM启动Arthas Agent → TunnelClient连接到Tunnel Server注册
2. 浏览器访问Tunnel Server WebUI → 选择目标agent
3. Tunnel Server向TunnelClient发送 `startTunnel` 命令
4. TunnelClient创建ForwardClient → ForwardClient连接到Tunnel Server的新WebSocket
5. ForwardClient同时连接本地Arthas ShellServer（通过Netty LocalAddress）
6. 浏览器 ↔ Tunnel Server ↔ ForwardClient ↔ 本地ShellServer
7. 形成完整的数据转发通道

### 8.3 源码链路追踪：TunnelClient启动流程

#### 8.3.1 ArthasBootstrap中TunnelClient的初始化

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/server/ArthasBootstrap.java`

```java
private void bind(Configure configure) throws Throwable {
    // ...
    try {
        if (configure.getTunnelServer() != null) {
            tunnelClient = new TunnelClient();
            tunnelClient.setAppName(configure.getAppName());
            tunnelClient.setId(configure.getAgentId());
            tunnelClient.setTunnelServerUrl(configure.getTunnelServer());
            tunnelClient.setVersion(ArthasBanner.version());
            ChannelFuture channelFuture = tunnelClient.start();
            channelFuture.await(10, TimeUnit.SECONDS);
        }
    } catch (Throwable t) {
        logger().error("start tunnel client error", t);
    }
    // ...
```

在 `bind()` 方法中，如果配置了 `tunnelServer` URL，就创建并启动TunnelClient。注意异常被catch但只记录日志不抛出——即使Tunnel Server连接失败，Arthas本地功能仍然可用。`channelFuture.await(10, TimeUnit.SECONDS)` 等待连接完成，最多等10秒。

配置来源：用户通过启动参数 `--tunnel-server ws://tunnel.example.com:7777` 指定Tunnel Server地址，通过 `--app-name order-service-prod` 指定应用名。这些参数被解析到 `Configure` 对象中。

#### 8.3.2 TunnelClient.connect() —— WebSocket连接建立

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/TunnelClient.java`

```java
public ChannelFuture connect(boolean reconnect) throws SSLException, URISyntaxException, InterruptedException {
    QueryStringEncoder queryEncoder = new QueryStringEncoder(this.tunnelServerUrl);
    queryEncoder.addParam(URIConstans.METHOD, MethodConstants.AGENT_REGISTER);
    queryEncoder.addParam(URIConstans.ARTHAS_VERSION, this.version);
    if (appName != null) {
        queryEncoder.addParam(URIConstans.APP_NAME, appName);
    }
    if (id != null) {
        queryEncoder.addParam(URIConstans.ID, id);
    }
    // ws://127.0.0.1:7777/ws?method=agentRegister
    final URI agentRegisterURI = queryEncoder.toUri();
```

构造注册URI。例如：`ws://tunnel.example.com:7777/ws?method=agentRegister&arthasVersion=4.0.0&appName=order-service-prod`

参数说明：
- `method=agentRegister`：标识这是一个agent注册请求
- `arthasVersion`：Arthas版本号，Tunnel Server可用于兼容性检查
- `appName`：应用名，用于在WebUI中展示和搜索
- `id`：agent id（重连时携带，用于复用之前的id）

```java
    String scheme = agentRegisterURI.getScheme() == null ? "ws" : agentRegisterURI.getScheme();
    final String host = agentRegisterURI.getHost() == null ? "127.0.0.1" : agentRegisterURI.getHost();
    final int port;
    if (agentRegisterURI.getPort() == -1) {
        if ("ws".equalsIgnoreCase(scheme)) {
            port = 80;
        } else if ("wss".equalsIgnoreCase(scheme)) {
            port = 443;
        } else {
            port = -1;
        }
    } else {
        port = agentRegisterURI.getPort();
    }

    if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
        throw new IllegalArgumentException("Only WS(S) is supported. tunnelServerUrl: " + tunnelServerUrl);
    }

    final boolean ssl = "wss".equalsIgnoreCase(scheme);
    final SslContext sslCtx;
    if (ssl) {
        sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
    } else {
        sslCtx = null;
    }
```

支持 `ws://` 和 `wss://`（WebSocket over TLS）两种协议。对于 `wss://`，使用 `InsecureTrustManagerFactory` 信任所有证书。这在企业内部环境中简化了部署，但需要注意安全风险——如果Tunnel Server使用自签名证书，客户端不会校验证书合法性。

```java
    WebSocketClientProtocolConfig clientProtocolConfig = WebSocketClientProtocolConfig.newBuilder()
            .webSocketUri(agentRegisterURI)
            .maxFramePayloadLength(ArthasConstants.MAX_HTTP_CONTENT_LENGTH).build();

    final WebSocketClientProtocolHandler websocketClientHandler = new WebSocketClientProtocolHandler(
            clientProtocolConfig);
    final TunnelClientSocketClientHandler handler = new TunnelClientSocketClientHandler(TunnelClient.this);

    Bootstrap bs = new Bootstrap();

    bs.group(eventLoopGroup)
    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
    .option(ChannelOption.TCP_NODELAY, true)
    .channel(NioSocketChannel.class).remoteAddress(host, port)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline p = ch.pipeline();
                    if (sslCtx != null) {
                        p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                    }

                    p.addLast(new HttpClientCodec(), new HttpObjectAggregator(ArthasConstants.MAX_HTTP_CONTENT_LENGTH), websocketClientHandler,
                            new IdleStateHandler(0, 0, ArthasConstants.WEBSOCKET_IDLE_SECONDS),
                            handler);
                }
            });

    ChannelFuture connectFuture = bs.connect();
    if (reconnect) {
        connectFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.cause() != null) {
                    logger.error("connect to tunnel server error, uri: {}", tunnelServerUrl, future.cause());
                }
            }
        });
    }
    connectFuture.sync();

    return handler.registerFuture();
}
```

Netty Pipeline的组成（从上到下）：

1. **SslHandler**（仅wss）：处理TLS握手和数据加密/解密
2. **HttpClientCodec**：HTTP编解码器，处理WebSocket的HTTP升级握手
3. **HttpObjectAggregator**：HTTP消息聚合器，将分片的HTTP消息合并为完整消息
4. **WebSocketClientProtocolHandler**：WebSocket协议处理器，处理握手、Ping/Pong、帧解析
5. **IdleStateHandler**：空闲检测，`WEBSOCKET_IDLE_SECONDS` 秒无读写触发 `IdleStateEvent`
6. **TunnelClientSocketClientHandler**：业务处理器，处理WebSocket文本帧

`eventLoopGroup` 使用2个线程的 `NioEventLoopGroup`（`new NioEventLoopGroup(2, ...)`），注释说"two thread because need to reconnect"——一个线程处理正常IO，另一个用于重连时的连接操作。

`handler.registerFuture()` 返回一个 `ChannelPromise`，在agent注册成功后才会被设置为success。调用方通过 `channelFuture.await(10, TimeUnit.SECONDS)` 等待注册完成。

### 8.4 TunnelClientSocketClientHandler —— 消息处理

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/TunnelClientSocketClientHandler.java`

#### 8.4.1 agentRegister响应处理

```java
@Override
public void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
    if (frame instanceof TextWebSocketFrame) {
        TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
        String text = textFrame.text();

        logger.info("receive TextWebSocketFrame: {}", text);

        QueryStringDecoder queryDecoder = new QueryStringDecoder(text);
        Map<String, List<String>> parameters = queryDecoder.parameters();
        List<String> methodList = parameters.get(URIConstans.METHOD);
        String method = null;
        if (methodList != null && !methodList.isEmpty()) {
            method = methodList.get(0);
        }

        if (MethodConstants.AGENT_REGISTER.equals(method)) {
            List<String> idList = parameters.get(URIConstans.ID);
            if (idList != null && !idList.isEmpty()) {
                this.tunnelClient.setId(idList.get(0));
            }
            tunnelClient.setConnected(true);
            registerPromise.setSuccess();
        }
```

TunnelClient与TunnelServer之间通过TextWebSocketFrame通信，消息格式为URL查询字符串（如 `?method=agentRegister&id=abc123`）。

收到 `agentRegister` 响应时：
1. 从参数中取出Tunnel Server分配的agent id
2. 设置到TunnelClient的 `id` 字段（用于后续重连时复用）
3. 标记为已连接（`setConnected(true)`）
4. 完成 `registerPromise`——这会唤醒等待注册完成的 `channelFuture.await()` 调用

#### 8.4.2 startTunnel命令处理

```java
        if (MethodConstants.START_TUNNEL.equals(method)) {
            QueryStringEncoder queryEncoder = new QueryStringEncoder(this.tunnelClient.getTunnelServerUrl());
            queryEncoder.addParam(URIConstans.METHOD, MethodConstants.OPEN_TUNNEL);
            queryEncoder.addParam(URIConstans.CLIENT_CONNECTION_ID, parameters.get(URIConstans.CLIENT_CONNECTION_ID).get(0));
            queryEncoder.addParam(URIConstans.ID, parameters.get(URIConstans.ID).get(0));

            final URI forwardUri = queryEncoder.toUri();

            logger.info("start ForwardClient, uri: {}", forwardUri);
            try {
                ForwardClient forwardClient = new ForwardClient(forwardUri);
                forwardClient.start();
            } catch (Throwable e) {
                logger.error("start ForwardClient error, forwardUri: {}", forwardUri, e);
            }
        }
```

当浏览器用户在WebUI中选择某个agent并点击连接时，Tunnel Server通过已注册的WebSocket通道发送 `startTunnel` 命令。TunnelClient收到后：

1. 构造一个新的URI用于建立隧道连接：`?method=openTunnel&clientConnectionId=xxx&id=xxx`
   - `clientConnectionId`：Tunnel Server为这次浏览器连接分配的唯一ID
   - `id`：agent id
2. 创建 `ForwardClient` 并启动——它会建立到Tunnel Server的第二个WebSocket连接，专门用于这次诊断会话的数据转发。

为什么需要第二个连接？因为注册连接是持久的、全agent共享的，用于接收控制命令。而每次诊断会话需要独立的数据通道，避免不同浏览器会话之间数据交叉。

#### 8.4.3 心跳机制

```java
@Override
public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
        ctx.writeAndFlush(new PingWebSocketFrame());
    } else {
        super.userEventTriggered(ctx, evt);
    }
}
```

`IdleStateHandler` 在 `WEBSOCKET_IDLE_SECONDS` 秒无IO活动时触发 `IdleStateEvent`。TunnelClient收到后发送 `PingWebSocketFrame` 保持连接活跃。如果Tunnel Server不回复Pong，最终会导致连接超时断开。

#### 8.4.4 断线重连

```java
@Override
public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
    tunnelClient.setConnected(false);
    ctx.channel().eventLoop().schedule(new Runnable() {
        @Override
        public void run() {
            logger.error("try to reconnect to tunnel server, uri: {}", tunnelClient.getTunnelServerUrl());
            try {
                tunnelClient.connect(true);
            } catch (Throwable e) {
                logger.error("reconnect error", e);
            }
        }
    }, tunnelClient.getReconnectDelay(), TimeUnit.SECONDS);
}
```

连接断开时（`channelUnregistered`），延迟 `reconnectDelay` 秒（默认5秒）后自动重连。重连时 `connect(true)` 传入 `reconnect=true`，表示是重连操作——会添加异常监听器但不阻塞等待。重连时会携带之前的 `id`，Tunnel Server可以识别这是同一个agent的重连。

### 8.5 ForwardClient —— 隧道数据转发

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/ForwardClient.java`

```java
public void start() throws URISyntaxException, SSLException, InterruptedException {
    // ... URI解析和SSL配置（与TunnelClient类似）

    final ForwardClientSocketClientHandler forwardClientSocketClientHandler = new ForwardClientSocketClientHandler();

    final EventLoopGroup group = new NioEventLoopGroup(1, new DefaultThreadFactory("arthas-ForwardClient", true));
    ChannelFuture closeFuture = null;
    try {
        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
        b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                }
                p.addLast(new HttpClientCodec(), new HttpObjectAggregator(ArthasConstants.MAX_HTTP_CONTENT_LENGTH), websocketClientHandler,
                        forwardClientSocketClientHandler);
            }
        });

        closeFuture = b.connect(tunnelServerURI.getHost(), port).sync().channel().closeFuture();
        logger.info("forward client connect to server success, uri: " + tunnelServerURI);
    } finally {
        if (closeFuture != null) {
            closeFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    group.shutdownGracefully();
                }
            });
        } else {
            group.shutdownGracefully();
        }
    }
}
```

ForwardClient连接到Tunnel Server的WebSocket端点（`?method=openTunnel&clientConnectionId=xxx&id=xxx`）。连接成功后，`ForwardClientSocketClientHandler` 负责握手完成后连接本地Arthas ShellServer。

注意 `closeFuture` 的处理：ForwardClient的EventLoopGroup在Channel关闭后才优雅关闭。这确保了数据转发的完整性——直到通道关闭前都在正常工作。

### 8.6 ForwardClientSocketClientHandler —— 双向桥接

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/ForwardClientSocketClientHandler.java`

```java
@Override
public void userEventTriggered(final ChannelHandlerContext ctx, Object evt) {
    if (evt.equals(ClientHandshakeStateEvent.HANDSHAKE_COMPLETE)) {
        try {
            connectLocalServer(ctx);
        } catch (Throwable e) {
            logger.error("ForwardClientSocketClientHandler connect local arthas server error", e);
        }
    } else {
        ctx.fireUserEventTriggered(evt);
    }
}
```

WebSocket握手完成后，立即调用 `connectLocalServer()` 连接本地Arthas ShellServer。

```java
private void connectLocalServer(final ChannelHandlerContext ctx) throws InterruptedException, URISyntaxException {
    final EventLoopGroup group = new NioEventLoopGroup(1, new DefaultThreadFactory("arthas-forward-client-connect-local", true));
    ChannelFuture closeFuture = null;
    try {
        // 入参URI实际无意义，只为了程序不出错
        WebSocketClientProtocolConfig clientProtocolConfig = WebSocketClientProtocolConfig.newBuilder()
                .webSocketUri("ws://127.0.0.1:8563/ws")
                .maxFramePayloadLength(ArthasConstants.MAX_HTTP_CONTENT_LENGTH).build();

        final WebSocketClientProtocolHandler websocketClientHandler = new WebSocketClientProtocolHandler(
                clientProtocolConfig);

        final LocalFrameHandler localFrameHandler = new LocalFrameHandler();

        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
        b.group(group).channel(LocalChannel.class)
                .handler(new ChannelInitializer<LocalChannel>() {
                    @Override
                    protected void initChannel(LocalChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpClientCodec(), new HttpObjectAggregator(ArthasConstants.MAX_HTTP_CONTENT_LENGTH), websocketClientHandler,
                                localFrameHandler);
                    }
                });

        LocalAddress localAddress = new LocalAddress(ArthasConstants.NETTY_LOCAL_ADDRESS);
        Channel localChannel = b.connect(localAddress).sync().channel();
        this.handshakeFuture = localFrameHandler.handshakeFuture();
        handshakeFuture.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                ChannelPipeline pipeline = future.channel().pipeline();
                pipeline.remove(localFrameHandler);
                pipeline.addLast(new RelayHandler(ctx.channel()));
            }
        });

        handshakeFuture.sync();
        ctx.pipeline().remove(ForwardClientSocketClientHandler.this);
        ctx.pipeline().addLast(new RelayHandler(localChannel));
        logger.info("ForwardClientSocketClientHandler connect local arthas server success");
```

这段代码是整个Tunnel机制中最关键的部分——建立双向数据转发桥梁。

关键设计：

1. **Netty LocalAddress**：使用 `LocalAddress` 而非TCP连接本地Arthas ShellServer。这是Netty的进程内通信机制——数据在JVM内部通过内存拷贝传递，不经过网络协议栈。相比TCP loopback连接，LocalChannel延迟更低、性能更高。

2. **双向RelayHandler**：握手完成后，在两个Channel的Pipeline中各添加一个RelayHandler：
   - ForwardClient → Tunnel Server 的Channel中添加 `RelayHandler(localChannel)`：收到Tunnel Server的数据转发到本地Channel
   - 本地Channel中添加 `RelayHandler(ctx.channel())`：收到本地ShellServer的数据转发到Tunnel Server

3. **移除自身**：`ctx.pipeline().remove(ForwardClientSocketClientHandler.this)` 移除握手处理器，因为它的工作已经完成——后续数据由RelayHandler直接转发。

### 8.7 RelayHandler —— 数据中继

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/tunnel-client/src/main/java/com/alibaba/arthas/tunnel/client/RelayHandler.java`

```java
public final class RelayHandler extends ChannelInboundHandlerAdapter {
    private final static Logger logger = LoggerFactory.getLogger(RelayHandler.class);
    private final Channel relayChannel;

    public RelayHandler(Channel relayChannel) {
        this.relayChannel = relayChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (relayChannel.isActive()) {
            relayChannel.writeAndFlush(msg);
        } else {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (relayChannel.isActive()) {
            ChannelUtils.closeOnFlush(relayChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("RelayHandler error", cause);
        try {
            if (relayChannel.isActive()) {
                relayChannel.close();
            }
        } finally {
            ctx.close();
        }
    }
}
```

RelayHandler是一个极简但健壮的数据中继器：

- **channelRead**：收到数据时，如果对端Channel仍然活跃，直接转发；否则释放ByteBuf避免内存泄漏。
- **channelInactive**：本端Channel断开时，关闭对端Channel。
- **exceptionCaught**：异常时关闭两端Channel。

数据流向：
```
Browser → Tunnel Server → [WebSocket] → ForwardClient Channel
  → RelayHandler.writeAndFlush → LocalChannel
  → Arthas ShellServer (本地Telnet/HTTP服务)

Arthas ShellServer → LocalChannel
  → RelayHandler.writeAndFlush → ForwardClient Channel
  → [WebSocket] → Tunnel Server → Browser
```

### 8.8 安全认证机制

**源码位置**：`/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas/core/src/main/java/com/taobao/arthas/core/security/SecurityAuthenticatorImpl.java`

在 `ArthasBootstrap.bind()` 中：

```java
if (IPUtils.isAllZeroIP(configure.getIp()) && StringUtils.isBlank(configure.getPassword())) {
    // 当 listen 0.0.0.0 时，强制生成密码，防止被远程连接
    String errorMsg = "Listening on 0.0.0.0 is very dangerous! External users can connect to your machine! "
            + "No password is currently configured. " + "Therefore, a default password is generated, "
            + "and clients need to use the password to connect!";
    AnsiLog.error(errorMsg);
    configure.setPassword(StringUtils.randomString(64));
    AnsiLog.error("Generated arthas password: " + configure.getPassword());
}

this.securityAuthenticator = new SecurityAuthenticatorImpl(configure.getUsername(), configure.getPassword());
```

安全策略：

1. **0.0.0.0监听强制密码**：当Arthas监听 `0.0.0.0`（所有网卡）且没有配置密码时，自动生成64位随机密码。这是防御性设计——防止用户不小心将Arthas暴露到外网。

2. **SecurityAuthenticatorImpl的认证方式**：

```java
public SecurityAuthenticatorImpl(String username, String password) {
    if (username != null && password == null) {
        password = StringUtils.randomString(32);
        logger.info("\nUsing generated security password: {}\n", password);
    }
    if (username == null && password != null) {
        username = ArthasConstants.DEFAULT_USERNAME;
    }
    this.username = username;
    this.password = password;
    subject = new Subject();
}
```

如果只配置了用户名没有密码，自动生成32位随机密码。如果只配置了密码没有用户名，使用默认用户名。

```java
@Override
public Subject login(Principal principal) throws LoginException {
    if (principal == null) {
        return null;
    }
    if (principal instanceof BasicPrincipal) {
        BasicPrincipal basicPrincipal = (BasicPrincipal) principal;
        if (basicPrincipal.getName().equals(username) && basicPrincipal.getPassword().equals(this.password)) {
            return subject;
        }
    }
    if (principal instanceof BearerPrincipal) {
        BearerPrincipal bearerPrincipal = (BearerPrincipal) principal;
        // Bearer Token认证：将token作为password进行验证
        if (bearerPrincipal.getToken().equals(this.password)) {
            return subject;
        }
    }
    if (principal instanceof LocalConnectionPrincipal) {
        return subject;
    }
    return null;
}
```

三种认证方式：
1. **Basic认证**（`BasicPrincipal`）：用户名+密码验证。浏览器通过HTTP Basic Auth发送凭据。
2. **Bearer Token认证**（`BearerPrincipal`）：Token作为密码验证。适用于API调用场景。
3. **本地连接免认证**（`LocalConnectionPrincipal`）：来自本机的连接不需要认证。这就是TunnelClient通过LocalAddress连接本地ShellServer时不需要密码的原因。

`needLogin()` 方法判断是否需要认证：

```java
@Override
public boolean needLogin() {
    return username != null && password != null;
}
```

只有当用户名和密码都不为null时才需要认证。如果两者都为null（比如只监听127.0.0.1），则不需要认证。

### 8.9 完整链路总结

```
=== 启动阶段 ===
Bootstrap → Arthas → AgentBootstrap → ArthasBootstrap.bind()
  → configure.getTunnelServer() != null
  → new TunnelClient()
    → setAppName / setId / setTunnelServerUrl / setVersion
  → tunnelClient.start()
    → connect(false)
      → 构造注册URI: ws://server:port/ws?method=agentRegister&arthasVersion=xxx&appName=xxx
      → Netty Bootstrap → WebSocket连接到Tunnel Server
      → Pipeline: SslHandler → HttpClientCodec → HttpObjectAggregator → WebSocketClientProtocolHandler → IdleStateHandler → TunnelClientSocketClientHandler
      → WebSocket握手完成
      → Tunnel Server返回: ?method=agentRegister&id=abc123
      → TunnelClientSocketClientHandler.channelRead0()
        → tunnelClient.setId("abc123")
        → registerPromise.setSuccess()
      → channelFuture.await(10, SECONDS) 返回

=== 诊断阶段 ===
浏览器 → Tunnel Server WebUI → 选择agent → 点击连接
  → Tunnel Server通过已注册的WebSocket发送: ?method=startTunnel&clientConnectionId=xxx&id=abc123
  → TunnelClientSocketClientHandler.channelRead0()
    → 收到startTunnel命令
    → 构造openTunnel URI: ws://server:port/ws?method=openTunnel&clientConnectionId=xxx&id=abc123
    → new ForwardClient(forwardUri) → forwardClient.start()
      → Netty Bootstrap → WebSocket连接到Tunnel Server
      → Pipeline: HttpClientCodec → HttpObjectAggregator → WebSocketClientProtocolHandler → ForwardClientSocketClientHandler
      → WebSocket握手完成
      → ForwardClientSocketClientHandler.userEventTriggered(HANDSHAKE_COMPLETE)
        → connectLocalServer(ctx)
          → Netty Bootstrap (LocalChannel) → 连接本地Arthas ShellServer
          → LocalAddress(NETTY_LOCAL_ADDRESS)
          → Pipeline: HttpClientCodec → HttpObjectAggregator → WebSocketClientProtocolHandler → LocalFrameHandler
          → 本地WebSocket握手完成
          → 移除LocalFrameHandler，添加RelayHandler(forwardClientChannel)
          → 移除ForwardClientSocketClientHandler，添加RelayHandler(localChannel)
          → 双向数据桥建立完成

=== 数据转发阶段 ===
浏览器输入命令 → Tunnel Server → WebSocket → ForwardClient Channel
  → RelayHandler.channelRead()
    → localChannel.writeAndFlush(msg)  // 转发到本地ShellServer
    → ShellServer处理命令
    → ShellServer输出结果 → LocalChannel
  → RelayHandler.channelRead()
    → forwardClientChannel.writeAndFlush(msg)  // 转发回Tunnel Server
    → Tunnel Server → WebSocket → 浏览器显示结果

=== 断线重连阶段 ===
TunnelClient连接断开 → channelUnregistered()
  → setConnected(false)
  → 延迟5秒 → connect(true)  // 重连
    → 携带之前的id → Tunnel Server识别为同一agent
```

### 8.10 Q&A

**Q1: Tunnel Server是如何管理多个agent的？**

A: Tunnel Server为每个注册的agent分配一个唯一ID，并维护agent的WebSocket连接。当浏览器在WebUI中选择某个agent时，Tunnel Server通过该agent的注册连接发送 `startTunnel` 命令，agent收到后创建ForwardClient建立专用隧道。Tunnel Server在转发数据时，通过 `clientConnectionId` 区分不同的浏览器会话，确保数据不会交叉。

**Q2: 如果Tunnel Server宕机了，已经连接的诊断会话会怎样？**

A: 诊断会话会断开——因为ForwardClient的WebSocket连接到Tunnel Server，Tunnel Server宕机后连接断开，RelayHandler的 `channelInactive` 会关闭对端Channel，导致本地ShellServer连接也断开。但TunnelClient的注册连接断开后会自动重连——当Tunnel Server恢复后，agent会重新注册。Tunnel Server的WebUI中会重新显示该agent为在线状态。

**Q3: LocalChannel和TCP loopback有什么区别？为什么选择LocalChannel？**

A: Netty的 `LocalChannel` 是进程内通信机制，数据通过内存拷贝传递，不经过TCP协议栈。相比TCP loopback（127.0.0.1），LocalChannel的优势在于：(1) 零网络开销，延迟更低；(2) 不占用端口；(3) 不可能被外部访问，安全性更高。Arthas在本地通信场景中优先使用LocalChannel，只在需要跨进程通信时才使用TCP。

**Q4: 多个浏览器用户能同时诊断同一个agent吗？**

A: 可以。每次浏览器连接时，Tunnel Server分配不同的 `clientConnectionId`，agent为每个连接创建独立的ForwardClient。每个ForwardClient有自己的LocalChannel连接到ShellServer。但需要注意——多个会话可能看到不同的命令输出，取决于ShellServer的会话隔离机制。

**Q5: 为什么TunnelClient的EventLoopGroup只有2个线程？**

A: TunnelClient需要两个线程是因为：一个线程处理正常的WebSocket IO（接收Tunnel Server的命令），另一个线程用于断线重连时的连接操作。注释中明确说明了这一点："two thread because need to reconnect. #1284"。如果只有一个线程，重连操作可能阻塞正常的IO处理。ForwardClient使用1个线程即可，因为它不需要重连——ForwardClient的生命周期与单次诊断会话绑定。

**Q6: 为什么SSL使用InsecureTrustManagerFactory？**

A: `InsecureTrustManagerFactory` 信任所有服务端证书，不校验证书链。这在企业内部环境中简化了部署——不需要为Tunnel Server配置受信任的CA证书。但在公网环境中，这会导致中间人攻击风险。如果需要安全通信，应该替换为自定义的TrustManager，校验Tunnel Server的证书。

### 8.11 场景总结

Tunnel Server远程诊断是Arthas企业级使用的核心功能。它通过WebSocket建立目标JVM到中心Tunnel Server的注册通道，再通过ForwardClient + LocalChannel建立诊断数据的双向转发桥梁。整体架构设计精巧——注册连接复用（一个agent一个持久连接）、诊断会话独立（每次浏览器连接创建独立的ForwardClient）、本地通信零开销（LocalChannel进程内通信）。安全方面，通过0.0.0.0监听强制密码、多认证方式（Basic/Bearer/LocalConnection）、断线自动重连等机制保障了可用性和安全性。RelayHandler的极简设计——仅40行代码实现了完整的双向数据中继——体现了Netty Channel Pipeline模式在数据转发场景下的优雅和高效。

---

## 总结

本文档分析了四个Arthas真实使用场景的完整源码链路：

1. **mc + redefine 热更新**：从DynamicCompiler的内存编译到Instrumentation.redefineClasses的字节码替换，核心在于ClassLoader的正确选择和ASM类名解析。

2. **tt 时间隧道**：从EnhancerCommand的字节码增强到TimeTunnelAdviceListener的回调链路，最精妙的是ring stack设计——用JDK原生Object[]避免ArthasClassLoader的GC泄漏。

3. **ognl Spring Bean操作**：从ClassLoader选择策略到OgnlExpress的OGNL引擎执行，核心在于ClassLoaderClassResolver的正确类解析和bind(new Object())的边界条件处理。

4. **Tunnel Server远程诊断**：从TunnelClient的WebSocket注册到ForwardClient + RelayHandler的双向桥接，核心在于Netty Pipeline的灵活组合和LocalChannel的进程内通信。

四个场景共同体现了Arthas源码设计的几个核心原则：ClassLoader安全意识（WeakReference、ring stack、unpooledExpress）、防御性编程（文件大小校验、参数合法性检查、0.0.0.0强制密码）、以及Netty Channel Pipeline模式的优雅应用。
# Arthas 源码全流程解析 —— 场景篇（三）

> 本篇覆盖四个真实生产场景的源码级分析：内存泄漏排查、类加载冲突排查、HTTP API 集成运维平台、Profiler 火焰图性能分析。每个场景从用户操作命令出发，逐层追踪源码调用链路，标注关键源码位置，附 Q&A 分析。

---

## 场景九：内存泄漏排查 —— heapdump + vmtool + profiler 联合诊断

### 9.1 用户故事

线上服务每隔几天就会 OOM 重启，怀疑是内存泄漏。GC 日志显示 Full GC 后老年代使用率仍不下降，堆内存持续增长直到 OOM。运维人员需要在 OOM 发生前抓取堆快照进行分析，同时查看哪些对象实例数量异常增长，最终定位泄漏根因。

### 9.2 操作命令与参数说明

```bash
# 第一步：查看 dashboard 内存面板，观察堆/非堆使用趋势
dashboard

# 第二步：查看指定类的实例数量
vmtool -a getInstances --className com.example.OrderRequest -l 100

# 第三步：堆内存分析（按类统计对象数量和内存占用）
vmtool -a heapAnalyze --classNum 30 --objectNum 10

# 第四步：分配内存 profiling
profiler start --event alloc
# 等待 30 秒
profiler stop --format flamegraph --file /tmp/alloc-profile.html

# 第五步：dump 堆快照
heapdump /tmp/heapdump.hprof
# 或只 dump 存活对象（触发一次 GC 后再 dump）
heapdump --live /tmp/heapdump-live.hprof
```

各命令参数说明：

| 命令 | 参数 | 说明 |
|------|------|------|
| `dashboard` | `-n` | 执行次数；`-i` 间隔毫秒数 |
| `vmtool` | `-a getInstances` | 获取指定类的实例 |
| `vmtool` | `--className` | 指定类全名 |
| `vmtool` | `-l/--limit` | 限制返回实例数量 |
| `vmtool` | `-a heapAnalyze` | 堆分析按类统计 |
| `vmtool` | `--classNum` | 返回对象数量最多的前 N 个类 |
| `vmtool` | `--objectNum` | 每个类最多展示前 N 个实例引用 |
| `profiler` | `--event alloc` | 分配内存事件 |
| `profiler` | `--format flamegraph` | 输出火焰图 HTML |
| `profiler` | `--file` | 输出文件路径 |
| `heapdump` | `--live` | 只 dump 存活对象（触发 GC） |

### 9.3 源码调用链路总览

```
dashboard:  ShellLineHandler → ProcessImpl → DashboardCommand.process → DashboardTimerTask.run → MemoryCommand.memoryInfo → ManagementFactory.getMemoryMXBean → DashboardModel → process.appendResult → 渲染输出
vmtool:    ShellLineHandler → ProcessImpl → VmToolCommand.process → vmTool.getInstances / vmTool.heapAnalyze (JNI) → VmToolModel → process.appendResult → process.end
profiler:  ShellLineHandler → ProcessImpl → ProfilerCommand.process → executeArgs → asyncProfiler.execute / asyncProfiler.stop (JNI) → ProfilerModel → process.end
heapdump:  ShellLineHandler → ProcessImpl → HeapDumpCommand.process → ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean) → dumpHeap(file, live) → HeapDumpModel → process.end
```

### 9.4 DashboardCommand 中的内存信息源码分析

#### 9.4.1 DashboardCommand.process() 入口

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/DashboardCommand.java`

```java
@Override
public void process(final CommandProcess process) {
    Session session = process.session();
    timer = new Timer("Timer-for-arthas-dashboard-" + session.getSessionId(), true);
    // ctrl-C support
    process.interruptHandler(new DashboardInterruptHandler(process, timer));

    Handler<Void> stopHandler = new Handler<Void>() {
        @Override
        public void handle(Void event) {
            stop();
        }
    };
    Handler<Void> restartHandler = new Handler<Void>() {
        @Override
        public void handle(Void event) {
            restart(process);
        }
    };
    process.suspendHandler(stopHandler);
    process.resumeHandler(restartHandler);
    process.endHandler(stopHandler);
    process.stdinHandler(new QExitHandler(process));
    // start the timer
    timer.scheduleAtFixedRate(new DashboardTimerTask(process), 0, getInterval());
}
```

逐段解释：

1. **Timer 创建**：创建一个守护线程 Timer，命名为 `Timer-for-arthas-dashboard-{sessionId}`。守护线程意味着即使 dashboard 还在运行，JVM 退出时也不会被阻塞。
2. **中断处理**：注册 `DashboardInterruptHandler`，用户按 Ctrl+C 时可以优雅停止 Timer。
3. **生命周期回调**：
   - `suspendHandler(stopHandler)`：当命令被挂起时停止 Timer，暂停采样。
   - `resumeHandler(restartHandler)`：当命令恢复时重启 Timer，继续采样。
   - `endHandler(stopHandler)`：命令结束时停止 Timer。
4. **`timer.scheduleAtFixedRate`**：以固定速率调度 `DashboardTimerTask`，初始延迟 0ms，间隔由 `getInterval()` 决定（默认 5000ms）。

#### 9.4.2 DashboardTimerTask.run() 核心逻辑

```java
@Override
public void run() {
    try {
        if (count.get() >= getNumOfExecutions()) {
            timer.cancel();
            timer.purge();
            process.end(0, "Process ends after " + getNumOfExecutions() + " time(s).");
            return;
        }
        DashboardModel dashboardModel = new DashboardModel();
        // thread sample
        List<ThreadVO> threads = ThreadUtil.getThreads();
        dashboardModel.setThreads(threadSampler.sample(threads));
        // memory
        dashboardModel.setMemoryInfo(MemoryCommand.memoryInfo());
        // gc
        addGcInfo(dashboardModel);
        // runtime
        addRuntimeInfo(dashboardModel);
        // tomcat
        try {
            addTomcatInfo(dashboardModel);
        } catch (Throwable e) {
            logger.error("try to read tomcat info error", e);
        }
        process.appendResult(dashboardModel);
        count.getAndIncrement();
        process.times().incrementAndGet();
    } catch (Throwable e) {
        String msg = "process dashboard failed: " + e.getMessage();
        logger.error(msg, e);
        process.end(-1, msg);
    }
}
```

逐段解释：

1. **执行次数检查**：`count.get() >= getNumOfExecutions()` 检查是否达到用户指定的执行次数上限。如果到达，取消 Timer 并结束命令。
2. **线程采样**：`ThreadUtil.getThreads()` 获取所有线程信息，`threadSampler.sample(threads)` 进行采样计算（计算 CPU 使用率等增量指标）。
3. **内存信息**：`MemoryCommand.memoryInfo()` 是核心调用，获取堆/非堆/缓冲区内存使用情况。
4. **GC 信息**：`addGcInfo()` 收集各 GC 收集器的执行次数和总耗时。
5. **运行时信息**：`addRuntimeInfo()` 收集 OS、JVM 版本、系统负载、CPU 核数、运行时间等。
6. **Tomcat 信息**：可选采集，通过 HTTP 请求本地 8006 端口获取 Tomcat 连接器统计信息。
7. **`process.appendResult(dashboardModel)`**：将采集结果附加到命令输出流，前端渲染为表格。

#### 9.4.3 MemoryCommand.memoryInfo() 详解

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/MemoryCommand.java`

```java
static Map<String, List<MemoryEntryVO>> memoryInfo() {
    List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
    Map<String, List<MemoryEntryVO>> memoryInfoMap = new LinkedHashMap<String, List<MemoryEntryVO>>();

    // heap
    MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    List<MemoryEntryVO> heapMemEntries = new ArrayList<MemoryEntryVO>();
    heapMemEntries.add(createMemoryEntryVO(TYPE_HEAP, TYPE_HEAP, heapMemoryUsage));
    for (MemoryPoolMXBean poolMXBean : memoryPoolMXBeans) {
        if (MemoryType.HEAP.equals(poolMXBean.getType())) {
            MemoryUsage usage = getUsage(poolMXBean);
            if (usage != null) {
                String poolName = StringUtils.beautifyName(poolMXBean.getName());
                heapMemEntries.add(createMemoryEntryVO(TYPE_HEAP, poolName, usage));
            }
        }
    }
    memoryInfoMap.put(TYPE_HEAP, heapMemEntries);

    // non-heap
    MemoryUsage nonHeapMemoryUsage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
    List<MemoryEntryVO> nonheapMemEntries = new ArrayList<MemoryEntryVO>();
    nonheapMemEntries.add(createMemoryEntryVO(TYPE_NON_HEAP, TYPE_NON_HEAP, nonHeapMemoryUsage));
    for (MemoryPoolMXBean poolMXBean : memoryPoolMXBeans) {
        if (MemoryType.NON_HEAP.equals(poolMXBean.getType())) {
            MemoryUsage usage = getUsage(poolMXBean);
            if (usage != null) {
                String poolName = StringUtils.beautifyName(poolMXBean.getName());
                nonheapMemEntries.add(createMemoryEntryVO(TYPE_NON_HEAP, poolName, usage));
            }
        }
    }
    memoryInfoMap.put(TYPE_NON_HEAP, nonheapMemEntries);

    addBufferPoolMemoryInfo(memoryInfoMap);
    return memoryInfoMap;
}
```

逐段解释：

1. **获取内存池 MXBean 列表**：`ManagementFactory.getMemoryPoolMXBeans()` 返回 JVM 中所有内存池的 MXBean。在 HotSpot JVM 中，典型的堆内存池包括 Eden、Survivor、Old Gen；非堆内存池包括 Metaspace、Code Cache、Compressed Class Space 等。
2. **堆内存汇总**：`ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()` 获取整个堆的 `MemoryUsage` 对象，包含 `used`（已用）、`committed`（已提交）、`max`（最大）三个核心指标。
3. **堆内存池明细**：遍历所有内存池，筛选 `MemoryType.HEAP` 类型的池，获取每个池的 `MemoryUsage`，用 `StringUtils.beautifyName()` 美化名称（如 `G1 Old Gen` → `g1_old_gen`）。
4. **非堆内存汇总**：同理处理非堆内存，包括 Metaspace、Code Cache 等。
5. **缓冲区内存**：`addBufferPoolMemoryInfo()` 通过反射加载 `java.lang.management.BufferPoolMXBean`，获取 direct buffer 和 mapped buffer 的使用情况。这里用反射是因为 `BufferPoolMXBean` 在 Java 9+ 才通过 `ManagementFactory` 暴露。
6. **返回结构**：返回 `LinkedHashMap`（保持插入顺序），包含三组：`heap`、`non_heap`、`buffer_pool`。

辅助方法 `createMemoryEntryVO`：

```java
private static MemoryEntryVO createMemoryEntryVO(String type, String name, MemoryUsage memoryUsage) {
    return new MemoryEntryVO(type, name, memoryUsage.getUsed(), memoryUsage.getCommitted(), memoryUsage.getMax());
}
```

将 `MemoryUsage` 的 `used`/`committed`/`max` 提取到 `MemoryEntryVO` 中，其中 `max` 为 `-1` 表示未定义（如某些非堆内存池）。

异常处理方法 `getUsage`：

```java
private static MemoryUsage getUsage(MemoryPoolMXBean memoryPoolMXBean) {
    try {
        return memoryPoolMXBean.getUsage();
    } catch (InternalError e) {
        // Defensive for potential InternalError with some specific JVM options.
        return null;
    }
}
```

这是一个防御性处理。`MemoryPoolMXBean.getUsage()` 在某些 JVM 选项下可能抛出 `InternalError`（Javadoc 说应返回 null，但实际是 JVM bug），这里 catch 后返回 null 跳过该池。

### 9.5 HeapDumpCommand 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/HeapDumpCommand.java`

```java
@Name("heapdump")
@Summary("Heap dump")
@Description(Constants.EXAMPLE +
        "  heapdump /tmp/dump.hprof\n" +
        "  heapdump --live /tmp/dump.hprof\n" +
        Constants.WIKI + Constants.WIKI_HOME + "heapdump")
public class HeapDumpCommand extends AnnotatedCommand {

    private String filePath;
    private boolean live;

    @Option(shortName = "l", longName = "live")
    @Description("Dump only live objects, which causes a full GC before dump")
    public void setLive(boolean live) {
        this.live = live;
    }

    @Override
    public void process(CommandProcess process) {
        HotSpotDiagnosticMXBean hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        if (hotSpotDiagnosticMXBean == null) {
            process.end(-1, "HotSpotDiagnosticMXBean is not available, maybe not HotSpot JVM.");
            return;
        }

        // generate file path
        String dumpPath = filePath;
        if (dumpPath == null || dumpPath.isEmpty()) {
            // use default path
            File tempFile = new File("");
            dumpPath = new File(tempFile.getAbsolutePath(), "heapdump" + System.currentTimeMillis() + ".hprof").getAbsolutePath();
        }

        try {
            // execute dump
            File dumpFile = new File(dumpPath);
            hotSpotDiagnosticMXBean.dumpHeap(dumpPath, live);
            HeapDumpModel result = new HeapDumpModel();
            result.setDumpPath(dumpFile.getAbsolutePath());
            result.setLive(live);
            process.appendResult(result);
            process.end();
        } catch (Exception e) {
            process.end(-1, "heap dump error: " + e.getMessage());
        }
    }
}
```

逐段解释：

1. **获取 HotSpotDiagnosticMXBean**：`ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)` 获取 JVM 平台 MXBean。这个 MXBean 是 HotSpot JVM 专有的（OpenJDK 和 Oracle JDK 都有），其他 JVM（如 IBM J9）可能不支持。如果返回 null，直接报错退出。

2. **文件路径生成**：如果用户未指定路径，使用当前工作目录生成一个 `heapdump{timestamp}.hprof` 文件名。`System.currentTimeMillis()` 保证文件名唯一性。

3. **执行 dumpHeap**：`hotSpotDiagnosticMXBean.dumpHeap(dumpPath, live)` 是核心调用。这个方法底层调用 `com.sun.management.HotSpotDiagnostic.dumpHeap()`，它通过 JVM TI 接口执行堆 dump。

4. **`--live` 参数**：当 `live=true` 时，JVM 在 dump 前会先执行一次 Full GC，清除所有不可达对象，只 dump 存活对象。这对于排查内存泄漏特别有用——存活对象就是泄漏嫌疑对象。代价是会触发 STW Full GC。

5. **结果封装**：`HeapDumpModel` 包含 dump 文件路径和是否 live dump 的标记，附加到结果流。

6. **异常处理**：常见异常包括磁盘空间不足、路径不可写、JVM 不支持等。

### 9.6 VmToolCommand 源码分析 —— getInstances

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/VmToolCommand.java`

```java
@Name("vmtool")
@Summary("Some jvm tools")
public class VmToolCommand extends AnnotatedCommand {

    private String action;
    private String className;
    private int limit;
    private String express;
    private int classNum;
    private int objectNum;

    @Option(longName = "action", shortName = "a")
    @Description("Action to execute")
    public void setAction(String action) {
        this.action = action;
    }

    @Option(longName = "className", shortName = "c")
    @Description("The class name")
    public void setClassName(String className) {
        this.className = className;
    }

    @Option(longName = "limit", shortName = "l")
    @Description("The limit of objects to get")
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Option(longName = "express")
    @Description("The ognl expression to evaluate")
    public void setExpress(String express) {
        this.express = express;
    }

    @Override
    public void process(CommandProcess process) {
        if ("getInstances".equals(action)) {
            processGetInstances(process);
        } else if ("heapAnalyze".equals(action)) {
            processHeapAnalyze(process);
        } else {
            process.end(-1, "Unsupported action: " + action);
        }
    }
```

`processGetInstances` 方法的核心逻辑：

```java
private void processGetInstances(CommandProcess process) {
    // normalize class name for arrays
    String normalizedClassName = normalizeClassName(className);
    
    try {
        Class<?> clazz = ClassUtils.forName(normalizedClassName);
        if (clazz == null) {
            process.end(-1, "Class not found: " + className);
            return;
        }
        
        // JNI call to get instances
        Object[] instances = vmTool.getInstances(clazz);
        
        // apply limit
        int actualLimit = limit > 0 ? limit : 10;
        int resultLen = Math.min(instances.length, actualLimit);
        
        Object[] limitedInstances = new Object[resultLen];
        System.arraycopy(instances, 0, limitedInstances, 0, resultLen);
        
        // apply OGNL expression if provided
        if (express != null && !express.isEmpty()) {
            // evaluate OGNL expression on each instance
            List<Object> results = new ArrayList<>();
            for (Object instance : limitedInstances) {
                Object result = OgnlExpress.evaluate(express, instance);
                results.add(result);
            }
            // return evaluated results
        }
        
        VmToolModel result = new VmToolModel();
        result.setAction("getInstances");
        result.setClassName(normalizedClassName);
        result.setTotalCount(instances.length);
        result.setInstances(limitedInstances);
        process.appendResult(result);
        process.end();
    } catch (Exception e) {
        process.end(-1, "getInstances error: " + e.getMessage());
    }
}
```

逐段解释：

1. **类名规范化**：`normalizeClassName()` 处理数组类名的特殊表示。例如用户输入 `int[][]`，需要转换为 JVM 内部表示 `[[I`。对于普通类名则保持不变。这是因为 `Class.forName("[[I")` 能正确加载，但 `Class.forName("int[][]")` 不行。

2. **类加载**：`ClassUtils.forName()` 通过 `Class.forName(className, false, ClassLoader.getSystemClassLoader())` 加载类。注意 `initialize=false`，不触发类初始化。

3. **JNI 获取实例**：`vmTool.getInstances(clazz)` 是核心调用。`vmTool` 是 Arthas 的 VM Tool 模块（`arthas-vmtool`），通过 JNI 调用 native 代码遍历 JVM 堆中指定类型的所有实例。底层使用 JVMTI（JVM Tool Interface）的 `IterateOverInstancesOfClass` 函数。

4. **`--limit` 限制**：JNI 返回所有实例后，在 Java 层截取前 N 个。默认 limit 为 10。注意 JNI 层面不限制——它遍历整个堆获取所有实例，然后再在 Java 层裁剪。对于实例数量巨大的类，这可能导致 OOM 或耗时过长。

5. **`--express` OGNL 表达式**：如果用户提供了 OGNL 表达式，会对每个实例执行表达式求值。例如 `--express "#req.userId"` 可以提取每个实例的 `userId` 字段。这使用 Arthas 的 `OgnlExpress` 引擎。

6. **结果封装**：`VmToolModel` 包含类名、实例总数、实例列表。前端渲染时展示实例的 `toString()` 和内存地址。

### 9.7 VmToolCommand 源码分析 —— heapAnalyze

```java
private void processHeapAnalyze(CommandProcess process) {
    int actualClassNum = classNum > 0 ? classNum : 30;
    int actualObjectNum = objectNum > 0 ? objectNum : 10;
    
    try {
        String analyzeResult = vmTool.heapAnalyze(actualClassNum, actualObjectNum);
        
        VmToolModel result = new VmToolModel();
        result.setAction("heapAnalyze");
        result.setHeapAnalyzeResult(analyzeResult);
        process.appendResult(result);
        process.end();
    } catch (Exception e) {
        process.end(-1, "heapAnalyze error: " + e.getMessage());
    }
}
```

逐段解释：

1. **参数默认值**：`classNum` 默认 30（展示对象数最多的前 30 个类），`objectNum` 默认 10（每类展示前 10 个实例引用）。

2. **JNI 堆分析**：`vmTool.heapAnalyze(classNum, objectNum)` 通过 JNI 调用 native 代码，遍历整个堆，按类统计对象数量和内存占用大小。native 层使用 JVMTI 的 `IterateOverHeap` 函数，对每个对象调用 `GetObjectClass` 获取类，`GetByteSize` 获取对象大小。

3. **返回分析文本**：native 层返回一段文本，格式类似：
   ```
   Class  | Count | Size
   java.lang.String | 12345 | 300KB
   java.util.HashMap$Node | 5000 | 200KB
   ...
   ```
   按 Size 降序排列，取前 `classNum` 个类。每类列出前 `objectNum` 个实例的地址引用。

4. **与 MAT 的对比**：`heapAnalyze` 比 `heapdump` 轻量得多——不需要生成完整 hprof 文件，直接在 JVM 内部遍历堆。适合在不能生成大文件的容器环境中使用。但精确度不如 MAT，因为不支持引用链分析。

### 9.8 ProfilerCommand 源码分析 —— start/stop

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/ProfilerCommand.java`

#### 9.8.1 命令参数定义

```java
@Name("profiler")
@Summary("Async Profiler. https://github.com/jvm-profiling-tools/async-profiler")
public class ProfilerCommand extends AnnotatedCommand {

    private String action;
    private String event = "cpu";
    private String format;
    private String file;
    private long duration;
    private long interval = -1;
    private boolean threads;
    private String include;
    private String exclude;

    @Option(longName = "action", shortName = "a")
    @Description("Action to execute")
    public void setAction(String action) {
        this.action = action;
    }

    @Option(longName = "event")
    @Description("Profiling event: cpu, alloc, lock, cache-misses, wall")
    public void setEvent(String event) {
        this.event = event;
    }

    @Option(longName = "format")
    @Description("Output format: flamegraph, tree, flat, collapsed, jfr")
    public void setFormat(String format) {
        this.format = format;
    }

    @Option(longName = "file")
    @Description("Output file path")
    public void setFile(String file) {
        this.file = file;
    }

    @Option(longName = "duration")
    @Description("Duration in seconds. Auto stop after duration")
    public void setDuration(long duration) {
        this.duration = duration;
    }

    @Option(longName = "interval", shortName = "i")
    @Description("Sampling interval in nanoseconds")
    public void setInterval(long interval) {
        this.interval = interval;
    }

    @Option(longName = "threads", shortName = "t")
    @Description("Profile threads separately")
    public void setThreads(boolean threads) {
        this.threads = threads;
    }

    @Option(longName = "include", shortName = "I")
    @Description("Include pattern for stack frames")
    public void setInclude(String include) {
        this.include = include;
    }

    @Option(longName = "exclude", shortName = "X")
    @Description("Exclude pattern for stack frames")
    public void setExclude(String exclude) {
        this.exclude = exclude;
    }
}
```

#### 9.8.2 process() 入口与 action 分发

```java
@Override
public void process(CommandProcess process) {
    if (action == null) {
        process.end(-1, "Please specify action: start, stop, status, list");
        return;
    }

    switch (action.toLowerCase()) {
        case "start":
            processStart(process);
            break;
        case "stop":
            processStop(process);
            break;
        case "status":
            processStatus(process);
            break;
        case "list":
            processList(process);
            break;
        default:
            process.end(-1, "Unsupported action: " + action);
    }
}
```

#### 9.8.3 processStart() —— 启动 profiling

```java
private void processStart(CommandProcess process) {
    try {
        // check if already running
        if (asyncProfiler.getStatus() == ProfilerStatus.RUNNING) {
            process.end(-1, "Profiler is already running. Use 'profiler stop' first.");
            return;
        }

        // build args string
        String args = executeArgs();
        
        // execute start
        asyncProfiler.execute(args);
        
        // schedule auto-stop if duration specified
        if (duration > 0) {
            scheduleStop(process, duration);
        }
        
        ProfilerModel result = new ProfilerModel();
        result.setAction("start");
        result.setEvent(event);
        result.setStatus("running");
        process.appendResult(result);
        process.end();
    } catch (Exception e) {
        process.end(-1, "profiler start error: " + e.getMessage());
    }
}
```

逐段解释：

1. **状态检查**：`asyncProfiler.getStatus()` 检查是否已有 profiler 在运行。async-profiler 不支持同时启动多个 profiling session。

2. **构建参数**：`executeArgs()` 将所有 CLI 参数拼接为 async-profiler 能识别的命令行字符串。

3. **执行启动**：`asyncProfiler.execute(args)` 通过 JNI 调用 async-profiler 的 native 库启动采样。底层调用 `ProfilerStart` 函数。

4. **定时停止**：如果指定了 `--duration`，创建一个延迟任务，在指定秒数后自动执行 stop。

#### 9.8.4 executeArgs() —— 构建参数字符串

```java
private String executeArgs() {
    StringBuilder args = new StringBuilder();
    
    // start sub-command
    args.append("start");
    
    // event
    if (event != null && !event.isEmpty()) {
        args.append(",event=").append(event);
    }
    
    // file
    if (file != null && !file.isEmpty()) {
        args.append(",file=").append(file);
    }
    
    // format
    if (format != null && !format.isEmpty()) {
        args.append(",").append(format);
    }
    
    // interval
    if (interval > 0) {
        args.append(",interval=").append(interval);
    }
    
    // threads
    if (threads) {
        args.append(",threads");
    }
    
    // include pattern
    if (include != null && !include.isEmpty()) {
        args.append(",include=").append(include);
    }
    
    // exclude pattern
    if (exclude != null && !exclude.isEmpty()) {
        args.append(",exclude=").append(exclude);
    }
    
    return args.toString();
}
```

生成的参数字符串示例：
```
start,event=alloc,file=/tmp/alloc-profile.html,flamegraph
```

这个字符串传递给 `asyncProfiler.execute()`，底层解析为 async-profiler 的 C API 调用参数。

#### 9.8.5 processStop() —— 停止 profiling

```java
private void processStop(CommandProcess process) {
    try {
        if (asyncProfiler.getStatus() != ProfilerStatus.RUNNING) {
            process.end(-1, "Profiler is not running.");
            return;
        }
        
        // stop profiling
        asyncProfiler.stop();
        
        // read output
        String resultFile = (file != null && !file.isEmpty()) ? file : "/tmp/arthas-output.html";
        
        ProfilerModel result = new ProfilerModel();
        result.setAction("stop");
        result.setFile(resultFile);
        result.setStatus("stopped");
        
        // if flamegraph format, try to convert to markdown
        if (format == null || "flamegraph".equals(format)) {
            String markdown = processStopMarkdown(resultFile);
            result.setMarkdownResult(markdown);
        }
        
        process.appendResult(result);
        process.end();
    } catch (Exception e) {
        process.end(-1, "profiler stop error: " + e.getMessage());
    }
}
```

逐段解释：

1. **状态检查**：确认 profiler 确实在运行。

2. **停止采样**：`asyncProfiler.stop()` 通过 JNI 调用 async-profiler 的 `ProfilerStop` 函数。stop 时 async-profiler 会将采集的采样数据按照指定格式输出到文件。

3. **文件生成**：如果用户指定了 `--file`，数据写入该文件；否则默认写到 `/tmp/arthas-output.html`。

4. **火焰图生成**：当 `format=flamegraph`（默认）时，async-profiler 在 stop 时生成一个自包含的 HTML 文件，包含 JavaScript 实现的交互式火焰图。文件大小通常在几百 KB 到几 MB。

5. **Markdown 转换**：`processStopMarkdown()` 尝试解析生成的火焰图 HTML，提取调用栈的文本表示，转换为 Markdown 表格格式，方便在终端中查看。

#### 9.8.6 --duration 定时停止

```java
private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, 
    new NamedThreadFactory("arthas-profiler-timer"));

private void scheduleStop(CommandProcess process, long durationSeconds) {
    scheduler.schedule(() -> {
        try {
            asyncProfiler.stop();
            // append stop result
            ProfilerModel result = new ProfilerModel();
            result.setAction("stop");
            result.setStatus("auto-stopped after " + durationSeconds + "s");
            if (file != null) {
                result.setFile(file);
            }
            process.appendResult(result);
        } catch (Exception e) {
            logger.error("auto stop profiler error", e);
        }
    }, durationSeconds, TimeUnit.SECONDS);
}
```

这是一个安全网机制：用户可能在 `profiler start` 后忘记 `profiler stop`，async-profiler 会一直运行，持续采样消耗资源。`--duration` 参数可以确保在指定时间后自动停止。使用 `ScheduledExecutorService` 延迟执行 stop 操作。

### 9.9 完整诊断链路：从 dashboard 到 heapdump 的联合分析流程

```
步骤1: dashboard
  ┌─────────────────────────────────────────────────────┐
  │ DashboardCommand.process()                           │
  │   └─ Timer.scheduleAtFixedRate(DashboardTimerTask)   │
  │       └─ DashboardTimerTask.run()                    │
  │           ├─ ThreadUtil.getThreads()                 │
  │           ├─ MemoryCommand.memoryInfo()              │
  │           │   ├─ ManagementFactory.getMemoryMXBean() │
  │           │   │   └─ getHeapMemoryUsage()            │
  │           │   ├─ getMemoryPoolMXBeans()              │
  │           │   └─ addBufferPoolMemoryInfo()           │
  │           ├─ addGcInfo()                              │
  │           ├─ addRuntimeInfo()                        │
  │           └─ addTomcatInfo()                         │
  └─────────────────────────────────────────────────────┘

步骤2: vmtool -a getInstances
  ┌─────────────────────────────────────────────────────┐
  │ VmToolCommand.process()                              │
  │   └─ processGetInstances()                           │
  │       ├─ normalizeClassName("com.example.OrderReq")  │
  │       ├─ ClassUtils.forName(className)               │
  │       ├─ vmTool.getInstances(clazz)   [JNI/JVMTI]    │
  │       │   └─ IterateOverInstancesOfClass()           │
  │       ├─ apply limit                                 │
  │       ├─ apply OGNL express (optional)               │
  │       └─ VmToolModel → process.appendResult           │
  └─────────────────────────────────────────────────────┘

步骤3: vmtool -a heapAnalyze
  ┌─────────────────────────────────────────────────────┐
  │ VmToolCommand.process()                              │
  │   └─ processHeapAnalyze()                            │
  │       ├─ vmTool.heapAnalyze(classNum, objectNum)     │
  │       │   [JNI/JVMTI]                                │
  │       │   ├─ IterateOverHeap()                       │
  │       │   ├─ GetObjectClass()                        │
  │       │   ├─ GetByteSize()                           │
  │       │   └─ 统计排序返回文本                          │
  │       └─ VmToolModel → process.appendResult           │
  └─────────────────────────────────────────────────────┘

步骤4: profiler start --event alloc
  ┌─────────────────────────────────────────────────────┐
  │ ProfilerCommand.process()                            │
  │   └─ processStart()                                 │
  │       ├─ executeArgs() → "start,event=alloc,..."     │
  │       ├─ asyncProfiler.execute(args)   [JNI]         │
  │       │   └─ ProfilerStart("start,event=alloc")     │
  │       └─ ProfilerModel(status=running)               │
  │                                                      │
  │ (30秒后) profiler stop                               │
  │   └─ processStop()                                   │
  │       ├─ asyncProfiler.stop()   [JNI]                │
  │       │   └─ ProfilerStop() → 生成HTML               │
  │       └─ processStopMarkdown() → Markdown表格         │
  └─────────────────────────────────────────────────────┘

步骤5: heapdump --live /tmp/heapdump-live.hprof
  ┌─────────────────────────────────────────────────────┐
  │ HeapDumpCommand.process()                            │
  │   ├─ ManagementFactory.getPlatformMXBean()           │
  │   │   → HotSpotDiagnosticMXBean                      │
  │   ├─ dumpPath = "/tmp/heapdump-live.hprof"           │
  │   ├─ hotSpotDiagnosticMXBean.dumpHeap(path, true)    │
  │   │   ├─ live=true → 触发Full GC                     │
  │   │   └─ 生成hprof文件                                │
  │   └─ HeapDumpModel → process.end()                   │
  └─────────────────────────────────────────────────────┘
```

### 9.10 async-profiler JNI 集成机制

Arthas 的 profiler 功能依赖于 async-profiler 这个第三方 native 工具。其 JNI 集成过程：

1. **native 库加载**：Arthas 的 `arthas-vmtool` 模块在不同平台下打包了对应的 async-profiler native 库：
   - Linux x86_64: `libasyncProfiler-linux-x64.so`
   - Linux aarch64: `libasyncProfiler-linux-arm64.so`
   - macOS x86_64: `libasyncProfiler-mac-x64.dylib`
   - macOS aarch64: `libasyncProfiler-mac-arm64.dylib`

2. **库选择逻辑**：运行时通过 `os.name` 和 `os.arch` 系统属性判断当前平台，选择对应的库文件，提取到临时目录后通过 `System.load()` 加载。

3. **信号机制**：async-profiler 使用操作系统信号进行采样：
   - `cpu` 事件：使用 `SIGVTALRM`（虚拟定时器信号），在 CPU 上消耗时间时触发
   - `alloc` 事件：使用 `SIGSEGV` 或 hook malloc，在内存分配时触发采样
   - `lock` 事件：hook pthread_mutex 操作
   - `wall` 事件：使用 `SIGPROF`（ PROF定时器），无论线程是否在 CPU 上都采样

4. **采样数据结构**：async-profiler 在 native 层维护一个调用栈计数表（hash map），key 是调用栈的 hash，value 是采样次数。`stop` 时遍历这个表，按照指定格式输出。

### 9.11 火焰图格式详解

#### collapsed 格式（单行折叠）

```
java.lang.Thread.run;java.util.concurrent.ThreadPoolExecutor$Worker.run;java.util.concurrent.ThreadPoolExecutor$Worker.runWorker;com.example.OrderService.process;com.example.OrderService.validate;java.util.HashMap.get 120
```

每个调用栈折叠为一行，分号分隔帧，末尾是采样次数。这是原始数据格式。

#### flamegraph HTML 格式

async-profiler 内嵌了一个简化版的 d3-flame-graph，生成自包含的 HTML 文件：
- 包含内联的 JavaScript 和 CSS
- 支持鼠标悬停查看帧的采样占比
- 支持点击放大某个调用栈分支
- 支持搜索（输入类名高亮匹配的帧）

### 9.12 Q&A 设计问题分析

**Q1: `heapdump --live` 和不带 `--live` 的区别是什么？对生产环境有什么影响？**

A: 带 `--live` 时，`dumpHeap(path, true)` 会先触发一次 Full GC，清除所有不可达对象后再 dump。不带 `--live` 则 dump 整个堆，包括不可达但尚未被 GC 回收的对象。

对生产环境的影响：
- `--live` 会触发 Full GC，导致 STW 停顿（可能几百毫秒到数秒，取决于堆大小）。适合排查内存泄漏——存活对象就是泄漏嫌疑。
- 不带 `--live` 不触发 GC，但 dump 文件更大（包含垃圾对象），分析时干扰信息更多。适合需要分析 GC 行为或对象分配模式的场景。

**Q2: `vmtool -a getInstances` 返回的实例数量和 `heapAnalyze` 统计的数量是否一致？**

A: 不一定一致。两者底层都使用 JVMTI 遍历堆，但执行时刻不同——如果在两次调用之间发生了 GC，结果可能不同。此外，`getInstances` 是遍历完所有实例后一次性返回，而 `heapAnalyze` 是边遍历边统计，不影响实例本身的存活状态。

**Q3: profiler 的 `--event alloc` 和 `heapdump` 各有什么优劣？**

A: `profiler --event alloc` 记录的是对象分配的热点路径（哪些方法分配了最多内存），是增量视角的；`heapdump` 是某一时刻堆内存的快照，是存量视角的。排查内存泄漏时两者互补：profiler 告诉你"谁在不停分配"，heapdump 告诉你"哪些对象没被回收"。

**Q4: dashboard 的内存数据来自 `ManagementFactory`，和 `heapAnalyze` 的数据来源有什么不同？**

A: dashboard 的 `memoryInfo()` 使用 JMX MXBean，获取的是 JVM 统计的内存区域使用量（used/committed/max），粒度是内存池级别。`heapAnalyze` 使用 JVMTI 直接遍历堆中每个对象，粒度是对象级别。前者是汇总数据，后者是明细数据。

**Q5: 在容器环境中，`heapdump` 生成的 hprof 文件可能非常大，有什么替代方案？**

A: 可以用 `vmtool -a heapAnalyze` 替代，它在 JVM 内部完成堆分析，只返回文本结果，不需要生成大文件。也可以用 `profiler --event alloc` 记录分配热点。如果必须生成 hprof，建议使用 `--live` 减小文件大小，并将文件挂载到共享卷。

### 9.13 场景总结

本场景演示了 Arthas 在内存泄漏排查中的联合诊断能力：

1. **dashboard** 提供 JVM 全景视图，通过定时采样发现内存增长趋势。核心源码在 `DashboardCommand.DashboardTimerTask.run()`，内存数据来自 `MemoryCommand.memoryInfo()`，底层通过 JMX `ManagementFactory` 获取。

2. **vmtool getInstances** 通过 JNI/JVMTI 直接获取指定类的存活实例，支持 `--limit` 限制和 `--express` OGNL 表达式提取字段，是快速验证对象泄漏的利器。

3. **vmtool heapAnalyze** 提供轻量级堆分析，按类统计对象数量和内存占用，无需生成 hprof 文件，适合容器环境。

4. **profiler --event alloc** 通过 async-profiler 的 JNI 集成，记录内存分配热点路径，生成火焰图帮助定位"谁在不停创建对象"。

5. **heapdump** 通过 `HotSpotDiagnosticMXBean.dumpHeap()` 生成标准 hprof 文件，可配合 MAT 等工具进行深度分析。`--live` 选项触发 GC 只 dump 存活对象，过滤干扰信息。

五条命令从不同维度互补：dashboard 监控趋势、getInstances 验证假设、heapAnalyze 统计分布、profiler 定位热点、heapdump 留存证据，形成完整的内存泄漏诊断闭环。

---

## 场景十：类加载冲突排查 —— sc + classloader + jad 三板斧

### 10.1 用户故事

应用启动时报 `ClassNotFoundException` 或 `NoSuchMethodError`，怀疑是类加载冲突——同一个类被不同的 ClassLoader 加载了不同版本。这在 Spring Boot fat jar、OSGi、Tomcat webapp 等多 ClassLoader 环境中极为常见。需要排查到底加载了哪个版本的类、在哪个 jar 包里、是被哪个 ClassLoader 加载的。

### 10.2 操作命令与参数说明

```bash
# 第一步：搜索类，看是否被多次加载
sc -d com.example.OrderService

# 第二步：查看 ClassLoader 树
classloader -t

# 第三步：查看每个 ClassLoader 加载了哪些类
classloader -c 18b4aac2 --all

# 第四步：查看 ClassLoader 的 URLs
classloader -c 18b4aac2

# 第五步：查看 jar 和类的对应关系
classloader -c 18b4aac2 --url-classes --jar example*

# 第六步：反编译不同 ClassLoader 加载的同名类
jad com.example.OrderService -c 18b4aac2 --source-only
jad com.example.OrderService -c 2f3a4b1c --source-only
```

参数说明：

| 命令 | 参数 | 说明 |
|------|------|------|
| `sc` | `-d/--detail` | 显示类详细信息 |
| `sc` | `-f/--field` | 显示字段信息 |
| `sc` | `-c/--classloader` | 按 ClassLoader hash 过滤 |
| `classloader` | `-t/--tree` | 树状展示 ClassLoader 层级 |
| `classloader` | `-c/--hash` | 指定 ClassLoader hash |
| `classloader` | `--all` | 列出该 ClassLoader 加载的所有类 |
| `classloader` | `--url-classes` | 显示 URL 和类对应关系 |
| `classloader` | `--jar` | 过滤 jar 包名 |
| `jad` | `-c/--classloader` | 指定 ClassLoader 反编译 |
| `jad` | `--source-only` | 只输出源代码 |

### 10.3 源码调用链路总览

```
sc:          ShellLineHandler → ProcessImpl → SearchClassCommand.process → SearchUtils.searchClass → ClassUtils.createClassInfo → SearchClassModel → process.end
classloader: ShellLineHandler → ProcessImpl → ClassLoaderCommand.process → processClassLoaders/processClassLoader/processUrlClasses → ClassLoaderModel → process.end
jad:         ShellLineHandler → ProcessImpl → JadCommand.process → ClassUtils.searchClass → ClassDecompiler → JadModel → process.end
```

### 10.4 SearchClassCommand (sc 命令) 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/SearchClassCommand.java`

#### 10.4.1 命令参数定义

```java
@Name("sc")
@Summary("Search all the classes loaded by JVM")
@Description(Constants.EXAMPLE +
        "  sc -d org.apache.commons.lang.StringUtils\n" +
        "  sc -d org.apache.commons.lang.*Utils\n" +
        "  sc -d -f org.apache.commons.lang.StringUtils\n" +
        "  sc -d org.apache.commons.lang.StringUtils -c 18b4aac2\n" +
        Constants.WIKI + Constants.WIKI_HOME + "sc")
public class SearchClassCommand extends AnnotatedCommand {

    private String classPattern;
    private boolean isRegEx = false;
    private boolean detail = false;
    private boolean isField = false;
    private String code;
    private Integer expand;

    @Argument(index = 0, argName = "class-pattern")
    @Description("Class name pattern, use either '.' or '/' as separator")
    public void setClassPattern(String classPattern) {
        this.classPattern = classPattern;
    }

    @Option(shortName = "d", longName = "detail")
    @Description("Print class detail info")
    public void setDetail(boolean detail) {
        this.detail = detail;
    }

    @Option(shortName = "f", longName = "field")
    @Description("Print class field info")
    public void setField(boolean isField) {
        this.isField = isField;
    }

    @Option(shortName = "c", longName = "classloader")
    @Description("The hash code of the special class's classLoader")
    public void setCode(String code) {
        this.code = code;
    }
}
```

#### 10.4.2 process() 核心逻辑

```java
@Override
public void process(CommandProcess process) {
    // 1. match and search loaded classes
    Set<Class<?>> matchedClasses = SearchUtils.searchClass(inst, classPattern, isRegEx, code);

    if (matchedClasses == null || matchedClasses.isEmpty()) {
        process.end(-1, "No class found for: " + classPattern);
        return;
    }

    // 2. build result model
    SearchClassModel result = new SearchClassModel();

    if (detail) {
        // -d: show detailed info for each matched class
        List<ClassDetailVO> classDetailVOs = new ArrayList<ClassDetailVO>();
        for (Class<?> clazz : matchedClasses) {
            ClassDetailVO classInfo = ClassUtils.createClassInfo(clazz, isField, expand);
            classDetailVOs.add(classInfo);
        }
        result.setClassDetailVOs(classDetailVOs);
    } else {
        // default: show simple info (name, hash, classloader)
        List<ClassVO> classVOs = ClassUtils.createClassVOList(matchedClasses);
        result.setMatchedClasses(classVOs);
    }

    process.appendResult(result);
    process.end();
}
```

逐段解释：

1. **类搜索**：`SearchUtils.searchClass(inst, classPattern, isRegEx, code)` 是核心调用。`inst` 是 Arthas 启动时通过 Java Agent 获取的 `Instrumentation` 对象。`classPattern` 是用户输入的类名模式（支持通配符或正则）。`code` 是 ClassLoader hash 码，用于过滤。

2. **搜索结果判空**：如果没有匹配到任何类，直接报错退出。

3. **详细模式**：当 `-d` 参数启用时，对每个匹配的类调用 `ClassUtils.createClassInfo()` 生成详细信息 VO（`ClassDetailVO`），包含 ClassLoader、code source、修饰符、注解、接口、父类等。

4. **简单模式**：默认只显示类名、ClassLoader hash、ClassLoader 名称（`ClassVO`）。

5. **结果输出**：`SearchClassModel` 封装结果，通过 `process.appendResult()` 附加到输出流。

### 10.5 SearchUtils.searchClass() 底层搜索机制

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/util/SearchUtils.java`

```java
public static Set<Class<?>> searchClass(Instrumentation inst, String classPattern, boolean isRegEx, String code) {
    Set<Class<?>> matchedClasses = searchClass(inst, classPattern, isRegEx);
    return filter(matchedClasses, code);
}
```

分两步：先按类名匹配搜索，再按 ClassLoader hash 过滤。

#### 10.5.1 按类名搜索

```java
public static Set<Class<?>> searchClass(Instrumentation inst, String classPattern, boolean isRegEx) {
    Matcher<String> classNameMatcher = classNameMatcher(classPattern, isRegEx);
    return GlobalOptions.isDisableSubClass ? searchClass(inst, classNameMatcher) :
            searchSubClass(inst, searchClass(inst, classNameMatcher));
}
```

1. **构建匹配器**：`classNameMatcher()` 根据 `isRegEx` 参数选择 `RegexMatcher` 或 `WildcardMatcher`。`WildcardMatcher` 支持 `*` 和 `?` 通配符，会将类名中的 `/` 替换为 `.`。

2. **子类搜索**：默认情况下（`GlobalOptions.isDisableSubClass = false`），搜索结果包含匹配类的子类。这是通过 `searchSubClass()` 实现的——遍历所有已加载类，使用 `isAssignableFrom()` 判断是否是目标类的子类或实现类。

3. **核心遍历**：

```java
public static Set<Class<?>> searchClass(Instrumentation inst, Matcher<String> classNameMatcher, int limit) {
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

关键点：`inst.getAllLoadedClasses()` 返回 JVM 中所有已加载的类。这是 `Instrumentation` 接口提供的能力，底层调用 JVMTI 的 `GetLoadedClasses` 函数。返回的数组包含：
- Bootstrap ClassLoader 加载的类（如 `java.lang.String`）
- App ClassLoader 加载的类
- 自定义 ClassLoader 加载的类

如果同一个类被两个不同的 ClassLoader 加载，会作为两个不同的 `Class<?>` 对象出现在数组中。

#### 10.5.2 按 ClassLoader hash 过滤

```java
private static Set<Class<?>> filter(Set<Class<?>> matchedClasses, String code) {
    if (code == null) {
        return matchedClasses;
    }
    Set<Class<?>> result = new HashSet<Class<?>>();
    if (matchedClasses != null) {
        for (Class<?> c : matchedClasses) {
            if (c.getClassLoader() != null && Integer.toHexString(c.getClassLoader().hashCode()).equals(code)) {
                result.add(c);
            }
        }
    }
    return result;
}
```

逐行解释：

1. 如果 `code` 为 null（用户未指定 `-c`），不做过滤。
2. 遍历匹配的类，取 `c.getClassLoader()` 获取加载该类的 ClassLoader。
3. 将 ClassLoader 的 `hashCode()` 转为十六进制字符串，与用户指定的 `code` 比较。
4. 注意 `getClassLoader()` 返回 null 表示 Bootstrap ClassLoader（加载核心 JDK 类），这种情况会被过滤掉。

### 10.6 ClassUtils.createClassInfo() 详解

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/util/ClassUtils.java`

```java
public static ClassDetailVO createClassInfo(Class clazz, boolean withFields, Integer expand) {
    CodeSource cs = clazz.getProtectionDomain().getCodeSource();
    ClassDetailVO classInfo = new ClassDetailVO();
    classInfo.setName(StringUtils.classname(clazz));
    classInfo.setClassInfo(StringUtils.classname(clazz));
    classInfo.setCodeSource(ClassUtils.getCodeSource(cs));
    classInfo.setInterface(clazz.isInterface());
    classInfo.setAnnotation(clazz.isAnnotation());
    classInfo.setEnum(clazz.isEnum());
    classInfo.setAnonymousClass(clazz.isAnonymousClass());
    classInfo.setArray(clazz.isArray());
    classInfo.setLocalClass(clazz.isLocalClass());
    classInfo.setMemberClass(clazz.isMemberClass());
    classInfo.setPrimitive(clazz.isPrimitive());
    classInfo.setSynthetic(clazz.isSynthetic());
    classInfo.setSimpleName(clazz.getSimpleName());
    classInfo.setModifier(StringUtils.modifier(clazz.getModifiers(), ','));
    classInfo.setAnnotations(TypeRenderUtils.getAnnotations(clazz));
    classInfo.setInterfaces(TypeRenderUtils.getInterfaces(clazz));
    classInfo.setSuperClass(TypeRenderUtils.getSuperClass(clazz));
    classInfo.setClassloader(TypeRenderUtils.getClassloader(clazz));
    classInfo.setClassLoaderHash(StringUtils.classLoaderHash(clazz));
    if (withFields) {
        classInfo.setFields(TypeRenderUtils.getFields(clazz, expand));
    }
    return classInfo;
}
```

逐行解释：

1. **CodeSource 获取**：`clazz.getProtectionDomain().getCodeSource()` 从类的 `ProtectionDomain` 中获取 `CodeSource`。`CodeSource.getLocation()` 返回该类加载来源的 URL（如 `file:/path/to/example.jar`）。这是判断类来自哪个 jar 的关键信息。

2. **类名规范化**：`StringUtils.classname(clazz)` 处理数组类名等特殊情况，返回可读的类名字符串。

3. **code source 提取**：

```java
public static String getCodeSource(final CodeSource cs) {
    if (null == cs || null == cs.getLocation() || null == cs.getLocation().getFile()) {
        return com.taobao.arthas.core.util.Constants.EMPTY_STRING;
    }
    return cs.getLocation().getFile();
}
```

返回类的来源文件路径。对于 jar 中的类，返回 `/path/to/example.jar`；对于非 jar 的类，返回 class 文件目录。如果 `CodeSource` 为 null（如动态生成的类），返回空字符串。

4. **类属性收集**：通过一系列 `isXxx()` 方法收集类的布尔属性：是否接口、是否注解、是否枚举、是否匿名类、是否数组、是否局部类、是否成员类、是否基本类型、是否合成类。

5. **修饰符**：`StringUtils.modifier(clazz.getModifiers(), ',')` 将 `getModifiers()` 返回的 int 值转换为可读的修饰符字符串（如 `public,abstract`）。

6. **ClassLoader 信息**：

```java
public static String classLoaderHash(Class<?> clazz) {
    if (clazz == null || clazz.getClassLoader() == null) {
        return "null";
    }
    return Integer.toHexString(clazz.getClassLoader().hashCode());
}
```

ClassLoader hash 是 `ClassLoader.hashCode()` 的十六进制表示。**同一个 ClassLoader 实例的 hashCode 是固定的**（默认是内存地址的某种映射），所以可以用这个值在 `classloader -c` 命令中指定。

### 10.7 ClassLoaderCommand 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/ClassLoaderCommand.java`

#### 10.7.1 命令参数

```java
@Name("classloader")
@Summary("Show classloader info")
public class ClassLoaderCommand extends AnnotatedCommand {

    private boolean tree = false;
    private String hash;
    private boolean all = false;
    private boolean urlClasses = false;
    private String jarPattern;

    @Option(shortName = "t", longName = "tree")
    @Description("Display ClassLoader tree")
    public void setTree(boolean tree) {
        this.tree = tree;
    }

    @Option(shortName = "c", longName = "hash")
    @Description("The hash code of the special classLoader")
    public void setHash(String hash) {
        this.hash = hash;
    }

    @Option(longName = "all")
    @Description("Display all classes loaded by classLoader")
    public void setAll(boolean all) {
        this.all = all;
    }

    @Option(longName = "url-classes")
    @Description("Display the url and classes relation")
    public void setUrlClasses(boolean urlClasses) {
        this.urlClasses = urlClasses;
    }

    @Option(longName = "jar")
    @Description("Filter jar name pattern")
    public void setJarPattern(String jarPattern) {
        this.jarPattern = jarPattern;
    }
}
```

#### 10.7.2 process() 分发逻辑

```java
@Override
public void process(CommandProcess process) {
    if (tree) {
        // -t: show ClassLoader tree
        processClassLoaders(process);
    } else if (hash != null) {
        // -c <hash>: operate on specific ClassLoader
        if (all) {
            // -c <hash> --all: list all classes loaded by this ClassLoader
            processClassLoaderClasses(process);
        } else if (urlClasses) {
            // -c <hash> --url-classes --jar <pattern>: show jar-class mapping
            processUrlClasses(process);
        } else {
            // -c <hash>: show ClassLoader URLs
            processClassLoaderUrls(process);
        }
    } else {
        // default: list all ClassLoaders
        processClassLoaders(process);
    }
}
```

分发逻辑清晰：
- `-t`：展示 ClassLoader 树
- `-c <hash>`：展示指定 ClassLoader 的 URLs
- `-c <hash> --all`：展示指定 ClassLoader 加载的所有类
- `-c <hash> --url-classes --jar <pattern>`：展示 jar 和类的对应关系
- 无参数：默认列出所有 ClassLoader

#### 10.7.3 processClassLoaders() —— ClassLoader 树构建

```java
private void processClassLoaders(CommandProcess process) {
    // 1. collect all classloaders from loaded classes
    Set<ClassLoader> classLoaders = new HashSet<ClassLoader>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        ClassLoader loader = clazz.getClassLoader();
        if (loader != null) {
            classLoaders.add(loader);
        }
    }
    // also add systemClassLoader and its parents
    ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
    while (systemLoader != null) {
        classLoaders.add(systemLoader);
        systemLoader = systemLoader.getParent();
    }

    // 2. build ClassLoader tree
    ClassLoaderNode root = buildTree(classLoaders);

    // 3. create model
    ClassLoaderModel model = new ClassLoaderModel();
    model.setClassLoaderTree(root);
    process.appendResult(model);
    process.end();
}
```

逐段解释：

1. **收集 ClassLoader**：遍历 `inst.getAllLoadedClasses()` 获取所有已加载类，从每个类的 `getClassLoader()` 获取对应的 ClassLoader。由于同一个 ClassLoader 会加载很多类，用 `Set` 去重。注意 Bootstrap ClassLoader 返回 null，不会加入集合。

2. **补充系统 ClassLoader 链**：从 `ClassLoader.getSystemClassLoader()` 开始，沿 `getParent()` 一直追溯到根（AppClassLoader → ExtClassLoader → null）。确保完整的 ClassLoader 层级被纳入。

3. **构建树**：`buildTree()` 方法的核心逻辑：

```java
private ClassLoaderNode buildTree(Set<ClassLoader> classLoaders) {
    // find root (parent == null or parent not in set)
    ClassLoaderNode root = null;
    Map<ClassLoader, ClassLoaderNode> nodeMap = new HashMap<>();

    // create nodes
    for (ClassLoader loader : classLoaders) {
        ClassLoaderNode node = new ClassLoaderNode();
        node.setLoader(loader);
        node.setHash(Integer.toHexString(loader.hashCode()));
        node.setName(loader.toString());
        nodeMap.put(loader, node);
    }

    // link parent-child
    for (ClassLoader loader : classLoaders) {
        ClassLoaderNode node = nodeMap.get(loader);
        ClassLoader parent = loader.getParent();
        if (parent == null || !nodeMap.containsKey(parent)) {
            // this is a root node
            if (root == null) {
                root = new ClassLoaderNode();
                root.setName("BootstrapClassLoader");
                root.setHash("null");
            }
            root.addChild(node);
        } else {
            ClassLoaderNode parentNode = nodeMap.get(parent);
            parentNode.addChild(node);
        }
    }
    return root;
}
```

关键点：
- 遍历所有 ClassLoader，创建 `ClassLoaderNode`，记录 hash 和 toString。
- 遍历第二次，通过 `getParent()` 建立父子关系。如果 parent 为 null 或不在集合中（Bootstrap ClassLoader），挂到虚拟根节点下。
- 这样构建出的树形结构能够清晰展示 ClassLoader 的层级关系。

#### 10.7.4 processClassLoaderClasses() —— 列出 ClassLoader 加载的类

```java
private void processClassLoaderClasses(CommandProcess process) {
    // find the ClassLoader by hash
    ClassLoader targetLoader = findClassLoaderByHash(hash);
    if (targetLoader == null) {
        process.end(-1, "Can not find classloader by hash: " + hash);
        return;
    }

    // get all classes loaded by this ClassLoader
    // using Instrumentation.getInitiatedClasses()
    Class[] classes = inst.getInitiatedClasses(targetLoader);
    
    List<ClassVO> classVOs = new ArrayList<>();
    for (Class<?> clazz : classes) {
        if (clazz.getClassLoader() == targetLoader) {
            // only show classes actually loaded by this loader (not delegated to parent)
            ClassVO vo = ClassUtils.createSimpleClassInfo(clazz);
            classVOs.add(vo);
        }
    }

    ClassLoaderModel model = new ClassLoaderModel();
    model.setHash(hash);
    model.setLoaderName(targetLoader.toString());
    model.setClasses(classVOs);
    process.appendResult(model);
    process.end();
}
```

逐段解释：

1. **查找 ClassLoader**：`findClassLoaderByHash(hash)` 遍历所有 ClassLoader，比较 hashCode 的十六进制字符串。

2. **获取已加载类**：`inst.getInitiatedClasses(targetLoader)` 返回该 ClassLoader **发起加载**的类列表。注意这与 `getAllLoadedClasses()` 不同——`getInitiatedClasses` 返回的是该 ClassLoader 作为初始加载器请求加载的类，但实际可能委托给了父加载器。因此需要 `clazz.getClassLoader() == targetLoader` 二次过滤，只保留实际由该 ClassLoader 加载的类。

3. **过滤实际加载类**：`getClassLoader() == targetLoader` 确保只显示实际由该 ClassLoader 定义的类，而非通过双亲委派委托给父加载器的类。这是排查类加载冲突的关键——如果同一个类名出现在多个 ClassLoader 中，说明双亲委派被打破。

#### 10.7.5 processClassLoaderUrls() —— 查看 ClassLoader URLs

```java
private void processClassLoaderUrls(CommandProcess process) {
    ClassLoader targetLoader = findClassLoaderByHash(hash);
    if (targetLoader == null) {
        process.end(-1, "Can not find classloader by hash: " + hash);
        return;
    }

    List<String> urls = new ArrayList<>();
    if (targetLoader instanceof URLClassLoader) {
        URLClassLoader urlClassLoader = (URLClassLoader) targetLoader;
        for (URL url : urlClassLoader.getURLs()) {
            urls.add(url.toString());
        }
    } else {
        // try to get URLs via reflection
        try {
            Method getURLs = targetLoader.getClass().getMethod("getURLs");
            URL[] urlArray = (URL[]) getURLs.invoke(targetLoader);
            for (URL url : urlArray) {
                urls.add(url.toString());
            }
        } catch (Exception e) {
            urls.add("Can not get URLs from: " + targetLoader.getClass().getName());
        }
    }

    ClassLoaderModel model = new ClassLoaderModel();
    model.setHash(hash);
    model.setLoaderName(targetLoader.toString());
    model.setUrls(urls);
    process.appendResult(model);
    process.end();
}
```

逐段解释：

1. **URLClassLoader 路径**：如果目标 ClassLoader 是 `URLClassLoader` 的实例（传统 Tomcat WebappClassLoader 就是），直接调用 `getURLs()` 获取 URL 列表。这些 URL 通常是 jar 文件路径或目录路径，代表该 ClassLoader 的搜索路径。

2. **反射路径**：如果不是 URLClassLoader（如 Spring Boot 的 LaunchedURLClassLoader，或某些自定义 ClassLoader），通过反射尝试调用 `getURLs()` 方法。这是一个兼容性处理。

3. **用途**：通过 URLs 可以看到该 ClassLoader 的 classpath，判断是否有冲突版本的 jar 包同时存在。

#### 10.7.6 processUrlClasses() —— jar 和类对应关系

```java
private void processUrlClasses(CommandProcess process) {
    ClassLoader targetLoader = findClassLoaderByHash(hash);
    if (targetLoader == null) {
        process.end(-1, "Can not find classloader by hash: " + hash);
        return;
    }

    // collect all classes loaded by this loader
    Set<Class<?>> loadedClasses = new HashSet<>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        if (clazz.getClassLoader() == targetLoader) {
            loadedClasses.add(clazz);
        }
    }

    // build url -> classes mapping using ProtectionDomain/CodeSource
    UrlClassStatBuilder builder = new UrlClassStatBuilder();
    for (Class<?> clazz : loadedClasses) {
        CodeSource cs = clazz.getProtectionDomain().getCodeSource();
        String url = (cs != null && cs.getLocation() != null) ? cs.getLocation().getFile() : "unknown";
        if (jarPattern != null && !url.contains(jarPattern)) {
            continue; // filter by jar pattern
        }
        builder.addClass(url, clazz.getName());
    }

    String result = builder.buildStatText();
    ClassLoaderModel model = new ClassLoaderModel();
    model.setHash(hash);
    model.setLoaderName(targetLoader.toString());
    model.setUrlClassStat(result);
    process.appendResult(model);
    process.end();
}
```

逐段解释：

1. **收集该 ClassLoader 加载的类**：遍历所有已加载类，筛选 `getClassLoader() == targetLoader` 的类。

2. **从 ProtectionDomain 获取来源 URL**：`clazz.getProtectionDomain().getCodeSource().getLocation().getFile()` 返回该类加载时的来源路径。对于 jar 中的类，这是 jar 文件路径；对于目录中的类，这是 class 文件所在目录。

3. **jar 过滤**：`jarPattern` 参数过滤 URL，只展示匹配的 jar 包对应的类。

4. **统计构建**：`UrlClassStatBuilder` 按 URL 分组，统计每个 URL 下有多少个类，生成文本统计结果。这帮助用户快速判断某个 jar 包是否被该 ClassLoader 加载，以及加载了哪些类。

### 10.8 完整链路：从类搜索到反编译

```
sc -d com.example.OrderService
  ┌─────────────────────────────────────────────────────────┐
  │ SearchClassCommand.process()                            │
  │   ├─ SearchUtils.searchClass(inst, "com.example.*", ..) │
  │   │   ├─ classNameMatcher("com.example.*", false)        │
  │   │   │   → WildcardMatcher                             │
  │   │   ├─ inst.getAllLoadedClasses()   [JVMTI]           │
  │   │   │   → 遍历所有已加载类                               │
  │   │   ├─ classNameMatcher.matching(clazz.getName())      │
  │   │   └─ SearchUtils.searchSubClass() (optional)         │
  │   │       └─ isAssignableFrom()                           │
  │   │   → Set<Class<?>> matchedClasses                     │
  │   │                                                      │
  │   ├─ SearchUtils.filter(matchedClasses, code)            │
  │   │   └─ classLoader.hashCode() → hex compare            │
  │   │   → Set<Class<?>> filtered                           │
  │   │                                                      │
  │   ├─ ClassUtils.createClassInfo(clazz, isField, expand)  │
  │   │   ├─ clazz.getProtectionDomain().getCodeSource()     │
  │   │   ├─ clazz.isInterface() / isEnum() / ...            │
  │   │   ├─ TypeRenderUtils.getClassloader(clazz)           │
  │   │   │   └─ clazz.getClassLoader().toString()            │
  │   │   └─ StringUtils.classLoaderHash(clazz)               │
  │   │       └─ Integer.toHexString(loader.hashCode())      │
  │   │   → ClassDetailVO                                   │
  │   │                                                      │
  │   └─ SearchClassModel → process.appendResult → end       │
  └─────────────────────────────────────────────────────────┘

classloader -t
  ┌─────────────────────────────────────────────────────────┐
  │ ClassLoaderCommand.process()                             │
  │   └─ processClassLoaders()                              │
  │       ├─ inst.getAllLoadedClasses()                     │
  │       ├─ 收集所有 ClassLoader (getClassLoader != null)   │
  │       ├─ 补充 SystemClassLoader 链                       │
  │       ├─ buildTree(classLoaders)                         │
  │       │   ├─ 创建 ClassLoaderNode                        │
  │       │   ├─ 通过 getParent() 建立父子关系                │
  │       │   └─ BootstrapClassLoader 作为虚拟根              │
  │       └─ ClassLoaderModel → process.end                  │
  └─────────────────────────────────────────────────────────┘

classloader -c 18b4aac2 --all
  ┌─────────────────────────────────────────────────────────┐
  │ ClassLoaderCommand.process()                             │
  │   └─ processClassLoaderClasses()                        │
  │       ├─ findClassLoaderByHash("18b4aac2")              │
  │       │   └─ 遍历所有 ClassLoader 比较 hashCode           │
  │       ├─ inst.getInitiatedClasses(targetLoader)          │
  │       ├─ 过滤 getClassLoader() == targetLoader            │
  │       ├─ ClassUtils.createSimpleClassInfo(clazz)         │
  │       └─ ClassLoaderModel → process.end                  │
  └─────────────────────────────────────────────────────────┘

classloader -c 18b4aac2 --url-classes --jar example*
  ┌─────────────────────────────────────────────────────────┐
  │ ClassLoaderCommand.process()                             │
  │   └─ processUrlClasses()                                │
  │       ├─ findClassLoaderByHash("18b4aac2")              │
  │       ├─ 收集 getClassLoader() == targetLoader 的类       │
  │       ├─ clazz.getProtectionDomain().getCodeSource()     │
  │       │   → 来源 URL                                     │
  │       ├─ jarPattern 过滤                                  │
  │       ├─ UrlClassStatBuilder 按URL分组统计                 │
  │       └─ ClassLoaderModel → process.end                  │
  └─────────────────────────────────────────────────────────┘

jad com.example.OrderService -c 18b4aac2 --source-only
  ┌─────────────────────────────────────────────────────────┐
  │ JadCommand.process()                                    │
  │   ├─ SearchUtils.searchClass(inst, className, false, c) │
  │   │   → 找到指定 ClassLoader 加载的类                     │
  │   ├─ ClassDecompiler.decompile(clazz)                   │
  │   │   └─ 使用 CFR/Procyon 反编译                        │
  │   └─ JadModel(source-only) → process.end                │
  └─────────────────────────────────────────────────────────┘
```

### 10.9 BootstrapClassLoader 的特殊处理

在 Java 中，Bootstrap ClassLoader 是 JVM 内部用 C++ 实现的，不是一个 Java 对象。因此：

- `clazz.getClassLoader()` 返回 `null` 表示该类由 Bootstrap ClassLoader 加载
- `ClassLoader.getSystemClassLoader().getParent()` 返回 `null` 表示到达了 Bootstrap 层级

在 Arthas 源码中，对 null 的处理体现在多处：

```java
// SearchUtils.filter() 中:
if (c.getClassLoader() != null && Integer.toHexString(c.getClassLoader().hashCode()).equals(code)) {
    result.add(c);
}
// 如果 getClassLoader() 返回 null，该类被跳过

// ClassUtils.createClassInfo() 中:
classInfo.setClassLoaderHash(StringUtils.classLoaderHash(clazz));
// classLoaderHash() 中:
if (clazz == null || clazz.getClassLoader() == null) {
    return "null";
}
// Bootstrap 加载的类，hash 显示为 "null"

// ClassUtils.createClassLoaderVO() 中:
classLoaderVO.setName(classLoader==null?"BootstrapClassLoader":classLoader.toString());
// Bootstrap 显示为 "BootstrapClassLoader" 字符串
```

### 10.10 Q&A 设计问题分析

**Q1: 为什么同一个类名会被多次加载？双亲委派机制不是应该防止这种情况吗？**

A: 双亲委派机制要求 ClassLoader 在加载类时先委托父加载器。但如果自定义 ClassLoader 重写了 `loadClass()` 方法（而非 `findClass()`），就可能绕过双亲委派。典型场景：
- Tomcat 的 WebappClassLoader 打破了双亲委派，每个 webapp 有独立的 ClassLoader
- OSGi 的 Bundle ClassLoader 实现了网状委派
- Spring Boot 的 fat jar 中，嵌套 jar 的类由 LaunchedURLClassLoader 加载

在 `sc -d` 的输出中，如果看到同名类有不同的 `classLoaderHash`，就是类加载冲突。

**Q2: `Instrumentation.getInitiatedClasses(loader)` 和 `getAllLoadedClasses()` 有什么区别？**

A: `getInitiatedClasses(loader)` 返回由指定 ClassLoader **发起**加载请求的类列表。这些类可能由该 ClassLoader 加载，也可能由其父加载器加载（因为双亲委派）。`getAllLoadedClasses()` 返回 JVM 中所有已加载的类，不区分 ClassLoader。

在 `classloader -c <hash> --all` 中，先用 `getInitiatedClasses` 获取候选列表，再用 `getClassLoader() == targetLoader` 精确过滤，确保只展示实际由该 ClassLoader 定义的类。

**Q3: `sc -d` 显示的 `code-source` 是怎么获取的？如果为空是什么原因？**

A: 通过 `clazz.getProtectionDomain().getCodeSource().getLocation()` 获取。`ProtectionDomain` 在类加载时由 ClassLoader 设置。如果为空，可能是：
- 动态生成的类（如 CGLIB 代理、Lambda 表达式）没有 `ProtectionDomain`
- 某些自定义 ClassLoader 没有正确设置 `ProtectionDomain`
- 基本类型和数组类型没有 `CodeSource`

**Q4: `classloader -c <hash>` 中 hash 值是否会变化？**

A: `ClassLoader.hashCode()` 基于对象的内存地址（Object 的默认实现），在 JVM 生命周期内是稳定的（只要该 ClassLoader 对象没被 GC）。但如果应用被热部署（旧的 ClassLoader 被 GC，新的被创建），hash 值会变化。因此 hash 值只在当前 JVM 进程内有效，不能跨重启使用。

**Q5: 在 `classloader -c <hash> --url-classes --jar <pattern>` 中，为什么用 ProtectionDomain 而不是遍历 ClassPath？**

A: ProtectionDomain 的 CodeSource 记录的是类实际加载的来源，而不是 ClassLoader 的搜索路径。搜索路径（URLs）可能包含很多 jar，但类可能只从其中部分 jar 加载。通过 ProtectionDomain 可以精确知道每个类来自哪个 jar，这对排查"同名类来自不同 jar"的冲突至关重要。

### 10.11 场景总结

本场景演示了 Arthas 在类加载冲突排查中的三板斧：

1. **sc -d** 通过 `Instrumentation.getAllLoadedClasses()` 搜索类，`ClassUtils.createClassInfo()` 提取类的 `CodeSource`、`ClassLoader`、`classLoaderHash` 等关键信息。如果同名类有不同 hash，说明被多个 ClassLoader 加载。

2. **classloader -t** 收集所有 ClassLoader，通过 `getParent()` 字段构建树形结构，展示 ClassLoader 层级关系。BootstrapClassLoader 作为虚拟根节点处理。

3. **classloader -c <hash> --all** 使用 `getInitiatedClasses()` + `getClassLoader()` 二次过滤，精确列出某 ClassLoader 实际加载的类。

4. **classloader -c <hash> --url-classes** 通过 `ProtectionDomain.getCodeSource()` 获取每个类的来源 URL，建立 jar 与类的对应关系，帮助定位冲突 jar 包。

5. **jad -c <hash>** 反编译指定 ClassLoader 加载的类，直接对比不同版本的字节码差异。

三板斧的组合使用流程：先用 sc 发现同名类被多次加载 → 用 classloader -t 理解 ClassLoader 层级 → 用 classloader -c --all 找到冲突类在哪个 ClassLoader → 用 classloader -c --url-classes 找到冲突 jar → 用 jad -c 对比字节码差异，定位根因。

---

## 场景十一：HTTP API 集成到运维平台 —— 自动化诊断

### 11.1 用户故事

公司有一套运维平台，希望集成 Arthas 能力，实现自动化的 Java 诊断。比如自动检查所有服务的线程状态、自动执行特定命令并收集结果。不能依赖人工 telnet 连接——运维平台需要通过 HTTP API 程序化地与 Arthas 交互，实现批量诊断、定时巡检、故障自动分析等能力。

### 11.2 操作方式与接口说明

```bash
# 第一步：初始化 Session
curl -X POST http://target-host:8563/api -d '{"action":"init_session"}'

# 返回：{"state":"SUCCEEDED","sessionId":"xxxx","consumerId":"yyyy"}

# 第二步：同步执行命令
curl -X POST http://target-host:8563/api -d '{
  "action":"exec",
  "sessionId":"xxxx",
  "command":"thread -n 3",
  "execTimeout":10000
}'

# 返回：{"state":"SUCCEEDED","body":[{...thread results...}]}

# 第三步：异步执行长耗时命令
curl -X POST http://target-host:8563/api -d '{
  "action":"async_exec",
  "sessionId":"xxxx",
  "command":"trace com.example.OrderService createOrder"
}'

# 返回：{"state":"SCHEDULED","jobId":"zzzz"}

# 第四步：拉取异步结果
curl -X POST http://target-host:8563/api -d '{
  "action":"pull_results",
  "sessionId":"xxxx",
  "consumerId":"yyyy"
}'

# 返回：{"state":"SUCCEEDED","body":[{...trace results...}]}
```

接口参数说明：

| Action | 参数 | 说明 |
|--------|------|------|
| `init_session` | 无 | 创建新 Session，返回 sessionId + consumerId |
| `exec` | sessionId, command, execTimeout | 同步执行命令，阻塞等待结果 |
| `async_exec` | sessionId, command | 异步执行命令，立即返回 jobId |
| `pull_results` | sessionId, consumerId | 拉取异步命令的结果队列 |
| `interrupt_job` | sessionId, jobId | 中断正在执行的异步 Job |
| `join_session` | sessionId | 加入已有 Session |
| `close_session` | sessionId | 关闭 Session |
| `session_info` | sessionId | 获取 Session 信息 |

### 11.3 源码调用链路总览

```
HTTP请求 → Netty Pipeline → HttpRequestHandler → HttpApiHandler.handle()
  → JSON解析 → ApiRequest → processRequest()
    → INIT_SESSION: sessionManager.createSession() → SharingResultDistributor → ResultConsumer → 返回sessionId
    → EXEC: dispatchRequest() → shellServer.createJob() → job.run() → waitForJob() → 打包结果返回
    → ASYNC_EXEC: dispatchRequest() → shellServer.createJob() → job.run() → 立即返回SCHEDULED
    → PULL_RESULTS: resultConsumer.poll() → 返回结果队列
```

### 11.4 ApiRequest / ApiResponse / ApiAction / ApiState 数据结构

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/api/`

#### 11.4.1 ApiRequest

```java
public class ApiRequest {
    private String action;       // 动作类型：init_session, exec, async_exec, pull_results 等
    private String command;       // 要执行的 Arthas 命令（如 "thread -n 3"）
    private String requestId;    // 请求唯一标识，用于追踪
    private String sessionId;     // Session ID
    private String consumerId;   // 结果消费者 ID
    private Integer execTimeout;  // 同步执行超时时间（毫秒）
    private String userId;       // 用户标识
}
```

这是一个纯 POJO，通过 JSON 反序列化填充。每个字段的用途：
- `action`：决定走哪个处理分支，对应 `ApiAction` 枚举
- `command`：完整的 Arthas 命令字符串，会被解析为 Job 执行
- `requestId`：用于关联请求和响应，方便调用方追踪
- `sessionId` / `consumerId`：Session 管理，多个命令可以在同一个 Session 中执行
- `execTimeout`：同步执行模式下的超时时间，默认 30 秒

#### 11.4.2 ApiResponse

```java
public class ApiResponse<T> {
    private String requestId;     // 对应请求的 requestId
    private ApiState state;       // 响应状态
    private String message;       // 错误消息（FAILED 时有值）
    private String sessionId;     // Session ID
    private String consumerId;   // 结果消费者 ID
    private String jobId;         // 异步执行返回的 Job ID
    private T body;              // 结果体（EXEC 返回命令输出，PULL_RESULTS 返回结果数组）
}
```

泛型设计：`body` 的类型根据 action 不同而变化。EXEC 的 body 是结果列表，INIT_SESSION 的 body 为空（sessionId 和 consumerId 在顶层字段中）。

#### 11.4.3 ApiAction 枚举

```java
public enum ApiAction {
    EXEC,            // 同步执行命令
    ASYNC_EXEC,      // 异步执行命令
    INTERRUPT_JOB,   // 中断正在执行的 Job
    PULL_RESULTS,    // 拉取异步结果
    INIT_SESSION,    // 创建新 Session
    JOIN_SESSION,    // 加入已有 Session
    CLOSE_SESSION,   // 关闭 Session
    SESSION_INFO     // 获取 Session 信息
}
```

#### 11.4.4 ApiState 枚举

```java
public enum ApiState {
    SCHEDULED,     // 异步任务已调度
    SUCCEEDED,     // 请求成功
    INTERRUPTED,   // 请求被中断
    FAILED,        // 请求失败
    REFUSED        // 请求被拒绝
}
```

### 11.5 HttpApiHandler.handle() 核心源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/shell/term/impl/http/api/HttpApiHandler.java`

#### 11.5.1 handle() 入口

```java
public void handle(HttpRequest request, HttpResponse response) {
    // 1. HTTP method check
    if (!request.getMethod().equals(HttpMethod.POST)) {
        sendErrorResponse(response, "Only POST method is supported", HttpResponseStatus.METHOD_NOT_ALLOWED);
        return;
    }

    // 2. Parse request body as JSON
    String body = request.getContent().toString(CharsetUtil.UTF_8);
    ApiRequest apiRequest;
    try {
        apiRequest = JSON.parseObject(body, ApiRequest.class);
    } catch (Exception e) {
        sendErrorResponse(response, "Invalid JSON body: " + e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        return;
    }

    if (apiRequest == null || apiRequest.getAction() == null) {
        sendErrorResponse(response, "Missing 'action' in request body", HttpResponseStatus.BAD_REQUEST);
        return;
    }

    // 3. Dispatch request
    processRequest(apiRequest, response);
}
```

逐段解释：

1. **HTTP 方法检查**：只接受 POST 请求。GET 等其他方法返回 405。这是因为 Arthas 的 API 需要在 body 中传递 JSON 参数。

2. **JSON 解析**：将 HTTP body 解析为 `ApiRequest` 对象。使用 FastJSON 解析。如果 JSON 格式错误，返回 400 Bad Request。

3. **参数校验**：检查 `action` 字段是否存在。如果缺失，返回 400。

4. **分发处理**：`processRequest()` 根据 action 类型分发到不同的处理方法。

#### 11.5.2 processRequest() 分发逻辑

```java
private void processRequest(ApiRequest request, HttpResponse response) {
    ApiAction action;
    try {
        action = ApiAction.valueOf(request.getAction().toUpperCase());
    } catch (IllegalArgumentException e) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Unknown action: " + request.getAction())
            .setRequestId(request.getRequestId()));
        return;
    }

    switch (action) {
        case INIT_SESSION:
            handleInitSession(request, response);
            break;
        case EXEC:
            handleExec(request, response);
            break;
        case ASYNC_EXEC:
            handleAsyncExec(request, response);
            break;
        case PULL_RESULTS:
            handlePullResults(request, response);
            break;
        case INTERRUPT_JOB:
            handleInterruptJob(request, response);
            break;
        case CLOSE_SESSION:
            handleCloseSession(request, response);
            break;
        case SESSION_INFO:
            handleSessionInfo(request, response);
            break;
        case JOIN_SESSION:
            handleJoinSession(request, response);
            break;
        default:
            sendResponse(response, new ApiResponse<>()
                .setState(ApiState.FAILED)
                .setMessage("Unsupported action: " + action)
                .setRequestId(request.getRequestId()));
    }
}
```

将用户输入的 action 字符串转为 `ApiAction` 枚举，然后 switch 分发。每个 action 对应一个 `handleXxx()` 方法。

#### 11.5.3 handleInitSession() —— 创建 Session

```java
private void handleInitSession(ApiRequest request, HttpResponse response) {
    try {
        // 1. create session via session manager
        Session session = sessionManager.createSession();

        // 2. create result distributor and consumer
        String sessionId = session.getSessionId();
        SharingResultDistributor distributor = new SharingResultDistributor(sessionId);
        ResultConsumer consumer = new ResultConsumer();
        distributor.addConsumer(consumer);

        // 3. register in session manager
        sessionManager.setResultDistributor(sessionId, distributor);
        String consumerId = consumer.getId();
        sessionManager.setResultConsumer(sessionId, consumerId, consumer);

        // 4. build response
        ApiResponse<Object> apiResponse = new ApiResponse<>();
        apiResponse.setRequestId(request.getRequestId());
        apiResponse.setState(ApiState.SUCCEEDED);
        apiResponse.setSessionId(sessionId);
        apiResponse.setConsumerId(consumerId);
        sendResponse(response, apiResponse);
    } catch (Exception e) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Failed to init session: " + e.getMessage())
            .setRequestId(request.getRequestId()));
    }
}
```

逐段解释：

1. **创建 Session**：`sessionManager.createSession()` 创建一个新的 `Session` 对象，分配唯一的 `sessionId`。Session 在 Arthas 中代表一个诊断会话上下文，包含命令历史、配置等状态。

2. **创建结果分发器**：`SharingResultDistributor` 是一个结果分发器——当命令执行产生结果时，结果通过 distributor 分发给所有注册的 consumer。一个 session 可以有多个 consumer（多个运维平台同时查看同一 session 的输出）。

3. **创建结果消费者**：`ResultConsumer` 内部维护一个结果队列。命令执行时，结果被推入 consumer 的队列。`pull_results` 时从队列中取出。

4. **注册关联**：将 sessionId、distributor、consumer 的关系注册到 `sessionManager` 中，后续操作通过 sessionId 查找。

5. **返回 sessionId 和 consumerId**：调用方需要保存这两个 ID，后续所有操作都需要。

#### 11.5.4 handleExec() —— 同步执行

```java
private void handleExec(ApiRequest request, HttpResponse response) {
    // 1. validate session
    Session session = sessionManager.getSession(request.getSessionId());
    if (session == null) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Session not found: " + request.getSessionId())
            .setRequestId(request.getRequestId()));
        return;
    }

    // 2. validate command
    String command = request.getCommand();
    if (command == null || command.isEmpty()) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Command is empty")
            .setRequestId(request.getRequestId()));
        return;
    }

    // 3. determine timeout
    int timeout = request.getExecTimeout() != null ? request.getExecTimeout() : 30000;

    try {
        // 4. dispatch and execute
        Job job = dispatchRequest(session, command);

        // 5. wait for job completion with timeout
        List<Result> results = waitForJob(job, timeout);

        // 6. build response
        ApiResponse<List<Object>> apiResponse = new ApiResponse<>();
        apiResponse.setRequestId(request.getRequestId());
        apiResponse.setState(ApiState.SUCCEEDED);
        apiResponse.setSessionId(request.getSessionId());
        apiResponse.setBody(convertResults(results));
        sendResponse(response, apiResponse);
    } catch (TimeoutException e) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Exec timeout after " + timeout + "ms")
            .setRequestId(request.getRequestId()));
    } catch (Exception e) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Exec failed: " + e.getMessage())
            .setRequestId(request.getRequestId()));
    }
}
```

逐段解释：

1. **Session 校验**：通过 `sessionManager.getSession()` 查找 session。如果 session 不存在或已关闭，返回 FAILED。

2. **命令校验**：确保 command 非空。

3. **超时确定**：默认 30 秒。用户可通过 `execTimeout` 自定义。超时后命令会被中断。

4. **命令分发与执行**：`dispatchRequest()` 是核心方法：

```java
private Job dispatchRequest(Session session, String command) {
    // create a virtual terminal for this request
    ApiTerm term = new ApiTerm(session, 1000, 200);
    
    // parse command and create job
    Job job = shellServer.createJob(command, term, session);
    return job;
}
```

`ApiTerm` 是一个虚拟终端，width=1000、height=200，不需要真实终端连接。它实现了 `Term` 接口，但输入输出都走 API 通道而非 telnet/websocket。

`shellServer.createJob()` 解析命令字符串，找到对应的 `Command` 实例，创建 `Job` 对象。

5. **等待完成**：`waitForJob()` 轮询检查 job 状态：

```java
private List<Result> waitForJob(Job job, int timeout) throws TimeoutException {
    long deadline = System.currentTimeMillis() + timeout;
    while (job.status() != Job.Status.TERMINATED) {
        if (System.currentTimeMillis() > deadline) {
            job.interrupt();
            throw new TimeoutException();
        }
        Thread.sleep(50);
    }
    // collect all results
    return job.getResults();
}
```

每 50ms 轮询一次 job 状态。超时则中断 job 并抛出 `TimeoutException`。

6. **结果转换**：`convertResults()` 将 Arthas 内部的 `Result` 对象列表转为 JSON 可序列化的格式。

#### 11.5.5 handleAsyncExec() —— 异步执行

```java
private void handleAsyncExec(ApiRequest request, HttpResponse response) {
    // 1. validate session
    Session session = sessionManager.getSession(request.getSessionId());
    if (session == null) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Session not found: " + request.getSessionId())
            .setRequestId(request.getRequestId()));
        return;
    }

    // 2. validate command
    String command = request.getCommand();
    if (command == null || command.isEmpty()) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Command is empty")
            .setRequestId(request.getRequestId()));
        return;
    }

    try {
        // 3. dispatch and start job
        Job job = dispatchRequest(session, command);
        job.run(); // start async execution
        
        // 4. immediately return SCHEDULED status
        ApiResponse<Object> apiResponse = new ApiResponse<>();
        apiResponse.setRequestId(request.getRequestId());
        apiResponse.setState(ApiState.SCHEDULED);
        apiResponse.setSessionId(request.getSessionId());
        apiResponse.setJobId(job.id());
        sendResponse(response, apiResponse);
    } catch (Exception e) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Async exec failed: " + e.getMessage())
            .setRequestId(request.getRequestId()));
    }
}
```

与同步执行的关键区别：
1. **不等待完成**：`job.run()` 启动异步执行后立即返回。
2. **返回 SCHEDULED 状态**：告知调用方命令已开始执行，结果需要通过 `pull_results` 拉取。
3. **返回 jobId**：用于后续中断操作（`interrupt_job`）。

#### 11.5.6 handlePullResults() —— 拉取结果

```java
private void handlePullResults(ApiRequest request, HttpResponse response) {
    // 1. validate session
    Session session = sessionManager.getSession(request.getSessionId());
    if (session == null) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Session not found: " + request.getSessionId())
            .setRequestId(request.getRequestId()));
        return;
    }

    // 2. get result consumer
    ResultConsumer consumer = sessionManager.getResultConsumer(request.getSessionId(), request.getConsumerId());
    if (consumer == null) {
        sendResponse(response, new ApiResponse<>()
            .setState(ApiState.FAILED)
            .setMessage("Consumer not found: " + request.getConsumerId())
            .setRequestId(request.getRequestId()));
        return;
    }

    // 3. poll results from queue
    List<Object> results = consumer.poll();

    // 4. build response
    ApiResponse<List<Object>> apiResponse = new ApiResponse<>();
    apiResponse.setRequestId(request.getRequestId());
    apiResponse.setState(ApiState.SUCCEEDED);
    apiResponse.setSessionId(request.getSessionId());
    apiResponse.setConsumerId(request.getConsumerId());
    apiResponse.setBody(results);
    sendResponse(response, apiResponse);
}
```

逐段解释：

1. **Session 校验**：确认 session 存在。
2. **Consumer 查找**：通过 sessionId + consumerId 查找对应的 `ResultConsumer`。
3. **结果拉取**：`consumer.poll()` 从 consumer 的内部队列中取出所有已产生的结果。这是一个非阻塞操作——如果队列为空，返回空列表。
4. **结果返回**：结果列表放入 `body` 字段返回。调用方可以反复调用 `pull_results` 直到 body 为空列表。

### 11.7 ApiTerm —— 虚拟终端

```java
public class ApiTerm implements Term {
    private final Session session;
    private final int width;
    private final int height;
    
    public ApiTerm(Session session, int width, int height) {
        this.session = session;
        this.width = width;
        this.height = height;
    }
    
    @Override
    public int width() {
        return width; // 1000, wide enough for table output
    }
    
    @Override
    public int height() {
        return height; // 200
    }
    
    @Override
    public Term flush() {
        return this; // no-op, API mode doesn't have a real terminal
    }
    
    @Override
    public void close() {
        // no real terminal to close
    }
}
```

`ApiTerm` 是 HTTP API 模式的虚拟终端实现。它不需要真正的终端连接，只是为了满足 `Term` 接口的要求。宽度和高度设置得足够大（1000x200），确保表格类输出不会被截断。

### 11.8 结果分发机制

```
┌──────────────────────────────────────────────────────────────┐
│ Session (sessionId)                                          │
│                                                              │
│  ┌──────────────┐     ┌──────────────────────┐             │
│  │   Job #1     │────▶│ SharingResultDistributor            │
│  │ (async_exec) │     │                                      │
│  └──────────────┘     │   ┌─────────────────┐              │
│                       │──▶│ ResultConsumer A │ (consumerId) │
│  ┌──────────────┐     │   │  ┌───────────┐  │              │
│  │   Job #2     │────▶│   │  │ result[0] │  │              │
│  │ (async_exec) │     │   │  │ result[1] │  │              │
│  └──────────────┘     │   │  │ result[2] │  │              │
│                       │   │  └───────────┘  │              │
│                       │   └─────────────────┘              │
│                       │                                      │
│                       │   ┌─────────────────┐              │
│                       │──▶│ ResultConsumer B │              │
│                           └─────────────────┘              │
└──────────────────────────────────────────────────────────────┘
```

关键设计：

1. **SharingResultDistributor**：一个 Session 有一个 distributor。当 Job 产生结果时，结果被推给 distributor，distributor 将结果复制到所有注册的 consumer 队列中。

2. **ResultConsumer**：每个 consumer 有独立的队列。多个运维平台可以同时消费同一个 session 的结果。`poll()` 是非阻塞的，取出队列中所有结果。

3. **Job 与 Session 的关系**：一个 Session 可以有多个 Job 并发执行。每个 Job 执行完成后，结果通过 distributor 分发。

### 11.9 安全认证机制

Arthas HTTP API 支持两种认证方式：

#### 11.9.1 Basic 认证

```java
private boolean authenticate(HttpRequest request) {
    String auth = request.headers().get("Authorization");
    if (auth == null || !auth.startsWith("Basic ")) {
        return false;
    }
    
    // decode Base64
    String decoded = new String(Base64.decode(auth.substring(6)), CharsetUtil.UTF_8);
    int colon = decoded.indexOf(':');
    if (colon < 0) {
        return false;
    }
    
    String username = decoded.substring(0, colon);
    String password = decoded.substring(colon + 1);
    
    // check credentials
    return checkCredentials(username, password);
}
```

调用方在 HTTP 头中传递 `Authorization: Basic base64(user:pass)`。Arthas 解码后验证用户名密码。凭据在 Arthas 启动参数中配置。

#### 11.9.2 Bearer Token 认证

```java
private boolean authenticate(HttpRequest request) {
    String auth = request.headers().get("Authorization");
    if (auth != null && auth.startsWith("Bearer ")) {
        String token = auth.substring(7);
        return validateToken(token);
    }
    // fall back to Basic auth
    return authenticateBasic(request);
}
```

Bearer Token 适合运维平台场景——平台获取 token 后在每次请求中携带，无需每次传输密码。Token 可以配置有效期和权限范围。

### 11.10 完整链路：从 HTTP 请求到命令执行结果返回

```
同步执行 (EXEC):
  ┌─────────────────────────────────────────────────────────────────┐
  │ HTTP POST /api                                                   │
  │   body: {"action":"exec","sessionId":"xxx","command":"thread"}  │
  │                                                                  │
  │  ↓ Netty Pipeline                                                │
  │  ↓ HttpServerHandler.channelRead()                                │
  │  ↓ HttpRequestDecoder → FullHttpRequest                          │
  │  ↓                                                                │
  │ HttpApiHandler.handle()                                          │
  │   ├─ JSON.parseObject(body, ApiRequest.class)                    │
  │   ├─ processRequest(apiRequest)                                  │
  │   │   └─ ApiAction.EXEC → handleExec()                           │
  │   │       ├─ sessionManager.getSession("xxx")                    │
  │   │       ├─ dispatchRequest(session, "thread")                  │
  │   │       │   ├─ new ApiTerm(session, 1000, 200)                │
  │   │       │   ├─ shellServer.createJob("thread", term, session) │
  │   │       │   │   └─ CommandParser.parse("thread")              │
  │   │       │   │   └─ new ProcessImpl(ThreadCommand, ...)        │
  │   │       │   └─ return Job                                       │
  │   │       ├─ job.run() → ThreadCommand.process()                  │
  │   │       │   └─ 结果写入 ResultDistributor                       │
  │   │       ├─ waitForJob(job, 30000)                              │
  │   │       │   └─ 轮询 job.status() == TERMINATED                  │
  │   │       └─ ApiResponse(SUCCEEDED, body=results)                │
  │   └─ JSON.toJSONString(apiResponse) → HTTP Response              │
  └──────────────────────────────────────────────────────────────────┘

异步执行 (ASYNC_EXEC):
  ┌─────────────────────────────────────────────────────────────────┐
  │ HTTP POST /api                                                   │
  │   body: {"action":"async_exec","sessionId":"xxx",               │
  │          "command":"trace com.example.Service method"}           │
  │                                                                  │
  │ HttpApiHandler.handle() → handleAsyncExec()                      │
  │   ├─ dispatchRequest() → createJob() → job.run()                 │
  │   │   └─ 异步执行，结果推入 ResultConsumer 队列                    │
  │   └─ ApiResponse(SCHEDULED, jobId="zzz") → 立即返回               │
  │                                                                  │
  │ (后续) HTTP POST /api                                            │
  │   body: {"action":"pull_results","sessionId":"xxx",             │
  │          "consumerId":"yyy"}                                     │
  │                                                                  │
  │ HttpApiHandler.handle() → handlePullResults()                    │
  │   ├─ resultConsumer.poll() → List<Result>                        │
  │   └─ ApiResponse(SUCCEEDED, body=results)                        │
  └──────────────────────────────────────────────────────────────────┘
```

### 11.11 ApiJobHandler 与 Job 生命周期

```java
public class ApiJobHandler implements JobListener {
    
    @Override
    public void onForeground(Job job) {
        // Job 转为前台执行（同步模式时）
        // 不需要特殊处理
    }
    
    @Override
    public void onBackground(Job job) {
        // Job 转为后台执行（异步模式时）
        // 结果会持续推入 ResultConsumer 队列
    }
    
    @Override
    public void onTerminate(Job job) {
        // Job 执行结束
        // 同步模式：waitForJob 检测到 TERMINATED 后返回结果
        // 异步模式：结果已推入队列，等待 pull_results
    }
    
    @Override
    public void onSuspend(Job job) {
        // Job 被挂起（如 dashboard、monitor 等持续运行命令）
        // 异步模式下，已产生的结果可以 pull_results
    }
}
```

关键点：
- `onTerminate`：Job 正常结束或异常终止时触发。同步模式下 `waitForJob` 退出轮询。
- `onSuspend`：dashboard、monitor 等命令不会自动终止，会持续产生结果。异步模式下每次 `pull_results` 可以获取增量结果。
- `onForeground` / `onBackground`：HTTP API 模式下一般不需要前台/后台切换，这些回调主要用于交互式终端场景。

### 11.12 Q&A 设计问题分析

**Q1: EXEC 同步执行如果命令一直不结束（如 `monitor` 持续监控命令），会怎样？**

A: `waitForJob` 会一直轮询直到 `execTimeout` 超时，然后调用 `job.interrupt()` 中断命令并返回 FAILED + 超时消息。对于持续运行类命令（dashboard、monitor、trace -n 不设上限），建议使用 `async_exec` + `pull_results` 模式，而不是同步 `exec`。

**Q2: 一个 Session 可以同时执行多个 async_exec 吗？**

A: 可以。每个 `async_exec` 创建一个独立的 Job，Job 之间互不影响。所有 Job 的结果通过同一个 `SharingResultDistributor` 分发到 consumer 队列。调用方通过 `pull_results` 可以获取所有 Job 的结果（按产生时间排序）。

**Q3: ResultConsumer 队列是否有大小限制？如果一直不拉取会怎样？**

A: consumer 内部使用有界队列（默认容量 1000 个 Result）。如果队列满了，新结果会被丢弃并记录日志。因此调用方应定期 `pull_results` 拉取结果，避免队列溢出导致结果丢失。

**Q4: HTTP API 和 telnet/websocket 有什么本质区别？**

A: telnet/websocket 是交互式连接——用户输入一行命令，看到结果后再输入下一行。HTTP API 是请求-响应模式——每次请求是一次完整的命令执行。HTTP API 不支持交互式命令（如 `jad` 后可以 `q` 退出这种交互），因为每次请求是独立的。但对于运维平台自动化场景，HTTP API 更适合——可以编程化地批量执行命令并收集结果。

**Q5: 如何在运维平台中实现"自动巡检所有服务的线程状态"？**

A: 平台流程：
1. 对每个目标服务调用 `init_session` 获取 sessionId + consumerId
2. 调用 `exec` 执行 `thread -n 3`（同步，10 秒超时）
3. 解析返回的 body 中的线程信息，检查是否有死锁或 CPU 飙高
4. 调用 `close_session` 清理
5. 汇总所有服务的巡检结果

对于需要持续监控的场景：
1. `init_session` 一次
2. `async_exec` 执行 `dashboard -i 5000 -n 12`（5 秒间隔，执行 12 次共 60 秒）
3. 每隔 10 秒 `pull_results` 拉取增量数据
4. 60 秒后最后一个 `pull_results` 获取完整结果
5. `close_session` 清理

### 11.13 场景总结

本场景演示了 Arthas HTTP API 的完整设计和源码实现：

1. **HttpApiHandler** 是入口，通过 Netty HTTP Pipeline 接收 POST 请求，JSON 解析为 `ApiRequest`，通过 `processRequest()` 按 action 分发。

2. **Session 管理**：`init_session` 创建 Session 和 `SharingResultDistributor` + `ResultConsumer` 对。Session 是命令执行的上下文，distributor 负责结果分发，consumer 负责结果缓存。

3. **同步执行 (EXEC)**：`dispatchRequest` 创建 `ApiTerm` 虚拟终端和 Job，`job.run()` 执行命令，`waitForJob` 轮询等待完成（默认 30 秒超时），打包所有结果一次性返回。

4. **异步执行 (ASYNC_EXEC)**：创建 Job 并启动后立即返回 `SCHEDULED` 状态和 jobId。结果持续推入 consumer 队列，调用方通过 `pull_results` 拉取。

5. **安全认证**：支持 Basic 认证和 Bearer Token 认证，适合运维平台的程序化访问场景。

6. **ApiTerm 虚拟终端**：width=1000, height=200，满足 `Term` 接口但不连接真实终端，是 HTTP API 模式的关键适配器。

HTTP API 的核心价值在于让 Arthas 从"人工诊断工具"升级为"可编程的诊断平台"——运维平台可以批量、定时、自动化地执行诊断命令并收集结果，无需人工干预。

---

## 场景十二：Profiler 火焰图性能分析 —— async-profiler 集成

### 12.1 用户故事

服务性能下降，接口 P99 延迟从 50ms 上升到 200ms，但 CPU 使用率没有明显异常。需要生成火焰图来分析 CPU 热点和方法调用链路，定位到底是哪个方法消耗了时间。需要用 async-profiler 采集数据并生成交互式火焰图。

### 12.2 操作命令与参数说明

```bash
# CPU profiling
profiler start --event cpu --format flamegraph --file /tmp/cpu-flame.html

# 内存分配 profiling
profiler start --event alloc --format flamegraph --file /tmp/alloc-flame.html

# 等待 60 秒后自动停止
profiler stop --duration 60

# 查看状态
profiler status

# 采样间隔 1ms（1000000 纳秒）
profiler start -i 1000000

# 只 profiling 特定线程
profiler start --event cpu --threads

# 过滤栈：只看 com.example 包，排除 java 包
profiler start -I "com.example.*" -X "java.*"

# 查看支持的事件列表
profiler list
```

参数说明：

| 参数 | 说明 |
|------|------|
| `--event` | 采样事件：cpu, alloc, lock, cache-misses, wall, itimer |
| `--format` | 输出格式：flamegraph, tree, flat, collapsed, jfr |
| `--file` | 输出文件路径 |
| `--duration` | 自动停止时间（秒） |
| `-i/--interval` | 采样间隔（纳秒） |
| `-t/--threads` | 按线程分离采样 |
| `-I/--include` | 包含模式：只保留匹配的栈帧 |
| `-X/--exclude` | 排除模式：过滤掉匹配的栈帧 |
| `--action` | start / stop / status / list |

### 12.3 源码调用链路总览

```
start: ShellLineHandler → ProcessImpl → ProfilerCommand.process → processStart → executeArgs → asyncProfiler.execute [JNI] → ProfilerModel(running) → process.end
stop:  ShellLineHandler → ProcessImpl → ProfilerCommand.process → processStop → asyncProfiler.stop [JNI] → 生成HTML → processStopMarkdown → ProfilerModel(stopped) → process.end
status: ShellLineHandler → ProcessImpl → ProfilerCommand.process → processStatus → asyncProfiler.getStatus → ProfilerModel → process.end
list:   ShellLineHandler → ProcessImpl → ProfilerCommand.process → processList → asyncProfiler.listEvents → ProfilerModel → process.end
```

### 12.4 ProfilerCommand 源码分析

> 源码位置：`arthas/core/src/main/java/com/taobao/arthas/core/command/monitor200/ProfilerCommand.java`

#### 12.4.1 命令参数定义

```java
@Name("profiler")
@Summary("Async Profiler. https://github.com/jvm-profiling-tools/async-profiler")
@Description(Constants.EXAMPLE +
        "  profiler start\n" +
        "  profiler stop --format flamegraph --file /tmp/flame.html\n" +
        "  profiler status\n" +
        "  profiler list\n" +
        Constants.WIKI + Constants.WIKI_HOME + "profiler")
public class ProfilerCommand extends AnnotatedCommand {

    private String action;
    private String event = "cpu";
    private String format;
    private String file;
    private long duration;
    private long interval = -1;
    private boolean threads;
    private String include;
    private String exclude;
    private String threadName;

    @Argument(index = 0, argName = "action")
    @Description("Action: start, stop, status, list")
    public void setAction(String action) {
        this.action = action;
    }

    @Option(longName = "event")
    @Description("Profiling event: cpu, alloc, lock, cache-misses, wall, itimer")
    public void setEvent(String event) {
        this.event = event;
    }

    @Option(longName = "format")
    @Description("Output format: flamegraph, tree, flat, collapsed, jfr")
    public void setFormat(String format) {
        this.format = format;
    }

    @Option(longName = "file")
    @Description("Output file path")
    public void setFile(String file) {
        this.file = file;
    }

    @Option(longName = "duration")
    @Description("Auto stop after N seconds")
    public void setDuration(long duration) {
        this.duration = duration;
    }

    @Option(shortName = "i", longName = "interval")
    @Description("Sampling interval in nanoseconds")
    public void setInterval(long interval) {
        this.interval = interval;
    }

    @Option(shortName = "t", longName = "threads")
    @Description("Profile threads separately")
    public void setThreads(boolean threads) {
        this.threads = threads;
    }

    @Option(shortName = "I", longName = "include")
    @Description("Include pattern for stack frames")
    public void setInclude(String include) {
        this.include = include;
    }

    @Option(shortName = "X", longName = "exclude")
    public void setExclude(String exclude) {
        this.exclude = exclude;
    }
}
```

参数说明：

- `action`：位置参数（第一个参数），不是 `--` 开头的选项。用户直接输入 `profiler start` 或 `profiler stop`。
- `event`：默认值 `"cpu"`，用户不指定时默认 CPU 采样。
- `interval`：默认 -1，表示使用 async-profiler 的默认间隔（CPU 事件默认 10ms = 10000000ns）。
- `format`：默认 null，会使用 async-profiler 默认的 flamegraph 格式。

#### 12.4.2 process() 入口与 action 分发

```java
@Override
public void process(CommandProcess process) {
    if (action == null) {
        process.end(-1, "Please specify action: start, stop, status, list");
        return;
    }

    try {
        switch (action.toLowerCase()) {
            case "start":
                processStart(process);
                break;
            case "stop":
                processStop(process);
                break;
            case "status":
                processStatus(process);
                break;
            case "list":
                processList(process);
                break;
            default:
                process.end(-1, "Unsupported action: " + action 
                    + ". Supported: start, stop, status, list");
        }
    } catch (Throwable e) {
        process.end(-1, "Profiler command error: " + e.getMessage());
    }
}
```

action 分发逻辑简单直接：四个 action 对应四个处理方法。异常兜底捕获所有 Throwable（包括 native 层的 UnsatisfiedLinkError）。

#### 12.4.3 processStart() 详解

```java
private void processStart(CommandProcess process) {
    // 1. check if already running
    if (asyncProfiler.getStatus() == ProfilerStatus.RUNNING) {
        process.end(-1, "Profiler is already running. Use 'profiler stop' first.");
        return;
    }

    // 2. build arguments string for async-profiler
    String args = executeArgs();

    // 3. execute start via JNI
    try {
        asyncProfiler.execute(args);
    } catch (Exception e) {
        process.end(-1, "Profiler start failed: " + e.getMessage());
        return;
    }

    // 4. schedule auto-stop if duration specified
    if (duration > 0) {
        scheduleAutoStop(process, duration);
    }

    // 5. build result model
    ProfilerModel result = new ProfilerModel();
    result.setAction("start");
    result.setEvent(event);
    result.setStatus("running");
    if (file != null) {
        result.setFile(file);
    }
    if (duration > 0) {
        result.setDuration(duration);
        result.setMessage("Profiler will auto stop after " + duration + " seconds.");
    }

    process.appendResult(result);
    process.end();
}
```

逐段解释：

1. **状态检查**：`asyncProfiler.getStatus()` 返回当前 profiler 状态（`RUNNING` / `STOPPED`）。async-profiler 在同一个 JVM 进程中只能运行一个 profiling session。如果已经在运行，拒绝启动。

2. **参数构建**：`executeArgs()` 将所有 CLI 参数拼接为 async-profiler 可识别的命令字符串。

3. **JNI 启动**：`asyncProfiler.execute(args)` 通过 JNI 调用 native 层的 `ProfilerStart` 函数。这个调用是非阻塞的——启动后立即返回，profiler 在后台通过信号机制持续采样。

4. **定时停止**：如果 `--duration` 参数指定了时长，创建延迟任务自动停止。

5. **结果返回**：返回当前状态（running）和配置信息（event、file、duration）。

#### 12.4.4 executeArgs() —— 参数字符串构建

```java
private String executeArgs() {
    StringBuilder args = new StringBuilder();
    
    // sub-command: start
    args.append("start");
    
    // event type
    if (event != null && !event.isEmpty()) {
        args.append(",event=").append(event);
    }
    
    // output file
    if (file != null && !file.isEmpty()) {
        args.append(",file=").append(file);
    }
    
    // output format
    if (format != null && !format.isEmpty()) {
        args.append(",").append(format);
    }
    
    // sampling interval
    if (interval > 0) {
        args.append(",interval=").append(interval);
    }
    
    // thread separation
    if (threads) {
        args.append(",threads");
    }
    
    // include pattern
    if (include != null && !include.isEmpty()) {
        args.append(",include=").append(include);
    }
    
    // exclude pattern
    if (exclude != null && !exclude.isEmpty()) {
        args.append(",exclude=").append(exclude);
    }
    
    return args.toString();
}
```

生成的参数字符串示例：
```
# profiler start --event cpu --format flamegraph --file /tmp/cpu.html
start,event=cpu,file=/tmp/cpu.html,flamegraph

# profiler start --event alloc -i 1000000 --threads
start,event=alloc,interval=1000000,threads

# profiler start --event cpu -I "com.example.*" -X "java.*"
start,event=cpu,include=com.example.*,exclude=java.*
```

这个字符串通过 JNI 传递给 async-profiler 的 `ProfilerExecute` 函数，native 层解析这些参数并配置采样。

#### 12.4.5 processStop() 详解

```java
private void processStop(CommandProcess process) {
    // 1. check if running
    if (asyncProfiler.getStatus() != ProfilerStatus.RUNNING) {
        process.end(-1, "Profiler is not running.");
        return;
    }

    // 2. stop profiling via JNI
    try {
        asyncProfiler.stop();
    } catch (Exception e) {
        process.end(-1, "Profiler stop failed: " + e.getMessage());
        return;
    }

    // 3. determine output file
    String resultFile = (file != null && !file.isEmpty()) ? file : "/tmp/arthas-output.html";

    // 4. build result model
    ProfilerModel result = new ProfilerModel();
    result.setAction("stop");
    result.setFile(resultFile);
    result.setStatus("stopped");

    // 5. convert to markdown if flamegraph format
    if (format == null || "flamegraph".equals(format)) {
        try {
            String markdown = processStopMarkdown(resultFile);
            result.setMarkdownResult(markdown);
        } catch (Exception e) {
            // markdown conversion is optional, don't fail
            logger.warn("Convert profiler output to markdown failed", e);
        }
    }

    process.appendResult(result);
    process.end();
}
```

逐段解释：

1. **状态检查**：确认 profiler 正在运行。如果未运行，无需 stop。

2. **JNI 停止**：`asyncProfiler.stop()` 调用 native 层的 `ProfilerStop` 函数。stop 时 async-profiler 会：
   - 停止信号采样
   - 汇总采样数据（调用栈计数表）
   - 按指定格式输出到文件
   - 释放采样资源

3. **文件确定**：如果用户在 start 时指定了 `--file`，结果写入该文件；否则默认 `/tmp/arthas-output.html`。

4. **Markdown 转换**：如果输出格式是 flamegraph（默认），尝试将 HTML 火焰图转换为 Markdown 表格格式，方便在终端中查看。

#### 12.4.6 processStopMarkdown() —— Markdown 转换

```java
private String processStopMarkdown(String flameFile) {
    // read the collapsed stacks from async-profiler
    // async-profiler can also output "collapsed" format alongside flamegraph
    String collapsedFile = flameFile.replace(".html", ".collapsed");
    File file = new File(collapsedFile);
    
    if (!file.exists()) {
        // try to read from the html file and extract stack data
        return "Flame graph saved to: " + flameFile;
    }

    // read collapsed stacks
    List<String> lines = FileUtils.readLines(file);
    
    // sort by sample count descending
    lines.sort((a, b) -> {
        int countA = Integer.parseInt(a.substring(a.lastIndexOf(' ') + 1));
        int countB = Integer.parseInt(b.substring(b.lastIndexOf(' ') + 1));
        return countB - countA;
    });

    // build markdown table
    StringBuilder sb = new StringBuilder();
    sb.append("| Stack | Samples |\n");
    sb.append("|-------|--------|\n");
    
    int maxRows = Math.min(lines.size(), 50); // top 50 stacks
    for (int i = 0; i < maxRows; i++) {
        String line = lines.get(i);
        int lastSpace = line.lastIndexOf(' ');
        String stack = line.substring(0, lastSpace);
        String count = line.substring(lastSpace + 1);
        // truncate long stacks
        if (stack.length() > 100) {
            stack = "..." + stack.substring(stack.length() - 97);
        }
        sb.append("| ").append(stack).append(" | ").append(count).append(" |\n");
    }
    
    return sb.toString();
}
```

这个方法尝试读取 collapsed 格式的栈数据文件，按采样次数降序排列，取前 50 个调用栈生成 Markdown 表格。这是火焰图的文本版补充——在终端无法打开 HTML 文件时，可以通过 Markdown 表格快速查看热点调用栈。

#### 12.4.7 processStatus() 和 processList()

```java
private void processStatus(CommandProcess process) {
    ProfilerStatus status = asyncProfiler.getStatus();
    ProfilerModel result = new ProfilerModel();
    result.setAction("status");
    result.setStatus(status == ProfilerStatus.RUNNING ? "running" : "stopped");
    
    if (status == ProfilerStatus.RUNNING) {
        // show current profiling config
        result.setEvent(event);
        result.setMessage("Profiler is running with event: " + event);
    }
    
    process.appendResult(result);
    process.end();
}

private void processList(CommandProcess process) {
    // list supported events
    String[] events = asyncProfiler.listEvents();
    ProfilerModel result = new ProfilerModel();
    result.setAction("list");
    result.setSupportedEvents(Arrays.asList(events));
    process.appendResult(result);
    process.end();
}
```

`processStatus` 显示当前 profiler 状态（running/stopped），`processList` 列出 async-profiler 支持的所有事件类型。

### 12.5 --duration 定时停止机制

```java
private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, 
    new NamedThreadFactory("arthas-profiler-timer", true));

private ScheduledFuture<?> stopFuture;

private void scheduleAutoStop(CommandProcess process, long durationSeconds) {
    stopFuture = scheduler.schedule(() -> {
        try {
            if (asyncProfiler.getStatus() == ProfilerStatus.RUNNING) {
                asyncProfiler.stop();
                
                // notify result
                ProfilerModel result = new ProfilerModel();
                result.setAction("stop");
                result.setStatus("auto-stopped");
                result.setMessage("Profiler auto stopped after " + durationSeconds + " seconds.");
                
                String resultFile = (file != null && !file.isEmpty()) ? file : "/tmp/arthas-output.html";
                result.setFile(resultFile);
                
                process.appendResult(result);
            }
        } catch (Exception e) {
            logger.error("Auto stop profiler failed", e);
        }
    }, durationSeconds, TimeUnit.SECONDS);
}
```

设计要点：

1. **守护线程池**：`newScheduledThreadPool(1, ...)` 创建单线程调度器，线程是守护线程（`true`），不影响 JVM 退出。

2. **幂等性检查**：延迟任务执行时再次检查 `getStatus() == RUNNING`，避免用户已经手动 stop 后重复 stop。

3. **结果通知**：自动 stop 后通过 `process.appendResult()` 附加结果。由于命令已经 `process.end()` 了（start 时就 end 了），这个结果会通过 session 的 result distributor 推送给消费者。在 telnet 模式下，用户会在终端看到 auto-stop 的消息。

4. **取消机制**：如果用户在 duration 到期前手动 `profiler stop`，理想情况下应取消 `stopFuture`。Arthas 在 `processStop()` 中可以检查并取消：

```java
private void processStop(CommandProcess process) {
    // cancel auto-stop if scheduled
    if (stopFuture != null) {
        stopFuture.cancel(false);
        stopFuture = null;
    }
    // ... then stop profiler
}
```

### 12.6 async-profiler JNI 集成详解

#### 12.6.1 native 库加载

Arthas 的 `arthas-vmtool` 模块负责加载 async-profiler 的 native 库：

```java
public class AsyncProfiler {
    private static volatile boolean loaded = false;
    private static AsyncProfiler instance;
    
    static {
        try {
            // determine platform
            String osName = System.getProperty("os.name").toLowerCase();
            String osArch = System.getProperty("os.arch").toLowerCase();
            
            String libName;
            if (osName.contains("linux")) {
                if (osArch.equals("aarch64")) {
                    libName = "libasyncProfiler-linux-arm64.so";
                } else {
                    libName = "libasyncProfiler-linux-x64.so";
                }
            } else if (osName.contains("mac")) {
                if (osArch.equals("aarch64")) {
                    libName = "libasyncProfiler-mac-arm64.dylib";
                } else {
                    libName = "libasyncProfiler-mac-x64.dylib";
                }
            } else {
                throw new UnsupportedOperationException("Unsupported platform: " + osName + " " + osArch);
            }
            
            // extract native lib from jar to temp file
            InputStream is = AsyncProfiler.class.getResourceAsStream("/" + libName);
            if (is == null) {
                throw new UnsatisfiedLinkError("Native library not found: " + libName);
            }
            File tempFile = File.createTempFile("asyncProfiler-", osName.contains("linux") ? ".so" : ".dylib");
            tempFile.deleteOnExit();
            FileUtils.copy(is, tempFile);
            
            // load native library
            System.load(tempFile.getAbsolutePath());
            loaded = true;
        } catch (Exception e) {
            // async-profiler not available, profiler command will show error
        }
    }
    
    public static AsyncProfiler getInstance() {
        if (!loaded) {
            throw new RuntimeException("async-profiler native library not loaded");
        }
        if (instance == null) {
            synchronized (AsyncProfiler.class) {
                if (instance == null) {
                    instance = new AsyncProfiler();
                }
            }
        }
        return instance;
    }
}
```

关键设计：

1. **平台检测**：通过 `os.name` 和 `os.arch` 系统属性选择对应的 native 库。支持 linux-x64, linux-arm64, mac-x64, mac-arm64 四个平台。

2. **库提取**：native 库打包在 Arthas 的 jar 中，运行时提取到临时文件再 `System.load()`。`deleteOnExit()` 确保 JVM 退出时清理临时文件。

3. **加载失败处理**：如果加载失败（如不支持的平台、缺少依赖库），`loaded` 保持 false，后续 `profiler` 命令会返回错误信息而不是崩溃。

4. **单例模式**：`AsyncProfiler` 是单例，整个 JVM 生命周期内只有一个 profiler 实例。这与 async-profiler 的设计一致——同一进程只能运行一个 profiling session。

#### 12.6.2 JNI 方法声明

```java
public class AsyncProfiler {
    // JNI native methods
    private native void execute0(String command) throws ProfilerException;
    private native void stop0() throws ProfilerException;
    private native String status0();
    private native String[] listEvents0();
    
    // Java wrappers
    public void execute(String args) throws ProfilerException {
        execute0(args);
    }
    
    public void stop() throws ProfilerException {
        stop0();
    }
    
    public ProfilerStatus getStatus() {
        String status = status0();
        return "running".equals(status) ? ProfilerStatus.RUNNING : ProfilerStatus.STOPPED;
    }
    
    public String[] listEvents() {
        return listEvents0();
    }
}
```

JNI 方法对应 async-profiler C API：
- `execute0` → `ProfilerExecute`：解析参数字符串并启动/停止/配置 profiler
- `stop0` → `ProfilerStop`：停止采样并输出结果
- `status0` → `ProfilerGetStatus`：查询当前状态
- `listEvents0` → `ProfilerListEvents`：列出支持的采样事件

#### 12.6.3 信号机制

async-profiler 使用操作系统信号进行采样，不同事件使用不同的信号：

| 事件 | 信号 | 说明 |
|------|------|------|
| `cpu` | SIGVTALRM | 虚拟 CPU 定时器，只在线程消耗 CPU 时触发 |
| `alloc` | SIGSEGV (hook) | 在内存分配路径 hook，每次分配触发采样 |
| `lock` | hook pthread | hook pthread_mutex_lock/unlock，记录竞争 |
| `wall` | SIGPROF | 真实时间定时器，无论线程状态都采样 |
| `cache-misses` | perf_event | 使用 Linux perf 子系统采集 cache miss |
| `itimer` | SIGALRM | 传统 itimer 定时器，兼容性好但精度低 |

工作原理（以 CPU 事件为例）：

1. **start**：设置 `SIGVTALRM` 信号处理器，配置定时器（间隔由 `-i` 参数指定，默认 10ms）。
2. **采样**：定时器到期时，JVM 线程收到 `SIGVTALRM` 信号，信号处理器中调用 `AsyncGetCallTrace`（JVM 内部 API）获取当前线程的调用栈。
3. **记录**：将调用栈 hash 后在 native 层的 hash map 中计数。
4. **stop**：停止定时器，遍历 hash map，按指定格式（flamegraph/collapsed/jfr）输出结果。

`AsyncGetCallTrace` 是 HotSpot JVM 的非标准内部 API（通过 `libjvm.so` 导出），能够安全地在信号处理器中获取 Java 调用栈。这是 async-profiler 能低开销工作的关键。

### 12.7 火焰图格式详解

#### 12.7.1 collapsed 格式（栈帧折叠）

原始数据格式，每行一个调用栈：

```
java.lang.Thread.run;java.util.concurrent.ThreadPoolExecutor$Worker.run;com.example.OrderService.createOrder;com.example.OrderService.validate;java.util.HashMap.get 340
java.lang.Thread.run;java.util.concurrent.ThreadPoolExecutor$Worker.run;com.example.OrderService.createOrder;com.example.OrderService.save;com.example.OrderRepository.insert;java.sql.PreparedStatement.execute 580
```

格式：`frame1;frame2;...;frameN count`

- 分号分隔调用栈帧，从底到顶（调用者在上，被调用者在下）
- 末尾空格后是采样次数
- 相同调用栈的计数在 stop 时汇总

#### 12.7.2 flamegraph HTML 格式

async-profiler 内嵌了简化版 d3-flame-graph，生成自包含 HTML 文件：

```html
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        /* 内联 CSS */
    </style>
</head>
<body>
    <script>
        // 内联 d3 库简化版
        // 内联采样数据（collapsed 格式）
        var stacks = "java.lang.Thread.run;...;java.util.HashMap.get 340\n...";
        // 渲染火焰图
    </script>
    <div id="chart"></div>
</body>
</html>
```

特性：
- **自包含**：所有 JS/CSS 内联，无外部依赖，可以离线打开
- **交互式**：鼠标悬停显示帧名称和采样占比，点击放大某个分支
- **搜索**：输入类名搜索，匹配的帧高亮显示
- **颜色**：默认按帧的类别着色（Java 帧=橙色，JVM 帧=绿色，Native 帧=黄色）

#### 12.7.3 jfr 格式

JFR（Java Flight Recorder）格式是 JDK 内置的二进制格式。优势：
- 可用 JDK Mission Control (JMC) 打开分析
- 包含更丰富的元数据（线程名、事件时间戳等）
- 支持事件级别的时间线分析

### 12.8 栈过滤机制

```bash
# 只看 com.example 包的栈帧
profiler start -I "com.example.*"

# 排除 java 和 sun 包
profiler start -X "java.*" -X "sun.*"

# 组合使用
profiler start --event cpu -I "com.example.*" -X "java.*"
```

过滤逻辑在 async-profiler native 层实现：

1. **include**：只保留匹配 include 模式的栈帧。如果一个调用栈中没有匹配 include 模式的帧，整个栈被丢弃。
2. **exclude**：移除匹配 exclude 模式的栈帧。移除后栈可能不连续，但上层帧仍然保留。
3. **组合**：先 include 后 exclude。先过滤只保留关心的帧，再排除噪声帧。

这在生产环境非常有用——火焰图可能包含数千个帧，通过过滤可以快速聚焦到业务代码。

### 12.9 完整链路：从 profiler start 到火焰图生成

```
profiler start --event cpu --format flamegraph --file /tmp/cpu-flame.html
  ┌──────────────────────────────────────────────────────────────────────┐
  │ ProfilerCommand.process()                                             │
  │   └─ processStart()                                                  │
  │       ├─ asyncProfiler.getStatus() == STOPPED ✓                       │
  │       ├─ executeArgs()                                                │
  │       │   → "start,event=cpu,file=/tmp/cpu-flame.html,flamegraph"    │
  │       │                                                               │
  │       ├─ asyncProfiler.execute(args)     [JNI]                       │
  │       │   └─ execute0("start,event=cpu,...")                          │
  │       │       └─ ProfilerExecute("start,event=cpu,...")   [C]        │
  │       │           ├─ 解析参数                                          │
  │       │           ├─ 安装 SIGVTALRM 信号处理器                         │
  │       │           ├─ 配置定时器（间隔 10ms）                            │
  │       │           └─ 返回                                             │
  │       │                                                               │
  │       │   (采样进行中... 每 10ms 一次)                                │
  │       │   ┌──────────────────────────────────┐                       │
  │       │   │ SIGVTALRM 信号到达                │                       │
  │       │   │   └─ 信号处理器                   │                       │
  │       │   │       └─ AsyncGetCallTrace()     │ [JVM内部API]          │
  │       │   │           → 获取当前调用栈        │                       │
  │       │   │       └─ hash(stack) → hash map  │                       │
  │       │   │           → count++              │                       │
  │       │   └──────────────────────────────────┘                       │
  │       │                                                               │
  │       └─ ProfilerModel(running, event=cpu) → process.end()            │
  └──────────────────────────────────────────────────────────────────────┘

profiler stop --duration 60  (或直接 profiler stop)
  ┌──────────────────────────────────────────────────────────────────────┐
  │ ProfilerCommand.process()                                             │
  │   └─ processStop()                                                   │
  │       ├─ asyncProfiler.getStatus() == RUNNING ✓                       │
  │       ├─ asyncProfiler.stop()              [JNI]                      │
  │       │   └─ stop0() → ProfilerStop()      [C]                       │
  │       │       ├─ 停止定时器                                            │
  │       │       ├─ 卸载信号处理器                                        │
  │       │       ├─ 遍历 hash map                                         │
  │       │       ├─ 按 flamegraph 格式生成 HTML                           │
  │       │       │   ├─ 嵌入内联 JS/CSS                                  │
  │       │       │   ├─ 嵌入 collapsed 栈数据                            │
  │       │       │   └─ 写入 /tmp/cpu-flame.html                         │
  │       │       └─ 释放采样资源                                          │
  │       │                                                               │
  │       ├─ processStopMarkdown("/tmp/cpu-flame.html")                   │
  │       │   → Markdown 表格 (top 50 调用栈)                             │
  │       │                                                               │
  │       └─ ProfilerModel(stopped, file=..., markdown=...) → process.end│
  └──────────────────────────────────────────────────────────────────────┘
```

### 12.10 --threads 按线程分离采样

```bash
profiler start --event cpu --threads
```

启用 `--threads` 后，async-profiler 在采样时记录线程信息，生成的火焰图中每个线程有独立的火焰图。这对于排查多线程性能问题很有用——可以看到每个线程的 CPU 消耗分布。

在 native 层，`--threads` 参数使 async-profiler 在 hash map 的 key 中加入线程 ID。stop 时按线程分组输出。

### 12.11 各事件类型的适用场景

| 事件 | 适用场景 | 开销 |
|------|---------|------|
| `cpu` | CPU 热点分析，找出消耗 CPU 最多的方法 | 低（~1-3%） |
| `alloc` | 内存分配热点，找出分配最多内存的方法 | 中（~5-10%） |
| `wall` | 延迟分析，找出等待时间最长的方法（含 IO 等待） | 低 |
| `lock` | 锁竞争分析，找出争用最严重的锁 | 中 |
| `cache-misses` | CPU 缓存命中率分析，微架构级优化 | 低（需 Linux perf） |
| `itimer` | 兼容模式，在不支持 SIGVTALRM 的平台上替代 cpu | 低 |

使用建议：
- **接口延迟升高**：先 `cpu` 排查 CPU 热点，再 `wall` 排查等待时间
- **内存泄漏/频繁 GC**：用 `alloc` 找出分配热点
- **线程阻塞/死锁排查**：用 `lock` 分析锁竞争
- **CPU 缓存优化**：用 `cache-misses` 分析缓存命中率

### 12.12 Q&A 设计问题分析

**Q1: async-profiler 的开销有多大？能在生产环境长期运行吗？**

A: async-profiler 基于信号采样，CPU 事件默认 10ms 间隔（100Hz），开销约 1-3%。可以在生产环境运行，但建议控制采样时长（60-120 秒足够），避免长时间运行产生过大的采样数据。`alloc` 事件开销更高（5-10%），需要谨慎。

**Q2: `profiler start` 后 JAR 包被替换了怎么办？**

A: async-profiler 的 native 库在 `start` 时已经加载到进程内存中，后续运行不依赖 jar 文件。但如果 JVM 重启，需要重新加载。Arthas 的 `arthas-vmtool` 模块在 JVM 启动时就加载了 native 库，不会因为 jar 替换而失效。

**Q3: 为什么火焰图中有些栈帧显示为 `unknown`？**

A: 信号到达时，如果线程正在执行 native 代码（JNI 调用、JVM 内部代码），`AsyncGetCallTrace` 可能无法获取 Java 调用栈，返回 unknown。常见于：
- 线程在 IO 系统调用中（如 socket read）
- 线程在 JNI 代码中
- GC 线程
- JIT 编译线程

这些 unknown 帧通常占比不大。如果占比很高，说明大量时间花在 native 代码中，可以考虑用 `wall` 事件（不依赖 `AsyncGetCallTrace`，使用 `AsyncGetStackTrace` 替代）。

**Q4: `--format jfr` 和 `--format flamegraph` 有什么区别？应该用哪个？**

A: flamegraph 是交互式 HTML，适合在浏览器中快速浏览，直观但分析能力有限。jfr 是 JDK 标准格式，可用 JMC 打开，支持时间线分析、事件详情查看、自动分析报告等，功能更强但需要额外工具。建议先用 flamegraph 快速定位，再用 jfr 深入分析。

**Q5: `-I "com.example.*" -X "java.*"` 的过滤逻辑是什么？如果栈中没有任何 com.example 帧会怎样？**

A: include 是"保留"逻辑——只有匹配 include 模式的帧会出现在结果中。如果一个调用栈中没有任何匹配 include 模式的帧，整个调用栈被丢弃，不会出现在火焰图中。exclude 是"移除"逻辑——匹配 exclude 模式的帧从栈中移除，但栈的其他帧保留。

组合使用时：先 include 过滤只保留 `com.example.*` 的帧，再 exclude 移除 `java.*` 的帧。实际效果是只看 com.example 包内的调用链路，过滤掉所有 java 标准库的帧。

### 12.13 场景总结

本场景深入分析了 Arthas 与 async-profiler 的集成机制：

1. **ProfilerCommand** 作为命令入口，通过 `action` 参数分发给 `processStart/processStop/processStatus/processList` 四个处理方法。

2. **executeArgs()** 将 CLI 参数拼接为 async-profiler 可识别的命令字符串（如 `start,event=cpu,file=/tmp/flame.html,flamegraph`），通过 JNI 传递给 native 层。

3. **async-profiler JNI 集成**：Arthas 的 `arthas-vmtool` 模块在 JVM 启动时加载平台对应的 native 库，通过 JNI 调用 async-profiler C API（ProfilerExecute/ProfilerStop）。native 库从 jar 中提取到临时文件后 `System.load()` 加载。

4. **信号采样机制**：不同事件使用不同信号——CPU 用 SIGVTALRM，wall 用 SIGPROF，alloc 用 hook malloc。信号处理器中调用 `AsyncGetCallTrace` 获取 Java 调用栈，在 native hash map 中计数。

5. **火焰图输出**：stop 时遍历采样数据，按指定格式生成输出。flamegraph 格式生成自包含 HTML（内联 JS/CSS/数据），collapsed 格式生成单行折叠文本，jfr 格式生成 JDK 标准二进制文件。

6. **栈过滤**：include（`-I`）保留匹配帧，exclude（`-X`）移除匹配帧，帮助在生产环境中聚焦业务代码。

7. **定时停止**：`--duration` 参数通过 `ScheduledExecutorService` 延迟执行 stop，避免用户忘记停止 profiler 导致资源持续消耗。

async-profiler 的核心价值在于低开销、精准采样——基于信号的采样方式开销通常在 1-3%，远低于基于 BCI 的 trace/watch 等命令，适合在生产环境长时间运行进行性能画像。
