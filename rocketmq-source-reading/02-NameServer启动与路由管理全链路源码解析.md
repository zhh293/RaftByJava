# NameServer启动与路由管理全链路源码解析

> 本文基于 Apache RocketMQ 5.x 源码，对 NameServer 模块进行逐行级别的深度剖析。
> 从启动入口 `NamesrvStartup.main()` 出发，贯穿控制器初始化、路由数据结构、Broker 注册/注销、
> 路由发现、心跳检测、请求处理器等全链路，力求做到**每一个关键方法都有源码佐证，每一个设计决策都有原理解释**。
>
> 阅读本文前，建议先对 RocketMQ 的整体架构有基本了解：Producer、Consumer、Broker、NameServer 四大角色的协作关系。

---

## 目录

- [一、全局调用链总览](#一全局调用链总览)
- [二、NameServer启动流程](#二nameserver启动流程)
- [三、NamesrvController初始化详解](#三namesrvcontroller初始化详解)
- [四、RouteInfoManager核心数据结构](#四routeinfomanager核心数据结构)
- [五、Broker注册全链路](#五broker注册全链路-registerbroker)
- [六、路由发现全链路](#六路由发现全链路-pickuptopicroutedata)
- [七、Broker注销全链路](#七broker注销全链路)
- [八、心跳与存活检测](#八心跳与存活检测)
- [九、DefaultRequestProcessor详解](#九defaultrequestprocessor详解)
- [十、ClientRequestProcessor详解](#十clientrequestprocessor详解)
- [十一、KVConfigManager详解](#十一kvconfigmanager详解)
- [十二、知识点总结](#十二知识点总结)

---

## 一、全局调用链总览

在深入每个类的源码之前，我们先用一张全局调用链图来建立对 NameServer 整体运作的宏观认知。
NameServer 的生命周期可以划分为三个大阶段：**启动阶段**、**运行阶段**、**关闭阶段**。

### 1.1 启动阶段调用链

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        NameServer 启动阶段                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  NamesrvStartup.main(args)                                                  │
│    │                                                                        │
│    └──► main0(args)                                                         │
│           │                                                                 │
│           ├──► parseCommandlineAndConfigFile(args)                           │
│           │      │                                                          │
│           │      ├── 解析命令行参数 (-c configFile, -p printConfig)           │
│           │      ├── 创建 NamesrvConfig (业务配置)                           │
│           │      ├── 创建 NettyServerConfig (服务端网络配置, port=9876)       │
│           │      ├── 创建 NettyClientConfig (客户端网络配置)                  │
│           │      ├── 从 -c 指定的文件加载属性，填充到三个 Config 对象          │
│           │      └── 返回 NamesrvConfig                                     │
│           │                                                                 │
│           └──► createAndStartNamesrvController()                            │
│                  │                                                          │
│                  ├──► new NamesrvController(namesrvConfig,                   │
│                  │                         nettyServerConfig,               │
│                  │                         nettyClientConfig)               │
│                  │      │                                                   │
│                  │      ├── this.kvConfigManager = new KVConfigManager()     │
│                  │      ├── this.brokerHousekeepingService =                 │
│                  │      │       new BrokerHousekeepingService()              │
│                  │      ├── this.routeInfoManager = new RouteInfoManager()   │
│                  │      └── this.configuration = new Configuration()         │
│                  │                                                          │
│                  ├──► controller.initialize()                               │
│                  │      │                                                   │
│                  │      ├── 1. loadConfig()                                 │
│                  │      │      └── kvConfigManager.load()                   │
│                  │      │                                                   │
│                  │      ├── 2. initiateNetworkComponents()                  │
│                  │      │      └── new NettyRemotingServer(                 │
│                  │      │              nettyServerConfig,                   │
│                  │      │              brokerHousekeepingService)           │
│                  │      │                                                   │
│                  │      ├── 3. initiateThreadExecutors()                    │
│                  │      │      ├── defaultExecutor (线程池)                  │
│                  │      │      └── clientRequestExecutor (线程池)            │
│                  │      │                                                   │
│                  │      ├── 4. registerProcessor()                          │
│                  │      │      ├── ClientRequestProcessor                   │
│                  │      │      │   → GET_ROUTEINFO_BY_TOPIC                 │
│                  │      │      └── DefaultRequestProcessor                  │
│                  │      │          → 所有其他请求码                           │
│                  │      │                                                   │
│                  │      ├── 5. startScheduleService()                       │
│                  │      │      ├── scanNotActiveBroker (每10s)               │
│                  │      │      ├── printAllPeriodically (每10min)            │
│                  │      │      └── printWaterMark (每1s)                     │
│                  │      │                                                   │
│                  │      ├── 6. initiateSslContext()                         │
│                  │      └── 7. initiateRpcHooks()                          │
│                  │             └── ZoneRouteRPCHook                         │
│                  │                                                          │
│                  ├──► 注册 JVM ShutdownHook                                 │
│                  │      └── controller.shutdown()                           │
│                  │                                                          │
│                  └──► controller.start()                                    │
│                         ├── remotingServer.start()                          │
│                         ├── fileWatchService.start() (如启用TLS)             │
│                         └── routeInfoManager.start()                       │
│                                └── batchUnregistrationService.start()      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 运行阶段调用链

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        NameServer 运行阶段                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─── Broker 侧触发 ──────────────────────────────────────────────┐         │
│  │                                                                │         │
│  │  Broker 启动/定时30s                                            │         │
│  │    └──► REGISTER_BROKER 请求                                   │         │
│  │          └──► DefaultRequestProcessor.registerBroker()         │         │
│  │                └──► RouteInfoManager.registerBroker()          │         │
│  │                      ├── 更新 clusterAddrTable                 │         │
│  │                      ├── 更新 brokerAddrTable                  │         │
│  │                      ├── 更新 topicQueueTable                  │         │
│  │                      ├── 更新 brokerLiveTable                  │         │
│  │                      └── 更新 filterServerTable                │         │
│  │                                                                │         │
│  │  Broker 心跳 (10s-30s)                                         │         │
│  │    └──► BROKER_HEARTBEAT 请求                                  │         │
│  │          └──► DefaultRequestProcessor.brokerHeartbeat()        │         │
│  │                └──► RouteInfoManager.updateBrokerInfoUpdateTs() │        │
│  │                                                                │         │
│  │  Broker 版本查询 (30s)                                          │         │
│  │    └──► QUERY_DATA_VERSION 请求                                │         │
│  │          └──► DefaultRequestProcessor.queryBrokerTopicConfig() │         │
│  │                └──► 比较 DataVersion → 决定是否全量注册          │         │
│  │                                                                │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                             │
│  ┌─── Client 侧触发 ─────────────────────────────────────────────┐         │
│  │                                                                │         │
│  │  Producer/Consumer 启动 & 定时30s                               │         │
│  │    └──► GET_ROUTEINFO_BY_TOPIC 请求                            │         │
│  │          └──► ClientRequestProcessor.getRouteInfoByTopic()     │         │
│  │                └──► RouteInfoManager.pickupTopicRouteData()    │         │
│  │                      ├── 查询 topicQueueTable                  │         │
│  │                      ├── 查询 brokerAddrTable                  │         │
│  │                      ├── 查询 filterServerTable                │         │
│  │                      └── 组装 TopicRouteData 返回               │         │
│  │                                                                │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                             │
│  ┌─── 定时任务触发 ──────────────────────────────────────────────┐          │
│  │                                                                │         │
│  │  每 scanNotActiveBrokerInterval (默认5s)                        │         │
│  │    └──► RouteInfoManager.scanNotActiveBroker()                 │         │
│  │          ├── 遍历 brokerLiveTable                              │         │
│  │          ├── 检查 lastUpdateTimestamp + BROKER_CHANNEL_EXPIRED  │         │
│  │          │   (默认 120000ms = 2分钟)                            │         │
│  │          ├── 超时则 onChannelDestroy()                         │         │
│  │          │     ├── 清理 brokerLiveTable                        │         │
│  │          │     ├── 清理 filterServerTable                      │         │
│  │          │     ├── 清理 brokerAddrTable                        │         │
│  │          │     ├── 清理 clusterAddrTable                       │         │
│  │          │     └── 清理 topicQueueTable                        │         │
│  │          └── 关闭 Channel                                      │         │
│  │                                                                │         │
│  │  每 10 分钟                                                     │         │
│  │    └──► RouteInfoManager.printAllPeriodically()                │         │
│  │                                                                │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                             │
│  ┌─── 网络事件触发 ──────────────────────────────────────────────┐          │
│  │                                                                │         │
│  │  Broker 连接断开 / 异常 / 空闲超时                               │         │
│  │    └──► BrokerHousekeepingService                              │         │
│  │          ├── onChannelClose()                                  │         │
│  │          ├── onChannelException()                              │         │
│  │          └── onChannelIdle()                                   │         │
│  │          均触发 → RouteInfoManager.onChannelDestroy()           │         │
│  │                                                                │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 关闭阶段调用链

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        NameServer 关闭阶段                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  JVM ShutdownHook 或手动调用                                                │
│    └──► NamesrvController.shutdown()                                        │
│           ├── remotingServer.shutdown()                                     │
│           ├── remotingClient.shutdown() (如果有)                             │
│           ├── defaultExecutor.shutdown()                                    │
│           ├── clientRequestExecutor.shutdown()                              │
│           ├── scheduledExecutorService.shutdown()                           │
│           ├── routeInfoManager.shutdown()                                   │
│           │     └── batchUnregistrationService.shutdown()                   │
│           └── fileWatchService.shutdown() (如果有)                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.4 模块依赖关系

```
┌────────────────────────────────────────────────────────────────────┐
│                      NamesrvController                             │
│  (NameServer 的中枢控制器，持有所有核心组件的引用)                     │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ┌──────────────────┐    ┌───────────────────────┐                 │
│  │  NamesrvConfig   │    │  NettyServerConfig     │                │
│  │  (业务配置)       │    │  (Netty服务端配置)      │                │
│  └──────────────────┘    └───────────────────────┘                 │
│                                                                    │
│  ┌──────────────────┐    ┌───────────────────────┐                 │
│  │ NettyClientConfig│    │  Configuration         │                │
│  │ (Netty客户端配置) │    │  (统一配置管理)         │                │
│  └──────────────────┘    └───────────────────────┘                 │
│                                                                    │
│  ┌──────────────────────────────────────────────┐                  │
│  │          RouteInfoManager                     │                 │
│  │  (路由信息管理器 - NameServer 核心)             │                 │
│  │  ┌─────────────────────────────────────────┐  │                 │
│  │  │  topicQueueTable                        │  │                 │
│  │  │  brokerAddrTable                        │  │                 │
│  │  │  clusterAddrTable                       │  │                 │
│  │  │  brokerLiveTable                        │  │                 │
│  │  │  filterServerTable                      │  │                 │
│  │  │  topicQueueMappingInfoTable             │  │                 │
│  │  └─────────────────────────────────────────┘  │                 │
│  │  ┌─────────────────────────────────────────┐  │                 │
│  │  │  BatchUnregistrationService             │  │                 │
│  │  │  (批量注销服务)                           │  │                 │
│  │  └─────────────────────────────────────────┘  │                 │
│  └──────────────────────────────────────────────┘                  │
│                                                                    │
│  ┌──────────────────┐    ┌───────────────────────┐                 │
│  │ KVConfigManager  │    │ BrokerHousekeeping     │                │
│  │ (KV配置管理)      │    │ Service                │                │
│  │                  │    │ (Broker连接管家)         │                │
│  └──────────────────┘    └───────────────────────┘                 │
│                                                                    │
│  ┌──────────────────────────────────────────────┐                  │
│  │        NettyRemotingServer                    │                 │
│  │  (Netty 网络通信服务端)                         │                 │
│  │  ┌─────────────────────────────────────────┐  │                 │
│  │  │  DefaultRequestProcessor (默认处理器)     │  │                 │
│  │  │  ClientRequestProcessor  (客户端处理器)   │  │                 │
│  │  └─────────────────────────────────────────┘  │                 │
│  └──────────────────────────────────────────────┘                  │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

以上就是 NameServer 的全局视图。接下来我们逐个模块、逐个方法地进行源码级解析。

---

## 二、NameServer启动流程

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/NamesrvStartup.java`（约243行）

NameServer 的启动入口类是 `NamesrvStartup`，这是一个标准的 Java main class。
它的职责非常清晰：解析命令行参数与配置文件 → 创建并初始化 NamesrvController → 注册关闭钩子 → 启动服务。

### 2.1 main() 方法

```java
// NamesrvStartup.java

public class NamesrvStartup {

    private final static Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);

    private static Properties properties = null;
    private static NamesrvConfig namesrvConfig = null;
    private static NettyServerConfig nettyServerConfig = null;
    private static NettyClientConfig nettyClientConfig = null;

    public static void main(String[] args) {
        main0(args);
        controllerManagerMain();  // 用于 Controller 模式 (5.x 新增)
    }
}
```

`main()` 方法极为简洁，只做两件事：
1. 调用 `main0(args)` 完成 NameServer 的核心启动流程
2. 调用 `controllerManagerMain()` 启动 Controller 管理器（这是 RocketMQ 5.x 引入的新特性，
   用于支持 Controller 模式下的主备自动切换，本文暂不展开）

**设计要点**：将真正的启动逻辑放在 `main0()` 而不是直接放在 `main()` 中，这是为了让
`main()` 可以作为一个组合入口，按需叠加其他启动逻辑（如 controllerManagerMain）。
这种模式在 RocketMQ 的 Broker 启动类中也有类似体现。

### 2.2 main0() 方法

```java
// NamesrvStartup.java

public static NamesrvController main0(String[] args) {
    try {
        parseCommandlineAndConfigFile(args);
        NamesrvController controller = createAndStartNamesrvController();
        return controller;
    } catch (Throwable e) {
        e.printStackTrace();
        System.exit(-1);
    }
    return null;
}
```

`main0()` 将启动流程拆分为两个核心步骤：

1. **`parseCommandlineAndConfigFile(args)`**：解析命令行参数和配置文件，初始化三大配置对象
2. **`createAndStartNamesrvController()`**：创建控制器实例，完成初始化，注册关闭钩子，启动服务

异常处理策略非常"暴力"——任何未捕获的异常都会导致 `System.exit(-1)` 直接退出 JVM。
这是合理的设计：NameServer 作为基础设施组件，启动失败后没有必要继续运行，
快速失败（Fail Fast）是最正确的策略。

### 2.3 parseCommandlineAndConfigFile() 方法

```java
// NamesrvStartup.java

public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
    // ========== 第一步：设置 RocketMQ 版本号到系统属性 ==========
    System.setProperty(RemotingCommand.REMOTING_VERSION_KEY,
        Integer.toString(MQVersion.CURRENT_VERSION));

    // ========== 第二步：构建命令行选项 ==========
    Options options = ServerUtil.buildCommandlineOptions(new Options());
    CommandLine commandLine = ServerUtil.parseCmdLine(
        "mqnamesrv", args, buildCommandlineOptions(options),
        new DefaultParser());
    if (null == commandLine) {
        System.exit(-1);
        return;
    }

    // ========== 第三步：创建三大配置对象 ==========
    namesrvConfig = new NamesrvConfig();
    nettyServerConfig = new NettyServerConfig();
    nettyClientConfig = new NettyClientConfig();

    // 设置 NameServer 默认监听端口为 9876
    nettyServerConfig.setListenPort(9876);

    // ========== 第四步：如果指定了 -c 配置文件，从文件加载属性 ==========
    if (commandLine.hasOption('c')) {
        String file = commandLine.getOptionValue('c');
        if (file != null) {
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            properties = new Properties();
            properties.load(in);

            // 通过反射将 properties 中的值填充到配置对象
            MixAll.properties2Object(properties, namesrvConfig);
            MixAll.properties2Object(properties, nettyServerConfig);
            MixAll.properties2Object(properties, nettyClientConfig);

            // 记录配置文件路径，后续可用于运维排查
            namesrvConfig.setConfigStorePath(file);
            in.close();
        }
    }

    // ========== 第五步：如果指定了 -p，打印配置并退出 ==========
    if (commandLine.hasOption('p')) {
        InternalLogger console = InternalLoggerFactory.getLogger(
            LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);
        MixAll.printObjectProperties(console, namesrvConfig);
        MixAll.printObjectProperties(console, nettyServerConfig);
        MixAll.printObjectProperties(console, nettyClientConfig);
        System.exit(0);
    }

    // ========== 第六步：命令行参数覆盖配置文件 ==========
    MixAll.properties2Object(
        ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

    // ========== 第七步：校验 ROCKETMQ_HOME ==========
    if (null == namesrvConfig.getRocketmqHome()) {
        System.out.printf("Please set the %s variable in your " +
            "environment to match the location of the RocketMQ " +
            "installation%n", MixAll.ROCKETMQ_HOME_ENV);
        System.exit(-2);
    }

    // ========== 第八步：初始化日志框架 ==========
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(lc);
    lc.reset();
    configurator.doConfigure(
        namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");
    log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    // ========== 第九步：打印配置信息到日志 ==========
    MixAll.printObjectProperties(log, namesrvConfig);
    MixAll.printObjectProperties(log, nettyServerConfig);
}
```

我们逐步拆解这个方法的九个关键步骤：

#### 步骤一：设置版本号

```java
System.setProperty(RemotingCommand.REMOTING_VERSION_KEY,
    Integer.toString(MQVersion.CURRENT_VERSION));
```

这一行将 RocketMQ 的版本号写入系统属性。当 NameServer 通过网络发送 RemotingCommand 时，
版本号会被写入协议头（Header），用于客户端和 Broker 的版本兼容性判断。

#### 步骤二：构建和解析命令行选项

RocketMQ 使用 Apache Commons CLI 库来解析命令行参数。`buildCommandlineOptions()` 方法
定义了 NameServer 支持的命令行选项：

```java
// NamesrvStartup.java

public static Options buildCommandlineOptions(final Options options) {
    // -c <configFile>: 指定配置文件路径
    Option opt = new Option("c", "configFile", true,
        "Name server config properties file");
    opt.setRequired(false);
    options.addOption(opt);

    // -p: 打印所有配置项并退出（用于运维检查）
    opt = new Option("p", "printConfigItem", false,
        "Print all config items");
    opt.setRequired(false);
    options.addOption(opt);

    return options;
}
```

NameServer 的命令行参数很简洁，只有两个：
- `-c <file>`：指定配置文件路径
- `-p`：打印所有配置项然后退出

#### 步骤三：创建三大配置对象

```java
namesrvConfig = new NamesrvConfig();
nettyServerConfig = new NettyServerConfig();
nettyClientConfig = new NettyClientConfig();
nettyServerConfig.setListenPort(9876);
```

NameServer 的配置由三个对象共同承载：

| 配置类 | 职责 | 关键配置项 |
|--------|------|-----------|
| `NamesrvConfig` | NameServer 业务相关配置 | `rocketmqHome`, `kvConfigPath`, `configStorePath`, `scanNotActiveBrokerInterval`, `unRegisterBrokerQueueCapacity` |
| `NettyServerConfig` | Netty 服务端网络配置 | `listenPort` (默认9876), `serverWorkerThreads`, `serverSelectorThreads`, `serverChannelMaxIdleTimeSeconds` |
| `NettyClientConfig` | Netty 客户端网络配置 | `clientWorkerThreads`, `connectTimeoutMillis`, `channelNotActiveInterval` |

**注意 9876 端口**：这是 RocketMQ NameServer 的"约定俗成"端口。Broker 和客户端默认
都会连接 9876 端口。这个端口号在 NameServer 启动时通过 `nettyServerConfig.setListenPort(9876)` 
硬编码设置，但可以通过配置文件或命令行参数覆盖。

#### 步骤四：配置文件加载

```java
if (commandLine.hasOption('c')) {
    String file = commandLine.getOptionValue('c');
    if (file != null) {
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        properties = new Properties();
        properties.load(in);
        MixAll.properties2Object(properties, namesrvConfig);
        MixAll.properties2Object(properties, nettyServerConfig);
        MixAll.properties2Object(properties, nettyClientConfig);
        namesrvConfig.setConfigStorePath(file);
        in.close();
    }
}
```

`MixAll.properties2Object()` 是一个基于 Java 反射的工具方法，它会遍历 Properties 中的
每个 key-value 对，尝试在目标对象上找到对应的 setter 方法并调用。例如，配置文件中的
`listenPort=9877` 会通过反射调用 `nettyServerConfig.setListenPort(9877)`。

**配置优先级**：命令行参数 > 配置文件 > 默认值。这是通过步骤六实现的——
在加载配置文件之后，再用命令行参数覆盖。

#### 步骤五：打印配置模式

```java
if (commandLine.hasOption('p')) {
    MixAll.printObjectProperties(console, namesrvConfig);
    MixAll.printObjectProperties(console, nettyServerConfig);
    MixAll.printObjectProperties(console, nettyClientConfig);
    System.exit(0);
}
```

`-p` 参数是一个运维友好的特性：运维人员可以通过 `mqnamesrv -c config.properties -p`
来验证配置是否正确加载，而不需要真正启动 NameServer。

#### 步骤七：ROCKETMQ_HOME 校验

```java
if (null == namesrvConfig.getRocketmqHome()) {
    System.out.printf("Please set the %s variable...", MixAll.ROCKETMQ_HOME_ENV);
    System.exit(-2);
}
```

`ROCKETMQ_HOME` 环境变量指向 RocketMQ 的安装目录。NameServer 需要它来定位日志配置文件
（`conf/logback_namesrv.xml`）和其他资源文件。如果没有设置，直接退出。

### 2.4 createAndStartNamesrvController() 方法

```java
// NamesrvStartup.java

public static NamesrvController createAndStartNamesrvController() throws Exception {
    // ========== 第一步：创建 NamesrvController 实例 ==========
    NamesrvController controller = new NamesrvController(
        namesrvConfig, nettyServerConfig, nettyClientConfig);

    // ========== 第二步：初始化 ==========
    boolean initResult = controller.initialize();
    if (!initResult) {
        controller.shutdown();
        System.exit(-3);
    }

    // ========== 第三步：注册 JVM 关闭钩子 ==========
    Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(
        log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }
    ));

    // ========== 第四步：启动 ==========
    controller.start();
    return controller;
}
```

这个方法的四个步骤清晰地对应了一个服务的标准生命周期：
**创建 → 初始化 → 注册关闭钩子 → 启动**。

#### JVM ShutdownHook 的重要性

```java
Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(
    log, (Callable<Void>) () -> {
        controller.shutdown();
        return null;
    }
));
```

ShutdownHook 确保在 JVM 正常退出时（如 `kill -15`、`Ctrl+C`、`System.exit()`），
NameServer 能够优雅地释放资源：关闭 Netty 服务端、停止线程池、持久化数据等。

**注意**：`kill -9` (SIGKILL) 不会触发 ShutdownHook。在生产环境中，应该使用
`kill -15`（SIGTERM）来停止 NameServer。

#### 初始化失败的处理

```java
boolean initResult = controller.initialize();
if (!initResult) {
    controller.shutdown();
    System.exit(-3);
}
```

如果初始化失败（比如端口被占用、配置文件解析错误等），先调用 `shutdown()` 清理
已经分配的资源，然后以退出码 `-3` 退出 JVM。这体现了"即使失败也要清理资源"的良好实践。

### 2.5 启动流程时序图

用一个时序图来总结整个启动过程：

```
 JVM              NamesrvStartup        NamesrvController       NettyServer       RouteInfoManager
  │                    │                       │                    │                    │
  │  main(args)        │                       │                    │                    │
  │───────────────────>│                       │                    │                    │
  │                    │                       │                    │                    │
  │                    │  parseCommandline      │                    │                    │
  │                    │  AndConfigFile(args)   │                    │                    │
  │                    │──────────┐             │                    │                    │
  │                    │          │ 创建Config  │                    │                    │
  │                    │<─────────┘             │                    │                    │
  │                    │                       │                    │                    │
  │                    │  new NamesrvController │                    │                    │
  │                    │──────────────────────>│                    │                    │
  │                    │                       │  new RouteInfoMgr  │                    │
  │                    │                       │───────────────────────────────────────>│
  │                    │                       │                    │                    │
  │                    │  initialize()          │                    │                    │
  │                    │──────────────────────>│                    │                    │
  │                    │                       │  loadConfig()      │                    │
  │                    │                       │──────┐             │                    │
  │                    │                       │<─────┘             │                    │
  │                    │                       │                    │                    │
  │                    │                       │  new NettyServer   │                    │
  │                    │                       │──────────────────>│                    │
  │                    │                       │                    │                    │
  │                    │                       │  registerProcessor │                    │
  │                    │                       │──────────────────>│                    │
  │                    │                       │                    │                    │
  │                    │                       │  startSchedule     │                    │
  │                    │                       │──────┐             │                    │
  │                    │                       │<─────┘             │                    │
  │                    │                       │                    │                    │
  │                    │  addShutdownHook       │                    │                    │
  │                    │──────┐                │                    │                    │
  │                    │<─────┘                │                    │                    │
  │                    │                       │                    │                    │
  │                    │  start()               │                    │                    │
  │                    │──────────────────────>│                    │                    │
  │                    │                       │  server.start()    │                    │
  │                    │                       │──────────────────>│                    │
  │                    │                       │                    │  bind(9876)        │
  │                    │                       │                    │──────┐             │
  │                    │                       │                    │<─────┘             │
  │                    │                       │  routeInfoMgr      │                    │
  │                    │                       │  .start()           │                    │
  │                    │                       │───────────────────────────────────────>│
  │                    │                       │                    │                    │
  │                    │  return controller     │                    │                    │
  │                    │<─────────────────────│                    │                    │
  │                    │                       │                    │                    │
  │  NameServer 就绪    │                       │                    │                    │
  │<───────────────────│                       │                    │                    │
```

---

## 三、NamesrvController初始化详解

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/NamesrvController.java`（约285行）

`NamesrvController` 是 NameServer 的中枢控制器，相当于 Spring 应用中的 ApplicationContext。
它持有所有核心组件的引用，负责组件的创建、初始化、启动和关闭。

### 3.1 类定义与成员变量

```java
// NamesrvController.java

public class NamesrvController {

    private static final Logger log = LoggerFactory.getLogger(
        LoggerName.NAMESRV_LOGGER_NAME);

    // ========== 配置相关 ==========
    private final NamesrvConfig namesrvConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    // ========== 核心组件 ==========
    private final RouteInfoManager routeInfoManager;
    private final KVConfigManager kvConfigManager;
    private final BrokerHousekeepingService brokerHousekeepingService;
    private final Configuration configuration;

    // ========== 网络通信 ==========
    private RemotingServer remotingServer;
    private RemotingClient remotingClient;  // 5.x 新增，用于主动连接

    // ========== 线程池 ==========
    private ExecutorService defaultExecutor;
    private ExecutorService clientRequestExecutor;
    private BlockingQueue<Runnable> defaultThreadPoolQueue;
    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    // ========== 定时任务 ==========
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledExecutorService scanExecutorService;

    // ========== TLS/SSL ==========
    private FileWatchService fileWatchService;

    // ========== 启动时间 ==========
    private volatile long startupTimeMillis;
}
```

各成员变量的职责：

| 变量 | 类型 | 职责 |
|------|------|------|
| `namesrvConfig` | NamesrvConfig | NameServer 业务配置 |
| `nettyServerConfig` | NettyServerConfig | Netty 服务端配置 |
| `nettyClientConfig` | NettyClientConfig | Netty 客户端配置 |
| `routeInfoManager` | RouteInfoManager | **路由信息管理器（核心中的核心）** |
| `kvConfigManager` | KVConfigManager | KV 配置管理器 |
| `brokerHousekeepingService` | BrokerHousekeepingService | Broker 连接管家（监听连接事件） |
| `configuration` | Configuration | 统一配置管理 |
| `remotingServer` | RemotingServer | Netty 服务端（接收请求） |
| `remotingClient` | RemotingClient | Netty 客户端（主动连接，5.x） |
| `defaultExecutor` | ExecutorService | 处理默认请求的线程池 |
| `clientRequestExecutor` | ExecutorService | 处理客户端路由查询的线程池 |
| `scheduledExecutorService` | ScheduledExecutorService | 定时任务调度器 |
| `scanExecutorService` | ScheduledExecutorService | Broker 存活扫描调度器 |
| `fileWatchService` | FileWatchService | TLS 证书文件监听 |
| `startupTimeMillis` | long | 启动时间戳 |

### 3.2 构造方法

```java
// NamesrvController.java

public NamesrvController(NamesrvConfig namesrvConfig,
                         NettyServerConfig nettyServerConfig,
                         NettyClientConfig nettyClientConfig) {
    this.namesrvConfig = namesrvConfig;
    this.nettyServerConfig = nettyServerConfig;
    this.nettyClientConfig = nettyClientConfig;

    // 创建 KV 配置管理器
    this.kvConfigManager = new KVConfigManager(this);

    // 创建 Broker 连接管家
    this.brokerHousekeepingService = new BrokerHousekeepingService(this);

    // 创建路由信息管理器 —— NameServer 的核心
    this.routeInfoManager = new RouteInfoManager(namesrvConfig, this);

    // 创建统一配置管理器
    this.configuration = new Configuration(log,
        this.namesrvConfig, this.nettyServerConfig);
    this.configuration.setStorePathFromConfig(
        this.namesrvConfig, "configStorePath");
}
```

构造方法做的事情很纯粹——只创建对象，不做任何初始化操作。
这遵循了"构造函数不做重活"的设计原则，复杂的初始化逻辑都放在 `initialize()` 方法中。

**注意依赖注入的方向**：`KVConfigManager`、`BrokerHousekeepingService`、`RouteInfoManager`
都以 `this`（即 NamesrvController）作为构造参数。这形成了一种"中心辐射"式的依赖关系：
所有子组件都可以通过持有的 controller 引用来访问其他组件。

### 3.3 initialize() 方法详解

`initialize()` 是 NamesrvController 最核心的方法，负责完成所有初始化工作。
我们按执行顺序逐步分析：

```java
// NamesrvController.java

public boolean initialize() {
    // ========== 步骤一：加载 KV 配置 ==========
    loadConfig();

    // ========== 步骤二：初始化网络组件 ==========
    initiateNetworkComponents();

    // ========== 步骤三：初始化线程池 ==========
    initiateThreadExecutors();

    // ========== 步骤四：注册请求处理器 ==========
    registerProcessor();

    // ========== 步骤五：启动定时任务 ==========
    startScheduleService();

    // ========== 步骤六：初始化 SSL 上下文 ==========
    initiateSslContext();

    // ========== 步骤七：初始化 RPC 钩子 ==========
    initiateRpcHooks();

    return true;
}
```

#### 步骤一：loadConfig()

```java
// NamesrvController.java

private void loadConfig() {
    this.kvConfigManager.load();
}
```

从磁盘加载 KV 配置。KVConfigManager 会尝试从 `{user.home}/namesrv/kvConfig.json` 
文件中读取之前持久化的 KV 配置数据。如果文件不存在（首次启动），则跳过。

#### 步骤二：initiateNetworkComponents()

```java
// NamesrvController.java

private void initiateNetworkComponents() {
    // 创建 Netty 服务端
    this.remotingServer = new NettyRemotingServer(
        this.nettyServerConfig,
        this.brokerHousekeepingService  // 作为 ChannelEventListener
    );

    // 创建 Netty 客户端（5.x 新增，用于 Controller 模式）
    this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
}
```

这里有一个非常重要的设计细节：**`brokerHousekeepingService` 作为 `ChannelEventListener` 
传入 NettyRemotingServer**。

`ChannelEventListener` 是 RocketMQ 自定义的接口，当 Netty Channel 发生状态变化
（连接关闭、异常、空闲超时）时，NettyRemotingServer 会回调这个监听器。
`BrokerHousekeepingService` 实现了这个接口，在 Broker 连接断开时自动清理路由信息。

这是**事件驱动模式**的典型应用：NameServer 不需要主动轮询每个 Broker 的连接状态，
而是通过 Netty 的事件机制被动感知连接变化。

#### 步骤三：initiateThreadExecutors()

```java
// NamesrvController.java

private void initiateThreadExecutors() {
    // 默认请求处理线程池
    this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(
        this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
    this.defaultExecutor = new ThreadPoolExecutor(
        this.namesrvConfig.getDefaultThreadPoolNums(),
        this.namesrvConfig.getDefaultThreadPoolNums(),
        1L * 60, TimeUnit.SECONDS,
        this.defaultThreadPoolQueue,
        new ThreadFactoryImpl("RemotingExecutorThread_")
    );

    // 客户端路由查询专用线程池
    this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(
        this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
    this.clientRequestExecutor = new ThreadPoolExecutor(
        this.namesrvConfig.getClientRequestThreadPoolNums(),
        this.namesrvConfig.getClientRequestThreadPoolNums(),
        1L * 60, TimeUnit.SECONDS,
        this.clientRequestThreadPoolQueue,
        new ThreadFactoryImpl("ClientRequestExecutorThread_")
    );
}
```

**为什么要分两个线程池？**

这是一个非常精妙的设计，目的是**资源隔离**：

1. **`defaultExecutor`**：处理来自 Broker 的请求（注册、心跳、注销等），这些操作涉及
   路由表的写操作，频率相对较低但每次操作较重（需要获取写锁）。

2. **`clientRequestExecutor`**：专门处理来自 Producer/Consumer 的路由查询请求
   （`GET_ROUTEINFO_BY_TOPIC`），这类请求频率高、操作轻（只需读锁），是 NameServer
   最主要的请求类型。

将两者隔离到不同线程池，可以防止以下场景：
- 大量 Broker 注册请求涌入时，不会影响客户端的路由查询
- 客户端路由查询风暴不会阻塞 Broker 的注册和心跳处理

这种**按调用方隔离线程池**的思想，在中间件设计中非常常见，类似于 Sentinel 的线程池隔离策略。

#### 步骤四：registerProcessor()

```java
// NamesrvController.java

private void registerProcessor() {
    // 客户端路由查询请求 → ClientRequestProcessor → clientRequestExecutor
    ClientRequestProcessor clientRequestProcessor =
        new ClientRequestProcessor(this);
    this.remotingServer.registerProcessor(
        RequestCode.GET_ROUTEINFO_BY_TOPIC,
        clientRequestProcessor,
        this.clientRequestExecutor
    );

    // 其他所有请求 → DefaultRequestProcessor → defaultExecutor
    DefaultRequestProcessor defaultRequestProcessor =
        new DefaultRequestProcessor(this);
    this.remotingServer.registerDefaultProcessor(
        defaultRequestProcessor,
        this.defaultExecutor
    );
}
```

RocketMQ 的 Netty 服务端支持两种处理器注册方式：

1. **`registerProcessor(requestCode, processor, executor)`**：为特定请求码注册专用处理器
2. **`registerDefaultProcessor(processor, executor)`**：注册默认处理器，处理所有未显式注册的请求码

在 NameServer 中：
- `GET_ROUTEINFO_BY_TOPIC`（路由查询）有自己的专用处理器和线程池
- 其余 20+ 种请求码都由 DefaultRequestProcessor 处理

这再次体现了**路由查询是 NameServer 的核心操作**，需要专门优化。

#### 步骤五：startScheduleService()

```java
// NamesrvController.java

private void startScheduleService() {
    // 定时任务调度线程池
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryImpl("NSScheduledThread"));
    this.scanExecutorService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryImpl("NSScanScheduledThread"));

    // ---- 定时任务 1：扫描不活跃的 Broker ----
    // 默认每 5 秒执行一次（scanNotActiveBrokerInterval 默认 5000ms）
    this.scanExecutorService.scheduleAtFixedRate(
        NamesrvController.this.routeInfoManager::scanNotActiveBroker,
        5,  // 初始延迟 5 秒
        this.namesrvConfig.getScanNotActiveBrokerInterval(),
        TimeUnit.MILLISECONDS
    );

    // ---- 定时任务 2：每 10 分钟打印一次所有路由信息（用于运维诊断）----
    this.scheduledExecutorService.scheduleAtFixedRate(
        NamesrvController.this.routeInfoManager::printAllPeriodically,
        1,  // 初始延迟 1 分钟
        10, // 每 10 分钟
        TimeUnit.MINUTES
    );

    // ---- 定时任务 3：每 1 秒打印一次线程池水位（用于监控）----
    this.scheduledExecutorService.scheduleAtFixedRate(() -> {
        NamesrvController.this.printWaterMark();
    }, 1, 1, TimeUnit.SECONDS);
}
```

三个定时任务的详细说明：

| 任务 | 频率 | 职责 |
|------|------|------|
| `scanNotActiveBroker` | 每5秒 | 扫描 brokerLiveTable，将超过2分钟没有心跳的 Broker 标记为不可用并清理 |
| `printAllPeriodically` | 每10分钟 | 将所有路由信息打印到日志文件，方便运维排查 |
| `printWaterMark` | 每1秒 | 打印线程池队列水位，用于监控线程池健康状况 |

**`scanNotActiveBroker` 是最关键的定时任务**，它是 NameServer 实现 Broker 故障检测的核心机制。
我们会在第八章详细分析其源码。

#### 步骤六：initiateSslContext()

```java
// NamesrvController.java

private void initiateSslContext() {
    if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
        // 初始化 SSL 上下文
        // 如果启用了文件监听模式，则创建 FileWatchService
        // 监听证书文件变化，支持热更新
    }
}
```

TLS/SSL 支持是生产环境的重要特性。NameServer 支持三种 TLS 模式：
- `DISABLED`：不启用 TLS
- `PERMISSIVE`：允许 TLS 和非 TLS 连接
- `ENFORCING`：强制 TLS 连接

#### 步骤七：initiateRpcHooks()

```java
// NamesrvController.java

private void initiateRpcHooks() {
    this.remotingServer.registerRPCHook(new ZoneRouteRPCHook());
}
```

`ZoneRouteRPCHook` 是 RocketMQ 5.x 新增的 RPC 钩子，用于支持**多可用区路由**。
当 Producer/Consumer 查询路由信息时，这个钩子会在返回结果中注入可用区相关的信息，
帮助客户端实现**就近访问**（优先访问同一可用区的 Broker）。

### 3.4 start() 方法

```java
// NamesrvController.java

public void start() throws Exception {
    // 启动 Netty 服务端，开始监听 9876 端口
    this.remotingServer.start();

    // 如果有 Netty 客户端，也启动（5.x Controller 模式）
    if (this.remotingClient != null) {
        this.remotingClient.updateNameServerAddressList(
            Collections.singletonList(
                NetworkUtil.getLocalAddress() + ":" +
                this.nettyServerConfig.getListenPort()
            ));
        this.remotingClient.start();
    }

    // 启动 TLS 证书文件监听服务（如果有）
    if (this.fileWatchService != null) {
        this.fileWatchService.start();
    }

    // 启动路由信息管理器
    this.routeInfoManager.start();

    // 记录启动时间
    this.startupTimeMillis = System.currentTimeMillis();
}
```

`start()` 方法按顺序启动各个组件。注意 `startupTimeMillis` 的记录——这个时间戳
在 `ClientRequestProcessor` 中会用到，用于实现**启动保护期**（grace period）。

### 3.5 shutdown() 方法

```java
// NamesrvController.java

public void shutdown() {
    // 按照创建的逆序关闭各组件
    this.remotingServer.shutdown();

    if (this.remotingClient != null) {
        this.remotingClient.shutdown();
    }

    if (this.fileWatchService != null) {
        this.fileWatchService.shutdown();
    }

    this.routeInfoManager.shutdown();

    // 停止线程池
    this.defaultExecutor.shutdown();
    this.clientRequestExecutor.shutdown();

    // 停止定时任务
    this.scheduledExecutorService.shutdown();
    this.scanExecutorService.shutdown();
}
```

关闭顺序很重要：
1. 先关闭网络层（停止接收新请求）
2. 再关闭业务组件（处理完已有请求）
3. 最后关闭线程池和定时任务

这遵循了"先断源头，再清存量"的优雅关闭原则。

---

## 四、RouteInfoManager核心数据结构

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/RouteInfoManager.java`（约1279行）

`RouteInfoManager` 是 NameServer 的心脏，管理着全部的路由信息。
理解它的数据结构是理解 NameServer 所有行为的基础。

### 4.1 核心数据结构概览

```java
// RouteInfoManager.java

public class RouteInfoManager {

    private static final Logger log = LoggerFactory.getLogger(
        LoggerName.NAMESRV_LOGGER_NAME);

    // Broker Channel 过期时间，默认 120 秒（2 分钟）
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME =
        1000 * 60 * 2;

    // 读写锁，保护下面所有的路由数据结构
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // ========== 核心路由表（5+1 张表）==========

    // 表1：Topic → {BrokerName → QueueData}
    // 记录每个 Topic 在各个 Broker 上的队列配置
    private final Map<String/* topic */,
                      Map<String/* brokerName */,
                          QueueData>> topicQueueTable;

    // 表2：BrokerName → BrokerData
    // 记录每个 Broker 名称对应的地址信息（主从地址映射）
    private final Map<String/* brokerName */,
                      BrokerData> brokerAddrTable;

    // 表3：ClusterName → Set<BrokerName>
    // 记录每个集群包含哪些 Broker
    private final Map<String/* clusterName */,
                      Set<String/* brokerName */>> clusterAddrTable;

    // 表4：BrokerAddrInfo → BrokerLiveInfo
    // 记录每个 Broker 地址的存活信息（最后心跳时间、Channel 等）
    private final Map<BrokerAddrInfo/* brokerAddr */,
                      BrokerLiveInfo> brokerLiveTable;

    // 表5：BrokerAddrInfo → List<FilterServer地址>
    // 记录每个 Broker 上的 FilterServer 列表（用于服务端消息过滤）
    private final Map<BrokerAddrInfo/* brokerAddr */,
                      List<String>/* filterServer */> filterServerTable;

    // 表6：Topic → {BrokerName → TopicQueueMappingInfo}
    // 记录 Topic 的逻辑队列映射信息（5.x Static Topic 特性）
    private final Map<String/* topic */,
                      Map<String/* brokerName */,
                          TopicQueueMappingInfo>> topicQueueMappingInfoTable;

    // 批量注销服务
    private final BatchUnregistrationService unRegisterService;

    // NameServer 配置
    private final NamesrvConfig namesrvConfig;
    private final NamesrvController namesrvController;
}
```

### 4.2 表1：topicQueueTable 详解

```
topicQueueTable: Map<String, Map<String, QueueData>>
│
├── "TopicA" ──► Map
│                ├── "broker-a" ──► QueueData {
│                │                     readQueueNums = 8,
│                │                     writeQueueNums = 8,
│                │                     perm = 6 (RW),
│                │                     topicSysFlag = 0,
│                │                     topicSynFlag = 0
│                │                 }
│                └── "broker-b" ──► QueueData {
│                                     readQueueNums = 8,
│                                     writeQueueNums = 8,
│                                     perm = 6 (RW),
│                                     topicSysFlag = 0,
│                                     topicSynFlag = 0
│                                 }
│
├── "TopicB" ──► Map
│                └── "broker-a" ──► QueueData { ... }
│
└── "%RETRY%ConsumerGroupA" ──► Map
                 └── "broker-a" ──► QueueData {
                                     readQueueNums = 1,
                                     writeQueueNums = 1,
                                     perm = 6
                                 }
```

`QueueData` 类的定义：

```java
public class QueueData implements Comparable<QueueData> {
    private String brokerName;       // Broker 名称
    private int readQueueNums;       // 可读队列数
    private int writeQueueNums;      // 可写队列数
    private int perm;                // 权限（2=W, 4=R, 6=RW）
    private int topicSysFlag;        // Topic 系统标志
    // ...
}
```

**关键设计点**：
- `topicQueueTable` 的第二层 key 是 `brokerName`（如 "broker-a"），不是 broker 地址
- 同一个 `brokerName` 下可能有多个物理节点（主从），但队列配置只存一份
- `readQueueNums` 和 `writeQueueNums` 可以不同，这支持了队列的"缩容不丢数据"场景

### 4.3 表2：brokerAddrTable 详解

```
brokerAddrTable: Map<String, BrokerData>
│
├── "broker-a" ──► BrokerData {
│                     cluster = "DefaultCluster",
│                     brokerName = "broker-a",
│                     brokerAddrs = {
│                         0L → "192.168.1.10:10911",  // Master
│                         1L → "192.168.1.11:10911",  // Slave-1
│                         2L → "192.168.1.12:10911"   // Slave-2
│                     },
│                     zoneName = "zone-a",
│                     enableActingMaster = true
│                 }
│
└── "broker-b" ──► BrokerData {
                     cluster = "DefaultCluster",
                     brokerName = "broker-b",
                     brokerAddrs = {
                         0L → "192.168.1.20:10911",  // Master
                         1L → "192.168.1.21:10911"   // Slave-1
                     },
                     zoneName = "zone-b",
                     enableActingMaster = false
                 }
```

`BrokerData` 类的定义：

```java
public class BrokerData implements Comparable<BrokerData> {
    private String cluster;          // 集群名称
    private String brokerName;       // Broker 名称
    // brokerId → brokerAddress 的映射
    // brokerId=0 表示 Master，>0 表示 Slave
    private HashMap<Long/* brokerId */, String/* brokerAddr */> brokerAddrs;
    private String zoneName;         // 可用区名称
    private boolean enableActingMaster;  // 是否启用 Acting Master
    // ...
}
```

**关键设计点**：
- `brokerAddrs` 是一个 `Map<Long, String>`，key 是 brokerId，value 是地址
- **brokerId = 0 表示 Master**，这是 RocketMQ 的约定
- 同一个 `brokerName` 下的所有节点共享同一份队列配置（topicQueueTable 中按 brokerName 索引）
- `enableActingMaster` 是 5.x 的新特性，允许 Slave 在 Master 不可用时临时充当 Master

### 4.4 表3：clusterAddrTable 详解

```
clusterAddrTable: Map<String, Set<String>>
│
├── "DefaultCluster" ──► Set { "broker-a", "broker-b", "broker-c" }
│
└── "OrderCluster"   ──► Set { "broker-order-1", "broker-order-2" }
```

这是一个简单的集群到 Broker 名称的反向索引。它的作用主要有两个：
1. 支持按集群维度查询路由信息（如运维工具查看某个集群下的所有 Broker）
2. 在 Broker 注册时，将 Broker 归入正确的集群

### 4.5 表4：brokerLiveTable 详解

```
brokerLiveTable: Map<BrokerAddrInfo, BrokerLiveInfo>
│
├── BrokerAddrInfo("DefaultCluster", "192.168.1.10:10911")
│   ──► BrokerLiveInfo {
│         lastUpdateTimestamp = 1700000000000,
│         heartbeatTimeoutMillis = 120000,
│         dataVersion = DataVersion{counter=5, timestamp=...},
│         channel = NettyChannel{...},
│         haServerAddr = "192.168.1.10:10912"
│       }
│
├── BrokerAddrInfo("DefaultCluster", "192.168.1.11:10911")
│   ──► BrokerLiveInfo { ... }
│
└── BrokerAddrInfo("DefaultCluster", "192.168.1.20:10911")
    ──► BrokerLiveInfo { ... }
```

`BrokerAddrInfo` 类的定义：

```java
// RouteInfoManager.java 的内部类

public static class BrokerAddrInfo {
    private String clusterName;   // 集群名称
    private String brokerAddr;    // Broker 地址 (ip:port)

    // hashCode 和 equals 基于 clusterName + brokerAddr
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result +
            ((clusterName == null) ? 0 : clusterName.hashCode());
        result = prime * result +
            ((brokerAddr == null) ? 0 : brokerAddr.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BrokerAddrInfo other = (BrokerAddrInfo) obj;
        // 同时比较 clusterName 和 brokerAddr
        return Objects.equals(clusterName, other.clusterName)
            && Objects.equals(brokerAddr, other.brokerAddr);
    }
}
```

**为什么用 `BrokerAddrInfo` 而不是直接用 String 地址做 key？**

因为理论上不同集群中的 Broker 可能使用相同的 IP:Port（虽然不常见）。
使用 `clusterName + brokerAddr` 组合作为唯一标识，可以避免跨集群的地址冲突。

`BrokerLiveInfo` 类的定义：

```java
// RouteInfoManager.java 的内部类

class BrokerLiveInfo {
    private long lastUpdateTimestamp;      // 最后一次心跳时间
    private long heartbeatTimeoutMillis;    // 心跳超时时间
    private DataVersion dataVersion;        // 数据版本号
    private Channel channel;                // Netty Channel
    private String haServerAddr;            // HA 服务地址
}
```

`BrokerLiveInfo` 是 Broker 存活检测的核心数据：
- `lastUpdateTimestamp`：每次收到 Broker 的心跳（注册请求或心跳请求）时更新
- `heartbeatTimeoutMillis`：心跳超时时间，默认 120000ms（2分钟）
- `channel`：Broker 与 NameServer 的 Netty 连接
- `dataVersion`：Broker 的数据版本号，用于增量同步判断

### 4.6 表5：filterServerTable 详解

```
filterServerTable: Map<BrokerAddrInfo, List<String>>
│
└── BrokerAddrInfo("DefaultCluster", "192.168.1.10:10911")
    ──► ["192.168.1.10:11811", "192.168.1.10:11812"]
```

FilterServer 是 RocketMQ 早期支持的一种服务端消息过滤机制。每个 Broker 可以挂载多个
FilterServer 进程，Consumer 可以向 FilterServer 拉取经过过滤的消息。

在现代 RocketMQ 中，FilterServer 已经不太常用（被 SQL92 过滤和 Tag 过滤取代），
但路由表中仍保留这个结构以保持向后兼容。

### 4.7 表6：topicQueueMappingInfoTable 详解

```
topicQueueMappingInfoTable: Map<String, Map<String, TopicQueueMappingInfo>>
│
└── "StaticTopic" ──► Map
                      ├── "broker-a" ──► TopicQueueMappingInfo { ... }
                      └── "broker-b" ──► TopicQueueMappingInfo { ... }
```

这是 RocketMQ 5.x 引入的 **Static Topic** 特性的配套数据结构。
Static Topic 的逻辑队列可以在不同 Broker 之间迁移，这个表记录了逻辑队列到物理队列的映射关系。

### 4.8 六张表的关系图

```
                          ┌──────────────────────┐
                          │   clusterAddrTable   │
                          │   (集群 → Broker名)   │
                          │                      │
                          │  "DefaultCluster"     │
                          │  ──► {broker-a,       │
                          │       broker-b}       │
                          └──────────┬───────────┘
                                     │
                          通过 brokerName 关联
                                     │
     ┌─────────────────────┐        ▼           ┌───────────────────────┐
     │  topicQueueTable    │  brokerAddrTable    │   brokerLiveTable     │
     │  (Topic → 队列配置)  │  (Broker名→地址)    │   (地址 → 存活信息)    │
     │                     │                    │                       │
     │  "TopicA"           │  "broker-a" ──►    │  BrokerAddrInfo        │
     │   ├─ broker-a ──►   │   BrokerData {     │  (cluster, addr)      │
     │   │  QueueData      │     0→10.0.0.1,    │  ──► BrokerLiveInfo { │
     │   └─ broker-b ──►   │     1→10.0.0.2     │    lastUpdateTime,    │
     │      QueueData      │   }                │    channel,           │
     └──────────┬──────────┘                    │    dataVersion        │
                │           └────────┬──────────┘    }                  │
     通过 brokerName             通过 brokerAddr    └───────────────────┘
     连接到 BrokerData           连接到 BrokerLiveInfo
                │                    │
                ▼                    ▼
    ┌────────────────────────────────────────────┐
    │           数据流向总结                       │
    │                                            │
    │  Client 查路由:                              │
    │    topicQueueTable → 获取 QueueData         │
    │    → 从 QueueData 拿到 brokerName           │
    │    → brokerAddrTable 查 BrokerData          │
    │    → 获得 brokerAddrs (主从地址)              │
    │                                            │
    │  NameServer 心跳检测:                        │
    │    brokerLiveTable → 检查 lastUpdateTime    │
    │    → 超时 → 从所有表中清理该 Broker           │
    └────────────────────────────────────────────┘
```

### 4.9 ReadWriteLock 并发控制

```java
// RouteInfoManager.java

private final ReadWriteLock lock = new ReentrantReadWriteLock();
```

所有对路由表的操作都通过 ReadWriteLock 来保护。这是一个经典的并发控制策略：

```
┌──────────────────────────────────────────────────────────┐
│                ReadWriteLock 使用场景                      │
├──────────────────┬───────────────────────────────────────┤
│     读锁 (共享)   │              写锁 (排他)              │
├──────────────────┼───────────────────────────────────────┤
│ pickupTopicRoute │ registerBroker                        │
│ Data             │ unRegisterBroker                      │
│                  │ onChannelDestroy                      │
│ getAllTopicList  │ wipeWritePermOfBrokerByLock            │
│                  │ addWritePermOfBrokerByLock             │
│ getTopicsByClust │ scanNotActiveBroker (间接通过          │
│ er               │   unRegisterBroker)                   │
│                  │                                       │
│ 高频、低延迟      │ 低频、操作重                           │
│ 允许并发执行      │ 必须互斥执行                           │
└──────────────────┴───────────────────────────────────────┘
```

**为什么用 ReadWriteLock 而不是 synchronized？**

NameServer 的读操作（路由查询）远多于写操作（Broker 注册/注销）。
使用 ReadWriteLock 可以让多个路由查询请求并发执行，只有在 Broker 注册/注销时才需要排他锁。
这大大提高了 NameServer 在高并发路由查询场景下的吞吐量。

具体的加锁模式：

```java
// 读操作示例
public TopicRouteData pickupTopicRouteData(String topic) {
    this.lock.readLock().lockInterruptibly();
    try {
        // 读取路由表...
    } finally {
        this.lock.readLock().unlock();
    }
}

// 写操作示例
public RegisterBrokerResult registerBroker(...) {
    this.lock.writeLock().lockInterruptibly();
    try {
        // 更新路由表...
    } finally {
        this.lock.writeLock().unlock();
    }
}
```

**注意**：RocketMQ 使用了 `lockInterruptibly()` 而不是 `lock()`，
这允许在等待锁的过程中被中断，避免了死锁的风险。

### 4.10 构造方法

```java
// RouteInfoManager.java

public RouteInfoManager(NamesrvConfig namesrvConfig,
                        NamesrvController namesrvController) {
    this.namesrvConfig = namesrvConfig;
    this.namesrvController = namesrvController;

    this.topicQueueTable = new ConcurrentHashMap<>(1024);
    this.brokerAddrTable = new ConcurrentHashMap<>(128);
    this.clusterAddrTable = new ConcurrentHashMap<>(32);
    this.brokerLiveTable = new ConcurrentHashMap<>(256);
    this.filterServerTable = new ConcurrentHashMap<>(256);
    this.topicQueueMappingInfoTable = new ConcurrentHashMap<>(1024);

    // 创建批量注销服务
    this.unRegisterService = new BatchUnregistrationService(
        this, namesrvConfig);
}
```

**ConcurrentHashMap 初始容量的选择**：
- `topicQueueTable`: 1024 — Topic 数量通常最多
- `brokerAddrTable`: 128 — Broker 数量适中
- `clusterAddrTable`: 32 — 集群数量最少
- `brokerLiveTable`: 256 — 每个 Broker 节点一个条目

这些初始容量的选择反映了生产环境中各表的典型大小比例。

虽然已经使用了 ReadWriteLock 保护，为什么底层还用 ConcurrentHashMap？
这是一种**防御性编程**：即使未来某处代码遗漏了加锁，ConcurrentHashMap 本身的线程安全性
也能提供基本的保护，避免出现 `ConcurrentModificationException` 等运行时错误。

---

## 五、Broker注册全链路 (registerBroker)

> 源码文件：`RouteInfoManager.java`，方法 `registerBroker()`，约 lines 226-409

Broker 注册是 NameServer 最核心的写操作。每当 Broker 启动或定期发送心跳时，
都会向所有 NameServer 发送注册请求。NameServer 收到请求后，会更新所有路由表。

### 5.1 方法签名

```java
// RouteInfoManager.java

public RegisterBrokerResult registerBroker(
    final String clusterName,          // 集群名称
    final String brokerAddr,           // Broker 地址 (ip:port)
    final String brokerName,           // Broker 名称
    final long brokerId,               // Broker ID (0=Master, >0=Slave)
    final String haServerAddr,         // HA 服务地址
    final String zoneName,             // 可用区名称
    final Long timeoutMillis,          // 心跳超时时间
    final Boolean enableActingMaster,  // 是否启用 Acting Master
    final TopicConfigSerializeWrapper topicConfigWrapper,  // Topic 配置
    final List<String> filterServerList,  // FilterServer 列表
    final Channel channel              // Netty Channel
) {
    // ... 具体逻辑见下文 ...
}
```

参数说明：

| 参数 | 类型 | 来源 | 说明 |
|------|------|------|------|
| `clusterName` | String | Broker 配置 | Broker 所属集群 |
| `brokerAddr` | String | Broker 网络地址 | 格式 "ip:port"，如 "192.168.1.10:10911" |
| `brokerName` | String | Broker 配置 | Broker 名称，主从相同 |
| `brokerId` | long | Broker 配置 | 0=Master, 1+=Slave |
| `haServerAddr` | String | Broker HA 模块 | HA 同步使用的地址 |
| `zoneName` | String | Broker 配置 | 多可用区部署时的区域标识 |
| `timeoutMillis` | Long | Broker 配置 | 心跳超时时间，null 则使用默认 2 分钟 |
| `enableActingMaster` | Boolean | Broker 配置 | 是否允许 Slave 临时充当 Master |
| `topicConfigWrapper` | TopicConfigSerializeWrapper | Broker 元数据 | 包含所有 Topic 配置和数据版本号 |
| `filterServerList` | List<String> | Broker FilterServer 模块 | FilterServer 地址列表 |
| `channel` | Channel | Netty 连接 | Broker 与 NameServer 的 TCP 连接 |

### 5.2 注册流程总览

```
                      registerBroker() 执行流程
┌───────────────────────────────────────────────────────────────┐
│                                                               │
│  ┌─────────────────────────────────┐                          │
│  │ Step 1: 更新 clusterAddrTable  │                          │
│  │ 将 brokerName 加入集群集合       │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 2: 更新 brokerAddrTable   │                          │
│  │ 创建或更新 BrokerData           │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 3: 设置 enableActingMaster│                          │
│  │ 和 zoneName                    │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 4: 记录 minBrokerId 变化  │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 5: 去重同地址节点          │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 6: 状态版本冲突检查        │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 7: 拒绝过早的单Topic注册  │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 8: 写入地址                │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 9: 判断 Master/Prime Slave│                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 10: 更新 topicQueueTable  │                          │
│  │ (删除旧 + createAndUpdate)      │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 11: 更新 brokerLiveTable  │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 12: 更新 filterServerTable│                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 13: 返回 Master 信息      │                          │
│  └─────────────────┬───────────────┘                          │
│                    ▼                                          │
│  ┌─────────────────────────────────┐                          │
│  │ Step 14: 通知 minBrokerId 变化 │                          │
│  └─────────────────────────────────┘                          │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### 5.3 Step 1：更新 clusterAddrTable

```java
// RouteInfoManager.java - registerBroker()

RegisterBrokerResult result = new RegisterBrokerResult();

this.lock.writeLock().lockInterruptibly();
try {
    // Step 1: 更新 clusterAddrTable
    Set<String> brokerNames = this.clusterAddrTable
        .computeIfAbsent(clusterName, k -> new ConcurrentHashSet<>());
    brokerNames.add(brokerName);
```

使用 `computeIfAbsent()` 原子性地创建集群对应的 Set（如果不存在）。
然后将当前 brokerName 加入集群集合。

**注意**：这里获取了**写锁**，后续所有步骤都在写锁保护下执行。
整个 `registerBroker()` 方法是一个**大事务**，保证了路由表更新的原子性。

### 5.4 Step 2：更新 brokerAddrTable

```java
    // Step 2: 更新 brokerAddrTable
    boolean registerFirst = false;
    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
    if (null == brokerData) {
        registerFirst = true;
        brokerData = new BrokerData(
            clusterName, brokerName,
            new HashMap<>());
        this.brokerAddrTable.put(brokerName, brokerData);
    }
```

如果这是该 brokerName 首次注册，创建新的 BrokerData 并加入表中。
`registerFirst` 标志后续会用到：首次注册的 Broker 必须更新 topicQueueTable。

### 5.5 Step 3：设置 enableActingMaster 和 zoneName

```java
    // Step 3: 设置 enableActingMaster 和 zoneName
    if (enableActingMaster != null) {
        brokerData.setEnableActingMaster(enableActingMaster);
    }
    if (zoneName != null) {
        brokerData.setZoneName(zoneName);
    }
```

这两个字段来自 Broker 的配置，每次注册时更新。`enableActingMaster` 决定了在 Master 宕机时，
是否允许 Slave 临时充当 Master 接收写请求。

### 5.6 Step 4：记录 minBrokerId 变化

```java
    // Step 4: 记录 minBrokerId 变化前的值
    Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
    boolean isMinBrokerIdChanged = false;
    long prevMinBrokerId = 0;
    if (!brokerAddrsMap.isEmpty()) {
        prevMinBrokerId = Collections.min(brokerAddrsMap.keySet());
    }
```

记录更新前的最小 brokerId。在主从架构中，最小 brokerId（通常是 0）对应 Master。
如果后续步骤中最小 brokerId 发生变化（例如原来的 Master 下线了），
需要通知相关的 Broker 进行角色切换。

### 5.7 Step 5：去重同地址节点

```java
    // Step 5: 去重 —— 如果同一个地址注册了不同的 brokerId，删除旧的
    if (brokerId != MixAll.MASTER_ID) {
        // 对于 Slave 节点，检查是否有其他 brokerId 使用了相同地址
        String oldBrokerAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
        if (oldBrokerAddr != null && oldBrokerAddr.equals(brokerAddr)) {
            // 如果 Slave 的地址和 Master 的地址相同（异常情况），移除旧的 Master 条目
            brokerData.getBrokerAddrs().remove(MixAll.MASTER_ID);
        }
    }

    // 遍历所有 brokerId，删除使用相同地址但不同 brokerId 的条目
    Iterator<Map.Entry<Long, String>> it =
        brokerAddrsMap.entrySet().iterator();
    while (it.hasNext()) {
        Map.Entry<Long, String> item = it.next();
        if (item.getValue().equals(brokerAddr)
            && brokerId != item.getKey()) {
            log.debug("Remove duplicate broker addr: {} from brokerName: {}",
                brokerAddr, brokerName);
            it.remove();
        }
    }
```

**为什么需要去重？**

在 Broker 进行主从切换时，同一个物理节点可能先以 Slave 身份注册（brokerId=1），
然后切换为 Master 后又以 brokerId=0 注册。此时需要清理之前的 brokerId=1 记录，
避免同一个地址出现在两个 brokerId 下。

### 5.8 Step 6：状态版本冲突检查

```java
    // Step 6: 状态版本冲突检查
    String oldBrokerAddr = brokerData.getBrokerAddrs().get(brokerId);
    if (oldBrokerAddr != null && !oldBrokerAddr.equals(brokerAddr)) {
        BrokerAddrInfo oldBrokerAddrInfo =
            new BrokerAddrInfo(clusterName, oldBrokerAddr);
        BrokerLiveInfo oldBrokerLiveInfo =
            brokerLiveTable.get(oldBrokerAddrInfo);
        if (oldBrokerLiveInfo != null) {
            long oldStateVersion =
                oldBrokerLiveInfo.getDataVersion().getStateVersion();
            long newStateVersion = topicConfigWrapper
                .getDataVersion().getStateVersion();
            if (oldStateVersion > newStateVersion) {
                log.warn("Registered broker conflicts with existing one, "
                    + "rejecting this request. Old: {}, New: {}",
                    oldBrokerAddr, brokerAddr);
                // 更新旧地址的心跳时间
                BrokerLiveInfo newBrokerLiveInfo =
                    brokerLiveTable.get(
                        new BrokerAddrInfo(clusterName, brokerAddr));
                if (newBrokerLiveInfo != null) {
                    newBrokerLiveInfo.setLastUpdateTimestamp(
                        System.currentTimeMillis());
                }
                // 保持旧地址不变
                return result;
            }
        }
    }
```

**这个检查解决了什么问题？**

考虑以下场景：
1. Broker-A（地址 10.0.0.1）作为 Master（brokerId=0）注册，stateVersion=5
2. Broker-A 宕机
3. Broker-B（地址 10.0.0.2）通过某种机制升级为 Master（brokerId=0），stateVersion=3
4. Broker-A 恢复，又以 Master 身份注册，stateVersion=5

此时如果直接接受 Broker-A 的注册，会覆盖 Broker-B 的地址，但 Broker-B 可能已经在处理流量了。
通过 `stateVersion` 比较，确保**版本号更高（更新）的注册请求才能覆盖**。

### 5.9 Step 7：拒绝过早的单 Topic 注册

```java
    // Step 7: 拒绝过早的单 Topic 注册
    if (!brokerData.getBrokerAddrs().containsKey(brokerId)
        && topicConfigWrapper.getTopicConfigTable().size() == 1) {
        log.warn("Can't register topicConfigWrapper={} because broker[{}]" +
            " has not registered.", topicConfigWrapper, brokerId);
        return result;
    }
```

**场景**：Broker 有时会单独为某个 Topic 发送注册请求（例如创建新 Topic 时）。
但如果该 Broker 还没有完成完整注册（`brokerAddrs` 中没有该 brokerId），
则拒绝这个单 Topic 注册请求。这避免了在 Broker 初始化阶段出现不完整的路由信息。

### 5.10 Step 8：写入地址

```java
    // Step 8: 将 brokerAddr 写入 BrokerData
    String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
    registerFirst = registerFirst || (StringUtils.isEmpty(oldAddr));
    boolean isMaster = MixAll.MASTER_ID == brokerId;
```

正式将 Broker 地址写入 `brokerAddrs` Map。如果是首次写入（`oldAddr` 为空），
则设置 `registerFirst = true`。

### 5.11 Step 9：判断是否需要更新 topicQueueTable

```java
    // Step 9: 判断是否是 Master 或 "Prime Slave"
    boolean isPrimeSlave = !isOldVersionBroker(topicConfigWrapper)
        && !isMaster
        && brokerData.isEnableActingMaster()
        && !brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID);
```

`isPrimeSlave` 的含义：如果当前 Broker 是 Slave，但满足以下条件，
则被视为 "Prime Slave"（首席从节点）：
1. 不是旧版本 Broker
2. 不是 Master
3. 启用了 Acting Master 机制
4. 当前没有 Master 在线

Prime Slave 会临时接管 Master 的角色，其 Topic 配置会被写入 topicQueueTable，
让 Producer 可以继续发送消息。

### 5.12 Step 10：更新 topicQueueTable

```java
    // Step 10: 更新 topicQueueTable
    if (isMaster || isPrimeSlave) {
        if (topicConfigWrapper != null
            && topicConfigWrapper.getTopicConfigTable() != null) {

            // 获取旧的 Topic 配置版本
            Map<String, TopicConfig> tcTable =
                topicConfigWrapper.getTopicConfigTable();

            if (registerFirst || needRegister(
                    clusterName, brokerAddr,
                    topicConfigWrapper.getDataVersion())) {

                // 遍历所有 Topic 配置
                for (Map.Entry<String, TopicConfig> entry
                     : tcTable.entrySet()) {

                    // 如果是 Prime Slave，修改权限为只读
                    if (isPrimeSlave) {
                        TopicConfig newTopicConfig =
                            new TopicConfig(entry.getValue());
                        newTopicConfig.setPerm(
                            newTopicConfig.getPerm() & (~PermName.PERM_WRITE));
                        // 创建或更新队列数据
                        createAndUpdateQueueData(brokerName, newTopicConfig);
                    } else {
                        createAndUpdateQueueData(brokerName, entry.getValue());
                    }
                }
            }
        }
    }
```

**关键逻辑解析**：

1. **只有 Master 或 Prime Slave 才能更新 topicQueueTable**。
   普通 Slave 的 Topic 配置不会写入路由表——这是因为 Producer 只需要知道 Master 的队列信息，
   Slave 的队列配置与 Master 完全相同。

2. **`registerFirst || needRegister()`**：只有首次注册或数据版本有变化时才更新。
   `needRegister()` 通过比较 DataVersion 来判断 Broker 的 Topic 配置是否发生了变化。
   这避免了每次心跳都全量更新 topicQueueTable，减少了不必要的写操作。

3. **Prime Slave 的权限处理**：如果当前是 Prime Slave 临时顶替 Master，
   其 Topic 配置的权限会被修改为**只读**（去掉 PERM_WRITE）。
   这意味着 Producer 可以看到这些队列，但只有读权限的队列 Producer 不会向其发送消息——
   等等，这里需要特别注意：实际上 Producer 写消息时会检查 `PERM_WRITE` 权限，
   如果没有写权限，Producer 不会选择这些队列。所以 Prime Slave 的作用更多是让
   Consumer 能够继续消费存量消息。

### 5.13 createAndUpdateQueueData() 方法

```java
// RouteInfoManager.java

private void createAndUpdateQueueData(final String brokerName,
                                      final TopicConfig topicConfig) {
    // 从 TopicConfig 构建 QueueData
    QueueData queueData = new QueueData();
    queueData.setBrokerName(brokerName);
    queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
    queueData.setReadQueueNums(topicConfig.getReadQueueNums());
    queueData.setPerm(topicConfig.getPerm());
    queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

    // 获取或创建该 Topic 的队列数据 Map
    Map<String, QueueData> queueDataMap = this.topicQueueTable
        .computeIfAbsent(topicConfig.getTopicName(),
            k -> new HashMap<>());

    // 获取旧的 QueueData
    QueueData old = queueDataMap.get(brokerName);

    if (old == null) {
        // 该 Broker 首次注册该 Topic
        log.info("new topic registered, {} {}",
            topicConfig.getTopicName(), queueData);
        queueDataMap.put(brokerName, queueData);
    } else if (!old.equals(queueData)) {
        // Topic 配置发生变化（队列数、权限等），更新
        log.info("topic changed, {} OLD: {} NEW: {}",
            topicConfig.getTopicName(), old, queueData);
        queueDataMap.put(brokerName, queueData);
    }
    // 如果 QueueData 没有变化，则不做任何操作（避免无谓的 put）
}
```

**设计要点**：
- 通过 `equals()` 比较来判断 QueueData 是否发生变化
- 只在有变化时才更新，减少了不必要的写操作和日志输出
- `topicQueueTable` 的第二层 Map 以 `brokerName` 为 key，保证每个 Broker 对每个 Topic 只有一条记录

### 5.14 Step 11：更新 brokerLiveTable

```java
    // Step 11: 更新 brokerLiveTable
    BrokerAddrInfo brokerAddrInfo =
        new BrokerAddrInfo(clusterName, brokerAddr);
    BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(
        brokerAddrInfo,
        new BrokerLiveInfo(
            System.currentTimeMillis(),
            timeoutMillis == null ?
                DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis,
            topicConfigWrapper == null ?
                new DataVersion() : topicConfigWrapper.getDataVersion(),
            channel,
            haServerAddr
        )
    );

    if (prevBrokerLiveInfo == null) {
        log.info("new broker registered, {} HAService: {}",
            brokerAddr, haServerAddr);
    }
```

每次注册都会更新 `brokerLiveTable`，刷新 `lastUpdateTimestamp`。
这就是 Broker 心跳机制的核心——定期注册 = 定期心跳。

**注意 `timeoutMillis` 参数**：Broker 可以自定义心跳超时时间。
如果 Broker 没有传递这个参数（null），则使用默认值 `DEFAULT_BROKER_CHANNEL_EXPIRED_TIME`（120秒）。

### 5.15 Step 12：更新 filterServerTable

```java
    // Step 12: 更新 filterServerTable
    if (filterServerList != null) {
        if (filterServerList.isEmpty()) {
            this.filterServerTable.remove(brokerAddrInfo);
        } else {
            this.filterServerTable.put(brokerAddrInfo, filterServerList);
        }
    }
```

逻辑简单：有 FilterServer 则更新，空列表则移除。

### 5.16 Step 13：返回 Master 信息

```java
    // Step 13: 构造返回结果
    if (MixAll.MASTER_ID != brokerId) {
        // 如果当前注册的是 Slave，返回 Master 的地址和 HA 地址
        String masterAddr = brokerData.getBrokerAddrs()
            .get(MixAll.MASTER_ID);
        if (masterAddr != null) {
            BrokerLiveInfo masterLiveInfo = this.brokerLiveTable.get(
                new BrokerAddrInfo(clusterName, masterAddr));
            if (masterLiveInfo != null) {
                result.setHaServerAddr(masterLiveInfo.getHaServerAddr());
                result.setMasterAddr(masterAddr);
            }
        }
    }
```

**为什么 Slave 注册时需要返回 Master 信息？**

Slave 需要知道 Master 的地址来进行数据同步（HA 复制）。
当 Slave 启动后向 NameServer 注册时，NameServer 在返回结果中告诉 Slave：
"你的 Master 在 xxx 地址，HA 同步地址是 yyy"。
Slave 收到后就知道该连接哪个地址进行数据同步了。

### 5.17 Step 14：通知 minBrokerId 变化

```java
} finally {
    this.lock.writeLock().unlock();
}

// Step 14: 通知 minBrokerId 变化（在锁外执行）
if (isMinBrokerIdChanged) {
    notifyMinBrokerIdChanged(brokerData.getBrokerAddrs());
}
```

**为什么在锁外执行通知？**

`notifyMinBrokerIdChanged()` 会通过网络通知其他 Broker（通过 RemotingClient 发送请求）。
如果在写锁内执行网络操作，可能会因为网络延迟或超时而长时间持有写锁，阻塞所有其他操作。

将网络通知放在锁外是一个**典型的降低锁粒度的优化**。虽然存在极短的数据不一致窗口，
但这种不一致是可接受的（最终一致性）。

### 5.18 注册流程完整总结

```
                     Broker 注册请求到达 NameServer
                              │
                              ▼
                    ┌── 获取写锁 ──┐
                    │              │
                    │  ┌───────────────────────────────────┐
                    │  │ 1. clusterAddrTable               │
                    │  │    添加 brokerName 到集群           │
                    │  │                                   │
                    │  │ 2. brokerAddrTable                │
                    │  │    创建/获取 BrokerData             │
                    │  │                                   │
                    │  │ 3. 设置 enableActingMaster/zone   │
                    │  │                                   │
                    │  │ 4. 记录旧 minBrokerId             │
                    │  │                                   │
                    │  │ 5. 去重同地址节点                   │
                    │  │                                   │
                    │  │ 6. stateVersion 冲突检查           │
                    │  │    (高版本优先)                     │
                    │  │                                   │
                    │  │ 7. 拒绝不完整的单 Topic 注册        │
                    │  │                                   │
                    │  │ 8. 写入 brokerAddrs               │
                    │  │                                   │
                    │  │ 9. 判断 Master/PrimeSlave          │
                    │  │                                   │
                    │  │ 10. 更新 topicQueueTable           │
                    │  │     (Master/PrimeSlave 才更新)     │
                    │  │                                   │
                    │  │ 11. 更新 brokerLiveTable            │
                    │  │     (刷新心跳时间戳)                 │
                    │  │                                   │
                    │  │ 12. 更新 filterServerTable          │
                    │  │                                   │
                    │  │ 13. 返回 Master 信息(给 Slave)      │
                    │  └───────────────────────────────────┘
                    │              │
                    └── 释放写锁 ──┘
                              │
                              ▼
                    14. 通知 minBrokerId 变化
                         (锁外执行，避免阻塞)
```

---

## 六、路由发现全链路 (pickupTopicRouteData)

> 源码文件：`RouteInfoManager.java`，方法 `pickupTopicRouteData()`，约 lines 700-801

路由发现是 NameServer 最核心的读操作。Producer 和 Consumer 在发送/消费消息前，
都需要先从 NameServer 获取 Topic 的路由信息。

### 6.1 方法签名

```java
// RouteInfoManager.java

public TopicRouteData pickupTopicRouteData(final String topic) {
    // ...
}
```

### 6.2 返回值：TopicRouteData

```java
public class TopicRouteData {
    private String orderTopicConf;                    // 顺序消息配置
    private List<QueueData> queueDatas;                // 队列数据列表
    private List<BrokerData> brokerDatas;              // Broker 数据列表
    private HashMap<String, List<String>> filterServerTable;  // FilterServer 表
    private Map<String, TopicQueueMappingInfo>
        topicQueueMappingByBroker;                     // 逻辑队列映射
}
```

`TopicRouteData` 是返回给客户端的路由信息结构，包含了客户端发送/消费消息所需的全部信息。

### 6.3 完整方法源码解析

```java
// RouteInfoManager.java

public TopicRouteData pickupTopicRouteData(final String topic) {
    TopicRouteData topicRouteData = new TopicRouteData();
    boolean foundQueueData = false;
    boolean foundBrokerData = false;

    // ========== 收集所有相关的 brokerName ==========
    List<BrokerData> brokerDataList = new LinkedList<>();
    Set<String> brokerNameSet = new HashSet<>();

    // 用于收集 FilterServer 信息
    HashMap<String, List<String>> filterServerMap = new HashMap<>();

    try {
        // ========== 获取读锁 ==========
        this.lock.readLock().lockInterruptibly();
        try {
            // ---------- Step 1: 查询 topicQueueTable ----------
            Map<String, QueueData> queueDataMap =
                this.topicQueueTable.get(topic);

            if (queueDataMap != null) {
                // 将 Map 的 values 转为 List
                topicRouteData.setQueueDatas(
                    new ArrayList<>(queueDataMap.values()));
                foundQueueData = true;

                // 收集所有涉及的 brokerName
                brokerNameSet.addAll(queueDataMap.keySet());

                // ---------- Step 2: 逐个查询 BrokerData ----------
                for (String brokerName : brokerNameSet) {
                    BrokerData brokerData =
                        this.brokerAddrTable.get(brokerName);
                    if (null != brokerData) {
                        // 深拷贝 BrokerData，避免并发修改
                        BrokerData bdClone = new BrokerData(
                            brokerData.getCluster(),
                            brokerData.getBrokerName(),
                            (HashMap<Long, String>)
                                brokerData.getBrokerAddrs().clone(),
                            brokerData.isEnableActingMaster(),
                            brokerData.getZoneName()
                        );
                        brokerDataList.add(bdClone);
                        foundBrokerData = true;

                        // ---------- Step 3: 查询 FilterServer ----------
                        // 遍历该 Broker 的所有地址
                        if (this.filterServerTable.isEmpty()) {
                            continue;
                        }
                        for (String brokerAddr
                             : brokerData.getBrokerAddrs().values()) {
                            BrokerAddrInfo addrInfo = new BrokerAddrInfo(
                                brokerData.getCluster(), brokerAddr);
                            List<String> filterServerList =
                                this.filterServerTable.get(addrInfo);
                            filterServerMap.put(brokerAddr,
                                filterServerList);
                        }
                    }
                }
            }
        } finally {
            this.lock.readLock().unlock();
        }
    } catch (Exception e) {
        log.error("pickupTopicRouteData Exception", e);
    }

    log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

    // ========== 组装返回结果 ==========
    if (foundBrokerData && foundQueueData) {
        topicRouteData.setBrokerDatas(brokerDataList);
        topicRouteData.setFilterServerTable(filterServerMap);

        // ---------- Step 4: 设置 topicQueueMappingByBroker ----------
        Map<String, TopicQueueMappingInfo> mappingInfoMap =
            this.topicQueueMappingInfoTable.get(topic);
        if (mappingInfoMap != null) {
            // 深拷贝映射信息
            topicRouteData.setTopicQueueMappingByBroker(
                new HashMap<>(mappingInfoMap));
        }

        // ---------- Step 5: Acting Master 提升逻辑 ----------
        if (!this.namesrvConfig.isSupportActingMaster()) {
            return topicRouteData;
        }

        // 如果启用了 Acting Master，检查是否需要提升 Slave
        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            if (brokerData.getBrokerAddrs() == null
                || brokerData.getBrokerAddrs().isEmpty()
                || !brokerData.isEnableActingMaster()) {
                continue;
            }

            // 如果没有 Master (brokerId=0)
            if (!brokerData.getBrokerAddrs().containsKey(
                    MixAll.MASTER_ID)) {
                // 找到最小 brokerId 的 Slave，提升为 Acting Master
                Long minBrokerId = Collections.min(
                    brokerData.getBrokerAddrs().keySet());
                String minBrokerAddr =
                    brokerData.getBrokerAddrs().get(minBrokerId);

                // 移除旧的条目，以 brokerId=0 重新添加
                brokerData.getBrokerAddrs().remove(minBrokerId);
                brokerData.getBrokerAddrs().put(
                    MixAll.MASTER_ID, minBrokerAddr);
            }
        }

        return topicRouteData;
    }

    // 如果没有找到任何数据，返回 null
    return null;
}
```

### 6.4 分步解析

#### Step 1：查询 topicQueueTable

```java
Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
```

以 topic 为 key 查询 `topicQueueTable`，获取该 Topic 在各个 Broker 上的队列配置。
如果返回 null，说明该 Topic 不存在（未注册过任何 Broker 拥有该 Topic）。

#### Step 2：收集 BrokerData

```java
for (String brokerName : brokerNameSet) {
    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
    if (null != brokerData) {
        BrokerData bdClone = new BrokerData(
            brokerData.getCluster(),
            brokerData.getBrokerName(),
            (HashMap<Long, String>) brokerData.getBrokerAddrs().clone(),
            // ...
        );
        brokerDataList.add(bdClone);
    }
}
```

**关键设计：深拷贝**。这里对 BrokerData 进行了 clone，特别是 `brokerAddrs` 这个 Map。
为什么？因为读锁释放后，原始数据可能被写操作修改。深拷贝确保返回给客户端的数据是
一个一致的快照，不会被后续的写操作影响。

#### Step 3：收集 FilterServer 信息

```java
if (this.filterServerTable.isEmpty()) {
    continue;  // 快速跳过，减少不必要的遍历
}
for (String brokerAddr : brokerData.getBrokerAddrs().values()) {
    BrokerAddrInfo addrInfo = new BrokerAddrInfo(
        brokerData.getCluster(), brokerAddr);
    List<String> filterServerList =
        this.filterServerTable.get(addrInfo);
    filterServerMap.put(brokerAddr, filterServerList);
}
```

优先判断 `filterServerTable.isEmpty()` 快速跳过——这是一个性能优化，
因为大部分部署环境中不使用 FilterServer。

#### Step 4：设置逻辑队列映射

```java
Map<String, TopicQueueMappingInfo> mappingInfoMap =
    this.topicQueueMappingInfoTable.get(topic);
if (mappingInfoMap != null) {
    topicRouteData.setTopicQueueMappingByBroker(
        new HashMap<>(mappingInfoMap));
}
```

如果是 Static Topic，需要附带逻辑队列映射信息。客户端会根据这个映射
来确定逻辑队列对应的物理 Broker 和物理队列。

#### Step 5：Acting Master 提升

```java
if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
    Long minBrokerId = Collections.min(
        brokerData.getBrokerAddrs().keySet());
    String minBrokerAddr =
        brokerData.getBrokerAddrs().get(minBrokerId);
    brokerData.getBrokerAddrs().remove(minBrokerId);
    brokerData.getBrokerAddrs().put(
        MixAll.MASTER_ID, minBrokerAddr);
}
```

**Acting Master 的核心逻辑**：

当一个 BrokerData 中没有 brokerId=0 的 Master 时（Master 宕机了），
并且该 Broker 启用了 `enableActingMaster`，则在返回给客户端的路由数据中，
将最小 brokerId 的 Slave "提升" 为 Master（brokerId=0）。

**注意：这只是路由层面的提升**，是在返回给客户端的 clone 数据上做的修改，
并不会修改 NameServer 本身的路由表。实际的数据同步和一致性由 Broker 端保证。

### 6.5 路由查询流程图

```
   Client (Producer/Consumer)           NameServer
        │                                  │
        │  GET_ROUTEINFO_BY_TOPIC          │
        │  {topic: "TopicA"}               │
        │─────────────────────────────────>│
        │                                  │
        │                          ┌───────┴───────┐
        │                          │   读锁内操作    │
        │                          │               │
        │                          │ 1. topicQueueTable
        │                          │    .get("TopicA")
        │                          │    → QueueData[]
        │                          │               │
        │                          │ 2. 收集 brokerNames
        │                          │    → {broker-a,
        │                          │       broker-b}
        │                          │               │
        │                          │ 3. brokerAddrTable
        │                          │    逐个查 BrokerData
        │                          │    (深拷贝!)
        │                          │               │
        │                          │ 4. filterServerTable
        │                          │    (通常为空)
        │                          │               │
        │                          └───────┬───────┘
        │                                  │
        │                          5. 设置 mapping
        │                          6. Acting Master
        │                             提升
        │                                  │
        │  TopicRouteData {                │
        │    queueDatas: [...],             │
        │    brokerDatas: [...],            │
        │    filterServerTable: {...},      │
        │    topicQueueMappingByBroker: {}  │
        │  }                               │
        │<─────────────────────────────────│
        │                                  │
```

---

## 七、Broker注销全链路

> 源码文件：`RouteInfoManager.java`，方法 `unRegisterBroker()` 系列

### 7.1 注销触发场景

Broker 注销有三种触发方式：

1. **主动注销**：Broker 正常关闭时，向所有 NameServer 发送 `UNREGISTER_BROKER` 请求
2. **心跳超时**：NameServer 定时扫描 `brokerLiveTable`，发现超过 120 秒没有心跳的 Broker
3. **连接断开**：Broker 与 NameServer 的 TCP 连接异常断开，触发 `BrokerHousekeepingService`

### 7.2 简单注销方法（单个注销委托到批量）

```java
// RouteInfoManager.java

public void unRegisterBroker(
    final String clusterName,
    final String brokerAddr,
    final String brokerName,
    final long brokerId
) {
    // 构造注销请求
    UnRegisterBrokerRequestHeader unRegisterRequest =
        new UnRegisterBrokerRequestHeader();
    unRegisterRequest.setClusterName(clusterName);
    unRegisterRequest.setBrokerAddr(brokerAddr);
    unRegisterRequest.setBrokerName(brokerName);
    unRegisterRequest.setBrokerId(brokerId);

    // 委托给批量注销服务
    this.unRegisterService.submit(unRegisterRequest);
}
```

单个注销请求不是直接执行，而是提交给 `BatchUnregistrationService`。

### 7.3 批量注销方法

```java
// RouteInfoManager.java

public void unRegisterBroker(
    Set<UnRegisterBrokerRequestHeader> unRegisterRequests
) {
    try {
        Set<String> removedBrokerNames = new HashSet<>();
        Set<BrokerAddrInfo> needRemoveBrokerAddrInfos = new HashSet<>();

        // ========== 获取写锁 ==========
        this.lock.writeLock().lockInterruptibly();
        try {
            for (UnRegisterBrokerRequestHeader request
                 : unRegisterRequests) {

                String clusterName = request.getClusterName();
                String brokerAddr = request.getBrokerAddr();
                String brokerName = request.getBrokerName();
                long brokerId = request.getBrokerId();

                BrokerAddrInfo brokerAddrInfo =
                    new BrokerAddrInfo(clusterName, brokerAddr);

                // ---- Step 1: 从 brokerLiveTable 移除 ----
                BrokerLiveInfo brokerLiveInfo =
                    this.brokerLiveTable.remove(brokerAddrInfo);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                    brokerLiveInfo != null ? "OK" : "FAIL", brokerAddrInfo);

                // ---- Step 2: 从 filterServerTable 移除 ----
                this.filterServerTable.remove(brokerAddrInfo);

                // ---- Step 3: 从 brokerAddrTable 移除 ----
                boolean removeBrokerName = false;
                BrokerData brokerData =
                    this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    String addr = brokerData.getBrokerAddrs()
                        .remove(brokerId);
                    log.info("unregisterBroker, remove addr from " +
                        "brokerAddrTable {}, {}",
                        addr != null ? "OK" : "FAIL",
                        brokerAddrInfo);

                    // 如果该 brokerName 下没有任何地址了，移除整个 BrokerData
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from " +
                            "brokerAddrTable OK, {}", brokerName);
                        removeBrokerName = true;
                    }
                }

                // ---- Step 4: 从 clusterAddrTable 移除 ----
                if (removeBrokerName) {
                    Set<String> nameSet =
                        this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from " +
                            "clusterAddrTable {}, {}",
                            removed ? "OK" : "FAIL", brokerName);
                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster" +
                                " from clusterAddrTable {}", clusterName);
                        }
                    }
                    // 记录需要清理 Topic 的 broker
                    removedBrokerNames.add(brokerName);
                }
            }

            // ---- Step 5: 清理 topicQueueTable ----
            cleanTopicByUnRegisterRequests(
                removedBrokerNames, unRegisterRequests);

        } finally {
            this.lock.writeLock().unlock();
        }
    } catch (Exception e) {
        log.error("unRegisterBroker Exception", e);
    }
}
```

### 7.4 清理 topicQueueTable 的逻辑

```java
// RouteInfoManager.java

private void cleanTopicByUnRegisterRequests(
    Set<String> removedBrokerNames,
    Set<UnRegisterBrokerRequestHeader> unRegisterRequests
) {
    // 遍历所有 Topic
    Iterator<Map.Entry<String, Map<String, QueueData>>> itMap =
        this.topicQueueTable.entrySet().iterator();

    while (itMap.hasNext()) {
        Map.Entry<String, Map<String, QueueData>> entry = itMap.next();
        String topic = entry.getKey();
        Map<String, QueueData> queueDataMap = entry.getValue();

        for (String brokerName : removedBrokerNames) {
            // 移除该 Broker 在该 Topic 下的 QueueData
            QueueData removedQueueData = queueDataMap.remove(brokerName);
            if (removedQueueData != null) {
                log.info("cleanTopicByUnRegisterRequests remove topic: {}, " +
                    "brokerName: {}, queueData: {}",
                    topic, brokerName, removedQueueData);
            }
        }

        // 如果该 Topic 下没有任何 Broker 的队列了，移除整个 Topic
        if (queueDataMap.isEmpty()) {
            log.info("cleanTopicByUnRegisterRequests remove topic: {}", topic);
            itMap.remove();
        }
    }
}
```

**注销的清理顺序**：

```
  注销流程的数据清理顺序
  ┌──────────────────────────────┐
  │ 1. brokerLiveTable.remove() │  ← 先移除存活信息（停止心跳检测）
  │ 2. filterServerTable.remove()│  ← 移除 FilterServer
  │ 3. brokerAddrTable           │  ← 移除地址映射
  │    .get(name).addrs.remove() │
  │    如果 addrs 空了:           │
  │      brokerAddrTable         │
  │        .remove(name)         │
  │ 4. clusterAddrTable          │  ← 从集群中移除 Broker 名称
  │    .get(cluster).remove(name)│
  │    如果集群空了:               │
  │      clusterAddrTable        │
  │        .remove(cluster)      │
  │ 5. topicQueueTable           │  ← 最后清理 Topic 路由
  │    对每个 Topic 移除该 Broker  │
  │    如果 Topic 下没有 Broker:   │
  │      移除整个 Topic            │
  └──────────────────────────────┘
```

### 7.5 BatchUnregistrationService 详解

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/BatchUnregistrationService.java`（约82行）

```java
// BatchUnregistrationService.java

public class BatchUnregistrationService extends ServiceThread {

    private final RouteInfoManager routeInfoManager;
    private final BlockingQueue<UnRegisterBrokerRequestHeader>
        unRegistrationQueue;

    public BatchUnregistrationService(
            RouteInfoManager routeInfoManager,
            NamesrvConfig namesrvConfig) {
        this.routeInfoManager = routeInfoManager;
        // 队列容量可配置，默认 1000
        this.unRegistrationQueue = new LinkedBlockingQueue<>(
            namesrvConfig.getUnRegisterBrokerQueueCapacity());
    }

    // 提交注销请求
    public void submit(UnRegisterBrokerRequestHeader unRegisterRequest) {
        this.unRegistrationQueue.offer(unRegisterRequest);
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 阻塞等待第一个注销请求
                UnRegisterBrokerRequestHeader first =
                    unRegistrationQueue.take();

                // 尝试批量获取更多请求
                Set<UnRegisterBrokerRequestHeader> requests =
                    new HashSet<>();
                requests.add(first);
                unRegistrationQueue.drainTo(requests);

                // 批量执行注销
                this.routeInfoManager.unRegisterBroker(requests);
            } catch (InterruptedException e) {
                log.warn("BatchUnregistrationService interrupted");
            } catch (Exception e) {
                log.error("BatchUnregistrationService error", e);
            }
        }
    }

    @Override
    public String getServiceName() {
        return "BatchUnregistrationService";
    }
}
```

**`take() + drainTo()` 模式的精妙之处**：

```
                 ┌─────────────────────────────────────────┐
                 │     BatchUnregistrationService          │
                 │                                         │
  submit(req1)──►│  ┌──────────────────────┐               │
  submit(req2)──►│  │  unRegistrationQueue │               │
  submit(req3)──►│  │  [req1, req2, req3]  │               │
                 │  └──────────┬───────────┘               │
                 │             │                           │
                 │             ▼                           │
                 │  take() ──► req1  (阻塞等待第一个)       │
                 │  drainTo() ──► {req2, req3} (非阻塞批取) │
                 │             │                           │
                 │             ▼                           │
                 │  routeInfoManager.unRegisterBroker(     │
                 │    {req1, req2, req3}                    │
                 │  )                                      │
                 │  ↑ 只获取一次写锁！                      │
                 │                                         │
                 └─────────────────────────────────────────┘
```

1. `take()` 阻塞等待——当没有注销请求时，线程挂起，不消耗 CPU
2. 当第一个请求到达，`take()` 返回
3. `drainTo()` 非阻塞地取出队列中所有剩余的请求
4. 将所有请求合并为一个批次，只获取一次写锁就完成所有注销操作

**为什么要批量化？**

每次注销都需要获取写锁，写锁会阻塞所有读操作（路由查询）。
如果短时间内多个 Broker 同时注销（例如整个机房掉电），逐个注销会导致写锁频繁获取释放，
严重影响路由查询的响应时间。批量化可以将多次写锁获取合并为一次，大幅减少锁竞争。

---

## 八、心跳与存活检测

### 8.1 scanNotActiveBroker() 方法

> 源码文件：`RouteInfoManager.java`

```java
// RouteInfoManager.java

public void scanNotActiveBroker() {
    try {
        log.info("scan not active broker begin, size = {}",
            this.brokerLiveTable.size());

        // 遍历 brokerLiveTable 中的所有条目
        for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry
             : this.brokerLiveTable.entrySet()) {

            BrokerAddrInfo brokerAddrInfo = entry.getKey();
            BrokerLiveInfo brokerLiveInfo = entry.getValue();

            // 计算距离上次心跳的时间间隔
            long last = brokerLiveInfo.getLastUpdateTimestamp();
            long timeoutMillis = brokerLiveInfo.getHeartbeatTimeoutMillis();

            if ((last + timeoutMillis) < System.currentTimeMillis()) {
                // 超时了！
                log.warn("The broker channel expired, {} {}ms",
                    brokerAddrInfo, timeoutMillis);

                // 关闭 Netty Channel
                RemotingHelper.closeChannel(
                    brokerLiveInfo.getChannel());

                // 触发 Channel 销毁处理
                this.onChannelDestroy(brokerAddrInfo);
            }
        }
    } catch (Exception e) {
        log.error("scanNotActiveBroker exception", e);
    }
}
```

**超时判断公式**：
```
如果 (lastUpdateTimestamp + heartbeatTimeoutMillis) < currentTimeMillis
则认为 Broker 已超时
```

默认超时时间是 120000ms（2分钟）。也就是说，如果 Broker 超过 2 分钟没有发送心跳，
NameServer 就会认为它已经宕机，并清理其路由信息。

**执行频率**：由 `startScheduleService()` 中的定时任务控制，
默认每 `scanNotActiveBrokerInterval`（5000ms，即5秒）执行一次。

**注意**：`scanNotActiveBroker()` 不直接获取写锁。它只是遍历 `brokerLiveTable`（读操作），
找到超时的 Broker 后调用 `onChannelDestroy()` → `unRegisterBroker()`，
写锁在 `unRegisterBroker()` 中获取。

### 8.2 onChannelDestroy() 方法

```java
// RouteInfoManager.java

// 重载1：通过 BrokerAddrInfo 触发（定时扫描超时时调用）
public void onChannelDestroy(BrokerAddrInfo brokerAddrInfo) {
    UnRegisterBrokerRequestHeader unRegisterRequest =
        setupUnRegisterRequest(brokerAddrInfo);
    if (unRegisterRequest != null) {
        this.unRegisterService.submit(unRegisterRequest);
    }
}

// 重载2：通过 Channel 触发（BrokerHousekeepingService 回调时调用）
public void onChannelDestroy(Channel channel) {
    // 遍历 brokerLiveTable 找到对应的 BrokerAddrInfo
    BrokerAddrInfo brokerAddrFound = null;
    for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry
         : this.brokerLiveTable.entrySet()) {
        if (entry.getValue().getChannel() == channel) {
            brokerAddrFound = entry.getKey();
            break;
        }
    }

    if (brokerAddrFound != null) {
        UnRegisterBrokerRequestHeader unRegisterRequest =
            setupUnRegisterRequest(brokerAddrFound);
        if (unRegisterRequest != null) {
            this.unRegisterService.submit(unRegisterRequest);
        }
    }
}
```

两个重载的区别：
- 重载1：已知 `BrokerAddrInfo`（定时扫描场景），直接构造注销请求
- 重载2：只知道 `Channel`（连接断开场景），需要先通过遍历 `brokerLiveTable` 找到对应的 `BrokerAddrInfo`

### 8.3 setupUnRegisterRequest() 方法

```java
// RouteInfoManager.java

private UnRegisterBrokerRequestHeader setupUnRegisterRequest(
    BrokerAddrInfo brokerAddrInfo
) {
    // 从 brokerAddrTable 中查找该地址对应的 brokerName 和 brokerId
    for (Map.Entry<String, BrokerData> entry
         : this.brokerAddrTable.entrySet()) {
        BrokerData brokerData = entry.getValue();
        for (Map.Entry<Long, String> addrEntry
             : brokerData.getBrokerAddrs().entrySet()) {
            if (addrEntry.getValue().equals(
                    brokerAddrInfo.getBrokerAddr())) {
                // 找到了！构造注销请求
                UnRegisterBrokerRequestHeader request =
                    new UnRegisterBrokerRequestHeader();
                request.setClusterName(
                    brokerAddrInfo.getClusterName());
                request.setBrokerAddr(
                    brokerAddrInfo.getBrokerAddr());
                request.setBrokerName(entry.getKey());
                request.setBrokerId(addrEntry.getKey());
                return request;
            }
        }
    }
    return null;
}
```

这个方法的作用是：给定一个 Broker 地址，反查出它的 `brokerName` 和 `brokerId`，
然后构造完整的注销请求。这需要遍历 `brokerAddrTable`，因为没有从地址到名称的反向索引。

### 8.4 BrokerHousekeepingService 详解

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/routeinfo/BrokerHousekeepingService.java`（约54行）

```java
// BrokerHousekeepingService.java

public class BrokerHousekeepingService
    implements ChannelEventListener {

    private static final Logger log = LoggerFactory.getLogger(
        LoggerName.NAMESRV_LOGGER_NAME);
    private final NamesrvController namesrvController;

    public BrokerHousekeepingService(
            NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
        // 连接建立时不做任何事
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        // 连接关闭时，触发路由清理
        this.namesrvController.getRouteInfoManager()
            .onChannelDestroy(channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        // 连接异常时，触发路由清理
        this.namesrvController.getRouteInfoManager()
            .onChannelDestroy(channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        // 连接空闲超时时，触发路由清理
        this.namesrvController.getRouteInfoManager()
            .onChannelDestroy(channel);
    }
}
```

`BrokerHousekeepingService` 是一个"管家"角色，职责非常单一：
监听 Broker 与 NameServer 之间的 TCP 连接状态变化，在连接断开时触发路由清理。

**三种触发场景**：
1. `onChannelClose()`：Broker 主动关闭连接或 TCP FIN
2. `onChannelException()`：连接出现异常（如 RST）
3. `onChannelIdle()`：连接空闲超时（由 Netty IdleStateHandler 触发）

无论哪种场景，处理方式都是一样的——调用 `onChannelDestroy()` 清理路由。

### 8.5 三种心跳机制对比

RocketMQ 5.x 中 Broker 向 NameServer 保持存活有三种方式：

```
┌───────────────────────────────────────────────────────────────────────┐
│                  三种心跳机制对比                                      │
├─────────────────┬────────────────────┬────────────────────────────────┤
│ 请求码           │ 说明               │ 特点                           │
├─────────────────┼────────────────────┼────────────────────────────────┤
│ REGISTER_BROKER │ 完整注册请求        │ 携带所有 Topic 配置            │
│                 │ (传统心跳方式)       │ 数据量大，但能同步全量信息      │
│                 │                    │ 默认每 30 秒一次               │
├─────────────────┼────────────────────┼────────────────────────────────┤
│ BROKER_HEARTBEAT│ 轻量心跳请求        │ 只更新 lastUpdateTimestamp     │
│                 │ (5.x 新增)          │ 数据量极小                     │
│                 │                    │ 用于频繁的存活检测              │
├─────────────────┼────────────────────┼────────────────────────────────┤
│ QUERY_DATA_     │ 数据版本查询        │ 比较 DataVersion               │
│ VERSION         │                    │ 如果版本不同，Broker 会发起     │
│                 │                    │ 完整注册来同步变化              │
│                 │                    │ 实现增量同步的效果              │
└─────────────────┴────────────────────┴────────────────────────────────┘
```

**三种方式的协作模式**：

```
Broker                                       NameServer
  │                                              │
  │  REGISTER_BROKER (首次，全量)                  │
  │─────────────────────────────────────────────>│  更新所有路由表
  │                                              │
  │  ... 30秒后 ...                               │
  │                                              │
  │  BROKER_HEARTBEAT (轻量心跳)                   │
  │─────────────────────────────────────────────>│  只更新时间戳
  │                                              │
  │  ... 10秒后 ...                               │
  │                                              │
  │  BROKER_HEARTBEAT (轻量心跳)                   │
  │─────────────────────────────────────────────>│  只更新时间戳
  │                                              │
  │  ... Broker 创建了新 Topic ...                 │
  │                                              │
  │  REGISTER_BROKER (全量，同步新 Topic)           │
  │─────────────────────────────────────────────>│  更新 topicQueueTable
  │                                              │
  │  ... 或者用 QUERY_DATA_VERSION 检查版本 ...     │
  │                                              │
  │  QUERY_DATA_VERSION                           │
  │─────────────────────────────────────────────>│  比较版本号
  │                                              │  返回：版本不同
  │  REGISTER_BROKER (全量同步)                    │
  │─────────────────────────────────────────────>│  更新路由表
  │                                              │
```

这种设计兼顾了两个需求：
1. **高频存活检测**：通过轻量心跳频繁检测 Broker 是否存活
2. **低开销数据同步**：通过版本号比较，避免每次心跳都传输全量 Topic 配置

---

## 九、DefaultRequestProcessor详解

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/processor/DefaultRequestProcessor.java`（约680行）

### 9.1 类定义

```java
// DefaultRequestProcessor.java

public class DefaultRequestProcessor
    extends AsyncNettyRequestProcessor
    implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(
        LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(
            ChannelHandlerContext ctx,
            RemotingCommand request) throws RemotingCommandException {
        // ... 请求分发逻辑 ...
    }
}
```

### 9.2 请求分发：processRequest()

`processRequest()` 方法通过 `switch` 语句将不同的请求码路由到对应的处理方法。
这是一个典型的**命令模式（Command Pattern）**实现：

```java
// DefaultRequestProcessor.java

@Override
public RemotingCommand processRequest(
        ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {

    if (ctx != null) {
        log.debug("receive request, {} {} {}",
            request.getCode(),
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
            request);
    }

    switch (request.getCode()) {
        case RequestCode.PUT_KV_CONFIG:
            return this.putKVConfig(ctx, request);
        case RequestCode.GET_KV_CONFIG:
            return this.getKVConfig(ctx, request);
        case RequestCode.DELETE_KV_CONFIG:
            return this.deleteKVConfig(ctx, request);
        case RequestCode.QUERY_DATA_VERSION:
            return this.queryBrokerTopicConfig(ctx, request);
        case RequestCode.REGISTER_BROKER:
            return this.registerBroker(ctx, request);
        case RequestCode.UNREGISTER_BROKER:
            return this.unregisterBroker(ctx, request);
        case RequestCode.BROKER_HEARTBEAT:
            return this.brokerHeartbeat(ctx, request);
        case RequestCode.GET_BROKER_MEMBER_GROUP:
            return this.getBrokerMemberGroup(ctx, request);
        case RequestCode.GET_BROKER_CLUSTER_INFO:
            return this.getBrokerClusterInfo(ctx, request);
        case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
            return this.wipeWritePermOfBroker(ctx, request);
        case RequestCode.ADD_WRITE_PERM_OF_BROKER:
            return this.addWritePermOfBroker(ctx, request);
        case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
            return this.getAllTopicListFromNameserver(ctx, request);
        case RequestCode.DELETE_TOPIC_IN_NAMESRV:
            return this.deleteTopicInNamesrv(ctx, request);
        case RequestCode.REGISTER_TOPIC_IN_NAMESRV:
            return this.registerTopicToNamesrv(ctx, request);
        case RequestCode.GET_KVLIST_BY_NAMESPACE:
            return this.getKVListByNamespace(ctx, request);
        case RequestCode.GET_TOPICS_BY_CLUSTER:
            return this.getTopicsByCluster(ctx, request);
        case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
            return this.getSystemTopicListFromNs(ctx, request);
        case RequestCode.GET_UNIT_TOPIC_LIST:
            return this.getUnitTopicList(ctx, request);
        case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
            return this.getHasUnitSubTopicList(ctx, request);
        case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
            return this.getHasUnitSubUnUnitTopicList(ctx, request);
        case RequestCode.UPDATE_NAMESRV_CONFIG:
            return this.updateConfig(ctx, request);
        case RequestCode.GET_NAMESRV_CONFIG:
            return this.getConfig(ctx, request);
        default:
            String error = " request type " + request.getCode()
                + " not supported";
            return RemotingCommand.createResponseCommand(
                RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                error);
    }
}
```

### 9.3 所有请求码汇总

| 序号 | 请求码 | 处理方法 | 说明 |
|------|--------|----------|------|
| 1 | `PUT_KV_CONFIG` | `putKVConfig()` | 写入 KV 配置 |
| 2 | `GET_KV_CONFIG` | `getKVConfig()` | 读取 KV 配置 |
| 3 | `DELETE_KV_CONFIG` | `deleteKVConfig()` | 删除 KV 配置 |
| 4 | `QUERY_DATA_VERSION` | `queryBrokerTopicConfig()` | 查询 Broker 数据版本 |
| 5 | `REGISTER_BROKER` | `registerBroker()` | Broker 注册（核心） |
| 6 | `UNREGISTER_BROKER` | `unregisterBroker()` | Broker 注销 |
| 7 | `BROKER_HEARTBEAT` | `brokerHeartbeat()` | Broker 轻量心跳 |
| 8 | `GET_BROKER_MEMBER_GROUP` | `getBrokerMemberGroup()` | 获取 Broker 成员组 |
| 9 | `GET_BROKER_CLUSTER_INFO` | `getBrokerClusterInfo()` | 获取集群信息 |
| 10 | `WIPE_WRITE_PERM_OF_BROKER` | `wipeWritePermOfBroker()` | 擦除 Broker 写权限 |
| 11 | `ADD_WRITE_PERM_OF_BROKER` | `addWritePermOfBroker()` | 恢复 Broker 写权限 |
| 12 | `GET_ALL_TOPIC_LIST_FROM_NAMESERVER` | `getAllTopicListFromNameserver()` | 获取所有 Topic 列表 |
| 13 | `DELETE_TOPIC_IN_NAMESRV` | `deleteTopicInNamesrv()` | 从 NameServer 删除 Topic |
| 14 | `REGISTER_TOPIC_IN_NAMESRV` | `registerTopicToNamesrv()` | 向 NameServer 注册 Topic |
| 15 | `GET_KVLIST_BY_NAMESPACE` | `getKVListByNamespace()` | 按命名空间获取 KV 列表 |
| 16 | `GET_TOPICS_BY_CLUSTER` | `getTopicsByCluster()` | 按集群获取 Topic 列表 |
| 17 | `GET_SYSTEM_TOPIC_LIST_FROM_NS` | `getSystemTopicListFromNs()` | 获取系统 Topic 列表 |
| 18 | `GET_UNIT_TOPIC_LIST` | `getUnitTopicList()` | 获取单元化 Topic 列表 |
| 19 | `GET_HAS_UNIT_SUB_TOPIC_LIST` | `getHasUnitSubTopicList()` | 获取有单元化订阅的 Topic |
| 20 | `GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST` | `getHasUnitSubUnUnitTopicList()` | 获取非单元化但有单元化订阅的 Topic |
| 21 | `UPDATE_NAMESRV_CONFIG` | `updateConfig()` | 动态更新 NameServer 配置 |
| 22 | `GET_NAMESRV_CONFIG` | `getConfig()` | 获取 NameServer 配置 |

### 9.4 registerBroker() 处理器详解

`registerBroker()` 是 DefaultRequestProcessor 中最复杂、最核心的处理方法。
它负责处理 Broker 发来的 `REGISTER_BROKER` 请求。

```java
// DefaultRequestProcessor.java

public RemotingCommand registerBroker(ChannelHandlerContext ctx,
                                      RemotingCommand request)
    throws RemotingCommandException {

    // ========== 第一步：解码请求头 ==========
    final RemotingCommand response = RemotingCommand.createResponseCommand(
        RegisterBrokerResponseHeader.class);
    final RegisterBrokerResponseHeader responseHeader =
        (RegisterBrokerResponseHeader) response.readCustomHeader();
    final RegisterBrokerRequestHeader requestHeader =
        (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(
            RegisterBrokerRequestHeader.class);

    // ========== 第二步：CRC32 数据校验 ==========
    if (!checksum(ctx, request, requestHeader)) {
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("crc32 not match");
        return response;
    }

    // ========== 第三步：解码请求体（RegisterBrokerBody）==========
    RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

    if (request.getBody() != null) {
        try {
            registerBrokerBody = RegisterBrokerBody.decode(
                request.getBody(),
                requestHeader.isCompressed());
        } catch (Exception e) {
            throw new RemotingCommandException(
                "Failed to decode RegisterBrokerBody", e);
        }
    } else {
        registerBrokerBody.getTopicConfigSerializeWrapper()
            .getDataVersion()
            .setCounter(new AtomicLong(0));
        registerBrokerBody.getTopicConfigSerializeWrapper()
            .getDataVersion()
            .setTimestamp(0);
    }

    // ========== 第四步：调用 RouteInfoManager.registerBroker() ==========
    RegisterBrokerResult result =
        this.namesrvController.getRouteInfoManager().registerBroker(
            requestHeader.getClusterName(),
            requestHeader.getBrokerAddr(),
            requestHeader.getBrokerName(),
            requestHeader.getBrokerId(),
            requestHeader.getHaServerAddr(),
            request.getExtFields().get(MixAll.ZONE_NAME),
            requestHeader.getHeartbeatTimeoutMillis(),
            requestHeader.getEnableActingMaster(),
            registerBrokerBody.getTopicConfigSerializeWrapper(),
            registerBrokerBody.getFilterServerList(),
            ctx.channel()
        );

    // ========== 第五步：构造响应 ==========
    responseHeader.setHaServerAddr(result.getHaServerAddr());
    responseHeader.setMasterAddr(result.getMasterAddr());

    // 如果 NameServer 需要返回顺序 Topic 配置
    if (this.namesrvController.getNamesrvConfig()
            .isReturnOrderTopicConfigToBroker()) {
        byte[] jsonValue = this.namesrvController
            .getKvConfigManager()
            .getKVListByNamespace(
                NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        response.setBody(jsonValue);
    }

    response.setCode(ResponseCode.SUCCESS);
    response.setRemark(null);
    return response;
}
```

#### CRC32 校验的重要性

```java
// DefaultRequestProcessor.java

private boolean checksum(ChannelHandlerContext ctx,
                         RemotingCommand request,
                         RegisterBrokerRequestHeader requestHeader) {
    if (requestHeader.getBodyCrc32() != 0) {
        final int crc32 = UtilAll.crc32(request.getBody());
        if (crc32 != requestHeader.getBodyCrc32()) {
            log.warn(String.format(
                "receive registerBroker request, but crc32 not match, " +
                "from %s",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel())));
            return false;
        }
    }
    return true;
}
```

CRC32 校验确保 Broker 注册请求的数据在网络传输过程中没有被损坏。
请求头中包含请求体的 CRC32 值，NameServer 收到后重新计算 CRC32 并比较。
如果不匹配，说明数据在传输中被篡改或损坏，拒绝该请求。

**为什么 `bodyCrc32 == 0` 时不校验？**

这是为了向后兼容旧版本的 Broker——旧版本可能不计算 CRC32，此时 `bodyCrc32` 为 0。

#### RegisterBrokerBody 解码

```java
RegisterBrokerBody registerBrokerBody = RegisterBrokerBody.decode(
    request.getBody(),
    requestHeader.isCompressed());
```

`RegisterBrokerBody` 包含两个核心数据：
1. `TopicConfigSerializeWrapper`：所有 Topic 配置 + DataVersion
2. `filterServerList`：FilterServer 地址列表

当 `isCompressed=true` 时，请求体经过了压缩（通常使用 GZIP），需要先解压再解码。
压缩对于拥有大量 Topic 的 Broker 非常有价值，可以显著减少网络传输量。

### 9.5 brokerHeartbeat() 处理器详解

```java
// DefaultRequestProcessor.java

public RemotingCommand brokerHeartbeat(ChannelHandlerContext ctx,
                                       RemotingCommand request)
    throws RemotingCommandException {

    final RemotingCommand response = RemotingCommand.createResponseCommand(null);
    final BrokerHeartbeatRequestHeader requestHeader =
        (BrokerHeartbeatRequestHeader) request.decodeCommandCustomHeader(
            BrokerHeartbeatRequestHeader.class);

    // 只更新心跳时间戳，不更新路由信息
    this.namesrvController.getRouteInfoManager()
        .updateBrokerInfoUpdateTimestamp(
            requestHeader.getClusterName(),
            requestHeader.getBrokerAddr()
        );

    response.setCode(ResponseCode.SUCCESS);
    response.setRemark(null);
    return response;
}
```

**与 REGISTER_BROKER 的区别**：

```
┌─────────────────────────────────────────────────────────────┐
│            REGISTER_BROKER vs BROKER_HEARTBEAT              │
├────────────────────────┬────────────────────────────────────┤
│    REGISTER_BROKER     │       BROKER_HEARTBEAT             │
├────────────────────────┼────────────────────────────────────┤
│ 携带全量 Topic 配置      │ 不携带任何业务数据                 │
│ 更新所有 5+1 张路由表     │ 只更新 brokerLiveTable 时间戳      │
│ 需要获取写锁             │ 不需要获取写锁（原子操作）          │
│ 数据量大、耗时较长        │ 数据量极小、耗时极短               │
│ 频率低（30秒一次）       │ 频率高（10-30秒一次）              │
└────────────────────────┴────────────────────────────────────┘
```

`BROKER_HEARTBEAT` 是 5.x 新增的轻量心跳机制，目的是**在不更新路由信息的前提下，
高频地证明 Broker 仍然存活**。这减少了 NameServer 获取写锁的频率，提高了整体性能。

### 9.6 queryBrokerTopicConfig() 处理器详解

```java
// DefaultRequestProcessor.java

public RemotingCommand queryBrokerTopicConfig(ChannelHandlerContext ctx,
                                              RemotingCommand request)
    throws RemotingCommandException {

    final RemotingCommand response = RemotingCommand.createResponseCommand(
        QueryDataVersionResponseHeader.class);
    final QueryDataVersionResponseHeader responseHeader =
        (QueryDataVersionResponseHeader) response.readCustomHeader();
    final QueryDataVersionRequestHeader requestHeader =
        (QueryDataVersionRequestHeader) request.decodeCommandCustomHeader(
            QueryDataVersionRequestHeader.class);

    // 解码请求中的 DataVersion
    DataVersion dataVersion = DataVersion.decode(
        request.getBody(), DataVersion.class);

    // 从 brokerLiveTable 中获取当前存储的 DataVersion
    BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(
        requestHeader.getClusterName(),
        requestHeader.getBrokerAddr());

    Boolean changed =
        this.namesrvController.getRouteInfoManager()
            .isBrokerTopicConfigChanged(
                brokerAddrInfo, dataVersion);

    // 不管版本是否变化，都更新心跳时间戳
    this.namesrvController.getRouteInfoManager()
        .updateBrokerInfoUpdateTimestamp(
            requestHeader.getClusterName(),
            requestHeader.getBrokerAddr());

    // 设置响应
    DataVersion nameServerDataVersion =
        this.namesrvController.getRouteInfoManager()
            .queryBrokerTopicConfig(brokerAddrInfo);

    responseHeader.setChanged(changed);
    response.setCode(ResponseCode.SUCCESS);
    response.setRemark(null);

    if (nameServerDataVersion != null) {
        response.setBody(nameServerDataVersion.encode());
    }
    return response;
}
```

**QUERY_DATA_VERSION 的工作流程**：

```
  Broker                                    NameServer
    │                                           │
    │  QUERY_DATA_VERSION                       │
    │  {dataVersion: v5}                        │
    │──────────────────────────────────────────>│
    │                                           │
    │                                    比较版本号：
    │                                    NameServer 存的: v5
    │                                    Broker 传的: v5
    │                                    changed = false
    │                                           │
    │  Response: {changed: false}                │
    │<──────────────────────────────────────────│
    │                                           │
    │  (版本相同，不需要全量注册)                  │
    │                                           │
    │  === Broker 创建了新 Topic ===              │
    │                                           │
    │  QUERY_DATA_VERSION                       │
    │  {dataVersion: v6}                        │
    │──────────────────────────────────────────>│
    │                                           │
    │                                    比较版本号：
    │                                    NameServer 存的: v5
    │                                    Broker 传的: v6
    │                                    changed = true
    │                                           │
    │  Response: {changed: true}                 │
    │<──────────────────────────────────────────│
    │                                           │
    │  (版本不同！需要全量注册同步)                │
    │                                           │
    │  REGISTER_BROKER (全量)                    │
    │──────────────────────────────────────────>│
    │                                           │
```

这种"先查版本，再按需全量注册"的模式，实现了一种**伪增量同步**。
虽然每次同步仍然是全量数据，但避免了不必要的全量传输。

### 9.7 其他处理器简要说明

#### wipeWritePermOfBroker() — 擦除写权限

```java
// DefaultRequestProcessor.java

public RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx,
                                             RemotingCommand request)
    throws RemotingCommandException {
    // 解码请求头，获取 brokerName
    // 调用 routeInfoManager.wipeWritePermOfBrokerByLock(brokerName)
    // 返回被修改的 Topic 数量
}
```

运维场景：需要下线某个 Broker 时，先擦除其写权限，让 Producer 不再向它发送消息，
等存量消息消费完毕后再关闭 Broker。这实现了**优雅下线**。

#### addWritePermOfBroker() — 恢复写权限

```java
// DefaultRequestProcessor.java

public RemotingCommand addWritePermOfBroker(ChannelHandlerContext ctx,
                                            RemotingCommand request)
    throws RemotingCommandException {
    // wipeWritePermOfBroker 的逆操作
    // 调用 routeInfoManager.addWritePermOfBrokerByLock(brokerName)
}
```

#### deleteTopicInNamesrv() — 删除 Topic

```java
// DefaultRequestProcessor.java

public RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx,
                                            RemotingCommand request)
    throws RemotingCommandException {
    // 从 topicQueueTable 中删除指定 Topic
    // 从 topicQueueMappingInfoTable 中删除
    // 是管理操作，通常由 mqadmin 工具调用
}
```

#### getBrokerClusterInfo() — 获取集群信息

```java
// DefaultRequestProcessor.java

public RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx,
                                            RemotingCommand request) {
    // 构造 ClusterInfo 对象
    // 包含 brokerAddrTable 和 clusterAddrTable 的全量信息
    // 序列化后返回
    // 主要供运维工具和 Dashboard 使用
}
```

---

## 十、ClientRequestProcessor详解

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/processor/ClientRequestProcessor.java`（约103行）

### 10.1 类定义

```java
// ClientRequestProcessor.java

public class ClientRequestProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(
        LoggerName.NAMESRV_LOGGER_NAME);

    protected final NamesrvController namesrvController;

    // 启动保护期时长（毫秒）
    private long startupTimeMillis;
    // NameServer 启动多久后才开始接受路由查询（默认与 scanNotActiveBrokerInterval 相同）
    private long waitSecondsForService;

    public ClientRequestProcessor(
            final NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        this.startupTimeMillis = System.currentTimeMillis();
        this.waitSecondsForService =
            this.namesrvController.getNamesrvConfig()
                .getWaitSecondsForService();
    }
}
```

### 10.2 processRequest() 方法

```java
// ClientRequestProcessor.java

@Override
public RemotingCommand processRequest(
        final ChannelHandlerContext ctx,
        final RemotingCommand request) throws Exception {

    return this.getRouteInfoByTopic(ctx, request);
}
```

`ClientRequestProcessor` 只处理一种请求：`GET_ROUTEINFO_BY_TOPIC`。
对应的处理方法是 `getRouteInfoByTopic()`。

### 10.3 getRouteInfoByTopic() 方法

```java
// ClientRequestProcessor.java

public RemotingCommand getRouteInfoByTopic(
        final ChannelHandlerContext ctx,
        final RemotingCommand request) throws RemotingCommandException {

    final RemotingCommand response = RemotingCommand.createResponseCommand(null);

    // ========== 解码请求头 ==========
    final GetRouteInfoRequestHeader requestHeader =
        (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(
            GetRouteInfoRequestHeader.class);

    // ========== 启动保护期检查 ==========
    // 如果 NameServer 刚启动，还没有完成第一轮 Broker 心跳检测，
    // 返回的路由信息可能不完整，此时拒绝查询请求
    if (!isStartupService()) {
        log.warn("NameServer not ready. Request topic: {}",
            requestHeader.getTopic());
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("NameServer not ready");
        return response;
    }

    // ========== 查询路由信息 ==========
    TopicRouteData topicRouteData =
        this.namesrvController.getRouteInfoManager()
            .pickupTopicRouteData(requestHeader.getTopic());

    if (topicRouteData != null) {
        // 找到了路由信息
        // 如果启用了顺序消息，附加顺序消息配置
        if (this.namesrvController.getNamesrvConfig()
                .isOrderMessageEnable()) {
            String orderTopicConf =
                this.namesrvController.getKvConfigManager()
                    .getKVConfig(
                        NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                        requestHeader.getTopic());
            topicRouteData.setOrderTopicConf(orderTopicConf);
        }

        // 序列化并返回
        byte[] content = topicRouteData.encode();
        response.setBody(content);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    // 未找到路由信息
    response.setCode(ResponseCode.TOPIC_NOT_EXIST);
    response.setRemark("No topic route info in name server for the topic: "
        + requestHeader.getTopic()
        + ", maybe topic not created yet or the name server not ready."
        + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
    return response;
}
```

### 10.4 启动保护期机制

```java
// ClientRequestProcessor.java

private boolean isStartupService() {
    if (waitSecondsForService <= 0) {
        return true;  // 如果设置为 0 或负数，不启用保护期
    }
    // 检查 NameServer 是否已经启动了足够长的时间
    return System.currentTimeMillis() - startupTimeMillis
        >= waitSecondsForService;
}
```

**为什么需要启动保护期？**

```
                       NameServer 启动保护期
  
  时间线: ──────┬───────────────────────┬──────────────────────►
               │                       │
          NameServer 启动         保护期结束
               │                       │
               │◄─── waitSeconds ────►│
               │                       │
               │  拒绝路由查询          │  正常服务
               │  (路由表可能不完整)     │  (路由表已填充)
               │                       │
```

NameServer 刚启动时，路由表是空的。Broker 的注册请求需要一定时间才能到达。
如果在这个窗口期内 Producer/Consumer 查询路由，会得到"Topic 不存在"的错误，
导致客户端误以为 Topic 真的不存在而报错。

启动保护期让 NameServer 在启动后等待一段时间（等待 Broker 注册），
之后才开始接受路由查询请求。这避免了启动初期的虚假错误。

**默认值**：`waitSecondsForService` 默认与 `scanNotActiveBrokerInterval` 相同（5秒），
但在生产环境中通常会设置更长（如 45 秒），确保所有 Broker 都完成了注册。

### 10.5 专用线程池的意义

回顾 NamesrvController 中的线程池隔离设计：

```
                    ┌─────────────────────────────┐
                    │       NettyRemotingServer    │
                    │                             │
   来自 Client 的    │  GET_ROUTEINFO_BY_TOPIC     │
   路由查询请求 ────>│  → ClientRequestProcessor   │
                    │  → clientRequestExecutor    │ ← 专用线程池
                    │                             │
   来自 Broker 的    │  REGISTER_BROKER 等         │
   注册/心跳请求 ──>│  → DefaultRequestProcessor  │
                    │  → defaultExecutor          │ ← 默认线程池
                    │                             │
                    └─────────────────────────────┘
```

这种隔离确保了：
1. **Broker 注册风暴不影响路由查询**：即使有大量 Broker 同时注册
  （如集群重启），defaultExecutor 被打满，clientRequestExecutor 仍然正常工作
2. **路由查询风暴不影响 Broker 注册**：即使有大量客户端同时查询路由，
   不会影响 Broker 的注册和心跳处理
3. **故障隔离**：任一线程池出现问题（如线程泄漏），不会传染到另一个

---

## 十一、KVConfigManager详解

> 源码文件：`namesrv/src/main/java/org/apache/rocketmq/namesrv/kvconfig/KVConfigManager.java`（约194行）

### 11.1 类定义与数据结构

```java
// KVConfigManager.java

public class KVConfigManager {

    private static final Logger log = LoggerFactory.getLogger(
        LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvController namesrvController;

    // 核心数据结构：二级 HashMap
    // 第一级 key: namespace (命名空间)
    // 第二级 key: key (配置项名称)
    // value: 配置值
    private final HashMap<String/* Namespace */,
                           HashMap<String/* Key */,
                                   String/* Value */>> configTable =
        new HashMap<>();

    // 保护 configTable 的读写锁
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public KVConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }
}
```

**二级 HashMap 的设计**：

```
configTable: HashMap<String, HashMap<String, String>>
│
├── "ORDER_TOPIC_CONFIG" (命名空间)
│    ├── "TopicA" → "broker-a:8;broker-b:8"  (顺序消息配置)
│    ├── "TopicB" → "broker-a:4"
│    └── "TopicC" → "broker-c:16"
│
├── "UNIT_CONFIG" (命名空间)
│    ├── "key1" → "value1"
│    └── "key2" → "value2"
│
└── "CUSTOM_NAMESPACE" (自定义命名空间)
     └── "configKey" → "configValue"
```

使用命名空间（Namespace）来隔离不同用途的配置，避免 key 冲突。

### 11.2 load() — 从磁盘加载

```java
// KVConfigManager.java

public void load() {
    // 从磁盘读取 JSON 文件
    String content = null;
    try {
        content = MixAll.file2String(
            this.namesrvController.getNamesrvConfig().getKvConfigPath());
    } catch (IOException e) {
        log.warn("Load KV config table exception", e);
    }

    if (content != null) {
        // 反序列化 JSON → KVConfigSerializeWrapper
        KVConfigSerializeWrapper kvConfigSerializeWrapper =
            KVConfigSerializeWrapper.fromJson(
                content, KVConfigSerializeWrapper.class);

        if (null != kvConfigSerializeWrapper) {
            this.configTable.putAll(
                kvConfigSerializeWrapper.getConfigTable());
            log.info("load KV config table OK");
        }
    }
}
```

**持久化路径**：默认是 `{user.home}/namesrv/kvConfig.json`。
文件内容是 JSON 格式，示例：

```json
{
    "configTable": {
        "ORDER_TOPIC_CONFIG": {
            "TopicA": "broker-a:8;broker-b:8",
            "TopicB": "broker-a:4"
        }
    }
}
```

### 11.3 putKVConfig() — 写入配置

```java
// KVConfigManager.java

public void putKVConfig(final String namespace,
                        final String key,
                        final String value) {
    try {
        this.lock.writeLock().lockInterruptibly();
        try {
            // 获取或创建命名空间对应的 Map
            HashMap<String, String> kvTable =
                this.configTable.computeIfAbsent(
                    namespace, k -> new HashMap<>());
            // 写入配置
            String prev = kvTable.put(key, value);
            if (null != prev) {
                log.info("putKVConfig update config item, " +
                    "Namespace = {} Key = {} Value = {}",
                    namespace, key, value);
            } else {
                log.info("putKVConfig create new config item, " +
                    "Namespace = {} Key = {} Value = {}",
                    namespace, key, value);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    } catch (InterruptedException e) {
        log.error("putKVConfig InterruptedException", e);
    }

    // 写入后立即持久化到磁盘
    this.persist();
}
```

**每次写入都立即持久化**——这保证了配置不会因为 NameServer 重启而丢失。
虽然牺牲了一些性能，但对于低频的配置操作来说完全可以接受。

### 11.4 deleteKVConfig() — 删除配置

```java
// KVConfigManager.java

public void deleteKVConfig(final String namespace,
                           final String key) {
    try {
        this.lock.writeLock().lockInterruptibly();
        try {
            HashMap<String, String> kvTable =
                this.configTable.get(namespace);
            if (null != kvTable) {
                String value = kvTable.remove(key);
                log.info("deleteKVConfig delete a config item, " +
                    "Namespace = {} Key = {} Value = {}",
                    namespace, key, value);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    } catch (InterruptedException e) {
        log.error("deleteKVConfig InterruptedException", e);
    }

    this.persist();
}
```

### 11.5 getKVConfig() — 读取配置

```java
// KVConfigManager.java

public String getKVConfig(final String namespace,
                          final String key) {
    try {
        this.lock.readLock().lockInterruptibly();
        try {
            HashMap<String, String> kvTable =
                this.configTable.get(namespace);
            if (null != kvTable) {
                return kvTable.get(key);
            }
        } finally {
            this.lock.readLock().unlock();
        }
    } catch (InterruptedException e) {
        log.error("getKVConfig InterruptedException", e);
    }

    return null;
}
```

读操作使用读锁，允许并发读取。

### 11.6 getKVListByNamespace() — 获取命名空间下所有配置

```java
// KVConfigManager.java

public byte[] getKVListByNamespace(final String namespace) {
    try {
        this.lock.readLock().lockInterruptibly();
        try {
            HashMap<String, String> kvTable =
                this.configTable.get(namespace);
            if (null != kvTable) {
                KVTable table = new KVTable();
                table.setTable(kvTable);
                return table.encode();
            }
        } finally {
            this.lock.readLock().unlock();
        }
    } catch (InterruptedException e) {
        log.error("getKVListByNamespace InterruptedException", e);
    }

    return null;
}
```

### 11.7 persist() — 持久化到磁盘

```java
// KVConfigManager.java

public void persist() {
    try {
        this.lock.readLock().lockInterruptibly();
        try {
            KVConfigSerializeWrapper kvConfigSerializeWrapper =
                new KVConfigSerializeWrapper();
            kvConfigSerializeWrapper.setConfigTable(this.configTable);

            String content = kvConfigSerializeWrapper.toJson();

            if (null != content) {
                MixAll.string2File(
                    content,
                    this.namesrvController.getNamesrvConfig()
                        .getKvConfigPath());
            }
        } finally {
            this.lock.readLock().unlock();
        }
    } catch (IOException e) {
        log.error("persist kvconfig Exception, "
            + this.namesrvController.getNamesrvConfig()
                .getKvConfigPath(), e);
    } catch (InterruptedException e) {
        log.error("persist kvconfig InterruptedException", e);
    }
}
```

**持久化使用读锁而非写锁**——因为持久化操作不修改内存中的数据，只是读取并序列化。
使用读锁不会阻塞其他的读操作（如 `getKVConfig()`），提高了并发性。

### 11.8 KVConfigManager 在 NameServer 中的作用

```
┌──────────────────────────────────────────────────────────────┐
│              KVConfigManager 的使用场景                        │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. 顺序消息配置                                              │
│     命名空间: "ORDER_TOPIC_CONFIG"                            │
│     key: TopicName                                           │
│     value: "brokerName:queueNum;brokerName:queueNum"         │
│     用途: 告诉 Producer 该 Topic 是顺序消息 Topic，             │
│           以及队列在各 Broker 上的分布                          │
│                                                              │
│  2. 单元化配置                                                │
│     命名空间: "UNIT_CONFIG"                                   │
│     用途: 多活部署中的单元化路由配置                             │
│                                                              │
│  3. 自定义配置                                                │
│     命名空间: 自定义                                           │
│     用途: 通过 mqadmin 工具动态管理任意 KV 配置                  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## 十二、知识点总结

### 12.1 为什么 NameServer 是 AP 而不是 CP？

在 CAP 理论中，NameServer 选择了 **AP（可用性 + 分区容忍性）** 而非 CP：

```
                       CAP 理论三角
                          C
                         / \
                        /   \
                       /     \
                      / Raft  \
                     / ZK/etcd \
                    /___________\
                   A ─────────── P
                   NameServer
                   (AP)
```

**具体表现**：

1. **多个 NameServer 之间不通信**：没有任何数据同步机制。每个 NameServer 独立工作，
   各自维护自己的路由表。

2. **Broker 向所有 NameServer 注册**：保证每个 NameServer 都有完整的路由数据。
   但由于网络延迟和时序差异，不同 NameServer 上的数据可能存在短暂的不一致。

3. **客户端随机选择 NameServer 查询**：可能查询到不同版本的路由信息。
   但这种不一致通常只持续几秒（下一次心跳就会同步），对消息系统来说是可接受的。

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│   NameServer-1          NameServer-2          NameServer-3   │
│   ┌──────────┐          ┌──────────┐          ┌──────────┐  │
│   │ 路由表 A  │          │ 路由表 B  │          │ 路由表 C  │  │
│   │          │          │          │          │          │  │
│   │ (可能与B、│          │ (可能与A、│          │ (可能与A、│  │
│   │  C略有不 │          │  C略有不 │          │  B略有不 │  │
│   │  同)      │          │  同)      │          │  同)      │  │
│   └──────────┘          └──────────┘          └──────────┘  │
│        ▲                     ▲                     ▲         │
│        │                     │                     │         │
│        └─────────────────────┼─────────────────────┘         │
│                              │                               │
│                         ┌────┴────┐                          │
│                         │ Broker  │                          │
│                         │         │                          │
│                         │ 向所有   │                          │
│                         │ NameServer│                         │
│                         │ 注册     │                          │
│                         └─────────┘                          │
│                                                              │
│   这三个 NameServer 之间没有任何通信！                          │
│   它们的数据一致性完全依赖于 Broker 的注册行为                   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**为什么这种设计是合理的？**

1. **简单性**：不需要实现复杂的一致性协议（如 Raft、Paxos），大幅降低了系统复杂度。
   NameServer 的代码量只有几千行，而 ZooKeeper 的代码量是几十万行。

2. **高可用**：每个 NameServer 都可以独立工作，即使其他 NameServer 全部宕机，
   只要有一个 NameServer 存活，系统就能正常工作。

3. **最终一致性够用**：消息系统对路由信息的一致性要求不高。
   Producer 发送消息时，即使用了稍微过时的路由信息（比如刚下线的 Broker 还在路由中），
   最多就是发送失败后重试到其他 Broker，不会丢消息。

### 12.2 为什么多个 NameServer 不需要通信？

```
┌──────────────────────────────────────────────────────────────┐
│           对比：ZooKeeper vs NameServer                       │
├───────────────────────────┬──────────────────────────────────┤
│       ZooKeeper           │         NameServer               │
├───────────────────────────┼──────────────────────────────────┤
│ 节点间通过 ZAB 协议通信    │ 节点间完全不通信                  │
│ 数据写入需要多数派确认      │ 数据直接写入本地内存               │
│ Leader 选举、日志复制       │ 无 Leader 概念                   │
│ 强一致性                   │ 最终一致性                        │
│ 写性能受限于共识协议        │ 写性能不受限                      │
│ 部署复杂（奇数节点）        │ 部署简单（任意数量节点）           │
│ 代码量大，故障排查困难       │ 代码量小，故障排查简单            │
│ 适合强一致场景              │ 适合最终一致场景                  │
└───────────────────────────┴──────────────────────────────────┘
```

RocketMQ 的设计哲学是**"能用简单方案解决的问题，绝不用复杂方案"**。
对于消息系统的路由管理，最终一致性完全满足需求，因此选择了最简单的 AP 方案。

### 12.3 为什么 Broker 要向所有 NameServer 注册？

```
                    为什么 Broker 要向所有 NameServer 注册？
  
    如果只向一个 NameServer 注册：
    ┌──────────┐     注册     ┌──────────┐
    │  Broker  │────────────>│ NS-1     │  ← 这个 NS 挂了，
    │          │              │ (故障!)   │     路由信息就丢了
    └──────────┘              └──────────┘
                              ┌──────────┐
                              │ NS-2     │  ← 这个 NS 没有该 Broker 的信息
                              │ (正常)    │     客户端查不到路由
                              └──────────┘
  
    如果向所有 NameServer 注册：
    ┌──────────┐     注册     ┌──────────┐
    │  Broker  │────────────>│ NS-1     │  ← 即使这个挂了
    │          │     注册     │ (故障!)   │
    │          │────────────>┌──────────┐
    │          │              │ NS-2     │  ← 这个仍有完整路由
    └──────────┘              │ (正常)    │     客户端可正常查询
                              └──────────┘
```

**代价**：每个 Broker 需要维护到所有 NameServer 的连接，注册请求量 = Broker 数 x NameServer 数。
**收益**：任何一个 NameServer 都拥有完整的路由信息，任何一个故障都不影响整体可用性。

在生产环境中，NameServer 通常部署 2-3 个，Broker 通常几十到几百个，
所以注册请求量 = 几百 x 3 = 几百次/30秒，完全不是问题。

### 12.4 ReadWriteLock 设计决策

```
┌──────────────────────────────────────────────────────────────┐
│               ReadWriteLock 设计权衡                          │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  方案1: synchronized                                         │
│    优点: 实现简单                                              │
│    缺点: 读写互斥，路由查询和 Broker 注册不能并发               │
│    适用: 读写比例相近                                          │
│                                                              │
│  方案2: ReadWriteLock (RocketMQ 选择)                         │
│    优点: 读读并发，只有写操作需要排他锁                          │
│    缺点: 写操作期间会阻塞所有读操作                             │
│    适用: 读多写少（路由查询远多于 Broker 注册）                  │
│                                                              │
│  方案3: CopyOnWrite                                          │
│    优点: 读操作完全无锁                                        │
│    缺点: 每次写操作需要复制整个数据结构，内存开销大               │
│    适用: 写极少、数据量小                                      │
│                                                              │
│  方案4: StampedLock                                           │
│    优点: 支持乐观读，性能最好                                   │
│    缺点: API 复杂，不支持重入，容易出 bug                       │
│    适用: 极致性能要求                                          │
│                                                              │
│  NameServer 选择 ReadWriteLock 的理由：                        │
│  - 路由查询 (读) 远多于 Broker 注册 (写)，读写比约 100:1       │
│  - 数据结构较大（多张路由表），CopyOnWrite 开销太大              │
│  - 代码可维护性好，不容易出 bug                                 │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 12.5 批量注销（BatchUnregistration）设计

```
┌──────────────────────────────────────────────────────────────┐
│              批量注销的性能优势                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  场景：某个机房有 100 台 Broker 同时断电                        │
│                                                              │
│  ── 逐个注销模式 ──                                           │
│  获取写锁 → 处理 Broker-1 → 释放写锁                          │
│  获取写锁 → 处理 Broker-2 → 释放写锁                          │
│  获取写锁 → 处理 Broker-3 → 释放写锁                          │
│  ...                                                         │
│  获取写锁 → 处理 Broker-100 → 释放写锁                        │
│                                                              │
│  100 次锁获取/释放！                                          │
│  每次获取写锁都会阻塞所有读操作（路由查询）                      │
│  路由查询延迟大幅增加                                          │
│                                                              │
│  ── 批量注销模式 (RocketMQ 采用) ──                            │
│  队列收集: Broker-1, Broker-2, ..., Broker-100                │
│  获取写锁 → 批量处理 100 个 Broker → 释放写锁                  │
│                                                              │
│  只有 1 次锁获取/释放！                                       │
│  路由查询只被阻塞一次                                          │
│  延迟影响降低 100 倍                                           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

BatchUnregistrationService 的 `take() + drainTo()` 模式是实现批量化的经典手法：

```java
// 伪代码
while (!stopped) {
    // take() 阻塞等待第一个请求（CPU 零消耗）
    request = queue.take();
    
    // 有请求了！用 drainTo 尽可能多地取出积压的请求
    batch = new Set();
    batch.add(request);
    queue.drainTo(batch);  // 非阻塞，取出队列中所有剩余请求
    
    // 批量处理（只获取一次写锁）
    routeInfoManager.unRegisterBroker(batch);
}
```

### 12.6 Acting Master 机制详解

Acting Master 是 RocketMQ 5.x 引入的一种轻量级高可用机制：

```
┌──────────────────────────────────────────────────────────────┐
│              Acting Master 机制                               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  正常状态:                                                    │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │ Master (0)  │    │ Slave-1 (1) │    │ Slave-2 (2) │      │
│  │ 读写         │    │ 只读         │    │ 只读         │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
│                                                              │
│  Master 宕机后 (无 Acting Master):                            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │ Master (0)  │    │ Slave-1 (1) │    │ Slave-2 (2) │      │
│  │ 宕机 ✗       │    │ 只读         │    │ 只读         │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
│  → Producer 无法写入，直到 Master 恢复                         │
│                                                              │
│  Master 宕机后 (有 Acting Master):                            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │ Master (0)  │    │ Slave-1 (1) │    │ Slave-2 (2) │      │
│  │ 宕机 ✗       │    │ Acting Master│    │ 只读         │      │
│  │             │    │ (brokerId=0 │    │             │      │
│  │             │    │  在路由中)   │    │             │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
│  → Slave-1 临时顶替 Master，客户端可继续消费                   │
│  → 但 Slave 的 Topic 权限被设为只读，Producer 不写入            │
│                                                              │
│  注意：Acting Master 只是路由层面的"提升"，                     │
│  实际的 Broker 进程并没有切换角色。                              │
│  它的主要作用是让 Consumer 能继续消费 Slave 上的消息。           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Acting Master 在代码中的两个实现点**：

1. **`registerBroker()` 中的 `isPrimeSlave` 判断**（第五章 Step 9-10）：
   当 Prime Slave 注册时，其 Topic 配置写入 topicQueueTable，但权限改为只读。

2. **`pickupTopicRouteData()` 中的提升逻辑**（第六章 Step 5）：
   返回路由信息时，如果某个 BrokerData 没有 Master，将最小 brokerId 的 Slave
   在返回数据中"提升"为 brokerId=0。

### 12.7 DataVersion 与增量同步

```
┌──────────────────────────────────────────────────────────────┐
│              DataVersion 增量同步机制                          │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  DataVersion 数据结构:                                        │
│  ┌─────────────────────────────────────┐                     │
│  │ DataVersion {                       │                     │
│  │   stateVersion: long,  // 全局版本号 │                     │
│  │   timestamp: long,     // 时间戳     │                     │
│  │   counter: AtomicLong  // 计数器     │                     │
│  │ }                                   │                     │
│  └─────────────────────────────────────┘                     │
│                                                              │
│  版本号变化时机:                                               │
│  - Broker 创建新 Topic → counter++, timestamp 更新             │
│  - Broker 删除 Topic → counter++, timestamp 更新               │
│  - Broker 修改 Topic 配置 → counter++, timestamp 更新          │
│  - Broker 角色切换 → stateVersion++                            │
│                                                              │
│  同步流程:                                                    │
│                                                              │
│  Broker                        NameServer                    │
│    │                               │                         │
│    │  QUERY_DATA_VERSION           │                         │
│    │  {version: v5}                │                         │
│    │──────────────────────────────>│                         │
│    │                               │ 比较 v5 vs 存储的 v5    │
│    │  Response: changed=false      │ 相同！不需要全量注册      │
│    │<──────────────────────────────│                         │
│    │                               │                         │
│    │  (Broker 创建了新 Topic)       │                         │
│    │  version 变为 v6               │                         │
│    │                               │                         │
│    │  QUERY_DATA_VERSION           │                         │
│    │  {version: v6}                │                         │
│    │──────────────────────────────>│                         │
│    │                               │ 比较 v6 vs 存储的 v5    │
│    │  Response: changed=true       │ 不同！需要全量注册        │
│    │<──────────────────────────────│                         │
│    │                               │                         │
│    │  REGISTER_BROKER (全量)        │                         │
│    │  {version: v6, topics: [...]} │                         │
│    │──────────────────────────────>│                         │
│    │                               │ 更新路由表，存储 v6       │
│    │  Response: OK                 │                         │
│    │<──────────────────────────────│                         │
│                                                              │
│  效果: 大部分心跳周期只传版本号(几十字节)，                      │
│        只在配置变化时才传全量 Topic 数据(可能几十 KB)             │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 12.8 NameServer 的设计哲学总结

```
┌──────────────────────────────────────────────────────────────┐
│              NameServer 设计哲学                               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. 简单优于复杂                                              │
│     - 不用 ZooKeeper，自己实现简单的路由管理                    │
│     - 不实现一致性协议，接受最终一致性                          │
│     - 代码量极少（核心类不超过 5 个）                           │
│                                                              │
│  2. 可用性优于一致性                                           │
│     - AP 模型，任何单点故障不影响整体                           │
│     - NameServer 之间不通信，互相独立                          │
│     - 客户端本地缓存路由，NameServer 全挂也能短时间工作          │
│                                                              │
│  3. 读优化优于写优化                                           │
│     - ReadWriteLock: 读操作并发，写操作排他                    │
│     - 专用线程池: 路由查询有独立线程池                          │
│     - 深拷贝返回: 避免读操作受写操作影响                        │
│                                                              │
│  4. 批量优于逐个                                              │
│     - BatchUnregistrationService: 合并注销请求                │
│     - 减少写锁获取次数                                        │
│                                                              │
│  5. 防御性编程                                                │
│     - ConcurrentHashMap + ReadWriteLock 双重保护               │
│     - CRC32 数据校验                                          │
│     - stateVersion 冲突检查                                   │
│     - 启动保护期                                               │
│                                                              │
│  6. 关注分离                                                  │
│     - ClientRequestProcessor: 只处理客户端路由查询             │
│     - DefaultRequestProcessor: 处理其他所有请求                │
│     - BrokerHousekeepingService: 专注于连接管理                │
│     - KVConfigManager: 专注于 KV 配置管理                     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 12.9 NameServer 面试高频问题

**Q1: NameServer 为什么不用 ZooKeeper？**

A: RocketMQ 早期版本确实使用 ZooKeeper 作为注册中心。但在实践中发现 ZooKeeper 的强一致性
对于消息系统的路由管理来说是过度设计。消息路由信息的变化频率低（Broker 加入/退出不频繁），
且客户端对路由信息的一致性要求不高（短暂的路由不一致只会导致重试，不会丢消息）。
用一个几千行代码的 NameServer 替换 ZooKeeper，可以减少外部依赖、降低运维成本、提高系统稳定性。

**Q2: NameServer 如何发现 Broker 宕机？**

A: 有两种机制：
1. **定时扫描**：每 5 秒扫描 `brokerLiveTable`，超过 120 秒没有心跳的 Broker 被标记为不可用
2. **连接事件**：通过 `BrokerHousekeepingService` 监听 Netty Channel 事件，
   Broker 连接断开时立即感知（不需要等待 120 秒超时）

实际上，大部分情况下 Broker 宕机会导致 TCP 连接断开（RST 或 FIN），
`BrokerHousekeepingService` 会在毫秒级别感知到。120 秒超时是兜底机制，
用于处理网络分区（TCP 连接仍然存在但实际已不通）的场景。

**Q3: 客户端如何处理 NameServer 返回的路由信息？**

A: 客户端（Producer/Consumer）的路由管理流程：
1. 启动时从 NameServer 获取路由信息并缓存到本地
2. 每 30 秒定时从 NameServer 更新路由信息
3. 发送/消费消息时使用本地缓存的路由信息
4. 如果发送失败（如 Broker 不可用），触发主动路由更新
5. 即使 NameServer 全部不可用，客户端仍可使用本地缓存的路由信息继续工作

**Q4: Broker 注册使用了写锁，会不会影响路由查询性能？**

A: 会有影响，但影响很小。原因：
1. Broker 注册频率低（每 30 秒一次），写锁持有时间短
2. 5.x 引入了轻量心跳（`BROKER_HEARTBEAT`），不需要获取写锁
3. 批量注销机制减少了写锁获取次数
4. 读操作可以并发（ReadWriteLock 的读锁是共享的）

**Q5: 如果 NameServer 重启，路由信息会丢失吗？**

A: 会。NameServer 的路由信息存储在内存中，不持久化到磁盘。
但这不是问题：NameServer 重启后，Broker 会在几秒内重新注册，路由信息自动恢复。
唯一持久化的是 KVConfigManager 管理的 KV 配置（如顺序消息配置），
这部分数据会保存到 `kvConfig.json` 文件中。

**Q6: 多个 NameServer 之间的数据不一致会导致什么问题？**

A: 可能导致以下情况（但都是可容忍的）：
1. Producer 短暂地向已宕机的 Broker 发送消息 → 发送失败，自动重试到其他 Broker
2. Consumer 短暂地看不到新创建的 Topic → 等待下一次路由更新（最多 30 秒）
3. 不同 Producer 看到不同的路由信息 → 消息分布可能短暂不均匀

这些问题都是暂时的，不会导致消息丢失或数据不一致。

---

## 附录A：NameServer 核心源文件清单

```
namesrv/src/main/java/org/apache/rocketmq/namesrv/
├── NamesrvStartup.java              (243行)  启动入口
├── NamesrvController.java           (285行)  中枢控制器
├── kvconfig/
│   └── KVConfigManager.java         (194行)  KV配置管理
├── processor/
│   ├── DefaultRequestProcessor.java (680行)  默认请求处理器
│   └── ClientRequestProcessor.java  (103行)  客户端请求处理器
└── routeinfo/
    ├── RouteInfoManager.java        (1279行) 路由信息管理器(核心)
    ├── BrokerHousekeepingService.java (54行)  Broker连接管家
    └── BatchUnregistrationService.java (82行) 批量注销服务
```

## 附录B：NameServer 关键配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `listenPort` | 9876 | NameServer 监听端口 |
| `scanNotActiveBrokerInterval` | 5000ms | 扫描不活跃 Broker 的间隔 |
| `BROKER_CHANNEL_EXPIRED_TIME` | 120000ms | Broker 心跳超时时间 |
| `unRegisterBrokerQueueCapacity` | 1000 | 批量注销队列容量 |
| `defaultThreadPoolNums` | 16 | 默认请求处理线程池大小 |
| `clientRequestThreadPoolNums` | 8 | 客户端请求处理线程池大小 |
| `waitSecondsForService` | 与scan间隔相同 | 启动保护期时长 |
| `supportActingMaster` | false | 是否支持 Acting Master |
| `kvConfigPath` | ~/namesrv/kvConfig.json | KV 配置持久化路径 |
| `orderMessageEnable` | false | 是否启用顺序消息支持 |

## 附录C：NameServer 请求码速查表

| 请求码 | 值 | 处理器 | 线程池 | 说明 |
|--------|-----|--------|--------|------|
| `GET_ROUTEINFO_BY_TOPIC` | 105 | ClientRequestProcessor | clientRequestExecutor | 路由查询 |
| `REGISTER_BROKER` | 103 | DefaultRequestProcessor | defaultExecutor | Broker注册 |
| `UNREGISTER_BROKER` | 104 | DefaultRequestProcessor | defaultExecutor | Broker注销 |
| `BROKER_HEARTBEAT` | 904 | DefaultRequestProcessor | defaultExecutor | 轻量心跳 |
| `QUERY_DATA_VERSION` | 322 | DefaultRequestProcessor | defaultExecutor | 版本查询 |
| `GET_BROKER_CLUSTER_INFO` | 106 | DefaultRequestProcessor | defaultExecutor | 集群信息 |
| `PUT_KV_CONFIG` | 100 | DefaultRequestProcessor | defaultExecutor | 写KV配置 |
| `GET_KV_CONFIG` | 101 | DefaultRequestProcessor | defaultExecutor | 读KV配置 |
| `DELETE_KV_CONFIG` | 102 | DefaultRequestProcessor | defaultExecutor | 删KV配置 |
| `WIPE_WRITE_PERM_OF_BROKER` | 205 | DefaultRequestProcessor | defaultExecutor | 擦除写权限 |
| `ADD_WRITE_PERM_OF_BROKER` | 327 | DefaultRequestProcessor | defaultExecutor | 添加写权限 |
| `GET_ALL_TOPIC_LIST_FROM_NAMESERVER` | 206 | DefaultRequestProcessor | defaultExecutor | 全量Topic列表 |
| `DELETE_TOPIC_IN_NAMESRV` | 216 | DefaultRequestProcessor | defaultExecutor | 删除Topic |
| `REGISTER_TOPIC_IN_NAMESRV` | 217 | DefaultRequestProcessor | defaultExecutor | 注册Topic |
| `GET_KVLIST_BY_NAMESPACE` | 219 | DefaultRequestProcessor | defaultExecutor | 命名空间KV列表 |
| `GET_TOPICS_BY_CLUSTER` | 224 | DefaultRequestProcessor | defaultExecutor | 按集群查Topic |
| `GET_SYSTEM_TOPIC_LIST_FROM_NS` | 304 | DefaultRequestProcessor | defaultExecutor | 系统Topic列表 |
| `GET_UNIT_TOPIC_LIST` | 311 | DefaultRequestProcessor | defaultExecutor | 单元化Topic |
| `GET_HAS_UNIT_SUB_TOPIC_LIST` | 312 | DefaultRequestProcessor | defaultExecutor | 有单元订阅的Topic |
| `GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST` | 313 | DefaultRequestProcessor | defaultExecutor | 非单元但有单元订阅 |
| `UPDATE_NAMESRV_CONFIG` | 318 | DefaultRequestProcessor | defaultExecutor | 更新NameServer配置 |
| `GET_NAMESRV_CONFIG` | 319 | DefaultRequestProcessor | defaultExecutor | 获取NameServer配置 |
| `GET_BROKER_MEMBER_GROUP` | 901 | DefaultRequestProcessor | defaultExecutor | Broker成员组 |

---

> **全文完**
>
> 本文从 NameServer 的启动入口出发，详细剖析了启动流程、控制器初始化、路由数据结构、
> Broker 注册/注销、路由发现、心跳检测、请求处理器等全部核心模块的源码实现。
> 希望读者通过本文能对 NameServer 的设计理念和实现细节有深入的理解。
>
> 建议读者在阅读本文的同时，对照 RocketMQ 5.x 源码进行验证，加深理解。