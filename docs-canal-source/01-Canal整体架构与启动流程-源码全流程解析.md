# Canal 整体架构与启动流程 —— 源码全流程解析

> 基于源码项目 `canal` 逐步分析，从 JVM 启动 main 方法到 Netty 端口监听、CanalInstance 四大组件就绪、binlog dump 线程启动，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
JVM 启动
  |
  +-- 1. CanalLauncher.main()
  |     -> 加载 canal.properties
  |     -> [可选] 连接 Canal Admin，从远程拉取配置 + 启动定时轮询配置变更
  |     -> 创建 CanalStarter，调用 start()
  |
  +-- 2. CanalStarter.start()
  |     -> [MQ模式] SPI加载CanalMQProducer并初始化
  |     -> new CanalController(properties)  <-- 核心初始化发生在构造函数中
  |     -> controller.start()
  |     -> [MQ模式] 启动CanalMQStarter
  |     -> [有admin.port] 启动CanalAdminWithNetty
  |
  +-- 3. CanalController 构造函数（核心初始化）
  |     +-- initGlobalConfig()
  |     |     -> 创建 instanceGenerator Lambda（决定如何创建CanalInstance）
  |     +-- initInstanceConfig()
  |     |     -> 解析 canal.destinations（如 "example,order,user"）
  |     +-- 创建 CanalServerWithEmbedded 单例
  |     +-- 创建 CanalServerWithNetty 单例
  |     +-- [有ZK] 初始化 ZkClientx
  |     +-- 为每个 destination 配置 ServerRunningMonitor（HA竞争机制）
  |     |     -> processActiveEnter 回调: embeddedServer.start(destination)
  |     |     -> processActiveExit  回调: embeddedServer.stop(destination)
  |     +-- [autoScan] 创建 InstanceConfigMonitor（配置变更扫描器）
  |
  +-- 4. CanalController.start()
  |     +-- ZK注册当前server节点（临时节点）
  |     +-- embeddedCanalServer.start()
  |     |     -> 创建 LazyMap<destination, CanalInstance>
  |     +-- 遍历所有 destination：
  |     |     +-- ServerRunningMonitor.start()
  |     |     |     -> ZK抢占运行权
  |     |     |     -> 成功后回调 processActiveEnter
  |     |     |         -> embeddedCanalServer.start(destination)
  |     |     |             -> canalInstances.get(destination)  <-- 触发LazyMap
  |     |     |                 -> instanceGenerator.generate(destination)
  |     |     |                     +-- [SPRING模式] SpringCanalInstanceGenerator
  |     |     |                     |     -> 加载 Spring XML → 获取 instance bean
  |     |     |                     +-- [MANAGER模式] PlainCanalInstanceGenerator
  |     |     |                           -> 远程拉取配置 → new CanalInstanceWithManager
  |     |     |             -> canalInstance.start()
  |     |     |                 +-- metaManager.start()     -- 消费位点管理
  |     |     |                 +-- alarmHandler.start()    -- 报警
  |     |     |                 +-- eventStore.start()      -- RingBuffer存储
  |     |     |                 +-- eventSink.start()       -- Sink桥接器
  |     |     |                 +-- eventParser.start()     -- binlog解析器（开始dump!）
  |     |     +-- instanceConfigMonitor.register(destination)
  |     +-- instanceConfigMonitor.start()  -- 启动配置变更扫描
  |     +-- canalServer.start()  -- 启动Netty TCP服务
  |           -> ServerBootstrap 绑定端口（默认11111）
  |           -> Pipeline: FixedHeaderFrameDecoder → Handshake → Auth → Session
  |
  +-- 5. 主线程 runningLatch.await() 阻塞，直到 ShutdownHook 触发
```

---

## Canal 整体架构概览

在深入源码之前，先理解 Canal 的整体架构。Canal 的核心设计理念是**伪装成 MySQL slave，通过 MySQL 的主从复制协议获取 binlog，然后解析成结构化的数据变更事件，提供给下游消费**。

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                    CanalController (调度中枢)                 │
                    │                                                             │
                    │  ┌───────────────────┐    ┌───────────────────────────┐     │
                    │  │  CanalServerWith  │    │  CanalServerWithNetty    │     │
                    │  │     Embedded      │    │  (Netty TCP 11111端口)    │     │
                    │  │  (数据服务层)       │◄───│  (网络协议层)             │     │
                    │  │  subscribe/get/   │    │  Protobuf编解码          │     │
                    │  │  ack/rollback     │    │  4字节Length帧协议        │     │
                    │  └────────┬──────────┘    └───────────────────────────┘     │
                    │           │                                                  │
                    │           │ canalInstances.get(destination)                  │
                    │           ▼                                                  │
                    │  ┌──────────────────────────────────────────────────────┐   │
                    │  │           CanalInstance (per destination)             │   │
                    │  │                                                      │   │
                    │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │   │
                    │  │  │ Parser   │→│  Sink    │→│  Store   │→│  Meta  │ │   │
                    │  │  │模拟slave │ │过滤/路由  │ │RingBuffer│ │位点管理│ │   │
                    │  │  │解析binlog│ │投递store │ │内存存储   │ │cursor  │ │   │
                    │  │  └──────────┘ └──────────┘ └──────────┘ └────────┘ │   │
                    │  └──────────────────────────────────────────────────────┘   │
                    └─────────────────────────────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │        HA 机制 (ServerRunningMonitor)  │
                    │  通过 ZooKeeper 临时节点竞争运行权      │
                    │  多个 Canal Server 只有一个active      │
                    └──────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │     配置监控 (InstanceConfigMonitor)   │
                    │  SPRING模式: 扫描conf目录文件变更       │
                    │  MANAGER模式: 轮询远程Admin API        │
                    └──────────────────────────────────────┘
```

### 核心类继承关系

```
CanalLifeCycle (接口)
  +-- CanalServer (接口)
  |     +-- CanalServerWithEmbedded (implements CanalServer, CanalService) [单例]
  |     +-- CanalServerWithNetty (implements CanalServer) [单例]
  +-- CanalInstance (接口)
  |     +-- AbstractCanalInstance (抽象类)
  |           +-- CanalInstanceWithManager (Manager模式)
  |           +-- CanalInstanceWithSpring  (Spring模式)
  +-- InstanceConfigMonitor (接口)
        +-- SpringInstanceConfigMonitor  (监听文件系统目录变化)
        +-- ManagerInstanceConfigMonitor (轮询远程Admin配置变化)

CanalService (接口) -- 定义 subscribe/get/ack/rollback 方法
CanalInstanceGenerator (接口) -- 工厂接口，通过 destination 生成 CanalInstance
```

### 模块职责划分

| 模块 | 核心类 | 职责 |
|------|--------|------|
| deployer | CanalLauncher / CanalStarter / CanalController | JVM 入口、生命周期编排、Instance 调度 |
| server | CanalServerWithEmbedded / CanalServerWithNetty | 数据服务(subscribe/get/ack)、网络协议层 |
| instance | CanalInstance / AbstractCanalInstance | 四大组件(parser/sink/store/meta)的容器 |
| parse | AbstractEventParser / MysqlEventParser | 模拟 slave 协议、binlog dump 与解析 |
| sink | EntryEventSink / GroupEventSink | 数据过滤、路由、多源归并 |
| store | MemoryEventStoreWithBuffer | RingBuffer 内存存储 |
| meta | MemoryMetaManager / FileMixed / ZooKeeper | 消费位点、batch 管理 |
| protocol | CanalEntry / CanalPacket | Protobuf 协议定义 |
| filter | AviaterRegexFilter | 表名正则过滤 |
| client | CanalConnector / SimpleCanalConnector | 客户端 SDK |
| client-adapter | OuterAdapter / ES / RDB / HBase | 数据同步到下游存储 |

---

## 第一阶段：CanalLauncher.main() —— JVM 启动入口

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalLauncher.java`

CanalLauncher 是整个 Canal Server 的 JVM 入口，它的 main 方法做了以下几件事：

### 1.1 加载配置文件

```java
public class CanalLauncher {

    public static void main(String[] args) throws Throwable {
        // 1. 设置全局未捕获异常处理器
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("UnCaughtException", e));

        // 2. 加载配置文件
        // 优先从系统属性 canal.conf 获取路径，默认为 classpath:canal.properties
        String conf = System.getProperty("canal.conf", "classpath:canal.properties");
        Properties properties = new Properties();
        // ... 加载 properties ...
```

配置加载的优先级链：系统属性 `-Dcanal.conf=xxx` → 默认 `classpath:canal.properties`。这个设计允许 Docker 环境通过环境变量覆盖配置文件路径。

### 1.2 判断是否启用 Canal Admin 管理模式

```java
        // 3. 判断是否启用 Canal Admin 远程管理
        String managerAddress = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
        if (StringUtils.isNotEmpty(managerAddress)) {
            // 连接远程 Admin 拉取配置
            PlainCanalConfigClient configClient = new PlainCanalConfigClient(
                managerAddress, user, passwd, registerIp, adminPort);
            PlainCanal canalConfig = configClient.findServer(null);

            Properties managerProperties = canalConfig.getProperties();
            managerProperties.putAll(properties); // 本地配置覆盖远程配置

            canalStarter.setProperties(managerProperties);
            canalStarter.start();

            // 启动定时任务，轮询远程配置变更
            executor.scheduleWithFixedDelay(new Runnable() {
                private String lastMd5;

                public void run() {
                    PlainCanal newConfig = configClient.findServer(lastMd5);
                    if (newConfig != null) {
                        // 远程配置 MD5 变化 -> 整个应用热重启
                        canalStarter.stop();
                        // ... merge properties ...
                        canalStarter.start();
                        lastMd5 = newConfig.getMd5();
                    }
                }
            }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
        }
```

**远程配置热更新机制的设计思路**：Canal Admin 是一个 Web 管理后台，集中管理多个 Canal Server 的配置。当管理员在 Admin 后台修改了 `canal.properties`（比如改了 MQ 模式或端口），Canal Server 会在下一次轮询周期检测到 MD5 变化，然后执行 `stop() → start()` 全量重启。这是一种简单粗暴但可靠的热更新方式——不用担心部分配置生效部分不生效的不一致问题。

### 1.3 阻塞主线程等待关闭

```java
        // 4. 注册 ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                canalStarter.stop();
            } finally {
                runningLatch.countDown();
            }
        }));

        // 5. 阻塞主线程
        runningLatch.await();
    }
}
```

`runningLatch` 是一个 `CountDownLatch(1)`，主线程在这里永久阻塞，直到 ShutdownHook 被触发（JVM 关闭时）调用 `countDown()`。这是 Java 服务端程序"保持 JVM 不退出"的标准写法。

---

## 第二阶段：CanalStarter —— 服务启动编排

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalStarter.java`

CanalStarter 是 Canal 的"启动编排器"，它决定 Canal 以什么模式运行（TCP 直连 vs MQ 投递），并编排各个组件的启动顺序。

### 2.1 判断运行模式

```java
public synchronized void start() throws Throwable {
    // 1. 读取 serverMode：tcp / kafka / rocketMQ / rabbitMQ / pulsarMQ
    String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);

    if (!"tcp".equalsIgnoreCase(serverMode)) {
        // MQ 模式：通过 SPI 加载对应的 MQProducer
        ExtensionLoader<CanalMQProducer> loader = ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
        canalMQProducer = new ProxyCanalMQProducer(loader.getExtension(serverMode.toLowerCase()));
        canalMQProducer.init(properties);

        // MQ 模式下不需要 Netty TCP 服务
        System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
    }
```

**为什么 MQ 模式要禁用 Netty？** Canal 有两种消费模式：（1）TCP 直连模式——客户端通过 Netty TCP 连接到 Canal，主动 get/ack 拉取数据；（2）MQ 模式——Canal 主动将数据推送到 Kafka/RocketMQ 等消息队列，客户端从 MQ 消费。两种模式互斥，MQ 模式下客户端不再直连 Canal，所以 Netty TCP 服务就不需要了。

### 2.2 创建并启动 CanalController

```java
    // 2. 创建 CanalController（核心初始化发生在构造函数中）
    controller = new CanalController(properties);
    controller.start();
```

CanalController 的构造函数做了**大量初始化工作**（见第三阶段详解），`start()` 才是真正的启动动作。

### 2.3 MQ 模式后续处理

```java
    // 3. MQ 模式：启动 MQStarter（为每个 destination 创建 worker 线程循环 get→send→ack）
    if (canalMQProducer != null) {
        canalMQStarter = new CanalMQStarter(canalMQProducer);
        String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
        canalMQStarter.start(destinations);
        controller.setCanalMQStarter(canalMQStarter);
    }

    // 4. 启动 Admin 管理端口（如果配置了 canal.admin.port）
    String adminPort = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT);
    if (canalAdmin != null) {
        canalAdmin = new CanalAdminController(this);
        adminNetty = CanalAdminWithNetty.instance();
        adminNetty.setCanalAdmin(canalAdmin);
        adminNetty.setPort(Integer.parseInt(adminPort));
        adminNetty.start();
    }
}
```

**CanalMQStarter 的工作原理**：它为每个 destination 创建一个独立的 worker 线程，线程内部执行一个无限循环：使用固定的 clientId=1001 向 CanalServerWithEmbedded 订阅→调用 `getWithoutAck()` 拉取数据→调用 `canalMQProducer.send()` 发送到 MQ→成功后 `ack()`，失败后 `rollback()`。本质上，MQ 模式下的 Canal 就是一个"内置的消费者客户端 + MQ 生产者"的组合。

---

## 第三阶段：CanalController 构造函数 —— 核心初始化

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalController.java`

CanalController 是整个 Canal 的**调度中枢**。它的构造函数是整个启动流程中最复杂的一步，需要初始化全局配置、Instance 配置、Server 组件、HA 机制、配置扫描器等所有核心组件。

### 3.1 initGlobalConfig() —— 创建全局配置和 instanceGenerator

> **这一步在干什么？**
>
> 在 Canal 的配置体系中，有两层配置："全局配置"和"实例配置"。全局配置定义了所有 destination 的默认行为——用什么模式创建 Instance（SPRING 还是 MANAGER）、Spring XML 路径是什么、是否懒加载等。实例配置可以覆盖全局配置的部分字段。
>
> 更重要的是，这一步创建了 `instanceGenerator` —— 一个 Lambda，后面每次需要创建 CanalInstance 时都会调用它。这个 Lambda 就是"工厂方法"，决定了"给我一个 destination 名字，我怎么把对应的 CanalInstance 造出来"。

```java
private void initGlobalConfig(Properties properties) {
    // 读取全局配置
    InstanceConfig globalConfig = new InstanceConfig();
    String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(CanalConstants.GLOBAL_NAME));
    if (StringUtils.isNotEmpty(modeStr)) {
        globalConfig.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
    }
    // ... lazy, manager.address, spring.xml 等配置 ...
    globalInstanceConfig = globalConfig;

    // ★★★ 创建 instanceGenerator（决定如何创建CanalInstance的工厂Lambda）★★★
    instanceGenerator = new CanalInstanceGenerator() {
        public CanalInstance generate(String destination) {
            InstanceConfig config = instanceConfigs.get(destination);
            if (config == null) {
                throw new CanalServerException("can't find destination:" + destination);
            }

            if (config.getMode().isManager()) {
                // MANAGER 模式：从远程 Admin 拉取配置，编程式构建
                PlainCanalInstanceGenerator generator = new PlainCanalInstanceGenerator(properties);
                generator.setCanalConfigClient(managerClients.get(config.getManagerAddress()));
                generator.setSpringXml(config.getSpringXml());
                return generator.generate(destination);
            } else if (config.getMode().isSpring()) {
                // SPRING 模式：加载 Spring XML，从容器获取 instance bean
                SpringCanalInstanceGenerator generator = new SpringCanalInstanceGenerator();
                generator.setSpringXml(config.getSpringXml());
                return generator.generate(destination);
            } else {
                throw new UnsupportedOperationException("unknown mode: " + config.getMode());
            }
        }
    };
}
```

**两种 Instance 创建模式的区别**：

| 特性 | SPRING 模式 | MANAGER 模式 |
|------|------------|-------------|
| 配置来源 | 本地 Spring XML + instance.properties | 远程 Canal Admin API |
| Instance 类型 | CanalInstanceWithSpring | CanalInstanceWithManager |
| 组件装配方式 | Spring 容器 DI | 编程式 new + setter |
| 适用场景 | 单机部署、开发调试 | 集群部署、运维管理 |
| 默认 Spring XML | `classpath:spring/file-instance.xml` | 由 Admin 下发 |

### 3.2 initInstanceConfig() —— 解析 destination 列表

```java
private void initInstanceConfig(Properties properties) {
    String destinationStr = getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
    // 支持逗号分隔：example,order,user
    // 支持表达式：dest{1-5} -> dest1,dest2,dest3,dest4,dest5
    String[] destinations = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT);

    for (String destination : destinations) {
        InstanceConfig config = parseInstanceConfig(properties, destination);
        // 子配置继承全局配置：如果子配置某字段为空，用全局配置兜底
        InstanceConfig oldConfig = instanceConfigs.put(destination, config);
        if (oldConfig != null) {
            logger.warn("destination:{} old config replaced", destination);
        }
    }
}
```

### 3.3 创建 Server 组件

```java
    // 3. 创建 Embedded Server（单例）
    embeddedCanalServer = CanalServerWithEmbedded.instance();
    embeddedCanalServer.setCanalInstanceGenerator(instanceGenerator);
    int metricsPort = ...;
    embeddedCanalServer.setMetricsPort(metricsPort);
    embeddedCanalServer.setUser(getProperty(properties, CanalConstants.CANAL_USER));
    embeddedCanalServer.setPasswd(getProperty(properties, CanalConstants.CANAL_PASSWD));

    // 4. 创建 Netty Server（单例，TCP模式才创建）
    String canalWithoutNetty = getProperty(properties, CanalConstants.CANAL_WITHOUT_NETTY);
    if (canalWithoutNetty == null || "false".equals(canalWithoutNetty)) {
        canalServer = CanalServerWithNetty.instance();
        canalServer.setIp(ip);
        canalServer.setPort(port);  // 默认 11111
    }
```

**Embedded vs Netty 的职责分工**：

CanalServerWithEmbedded 是"数据服务核心"——它管理所有 CanalInstance 的生命周期，提供 subscribe/get/ack/rollback 等数据操作方法。CanalServerWithNetty 是"网络壳"——它用 Netty 接收远程客户端的 TCP 连接，解析 Protobuf 协议，然后把请求转发给 Embedded Server 处理。

如果你用 Canal 做嵌入式开发（在你的 Java 程序里直接调 API），只需要 Embedded Server，不需要 Netty。如果你要远程消费数据，就需要 Netty Server 对外暴露端口。

### 3.4 初始化 ZooKeeper（HA 机制基础）

```java
    // 5. 初始化 ZooKeeper（如果配置了 zkServers）
    if (StringUtils.isNotEmpty(zkServers)) {
        zkClientx = ZkClientx.getZkClient(zkServers);
        // 创建必要的持久节点
        String path = ZookeeperPathUtils.getDestinationPath(destination);
        zkClientx.createPersistent(path, true);
        String clusterPath = ZookeeperPathUtils.getCanalClusterNode(...);
        zkClientx.createPersistent(clusterPath, true);
    }
```

### 3.5 配置 ServerRunningMonitor —— HA 竞争的核心

> **这一步在干什么？**
>
> Canal 支持多机部署高可用——多个 Canal Server 可以同时运行，但对于同一个 destination，**同一时刻只有一个 Server 能真正运行 Instance**。这个"谁来运行"的竞争机制就是通过 ZooKeeper 临时节点实现的。
>
> 为每个 destination 创建一个 `ServerRunningMonitor`，它负责在 ZK 上抢占临时节点。抢到的节点设置四个回调——"我抢到了/我失去了"分别对应 Instance 的启动和停止。

```java
    // 6. 为每个 destination 配置 HA 运行监控
    ServerRunningMonitors.setRunningMonitors(
        MigrateMap.makeComputingMap((Function<String, ServerRunningMonitor>) destination -> {
            ServerRunningMonitor runningMonitor = new ServerRunningMonitor(serverData);
            runningMonitor.setDestination(destination);
            runningMonitor.setListener(new ServerRunningListener() {

                // ★ 回调1：当前节点抢占到运行权
                public void processActiveEnter() {
                    try {
                        // 启动该 destination 的 Instance
                        embeddedCanalServer.start(destination);
                        // 如果是 MQ 模式，启动 MQ worker
                        if (canalMQStarter != null) {
                            canalMQStarter.startDestination(destination);
                        }
                    } catch (Exception e) {
                        logger.error("start failed", e);
                    }
                }

                // ★ 回调2：当前节点失去运行权
                public void processActiveExit() {
                    try {
                        // 停止 MQ worker
                        if (canalMQStarter != null) {
                            canalMQStarter.stopDestination(destination);
                        }
                        // 停止该 destination 的 Instance
                        embeddedCanalServer.stop(destination);
                    } catch (Exception e) {
                        logger.error("stop failed", e);
                    }
                }

                // ★ 回调3：Monitor 启动时，注册集群成员
                public void processStart() {
                    if (zkClientx != null) {
                        // 在 ZK 中创建临时节点注册集群成员
                        final String path = ZookeeperPathUtils.getDestinationClusterNode(
                            destination, registerIp + ":" + port);
                        zkClientx.createEphemeral(path);
                    }
                }

                // ★ 回调4：Monitor 停止时，释放注册
                public void processStop() {
                    if (zkClientx != null) {
                        final String path = ZookeeperPathUtils.getDestinationClusterNode(
                            destination, registerIp + ":" + port);
                        releaseCid(path);
                    }
                }
            });
            return runningMonitor;
        })
    );
```

**HA 竞争流程图**：

```
Canal Server A                        ZooKeeper                        Canal Server B
     |                                    |                                    |
     |--- createEphemeral(dest/running)-->|                                    |
     |<-- 创建成功 (抢到运行权) ----------|                                    |
     |                                    |                                    |
     |  processActiveEnter()              |                                    |
     |  -> embeddedServer.start(dest)     |                                    |
     |  -> Instance 开始 dump binlog      |                                    |
     |                                    |                                    |
     |                                    |<-- createEphemeral(dest/running)---|
     |                                    |--- 节点已存在，创建失败 ----------->|
     |                                    |                                    |
     |                                    |    Server B 注册 Watcher 监听      |
     |                                    |    该节点的删除事件                  |
     |                                    |                                    |
     |  (Server A 宕机)                   |                                    |
     |  临时节点自动删除 --------------->|                                    |
     |                                    |--- 通知 Watcher: 节点已删除 ------>|
     |                                    |                                    |
     |                                    |<-- createEphemeral(dest/running)---|
     |                                    |--- 创建成功 ---------------------->|
     |                                    |                                    |
     |                                    |    processActiveEnter()             |
     |                                    |    -> embeddedServer.start(dest)    |
     |                                    |    -> Instance 开始 dump binlog     |
```

#### ServerRunningMonitor 内部实现详解

ServerRunningMonitor 是 Canal HA 竞争机制的核心执行者。理解它的内部实现对于理解 Canal 的高可用机制至关重要。

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/monitor/ServerRunningMonitor.java`

```java
public class ServerRunningMonitor extends AbstractCanalLifeCycle {

    private ZkClientx              zkClient;
    private String                 destination;
    private ServerRunningData      serverData;       // 当前 server 的 IP + 端口信息
    private IZkDataListener        dataListener;     // ZK 数据变更监听器
    private BooleanMutex           mutex = new BooleanMutex(false);  // 互斥信号量
    private volatile boolean       release = false;
    private volatile ServerRunningData activeData;   // 当前活跃节点的信息
    private ServerRunningListener  listener;          // 回调监听器（CanalController 传入的那4个回调）
```

> **关键字段说明**：`BooleanMutex` 是 Canal 自己实现的一个基于 AQS（AbstractQueuedSynchronizer）的布尔信号量。当值为 `false` 时，调用 `mutex.get()` 的线程会被阻塞；当某个线程调用 `mutex.set(true)` 时，所有等待线程被唤醒。这个机制用于实现"抢不到运行权时阻塞等待，直到有机会再尝试"。

**start() 方法 —— 启动监控**：

```java
public void start() {
    super.start();

    // 1. 调用 listener.processStart()，在 ZK 上注册集群成员临时节点
    processStart();

    if (zkClient != null) {
        // 2. 注册 ZK 数据监听器
        //    监听 /otter/canal/destinations/{dest}/running 节点的变化
        String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
        zkClient.subscribeDataChanges(path, dataListener);

        // 3. 首次尝试抢占运行权
        initRunning();
    } else {
        // 无 ZK 模式，直接标记为 active（单机模式）
        processActiveEnter();
    }
}
```

**initRunning() 方法 —— ZK 临时节点抢占的核心逻辑**：

```java
private void initRunning() {
    if (!isStart()) {
        return;
    }

    String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
    try {
        // 尝试创建临时节点
        zkClient.createEphemeral(path, JsonUtils.marshalToString(serverData));
        // 创建成功，说明没有其他节点在运行
        activeData = serverData;
        // 触发 processActiveEnter() 回调
        processActiveEnter();
    } catch (ZkNodeExistsException e) {
        // 节点已存在——说明有其他 Canal Server 正在运行这个 destination
        ServerRunningData data = JsonUtils.unmarshalFromByte(
            zkClient.readData(path, true), ServerRunningData.class);

        if (data != null && data.getAddress().equals(serverData.getAddress())) {
            // 特殊情况：节点数据显示是自己
            // 这说明上次宕机后 ZK session 还没过期，临时节点还在
            // 先删除再重建
            zkClient.delete(path);
            zkClient.createEphemeral(path, JsonUtils.marshalToString(serverData));
            activeData = serverData;
            processActiveEnter();
        } else {
            // 确实是其他 Server 在运行
            // 记录当前 active 节点信息，等待机会
            activeData = data;
        }
    } catch (ZkNoNodeException e) {
        // 父节点不存在，先创建父节点再重试
        zkClient.createPersistent(
            ZookeeperPathUtils.getDestinationPath(destination), true);
        initRunning();  // 递归重试
    }
}
```

> **这里有一个精妙的自我恢复逻辑**：当 Canal Server 异常退出后快速重启时，ZK 上的临时节点可能还没过期（ZK session timeout 通常是几十秒）。这时新启动的 Server 会发现 running 节点的数据是自己的 IP:PORT——于是它知道"这是我之前留下的孤儿节点"，直接删除重建，避免了要等 session 超时才能恢复的问题。

**dataListener —— 监听活跃节点变化**：

```java
dataListener = new IZkDataListener() {

    // running 节点被删除（活跃节点宕机）
    public void handleDataDeleted(String dataPath) throws Exception {
        // 之前的 active 不是自己
        mutex.set(false);  // 重置互斥量

        if (!release && activeData != null
            && activeData.getAddress().equals(serverData.getAddress())) {
            // 之前 active 是自己（正常不会走到这里）
            // 可能是因为 session 抖动导致临时节点被删，需要重新抢占
        }

        // 触发重新抢占
        initRunning();
    }

    // running 节点数据变更
    public void handleDataChange(String dataPath, Object data) throws Exception {
        // 数据变了，检查是否还是自己
        ServerRunningData newData = JsonUtils.unmarshalFromByte(
            (byte[]) data, ServerRunningData.class);

        if (!serverData.getAddress().equals(newData.getAddress())) {
            // 不再是自己了，触发 processActiveExit()
            processActiveExit();
        }
        // 更新 activeData
        activeData = newData;
    }
};
```

**BooleanMutex 的 AQS 实现**：

```java
public class BooleanMutex {

    private Sync sync;

    public BooleanMutex(boolean mutex) {
        sync = new Sync();
        set(mutex);
    }

    // 阻塞等待，直到 state 为 true
    public void get() throws InterruptedException {
        sync.innerGet();
    }

    // 带超时的阻塞等待
    public void get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        sync.innerGet(unit.toNanos(timeout));
    }

    // 设置状态
    public void set(boolean mutex) {
        if (mutex) {
            sync.innerSetTrue();   // 唤醒所有等待线程
        } else {
            sync.innerSetFalse();  // 标记为不可用
        }
    }

    private class Sync extends AbstractQueuedSynchronizer {
        private static final int TRUE  = 1;
        private static final int FALSE = 0;

        protected int tryAcquireShared(int arg) {
            return getState() == TRUE ? 1 : -1;
        }

        protected boolean tryReleaseShared(int arg) {
            setState(TRUE);
            return true;
        }

        void innerGet() throws InterruptedException {
            acquireSharedInterruptibly(0);
        }

        void innerSetTrue() {
            releaseShared(0);
        }

        void innerSetFalse() {
            setState(FALSE);
        }
    }
}
```

> **为什么不用 CountDownLatch？** 因为 CountDownLatch 是一次性的（countDown 到 0 后无法重置）。Canal 的 HA 需要多次 "等待-唤醒-再等待" 的循环，BooleanMutex 支持 `set(false)` 重置状态后再次阻塞，完美适配 HA 竞争场景。

### 3.6 初始化配置变更监控器

```java
    // 7. 初始化配置变更监控器（autoScan = true 时）
    if (autoScan) {
        defaultAction = new InstanceAction() {
            public void start(String destination) {
                // 读取/创建 InstanceConfig，启动 Monitor
                InstanceConfig config = instanceConfigs.get(destination);
                if (config == null) {
                    config = parseInstanceConfig(properties, destination);
                    instanceConfigs.put(destination, config);
                }
                if (!embeddedCanalServer.isStart(destination)) {
                    ServerRunningMonitor monitor = ServerRunningMonitors.getRunningMonitor(destination);
                    if (!monitor.isStart()) {
                        monitor.start();
                    }
                }
            }
            public void stop(String destination) {
                // 停止 Instance 和 Monitor
                embeddedCanalServer.stop(destination);
                ServerRunningMonitor monitor = ServerRunningMonitors.getRunningMonitor(destination);
                if (monitor.isStart()) {
                    monitor.stop();
                }
            }
            public void reload(String destination) {
                stop(destination);
                start(destination);
            }
        };

        // 根据模式选择扫描器
        instanceConfigMonitors = MigrateMap.makeComputingMap(mode -> {
            if (mode.isSpring()) {
                SpringInstanceConfigMonitor monitor = new SpringInstanceConfigMonitor();
                monitor.setScanIntervalInSecond(scanInterval);
                monitor.setDefaultAction(defaultAction);
                // 扫描 conf/ 目录下各 destination 子目录的 instance.properties 变化
                String rootDir = getProperty(properties, CanalConstants.CANAL_CONF_DIR);
                monitor.setRootConf(rootDir);
                return monitor;
            } else if (mode.isManager()) {
                return new ManagerInstanceConfigMonitor();
                // 轮询远程 Admin API 获取 instance 配置变化
            }
        });
    }
```

**SpringInstanceConfigMonitor 的工作原理**：它周期性扫描 `conf/` 目录，检查每个子目录（destination）下的 `instance.properties` 文件：

| 扫描结果 | 触发动作 |
|---------|---------|
| 发现新的 destination 子目录 | 调用 `defaultAction.start()` |
| 发现 destination 子目录被删除 | 调用 `defaultAction.stop()` |
| 发现 `instance.properties` 文件修改时间变化 | 调用 `defaultAction.reload()`（stop + start） |

---

## 第四阶段：CanalController.start() —— 真正启动

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalController.java`

构造函数完成了所有初始化工作（准备好材料），`start()` 方法才是真正"点火"的地方。

### 4.1 ZK 注册当前 Server 节点

```java
public void start() throws Throwable {
    // 1. 在 ZK 中注册当前 server 节点（集群发现）
    final String path = ZookeeperPathUtils.getCanalClusterNode(registerIp + ":" + port);
    initCid(path);
    // 创建临时节点，其他 Canal Server 可以通过这个节点发现集群成员
```

### 4.2 启动 Embedded Server

```java
    // 2. 优先启动 Embedded Server
    embeddedCanalServer.start();
```

Embedded Server 的 `start()` 做的事情很精简——加载 Prometheus 监控（通过 SPI），然后创建一个 `LazyMap<String, CanalInstance>`。这个 LazyMap 的关键在于：当第一次通过 `get(destination)` 访问某个 destination 时，才会调用 `instanceGenerator.generate(destination)` 创建 CanalInstance。

```java
// CanalServerWithEmbedded.start()
public void start() {
    if (!isStart()) {
        super.start();
        // 加载 Prometheus 监控
        loadCanalMetrics();
        metrics.initialize();

        // ★ 创建 LazyMap：首次 get 时才创建 Instance
        canalInstances = MigrateMap.makeComputingMap(
            destination -> canalInstanceGenerator.generate(destination)
        );
    }
}
```

**为什么用 LazyMap？** 因为 Canal 支持"懒加载"模式（`canal.instance.global.lazy=true`）。在懒加载模式下，只有当客户端首次 subscribe 某个 destination 时，才会创建对应的 Instance（连接 MySQL、开始 dump）。这在 Canal 管理大量 destination 但不是所有 destination 都有客户端消费时，可以显著降低资源开销。

### 4.3 遍历 destinations，启动非懒加载的 Instance

```java
    // 3. 遍历所有 destination 配置，启动非 lazy 的 destination
    for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
        final String destination = entry.getKey();
        InstanceConfig config = entry.getValue();

        if (!embeddedCanalServer.isStart(destination)) {
            ServerRunningMonitor runningMonitor =
                ServerRunningMonitors.getRunningMonitor(destination);
            if (!config.getLazy() && !runningMonitor.isStart()) {
                // 非懒加载模式：立即启动 Monitor
                runningMonitor.start();
                // Monitor.start() 会尝试在 ZK 上抢占临时节点
                // 如果抢到，触发 processActiveEnter 回调
                // -> embeddedCanalServer.start(destination)
                // -> canalInstances.get(destination)  <-- 触发 LazyMap 创建 Instance
                // -> canalInstance.start()  <-- 启动四大组件
            }
        }

        // 注册到配置监控器
        if (autoScan) {
            instanceConfigMonitors.get(config.getMode()).register(destination, defaultAction);
        }
    }
```

这一步的调用链比较深，让我们追踪一下从 `runningMonitor.start()` 到 `canalInstance.start()` 的完整路径：

```
runningMonitor.start()
  -> ZK 创建临时节点抢占运行权
  -> 抢到后触发 processActiveEnter 回调
     -> embeddedCanalServer.start(destination)
        -> canalInstances.get(destination)        // LazyMap 触发
           -> instanceGenerator.generate(destination)
              -> [SPRING模式] SpringCanalInstanceGenerator.generate()
                 -> 创建 Spring ApplicationContext
                 -> 加载 spring/file-instance.xml
                 -> context.getBean("instance")   // 获取 CanalInstanceWithSpring
              -> [MANAGER模式] PlainCanalInstanceGenerator.generate()
                 -> configClient.findInstance(destination)  // 从远程拉取配置
                 -> new CanalInstanceWithManager(canal, filter)
                    -> initAlarmHandler()    // 报警
                    -> initMetaManager()     // 位点管理
                    -> initEventStore()      // RingBuffer
                    -> initEventSink()       // Sink
                    -> initEventParser()     // Parser(最复杂)
           -> 返回 CanalInstance
        -> canalInstance.start()
           -> metaManager.start()       // ① 消费位点管理启动
           -> alarmHandler.start()      // ② 报警启动
           -> eventStore.start()        // ③ RingBuffer 初始化
           -> eventSink.start()         // ④ Sink 桥接器启动
           -> eventParser.start()       // ⑤ 开始 dump binlog（核心！）
```

### 4.4 启动配置扫描器和 Netty 服务

```java
    // 4. 启动配置变更监控器
    if (autoScan) {
        instanceConfigMonitors.get(globalInstanceConfig.getMode()).start();
        // SpringInstanceConfigMonitor 开始周期扫描 conf/ 目录
        // ManagerInstanceConfigMonitor 开始周期轮询远程 Admin API
    }

    // 5. 启动 Netty TCP 服务（TCP 模式才启动）
    if (canalServer != null) {
        canalServer.start();
    }
}
```

---

## 第五阶段：CanalServerWithNetty —— Netty 网络层启动

**源码位置**: `server/src/main/java/com/alibaba/otter/canal/server/netty/CanalServerWithNetty.java`

### 5.1 Pipeline 构建与端口绑定

```java
public void start() {
    super.start();
    // 确保 Embedded Server 已启动
    if (!embeddedServer.isStart()) {
        embeddedServer.start();
    }

    // 创建 Netty ServerBootstrap（注意：Canal 使用的是 Netty 3.x）
    this.bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(),  // boss 线程池
            Executors.newCachedThreadPool()   // worker 线程池
        )
    );
    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setOption("child.tcpNoDelay", true);

    // 构建 Pipeline
    bootstrap.setPipelineFactory(() -> {
        ChannelPipeline pipelines = Channels.pipeline();

        // Handler 1: 定长Header帧解码器
        pipelines.addLast(FixedHeaderFrameDecoder.class.getName(),
            new FixedHeaderFrameDecoder());

        // Handler 2: 握手初始化
        pipelines.addLast(HandshakeInitializationHandler.class.getName(),
            new HandshakeInitializationHandler(childGroups));

        // Handler 3: 客户端认证
        pipelines.addLast(ClientAuthenticationHandler.class.getName(),
            new ClientAuthenticationHandler(embeddedServer));

        // Handler 4: 会话处理（核心业务Handler）
        pipelines.addLast(SessionHandler.class.getName(),
            new SessionHandler(embeddedServer));

        return pipelines;
    });

    // 绑定端口（默认 11111）
    this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
}
```

**Pipeline 处理顺序（入站方向）**：

```
[网络字节流]
    |
    v
FixedHeaderFrameDecoder      -- 读4字节int作为长度，再读对应长度的body
    |                            协议格式：[4字节长度][body字节]
    v
HandshakeInitializationHandler -- 连接建立时发送 Handshake 包（含8字节随机种子）
    |                              一次性使用，认证完成后从 Pipeline 移除
    v
ClientAuthenticationHandler   -- 校验客户端的 username/password
    |                            认证完成后从 Pipeline 移除，动态添加 IdleStateHandler
    v
SessionHandler               -- 处理所有业务请求：SUBSCRIPTION/GET/ACK/ROLLBACK
    |                            转发给 CanalServerWithEmbedded 处理
    v
[响应字节流]
```

### 5.2 FixedHeaderFrameDecoder —— 帧解码

```java
public class FixedHeaderFrameDecoder extends ReplayingDecoder<VoidEnum> {
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
                           ChannelBuffer buffer, VoidEnum state) {
        return buffer.readBytes(buffer.readInt());
        // 先读4字节int作为包体长度，再读对应长度的字节作为包体
    }
}
```

Canal 的网络协议设计极其简洁：`[4字节大端序长度][Protobuf序列化的body]`。这是"Length-Field-Based"帧协议的最简形式。

### 5.3 HandshakeInitializationHandler —— 握手

```java
public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // 1. 将新连接加入 childGroups（用于统一管理和关闭）
    childGroups.add(ctx.getChannel());

    // 2. 生成 8 字节随机种子（用于密码加密校验）
    byte[] seed = SecurityUtil.seed(8);

    // 3. 构建 Handshake 包发送给客户端
    byte[] body = Packet.newBuilder()
        .setType(PacketType.HANDSHAKE)
        .setVersion(NettyUtils.VERSION)
        .setBody(Handshake.newBuilder().setSeeds(ByteString.copyFrom(seed)).build().toByteString())
        .build().toByteArray();
    NettyUtils.write(ctx.getChannel(), body, future -> {
        // 发送完成后，将 seed 传递给下一个 Handler
        ctx.getPipeline().get(ClientAuthenticationHandler.class).setSeed(seed);
    });
}
```

### 5.4 ClientAuthenticationHandler —— 认证

```java
public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
    Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
    ClientAuth clientAuth = ClientAuth.parseFrom(packet.getBody());

    // 调用 Embedded Server 的 auth 方法校验密码
    boolean isOk = embeddedServer.auth(
        clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed);

    if (isOk) {
        // 认证成功：发送 ACK
        NettyUtils.ack(ctx.getChannel(), future -> {
            // ★★★ 认证完成后动态修改 Pipeline ★★★
            ChannelPipeline pipeline = ctx.getPipeline();

            // 移除一次性 Handler
            pipeline.remove(HandshakeInitializationHandler.class.getName());
            pipeline.remove(ClientAuthenticationHandler.class.getName());

            // 添加空闲检测（默认 1 小时无活动则断连）
            int idleTimeout = 60 * 60 * 1000;  // 1 hour
            pipeline.addBefore(SessionHandler.class.getName(), IdleStateHandler.class.getName(),
                new IdleStateHandler(new HashedWheelTimer(), 0, 0, idleTimeout, TimeUnit.MILLISECONDS));
            pipeline.addBefore(SessionHandler.class.getName(),
                IdleStateAwareChannelHandler.class.getName(),
                new IdleStateAwareChannelHandler() {
                    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) {
                        // 空闲超时，关闭连接
                        ctx.getChannel().close();
                    }
                });
        });
    } else {
        // 认证失败：发送错误包并关闭连接
        NettyUtils.error(401, "auth failed", ctx.getChannel(), null);
    }
}
```

**Pipeline 动态重构**是一个精巧的设计：握手和认证只需要在连接建立时执行一次，之后就不再需要了。认证完成后动态移除这两个 Handler，避免后续每个业务包都经过它们，减少不必要的处理开销。同时在认证后添加空闲检测，防止"认证完就不说话"的僵尸连接占用资源。

认证完成后的 Pipeline 变为：

```
FixedHeaderFrameDecoder → IdleStateHandler → IdleStateAwareChannelHandler → SessionHandler
```

### 5.5 SessionHandler —— 核心业务处理

SessionHandler 是认证后处理所有业务请求的 Handler，根据 `Packet.type` 分发到不同的处理逻辑。这里只展示关键的 GET 请求处理，因为它包含了一个重要的性能优化：

```java
case GET:
    Get get = CanalPacket.Get.parseFrom(packet.getBody());
    // ... 构建 clientIdentity ...

    Message message;
    if (get.getTimeout() == -1) {
        message = embeddedServer.getWithoutAck(clientIdentity, get.getFetchSize());
    } else {
        message = embeddedServer.getWithoutAck(clientIdentity,
            get.getFetchSize(), get.getTimeout(), unit);
    }

    // ★ 高性能序列化路径
    if (message.getId() != -1 && message.isRaw()) {
        // raw 模式：绕过 Protobuf Builder，直接用 CodedOutputStream 手工序列化
        byte[] body = new byte[calculateSize(message)];
        CodedOutputStream output = CodedOutputStream.newInstance(body);
        output.writeEnum(3, PacketType.MESSAGES.getNumber());
        output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        output.writeRawVarint32(messageSize);
        output.writeInt64(1, message.getId());
        for (ByteString rawEntry : message.getRawEntries()) {
            output.writeBytes(2, rawEntry);
        }
        NettyUtils.write(ctx.getChannel(), body, null);
    } else {
        // 标准路径：使用 Protobuf Builder 序列化
        Messages.Builder messageBuilder = CanalPacket.Messages.newBuilder();
        messageBuilder.setBatchId(message.getId());
        // ... 填充 entries ...
        NettyUtils.write(ctx.getChannel(), packet.toByteArray(), null);
    }
```

**为什么要绕过 Protobuf Builder？** Protobuf 的 Builder 模式在构建消息时会创建大量中间对象（Builder、repeated field 的 ArrayList、ByteString 的 copy 等），对于高频的 GET 请求来说 GC 压力很大。raw 模式下，Entry 数据已经是 Protobuf 序列化后的 ByteString，不需要反序列化再重新序列化。直接用 `CodedOutputStream` 按 wire format 手工写入字节，跳过所有中间对象的创建，性能提升显著。

---

## 第六阶段：CanalInstance 的组件装配与生命周期

### 6.1 CanalInstance 接口 —— 五大组件的容器

**源码位置**: `instance/core/src/main/java/com/alibaba/otter/canal/instance/core/CanalInstance.java`

```java
public interface CanalInstance extends CanalLifeCycle {
    String getDestination();
    CanalEventParser getEventParser();     // binlog 解析器
    CanalEventSink getEventSink();         // 数据过滤/路由桥接器
    CanalEventStore getEventStore();       // 数据存储（RingBuffer）
    CanalMetaManager getMetaManager();     // 消费位点管理
    CanalAlarmHandler getAlarmHandler();    // 报警处理
    boolean subscribeChange(ClientIdentity identity);
    CanalMQConfig getMqConfig();
}
```

一个 CanalInstance 就是一个"数据管道"——从一个 MySQL 实例（或一组 MySQL 实例）读取 binlog，解析、过滤、存储，然后提供给客户端消费。每个 destination 对应一个 CanalInstance。

### 6.2 AbstractCanalInstance.start() —— 严格有序的组件启动

**源码位置**: `instance/core/src/main/java/com/alibaba/otter/canal/instance/core/AbstractCanalInstance.java`

```java
public void start() {
    super.start();

    // ★ 启动顺序严格有序，不可调换 ★

    // 1. 先启动消费位点管理（其他组件可能依赖它查位点）
    if (!metaManager.isStart()) {
        metaManager.start();
    }

    // 2. 启动报警（后续组件异常时需要报警）
    if (!alarmHandler.isStart()) {
        alarmHandler.start();
    }

    // 3. 启动存储（Sink 写入时依赖 Store 已就绪）
    if (!eventStore.isStart()) {
        eventStore.start();
    }

    // 4. 启动 Sink 桥接器（Parser 产出数据时依赖 Sink 已就绪）
    if (!eventSink.isStart()) {
        eventSink.start();
    }

    // 5. 最后启动 Parser（它是数据流的源头，一旦启动就开始 dump binlog）
    beforeStartEventParser(eventParser);  // 启动 LogPositionManager、HAController
    if (!eventParser.isStart()) {
        eventParser.start();
    }
    afterStartEventParser(eventParser);   // 恢复历史 filter
}
```

**为什么启动顺序不能调换？** 数据流是 `Parser → Sink → Store`，如果先启动 Parser，Parser 产出的数据没有地方写（Sink/Store 还没启动），就会丢数据。如果先启动 Store 但没启动 MetaManager，客户端 get 时找不到位点信息。所以启动顺序必须是依赖链的**逆序**：先启动下游（被依赖方），后启动上游（依赖方）。

停止顺序则相反——先停上游（Parser），再停下游（Store/Meta）：

```java
public void stop() {
    // 停止顺序与启动相反
    if (eventParser.isStart()) {
        beforeStopEventParser(eventParser);
        eventParser.stop();
        afterStopEventParser(eventParser);
    }
    if (eventSink.isStart()) { eventSink.stop(); }
    if (eventStore.isStart()) { eventStore.stop(); }
    if (metaManager.isStart()) { metaManager.stop(); }
    if (alarmHandler.isStart()) { alarmHandler.stop(); }
    super.stop();
}
```

### 6.3 beforeStartEventParser() —— Parser 启动前置处理

```java
protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
    // 1. 启动 LogPositionManager（位点管理器）
    if (eventParser instanceof AbstractEventParser) {
        AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
        CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
        if (!logPositionManager.isStart()) {
            logPositionManager.start();
        }
    }

    // 2. 启动 HAController（心跳检测主备切换）
    if (eventParser instanceof MysqlEventParser) {
        MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
        CanalHAController haController = mysqlEventParser.getHaController();
        if (haController instanceof HeartBeatHAController) {
            ((HeartBeatHAController) haController).setCanalHASwitchable(mysqlEventParser);
        }
        if (!haController.isStart()) {
            haController.start();
        }
    }
}
```

### 6.4 afterStartEventParser() —— 恢复历史 Filter

```java
protected void afterStartEventParser(CanalEventParser eventParser) {
    // 恢复历史订阅的 filter 信息
    List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
    for (ClientIdentity clientIdentity : clientIdentitys) {
        subscribeChange(clientIdentity);
    }
}
```

**为什么要恢复 filter？** Canal 重启后，之前客户端订阅时设置的表名过滤规则（如 `test\..*`）不会自动恢复——Parser 重新创建了，filter 是空的。这一步从 MetaManager 中读取历史订阅信息，把 filter 重新设置到 Parser 上，确保重启后过滤行为与之前一致。

### 6.5 CanalInstanceWithManager —— Manager 模式的组件初始化

**源码位置**: `instance/manager/src/main/java/com/alibaba/otter/canal/instance/manager/CanalInstanceWithManager.java`

这个类在构造函数中按序初始化五大组件：

```java
public CanalInstanceWithManager(Canal canal, String filter) {
    this.parameters = canal.getCanalParameter();
    this.destination = canal.getName();
    this.filter = filter;

    initAlarmHandler();  // 1. 报警（默认 LogAlarmHandler，只打日志）
    initMetaManager();   // 2. 位点管理
    initEventStore();    // 3. RingBuffer
    initEventSink();     // 4. Sink
    initEventParser();   // 5. Parser（最复杂）
}
```

**initMetaManager() 的模式选择**：

| MetaMode 配置 | 实现类 | 特点 |
|-------------|--------|------|
| MEMORY | `MemoryMetaManager` | 纯内存，重启丢失所有位点 |
| ZOOKEEPER | `ZooKeeperMetaManager` | ZK 持久化，支持 HA |
| MIXED | `PeriodMixedMetaManager` | 内存 + 定期刷 ZK |
| LOCAL_FILE | `FileMixedMetaManager` | 内存 + 定期刷本地文件（默认） |

**initEventStore()** 目前只支持 MEMORY 模式，即 `MemoryEventStoreWithBuffer`（基于 RingBuffer 的内存存储，后续文档详细分析）。

**initEventParser()** 是最复杂的初始化——需要配置数据库连接信息、心跳检测、slaveId、binlog 位点、表结构追踪（TSDB）、并行解析线程数、事件过滤规则等几十个参数。具体内容在 Parse 模块文档中详述。

### 6.6 Spring XML 配置体系（SPRING 模式）

Canal 提供了四种预置的 Spring XML 配置：

| 配置文件 | MetaManager | LogPositionManager | 适用场景 |
|---------|-------------|-------------------|---------|
| `memory-instance.xml` | MemoryMetaManager | MemoryLogPositionManager | 开发测试 |
| `file-instance.xml` | FileMixedMetaManager | Failback(Memory+Meta) | **生产默认** |
| `default-instance.xml` | PeriodMixedMetaManager(ZK) | Failback(Memory+Meta) | 集群 HA |
| `group-instance.xml` | 同上 | 同上 | 多数据源分组 |

以默认的 `file-instance.xml` 为例：

```xml
<!-- 核心 bean：CanalInstanceWithSpring -->
<bean id="instance" class="CanalInstanceWithSpring">
    <property name="destination" value="${canal.instance.destination}" />
    <property name="eventParser" ref="eventParser" />
    <property name="eventSink" ref="eventSink" />
    <property name="eventStore" ref="eventStore" />
    <property name="metaManager" ref="metaManager" />
    <property name="alarmHandler" ref="alarmHandler" />
</bean>

<!-- MetaManager：内存 + 文件定期刷盘 -->
<bean id="metaManager" class="FileMixedMetaManager">
    <property name="dataDir" value="${canal.file.data.dir:../conf}" />
    <property name="period" value="${canal.file.flush.period:1000}" />  <!-- 1秒刷盘 -->
</bean>

<!-- EventStore：RingBuffer -->
<bean id="eventStore" class="MemoryEventStoreWithBuffer">
    <property name="bufferSize" value="${canal.instance.memory.buffer.size:16384}" />
    <property name="bufferMemUnit" value="${canal.instance.memory.buffer.memunit:1024}" />
    <property name="batchMode" value="${canal.instance.memory.batch.mode:MEMSIZE}" />
</bean>

<!-- EventSink：单源 Sink -->
<bean id="eventSink" class="EntryEventSink">
    <property name="eventStore" ref="eventStore" />
</bean>

<!-- LogPositionManager：两级降级（内存 → Meta） -->
<bean id="logPositionManager" class="FailbackLogPositionManager">
    <constructor-arg>
        <bean class="MemoryLogPositionManager" />
    </constructor-arg>
    <constructor-arg>
        <bean class="MetaLogPositionManager">
            <constructor-arg ref="metaManager"/>
        </bean>
    </constructor-arg>
</bean>
```

配置加载优先级：`canal.properties` → `{destination}/instance.properties`（后者覆盖前者），同时支持 System Properties 覆盖。

---

## 第七阶段：CanalServerWithEmbedded —— 嵌入式数据服务

**源码位置**: `server/src/main/java/com/alibaba/otter/canal/server/embedded/CanalServerWithEmbedded.java`

### 7.1 设计模式

CanalServerWithEmbedded 采用**单例模式**（静态内部类实现），同时实现了 `CanalServer`（生命周期管理）和 `CanalService`（数据服务 API）两个接口。它是 Canal 数据消费链路的核心——所有的 subscribe/get/ack/rollback 操作最终都由它完成，无论是 Netty TCP 客户端还是 MQ 模式的内置消费者。

### 7.2 subscribe() —— 客户端订阅

```java
public void subscribe(ClientIdentity clientIdentity) {
    checkStart(clientIdentity.getDestination());
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

    // 确保 metaManager 已启动
    if (!canalInstance.getMetaManager().isStart()) {
        canalInstance.getMetaManager().start();
    }

    // 记录订阅信息
    canalInstance.getMetaManager().subscribe(clientIdentity);

    // 获取或初始化消费位点
    Position position = canalInstance.getMetaManager().getCursor(clientIdentity);
    if (position == null) {
        // 首次订阅，从 eventStore 获取第一条数据的位置
        position = canalInstance.getEventStore().getFirstPosition();
        if (position != null) {
            canalInstance.getMetaManager().updateCursor(clientIdentity, position);
        }
    }

    // 通知订阅关系变化（更新 Parser 的 filter）
    canalInstance.subscribeChange(clientIdentity);
}
```

### 7.3 getWithoutAck() —— 不自动确认的数据获取

```java
public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize,
                             Long timeout, TimeUnit unit) {
    checkStart(clientIdentity.getDestination());
    checkSubscribe(clientIdentity);
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

    synchronized (canalInstance) {  // 保证同一个 instance 的操作串行化
        // 检查是否有未 ack 的流式数据
        PositionRange<LogPosition> positionRanges =
            canalInstance.getMetaManager().getLastestBatch(clientIdentity);

        Events<Event> events = null;
        if (positionRanges != null) {
            // 存在未 ack 的数据，从上次位置继续
            events = getEvents(canalInstance.getEventStore(),
                positionRanges.getStart(), batchSize, timeout, unit);
        } else {
            // ack 后第一次获取
            Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
            if (start == null) {
                start = canalInstance.getEventStore().getFirstPosition();
            }
            events = getEvents(canalInstance.getEventStore(),
                start, batchSize, timeout, unit);
        }

        if (CollectionUtils.isEmpty(events.getEvents())) {
            return new Message(-1, true, new ArrayList<>());  // 空包
        } else {
            // 在 MetaManager 中记录这个 batch
            Long batchId = canalInstance.getMetaManager()
                .addBatch(clientIdentity, events.getPositionRange());
            // 提取 Entry 列表
            List entrys = events.getEvents().stream()
                .map(Event::getRawEntry).collect(Collectors.toList());
            return new Message(batchId, true, entrys);
        }
    }
}
```

### 7.4 ack() —— 确认消费

```java
public void ack(ClientIdentity clientIdentity, long batchId) {
    checkStart(clientIdentity.getDestination());
    checkSubscribe(clientIdentity);
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

    // 从 meta 中移除 batch 记录
    PositionRange<LogPosition> positionRanges =
        canalInstance.getMetaManager().removeBatch(clientIdentity, batchId);
    if (positionRanges == null) {
        throw new CanalServerException("ack error, batchId is not exist");
    }

    // 更新 cursor 到 ack 位置
    if (positionRanges.getAck() != null) {
        canalInstance.getMetaManager().updateCursor(clientIdentity, positionRanges.getAck());
    }

    // 通知 eventStore 释放已消费的数据空间
    canalInstance.getEventStore().ack(positionRanges.getEnd(), positionRanges.getEndSeq());
}
```

### 7.5 rollback() —— 回滚

```java
public void rollback(ClientIdentity clientIdentity) {
    checkStart(clientIdentity.getDestination());
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

    synchronized (canalInstance) {
        // 清除所有未 ack 的 batch
        canalInstance.getMetaManager().clearAllBatchs(clientIdentity);
        // 回滚 eventStore 的 getSequence 到 ackSequence
        canalInstance.getEventStore().rollback();
    }
}
```

---

## 完整启动时序总结

```
CanalLauncher.main()
  ├── 加载 canal.properties
  ├── [可选] 创建 PlainCanalConfigClient，启动远程配置轮询
  └── CanalStarter.start()
        ├── [非TCP模式] SPI 加载 CanalMQProducer 并初始化
        ├── new CanalController(properties)  ★构造函数做大量初始化★
        │     ├── initGlobalConfig()  -- 读取全局配置，创建 instanceGenerator Lambda
        │     ├── initInstanceConfig() -- 解析 canal.destinations，创建 InstanceConfig
        │     ├── 创建 CanalServerWithEmbedded 单例，设置 instanceGenerator
        │     ├── [TCP模式] 创建 CanalServerWithNetty 单例
        │     ├── [有ZK] 初始化 ZkClientx
        │     ├── 配置 ServerRunningMonitors（HA回调：processActiveEnter/Exit）
        │     └── [autoScan] 创建 InstanceAction 和 InstanceConfigMonitor
        │
        ├── controller.start()  ★真正启动★
        │     ├── ZK 注册当前 server 节点
        │     ├── embeddedCanalServer.start()
        │     │     ├── 加载 CanalMetrics（SPI）
        │     │     └── 创建 LazyMap<destination, CanalInstance>
        │     ├── 遍历 destinations：
        │     │     ├── ServerRunningMonitor.start() -- ZK 抢占运行权
        │     │     │     └── processActiveEnter 回调：
        │     │     │           └── embeddedCanalServer.start(destination)
        │     │     │                 └── canalInstances.get(destination) -- 触发 LazyMap
        │     │     │                       └── instanceGenerator.generate(destination)
        │     │     │                             ├── [SPRING] 加载 Spring XML → 创建 CanalInstanceWithSpring
        │     │     │                             └── [MANAGER] 远程拉配置 → 创建 CanalInstanceWithManager
        │     │     │                                   ├── initAlarmHandler()
        │     │     │                                   ├── initMetaManager()
        │     │     │                                   ├── initEventStore()
        │     │     │                                   ├── initEventSink()
        │     │     │                                   └── initEventParser()
        │     │     │                       └── canalInstance.start()
        │     │     │                             ├── metaManager.start()
        │     │     │                             ├── alarmHandler.start()
        │     │     │                             ├── eventStore.start()
        │     │     │                             ├── eventSink.start()
        │     │     │                             └── eventParser.start() -- 开始 dump binlog
        │     │     └── instanceConfigMonitor.register(destination)
        │     ├── instanceConfigMonitor.start() -- 启动配置变更扫描
        │     └── canalServer.start() -- 启动 Netty TCP 服务
        │           ├── 创建 ServerBootstrap (NIO)
        │           ├── 构建 Pipeline:
        │           │     FixedHeaderFrameDecoder → Handshake → Auth → Session
        │           └── bind(ip:port)
        │
        ├── [MQ模式] CanalMQStarter.start(destinations)
        │     └── 每个 destination 一个 worker 线程
        │           循环 getWithoutAck → send → ack
        │
        └── [有admin.port] 启动 CanalAdminWithNetty 管理端口
```

---

## 关键设计亮点总结

### 1. 懒加载 + LazyMap

`CanalServerWithEmbedded` 中的 `canalInstances` 是 `ComputingMap`（Guava），首次通过 `get(destination)` 访问时才调用 `instanceGenerator.generate()` 创建 Instance。这意味着：配置了 100 个 destination 但只有 10 个客户端在消费时，只会创建 10 个 Instance，其余 90 个不占用任何资源（不连接 MySQL、不开线程）。

### 2. HA 机制 —— ZK 临时节点竞争

通过 ZooKeeper 的临时节点实现 destination 级别的主备竞争。同一时刻同一个 destination 只有一个 Canal Server 在运行，其他 Server 作为 standby 监听节点变化。当 active 节点宕机，临时节点自动删除，standby 节点感知到后立即抢占，实现秒级故障转移。

### 3. 双模式 Instance 创建

SPRING 模式通过 Spring XML 灵活装配组件（适合开发者定制），MANAGER 模式通过远程 API 拉取配置编程式构建（适合运维管理大量 Instance）。两种模式最终都产出相同的 `CanalInstance` 抽象，上层代码无感知。

### 4. Netty Pipeline 动态重构

认证完成后动态移除握手和认证 Handler，添加空闲检测 Handler。精确控制连接生命周期各阶段的处理逻辑，避免不必要的 Handler 处理开销。

### 5. 协议设计极简 + 高性能序列化

4 字节长度 + Protobuf body 的极简帧协议。GET 响应中对 raw 模式数据绕过 Protobuf Builder，直接用 `CodedOutputStream` 手工序列化，显著减少 GC 压力。

### 6. 三层配置热更新

| 层级 | 范围 | 机制 |
|------|------|------|
| CanalLauncher | 整个 canal.properties | 远程轮询 Admin API，MD5 变化时全量重启 |
| ManagerInstanceConfigMonitor | 单个 instance 配置 | 远程轮询 Admin API，发现增删改 |
| SpringInstanceConfigMonitor | 单个 instance 配置 | 本地文件系统扫描，检测文件修改时间变化 |

### 7. 组件启动顺序保证

AbstractCanalInstance 严格按"下游先启动、上游后启动"的顺序初始化组件：MetaManager → AlarmHandler → EventStore → EventSink → EventParser。停止时反序。这个设计保证了 Parser 开始产出数据时，整个数据管道已经就绪，不会丢数据。

---

## 附录一：CanalMQStarter —— MQ 模式的完整工作流程

> **这部分在干什么？**
>
> 当 `canal.serverMode` 配置为 kafka/rocketMQ/rabbitMQ/pulsarMQ 时，Canal 不再对外暴露 Netty TCP 端口等待客户端来拉数据，而是**主动**把数据推送到消息队列。CanalMQStarter 就是这个"内置推送者"——它在 Canal Server 内部扮演了一个"永不停歇的消费者客户端"，不断从 CanalServerWithEmbedded 拉取数据然后发送到 MQ。

**源码位置**: `server/src/main/java/com/alibaba/otter/canal/server/CanalMQStarter.java`

### MQ 模式的整体架构

```
                    ┌─────────────────────────────────────────────────┐
                    │              Canal Server JVM                    │
                    │                                                 │
                    │  CanalInstance (Parser → Sink → Store)          │
                    │       ↓                                         │
                    │  CanalServerWithEmbedded                       │
                    │       ↓ getWithoutAck()                        │
                    │  CanalMQStarter                                 │
                    │       ↓ canalMQProducer.send()                 │
                    │  ┌─────────────────────────────┐               │
                    │  │  CanalMQProducer (SPI)       │               │
                    │  │  ├─ CanalKafkaProducer       │               │
                    │  │  ├─ CanalRocketMQProducer    │               │
                    │  │  ├─ CanalRabbitMQProducer    │               │
                    │  │  └─ CanalPulsarMQProducer    │               │
                    │  └─────────────────────────────┘               │
                    └─────────────────────────────────────────────────┘
                                         │
                                         ▼
                              ┌────────────────────┐
                              │  Kafka / RocketMQ   │
                              │  RabbitMQ / Pulsar  │
                              └────────────────────┘
```

### Worker 线程模型

CanalMQStarter 为每个 destination 创建一个独立的 worker 线程，线程内部执行一个无限循环：

```java
public class CanalMQStarter {

    private volatile boolean           running = false;
    private ExecutorService            executorService;
    private CanalMQProducer            canalMQProducer;
    private CanalServerWithEmbedded    canalServer;
    private Map<String, CanalMQRunnable> canalMQWorks = new ConcurrentHashMap<>();

    public CanalMQStarter(CanalMQProducer canalMQProducer) {
        this.canalMQProducer = canalMQProducer;
    }

    public synchronized void start(String destinations) {
        // 解析 destinations 列表
        String[] dests = StringUtils.split(destinations, ",");
        for (String destination : dests) {
            startDestination(destination.trim());
        }
    }

    public synchronized void startDestination(String destination) {
        CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
        canalMQWorks.put(destination, canalMQRunnable);
        executorService.execute(canalMQRunnable);
    }
}
```

### 消费循环核心逻辑

```java
private class CanalMQRunnable implements Runnable {

    private String destination;

    CanalMQRunnable(String destination) {
        this.destination = destination;
    }

    public void run() {
        // 使用固定的 clientId=1001 订阅
        final ClientIdentity clientIdentity = new ClientIdentity(destination, (short) 1001, "");

        while (running) {
            try {
                // 1. 确保已订阅
                if (!canalServer.getCanalInstances().containsKey(destination)) {
                    try { Thread.sleep(3000); } catch (InterruptedException e) { break; }
                    continue;
                }
                canalServer.subscribe(clientIdentity);

                while (running) {
                    // 2. 拉取数据（不自动 ack）
                    Message message = canalServer.getWithoutAck(clientIdentity, batchSize);

                    if (message == null || message.getId() == -1L) {
                        // 无数据，短暂休眠
                        Thread.sleep(100);
                        continue;
                    }

                    // 3. 发送到 MQ
                    canalMQProducer.send(
                        canalInstance.getMqConfig(),  // MQ 配置（topic、partition 等）
                        message,                      // 数据
                        new CanalMQProducer.Callback() {
                            // 4. 发送成功回调：ack
                            @Override
                            public void commit() {
                                canalServer.ack(clientIdentity, message.getId());
                            }

                            // 5. 发送失败回调：rollback
                            @Override
                            public void rollback() {
                                canalServer.rollback(clientIdentity, message.getId());
                            }
                        }
                    );
                }
            } catch (Exception e) {
                logger.error("process error!", e);
                try { Thread.sleep(1000); } catch (InterruptedException ex) { break; }
            }
        }
    }
}
```

**关键设计分析：**

| 设计点 | 解释 |
|--------|------|
| clientId=1001 | MQ 模式使用固定的 clientId，与 TCP 客户端（默认 clientId=1001）不冲突——因为 MQ 模式下 Netty 已被禁用 |
| 异步回调 ack/rollback | 发送到 MQ 是异步的（Kafka producer.send 返回 Future），通过 Callback 在发送完成后决定 ack 还是 rollback |
| 无数据时 sleep 100ms | 避免空轮询消耗 CPU |
| 异常后 sleep 1s | 避免错误风暴，但不退出循环 |

### CanalMQProducer SPI 加载

MQ Producer 通过 SPI 机制动态加载，支持运行时替换：

```java
// CanalStarter.java
ExtensionLoader<CanalMQProducer> loader =
    ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
canalMQProducer = new ProxyCanalMQProducer(
    loader.getExtension(serverMode.toLowerCase())  // "kafka" / "rocketmq" / "rabbitmq" / "pulsarmq"
);
canalMQProducer.init(properties);
```

SPI 描述文件位于 `META-INF/canal/com.alibaba.otter.canal.connector.core.spi.CanalMQProducer`，每个 connector 模块（kafka-connector、rocketmq-connector 等）各自注册自己的实现。

---

## 附录二：Canal Admin 远程管理机制

### Admin 模式的配置体系

当配置了 `canal.admin.manager` 时，Canal Server 进入 Admin 管理模式：

```properties
# canal.properties
canal.admin.manager = 127.0.0.1:8089
canal.admin.port = 11110
canal.admin.user = admin
canal.admin.passwd = admin
```

### 远程配置拉取

`PlainCanalConfigClient` 通过 HTTP 请求从 Canal Admin 后台拉取配置：

```java
public class PlainCanalConfigClient {

    private String managerAddress;  // Admin 地址

    // 拉取全局 canal.properties
    public PlainCanal findServer(String currentMd5) {
        // GET /api/v1/config/server_polling?md5={currentMd5}
        // 如果 MD5 未变化，返回 null
        // 如果变化，返回新的配置内容 + 新 MD5
    }

    // 拉取单个 instance 的配置
    public String findInstance(String destination, String md5) {
        // GET /api/v1/config/instance_polling/{destination}?md5={md5}
    }
}
```

### CanalAdminWithNetty —— Admin 管理端口

Canal Admin 还通过一个独立的 Netty 端口（默认 11110）对外暴露管理 API，支持远程操作：

```java
public interface CanalAdmin {
    boolean check();                              // 健康检查
    boolean start(String destination);            // 启动 instance
    boolean stop(String destination);             // 停止 instance
    boolean restart(String destination);          // 重启 instance
    String getRunningInstances();                 // 获取运行中的 instance 列表
    boolean isRunning(String destination);        // 检查 instance 是否运行
    String listCanalLog();                        // 列出日志文件
    String canalLog(int lines);                   // 获取日志内容
    String listInstanceLog(String destination);   // 列出 instance 日志
    String instanceLog(String destination, String file, int lines);  // 获取 instance 日志
}
```

---

## 附录三：Prometheus 监控集成

### 监控指标加载

Canal 通过 Java SPI 机制加载 Prometheus 监控实现：

```java
// CanalServerWithEmbedded.start()
private void loadCanalMetrics() {
    ServiceLoader<CanalMetricsProvider> providers =
        ServiceLoader.load(CanalMetricsProvider.class);
    for (CanalMetricsProvider provider : providers) {
        this.metrics = provider.getService();
        break;
    }
}
```

**监控指标体系**：

```
canal_instance                    -- instance 级别指标
  ├── canal_instance_publish_blocking_time   -- Parser 投递 store 阻塞时间
  ├── canal_instance_received_binlog_bytes   -- 接收的 binlog 字节数
  ├── canal_instance_parser_mode             -- 解析模式（parallel/serial）
  ├── canal_instance_client_packets          -- 客户端请求包数
  ├── canal_instance_client_bytes            -- 客户端数据字节数
  ├── canal_instance_client_empty_batches    -- 空批次数
  ├── canal_instance_client_request_error    -- 客户端请求错误数
  ├── canal_instance_client_request_latency  -- 客户端请求延迟
  ├── canal_instance_sink_blocking_time      -- Sink 投递阻塞时间
  ├── canal_instance_store_produce_seq       -- Store putSequence
  ├── canal_instance_store_consume_seq       -- Store getSequence
  ├── canal_instance_store_produce_mem       -- Store putMemSize
  ├── canal_instance_store_consume_mem       -- Store getMemSize
  ├── canal_instance_put_rows                -- 写入行数
  ├── canal_instance_get_rows                -- 读取行数
  ├── canal_instance_ack_rows                -- 确认行数
  ├── canal_instance_traffic_delay           -- 数据延迟（当前时间 - binlog 时间）
  └── canal_instance_put_delay               -- 投递延迟
```

启动后可通过 `http://canal-server:11112/metrics` 访问 Prometheus 格式的指标数据。

---

## 附录四：配置参数速查表

### canal.properties 全局配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `canal.id` | 1 | Canal Server 的唯一标识 |
| `canal.ip` | 自动探测 | 绑定 IP |
| `canal.port` | 11111 | Netty TCP 端口 |
| `canal.zkServers` | 空 | ZooKeeper 地址（HA 模式必填）|
| `canal.serverMode` | tcp | 运行模式：tcp / kafka / rocketMQ / rabbitMQ / pulsarMQ |
| `canal.destinations` | example | destination 列表（逗号分隔）|
| `canal.auto.scan` | true | 是否自动扫描配置变更 |
| `canal.auto.scan.interval` | 5 | 扫描间隔（秒）|
| `canal.instance.global.mode` | spring | Instance 创建模式：spring / manager |
| `canal.instance.global.lazy` | false | 是否懒加载 |
| `canal.instance.global.spring.xml` | classpath:spring/file-instance.xml | Spring XML 路径 |
| `canal.admin.manager` | 空 | Canal Admin 地址 |
| `canal.admin.port` | 11110 | Admin 管理端口 |
| `canal.user` | 空 | 客户端认证用户名 |
| `canal.passwd` | 空 | 客户端认证密码 |
| `canal.withoutNetty` | false | 是否禁用 Netty |

### instance.properties 实例配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `canal.instance.master.address` | 必填 | MySQL 主库地址 |
| `canal.instance.master.journal.name` | 空 | 起始 binlog 文件名 |
| `canal.instance.master.position` | 空 | 起始 binlog 偏移量 |
| `canal.instance.master.timestamp` | 空 | 起始 binlog 时间戳 |
| `canal.instance.master.gtid` | 空 | 起始 GTID |
| `canal.instance.dbUsername` | 必填 | MySQL 用户名 |
| `canal.instance.dbPassword` | 必填 | MySQL 密码 |
| `canal.instance.connectionCharset` | UTF-8 | 连接字符集 |
| `canal.instance.filter.regex` | .\\..* | 表名过滤正则（白名单）|
| `canal.instance.filter.black.regex` | 空 | 表名过滤正则（黑名单）|
| `canal.instance.memory.buffer.size` | 16384 | RingBuffer 大小（必须 2 的幂）|
| `canal.instance.memory.buffer.memunit` | 1024 | 内存单元大小（字节）|
| `canal.instance.memory.batch.mode` | MEMSIZE | 批量模式：ITEMSIZE / MEMSIZE |
| `canal.instance.detecting.enable` | false | 是否开启心跳检测 |
| `canal.instance.detecting.sql` | select 1 | 心跳检测 SQL |
| `canal.instance.detecting.interval.time` | 3 | 心跳检测间隔（秒）|
| `canal.instance.detecting.retry.threshold` | 3 | 心跳失败重试阈值 |
| `canal.instance.tsdb.enable` | true | 是否开启表结构追踪 |
| `canal.instance.parser.parallel` | true | 是否并行解析 |
| `canal.instance.parser.parallelThreadSize` | CPU*60% | 并行解析线程数 |
| `canal.instance.parser.parallelBufferSize` | 256 | Disruptor RingBuffer 大小 |
| `canal.instance.standby.address` | 空 | MySQL 备库地址（HA 用）|
| `canal.instance.fallbackIntervalInSeconds` | 60 | HA 切换时位点回退时间（秒）|

---

## 附录五：ZooKeeper 节点结构

```
/otter/canal/
  ├── cluster/                                    -- 集群成员注册
  │     └── {ip}:{port}                          -- 临时节点（server 存活标志）
  │
  └── destinations/                               -- destination 管理
        └── {destination}/
              ├── running                         -- 临时节点（运行权竞争）
              │     数据: {"active":true, "address":"ip:port", "cid":xxx}
              │
              ├── cluster/                        -- 该 destination 的可用 server 列表
              │     └── {ip}:{port}              -- 临时节点
              │
              ├── {clientId}/                     -- 客户端消费信息
              │     ├── cursor                   -- 消费位点（JSON）
              │     ├── filter                   -- 过滤规则
              │     └── batch_mark/              -- batch 标记
              │           ├── 0000000001         -- batch 数据（JSON PositionRange）
              │           ├── 0000000002
              │           └── ...
              │
              └── parse/                          -- Parser 位点信息
                    数据: LogPosition JSON

  ZK 路径常量定义：
  - /otter/canal/cluster/{ip}:{port}                   -- server 注册
  - /otter/canal/destinations/{dest}/running            -- 运行权竞争
  - /otter/canal/destinations/{dest}/cluster/{ip}:{port} -- destination 级注册
  - /otter/canal/destinations/{dest}/{clientId}/cursor   -- 消费位点
  - /otter/canal/destinations/{dest}/{clientId}/filter   -- 过滤规则
  - /otter/canal/destinations/{dest}/{clientId}/batch_mark -- batch 目录
  - /otter/canal/destinations/{dest}/parse               -- parse 位点
```

**节点类型说明**：

| 节点 | 类型 | 作用 |
|------|------|------|
| cluster/{ip}:{port} | 临时（Ephemeral） | server 存活检测，server 宕机自动消失 |
| {dest}/running | 临时（Ephemeral） | 运行权竞争，持有者宕机自动释放 |
| {dest}/cluster/{ip}:{port} | 临时（Ephemeral） | destination 级别的 server 注册 |
| {dest}/{clientId}/cursor | 持久（Persistent） | 消费位点持久化，server 重启后可恢复 |
| {dest}/{clientId}/batch_mark/* | 持久顺序（PersistentSequential） | batch 记录，自动递增序号作 batchId |

---

## 附录六：常见问题的源码级解答

### Q1：Canal 如何保证不丢数据？

**三层保障机制**：

1. **事务边界持久化**：EventTransactionBuffer 按事务 END 攒批，位点只在事务提交后才持久化到 LogPositionManager。即使 Canal 中途崩溃，重启后也会从上次事务边界继续。

2. **at-least-once 语义**：客户端通过 getWithoutAck/ack 两步操作消费数据。如果客户端在 get 后、ack 前崩溃，Store 中的 getSequence 不会推进到 ackSequence 之后，下次 get 会重新读取未 ack 的数据。

3. **HA 切换位点回退**：当发生 MySQL 主备切换时，Canal 会将消费位点回退 `fallbackIntervalInSeconds`（默认 60 秒），然后按时间戳重新查找位点，宁可重复消费也不丢数据。

### Q2：Canal 如何处理 MySQL binlog 被清理？

Parser 模块通过 `dumpErrorCount` 计数器追踪 `errno=1236`（日志文件被清理）错误。当 `DirectLogFetcher.fetch()` 收到 MySQL 的 Error Packet 包含 "purged binary logs" 信息时，抛出 `ServerLogPurgedException`。MysqlEventParser 的 `processDumpError()` 会累加计数，当超过阈值时自动判断处理方式——如果配了 `autoResetLatestPosMode=true` 则跳到最新位点，否则尝试按时间戳回退查找。

### Q3：Canal 如何支持 MySQL 8.0？

Canal 通过版本探测做了多处兼容：
- **`show master status`** → MySQL 8.4 改为 `show binary log status`
- **`@master_binlog_checksum`** → 支持 CRC32 校验和协商
- **`binlog_row_metadata=FULL`** → 利用 `existOptionalMetaData` 对 UNSIGNED、NULLABLE 等属性做交叉校验
- **`caching_sha2_password`** → 通过 `MysqlConnector` 支持新的认证插件

### Q4：Canal 的表结构是怎么追踪的？

Canal 提供两种表结构追踪方式：

1. **实时查询**（默认）：每次遇到 TABLE_MAP 事件时，通过 `TableMetaCache` 缓存表结构。缓存 miss 时执行 `SHOW CREATE TABLE` 获取最新表结构。DDL 事件触发缓存失效。

2. **TSDB 模式**（`canal.instance.tsdb.enable=true`）：使用 `DatabaseTableMeta` 维护表结构的时间序列数据库（存储在 H2 或 MySQL 中）。每次 DDL 变更都记录历史快照，支持按位点或时间戳回滚到任意时刻的表结构。解决了"解析旧 binlog 时列数不匹配"的问题。

### Q5：Canal 的 Disruptor 并行解析是怎么保证顺序的？

MysqlMultiStageCoprocessor 设计了四阶段流水线：

- **Stage 1（dump 线程）**：从 Socket 读取 binlog 字节流，publish 到 RingBuffer
- **Stage 2（单线程）**：LogDecoder 解码 + TableMeta 建立——**必须单线程**，因为 TableMapEvent 必须在对应的 RowsEvent 之前处理
- **Stage 3（多线程）**：DML 行数据深度解析——多个 WorkHandler 并行处理不同 slot，提升吞吐
- **Stage 4（单线程）**：按 RingBuffer 序号顺序投递到 EventTransactionBuffer——**保证最终顺序 = binlog 原始顺序**

关键：Stage 3 虽然多线程并行，但 Stage 4 是单线程按序号消费的 BatchEventProcessor，所以"重活并行、投递保序"。

### Q6：Canal 和 MySQL 主从复制有什么本质区别？

从协议层面看，Canal 和真正的 MySQL Slave 完全相同——都是通过 `COM_BINLOG_DUMP` 命令请求 Master 推送 binlog 事件流。MySQL Master 无法区分连上来的是 Canal 还是真正的 Slave。

但 Canal 和真正的 Slave 有三个本质区别：

第一，Canal **不回放 SQL**。真正的 Slave 拿到 binlog 事件后会在本地执行 SQL（通过 SQL Thread 回放 relay log），目的是让备库的数据和主库保持一致。Canal 不做这件事——它只解析 binlog 事件，将其转换为结构化的 CanalEntry 数据（包含 before/after 镜像），然后交给下游消费者处理。Canal 是一个"数据管道"，不是一个"数据库副本"。

第二，Canal **不维护 relay log**。真正的 Slave 会把 Master 推送的 binlog 先写入本地的 relay log 文件，然后由另一个线程（SQL Thread）读取 relay log 回放。Canal 不写 relay log——它拿到 binlog 字节流后直接在内存中解析（通过 LogDecoder），解析完的结构化数据存入内存 RingBuffer（MemoryEventStoreWithBuffer）。这意味着 Canal 没有"两阶段"（IO Thread + SQL Thread）的设计，整个数据流是单通道的：网络读取 → 内存解析 → 内存存储 → 客户端消费。

第三，Canal **支持灵活的下游消费**。MySQL Slave 的唯一消费者就是 SQL Thread。Canal 的消费者可以是任意系统——通过 TCP 客户端消费、通过 MQ（Kafka/RocketMQ）消费、通过 Client Adapter 直接写入 ES/HBase/RDB。Canal 把"数据变更的获取"和"数据变更的应用"彻底解耦了。

### Q7：单机模式（不配 ZooKeeper）下 Canal 的行为有什么不同？

不配 ZooKeeper 时，Canal 进入"单机模式"。这时有几个显著的行为差异：

ServerRunningMonitor 在 `start()` 方法中检测到 `zkClient == null`，会直接调用 `processActiveEnter()` 触发 Instance 启动，不需要做任何抢占逻辑。没有 HA 竞争，也没有故障转移能力——Server 宕机后 Instance 就停止了，需要人工重启。

MetaManager 使用 `FileMixedMetaManager`（文件模式）而非 `ZooKeeperMetaManager`。消费位点存储在本地文件 `conf/{destination}/meta.dat` 中，JSON 格式。位点更新时先写到内存缓存，定期刷盘。如果 Server 宕机且没有正常关闭，可能会丢失最后一次刷盘之后的位点变更，导致客户端重启后重复消费少量数据。

LogPositionManager 使用 `FailbackLogPositionManager`——它以内存（`MemoryLogPositionManager`）为主、文件/ZK 为备。在单机模式下，failback 的备用实现同样是文件存储。

总结来说，单机模式适合开发测试和小规模数据同步场景。生产环境如果对可用性有要求，建议配置 ZooKeeper 实现 HA。

---

## 附录七：Canal 与同类工具对比

| 维度 | Canal | Maxwell | Debezium |
|------|-------|---------|----------|
| 开发语言 | Java | Java | Java |
| 协议模拟 | MySQL Slave 协议 | MySQL Slave 协议 | MySQL Slave 协议 |
| 数据输出格式 | Protobuf (CanalEntry) | JSON | JSON (CloudEvents) |
| 消费方式 | TCP Client / MQ / Adapter | Kafka / Kinesis / stdout | Kafka Connect |
| HA 机制 | ZooKeeper 临时节点竞争 | 无原生 HA | Kafka Connect 分布式模式 |
| 全量同步 | 通过 Client Adapter 支持 | 支持 bootstrap | 支持 snapshot |
| 表过滤 | Perl5 正则（Aviator） | 黑白名单 | 正则 include/exclude |
| DDL 处理 | 解析为 ROWDATA 事件 | 发送 DDL JSON | 发送 schema change 事件 |
| 监控 | Prometheus 指标 | HTTP 指标 | JMX + Kafka Connect metrics |
| 社区活跃度 | 阿里巴巴主导，中文社区活跃 | Zendesk 开源 | Red Hat 主导，国际社区活跃 |
| 适用场景 | 阿里云 RDS 生态、大规模集群 | 轻量级单机使用 | Kafka 生态深度集成 |

Canal 的核心优势在于：成熟的 HA 机制（ZooKeeper）、丰富的下游 Adapter 生态（RDB/ES/HBase/MQ）、阿里云 RDS 原生支持、以及经过阿里巴巴大规模生产验证的稳定性。它的设计哲学是"做好一个管道"——专注于 binlog 的获取和分发，把数据应用的逻辑留给消费者。

---

## 附录八：优雅停机流程

Canal 实现了完整的优雅停机机制，保证在进程退出前正确释放所有资源、持久化位点、关闭网络连接。

**JVM ShutdownHook 注册**（CanalLauncher.main）：

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    try {
        logger.info("## stop the canal server");
        canalStarter.stop();
    } catch (Throwable e) {
        logger.warn("## something goes wrong when stopping canal Server:", e);
    } finally {
        logger.info("## canal server is down.");
    }
}));
```

**CanalController.stop() 停机顺序**：

```java
public void stop() {
    // 1. 停止配置扫描器（不再接受新的 destination 变更）
    for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
        monitor.stop();
    }

    // 2. 逐个停止所有 destination 的 Monitor
    for (ServerRunningMonitor runningMonitor : ServerRunningMonitors.getRunningMonitors().values()) {
        if (runningMonitor.isStart()) {
            runningMonitor.stop();
            // → processActiveExit() → embeddedServer.stop(destination)
            // → AbstractCanalInstance.stop()
            //   → EventParser.stop()   ← 关闭 MySQL 连接，停止 dump 线程
            //   → EventSink.stop()     ← 停止过滤
            //   → EventStore.stop()    ← 释放 RingBuffer
            //   → AlarmHandler.stop()  ← 停止告警
            //   → MetaManager.stop()   ← 刷盘位点信息
        }
    }

    // 3. 停止 Embedded Server
    if (embeddedCanalServer != null) {
        embeddedCanalServer.stop();
    }

    // 4. 停止 Netty Server（关闭所有客户端连接）
    if (canalServerWithNetty != null) {
        canalServerWithNetty.stop();
    }

    // 5. 停止 MQ Starter（关闭 Kafka Producer 等）
    if (canalMQStarter != null) {
        canalMQStarter.stop();
    }

    // 6. 在 ZK 上注销当前 Server 节点
    if (zkClientx != null) {
        zkClientx.close();
    }
}
```

**停机顺序的关键设计**：先停 Parser（不再产出新数据），然后停 Sink 和 Store（清空管道），最后停 MetaManager（确保最后的位点被持久化）。这个"上游先停、下游后停"的顺序是 `AbstractCanalInstance.start()` 中"下游先启、上游后启"的**镜像反转**——启动和停止是完全对称的，避免数据在管道中丢失。

---

# 真实使用案例：不同场景下的源码全链路解析

> 下面列举了 14 种 Canal 在生产环境中最常见的使用场景。每个案例都从用户的配置出发，一步一步追踪底层源码的完整执行路径，不跳步。通过这些案例，你可以理解：同样是 Canal，不同的配置组合会走完全不同的代码分支，调起完全不同的组件，产生完全不同的行为。

---

## 案例一：TCP 单机模式 —— 最基础的 Java 客户端消费

### 场景描述

这是 Canal 最简单的使用方式：单台 Canal Server 监听单个 MySQL 实例，一个 Java 客户端通过 TCP 连接到 Canal Server，拉取变更数据后做业务处理（比如刷新缓存）。不使用 ZooKeeper、不使用 MQ、不使用 Adapter。

### 配置

```properties
# canal.properties
canal.serverMode = tcp
canal.port = 11111
canal.destinations = example
canal.instance.global.mode = spring
canal.instance.global.spring.xml = classpath:spring/file-instance.xml

# conf/example/instance.properties
canal.instance.master.address = 127.0.0.1:3306
canal.instance.dbUsername = canal
canal.instance.dbPassword = canal
canal.instance.filter.regex = mydb\\..*
```

```java
// 客户端代码
CanalConnector connector = CanalConnectors.newSingleConnector(
    new InetSocketAddress("127.0.0.1", 11111), "example", "", "");
connector.connect();
connector.subscribe("mydb\\..*");
while (true) {
    Message message = connector.getWithoutAck(100);
    long batchId = message.getId();
    if (batchId != -1 && message.getEntries().size() > 0) {
        // 处理变更数据...
    }
    connector.ack(batchId);
}
```

### 全链路源码追踪

**Step 1：CanalStarter.start() —— 判断运行模式**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalStarter.java`

```java
public synchronized void start() throws Throwable {
    String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
    // serverMode = "tcp"
    
    if (!"tcp".equalsIgnoreCase(serverMode)) {
        // Kafka/RocketMQ/RabbitMQ 等 MQ 模式
        // 本案例不走这里！
        canalMQProducer = loader.getExtension(serverMode.toLowerCase());
    }
    // serverMode == "tcp"，不加载任何 MQ Producer
    // canalMQProducer 保持 null
    
    // 构造 CanalController 并启动
    controller = new CanalController(properties);
    controller.start();
}
```

> **这一步在干什么？** CanalStarter 是整个 Canal 的启动编排器。它做的第一个分支判断就是看 `serverMode`：如果是 "tcp"，不加载任何 MQ 插件，直接走 Netty TCP 模式；如果是 "kafka" 等，会通过 SPI 加载对应的 MQ Producer。本案例是 tcp 模式，所以整个 MQ 相关的代码路径完全不会被触发。

**Step 2：CanalController 构造 —— 创建 Netty Server**

```java
// CanalController 构造函数
String canalWithoutNetty = getProperty(properties, CanalConstants.CANAL_WITHOUT_NETTY);
// canalWithoutNetty = null（没配置）
if (canalWithoutNetty == null || "false".equals(canalWithoutNetty)) {
    // TCP 模式走这里：创建 Netty Server
    canalServer = CanalServerWithNetty.instance();
    canalServer.setIp(ip);    // 默认自动探测本机 IP
    canalServer.setPort(port); // 默认 11111
}
```

同时，由于 `canal.zkServers` 未配置（单机模式），不会初始化 ZooKeeper 客户端：

```java
if (StringUtils.isNotEmpty(getProperty(properties, CanalConstants.CANAL_ZKSERVERS))) {
    // 单机模式不走这里！zkClientx 保持 null
    this.zkClientx = ZkClientx.getZkClient(...);
}
```

> **这一步在干什么？** 两个关键分支：1）`canalWithoutNetty` 为 null → 创建 CanalServerWithNetty（TCP 模式必须），如果是 MQ 模式会设为 "true" 跳过。2）`zkServers` 为空 → 不创建 ZK 客户端，单机模式没有 HA 竞争。

**Step 3：CanalController.start() —— 启动 Instance 和 Netty**

```java
public void start() throws Throwable {
    // 1. ZK 注册 —— 单机模式跳过
    // (zkClientx == null，不注册)
    
    // 2. 启动 Embedded Server
    embeddedCanalServer.start();
    
    // 3. 遍历 destinations，启动 Instance
    for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
        final String destination = entry.getKey(); // "example"
        ServerRunningMonitor runningMonitor = 
            ServerRunningMonitors.getRunningMonitor(destination);
        if (!config.getLazy() && !runningMonitor.isStart()) {
            runningMonitor.start();
        }
    }
    
    // 4. 启动 Netty Server
    if (canalServer != null) {
        canalServer.start(); // 绑定 11111 端口，开始接受 TCP 连接
    }
}
```

由于 `zkClientx == null`（单机模式），`ServerRunningMonitor.start()` 内部直接调用 `processActiveEnter()`，不做任何 ZK 竞争：

```java
// ServerRunningMonitor.start()
public void start() {
    processStart();
    if (zkClient != null) {
        // HA 模式走这里
    } else {
        // 单机模式直接启动
        processActiveEnter(); // → embeddedCanalServer.start("example")
    }
}
```

> **这一步在干什么？** 单机模式的启动路径非常简洁——没有 ZK 注册、没有 HA 竞争、没有 running 节点创建。`processActiveEnter()` 直接触发 `embeddedCanalServer.start("example")`，Instance 立刻开始连接 MySQL、dump binlog。


**Step 4：CanalServerWithNetty.start() —— Netty Pipeline 建立 TCP 监听**

```java
// CanalServerWithNetty.start()
public void start() {
    this.bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));

    bootstrap.setPipelineFactory(() -> {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast(FixedHeaderFrameDecoder.class.getName(),
            new FixedHeaderFrameDecoder());           // 4 字节长度帧解码
        pipeline.addLast(HandshakeInitializationHandler.class.getName(),
            new HandshakeInitializationHandler(childGroups));  // 握手
        pipeline.addLast(ClientAuthenticationHandler.class.getName(),
            new ClientAuthenticationHandler(embeddedServer));  // 认证
        pipeline.addLast(SessionHandler.class.getName(),
            new SessionHandler(embeddedServer));       // 业务处理
        return pipeline;
    });

    // 绑定 11111 端口
    this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
}
```

> **这一步在干什么？** Netty 3.x 的 Pipeline 由 4 个 Handler 组成。客户端 TCP 连接建立后，会依次经过握手（发送随机 seed）、认证（校验密码）、会话处理（dispatch subscribe/get/ack 请求）。这是 TCP 模式独有的网络层——Kafka 模式下 Netty 不会启动。

**Step 5：SimpleCanalConnector.connect() —— 客户端建连**

```java
// SimpleCanalConnector.doConnect()
private void doConnect() throws CanalClientException {
    // 1. 建立 TCP 连接
    this.channel = SocketChannel.open();
    this.channel.connect(this.address);  // TCP 三次握手

    // 2. 接收 HANDSHAKE 包（含随机 seed）
    Packet packet = Packet.parseFrom(readNextPacket());
    // packet.type == HANDSHAKE
    Handshake handshake = Handshake.parseFrom(packet.getBody());
    byte[] seed = handshake.getSeeds().toByteArray();  // 服务端生成的随机种子

    // 3. 用 seed 加密密码，发送 CLIENTAUTHENTICATION
    ByteString scrambled = ByteString.copyFrom(
        SecurityUtil.scramble411(passwd.getBytes(), seed));
    ClientAuth clientAuth = ClientAuth.newBuilder()
        .setUsername(user)
        .setPassword(scrambled)
        .setNetReadTimeout(idleTimeout)
        .setNetWriteTimeout(idleTimeout)
        .build();
    writeWithHeader(Packet.newBuilder()
        .setType(PacketType.CLIENTAUTHENTICATION)
        .setBody(clientAuth.toByteString())
        .build().toByteArray());

    // 4. 接收 ACK
    Packet ackPacket = Packet.parseFrom(readNextPacket());
    Ack ack = Ack.parseFrom(ackPacket.getBody());
    if (ack.getErrorCode() > 0) {
        throw new CanalClientException("auth failed: " + ack.getErrorMessage());
    }
    // 连接成功！
    this.connected = true;
}
```

> **这一步在干什么？** 客户端的连接过程完全模拟了 MySQL 的握手认证协议——服务端先发 seed，客户端用 seed 对密码做 SHA1 加扰（scramble411 算法），服务端比对。密码不会以明文在网络上传输。整个过程使用 Protobuf 编解码，帧格式是"4 字节长度 + body"。

**Step 6：SimpleCanalConnector.subscribe() —— 订阅 destination**

```java
public void subscribe(String filter) throws CanalClientException {
    Sub sub = Sub.newBuilder()
        .setDestination(this.destination)  // "example"
        .setClientId(String.valueOf(this.clientId))  // 默认 1001
        .setFilter(filter != null ? filter : "")  // "mydb\\..*"
        .build();

    writeWithHeader(Packet.newBuilder()
        .setType(PacketType.SUBSCRIPTION)
        .setBody(sub.toByteString())
        .build().toByteArray());

    // 等待 ACK
    Packet packet = Packet.parseFrom(readNextPacket());
    Ack ack = Ack.parseFrom(packet.getBody());
    // 订阅成功
}
```

服务端 `SessionHandler` 收到 SUBSCRIPTION 包后调用 `embeddedServer.subscribe(clientIdentity)`，服务端处理：

```java
// CanalServerWithEmbedded.subscribe()
public void subscribe(ClientIdentity clientIdentity) {
    CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
    // "example" → 找到对应的 Instance

    canalInstance.getMetaManager().subscribe(clientIdentity);
    // 存储客户端信息到 MetaManager（文件或 ZK）

    // 如果客户端带了 filter，动态替换 Parser 的过滤规则
    if (StringUtils.isNotEmpty(clientIdentity.getFilter())) {
        canalInstance.subscribeChange(clientIdentity);
        // → 更新 eventParser 的 eventFilter
    }
}
```

> **这一步在干什么？** 客户端订阅触发了两件事：1）MetaManager 记录这个客户端的信息（clientId、filter），后续 ack/rollback 都基于这个客户端标识。2）如果客户端的 filter 和 Instance 初始配置不同，**动态替换** Parser 的过滤规则——这意味着客户端可以在运行时缩小或扩大监听范围。

**Step 7：SimpleCanalConnector.getWithoutAck() —— 拉取变更数据**

```java
// 客户端
public Message getWithoutAck(int batchSize) throws CanalClientException {
    Get get = Get.newBuilder()
        .setDestination(destination)
        .setClientId(String.valueOf(clientId))
        .setFetchSize(batchSize)  // 100
        .setAutoAck(false)
        .setTimeout(-1)  // 无超时
        .setUnit(-1)
        .build();

    writeWithHeader(Packet.newBuilder()
        .setType(PacketType.GET)
        .setBody(get.toByteString())
        .build().toByteArray());

    // 接收 MESSAGES 响应
    Packet packet = Packet.parseFrom(readNextPacket());
    return CanalMessageDeserializer.deserializer(packet.getBody());
}
```

服务端处理链路：

```java
// CanalServerWithEmbedded.getWithoutAck()
public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, ...) {
    CanalInstance instance = canalInstances.get(clientIdentity.getDestination());

    // 1. 获取当前消费位点
    Position start = instance.getMetaManager().getCursor(clientIdentity);

    // 2. 从 RingBuffer 中读取数据
    Events<Event> events = instance.getEventStore().get(start, batchSize, timeout, unit);
    // → MemoryEventStoreWithBuffer.get()
    //   从 RingBuffer 中按 getSequence 读取 batchSize 条数据
    //   推进 getSequence

    // 3. 记录 batch（不 ack，等客户端确认）
    Long batchId = instance.getMetaManager().addBatch(
        clientIdentity, events.getPositionRange());

    // 4. 组装 Message 返回
    Message message = new Message(batchId, events.getEvents());
    return message;
}
```

> **这一步在干什么？** 这是数据消费的核心路径。客户端发送 GET 请求 → 服务端从 MemoryEventStoreWithBuffer 的 RingBuffer 中读取数据 → MetaManager 记录 batch 信息（batchId → positionRange 映射）→ 返回给客户端。`getWithoutAck` 意味着读完不自动推进 cursor，必须客户端显式 ack。

**Step 8：SimpleCanalConnector.ack() —— 确认消费**

```java
// 客户端
public void ack(long batchId) throws CanalClientException {
    ClientAck clientAck = ClientAck.newBuilder()
        .setDestination(destination)
        .setClientId(String.valueOf(clientId))
        .setBatchId(batchId)
        .build();

    writeWithHeader(Packet.newBuilder()
        .setType(PacketType.CLIENTACK)
        .setBody(clientAck.toByteString())
        .build().toByteArray());
}
```

服务端：

```java
// CanalServerWithEmbedded.ack()
public void ack(ClientIdentity clientIdentity, long batchId) {
    CanalInstance instance = canalInstances.get(clientIdentity.getDestination());

    // 1. 从 MetaManager 移除 batch 记录，获取 positionRange
    PositionRange<LogPosition> positionRange =
        instance.getMetaManager().removeBatch(clientIdentity, batchId);

    // 2. 推进 EventStore 的 ackSequence
    instance.getEventStore().ack(positionRange.getEnd());
    // → MemoryEventStoreWithBuffer.ack()
    //   ackSequence 前移，RingBuffer 腾出空间给 Parser 写入新数据

    // 3. 更新 MetaManager 的消费游标（cursor）
    instance.getMetaManager().updateCursor(clientIdentity, positionRange.getEnd());
    // → FileMixedMetaManager：内存更新 + 周期刷盘
}
```

> **这一步在干什么？** ack 触发三个关键动作：1）从 MetaManager 清除 batch 记录（该 batch 不会再被 rollback）。2）推进 Store 的 ackSequence——这是 RingBuffer 的"回收"指针，ack 之后该区间的 buffer slot 可以被 Parser 重用。3）更新消费游标到 batch 的末尾位置，下次 get 从这个位置继续。

**TCP 单机模式 —— 完整数据流图**

```
  Java Client (JVM)                    Canal Server (JVM)                     MySQL
       |                                     |                                  |
   connect() ----TCP三次握手-------------→ accept()                              |
       |←------HANDSHAKE(seed)------------ HandshakeHandler                    |
   scramble411() ----CLIENTAUTH----------→ ClientAuthHandler                    |
       |←------ACK(success)-------------- (Pipeline 重构)                      |
       |                                     |                                  |
   subscribe("mydb\\..*") --SUBSCRIPTION--→ embeddedServer.subscribe()          |
       |                                  → MetaManager.subscribe()             |
       |                                  → eventParser.setFilter()             |
       |←------ACK(success)-------------- (动态更新过滤规则)                    |
       |                                     |                                  |
       |                              Instance.start()                          |
       |                              → MysqlEventParser.start()                |
       |                                → MysqlConnection.connect()  --------→ MySQL
       |                                → COM_REGISTER_SLAVE        --------→  |
       |                                → COM_BINLOG_DUMP           --------→  |
       |                                ←--- binlog event stream ----------- dump
       |                                → LogDecoder.decode()                   |
       |                                → LogEventConvert.parse()               |
       |                                → AviaterRegexFilter.filter()           |
       |                                → EventTransactionBuffer.add()          |
       |                                → EventStore.put()  (RingBuffer)        |
       |                                     |                                  |
   getWithoutAck(100) ----GET------------→ eventStore.get()                     |
       |                                  → MetaManager.addBatch()              |
       |←------MESSAGES(batch)----------- (batchId + entries)                  |
       |                                     |                                  |
   处理业务逻辑（刷新缓存等）               |                                  |
       |                                     |                                  |
   ack(batchId) ----CLIENTACK------------→ eventStore.ack()                     |
       |                                  → MetaManager.updateCursor()          |
       |                                  → MetaManager.removeBatch()           |
       |                                     |                                  |
   (循环继续...)                            |                                  |
```

---

## 案例二：Kafka MQ 模式 —— 变更数据自动推送到 Kafka

### 场景描述

电商平台的订单系统使用 MySQL 存储订单数据。需要把订单表的变更实时推送到 Kafka，供下游多个消费系统（搜索引擎、数据分析、风控）各自消费。不需要写 Java 客户端——Canal 自己就是"内置的消费者+生产者"。

### 配置

```properties
# canal.properties
canal.serverMode = kafka
canal.destinations = order_sync
canal.mq.servers = kafka-broker1:9092,kafka-broker2:9092
canal.mq.flatMessage = true
canal.mq.compressionType = lz4
canal.mq.acks = all

# conf/order_sync/instance.properties
canal.instance.master.address = 10.0.1.100:3306
canal.instance.dbUsername = canal
canal.instance.dbPassword = canal
canal.instance.filter.regex = orderdb\\.orders,orderdb\\.order_items
canal.mq.topic = canal_orders
canal.mq.dynamicTopic = orderdb\\.orders:topic_orders,orderdb\\.order_items:topic_order_items
canal.mq.partitionsNum = 8
canal.mq.partitionHash = orderdb\\.orders:order_id,orderdb\\.order_items:order_id
```

### 全链路源码追踪

**Step 1：CanalStarter —— SPI 加载 CanalKafkaProducer**

```java
// CanalStarter.start()
String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
// serverMode = "kafka"，不等于 "tcp"

if (!"tcp".equalsIgnoreCase(serverMode)) {
    // 走这里！通过 SPI 加载 Kafka Producer
    ExtensionLoader<CanalMQProducer> loader =
        ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
    canalMQProducer = new ProxyCanalMQProducer(
        loader.getExtension(serverMode.toLowerCase(),  // "kafka"
            CONNECTOR_SPI_DIR,        // "META-INF/canal/"
            CONNECTOR_STANDBY_SPI_DIR // "META-INF/canal/standby/"
        )
    );
}
```

> **这一步在干什么？** `serverMode=kafka` 触发 SPI 加载。Canal 在 `META-INF/canal/com.alibaba.otter.canal.connector.core.spi.CanalMQProducer` 中注册了 `kafka=com.alibaba.otter.canal.connector.kafka.producer.CanalKafkaProducer`。`ProxyCanalMQProducer` 包装了一层 ClassLoader 隔离——因为 Kafka client JAR 是在 `plugin/` 目录下独立加载的，不与 Canal 核心的 ClassLoader 混用。

**Step 2：CanalKafkaProducer.init() —— 初始化 Kafka Producer**

```java
// CanalKafkaProducer.init()
public void init(Properties properties) {
    KafkaProducerConfig kafkaConfig = new KafkaProducerConfig();

    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", kafkaConfig.getServers());
    kafkaProps.put("acks", kafkaConfig.getAcks());
    kafkaProps.put("compression.type", kafkaConfig.getCompressionType());
    kafkaProps.put("batch.size", kafkaConfig.getBatchSize());
    kafkaProps.put("linger.ms", kafkaConfig.getLingerMs());
    kafkaProps.put("max.request.size", kafkaConfig.getMaxRequestSize());
    kafkaProps.put("buffer.memory", kafkaConfig.getBufferMemory());
    // 关键：保证分区内有序
    kafkaProps.put("max.in.flight.requests.per.connection", 1);

    kafkaProps.put("key.serializer", StringSerializer.class.getName());
    kafkaProps.put("value.serializer", ByteArraySerializer.class.getName());

    producer = new KafkaProducer<>(kafkaProps);
}
```

> **这一步在干什么？** 创建 Kafka 原生 Producer。注意 `max.in.flight.requests.per.connection=1` 这个硬编码——这是为了保证同一分区内消息的严格有序性（如果允许多个 in-flight 请求，网络重传可能导致乱序）。

**Step 3：CanalController 构造 —— 禁用 Netty**

```java
// CanalStarter.start() 中，MQ 模式设置 withoutNetty
if (canalMQProducer != null) {
    System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
}
```

```java
// CanalController 构造函数
String canalWithoutNetty = getProperty(properties, CanalConstants.CANAL_WITHOUT_NETTY);
// canalWithoutNetty = "true"
if (canalWithoutNetty == null || "false".equals(canalWithoutNetty)) {
    // Kafka 模式不走这里！CanalServerWithNetty 不会被创建
    canalServer = CanalServerWithNetty.instance();
}
// canalServer 保持 null
```

> **这一步在干什么？** Kafka 模式不需要 TCP 端口（没有外部客户端来连接），所以 Netty 被禁用。11111 端口不会被监听。数据消费由内部的 CanalMQStarter 完成。

**Step 4：CanalMQStarter.start() —— 为每个 destination 创建 Worker 线程**

```java
// CanalStarter.start() 中
canalMQStarter = new CanalMQStarter(canalMQProducer);
controller.start();
String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
canalMQStarter.start(destinations);  // "order_sync"
```

```java
// CanalMQStarter.start()
public synchronized void start(String destinations) {
    String[] dests = StringUtils.split(destinations, ",");
    for (String destination : dests) {
        startDestination(destination.trim());  // "order_sync"
    }
}

public synchronized void startDestination(String destination) {
    CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
    canalMQWorks.put(destination, canalMQRunnable);
    executorService.execute(canalMQRunnable);
    // 启动一个独立线程，不断从 embeddedServer 拉数据、发到 Kafka
}
```

> **这一步在干什么？** CanalMQStarter 为每个 destination 创建一个 Worker 线程。这个线程扮演了"内置客户端"的角色——它使用 `clientId=1001` 订阅 CanalServerWithEmbedded，不断拉取数据，然后通过 CanalKafkaProducer 发送到 Kafka。

**Step 5：CanalMQRunnable.run() —— 消费循环**

```java
// CanalMQRunnable.run() 核心循环
while (running) {
    canalServer.subscribe(clientIdentity);

    while (running) {
        Message message = canalServer.getWithoutAck(clientIdentity, batchSize);

        if (message == null || message.getId() == -1L) {
            Thread.sleep(100);  // 无数据时休眠 100ms
            continue;
        }

        MQDestination mqDestination = new MQDestination();
        mqDestination.setTopic(mqConfig.getTopic());             // "canal_orders"
        mqDestination.setDynamicTopic(mqConfig.getDynamicTopic());
        mqDestination.setPartitionsNum(mqConfig.getPartitionsNum()); // 8
        mqDestination.setPartitionHash(mqConfig.getPartitionHash());

        canalMQProducer.send(mqDestination, message, new Callback() {
            public void commit() {
                canalServer.ack(clientIdentity, message.getId());
            }
            public void rollback() {
                canalServer.rollback(clientIdentity, message.getId());
            }
        });
    }
}
```

> **这一步在干什么？** Worker 线程执行一个无限循环：从 Store 拉取 → 发到 Kafka → 异步回调 ack/rollback。Kafka 的 `producer.send()` 是异步的，只有当 Kafka broker 确认收到后才调用 commit()（ack），如果发送失败则调用 rollback()——数据会被重新拉取。

**Step 6：CanalKafkaProducer.send() —— 动态 Topic + 分区哈希**

```java
// CanalKafkaProducer.send()
public void send(MQDestination destination, Message message, Callback callback) {
    if (StringUtils.isNotEmpty(destination.getDynamicTopic())) {
        // 动态 topic 模式：按表名路由到不同 topic
        Map<String, Message> topicMessages =
            MQMessageUtils.messageTopics(message, destination.getTopic(),
                destination.getDynamicTopic());
        // topicMessages = {
        //   "topic_orders": [orders 表的变更],
        //   "topic_order_items": [order_items 表的变更]
        // }

        ExecutorTemplate template = new ExecutorTemplate(sendExecutor);
        for (Map.Entry<String, Message> entry : topicMessages.entrySet()) {
            final String topicName = entry.getKey().replace(".", "_");
            final Message topicMsg = entry.getValue();
            template.submit(() -> {
                sendMessage(topicName, topicMsg, destination);
            });
        }
        template.waitForResult();  // 等待所有 topic 发送完成
    } else {
        sendMessage(destination.getTopic(), message, destination);
    }

    callback.commit();  // 全部发送成功，ack
}
```

```java
// sendMessage() —— 分区路由
private void sendMessage(String topic, Message message, MQDestination destination) {
    if (destination.getPartitionHash() != null
        && !destination.getPartitionHash().isEmpty()) {
        // 哈希分区模式：按 PK 字段值哈希
        Message[] partitionMessages = MQMessageUtils.messagePartition(
            message, destination.getPartitionsNum(),  // 8
            destination.getPartitionHash(),
            mqProperties.isDatabaseHash());
        // partitionMessages[0..7]：8 个分区对应的消息

        for (int i = 0; i < partitionMessages.length; i++) {
            Message partMsg = partitionMessages[i];
            if (partMsg != null) {
                byte[] body = flatMessage ?
                    JSON.toJSONBytes(FlatMessage.messageConverter(partMsg)) :
                    CanalMessageSerializerUtil.serializer(partMsg);
                ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>(topic, i, null, body);
                Future<RecordMetadata> future = producer.send(record);
                futures.add(future);
            }
        }
        producer.flush();
        for (Future<RecordMetadata> future : futures) {
            future.get();  // 阻塞等待 Kafka 确认
        }
    }
}
```

> **这一步在干什么？** 这是 Kafka 模式最核心的逻辑，包含两层路由：第一层是**动态 Topic 路由**，按表名将不同表的变更路由到不同的 Kafka Topic。第二层是**分区哈希路由**，按指定字段（order_id）的值做哈希取模，保证同一个 order_id 的所有变更始终落到同一个 Kafka 分区，下游消费者只需要保证"同分区内顺序消费"即可保证同一订单的变更顺序。

**Kafka 模式 —— 完整数据流图**

```
  MySQL                    Canal Server                              Kafka
    |                          |                                       |
    |--binlog events-------→ MysqlEventParser                         |
    |                        → LogDecoder                              |
    |                        → LogEventConvert                         |
    |                        → AviaterRegexFilter                      |
    |                          (只保留 orders + order_items)            |
    |                        → EventStore (RingBuffer)                 |
    |                          |                                       |
    |                        CanalMQStarter (Worker 线程)               |
    |                        → getWithoutAck(batchSize)                |
    |                          |                                       |
    |                        CanalKafkaProducer.send()                 |
    |                        → messageTopics()                         |
    |                          |  orders → topic_orders               |
    |                          |  order_items → topic_order_items      |
    |                        → messagePartition()                      |
    |                          |  hash(order_id) % 8 → partition      |
    |                        → producer.send(ProducerRecord) --------→ |
    |                        → producer.flush()                  ack   |
    |                        ← future.get() (等待确认)  ←-----------   |
    |                        → callback.commit()                       |
    |                        → canalServer.ack()                       |
    |                        → eventStore.ack() (回收 RingBuffer)      |
```

---

## 案例三：ZooKeeper HA 双机热备 —— 故障自动转移

### 场景描述

核心交易数据库的 Canal 同步必须 7x24 高可用。部署两台 Canal Server 配置相同的 destination，通过 ZooKeeper 实现主备竞争。Active 宕机后 Standby 秒级接管。

### 配置

```properties
# 两台 Server 的 canal.properties 完全相同（除了 canal.id）
# Server A: canal.id = 1
# Server B: canal.id = 2
canal.zkServers = zk1:2181,zk2:2181,zk3:2181
canal.serverMode = tcp
canal.destinations = trade_sync
canal.instance.global.spring.xml = classpath:spring/default-instance.xml
```

### 全链路源码追踪

**Step 1：CanalController 构造 —— 初始化 ZooKeeper 客户端**

```java
String zkServers = getProperty(properties, CanalConstants.CANAL_ZKSERVERS);
// zkServers = "zk1:2181,zk2:2181,zk3:2181"（非空）

if (StringUtils.isNotEmpty(zkServers)) {
    // HA 模式走这里！
    this.zkClientx = ZkClientx.getZkClient(zkServers);
}
```

**Step 2：配置 ServerRunningMonitor 的 4 个回调**

```java
ServerRunningData serverData = new ServerRunningData(
    cid, ip + ":" + port);  // "10.0.1.1:11111"

ServerRunningMonitors.setRunningMonitors(
    MigrateMap.makeComputingMap(destination -> {
        ServerRunningMonitor monitor = new ServerRunningMonitor(serverData);
        monitor.setDestination(destination);
        monitor.setListener(new ServerRunningListener() {
            public void processActiveEnter() {
                // 抢到运行权 → 启动 Instance
                embeddedCanalServer.start(destination);
            }
            public void processActiveExit() {
                // 失去运行权 → 停止 Instance
                embeddedCanalServer.stop(destination);
            }
            public void processStart() {
                // 注册 ZK 集群节点
                if (zkClientx != null) {
                    String path = ZookeeperPathUtils
                        .getDestinationClusterNode(destination, ip + ":" + port);
                    zkClientx.createEphemeral(path);
                }
            }
            public void processStop() {
                // 注销 ZK 集群节点
                if (zkClientx != null) {
                    String path = ZookeeperPathUtils
                        .getDestinationClusterNode(destination, ip + ":" + port);
                    zkClientx.delete(path);
                }
            }
        });
        monitor.setZkClient(zkClientx);
        return monitor;
    })
);
```

**Step 3：Server A（先启动）抢占成功**

```java
// ServerRunningMonitor.initRunning()
private void initRunning() {
    String path = "/otter/canal/destinations/trade_sync/running";
    try {
        zkClient.createEphemeral(path, JsonUtils.marshalToByte(serverData));
        // 创建成功！
        activeData = serverData;
        processActiveEnter();
        // → embeddedCanalServer.start("trade_sync")
        mutex.set(true);
    } catch (ZkNodeExistsException e) {
        // 不会到这里（Server A 是第一个）
    }
}
```

**Server B（后启动）进入 Standby**

```java
private void initRunning() {
    try {
        zkClient.createEphemeral(path, bytes);
        // 创建失败！节点已被 Server A 占用
    } catch (ZkNodeExistsException e) {
        byte[] data = zkClient.readData(path, true);
        ServerRunningData activeNodeData = JsonUtils.unmarshalFromByte(data, ...);
        // activeNodeData = {"cid":1, "address":"10.0.1.1:11111"}

        if (activeNodeData.getAddress().equals(serverData.getAddress())) {
            // 不走这里（不是自己的残留节点）
        } else {
            activeData = activeNodeData;
            // Server B 进入 Standby，通过 dataListener 监听节点变化
        }
    }
}
```

**Step 4：Server A 宕机 → Server B 接管**

```java
// Server B 的 dataListener 被触发
public void handleDataDeleted(String dataPath) throws Exception {
    mutex.set(false);
    // 延迟 5 秒后重新抢占（避免网络抖动误判）
    delayExec.schedule(() -> {
        initRunning();
    }, delayTime, TimeUnit.SECONDS);
}

// 5 秒后...
private void initRunning() {
    try {
        zkClient.createEphemeral(path, bytes);
        // 创建成功！Server B 抢到运行权
        activeData = serverData;
        processActiveEnter();
        // → embeddedCanalServer.start("trade_sync")
        // → 从 ZK 恢复消费位点
        // → 从断点继续消费
        mutex.set(true);
    } catch (ZkNodeExistsException e) { ... }
}
```

> **关键时间线**：Server A 宕机 → ZK session 超时（~30秒）→ 临时节点删除 → Server B 收到通知 → 延迟 5 秒 → 抢占成功 → 恢复消费。总故障转移时间约 35 秒。HA 模式必须用 `default-instance.xml`（ZooKeeperMetaManager），否则 Server B 无法读取 Server A 的消费位点。

---

## 案例四：GTID 模式 —— 跨 MySQL 主从切换不丢数据

### 场景描述

MySQL 使用 GTID 模式，业务需要在 MySQL 主从切换时 Canal 自动跟随新主库继续消费。

### 配置

```properties
canal.instance.gtidon = true
canal.instance.master.gtid = 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5
```

### 全链路源码追踪

**Step 1：findStartPosition() —— GTID 分支**

```java
// MysqlEventParser.findStartPosition()
if (isGTIDMode()) {
    // 1. 检查持久化的 GTID 位点
    LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
    if (logPosition != null && StringUtils.isNotEmpty(logPosition.getPostion().getGtid())) {
        return logPosition.getPostion();
        // 从持久化的 GTID 恢复
    }
    // 2. 使用初始配置 GTID
    if (masterPosition != null && StringUtils.isNotEmpty(masterPosition.getGtid())) {
        return masterPosition;
    }
}
// 非 GTID 模式走 file+position
return findStartPositionInternal(connection);
```

**Step 2：COM_BINLOG_DUMP_GTID 命令**

```java
// MysqlConnection.dump(String gtid, SinkFunction func)
public void dump(String gtid, SinkFunction func) throws IOException {
    GTIDSet gtidSet = MysqlGTIDSet.parse(gtid);
    // "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"

    BinlogDumpGTIDCommandPacket command = new BinlogDumpGTIDCommandPacket();
    command.setSlaveServerId(slaveId);
    command.setGtidSet(gtidSet);
    // 命令字节：0x1e (COM_BINLOG_DUMP_GTID)
    // flags = 0x04 (BINLOG_THROUGH_GTID)

    connector.getChannel().writeCache(command.toBytes());

    LogContext context = new LogContext();
    context.setGtidSet(gtidSet);
    // 后续 GtidLogEvent 会调用 gtidSet.update() 累加
}
```

**Step 3：GTID 累加与位点持久化**

```java
// LogDecoder.decode() 中
case LogEvent.GTID_LOG_EVENT:
    GtidLogEvent gtidEvent = new GtidLogEvent(header, buffer, descriptionEvent);
    String gtidStr = gtidEvent.getGtidStr();
    // "3E11FA47-71CA-11E1-9E33-C80AA9429562:1001"
    context.getGtidSet().update(gtidStr);
    // gtidSet → "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1001"
    header.putGtid(gtidStr);
    break;
```

```java
// AbstractEventParser.buildLastPosition()
position.setGtid(entry.getHeader().getGtid());
// 持久化到 ZK/文件时包含 GTID 信息
```

> **GTID vs file+position 关键差异**：GTID 模式用 `COM_BINLOG_DUMP_GTID`（0x1e）代替 `COM_BINLOG_DUMP`（0x12），不需要指定 binlog 文件名和偏移量。MySQL 主从切换后 binlog 文件名可能完全不同，但 GTID 是全局唯一的，在新主库上也能正确定位。

---


## 案例五：多 Destination 同时运行 —— 一台 Server 监控多个 MySQL 实例

### 场景描述

一台 Canal Server 同时监控 3 个不同的 MySQL 实例（用户库、订单库、商品库）。每个 destination 有完全独立的组件，互不干扰。同时支持表达式简写 `example{1-3}` 批量展开，以及运行时动态新增/删除 destination 的配置热更新。

### 配置

```properties
# canal.properties
canal.destinations = user_sync,order_sync,product_sync
# 也可写成表达式：example{1-3} → example1,example2,example3
canal.instance.global.mode = spring
canal.instance.global.spring.xml = classpath:spring/file-instance.xml
canal.auto.scan = true
canal.auto.scan.interval = 5

# conf/user_sync/instance.properties
canal.instance.master.address = mysql-user:3306
canal.instance.filter.regex = userdb\\..*

# conf/order_sync/instance.properties
canal.instance.master.address = mysql-order:3306
canal.instance.filter.regex = orderdb\\..*

# conf/product_sync/instance.properties
canal.instance.master.address = mysql-product:3306
canal.instance.filter.regex = productdb\\..*
```

### 全链路源码追踪

**Step 1：CanalController.initInstanceConfig() —— destination 字符串解析与表达式展开**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalController.java`

```java
private void initInstanceConfig(Properties properties) {
    // 1. 读取 canal.destinations 配置
    String destinationStr = getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
    // destinationStr = "user_sync,order_sync,product_sync"

    // 2. 表达式展开：支持 example{1-3} 语法
    String[] destinations = StringUtils.split(destinationStr, ",");
    for (String destination : destinations) {
        destination = destination.trim();
        // 检查是否包含 {数字-数字} 表达式
        if (destination.contains("{") && destination.contains("}")) {
            // parseExpr() 用正则 (\\d+)-(\\d+) 提取范围
            // "example{1-3}" → ["example1", "example2", "example3"]
            List<String> expanded = parseExpr(destination);
            for (String dest : expanded) {
                InstanceConfig config = parseInstanceConfig(properties, dest);
                instanceConfigs.put(dest, config);
            }
        } else {
            // 普通名称，直接解析
            InstanceConfig config = parseInstanceConfig(properties, destination);
            instanceConfigs.put(destination, config);
        }
    }
    // 最终 instanceConfigs = {
    //   "user_sync": InstanceConfig, 
    //   "order_sync": InstanceConfig, 
    //   "product_sync": InstanceConfig
    // }
}

// parseExpr() 表达式展开
private List<String> parseExpr(String destination) {
    // 正则匹配 {起始-结束}
    Matcher matcher = Pattern.compile("(\\d+)-(\\d+)").matcher(destination);
    List<String> result = new ArrayList<>();
    if (matcher.find()) {
        int start = Integer.parseInt(matcher.group(1)); // 1
        int end = Integer.parseInt(matcher.group(2));   // 3
        String prefix = destination.substring(0, destination.indexOf("{")); // "example"
        for (int i = start; i <= end; i++) {
            result.add(prefix + i); // example1, example2, example3
        }
    }
    return result;
}

// parseInstanceConfig() 继承全局配置
private InstanceConfig parseInstanceConfig(Properties properties, String destination) {
    InstanceConfig config = new InstanceConfig();
    // 从全局配置 canal.instance.global.* 继承
    config.setMode(getProperty(properties, "canal.instance.global.mode")); // "spring"
    config.setLazy(Boolean.valueOf(getProperty(properties, 
        "canal.instance." + destination + ".lazy", "false")));
    // 读取 conf/{destination}/instance.properties 覆盖全局
    // 每个 destination 的 master.address、filter.regex 等各不相同
    return config;
}
```

> **这一步在干什么？** 这一步把 `canal.destinations` 字符串解析成具体的 destination 列表。支持两种写法：逗号分隔的显式列表（`user_sync,order_sync`），以及 `{起始-结束}` 的表达式简写（`example{1-3}` 展开为 3 个 destination）。每个 destination 都会调用 `parseInstanceConfig()` 生成独立的配置对象，配置先从全局 `canal.instance.global.*` 继承，再用 `conf/{destination}/instance.properties` 覆盖——这就是为什么每个 destination 可以连不同的 MySQL、用不同的过滤规则。

**Step 2：MigrateMap.makeComputingMap() —— 延迟创建 CanalInstance 的 Guava LoadingCache**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalController.java` + `common/src/main/java/com/alibaba/otter/canal/common/MigrateMap.java`

```java
// CanalController 构造函数中创建 canalInstances Map
canalInstances = MigrateMap.makeComputingMap(
    new ConcurrentHashMap<String, CanalInstance>(),
    new Function<String, CanalInstance>() {
        public CanalInstance apply(String destination) {
            // 当 canalInstances.get(destination) 首次调用时触发
            InstanceConfig config = instanceConfigs.get(destination);
            // 调用 instanceGenerator 生成 Instance
            CanalInstance instance = instanceGenerator.generate(destination, config);
            return instance;
        }
    });

// MigrateMap.makeComputingMap() 内部实现
public static <K, V> Map<K, V> makeComputingMap(
        Map<K, V> backingMap, Function<K, V> computingFunction) {
    return new MigrateConcurrentMap<>(backingMap, computingFunction);
}

// MigrateConcurrentMap.get() —— 延迟加载核心
public V get(Object key) {
    // 先查缓存
    V value = map.get(key);
    if (value == null) {
        // 缓存未命中，触发 computingFunction.apply(key)
        // 即调用 instanceGenerator.generate(destination)
        value = computingFunction.apply((K) key);
        map.put((K) key, value);
    }
    return value;
}
```

> **这一步在干什么？** `MigrateMap.makeComputingMap()` 创建了一个"计算 Map"——当首次调用 `canalInstances.get("user_sync")` 时，Map 发现没有缓存值，就触发 `instanceGenerator.generate()` 真正创建 CanalInstance。这是一种**延迟初始化**设计：3 个 destination 配置时不会立刻全部启动，只有在真正需要数据时（如客户端 subscribe）才创建对应的 Instance。这避免了启动时一次性连接 3 个 MySQL 实例的压力。

**Step 3：SpringCanalInstanceGenerator.generate() —— 每个 destination 独立的 Spring ApplicationContext**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/SpringCanalInstanceGenerator.java`

```java
public CanalInstance generate(String destination, InstanceConfig instanceConfig) {
    // 全局同步锁：防止多线程同时创建 Instance 时的 System.setProperty 竞争
    synchronized (CanalEventParser.class) {
        // 1. 设置 JVM 全局系统属性
        //    这是 Spring XML 中 ${canal.instance.destination} 占位符的值来源
        //    必须用 synchronized 防止：
        //    线程A setProperty("user_sync") → 线程B setProperty("order_sync") 
        //    → 线程A 加载 XML 读到 "order_sync" ← 错误！
        System.setProperty("canal.instance.destination", destination);

        // 2. 读取该 destination 的 Spring XML 路径
        String springXml = instanceConfig.getSpringXml();
        // "classpath:spring/file-instance.xml"

        // 3. 为每个 destination 创建独立的 ClassPathXmlApplicationContext
        //    注意：这不是复用全局 ApplicationContext！
        //    每个 destination 有完全独立的 bean 容器
        this.beanFactory = new ClassPathXmlApplicationContext(springXml);

        // 4. 从容器中获取 instance bean
        CanalInstance canalInstance = (CanalInstance) beanFactory.getBean("instance");

        // 5. 设置 destination 标识
        canalInstance.setDestination(destination);
        return canalInstance;
    }
}
```

> **这一步在干什么？** 这是多 destination 隔离的核心。每个 destination 调用 `generate()` 时都会 `new ClassPathXmlApplicationContext(springXml)` 创建一个全新的 Spring 容器——意味着独立的 Parser、Sink、Store、MetaManager bean 实例。关键细节是 `synchronized (CanalEventParser.class)` 全局锁：因为 `System.setProperty()` 是 JVM 级别的全局变量，如果不加锁，线程 A 设 `user_sync` 后还没来得及加载 XML，线程 B 就把属性改成了 `order_sync`，导致 A 创建的 Instance 连到了错误的 MySQL。

**Step 4：CanalController.start() —— 逐个 destination 启动 ServerRunningMonitor**

```java
// CanalController.start()
public void start() throws Throwable {
    // 1. 启动 Embedded Server
    embeddedCanalServer.start();

    // 2. 遍历所有 destination，逐个启动
    for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
        final String destination = entry.getKey();
        // "user_sync", "order_sync", "product_sync"

        // 获取该 destination 的 ServerRunningMonitor
        ServerRunningMonitor runningMonitor = 
            ServerRunningMonitors.getRunningMonitor(destination);

        if (!config.getLazy() && !runningMonitor.isStart()) {
            // 非 lazy 模式 → 立即启动
            runningMonitor.start();
            // 启动顺序：
            // Monitor("user_sync").start()    → 连接 mysql-user:3306    → dump binlog
            // Monitor("order_sync").start()   → 连接 mysql-order:3306   → dump binlog
            // Monitor("product_sync").start() → 连接 mysql-product:3306 → dump binlog
        }
    }

    // 3. 启动配置变更监控
    if (autoScan) {
        instanceConfigMonitor.start();
    }

    // 4. 启动 Netty TCP Server
    canalServer.start();
}
```

> **这一步在干什么？** 3 个 destination 按 `instanceConfigs` 的遍历顺序依次启动。每个 `ServerRunningMonitor.start()` 内部会触发 `embeddedCanalServer.start(destination)` → `canalInstances.get(destination)` → MigrateMap 延迟创建 → `instanceGenerator.generate()` → 创建 CanalInstance → 启动 parser/sink/store/meta 四大组件。3 个 MySQL 连接在 3 个独立线程中并行 dump binlog。

**Step 5：InstanceConfigMonitor 运行时检测 —— 新增/删除/修改 destination**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/monitor/SpringInstanceConfigMonitor.java` + `ManagerInstanceConfigMonitor.java`

```java
// SpringInstanceConfigMonitor —— 文件系统扫描（本地模式）
public void start() {
    executor.scheduleWithFixedDelay(() -> {
        // 每 5 秒扫描一次 conf/ 目录
        File rootDir = new File(rootConf);
        File[] instanceDirs = rootDir.listFiles(File::isDirectory);
        // 扫描到: [user_sync/, order_sync/, product_sync/, new_sync/]

        for (File dir : instanceDirs) {
            File instanceProps = new File(dir, "instance.properties");
            long lastModified = instanceProps.lastModified();

            String destination = dir.getName();
            Long prevModified = instanceLastModified.get(destination);
            if (prevModified == null) {
                // 新增 destination！
                // → 创建 InstanceConfig → start Instance
                addDestination(destination);
            } else if (lastModified > prevModified) {
                // 配置文件被修改！
                // 顺序：stop → reload config → start
                stopDestination(destination);
                reloadDestination(destination);
                startDestination(destination);
            }
            instanceLastModified.put(destination, lastModified);
        }
        // 检测已删除的 destination
        for (String existing : instanceLastModified.keySet()) {
            if (!new File(rootConf, existing).exists()) {
                stopDestination(existing);
                removeDestination(existing);
            }
        }
    }, 0, 5, TimeUnit.SECONDS); // 5 秒间隔
}

// ManagerInstanceConfigMonitor —— HTTP 轮询（远程 Admin 模式）
public void start() {
    executor.scheduleWithFixedDelay(() -> {
        // 每 5 秒轮询 Canal Admin API
        ResponseModel<InstanceConfig> response = 
            canalConfigClient.findInstances(lastMd5);
        
        if (response.getCode() == 200) {
            List<InstanceConfig> newConfigs = response.getData();
            for (InstanceConfig newConfig : newConfigs) {
                String dest = newConfig.getDestination();
                String newMd5 = DigestUtils.md5Hex(newConfig.getContent());
                String oldMd5 = instanceMd5Map.get(dest);
                
                if (oldMd5 == null) {
                    // 新增
                    addDestination(dest, newConfig);
                } else if (!newMd5.equals(oldMd5)) {
                    // MD5 不同 → 配置变更
                    // 顺序：stop → reload → start
                    stopDestination(dest);
                    reloadDestination(dest, newConfig);
                    startDestination(dest);
                }
            }
        }
    }, 0, 5, TimeUnit.SECONDS);
}
```

> **这一步在干什么？** 运行时配置热更新有两种检测机制。**Spring 模式**通过文件系统扫描：每 5 秒遍历 `conf/` 下的子目录，用 `instance.properties.lastModified()` 对比上次扫描时间，发现新增目录就启动 Instance，发现文件修改就 stop→reload→start，发现目录消失就 stop→remove。**Manager 模式**通过 HTTP 轮询 Canal Admin：每 5 秒请求一次 API，用 MD5 摘要对比配置内容是否变化。两种方式的处理顺序相同：先停旧 Instance，再加载新配置，最后启动新 Instance。

**Step 6：SessionHandler 请求路由 —— destination 字段分发到正确 Instance**

**源码位置**: `server/src/main/java/com/alibaba/otter/canal/server/netty/SessionHandler.java`

```java
// 客户端发送的每个 Protobuf Packet 都携带 destination 字段
public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    Packet packet = Packet.parseFrom((byte[]) e.getMessage());
    switch (packet.getType()) {
        case SUBSCRIPTION:
            Sub sub = Sub.parseFrom(packet.getBody());
            String destination = sub.getDestination(); // "order_sync"
            ClientIdentity clientIdentity = new ClientIdentity(
                destination, sub.getClientId(), sub.getFilter());
            
            // embeddedServer 内部按 destination 路由
            embeddedServer.subscribe(clientIdentity);
            // → canalInstances.get("order_sync") → 找到对应的 Instance
            break;
        case GET:
            Get get = Get.parseFrom(packet.getBody());
            // get.getDestination() = "order_sync"
            Message message = embeddedServer.getWithoutAck(
                new ClientIdentity(get.getDestination(), get.getClientId()),
                get.getFetchSize(), ...);
            break;
    }
}

// CanalServerWithEmbedded —— 每个 Instance 独立锁
public void subscribe(ClientIdentity clientIdentity) {
    String destination = clientIdentity.getDestination();
    // 每个 destination 有独立的 synchronized 锁
    // 不同 destination 的请求可以并行处理
    synchronized (canalInstances.get(destination)) {
        CanalInstance instance = canalInstances.get(destination);
        instance.getMetaManager().subscribe(clientIdentity);
        // ... 更新 filter 等
    }
}
```

> **这一步在干什么？** 多 destination 环境下，所有客户端请求都通过同一个 Netty 端口进入。每个 Protobuf Packet 都携带 `destination` 字段，`SessionHandler` 解析后传给 `CanalServerWithEmbedded`，后者通过 `canalInstances.get(destination)` 路由到正确的 Instance。关键细节：`synchronized` 锁是 per-instance 的——`user_sync` 的请求不会阻塞 `order_sync` 的请求，3 个 destination 的数据消费完全并行。

**多 Destination 资源隔离对比表**

| 资源 | user_sync | order_sync | product_sync | 隔离机制 |
|------|-----------|------------|--------------|----------|
| Spring ApplicationContext | 独立容器 | 独立容器 | 独立容器 | `new ClassPathXmlApplicationContext()` |
| MySQL 连接 | mysql-user:3306 | mysql-order:3306 | mysql-product:3306 | 独立 MysqlConnection |
| dump 线程 | 独立线程 | 独立线程 | 独立线程 | 独立 EventParser.start() |
| RingBuffer | 独立 16K slot | 独立 16K slot | 独立 16K slot | 独立 MemoryEventStoreWithBuffer |
| MetaManager | 独立位点文件 | 独立位点文件 | 独立位点文件 | 独立 FileMixedMetaManager |
| 过滤规则 | userdb\\..* | orderdb\\..* | productdb\\..* | 独立 AviaterRegexFilter |
| synchronized 锁 | per-instance | per-instance | per-instance | `synchronized (instance)` |
| 配置文件 | conf/user_sync/ | conf/order_sync/ | conf/product_sync/ | 独立 instance.properties |

---

## 案例六：表过滤 —— 黑白名单正则匹配与字段级过滤

### 场景描述

数据库有上百张表，只需要同步 `order_*` 表，排除 `order_log` 和 `order_archive`。同时需要对 `order_main` 表做字段级过滤——只同步 `id`、`order_no`、`amount` 三个字段，排除 `internal_note` 字段。还需处理 DDL 语句（包括 RENAME TABLE）的过滤，以及客户端运行时动态覆盖过滤规则。

### 配置

```properties
# conf/example/instance.properties
canal.instance.filter.regex = orderdb\\.order_.*
canal.instance.filter.black.regex = orderdb\\.order_log,orderdb\\.order_archive
canal.instance.filter.field.dtype = orderdb\\.order_main:id,order_no,amount
canal.instance.filter.field.black.regex = orderdb\\.order_main:internal_note
```

### 全链路源码追踪

**Step 1：AviaterRegexFilter 构造 —— 正则模式预处理与 Aviator 表达式编译**

**源码位置**: `filter/src/main/java/com/alibaba/otter/canal/filter/AviaterRegexFilter.java`

```java
public AviaterRegexFilter(String pattern, boolean defaultEmptyValue) {
    // 白名单 defaultEmptyValue = true（空模式 → 全通过）
    // 黑名单 defaultEmptyValue = false（空模式 → 全不匹配）
    this.defaultEmptyValue = defaultEmptyValue;

    // 1. 拆分多个正则模式，处理 {} 内的逗号不被误拆
    List<String> list = splitPattern(pattern);
    // 黑名单输入: "orderdb\\.order_log,orderdb\\.order_archive"
    // → ["orderdb\\.order_log", "orderdb\\.order_archive"]

    // 2. 按长度降序排列 —— 解决前缀匹配优先级问题
    // 如果有 "orderdb.order_*" 和 "orderdb.order_main"，
    // 长的排前面先匹配，避免短模式先命中
    list.sort((a, b) -> b.length() - a.length());

    // 3. 每个模式加 ^...$ 锚定，用 | 连接
    StringBuilder sb = new StringBuilder();
    for (String item : list) {
        sb.append("^").append(item).append("$").append("|");
    }
    this.pattern = sb.substring(0, sb.length() - 1);
    // 白名单结果: "^orderdb\\.order_.*$"
    // 黑名单结果: "^orderdb\\.order_archive$|^orderdb\\.order_log$"

    // 4. 编译 Aviator 表达式 "regex(pattern, target)"
    // regex 是自定义函数，内部调用 Jakarta ORO Perl5 正则引擎
    this.exp = AviatorEvaluator.compile("regex(pattern, target)", true);
}

// splitPattern() —— 处理 {} 内的逗号
private List<String> splitPattern(String pattern) {
    List<String> result = new ArrayList<>();
    int depth = 0; // {} 嵌套深度
    StringBuilder current = new StringBuilder();
    for (char c : pattern.toCharArray()) {
        if (c == '{') depth++;
        if (c == '}') depth--;
        if (c == ',' && depth == 0) {
            // 只有在 {} 外层的逗号才拆分
            result.add(current.toString().trim());
            current = new StringBuilder();
        } else {
            current.append(c);
        }
    }
    result.add(current.toString().trim());
    return result;
}
```

> **这一步在干什么？** 正则过滤器在构造时就完成了所有预处理工作：1）`splitPattern()` 智能拆分逗号，`{}` 内的逗号不拆（比如 `example{1,2,3}` 不会被拆成三段）；2）按长度降序排列，保证更具体的模式优先匹配；3）每个子模式用 `^...$` 锚定完整匹配，用 `|` 组合成一个大正则；4）最终通过 Aviator 表达式引擎编译，实际正则匹配委托给 Jakarta ORO 的 Perl5 引擎。

**Step 2：RegexFunction 调用链 —— Aviator 自定义函数 → Perl5 正则引擎 → 软引用缓存**

**源码位置**: `filter/src/main/java/com/alibaba/otter/canal/filter/aviater/RegexFunction.java` + `PatternUtils.java`

```java
// RegexFunction —— Aviator 自定义函数
public class RegexFunction extends AbstractFunction {
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        String pattern = (String) arg1.getValue(env); // "^orderdb\\.order_.*$"
        String target = (String) arg2.getValue(env);  // "orderdb.order_main"

        // 调用 PatternUtils 获取编译好的 Perl5Pattern（有缓存）
        Pattern regexPattern = PatternUtils.getPattern(pattern);
        // 使用 Jakarta ORO Perl5Matcher 做匹配
        Perl5Matcher matcher = new Perl5Matcher();
        boolean result = matcher.matches(target, (Perl5Pattern) regexPattern);
        return AviatorRuntimeJavaType.valueOf(result);
    }
}

// PatternUtils —— 软引用缓存
public class PatternUtils {
    private static final LoadingCache<String, Pattern> patterns = 
        CacheBuilder.newBuilder()
            .softValues() // 软引用：内存不足时 GC 自动回收
            .build(new CacheLoader<String, Pattern>() {
                public Pattern load(String pattern) {
                    Perl5Compiler compiler = new Perl5Compiler();
                    // CASE_INSENSITIVE | READ_ONLY | SINGLELINE
                    return compiler.compile(pattern,
                        Perl5Compiler.CASE_INSENSITIVE_MASK
                        | Perl5Compiler.READ_ONLY_MASK
                        | Perl5Compiler.SINGLELINE_MASK);
                }
            });

    public static Pattern getPattern(String pattern) {
        return patterns.get(pattern); // 缓存命中直接返回，未命中触发 load()
    }
}
```

> **这一步在干什么？** 正则匹配不直接用 Java 的 `Pattern`，而是通过 Aviator 表达式引擎间接调用 Jakarta ORO 的 Perl5 引擎——这是因为 Canal 早期版本依赖 ORO 的 Perl5 兼容语法。`PatternUtils` 用 Guava `CacheBuilder.newBuilder().softValues()` 做编译结果缓存：软引用值在内存充足时保留、内存不足时被 GC 自动回收，避免正则编译（CPU 密集）重复执行，同时不会造成 OOM。

**Step 3：LogEventConvert 完整过滤管线 —— 7 个过滤阶段**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/LogEventConvert.java`

| 阶段 | 配置项 | 过滤对象 | 作用 |
|------|--------|----------|------|
| 1. filterRows | canal.instance.filter.rows | DML 类型过滤 | 只保留 INSERT/UPDATE/DELETE 子集 |
| 2. nameFilter | canal.instance.filter.regex | AviaterRegexFilter | 白名单：表名正则匹配 |
| 3. nameBlackFilter | canal.instance.filter.black.regex | AviaterRegexFilter | 黑名单：排除表名 |
| 4. fieldFilterMap | canal.instance.filter.field.dtype | Map<String, List<String>> | 字段白名单：只保留指定列 |
| 5. fieldBlackFilterMap | canal.instance.filter.field.black.regex | Map<String, List<String>> | 字段黑名单：排除指定列 |
| 6. filterQueryDdl | canal.instance.filter.query.dcl | Boolean | DDL 语句过滤 |
| 7. filterQueryDml | canal.instance.filter.query.dml | Boolean | DML 查询语句过滤 |

```java
// DML 事件过滤代码流
protected boolean processRowsEvent(LogEvent event) {
    String schemaName = ...; // "orderdb"
    String tableName = ...;  // "order_main"
    String fullName = schemaName + "." + tableName; // "orderdb.order_main"

    // 阶段1: DML 类型过滤（只保留 INSERT/UPDATE/DELETE 中的某些类型）
    if (filterRows != null && !filterRows.filter(eventType)) {
        return false; // 丢弃
    }

    // 阶段2: 白名单表名过滤
    if (nameFilter != null && !nameFilter.filter(fullName)) {
        return false; // "orderdb.users" 不匹配 "^orderdb\\.order_.*$" → 丢弃
    }

    // 阶段3: 黑名单表名过滤
    if (nameBlackFilter != null && nameBlackFilter.filter(fullName)) {
        return false; // "orderdb.order_log" 匹配黑名单 → 丢弃
    }

    // 阶段4+5: 字段级过滤（在 parseOneRow 中执行）
    // ...

    return true; // 通过所有过滤，继续解析行数据
}

// DDL 事件过滤代码流
protected boolean processQueryEvent(LogEvent event) {
    String query = ...; // "RENAME TABLE order_old TO order_main"
    
    // 阶段6: DDL 类型过滤
    if (!filterQueryDdl) {
        return false; // DDL 被禁用 → 丢弃
    }

    // 提取 DDL 涉及的表名
    List<String> tableNames = DruidDdlParser.parse(query, schemaName);
    // RENAME: ["order_old", "order_main"]

    // 对每个表名做白名单+黑名单检查
    for (String table : tableNames) {
        if (nameFilter != null && nameFilter.filter(schemaName + "." + table)) {
            // 白名单匹配 → 保留此 DDL
            return true;
        }
    }
    return false;
}
```

> **这一步在干什么？** Canal 的过滤不是单一的正则匹配，而是 7 个阶段的管线。DML 事件依次经过：类型过滤→白名单表名→黑名单表名→字段白名单→字段黑名单。DDL 事件额外经过 DDL 类型开关和 DDL 表名过滤。任何一个阶段返回 false 就丢弃该事件——不匹配的表在 binlog 解码阶段就被丢弃，不占用 RingBuffer 空间。

**Step 4：字段级过滤 —— 白名单优先于黑名单**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/LogEventConvert.java`

```java
// parseFieldFilterMap() —— 解析字段过滤配置
// 输入: "orderdb.order_main:id,order_no,amount"
private Map<String, List<String>> parseFieldFilterMap(String filterField) {
    Map<String, List<String>> result = new HashMap<>();
    // 按逗号拆分多个表配置
    for (String entry : filterField.split(",")) {
        // "orderdb.order_main:id,order_no,amount"
        String[] parts = entry.split(":");
        String tableKey = parts[0]; // "orderdb.order_main"
        String[] columns = parts[1].split("/"); // ["id", "order_no", "amount"]
        result.put(tableKey, Arrays.asList(columns));
    }
    return result;
}

// needField() —— 判断某个字段是否需要
private boolean needField(String schemaName, String tableName, String fieldName) {
    String key = schemaName + "." + tableName;
    List<String> whitelist = fieldFilterMap.get(key);
    List<String> blacklist = fieldBlackFilterMap.get(key);

    if (whitelist != null && whitelist.size() > 0) {
        // 白名单存在 → 只保留白名单中的字段
        return whitelist.contains(fieldName);
    }
    if (blacklist != null && blacklist.contains(fieldName)) {
        // 白名单不存在，黑名单匹配 → 排除
        return false;
    }
    // 白名单不存在，黑名单不匹配 → 保留
    return true;
}

// parseOneRow() —— 实际过滤字段
private RowData parseOneRow(TableMapLogEvent tableMapEvent, ...) {
    for (int i = 0; i < columnCount; i++) {
        String columnName = columnInfo[i].getName();
        if (!needField(schemaName, tableName, columnName)) {
            continue; // 跳过不需要的字段
        }
        // 保留该字段值
        // ...
    }
}
```

> **这一步在干什么？** 字段级过滤的配置格式是 `schema.table:col1/col2/col3`，冒号前是表名，冒号后是斜杠分隔的列名。`needField()` 的优先级规则：**白名单存在时只保留白名单字段**（黑名单被忽略）；白名单不存在时排除黑名单字段。这意味着 `order_main` 表只会输出 `id`、`order_no`、`amount` 三个字段，`internal_note` 等其他字段在 `parseOneRow()` 中直接跳过。

**Step 5：DDL 与 RENAME TABLE 处理 —— Druid AST 解析 + 缓存失效**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/ddl/DruidDdlParser.java`

```java
// DruidDdlParser.parse() —— 提取 DDL 涉及的表名
public static List<String> parse(String ddl, String schamaName) {
    List<SQLStatement> statements = SQLUtils.parseStatements(ddl, "mysql");
    List<String> tableNames = new ArrayList<>();
    for (SQLStatement stmt : statements) {
        if (stmt instanceof SQLRenameTableStatement) {
            // RENAME TABLE order_old TO order_main
            SQLRenameTableStatement rename = (SQLRenameTableStatement) stmt;
            // 旧表名和新表名都要收集
            tableNames.add(rename.getOldName().getSimpleName()); // "order_old"
            tableNames.add(rename.getNewName().getSimpleName()); // "order_main"
        } else {
            // CREATE/ALTER/DROP TABLE
            tableNames.addAll(extractTableNames(stmt));
        }
    }
    return tableNames;
}

// RENAME TABLE 的白名单/黑名单判定逻辑
// 白名单：旧表名或新表名任一匹配 → 保留 DDL
//   （因为 RENAME 后可能变成需要监听的表）
// 黑名单：旧表名和新表名都匹配 → 丢弃 DDL
//   （两个都是不关心的表，没必要保留）
boolean shouldKeepRename(String oldTable, String newTable, String schema) {
    // 白名单检查：EITHER matches
    if (nameFilter != null) {
        boolean oldMatch = nameFilter.filter(schema + "." + oldTable);
        boolean newMatch = nameFilter.filter(schema + "." + newTable);
        if (!oldMatch && !newMatch) {
            return false; // 都不匹配白名单 → 丢弃
        }
    }
    // 黑名单检查：BOTH match
    if (nameBlackFilter != null) {
        boolean oldBlack = nameBlackFilter.filter(schema + "." + oldTable);
        boolean newBlack = nameBlackFilter.filter(schema + "." + newTable);
        if (oldBlack && newBlack) {
            return false; // 都匹配黑名单 → 丢弃
        }
    }
    return true;
}

// DDL 触发表结构缓存失效
if (eventType == EventType.QUERY && isDdl(query)) {
    // 清除该表的 TableMeta 缓存
    // 下次 DML 事件时会重新从 MySQL 拉取最新表结构
    tableMetaCache.invalidate(schemaName, tableName);
}
```

> **这一步在干什么？** DDL 过滤比 DML 复杂——需要先用 Druid SQL Parser 解析 DDL 语句提取涉及的表名。RENAME TABLE 特别 tricky：白名单用"或"逻辑（旧表名和新表名任一匹配就保留，因为 RENAME 后可能变成需要监听的表），黑名单用"与"逻辑（两个表名都匹配才丢弃）。DDL 还会触发表结构缓存失效：执行了 ALTER TABLE 加字段后，后续 DML 事件必须用新表结构解析，所以 `tableMetaCache.invalidate()` 强制下次重新拉取 meta。

**Step 6：动态过滤覆盖 —— 客户端 subscribe 替换白名单**

**源码位置**: `instance/src/main/java/com/alibaba/otter/canal/instance/manager/CanalInstanceWithManager.java` + `AbstractCanalInstance.java`

```java
// 客户端调用 subscribe("orderdb\\.order_main") 时
// 触发服务端 subscribeChange()
public void subscribeChange(ClientIdentity clientIdentity) {
    String filter = clientIdentity.getFilter();
    if (StringUtils.isNotEmpty(filter)) {
        // 只替换白名单 eventFilter，不替换黑名单 nameBlackFilter
        AviaterRegexFilter newFilter = new AviaterRegexFilter(filter, true);
        eventParser.setEventFilter(newFilter);
        // 原有黑名单保持不变！
        // 这样客户端只能缩小范围（从 order_* 缩小到 order_main），
        // 不能扩大到被黑名单排除的表
    }
}

// 单客户端限制：如果多个客户端订阅同一个 destination，
// 后一个客户端的 filter 会覆盖前一个
// 这是 Canal 的已知限制——多客户端场景应使用不同 destination
```

> **这一步在干什么？** 客户端 subscribe 时可以传入自定义过滤规则，动态覆盖 Instance 初始配置的**白名单**。关键细节：只替换白名单（`eventFilter`），不替换黑名单（`nameBlackFilter`）——这意味着客户端可以缩小监听范围（从所有 `order_*` 表缩小到只看 `order_main`），但不能绕过黑名单去监听被排除的表。单 destination 多客户端时存在覆盖问题：后订阅的客户端 filter 会覆盖前者，这是 Canal 的已知限制。

**过滤执行顺序与示例表**

| 执行顺序 | 过滤阶段 | 输入示例 | 配置 | 输出 |
|----------|----------|----------|------|------|
| 1 | filterRows | INSERT 事件 | `INSERT,UPDATE` | 通过（INSERT 在白名单） |
| 2 | nameFilter (白名单) | `orderdb.order_main` | `orderdb\\.order_.*` | 通过（正则匹配） |
| 3 | nameBlackFilter (黑名单) | `orderdb.order_main` | `orderdb\\.order_log` | 通过（不匹配黑名单） |
| 4 | fieldFilterMap (白名单) | `internal_note` 字段 | `id,order_no,amount` | **丢弃**（不在白名单） |
| 5 | fieldBlackFilterMap (黑名单) | `internal_note` 字段 | `internal_note` | 丢弃（匹配黑名单） |
| 6 | filterQueryDdl | `ALTER TABLE` | `true` | 通过 |
| 7 | filterQueryDml | `SELECT` | `false` | **丢弃** |

---

## 案例七：RDB Adapter —— MySQL 到 MySQL 异构实时同步

### 场景描述

业务库 `source_db.users` 实时同步到分析库 `target_db.users_mirror`，需要做字段映射（`create_time` → `created_at`），并保证同一主键的操作在同一个线程内有序执行。同时支持 mirror-db 模式（自动建表、DDL 回放）。

### 配置

```yaml
# client-adapter/rdb/src/main/resources/rdb/mytest_user.yml
dbMapping:
  database: source_db
  table: users
  targetTable: target_db.users_mirror
  targetPk: { id: id }
  targetColumns: { id: id, username: username, create_time: created_at }
  commitBatch: 3000
  # mirrorDb 模式（自动建表+DDL回放）
  # mirrorDb: true
```

### 全链路源码追踪

**Step 1：SPI 加载链 —— ExtensionLoader → URLClassExtensionLoader → ProxyOuterAdapter**

**源码位置**: `client-adapter/launcher/src/main/java/com/alibaba/otter/canal/client/adapter/loader/ExtensionLoader.java`

```java
// ExtensionLoader 扫描 plugin/ 目录下的 JAR
public T getExtension(String name, String dir) {
    // 1. 扫描 plugin/ 目录
    File pluginDir = new File(dir); // "plugin/"
    URL[] urls = pluginDir.listFiles()
        .stream().map(f -> f.toURI().toURL()).toArray(URL[]::new);

    // 2. 创建 child-first ClassLoader
    URLClassExtensionLoader classLoader = new URLClassExtensionLoader(urls, 
        Thread.currentThread().getContextClassLoader());
    // URLClassExtensionLoader 是 child-first 委派模型：
    // 先尝试自己加载，加载不到才委派给 parent
    // 但 java.*/slf4j.*/druid.* 等基础包强制委派给 parent（避免冲突）

    // 3. 读取 SPI 配置文件
    // META-INF/canal/com.alibaba.otter.canal.client.adapter.OuterAdapter
    // 内容: rdb=com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter
    Class<?> clazz = classLoader.loadClass(spiClass);
    T instance = (T) clazz.newInstance();

    // 4. 用 ProxyOuterAdapter 包装，切换 ClassLoader 上下文
    return new ProxyOuterAdapter(instance, classLoader);
}

// ProxyOuterAdapter.changeCL() —— ClassLoader 上下文切换
public class ProxyOuterAdapter implements OuterAdapter {
    private ClassLoader adapterClassLoader;

    public void sync(List<Dml> dmls) {
        ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
        try {
            // 切换到 Adapter 的 ClassLoader
            // 因为 Adapter 内部用到的 JDBC 驱动等类在 plugin/ JAR 中
            Thread.currentThread().setContextClassLoader(adapterClassLoader);
            targetAdapter.sync(dmls); // 实际调用 RdbAdapter.sync()
        } finally {
            // 恢复原始 ClassLoader
            Thread.currentThread().setContextClassLoader(originalCL);
        }
    }
}
```

> **这一步在干什么？** RDB Adapter 通过 Canal 的 SPI 机制加载。关键在于 ClassLoader 隔离：RDB Adapter 的 JAR 包（含 JDBC 驱动、Druid 连接池等）放在 `plugin/` 目录下独立加载，使用 child-first 委派模型——先尝试自己加载类，加载不到才委派给父 ClassLoader。`ProxyOuterAdapter` 在每次调用 `sync()` 前切换 `Thread.currentThread().setContextClassLoader()`，确保 Adapter 内部的类加载和资源查找都在正确的 ClassLoader 上下文中执行。

**Step 2：RdbSyncService 线程池架构 —— 三组并行数组 + 主键哈希分区**

**源码位置**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/service/RdbSyncService.java`

```java
public class RdbSyncService {
    // 三组并行数组，按 partition 索引
    private Dml[] dmlsPartition;        // 每个 partition 的 DML 缓冲
    private BatchExecutor[] batchExecutors;  // 每个 partition 的 JDBC 批处理器
    private ExecutorService[] executorThreads; // 每个 partition 的线程池

    // pkHash() —— 主键哈希（用加法，不用 XOR）
    private int pkHash(String schema, String table, Map<String, Object> pkValues) {
        int hash = 0;
        for (Object pkValue : pkValues.values()) {
            // 注意：用加法而非 XOR
            // 加法保证 (a,b) 和 (b,a) 的 hash 不同（更好的分散性）
            // XOR 有交换律：a^b == b^a，可能导致不对称分区的聚集
            hash += pkValue.hashCode();
        }
        return Math.abs(hash);
    }

    // sync() 入口
    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            // 拆分为单行 DML
            List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
            
            for (SingleDml singleDml : singleDmls) {
                // 按主键哈希分配 partition
                int partition = pkHash(...) % threads;
                // 同一主键的操作总是落到同一个 partition → 同一个线程
                // 保证同一主键的 INSERT→UPDATE→DELETE 顺序执行
                
                // 提交到对应 partition 的线程
                final int part = partition;
                executorThreads[partition].submit(() -> {
                    sync(batchExecutors[part], config, singleDml);
                });
            }
        }
    }
}
```

> **这一步在干什么？** RdbSyncService 用三组并行数组实现多线程并行同步：`dmlsPartition[]` 缓冲数据、`batchExecutors[]` 管理每个分区的 JDBC 连接、`executorThreads[]` 是线程池。`pkHash()` 用**加法**而非 XOR 来计算主键哈希——加法不具有交换律（`a+b != b+a` 当 hash 冲突时），比 XOR 有更好的分散性。同一个主键的所有操作（INSERT→UPDATE→DELETE）始终落到同一个 partition、同一个线程，保证了单主键的操作顺序性。不同主键的操作在多个线程上并行执行。

**Step 3：DML 处理循环 —— 拆分单行 → 分区提交 → 跨分区并行**

```java
// dml2SingleDmls() —— 批量 DML 拆分为单行
// 一条 DML 可能影响多行（如 UPDATE ... WHERE status=0 影响 100 行）
List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
// 输入: Dml{type=UPDATE, data=[{id:1,...}, {id:2,...}], old=[{status:1}, {status:1}]}
// 输出: [
//   SingleDml{type=UPDATE, data={id:1,...}, old={status:1}},
//   SingleDml{type=UPDATE, data={id:2,...}, old={status:1}}
// ]

// 每个 SingleDml 按主键哈希分配到不同 partition
for (SingleDml singleDml : singleDmls) {
    int partition = Math.abs(pkHash(schema, table, singleDml.getPkValues())) % threads;
    // partition 0 → 线程 0 → BatchExecutor 0
    // partition 1 → 线程 1 → BatchExecutor 1
    
    Future<?> future = executorThreads[partition].submit(() -> {
        sync(batchExecutors[partition], config, singleDml);
    });
    futures.add(future);
}

// 等待所有分区完成
for (Future<?> future : futures) {
    future.get(); // 阻塞等待该分区完成
}
// 所有分区并行执行，最后统一等待
```

> **这一步在干什么？** 一条 DML 可能影响多行（如 `UPDATE users SET status=1 WHERE status=0` 影响 100 行），`dml2SingleDmls()` 将其拆分为 100 个单行操作。每个单行操作按主键哈希分配到不同 partition，提交到对应线程的 `executorThreads[partition]`。不同 partition 的操作在各自线程上并行执行，最后通过 `Future.get()` 等待所有分区完成——这是一种 fork-join 模式，既有并行吞吐又保证最终一致性。

**Step 4：INSERT SQL 生成 —— 字段映射 + 列名转义**

```java
// RdbSyncService.sync() —— INSERT 分支
if ("INSERT".equals(type)) {
    // 1. 获取目标列值（做字段映射）
    Map<String, Object> targetValues = getTargetColumnValues(
        dbMapping.getTargetColumns(), singleDml.getData());
    // 源字段 create_time=2024-01-01 → 目标字段 created_at=2024-01-01
    // {id: 1, username: "alice", created_at: "2024-01-01"}

    // 2. 构建 INSERT SQL
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO `").append(dbMapping.getTargetDatabase())
       .append("`.`").append(dbMapping.getTargetTable()).append("` (");
    // INSERT INTO `target_db`.`users_mirror` (

    // 列名用反引号转义，防止保留字冲突
    for (String column : targetValues.keySet()) {
        sql.append("`").append(column).append("`, ");
    }
    // `id`, `username`, `created_at`,
    sql.setLength(sql.length() - 2); // 去掉末尾 ", "
    sql.append(") VALUES (");
    for (int i = 0; i < targetValues.size(); i++) {
        sql.append("?, ");
    }
    sql.setLength(sql.length() - 2);
    sql.append(")");
    // INSERT INTO `target_db`.`users_mirror` (`id`, `username`, `created_at`) VALUES (?, ?, ?)

    // 3. 执行
    batchExecutor.execute(sql.toString(), targetValues.values());
}
```

> **这一步在干什么？** INSERT SQL 的生成分两步：先通过 `getTargetColumnValues()` 做字段映射——源表的 `create_time` 映射为目标表的 `created_at`，映射关系来自 yml 配置的 `targetColumns`。然后拼接 SQL，列名用反引号 `` ` `` 转义以避免 MySQL 保留字冲突（比如 `order`、`group` 等列名）。值用 `?` 占位符传入 PreparedStatement，防止 SQL 注入。

**Step 5：UPDATE SQL 生成 —— 只 SET 变更字段**

```java
if ("UPDATE".equals(type)) {
    Map<String, Object> oldValues = singleDml.getOld(); // 变更前的值
    // oldValues 只有发生变化的字段：{status: 0}
    // → 说明 status 字段从 0 变成了新值

    // 1. 找出变更的字段，映射到目标列名
    Map<String, Object> targetChanges = new LinkedHashMap<>();
    for (String sourceCol : oldValues.keySet()) {
        String targetCol = dbMapping.getTargetColumns().get(sourceCol);
        if (targetCol != null) {
            targetChanges.put(targetCol, singleDml.getData().get(sourceCol));
        }
    }

    // 2. 构建 UPDATE SQL —— 只 SET 变更的列
    StringBuilder sql = new StringBuilder();
    sql.append("UPDATE `").append(targetDb).append("`.`").append(targetTable)
       .append("` SET ");
    for (String column : targetChanges.keySet()) {
        sql.append("`").append(column).append("` = ?, ");
    }
    sql.setLength(sql.length() - 2);
    // UPDATE `target_db`.`users_mirror` SET `status` = ?

    // 3. WHERE 条件只用主键
    sql.append(" WHERE ");
    for (String pk : dbMapping.getTargetPk().keySet()) {
        sql.append("`").append(pk).append("` = ? AND ");
    }
    sql.setLength(sql.length() - 5);
    // UPDATE `target_db`.`users_mirror` SET `status` = ? WHERE `id` = ?

    batchExecutor.execute(sql.toString(), values);
}
```

> **这一步在干什么？** UPDATE 优化点：只 SET 发生变更的字段，而不是全字段更新。变更字段通过 `singleDml.getOld()` 获取——`old` Map 只包含发生变化的字段及其旧值，没变化的字段不在其中。这样 `UPDATE users_mirror SET status=1 WHERE id=1` 而不是 `UPDATE users_mirror SET id=1, username='alice', status=1, created_at='2024-01-01' WHERE id=1`，大幅减少写入量。WHERE 条件只用主键，不支持复合条件更新。

**Step 6：DELETE SQL 生成 —— 主键删除**

```java
if ("DELETE".equals(type)) {
    // DELETE 最简单：只需主键值
    Map<String, Object> pkValues = singleDml.getPkValues();

    StringBuilder sql = new StringBuilder();
    sql.append("DELETE FROM `").append(targetDb).append("`.`").append(targetTable)
       .append("` WHERE ");
    for (String pk : dbMapping.getTargetPk().keySet()) {
        sql.append("`").append(pk).append("` = ? AND ");
    }
    sql.setLength(sql.length() - 5);
    // DELETE FROM `target_db`.`users_mirror` WHERE `id` = ?

    batchExecutor.execute(sql.toString(), pkValues.values());
}
```

> **这一步在干什么？** DELETE 最简单——只需要目标表的主键值就能定位行。主键映射通过 `dbMapping.getTargetPk()` 配置（`{id: id}` 表示源表 `id` 映射为目标表 `id`），如果源表和目标表主键名不同，这里会自动转换。

**Step 7：BatchExecutor 内部 —— 无 PreparedStatement 缓存 + 共享事务**

**源码位置**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/service/BatchExecutor.java`

```java
public class BatchExecutor {
    private Connection connection; // 单个 JDBC 连接
    // autoCommit = false，手动控制事务

    // 注意：没有 PreparedStatement 缓存！
    // 每次 execute 都创建新的 PreparedStatement
    public void execute(String sql, List<Object> values) {
        PreparedStatement pstmt = connection.prepareStatement(sql);
        // 每次都新建 pstmt，不缓存复用
        // 原因：不同 DML 的 SQL 不同（INSERT/UPDATE/DELETE 各一套），
        // 且 partition 内的 SQL 频繁变化，缓存命中率低
        
        for (int i = 0; i < values.size(); i++) {
            pstmt.setObject(i + 1, values.get(i));
        }
        pstmt.execute();
        pstmt.close(); // 用完立即关闭
        
        count++;
        if (count >= commitBatch) { // commitBatch = 3000
            commit(); // 攒满 3000 条提交一次
        }
    }

    public void commit() {
        connection.commit(); // 提交事务
        count = 0;
    }
}
// autoCommit=false 意味着 3000 条 DML 在同一个事务内
// 如果中间出错，整个 batch 回滚
```

> **这一步在干什么？** BatchExecutor 有两个关键设计：1）**不做 PreparedStatement 缓存**——每次 `execute()` 都创建新的 PreparedStatement 并立即关闭。虽然看起来浪费，但同一个 partition 内 SQL 类型频繁切换（INSERT→UPDATE→DELETE 交替），缓存命中率低，且 PreparedStatement 缓存会占用数据库端资源。2）**autoCommit=false 共享事务**——3000 条 DML 在同一个 JDBC 事务内执行，攒满 `commitBatch` 阈值才 `connection.commit()`。这大幅减少数据库 commit 次数（从每条一次降到每 3000 条一次），但如果中间出错整个 batch 回滚。

**Step 8：SyncUtil 类型转换 —— JDBC 类型分派**

**源码位置**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/support/SyncUtil.java`

```java
public static Object getVal(ColumnMeta columnMeta, Object val) {
    int sqlType = columnMeta.getSqlType(); // java.sql.Types

    // 1. 无符号整数提升
    if (sqlType == Types.INTEGER && columnMeta.isUnsigned()) {
        // MySQL unsigned int 范围 0~4294967295，超过 Java int 上限
        // 提升为 Long
        return ((Number) val).longValue();
    }
    if (sqlType == Types.BIGINT && columnMeta.isUnsigned()) {
        // unsigned bigint 范围 0~18446744073709551615，超过 Java long 上限
        // 提升为 BigDecimal
        return new BigDecimal(val.toString());
    }

    // 2. 二进制类型
    if (sqlType == Types.BLOB || sqlType == Types.LONGVARBINARY) {
        if (val instanceof Blob) return ((Blob) val).getBytes(1, (int) ((Blob) val).length());
        if (val instanceof byte[]) return val;
        if (val instanceof String) return ((String) val).getBytes(StandardCharsets.ISO_8859_1);
    }

    // 3. CLOB 类型
    if (sqlType == Types.CLOB) {
        Clob clob = (Clob) val;
        return clob.getSubString(1, (int) clob.length());
    }

    // 4. DATE 类型 —— 跳过 0000-00-00 非法日期
    if (sqlType == Types.DATE) {
        String dateStr = val.toString();
        if ("0000-00-00".equals(dateStr) || dateStr.startsWith("0000")) {
            return null; // MySQL 允许 0000-00-00，但 JDBC 不接受
        }
        return java.sql.Date.valueOf(dateStr);
    }

    // 5. TIMESTAMP 类型 —— 纳秒精度通过 LocalDateTime
    if (sqlType == Types.TIMESTAMP) {
        if (val instanceof LocalDateTime) {
            // LocalDateTime 支持纳秒精度
            return Timestamp.valueOf((LocalDateTime) val);
        }
        return Timestamp.valueOf(val.toString());
    }

    return val; // 其他类型直接透传
}
```

> **这一步在干什么？** 类型转换是异构同步最容易出错的地方。SyncUtil 处理了 5 类边界情况：1）**无符号整数提升**——MySQL `unsigned int` 超过 Java `int` 上限，提升为 `Long`；`unsigned bigint` 超过 `long` 上限，提升为 `BigDecimal`。2）**二进制类型**——Blob/byte[]/String 互转。3）**CLOB** 转 String。4）**非法日期**——MySQL 允许 `0000-00-00` 但 JDBC 不接受，转成 null。5）**TIMESTAMP 纳秒精度**——通过 `LocalDateTime` 保留纳秒级精度，避免精度丢失。

**Step 9：mirror-db 模式 —— 自动建表 + DDL 回放**

```java
// mirror-db 模式：不需要手动配置字段映射
// Adapter 自动从源库发现表结构，在目标库创建镜像表
if (mirrorDb) {
    // 1. 自动发现源库表
    List<TableMeta> tables = sourceDbMetaData.getTables();
    for (TableMeta table : tables) {
        // 在目标库创建同名表（结构完全复制）
        targetDb.execute(table.getCreateDdl());
    }

    // 2. DDL 回放
    if (dml.getType() == "DDL") {
        // 先 flush 所有待提交的 DML
        for (BatchExecutor executor : batchExecutors) {
            executor.commit();
        }
        // 再执行 DDL
        // 注意顺序：必须先提交 DML，否则 DDL 可能锁表导致死锁
        statement.execute(dml.getDdl());
        // ALTER TABLE users ADD COLUMN email VARCHAR(255);
        
        // DDL 后清除表结构缓存
        tableMetaCache.invalidate();
    }

    // 3. DML 使用 identity 映射（源列名 = 目标列名）
    // 不需要 targetColumns 配置，自动 1:1 映射
}
```

> **这一步在干什么？** mirror-db 模式是"镜像数据库"——不需要手动配置字段映射，Adapter 自动从源库发现表结构并在目标库创建同名同结构的表。DDL 语句（ALTER TABLE、CREATE TABLE 等）直接在目标库回放。关键细节：DDL 执行前必须先 flush 所有待提交的 DML——如果 DML 还在事务中未提交，DDL 可能触发表锁导致死锁。DML 使用 identity 映射（源列名 = 目标列名），无需 `targetColumns` 配置。

**RDB Adapter 架构对比表**

| 特性 | 映射模式 (mapping) | 镜像模式 (mirror-db) |
|------|---------------------|----------------------|
| 字段映射 | 手动配置 targetColumns | 自动 1:1（源列名=目标列名） |
| 表名映射 | 手动配置 targetTable | 同名复制 |
| DDL 处理 | 不支持 | 支持（DDL 回放） |
| 自动建表 | 不支持 | 支持 |
| 配置复杂度 | 每表一个 yml | 只需 mirrorDb: true |
| 适用场景 | 字段重命名/筛选 | 全库备份/异构容灾 |

---

## 案例八：ES Adapter —— 实时构建 Elasticsearch 搜索索引

### 场景描述

MySQL 商品表 `product` 和分类表 `category` 做 JOIN，实时同步到 Elasticsearch 索引。商品表变更时需要回查 JOIN SQL 获取分类名称，分类表变更时需要反查所有引用该分类的商品并批量更新 ES。支持父子文档（parent-child）、geo_point 地理坐标、数组类型等 ES 特有映射。

### 配置

```yaml
# client-adapter/es7/src/main/resources/es7/product_index.yml
esMapping:
  _index: products
  _id: _id
  _type: _doc            # ES7 已废弃 _type，保留仅为兼容
  sql: >
    SELECT p.id AS _id, p.name, p.price,
           c.category_name, p.description, p.location
    FROM product p
    LEFT JOIN category c ON c.id = p.category_id
  commitBatch: 3000
  etlCondition: "where p.update_time >= '{}'"
```

### 全链路源码追踪

**Step 1：SqlParser 解析 SQL → SchemaItem —— Druid AST 遍历**

**源码位置**: `client-adapter/es7x/src/main/java/com/alibaba/otter/canal/client/adapter/es7x/support/SqlParser.java`

```java
public static SchemaItem parse(String sql) {
    // 1. Druid MySQL 语法解析器
    MySqlStatementParser parser = new MySqlStatementParser(sql);
    SQLStatement statement = parser.parseStatement();
    SQLSelectStatement selectStmt = (SQLSelectStatement) statement;

    // 2. 遍历 FROM 子句，处理 3 种 TableSource 类型
    SQLTableSource tableSource = selectStmt.getSelect().getQueryBlock().getFrom();
    SchemaItem schemaItem = new SchemaItem();

    if (tableSource instanceof SQLExprTableSource) {
        // 单表：SELECT * FROM product p
        // → mainTable = product (alias "p")
        schemaItem.setMainTable(parseTable((SQLExprTableSource) tableSource));
    } else if (tableSource instanceof SQLJoinTableSource) {
        // JOIN：SELECT ... FROM product p LEFT JOIN category c ON c.id = p.category_id
        SQLJoinTableSource join = (SQLJoinTableSource) tableSource;
        // 左表 = mainTable (product)
        schemaItem.setMainTable(parseTable(join.getLeft()));
        // 右表 = aliasTableItem (category)
        schemaItem.addAliasTable(parseTable(join.getRight()));
        // ON 条件 → relations
        // ("c.id", "p.category_id") 表示 c.id = p.category_id
        schemaItem.addRelation(parseOnCondition(join.getCondition()));
    } else if (tableSource instanceof SQLSubqueryTableSource) {
        // 子查询：SELECT * FROM (SELECT ... FROM t1) sub
        // → 递归解析子查询
        schemaItem = parse(((SQLSubqueryTableSource) tableSource).getSelect());
    }

    // 3. 遍历 SELECT 字段，用 ColumnVisitor 处理 6 种表达式类型
    List<SQLSelectItem> selectItems = selectStmt.getSelect().getQueryBlock().getSelectList();
    for (SQLSelectItem item : selectItems) {
        SQLExpr expr = item.getExpr();
        // SQLIdentifierExpr: "name" → 字段名
        // SQLPropertyExpr: "p.name" → 别名.字段名
        // SQLMethodInvokeExpr: "CONCAT(a, b)" → 函数调用
        // SQLBinaryOpExpr: "a + b" → 二元运算
        // SQLCaseExpr: "CASE WHEN ..." → 条件表达式
        // SQLAllColumnExpr: "p.*" → 全字段
        FieldItem field = ColumnVisitor.visit(expr);
        schemaItem.addSelectField(item.getAlias(), field);
    }
    // 最终 SchemaItem:
    //   mainTable: product (alias "p")
    //   aliasTableItems: {"c": category}
    //   selectFields: {"_id": p.id, "name": p.name, "price": p.price, 
    //                   "category_name": c.category_name, "description": p.description}
    //   relations: [("c.id", "p.category_id")]
    return schemaItem;
}
```

> **这一步在干什么？** SqlParser 用 Druid 的 MySQL 语法解析器把 yml 中的 SQL 字符串解析成 AST，再遍历 AST 提取出同步所需的结构信息：主表、关联表、SELECT 字段、JOIN 条件。FROM 子句有 3 种类型（单表、JOIN、子查询），SELECT 字段有 6 种表达式类型（标识符、属性引用、函数调用、二元运算、CASE、全字段）。这些信息决定了后续变更同步的分发路径。

**Step 2：ESSyncService.sync() 分发 —— 4 条路径**

**源码位置**: `client-adapter/es7x/src/main/java/com/alibaba/otter/canal/client/adapter/es7x/service/ESSyncService.java`

```java
public void sync(Dml dml, SchemaItem schemaItem) {
    String table = dml.getTable(); // 变更的表名
    String type = dml.getType();   // INSERT/UPDATE/DELETE

    // 路径判断
    if (schemaItem.getAliasTableItems().isEmpty()) {
        // (a) 无 JOIN，单表简单字段 → 直接映射
        mainTableSimpleFieldOperation(dml, schemaItem);
    } else if (schemaItem.getMainTable().getTableName().equals(table)) {
        // (b) 主表变更 + 有 JOIN
        // 检查：变更的字段是否都是简单字段（非函数/非运算）
        //   AND 外键字段没有变化
        if (allFieldsSimple(dml) && !fkChanged(dml, schemaItem)) {
            // 简单字段变更且外键没变 → 只更新 ES 文档中对应的字段
            mainTableSimpleFieldOperation(dml, schemaItem);
        } else {
            // 外键变了 或 字段需要函数计算 → 必须回查 SQL 获取完整数据
            mainTableComplexFieldOperation(dml, schemaItem);
            // 执行原始 JOIN SQL + WHERE pk=? 回查数据库
        }
    } else {
        // (c) 关联表变更 → 需要反查主表
        for (AliasTableItem aliasTable : schemaItem.getAliasTableItems().values()) {
            if (aliasTable.getTableName().equals(table)) {
                // category 表变更 → 找到所有引用该 category 的 product
                joinTableSimpleFieldOperation(dml, schemaItem, aliasTable);
            }
        }
    }
    // (d) 子查询/子表 → 递归处理
}
```

> **这一步在干什么？** ESSyncService 根据 SchemaItem 的结构和变更来源分发到 4 条路径：(a) 单表无 JOIN，字段直接映射到 ES 文档；(b) 主表变更但有 JOIN，如果变更字段都是简单字段且外键没变，只更新对应字段，否则必须回查 JOIN SQL 获取完整数据（比如 category_name 需要关联查询）；(c) 关联表变更（如 category 表改名），需要反查所有引用该 category 的主表记录，批量更新 ES；(d) 子查询场景递归处理。

**Step 3：主表 INSERT + JOIN —— 回查 SQL 构建与 ES 写入**

```java
// mainTableComplexFieldOperation() —— 主表变更，需回查 JOIN SQL
public void mainTableComplexFieldOperation(Dml dml, SchemaItem schemaItem) {
    // 1. 获取主键值
    Map<String, Object> pkValues = dml.getPkValues(); // {id: 1001}

    // 2. 构建回查 SQL = 原始 JOIN SQL + WHERE 主键=?
    String querySql = schemaItem.getSql() + " WHERE p.id = ?";
    // SELECT p.id AS _id, p.name, p.price,
    //        c.category_name, p.description, p.location
    // FROM product p LEFT JOIN category c ON c.id = p.category_id
    // WHERE p.id = 1001

    // 3. 执行回查
    Connection conn = dataSource.getConnection();
    PreparedStatement pstmt = conn.prepareStatement(querySql);
    pstmt.setObject(1, pkValues.get("id"));
    ResultSet rs = pstmt.executeQuery();

    // 4. ResultSet → Map（ES 文档格式）
    if (rs.next()) {
        Map<String, Object> esData = new HashMap<>();
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = rs.getObject(i);
            // 类型转换：ES 特有类型处理
            esData.put(columnName, ESSyncUtil.typeConversion(value, columnMeta));
        }
        // esData = {_id: 1001, name: "iPhone", price: 999, 
        //           category_name: "Electronics", description: "..."}

        // 5. 写入 ES
        esTemplate.insert(mapping, pkValues.get("id").toString(), esData);
    }
}
```

> **这一步在干什么？** 当主表 INSERT 且配置了 JOIN 时，不能只把 product 表的数据写入 ES——因为 `category_name` 来自 category 表。所以必须回查数据库：用原始 JOIN SQL 加上 `WHERE p.id=?` 条件，查出包含分类名称的完整记录，转成 Map 后写入 ES。这是一次额外的数据库查询，是 ES 同步相比 RDB 同步多出的成本。

**Step 4：关联表 UPDATE —— 反查受影响的主表记录批量更新**

```java
// joinTableSimpleFieldOperation() —— category 表变更
public void joinTableSimpleFieldOperation(Dml dml, SchemaItem schemaItem, 
                                          AliasTableItem aliasTable) {
    // category 表 UPDATE: category_name 改了
    // → 所有引用该 category 的 product 都需要更新 ES

    // 1. 获取关联表变更的值（用于反查）
    Map<String, Object> changedValues = dml.getData();
    // {id: 5, category_name: "Consumer Electronics"}

    // 2. 从 JOIN 条件提取反查条件
    // ON c.id = p.category_id → WHERE c.id = 5
    RelationFields relation = schemaItem.getRelation(aliasTable.getAlias());
    // relation.leftField = "c.id", relation.rightField = "p.category_id"
    Object joinValue = changedValues.get("id"); // 5

    // 3. 构建反查 SQL：找出所有 category_id=5 的 product
    String reverseQuerySql = schemaItem.getSql() + " WHERE c.id = ?";
    // SELECT p.id AS _id, p.name, p.price,
    //        c.category_name, p.description, p.location
    // FROM product p LEFT JOIN category c ON c.id = p.category_id
    // WHERE c.id = 5

    // 4. 执行反查，逐条更新 ES
    ResultSet rs = dataSource.query(reverseQuerySql, joinValue);
    while (rs.next()) {
        Map<String, Object> esData = resultSetToMap(rs);
        String esId = rs.getString("_id"); // product id
        // 每个受影响的商品都更新 ES 文档
        esTemplate.update(mapping, esId, esData);
        // 50 个商品引用了 category 5 → 50 条 ES update
    }
}
```

> **这一步在干什么？** 关联表（category）变更时，ES 中所有引用该 category 的 product 文档都需要更新。反查逻辑：从 JOIN ON 条件提取关联字段（`c.id = p.category_id`），用 category 变更后的 `id` 值构建反查 SQL，找出所有 `category_id=5` 的 product 记录。逐条更新对应的 ES 文档——如果一个分类被 50 个商品引用，就产生 50 条 ES update 请求。

**Step 5：ESSyncUtil 类型转换 —— ES 特有类型处理**

**源码位置**: `client-adapter/es7x/src/main/java/com/alibaba/otter/canal/client/adapter/es7x/support/ESSyncUtil.java`

```java
public static Object typeConversion(Object val, FieldMeta fieldMeta) {
    String esType = fieldMeta.getEsType();
    if (val == null) return null;

    // 1. 整数类型 —— 兼容 Number 或 String
    if ("integer".equals(esType) || "long".equals(esType) || "short".equals(esType)) {
        if (val instanceof Number) return ((Number) val).longValue();
        if (val instanceof String) return Long.parseLong((String) val);
    }

    // 2. scaled_float —— BigDecimal
    if ("scaled_float".equals(esType)) {
        return new BigDecimal(val.toString());
    }

    // 3. boolean
    if ("boolean".equals(esType)) {
        if (val instanceof Boolean) return val;
        if (val instanceof Number) return ((Number) val).intValue() != 0;
        if (val instanceof String) return "true".equalsIgnoreCase((String) val);
    }

    // 4. date —— 5 种子分支
    if ("date".equals(esType)) {
        if (val instanceof Time) return val.toString();          // "14:30:00"
        if (val instanceof Timestamp) return ((Timestamp) val).getTime(); // 毫秒时间戳
        if (val instanceof Date) return new SimpleDateFormat("yyyy-MM-dd").format(val);
        if (val instanceof Long) return val;                     // 已是时间戳
        if (val instanceof String) return val;                   // 字符串格式日期
    }

    // 5. binary —— Base64 编码
    if ("binary".equals(esType)) {
        if (val instanceof byte[]) return Base64.getEncoder().encodeToString((byte[]) val);
        if (val instanceof String) return val; // 已是 Base64
    }

    // 6. geo_point —— "lat,lon" → Map
    if ("geo_point".equals(esType)) {
        String[] parts = val.toString().split(",");
        Map<String, Object> geoPoint = new HashMap<>();
        geoPoint.put("lat", Double.parseDouble(parts[0].trim()));
        geoPoint.put("lon", Double.parseDouble(parts[1].trim()));
        return geoPoint;
    }

    // 7. array —— 自动检测分隔符
    if (esType != null && esType.startsWith("array")) {
        String str = val.toString();
        // 自动检测分隔符：逗号/分号/竖线
        String delimiter = str.contains(",") ? "," : 
                           str.contains(";") ? ";" : "\\|";
        return Arrays.asList(str.split(delimiter));
    }

    // 8. object —— JSON → Map
    if ("object".equals(esType) || "nested".equals(esType)) {
        return JSON.parseObject(val.toString(), Map.class);
    }

    return val; // 默认透传
}
```

> **这一步在干什么？** ES 比关系型数据库有更多特有类型，ESSyncUtil 处理了 8 大类转换。date 类型最复杂——有 5 个子分支处理 Time、Timestamp、Date、Long、String 不同输入。binary 转 Base64 字符串（ES 的 binary 类型要求 Base64）。geo_point 把 `"30.12,120.15"` 解析成 `{"lat":30.12, "lon":120.15}` 的 Map。array 类型自动检测分隔符（逗号/分号/竖线）。object/nested 把 JSON 字符串解析成 Map。

**Step 6：父子文档 —— relations 配置与 _routing 注入**

```java
// 父子文档配置
// esMapping:
//   _index: products
//   _id: _id
//   relations:
//     - name: product_review
//       parent: product
//       child: review
//       routing: product_id

public void insertParentChild(ESMapping mapping, Dml dml) {
    String relationName = mapping.getRelationName(); // "product_review"
    String parentType = mapping.getParentType();     // "product"
    String childType = mapping.getChildType();       // "review"

    if (dml.getTable().equals(childType)) {
        // 子文档（review 表变更）
        Map<String, Object> data = dml.getData();
        String parentId = data.get(mapping.getRouting()).toString(); // product_id

        IndexRequest indexRequest = new IndexRequest(mapping.get_index())
            .id(data.get("_id").toString())
            .source(data);
        
        // 注入 _routing = 父文档 ID
        // ES 用 routing 值确保父子文档分配到同一个分片
        indexRequest.routing(parentId);
        
        // 同时在 source 中设置 relations 字段
        data.put("product_review", Collections.singletonMap("name", "review"));
        // _routing 保证父子同分片，这是 ES join 查询的前提
    }
}

// DELETE 路由问题
public void deleteParentChild(ESMapping mapping, String esId) {
    DeleteRequest deleteRequest = new DeleteRequest(mapping.get_index(), esId);
    // 注意：DELETE 请求没有设置 routing！
    // 如果 ES 集群有多个分片，不带 routing 的 DELETE 可能路由到错误分片
    // 导致删除不生效 —— 这是一个已知的潜在 bug
    // 修复方案：deleteRequest.routing(parentId);
}
```

> **这一步在干什么？** ES 的 parent-child 关系要求父子文档必须在同一个分片上，通过 `_routing` 值实现。INSERT 子文档时设置 `indexRequest.routing(parentId)` 确保路由正确。但 DELETE 时 Canal 没有设置 routing——如果索引有多个分片，不带 routing 的 DELETE 请求可能被路由到错误分片，导致删除不生效。这是一个已知的潜在 bug，修复方案是在 DeleteRequest 上也设置 routing。

**Step 7：ES7xTemplate Bulk 机制 —— 攒批提交与错误处理**

```java
public class ES7xTemplate implements ESTemplate {
    private BulkRequest bulkRequest = new BulkRequest();
    private int bulkCount = 0;

    public void insert(ESMapping mapping, String esId, Map<String, Object> esData) {
        IndexRequest indexRequest = new IndexRequest(mapping.get_index())
            .id(esId)
            .source(esData);
        bulkRequest.add(indexRequest);
        bulkCount++;
        commitIfNecessary();
    }

    public void update(ESMapping mapping, String esId, Map<String, Object> esData) {
        UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), esId)
            .doc(esData);
        bulkRequest.add(updateRequest);
        bulkCount++;
        commitIfNecessary();
    }

    public void delete(ESMapping mapping, String esId) {
        DeleteRequest deleteRequest = new DeleteRequest(mapping.get_index(), esId);
        bulkRequest.add(deleteRequest);
        bulkCount++;
        commitIfNecessary();
    }

    private void commitIfNecessary() {
        if (bulkCount >= mapping.getCommitBatch()) { // 3000
            BulkResponse response = restHighLevelClient.bulk(bulkRequest, 
                RequestOptions.DEFAULT);
            
            // 错误处理
            if (response.hasFailures()) {
                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        logger.error("ES bulk failure: " + item.getFailureMessage());
                        // 可配置：失败重试 or 跳过
                    }
                }
            }
            // 重置 bulk
            bulkRequest = new BulkRequest();
            bulkCount = 0;
        }
    }
}
```

> **这一步在干什么？** ES 的 Bulk API 把多个单条操作（INSERT/UPDATE/DELETE）合并成一个批量 HTTP 请求，大幅减少网络往返。ES7xTemplate 在内存中累积 `BulkRequest`，达到 `commitBatch`（默认 3000）阈值时一次性提交。提交后检查 `BulkResponse.hasFailures()`——Bulk 中部分操作可能失败（如版本冲突、映射错误），逐条记录失败信息但不中断整体同步。

**Step 8：REST vs Transport 双模式 —— 版本兼容**

```java
// ESConnection —— 双模式封装
public class ESConnection {
    private RestHighLevelClient restHighLevelClient; // REST 模式（ES7 推荐）
    private TransportClient transportClient;          // Transport 模式（ES6 及以前）
    private String mode; // "REST" or "Transport"

    // 每个请求都要判断模式
    public BulkResponse bulk(BulkRequest request) {
        if ("REST".equals(mode)) {
            return restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        } else {
            return transportClient.bulk(request).get();
        }
    }
}

// ES7 vs ES6 的关键差异：type 移除
// ES6: new IndexRequest("index", "type", "id")
// ES7: new IndexRequest("index").id("id")  // type 被废弃
// Canal 的 ES7xTemplate 使用 ES7 API（无 type）
// ES6xTemplate 保留 type 参数
```

> **这一步在干什么？** ES 经历了 TransportClient（TCP 二进制协议）到 RestHighLevelClient（HTTP REST）的迁移。ESConnection 同时持有两种客户端，通过配置选择模式。ES7 还移除了 `type` 概念（ES6 的 `index/type/id` 变成 ES7 的 `index/id`），Canal 分别提供 `ES7xTemplate` 和 `ES6xTemplate` 处理 API 差异。

**ES Adapter 架构图**

```
  MySQL binlog                      ES Adapter                              Elasticsearch
       |                                |                                       |
  product INSERT ─────────────→ ESSyncService.sync()                         |
       |                         ├─ SchemaItem: mainTable=product             |
       |                         ├─ 有 JOIN → 回查 SQL                        |
       |                         │  SELECT p.*, c.category_name              |
       |                         │  FROM product p JOIN category c           |
       |                         │  WHERE p.id = 1001                        |
       |                         │        ↓                                  |
       |                         │  ResultSet → Map → ESSyncUtil 类型转换     |
       |                         │        ↓                                  |
       |                         └─ esTemplate.insert() ──BulkRequest───→ bulk API
       |                                                                    → index products
       |
  category UPDATE ─────────────→ ESSyncService.sync()                         |
       |                         ├─ SchemaItem: aliasTable=category           |
       |                         ├─ 关联表变更 → 反查主表                      |
       |                         │  SELECT p.* FROM product p JOIN category c |
       |                         │  WHERE c.id = 5                            |
       |                         │        ↓                                  |
       |                         │  50 条 product → 50 个 ES update           |
       |                         └─ esTemplate.update() × 50 ──Bulk──→ bulk API
       |                                                                    → 50 docs updated
```

---

## 案例九：Spring XML 模式 vs Manager 模式 —— 两种配置装配方式的全链路对比

### 场景描述

Canal 的 Instance 配置有两种来源：Spring XML 模式从本地文件读取 `instance.properties`，Manager 模式从 Canal Admin 的 HTTP API 远程拉取配置。两种模式最终都用同一套 Spring XML 装配组件，区别只在于占位符的值来源不同。本案例完整追踪两条路径的源码，并对比配置热更新机制。

### 配置

```properties
# Spring 模式
canal.instance.global.mode = spring
canal.instance.global.spring.xml = classpath:spring/file-instance.xml

# Manager 模式
canal.instance.global.mode = manager
canal.instance.global.manager.address = 127.0.0.1:8089  # Canal Admin 地址
```

### 全链路源码追踪

**Step 1：Spring XML Bean 定义 —— 5 大组件装配**

**源码位置**: `deployer/src/main/resources/spring/default-instance.xml`

```xml
<!-- default-instance.xml：5 大核心组件的 Bean 定义 -->
<bean id="instance" class="com.alibaba.otter.canal.instance.spring.CanalInstanceWithSpring">
    <property name="eventParser"   ref="eventParser" />
    <property name="eventSink"     ref="eventSink" />
    <property name="eventStore"    ref="eventStore" />
    <property name="metaManager"   ref="metaManager" />
    <property name="alarmHandler"  ref="alarmHandler" />
</bean>

<!-- 1. 报警处理器 -->
<bean id="alarmHandler" class="com.alibaba.otter.canal.common.alarm.LogAlarmHandler" />

<!-- 2. 位点管理器 —— PeriodMixed = 内存 + 定期刷 ZK -->
<bean id="metaManager" class="com.alibaba.otter.canal.instance.manager.PeriodMixedMetaManager">
    <property name="zooKeeperMetaManager">
        <bean class="com.alibaba.otter.canal.instance.manager.ZooKeeperMetaManager">
            <property name="zkClientx" value="${canal.zkServers}" />
        </bean>
    </property>
    <property name="period" value="1000" /> <!-- 1秒刷一次 ZK -->
</bean>

<!-- 3. 事件存储 —— RingBuffer -->
<bean id="eventStore" class="com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer">
    <property name="bufferSize" value="${canal.instance.memory.buffer.size:16384}" />
    <property name="bufferMemUnit" value="${canal.instance.memory.buffer.memunit:1024}" />
    <property name="batchMode" value="${canal.instance.memory.batch.mode:ITEMSIZE}" />
</bean>

<!-- 4. 事件 Sink —— 过滤+投递 -->
<bean id="eventSink" class="com.alibaba.otter.canal.sink.entry.EntryEventSink">
    <property name="eventStore" ref="eventStore" />
    <property name="filterTransactionEntry" value="${canal.instance.filter.transaction.entry:false}" />
</bean>

<!-- 5. 事件 Parser —— binlog 解析器（RDS 代理模式） -->
<bean id="eventParser" 
      class="com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogEventParserProxy">
    <property name="destination" value="${canal.instance.destination}" />
    <property name="masterConnection" ref="masterConnection" />
    <property name="eventFilter" ref="eventFilter" />           <!-- 白名单 -->
    <property name="eventBlackFilter" ref="eventBlackFilter" /> <!-- 黑名单 -->
    <property name="heartbeatHaMeasurer" ref="heartbeatHaMeasurer" />
    <!-- SSL 配置 -->
    <property name="enableTsdb" value="${canal.instance.tsdb.enable:true}" />
</bean>
```

> **这一步在干什么？** `default-instance.xml` 定义了 CanalInstance 的 5 大组件：alarmHandler（日志报警）、metaManager（位点管理，PeriodMixed 包装 ZooKeeperMetaManager 实现内存+定期刷盘）、eventStore（RingBuffer 内存存储，默认 16384 slot）、eventSink（EntryEventSink 过滤+投递）、eventParser（RdsBinlogEventParserProxy 解析器，含心跳/过滤/SSL 配置）。所有 `${...}` 占位符的值在 Spring 模式下来自 `instance.properties` 文件，在 Manager 模式下来自远程 HTTP API。

**Step 2：PropertyPlaceholderConfigurer —— 占位符解析优先级链**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/PropertyPlaceholderConfigurer.java`

```java
public class PropertyPlaceholderConfigurer 
        extends org.springframework.beans.factory.config.PropertyPlaceholderConfigurer {

    // ThreadLocal —— Manager 模式注入远程配置的通道
    public static ThreadLocal<Properties> propertiesLocal = new ThreadLocal<>();

    protected String resolvePlaceholder(String placeholder, Properties props, int systemMode) {
        String value = null;

        // 优先级链（从高到低）：
        
        // 1. System properties (OVERRIDE 模式) —— 最高优先级
        //    -Dcanal.instance.master.address=xxx 启动参数覆盖一切
        if (systemMode == SYSTEM_PROPERTIES_MODE_OVERRIDE) {
            value = System.getProperty(placeholder);
            if (value != null) return value;
        }

        // 2. ThreadLocal properties —— Manager 模式注入的远程配置
        Properties localProps = propertiesLocal.get();
        if (localProps != null) {
            value = localProps.getProperty(placeholder);
            if (value != null) return value;
        }

        // 3. File properties —— conf/{dest}/instance.properties
        value = props.getProperty(placeholder);
        if (value != null) return value;

        // 4. System properties (FALLBACK 模式) —— 兜底
        if (systemMode == SYSTEM_PROPERTIES_MODE_FALLBACK) {
            value = System.getProperty(placeholder);
            if (value != null) return value;
        }

        // 5. 默认值（placeholder:default 语法）
        return null; // 返回 null → Spring 用 ${...:defaultValue} 中的默认值
    }
}
```

> **这一步在干什么？** 这是 Spring 和 Manager 两种模式能共用同一套 XML 的关键。占位符解析有 5 级优先级链：1）`-D` 启动参数（OVERRIDE，最高）；2）ThreadLocal `propertiesLocal`——这是 Manager 模式的注入通道，远程拉取的 Properties 放进 ThreadLocal，解析时从这里取值；3）文件 Properties——这是 Spring 模式的值来源，`conf/{dest}/instance.properties`；4）`-D` 启动参数（FALLBACK，兜底）；5）XML 中 `${...:default}` 的默认值。Manager 模式走第 2 级，Spring 模式走第 3 级。

**Step 3：SpringCanalInstanceGenerator.generate() —— Spring 模式完整流程**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/SpringCanalInstanceGenerator.java`

```java
public CanalInstance generate(String destination, InstanceConfig instanceConfig) {
    synchronized (CanalEventParser.class) {
        // 1. 设置 JVM 全局属性
        //    Spring XML 中 ${canal.instance.destination} 通过 System.getProperty() 读取
        System.setProperty("canal.instance.destination", destination);
        //    "user_sync"

        // 2. 清除 ThreadLocal（Spring 模式不使用远程配置）
        PropertyPlaceholderConfigurer.propertiesLocal.remove();

        // 3. 创建 Spring ApplicationContext
        String springXml = instanceConfig.getSpringXml();
        // "classpath:spring/file-instance.xml"
        this.beanFactory = new ClassPathXmlApplicationContext(springXml);
        // → 加载 XML → 解析占位符 → 实例化 5 大组件 Bean
        //   占位符值来自 conf/{destination}/instance.properties（第 3 级优先级）

        // 4. 获取 instance Bean
        CanalInstance canalInstance = (CanalInstance) beanFactory.getBean("instance");
        canalInstance.setDestination(destination);
        return canalInstance;
    }
}
```

> **这一步在干什么？** Spring 模式的 `generate()` 流程：先 `System.setProperty` 设 destination（XML 中 `${canal.instance.destination}` 通过 System.getProperty 读取），然后清除 ThreadLocal（确保不误用 Manager 模式残留的远程配置），最后创建 `ClassPathXmlApplicationContext` 加载 Spring XML。占位符从 `conf/{destination}/instance.properties` 文件解析——这是 Spring 模式与 Manager 模式的唯一区别：值来源不同，XML 装配逻辑完全相同。

**Step 4：PlainCanalInstanceGenerator.generate() —— Manager 模式完整流程**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/PlainCanalInstanceGenerator.java`

```java
public CanalInstance generate(String destination, InstanceConfig instanceConfig) {
    synchronized (CanalEventParser.class) {
        // 1. HTTP 调用远程 Canal Admin 获取配置
        PlainCanal canal = canalConfigClient.findInstance(destination, null);
        // GET http://127.0.0.1:8089/api/v1/config/instance_polling/user_sync?md5=null
        // 返回: {
        //   "content": "canal.instance.master.address=10.0.0.1:3306\ncanal.instance.filter.regex=...",
        //   "md5": "a1b2c3d4..."
        // }

        // 2. 解析远程配置为 Properties
        Properties remoteProperties = canal.getProperties();
        // {canal.instance.master.address: "10.0.0.1:3306", ...}

        // 3. 合并本地 canal.properties 的全局配置
        //    本地配置作为基础，远程配置覆盖
        Properties mergedProperties = new Properties();
        mergedProperties.putAll(this.canalConfig); // 本地全局配置
        mergedProperties.putAll(remoteProperties);  // 远程 Instance 配置覆盖

        // 4. 注入 ThreadLocal —— 占位符解析的第 2 级优先级
        PropertyPlaceholderConfigurer.propertiesLocal.set(mergedProperties);
        //    这一步是关键！ThreadLocal 中的 Properties 会在 XML 占位符解析时被读取

        // 5. 同样创建 Spring ApplicationContext
        System.setProperty("canal.instance.destination", destination);
        String springXml = instanceConfig.getSpringXml();
        // Manager 模式默认用 classpath:spring/default-instance.xml
        this.beanFactory = new ClassPathXmlApplicationContext(springXml);
        // → 加载 XML → 解析占位符 → 从 ThreadLocal 取值！

        // 6. 获取 instance Bean
        CanalInstance canalInstance = (CanalInstance) beanFactory.getBean("instance");
        canalInstance.setDestination(destination);
        return canalInstance;
    }
}
```

> **这一步在干什么？** Manager 模式的 `generate()` 与 Spring 模式的核心差异在第 1-4 步：先通过 HTTP 从 Canal Admin 拉取该 destination 的配置（`findInstance()`），解析成 Properties，合并本地全局配置后注入 `PropertyPlaceholderConfigurer.propertiesLocal` ThreadLocal。后续创建 `ClassPathXmlApplicationContext` 加载 XML 时，占位符解析器从 ThreadLocal（第 2 级优先级）取值，而非从文件（第 3 级）取值。XML 文件和 Bean 装配逻辑完全相同——两种模式的差异仅在于占位符值来源。

**Step 5：PlainCanalConfigClient HTTP 协议 —— 3 个端点 + MD5 差量**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/monitor/PlainCanalConfigClient.java`

```java
public class PlainCanalConfigClient {
    // 3 个 HTTP 端点
    // 1. findServer —— 获取 canal.properties 全局配置
    public PlainCanal findServer(String md5) {
        // GET /api/v1/config/server_polling?md5={md5}
        // md5 != null 时，服务端对比 MD5，相同返回 304（无变更）
        // md5 == null 时，返回完整配置
        ResponseModel<CanalConfig> response = httpGet("/api/v1/config/server_polling?md5=" + md5);
        if (response.getCode() == 200) {
            return new PlainCanal(response.getData().getContent(), response.getData().getMd5());
        }
        return null; // 304 或无变更
    }

    // 2. findInstance —— 获取某个 destination 的 instance.properties
    public PlainCanal findInstance(String destination, String md5) {
        // GET /api/v1/config/instance_polling/{destination}?md5={md5}
        ResponseModel<CanalConfig> response = httpGet(
            "/api/v1/config/instance_polling/" + destination + "?md5=" + md5);
        if (response.getCode() == 200) {
            return new PlainCanal(response.getData().getContent(), response.getData().getMd5());
        }
        return null;
    }

    // 3. findInstances —— 获取所有 destination 列表（用于动态发现）
    public List<InstanceConfig> findInstances(String md5) {
        // GET /api/v1/config/instances_polling?md5={md5}
        ResponseModel<List<InstanceConfig>> response = httpGet(
            "/api/v1/config/instances_polling?md5=" + md5);
        return response.getData();
    }
}

// ResponseModel 格式
public class ResponseModel<T> {
    private int code;    // 200=有变更, 304=无变更
    private T data;      // CanalConfig or List<InstanceConfig>
}
// CanalConfig
public class CanalConfig {
    private String content; // 配置内容（properties 格式文本）
    private String md5;     // 内容的 MD5 摘要
}
```

> **这一步在干什么？** PlainCanalConfigClient 封装了与 Canal Admin 的 HTTP 通信，有 3 个端点：`findServer` 拉取全局配置、`findInstance` 拉取单个 destination 配置、`findInstances` 拉取所有 destination 列表。每个请求都带 `md5` 参数——服务端对比 MD5，如果配置没变就返回 304（不传输完整内容），只有配置变化时才返回 200+新内容。这是一种增量轮询优化：5 秒轮询一次，但大部分请求得到 304，只有真正变更时才传输数据。

**Step 6：配置热更新 —— 两种监控器的扫描循环**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/monitor/ManagerInstanceConfigMonitor.java` + `SpringInstanceConfigMonitor.java`

```java
// ManagerInstanceConfigMonitor —— 远程配置热更新
public void start() {
    executor.scheduleWithFixedDelay(() -> {
        // 每 5 秒轮询所有 destination 配置
        for (String destination : instanceConfigs.keySet()) {
            String oldMd5 = instanceMd5Map.get(destination);
            PlainCanal newConfig = canalConfigClient.findInstance(destination, oldMd5);
            
            if (newConfig != null) {
                // 配置变更了（MD5 不同）
                String newMd5 = newConfig.getMd5();
                instanceMd5Map.put(destination, newMd5);

                // 处理顺序：stop → reload → start
                // 1. 停止旧 Instance
                ServerRunningMonitor monitor = ServerRunningMonitors.getRunningMonitor(destination);
                monitor.stop(); // → embeddedServer.stop(dest) → canalInstance.stop()
                // → parser.stop() → MySQL 连接断开 → dump 线程退出

                // 2. 重新加载配置
                InstanceConfig newInstanceConfig = parseInstanceConfig(newConfig.getProperties());
                instanceConfigs.put(destination, newInstanceConfig);

                // 3. 重新启动
                monitor.start(); // → canalInstances.get(dest) 触发 MigrateMap 重新创建
                // → instanceGenerator.generate() → 从远程拉取新配置 → new CanalInstance
            }
        }
    }, 0, 5, TimeUnit.SECONDS);
}

// SpringInstanceConfigMonitor —— 本地文件热更新
public void start() {
    executor.scheduleWithFixedDelay(() -> {
        // 每 5 秒扫描 conf/ 目录
        File confDir = new File(rootConf);
        for (File instanceDir : confDir.listFiles(File::isDirectory)) {
            File propsFile = new File(instanceDir, "instance.properties");
            String destination = instanceDir.getName();
            
            long currentModified = propsFile.lastModified();
            Long lastModified = fileModifiedMap.get(destination);
            
            if (lastModified == null) {
                // 新增 destination
                startDestination(destination);
            } else if (currentModified > lastModified) {
                // 配置文件被修改（touch 或编辑）
                // 同样的顺序：stop → reload → start
                stopDestination(destination);
                reloadDestination(destination); // 重新读取 instance.properties
                startDestination(destination);
            }
            fileModifiedMap.put(destination, currentModified);
        }
    }, 0, 5, TimeUnit.SECONDS);
}
```

> **这一步在干什么？** 两种监控器都按 5 秒间隔检测配置变更，处理顺序相同：stop→reload→start。Manager 模式通过 HTTP MD5 对比检测变更——服务端返回非 null 表示配置变了。Spring 模式通过文件 `lastModified()` 时间戳对比——文件被 touch 或编辑后时间戳变化。检测到变更后先停旧 Instance（断开 MySQL 连接、停止 dump 线程），再加载新配置，最后启动新 Instance（重新连接 MySQL、从新位点开始 dump）。

**Spring 模式 vs Manager 模式对比表**

| 特性 | Spring 模式 | Manager 模式 |
|------|------------|--------------|
| 配置来源 | 本地 `conf/{dest}/instance.properties` | Canal Admin HTTP API |
| 占位符取值优先级 | 第 3 级（File properties） | 第 2 级（ThreadLocal） |
| ThreadLocal 使用 | 不使用（remove） | 使用（set 远程 Properties） |
| 配置变更检测 | 文件 lastModified 扫描 | HTTP MD5 对比 |
| 适用规模 | 少量 Instance（< 20） | 大规模集中管理（100+） |
| 运维方式 | SSH 登录修改文件 | Web 界面统一管理 |
| 配置版本管理 | 无（文件覆盖） | Canal Admin 支持版本/回滚 |

**XML 文件变体对比表**

| XML 文件 | metaManager | eventStore | 适用场景 |
|----------|-------------|------------|----------|
| `memory-instance.xml` | MemoryMetaManager（纯内存） | MemoryEventStoreWithBuffer | 测试/开发，重启丢位点 |
| `file-instance.xml` | FileMixedMetaManager（内存+文件） | MemoryEventStoreWithBuffer | 单机生产，位点持久化到本地文件 |
| `default-instance.xml` | PeriodMixedMetaManager（内存+ZK） | MemoryEventStoreWithBuffer | HA 集群，位点持久化到 ZK |
| `group-instance.xml` | PeriodMixed + GroupEventParser | MemoryEventStoreWithBuffer | 多 MySQL 源合并（_group_） |

---


## 案例十：Disruptor 并行解析 —— 四阶段流水线加速 Binlog 处理

### 场景描述

单线程串行解析 binlog 在高 TPS 场景下成为瓶颈：一条 RowsLogEvent 的 DML 解析（字段映射、类型转换、Entry 构建）可能耗时数毫秒，串行时这些开销全部叠加在主循环上。Canal 1.1.1 引入了基于 LMAX Disruptor 的四阶段并行流水线：Stage 1（Publish）→ Stage 2（SimpleParserStage，轻量分发）→ Stage 3（DmlParserStage，WorkerPool 多线程 DML 解析）→ Stage 4（SinkStoreStage，顺序入库）。开启后，Stage 3 的多个 Worker 可以并行处理 DML 事件，而 Stage 4 保证最终入 Store 的顺序与 binlog 原始顺序一致。

### 配置

```properties
# canal.properties 或 instance.properties
canal.instance.parser.parallel = true
# 默认即为 true，显式设为 false 则走串行模式
canal.instance.parser.parallelBufferSize = 256
# Disruptor RingBuffer 大小，必须是 2 的幂
```

### 全链路源码追踪

**Step 1：AbstractEventParser.start() —— 并行与串行的分支判断**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/AbstractEventParser.java`

```java
public void start() {
    // ...
    // 1. 读取 parallel 配置
    // this.parallel = true（默认值）

    // 2. 构建 Coprocessor（协处理器）
    if (parallel) {
        // ===== 并行模式 =====
        // 构建 Disruptor 四阶段流水线
        int ringBufferSize = getParallelBufferSize(); // 默认 256，必须 2 的幂
        buildMultiStageCoprocessor(ringBufferSize);
        // → 创建 MysqlMultiStageCoprocessor 并 start()
    }
    // 如果 parallel == false，coprocessor 保持 null → 后续走串行路径

    // 3. 启动 parseThread
    parseThread = new Thread(new Runnable() {
        public void run() {
            while (running) {
                // dump binlog 主循环
                // ...
                if (parallel) {
                    // 并行模式：事件交给 coprocessor 处理
                    // coprocessor.publish(buffer);
                } else {
                    // 串行模式：直接在当前线程完成全部解析
                    // LogEvent event = decoder.decode(buffer);
                    // Entry entry = logEventConvert.parse(event);
                    // transactionBuffer.add(entry);
                }
            }
        }
    });
    parseThread.start();
}
```

串行模式下，dump 线程自己完成 decode → parse → filter → transactionBuffer 的全部工作。这是一条没有任何并发的简单路径：

```java
// 串行路径（parallel == false）
// 在 parseThread 内部的 dump callback 中
LogEvent logEvent = decoder.decode(new LogBuffer(buffer));
// → 直接 decode，无需经过 RingBuffer

Entry entry = logEventConvert.parse(logEvent, isSeek);
// → 完整解析：字段映射 + 类型转换 + Entry 构建

if (entry != null) {
    transactionBuffer.add(entry);
    // → 直接加入事务缓冲区，等事务结束后 flush 到 EventSink
}
```

> **这一步在干什么？** `parallel` 配置决定了 Canal 解析 binlog 的核心执行模型。并行模式构建一个 Disruptor 四阶段流水线，dump 线程只负责把原始 buffer 投递到 RingBuffer（Stage 1），解析工作由后续阶段的独立线程完成。串行模式则把所有工作塞进 dump 线程自身——简单但吞吐量受限。对于高 TPS 场景（例如每秒数万行变更），串行模式 CPU 利用率只能打满一个核心，而并行模式可以利用多核并发解析 DML。

**Step 2：MysqlMultiStageCoprocessor 构造 —— Disruptor RingBuffer 与三级 SequenceBarrier 链**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`

```java
public class MysqlMultiStageCoprocessor implements MultiStageCoprocessor {

    // ===== RingBuffer 创建 =====
    // SingleProducer：只有 dump 线程一个生产者，无需 CAS
    private RingBuffer<MessageEvent> disruptorMsgBuffer =
        RingBuffer.createSingleProducer(
            new MessageEventFactory(),  // → 为每个 slot 预分配 MessageEvent 对象
            bufferSize,                 // 256（配置的 parallelBufferSize）
            new BlockingWaitStrategy()  // 消费者等待策略：park/unpark
        );

    public MysqlMultiStageCoprocessor(int ringBufferSize,
                                      LogEventConvert logEventConvert,
                                      EventTransactionBuffer transactionBuffer,
                                      String destination) {
        // ===== 四阶段 SequenceBarrier 链 =====

        // Stage 2 的屏障：等待 Stage 1（Producer）发布
        SequenceBarrier barrier1 = disruptorMsgBuffer.newBarrier();
        // Stage 2 消费者：SimpleParserStage（单线程，轻量分发）
        BatchEventProcessor<MessageEvent> simpleParserStage =
            new BatchEventProcessor<>(disruptorMsgBuffer, barrier1, new SimpleParserStage());

        // Stage 3 的屏障：等待 Stage 2 完成
        SequenceBarrier barrier2 =
            disruptorMsgBuffer.newBarrier(simpleParserStage.getSequence());
        // Stage 3 消费者：DmlParserStage × parserThreadCount（WorkerPool 多线程）
        int parserThreadCount = Runtime.getRuntime().availableProcessors() * 60 / 100;
        // → 取 CPU 核数的 60%，至少 2 个 Worker
        parserThreadCount = Math.max(2, parserThreadCount);
        WorkHandler<MessageEvent>[] dmlParsers = new DmlParserStage[parserThreadCount];
        for (int i = 0; i < parserThreadCount; i++) {
            dmlParsers[i] = new DmlParserStage();
        }
        WorkerPool<MessageEvent> dmlParserPool =
            new WorkerPool<>(disruptorMsgBuffer, barrier2,
                new SimpleFatalExceptionHandler(), dmlParsers);

        // Stage 4 的屏障：等待 Stage 3 所有 Worker 完成
        SequenceBarrier barrier3 =
            disruptorMsgBuffer.newBarrier(dmlParserPool.getWorkerSequences());
        // Stage 4 消费者：SinkStoreStage（单线程，顺序入库）
        BatchEventProcessor<MessageEvent> sinkStoreStage =
            new BatchEventProcessor<>(disruptorMsgBuffer, barrier3, new SinkStoreStage());

        // ===== 注册 gatingSequence =====
        // RingBuffer 通过 gatingSequence 判断哪些 slot 可以复用
        // 只注册 Stage 4（最慢消费者）的 Sequence
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());
    }
}
```

> **这一步在干什么？** 这是整条流水线的骨架搭建。RingBuffer 使用 `createSingleProducer` 因为只有 dump 线程一个生产者，避免了多生产者模式下的 CAS 竞争。三个 SequenceBarrier 形成严格的依赖链：Stage 2 必须等 Stage 1 发布 → Stage 3 必须等 Stage 2 分发完 → Stage 4 必须等 Stage 3 所有 Worker 都处理完。`addGatingSequences` 只注册了 Stage 4（最终消费者），因为 RingBuffer 的 slot 只有在 Stage 4 消费完之后才能被生产者重用。Stage 3 的 Worker 数量取 CPU 核数的 60%（至少 2 个），这是在并发解析吞吐和线程切换开销之间的经验权衡。

**Step 3：MessageEvent 数据结构 —— 贯穿四个阶段的事件载体**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`（内部类）

```java
public static class MessageEvent {
    // ===== Stage 1 设置 =====
    private LogBuffer buffer;          // 原始 binlog 字节缓冲区（Stage 1 publish 设置）

    // ===== Stage 2 设置 =====
    private CanalEntry.Entry entry;    // 解析后的 Entry（Stage 2 对非 DML 事件设置）
    private boolean needDmlParse;      // true = 需要 Stage 3 继续解析（Stage 2 对 DML 设置）
    private TableMeta table;           // 表元数据（Stage 2 预查好，Stage 3 直接使用）
    private LogEvent event;            // decode 后的 LogEvent（Stage 2 设置，Stage 3 读取）

    // ===== Stage 3 设置 =====
    // Stage 3 的 DmlParserStage 在 needDmlParse=true 时做完整解析，
    // 将结果写回 entry 字段

    // ===== 迭代相关（一个 binlog event 可能生成多个 Entry）=====
    private boolean needIterate;                // 是否有多个 Entry 需要迭代
    private List<LogEvent> iterateEvents;       // 迭代的 LogEvent 列表
    private List<TableMeta> iterateTables;      // 对应的 TableMeta 列表
    private List<CanalEntry.Entry> iterateEntrys; // 迭代生成的 Entry 列表

    // 每个 RingBuffer slot 预分配一个 MessageEvent，生命周期 = RingBuffer 生命周期
    // Stage 4 处理完后必须 null 掉所有字段（GC 友好）
}
```

各阶段对 MessageEvent 字段的读写关系：

```
字段            │ Stage 1(写)  │ Stage 2(读/写)   │ Stage 3(读/写)  │ Stage 4(读/清)
────────────────┼──────────────┼──────────────────┼─────────────────┼───────────────
buffer          │ ✓ 设置       │ ✓ 读取+decode    │ -               │ ✓ null 清理
entry           │ -            │ ✓ 非DML时设置     │ ✓ DML时设置     │ ✓ 读取+null
needDmlParse    │ -            │ ✓ 设置(true/false)│ ✓ 读取          │ -
table           │ -            │ ✓ DML时预查设置   │ ✓ 读取          │ ✓ null 清理
event           │ -            │ ✓ 设置            │ ✓ 读取          │ ✓ null 清理
needIterate     │ -            │ ✓ 设置            │ ✓ 读取          │ -
iterateEvents   │ -            │ -                 │ ✓ 设置          │ ✓ 读取+null
iterateTables   │ -            │ -                 │ ✓ 设置          │ ✓ null 清理
iterateEntrys   │ -            │ -                 │ ✓ 设置          │ ✓ 读取+null
```

> **这一步在干什么？** MessageEvent 是 Disruptor RingBuffer 中每个 slot 的数据载体，它的 9 个字段在不同阶段被不同线程读写。设计上遵循 Disruptor 的"预分配 + 字段复用"模式——对象在 RingBuffer 初始化时就创建好，后续只修改字段值，不产生新对象分配。Stage 4 处理完后必须把所有引用字段设为 null，否则 RingBuffer slot 会一直持有已过时的大对象（比如 LogBuffer），导致内存泄漏。`needDmlParse` 是 Stage 2 和 Stage 3 之间的协调信号——Stage 2 把简单事件直接解析完（entry 字段填好，needDmlParse=false），复杂 DML 事件只做轻量预处理（查好 TableMeta、needDmlParse=true），留给 Stage 3 的多个 Worker 并行完成重活。

**Step 4：Stage 1 publish() —— 背压控制与下游故障检测**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`

```java
public boolean publish(LogBuffer buffer) {
    // 1. 先检查下游是否已经出错
    if (exception != null) {
        // volatile 字段，下游任何阶段抛异常都会设置它
        throw exception;
        // → 快速失败，不再继续 publish
    }

    boolean interupted = false;
    long blockingStart = 0L;
    int fullTimes = 0;

    // 2. 自旋获取 RingBuffer 的下一个 slot
    do {
        try {
            long next = disruptorMsgBuffer.tryNext();
            // tryNext() 非阻塞：如果 RingBuffer 满了，抛 InsufficientCapacityException

            // 拿到 slot，填入 buffer
            MessageEvent data = disruptorMsgBuffer.get(next);
            data.setBuffer(buffer);
            // 只设置 buffer，其他字段由后续 Stage 填写

            disruptorMsgBuffer.publish(next);
            // 发布！Stage 2 的 SequenceBarrier 会被唤醒
            break; // 成功，退出循环

        } catch (InsufficientCapacityException e) {
            // RingBuffer 满了！执行背压策略
            if (blockingStart == 0L) {
                blockingStart = System.nanoTime();
            }

            // 前 3 次：Thread.yield()，让出 CPU 时间片
            // 之后：LockSupport.parkNanos()，精确等待
            fullTimes++;
            if (fullTimes <= 3) {
                Thread.yield();
            } else {
                // parkNanos 的等待时间 = 100us × min(fullTimes, 10)
                // 即最大等待 1ms = 100us × 10
                LockSupport.parkNanos(
                    100L * 1000L * Math.min(fullTimes, 10));
            }

            // 再次检查下游是否出错
            if (exception != null) {
                throw exception;
            }
        }
    } while (true);

    return interupted;
}
```

> **这一步在干什么？** Stage 1 是 dump 线程向 Disruptor RingBuffer 投递原始 binlog buffer 的入口。它实现了精细的背压（back-pressure）策略：使用 `tryNext()` 非阻塞尝试获取 slot，如果 RingBuffer 满了（说明下游消费跟不上），不是直接阻塞，而是采用递进等待——先 yield 3 次（给其他线程执行机会），再 parkNanos 逐步增大等待时间（100us → 200us → ... → 1ms 封顶）。`fullTimes` 上限 10 防止等待时间无限增长。每次等待前后都会检查 `volatile exception` 字段——如果下游任何阶段抛了异常，publish 立即抛出同一个异常，让 dump 线程的主循环捕获并处理。这种设计确保了：1）生产者不会无限等待导致 dump 线程假死；2）下游故障能快速传播到生产者。

**Step 5：Stage 2 SimpleParserStage.onEvent() —— 轻量分发与事件类型路由**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`（内部类）

```java
public class SimpleParserStage implements EventHandler<MessageEvent> {

    // LogDecoder：将原始字节 decode 为 LogEvent 对象
    private LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);

    @Override
    public void onEvent(MessageEvent event, long sequence, boolean endOfBatch)
            throws Exception {
        // 1. 将原始 buffer decode 为 LogEvent
        LogEvent logEvent = decoder.decode(event.getBuffer(), logContext);
        event.setEvent(logEvent);

        // 2. 根据 LogEvent 类型做不同处理
        int eventType = logEvent.getHeader().getType();

        if (logEvent instanceof RowsLogEvent) {
            // ===== DML 事件（INSERT/UPDATE/DELETE）=====
            // 轻量处理：只查 TableMeta，不做完整解析
            TableMeta tableMeta = logEventConvert.parseRowsEventForTableMeta(
                (RowsLogEvent) logEvent);
            event.setTable(tableMeta);
            // 标记需要 Stage 3 继续解析
            event.setNeedDmlParse(true);
            // 此时 entry 字段为 null，Stage 3 负责填写

        } else if (logEvent instanceof QueryLogEvent) {
            // ===== DDL 事件（ALTER/CREATE/DROP 等）=====
            // 完整解析：DDL 不频繁，直接在 Stage 2 处理完
            CanalEntry.Entry entry = logEventConvert.parse(logEvent, false);
            event.setEntry(entry);
            event.setNeedDmlParse(false);
            // Stage 3 看到 needDmlParse=false 会直接跳过

        } else if (logEvent instanceof XidLogEvent
                || logEvent instanceof GtidLogEvent) {
            // ===== 事务边界事件 =====
            // XID = 事务提交，GTID = 事务开始的全局事务 ID
            CanalEntry.Entry entry = logEventConvert.parse(logEvent, false);
            event.setEntry(entry);
            event.setNeedDmlParse(false);

        } else if (logEvent instanceof HeartbeatLogEvent) {
            // ===== MySQL 心跳事件 =====
            // 忽略，不生成 Entry
            event.setEntry(null);
            event.setNeedDmlParse(false);

        } else if (eventType == LogEvent.TRANSACTION_PAYLOAD_EVENT) {
            // ===== MySQL 8.0.20+ 压缩事务载荷 =====
            // 需要先解压，然后可能产生多个子事件
            // 解压后设置 iterateEvents 列表
            event.setNeedDmlParse(true);
            event.setNeedIterate(true);
            // → Stage 3 遍历 iterateEvents 逐个解析

        } else if (logEvent instanceof RotateLogEvent) {
            // ===== binlog 文件切换事件 =====
            // parse 返回 null，但要更新 binlog 文件名
            CanalEntry.Entry entry = logEventConvert.parse(logEvent, false);
            // entry 通常为 null
            event.setEntry(entry);
            event.setNeedDmlParse(false);

        } else {
            // 其他事件类型：FormatDescriptionEvent 等
            CanalEntry.Entry entry = logEventConvert.parse(logEvent, false);
            event.setEntry(entry);
            event.setNeedDmlParse(false);
        }
    }
}
```

> **这一步在干什么？** Stage 2 是单线程的"分诊台"，职责是把不同类型的 binlog 事件路由到正确的处理方式。核心设计决策是：**DML 事件（RowsLogEvent）只做轻量的 TableMeta 查询，把耗时的完整解析留给 Stage 3 的多线程 WorkerPool**。非 DML 事件（DDL、事务边界、心跳等）因为数量少、解析快，直接在 Stage 2 完成。对于 MySQL 8.0.20+ 的 `TRANSACTION_PAYLOAD_EVENT`（压缩事务），Stage 2 负责解压并设置 `needIterate=true`，让 Stage 3 遍历内部的多个子事件。RotateLogEvent 代表 binlog 文件切换，parse 结果通常为 null，但要确保后续 Stage 不会因为 null 出错。

**Step 6：Stage 3 DmlParserStage Worker 分配 —— WorkerPool CAS 竞争与乱序处理**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`（内部类）

```java
public class DmlParserStage implements WorkHandler<MessageEvent> {

    @Override
    public void onEvent(MessageEvent event) throws Exception {
        // WorkerPool 调度：每个 Worker 通过 CAS 竞争 workSequence 领取任务

        if (event.isNeedDmlParse()) {
            // ===== 需要完整 DML 解析 =====

            if (event.isNeedIterate()) {
                // 压缩事务载荷：多个子事件
                List<CanalEntry.Entry> entries = new ArrayList<>();
                for (int i = 0; i < event.getIterateEvents().size(); i++) {
                    LogEvent subEvent = event.getIterateEvents().get(i);
                    TableMeta subTable = event.getIterateTables().get(i);
                    // 完整解析每个子事件
                    CanalEntry.Entry subEntry = logEventConvert.parseRowsEvent(
                        (RowsLogEvent) subEvent, subTable);
                    if (subEntry != null) {
                        entries.add(subEntry);
                    }
                }
                event.setIterateEntrys(entries);
            } else {
                // 普通 DML 事件：一个 RowsLogEvent 生成一个 Entry
                CanalEntry.Entry entry = logEventConvert.parseRowsEvent(
                    (RowsLogEvent) event.getEvent(), event.getTable());
                // parseRowsEvent() 是最耗时的操作：
                //   1. 按 TableMeta 的列定义逐字段解析
                //   2. 字段类型转换（MySQL类型 → Java类型）
                //   3. 构建 RowData（before/after image）
                //   4. 构建 RowChange → Entry
                event.setEntry(entry);
            }

        }
        // needDmlParse=false：Stage 2 已经解析完，直接跳过
    }
}
```

WorkerPool 内部的任务分配机制：

```java
// Disruptor WorkerPool 核心逻辑（简化）
// 每个 Worker 线程的 run() 方法：
public void run() {
    while (running) {
        long nextSequence;
        // CAS 竞争 workSequence
        // 多个 Worker 同时尝试 CAS(current, current+1)
        // 只有一个 Worker 能抢到 nextSequence
        do {
            nextSequence = workSequence.get() + 1;
        } while (!workSequence.compareAndSet(nextSequence - 1, nextSequence));

        // 等待该 sequence 可用（Stage 2 已处理完）
        barrier.waitFor(nextSequence);

        // 处理事件
        MessageEvent event = ringBuffer.get(nextSequence);
        workHandler.onEvent(event); // → DmlParserStage.onEvent()
    }
}
```

> **这一步在干什么？** Stage 3 使用 Disruptor 的 WorkerPool 模式实现多线程并行解析。多个 DmlParserStage Worker 通过 CAS 竞争一个共享的 `workSequence` 来领取任务——这意味着任务分配是无锁的，没有队列竞争。但 CAS 竞争也意味着 **Worker 处理事件的顺序可能与 RingBuffer 中的原始顺序不同**：Worker A 可能先抢到 sequence=100 但处理慢，Worker B 后抢到 sequence=101 但处理快。这就是为什么需要 Stage 4——Stage 4 的 SequenceBarrier 会确保必须等所有 Worker 都完成后，才按照原始 sequence 顺序消费，从而恢复有序性。`parseRowsEvent()` 是整个流水线中最耗时的操作，涉及字段级的逐一解析、类型转换和 Protobuf Entry 构建，这正是需要多线程加速的瓶颈点。

**Step 7：Stage 4 SinkStoreStage —— 顺序投递、半同步 ACK 与 GC 清理**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`（内部类）

```java
public class SinkStoreStage implements EventHandler<MessageEvent> {

    @Override
    public void onEvent(MessageEvent event, long sequence, boolean endOfBatch)
            throws Exception {
        try {
            if (event.getEntry() != null) {
                // 1. 顺序投递到 EventTransactionBuffer
                transactionBuffer.add(event.getEntry());
                // → EventTransactionBuffer 缓存事务内的所有 Entry
                // → 遇到事务结束事件时 flush → EventSink.sink()
            }

            // 处理迭代型事件（压缩事务载荷解压后的多个 Entry）
            if (event.isNeedIterate() && event.getIterateEntrys() != null) {
                for (CanalEntry.Entry iterateEntry : event.getIterateEntrys()) {
                    transactionBuffer.add(iterateEntry);
                }
            }

            // 2. 半同步 ACK（semi-sync replication 支持）
            // 如果 MySQL 配置了 semi-sync，Canal 作为 slave 需要在收到 event 后回 ACK
            if (event.needSemiSyncAck()) {
                // 通过 MySQL 连接发送 SemiSyncAckPacket
                connection.semiSyncAck(event.getSemiSyncPosition());
            }

        } finally {
            // 3. GC 清理：null 掉所有字段，释放引用
            // 这是必须的！RingBuffer slot 不会被释放，只会被复用
            // 如果不 null，旧的 LogBuffer / LogEvent 会一直被 slot 持有
            event.setBuffer(null);
            event.setEntry(null);
            event.setTable(null);
            event.setEvent(null);
            event.setNeedDmlParse(false);
            event.setNeedIterate(false);
            event.setIterateEvents(null);
            event.setIterateTables(null);
            event.setIterateEntrys(null);
        }
    }
}
```

> **这一步在干什么？** Stage 4 是流水线的终点，单线程执行，保证了投递到 `EventTransactionBuffer` 的事件顺序与 binlog 原始顺序完全一致——即使 Stage 3 的多个 Worker 是乱序完成的。Disruptor 的 `BatchEventProcessor` 严格按 sequence 递增顺序调用 `onEvent()`，所以 Stage 4 天然就是有序的。`finally` 块中对所有 9 个字段的 null 清理是关键的内存管理操作——RingBuffer 的 slot 对象（MessageEvent）永远不会被 GC 回收，如果不清理字段引用，每个 slot 会持有上一轮的 LogBuffer（可能数十 KB）和 LogEvent 直到被覆盖，在 bufferSize=256 的情况下可能浪费数 MB 内存。

**Step 8：错误处理哲学 —— Fail-Fast 设计与 volatile 异常传播**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlMultiStageCoprocessor.java`

```java
// ===== 各 Stage 的异常处理 =====
// 以 SimpleParserStage 为例（Stage 2/3/4 相同模式）
public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) {
    try {
        // ... 正常处理逻辑 ...
    } catch (Throwable e) {
        // catch Throwable（不是 Exception），连 OOM 也要捕获
        exception = e;
        // exception 是 MysqlMultiStageCoprocessor 的 volatile 字段
        // 赋值后，Stage 1 的 publish() 在下一次循环检查时会立刻看到
        throw new RuntimeException(e);
        // 重新抛出，让 Disruptor 的 ExceptionHandler 处理
    }
}

// ===== SimpleFatalExceptionHandler =====
// Disruptor 的全局异常处理器
public class SimpleFatalExceptionHandler implements ExceptionHandler<Object> {
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
        // 不吞异常、不跳过、不重试
        // 直接重新抛出 → 消费者线程退出
        throw new RuntimeException(ex);
    }
    // 这是 Bug #968 的修复：
    // 旧版本使用 FatalExceptionHandler，它会调用 System.exit()
    // 导致整个 JVM 退出而不是仅停止这个 Instance
    // SimpleFatalExceptionHandler 只终止当前消费者线程
}

// ===== volatile exception 字段 =====
private volatile Throwable exception;
// 写入：任何 Stage 的 catch(Throwable) 块
// 读取：Stage 1 的 publish() 方法，每次循环开头和背压等待中
// 效果：下游故障 → volatile 写入 → 上游快速感知 → 整个流水线停止
```

> **这一步在干什么？** Canal 的并行流水线采用 fail-fast（快速失败）策略，不做任何异常跳过或重试。设计哲学是：binlog 是有序的、不可跳过的流，一旦某条事件解析失败，后续所有事件都不应该被处理（否则会导致数据不一致）。异常传播路径是：下游 Stage 捕获 Throwable → 设置 volatile exception 字段 → Stage 1 的 publish() 在下一次循环检查到 → 抛出异常 → dump 线程主循环捕获 → 断开 MySQL 连接 → 等待重连重试。`SimpleFatalExceptionHandler` 是对 Bug #968 的修复——旧版本用的 `FatalExceptionHandler` 会直接 `System.exit()` 杀掉整个 JVM，影响同一进程中的其他 Instance。修复后只终止出错的消费者线程，由上层捕获并做 Instance 级别的重启。

**四阶段流水线示意图**

```
 Dump线程(Stage1)      SimpleParserStage(Stage2)     DmlParserStage×N(Stage3)     SinkStoreStage(Stage4)
     │                       │                              │                            │
     │   publish(buffer)     │                              │                            │
     │──────────────────────→│                              │                            │
     │   tryNext()+publish() │  onEvent():                  │                            │
     │                       │  decode + 类型路由            │                            │
     │                       │  DML → needDmlParse=true     │                            │
     │                       │  DDL → entry=完整, skip S3   │                            │
     │                       │──────────────────────────────→│                            │
     │                       │        barrier2               │  onEvent():               │
     │                       │                               │  CAS 抢 workSequence      │
     │                       │                               │  parseRowsEvent()          │
     │                       │                               │  (并行，乱序完成)          │
     │                       │                               │──────────────────────────→│
     │                       │                               │        barrier3            │
     │                       │                               │                            │ onEvent():
     │                       │                               │                            │ 按 sequence 顺序
     │                       │                               │                            │ → transactionBuffer
     │                       │                               │                            │ → semiSync ACK
     │                       │                               │                            │ → null 所有字段(GC)
     │                       │                               │                            │
  ←──────── volatile exception 传播 ─── 任何 Stage 出错 ──────────────────────────────────│
```

**串行 vs 并行性能对比**

| 对比项 | 串行模式 (`parallel=false`) | 并行模式 (`parallel=true`) |
|--------|---------------------------|---------------------------|
| 线程数 | 1（dump 线程） | 1 + 1 + N + 1 = N+3（N=CPU×60%） |
| 瓶颈位置 | dump 线程做全部工作 | Stage 3 DML 解析（多线程分摊） |
| 事件顺序 | 天然有序 | Stage 3 乱序，Stage 4 恢复有序 |
| 内存开销 | 无额外开销 | RingBuffer × MessageEvent 预分配 |
| CPU 利用率 | 单核 | 多核并行（Stage 3） |
| 适用场景 | 低 TPS（< 1000 event/s） | 高 TPS（> 5000 event/s） |
| 背压策略 | N/A（同步） | yield→parkNanos 递进等待 |
| 故障传播 | 直接异常 | volatile + fail-fast |

---

## 案例十一：RocketMQ 模式 —— 变更数据推送到 RocketMQ

### 场景描述

Canal Server 将 MySQL 变更数据推送到 RocketMQ，下游消费者（比如数据同步服务、搜索索引更新服务）从 RocketMQ Topic 消费。与 Kafka 模式类似，但 RocketMQ 在美团、阿里等公司有更广泛的内部使用基础。RocketMQ 模式支持 ACL 权限认证、Tag 消息标签、动态队列数查询等 Kafka 模式没有的特性。

### 配置

```properties
# canal.properties
canal.serverMode = rocketMQ
canal.mq.servers = namesrv1:9876;namesrv2:9876
canal.mq.producerGroup = canal-producer-group
canal.mq.accessChannel = local
# ACL 认证（可选）
canal.mq.accessKey = yourAccessKey
canal.mq.secretKey = yourSecretKey
canal.mq.vipChannelEnabled = false

# instance.properties
canal.mq.topic = canal-topic
canal.mq.partition = 0
canal.mq.partitionsNum = 4
canal.mq.flatMessage = true
```

### 全链路源码追踪

**Step 1：SPI 加载 —— ExtensionLoader 发现 CanalRocketMQProducer**

**源码位置**: `deployer/src/main/java/com/alibaba/otter/canal/deployer/CanalStarter.java`

```java
public synchronized void start() throws Throwable {
    String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
    // serverMode = "rocketMQ"

    if (!"tcp".equalsIgnoreCase(serverMode)) {
        // 走 MQ 分支
        ExtensionLoader<CanalMQProducer> loader =
            ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
        // ExtensionLoader 扫描 META-INF/canal/ 目录下的 SPI 配置文件
        // 文件名 = 接口全限定名：com.alibaba.otter.canal.spi.CanalMQProducer
        // 文件内容（每行 key=实现类）：
        //   kafka=com.alibaba.otter.canal.kafka.CanalKafkaProducer
        //   rocketmq=com.alibaba.otter.canal.rocketmq.CanalRocketMQProducer
        //   rabbitmq=com.alibaba.otter.canal.rabbitmq.CanalRabbitMQProducer
        //   pulsarmq=com.alibaba.otter.canal.pulsarmq.CanalPulsarMQProducer

        canalMQProducer = loader.getExtension(serverMode.toLowerCase());
        // serverMode.toLowerCase() = "rocketmq"
        // → 返回 CanalRocketMQProducer 实例
    }
}
```

> **这一步在干什么？** Canal 使用自定义的 `ExtensionLoader`（类似 Dubbo SPI）实现 MQ Producer 的可插拔加载。SPI 配置文件中用 key-value 对映射：`rocketmq=CanalRocketMQProducer`。这与 Kafka 模式的加载机制完全相同，只是 key 不同。SPI 机制使得新增 MQ 类型只需要实现 `CanalMQProducer` 接口并添加一行 SPI 配置，不需要修改 Canal 的核心代码。

**Step 2：CanalRocketMQProducer.init() —— DefaultMQProducer 创建与配置**

**源码位置**: `connector/rocketmq-connector/src/main/java/com/alibaba/otter/canal/connector/rocketmq/producer/CanalRocketMQProducer.java`

```java
@Override
public void init(Properties properties) {
    // 1. 解析 MQ 配置
    RocketMQProducerConfig rocketMQProperties = new RocketMQProducerConfig();
    this.mqProperties = rocketMQProperties;
    loadRocketMQProperties(properties, rocketMQProperties);
    // namesrvAddr = "namesrv1:9876;namesrv2:9876"
    // producerGroup = "canal-producer-group"
    // accessKey / secretKey（如果配置了 ACL）

    // 2. 创建 DefaultMQProducer
    RPCHook rpcHook = null;
    if (StringUtils.isNotEmpty(rocketMQProperties.getAccessKey())
            && StringUtils.isNotEmpty(rocketMQProperties.getSecretKey())) {
        // ===== 有 ACL 认证 =====
        // 创建 AclClientRPCHook
        SessionCredentials credentials = new SessionCredentials(
            rocketMQProperties.getAccessKey(),
            rocketMQProperties.getSecretKey());
        rpcHook = new AclClientRPCHook(credentials);
    }

    this.defaultMQProducer = new DefaultMQProducer(
        rocketMQProperties.getProducerGroup(),
        rpcHook,  // null 或 AclClientRPCHook
        rocketMQProperties.isEnableMsgTrace(),
        rocketMQProperties.getCustomizedTraceTopic());

    // 3. 设置 NameServer 地址
    defaultMQProducer.setNamesrvAddr(rocketMQProperties.getNamesrvAddr());

    // 4. 禁用 VIP 通道（避免端口偏移问题）
    defaultMQProducer.setVipChannelEnabled(
        rocketMQProperties.isVipChannelEnabled()); // false
    // VipChannel 会在 NameServer 端口上减 2（9876→9874），
    // 在某些网络环境下不可用

    // 5. 设置重试次数
    defaultMQProducer.setRetryTimesWhenSendFailed(
        rocketMQProperties.getRetryTimesWhenSendFailed()); // 默认 0

    // 6. 启动 Producer
    defaultMQProducer.start();
}
```

> **这一步在干什么？** 这一步创建并启动 RocketMQ 的 `DefaultMQProducer`。关键配置有三个：1）ACL 认证——如果配置了 accessKey/secretKey，构造 `AclClientRPCHook` 注入到 Producer 中（详见 Step 3）。2）VipChannel 禁用——RocketMQ 的 VIP 通道会使用端口-2 的策略（9876→9874），在 Docker 或云环境下这个端口可能不通，所以默认禁用。3）重试次数设为 0——Canal 选择在上层自己控制重试策略（整批 rollback 重发），而不是让 RocketMQ Client 自动重试。

**Step 3：ACL 认证 —— AclClientRPCHook 的 HMAC-SHA1 签名注入**

**源码位置**: `org.apache.rocketmq.acl.common.AclClientRPCHook`（RocketMQ Client 内部类）

```java
public class AclClientRPCHook implements RPCHook {

    private SessionCredentials sessionCredentials;

    public AclClientRPCHook(SessionCredentials sessionCredentials) {
        this.sessionCredentials = sessionCredentials;
        // sessionCredentials 包含 accessKey、secretKey、securityToken
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        // 每次发送 RPC 请求前调用

        // 1. 注入 AccessKey 到请求头
        HashMap<String, String> extFields = request.getExtFields();
        extFields.put(SessionCredentials.ACCESS_KEY,
            sessionCredentials.getAccessKey());

        // 2. 如果有 SecurityToken（STS 临时凭证），也注入
        if (StringUtils.isNotEmpty(sessionCredentials.getSecurityToken())) {
            extFields.put(SessionCredentials.SECURITY_TOKEN,
                sessionCredentials.getSecurityToken());
        }

        // 3. 计算 HMAC-SHA1 签名
        // 签名内容 = 请求体的部分字段拼接
        byte[] content = AclUtils.combineRequestContent(request,
            SortedMap(extFields));
        String signature = AclUtils.calSignature(
            content, sessionCredentials.getSecretKey());
        // calSignature() 内部：
        //   Mac mac = Mac.getInstance("HmacSHA1");
        //   mac.init(new SecretKeySpec(secretKey.getBytes(), "HmacSHA1"));
        //   byte[] hash = mac.doFinal(content);
        //   return Base64.encodeBase64String(hash);

        // 4. 注入签名到请求头
        extFields.put(SessionCredentials.SIGNATURE, signature);

        request.setExtFields(extFields);
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
            RemotingCommand response) {
        // 响应后钩子，ACL 场景不需要处理
    }
}
```

> **这一步在干什么？** ACL 认证的核心是 RPC 钩子（RPCHook）机制。`AclClientRPCHook` 在每次 Producer 向 Broker 发送请求（发消息、查询路由等）之前，自动在请求头中注入三个字段：AccessKey（明文身份标识）、SecurityToken（可选的临时凭证）、Signature（用 SecretKey 对请求内容做 HMAC-SHA1 签名后 Base64 编码的结果）。Broker 端收到请求后，用同样的 SecretKey 重新计算签名并比对——密钥本身不在网络上传输。这与 Kafka 的 Kerberos/SASL 认证体系完全不同：Kafka 需要 JAAS 配置文件和票据，RocketMQ 只需要两个字符串配置。

**Step 4：send() —— 非 Flat 模式 vs Flat 模式的完整发送路径**

**源码位置**: `connector/rocketmq-connector/src/main/java/com/alibaba/otter/canal/connector/rocketmq/producer/CanalRocketMQProducer.java`

```java
@Override
public void send(MQDestination canalDestination, com.alibaba.otter.canal.protocol.Message message,
                 Callback callback) {
    try {
        if (!mqProperties.isFlatMessage()) {
            // ===== 非 Flat 模式 =====
            // 整个 Message 序列化为一个字节 blob
            // 按 partition 分组后，每个 partition 发一条 RocketMQ 消息
            List<List<com.alibaba.otter.canal.protocol.Message>> partitionMessages =
                MQMessageUtils.messagePartition(message,
                    canalDestination.getPartition(),
                    canalDestination.getPartitionsNum());

            int length = partitionMessages.size();
            for (int i = 0; i < length; i++) {
                List<com.alibaba.otter.canal.protocol.Message> msgs = partitionMessages.get(i);
                if (msgs == null || msgs.isEmpty()) continue;

                // 序列化
                byte[] body = CanalMessageSerializerUtil.serializer(msgs,
                    mqProperties.isFilterTransactionEntry());

                // 构造 RocketMQ Message
                org.apache.rocketmq.common.message.Message rocketMsg =
                    new org.apache.rocketmq.common.message.Message(
                        canalDestination.getTopic(),  // topic
                        canalDestination.getMqTag(),   // tag（Kafka 没有此概念）
                        body);

                // 同步发送到指定队列
                SendResult result = this.defaultMQProducer.send(rocketMsg,
                    new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs,
                                org.apache.rocketmq.common.message.Message msg, Object arg) {
                            // partition → queue 映射
                            int targetPartition = (int) arg;
                            return mqs.get(targetPartition % mqs.size());
                        }
                    }, i); // i 就是 partition 编号
            }

        } else {
            // ===== Flat 模式 =====
            // 每个 Entry 转换为一个 FlatMessage（JSON 格式）
            // 按 partition 分组后，批量发送

            // 1. 将 CanalEntry 列表转换为 FlatMessage 列表
            List<FlatMessage> flatMessages =
                MQMessageUtils.messageConverter(message, canalDestination);

            // 2. 按 partition 分组
            // Map<partition编号, List<RocketMQ Message>>
            Map<Integer, List<org.apache.rocketmq.common.message.Message>> messageMap =
                new HashMap<>();

            for (FlatMessage flatMessage : flatMessages) {
                int partition = cyclePartition(flatMessage, canalDestination);
                org.apache.rocketmq.common.message.Message rocketMsg =
                    new org.apache.rocketmq.common.message.Message(
                        canalDestination.getTopic(),
                        canalDestination.getMqTag(),
                        JSON.toJSONString(flatMessage).getBytes(StandardCharsets.UTF_8));

                messageMap.computeIfAbsent(partition, k -> new ArrayList<>())
                    .add(rocketMsg);
            }

            // 3. 每个 partition 的消息列表批量发送
            for (Map.Entry<Integer, List<org.apache.rocketmq.common.message.Message>> entry
                    : messageMap.entrySet()) {
                int partition = entry.getKey();
                List<org.apache.rocketmq.common.message.Message> msgs = entry.getValue();

                // 计算目标 MessageQueue
                TopicPublishInfo topicPublishInfo = getTopicPublishInfo(
                    canalDestination.getTopic());
                List<MessageQueue> mqs = topicPublishInfo.getMessageQueueList();
                MessageQueue targetQueue = mqs.get(partition % mqs.size());

                // 批量发送！绕过 MessageQueueSelector
                // send(List<Message>, MessageQueue) 直接指定队列
                SendResult result = this.defaultMQProducer.send(msgs, targetQueue);
                // 批量发送比逐条发送减少网络往返
            }
        }

        // 全部发送成功
        callback.commit();

    } catch (Exception e) {
        // 发送失败
        callback.rollback();
        // → CanalMQStarter 收到 rollback 后会重新拉取同一批数据重发
        throw new RuntimeException(e);
    }
}
```

> **这一步在干什么？** send 方法的核心分支是 `flatMessage` 开关。非 Flat 模式把整个 Canal Message 序列化为一个字节 blob，对端需要用 Canal 的反序列化工具解码——数据紧凑但下游必须依赖 Canal Client 库。Flat 模式把每个 Entry 转换为 JSON 格式的 FlatMessage——任何语言都能解析，但体积更大。Flat 模式的一个重要优化是**批量发送**：同一个 partition 的多条 FlatMessage 收集到一个 List 中，调用 `send(List<Message>, MessageQueue)` 一次性发送，减少了网络往返次数。这个方法直接指定 `MessageQueue` 而不使用 `MessageQueueSelector`，因为批量发送 API 不支持 Selector 回调。

**Step 5：动态队列数查询 —— 三级 partitionsNum 解析策略**

**源码位置**: `connector/rocketmq-connector/src/main/java/com/alibaba/otter/canal/connector/rocketmq/producer/CanalRocketMQProducer.java`

```java
private int getTopicDynamicQueuesSize(String topicName, int defaultPartitions) {
    // 三级解析策略：
    // Level 1: 配置文件中 per-topic 的静态配置
    //   canal.mq.partitionsNum = 4
    //   如果 partitionsNum > 0，直接使用，不查询 NameServer

    // Level 2: 从 NameServer 动态查询
    if (defaultPartitions <= 0) {
        try {
            // 深入 DefaultMQProducerImpl 内部获取路由信息
            DefaultMQProducerImpl producerImpl =
                this.defaultMQProducer.getDefaultMQProducerImpl();
            TopicPublishInfo topicPublishInfo =
                producerImpl.getTopicPublishInfoTable().get(topicName);

            if (topicPublishInfo == null) {
                // 本地缓存没有，主动从 NameServer 拉取
                producerImpl.getmQClientFactory()
                    .updateTopicRouteInfoFromNameServer(topicName);
                topicPublishInfo =
                    producerImpl.getTopicPublishInfoTable().get(topicName);
            }

            if (topicPublishInfo != null
                    && topicPublishInfo.getMessageQueueList() != null) {
                return topicPublishInfo.getMessageQueueList().size();
                // 返回该 Topic 在 Broker 上实际的写队列数
            }
        } catch (Exception e) {
            logger.error("get topic queue size error", e);
        }
    }

    // Level 3: 全局 fallback
    return defaultPartitions > 0 ? defaultPartitions : 1;
    // 如果 NameServer 也查不到，使用全局默认值（1 个队列）
}
```

> **这一步在干什么？** RocketMQ 的队列数（queue number）等价于 Kafka 的分区数（partition number），决定了下游消费的并行度。Canal 采用三级策略确定队列数：1）如果用户在配置文件中显式指定了 `partitionsNum`（> 0），直接使用，不查询 NameServer——适用于固定分区的场景。2）如果没有显式指定，则深入 `DefaultMQProducerImpl` 的内部（`getTopicPublishInfoTable()`）获取路由信息，如果本地缓存为空则主动调用 `updateTopicRouteInfoFromNameServer()` 从 NameServer 拉取——这反映了 Topic 在 Broker 上的实际队列配置。3）如果 NameServer 也查不到（Topic 尚未创建或网络问题），回退到默认值 1。注意这里直接操作了 `DefaultMQProducerImpl` 的内部 API，是一种"破窗"式的实现——如果 RocketMQ Client 升级内部实现可能会出问题。

**Step 6：MessageQueueSelector —— 内联 Lambda 的分区映射**

**源码位置**: `connector/rocketmq-connector/src/main/java/com/alibaba/otter/canal/connector/rocketmq/producer/CanalRocketMQProducer.java`

```java
// 非 Flat 模式的发送调用
SendResult result = this.defaultMQProducer.send(
    rocketMsg,
    // MessageQueueSelector：内联匿名实现
    (mqs, msg, arg) -> {
        // mqs = Broker 上该 Topic 的所有 MessageQueue 列表
        // msg = 当前发送的消息
        // arg = 用户传入的参数（这里是 partition 编号 i）
        int partition = (int) arg;
        // 取模映射：partition 编号 → MessageQueue 下标
        return mqs.get(partition % mqs.size());
        // 例如：partition=5, mqs.size()=4 → queue[1]
    },
    i   // partition 编号作为 arg 传入
);
```

> **这一步在干什么？** `MessageQueueSelector` 是 RocketMQ 提供的有序消息选择器接口。Canal 用一个简单的取模策略将逻辑 partition 编号映射到物理 MessageQueue。这保证了同一个 partition 的消息始终发送到同一个 MessageQueue——如果 partition 是按表名或主键哈希分配的，那么同一张表（或同一行）的变更事件会被顺序地发送到同一个队列，下游单个消费者就能按顺序消费。注意：如果 Broker 上的队列数发生变化（扩缩容），取模结果会变化，可能导致短暂的消息乱序。

**Step 7：重试策略 —— 零重试 + 同步发送 + 整批回滚**

**源码位置**: `connector/rocketmq-connector/src/main/java/com/alibaba/otter/canal/connector/rocketmq/producer/CanalRocketMQProducer.java`

```java
// 初始化时设置
defaultMQProducer.setRetryTimesWhenSendFailed(0);
// RocketMQ Client 的自动重试次数 = 0
// 即：一次发送失败就立刻抛异常，不让 RocketMQ Client 自己重试

// send() 方法中的异常处理
try {
    // ... 发送逻辑 ...
    callback.commit(); // 全部成功 → 提交
} catch (Exception e) {
    callback.rollback(); // 任何失败 → 回滚
    // rollback 的效果：
    //   CanalMQStarter 不推进消费位点
    //   下一次循环重新从 EventStore 拉取同一批数据
    //   重新调用 send() → 整批重发
    throw new RuntimeException(e);
}
// 注意：send() 是同步的（SendResult = producer.send(msg, selector, arg)）
// RocketMQ 也支持 async send，但 Canal 没有使用
// 同步发送确保 callback.commit()/rollback() 的语义正确
```

> **这一步在干什么？** Canal 的 RocketMQ 重试策略非常保守：RocketMQ Client 层面零重试（`retryTimesWhenSendFailed=0`），一旦发送失败立刻抛异常。异常被 send() 的 catch 块捕获后调用 `callback.rollback()`——这通知 `CanalMQStarter` 不要推进消费位点，下一轮循环会重新拉取同一批数据并整批重发。使用同步发送（而非异步）是为了保证发送结果确定性——异步发送时 callback 可能在另一个线程回调，而 Canal 的 commit/rollback 语义要求在同一个线程中确定批次状态。整体设计是"宁可重发，不可丢失"——幂等性由下游消费者保证。

**Step 8：Kafka 模式 vs RocketMQ 对比**

| 对比项 | Kafka 模式 | RocketMQ 模式 |
|--------|-----------|---------------|
| SPI Key | `kafka` | `rocketmq` |
| Producer 类 | `CanalKafkaProducer` | `CanalRocketMQProducer` |
| 底层 Client | `KafkaProducer` | `DefaultMQProducer` |
| 发送模式 | 异步（Future.get 可选同步） | 同步（send 直接返回 SendResult） |
| 批量发送 | 单条 send + 异步攒批（linger.ms） | `send(List<Message>, MessageQueue)` 显式批量 |
| 分区选择 | `KafkaMessageSerializer` 内部计算 | `MessageQueueSelector` 内联 lambda |
| 认证机制 | Kerberos/SASL（JAAS 配置文件） | ACL（AclClientRPCHook，HMAC-SHA1） |
| 消息标签 | 不支持 | 支持 Tag（`canal.mq.tag`） |
| 重试策略 | `retries=0`（Client 层） + 整批 rollback | `retryTimesWhenSendFailed=0` + 整批 rollback |
| 队列数获取 | Kafka Admin API / 配置文件 | NameServer 路由查询 / 配置文件 |
| 服务发现 | Bootstrap Servers 直连 | NameServer 路由发现 |
| 事务消息 | 不使用 | 不使用（虽然 RocketMQ 支持） |
| VIP 通道 | 无此概念 | `vipChannelEnabled=false`（端口偏移问题） |

**RocketMQ 模式数据流图**

```
  MySQL                Canal Server                                    RocketMQ
    |                      |                                              |
    |---binlog stream---→  MysqlEventParser                               |
    |                      → LogDecoder.decode()                          |
    |                      → LogEventConvert.parse()                      |
    |                      → EventTransactionBuffer                       |
    |                      → MemoryEventStoreWithBuffer                   |
    |                      |                                              |
    |                  CanalMQStarter.worker()                            |
    |                      → eventStore.get()                             |
    |                      → CanalRocketMQProducer.send()                 |
    |                      |                                              |
    |              flatMessage=false?                                     |
    |              ├─ YES: serialize → send(msg, selector, partition) ──→ Broker
    |              └─ NO:  FlatMessage→JSON → send(List, queue) ────────→ Broker
    |                      |                                              |
    |              成功 → callback.commit() → 推进位点                     |
    |              失败 → callback.rollback() → 整批重发                   |
```

---

## 案例十二：心跳检测与 MySQL 连接自愈 —— 两层保护机制

### 场景描述

Canal Server 长时间运行时，MySQL 连接可能因为网络抖动、MySQL 重启、防火墙超时等原因断开。Canal 实现了两层保护机制：第一层是主动心跳检测（定时发送 SQL 或合成心跳事件），检测到异常后触发 HA 切换或重连；第二层是 dump 线程的 catch-reconnect 循环，作为最后防线。

### 配置

```properties
# instance.properties
canal.instance.master.address = master-mysql:3306
canal.instance.standby.address = standby-mysql:3306

# 心跳配置
canal.instance.detecting.enable = true
canal.instance.detecting.sql = select 1
canal.instance.detecting.interval.time = 3
canal.instance.detecting.retry.threshold = 3

# HA 切换
canal.instance.standby.switch.enable = true
canal.instance.fallbackIntervalInSeconds = 60
```

### 全链路源码追踪

**Step 1：startHeartBeat() —— 心跳定时器启动与双模式分支**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/AbstractEventParser.java`

```java
protected void startHeartBeat() {
    // 获取心跳检测间隔（秒）
    long detectingIntervalInSeconds = this.detectingIntervalInSeconds;
    // 默认 3 秒

    // 构建心跳定时任务
    if (timer == null) {
        timer = new Timer("canal-heartbeat-" + destination, true);
        // daemon 线程，不阻止 JVM 退出
    }

    if (StringUtils.isNotEmpty(detectingSQL)) {
        // ===== 模式一：主动 SQL 探测 =====
        // detectingSQL = "select 1"
        // 用独立连接定时执行 SQL，检测 MySQL 可达性
        TimerTask task = buildHeartBeatTimeTask(
            new MysqlDetectingTimeTask(connection.fork()));
        // connection.fork() 创建一个独立的 MySQL 连接（详见 Step 2）
        timer.schedule(task, detectingIntervalInSeconds * 1000L,
            detectingIntervalInSeconds * 1000L);
        // 每 3 秒执行一次

    } else {
        // ===== 模式二：被动心跳合成 =====
        // 不发送 SQL，而是定期检查 binlog dump 是否有数据流入
        // 如果超时没有收到任何事件，合成一个 HEARTBEAT 类型的 Entry
        TimerTask task = buildHeartBeatTimeTask(null);
        // detectingTimeTask = null → 走被动模式
        timer.schedule(task, detectingIntervalInSeconds * 1000L,
            detectingIntervalInSeconds * 1000L);
    }
}

// buildHeartBeatTimeTask —— 心跳任务封装
protected TimerTask buildHeartBeatTimeTask(
        MysqlDetectingTimeTask detectingTimeTask) {
    return new TimerTask() {
        @Override
        public void run() {
            try {
                if (detectingTimeTask != null) {
                    // 主动模式：执行 SQL 探测
                    detectingTimeTask.run();
                } else {
                    // 被动模式：检查最后一次收到事件的时间
                    long lastEntryTime = AbstractEventParser.this.lastEntryTime;
                    long now = System.currentTimeMillis();
                    long gap = now - lastEntryTime;

                    if (gap > detectingIntervalInSeconds * 1000L) {
                        // 超过检测间隔没有收到事件
                        // 合成一个 HEARTBEAT Entry 投递到下游
                        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                            .setEventType(CanalEntry.EventType.HEARTBEAT)
                            .setExecuteTime(now)
                            .build();
                        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                            .setHeader(header)
                            .setEntryType(CanalEntry.EntryType.HEARTBEAT)
                            .build();
                        // 投递到 transactionBuffer
                        consumeTheEventAndProfilingIfNecessary(entry);
                    }
                }
            } catch (Throwable e) {
                logger.warn("heartbeat run failed", e);
            }
        }
    };
}
```

> **这一步在干什么？** Canal 的心跳检测有两种模式：1）主动 SQL 探测模式——配置了 `detectingSQL`（如 `select 1`）时，Canal 用一个独立的 MySQL 连接定时执行这个 SQL，通过连接是否可用来判断 MySQL 是否存活。2）被动心跳合成模式——不发送 SQL，而是检查 dump 线程最后收到 binlog 事件的时间 `lastEntryTime`，如果超过 `detectingIntervalInSeconds` 没有新事件，就合成一个 `EntryType.HEARTBEAT` 类型的虚拟事件投递到下游。被动模式的合成心跳会流经整个 Sink→Store 链路，下游客户端的 `HeartBeatEntryEventHandler` 会在消费前把它过滤掉。

**Step 2：connection.fork() —— 独立心跳连接的创建**

**源码位置**: `dbsync/src/main/java/com/alibaba/otter/canal/parse/driver/mysql/MysqlConnection.java`

```java
public MysqlConnection fork() {
    MysqlConnection forked = new MysqlConnection();
    // ===== 复制的属性（配置层面）=====
    forked.setCharset(this.charset);           // 字符集
    forked.setSlaveId(this.slaveId);           // slave ID（心跳连接可用相同 ID）
    forked.setAddress(this.address);           // MySQL 地址
    forked.setUsername(this.username);         // 认证用户名
    forked.setPassword(this.password);         // 认证密码
    forked.setDefaultDatabaseName(this.defaultDatabaseName);
    forked.setConnector(this.connector.fork()); // Connector 属性也 fork
    // Connector.fork() 复制：
    //   - soTimeout, sendBufferSize, receiveBufferSize
    //   - keepAlive 等 Socket 选项

    // ===== 不复制的内容（连接层面）=====
    // TCP Socket：新对象没有 Socket，需要单独 connect()
    // 连接状态：新对象是 disconnected 状态
    // dump 状态：不继承 binlog dump 的 position

    return forked;
    // 返回一个配置相同但未连接的 MysqlConnection
    // 调用者需要自己 connect() 建立 TCP 连接
}
```

> **这一步在干什么？** `fork()` 创建一个配置完全相同但 TCP 连接独立的 `MysqlConnection`。为什么需要独立连接？因为心跳探测必须在 dump 线程之外执行——dump 线程正在阻塞式读取 binlog 流，如果在同一个连接上发送 `select 1`，会干扰 binlog dump 协议。forked 连接复制了所有认证信息（用户名、密码、字符集），但没有 TCP Socket——需要在首次使用时调用 `connect()` 建立新的 TCP 连接。这意味着心跳探测使用的是一条与 binlog dump 完全独立的 MySQL 连接。

**Step 3：MysqlDetectingTimeTask.run() —— 重连状态机**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlDetectingTimeTask.java`

```java
public class MysqlDetectingTimeTask extends TimerTask {

    private MysqlConnection connection;  // fork 出来的独立连接
    private boolean reconnect = false;   // 重连标志

    @Override
    public void run() {
        try {
            // ===== 状态机：检查是否需要重连 =====
            if (reconnect) {
                // 上一次执行失败过，需要先重连
                reconnect = false; // 重置标志
                connection.disconnect(); // 断开旧连接（如果还有的话）
                connection.connect();    // 建立新 TCP 连接
                // 如果 connect() 也失败，会被下面的 catch 捕获
                // → reconnect 再次设为 true → 下一个 tick 继续尝试
            } else if (!connection.isConnected()) {
                // 首次连接（fork 出来的连接还没 connect 过）
                connection.connect();
            }

            // ===== 执行探测 SQL =====
            // detectingSQL = "select 1"
            connection.query(detectingSQL);
            // 如果 MySQL 正常，query 会成功返回
            // 如果 MySQL 已断开/超时，query 会抛 IOException

            // 探测成功 → 通知 HA Controller
            if (haController != null) {
                haController.onSuccess();
                // → 重置失败计数器
            }

        } catch (Throwable e) {
            // 探测失败！
            reconnect = true;
            // 下一个 tick（3 秒后）会先执行 disconnect + connect

            if (haController != null) {
                haController.onFailed();
                // → 失败计数器 +1
                // → 如果超过阈值，触发 HA 切换（详见 Step 5）
            }
        }
    }
}
```

重连状态机的状态转换：

```
                    ┌──────────────────────────────────┐
                    │                                  │
                    ▼                                  │
        ┌───────────────────┐    成功                  │
  ───→  │ reconnect = false │──────────→ query() 成功  │
        │ 正常执行 query()   │           onSuccess()    │
        └───────────────────┘                          │
                    │                                  │
                    │ query() 失败                     │
                    │ IOException                      │
                    ▼                                  │
        ┌───────────────────┐    connect 成功           │
        │ reconnect = true  │──────────────────────────┘
        │ 下一 tick:         │
        │ disconnect()      │    connect 失败
        │ connect()         │──────────→ reconnect = true
        └───────────────────┘           （继续下一 tick 重试）
```

> **这一步在干什么？** `MysqlDetectingTimeTask` 实现了一个简洁的两状态状态机：`reconnect=false`（正常态）和 `reconnect=true`（需要重连态）。正常态下直接执行 `query(detectingSQL)`；如果 query 失败（MySQL 不可达），标记 `reconnect=true`。下一个 tick（3 秒后）进入重连态：先 `disconnect()` 清理旧连接，再 `connect()` 建立新连接。如果 connect 也失败，catch 块再次设置 `reconnect=true`，下一个 tick 继续重试——形成一个自愈循环。每次成功或失败都会通知 `haController`（详见 Step 5），由 HA 控制器决定是否触发主备切换。

**Step 4：被动心跳模式 —— 合成 HEARTBEAT 事件与过滤**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/AbstractEventParser.java`

```java
// 被动模式：buildHeartBeatTimeTask 中 detectingTimeTask=null 的分支
// 已在 Step 1 展示，这里展示下游的 HeartBeatEntryEventHandler

// ===== HeartBeatEntryEventHandler =====
// 源码位置: parse/src/main/java/com/alibaba/otter/canal/parse/inbound/
//          HeartBeatEntryEventHandler.java
public class HeartBeatEntryEventHandler implements CanalEventHandler {

    @Override
    public void handle(CanalEntry.Entry entry) {
        if (entry.getEntryType() == CanalEntry.EntryType.HEARTBEAT) {
            // 心跳事件：记录最后心跳时间，不投递给下游
            lastHeartBeatTime = System.currentTimeMillis();
            // 不调用 next.handle(entry)，事件到此为止
            return;
        }
        // 非心跳事件：正常传递
        next.handle(entry);
    }
}

// ===== 合成心跳事件的完整流经路径 =====
// 1. Timer 线程合成 HEARTBEAT Entry
// 2. → consumeTheEventAndProfilingIfNecessary()
// 3. → EventTransactionBuffer.add()
// 4. → EventSink.sink()
//      （AviaterRegexFilter 不过滤 HEARTBEAT——它只过滤 ROWDATA）
// 5. → EventStore.put()
// 6. → 客户端 get() 拿到（但通常被 HeartBeatEntryEventHandler 过滤）
```

> **这一步在干什么？** 被动心跳模式不发送 SQL 探测，而是在 binlog 数据"沉默"时合成虚拟的 `HEARTBEAT` 事件。这些合成事件会走完整个 Sink→Store 链路——它们不会被 `AviaterRegexFilter` 过滤（因为过滤器只处理 `ROWDATA` 类型）。但在客户端消费侧，`HeartBeatEntryEventHandler` 会拦截这些心跳事件，记录最后心跳时间后丢弃，不传递给业务处理逻辑。被动模式的优点是不需要额外的 MySQL 连接，但缺点是无法区分"MySQL 正常但没有写入"和"MySQL 已经挂了"——两种情况都没有 binlog 事件，但前者不应该报警。

**Step 5：HeartBeatHAController —— 失败计数与 HA 切换决策**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/ha/HeartBeatHAController.java`

```java
public class HeartBeatHAController implements CanalHAController {

    private int detectingRetryTimes = 3;  // 连续失败阈值
    private int failedTimes = 0;          // 当前连续失败次数
    private boolean switchEnable = false; // 是否允许 HA 切换

    @Override
    public void onFailed() {
        failedTimes++;
        // 每次心跳检测失败，计数 +1

        if (failedTimes >= detectingRetryTimes) {
            // 连续失败次数达到阈值
            if (switchEnable) {
                // ===== 允许 HA 切换 =====
                // canal.instance.standby.switch.enable = true
                eventParser.doSwitch();
                // → 切换到 standby MySQL（详见 Step 6）
                failedTimes = 0; // 重置计数
            } else {
                // ===== 不允许 HA 切换 =====
                // 只记录日志，不做任何操作
                // 等待 dump 线程的 catch-reconnect 循环自愈（Step 7）
                logger.warn("heart beat failed {} times, "
                    + "switch is disabled", failedTimes);
            }
        }
    }

    @Override
    public void onSuccess() {
        // 心跳成功，重置失败计数
        failedTimes = 0;
    }
}
```

> **这一步在干什么？** `HeartBeatHAController` 是心跳检测与 HA 切换之间的决策层。它维护一个 `failedTimes` 计数器：每次心跳失败 +1，成功则归零。当连续失败次数达到阈值（默认 3 次，即 9 秒内 3 次检测都失败），检查 `switchEnable` 开关——如果允许切换，调用 `eventParser.doSwitch()` 触发主备切换；如果不允许，只打日志不动作，依赖 dump 线程自己的重连机制。这是一个简单但有效的策略：3 次失败确认避免了网络瞬间抖动导致的误切换，而 `switchEnable` 开关给运维人员提供了控制权。

**Step 6：MysqlEventParser.doSwitch() —— 完整的主备切换流程**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/MysqlEventParser.java`

```java
public void doSwitch() {
    // ===== 前置检查 =====
    // 1. standby 地址是否存在
    if (standbyInfo == null) {
        logger.warn("standby is null, cannot switch");
        return;
        // 没有配置 standby，切换无意义
    }

    // 2. standby 是否和当前 master 不同
    if (standbyInfo.getAddress().equals(runningInfo.getAddress())) {
        logger.warn("standby is same as master, skip switch");
        return;
        // 配置错误：standby 和 master 是同一个地址
    }

    // ===== 执行切换 =====
    // 3. 停止当前 Parser
    stop();
    // stop() 的执行序列：
    //   a. timer.cancel() → 取消心跳定时器
    //   b. parseThread.interrupt() → 中断 dump 线程
    //   c. if (multiStageCoprocessor != null) coprocessor.stop()
    //      → 停止 Disruptor 流水线（Stage 2/3/4 线程）
    //   d. connection.disconnect() → 断开 MySQL 连接（dump 连接）
    //   e. detectingConnection.disconnect() → 断开心跳连接

    // 4. 交换主备地址
    AuthenticationInfo tmp = this.runningInfo;
    this.runningInfo = this.standbyInfo;
    this.standbyInfo = tmp;
    // 交换后：
    //   runningInfo = 原 standby 地址
    //   standbyInfo = 原 master 地址

    // 5. 重新启动
    start();
    // start() 的执行序列：
    //   a. 新的 parseThread 启动
    //   b. MysqlConnection.connect() → 连接到新的 master（原 standby）
    //   c. findStartPosition() → 在新 master 上查找起始位点
    //      → 如果有 fallbackIntervalInSeconds=60：
    //        当前时间往回滚 60 秒，找到对应的 binlog 位置
    //        确保不会因为切换延迟而丢失数据
    //   d. COM_BINLOG_DUMP → 开始从新 master dump binlog
    //   e. startHeartBeat() → 启动新的心跳定时器
}

// findStartPosition 中的 fallback 逻辑
private EntryPosition findStartPositionWithFallback() {
    // fallbackIntervalInSeconds = 60（默认）
    long fallbackTimestamp = System.currentTimeMillis()
        - fallbackIntervalInSeconds * 1000L;
    // 在新 master 的 binlog 中找到 fallbackTimestamp 对应的位置
    // 从那个位置开始 dump，确保覆盖切换期间可能遗漏的事件
    return findByTimestamp(fallbackTimestamp);
}
```

> **这一步在干什么？** `doSwitch()` 是一个"停止→交换→启动"的三步切换流程。停止阶段非常彻底：取消心跳定时器、中断 dump 线程、停止 Disruptor 流水线、断开所有 MySQL 连接——确保没有任何残留线程或连接。交换阶段直接 swap `runningInfo` 和 `standbyInfo`——简单的引用交换。启动阶段在新的 master（原 standby）上重新建连、查找起始位点、开始 dump。`fallbackIntervalInSeconds=60` 是关键的安全参数：切换后不是从最新位置开始 dump，而是往回找 60 秒——因为 master 故障到检测到故障再到切换完成可能需要若干秒，回退 60 秒确保这段时间内的变更不会丢失。代价是可能会重复消费一些事件，但 Canal 的设计哲学是"宁可重复，不可丢失"。

**Step 7：Dump 线程自愈 —— 最后防线的 catch-reconnect 循环**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/AbstractEventParser.java`

```java
// dump 线程的 run() 方法核心结构
public void run() {
    while (running) {
        try {
            // 1. 建立 MySQL 连接
            connection.connect();
            // 2. 注册为 Slave
            connection.register();
            // 3. 发送 COM_BINLOG_DUMP
            connection.dump(position, handler);
            // dump() 是阻塞的：持续接收 binlog 直到连接断开或异常

        } catch (Throwable e) {
            // ===== 连接断开或任何异常 =====
            if (!running) {
                // 正常停止（stop() 调用导致），不需要重连
                break;
            }

            logger.error("dump error, retry after sleep", e);

            // 1. 断开当前连接（清理资源）
            try {
                connection.disconnect();
            } catch (IOException ioe) {
                logger.warn("disconnect failed", ioe);
            }

            // 2. 随机等待 10-20 秒后重试
            // 随机化避免多个 Canal Instance 同时重连冲击 MySQL
            long sleepTime = 10000 + (long) (Math.random() * 10000);
            // sleepTime ∈ [10000, 20000) 毫秒
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ie) {
                // 被中断说明 stop() 被调用了
                break;
            }

            // 3. 重试：回到 while 循环头部
            // → connect() → register() → dump()
        }
    }
}
```

> **这一步在干什么？** 这是 Canal 连接保护的"最后防线"。不管心跳检测是否启用，dump 线程自身的 `while(running)` + `catch(Throwable)` 循环确保了：任何原因导致的连接断开（MySQL 重启、网络超时、协议错误、甚至 OOM 以外的任何异常）都会被捕获，执行 `disconnect()` 清理 → 随机 sleep 10-20 秒 → 重新 `connect()` + `dump()`。随机 sleep 是一个反惊群（anti-thundering-herd）策略——如果 MySQL 重启导致 10 个 Canal Instance 同时断开，它们不会同时发起重连请求。这个循环与心跳检测是互补关系：心跳检测是"主动发现问题并可能触发 HA 切换"，dump 循环是"被动处理连接断开并就地重连"。

**Step 8：线程安全分析 —— 良性数据竞争与独立连接设计**

```java
// ===== 线程安全相关的字段分析 =====

// 1. lastEntryTime —— 非 volatile 普通字段
private long lastEntryTime = 0;
// 写入线程：dump 线程（每处理一个 binlog event 更新）
// 读取线程：Timer 心跳线程（被动模式下检查时间差）
// 数据竞争：是的，可能读到旧值
// 后果：心跳线程可能多合成一次 HEARTBEAT 或少合成一次
//       → 无业务影响（HEARTBEAT 事件本身是幂等的）
//       → 良性数据竞争（benign data race）

// 2. exception —— volatile 字段（Disruptor 流水线中的）
private volatile Throwable exception;
// 写入：Stage 2/3/4 的 catch 块
// 读取：Stage 1 的 publish() 方法
// volatile 保证了跨线程可见性，一旦下游出错，上游立刻感知

// 3. reconnect —— MysqlDetectingTimeTask 的普通字段
private boolean reconnect = false;
// 写入和读取都在同一个 Timer 线程中（单线程定时器）
// 不存在数据竞争

// 4. forked connection —— 独立 TCP Socket
// dump 连接 vs 心跳连接：
//   - 各自拥有独立的 Socket 和 InputStream/OutputStream
//   - 互不干扰：dump 线程阻塞读 binlog 不影响心跳线程发 select 1
//   - 独立故障：心跳连接断开不影响 dump 连接，反之亦然
//   - 独立重连：心跳连接通过 MysqlDetectingTimeTask 状态机自愈
//              dump 连接通过 while(running) catch 循环自愈
```

> **这一步在干什么？** Canal 在连接自愈模块中的线程安全设计是实用主义的。`lastEntryTime` 故意不用 volatile——因为心跳线程读到稍旧的值只会多合成一个心跳事件（幂等、无害），而加 volatile 会在每次 binlog event 处理时增加内存屏障开销，在高 TPS 场景下影响性能。Disruptor 流水线中的 `exception` 字段使用 volatile 是必须的——异常传播必须实时可见，否则 Stage 1 会继续往 RingBuffer 塞数据直到满了才停。心跳连接和 dump 连接使用完全独立的 TCP Socket，这是最简单也最安全的设计——不需要在同一个连接上做多路复用，避免了复杂的同步逻辑。

**两层保护机制示意图**

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                     第一层：主动心跳检测                          │
  │                                                                 │
  │  Timer 线程 ──→ MysqlDetectingTimeTask                         │
  │                    │                                            │
  │              fork() 独立连接                                     │
  │                    │                                            │
  │             query("select 1")                                   │
  │               ┌────┴────┐                                       │
  │            成功│        │失败                                    │
  │               ▼        ▼                                        │
  │          onSuccess()  onFailed()                                │
  │          重置计数     failedTimes++                              │
  │                       │                                         │
  │                 ≥ threshold?                                     │
  │               ┌───┴───┐                                         │
  │            否 │       │ 是                                      │
  │              等待    switchEnable?                               │
  │               ┌───┴───┐                                         │
  │            否 │       │ 是                                      │
  │            日志      doSwitch()                                  │
  │                   stop→swap→start                               │
  └─────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────┐
  │              第二层：Dump 线程 catch-reconnect 循环               │
  │                                                                 │
  │  while (running) {                                              │
  │      try {                                                      │
  │          connect() → register() → dump()  // 阻塞式读 binlog    │
  │      } catch (Throwable) {                                      │
  │          disconnect()                                           │
  │          sleep(random 10~20s)  // 随机退避                      │
  │          // 回到 while 循环头部重试                               │
  │      }                                                          │
  │  }                                                              │
  └─────────────────────────────────────────────────────────────────┘
```

---

## 案例十三：全量数据同步（ETL 模式）—— REST API 触发的数据迁移

### 场景描述

Canal 除了增量 binlog 订阅，还支持通过 REST API 触发全量数据同步（ETL）。适用场景：初始化新的下游存储（ES、RDB）、数据修复、定期全量对账。ETL 模式会暂停增量同步，执行全表扫描（SELECT），将数据写入目标存储，完成后恢复增量同步。

### 配置

```properties
# application.yml（Canal Adapter）
canal.conf:
  mode: tcp
  canalServerHost: 127.0.0.1:11111
  srcDataSources:
    defaultDS:
      url: jdbc:mysql://127.0.0.1:3306/mydb
      username: root
      password: root

# REST API 调用
# POST /etl/{type}/{task}
# 例如：POST /etl/rdb/mydb_users.yml
# 带条件：POST /etl/rdb/mydb_users.yml?params=id>1000;status=1
```

### 全链路源码追踪

**Step 1：REST API 入口 —— CommonRest.etl() 的四步编排**

**源码位置**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/CommonRest.java`

```java
@PostMapping("/etl/{type}/{task}")
public EtlResult etl(@PathVariable String type,
                     @PathVariable String task,
                     @RequestParam(required = false) String params) {
    // type = "rdb" 或 "es" 等
    // task = "mydb_users.yml"
    // params = "id>1000;status=1"（可选条件）

    // ===== 四步编排 =====

    // Step 1: 获取锁 —— 防止并发 ETL
    EtlLock etlLock = EtlLock.get(type + "-" + task);
    if (!etlLock.tryLock()) {
        return EtlResult.error("ETL task is running, please wait");
        // 已有同名 ETL 任务在执行，拒绝重复提交
    }

    try {
        // Step 2: 暂停增量同步
        syncSwitch.off(type, task);
        // → BooleanMutex.off() → 增量消费线程阻塞（详见 Step 3）

        // Step 3: 执行 ETL
        OuterAdapter adapter = findAdapter(type, task);
        // 根据 type 找到对应的 Adapter（RdbAdapter、EsAdapter 等）
        EtlResult result = adapter.etl(task, params);
        // → AbstractEtlService.importData()（详见 Step 5）

        return result;

    } finally {
        // Step 4: 恢复增量同步（无论 ETL 是否成功）
        syncSwitch.on(type, task);
        // → BooleanMutex.on() → 唤醒增量消费线程

        etlLock.unlock();
    }
}
```

> **这一步在干什么？** ETL 的入口是一个标准的 Spring MVC REST 接口。四步编排保证了操作的安全性：1）获取锁——防止同一个 ETL 任务被并发触发（详见 Step 2）。2）暂停增量同步——ETL 期间不能有增量数据写入，否则可能覆盖全量数据或产生冲突。3）执行实际的 ETL 数据导入。4）在 finally 块中恢复增量同步并释放锁——即使 ETL 失败也必须恢复，否则增量同步会永远卡住。

**Step 2：EtlLock 双实现 —— 本地锁 vs ZooKeeper 分布式锁**

**源码位置**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/EtlLock.java`

```java
public class EtlLock {

    private static boolean distributed = false; // 是否使用分布式锁
    private static CuratorFramework zkClient;   // ZK 客户端

    // ===== 初始化时自动选择锁类型 =====
    @PostConstruct
    public void init() {
        String zkServers = config.getZkServers();
        if (StringUtils.isNotEmpty(zkServers)) {
            // 配置了 ZK → 使用分布式锁
            distributed = true;
            zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServers)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
            zkClient.start();
        }
        // 没配置 ZK → 使用本地锁
    }

    // ===== 本地模式：ReentrantLock =====
    private ReentrantLock localLock = new ReentrantLock();

    // ===== 分布式模式：Curator InterProcessMutex =====
    private InterProcessMutex distributedLock;

    public boolean tryLock() {
        if (distributed) {
            // ZK 分布式锁
            try {
                distributedLock = new InterProcessMutex(
                    zkClient, "/canal/etl/lock/" + lockKey);
                return distributedLock.acquire(500, TimeUnit.MILLISECONDS);
                // 500ms 超时：如果 500ms 内拿不到锁，返回 false
                // InterProcessMutex 在 ZK 上创建临时有序节点
                // 最小序号的节点持有锁
            } catch (Exception e) {
                logger.error("acquire distributed lock failed", e);
                return false;
            }
        } else {
            // 本地锁
            return localLock.tryLock();
            // 非阻塞：立即返回 true/false
        }
    }

    public void unlock() {
        if (distributed) {
            try {
                if (distributedLock != null) {
                    distributedLock.release();
                    // 释放 ZK 临时节点
                }
            } catch (Exception e) {
                logger.error("release distributed lock failed", e);
            }
        } else {
            localLock.unlock();
        }
    }
}
```

> **这一步在干什么？** ETL 锁根据是否配置了 ZooKeeper 自动选择实现方式。本地模式用 `ReentrantLock.tryLock()`——单进程内互斥，适用于单机部署。分布式模式用 Curator 的 `InterProcessMutex`——在 ZK 上创建临时有序节点实现分布式互斥锁，500ms 超时防止长时间等待。锁的粒度是 `type + task`，即不同类型或不同任务的 ETL 可以并行执行。`@PostConstruct` 自动检测 ZK 配置——运维人员不需要显式选择锁类型，只要配置了 ZK 地址就自动升级为分布式锁。

**Step 3：SyncSwitch 机制 —— BooleanMutex (AQS) 实现增量暂停/恢复**

**源码位置**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/SyncSwitch.java`

```java
public class SyncSwitch {

    // 每个 adapter 任务有一个独立的 BooleanMutex
    private Map<String, BooleanMutex> switchMap = new ConcurrentHashMap<>();

    public void off(String type, String task) {
        BooleanMutex mutex = switchMap.get(type + "-" + task);
        mutex.off();
        // → 内部设置 state = FALSE
        // → 正在 mutex.get() 上等待的线程会阻塞
    }

    public void on(String type, String task) {
        BooleanMutex mutex = switchMap.get(type + "-" + task);
        mutex.on();
        // → 内部设置 state = TRUE
        // → releaseShared() 唤醒所有阻塞的线程
    }
}

// ===== BooleanMutex —— 基于 AQS 的布尔开关 =====
public class BooleanMutex {

    private Sync sync;

    // Sync 继承 AbstractQueuedSynchronizer
    private static final class Sync extends AbstractQueuedSynchronizer {
        private static final int TRUE = 1;   // 开启状态
        private static final int FALSE = 0;  // 关闭状态

        // off() 设置为 FALSE
        public void off() {
            setState(FALSE);
            // AQS state = 0
        }

        // on() 设置为 TRUE 并唤醒等待者
        public void on() {
            releaseShared(TRUE);
            // → tryReleaseShared(TRUE) 设置 state = TRUE
            // → AQS 框架唤醒所有在 acquireSharedInterruptibly() 上阻塞的线程
        }

        // get() 阻塞等待直到 state = TRUE
        public void get() throws InterruptedException {
            acquireSharedInterruptibly(0);
            // → tryAcquireShared(0)：
            //     return (getState() == TRUE) ? 1 : -1;
            //   如果 state = FALSE → 返回 -1 → AQS 挂起线程
            //   如果 state = TRUE → 返回 1 → 立即返回
        }

        @Override
        protected int tryAcquireShared(int arg) {
            return (getState() == TRUE) ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(int arg) {
            setState(TRUE);
            return true;
        }
    }

    public void get() throws InterruptedException {
        sync.get();
    }
}

// ===== AdapterProcessor 中的阻塞点 =====
// 增量同步的消费循环
public class AdapterProcessor {
    public void process() {
        while (running) {
            // 每次消费前检查开关
            syncSwitch.get();
            // → 如果 off() 被调用过，这里会阻塞
            // → 直到 on() 被调用才恢复

            // 正常消费增量数据
            Message message = connector.getWithoutAck(batchSize);
            // ... 处理 ...
        }
    }
}
```

> **这一步在干什么？** `SyncSwitch` 是 ETL 暂停/恢复增量同步的核心机制。底层使用 `BooleanMutex`——一个基于 AQS（AbstractQueuedSynchronizer）的自定义同步器。`off()` 把 AQS 的 state 设为 0（FALSE），此时任何调用 `get()` 的线程都会被 AQS 挂起（通过 `acquireSharedInterruptibly`）。`on()` 把 state 设为 1（TRUE），并调用 `releaseShared()` 唤醒所有等待线程。增量同步的 `AdapterProcessor` 在每次消费循环的开头调用 `syncSwitch.get()`——如果 ETL 正在进行（开关为 off），增量消费线程会阻塞在这里直到 ETL 完成。这比用 Thread.suspend()/resume() 安全得多：AQS 保证了正确的内存可见性和中断响应。

**Step 4：etlCondition 模板 —— 占位符替换与参数传递**

**源码位置**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/AbstractEtlService.java`

```java
// ETL 配置文件（yml）中的条件模板
// etlCondition: "where id > {} and status = {}"

// REST API 调用时传参
// POST /etl/rdb/mydb_users.yml?params=1000;1
// params 用分号分隔："1000" 和 "1"

private String buildEtlCondition(String etlCondition, String params) {
    if (StringUtils.isEmpty(etlCondition)) {
        return "";
    }

    if (StringUtils.isEmpty(params)) {
        // 没有参数：移除所有占位符
        return etlCondition.replace("{}", "");
    }

    // 分号分隔参数
    String[] paramArray = params.split(";");
    // paramArray = ["1000", "1"]

    // ===== 占位符替换 =====
    // 注意：使用 String.replace("{}", "?") 替换
    // String.replace 替换 ALL 匹配项（不是 replaceFirst）
    String sql = etlCondition.replace("{}", "?");
    // "where id > {} and status = {}" → "where id > ? and status = ?"

    // paramArray 通过 PreparedStatement.setObject() 绑定
    // 这里有一个细节：如果 {} 出现次数 ≠ params 个数
    // 不会报错，但会导致 SQL 参数绑定异常

    return sql;
    // 返回的 SQL 和 params 传给 importData() 使用
}
```

> **这一步在干什么？** ETL 条件模板使用 `{}` 作为占位符（类似 SLF4J 的日志格式）。替换过程分两步：1）把所有 `{}` 替换为 JDBC 的 `?` 占位符。2）参数通过分号分隔传入，按顺序绑定到 `PreparedStatement`。注意 `String.replace()` 替换的是所有匹配项——如果模板中有 3 个 `{}` 但只传了 2 个参数，JDBC 执行时会因为参数不足而报错。这个设计简单但不够健壮：没有参数个数校验，也不支持命名参数。REST API 中 params 用分号而不是逗号分隔，可能是为了避免 URL 中逗号的编码问题。

**Step 5：AbstractEtlService.importData() —— 并行策略与分页**

**源码位置**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/AbstractEtlService.java`

```java
public EtlResult importData(String task, String params) {
    // 1. 查询总行数
    String countSql = "SELECT COUNT(*) FROM " + tableName;
    if (StringUtils.isNotEmpty(etlCondition)) {
        countSql += " " + etlCondition;
    }
    long cnt = jdbcTemplate.queryForObject(countSql, Long.class, paramValues);
    // cnt = 总行数（比如 500000）

    // 2. 根据数据量选择策略
    if (cnt >= 10000) {
        // ===== 大数据量：多线程分页导入 =====
        int threadCount = Runtime.getRuntime().availableProcessors();
        // 线程数 = CPU 核心数

        int pageSize = 10000;
        // 每页 10000 条

        long totalPages = (cnt + pageSize - 1) / pageSize;
        // 总页数 = ceil(cnt / pageSize)

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (long page = 0; page < totalPages; page++) {
            final long offset = page * pageSize;
            futures.add(executor.submit(() -> {
                // 分页查询
                String pageSql = "SELECT * FROM " + tableName
                    + " " + etlCondition
                    + " LIMIT " + offset + ", " + pageSize;

                // 每页数据逐行写入目标存储
                List<Map<String, Object>> rows =
                    jdbcTemplate.queryForList(pageSql, paramValues);
                for (Map<String, Object> row : rows) {
                    // 写入目标（RDB 或 ES）
                    importRow(row);
                }
                return true;
            }));
        }

        // 等待所有页完成
        for (Future<Boolean> future : futures) {
            future.get(); // 阻塞等待
        }

    } else {
        // ===== 小数据量：单线程导入 =====
        String sql = "SELECT * FROM " + tableName + " " + etlCondition;
        List<Map<String, Object>> rows =
            jdbcTemplate.queryForList(sql, paramValues);
        for (Map<String, Object> row : rows) {
            importRow(row);
        }
    }

    return EtlResult.success(cnt);
}
```

> **这一步在干什么？** ETL 的数据导入根据数据量自动选择并行策略：10000 条以上使用多线程分页导入（线程数=CPU 核心数，每页 10000 条），10000 条以下单线程直接导入。分页使用 `LIMIT offset, pageSize` 实现——在大表上 offset 很大时性能会急剧下降（MySQL 的 LIMIT 优化问题），但 Canal 没有使用 cursor-based 分页。多线程分页的隐含风险：各页的数据互相独立，如果表在 ETL 期间有写入（理论上增量已暂停，但如果有其他写入来源），可能读到不一致的快照。`importRow()` 是模板方法，由子类（`RdbEtlService`、`EsEtlService`）实现不同的写入逻辑。

**Step 6：RDB ETL 实现 —— DELETE+INSERT 幂等写入**

**源码位置**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/service/RdbEtlService.java`

```java
// RdbEtlService 继承 AbstractEtlService
// importRow() 的实现

@Override
protected void importRow(Map<String, Object> row) {
    // 1. 构建主键条件
    String pkColumn = mapping.getPkColumn(); // 例如 "id"
    Object pkValue = row.get(pkColumn);

    // 2. DELETE 已存在的行
    String deleteSql = "DELETE FROM " + targetTable
        + " WHERE " + pkColumn + " = ?";
    targetJdbcTemplate.update(deleteSql, pkValue);
    // 先删再插 → 幂等：多次执行结果相同

    // 3. INSERT 新行
    StringBuilder columns = new StringBuilder();
    StringBuilder placeholders = new StringBuilder();
    List<Object> values = new ArrayList<>();
    for (Map.Entry<String, Object> field : row.entrySet()) {
        if (columns.length() > 0) {
            columns.append(", ");
            placeholders.append(", ");
        }
        columns.append(field.getKey());
        placeholders.append("?");
        values.add(field.getValue());
    }
    String insertSql = "INSERT INTO " + targetTable
        + " (" + columns + ") VALUES (" + placeholders + ")";
    targetJdbcTemplate.update(insertSql, values.toArray());

    // 4. commitBatch 控制
    batchCount++;
    if (batchCount >= commitBatch) {
        // 每 commitBatch 条提交一次事务
        // 避免单个巨大事务
        connection.commit();
        batchCount = 0;
    }
}

// 注意：RdbEtlService.importData() 即使内部某行 importRow() 失败
// 也只打日志不抛异常，最终返回 true
// 这意味着部分失败时调用方看到的是"成功"
public EtlResult importData(String task, String params) {
    try {
        // ... 导入逻辑 ...
        return EtlResult.success(cnt);
    } catch (Exception e) {
        logger.error("importData error", e);
        return EtlResult.success(0);
        // 注意：即使捕获异常也返回 "success"
        // 这是一个设计缺陷
    }
}
```

> **这一步在干什么？** RDB ETL 的写入策略是 DELETE+INSERT：先按主键删除目标表中已存在的行，再插入新数据。这保证了幂等性——无论执行多少次，目标数据最终与源数据一致。`commitBatch` 控制事务提交频率，避免单个巨大事务导致的锁等待或 redo log 暴涨。但有两个值得注意的问题：1）错误处理不严谨——`importRow()` 如果失败只打日志，不影响后续行的处理，这意味着最终结果可能包含缺失的行。2）`importData()` 即使捕获异常也返回 success——调用者无法通过返回值判断 ETL 是否完全成功，这是一个设计缺陷。

**Step 7：ES ETL 实现 —— Upsert 语义与 Bulk 写入**

**源码位置**: `client-adapter/escore/src/main/java/com/alibaba/otter/canal/client/adapter/es/core/service/EsEtlService.java`

```java
// EsEtlService 继承 AbstractEtlService

@Override
protected void importRow(Map<String, Object> row) {
    // 1. 构建文档 ID（通常是主键值）
    String docId = row.get(mapping.getId()).toString();

    // 2. 构建 IndexRequest（Upsert 语义）
    // ES 的 INDEX 操作天然是 upsert：
    //   文档不存在 → 创建
    //   文档已存在 → 覆盖（全量替换）
    IndexRequest indexRequest = new IndexRequest(mapping.getIndex())
        .id(docId)
        .source(row);

    // 3. 加入 BulkRequest 批量缓冲
    bulkRequest.add(indexRequest);
    bulkCount++;

    if (bulkCount >= commitBatch) {
        // 达到批量阈值，执行 Bulk 写入
        BulkResponse response = esClient.bulk(bulkRequest);

        // 4. 检查 Bulk 响应
        if (response.hasFailures()) {
            // Bulk 中部分文档写入失败
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    logger.error("ES bulk item failed: {}, doc id: {}",
                        item.getFailureMessage(), item.getId());
                    // 只打日志，不重试
                    // → 部分失败的文档会丢失
                }
            }
        }

        // 5. 重置 Bulk 缓冲
        bulkRequest = new BulkRequest();
        bulkCount = 0;
    }
}

// ===== ES ETL 没有事务保护 =====
// ES 不支持事务，Bulk 操作是原子的吗？不是。
// Bulk 中的每个文档操作是独立的：
//   文档 A 成功，文档 B 失败 → A 已写入，B 没有
// 没有回滚机制，也没有重试机制
// 幂等性靠 upsert 保证：重新 ETL 会覆盖已有文档
```

> **这一步在干什么？** ES ETL 利用 Elasticsearch 的 INDEX 操作天然的 upsert 语义——如果文档已存在则覆盖，不存在则创建。不需要像 RDB 那样先 DELETE 再 INSERT。数据通过 `BulkRequest` 批量写入以提高性能（减少网络往返和 ES 内部的刷新次数）。但 Bulk 操作不是原子的——一个 Bulk 中的多个文档操作是独立的，部分成功部分失败是可能的。失败的文档只打日志不重试，也不会导致整个 ETL 失败——这与 RDB ETL 的问题类似。重新执行 ETL 是安全的：upsert 语义保证了重复写入不会产生脏数据。

**Step 8：错误恢复分析 —— 无断点续传的幂等设计**

```java
// ===== 核心问题：ETL 中断后怎么办？=====

// 1. 没有断点续传（checkpoint）机制
//    ETL 不记录"已导入到第几行/第几页"
//    中断后只能从头重新执行

// 2. 重新执行是安全的（幂等性保证）
//    RDB ETL：DELETE + INSERT → 同一行重复导入不会产生重复数据
//    ES ETL：upsert → 同一文档重复写入只是覆盖

// 3. 中断时已提交的数据不会回滚
//    RDB：已 commit 的 batch 保留在目标库中
//    ES：已 bulk 的文档保留在 ES 中
//    → 部分成功的状态是合法的（因为重新 ETL 会补齐）

// 4. RdbEtlService 的隐藏问题
//    importData() 即使 inner catch 到异常也返回 success
//    调用者需要检查日志而不是返回值来确认是否成功

// ===== 设计权衡 =====
// 优点：实现简单，无需额外的 checkpoint 存储
// 缺点：大表 ETL 失败后需要完全重跑，浪费时间和资源
//       部分失败不可见（返回值为 success）
```

> **这一步在干什么？** Canal 的 ETL 模式选择了"简单 + 幂等"的设计哲学：不实现断点续传，而是保证每次执行都是幂等的。RDB 通过 DELETE+INSERT 实现幂等（同一行多次导入不会重复），ES 通过 upsert 实现幂等（同一文档多次写入只是覆盖）。中断后重新运行 ETL 会从头开始，已完成的部分会被 DELETE/覆盖。这在小数据量场景下是可接受的，但对于百万级大表，一次失败意味着数小时的工作白费。没有断点续传也意味着无法做进度展示——REST API 调用者只能等到返回才知道结果。

**ES ETL vs RDB ETL 对比表**

| 对比项 | RDB ETL | ES ETL |
|--------|---------|--------|
| 写入语义 | DELETE + INSERT（两步） | INDEX（upsert，一步） |
| 事务支持 | JDBC 事务（commitBatch 控制） | 无事务（Bulk 非原子） |
| 幂等保证 | DELETE 保证不重复 | upsert 天然幂等 |
| 批量机制 | commitBatch（JDBC batch） | BulkRequest（HTTP batch） |
| 部分失败 | 打日志 + 继续 | 打日志 + 继续 |
| 返回值 | 即使异常也返回 success | 同上 |
| 断点续传 | 不支持 | 不支持 |
| 目标存储 | MySQL/Oracle/PostgreSQL | Elasticsearch 6.x/7.x/8.x |
| 写入性能 | 受 JDBC batch size 和事务影响 | 受 Bulk size 和 ES refresh_interval 影响 |

---

## 案例十四：Group 模式（多 MySQL 源合并）—— 多库归一

### 场景描述

多个分库分表的 MySQL 实例（比如 user_db_0、user_db_1、user_db_2）的变更数据需要合并到一个 Canal destination 中，统一投递给下游消费者。Group 模式为每个 MySQL 源启动独立的 EventParser，通过 GroupEventSink 中的 Timeline Barrier 按时间戳排序合并，保证跨库事件的时间有序性。

### 配置

```xml
<!-- conf/example/group-instance.xml -->
<bean id="instance" class="com.alibaba.otter.canal.instance.spring.CanalInstanceWithSpring">
    <property name="eventParser" ref="groupEventParser"/>
    <property name="eventSink" ref="groupEventSink"/>
    <property name="eventStore" ref="memoryEventStore"/>
</bean>

<bean id="groupEventParser" class="com.alibaba.otter.canal.parse.inbound.group.GroupEventParser">
    <property name="eventParsers">
        <list>
            <ref bean="mysqlParser0"/>  <!-- user_db_0 -->
            <ref bean="mysqlParser1"/>  <!-- user_db_1 -->
            <ref bean="mysqlParser2"/>  <!-- user_db_2 -->
        </list>
    </property>
</bean>

<bean id="groupEventSink" class="com.alibaba.otter.canal.sink.entry.group.GroupEventSink">
    <property name="groupSize" value="3"/>
    <property name="filterTransactionEntry" value="true"/>
    <property name="eventStore" ref="memoryEventStore"/>
</bean>

<!-- 每个 Parser 有独立的 MetaManager -->
<bean id="mysqlParser0" class="com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser">
    <property name="masterInfo">
        <bean class="com.alibaba.otter.canal.parse.support.AuthenticationInfo">
            <property name="address" value="mysql-0:3306"/>
        </bean>
    </property>
    <property name="logPositionManager" ref="metaManager0"/>
</bean>
```

### 全链路源码追踪

**Step 1：GroupEventParser 生命周期 —— 简单迭代与零容错**

**源码位置**: `parse/src/main/java/com/alibaba/otter/canal/parse/inbound/group/GroupEventParser.java`

```java
public class GroupEventParser extends AbstractCanalLifeCycle implements CanalEventParser {

    private List<CanalEventParser> eventParsers;  // 子 Parser 列表

    @Override
    public void start() {
        super.start();
        // 简单迭代启动所有子 Parser
        for (CanalEventParser eventParser : eventParsers) {
            eventParser.start();
            // 如果 parser0 启动成功，parser1 启动抛异常：
            //   → parser0 已经在运行了（有 dump 线程在读 binlog）
            //   → parser1 的异常向上传播
            //   → parser2.start() 永远不会被调用
            //   → GroupEventParser 处于半启动状态
            //   → 没有清理逻辑（已启动的 parser0 不会被 stop）
        }
    }

    @Override
    public void stop() {
        // 停止所有子 Parser
        for (CanalEventParser eventParser : eventParsers) {
            eventParser.stop();
        }
        super.stop();
    }

    // ===== 运行时错误不可见 =====
    // GroupEventParser 没有监控子 Parser 的健康状态
    // 如果某个子 Parser 在运行时 dump 线程退出（MySQL 断连且重连失败）
    //   → GroupEventParser 不知道
    //   → 该源的数据停止流入 GroupEventSink
    //   → TimelineBarrier 会永久阻塞所有源（详见 Step 6）
}
```

> **这一步在干什么？** `GroupEventParser` 是一个非常简单的"容器"——它只做一件事：遍历子 Parser 列表并逐个 start/stop。但简单意味着脆弱：启动过程没有 try-catch 保护，如果第 2 个 Parser 启动失败，第 1 个已启动的 Parser 不会被回滚停止，留下半启动状态。更严重的是运行时错误不可见——`GroupEventParser` 不监控子 Parser 的健康状态。如果某个子 Parser 的 dump 线程因 MySQL 断连退出，GroupEventParser 不知道，但 `GroupEventSink` 的 `TimelineBarrier` 会因为收不到该源的数据而阻塞所有源，导致整个 Group 停摆。

**Step 2：GroupEventSink 初始化 —— 与 Parser 的间接耦合和 Barrier 类型选择**

**源码位置**: `sink/src/main/java/com/alibaba/otter/canal/sink/entry/group/GroupEventSink.java`

```java
public class GroupEventSink extends AbstractCanalEventSink<List<CanalEntry.Entry>> {

    private int groupSize;  // 源的数量（=子 Parser 数量）
    private boolean filterTransactionEntry = false;
    private TimelineBarrier barrier;
    private EventStore eventStore;

    @Override
    public void start() {
        super.start();

        // ===== Barrier 类型选择 =====
        if (filterTransactionEntry) {
            // filterTransactionEntry = true
            // → 过滤事务边界事件（TRANSACTIONBEGIN/END）
            // → 使用简单的 TimelineBarrier
            barrier = new TimelineBarrier(groupSize);
            // TimelineBarrier 只按时间戳排序，不感知事务边界

        } else {
            // filterTransactionEntry = false
            // → 保留事务边界事件
            // → 使用 TimelineTransactionBarrier
            barrier = new TimelineTransactionBarrier(groupSize);
            // TimelineTransactionBarrier 在事务内跳过时间戳检查
            // 保证事务的原子性（详见 Step 4）
        }

        // ===== 注意：GroupEventSink 不直接引用 GroupEventParser =====
        // 两者的关联通过 Spring XML 配置：
        //   每个子 Parser 的 eventSink 属性都指向同一个 groupEventSink
        //   → 子 Parser 调用 eventSink.sink() 时，都走到 GroupEventSink
        //   → GroupEventSink 通过 groupSize 知道有几个源
        //   → 但不知道每个源的身份或状态
    }
}
```

> **这一步在干什么？** `GroupEventSink` 的初始化有两个关键决策：1）根据 `filterTransactionEntry` 选择 Barrier 类型——如果为 true，使用简单的 `TimelineBarrier`（按时间戳排序，忽略事务边界）；如果为 false，使用 `TimelineTransactionBarrier`（保留事务语义，事务内的事件不做时间戳排序）。2）`GroupEventSink` 和 `GroupEventParser` 之间没有直接引用——它们通过 Spring 配置间接关联：每个子 Parser 的 `eventSink` 属性都引用同一个 `groupEventSink` Bean。GroupEventSink 只知道 `groupSize`（有几个源），不知道每个源是谁，也不知道每个源的健康状态。

**Step 3：TimelineBarrier 完整算法 —— PriorityBlockingQueue 与 await/permit/single/clear 循环**

**源码位置**: `sink/src/main/java/com/alibaba/otter/canal/sink/entry/group/TimelineBarrier.java`

```java
public class TimelineBarrier {

    private int groupSize;                    // 源的数量
    private volatile long threshold = Long.MIN_VALUE;  // 当前时间戳阈值
    private PriorityBlockingQueue<Long> lastTimestamps;
    // 所有源提交的最新时间戳，按自然顺序排列
    // 注意：这是一个 flat multiset，不是按源分组的
    // 如果 3 个源都提交了时间戳，queue 里有 3 个元素

    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public TimelineBarrier(int groupSize) {
        this.groupSize = groupSize;
        this.lastTimestamps = new PriorityBlockingQueue<>(groupSize);
    }

    // ===== Step 3a: await() —— 源提交时间戳并等待放行 =====
    public void await(long timestamp) throws InterruptedException {
        // 每个子 Parser 的 dump 线程调用此方法
        // timestamp = 当前 binlog event 的时间戳

        lastTimestamps.add(timestamp);
        // 加入优先级队列

        lock.lock();
        try {
            // 尝试触发 single()
            single();

            // 检查是否被允许通过
            while (!isPermit(timestamp)) {
                // 不允许：阻塞等待
                condition.await();
                // 被 notify() 唤醒后重新检查
            }
        } finally {
            lock.unlock();
        }
    }

    // ===== Step 3b: single() —— 当所有源都提交后设置阈值 =====
    private void single() {
        if (lastTimestamps.size() >= groupSize) {
            // 所有 groupSize 个源都提交了时间戳
            // 取最小值作为阈值
            threshold = lastTimestamps.peek();
            // peek() 返回 PriorityBlockingQueue 中的最小元素
            // threshold = 所有源中最早的时间戳
            // → 只有时间戳 <= threshold 的事件才允许通过
        }
        // 如果还没凑齐 groupSize 个时间戳
        // threshold 保持为 Long.MIN_VALUE
        // → isPermit() 返回 false → 所有源阻塞
    }

    // ===== Step 3c: isPermit() —— 检查是否放行 =====
    public boolean isPermit(long timestamp) {
        return timestamp <= threshold;
        // 只有当前事件的时间戳 ≤ 全局最小时间戳才放行
        // 这保证了：时间戳最小的事件先被处理
        // → 跨源的时间有序性
    }

    // ===== Step 3d: clear() —— 事件处理完后移除时间戳 =====
    public void clear(long timestamp) {
        lastTimestamps.remove(timestamp);
        // 从队列中移除已处理的时间戳
        // → 为下一轮 single() 做准备
    }

    // ===== Step 3e: notify() —— 唤醒所有等待线程 =====
    public void notify_() {
        lock.lock();
        try {
            condition.signalAll();
            // 唤醒所有在 await() 中阻塞的线程
            // 它们会重新检查 isPermit()
        } finally {
            lock.unlock();
        }
    }
}

// ===== GroupEventSink.sink() 中的完整调用序列 =====
public boolean sink(List<CanalEntry.Entry> entries, ...) {
    for (CanalEntry.Entry entry : entries) {
        long timestamp = entry.getHeader().getExecuteTime();

        // 1. await：提交时间戳并等待放行
        barrier.await(timestamp);

        // 2. 放行后：写入 EventStore
        eventStore.put(entry);

        // 3. clear：移除已处理的时间戳
        barrier.clear(timestamp);

        // 4. notify：唤醒其他等待的源
        barrier.notify_();
    }
    return true;
}
```

> **这一步在干什么？** `TimelineBarrier` 是 Group 模式的核心排序算法。它使用一个 `PriorityBlockingQueue<Long>` 收集所有源提交的时间戳（注意：这是一个扁平的优先级队列，不按源分组）。当所有 `groupSize` 个源都提交了时间戳后（`lastTimestamps.size() >= groupSize`），取最小值作为 `threshold`——只有时间戳 <= threshold 的事件才允许通过。这保证了全局时间有序性：时间戳最小的事件先被写入 EventStore。完整的调用循环是：await（提交并等待）→ isPermit（检查放行）→ 放行后写入 → clear（移除已处理的时间戳）→ notify（唤醒其他源）。使用 `ReentrantLock`/`Condition` 而不是 `wait()/notify()` 是因为 Condition 支持精确唤醒和多条件等待。

**Step 4：TimelineTransactionBarrier —— 事务内跳过时间戳检查**

**源码位置**: `sink/src/main/java/com/alibaba/otter/canal/sink/entry/group/TimelineTransactionBarrier.java`

```java
public class TimelineTransactionBarrier extends TimelineBarrier {

    // 事务状态（跨线程共享）
    private AtomicInteger txState = new AtomicInteger(0);
    // 0 = idle（空闲）
    // 1 = in-transaction（事务进行中）
    // 2 = non-transaction（非事务事件通过中）

    // 每个线程的事务状态
    private ThreadLocal<Boolean> inTransaction = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    @Override
    public void await(long timestamp) throws InterruptedException {
        CanalEntry.EntryType entryType = currentEntryType.get();

        if (entryType == CanalEntry.EntryType.TRANSACTIONBEGIN) {
            // ===== 事务开始 =====
            // 必须通过 Timeline 检查（确定这个事务在时间线上的位置）
            super.await(timestamp);
            // 通过后标记：当前线程进入事务状态

            // CAS 设置全局事务状态：idle(0) → in-transaction(1)
            while (!txState.compareAndSet(0, 1)) {
                // 如果 txState != 0（另一个源正在事务中）
                // 等待：只有一个源的事务可以同时通过
                // 这保证了事务的串行化
                lock.lock();
                try {
                    condition.await();
                } finally {
                    lock.unlock();
                }
            }
            inTransaction.set(true);

        } else if (entryType == CanalEntry.EntryType.TRANSACTIONEND) {
            // ===== 事务结束 =====
            // 直接放行（事务内的事件已经确定了时间线位置）
            // 不做 Timeline 检查
            inTransaction.set(false);
            // 重置事务状态：in-transaction(1) → idle(0)
            txState.set(0);

        } else {
            // ===== 普通数据事件 =====
            if (inTransaction.get()) {
                // 在事务内：跳过 Timeline 检查，直接放行
                // 理由：事务的 BEGIN 已经确定了时间线位置
                //       事务内的所有事件应该连续处理
                return;
            } else {
                // 不在事务内：走正常 Timeline 检查
                super.await(timestamp);
            }
        }
    }

    // ===== clear() 重置 =====
    @Override
    public void clear(long timestamp) {
        CanalEntry.EntryType entryType = currentEntryType.get();
        if (entryType == CanalEntry.EntryType.TRANSACTIONEND) {
            // 事务结束：清理 Timeline 中的时间戳
            super.clear(timestamp);
            // 唤醒可能在等待的其他源
            notify_();
        } else if (!inTransaction.get()) {
            // 非事务事件：正常清理
            super.clear(timestamp);
        }
        // 事务内的事件：不清理（BEGIN 的时间戳保持到 END）
    }

    // ===== interrupt/reset 用于死锁预防 =====
    public void interrupt() {
        // 中断所有等待线程
        // 用于 Parser 停止时清理
        txState.set(0);
        inTransaction.remove();
    }
}
```

> **这一步在干什么？** `TimelineTransactionBarrier` 在 `TimelineBarrier` 基础上增加了事务感知。核心思想是：事务的 `TRANSACTIONBEGIN` 事件必须通过时间戳排序检查（确定这个事务在全局时间线上的位置），但一旦 BEGIN 通过，事务内的所有后续事件（INSERT/UPDATE/DELETE）都直接放行，不做时间戳检查——直到 `TRANSACTIONEND`。这保证了一个事务的所有事件被连续处理，不会被其他源的事件插入打断。`txState` 使用 `AtomicInteger` 实现 CAS 竞争：同一时刻只有一个源的事务可以通过，其他源的事务 BEGIN 会阻塞等待。`ThreadLocal<Boolean> inTransaction` 让每个 dump 线程知道自己是否在事务中。`interrupt()` 和 `reset()` 用于 Parser 停止时清理状态，防止残留的事务状态导致死锁。

**Step 5：位点管理 —— 每个子 Parser 独立的 LogPositionManager**

**源码位置**: `conf/example/group-instance.xml`（Spring 配置）

```xml
<!-- 每个子 Parser 有独立的 MetaManager -->
<bean id="metaManager0" class="...FileMixedMetaManager">
    <property name="dataDir" value="${canal.file.data.dir:../conf}/sub0"/>
</bean>
<bean id="metaManager1" class="...FileMixedMetaManager">
    <property name="dataDir" value="${canal.file.data.dir:../conf}/sub1"/>
</bean>
<bean id="metaManager2" class="...FileMixedMetaManager">
    <property name="dataDir" value="${canal.file.data.dir:../conf}/sub2"/>
</bean>

<!-- 每个 Parser 引用各自的 MetaManager -->
<bean id="mysqlParser0" class="...MysqlEventParser">
    <property name="logPositionManager" ref="metaManager0"/>
</bean>
<bean id="mysqlParser1" class="...MysqlEventParser">
    <property name="logPositionManager" ref="metaManager1"/>
</bean>
```

```java
// ===== 位点管理的独立性 =====
// 每个子 Parser 独立跟踪自己的 binlog 位点
// Parser0 → metaManager0 → conf/sub0/meta.dat
// Parser1 → metaManager1 → conf/sub1/meta.dat
// Parser2 → metaManager2 → conf/sub2/meta.dat

// 没有跨源的位点协调：
//   如果 Parser0 处理到 binlog position 1000
//   Parser1 处理到 binlog position 500
//   两者互不影响

// 重启后每个 Parser 从各自的 meta.dat 恢复位点
// 不存在"Group 整体位点"的概念
// GroupEventSink 的 TimelineBarrier 只做实时排序
// 不参与位点持久化
```

> **这一步在干什么？** Group 模式中，每个子 Parser 有完全独立的 `LogPositionManager`——不同的 Bean 实例、不同的持久化文件（或 ZK 路径）。这意味着每个源的 binlog 消费进度是独立跟踪的，没有"Group 整体位点"的概念。`GroupEventSink` 的 `TimelineBarrier` 只做运行时的事件排序，不参与位点管理。重启后每个 Parser 从各自的 meta 文件恢复，继续从上次断点开始 dump。这种设计的好处是简单可靠（各源互不影响），但也意味着无法做"Group 级别"的一致性回滚——如果需要回退某个源的位点，其他源不会自动配合。

**Step 6：慢源/故障源行为 —— 全局阻塞与无超时设计**

**源码位置**: `sink/src/main/java/com/alibaba/otter/canal/sink/entry/group/TimelineBarrier.java`

```java
// ===== 慢源问题 =====
// 假设 groupSize=3，但只有 2 个源正常工作
// 第 3 个源因 MySQL 断连而停止提交时间戳

private void single() {
    if (lastTimestamps.size() >= groupSize) {
        // lastTimestamps.size() = 2 < groupSize = 3
        // 条件不满足！
        // threshold 保持为初始值 Long.MIN_VALUE
        threshold = lastTimestamps.peek();
    }
    // threshold = Long.MIN_VALUE（永远不更新）
}

public boolean isPermit(long timestamp) {
    return timestamp <= threshold;
    // timestamp（任何正常的时间戳）<= Long.MIN_VALUE
    // → 永远返回 false
    // → 所有源的 await() 都永远阻塞
}

// ===== 这是有意为之的设计 =====
// 代码注释原文：
// "一旦库解析异常，就不会再sink数据"
// 理由：如果放行其他源的数据，会破坏时间有序性保证
//       下游消费者可能看到"时间倒退"的事件

// ===== 超时变体（broken）=====
// TimelineBarrier 有一个带超时的 await() 重载
public void await(long timestamp, long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
    lastTimestamps.add(timestamp);
    lock.lock();
    try {
        single();
        while (!isPermit(timestamp)) {
            boolean success = condition.await(timeout, unit);
            // await(timeout) 返回 false 意味着超时
            // 但 !success 后没有 throw TimeoutException
            // 只是再次检查 isPermit() → 还是 false → 继续等
            // → 超时机制实质上被架空了
            // → 这是一个 bug 或未完成的实现
        }
    } finally {
        lock.unlock();
    }
    // 永远不会抛 TimeoutException
    // 因为 while 循环的退出条件是 isPermit() = true
    // 而不是超时
}

// ===== 没有 stall 检测机制 =====
// GroupEventParser 不监控子 Parser 状态
// GroupEventSink 不检测是否有源长时间未提交
// 运维人员只能通过外部监控发现 Group 停摆
// （比如 EventStore 的 putSequence 长时间不增长）
```

> **这一步在干什么？** 这是 Group 模式最大的设计风险。当某个源因故障停止提交事件时，`lastTimestamps.size()` 永远达不到 `groupSize`，`threshold` 保持 `Long.MIN_VALUE`——所有源的 `isPermit()` 返回 false，所有 dump 线程都永久阻塞。这是有意为之的：设计者认为"一旦库解析异常就不应该继续 sink 数据"，因为单独放行其他源会破坏时间有序性。但问题是没有配套的检测和恢复机制——没有超时报警、没有自动降级、没有 stall 检测。带超时参数的 `await()` 重载实际上是坏的：`condition.await(timeout)` 超时后不抛异常，只是重新检查 `isPermit()`（仍然 false），继续等——等同于无超时。运维人员只能通过外部监控（比如检查 EventStore 的写入指标是否停滞）来发现 Group 停摆。

**多源数据流图**

```
  MySQL-0             MySQL-1             MySQL-2
    │                   │                   │
    │ binlog            │ binlog            │ binlog
    ▼                   ▼                   ▼
  Parser0             Parser1             Parser2
    │                   │                   │
    │ dump线程0          │ dump线程1          │ dump线程2
    │ (独立位点管理)      │ (独立位点管理)      │ (独立位点管理)
    ▼                   ▼                   ▼
  ┌──────────────────────────────────────────────┐
  │           GroupEventSink                      │
  │                                               │
  │   await(ts=100) ─┐  await(ts=95) ─┐  await(ts=110) ─┐   │
  │                  │               │               │   │
  │                  ▼               ▼               ▼   │
  │            ┌─────────────────────────────┐        │
  │            │     TimelineBarrier          │        │
  │            │                              │        │
  │            │  lastTimestamps = [95,100,110]│        │
  │            │  (PriorityBlockingQueue)      │        │
  │            │                              │        │
  │            │  size(3) >= groupSize(3) ✓    │        │
  │            │  threshold = min = 95        │        │
  │            │                              │        │
  │            │  isPermit(95)  = true  → 放行 │        │
  │            │  isPermit(100) = false → 等待 │        │
  │            │  isPermit(110) = false → 等待 │        │
  │            └─────────────────────────────┘        │
  │                                               │
  │   ts=95 写入 EventStore → clear(95) → notify  │
  │   → threshold 更新 → ts=100 放行 → ...        │
  └──────────────────────────────────────────────┘
                    │
                    ▼
            MemoryEventStoreWithBuffer
            （时间有序的合并事件流）
                    │
                    ▼
              下游消费者
```

**TimelineBarrier 状态转换图**

```
  初始状态
  threshold = Long.MIN_VALUE
  lastTimestamps = []
        │
        ▼
  源1 提交 await(ts1)     lastTimestamps = [ts1]
        │                 size(1) < groupSize(3)
        │                 threshold 不变 → 源1 阻塞
        ▼
  源2 提交 await(ts2)     lastTimestamps = [ts1, ts2]
        │                 size(2) < groupSize(3)
        │                 threshold 不变 → 源1、2 阻塞
        ▼
  源3 提交 await(ts3)     lastTimestamps = [ts1, ts2, ts3]
        │                 size(3) >= groupSize(3) ✓
        │                 threshold = min(ts1, ts2, ts3)
        ▼
  最小时间戳的源放行      isPermit(min_ts) = true
        │
        ▼
  写入 EventStore → clear(min_ts)
  lastTimestamps = [剩余2个]
  notify() → 其他源重新检查
        │
        ▼
  等待该源提交下一个时间戳，凑齐 groupSize 后继续
```

**普通模式 vs Group 模式对比表**

| 对比项 | 普通模式（单源） | Group 模式（多源） |
|--------|-----------------|-------------------|
| Parser | 1 个 MysqlEventParser | GroupEventParser（N 个子 Parser） |
| Sink | EntryEventSink | GroupEventSink |
| 排序 | 天然有序（单源 binlog） | TimelineBarrier 时间戳排序 |
| 位点管理 | 1 个 MetaManager | N 个独立 MetaManager |
| 事务保证 | 事务内事件天然连续 | TimelineTransactionBarrier 保证 |
| 故障影响 | 只影响当前 destination | 一个源故障 → 所有源阻塞 |
| 超时处理 | dump 线程 catch-reconnect | 无有效超时（broken） |
| 配置复杂度 | instance.properties | group-instance.xml（Spring Bean 配置） |
| 适用场景 | 单库 | 分库分表合并 |

---

## 各案例涉及的代码路径总览

下表汇总了全部 14 个案例中的关键配置分支点。同一份 Canal 代码，不同的配置组合会走完全不同的代码路径。

| # | 判断条件 | 走 A 路径 | 走 B 路径 |
|---|---------|----------|----------|
| 1 | `canal.serverMode` = tcp | CanalServerWithNetty 启动 Netty TCP 监听 | MQ 模式：SPI 加载 CanalMQProducer |
| 2 | `canal.serverMode` = kafka / rocketmq / rabbitmq | ExtensionLoader 按 key 加载对应 MQ Producer | tcp 模式不加载任何 MQ 插件 |
| 3 | `canal.zkServers` 是否配置 | 有 ZK：创建 ZkClientx，HA 竞争 running 节点 | 无 ZK：单机模式直接 processActiveEnter() |
| 4 | `canal.instance.global.spring.xml` 文件选择 | file-instance.xml：FileMixedMetaManager（文件持久化） | default-instance.xml：PeriodMixedMetaManager（ZK 持久化） |
| 5 | `canal.instance.global.spring.xml` = group-instance.xml | GroupEventParser + GroupEventSink（多源合并） | 普通单源 MysqlEventParser + EntryEventSink |
| 6 | `canal.mq.flatMessage` | true：每个 Entry → FlatMessage JSON，批量发送 | false：整个 Message 序列化为二进制 blob |
| 7 | `canal.instance.detecting.enable` | true：主动 SQL 探测心跳（fork 独立连接） | false：被动心跳合成 HEARTBEAT Entry |
| 8 | `canal.instance.standby.switch.enable` | true：心跳失败达阈值触发 doSwitch() 主备切换 | false：只打日志不切换，依赖 dump 线程自愈 |
| 9 | `canal.instance.parser.parallel` | true：Disruptor 四阶段流水线（多线程解析） | false：串行解析（dump 线程做所有工作） |
| 10 | `filterTransactionEntry`（GroupEventSink） | true：TimelineBarrier（忽略事务边界） | false：TimelineTransactionBarrier（保留事务语义） |
| 11 | RocketMQ ACL（accessKey/secretKey 是否配置） | 有：AclClientRPCHook 注入 HMAC-SHA1 签名 | 无：rpcHook=null，不做认证 |
| 12 | EtlLock 锁类型（ZK 是否可用） | 有 ZK：InterProcessMutex 分布式锁 | 无 ZK：ReentrantLock 本地锁 |
| 13 | ETL 数据量 cnt ≥ 10000 | 多线程分页导入（线程数=CPU 核心数，pageSize=10000） | 单线程一次性导入 |
| 14 | ETL 目标存储类型 | RDB：DELETE+INSERT（JDBC 事务） | ES：Upsert（Bulk 非原子） |
| 15 | `canal.instance.global.mode` = spring / manager | spring：本地文件加载配置，lastModified 热更新 | manager：Canal Admin HTTP 加载，MD5 热更新 |
| 16 | `canal.auto.scan` = true | 启动 InstanceConfigMonitor 定时扫描 conf/ 目录 | 静态配置，不热更新 |
| 17 | `canal.instance.global.lazy` = true | 延迟启动：客户端首次 subscribe 时才创建 Instance | 非延迟：CanalController.start() 立即启动 |
| 18 | `canal.canalWithoutNetty` = true | MQ 模式下不启动 Netty TCP Server | null/false：启动 Netty TCP Server |
| 19 | Kafka partitionHash 是否配置 | 有：按表名/主键哈希分区 | 无：所有消息写入 partition=0 |
| 20 | RocketMQ `partitionsNum` > 0 | 使用配置值（不查询 NameServer） | ≤ 0：从 NameServer 动态查询队列数 |
| 21 | `canal.instance.tsdb.enable` = true | 启用 TableMetaTSDB 时间序列元数据缓存 | 禁用：每次 DML 从 MySQL 实时查询 TableMeta |
| 22 | SyncSwitch 状态（ETL 进行中） | off：AdapterProcessor.process() 阻塞在 BooleanMutex.get() | on：正常消费增量数据 |
