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
