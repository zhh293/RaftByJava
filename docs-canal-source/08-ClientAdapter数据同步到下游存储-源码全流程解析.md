# Client-Adapter 数据同步到下游存储 —— 源码全流程解析

> 基于源码项目 `canal/client-adapter` 逐步分析，从 Spring Boot 启动 Adapter 应用，到 SPI 动态加载适配器插件、消费 Canal Server 消息、分发到各 OuterAdapter 的 sync() 方法、最终写入 RDB/ES/HBase 等下游存储的全链路，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
JVM 启动
  |
  +-- 1. CanalAdapterApplication.main()
  |     -> Spring Boot 启动（排除 DataSourceAutoConfiguration）
  |     -> 自动扫描加载 @Component / @ConfigurationProperties
  |
  +-- 2. Spring 容器初始化
  |     +-- AdapterCanalConfig 被加载（@ConfigurationProperties(prefix = "canal.conf")）
  |     |     -> 解析 application.yml 中的 canal.conf 配置段
  |     |     -> 初始化 srcDataSources（DruidDataSource 连接池）
  |     |     -> 记录所有 destination 到 DESTINATIONS Set
  |     +-- SyncSwitch 被初始化（@PostConstruct）
  |     |     -> 本地模式: 每个 destination 一个 BooleanMutex
  |     |     -> 分布式模式: ZK 节点监听同步开关
  |
  +-- 3. CanalAdapterService.init()（@PostConstruct）
  |     -> syncSwitch.refresh()
  |     -> new CanalAdapterLoader(adapterCanalConfig)
  |     -> adapterLoader.init()
  |
  +-- 4. CanalAdapterLoader.init()（核心初始化）
  |     +-- ExtensionLoader.getExtensionLoader(OuterAdapter.class)
  |     |     -> 扫描 plugin/ 目录下所有 jar
  |     |     -> 解析 META-INF/canal/com.alibaba.otter.canal.client.adapter.OuterAdapter
  |     |     -> URLClassLoader 加载 SPI 实现类
  |     +-- 遍历配置 canalAdapters → groups → outerAdapters：
  |     |     +-- loadAdapter(config, canalOuterAdapters)
  |     |     |     -> loader.getExtension(name, key)  // SPI 获取实例
  |     |     |     -> new ProxyOuterAdapter(extension)
  |     |     |     -> adapter.init(config, envProperties)  // 初始化适配器
  |     |     |         +-- [RDB] RdbAdapter.init()
  |     |     |         |     -> ConfigLoader.load() 加载 rdb/*.yml 映射配置
  |     |     |         |     -> 创建 DruidDataSource 目标库连接池
  |     |     |         |     -> new RdbSyncService(dataSource, threads, skipDupException)
  |     |     |         +-- [ES]  ES7xAdapter.init()
  |     |     |         |     -> new ESConnection(hosts, properties, mode)
  |     |     |         |     -> new ES7xTemplate(esConnection)
  |     |     |         |     -> super.init() 加载 es7x/*.yml 映射配置
  |     |     |         +-- [HBase] HbaseAdapter.init()
  |     |     |               -> MappingConfigLoader.load() 加载 hbase/*.yml
  |     |     |               -> HBaseConfiguration.create() + new HbaseTemplate
  |     |     |               -> new HbaseSyncService(hbaseTemplate)
  |     |     +-- 创建 AdapterProcessor
  |     |     |     -> SPI 加载 CanalMsgConsumer (tcp/kafka/rocketMQ)
  |     |     |     -> canalMsgConsumer.init(properties, destination, groupId)
  |     |     +-- adapterProcessor.start()  // 启动消费线程
  |
  +-- 5. AdapterProcessor.process()（消费主循环）
  |     -> syncSwitch.get(destination)  // 等待同步开关打开
  |     -> canalMsgConsumer.connect()   // 连接 Canal Server 或 MQ
  |     -> while (running):
  |     |     +-- canalMsgConsumer.getMessage(timeout)  // 拉取消息
  |     |     +-- writeOut(commonMessages)              // 分发到适配器
  |     |     |     +-- MessageUtil.flatMessage2Dml()   // 消息 → Dml 列表
  |     |     |     +-- batchSync(dmls, adapter)        // 分批同步
  |     |     |     |     +-- adapter.sync(dmls)        // 调用适配器同步
  |     |     |     |         +-- [RDB]  rdbSyncService.sync(mappingConfigCache, dmls)
  |     |     |     |         |     -> appendDmlPartition()  // 按主键 hash 分区
  |     |     |     |         |     -> sync(batchExecutor, config, singleDml)
  |     |     |     |         |         -> insert() / update() / delete() / truncate()
  |     |     |     |         |         -> batchExecutor.execute(sql, values)
  |     |     |     |         |     -> batchExecutor.commit()
  |     |     |     |         +-- [ES]  esSyncService.sync()
  |     |     |     |         |     -> esTemplate.insert/update/delete()
  |     |     |     |         |     -> ES7xIndexRequest / ES7xUpdateRequest / ES7xDeleteRequest
  |     |     |     |         |     -> esBulkRequest.bulk()  // 批量提交
  |     |     |     |         +-- [HBase] hbaseSyncService.sync()
  |     |     |     |               -> insert() → HRow → hbaseTemplate.puts()
  |     |     |     |               -> update() → HRow → hbaseTemplate.puts()
  |     |     |     |               -> delete() → rowKeys → hbaseTemplate.deletes()
  |     |     +-- canalMsgConsumer.ack()   // 确认消费
  |     |     +-- [异常] canalMsgConsumer.rollback()  // 回滚
  |
  +-- 6. CanalAdapterService.destroy()（@PreDestroy）
        -> adapterLoader.destroy()
        |     -> 并行停止所有 AdapterProcessor
        |     -> AdapterProcessor.stop()
        |         -> running = false
        |         -> thread.join()
        |         -> outerAdapters.forEach(OuterAdapter::destroy)
        -> 关闭所有 DruidDataSource
```

---

## 一、Adapter 模块整体架构

### 1.1 Canal + Adapter 的完整数据管道

在深入源码之前，先理解 Canal Client-Adapter 在整个数据同步管道中的位置。Canal 的核心设计是一个**两阶段管道**：

```
                          数据变更采集（Canal Server）
 ┌──────────┐        ┌──────────────────────────────────┐
 │  MySQL   │        │         Canal Server              │
 │ (Master) │  ───►  │  Parser → Sink → Store            │
 │  binlog  │  dump  │  (模拟 slave 拉取并解析 binlog)     │
 └──────────┘        └────────────┬─────────────────────┘
                                  │
                                  │  Canal 协议 / Kafka / RocketMQ / RabbitMQ
                                  │
                          数据变更消费与写入（Client-Adapter）
                     ┌────────────▼───────────────────────────────┐
                     │            Canal Client-Adapter              │
                     │                                             │
                     │  ┌─────────────┐   ┌───────────────────┐   │
                     │  │ Adapter     │   │   OuterAdapter     │   │
                     │  │ Processor   │──►│   SPI 插件          │   │
                     │  │ (消费线程)   │   │                    │   │
                     │  └─────────────┘   │  ┌─── RdbAdapter  │   │
                     │                    │  ├─── ES7xAdapter  │   │
                     │                    │  ├─── HbaseAdapter │   │
                     │                    │  └─── ...          │   │
                     │                    └───────────────────┘   │
                     └────────────────────────────────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
        ┌──────────┐       ┌──────────┐        ┌──────────┐
        │  MySQL   │       │   ES     │        │  HBase   │
        │ PostgreSQL│       │ 7.x/8.x │        │          │
        │ Oracle   │       └──────────┘        └──────────┘
        └──────────┘
```

### 1.2 为什么需要 Adapter？

Canal Server 本身只负责**采集**——从 MySQL binlog 中解析出数据变更事件，存储到内部的 RingBuffer（EventStore）。Canal Client（SDK）负责从 Canal Server **拉取**这些事件。但拉取之后呢？

在没有 Adapter 之前，用户需要自己编写 Client 程序：

1. 创建 `CanalConnector`，连接 Canal Server
2. 调用 `getWithoutAck()` 拉取消息
3. 解析 `CanalEntry.RowChange`，提取出表名、列值、操作类型
4. 根据目标存储类型，手动拼装 SQL / ES 文档 / HBase Put 操作
5. 执行写入
6. 调用 `ack()` 确认

这个过程非常繁琐，且每个项目都要重复编写。**Adapter 的核心价值是将 "消费 → 转换 → 写入" 这条链路做成开箱即用的框架**，用户只需要编写 YAML 配置文件，声明源表与目标表的映射关系，Adapter 就能自动完成同步。

### 1.3 支持的下游存储

从源码中的 SPI 注解可以看到，Canal 内置了以下适配器：

| 适配器 | SPI 名称 | 说明 |
|--------|---------|------|
| `RdbAdapter` | `rdb` | 关系型数据库（MySQL、PostgreSQL、Oracle、SQL Server 等） |
| `ES7xAdapter` | `es7` | Elasticsearch 7.x |
| `ES6xAdapter` | `es6` | Elasticsearch 6.x |
| `ES8xAdapter` | `es8` | Elasticsearch 8.x |
| `HbaseAdapter` | `hbase` | Apache HBase |
| `LoggerAdapter` | `logger` | 日志输出（默认适配器，用于调试） |

此外，社区和阿里云版本还支持 ClickHouse、Kudu、Phoenix、TableStore 等。

### 1.4 Adapter 与 Canal Client 的关系

Adapter 并不直接使用传统的 `CanalConnector`。从 `AdapterProcessor` 的源码可以看到，它使用的是 `CanalMsgConsumer` SPI：

```java
// AdapterProcessor 构造函数
ExtensionLoader<CanalMsgConsumer> loader = new ExtensionLoader<>(CanalMsgConsumer.class);
String key = destination + "_" + groupId;
canalMsgConsumer = new ProxyCanalMsgConsumer(
    loader.getExtension(canalClientConfig.getMode().toLowerCase(),
        key, CONNECTOR_SPI_DIR, CONNECTOR_STANDBY_SPI_DIR));
```

`CanalMsgConsumer` 是一个更高层的抽象，根据 `mode` 配置（`tcp`/`kafka`/`rocketMQ`/`rabbitMQ`/`pulsarMQ`）加载不同的消费者实现。这意味着 Adapter 既支持直接连接 Canal Server（TCP 模式），也支持从消息队列消费。

---

## 二、Adapter 框架层设计

### 2.1 OuterAdapter —— SPI 接口定义

`OuterAdapter` 是所有外部适配器必须实现的核心接口。它定义了适配器的完整生命周期：

**源码文件**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/OuterAdapter.java`

```java
@SPI("logger")
public interface OuterAdapter {

    /**
     * 外部适配器初始化接口
     *
     * @param configuration 外部适配器配置信息
     * @param envProperties 环境变量的配置属性
     */
    void init(OuterAdapterConfig configuration, Properties envProperties);

    /**
     * 往适配器中同步数据
     *
     * @param dmls 数据包
     */
    void sync(List<Dml> dmls);

    /**
     * 外部适配器销毁接口
     */
    void destroy();

    /**
     * Etl操作 —— 全量同步
     */
    default EtlResult etl(String task, List<String> params) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    /**
     * 计算总数
     */
    default Map<String, Object> count(String task) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    /**
     * 通过task获取对应的destination
     */
    default String getDestination(String task) {
        return null;
    }
}
```

**设计要点**：

| 方法 | 作用 | 调用时机 |
|------|------|---------|
| `init()` | 初始化适配器（建连接、加载映射配置、创建同步服务） | 应用启动时，`CanalAdapterLoader.loadAdapter()` 调用 |
| `sync()` | 增量同步——将一批 Dml 写入目标存储 | 消费循环中，每次拉取消息后调用 |
| `destroy()` | 销毁适配器（关闭连接池、释放资源） | 应用停止时，`AdapterProcessor.stop()` 调用 |
| `etl()` | 全量同步——从源库读取全量数据写入目标存储 | REST API 手动触发 |
| `count()` | 查询目标存储中的记录总数 | REST API 手动触发 |

注意 `@SPI("logger")` 注解，它指定了默认的 SPI 实现名称。当没有显式指定适配器名称时，将使用名为 "logger" 的实现。

### 2.2 Dml 数据结构 —— 标准化的数据变更描述

`Dml`（Data Manipulation Language）是 Adapter 内部流转的核心数据结构，它将 Canal 原始的 `CanalEntry.RowChange` 或 MQ 的 `CommonMessage` 统一转换为一种标准化格式：

**源码文件**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/Dml.java`

```java
public class Dml implements Serializable {

    private String                    destination;  // 对应 canal 的实例或 MQ 的 topic
    private String                    groupId;      // 对应 MQ 的 group id
    private String                    database;     // 数据库或 schema
    private String                    table;        // 表名
    private List<String>              pkNames;      // 主键字段名列表
    private Boolean                   isDdl;        // 是否为 DDL 语句
    private String                    type;         // 类型: INSERT / UPDATE / DELETE
    private Long                      es;           // binlog executeTime
    private Long                      ts;           // dml build timeStamp
    private String                    sql;          // 执行的 sql, DML 时为空
    private List<Map<String, Object>> data;         // 数据列表（当前值）
    private List<Map<String, Object>> old;          // 旧数据列表（仅 UPDATE 时有值）
    // ...
}
```

**字段详解**：

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `destination` | String | Canal 实例名或 MQ topic | `"example"` |
| `groupId` | String | MQ 消费组 ID | `"g1"` |
| `database` | String | 源库名 | `"mydb"` |
| `table` | String | 源表名 | `"user"` |
| `pkNames` | List\<String\> | 主键列名列表 | `["id"]` |
| `isDdl` | Boolean | 是否为 DDL | `false` |
| `type` | String | DML 操作类型 | `"INSERT"` / `"UPDATE"` / `"DELETE"` |
| `es` | Long | binlog 中记录的执行时间戳 | `1697000000000` |
| `ts` | Long | Adapter 处理时间戳 | `1697000001000` |
| `sql` | String | DDL 语句内容（DML 时为 null） | `"ALTER TABLE ..."` |
| `data` | List\<Map\> | 当前行数据（INSERT: 新值, UPDATE: 新值, DELETE: 被删除的值） | `[{"id":1,"name":"张三"}]` |
| `old` | List\<Map\> | 旧值（仅 UPDATE 时有值，记录被修改的字段的旧值） | `[{"name":"李四"}]` |

**data 和 old 的对应关系**：

```
INSERT: data = [新行数据],           old = null
UPDATE: data = [当前行完整数据],     old = [被修改字段的旧值]  （size 一一对应）
DELETE: data = [被删除行的完整数据], old = null
```

### 2.3 MessageUtil —— 消息到 Dml 的转换

`MessageUtil` 是连接 Canal 消息协议和 Adapter 内部数据模型的桥梁。它提供了两种转换路径：

**源码文件**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/MessageUtil.java`

#### 2.3.1 TCP 模式：CanalEntry → Dml

```java
public static List<Dml> parse4Dml(String destination, String groupId, Message message) {
    List<CanalEntry.Entry> entries = message.getEntries();
    List<Dml> dmls = new ArrayList<>(entries.size());
    for (CanalEntry.Entry entry : entries) {
        // 跳过事务开始和结束标记
        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
            || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
            continue;
        }

        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        CanalEntry.EventType eventType = rowChange.getEventType();

        final Dml dml = new Dml();
        dml.setIsDdl(rowChange.getIsDdl());
        dml.setDestination(destination);
        dml.setDatabase(entry.getHeader().getSchemaName());
        dml.setTable(entry.getHeader().getTableName());
        dml.setType(eventType.toString());
        dml.setEs(entry.getHeader().getExecuteTime());
        dml.setTs(System.currentTimeMillis());
        dml.setSql(rowChange.getSql());
        // ... 遍历 RowData，填充 data 和 old
    }
    return dmls;
}
```

**转换过程**：

```
CanalEntry.Entry
  |-- header.schemaName   → dml.database
  |-- header.tableName    → dml.table
  |-- header.executeTime  → dml.es
  |-- storeValue → RowChange
       |-- isDdl          → dml.isDdl
       |-- sql            → dml.sql
       |-- eventType      → dml.type (INSERT/UPDATE/DELETE)
       |-- rowDatasList   → 遍历每行
            |-- afterColumnsList  → data (INSERT/UPDATE)
            |-- beforeColumnsList → data (DELETE) / old (UPDATE 中被修改字段的旧值)
```

对于 UPDATE 操作，有一个精妙的细节：

```java
// 获取 update 为 true 的字段
if (column.getUpdated()) {
    updateSet.add(column.getName());
}
// ...
// update 操作将记录修改前的值（只记录被修改的字段）
for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
    if (updateSet.contains(column.getName())) {
        rowOld.put(column.getName(), ...);
    }
}
```

只有 `getUpdated() == true` 的字段才会被放入 `old` 列表。这样 old 中只包含**实际发生变化的字段的旧值**，而不是所有字段。

#### 2.3.2 MQ 模式：CommonMessage → Dml

```java
public static List<Dml> flatMessage2Dml(String destination, String groupId,
                                         List<CommonMessage> commonMessages) {
    List<Dml> dmls = new ArrayList<>(commonMessages.size());
    for (CommonMessage commonMessage : commonMessages) {
        Dml dml = flatMessage2Dml(destination, groupId, commonMessage);
        if (dml != null) {
            dmls.add(dml);
        }
    }
    return dmls;
}
```

MQ 模式下的 `CommonMessage` 已经是扁平化的结构，转换非常直接——字段一一对应复制即可。

### 2.4 SPI 加载机制 —— ExtensionLoader 深度解析

Canal Adapter 的 SPI 机制借鉴了 Dubbo 的 `ExtensionLoader` 设计，但做了定制化改造。这是整个插件化架构的基石。

**源码文件**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/ExtensionLoader.java`

#### 2.4.1 核心数据结构

```java
public class ExtensionLoader<T> {

    // SPI 描述文件扫描路径
    private static final String SERVICES_DIRECTORY = "META-INF/services/";
    private static final String CANAL_DIRECTORY    = "META-INF/canal/";

    // 全局缓存：接口类型 → ExtensionLoader 实例（避免重复创建）
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS
        = new ConcurrentHashMap<>();

    // 全局缓存：实现类 → 单例实例
    private static final ConcurrentMap<Class<?>, Object> EXTENSION_INSTANCES
        = new ConcurrentHashMap<>();

    // 支持 key 隔离的实例缓存（同一个 name 可以创建多个实例）
    private static final ConcurrentMap<String, Object> EXTENSION_KEY_INSTANCE
        = new ConcurrentHashMap<>();

    // 当前 loader 负责的接口类型
    private final Class<?> type;

    // 类加载器策略：internal（子优先）/ external（父优先）
    private final String classLoaderPolicy;

    // 缓存：名称 → 已加载的 Class
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();

    // 缓存：名称 → 已创建的实例
    private final ConcurrentMap<String, Holder<Object>> cachedInstances
        = new ConcurrentHashMap<>();
}
```

#### 2.4.2 加载流程

```
ExtensionLoader.getExtensionLoader(OuterAdapter.class)
  |
  +-- 1. 校验：type 必须是接口，且标注了 @SPI 注解
  |
  +-- 2. 从 EXTENSION_LOADERS 缓存中查找，没有则创建新的 ExtensionLoader
  |
  +-- 3. 调用 getExtension(name, key)
       |
       +-- 从 cachedInstances 中查找（double-check locking）
       |
       +-- 未找到则调用 createExtension(name, key)
            |
            +-- getExtensionClasses()  // 首次调用时触发类扫描
            |    |
            |    +-- loadExtensionClasses()
            |         |
            |         +-- 解析 @SPI 注解的 value 作为默认实现名
            |         |
            |         +-- 扫描 plugin/ 目录下所有 .jar 文件
            |         |    |
            |         |    +-- 为每个 jar 创建独立的 ClassLoader：
            |         |    |   - internal 策略: URLClassExtensionLoader（子优先加载）
            |         |    |   - external 策略: 普通 URLClassLoader（父优先加载）
            |         |    |
            |         |    +-- loadFile(extensionClasses, "META-INF/canal/", classLoader)
            |         |    +-- loadFile(extensionClasses, "META-INF/services/", classLoader)
            |         |
            |         +-- 返回 Map<name, Class>
            |
            +-- clazz.newInstance()  // 反射创建实例
            |
            +-- 缓存到 EXTENSION_KEY_INSTANCE
```

#### 2.4.3 SPI 描述文件格式

SPI 描述文件位于 jar 包中的 `META-INF/canal/com.alibaba.otter.canal.client.adapter.OuterAdapter`，内容格式如下：

```properties
rdb=com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter
```

等号左边是 SPI 名称（与配置中的 `name` 字段对应），右边是实现类的全限定名。

`loadFile()` 方法逐行解析该文件：

```java
private void loadFile(Map<String, Class<?>> extensionClasses,
                       String dir, ClassLoader classLoader) {
    String fileName = dir + type.getName();
    Enumeration<URL> urls = classLoader.getResources(fileName);
    while (urls.hasMoreElements()) {
        URL url = urls.nextElement();
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(url.openStream(), StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            // 去掉注释
            final int ci = line.indexOf('#');
            if (ci >= 0) line = line.substring(0, ci);
            line = line.trim();
            if (line.length() > 0) {
                // 解析 name=className
                String name = null;
                int i = line.indexOf('=');
                if (i > 0) {
                    name = line.substring(0, i).trim();
                    line = line.substring(i + 1).trim();
                }
                // 用当前 jar 的 ClassLoader 加载类
                Class<?> clazz = classLoader.loadClass(line);
                // 验证是否实现了目标接口
                if (!type.isAssignableFrom(clazz)) {
                    throw new IllegalStateException("...");
                }
                extensionClasses.put(name, clazz);
            }
        }
    }
}
```

#### 2.4.4 URLClassExtensionLoader —— 子优先类加载策略

Canal 设计了自定义的 `URLClassExtensionLoader`，它**打破了双亲委派模型**，实现了子优先（child-first）的类加载策略：

**源码文件**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/URLClassExtensionLoader.java`

```java
public class URLClassExtensionLoader extends URLClassLoader {

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        Class<?> c = findLoadedClass(name);
        if (c != null) return c;

        // 这些基础类仍然委托给父加载器
        if (name.startsWith("java.") || name.startsWith("org.slf4j.")
            || name.startsWith("org.apache.logging")
            || name.startsWith("org.apache.zookeeper.")
            || name.startsWith("com.alibaba.druid")) {
            c = super.loadClass(name);
        }
        if (c != null) return c;

        try {
            // 先从自己的 jar 中查找 —— 子优先！
            c = findClass(name);
        } catch (ClassNotFoundException e) {
            c = null;
        }
        if (c != null) return c;

        // 最后再委托给父加载器
        return super.loadClass(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        // 只返回本 jar 中的资源，不返回父加载器的
        Enumeration<URL>[] tmp = new Enumeration[2];
        tmp[0] = findResources(name);
        return new CompoundEnumeration<>(tmp);
    }
}
```

**为什么需要子优先加载？**

每个 adapter 插件 jar 可能依赖不同版本的第三方库（例如 ES 7.x adapter 依赖 ES 7.x client，ES 6.x adapter 依赖 ES 6.x client）。如果使用标准的双亲委派，父 ClassLoader 加载的版本会覆盖子的，导致版本冲突。子优先加载确保每个插件 jar 优先使用自己携带的依赖版本。

白名单中的类（`java.*`、`org.slf4j.*`、`com.alibaba.druid`）是框架层共享的基础依赖，必须使用同一个版本，所以仍然委托给父加载器。

### 2.5 @SPI 注解

```java
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface SPI {
    // 默认 SPI 实现名称
    String value() default "";
}
```

`@SPI` 注解标注在接口上，用于：
1. 标识该接口是一个可扩展的 SPI 接口
2. 指定默认实现的名称（`value` 属性）

例如 `@SPI("logger")` 表示 `OuterAdapter` 的默认实现是名为 `logger` 的适配器。

### 2.6 Adapter 配置体系

#### 2.6.1 CanalClientConfig —— 全局配置模型

**源码文件**: `client-adapter/common/src/main/java/com/alibaba/otter/canal/client/adapter/support/CanalClientConfig.java`

这是 Adapter 的**全局配置根类**，对应 `application.yml` 中 `canal.conf` 配置段的全部字段：

```java
public class CanalClientConfig {
    private String  canalServerHost;    // 单机模式下 canal server 的 ip:port
    private String  zookeeperHosts;     // 集群模式下的 zk 地址
    private String  mqServers;          // Kafka/RocketMQ 地址
    private Boolean flatMessage = true; // 是否以 flatMessage 模式传输（仅 MQ 模式）
    private Integer batchSize;          // 批大小
    private Integer syncBatchSize = 1000; // 同步分批提交大小
    private Integer retries;            // 重试次数
    private Long    timeout = 500L;     // 消费超时时间(ms)
    private String  mode = "tcp";       // 模式: tcp / kafka / rocketMQ / rabbitMQ
    private Boolean terminateOnException = false; // 异常时是否终止同步

    // 嵌套配置
    private List<CanalAdapter> canalAdapters;  // 适配器实例列表

    // 内部类 CanalAdapter
    public static class CanalAdapter {
        private String      instance;  // canal 实例名或 MQ topic
        private List<Group> groups;    // 适配器分组列表
    }

    // 内部类 Group
    public static class Group {
        private String                   groupId = "default";
        private List<OuterAdapterConfig> outerAdapters;          // 适配器列表
        private Map<String, OuterAdapterConfig> outerAdaptersMap; // 适配器 Map
    }
}
```

#### 2.6.2 OuterAdapterConfig —— 单个适配器的配置

```java
public class OuterAdapterConfig {
    private String              name;       // 适配器名称: logger, hbase, es7, rdb
    private String              key;        // 适配器唯一键（用于区分同名适配器的不同实例）
    private String              hosts;      // 适配器连接地址（如 ES 的 http://localhost:9200）
    private String              zkHosts;    // 适配器内部的 ZK 地址（如 HBase 的 ZK）
    private Map<String, String> properties; // 其余参数（如 jdbc.url, jdbc.username 等）
}
```

#### 2.6.3 AdapterCanalConfig —— Spring 配置绑定

**源码文件**: `client-adapter/launcher/src/main/java/com/alibaba/otter/canal/adapter/launcher/config/AdapterCanalConfig.java`

```java
@Component
@ConfigurationProperties(prefix = "canal.conf")
public class AdapterCanalConfig extends CanalClientConfig {

    public final Set<String> DESTINATIONS = new LinkedHashSet<>();

    private Map<String, DatasourceConfig> srcDataSources;

    @Override
    public void setCanalAdapters(List<CanalAdapter> canalAdapters) {
        super.setCanalAdapters(canalAdapters);
        // 收集所有 destination
        if (canalAdapters != null) {
            synchronized (DESTINATIONS) {
                DESTINATIONS.clear();
                for (CanalAdapter canalAdapter : canalAdapters) {
                    if (canalAdapter.getInstance() != null) {
                        DESTINATIONS.add(canalAdapter.getInstance());
                    }
                }
            }
        }
    }

    public void setSrcDataSources(Map<String, DatasourceConfig> srcDataSources) {
        this.srcDataSources = srcDataSources;
        // 为每个源数据源创建 Druid 连接池
        if (srcDataSources != null) {
            for (Map.Entry<String, DatasourceConfig> entry : srcDataSources.entrySet()) {
                DruidDataSource ds = new DruidDataSource();
                ds.setDriverClassName(entry.getValue().getDriver());
                ds.setUrl(entry.getValue().getUrl());
                ds.setUsername(entry.getValue().getUsername());
                ds.setPassword(entry.getValue().getPassword());
                ds.setInitialSize(1);
                ds.setMinIdle(1);
                ds.setMaxActive(entry.getValue().getMaxActive());
                ds.init();
                DatasourceConfig.DATA_SOURCES.put(entry.getKey(), ds);
            }
        }
    }
}
```

这个类通过 Spring Boot 的 `@ConfigurationProperties` 自动将 YAML 配置映射到 Java 对象。当 `srcDataSources` 被设置时，会立即为每个源数据源创建 Druid 连接池，存入全局的 `DatasourceConfig.DATA_SOURCES` 静态 Map 中。

#### 2.6.4 application.yml 配置结构示例

```yaml
canal.conf:
  mode: tcp                           # 消费模式
  canalServerHost: 127.0.0.1:11111    # Canal Server 地址
  batchSize: 500
  syncBatchSize: 1000
  retries: 0
  timeout: 1000

  srcDataSources:                     # 源数据源（用于全量 ETL）
    defaultDS:
      url: jdbc:mysql://127.0.0.1:3306/mydb?useUnicode=true
      username: root
      password: 123456

  canalAdapters:                      # 适配器列表
    - instance: example               # canal 实例名
      groups:
        - groupId: g1
          outerAdapters:
            - name: rdb               # 适配器类型
              key: oracle1            # 适配器唯一标识
              properties:
                jdbc.driverClassName: oracle.jdbc.OracleDriver
                jdbc.url: jdbc:oracle:thin:@localhost:1521:orcl
                jdbc.username: test
                jdbc.password: test
            - name: es7
              key: esKey1
              hosts: http://127.0.0.1:9200
              properties:
                mode: rest
                cluster.name: elasticsearch
```

#### 2.6.5 MappingConfigsLoader —— 映射配置文件加载

每种 adapter 都有自己的映射配置文件目录（如 `conf/rdb/`、`conf/es7/`、`conf/hbase/`）。`MappingConfigsLoader` 负责从这些目录中加载 `.yml` 文件：

```java
public class MappingConfigsLoader {

    public static Map<String, String> loadConfigs(String name) {
        Map<String, String> configContentMap = new HashMap<>();
        // 先取本地文件（../conf/{name}/），再取类路径
        File configDir = new File(".." + File.separator + "conf" + File.separator + name);
        if (!configDir.exists()) {
            URL url = MappingConfigsLoader.class.getClassLoader().getResource("");
            if (url != null) {
                configDir = new File(url.getPath() + name + File.separator);
            }
        }
        File[] files = configDir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.getName().endsWith(".yml")) continue;
                // 读取文件内容
                byte[] bytes = new byte[in.available()];
                in.read(bytes);
                configContentMap.put(file.getName(), new String(bytes, StandardCharsets.UTF_8));
            }
        }
        return configContentMap;
    }
}
```

**查找路径优先级**：`../conf/{adapterName}/` → classpath:`{adapterName}/`

### 2.7 DatasourceConfig —— 全局数据源注册中心

```java
public class DatasourceConfig {
    // 全局数据源容器：key → DruidDataSource
    public final static Map<String, DruidDataSource> DATA_SOURCES = new ConcurrentHashMap<>();

    private String  driver    = "com.mysql.jdbc.Driver";
    private String  url;
    private String  database;
    private String  type      = "mysql";
    private String  username;
    private String  password;
    private Integer maxActive = 3;
}
```

`DATA_SOURCES` 是一个静态的全局注册中心，所有源数据源（在 `application.yml` 的 `srcDataSources` 段配置）都会被注册到这里。ETL 全量同步时，adapter 通过 `DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey())` 获取源库连接。

### 2.8 EtlResult —— 全量同步结果

```java
public class EtlResult implements Serializable {
    private boolean succeeded = false;
    private String  resultMessage;
    private String  errorMessage;
}
```

---

## 三、Launcher 启动流程

### 3.1 CanalAdapterApplication —— Spring Boot 启动入口

**源码文件**: `client-adapter/launcher/src/main/java/com/alibaba/otter/canal/adapter/launcher/CanalAdapterApplication.java`

```java
@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class CanalAdapterApplication {

    public static void main(String[] args) {
        // 支持 RocketMQ client 配置日志路径
        System.setProperty("rocketmq.client.logUseSlf4j", "true");

        SpringApplication application = new SpringApplication(CanalAdapterApplication.class);
        application.setBannerMode(Banner.Mode.OFF);
        application.run(args);
    }
}
```

**关键细节**：

1. **排除 DataSourceAutoConfiguration**：Adapter 管理自己的数据源（Druid），不需要 Spring Boot 自动配置
2. **关闭 Banner**：生产级应用，不需要启动 Banner
3. **RocketMQ 日志配置**：确保 RocketMQ 客户端使用 SLF4J 而不是自带的日志实现

### 3.2 CanalAdapterService —— 适配器启动业务类

**源码文件**: `client-adapter/launcher/src/main/java/com/alibaba/otter/canal/adapter/launcher/loader/CanalAdapterService.java`

这是 Adapter 启动的核心协调类，通过 Spring 的 `@PostConstruct` / `@PreDestroy` 管理生命周期：

```java
@Component
@RefreshScope
public class CanalAdapterService {

    private CanalAdapterLoader adapterLoader;

    @Resource
    private AdapterCanalConfig adapterCanalConfig;
    @Resource
    private SyncSwitch         syncSwitch;

    private volatile boolean running = false;

    @PostConstruct
    public synchronized void init() {
        if (running) return;
        try {
            // 1. 刷新同步开关
            syncSwitch.refresh();

            // 2. 创建 CanalAdapterLoader 并初始化
            adapterLoader = new CanalAdapterLoader(adapterCanalConfig);
            adapterLoader.init();

            running = true;
            logger.info("## the canal client adapters are running now ......");
        } catch (Exception e) {
            logger.error("## something goes wrong when starting up the canal client adapters:", e);
        }
    }

    @PreDestroy
    public synchronized void destroy() {
        if (!running) return;
        try {
            running = false;
            // 1. 停止 adapterLoader
            if (adapterLoader != null) {
                adapterLoader.destroy();
                adapterLoader = null;
            }
            // 2. 关闭所有源数据源连接池
            for (DruidDataSource druidDataSource : DatasourceConfig.DATA_SOURCES.values()) {
                druidDataSource.close();
            }
            DatasourceConfig.DATA_SOURCES.clear();
        } catch (Throwable e) {
            logger.warn("## something goes wrong when stopping canal client adapters:", e);
        }
    }
}
```

**注意 `@RefreshScope`**：支持 Spring Cloud Config 的配置刷新。当配置变更时，这个 Bean 会被重新创建，触发 `destroy()` + `init()`，实现热更新。

### 3.3 CanalAdapterLoader —— 适配器加载器

**源码文件**: `client-adapter/launcher/src/main/java/com/alibaba/otter/canal/adapter/launcher/loader/CanalAdapterLoader.java`

这是**整个 Adapter 框架最核心的类之一**，它负责：

1. 通过 SPI 加载所有 OuterAdapter 实现
2. 初始化每个 adapter 实例
3. 为每个 destination+group 创建消费线程（AdapterProcessor）

```java
public class CanalAdapterLoader {

    private CanalClientConfig             canalClientConfig;
    private Map<String, AdapterProcessor> canalAdapterProcessors = new HashMap<>();
    private ExtensionLoader<OuterAdapter> loader;

    public void init() {
        // 1. 获取 OuterAdapter 的 SPI 加载器
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);

        // 2. 遍历配置结构：canalAdapters → groups → outerAdapters
        for (CanalClientConfig.CanalAdapter canalAdapter : canalClientConfig.getCanalAdapters()) {
            for (CanalClientConfig.Group group : canalAdapter.getGroups()) {
                int autoGenId = 0;
                List<List<OuterAdapter>> canalOuterAdapterGroups = new CopyOnWriteArrayList<>();
                List<OuterAdapter> canalOuterAdapters = new CopyOnWriteArrayList<>();

                for (OuterAdapterConfig config : group.getOuterAdapters()) {
                    // 保证每个 adapter 一定有 key
                    if (StringUtils.isEmpty(config.getKey())) {
                        String key = StringUtils.join(
                            new String[] { Util.AUTO_GENERATED_PREFIX,
                                           canalAdapter.getInstance(),
                                           group.getGroupId(),
                                           String.valueOf(autoGenId) }, '-');
                        config.setKey(key);
                    }
                    autoGenId++;
                    // 3. 加载并初始化单个 adapter
                    loadAdapter(config, canalOuterAdapters);
                }
                canalOuterAdapterGroups.add(canalOuterAdapters);

                // 4. 校验：所有 adapter 必须全部初始化成功
                if (CollectionUtils.isEmpty(canalOuterAdapters)
                    || canalOuterAdapters.size() != group.getOuterAdapters().size()) {
                    throw new RuntimeException("Load OuterAdapters is Empty");
                }

                // 5. 创建 AdapterProcessor 并启动消费线程
                AdapterProcessor adapterProcessor = canalAdapterProcessors.computeIfAbsent(
                    canalAdapter.getInstance() + "|" + StringUtils.trimToEmpty(group.getGroupId()),
                    f -> new AdapterProcessor(canalClientConfig,
                        canalAdapter.getInstance(),
                        group.getGroupId(),
                        canalOuterAdapterGroups));
                adapterProcessor.start();
            }
        }
    }
}
```

**loadAdapter 方法**——加载并初始化单个适配器：

```java
private void loadAdapter(OuterAdapterConfig config, List<OuterAdapter> canalOutConnectors) {
    try {
        // 1. 通过 SPI 获取 adapter 实例
        OuterAdapter adapter;
        adapter = new ProxyOuterAdapter(
            loader.getExtension(config.getName(), config.getKey()));

        // 2. 收集 Spring 环境变量
        Environment env = (Environment) SpringContext.getBean(Environment.class);
        Properties evnProperties = null;
        if (env instanceof StandardEnvironment) {
            evnProperties = new Properties();
            for (PropertySource<?> propertySource :
                 ((StandardEnvironment) env).getPropertySources()) {
                if (propertySource instanceof EnumerablePropertySource) {
                    String[] names = ((EnumerablePropertySource<?>) propertySource)
                        .getPropertyNames();
                    for (String name : names) {
                        Object val = env.getProperty(name);
                        if (val != null) {
                            evnProperties.put(name, val);
                        }
                    }
                }
            }
        }

        // 3. 初始化 adapter
        adapter.init(config, evnProperties);

        // 4. 加入 adapter 列表
        canalOutConnectors.add(adapter);
        logger.info("Load canal adapter: {} succeed", config.getName());
    } catch (Exception e) {
        logger.error("Load canal adapter: {} failed", config.getName(), e);
    }
}
```

**注意**：`loader.getExtension(config.getName(), config.getKey())` 使用了两个参数。`name` 决定加载哪个 SPI 实现类（如 `"rdb"`），`key` 用于创建该实现的独立实例。这样同一个 `RdbAdapter` 类可以创建多个实例，每个实例连接不同的目标库。

**销毁流程**：

```java
public void destroy() {
    if (!canalAdapterProcessors.isEmpty()) {
        // 并行销毁所有 processor（避免串行阻塞）
        ExecutorService stopExecutorService =
            Executors.newFixedThreadPool(canalAdapterProcessors.size());
        for (AdapterProcessor adapterProcessor : canalAdapterProcessors.values()) {
            stopExecutorService.execute(adapterProcessor::stop);
        }
        stopExecutorService.shutdown();
        while (!stopExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
            // 等待所有 processor 停止
        }
    }
}
```

### 3.4 AdapterProcessor —— 消费主循环

**源码文件**: `client-adapter/launcher/src/main/java/com/alibaba/otter/canal/adapter/launcher/loader/AdapterProcessor.java`

`AdapterProcessor` 是每个 destination+group 对应的消费处理器，它在独立线程中运行消费循环。

#### 3.4.1 构造函数

```java
public AdapterProcessor(CanalClientConfig canalClientConfig, String destination,
                         String groupId, List<List<OuterAdapter>> canalOuterAdapters) {
    this.canalClientConfig = canalClientConfig;
    this.canalDestination = destination;
    this.groupId = groupId;
    this.canalOuterAdapters = canalOuterAdapters;

    // 组内工作线程池
    this.groupInnerExecutorService =
        Util.newFixedThreadPool(canalOuterAdapters.size(), 5000L);

    // 获取同步开关
    syncSwitch = (SyncSwitch) SpringContext.getBean(SyncSwitch.class);

    // SPI 加载消息消费者（tcp/kafka/rocketMQ/rabbitMQ）
    ExtensionLoader<CanalMsgConsumer> loader =
        new ExtensionLoader<>(CanalMsgConsumer.class);
    String key = destination + "_" + groupId;
    canalMsgConsumer = new ProxyCanalMsgConsumer(
        loader.getExtension(canalClientConfig.getMode().toLowerCase(),
            key, CONNECTOR_SPI_DIR, CONNECTOR_STANDBY_SPI_DIR));

    // 初始化消费者
    Properties properties = canalClientConfig.getConsumerProperties();
    properties.put(CanalConstants.CANAL_MQ_FLAT_MESSAGE, canalClientConfig.getFlatMessage());
    properties.put(CanalConstants.CANAL_ALIYUN_ACCESS_KEY, canalClientConfig.getAccessKey());
    properties.put(CanalConstants.CANAL_ALIYUN_SECRET_KEY, canalClientConfig.getSecretKey());
    canalMsgConsumer.init(properties, canalDestination, groupId);
}
```

#### 3.4.2 消费主循环 process()

```java
private void process() {
    // 计算重试次数
    int retry = canalClientConfig.getRetries() == null
                || canalClientConfig.getRetries() == 0 ? 1 : canalClientConfig.getRetries();
    if (retry == -1) {
        retry = Integer.MAX_VALUE; // -1 表示无限重试
    }

    while (running) {
        try {
            // 1. 等待同步开关打开
            syncSwitch.get(canalDestination);

            // 2. 连接消息源
            canalMsgConsumer.connect();

            // 3. 消费循环
            out: while (running) {
                // 检查同步开关（带超时，支持动态关闭）
                try {
                    syncSwitch.get(canalDestination, 1L, TimeUnit.MINUTES);
                } catch (TimeoutException e) {
                    break;
                }

                // 4. 重试循环
                for (int i = 0; i < retry; i++) {
                    try {
                        // 5. 拉取消息
                        List<CommonMessage> commonMessages =
                            canalMsgConsumer.getMessage(timeout, TimeUnit.MILLISECONDS);

                        // 6. 写出到适配器
                        writeOut(commonMessages);

                        // 7. 确认消费
                        canalMsgConsumer.ack();
                        break; // 成功则跳出重试循环

                    } catch (Exception e) {
                        // 处理连接级错误（断开重连）
                        Throwable th = e.getCause();
                        if (th instanceof CanalClientException) {
                            String message = ExceptionUtils.getRootCauseMessage(th);
                            if (message.contains("end of stream")
                                || message.contains("Connection reset")
                                || message.contains("Broken pipe")) {
                                break out; // 跳出消费循环，重连
                            }
                        }

                        // 处理写入级错误
                        if (i != retry - 1) {
                            canalMsgConsumer.rollback(); // 回滚，重试
                        } else {
                            if (canalClientConfig.getTerminateOnException()) {
                                canalMsgConsumer.rollback();
                                syncSwitch.off(canalDestination); // 关闭同步
                            } else {
                                canalMsgConsumer.ack(); // 跳过，强制 ACK
                            }
                        }
                        Thread.sleep(500);
                    }
                }
            }
            canalMsgConsumer.disconnect();
        } catch (Throwable e) {
            logger.error("process error!", e);
        }

        // 重连等待
        if (running) {
            Thread.sleep(1000);
        }
    }
}
```

**消费循环的错误处理策略**：

```
getMessage() / writeOut() 异常
  |
  +-- 连接级异常（end of stream / Connection reset / Broken pipe）
  |     -> break out —— 退出消费循环，外层循环重新 connect()
  |
  +-- 写入级异常
       |
       +-- 非最后一次重试
       |     -> rollback() + 继续重试
       |
       +-- 最后一次重试
            |
            +-- terminateOnException = true
            |     -> rollback() + syncSwitch.off() 关闭同步
            |
            +-- terminateOnException = false
                  -> ack() 跳过该批消息，继续消费
```

#### 3.4.3 writeOut() —— 分发到适配器

```java
public void writeOut(final List<CommonMessage> commonMessages) {
    List<Future<Boolean>> futures = new ArrayList<>();

    // 组间适配器并行运行
    canalOuterAdapters.forEach(outerAdapters -> {
        futures.add(groupInnerExecutorService.submit(() -> {
            try {
                // 组内适配器串行运行
                outerAdapters.forEach(adapter -> {
                    List<Dml> dmls = MessageUtil.flatMessage2Dml(
                        canalDestination, groupId, commonMessages);
                    batchSync(dmls, adapter);
                });
                return true;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
        }));

        // 等待所有组完成
        RuntimeException exception = null;
        for (Future<Boolean> future : futures) {
            if (!future.get()) {
                exception = new RuntimeException("Outer adapter sync failed!");
            }
        }
        if (exception != null) throw exception;
    });
}
```

**并行模型**：

```
canalOuterAdapters = [[adapter1, adapter2], [adapter3, adapter4]]
                       |--- Group 1 ---|     |--- Group 2 ---|
                       
Group 1 和 Group 2 并行执行（通过 groupInnerExecutorService）
Group 内的 adapter1 和 adapter2 串行执行
```

#### 3.4.4 batchSync() —— 分批同步

```java
private void batchSync(List<Dml> dmls, OuterAdapter adapter) {
    if (dmls.size() <= canalClientConfig.getSyncBatchSize()) {
        // 总量不超过批次大小，一次性同步
        adapter.sync(dmls);
    } else {
        // 分批同步
        int len = 0;
        List<Dml> dmlsBatch = new ArrayList<>();
        for (Dml dml : dmls) {
            dmlsBatch.add(dml);
            if (dml.getData() == null || dml.getData().isEmpty()) {
                len += 1;
            } else {
                len += dml.getData().size();  // 按行数计算
            }
            if (len >= canalClientConfig.getSyncBatchSize()) {
                adapter.sync(dmlsBatch);
                dmlsBatch.clear();
                len = 0;
            }
        }
        if (!dmlsBatch.isEmpty()) {
            adapter.sync(dmlsBatch);
        }
    }
}
```

**注意**：分批大小 `syncBatchSize` 是按**行数**计算的，而不是按 Dml 对象数量。一个 Dml 可能包含多行数据（`data.size() > 1`），所以这里用 `dml.getData().size()` 来计算。

### 3.5 SyncSwitch —— 同步开关

**源码文件**: `client-adapter/launcher/src/main/java/com/alibaba/otter/canal/adapter/launcher/common/SyncSwitch.java`

同步开关支持**本地模式**和**分布式模式**（基于 ZooKeeper）：

```java
@Component
public class SyncSwitch {

    private static final String SYN_SWITCH_ZK_NODE = "/sync-switch/";

    private static final Map<String, BooleanMutex> LOCAL_LOCK       = new ConcurrentHashMap<>();
    private static final Map<String, BooleanMutex> DISTRIBUTED_LOCK = new ConcurrentHashMap<>();

    private Mode mode = Mode.LOCAL;

    @PostConstruct
    public void init() {
        CuratorFramework curator = curatorClient.getCurator();
        if (curator != null) {
            mode = Mode.DISTRIBUTED;
            // 为每个 destination 创建 ZK 节点监听
            for (String destination : adapterCanalConfig.DESTINATIONS) {
                BooleanMutex mutex = new BooleanMutex(true);
                initMutex(curator, destination, mutex);
                DISTRIBUTED_LOCK.put(destination, mutex);
                startListen(destination, mutex); // 监听 ZK 节点变更
            }
        } else {
            mode = Mode.LOCAL;
            for (String destination : adapterCanalConfig.DESTINATIONS) {
                LOCAL_LOCK.put(destination, new BooleanMutex(true));
            }
        }
    }
}
```

**BooleanMutex** 是一个可阻塞的布尔锁：
- `mutex.get()`：如果 `state == false`，阻塞当前线程，直到变为 `true`
- `mutex.set(true/false)`：设置状态，唤醒等待线程

在消费循环中，`syncSwitch.get(canalDestination)` 会阻塞直到同步开关打开。管理员可以通过 REST API 或 ZK 节点动态开启/关闭某个 destination 的同步。

---

## 四、RDB 适配器 —— 关系型数据库同步

### 4.1 RdbAdapter 核心实现

**源码文件**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/RdbAdapter.java`

```java
@SPI("rdb")
public class RdbAdapter implements OuterAdapter {

    // 文件名 → 映射配置（一个 .yml 文件对应一个 MappingConfig）
    private Map<String, MappingConfig>              rdbMapping          = new ConcurrentHashMap<>();

    // 库名-表名 → 映射配置列表（同一张表可能有多个映射规则）
    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();

    // 镜像库配置
    private Map<String, MirrorDbConfig>             mirrorDbConfigCache = new ConcurrentHashMap<>();

    private DruidDataSource                         dataSource;          // 目标库连接池
    private RdbSyncService                          rdbSyncService;      // 增量同步服务
    private RdbMirrorDbSyncService                  rdbMirrorDbSyncService; // 镜像库同步服务
    private RdbConfigMonitor                        rdbConfigMonitor;    // 配置文件监控
    // ...
}
```

#### 4.1.1 init() —— 初始化流程

```java
@Override
public void init(OuterAdapterConfig configuration, Properties envProperties) {
    this.envProperties = envProperties;
    this.configuration = configuration;

    // 1. 从 jdbc.url 获取数据库类型（mysql/oracle/postgresql...）
    Map<String, String> properties = configuration.getProperties();
    String dbType = JdbcUtils.getDbType(properties.get("jdbc.url"), null);

    // 2. 加载 conf/rdb/ 目录下的所有 .yml 映射配置
    Map<String, MappingConfig> rdbMappingTmp = ConfigLoader.load(envProperties);

    // 3. 过滤：只保留与当前 adapter key 匹配的配置
    rdbMappingTmp.forEach((key, config) -> {
        addConfig(key, config);
    });

    if (rdbMapping.isEmpty()) {
        throw new RuntimeException("No rdb adapter found for config key: "
                                   + configuration.getKey());
    }

    // 4. 初始化目标库 Druid 连接池
    dataSource = new DruidDataSource();
    dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
    dataSource.setUrl(properties.get("jdbc.url"));
    dataSource.setUsername(properties.get("jdbc.username"));
    dataSource.setPassword(properties.get("jdbc.password"));
    dataSource.setInitialSize(1);
    dataSource.setMinIdle(1);
    dataSource.setMaxActive(30);
    dataSource.setMaxWait(60000);
    dataSource.setTimeBetweenEvictionRunsMillis(60000);
    dataSource.setMinEvictableIdleTimeMillis(300000);
    dataSource.setUseUnfairLock(true);
    dataSource.setDbType(dbType);

    // 慢 SQL 统计
    if ("true".equals(properties.getOrDefault("druid.stat.enable", "true"))) {
        StatFilter statFilter = new StatFilter();
        statFilter.setSlowSqlMillis(
            Long.parseLong(properties.getOrDefault("druid.stat.slowSqlMillis", "1000")));
        statFilter.setMergeSql(true);
        statFilter.setLogSlowSql(true);
        dataSource.setProxyFilters(Collections.singletonList(statFilter));
    }

    dataSource.init();

    // 5. 创建增量同步服务
    String threads = properties.get("threads");
    boolean skipDupException = BooleanUtils.toBoolean(
        properties.getOrDefault("skipDupException", "true"));
    rdbSyncService = new RdbSyncService(dataSource,
        threads != null ? Integer.valueOf(threads) : null,
        skipDupException);

    // 6. 创建镜像库同步服务
    rdbMirrorDbSyncService = new RdbMirrorDbSyncService(mirrorDbConfigCache,
        dataSource, threads != null ? Integer.valueOf(threads) : null,
        rdbSyncService.getColumnsTypeCache(), skipDupException);

    // 7. 启动配置文件监控（热更新）
    rdbConfigMonitor = new RdbConfigMonitor();
    rdbConfigMonitor.init(configuration.getKey(), this, envProperties);
}
```

#### 4.1.2 配置匹配机制

```java
private boolean match(MappingConfig config) {
    // 精确匹配：配置文件指定了 outerAdapterKey，且与当前 adapter 的 key 一致
    boolean sameMatch = config.getOuterAdapterKey() != null
        && config.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey());

    // 前缀匹配：配置文件未指定 outerAdapterKey，根据 destination + groupId 自动匹配
    boolean prefixMatch = config.getOuterAdapterKey() == null
        && configuration.getKey().startsWith(StringUtils.join(
            new String[]{Util.AUTO_GENERATED_PREFIX,
                         config.getDestination(),
                         config.getGroupId()}, '-'));

    return sameMatch || prefixMatch;
}
```

#### 4.1.3 sync() —— 增量同步入口

```java
@Override
public void sync(List<Dml> dmls) {
    if (dmls == null || dmls.isEmpty()) return;
    try {
        if (!mappingConfigCache.isEmpty()) {
            rdbSyncService.sync(mappingConfigCache, dmls, envProperties);
        }
        rdbMirrorDbSyncService.sync(dmls);
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}
```

### 4.2 MappingConfig —— RDB 表映射配置

**源码文件**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/config/MappingConfig.java`

```java
public class MappingConfig implements AdapterConfig {

    private String    dataSourceKey;   // 数据源 key（对应 srcDataSources 的 key）
    private String    destination;     // canal 实例或 MQ topic
    private String    groupId;         // groupId
    private String    outerAdapterKey; // adapter key
    private boolean   concurrent;      // 是否并行同步
    private DbMapping dbMapping;       // 表映射详情

    public static class DbMapping implements AdapterMapping {
        private boolean             mirrorDb;       // 是否镜像库模式
        private String              database;       // 源数据库名
        private String              table;          // 源表名
        private Map<String, String> targetPk;       // 目标表主键映射
        private boolean             mapAll;         // 是否映射所有字段
        private String              targetDb;       // 目标库名
        private String              targetTable;    // 目标表名
        private Map<String, String> targetColumns;  // 字段映射（源字段 → 目标字段）
        private boolean             caseInsensitive; // 目标表字段不区分大小写
        private String              etlCondition;   // ETL 全量同步筛选条件
        private int                 readBatch = 5000;
        private int                 commitBatch = 5000;
    }
}
```

**映射配置 YAML 示例** (`conf/rdb/mydb_user.yml`)：

```yaml
dataSourceKey: defaultDS
destination: example
groupId: g1
outerAdapterKey: oracle1
dbMapping:
  database: mydb
  table: user
  targetDb: target_db
  targetTable: t_user
  targetPk:
    id: id
  mapAll: true
  targetColumns:
    user_name: name
  commitBatch: 3000
  etlCondition: "where create_time >= '{}'"
```

### 4.3 RdbSyncService —— 增量同步核心

**源码文件**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/service/RdbSyncService.java`

#### 4.3.1 并行分区模型

RdbSyncService 使用了**基于主键 hash 的分区并行**模型：

```java
public RdbSyncService(DruidDataSource dataSource, Integer threads,
                       boolean skipDupException) {
    if (threads != null) {
        this.threads = threads;
    }
    // 创建 threads 个分区，每个分区一个 BatchExecutor 和一个单线程执行器
    this.dmlsPartition = new List[this.threads];
    this.batchExecutors = new BatchExecutor[this.threads];
    this.executorThreads = new ExecutorService[this.threads];
    for (int i = 0; i < this.threads; i++) {
        dmlsPartition[i] = new ArrayList<>();
        batchExecutors[i] = new BatchExecutor(dataSource);
        executorThreads[i] = Executors.newSingleThreadExecutor();
    }
}
```

```
┌─── Thread 0 ───┐  ┌─── Thread 1 ───┐  ┌─── Thread 2 ───┐
│ dmlsPartition[0]│  │ dmlsPartition[1]│  │ dmlsPartition[2]│
│ batchExecutors[0]│  │ batchExecutors[1]│  │ batchExecutors[2]│
│ executorThreads[0] │ executorThreads[1] │ executorThreads[2]│
└─────────────────┘  └─────────────────┘  └─────────────────┘
     ▲                     ▲                     ▲
     │                     │                     │
  pkHash=0              pkHash=1              pkHash=2
```

**分区算法**：

```java
public int pkHash(DbMapping dbMapping, Map<String, Object> d) {
    int hash = 0;
    for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
        String srcColumnName = entry.getValue();
        if (srcColumnName == null) {
            srcColumnName = Util.cleanColumn(entry.getKey());
        }
        Object value = d.get(srcColumnName);
        if (value != null) {
            hash += value.hashCode();
        }
    }
    hash = Math.abs(hash) % threads;
    return Math.abs(hash);
}
```

**为什么按主键 hash 分区？** 保证同一行数据（相同主键）的所有操作（INSERT → UPDATE → DELETE）都被分到同一个线程执行，从而保证**单行操作的顺序性**。不同行的操作可以并行执行，提高吞吐量。

#### 4.3.2 sync() —— 分区分发 + 并行执行

```java
public void sync(Map<String, Map<String, MappingConfig>> mappingConfig,
                  List<Dml> dmls, Properties envProperties) {
    sync(dmls, dml -> {
        if (dml.getIsDdl() != null && dml.getIsDdl()) {
            // DDL 事件：清除字段类型缓存
            columnsTypeCache.remove(
                dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
            return false;
        } else {
            // DML 事件：查找映射配置，分发到分区
            String key = destination + "_" + database + "-" + table;
            Map<String, MappingConfig> configMap = mappingConfig.get(key);
            if (configMap == null) return false;

            for (MappingConfig config : configMap.values()) {
                appendDmlPartition(config, dml);
            }
            return true;
        }
    });
}
```

**appendDmlPartition() —— 将 Dml 拆分为 SingleDml 并分配到分区**：

```java
public void appendDmlPartition(MappingConfig config, Dml dml) {
    boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
    if (config.getConcurrent()) {
        // 并行模式：按主键 hash 分散到不同线程
        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
        singleDmls.forEach(singleDml -> {
            int hash = pkHash(config.getDbMapping(), singleDml.getData());
            dmlsPartition[hash].add(new SyncItem(config, singleDml));
        });
    } else {
        // 串行模式：全部放到 partition[0]
        List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
        singleDmls.forEach(singleDml -> {
            dmlsPartition[0].add(new SyncItem(config, singleDml));
        });
    }
}
```

#### 4.3.3 SingleDml —— 单行 DML

```java
public static List<SingleDml> dml2SingleDmls(Dml dml, boolean caseInsensitive) {
    List<SingleDml> singleDmls = new ArrayList<>();
    if (dml.getData() != null) {
        int size = dml.getData().size();
        for (int i = 0; i < size; i++) {
            SingleDml singleDml = new SingleDml();
            singleDml.setDestination(dml.getDestination());
            singleDml.setDatabase(dml.getDatabase());
            singleDml.setTable(dml.getTable());
            singleDml.setType(dml.getType());
            Map<String, Object> data = dml.getData().get(i);
            if (caseInsensitive) {
                data = toCaseInsensitiveMap(data); // 转为大小写不敏感的 Map
            }
            singleDml.setData(data);
            if (dml.getOld() != null) {
                singleDml.setOld(dml.getOld().get(i));
            }
            singleDmls.add(singleDml);
        }
    }
    return singleDmls;
}
```

一个 `Dml` 可能包含多行数据（例如一个事务中修改了 5 行），`dml2SingleDmls()` 将其拆分为 5 个 `SingleDml`，每个 SingleDml 只包含一行数据。

#### 4.3.4 INSERT 操作

```java
private void insert(BatchExecutor batchExecutor, MappingConfig config,
                     SingleDml dml) throws SQLException {
    Map<String, Object> data = dml.getData();
    DbMapping dbMapping = config.getDbMapping();
    String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
    Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);

    // 构建 INSERT SQL
    StringBuilder insertSql = new StringBuilder();
    insertSql.append("INSERT INTO ")
             .append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()))
             .append(" (");

    columnsMap.forEach((targetColumnName, srcColumnName) ->
        insertSql.append(backtick).append(targetColumnName).append(backtick).append(","));

    insertSql.delete(insertSql.length() - 1, insertSql.length()).append(") VALUES (");
    for (int i = 0; i < columnsMap.size(); i++) {
        insertSql.append("?,");
    }
    insertSql.delete(insertSql.length() - 1, insertSql.length()).append(")");

    // 构建参数列表
    Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);
    List<Map<String, ?>> values = new ArrayList<>();
    for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
        Integer type = ctype.get(entry.getKey().toLowerCase());
        Object value = data.get(entry.getValue());
        BatchExecutor.setValue(values, type, value);
    }

    try {
        batchExecutor.execute(insertSql.toString(), values);
    } catch (SQLException e) {
        // 跳过主键冲突异常（可配置）
        if (skipDupException
            && (e.getMessage().contains("Duplicate entry")
                || e.getMessage().contains("duplicate key")
                || e.getMessage().startsWith("ORA-00001:"))) {
            // ignore
        } else {
            throw e;
        }
    }
}
```

**生成的 SQL 示例**：

```sql
INSERT INTO `target_db`.`t_user` (`id`, `name`, `age`) VALUES (?, ?, ?)
```

#### 4.3.5 UPDATE 操作

```java
private void update(BatchExecutor batchExecutor, MappingConfig config,
                     SingleDml dml) throws SQLException {
    Map<String, Object> data = dml.getData();
    Map<String, Object> old = dml.getOld();

    DbMapping dbMapping = config.getDbMapping();
    Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
    Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

    StringBuilder updateSql = new StringBuilder();
    updateSql.append("UPDATE ")
             .append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()))
             .append(" SET ");

    List<Map<String, ?>> values = new ArrayList<>();
    boolean hasMatched = false;

    // 只更新 old 中存在的字段（即实际被修改的字段）
    for (String srcColumnName : old.keySet()) {
        List<String> targetColumnNames = new ArrayList<>();
        columnsMap.forEach((targetColumn, srcColumn) -> {
            if (srcColumnName.equalsIgnoreCase(srcColumn)) {
                targetColumnNames.add(targetColumn);
            }
        });
        if (!targetColumnNames.isEmpty()) {
            hasMatched = true;
            for (String targetColumnName : targetColumnNames) {
                updateSql.append(backtick).append(targetColumnName)
                         .append(backtick).append("=?, ");
                Integer type = ctype.get(targetColumnName.toLowerCase());
                BatchExecutor.setValue(values, type, data.get(srcColumnName));
            }
        }
    }

    if (!hasMatched) return; // 没有匹配的字段，跳过

    updateSql.delete(updateSql.length() - 2, updateSql.length()).append(" WHERE ");

    // 拼接主键 WHERE 条件
    appendCondition(dbMapping, updateSql, ctype, values, data, old);
    batchExecutor.execute(updateSql.toString(), values);
}
```

**关键细节**：UPDATE 时只更新 `old` 中出现的字段，而不是所有字段。这与 `Dml.old` 只记录被修改字段的设计一致，大大减少了 UPDATE SQL 的字段数量。

**WHERE 条件中的主键处理**：

```java
private void appendCondition(DbMapping dbMapping, StringBuilder sql,
                              Map<String, Integer> ctype, List<Map<String, ?>> values,
                              Map<String, Object> d, Map<String, Object> o) {
    for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
        String targetColumnName = entry.getKey();
        String srcColumnName = entry.getValue();
        sql.append(backtick).append(targetColumnName).append(backtick).append("=? AND ");
        // 如果主键也被修改了，WHERE 条件用旧值
        if (o != null && o.containsKey(srcColumnName)) {
            BatchExecutor.setValue(values, type, o.get(srcColumnName));
        } else {
            BatchExecutor.setValue(values, type, d.get(srcColumnName));
        }
    }
    sql.delete(sql.length() - 4, sql.length()); // 去掉最后的 " AND "
}
```

#### 4.3.6 DELETE 操作

```java
private void delete(BatchExecutor batchExecutor, MappingConfig config,
                     SingleDml dml) throws SQLException {
    Map<String, Object> data = dml.getData();
    DbMapping dbMapping = config.getDbMapping();
    Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);

    StringBuilder sql = new StringBuilder();
    sql.append("DELETE FROM ")
       .append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()))
       .append(" WHERE ");

    List<Map<String, ?>> values = new ArrayList<>();
    appendCondition(dbMapping, sql, ctype, values, data);
    batchExecutor.execute(sql.toString(), values);
}
```

DELETE 比较简单，只需要根据主键构建 WHERE 条件。

### 4.4 BatchExecutor —— 批量事务执行器

**源码文件**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/support/BatchExecutor.java`

```java
public class BatchExecutor implements Closeable {

    private DataSource    dataSource;
    private Connection    conn;
    private AtomicInteger idx = new AtomicInteger(0);

    public Connection getConn() {
        if (conn == null) {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false); // 关闭自动提交！
        }
        return conn;
    }

    public void execute(String sql, List<Map<String, ?>> values) throws SQLException {
        PreparedStatement pstmt = getConn().prepareStatement(sql);
        for (int i = 0; i < values.size(); i++) {
            int type = (Integer) values.get(i).get("type");
            Object value = values.get(i).get("value");
            SyncUtil.setPStmt(type, pstmt, value, i + 1);
        }
        pstmt.execute();
        idx.incrementAndGet();
        pstmt.close();
    }

    public void commit() throws SQLException {
        getConn().commit();
        idx.set(0);
    }

    public void rollback() throws SQLException {
        getConn().rollback();
        idx.set(0);
    }
}
```

**关键设计**：

1. **手动事务控制**：`setAutoCommit(false)`，一批 DML 执行完毕后才 `commit()`
2. **每条 SQL 独立 PreparedStatement**：不是 JDBC batch addBatch()，而是每条 SQL 单独 execute()
3. **commit 粒度**：一个分区内的所有 SyncItem 执行完毕后统一 commit

### 4.5 SyncUtil —— SQL 构建工具

**源码文件**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/support/SyncUtil.java`

`SyncUtil` 提供了两个核心功能：

**1. 字段映射解析**：

```java
public static Map<String, String> getColumnsMap(DbMapping dbMapping,
                                                  Collection<String> columns) {
    if (dbMapping.getMapAll()) {
        // mapAll=true: 所有源字段直接映射，除了显式配置的
        Map<String, String> columnsMap = new LinkedHashMap<>();
        for (String srcColumn : columns) {
            boolean flag = true;
            if (dbMapping.getTargetColumns() != null) {
                for (Map.Entry<String, String> entry :
                     dbMapping.getTargetColumns().entrySet()) {
                    if (srcColumn.equals(entry.getValue())) {
                        columnsMap.put(entry.getKey(), srcColumn);
                        flag = false;
                        break;
                    }
                }
            }
            if (flag) {
                columnsMap.put(srcColumn, srcColumn); // 同名映射
            }
        }
        return columnsMap;
    } else {
        return dbMapping.getTargetColumns(); // 只映射显式配置的字段
    }
}
```

**2. PreparedStatement 参数设置**（类型安全的参数绑定）：

`setPStmt()` 方法处理了 JDBC 所有主要数据类型的转换，包括：
- BIT/BOOLEAN → `setBoolean()`
- CHAR/VARCHAR/LONGVARCHAR → `setString()`
- TINYINT → `setShort()`（向上提升，处理 unsigned）
- SMALLINT → `setInt()`
- INTEGER → `setLong()`
- BIGINT → `setBigDecimal()`
- DECIMAL/NUMERIC → `setBigDecimal()`
- FLOAT/DOUBLE → `setDouble()`
- BINARY/BLOB → `setBytes()`
- CLOB → `setCharacterStream()`
- DATE/TIME/TIMESTAMP → 对应的 JDBC 时间类型

**3. 数据库方言处理**：

```java
public static String getBacktickByDbType(String dbTypeName) {
    DbType dbType = DbType.of(dbTypeName);
    switch (dbType) {
        case mysql:
        case mariadb:
        case oceanbase:
            return "`";       // MySQL 系使用反引号
        case postgresql:
            return "\"";      // PostgreSQL 使用双引号
        default:
            return "";        // Oracle 等不需要转义
    }
}
```

### 4.6 RdbEtlService —— 全量同步

**源码文件**: `client-adapter/rdb/src/main/java/com/alibaba/otter/canal/client/adapter/rdb/service/RdbEtlService.java`

全量同步的流程是：从源库分页读取 → 先 DELETE 再 INSERT 到目标库（确保幂等）。

```java
public class RdbEtlService extends AbstractEtlService {

    public EtlResult importData(List<String> params) {
        DbMapping dbMapping = config.getDbMapping();
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        String sql = "SELECT * FROM " +
            SyncUtil.getSourceDbTableName(dbMapping, dataSource.getDbType());
        return importData(sql, params); // 调用父类的分页+多线程导入
    }

    @Override
    protected boolean executeSqlImport(DataSource srcDS, String sql, List<Object> values,
                                        AdapterConfig.AdapterMapping mapping,
                                        AtomicLong impCount, List<String> errMsg) {
        // 1. 获取目标表字段类型
        // 2. 遍历源库查询结果
        Util.sqlRS(srcDS, sql, values, rs -> {
            while (rs.next()) {
                // 3. 先删除目标表中同主键的记录
                // DELETE FROM target_table WHERE pk = ?
                // 4. 再插入新记录
                // INSERT INTO target_table (col1, col2, ...) VALUES (?, ?, ...)
                pstmt.execute();

                // 5. 按 commitBatch 分批提交
                if (idx % dbMapping.getCommitBatch() == 0) {
                    connTarget.commit();
                }
            }
        });
        return true;
    }
}
```

父类 `AbstractEtlService` 提供了多线程分页能力：

```java
public abstract class AbstractEtlService {

    private final long CNT_PER_TASK = 10000L;

    protected EtlResult importData(String sql, List<String> params) {
        // 1. 获取源数据源
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());

        // 2. 获取总数
        String countSql = "SELECT COUNT(1) FROM (" + sql + ") _CNT";
        long cnt = Util.sqlRS(dataSource, countSql, ...);

        // 3. 大于 1 万条时开启多线程
        if (cnt >= 10000) {
            int threadCount = Runtime.getRuntime().availableProcessors();
            long size = CNT_PER_TASK;
            long workerCnt = cnt / size + (cnt % size == 0 ? 0 : 1);

            ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
            List<Future<Boolean>> futures = new ArrayList<>();
            for (long i = 0; i < workerCnt; i++) {
                long offset = size * i;
                String sqlFinal = sql + " LIMIT " + offset + "," + size;
                futures.add(executor.submit(() ->
                    executeSqlImport(dataSource, sqlFinal, values, mapping, impCount, errMsg)));
            }
            for (Future<Boolean> future : futures) {
                future.get();
            }
            executor.shutdown();
        } else {
            executeSqlImport(dataSource, sql, values, mapping, impCount, errMsg);
        }
    }
}
```

---

## 五、ES 适配器 —— Elasticsearch 同步

### 5.1 ES7xAdapter 核心实现

**源码文件**: `client-adapter/es7x/src/main/java/com/alibaba/otter/canal/client/adapter/es7x/ES7xAdapter.java`

```java
@SPI("es7")
public class ES7xAdapter extends ESAdapter {

    private ESConnection esConnection;

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        Map<String, String> properties = configuration.getProperties();
        String[] hostArray = configuration.getHosts().split(",");
        String mode = properties.get("mode");

        // 根据 mode 选择 REST 或 Transport 客户端
        if ("rest".equalsIgnoreCase(mode) || "http".equalsIgnoreCase(mode)) {
            esConnection = new ESConnection(hostArray, properties, ESClientMode.REST);
        } else {
            esConnection = new ESConnection(hostArray, properties, ESClientMode.TRANSPORT);
        }

        this.esTemplate = new ES7xTemplate(esConnection);
        envProperties.put("es.version", "es7");
        super.init(configuration, envProperties); // 加载映射配置
    }
}
```

`ES7xAdapter` 继承自 `ESAdapter`（es-core 模块），大部分同步逻辑在父类中实现。ES7x 特有的部分是 `ESConnection` 和 `ES7xTemplate`。

### 5.2 ESConnection —— ES 连接封装

**源码文件**: `client-adapter/es7x/src/main/java/com/alibaba/otter/canal/client/adapter/es7x/support/ESConnection.java`

ESConnection 封装了两种 ES 客户端模式：

```java
public class ESConnection {

    public enum ESClientMode {
        TRANSPORT, REST
    }

    private ESClientMode        mode;
    private TransportClient     transportClient;   // Transport 模式（已废弃）
    private RestHighLevelClient restHighLevelClient; // REST 模式（推荐）

    public ESConnection(String[] hosts, Map<String, String> properties,
                         ESClientMode mode) throws UnknownHostException {
        this.mode = mode;
        if (mode == ESClientMode.TRANSPORT) {
            Settings.Builder settingBuilder = Settings.builder();
            settingBuilder.put("cluster.name", properties.get("cluster.name"));
            transportClient = new PreBuiltTransportClient(settingBuilder.build());
            for (String host : hosts) {
                int i = host.indexOf(":");
                transportClient.addTransportAddress(new TransportAddress(
                    InetAddress.getByName(host.substring(0, i)),
                    Integer.parseInt(host.substring(i + 1))));
            }
        } else {
            HttpHost[] httpHosts = Arrays.stream(hosts)
                .map(this::createHttpHost).toArray(HttpHost[]::new);
            RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
            // 支持 Basic Auth
            String nameAndPwd = properties.get("security.auth");
            if (StringUtils.isNotEmpty(nameAndPwd) && nameAndPwd.contains(":")) {
                String[] arr = nameAndPwd.split(":");
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(arr[0], arr[1]));
                restClientBuilder.setHttpClientConfigCallback(
                    b -> b.setDefaultCredentialsProvider(credentialsProvider));
            }
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }
    }
}
```

ESConnection 还定义了一系列内部类，作为 ES 操作的统一抽象：

| 内部类 | 说明 |
|--------|------|
| `ES7xIndexRequest` | 封装 `IndexRequest` / `IndexRequestBuilder` |
| `ES7xUpdateRequest` | 封装 `UpdateRequest` / `UpdateRequestBuilder` |
| `ES7xDeleteRequest` | 封装 `DeleteRequest` / `DeleteRequestBuilder` |
| `ESSearchRequest` | 封装 `SearchRequest` / `SearchRequestBuilder` |
| `ES7xBulkRequest` | 封装 `BulkRequest` / `BulkRequestBuilder` |
| `ES7xBulkResponse` | 封装 `BulkResponse` |

每个内部类都同时支持 Transport 和 REST 两种模式，通过 `if (mode == ESClientMode.TRANSPORT)` 分支选择。

### 5.3 ES7xTemplate —— ES 操作模板

**源码文件**: `client-adapter/es7x/src/main/java/com/alibaba/otter/canal/client/adapter/es7x/support/ES7xTemplate.java`

ES7xTemplate 实现了 `ESTemplate` 接口，提供 insert/update/delete/commit 操作：

#### 5.3.1 insert() —— 插入文档

```java
@Override
public void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
    if (mapping.getId() != null) {
        // 有显式 ID 映射
        String parentVal = (String) esFieldData.remove("$parent_routing");
        if (mapping.isUpsert()) {
            // upsert 模式：存在则更新，不存在则插入
            ESUpdateRequest updateRequest = esConnection.new ES7xUpdateRequest(
                mapping.getIndex(), pkVal.toString())
                .setDoc(esFieldData)
                .setDocAsUpsert(true);
            if (StringUtils.isNotEmpty(parentVal)) {
                updateRequest.setRouting(parentVal);
            }
            getBulk().add(updateRequest);
        } else {
            // 直接 index（覆盖写入）
            ESIndexRequest indexRequest = esConnection.new ES7xIndexRequest(
                mapping.getIndex(), pkVal.toString())
                .setSource(esFieldData);
            if (StringUtils.isNotEmpty(parentVal)) {
                indexRequest.setRouting(parentVal);
            }
            getBulk().add(indexRequest);
        }
        commitBulk(); // 达到批量阈值时提交
    } else {
        // 没有显式 ID，通过 PK 查询后更新
        ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(
            mapping.getIndex())
            .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
            .size(10000);
        SearchResponse response = esSearchRequest.getResponse();
        for (SearchHit hit : response.getHits()) {
            ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(
                mapping.getIndex(), hit.getId())
                .setDoc(esFieldData);
            getBulk().add(esUpdateRequest);
            commitBulk();
        }
    }
}
```

#### 5.3.2 delete() —— 删除文档

```java
@Override
public void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
    if (mapping.getId() != null) {
        ESDeleteRequest esDeleteRequest = this.esConnection.new ES7xDeleteRequest(
            mapping.getIndex(), pkVal.toString());
        getBulk().add(esDeleteRequest);
        commitBulk();
    } else {
        // 先查再删（通过 update 清空字段）
        ESSearchRequest esSearchRequest = ...;
        for (SearchHit hit : response.getHits()) {
            ESUpdateRequest esUpdateRequest = ...;
            getBulk().add(esUpdateRequest);
            commitBulk();
        }
    }
}
```

#### 5.3.3 批量提交机制

```java
private static final int MAX_BATCH_SIZE = 1000;

private void commitBulk() {
    if (getBulk().numberOfActions() >= MAX_BATCH_SIZE) {
        commit();
    }
}

@Override
public void commit() {
    if (getBulk().numberOfActions() > 0) {
        ESBulkResponse response = getBulk().bulk();
        if (response.hasFailures()) {
            response.processFailBulkResponse("ES sync commit error ");
        }
        resetBulkRequestBuilder();
    }
}
```

当批量请求中的操作数量达到 1000 时自动提交。`ES7xBulkRequest.bulk()` 底层调用 `restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT)`。

#### 5.3.4 ES Mapping 类型缓存

```java
// 本地缓存：index-type → {fieldName → esType}
private static ConcurrentMap<String, Map<String, String>> esFieldTypes
    = new ConcurrentHashMap<>();

private String getEsType(ESMapping mapping, String fieldName) {
    String key = mapping.getIndex() + "-" + mapping.getType();
    Map<String, String> fieldType = esFieldTypes.get(key);
    if (fieldType != null) {
        return fieldType.get(fieldName);
    } else {
        // 从 ES 集群获取 mapping 元数据
        MappingMetaData mappingMetaData = esConnection.getMapping(mapping.getIndex());
        fieldType = new LinkedHashMap<>();
        Map<String, Object> sourceMap = mappingMetaData.getSourceAsMap();
        Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
        for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
            Map<String, Object> value = (Map<String, Object>) entry.getValue();
            if (value.containsKey("properties")) {
                fieldType.put(entry.getKey(), "object");
            } else {
                fieldType.put(entry.getKey(), (String) value.get("type"));
            }
        }
        esFieldTypes.put(key, fieldType);
        return fieldType.get(fieldName);
    }
}
```

### 5.4 ESEtlService —— ES 全量同步

**源码文件**: `client-adapter/es7x/src/main/java/com/alibaba/otter/canal/client/adapter/es7x/etl/ESEtlService.java`

```java
public class ESEtlService extends AbstractEtlService {

    public EtlResult importData(List<String> params) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql(); // 从 ES 映射配置中获取源库查询 SQL
        return importData(sql, params);
    }

    @Override
    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                        AdapterConfig.AdapterMapping adapterMapping,
                                        AtomicLong impCount, List<String> errMsg) {
        ESMapping mapping = (ESMapping) adapterMapping;
        Util.sqlRS(ds, sql, values, rs -> {
            ESBulkRequest esBulkRequest = this.esConnection.new ES7xBulkRequest();
            while (rs.next()) {
                // 1. 从 ResultSet 提取字段值，构建 esFieldData
                Map<String, Object> esFieldData = new LinkedHashMap<>();
                Object idVal = null;
                for (FieldItem fieldItem : mapping.getSchemaItem().getSelectFields().values()) {
                    if (fieldItem.getFieldName().equals(mapping.getId())) {
                        idVal = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                    } else {
                        esFieldData.put(fieldName, val);
                    }
                }

                // 2. 处理父子文档关联
                if (!mapping.getRelations().isEmpty()) {
                    // ... 构建 relations 字段
                }

                // 3. 构建 ES 请求
                if (idVal != null) {
                    if (mapping.isUpsert()) {
                        esBulkRequest.add(
                            esConnection.new ES7xUpdateRequest(mapping.getIndex(), idVal.toString())
                                .setDoc(esFieldData).setDocAsUpsert(true));
                    } else {
                        esBulkRequest.add(
                            esConnection.new ES7xIndexRequest(mapping.getIndex(), idVal.toString())
                                .setSource(esFieldData));
                    }
                }

                // 4. 批量提交
                if (esBulkRequest.numberOfActions() % mapping.getCommitBatch() == 0) {
                    ESBulkResponse rp = esBulkRequest.bulk();
                    if (rp.hasFailures()) {
                        rp.processFailBulkResponse("全量数据 etl 异常");
                    }
                    esBulkRequest.resetBulk();
                }
                impCount.incrementAndGet();
            }
            // 提交最后一批
            if (esBulkRequest.numberOfActions() > 0) {
                esBulkRequest.bulk();
            }
        });
        return true;
    }
}
```

---

## 六、HBase 适配器 —— HBase 同步

### 6.1 HbaseAdapter 核心实现

**源码文件**: `client-adapter/hbase/src/main/java/com/alibaba/otter/canal/client/adapter/hbase/HbaseAdapter.java`

```java
@SPI("hbase")
public class HbaseAdapter implements OuterAdapter {

    private Map<String, MappingConfig>              hbaseMapping       = new ConcurrentHashMap<>();
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();

    private HbaseSyncService hbaseSyncService;
    private HbaseTemplate    hbaseTemplate;

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        // 1. 加载映射配置
        Map<String, MappingConfig> hbaseMappingTmp = MappingConfigLoader.load(envProperties);
        hbaseMappingTmp.forEach((key, config) -> addConfig(key, config));

        // 2. 创建 HBase 连接
        Map<String, String> properties = configuration.getProperties();
        Configuration hbaseConfig = HBaseConfiguration.create();
        properties.forEach(hbaseConfig::set); // 将所有配置属性传给 HBase Configuration
        hbaseTemplate = new HbaseTemplate(hbaseConfig);

        // 3. 创建同步服务
        hbaseSyncService = new HbaseSyncService(hbaseTemplate);

        // 4. 启动配置监控
        configMonitor = new HbaseConfigMonitor();
        configMonitor.init(this, envProperties);
    }

    @Override
    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            // 查找映射配置
            Map<String, MappingConfig> configMap = mappingConfigCache.get(key);
            if (configMap != null) {
                configMap.values().stream()
                    .filter(config -> config.getGroupId() == null
                                   || config.getGroupId().equals(dml.getGroupId()))
                    .forEach(config -> hbaseSyncService.sync(config, dml));
            }
        }
    }
}
```

**注意**：与 RdbAdapter 不同，HbaseAdapter 的 `sync()` 是**逐条 Dml 处理**的（`for (Dml dml : dmls)`），而不是批量。这是因为 HBase 的 Put/Delete 操作本身就是批量的（通过 `hbaseTemplate.puts()` 批量写入），Dml 内部的多行数据在 `HbaseSyncService` 中以 `List<HRow>` 形式批量提交。

### 6.2 HBase MappingConfig —— 映射配置

**源码文件**: `client-adapter/hbase/src/main/java/com/alibaba/otter/canal/client/adapter/hbase/config/MappingConfig.java`

```java
public class MappingConfig implements AdapterConfig {

    private String       dataSourceKey;
    private String       outerAdapterKey;
    private String       groupId;
    private String       destination;
    private HbaseMapping hbaseMapping;

    // HBase 特有的映射配置
    public static class HbaseMapping implements AdapterMapping {
        private Mode                    mode = Mode.STRING;     // 值转换模式
        private String                  database;               // 源数据库名
        private String                  table;                  // 源表名
        private String                  hbaseTable;             // HBase 目标表名
        private String                  family = "CF";          // 默认 Column Family
        private boolean                 uppercaseQualifier = true; // Qualifier 是否转大写
        private boolean                 autoCreateTable = false;// 自动建表
        private String                  rowKey;                 // 复合 RowKey 配置
        private Map<String, String>     columns;                // 字段映射
        private List<String>            excludeColumns;         // 排除字段
        private ColumnItem              rowKeyColumn;           // RowKey 字段配置
        private String                  etlCondition;           // ETL 条件
        private int                     commitBatch = 5000;     // 批量提交大小
    }

    // 列项配置
    public static class ColumnItem {
        private boolean isRowKey = false;
        private Integer rowKeyLen;        // RowKey 长度（零填充）
        private String  column;           // 源字段名
        private String  family;           // 目标 Column Family
        private String  qualifier;        // 目标 Qualifier
        private String  type;             // 类型转换
    }

    // 值转换模式
    public enum Mode {
        STRING,   // 所有值转字符串
        NATIVE,   // 使用 Java 原生类型的字节序列化
        PHOENIX   // Phoenix 格式的字节序列化
    }
}
```

**HBase 映射配置 YAML 示例** (`conf/hbase/mydb_user.yml`)：

```yaml
dataSourceKey: defaultDS
destination: example
hbaseMapping:
  mode: STRING
  database: mydb
  table: user
  hbaseTable: MYDB:T_USER
  family: CF
  uppercaseQualifier: true
  rowKey: id,type
  commitBatch: 3000
  columns:
    id: ROWKEY
    name: CF:NAME
    age: CF:AGE$int
    create_time: CF:CREATE_TIME
```

**columns 映射语法**：

| 配置值 | 解析结果 |
|--------|---------|
| `ROWKEY` | 标记为 RowKey 字段 |
| `ROWKEY(LEN:10)` | RowKey，零填充到 10 位 |
| `CF:NAME` | family=CF, qualifier=NAME |
| `CF2:ADDR` | family=CF2, qualifier=ADDR |
| `CF:AGE$int` | family=CF, qualifier=AGE, type=int |
| `""` (空值) | family=默认 family, qualifier=字段名 |

### 6.3 HbaseSyncService —— 同步核心

**源码文件**: `client-adapter/hbase/src/main/java/com/alibaba/otter/canal/client/adapter/hbase/service/HbaseSyncService.java`

#### 6.3.1 INSERT 操作

```java
private void insert(MappingConfig config, Dml dml) {
    List<Map<String, Object>> data = dml.getData();
    MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

    int i = 1;
    boolean complete = false;
    List<HRow> rows = new ArrayList<>();

    for (Map<String, Object> r : data) {
        HRow hRow = new HRow();

        // 1. 构建 RowKey（支持复合主键）
        if (hbaseMapping.getRowKey() != null) {
            String[] rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");
            String rowKeyValue = getRowKeys(rowKeyColumns, r);
            hRow.setRowKey(Bytes.toBytes(rowKeyValue));
        }

        // 2. 将 Map 数据转换为 HRow
        convertData2Row(hbaseMapping, hRow, r);

        if (hRow.getRowKey() == null) {
            throw new RuntimeException("empty rowKey");
        }
        rows.add(hRow);
        complete = false;

        // 3. 按 commitBatch 分批提交
        if (i % config.getHbaseMapping().getCommitBatch() == 0 && !rows.isEmpty()) {
            hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
            rows.clear();
            complete = true;
        }
        i++;
    }

    // 4. 提交剩余数据
    if (!complete && !rows.isEmpty()) {
        hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
    }
}
```

**复合 RowKey 的拼接**：

```java
private static String getRowKeys(String[] rowKeyColumns, Map<String, Object> data) {
    StringBuilder rowKeyValue = new StringBuilder();
    for (String rowKeyColumnName : rowKeyColumns) {
        Object obj = data.get(rowKeyColumnName);
        if (obj != null) {
            rowKeyValue.append(obj.toString());
        }
        rowKeyValue.append("|");  // 用 | 分隔
    }
    // 去掉最后一个 |
    int len = rowKeyValue.length();
    if (len > 0) {
        rowKeyValue.delete(len - 1, len);
    }
    return rowKeyValue.toString();
}
```

例如，`rowKey: id,type`，数据为 `{id: 100, type: "A"}`，生成的 RowKey 为 `"100|A"`。

#### 6.3.2 UPDATE 操作

HBase 的 UPDATE 就是 PUT（HBase 天然支持覆盖写入）。但有一个特殊情况——**RowKey 被修改**：

```java
private void update(MappingConfig config, Dml dml) {
    // ...
    out: for (Map<String, Object> r : data) {
        if (hbaseMapping.getRowKey() != null) {
            String[] rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");

            // 检查是否有复合主键被修改
            for (String updateColumn : old.get(index).keySet()) {
                for (String rowKeyColumnName : rowKeyColumns) {
                    if (rowKeyColumnName.equalsIgnoreCase(updateColumn)) {
                        // RowKey 被修改！需要 delete 旧行 + insert 新行
                        deleteAndInsert(config, dml);
                        continue out;
                    }
                }
            }

            // RowKey 未被修改，正常 PUT 更新
            String rowKeyValue = getRowKeys(rowKeyColumns, r);
            rowKeyBytes = Bytes.toBytes(rowKeyValue);
        }
        // ... 只 PUT 被修改的列（old 中有的字段）
    }
}
```

**当 RowKey 被修改时**：

```
旧 RowKey: "100|A"  →  新 RowKey: "200|A"

操作顺序:
1. DELETE RowKey="100|A" 的行
2. INSERT RowKey="200|A" 的新行
```

这是因为 HBase 不支持修改 RowKey——RowKey 是不可变的。

#### 6.3.3 DELETE 操作

```java
private void delete(MappingConfig config, Dml dml) {
    List<Map<String, Object>> data = dml.getData();
    MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

    Set<byte[]> rowKeys = new HashSet<>();
    for (Map<String, Object> r : data) {
        byte[] rowKeyBytes;
        if (hbaseMapping.getRowKey() != null) {
            String[] rowKeyColumns = hbaseMapping.getRowKey().trim().split(",");
            String rowKeyValue = getRowKeys(rowKeyColumns, r);
            rowKeyBytes = Bytes.toBytes(rowKeyValue);
        } else if (rowKeyColumn == null) {
            rowKeyBytes = typeConvert(null, hbaseMapping, r.values().iterator().next());
        } else {
            rowKeyBytes = getRowKeyBytes(hbaseMapping, rowKeyColumn, r);
        }
        rowKeys.add(rowKeyBytes);

        // 分批删除
        if (i % commitBatch == 0 && !rowKeys.isEmpty()) {
            hbaseTemplate.deletes(hbaseMapping.getHbaseTable(), rowKeys);
            rowKeys.clear();
        }
    }
    if (!rowKeys.isEmpty()) {
        hbaseTemplate.deletes(hbaseMapping.getHbaseTable(), rowKeys);
    }
}
```

#### 6.3.4 类型转换

```java
private static byte[] typeConvert(MappingConfig.ColumnItem columnItem,
                                    MappingConfig.HbaseMapping hbaseMapping,
                                    Object value) {
    if (columnItem == null || columnItem.getType() == null) {
        // 没有指定类型，按 mode 默认处理
        if (Mode.STRING == hbaseMapping.getMode()) {
            return Bytes.toBytes(value.toString());        // 全部转字符串
        } else if (Mode.NATIVE == hbaseMapping.getMode()) {
            return TypeUtil.toBytes(value);                // Java 原生序列化
        } else if (Mode.PHOENIX == hbaseMapping.getMode()) {
            return PhTypeUtil.toBytes(value, phType);      // Phoenix 格式
        }
    } else {
        // 有指定类型，按类型转换
        if (Mode.NATIVE == hbaseMapping.getMode()) {
            Type type = Type.getType(columnItem.getType());
            return TypeUtil.toBytes(value, type);
        }
        // ...
    }
}
```

### 6.4 HbaseTemplate —— HBase 操作模板

**源码文件**: `client-adapter/hbase/src/main/java/com/alibaba/otter/canal/client/adapter/hbase/support/HbaseTemplate.java`

```java
public class HbaseTemplate {

    private Configuration hbaseConfig;
    private Connection    conn;

    public HbaseTemplate(Configuration hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
        this.conn = ConnectionFactory.createConnection(hbaseConfig);
    }

    // 批量插入
    public Boolean puts(String tableName, List<HRow> rows) {
        HTable table = (HTable) getConnection().getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();
        for (HRow hRow : rows) {
            Put put = new Put(hRow.getRowKey());
            for (HRow.HCell hCell : hRow.getCells()) {
                put.addColumn(Bytes.toBytes(hCell.getFamily()),
                              Bytes.toBytes(hCell.getQualifier()),
                              hCell.getValue());
            }
            puts.add(put);
        }
        if (!puts.isEmpty()) {
            table.put(puts);
        }
        return true;
    }

    // 批量删除
    public Boolean deletes(String tableName, Set<byte[]> rowKeys) {
        HTable table = (HTable) getConnection().getTable(TableName.valueOf(tableName));
        List<Delete> deletes = new ArrayList<>();
        for (byte[] rowKey : rowKeys) {
            deletes.add(new Delete(rowKey));
        }
        if (!deletes.isEmpty()) {
            table.delete(deletes);
        }
        return true;
    }

    // 建表
    public void createTable(String tableName, String... familyNames) {
        HBaseAdmin admin = (HBaseAdmin) getConnection().getAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            desc.addFamily(new HColumnDescriptor(familyName));
        }
        admin.createTable(desc);
    }

    // 判断表是否存在
    public boolean tableExists(String tableName) {
        HBaseAdmin admin = (HBaseAdmin) getConnection().getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    // 禁用表
    public void disableTable(String tableName) { ... }

    // 删除表
    public void deleteTable(String tableName) { ... }
}
```

### 6.5 HRow —— 行数据抽象

**源码文件**: `client-adapter/hbase/src/main/java/com/alibaba/otter/canal/client/adapter/hbase/support/HRow.java`

```java
public class HRow {

    private byte[]      rowKey;
    private List<HCell> cells = new ArrayList<>();

    public void addCell(String family, String qualifier, byte[] value) {
        HCell hCell = new HCell(family, qualifier, value);
        cells.add(hCell);
    }

    public static class HCell {
        private String family;
        private String qualifier;
        private byte[] value;
    }
}
```

`HRow` 是对 HBase 一行数据的抽象，它由一个 `rowKey` 和多个 `HCell`（列）组成。`HCell` 则包含 `family`（列族）、`qualifier`（列限定符）和 `value`（值，字节数组）三个部分。这个设计与 HBase 的数据模型完全对应。

### 6.6 HbaseEtlService —— 全量同步

**源码文件**: `client-adapter/hbase/src/main/java/com/alibaba/otter/canal/client/adapter/hbase/service/HbaseEtlService.java`

```java
public class HbaseEtlService extends AbstractEtlService {

    public EtlResult importData(List<String> params) {
        MappingConfig.HbaseMapping hbaseMapping = config.getHbaseMapping();

        // 支持 rebuild 模式：删除旧表后重建
        if (params != null && params.size() == 1
            && "rebuild".equalsIgnoreCase(params.get(0))) {
            if (hbaseTemplate.tableExists(hbaseMapping.getHbaseTable())) {
                hbaseTemplate.disableTable(hbaseMapping.getHbaseTable());
                hbaseTemplate.deleteTable(hbaseMapping.getHbaseTable());
            }
            params = null;
        }

        // 自动建表
        createTable();

        // 拼接源库查询 SQL
        String sql = "SELECT * FROM `" + hbaseMapping.getDatabase() + "`.`"
                   + hbaseMapping.getTable() + "`";
        return super.importData(sql, params); // 父类分页 + 多线程
    }

    @Override
    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                        AdapterConfig.AdapterMapping mapping,
                                        AtomicLong impCount, List<String> errMsg) {
        HbaseMapping hbaseMapping = (HbaseMapping) mapping;
        Util.sqlRS(ds, sql, values, rs -> {
            List<HRow> rows = new ArrayList<>();
            while (rs.next()) {
                HRow row = new HRow();
                // 构建 RowKey
                // 遍历所有列，按 mode 转换类型，添加到 row
                rows.add(row);
                if (i % hbaseMapping.getCommitBatch() == 0) {
                    hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
                    rows.clear();
                }
                impCount.incrementAndGet();
            }
            if (!rows.isEmpty()) {
                hbaseTemplate.puts(hbaseMapping.getHbaseTable(), rows);
            }
        });
        return true;
    }
}
```

---

## 七、其他适配器概述

除了 RDB、ES、HBase 三大核心适配器外，Canal Client-Adapter 还支持以下下游存储：

### 7.1 ClickHouse 适配器

ClickHouse 适配器复用了 RDB 适配器的大部分逻辑，因为 ClickHouse 支持标准 JDBC 接口。主要差异在于：

| 差异点 | 说明 |
|--------|------|
| JDBC Driver | 使用 `ru.yandex.clickhouse.ClickHouseDriver` |
| INSERT 语句 | ClickHouse 不支持 `ON DUPLICATE KEY UPDATE`，需要特殊处理 |
| UPDATE/DELETE | ClickHouse 的 MergeTree 引擎不支持直接 UPDATE/DELETE，需要使用 `ALTER TABLE ... UPDATE/DELETE` |
| 批量写入 | ClickHouse 对批量 INSERT 有更好的优化 |

### 7.2 Kudu 适配器

Apache Kudu 是一种列式存储引擎，适用于实时分析场景。Kudu 适配器的特点：

- 使用 Kudu Java Client（非 JDBC）
- 支持 Upsert 操作（INSERT_OR_UPDATE）
- 需要映射 Kudu 表的 Schema（包括列类型和分区策略）
- 支持批量写入通过 `KuduSession.apply()` + `flush()`

### 7.3 Phoenix 适配器

Apache Phoenix 是 HBase 之上的 SQL 层。Phoenix 适配器的特点：

- 通过 JDBC 接口操作 HBase
- 支持标准 SQL（INSERT/UPDATE/DELETE）
- 数据类型与 Phoenix 的类型系统对应
- 可以利用 Phoenix 的二级索引

### 7.4 TableStore（OTS）适配器

阿里云表格存储（TableStore）适配器：

- 使用 TableStore Java SDK
- RowKey 设计与 HBase 类似
- 支持 PutRow/UpdateRow/DeleteRow 操作
- 适用于阿里云环境下的数据同步

---

## 八、关键设计总结

### 8.1 SPI 插件化架构

```
                     ┌─────────────────────────────┐
                     │      OuterAdapter (SPI)      │
                     │  @SPI("logger")               │
                     │  init() / sync() / destroy()  │
                     └──────────┬──────────────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          │                     │                     │
     ┌────┴────┐          ┌─────┴─────┐         ┌────┴────┐
     │  RDB    │          │    ES     │         │  HBase  │
     │@SPI(rdb)│          │ @SPI(es7) │         │@SPI(hbase)│
     └─────────┘          └───────────┘         └──────────┘
          │                     │                     │
     plugin/                plugin/              plugin/
     rdb-x.jar             es7x-x.jar           hbase-x.jar
```

核心价值：
1. **完全解耦**：launcher 不依赖任何具体适配器，通过 SPI 动态发现
2. **热插拔**：新增适配器只需放一个 jar 到 plugin/ 目录
3. **类隔离**：`URLClassExtensionLoader` 实现了类加载隔离，避免依赖冲突
4. **多实例**：通过 `getExtension(name, key)` 支持同类型适配器的多实例

### 8.2 配置驱动的映射规则

```
application.yml (全局配置)
    │
    ├── canal.conf.canalAdapters[0]
    │     ├── instance: example              ← Canal 实例名
    │     └── groups[0]
    │           ├── groupId: g1
    │           └── outerAdapters[0]
    │                 ├── name: rdb           ← SPI 名称
    │                 ├── key: oracle1        ← 唯一标识
    │                 ├── hosts: ...          ← 连接地址
    │                 └── properties:         ← 连接属性
    │                       jdbc.url: ...
    │                       jdbc.username: ...
    │
    └── conf/rdb/                            ← 映射配置目录
          ├── mydb_user.yml                  ← 表映射配置
          ├── mydb_order.yml
          └── ...
```

每个 Adapter 有两层配置：
- **全局配置**（`application.yml`）：定义适配器的类型、连接信息、实例绑定关系
- **映射配置**（`conf/<type>/*.yml`）：定义具体的表级别映射规则

通过 `outerAdapterKey` 将映射配置与适配器实例关联起来。

### 8.3 批量处理与性能优化

| 优化点 | 实现方式 |
|--------|----------|
| **批量消费** | `canalMsgConsumer.getMessage(timeout)` 批量拉取消息 |
| **分批同步** | `batchSync()` 按 `syncBatchSize` 分批调用 `adapter.sync()` |
| **并行处理** | `canalOuterAdapters` 组间并行，组内串行 |
| **多线程分区** | RDB 同步支持多线程分区执行，按主键 hash 分配到不同线程 |
| **批量写入** | RDB 使用手动事务；ES 使用 BulkRequest；HBase 使用 `table.put(List<Put>)` |
| **全量同步多线程** | `AbstractEtlService` 在数据量 > 1 万时自动启用多线程分页导入 |
| **目标列类型缓存** | RDB `columnsTypeCache`、ES `esFieldTypes`，避免重复查询元数据 |

### 8.4 全量 + 增量的同步模型

```
┌─────────────────────────────────────────────────────────────────┐
│                        Canal Adapter                            │
│                                                                 │
│  ┌───────────────────────────┐  ┌────────────────────────────┐ │
│  │      增量同步（实时）       │  │      全量同步（按需）        │ │
│  │                           │  │                            │ │
│  │  Canal Server             │  │  REST API / 命令行触发      │ │
│  │    → getMessage()         │  │    → etl(task, params)     │ │
│  │    → Dml                  │  │    → AbstractEtlService    │ │
│  │    → adapter.sync(dmls)   │  │    → importData(sql)       │ │
│  │    → INSERT/UPDATE/DELETE │  │    → 分页查询源库           │ │
│  │    → ack()                │  │    → 批量写入目标库         │ │
│  │                           │  │                            │ │
│  │  特点:                    │  │  特点:                     │ │
│  │  - 实时，毫秒级延迟        │  │  - 全量，支持条件过滤       │ │
│  │  - 自动重试+回滚           │  │  - 多线程并行导入           │ │
│  │  - 消费位点管理             │  │  - 先删后插保证幂等         │ │
│  └───────────────────────────┘  └────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

增量同步和全量同步是互补的：
1. **全量同步**：用于初始化目标库数据，或者修复数据不一致
2. **增量同步**：用于实时追踪源库变更，持续同步到目标库

典型工作流程：
1. 启动 Adapter，建立与 Canal Server 的连接
2. 调用 REST API 触发全量同步，将历史数据导入目标库
3. 增量同步自动接管，持续消费 binlog 变更
4. 如果发现数据不一致，可以再次触发全量同步修复

### 8.5 容错与可靠性设计

```
消息消费循环:

    for (retry : 0 → maxRetries) {
        try {
            messages = getMessage(timeout);
            writeOut(messages);     // 分发到各 adapter
            ack();                  // 成功，确认消费
            break;
        } catch (Exception e) {
            if (retry < maxRetries - 1) {
                rollback();         // 失败，回滚重试
            } else {
                if (terminateOnException) {
                    rollback();     // 最终失败，关闭同步开关
                    syncSwitch.off(destination);
                } else {
                    ack();          // 跳过错误数据，继续
                }
            }
        }
    }
```

| 容错机制 | 说明 |
|----------|------|
| **重试** | 可配置重试次数，`retries = -1` 表示无限重试 |
| **回滚** | 失败时 rollback 消息，下次重新消费 |
| **熔断** | `terminateOnException = true` 时，达到最大重试次数后关闭同步开关 |
| **跳过** | `terminateOnException = false` 时，达到最大重试次数后 ACK 跳过 |
| **同步开关** | `SyncSwitch` 支持本地/ZK 分布式两种模式的同步开关控制 |
| **断线重连** | 消费循环外层 `while (running)` 保证断线后自动重连 |
| **主键冲突** | RDB 适配器 `skipDupException` 配置可跳过主键冲突异常 |

### 8.6 配置热更新

Adapter 支持映射配置的热更新：

1. **RdbConfigMonitor** / **HbaseConfigMonitor**：监控 `conf/<type>/` 目录下的 YAML 文件变更
2. 文件新增 → `addConfig()`
3. 文件修改 → `updateConfig()`
4. 文件删除 → `deleteConfig()`

配置热更新不需要重启 Adapter 进程，实现了运行时动态调整映射规则。

---

## 九、全链路数据流总结

最后，我们以一条 MySQL INSERT 语句为例，完整追踪数据从源库到目标库的全流程：

```
1. MySQL 源库执行: INSERT INTO user (id, name, age) VALUES (1, 'Alice', 25)
         │
         ▼
2. MySQL 写入 binlog (ROW 格式)
         │
         ▼
3. Canal Server (EventParser)
   - 伪装成 slave，通过 COM_BINLOG_DUMP 获取 binlog
   - 解析 WRITE_ROWS_EVENT
   - 转换为 CanalEntry.RowChange
   - 经过 EventSink 过滤 → 存入 EventStore (RingBuffer)
         │
         ▼
4. AdapterProcessor.process()
   - canalMsgConsumer.getMessage(timeout)
   - 获取到 List<CommonMessage>
         │
         ▼
5. AdapterProcessor.writeOut(commonMessages)
   - MessageUtil.flatMessage2Dml() 转换为 List<Dml>
   - Dml {
       destination: "example",
       database: "mydb",
       table: "user",
       type: "INSERT",
       data: [{id: 1, name: "Alice", age: 25}],
       pkNames: ["id"]
     }
         │
         ▼
6. batchSync(dmls, adapter)
   - 按 syncBatchSize 分批
   - adapter.sync(dmls)
         │
         ├──── RDB Adapter ────────────────────────────────────┐
         │  - 查找 MappingConfig (destination_mydb-user)        │
         │  - SingleDml.dml2SingleDmls() 拆分为单行操作          │
         │  - pkHash() 计算分区号                               │
         │  - insert(batchExecutor, config, singleDml)          │
         │    → INSERT INTO target_user (`id`,`name`,`age`)     │
         │      VALUES (?,?,?)                                  │
         │  - batchExecutor.commit()                            │
         │                                                      │
         ├──── ES Adapter ─────────────────────────────────────┐
         │  - ESSyncService.sync(config, dml)                   │
         │  - esTemplate.insert(mapping, pkVal, esFieldData)    │
         │    → IndexRequest("user_index", "1")                 │
         │      .source({name: "Alice", age: 25})               │
         │  - BulkRequest.add(indexRequest)                     │
         │  - commit() → restHighLevelClient.bulk()             │
         │                                                      │
         ├──── HBase Adapter ──────────────────────────────────┐
         │  - hbaseSyncService.sync(config, dml)                │
         │  - insert(config, dml)                               │
         │    → HRow { rowKey: "1", cells: [                    │
         │        {family: "CF", qualifier: "NAME", value: ...}, │
         │        {family: "CF", qualifier: "AGE",  value: ...}  │
         │      ]}                                              │
         │  - hbaseTemplate.puts("MYDB:T_USER", rows)           │
         │    → table.put(List<Put>)                            │
         │                                                      │
         ▼
7. canalMsgConsumer.ack()
   - 确认消费成功，更新消费位点
```

这就是 Canal Client-Adapter 数据同步到下游存储的完整链路。整个框架通过 SPI 插件化、配置驱动映射、批量处理优化三大核心设计，实现了对多种下游存储的统一、高效、可靠的数据同步。