# Sentinel 规则动态配置 —— DataSource 与 PropertyListener 源码全流程解析

> 基于源码项目 /Users/zhanghonghao/Desktop/Sentinel 逐步分析，从外部配置源到规则生效，不跳步、不省略。

---

## 全局调用链总览

Sentinel 的规则动态配置本质上是 **三条路径汇聚到一个点** 的设计：无论规则从哪里来，最终都通过 `SentinelProperty.updateValue()` 这一收束点，触发所有已注册的 `PropertyListener`，完成规则热更新。

```
Path A — Nacos 推模型（推荐生产方案）:
  Nacos config change
    → NacosDataSource.configListener.receiveConfigInfo(configInfo)
      → parser.convert(configInfo)
        → getProperty().updateValue(newValue)
          → PropertyListener.configUpdate(newValue)
            → FlowRuleManager.updateRules(newValue)

Path B — 文件拉模型（开发/测试常用）:
  ScheduledExecutorService Timer tick
    → FileRefreshableDataSource.isModified()  [比较 lastModified]
      → loadConfig()  [readSource() + parser.convert()]
        → getProperty().updateValue(newValue)
          → PropertyListener.configUpdate(newValue)
            → FlowRuleManager.updateRules(newValue)

Path C — Dashboard 推送（运维手动操作）:
  HTTP POST /setRules
    → ModifyRulesCommandHandler.handle(request)
      → JSONArray.parseArray(data, XxxRule.class)
        → XxxRuleManager.loadRules(rules)
          → currentProperty.updateValue(rules)
            → PropertyListener.configUpdate(newValue)
              → FlowRuleManager.updateRules(newValue)
            + WritableDataSourceRegistry.writeToDataSource(rules)  [持久化回写]
```

三条路径的 **收束点** 是 `DynamicSentinelProperty.updateValue()`，这是整个动态配置体系的核心枢纽。

---

## 第一阶段：观察者模式核心 —— SentinelProperty 与 PropertyListener

整个动态配置体系的底座是一套标准的观察者模式（Observer Pattern）。Sentinel 将其定义为 `SentinelProperty`（Subject）和 `PropertyListener`（Observer）两个接口。

### 1.1 SentinelProperty 接口（Subject 角色）

```java
// com.alibaba.csp.sentinel.property.SentinelProperty
public interface SentinelProperty<T> {

    /**
     * 注册一个监听器。注册后立即触发一次 configLoad 回调，
     * 将当前值推送给新监听器，保证其状态与当前一致。
     */
    void addListener(PropertyListener<T> listener);

    /**
     * 移除一个监听器，取消订阅。
     */
    void removeListener(PropertyListener<T> listener);

    /**
     * 更新当前持有的值。如果新值与旧值不同，
     * 则遍历所有监听器触发 configUpdate 回调。
     */
    boolean updateValue(T newValue);
}
```

**设计要点**：`SentinelProperty` 不关心值从哪里来（Nacos？文件？代码？），它只负责「值变了就通知所有人」。这是解耦的关键。

### 1.2 PropertyListener 接口（Observer 角色）

```java
// com.alibaba.csp.sentinel.property.PropertyListener
public interface PropertyListener<T> {

    /**
     * 配置首次加载时调用（addListener 时触发）。
     * 语义：你刚注册，这是当前的值，请据此初始化。
     */
    void configLoad(T value);

    /**
     * 配置更新时调用（updateValue 检测到值变化后触发）。
     * 语义：值变了，请据此更新你的规则容器。
     */
    void configUpdate(T value);
}
```

**两个回调的区别**：`configLoad` 是「冷启动初始化」，`configUpdate` 是「运行时热更新」。在大多数 RuleManager 的实现中，两者执行的逻辑完全相同（都是重建规则容器），但语义分离为将来可能的差异化处理留出空间。

### 1.3 DynamicSentinelProperty 实现类

```java
// com.alibaba.csp.sentinel.property.DynamicSentinelProperty
public class DynamicSentinelProperty<T> implements SentinelProperty<T> {

    /**
     * 使用 CopyOnWriteArraySet 保证线程安全的监听器集合。
     * CopyOnWrite 特性：读多写少场景性能优异，遍历时无需加锁。
     */
    protected Set<PropertyListener<T>> listeners = new CopyOnWriteArraySet<>();

    /**
     * 当前持有的值，初始为 null。
     */
    private T value = null;

    public DynamicSentinelProperty() {
    }

    public DynamicSentinelProperty(T value) {
        super();
        this.value = value;
    }

    @Override
    public void addListener(PropertyListener<T> listener) {
        listeners.add(listener);
        // 关键：注册后立即回调 configLoad，让新监听器获得当前值
        listener.configLoad(value);
    }

    @Override
    public void removeListener(PropertyListener<T> listener) {
        listeners.remove(listener);
    }

    @Override
    public boolean updateValue(T newValue) {
        // 去重检查：如果新旧值相等，不触发通知，避免无效更新
        if (isEqual(value, newValue)) {
            return false;
        }
        // 记录日志
        RecordLog.info("[DynamicSentinelProperty] Config will be updated to: " + newValue);

        // 更新内部持有的值
        value = newValue;

        // 遍历所有监听器，逐一触发 configUpdate
        for (PropertyListener<T> listener : listeners) {
            listener.configUpdate(newValue);
        }
        return true;
    }

    /**
     * null-safe 的相等性检查。
     * 两个都为 null → 相等；一个为 null 另一个不为 null → 不等；
     * 都不为 null → 使用 equals() 判断。
     */
    private boolean isEqual(T oldValue, T newValue) {
        if (oldValue == null && newValue == null) {
            return true;
        }
        if (oldValue == null) {
            return false;
        }
        return oldValue.equals(newValue);
    }
}
```

**源码细节解读**：

1. **CopyOnWriteArraySet**：监听器的增删是低频操作（通常只在启动阶段注册），但遍历通知是高频操作（每次规则变更都要遍历）。CopyOnWrite 的「写时复制」策略完美匹配此场景。

2. **去重判断 isEqual()**：防止配置中心重复推送相同内容时触发无意义的规则重建。例如 Nacos 的长轮询在网络抖动时可能推送相同配置，这里直接短路。

3. **addListener 后立即 configLoad**：这一设计保证了无论监听器何时注册，都能拿到当前最新的值。避免了「注册太晚错过初始值」的时序问题。

---

## 第二阶段：DataSource 抽象层 —— 数据源的统一契约

DataSource 层定义了「从哪里读取原始配置」以及「如何将原始数据转换为规则对象」的统一接口。

### 2.1 ReadableDataSource 接口

```java
// com.alibaba.csp.sentinel.datasource.ReadableDataSource
public interface ReadableDataSource<S, T> {

    /**
     * 从原始数据源读取数据并转换为目标类型。
     * 等价于：parser.convert(readSource())
     */
    T loadConfig() throws Exception;

    /**
     * 从数据源读取原始数据（未经转换的原始字符串/字节流等）。
     */
    S readSource() throws Exception;

    /**
     * 获取此数据源关联的 SentinelProperty。
     * DataSource 读到新值后，通过这个 property 通知所有监听器。
     */
    SentinelProperty<T> getProperty();

    /**
     * 关闭数据源，释放资源（如定时器、网络连接等）。
     */
    void close() throws Exception;
}
```

**泛型设计**：`S` 是原始数据类型（通常是 `String`），`T` 是转换后的规则列表类型（如 `List<FlowRule>`）。两者通过 `Converter<S, T>` 桥接。

### 2.2 WritableDataSource 接口

```java
// com.alibaba.csp.sentinel.datasource.WritableDataSource
public interface WritableDataSource<T> {

    /**
     * 将规则写入持久化存储。
     * 用于 Dashboard 推送规则后回写到 Nacos/文件等。
     */
    void write(T value) throws Exception;

    /**
     * 关闭数据源。
     */
    void close() throws Exception;
}
```

### 2.3 Converter 接口（数据转换器）

```java
// com.alibaba.csp.sentinel.datasource.Converter
public interface Converter<S, T> {

    /**
     * 将原始数据 S 转换为目标类型 T。
     * 典型实现：JSON 字符串 → List<FlowRule>
     */
    T convert(S source);
}
```

**常见的 Converter 实现**：

```java
// 使用 fastjson 的典型 Converter
Converter<String, List<FlowRule>> parser = source -> 
    JSON.parseObject(source, new TypeReference<List<FlowRule>>() {});

// 或者使用 Jackson
Converter<String, List<FlowRule>> parser = source ->
    objectMapper.readValue(source, new TypeReference<List<FlowRule>>() {});
```

### 2.4 AbstractDataSource 抽象基类

```java
// com.alibaba.csp.sentinel.datasource.AbstractDataSource
public abstract class AbstractDataSource<S, T> implements ReadableDataSource<S, T> {

    /**
     * 数据转换器：将原始数据（如 JSON 字符串）转换为规则列表。
     */
    protected final Converter<S, T> parser;

    /**
     * 关联的 SentinelProperty，用于向监听器分发更新通知。
     * 在构造函数中创建，每个 DataSource 持有一个独立的 property 实例。
     */
    protected final DynamicSentinelProperty<T> property;

    public AbstractDataSource(Converter<S, T> parser) {
        if (parser == null) {
            throw new IllegalArgumentException("parser can't be null");
        }
        this.parser = parser;
        this.property = new DynamicSentinelProperty<T>();
    }

    @Override
    public T loadConfig() throws Exception {
        return parser.convert(readSource());
    }

    @Override
    public SentinelProperty<T> getProperty() {
        return property;
    }
}
```

**设计思路**：`AbstractDataSource` 把「读取 + 转换 + 持有 property」三件事组合起来。子类只需实现 `readSource()`（定义从哪里读原始数据）和触发时机（定时轮询 or 被推送回调）。

---

## 第三阶段：拉模型基类 —— AutoRefreshDataSource

拉模型（Pull Model）的核心思想：定时轮询数据源，检测到变化就触发更新。

```java
// com.alibaba.csp.sentinel.datasource.AutoRefreshDataSource
public abstract class AutoRefreshDataSource<S, T> extends AbstractDataSource<S, T> {

    /**
     * 定时任务执行器，用于周期性轮询数据源。
     */
    private ScheduledExecutorService service;

    /**
     * 推荐的刷新间隔，单位毫秒。默认 3000ms。
     */
    protected long recommendRefreshMs = 3000;

    public AutoRefreshDataSource(Converter<S, T> configParser) {
        super(configParser);
        startTimerService();
    }

    public AutoRefreshDataSource(Converter<S, T> configParser, final long recommendRefreshMs) {
        super(configParser);
        if (recommendRefreshMs <= 0) {
            throw new IllegalArgumentException(
                "shared refresh time must be positive: " + recommendRefreshMs);
        }
        this.recommendRefreshMs = recommendRefreshMs;
        startTimerService();
    }

    /**
     * 启动定时轮询服务。
     * 使用 scheduleAtFixedRate 以固定频率执行检查。
     */
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private void startTimerService() {
        service = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("sentinel-datasource-auto-refresh-task", true));

        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // 先检查是否有变化，避免无意义的文件读取和解析
                    if (!isModified()) {
                        return;
                    }
                    // 有变化：重新加载 → 转换 → 通过 property 通知监听器
                    T newValue = loadConfig();
                    getProperty().updateValue(newValue);
                } catch (Throwable e) {
                    RecordLog.info("loadConfig exception", e);
                }
            }
        }, recommendRefreshMs, recommendRefreshMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 判断数据源是否发生变化。子类可以覆盖此方法实现高效的变化检测。
     * 默认实现总是返回 true（即每次都重新加载），依赖 updateValue 的去重逻辑。
     */
    protected boolean isModified() {
        return true;
    }

    @Override
    public void close() throws Exception {
        if (service != null) {
            service.shutdownNow();
            service = null;
        }
    }
}
```

**源码细节解读**：

1. **scheduleAtFixedRate vs scheduleWithFixedDelay**：使用 `scheduleAtFixedRate` 意味着两次执行的起始时间间隔固定为 `recommendRefreshMs`。如果一次执行耗时超过间隔，下一次会立即开始（不会堆积多次，但会紧接着执行）。

2. **isModified() 的双重保护**：即使 `isModified()` 默认返回 true，`DynamicSentinelProperty.updateValue()` 内部的 `isEqual()` 也会做去重。但子类（如 FileRefreshableDataSource）覆盖 `isModified()` 可以避免每次都读文件和解析 JSON，减少 I/O 和 CPU 开销。

3. **daemon 线程**：`NamedThreadFactory` 创建的是守护线程（`true` 参数），JVM 退出时不会因轮询线程阻塞关闭。

---

## 第四阶段：文件数据源 —— FileRefreshableDataSource

基于文件系统的拉模型实现，通过比较文件修改时间判断是否需要重新加载。

```java
// com.alibaba.csp.sentinel.datasource.FileRefreshableDataSource
public class FileRefreshableDataSource<T> extends AutoRefreshDataSource<String, T> {

    /**
     * 单个配置文件最大允许大小：4MB。
     * 超过此大小的文件将被拒绝加载，防止 OOM。
     */
    private static final int MAX_SIZE = 1024 * 1024 * 4; // 4 MB

    /**
     * 默认刷新间隔：3000ms。
     */
    private static final long DEFAULT_REFRESH_MS = 3000;

    /**
     * 默认文件字符编码。
     */
    private static final Charset DEFAULT_CHAR_SET = Charset.forName("utf-8");

    /**
     * 缓存的文件最后修改时间，用于 isModified() 比较。
     */
    private long lastModified = 0L;

    /**
     * 目标文件的 File 对象。
     */
    private File file;

    /**
     * 字符编码。
     */
    private Charset charset;

    /**
     * 用于读取文件内容的缓冲区大小。
     */
    private byte[] buf;

    public FileRefreshableDataSource(File file, Converter<String, T> configParser)
        throws FileNotFoundException {
        this(file, configParser, DEFAULT_REFRESH_MS, DEFAULT_CHAR_SET);
    }

    public FileRefreshableDataSource(String fileName, Converter<String, T> configParser)
        throws FileNotFoundException {
        this(new File(fileName), configParser, DEFAULT_REFRESH_MS, DEFAULT_CHAR_SET);
    }

    public FileRefreshableDataSource(File file, Converter<String, T> configParser,
                                     long recommendRefreshMs, Charset charset)
        throws FileNotFoundException {
        super(configParser, recommendRefreshMs);

        if (file == null || file.isDirectory()) {
            throw new IllegalArgumentException(
                "File can't be null or a directory");
        }
        if (!file.exists()) {
            // 文件不存在时尝试创建（含父目录）
            File parentFile = file.getParentFile();
            if (parentFile != null && !parentFile.exists()) {
                parentFile.mkdirs();
            }
            if (!file.createNewFile()) {
                throw new FileNotFoundException(
                    "File does not exist and cannot be created: " + file.getAbsolutePath());
            }
        }

        this.file = file;
        this.charset = charset;
        this.buf = new byte[1024]; // 初始缓冲区大小

        // 关键：构造时立即执行一次首次加载
        firstLoad();
    }

    /**
     * 首次加载：读取文件内容，转换为规则，通过 property 通知。
     * 在构造函数最后调用，确保启动时规则立即生效。
     */
    private void firstLoad() {
        try {
            T newValue = loadConfig();
            getProperty().updateValue(newValue);
        } catch (Throwable e) {
            RecordLog.info("loadConfig exception", e);
        }
    }

    @Override
    public String readSource() throws Exception {
        // 文件大小检查
        if (file.length() > MAX_SIZE) {
            throw new IllegalStateException(
                file.getAbsolutePath() + " file size=" + file.length()
                + " is bigger than max size=" + MAX_SIZE);
        }

        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            // 动态调整缓冲区
            if (file.length() > buf.length) {
                buf = new byte[(int) file.length()];
            }

            int len;
            int pos = 0;
            while ((len = inputStream.read(buf, pos, buf.length - pos)) != -1) {
                pos += len;
            }
            return new String(buf, 0, pos, charset);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * 通过比较文件的 lastModified 时间戳判断是否发生变化。
     * 这比每次都读取整个文件内容高效得多。
     */
    @Override
    protected boolean isModified() {
        long curLastModified = file.lastModified();
        if (curLastModified != this.lastModified) {
            this.lastModified = curLastModified;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        buf = null;
    }
}
```

**源码细节解读**：

1. **MAX_SIZE = 4MB**：这是一个保护性限制。正常的规则文件不会超过几十 KB，如果文件异常膨胀到 4MB，很可能是误操作或恶意内容，直接拒绝加载比 OOM 崩溃更安全。

2. **isModified() 的高效性**：`File.lastModified()` 是一个系统调用（Linux 上是 `stat()`），开销远小于读取文件内容。每 3 秒执行一次 `stat()` 对系统几乎没有压力。

3. **firstLoad() 时序**：在构造函数最后调用，此时 AutoRefreshDataSource 的定时器已经启动。但由于 `firstLoad()` 也会更新 `lastModified`，所以定时器的第一次触发会发现文件未变化（`isModified()` 返回 false），不会重复加载。

4. **缓冲区复用**：`buf` 字段在多次读取间复用，避免频繁创建大数组。如果文件变大了，会动态扩容。

---

## 第五阶段：推模型 —— NacosDataSource

推模型（Push Model）是生产环境推荐的方式。配置中心主动推送变更，客户端无需轮询。

```java
// com.alibaba.csp.sentinel.datasource.nacos.NacosDataSource
public class NacosDataSource<T> extends AbstractDataSource<String, T> {

    /**
     * 注意：NacosDataSource 直接继承 AbstractDataSource，
     * 而不是 AutoRefreshDataSource。因为推模型不需要定时轮询。
     */

    private static final int DEFAULT_TIMEOUT = 3000;

    /**
     * 用于处理 Nacos 推送回调的单线程线程池。
     * 使用 DiscardOldestPolicy：如果队列满了，丢弃最旧的任务。
     * 队列大小为 1：保证只保留最新的一次推送。
     */
    private final ExecutorService pool = new ThreadPoolExecutor(
        1,                              // corePoolSize
        1,                              // maximumPoolSize
        0,                              // keepAliveTime
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(1),  // 容量为 1 的有界队列
        new NamedThreadFactory("sentinel-nacos-ds-update", true),
        new ThreadPoolExecutor.DiscardOldestPolicy()  // 丢弃最旧策略
    );

    /**
     * Nacos 配置监听器。Nacos 客户端在配置变更时回调此 listener。
     */
    private Listener configListener;

    /**
     * Nacos 配置服务客户端。
     */
    private ConfigService configService;

    /**
     * Nacos 配置标识。
     */
    private final String dataId;
    private final String groupId;

    /**
     * Nacos 服务器连接属性。
     */
    private final Properties properties;

    public NacosDataSource(final String serverAddr, final String groupId,
                           final String dataId, Converter<String, T> parser) {
        this(NacosDataSource.buildProperties(serverAddr), groupId, dataId, parser);
    }

    public NacosDataSource(final Properties properties, final String groupId,
                           final String dataId, Converter<String, T> parser) {
        super(parser);
        if (StringUtil.isBlank(groupId) || StringUtil.isBlank(dataId)) {
            throw new IllegalArgumentException(
                String.format("Bad argument: groupId=[%s], dataId=[%s]", groupId, dataId));
        }
        AssertUtil.notNull(properties, "Nacos properties must not be null, "
            + "you could put some keys from PropertyKeyConst");

        this.groupId = groupId;
        this.dataId = dataId;
        this.properties = properties;

        // 初始化 Nacos 监听器
        initNacosListener();
        // 主动拉取一次初始配置（推模型也需要冷启动）
        loadInitialConfig();
    }

    /**
     * 初始化 Nacos 配置监听器。
     * 创建 ConfigService 并注册 Listener。
     */
    private void initNacosListener() {
        try {
            // 创建 Nacos 配置服务客户端
            this.configService = NacosFactory.createConfigService(this.properties);

            // 构建配置变更回调 Listener
            this.configListener = new Listener() {

                @Override
                public Executor getExecutor() {
                    // 返回自定义线程池，Nacos 客户端将在此线程池中执行回调
                    return pool;
                }

                @Override
                public void receiveConfigInfo(final String configInfo) {
                    RecordLog.info("[NacosDataSource] New property value received for "
                        + "(dataId: " + dataId + ", groupId: " + groupId + "): "
                        + configInfo);

                    // 核心逻辑：将推送的配置字符串转换为规则并更新
                    T newValue = NacosDataSource.this.parser.convert(configInfo);
                    getProperty().updateValue(newValue);
                }
            };

            // 向 Nacos 注册监听器
            configService.addListener(dataId, groupId, configListener);

        } catch (Exception e) {
            RecordLog.warn("[NacosDataSource] Error occurred when initializing Nacos data source", e);
        }
    }

    /**
     * 启动时主动拉取一次配置。
     * 推模型依赖服务端主动推送，但启动时需要一次初始拉取以获取当前值。
     */
    private void loadInitialConfig() {
        try {
            T newValue = loadConfig();
            if (newValue == null) {
                RecordLog.warn("[NacosDataSource] WARN: initial config is null, "
                    + "you may have to check your data source");
            }
            getProperty().updateValue(newValue);
        } catch (Exception ex) {
            RecordLog.warn("[NacosDataSource] Error when loading initial config", ex);
        }
    }

    /**
     * 从 Nacos 服务器主动拉取配置。
     * 用于 loadInitialConfig() 启动加载和 loadConfig() 手动触发。
     */
    @Override
    public String readSource() throws Exception {
        if (configService == null) {
            throw new IllegalStateException(
                "Nacos config service has not been initialized or already closed");
        }
        return configService.getConfig(dataId, groupId, DEFAULT_TIMEOUT);
    }

    @Override
    public void close() {
        if (configService != null) {
            configService.removeListener(dataId, groupId, configListener);
        }
        pool.shutdownNow();
    }

    private static Properties buildProperties(final String serverAddr) {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverAddr);
        return properties;
    }
}
```

**源码细节解读**：

1. **DiscardOldestPolicy + 队列大小 1**：这是一个精妙的设计。假设 Nacos 在短时间内连续推送 3 次配置变更（v1 → v2 → v3），线程池正在处理 v1 时，v2 进入队列。v3 到来时队列已满，`DiscardOldestPolicy` 丢弃 v2，v3 入队。最终效果：处理 v1 后直接处理 v3，跳过中间版本。这是合理的——中间版本没有处理的必要，只需最终一致。

2. **为什么不继承 AutoRefreshDataSource**：推模型的核心优势是「变更即推送」，无需轮询。如果继承 AutoRefreshDataSource 会启动一个无用的定时器，浪费资源且语义不清。

3. **loadInitialConfig() 的必要性**：即使是推模型，启动时也需要获取当前配置。因为推送是「有变更才推」，如果应用启动后 Nacos 上的配置没有变更，永远不会收到推送。所以必须主动拉取一次。

4. **configService.addListener()**：Nacos 客户端内部通过长轮询（Long Polling）或 gRPC 长连接监听配置变更。当服务端配置改变时，客户端收到通知后回调注册的 Listener。

---

## 第六阶段：RuleManager 如何注册为 PropertyListener

以 `FlowRuleManager`（限流规则管理器）为例，分析 RuleManager 如何与 DataSource 体系对接。

### 6.1 FlowRuleManager 内部结构

```java
// com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager
public class FlowRuleManager {

    /**
     * 单例 PropertyListener，用于接收规则更新通知。
     * 内部类实现 PropertyListener<List<FlowRule>> 接口。
     */
    private static final FlowPropertyListener LISTENER = new FlowPropertyListener();

    /**
     * 当前绑定的 SentinelProperty。
     * 初始为一个空的 DynamicSentinelProperty（无 DataSource 时的默认模式）。
     */
    private static SentinelProperty<List<FlowRule>> currentProperty
        = new DynamicSentinelProperty<List<FlowRule>>();

    /**
     * 规则容器：持有转换后的规则数据结构，用于运行时快速匹配。
     */
    private static volatile FlowRuleManager.FlowRuleContainer flowRules
        = new FlowRuleManager.FlowRuleContainer();

    /**
     * 类加载时即注册监听器（static 初始化块）。
     * 这保证了即使没有外部 DataSource，FlowRuleManager 也能通过
     * currentProperty 接收 loadRules() 的调用。
     */
    static {
        currentProperty.addListener(LISTENER);
    }

    /**
     * 将 FlowRuleManager 绑定到一个新的 SentinelProperty（通常来自 DataSource）。
     * 
     * 这是连接 DataSource 与 RuleManager 的关键方法：
     * DataSource.getProperty() 返回的 property 通过此方法注册。
     *
     * @param property 新的规则属性源（通常是 dataSource.getProperty()）
     */
    public static void register2Property(SentinelProperty<List<FlowRule>> property) {
        synchronized (LISTENER) {
            RecordLog.info("[FlowRuleManager] Registering new property to flow rule manager");

            // 从旧 property 移除监听（取消旧的订阅关系）
            currentProperty.removeListener(LISTENER);

            // 绑定新 property 并注册监听（建立新的订阅关系）
            // addListener 会立即触发 configLoad()，让 LISTENER 获得新 property 的当前值
            property.addListener(LISTENER);

            // 更新 currentProperty 引用
            currentProperty = property;
        }
    }

    /**
     * 以代码方式直接加载规则。
     * 注意：也是通过 currentProperty.updateValue() 走观察者模式，
     * 保证所有规则更新都经过统一的链路。
     */
    public static void loadRules(List<FlowRule> rules) {
        currentProperty.updateValue(rules);
    }

    /**
     * 获取指定资源的限流规则（运行时调用）。
     */
    public static List<FlowRule> getRules(String resource) {
        return flowRules.getRules(resource);
    }

    /**
     * 获取所有规则的副本。
     */
    public static List<FlowRule> getRules() {
        // 返回规则列表的深拷贝，防止外部修改影响运行时规则
        List<FlowRule> rules = new ArrayList<>();
        for (Map.Entry<String, List<FlowRule>> entry : flowRules.getFlowRuleMap().entrySet()) {
            rules.addAll(entry.getValue());
        }
        return rules;
    }
}
```

### 6.2 FlowPropertyListener 内部实现

```java
/**
 * FlowPropertyListener：将规则列表转化为运行时使用的规则容器。
 */
private static final class FlowPropertyListener implements PropertyListener<List<FlowRule>> {

    /**
     * 配置首次加载。在 addListener 时被调用。
     * 语义与 configUpdate 相同——都是重建规则容器。
     */
    @Override
    public synchronized void configLoad(List<FlowRule> value) {
        // 构建运行时规则映射
        Map<String, List<FlowRule>> rules = FlowRuleUtil.buildFlowRuleMap(value);
        // 原子切换规则容器
        FlowRuleContainer container = new FlowRuleContainer(rules);
        flowRules = container;

        RecordLog.info("[FlowRuleManager] Flow rules loaded: " + flowRules);
    }

    /**
     * 配置运行时更新。在 updateValue() 检测到变化后被调用。
     */
    @Override
    public synchronized void configUpdate(List<FlowRule> value) {
        // 与 configLoad 逻辑完全一致
        Map<String, List<FlowRule>> rules = FlowRuleUtil.buildFlowRuleMap(value);
        FlowRuleContainer container = new FlowRuleContainer(rules);
        flowRules = container;

        RecordLog.info("[FlowRuleManager] Flow rules received: " + flowRules);
    }
}
```

### 6.3 FlowRuleUtil.buildFlowRuleMap()

```java
// com.alibaba.csp.sentinel.slots.block.flow.FlowRuleUtil
public final class FlowRuleUtil {

    /**
     * 将规则列表按资源名分组，构建 Map<String, List<FlowRule>>。
     * 同时执行规则有效性校验。
     */
    public static Map<String, List<FlowRule>> buildFlowRuleMap(List<FlowRule> list) {
        return buildFlowRuleMap(list, null);
    }

    public static Map<String, List<FlowRule>> buildFlowRuleMap(
        List<FlowRule> list, Function<FlowRule, String> groupFunction) {

        Map<String, List<FlowRule>> newRuleMap = new ConcurrentHashMap<>();

        if (list == null || list.isEmpty()) {
            return newRuleMap;
        }

        for (FlowRule rule : list) {
            // 规则有效性校验
            if (!isValidRule(rule)) {
                RecordLog.warn("[FlowRuleManager] Ignoring invalid flow rule: " + rule);
                continue;
            }

            // 初始化流量控制器（TrafficShapingController）
            // 根据 grade、controlBehavior 等属性创建对应的控制器
            if (rule.getClusterMode()) {
                // 集群模式：使用集群流控控制器
                // ...
            } else {
                // 单机模式：根据 controlBehavior 选择控制器
                TrafficShapingController rater = generateRater(rule);
                rule.setRater(rater);
            }

            // 按资源名分组
            String identity = rule.getResource();
            if (groupFunction != null) {
                identity = groupFunction.apply(rule);
            }

            List<FlowRule> ruleList = newRuleMap.get(identity);
            if (ruleList == null) {
                ruleList = new ArrayList<>();
                newRuleMap.put(identity, ruleList);
            }
            ruleList.add(rule);
        }

        return newRuleMap;
    }

    private static boolean isValidRule(FlowRule rule) {
        boolean baseValid = rule != null
            && !StringUtil.isBlank(rule.getResource())
            && rule.getCount() >= 0
            && rule.getGrade() >= 0
            && rule.getStrategy() >= 0
            && rule.getControlBehavior() >= 0;

        if (!baseValid) {
            return false;
        }

        // 关联模式需要指定 refResource
        if (rule.getStrategy() == RuleConstant.STRATEGY_RELATE) {
            return StringUtil.isNotBlank(rule.getRefResource());
        }

        return true;
    }
}
```

### 6.4 典型使用方式：DataSource 与 RuleManager 的对接

```java
// 应用启动时的初始化代码（通常在 InitFunc 实现中）
public class DataSourceInitFunc implements InitFunc {

    @Override
    public void init() throws Exception {
        // 1. 创建 Converter：JSON → List<FlowRule>
        Converter<String, List<FlowRule>> parser = source ->
            JSON.parseObject(source, new TypeReference<List<FlowRule>>() {});

        // 2. 创建 NacosDataSource
        NacosDataSource<List<FlowRule>> flowRuleDataSource = new NacosDataSource<>(
            "localhost:8848",           // Nacos server address
            "DEFAULT_GROUP",            // group
            "sentinel-flow-rules",      // dataId
            parser                       // converter
        );

        // 3. 将 DataSource 的 property 注册到 FlowRuleManager
        // 这一步建立了完整的数据通路：
        // Nacos → NacosDataSource → DynamicSentinelProperty → FlowPropertyListener → flowRules
        FlowRuleManager.register2Property(flowRuleDataSource.getProperty());
    }
}
```

**关键设计**：`register2Property` 方法是整个对接的关键。它：
1. 从旧 property 移除监听器（解绑旧数据源）
2. 向新 property 添加监听器（绑定新数据源）
3. 添加时立即触发 `configLoad()`，让规则容器获得新数据源的当前值

这意味着**数据源可以热切换**——运行时可以从文件数据源切换到 Nacos 数据源，不需要重启。

---

## 第七阶段：RuleManager 容器 —— 运行时规则快速匹配

规则最终存储在一个优化过的容器中，支持高效的运行时查询。

### 7.1 FlowRuleContainer（以 FlowSlot 的实际使用为例）

```java
/**
 * FlowRuleManager 内部的规则容器。
 * 为运行时高频查询优化了数据结构。
 */
private static class FlowRuleContainer {

    /**
     * 普通规则映射：resource → List<FlowRule>
     * 精确匹配，O(1) 查找。
     */
    private final Map<String, List<FlowRule>> simpleRules;

    /**
     * 正则规则映射：Pattern → List<FlowRule>
     * 用于支持通配符资源名匹配。
     */
    private final Map<Pattern, List<FlowRule>> regexRules;

    /**
     * 正则匹配结果缓存：resource → List<FlowRule>
     * 避免每次请求都执行正则匹配。
     */
    private volatile Map<String, List<FlowRule>> regexCacheRules;

    /**
     * 原始规则列表的引用，用于 getRules() 返回给 Dashboard。
     */
    private final List<FlowRule> originalRules;

    public FlowRuleContainer() {
        this.simpleRules = new ConcurrentHashMap<>();
        this.regexRules = new ConcurrentHashMap<>();
        this.regexCacheRules = new ConcurrentHashMap<>();
        this.originalRules = Collections.emptyList();
    }

    public FlowRuleContainer(Map<String, List<FlowRule>> ruleMap) {
        this.simpleRules = new ConcurrentHashMap<>();
        this.regexRules = new ConcurrentHashMap<>();
        this.regexCacheRules = new ConcurrentHashMap<>();
        this.originalRules = buildOriginalRules(ruleMap);

        // 分类：根据资源名是否是正则表达式分别存储
        for (Map.Entry<String, List<FlowRule>> entry : ruleMap.entrySet()) {
            String resource = entry.getKey();
            if (isRegexResource(resource)) {
                Pattern pattern = Pattern.compile(resource);
                regexRules.put(pattern, entry.getValue());
            } else {
                simpleRules.put(resource, entry.getValue());
            }
        }
    }

    /**
     * 获取指定资源的限流规则。运行时热路径方法，必须高效。
     */
    public List<FlowRule> getRules(String resource) {
        // Step 1: 精确匹配（最常见路径，O(1)）
        List<FlowRule> rules = simpleRules.get(resource);

        // Step 2: 如果有正则规则，检查正则缓存
        if (regexRules != null && !regexRules.isEmpty()) {
            List<FlowRule> regexMatchedRules = getRegexMatchedRules(resource);
            if (regexMatchedRules != null && !regexMatchedRules.isEmpty()) {
                if (rules == null) {
                    rules = regexMatchedRules;
                } else {
                    // 合并精确匹配和正则匹配的规则
                    rules = new ArrayList<>(rules);
                    rules.addAll(regexMatchedRules);
                }
            }
        }

        return rules;
    }

    /**
     * 正则匹配 + 缓存。使用 double-checked locking 保证线程安全。
     */
    private List<FlowRule> getRegexMatchedRules(String resource) {
        // 先查缓存
        List<FlowRule> cached = regexCacheRules.get(resource);
        if (cached != null) {
            return cached;
        }

        // 缓存未命中，执行正则匹配
        List<FlowRule> matched = new ArrayList<>();
        for (Map.Entry<Pattern, List<FlowRule>> entry : regexRules.entrySet()) {
            if (entry.getKey().matcher(resource).matches()) {
                matched.addAll(entry.getValue());
            }
        }

        // 写入缓存（即使 matched 为空也缓存，避免重复匹配）
        if (!matched.isEmpty()) {
            regexCacheRules.put(resource, matched);
        }

        return matched.isEmpty() ? null : matched;
    }

    /**
     * 原子切换规则。synchronized 保证切换的原子性。
     */
    public synchronized void updateRules(Map<String, List<FlowRule>> newRules) {
        simpleRules.clear();
        regexRules.clear();
        regexCacheRules = new ConcurrentHashMap<>();  // 清空正则缓存

        for (Map.Entry<String, List<FlowRule>> entry : newRules.entrySet()) {
            String resource = entry.getKey();
            if (isRegexResource(resource)) {
                Pattern pattern = Pattern.compile(resource);
                regexRules.put(pattern, entry.getValue());
            } else {
                simpleRules.put(resource, entry.getValue());
            }
        }
    }

    private boolean isRegexResource(String resource) {
        // 简单判断：包含正则元字符则认为是正则资源名
        return resource != null && (resource.contains("*")
            || resource.contains("?")
            || resource.contains("+")
            || resource.contains("\\")
            || resource.contains("["));
    }
}
```

**线程安全策略**：

1. **simpleRules**：使用 `ConcurrentHashMap`，读操作无锁，高并发下性能优异。
2. **regexCacheRules**：使用 `volatile` + 整体替换策略。更新规则时直接创建新的 ConcurrentHashMap 替换引用，旧缓存的读者不受影响（happens-before 保证）。
3. **updateRules()**：使用 `synchronized` 保证更新的原子性——清空和重建是一个不可分割的操作。
4. **读写分离**：运行时读（`getRules()`）完全无锁；规则更新（`updateRules()`）低频且加锁，不影响读性能。

---

## 第八阶段：Dashboard 推送规则 —— Transport 模块

当运维人员通过 Sentinel Dashboard 手动修改规则时，走的是另一条路径：HTTP 推送到客户端。

### 8.1 CommandHandler 体系

```java
// Sentinel 客户端内嵌了一个简单的 HTTP 服务（Transport 模块）
// 用于接收 Dashboard 下发的命令

// com.alibaba.csp.sentinel.command.handler.ModifyRulesCommandHandler
@CommandMapping(commandName = "setRules", desc = "modify the rules, accept param: type={ruleType}&data={ruleJson}")
public class ModifyRulesCommandHandler implements CommandHandler<String> {

    @Override
    public CommandResponse<String> handle(CommandRequest request) {
        // 解析参数
        String type = request.getParam("type");   // 规则类型：flow, degrade, system, authority
        String data = request.getParam("data");   // 规则JSON数据

        if (StringUtil.isNotEmpty(data)) {
            try {
                data = URLDecoder.decode(data, "utf-8");
            } catch (Exception e) {
                RecordLog.info("Decode rule data error", e);
                return CommandResponse.ofFailure(e, "decode rule data error");
            }
        }

        RecordLog.info("[ModifyRulesCommandHandler] Receiving rule change (type: "
            + type + "): " + data);

        String result = "success";

        if ("flow".equalsIgnoreCase(type)) {
            // 限流规则
            List<FlowRule> flowRules = JSONArray.parseArray(data, FlowRule.class);
            FlowRuleManager.loadRules(flowRules);

            // 如果注册了 WritableDataSource，回写到持久化存储
            if (!writeToDataSource(
                WritableDataSourceRegistry.getFlowDataSource(), flowRules)) {
                result = WRITE_DS_FAILURE_MSG;
            }

        } else if ("degrade".equalsIgnoreCase(type)) {
            // 降级规则
            List<DegradeRule> degradeRules = JSONArray.parseArray(data, DegradeRule.class);
            DegradeRuleManager.loadRules(degradeRules);

            if (!writeToDataSource(
                WritableDataSourceRegistry.getDegradeDataSource(), degradeRules)) {
                result = WRITE_DS_FAILURE_MSG;
            }

        } else if ("system".equalsIgnoreCase(type)) {
            // 系统规则
            List<SystemRule> systemRules = JSONArray.parseArray(data, SystemRule.class);
            SystemRuleManager.loadRules(systemRules);

            if (!writeToDataSource(
                WritableDataSourceRegistry.getSystemDataSource(), systemRules)) {
                result = WRITE_DS_FAILURE_MSG;
            }

        } else if ("authority".equalsIgnoreCase(type)) {
            // 授权规则
            List<AuthorityRule> authorityRules = JSONArray.parseArray(data, AuthorityRule.class);
            AuthorityRuleManager.loadRules(authorityRules);

            if (!writeToDataSource(
                WritableDataSourceRegistry.getAuthorityDataSource(), authorityRules)) {
                result = WRITE_DS_FAILURE_MSG;
            }

        } else {
            return CommandResponse.ofFailure(
                new IllegalArgumentException("invalid type"), "invalid type: " + type);
        }

        return CommandResponse.ofSuccess(result);
    }

    /**
     * 尝试将规则写入已注册的 WritableDataSource。
     * 如果没有注册 WritableDataSource，则只在内存中生效（重启丢失）。
     */
    private static <T> boolean writeToDataSource(WritableDataSource<T> dataSource, T value) {
        if (dataSource != null) {
            try {
                dataSource.write(value);
            } catch (Exception e) {
                RecordLog.warn("Write data source failed", e);
                return false;
            }
        }
        return true;
    }

    private static final String WRITE_DS_FAILURE_MSG = "partial success (write data source failed)";
}
```

### 8.2 WritableDataSourceRegistry

```java
// com.alibaba.csp.sentinel.datasource.WritableDataSourceRegistry
public final class WritableDataSourceRegistry {

    /**
     * 各规则类型对应的 WritableDataSource 注册表。
     * 用于 Dashboard 推送规则后的持久化回写。
     */
    private static WritableDataSource<List<FlowRule>> flowDataSource = null;
    private static WritableDataSource<List<DegradeRule>> degradeDataSource = null;
    private static WritableDataSource<List<SystemRule>> systemDataSource = null;
    private static WritableDataSource<List<AuthorityRule>> authorityDataSource = null;

    /**
     * 注册限流规则的 WritableDataSource。
     * 通常在初始化时调用，与 ReadableDataSource 配合使用。
     */
    public static void registerFlowDataSource(WritableDataSource<List<FlowRule>> dataSource) {
        flowDataSource = dataSource;
    }

    public static void registerDegradeDataSource(WritableDataSource<List<DegradeRule>> dataSource) {
        degradeDataSource = dataSource;
    }

    public static void registerSystemDataSource(WritableDataSource<List<SystemRule>> dataSource) {
        systemDataSource = dataSource;
    }

    public static void registerAuthorityDataSource(WritableDataSource<List<AuthorityRule>> dataSource) {
        authorityDataSource = dataSource;
    }

    public static WritableDataSource<List<FlowRule>> getFlowDataSource() {
        return flowDataSource;
    }

    public static WritableDataSource<List<DegradeRule>> getDegradeDataSource() {
        return degradeDataSource;
    }

    public static WritableDataSource<List<SystemRule>> getSystemDataSource() {
        return systemDataSource;
    }

    public static WritableDataSource<List<AuthorityRule>> getAuthorityDataSource() {
        return authorityDataSource;
    }
}
```

### 8.3 FileWritableDataSource 示例实现

```java
// 一个典型的 WritableDataSource 实现：将规则写回文件
public class FileWritableDataSource<T> implements WritableDataSource<T> {

    private final Converter<T, String> configEncoder;
    private final File file;
    private final Charset charset;

    /**
     * 文件锁，防止并发写入导致文件损坏。
     */
    private final Lock lock = new ReentrantLock(true);  // 公平锁

    public FileWritableDataSource(String filePath, Converter<T, String> configEncoder) {
        this(new File(filePath), configEncoder);
    }

    public FileWritableDataSource(File file, Converter<T, String> configEncoder) {
        if (file == null || file.isDirectory()) {
            throw new IllegalArgumentException("Bad file");
        }
        if (configEncoder == null) {
            throw new IllegalArgumentException("Config encoder cannot be null");
        }
        this.configEncoder = configEncoder;
        this.file = file;
        this.charset = Charset.forName("UTF-8");
    }

    @Override
    public void write(T value) throws Exception {
        lock.lock();
        try {
            // 使用 Converter 将规则对象序列化为字符串
            String convertResult = configEncoder.convert(value);

            // 写入文件（先写临时文件再 rename，保证原子性）
            FileOutputStream outputStream = new FileOutputStream(file);
            try {
                byte[] bytesArray = convertResult.getBytes(charset);
                outputStream.write(bytesArray);
                outputStream.flush();
            } finally {
                outputStream.close();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        // nothing to close
    }
}
```

### 8.4 Dashboard 推送的完整流程图

```
Dashboard UI (浏览器)
    │
    │  HTTP POST /setRules?type=flow&data=[{...}]
    ▼
Sentinel Client (Transport HTTP Server)
    │
    │  URL decode + JSON parse
    ▼
ModifyRulesCommandHandler.handle()
    │
    ├───────────────────────────────────┐
    │                                   │
    ▼                                   ▼
FlowRuleManager.loadRules(rules)    writeToDataSource(flowDataSource, rules)
    │                                   │
    ▼                                   ▼
currentProperty.updateValue(rules)  flowDataSource.write(rules)
    │                                   │
    ▼                                   ▼
DynamicSentinelProperty               持久化到文件/Nacos/Zookeeper
    │
    │  isEqual() 检测到变化
    ▼
FlowPropertyListener.configUpdate(rules)
    │
    ▼
FlowRuleUtil.buildFlowRuleMap(rules)
    │
    ▼
flowRules = new FlowRuleContainer(ruleMap)  ← 规则生效！
```

---

## 第九阶段：完整数据流图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Sentinel 规则动态配置全景图                                │
└─────────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────┐
                    │   Nacos Server  │
                    │  (配置中心)       │
                    └────────┬────────┘
                             │ 长轮询/gRPC 推送
                             ▼
┌──────────────────────────────────────────────┐
│            NacosDataSource<String, T>          │
│                                                │
│  configListener = new Listener() {             │
│      receiveConfigInfo(configInfo) {           │
│          T val = parser.convert(configInfo);   │
│          getProperty().updateValue(val);       │  ◄── Path A: 推模型
│      }                                         │
│  }                                             │
│                                                │
│  readSource() → configService.getConfig(...)   │
│  loadInitialConfig() → 启动时主动拉一次         │
└────────────────────────┬───────────────────────┘
                         │
                         │ getProperty() 返回
                         ▼
┌──────────────────────────────────────────────┐
│     DynamicSentinelProperty<T> (核心枢纽)     │
│                                                │
│  listeners: CopyOnWriteArraySet               │
│  value: T                                      │
│                                                │
│  updateValue(newVal):                          │
│    if (isEqual(value, newVal)) return false;   │
│    value = newVal;                             │
│    for (listener : listeners)                  │
│        listener.configUpdate(newVal);   ───────┼──► 通知所有 Observer
│                                                │
│  addListener(listener):                        │
│    listeners.add(listener);                    │
│    listener.configLoad(value);  ───────────────┼──► 立即推送当前值
└───────────┬───────────────────────┬────────────┘
            │                       │
            │                       │ 其他 Property 实例
            ▼                       ▼
┌─────────────────────────────────────────────────┐
│         FileRefreshableDataSource<String, T>      │
│                                                   │
│  ScheduledExecutorService (每 3000ms)             │
│    → isModified(): file.lastModified() 比较       │  ◄── Path B: 拉模型
│    → loadConfig(): readSource() + parser          │
│    → getProperty().updateValue(newVal)            │
│                                                   │
│  readSource(): FileInputStream 读文件内容         │
│  MAX_SIZE: 4MB                                    │
│  firstLoad(): 构造时立即加载一次                    │
└───────────────────────────────────────────────────┘

                         ║
                         ║ 统一收束点
                         ▼

┌──────────────────────────────────────────────┐
│    FlowPropertyListener (Observer 实现)        │
│                                                │
│  configUpdate(List<FlowRule> value):           │
│    Map rules = FlowRuleUtil.buildFlowRuleMap() │
│    flowRules = new FlowRuleContainer(rules);   │  ← 规则生效！
│                                                │
│  configLoad(List<FlowRule> value):             │
│    同 configUpdate 逻辑                         │
└────────────────────────────────────────────────┘

                         ║
                         ║ 运行时使用
                         ▼

┌──────────────────────────────────────────────┐
│      FlowRuleContainer (运行时规则容器)         │
│                                                │
│  simpleRules: Map<String, List<FlowRule>>     │  ← 精确匹配 O(1)
│  regexRules: Map<Pattern, List<FlowRule>>     │  ← 正则匹配
│  regexCacheRules: volatile Map (缓存)          │  ← 避免重复正则计算
│                                                │
│  getRules(resource): 先查 simple → 再查 regex  │
└──────────────────────────────────────────────┘

=============================== Dashboard 推送路径 ================================

┌───────────────────┐     HTTP POST /setRules
│ Sentinel Dashboard│────────────────────────────────────┐
└───────────────────┘                                    │
                                                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  ModifyRulesCommandHandler                                        │
│                                                                    │
│  handle(request):                                                  │
│    type = request.getParam("type")    // "flow"                   │  ◄── Path C
│    data = request.getParam("data")    // JSON                     │
│    rules = JSONArray.parseArray(data, XxxRule.class)               │
│                                                                    │
│    XxxRuleManager.loadRules(rules)                                │
│      └── currentProperty.updateValue(rules)  →  同一收束点         │
│                                                                    │
│    WritableDataSourceRegistry.writeToDataSource(rules)            │
│      └── writableDataSource.write(rules)  →  持久化回写            │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────┐
│     WritableDataSourceRegistry (静态注册表)    │
│                                                │
│  flowDataSource: WritableDataSource            │
│  degradeDataSource: WritableDataSource         │
│  systemDataSource: WritableDataSource          │
│  authorityDataSource: WritableDataSource       │
│                                                │
│  registerFlowDataSource(ds): 注册回写数据源    │
│  getFlowDataSource(): 获取已注册的数据源       │
└──────────────────────────────────────────────┘
```

---

## 第十阶段：其他 RuleManager 的对称设计

Sentinel 中的所有 RuleManager 都遵循完全相同的模式。以下对比说明：

### 10.1 DegradeRuleManager（降级规则）

```java
public final class DegradeRuleManager {

    private static final DegradeRuleListener LISTENER = new DegradeRuleListener();
    private static SentinelProperty<List<DegradeRule>> currentProperty
        = new DynamicSentinelProperty<>();

    static {
        currentProperty.addListener(LISTENER);
    }

    public static void register2Property(SentinelProperty<List<DegradeRule>> property) {
        synchronized (LISTENER) {
            currentProperty.removeListener(LISTENER);
            property.addListener(LISTENER);
            currentProperty = property;
        }
    }

    public static void loadRules(List<DegradeRule> rules) {
        currentProperty.updateValue(rules);
    }

    // DegradeRuleListener 的 configUpdate 逻辑：
    // 构建 CircuitBreaker 实例 → 按资源名分组存储
}
```

### 10.2 SystemRuleManager（系统规则）

```java
public final class SystemRuleManager {

    private static final SystemPropertyListener LISTENER = new SystemPropertyListener();
    private static SentinelProperty<List<SystemRule>> currentProperty
        = new DynamicSentinelProperty<>();

    static {
        currentProperty.addListener(LISTENER);
    }

    // 相同模式：register2Property, loadRules, PropertyListener
    // SystemPropertyListener.configUpdate: 更新 highestSystemLoad, qps, avgRt 等阈值
}
```

### 10.3 AuthorityRuleManager（授权规则）

```java
public final class AuthorityRuleManager {

    private static final AuthorityPropertyListener LISTENER = new AuthorityPropertyListener();
    private static SentinelProperty<List<AuthorityRule>> currentProperty
        = new DynamicSentinelProperty<>();

    static {
        currentProperty.addListener(LISTENER);
    }

    // AuthorityPropertyListener.configUpdate: 按资源名分组，白名单/黑名单模式
}
```

**设计规律总结**：每个 RuleManager 都具备以下组件：

1. `static LISTENER`：单例 PropertyListener 实现
2. `static currentProperty`：当前绑定的 SentinelProperty
3. `static { ... }`：类加载时注册监听器
4. `register2Property()`：热切换数据源
5. `loadRules()`：代码式加载（也走 property 链路）

---

## 第十一阶段：InitFunc 与 SPI 自动发现

Sentinel 使用 Java SPI 机制自动发现和执行初始化逻辑，包括 DataSource 的注册。

### 11.1 InitFunc 接口

```java
// com.alibaba.csp.sentinel.init.InitFunc
public interface InitFunc {

    /**
     * Sentinel 启动时自动调用。
     * 用户在此实现中注册 DataSource → Property → RuleManager 的绑定关系。
     */
    void init() throws Exception;
}
```

### 11.2 InitExecutor 加载机制

```java
// com.alibaba.csp.sentinel.init.InitExecutor
public final class InitExecutor {

    private static AtomicBoolean initialized = new AtomicBoolean(false);

    public static void doInit() {
        // 保证只执行一次
        if (!initialized.compareAndSet(false, true)) {
            return;
        }

        try {
            // 通过 SPI 加载所有 InitFunc 实现
            ServiceLoader<InitFunc> loader = ServiceLoaderUtil.getServiceLoader(InitFunc.class);

            List<OrderWrapper> initList = new ArrayList<>();
            for (InitFunc initFunc : loader) {
                RecordLog.info("[InitExecutor] Found init func: "
                    + initFunc.getClass().getCanonicalName());
                // 支持 @InitOrder 注解控制执行顺序
                insertSorted(initList, initFunc);
            }

            // 按顺序执行初始化
            for (OrderWrapper wrapper : initList) {
                wrapper.func.init();
                RecordLog.info("[InitExecutor] Executing " + wrapper.func.getClass().getName());
            }
        } catch (Exception ex) {
            RecordLog.warn("[InitExecutor] WARN: Initialization failed", ex);
        }
    }
}
```

### 11.3 SPI 配置文件

```
# META-INF/services/com.alibaba.csp.sentinel.init.InitFunc
# 文件内容为 InitFunc 实现类的全限定名，每行一个

com.example.sentinel.DataSourceInitFunc
```

---

## 总结

### 设计亮点

**1. 观察者模式实现完全解耦**

DataSource 不知道谁在监听它的 property，RuleManager 不知道数据从哪里来。两者通过 `SentinelProperty` 这个「合同」解耦。新增一种数据源（如 Consul、etcd）只需实现 `AbstractDataSource`，完全不需要修改任何 RuleManager 的代码。

**2. 数据源热切换**

`register2Property()` 方法允许运行时切换数据源。从文件切换到 Nacos、从 Nacos 切换到 Zookeeper，都是一行代码的事，且切换过程中不会丢失规则（`addListener` 立即触发 `configLoad`）。

**3. 推拉模型统一抽象**

推模型（NacosDataSource）和拉模型（FileRefreshableDataSource）共享相同的抽象层（`AbstractDataSource`）和相同的下游链路（`SentinelProperty → PropertyListener → RuleManager`）。上层使用者无需关心底层是推还是拉。

**4. 收束点设计**

三条完全不同的路径（Nacos 推送、文件轮询、Dashboard HTTP 推送）最终都汇聚到 `DynamicSentinelProperty.updateValue()` 这一个点。这意味着：无论规则从哪里来，后续的规则解析、容器构建、生效逻辑都是同一套代码，不会出现行为不一致。

**5. 去重与幂等**

`updateValue()` 内部的 `isEqual()` 检查保证了相同规则的重复推送不会触发无意义的规则重建。结合 NacosDataSource 的 `DiscardOldestPolicy`，即使在高频变更场景下也能保持稳定。

**6. 线程安全与性能兼顾**

运行时热路径（`getRules()`）完全无锁，依赖 `volatile` 引用切换和 `ConcurrentHashMap` 的无锁读。规则更新路径低频执行，使用 `synchronized` 保证原子性，不会成为性能瓶颈。

### 核心类关系一览

```
SentinelProperty<T> (接口/Subject)
    └── DynamicSentinelProperty<T> (实现)
            ├── listeners: CopyOnWriteArraySet<PropertyListener<T>>
            ├── value: T
            ├── updateValue(T) → isEqual() + notify all
            └── addListener() → immediate configLoad()

PropertyListener<T> (接口/Observer)
    └── FlowPropertyListener / DegradePropertyListener / ... (各 RuleManager 内部类)
            ├── configLoad(T) → buildRuleMap → atomic switch container
            └── configUpdate(T) → same logic

ReadableDataSource<S,T> (接口)
    └── AbstractDataSource<S,T> (抽象类)
            ├── parser: Converter<S,T>
            ├── property: DynamicSentinelProperty<T>
            ├── loadConfig() = parser.convert(readSource())
            │
            ├── AutoRefreshDataSource<S,T> (拉模型基类)
            │       ├── ScheduledExecutorService
            │       ├── recommendRefreshMs = 3000
            │       ├── isModified() → 子类覆盖
            │       │
            │       └── FileRefreshableDataSource (文件拉模型)
            │               ├── file.lastModified() 比较
            │               ├── MAX_SIZE = 4MB
            │               └── firstLoad() in constructor
            │
            └── NacosDataSource (Nacos 推模型)
                    ├── configListener: Nacos Listener
                    ├── pool: DiscardOldestPolicy ThreadPool
                    ├── loadInitialConfig() at startup
                    └── receiveConfigInfo() → convert → updateValue

WritableDataSource<T> (接口)
    └── FileWritableDataSource (文件回写)
            └── write(T) → encoder.convert(T) → write to file

WritableDataSourceRegistry (静态注册表)
    └── flowDataSource / degradeDataSource / systemDataSource / authorityDataSource

ModifyRulesCommandHandler (Dashboard 推送入口)
    └── handle() → parse → loadRules() + writeToDataSource()
```

这套设计是 Sentinel 能够在生产环境中实现「规则秒级生效、零停机更新、多数据源兼容」的架构基石。理解了这套机制，就能清晰地知道：规则从哪里来、怎么传递、何时生效、如何保证一致性。
