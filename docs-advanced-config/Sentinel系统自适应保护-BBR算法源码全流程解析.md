# Sentinel 系统自适应保护 —— BBR 算法源码全流程解析

> 基于源码项目 `/Users/zhanghonghao/Desktop/Sentinel` 逐步分析，从 SystemSlot 入口到 BBR 自适应判定，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
责任链中 SystemSlot 被触发：
  |
  +-- 1. SystemSlot.entry()
  |     -> @Spi(order = -5000)
  |     -> 仅当 resourceWrapper.getEntryType() == EntryType.IN 时才检查
  |     -> 委托给 SystemRuleManager.checkSystem(resourceWrapper, count)
  |
  +-- 2. SystemRuleManager.checkSystem()
  |     -> 前置条件：checkSystemStatus.get() == true
  |     -> 前置条件：entryType == EntryType.IN
  |     -> 获取全局入站统计节点 Constants.ENTRY_NODE (ClusterNode)
  |     -> 五重检查顺序执行：
  |        [1] QPS 检查:     currentQps + count > qps         → SystemBlockException("qps")
  |        [2] Thread 检查:  currentThread > maxThread         → SystemBlockException("thread")
  |        [3] RT 检查:      rt > maxRt                        → SystemBlockException("rt")
  |        [4] Load 检查:    load > highestSystemLoad          → 进入 BBR 判定
  |             -> checkBbr(currentThread)                     → SystemBlockException("load")
  |        [5] CPU 检查:     cpuUsage > highestCpuUsage        → SystemBlockException("cpu")
  |
  +-- 3. checkBbr(int currentThread) —— BBR 自适应判定
  |     -> if currentThread > 1
  |        && currentThread > ENTRY_NODE.maxSuccessQps() * ENTRY_NODE.minRt() / 1000
  |        → return false (拒绝)
  |     -> 否则 return true (放行)
  |     -> 物理含义：inflight_requests > bandwidth × delay (Little's Law)
  |
  +-- 4. 系统指标采集 (SystemStatusListener)
  |     -> 定时任务，每 1 秒执行一次
  |     -> 采集 systemLoadAverage、systemCpuLoad、processCpuUsage
  |     -> 更新 SystemRuleManager 中的静态字段
  |
  +-- 5. 规则管理 (SystemRuleManager)
        -> loadRules() → 提取所有规则中最严格的阈值
        -> 当存在有效规则时 checkSystemStatus.set(true)
```

---

## 第一阶段：SystemSlot 入口

### 1.1 SystemSlot 类定义

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/system/SystemSlot.java`

`SystemSlot` 是责任链中负责系统自适应保护的 Slot，通过 SPI 机制加载，优先级 `order = -5000`，排在 AuthoritySlot 之后、FlowSlot 之前：

```java
@Spi(order = Constants.ORDER_SYSTEM_SLOT)
public class SystemSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node,
                      int count, boolean prioritized, Object... args) throws Throwable {
        SystemRuleManager.checkSystem(resourceWrapper, count);
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        fireExit(context, resourceWrapper, count, args);
    }
}
```

关键点解析：

- **`@Spi(order = -5000)`**：`Constants.ORDER_SYSTEM_SLOT = -5000`，确保系统保护在流量控制和熔断之前执行。这体现了 Sentinel 的设计哲学——系统级保护具有最高优先级，当系统负载过高时，不应该让流量进入后续的 FlowSlot 和 DegradeSlot。
- **仅 entry 有逻辑**：exit 方法只是简单地调用 `fireExit` 传递到下一个 Slot，因为系统保护不需要在请求完成时做任何清理。
- **无条件委托**：所有判断逻辑都在 `SystemRuleManager.checkSystem()` 中，SystemSlot 本身只是一个薄薄的壳，职责单一。

### 1.2 为什么只检查 EntryType.IN

系统自适应保护的目标是防止入站流量压垮当前节点。对于出站流量（本节点作为消费者调用外部服务），系统负载不会因为出站请求增加而升高（出站请求的 RT 主要消耗在网络等待上），因此 SystemSlot 只对 `EntryType.IN` 的流量进行保护。

这个判断实际上在 `SystemRuleManager.checkSystem()` 方法内部完成，而非在 SystemSlot 层面：

```java
// SystemRuleManager.checkSystem() 内部
if (resourceWrapper.getEntryType() != EntryType.IN) {
    return;
}
```

---

## 第二阶段：SystemRule 模型

### 2.1 SystemRule 类定义

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/system/SystemRule.java`

```java
public class SystemRule extends AbstractRule {

    // 系统负载阈值（Linux 1分钟平均负载）
    private double highestSystemLoad = -1;

    // CPU 使用率阈值 [0, 1]
    private double highestCpuUsage = -1;

    // 入站 QPS 阈值
    private double qps = -1;

    // 平均响应时间阈值（毫秒）
    private long avgRt = -1;

    // 最大并发线程数阈值
    private long maxThread = -1;

    // getter/setter 省略...
}
```

### 2.2 五个维度的含义

| 字段 | 含义 | 默认值 | 说明 |
|------|------|--------|------|
| `highestSystemLoad` | 系统负载上限 | -1（禁用） | Linux `uptime` 中的 1min load average |
| `highestCpuUsage` | CPU 使用率上限 | -1（禁用） | 范围 [0, 1]，1 表示 100% |
| `qps` | 入站 QPS 上限 | -1（禁用） | 当前入站 QPS 超过此值则拒绝 |
| `avgRt` | 平均 RT 上限 | -1（禁用） | 单位毫秒 |
| `maxThread` | 并发线程数上限 | -1（禁用） | 入站请求占用的线程数 |

- **-1 表示禁用**：当某个维度的值为 -1 时，该维度的检查会被跳过。
- **可以同时配置多个维度**：五个维度的检查是串行执行的，任意一个不满足即拒绝。
- **多条规则时取最严格值**：当配置了多条 SystemRule 时，SystemRuleManager 会提取每个维度的最严格（最小）阈值。

### 2.3 AbstractRule 基类

```java
public abstract class AbstractRule implements Rule {
    private String resource;
    private String limitApp;
    // ...
}
```

SystemRule 虽然继承了 `AbstractRule`，但在系统保护场景中 `resource` 和 `limitApp` 字段并不使用——系统保护是全局性的，不针对特定资源。

---

## 第三阶段：SystemRuleManager.checkSystem() 核心逻辑

### 3.1 完整源码

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/system/SystemRuleManager.java`

```java
public class SystemRuleManager {

    // 系统保护是否激活的标志
    private static volatile boolean checkSystemStatus = false;

    // 五个维度的全局阈值（从所有规则中提取最严格值）
    private static volatile double highestSystemLoad = Double.MAX_VALUE;
    private static volatile double highestCpuUsage = Double.MAX_VALUE;
    private static volatile double qps = Double.MAX_VALUE;
    private static volatile long maxRt = Long.MAX_VALUE;
    private static volatile long maxThread = Long.MAX_VALUE;

    // 系统指标 —— 由 SystemStatusListener 定时更新
    private static volatile double currentLoad = -1;
    private static volatile double currentCpuUsage = -1;

    /**
     * 核心检查方法，由 SystemSlot.entry() 调用
     */
    public static void checkSystem(ResourceWrapper resourceWrapper, int count)
            throws BlockException {

        // 条件1：没有任何系统规则配置时，直接跳过
        if (!checkSystemStatus) {
            return;
        }

        // 条件2：只检查入站流量
        if (resourceWrapper.getEntryType() != EntryType.IN) {
            return;
        }

        // 获取全局入站流量统计节点
        // Constants.ENTRY_NODE 是一个全局唯一的 ClusterNode
        // 所有 EntryType.IN 的资源共享这个统计节点
        ClusterNode entryNode = Constants.ENTRY_NODE;

        // ========== 检查 1：QPS ==========
        double currentQps = entryNode.passQps();
        if (currentQps + count > qps) {
            throw new SystemBlockException(resourceWrapper.getName(), "qps");
        }

        // ========== 检查 2：并发线程数 ==========
        int currentThread = entryNode.curThreadNum();
        if (currentThread > maxThread) {
            throw new SystemBlockException(resourceWrapper.getName(), "thread");
        }

        // ========== 检查 3：平均 RT ==========
        double rt = entryNode.avgRt();
        if (rt > maxRt) {
            throw new SystemBlockException(resourceWrapper.getName(), "rt");
        }

        // ========== 检查 4：系统负载 + BBR 自适应 ==========
        if (highestSystemLoad != Double.MAX_VALUE) {
            if (currentLoad > highestSystemLoad) {
                if (!checkBbr(currentThread)) {
                    throw new SystemBlockException(resourceWrapper.getName(), "load");
                }
            }
        }

        // ========== 检查 5：CPU 使用率 ==========
        if (highestCpuUsage != Double.MAX_VALUE) {
            if (currentCpuUsage > highestCpuUsage) {
                throw new SystemBlockException(resourceWrapper.getName(), "cpu");
            }
        }
    }
}
```

### 3.2 逻辑拆解

**前置检查**：

1. `checkSystemStatus` 是一个 `volatile boolean`，只有当存在有效的 SystemRule 时才为 true。这是一个快速路径优化——大多数应用不会配置系统规则，这个判断让 SystemSlot 对性能的影响降到最低。
2. `EntryType.IN` 过滤确保只有入站流量受到系统保护。

**五个检查的执行顺序及设计考量**：

```
QPS → Thread → RT → Load+BBR → CPU
```

为什么是这个顺序？

- **QPS 最先**：QPS 检查最轻量，只需要读取滑动窗口的统计值。而且 QPS 超限往往意味着瞬时流量洪峰，应最先拦截。
- **Thread 其次**：线程数检查同样轻量，但反映的是系统的并发处理能力。
- **RT 第三**：平均 RT 升高通常是系统开始变慢的信号。
- **Load + BBR 第四**：系统负载的计算涉及 BBR 算法的额外判断，稍微重一些，且 Load 是操作系统级别的指标，粒度较粗（1秒更新一次）。
- **CPU 最后**：CPU 使用率同样是 1 秒更新一次，放在最后。

### 3.3 Constants.ENTRY_NODE —— 全局入站统计节点

```java
public final class Constants {
    // 全局入站流量统计
    public static volatile ClusterNode ENTRY_NODE = new ClusterNode(
        Constants.TOTAL_IN_RESOURCE_NAME, ResourceTypeConstants.COMMON);
    
    public static final String TOTAL_IN_RESOURCE_NAME = "__total_inbound_traffic__";
}
```

`ENTRY_NODE` 是一个特殊的 `ClusterNode`，它汇总了所有 `EntryType.IN` 的资源的流量统计数据。这意味着系统保护是基于全局入站流量的聚合视图来判断的，而不是针对单个资源。

`ClusterNode` 内部使用滑动时间窗口（`ArrayMetric`）来统计 passQps、curThreadNum、avgRt 等指标。

### 3.4 SystemBlockException

```java
public class SystemBlockException extends BlockException {

    private final String resourceName;
    private final String limitType;  // "qps", "thread", "rt", "load", "cpu"

    public SystemBlockException(String resourceName, String limitType) {
        super(resourceName + " blocked by system rule, type: " + limitType);
        this.resourceName = resourceName;
        this.limitType = limitType;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;  // 性能优化：不填充堆栈
    }
}
```

注意 `fillInStackTrace()` 返回 `this` 而不调用 `super.fillInStackTrace()`——这是 Sentinel 的一个重要性能优化。在高并发场景下，被拒绝的请求量可能非常大，如果每次都填充堆栈信息，会带来显著的性能开销。

---

## 第四阶段：BBR 自适应算法

### 4.1 checkBbr() 源码

```java
private static boolean checkBbr(int currentThread) {
    if (currentThread > 1 &&
        currentThread > Constants.ENTRY_NODE.maxSuccessQps()
                        * Constants.ENTRY_NODE.minRt() / 1000) {
        return false;  // 拒绝
    }
    return true;  // 放行
}
```

### 4.2 算法原理 —— Little's Law（利特尔法则）

BBR（Bottleneck Bandwidth and Round-trip propagation time）的核心思想来源于网络拥塞控制领域。Sentinel 将其应用到系统自适应保护中：

**Little's Law**：在一个稳定系统中，平均并发请求数 = 平均到达率 × 平均服务时间

转换为 Sentinel 的语境：

```
inflight_requests ≤ bandwidth × delay

即：当前并发线程数 ≤ 最大处理速率 × 最小响应时间
```

**公式分解**：

| 变量 | Sentinel 中的含义 | 获取方式 |
|------|------------------|----------|
| `currentThread` | 当前正在处理的入站请求数（inflight requests） | `ENTRY_NODE.curThreadNum()` |
| `maxSuccessQps` | 系统在滑动窗口内观测到的最大成功 QPS（bandwidth） | `ENTRY_NODE.maxSuccessQps()` |
| `minRt` | 系统在滑动窗口内观测到的最小 RT（delay，毫秒） | `ENTRY_NODE.minRt()` |
| `1000` | 毫秒到秒的转换因子 | 常量 |

**计算过程**：

```
管道容量 = maxSuccessQps × (minRt / 1000)
         = 最大处理速率(请求/秒) × 最小处理时间(秒)
         = 系统在理想状态下能同时处理的最大请求数

当 currentThread > 管道容量 时，说明系统中积压的请求超过了其最优处理能力，应该拒绝新请求。
```

### 4.3 为什么需要 currentThread > 1 的前置条件

```java
if (currentThread > 1 && ...)
```

当 `currentThread == 1` 时，系统只有一个请求在处理，不可能处于过载状态。加入这个条件可以避免：

1. 系统刚启动时，`maxSuccessQps()` 和 `minRt()` 可能还没有有效值（返回 0 或极小值），此时 `maxSuccessQps * minRt / 1000` 可能为 0，导致第一个请求就被拒绝。
2. 极低并发时的误判——只有一个请求在执行时，系统显然不会过载。

### 4.4 BBR 与 Load 检查的关系

BBR 算法并**不是独立执行**的。回顾 checkSystem() 中的逻辑：

```java
// 检查 4：系统负载 + BBR 自适应
if (highestSystemLoad != Double.MAX_VALUE) {    // 配置了 Load 规则
    if (currentLoad > highestSystemLoad) {       // 系统负载超过阈值
        if (!checkBbr(currentThread)) {          // BBR 判定失败才拒绝
            throw new SystemBlockException(resourceWrapper.getName(), "load");
        }
    }
}
```

**两层保护的含义**：

1. **第一层——Load 阈值**：系统负载（1min load average）超过配置的阈值。这是一个粗粒度的信号，表明系统"可能"过载了。
2. **第二层——BBR 判定**：即使 Load 超标，如果 BBR 判断管道还没有满（当前并发数未超过系统最优处理能力），仍然放行。

这种设计避免了单纯依赖 Load 值的问题：

- Load 是一个延迟指标，反映的是过去一段时间的平均值
- 某些场景下 Load 短暂升高并不意味着系统真的过载了
- BBR 提供了一个更精确的实时判断——"当前系统的处理管道是否已满"

### 4.5 maxSuccessQps() 和 minRt() 的实现

这两个方法来自 `ClusterNode`（继承自 `StatisticNode`）：

```java
// StatisticNode.java
@Override
public double maxSuccessQps() {
    // 返回滑动窗口中每秒成功通过的最大值
    return rollingCounterInSecond.maxSuccess();
}

@Override
public double minRt() {
    // 返回滑动窗口中观测到的最小 RT
    return rollingCounterInSecond.minRt();
}
```

`rollingCounterInSecond` 是一个基于 `LeapArray`（滑动窗口）的统计器，默认配置为：

- 时间窗口大小：1000ms（1秒）
- 桶数量：2（每个桶 500ms）

```java
// StatisticNode 构造
private transient volatile Metric rollingCounterInSecond = 
    new ArrayMetric(SampleCountProperty.SAMPLE_COUNT,    // 默认 2
                    IntervalProperty.INTERVAL);            // 默认 1000ms
```

`maxSuccess()` 返回所有时间窗格（bucket）中 `MetricBucket.success()` 的最大值：

```java
// ArrayMetric.java
@Override
public long maxSuccess() {
    data.currentWindow();  // 确保当前窗口已初始化
    long success = 0;
    List<MetricBucket> list = data.values();
    for (MetricBucket window : list) {
        if (window.success() > success) {
            success = window.success();
        }
    }
    // 转换为 QPS（除以每个桶的时间长度）
    return success * 1000 / data.getWindowIntervalInMs() * data.getSampleCount();
}
```

`minRt()` 返回所有时间窗格中观测到的最小 RT 值：

```java
@Override
public double minRt() {
    data.currentWindow();
    double minRt = Double.MAX_VALUE;
    List<MetricBucket> list = data.values();
    for (MetricBucket bucket : list) {
        double rt = bucket.minRt();
        if (rt < minRt) {
            minRt = rt;
        }
    }
    return minRt;
}
```

### 4.6 BBR 在实际场景中的效果

考虑一个具体场景：

```
配置：highestSystemLoad = 5.0
运行时观测值：
  - currentLoad = 6.0 （超过阈值）
  - maxSuccessQps = 2000 （系统最高处理速率）
  - minRt = 10ms （最小响应时间）
  - currentThread = 15 （当前并发数）

BBR 计算：
  管道容量 = 2000 × 10 / 1000 = 20
  判断：currentThread(15) > 管道容量(20) ? → 否
  结果：放行！尽管 Load 超标，但管道未满，系统仍有处理能力
```

再看另一种情况：

```
配置：highestSystemLoad = 5.0
运行时观测值：
  - currentLoad = 6.0 （超过阈值）
  - maxSuccessQps = 2000
  - minRt = 10ms
  - currentThread = 25 （当前并发数更高）

BBR 计算：
  管道容量 = 2000 × 10 / 1000 = 20
  判断：currentThread(25) > 管道容量(20) ? → 是
  结果：拒绝！Load 超标且管道已满，系统确实过载
```

---

## 第五阶段：系统指标采集 —— SystemStatusListener

### 5.1 定时任务调度

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/system/SystemRuleManager.java`

系统指标的采集通过一个定时任务完成，在 `SystemRuleManager` 类加载时启动：

```java
public class SystemRuleManager {

    // 静态初始化块中启动定时采集任务
    static {
        // 延迟 0 秒启动，每 1 秒执行一次
        statusListener = new SystemStatusListener();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("sentinel-system-status-listener", true));
        scheduler.scheduleAtFixedRate(statusListener, 0, 1, TimeUnit.SECONDS);
    }
}
```

### 5.2 SystemStatusListener 完整实现

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/system/SystemStatusListener.java`

```java
public class SystemStatusListener implements Runnable {

    // 上一次采集时的 CPU 时间（用于计算 CPU 使用率）
    volatile long prevCpuTime = 0;
    volatile long prevUpTime = 0;
    volatile int cpuCount = 0;

    @Override
    public void run() {
        try {
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

            // ========== 采集系统负载 ==========
            // 在 Linux 上返回 1 分钟平均负载，在 Windows 上返回 -1
            double systemAvgLoad = osBean.getSystemLoadAverage();
            SystemRuleManager.currentLoad = systemAvgLoad;

            // ========== 采集 CPU 使用率 ==========
            double systemCpuUsage = getSystemCpuUsage(osBean);
            double processCpuUsage = getProcessCpuUsage(osBean, runtimeBean);

            // 取两者最大值，兼容容器场景
            SystemRuleManager.currentCpuUsage = Math.max(systemCpuUsage, processCpuUsage);

        } catch (Throwable e) {
            RecordLog.warn("[SystemStatusListener] Failed to get system metrics", e);
        }
    }

    /**
     * 获取系统级 CPU 使用率
     * 通过反射调用 com.sun.management.OperatingSystemMXBean 的 getSystemCpuLoad()
     */
    private double getSystemCpuUsage(OperatingSystemMXBean osBean) {
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) osBean).getSystemCpuLoad();
        }
        return -1;
    }

    /**
     * 计算进程 CPU 使用率
     * 通过两次采样之间的 CPU 时间差来精确计算
     */
    private double getProcessCpuUsage(OperatingSystemMXBean osBean,
                                       RuntimeMXBean runtimeBean) {
        // 获取当前进程的 CPU 时间（纳秒）
        long cpuTime;
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            cpuTime = ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuTime();
        } else {
            return -1;
        }

        // 获取 JVM 运行时间（毫秒）
        long upTime = runtimeBean.getUptime();

        // 获取可用处理器数量
        int cpuCnt = osBean.getAvailableProcessors();

        // 首次采集，记录基准值
        if (prevUpTime == 0) {
            prevUpTime = upTime;
            prevCpuTime = cpuTime;
            cpuCount = cpuCnt;
            return -1;  // 首次无法计算
        }

        // 计算时间差
        long elapsedCpu = cpuTime - prevCpuTime;     // 纳秒
        long elapsedTime = upTime - prevUpTime;       // 毫秒

        // 更新基准值
        prevCpuTime = cpuTime;
        prevUpTime = upTime;
        cpuCount = cpuCnt;

        // 计算 CPU 使用率
        // elapsedCpu 是纳秒，elapsedTime 是毫秒
        // CPU 使用率 = CPU时间 / (挂钟时间 × CPU核数)
        if (elapsedTime > 0) {
            return Math.min(1.0,
                (double) elapsedCpu / (elapsedTime * 1_000_000.0 * cpuCnt));
        }
        return -1;
    }
}
```

### 5.3 为什么取 max(systemCpuUsage, processCpuUsage)

```java
SystemRuleManager.currentCpuUsage = Math.max(systemCpuUsage, processCpuUsage);
```

这是为了**兼容容器化部署场景**：

- **物理机/虚拟机场景**：`systemCpuLoad` 和 `processCpuUsage` 通常比较接近（当只有一个主要应用时）。
- **容器场景（Docker/K8s）**：JVM 可能受到 cgroup 的 CPU 限制。此时 `systemCpuLoad` 可能显示较低的值（因为它看到的是宿主机的CPU），而 `processCpuUsage` 会更准确地反映容器内的实际 CPU 使用情况。反之，当容器内运行多个进程时，`systemCpuLoad` 可能更高。

取两者最大值确保了在各种部署环境下都能正确检测到 CPU 过载。

### 5.4 systemLoadAverage 的平台差异

```java
double systemAvgLoad = osBean.getSystemLoadAverage();
```

- **Linux/macOS**：返回系统 1 分钟平均负载（等同于 `uptime` 命令的第一个值）。表示在过去 1 分钟内，平均有多少进程在等待 CPU 时间片。
- **Windows**：始终返回 -1，因为 Windows 没有 Unix 风格的 load average 概念。

因此在 Windows 环境下，基于 `highestSystemLoad` 的规则无法生效（因为 `currentLoad = -1`，永远小于任何正数阈值）。

### 5.5 采集频率的考量

每 **1 秒** 采集一次：

- 足够频繁以捕获系统状态变化
- 不会对系统产生显著的性能开销
- `systemLoadAverage` 本身就是 1 分钟的滑动平均值，更频繁的采集没有意义
- CPU 使用率的计算基于两次采样的差值，1 秒的间隔提供了合理的精度

---

## 第六阶段：规则管理

### 6.1 SystemRuleManager 规则加载机制

```java
public class SystemRuleManager {

    // 规则集合
    private static volatile Set<SystemRule> rules = Collections.emptySet();

    // 动态数据源属性
    private static SentinelProperty<List<SystemRule>> currentProperty =
        new DynamicSentinelProperty<>();

    // 属性监听器
    private static final SystemPropertyListener listener = new SystemPropertyListener();

    static {
        currentProperty.addListener(listener);
    }

    /**
     * 加载系统规则的公开 API
     */
    public static void loadRules(List<SystemRule> rules) {
        currentProperty.updateValue(rules);
    }

    /**
     * 注册动态数据源
     */
    public static void register2Property(SentinelProperty<List<SystemRule>> property) {
        synchronized (listener) {
            currentProperty.removeListener(listener);
            property.addListener(listener);
            currentProperty = property;
        }
    }
}
```

### 6.2 SystemPropertyListener —— 规则变更监听

```java
static class SystemPropertyListener implements PropertyListener<List<SystemRule>> {

    @Override
    public synchronized void configUpdate(List<SystemRule> rules) {
        restoreSetting();  // 重置所有阈值为默认值

        if (rules == null || rules.isEmpty()) {
            checkSystemStatus = false;
            return;
        }

        // 遍历所有规则，提取每个维度的最严格阈值
        boolean hasValidRule = false;
        for (SystemRule rule : rules) {
            // Load 规则
            if (rule.getHighestSystemLoad() >= 0) {
                highestSystemLoad = Math.min(highestSystemLoad, rule.getHighestSystemLoad());
                hasValidRule = true;
            }
            // CPU 规则
            if (rule.getHighestCpuUsage() >= 0) {
                highestCpuUsage = Math.min(highestCpuUsage, rule.getHighestCpuUsage());
                hasValidRule = true;
            }
            // QPS 规则
            if (rule.getQps() >= 0) {
                qps = Math.min(qps, rule.getQps());
                hasValidRule = true;
            }
            // RT 规则
            if (rule.getAvgRt() >= 0) {
                maxRt = Math.min(maxRt, rule.getAvgRt());
                hasValidRule = true;
            }
            // Thread 规则
            if (rule.getMaxThread() >= 0) {
                maxThread = Math.min(maxThread, rule.getMaxThread());
                hasValidRule = true;
            }
        }

        // 只有存在至少一条有效规则时，才激活系统保护检查
        checkSystemStatus = hasValidRule;

        // 保存规则集合
        SystemRuleManager.rules = new HashSet<>(rules);
    }

    @Override
    public synchronized void configLoad(List<SystemRule> rules) {
        configUpdate(rules);
    }

    private void restoreSetting() {
        highestSystemLoad = Double.MAX_VALUE;
        highestCpuUsage = Double.MAX_VALUE;
        qps = Double.MAX_VALUE;
        maxRt = Long.MAX_VALUE;
        maxThread = Long.MAX_VALUE;
        checkSystemStatus = false;
    }
}
```

### 6.3 规则管理的设计要点

**1. 多规则合并策略——取最严格值**：

当配置了多条 SystemRule 时，每个维度取所有规则中的最小值（最严格的阈值）。例如：

```java
// 规则1：highestSystemLoad = 5.0, qps = 1000
// 规则2：highestSystemLoad = 4.0, qps = 2000
// 合并后：highestSystemLoad = 4.0, qps = 1000
```

**2. checkSystemStatus 快速路径**：

```java
// checkSystem() 方法的第一行
if (!checkSystemStatus) {
    return;  // 没有规则时，零开销
}
```

这个 `volatile boolean` 确保在没有配置任何系统规则时，SystemSlot 几乎不产生任何性能开销。

**3. 动态规则更新**：

通过 `SentinelProperty` + `PropertyListener` 模式，支持规则的动态更新（例如从 Nacos、Apollo 等配置中心推送）。规则变更时通过 `configUpdate()` 回调重新计算全局阈值。

**4. synchronized 保证原子性**：

`configUpdate()` 方法是 `synchronized` 的，确保多个维度的阈值更新是原子的——不会出现某些维度更新了而另一些还是旧值的中间状态。

### 6.4 规则配置示例

```java
// 编程方式配置
List<SystemRule> rules = new ArrayList<>();

// 规则1：限制系统负载
SystemRule loadRule = new SystemRule();
loadRule.setHighestSystemLoad(5.0);
rules.add(loadRule);

// 规则2：限制 CPU 使用率
SystemRule cpuRule = new SystemRule();
cpuRule.setHighestCpuUsage(0.8);  // 80%
rules.add(cpuRule);

// 规则3：限制入站 QPS
SystemRule qpsRule = new SystemRule();
qpsRule.setQps(2000);
rules.add(qpsRule);

SystemRuleManager.loadRules(rules);
```

---

## 总结

### 整体设计哲学

Sentinel 的系统自适应保护体现了**多层次、自适应**的设计理念：

1. **多维度保护**：五个维度覆盖了系统健康的各个方面——从应用层（QPS、Thread、RT）到操作系统层（Load、CPU），形成了立体的保护网。

2. **BBR 自适应算法**：不同于简单的阈值判断，BBR 引入了 Little's Law 来动态评估系统的实际处理能力。即使系统负载超标，只要管道未满，仍然允许新请求进入——这大大减少了误杀。

3. **全局视角**：系统保护基于 `Constants.ENTRY_NODE` 这个全局入站统计节点，而不是单个资源。这是合理的——系统过载是一个全局现象，不应该从单个资源的角度来判断。

4. **快速路径优化**：`checkSystemStatus` 标志让未配置规则时的开销趋近于零；BBR 的 `currentThread > 1` 前置条件避免了冷启动时的误判。

### 核心公式一览

```
BBR 判定：
  拒绝条件 = currentThread > 1 
           && currentThread > maxSuccessQps × minRt / 1000
  
  物理含义：当前在途请求数 > 系统最大处理速率 × 最小处理延迟
           即：inflight > bandwidth × delay (Little's Law)

CPU 计算：
  processCpuUsage = ΔcpuTime(ns) / (ΔwallTime(ms) × 1,000,000 × cpuCount)
  effectiveCpuUsage = max(systemCpuUsage, processCpuUsage)
```

### 调用链完整回顾

```
SystemSlot.entry()
  → SystemRuleManager.checkSystem(resourceWrapper, count)
    → [前置] checkSystemStatus == false → return
    → [前置] entryType != IN → return
    → [检查1] currentQps + count > qps → throw SystemBlockException("qps")
    → [检查2] currentThread > maxThread → throw SystemBlockException("thread")
    → [检查3] avgRt > maxRt → throw SystemBlockException("rt")
    → [检查4] currentLoad > highestSystemLoad
        → checkBbr(currentThread): thread > maxQps*minRt/1000 → throw SystemBlockException("load")
    → [检查5] currentCpuUsage > highestCpuUsage → throw SystemBlockException("cpu")
    → 全部通过 → fireEntry() 传递到下一个 Slot
```
