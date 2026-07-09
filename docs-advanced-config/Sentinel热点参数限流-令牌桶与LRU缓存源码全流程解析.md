# Sentinel 热点参数限流 —— 令牌桶与 LRU 缓存源码全流程解析

> 基于源码项目 `/Users/zhanghonghao/Desktop/Sentinel` 逐步分析，从 ParamFlowSlot 入口到每个参数值独立的令牌桶判定，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
责任链中 ParamFlowSlot 被触发：
  |
  +-- 1. ParamFlowSlot.entry()
  |     -> @Spi(order = -3000)
  |     -> 位于 sentinel-extension/sentinel-parameter-flow-control 模块
  |     -> checkFlow(resourceWrapper, count, args)
  |
  +-- 2. checkFlow()
  |     -> 从 ParamFlowRuleManager 获取当前资源的规则列表
  |     -> 遍历每条规则：
  |        -> applyRealParamIdx(rule, args.length)  计算实际参数下标
  |        -> initHotParamMetricsFor(resourceWrapper, rule)  初始化指标存储
  |        -> ParamFlowChecker.passCheck(resourceWrapper, rule, count, args)
  |
  +-- 3. ParamFlowChecker.passCheck()
  |     -> 提取 args[paramIdx] 的值
  |     -> null → 直接放行
  |     -> Collection/Array → 逐元素检查
  |     -> 根据 rule.getGrade() 分流：
  |        [THREAD] → 直接比较线程数
  |        [QPS]    → 根据 rule.getControlBehavior() 分流：
  |           [DEFAULT]       → passDefaultLocalCheck() (令牌桶)
  |           [RATE_LIMITER]  → passThrottleLocalCheck() (漏桶/排队)
  |
  +-- 4. passDefaultLocalCheck() —— 令牌桶算法
  |     -> 每个参数值独立一个 TokenUpdateStatus (CAS 自旋)
  |     -> CacheMap<Object, AtomicReference<TokenUpdateStatus>>
  |     -> 补充令牌 → CAS 扣减 → 判断 restQps >= 0
  |
  +-- 5. passThrottleLocalCheck() —— 漏桶/匀速排队算法
  |     -> 每个参数值独立一个 lastPassTime (AtomicLong)
  |     -> 计算 expectedTime → 判断等待时间是否超过 maxQueueingTimeMs
  |
  +-- 6. ParameterMetric —— 指标存储
  |     -> ruleTokenCounter: 令牌桶存储
  |     -> ruleTimeCounters: 漏桶存储
  |     -> threadCountMap: 线程数存储
  |
  +-- 7. CacheMap (ConcurrentLinkedHashMapWrapper)
        -> 基于 W-TinyLFU 的 LRU 缓存
        -> 限制最大容量，自动淘汰冷数据
        -> 防止高基数参数导致内存溢出
```

---

## 第一阶段：ParamFlowSlot 入口

### 1.1 ParamFlowSlot 类定义

**源码位置**: `sentinel-extension/sentinel-parameter-flow-control/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/param/ParamFlowSlot.java`

```java
@Spi(order = Constants.ORDER_PARAM_FLOW_SLOT)
public class ParamFlowSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node,
                      int count, boolean prioritized, Object... args) throws Throwable {
        if (!ParamFlowRuleManager.hasRules(resourceWrapper.getName())) {
            fireEntry(context, resourceWrapper, node, count, prioritized, args);
            return;
        }

        checkFlow(resourceWrapper, count, args);
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        // 出口处理：线程模式需要递减线程计数
        if (args != null && args.length > 0) {
            List<ParamFlowRule> rules = ParamFlowRuleManager.getRulesOfResource(
                resourceWrapper.getName());
            if (rules != null) {
                for (ParamFlowRule rule : rules) {
                    if (rule.getGrade() == RuleConstant.FLOW_GRADE_THREAD) {
                        int idx = rule.getParamIdx();
                        if (idx < 0) {
                            idx += args.length;
                        }
                        if (idx >= 0 && idx < args.length) {
                            Object value = args[idx];
                            ParameterMetric metric = getParameterMetric(resourceWrapper);
                            if (metric != null) {
                                metric.decreaseThreadCount(value);
                            }
                        }
                    }
                }
            }
        }
        fireExit(context, resourceWrapper, count, args);
    }
}
```

关键点解析：

- **`@Spi(order = -3000)`**：`Constants.ORDER_PARAM_FLOW_SLOT = -3000`，位于 FlowSlot（-2000）之后。热点参数限流是比普通流控更细粒度的控制。
- **快速路径**：`ParamFlowRuleManager.hasRules()` 方法先判断当前资源是否有规则，如果没有则直接放行，避免不必要的开销。
- **exit 逻辑**：线程模式需要在请求完成时递减线程计数，这是 ParamFlowSlot 与 SystemSlot 的不同之处。

### 1.2 checkFlow() 方法

```java
void checkFlow(ResourceWrapper resourceWrapper, int count, Object... args)
        throws BlockException {
    if (args == null || args.length == 0) {
        return;
    }

    List<ParamFlowRule> rules = ParamFlowRuleManager.getRulesOfResource(
        resourceWrapper.getName());
    if (rules == null || rules.isEmpty()) {
        return;
    }

    for (ParamFlowRule rule : rules) {
        // 计算实际参数下标（支持负数索引）
        applyRealParamIdx(rule, args.length);

        // 初始化该规则的参数指标存储
        ParameterMetric metric = getOrCreateParameterMetric(resourceWrapper);
        metric.initialize(rule);

        // 核心判断
        if (!ParamFlowChecker.passCheck(resourceWrapper, rule, count, args)) {
            String triggeredParam = "";
            if (rule.getParamIdx() >= 0 && rule.getParamIdx() < args.length) {
                Object value = args[rule.getParamIdx()];
                triggeredParam = String.valueOf(value);
            }
            throw new ParamFlowException(resourceWrapper.getName(),
                triggeredParam, rule);
        }
    }
}
```

### 1.3 applyRealParamIdx —— 负数索引支持

```java
static void applyRealParamIdx(ParamFlowRule rule, int length) {
    int paramIdx = rule.getParamIdx();
    if (paramIdx < 0) {
        // 负数索引：-1 表示最后一个参数，-2 表示倒数第二个
        int finalIdx = paramIdx + length;
        if (finalIdx >= 0) {
            rule.setParamIdx(finalIdx);
        }
    }
}
```

这个设计允许用户通过负数索引来定位参数，例如 `paramIdx = -1` 表示取方法的最后一个参数。这在参数列表长度不确定时特别有用。

---

## 第二阶段：ParamFlowRule 模型

### 2.1 ParamFlowRule 类定义

**源码位置**: `sentinel-extension/sentinel-parameter-flow-control/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/param/ParamFlowRule.java`

```java
public class ParamFlowRule extends AbstractRule {

    // 限流模式：0=线程数, 1=QPS
    private int grade = RuleConstant.FLOW_GRADE_QPS;

    // 参数下标（支持负数）
    private int paramIdx;

    // 默认限流阈值（每个参数值的 QPS 或线程数上限）
    private double count;

    // 流控效果：0=直接拒绝(令牌桶), 2=匀速排队(漏桶)
    private int controlBehavior = RuleConstant.CONTROL_BEHAVIOR_DEFAULT;

    // 排队模式下的最大等待时间（毫秒）
    private int maxQueueingTimeMs = 0;

    // 额外允许的突发量
    private int burstCount = 0;

    // 统计时间窗口（秒），默认 1 秒
    private long durationInSec = 1;

    // 特例参数列表——指定某些参数值使用不同的阈值
    private List<ParamFlowItem> paramFlowItemList = new ArrayList<>();

    // 内部解析后的特例参数 Map：参数值 → 阈值
    private Map<Object, Integer> hotItems = new HashMap<>();

    // ... getter/setter
}
```

### 2.2 字段详解

| 字段 | 类型 | 含义 | 示例 |
|------|------|------|------|
| `grade` | int | 限流维度 | 0=线程数, 1=QPS |
| `paramIdx` | int | 参数位置 | 0=第一个参数, -1=最后一个 |
| `count` | double | 默认阈值 | 100 表示每个参数值每秒最多 100 次 |
| `controlBehavior` | int | 流控效果 | 0=令牌桶, 2=漏桶排队 |
| `maxQueueingTimeMs` | int | 最大排队时间 | 500 表示最多等 500ms |
| `burstCount` | int | 突发容量 | 10 表示允许额外突发 10 个请求 |
| `durationInSec` | long | 统计窗口 | 1 表示 1 秒为一个窗口 |
| `paramFlowItemList` | List | 特例配置 | 某些热点值使用独立阈值 |

### 2.3 ParamFlowItem —— 特例参数配置

```java
public class ParamFlowItem {
    // 参数值的类对全限定名（如 "java.lang.String"）
    private String classType;
    // 参数值（字符串形式）
    private String object;
    // 该参数值的独立阈值
    private int count;
}
```

特例参数的使用场景：假设一个商品查询接口，绝大多数商品 ID 限流 100 QPS，但爆款商品（如 `item_12345`）限流 1000 QPS。通过 `paramFlowItemList` 可以实现这种差异化限流。

### 2.4 hotItems 解析

在规则加载时，`paramFlowItemList` 会被解析为 `hotItems` Map：

```java
// ParamFlowRuleUtil.parseHotItems(rule)
public static Map<Object, Integer> parseHotItems(ParamFlowRule rule) {
    Map<Object, Integer> itemMap = new HashMap<>();
    if (rule.getParamFlowItemList() == null) {
        return itemMap;
    }
    for (ParamFlowItem item : rule.getParamFlowItemList()) {
        // 根据 classType 将字符串形式的值转换为实际类型
        Object value = parseValue(item.getObject(), item.getClassType());
        if (value != null) {
            itemMap.put(value, item.getCount());
        }
    }
    return itemMap;
}
```

---

## 第三阶段：ParamFlowChecker.passCheck()

### 3.1 核心入口

**源码位置**: `sentinel-extension/sentinel-parameter-flow-control/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/param/ParamFlowChecker.java`

```java
public class ParamFlowChecker {

    public static boolean passCheck(ResourceWrapper resourceWrapper,
                                     ParamFlowRule rule, int count, Object... args) {
        if (args == null || args.length == 0) {
            return true;
        }

        int paramIdx = rule.getParamIdx();
        if (paramIdx < 0 || args.length <= paramIdx) {
            return true;  // 索引越界，跳过
        }

        // 提取参数值
        Object value = args[paramIdx];

        // null 参数值直接放行
        if (value == null) {
            return true;
        }

        // 支持 ParamFlowArgument 接口自定义键提取
        if (value instanceof ParamFlowArgument) {
            value = ((ParamFlowArgument) value).paramFlowKey();
        }

        // 集合/数组类型：逐元素检查
        if (value instanceof Collection) {
            for (Object element : (Collection<?>) value) {
                if (!passSingleValueCheck(resourceWrapper, rule, count, element)) {
                    return false;
                }
            }
            return true;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) {
                Object element = Array.get(value, i);
                if (!passSingleValueCheck(resourceWrapper, rule, count, element)) {
                    return false;
                }
            }
            return true;
        }

        // 普通单值检查
        return passSingleValueCheck(resourceWrapper, rule, count, value);
    }
}
```

### 3.2 ParamFlowArgument 接口

```java
public interface ParamFlowArgument {
    /**
     * 返回用于热点参数限流的键
     * 允许用户自定义如何从复杂对象中提取限流键
     */
    Object paramFlowKey();
}
```

使用场景：当参数是一个复杂对象（如 `OrderRequest`）时，可能需要按其中的某个字段（如 `userId`）来限流。通过实现此接口，可以灵活定义限流键的提取逻辑。

### 3.3 passSingleValueCheck —— 根据模式分流

```java
static boolean passSingleValueCheck(ResourceWrapper resourceWrapper,
                                     ParamFlowRule rule, int count, Object value) {
    // 集群模式检查（本文仅分析本地模式）
    if (rule.isClusterMode()) {
        return passClusterCheck(resourceWrapper, rule, count, value);
    }
    return passLocalCheck(resourceWrapper, rule, count, value);
}

private static boolean passLocalCheck(ResourceWrapper resourceWrapper,
                                       ParamFlowRule rule, int count, Object value) {
    // 根据限流维度分流
    if (rule.getGrade() == RuleConstant.FLOW_GRADE_QPS) {
        // QPS 模式
        if (rule.getControlBehavior() == RuleConstant.CONTROL_BEHAVIOR_RATE_LIMITER) {
            return passThrottleLocalCheck(resourceWrapper, rule, count, value);
        } else {
            return passDefaultLocalCheck(resourceWrapper, rule, count, value);
        }
    } else if (rule.getGrade() == RuleConstant.FLOW_GRADE_THREAD) {
        // 线程数模式
        return passThreadCheck(resourceWrapper, rule, count, value);
    }
    return true;
}
```

---

## 第四阶段：令牌桶算法 —— passDefaultLocalCheck()

### 4.1 完整源码

```java
static boolean passDefaultLocalCheck(ResourceWrapper resourceWrapper,
                                      ParamFlowRule rule, int acquireCount, Object value) {
    // 获取该规则对应的令牌桶存储
    ParameterMetric metric = getParameterMetric(resourceWrapper);
    if (metric == null) {
        return true;
    }
    CacheMap<Object, AtomicReference<TokenUpdateStatus>> tokenCounters =
        metric.getRuleTokenCounter(rule);
    if (tokenCounters == null) {
        return true;
    }

    // 确定该参数值的限流阈值（考虑特例配置）
    double tokenCount = getRuleThreshold(rule, value);
    if (tokenCount == 0) {
        return false;  // 阈值为 0，直接拒绝
    }

    // 最大令牌数 = 阈值 + 突发容量
    long maxCount = (long) tokenCount + rule.getBurstCount();

    // 当前时间
    long currentTime = TimeUtil.currentTimeMillis();

    // ===== CAS 自旋循环 =====
    while (true) {
        AtomicReference<TokenUpdateStatus> currentStatus = tokenCounters.get(value);

        // 情况1：首次访问该参数值
        if (currentStatus == null) {
            TokenUpdateStatus newStatus = new TokenUpdateStatus(currentTime,
                maxCount - acquireCount);
            AtomicReference<TokenUpdateStatus> ref = new AtomicReference<>(newStatus);
            AtomicReference<TokenUpdateStatus> racer = tokenCounters.putIfAbsent(value, ref);

            if (racer == null) {
                // putIfAbsent 成功，当前线程获得了初始化权
                return true;
            } else {
                // 存在竞争，使用已有的 ref 继续 CAS
                currentStatus = racer;
            }
        }

        // 情况2：已存在令牌桶，尝试 CAS 更新
        TokenUpdateStatus oldStatus = currentStatus.get();
        long lastAddTokenTime = oldStatus.getLastAddTokenTime();
        long restQps = oldStatus.getRestQps();

        // 计算距离上次补充令牌的时间差
        long passTime = currentTime - lastAddTokenTime;

        if (passTime > rule.getDurationInSec() * 1000) {
            // ===== 时间窗口已过，需要补充令牌 =====
            // 计算应补充的令牌数
            long toAddCount = (passTime * (long) tokenCount) / (rule.getDurationInSec() * 1000);
            // 新的剩余令牌数 = min(旧剩余 + 补充量, 最大容量) - 本次消耗
            long newRestQps = Math.min(restQps + toAddCount, maxCount) - acquireCount;

            TokenUpdateStatus newStatus = new TokenUpdateStatus(currentTime, newRestQps);
            if (currentStatus.compareAndSet(oldStatus, newStatus)) {
                return newRestQps >= 0;
            }
            // CAS 失败，自旋重试
            Thread.yield();

        } else {
            // ===== 在同一时间窗口内，直接扣减令牌 =====
            long newRestQps = restQps - acquireCount;

            TokenUpdateStatus newStatus = new TokenUpdateStatus(lastAddTokenTime, newRestQps);
            if (currentStatus.compareAndSet(oldStatus, newStatus)) {
                return newRestQps >= 0;
            }
            // CAS 失败，自旋重试
            Thread.yield();
        }
    }
}
```

### 4.2 TokenUpdateStatus —— 不可变状态对象

```java
/**
 * 令牌桶的状态快照（不可变对象，用于 CAS）
 */
class TokenUpdateStatus {
    // 上次补充令牌的时间
    private final long lastAddTokenTime;
    // 剩余令牌数（可以为负数，表示已超限）
    private final long restQps;

    public TokenUpdateStatus(long lastAddTokenTime, long restQps) {
        this.lastAddTokenTime = lastAddTokenTime;
        this.restQps = restQps;
    }

    public long getLastAddTokenTime() {
        return lastAddTokenTime;
    }

    public long getRestQps() {
        return restQps;
    }
}
```

**为什么用不可变对象 + CAS 而不是加锁？**

- 热点参数限流的场景下，同一参数值可能有极高的并发访问
- 加锁（synchronized 或 ReentrantLock）会导致线程阻塞和上下文切换
- CAS + 不可变对象是无锁编程的经典模式：读取旧值 → 计算新值 → CAS 替换，失败则重试
- `TokenUpdateStatus` 必须是不可变的，否则在 CAS 比较时可能出现 ABA 问题

### 4.3 令牌桶算法流程图解

```
                     首次访问？
                        |
              ┌─── Yes ─┤─── No ──┐
              |                     |
     分配 maxCount-acquireCount     |
     个令牌, 直接放行              |
                              计算 passTime
                                    |
                        ┌─ > durationInSec*1000 ─┐
                        |                          |
                   补充令牌:                  同窗口内:
              toAdd = passTime*token             直接扣减
                      /duration                 restQps -= acquireCount
              rest = min(old+toAdd,max)              |
                     - acquireCount            CAS 更新
                        |                          |
                   CAS 更新                  rest >= 0 ?
                        |                    Yes → 放行
                   rest >= 0 ?              No  → 拒绝
                   Yes → 放行
                   No  → 拒绝
```

### 4.4 getRuleThreshold —— 获取阈值（含特例处理）

```java
static double getRuleThreshold(ParamFlowRule rule, Object value) {
    // 检查该参数值是否有特例配置
    Map<Object, Integer> hotItems = rule.getParsedHotItems();
    if (hotItems != null && !hotItems.isEmpty()) {
        Integer itemCount = hotItems.get(value);
        if (itemCount != null) {
            return itemCount;  // 使用特例阈值
        }
    }
    // 使用默认阈值
    return rule.getCount();
}
```

### 4.5 突发流量处理 —— burstCount

```java
long maxCount = (long) tokenCount + rule.getBurstCount();
```

`burstCount` 允许令牌桶在初始时或经过一段空闲后积累超过 `tokenCount` 的令牌。例如：

- `tokenCount = 100`（QPS 限制 100）
- `burstCount = 20`（允许突发 20）
- 实际令牌桶容量 = 120

这意味着如果某个参数值有一段时间没有被访问（令牌积满到 120），突然来了一个瞬时高峰，可以允许最多 120 个请求通过，而不是严格的 100。这对应对突发流量非常有用。

### 4.6 时间窗口与令牌补充速率

```java
// durationInSec 默认为 1，表示 1 秒内补充 tokenCount 个令牌
long toAddCount = (passTime * (long) tokenCount) / (rule.getDurationInSec() * 1000);
```

- 如果 `tokenCount = 100`，`durationInSec = 1`，则每毫秒补充 0.1 个令牌
- 如果 `tokenCount = 100`，`durationInSec = 10`，则每毫秒补充 0.01 个令牌（10 秒内限 100 次）

`durationInSec` 支持大于 1 的值，使得热点参数限流不仅仅局限于 QPS 维度，还可以实现"N 秒内限 M 次"的效果。

---

## 第五阶段：漏桶算法 —— passThrottleLocalCheck()

### 5.1 完整源码

```java
static boolean passThrottleLocalCheck(ResourceWrapper resourceWrapper,
                                       ParamFlowRule rule, int acquireCount, Object value) {
    ParameterMetric metric = getParameterMetric(resourceWrapper);
    if (metric == null) {
        return true;
    }
    CacheMap<Object, AtomicLong> timeRecorders = metric.getRuleTimeCounter(rule);
    if (timeRecorders == null) {
        return true;
    }

    // 获取该参数值的阈值
    double tokenCount = getRuleThreshold(rule, value);
    if (tokenCount == 0) {
        return false;
    }

    long maxQueueingTimeMs = rule.getMaxQueueingTimeMs();
    long currentTime = TimeUtil.currentTimeMillis();

    // 计算每个请求的固定间隔时间
    // costTime = 1000 * acquireCount * durationInSec / tokenCount
    // 含义：如果限流 100 QPS (tokenCount=100, durationInSec=1)
    //       则每个请求之间应间隔 10ms
    long costTime = Math.round(1.0d * 1000 * acquireCount
                               * rule.getDurationInSec() / tokenCount);

    // ===== CAS 自旋循环 =====
    while (true) {
        AtomicLong lastPassTimeRef = timeRecorders.get(value);

        // 首次访问该参数值
        if (lastPassTimeRef == null) {
            AtomicLong newTime = new AtomicLong(currentTime);
            AtomicLong racer = timeRecorders.putIfAbsent(value, newTime);
            if (racer == null) {
                return true;  // 首次直接通过
            } else {
                lastPassTimeRef = racer;
            }
        }

        long lastPassTime = lastPassTimeRef.get();

        // 计算预期通过时间
        long expectedTime = lastPassTime + costTime;

        if (expectedTime <= currentTime) {
            // ===== 当前时间已超过预期时间，可以立即通过 =====
            if (lastPassTimeRef.compareAndSet(lastPassTime, currentTime)) {
                return true;
            }
            // CAS 失败，自旋重试
        } else {
            // ===== 需要排队等待 =====
            long waitTime = expectedTime - currentTime;

            if (waitTime > maxQueueingTimeMs) {
                // 等待时间超过最大排队时间，直接拒绝
                return false;
            }

            // 尝试预约该时间点
            long oldTime = lastPassTimeRef.get();
            long newPassTime = oldTime + costTime;

            if (lastPassTimeRef.compareAndSet(oldTime, newPassTime)) {
                // 预约成功，计算实际需要等待的时间
                long actualWaitTime = newPassTime - currentTime;

                if (actualWaitTime > maxQueueingTimeMs) {
                    // 再次检查：由于并发，实际等待时间可能已超限
                    // 回滚预约
                    lastPassTimeRef.addAndGet(-costTime);
                    return false;
                }

                // 等待到预约时间
                if (actualWaitTime > 0) {
                    try {
                        Thread.sleep(actualWaitTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return true;
            }
            // CAS 失败，自旋重试
        }
        Thread.yield();
    }
}
```

### 5.2 漏桶算法原理

漏桶算法（Leaky Bucket）的核心思想是：以恒定的速率处理请求，就像水从桶底的小孔以恒定速率流出一样。

```
请求到达 → 放入桶中排队 → 以恒定速率离开（被处理）

匀速排队示意：
  时间轴: ---|-----|-----|-----|-----|---→
              t1    t2    t3    t4    t5
  间隔:    costTime  costTime  ...
  
  每个参数值独立一个时间线
  lastPassTime 记录上一个请求的预约通过时间
  下一个请求的预期时间 = lastPassTime + costTime
```

### 5.3 costTime 的计算

```java
long costTime = Math.round(1.0d * 1000 * acquireCount * rule.getDurationInSec() / tokenCount);
```

| tokenCount | durationInSec | costTime (ms) | 含义 |
|-----------|---------------|---------------|------|
| 100 | 1 | 10 | 每个参数值每 10ms 通过一个请求 |
| 50 | 1 | 20 | 每个参数值每 20ms 通过一个请求 |
| 100 | 10 | 100 | 每个参数值每 100ms 通过一个请求 |
| 1 | 1 | 1000 | 每个参数值每秒只能通过一个请求 |

### 5.4 排队与超时机制

漏桶模式下，请求不是立即被拒绝，而是可以排队等待：

```
请求到达时间: currentTime = 1000ms
上次预约时间: lastPassTime = 990ms
固定间隔:    costTime = 10ms
预期通过时间: expectedTime = 990 + 10 = 1000ms

情况1: expectedTime(1000) <= currentTime(1000) → 立即通过

---

请求到达时间: currentTime = 1000ms
上次预约时间: lastPassTime = 1005ms （前面有排队的请求）
固定间隔:    costTime = 10ms
预期通过时间: expectedTime = 1005 + 10 = 1015ms
等待时间:    waitTime = 1015 - 1000 = 15ms

情况2: waitTime(15ms) <= maxQueueingTimeMs(500ms) → sleep(15ms) 后通过

---

请求到达时间: currentTime = 1000ms
上次预约时间: lastPassTime = 2000ms （队列很长）
固定间隔:    costTime = 10ms
预期通过时间: expectedTime = 2000 + 10 = 2010ms
等待时间:    waitTime = 2010 - 1000 = 1010ms

情况3: waitTime(1010ms) > maxQueueingTimeMs(500ms) → 直接拒绝
```

### 5.5 令牌桶 vs 漏桶对比

| 特性 | 令牌桶 (passDefaultLocalCheck) | 漏桶 (passThrottleLocalCheck) |
|------|------|------|
| 流量整形方式 | 允许突发，平均速率受限 | 严格匀速，无突发 |
| 请求处理 | 有令牌立即通过，无令牌拒绝 | 排队等待或超时拒绝 |
| 适用场景 | 容忍突发的普通限流 | 需要匀速处理的场景（如消息队列消费） |
| 存储结构 | CacheMap<Object, AtomicReference<TokenUpdateStatus>> | CacheMap<Object, AtomicLong> |
| 时间延迟 | 无延迟（要么通过要么拒绝） | 可能产生排队延迟 |

---

## 第六阶段：ParameterMetric 存储结构

### 6.1 ParameterMetric 类定义

**源码位置**: `sentinel-extension/sentinel-parameter-flow-control/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/param/ParameterMetric.java`

```java
public class ParameterMetric {

    // 漏桶模式：存储每个参数值的上次通过时间
    // key = ParamFlowRule, value = CacheMap<参数值, AtomicLong(lastPassTime)>
    private final Map<ParamFlowRule, CacheMap<Object, AtomicLong>> ruleTimeCounters =
        new ConcurrentHashMap<>();

    // 令牌桶模式：存储每个参数值的令牌状态
    // key = ParamFlowRule, value = CacheMap<参数值, AtomicReference<TokenUpdateStatus>>
    private final Map<ParamFlowRule, CacheMap<Object, AtomicReference<TokenUpdateStatus>>>
        ruleTokenCounter = new ConcurrentHashMap<>();

    // 线程数模式：存储每个参数值的当前线程数
    // key = paramIdx, value = CacheMap<参数值, AtomicInteger(threadCount)>
    private final Map<Integer, CacheMap<Object, AtomicInteger>> threadCountMap =
        new ConcurrentHashMap<>();

    /**
     * 为指定规则初始化存储结构
     */
    public void initialize(ParamFlowRule rule) {
        if (!ruleTimeCounters.containsKey(rule)) {
            // 计算 CacheMap 容量
            long capacity = calculateCapacity(rule);
            synchronized (lock) {
                if (!ruleTimeCounters.containsKey(rule)) {
                    ruleTimeCounters.put(rule, new ConcurrentLinkedHashMapWrapper<>(capacity));
                }
            }
        }
        if (!ruleTokenCounter.containsKey(rule)) {
            long capacity = calculateCapacity(rule);
            synchronized (lock) {
                if (!ruleTokenCounter.containsKey(rule)) {
                    ruleTokenCounter.put(rule, new ConcurrentLinkedHashMapWrapper<>(capacity));
                }
            }
        }
        if (!threadCountMap.containsKey(rule.getParamIdx())) {
            long capacity = calculateCapacity(rule);
            synchronized (lock) {
                if (!threadCountMap.containsKey(rule.getParamIdx())) {
                    threadCountMap.put(rule.getParamIdx(),
                        new ConcurrentLinkedHashMapWrapper<>(capacity));
                }
            }
        }
    }

    /**
     * 计算 CacheMap 容量
     * capacity = min(4000 * durationInSec, 200000)
     */
    private long calculateCapacity(ParamFlowRule rule) {
        long base = 4000L * rule.getDurationInSec();
        return Math.min(base, 200_000L);
    }

    // ===== 漏桶相关方法 =====
    public CacheMap<Object, AtomicLong> getRuleTimeCounter(ParamFlowRule rule) {
        return ruleTimeCounters.get(rule);
    }

    // ===== 令牌桶相关方法 =====
    public CacheMap<Object, AtomicReference<TokenUpdateStatus>> getRuleTokenCounter(
            ParamFlowRule rule) {
        return ruleTokenCounter.get(rule);
    }

    // ===== 线程数相关方法 =====
    public void addThreadCount(Object value, int paramIdx) {
        CacheMap<Object, AtomicInteger> map = threadCountMap.get(paramIdx);
        if (map != null) {
            AtomicInteger count = map.get(value);
            if (count == null) {
                AtomicInteger newCount = new AtomicInteger(0);
                AtomicInteger existing = map.putIfAbsent(value, newCount);
                count = (existing != null) ? existing : newCount;
            }
            count.incrementAndGet();
        }
    }

    public void decreaseThreadCount(Object value) {
        // 遍历所有 paramIdx 的 threadCountMap 递减
        for (Map.Entry<Integer, CacheMap<Object, AtomicInteger>> entry :
                threadCountMap.entrySet()) {
            CacheMap<Object, AtomicInteger> map = entry.getValue();
            if (map != null) {
                AtomicInteger count = map.get(value);
                if (count != null && count.get() > 0) {
                    count.decrementAndGet();
                }
            }
        }
    }

    public long getThreadCount(int paramIdx, Object value) {
        CacheMap<Object, AtomicInteger> map = threadCountMap.get(paramIdx);
        if (map == null) {
            return 0;
        }
        AtomicInteger count = map.get(value);
        return count == null ? 0 : count.get();
    }
}
```

### 6.2 三种存储的关系图

```
ParameterMetric (per Resource)
├── ruleTimeCounters: Map<ParamFlowRule, CacheMap>
│     └── CacheMap<Object(参数值), AtomicLong(lastPassTime)>   ← 漏桶模式
│
├── ruleTokenCounter: Map<ParamFlowRule, CacheMap>
│     └── CacheMap<Object(参数值), AtomicRef<TokenUpdateStatus>> ← 令牌桶模式
│
└── threadCountMap: Map<Integer(paramIdx), CacheMap>
      └── CacheMap<Object(参数值), AtomicInteger(threadCount)>  ← 线程数模式
```

### 6.3 容量计算策略

```java
long capacity = Math.min(4000L * rule.getDurationInSec(), 200_000L);
```

| durationInSec | 计算容量 | 最终容量 |
|--------------|---------|---------|
| 1 | 4000×1 = 4000 | 4000 |
| 10 | 4000×10 = 40000 | 40000 |
| 60 | 4000×60 = 240000 | 200000 (cap) |
| 100 | 4000×100 = 400000 | 200000 (cap) |

**设计考量**：

- `durationInSec` 越大，意味着统计窗口越长，可能出现的不同参数值越多，所以需要更大的缓存容量。
- 设置 200000 的硬上限是为了防止内存无限增长。
- 4000 的系数是一个经验值，假设在 1 秒内一个典型系统最多出现 4000 个不同的参数值。

### 6.4 ParameterMetric 的生命周期管理

`ParameterMetric` 通过 `ParameterMetricStorage` 管理，与资源（`ResourceWrapper`）绑定：

```java
public class ParameterMetricStorage {

    // 全局存储：资源名 → ParameterMetric
    private static final Map<String, ParameterMetric> metricsMap =
        new ConcurrentHashMap<>();

    public static ParameterMetric getOrCreateParamMetric(ResourceWrapper resourceWrapper) {
        String resourceName = resourceWrapper.getName();
        ParameterMetric metric = metricsMap.get(resourceName);
        if (metric == null) {
            synchronized (lock) {
                metric = metricsMap.get(resourceName);
                if (metric == null) {
                    metric = new ParameterMetric();
                    metricsMap.put(resourceName, metric);
                }
            }
        }
        return metric;
    }
}
```

---

## 第七阶段：LRU 缓存淘汰 —— CacheMap

### 7.1 CacheMap 接口

```java
public interface CacheMap<K, V> {

    /**
     * 如果 key 不存在则放入，返回 null
     * 如果 key 已存在则返回已有的 value（不覆盖）
     */
    V putIfAbsent(K key, V value);

    /**
     * 获取 key 对应的 value
     */
    V get(K key);

    /**
     * 放入 key-value，覆盖已有值
     */
    V put(K key, V value);

    /**
     * 删除 key
     */
    V remove(K key);

    /**
     * 当前缓存大小
     */
    long size();
}
```

### 7.2 ConcurrentLinkedHashMapWrapper —— 核心实现

**源码位置**: `sentinel-extension/sentinel-parameter-flow-control/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/param/ConcurrentLinkedHashMapWrapper.java`

```java
public class ConcurrentLinkedHashMapWrapper<K, V> implements CacheMap<K, V> {

    // 底层使用 Google Guava 的 ConcurrentLinkedHashMap
    private final ConcurrentLinkedHashMap<K, V> map;

    public ConcurrentLinkedHashMapWrapper(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.map = new ConcurrentLinkedHashMap.Builder<K, V>()
            .maximumWeightedCapacity(capacity)
            .concurrencyLevel(16)
            .build();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return map.putIfAbsent(key, value);
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    public long size() {
        return map.size();
    }
}
```

### 7.3 ConcurrentLinkedHashMap 的淘汰策略

`ConcurrentLinkedHashMap` 是 Google 开源的高性能并发 LRU 缓存实现，其核心特性：

**1. W-TinyLFU / LRU 淘汰**：

当缓存条目数超过 `maximumWeightedCapacity` 时，会自动淘汰最不常用（least recently used）的条目。在热点参数限流场景中，这意味着：

- 高频访问的参数值（热点）会被保留在缓存中
- 低频或一次性的参数值会被自动淘汰
- 被淘汰的参数值下次访问时会被当作"首次访问"，重新分配令牌

**2. 并发安全**：

- 使用分段锁（`concurrencyLevel = 16`），支持高并发读写
- 内部维护了一个基于 CAS 的双向链表来记录访问顺序

**3. O(1) 时间复杂度**：

- get/put/remove 都是 O(1)
- 淘汰操作也是 O(1)（直接从链表尾部移除）

### 7.4 为什么需要 LRU 缓存

考虑一个实际场景——一个搜索接口按搜索关键词限流：

```
paramIdx = 0  (第一个参数是搜索关键词)
count = 10    (每个关键词每秒最多查 10 次)
```

在生产环境中，搜索关键词的种类可能是无限的（用户可以搜任何内容）。如果不限制缓存大小：

- 每个唯一的搜索关键词都会创建一个 `AtomicReference<TokenUpdateStatus>` 对象
- 随着时间推移，内存占用会无限增长
- 最终导致 OOM（Out of Memory）

LRU 缓存通过设置容量上限解决了这个问题：

```
容量 = min(4000 * durationInSec, 200000)

以默认 durationInSec=1 为例，最多缓存 4000 个参数值的令牌桶状态。
超过 4000 个不同参数值时，最不活跃的会被淘汰。
```

### 7.5 淘汰对限流准确性的影响

当一个参数值被 LRU 淘汰后再次出现：

- **令牌桶模式**：被视为"首次访问"，会分配 `maxCount - acquireCount` 个令牌。这意味着该参数值获得了"重生"，短暂地拥有了满额度。
- **漏桶模式**：被视为"首次访问"，`lastPassTime` 重置为当前时间，可以立即通过。

这在实践中是可接受的，因为被 LRU 淘汰的参数值本身就是低频的——它们之间已经有很长的时间间隔，即使不被淘汰，令牌桶也会补满。

---

## 第八阶段：线程数模式

### 8.1 passThreadCheck 源码

```java
static boolean passThreadCheck(ResourceWrapper resourceWrapper,
                                ParamFlowRule rule, int count, Object value) {
    ParameterMetric metric = getParameterMetric(resourceWrapper);
    if (metric == null) {
        return true;
    }

    // 获取该参数值的限流阈值
    double threadCount = getRuleThreshold(rule, value);
    if (threadCount == 0) {
        return false;
    }

    // 获取当前线程数
    long currentThread = metric.getThreadCount(rule.getParamIdx(), value);

    // 直接比较：当前线程数 >= 阈值则拒绝
    return currentThread < threadCount;
}
```

### 8.2 线程计数的增减时机

```java
// 请求进入时（在 passCheck 之后）增加线程数
// 实际在 ParamFlowStatisticEntryCallback 中调用
public class ParamFlowStatisticEntryCallback implements ProcessorSlotEntryCallback<DefaultNode> {

    @Override
    public void onPass(Context context, ResourceWrapper resourceWrapper,
                       DefaultNode node, int count, Object... args) {
        // 请求通过后，增加对应参数值的线程计数
        if (args == null || args.length == 0) {
            return;
        }
        List<ParamFlowRule> rules = ParamFlowRuleManager.getRulesOfResource(
            resourceWrapper.getName());
        if (rules == null) {
            return;
        }
        for (ParamFlowRule rule : rules) {
            if (rule.getGrade() == RuleConstant.FLOW_GRADE_THREAD) {
                int idx = rule.getParamIdx();
                if (idx >= 0 && idx < args.length) {
                    Object value = args[idx];
                    ParameterMetric metric = ParameterMetricStorage
                        .getOrCreateParamMetric(resourceWrapper);
                    metric.addThreadCount(value, idx);
                }
            }
        }
    }
}
```

```java
// 请求退出时（在 ParamFlowSlot.exit() 中）减少线程数
@Override
public void exit(Context context, ResourceWrapper resourceWrapper,
                 int count, Object... args) {
    // ... 递减线程计数（见第一阶段 1.1 的 exit 方法）
}
```

### 8.3 线程数模式 vs QPS 模式对比

| 特性 | 线程数模式 | QPS 模式 |
|------|-----------|---------|
| 限流依据 | 同时处理的请求数 | 单位时间内的请求数 |
| 计数方式 | entry 时 +1, exit 时 -1 | 基于令牌桶/漏桶算法 |
| 适用场景 | 保护下游慢服务 | 流量整形、削峰填谷 |
| 对慢请求的敏感度 | 高（慢请求会长期占用线程配额） | 低（只看请求数量不看耗时） |
| 实现复杂度 | 简单（原子递增递减） | 复杂（CAS 自旋、时间窗口计算） |

---

## 总结

### 整体设计哲学

Sentinel 热点参数限流的核心设计理念是**精细化、自适应、无锁化**：

1. **每个参数值独立限流**：不同于传统的资源级限流（如 FlowSlot），热点参数限流为每个参数值维护独立的令牌桶或计数器。这使得即使某个参数值被限流，也不影响其他参数值的正常访问。

2. **CAS 无锁设计**：令牌桶和漏桶算法都采用 CAS 自旋 + 不可变对象的模式，避免了锁竞争。在高并发场景下，CAS 的性能远优于互斥锁。

3. **LRU 缓存保护内存**：通过 `ConcurrentLinkedHashMap` 限制每个规则的最大参数值缓存数量，防止高基数参数（如用户 ID、搜索关键词）导致内存无限增长。

4. **灵活的流控策略**：支持令牌桶（允许突发）、漏桶（匀速排队）、线程数（并发控制）三种模式，覆盖不同业务场景。

5. **特例参数机制**：通过 `paramFlowItemList` 支持对特定参数值设置不同阈值，满足差异化限流需求。

### 核心数据结构一览

```
ParamFlowSlot
  └── ParamFlowChecker
        ├── passDefaultLocalCheck (令牌桶)
        │     └── CacheMap<参数值, AtomicReference<TokenUpdateStatus{lastAddTokenTime, restQps}>>
        │           └── CAS自旋: 补充令牌 → 扣减令牌 → 判断 restQps >= 0
        │
        ├── passThrottleLocalCheck (漏桶)
        │     └── CacheMap<参数值, AtomicLong(lastPassTime)>
        │           └── CAS自旋: 计算expectedTime → sleep等待 或 超时拒绝
        │
        └── passThreadCheck (线程数)
              └── CacheMap<参数值, AtomicInteger(threadCount)>
                    └── 直接比较: currentThread >= threshold → 拒绝
```

### 关键公式

```
令牌桶：
  maxCount = tokenCount + burstCount
  toAddCount = passTime × tokenCount / (durationInSec × 1000)
  newRestQps = min(oldRest + toAddCount, maxCount) - acquireCount
  通过条件：newRestQps >= 0

漏桶：
  costTime = 1000 × acquireCount × durationInSec / tokenCount
  expectedTime = lastPassTime + costTime
  waitTime = expectedTime - currentTime
  通过条件：waitTime <= maxQueueingTimeMs（需要 sleep(waitTime)）
  拒绝条件：waitTime > maxQueueingTimeMs

缓存容量：
  capacity = min(4000 × durationInSec, 200000)
```

### 完整调用链回顾

```
ParamFlowSlot.entry()
  → checkFlow(resourceWrapper, count, args)
    → ParamFlowRuleManager.getRulesOfResource(resourceName)
    → for each rule:
        → applyRealParamIdx(rule, args.length)   // 处理负数索引
        → metric.initialize(rule)                  // 初始化 CacheMap
        → ParamFlowChecker.passCheck(resource, rule, count, args)
            → 提取 args[paramIdx]
            → null → pass
            → Collection/Array → 逐元素检查
            → passLocalCheck():
                [QPS + DEFAULT]       → passDefaultLocalCheck()  // 令牌桶 CAS
                [QPS + RATE_LIMITER]  → passThrottleLocalCheck() // 漏桶排队
                [THREAD]              → passThreadCheck()        // 线程数比较
            → 不通过 → throw ParamFlowException
  → fireEntry() 传递到下一个 Slot
```
