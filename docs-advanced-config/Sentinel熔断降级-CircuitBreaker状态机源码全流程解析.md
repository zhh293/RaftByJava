# Sentinel 熔断降级 —— CircuitBreaker 状态机源码全流程解析

> 基于源码项目 /Users/zhanghonghao/Desktop/Sentinel 逐步分析，从 DegradeSlot 入口到 CircuitBreaker 状态机转换，不跳步、不省略。

---

## 全局调用链总览

```
DegradeSlot.entry() → performChecking() → CircuitBreaker.tryPass()
  CLOSED → 直接放行
  OPEN → 检查超时 → CAS转HALF_OPEN → 允许一个probe请求
  HALF_OPEN → 拒绝（只允许一个probe）
  
DegradeSlot.exit() → CircuitBreaker.onRequestComplete()
  HALF_OPEN: probe结果判定 → 正常则HALF_OPEN→CLOSED / 异常则HALF_OPEN→OPEN
  CLOSED: 统计窗口汇总 → 超阈值则CLOSED→OPEN
```

详细的调用层次如下：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         请求进入 SPI Chain                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────────────────────────────────────────────┐               │
│  │  DegradeSlot.entry(context, resourceWrapper, ...)    │               │
│  │      │                                               │               │
│  │      ▼                                               │               │
│  │  performChecking(context, resource)                  │               │
│  │      │                                               │               │
│  │      ▼                                               │               │
│  │  for (CircuitBreaker cb : breakers)                  │               │
│  │      │                                               │               │
│  │      ▼                                               │               │
│  │  cb.tryPass(context)                                 │               │
│  │      │                                               │               │
│  │      ├── CLOSED  → return true (放行)                │               │
│  │      ├── OPEN    → retryTimeoutArrived()?            │               │
│  │      │               ├── YES → CAS→HALF_OPEN        │               │
│  │      │               │         return true (probe)   │               │
│  │      │               └── NO  → return false (熔断)   │               │
│  │      └── HALF_OPEN → return false (已有probe在跑)    │               │
│  │                                                      │               │
│  │  if tryPass == false:                                │               │
│  │      throw DegradeException(rule.getLimitApp())       │               │
│  └──────────────────────────────────────────────────────┘               │
│                                                                         │
│  ┌──────────────────────────────────────────────────────┐               │
│  │  DegradeSlot.exit(context, resourceWrapper, count)   │               │
│  │      │                                               │               │
│  │      ▼                                               │               │
│  │  if (!context.getCurEntry().getBlockError())          │               │
│  │      │                                               │               │
│  │      ▼                                               │               │
│  │  for (CircuitBreaker cb : breakers)                  │               │
│  │      │                                               │               │
│  │      ▼                                               │               │
│  │  cb.onRequestComplete(context)                       │               │
│  │      │                                               │               │
│  │      ├── HALF_OPEN:                                  │               │
│  │      │     ├── 请求正常 → fromHalfOpenToClose()      │               │
│  │      │     └── 请求异常 → fromHalfOpenToOpen()       │               │
│  │      │                                               │               │
│  │      └── CLOSED:                                     │               │
│  │            统计窗口汇总                               │               │
│  │            if 超阈值 → fromCloseToOpen()              │               │
│  └──────────────────────────────────────────────────────┘               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 第一阶段：DegradeSlot 入口

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/DegradeSlot.java`

DegradeSlot 是 Sentinel 责任链（ProcessorSlotChain）中负责熔断降级的处理槽，通过 `@Spi(order = -1000)` 注解声明其在链中的优先级。

### 1.1 类声明与 SPI 注册

```java
@Spi(order = Constants.ORDER_DEGRADE_SLOT)  // ORDER_DEGRADE_SLOT = -1000
public class DegradeSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node,
                      int count, boolean prioritized, Object... args) throws Throwable {
        performChecking(context, resourceWrapper);
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        Entry curEntry = context.getCurEntry();
        // 只有未被阻断的请求才需要做完成统计
        if (curEntry.getBlockError() != null) {
            fireExit(context, resourceWrapper, count, args);
            return;
        }
        List<CircuitBreaker> circuitBreakers = DegradeRuleManager.getCircuitBreakers(resourceWrapper.getName());
        if (circuitBreakers == null || circuitBreakers.isEmpty()) {
            fireExit(context, resourceWrapper, count, args);
            return;
        }
        // 逐一通知每个 CircuitBreaker 请求完成
        if (curEntry.getBlockError() == null) {
            for (CircuitBreaker cb : circuitBreakers) {
                cb.onRequestComplete(context);
            }
        }
        fireExit(context, resourceWrapper, count, args);
    }
}
```

### 1.2 performChecking 核心逻辑

```java
void performChecking(Context context, ResourceWrapper r) throws BlockException {
    // 从 DegradeRuleManager 获取当前资源关联的所有 CircuitBreaker
    List<CircuitBreaker> circuitBreakers = DegradeRuleManager.getCircuitBreakers(r.getName());
    if (circuitBreakers == null || circuitBreakers.isEmpty()) {
        return;
    }
    for (CircuitBreaker cb : circuitBreakers) {
        if (!cb.tryPass(context)) {
            // 熔断器拒绝通过，抛出 DegradeException（BlockException 的子类）
            throw new DegradeException(cb.getRule().getLimitApp(), cb.getRule());
        }
    }
}
```

**关键点说明**：

1. `performChecking` 遍历当前资源对应的所有 CircuitBreaker（一个资源可能配置多条熔断规则，每条规则对应一个 CircuitBreaker 实例）。
2. 只要有一个 CircuitBreaker 返回 `false`（即拒绝通过），立即抛出 `DegradeException`，中断后续处理。
3. `DegradeException` 继承自 `BlockException`，上层可通过 `entry.getBlockError()` 判断本次请求是否被熔断。

### 1.3 exit 阶段的统计更新

```java
// exit 中的核心逻辑
if (curEntry.getBlockError() == null) {
    for (CircuitBreaker cb : circuitBreakers) {
        cb.onRequestComplete(context);
    }
}
```

**设计要点**：
- 只有「通过」的请求（未被任何 Slot 阻断）才会触发 `onRequestComplete`，这是统计的数据来源。
- 被阻断的请求不参与熔断统计，避免「因熔断而被阻断的请求又去影响熔断统计」的递归效应。
- `onRequestComplete` 是 CircuitBreaker 判断是否需要触发状态转换的核心回调。

---

## 第二阶段：DegradeRule 规则模型

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/DegradeRule.java`

DegradeRule 继承自 `AbstractRule`，定义了一条熔断降级规则的完整参数：

### 2.1 核心字段定义

```java
public class DegradeRule extends AbstractRule {

    /**
     * 熔断策略类型
     * 0 = SLOW_REQUEST_RATIO (慢调用比例，基于 RT)
     * 1 = ERROR_RATIO (异常比例)
     * 2 = ERROR_COUNT (异常数)
     */
    private int grade = RuleConstant.DEGRADE_GRADE_RT;

    /**
     * 阈值
     * - grade=0 时: 表示慢调用 RT 阈值（毫秒），超过此值算「慢调用」
     * - grade=1 时: 表示异常比例阈值 [0.0, 1.0]
     * - grade=2 时: 表示异常数阈值
     */
    private double count;

    /**
     * 熔断恢复超时（秒）
     * 从 OPEN 状态进入后，经过 timeWindow 秒后允许进入 HALF_OPEN
     */
    private int timeWindow;

    /**
     * 最小请求数（默认 5）
     * 统计窗口内请求总数不足此值时，不触发熔断判定
     * 防止因极少量请求导致误判
     */
    private int minRequestAmount = RuleConstant.DEGRADE_DEFAULT_MIN_REQUEST_AMOUNT; // 5

    /**
     * 慢调用比例阈值（仅 grade=0 时生效）
     * 默认 1.0，即 100% 的请求都是慢调用才触发
     * 可设置为 0.5 表示超过 50% 的请求为慢调用时触发熔断
     */
    private double slowRatioThreshold = 1.0d;

    /**
     * 统计时间窗口（毫秒，默认 1000ms）
     * CircuitBreaker 内部的 LeapArray 将以此为周期进行统计
     */
    private int statIntervalMs = 1000;
}
```

### 2.2 字段详解

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `grade` | int | 0 | 熔断策略：0=慢调用比例, 1=异常比例, 2=异常数 |
| `count` | double | - | 阈值（含义取决于 grade） |
| `timeWindow` | int | - | 熔断恢复超时，单位秒 |
| `minRequestAmount` | int | 5 | 最小请求数，低于此值不触发熔断 |
| `slowRatioThreshold` | double | 1.0 | 慢调用比例阈值（0~1），仅 grade=0 有效 |
| `statIntervalMs` | int | 1000 | 统计窗口长度（毫秒） |

### 2.3 规则校验

```java
public boolean passCheck(Context context, DefaultNode node, int acquireCount, Object... args) {
    // DegradeRule 的 passCheck 在新版本中已废弃
    // 实际逻辑全部委托给 CircuitBreaker
    return true;
}
```

**历史演进说明**：在 Sentinel 1.8.0 之前，DegradeRule 自身承载了熔断逻辑。1.8.0 重构后引入 CircuitBreaker 状态机抽象，DegradeRule 退化为纯数据模型，逻辑全部由 CircuitBreaker 承载。

---

## 第三阶段：DegradeRuleManager —— 规则管理与 CircuitBreaker 创建

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/DegradeRuleManager.java`

DegradeRuleManager 负责管理熔断规则到 CircuitBreaker 实例的映射关系，是连接「规则配置」与「状态机实例」的桥梁。

### 3.1 核心数据结构

```java
public final class DegradeRuleManager {

    // 资源名 → CircuitBreaker 列表
    private static volatile Map<String, List<CircuitBreaker>> circuitBreakers = new HashMap<>();

    // 资源名 → DegradeRule 列表（用于规则查询）
    private static volatile Map<String, Set<DegradeRule>> ruleMap = new HashMap<>();

    // 规则属性监听器
    private static final RulePropertyListener LISTENER = new RulePropertyListener();
    private static SentinelProperty<List<DegradeRule>> currentProperty
            = new DynamicSentinelProperty<>();

    static {
        currentProperty.addListener(LISTENER);
    }
}
```

### 3.2 获取 CircuitBreaker 列表

```java
public static List<CircuitBreaker> getCircuitBreakers(String resourceName) {
    return circuitBreakers.get(resourceName);
}
```

此方法被 `DegradeSlot.performChecking()` 和 `DegradeSlot.exit()` 调用，获取指定资源的所有熔断器实例。

### 3.3 RulePropertyListener —— 规则变更监听

```java
private static class RulePropertyListener implements PropertyListener<List<DegradeRule>> {

    private synchronized void reloadFrom(List<DegradeRule> list) {
        Map<String, List<CircuitBreaker>> cbs = buildCircuitBreakers(list);
        Map<String, Set<DegradeRule>> rm = buildRuleMap(list);
        // 原子替换引用
        circuitBreakers = cbs;
        ruleMap = rm;
    }

    @Override
    public void configUpdate(List<DegradeRule> conf) {
        reloadFrom(conf);
    }

    @Override
    public void configLoad(List<DegradeRule> conf) {
        reloadFrom(conf);
    }
}
```

### 3.4 buildCircuitBreakers —— 根据规则创建状态机

```java
private static Map<String, List<CircuitBreaker>> buildCircuitBreakers(List<DegradeRule> list) {
    Map<String, List<CircuitBreaker>> cbMap = new HashMap<>();
    if (list == null || list.isEmpty()) {
        return cbMap;
    }
    for (DegradeRule rule : list) {
        if (!isValidRule(rule)) {
            RecordLog.warn("[DegradeRuleManager] Ignoring invalid rule: {}", rule);
            continue;
        }
        if (StringUtil.isBlank(rule.getLimitApp())) {
            rule.setLimitApp(RuleConstant.LIMIT_APP_DEFAULT);
        }

        String resource = rule.getResource();
        List<CircuitBreaker> cbList = cbMap.computeIfAbsent(resource, k -> new ArrayList<>());

        // 关键：尝试复用已有的同规则 CircuitBreaker（保留状态）
        CircuitBreaker cb = getExistingSameCbOrNew(rule);
        cbList.add(cb);
    }
    return cbMap;
}
```

### 3.5 getExistingSameCbOrNew —— 状态保持策略

```java
private static CircuitBreaker getExistingSameCbOrNew(DegradeRule rule) {
    List<CircuitBreaker> existingCbs = circuitBreakers.get(rule.getResource());
    if (existingCbs == null || existingCbs.isEmpty()) {
        return newCircuitBreaker(rule);
    }
    // 遍历已有 CircuitBreaker，如果规则参数完全一致，则复用（保留当前状态）
    for (CircuitBreaker existingCb : existingCbs) {
        if (existingCb.getRule().equals(rule)) {
            // 规则未变，复用原 CB，保留其 OPEN/HALF_OPEN/CLOSED 状态
            return existingCb;
        }
    }
    // 规则变更或新规则，创建全新的 CB（初始状态为 CLOSED）
    return newCircuitBreaker(rule);
}
```

**设计意义**：动态规则变更时，只有参数真正改变的规则才会重建 CircuitBreaker。未变更的规则保留原来的状态机状态，避免因规则推送导致所有熔断器「被重置为 CLOSED」。

### 3.6 newCircuitBreaker —— 工厂方法

```java
private static CircuitBreaker newCircuitBreaker(DegradeRule rule) {
    switch (rule.getGrade()) {
        case RuleConstant.DEGRADE_GRADE_RT:
            // grade=0: 慢调用比例熔断器
            return new ResponseTimeCircuitBreaker(rule);
        case RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO:
            // grade=1: 异常比例熔断器
            return new ExceptionCircuitBreaker(rule);
        case RuleConstant.DEGRADE_GRADE_EXCEPTION_COUNT:
            // grade=2: 异常数熔断器（与异常比例复用同一实现，通过 strategy 区分）
            return new ExceptionCircuitBreaker(rule);
        default:
            return null;
    }
}
```

---

## 第四阶段：CircuitBreaker 接口与抽象基类

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/circuitbreaker/CircuitBreaker.java`

### 4.1 CircuitBreaker 接口

```java
public interface CircuitBreaker {

    /**
     * 获取关联的降级规则
     */
    DegradeRule getRule();

    /**
     * 尝试通过熔断器
     * @return true=允许通过, false=熔断拒绝
     */
    boolean tryPass(Context context);

    /**
     * 获取当前状态
     */
    State currentState();

    /**
     * 请求完成时的回调，用于统计和状态判定
     */
    void onRequestComplete(Context context);
}
```

### 4.2 State 枚举

```java
public enum State {
    /**
     * 熔断器打开，拒绝所有请求（除非超时后允许probe）
     */
    OPEN,
    /**
     * 半开状态，允许一个probe请求通过以试探下游是否恢复
     */
    HALF_OPEN,
    /**
     * 关闭状态，正常放行所有请求
     */
    CLOSED
}
```

### 4.3 AbstractCircuitBreaker 抽象基类

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/circuitbreaker/AbstractCircuitBreaker.java`

```java
public abstract class AbstractCircuitBreaker implements CircuitBreaker {

    protected final DegradeRule rule;
    protected final int recoveryTimeoutMs;

    // 状态机核心：使用 AtomicReference 保证 CAS 原子转换
    protected final AtomicReference<State> currentState = new AtomicReference<>(State.CLOSED);

    // OPEN 状态下的下次重试时间戳
    protected volatile long nextRetryTimestamp;

    public AbstractCircuitBreaker(DegradeRule rule) {
        this.rule = rule;
        // timeWindow 的单位是秒，转换为毫秒
        this.recoveryTimeoutMs = rule.getTimeWindow() * 1000;
    }
}
```

### 4.4 tryPass() —— 核心通行判定

```java
@Override
public boolean tryPass(Context context) {
    // 1. CLOSED 状态：直接放行
    if (currentState.get() == State.CLOSED) {
        return true;
    }
    // 2. OPEN 状态：检查是否到了重试时间
    if (currentState.get() == State.OPEN) {
        // 判断 recovery timeout 是否已过期
        return retryTimeoutArrived() && fromOpenToHalfOpen(context);
    }
    // 3. HALF_OPEN 状态：已有 probe 请求在执行，拒绝其他请求
    return false;
}
```

**三个状态的行为总结**：
- `CLOSED`：无条件放行，但 exit 时会做统计判定
- `OPEN`：检查 `nextRetryTimestamp`，超时则尝试 CAS 转 HALF_OPEN 并放行 probe
- `HALF_OPEN`：直接返回 false，因为已经有一个 probe 请求在跑

### 4.5 retryTimeoutArrived() —— 超时判定

```java
protected boolean retryTimeoutArrived() {
    return TimeUtil.currentTimeMillis() >= nextRetryTimestamp;
}

protected void updateNextRetryTimestamp() {
    this.nextRetryTimestamp = TimeUtil.currentTimeMillis() + recoveryTimeoutMs;
}
```

### 4.6 fromOpenToHalfOpen() —— OPEN → HALF_OPEN 转换

```java
protected boolean fromOpenToHalfOpen(Context context) {
    // CAS 原子操作：OPEN → HALF_OPEN
    if (currentState.compareAndSet(State.OPEN, State.HALF_OPEN)) {
        // 转换成功，通知观察者
        notifyObservers(State.OPEN, State.HALF_OPEN, null);

        // 关键：注册 entry 的 whenTerminate 回调
        // 目的：如果 probe 请求被其他 Slot（如 FlowSlot）阻断，
        // 需要将状态从 HALF_OPEN 回退到 OPEN
        // 这是 issue #1638 的修复方案
        Entry entry = context.getCurEntry();
        entry.whenTerminate(new Entry.TerminateHandler() {
            @Override
            public void handle(Entry entry, Context context) {
                // 如果 probe 请求被阻断（非本 CB 阻断），回退状态
                if (entry.getBlockError() != null) {
                    // probe 被其他规则阻断，回退到 OPEN
                    currentState.compareAndSet(State.HALF_OPEN, State.OPEN);
                    notifyObservers(State.HALF_OPEN, State.OPEN, rule.getCount());
                }
            }
        });
        return true;
    }
    // CAS 失败，说明另一个线程已经完成了转换
    return false;
}
```

**Issue #1638 问题场景**：
1. CB 处于 OPEN 状态，超时后允许一个 probe
2. probe 请求通过了 DegradeSlot（tryPass=true）
3. 但后续被 FlowSlot（限流）阻断
4. 此时 exit 中不会调用 `onRequestComplete`（因为 blockError != null）
5. HALF_OPEN 状态会一直卡住，没有后续请求能触发状态转换

**解决方案**：通过 `whenTerminate` 回调，在 probe 请求被其他 Slot 阻断时，主动将状态从 HALF_OPEN 回退到 OPEN，等待下次超时重试。

### 4.7 fromCloseToOpen() —— CLOSED → OPEN 转换

```java
protected void fromCloseToOpen(double snapshotValue) {
    // CAS: CLOSED → OPEN
    if (currentState.compareAndSet(State.CLOSED, State.OPEN)) {
        // 设置下次重试时间
        updateNextRetryTimestamp();
        // 通知观察者
        notifyObservers(State.CLOSED, State.OPEN, snapshotValue);
    }
}
```

### 4.8 fromHalfOpenToClose() —— HALF_OPEN → CLOSED 转换

```java
protected void fromHalfOpenToClose() {
    // CAS: HALF_OPEN → CLOSED
    if (currentState.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
        // 重置统计数据（新一轮统计从零开始）
        resetStat();
        // 通知观察者
        notifyObservers(State.HALF_OPEN, State.CLOSED, null);
    }
}
```

**resetStat()** 是抽象方法，由子类实现——恢复正常后需要清空之前的统计窗口，避免残留数据影响后续判断。

### 4.9 fromHalfOpenToOpen() —— HALF_OPEN → OPEN 转换

```java
protected void fromHalfOpenToOpen(double snapshotValue) {
    // CAS: HALF_OPEN → OPEN
    if (currentState.compareAndSet(State.HALF_OPEN, State.OPEN)) {
        updateNextRetryTimestamp();
        notifyObservers(State.HALF_OPEN, State.OPEN, snapshotValue);
    }
}
```

### 4.10 状态转换汇总

```
         ┌──────────────────────────────────────────────────────────┐
         │                                                          │
         │                    统计窗口超阈值                         │
         │               fromCloseToOpen(snapshot)                  │
         │                                                          │
         ▼                                                          │
    ┌─────────┐         retryTimeout到期           ┌─────────┐     │
    │  OPEN   │ ───────── CAS成功 ────────────────▶│HALF_OPEN│     │
    └─────────┘    fromOpenToHalfOpen()            └─────────┘     │
         ▲                                           │    │         │
         │                                           │    │         │
         │            probe异常                      │    │    ┌─────────┐
         │      fromHalfOpenToOpen(snapshot)          │    │    │ CLOSED  │
         └───────────────────────────────────────────┘    │    └─────────┘
                                                          │         ▲
                                                          │         │
                                                          │  probe正常│
                                                          └─────────┘
                                                      fromHalfOpenToClose()
```

### 4.11 所有状态转换都使用 CAS 的设计意义

所有状态转换方法均使用 `AtomicReference.compareAndSet()` 保证线程安全：

1. **并发场景**：多个线程可能同时检测到超时并尝试 OPEN→HALF_OPEN，CAS 保证只有一个线程成功。
2. **无锁设计**：避免使用 synchronized 锁，在高并发场景下不会成为瓶颈。
3. **幂等性**：CAS 失败的线程直接返回 false，不会重复触发状态转换。
4. **通知一致性**：只有 CAS 成功的线程才会触发 notifyObservers，避免重复通知。

---

## 第五阶段：ResponseTimeCircuitBreaker（慢调用比例熔断器）

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/circuitbreaker/ResponseTimeCircuitBreaker.java`

ResponseTimeCircuitBreaker 基于 RT（响应时间）统计，当慢调用比例超过阈值时触发熔断。

### 5.1 核心字段

```java
public class ResponseTimeCircuitBreaker extends AbstractCircuitBreaker {

    /**
     * 慢调用 RT 阈值（毫秒）
     * 请求 RT > maxAllowedRt 即被计为「慢调用」
     */
    private final long maxAllowedRt;

    /**
     * 慢调用比例阈值 [0.0, 1.0]
     * 慢调用数 / 总请求数 > maxSlowRequestRatio 时触发熔断
     */
    private final double maxSlowRequestRatio;

    /**
     * 最小请求数
     * 窗口内总请求数不足此值时不做熔断判定
     */
    private final int minRequestAmount;

    /**
     * 滑动窗口计数器
     * LeapArray<SlowRequestCounter> 类型
     * sampleCount=1, intervalInMs=statIntervalMs
     */
    private final LeapArray<SlowRequestCounter> slidingCounter;

    public ResponseTimeCircuitBreaker(DegradeRule rule) {
        super(rule);
        this.maxAllowedRt = Math.round(rule.getCount());  // count 作为 RT 阈值
        this.maxSlowRequestRatio = rule.getSlowRatioThreshold();
        this.minRequestAmount = rule.getMinRequestAmount();
        // 单桶滑动窗口
        this.slidingCounter = new SlowRequestLeapArray(1, rule.getStatIntervalMs());
    }
}
```

### 5.2 SlowRequestCounter 计数器

```java
public class SlowRequestCounter {
    // 慢调用计数
    private LongAdder slowCount;
    // 总请求计数
    private LongAdder totalCount;

    public SlowRequestCounter() {
        this.slowCount = new LongAdder();
        this.totalCount = new LongAdder();
    }

    public long getSlowCount() {
        return slowCount.sum();
    }

    public long getTotalCount() {
        return totalCount.sum();
    }

    public SlowRequestCounter reset() {
        slowCount.reset();
        totalCount.reset();
        return this;
    }
}
```

**使用 LongAdder 而非 AtomicLong 的原因**：LongAdder 在高并发场景下通过 Cell 数组分散竞争，吞吐量远高于 AtomicLong 的单点 CAS。

### 5.3 SlowRequestLeapArray

```java
public class SlowRequestLeapArray extends LeapArray<SlowRequestCounter> {

    public SlowRequestLeapArray(int sampleCount, int intervalInMs) {
        super(sampleCount, intervalInMs);
    }

    @Override
    public SlowRequestCounter newEmptyBucket(long timeMillis) {
        return new SlowRequestCounter();
    }

    @Override
    public WindowWrap<SlowRequestCounter> resetWindowTo(
            WindowWrap<SlowRequestCounter> windowWrap, long startTime) {
        // 时间窗口过期后重置计数器
        windowWrap.resetTo(startTime);
        windowWrap.value().reset();
        return windowWrap;
    }
}
```

### 5.4 onRequestComplete() —— 请求完成时的统计与判定

```java
@Override
public void onRequestComplete(Context context) {
    // 获取当前时间窗口的桶
    SlowRequestCounter counter = slidingCounter.currentWindow().value();

    // 计算本次请求的 RT
    Entry entry = context.getCurEntry();
    long completeTime = entry.getCompleteTimestamp();
    if (completeTime <= 0) {
        completeTime = TimeUtil.currentTimeMillis();
    }
    long rt = completeTime - entry.getCreateTimestamp();

    // 判断是否为慢调用
    if (rt > maxAllowedRt) {
        counter.getSlowCount().add(1);
    }
    counter.getTotalCount().add(1);

    // 判断是否需要状态转换
    handleStateChangeWhenThresholdExceeded(rt);
}
```

### 5.5 handleStateChangeWhenThresholdExceeded() —— 状态转换判定

```java
private void handleStateChangeWhenThresholdExceeded(long rt) {
    // 当前已经是 OPEN 状态，无需重复判断
    if (currentState.get() == State.OPEN) {
        return;
    }

    // HALF_OPEN 状态：probe 请求结果判定
    if (currentState.get() == State.HALF_OPEN) {
        if (rt > maxAllowedRt) {
            // probe 请求仍然是慢调用 → 下游未恢复 → 回到 OPEN
            fromHalfOpenToOpen(1.0d);
        } else {
            // probe 请求正常 → 下游已恢复 → 关闭熔断器
            fromHalfOpenToClose();
        }
        return;
    }

    // CLOSED 状态：统计窗口汇总判定
    // 遍历所有桶（实际只有一个桶），累加 slowCount 和 totalCount
    List<SlowRequestCounter> counters = slidingCounter.values();
    long slowCount = 0;
    long totalCount = 0;
    for (SlowRequestCounter counter : counters) {
        slowCount += counter.getSlowCount();
        totalCount += counter.getTotalCount();
    }

    // 总请求数不足最小值，不做判定
    if (totalCount < minRequestAmount) {
        return;
    }

    // 计算慢调用比例
    double currentRatio = slowCount * 1.0d / totalCount;

    // 超过阈值，触发熔断 CLOSED → OPEN
    if (currentRatio > maxSlowRequestRatio) {
        transformToOpen(currentRatio);
    }

    // 特殊情况：阈值为 1.0 且 slowCount == totalCount（100% 慢调用）
    if (Double.compare(currentRatio, maxSlowRequestRatio) == 0
            && Double.compare(maxSlowRequestRatio, 1.0d) == 0) {
        transformToOpen(currentRatio);
    }
}
```

### 5.6 transformToOpen()

```java
private void transformToOpen(double triggerValue) {
    fromCloseToOpen(triggerValue);
}
```

### 5.7 resetStat() —— 恢复后清空统计

```java
@Override
protected void resetStat() {
    // HALF_OPEN → CLOSED 时调用，重置滑动窗口
    slidingCounter.currentWindow().value().reset();
}
```

### 5.8 完整执行流程示例

假设规则配置：`maxAllowedRt=200ms, maxSlowRequestRatio=0.5, minRequestAmount=5, timeWindow=10s, statIntervalMs=1000ms`

```
时刻 T=0~1000ms (CLOSED 状态):
  请求1: RT=100ms → slowCount=0, totalCount=1
  请求2: RT=300ms → slowCount=1, totalCount=2  (300>200, 慢调用)
  请求3: RT=250ms → slowCount=2, totalCount=3  (250>200, 慢调用)
  请求4: RT=50ms  → slowCount=2, totalCount=4
  请求5: RT=400ms → slowCount=3, totalCount=5  (400>200, 慢调用)
    → totalCount(5) >= minRequestAmount(5)
    → currentRatio = 3/5 = 0.6 > 0.5
    → 触发 fromCloseToOpen(0.6)
    → 状态变为 OPEN, nextRetryTimestamp = now + 10000ms

时刻 T=1000~11000ms (OPEN 状态):
  所有请求被 tryPass() 拒绝，抛出 DegradeException

时刻 T=11000ms (超时到期):
  请求N: tryPass() → retryTimeoutArrived()=true
    → CAS OPEN→HALF_OPEN 成功
    → 允许此请求作为 probe 通过

时刻 T=11000ms+ (HALF_OPEN 状态):
  probe请求完成: RT=150ms (< 200ms)
    → handleStateChangeWhenThresholdExceeded
    → rt <= maxAllowedRt → fromHalfOpenToClose()
    → 状态变为 CLOSED, 统计重置
```

---

## 第六阶段：ExceptionCircuitBreaker（异常比例/异常数熔断器）

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/circuitbreaker/ExceptionCircuitBreaker.java`

ExceptionCircuitBreaker 统一处理「异常比例」和「异常数」两种熔断策略，通过 `strategy` 字段区分。

### 6.1 核心字段

```java
public class ExceptionCircuitBreaker extends AbstractCircuitBreaker {

    /**
     * 策略类型
     * DEGRADE_GRADE_EXCEPTION_RATIO (1) 或 DEGRADE_GRADE_EXCEPTION_COUNT (2)
     */
    private final int strategy;

    /**
     * 阈值
     * - 异常比例模式: [0.0, 1.0] 之间的小数
     * - 异常数模式: 异常次数的整数
     */
    private final double threshold;

    /**
     * 最小请求数
     */
    private final int minRequestAmount;

    /**
     * 滑动窗口计数器
     * LeapArray<SimpleErrorCounter> 类型
     */
    private final LeapArray<SimpleErrorCounter> stat;

    public ExceptionCircuitBreaker(DegradeRule rule) {
        super(rule);
        this.strategy = rule.getGrade();
        this.threshold = rule.getCount();
        this.minRequestAmount = rule.getMinRequestAmount();
        // 单桶滑动窗口
        this.stat = new SimpleErrorCounterLeapArray(1, rule.getStatIntervalMs());
    }
}
```

### 6.2 SimpleErrorCounter 计数器

```java
public class SimpleErrorCounter {
    // 异常请求计数
    private LongAdder errorCount;
    // 总请求计数
    private LongAdder totalCount;

    public SimpleErrorCounter() {
        this.errorCount = new LongAdder();
        this.totalCount = new LongAdder();
    }

    public long getErrorCount() {
        return errorCount.sum();
    }

    public long getTotalCount() {
        return totalCount.sum();
    }

    public SimpleErrorCounter reset() {
        errorCount.reset();
        totalCount.reset();
        return this;
    }
}
```

### 6.3 SimpleErrorCounterLeapArray

```java
public class SimpleErrorCounterLeapArray extends LeapArray<SimpleErrorCounter> {

    public SimpleErrorCounterLeapArray(int sampleCount, int intervalInMs) {
        super(sampleCount, intervalInMs);
    }

    @Override
    public SimpleErrorCounter newEmptyBucket(long timeMillis) {
        return new SimpleErrorCounter();
    }

    @Override
    public WindowWrap<SimpleErrorCounter> resetWindowTo(
            WindowWrap<SimpleErrorCounter> windowWrap, long startTime) {
        windowWrap.resetTo(startTime);
        windowWrap.value().reset();
        return windowWrap;
    }
}
```

### 6.4 onRequestComplete() —— 异常统计与判定

```java
@Override
public void onRequestComplete(Context context) {
    Entry entry = context.getCurEntry();
    // 获取请求过程中捕获的异常
    Throwable error = entry.getError();

    // 获取当前窗口的计数器
    SimpleErrorCounter counter = stat.currentWindow().value();

    // 如果有异常，累加异常计数
    if (error != null) {
        counter.getErrorCount().add(1);
    }
    // 总请求数 +1
    counter.getTotalCount().add(1);

    // 判断是否需要状态转换
    handleStateChangeWhenThresholdExceeded(error);
}
```

### 6.5 handleStateChangeWhenThresholdExceeded() —— 状态转换判定

```java
private void handleStateChangeWhenThresholdExceeded(Throwable error) {
    // OPEN 状态无需判断
    if (currentState.get() == State.OPEN) {
        return;
    }

    // HALF_OPEN 状态：根据 probe 结果判定
    if (currentState.get() == State.HALF_OPEN) {
        if (error == null) {
            // probe 请求没有异常 → 下游已恢复 → 关闭熔断器
            fromHalfOpenToClose();
        } else {
            // probe 请求有异常 → 下游仍然异常 → 回到 OPEN
            fromHalfOpenToOpen(1.0d);
        }
        return;
    }

    // CLOSED 状态：统计窗口汇总判定
    List<SimpleErrorCounter> counters = stat.values();
    long errCount = 0;
    long totalCount = 0;
    for (SimpleErrorCounter counter : counters) {
        errCount += counter.getErrorCount();
        totalCount += counter.getTotalCount();
    }

    // 总请求数不足最小值，不做判定
    if (totalCount < minRequestAmount) {
        return;
    }

    // 根据策略计算当前值
    double curCount;
    if (strategy == RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO) {
        // 异常比例模式：curCount = 异常数 / 总数
        curCount = errCount * 1.0d / totalCount;
    } else {
        // 异常数模式：curCount = 异常数
        curCount = errCount;
    }

    // 超过阈值，触发熔断
    if (curCount > threshold) {
        transformToOpen(curCount);
    }

    // 特殊处理：异常比例模式下，阈值为 1.0 且恰好等于
    if (strategy == RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO) {
        if (Double.compare(curCount, threshold) == 0
                && Double.compare(threshold, 1.0d) == 0) {
            transformToOpen(curCount);
        }
    }
}
```

### 6.6 transformToOpen()

```java
private void transformToOpen(double triggerValue) {
    fromCloseToOpen(triggerValue);
}
```

### 6.7 resetStat()

```java
@Override
protected void resetStat() {
    stat.currentWindow().value().reset();
}
```

### 6.8 异常比例与异常数的区别

| 维度 | 异常比例 (grade=1) | 异常数 (grade=2) |
|------|-------------------|-----------------|
| threshold 含义 | 比例值 [0.0, 1.0] | 异常次数 |
| 计算公式 | errCount / totalCount | errCount |
| 触发条件 | 比例 > threshold | 异常数 > threshold |
| 典型配置 | count=0.5, minReq=10 | count=10, minReq=5 |

### 6.9 完整执行流程示例（异常比例模式）

假设规则配置：`grade=1, count=0.3, minRequestAmount=10, timeWindow=5s, statIntervalMs=1000ms`

```
时刻 T=0~1000ms (CLOSED 状态):
  10次请求中有4次异常:
    → errCount=4, totalCount=10
    → totalCount(10) >= minRequestAmount(10)
    → curCount = 4/10 = 0.4 > 0.3
    → 触发 fromCloseToOpen(0.4)
    → 状态变为 OPEN, nextRetryTimestamp = now + 5000ms

时刻 T=5000ms+ (超时到期):
  请求N: tryPass() → CAS OPEN→HALF_OPEN → probe通过

时刻 T=5000ms+ (HALF_OPEN 状态):
  case A - probe无异常:
    → error == null → fromHalfOpenToClose()
    → 状态变为 CLOSED, 统计重置

  case B - probe有异常:
    → error != null → fromHalfOpenToOpen(1.0)
    → 状态变为 OPEN, 再等 5s
```

---

## 第七阶段：状态变更观察者

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/degrade/circuitbreaker/EventObserverRegistry.java`

### 7.1 EventObserverRegistry —— 观察者注册中心

```java
public class EventObserverRegistry {

    // 单例模式
    private static final EventObserverRegistry INSTANCE = new EventObserverRegistry();

    // 观察者映射：name → observer
    private final Map<String, CircuitBreakerStateChangeObserver> observerMap = new ConcurrentHashMap<>();

    private EventObserverRegistry() {}

    public static EventObserverRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * 注册观察者
     */
    public void addStateChangeObserver(String name, CircuitBreakerStateChangeObserver observer) {
        AssertUtil.notNull(name, "name cannot be null");
        AssertUtil.notNull(observer, "observer cannot be null");
        observerMap.put(name, observer);
    }

    /**
     * 移除观察者
     */
    public boolean removeStateChangeObserver(String name) {
        return observerMap.remove(name) != null;
    }

    /**
     * 获取所有观察者
     */
    public List<CircuitBreakerStateChangeObserver> getStateChangeObservers() {
        return new ArrayList<>(observerMap.values());
    }
}
```

### 7.2 CircuitBreakerStateChangeObserver 接口

```java
public interface CircuitBreakerStateChangeObserver {

    /**
     * 状态变更回调
     * @param prevState 变更前状态
     * @param newState 变更后状态
     * @param rule 关联的降级规则
     * @param snapshotValue 触发时的快照值（如慢调用比例、异常数等）
     */
    void onStateChange(State prevState, State newState, DegradeRule rule, Double snapshotValue);
}
```

### 7.3 AbstractCircuitBreaker 中的通知机制

```java
// AbstractCircuitBreaker.java
protected void notifyObservers(State prevState, State newState, Double snapshotValue) {
    for (CircuitBreakerStateChangeObserver observer :
            EventObserverRegistry.getInstance().getStateChangeObservers()) {
        observer.onStateChange(prevState, newState, rule, snapshotValue);
    }
}
```

### 7.4 使用场景

```java
// 用户代码中注册观察者，用于日志记录、告警等
EventObserverRegistry.getInstance().addStateChangeObserver("logging",
    (prevState, newState, rule, snapshotValue) -> {
        if (newState == State.OPEN) {
            // 熔断器打开，发送告警
            logger.warn("CircuitBreaker OPEN for resource: {}, rule: {}, triggerValue: {}",
                rule.getResource(), rule, snapshotValue);
            alertService.sendAlert(rule.getResource(), snapshotValue);
        } else if (newState == State.CLOSED) {
            // 熔断器恢复
            logger.info("CircuitBreaker CLOSED for resource: {}", rule.getResource());
        }
    });
```

### 7.5 各状态转换的通知参数

| 转换 | prevState | newState | snapshotValue |
|------|-----------|----------|---------------|
| CLOSED → OPEN | CLOSED | OPEN | 触发时的统计值（比例/计数） |
| OPEN → HALF_OPEN | OPEN | HALF_OPEN | null |
| HALF_OPEN → CLOSED | HALF_OPEN | CLOSED | null |
| HALF_OPEN → OPEN | HALF_OPEN | OPEN | 1.0（probe 失败标记） |

---

## 第八阶段：滑动窗口在熔断中的应用

**文件路径**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/statistic/base/LeapArray.java`

### 8.1 熔断器中的窗口配置

两种 CircuitBreaker 实现都使用 `sampleCount=1` 的 LeapArray：

```java
// ResponseTimeCircuitBreaker 构造函数中
this.slidingCounter = new SlowRequestLeapArray(1, rule.getStatIntervalMs());

// ExceptionCircuitBreaker 构造函数中
this.stat = new SimpleErrorCounterLeapArray(1, rule.getStatIntervalMs());
```

**参数含义**：
- `sampleCount = 1`：只有一个桶
- `intervalInMs = rule.getStatIntervalMs()`：窗口总长度等于统计间隔

### 8.2 单桶窗口的行为

```java
// LeapArray 核心逻辑
public WindowWrap<T> currentWindow(long timeMillis) {
    // 计算时间对应的桶索引
    int idx = calculateTimeIdx(timeMillis);
    // 计算桶的开始时间
    long windowStart = calculateWindowStart(timeMillis);

    WindowWrap<T> old = array.get(idx);
    if (old == null) {
        // 桶不存在，创建新桶
        WindowWrap<T> window = new WindowWrap<>(windowLengthInMs, windowStart, newEmptyBucket(timeMillis));
        if (array.compareAndSet(idx, null, window)) {
            return window;
        }
    } else if (windowStart == old.windowStart()) {
        // 桶未过期，直接使用
        return old;
    } else if (windowStart > old.windowStart()) {
        // 桶已过期，重置
        // 这就是「滑动」的关键：过期后重置计数器
        if (updateLock.tryLock()) {
            try {
                return resetWindowTo(old, windowStart);
            } finally {
                updateLock.unlock();
            }
        }
    }
    // ...
}
```

### 8.3 单桶设计的含义

由于 `sampleCount=1`，整个 `statIntervalMs` 期间只有一个统计桶。这意味着：

1. **统计粒度**：以 `statIntervalMs` 为周期进行统计，不存在多桶之间的「滑动」效果。
2. **窗口切换**：当时间超过当前桶的有效期时，调用 `resetWindowTo` 重置计数器。
3. **语义**：「最近一个 statIntervalMs 时间段内的统计」。

```
时间轴：
|---- statIntervalMs ----|---- statIntervalMs ----|
|      Bucket 0          |      Bucket 0 (reset)  |
| slowCount, totalCount  | slowCount, totalCount   |
| (累加中)               | (重新从0开始)           |
```

### 8.4 为什么用单桶而不是多桶

在限流场景（FlowSlot）中，Sentinel 使用多桶 LeapArray（如 `sampleCount=2, intervalInMs=1000`）以实现平滑统计。而熔断场景使用单桶的原因：

1. **熔断判定的时间粒度较粗**：通常 `statIntervalMs >= 1000ms`，不需要亚秒级精度。
2. **简化逻辑**：单桶意味着 `values()` 只返回一个元素，遍历求和退化为直接取值。
3. **配置灵活性**：用户可以通过 `statIntervalMs` 直接控制统计窗口大小。
4. **避免边界效应**：多桶在窗口滑动时可能导致统计值突变，单桶更可预测。

### 8.5 resetWindowTo 的线程安全

```java
@Override
public WindowWrap<SlowRequestCounter> resetWindowTo(
        WindowWrap<SlowRequestCounter> windowWrap, long startTime) {
    windowWrap.resetTo(startTime);
    windowWrap.value().reset();  // LongAdder.reset() 本身是线程安全的
    return windowWrap;
}
```

**注意**：`resetWindowTo` 在 `updateLock.tryLock()` 保护下执行，只有一个线程会执行重置操作。其他线程在 `tryLock` 失败后会 `Thread.yield()` 等待。

### 8.6 与限流滑动窗口的对比

| 维度 | 限流 (FlowSlot) | 熔断 (DegradeSlot) |
|------|-----------------|-------------------|
| sampleCount | 2（默认） | 1 |
| intervalInMs | 1000（默认） | rule.statIntervalMs |
| 桶数据类型 | MetricBucket | SlowRequestCounter / SimpleErrorCounter |
| 窗口行为 | 真正的滑动（多桶交替） | 定期重置（单桶翻转） |
| 统计方式 | 跨桶累加 | 单桶直接取值 |

---

## 总结

### 核心设计模式

**1. 状态机模式 + CAS 原子转换**

CircuitBreaker 采用经典的三态状态机（CLOSED → OPEN → HALF_OPEN → CLOSED/OPEN），所有状态转换通过 `AtomicReference.compareAndSet()` 实现无锁线程安全。CAS 保证了在高并发场景下：只有一个线程能成功执行状态转换，失败的线程安全退出不会造成副作用。

**2. Probe 机制（HALF_OPEN 探针）**

OPEN 状态超时后不是直接恢复，而是进入 HALF_OPEN 状态只放行一个请求作为"探针"。这个设计避免了超时后大量请求同时涌入尚未完全恢复的下游服务。探针成功则恢复，失败则继续熔断。

**3. Observer 模式实现状态变更通知**

通过 `EventObserverRegistry` 和 `CircuitBreakerStateChangeObserver` 接口，实现了状态变更的可观测性。用户可以注册自定义观察者用于日志记录、告警通知、监控打点等。

**4. 单桶滑动窗口简化统计**

熔断器使用 `sampleCount=1` 的 LeapArray，将统计窗口简化为单桶定期翻转模型。既保留了 LeapArray 的线程安全和时间窗口管理能力，又避免了多桶统计的复杂性。

**5. 规则热更新与状态保持**

`DegradeRuleManager.getExistingSameCbOrNew()` 方法确保动态规则变更时，未变化的规则保留原有 CircuitBreaker 实例（及其状态），只有参数变化的规则才重建。避免了规则推送导致所有熔断器被错误重置。

**6. Issue #1638 的 whenTerminate 补偿**

当 probe 请求通过了 DegradeSlot 但被其他 Slot（如 FlowSlot）阻断时，通过 `Entry.whenTerminate` 回调将 HALF_OPEN 回退到 OPEN，防止状态机卡死在 HALF_OPEN。

### 状态机完整转换图

```
                    ┌───────────────────────────────────────────┐
                    │                                           │
                    │    统计窗口汇总 + 超阈值触发               │
                    │    fromCloseToOpen(snapshotValue)          │
                    │                                           │
                    ▼                                           │
              ┌──────────┐                               ┌──────────┐
              │          │     retryTimeout 到期          │          │
              │   OPEN   │─────── CAS成功 ──────────────▶│HALF_OPEN │
              │          │   fromOpenToHalfOpen()         │          │
              └──────────┘                               └──────────┘
                    ▲                                      │       │
                    │                                      │       │
                    │  probe异常/慢调用                    │       │  probe正常
                    │  fromHalfOpenToOpen(snapshot)         │       │  fromHalfOpenToClose()
                    │                                      │       │
                    └──────────────────────────────────────┘       │
                                                                   │
                                                                   ▼
                                                            ┌──────────┐
                                                            │          │
                                                            │  CLOSED  │
                                                            │          │
                                                            └──────────┘
                                                              (resetStat)
```

### 关键类一览

| 类名 | 职责 |
|------|------|
| `DegradeSlot` | SPI 处理槽入口，协调 tryPass/onRequestComplete |
| `DegradeRule` | 熔断规则数据模型 |
| `DegradeRuleManager` | 规则管理、CircuitBreaker 生命周期管理 |
| `CircuitBreaker` | 熔断器接口定义 |
| `AbstractCircuitBreaker` | 状态机骨架、CAS 转换方法 |
| `ResponseTimeCircuitBreaker` | 慢调用比例熔断实现 |
| `ExceptionCircuitBreaker` | 异常比例/异常数熔断实现 |
| `SlowRequestCounter` | 慢调用统计桶 |
| `SimpleErrorCounter` | 异常统计桶 |
| `EventObserverRegistry` | 状态变更观察者注册中心 |
| `CircuitBreakerStateChangeObserver` | 观察者接口 |
| `LeapArray` | 通用滑动窗口基础设施 |
