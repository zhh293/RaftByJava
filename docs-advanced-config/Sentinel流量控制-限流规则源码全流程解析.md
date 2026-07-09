# Sentinel 流量控制（限流规则） —— 源码全流程解析

> 基于源码项目 `/Users/zhanghonghao/Desktop/Sentinel` 逐步分析，从 `FlowSlot.entry()` 触发限流检查，到 `TrafficShapingController.canPass()` 做出最终放行/拒绝决策，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
责任链执行到 FlowSlot（order = -2000）
  |
  +-- 1. FlowSlot.entry()
  |     -> checkFlow(resourceWrapper, context, node, acquireCount, prioritized)
  |        -> ruleProvider.apply(resource.getName()) 获取该资源的所有 FlowRule
  |           （ruleProvider 是一个 Function，实际调用 FlowRuleManager.getFlowRuleMap().get(resource)）
  |
  +-- 2. FlowRuleChecker.checkFlow()
  |     -> 遍历所有 FlowRule，逐条调用 canPassCheck()
  |        -> 判断 clusterMode：
  |           +-- true  -> passClusterCheck()（集群限流，本文不展开）
  |           +-- false -> passLocalCheck()
  |
  +-- 3. FlowRuleChecker.passLocalCheck()
  |     -> selectNodeByRequesterAndStrategy(rule, context, node)
  |        -> 根据 limitApp + strategy 选出目标统计节点 selectedNode
  |     -> rule.getRater().canPass(selectedNode, acquireCount, prioritized)
  |        -> 调用具体的 TrafficShapingController 实现
  |
  +-- 4. selectNodeByRequesterAndStrategy() 节点选择逻辑：
  |     +-- limitApp 匹配 origin：
  |     |     STRATEGY_DIRECT  -> context.getOriginNode()
  |     |     STRATEGY_RELATE  -> selectReferenceNode(rule, context, node)
  |     |     STRATEGY_CHAIN   -> selectReferenceNode(rule, context, node)
  |     +-- limitApp == "default"：
  |     |     STRATEGY_DIRECT  -> node.getClusterNode()
  |     |     STRATEGY_RELATE  -> selectReferenceNode(rule, context, node)
  |     |     STRATEGY_CHAIN   -> selectReferenceNode(rule, context, node)
  |     +-- limitApp == "other"：
  |           -> 特殊处理（排除已有专属规则的 origin）
  |
  +-- 5. selectReferenceNode()：
  |     +-- STRATEGY_RELATE  -> ClusterBuilderSlot.getClusterNode(rule.getRefResource())
  |     +-- STRATEGY_CHAIN   -> 仅当 refResource == context.getName() 时返回当前 node
  |
  +-- 6. TrafficShapingController.canPass() —— 四种实现：
  |     [0] DefaultController         直接拒绝（超阈值立即 Block）
  |     [1] WarmUpController          预热/冷启动（令牌桶渐进放量）
  |     [2] ThrottlingController      匀速排队（漏桶算法）
  |     [3] WarmUpRateLimiterController 预热 + 匀速排队
  |
  +-- 7. 判定结果：
        +-- canPass 返回 true  -> 放行，继续执行下一个 Slot
        +-- canPass 返回 false -> 抛出 FlowException（extends BlockException）
```

---

## 第一阶段：FlowSlot 入口

### 1.1 FlowSlot —— 流量控制 Slot

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/FlowSlot.java`

`FlowSlot` 是 Sentinel 责任链中负责流量控制的核心 Slot，通过 SPI 机制加载，`order = -2000`，排在 `SystemSlot`（-5000）之后、`DegradeSlot`（-1000）之前。

```java
@Spi(order = Constants.ORDER_FLOW_SLOT)  // ORDER_FLOW_SLOT = -2000
public class FlowSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    private final FlowRuleChecker checker;

    // ruleProvider: 根据资源名获取对应的 FlowRule 列表
    private final Function<String, Collection<FlowRule>> ruleProvider = new Function<String, Collection<FlowRule>>() {
        @Override
        public Collection<FlowRule> apply(String resource) {
            // 从 FlowRuleManager 的内部 Map 中获取
            Map<String, List<FlowRule>> flowRules = FlowRuleManager.getFlowRuleMap();
            return flowRules.get(resource);
        }
    };

    public FlowSlot() {
        this(new FlowRuleChecker());
    }

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node,
                      int count, boolean prioritized, Object... args) throws Throwable {
        checkFlow(resourceWrapper, context, node, count, prioritized);
        // 通过限流检查后，继续执行下一个 Slot
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    void checkFlow(ResourceWrapper resource, Context context, DefaultNode node,
                   int count, boolean prioritized) throws BlockException {
        checker.checkFlow(ruleProvider, resource, context, node, count, prioritized);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        fireExit(context, resourceWrapper, count, args);
    }
}
```

关键设计点：

- `ruleProvider` 是一个 `Function<String, Collection<FlowRule>>`，将规则获取逻辑与检查逻辑解耦。FlowSlot 不直接依赖 FlowRuleManager 的静态方法，而是通过函数式接口注入，方便测试和扩展。
- `entry()` 方法中先调用 `checkFlow()` 进行限流判断，如果不通过会抛出 `FlowException`，阻止后续 Slot 执行；通过则调用 `fireEntry()` 传递到下一个 Slot。
- `exit()` 方法中 FlowSlot 没有额外逻辑，直接 `fireExit()` 传递。

---

## 第二阶段：FlowRuleChecker 规则校验

### 2.1 FlowRuleChecker.checkFlow() —— 遍历规则逐条校验

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/FlowRuleChecker.java`

```java
public class FlowRuleChecker {

    public void checkFlow(Function<String, Collection<FlowRule>> ruleProvider,
                          ResourceWrapper resource, Context context, DefaultNode node,
                          int acquireCount, boolean prioritized) throws BlockException {
        if (ruleProvider == null || resource == null) {
            return;
        }
        Collection<FlowRule> rules = ruleProvider.apply(resource.getName());
        if (rules != null) {
            for (FlowRule rule : rules) {
                if (!canPassCheck(rule, context, node, acquireCount, prioritized)) {
                    throw new FlowException(rule.getLimitApp(), rule);
                }
            }
        }
    }
}
```

逻辑非常清晰：获取当前资源的所有限流规则，逐条检查。只要有一条规则不通过，立即抛出 `FlowException`，不再检查后续规则。这意味着**规则之间是 OR 关系**——任何一条规则触发限流，请求就会被拒绝。

### 2.2 canPassCheck() —— 集群/本地分流

```java
public boolean canPassCheck(FlowRule rule, Context context, DefaultNode node,
                            int acquireCount, boolean prioritized) {
    String limitApp = rule.getLimitApp();
    if (limitApp == null) {
        return true;
    }

    if (rule.isClusterMode()) {
        return passClusterCheck(rule, context, node, acquireCount, prioritized);
    }

    return passLocalCheck(rule, context, node, acquireCount, prioritized);
}
```

`clusterMode` 为 true 时走集群限流逻辑（需要 Token Server），本文聚焦于单机限流，即 `passLocalCheck()` 分支。

### 2.3 passLocalCheck() —— 选节点 + 调控制器

```java
private static boolean passLocalCheck(FlowRule rule, Context context, DefaultNode node,
                                      int acquireCount, boolean prioritized) {
    // 第一步：根据规则的 limitApp 和 strategy 选出统计节点
    Node selectedNode = selectNodeByRequesterAndStrategy(rule, context, node);
    if (selectedNode == null) {
        return true;  // 选不到节点说明规则不适用于当前请求，直接放行
    }

    // 第二步：调用规则绑定的流量整形控制器进行判断
    return rule.getRater().canPass(selectedNode, acquireCount, prioritized);
}
```

这里有两个核心步骤：节点选择和流量判定。节点选择决定了"用哪个统计数据来判断"，控制器决定了"怎么判断"。

### 2.4 selectNodeByRequesterAndStrategy() —— 节点选择策略

这是 FlowRuleChecker 中最复杂的方法，根据 `rule.getLimitApp()`（限流针对的调用方）和 `rule.getStrategy()`（流控策略）两个维度来选择统计节点：

```java
static Node selectNodeByRequesterAndStrategy(FlowRule rule, Context context, DefaultNode node) {
    String limitApp = rule.getLimitApp();
    int strategy = rule.getStrategy();
    String origin = context.getOrigin();

    // 情况一：limitApp 匹配当前请求的 origin（针对特定调用方限流）
    if (limitApp.equals(origin) && filterOrigin(origin)) {
        if (strategy == RuleConstant.STRATEGY_DIRECT) {
            // 直接策略：使用 origin 维度的统计节点
            return context.getOriginNode();
        }
        // 关联/链路策略：使用关联资源的节点
        return selectReferenceNode(rule, context, node);
    }

    // 情况二：limitApp 是 "default"（对所有调用方生效）
    else if (RuleConstant.LIMIT_APP_DEFAULT.equals(limitApp)) {
        if (strategy == RuleConstant.STRATEGY_DIRECT) {
            // 直接策略：使用 ClusterNode（该资源的全局统计）
            return node.getClusterNode();
        }
        // 关联/链路策略
        return selectReferenceNode(rule, context, node);
    }

    // 情况三：limitApp 是 "other"（对没有专属规则的调用方生效）
    else if (RuleConstant.LIMIT_APP_OTHER.equals(limitApp)
             && FlowRuleManager.isOtherOrigin(origin, rule.getResource())) {
        if (strategy == RuleConstant.STRATEGY_DIRECT) {
            return context.getOriginNode();
        }
        return selectReferenceNode(rule, context, node);
    }

    return null;  // 规则不适用于当前请求
}
```

三种 `limitApp` 的含义：

- **具体应用名**（如 `"serviceA"`）：只对来自 serviceA 的请求生效。使用 `originNode` 统计，因为 originNode 记录的是特定来源的 QPS/线程数。
- **`"default"`**：对所有调用方生效。使用 `clusterNode` 统计，因为 clusterNode 是该资源的全局汇总数据。
- **`"other"`**：对没有被其他规则覆盖的调用方生效。通过 `FlowRuleManager.isOtherOrigin()` 判断当前 origin 是否已有专属规则。

### 2.5 selectReferenceNode() —— 关联/链路节点选择

```java
static Node selectReferenceNode(FlowRule rule, Context context, DefaultNode node) {
    String refResource = rule.getRefResource();
    int strategy = rule.getStrategy();

    if (StringUtil.isEmpty(refResource)) {
        return null;
    }

    if (strategy == RuleConstant.STRATEGY_RELATE) {
        // 关联策略：返回关联资源的 ClusterNode
        // 例如：写接口流量大时，限制读接口
        return ClusterBuilderSlot.getClusterNode(refResource);
    }

    if (strategy == RuleConstant.STRATEGY_CHAIN) {
        // 链路策略：只有当前调用链路的入口匹配 refResource 时才生效
        if (!refResource.equals(context.getName())) {
            return null;  // 入口不匹配，规则不适用
        }
        return node;  // 返回当前 DefaultNode（链路维度的统计）
    }

    return null;
}
```

两种关联策略的典型场景：

- **STRATEGY_RELATE（关联）**：当资源 A 的流量过大时，限制资源 B。例如写接口 QPS 过高时限制读接口，保护数据库。`refResource` 指向被监控的关联资源，取其 ClusterNode 的统计数据来判断。
- **STRATEGY_CHAIN（链路）**：只统计从特定入口进来的流量。例如同一个资源被多个入口调用，只想限制从入口 A 进来的流量。`refResource` 指定入口名，只有 `context.getName()` 匹配时才生效，返回当前 DefaultNode（它只统计当前链路的数据）。

---

## 第三阶段：FlowRule 规则模型

### 3.1 FlowRule 字段定义

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/FlowRule.java`

```java
public class FlowRule extends AbstractRule {

    // ========== 限流阈值类型 ==========
    /**
     * 0: 线程数限流 (FLOW_GRADE_THREAD)
     * 1: QPS 限流 (FLOW_GRADE_QPS)
     */
    private int grade = RuleConstant.FLOW_GRADE_QPS;

    /**
     * 限流阈值
     * grade=0 时表示最大并发线程数
     * grade=1 时表示每秒最大请求数
     */
    private double count;

    // ========== 流控策略 ==========
    /**
     * 0: STRATEGY_DIRECT  直接（基于自身资源统计）
     * 1: STRATEGY_RELATE  关联（基于关联资源统计）
     * 2: STRATEGY_CHAIN   链路（基于入口资源统计）
     */
    private int strategy = RuleConstant.STRATEGY_DIRECT;

    /**
     * 关联资源名 / 入口资源名
     * strategy=1 时为关联资源
     * strategy=2 时为入口资源
     */
    private String refResource;

    // ========== 流控效果 ==========
    /**
     * 0: CONTROL_BEHAVIOR_DEFAULT       直接拒绝
     * 1: CONTROL_BEHAVIOR_WARM_UP       预热/冷启动
     * 2: CONTROL_BEHAVIOR_RATE_LIMITER  匀速排队
     * 3: CONTROL_BEHAVIOR_WARM_UP_RATE_LIMITER  预热+匀速排队
     */
    private int controlBehavior = RuleConstant.CONTROL_BEHAVIOR_DEFAULT;

    private int warmUpPeriodSec = 10;       // 预热时长（秒）
    private int maxQueueingTimeMs = 500;    // 最大排队等待时间（毫秒）

    // ========== 集群模式 ==========
    private boolean clusterMode;
    private ClusterFlowConfig clusterConfig;

    // ========== 运行时绑定的控制器 ==========
    /**
     * 流量整形控制器，在规则加载时由 FlowRuleUtil.generateRater() 创建
     * 不参与序列化，是运行时对象
     */
    private TrafficShapingController controller;

    public TrafficShapingController getRater() {
        return controller;
    }

    public void setRater(TrafficShapingController controller) {
        this.controller = controller;
    }
}
```

`FlowRule` 继承自 `AbstractRule`，后者提供了 `resource`（资源名）和 `limitApp`（限流针对的调用方）两个基础字段。FlowRule 在此基础上增加了限流的核心配置。

关键设计：`controller` 字段是运行时绑定的 `TrafficShapingController` 实例，在规则加载时根据 `controlBehavior` 创建对应的实现类。这样在限流判断时直接调用 `rule.getRater().canPass()` 即可，无需再做 switch-case 分发。

---

## 第四阶段：流量整形控制器

### 4.0 TrafficShapingController 接口

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/TrafficShapingController.java`

```java
public interface TrafficShapingController {

    /**
     * 判断当前请求是否可以通过
     *
     * @param node         统计节点（提供 QPS、线程数等实时数据）
     * @param acquireCount 本次请求需要获取的令牌数
     * @param prioritized  是否为优先级请求（可以提前占用未来窗口的令牌）
     * @return true 放行，false 拒绝
     */
    boolean canPass(Node node, int acquireCount, boolean prioritized);

    /**
     * 判断当前请求是否可以通过（带参数版本）
     */
    boolean canPass(Node node, int acquireCount);
}
```

### 4.1 FlowRuleUtil.generateRater() —— 控制器工厂方法

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/FlowRuleUtil.java`

```java
private static TrafficShapingController generateRater(FlowRule rule) {
    if (rule.getGrade() == RuleConstant.FLOW_GRADE_QPS) {
        switch (rule.getControlBehavior()) {
            case RuleConstant.CONTROL_BEHAVIOR_WARM_UP:
                return new WarmUpController(rule.getCount(), rule.getWarmUpPeriodSec(),
                        ColdFactorProperty.coldFactor);

            case RuleConstant.CONTROL_BEHAVIOR_RATE_LIMITER:
                return new ThrottlingController(rule.getMaxQueueingTimeMs(), rule.getCount());

            case RuleConstant.CONTROL_BEHAVIOR_WARM_UP_RATE_LIMITER:
                return new WarmUpRateLimiterController(rule.getCount(), rule.getWarmUpPeriodSec(),
                        rule.getMaxQueueingTimeMs(), ColdFactorProperty.coldFactor);

            case RuleConstant.CONTROL_BEHAVIOR_DEFAULT:
            default:
                return new DefaultController(rule.getCount(), rule.getGrade());
        }
    } else {
        // 线程数限流模式，只使用 DefaultController
        return new DefaultController(rule.getCount(), rule.getGrade());
    }
}
```

注意：只有 QPS 模式才支持四种流控效果。线程数模式下只有"直接拒绝"一种效果，因为线程数是瞬时值，不存在"排队等待"或"预热"的语义。

---

### 4.2 DefaultController —— 直接拒绝

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/controller/DefaultController.java`

这是最简单也是最常用的控制器：当前已使用的令牌数 + 本次请求的令牌数 > 阈值时，直接拒绝。

```java
public class DefaultController implements TrafficShapingController {

    private static final int DEFAULT_AVG_USED_TOKENS = 0;

    private double count;   // 阈值
    private int grade;      // 0=线程数, 1=QPS

    public DefaultController(double count, int grade) {
        this.count = count;
        this.grade = grade;
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        // 获取当前已使用的令牌数
        int curCount = avgUsedTokens(node);

        // 核心判断：已用 + 本次请求 > 阈值 → 拒绝
        if (curCount + acquireCount > count) {
            // 优先级机制：prioritized=true 且 QPS 模式时，尝试占用未来窗口
            if (prioritized && grade == RuleConstant.FLOW_GRADE_QPS) {
                long currentTime = TimeUtil.currentTimeMillis();
                long waitInMs = node.tryOccupyNext(currentTime, acquireCount, count);
                if (waitInMs < OccupyTimeoutProperty.getOccupyTimeout()) {
                    node.addWaitingRequest(currentTime + waitInMs, acquireCount);
                    node.addOccupiedPass(acquireCount);
                    sleep(waitInMs);
                    // 抛出 PriorityWaitException 通知上层：请求会通过，但需要等待
                    throw new PriorityWaitException(waitInMs);
                }
            }
            return false;
        }
        return true;
    }

    private int avgUsedTokens(Node node) {
        if (node == null) {
            return DEFAULT_AVG_USED_TOKENS;
        }
        // 根据限流类型获取不同的统计指标
        return grade == RuleConstant.FLOW_GRADE_THREAD
                ? node.curThreadNum()    // 当前并发线程数
                : (int)(node.passQps()); // 当前通过的 QPS
    }

    private void sleep(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}
```

**核心逻辑解析**：

`avgUsedTokens()` 方法根据 `grade` 选择统计指标：线程数模式取 `node.curThreadNum()`（当前正在执行的线程数），QPS 模式取 `node.passQps()`（当前时间窗口内已通过的请求数）。

**优先级等待机制（Priority Wait）**：

当 `prioritized=true` 且当前是 QPS 模式时，即使超过阈值也不会立即拒绝，而是尝试"借用"未来时间窗口的令牌：

1. 调用 `node.tryOccupyNext()` 计算需要等待多长时间才能获得令牌
2. 如果等待时间在可接受范围内（`OccupyTimeout`），则：
   - 将请求加入等待队列 `addWaitingRequest()`
   - 记录已占用的通过数 `addOccupiedPass()`
   - 当前线程 sleep 等待
   - 抛出 `PriorityWaitException`（这不是错误，而是一种信号）
3. `StatisticSlot` 会捕获 `PriorityWaitException`，将其视为"通过但需等待"

这个机制主要用于 Sentinel 内部的优先级场景，普通用户请求的 `prioritized` 默认为 false。

---

### 4.3 WarmUpController —— 预热/冷启动

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/controller/WarmUpController.java`

WarmUpController 实现了冷启动（预热）机制：系统长时间未收到请求后，不会立即允许全量流量通过，而是逐步提升允许的 QPS，避免冷系统被突发流量打垮。算法基于 Guava 的 `SmoothWarmingUp` 令牌桶。

#### 4.3.1 核心字段与构造

```java
public class WarmUpController implements TrafficShapingController {

    protected double count;          // 阈值（稳态时的最大 QPS）
    private int coldFactor;          // 冷却因子，默认 3
    protected int warningToken;      // 警戒令牌数
    private int maxToken;            // 最大令牌数
    private double slope;            // 斜率（用于计算冷启动期间的动态阈值）

    protected AtomicLong storedTokens = new AtomicLong(0);   // 当前存储的令牌数
    protected AtomicLong lastFilledTime = new AtomicLong(0); // 上次填充令牌的时间

    public WarmUpController(double count, int warmUpPeriodInSec, int coldFactor) {
        construct(count, warmUpPeriodInSec, coldFactor);
    }

    private void construct(double count, int warmUpPeriodInSec, int coldFactor) {
        if (coldFactor <= 1) {
            throw new IllegalArgumentException("Cold factor should be larger than 1");
        }

        this.count = count;
        this.coldFactor = coldFactor;

        // 警戒令牌数 = 预热时长 * 阈值 / (冷却因子 - 1)
        // 当 storedTokens > warningToken 时，系统处于"冷"状态
        warningToken = (int)(warmUpPeriodInSec * count) / (coldFactor - 1);

        // 最大令牌数 = 警戒令牌数 + 2 * 预热时长 * 阈值 / (1 + 冷却因子)
        maxToken = warningToken + (int)(2 * warmUpPeriodInSec * count / (1.0 + coldFactor));

        // 斜率 = (冷却因子 - 1) / 阈值 / (最大令牌数 - 警戒令牌数)
        // 用于在冷启动区间线性插值计算动态阈值
        slope = (coldFactor - 1.0) / count / (maxToken - warningToken);
    }
}
```

**令牌桶模型解释**：

想象一个令牌桶，桶里的令牌数 `storedTokens` 反映系统的"冷热程度"：

- `storedTokens` 很高（接近 `maxToken`）：系统很"冷"，长时间没有请求，令牌积累了很多。此时允许的 QPS 很低。
- `storedTokens` 很低（低于 `warningToken`）：系统很"热"，一直在处理请求，令牌被消耗。此时允许全量 QPS。
- `storedTokens` 在 `warningToken` 和 `maxToken` 之间：系统正在预热，允许的 QPS 随令牌减少而线性增加。

公式推导（以默认 `coldFactor=3`，`count=10`，`warmUpPeriodSec=10` 为例）：

- `warningToken = 10 * 10 / (3 - 1) = 50`
- `maxToken = 50 + 2 * 10 * 10 / (1 + 3) = 50 + 50 = 100`
- `slope = (3 - 1) / 10 / (100 - 50) = 0.004`

#### 4.3.2 canPass() —— 预热判断逻辑

```java
@Override
public boolean canPass(Node node, int acquireCount, boolean prioritized) {
    long passQps = (long) node.passQps();
    long previousQps = (long) node.previousPassQps();
    // 第一步：根据时间流逝填充令牌，根据上一秒 QPS 消耗令牌
    syncToken(previousQps);

    long restToken = storedTokens.get();

    // 第二步：根据当前令牌数判断是否处于预热阶段
    if (restToken >= warningToken) {
        // 冷启动阶段：令牌数高于警戒线，需要限制 QPS
        long aboveToken = restToken - warningToken;
        // 动态计算当前允许的 QPS（随令牌减少而增大）
        double warningQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));
        if (passQps + acquireCount <= warningQps) {
            return true;
        }
    } else {
        // 已预热完成：令牌数低于警戒线，使用全量阈值
        if (passQps + acquireCount <= count) {
            return true;
        }
    }

    return false;
}
```

**预热阶段的 QPS 计算**：

当 `restToken >= warningToken` 时，系统处于冷启动阶段。允许的 QPS 通过以下公式计算：

```
warningQps = 1 / (aboveToken * slope + 1/count)
```

其中 `aboveToken = restToken - warningToken`。当 `aboveToken` 很大（系统很冷）时，分母很大，`warningQps` 很小；随着令牌被消耗，`aboveToken` 减小，`warningQps` 逐渐增大，直到 `restToken` 降到 `warningToken` 以下，切换到全量模式。

#### 4.3.3 syncToken() —— 令牌同步

```java
protected void syncToken(long passQps) {
    long currentTime = TimeUtil.currentTimeMillis();
    // 对齐到秒级时间窗口
    currentTime = currentTime - currentTime % 1000;
    long oldLastFillTime = lastFilledTime.get();
    if (currentTime <= oldLastFillTime) {
        return;  // 同一秒内不重复填充
    }

    long oldValue = storedTokens.get();
    // 根据时间流逝计算新的令牌数
    long newValue = coolDownTokens(currentTime, oldValue);

    if (storedTokens.compareAndSet(oldValue, newValue)) {
        // CAS 成功后，减去上一秒消耗的令牌
        long currentValue = storedTokens.addAndGet(0 - passQps);
        if (currentValue < 0) {
            storedTokens.set(0L);
        }
        lastFilledTime.set(currentTime);
    }
}

private long coolDownTokens(long currentTime, long oldValue) {
    long newValue = oldValue;
    // 如果当前令牌数低于警戒线（系统热），按 count/秒 的速率填充
    if (oldValue < warningToken) {
        newValue = (long)(oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
    }
    // 如果当前令牌数高于警戒线（系统冷），只有在 passQps 很低时才填充
    // 这确保了系统在冷状态下不会无限积累令牌
    else if (oldValue > warningToken) {
        if (passQps < (int) count / coldFactor) {
            newValue = (long)(oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
        }
    }
    return Math.min(newValue, maxToken);
}
```

`syncToken()` 的作用是在每个新的时间窗口开始时更新令牌桶状态：先根据时间流逝填充令牌（`coolDownTokens`），再减去上一秒实际消耗的令牌（`passQps`）。使用 CAS 保证并发安全。

---

### 4.4 ThrottlingController —— 匀速排队（漏桶）

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/controller/ThrottlingController.java`

ThrottlingController 实现了漏桶算法：请求以固定速率通过，多余的请求排队等待，等待超时则拒绝。适用于需要平滑处理突发流量的场景，如消息队列消费。

```java
public class ThrottlingController implements TrafficShapingController {

    // 最大排队等待时间（毫秒）
    private final int maxQueueingTimeMs;
    // 阈值（QPS）
    private final double count;
    // 统计窗口时长（毫秒），默认 1000ms
    private final int statDurationMs;

    // 上一个请求通过的时间（虚拟时间线）
    private final AtomicLong latestPassedTime = new AtomicLong(-1);

    public ThrottlingController(int maxQueueingTimeMs, double count) {
        this.maxQueueingTimeMs = maxQueueingTimeMs;
        this.count = count;
        this.statDurationMs = 1000;  // 默认 1 秒窗口
    }

    public ThrottlingController(int maxQueueingTimeMs, double count, int statDurationMs) {
        this.maxQueueingTimeMs = maxQueueingTimeMs;
        this.count = count;
        this.statDurationMs = statDurationMs;
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        if (acquireCount <= 0) {
            return true;
        }
        if (count <= 0) {
            return false;
        }

        long currentTime = TimeUtil.currentTimeMillis();

        // 计算两个请求之间应该间隔的时间（毫秒）
        // 例如 count=10, statDurationMs=1000, acquireCount=1
        // costTime = 1000 * 1 / 10 = 100ms（每 100ms 放行一个请求）
        long costTime = Math.round(1.0 * statDurationMs * acquireCount / count);

        // 计算本次请求的期望通过时间
        long expectedTime = costTime + latestPassedTime.get();

        if (expectedTime <= currentTime) {
            // 期望时间已过，可以立即通过
            // CAS 更新 latestPassedTime 为当前时间
            latestPassedTime.set(currentTime);
            return true;
        } else {
            // 需要等待
            long waitTime = costTime + latestPassedTime.get() - currentTime;
            if (waitTime > maxQueueingTimeMs) {
                // 等待时间超过最大排队时间，直接拒绝
                return false;
            }

            // CAS 更新 latestPassedTime（在虚拟时间线上前进）
            long oldTime = latestPassedTime.addAndGet(costTime);
            long waitTimeRecalc = oldTime - currentTime;

            if (waitTimeRecalc > maxQueueingTimeMs) {
                // 并发场景下重新计算后超时，回退并拒绝
                latestPassedTime.addAndGet(-costTime);
                return false;
            }

            // 等待对应时间后通过
            if (waitTimeRecalc > 0) {
                sleep(waitTimeRecalc);
            }
            return true;
        }
    }

    private void sleep(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}
```

**漏桶算法核心思想**：

漏桶以固定速率"漏水"（放行请求）。`latestPassedTime` 维护了一条虚拟时间线，每个请求在这条时间线上占据 `costTime` 的时间段。

举例说明（`count=10`，即每秒 10 个请求，`maxQueueingTimeMs=500`）：

- `costTime = 1000 * 1 / 10 = 100ms`（每个请求间隔 100ms）
- 假设当前时间 1000ms，`latestPassedTime = 900ms`
- `expectedTime = 100 + 900 = 1000ms`，等于当前时间，立即通过
- 下一个请求到来时 `latestPassedTime = 1000ms`
- `expectedTime = 100 + 1000 = 1100ms`，需要等待 100ms
- 如果突然来了 10 个并发请求，它们会依次排队：等 100ms、200ms、300ms...
- 第 6 个请求需要等 600ms > 500ms（maxQueueingTimeMs），被拒绝

**并发安全**：使用 `AtomicLong.addAndGet()` 进行 CAS 操作。如果 CAS 后发现等待时间超限，通过 `addAndGet(-costTime)` 回退，保证时间线的正确性。

---

### 4.5 WarmUpRateLimiterController —— 预热 + 匀速排队

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/controller/WarmUpRateLimiterController.java`

这是 WarmUpController 和 ThrottlingController 的组合体：在预热阶段使用动态计算的较低 QPS 作为漏桶速率，预热完成后使用全量 QPS 作为漏桶速率。

```java
public class WarmUpRateLimiterController extends WarmUpController {

    private final int timeoutInMs;       // 最大排队等待时间
    private final AtomicLong latestPassedTime = new AtomicLong(-1);

    public WarmUpRateLimiterController(double count, int warmUpPeriodSec,
                                       int timeOutMs, int coldFactor) {
        super(count, warmUpPeriodSec, coldFactor);
        this.timeoutInMs = timeOutMs;
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        long previousQps = (long) node.previousPassQps();
        syncToken(previousQps);

        long currentTime = TimeUtil.currentTimeMillis();
        long restToken = storedTokens.get();
        long costTime = 0;

        // 根据当前令牌数动态计算请求间隔
        if (restToken >= warningToken) {
            // 预热阶段：使用较大的间隔（较低的 QPS）
            long aboveToken = restToken - warningToken;
            double warningQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));
            costTime = Math.round(1.0 * 1000 * acquireCount / warningQps);
        } else {
            // 已预热：使用正常间隔
            costTime = Math.round(1.0 * 1000 * acquireCount / count);
        }

        // 以下逻辑与 ThrottlingController 相同：漏桶排队
        long expectedTime = costTime + latestPassedTime.get();

        if (expectedTime <= currentTime) {
            latestPassedTime.set(currentTime);
            return true;
        }

        long waitTime = costTime + latestPassedTime.get() - currentTime;
        if (waitTime > timeoutInMs) {
            return false;
        }

        long oldTime = latestPassedTime.addAndGet(costTime);
        waitTime = oldTime - currentTime;

        if (waitTime > timeoutInMs) {
            latestPassedTime.addAndGet(-costTime);
            return false;
        }

        if (waitTime > 0) {
            sleep(waitTime);
        }
        return true;
    }

    private void sleep(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}
```

**设计精妙之处**：

通过继承 `WarmUpController`，复用了令牌桶的预热逻辑（`syncToken()`、`storedTokens`、`warningToken`、`slope` 等）。在此基础上，将 WarmUpController 中"直接拒绝"的判断替换为"漏桶排队"的判断。`costTime` 不再是固定值，而是根据预热状态动态计算：冷状态下 `costTime` 大（请求间隔长），热状态下 `costTime` 小（请求间隔短）。

---

## 第五阶段：规则管理与加载

### 5.1 FlowRuleManager —— 规则管理器

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/FlowRuleManager.java`

FlowRuleManager 是限流规则的管理中心，负责规则的存储、更新和查询。采用观察者模式，支持动态数据源推送规则更新。

```java
public class FlowRuleManager {

    // 规则存储：资源名 -> 规则列表
    private static volatile Map<String, List<FlowRule>> flowRules = new HashMap<>();

    // 属性监听器（观察者）
    private static final FlowPropertyListener LISTENER = new FlowPropertyListener();

    // 动态属性（被观察者），默认实现是内存存储
    private static SentinelProperty<List<FlowRule>> currentProperty
            = new DynamicSentinelProperty<>();

    static {
        // 静态初始化时注册监听器
        currentProperty.addListener(LISTENER);
    }

    /**
     * 加载规则（最常用的 API）
     * 内部通过 currentProperty.updateValue() 触发监听器
     */
    public static void loadRules(List<FlowRule> rules) {
        currentProperty.updateValue(rules);
    }

    /**
     * 获取规则 Map（供 FlowSlot 的 ruleProvider 调用）
     */
    public static Map<String, List<FlowRule>> getFlowRuleMap() {
        return flowRules;
    }

    /**
     * 注册外部数据源
     * 用于接入 Nacos、Apollo、ZooKeeper 等配置中心
     */
    public static void register2Property(SentinelProperty<List<FlowRule>> property) {
        synchronized (LISTENER) {
            currentProperty.removeListener(LISTENER);
            property.addListener(LISTENER);
            currentProperty = property;
        }
    }

    /**
     * 判断某个 origin 是否属于 "other" 类别
     * 即该 origin 没有被任何规则的 limitApp 直接指定
     */
    public static boolean isOtherOrigin(String origin, String resourceName) {
        if (StringUtil.isEmpty(origin)) {
            return false;
        }
        List<FlowRule> rules = flowRules.get(resourceName);
        if (rules != null) {
            for (FlowRule rule : rules) {
                if (origin.equals(rule.getLimitApp())) {
                    return false;  // 有专属规则，不属于 "other"
                }
            }
        }
        return true;
    }
}
```

### 5.2 FlowPropertyListener —— 规则变更监听器

```java
private static final class FlowPropertyListener implements PropertyListener<List<FlowRule>> {

    @Override
    public synchronized void configUpdate(List<FlowRule> value) {
        // 将规则列表转换为 Map，并为每条规则生成对应的 TrafficShapingController
        Map<String, List<FlowRule>> rules = FlowRuleUtil.buildFlowRuleMap(value);
        flowRules = rules;
    }

    @Override
    public synchronized void configLoad(List<FlowRule> conf) {
        Map<String, List<FlowRule>> rules = FlowRuleUtil.buildFlowRuleMap(conf);
        flowRules = rules;
    }
}
```

### 5.3 FlowRuleUtil.buildFlowRuleMap() —— 规则构建

```java
public static Map<String, List<FlowRule>> buildFlowRuleMap(List<FlowRule> list) {
    return buildFlowRuleMap(list, null, true);
}

public static Map<String, List<FlowRule>> buildFlowRuleMap(List<FlowRule> list,
        Function<List<FlowRule>, List<FlowRule>> filter, boolean shouldSort) {
    Map<String, List<FlowRule>> newRuleMap = new ConcurrentHashMap<>();

    if (list == null || list.isEmpty()) {
        return newRuleMap;
    }

    for (FlowRule rule : list) {
        if (!isValidRule(rule)) {
            // 跳过无效规则
            continue;
        }
        // 规则合法性校正
        if (StringUtil.isBlank(rule.getLimitApp())) {
            rule.setLimitApp(RuleConstant.LIMIT_APP_DEFAULT);
        }

        // 为每条规则生成流量整形控制器
        TrafficShapingController rater = generateRater(rule);
        rule.setRater(rater);

        // 按资源名分组
        String identity = rule.getResource();
        List<FlowRule> ruleM = newRuleMap.get(identity);
        if (ruleM == null) {
            ruleM = new ArrayList<>();
            newRuleMap.put(identity, ruleM);
        }
        ruleM.add(rule);
    }

    // 可选：对规则排序（确保执行顺序确定性）
    if (shouldSort) {
        // ... 排序逻辑
    }

    return newRuleMap;
}
```

**规则加载的完整链路**：

```
用户调用 FlowRuleManager.loadRules(rules)
  -> currentProperty.updateValue(rules)
     -> DynamicSentinelProperty.updateValue()
        -> 通知所有 PropertyListener
           -> FlowPropertyListener.configUpdate(rules)
              -> FlowRuleUtil.buildFlowRuleMap(rules)
                 -> 遍历每条规则：
                    -> isValidRule() 校验
                    -> generateRater() 创建 TrafficShapingController
                    -> rule.setRater(controller)
                    -> 按 resource 分组放入 Map
              -> flowRules = newRuleMap（volatile 写，对所有线程可见）
```

**外部数据源集成**：

通过 `register2Property()` 可以将规则来源切换为外部配置中心。例如接入 Nacos：

```java
// 创建 Nacos 数据源
ReadableDataSource<String, List<FlowRule>> flowRuleDataSource = new NacosDataSource<>(
    remoteAddress, groupId, dataId,
    source -> JSON.parseObject(source, new TypeReference<List<FlowRule>>() {})
);
// 注册到 FlowRuleManager
FlowRuleManager.register2Property(flowRuleDataSource.getProperty());
```

此后 Nacos 配置变更会自动触发 `FlowPropertyListener.configUpdate()`，实现规则热更新。

---

## 第六阶段：异常体系

### 6.1 FlowException —— 限流异常

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/FlowException.java`

```java
public class FlowException extends BlockException {

    public FlowException(String ruleLimitApp) {
        super(ruleLimitApp);
    }

    public FlowException(String ruleLimitApp, FlowRule rule) {
        super(ruleLimitApp, rule);
    }

    public FlowException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlowException(String ruleLimitApp, String message) {
        super(ruleLimitApp, message);
    }

    /**
     * 性能优化：不填充堆栈信息
     * BlockException 是高频异常（每次限流都会抛出），
     * 填充堆栈的开销很大且无实际意义（限流不是 bug）
     */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
```

**性能优化设计**：

`fillInStackTrace()` 返回 `this` 而不是调用 `super.fillInStackTrace()`。在 Java 中，创建异常对象时默认会调用 `fillInStackTrace()` 收集当前线程的完整调用栈，这是一个相对昂贵的操作。对于 Sentinel 这种高频抛出异常的场景（每次限流都会抛出 FlowException），省略堆栈填充可以显著降低性能开销。

### 6.2 BlockException —— 阻断异常基类

```java
public abstract class BlockException extends Exception {

    // 触发阻断的规则
    private AbstractRule rule;
    // 规则中的 limitApp
    private String ruleLimitApp;

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public AbstractRule getRule() {
        return rule;
    }

    /**
     * 判断一个异常是否是 BlockException（包括被包装的情况）
     */
    public static boolean isBlockException(Throwable t) {
        if (t == null) {
            return false;
        }
        int counter = 0;
        Throwable cause = t;
        while (cause != null && counter++ < 50) {
            if (cause instanceof BlockException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
```

### 6.3 PriorityWaitException —— 优先级等待信号

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/block/flow/PriorityWaitException.java`

```java
public class PriorityWaitException extends RuntimeException {

    private final long waitInMs;

    public PriorityWaitException(long waitInMs) {
        this.waitInMs = waitInMs;
    }

    public long getWaitInMs() {
        return waitInMs;
    }

    /**
     * 同样不填充堆栈（性能优化）
     */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
```

`PriorityWaitException` 不是真正的"异常"，而是一种控制流信号。当 `DefaultController` 中优先级请求成功占用未来窗口时抛出，`StatisticSlot` 会捕获它并正常记录通过指标（而不是记录为 Block）。这是一种利用异常机制实现控制流跳转的设计模式。

---

## 总结

### 设计亮点

**策略模式（Strategy Pattern）**：`TrafficShapingController` 接口定义了统一的 `canPass()` 方法，四种控制器（DefaultController、WarmUpController、ThrottlingController、WarmUpRateLimiterController）各自实现不同的流控效果。规则加载时通过工厂方法 `generateRater()` 创建对应实例，运行时通过多态调用，完全消除了 if-else/switch-case 分支。

**观察者模式（Observer Pattern）**：`FlowRuleManager` 通过 `SentinelProperty` + `PropertyListener` 实现规则的动态更新。外部数据源（Nacos、Apollo、ZooKeeper）只需实现 `ReadableDataSource` 接口并注册到 FlowRuleManager，即可实现规则热推送，无需修改任何限流逻辑代码。

**函数式解耦**：`FlowSlot` 通过 `Function<String, Collection<FlowRule>>` 获取规则，而不是直接调用 `FlowRuleManager` 的静态方法。这使得 FlowSlot 可以在单元测试中注入 mock 的规则提供者。

**优先级等待机制**：`DefaultController` 中的 `prioritized` 参数支持高优先级请求"借用"未来时间窗口的令牌。这为 Sentinel 内部的一些特殊场景（如系统规则的 BBR 算法）提供了灵活性。

**性能优化**：`FlowException` 和 `PriorityWaitException` 都重写了 `fillInStackTrace()` 返回 `this`，避免了高频异常创建时的堆栈收集开销。在高并发限流场景下，这个优化的收益非常可观。

**CAS 无锁设计**：`WarmUpController` 的 `storedTokens`/`lastFilledTime` 和 `ThrottlingController` 的 `latestPassedTime` 都使用 `AtomicLong` + CAS 操作，避免了 synchronized 锁竞争，在高并发场景下保持良好的性能。

### 四种流控效果对比

| 控制器 | 算法模型 | 适用场景 | 超阈值行为 |
|--------|----------|----------|------------|
| DefaultController | 计数器 | 通用场景，快速失败 | 立即拒绝（或优先级等待） |
| WarmUpController | 令牌桶（Guava SmoothWarmingUp） | 系统冷启动保护 | 立即拒绝 |
| ThrottlingController | 漏桶 | 削峰填谷，平滑流量 | 排队等待或超时拒绝 |
| WarmUpRateLimiterController | 令牌桶 + 漏桶 | 冷启动 + 平滑流量 | 排队等待或超时拒绝 |

### 完整数据流

```
FlowRuleManager.loadRules(rules)
  -> buildFlowRuleMap() -> generateRater() -> rule.setRater(controller)
  -> flowRules = newMap (volatile)

请求到来 -> SlotChain 执行到 FlowSlot
  -> FlowSlot.entry()
     -> checker.checkFlow(ruleProvider, resource, context, node, count, prioritized)
        -> ruleProvider.apply(resource) 获取规则列表
        -> 遍历规则: canPassCheck(rule, context, node, count, prioritized)
           -> passLocalCheck()
              -> selectNodeByRequesterAndStrategy() 选统计节点
              -> rule.getRater().canPass(selectedNode, count, prioritized)
                 -> [具体控制器的限流算法]
                    -> true: 放行
                    -> false: throw FlowException
```
