# Sentinel 核心入口 —— 责任链执行全流程源码解析

> 基于源码项目 `/Users/zhanghonghao/Desktop/Sentinel` 逐步分析，从 `SphU.entry()` 调用到 SlotChain 每个 Slot 逐一执行，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
用户代码: SphU.entry("myResource")
  |
  +-- 1. SphU.entry() 静态方法
  |     -> 委托给 Env.sph.entry()
  |        （Env.sph 是 CtSph 单例，类加载时通过 static 块触发 InitExecutor.doInit()）
  |
  +-- 2. CtSph.entryWithPriority()
  |     -> 2.1 从 ThreadLocal 获取 Context（没有则创建默认 Context）
  |     -> 2.2 lookProcessChain(resourceWrapper)
  |              -> chainMap.get() 命中缓存直接返回
  |              -> 未命中：SlotChainProvider.newSlotChain() 构建链
  |                   -> DefaultSlotChainBuilder.build()
  |                        -> SPI 加载所有 ProcessorSlot 并按 order 排序
  |                        -> chain.addLast() 逐个挂载
  |     -> 2.3 new CtEntry(resourceWrapper, chain, context)
  |              -> setUpEntryFor(context): 维护 parent/child 调用栈
  |     -> 2.4 chain.entry(context, resourceWrapper, null, count, prioritized, args)
  |
  +-- 3. DefaultProcessorSlotChain.entry()
  |     -> first(哨兵节点).transformEntry()
  |        -> first.entry() -> first.fireEntry()
  |           -> 传递到第一个真实 Slot
  |
  +-- 4. 责任链逐个执行（通过 fireEntry 传递到下一个 Slot）：
  |     [1] NodeSelectorSlot   (order=-10000)  构建调用树
  |     [2] ClusterBuilderSlot (order=-9000)   分配 ClusterNode
  |     [3] LogSlot            (order=-8000)   日志记录
  |     [4] StatisticSlot      (order=-7000)   统计（先 fireEntry 再记录）
  |     [5] AuthoritySlot      (order=-6000)   黑白名单
  |     [6] SystemSlot         (order=-5000)   系统保护
  |     [7] FlowSlot           (order=-2000)   流量控制
  |     [8] DefaultCircuitBreakerSlot (order=-1500)
  |     [9] DegradeSlot        (order=-1000)   熔断降级
  |
  +-- 5. 所有 Slot 通过后，回到 StatisticSlot 记录 pass 指标
  |     -> node.increaseThreadNum()
  |     -> node.addPassRequest(count)
  |
  +-- 6. entry 返回给用户代码
  |
  +-- 7. 用户业务执行完毕后调用 entry.exit()
        -> CtEntry.exitForContext()
           -> chain.exit(context, resourceWrapper, count, args)
              -> 所有 Slot 的 exit() 逐一执行（反向）
              -> StatisticSlot.exit() 记录 RT、success、decreaseThreadNum
           -> 恢复调用栈 context.setCurEntry(parent)
```

---

## 第一阶段：入口门面与环境初始化

### 1.1 SphU —— 静态入口门面

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/SphU.java`

`SphU` 是用户唯一需要直接调用的类，提供了多个重载的 `entry()` 静态方法。所有方法都委托给 `Env.sph`：

```java
public class SphU {
    private static final Object[] OBJECTS0 = new Object[0];

    public static Entry entry(String name) throws BlockException {
        return Env.sph.entry(name, EntryType.OUT, 1, OBJECTS0);
    }

    public static Entry entry(String name, EntryType trafficType) throws BlockException {
        return Env.sph.entry(name, trafficType, 1, OBJECTS0);
    }

    public static Entry entry(String name, EntryType trafficType, int batchCount) throws BlockException {
        return Env.sph.entry(name, trafficType, batchCount, OBJECTS0);
    }

    public static Entry entry(String name, EntryType trafficType, int batchCount, Object... args)
        throws BlockException {
        return Env.sph.entry(name, trafficType, batchCount, args);
    }

    public static Entry entry(String name, int resourceType, EntryType trafficType) throws BlockException {
        return Env.sph.entryWithType(name, resourceType, trafficType, 1, OBJECTS0);
    }
}
```

参数说明：
- `name`：资源名，是 Sentinel 的最核心概念，一个资源对应一条 ProcessorSlotChain
- `trafficType`：`EntryType.IN`（入站，如接口被调用）或 `EntryType.OUT`（出站，如调用下游）
- `batchCount`：本次请求消耗的令牌数，默认 1
- `args`：附加参数，用于热点参数限流

---

### 1.2 Env —— 全局环境持有者

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/Env.java`

```java
public class Env {
    public static final Sph sph = new CtSph();

    static {
        InitExecutor.doInit();
    }
}
```

`Env` 的 static 块在类加载时触发 `InitExecutor.doInit()`，通过 SPI 加载所有 `InitFunc` 实现类并按 order 排序执行（比如 `MetricCallbackInit`、`HeartbeatSenderInit` 等）。`sph` 字段就是 `CtSph` 单例。

---

### 1.3 Constants —— 全局常量与 ROOT 节点

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/Constants.java`

```java
public final class Constants {
    public final static int MAX_CONTEXT_NAME_SIZE = 2000;     // Context 名称数量上限
    public final static int MAX_SLOT_CHAIN_SIZE = 6000;       // 资源（SlotChain）数量上限
    public final static String CONTEXT_DEFAULT_NAME = "sentinel_default_context";

    // 全局根节点 —— 所有 EntranceNode 的父节点
    public final static DefaultNode ROOT = new EntranceNode(
        new StringResourceWrapper("machine-root", EntryType.IN),
        new ClusterNode("machine-root", ResourceTypeConstants.COMMON));

    // 全局入站流量统计节点 —— SystemSlot 检查时使用
    public final static ClusterNode ENTRY_NODE = new ClusterNode(
        "__total_inbound_traffic__", ResourceTypeConstants.COMMON);

    public static volatile boolean ON = true;  // 全局开关

    // 各 Slot 的排序常量
    public static final int ORDER_NODE_SELECTOR_SLOT = -10000;
    public static final int ORDER_CLUSTER_BUILDER_SLOT = -9000;
    public static final int ORDER_LOG_SLOT = -8000;
    public static final int ORDER_STATISTIC_SLOT = -7000;
    public static final int ORDER_AUTHORITY_SLOT = -6000;
    public static final int ORDER_SYSTEM_SLOT = -5000;
    public static final int ORDER_FLOW_SLOT = -2000;
    public static final int ORDER_DEFAULT_CIRCUIT_BREAKER_SLOT = -1500;
    public static final int ORDER_DEGRADE_SLOT = -1000;
}
```

节点层次结构预览：
```
ROOT (EntranceNode, "machine-root")
  ├── EntranceNode ("sentinel_default_context")
  │     ├── DefaultNode (resourceA) ──→ ClusterNode (resourceA, 全局)
  │     └── DefaultNode (resourceB) ──→ ClusterNode (resourceB, 全局)
  └── EntranceNode ("my-context")
        └── DefaultNode (resourceA) ──→ ClusterNode (resourceA, 同上复用)
```

---

## 第二阶段：Context 创建与管理

### 2.1 ContextUtil —— ThreadLocal 管理 Context

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/context/ContextUtil.java`

Context 是当前调用链的执行上下文，通过 ThreadLocal 管理，每个线程一个。

```java
public class ContextUtil {
    private static ThreadLocal<Context> contextHolder = new ThreadLocal<>();
    private static volatile Map<String, DefaultNode> contextNameNodeMap = new HashMap<>();
    private static final ReentrantLock LOCK = new ReentrantLock();
    private static final Context NULL_CONTEXT = new NullContext();

    static {
        // 启动时初始化默认 Context 的 EntranceNode
        initDefaultContext();
    }

    private static void initDefaultContext() {
        String defaultContextName = Constants.CONTEXT_DEFAULT_NAME;
        EntranceNode node = new EntranceNode(
            new StringResourceWrapper(defaultContextName, EntryType.IN), null);
        Constants.ROOT.addChild(node);              // 挂到 ROOT 下面
        contextNameNodeMap.put(defaultContextName, node);
    }
```

**`trueEnter()` 方法 —— 创建或获取 Context**：

```java
    protected static Context trueEnter(String name, String origin) {
        Context context = contextHolder.get();
        if (context == null) {
            Map<String, DefaultNode> localCacheNameMap = contextNameNodeMap;
            DefaultNode node = localCacheNameMap.get(name);
            if (node == null) {
                if (localCacheNameMap.size() > Constants.MAX_CONTEXT_NAME_SIZE) {
                    // Context 名称数量超限，返回 NullContext（后续不做任何规则检查）
                    setNullContext();
                    return NULL_CONTEXT;
                } else {
                    LOCK.lock();
                    try {
                        node = contextNameNodeMap.get(name);
                        if (node == null) {
                            if (contextNameNodeMap.size() > Constants.MAX_CONTEXT_NAME_SIZE) {
                                setNullContext();
                                return NULL_CONTEXT;
                            } else {
                                // 创建新的 EntranceNode 并挂到 ROOT 下
                                node = new EntranceNode(
                                    new StringResourceWrapper(name, EntryType.IN), null);
                                Constants.ROOT.addChild(node);
                                // copy-on-write 更新 map
                                Map<String, DefaultNode> newMap = new HashMap<>(contextNameNodeMap.size() + 1);
                                newMap.putAll(contextNameNodeMap);
                                newMap.put(name, node);
                                contextNameNodeMap = newMap;
                            }
                        }
                    } finally {
                        LOCK.unlock();
                    }
                }
            }
            // 用 EntranceNode 构建 Context，设置 origin，放入 ThreadLocal
            context = new Context(node, name);
            context.setOrigin(origin);
            contextHolder.set(context);
        }
        return context;
    }
```

关键设计：
- 每个 Context 名称对应一个共享的 `EntranceNode`（挂在 ROOT 下面）
- Context 数量有硬上限 2000，超限后返回 `NullContext`，不做任何规则检查（降级保护自身）
- `contextNameNodeMap` 使用 copy-on-write 更新，读操作无锁

---

### 2.2 Context —— 调用上下文

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/context/Context.java`

```java
public class Context {
    private final String name;             // Context 名称（如 "sentinel_default_context"）
    private DefaultNode entranceNode;      // 入口节点
    private Entry curEntry;                // 当前 Entry（调用栈栈顶）
    private String origin = "";            // 调用来源标识（用于 Authority 黑白名单 + 关联限流）
    private final boolean async;           // 是否异步模式
}
```

---

## 第三阶段：核心执行引擎 CtSph

### 3.1 CtSph.entryWithPriority() —— 最核心的方法

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/CtSph.java`

这是整个 Sentinel 的"心脏"方法，所有入口最终都走到这里：

```java
private Entry entryWithPriority(ResourceWrapper resourceWrapper, int count, boolean prioritized, Object... args)
    throws BlockException {
    // 1. 从 ThreadLocal 获取 Context
    Context context = ContextUtil.getContext();

    // 1.1 如果是 NullContext（Context 数量超限），直接放行不检查
    if (context instanceof NullContext) {
        return new CtEntry(resourceWrapper, null, context);
    }

    // 1.2 如果 ThreadLocal 里没有 Context，创建默认的
    if (context == null) {
        context = InternalContextUtil.internalEnter(Constants.CONTEXT_DEFAULT_NAME);
    }

    // 2. 全局开关检查
    if (!Constants.ON) {
        return new CtEntry(resourceWrapper, null, context);
    }

    // 3. 获取该资源对应的 ProcessorSlotChain（核心缓存逻辑）
    ProcessorSlot<Object> chain = lookProcessChain(resourceWrapper);

    // 3.1 chain 为 null 表示资源数量超过 MAX_SLOT_CHAIN_SIZE（6000），放行不检查
    if (chain == null) {
        return new CtEntry(resourceWrapper, null, context);
    }

    // 4. 创建 CtEntry 并建立调用栈
    Entry e = new CtEntry(resourceWrapper, chain, context, count, args);

    try {
        // 5. ★ 执行 SlotChain —— 这里是所有规则检查的入口
        chain.entry(context, resourceWrapper, null, count, prioritized, args);
    } catch (BlockException e1) {
        // 6. 被任何 Slot 拒绝时，先退出 Entry 再抛异常
        e.exit(count, args);
        throw e1;
    } catch (Throwable e1) {
        RecordLog.info("Sentinel unexpected exception", e1);
    }
    return e;
}
```

---

### 3.2 lookProcessChain() —— SlotChain 缓存与创建

```java
ProcessorSlot<Object> lookProcessChain(ResourceWrapper resourceWrapper) {
    ProcessorSlotChain chain = chainMap.get(resourceWrapper);
    if (chain == null) {
        synchronized (LOCK) {
            chain = chainMap.get(resourceWrapper);
            if (chain == null) {
                // 资源数量保护
                if (chainMap.size() >= Constants.MAX_SLOT_CHAIN_SIZE) {
                    return null;
                }
                // ★ 通过 SPI 构建新的 SlotChain
                chain = SlotChainProvider.newSlotChain();
                // copy-on-write 更新 chainMap
                Map<ResourceWrapper, ProcessorSlotChain> newMap = new HashMap<>(chainMap.size() + 1);
                newMap.putAll(chainMap);
                newMap.put(resourceWrapper, chain);
                chainMap = newMap;
            }
        }
    }
    return chain;
}
```

关键设计：
- `chainMap` 是 `volatile` 的 `Map<ResourceWrapper, ProcessorSlotChain>`，读操作无锁
- `ResourceWrapper` 的 `equals/hashCode` 只基于 `name`，所以相同资源名共享同一条链
- 创建新链时使用 synchronized + 双重检查锁
- 资源数上限 6000，超限后返回 null，不做检查（自我保护）

---

## 第四阶段：SlotChain 的构建

### 4.1 SlotChainProvider —— SPI 加载链构建器

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slotchain/SlotChainProvider.java`

```java
public final class SlotChainProvider {
    private static volatile SlotChainBuilder slotChainBuilder = null;

    public static ProcessorSlotChain newSlotChain() {
        if (slotChainBuilder != null) {
            return slotChainBuilder.build();
        }
        // 通过 SPI 加载 SlotChainBuilder（可自定义替换）
        slotChainBuilder = SpiLoader.of(SlotChainBuilder.class).loadFirstInstanceOrDefault();
        if (slotChainBuilder == null) {
            slotChainBuilder = new DefaultSlotChainBuilder();
        }
        return slotChainBuilder.build();
    }
}
```

---

### 4.2 DefaultSlotChainBuilder —— 默认链构建器

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/DefaultSlotChainBuilder.java`

```java
@Spi(isDefault = true)
public class DefaultSlotChainBuilder implements SlotChainBuilder {

    @Override
    public ProcessorSlotChain build() {
        ProcessorSlotChain chain = new DefaultProcessorSlotChain();

        // 通过 SPI 加载所有 ProcessorSlot 实现，并按 @Spi(order=...) 排序
        List<ProcessorSlot> sortedSlotList = SpiLoader.of(ProcessorSlot.class).loadInstanceListSorted();
        for (ProcessorSlot slot : sortedSlotList) {
            if (!(slot instanceof AbstractLinkedProcessorSlot)) {
                RecordLog.warn("...");
                continue;
            }
            chain.addLast((AbstractLinkedProcessorSlot<?>) slot);
        }
        return chain;
    }
}
```

**SPI 配置文件位置**: `sentinel-core/src/main/resources/META-INF/services/com.alibaba.csp.sentinel.slotchain.ProcessorSlot`

```
com.alibaba.csp.sentinel.slots.nodeselector.NodeSelectorSlot
com.alibaba.csp.sentinel.slots.clusterbuilder.ClusterBuilderSlot
com.alibaba.csp.sentinel.slots.logger.LogSlot
com.alibaba.csp.sentinel.slots.statistic.StatisticSlot
com.alibaba.csp.sentinel.slots.block.authority.AuthoritySlot
com.alibaba.csp.sentinel.slots.system.SystemSlot
com.alibaba.csp.sentinel.slots.block.flow.FlowSlot
com.alibaba.csp.sentinel.slots.block.degrade.DegradeSlot
com.alibaba.csp.sentinel.slots.block.degrade.DefaultCircuitBreakerSlot
```

排序由各 Slot 类上的 `@Spi(order=...)` 注解决定，值越小越先执行。

---

### 4.3 DefaultProcessorSlotChain —— 链的数据结构

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slotchain/DefaultProcessorSlotChain.java`

链采用单链表结构，`first` 是哨兵头节点，`end` 指向尾节点：

```java
public class DefaultProcessorSlotChain extends ProcessorSlotChain {

    // 哨兵头节点：自己不做任何事，只是调用 fireEntry 传到下一个节点
    AbstractLinkedProcessorSlot<?> first = new AbstractLinkedProcessorSlot<Object>() {
        @Override
        public void entry(Context context, ResourceWrapper resourceWrapper, Object t, int count,
                          boolean prioritized, Object... args) throws Throwable {
            super.fireEntry(context, resourceWrapper, t, count, prioritized, args);
        }

        @Override
        public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
            super.fireExit(context, resourceWrapper, count, args);
        }
    };
    AbstractLinkedProcessorSlot<?> end = first;

    @Override
    public void addLast(AbstractLinkedProcessorSlot<?> protocolProcessor) {
        end.setNext(protocolProcessor);
        end = protocolProcessor;
    }

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, Object t, int count,
                      boolean prioritized, Object... args) throws Throwable {
        // 入口：从 first 开始执行链
        first.transformEntry(context, resourceWrapper, t, count, prioritized, args);
    }
}
```

---

### 4.4 AbstractLinkedProcessorSlot —— 链节点基类

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slotchain/AbstractLinkedProcessorSlot.java`

每个 Slot 都继承此类，通过 `fireEntry` 方法将执行传递给下一个 Slot：

```java
public abstract class AbstractLinkedProcessorSlot<T> implements ProcessorSlot<T> {

    private AbstractLinkedProcessorSlot<?> next = null;

    @Override
    public void fireEntry(Context context, ResourceWrapper resourceWrapper, Object obj, int count,
                          boolean prioritized, Object... args) throws Throwable {
        if (next != null) {
            next.transformEntry(context, resourceWrapper, obj, count, prioritized, args);
        }
    }

    @SuppressWarnings("unchecked")
    void transformEntry(Context context, ResourceWrapper resourceWrapper, Object o, int count,
                        boolean prioritized, Object... args) throws Throwable {
        T t = (T) o;  // 泛型转换
        entry(context, resourceWrapper, t, count, prioritized, args);
    }

    @Override
    public void fireExit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        if (next != null) {
            next.exit(context, resourceWrapper, count, args);
        }
    }
}
```

**责任链模式的核心机制**：每个 Slot 的 `entry()` 方法在自己的逻辑完成后，调用 `fireEntry()` 把控制权传给下一个 Slot。如果某个 Slot 要拒绝请求，直接抛 `BlockException`，不调用 `fireEntry()`，链就断了。

---

## 第五阶段：各 Slot 逐一执行

### 5.1 NodeSelectorSlot —— 构建调用树

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/nodeselector/NodeSelectorSlot.java`

```java
@Spi(isSingleton = false, order = Constants.ORDER_NODE_SELECTOR_SLOT)
public class NodeSelectorSlot extends AbstractLinkedProcessorSlot<Object> {

    // key=context名称, value=该 context 下的 DefaultNode
    private volatile Map<String, DefaultNode> map = new HashMap<>(10);

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, Object obj, int count,
                      boolean prioritized, Object... args) throws Throwable {
        // 按 context 名称查找或创建 DefaultNode
        DefaultNode node = map.get(context.getName());
        if (node == null) {
            synchronized (this) {
                node = map.get(context.getName());
                if (node == null) {
                    node = new DefaultNode(resourceWrapper, null);
                    // copy-on-write 更新 map
                    HashMap<String, DefaultNode> cacheMap = new HashMap<>(map.size());
                    cacheMap.putAll(map);
                    cacheMap.put(context.getName(), node);
                    map = cacheMap;
                    // ★ 构建调用树：把新节点挂到上一层节点下面
                    ((DefaultNode) context.getLastNode()).addChild(node);
                }
            }
        }
        // 把当前节点设到 Context 的 curEntry 上
        context.setCurNode(node);
        // ★ 注意：传给下一个 Slot 的 param 变成了 node（不再是 null）
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }
}
```

**关键设计点**：`@Spi(isSingleton = false)` 意味着每个资源的 SlotChain 有自己独立的 `NodeSelectorSlot` 实例。`map` 的 key 是 context 名称，所以同一个资源在不同 context 下有不同的 `DefaultNode`（维度：资源 × context）。

---

### 5.2 ClusterBuilderSlot —— 分配 ClusterNode

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/clusterbuilder/ClusterBuilderSlot.java`

```java
@Spi(isSingleton = false, order = Constants.ORDER_CLUSTER_BUILDER_SLOT)
public class ClusterBuilderSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    private static volatile Map<ResourceWrapper, ClusterNode> clusterNodeMap = new HashMap<>();
    private static final Object lock = new Object();
    private volatile ClusterNode clusterNode = null;

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node, int count,
                      boolean prioritized, Object... args) throws Throwable {
        if (clusterNode == null) {
            synchronized (lock) {
                if (clusterNode == null) {
                    // 为该资源创建全局唯一的 ClusterNode
                    clusterNode = new ClusterNode(resourceWrapper.getName(), resourceWrapper.getResourceType());
                    HashMap<ResourceWrapper, ClusterNode> newMap = new HashMap<>(Math.max(clusterNodeMap.size(), 16));
                    newMap.putAll(clusterNodeMap);
                    newMap.put(node.getId(), clusterNode);
                    clusterNodeMap = newMap;
                }
            }
        }
        // ★ 关键：把 ClusterNode 设置到 DefaultNode 上
        node.setClusterNode(clusterNode);

        // 如果设置了调用来源(origin)，还要创建 origin 级别的统计节点
        if (!"".equals(context.getOrigin())) {
            Node originNode = node.getClusterNode().getOrCreateOriginNode(context.getOrigin());
            context.getCurEntry().setOriginNode(originNode);
        }

        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }
}
```

**核心作用**：每个资源全局只有一个 `ClusterNode`（无论被多少 context 调用），聚合该资源所有调用的统计数据。`DefaultNode` 则是 (资源 × context) 维度的统计。

---

### 5.3 StatisticSlot —— 统计记录中心

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/statistic/StatisticSlot.java`

`StatisticSlot` 的位置很巧妙——它排在所有规则检查 Slot 之前，但它先调用 `fireEntry()` 让后续 Slot 先做规则检查，然后根据结果记录统计：

```java
@Spi(order = Constants.ORDER_STATISTIC_SLOT)
public class StatisticSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node, int count,
                      boolean prioritized, Object... args) throws Throwable {
        try {
            // ★ 先执行后续所有规则检查 Slot（Authority/System/Flow/Degrade）
            fireEntry(context, resourceWrapper, node, count, prioritized, args);

            // --- 到这里说明所有检查都通过了 ---
            // 记录通过指标
            node.increaseThreadNum();
            node.addPassRequest(count);
            if (context.getCurEntry().getOriginNode() != null) {
                context.getCurEntry().getOriginNode().increaseThreadNum();
                context.getCurEntry().getOriginNode().addPassRequest(count);
            }
            if (resourceWrapper.getEntryType() == EntryType.IN) {
                Constants.ENTRY_NODE.increaseThreadNum();
                Constants.ENTRY_NODE.addPassRequest(count);
            }

            // 触发回调
            for (ProcessorSlotEntryCallback<DefaultNode> handler :
                     StatisticSlotCallbackRegistry.getEntryCallbacks()) {
                handler.onPass(context, resourceWrapper, node, count, args);
            }

        } catch (PriorityWaitException ex) {
            // 优先级等待（DefaultController 中优先级请求抢占未来窗口令牌后抛出）
            node.increaseThreadNum();
            if (context.getCurEntry().getOriginNode() != null) {
                context.getCurEntry().getOriginNode().increaseThreadNum();
            }
            if (resourceWrapper.getEntryType() == EntryType.IN) {
                Constants.ENTRY_NODE.increaseThreadNum();
            }
            for (ProcessorSlotEntryCallback<DefaultNode> handler :
                     StatisticSlotCallbackRegistry.getEntryCallbacks()) {
                handler.onPass(context, resourceWrapper, node, count, args);
            }

        } catch (BlockException e) {
            // ★ 被规则检查 Slot 拒绝
            context.getCurEntry().setBlockError(e);
            // 记录拒绝指标
            node.increaseBlockQps(count);
            if (context.getCurEntry().getOriginNode() != null) {
                context.getCurEntry().getOriginNode().increaseBlockQps(count);
            }
            if (resourceWrapper.getEntryType() == EntryType.IN) {
                Constants.ENTRY_NODE.increaseBlockQps(count);
            }
            for (ProcessorSlotEntryCallback<DefaultNode> handler :
                     StatisticSlotCallbackRegistry.getEntryCallbacks()) {
                handler.onBlocked(e, context, resourceWrapper, node, count, args);
            }
            throw e;  // 重新抛出，让 CtSph 捕获并返回给用户

        } catch (Throwable e) {
            context.getCurEntry().setError(e);
            throw e;
        }
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        Node node = context.getCurNode();

        if (context.getCurEntry().getBlockError() == null) {
            // 请求正常完成（未被 block）
            long completeStatTime = TimeUtil.currentTimeMillis();
            context.getCurEntry().setCompleteTimestamp(completeStatTime);
            long rt = completeStatTime - context.getCurEntry().getCreateTimestamp();

            Throwable error = context.getCurEntry().getError();

            // 记录 RT + 成功数 + 减少线程数
            recordCompleteFor(node, count, rt, error);
            recordCompleteFor(context.getCurEntry().getOriginNode(), count, rt, error);
            if (resourceWrapper.getEntryType() == EntryType.IN) {
                recordCompleteFor(Constants.ENTRY_NODE, count, rt, error);
            }
        }

        // 触发 exit 回调
        for (ProcessorSlotExitCallback handler : StatisticSlotCallbackRegistry.getExitCallbacks()) {
            handler.onExit(context, resourceWrapper, count, args);
        }
        fireExit(context, resourceWrapper, count, args);
    }

    private void recordCompleteFor(Node node, int batchCount, long rt, Throwable error) {
        if (node == null) { return; }
        node.addRtAndSuccess(rt, batchCount);    // 记录 RT 和成功数
        node.decreaseThreadNum();                 // 减少并发线程数
        if (error != null && !(error instanceof BlockException)) {
            node.increaseExceptionQps(batchCount);  // 记录业务异常
        }
    }
}
```

**StatisticSlot 的巧妙设计**：它排在规则检查 Slot 前面，但先 `fireEntry()` 让后面的 Slot 执行规则检查。这样：
- 通过 → 记录 pass 指标
- 被拒绝（BlockException）→ 记录 block 指标
- exit 时 → 记录 RT 和 success 指标

---

## 第六阶段：Entry 与调用栈管理

### 6.1 CtEntry —— 调用栈节点

**源码位置**: `sentinel-core/src/main/java/com/alibaba/csp/sentinel/CtEntry.java`

```java
class CtEntry extends Entry {
    protected Entry parent = null;     // 父 Entry
    protected Entry child = null;      // 子 Entry
    protected ProcessorSlot<Object> chain;
    protected Context context;

    CtEntry(ResourceWrapper resourceWrapper, ProcessorSlot<Object> chain, Context context, int count, Object[] args) {
        super(resourceWrapper, count, args);
        this.chain = chain;
        this.context = context;
        setUpEntryFor(context);  // 建立调用栈关系
    }

    // ★ 建立 parent-child 链表，形成调用栈
    private void setUpEntryFor(Context context) {
        if (context instanceof NullContext) { return; }
        this.parent = context.getCurEntry();
        if (parent != null) {
            ((CtEntry) parent).child = this;
        }
        context.setCurEntry(this);  // 栈顶指向自己
    }

    @Override
    protected Entry trueExit(int count, Object... args) throws ErrorEntryFreeException {
        exitForContext(context, count, args);
        return parent;
    }

    protected void exitForContext(Context context, int count, Object... args) {
        if (context != null) {
            if (context instanceof NullContext) { return; }

            // ★ 退出顺序检查：必须 LIFO（后进先出）
            if (context.getCurEntry() != this) {
                // 退出顺序错误！自动清理并抛异常
                CtEntry e = (CtEntry) context.getCurEntry();
                while (e != null) {
                    e.exit(count, args);
                    e = (CtEntry) e.parent;
                }
                throw new ErrorEntryFreeException("...");
            } else {
                // 正常退出：触发 chain.exit()
                if (chain != null) {
                    chain.exit(context, resourceWrapper, count, args);
                }
                // 恢复调用栈
                context.setCurEntry(parent);
                if (parent != null) {
                    ((CtEntry) parent).child = null;
                }
                if (parent == null) {
                    // 最外层 Entry 退出，如果是默认 Context 则自动清理 ThreadLocal
                    if (ContextUtil.isDefaultContext(context)) {
                        ContextUtil.exit();
                    }
                }
                clearEntryContext();
            }
        }
    }
}
```

**调用栈的维护**：Entry 形成 parent/child 双向链表。每次 `SphU.entry()` 创建新 CtEntry 时压栈，`entry.exit()` 时弹栈。必须严格按 LIFO 顺序退出，否则抛 `ErrorEntryFreeException`。

---

## 第七阶段：节点统计体系

### 7.1 节点层次结构

```
                    ┌──────────────────────────────────────────┐
                    │  StatisticNode                            │
                    │  - rollingCounterInSecond (滑动窗口/秒)   │
                    │  - rollingCounterInMinute (滑动窗口/分)   │
                    │  - curThreadNum (LongAdder)              │
                    └──────────┬───────────────────────────────┘
                               │ extends
               ┌───────────────┼───────────────────┐
               │                                   │
    ┌──────────▼──────────┐          ┌─────────────▼─────────┐
    │  DefaultNode         │          │  ClusterNode           │
    │  - childList         │          │  - originCountMap      │
    │  - clusterNode ─────────────────│    (来源→StatisticNode)│
    │  (资源×context 维度) │          │  (资源全局 维度)        │
    └──────────┬───────────┘          └────────────────────────┘
               │ extends
    ┌──────────▼──────────┐
    │  EntranceNode        │
    │  (context 入口节点)   │
    │  passQps/blockQps    │
    │  等方法聚合子节点统计  │
    └──────────────────────┘
```

### 7.2 DefaultNode 的双写机制

```java
public class DefaultNode extends StatisticNode {
    private ClusterNode clusterNode;

    @Override
    public void addPassRequest(int count) {
        super.addPassRequest(count);           // 写入 DefaultNode 自己的滑动窗口
        this.clusterNode.addPassRequest(count); // ★ 同时写入 ClusterNode（全局聚合）
    }

    @Override
    public void increaseBlockQps(int count) {
        super.increaseBlockQps(count);
        this.clusterNode.increaseBlockQps(count);
    }
    // ... 其他指标方法同理
}
```

这就是为什么 `FlowSlot` 可以选择用 `DefaultNode`（按 context 隔离）或 `ClusterNode`（全局）的统计数据来做限流判断。

---

## 总结

Sentinel 的核心执行流程可以用一句话概括：**每次 `SphU.entry()` 调用，都会找到（或创建）该资源对应的 ProcessorSlotChain，然后沿着链逐个执行 Slot，前三个 Slot 负责构建节点树和统计基础设施，StatisticSlot 负责记录指标，后续 Slot 基于统计数据做各种规则检查，任何一个 Slot 可以通过抛出 BlockException 来拒绝请求**。

设计亮点：
1. **SPI 可扩展**：SlotChain 的组成完全由 SPI 配置决定，用户可自定义 Slot 插入链中
2. **copy-on-write**：chainMap、contextNameNodeMap 等核心数据结构都用 COW 避免读锁
3. **资源保护**：Context 数量上限 2000、资源数量上限 6000，超限直接放行而非拒绝
4. **双维度统计**：DefaultNode（资源×context）+ ClusterNode（资源全局），支持不同粒度的流控策略
5. **统计与规则检查解耦**：StatisticSlot 只负责记录，具体判断逻辑在各规则 Slot 中