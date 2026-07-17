# CanalInstance装配与Meta位点持久化 —— 源码全流程解析

> 本文深入剖析 Canal 中 `CanalInstance` 的两种装配方式——编程式的 `CanalInstanceWithManager` 与 Spring-DI 式的 `CanalInstanceWithSpring`——
> 以及支撑其"断点续传"能力的两大基础设施：`CanalMetaManager`（客户端订阅/ACK 元数据管理）与 `CanalLogPositionManager`（Binlog 解析位点管理）。
> 从五大组件的职责划分，到严格有序的生命周期管理，再到 Memory/File/ZooKeeper/Mixed 四种存储介质的位点持久化实现细节，
> 逐行解读 Canal 如何在"至少一次投递"的前提下，保证进程重启后不丢数据、不重复消费过多数据。

---

## 全局定位：CanalInstance 在整体架构中的位置

在前几篇文档中，我们已经分别拆解了 Parser（模拟 Slave 协议 + Binlog 解析）、Sink（过滤 + 投递）、Store（环形缓冲区）三大数据面组件。但这些组件并不是孤立运行的——它们必须被"装配"成一个完整的、可以独立启动/停止的运行单元，这个单元就是 `CanalInstance`，对应 Canal 概念中的一个 `destination`（一个待订阅的 MySQL 数据源）。

```
                          CanalServer（一个 canal-deployer 进程）
        ┌─────────────────────────────────────────────────────────────────────┐
        │                                                                     │
        │   destination=test        destination=order        destination=... │
        │  ┌───────────────┐      ┌───────────────┐      ┌───────────────┐    │
        │  │ CanalInstance  │      │ CanalInstance  │      │ CanalInstance  │    │
        │  │ ┌───────────┐ │      │ ┌───────────┐ │      │ ┌───────────┐ │    │
        │  │ │ Parser    │ │      │ │ Parser    │ │      │ │ Parser    │ │    │
        │  │ │ Sink      │ │      │ │ Sink      │ │      │ │ Sink      │ │    │
        │  │ │ Store     │ │      │ │ Store     │ │      │ │ Store     │ │    │
        │  │ │MetaManager│ │      │ │MetaManager│ │      │ │MetaManager│ │    │
        │  │ │AlarmHandler│ │      │ │AlarmHandler│ │      │ │AlarmHandler│ │    │
        │  │ └───────────┘ │      │ └───────────┘ │      │ └───────────┘ │    │
        │  └───────────────┘      └───────────────┘      └───────────────┘    │
        │                                                                     │
        └─────────────────────────────────────────────────────────────────────┘
```

一个 `CanalServer` 进程内可以同时运行多个 `CanalInstance`，彼此独立、互不干扰，各自维护自己的 Parser 连接、Store 缓冲区和 Meta 元数据。本文关注两个问题：

1. **装配**：这五大组件是如何被创建、被注入依赖、按什么顺序启动/停止的？Canal 提供了编程式（`CanalInstanceWithManager`）和 Spring-DI 式（`CanalInstanceWithSpring`）两种装配方式，二者各自的实现细节是什么？
2. **持久化**：Parser 解析到哪个 Binlog 位置了（`LogPosition`）、客户端消费到哪个批次了（`ClientIdentityBatch`）——这些"断点"信息如何持久化，才能保证 Canal 进程重启、Failover 切换后，既不丢数据、也不需要从头开始重放整个 Binlog？

---

## 第一章：CanalInstance 接口与五大组件

### 1.1 CanalInstance 接口定义

```java
package com.alibaba.otter.canal.instance.core;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

/**
 * 代表单个canal实例，比如一个destination会独立一个实例
 *
 * @author jianghang 2012-7-12 下午01:34:49
 * @version 1.0.0
 */
public interface CanalInstance extends CanalLifeCycle {

    /**
     * 获取当前实例的标识
     */
    String getDestination();

    /**
     * 获取当前实例对应的数据解析协议
     */
    CanalEventParser getEventParser();

    /**
     * 获取当前实例对应的数据过滤与投递管理
     */
    CanalEventSink getEventSink();

    /**
     * 获取当前实例对应的数据存储
     */
    CanalEventStore getEventStore();

    /**
     * 获取当前该实例的metaManager信息
     */
    CanalMetaManager getMetaManager();

    /**
     * 报警处理类
     */
    CanalAlarmHandler getAlarmHandler();

    /**
     * 是否有需要处理的变更
     */
    boolean subscribeChange(ClientIdentity identity);

    CanalMQConfig getMqConfig();
}
```

**这一步在干什么？**

这份接口简洁地定义了一个 `CanalInstance` 应该具备的能力：它首先是一个 `CanalLifeCycle`（拥有 `start()`/`stop()`/`isStart()`），然后暴露五个核心组件的 getter，再加上一个动态订阅变更的钩子 `subscribeChange`。这五个组件并不是随意挑选的，而是精确对应 Canal 的数据流转链路上的五个必要角色：

| 组件 | 职责 | 在数据流中的位置 |
|------|------|------------------|
| `CanalEventParser` | 伪装成 MySQL Slave，执行 `dump` 协议获取 Binlog 并解析为 `Entry` | 数据源头，负责"取数"和"解析" |
| `CanalEventSink` | 对 Parser 吐出的 `Entry` 做过滤（schema/table 正则）、事务合并、Handler 责任链加工，再投递到 Store | 数据面的"过滤层" |
| `CanalEventStore` | 环形缓冲区，解耦 Parser（生产者）与 Client（消费者）的处理速度差异 | 数据面的"缓冲层" |
| `CanalMetaManager` | 记录每个客户端订阅了哪些表、消费到哪个批次（batchId）、ack 到哪个 cursor | 控制面，保证"断点续传" |
| `CanalAlarmHandler` | 当 Parser 连接失败、DDL 解析异常等场景发生时对外报警（日志/短信/邮件等） | 控制面，运维可观测性 |

为什么恰好是这五个？因为 Canal 的核心使命就是"从 MySQL 拿到变更数据，可靠地转交给下游消费者"。Parser/Sink/Store 三者构成了数据搬运的流水线（这在第 02~04 篇文档中已详细拆解），而 MetaManager 和 AlarmHandler 则是让这条流水线具备"生产级可用性"所必需的配套设施——没有 MetaManager，Canal 重启后就不知道该从哪里继续读 Binlog、也不知道客户端消费到哪儿了；没有 AlarmHandler，运维就无法感知 Parser 连接异常、复制延迟过大等问题。

`subscribeChange(ClientIdentity)` 是一个特殊的钩子方法：当某个客户端通过管理端修改了它的过滤规则（filter）后，正在运行中的 Parser 需要"热更新"其内部的过滤器，而不必重启整个 Instance。这个能力在第二章会详细展开。

---

## 第二章：AbstractCanalInstance 生命周期管理

`AbstractCanalInstance` 是 `CanalInstance` 接口的骨架实现，`CanalInstanceWithManager` 和 `CanalInstanceWithSpring` 都继承自它。它不关心五大组件具体是怎么被创建出来的（这是子类的职责），只负责这些组件"活起来"之后应该按什么顺序启动、按什么顺序停止，以及一些通用的横切逻辑（订阅变更、启动前后钩子）。

### 2.1 完整源码

```java
package com.alibaba.otter.canal.instance.core;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.google.common.collect.MigrateMap;

public abstract class AbstractCanalInstance extends AbstractCanalLifeCycle implements CanalInstance {

    protected String              destination;
    protected CanalEventParser    eventParser;
    protected CanalEventSink      eventSink;
    protected CanalEventStore     eventStore;
    protected CanalMetaManager    metaManager;
    protected CanalAlarmHandler   alarmHandler;
    protected CanalMQConfig       mqConfig = new CanalMQConfig();

    public void start() {
        super.start();
        // 首先启动 metaManager, 因为 eventSink 的初始化依赖了 metaManager
        if (!metaManager.isStart()) {
            metaManager.start();
        }

        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }

        if (!eventStore.isStart()) {
            eventStore.start();
        }

        if (!eventSink.isStart()) {
            eventSink.start();
        }

        beforeStartEventParser(eventParser);
        if (!eventParser.isStart()) {
            eventParser.start();
        }
        afterStartEventParser(eventParser);
    }

    public void stop() {
        super.stop();

        beforeStopEventParser(eventParser);
        if (eventParser.isStart()) {
            eventParser.stop();
        }
        afterStopEventParser(eventParser);

        if (eventSink.isStart()) {
            eventSink.stop();
        }

        if (eventStore.isStart()) {
            eventStore.stop();
        }

        if (alarmHandler.isStart()) {
            alarmHandler.stop();
        }

        if (metaManager.isStart()) {
            metaManager.stop();
        }
    }

    public boolean subscribeChange(ClientIdentity identity) {
        if (StringUtils.isNotEmpty(identity.getFilter())) {
            logger.info("subscribe filter change to " + identity.getFilter());
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());

            if (eventParser instanceof GroupEventParser) {
                // 处理group的模式
                List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
                for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动一下
                    ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
                }
            } else {
                ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
            }
        }

        // filter有变化，标志需要重新加载数据
        return true;
    }

    protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            abstractEventParser.setLogPositionManager(logPositionManager);

            CanalHAController haController = abstractEventParser.getHaController();

            if (!haController.isStart()) {
                haController.start();
            }
        }
    }

    protected void stopEventParserInternal(CanalEventParser eventParser) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            CanalHAController haController = abstractEventParser.getHaController();

            if (haController.isStart()) {
                haController.stop();
            }
        }
    }

    protected void beforeStartEventParser(CanalEventParser eventParser) {
        if (eventParser instanceof GroupEventParser) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动一下
                startEventParserInternal(singleEventParser, true);
            }
        } else {
            startEventParserInternal(eventParser, false);
        }
    }

    protected void afterStartEventParser(CanalEventParser eventParser) {
        // 因为canal的filter机制是通过参数动态传递的方式，所以这里需要修改下filter
        List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
        for (ClientIdentity clientIdentity : clientIdentitys) {
            subscribeChange(clientIdentity);
        }
    }

    protected void beforeStopEventParser(CanalEventParser eventParser) {
        // ...
    }

    protected void afterStopEventParser(CanalEventParser eventParser) {
        if (eventParser instanceof GroupEventParser) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动一下
                stopEventParserInternal(singleEventParser);
            }
        } else {
            stopEventParserInternal(eventParser);
        }
    }

    // ================ setter / getter ==================
}
```

> 说明：为方便阅读，上面代码块按照源码逻辑重新整理排版，方法体与真实源码保持一致；下文各小节将逐段还原并解释关键分支。

### 2.2 start()：为什么必须严格有序启动

```java
public void start() {
    super.start();
    // 首先启动 metaManager, 因为 eventSink 的初始化依赖了 metaManager
    if (!metaManager.isStart()) {
        metaManager.start();
    }

    if (!alarmHandler.isStart()) {
        alarmHandler.start();
    }

    if (!eventStore.isStart()) {
        eventStore.start();
    }

    if (!eventSink.isStart()) {
        eventSink.start();
    }

    beforeStartEventParser(eventParser);
    if (!eventParser.isStart()) {
        eventParser.start();
    }
    afterStartEventParser(eventParser);
}
```

**这一步在干什么？**

启动顺序是：`metaManager → alarmHandler → eventStore → eventSink → eventParser`。这个顺序不是随意排列的，而是严格按照"被依赖者先启动"的原则设计的：

1. **`metaManager` 最先启动**。源码注释直接点明了原因：`eventSink` 的初始化（准确地说是运行时行为，比如 `EntryEventSink` 判断是否需要过滤事务空提交）依赖 `metaManager` 已经就绪；更重要的是，`eventParser` 启动时会调用 `afterStartEventParser` 从 `metaManager` 里读取所有已订阅的客户端信息来恢复 filter（见 2.5 节），如果 `metaManager` 还没启动，这一步会直接报错或读到空数据。
2. **`alarmHandler` 次之**。它是一个相对独立的报警通道，不依赖其它组件，但需要在 `eventStore`/`eventSink`/`eventParser` 之前就绪，这样一旦后续组件初始化或运行过程中出现异常，能第一时间报警。
3. **`eventStore` 先于 `eventSink`**。因为 `eventSink.sink()` 内部要调用 `eventStore.put()`，如果 Store 还没启动（环形缓冲区数组还未分配），Sink 投递数据会直接失败。
4. **`eventSink` 先于 `eventParser`**。因为 Parser 拿到 Binlog 解析出 `Entry` 后的第一件事就是调用 `eventSink.sink()` 进行投递，如果此时 Sink 还未就绪，数据无处可去。
5. **`eventParser` 最后启动**，因为它是整条链路的"生产端"，一旦启动就会立即向 MySQL 发起模拟 Slave 连接并开始 dump Binlog、疯狂往下游推送数据。必须保证下游（Store、Sink、Meta、Alarm）全部准备就绪后再打开这个"水龙头"，否则数据会因为下游未就绪而丢失或抛异常。

这个顺序背后体现的是一种通用的分布式系统工程原则："先备好水管和水池，再打开水龙头"。

### 2.3 stop()：反序停止

```java
public void stop() {
    super.stop();

    beforeStopEventParser(eventParser);
    if (eventParser.isStart()) {
        eventParser.stop();
    }
    afterStopEventParser(eventParser);

    if (eventSink.isStart()) {
        eventSink.stop();
    }

    if (eventStore.isStart()) {
        eventStore.stop();
    }

    if (alarmHandler.isStart()) {
        alarmHandler.stop();
    }

    if (metaManager.isStart()) {
        metaManager.stop();
    }
}
```

**这一步在干什么？**

停止顺序恰好与启动顺序相反：`eventParser → eventSink → eventStore → alarmHandler → metaManager`。原理同样是"生产者先关，消费者后关，元数据基础设施最后关"：

- 先停 `eventParser`：立刻断开与 MySQL 的模拟 Slave 连接，停止产生新数据，这是防止"边关边生产"导致状态不一致的第一步。
- 再停 `eventSink`：此时 Parser 已经不再推送新数据，可以安全地停止过滤/投递逻辑。
- 再停 `eventStore`：缓冲区里可能还有未被 Client 消费完的数据，但既然整个 Instance 都要停止了，此时清空/停止是安全的。
- `alarmHandler` 和 `metaManager` 放到最后关闭，是因为在前面几个组件停止的过程中，仍然可能需要报警或读取元数据（例如 `eventParser.stop()` 内部还会调用 `logPositionManager`/`haController` 的停止逻辑，如果它们依赖 `metaManager`，此时 `metaManager` 必须还活着）。

### 2.4 subscribeChange()：不重启进程的动态过滤规则热更新

```java
public boolean subscribeChange(ClientIdentity identity) {
    if (StringUtils.isNotEmpty(identity.getFilter())) {
        logger.info("subscribe filter change to " + identity.getFilter());
        AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());

        if (eventParser instanceof GroupEventParser) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动一下
                ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
            }
        } else {
            ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
        }
    }

    // filter有变化，标志需要重新加载数据
    return true;
}
```

**这一步在干什么？**

Canal 客户端在调用 `subscribe(filter)` 时，服务端不会重启整个 `CanalInstance`，而是通过这个方法直接把新的 `AviaterRegexFilter` 塞进正在运行的 `AbstractEventParser` 实例里（`setEventFilter` 是一个普通的 setter，Parser 的主循环每次处理事件时都会读取这个字段的最新值，因此天然支持热更新，不存在可见性问题——因为 `eventFilter` 字段被声明为 `volatile`）。

这里有一个分支处理：如果当前 `eventParser` 是 `GroupEventParser`（多源合并场景，通常用于分库分表的多个物理 MySQL 实例合并为一个逻辑 destination），则需要遍历它内部持有的所有子 `EventParser`，逐一替换 filter——因为每个子 Parser 都是独立解析自己那一路 Binlog 的，过滤规则必须同步下发到每一路。

返回值固定为 `true`，语义上表示"过滤条件已发生变化，需要通知调用方重新拉取/重置数据视图"（虽然从当前实现看这个返回值目前恒为 true，并未有 false 分支，但接口留出了这个扩展点）。

### 2.5 beforeStartEventParser / afterStartEventParser：位点管理器与 HA 控制器的注入时机

```java
protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
    if (eventParser instanceof AbstractEventParser) {
        AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
        // 首先启动log position管理器
        abstractEventParser.setLogPositionManager(logPositionManager);

        CanalHAController haController = abstractEventParser.getHaController();

        if (!haController.isStart()) {
            haController.start();
        }
    }
}

protected void beforeStartEventParser(CanalEventParser eventParser) {
    if (eventParser instanceof GroupEventParser) {
        List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
        for (CanalEventParser singleEventParser : eventParsers) {
            startEventParserInternal(singleEventParser, true);
        }
    } else {
        startEventParserInternal(eventParser, false);
    }
}

protected void afterStartEventParser(CanalEventParser eventParser) {
    // 因为canal的filter机制是通过参数动态传递的方式，所以这里需要修改下filter
    List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
    for (ClientIdentity clientIdentity : clientIdentitys) {
        subscribeChange(clientIdentity);
    }
}
```

**这一步在干什么？**

`beforeStartEventParser` 在真正调用 `eventParser.start()` 之前执行，做两件事：

1. 把 `logPositionManager`（本文第五章的主角）注入到 `AbstractEventParser` 中——注意这里是"在 Parser 启动前"注入，因为 Parser 的 `start()` 方法内部第一件事就是要通过 `logPositionManager.getLatestIndexBy(destination)` 查询上一次持久化的位点，决定这次从哪里开始 dump Binlog，所以必须提前注入好。
2. 启动 `CanalHAController`（心跳检测 + 主备切换控制器），保证 Parser 一旦启动就有 HA 探测能力在后台运行。

对于 `GroupEventParser`（多源合并）的场景，这两步需要对每一个子 Parser 都重复执行一遍——因为每个子 Parser 都独立维护自己的位点和 HA 状态。

`afterStartEventParser` 则是在 Parser 真正启动、开始工作之后执行的收尾逻辑：由于 Canal 的 filter 机制是"参数动态传递"式的（不是持久化在 Parser 内部，而是每次都需要外部显式设置），所以 Instance 重启后，需要从 `metaManager` 里查出所有历史订阅过的客户端（`listAllSubscribeInfo`），把它们各自的 filter 通过 `subscribeChange()` 重新灌回正在运行的 Parser 中。这保证了"进程重启 = 恢复到重启前的过滤状态"，而不会因为重启导致 filter 丢失、退化为"不过滤全量转发"的危险状态。

至此，`AbstractCanalInstance` 已经把"骨架"搭好：五大组件按什么顺序生死、Parser 启动前后要注入什么、filter 怎么热更新——都已经明确。但这些组件本身究竟是"谁"、由"谁创建"、注入了哪些具体参数，则是下面两章的主题。

---

## 第三章：CanalInstanceWithManager —— 编程式组件初始化

`CanalInstanceWithManager` 是 Canal 在集中式管理场景下使用的 Instance 实现，它不依赖 Spring XML，而是接收一个 CanalParameter（通常来自一个集中管理的 Admin 数据库，例如通过 otter-canal-manager 后台配置），在构造函数里用纯 Java 代码 new 出全部五大组件并完成相互注入。这种方式的好处是配置可以来自任意动态数据源（数据库、配置中心等），不局限于本地 XML 文件。

### 3.1 构造函数：初始化的总入口

```java
public CanalInstanceWithManager(CanalParameter parameters, String filter){
    try {
        this.parameters = parameters;
        this.destination = parameters.getDestination();
        this.filter = filter;
        logger.info("init CanalInstance for {}-{} with parameters:{}", parameters.getId(), destination, parameters);
        // 初始化报警机制
        initAlarmHandler();
        // 初始化metaManager
        initMetaManager();
        // 初始化eventStore
        initEventStore();
        // 初始化eventSink
        initEventSink();
        // 初始化eventParser
        initEventParser();

        // 触发创建一下is_alive
        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }

        if (!metaManager.isStart()) {
            metaManager.start();
        }
    } catch (Throwable e) {
        logger.error("init failed cause by:", e);
        throw new CanalException(e);
    }
}
```

**这一步在干什么？**

构造函数按照固定顺序调用五个 initXxx() 方法完成组件创建：initAlarmHandler、initMetaManager、initEventStore、initEventSink、initEventParser。注意这个顺序和 AbstractCanalInstance.start() 里的启动顺序是一致的，因为后面的组件在创建时往往需要引用前面已经创建好的组件（例如 initEventSink 需要把 eventStore 注入进去，initEventParser 需要把 eventSink、alarmHandler、logPositionManager 等全部注入进去）。

构造函数末尾还额外主动 start() 了 alarmHandler 和 metaManager 两个组件。这看起来和 AbstractCanalInstance.start() 里的逻辑重复（那里也会判断 isStart() 再启动一次），但源码注释解释了原因：触发创建一下is_alive。某些 metaManager 实现（例如基于 ZooKeeper 的实现）在 start() 时会立即在 ZK 上创建一个临时的运行中标记节点，用于外部感知这个 destination 当前是否有实例正在管理它；这个动作希望在构造阶段就尽早触发（哪怕之后 AbstractCanalInstance.start() 还会走一遍，好在 isStart() 判断保证了幂等，不会重复启动）。

下面逐一拆解这五个 initXxx() 方法。

### 3.2 initAlarmHandler：默认日志报警加插件化自定义报警

```java
private void initAlarmHandler() {
    if (parameters.getCanalAlarmHandlerClazz() != null) {
        try {
            synchronized (CanalInstanceWithManager.class) {
                URLClassLoader urlClassLoader = null;
                if (StringUtils.isNotEmpty(parameters.getCanalAlarmHandlerDir())) {
                    File rootFile = new File(parameters.getCanalAlarmHandlerDir());
                    if (rootFile.exists()) {
                        List<URL> urls = new ArrayList<URL>();
                        File[] jarFiles = rootFile.listFiles(new FilenameFilter() {
                            public boolean accept(File dir, String name) {
                                return StringUtils.endsWithIgnoreCase(name, ".jar");
                            }
                        });
                        for (File file : jarFiles) {
                            urls.add(new URL("jar:file:" + file.getPath() + "!/"));
                        }
                        urlClassLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]),
                            Thread.currentThread().getContextClassLoader());
                    }
                }
                Class alarmHandlerClass = null;
                if (urlClassLoader != null) {
                    alarmHandlerClass = urlClassLoader.loadClass(parameters.getCanalAlarmHandlerClazz());
                } else {
                    alarmHandlerClass = Class.forName(parameters.getCanalAlarmHandlerClazz());
                }
                alarmHandler = (CanalAlarmHandler) alarmHandlerClass.newInstance();
            }
        } catch (Exception e) {
            throw new CanalException(e);
        }
    } else {
        alarmHandler = new LogAlarmHandler();
    }
}
```

**这一步在干什么？**

默认情况下（未配置 canalAlarmHandlerClazz），Canal 使用最朴素的 LogAlarmHandler，报警就是打一条 ERROR 级别的日志，依赖外部日志采集系统去做进一步的告警触达。

如果用户配置了自定义的报警实现类名，Canal 支持运行时动态加载外部 jar 包，具体分三步：从 canalAlarmHandlerDir 指定目录下扫描所有 .jar 文件；把这些 jar 包的 URL 组装成一个 URLClassLoader，父加载器设为当前线程的 ContextClassLoader，形成双亲委派；用这个新建的 URLClassLoader 去 loadClass 用户指定的报警实现类，再反射 newInstance 创建实例。

这是一个典型的插件化设计：不需要把所有可能的报警渠道实现都打进 Canal 的核心 jar 包里，用户只需实现 CanalAlarmHandler 接口，打成独立 jar 放到指定目录，通过配置类名即可插拔式接入。整个过程用 synchronized 类级别锁，避免多个 Instance 并发初始化时重复加载类导致的线程安全问题。

### 3.3 initMetaManager：四种 MetaMode 的选择

```java
private void initMetaManager() {
    MetaMode mode = parameters.getMetaMode();
    if (mode.isMemory()) {
        metaManager = new MemoryMetaManager();
    } else if (mode.isZookeeper()) {
        metaManager = new ZooKeeperMetaManager();
        ((ZooKeeperMetaManager) metaManager).setZkClientx(getZkclientx());
    } else if (mode.isMixed()) {
        PeriodMixedMetaManager periodMixedMetaManager = new PeriodMixedMetaManager();
        ZooKeeperMetaManager zooKeeperMetaManager = new ZooKeeperMetaManager();
        zooKeeperMetaManager.setZkClientx(getZkclientx());
        periodMixedMetaManager.setZooKeeperMetaManager(zooKeeperMetaManager);
        metaManager = periodMixedMetaManager;
    } else if (mode.isLocalFile()) {
        FileMixedMetaManager fileMixedMetaManager = new FileMixedMetaManager();
        fileMixedMetaManager.setDataDir(new File(parameters.getDataDir()));
        metaManager = fileMixedMetaManager;
    } else {
        throw new UnsupportedOperationException("unknow metaMode:" + mode);
    }
}
```

**这一步在干什么？**

CanalParameter 中的 MetaMode 定义了四种取值，initMetaManager 用一个 if-else 链根据配置选出具体实现：

| MetaMode | 具体实现 | 特点 | 适用场景 |
|----------|----------|------|----------|
| MEMORY | MemoryMetaManager | 纯内存，重启即丢失 | 测试临时环境 |
| ZOOKEEPER | ZooKeeperMetaManager | 所有读写直接落 ZK | 强一致性、多实例容灾切换场景 |
| MIXED | PeriodMixedMetaManager 内部包一个 ZooKeeperMetaManager | 内存为主，定期异步刷 ZK，容忍短暂 ZK 抖动 | 生产环境常用，兼顾性能与容灾 |
| LOCAL_FILE | FileMixedMetaManager | 内存为主，定期刷本地文件 | 单机部署，无 ZK 依赖 |

注意 MIXED 模式源码注释写得很直白：目前只考虑一种情况，主要为了应对短暂 zk 异常抖动不受影响。也就是说 PeriodMixedMetaManager 的设计初衷不是为了性能优化，而是为了容错：即使 ZK 短暂不可用，读写请求仍然可以直接命中内存副本而不报错，等 ZK 恢复后再异步补齐。这个实现细节会在第四章详细展开。

### 3.4 initEventStore：MemoryEventStoreWithBuffer 的完整配置

```java
private void initEventStore() {
    MemoryEventStoreWithBuffer memoryEventStore = new MemoryEventStoreWithBuffer();
    memoryEventStore.setBufferSize(parameters.getMemoryStorageBufferSize());
    memoryEventStore.setBufferMemUnit(parameters.getMemoryStorageBufferMemUnit());
    memoryEventStore.setBatchMode(BatchMode.valueOf(parameters.getStorageBatchMode()));
    memoryEventStore.setRaw(false);
    memoryEventStore.setDdlIsolation(parameters.isDdlIsolation());

    if (parameters.getStorageScavengeMode().isOnAck()) {
        eventStoreCallback = new EntryEventStoreScavengeCallback(destination);
        AbstractCanalStoreScavenge.regist(eventStoreCallback);
    }

    eventStore = memoryEventStore;
}
```

**这一步在干什么？**

initEventStore 用 CanalParameter 里的一系列参数配置 MemoryEventStoreWithBuffer（详细的三指针环形缓冲区实现已在第 04 篇文档中深入剖析，此处只关注配置项的来源与含义）。bufferSize 是环形缓冲区容量，必须是 2 的幂，用于位运算取模；bufferMemUnit 是按内存大小计算容量时的基本单位；batchMode 取值 ITEMSIZE（按事件条数限流）或 MEMSIZE（按估算内存占用限流）；raw 这里被强制设为 false，表示 Store 里存放的是反序列化后的 Entry 对象引用而不是原始字节数组，因为 CanalInstanceWithManager 场景通常需要对数据做进一步处理，保留对象引用更方便，对比之下 Spring 配置中默认 raw 为 true，直接存 protobuf 字节数组以节省内存和序列化开销；ddlIsolation 决定 DDL 语句是否要单独成一个批次返回给客户端，隔离 DDL 和 DML，避免客户端处理逻辑混淆。

此外还有一个清理回调的注册：如果 storageScavengeMode 配置为 onAck，即客户端每次 ack 后触发一次清理检查，就创建一个 EntryEventStoreScavengeCallback 并注册到 AbstractCanalStoreScavenge 的静态注册表中。这是一种典型的观察者模式：客户端 ack 消费进度后，Store 可以借此机会回收整理已经被全部消费完的缓冲区空间。

### 3.5 initEventSink：单源 EntryEventSink 与多源 GroupEventSink 的抉择

```java
private void initEventSink() {
    int groupSize = getGroupSize();
    if (groupSize <= 1) {
        eventSink = new EntryEventSink();
    } else {
        eventSink = new GroupEventSink(groupSize);
    }

    ((AbstractCanalEventSink) eventSink).setFilterTransactionEntry(!parameters.getSyncTransactionDataToDbEnable());
    ((EntryEventSink) eventSink).setEventStore(getEventStoreWithOutMemory());
}

private int getGroupSize() {
    List<List<InetSocketAddress>> groupDbAddresses = parameters.getGroupDbAddresses();
    int size = 1;
    if (groupDbAddresses != null && !groupDbAddresses.isEmpty()) {
        size = groupDbAddresses.get(0).size();
    }
    return size;
}
```

**这一步在干什么？**

getGroupSize 通过检查 CanalParameter 的 groupDbAddresses（一组分组数据库地址列表）来判断当前 destination 是否是一个多源合并场景。如果配置了多组地址，说明这是要把多个物理 MySQL 实例的 Binlog 合并成一路逻辑数据流，典型场景是分库分表后，多个物理库的变更需要按全局事务时间线排序合并。

当 groupSize 小于等于 1 时，走普通单源场景，使用 EntryEventSink（在第 04 篇文档中已详细拆解：schema 与 table 正则过滤加 Handler 责任链再直接 eventStore.tryPut）；当 groupSize 大于 1 时，走多源场景，使用 GroupEventSink，它在 EntryEventSink 的基础上额外引入了归并排序屏障（TimelineBarrier 与 TimelineTxBarrier），保证多路并发到达的 Entry 能按照全局时间线正确交织合并后再统一投递到同一个 Store。

setFilterTransactionEntry 的参数是 syncTransactionDataToDbEnable 取反：如果不需要把纯粹的事务边界标记（TRANSACTIONBEGIN 与 TRANSACTIONEND，不含任何行数据）同步给下游 DB，就把这个开关打开，Sink 会在合适的时机过滤掉空事务，减少无效投递。

### 3.6 initEventParser：最复杂的一环

```java
private void initEventParser() {
    List<List<InetSocketAddress>> groupDbAddresses = parameters.getGroupDbAddresses();
    if (groupDbAddresses == null || groupDbAddresses.isEmpty()) {
        eventParser = doInitEventParser();
    } else {
        eventParser = new GroupEventParser();
        List<CanalEventParser> eventParsers = new ArrayList<CanalEventParser>();
        for (int i = 0; i < groupDbAddresses.size(); i++) {
            List<InetSocketAddress> dbAddresses = groupDbAddresses.get(i);
            parameters.setMasterAddress(dbAddresses.get(0));
            if (dbAddresses.size() > 1) {
                parameters.setStandbyAddress(dbAddresses.get(1));
            }
            eventParsers.add(doInitEventParser());
        }
        ((GroupEventParser) eventParser).setEventParsers(eventParsers);
    }
}
```

**这一步在干什么？**

如果没有配置 groupDbAddresses，就走单源逻辑，直接调用 doInitEventParser 创建单个 Parser。如果配置了，则针对分组里的每一组地址（dbAddresses，通常是主备两个地址）依次设置到共享的 parameters 对象上并调用 doInitEventParser 生成一个独立的子 Parser，最终把所有子 Parser 收集起来包装进一个 GroupEventParser。

这里有个隐含的技巧：parameters.setMasterAddress 与 setStandbyAddress 是在一个共享的 CanalParameter 实例上反复就地修改再传给 doInitEventParser 的，因为 doInitEventParser 内部读取的正是 parameters 当前的这两个字段，所以只要保证每次调用前先设置好本轮要用的地址，再调用创建方法，就能达到用同一份参数模板批量生产出多个不同连接目标的 Parser 的效果。

下面是真正干重活的 doInitEventParser 方法，篇幅较长，先看它的主体骨架（三种 Parser 类型的选择）：

```java
private CanalEventParser doInitEventParser() {
    CanalEventParser eventParser;
    SourcingType type = parameters.getSourcingType();
    if (type != null && type.isMysql()) {
        eventParser = new MysqlEventParser();
        MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
        mysqlEventParser.setDestination(destination);
        mysqlEventParser.setConnectionCharset(Charset.forName(parameters.getConnectionCharset()));
        mysqlEventParser.setDefaultConnectionTimeoutInSeconds(parameters.getDefaultConnectionTimeoutInSeconds());
        mysqlEventParser.setSendBufferSize(parameters.getSendBufferSize());
        mysqlEventParser.setReceiveBufferSize(parameters.getReceiveBufferSize());
        mysqlEventParser.setDetectingEnable(parameters.getDetectingEnable());
        mysqlEventParser.setDetectingSQL(parameters.getDetectingSQL());
        mysqlEventParser.setDetectingIntervalInSeconds(parameters.getDetectingIntervalInSeconds());
        mysqlEventParser.setSlaveId(parameters.getSlaveId());
        mysqlEventParser.setDetectingRetryTimes(parameters.getDetectingRetryTimes());
        mysqlEventParser.setMasterInfo(new AuthenticationInfo(parameters.getMasterAddress(),
            parameters.getDbUsername(), parameters.getDbPassword(), parameters.getDefaultDatabaseName()));
        mysqlEventParser.setStandbyInfo(new AuthenticationInfo(parameters.getStandbyAddress(),
            parameters.getDbUsername(), parameters.getDbPassword(), parameters.getDefaultDatabaseName()));
        mysqlEventParser.setFilterTableError(parameters.getFilterTableError());
        // tsdb 与并行解析配置，见下文
        if (StringUtils.isNotEmpty(parameters.getRdsAccesskey())
            && StringUtils.isNotEmpty(parameters.getRdsSecretkey())
            && StringUtils.isNotEmpty(parameters.getRdsInstanceId())) {
            eventParser = new RdsBinlogEventParserProxy(mysqlEventParser,
                parameters.getRdsAccesskey(), parameters.getRdsSecretkey(), parameters.getRdsInstanceId());
        }
    } else if (type != null && type.isLocalBinlog()) {
        LocalBinlogEventParser localBinlogEventParser = new LocalBinlogEventParser();
        localBinlogEventParser.setDestination(destination);
        localBinlogEventParser.setBufferSize(parameters.getReceiveBufferSize());
        localBinlogEventParser.setDirectory(parameters.getLocalBinlogDirectory());
        localBinlogEventParser.setBatchFileSize(parameters.getLocalBinlogBatchFileSize());
        eventParser = localBinlogEventParser;
    } else {
        throw new UnsupportedOperationException("unknow sourceingType:" + type + " for detectingSQL");
    }
    // 公共收尾逻辑，见下文
    return eventParser;
}
```

**这一步在干什么？**

doInitEventParser 首先根据 SourcingType（数据来源类型）在三种 Parser 实现中做选择：

| SourcingType | 具体实现 | 场景 |
|--------------|----------|------|
| MYSQL 且未配置 RDS AK SK | MysqlEventParser | 标准自建 MySQL，直连模拟 Slave |
| MYSQL 且配置了 RDS AK SK InstanceId | RdsBinlogEventParserProxy（内部包一个 MysqlEventParser） | 阿里云 RDS，可通过 OpenAPI 获取备份的 Binlog 文件进行补偿或加速解析 |
| LOCAL_BINLOG | LocalBinlogEventParser | 离线场景，直接从本地磁盘目录读取已经落盘的 Binlog 文件解析，不需要连接真实 MySQL |

对于 MysqlEventParser 分支，配置项主要分为几类：连接与网络参数（编码字符集 connectionCharset、连接超时、收发缓冲区大小，这些都是模拟 MySQL Slave 协议握手连接时用到的底层 socket 参数）；心跳检测参数（detectingEnable、detectingSQL、detectingIntervalInSeconds，用于周期性对主库执行一条轻量 SQL 探测连接和复制的健康状态，是 HA 切换判断的数据来源之一）；主备连接信息（slaveId 是模拟 Slave 时上报给 MySQL 的唯一标识，同一个 MySQL 集群下不同 Canal 实例需要不同的 slaveId，否则会被 MySQL 视为同一个 Slave 冲突断开；masterInfo 与 standbyInfo 是两个 AuthenticationInfo，包含地址、用户名、密码、默认库名，用于 Parser 在主库不可用时切到备库继续解析）。

**TSDB（表结构快照）的条件化装配**，这是一段值得单独展开的逻辑：

```java
boolean tsdbEnable = booleanValue(parameters.getTsdbEnable(), false);
mysqlEventParser.setEnableTsdb(tsdbEnable);
if (tsdbEnable) {
    final TableMetaTSDBBuilder tsdbBuilder = new DefaultTableMetaTSDBFactory() {

        protected TableMetaTSDB doBuild(String destination) {
            doPropertyBeforeBuild();
            TableMetaTSDB tableMetaTSDB = super.doBuild(destination);
            doPropertyAfterBuild();
            return tableMetaTSDB;
        }

        private void doPropertyBeforeBuild() {
            System.setProperty("canal.instance.tsdb.url", parameters.getTsdbJdbcUrl());
            System.setProperty("canal.instance.tsdb.dbUsername", parameters.getTsdbJdbcUserName());
            System.setProperty("canal.instance.tsdb.dbPassword", parameters.getTsdbJdbcPassword());
        }

        private void doPropertyAfterBuild() {
            System.clearProperty("canal.instance.tsdb.url");
            System.clearProperty("canal.instance.tsdb.dbUsername");
            System.clearProperty("canal.instance.tsdb.dbPassword");
        }
    }.build(destination);
    mysqlEventParser.setTableMetaTSDB(tsdbBuilder);
}
```

TSDB（Table Structure TimeSeries DataBase）用于记录表结构在每个时间点或每次 DDL 变更点的快照，使得 Canal 在解析某条历史 Binlog 记录时，能拿到当时而非当前的表结构，这对正确解析行数据格式至关重要，因为表结构可能在 Binlog 产生之后又发生了变更。

这里用了一个巧妙但略显 hacky 的手法：由于 TableMetaTSDBBuilder 与 DefaultTableMetaTSDBFactory 底层是通过读取全局 System Properties 来获取 TSDB 的 JDBC 连接信息的（这套机制原本是为 Spring 配置模式设计的，习惯用系统属性传参），而 CanalInstanceWithManager 场景下这些参数来自 CanalParameter 对象而非系统属性，所以这里通过匿名内部类覆写 doBuild，在真正构建之前临时把参数写入 System.setProperty，构建完成后立刻 System.clearProperty 清理掉。本质上是把实例级配置通过短暂借用进程级全局状态的方式传递给一个原本只认全局状态的工厂方法。这种写法虽然实现了目的，但也隐含着多线程并发构建多个 destination 的 TSDB 时可能相互踩踏系统属性的风险，好在这段代码在 CanalInstanceWithManager 构造函数中执行，通常发生在启动阶段，并发概率较低。

**其余公共收尾逻辑**（对单源和 group 场景、mysql 与 local-binlog 场景都统一生效）：

```java
mysqlEventParser.setParallel(parameters.getParallel());
mysqlEventParser.setParallelBufferSize(parameters.getParallelBufferSize());
mysqlEventParser.setParallelThreadSize(parameters.getParallelThreadSize());

if (StringUtils.isNotEmpty(filter)) {
    AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(filter);
    ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
}

if (parameters.getGtidEnable()) {
    ((AbstractEventParser) eventParser).setGTIDMode(true);
}

((AbstractEventParser) eventParser).setAlarmHandler(getAlarmHandler());
HeartBeatHAController haController = new HeartBeatHAController();
haController.setSwitchEnable(parameters.getHeartbeatHaEnable());
haController.setDetectingRetryTimes(parameters.getDetectingRetryTimes());
((AbstractEventParser) eventParser).setHaController(haController);

((AbstractEventParser) eventParser).setTransactionSize(parameters.getTransactionSize());
((AbstractEventParser) eventParser).setLogPositionManager(initLogPositionManager());
((AbstractEventParser) eventParser).setEventSink(eventSink);
```

并行解析参数 parallel、parallelBufferSize、parallelThreadSize 对应 Canal 的多线程并行 Binlog 解析能力，即拆分并发解码字节流成 Entry 对象这个 CPU 密集型工作，充分利用多核。

过滤规则注入：正则黑白名单 filter（AviaterRegexFilter）在这里完成首次注入，对应第一章讨论的、后续可通过 subscribeChange 热更新的那个字段。

GTID 模式：如果开启了 gtidEnable，设置 Parser 按 GTID（Global Transaction Identifier）而非传统的 binlog 文件名加偏移量方式定位续传位点，这在做主备切换时能提供更强的位点一致性保证。

报警处理器注入：把之前 initAlarmHandler 创建好的 alarmHandler 注入进 Parser，这样 Parser 内部但凡出现连接异常、解析异常都能统一走报警通道。

事务缓冲区大小：transactionSize 对应 EventTransactionBuffer 的容量，即第 04 篇文档中提到的环形缓冲结构，用于按事务边界攒批再统一 flush。

位点管理器：调用 initLogPositionManager（见 3.8 节）拿到具体的 CanalLogPositionManager 实现并注入。

EventSink 注入：最后把整条链路下游的 eventSink 注入进 Parser，形成 Parser 解析完直接调用 Sink 的完整闭环。

### 3.7 initHaController：目前只支持心跳探测 HA

结合上面 doInitEventParser 中的片段：

```java
HeartBeatHAController haController = new HeartBeatHAController();
haController.setSwitchEnable(parameters.getHeartbeatHaEnable());
haController.setDetectingRetryTimes(parameters.getDetectingRetryTimes());
((AbstractEventParser) eventParser).setHaController(haController);
```

不同于 Spring 配置文件中把 HA 控制器直接内联在 XML 里声明的做法，CanalInstanceWithManager 在代码里直接固定创建 HeartBeatHAController（基于前面提到的心跳探测 SQL 判断主库健康状况，探测连续失败达到 detectingRetryTimes 次后触发主备切换）。switchEnable 是一个总开关，即便配置了备库信息，如果这个开关关闭，Parser 也只会不断重试连接主库，不会自动切到备库，这是为了避免在某些对切换比较敏感的业务场景中，Canal 擅自做主切换导致的数据源不可控变化。

值得注意 Canal 目前的 HA 机制仅支持这一种基于心跳探测的实现。源码中虽然定义了 HAMode 枚举，但 CanalInstanceWithManager 没有像 MetaMode 与 IndexMode 那样做多态分支处理，而是硬编码只创建 HeartBeatHAController。

### 3.8 initLogPositionManager：五种 IndexMode 的选择

```java
private CanalLogPositionManager initLogPositionManager() {
    CanalLogPositionManager logPositionManager;
    IndexMode indexMode = parameters.getIndexMode();
    if (indexMode.isMemory()) {
        logPositionManager = new MemoryLogPositionManager();
    } else if (indexMode.isZookeeper()) {
        logPositionManager = new ZooKeeperLogPositionManager(getZkclientx());
    } else if (indexMode.isMixed()) {
        MemoryLogPositionManager memoryLogPositionManager = new MemoryLogPositionManager();
        ZooKeeperLogPositionManager zooKeeperLogPositionManager = new ZooKeeperLogPositionManager(getZkclientx());
        logPositionManager = new PeriodMixedLogPositionManager(memoryLogPositionManager,
            zooKeeperLogPositionManager, 1000);
    } else if (indexMode.isMeta()) {
        if (metaManager instanceof MemoryMetaManager) {
            throw new CanalException("meta mode is not support for memory mode");
        }
        logPositionManager = new MetaLogPositionManager(metaManager);
    } else if (indexMode.isMemoryMetaFailback()) {
        MemoryLogPositionManager memoryLogPositionManager = new MemoryLogPositionManager();
        MetaLogPositionManager metaLogPositionManager = new MetaLogPositionManager(metaManager);
        logPositionManager = new FailbackLogPositionManager(memoryLogPositionManager, metaLogPositionManager);
    } else {
        throw new UnsupportedOperationException("unknow indexMode:" + indexMode);
    }
    return logPositionManager;
}
```

**这一步在干什么？**

IndexMode 有五种取值，映射关系如下：

| IndexMode | 具体实现 | 说明 |
|-----------|----------|------|
| MEMORY | MemoryLogPositionManager | 纯内存，重启丢失位点 |
| ZOOKEEPER | ZooKeeperLogPositionManager | 每次持久化都直接写 ZK |
| MIXED | PeriodMixedLogPositionManager（内存加定期刷 ZK，周期硬编码 1000 毫秒） | 兼顾性能与容灾 |
| META | MetaLogPositionManager（委托给 metaManager） | 复用 Meta 模块已经持久化的客户端 cursor 位点推算 Parser 位点，见第五章 |
| MEMORY_META_FAILBACK | FailbackLogPositionManager(Memory, Meta) | 两级降级：内存优先，内存没有则回退到 Meta 推算 |

有一个防御性校验值得注意：当选择 META 模式时，代码显式检查 metaManager 是否是 MemoryMetaManager 的实例并抛出异常，因为如果 metaManager 本身都是纯内存实现（重启即丢失），那么委托给它推算位点毫无意义（推算出来的位点同样在重启后归零），这种组合在语义上是自相矛盾的，所以直接在初始化阶段就快速失败（fail-fast），而不是留到运行时才暴露出诡异的行为。

getZkclientx 方法用于给 ZK 相关实现注入连接，内部做了地址排序处理，核心逻辑是把配置的多个 ZK 集群地址排序后拼接成字符串作为 ZkClientx 连接池的 key，这样只要地址集合相同，无论传入顺序如何，都会复用同一个底层 ZkClient 连接，避免重复建连。

至此，CanalInstanceWithManager 的完整初始化流程已经拆解完毕：五个 initXxx 方法层层递进，把 CanalParameter 里几十个配置项精确地转换为一整套相互关联、可运行的组件实例。

---

## 第四章：CanalInstanceWithSpring —— Spring DI 组件装配

如果说 CanalInstanceWithManager 是把装配逻辑写在 Java 代码里，那么 CanalInstanceWithSpring 走的是完全相反的路线：它本身几乎不包含任何业务逻辑，只是一组标准的 Spring bean setter，真正的装配工作全部下放给 Spring 容器和 XML 配置文件完成。这是 canal-deployer 独立部署模式（不依赖任何 manager 后台）默认采用的方式。

### 4.1 CanalInstanceWithSpring 源码

```java
package com.alibaba.otter.canal.instance.spring;

import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.instance.core.AbstractCanalInstance;
import com.alibaba.otter.canal.instance.core.CanalMQConfig;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

public class CanalInstanceWithSpring extends AbstractCanalInstance {

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setEventParser(CanalEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setEventSink(CanalEventSink eventSink) {
        this.eventSink = eventSink;
    }

    public void setEventStore(CanalEventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void setMetaManager(CanalMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

    public void setMqConfig(CanalMQConfig mqConfig) {
        this.mqConfig = mqConfig;
    }
}
```

**这一步在干什么？**

整个类只有一堆 setter，没有任何自定义的初始化逻辑，完全依赖 Spring 的依赖注入机制把已经在 XML 中定义好的各个 bean 注入进来。所有五大组件的具体类型选择、参数配置，全部转移到了 XML 配置层面，Java 代码本身对应该用哪种 MetaManager、应该用哪种 LogPositionManager 一无所知，它只是被动接收容器塞进来的引用。

这种设计的优势是配置和代码彻底解耦：运维人员只需要修改 XML 或 properties 文件即可切换 Instance 的存储介质、连接参数，无需重新编译或走管理端接口。canal-deployer 官方发行包正是内置了四套预设的 Spring XML 组合（memory、file、default、group），供用户根据部署形态直接选用。

### 4.2 base-instance.xml：公共基础设施

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:tx="http://www.springframework.org/schema/tx"
	xmlns:aop="http://www.springframework.org/schema/aop" xmlns:lang="http://www.springframework.org/schema/lang"
	xmlns:context="http://www.springframework.org/schema/context"
	xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
           http://www.springframework.org/schema/aop http://www.springframework.org/schema/aop/spring-aop-2.0.xsd
           http://www.springframework.org/schema/lang http://www.springframework.org/schema/lang/spring-lang-2.0.xsd
           http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx-2.0.xsd
           http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context-2.5.xsd"
	default-autowire="byName">

	<bean class="com.alibaba.otter.canal.instance.spring.support.PropertyPlaceholderConfigurer" lazy-init="false">
		<property name="ignoreResourceNotFound" value="true" />
		<property name="systemPropertiesModeName" value="SYSTEM_PROPERTIES_MODE_OVERRIDE"/>
		<property name="locationNames">
			<list>
				<value>classpath:canal.properties</value>
				<value>classpath:${canal.instance.destination:}/instance.properties</value>
			</list>
		</property>
	</bean>
	
	<bean id="socketAddressEditor" class="com.alibaba.otter.canal.instance.spring.support.SocketAddressEditor" />
	<bean class="org.springframework.beans.factory.config.CustomEditorConfigurer"> 
		<property name="propertyEditorRegistrars">
			<list>
				<ref bean="socketAddressEditor" />
			</list>
		</property>
	</bean>
	
	<bean id="baseEventParser" class="com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogEventParserProxy" abstract="true">
		<property name="accesskey" value="${canal.aliyun.accesskey:}" />
		<property name="secretkey" value="${canal.aliyun.secretkey:}" />
		<property name="instanceId" value="${canal.instance.rds.instanceId:}" />
	</bean>
</beans>
```

**这一步在干什么？**

base-instance.xml 被四套具体 profile 通过 import 标签共同引入，提供三样公共基础设施。

第一是属性占位符加载器（PropertyPlaceholderConfigurer）：按顺序加载 classpath 下的 canal.properties（全局公共配置）和 destination 专属目录下的 instance.properties（路径中用占位符动态拼接目录名）。ignoreResourceNotFound 设为 true 表示即使某个文件不存在也不报错，保证不同环境的灵活性。特别值得注意的是 systemPropertiesModeName 设为 SYSTEM_PROPERTIES_MODE_OVERRIDE：这个模式允许 JVM 启动参数里的 -D 参数覆盖 properties 文件里同名的配置项，形成文件配置为默认值、命令行参数可覆盖的优先级顺序，这是一种常见的运维友好设计，方便临时调参而不必修改配置文件。

第二是自定义属性编辑器（SocketAddressEditor 加 CustomEditorConfigurer）：让 Spring 能够把 properties 里形如字符串形式的地址端口自动转换成 InetSocketAddress 对象类型的 bean 属性，省去了每个 XML 里手写地址 bean 的繁琐。

第三是 baseEventParser 抽象 bean：这是一个 abstract 为 true 的模板 bean，类型固定为 RdsBinlogEventParserProxy（即前面在 CanalInstanceWithManager 中提到的支持阿里云 RDS 加速解析的代理类），预置了 accesskey、secretkey、instanceId 三个属性，均从 properties 读取，默认空字符串。四套具体 profile 里的 eventParser bean 都通过 parent 属性继承这个模板，这样无论是否真的使用 RDS 加速，所有 Parser 都统一走同一个代理类，若 AK SK InstanceId 为空则代理类内部会退化为普通的直连 MysqlEventParser 行为，这个降级判断逻辑封装在 RdsBinlogEventParserProxy 内部，属于 Parser 模块范畴，不在本文展开。

### 4.3 四套预置 Spring XML 对比总览

canal-deployer 在 conf/spring 目录下预置了四套可直接使用的 Instance 配置文件，用户通过 canal.instance.global.spring.xml 属性选择加载哪一套：

| Profile | MetaManager | LogPositionManager | EventStore raw | 适用场景 |
|---------|-------------|---------------------|-----------------|----------|
| memory-instance.xml | MemoryMetaManager | MemoryLogPositionManager | true | 测试调试，重启即丢失所有状态 |
| file-instance.xml | FileMixedMetaManager | FailbackLogPositionManager(Memory, Meta) | true | 单机部署，无 ZK 依赖，用本地文件持久化 |
| default-instance.xml | PeriodMixedMetaManager 内部包 ZooKeeperMetaManager | FailbackLogPositionManager(Memory, Meta) | true | 生产环境标准配置，ZK 持久化加内存加速 |
| group-instance.xml | MemoryMetaManager | MemoryLogPositionManager | true | 多源合并（分库分表）场景，双 Parser 归并 |

下面逐一展开每套配置的关键差异点。

### 4.4 memory-instance.xml：一切皆内存

```xml
<bean id="metaManager" class="com.alibaba.otter.canal.meta.MemoryMetaManager" />

<bean id="eventStore" class="com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer">
	<property name="bufferSize" value="${canal.instance.memory.buffer.size:16384}" />
	<property name="bufferMemUnit" value="${canal.instance.memory.buffer.memunit:1024}" />
	<property name="batchMode" value="${canal.instance.memory.batch.mode:MEMSIZE}" />
	<property name="ddlIsolation" value="${canal.instance.get.ddl.isolation:false}" />
	<property name="raw" value="${canal.instance.memory.rawEntry:true}" />
</bean>

<bean id="eventSink" class="com.alibaba.otter.canal.sink.entry.EntryEventSink">
	<property name="eventStore" ref="eventStore" />
	<property name="filterTransactionEntry" value="${canal.instance.filter.transaction.entry:false}"/>
</bean>

<bean id="eventParser" parent="baseEventParser">
	<property name="destination" value="${canal.instance.destination}" />
	<property name="slaveId" value="${canal.instance.mysql.slaveId:0}" />
	<property name="logPositionManager">
		<bean class="com.alibaba.otter.canal.parse.index.MemoryLogPositionManager" />
	</property>
</bean>
```

**这一步在干什么？**

memory-instance.xml 是最简单的一套配置：metaManager 直接是 MemoryMetaManager，eventParser 里的 logPositionManager 也直接是裸的 MemoryLogPositionManager，没有任何持久化落地的动作。这意味着一旦 canal-deployer 进程重启，客户端订阅信息、ack 位置、Parser 解析位点会全部归零，Parser 只能按照配置文件里写死的 masterPosition 起始位点重新开始，如果没配置，则从当前最新的 Binlog 位置开始，历史数据全部丢失。这套配置仅适合快速验证、功能测试等对数据可靠性没有要求的场景。

### 4.5 file-instance.xml：本地文件持久化加两级位点降级

```xml
<bean id="metaManager" class="com.alibaba.otter.canal.meta.FileMixedMetaManager">
	<property name="dataDir" value="${canal.file.data.dir:../conf}" />
	<property name="period" value="${canal.file.flush.period:1000}" />
</bean>
```

```xml
<property name="logPositionManager">
	<bean class="com.alibaba.otter.canal.parse.index.FailbackLogPositionManager">
		<constructor-arg>
			<bean class="com.alibaba.otter.canal.parse.index.MemoryLogPositionManager" />
		</constructor-arg>
		<constructor-arg>
			<bean class="com.alibaba.otter.canal.parse.index.MetaLogPositionManager">
				<constructor-arg ref="metaManager"/>
			</bean>
		</constructor-arg>
	</bean>
</property>
```

**这一步在干什么？**

file-instance.xml 相比 memory-instance.xml 有两处关键升级。

第一，metaManager 换成了 FileMixedMetaManager，客户端订阅信息、ack 批次数据会内存优先写入，同时按 period（默认 1000 毫秒）周期性地刷到 dataDir（默认 ../conf）下的本地文件中，重启后可以从文件恢复。

第二，logPositionManager 换成了 FailbackLogPositionManager，两个构造参数分别是 MemoryLogPositionManager（primary，一级）和 MetaLogPositionManager（secondary，二级，内部包装了刚刚创建的 metaManager）。这意味着 Parser 查询上次解析到哪里了时，优先看内存里有没有记录，如果进程从未重启过，内存里当然有最新值，查询最快；如果内存里没有，典型场景是进程刚重启、内存态清空，则退化去问 MetaLogPositionManager，它并不直接存储 Parser 自己的位点，而是通过 metaManager 反查所有订阅了这个 destination 的客户端各自消费到哪个 cursor 了，取其中最小值作为 Parser 应该从哪里继续解析的依据，这个以慢打快的推算逻辑会在第五章和第六章详细展开。这样即使 Parser 自身从未显式持久化过位点，也能借助客户端消费进度这个侧面信息间接推算出一个安全的重启起点，不会有数据被跳过。

### 4.6 default-instance.xml：ZooKeeper 持久化的生产标准配置

```xml
<bean id="zkClientx" class="org.springframework.beans.factory.config.MethodInvokingFactoryBean" >
	<property name="targetClass" value="com.alibaba.otter.canal.common.zookeeper.ZkClientx" />
	<property name="targetMethod" value="getZkClient" />
	<property name="arguments">
		<list>
			<value>${canal.zkServers:127.0.0.1:2181}</value>
		</list>
	</property>
</bean>

<bean id="metaManager" class="com.alibaba.otter.canal.meta.PeriodMixedMetaManager">
	<property name="zooKeeperMetaManager">
		<bean class="com.alibaba.otter.canal.meta.ZooKeeperMetaManager">
			<property name="zkClientx" ref="zkClientx" />
		</bean>
	</property>
	<property name="period" value="${canal.zookeeper.flush.period:1000}" />
</bean>
```

**这一步在干什么？**

zkClientx 这个 bean 是一个典型的 Spring MethodInvokingFactoryBean 用法：通过反射调用 ZkClientx.getZkClient 这个静态工厂方法，把返回的连接对象注册为一个普通 bean，供后续多个 bean 通过 ref 复用。ZkClientx 内部对相同的地址字符串会做连接复用缓存，因此即便多个 destination 各自的 Spring 容器都调用这个方法，只要地址相同，底层物理连接也是共享的，与 3.8 节 getZkclientx 的排序拼接 key 逻辑是同一套设计思想的两种实现载体。

metaManager 使用 PeriodMixedMetaManager，内存优先，定期把 cursor 刷到内嵌的 ZooKeeperMetaManager，logPositionManager 的配置和 file-instance.xml 完全一样，也是 FailbackLogPositionManager 包装 Memory 与 Meta，只是这里的 metaManager 背后是 ZK 而不是本地文件。这是官方推荐的生产环境标准配置：既能借助 ZK 的高可用与多机可见性支持多 Canal Server 之间的协调，比如同一个 destination 在多台机器上部署时的抢占式接管，又通过内存优先加定期异步刷的方式规避了每次读写都走网络 IO 到 ZK 带来的性能损耗。

### 4.7 group-instance.xml：多源合并的双 Parser 结构

```xml
<bean id="metaManager" class="com.alibaba.otter.canal.meta.MemoryMetaManager" />

<bean id="eventParser" class="com.alibaba.otter.canal.parse.inbound.group.GroupEventParser">
	<property name="eventParsers">
		<list>
			<ref bean="eventParser1" />
			<ref bean="eventParser2" />
		</list>
	</property>
</bean>

<bean id="eventParser1" parent="baseEventParser">
	<property name="destination" value="${canal.instance.destination}" />
	<property name="masterInfo">
		<bean class="com.alibaba.otter.canal.parse.support.AuthenticationInfo" init-method="initPwd">
			<property name="address" value="${canal.instance.master1.address}" />
		</bean>
	</property>
</bean>

<bean id="eventParser2" parent="baseEventParser">
	<property name="destination" value="${canal.instance.destination}" />
	<property name="masterInfo">
		<bean class="com.alibaba.otter.canal.parse.support.AuthenticationInfo" init-method="initPwd">
			<property name="address" value="${canal.instance.master2.address}" />
		</bean>
	</property>
</bean>
```

**这一步在干什么？**

group-instance.xml 展示了如何用 Spring XML 组装出第三章介绍过的多源合并拓扑：eventParser 这个顶层 bean 的类型是 GroupEventParser，它的 eventParsers 属性是一个包含 eventParser1 和 eventParser2 两个独立子 Parser 的列表。这两个子 Parser 各自都是 parent 为 baseEventParser 的完整配置，拥有独立的 master1.address 与 master2.address 等专属占位符，意味着它们各自连接不同的物理 MySQL 实例，但是它们共享同一个 metaManager、同一个 eventSink（EntryEventSink）、同一个 eventStore（MemoryEventStoreWithBuffer），这正是多个 Binlog 数据源合并写入同一个逻辑 destination 的一个 Store 的物理体现。

值得注意的是 group-instance.xml 中 metaManager 和 logPositionManager 都退回到最简单的内存实现，并没有像 file 或 default 那样接入持久化，这可能是因为多源合并场景下位点恢复的语义更复杂，需要分别记录每个子源各自的位点，社区提供的这套样例配置更多是展示拓扑结构本身，实际生产若要用 group 模式，往往需要用户根据自身需求进一步定制持久化策略。

至此，四套预置配置的差异点已经梳理完毕：从 memory 到 file 到 default 是持久化能力递增的单源部署形态，group 则是另一个维度上横向合并多个数据源的拓扑扩展，二者可以正交组合，虽然官方样例中 group 没有搭配持久化 MetaManager，但用户完全可以参照 default-instance.xml 的模式自行为 group 场景接入 ZK 持久化。

---

## 第五章：Meta 模块 —— 元数据管理

前面两章介绍了五大组件如何被装配起来，其中反复出现的 metaManager 究竟管理什么、内部结构如何，是本章要详细拆解的内容。CanalMetaManager 承担着 Canal 至少一次投递语义的核心记账职责：谁订阅了哪个 destination、订阅规则是什么、每个客户端消费到哪个批次了、每个批次对应的 Binlog 位置区间是什么。

### 5.1 CanalMetaManager 接口：三组 API

```java
package com.alibaba.otter.canal.meta;

import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;

public interface CanalMetaManager extends CanalLifeCycle {

    void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException;

    List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException;

    PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    PositionRange getLastestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException;

    void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId)
                                                                                           throws CanalMetaManagerException;

    PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException;

    PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException;

    Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException;

    void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException;

}
```

**这一步在干什么？**

这份接口的 12 个方法可以清晰地划分为三组。

第一组是订阅管理：subscribe、hasSubscribe、unsubscribe、listAllSubscribeInfo。ClientIdentity 是 destination 加 clientId 加 filter 的组合，一个 destination 下可以有多个不同 clientId 的客户端各自订阅不同的过滤规则，这组 API 维护的是谁在关注这个 destination、关注什么内容这层元信息。第二章 afterStartEventParser 里调用的 metaManager.listAllSubscribeInfo 正是这组 API 的消费者。

第二组是消费位点（cursor）管理：getCursor、updateCursor。Position 是一个抽象的位置标识，通常底层是 EntryPosition，即 binlog 文件名加偏移量，代表某个客户端确认消费完成之后的最新进度。这个 cursor 与 Parser 自己的 LogPositionManager 位点是两个不同维度的概念：Parser 的位点回答我解析到哪儿了，而 Meta 的 cursor 回答某个具体客户端确认收到并处理完哪儿了。同一个 destination 可能有多个客户端，各自的 cursor 进度可能不同，而 Parser 只有一个（对单源场景），二者存在依赖关系，第六章会讲到 MetaLogPositionManager 正是用所有客户端 cursor 的最小值来推算 Parser 应该从哪里重新开始。

第三组是批次（batch）管理：addBatch（两个重载）、getBatch、removeBatch、getFirstBatch、getLastestBatch、listAllBatchs、clearAllBatchs。这组 API 支撑的是 Canal 客户端典型的 get 加 ack 或 rollback 拉取模式：客户端每次调用 get 从 Store 拉取一批数据，服务端就会调用 addBatch 给这批数据分配一个唯一递增的 batchId 并记录下这批数据对应的 PositionRange（起止位置区间），客户端处理完毕后调用 ack(batchId) 触发 removeBatch 确认消费完成，或者调用 rollback 放弃本次消费，不移除 batch，下次 get 时会重新分配。这套批次追踪机制正是至少一次投递语义的实现基础：只要客户端没有显式 ack，这批数据就被认为可能还未被成功处理，即使 canal-server 重启，只要 Store 里数据还在，或者能重新从 Binlog 位置回放，客户端依然可以重新拉取到同一批数据。

下面依次深入五种具体实现。

### 5.2 MemoryMetaManager：三个 Map 与 MemoryClientIdentityBatch

```java
package com.alibaba.otter.canal.meta;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.MigrateMap;

public class MemoryMetaManager extends AbstractCanalLifeCycle implements CanalMetaManager {

    protected Map<String, List<ClientIdentity>>              destinations;
    protected Map<ClientIdentity, MemoryClientIdentityBatch> batches;
    protected Map<ClientIdentity, Position>                  cursors;

    public void start() {
        super.start();
        batches = MigrateMap.makeComputingMap(MemoryClientIdentityBatch::create);
        cursors = new MapMaker().makeMap();
        destinations = MigrateMap.makeComputingMap(destination -> new ArrayList<>());
    }

    public void stop() {
        super.stop();
        destinations.clear();
        cursors.clear();
        for (MemoryClientIdentityBatch batch : batches.values()) {
            batch.clearPositionRanges();
        }
    }

    public synchronized void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        if (clientIdentitys.contains(clientIdentity)) {
            clientIdentitys.remove(clientIdentity);
        }
        clientIdentitys.add(clientIdentity);
    }

    public synchronized boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        return clientIdentitys != null && clientIdentitys.contains(clientIdentity);
    }

    public synchronized void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        List<ClientIdentity> clientIdentitys = destinations.get(clientIdentity.getDestination());
        if (clientIdentitys != null && clientIdentitys.contains(clientIdentity)) {
            clientIdentitys.remove(clientIdentity);
        }
    }

    public synchronized List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException {
        // fixed issue #657, fixed ConcurrentModificationException
        return Lists.newArrayList(destinations.get(destination));
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return cursors.get(clientIdentity);
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        cursors.put(clientIdentity, position);
    }

    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        return batches.get(clientIdentity).addPositionRange(positionRange);
    }

    public void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId)
                                                                                                  throws CanalMetaManagerException {
        batches.get(clientIdentity).addPositionRange(positionRange, batchId);// 添加记录到指定batchId
    }

    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).removePositionRange(batchId);
    }

    public PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getPositionRange(batchId);
    }

    public PositionRange getLastestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getLastestPositionRange();
    }

    public PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).getFirstPositionRange();
    }

    public Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return batches.get(clientIdentity).listAllPositionRange();
    }

    public void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        batches.get(clientIdentity).clearPositionRanges();
    }

    // ============================

    public static class MemoryClientIdentityBatch {

        private ClientIdentity           clientIdentity;
        private Map<Long, PositionRange> batches          = new MapMaker().makeMap();
        private AtomicLong               atomicMaxBatchId = new AtomicLong(1);

        public static MemoryClientIdentityBatch create(ClientIdentity clientIdentity) {
            return new MemoryClientIdentityBatch(clientIdentity);
        }

        public MemoryClientIdentityBatch(){
        }

        protected MemoryClientIdentityBatch(ClientIdentity clientIdentity){
            this.clientIdentity = clientIdentity;
        }

        public synchronized void addPositionRange(PositionRange positionRange, Long batchId) {
            updateMaxId(batchId);
            batches.put(batchId, positionRange);
        }

        public synchronized Long addPositionRange(PositionRange positionRange) {
            Long batchId = atomicMaxBatchId.getAndIncrement();
            batches.put(batchId, positionRange);
            return batchId;
        }

        public synchronized PositionRange removePositionRange(Long batchId) {
            if (batches.containsKey(batchId)) {
                Long minBatchId = Collections.min(batches.keySet());
                if (!minBatchId.equals(batchId)) {
                    // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
                    throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d",
                        batchId, minBatchId));
                }
                return batches.remove(batchId);
            } else {
                return null;
            }
        }

        public synchronized PositionRange getPositionRange(Long batchId) {
            return batches.get(batchId);
        }

        public synchronized PositionRange getLastestPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.max(batches.keySet());
                return batches.get(batchId);
            }
        }

        public synchronized PositionRange getFirstPositionRange() {
            if (batches.size() == 0) {
                return null;
            } else {
                Long batchId = Collections.min(batches.keySet());
                return batches.get(batchId);
            }
        }

        public synchronized Map<Long, PositionRange> listAllPositionRange() {
            Set<Long> batchIdSets = batches.keySet();
            List<Long> batchIds = new ArrayList<>(batchIdSets);
            Collections.sort(new ArrayList<>(batchIds));
            return Maps.newHashMap(batches);
        }

        public synchronized void clearPositionRanges() {
            batches.clear();
        }

        private synchronized void updateMaxId(Long batchId) {
            if (atomicMaxBatchId.get() < batchId + 1) {
                atomicMaxBatchId.set(batchId + 1);
            }
        }
    }
}
```

**这一步在干什么？**

MemoryMetaManager 用三个 Map 分别承载三组 API 的数据：

- `destinations`：`Map<String destination, List<ClientIdentity>>`，记录每个 destination 下有哪些客户端订阅；
- `cursors`：`Map<ClientIdentity, Position>`，记录每个客户端的最新确认消费位置；
- `batches`：`Map<ClientIdentity, MemoryClientIdentityBatch>`，记录每个客户端当前所有"已分配但未 ack"的批次。

三个 Map 都用 Guava 的 `MigrateMap.makeComputingMap`（`destinations`/`batches`）或 `MapMaker().makeMap()`（`cursors`）构建，前者的特点是"访问不存在的 key 时自动用工厂函数计算并放入一个默认值再返回"，免去了到处写 `if (map.get(key) == null) map.put(key, new XXX())` 的样板代码——比如 `destinations.get(clientIdentity.getDestination())` 即使这个 destination 第一次被访问，也会自动得到一个空的 `ArrayList`，不会返回 null 引发 NPE。

内部静态类 `MemoryClientIdentityBatch` 是每个客户端自己的批次管理器，核心是一个 `AtomicLong atomicMaxBatchId`（初始值 1）加一个 `Map<Long, PositionRange> batches`：

- `addPositionRange(range)`（不指定 batchId 的重载）：调用 `atomicMaxBatchId.getAndIncrement()` 拿到一个全新的、单调递增的 batchId，然后存入 map。这是最常见的调用路径——客户端每次 `get()` 数据时，服务端就是这样自动分配 batchId 的。
- `addPositionRange(range, batchId)`（指定 batchId 的重载）：用于一些需要显式指定 batchId 的场景（比如某些客户端重放历史 batch 数据），插入后调用 `updateMaxId(batchId)` 确保内部维护的 `atomicMaxBatchId` 不会因为这次"插队式"的插入而变得比新插入的 batchId 还小，保证后续自动分配的 batchId 依然严格递增不冲突。
- `removePositionRange(batchId)`：这是最能体现设计意图的一段代码。它并不是简单地 `batches.remove(batchId)`，而是先检查 `Collections.min(batches.keySet())` 是否恰好等于要移除的 `batchId`，如果不是，直接抛出 `CanalMetaManagerException`。这个限制强制要求客户端必须**严格按照 batchId 分配的先后顺序依次 ack**，不允许"跳着 ack"（比如先 ack 批次 5 而不先 ack 批次 3、4）。为什么要这么设计？因为 Canal 的位点持久化是按 `PositionRange` 的连续区间推进的，如果允许乱序 ack，会出现批次 3、4 还没确认但批次 5 已经确认的情况，此时无法安全地把 Parser 的持久化位点往前推进到批次 5 的终点（因为批次 3、4 对应的数据客户端可能还没处理，一旦此时 Canal 重启，位点从批次 5 之后开始，批次 3、4 的数据就永久丢失了）。强制顺序 ack 从根本上杜绝了这种"位点空洞"问题。
- `getLastestPositionRange()`/`getFirstPositionRange()`：分别取 `batches.keySet()` 的最大值和最小值对应的 `PositionRange`——因为 batchId 是严格递增分配的，所以最大 batchId 对应最新的一批数据，最小 batchId 对应最老的、最先被分配出去但可能还未被 ack 的一批数据。

至此可以看出，`MemoryMetaManager` 本身逻辑并不复杂，真正精妙的地方在于 `removePositionRange` 里那个"必须最小 batchId 优先"的顺序约束——这是保证数据不丢失的关键防线，也是后面 `FileMixedMetaManager`、`ZooKeeperMetaManager` 复用/重现同样约束的原因（`FileMixedMetaManager` 直接继承 `MemoryMetaManager` 复用这段逻辑，`ZooKeeperMetaManager` 在 ZK 版本里重新实现了同样的检查）。

### 5.3 FileMixedMetaManager：先写内存再定期刷文件

```java
public class FileMixedMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private File                     dataDir;
    private String                   dataFileName = "meta.dat";
    private Map<String, File>        dataFileCaches;
    private ScheduledExecutorService executor;
    private final Position           nullCursor   = new Position() {};
    private long                     period       = 1000; // 单位ms
    private Set<ClientIdentity>      updateCursorTasks;

    public void start() {
        super.start();
        Assert.notNull(dataDir);
        if (!dataDir.exists()) {
            FileUtils.forceMkdir(dataDir);
        }
        dataFileCaches = MigrateMap.makeComputingMap(this::getDataFile);
        executor = Executors.newScheduledThreadPool(1);
        destinations = MigrateMap.makeComputingMap(this::loadClientIdentity);

        cursors = MigrateMap.makeComputingMap(clientIdentity -> {
            Position position = loadCursor(clientIdentity.getDestination(), clientIdentity);
            return position == null ? nullCursor : position; // 返回一个空对象标识，避免出现异常
        });

        updateCursorTasks = Collections.synchronizedSet(new HashSet<>());

        // 启动定时工作任务
        executor.scheduleAtFixedRate(() -> {
            List<ClientIdentity> tasks = new ArrayList<>(updateCursorTasks);
            for (ClientIdentity clientIdentity : tasks) {
                try {
                    updateCursorTasks.remove(clientIdentity);
                    // 定时将内存中的最新值刷到file中，多次变更只刷一次
                    flushDataToFile(clientIdentity.getDestination());
                } catch (Throwable e) {
                    // ignore
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        flushDataToFile();// 刷新数据
        super.stop();
        executor.shutdownNow();
        destinations.clear();
        batches.clear();
    }

    public void subscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.subscribe(clientIdentity);
        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> flushDataToFile(clientIdentity.getDestination()));
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        updateCursorTasks.add(clientIdentity);// 添加到任务队列中进行触发
        super.updateCursor(clientIdentity, position);
    }

    private void flushDataToFile(String destination, File dataFile) {
        FileMetaInstanceData data = new FileMetaInstanceData();
        if (destinations.containsKey(destination)) {
            synchronized (destination.intern()) { // 基于destination控制一下并发更新
                data.setDestination(destination);
                List<FileMetaClientIdentityData> clientDatas = new ArrayList<>();
                for (ClientIdentity clientIdentity : destinations.get(destination)) {
                    FileMetaClientIdentityData clientData = new FileMetaClientIdentityData();
                    clientData.setClientIdentity(clientIdentity);
                    Position position = cursors.get(clientIdentity);
                    if (position != null && position != nullCursor) {
                        clientData.setCursor((LogPosition) position);
                    }
                    clientDatas.add(clientData);
                }
                data.setClientDatas(clientDatas);
            }
            // fixed issue https://github.com/alibaba/canal/issues/4312
            // 客户端数据为空时不覆盖文件内容（适合单客户端）
            if (data.getClientDatas().isEmpty()) {
                return;
            }
            String json = JsonUtils.marshalToString(data);
            FileUtils.writeStringToFile(dataFile, json);
        }
    }
}
```

**这一步在干什么？**

`FileMixedMetaManager` 直接 `extends MemoryMetaManager`，也就是说三个 Map（`destinations`/`cursors`/`batches`）、`MemoryClientIdentityBatch` 的顺序约束等等全部原样复用，它做的增量工作只有一件事：**把内存里的订阅信息和消费位点周期性地落盘成 JSON 文件**，让 canal-server 重启后能够恢复。落盘的文件路径是 `{dataDir}/{destination}/meta.dat`，内容是一个 `FileMetaInstanceData`（包含 destination 名 + 该 destination 下所有客户端的 `FileMetaClientIdentityData` 列表，每个客户端记录自己的 `ClientIdentity` 和 `cursor`）序列化后的 JSON。

值得注意的是它**只持久化订阅信息和 cursor（消费位点），不持久化 batch（未 ack 的批次）**——这一点从代码里可以看出：`batches` 这个 Map 完全沿用父类 `MemoryMetaManager` 的初始化逻辑（在 `start()` 里没有被重新赋值），没有任何从文件加载或刷新到文件的代码路径。这是因为 batch 数据本身生命周期很短（客户端一次 get-ack 循环通常在秒级完成），即使 canal-server 重启导致这部分内存数据丢失，也只是让客户端重新走一次 get 流程，不会造成数据丢失或重复消费——真正决定"数据会不会丢"的是 cursor（已确认消费到哪里）而不是 batch（有没有正在飞行中的一批数据）。

具体的刷新策略上有两个考量点：

1. **debounce（去抖）**：`updateCursor()` 被高频调用（客户端每 ack 一次都可能触发一次 cursor 更新），如果每次都同步写文件，IO 压力会很大。所以这里的策略是把变更过 cursor 的 `clientIdentity` 记录到一个 `Set<ClientIdentity> updateCursorTasks`（`Collections.synchronizedSet` 包装的线程安全集合）里，真正的写文件动作交给一个 `ScheduledExecutorService`，按固定周期（默认 1000ms）扫描这个 Set，对其中每一个 `clientIdentity` 调用一次 `flushDataToFile()`，然后从 Set 中移除。这样无论这 1 秒内 cursor 被更新了多少次，最终只会落盘写一次，是非常典型的"合并多次变更为一次 IO"的去抖设计。
2. **订阅关系变更走异步立即刷新**：`subscribe()`/`unsubscribe()` 这类操作频率远低于 cursor 更新，不需要走去抖，所以直接 `executor.submit()` 提交一个立即执行的刷新任务（而不是等下一个周期）。

另外源码注释里提到的 `issue #4312` 是一个实际线上问题的修复：如果 `clientDatas` 为空（比如某个 destination 下所有客户端都被 unsubscribe 了），`flushDataToFile` 会直接 `return` 而不覆盖已有文件内容。这是为了保护"单客户端场景下重启瞬间 destinations 尚未从文件恢复完成"时不会用一个空列表把历史数据文件冲掉。

### 5.4 PeriodMixedMetaManager：内存 + 定期刷 ZooKeeper

```java
public class PeriodMixedMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private ScheduledExecutorService executor;
    private ZooKeeperMetaManager     zooKeeperMetaManager;
    private final Position           nullCursor = new Position() {};
    private long                     period     = 1000; // 单位ms
    private Set<ClientIdentity>      updateCursorTasks;

    public void start() {
        super.start();
        Assert.notNull(zooKeeperMetaManager);
        if (!zooKeeperMetaManager.isStart()) {
            zooKeeperMetaManager.start();
        }

        executor = Executors.newScheduledThreadPool(1);
        destinations = MigrateMap.makeComputingMap(destination -> zooKeeperMetaManager.listAllSubscribeInfo(destination));

        cursors = MigrateMap.makeComputingMap(clientIdentity -> {
            Position position = zooKeeperMetaManager.getCursor(clientIdentity);
            return position == null ? nullCursor : position;
        });

        batches = MigrateMap.makeComputingMap(clientIdentity -> {
            // 读取一下zookeeper信息，初始化一次
            MemoryClientIdentityBatch batches = MemoryClientIdentityBatch.create(clientIdentity);
            Map<Long, PositionRange> positionRanges = zooKeeperMetaManager.listAllBatchs(clientIdentity);
            for (Map.Entry<Long, PositionRange> entry : positionRanges.entrySet()) {
                batches.addPositionRange(entry.getValue(), entry.getKey());
            }
            return batches;
        });

        updateCursorTasks = Collections.synchronizedSet(new HashSet<>());

        executor.scheduleAtFixedRate(() -> {
            List<ClientIdentity> tasks = new ArrayList<>(updateCursorTasks);
            for (ClientIdentity clientIdentity : tasks) {
                try {
                    updateCursorTasks.remove(clientIdentity);
                    // 定时将内存中的最新值刷到zookeeper中，多次变更只刷一次
                    zooKeeperMetaManager.updateCursor(clientIdentity, getCursor(clientIdentity));
                } catch (Throwable e) {
                    // ignore
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void subscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.subscribe(clientIdentity);
        executor.submit(() -> zooKeeperMetaManager.subscribe(clientIdentity));
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        super.updateCursor(clientIdentity, position);
        updateCursorTasks.add(clientIdentity);
    }
}
```

**这一步在干什么？**

`PeriodMixedMetaManager` 的结构与 `FileMixedMetaManager` 几乎是镜像的——同样继承 `MemoryMetaManager`，同样用"内存优先、周期性刷远端存储"的模式，只是远端存储从本地文件换成了 ZooKeeper（通过内部持有的一个 `ZooKeeperMetaManager` 实例委派实际的 ZK 读写）。三个 Map 的构造方式也几乎一致，唯一的区别是 `destinations`/`cursors`/`batches` 三个 `MigrateMap` 的计算函数改成了从 `zooKeeperMetaManager` 里读取初始值，而不是从本地文件读取。

但源码类注释里点出了两处与 `FileMixedMetaManager` 不同的关键优化：

1. **去除 batch 数据刷新到 ZK**：注释原文是"去除 batch 数据刷新到 zk 中，切换时 batch 数据可忽略，重新从头开始获取"。也就是说 `addBatch`/`removeBatch` 完全没有重写，全部使用父类 `MemoryMetaManager` 的纯内存实现——`batches` 这个 Map 只在 `start()` 时从 ZK 拉取一次初始快照，运行期间对 batch 的所有增删都只发生在内存里，不会再写回 ZK。这与旧版本的 `MixedMetaManager`（见下文 5.6）形成了鲜明对比——`MixedMetaManager` 会为每一次 `addBatch`/`removeBatch` 都异步提交一次 ZK 写操作，而 `PeriodMixedMetaManager` 认为这种高频的 ZK 写没有必要，因为 batch 本身生命周期短、丢了可以重新分配，属于典型的"为性能牺牲一点点极端情况下的重复消费概率"的权衡。
2. **cursor 走跟 `FileMixedMetaManager` 一样的定时合并刷新策略**：同样用 `updateCursorTasks` 这个 Set 做去抖，只是刷新目标变成了 `zooKeeperMetaManager.updateCursor()`。这里能看出 Canal 在文件版和 ZK 版之间刻意保持了几乎一致的代码结构和优化思路，只是把最终持久化的介质换掉了。

`default-instance.xml` 里配置的 `metaManager` 正是 `PeriodMixedMetaManager`，说明官方默认（分布式部署、依赖 ZK 做 HA 协调）场景下，Canal 团队选择的是"cursor 定期刷 ZK、batch 完全放内存不做持久化"这套折中方案。

### 5.5 ZooKeeperMetaManager：ZK 存储路径与批次自动分配

```java
public class ZooKeeperMetaManager extends AbstractCanalLifeCycle implements CanalMetaManager {

    private ZkClientx zkClientx;

    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        String path = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(), clientIdentity.getClientId());
        byte[] data = JsonUtils.marshalToByte(positionRange, JSONWriter.Feature.WriteClassName);
        String batchPath = zkClientx.createPersistentSequential(path + ZookeeperPathUtils.ZOOKEEPER_SEPARATOR, data, true);
        String batchIdString = StringUtils.substringAfterLast(batchPath, ZookeeperPathUtils.ZOOKEEPER_SEPARATOR);
        return ZookeeperPathUtils.getBatchMarkId(batchIdString);
    }

    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        String batchsPath = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(), clientIdentity.getClientId());
        List<String> nodes = zkClientx.getChildren(batchsPath);
        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }
        // 找到最小的Id
        ArrayList<Long> batchIds = new ArrayList<>(nodes.size());
        for (String batchIdString : nodes) {
            batchIds.add(Long.valueOf(batchIdString));
        }
        Long minBatchId = Collections.min(batchIds);
        if (!minBatchId.equals(batchId)) {
            // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
            throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d", batchId, minBatchId));
        }
        if (!batchIds.contains(batchId)) {
            return null;
        }
        PositionRange positionRange = getBatch(clientIdentity, batchId);
        if (positionRange != null) {
            String path = ZookeeperPathUtils.getBatchMarkWithIdPath(clientIdentity.getDestination(), clientIdentity.getClientId(), batchId);
            zkClientx.delete(path);
        }
        return positionRange;
    }

    public PositionRange getLastestBatch(ClientIdentity clientIdentity) {
        String path = ZookeeperPathUtils.getBatchMarkPath(clientIdentity.getDestination(), clientIdentity.getClientId());
        List<String> nodes = zkClientx.getChildren(path);
        if (CollectionUtils.isEmpty(nodes)) {
            return null;
        }
        ArrayList<Long> batchIds = new ArrayList<>(nodes.size());
        for (String batchIdString : nodes) {
            batchIds.add(Long.valueOf(batchIdString));
        }
        Long maxBatchId = Collections.max(batchIds);
        PositionRange result = getBatch(clientIdentity, maxBatchId);
        if (result == null) { // 出现为null，说明zk节点有变化，重新获取
            return getLastestBatch(clientIdentity);
        } else {
            return result;
        }
    }
}
```

**这一步在干什么？**

`ZooKeeperMetaManager` 是 Meta 模块里唯一直接对接 ZooKeeper 存储的实现，它类注释里画出的存储路径结构是理解一切的钥匙：

```
/otter
   canal
     destinations
       dest1
         client1
           filter
           batch_mark
             1
             2
             3
```

也就是说，路径的层级是 `/otter/canal/destinations/{destination}/{clientId}/...`，每个客户端节点下面有两类子节点：`filter`（该客户端订阅时提交的过滤表达式，一个持久节点，内容就是过滤正则字符串本身）和 `batch_mark`（一个持久节点，下面挂着若干个数字命名的子节点，每个数字就是一个 batchId，节点内容是该批次对应的 `PositionRange` 序列化 JSON）。

三组 API 分别落到不同的路径操作上：

- **订阅管理**：`subscribe()` 创建 `getClientIdNodePath()` 对应的持久节点（客户端节点本身），再在 `filter` 子路径写入过滤表达式；`unsubscribe()` 直接 `deleteRecursive()` 递归删除整个客户端节点；`listAllSubscribeInfo()` 通过 `zkClientx.getChildren()` 列出 destination 节点下所有数字命名的子节点（clientId），逐个读取对应的 filter 节点内容后组装成 `ClientIdentity` 列表。
- **消费位点**：`getCursor()`/`updateCursor()` 直接读写 `getCursorPath()` 对应的单个持久节点，内容是 `Position` 的 JSON 序列化（这里用了 fastjson2 的 `WriteClassName` 特性写入类型信息，因为 `Position` 有 `LogPosition` 等多态子类，反序列化时需要知道具体类型）。`updateCursor()` 用 `try { writeData } catch (ZkNoNodeException) { createPersistent }` 的模式处理"节点可能第一次还不存在"的情况。
- **批次管理**：这是全篇最精彩的部分。`addBatch()`（不指定 batchId 的重载）调用的是 `zkClientx.createPersistentSequential()`——**ZooKeeper 原生的顺序节点特性**：创建一个以指定前缀命名的持久顺序节点，ZK 会自动在节点名后面追加一个全局单调递增的十位数字后缀（比如 `batch_mark/0000000001`），这天然就是一个分布式环境下无冲突的自增 ID 生成器，不需要像 `MemoryMetaManager` 那样自己维护一个 `AtomicLong`。拿到实际创建出的节点全路径后，用 `StringUtils.substringAfterLast()` 截取出数字后缀字符串，再通过 `ZookeeperPathUtils.getBatchMarkId()` 解析成 `Long` 返回给调用方作为 batchId。

`removeBatch()` 重新实现了与 `MemoryClientIdentityBatch.removePositionRange()` 完全相同的"必须最小 batchId 优先"顺序检查逻辑（代码注释文字都一字不差），只是数据源从内存 Map 换成了 `zkClientx.getChildren()` 列出的子节点名称集合。这说明"顺序 ack"是这套 Meta 体系里贯穿所有实现的不变量，不因存储介质而改变。

`getLastestBatch()`/`getFirstBatch()`/`listAllBatchs()` 三个方法有一个共同的、很容易被忽略的防御性设计——**递归重试**。以 `getLastestBatch()` 为例：先列出所有子节点找出最大 batchId，再调用 `getBatch()` 去读取该 batchId 对应节点的实际内容；但如果两次 ZK 访问之间，这个节点恰好被另一个并发的 `removeBatch()` 调用删除掉了（分布式环境下完全可能发生），`getBatch()` 就会读到 `null`。此时代码没有直接返回 `null` 或抛异常，而是**直接递归调用自己 `return getLastestBatch(clientIdentity)`**，相当于重新走一遍"列子节点 + 取最大值 + 读内容"的完整流程，直到某次调用里读取到的数据和列出的子节点保持一致为止。这是一种简单但有效的"乐观重试"模式，专门用来对抗"ZK 节点在读取过程中被并发删除"这类竞态条件，避免因为一次瞬时的不一致就让调用方拿到错误的 null 结果。

### 5.6 MixedMetaManager：早期的同步内存 + 异步全量透传 ZK 方案

```java
public class MixedMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private ExecutorService      executor;
    private ZooKeeperMetaManager zooKeeperMetaManager;

    public Long addBatch(final ClientIdentity clientIdentity, final PositionRange positionRange) throws CanalMetaManagerException {
        final Long batchId = super.addBatch(clientIdentity, positionRange);
        // 异步刷新
        executor.submit(() -> zooKeeperMetaManager.addBatch(clientIdentity, positionRange, batchId));
        return batchId;
    }

    public PositionRange removeBatch(final ClientIdentity clientIdentity, final Long batchId) throws CanalMetaManagerException {
        PositionRange positionRange = super.removeBatch(clientIdentity, batchId);
        // 异步刷新
        executor.submit(() -> zooKeeperMetaManager.removeBatch(clientIdentity, batchId));
        return positionRange;
    }
}
```

**这一步在干什么？**

`MixedMetaManager` 是这五种实现里最早期、最"朴素"的一版组合方案，类注释写的就是"组合 memory + zookeeper 的使用模式"。它的策略与 `PeriodMixedMetaManager` 形成鲜明对比：**每一个写操作（subscribe/unsubscribe/updateCursor/addBatch/addBatch(带batchId)/removeBatch/clearAllBatchs）都会在同步完成内存写入后，立即 `executor.submit()` 提交一个异步任务把同样的操作原样透传给 `zooKeeperMetaManager`**，包括高频的 batch 增删也不例外。

这意味着：

- 内存操作永远是同步、立即返回的（保证读写接口的响应速度）；
- ZK 写入永远是异步、fire-and-forget 的（用一个单线程 `ExecutorService` 顺序执行，不阻塞调用方，但也不等待/校验 ZK 写入是否成功）；
- 与 `PeriodMixedMetaManager` 相比，它没有做任何"合并多次变更"的去抖优化，也没有放弃 batch 数据的 ZK 持久化——是一种更早期、更简单直接、但 ZK 写压力也更大的实现方式。

从代码演进的角度看，`PeriodMixedMetaManager` 可以视为 `MixedMetaManager` 的优化版：既然 batch 数据丢了代价很小（重新分配即可），就没必要为每次 batch 变更都触发一次 ZK 写入；既然 cursor 频繁变更但只关心"最新值"，就可以用去抖策略合并多次写入为一次。这也是本章通过五种实现的演进关系，能看到 Canal 团队在"一致性强度"与"性能开销"之间反复权衡取舍的一个缩影。

---

## 第六章：LogPositionManager —— 位点管理

如果说 Meta 模块管理的是"每个**客户端**消费到哪里了"，那么 `LogPositionManager` 管理的是完全不同维度的另一件事："Parser 自己应该从 Binlog 的哪个位置开始解析"。这是 canal-server 作为一个整体（而不是某个具体客户端）对上游 MySQL 的位点记录，接口定义在 `parse` 模块：

```java
public interface CanalLogPositionManager extends CanalLifeCycle {

    LogPosition getLatestIndexBy(String destination);

    void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException;

}
```

**这一步在干什么？**

接口只有两个方法：`getLatestIndexBy()` 在 Parser 启动（或异常重启）时被调用，用来确定"这次应该从哪个 binlog 文件 + 哪个 offset 开始订阅"；`persistLogPosition()` 在 Parser 每成功处理完一个事务后被调用，把这个事务结束位置持久化下来。二者的语义正好是一对"读位点、写位点"。

与上一章 Meta 模块里五花八门的实现类似，`CanalLogPositionManager` 也有多种实现，全部继承自一个几乎不做任何事的抽象基类：

```java
public abstract class AbstractLogPositionManager extends AbstractCanalLifeCycle implements CanalLogPositionManager {
}
```

`AbstractLogPositionManager` 只是继承了 `AbstractCanalLifeCycle`（提供 `start`/`stop`/`isStart` 的默认布尔标志位实现），本身不含任何业务逻辑，纯粹是为了让所有实现类共享统一的生命周期管理骨架。

### 6.1 MemoryLogPositionManager：纯内存实现

```java
public class MemoryLogPositionManager extends AbstractLogPositionManager {

    private Map<String, LogPosition> positions;

    @Override
    public void start() {
        super.start();
        positions = new MapMaker().makeMap();
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        return positions.get(destination);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        positions.put(destination, logPosition);
    }

    public Set<String> destinations() {
        return positions.keySet();
    }

}
```

**这一步在干什么？**

最简单的实现，一个 `Map<String destination, LogPosition>`，`getLatestIndexBy`/`persistLogPosition` 就是直接的 get/put，没有任何持久化能力——一旦 canal-server 进程重启，位点信息全部丢失，Parser 会退化为从 `masterPosition`（Instance 配置里手工指定的起始位置，如果配置了的话）或直接从当前最新 binlog 位置开始订阅。它是最基础的构件，会被后面的 `FailbackLogPositionManager`、`FileMixedLogPositionManager`、`PeriodMixedLogPositionManager` 作为"内存这一级缓存"来复用（这些组合实现内部都持有一个 `MemoryLogPositionManager` 实例，而不是重新自己维护一个 Map）。额外暴露的 `destinations()` 方法就是专门给这些包装类遍历"当前内存里已经记录了哪些 destination 的位点"用的，比如上面 `FileMixedLogPositionManager.flushDataToFile()` 就是靠它决定要遍历刷新哪些 destination。

### 6.2 ZooKeeperLogPositionManager：单节点存储

```java
public class ZooKeeperLogPositionManager extends AbstractLogPositionManager {

    private final ZkClientx zkClientx;

    public ZooKeeperLogPositionManager(ZkClientx zkClient){
        if (zkClient == null) {
            throw new NullPointerException("null zkClient");
        }
        this.zkClientx = zkClient;
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        String path = ZookeeperPathUtils.getParsePath(destination);
        byte[] data = zkClientx.readData(path, true);
        if (data == null || data.length == 0) {
            return null;
        }
        return JsonUtils.unmarshalFromByte(data, LogPosition.class);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        String path = ZookeeperPathUtils.getParsePath(destination);
        byte[] data = JsonUtils.marshalToByte(logPosition);
        try {
            zkClientx.writeData(path, data);
        } catch (ZkNoNodeException e) {
            zkClientx.createPersistent(path, data, true);
        }
    }

}
```

**这一步在干什么？**

与 `ZooKeeperMetaManager` 里"每个 batch 一个独立的顺序节点"的丰富结构不同，`ZooKeeperLogPositionManager` 的存储结构极其简单：每个 destination 只对应**一个**持久节点（路径由 `ZookeeperPathUtils.getParsePath(destination)` 给出），节点内容就是当前 `LogPosition` 序列化后的 JSON。因为 Parser 的位点是"全局唯一的、单调推进的一条线"（不像客户端消费位点那样每个客户端各有各的进度），所以不需要像 batch 那样维护一组历史记录，直接覆盖写最新值即可。`persistLogPosition` 同样用 `try writeData catch ZkNoNodeException createPersistent` 的模式处理第一次节点不存在的情况。

### 6.3 FailbackLogPositionManager：primary/secondary 两级降级

```java
public class FailbackLogPositionManager extends AbstractLogPositionManager {

    private final CanalLogPositionManager primary;
    private final CanalLogPositionManager secondary;

    public FailbackLogPositionManager(CanalLogPositionManager primary, CanalLogPositionManager secondary){
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public void start() {
        super.start();
        if (!primary.isStart()) {
            primary.start();
        }
        if (!secondary.isStart()) {
            secondary.start();
        }
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = primary.getLatestIndexBy(destination);
        if (logPosition != null) {
            return logPosition;
        }
        return secondary.getLatestIndexBy(destination);
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        try {
            primary.persistLogPosition(destination, logPosition);
        } catch (CanalParseException e) {
            logger.warn("persistLogPosition use primary log position manager exception. destination: {}, logPosition: {}",
                destination, logPosition, e);
            secondary.persistLogPosition(destination, logPosition);
        }
    }
}
```

**这一步在干什么？**

这是一个纯粹的"组合模式 + 降级策略"实现，不关心 `primary`/`secondary` 具体是什么类型的 `CanalLogPositionManager`（这一点通过构造函数注入两个接口类型的字段体现），只负责编排二者的调用顺序：

- **读取（`getLatestIndexBy`）**：优先查 `primary`，只有 `primary` 返回 `null`（即找不到记录）时才去查 `secondary`。这是"能查到就用最新的，查不到就退而求其次"的语义，类注释里给出的典型应用场景是"针对内存 buffer，出现 HA 切换，先尝试从内存 buffer 区中找到 lastest position，如果不存在才尝试找一下 meta 里消费的信息"——也就是说 `primary` 通常是速度快但可能刚重启丢数据的 `MemoryLogPositionManager`，`secondary` 通常是慢一点但更可靠的 `MetaLogPositionManager` 或 ZK 实现。
- **写入（`persistLogPosition`）**：只在 `primary` 写入抛出 `CanalParseException` 异常时才降级写入 `secondary`，正常情况下只写 `primary`，不会双写。

源码类注释里的场景描述精准点出了它的用武之地：Parser 在正常运行时位点全部走内存（`primary`），一旦发生 HA 切换或进程重启导致内存丢失，`getLatestIndexBy` 从 `primary` 查不到就自动 fallback 到 `secondary`（通常是 `MetaLogPositionManager`，见下一节），保证即使内存丢失也能找到一个合理的"退而求其次"的起点，而不是直接从头开始重新订阅整个 binlog。

`file-instance.xml` 和 `default-instance.xml` 里配置的 `logPositionManager` bean 都是这个模式的具体应用：`FailbackLogPositionManager(MemoryLogPositionManager, MetaLogPositionManager(metaManager))`。

### 6.4 MetaLogPositionManager：借道 Meta 模块、以慢打快

```java
public class MetaLogPositionManager extends AbstractLogPositionManager {

    private final CanalMetaManager metaManager;

    public MetaLogPositionManager(CanalMetaManager metaManager){
        this.metaManager = metaManager;
    }

    @Override
    public void start() {
        super.start();
        if (!metaManager.isStart()) {
            metaManager.start();
        }
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        List<ClientIdentity> clientIdentities = metaManager.listAllSubscribeInfo(destination);
        LogPosition result = null;
        if (!CollectionUtils.isEmpty(clientIdentities)) {
            // 尝试找到一个最小的logPosition
            for (ClientIdentity clientIdentity : clientIdentities) {
                LogPosition position = (LogPosition) metaManager.getCursor(clientIdentity);
                if (position == null) {
                    continue;
                }
                if (result == null) {
                    result = position;
                } else {
                    result = CanalEventUtils.min(result, position);
                }
            }
        }
        return result;
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        // do nothing
        logger.info("destination [{}] persist LogPosition:{}", destination, logPosition);
    }
}
```

**这一步在干什么？**

这是本章设计思想最精妙的一个实现。`MetaLogPositionManager` 自己完全不存储任何位点数据（`persistLogPosition` 是空实现，只打了一行日志），它把"Parser 应该从哪里开始"这个问题，转化成了"**去问一遍所有订阅了这个 destination 的客户端，他们各自消费到哪儿了，取其中最慢的那一个**"。

具体做法是调用 `metaManager.listAllSubscribeInfo(destination)` 拿到该 destination 下所有客户端的 `ClientIdentity` 列表，再逐个调用 `metaManager.getCursor(clientIdentity)` 拿到每个客户端最新确认的消费位置，用 `CanalEventUtils.min()` 逐一比较取出**全局最小**的那个 `LogPosition` 作为返回结果。

为什么要取最小值而不是最大值？这里体现的是一个非常朴素但关键的正确性原则——**以慢打快**：假如 Parser 重启后从某个客户端已经消费过的位置（比如最快的那个客户端的位置）继续订阅，那么还没跟上进度的、比较慢的客户端将永远失去读取到那部分被跳过数据的机会，造成事实上的丢数据。只有从**所有客户端里进度最慢的那个位置**继续订阅，才能保证不会有任何一个客户端的数据被意外跳过——宁可让快的客户端重复收到一些已经处理过的数据（这些重复数据可以通过客户端自身的幂等处理消化），也绝不能让慢的客户端漏掉数据。

正因为这种"必须依赖 metaManager 的真实消费记录"的设计，`initLogPositionManager()` 里对 `IndexMode.META` 有一个额外的防御性检查：如果当前配置的 `metaManager` 恰好是纯内存的 `MemoryMetaManager`，直接抛出 `CanalException` 拒绝创建 `MetaLogPositionManager`。原因很直白：如果 metaManager 本身都是内存实现（重启就清空），那么"去问 metaManager 里所有客户端消费到哪儿了"这个操作本身就是没有意义的——两者都会在同一次重启中同时丢失数据，无法起到"以 metaManager 的持久化位点作为兜底"的效果。这是一处"配置组合冲突检测"的典型例子，在 3.8 节已经详细分析过对应源码。

### 6.5 FileMixedLogPositionManager 与 PeriodMixedLogPositionManager：文件/ZK 周期刷新变体

```java
public class FileMixedLogPositionManager extends AbstractLogPositionManager {

    private File                     dataDir;
    private Map<String, File>        dataFileCaches;
    private ScheduledExecutorService executorService;
    private final LogPosition        nullPosition = new LogPosition() {};
    private MemoryLogPositionManager memoryLogPositionManager;
    private long                     period;
    private Set<String>              persistTasks;

    @Override
    public void start() {
        super.start();
        if (!memoryLogPositionManager.isStart()) {
            memoryLogPositionManager.start();
        }
        // 启动定时工作任务
        executorService.scheduleAtFixedRate(() -> {
            List<String> tasks = new ArrayList<>(persistTasks);
            for (String destination : tasks) {
                try {
                    // 定时将内存中的最新值刷到file中，多次变更只刷一次
                    flushDataToFile(destination);
                    persistTasks.remove(destination);
                } catch (Throwable e) {
                    // ignore
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = memoryLogPositionManager.getLatestIndexBy(destination);
        if (logPosition != null) {
            return logPosition;
        }
        logPosition = loadDataFromFile(dataFileCaches.get(destination));
        return logPosition == null ? nullPosition : logPosition;
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        persistTasks.add(destination);
        memoryLogPositionManager.persistLogPosition(destination, logPosition);
    }
}
```

```java
public class PeriodMixedLogPositionManager extends AbstractLogPositionManager {

    private MemoryLogPositionManager    memoryLogPositionManager;
    private ZooKeeperLogPositionManager zooKeeperLogPositionManager;
    private ScheduledExecutorService    executorService;
    private long                        period;
    private Set<String>                 persistTasks;

    public PeriodMixedLogPositionManager(MemoryLogPositionManager memoryLogPositionManager,
                                         ZooKeeperLogPositionManager zooKeeperLogPositionManager, long period){
        this.memoryLogPositionManager = memoryLogPositionManager;
        this.zooKeeperLogPositionManager = zooKeeperLogPositionManager;
        this.period = period;
        this.persistTasks = Collections.synchronizedSet(new HashSet<>());
        this.executorService = Executors.newScheduledThreadPool(1);
    }

    @Override
    public void start() {
        super.start();
        // 启动定时工作任务
        executorService.scheduleAtFixedRate(() -> {
            List<String> tasks = new ArrayList<>(persistTasks);
            for (String destination : tasks) {
                try {
                    // 定时将内存中的最新值刷到zookeeper中，多次变更只刷一次
                    zooKeeperLogPositionManager.persistLogPosition(destination, getLatestIndexBy(destination));
                    persistTasks.remove(destination);
                } catch (Throwable e) {
                    // ignore
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = memoryLogPositionManager.getLatestIndexBy(destination);
        return logPosition == nullPosition ? null : logPosition;
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        persistTasks.add(destination);
        memoryLogPositionManager.persistLogPosition(destination, logPosition);
    }
}
```

**这一步在干什么？**

这两个类的结构和 Meta 模块里的 `FileMixedMetaManager`/`PeriodMixedMetaManager` 如出一辙，都是"内存优先 + 周期性去抖刷新到持久介质"的模式，唯一的区别是这里包装的不是自己继承 `MemoryMetaManager`，而是**组合持有**一个 `MemoryLogPositionManager` 实例（构造函数注入）：

- `FileMixedLogPositionManager`：`persistLogPosition()` 把 destination 记录到 `persistTasks`（去抖 Set）里，同时立即同步写入内存版；`getLatestIndexBy()` 优先读内存，内存没有再去读本地文件缓存（`loadDataFromFile`）。后台 `ScheduledExecutorService` 定时扫描 `persistTasks`，把内存中的最新位点序列化写入 `{dataDir}/{destination}/parse.dat` 文件（对比 Meta 模块的 `meta.dat`，这里文件名是 `parse.dat`，语义上专指"Parser 自己的解析位点"而非"客户端消费位点"，两者存储在不同文件里互不干扰）。
- `PeriodMixedLogPositionManager`：同样的去抖 `persistTasks` 集合，只是刷新目标换成 `zooKeeperLogPositionManager.persistLogPosition()`，写入的是 6.2 节介绍的那个单节点 ZK 路径。

两者都体现了与 Meta 模块完全一致的设计哲学——**内存承担高频读写的性能压力，持久化介质只在周期性窗口内合并写入一次最新值**，这种"去抖 + 定时批量刷盘"的模式在 Canal 全代码库里反复出现，是一种非常值得复用的通用工程套路。

至此，位点管理体系里所有的具体实现类型都已经介绍完毕。回顾 3.8 节 `initLogPositionManager()` 的五个分支，可以看到它们分别对应：MEMORY → `MemoryLogPositionManager`（6.1）；ZOOKEEPER → `ZooKeeperLogPositionManager`（6.2）；MIXED → `PeriodMixedLogPositionManager`（6.5）；META → `MetaLogPositionManager`（6.4）；MEMORY_META_FAILBACK → `FailbackLogPositionManager(Memory, Meta)`（6.3 组合 6.1 与 6.4）。五种模式的排列组合，本质上都是在"性能"（内存）、"持久性"（文件/ZK）、"正确性兜底"（借道 Meta 以慢打快）三个维度之间做取舍。

---

## 第七章：位点安全设计总结

前面两章分别介绍了 Meta 模块（客户端消费位点）和 LogPositionManager（Parser 解析位点）各自的存储实现，但还没有回答一个更本质的问题：**Parser 到底在什么时刻才会去调用 `persistLogPosition()`？** 这个时机选择得对不对，直接决定了 Canal 在异常重启后会不会丢数据、会不会产生数据错乱。这一章把散落在 `EventTransactionBuffer` 和 `AbstractEventParser` 里的相关代码串起来，还原"位点只在事务边界持久化"这一安全设计的全貌。

### 7.1 EventTransactionBuffer：只在事务完整时才触发 flush

```java
public class EventTransactionBuffer extends AbstractCanalLifeCycle {

    private static final long        INIT_SQEUENCE = -1;
    private int                      bufferSize    = 1024;
    private int                      indexMask;
    private CanalEntry.Entry[]       entries;

    private AtomicLong               putSequence   = new AtomicLong(INIT_SQEUENCE); // 代表当前put操作最后一次写操作发生的位置
    private AtomicLong               flushSequence = new AtomicLong(INIT_SQEUENCE); // 代表满足flush条件后最后一次数据flush的时间

    private TransactionFlushCallback flushCallback;

    public void add(CanalEntry.Entry entry) throws InterruptedException {
        switch (entry.getEntryType()) {
            case TRANSACTIONBEGIN:
                flush();// 刷新上一次的数据
                put(entry);
                break;
            case TRANSACTIONEND:
                put(entry);
                flush();
                break;
            case ROWDATA:
                put(entry);
                // 针对非DML的数据，直接输出，不进行buffer控制
                EventType eventType = entry.getHeader().getEventType();
                if (eventType != null && !isDml(eventType)) {
                    flush();
                }
                break;
            case HEARTBEAT:
                // master过来的heartbeat，说明binlog已经读完了，是idle状态
                put(entry);
                flush();
                break;
            default:
                break;
        }
    }

    public void reset() {
        putSequence.set(INIT_SQEUENCE);
        flushSequence.set(INIT_SQEUENCE);
    }

    private void flush() throws InterruptedException {
        long start = this.flushSequence.get() + 1;
        long end = this.putSequence.get();

        if (start <= end) {
            List<CanalEntry.Entry> transaction = new ArrayList<>();
            for (long next = start; next <= end; next++) {
                transaction.add(this.entries[getIndex(next)]);
            }

            flushCallback.flush(transaction);
            flushSequence.set(end);// flush成功后，更新flush位置
        }
    }
}
```

**这一步在干什么？**

`EventTransactionBuffer` 是一个环形缓冲区（ring buffer），本质上是 Binlog 解析出来的原始 `Entry` 事件流和"按事务打包成一批"这个语义之间的转换器。它的核心是两个游标 `putSequence`（已写入的最新位置）和 `flushSequence`（已刷新的最新位置），`add()` 方法根据事件类型决定何时调用 `flush()`：

- `TRANSACTIONBEGIN`（事务开始）：先 `flush()` 把上一个事务积累但还没刷出去的数据清空（正常情况下不应该有残留，这里是防御性调用），再 `put()` 把这个 BEGIN 事件本身放入缓冲区——**注意 flush 在 put 之前**，也就是说事务开始事件本身不会立即触发刷新，它只是作为新一轮缓冲的起点先存进去。
- `TRANSACTIONEND`（事务结束）：先 `put()` 把 END 事件放入缓冲区，再调用 `flush()`——**这才是唯一会把一个完整事务（从 BEGIN 到 END 之间所有 ROWDATA）真正推送给下游的时机**。
- `ROWDATA`：正常的 DML（INSERT/UPDATE/DELETE）只是 `put()` 存入缓冲区，并不触发 flush，必须等到后续的 TRANSACTIONEND 才会被一起刷出去；但如果是非 DML 类型的行数据（比如 DDL），则会立即 `flush()`，因为 DDL 语句通常不在标准的 BEGIN/COMMIT 事务包裹之内，需要单独即时处理。
- `HEARTBEAT`：心跳事件同样立即 `put()` 后 `flush()`，因为心跳的语义就是"Binlog 已经读到最新、没有更多数据了"，需要让下游及时感知到 Parser 处于 idle 状态。

这个设计最关键的一点是：**只有 `flush()` 被调用时，`flushCallback.flush(transaction)` 才会被执行，而只有这个回调执行完毕，才有可能触发后续的位点持久化**。而 `flush()` 只会在 TRANSACTIONBEGIN（清空上一批）、TRANSACTIONEND（正常提交）、非 DML 的 ROWDATA、HEARTBEAT 这几个语义边界上被调用。这意味着一个事务中间的、尚未提交完成的 ROWDATA 永远不会单独触发 flush，也就永远不会有机会让下游据此去持久化一个"事务中间态"的位点——这是"位点只在事务边界持久化"的第一层保障，在缓冲区这一级就已经确立了。

### 7.2 AbstractEventParser 的 flush 回调：消费成功才持久化位点

```java
public AbstractEventParser(){
    // 初始化一下
    transactionBuffer = new EventTransactionBuffer(transaction -> {
        boolean successed = consumeTheEventAndProfilingIfNecessary(transaction);
        if (!running) {
            return;
        }

        if (!successed) {
            throw new CanalParseException("consume failed!");
        }

        LogPosition position = buildLastTransactionPosition(transaction);
        if (position != null) { // 可能position为空
            logPositionManager.persistLogPosition(AbstractEventParser.this.destination, position);
        }
    });
}
```

**这一步在干什么？**

这段代码是整个"位点安全设计"的心脏。`AbstractEventParser` 构造函数里把一个 lambda 表达式作为 `TransactionFlushCallback` 注册进 `transactionBuffer`，这个回调会在 7.1 节讲到的每次 `flush()` 被调用时执行，逻辑分三步：

1. `consumeTheEventAndProfilingIfNecessary(transaction)`：把这一批事件（一个完整事务，或者一条 DDL/心跳）真正投递给下游——即调用 `eventSink.sink()`，也就是本系列文档第 4 篇讲到的 Sink 过滤与 Store 环形缓冲区那一整套流程。这一步如果因为 Store 缓冲区满、下游异常等原因失败，返回 `false`。
2. 只有 `successed == true`（下游确认消费成功）才会继续往下走。如果消费失败，直接 `throw new CanalParseException`，**根本不会执行 `persistLogPosition`**——这是保证"位点只在数据确实已经安全送达 Store 之后才推进"的关键判断，避免出现"位点已经往前记了、但数据其实还没真正进到 Store"这种数据丢失的窗口。
3. 消费成功后，调用 `buildLastTransactionPosition(transaction)` 从这批数据里找出最后一个事务结束点对应的位置，再调用 `logPositionManager.persistLogPosition()` 落盘。

也就是说，从 7.1 到 7.2，位点持久化的完整链路是：**事务边界触发 flush → flush 回调里先投递给 Sink → Sink 确认消费成功 → 才计算并持久化位点**。任何一环失败，位点都不会往前推进，Parser 仍然停留在上一个已确认安全的位置。

### 7.3 buildLastTransactionPosition：只认最后一个 TRANSACTIONEND

```java
protected LogPosition buildLastTransactionPosition(List<CanalEntry.Entry> entries) { // 初始化一下
    for (int i = entries.size() - 1; i > 0; i--) {
        CanalEntry.Entry entry = entries.get(i);
        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {// 尽量记录一个事务做为position
            return buildLastPosition(entry);
        }
    }

    return null;
}

protected LogPosition buildLastPosition(CanalEntry.Entry entry) { // 初始化一下
    LogPosition logPosition = new LogPosition();
    EntryPosition position = new EntryPosition();
    position.setJournalName(entry.getHeader().getLogfileName());
    position.setPosition(entry.getHeader().getLogfileOffset());
    position.setTimestamp(entry.getHeader().getExecuteTime());
    position.setServerId(entry.getHeader().getServerId());
    position.setGtid(entry.getHeader().getGtid());

    logPosition.setPostion(position);

    LogIdentity identity = new LogIdentity(runningInfo.getAddress(), -1L);
    logPosition.setIdentity(identity);
    return logPosition;
}
```

**这一步在干什么？**

`buildLastTransactionPosition` 从这一批即将被持久化的 `entries` 列表**倒序**遍历，找到的第一个（也就是原列表里最后一个）`TRANSACTIONEND` 类型的 Entry，把它的 Binlog 文件名、offset、执行时间、serverId、GTID 等信息包装成一个 `LogPosition` 返回。如果这批数据里根本没有 TRANSACTIONEND（比如这批数据只是一条心跳或者 DDL），则返回 `null`，调用方 `logPositionManager.persistLogPosition()` 的调用会被跳过（"可能 position 为空"的注释即指此情况）。

这里选择"只认 TRANSACTIONEND"而不是直接用这批数据里最后一条 Entry 的位置，原因在于：一批被 flush 出去的数据中，最后一条不一定恰好是 TRANSACTIONEND（比如 flush 是被非 DML 的 ROWDATA 或者 HEARTBEAT 触发的），如果不加区分地把"这批数据的最后一条"当作位点，就可能把位点错误地记录在一个事务内部、尚未提交完成的中间位置上。只有明确找到 TRANSACTIONEND，才能保证记录下来的位点对应的是一个**完整、已提交的事务边界**，这样即使 Canal 在下一秒崩溃重启，从这个位点重新订阅 Binlog 时，得到的永远是"下一个事务的开始"，不会出现从一个事务的中间数据开始解析、导致该事务数据不完整或被重复处理一半的情况。

### 7.4 异常路径：transactionBuffer.reset() 丢弃未提交数据

```java
// 出异常了，退出sink消费，释放一下状态
eventSink.interrupt();
transactionBuffer.reset();// 重置一下缓冲队列，重新记录数据
binlogParser.reset();// 重新置位
if (multiStageCoprocessor != null && multiStageCoprocessor.isStart()) {
    try {
        multiStageCoprocessor.stop();
    } catch (Throwable t) {
        logger.debug("multi processor rejected:", t);
    }
}

if (running) {
    // sleep一段时间再进行重试
    try {
        Thread.sleep(10000 + RandomUtils.nextInt(10000));
    } catch (InterruptedException e) {
    }
    // ... 重新连接、重新查找起始位置、重新开始解析
}
```

回顾 `EventTransactionBuffer.reset()` 的实现：

```java
public void reset() {
    putSequence.set(INIT_SQEUENCE);
    flushSequence.set(INIT_SQEUENCE);
}
```

**这一步在干什么？**

`AbstractEventParser` 主循环一旦捕获到异常（网络断开、下游 Sink 抛异常、Binlog 解析出错等等），会依次执行：`eventSink.interrupt()` 中断下游消费、`transactionBuffer.reset()` 重置环形缓冲区、`binlogParser.reset()` 重置底层 Binlog 解析器状态，然后 sleep 一段带随机抖动的时间（`10000 + random(10000)` 毫秒，即 10~20 秒）后重新连接、重新走一遍 `findStartPosition()` 确定起始位置、重新开始解析。

`transactionBuffer.reset()` 的实现极其简单——**只是把两个游标 `putSequence`/`flushSequence` 都重置回初始值 `-1`，并没有真的去清空 `entries` 数组里的内容**（那些 Entry 对象引用依然留在数组里，但由于游标归零，下一次 `put()` 会直接从数组下标 0 开始覆盖写入，相当于逻辑上"当作没发生过"）。这意味着：**任何还停留在环形缓冲区里、尚未被 `flush()` 推送给下游、因而也就没有机会被持久化位点的数据，在这次重启后会被彻底丢弃**，不会有任何补偿或重放动作。

这个设计初看起来像是"丢数据"，但恰恰是安全性的保障：因为根据前三节的分析，**没有被 flush 出去的数据，一定对应着一个尚未提交完整的事务**（否则早就在 TRANSACTIONEND 时触发 flush 了），而这部分数据也就从未被持久化为一个安全的位点。重启后 Parser 会重新连接 MySQL，并从上一次已经确认持久化的位点（也就是上一个完整提交的事务的结束位置）重新开始拉取 Binlog——MySQL 会重新把这部分"看起来被丢弃了"的数据重新推送一遍，Canal 会重新解析、重新走一遍完整的 BEGIN...COMMIT 流程。所以这里的"丢弃"只是丢弃内存里这一份不完整的中间状态，数据本身并没有真的丢失，而是通过"回到上一个安全位点、重新拉取"来恢复。

### 7.5 重启幂等性的完整闭环

把 7.1 至 7.4 串起来，可以总结出 Canal 位点安全设计的完整逻辑闭环：

1. **只在事务边界推送数据**：`EventTransactionBuffer` 保证只有完整的事务（BEGIN 到 END 之间的全部 ROWDATA）才会被打包一起 flush，不会有"半个事务"被单独推送给下游。
2. **只在下游确认消费成功后才持久化位点**：`AbstractEventParser` 的 flush 回调先调用 `eventSink.sink()` 投递数据，只有返回成功才继续调用 `persistLogPosition()`，杜绝了"位点已经推进、但数据其实还没送达"的不一致窗口。
3. **位点必须对应一个完整事务的结束点**：`buildLastTransactionPosition` 只认 TRANSACTIONEND，保证持久化的位点永远是一个安全的、可以从此处重新开始订阅的"事务边界点"，不会是事务内部的某个中间位置。
4. **异常时彻底丢弃未提交的中间状态**：`transactionBuffer.reset()` 不做任何"部分恢复"的尝试，直接清空游标，依赖"重新连接 MySQL、从上一个已持久化的安全位点重新拉取 Binlog"这个更简单可靠的机制来补偿，而不是试图在内存里做复杂的断点续传。

这四点合在一起，构成了一个典型的"至少一次（at-least-once）"语义的幂等恢复设计：**Canal 自己保证的是"不丢事务"，而不是"不重复事务"**——重启后完全可能重新拉取到、重新处理一遍某个事务（如果这个事务恰好卡在崩溃前的最后一批还未来得及确认），下游 Sink/Store/客户端如果需要严格的精确一次（exactly-once）语义，需要自己在消费端做幂等处理（比如按照 GTID 或者 `journalName + position` 做去重）。而"每个客户端各自的消费位点"（本篇第五章的 Meta 模块）和"Parser 自己的解析位点"（第六章的 LogPositionManager）之所以要分开管理、分别持久化，也正是因为这是两条独立的安全防线——即使 Parser 自己的解析位点因为某些原因回退重放了一部分数据，只要 Meta 模块记录的客户端消费位点没有被错误地往前跳跃，下游客户端依然可以通过自己的 ack 机制正确地识别出哪些数据是重复的、该如何处理。

---

## 完整装配与数据流总结

前七章分别拆解了 `CanalInstance` 的骨架（第一、二章）、两种装配方式的具体实现（第三、四章）、Meta 模块的五种实现（第五章）、LogPositionManager 的五种实现（第六章），以及位点安全性的完整闭环（第七章）。本章把这些碎片重新拼装起来，还原一个 `CanalInstance` 从"配置文件"到"稳定运行、可安全重启"的完整生命周期。

### 8.1 一张图看懂五大组件 + 两条持久化线

```
                        ┌─────────────────────────────────────────────────────────┐
                        │                     CanalInstance                        │
                        │                                                           │
   MySQL Master         │   ┌───────────┐     ┌───────────┐     ┌───────────┐       │      客户端
  ┌─────────────┐       │   │           │     │           │     │           │       │    ┌─────────┐
  │  Binlog Dump │◀──────── │  Parser   │────▶│   Sink    │────▶│   Store   │──────────▶│ getWithoutAck│
  │  (模拟Slave) │       │   │           │     │(过滤/路由)│     │(环形缓冲区)│       │    │ ack / rollback│
  └─────────────┘       │   └─────┬─────┘     └───────────┘     └─────┬─────┘       │    └─────────┘
                        │         │                                   │             │
                        │         │ 解析位点                    消费位点/批次        │
                        │         ▼                                   ▼             │
                        │  ┌──────────────────┐              ┌──────────────────┐    │
                        │  │LogPositionManager │              │  CanalMetaManager │    │
                        │  │ (Parser自己的安全线)│              │ (每个客户端的安全线)│    │
                        │  └─────────┬────────┘              └─────────┬────────┘    │
                        │            │                                 │             │
                        └────────────┼─────────────────────────────────┼─────────────┘
                                     ▼                                 ▼
                        Memory / File / ZooKeeper / Failback   Memory / File / ZooKeeper / Period
                            （持久化 Parser 解析到哪儿了）        （持久化客户端消费/ACK到哪个批次）

                        ┌───────────────────┐          ┌───────────────────┐
                        │  CanalAlarmHandler │          │ CanalMetaManager   │  ← CanalInstance 五大组件之五
                        │   （报警通道）      │          │（同时也是上面的持久化线）│
                        └───────────────────┘          └───────────────────┘
```

这张图把本文的核心结论浓缩成一句话：**Parser、Sink、Store 三大组件构成数据从 MySQL 流向客户端的"数据面"，而 LogPositionManager 与 CanalMetaManager 分别是这条数据面上，"生产端"（Parser 读到哪了）和"消费端"（客户端消费到哪了）各自独立的"安全刹车"**。二者持久化的时机、频率、存储介质都可以完全不同，却共同保证了 Canal 在任意时刻崩溃重启，都能找回一个安全、一致的续传点。

### 8.2 五大组件回顾（对应第一、二章）

| 组件 | 接口 | 职责 | 本文对应章节 |
| --- | --- | --- | --- |
| `CanalEventParser` | `getEventParser()` | 模拟 MySQL Slave 协议，拉取并解析 Binlog，产出 `CanalEntry` | 前置文档（Parser 篇） |
| `CanalEventSink` | `getEventSink()` | 对 Parser 产出的数据做过滤、路由、事务合并，投递给 Store | 前置文档（Sink 篇） |
| `CanalEventStore` | `getEventStore()` | 环形缓冲区，解耦 Parser 的生产速度与客户端的消费速度 | 前置文档（Store 篇） |
| `CanalMetaManager` | `getMetaManager()` | 管理客户端订阅关系、消费游标（cursor）、未 ACK 的批次（batch） | 本文第五章 |
| `CanalAlarmHandler` | `getAlarmHandler()` | 出现异常（比如 Parser 反复重连失败）时触发报警 | 本文第一章 1.1 |

`AbstractCanalInstance`（第二章）把这五大组件按照严格的依赖顺序串联起来：`MetaManager → AlarmHandler → EventStore → EventSink → EventParser` 依次 `start()`，`stop()` 时逆序释放——这个顺序本身就体现了"先准备好持久化能力和报警能力，再准备缓冲区，再准备投递链路，最后才真正开始从 MySQL 拉取数据"的设计考量：任何一个下游组件没有就绪之前，Parser 都不应该开始产生数据。

### 8.3 两种装配方式回顾（对应第三、四章）

| 维度 | CanalInstanceWithManager（第三章） | CanalInstanceWithSpring（第四章） |
| --- | --- | --- |
| 组件创建方式 | 编程式：在 Java 代码里手工 `new` 出每个组件，通过一连串 `if/else` 分支判断配置项来决定用哪个实现类 | 声明式：在 `instance.xml` 中用 Spring Bean 定义每个组件，容器负责实例化与依赖注入 |
| 配置来源 | `CanalParameter`（一个巨大的参数对象，字段覆盖 Parser/Sink/Store/Meta 的所有配置项） | Spring `PropertyPlaceholderConfigurer` 读取的 `.properties` 文件 + XML 中的 `${}` 占位符 |
| 典型使用场景 | canal-admin 等需要"动态生成实例、无法预先写死 XML"的管理端场景 | canal-deployer 独立部署场景，每个 destination 对应一份 `conf/{destination}/instance.properties` |
| 灵活性 | 每新增一种组件实现，需要在 `CanalInstanceWithManager` 里新增一个 `if` 分支 | 每新增一种组件实现，只需要提供一份新的 XML profile（如 `spring/file-instance.xml`、`spring/default-instance.xml`），无需改动 Java 代码 |
| 与本文 Meta/位点章节的关系 | 3.8 节展示了 `MemoryMetaManager` 与 `MetaLogPositionManager` 搭配时的 fail-fast 保护逻辑 | `default-instance.xml`/`file-instance.xml` 分别对应 `PeriodMixedMetaManager`+`FailbackLogPositionManager` 和 `FileMixedMetaManager`+`FileMixedLogPositionManager` 两种持久化组合 |

两种装配方式本质上是"同一份五大组件的依赖图"用两种不同的构造手段搭出来——理解了 `AbstractCanalInstance` 定义的启动顺序和组件依赖关系，无论是读 `CanalInstanceWithManager` 里一长串 `if/else`，还是读 Spring XML 里错综复杂的 `<bean>` 引用，都能一眼看出"这是在装配哪个组件、依赖了谁"。

### 8.4 Meta 模块五种实现回顾（对应第五章）

| 实现 | 存储介质 | batch 数据是否持久化 | 典型使用场景 |
| --- | --- | --- | --- |
| `MemoryMetaManager` | 纯内存 | 是（内存中） | 单机测试、不要求重启保留消费进度 |
| `FileMixedMetaManager` | 内存 + 本地文件（`meta.dat`） | 否，只持久化订阅信息与 cursor | 单机部署，重启需要保留消费进度但不依赖外部组件 |
| `PeriodMixedMetaManager` | 内存 + ZooKeeper（周期性/防抖刷新） | 否，只持久化订阅信息与 cursor（batch 显式放弃持久化） | canal-deployer 默认配置（`default-instance.xml`），支持多机房 HA 场景 |
| `ZooKeeperMetaManager` | 纯 ZooKeeper（每次操作强一致写入） | 是，每个 batch 对应一个持久顺序节点 | 需要严格保证 batch 分配全局唯一、不允许有任何丢失窗口的场景 |
| `MixedMetaManager` | 内存 + 异步全量写穿 ZooKeeper（单线程，每次操作都触发） | 是，但采用"先内存后异步 ZK"，一致性弱于 `ZooKeeperMetaManager` | 早期实现，已被 `PeriodMixedMetaManager` 取代 |

五种实现共享同一个核心约束——`removePositionRange(batchId)` 必须严格按照 `batchId == 当前最小未 ACK batchId` 的顺序调用，否则抛出 `CanalMetaManagerException`。这是防止"客户端乱序 ACK 导致中间出现未确认的位点空洞"的关键设计：无论存储介质是内存 Map 还是 ZooKeeper 节点，这条顺序约束都必须被保留。

### 8.5 LogPositionManager 五种实现回顾（对应第六章）

| 实现 | 存储介质 | 特点 |
| --- | --- | --- |
| `MemoryLogPositionManager` | 纯内存 `Map` | 最基础的实现，常被其他实现组合复用 |
| `ZooKeeperLogPositionManager` | ZooKeeper 单节点（每 destination 一个节点） | 每次持久化都是一次同步 ZK 写入，无内存缓冲 |
| `FailbackLogPositionManager` | 组合 primary + secondary 两个 `CanalLogPositionManager` | 读时 primary 优先、找不到再退化到 secondary；写时 primary 优先，异常再退化到 secondary |
| `MetaLogPositionManager` | 依附于 `CanalMetaManager` | `persistLogPosition` 是空实现；`getLatestIndexBy` 通过扫描所有客户端 cursor 取最小值，实现"以慢打快" |
| `FileMixedLogPositionManager` / `PeriodMixedLogPositionManager` | 内存 + 本地文件（`parse.dat`）/ 内存 + ZooKeeper（防抖周期刷新） | 与 Meta 模块的 File/Period 版本使用同一套防抖 + 定时刷新模式，但文件名与 ZK 节点结构均与 Meta 模块相互独立 |

`default-instance.xml` 中实际组合出的 `FailbackLogPositionManager(MemoryLogPositionManager, MetaLogPositionManager(metaManager))`，正是把"内存中最新解析位点"作为第一优先级、把"所有客户端里最慢的消费位点"作为兜底优先级——这个组合本身就是"性能优先、安全兜底"设计哲学的直接体现：正常运行时直接读内存效率最高；一旦发生 Failover、内存态丢失，退化到用最保守（最慢客户端）的位点重新开始解析，宁可多推送重复数据，也不能让任何一个客户端的数据出现空洞。

### 8.6 一次完整重启的时间线

把前面所有章节串成一条时间线，可以还原出"canal-deployer 进程重启后重新追上数据"的完整过程：

1. **进程启动**：读取 `canal.properties` 决定装配方式（`manager` 或 `spring`），进而决定使用哪一套 Meta/LogPositionManager 组合。
2. **`AbstractCanalInstance.start()` 按顺序启动五大组件**：`metaManager.start()` → `alarmHandler.start()` → `eventStore.start()` → `eventSink.start()` → `eventParser.start()`（第二章）。
3. **`eventParser.start()` 内部调用 `findStartPosition()`**：依次尝试从 `logPositionManager.getLatestIndexBy(destination)` 拿到上次持久化的解析位点；如果是 `FailbackLogPositionManager`，先查内存（重启后为空），再查 `MetaLogPositionManager`（第六章 6.4），也就是取所有客户端里消费进度最慢的那个位点。
4. **从该位点重新订阅 Binlog**：MySQL Master 会从这个位置开始重新推送 Binlog Event，包括那些"上次崩溃前已经被 `transactionBuffer.reset()` 丢弃的未提交事务数据"（第七章 7.4）。
5. **`EventTransactionBuffer` 重新按事务边界攒批**：只有等到完整的 `TRANSACTIONEND` 出现才会 `flush()`，保证下游拿到的永远是完整事务（第七章 7.1）。
6. **`eventSink.sink()` 消费成功后才 `persistLogPosition()`**：位点只在下游确认后才前进，且只记录到 TRANSACTIONEND 对应的位置（第七章 7.2、7.3）。
7. **客户端重新连接并 `getWithoutAck`**：`CanalMetaManager` 中记录的 cursor 和未 ACK 的 batch 依然保留（除非用的是纯内存实现），客户端可以从上次的 batch 继续 ACK，或者 `rollback` 后重新拉取（第五章）。

整个链路中，**"数据从哪里重新开始拉"由 LogPositionManager 决定，"客户端从哪里重新开始消费"由 CanalMetaManager 决定**，两者各自独立演进、互不依赖，却又通过 `MetaLogPositionManager` 这座桥梁（用所有客户端的最慢 cursor 兜底 Parser 的解析位点）产生了必要的联系——这正是本文标题"CanalInstance 装配与 Meta 位点持久化"想要传达的核心图景：**装配决定了组件如何被组织在一起，而位点持久化决定了这套组件在任何异常面前，都能找到一条安全、一致、可重复的"回退再前进"的路径。**
