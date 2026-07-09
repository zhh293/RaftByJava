# Disruptor 高性能环形缓冲区全源码深度解析

> **Netty 源码深度研究系列 · 番外篇**
>
> LMAX Disruptor 是一个高性能的线程间消息传递框架，其设计理念深刻影响了 Netty 的 MpscQueue、JCTools 无锁队列以及整个 Java 高性能并发生态。本文基于 Disruptor 4.0.0 / 3.4.4 源码，从环形缓冲区的底层数据结构出发，逐层拆解 Sequence、RingBuffer、Sequencer、SequenceBarrier、EventProcessor、WaitStrategy 等每一个核心组件的实现细节，深入分析其设计思路、设计方式、为什么这么设计，并覆盖高级用法与性能调优的全貌。

---

## 目录

```
一、Disruptor 全景：从问题到解决方案
  1.1 传统队列为什么慢
  1.2 Disruptor 的核心思想
  1.3 与 JDK 队列、Netty MpscQueue 的定位差异
  1.4 整体架构图与组件关系

二、Sequence：最小的并发基元
  2.1 为什么不用 AtomicLong
  2.2 缓存行填充的继承链实现
  2.3 Sequence 的 CAS 与 lazySet 语义
  2.4 从 Unsafe 到 VarHandle 的演进
  2.5 INITIAL_VALUE = -1 的设计考量

三、RingBuffer：环形数组的数据结构
  3.1 为什么是环形数组而非链表
  3.2 数组大小必须是 2 的幂
  3.3 预分配 Entry 对象与 EventFactory
  3.4 RingBufferFields 的缓存行填充
  3.5 elementAt()：通过 Unsafe 直接计算数组偏移
  3.6 publishEvent() 与 tryPublishEvent()
  3.7 get() 方法：消费者如何读取事件

四、Sequencer：生产者协议的核心
  4.1 Sequencer 接口定义
  4.2 SingleProducerSequencer 全源码解析
    4.2.1 next() / next(n)：申请序列号
    4.2.2 publish() / publish(lo, hi)：发布事件
    4.2.3 cachedGatingSequence 优化
    4.2.4 hasAvailableCapacity() 与 remainingCapacity()
  4.3 MultiProducerSequencer 全源码解析
    4.3.1 availableBuffer 数组：标记已发布的槽位
    4.3.2 next() / next(n)：CAS 竞争序列号
    4.3.3 publish()：标记 availableBuffer
    4.3.4 isAvailable() 与 getHighestPublishedSequence()
    4.3.5 多生产者下的连续性保证
  4.4 SingleProducer vs MultiProducer 的性能差异量化

五、SequenceBarrier：消费者等待的屏障
  5.1 为什么需要 SequenceBarrier
  5.2 ProcessingSequenceBarrier 全源码解析
  5.3 dependentSequence：消费者依赖链
  5.4 waitFor() 的协作流程
  5.5 alert 机制：优雅关闭

六、WaitStrategy：等待策略全族谱
  6.1 BlockingWaitStrategy（默认）
  6.2 SleepingWaitStrategy（三阶段退避）
  6.3 YieldingWaitStrategy（自旋+yield）
  6.4 BusySpinWaitStrategy（纯自旋，绑核场景）
  6.5 LiteBlockingWaitStrategy（轻量阻塞）
  6.6 TimeoutBlockingWaitStrategy（超时阻塞）
  6.7 PhasedBackoffWaitStrategy（组合策略）
  6.8 各策略的适用场景与延迟-吞吐量权衡矩阵

七、EventProcessor 与 BatchEventProcessor：消费者引擎
  7.1 EventProcessor 接口
  7.2 BatchEventProcessor 全源码解析
    7.2.1 run() 主循环
    7.2.2 processEvents()：批量消费的实现
    7.2.3 为什么是"批量"而非逐个消费
    7.2.4 异常处理：ExceptionHandler 的三种策略
  7.3 WorkProcessor 与 WorkHandler：多消费者竞争消费
    7.3.1 WorkProcessor.run() 源码
    7.3.2 workSequence 的 CAS 竞争
    7.3.3 与 BatchEventProcessor 的设计差异
  7.4 EventHandler / LifecycleAware / TimeoutHandler 回调接口

八、EventFactory 与预分配：消除运行时内存分配
  8.1 预分配的设计哲学
  8.2 预分配的实现代码
  8.3 预分配 vs 即时分配的性能对比
  8.4 EventTranslator：简化发布的语法糖

九、Disruptor DSL：声明式 API 与消费者依赖图
  9.1 DSL 设计的动机
  9.2 Disruptor 类的核心字段
  9.3 handleEventsWith()：注册消费者
  9.4 then()：建立消费者依赖链
  9.5 构建复杂的消费者拓扑
  9.6 start()：启动所有消费者
  9.7 shutdown()：优雅关闭

十、缓存行填充与 Mechanical Sympathy
  10.1 Disruptor 中的缓存行填充全景
  10.2 Sequence 的填充细节
  10.3 RingBuffer 的数组填充
  10.4 SingleProducerSequencer 的填充
  10.5 与 Netty JCTools MpscArrayQueue 的填充对比

十一、性能基准与实际测量
  11.1 Disruptor 官方基准测试结构
  11.2 Disruptor vs ArrayBlockingQueue 性能对比
  11.3 现代硬件上的预期性能
  11.4 影响性能的关键配置

十二、高级使用模式
  12.1 多阶段处理流水线（Pipeline）
  12.2 多生产者共享 RingBuffer
  12.3 清理事件数据防止内存泄漏
  12.4 利用 endOfBatch 实现批量优化
  12.5 温热启动与消除 JIT 编译抖动

十三、Disruptor 在知名项目中的应用
  13.1 Log4j2 AsyncLogger
  13.2 LMAX Exchange 交易引擎
  13.3 与 Netty 的技术血缘关系

十四、Disruptor 的设计模式总结
  14.1 核心设计模式
  14.2 核心高性能并发技术

十五、源码版本演进与历史
  15.1 主要版本节点
  15.2 从 Disruptor 到 JCTools 的技术传承

十六、常见问题与最佳实践
  16.1 如何选择 bufferSize？
  16.2 事件对象的设计原则
  16.3 异常处理策略的选择
  16.4 监控与可观测性
  16.5 常见陷阱

十七、Disruptor 源码的工程美学
  17.1 极致的 API 最小化
  17.2 Mechanical Sympathy 的实践教科书
  17.3 与传统并发编程的思维差异
  17.4 性能优化的层次

十八、完整代码示例：从零构建 Disruptor 应用
  18.1 最简示例：单生产者-单消费者
  18.2 菱形依赖示例
  18.3 使用 EventTranslator 的推荐发布方式
  18.4 批量发布示例
  18.5 WorkerPool 竞争消费示例
  18.6 混合模式：广播 + 竞争 + 管道

十九、附录：Disruptor 核心类索引
```

---

## 一、Disruptor 全景：从问题到解决方案

### 1.1 传统队列为什么慢

在理解 Disruptor 之前，首先需要理解它要解决的问题：传统的线程间消息传递队列——无论是 JDK 的 `ArrayBlockingQueue`、`LinkedBlockingQueue`，还是 `ConcurrentLinkedQueue`——在高吞吐、低延迟的场景下都存在系统性的性能瓶颈。这些瓶颈并非算法层面的问题，而是根植于现代 CPU 架构与 JVM 内存模型的物理限制。

**第一个瓶颈：锁竞争。** `ArrayBlockingQueue` 和 `LinkedBlockingQueue` 的核心操作（`put()` / `take()`）都依赖 `ReentrantLock`。在多线程高并发场景下，锁会导致线程上下文切换，每次上下文切换的开销在微秒级（Linux 上约 1~10μs），这对于追求纳秒级延迟的系统来说是不可接受的。更关键的是，锁的获取和释放本身需要 CAS 操作和内存屏障，即使在无竞争的情况下也有不可忽略的开销。

```java
// ArrayBlockingQueue.put() —— 锁是性能瓶颈的根源
public void put(E e) throws InterruptedException {
    Objects.requireNonNull(e);
    final ReentrantLock lock = this.lock;  // 所有生产者和消费者共享同一把锁
    lock.lockInterruptibly();               // 获取锁 → 如果有竞争就阻塞
    try {
        while (count == items.length)
            notFull.await();                // 队列满 → 等待
        enqueue(e);                         // 入队
    } finally {
        lock.unlock();                      // 释放锁
    }
}
```

注意上面代码中所有生产者和消费者**共享同一把锁** `this.lock`。这意味着生产者放入元素时，消费者不能取出元素——这是一种极其保守的并发策略。`LinkedBlockingQueue` 虽然把生产和消费分成了两把锁（`putLock` 和 `takeLock`），但每把锁内部的竞争依然存在，而且链表节点的频繁创建还会带来额外的 GC 压力。

**第二个瓶颈：伪共享（False Sharing）。** 现代 CPU 的缓存以"缓存行"（Cache Line）为最小单位进行加载和失效，一条缓存行通常是 64 字节。当生产者的写游标（putIndex）和消费者的读游标（takeIndex）恰好落在同一条缓存行时，任何一方的修改都会导致另一方的缓存行被无效化（MESI 协议中的 Invalidate），即使它们操作的是完全不同的字段。JDK 的 `ArrayBlockingQueue` 就存在这个问题：

```java
// ArrayBlockingQueue 中的字段布局 —— 极易伪共享
public class ArrayBlockingQueue<E> {
    final Object[] items;
    int takeIndex;      // 消费者写、生产者读 ← 这两个字段
    int putIndex;       // 生产者写、消费者读 ← 很可能在同一缓存行
    int count;
    // ...
}
```

`takeIndex`（4字节）、`putIndex`（4字节）、`count`（4字节）加起来才12字节，它们几乎必然位于同一条64字节的缓存行中。生产者每次修改 `putIndex`，都会导致消费者核心上缓存的 `takeIndex` 被迫从主存重新加载——即使 `takeIndex` 的值根本没有变化。LMAX 的测试数据表明，伪共享可以导致吞吐量下降 **20~40 倍**。

**第三个瓶颈：内存分配与 GC。** `LinkedBlockingQueue` 每次入队都需要创建一个 `Node` 对象（`new Node<>(e)`），高频入队会产生大量短生命周期对象，增加 GC 频率。即使使用 `ArrayBlockingQueue` 避免了节点分配，队列中存储的事件对象本身仍然需要 `new`。在追求极致延迟的场景中，任何一次 GC 暂停（即使是几十毫秒的 Young GC）都是灾难性的——对于金融交易系统来说，一次 100ms 的 GC 暂停可能意味着数百万美元的损失。

**第四个瓶颈：顺序屏障的缺失。** `ConcurrentLinkedQueue` 虽然是无锁的，但它是无界的、不支持背压、且无法建立消费者之间的依赖关系。当你需要"事件先经过解码器处理，再经过业务处理器，最后经过日志记录器"这种管道式处理时，标准的 JDK 队列需要多个队列串联，每一次中转都引入额外的延迟和内存拷贝。

LMAX 在 2010 年发布的技术论文中给出了令人震惊的基准数据：在一台 2.2GHz 的 Nehalem 处理器上，`ArrayBlockingQueue` 的单生产者-单消费者吞吐量约为 **500 万次/秒**，而 Disruptor 达到了 **超过 2500 万次/秒**——高出 **5 倍以上**，且 P99 延迟低了两个数量级。

### 1.2 Disruptor 的核心思想

面对上述四大瓶颈，Disruptor 的解决方案可以浓缩为四个核心思想：

**（1）环形数组替代链表/阻塞队列。** 使用一个固定大小的数组（环形缓冲区 RingBuffer），数组中的每个槽位（slot）在初始化时就预分配好事件对象。生产者和消费者通过序列号（Sequence）来协调位置，而不是通过锁。数组的大小强制为 2 的幂，这样取模运算就可以用位运算（`sequence & (bufferSize - 1)`）替代，快了一个数量级。

**（2）缓存行填充消除伪共享。** Disruptor 中所有需要被不同线程读写的字段——生产者的 cursor、消费者的 sequence、RingBuffer 的内部字段——都用缓存行填充进行隔离。每个 `Sequence` 对象独占至少一条完整的缓存行（通过在字段前后各填充 56 字节的 padding），确保不同核心上的线程修改各自的 Sequence 时不会相互干扰。

**（3）内存预分配消除 GC。** RingBuffer 在创建时就通过 `EventFactory` 把每个槽位的事件对象都 `new` 出来。后续的"发布事件"不是把新对象放入数组，而是通过 `EventTranslator` 就地修改已有对象的字段值。这样在整个生命周期中几乎不产生新的对象分配，彻底避免了 GC 压力。

**（4）SequenceBarrier 实现有序消费。** Disruptor 引入了 `SequenceBarrier` 的概念，允许消费者声明"我依赖另一个消费者先处理完"。多个消费者可以构成一个有向无环图（DAG），Disruptor 自动保证执行顺序，而不需要在消费者之间传递中间队列。每个消费者只需跟踪自己的序列号和依赖者的序列号，通过简单的比较（而非锁或条件变量）来判断是否可以继续消费。

这四个思想并非各自独立，而是形成了一个自洽的系统：环形数组提供了固定的内存布局，使得缓存行填充成为可能；预分配消除了 GC，使得延迟可预测；SequenceBarrier 用序列号比较替代了锁同步，使得整个系统几乎完全无锁。Martin Thompson（Disruptor 的主要设计者）将这种设计哲学称为"Mechanical Sympathy"——让软件的行为与底层硬件的工作方式相"共情"。

### 1.3 与 JDK 队列、Netty MpscQueue 的定位差异

Disruptor 并非万能的银弹，它与 JDK 标准队列和 Netty 的 MpscQueue 在定位上有本质差异：

JDK 的 `ArrayBlockingQueue` 和 `LinkedBlockingQueue` 是**通用目的**的并发集合，追求的是正确性、易用性和通用性。它们适用于大多数并发场景，不需要特殊的知识就能正确使用。代价是在极端高性能场景下有显著的性能损失。

Netty 的 `MpscArrayQueue`（来自 JCTools 库）是一个**嵌入式组件**，专为"多生产者单消费者"模式优化。它通常被嵌入到 EventLoop 内部作为任务队列使用，API 极其简洁（只有 `offer()` 和 `poll()`），不支持消费者依赖图、不支持批量消费回调、不支持等待策略。它的设计目标是"作为框架内部的高效管道"，而不是面向用户的编程模型。

Disruptor 是一个**完整的消息传递框架**，提供了丰富的消费者编排能力（依赖图、广播、竞争消费）、可插拔的等待策略、批量消费接口、完整的生命周期管理。它适用于需要极致延迟和高吞吐的独立应用（如金融交易系统、实时风控系统），通常作为应用架构的核心组件而非嵌入在某个框架内部。

三者在设计上的共同点在于：都使用了环形/数组结构、都通过 CAS 避免锁、都关注缓存行填充问题。Disruptor 是这些优化技术的"鼻祖"——JCTools 和 Netty 的 MpscQueue 在很大程度上受到了 Disruptor 的启发。

### 1.4 整体架构图与组件关系

```
                        ┌─────────────────────────────────────────────────────────┐
                        │                    Disruptor DSL                        │
                        │  handleEventsWith() / then() / after() / start()       │
                        └────────────────────────┬────────────────────────────────┘
                                                 │ 创建并连接
                    ┌────────────────────────────┼────────────────────────────────┐
                    │                            │                                │
                    ▼                            ▼                                ▼
         ┌──────────────────┐      ┌──────────────────────┐      ┌──────────────────────┐
         │  EventProcessor  │      │   SequenceBarrier     │      │    WaitStrategy      │
         │ (BatchEvent-     │ ────▶│ (Processing-          │ ────▶│ (Blocking/Yielding/  │
         │  Processor /     │      │  SequenceBarrier)     │      │  Sleeping/BusySpin)  │
         │  WorkProcessor)  │      └──────────┬───────────┘      └──────────────────────┘
         └────────┬─────────┘                 │
                  │                           │ waitFor(sequence)
                  │ 调用 EventHandler          │
                  ▼                           ▼
         ┌──────────────────┐      ┌──────────────────────┐
         │   EventHandler   │      │      Sequencer        │
         │ (用户业务逻辑)    │      │ (Single/Multi-        │
         └──────────────────┘      │  ProducerSequencer)   │
                                   └──────────┬───────────┘
                                              │ 管理
                                              ▼
         ┌──────────────────────────────────────────────────────────────────┐
         │                         RingBuffer                               │
         │  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐ │
         │  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │10 │11 │12 │13 │...│ │
         │  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘ │
         │  entries[] —— 预分配的事件对象数组                                  │
         └─────────────────────────────────────────────────────────────────┘

         ┌──────────────────────────────────────────────────────────────────┐
         │                         Sequence                                 │
         │  ┌─────────────────────────────────────────────────────────────┐ │
         │  │  padding (56 bytes) | volatile long value | padding (56B)  │ │
         │  └─────────────────────────────────────────────────────────────┘ │
         │  cursor (生产者)  /  gatingSequences (消费者)  /  依赖链           │
         └──────────────────────────────────────────────────────────────────┘

生产者流程: producer → sequencer.next() → ringBuffer.get(seq) → translator.translateTo(event) → sequencer.publish(seq)
消费者流程: barrier.waitFor(nextSeq) → ringBuffer.get(seq) → handler.onEvent(event, seq, endOfBatch)
```

上图展示了 Disruptor 的核心组件关系。从上到下：用户通过 **Disruptor DSL** 配置和启动系统；DSL 内部为每个消费者创建 **EventProcessor** 和 **SequenceBarrier**；SequenceBarrier 连接到 **Sequencer**（管理生产者的 cursor）和依赖消费者的 **Sequence**；所有数据存储在 **RingBuffer** 的预分配数组中；每个 Sequence 都通过缓存行填充确保独占缓存行。

整个系统中没有任何锁——生产者通过 CAS 争夺序列号，消费者通过 volatile 读跟踪依赖者的进度，等待策略决定在没有新数据时如何退避。

---

## 二、Sequence：最小的并发基元

### 2.1 为什么不用 AtomicLong

Sequence 是 Disruptor 中最基础的并发基元，它本质上是一个"被缓存行填充包围的 volatile long"。一个自然的问题是：为什么不直接用 JDK 的 `AtomicLong`？

答案有两层。

**第一层：伪共享。** `AtomicLong` 内部只有一个 `volatile long value` 字段（8 字节），加上对象头（在 64 位 JVM 开启压缩指针时为 12 字节）和对齐填充（4 字节），一个 `AtomicLong` 对象总共只有 24 字节。一条缓存行是 64 字节，这意味着一条缓存行可以容纳约 2~3 个 `AtomicLong` 对象。当两个被不同线程操作的 `AtomicLong` 恰好相邻分配在同一缓存行时，就会发生伪共享——线程 A 修改自己的 `AtomicLong`，CPU 核心 B 上缓存的另一个 `AtomicLong` 也被迫失效。

在 Disruptor 中，生产者的 cursor（一个 Sequence）和消费者的 sequence（另一个 Sequence）是被不同线程频繁读写的热点字段。如果使用 `AtomicLong`，它们极有可能被 JVM 分配在相邻的内存位置（尤其是在同一个构造函数中连续 `new` 出来时），导致严重的伪共享。

**第二层：语义精确性。** `AtomicLong` 提供了 `getAndIncrement()`、`addAndGet()` 等通用方法，但 Disruptor 的 Sequence 只需要极少几个操作：`get()`（volatile 读）、`set(value)`（volatile 写）、`compareAndSet(expect, update)`（CAS），以及最关键的 `setVolatile()` / `setRelease()`（不同语义的写入）。`AtomicLong` 的方法集合过于丰富，而 Sequence 需要的是一个最小化、语义精确的并发原语，同时保证每个实例独占完整的缓存行。

### 2.2 缓存行填充的继承链实现

Disruptor 的 Sequence 使用了一种经典的**继承链填充**技术来确保 `value` 字段独占一条完整的缓存行。在 Disruptor 3.x 中，填充通过以下继承链实现：

```java
// 第一层：左侧填充
class LhsPadding {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

// 第二层：实际值
class Value extends LhsPadding {
    protected volatile long value;
}

// 第三层：右侧填充
class RhsPadding extends Value {
    protected long p9, p10, p11, p12, p13, p14, p15;
}

// 第四层：对外暴露的 Sequence 类
public class Sequence extends RhsPadding {
    static final long INITIAL_VALUE = -1L;
    // ... 方法定义
}
```

为什么要用继承而不是把所有字段放在一个类里？这是因为 **JVM 的字段重排序规则**。JVM 规范允许在同一个类内部对字段进行重排序（优化内存布局和对齐），但禁止将子类的字段插入到父类字段之间。通过继承链，我们可以保证内存布局是：

```
| 对象头 (12B) | p1~p7 (56B) | value (8B) | p9~p15 (56B) | 对齐 (4B) |
   ↑ LhsPadding 的字段       ↑ Value 的字段   ↑ RhsPadding 的字段
```

总大小 = 12 + 56 + 8 + 56 + 4 = 136 字节 = 2.125 条缓存行。这确保了无论 `value` 落在哪条缓存行的哪个位置，它的前后各有至少 56 字节的填充，足以独占一条完整的 64 字节缓存行。

如果把所有 padding 字段和 value 放在同一个类中：

```java
// 反面示例：不要这样做！
class Sequence {
    long p1, p2, p3, p4, p5, p6, p7;    // 左填充
    volatile long value;                   // 实际值
    long p9, p10, p11, p12, p13, p14, p15; // 右填充
}
```

JVM 可能会把 `value` 重排序到 `p1` 之前或 `p15` 之后，使得填充失效。继承链是 Java 中唯一可靠的、在所有 JVM 实现上都有效的缓存行填充方法（在 Java 8 之前没有 `@Contended` 注解时）。

### 2.3 Sequence 的 CAS 与 lazySet 语义

Sequence 提供了三种不同语义的写操作，每种对应不同的内存屏障强度：

```java
public class Sequence extends RhsPadding {
    private static final long VALUE_OFFSET;

    static {
        try {
            VALUE_OFFSET = UNSAFE.objectFieldOffset(Value.class.getDeclaredField("value"));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    // （1）volatile 写 —— 完整的 StoreLoad 屏障
    public void set(final long value) {
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
    }

    // （2）volatile 读 —— LoadLoad + LoadStore 屏障
    public long get() {
        return value;  // volatile 读
    }

    // （3）CAS —— 原子性比较并交换
    public boolean compareAndSet(final long expectedValue, final long newValue) {
        return UNSAFE.compareAndSwapLong(this, VALUE_OFFSET, expectedValue, newValue);
    }

    // （4）setVolatile —— 真正的 volatile 写语义
    public void setVolatile(final long value) {
        UNSAFE.putLongVolatile(this, VALUE_OFFSET, value);
    }
}
```

这里有一个精妙但容易被忽略的细节：**`set()` 方法使用的是 `putOrderedLong()` 而不是 `putLongVolatile()`**。

`putOrderedLong()` 对应的是 **release 语义**（也叫 lazySet），它只保证前面的写操作不会被重排序到这个写之后，但不保证这个写立即对其他线程可见——其他线程可能在"稍后"才看到这个新值。在 x86 架构上，`putOrderedLong` 编译为一条普通的 `MOV` 指令（因为 x86 的 TSO 内存模型本身就保证了 store-store 顺序），而 `putLongVolatile` 编译为 `MOV` + `LOCK` 前缀（或 `MFENCE`），后者要昂贵得多。

**为什么 `set()` 可以用 lazySet？** 因为在 Disruptor 的使用场景中，消费者调用 `sequence.set(newValue)` 来更新自己的消费进度，而生产者读取消费者的 sequence 来判断是否可以覆盖旧数据。如果生产者"稍晚"看到消费者的新进度，最坏的结果只是生产者多等一轮——不会产生正确性问题，只是轻微的性能浪费。这种"正确但可能稍慢"的权衡换来了每次写操作省掉一条 `MFENCE` 指令的收益。

但 `setVolatile()` 仍然被保留，用于某些必须立即可见的场景（如 cursor 的发布），此时必须使用完整的 volatile 写语义。

### 2.4 从 Unsafe 到 VarHandle 的演进

在 Disruptor 3.x 中，Sequence 的底层实现完全依赖 `sun.misc.Unsafe`：

```java
private static final Unsafe UNSAFE;
static {
    try {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        UNSAFE = (Unsafe) theUnsafe.get(null);
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}
```

这种做法在 Java 9 的模块化系统（JPMS）引入后变得越来越不被推荐。`sun.misc.Unsafe` 不是公开 API，随时可能被移除或限制访问。

Disruptor 4.x 开始迁移到 `java.lang.invoke.VarHandle`（Java 9 引入的官方替代品）：

```java
public class Sequence {
    private static final VarHandle VALUE_HANDLE;
    static {
        try {
            VALUE_HANDLE = MethodHandles.lookup()
                .findVarHandle(Sequence.class, "value", long.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private volatile long value;

    public long get() {
        return value;  // volatile 读，等价于 VALUE_HANDLE.getVolatile(this)
    }

    public void set(final long value) {
        VALUE_HANDLE.setRelease(this, value);  // 对应原来的 putOrderedLong
    }

    public void setVolatile(final long value) {
        VALUE_HANDLE.setVolatile(this, value);  // 对应原来的 putLongVolatile
    }

    public boolean compareAndSet(final long expectedValue, final long newValue) {
        return VALUE_HANDLE.compareAndSet(this, expectedValue, newValue);
    }
}
```

`VarHandle` 提供了与 `Unsafe` 完全对等的语义（`getVolatile` / `setRelease` / `setVolatile` / `compareAndSet` / `getOpaque` 等），但作为 `java.lang.invoke` 包的一部分是官方支持的 API，不受模块化系统的限制，且在 JIT 编译后性能与 `Unsafe` 完全相同（都编译为相同的机器指令）。

### 2.5 INITIAL_VALUE = -1 的设计考量

```java
public class Sequence extends RhsPadding {
    static final long INITIAL_VALUE = -1L;

    public Sequence() {
        this(INITIAL_VALUE);
    }

    public Sequence(final long initialValue) {
        this.value = initialValue;
    }
}
```

为什么初始值是 -1 而不是 0？

因为序列号从 0 开始递增。如果初始值是 0，消费者创建后的初始序列号就是 0，而第一个事件发布到序列号 0 的位置。这会导致一个歧义：消费者的序列号 0 到底意味着"我还没消费任何事件"还是"我已经消费了序列号 0 的事件"？

通过将初始值设为 -1，语义变得清晰：序列号 -1 表示"尚未消费任何事件"，而序列号 0 明确表示"已消费了第一个事件（序列号 0）"。消费者的下一个待消费序列号就是 `sequence.get() + 1`。这种"哨兵值"模式简化了边界条件的处理，避免了额外的布尔标志位。

---

## 三、RingBuffer：环形数组的数据结构

### 3.1 为什么是环形数组而非链表

RingBuffer 是 Disruptor 最核心的数据结构——一个固定大小的数组，逻辑上首尾相连形成环形。选择数组而非链表的原因涉及三个层面：

**CPU 缓存友好性。** 数组在内存中是连续存储的，当 CPU 读取 `entries[i]` 时，缓存预取器（Hardware Prefetcher）会自动将 `entries[i+1]`、`entries[i+2]` 等后续元素加载到缓存中。链表节点散布在堆的各个位置，每次跟随 `next` 指针都可能触发缓存未命中（Cache Miss）。在连续遍历场景下，数组的访问延迟可以低至 1~4 个时钟周期（L1 命中），而链表的平均延迟在 50~100 个时钟周期（L2/L3 未命中或主存访问）。

**GC 影响。** 链表每次入队需要 `new Node()`，出队后 Node 成为垃圾等待回收。数组则在初始化时一次性分配，之后不再产生新的对象分配或回收。

**取模定位。** 环形的核心操作是把序列号映射到数组下标：`index = sequence % bufferSize`。对于 2 的幂次大小，这等价于 `sequence & (bufferSize - 1)`，一条 `AND` 指令就完成了。链表需要维护头尾指针并逐个遍历来定位。

### 3.2 数组大小必须是 2 的幂

```java
public final class RingBuffer<E> extends RingBufferFields<E> {
    // ...
    public static <E> RingBuffer<E> createSingleProducer(
            EventFactory<E> factory, int bufferSize, WaitStrategy waitStrategy) {
        SingleProducerSequencer sequencer =
            new SingleProducerSequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(sequencer, factory);
    }
}

abstract class RingBufferFields<E> extends RingBufferPad {
    private final int indexMask;
    private final Object[] entries;
    protected final int bufferSize;
    protected final Sequencer sequencer;

    RingBufferFields(EventFactory<E> eventFactory, Sequencer sequencer) {
        this.sequencer = sequencer;
        this.bufferSize = sequencer.getBufferSize();

        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.indexMask = bufferSize - 1;
        this.entries = new Object[sequencer.getBufferSize() + 2 * BUFFER_PAD];
        fill(eventFactory);
    }
}
```

`Integer.bitCount(bufferSize) != 1` 检查 `bufferSize` 是否是 2 的幂：只有 2 的幂在二进制表示中恰好有且只有一个 1。

`indexMask = bufferSize - 1` 就是后续做位运算取模的掩码。例如 `bufferSize = 1024`（2^10），则 `indexMask = 1023 = 0b1111111111`。任何序列号与 `indexMask` 做 AND 运算，就得到了 0~1023 之间的下标。

注意 entries 数组的实际大小是 `bufferSize + 2 * BUFFER_PAD`。`BUFFER_PAD` 的作用是在数组的首尾各添加填充元素，防止数组的头部元素与其他对象共享缓存行、尾部元素与其他对象共享缓存行。

```java
private static final int REF_ARRAY_BASE;
private static final int REF_ELEMENT_SHIFT;
private static final int BUFFER_PAD;

static {
    final int scale = UNSAFE.arrayIndexScale(Object[].class);  // 引用的大小（4 或 8 字节）
    if (4 == scale) {
        REF_ELEMENT_SHIFT = 2;
    } else if (8 == scale) {
        REF_ELEMENT_SHIFT = 3;
    } else {
        throw new IllegalStateException("Unknown pointer size");
    }
    BUFFER_PAD = 128 / scale;  // 128 字节 / 引用大小 = 填充元素个数
    REF_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class) + (BUFFER_PAD << REF_ELEMENT_SHIFT);
}
```

这里 `BUFFER_PAD = 128 / scale`。如果引用大小是 4 字节（开启压缩指针），则 `BUFFER_PAD = 32`；如果引用大小是 8 字节（关闭压缩指针），则 `BUFFER_PAD = 16`。无论哪种情况，前后各填充 128 字节（2 条缓存行），确保数组的有效元素区域不会与外部对象的内存区域共享缓存行。

### 3.3 预分配 Entry 对象与 EventFactory

```java
private void fill(EventFactory<E> eventFactory) {
    for (int i = 0; i < bufferSize; i++) {
        entries[BUFFER_PAD + i] = eventFactory.newInstance();
    }
}
```

这个循环在 RingBuffer 创建时就把所有事件对象预分配好了。`EventFactory` 是一个简单的工厂接口：

```java
public interface EventFactory<T> {
    T newInstance();
}
```

用户需要提供自己的实现：

```java
public class OrderEvent {
    private long orderId;
    private double price;
    private int quantity;

    // getters, setters, clear()
}

public class OrderEventFactory implements EventFactory<OrderEvent> {
    @Override
    public OrderEvent newInstance() {
        return new OrderEvent();
    }
}
```

预分配的关键意义在于：在 RingBuffer 创建之后的整个生命周期中，**不再需要创建新的事件对象**。生产者发布事件时，不是"把新对象放入数组"，而是"从数组中取出已有的对象，修改它的字段值"。这彻底消除了高频事件处理中的对象分配和 GC 压力。

这种设计也意味着事件对象会被反复复用。如果消费者在事件处理完后还持有事件的引用（例如把它加入了一个 List），后续生产者覆写同一个槽位时，消费者看到的"旧事件"内容就会变——这是 Disruptor 的一个重要约束：**消费者不应该在 `onEvent()` 返回后继续持有事件对象的引用**。如果需要保存，必须深拷贝。

### 3.4 RingBufferFields 的缓存行填充

RingBuffer 本身也做了缓存行填充，保护其内部字段不被外部对象干扰：

```java
// 左侧填充
abstract class RingBufferPad {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

// 实际字段
abstract class RingBufferFields<E> extends RingBufferPad {
    // ... indexMask, entries, bufferSize, sequencer 等
}

// 右侧填充 + 对外暴露的 API
public final class RingBuffer<E> extends RingBufferFields<E>
    implements Cursored, EventSequencer<E>, EventSink<E> {
    // 右侧填充
    protected long p1, p2, p3, p4, p5, p6, p7;

    // ... 公开方法
}
```

继承链：`RingBufferPad`（左填充 7×8=56B）→ `RingBufferFields`（实际字段）→ `RingBuffer`（右填充 56B + 方法）。确保 `indexMask`、`entries`、`bufferSize` 等字段不会与其他对象共享缓存行。

### 3.5 elementAt()：通过 Unsafe 直接计算数组偏移

```java
@SuppressWarnings("unchecked")
protected final E elementAt(long sequence) {
    return (E) UNSAFE.getObject(entries, REF_ARRAY_BASE + ((sequence & indexMask) << REF_ELEMENT_SHIFT));
}
```

这行代码是 Disruptor 中对性能最敏感的操作之一——根据序列号从数组中取出事件对象。我们拆解它：

1. `sequence & indexMask`：把序列号映射为数组下标（0 ~ bufferSize-1）
2. `<< REF_ELEMENT_SHIFT`：下标乘以引用大小（相当于计算字节偏移量）
3. `+ REF_ARRAY_BASE`：加上数组基础偏移（包含了 BUFFER_PAD 的跳过量）
4. `UNSAFE.getObject(entries, offset)`：直接通过内存地址读取引用

为什么不用 `entries[(int)(sequence & indexMask)]`？因为普通的数组访问 `entries[i]` 会插入数组边界检查（`if (i < 0 || i >= entries.length) throw ArrayIndexOutOfBoundsException`），这是一条条件分支指令。在主循环中每次事件处理都要做一次数组访问，消除边界检查可以省掉分支预测失败的代价。通过 Unsafe 直接计算偏移量，绕过了边界检查，且 JIT 编译器可以直接生成最优的 `MOV` 指令。

### 3.6 publishEvent() 与 tryPublishEvent()

生产者发布事件有两种主要路径：

```java
// 阻塞式发布：如果 RingBuffer 满了，会等待
public <A> void publishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {
    final long sequence = sequencer.next();    // 申请下一个序列号（可能阻塞）
    translateAndPublish(translator, sequence, arg0);
}

// 非阻塞式发布：如果 RingBuffer 满了，返回 false
public <A> boolean tryPublishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {
    try {
        final long sequence = sequencer.tryNext();  // 尝试申请（不阻塞）
        translateAndPublish(translator, sequence, arg0);
        return true;
    } catch (InsufficientCapacityException e) {
        return false;
    }
}

private <A> void translateAndPublish(
        EventTranslatorOneArg<E, A> translator, long sequence, A arg0) {
    try {
        translator.translateTo(get(sequence), sequence, arg0);  // 就地修改事件对象
    } finally {
        sequencer.publish(sequence);  // 发布（让消费者可见）
    }
}
```

`EventTranslatorOneArg` 是一个函数式接口：

```java
public interface EventTranslatorOneArg<T, A> {
    void translateTo(T event, long sequence, A arg0);
}
```

用户的典型实现：

```java
private static final EventTranslatorOneArg<OrderEvent, OrderData> TRANSLATOR =
    (event, sequence, data) -> {
        event.setOrderId(data.getId());
        event.setPrice(data.getPrice());
        event.setQuantity(data.getQuantity());
    };

// 发布事件
ringBuffer.publishEvent(TRANSLATOR, orderData);
```

注意 `translateAndPublish` 中的 `try-finally` 结构：即使 `translateTo` 抛出异常，`sequencer.publish(sequence)` 也会被调用。这是因为序列号已经被 `next()` 占用了，如果不发布，消费者会永远等待这个序列号，导致整个系统死锁。发布一个"空"或"异常"的事件总比死锁好——消费者端可以通过检查事件的某个标志位来识别无效事件。

### 3.7 get() 方法：消费者如何读取事件

```java
@SuppressWarnings("unchecked")
public E get(long sequence) {
    return elementAt(sequence);
}
```

消费者通过序列号从 RingBuffer 中获取事件对象的引用。由于事件对象是预分配的，`get()` 返回的是**同一个对象的引用**——消费者对事件的读取实际上是在读取数组中固定位置的对象的字段。

这里有一个重要的线程安全保证：当消费者调用 `get(sequence)` 时，它已经通过 SequenceBarrier 确认了这个序列号的事件已被生产者发布。生产者在发布前通过 `translator.translateTo()` 修改了事件对象的字段，发布操作（`sequencer.publish()`）包含了 volatile 写或 StoreStore 屏障，确保字段修改对消费者可见。消费者通过 `barrier.waitFor()` 中的 volatile 读获取到 cursor 值后，happens-before 关系保证了它能看到生产者写入的所有数据。

---

## 四、Sequencer：生产者协议的核心

### 4.1 Sequencer 接口定义

`Sequencer` 是生产者端的核心抽象，定义了申请序列号、发布事件、查询容量的协议：

```java
public interface Sequencer extends Cursored, Sequenced {
    long INITIAL_CURSOR_VALUE = -1L;

    void claim(long sequence);
    boolean isAvailable(long sequence);
    void addGatingSequences(Sequence... gatingSequences);
    boolean removeGatingSequence(Sequence sequence);
    SequenceBarrier newBarrier(Sequence... sequencesToTrack);
    long getMinimumSequence();
    long getHighestPublishedSequence(long nextSequence, long availableSequence);
    <T> void addGatingSequences(EventHandler<T>... gatingSequenceBarriers);
}

public interface Sequenced {
    int getBufferSize();
    boolean hasAvailableCapacity(int requiredCapacity);
    long remainingCapacity();
    long next();
    long next(int n);
    long tryNext() throws InsufficientCapacityException;
    long tryNext(int n) throws InsufficientCapacityException;
    void publish(long sequence);
    void publish(long lo, long hi);
}
```

`Sequencer` 有两个核心实现：`SingleProducerSequencer`（单生产者，无 CAS）和 `MultiProducerSequencer`（多生产者，CAS 竞争）。

### 4.2 SingleProducerSequencer 全源码解析

#### 4.2.1 next() / next(n)：申请序列号

```java
abstract class SingleProducerSequencerPad extends AbstractSequencer {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad {
    protected long nextValue = Sequence.INITIAL_VALUE;   // 下一个要分配的序列号
    protected long cachedValue = Sequence.INITIAL_VALUE;  // 缓存的最小消费者进度
}

public final class SingleProducerSequencer extends SingleProducerSequencerFields {
    protected long p1, p2, p3, p4, p5, p6, p7;

    @Override
    public long next(int n) {
        if (n < 1 || n > bufferSize) {
            throw new IllegalArgumentException("n must be > 0 and <= bufferSize");
        }

        long nextValue = this.nextValue;       // 当前生产者进度（线程本地，不需要 volatile）
        long nextSequence = nextValue + n;     // 申请到 nextSequence 位置
        long wrapPoint = nextSequence - bufferSize;  // 可能覆盖的位置

        long cachedGatingSequence = this.cachedValue;  // 上次缓存的最小消费者进度

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue) {
            // 可能要覆盖消费者还没读的数据 → 需要检查消费者的真实进度
            cursor.setVolatile(nextValue);  // 先发布当前进度（StoreLoad fence）

            long minSequence;
            // 自旋等待：直到最慢的消费者进度超过 wrapPoint
            while (wrapPoint > (minSequence = Util.getMinimumSequence(gatingSequences, nextValue))) {
                LockSupport.parkNanos(1L);  // 等待 1ns（实际上是让出 CPU 时间片）
            }

            this.cachedValue = minSequence;  // 更新缓存
        }

        this.nextValue = nextSequence;  // 前进生产者进度
        return nextSequence;
    }
}
```

这段代码是 Disruptor 中**最关键的控制流之一**，逐行拆解：

**`nextValue`**：这是生产者的本地计数器，不需要 volatile 修饰，因为单生产者场景下只有一个线程会写这个字段。每次申请序列号，只需要 `nextValue += n` 即可，没有任何原子操作，这就是单生产者模式极快的根源。

**`wrapPoint = nextSequence - bufferSize`**：这是环形数组"绕一圈回来"后的位置。例如 bufferSize=1024，当前 nextSequence=1025，则 wrapPoint=1。这意味着序列号 1025 和序列号 1 映射到数组的同一个位置（`1025 & 1023 == 1`）。如果消费者还没有消费序列号 1 的事件，生产者就不能发布序列号 1025，否则会覆盖未消费的数据。

**`cachedGatingSequence`**：这是一个优化——缓存最小消费者进度，避免每次都去读 `gatingSequences` 数组。只有当 `wrapPoint > cachedGatingSequence`（可能要覆盖）时才真正去检查消费者的最新进度。这个优化在 RingBuffer 较大（如 65536）且消费者跟得上生产者的情况下，可以让大部分 `next()` 调用跳过 `Util.getMinimumSequence()` 这个需要遍历所有消费者 Sequence 的操作。

**`cursor.setVolatile(nextValue)`**：在进入等待循环前，生产者先发布自己的当前进度。这是因为消费者需要通过 cursor 来知道"有多少事件可以消费"。如果不先发布，可能出现死锁：生产者在等消费者前进，消费者在等生产者发布新事件。

**`LockSupport.parkNanos(1L)`**：名义上是"等待 1 纳秒"，但实际上 `parkNanos(1)` 的行为更接近于 `Thread.yield()` 或一次调度让步。它让当前线程让出 CPU 时间片，避免纯自旋消耗 100% CPU。这种方式比 `Thread.sleep(1)` 开销更低（sleep 的最小精度通常是 1ms），比 `Thread.yield()` 更可靠（yield 在某些 OS 上可能被忽略）。

**为什么条件判断还包含 `cachedGatingSequence > nextValue`？** 这处理的是初始化和 sequence 环绕的边界情况。当 cachedGatingSequence 因为某种原因（初始值 -1）大于 nextValue 时，说明缓存失效了，需要重新读取消费者的真实进度。

#### 4.2.2 publish() / publish(lo, hi)：发布事件

```java
@Override
public void publish(long sequence) {
    cursor.set(sequence);               // lazySet：release 语义
    waitStrategy.signalAllWhenBlocking(); // 唤醒等待的消费者
}

@Override
public void publish(long lo, long hi) {
    publish(hi);  // 批量发布只需要发布最后一个序列号
}
```

`cursor.set(sequence)` 使用的是 lazySet（release 语义）。前面分析过，Sequence 的 `set()` 方法底层是 `putOrderedLong()`，只提供 StoreStore 屏障而不提供 StoreLoad 屏障。这里的设计权衡是：

- 使用 lazySet 后，消费者可能不会立即看到新的 cursor 值（延迟通常在纳秒级）
- 但 `waitStrategy.signalAllWhenBlocking()` 会通过其他机制（如 `Condition.signalAll()` 或 volatile 写）来确保及时唤醒
- 在高吞吐场景下，消费者通常是自旋等待的（BusySpin/Yielding 策略），不依赖 signal 机制，此时 lazySet 的延迟可以忽略不计
- 省掉 StoreLoad 屏障在 x86 上节省的时间约 10~30ns，在高频发布时累积效果显著

批量发布 `publish(lo, hi)` 只需要发布最后一个序列号 `hi`，因为序列号是连续递增的，消费者看到 cursor >= hi，自然知道 lo 到 hi 的所有事件都已发布。

#### 4.2.3 cachedGatingSequence 优化

`cachedGatingSequence` 是 SingleProducerSequencer 中最精妙的优化之一。我们来分析它的效果：

假设 RingBuffer 大小为 1024，有 3 个消费者（C1, C2, C3），生产者以极高速度生产事件。不使用缓存的情况下，每次 `next()` 都需要调用 `Util.getMinimumSequence(gatingSequences, nextValue)`，该方法遍历 3 个 Sequence 对象，读取 3 个 volatile long——这意味着每次生产一个事件都需要 3 次 volatile 读（跨核缓存一致性流量）。

使用缓存后：只要 `nextSequence - bufferSize <= cachedGatingSequence`，就直接跳过检查。消费者只要不落后超过整个 bufferSize，生产者可以连续发布 1024 个事件而只做 1 次 `getMinimumSequence()` 调用。这将 volatile 读的频率降低了约 **1024 倍**。

这个优化的思路与 Netty 的 MpscAtomicIntegerArrayQueue 中的 `producerLimit` 缓存异曲同工——都是"缓存一个阈值，在阈值内免读 volatile"。第 14 篇文档中详细分析过这个技术（见第六章第 8 节"producerLimit 缓存"）。

#### 4.2.4 hasAvailableCapacity() 与 remainingCapacity()

```java
@Override
public boolean hasAvailableCapacity(int requiredCapacity) {
    return hasAvailableCapacity(requiredCapacity, false);
}

private boolean hasAvailableCapacity(int requiredCapacity, boolean doStore) {
    long nextValue = this.nextValue;
    long wrapPoint = (nextValue + requiredCapacity) - bufferSize;
    long cachedGatingSequence = this.cachedValue;

    if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue) {
        if (doStore) {
            cursor.setVolatile(nextValue);
        }
        long minSequence = Util.getMinimumSequence(gatingSequences, nextValue);
        this.cachedValue = minSequence;
        if (wrapPoint > minSequence) {
            return false;
        }
    }
    return true;
}

@Override
public long remainingCapacity() {
    long nextValue = this.nextValue;
    long consumed = Util.getMinimumSequence(gatingSequences, nextValue);
    long produced = nextValue;
    return getBufferSize() - (produced - consumed);
}
```

`hasAvailableCapacity()` 是 `tryNext()` 的前置检查版本：判断是否有足够的空间，但不实际申请序列号。同样使用了 `cachedGatingSequence` 优化。

`remainingCapacity()` 计算剩余容量：`bufferSize - (produced - consumed)`。注意这里没有使用缓存，而是直接读取消费者的最新进度，因为 `remainingCapacity()` 通常用于监控/诊断，调用频率低，不需要极致性能。

### 4.3 MultiProducerSequencer 全源码解析

#### 4.3.1 availableBuffer 数组：标记已发布的槽位

多生产者场景引入了一个关键的复杂性：多个生产者通过 CAS 竞争序列号，它们可能以不同的顺序完成事件的填充和发布。例如，线程 A 获得序列号 5，线程 B 获得序列号 6，但线程 B 可能先完成发布——此时消费者不能消费序列号 6，因为序列号 5 还没发布。

`MultiProducerSequencer` 引入了一个 `int[] availableBuffer` 来追踪每个槽位是否已发布：

```java
public final class MultiProducerSequencer extends AbstractSequencer {
    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final int[] availableBuffer;
    private final int indexMask;
    private final int indexShift;

    public MultiProducerSequencer(int bufferSize, WaitStrategy waitStrategy) {
        super(bufferSize, waitStrategy);
        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        indexShift = Util.log2(bufferSize);
        initialiseAvailableBuffer();
    }

    private void initialiseAvailableBuffer() {
        for (int i = availableBuffer.length - 1; i != 0; i--) {
            setAvailableBufferValue(i, -1);
        }
        setAvailableBufferValue(0, -1);
    }
}
```

`availableBuffer` 的大小与 RingBuffer 相同，每个槽位对应一个 int 值。这个 int 值存储的是"第几圈"的标记——当槽位 `i` 上的事件属于第 `flag` 圈时，`availableBuffer[i] = flag`。初始值 -1 表示"尚未发布过"。

`indexShift = Util.log2(bufferSize)` 是计算"圈数"时需要的右移量。例如 bufferSize=1024 时 indexShift=10，序列号 1025 的圈数是 `1025 >>> 10 = 1`（第 1 圈，从 0 开始计数）。

#### 4.3.2 next() / next(n)：CAS 竞争序列号

```java
@Override
public long next(int n) {
    if (n < 1 || n > bufferSize) {
        throw new IllegalArgumentException("n must be > 0 and <= bufferSize");
    }

    long current;
    long next;

    do {
        current = cursor.get();    // volatile 读当前游标
        next = current + n;

        long wrapPoint = next - bufferSize;
        long cachedGatingSequence = gatingSequenceCache.get();

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current) {
            long gatingSequence = Util.getMinimumSequence(gatingSequences, current);

            if (wrapPoint > gatingSequence) {
                LockSupport.parkNanos(1L);
                continue;  // 容量不足，重试
            }

            gatingSequenceCache.set(gatingSequence);  // 更新缓存
        } else if (cursor.compareAndSet(current, next)) {
            break;  // CAS 成功，序列号申请完成
        }
    } while (true);

    return next;
}
```

与 SingleProducerSequencer 的关键区别：

1. **`cursor` 是一个 Sequence 对象而非普通字段**。多个生产者共享同一个 cursor，需要通过 CAS 竞争。`cursor.get()` 是 volatile 读，`cursor.compareAndSet()` 是原子操作。

2. **do-while CAS 循环**。如果两个生产者同时读到 `current=100` 并尝试 `CAS(100, 101)`，只有一个会成功，另一个重新读取 `current`（此时已变为 101）再尝试 `CAS(101, 102)`。这就是典型的"乐观锁"模式——不加锁，用 CAS 重试替代。

3. **`gatingSequenceCache` 是一个 Sequence 对象**（而非 SingleProducer 中的普通 long 字段），因为多个生产者线程可能同时读写缓存，需要 volatile 语义保证可见性。但注意它使用 `set()`（lazySet）而非 `setVolatile()`——因为缓存的一致性不需要实时精确，稍有延迟也只是导致多做一次 `getMinimumSequence()` 检查。

4. **容量不足时用 `continue` 重试而非 `while` 等待**。这是因为 CAS 循环的外层已经是 `do-while(true)`，容量不足时 `continue` 回到循环顶部重新检查。

#### 4.3.3 publish()：标记 availableBuffer

```java
@Override
public void publish(final long sequence) {
    setAvailable(sequence);
    waitStrategy.signalAllWhenBlocking();
}

@Override
public void publish(long lo, long hi) {
    for (long l = lo; l <= hi; l++) {
        setAvailable(l);
    }
    waitStrategy.signalAllWhenBlocking();
}

private void setAvailable(final long sequence) {
    setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
}

private void setAvailableBufferValue(int index, int flag) {
    long bufferAddress = (index * SCALE) + BASE;
    UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
}

private int calculateAvailabilityFlag(final long sequence) {
    return (int) (sequence >>> indexShift);
}

private int calculateIndex(final long sequence) {
    return ((int) sequence) & indexMask;
}
```

发布操作分两步：

1. `calculateIndex(sequence)`：计算数组下标，`sequence & indexMask`
2. `calculateAvailabilityFlag(sequence)`：计算圈数标记，`sequence >>> indexShift`
3. `setAvailableBufferValue(index, flag)`：通过 `putOrderedInt` 写入 availableBuffer

例如 bufferSize=1024（indexShift=10），序列号 2050：
- `index = 2050 & 1023 = 2`（映射到数组下标 2）
- `flag = 2050 >>> 10 = 2`（第 2 圈）
- `availableBuffer[2] = 2`

消费者检查序列号 2050 是否已发布时，会检查 `availableBuffer[2] == 2`。如果值为 1（上一圈的标记），说明新事件尚未发布。

`putOrderedInt` 是 lazySet 语义——与 SingleProducerSequencer 的 `publish()` 一样，使用 release 语义降低内存屏障开销。

#### 4.3.4 isAvailable() 与 getHighestPublishedSequence()

```java
@Override
public boolean isAvailable(long sequence) {
    int index = calculateIndex(sequence);
    int flag = calculateAvailabilityFlag(sequence);
    long bufferAddress = (index * SCALE) + BASE;
    return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
}

@Override
public long getHighestPublishedSequence(long lowerBound, long availableSequence) {
    for (long sequence = lowerBound; sequence <= availableSequence; sequence++) {
        if (!isAvailable(sequence)) {
            return sequence - 1;
        }
    }
    return availableSequence;
}
```

`isAvailable()` 检查特定序列号是否已发布：读取 `availableBuffer[index]` 的值，与预期的 flag 比较。注意这里使用 `getIntVolatile`（volatile 读），确保看到最新的发布状态。

`getHighestPublishedSequence()` 是消费者用来确定连续已发布范围的：从 `lowerBound` 开始往上扫描，找到第一个未发布的序列号，返回它的前一个。例如序列号 5、6、8 已发布但 7 未发布（生产者 B 获得了 7 但还没填完），则 `getHighestPublishedSequence(5, 8)` 返回 6——消费者只能消费到 6，不能跳过 7 直接消费 8。

这就是 **连续性保证**：消费者始终按序列号顺序消费，不会出现"空洞"。

#### 4.3.5 多生产者下的连续性保证

多生产者模式的核心复杂性在于：CAS 竞争只保证序列号的唯一分配，但不保证发布的顺序。生产者 A 拿到序列号 5 后填充事件可能需要 10μs，生产者 B 拿到序列号 6 后填充只需要 1μs——B 先完成发布，A 后完成。

`availableBuffer` 的设计完美解决了这个问题：

1. 每个槽位独立标记发布状态，不需要等待前面的序列号先发布
2. `getHighestPublishedSequence()` 在消费者端做连续性扫描，确保不跳过空洞
3. 扫描操作是纯内存读取（读 int 数组），没有原子操作，性能很高

这种设计的代价是：消费者可能看到连续范围比实际可用范围短（因为中间有生产者还没发布完）。但这只是延迟了消费，不影响正确性。在实际场景中，事件填充通常很快完成，空洞持续时间极短。

### 4.4 SingleProducer vs MultiProducer 的性能差异量化

Martin Thompson 的基准测试数据（2011 年，3.2GHz Core i7）：

| 场景 | SingleProducer | MultiProducer | 差异 |
|------|---------------|---------------|------|
| 1P-1C 吞吐 (ops/s) | ~180M | ~22M | 8x |
| 1P-1C P99 延迟 (ns) | ~52 | ~150 | 3x |
| 空 next() 调用 | ~6ns | ~30ns | 5x |

SingleProducer 的 `next()` 只做了一次普通的 long 加法和一次条件判断（大多数时候条件为 false 直接跳过），没有任何 volatile 读/写或 CAS。MultiProducer 的 `next()` 需要一次 volatile 读（`cursor.get()`）、一次 CAS（`cursor.compareAndSet()`），在竞争激烈时还需要多次重试。

**核心教训**：如果你的应用只有一个生产者线程（这在大多数场景下是成立的），一定要用 `SingleProducerSequencer`。Disruptor 的 DSL 默认就是单生产者模式，需要多生产者时才显式指定 `ProducerType.MULTI`。

---

## 五、SequenceBarrier：消费者等待的屏障

### 5.1 为什么需要 SequenceBarrier

在 Disruptor 的设计中，消费者不直接与 Sequencer 或 RingBuffer 交互来判断"有没有新事件可消费"。取而代之，消费者通过一个 `SequenceBarrier` 来协调等待。为什么要引入这个中间层？

原因在于 Disruptor 支持复杂的消费者依赖图。考虑以下场景：

```
Producer → [Journal Handler] → [Replication Handler] → [Business Handler]
                              ↗
```

Business Handler 需要在 Journal Handler 和 Replication Handler 都处理完之后才能开始处理。如果没有 SequenceBarrier，Business Handler 需要自己去轮询 Journal 和 Replication 的 Sequence，并计算最小值，逻辑会非常复杂且容易出错。

SequenceBarrier 封装了这个逻辑：它知道当前消费者依赖哪些前置消费者（dependentSequences），并通过 `waitFor(sequence)` 方法阻塞/自旋直到所有依赖都满足。

### 5.2 ProcessingSequenceBarrier 全源码解析

```java
final class ProcessingSequenceBarrier implements SequenceBarrier {
    private final WaitStrategy waitStrategy;
    private final Sequence dependentSequence;
    private volatile boolean alerted = false;
    private final Sequence cursorSequence;
    private final Sequencer sequencer;

    ProcessingSequenceBarrier(
            final Sequencer sequencer,
            final WaitStrategy waitStrategy,
            final Sequence cursorSequence,
            final Sequence... dependentSequences) {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;
        if (0 == dependentSequences.length) {
            dependentSequence = cursorSequence;
        } else {
            dependentSequence = new FixedSequenceGroup(dependentSequences);
        }
    }

    @Override
    public long waitFor(final long sequence) throws AlertException, InterruptedException, TimeoutException {
        checkAlert();

        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this);

        if (availableSequence < sequence) {
            return availableSequence;
        }

        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    @Override
    public long getCursor() {
        return dependentSequence.get();
    }

    @Override
    public boolean isAlerted() {
        return alerted;
    }

    @Override
    public void alert() {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void clearAlert() {
        alerted = false;
    }

    @Override
    public void checkAlert() throws AlertException {
        if (alerted) {
            throw AlertException.INSTANCE;
        }
    }
}
```

### 5.3 dependentSequence：消费者依赖链

构造函数中的 `dependentSequences` 参数决定了这个消费者依赖谁：

- **无依赖（`dependentSequences.length == 0`）**：消费者直接依赖生产者的 cursor。`dependentSequence = cursorSequence`，即消费者等待的是生产者的发布进度。
- **有依赖**：消费者依赖其他消费者。`dependentSequence = new FixedSequenceGroup(dependentSequences)`，`FixedSequenceGroup.get()` 返回所有依赖 Sequence 的最小值。

```java
public final class FixedSequenceGroup extends Sequence {
    private final Sequence[] sequences;

    public FixedSequenceGroup(Sequence[] sequences) {
        this.sequences = Arrays.copyOf(sequences, sequences.length);
    }

    @Override
    public long get() {
        return Util.getMinimumSequence(sequences);
    }

    // set() 和 compareAndSet() 抛 UnsupportedOperationException
}
```

`FixedSequenceGroup` 是一个"虚拟的 Sequence"——它的 `get()` 返回一组 Sequence 的最小值，而自身不存储任何状态。这是一个典型的**组合模式（Composite Pattern）**的应用，让单个 Sequence 和一组 Sequence 具有统一的接口。

### 5.4 waitFor() 的协作流程

`waitFor(sequence)` 的完整流程：

1. `checkAlert()`：检查是否收到关闭信号（alert），如果是则抛出 `AlertException`
2. `waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this)`：通过等待策略等待直到 `dependentSequence.get() >= sequence`。不同的 WaitStrategy 有不同的等待方式（自旋、yield、阻塞等）
3. 如果 `availableSequence < sequence`：说明等待被中断了（通常是 alert），返回当前进度
4. `sequencer.getHighestPublishedSequence(sequence, availableSequence)`：对于 MultiProducerSequencer，需要确认 [sequence, availableSequence] 范围内的所有序列号都已发布（消除空洞）。对于 SingleProducerSequencer，直接返回 `availableSequence`（单生产者不会有空洞）

### 5.5 alert 机制：优雅关闭

`alert` 是一个 volatile boolean 标志，用于优雅关闭消费者。当需要停止消费者时：

1. 调用 `barrier.alert()`：设置 `alerted = true`，并通过 `waitStrategy.signalAllWhenBlocking()` 唤醒可能阻塞的消费者线程
2. 消费者线程在 `waitFor()` 中通过 `checkAlert()` 检测到 alert，抛出 `AlertException`
3. `BatchEventProcessor` 捕获 `AlertException`，退出主循环，调用 `EventHandler.onShutdown()`

`AlertException` 使用了单例模式：

```java
public final class AlertException extends Exception {
    public static final AlertException INSTANCE = new AlertException();

    private AlertException() {
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;  // 不填充堆栈跟踪，避免创建开销
    }
}
```

覆盖 `fillInStackTrace()` 返回 `this` 是一个性能优化——标准的 `new Exception()` 会调用 `fillInStackTrace()` 来捕获当前线程的完整调用栈，这是一个相当昂贵的操作（需要遍历栈帧）。由于 `AlertException` 只是一个信号，不需要堆栈信息，覆盖掉可以消除这个开销。

---

## 六、WaitStrategy：等待策略全族谱

WaitStrategy 决定了消费者在没有新事件时如何等待。这是 Disruptor 最体现"Mechanical Sympathy"理念的组件——不同的硬件配置和延迟要求需要不同的等待策略。

### 6.1 BlockingWaitStrategy（默认）

```java
public final class BlockingWaitStrategy implements WaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();

    @Override
    public long waitFor(long sequence, Sequence cursorSequence,
                        Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        if (cursorSequence.get() < sequence) {
            lock.lock();
            try {
                while (cursorSequence.get() < sequence) {
                    barrier.checkAlert();
                    processorNotifyCondition.await();
                }
            } finally {
                lock.unlock();
            }
        }

        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        lock.lock();
        try {
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
```

BlockingWaitStrategy 使用 `ReentrantLock` + `Condition` 实现经典的等待/通知模式。当消费者发现 `cursorSequence.get() < sequence`（没有新事件）时，调用 `condition.await()` 让线程挂起，直到生产者调用 `signalAllWhenBlocking()` 唤醒。

第二个 `while` 循环处理依赖 Sequence 的等待：即使生产者已发布，消费者可能还需要等待前置消费者完成。这里用自旋（配合 `ThreadHints.onSpinWait()`）而非再次阻塞——因为前置消费者通常很快完成，自旋比再走一遍 lock/await/signal 更高效。

`ThreadHints.onSpinWait()` 在 Java 9+ 中调用 `Thread.onSpinWait()`，在 x86 上编译为 `PAUSE` 指令——告诉 CPU"我在自旋等待"，CPU 会降低功耗并避免 Pipeline 饥饿。在 Java 9 之前，这是一个空操作。

**适用场景**：低延迟不是首要目标，但不希望消费者空转浪费 CPU。适合事件频率中等偏低的场景。
**延迟**：从事件发布到消费的延迟取决于 `condition.await()` 到 `signalAll()` 的唤醒延迟，通常在 **微秒级**（1~50μs）。

### 6.2 SleepingWaitStrategy（三阶段退避）

```java
public final class SleepingWaitStrategy implements WaitStrategy {
    private static final int DEFAULT_RETRIES = 200;
    private static final long DEFAULT_SLEEP = 100;

    private final int retries;
    private final long sleepTimeNs;

    public SleepingWaitStrategy() {
        this(DEFAULT_RETRIES, DEFAULT_SLEEP);
    }

    public SleepingWaitStrategy(int retries, long sleepTimeNs) {
        this.retries = retries;
        this.sleepTimeNs = sleepTimeNs;
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence,
                        SequenceBarrier barrier)
            throws AlertException {
        long availableSequence;
        int counter = retries;

        while ((availableSequence = dependentSequence.get()) < sequence) {
            counter = applyWaitMethod(barrier, counter);
        }

        return availableSequence;
    }

    private int applyWaitMethod(final SequenceBarrier barrier, int counter)
            throws AlertException {
        barrier.checkAlert();

        if (counter > 100) {
            // 第一阶段：前 100 次纯自旋
            --counter;
        } else if (counter > 0) {
            // 第二阶段：100 次 yield
            --counter;
            Thread.yield();
        } else {
            // 第三阶段：parkNanos 睡眠
            LockSupport.parkNanos(sleepTimeNs);
        }

        return counter;
    }

    @Override
    public void signalAllWhenBlocking() {
        // 不需要信号，消费者会自己醒来
    }
}
```

SleepingWaitStrategy 实现了一个**三阶段退避**策略：

1. **自旋阶段（counter 200→100）**：前 100 次迭代纯自旋，不做任何让步。如果事件在这个阶段到来，延迟最低（纳秒级）。
2. **yield 阶段（counter 100→0）**：接下来 100 次调用 `Thread.yield()`，让当前线程让出 CPU 时间片给同优先级的其他线程，但仍然保持可运行状态。
3. **睡眠阶段（counter <= 0）**：进入 `LockSupport.parkNanos(100)` 短睡眠模式，让 CPU 几乎空闲。

`signalAllWhenBlocking()` 是空实现——消费者会自己从 parkNanos 中醒来，不需要生产者显式唤醒。

**适用场景**：对延迟有一定要求，但不想消耗太多 CPU。是**性能和 CPU 使用率之间最好的折中方案**。在异步日志（如 Log4j2 AsyncLogger）等场景中广泛使用。
**延迟**：最好情况纳秒级（自旋阶段命中），最差情况约 100ns~1μs（睡眠阶段）。

### 6.3 YieldingWaitStrategy（自旋+yield）

```java
public final class YieldingWaitStrategy implements WaitStrategy {
    private static final int SPIN_TRIES = 100;

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence,
                        SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        int counter = SPIN_TRIES;

        while ((availableSequence = dependentSequence.get()) < sequence) {
            counter = applyWaitMethod(barrier, counter);
        }

        return availableSequence;
    }

    private int applyWaitMethod(final SequenceBarrier barrier, int counter)
            throws AlertException {
        barrier.checkAlert();

        if (0 == counter) {
            Thread.yield();
        } else {
            --counter;
        }

        return counter;
    }

    @Override
    public void signalAllWhenBlocking() {
        // 不需要信号
    }
}
```

YieldingWaitStrategy 只有两个阶段：前 100 次纯自旋，之后持续 `Thread.yield()`。没有睡眠阶段，所以 CPU 使用率比 SleepingWaitStrategy 高，但延迟更低。

**适用场景**：需要低延迟且可以容忍消费者线程占满一个 CPU 核心。如果你的消费者线程数不超过物理核心数，这是一个很好的选择。
**延迟**：通常在 **亚微秒到微秒级**。

### 6.4 BusySpinWaitStrategy（纯自旋，绑核场景）

```java
public final class BusySpinWaitStrategy implements WaitStrategy {
    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence,
                        SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;

        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        // 不需要信号
    }
}
```

BusySpinWaitStrategy 是最激进的策略——**永远不让出 CPU**。循环中只做 `dependentSequence.get()` 的 volatile 读和 `ThreadHints.onSpinWait()`（x86 上的 `PAUSE` 指令）。

**适用场景**：追求极致延迟（纳秒级），且消费者线程被绑定到专用的 CPU 核心上（通过 `taskset` 或 `isolcpus` 内核参数）。如果没有绑核就使用这个策略，消费者线程会 100% 占满一个核心，影响同核心上的其他线程。
**延迟**：**纳秒级**，是所有策略中最低的。
**CPU 使用**：100%——消费者线程永远在运行，即使没有事件可消费。

### 6.5 LiteBlockingWaitStrategy（轻量阻塞）

```java
public final class LiteBlockingWaitStrategy implements WaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    private final AtomicBoolean signalNeeded = new AtomicBoolean(false);

    @Override
    public long waitFor(long sequence, Sequence cursorSequence,
                        Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException {
        long availableSequence;
        if (cursorSequence.get() < sequence) {
            lock.lock();
            try {
                do {
                    signalNeeded.getAndSet(true);  // 标记"我需要被唤醒"

                    if (cursorSequence.get() >= sequence) {
                        break;
                    }

                    barrier.checkAlert();
                    processorNotifyCondition.await();
                } while (cursorSequence.get() < sequence);
            } finally {
                lock.unlock();
            }
        }

        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        if (signalNeeded.getAndSet(false)) {  // 只有消费者确实在等待时才唤醒
            lock.lock();
            try {
                processorNotifyCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
```

LiteBlockingWaitStrategy 在 BlockingWaitStrategy 的基础上做了一个关键优化：**通过 `signalNeeded` 标志避免不必要的唤醒操作**。

在 BlockingWaitStrategy 中，每次生产者发布事件都会调用 `signalAllWhenBlocking()`，即使消费者可能正在自旋而非阻塞。`lock.lock()` + `condition.signalAll()` + `lock.unlock()` 的组合即使在无竞争时也有可观的开销（约 100~200ns）。

LiteBlockingWaitStrategy 用一个 `AtomicBoolean signalNeeded` 来标记"消费者是否确实在等待"。消费者进入 `await()` 前设置 `signalNeeded = true`，生产者在 `signalAllWhenBlocking()` 中先检查 `signalNeeded.getAndSet(false)`——如果为 false，说明没有消费者在等待，直接返回而不走 lock/signal 路径。

**适用场景**：与 BlockingWaitStrategy 相同，但在"消费者大部分时间跟得上生产者"的场景下性能更好。

### 6.6 TimeoutBlockingWaitStrategy（超时阻塞）

```java
public class TimeoutBlockingWaitStrategy implements WaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    private final long timeoutInNanos;

    public TimeoutBlockingWaitStrategy(final long timeout, final TimeUnit units) {
        timeoutInNanos = units.toNanos(timeout);
    }

    @Override
    public long waitFor(long sequence, Sequence cursorSequence,
                        Sequence dependentSequence, SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException {
        long nanos = timeoutInNanos;

        long availableSequence;
        if (cursorSequence.get() < sequence) {
            lock.lock();
            try {
                while (cursorSequence.get() < sequence) {
                    barrier.checkAlert();
                    nanos = processorNotifyCondition.awaitNanos(nanos);
                    if (nanos <= 0) {
                        throw TimeoutException.INSTANCE;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }
}
```

TimeoutBlockingWaitStrategy 在 BlockingWaitStrategy 的基础上增加了超时控制。如果消费者等待超过指定时间（如 1 秒），会抛出 `TimeoutException`，消费者可以在异常处理中做"心跳"操作（如检查是否需要关闭、输出统计信息等）。

`TimeoutException.INSTANCE` 与 `AlertException.INSTANCE` 一样是单例，且覆盖了 `fillInStackTrace()` 避免堆栈捕获开销。

**适用场景**：需要消费者定期执行超时任务的场景，如心跳检查、定期刷新缓存等。

### 6.7 PhasedBackoffWaitStrategy（组合策略）

```java
public final class PhasedBackoffWaitStrategy implements WaitStrategy {
    private static final int SPIN_TRIES = 10000;
    private final long spinTimeoutNanos;
    private final long yieldTimeoutNanos;
    private final WaitStrategy fallbackStrategy;

    private PhasedBackoffWaitStrategy(long spinTimeout, long yieldTimeout,
                                      TimeUnit units, WaitStrategy fallbackStrategy) {
        this.spinTimeoutNanos = units.toNanos(spinTimeout);
        this.yieldTimeoutNanos = spinTimeoutNanos + units.toNanos(yieldTimeout);
        this.fallbackStrategy = fallbackStrategy;
    }

    public static PhasedBackoffWaitStrategy withLock(
            long spinTimeout, long yieldTimeout, TimeUnit units) {
        return new PhasedBackoffWaitStrategy(
            spinTimeout, yieldTimeout, units,
            new BlockingWaitStrategy());
    }

    public static PhasedBackoffWaitStrategy withLiteLock(
            long spinTimeout, long yieldTimeout, TimeUnit units) {
        return new PhasedBackoffWaitStrategy(
            spinTimeout, yieldTimeout, units,
            new LiteBlockingWaitStrategy());
    }

    public static PhasedBackoffWaitStrategy withSleep(
            long spinTimeout, long yieldTimeout, TimeUnit units) {
        return new PhasedBackoffWaitStrategy(
            spinTimeout, yieldTimeout, units,
            new SleepingWaitStrategy(0, 1));
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence,
                        SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException {
        long availableSequence;
        long startTime = 0;
        int counter = SPIN_TRIES;

        do {
            if ((availableSequence = dependentSequence.get()) >= sequence) {
                return availableSequence;
            }

            if (0 == --counter) {
                if (0 == startTime) {
                    startTime = System.nanoTime();
                } else {
                    long timeDelta = System.nanoTime() - startTime;
                    if (timeDelta > yieldTimeoutNanos) {
                        return fallbackStrategy.waitFor(sequence, cursor, dependentSequence, barrier);
                    } else if (timeDelta > spinTimeoutNanos) {
                        Thread.yield();
                    }
                }
                counter = SPIN_TRIES;
            }
        } while (true);
    }
}
```

PhasedBackoffWaitStrategy 是最灵活的策略，将等待过程分为三个精确定时的阶段：

1. **自旋阶段**（0 ~ spinTimeout）：纯自旋等待，延迟最低
2. **yield 阶段**（spinTimeout ~ yieldTimeout）：调用 `Thread.yield()`，让出 CPU 但保持可运行
3. **fallback 阶段**（超过 yieldTimeout）：委托给一个 fallback 策略（BlockingWaitStrategy / LiteBlockingWaitStrategy / SleepingWaitStrategy）

这种设计允许用户精确控制在每个阶段花费多长时间。例如 `withLock(10, 1000, MICROSECONDS)` 表示"先自旋 10μs，再 yield 1000μs，然后阻塞等待"。

### 6.8 各策略的适用场景与延迟-吞吐量权衡矩阵

| 策略 | CPU 使用 | 最佳延迟 | 最差延迟 | 适用场景 | 需要绑核 |
|------|---------|---------|---------|---------|---------|
| BusySpinWaitStrategy | 100% | ~ns | ~ns | 极致低延迟（金融交易） | 是 |
| YieldingWaitStrategy | 高 | ~ns | ~μs | 低延迟 + 允许少量 CPU 让步 | 推荐 |
| SleepingWaitStrategy | 中 | ~ns | ~100ns | 异步日志、后台处理 | 否 |
| BlockingWaitStrategy | 低 | ~μs | ~50μs | 通用场景、低频事件 | 否 |
| LiteBlockingWaitStrategy | 低 | ~μs | ~50μs | 消费者通常能跟上的场景 | 否 |
| TimeoutBlockingWaitStrategy | 低 | ~μs | ~50μs | 需要超时回调的场景 | 否 |
| PhasedBackoffWaitStrategy | 可调 | ~ns | 取决于fallback | 需要精确控制退避时间的场景 | 可选 |

选择策略的决策树：
- 是否追求纳秒级延迟？→ 是：是否能绑核？→ 是：BusySpin → 否：Yielding
- 是否追求低 CPU 使用率？→ 是：事件频率是否高？→ 高：LiteBlocking → 低：Blocking
- 中间路线？→ Sleeping（Log4j2 的默认选择）

---

## 七、EventProcessor 与 BatchEventProcessor：消费者引擎

### 7.1 EventProcessor 接口

```java
public interface EventProcessor extends Runnable {
    Sequence getSequence();
    void halt();
    boolean isRunning();
}
```

EventProcessor 是消费者的执行引擎，它实现了 `Runnable` 接口，由一个独立的线程来运行。每个 EventProcessor 拥有一个 `Sequence` 来跟踪自己的消费进度。`halt()` 用于优雅关闭。

### 7.2 BatchEventProcessor 全源码解析

#### 7.2.1 run() 主循环

```java
public final class BatchEventProcessor<T> implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private ExceptionHandler<? super T> exceptionHandler;
    private final DataProvider<T> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final EventHandler<? super T> eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final TimeoutHandler timeoutHandler;
    private final BatchStartAware batchStartAware;

    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            sequenceBarrier.clearAlert();

            notifyStart();  // 通知 EventHandler.onStart()
            try {
                if (running.get() == RUNNING) {
                    processEvents();
                }
            } finally {
                notifyShutdown();  // 通知 EventHandler.onShutdown()
                running.set(IDLE);
            }
        } else {
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running");
            } else {
                earlyExit();
            }
        }
    }
}
```

`run()` 方法的入口通过 CAS 保证只有一个线程能启动处理器。`running` 字段有三个状态：IDLE（未启动）、RUNNING（运行中）、HALTED（已停止）。启动前清除 alert 标志，确保不会因为上一次的关闭信号而立即退出。

#### 7.2.2 processEvents()：批量消费的实现

```java
private void processEvents() {
    T event = null;
    long nextSequence = sequence.get() + 1L;  // 下一个待消费的序列号

    while (true) {
        try {
            final long availableSequence = sequenceBarrier.waitFor(nextSequence);

            if (batchStartAware != null && availableSequence >= nextSequence) {
                batchStartAware.onBatchStart(availableSequence - nextSequence + 1);
            }

            while (nextSequence <= availableSequence) {
                event = dataProvider.get(nextSequence);
                eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                nextSequence++;
            }

            sequence.set(availableSequence);  // 更新消费进度

        } catch (final TimeoutException e) {
            notifyTimeout(sequence.get());
        } catch (final AlertException ex) {
            if (running.get() != RUNNING) {
                break;  // 优雅关闭
            }
        } catch (final Throwable ex) {
            handleEventException(ex, nextSequence, event);
            sequence.set(nextSequence);
            nextSequence++;
        }
    }
}
```

这是 Disruptor 消费者端最核心的代码。逐行分析：

**`nextSequence = sequence.get() + 1L`**：消费者的初始 sequence 是 -1（`INITIAL_CURSOR_VALUE`），所以第一次消费从序列号 0 开始。

**`sequenceBarrier.waitFor(nextSequence)`**：等待直到有新事件可消费。返回值 `availableSequence` 是当前可消费的最大序列号。例如消费者当前进度是 5，`waitFor(6)` 可能返回 10——表示序列号 6 到 10 的事件都可以消费。

**内层 `while (nextSequence <= availableSequence)` 循环**：这就是**批量消费**的关键。消费者不是"等一个消费一个"，而是"等到有数据后，把能消费的都消费掉"。如果 `waitFor(6)` 返回 10，消费者会连续调用 `eventHandler.onEvent()` 5 次（序列号 6~10）。

**`eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence)`**：第三个参数 `endOfBatch` 是一个重要的信号——当它为 `true` 时，表示这是当前批次的最后一个事件。消费者可以利用这个信号做批量操作的提交，例如批量写入数据库后在 `endOfBatch == true` 时执行 `commit()`。

**`sequence.set(availableSequence)`**：批量消费完成后一次性更新消费进度。使用 lazySet（release 语义），因为消费进度的可见性不需要立即精确——生产者多等一次循环检查是可接受的。

**异常处理分支**：
- `TimeoutException`：等待超时，调用 `timeoutHandler.onTimeout()`，但不退出循环
- `AlertException`：收到关闭信号，检查 running 状态，如果是 HALTED 则退出循环
- 其他异常：调用 `exceptionHandler` 处理，然后**跳过当前事件继续消费**（`nextSequence++`）

#### 7.2.3 为什么是"批量"而非逐个消费

批量消费的设计有三个性能优势：

**减少 waitFor() 的调用频率。** 每次 `waitFor()` 都需要读取 volatile 变量、可能做自旋或阻塞等待。如果每消费一个事件就调用一次 `waitFor()`，开销太大。批量模式下，一次 `waitFor()` 可以获得多个可消费事件，后续直接在 `while` 循环中消费，不再有等待开销。

**减少 `sequence.set()` 的调用频率。** 消费进度的更新（`sequence.set(availableSequence)`）是一次 volatile 写（虽然是 lazySet，但仍有内存屏障）。批量模式下，消费 N 个事件只做一次 `sequence.set()`，而非 N 次。

**为消费者提供批量优化的机会。** 通过 `endOfBatch` 参数，消费者可以实现批量优化。例如，一个写数据库的消费者可以在每个事件到来时只准备 SQL 语句，在 `endOfBatch == true` 时执行批量 `executeBatch()` —— 这比逐条 `execute()` 快得多。

LMAX 的测试数据显示，批量消费可以将吞吐量提升 **3~10 倍**，取决于批次大小和消费者的处理逻辑。

#### 7.2.4 异常处理：ExceptionHandler 的三种策略

```java
public interface ExceptionHandler<T> {
    void handleEventException(Throwable ex, long sequence, T event);
    void handleOnStartException(Throwable ex);
    void handleOnShutdownException(Throwable ex);
}
```

Disruptor 提供了三种内置的 ExceptionHandler：

```java
// 1. FatalExceptionHandler：打印日志后抛出 RuntimeException，终止消费者
public final class FatalExceptionHandler implements ExceptionHandler<Object> {
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
        logger.log(Level.SEVERE, "Exception processing: " + sequence + " " + event, ex);
        throw new RuntimeException(ex);
    }
}

// 2. IgnoreExceptionHandler：只打印日志，继续消费下一个事件
public final class IgnoreExceptionHandler implements ExceptionHandler<Object> {
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
        logger.log(Level.INFO, "Exception processing: " + sequence + " " + event, ex);
    }
}
```

默认使用 `FatalExceptionHandler`——任何未处理的异常都会终止消费者。这是一个安全的默认策略：如果消费者的 `onEvent()` 抛出异常，说明业务逻辑有 bug，继续消费可能导致数据不一致。

在生产环境中，通常需要自定义 ExceptionHandler，实现重试、告警、降级等策略。

### 7.3 WorkProcessor 与 WorkHandler：多消费者竞争消费

#### 7.3.1 WorkProcessor.run() 源码

BatchEventProcessor 实现的是**广播模式**——每个消费者都消费所有事件。有时我们需要**竞争模式**——多个消费者分摊工作，每个事件只被一个消费者处理。`WorkProcessor` 就是为此设计的。

```java
public final class WorkProcessor<T> implements EventProcessor {
    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final RingBuffer<T> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final WorkHandler<? super T> workHandler;
    private final ExceptionHandler<? super T> exceptionHandler;
    private final Sequence workSequence;  // 共享的工作序列号

    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            sequenceBarrier.clearAlert();
            notifyStart();

            boolean processedSequence = true;
            long cachedAvailableSequence = Long.MIN_VALUE;
            long nextSequence = sequence.get();

            T event = null;
            while (true) {
                try {
                    if (processedSequence) {
                        processedSequence = false;
                        do {
                            nextSequence = workSequence.get() + 1L;
                            sequence.set(nextSequence - 1L);
                        } while (!workSequence.compareAndSet(nextSequence - 1L, nextSequence));
                    }

                    if (cachedAvailableSequence >= nextSequence) {
                        event = ringBuffer.get(nextSequence);
                        workHandler.onEvent(event);
                        processedSequence = true;
                    } else {
                        cachedAvailableSequence = sequenceBarrier.waitFor(nextSequence);
                    }
                } catch (final AlertException ex) {
                    if (running.get() != RUNNING) {
                        break;
                    }
                } catch (final Throwable ex) {
                    exceptionHandler.handleEventException(ex, nextSequence, event);
                    processedSequence = true;
                }
            }

            notifyShutdown();
            running.set(IDLE);
        }
    }
}
```

#### 7.3.2 workSequence 的 CAS 竞争

核心差异在于 `workSequence`——这是多个 WorkProcessor **共享的**一个 Sequence。每个 WorkProcessor 通过 CAS 竞争来"认领"下一个待处理的序列号：

```java
do {
    nextSequence = workSequence.get() + 1L;
    sequence.set(nextSequence - 1L);  // 先更新自己的进度
} while (!workSequence.compareAndSet(nextSequence - 1L, nextSequence));
```

这个 CAS 循环确保每个序列号只被一个 WorkProcessor 认领。例如三个 WorkProcessor 同时竞争 `workSequence = 99`：
- WP-1 读到 `workSequence = 99`，尝试 `CAS(99, 100)`，成功 → WP-1 处理序列号 100
- WP-2 读到 `workSequence = 99`，尝试 `CAS(99, 100)`，失败 → 重试，读到 `workSequence = 100`，尝试 `CAS(100, 101)`，成功 → WP-2 处理序列号 101
- WP-3 类似，处理序列号 102

注意 `sequence.set(nextSequence - 1L)` 在 CAS 之前执行——这是为了让上游的 SequenceBarrier 在计算最小消费进度时，能看到这个 WorkProcessor 即将处理的位置。如果不先更新，上游可能认为这个 WorkProcessor 还停留在很早的位置，导致 RingBuffer 无法回收空间。

#### 7.3.3 与 BatchEventProcessor 的设计差异

| 维度 | BatchEventProcessor | WorkProcessor |
|------|-------------------|---------------|
| 消费模式 | 广播：每个消费者处理所有事件 | 竞争：每个事件只被一个消费者处理 |
| 序列号协调 | 每个消费者独立跟踪自己的 Sequence | 多个 Worker 共享一个 workSequence，通过 CAS 竞争 |
| 批量消费 | 支持（内层 while 循环 + endOfBatch） | 不支持（一次只处理一个事件） |
| Handler 接口 | EventHandler（含 sequence 和 endOfBatch） | WorkHandler（只有 event） |
| 性能特征 | 吞吐量高（批量 + 无竞争） | 吞吐量受 CAS 竞争影响 |

### 7.4 EventHandler / LifecycleAware / TimeoutHandler 回调接口

```java
// 核心事件处理接口
public interface EventHandler<T> {
    void onEvent(T event, long sequence, boolean endOfBatch) throws Exception;
}

// 生命周期回调
public interface LifecycleAware {
    void onStart();      // EventProcessor 线程启动时调用
    void onShutdown();   // EventProcessor 线程关闭时调用
}

// 超时回调（配合 TimeoutBlockingWaitStrategy）
public interface TimeoutHandler {
    void onTimeout(long sequence) throws Exception;
}

// 竞争消费处理接口
public interface WorkHandler<T> {
    void onEvent(T event) throws Exception;
}

// 批次开始回调
public interface BatchStartAware {
    void onBatchStart(long batchSize);
}
```

如果一个 EventHandler 同时实现了 `LifecycleAware`，BatchEventProcessor 会在 `run()` 的开头和结尾自动调用 `onStart()` 和 `onShutdown()`。这种"接口嗅探"（Interface Detection）模式避免了让所有 Handler 都必须实现生命周期回调方法，保持了接口的最小化。

---

## 八、EventFactory 与预分配：消除运行时内存分配

### 8.1 预分配的设计哲学

Disruptor 的核心性能特征之一是**事件对象的预分配**。在 RingBuffer 创建时，就一次性创建好所有事件对象，填满整个数组。运行时生产者不需要 `new` 新对象，而是获取已有对象的引用，修改其字段值，然后发布。

```java
public interface EventFactory<T> {
    T newInstance();
}
```

EventFactory 只有一个方法：创建一个事件对象的空实例。RingBuffer 在初始化时会调用这个方法 `bufferSize` 次来预填充数组。

### 8.2 预分配的实现代码

```java
// RingBufferFields 构造函数中的预分配逻辑
RingBufferFields(
        EventFactory<E> eventFactory,
        Sequencer sequencer) {
    this.sequencer = sequencer;
    this.bufferSize = sequencer.getBufferSize();

    if (bufferSize < 1) {
        throw new IllegalArgumentException("bufferSize must not be less than 1");
    }
    if (Integer.bitCount(bufferSize) != 1) {
        throw new IllegalArgumentException("bufferSize must be a power of 2");
    }

    this.indexMask = bufferSize - 1;
    this.entries = new Object[sequencer.getBufferSize() + 2 * BUFFER_PAD];
    fill(eventFactory);
}

private void fill(EventFactory<E> eventFactory) {
    for (int i = 0; i < bufferSize; i++) {
        entries[BUFFER_PAD + i] = eventFactory.newInstance();
    }
}
```

注意 `entries` 数组的大小是 `bufferSize + 2 * BUFFER_PAD`：在数组的头部和尾部各填充了 `BUFFER_PAD` 个空位，防止数组中的事件对象与数组头部/尾部的其他数据产生 False Sharing。`BUFFER_PAD` 的值是 32（32 个引用 × 4/8 字节 = 128/256 字节，远超一个缓存行的 64 字节）。

### 8.3 预分配 vs 即时分配的性能对比

| 维度 | 预分配（Disruptor） | 即时分配（ArrayBlockingQueue） |
|------|-------------------|---------------------------|
| 对象创建时机 | 初始化时一次性创建 | 每次 put() 时由生产者创建 |
| 运行时 GC 压力 | 接近零（事件对象长期存活，不被回收） | 高（大量短生命周期对象） |
| 内存局部性 | 好（对象在堆中连续分配） | 差（对象分散在堆中） |
| 数据传递方式 | 修改字段值（"两阶段提交"） | 传递引用 |
| 内存占用 | 固定（bufferSize × objectSize） | 变化（取决于队列当前大小） |

预分配的代价是需要一个"两阶段提交"的发布模式：

```java
// 传统队列的发布方式
queue.put(new OrderEvent(orderId, price));  // 每次 new 一个新对象

// Disruptor 的发布方式
long sequence = ringBuffer.next();          // 1. 获取下一个序列号
try {
    OrderEvent event = ringBuffer.get(sequence);  // 2. 获取预分配的事件对象
    event.setOrderId(orderId);                     // 3. 填充数据
    event.setPrice(price);
} finally {
    ringBuffer.publish(sequence);            // 4. 发布
}
```

这种模式稍微复杂一些，但换来了零运行时内存分配——在高吞吐场景下，避免 GC 暂停带来的延迟抖动是值得的。

### 8.4 EventTranslator：简化发布的语法糖

Disruptor 提供了 `EventTranslator` 系列接口来简化两阶段提交的语法：

```java
public interface EventTranslator<T> {
    void translateTo(T event, long sequence);
}

public interface EventTranslatorOneArg<T, A> {
    void translateTo(T event, long sequence, A arg0);
}

public interface EventTranslatorTwoArg<T, A, B> {
    void translateTo(T event, long sequence, A arg0, B arg1);
}

public interface EventTranslatorThreeArg<T, A, B, C> {
    void translateTo(T event, long sequence, A arg0, B arg1, C arg2);
}

public interface EventTranslatorVararg<T> {
    void translateTo(T event, long sequence, Object... args);
}
```

使用 EventTranslator 的发布方式：

```java
// 定义 Translator（通常作为静态常量）
private static final EventTranslatorTwoArg<OrderEvent, Long, Double> TRANSLATOR =
    (event, sequence, orderId, price) -> {
        event.setOrderId(orderId);
        event.setPrice(price);
    };

// 发布事件（一行代码）
ringBuffer.publishEvent(TRANSLATOR, orderId, price);
```

`publishEvent()` 内部的实现：

```java
@Override
public <A, B> void publishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
    final long sequence = sequencer.next();
    translateAndPublish(translator, sequence, arg0, arg1);
}

private <A, B> void translateAndPublish(
        EventTranslatorTwoArg<E, A, B> translator, long sequence, A arg0, B arg1) {
    try {
        translator.translateTo(get(sequence), sequence, arg0, arg1);
    } finally {
        sequencer.publish(sequence);
    }
}
```

EventTranslator 封装了 next() → get() → translate → publish() 的完整流程，减少了使用者出错的可能（比如忘记在 finally 中 publish）。为什么有 OneArg、TwoArg、ThreeArg 和 Vararg 四种？因为 Vararg 版本需要创建 `Object[]` 数组（GC 压力），而类型特化的版本可以避免这个开销。

---

## 九、Disruptor DSL：声明式 API 与消费者依赖图

### 9.1 DSL 设计的动机

直接使用 RingBuffer、Sequencer、SequenceBarrier、BatchEventProcessor 等底层 API 来构建一个 Disruptor 实例需要大量样板代码。Disruptor 类（DSL 入口）将所有组件的创建和依赖关系管理封装成一个流畅的声明式 API。

```java
// 不用 DSL 的底层 API 用法（伪代码，约 30 行）
RingBuffer<OrderEvent> ringBuffer = RingBuffer.createSingleProducer(
    OrderEvent::new, 1024, new YieldingWaitStrategy());
SequenceBarrier barrier1 = ringBuffer.newBarrier();
BatchEventProcessor<OrderEvent> journalProcessor = new BatchEventProcessor<>(
    ringBuffer, barrier1, new JournalHandler());
ringBuffer.addGatingSequences(journalProcessor.getSequence());
SequenceBarrier barrier2 = ringBuffer.newBarrier(journalProcessor.getSequence());
BatchEventProcessor<OrderEvent> bizProcessor = new BatchEventProcessor<>(
    ringBuffer, barrier2, new BusinessHandler());
ringBuffer.addGatingSequences(bizProcessor.getSequence());
Executor executor = Executors.newFixedThreadPool(2);
executor.execute(journalProcessor);
executor.execute(bizProcessor);

// 使用 DSL 的简洁用法（约 5 行）
Disruptor<OrderEvent> disruptor = new Disruptor<>(
    OrderEvent::new, 1024, DaemonThreadFactory.INSTANCE,
    ProducerType.SINGLE, new YieldingWaitStrategy());
disruptor.handleEventsWith(new JournalHandler())
         .then(new BusinessHandler());
disruptor.start();
```

### 9.2 Disruptor 类的核心字段

```java
public class Disruptor<T> {
    private final RingBuffer<T> ringBuffer;
    private final Executor executor;
    private final ConsumerRepository<T> consumerRepository = new ConsumerRepository<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private ExceptionHandler<? super T> exceptionHandler = new ExceptionHandlerWrapper<>();
}
```

`consumerRepository` 是 DSL 的核心数据结构——它维护了所有消费者（EventProcessor）的注册信息和依赖关系图。

### 9.3 handleEventsWith()：注册消费者

```java
@SafeVarargs
public final EventHandlerGroup<T> handleEventsWith(final EventHandler<? super T>... handlers) {
    return createEventProcessors(new Sequence[0], handlers);
}

EventHandlerGroup<T> createEventProcessors(
        final Sequence[] barrierSequences,
        final EventHandler<? super T>[] eventHandlers) {
    checkNotStarted();

    final Sequence[] processorSequences = new Sequence[eventHandlers.length];
    final SequenceBarrier barrier = ringBuffer.newBarrier(barrierSequences);

    for (int i = 0, eventHandlersLength = eventHandlers.length; i < eventHandlersLength; i++) {
        final EventHandler<? super T> eventHandler = eventHandlers[i];

        final BatchEventProcessor<T> batchEventProcessor =
            new BatchEventProcessor<>(ringBuffer, barrier, eventHandler);

        if (exceptionHandler != null) {
            batchEventProcessor.setExceptionHandler(exceptionHandler);
        }

        consumerRepository.add(batchEventProcessor, eventHandler, barrier);
        processorSequences[i] = batchEventProcessor.getSequence();
    }

    updateGatingSequencesForNextInChain(barrierSequences, processorSequences);

    return new EventHandlerGroup<>(this, consumerRepository, processorSequences);
}
```

`createEventProcessors()` 为每个 EventHandler 创建一个 `BatchEventProcessor`，并注册到 `consumerRepository` 中。`barrierSequences` 是这组消费者的依赖——如果为空数组（第一组消费者），则直接依赖生产者的 cursor。

`updateGatingSequencesForNextInChain()` 更新 gating sequences：

```java
private void updateGatingSequencesForNextInChain(
        final Sequence[] barrierSequences, final Sequence[] processorSequences) {
    if (barrierSequences.length != 0) {
        ringBuffer.removeGatingSequence(barrierSequences);
    }
    ringBuffer.addGatingSequences(processorSequences);
}
```

这里有一个精妙的设计：当新的消费者组被添加到链条末端时，前一组消费者的 Sequence 从 gating sequences 中移除，只保留最末端的消费者。因为生产者只需要确保**最慢的消费者**不被追上，而链式依赖保证了末端消费者的进度一定 <= 前置消费者的进度。

### 9.4 then()：建立消费者依赖链

```java
public class EventHandlerGroup<T> {
    private final Disruptor<T> disruptor;
    private final ConsumerRepository<T> consumerRepository;
    private final Sequence[] sequences;  // 当前组的消费者 Sequences

    @SafeVarargs
    public final EventHandlerGroup<T> then(final EventHandler<? super T>... handlers) {
        return handleEventsWith(handlers);
    }

    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(
            final EventHandler<? super T>... handlers) {
        return disruptor.createEventProcessors(sequences, handlers);
    }
}
```

`then()` 的实现非常简单——它把当前组的 `sequences` 作为 `barrierSequences` 传给 `createEventProcessors()`，这样新组的 SequenceBarrier 就会依赖当前组的进度。

### 9.5 构建复杂的消费者拓扑

DSL 支持多种消费者拓扑结构：

```java
// 1. 串行链：A → B → C
disruptor.handleEventsWith(A).then(B).then(C);

// 2. 并行（广播）：A 和 B 同时消费所有事件
disruptor.handleEventsWith(A, B);

// 3. 菱形依赖：A 和 B 并行，C 在 A 和 B 都完成后执行
disruptor.handleEventsWith(A, B).then(C);

// 4. 复杂图：A → C, B → C, A → D（A 的输出同时给 C 和 D）
EventHandlerGroup<E> groupA = disruptor.handleEventsWith(A);
EventHandlerGroup<E> groupB = disruptor.handleEventsWith(B);
groupA.and(groupB).then(C);
groupA.then(D);

// 5. 竞争消费：多个 Worker 分摊工作
disruptor.handleEventsWithWorkerPool(W1, W2, W3);

// 6. 混合：先广播给 A 和 Worker Pool，然后汇聚到 C
disruptor.handleEventsWith(A)
         .and(disruptor.handleEventsWithWorkerPool(W1, W2))
         .then(C);
```

### 9.6 start()：启动所有消费者

```java
public RingBuffer<T> start() {
    checkOnlyStartedOnce();

    consumerRepository.getConsumerInfos().forEach(consumerInfo -> {
        consumerInfo.start(executor);
    });

    return ringBuffer;
}
```

`start()` 遍历所有注册的消费者（ConsumerInfo），通过 executor 启动每个 EventProcessor 的线程。

### 9.7 shutdown()：优雅关闭

```java
public void shutdown(long timeout, TimeUnit timeUnit) throws TimeoutException {
    final long timeoutAt = System.currentTimeMillis() + timeUnit.toMillis(timeout);
    while (hasBacklog()) {
        if (timeout >= 0 && System.currentTimeMillis() > timeoutAt) {
            throw TimeoutException.INSTANCE;
        }
        // 等待所有消费者处理完积压的事件
    }
    halt();
}

public void halt() {
    for (final ConsumerInfo consumerInfo : consumerRepository.getConsumerInfos()) {
        consumerInfo.halt();  // 对每个 EventProcessor 调用 halt()
    }
}

private boolean hasBacklog() {
    final long cursor = ringBuffer.getCursor();
    for (final Sequence consumer : consumerRepository.getLastSequenceInChain(false)) {
        if (cursor > consumer.get()) {
            return true;  // 还有消费者没消费完
        }
    }
    return false;
}
```

`shutdown()` 的优雅关闭流程：
1. 等待所有消费者处理完 RingBuffer 中的积压事件（通过 `hasBacklog()` 检查）
2. 等待超时后抛出 `TimeoutException`
3. 调用 `halt()` 向每个 EventProcessor 发送 alert 信号
4. EventProcessor 在 `waitFor()` 中捕获 `AlertException`，退出主循环

---

## 十、缓存行填充与 Mechanical Sympathy

### 10.1 Disruptor 中的缓存行填充全景

缓存行填充是 Disruptor 最核心的性能优化技术之一。在整个代码库中，至少有以下位置使用了缓存行填充：

1. **Sequence 类**：通过继承链在 `value` 字段前后各填充 7 个 long
2. **RingBuffer（RingBufferFields）**：`entries` 数组头尾各填充 `BUFFER_PAD` 个位置
3. **SingleProducerSequencer（SingleProducerSequencerFields）**：`nextValue` 和 `cachedValue` 前后填充
4. **MultiProducerSequencer**：通过继承链填充

### 10.2 Sequence 的填充细节

前面第三章已经展示了 Sequence 的继承链。这里深入分析为什么要用继承而非直接在一个类中声明所有填充字段。

```java
// 假设不用继承，直接在一个类中声明：
class Sequence {
    long p1, p2, p3, p4, p5, p6, p7;  // 前填充
    volatile long value;
    long p9, p10, p11, p12, p13, p14, p15;  // 后填充
}
```

这种写法的问题是：JVM（特别是 HotSpot）可能会**重排字段顺序**。JVM 按以下优先级排列字段：long/double → int/float → short/char → byte/boolean → reference。同类型的字段可能被任意重排。因此 `value` 可能被排到 `p7` 前面，填充失效。

使用继承链可以保证字段顺序：

```java
class LhsPadding { long p1, p2, p3, p4, p5, p6, p7; }   // 子类字段不会插入父类布局
class Value extends LhsPadding { volatile long value; }
class RhsPadding extends Value { long p9, p10, p11, p12, p13, p14, p15; }
class Sequence extends RhsPadding { ... }
```

JVM 保证父类的字段在子类字段之前（即使同类型），所以 `p1~p7` 一定在 `value` 之前，`p9~p15` 一定在 `value` 之后。每个 long 占 8 字节，7 个 long = 56 字节。加上对象头（12~16 字节）和 `value`（8 字节），`value` 距离对象起始位置约 68~72 字节。后面再填充 56 字节，确保 `value` 所在的缓存行（64 字节）不会与任何其他数据共享。

### 10.3 RingBuffer 的数组填充

```java
abstract class RingBufferPad {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RingBufferFields<E> extends RingBufferPad {
    private static final int BUFFER_PAD;
    // ...
    private final Object[] entries;

    static {
        final int scale = UNSAFE.arrayIndexScale(Object[].class);  // 引用大小：4 或 8
        BUFFER_PAD = 128 / scale;  // 128 / 4 = 32 或 128 / 8 = 16
    }

    RingBufferFields(EventFactory<E> eventFactory, Sequencer sequencer) {
        // ...
        this.entries = new Object[sequencer.getBufferSize() + 2 * BUFFER_PAD];
        fill(eventFactory);
    }

    @SuppressWarnings("unchecked")
    protected final E elementAt(long sequence) {
        return (E) UNSAFE.getObject(entries, REF_ARRAY_BASE + ((sequence & indexMask) << REF_ELEMENT_SHIFT));
    }
}
```

`entries` 数组的前 `BUFFER_PAD` 个位置和后 `BUFFER_PAD` 个位置都是空的——真正的事件对象存储在 `entries[BUFFER_PAD]` 到 `entries[BUFFER_PAD + bufferSize - 1]`。这确保了数组头部和尾部的元素不会与数组外的数据共享缓存行。

`BUFFER_PAD` 的计算基于引用大小：在压缩指针（CompressedOops，默认启用）下引用大小是 4 字节，`BUFFER_PAD = 128 / 4 = 32`，总填充 = 32 × 4 = 128 字节 = 2 个缓存行。在 64 位无压缩指针时引用大小是 8 字节，`BUFFER_PAD = 128 / 8 = 16`，总填充 = 16 × 8 = 128 字节。

### 10.4 SingleProducerSequencer 的填充

```java
abstract class SingleProducerSequencerPad extends AbstractSequencer {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad {
    long nextValue = Sequence.INITIAL_VALUE;
    long cachedValue = Sequence.INITIAL_VALUE;
}

public final class SingleProducerSequencer extends SingleProducerSequencerFields {
    protected long p1, p2, p3, p4, p5, p6, p7;
    // ...
}
```

`nextValue`（下一个要分配的序列号）和 `cachedValue`（缓存的最小消费者进度）在填充的保护下不会与其他字段共享缓存行。这两个字段都是生产者线程独占的（单生产者模式），不需要 volatile，但仍然需要填充——因为它们可能与 `AbstractSequencer` 中的其他字段（如 `gatingSequences`）在同一缓存行内，而 `gatingSequences` 会被消费者线程读取。

### 10.5 与 Netty JCTools MpscArrayQueue 的填充对比

Disruptor 的 Sequence 使用 7+7 个 long（112 字节）填充，JCTools 的 MpscArrayQueue 使用 7 层继承链实现 128 字节填充。两者的设计思想一致：

| 维度 | Disruptor Sequence | JCTools MpscArrayQueue |
|------|-------------------|------------------------|
| 填充策略 | 继承链 + 7 个 long | 继承链 + 15 个 long |
| 填充量 | 前 56B + 后 56B = 112B | 每层前后各 120B |
| 核心变量 | volatile long value | producerIndex / consumerIndex |
| 防止 JVM 重排 | 是（继承链保证） | 是（继承链保证） |
| Java 8+ 替代方案 | @Contended 可用但未采用 | 同上 |

Disruptor 选择不使用 `@Contended` 注解的原因：(1) 需要 `-XX:-RestrictContended` JVM 参数；(2) Disruptor 追求对 JVM 版本的最大兼容性；(3) 继承链方案在所有 JVM 上都可靠工作。

---

## 十一、性能基准与实际测量

### 11.1 Disruptor 官方基准测试结构

Disruptor 源码中包含一套完整的基准测试（`src/perftest/java/`），覆盖了各种生产者-消费者配置：

```
OneToOne: 1 个生产者 → 1 个消费者
OneToThree: 1 个生产者 → 3 个消费者（广播）
OneToThreeDiamond: 1 个生产者 → 2 个消费者 → 1 个汇聚消费者
ThreeToOne: 3 个生产者 → 1 个消费者
ThreeToThree: 3 个生产者 → 3 个消费者
```

每个配置都与 JDK `ArrayBlockingQueue` 进行对比。

### 11.2 Disruptor vs ArrayBlockingQueue 性能对比

Martin Thompson 的经典基准测试数据（2011 年论文，Intel Core i7-2600 3.4GHz，Java 6 Update 25）：

| 场景 | ABQ (ops/s) | Disruptor (ops/s) | 倍数 |
|------|------------|-------------------|------|
| 1P-1C Unicast | 5,339,256 | 25,998,336 | 4.9x |
| 1P-3C Pipeline | 2,128,918 | 16,806,405 | 7.9x |
| 1P-3C Diamond | 2,048,030 | 14,403,285 | 7.0x |
| 3P-1C MultiCast | 5,539,531 | 13,561,129 | 2.4x |

延迟数据（1P-1C，纳秒）：

| 百分位 | ABQ | Disruptor |
|--------|-----|----------|
| Mean | 32,757 | 52 |
| 50th | 32,767 | 50 |
| 90th | 32,767 | 50 |
| 99th | 36,863 | 54 |
| 99.99th | 3,080,191 | 168 |

ArrayBlockingQueue 的延迟在微秒级（32μs），而 Disruptor 在纳秒级（52ns）——差距约 **630 倍**。这个巨大差距来源于：

1. ABQ 使用 `ReentrantLock`（mutex），即使无竞争也有 CAS + 可能的 park/unpark 开销
2. ABQ 每次 put/take 都要获取和释放锁
3. ABQ 的 `putIndex`、`takeIndex`、`count` 字段没有缓存行填充，存在 False Sharing
4. ABQ 没有预分配事件对象，每次 put 需要传入新创建的对象

### 11.3 现代硬件上的预期性能

在现代硬件（AMD EPYC 7763 / Intel Xeon Gold 6354, Java 17+, 2023 年）上，Disruptor 的单生产者-单消费者吞吐量可达 **150M~300M ops/s**（取决于事件大小和 WaitStrategy）。使用 BusySpinWaitStrategy + 绑核的延迟可低至 **20~30ns**。

### 11.4 影响性能的关键配置

| 配置项 | 低延迟推荐 | 高吞吐推荐 | 低 CPU 推荐 |
|--------|-----------|-----------|------------|
| ProducerType | SINGLE | SINGLE | SINGLE |
| WaitStrategy | BusySpin | Yielding | Sleeping/Blocking |
| bufferSize | 1024~4096 | 65536~1048576 | 1024~8192 |
| 绑核 | 是 | 可选 | 否 |
| EventHandler 实现 | 内联、避免分支 | 批量提交（利用 endOfBatch） | 无特殊要求 |

`bufferSize` 的选择需要考虑：太小会导致生产者频繁被反压（等待消费者），太大会浪费内存并可能导致更多 L3 cache miss。经验法则是：bufferSize ≈ 预期的突发事件数量 × 2。

---

## 十二、高级使用模式

### 12.1 多阶段处理流水线（Pipeline）

在金融交易系统中，一笔订单可能需要经过以下处理阶段：

```
接收订单 → 风控检查 → 撮合引擎 → 持久化日志 → 发送回报
```

使用 Disruptor 实现这个流水线：

```java
Disruptor<OrderEvent> disruptor = new Disruptor<>(
    OrderEvent::new, 4096, DaemonThreadFactory.INSTANCE,
    ProducerType.SINGLE, new YieldingWaitStrategy());

disruptor
    .handleEventsWith(new RiskCheckHandler())      // 阶段 1：风控
    .then(new MatchingEngineHandler())              // 阶段 2：撮合
    .then(new JournalHandler(), new ReportHandler());  // 阶段 3：日志和回报并行

disruptor.start();
```

每个阶段运行在独立的线程中，阶段之间通过 RingBuffer 自动协调。与使用多个 BlockingQueue 串联相比，Disruptor 的流水线有两个关键优势：

1. **零拷贝**：所有阶段操作的是同一个事件对象（RingBuffer 中的预分配对象），不需要在队列之间传递数据的副本
2. **批量效应传递**：如果第一阶段批量处理了 100 个事件，第二阶段的 `waitFor()` 会一次性返回所有 100 个序列号，也能批量处理

### 12.2 多生产者共享 RingBuffer

当多个线程需要向同一个 RingBuffer 发布事件时，必须使用 `ProducerType.MULTI`：

```java
Disruptor<OrderEvent> disruptor = new Disruptor<>(
    OrderEvent::new, 4096, DaemonThreadFactory.INSTANCE,
    ProducerType.MULTI,  // 关键：指定多生产者
    new BlockingWaitStrategy());
```

多个生产者线程可以安全地并发调用 `ringBuffer.publishEvent()`——内部通过 MultiProducerSequencer 的 CAS 保证序列号的原子分配。

**注意事项**：即使只有两个生产者线程，MultiProducerSequencer 的开销也显著高于 SingleProducerSequencer。如果可能，应该考虑以下替代方案：

1. 使用多个单生产者 Disruptor，每个生产者有自己的 RingBuffer
2. 将多个生产者的事件先汇聚到一个单线程中（通过 MpscQueue 或类似机制），再由该线程发布到 Disruptor

### 12.3 清理事件数据防止内存泄漏

由于事件对象是预分配的，它们会在 RingBuffer 中长期存活。如果事件包含大对象的引用（如 `byte[]`、`String`），即使事件已被所有消费者处理完毕，这些引用仍然存在，阻止 GC 回收。

解决方案是在最后一个消费者处理完事件后清理引用：

```java
public class ClearingHandler implements EventHandler<OrderEvent> {
    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        event.clear();  // 将大对象引用设为 null
    }
}

// 在依赖链末端添加清理 Handler
disruptor.handleEventsWith(businessHandler).then(clearingHandler);
```

### 12.4 利用 endOfBatch 实现批量优化

BatchEventProcessor 传递给 EventHandler 的 `endOfBatch` 参数是实现高效批量操作的关键：

```java
public class BatchingJdbcHandler implements EventHandler<TradeEvent> {
    private final PreparedStatement stmt;
    
    @Override
    public void onEvent(TradeEvent event, long sequence, boolean endOfBatch) throws Exception {
        stmt.setLong(1, event.getTradeId());
        stmt.setDouble(2, event.getPrice());
        stmt.addBatch();  // 每个事件只是添加到批次中
        
        if (endOfBatch) {
            stmt.executeBatch();  // 批次结束时统一提交
            connection.commit();
        }
    }
}
```

这种模式将 N 次数据库交互压缩为 1 次批量执行，在高吞吐场景下性能提升显著。同样的模式适用于：批量写入文件、批量发送网络请求、批量更新缓存等。

### 12.5 温热启动与消除 JIT 编译抖动

在极低延迟的场景中（如金融高频交易），JIT 编译器在运行初期的优化编译会导致延迟抖动。Disruptor 的使用者通常会做"温热启动"：

```java
// 启动后先发送一批虚拟事件，触发 JIT 编译
for (int i = 0; i < 1_000_000; i++) {
    ringBuffer.publishEvent(WARM_UP_TRANSLATOR);
}
// 等待所有虚拟事件被消费
Thread.sleep(1000);
// 重置消费者统计
// 现在开始接收真实事件
```

这确保了 EventHandler.onEvent()、Sequencer.next()、WaitStrategy.waitFor() 等热路径方法都已被 JIT 编译为本地代码，避免在处理真实事件时触发编译暂停。

---

## 十三、Disruptor 在知名项目中的应用

### 13.1 Log4j2 AsyncLogger

Apache Log4j2 的 AsyncLogger 是 Disruptor 最知名的应用案例之一。当启用 AsyncLogger 时，日志事件被发布到一个 Disruptor RingBuffer，由独立的后台线程异步消费并写入 Appender。

```
应用线程                                     后台日志线程
   |                                              |
   +-- logger.info("...")                         |
   |     +-- ringBuffer.publishEvent()            |
   |          +-- translate: 填充 RingBufferLogEvent |
   |          +-- sequencer.publish()             |
   |               +-- signalAllWhenBlocking()  --+--> WaitStrategy 唤醒
   |                                              |
   |                                       waitFor() 返回
   |                                       onEvent(RingBufferLogEvent)
   |                                         +-- logEvent.prepareForDeferredProcessing()
   |                                         +-- appender.append(logEvent)
   |                                         +-- logEvent.clear()
```

Log4j2 选择 Disruptor 的原因：

1. **零内存分配**：`RingBufferLogEvent` 是预分配的，日志字符串通过 `StringBuilder` 复用来格式化，整个日志路径几乎不产生 GC 压力
2. **吞吐量**：异步模式下吞吐量比同步模式高 6~68 倍（Log4j2 官方基准数据）
3. **延迟**：99.99% 百分位延迟从同步模式的微秒级降低到纳秒级

Log4j2 默认使用 `TimeoutBlockingWaitStrategy`（超时 10ms），在超时时执行 RingBuffer 监控统计。可以通过系统属性配置为 `SleepingWaitStrategy` 或其他策略。

### 13.2 LMAX Exchange 交易引擎

LMAX Exchange 是 Disruptor 的诞生地。其交易引擎使用 Disruptor 实现了每秒 600 万笔交易、99.99% 延迟低于 1 毫秒的性能指标。

交易引擎的架构：

```
网络层（Netty）→ [Journal Disruptor] → [Replication Disruptor] → [Business Logic Disruptor]
                       |                       |
                  持久化到磁盘              复制到备机
```

三个 Disruptor 串联：Journal（持久化）、Replication（复制到灾备节点）、Business Logic（撮合引擎）。Business Logic 在一个单线程中运行所有撮合逻辑——没有锁、没有同步、没有共享可变状态，所有状态都在这一个线程中维护。

### 13.3 与 Netty 的技术血缘关系

Disruptor 和 Netty 在底层技术上有显著的血缘关系：

| 技术 | Disruptor 的应用 | Netty 的应用 |
|------|-----------------|-------------|
| 缓存行填充 | Sequence 继承链 7+7 个 long | JCTools MpscArrayQueue 7 层继承链 |
| CAS 无锁编程 | MultiProducerSequencer.next() | MpscQueue.offer() |
| 批量消费 | BatchEventProcessor 内层 while | EventLoop.runAllTasks() 批量执行 |
| 预分配 | RingBuffer 预填充事件对象 | PooledByteBufAllocator 预分配内存块 |
| lazySet 优化 | Sequence.set() 使用 putOrderedLong | MpscAtomicIntegerArrayQueue.lazySet |
| 单消费者优化 | SingleProducerSequencer 无原子操作 | EventLoop 单线程消费 taskQueue |
| 序列号/索引缓存 | cachedValue 减少 volatile 读 | producerLimit 减少跨核读频率 |

两者都深受 Martin Thompson "Mechanical Sympathy" 理念的影响：理解硬件的工作方式（CPU 缓存层次、缓存一致性协议、内存屏障），并据此设计软件，而非依赖编译器和运行时的自动优化。

---

## 十四、Disruptor 的设计模式总结

### 14.1 核心设计模式

| 设计模式 | 应用位置 | 说明 |
|---------|---------|------|
| 享元模式（Flyweight） | AlertException.INSTANCE / TimeoutException.INSTANCE | 单例异常对象 + fillInStackTrace() 覆盖，避免重复创建 |
| 策略模式（Strategy） | WaitStrategy 的 7 种实现 | 消费者等待行为与核心逻辑解耦，允许运行时选择最优策略 |
| 模板方法模式（Template Method） | BatchEventProcessor.processEvents() | 定义消费主循环骨架，EventHandler.onEvent() 由用户实现 |
| 组合模式（Composite） | FixedSequenceGroup | 让单个 Sequence 和一组 Sequence 具有统一接口 |
| 建造者模式（Builder / DSL） | Disruptor 类的 handleEventsWith().then() 链式 API | 声明式构建消费者依赖图 |
| 观察者模式（Observer） | EventHandler / WorkHandler 回调 | 事件到来时通知已注册的处理器 |
| 工厂模式（Factory） | EventFactory / ThreadFactory | 对象创建逻辑与使用逻辑分离 |
| 序列号模式（Sequence Pattern） | Sequence + Sequencer | 单调递增的 64-bit 序列号替代 boolean 标志，消除 ABA 问题 |
| 两阶段提交（Two-Phase Commit） | next() + publish() | 先获取序列号（预留位置），再发布（对消费者可见） |

### 14.2 核心高性能并发技术

| 技术 | 应用位置 | 核心收益 |
|------|---------|----------|
| 缓存行填充（Cache Line Padding） | Sequence、RingBuffer entries、SingleProducerSequencer | 消除 False Sharing，将多核间的缓存一致性流量降到最低 |
| CAS 无锁编程 | MultiProducerSequencer.next() | 避免 mutex 的上下文切换开销 |
| lazySet / putOrderedLong | Sequence.set()、SingleProducerSequencer.publish() | Store-Store 屏障替代 StoreLoad 屏障，减少 CPU 流水线停顿 |
| 位运算取模 | sequence & indexMask | 替代 `%` 除法运算（约 25 个时钟周期 vs 1 个时钟周期） |
| 预分配（Pre-allocation） | RingBuffer.fill() | 消除运行时内存分配和 GC 压力 |
| 批量消费（Batching） | BatchEventProcessor.processEvents() 内层 while | 减少 waitFor() 和 sequence.set() 的调用频率 |
| 序列号缓存（Cached Gating） | SingleProducerSequencer.cachedValue | 将 volatile 读频率从每次 next() 降低到约每 bufferSize 次 |
| 单写者原则（Single Writer Principle） | SingleProducerSequencer 的 nextValue | 生产者端零原子操作 |
| 内存屏障显式控制 | UNSAFE.putOrderedLong / getIntVolatile | 精确控制可见性语义，避免过强的内存屏障 |
| availableBuffer 标记数组 | MultiProducerSequencer | 用 int 数组标记发布状态，支持乱序发布 + 连续性扫描 |
| 线程亲和性（Thread Affinity） | BusySpinWaitStrategy 配合绑核 | 消除线程迁移导致的缓存失效 |

---

## 十五、源码版本演进与历史

### 15.1 主要版本节点

| 版本 | 时间 | 关键变化 |
|------|------|----------|
| 1.0 | 2011 | 初始开源版本，来自 LMAX Exchange 内部 |
| 2.0 | 2012 | 引入 DSL API（Disruptor 类），简化使用方式 |
| 3.0 | 2013 | 重构 Sequencer 接口，分离 Single/Multi 实现 |
| 3.2 | 2014 | 添加 LiteBlockingWaitStrategy |
| 3.3 | 2015 | 改进 BatchEventProcessor 的异常处理 |
| 3.4 | 2018 | 添加 BatchStartAware 接口 |
| 4.0 | 2023 | 模块化、Java 11+ 基线、性能改进 |

### 15.2 从 Disruptor 到 JCTools 的技术传承

Disruptor 的成功催生了 JCTools 库——一个专门的 Java 并发工具箱。JCTools 的 MpscArrayQueue 借鉴了 Disruptor 的核心思想（缓存行填充、CAS 序列号、消费者端无锁），但面向不同的使用场景：

| 维度 | Disruptor RingBuffer | JCTools MpscArrayQueue |
|------|---------------------|------------------------|
| 设计目标 | 完整的事件处理框架 | 轻量级并发队列 |
| 事件模型 | 预分配 + 两阶段提交 | 标准的 offer/poll |
| 消费者模型 | 内置 EventProcessor + WaitStrategy | 用户自行实现消费循环 |
| 消费者依赖 | 支持复杂依赖图 | 不支持 |
| 使用者 | LMAX、Log4j2、Storm | Netty、Project Reactor、Akka |

Netty 选择 JCTools 而非 Disruptor 的原因是：Netty 的 EventLoop 已经有自己的线程模型和任务消费循环，只需要一个高性能的 MPSC 队列来做 taskQueue，不需要 Disruptor 的 WaitStrategy、EventProcessor、DSL 等上层抽象。JCTools 作为纯队列库，集成更轻量。

---

## 十六、常见问题与最佳实践

### 16.1 如何选择 bufferSize？

bufferSize 必须是 2 的幂次方，常见选择：

- **1024~4096**：适合低延迟场景（小 buffer 意味着更好的缓存局部性）
- **65536~262144**：适合高吞吐场景（大 buffer 容忍消费者的瞬时停顿）
- **1048576+**：仅在消费者可能长时间暂停时使用（如需要做 Full GC 的消费者）

公式参考：`bufferSize >= maxExpectedBurstSize × 2`，其中 `maxExpectedBurstSize` 是预期的最大突发事件数量。

### 16.2 事件对象的设计原则

1. **所有字段都应该是可变的**：事件对象是复用的，需要在每次发布时重新赋值
2. **避免持有大对象的引用**：或者在最后一个消费者中调用 `event.clear()`
3. **优先使用基本类型**：`long` 而非 `Long`，避免自动装箱
4. **保持事件对象小巧**：大对象会增加缓存 miss 的概率

```java
// 好的设计
public class OrderEvent {
    private long orderId;
    private double price;
    private int quantity;
    private byte side;  // 0=buy, 1=sell
    
    public void clear() {
        orderId = 0;
        price = 0;
        quantity = 0;
        side = 0;
    }
}

// 不好的设计
public class BadOrderEvent {
    private Long orderId;           // 装箱类型
    private BigDecimal price;       // 大对象
    private List<String> tags;      // 引用类型集合
    private byte[] payload;         // 大数组
}
```

### 16.3 异常处理策略的选择

| 策略 | 行为 | 适用场景 |
|------|------|----------|
| FatalExceptionHandler（默认） | 打印日志 + 抛异常 + 终止消费者 | 开发和测试阶段 |
| IgnoreExceptionHandler | 只打印日志，继续消费 | 允许丢弃个别事件的场景 |
| 自定义 Handler | 重试、告警、降级、记录到死信队列 | 生产环境 |

生产环境建议始终使用自定义 ExceptionHandler，至少包含：

```java
public class ProductionExceptionHandler implements ExceptionHandler<OrderEvent> {
    @Override
    public void handleEventException(Throwable ex, long sequence, OrderEvent event) {
        // 1. 记录详细日志
        logger.error("处理事件失败 seq={} event={}", sequence, event, ex);
        // 2. 发送告警
        alertService.sendAlert("Disruptor 事件处理异常", ex);
        // 3. 记录到死信队列（可选）
        deadLetterQueue.enqueue(event, ex);
        // 注意：不要抛出异常，否则消费者会停止
    }
}
```

### 16.4 监控与可观测性

在生产环境中，需要监控以下指标：

```java
// 1. RingBuffer 剩余容量（反压指标）
long remainingCapacity = ringBuffer.remainingCapacity();
// remainingCapacity / bufferSize < 0.1 时应告警

// 2. 消费者落后程度
long producerPos = ringBuffer.getCursor();
long consumerPos = consumer.getSequence().get();
long lag = producerPos - consumerPos;
// lag 持续增长说明消费者跟不上

// 3. 批次大小（通过 BatchStartAware）
public class MonitoringHandler implements EventHandler<Event>, BatchStartAware {
    private final Histogram batchSizeHist = new Histogram(1, 100000, 3);
    
    @Override
    public void onBatchStart(long batchSize) {
        batchSizeHist.recordValue(batchSize);
    }
}
```

### 16.5 常见陷阱

**陷阱 1：忘记 publish()**

```java
long seq = ringBuffer.next();
Event event = ringBuffer.get(seq);
event.setValue(42);
// 忘记调用 ringBuffer.publish(seq) → 消费者永远等待！
```

必须在 finally 块中调用 publish()，或使用 EventTranslator 避免手动管理。

**陷阱 2：在 EventHandler 中持有事件引用**

```java
public class BadHandler implements EventHandler<OrderEvent> {
    private OrderEvent lastEvent;  // 危险！
    
    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        lastEvent = event;  // 这个引用指向 RingBuffer 中的预分配对象
        // 当 RingBuffer 回绕后，lastEvent 的内容会被覆盖！
    }
}
```

如果需要保留事件数据，必须复制（深拷贝）而非保留引用。

**陷阱 3：ProducerType 选错**

使用 `ProducerType.SINGLE` 但有多个线程调用 `publishEvent()` → 数据竞争，事件丢失或覆盖。使用 `ProducerType.MULTI` 但实际只有一个生产者 → 不必要的性能开销。

**陷阱 4：bufferSize 不是 2 的幂**

Disruptor 会在构造函数中检查并抛出 `IllegalArgumentException`。但如果通过底层 API 绕过检查，位运算取模（`sequence & indexMask`）会产生错误的映射。

---

## 十七、Disruptor 源码的工程美学

### 17.1 极致的 API 最小化

Disruptor 的核心 API 极其精简：

- **生产者侧**：`next()` + `get()` + `publish()`，或一个 `publishEvent()` 调用
- **消费者侧**：实现一个 `onEvent()` 方法
- **配置侧**：选择 ProducerType、WaitStrategy、bufferSize

整个框架的源码量不到 5000 行（不含测试和性能测试），核心类不超过 15 个。这种克制来自 LMAX 团队的设计哲学：**做一件事，做到极致**。Disruptor 不试图成为通用的消息队列——它只解决"线程间高效传递事件"这一个问题。

### 17.2 Mechanical Sympathy 的实践教科书

Disruptor 是 Mechanical Sympathy 理念的最佳实践案例：

1. **理解 CPU 缓存**：缓存行填充消除 False Sharing
2. **理解缓存一致性协议**：lazySet 减少 MESI 协议流量
3. **理解分支预测**：SingleProducerSequencer.next() 中的 `if (wrapPoint > cachedGatingSequence)` 大多数时候为 false（cache 有效），分支预测器可以高效预测
4. **理解内存模型**：精确区分 volatile 读/写、lazySet、CAS 的语义差异，选择最弱但足够的语义
5. **理解操作系统调度**：BusySpinWaitStrategy 配合绑核消除上下文切换

### 17.3 与传统并发编程的思维差异

传统并发编程的思路是"保护共享资源"：给数据加锁、用 synchronized、用 ConcurrentHashMap。Disruptor 的思路是**消除共享**：

- 每个消费者有自己的 Sequence，不与其他消费者共享
- 每个 Sequence 有缓存行填充，不与其他数据共享缓存行
- SingleProducerSequencer 的 nextValue/cachedValue 只有一个线程写入，不需要同步
- 事件处理在 EventProcessor 的线程中顺序执行，不存在并发访问

这种"单写者原则"（Single Writer Principle）是 Disruptor 性能的根基：如果一个数据只有一个线程写入，就不需要任何同步机制——volatile 读对于读者就够了，而写者甚至可以用 lazySet（比 volatile 写更便宜）。

### 17.4 性能优化的层次

Disruptor 的性能优化可以分为四个层次，每个层次的收益递减但累积效果巨大：

1. **架构层**（10x~100x 收益）：无锁设计替代锁、预分配替代即时分配、批量消费替代逐个消费
2. **算法层**（2x~5x 收益）：位运算取模、序列号缓存减少 volatile 读
3. **内存层**（1.5x~3x 收益）：缓存行填充消除 False Sharing、数组填充
4. **指令层**（1.1x~1.5x 收益）：lazySet 替代 volatile 写、PAUSE 指令提示

每一层的优化都建立在对底层硬件的深刻理解之上。单看某一个优化可能"只"带来 20% 的提升，但四个层次的优化组合在一起，总收益是乘法而非加法——这就是 Disruptor 能比 ArrayBlockingQueue 快 **数百倍** 的根本原因。

---

## 十八、完整代码示例：从零构建 Disruptor 应用

### 18.1 最简示例：单生产者-单消费者

```java
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

// 1. 定义事件对象
public class LongEvent {
    private long value;

    public void set(long value) {
        this.value = value;
    }

    public long get() {
        return value;
    }

    public void clear() {
        this.value = 0L;
    }
}

// 2. 定义事件工厂
public class LongEventFactory implements EventFactory<LongEvent> {
    @Override
    public LongEvent newInstance() {
        return new LongEvent();
    }
}

// 3. 定义事件处理器
public class LongEventHandler implements EventHandler<LongEvent> {
    @Override
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) {
        System.out.println("消费事件: sequence=" + sequence + ", value=" + event.get());
    }
}

// 4. 主程序
public class SimpleDisruptorExample {
    public static void main(String[] args) throws InterruptedException {
        int bufferSize = 1024;  // 必须是 2 的幂

        // 创建 Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<>(
            LongEvent::new,                    // EventFactory
            bufferSize,                         // RingBuffer 大小
            DaemonThreadFactory.INSTANCE,       // ThreadFactory
            ProducerType.SINGLE,                // 单生产者
            new YieldingWaitStrategy()          // 等待策略
        );

        // 注册消费者
        disruptor.handleEventsWith(new LongEventHandler());

        // 启动 Disruptor
        RingBuffer<LongEvent> ringBuffer = disruptor.start();

        // 发布事件
        for (long i = 0; i < 1_000_000; i++) {
            long sequence = ringBuffer.next();
            try {
                LongEvent event = ringBuffer.get(sequence);
                event.set(i);
            } finally {
                ringBuffer.publish(sequence);
            }
        }

        // 等待消费完成
        Thread.sleep(1000);
        disruptor.shutdown();
    }
}
```

### 18.2 菱形依赖示例

```java
public class DiamondPatternExample {
    // 事件
    static class TradeEvent {
        long tradeId;
        double price;
        boolean riskChecked;
        boolean journaled;

        void clear() {
            tradeId = 0; price = 0;
            riskChecked = false; journaled = false;
        }
    }

    // 风控检查
    static class RiskCheckHandler implements EventHandler<TradeEvent> {
        @Override
        public void onEvent(TradeEvent event, long sequence, boolean endOfBatch) {
            event.riskChecked = true;
            // 模拟风控检查
        }
    }

    // 持久化日志
    static class JournalHandler implements EventHandler<TradeEvent> {
        @Override
        public void onEvent(TradeEvent event, long sequence, boolean endOfBatch) {
            event.journaled = true;
            // 模拟日志写入
        }
    }

    // 业务处理（依赖 RiskCheck 和 Journal 都完成）
    static class BusinessHandler implements EventHandler<TradeEvent> {
        @Override
        public void onEvent(TradeEvent event, long sequence, boolean endOfBatch) {
            assert event.riskChecked && event.journaled;
            // 执行撮合或其他业务逻辑
        }
    }

    // 清理（依赖 Business 完成）
    static class ClearingHandler implements EventHandler<TradeEvent> {
        @Override
        public void onEvent(TradeEvent event, long sequence, boolean endOfBatch) {
            event.clear();
        }
    }

    public static void main(String[] args) {
        Disruptor<TradeEvent> disruptor = new Disruptor<>(
            TradeEvent::new, 4096, DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE, new YieldingWaitStrategy());

        // 菱形依赖拓扑：
        //          ┌→ RiskCheck ─┐
        // Producer ─┤              ├→ Business → Clearing
        //          └→ Journal ────┘
        disruptor
            .handleEventsWith(new RiskCheckHandler(), new JournalHandler())
            .then(new BusinessHandler())
            .then(new ClearingHandler());

        disruptor.start();
        // ... 发布事件 ...
    }
}
```

### 18.3 使用 EventTranslator 的推荐发布方式

```java
public class TranslatorExample {
    // 定义静态 Translator（避免 lambda 每次创建匿名类实例）
    private static final EventTranslatorTwoArg<TradeEvent, Long, Double> TRANSLATOR =
        (event, sequence, tradeId, price) -> {
            event.tradeId = tradeId;
            event.price = price;
        };

    public void publish(RingBuffer<TradeEvent> ringBuffer, long tradeId, double price) {
        // 一行代码完成发布，内部自动处理 next() + translate + publish()
        ringBuffer.publishEvent(TRANSLATOR, tradeId, price);
    }

    // 非阻塞发布（RingBuffer 满时返回 false 而非阻塞）
    public boolean tryPublish(RingBuffer<TradeEvent> ringBuffer, long tradeId, double price) {
        return ringBuffer.tryPublishEvent(TRANSLATOR, tradeId, price);
    }
}
```

### 18.4 批量发布示例

```java
public class BatchPublishExample {
    public void publishBatch(RingBuffer<LongEvent> ringBuffer, long[] values) {
        int n = values.length;
        long hi = ringBuffer.next(n);  // 批量申请 n 个序列号
        long lo = hi - (n - 1);

        try {
            for (long seq = lo; seq <= hi; seq++) {
                LongEvent event = ringBuffer.get(seq);
                event.set(values[(int)(seq - lo)]);
            }
        } finally {
            ringBuffer.publish(lo, hi);  // 批量发布
        }
    }
}
```

批量发布的优势：一次 `next(n)` 调用替代 n 次 `next()` 调用，减少了 CAS 竞争（多生产者模式）或条件检查（单生产者模式）的次数。发布时 `publish(lo, hi)` 也只需要一次 `signalAllWhenBlocking()` 而非 n 次。

### 18.5 WorkerPool 竞争消费示例

```java
public class WorkerPoolExample {
    static class OrderWorkHandler implements WorkHandler<OrderEvent> {
        private final String name;

        OrderWorkHandler(String name) {
            this.name = name;
        }

        @Override
        public void onEvent(OrderEvent event) throws Exception {
            System.out.println(name + " 处理订单: " + event.getOrderId());
            // 每个订单只被一个 Worker 处理
        }
    }

    public static void main(String[] args) {
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
            OrderEvent::new, 4096, DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE, new SleepingWaitStrategy());

        // 3 个 Worker 竞争消费，每个事件只被一个 Worker 处理
        disruptor.handleEventsWithWorkerPool(
            new OrderWorkHandler("Worker-1"),
            new OrderWorkHandler("Worker-2"),
            new OrderWorkHandler("Worker-3")
        );

        disruptor.start();
    }
}
```

### 18.6 混合模式：广播 + 竞争 + 管道

```java
public class HybridPatternExample {
    public static void main(String[] args) {
        Disruptor<TradeEvent> disruptor = new Disruptor<>(
            TradeEvent::new, 8192, DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE, new YieldingWaitStrategy());

        // 复杂拓扑：
        // 1. 审计 Handler（广播，每个事件都要审计记录）
        // 2. Worker Pool（竞争消费，3 个 Worker 并行处理业务）
        // 3. 汇报 Handler（管道末端，在审计和业务处理都完成后执行）

        EventHandlerGroup<TradeEvent> auditGroup =
            disruptor.handleEventsWith(new AuditHandler());
        EventHandlerGroup<TradeEvent> workerGroup =
            disruptor.handleEventsWithWorkerPool(
                new BizWorker("W1"), new BizWorker("W2"), new BizWorker("W3"));

        auditGroup.and(workerGroup).then(new ReportHandler());

        disruptor.start();
    }
}
```

---

## 十九、附录：Disruptor 核心类索引

| 类名 | 包名 | 职责 |
|------|------|------|
| `Sequence` | com.lmax.disruptor | 缓存行填充的 volatile long 包装器，所有进度跟踪的基础 |
| `RingBuffer` | com.lmax.disruptor | 环形数组数据结构，管理事件存储和序列号分配 |
| `SingleProducerSequencer` | com.lmax.disruptor | 单生产者序列号分配器，无原子操作 |
| `MultiProducerSequencer` | com.lmax.disruptor | 多生产者序列号分配器，CAS 竞争 + availableBuffer |
| `ProcessingSequenceBarrier` | com.lmax.disruptor | 消费者等待屏障，封装等待策略和依赖链 |
| `BatchEventProcessor` | com.lmax.disruptor | 广播模式消费引擎，支持批量消费 |
| `WorkProcessor` | com.lmax.disruptor | 竞争模式消费引擎，通过共享 workSequence 分配工作 |
| `FixedSequenceGroup` | com.lmax.disruptor | 虚拟 Sequence，get() 返回一组 Sequence 的最小值 |
| `Disruptor` | com.lmax.disruptor.dsl | DSL 入口类，声明式构建消费者拓扑并管理生命周期 |
| `EventHandlerGroup` | com.lmax.disruptor.dsl | DSL 中间结构，支持 then() / and() 链式调用 |
| `ConsumerRepository` | com.lmax.disruptor.dsl | 消费者注册表，维护所有 EventProcessor 的信息 |
| `BlockingWaitStrategy` | com.lmax.disruptor | 基于 Lock/Condition 的阻塞等待 |
| `YieldingWaitStrategy` | com.lmax.disruptor | 自旋 + yield 的低延迟等待 |
| `SleepingWaitStrategy` | com.lmax.disruptor | 三阶段退避（spin → yield → parkNanos） |
| `BusySpinWaitStrategy` | com.lmax.disruptor | 纯自旋，最低延迟，需要绑核 |
| `LiteBlockingWaitStrategy` | com.lmax.disruptor | 优化的阻塞等待，减少不必要的 signalAll |
| `TimeoutBlockingWaitStrategy` | com.lmax.disruptor | 带超时的阻塞等待 |
| `PhasedBackoffWaitStrategy` | com.lmax.disruptor | 可配置的分阶段退避策略 |
| `AlertException` | com.lmax.disruptor | 关闭信号异常（单例 + 无堆栈填充） |
| `InsufficientCapacityException` | com.lmax.disruptor | 容量不足异常（单例 + 无堆栈填充） |
| `Util` | com.lmax.disruptor.util | 工具类：getMinimumSequence()、log2() 等 |
| `ThreadHints` | com.lmax.disruptor.util | Thread.onSpinWait() 的兼容性封装 |

---

**全文完。**

本文基于 LMAX Disruptor 源码（版本 3.4.x / 4.0.x），从 RingBuffer 的底层数据结构出发，逐步展开 Sequence、Sequencer（Single/Multi）、SequenceBarrier、WaitStrategy（7 种实现）、EventProcessor（Batch/Work 两种模式）、EventFactory 预分配、DSL API 等核心组件的全部源码。在每个组件的分析中，不仅解释了"是什么"和"怎么做"，更深入探讨了"为什么这么设计"——从 CPU 缓存层次结构到 MESI 协议，从 CAS 原子操作到 lazySet 的内存屏障语义，从位运算取模到继承链填充。Disruptor 不仅是一个高性能的并发框架，更是 Mechanical Sympathy 思想的完整实践教科书。