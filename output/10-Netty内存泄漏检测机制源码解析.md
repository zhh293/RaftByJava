# Netty 内存泄漏检测机制源码解析

> 基于 Netty 源码，深度解析 ResourceLeakDetector 的完整实现。从一个真实的内存泄漏案例出发，逐步拆解 WeakReference + ReferenceQueue 的检测原理、track() 采样决策、TraceRecord 指数退避记录链、LeakAwareByteBuf 包装策略，以及 SimpleChannelInboundHandler 的自动 release 机制，完整揭示 Netty 如何在几乎零性能开销下发现池化 ByteBuf 的引用计数泄漏。

---

## 一、从一个线上 OOM 事故说起

下面这段代码是一个典型的 Netty 服务端 Handler，看起来没有任何问题，但它会在高并发下缓慢吃掉所有内存，最终触发 OOM：

```java
public class LeakyHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        // 读取数据并处理业务逻辑
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        String request = new String(data, CharsetUtil.UTF_8);
        System.out.println("Received: " + request);

        // 构造响应并发送
        ByteBuf response = ctx.alloc().buffer();
        response.writeBytes("OK".getBytes(CharsetUtil.UTF_8));
        ctx.writeAndFlush(response);

        // 问题在这里：忘记 release 入站的 msg！
        // 正确做法：ReferenceCountUtil.release(msg);
    }
}
```

这段代码有一个致命的缺陷：入站的 `ByteBuf msg` 读完之后没有调用 `release()`。由于 Netty 默认使用池化内存分配器（`PooledByteBufAllocator`），这个 ByteBuf 底层对应的 PoolChunk 中的内存块永远不会被归还到内存池。随着请求的积累，池中可用内存越来越少，最终 Netty 不得不持续向操作系统申请新的 Chunk，直到堆外内存耗尽。

如果你在启动参数中开启了泄漏检测（默认 SIMPLE 级别即已开启），一段时间后你会在日志中看到这样的告警：

```
ERROR io.netty.util.ResourceLeakDetector - LEAK: ByteBuf.release() was not called before it's garbage-collected. 
See https://netty.io/wiki/reference-counted-objects.html for more information.
Recent access records: 
Created at:
	io.netty.buffer.PooledByteBufAllocator.newDirectBuffer(PooledByteBufAllocator.java:402)
	io.netty.buffer.AbstractByteBufAllocator.directBuffer(AbstractByteBufAllocator.java:188)
	io.netty.buffer.AbstractByteBufAllocator.buffer(AbstractByteBufAllocator.java:124)
	io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1410)
	...
```

这条日志告诉我们：某个 ByteBuf 在被 JVM 垃圾回收之前，没有调用 `release()` 将引用计数降为 0。那么问题来了——Netty 是如何在不影响正常业务性能的前提下，检测到一个对象"应该被释放但没被释放"这件事的？

答案就是 `ResourceLeakDetector`——一个基于 `WeakReference` + `ReferenceQueue` 的巧妙检测机制。

---

## 二、引用计数的困境：谁来监督"最后的 release"？

在深入 ResourceLeakDetector 之前，我们需要理解它试图解决的核心矛盾。

Netty 的池化 ByteBuf 使用引用计数管理生命周期。每次调用 `retain()` 引用计数加 1，每次调用 `release()` 引用计数减 1。当引用计数降为 0 时，ByteBuf 被归还到内存池。这个机制本身是完善的，问题出在人——开发者可能忘记调用 `release()`。

引用计数泄漏有两种典型场景：

**场景一：ChannelHandler 读完不 release**。消息沿 Pipeline 传播，最终到达某个 Handler 被消费，但消费者忘记 release。这是最常见的泄漏原因。

**场景二：retain 之后忘记对应的 release**。在消息转发、异步处理等场景中，开发者调用 `retain()` 增加引用计数以延长生命周期，但在异步回调中忘记调用 `release()`。

这两种场景的共同特征是：ByteBuf 的 Java 对象最终会因为没有强引用而被 GC 回收，但它的引用计数仍然大于 0，底层的池化内存块永远无法归还。GC 能回收 Java 对象，却无法知道这个对象还"欠"池一个 release。

**ResourceLeakDetector 的核心思路**就是：利用 JDK 的 `WeakReference` + `ReferenceQueue` 机制，在 GC 回收 ByteBuf Java 对象时，检查它是否已经正确调用了 `release()`（即是否调用了 `leak.close()`）。如果 GC 回收了对象但 leak 没有被 close，就说明发生了泄漏。

---

## 三、四个检测级别：从零开销到全量追踪

ResourceLeakDetector 提供了四个检测级别，通过 JVM 参数 `-Dio.netty.leakDetection.level` 配置：

```java
// ResourceLeakDetector.Level
public enum Level {
    DISABLED,   // 完全关闭，不创建任何 tracker
    SIMPLE,     // 默认级别，采样检测，只报告是否泄漏
    ADVANCED,   // 采样检测，报告泄漏 + 最近访问记录
    PARANOID;   // 100% 检测，报告泄漏 + 所有访问记录
}
```

四个级别的核心差异在于两个维度——**采样率**和**记录深度**：

**DISABLED**：track() 方法直接返回 null，不创建 DefaultResourceLeak 对象，零开销。适用于性能极度敏感的生产环境（但不推荐，因为失去了所有泄漏检测能力）。

**SIMPLE**（默认）：以 1/128 的概率对新分配的 ByteBuf 进行采样。被采样到的 ByteBuf 会被包装为 `SimpleLeakAwareByteBuf`，touch() 方法是空操作，不记录任何访问调用栈。发现泄漏时只报告"有泄漏发生"，不提供调用栈信息。性能开销极低，适用于生产环境。

**ADVANCED**：同样 1/128 概率采样，但被采样到的 ByteBuf 被包装为 `AdvancedLeakAwareByteBuf`，每次读写操作都会记录调用栈（TraceRecord）。发现泄漏时报告最近的访问记录，帮助定位泄漏点。性能开销较高（因为 `Throwable.fillInStackTrace()` 是昂贵操作），适用于测试/预发环境。

**PARANOID**：100% 采样（每个 ByteBuf 都被检测），记录策略与 ADVANCED 相同。性能开销最高，只适用于单元测试和调试。

这些配置参数在类加载时通过静态初始化块读取：

```java
// ResourceLeakDetector 静态初始化
static {
    // ...
    Level defaultLevel = disabled ? Level.DISABLED : DEFAULT_LEVEL;
    String levelStr = SystemPropertyUtil.get(PROP_LEVEL_OLD, defaultLevel.name());
    levelStr = SystemPropertyUtil.get(PROP_LEVEL, levelStr);
    Level level = Level.parseLevel(levelStr);

    TARGET_RECORDS = SystemPropertyUtil.getInt(PROP_TARGET_RECORDS, DEFAULT_TARGET_RECORDS);  // 默认 4
    SAMPLING_INTERVAL = SystemPropertyUtil.getInt(PROP_SAMPLING_INTERVAL, DEFAULT_SAMPLING_INTERVAL);  // 默认 128
    TRACK_CLOSE = SystemPropertyUtil.getBoolean(PROP_TRACK_CLOSE, DEFAULT_TRACK_CLOSE);  // 默认 true

    ResourceLeakDetector.level = level;
}
```

其中 `TARGET_RECORDS` 控制每个 leak tracker 最多保留多少条 TraceRecord（默认 4），`SAMPLING_INTERVAL` 控制采样间隔（默认 128，即 1/128 概率），`TRACK_CLOSE` 控制是否在 close 时记录调用栈（默认 true，用于调试重复 release）。

---

## 四、核心原理：WeakReference + ReferenceQueue 的 GC 协作

ResourceLeakDetector 的检测原理可以用一句话概括：**为被追踪的 ByteBuf 创建一个 WeakReference，当 ByteBuf 被 GC 回收时 WeakReference 自动进入 ReferenceQueue，此时检查该 WeakReference 对应的 leak tracker 是否已经被 close（即 ByteBuf 是否正确 release），如果没有被 close，则判定为泄漏。**

这个机制的精妙之处在于：它不需要任何轮询、定时器或 Finalizer，完全借助 JVM 的 GC 机制实现被动检测。

下面是整个检测流程的示意图：

```
分配阶段                           使用阶段                        GC 阶段
─────────                         ─────────                      ─────────

allocator.buffer()                 handler 使用 ByteBuf           ByteBuf 失去所有强引用
    │                                  │                              │
    ▼                                  │                              ▼
track(buf)                             │                         JVM GC 回收 ByteBuf
    │                                  │                              │
    ▼                                  │                              ▼
创建 DefaultResourceLeak               │                    WeakReference 进入 ReferenceQueue
(继承 WeakReference<ByteBuf>)          │                              │
    │                                  │                              ▼
    ▼                                  │                   下次 track() 调用 reportLeak()
加入 allLeaks 集合                     │                              │
    │                                  │                              ▼
    ▼                                  ▼                     refQueue.poll() 取出 leak
包装为 LeakAwareByteBuf          正常路径: release()                   │
    │                              → leak.close()                     ▼
    ▼                              → 从 allLeaks 移除          dispose(): 检查是否在 allLeaks 中
返回给调用方                       → clear() 清除弱引用               │
                                                              ┌──────┴──────┐
                                 泄漏路径: 忘记 release()      │             │
                                   → leak.close() 未被调用   在 allLeaks   不在 allLeaks
                                   → allLeaks 中仍有此 leak     │             │
                                                              泄漏!         已正常关闭
                                                              输出告警日志    （无操作）
```

关键理解点：

**为什么用 WeakReference 而不是 PhantomReference？** PhantomReference 的 `get()` 方法始终返回 null，无法获取被引用的对象。而 WeakReference 在对象被 GC 之前可以通过 `get()` 获取引用——虽然 DefaultResourceLeak 并不需要这个能力（它不持有强引用），但 WeakReference 的入队时机（GC 判定对象弱可达时）比 PhantomReference 更早一步，能更及时地检测到泄漏。

**为什么不持有 ByteBuf 的强引用？** DefaultResourceLeak 的构造函数中只保存了 `System.identityHashCode(referent)` 而非 referent 本身。如果持有强引用，ByteBuf 永远不会被 GC 回收，WeakReference 也永远不会进入 ReferenceQueue，检测机制就失效了。

---

## 五、源码逐步分析：track() 的采样决策

泄漏检测的入口是 `ResourceLeakDetector.track()` 方法，它在每次分配 ByteBuf 时被调用：

```java
// ResourceLeakDetector.java
public ResourceLeakTracker<T> track(T obj) {
    return track0(obj, false);
}

private DefaultResourceLeak<T> track0(T obj, boolean force) {
    Level level = ResourceLeakDetector.level;
    if (force ||
            level == Level.PARANOID ||
            (level != Level.DISABLED && ThreadLocalRandom.current().nextInt(samplingInterval) == 0)) {
        reportLeak();
        return new DefaultResourceLeak<>(obj, refQueue, allLeaks, getInitialHint(resourceType));
    }
    return null;
}
```

这段代码的采样逻辑非常清晰：

1. 如果 `force` 为 true（由 `trackForcibly()` 调用），无条件创建 tracker
2. 如果级别是 `PARANOID`，无条件创建 tracker（100% 采样）
3. 如果级别不是 `DISABLED`，以 `1/samplingInterval`（默认 1/128）的概率创建 tracker
4. 如果级别是 `DISABLED`，直接返回 null

注意这里用的是 `ThreadLocalRandom` 而非 `Math.random()`。ThreadLocalRandom 是线程本地的随机数生成器，不需要任何同步操作，在高并发场景下比共享的 Random 快一个数量级。同时 `samplingInterval` 默认值 128 是 2 的幂，ThreadLocalRandom 对 2 的幂次取模有额外的性能优化（位运算替代取模）。

**在创建新 tracker 之前，track0() 先调用 reportLeak()**。这是一个很精巧的设计：利用新 ByteBuf 分配的时机来检查之前是否有泄漏。由于泄漏检测依赖 GC 将 WeakReference 放入 ReferenceQueue，而 GC 的发生时机不可控，在 track() 中轮询 ReferenceQueue 是一个低成本的策略——只有在有新 ByteBuf 分配时才检查，不需要额外的后台线程。

调用方是 `AbstractByteBufAllocator.toLeakAwareBuffer()`，它在每次分配 ByteBuf 后调用：

```java
// AbstractByteBufAllocator.java
protected static ByteBuf toLeakAwareBuffer(ByteBuf buf) {
    ResourceLeakTracker<ByteBuf> leak = AbstractByteBuf.leakDetector.track(buf);
    if (leak != null) {
        if (AbstractByteBuf.leakDetector.isRecordEnabled()) {
            buf = new AdvancedLeakAwareByteBuf(buf, leak);
        } else {
            buf = new SimpleLeakAwareByteBuf(buf, leak);
        }
    }
    return buf;
}
```

`isRecordEnabled()` 的判断逻辑：

```java
public boolean isRecordEnabled() {
    Level level = getLevel();
    return (level == Level.ADVANCED || level == Level.PARANOID) && TARGET_RECORDS > 0;
}
```

只有 ADVANCED 和 PARANOID 级别才会启用 record，此时 ByteBuf 被包装为 `AdvancedLeakAwareByteBuf`；SIMPLE 级别使用 `SimpleLeakAwareByteBuf`。如果 track() 返回 null（DISABLED 或未被采样到），ByteBuf 不做任何包装，完全零开销。

---

## 六、DefaultResourceLeak：WeakReference 的子类

`DefaultResourceLeak` 是 ResourceLeakDetector 的私有静态内部类，它是整个泄漏检测机制的核心数据结构。

```java
private static final class DefaultResourceLeak<T>
        extends WeakReference<Object> implements ResourceLeakTracker<T>, ResourceLeak {

    private static final AtomicReferenceFieldUpdater<DefaultResourceLeak<?>, TraceRecord> headUpdater =
            (AtomicReferenceFieldUpdater)
                    AtomicReferenceFieldUpdater.newUpdater(DefaultResourceLeak.class, TraceRecord.class, "head");

    private static final AtomicIntegerFieldUpdater<DefaultResourceLeak<?>> droppedRecordsUpdater =
            (AtomicIntegerFieldUpdater)
                    AtomicIntegerFieldUpdater.newUpdater(DefaultResourceLeak.class, "droppedRecords");

    private volatile TraceRecord head;
    private volatile int droppedRecords;

    private final Set<DefaultResourceLeak<?>> allLeaks;
    private final int trackedHash;

    DefaultResourceLeak(
            Object referent,
            ReferenceQueue<Object> refQueue,
            Set<DefaultResourceLeak<?>> allLeaks,
            Object initialHint) {
        super(referent, refQueue);  // 关键：将自己注册到 refQueue

        this.allLeaks = allLeaks;
        // 只保存 hash 值，不保存强引用！
        trackedHash = System.identityHashCode(referent);
        allLeaks.add(this);
        // 创建初始 TraceRecord，记录分配时的调用栈
        headUpdater.set(this, initialHint == null ?
                new TraceRecord(TraceRecord.BOTTOM) : new TraceRecord(TraceRecord.BOTTOM, initialHint));
    }
    // ...
}
```

构造函数做了四件事：

1. **`super(referent, refQueue)`**：调用 WeakReference 的构造函数，将被追踪的 ByteBuf 作为 referent，并注册到 ReferenceQueue。当 ByteBuf 被 GC 回收时，这个 DefaultResourceLeak 对象会自动进入 refQueue。

2. **`trackedHash = System.identityHashCode(referent)`**：保存 ByteBuf 的 identity hash code（而非对象引用），后续 `close(T trackedObject)` 时用 assert 校验传入的对象是否是同一个。使用 identityHashCode 而不是 hashCode 避免了用户重写 hashCode 导致的问题。

3. **`allLeaks.add(this)`**：将自己加入全局的 `allLeaks` 集合（`ConcurrentHashMap.newKeySet()`）。这个集合是判断泄漏的关键——如果 ByteBuf 被 GC 回收时，对应的 leak 仍在 allLeaks 中，就说明 `close()` 没有被调用，即发生了泄漏。

4. **创建初始 TraceRecord**：记录 ByteBuf 创建时的调用栈，作为 TraceRecord 链表的第一个节点。

### close()：正常释放的路径

当 ByteBuf 被正确 release 时，LeakAwareByteBuf 会调用 `leak.close()`：

```java
@Override
public boolean close() {
    if (allLeaks.remove(this)) {
        // Call clear so the reference is not even enqueued.
        clear();
        headUpdater.set(this, TRACK_CLOSE ? new TraceRecord(true) : null);
        return true;
    }
    return false;
}
```

close() 做了三件事：

1. **`allLeaks.remove(this)`**：从全局集合中移除自己。后续 reportLeak() 检查时就不会将此 leak 判定为泄漏。
2. **`clear()`**：清除 WeakReference 对 ByteBuf 的弱引用。调用 clear() 后，即使 ByteBuf 被 GC 回收，这个 WeakReference 也不会进入 ReferenceQueue，避免了不必要的检查。
3. **记录 CLOSE_MARK**：如果 `TRACK_CLOSE` 为 true（默认），创建一个带 CLOSE_MARK 的 TraceRecord，记录 close 时的调用栈，便于调试"重复 release"等问题。

### dispose()：GC 回收时的路径

当 ByteBuf 被 GC 回收后，DefaultResourceLeak 从 ReferenceQueue 中被 poll 出来，reportLeak() 调用 `dispose()`：

```java
boolean dispose() {
    clear();
    return allLeaks.remove(this);
}
```

如果 `allLeaks.remove(this)` 返回 true，说明 close() 从未被调用过——ByteBuf 被 GC 回收了但引用计数没有归零，这就是一个泄漏。如果返回 false，说明 close() 已经调用过（ByteBuf 被正确 release），不是泄漏。

---

## 七、TraceRecord：继承 Throwable 的调用栈记录

TraceRecord 是 ResourceLeakDetector 的私有静态内部类，它的设计非常巧妙——**通过继承 Throwable 来自动捕获创建时的调用栈**：

```java
private static class TraceRecord extends Throwable {
    private static final long serialVersionUID = 6065153674892850720L;
    public static final int BOTTOM_POS = -1;
    public static final int CLOSE_MARK_POS = -2;

    private static final TraceRecord BOTTOM = new TraceRecord(false) {
        @Override
        public Throwable fillInStackTrace() {
            return this;  // BOTTOM 不需要调用栈
        }
    };

    private final String hintString;
    private final TraceRecord next;
    private final int pos;

    TraceRecord(TraceRecord next, Object hint) {
        hintString = hint instanceof ResourceLeakHint ?
                ((ResourceLeakHint) hint).toHintString() : hint.toString();
        this.next = next;
        this.pos = next.pos + 1;
    }

    TraceRecord(TraceRecord next) {
        hintString = null;
        this.next = next;
        this.pos = next.pos + 1;
    }
    // ...
}
```

**为什么继承 Throwable 而不是自己实现栈追踪？** 因为 Throwable 的构造函数默认会调用 `fillInStackTrace()` 这个 native 方法，它能够以最快的速度（JVM 内部直接遍历栈帧）获取当前线程的完整调用栈。如果用 `Thread.currentThread().getStackTrace()` 则需要额外创建一个 Throwable 再获取栈，反而更慢。直接继承 Throwable 是获取调用栈的最高效方式。

TraceRecord 采用 **单链表** 结构，每个新记录指向前一个记录（`this.next = next`），链表的尾部是 `BOTTOM` 哨兵节点。`pos` 字段记录当前节点在链表中的位置（从 0 开始递增），用于指数退避策略的判断。

`BOTTOM` 是一个特殊的哨兵节点，它覆写了 `fillInStackTrace()` 返回 this，避免为这个不需要调用栈的标记节点执行昂贵的 native 调用。

TraceRecord 的 `toString()` 方法负责将调用栈格式化为可读的字符串：

```java
@Override
public String toString() {
    StringBuilder buf = new StringBuilder(2048);
    if (hintString != null) {
        buf.append("\tHint: ").append(hintString).append(NEWLINE);
    }

    StackTraceElement[] array = getStackTrace();
    // 跳过前 3 个元素（TraceRecord/record0/record 的栈帧）
    out: for (int i = 3; i < array.length; i++) {
        StackTraceElement element = array[i];
        // 过滤噪音栈帧（如 ReferenceCountUtil.touch）
        String[] exclusions = excludedMethods.get();
        for (int k = 0; k < exclusions.length; k += 2) {
            if (exclusions[k].equals(element.getClassName())
                    && exclusions[k + 1].equals(element.getMethodName())) {
                continue out;
            }
        }
        buf.append('\t');
        buf.append(element.toString());
        buf.append(NEWLINE);
    }
    return buf.toString();
}
```

它跳过前 3 个栈帧（这些是 TraceRecord 自身构造链的噪音），并通过 `excludedMethods` 排除如 `ReferenceCountUtil.touch()`、`AdvancedLeakAwareByteBuf.touch()` 等中间方法，让最终输出的调用栈更加清晰、直指问题代码。

---

## 八、record0() 的指数退避策略

当 ByteBuf 被包装为 `AdvancedLeakAwareByteBuf` 时，每次读写操作都会调用 `leak.record()` 记录调用栈。如果一个 ByteBuf 被频繁访问（比如缓存中的 ByteBuf），记录数量会快速增长，消耗大量内存。

为了控制记录数量，`record0()` 实现了一个精巧的**指数退避**策略：

```java
private void record0(Object hint) {
    if (TARGET_RECORDS > 0) {
        TraceRecord oldHead;
        TraceRecord prevHead;
        TraceRecord newHead;
        boolean dropped;
        do {
            if ((prevHead = oldHead = headUpdater.get(this)) == null ||
                    oldHead.pos == TraceRecord.CLOSE_MARK_POS) {
                // already closed.
                return;
            }
            final int numElements = oldHead.pos + 1;
            if (numElements >= TARGET_RECORDS) {
                final int backOffFactor = Math.min(numElements - TARGET_RECORDS, 30);
                dropped = ThreadLocalRandom.current().nextInt(1 << backOffFactor) != 0;
                if (dropped) {
                    prevHead = oldHead.next;  // 丢弃最顶部的旧记录
                }
            } else {
                dropped = false;
            }
            newHead = hint != null ? new TraceRecord(prevHead, hint) : new TraceRecord(prevHead);
        } while (!headUpdater.compareAndSet(this, oldHead, newHead));
        if (dropped) {
            droppedRecordsUpdater.incrementAndGet(this);
        }
    }
}
```

这段代码的核心逻辑如下：

**当记录数小于 TARGET_RECORDS（默认 4）时**：直接压入新记录，不丢弃任何旧记录。这保证了少量访问时的完整记录。

**当记录数达到 TARGET_RECORDS 后**：计算退避因子 `backOffFactor = min(numElements - TARGET_RECORDS, 30)`，以 `1 - 1/2^backOffFactor` 的概率丢弃最顶部的旧记录。具体来说：

- 第 5 条记录（超出 TARGET_RECORDS 1 条）：backOffFactor=1，`1/2^1 = 1/2` 概率保留旧记录，`1/2` 概率丢弃
- 第 6 条记录：backOffFactor=2，`1/4` 概率保留，`3/4` 概率丢弃
- 第 7 条记录：backOffFactor=3，`1/8` 概率保留，`7/8` 概率丢弃
- 第 34+ 条记录：backOffFactor=30（上限），`1/2^30 ≈ 十亿分之一` 概率保留

这个设计有几个优雅的特性：

1. **最近的记录一定被保留**：新记录总是作为新的 head 压入，被丢弃的是之前的 head（即第二新的记录），所以最后一次访问的调用栈一定会被记录。这对泄漏排查至关重要，因为泄漏通常发生在"最后一次使用但忘记 release"的地方。

2. **记录数量有概率性上限**：虽然没有硬上限，但由于指数退避，实际记录数很难超过 `TARGET_RECORDS + 常数`。

3. **早期记录有更大的保留概率**：由于创建时的 TraceRecord（`BOTTOM` 的上一层）位于链表最底部，不会被退避策略丢弃，所以 ByteBuf 的创建点始终可见。

4. **无锁并发安全**：整个操作通过 `headUpdater.compareAndSet()` 实现 CAS 替换，多线程并发 record 时不需要任何锁。如果 CAS 失败，说明另一个线程刚刚修改了 head，当前线程重试即可。

---

## 九、reportLeak()：泄漏报告的完整流程

`reportLeak()` 是在每次 `track0()` 时被调用的，它负责从 ReferenceQueue 中取出已被 GC 回收的 leak 对象并检查是否存在泄漏：

```java
private void reportLeak() {
    if (!needReport()) {
        clearRefQueue();
        return;
    }

    // Detect and report previous leaks.
    for (;;) {
        DefaultResourceLeak<?> ref = (DefaultResourceLeak<?>) refQueue.poll();
        if (ref == null) {
            break;
        }

        if (!ref.dispose()) {
            continue;
        }

        String records = ref.getReportAndClearRecords();
        if (reportedLeaks.add(records)) {
            if (records.isEmpty()) {
                reportUntracedLeak(resourceType);
            } else {
                reportTracedLeak(resourceType, records);
            }

            LeakListener listener = leakListener;
            if (listener != null) {
                listener.onLeak(resourceType, records);
            }
        }
    }
}
```

整个流程分为以下步骤：

**第一步：检查日志级别**。如果 ERROR 日志未启用（`!needReport()`），只清空 ReferenceQueue（避免积压），不做报告。

**第二步：轮询 ReferenceQueue**。通过 `refQueue.poll()` 非阻塞地取出所有已被 GC 回收的 WeakReference。poll() 返回 null 表示队列为空。

**第三步：dispose() 检查**。对每个 poll 出来的 leak 调用 `dispose()`。如果 dispose() 返回 false，说明这个 leak 已经被正确 close 过（ByteBuf 已正确 release），跳过。返回 true 则说明发现泄漏。

**第四步：去重**。通过 `reportedLeaks.add(records)` 对泄漏报告去重。如果相同的调用栈模式已经报告过，不再重复输出。`reportedLeaks` 是 `ConcurrentHashMap.newKeySet()`，天然支持并发。

**第五步：输出报告**。根据 records 是否为空，调用不同的报告方法：

```java
// 有调用栈记录（ADVANCED/PARANOID 级别）
protected void reportTracedLeak(String resourceType, String records) {
    logger.error(
            "LEAK: {}.release() was not called before it's garbage-collected. " +
            "See https://netty.io/wiki/reference-counted-objects.html for more information.{}",
            resourceType, records);
}

// 无调用栈记录（SIMPLE 级别）
protected void reportUntracedLeak(String resourceType) {
    logger.error("LEAK: {}.release() was not called before it's garbage-collected. " +
            "Enable advanced leak reporting to find out where the leak occurred. " +
            "To enable advanced leak reporting, " +
            "specify the JVM option '-D{}={}' or call {}.setLevel() " +
            "See https://netty.io/wiki/reference-counted-objects.html for more information.",
            resourceType, PROP_LEVEL, Level.ADVANCED.name().toLowerCase(), simpleClassName(this));
}
```

SIMPLE 级别的 untraced 报告会友好地提示用户如何开启 ADVANCED 级别以获取详细的调用栈信息。

`getReportAndClearRecords()` 方法负责将 TraceRecord 链表格式化为可读的报告字符串：

```java
String getReportAndClearRecords() {
    TraceRecord oldHead = headUpdater.getAndSet(this, null);
    return generateReport(oldHead);
}

private String generateReport(TraceRecord oldHead) {
    if (oldHead == null) {
        return EMPTY_STRING;
    }

    final int dropped = droppedRecordsUpdater.get(this);
    int duped = 0;
    int present = oldHead.pos + 1;

    StringBuilder buf = new StringBuilder(present * 2048).append(NEWLINE);
    buf.append("Recent access records: ").append(NEWLINE);

    int i = 1;
    Set<String> seen = new HashSet<>(present);
    for (; oldHead != TraceRecord.BOTTOM; oldHead = oldHead.next) {
        String s = oldHead.toString();
        if (seen.add(s)) {
            if (oldHead.next == TraceRecord.BOTTOM) {
                buf.append("Created at:").append(NEWLINE).append(s);
            } else {
                buf.append('#').append(i++).append(':').append(NEWLINE).append(s);
            }
        } else {
            duped++;
        }
    }

    if (duped > 0) {
        buf.append(": ").append(duped)
           .append(" leak records were discarded because they were duplicates").append(NEWLINE);
    }
    if (dropped > 0) {
        buf.append(": ").append(dropped)
           .append(" leak records were discarded because the leak record count is targeted to ")
           .append(TARGET_RECORDS).append(". Use system property ")
           .append(PROP_TARGET_RECORDS).append(" to increase the limit.").append(NEWLINE);
    }

    buf.setLength(buf.length() - NEWLINE.length());
    return buf.toString();
}
```

报告的格式非常清晰：最近的访问记录编号为 `#1`、`#2` 等，最底部的链表节点（紧邻 BOTTOM 的节点）标记为 `Created at:`，表示 ByteBuf 的创建位置。重复的调用栈和被退避策略丢弃的记录会在报告末尾汇总显示。

---

## 十、SimpleLeakAwareByteBuf vs AdvancedLeakAwareByteBuf

ByteBuf 被 track() 成功后，需要被包装成一个"泄漏感知"的代理对象。Netty 提供了两种包装策略，对应不同的检测深度。

### 10.1 SimpleLeakAwareByteBuf：轻量级包装

`SimpleLeakAwareByteBuf` 继承 `WrappedByteBuf`，是 SIMPLE 级别下的包装器：

```java
class SimpleLeakAwareByteBuf extends WrappedByteBuf {

    private final ByteBuf trackedByteBuf;
    final ResourceLeakTracker<ByteBuf> leak;

    SimpleLeakAwareByteBuf(ByteBuf wrapped, ByteBuf trackedByteBuf, ResourceLeakTracker<ByteBuf> leak) {
        super(wrapped);
        this.trackedByteBuf = ObjectUtil.checkNotNull(trackedByteBuf, "trackedByteBuf");
        this.leak = ObjectUtil.checkNotNull(leak, "leak");
    }

    @Override
    public ByteBuf touch() {
        return this;  // 空操作！
    }

    @Override
    public ByteBuf touch(Object hint) {
        return this;  // 空操作！
    }

    @Override
    public boolean release() {
        try {
            if (super.release()) {
                closeLeak();
                return true;
            }
            return false;
        } catch (IllegalReferenceCountException irce) {
            ThrowableUtil.addSuppressed(irce, leak.getCloseStackTraceIfAny());
            throw irce;
        }
    }

    private void closeLeak() {
        boolean closed = leak.close(trackedByteBuf);
        assert closed;
    }
    // ...
}
```

SimpleLeakAwareByteBuf 的核心特征：

1. **touch() 是空操作**：不记录任何调用栈，因为 SIMPLE 级别不需要访问记录。这意味着 SIMPLE 级别的性能开销极低，几乎只有一次 WeakReference 创建的成本。

2. **release() 时调用 leak.close()**：当引用计数降为 0（`super.release()` 返回 true），立即调用 `closeLeak()` 将 leak 从 allLeaks 集合中移除并清除 WeakReference。这确保了正确 release 的 ByteBuf 不会被误报为泄漏。

3. **异常增强**：如果 release 时抛出 `IllegalReferenceCountException`（比如对已销毁的 ByteBuf 重复 release），通过 `addSuppressed` 将 close 时的调用栈附加到异常中，帮助调试。

### 10.2 AdvancedLeakAwareByteBuf：全方位追踪

`AdvancedLeakAwareByteBuf` 继承 `SimpleLeakAwareByteBuf`，是 ADVANCED/PARANOID 级别下的包装器。它覆写了 ByteBuf 的几乎所有方法，在每次操作前记录调用栈：

```java
final class AdvancedLeakAwareByteBuf extends SimpleLeakAwareByteBuf {

    private static final boolean ACQUIRE_AND_RELEASE_ONLY;

    static {
        ACQUIRE_AND_RELEASE_ONLY = SystemPropertyUtil.getBoolean(PROP_ACQUIRE_AND_RELEASE_ONLY, false);
        ResourceLeakDetector.addExclusions(
                AdvancedLeakAwareByteBuf.class, "touch", "recordLeakNonRefCountingOperation");
    }

    static void recordLeakNonRefCountingOperation(ResourceLeakTracker<ByteBuf> leak) {
        if (!ACQUIRE_AND_RELEASE_ONLY) {
            leak.record();
        }
    }

    @Override
    public byte getByte(int index) {
        recordLeakNonRefCountingOperation(leak);
        return super.getByte(index);
    }

    @Override
    public ByteBuf writeByte(int value) {
        recordLeakNonRefCountingOperation(leak);
        return super.writeByte(value);
    }

    @Override
    public ByteBuf retain() {
        leak.record();  // retain/release 始终记录
        return super.retain();
    }

    @Override
    public boolean release() {
        leak.record();  // retain/release 始终记录
        return super.release();
    }

    @Override
    public ByteBuf touch() {
        leak.record();  // touch 不再是空操作！
        return this;
    }

    @Override
    public ByteBuf touch(Object hint) {
        leak.record(hint);
        return this;
    }
    // ... 省略数十个类似的覆写方法
}
```

AdvancedLeakAwareByteBuf 与 SimpleLeakAwareByteBuf 的关键差异：

1. **所有读写方法都调用 `recordLeakNonRefCountingOperation(leak)`**：getByte、writeInt、readLong、slice、duplicate、copy 等数十个方法都被覆写，在调用前先通过 `leak.record()` 记录调用栈。这就是 ADVANCED 级别性能开销高的原因——每次 ByteBuf 操作都会创建一个 TraceRecord（继承 Throwable，触发 `fillInStackTrace()`）。

2. **retain/release/touch 始终调用 leak.record()**：这些引用计数相关的操作无条件记录，即使 `ACQUIRE_AND_RELEASE_ONLY` 为 true。这是因为引用计数操作本身就是泄漏排查的核心线索。

3. **ACQUIRE_AND_RELEASE_ONLY 开关**：通过 `-Dio.netty.leakDetection.acquireAndReleaseOnly=true` 可以让非引用计数操作（如 getByte、writeInt）不记录调用栈，只保留 retain/release/touch 的记录。这在 ADVANCED 级别下是一个有用的折中——减少记录量的同时保留关键信息。

4. **touch() 不再是空操作**：在 ADVANCED 级别下，`touch()` 调用 `leak.record()`，`touch(hint)` 调用 `leak.record(hint)`。开发者可以在代码中通过 `buf.touch("传递给异步处理")` 为 ByteBuf 的流转路径添加语义化注释，这些注释会出现在泄漏报告中。

静态初始化块中的 `ResourceLeakDetector.addExclusions()` 将 `touch` 和 `recordLeakNonRefCountingOperation` 方法加入排除列表，避免这些"中间人"方法出现在 TraceRecord 的调用栈中。

---

## 十一、SimpleChannelInboundHandler 的自动 release 机制

为了降低开发者忘记 release 的概率，Netty 提供了 `SimpleChannelInboundHandler`，它在消息处理完成后自动调用 `ReferenceCountUtil.release(msg)`：

```java
public abstract class SimpleChannelInboundHandler<I> extends ChannelInboundHandlerAdapter {

    private final TypeParameterMatcher matcher;
    private final boolean autoRelease;

    protected SimpleChannelInboundHandler() {
        this(true);  // 默认 autoRelease = true
    }

    protected SimpleChannelInboundHandler(boolean autoRelease) {
        matcher = TypeParameterMatcher.find(this, SimpleChannelInboundHandler.class, "I");
        this.autoRelease = autoRelease;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean release = true;
        try {
            if (acceptInboundMessage(msg)) {
                @SuppressWarnings("unchecked")
                I imsg = (I) msg;
                channelRead0(ctx, imsg);
            } else {
                release = false;
                ctx.fireChannelRead(msg);
            }
        } finally {
            if (autoRelease && release) {
                ReferenceCountUtil.release(msg);
            }
        }
    }

    protected abstract void channelRead0(ChannelHandlerContext ctx, I msg) throws Exception;
}
```

这段代码的设计值得仔细品味：

**类型匹配机制**：通过 `TypeParameterMatcher` 自动推断泛型参数 `I` 的实际类型，`acceptInboundMessage(msg)` 检查消息是否匹配。只有匹配的消息才会传给 `channelRead0()`，不匹配的消息通过 `ctx.fireChannelRead(msg)` 继续沿 Pipeline 传播（此时 `release = false`，不释放消息）。

**finally 块保证释放**：即使 `channelRead0()` 抛出异常，finally 块仍然会执行 release。这避免了业务异常导致的内存泄漏。

**autoRelease 开关**：如果开发者需要在 `channelRead0()` 中将消息传递给异步操作（如写入另一个 Channel），可以通过构造函数 `super(false)` 关闭自动释放，但此时开发者需要自己负责在异步操作完成后调用 release。

**注意事项**：如果在 `channelRead0()` 中将 msg 传递给下一个 Handler（`ctx.fireChannelRead(msg)`），必须先调用 `msg.retain()` 增加引用计数，因为 finally 块会在方法返回后释放一次。或者使用 `super(false)` 关闭自动释放。

---

## 十二、ReferenceCountUtil：引用计数的便捷工具

`ReferenceCountUtil` 是一个静态工具类，封装了对 `ReferenceCounted` 接口的安全操作：

```java
public final class ReferenceCountUtil {

    static {
        ResourceLeakDetector.addExclusions(ReferenceCountUtil.class, "touch");
    }

    public static boolean release(Object msg) {
        if (msg instanceof ReferenceCounted) {
            return ((ReferenceCounted) msg).release();
        }
        return false;
    }

    public static void safeRelease(Object msg) {
        try {
            release(msg);
        } catch (Throwable t) {
            logger.warn("Failed to release a message: {}", msg, t);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T retain(T msg) {
        if (msg instanceof ReferenceCounted) {
            return (T) ((ReferenceCounted) msg).retain();
        }
        return msg;
    }

    @SuppressWarnings("unchecked")
    public static <T> T touch(T msg) {
        if (msg instanceof ReferenceCounted) {
            return (T) ((ReferenceCounted) msg).touch();
        }
        return msg;
    }

    @SuppressWarnings("unchecked")
    public static <T> T touch(T msg, Object hint) {
        if (msg instanceof ReferenceCounted) {
            return (T) ((ReferenceCounted) msg).touch(hint);
        }
        return msg;
    }
    // ...
}
```

**release(Object) vs safeRelease(Object)** 的核心区别在于异常处理。`release()` 会将异常（如 `IllegalReferenceCountException`）直接抛给调用方，适用于需要感知释放结果的场景。`safeRelease()` 用 try-catch 包裹，异常只打 warn 日志不抛出，适用于清理逻辑中"尽力释放、出错也不影响主流程"的场景，例如 Pipeline 中的异常处理 Handler。

静态初始化块中的 `ResourceLeakDetector.addExclusions(ReferenceCountUtil.class, "touch")` 将 touch 方法加入排除列表，确保泄漏报告中的调用栈不会显示这个中间方法，而是直接指向实际调用 touch 的业务代码。

这些方法之所以接受 `Object` 而非 `ReferenceCounted` 参数，是因为在 Pipeline 中传播的消息类型是 `Object`，并非所有消息都实现了 `ReferenceCounted`。通过 `instanceof` 检查，这些方法可以安全地应用于任何消息，不需要调用方先做类型判断。

---

## 十三、ResourceLeakDetectorFactory：抽象工厂模式

`ResourceLeakDetectorFactory` 采用抽象工厂 + 单例模式管理 ResourceLeakDetector 的创建：

```java
public abstract class ResourceLeakDetectorFactory {
    private static volatile ResourceLeakDetectorFactory factoryInstance =
            new DefaultResourceLeakDetectorFactory();

    public static ResourceLeakDetectorFactory instance() {
        return factoryInstance;
    }

    public static void setResourceLeakDetectorFactory(ResourceLeakDetectorFactory factory) {
        factoryInstance = ObjectUtil.checkNotNull(factory, "factory");
    }

    public final <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource) {
        return newResourceLeakDetector(resource, ResourceLeakDetector.SAMPLING_INTERVAL);
    }
    // ...
}
```

`DefaultResourceLeakDetectorFactory` 在构造时会检查 `io.netty.customResourceLeakDetector` 系统属性。如果配置了自定义的 ResourceLeakDetector 实现类，工厂会通过反射创建该类的实例。这个扩展点允许用户自定义泄漏检测行为，例如将泄漏信息发送到监控系统而非仅打印日志。

---

## 十四、最佳实践与常见泄漏场景

### 14.1 各环境推荐配置

**开发环境**：使用 PARANOID 级别，100% 采样，确保每个泄漏都能被捕获。

```
-Dio.netty.leakDetection.level=PARANOID
```

**测试/预发环境**：使用 ADVANCED 级别，1/128 采样率可以在保持可接受性能的前提下提供完整的调用栈。

```
-Dio.netty.leakDetection.level=ADVANCED
-Dio.netty.leakDetection.targetRecords=8
```

**生产环境**：使用默认的 SIMPLE 级别。不要轻易关闭泄漏检测（DISABLED），因为 SIMPLE 级别的性能开销极低（1/128 采样率 + 无 TraceRecord 记录），却能在泄漏发生时提供关键的告警。

```
-Dio.netty.leakDetection.level=SIMPLE
```

**CI 集成**：Netty 自身的 CI 流水线中包含一个 `.github/scripts/check_leak.sh` 脚本，它会检查构建日志中是否出现 `"LEAK:"` 前缀的告警，从而在持续集成中自动捕获泄漏回归。

### 14.2 常见泄漏场景

**场景一：ChannelHandler 消费消息后忘记 release**

这是最常见的泄漏场景。解决方案是使用 `SimpleChannelInboundHandler` 代替 `ChannelInboundHandlerAdapter`，或者在 finally 块中手动 release。

```java
// 推荐方案：使用 SimpleChannelInboundHandler
public class SafeHandler extends SimpleChannelInboundHandler<ByteBuf> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        // msg 会在方法返回后自动 release
        processMessage(msg);
    }
}
```

**场景二：异常路径上忘记 release**

在解码器或 Handler 中，如果正常路径调用了 release 但异常路径没有，也会泄漏。

```java
// 错误示例
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf buf = (ByteBuf) msg;
    if (buf.readableBytes() < 4) {
        return;  // 早返回但忘记 release！
    }
    // ... 处理逻辑
    buf.release();
}

// 正确示例
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf buf = (ByteBuf) msg;
    try {
        if (buf.readableBytes() < 4) {
            return;
        }
        // ... 处理逻辑
    } finally {
        buf.release();
    }
}
```

**场景三：retain 后跨线程传递忘记 release**

在将 ByteBuf 传递到另一个线程异步处理时，通常需要先 retain，但异步回调中容易忘记 release。

```java
// 错误示例
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf buf = (ByteBuf) msg;
    buf.retain();
    executor.submit(() -> {
        processAsync(buf);
        // 忘记 buf.release()!
    });
    buf.release();  // 释放当前引用
}

// 正确示例
public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf buf = (ByteBuf) msg;
    buf.retain();
    executor.submit(() -> {
        try {
            processAsync(buf);
        } finally {
            buf.release();
        }
    });
    buf.release();
}
```

**场景四：CompositeByteBuf 添加组件后忘记 release 原始 Buffer**

`CompositeByteBuf.addComponent()` 会 retain 被添加的 ByteBuf，如果调用方仍然持有对原始 ByteBuf 的引用，需要注意引用计数的平衡。使用 `addComponent(true, buf)` 可以自动管理 writerIndex，但引用计数仍需开发者注意。

---

## 十五、本篇涉及的设计模式

1. **代理模式（Proxy）/ 装饰器模式（Decorator）**：`SimpleLeakAwareByteBuf` 和 `AdvancedLeakAwareByteBuf` 都是对原始 ByteBuf 的装饰包装。它们继承 `WrappedByteBuf`，在不改变原始 ByteBuf 行为的前提下，透明地增加泄漏追踪能力。SIMPLE 级别的包装器几乎不增加开销（touch 空操作），ADVANCED 级别的包装器在每次操作前插入 record 调用。两层包装器的继承关系（Advanced 继承 Simple）也是装饰器模式的典型应用——逐层增强功能。

2. **观察者模式（Observer）**：`WeakReference` + `ReferenceQueue` 本质上是一个 JVM 级别的观察者模式。DefaultResourceLeak（观察者）通过 WeakReference 注册到 ReferenceQueue（事件总线），当 ByteBuf（被观察者）被 GC 回收时，JVM 自动将 WeakReference 放入 ReferenceQueue，触发 reportLeak() 的检查逻辑。整个过程不需要显式的事件注册/注销，由 JVM 的垃圾回收器充当事件分发器。

3. **抽象工厂模式（Abstract Factory）**：`ResourceLeakDetectorFactory` 是一个抽象工厂，通过 `newResourceLeakDetector()` 方法创建 ResourceLeakDetector 实例。`DefaultResourceLeakDetectorFactory` 是默认实现，支持通过 `io.netty.customResourceLeakDetector` 系统属性注入自定义的工厂实现。这个设计允许用户在不修改 Netty 源码的前提下替换泄漏检测器。

4. **模板方法模式（Template Method）**：`ResourceLeakDetector` 中的 `reportTracedLeak()` 和 `reportUntracedLeak()` 是 protected 方法，可以被子类覆写以自定义泄漏报告的输出方式。`needReport()` 同样可以被覆写以控制报告条件。基类定义了 `reportLeak()` 的完整流程（poll → dispose → 去重 → 报告），子类只需覆写报告输出部分。

5. **哨兵模式（Sentinel）**：`TraceRecord.BOTTOM` 是 TraceRecord 链表的哨兵节点，它覆写 `fillInStackTrace()` 返回 this（避免不必要的 native 调用），`pos` 设为 `BOTTOM_POS(-1)` 作为链表终止标记。所有链表遍历都以 `oldHead != TraceRecord.BOTTOM` 为终止条件，避免了空指针检查。

---

## 十六、本篇涉及的高性能并发技术

1. **WeakReference + ReferenceQueue 的 GC 协作检测**：这是整个泄漏检测机制的基石。通过 JDK 内置的弱引用机制，在 GC 回收 ByteBuf 对象时自动将 DefaultResourceLeak 放入 ReferenceQueue，实现了零轮询、零定时器的被动检测。相比 Finalizer 或 PhantomReference + Cleaner，WeakReference 的入队时机更早，检测延迟更低。这种利用 GC 副作用实现资源审计的思路，是对 JVM 引用机制的深度运用。

2. **AtomicReferenceFieldUpdater 的无锁 CAS 更新**：`DefaultResourceLeak` 使用 `AtomicReferenceFieldUpdater<DefaultResourceLeak<?>, TraceRecord>` 对 `head` 字段进行 CAS 更新，而不是使用 `AtomicReference` 包装类。这避免了额外对象头的内存开销（每个 DefaultResourceLeak 节省约 16 字节），在存在大量被追踪 ByteBuf 的场景下，内存节省显著。同样，`droppedRecords` 使用 `AtomicIntegerFieldUpdater` 替代 `AtomicInteger`。

3. **概率采样的性能控制**：track0() 使用 `ThreadLocalRandom.current().nextInt(samplingInterval)` 实现概率采样。ThreadLocalRandom 是线程本地的随机数生成器，完全无竞争。采样间隔默认 128（2 的幂），使得 ThreadLocalRandom 内部可以使用位运算替代取模，进一步降低开销。1/128 的采样率意味着每 128 次 ByteBuf 分配只有 1 次会创建 DefaultResourceLeak 对象，将泄漏检测对正常分配路径的性能影响降至几乎不可感知。

4. **指数退避的记录控制**：record0() 的退避策略通过 `ThreadLocalRandom.current().nextInt(1 << backOffFactor) != 0` 实现概率丢弃。backOffFactor 从 0 开始线性增长（上限 30），丢弃概率从 0 指数增长到接近 100%。这个设计在记录数量和信息价值之间取得了精妙的平衡：最近的访问和最初的创建始终被保留（对排查泄漏最有价值），中间的大量重复访问以指数递减的概率被保留（节省内存且不丢失代表性样本）。

5. **ConcurrentHashMap.newKeySet() 的并发安全集合**：`allLeaks` 和 `reportedLeaks` 都使用 `ConcurrentHashMap.newKeySet()` 创建并发安全的 Set。这个方法返回的是 `ConcurrentHashMap.KeySetView`，底层基于 ConcurrentHashMap 的分段锁（JDK 8+ 是 CAS + synchronized 的混合策略），在高并发下比 `Collections.synchronizedSet()` 有更好的吞吐量。allLeaks 的 add/remove 操作分别发生在 track() 和 close()/dispose() 中，可能被不同线程同时调用。

6. **fillInStackTrace 的选择性抑制**：`TraceRecord.BOTTOM` 覆写 `fillInStackTrace()` 返回 this，避免了对这个不需要调用栈的哨兵节点执行昂贵的 native 调用。`fillInStackTrace()` 是 Throwable 构造函数中最耗时的操作（需要遍历整个线程调用栈），在高频创建 TraceRecord 的 ADVANCED/PARANOID 级别下，这个优化避免了一次无意义的 native 调用开销。
