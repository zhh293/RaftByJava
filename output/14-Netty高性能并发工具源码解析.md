# Netty 高性能并发工具源码解析

> **Netty 源码深度研究系列 · 第 14 篇**
>
> 基于 Netty 主分支源码，深入剖析 Netty 自研的五大高性能并发基础设施——Recycler 对象池、FastThreadLocal、MpscIntQueue 无锁队列、Promise/Future 异步模型、以及 AtomicIntegerFieldUpdater 的系统性应用，揭示 Netty 在"最后一公里"极致性能优化上的工程哲学。

---

## 一、Recycler：轻量级对象池

### 1.1 JDK 原生方案的问题

在高吞吐网络框架中，ByteBuf、Entry、ChannelOutboundBuffer.Entry 等对象的创建和销毁频率极高。每次 `new` 操作都会触发堆内存分配，而大量短生命周期对象又会给 GC 带来沉重压力——尤其在 Young GC 频繁触发的场景下，Stop-The-World 暂停会直接影响尾部延迟。JDK 标准库没有提供通用的轻量级对象池（`commons-pool2` 等第三方库又过于重量级，引入了过多同步开销），Netty 因此自研了 `Recycler`。

### 1.2 Netty 的改进：LocalPool 架构

当前版本的 Recycler 已从早期的 Stack + WeakOrderQueue 架构演进为 **LocalPool + JCTools MessagePassingQueue** 的设计。核心思想是：每个线程持有一个 `LocalPool` 实例（通过 FastThreadLocal 存储），对象的获取和回收都尽量在本线程内完成，跨线程回收则通过无锁队列中转。

`LocalPool` 的核心数据结构包含以下几个部分：

```java
private abstract static class LocalPool<H, T> {
    private final int ratioInterval;      // 采样间隔（默认 8）
    private final H[] batch;              // 批量回收数组
    private int batchSize;                // batch 当前大小
    private Thread owner;                 // 所属线程
    private MessagePassingQueue<H> pooledHandles;  // 底层队列
    private int ratioCounter;             // 采样计数器
}
```

队列的选择取决于 `owner` 是否为 null：当 `owner != null` 时（即线程私有池），使用 MPSC 队列（`newMpscQueue`），因为只有 owner 线程会消费；当 `owner == null` 时（即共享池，通过 `Recycler(int maxCapacity, boolean unguarded)` 构造），使用 MPMC 队列（`newFixedMpmcQueue`），允许多个线程并发消费。

### 1.3 源码实现：acquire / release / get

**acquire() —— 对象获取**的路径是先查 batch 数组，再降级到队列：

```java
protected final H acquire() {
    int size = batchSize;
    if (size == 0) {
        final MessagePassingQueue<H> handles = pooledHandles;
        if (handles == null) {
            return null;
        }
        return handles.relaxedPoll();    // 无序列化保证的 poll，更高效
    }
    int top = size - 1;
    final H h = batch[top];
    batchSize = top;
    batch[top] = null;
    return h;
}
```

batch 数组是一个栈结构，从顶部取出元素。只有当 batch 为空时，才会去 `pooledHandles` 队列做一次 `relaxedPoll()`。这里使用 `relaxedPoll()` 而非 `poll()` 是因为对象池场景下不需要严格的内存序列化保证，放宽语义可以减少不必要的内存屏障。

**release() —— 对象归还**的逻辑分三种情况：

```java
protected final void release(H handle) {
    Thread owner = this.owner;
    if (owner != null && Thread.currentThread() == owner && batchSize < batch.length) {
        batch[batchSize] = handle;       // 快路径：直接放 batch
        batchSize++;
    } else if (owner != null && isTerminated(owner)) {
        pooledHandles = null;            // owner 线程已终止，清空池
        this.owner = null;
    } else {
        MessagePassingQueue<H> handles = pooledHandles;
        if (handles != null) {
            handles.relaxedOffer(handle); // 跨线程：放入无锁队列
        }
    }
}
```

第一条路径是最高频的"热路径"：当前线程就是 owner 且 batch 未满，直接把 handle 放进 batch 数组，零竞争、零同步。第二条路径处理 owner 线程已经终止的边界情况——此时池中的对象不再有意义，直接清空以避免内存泄漏。第三条路径是跨线程回收，通过 `relaxedOffer()` 将 handle 放入共享的 MPSC 队列，等待 owner 线程下次 acquire 时消费。

**get() —— 顶层入口**还包含一个重要的快速退出路径：

```java
public final T get() {
    if (localPool != null) {
        return localPool.getWith(this);
    } else {
        if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
            return newObject((Handle<T>) NOOP_HANDLE);  // 非 FastThreadLocalThread，直接 new
        }
        return threadLocalPool.get().getWith(this);
    }
}
```

如果当前线程不是 `FastThreadLocalThread`（例如用户自己创建的普通线程），Recycler 不会尝试池化，而是直接创建新对象并关联一个 `NOOP_HANDLE`——这个 handle 的 `recycle()` 方法是空操作。这个决策背后的考量是：非 FastThreadLocalThread 没有高效的 ThreadLocal 支持，强行池化反而会引入 JDK ThreadLocal 的哈希查找开销。

### 1.4 设计要点：GuardedLocalPool vs UnguardedLocalPool

Recycler 提供了两种 LocalPool 变体。`GuardedLocalPool` 通过 `DefaultHandle` 中的 `AtomicIntegerFieldUpdater` 进行状态检查，handle 只有 `STATE_CLAIMED`（已分配）和 `STATE_AVAILABLE`（可回收）两种状态：

```java
private static final int STATE_CLAIMED = 0;
private static final int STATE_AVAILABLE = 1;
private static final AtomicIntegerFieldUpdater<DefaultHandle<?>> STATE_UPDATER;

private void toAvailable() {
    int prev = STATE_UPDATER.getAndSet(this, STATE_AVAILABLE);
    if (prev == STATE_AVAILABLE) {
        throw new IllegalStateException("Object has been recycled already.");
    }
}
```

`recycle()` 时先 CAS 将状态从 CLAIMED 切换到 AVAILABLE，如果发现已经是 AVAILABLE 则抛出异常——这可以检测出"同一对象被回收两次"的 Bug。`claim()` 时使用 `lazySet` 将状态切回 CLAIMED，因为 acquire 发生在同一线程内，不需要完整的内存屏障。

`UnguardedLocalPool` 则跳过所有状态检查，直接操作对象本身。它适用于确定不会出现并发回收的场景（例如 EventLoop 线程内部的对象），可以节省一次原子操作的开销。

### 1.5 采样率控制

Recycler 并不会池化所有对象。`ratioInterval`（默认值 8）控制着"每 N 次分配请求中只有 1 次真正池化"：

```java
boolean canAllocatePooled() {
    if (ratioInterval < 0) { return false; }
    if (ratioInterval == 0) { return true; }
    if (++ratioCounter >= ratioInterval) {
        ratioCounter = 0;
        return true;
    }
    return false;
}
```

这意味着每 8 次 `get()` 调用中，只有 1 次会创建带有真实 handle 的池化对象，其余 7 次创建的对象关联 `NOOP_HANDLE`，回收时直接丢弃。这种采样策略的意义在于：在突发流量场景下，如果每个对象都池化，池的容量会急剧膨胀，反而浪费内存。通过采样，池的增长速度被控制在合理范围内，同时在稳态下仍能提供足够的复用率。

---

## 二、FastThreadLocal：高性能线程局部变量

### 2.1 JDK ThreadLocal 的线性探测问题

JDK 原生的 `ThreadLocal` 在内部使用 `ThreadLocalMap`，这是一个基于开放地址法（线性探测）的自定义哈希表。每次 `get()` 操作需要计算哈希值，然后在 `Entry[]` 数组中进行探测——如果发生哈希冲突，就需要逐个向后扫描，直到找到匹配的 key 或空槽。当一个线程绑定了大量 ThreadLocal 变量时（Netty 的 EventLoop 线程通常绑定数十个），冲突概率上升，`get()` 操作退化为 O(n) 扫描。此外，ThreadLocal 的 key 是弱引用，每次 `get()`/`set()`/`remove()` 都需要执行过期条目清理（`expungeStaleEntry`），这又引入了额外的遍历开销。

### 2.2 Netty 的改进：数组直接索引 O(1)

FastThreadLocal 的核心思路极其简洁：**用全局递增的整数 index 替代哈希查找**。每个 `FastThreadLocal` 实例在构造时分配一个唯一的递增索引：

```java
private final int index;

public FastThreadLocal() {
    index = InternalThreadLocalMap.nextVariableIndex();
}
```

`nextVariableIndex()` 是一个基于 `AtomicInteger` 的全局计数器，保证每个 FastThreadLocal 实例拿到的 index 互不冲突。读取时直接按 index 下标访问 `InternalThreadLocalMap` 中的 `Object[] indexedVariables` 数组：

```java
public final V get() {
    InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
    Object v = threadLocalMap.indexedVariable(index);
    if (v != InternalThreadLocalMap.UNSET) {
        return (V) v;
    }
    return initialize(threadLocalMap);
}

public Object indexedVariable(int index) {
    Object[] lookup = indexedVariables;
    return index < lookup.length ? lookup[index] : UNSET;
}
```

这实现了真正的 O(1) 常量时间访问——没有哈希计算、没有冲突探测、没有过期清理，就是一次数组下标访问。

### 2.3 InternalThreadLocalMap 的存储结构

`InternalThreadLocalMap` 是 FastThreadLocal 的核心存储，它包含一个 `Object[] indexedVariables` 数组，初始大小为 32，所有槽位初始化为 `UNSET` 哨兵对象：

```java
private static final int INDEXED_VARIABLE_TABLE_INITIAL_SIZE = 32;
public static final Object UNSET = new Object();

private static Object[] newIndexedVariableTable() {
    Object[] array = new Object[INDEXED_VARIABLE_TABLE_INITIAL_SIZE];
    Arrays.fill(array, UNSET);
    return array;
}
```

当 index 超出当前数组长度时，按 2 的幂扩容：

```java
private void expandIndexedVariableTableAndSet(int index, Object value) {
    Object[] oldArray = indexedVariables;
    final int oldCapacity = oldArray.length;
    int newCapacity;
    if (index < ARRAY_LIST_CAPACITY_EXPAND_THRESHOLD) {
        newCapacity = index;
        newCapacity |= newCapacity >>> 1;
        newCapacity |= newCapacity >>> 2;
        newCapacity |= newCapacity >>> 4;
        newCapacity |= newCapacity >>> 8;
        newCapacity |= newCapacity >>> 16;
        newCapacity++;
    } else {
        newCapacity = ARRAY_LIST_CAPACITY_MAX_SIZE;
    }
    Object[] newArray = Arrays.copyOf(oldArray, newCapacity);
    Arrays.fill(newArray, oldCapacity, newArray.length, UNSET);
    newArray[index] = value;
    indexedVariables = newArray;
}
```

这段位运算实现了"向上取整到 2 的幂"，与 HashMap 的容量计算方式完全一致。2 的幂大小保证了良好的缓存行对齐特性。

数组的 index 0 被保留给 `VARIABLES_TO_REMOVE_INDEX`，存放一个 `Set<FastThreadLocal<?>>`，记录当前线程绑定的所有 FastThreadLocal 实例。`removeAll()` 时遍历这个 Set 逐个清理，确保不会泄漏。

### 2.4 双路径获取：fastGet vs slowGet

`InternalThreadLocalMap.get()` 根据当前线程类型走不同路径：

```java
public static InternalThreadLocalMap get() {
    Thread thread = Thread.currentThread();
    if (thread instanceof FastThreadLocalThread) {
        return fastGet((FastThreadLocalThread) thread);
    } else {
        return slowGet();
    }
}

private static InternalThreadLocalMap fastGet(FastThreadLocalThread thread) {
    InternalThreadLocalMap threadLocalMap = thread.threadLocalMap();
    if (threadLocalMap == null) {
        thread.setThreadLocalMap(threadLocalMap = new InternalThreadLocalMap());
    }
    return threadLocalMap;
}

private static InternalThreadLocalMap slowGet() {
    InternalThreadLocalMap ret = slowThreadLocalMap.get();
    if (ret == null) {
        ret = new InternalThreadLocalMap();
        slowThreadLocalMap.set(ret);
    }
    return ret;
}
```

**快路径**：当前线程是 `FastThreadLocalThread` 时，`threadLocalMap` 是该线程对象上的一个普通实例字段，直接读取即可——这比 JDK ThreadLocal 的哈希查找快得多。`FastThreadLocalThread` 在 Netty 中由 `DefaultThreadFactory` 创建，EventLoop 线程天然就是这种类型。

**慢路径**：对于普通的 `java.lang.Thread`，降级到一个静态的 `ThreadLocal<InternalThreadLocalMap>` 来存储。虽然这条路径仍然要经过 JDK ThreadLocal 的哈希查找，但由于每个线程只需要查找一次（得到 InternalThreadLocalMap 后，后续所有 FastThreadLocal 的访问都在 Map 内部的数组上完成），所以哈希查找的成本被摊薄到了可忽略的程度。

### 2.5 FastThreadLocalRunnable 与生命周期管理

`FastThreadLocalThread` 的构造函数会自动将 Runnable 包装为 `FastThreadLocalRunnable`：

```java
public FastThreadLocalThread(Runnable target) {
    super(FastThreadLocalRunnable.wrap(target));
    cleanupFastThreadLocals = true;
}

// FastThreadLocalRunnable
public void run() {
    try {
        runnable.run();
    } finally {
        FastThreadLocal.removeAll();
    }
}
```

`finally` 块中的 `removeAll()` 确保线程结束时所有 FastThreadLocal 变量被清理，防止在线程池环境下出现变量泄漏。

### 2.6 FallbackThreadSet：虚拟线程支持

为了支持 Java 虚拟线程等无法继承 `FastThreadLocalThread` 的场景，Netty 引入了 `FallbackThreadSet` 机制。`runWithFastThreadLocal(Runnable)` 方法允许任何线程临时注册为"快路径"线程：

```java
public static void runWithFastThreadLocal(Runnable runnable) {
    long id = current.getId();
    fallbackThreads.updateAndGet(set -> set.add(id));
    try {
        runnable.run();
    } finally {
        fallbackThreads.getAndUpdate(set -> set.remove(id));
        FastThreadLocal.removeAll();
    }
}
```

`FallbackThreadSet` 是一个不可变的数据结构，内部使用 `LongLongHashMap` 存储线程 ID 的位图（每个 long 存储 64 个线程 ID 的状态），通过 `AtomicReference` 的 CAS 更新实现线程安全的无锁修改。`currentThreadWillCleanupFastThreadLocals()` 方法在检查完 `FastThreadLocalThread` 后，还会查询 `fallbackThreads` 中是否包含当前线程 ID，从而让 Recycler 等依赖此方法的组件也能在虚拟线程上使用池化优化。

---

## 三、MpscIntQueue：多生产者单消费者无锁队列

### 3.1 MPSC 语义为何适合 EventLoop

Netty 的 EventLoop 模型天然匹配 MPSC（Multi-Producer Single-Consumer）语义：多个业务线程可能同时向 EventLoop 提交任务（`execute(Runnable)`），而只有 EventLoop 线程自身会从 taskQueue 中取出并执行任务。这种"多写一读"的模式如果使用 `LinkedBlockingQueue` 等 JDK 并发队列，消费端会受到不必要的锁竞争（或 CAS 竞争）。MPSC 队列可以针对"单消费者"做大量优化——消费端不需要任何原子操作，只需要普通的读写。

### 3.2 Netty 自研的 MpscAtomicIntegerArrayQueue

Netty 实现了一个专门存储 `int` 值的 MPSC 无锁队列 `MpscIntQueue`，其核心实现类 `MpscAtomicIntegerArrayQueue` 直接继承自 `AtomicIntegerArray`——这是一个非常巧妙的设计，将环形缓冲区与原子操作融合到一个对象中，避免了额外的数组引用间接寻址：

```java
final class MpscAtomicIntegerArrayQueue extends AtomicIntegerArray implements MpscIntQueue {
    private final int mask;
    private final int emptyValue;
    private volatile long producerIndex;
    private volatile long producerLimit;
    private volatile long consumerIndex;
}
```

容量在构造时被规整为 2 的幂（`safeFindNextPositivePowerOfTwo`），`mask = length() - 1` 用于位运算取模。

**offer() —— 生产者端**的核心逻辑：

```java
public boolean offer(int value) {
    final int mask = this.mask;
    long producerLimit = this.producerLimit;
    long pIndex;
    do {
        pIndex = producerIndex;
        if (pIndex >= producerLimit) {
            final long cIndex = consumerIndex;
            producerLimit = cIndex + mask + 1;
            if (pIndex >= producerLimit) {
                return false;           // 队列满
            } else {
                PRODUCER_LIMIT.lazySet(this, producerLimit);
            }
        }
    } while (!PRODUCER_INDEX.compareAndSet(this, pIndex, pIndex + 1));
    final int offset = (int) (pIndex & mask);
    lazySet(offset, value);             // lazySet 写入元素
    return true;
}
```

这段代码有两个关键优化。第一是 **producerLimit 缓存**：生产者不是每次 offer 都去读 `consumerIndex`（这是一次 volatile 读，会触发 CPU 缓存失效），而是缓存一个 `producerLimit = consumerIndex + capacity`。只要 `producerIndex < producerLimit`，就可以确定队列未满，直接 CAS 抢占位置。只有当 `producerIndex >= producerLimit` 时才重新读取 `consumerIndex` 并更新缓存。在队列远未满的常见情况下，这将 volatile 读的频率从"每次 offer"降低到"每 capacity 次 offer"。第二是使用 `lazySet` 写入元素而非 `set`（volatile 写），避免了 StoreLoad 屏障——这是安全的，因为消费者端会通过其他机制确保可见性。

**poll() —— 消费者端**的核心逻辑：

```java
public int poll() {
    final long cIndex = consumerIndex;
    final int offset = (int) (cIndex & mask);
    int value = get(offset);
    if (emptyValue == value) {
        if (cIndex != producerIndex) {
            do {
                value = get(offset);
            } while (emptyValue == value);  // 自旋等待"飞行中"元素
        } else {
            return emptyValue;              // 队列确实为空
        }
    }
    lazySet(offset, emptyValue);
    CONSUMER_INDEX.lazySet(this, cIndex + 1);
    return value;
}
```

这里的"飞行中元素"（in-flight element）是 MPSC 队列的经典问题：生产者 P1 已经通过 CAS 抢到了 `producerIndex` 的位置，但还没来得及写入元素值（`lazySet(offset, value)` 尚未执行或尚未对消费者可见），此时消费者读到的是 `emptyValue`。但由于 `cIndex != producerIndex`，消费者知道一定有元素正在写入，所以自旋等待而不是返回空。这个自旋通常只需要几个 CPU 周期，因为生产者在 CAS 成功后紧接着就会执行写入。

消费者端全程使用 `lazySet` 更新 `consumerIndex`——由于只有一个消费者，不需要原子操作或内存屏障来保护写入。

### 3.3 JCTools MpscChunkedArrayQueue

对于通用的对象类型任务队列，Netty 通过 `PlatformDependent.newMpscQueue()` 适配 JCTools 库：

```java
static <T> Queue<T> newChunkedMpscQueue(final int chunkSize, final int capacity) {
    return USE_MPSC_CHUNKED_ARRAY_QUEUE
        ? new MpscChunkedArrayQueue<T>(chunkSize, capacity)
        : new MpscChunkedAtomicArrayQueue<T>(chunkSize, capacity);
}
```

当 Unsafe 可用时使用 `MpscChunkedArrayQueue`（通过 Unsafe 直接操作内存，避免数组边界检查），否则降级到 `MpscChunkedAtomicArrayQueue`（纯 Java 实现，使用 `AtomicReferenceArray`）。"Chunked" 意味着队列按 chunk 分段增长，而非一次分配全部容量，适合容量上限很大但实际使用量通常较小的场景（如 EventLoop 的 taskQueue，默认上限 `Integer.MAX_VALUE` 但通常只有几十个任务）。

---

## 四、Promise/Future 异步编程模型

### 4.1 JDK Future 的局限

JDK 的 `java.util.concurrent.Future` 提供的能力非常有限：`isDone()` 只能轮询，`get()` 只能阻塞等待，没有"完成时回调"的机制。在 Netty 这样的异步框架中，几乎所有 I/O 操作都是非阻塞的——`channel.write()` 不会等待数据真正发送完毕，`channel.connect()` 不会等待 TCP 三次握手完成。如果每次都阻塞等待结果，EventLoop 线程就会被挂起，整个事件循环停摆。因此 Netty 需要一个"完成时通知"的 Future 模型。

### 4.2 Netty 的改进：ChannelFuture 与 ChannelPromise

Netty 定义了自己的 `Future` 接口（继承 JDK Future），增加了 `addListener(GenericFutureListener)` 方法，支持注册完成回调。在此基础上，`Promise` 接口进一步增加了 `setSuccess()` / `setFailure()` 方法，使得 Future 变成了可写的——Promise 就是"可以被设置结果的 Future"。`ChannelFuture` 和 `ChannelPromise` 是 Channel I/O 操作专用的子类型，`DefaultChannelPromise` 重写了 `executor()` 方法使其默认返回 `channel().eventLoop()`，确保监听器回调在 Channel 所属的 EventLoop 线程上执行。

### 4.3 DefaultPromise 源码深度解析

`DefaultPromise` 是整个异步编程模型的核心实现，它的复杂度集中在以下几个精心设计的机制上。

**volatile result 状态编码**。`DefaultPromise` 使用一个 `volatile Object result` 字段编码所有状态，而不是用多个字段或枚举：

```java
private static final Object SUCCESS = new Object();
private static final Object UNCANCELLABLE = new Object();
private volatile Object result;
```

`result` 的取值与语义对应关系为：`null` 表示未完成，`UNCANCELLABLE` 表示未完成但已标记为不可取消，`SUCCESS` 表示成功完成且结果为 null，`CauseHolder` 包装对象表示失败（内含异常），其他任何非 null 值表示成功完成且结果为该值。这种"一个字段编码多种状态"的技巧在 Netty 中随处可见，它的优势在于：整个状态判断只需要一次 volatile 读。

**CAS 状态转换**。`setValue0()` 方法通过两次 CAS 完成状态设置：

```java
private boolean setValue0(Object objResult) {
    if (RESULT_UPDATER.compareAndSet(this, null, objResult) ||
        RESULT_UPDATER.compareAndSet(this, UNCANCELLABLE, objResult)) {
        if (checkNotifyWaiters()) {
            notifyListeners();
        }
        return true;
    }
    return false;
}
```

第一次 CAS 尝试 `null → objResult`（正常路径），第二次 CAS 尝试 `UNCANCELLABLE → objResult`（已标记不可取消的路径）。如果两次都失败，说明 Promise 已经完成（或已被取消），返回 false。注意这里使用的是 `AtomicReferenceFieldUpdater` 而非 `AtomicReference`——这是 Netty 一贯的内存优化策略（详见第六节）。

**监听器分级存储**。`DefaultPromise` 对监听器的存储做了精细的分级优化：

```java
private GenericFutureListener<? extends Future<?>> listener;     // 单个监听器
private DefaultFutureListeners listeners;                         // 多个监听器

private void addListener0(GenericFutureListener<? extends Future<? super V>> listener) {
    if (this.listener == null) {
        if (listeners == null) {
            this.listener = listener;           // 第一个：直接字段引用
        } else {
            listeners.add(listener);            // 已有多个：加入列表
        }
    } else {
        listeners = new DefaultFutureListeners(this.listener, listener);  // 第二个：升级
        this.listener = null;
    }
}
```

当只有一个监听器时，直接用 `listener` 字段引用，不创建任何容器对象。当添加第二个监听器时，才创建 `DefaultFutureListeners`（内部是一个 `GenericFutureListener[]` 数组，初始容量 2，按倍数扩容）。由于绝大多数 Promise 只会注册一个监听器，这种分级策略避免了大量不必要的数组分配。

**栈溢出保护**。当监听器 A 的回调中触发了另一个 Promise 的完成，而那个 Promise 的监听器 B 又触发了下一个 Promise……这种链式通知会导致调用栈无限增长，最终 `StackOverflowError`。`DefaultPromise` 通过 `InternalThreadLocalMap` 中的 `futureListenerStackDepth` 计数器来检测递归深度：

```java
private void notifyListeners() {
    EventExecutor executor = executor();
    if (executor.inEventLoop()) {
        final InternalThreadLocalMap threadLocals = InternalThreadLocalMap.get();
        final int stackDepth = threadLocals.futureListenerStackDepth();
        if (stackDepth < MAX_LISTENER_STACK_DEPTH) {      // 默认 8
            threadLocals.setFutureListenerStackDepth(stackDepth + 1);
            try {
                notifyListenersNow();
            } finally {
                threadLocals.setFutureListenerStackDepth(stackDepth);
            }
            return;
        }
    }
    safeExecute(executor, () -> notifyListenersNow());     // 深度超限，异步调度
}
```

当递归深度达到 `MAX_LISTENER_STACK_DEPTH`（默认 8）时，不再直接在当前调用栈上执行回调，而是将 `notifyListenersNow()` 包装成 Runnable 提交到 executor 异步执行，从而截断递归链条。

**无锁自旋通知**。`notifyListenersNow()` 的实现非常精巧——它通过 `notifyingListeners` 标志避免重复入栈，同时使用"锁内取出、锁外执行"的模式减少锁持有时间：

```java
private void notifyListenersNow() {
    GenericFutureListener listener;
    DefaultFutureListeners listeners;
    synchronized (this) {
        if (notifyingListeners || (listener == null && listeners == null)) {
            return;
        }
        notifyingListeners = true;
        // 取出监听器引用，清空字段
    }
    for (;;) {
        // 在 synchronized 外执行回调
        if (listener != null) { notifyListener0(this, listener); }
        else { notifyListeners0(listeners); }
        synchronized (this) {
            if (this.listener == null && this.listeners == null) {
                notifyingListeners = false;
                return;
            }
            // 有新增的监听器，继续循环
        }
    }
}
```

`synchronized` 块只用于安全地取出和设置监听器引用，实际的回调执行（`notifyListener0`）在锁外进行。循环结构确保在执行回调期间新添加的监听器也会被处理——每次循环回到 `synchronized` 块检查是否有新增，如果有则继续执行。

**await 优化**。`await0()` 包含多个精心设计的优化：

```java
private boolean await0(long timeoutNanos, boolean interruptable) throws InterruptedException {
    if (isDone()) { return true; }                          // 快路径
    if (timeoutNanos <= 0) { return isDone(); }
    if (interruptable && Thread.interrupted()) { throw new InterruptedException(toString()); }
    checkDeadLock();                                        // 防止 EventLoop 线程死锁
    final long startTime = System.nanoTime();               // 延迟调用 nanoTime
    synchronized (this) {
        // ... wait/notify 循环
    }
}
```

首先是 `isDone()` 的快路径检查——如果 Promise 已经完成，直接返回，不需要进入 `synchronized` 块。然后是 `checkDeadLock()`：如果当前线程就是 Promise 绑定的 EventExecutor（即 EventLoop 线程），调用 `await()` 会造成死锁——因为 EventLoop 线程被阻塞后，没有其他线程会处理 I/O 事件来完成这个 Promise。Netty 在这里直接抛出 `BlockingOperationException`，将这种隐蔽的死锁转化为一个明确的异常。`System.nanoTime()` 的调用被延迟到检查之后——因为 `nanoTime()` 涉及系统调用，如果前面的检查就能短路返回，就不需要付出这个成本。

`waiters` 字段使用 `short` 类型而非 `int`，节省 2 字节内存。虽然单个对象节省的内存微不足道，但 Netty 中可能同时存在数十万个 Promise 对象（每个 I/O 操作都会创建一个），累积效果可观。

---

## 五、AtomicIntegerFieldUpdater 的系统性应用

### 5.1 为什么不用 AtomicInteger

在 Netty 源码中，几乎看不到直接使用 `AtomicInteger` / `AtomicLong` / `AtomicReference` 作为实例字段的场景，取而代之的是大量的 `AtomicIntegerFieldUpdater` / `AtomicLongFieldUpdater` / `AtomicReferenceFieldUpdater` 搭配 `volatile` 字段。

原因在于内存布局。一个 `AtomicInteger` 对象包含 12 字节的对象头（Mark Word + Klass Pointer，开启压缩指针时）加上 4 字节的 int 值，共计 16 字节（按 8 字节对齐）。而使用 `volatile int` 字段，只占用 4 字节——内存开销减少 75%。`AtomicIntegerFieldUpdater` 是一个静态共享的单例，通过反射获取字段偏移量后，使用 `Unsafe.compareAndSwapInt()` 直接对目标对象的字段执行 CAS 操作，功能完全等价于 `AtomicInteger`。

对于像 Recycler Handle、Promise、HashedWheelTimeout 这样可能存在数十万个实例的类，每个实例节省 12 字节意味着总共节省数 MB 内存。在 GC 敏感的场景下，更少的对象引用也意味着更短的 GC 扫描时间。

### 5.2 七个核心使用场景

**场景一：Recycler.DefaultHandle 的 STATE_UPDATER**。控制 handle 在 `STATE_CLAIMED`（0）和 `STATE_AVAILABLE`（1）之间的状态转换，防止同一对象被回收两次。`claim()` 时使用 `lazySet`（单线程读取，不需要完整屏障），`toAvailable()` 时使用 `getAndSet`（可能跨线程调用，需要原子性保证）。

**场景二：HashedWheelTimer 的 WORKER_STATE_UPDATER 与 Timeout.STATE_UPDATER**。`WORKER_STATE_UPDATER` 管理定时器工作线程的生命周期状态（INIT → STARTED → SHUTDOWN），在 `start()` 方法中通过 CAS 确保只有一个线程能启动 Worker。`HashedWheelTimeout.STATE_UPDATER` 管理定时任务的状态（INIT → CANCELLED 或 INIT → EXPIRED），`cancel()` 方法通过 CAS 实现无锁取消。

**场景三：SingleThreadEventExecutor 的 STATE_UPDATER**。管理 EventLoop 线程的五种状态（ST_NOT_STARTED → ST_STARTED → ST_SHUTTING_DOWN → ST_SHUTDOWN → ST_TERMINATED），状态转换通过 CAS 保证线程安全，例如 `shutdownGracefully()` 中从 ST_STARTED 到 ST_SHUTTING_DOWN 的转换。

**场景四：ChannelOutboundBuffer 的 UNWRITABLE_UPDATER**。使用一个 `int` 字段的各个 bit 位来标记不同原因的不可写状态（bit 0 用于水位线标记，其他 bit 用于用户自定义标记）。状态变更通过 CAS + 位运算实现无锁更新，当 `unwritable` 从 0 变为非 0（或从非 0 变为 0）时触发 `channelWritabilityChanged` 事件。

**场景五：AbstractChannelHandlerContext 的 HANDLER_STATE_UPDATER**。管理 Handler 在 Pipeline 中的生命周期状态（INIT → ADD_PENDING → ADD_COMPLETE → REMOVE_COMPLETE），确保 `handlerAdded()` 和 `handlerRemoved()` 回调的正确触发。`setAddComplete()` 中使用 CAS 防止在 REMOVE_COMPLETE 状态下误设状态。

**场景六：RefCnt 的 AtomicRefCnt.UPDATER**。引用计数管理使用了一个精巧的"偶数编码"策略：`volatile int value` 存储的是实际引用计数的 2 倍（偶数），当引用计数归零时设为 1（奇数）。`retain()` 通过 `getAndAdd(2)` 增加引用计数，`release()` 通过 CAS 循环减少引用计数。偶/奇校验用于检测"对已释放对象再次 release"的错误。在支持 Unsafe 的平台上，RefCnt 会优先使用 `Unsafe` 直接操作字段偏移量（`UnsafeRefCnt`），次选 `VarHandle`（`VarHandleRefCnt`），最后才降级到 `AtomicIntegerFieldUpdater`（`AtomicRefCnt`）。

**场景七：DefaultPromise 的 RESULT_UPDATER**。这里使用的是 `AtomicReferenceFieldUpdater` 而非 `AtomicIntegerFieldUpdater`，但设计思想一致。通过对 `volatile Object result` 字段的 CAS 操作实现状态转换（`null → result`、`UNCANCELLABLE → result`、`null → CANCELLATION_CAUSE_HOLDER`），在 `setValue0()` 和 `cancel()` 中保证多线程竞争下只有一个线程能成功设置结果。

---

## 六、缓存行填充（Cache Line Padding）与伪共享（False Sharing）

在前面五个章节中，我们多次提到了"减少跨核 volatile 读频率"、"lazySet 避免 StoreLoad 屏障"、"producerLimit 缓存减少缓存一致性流量"等优化手段，但一直没有触及这些优化背后更底层的硬件原理——**CPU 缓存行**和**伪共享问题**。这个话题是理解所有无锁并发数据结构设计的基石，也是 Netty 的 MpscIntQueue、JCTools 队列、以及 LMAX Disruptor 之所以"系出同源"的技术根基。

### 6.1 CPU 缓存体系：为什么需要关心缓存行

现代 CPU 的存储体系是一个多级金字塔：寄存器（~0.3ns） → L1 Cache（~1ns） → L2 Cache（~3-5ns） → L3 Cache（~10-20ns） → 主存（~60-100ns）。每上升一级，容量增大但延迟也急剧增长——主存的访问延迟是 L1 的 60-100 倍。CPU 绝大多数时间都在和 L1/L2 Cache 打交道，只有 Cache Miss 时才会降级到更慢的层级。

关键在于：**CPU 缓存的最小读写单位不是 1 字节，而是一个"缓存行"（Cache Line）**。在 x86/ARM64 架构上，一个缓存行通常是 **64 字节**。当 CPU 需要读取某个内存地址的数据时，它不会只读取那 4 字节或 8 字节，而是把该地址所在的整个 64 字节缓存行一次性加载到 Cache 中。写入也一样——修改一个字节，就会标记整个 64 字节缓存行为"脏"。

这意味着：如果两个在逻辑上毫无关系的变量，恰好落在了同一个 64 字节的缓存行中，那么当一个 CPU 核心修改其中一个变量时，另一个 CPU 核心上缓存的那个缓存行会被整体失效——即使另一个核心关心的是同一缓存行中的另一个变量。这就是**伪共享（False Sharing）**。

### 6.2 伪共享的产生机制：MESI 协议详解

要理解伪共享的代价，需要了解多核 CPU 的缓存一致性协议——MESI（Modified, Exclusive, Shared, Invalid）。每个缓存行在每个 CPU 核心中都处于以下四种状态之一：

**M（Modified）**：当前核心修改过这个缓存行，且尚未写回主存。此时只有当前核心有最新数据，其他核心的副本都已失效。

**E（Exclusive）**：当前核心独占这个缓存行，数据与主存一致，但其他核心没有缓存这一行。当前核心可以直接修改而不需要通知其他核心（修改后变为 M 状态）。

**S（Shared）**：多个核心同时缓存了这个缓存行，数据与主存一致。如果要修改，必须先通过总线广播一个"Invalidate"消息使其他核心的副本失效（即转为 I 状态），然后自己变为 M 状态。

**I（Invalid）**：这个缓存行的副本已无效。如果要读取，必须从其他核心（如果它们有 M 或 E 状态的副本）或主存重新加载。

现在考虑一个具体的伪共享场景。假设有两个 volatile long 变量 `producerIndex` 和 `consumerIndex`，它们在内存中紧挨着，落在同一个 64 字节的缓存行中。生产者线程（Core 0）频繁更新 `producerIndex`，消费者线程（Core 1）频繁更新 `consumerIndex`：

```
步骤 1：Core 0 写入 producerIndex
  - Core 0 发送 Invalidate 消息，将 Core 1 的缓存行副本标记为 I
  - Core 0 的缓存行变为 M 状态
  - 代价：总线消息 + Core 1 缓存失效

步骤 2：Core 1 读取 consumerIndex（需要写入前先读取）
  - Core 1 发现缓存行已是 I 状态，必须发送 Read 请求
  - Core 0 收到请求，将 M 状态的缓存行写回主存并降级为 S
  - Core 1 从主存/Core 0 加载整个缓存行，标记为 S
  - 代价：总线消息 + 主存访问（~60-100ns）

步骤 3：Core 1 写入 consumerIndex
  - Core 1 发送 Invalidate 消息，将 Core 0 的缓存行副本标记为 I
  - Core 1 的缓存行变为 M 状态
  - 代价：总线消息 + Core 0 缓存失效

步骤 4：Core 0 读取 producerIndex（需要写入前先读取）
  - 又回到步骤 1 的情况……
```

这个过程形成了一个"乒乓效应"——两个核心不断地把同一个缓存行在自己和对方之间搬来搬去，每次搬运都需要 ~60-100ns 的总线通信开销。注意，`producerIndex` 和 `consumerIndex` 在逻辑上毫无关系——生产者根本不关心消费者的写入，消费者也不关心生产者的写入——但因为它们共享了同一个缓存行，两者的写操作互相干扰。这就是伪共享的名字由来：**看起来在"共享"数据，实际上只是在"共享"缓存行**。

### 6.3 伪共享的性能代价：量化分析

伪共享的性能影响可以非常严重。在一个典型的 x86 多核处理器上：

L1 Cache 命中的延迟大约 1ns（~4 个时钟周期）。当发生伪共享导致缓存行失效时，需要通过总线协议从其他核心获取最新数据，延迟上升到 ~40-60ns（跨核同 socket）甚至 ~100-200ns（跨 socket/NUMA 节点）。这意味着一次本来只需要 1ns 的内存访问，因为伪共享变成了 40-200ns——**慢了 40-200 倍**。

在高吞吐的无锁队列场景中，生产者和消费者每秒各自可能执行数百万次索引更新。如果每次更新都触发缓存行失效，整体吞吐量可能下降 10-50 倍。LMAX Disruptor 的论文中给出的测试数据显示，在消除伪共享后，吞吐量从约 2200 万 ops/s 提升到约 5.85 亿 ops/s——**提升了 26 倍**。

### 6.4 缓存行填充：解决伪共享的经典方法

解决伪共享的核心思路非常简单：**在热点变量两侧填充足够的无用字节，确保它独占一个完整的缓存行**。这样无论其他变量怎么排列，热点变量都不会与任何其他变量共享缓存行。

在 Java 中，一个 `long` 占 8 字节，一个缓存行是 64 字节。如果我们要保护一个 `long` 类型的热点变量，需要在它前后各填充 56 字节（7 个 `long`）的 padding。但实际操作时需要考虑对象头的影响——Java 对象在堆中的布局包含 12 字节的对象头（开启压缩指针时），之后才是实例字段。所以通常的做法是：

```java
// 方法一：手工填充（Java 8 之前的经典做法）
abstract class ProducerIndexPadding {
    // 前置填充：7 个 long = 56 字节
    long p1, p2, p3, p4, p5, p6, p7;
}

abstract class ProducerIndexValue extends ProducerIndexPadding {
    volatile long producerIndex;  // 真正的热点变量
}

abstract class ProducerIndexPostPadding extends ProducerIndexValue {
    // 后置填充：7 个 long = 56 字节
    long p8, p9, p10, p11, p12, p13, p14;
}

abstract class ConsumerIndexPadding extends ProducerIndexPostPadding {
    // 消费者索引的前置填充：7 个 long = 56 字节
    long p15, p16, p17, p18, p19, p20, p21;
}

abstract class ConsumerIndexValue extends ConsumerIndexPadding {
    volatile long consumerIndex;  // 真正的热点变量
}

abstract class ConsumerIndexPostPadding extends ConsumerIndexValue {
    // 消费者索引的后置填充：7 个 long = 56 字节
    long p22, p23, p24, p25, p26, p27, p28;
}
```

这种"继承链式 padding"的写法看起来非常奇怪，但它利用了 JVM 的对象布局规则：**父类的字段在内存中排在子类字段之前，同一类中的字段按声明顺序排列**。通过继承链，我们可以精确控制字段的内存布局顺序，确保填充字段紧贴在热点变量两侧。

为什么用继承而不是直接在一个类中写所有字段？因为 JVM 有权对同一类中的字段进行重排（按类型大小排序以减少内存对齐浪费），但**不会跨继承层级重排字段**。继承链保证了字段的相对顺序不会被 JVM 打乱。

### 6.5 JDK 8+ 的 @Contended 注解

手工填充虽然有效，但代码丑陋且容易出错（不同 JVM 实现的对象头大小、字段对齐规则可能不同）。JDK 8 引入了 `@sun.misc.Contended`（JDK 9+ 改为 `@jdk.internal.vm.annotation.Contended`）注解，让 JVM 自动为标记字段添加缓存行填充：

```java
// 方法二：@Contended 注解（JDK 8+）
public class MyQueue {
    @Contended
    volatile long producerIndex;
    
    @Contended
    volatile long consumerIndex;
}
```

使用 `@Contended` 注解时，JVM 会在被标注的字段两侧各填充 128 字节（不是 64 字节！因为 Intel 的 CPU 预取器可能会以 128 字节为单位预取两个相邻缓存行）。

但 `@Contended` 有一个重要限制：**它默认只对 JDK 内部类生效，用户类必须在 JVM 启动参数中添加 `-XX:-RestrictContended` 才能生效**。这个限制的原因是缓存行填充会显著增加对象的内存占用（一个字段就多出 256 字节），如果滥用会导致内存浪费。JDK 自身的一些类使用了 `@Contended`，例如：

```java
// java.lang.Thread 中的 @Contended 使用
public class Thread implements Runnable {
    // ThreadLocalRandom 的种子，每个线程独立更新
    @Contended("tlr")
    long threadLocalRandomSeed;
    
    @Contended("tlr")
    int threadLocalRandomProbe;
    
    @Contended("tlr")
    int threadLocalRandomSecondarySeed;
}
```

`@Contended` 还支持分组：相同组名的字段会被放在同一个缓存行中（它们之间可以共享缓存行，因为它们通常由同一个线程访问），不同组的字段则被隔离到不同缓存行。上面 Thread 中的三个 `"tlr"` 字段就会被放在同一个缓存行中，与 Thread 的其他字段隔离。

### 6.6 JCTools 中的缓存行填充：MpscArrayQueue 的继承链

JCTools（Java Concurrency Tools）是 Netty 使用的无锁队列库，它的缓存行填充实现是业界最经典的范例。以 `MpscArrayQueue` 为例，它的继承链设计如下：

```java
// 第 1 层：数组引用（所有队列共用）
abstract class ConcurrentCircularArrayQueueL0Pad<E> extends AbstractQueue<E> {
    byte b000,b001,b002,b003,b004,b005,b006,b007;  // 8 bytes
    byte b008,b009,b010,b011,b012,b013,b014,b015;  // 8 bytes
    byte b016,b017,b018,b019,b020,b021,b022,b023;  // 8 bytes
    byte b024,b025,b026,b027,b028,b029,b030,b031;  // 8 bytes
    byte b032,b033,b034,b035,b036,b037,b038,b039;  // 8 bytes
    byte b040,b041,b042,b043,b044,b045,b046,b047;  // 8 bytes
    byte b048,b049,b050,b051,b052,b053,b054,b055;  // 8 bytes
    byte b056,b057,b058,b059,b060,b061,b062,b063;  // 8 bytes
    byte b064,b065,b066,b067,b068,b069,b070,b071;  // 8 bytes
    byte b072,b073,b074,b075,b076,b077,b078,b079;  // 8 bytes
    byte b080,b081,b082,b083,b084,b085,b086,b087;  // 8 bytes
    byte b088,b089,b090,b091,b092,b093,b094,b095;  // 8 bytes
    byte b096,b097,b098,b099,b100,b101,b102,b103;  // 8 bytes
    byte b104,b105,b106,b107,b108,b109,b110,b111;  // 8 bytes
    byte b112,b113,b114,b115,b116,b117,b118,b119;  // 8 bytes
    byte b120,b121,b122,b123,b124,b125,b126,b127;  // 8 bytes
    // 总共 128 字节 padding
}

// 第 2 层：数组引用和 mask
abstract class ConcurrentCircularArrayQueue<E> extends ConcurrentCircularArrayQueueL0Pad<E> {
    protected long mask;
    protected E[] buffer;   // 环形缓冲区数组引用
}

// 第 3 层：生产者索引的前置 padding
abstract class MpscArrayQueueMidPad<E> extends MpscArrayQueueProducerIndexField<E> {
    // 128 bytes padding...
}

// 第 4 层：producerLimit（缓存的消费者限制）
abstract class MpscArrayQueueProducerLimitField<E> extends MpscArrayQueueMidPad<E> {
    private volatile long producerLimit;
}

// 第 5 层：producerLimit 的后置 padding + consumerIndex 的前置 padding
abstract class MpscArrayQueueL2Pad<E> extends MpscArrayQueueProducerLimitField<E> {
    // 128 bytes padding...
}

// 第 6 层：consumerIndex
abstract class MpscArrayQueueConsumerIndexField<E> extends MpscArrayQueueL2Pad<E> {
    private volatile long consumerIndex;
}

// 第 7 层：consumerIndex 的后置 padding
abstract class MpscArrayQueueL3Pad<E> extends MpscArrayQueueConsumerIndexField<E> {
    // 128 bytes padding...
}

// 最终类
public class MpscArrayQueue<E> extends MpscArrayQueueL3Pad<E> {
    // 队列的公共 API
}
```

这个继承链的内存布局效果如下：

```
对象内存布局（从低地址到高地址）：

[对象头]             12 bytes (压缩指针)
[L0Pad padding]      128 bytes  ← 隔离对象头
[mask + buffer]      8 + 8 = 16 bytes
[padding]            128 bytes  ← 隔离 buffer 和 producerIndex
[producerIndex]      8 bytes    ← 生产者热点字段，独占缓存行
[MidPad padding]     128 bytes  ← 隔离 producerIndex 和 producerLimit
[producerLimit]      8 bytes    ← 生产者热点字段，独占缓存行
[L2Pad padding]      128 bytes  ← 隔离 producerLimit 和 consumerIndex
[consumerIndex]      8 bytes    ← 消费者热点字段，独占缓存行
[L3Pad padding]      128 bytes  ← 隔离 consumerIndex 和后续对象
```

之所以使用 128 字节而非 64 字节的填充，是因为 Intel CPU 的**相邻缓存行预取（Adjacent Cache Line Prefetch）**特性：当 CPU 加载一个 64 字节缓存行时，硬件预取器可能会自动将相邻的下一个缓存行也加载到 Cache 中。128 字节的填充确保即使预取也不会把两个热点变量拉到同一组被预取的缓存行中。

### 6.7 Netty MpscAtomicIntegerArrayQueue 的伪共享处理

回过头来看第三章分析的 Netty 自研 `MpscAtomicIntegerArrayQueue`：

```java
final class MpscAtomicIntegerArrayQueue extends AtomicIntegerArray implements MpscIntQueue {
    private final int mask;
    private final int emptyValue;
    private volatile long producerIndex;
    private volatile long producerLimit;
    private volatile long consumerIndex;
}
```

细心的读者会发现：这里的 `producerIndex`、`producerLimit`、`consumerIndex` 三个 volatile long 字段**紧挨在一起**，没有任何缓存行填充！按照前面的分析，这三个字段很可能落在同一个或相邻的缓存行中，存在严重的伪共享风险。

这是 Netty 有意为之的权衡。`MpscAtomicIntegerArrayQueue` 是一个面向特定场景的轻量级实现——它用于 Recycler 内部存储 `int` 类型的 handle 标识，队列容量通常很小（默认 256），竞争强度也相对较低。在这个场景下，Netty 团队判断：添加 128 字节 × 3 = 384 字节的填充会使每个 Recycler 实例的内存占用增加数倍（一个小的环形缓冲区本身可能只有 1-2KB），而性能收益在低竞争场景下并不显著。这是一个典型的"内存 vs 性能"的 trade-off，Netty 选择了节省内存。

相比之下，当 Netty 需要高性能的通用对象队列时（如 EventLoop 的 taskQueue），它会使用 JCTools 的 `MpscChunkedArrayQueue`——后者做了完整的缓存行填充。

### 6.8 producerLimit 缓存：另一种避免伪共享代价的方法

除了物理隔离（填充），还有一种避免伪共享代价的方法：**减少跨核访问的频率**。`producerLimit` 就是这种思路的体现。

在最朴素的 MPSC 队列设计中，每次 `offer()` 都需要读取 `consumerIndex` 来判断队列是否已满。`consumerIndex` 由消费者线程更新，因此每次读取都是一次跨核的 volatile 读——触发缓存一致性协议，如果存在伪共享则代价更大。

`producerLimit` 的引入将这个跨核读取的频率从"每次 offer"降低到"每 capacity 次 offer"：

```java
// 不用 producerLimit 的朴素实现——每次 offer 都读 consumerIndex
public boolean naiveOffer(int value) {
    long pIndex = producerIndex;
    long cIndex = consumerIndex;      // ← 每次都跨核读取！伪共享代价 ×N
    if (pIndex - cIndex >= capacity) {
        return false;                 // 队列满
    }
    // ...
}

// 使用 producerLimit 的优化实现
public boolean optimizedOffer(int value) {
    long pIndex = producerIndex;
    if (pIndex < producerLimit) {     // ← 本地读取 producerLimit，不跨核！
        // 快路径：不需要读 consumerIndex
    } else {
        long cIndex = consumerIndex;  // ← 只在 producerLimit 耗尽时才跨核读取
        producerLimit = cIndex + capacity;
    }
    // ...
}
```

在队列远未满的常见情况下（稳态下 `pIndex` 远小于 `producerLimit`），生产者只需要读取自己核心上缓存的 `producerLimit` 值，完全避免了跨核通信。只有当 `producerIndex` 追上了 `producerLimit` 时（意味着之前预留的空间用完了），才去读一次 `consumerIndex` 重新计算。这将跨核读取的频率降低了 `capacity` 倍。

这个技巧在 LMAX Disruptor 中同样存在——Disruptor 的 `cachedGatingSequence` 字段扮演的角色和 `producerLimit` 完全一致。可以说，producerLimit 缓存和缓存行填充是解决"生产者和消费者索引位于不同核心"问题的两种互补手段：缓存行填充降低了每次跨核通信的代价（避免无谓的缓存行失效），producerLimit 降低了跨核通信的频率（能不跨就不跨）。两者结合，效果最佳。

### 6.9 不同 Java 版本中缓存行填充的实践变迁

缓存行填充的实现手法随着 Java 版本的演进而变化：

**Java 6 及之前**：只能使用"声明无用 long 字段"的方式。但 JVM 的 JIT 编译器可能会将未读取的字段优化掉（Dead Code Elimination），导致填充失效。为了防止这种优化，有的项目会在某个不可能执行到的 if 分支中"假装"读取这些填充字段。

**Java 7**：JVM 对字段排序的规则更加明确，继承链式 padding 的可靠性提高。JCTools 和 Disruptor 都采用了这种方式，并且通过大量的基准测试验证了其有效性。

**Java 8**：引入 `@sun.misc.Contended` 注解，JVM 原生支持缓存行填充。但需要 `-XX:-RestrictContended` 参数才能对用户类生效，且对库作者来说不够可靠（无法控制用户的 JVM 参数）。JDK 内部类（如 `Thread`、`ForkJoinPool`、`Striped64`）广泛使用此注解。

**Java 9+**：`@Contended` 移到 `jdk.internal.vm.annotation` 包，外部访问更加困难。JCTools 继续使用继承链式 padding，这种方式在所有 JVM 实现上都可靠工作。

**当前最佳实践**：库作者（如 JCTools、Netty）使用继承链式 padding 保证跨 JVM 兼容性；JDK 内部和已知部署环境固定的应用使用 `@Contended` 简化代码。Netty 自身在 `MpscAtomicIntegerArrayQueue` 上选择了不做 padding 以节省内存（适用于低竞争场景），在高竞争场景下则委托给做了完整 padding 的 JCTools 队列。

### 6.10 如何验证伪共享是否存在：perf c2c 与 JMH

在实际工作中，可以通过以下工具来检测和验证伪共享：

**Linux perf c2c**（cache-to-cache）：这是专门用于检测伪共享的 Linux 性能工具。

```bash
# 采集缓存一致性事件
perf c2c record -g -- java -jar your-app.jar

# 分析报告
perf c2c report --stdio
```

输出会展示哪些缓存行上发生了频繁的跨核无效化（Invalidation），以及涉及的代码行和数据地址。如果看到两个不同线程访问的变量落在同一个缓存行上，且 "HITM"（Hit Modified，即"读到了其他核心修改过的缓存行"）计数很高，就是伪共享的证据。

**JMH（Java Microbenchmark Harness）**：可以编写对比基准测试来量化伪共享的影响。

```java
@State(Scope.Group)
public class FalseSharingBenchmark {
    
    // 伪共享版本：两个 volatile long 紧挨着
    volatile long value1;
    volatile long value2;
    
    @Benchmark
    @Group("falseSharing")
    @GroupThreads(1)
    public long writer1() { return ++value1; }
    
    @Benchmark
    @Group("falseSharing")
    @GroupThreads(1)
    public long writer2() { return ++value2; }
}

@State(Scope.Group)
public class NoPaddingBenchmark {
    
    // 消除伪共享版本：填充 7 个 long
    volatile long value1;
    long p1, p2, p3, p4, p5, p6, p7;
    volatile long value2;
    
    @Benchmark
    @Group("noPadding")
    @GroupThreads(1)
    public long writer1() { return ++value1; }
    
    @Benchmark
    @Group("noPadding")
    @GroupThreads(1)
    public long writer2() { return ++value2; }
}
```

典型结果：消除伪共享后的版本吞吐量可能提升 2-10 倍（取决于硬件和竞争强度）。

**JOL（Java Object Layout）**：用于查看 Java 对象的实际内存布局，验证填充是否生效。

```bash
# 打印对象布局
java -jar jol-cli.jar internals org.jctools.queues.MpscArrayQueue
```

JOL 会显示每个字段在对象内存中的偏移量，可以直接验证 padding 字段是否真的将热点变量隔离到了不同的缓存行。

### 6.11 与 Disruptor 的技术同源关系

Netty 的 MpscIntQueue 和 LMAX Disruptor 的 RingBuffer 解决的是同一类问题——高吞吐的线程间数据传递，它们共享以下核心技术基因：

**环形缓冲区 + 序号递增**：两者都使用 2 的幂大小的数组作为环形缓冲区，通过单调递增的序号（producerIndex / consumerIndex 或 Sequence）配合位运算取模来定位槽位。

**生产者索引的 CAS 保护**：多生产者场景下，生产者通过 CAS 竞争递增 producerIndex。Disruptor 的 `MultiProducerSequencer.next()` 和 MpscIntQueue 的 `offer()` 中的 CAS 循环在结构上几乎一致。

**消费者端无原子操作**：由于是单消费者，`consumerIndex` 的更新只需要普通的 lazySet/ordered write，不需要 CAS。

**producerLimit / cachedGatingSequence 缓存**：两者都通过缓存一个"已知安全的上界"来减少对消费者索引的跨核读取。

**缓存行填充隔离热点变量**：Disruptor 的 `Sequence` 类中有完整的 padding，JCTools 的队列继承链中也有完整的 padding。Netty 的 MpscAtomicIntegerArrayQueue 出于轻量级考虑省略了 padding，但在使用 JCTools 时就自动获得了这些优化。

两者的主要差异在于：Disruptor 设计目标是"极致延迟"的金融交易系统，所以做了更极端的优化（如完全消除内存分配、预分配所有事件对象、支持批量消费等）；而 Netty 的 MpscQueue 设计目标是"足够好"的通用 IO 框架任务队列，在内存占用和性能之间取了更实用的平衡。

---

## 七、性能优化哲学总结

Netty 的并发工具体现了一套系统性的性能优化哲学，贯穿于每一个基础设施的设计中。

**避免分配，而非优化分配**。Recycler 的存在本身就说明了这一点：与其让 GC 高效回收对象，不如从一开始就不创建新对象。采样率控制进一步说明了"不是所有优化都需要做到极致"——只池化 1/8 的对象就足以显著降低 GC 压力，而过度池化反而会浪费内存。

**将通用问题特化**。JDK 的 ThreadLocal 需要处理所有线程类型和所有使用模式，所以不得不使用通用的哈希表。FastThreadLocal 将问题特化为"Netty 管理的线程 + 全局唯一 index"，就可以用数组直接索引替代哈希查找。MpscIntQueue 将问题特化为"多生产者单消费者 + int 类型"，消费端完全无锁。

**延迟成本直到真正需要**。DefaultPromise 的 `await0()` 将 `System.nanoTime()` 调用延迟到所有快路径检查之后；Recycler 只在 FastThreadLocalThread 上才启用池化；InternalThreadLocalMap 只在 index 超出数组长度时才扩容。

**用字段代替对象**。AtomicIntegerFieldUpdater 替代 AtomicInteger、`short waiters` 替代 `int waiters`、单个 `volatile Object result` 编码多种状态、`listener` 字段直接引用替代容器——这些看似微小的优化，在百万级对象规模下产生质变。

**放宽语义换取性能**。Recycler 中大量使用 `relaxedPoll()` / `relaxedOffer()` 替代标准的 `poll()` / `offer()`；MpscAtomicIntegerArrayQueue 使用 `lazySet` 替代 volatile 写来更新元素和消费者索引；RefCnt 的 `resetRefCnt()` 使用 release 语义而非 volatile 写。每一处放宽都经过仔细分析，确保在特定使用模式下不会产生正确性问题，同时减少了不必要的内存屏障指令。

---

## 本篇涉及的设计模式

| 设计模式 | 应用场景 | 说明 |
|---------|---------|------|
| 对象池模式（Object Pool） | Recycler / LocalPool | 通过池化复用高频创建的对象，减少 GC 压力 |
| 享元模式（Flyweight） | NOOP_HANDLE / SUCCESS / UNCANCELLABLE | 用共享的不可变哨兵对象代替重复创建 |
| 观察者模式（Observer） | DefaultPromise 的 Listener 机制 | 异步操作完成时通知所有已注册的监听器 |
| 策略模式（Strategy） | RefCnt 的三路实现（Unsafe / VarHandle / AtomicUpdater） | 根据平台能力选择最优的原子操作实现 |
| 空对象模式（Null Object） | NOOP_HANDLE / NOOP_LOCAL_POOL | 用"什么都不做"的对象替代 null 检查 |
| 模板方法模式（Template Method） | Recycler.newObject() / LocalPool.getWith() | 父类定义流程骨架，子类实现具体创建逻辑 |
| 装饰器模式（Decorator） | FastThreadLocalRunnable 包装 Runnable | 在原始任务前后添加线程局部变量清理逻辑 |

## 本篇涉及的高性能并发技术

| 技术 | 典型应用 | 核心收益 |
|------|---------|---------|
| AtomicIntegerFieldUpdater / AtomicReferenceFieldUpdater | 全部七大场景 | 零对象头开销的 CAS 操作，每实例节省 12+ 字节 |
| CAS 无锁编程 | MpscIntQueue.offer()、DefaultPromise.setValue0()、RefCnt.release() | 避免锁竞争，多线程下线性可伸缩 |
| lazySet / relaxedOffer / relaxedPoll | MpscAtomicIntegerArrayQueue、Recycler.LocalPool | 放宽内存屏障语义，减少 StoreLoad fence 开销 |
| volatile 单字段状态编码 | DefaultPromise.result、DefaultHandle.state | 一次 volatile 读判定所有状态，消除多字段一致性问题 |
| 线程局部存储（Thread-Local Storage） | FastThreadLocal + InternalThreadLocalMap | O(1) 数组直接索引替代哈希查找 |
| MPSC 无锁队列 | EventLoop taskQueue、Recycler pooledHandles | 消费端零原子操作，适配"多写一读"模式 |
| producerLimit 缓存 | MpscAtomicIntegerArrayQueue | 减少跨核 volatile 读频率，降低缓存一致性流量 |
| 批量操作（Batching） | Recycler batch[] 数组 | 将逐个操作聚合为批量操作，减少队列交互次数 |
| 采样率控制 | Recycler ratioInterval / ratioCounter | 控制池增长速度，平衡复用率与内存占用 |
| 偶数编码引用计数 | RefCnt（value 存 refCnt * 2） | 通过奇偶校验检测 use-after-free 错误 |
| 栈深度保护 | DefaultPromise.notifyListeners() + futureListenerStackDepth | 防止链式回调导致 StackOverflowError |
| 死锁检测 | DefaultPromise.checkDeadLock() | 在 EventLoop 线程上阻塞等待时立即抛异常 |
| 位图集合 | FallbackThreadSet（LongLongHashMap 存 64-bit 位图） | 高效存储和查询线程 ID 集合 |
| 缓存行填充（Cache Line Padding） | JCTools MpscArrayQueue 继承链式 128 字节 padding | 消除伪共享（False Sharing），避免多核间缓存行乒乓 |
| producerLimit 缓存 + 缓存行隔离 | MpscArrayQueue / Disruptor cachedGatingSequence | 减少跨核 volatile 读频率 × 消除伪共享代价，双重优化 |
| @Contended 注解 | JDK Thread.threadLocalRandomSeed / Striped64 | JVM 原生缓存行填充，自动插入 128 字节 padding |
