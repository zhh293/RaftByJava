# ConnectableFlux 与多播机制源码解析

> **Reactor Core 源码深度研究系列 · 第 12 篇**
> 本文从源码层面深度剖析 Reactor 中的多播（multicast）机制，涵盖 `ConnectableFlux`、`FluxPublish`、`FluxReplay`、`FluxRefCount` 和 `FluxAutoConnect` 五个核心类的实现原理，揭示"一份数据，多个消费者"的底层设计。

---

## 一、全局架构总览

```
                          ┌──────────────────────────────────────────┐
                          │          ConnectableFlux<T>              │
                          │   (abstract, extends Flux<T>)           │
                          │                                          │
                          │  connect(Consumer<Disposable>)  ← 抽象   │
                          │  autoConnect(n)   → FluxAutoConnect     │
                          │  refCount(n)      → FluxRefCount        │
                          └──────────┬───────────────┬───────────────┘
                                     │               │
                 ┌───────────────────┘               └───────────────────┐
                 ▼                                                       ▼
    ┌────────────────────────┐                          ┌────────────────────────────┐
    │    FluxPublish<T>      │                          │     FluxReplay<T>          │
    │  (publish() 操作符)     │                          │   (replay() 操作符)        │
    │                        │                          │                            │
    │  source: Flux<T>       │                          │  source: CorePublisher<T>  │
    │  prefetch: int         │                          │  history: int              │
    │  connection: volatile  │                          │  ttl: long                 │
    │  PublishSubscriber     │                          │  scheduler: Scheduler      │
    └────────┬───────────────┘                          │  connection: volatile      │
             │                                          │  ReplaySubscriber          │
             ▼                                          └────────┬───────────────────┘
    ┌────────────────────────┐                                   │
    │  PublishSubscriber<T>  │                                   ▼
    │  (InnerConsumer)       │                          ┌────────────────────────────┐
    │                        │                          │  ReplaySubscriber<T>       │
    │  subscribers:          │                          │  (InnerConsumer)           │
    │   PubSubInner<T>[]     │                          │                            │
    │  queue: Queue<T>       │                          │  buffer: ReplayBuffer<T>   │
    │  state: volatile long  │                          │  subscribers:              │
    └────────┬───────────────┘                          │   ReplaySubscription<T>[]  │
             │                                          └────────────────────────────┘
             ▼
    ┌────────────────────────┐          连接控制层
    │  PublishInner<T>       │     ┌─────────────────────┐   ┌────────────────────────┐
    │  (PubSubInner)         │     │ FluxAutoConnect<T>  │   │  FluxRefCount<T>       │
    │                        │     │                     │   │                        │
    │  actual: Subscriber    │     │ remaining: AtomicInt│   │  source: Connectable   │
    │  requested: long       │     │ cancelSupport       │   │  n: int (阈值)         │
    │  parent: PublishSub    │     └─────────────────────┘   │  connection: RefCount  │
    └────────────────────────┘                               │    Monitor             │
                                                             └────────────────────────┘
```

**核心调用时序图**：

```
    Subscriber-A      Subscriber-B     ConnectableFlux     PublishSubscriber     Source
        │                  │                  │                   │                │
        │── subscribe() ──▶│                  │                   │                │
        │                  │    subscribe()   │                   │                │
        │                  │────────────────▶ │                   │                │
        │                  │                  │   add(InnerA)     │                │
        │                  │                  │─────────────────▶ │                │
        │                  │                  │                   │                │
        │                  │── subscribe() ──▶│                   │                │
        │                  │                  │   add(InnerB)     │                │
        │                  │                  │─────────────────▶ │                │
        │                  │                  │                   │                │
        │                  │   connect()      │                   │                │
        │  ◀──────────────────────────────────│                   │                │
        │                  │                  │   tryConnect()    │                │
        │                  │                  │─────────────────▶ │                │
        │                  │                  │                   │── subscribe()─▶│
        │                  │                  │                   │                │
        │                  │                  │                   │◀── onNext(v) ──│
        │◀──────────── onNext(v) ─────────── │◀── drain() ───── │                │
        │                  │◀── onNext(v) ───│                   │                │
```

---

## 二、ConnectableFlux：多播的抽象基类

### 2.1 设计定位

`ConnectableFlux<T>` 是 Reactor 多播机制的抽象基类，继承自 `Flux<T>`。它的核心设计思想是：**将"订阅上游"和"开始推送数据"两个动作解耦**。在普通 `Flux` 中，`subscribe()` 同时完成这两件事；而在 `ConnectableFlux` 中，`subscribe()` 仅仅是注册下游消费者，真正触发上游数据流需要调用 `connect()`。

源码位置：`reactor/core/publisher/ConnectableFlux.java`

```java
public abstract class ConnectableFlux<T> extends Flux<T> {

    public abstract void connect(Consumer<? super Disposable> cancelSupport);

    public final Disposable connect() {
        final Disposable[] out = { null };
        connect(r -> out[0] = r);
        return out[0];
    }
}
```

`connect()` 的无参版本内部构造了一个单元素数组 `Disposable[] out`，通过 lambda 捕获返回值。这是一个典型的"通过回调捕获返回值"的模式——为什么不直接返回？因为抽象方法 `connect(Consumer)` 需要在连接过程中异步地提供 `Disposable`，而这个过程可能涉及 CAS 竞争。

### 2.2 为什么需要 ConnectableFlux？

**反例分析**：如果没有 `ConnectableFlux`，只用普通 `Flux`，每次 `subscribe()` 都会创建一个全新的上游订阅。假设我们有一个 HTTP 请求产生的 `Flux`：

```java
Flux<String> httpFlux = Flux.defer(() -> makeHttpRequest());
httpFlux.subscribe(subscriberA);  // 发起第1次 HTTP 请求
httpFlux.subscribe(subscriberB);  // 发起第2次 HTTP 请求 —— 重复了!
```

使用 `ConnectableFlux` 则只触发一次上游订阅：

```java
ConnectableFlux<String> shared = httpFlux.publish();
shared.subscribe(subscriberA);  // 仅注册，不触发
shared.subscribe(subscriberB);  // 仅注册，不触发
shared.connect();               // 此时才触发一次 HTTP 请求，数据广播给 A 和 B
```

### 2.3 连接控制方法

`ConnectableFlux` 提供了两种自动连接策略：

**autoConnect(n)**：达到 n 个订阅者后自动 `connect()`，但不会自动断开。

```java
public final Flux<T> autoConnect(int minSubscribers, Consumer<? super Disposable> cancelSupport) {
    if (minSubscribers == 0) {
        connect(cancelSupport);
        return this;
    }
    if(this instanceof Fuseable){
        return onAssembly(new FluxAutoConnectFuseable<>(this, minSubscribers, cancelSupport));
    }
    return onAssembly(new FluxAutoConnect<>(this, minSubscribers, cancelSupport));
}
```

当 `minSubscribers == 0` 时，直接立即连接，返回自身。这是一个很巧妙的短路优化。

**refCount(n)**：达到 n 个订阅者后自动 `connect()`，所有订阅者取消后自动 `disconnect()`。

```java
public final Flux<T> refCount(int minSubscribers) {
    return onAssembly(new FluxRefCount<>(this, minSubscribers));
}
```

还有一个带 `gracePeriod` 的版本，在所有订阅者取消后等待一段时间再断开，防止频繁的连接/断开抖动。

```java
public final Flux<T> refCount(int minSubscribers, Duration gracePeriod, Scheduler scheduler) {
    return onAssembly(new FluxRefCountGrace<>(this, minSubscribers, gracePeriod, scheduler));
}
```

---

## 三、FluxPublish：实时广播的核心实现

### 3.1 类结构与核心字段

`FluxPublish<T>` 是 `publish()` 操作符的实现，它将一个上游 `Flux` 的数据广播给多个下游 `Subscriber`。

源码位置：`reactor/core/publisher/FluxPublish.java`

```java
final class FluxPublish<T> extends ConnectableFlux<T> implements Scannable {
    final Flux<? extends T> source;
    final int prefetch;
    final Supplier<? extends Queue<T>> queueSupplier;
    final boolean resetUponSourceTermination;

    volatile @Nullable PublishSubscriber<T> connection;

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<FluxPublish, @Nullable PublishSubscriber> CONNECTION =
            AtomicReferenceFieldUpdater.newUpdater(FluxPublish.class,
                    PublishSubscriber.class, "connection");
}
```

关键字段解析：

- **`source`**：上游数据源，仅订阅一次。
- **`prefetch`**：预取数量，控制从上游一次请求多少数据。
- **`connection`**：volatile 字段，指向当前的 `PublishSubscriber` 实例。通过 `AtomicReferenceFieldUpdater` 实现无锁的 CAS 更新。
- **`resetUponSourceTermination`**：是否在上游终止后允许重新连接。这个标志决定了终止后是否清除 connection。

### 3.2 connect() 的 CAS 循环

```java
@Override
public void connect(Consumer<? super Disposable> cancelSupport) {
    boolean doConnect;
    PublishSubscriber<T> s;
    for (; ; ) {
        s = connection;
        if (s == null || s.isTerminated()) {
            PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);
            if (!CONNECTION.compareAndSet(this, s, u)) {
                continue;
            }
            s = u;
        }
        doConnect = s.tryConnect();
        break;
    }
    cancelSupport.accept(s);
    if (doConnect) {
        source.subscribe(s);
    }
}
```

这段代码有几个精妙之处：

1. **CAS 无限循环**：如果当前 `connection` 为 null 或已终止，创建新的 `PublishSubscriber`，然后用 CAS 尝试设置。如果 CAS 失败（说明有并发线程也在 connect），重新循环。
2. **`tryConnect()` 幂等性**：`tryConnect()` 通过状态位标记确保 `source.subscribe(s)` 只执行一次，即使多次调用 `connect()` 也不会重复订阅上游。
3. **先 `cancelSupport.accept(s)` 后 `source.subscribe(s)`**：这个顺序确保调用者在上游开始推送数据之前就拿到了 `Disposable`。

### 3.3 subscribe() — 下游注册

```java
@Override
public void subscribe(CoreSubscriber<? super T> actual) {
    PublishInner<T> inner = new PublishInner<>(actual);
    actual.onSubscribe(inner);
    for (; ; ) {
        if (inner.isCancelled()) {
            break;
        }
        PublishSubscriber<T> c = connection;
        if (c == null || (this.resetUponSourceTermination && c.isTerminated())) {
            PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);
            if (!CONNECTION.compareAndSet(this, c, u)) {
                continue;
            }
            c = u;
        }
        if (c.add(inner)) {
            if (inner.isCancelled()) {
                c.remove(inner);
            } else {
                inner.parent = c;
            }
            c.drainFromInner();
            break;
        }
        // ...
    }
}
```

注意 `subscribe()` 并不会触发 `source.subscribe()`——它只是将 `PublishInner` 添加到 `PublishSubscriber` 的订阅者数组中。这就是"冷启动"的关键：先注册，后连接。

### 3.4 PublishSubscriber：多播的心脏

`PublishSubscriber` 是 `FluxPublish` 的内部核心类，它同时实现了 `InnerConsumer`（消费上游数据）和 `Disposable`（支持断开连接）。

```java
static final class PublishSubscriber<T> implements InnerConsumer<T>, Disposable {
    final int prefetch;
    final FluxPublish<T> parent;
    Subscription s;
    volatile PubSubInner<T>[] subscribers;
    volatile long state;
    Queue<T> queue;
    int sourceMode;
    boolean done;
    volatile @Nullable Throwable error;
}
```

**状态位设计**——`state` 字段使用 long 的不同位来表示不同状态，这是一种高性能的状态机设计：

```java
static final long FINALIZED_FLAG =
    0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
static final long CANCELLED_FLAG =
    0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
static final long TERMINATED_FLAG =
    0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
static final long SUBSCRIPTION_SET_FLAG =
    0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
static final long CONNECTED_FLAG =
    0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
static final long WORK_IN_PROGRESS_MASK =
    0b0000_0000_0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111_1111_1111_1111L;
```

为什么用 long 位而不是多个 boolean？**因为需要原子地同时修改多个状态**。例如 `markTerminated()` 需要同时设置 `TERMINATED_FLAG` 并增加工作计数器，这用多个 boolean 字段做不到。一个 `AtomicLong` CAS 操作就能原子地完成所有状态变更。

### 3.5 drain() — request 聚合与数据广播

`drain()` 方法是数据分发的核心。它实现了一个关键策略：**取所有下游的最小 request 值作为本轮发送量**。

```java
void drain(long expectedState) {
    for (; ; ) {
        // ...
        PubSubInner<T>[] a = subscribers;
        if (a != CANCELLED && !empty) {
            long maxRequested = Long.MAX_VALUE;
            int len = a.length;
            int cancel = 0;

            for (PubSubInner<T> inner : a) {
                long r = inner.requested;
                if (r >= 0L) {
                    maxRequested = Math.min(maxRequested, r);
                } else { // Long.MIN_VALUE 表示已取消
                    cancel++;
                }
            }

            if (len == cancel) {
                // 所有下游都取消了，丢弃数据
                T v = q.poll();
                if (checkTerminated(d, v == null, v)) { return; }
                if (mode != Fuseable.SYNC) { s.request(1); }
                continue;
            }

            int e = 0;
            while (e < maxRequested && cancel != Integer.MIN_VALUE) {
                // ... poll from queue and broadcast
                for (PubSubInner<T> inner : a) {
                    inner.actual.onNext(v);
                    Operators.producedCancellable(PubSubInner.REQUESTED, inner, 1);
                }
                e++;
            }

            if (e != 0 && mode != Fuseable.SYNC) {
                s.request(e);
            }
        }
        // ...
    }
}
```

**为什么取最小值（min）而不是最大值（max）？**

如果取最大值，慢消费者的缓冲区会无限增长，最终导致 OOM。取最小值意味着"以最慢的消费者为准"，这是背压（backpressure）的正确语义。当然这也意味着一个慢消费者会拖慢所有消费者。如果去掉这个约束，就会违背 Reactive Streams 规范对背压的要求。

### 3.6 订阅者数组的 Copy-on-Write 策略

```java
boolean add(PublishInner<T> inner) {
    for (; ; ) {
        PubSubInner<T>[] a = subscribers;
        if (a == TERMINATED) {
            return false;
        }
        int n = a.length;
        PubSubInner<?>[] b = new PubSubInner[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = inner;
        if (SUBSCRIBERS.compareAndSet(this, a, b)) {
            return true;
        }
    }
}
```

这是经典的 COW（Copy-on-Write）无锁并发模式。为什么不用 `ConcurrentLinkedQueue` 或 `synchronized`？

- `ConcurrentLinkedQueue` 遍历时无法保证快照一致性，而 `drain()` 需要在一轮循环内看到一致的订阅者列表。
- `synchronized` 会阻塞其他操作（如 `drain()`、`remove()`），严重降低吞吐量。
- COW 数组的遍历是零开销的（直接 for-each），且数组的 volatile 读保证了可见性。订阅者变更（add/remove）相对于数据推送是低频操作，COW 的复制成本可以接受。

### 3.7 三个哨兵数组

```java
static final PubSubInner[] INIT       = new PublishInner[0];
static final PubSubInner[] CANCELLED  = new PublishInner[0];
static final PubSubInner[] TERMINATED = new PublishInner[0];
```

三个空数组实例，虽然内容相同但**引用不同**，用来表示三种不同状态。通过引用相等（`==`）判断当前状态，比使用 enum 字段更高效，因为它与 subscribers 数组复用了同一个 volatile 字段，减少了一次 volatile 读。

---

## 四、FluxReplay：带缓存的多播

### 4.1 设计定位

`FluxReplay<T>` 是 `replay()` 操作符的实现。与 `FluxPublish` 的区别在于：**新加入的 Subscriber 可以收到之前缓存的历史数据**。

源码位置：`reactor/core/publisher/FluxReplay.java`

```java
final class FluxReplay<T> extends ConnectableFlux<T> implements Scannable, Fuseable {
    final CorePublisher<T> source;
    final int              history;
    final long             ttl;
    final @Nullable Scheduler scheduler;
    volatile @Nullable ReplaySubscriber<T> connection;
}
```

- **`history`**：最多缓存多少个元素。
- **`ttl`**：元素存活时间（纳秒）。只有 `scheduler` 不为 null 时生效。
- **`scheduler`**：提供时钟，用于时间窗口的回放策略。

### 4.2 三种 ReplayBuffer 策略

`FluxReplay` 的核心在于 `ReplayBuffer<T>` 接口，它定义了缓存的存储和回放行为：

```java
interface ReplayBuffer<T> {
    void add(T value);
    void onError(Throwable ex);
    void onComplete();
    void replay(ReplaySubscription<T> rs);
    boolean isDone();
    T poll(ReplaySubscription<T> rs);
    int capacity();
    boolean isExpired();
}
```

`newState()` 方法根据参数选择不同的策略实现：

```java
ReplaySubscriber<T> newState() {
    if (scheduler != null) {
        return new ReplaySubscriber<>(new SizeAndTimeBoundReplayBuffer<>(history, ttl, scheduler), this, history);
    }
    if (history != Integer.MAX_VALUE) {
        return new ReplaySubscriber<>(new SizeBoundReplayBuffer<>(history), this, history);
    }
    return new ReplaySubscriber<>(new UnboundedReplayBuffer<>(Queues.SMALL_BUFFER_SIZE), this, Queues.SMALL_BUFFER_SIZE);
}
```

#### 策略一：UnboundedReplayBuffer — 无限缓存

```java
static final class UnboundedReplayBuffer<T> implements ReplayBuffer<T> {
    final int batchSize;
    volatile int size;
    final Object[] head;
    Object[] tail;
    int tailIndex;
    volatile boolean done;
}
```

内部用分段的 `Object[]` 数组链表实现（每段 `batchSize + 1` 个元素，最后一个元素指向下一段）。`capacity()` 返回 `Integer.MAX_VALUE`，`isExpired()` 永远返回 `false`。

**为什么用分段数组而不是 `ArrayList`？** 因为 `ArrayList` 在扩容时需要复制整个数组，而分段数组只需要分配新段并链接，扩容成本是 O(1) 而非 O(n)。

#### 策略二：SizeBoundReplayBuffer — 固定大小缓存

```java
static final class SizeBoundReplayBuffer<T> implements ReplayBuffer<T> {
    final int limit;
    volatile Node<T> head;
    Node<T> tail;
    int size;
    volatile boolean done;
}
```

使用单向链表（`Node<T> extends AtomicReference<Node<T>>`），当 `size == limit` 时，滑动 `head` 指针丢弃最旧的元素：

```java
public void add(T value) {
    final Node<T> tail = this.tail;
    final Node<T> n = new Node<>(tail.index + 1, value);
    tail.set(n);
    this.tail = n;
    int s = size;
    if (s == limit) {
        Node<T> afterHead = head.get();
        head = afterHead;
    } else {
        size = s + 1;
    }
}
```

#### 策略三：SizeAndTimeBoundReplayBuffer — 大小+时间双约束

```java
static final class SizeAndTimeBoundReplayBuffer<T> implements ReplayBuffer<T> {
    final int limit;
    final long maxAge;
    final Scheduler scheduler;
    volatile TimedNode<T> head;
    TimedNode<T> tail;
    int size;
    volatile long done = NOT_DONE;
}
```

`TimedNode` 在 `Node` 的基础上增加了 `time` 字段。`add()` 时同时检查大小和时间约束，清除过期节点：

```java
public void add(T value) {
    final TimedNode<T> valueNode = new TimedNode<>(tail.index + 1, value,
            scheduler.now(TimeUnit.NANOSECONDS));
    tail.set(valueNode);
    this.tail = valueNode;
    // 大小约束
    if (s == limit) { head = afterHead; }
    // 时间约束
    long limit = scheduler.now(TimeUnit.NANOSECONDS) - maxAge;
    // 遍历链表，清除过期节点
    // ...
}
```

`isExpired()` 方法用于检测整个缓冲区是否已过期：

```java
public boolean isExpired() {
    long done = this.done;
    return done != NOT_DONE && scheduler.now(TimeUnit.NANOSECONDS) - maxAge > done;
}
```

### 4.3 ReplaySubscriber 的 request 管理

与 `FluxPublish` 的 `PublishSubscriber` 不同，`ReplaySubscriber` 通过 `manageRequest()` 方法实现更精细的背压控制。它跟踪所有下游订阅者的消费进度（通过 `index()`），只有当所有活跃订阅者都推进到 `nextPrefetchIndex` 时，才向上游请求更多数据：

```java
void manageRequest(long currentState) {
    final Subscription p = this.s;
    for (; ; ) {
        int nextPrefetchIndex = this.nextPrefetchIndex;
        boolean shouldPrefetch;
        final ReplaySubscription<T>[] subscribers = this.subscribers;
        if (subscribers.length > 0) {
            shouldPrefetch = true;
            for (ReplaySubscription<T> rp : subscribers) {
                if (rp.index() < nextPrefetchIndex) {
                    shouldPrefetch = false;
                    break;
                }
            }
        } else {
            shouldPrefetch = this.produced >= nextPrefetchIndex;
        }
        if (shouldPrefetch) {
            final int limit = this.limit;
            this.nextPrefetchIndex = nextPrefetchIndex + limit;
            p.request(limit);
        }
        // ...
    }
}
```

---

## 五、FluxRefCount：引用计数的自动连接/断开

### 5.1 设计目标

`FluxRefCount<T>` 实现了"自动连接 + 自动断开"的生命周期管理：当订阅者数量达到 `n` 时自动 `connect()`，当所有订阅者都取消时自动 `disconnect()`。

源码位置：`reactor/core/publisher/FluxRefCount.java`

```java
final class FluxRefCount<T> extends Flux<T> implements Scannable, Fuseable {
    final ConnectableFlux<? extends T> source;
    final int n;
    @Nullable RefCountMonitor<T> connection;
}
```

### 5.2 subscribe() 中的引用计数

```java
@Override
public void subscribe(CoreSubscriber<? super T> actual) {
    RefCountMonitor<T> conn;
    RefCountInner<T> inner = new RefCountInner<>(actual);
    source.subscribe(inner);

    boolean connect = false;
    synchronized (this) {
        conn = connection;
        if (conn == null || conn.terminated) {
            conn = new RefCountMonitor<>(this);
            connection = conn;
        }
        long c = conn.subscribers;
        conn.subscribers = c + 1;
        if (!conn.connected && c + 1 == n) {
            connect = true;
            conn.connected = true;
        }
    }
    inner.setRefCountMonitor(conn);
    if (connect) {
        source.connect(conn);
    }
}
```

注意这里用了 `synchronized` 而非 CAS。为什么？因为 `RefCountMonitor` 的状态涉及多个字段（`subscribers`、`connected`、`terminated`）的联动修改，CAS 只能保证单个字段的原子性，而 `synchronized` 可以保证整组修改的原子性。

### 5.3 cancel() 的引用计数递减

```java
void cancel(RefCountMonitor rc) {
    Disposable dispose = null;
    synchronized (this) {
        if (rc.terminated) { return; }
        long c = rc.subscribers - 1;
        rc.subscribers = c;
        if (c != 0L || !rc.connected) { return; }
        if (rc == connection) {
            dispose = RefCountMonitor.DISCONNECT.getAndSet(rc, Disposables.disposed());
            connection = null;
        }
    }
    if (dispose != null) {
        dispose.dispose();
    }
}
```

当 `subscribers` 递减到 0 且已连接时，触发断开连接。`Disposables.disposed()` 是一个已处置的哨兵值，防止重复 dispose。

### 5.4 RefCountInner 的状态机

```java
static final int MONITOR_SET_FLAG = 0b0010_0000_0000_0000_0000_0000_0000_0000;
static final int TERMINATED_FLAG  = 0b0100_0000_0000_0000_0000_0000_0000_0000;
static final int CANCELLED_FLAG   = 0b1000_0000_0000_0000_0000_0000_0000_0000;
```

`RefCountInner` 有一个重要的时序问题：`onSubscribe()` 在 `setRefCountMonitor()` 之前调用（因为先订阅 source，再设置 monitor）。如果在这个窗口期内收到了 `onComplete/onError`，需要延迟到 `setRefCountMonitor()` 时再传递：

```java
void setRefCountMonitor(RefCountMonitor<T> connection) {
    this.connection = connection;
    this.actual.onSubscribe(this);
    for (;;) {
        int previousState = this.state;
        if (isCancelled(previousState)) { return; }
        if (isTerminated(previousState)) {
            connection.upstreamFinished();
            // 延迟传递终止信号
            Throwable e = this.error;
            if (e != null) { this.actual.onError(e); }
            else { this.actual.onComplete(); }
            return;
        }
        if (STATE.compareAndSet(this, previousState, previousState | MONITOR_SET_FLAG)) {
            return;
        }
    }
}
```

---

## 六、FluxAutoConnect：火即忘的自动连接

### 6.1 极简实现

`FluxAutoConnect` 的实现非常精练——整个类不到 80 行代码：

源码位置：`reactor/core/publisher/FluxAutoConnect.java`

```java
final class FluxAutoConnect<T> extends Flux<T> implements Scannable {
    final ConnectableFlux<? extends T> source;
    final Consumer<? super Disposable> cancelSupport;

    volatile int remaining;
    static final AtomicIntegerFieldUpdater<FluxAutoConnect> REMAINING =
            AtomicIntegerFieldUpdater.newUpdater(FluxAutoConnect.class, "remaining");

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        source.subscribe(actual);
        if (remaining > 0 && REMAINING.decrementAndGet(this) == 0) {
            source.connect(cancelSupport);
        }
    }
}
```

### 6.2 与 refCount 的关键区别

**FluxAutoConnect 只负责连接，不负责断开**。一旦连接建立，即使所有订阅者都取消了，连接也不会断开。这就是"fire-and-forget"语义。

为什么需要这种语义？在某些场景下，数据源是持续推送的（如 Kafka 消费），断开再重连的代价很高。此时用 `autoConnect()` 配合外部生命周期管理更合适。

另外注意 `remaining` 计数器是一次性的——递减到 0 后不会重置。**如果 `autoConnect(2)` 后先来了 3 个订阅者，不会触发第二次连接**。

---

## 七、反例总结：普通 Flux 为什么做不到多播

| 行为 | 普通 Flux | ConnectableFlux (publish) |
|------|----------|--------------------------|
| subscribe() 触发上游 | 是，每次都创建新的数据流 | 否，仅注册下游 |
| 多个 Subscriber 共享数据 | 不共享，各走各的 | 共享同一个数据流 |
| 背压语义 | 每个 Subscriber 独立背压 | 取所有下游的最小 request |
| 上游订阅次数 | 每次 subscribe 一次 | 只 subscribe 一次 |

---

## 八、归纳表格：多播策略对照表

| 特性 | FluxPublish | FluxReplay | 
|------|------------|------------|
| **操作符** | `publish()` | `replay()` / `replay(n)` / `replay(Duration)` |
| **缓存历史数据** | 不缓存 | 缓存（按大小/时间/无限） |
| **新 Subscriber 能否收到历史数据** | 不能 | 能 |
| **内部缓冲区** | Queue<T> (单一) | ReplayBuffer<T> (链表) |
| **背压策略** | 取所有下游最小 request | 跟踪每个下游的 index |
| **支持 Fuseable** | 是 (source mode) | 是 (ASYNC fusion) |

| 连接控制 | autoConnect(n) | refCount(n) | refCount(n, Duration) |
|---------|---------------|-------------|----------------------|
| **自动连接** | 达到 n 个订阅者 | 达到 n 个订阅者 | 达到 n 个订阅者 |
| **自动断开** | 不断开 | 所有订阅者取消时立即断开 | 所有订阅者取消后等 grace period |
| **重新连接** | 不支持 | 支持 | 支持 |
| **并发安全机制** | AtomicInteger CAS | synchronized + AtomicRef | scheduled task |
| **适用场景** | 长连接、外部管理生命周期 | 短连接、自管理生命周期 | 抖动场景、防止频繁重连 |

| 状态位设计 | FluxPublish.PublishSubscriber | FluxReplay.ReplaySubscriber | FluxRefCount.RefCountInner |
|-----------|-------------------------------|-----------------------------|----|
| **位宽** | long (64 bit) | long (64 bit) | int (32 bit) |
| **CONNECTED** | bit 58 | bit 60 | N/A |
| **TERMINATED** | bit 62 | N/A | bit 30 |
| **CANCELLED** | bit 61 | N/A | bit 31 |
| **WORK_IN_PROGRESS** | 低 32 位 | 低 60 位 | N/A |
| **SUBSCRIBED** | bit 59 | bit 61 | N/A |
| **MONITOR_SET** | N/A | N/A | bit 29 |

---

## 九、设计启示

1. **状态压缩到 long 位字段**：`PublishSubscriber` 用一个 `volatile long` 同时表达 5 种状态和工作计数器，只需一次 CAS 即可原子修改。这是高性能响应式编程的标准模式。

2. **COW 数组 vs synchronized**：`FluxPublish` 的订阅者管理用 COW + CAS，因为读多写少；`FluxRefCount` 用 synchronized，因为需要多字段联动。选择取决于读写比和原子性需求。

3. **哨兵对象区分状态**：`INIT`、`CANCELLED`、`TERMINATED` 三个空数组引用不同但内容相同，通过 `==` 比较取代 boolean 字段，减少 volatile 读次数。

4. **背压聚合取最小值**：这是多播场景下唯一正确的背压策略，但代价是慢消费者会拖慢整体。实际应用中通常配合 `onBackpressureBuffer/Drop` 使用。
