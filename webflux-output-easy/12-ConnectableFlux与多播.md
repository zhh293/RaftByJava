# ConnectableFlux 与多播机制（易懂版）

> **Reactor Core 源码解析系列 · 第 12 篇 · 易懂版**
> 用"广播电台"的类比，把 Reactor 的多播机制从头到尾讲清楚。

---

## 一、从一个真实的业务痛点说起

假设你在开发一个实时股价推送系统。后端通过 WebSocket 从交易所接收实时行情数据，前端有多个面板需要消费这些数据——K线图面板要画蜡烛图，盘口面板要显示买卖五档，成交明细面板要滚动展示最新成交。

如果用普通的 `Flux`，你会写出这样的代码：

```java
Flux<StockPrice> stockFlux = Flux.defer(() -> connectWebSocket());

// K线面板订阅
stockFlux.subscribe(kLinePanel::update);

// 盘口面板订阅
stockFlux.subscribe(orderBookPanel::update);

// 成交明细面板订阅
stockFlux.subscribe(tradePanel::update);
```

看起来没问题？运行一下你会发现：**WebSocket 被连接了三次**。每个 `subscribe()` 都创建了一条全新的数据通道，交易所那边收到了三个连接请求，数据被推了三遍。

这就好比你家里有三台收音机想听同一个电台节目，结果每打开一台收音机，电台就重新开播一次——节目被播了三遍，而且不同步。

**你需要的是"广播电台"模式**：电台只播一次，所有收音机调到同一个频道，同时收到同一条消息。

这就是 `ConnectableFlux` 要解决的问题。

---

## 二、普通 Flux vs ConnectableFlux：一对一电话 vs 广播电台

### 2.1 普通 Flux 是"一对一电话"

普通 `Flux` 的每次 `subscribe()` 都会从上游重新建立一条数据通道。你可以把它想象成"一对一电话"——每个订阅者都有一根专属的电话线，上游为每个订阅者单独服务一次。

```
普通 Flux 的行为：
                                    ┌── subscriberA ←── [HTTP请求 #1]
source(Flux) ── subscribe() × 3 ───┼── subscriberB ←── [HTTP请求 #2]
                                    └── subscriberC ←── [HTTP请求 #3]

结果：上游被执行了 3 次，数据流了 3 遍
```

### 2.2 ConnectableFlux 是"广播电台"

`ConnectableFlux` 把"注册订阅者"和"开始推送数据"拆成了两步：

- **第一步**：`subscribe()` 只是"调频道"——告诉电台"我要听这个节目"，但不让电台开始广播。
- **第二步**：`connect()` 才是"按下开播按钮"——此时电台开始广播，所有已调频道的订阅者同时收到数据。

```
ConnectableFlux 的行为：

           subscribe(A) → A 调到 95.7 MHz（仅注册，不开播）
           subscribe(B) → B 调到 95.7 MHz（仅注册，不开播）
           connect()    → 按下开播按钮！
                              │
                  ┌───────────┼───────────┐
                  ▼           ▼           ▼
              subscriberA  subscriberB  subscriberC
              
结果：上游只执行 1 次，数据广播给所有订阅者
```

用代码实现上面的股价场景：

```java
ConnectableFlux<StockPrice> sharedFlux = stockFlux.publish();

// 三个面板"调频道"——不触发 WebSocket 连接
sharedFlux.subscribe(kLinePanel::update);
sharedFlux.subscribe(orderBookPanel::update);
sharedFlux.subscribe(tradePanel::update);

// "按下开播按钮"——此时才连接 WebSocket，数据广播给三个面板
sharedFlux.connect();
```

完美解决——WebSocket 只连接一次，三个面板同时收到同一份行情数据。

### 2.3 为什么要拆分 subscribe 和 connect？

你可能会问：为什么不直接让 `subscribe()` 自动开始广播？

因为**时序问题**。如果你有三个订阅者，第一个 `subscribe()` 就触发广播，那么第二个和第三个订阅者会错过在广播开始前已经推送的数据。拆分后，你可以先注册好所有订阅者，再统一 `connect()`，保证所有人从同一个起点开始接收。

这就像广播电台不会在第一个听众调好频道后就立刻开播——它会等一会儿，让更多听众调好频道，然后准时开播。

---

## 三、publish() vs replay() vs refCount()：三种广播策略

`ConnectableFlux` 有三个最常用的变体，它们对应三种不同的广播策略。我们继续用广播电台来类比。

### 3.1 publish() —— 实时广播，迟到者听不到之前的

`publish()` 是最基本的广播策略。电台开始广播后，所有已经调好频道的听众能实时听到节目。但如果有人迟到了——电台已经播了 10 分钟新闻，这时候新来一个听众调频道——他只能听到从现在开始的内容，之前 10 分钟的新闻他听不到了。

```java
ConnectableFlux<Integer> flux = Flux.range(1, 10)
    .delayElements(Duration.ofMillis(100))
    .publish();

flux.subscribe(v -> System.out.println("A: " + v));
flux.connect();

// B 延迟 500ms 才订阅，错过了前几个数据
Thread.sleep(500);
flux.subscribe(v -> System.out.println("B: " + v));

// 输出：
// A: 1
// A: 2
// A: 3
// A: 4
// A: 5
// A: 6
// B: 7    ← B 从第 7 个才开始收到
// A: 7
// B: 8
// A: 8
// ...
```

**适用场景**：实时性要求高、历史数据无意义的场景。比如股价推送——你只需要最新价格，5 分钟前的价格已经过期了。

### 3.2 replay() —— 带回放功能，迟到者可以听录音

`replay()` 在广播的基础上增加了"回放"功能。电台会把播过的内容录下来，新来的听众调好频道后，先听一遍录音，再跟上实时广播。

```java
ConnectableFlux<Integer> flux = Flux.range(1, 10)
    .delayElements(Duration.ofMillis(100))
    .replay(3);  // 缓存最近 3 个元素

flux.subscribe(v -> System.out.println("A: " + v));
flux.connect();

Thread.sleep(500);
flux.subscribe(v -> System.out.println("B: " + v));

// 输出：
// A: 1
// A: 2
// A: 3
// A: 4
// A: 5
// A: 6
// B: 4    ← B 先收到回放的最近 3 个元素
// B: 5
// B: 6
// B: 7    ← 然后跟上实时广播
// A: 7
// ...
```

`replay()` 有三种参数模式：

```java
// 模式一：缓存最近 N 个元素
flux.replay(3);

// 模式二：缓存指定时间窗口内的元素
flux.replay(Duration.ofSeconds(5));

// 模式三：同时限制数量和时间
flux.replay(3, Duration.ofSeconds(5));
```

**适用场景**：历史数据有意义的场景。比如聊天室——新加入的用户可以看到最近几条聊天记录。

### 3.3 refCount() —— 没人听了就关电台，有人来了再开

`publish()` 和 `replay()` 都需要手动调用 `connect()`。但很多时候你想让连接自动管理——有订阅者就开电台，没人听了就关掉。

`refCount()` 就是干这个的。它像一个"智能电台管理员"：

- 当订阅者数量达到阈值（默认 1 个），自动 `connect()` 开播。
- 当所有订阅者都取消了，自动断开连接。
- 如果又有人来订阅了，重新 `connect()`。

```java
Flux<Integer> sharedFlux = Flux.range(1, 5)
    .delayElements(Duration.ofMillis(100))
    .publish()
    .refCount(1);  // 1 个订阅者就开播，全部取消就关停

Disposable d1 = sharedFlux.subscribe(v -> System.out.println("A: " + v));
// 订阅者数量 = 1，自动 connect()，开始推送

Thread.sleep(300);
d1.dispose();  // A 取消订阅
// 订阅者数量 = 0，自动断开连接

// 一段时间后 B 来订阅
Thread.sleep(100);
sharedFlux.subscribe(v -> System.out.println("B: " + v));
// 订阅者数量 = 1，重新 connect()，从头开始推送
```

⚠️ **踩坑提醒**：`refCount()` 在所有订阅者取消后会断开上游连接。如果上游是有状态的数据源（比如数据库游标），断开再重连可能导致从头开始。如果你不希望频繁重连，可以用 `refCount(n, Duration)` 版本——在所有订阅者取消后等待一个 grace period，如果在等待期内又有新订阅者来了，就不断开。

```java
// 至少 1 个订阅者才连接，全部取消后等 5 秒再断开
flux.publish().refCount(1, Duration.ofSeconds(5));
```

### 3.4 autoConnect() —— 开了就不关

`autoConnect(n)` 和 `refCount(n)` 类似，都是达到 n 个订阅者后自动连接。区别在于：**`autoConnect()` 一旦连接就不会再断开**，即使所有订阅者都取消了。

```java
// 达到 2 个订阅者后自动连接，但之后不会自动断开
flux.publish().autoConnect(2);
```

⚠️ **踩坑提醒**：`autoConnect()` 的 `remaining` 计数器是一次性的。如果你设置 `autoConnect(2)`，来了 2 个订阅者触发连接后，即使这 2 个都取消了再来 2 个新的，也不会触发第二次连接。它只负责"打开"，不负责"关闭"也不负责"重开"。

**适用场景**：长连接数据源（如 Kafka 消费者），断开重连代价很高，通常由外部生命周期管理来控制断开时机。

---

## 四、底层源码：FluxPublish 怎么实现广播

理解了"是什么"和"怎么用"，我们来看看底层是怎么实现的。`publish()` 操作符的核心实现类是 `FluxPublish`。

### 4.1 核心字段

```java
final class FluxPublish<T> extends ConnectableFlux<T> implements Scannable {
    final Flux<? extends T> source;    // 上游数据源
    final int prefetch;                // 预取数量
    volatile PublishSubscriber<T> connection;  // 当前的连接对象

    // 用 AtomicReferenceFieldUpdater 实现 CAS 更新 connection
    static final AtomicReferenceFieldUpdater<FluxPublish, PublishSubscriber>
        CONNECTION = AtomicReferenceFieldUpdater.newUpdater(...);
}
```

这里有一个关键设计：`connection` 是 `volatile` 的，通过 `AtomicReferenceFieldUpdater` 做 CAS 更新。为什么？因为多个线程可能同时调用 `connect()`，需要保证只有一个线程真正执行 `source.subscribe()`。

### 4.2 connect() 的 CAS 循环——谁先抢到谁开播

```java
public void connect(Consumer<? super Disposable> cancelSupport) {
    boolean doConnect;
    PublishSubscriber<T> s;
    for (;;) {
        s = connection;
        if (s == null || s.isTerminated()) {
            // 没有连接或已终止，创建新的
            PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);
            if (!CONNECTION.compareAndSet(this, s, u)) {
                continue;  // CAS 失败，有人抢先了，重试
            }
            s = u;
        }
        doConnect = s.tryConnect();  // 幂等：确保只订阅一次
        break;
    }
    cancelSupport.accept(s);
    if (doConnect) {
        source.subscribe(s);  // 只有赢家才真正订阅上游
    }
}
```

这段代码的精妙之处：

1. **CAS 无限循环**：多个线程同时 `connect()` 时，只有一个能成功设置 `connection`，其他线程循环后会发现 `connection` 已经存在，直接复用。
2. **`tryConnect()` 幂等性**：即使 `connect()` 被调用多次，`source.subscribe()` 也只执行一次。
3. **先 `cancelSupport.accept(s)` 后 `source.subscribe(s)`**：确保调用者在上游开始推送数据前就拿到了 `Disposable`，可以随时取消。

这就像广播电台的"开播流程"：多个工作人员可能同时按下开播按钮，但只有第一个人的指令生效，电台只开播一次。

### 4.3 subscribe() —— 只调频道，不开播

```java
public void subscribe(CoreSubscriber<? super T> actual) {
    PublishInner<T> inner = new PublishInner<>(actual);
    actual.onSubscribe(inner);
    for (;;) {
        if (inner.isCancelled()) break;
        PublishSubscriber<T> c = connection;
        if (c == null || c.isTerminated()) {
            PublishSubscriber<T> u = new PublishSubscriber<>(prefetch, this);
            if (!CONNECTION.compareAndSet(this, c, u)) continue;
            c = u;
        }
        if (c.add(inner)) {
            inner.parent = c;
            c.drainFromInner();
            break;
        }
    }
}
```

注意：`subscribe()` 不会调用 `source.subscribe()`。它只是创建一个 `PublishInner`（订阅者的代理），然后把它添加到 `PublishSubscriber` 的订阅者列表中。

这就是"调频道不开播"的实现——你把收音机频率调对了，但电台还没开始广播。

### 4.4 PublishSubscriber —— 广播电台的心脏

`PublishSubscriber` 是 `FluxPublish` 的内部核心类，它同时是上游的消费者和下游的广播者。

```java
static final class PublishSubscriber<T> implements InnerConsumer<T>, Disposable {
    final int prefetch;
    volatile PubSubInner<T>[] subscribers;  // 所有下游订阅者
    volatile long state;                    // 状态位（连接/终止/取消/WIP）
    Queue<T> queue;                         // 数据缓冲队列
}
```

**状态位设计**——`state` 是一个 `volatile long`，用不同位段表示不同状态：

```
bit 63: FINALIZED     (最终状态)
bit 62: TERMINATED    (上游已终止)
bit 61: CANCELLED     (已取消)
bit 59: SUBSCRIBED    (有订阅者)
bit 58: CONNECTED     (已连接)
bit 0-31: WIP          (工作计数器，用于 drain 串行化)
```

为什么用一个 long 而不是多个 boolean？因为需要**原子地同时修改多个状态**。比如"标记终止"时需要同时设置 `TERMINATED` 位并增加 WIP 计数器，一次 CAS 搞定，不需要多步加锁。

### 4.5 drain() —— 数据广播的核心：以最慢的人为准

`drain()` 方法是数据分发的核心。它的关键策略是：**取所有下游的最小 request 值作为本轮发送量**。

```java
void drain(long expectedState) {
    for (;;) {
        PubSubInner<T>[] a = subscribers;
        long maxRequested = Long.MAX_VALUE;
        
        for (PubSubInner<T> inner : a) {
            long r = inner.requested;
            if (r >= 0L) {
                maxRequested = Math.min(maxRequested, r);
            }
        }
        
        // 只发送 maxRequested 个数据，然后广播给所有订阅者
        int e = 0;
        while (e < maxRequested) {
            T v = q.poll();
            for (PubSubInner<T> inner : a) {
                inner.actual.onNext(v);  // 广播给每个人
            }
            e++;
        }
        
        // 向上游补充请求
        if (e != 0) s.request(e);
    }
}
```

**为什么取最小值而不是最大值？**

想象广播电台的情况：电台正在直播，听众 A 说"我能接收 100 条消息"，听众 B 说"我只能接收 3 条"。如果电台按 A 的需求发 100 条，B 的缓冲区就会被撑爆。所以电台只能以最慢的 B 为准——先发 3 条，等 B 消费完再继续。

这就是背压（backpressure）的正确语义：**以最慢的消费者为准**。

⚠️ **踩坑提醒**：这也意味着一个慢消费者会拖慢所有消费者。如果你的某个下游处理特别慢，整个广播都会被卡住。解决方案是给慢消费者加 `onBackpressureBuffer()` 或 `onBackpressureDrop()`，让它自己缓冲或丢弃溢出的数据，而不是拖累整个广播。

### 4.6 订阅者管理：Copy-on-Write 策略

订阅者的添加和删除使用 COW（Copy-on-Write）策略：

```java
boolean add(PublishInner<T> inner) {
    for (;;) {
        PubSubInner<T>[] a = subscribers;
        if (a == TERMINATED) return false;
        int n = a.length;
        PubSubInner<?>[] b = new PubSubInner[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = inner;
        if (SUBSCRIBERS.compareAndSet(this, a, b)) return true;
    }
}
```

每次添加/删除订阅者时，都复制整个数组。为什么不用 `ConcurrentLinkedQueue` 或 `synchronized`？

- `ConcurrentLinkedQueue` 遍历时无法保证快照一致性，而 `drain()` 需要在一次循环内看到一致的订阅者列表。
- `synchronized` 会阻塞 `drain()` 和 `remove()`，降低吞吐量。
- COW 数组的遍历是零开销的（直接 for-each），订阅者变更相对于数据推送是低频操作，复制成本可以接受。

这就像电台的听众名单——平时读名单（广播时遍历订阅者）很频繁，改名单（有人加入/离开）很少。所以改的时候复制一份新的，读的时候直接用旧的快照，互不干扰。

### 4.7 三个哨兵数组

```java
static final PubSubInner[] INIT       = new PublishInner[0];
static final PubSubInner[] CANCELLED  = new PublishInner[0];
static final PubSubInner[] TERMINATED = new PublishInner[0];
```

三个空数组，内容相同但**引用不同**，用来表示三种状态。通过 `==` 比较引用就能判断状态，比用额外的 boolean 字段更高效——因为它复用了 `subscribers` 这个 volatile 字段，不需要额外读一次内存。

---

## 五、FluxReplay 的缓存机制：录音回放怎么实现

`replay()` 的核心区别在于：它有一个 `ReplayBuffer` 来缓存历史数据。新订阅者来了之后，先回放缓存的数据，再跟上实时流。

### 5.1 三种缓存策略

```java
ReplaySubscriber<T> newState() {
    if (scheduler != null) {
        // 大小 + 时间双约束
        return new ReplaySubscriber<>(
            new SizeAndTimeBoundReplayBuffer<>(history, ttl, scheduler), ...);
    }
    if (history != Integer.MAX_VALUE) {
        // 仅大小约束
        return new ReplaySubscriber<>(
            new SizeBoundReplayBuffer<>(history), ...);
    }
    // 无限缓存
    return new ReplaySubscriber<>(
        new UnboundedReplayBuffer<>(Queues.SMALL_BUFFER_SIZE), ...);
}
```

| 缓存策略 | 类名 | 数据结构 | 适用场景 |
|---------|------|---------|---------|
| 无限缓存 | `UnboundedReplayBuffer` | 分段 Object[] 链表 | 数据量可控、需要完整回放 |
| 固定大小 | `SizeBoundReplayBuffer` | 单向链表 | 只需最近 N 条 |
| 大小+时间 | `SizeAndTimeBoundReplayBuffer` | 带时间戳的链表 | 时间窗口内的回放 |

⚠️ **踩坑提醒**：`UnboundedReplayBuffer` 如果上游是一个无限流，内存会持续增长直到 OOM。务必在数据量可控的场景下使用，或者改用 `SizeBoundReplayBuffer`。

**为什么 `UnboundedReplayBuffer` 用分段数组而不是 `ArrayList`？**

因为 `ArrayList` 扩容时需要复制整个数组（O(n)），而分段数组只需要分配新段并链接（O(1)）。对于持续追加的缓存场景，这个差异很大。

### 5.2 ReplaySubscriber 的精细背压

`FluxPublish` 的背压是"取所有下游最小 request"，而 `ReplaySubscriber` 更精细——它跟踪每个下游订阅者的消费进度（index），只有当所有活跃订阅者都推进到一定位置时，才向上游请求更多数据：

```java
void manageRequest(long currentState) {
    for (;;) {
        int nextPrefetchIndex = this.nextPrefetchIndex;
        boolean shouldPrefetch = true;
        for (ReplaySubscription<T> rp : subscribers) {
            if (rp.index() < nextPrefetchIndex) {
                shouldPrefetch = false;  // 还有人没跟上，不请求
                break;
            }
        }
        if (shouldPrefetch) {
            this.nextPrefetchIndex = nextPrefetchIndex + limit;
            p.request(limit);
        }
    }
}
```

这比 `FluxPublish` 更合理：回放场景下，不同订阅者可能处于不同的回放进度（有人刚来在听录音，有人已经在听直播了），用 index 跟踪比用 request 计数更精确。

---

## 六、FluxRefCount：引用计数怎么实现

`refCount()` 的核心是 `FluxRefCount` 类，它实现了"自动连接 + 自动断开"。

### 6.1 subscribe() 中的引用计数

```java
public void subscribe(CoreSubscriber<? super T> actual) {
    RefCountInner<T> inner = new RefCountInner<>(actual);
    source.subscribe(inner);  // 先订阅 ConnectableFlux（仅注册，不触发连接）

    synchronized (this) {
        RefCountMonitor<T> conn = connection;
        if (conn == null || conn.terminated) {
            conn = new RefCountMonitor<>(this);
            connection = conn;
        }
        long c = conn.subscribers;
        conn.subscribers = c + 1;
        if (!conn.connected && c + 1 == n) {
            connect = true;   // 达到阈值，触发连接
            conn.connected = true;
        }
    }
    if (connect) {
        source.connect(conn);  // 真正按下开播按钮
    }
}
```

⚠️ **踩坑提醒**：注意这里用了 `synchronized` 而不是 CAS。为什么？因为 `RefCountMonitor` 的状态涉及多个字段（`subscribers`、`connected`、`terminated`）的联动修改，CAS 只能保证单个字段的原子性。`synchronized` 可以保证整组修改的原子性。在订阅者变更不频繁的场景下，`synchronized` 的性能完全够用。

### 6.2 cancel() 的引用计数递减

```java
void cancel(RefCountMonitor rc) {
    synchronized (this) {
        long c = rc.subscribers - 1;
        rc.subscribers = c;
        if (c != 0L || !rc.connected) return;  // 还有订阅者，不断开
        if (rc == connection) {
            dispose = RefCountMonitor.DISCONNECT.getAndSet(rc, Disposables.disposed());
            connection = null;
        }
    }
    if (dispose != null) {
        dispose.dispose();  // 所有订阅者都走了，断开连接
    }
}
```

当 `subscribers` 递减到 0 且当前已连接时，触发断开。`Disposables.disposed()` 是一个哨兵值，防止重复 dispose。

### 6.3 RefCountInner 的时序问题

`RefCountInner` 有一个棘手的时序问题：`onSubscribe()` 在 `setRefCountMonitor()` 之前调用（因为先订阅 source，再设置 monitor）。如果在这个窗口期内上游就完成了（`onComplete/onError`），需要延迟到 `setRefCountMonitor()` 时再传递终止信号：

```java
void setRefCountMonitor(RefCountMonitor<T> connection) {
    this.connection = connection;
    this.actual.onSubscribe(this);
    for (;;) {
        int previousState = this.state;
        if (isCancelled(previousState)) return;
        if (isTerminated(previousState)) {
            connection.upstreamFinished();
            // 延迟传递终止信号
            Throwable e = this.error;
            if (e != null) this.actual.onError(e);
            else this.actual.onComplete();
            return;
        }
        if (STATE.compareAndSet(this, previousState, previousState | MONITOR_SET_FLAG))
            return;
    }
}
```

这就像你刚调好收音机频道，还没来得及告诉管理员"我到了"，电台就播完了。这时候电台需要把"播完了"这个消息暂存一下，等管理员确认你到位后再告诉你。

---

## 七、FluxAutoConnect：开了就不关

`FluxAutoConnect` 是最简单的实现——整个类不到 80 行代码：

```java
final class FluxAutoConnect<T> extends Flux<T> implements Scannable {
    volatile int remaining;
    
    public void subscribe(CoreSubscriber<? super T> actual) {
        source.subscribe(actual);
        if (remaining > 0 && REMAINING.decrementAndGet(this) == 0) {
            source.connect(cancelSupport);  // 达到阈值，连接
        }
    }
}
```

**与 refCount 的关键区别**：`FluxAutoConnect` 只负责连接，不负责断开。一旦连接建立，即使所有订阅者都取消了，连接也不会断开。这就是"fire-and-forget"（点火就不管了）语义。

为什么需要这种语义？某些数据源是持续推送的（如 Kafka 消费），断开再重连的代价很高。此时用 `autoConnect()` 配合外部生命周期管理更合适——你来决定什么时候断开，而不是自动管理。

---

## 八、普通 Flux 为什么做不到多播？

| 行为 | 普通 Flux | ConnectableFlux (publish) |
|------|----------|--------------------------|
| `subscribe()` 触发上游 | 是，每次都创建新的数据流 | 否，仅注册下游 |
| 多个 Subscriber 共享数据 | 不共享，各走各的 | 共享同一个数据流 |
| 背压语义 | 每个 Subscriber 独立背压 | 取所有下游的最小 request |
| 上游订阅次数 | 每次 subscribe 一次 | 只 subscribe 一次 |

根本原因在于普通 `Flux` 的 `subscribe()` 同时做了两件事——注册订阅者和触发上游。`ConnectableFlux` 把这两件事拆开了。

---

## 九、完整的多播策略对照表

### 广播策略对照表

| 特性 | publish() | replay(n) | replay(Duration) |
|------|-----------|-----------|-------------------|
| **缓存历史数据** | 不缓存 | 缓存最近 N 个 | 缓存时间窗口内 |
| **新订阅者收到历史** | 不能 | 能 | 能 |
| **内部缓冲区** | Queue<T> | SizeBoundReplayBuffer | SizeAndTimeBoundReplayBuffer |
| **背压策略** | 取所有下游最小 request | 跟踪每个下游的 index |
| **类比** | 实时广播 | 带回放功能 | 带过期回放 |

### 连接控制对照表

| 特性 | autoConnect(n) | refCount(n) | refCount(n, Duration) |
|------|---------------|-------------|----------------------|
| **自动连接** | 达到 n 个订阅者 | 达到 n 个订阅者 | 达到 n 个订阅者 |
| **自动断开** | 不断开 | 立即断开 | 等 grace period 后断开 |
| **重新连接** | 不支持 | 支持 | 支持 |
| **并发安全** | AtomicInteger CAS | synchronized | scheduled task |
| **类比** | 开了就不关 | 没人听就关 | 没人听等一会儿再关 |
| **适用场景** | 长连接、外部管理 | 短连接、自管理 | 抖动场景、防频繁重连 |

### 状态位设计对照表

| 状态位 | PublishSubscriber | RefCountInner |
|--------|-------------------|---------------|
| **位宽** | long (64 bit) | int (32 bit) |
| **CONNECTED** | bit 58 | N/A |
| **TERMINATED** | bit 62 | bit 30 |
| **CANCELLED** | bit 61 | bit 31 |
| **WIP** | 低 32 位 | N/A |
| **MONITOR_SET** | N/A | bit 29 |

---

## 十、设计启示与最佳实践

1. **状态压缩到 long 位字段**：`PublishSubscriber` 用一个 `volatile long` 同时表达 5 种状态和工作计数器，只需一次 CAS 即可原子修改。这是高性能响应式编程的标准模式。

2. **COW 数组 vs synchronized**：`FluxPublish` 的订阅者管理用 COW + CAS（读多写少）；`FluxRefCount` 用 synchronized（多字段联动）。选择取决于读写比和原子性需求。

3. **哨兵对象区分状态**：`INIT`、`CANCELLED`、`TERMINATED` 三个空数组引用不同但内容相同，通过 `==` 比较取代 boolean 字段，减少 volatile 读次数。

4. **背压聚合取最小值**：这是多播场景下唯一正确的背压策略，但代价是慢消费者会拖慢整体。实际应用中通常配合 `onBackpressureBuffer/Drop` 使用。

5. **延迟求值避免不必要开销**：`replay()` 的缓存只有在有订阅者时才真正开始填充；`refCount()` 的连接只有在达到阈值时才触发。没有订阅者时，整个多播机制几乎零开销。

6. **实际开发建议**：
   - 实时数据流用 `publish().refCount()`——自动管理连接生命周期。
   - 需要回放用 `replay(n).refCount()`——既回放又自动管理。
   - 长连接数据源用 `publish().autoConnect(1)`——连上就不断。
   - 手动精细控制用 `publish()` + 手动 `connect()`——完全自己掌控。
