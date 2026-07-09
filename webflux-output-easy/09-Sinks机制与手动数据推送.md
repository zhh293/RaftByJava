# Sinks 机制与手动数据推送（易懂版）

> **Reactor Core 源码解析系列 · 第 09 篇 · 易懂版**

---

## 一、从一个真实场景说起：你为什么需要 Sinks？

假设你在做一个 WebSocket 聊天室。每当有用户发消息，服务端要把这条消息推给所有在线的人。

用 Flux 怎么写？

```java
// 不太对劲的写法
Flux.just("hello", "world")  // 数据在一开始就写死了，没法后续添加
    .subscribe(msg -> sendToClient(msg));
```

问题来了——`Flux.just()` 是"一次性把数据定好"的模式。可聊天室的消息是源源不断来的，你没办法在代码写好的那一刻就知道后面会有什么消息。

你可能会想到 `Flux.create()`：

```java
Flux.create(sink -> {
    // 把 sink 存起来，后面有消息了调 sink.next()
    messageHandler.register(msg -> sink.next(msg));
})
.subscribe(msg -> sendToClient(msg));
```

这样能用，但有个问题：**sink 被"关"在 `create` 的回调函数里面了**。如果你想在别的地方（比如另一个 Controller 方法里）推送数据，就得想办法把 sink 传出来，很不优雅。

**Sinks 就是为了解决这个问题而生的。** 它把"数据推送"和"数据订阅"彻底拆开了：

```java
// 创建一个 Sinks —— 相当于拿到了一个"手动阀门"
Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

// 在任何地方、任何时候往里推数据
sink.tryEmitNext("用户A说：大家好");
sink.tryEmitNext("用户B说：你好");

// 订阅者从 Flux 那头接水
sink.asFlux().subscribe(msg -> sendToClient(msg));
```

**类比：之前的 `Flux.just()` / `Flux.create()` 都是"接好水管就自动来水"，而 Sinks 给你一个手动阀门——你拧一下出一滴水（`tryEmitNext`），拧到底关水（`tryEmitComplete`），管道坏了就报修（`tryEmitError`）。**

---

## 二、Sinks 的家族图谱：我该用哪个？

**Q：Sinks 有好多种，怎么选？**

先问自己两个问题：
1. **你要推多少个值？** 一个值用 `Sinks.one()`，零个值用 `Sinks.empty()`，多个值用 `Sinks.many()`。
2. **有多少人接水？** 一个订阅者用 `unicast()`，多个订阅者用 `multicast()` 或 `replay()`。

画个选择流程图：

```
要推几个值？
├── 0个值（只通知完成/失败）──→ Sinks.empty()
├── 1个值 ──→ Sinks.one()
└── 多个值 ──→ Sinks.many()
     └── 几个人接水？
          ├── 只有1个人 ──→ .unicast()
          │    ├── 来不及接就存着 ──→ .onBackpressureBuffer()
          │    └── 来不及接就报错 ──→ .onBackpressureError()
          ├── 多个人同时接 ──→ .multicast()
          │    ├── 有缓冲区 ──→ .onBackpressureBuffer()
          │    ├── 全有或全无 ──→ .directAllOrNothing()
          │    └── 尽力而为 ──→ .directBestEffort()
          └── 多个人 + 迟到的也能看到历史消息 ──→ .replay()
               ├── 所有历史 ──→ .all()
               ├── 最近N条 ──→ .limit(N)
               └── 最近N条且不超过时间 ──→ .limit(N, maxAge)
```

**生活类比：**
- `unicast()` = 私聊（只能一对一）
- `multicast()` = 群聊（多人同时收到当前消息）
- `replay()` = 群聊 + 聊天记录（新加入的人也能看到之前的消息）

---

## 三、tryEmitNext 返回值：别再默默丢数据了

**Q：为什么不是 `sink.next(value)` 而是 `sink.tryEmitNext(value)`？这个 `try` 是什么意思？**

以前的老 API（Processor）是这样的：

```java
processor.onNext(value);  // 成功了？失败了？谁知道呢...
```

如果推送失败了（比如队列满了、消费者跑了），要么默默丢弃数据，要么直接抛异常把你的线程炸了。你完全不知道发生了什么。

Sinks 的 `tryEmitNext` 返回一个 `EmitResult` 枚举，明确告诉你结果：

```java
Sinks.EmitResult result = sink.tryEmitNext("hello");

switch (result) {
    case OK:                   // 成功了，放心
        break;
    case FAIL_OVERFLOW:        // 队列满了，消费者太慢
        log.warn("消费者跟不上了，考虑降速");
        break;
    case FAIL_CANCELLED:       // 消费者已经跑了
        log.info("消费者取消了订阅");
        break;
    case FAIL_TERMINATED:      // 管道已经关了（complete 或 error 过了）
        log.warn("Sink 已经终止了，别再推了");
        break;
    case FAIL_NON_SERIALIZED:  // 有人和你同时在推，并发冲突
        log.debug("并发竞争，稍后重试");
        break;
    case FAIL_ZERO_SUBSCRIBER: // 还没人来接水呢
        log.info("还没有订阅者");
        break;
}
```

**类比：以前往邮箱扔信，扔完就走，不知道信箱满了还是没人住。现在是快递柜，放进去会告诉你"投递成功"、"柜子满了"、"收件人搬走了"。**

### 便捷方法 emitNext + EmitFailureHandler

如果你不想每次都手动检查结果，可以用 `emitNext`：

```java
// 方式一：失败直接抛异常
sink.emitNext("hello", Sinks.EmitFailureHandler.FAIL_FAST);

// 方式二：遇到并发冲突就自旋重试（最多等100ms）
sink.emitNext("hello", Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
```

**Q：`busyLooping` 为什么只重试 `FAIL_NON_SERIALIZED`，不重试 `FAIL_OVERFLOW`？**

因为 `FAIL_NON_SERIALIZED` 意味着"有另一个线程正在推数据，等它推完你就能推了"——这是一个**暂时**的状态，等一下就好。而 `FAIL_OVERFLOW`（队列满了）不是等一下就能解决的，你在这里自旋等待，消费者也不会因此变快。所以自旋重试只对并发冲突有意义。

---

## 四、多线程同时推数据会怎样？——序列化机制

**Q：如果两个线程同时调 `sink.tryEmitNext()`，会不会出问题？**

先回答一个前置问题：**为什么并发推送是危险的？**

Reactive Streams 规范有一条铁律：**`onNext` 调用必须是串行的**。也就是说，不能两个线程同时调用下游的 `onNext()`，否则下游的状态会乱。

**类比：想象一条传送带（下游消费者），多个厨师（生产者线程）同时往传送带上放菜。如果两个厨师同时把菜放在同一个位置，菜就叠在一起了（数据损坏）。我们需要一个机制让厨师排队放菜。**

### Sinks 默认就是线程安全的

当你用 `Sinks.many().unicast().onBackpressureBuffer()` 创建 Sink 时，实际上你拿到的不是一个裸的 `SinkManyUnicast`，而是被 `SinkManySerialized` 包装过的版本：

```
你以为你拿到的：      SinkManyUnicast
你实际拿到的：   SinkManySerialized → 内部包着 → SinkManyUnicast
```

`SinkManySerialized` 的工作原理类似一个"非阻塞锁"：

```java
// 简化版 tryEmitNext 逻辑
public EmitResult tryEmitNext(T value) {
    Thread currentThread = Thread.currentThread();
    
    // 第一步：尝试获取"推送权"
    if (!tryAcquire(currentThread)) {
        return EmitResult.FAIL_NON_SERIALIZED;  // 获取失败，告诉调用者"有人在推了"
    }
    
    try {
        // 第二步：只有拿到权限的线程才能真正推送
        return delegate.tryEmitNext(value);
    } finally {
        // 第三步：释放"推送权"
        release(currentThread);
    }
}
```

**Q：为什么不用 `synchronized` 或 `ReentrantLock`？**

因为响应式编程的核心原则是**不阻塞**。如果用 `synchronized`，线程 A 正在推数据，线程 B 就得阻塞等待——这违反了 Reactive Streams 的非阻塞约束。

`SinkManySerialized` 的做法是：拿不到权限就直接返回 `FAIL_NON_SERIALIZED`，让调用者自己决定怎么办（重试、丢弃、排队），而不是强制阻塞你的线程。

**类比：不是"排队等厕所"（`synchronized`），而是"看到厕所有人就去别的厕所或者等一会儿再来"（非阻塞）。**

### tryAcquire 的细节：CAS + 线程亲和性

```java
boolean tryAcquire(Thread currentThread) {
    if (WIP.compareAndSet(this, 0, 1)) {
        // CAS 成功：我是第一个来的，记录"当前持有者"
        LOCKED_AT.lazySet(this, currentThread);
        return true;
    } else {
        // 已经有人在推了
        if (LOCKED_AT.get(this) == currentThread) {
            // 是我自己重入的（同一个线程递归调用），允许
            WIP.incrementAndGet(this);
            return true;
        }
        return false;  // 是别的线程，拒绝
    }
}
```

这里用了两个原子变量：
- `WIP`（work-in-progress）：0 表示空闲，>0 表示有人在推
- `LOCKED_AT`：记录当前持有"推送权"的线程

**支持可重入**意味着如果你在推送过程中触发了某个回调，回调里又推送了数据，同一个线程不会被自己拒绝。

### unsafe() 模式：我自己管并发安全

如果你能保证只有一个线程推数据（比如在操作符内部），可以用 `Sinks.unsafe()` 跳过序列化包装：

```java
// 不加序列化包装，性能更好，但调用者自己负责线程安全
Sinks.Many<String> sink = Sinks.unsafe().many().unicast().onBackpressureBuffer();
```

**生产环境建议：除非你对并发有十足把握，否则不要用 `unsafe()`。**

---

## 五、SinkManyUnicast：单人接水的水龙头

**Q：`Sinks.many().unicast().onBackpressureBuffer()` 底层是怎么工作的？**

### 核心结构

`SinkManyUnicast` 内部有一个队列（`Queue`），数据先进队列，再从队列搬给订阅者：

```
生产者 ──tryEmitNext()──→ [队列 Queue] ──drain()──→ 订阅者 onNext()
```

关键字段：

| 字段 | 类型 | 作用 |
|------|------|------|
| `queue` | `Queue<T>` | 缓冲未消费的数据 |
| `actual` | `CoreSubscriber` | 唯一的订阅者 |
| `requested` | `volatile long` | 订阅者还需要多少数据 |
| `wip` | `volatile int` | drain 循环的"锁" |
| `done` | `volatile boolean` | 是否已 complete/error |
| `cancelled` | `volatile boolean` | 订阅者是否取消了 |
| `once` | `volatile int` | 保证只有一个订阅者 |

### tryEmitNext：推数据的过程

```java
public EmitResult tryEmitNext(T t) {
    if (done) return EmitResult.FAIL_TERMINATED;       // 1. 管道已关
    if (cancelled) return EmitResult.FAIL_CANCELLED;    // 2. 消费者跑了
    if (!queue.offer(t)) {                              // 3. 尝试入队
        return (once > 0) ? EmitResult.FAIL_OVERFLOW    //    有人订阅但队列满了
                          : EmitResult.FAIL_ZERO_SUBSCRIBER; // 没人订阅
    }
    drain(t);                                           // 4. 尝试把队列里的数据搬给消费者
    return EmitResult.OK;
}
```

**类比：你往传送带上放了一盘菜（`queue.offer`），然后喊一声"有新菜了！"（`drain`），服务员（订阅者）来取。**

### drain 机制：搬运工的工作流程

`drain()` 是整个 Sink 最核心的方法——它负责把队列里的数据搬给订阅者。但这个搬运过程有几个规矩：

**规矩一：同一时间只能有一个搬运工在干活（WIP 计数器）**

```java
void drain(T dataSignalOfferedBeforeDrain) {
    if (WIP.getAndIncrement(this) != 0) {
        // 已经有人在搬了，我就不进去了
        // 但如果管道已取消/终止，要处理刚放上来的那盘菜
        return;
    }
    // 只有第一个进来的线程才会执行实际的搬运
    // ...
}
```

**规矩二：只搬消费者要求的数量（背压控制）**

```java
void drainRegular(CoreSubscriber<? super T> a) {
    for (;;) {
        long r = requested;   // 消费者说"我要 r 个"
        long e = 0L;          // 已经搬了 e 个
        
        while (r != e) {      // 还没搬够
            T t = queue.poll();
            if (t == null) break;  // 队列空了
            a.onNext(t);       // 搬给消费者
            e++;
        }
        
        if (e != 0 && r != Long.MAX_VALUE) {
            REQUESTED.addAndGet(this, -e);  // 扣减已搬数量
        }
        // ...
    }
}
```

**类比：服务员说"我只能端3盘菜"（`requested = 3`），搬运工就只搬3盘，不多不少。这就是背压（Backpressure）——消费者控制速度，防止被撑爆。**

**规矩三："遗漏检查"（missed check）——搬完再看看有没有新菜**

```java
// drain 循环的末尾
missed = WIP.addAndGet(this, -missed);
if (missed == 0) {
    break;  // 没有新数据了，可以收工
}
// missed > 0 说明有新数据在我搬运期间入队了，继续搬
```

这个模式保证了不会漏掉任何数据：即使在你搬运的过程中有新数据入队，你也能发现并处理。

### 单订阅保证

`SinkManyUnicast` 只允许一个订阅者——第二个想订阅的人会收到错误：

```java
public void subscribe(CoreSubscriber<? super T> actual) {
    if (ONCE.compareAndSet(this, 0, 1)) {
        // 第一个订阅者，欢迎
        this.actual = actual;
        // ...
    } else {
        // 第二个订阅者，拒绝
        Operators.error(actual, new IllegalStateException(
            "Sinks.many().unicast() sinks only allow a single Subscriber"));
    }
}
```

---

## 六、SinkManyUnicastNoBackpressure：不要队列的极速模式

**Q：如果我确定消费者一定能跟上，队列不是多余的吗？**

`onBackpressureError()` 创建的 `SinkManyUnicastNoBackpressure` 完全去掉了队列：

```java
public EmitResult tryEmitNext(T t) {
    if (state == SUBSCRIBED) {
        if (requested == 0L) {
            return EmitResult.FAIL_OVERFLOW;  // 消费者没需求，直接失败
        }
        actual.onNext(t);                     // 直接推给消费者，没有队列
        Operators.produced(REQUESTED, this, 1);
        return EmitResult.OK;
    }
    // ... 其他状态处理
}
```

**类比：没有传送带，厨师直接把菜端给客人。快，但如果客人还没准备好吃，菜就掉地上了。**

**适用场景：** 热点数据流（如股票行情）——数据要么立即被消费，要么直接丢弃。旧数据毫无价值，不值得缓冲。

---

## 七、多人接水：SinkManyEmitterProcessor

**Q：`Sinks.many().multicast().onBackpressureBuffer()` 多个订阅者是怎么管理的？**

### 核心设计：copy-on-write 订阅者数组

```java
boolean add(EmitterInner<T> inner) {
    for (;;) {
        PubSubInner<T>[] a = subscribers;      // 当前数组
        PubSubInner<?>[] b = new PubSubInner[a.length + 1];  // 新数组，多一个位
        System.arraycopy(a, 0, b, 0, a.length);
        b[a.length] = inner;                   // 新订阅者放在最后
        if (SUBSCRIBERS.compareAndSet(this, a, b)) {
            return true;                       // CAS 替换成功
        }
        // CAS 失败说明有并发修改，重试
    }
}
```

每次添加/移除订阅者都创建新数组然后 CAS 替换——这叫 **copy-on-write**。虽然每次写操作有数组复制的开销，但读操作（drain 循环里遍历订阅者）是零成本的，不需要加锁。

### 最小需求对齐：最慢的人拖住所有人

```java
// drain 中计算推送量
long maxRequested = Long.MAX_VALUE;
for (PubSubInner<T> inner : subscribers) {
    maxRequested = Math.min(maxRequested, inner.requested);  // 取最小值
}
```

**类比：旅行团过马路——最慢的大爷走到对面之前，所有人都不能动。**

如果订阅者 A 说"我要100个数据"，订阅者 B 说"我只要3个"，那 drain 循环只会推3个。因为多播语义要求**每个订阅者看到相同的数据**——如果给 A 推了100个但只给 B 推3个，数据就不一致了。

⚠️ **踩坑提醒：一个慢订阅者会拖慢所有人。** 如果你的场景中某些订阅者很慢（比如写数据库的比较慢），考虑用 `directBestEffort()` 替代 `onBackpressureBuffer()`。`bestEffort` 模式下，推不进去的订阅者会被跳过，不会拖慢其他人。

### 队列延迟初始化

`SinkManyEmitterProcessor` 的队列不是在构造时创建的，而是在第一次 `tryEmitNext` 时才创建：

```java
public EmitResult tryEmitNext(T t) {
    Queue<T> q = queue;
    if (q == null) {
        // 第一次推数据，现在才创建队列
        q = Queues.<T>get(prefetch).get();
        queue = q;
    }
    // ...
}
```

**为什么延迟？** 因为 `SinkManyEmitterProcessor` 可以同时当"手动 Sink"用，也可以当"中间操作符"订阅上游 Publisher 用。两种模式下队列的创建时机和类型可能不同，所以推迟到实际使用时再创建。

---

## 八、历史回放：SinkManyReplayProcessor

**Q：新订阅者想看之前的数据怎么办？**

`Sinks.many().replay()` 创建的 `SinkManyReplayProcessor` 会把所有数据存在一个 `ReplayBuffer` 里：

```java
public EmitResult tryEmitNext(T t) {
    buffer.add(t);                    // 存入缓冲区
    for (ReplaySubscription<T> rs : subscribers) {
        buffer.replay(rs);            // 通知所有订阅者
    }
    return EmitResult.OK;
}
```

**关键区别：ReplayProcessor 永远不会返回 `FAIL_ZERO_SUBSCRIBER`。** 即使没有人订阅，数据也会被存起来，等人来了再回放。

三种回放策略：

| 策略 | API | 行为 |
|------|-----|------|
| 全部回放 | `replay().all()` | 无限存储所有历史数据（小心内存！） |
| 最近N条 | `replay().limit(N)` | 只保留最近N条 |
| 最近N条 + 超时 | `replay().limit(N, maxAge)` | 保留最近N条，但超过 maxAge 的自动丢弃 |

**类比：**
- `all()` = 聊天记录永久保存（微信）
- `limit(100)` = 只保留最近100条消息
- `limit(100, 24h)` = 保留最近100条且24小时内的（阅后即焚+数量限制）

⚠️ **踩坑提醒：`replay().all()` 在生产环境要慎用！** 如果数据源是无限流，`ReplayBuffer` 会无限增长，最终 OOM。大多数场景应该用 `replay().limit(N)` 限制缓冲大小。

---

## 九、Sinks.one() 和 Sinks.empty()：一次性 Promise

**Q：只推一个值的场景用什么？**

`Sinks.one()` 相当于一个 Promise（或 CompletableFuture）：

```java
Sinks.One<String> sink = Sinks.one();

// 某个异步操作完成后设置结果
asyncOperation.onComplete(result -> {
    sink.tryEmitValue(result);
});

// 订阅者等待结果
sink.asMono().subscribe(value -> System.out.println("得到结果：" + value));
```

**有趣的细节：** 如果你调 `sink.tryEmitValue(null)`，它不会推一个 null 值（Reactor 不允许 null），而是降级为 `tryEmitEmpty()`——也就是"无值完成"。这和 `Mono.empty()` 的语义一致。

`Sinks.empty()` 更简单——只能发 complete 或 error 信号，不能携带值：

```java
Sinks.Empty<Void> sink = Sinks.empty();

// 通知任务完成
sink.tryEmitEmpty();

// 或者通知任务失败
// sink.tryEmitError(new RuntimeException("出错了"));
```

**类比：`Sinks.one()` 是等快递（等一个包裹到），`Sinks.empty()` 是等通知（只关心"事情办完了"或"办砸了"，没有具体内容）。**

### 多订阅者支持

`SinkOneMulticast` 支持多个订阅者。如果 Sink 已经有值了，新来的订阅者会立即收到这个值——这是"replay 单值"的语义。

底层用了一个巧妙的状态机：`EMPTY`、`TERMINATED_EMPTY`、`TERMINATED_ERROR`、`TERMINATED_VALUE` 都是 `Inner[]` 类型的空数组，但它们是**不同的对象引用**。通过 `==` 比较引用就能区分状态，不需要额外的状态字段。

---

## 十、Sinks vs 旧版 Processor：为什么要换？

**Q：之前的 `EmitterProcessor`、`UnicastProcessor` 不是也能用吗？为什么要换成 Sinks？**

旧版 Processor 有两个致命问题：

**问题一：接口暴露过多**

Processor 同时实现了 `Publisher` 和 `Subscriber`。这意味着用户不仅能往里推数据（`onNext`），还能调 `onSubscribe`、`request`、`cancel` 去干扰内部状态：

```java
// 旧版：用户可以做出这种危险操作
EmitterProcessor<String> processor = EmitterProcessor.create();
processor.onSubscribe(someSubscription);  // 破坏内部状态！
processor.cancel();                       // 外部取消！
```

Sinks 把"推送 API"和"订阅 API"彻底分开：
- 推送用 `tryEmitNext()` / `tryEmitError()` / `tryEmitComplete()`
- 订阅用 `asFlux()` 返回一个普通的 `Flux`

用户根本接触不到内部的 `Subscriber` 接口。

**问题二：失败不可见**

旧版 `Processor.onNext()` 在队列满或终止后，要么默默丢弃数据，要么抛异常。调用者无法区分"推成功了但下游还没消费"和"推失败了因为队列满了"。

Sinks 用 `EmitResult` 枚举让每次推送的结果都透明可见。

**映射关系表：**

| 旧版 Processor | 新版 Sinks API |
|---------------|---------------|
| `UnicastProcessor` | `Sinks.many().unicast().onBackpressureBuffer()` |
| `EmitterProcessor` | `Sinks.many().multicast().onBackpressureBuffer()` |
| `ReplayProcessor` | `Sinks.many().replay().all()` / `.limit(N)` |
| `DirectProcessor` | `Sinks.many().multicast().directAllOrNothing()` |
| `MonoProcessor` | `Sinks.one()` |

---

## 十一、实战：用 Sinks 实现一个简易的事件总线

```java
@Component
public class EventBus {
    
    // 创建一个多播 Sink，支持多个订阅者
    private final Sinks.Many<Event> sink = Sinks.many()
        .multicast()
        .onBackpressureBuffer();
    
    /**
     * 发布事件 —— 可以在任何地方、任何线程调用
     */
    public void publish(Event event) {
        EmitResult result = sink.tryEmitNext(event);
        if (result.isFailure()) {
            // 推送失败的处理
            log.warn("事件发布失败：{}，原因：{}", event, result);
            
            if (result == EmitResult.FAIL_NON_SERIALIZED) {
                // 并发冲突，用 busyLooping 重试
                sink.emitNext(event, 
                    Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
            }
        }
    }
    
    /**
     * 订阅特定类型的事件
     */
    public <T extends Event> Flux<T> on(Class<T> eventType) {
        return sink.asFlux()
            .filter(eventType::isInstance)
            .cast(eventType);
    }
}

// 使用
@RestController
public class OrderController {
    @Autowired EventBus eventBus;
    
    @PostMapping("/orders")
    public Mono<Order> createOrder(@RequestBody Order order) {
        return orderService.save(order)
            .doOnSuccess(saved -> eventBus.publish(new OrderCreatedEvent(saved)));
    }
}

@Service
public class NotificationService {
    @Autowired EventBus eventBus;
    
    @PostConstruct
    void init() {
        eventBus.on(OrderCreatedEvent.class)
            .subscribe(event -> sendNotification(event.getOrder()));
    }
}
```

⚠️ **踩坑提醒：用 `multicast()` 时，如果先 `tryEmitNext` 再有人 `subscribe`，先推的数据会丢失！** 因为 `multicast` 不做回放。如果你需要"先发后订也能收到"，用 `replay()` 或先确保所有订阅者就位后再推数据。

---

## 十二、资源清理：取消时队列里的数据怎么办？

**Q：如果队列里缓冲了10个数据库连接对象，消费者突然取消了，这些连接会泄漏吗？**

会！除非你注册了 `onDiscard` 钩子。

`SinkManyUnicast` 在取消时会调用 `Operators.onDiscard` 来清理队列中的数据：

```java
// SinkManyUnicast 取消处理
if (cancelled) {
    Operators.onDiscard(t, actual.currentContext());           // 清理当前元素
    Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);  // 清理队列中所有元素
}
```

但 `onDiscard` 只在 Context 中注册了清理钩子时才生效。所以你需要：

```java
sink.asFlux()
    .contextWrite(ctx -> ctx.put("reactor.onDiscard.local", (Consumer<Object>) obj -> {
        if (obj instanceof Connection) {
            ((Connection) obj).close();  // 关闭连接
        }
    }))
    .subscribe(conn -> useConnection(conn));
```

⚠️ **踩坑提醒：如果你的 Sink 中流转的数据涉及需要关闭的资源（数据库连接、文件句柄、网络连接等），一定要注册 `onDiscard` 钩子。否则取消订阅时，队列里还没来得及消费的资源会永远泄漏。**

---

## 十三、归纳总结表格

### 表1：Sinks 类型选择指南

| 场景 | 推荐 API | 底层实现 | 特点 |
|------|---------|---------|------|
| 单值异步结果（类似 Future） | `Sinks.one()` | `SinkOneMulticast` | 只能推一个值，支持多订阅者 |
| 只通知完成/失败 | `Sinks.empty()` | `SinkEmptyMulticast` | 不携带值 |
| 单消费者 + 缓冲 | `many().unicast().onBackpressureBuffer()` | `SinkManyUnicast` | 有队列缓冲，只允许1个订阅者 |
| 单消费者 + 无缓冲 | `many().unicast().onBackpressureError()` | `SinkManyUnicastNoBackpressure` | 无队列，推不进去就报错 |
| 多消费者 + 缓冲 | `many().multicast().onBackpressureBuffer()` | `SinkManyEmitterProcessor` | 有队列，最慢消费者决定速度 |
| 多消费者 + 不等慢的 | `many().multicast().directBestEffort()` | `SinkManyBestEffort` | 跳过慢消费者 |
| 多消费者 + 历史回放 | `many().replay().limit(N)` | `SinkManyReplayProcessor` | 新订阅者能看到最近N条 |
| 性能极致要求 | `Sinks.unsafe().many().*` | 同上但无序列化包装 | 调用者自己保证线程安全 |

### 表2：EmitResult 失败类型与应对策略

| EmitResult | 含义 | 可恢复？ | 建议应对 |
|-----------|------|---------|---------|
| `OK` | 推送成功 | - | - |
| `FAIL_OVERFLOW` | 队列满了 | 取决于场景 | 降低推送速率 / 增加缓冲 / 丢弃 |
| `FAIL_CANCELLED` | 消费者已取消 | 否 | 停止推送，清理资源 |
| `FAIL_TERMINATED` | Sink 已终止 | 否 | 停止推送 |
| `FAIL_NON_SERIALIZED` | 并发竞争 | 是 | 用 `busyLooping` 短暂重试 |
| `FAIL_ZERO_SUBSCRIBER` | 无订阅者 | 取决于场景 | 等待订阅 / 缓冲 / 丢弃 |

### 表3：序列化机制对比

| 方式 | 创建方法 | 并发安全 | 性能 | 适用场景 |
|------|---------|---------|------|---------|
| 安全模式（默认） | `Sinks.many().*` | 自动保证 | 有 CAS 开销 | 多线程推送（大多数场景） |
| unsafe 模式 | `Sinks.unsafe().many().*` | 调用者负责 | 零额外开销 | 单线程推送 / 操作符内部 |

### 表4：核心源码类对照

| 源码类 | 作用 | 关键方法 |
|--------|------|---------|
| `Sinks` | API 入口，工厂方法 | `many()`, `one()`, `empty()`, `unsafe()` |
| `SinksSpecs` | Spec 构建器实现 | `wrapMany()`, 各种 Spec 实现类 |
| `SinkManySerialized` | 序列化包装层 | `tryAcquire()`, `tryEmitNext()` |
| `SinkManyUnicast` | 单订阅者实现 | `drain()`, `drainRegular()`, `checkTerminated()` |
| `SinkManyUnicastNoBackpressure` | 无队列单订阅者 | `tryEmitNext()` 直接调 `onNext` |
| `SinkManyEmitterProcessor` | 多订阅者实现 | `add()`, `drain()` 最小需求对齐 |
| `SinkManyReplayProcessor` | 回放实现 | `tryEmitNext()` 入 buffer + replay |
| `SinkOneMulticast` | 单值 Mono 实现 | `tryEmitValue()` |
| `SinkEmptyMulticast` | 空 Mono 实现 | `tryEmitEmpty()`, `tryEmitError()` |
| `EmitResult` | 推送结果枚举 | `isSuccess()`, `isFailure()`, `orThrow()` |
| `EmitFailureHandler` | 失败重试策略 | `FAIL_FAST`, `busyLooping(Duration)` |
