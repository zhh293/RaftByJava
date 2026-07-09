# Scheduler 调度器体系（易懂版）

> Reactor Core 源码解析系列 · 第 06 篇 · 易懂版
> 用"餐厅厨房"的比喻，把 Reactor 的四种线程池讲清楚：它们各自适合干什么活、为什么 WebFlux 里查库必须用 boundedElastic、以及底层是怎么管理线程的。

---

## 开场：先想象一家餐厅

假设你开了一家餐厅，后厨有好几个不同的工作区：

- **快速热炒区**：几个灶台一字排开，几个灶台就配几个厨师，厨师手不停地颠勺，追求的是"火力全开、不浪费"。
- **弹性后厨**：平时人不多，一旦订单暴涨就临时加人手，订单少了闲着的人就下班回家，避免白发工资。
- **私房菜工作台**：只有一位大厨，所有菜都他一个人按顺序做，绝不会出现"两个人同时炒同一道菜导致乱套"。
- **顾客自己动手区**：干脆不请厨师，你（顾客）自己上手做，端到自己面前。

Reactor 的调度器（Scheduler）就是这四个"工作区"：

| 餐厅比喻 | Reactor 调度器 | 一句话定位 |
|---|---|---|
| 快速热炒区（厨师数=灶台数=CPU 核数） | `Schedulers.parallel()` | CPU 密集型的纯计算 |
| 弹性后厨（忙加人、闲裁人） | `Schedulers.boundedElastic()` | 阻塞式 I/O（查库、读文件、调 HTTP） |
| 私房菜（只有一位大厨） | `Schedulers.single()` | 需要严格串行、单线程顺序执行 |
| 顾客自己动手 | `Schedulers.immediate()` | 不切线程，直接在当前线程干 |

这篇文章就是要把这四个"工作区"讲透，最后再回答那个最扎心的实战问题：**为什么在 WebFlux 里查数据库必须用 boundedElastic，用 parallel 会出大事？**

---

## 一、为什么需要"调度器"这个东西？从一个真实场景说起

### 问题：响应式代码里，"我这段逻辑到底跑在哪个线程上？"

先看一段最朴素的响应式代码：

```java
Flux.range(1, 5)
    .map(i -> i * 2)
    .filter(i -> i > 4)
    .subscribe(System.out::println);
```

**问：这段代码里的 `map`、`filter`、`subscribe` 分别跑在哪个线程上？**

答：**全部跑在调用 `subscribe()` 的那个线程上**（默认情况下，Reactor 不会主动切换线程）。也就是说，如果你在 main 线程调用它，那从头到尾都是 main 线程干活。

这就带来一个问题。假设 `map` 里干的是一件**特别费 CPU** 的活（比如复杂加密），你希望它多个核并行；或者 `map` 里干的是一件**会阻塞**的活（比如查数据库要等 200ms），你不希望它把当前线程卡死。这时候你就需要一个"工具"来把某段逻辑**扔到另一批线程上去执行**。

这个工具就是 **Scheduler**。它就是响应式世界里的"线程池抽象"。

> ⚠️ 踩坑提醒：很多人以为"用了 Reactor 就是异步的、就自动多线程了"。**大错特错。** Reactor 默认是单线程的，同步执行。只有你显式用 `subscribeOn` / `publishOn` 配合 Scheduler，才会发生线程切换。第 07 篇会详细讲这两个操作符。

---

## 二、Scheduler 是什么？它长什么样？

### 2.1 先看它的"招牌"（接口定义）

Scheduler 本质上是一个接口，你可以把它理解成"一个能帮你跑任务的线程管理器"。它的核心能力就三样：

```java
public interface Scheduler extends Disposable {

    // 1. 给我一个任务，你找个线程帮我跑（可能立刻，可能延迟）
    Disposable schedule(Runnable task);

    // 2. 延迟一段时间后再跑（不是所有调度器都支持）
    default Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        throw Exceptions.failWithRejectedNotTimeCapable();
    }

    // 3. 创建一个"工人(Worker)"，工人保证交给它的活按顺序一件件干
    Worker createWorker();

    default void init() { start(); }
    default void dispose() { }
}
```

这里有两个设计细节值得用大白话解释一下。

**为什么 `schedule(task, delay, unit)`（延迟执行）默认是抛异常的？**

因为不是所有"工作区"都支持"过一会儿再干"。比如"顾客自己动手区"（immediate），你自己站在那儿，想延迟执行只能干等着（`Thread.sleep`），那不就把你自己卡住了嘛。对一个号称"非阻塞"的框架来说，这是不能接受的。所以设计者干脆让不支持延迟的调度器保持默认——你一调用它就明确告诉你"我不支持定时"，比硬塞一个假的实现要诚实。

**为什么一定要有 `createWorker()`（创建工人）？**

因为很多操作符（比如 `publishOn`）需要**保证数据按顺序投递**。想象快递分拣：包裹必须按 1、2、3 的顺序处理，不能乱。`schedule()` 只是"随便找个线程跑一下"，不保证顺序；而 **Worker（工人）保证交给同一个工人的活是排队一件件干的（FIFO）**。这对数据流的有序性至关重要。

### 2.2 Worker（工人）：真正干活的单元

```java
interface Worker extends Disposable {
    Disposable schedule(Runnable task);   // 交给我，我排队干
    // ...
    // dispose() 干完了，把我这个工人辞退，释放占用的资源
}
```

**Scheduler 和 Worker 的关系，就像"工作区"和"工人"**：一个工作区（Scheduler）可以派出多个工人（Worker），每个工人绑定一处执行资源。工人下班（`dispose()`）只释放自己占的东西；整个工作区关门（Scheduler 的 `dispose()`）才释放全部资源。

---

## 三、四种厨房逐个看：怎么用、什么脾气、底层怎么实现

### 3.1 immediate()：顾客自己动手，零开销

**是什么**：`Schedulers.immediate()` 根本不切线程。你给它一个任务，它就在**当前线程**上直接 `task.run()` 跑掉，跑完拉倒。

**怎么用**：一般用于测试，或者当某个 API 要求你传一个 Scheduler、但你其实不想切线程的时候，传它就等于"什么都不做"。

**底层长这样**（极简）：

```java
final class ImmediateScheduler implements Scheduler {
    @Override
    public Disposable schedule(Runnable task) {
        task.run();          // 就地执行，不找别的线程
        return FINISHED;     // 返回一个"已完成"的空对象，因为任务已经跑完了，没啥可取消的
    }
    @Override
    public void dispose() { /* 啥也不干 */ }
}
```

**它是一个全局单例，永远存活**。有意思的是：Scheduler 本身无状态（因为它是永久单例），但它派出的 Worker 有一个 `shutdown` 标志——工人可以"下班"，但整个工作区（单例）不会关门。

> ⚠️ 踩坑提醒：immediate() 不支持延迟调度。因为要在当前线程实现延迟，只能 `Thread.sleep()`，这会把你的线程直接阻塞死。所以你对它调 `schedule(task, delay, unit)` 会直接抛异常。

### 3.2 single()：只有一位大厨的私房菜

**是什么**：`Schedulers.single()` 底层就是**一个只有 1 个线程的线程池**。所有任务都交给这唯一的线程，按顺序一件件执行。这就像 UI 框架里的"事件派发线程"——所有事件都在一个线程上串行处理，天然保证顺序，绝无并发问题。

**怎么用**：适合那些**必须严格串行**、且不想被并发困扰的场景，比如维护一个全局计数器、写一个不加锁的日志文件。

**底层核心**：

```java
// 创建一个核心=最大=1 的定时线程池
ScheduledThreadPoolExecutor e =
    (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, this.factory);
e.setRemoveOnCancelPolicy(true);   // 关键：任务被取消后立刻从队列里删掉
e.setMaximumPoolSize(1);
```

这里的 `setRemoveOnCancelPolicy(true)` 很重要。打个比方：如果你点了一份"1 小时后送达"的外卖，然后又取消了。如果不设这个策略，这份被取消的订单会**一直赖在队列里，直到 1 小时后才被拿出来发现"哦已经取消了"**。在 `timeout` 这种高频取消的场景下，队列会被这些"僵尸订单"塞爆，导致内存泄漏。设了它，取消即删除，干干净净。

**一个容易误会的点**：`single()` 全局共享同一个线程。你调 `createWorker()` 创建再多工人，它们背后都是**同一位大厨**。如果你真的想要多个各自独立的单线程，得用 `Schedulers.newSingle()` 手动创建多个实例。

### 3.3 parallel()：快速热炒区，厨师数 = 灶台数（CPU 核数）

**是什么**：`Schedulers.parallel()` 底层是 **N 个单线程池组成的固定数组**，N 默认等于 **CPU 核数**。

**为什么线程数正好等于 CPU 核数？** 这就是"热炒区"的精髓：对于**纯计算、不阻塞**的活，线程数等于 CPU 核数时效率最高。因为每个核同时只能真正跑一个线程，你开再多线程也只是徒增线程切换的开销（就像灶台只有 4 个，你叫来 40 个厨师，剩下 36 个只能互相挤着抢灶台，反而更慢）。

**怎么用**：CPU 密集型计算——加密解密、图像处理、大量数学运算、序列化等。

**底层的"轮流分配"机制（Round-Robin）**：

```java
ScheduledExecutorService pick() {
    // ...
    int idx = roundRobin;
    if (idx == n) { idx = 0; roundRobin = 1; }
    else { roundRobin = idx + 1; }
    return a.currentResource[idx];   // 轮流返回第 0、1、2... 个线程池
}
```

每次要分配线程，就轮流指派给第 0 个、第 1 个、第 2 个……循环往复，让负载尽量均匀。

**有意思的细节**：这个 `roundRobin` 计数器**没加锁、也不是 volatile**。源码注释明说了："这里的竞态无所谓，反正谁拿到哪个线程本来就是随机的"。最坏情况就是两个工人碰巧拿到同一个线程，任务照样能跑，只是均匀性稍微差一点点。为这种无害的竞态加锁反而是浪费。这是一个"该偷懒时就偷懒"的务实设计。

### 3.4 boundedElastic()：弹性后厨，忙时加人、闲时裁人

这是四个里面**最复杂、也是 WebFlux 里最常用**的一个。

**是什么**：一个**动态伸缩的线程池**——需要时创建新线程（最多到上限），线程空闲久了（默认 60 秒）就回收掉。它专门为**阻塞操作**设计。

**三个关键默认值**：

```java
// 1. 线程数上限 = 10 × CPU 核数
DEFAULT_BOUNDED_ELASTIC_SIZE = 10 * Runtime.getRuntime().availableProcessors();

// 2. 每个线程的排队上限 = 10 万
DEFAULT_BOUNDED_ELASTIC_QUEUESIZE = 100000;

// 3. 线程空闲超过 60 秒就回收（TTL）
DEFAULT_TTL_SECONDS = 60;
```

**为什么线程数是 CPU 核数的 10 倍，而不是像 parallel 那样正好等于核数？**

因为阻塞操作的特点是"大部分时间在**等**，没在真正用 CPU"。比如查数据库，线程发出 SQL 后就干等 200ms，这 200ms 里 CPU 是闲着的。既然 CPU 闲着，那我就可以多开几倍的线程去处理更多并发的阻塞请求。10 倍是一个经验值：足够扛住常见的阻塞并发，又不至于无限膨胀。

**为什么要有"上限"？（bounded 的由来）**

老版本 Reactor 有个 `elastic()` 调度器，是**无上限**的。结果如果上游疯狂产生阻塞任务，线程数会无限增长，最终把内存撑爆（OOM）。所以新版设了上限，这就是名字里 "bounded"（有界）的含义——它是"弹性但有天花板"的。

> ⚠️ 踩坑提醒：如果你手动把 boundedElastic 的 size 设成 `Integer.MAX_VALUE`，就等于把这道保护墙拆了，退化成老的无界 elastic，随时可能 OOM。别这么干。

**为什么每个线程排队上限是 10 万？**

这是背压安全的最后一道防线。当所有线程都忙不过来，新任务只能排队。队列如果无上限，内存就会一直涨。10 万是个折中：大部分应用够用，不会因为瞬时高峰就拒绝任务；又不至于大到内存泄漏时还傻乎乎地一直收。超过 10 万就直接抛 `RejectedExecutionException` 报警，让你及早发现问题。

**底层的"三级选人策略"**（pick 方法，也就是"派活给哪个线程"）：

弹性后厨来了新活，经理（pick 方法）按这个优先级找人：

1. **先看有没有空闲的老员工**（idleQueue 里有没有闲着的线程）——有就直接复用，省下招人成本。
2. **没空闲的，但还没到人数上限**——那就招一个新员工（创建新线程）。
3. **人也招满了（到上限了）**——那就从现有的忙人里，挑一个**手上活最少**的（`markCount` 最小），把新活也塞给他。

这个逻辑用大白话就是：**能复用就复用，不够就扩容，扩容到顶了就找最闲的人分担**。

**"闲人裁员"机制（TTL 驱逐）**：

```java
boolean tryEvict(long now, long ttlMillis) {
    long idleSince = this.idleSinceTimestamp;   // 从什么时候开始闲着
    if (idleSince < 0) return false;
    if (now - idleSince >= ttlMillis) {          // 闲得超过 60 秒了
        if (MARK_COUNT.compareAndSet(this, 0, EVICTED)) {  // 用 CAS 抢占"裁员"资格
            executor.shutdownNow();              // 让这个线程下班
            return true;
        }
    }
    return false;
}
```

有一个专门的"裁员线程"（evictor），每隔 60 秒巡视一遍空闲队列，把闲了超过 60 秒的线程关掉，释放资源。用 CAS `(0 → EVICTED)` 保证"正准备裁的人"和"刚好又被派活的人"不会冲突。

**任务队列限制怎么保证不超 10 万的？（一个精妙的并发细节）**

```java
@Override
public synchronized <T> Future<T> submit(Callable<T> task) {
    ensureQueueCapacity(1);   // 先检查队列还有没有空位
    return super.submit(task);
}
```

注意这个 `synchronized`。为什么要加锁？因为"检查队列大小"和"提交任务"这两步之间如果不上锁，可能出现两个线程同时检查（都以为还有空位），然后都提交，结果就超了。加锁保证"检查—提交"是一个原子动作，绝不会超限。

---

## 四、灵魂拷问：为什么 WebFlux 里查库必须用 boundedElastic，不能用 parallel？

这是本篇最重要的实战问题，也是无数人踩过的坑。我们先看错误示范，再讲原理。

### 4.1 一个会出大事的写法

```java
// ❌ 错误示范：把阻塞的 JDBC 查询放到 parallel 上
public Mono<User> getUser(Long id) {
    return Mono.fromCallable(() -> jdbcUserRepository.findById(id))  // 这是阻塞的 JDBC 调用
               .subscribeOn(Schedulers.parallel());   // ❌ 用错调度器了！
}
```

### 4.2 为什么这样会出大事？

回到餐厅比喻。`parallel()` 是"快速热炒区"，它的厨师数 = 灶台数 = **CPU 核数**（假设 4 核，就只有 4 个线程）。这几个线程是整个应用**共享**的，用来处理所有 CPU 密集型的活，包括 Netty 事件循环之外的响应式计算。

现在你让这 4 个宝贵的线程去干"查数据库"这种**会阻塞 200ms** 的活。会发生什么？

- 线程 1 查库，卡住 200ms，什么都干不了；
- 线程 2、3、4 也去查库，全部卡住；
- **这 4 个线程全被阻塞占满了**，此时任何真正的 CPU 计算任务都没线程可用；
- 更糟的是，如果并发查询请求一多，大家都在排队等这 4 个线程，整个应用的吞吐量断崖式下跌，甚至**假死**。

这就好比：热炒区就 4 个灶台 4 个厨师，你非要让他们去"炖一锅要炖 3 小时的汤"（阻塞操作）。结果 4 个厨师全去守着炖汤锅发呆，热炒区彻底瘫痪，后面所有炒菜订单全堆积。

### 4.3 为什么 boundedElastic 就没这个问题？

因为"弹性后厨"就是**专门为"要长时间等待的活"设计的**：

1. **线程多**（10 倍 CPU 核数），扛得住大量并发的阻塞等待；
2. **会弹性扩容**，忙的时候临时加人，不会因为几个慢查询就把整个应用拖垮；
3. **和 parallel 隔离**，查库的阻塞不会污染到 CPU 计算的线程池；
4. **线程不标记为 NonBlocking**（下面会讲），允许在上面执行阻塞代码。

所以正确写法是：

```java
// ✅ 正确示范：阻塞查询放到 boundedElastic
public Mono<User> getUser(Long id) {
    return Mono.fromCallable(() -> jdbcUserRepository.findById(id))
               .subscribeOn(Schedulers.boundedElastic());   // ✅ 阻塞活交给弹性后厨
}
```

### 4.4 Reactor 甚至会主动阻止你在错误的线程上阻塞

Reactor 有个**空标记接口** `NonBlocking`：

```java
public interface NonBlocking { }   // 空的，什么方法都没有，纯粹是个"标签"
```

`parallel()` 和 `single()` 创建的线程都实现了这个接口（叫 `NonBlockingThread`），相当于给这些线程贴了个标签："我是非阻塞线程，别在我身上干阻塞的活"。

```java
public static boolean isNonBlockingThread(Thread t) {
    return t instanceof NonBlocking || nonBlockingThreadPredicate.test(t);
}
```

当你在这些线程上调用 `Mono.block()`、`Flux.blockFirst()` 这类阻塞 API 时，Reactor 会检查当前线程是不是贴了 `NonBlocking` 标签，如果是，**直接抛异常**，从源头拦住你的错误代码。

> ⚠️ 踩坑提醒：如果你在 WebFlux 的 Netty 事件循环线程或 parallel 线程上不小心调了 `.block()`，会看到 `IllegalStateException: block()/blockFirst()/blockLast() are blocking, which is not supported in thread ...`。看到这个报错，就说明你把阻塞操作放错线程了，赶紧用 `subscribeOn(Schedulers.boundedElastic())` 把它挪走。

**而 boundedElastic 的线程故意不贴这个标签**：

```java
new ReactorThreadFactory(name, ..., /* rejectBlocking = */ false, ...)
//                                                          ↑ 这个 false 表示：允许阻塞
```

因为它天生就是给阻塞操作用的，贴了标签反而误伤。

---

## 五、幕后管家：这些调度器是怎么被创建和复用的？

### 5.1 CachedScheduler：全局共享，不许你随便关

你每次调 `Schedulers.parallel()`，拿到的**都是同一个实例**（缓存的单例）。这靠一个叫 `CachedScheduler` 的包装类实现，它有个反直觉的设计——`dispose()` 是空操作：

```java
@Override
public void dispose() {
    // 故意什么都不做！
}
```

**为什么关不掉？** 因为它是全局共享的。如果谁都能把它关掉，那别的正在用这个调度器的代码就全崩了（收到 `RejectedExecutionException`）。所以它保护自己：只有通过 `Schedulers.shutdownNow()` 这种全局操作才能真正关闭。这叫"受保护的单例"。

用大白话：这个公共厨房是大家共用的，不允许某个厨师说走就把总闸拉了，得餐厅老板统一决定关门。

### 5.2 状态管理靠 CAS，不靠锁

创建调度器时用了 `AtomicReferenceFieldUpdater` + CAS（比较并交换）来保证多线程环境下"只有一个线程能成功初始化"。抢输的那个线程会把自己多创建的线程池立刻关掉，避免资源泄漏。这套机制在四个调度器里都能看到，好处是无锁、高效。

### 5.3 Java 21 虚拟线程（了解即可）

Reactor 3.6.0+ 支持让 boundedElastic 跑在虚拟线程上（每个任务一个虚拟线程）。虚拟线程不绑定操作系统线程，阻塞时几乎不浪费真实线程资源，所以线程上限可以设得很大。开启方式：设置系统属性 `-Dreactor.schedulers.defaultBoundedElasticOnVirtualThreads=true`（且运行在 JDK 21+）。在低于 JDK 21 的环境开这个开关，会打印警告并回退到普通的 boundedElastic。

---

## 六、容器环境下的一个大坑（附赠彩蛋）

> ⚠️ 踩坑提醒：`parallel()` 的线程数默认取 `Runtime.getRuntime().availableProcessors()`。但在**容器（Docker/K8s）**里，老版本 JVM 可能读到的是**宿主机的核数**（比如宿主机 64 核），而不是容器实际分到的 CPU 配额（比如 2 核）。结果就是你以为开了 2 个线程，实际开了 64 个，线程疯狂抢那 2 核 CPU，上下文切换开销爆炸。
>
> 解决办法：用系统属性手动覆盖 `-Dreactor.schedulers.defaultPoolSize=2`。boundedElastic 同理有 `-Dreactor.schedulers.defaultBoundedElasticSize=xxx`。

---

## 七、归纳总表

### 四种调度器速查

| 维度 | `immediate()` | `single()` | `parallel()` | `boundedElastic()` |
|---|---|---|---|---|
| **餐厅比喻** | 顾客自己动手 | 只有一位大厨的私房菜 | 快速热炒区 | 弹性后厨 |
| **实现类** | `ImmediateScheduler` | `SingleScheduler` | `ParallelScheduler` | `BoundedElasticScheduler` |
| **线程数** | 0（当前线程） | 1 | CPU 核数 | 0 ~ 10×CPU 核数 |
| **线程会不会回收** | 无 | 永久存活 | 永久存活 | 空闲 60 秒回收 |
| **是否 NonBlocking 线程** | 不涉及 | 是（禁止阻塞） | 是（禁止阻塞） | 否（允许阻塞） |
| **支持延迟/定时** | 否 | 是 | 是 | 是 |
| **任务排队上限** | 无 | 无 | 无 | 每线程 10 万 |
| **工人分配方式** | 每次新建 | 全部共享同一线程 | 轮流分配到 N 个线程 | 空闲优先→新建→挑最闲的 |
| **适合干什么** | 测试、不想切线程 | 严格串行、单线程顺序 | CPU 密集计算 | 阻塞 I/O（查库、读文件、调 HTTP） |
| **千万别拿它干什么** | 别指望它并发 | 别放耗时任务（会堵死唯一线程） | 别放阻塞操作（会拖垮 CPU 池） | 别放纯 CPU 密集计算（浪费弹性能力） |

### 关键决策口诀

| 你的活是什么性质 | 用哪个调度器 |
|---|---|
| 纯 CPU 计算，不等待任何东西 | `parallel()` |
| 会阻塞等待（数据库、文件、老式 HTTP 客户端、`Thread.sleep`） | `boundedElastic()` |
| 必须严格按顺序、单线程执行 | `single()` |
| 就想在当前线程跑，不切换 | `immediate()` |

### 三个最容易踩的坑

| 坑 | 后果 | 正确做法 |
|---|---|---|
| 把阻塞查询放到 `parallel()` | CPU 线程池被占满，应用假死 | 改用 `boundedElastic()` |
| 在 Netty/parallel 线程上调 `.block()` | 抛 `IllegalStateException` | 用 `subscribeOn(boundedElastic())` 把阻塞挪走 |
| 容器里不设 `defaultPoolSize` | 线程数按宿主机核数创建，过度竞争 | 用系统属性手动指定 |

---

## 结语

记住这一句话你就掌握了 80%：**CPU 密集用 parallel，阻塞等待用 boundedElastic，串行用 single，不切线程用 immediate。** 而在 WebFlux 里，只要你的操作"会等"（查库、读文件、调外部服务的阻塞客户端），就无脑 `subscribeOn(Schedulers.boundedElastic())`，把这些慢活关进专门的弹性后厨，别去污染宝贵的 CPU 热炒区。

下一篇（第 07 篇）我们会讲清楚 `subscribeOn` 和 `publishOn` 这两个"线程切换开关"到底怎么配合调度器工作——这是响应式编程里最容易让人迷糊的地方。
