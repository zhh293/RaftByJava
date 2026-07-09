# 线程切换：subscribeOn 与 publishOn（易懂版）

> Reactor Core 源码解析系列 · 第 07 篇 · 易懂版
> 这是响应式编程里最容易让人迷糊的主题，没有之一。这篇我们用"快递分拣中心"的比喻把它彻底讲透：subscribeOn 决定"包裹从哪个仓库发出"，publishOn 决定"分拣员在哪个区域工作"。看完你就能一眼看懂任何线程切换代码到底在哪个线程上跑。

---

## 开场：一个让无数人翻车的问题

先来做个测试。下面这段代码，你能说出每一步跑在哪个线程上吗？

```java
System.out.println("调用前: " + Thread.currentThread().getName());

Flux.range(1, 3)
    .map(i -> {
        System.out.println("map: " + Thread.currentThread().getName());
        return i * 2;
    })
    .subscribeOn(Schedulers.boundedElastic())   // ①
    .publishOn(Schedulers.parallel())            // ②
    .map(i -> {
        System.out.println("第二个 map: " + Thread.currentThread().getName());
        return i + 1;
    })
    .subscribe(i ->
        System.out.println("subscribe: " + Thread.currentThread().getName()));
```

如果你现在不确定答案，别急，这很正常。看完这篇你会发现它其实有规律可循。我们先建立比喻，再讲原理，最后回来解这道题。

---

## 一、核心比喻：快递分拣中心

想象一条快递流水线：

- **仓库（源头）**：包裹最初从这里发出。对应 Reactor 里的 `Flux.range(...)`、`Flux.just(...)` 这些**数据源**。
- **一个个分拣工位**：包裹在流水线上依次经过好几个分拣员的手，每个分拣员做一件事（贴标签、称重、分区）。对应 `map`、`filter` 这些**操作符**。
- **收件人**：包裹最终送到的人。对应 `subscribe(...)` 里的消费逻辑。

现在关键来了，有两个"线程开关"：

### subscribeOn = 决定"包裹从哪个仓库发出"

`subscribeOn` 影响的是**源头在哪个线程上开始产生数据**。就像你决定"这批包裹从北京仓发货还是上海仓发货"。**一旦仓库定了，从仓库出来的包裹默认就一路在这条线路上跑**（除非中途遇到 publishOn 换线）。

### publishOn = 决定"从这里往后，分拣员在哪个区域工作"

`publishOn` 影响的是**它之后的操作符和消费者在哪个线程上处理数据**。就像流水线上有个"换区点"，过了这个点，包裹就被转移到另一个分拣区，由那个区的分拣员接手。

**一句话记住**：
- **subscribeOn 往上游看**（影响源头和它上面的一切）；
- **publishOn 往下游看**（影响它下面的一切，直到遇到下一个 publishOn 或流结束）。

---

## 二、为什么 subscribeOn 影响的是"上游"？这反直觉但有原因

新手最困惑的一点是：`subscribeOn` 明明写在链条中间，为什么它影响的是**它上面**的代码，而不是下面？

要理解这个，得先知道一个反直觉的事实：**订阅（subscribe）是从下往上传播的。**

### 订阅的方向：从下往上"点单"

当你调用 `.subscribe()` 时，这个"我要数据"的信号不是从上往下流，而是**从最下游往最上游一层层往上传**，就像点菜：

```
你(subscribe) → "我要数据" → 上一层操作符 → "我也要数据" → 再上一层 → ... → 源头
```

源头收到最顶层的"要数据"信号后，才开始**从上往下**吐数据。

所以 `subscribeOn` 做的事情是：**在"订阅信号往上传"的过程中，把'触发源头开始订阅'这个动作，扔到指定的线程上执行**。因为源头是在那个线程上被"唤醒"的，所以源头产生数据、以及数据往下流经各个操作符，默认就都在那个线程上了。

### 看源码：subscribeOn 到底做了什么

```java
@Override
public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
    Worker worker = scheduler.createWorker();   // 从调度器要一个"工人"（线程）

    SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(source, actual, worker, ...);
    actual.onSubscribe(parent);       // ① 先在【当前线程】让下游拿到订阅句柄

    worker.schedule(parent);          // ② 再把 parent 这个任务扔到【Worker 线程】上跑
    return null;
}
```

注意这个顺序：**先在当前线程通知下游（`onSubscribe`），再把真正的订阅动作调度到 Worker 线程**。而 `parent` 里面装的活是什么？看它的 `run()`：

```java
@Override
public void run() {
    THREAD.lazySet(this, Thread.currentThread());  // 记下"我这个工人是哪个线程"
    source.subscribe(this);   // 关键！在 Worker 线程上，去订阅源头
}
```

**`source.subscribe(this)` 这一行就是全部秘密**。它在 Worker 线程上触发了整个上游链的订阅，于是源头产生数据、数据往下流，全都发生在这个 Worker 线程上。

用快递比喻：`subscribeOn` 就是"把'去仓库发货'这个动作安排到某个仓库（线程）去做"。仓库一旦在那儿开始发货，包裹自然就从那条线路出来了。

---

## 三、为什么 publishOn 影响的是"下游"？

`publishOn` 的机制完全不同——它是一个真正的"传送带 + 换人"装置。

### 比喻：分拣区的换区点

`publishOn` 在流水线上放了一个**中转站（队列）**。上游的分拣员把包裹放到中转站的传送带上就走了；下游换了一批分拣员（另一个线程），从传送带上取包裹继续处理。

### 看源码：publishOn 的运作

`publishOn` 的核心是一个 `PublishOnSubscriber`，它同时是"订阅者"和"一个可运行的任务（Runnable）"。它的工作分两半：

**上半场——上游线程往队列里塞数据**：

```java
@Override
public void onNext(T t) {
    // ... 省略一些检查
    if (!queue.offer(t)) {   // 把数据放进队列（中转站的传送带）
        // 队列满了就报背压溢出错误
    }
    trySchedule(this, null, t);   // 触发一次"排空调度"
}
```

**下半场——Worker 线程从队列里取数据推给下游**：

```java
@Override
public void run() {   // 这段在 Worker 线程上执行
    // ...
    void runAsync() {
        for (;;) {
            while (e != r) {
                T v = q.poll();       // 从队列取出数据
                a.onNext(v);          // 在【Worker 线程】上推给下游
                // ...
            }
        }
    }
}
```

看到了吗？`onNext(t)` 是上游线程调用的（往队列塞），而 `a.onNext(v)`（推给下游）是在 Worker 线程 `run()` 里执行的。**数据经过队列这个中转站，就从上游线程"换手"到了 Worker 线程**。所以 publishOn 之后的所有操作符和 subscribe，都在这个新线程上跑。

### WIP 门控：为什么不是每来一个数据就调度一次线程？

这里有个精妙设计。你可能会想：既然要把数据从队列取出来推给下游，那每次 `onNext` 都调度一次 Worker 不就行了？

**不行，那样开销太大。** 想象每来一个包裹就叫一次分拣员过来，分拣员跑来跑去累死。正确做法是：**第一个包裹到了就叫分拣员来"值班"，之后来的包裹只在计数器上 +1，值班的分拣员会自己循环把队列清空，不用反复叫。**

这就是 WIP（Work-In-Progress，进行中的工作量）门控：

```java
void trySchedule(...) {
    if (WIP.getAndIncrement(this) != 0) {
        // 旧值不是 0，说明已经有分拣员在值班了，我只是 +1，不用再叫人
        return;
    }
    // 旧值是 0，说明没人值班，我来叫一个分拣员（调度 Worker）
    worker.schedule(this);
}
```

`getAndIncrement()` 返回加之前的旧值：
- 旧值 = 0：没人干活，需要调度 Worker 启动一轮"排空循环"；
- 旧值 ≠ 0：已经有人在干了，只需计数 +1，正在跑的循环会自动处理掉新数据。

这样保证**同一时刻只有一个排空任务在 Worker 上跑**，避免海量的任务提交开销。

### prefetch 与补货：流水线不能断粮

`publishOn` 还有个"预取（prefetch）+ 补货"机制。它默认一次向上游要 **256** 个元素预备着（prefetch），而不是要一个处理一个。

更妙的是补货时机：它**不等队列空了才补货**，而是消费到 **75%** 时就提前向上游再要一批：

```java
this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
// 默认 limit = prefetch - (prefetch >> 2) = prefetch 的 75%
```

**为什么提前到 75% 补货？** 如果等队列空了再补，会出现"下游在等数据、上游还没收到补货请求"的空窗期，吞吐量掉下来。提前补货让"上游生产"和"下游消费"形成流水线，队列始终保持部分填充，不断粮。这就像超市货架不等卖光了才补货，而是货架空了 1/4 就开始上货。

---

## 四、用一个完整的 WebFlux 场景把它们串起来

现在讲最实用的部分。一个典型的 WebFlux Controller 处理请求，理想的线程编排是这样的：

```java
@GetMapping("/user/{id}")
public Mono<UserVO> getUser(@PathVariable Long id) {
    return Mono.fromCallable(() -> blockingUserRepository.findById(id))  // 阻塞查库
        .subscribeOn(Schedulers.boundedElastic())   // ① 把查库这件"慢活"扔到弹性后厨
        .map(user -> {                                // ② 处理结果（跟着在 boundedElastic）
            return convertToVO(user);
        })
        .publishOn(Schedulers.parallel());            // ③ 后续处理切回计算线程
}
```

我们逐段拆解，看每一步在哪个线程：

### 阶段一：请求到达（Netty IO 线程）

WebFlux 底层是 Netty，请求进来时你在一个 **Netty 事件循环线程**（比如 `reactor-http-nio-2`）上。这个线程非常宝贵，绝对不能阻塞它，否则整个服务的吞吐量都会受影响。

### 阶段二：查库（subscribeOn 切到 boundedElastic）

`Mono.fromCallable(() -> blockingUserRepository.findById(id))` 里面是**阻塞的 JDBC 查询**。我们用 `subscribeOn(Schedulers.boundedElastic())`，让这个"发货动作"从**弹性后厨的线程**（比如 `boundedElastic-1`）发起。于是这个阻塞查询就在 boundedElastic 线程上执行，**Netty IO 线程被解放出来去处理别的请求**。

> ⚠️ 踩坑提醒：这里必须用 `subscribeOn` 而不是 `publishOn`。因为阻塞发生在**源头**（`fromCallable` 里的查库），我们要控制的是"源头在哪个线程执行"，这正是 subscribeOn 的职责。如果你用 publishOn，源头（`fromCallable`）还是会在订阅它的那个线程上执行，阻塞照样发生在错误的线程上。

### 阶段三：处理结果（map 跟随在 boundedElastic）

紧接着的 `.map(convertToVO)` 会在哪个线程？因为它上游是 boundedElastic（数据从那儿流下来），且中间没有 publishOn 换线，所以 **map 也在 boundedElastic 线程上执行**。这符合"包裹从哪个仓库发出，就一路在这条线路上跑"的规律。

### 阶段四：切回计算线程（publishOn 切到 parallel）

最后 `.publishOn(Schedulers.parallel())` 把后续处理（如果还有 map、以及最终框架返回响应的编码）切换到 **parallel 计算线程**。这样纯 CPU 的活由计算线程池承担，和阻塞的查库线程隔离开。

> ⚠️ 踩坑提醒：实战中你未必需要最后这个 publishOn。WebFlux 框架自己会在合适的地方处理线程。这里加上是为了演示"处理结果后切回计算线程"这个完整链路。真实项目里，最关键的就是**用 subscribeOn(boundedElastic()) 把阻塞查库隔离出去**这一步。

### 一张图看清整个线程流转

```
请求进来          查库(阻塞)              处理结果            后续CPU处理
Netty-IO线程  →  boundedElastic线程  →  boundedElastic线程  →  parallel线程
   (源头订阅被    (subscribeOn的效果:    (跟随上游,无换线)     (publishOn的效果:
    扔到别处)      源头在此线程执行)                            此后切到新线程)
```

---

## 五、最经典的坑：为什么多个 subscribeOn 只有最上游的生效？

这是面试高频题，也是实战中容易白写代码的地方。

### 现象

```java
Flux.range(1, 10)
    .subscribeOn(schedulerA)   // subscribeOn-1
    .map(x -> x * 2)
    .subscribeOn(schedulerB)   // subscribeOn-2
    .subscribe(System.out::println);
```

**结果：数据实际在 schedulerA 上产生，schedulerB 基本白写了。**

### 为什么？回到"订阅从下往上传"

我们跟着订阅信号走一遍（记住：订阅从下往上传，每个 subscribeOn 都会"把往上的订阅动作扔到自己的线程"）：

```
1. subscribeOn-2 先被订阅：
   - 它把"继续往上订阅"这个动作，扔到 schedulerB 的线程去做

2. 到了 schedulerB 线程，执行 source.subscribe()，往上碰到 subscribeOn-1：
   - subscribeOn-1 又把"继续往上订阅"这个动作，扔到 schedulerA 的线程去做

3. 到了 schedulerA 线程，执行 source.subscribe()，碰到 Flux.range：
   - Flux.range 在 schedulerA 线程上开始产生数据！
```

看明白了吗？**最靠近源头（最上游）的那个 subscribeOn，才是最后一个"接手"源头订阅的，所以它说了算。** 下游的 subscribeOn-2 只是把"去调用 subscribeOn-1"这个动作扔到了 schedulerB，然后 subscribeOn-1 又立刻把真正的活转走了。schedulerB 只是"中间打了个短工"，做了个无意义的线程跳转。

用快递比喻：你告诉北京仓（B）"帮我从上海仓（A）发货"。北京仓收到指令后打电话给上海仓，上海仓真正发了货。所以包裹是**从上海仓（A）发出**的，北京仓只是个传话的。**离货源最近的仓库决定了发货地。**

> ⚠️ 踩坑提醒：链条里写多个 subscribeOn 是常见的误用，只有最上游的生效，其余的纯属浪费（还多了线程跳转开销）。想控制源头线程，一个 subscribeOn 就够了，写在哪个位置都行（因为它总是影响到源头）。

### 对比：多个 publishOn 却都生效

```java
Flux.range(1, 10)
    .publishOn(schedulerA)    // publishOn-1: 让下面的 map 在 A 线程跑
    .map(x -> x * 2)
    .publishOn(schedulerB)    // publishOn-2: 让下面的 subscribe 在 B 线程跑
    .subscribe(System.out::println);
```

**这两个 publishOn 都生效**：`publishOn-1` 让 `map` 在 schedulerA 上执行；`publishOn-2` 让最终的 `subscribe` 在 schedulerB 上接收。

因为每个 publishOn 都是一个**独立的"队列 + 换区点"**，各自把数据换手到自己的线程。就像流水线上放了两个换区点，包裹依次经过两次换区，每次都真实地换了一批分拣员。

**记忆口诀**：
- **多个 subscribeOn：只有最上游生效**（都在抢"决定源头线程"，最靠源头的赢）。
- **多个 publishOn：每个都生效**（各管各的下游区段，层层换线）。

---

## 六、揭开一个隐藏细节：subscribeOn 为什么要"线程感知"地转发 request？

这是源码里一个很精妙、但容易被忽略的点。

前面说过，订阅时下游可能会立刻调 `request(n)`（"我要 n 个数据"）。问题是：这个 `request(n)` 应该在哪个线程上转发给上游？

如果 request 在**调用它的线程**（比如 main）上直接转发给上游，那上游数据源可能就在 **main 线程**上被触发产生数据了——这就绕过了 subscribeOn 指定的线程，功亏一篑！

所以 subscribeOn 做了"线程感知"处理：

```java
void requestUpstream(final long n, final Subscription s) {
    if (!requestOnSeparateThread || Thread.currentThread() == THREAD.get(this)) {
        s.request(n);   // 已经在 Worker 线程上了，直接转发
    }
    else {
        // 不在 Worker 线程上，把 request 也调度到 Worker 线程去
        worker.schedule(() -> s.request(n));
    }
}
```

翻译成人话：**如果发现"要数据"的请求是在别的线程发起的，就把这个请求也搬到 Worker 线程上去执行**，确保连"因请求而触发的数据生产"也发生在正确的线程上。这里的线程比较用的是 `==`（引用比较，判断是不是同一个线程对象），而不是 `equals`。

还有个细节：因为 `onSubscribe`（在当前线程）和 `worker.schedule`（在 Worker 线程）是异步的，下游可能在上游 Subscription 还没到达时就 `request` 了。所以 subscribeOn 用一个 `REQUESTED` 字段先**攒着**这些提前来的请求，等上游 Subscription 一到，再把攒的量一次性转发上去。

---

## 七、Mono 版本有什么不同？

`Mono` 最多只有一个值，所以它的 publishOn 实现更简单——**不需要队列、不需要 prefetch/补货**：

```java
@Override
public void onNext(T t) {
    value = t;
    trySchedule(this, null, t);
}

void trySchedule(...) {
    if (future != null) return;
    future = this.scheduler.schedule(this);   // 注意：直接用 scheduler.schedule，不是 worker.schedule
}
```

因为只有一个值，不需要 Worker 保证 FIFO 顺序（就一个值哪来的顺序问题），直接用 `scheduler.schedule()` 更轻量。

`MonoSubscribeOn` 的结构和 Flux 版基本一样，只有一个小区别：它总是把 request 调度到 Worker 线程（当不在 Worker 线程时），而 `FluxSubscribeOn` 有个 `requestOnSeparateThread` 开关可以控制这个行为。

---

## 八、回到开场那道题

现在我们回来解开头那道题：

```java
Flux.range(1, 3)
    .map(i -> { /* 第一个 map */ return i * 2; })
    .subscribeOn(Schedulers.boundedElastic())   // ①
    .publishOn(Schedulers.parallel())            // ②
    .map(i -> { /* 第二个 map */ return i + 1; })
    .subscribe(...);
```

逐步分析：

1. **`subscribeOn(boundedElastic())`**：影响源头，所以 `Flux.range` 在 **boundedElastic 线程**上产生数据。
2. **第一个 map**：它在 subscribeOn 上面、publishOn 下面……等等，注意它在**源头到 publishOn 之间**这一段，所以跟着源头，在 **boundedElastic 线程**上执行。
3. **`publishOn(parallel())`**：换区点，它之后的一切切换到 **parallel 线程**。
4. **第二个 map**：在 publishOn 之后，所以在 **parallel 线程**上执行。
5. **`subscribe`**：也在 publishOn 之后，所以在 **parallel 线程**上接收。

**答案**：
- 第一个 map → boundedElastic 线程
- 第二个 map → parallel 线程
- subscribe → parallel 线程

规律再重复一遍：**subscribeOn 管到源头，publishOn 管住它到下一个换线点之间的所有下游。**

---

## 九、归纳总表

### subscribeOn vs publishOn 核心对比

| 维度 | `subscribeOn` | `publishOn` |
|---|---|---|
| **快递比喻** | 决定包裹从哪个仓库发出 | 分拣区的换区点 |
| **影响方向** | 往上游（影响源头及以上） | 往下游（影响它之后到下一个换线点） |
| **切换的是什么线程** | 源头订阅/产生数据的线程 | 下游接收（onNext）的线程 |
| **多次使用** | 只有最靠源头的生效 | 每个都生效，层层换线 |
| **数据怎么传** | 直通转发，不经队列 | 经过队列中转，异步换手 |
| **有没有队列** | 无 | 有（SpscArrayQueue 或熔合队列） |
| **有没有 prefetch** | 无 | 有，默认 256 |
| **补货机制** | 无，直接转发 request | 消费到 75% 时向上游补货 |
| **实现类** | `FluxSubscribeOn`/`MonoSubscribeOn` | `FluxPublishOn`/`MonoPublishOn` |
| **典型用途** | 把阻塞源头挪到 boundedElastic | IO 回调线程切到计算线程、限制下游处理线程 |

### 实战决策表

| 你想干什么 | 用哪个 | 放在哪 |
|---|---|---|
| 让阻塞的源头（查库、读文件）在 boundedElastic 上执行 | `subscribeOn(boundedElastic())` | 链条里任意位置（反正影响源头） |
| 从某个点开始，把下游处理切到别的线程 | `publishOn(目标调度器)` | 想换线的那个位置 |
| 想分段用不同线程处理（IO段→计算段） | 多个 `publishOn` | 每个换线点各放一个 |
| 控制源头线程 | 一个 `subscribeOn` 就够 | 别写多个，浪费 |

### 三个必记结论

| 结论 | 一句话解释 |
|---|---|
| subscribeOn 影响上游 | 因为订阅从下往上传，它把"触发源头订阅"的动作扔到指定线程 |
| publishOn 影响下游 | 因为它用队列把数据换手到新线程，之后的操作符都在新线程跑 |
| 多个 subscribeOn 只有最上游生效 | 离源头最近的那个才是真正决定源头线程的 |

---

## 结语

线程切换的本质就一句话：**subscribeOn 决定"数据从哪个线程开始产生"，publishOn 决定"从这里往后数据在哪个线程被处理"。** 配合上一篇的调度器知识——阻塞源头用 `subscribeOn(boundedElastic())` 隔离出去，CPU 处理段用 `publishOn(parallel())` 换到计算线程——你就能精准控制响应式链路里每一段代码的执行线程了。

下一篇（第 08 篇）我们讲 Reactor 的性能黑魔法——**熔合（Fuseable）**：为什么相邻的 `map`、`filter` 能被"合并"成一个高效流程，以及 publishOn 传的那个神秘的 `THREAD_BARRIER` 标志到底防的是什么。你会发现它和这篇讲的线程切换紧密相关。
