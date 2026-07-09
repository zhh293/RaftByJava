# 熔合优化机制 Fuseable（易懂版）

> Reactor Core 源码解析系列 · 第 08 篇 · 易懂版
> 用"工厂流水线优化"的比喻，讲清楚 Reactor 是怎么把相邻的 map、filter 悄悄"合并"成一条高效流水线的。这是 Reactor 性能远超普通 Stream 的核心秘密之一，也是它内部最"黑魔法"的部分。

---

## 开场：一个你平时看不见、却一直在帮你省钱的优化

你写下这么一段代码：

```java
Flux.just(1, 2, 3, 4, 5)
    .map(x -> x * 2)
    .filter(x -> x > 4)
    .subscribe(System.out::println);
```

你以为它是这样跑的：源头把 1 交给 map，map 变成 2 交给 filter，filter 判断后……每一步都规规矩矩地"交接"。

**但实际上，Reactor 在背后偷偷做了一件事：它把 map 和 filter"熔合"成了一条流水线，中间的所有交接手续全省了。** 这个优化叫 **Fuseable（可熔合）**，它能让纯内存操作的吞吐量提升 2~5 倍。

这篇我们就来揭开这个黑魔法。先讲清楚"正常模式有多啰嗦"，再看"熔合怎么省事"，最后讲那几个神秘的常量和 `ConditionalSubscriber` 优化。

---

## 一、核心比喻：工厂流水线的两种模式

### 正常模式（NONE）：每个工位都要"签收"

想象一条流水线，产品要经过 3 个工位（A→B→C）。**正常模式下**，每个工位都要走一套完整的手续：

1. 上一个工位把产品**推**给我（`onNext`）；
2. 我处理完，**推**给下一个工位；
3. 如果我这个工位不要这个产品（比如 filter 过滤掉了），我得**打电话给上游"再给我一个"**（`request(1)`）。

这就像快递每经过一个中转站都要**扫码签收**一次，还要**打电话确认**。产品少还好，产品有一百万个的时候，光是"签收 + 打电话"的手续开销就非常可观。

### SYNC 熔合：把相邻工位合并成一个

**SYNC 熔合**的思路是：既然这几个工位都在同一条线上、同一个工人、同一个节奏，那干脆**把它们合并成一个大工位**。原材料直接从第一个工位"流"到最后一个工位，中间**不需要任何签收和确认电话**。

具体怎么做？**改"推"为"拉"**。最下游的消费者说"我要一个产品"，然后它**主动去最上游拉**（`poll`）：

```
消费者.poll()  →  C工位.poll()  →  B工位.poll()  →  A工位(源头).poll() → 返回原始数据
                                                                            ↓
                返回最终结果  ←  C处理  ←  B处理  ←  A提供原料
```

一次 `poll()` 调用，就沿着链条把数据同步拉了上来，一路加工好返回。**没有 onNext 的层层推送，没有 request 的往返电话。**

### ASYNC 熔合：工位之间放一条共享传送带

**ASYNC 熔合**用于"数据是异步到达"的场景（比如 publishOn）。它的思路是：工位之间放一条**共享的传送带（队列）**。上游把产品放上传送带就走人，下游自己来传送带上取。

关键在于"**共享**"——不需要每个环节都搞一条自己的传送带互相倒腾，大家用同一条。上游只需喊一声"传送带上有货了"（`onNext(null)` 通知），下游就自己来 `poll` 取货。

---

## 二、为什么这个优化这么值钱？先算一笔账

拿一个真实的百万级管道来算：

```java
Flux.range(1, 1_000_000)
    .map(i -> i * 2)
    .filter(i -> i % 3 == 0)   // 大约 2/3 的元素被过滤掉
    .subscribe(...);
```

### 不熔合（NONE）的开销

- **100 万个元素 × 2 层操作符 = 200 万次 `onNext` 虚方法调用**；
- 约 **66.7 万次 `request(1)` 原子操作**（每被过滤一个就要向上游"再要一个"，2/3 被过滤）；
- 每次 request 至少涉及一次原子操作（`getAndAdd` 或 `compareAndSet`），这些在多核下是有真实成本的（缓存一致性、内存屏障）。

### SYNC 熔合的开销

- 数据通过 `poll()` 链拉取，**0 次 onNext 推送**；
- **0 次 request 原子操作**（SYNC 模式数据全在源头，不需要 request 记账）；
- filter 跳过不匹配元素只是内部 `continue`，**不产生跨工位的 request 往返**。

**结论**：微基准测试里，SYNC 熔合对纯内存操作能带来 **2~5 倍**吞吐提升。此外还省内存——不熔合时 publishOn 要单独分配一个 256 槽的队列，熔合后可以复用上游的队列，省掉这笔分配。

---

## 三、熔合的"暗号"：五个常量（Fuseable 的词汇表）

工位之间要协商"咱们能不能熔合、用哪种熔合"，得先有一套暗号。这套暗号就是 `Fuseable` 接口里的五个常量：

```java
public interface Fuseable {
    int NONE           = 0;      // 000  不熔合 / 熔合被拒绝
    int SYNC           = 1;      // 001  同步熔合：数据已就绪，直接 poll
    int ASYNC          = 2;      // 010  异步熔合：数据异步到达，共享队列
    int ANY            = 3;      // 011  两种都行，你上游看着办（只用于请求方）
    int THREAD_BARRIER = 0b100;  // 100  线程边界标志，附加用的
}
```

用大白话解释：

| 常量 | 值 | 含义（人话） |
|---|---|---|
| `NONE` | 0 | "不熔合" 或 "我拒绝你的熔合请求" |
| `SYNC` | 1 | "数据已经全在这儿了，你随时来拉" |
| `ASYNC` | 2 | "数据陆续到，但咱共用一条传送带" |
| `ANY` | 3 | "SYNC 或 ASYNC 我都能接受，你定" |
| `THREAD_BARRIER` | 4 | "注意！我会换到别的线程消费你的数据" |

### 几个巧妙的位运算设计

**为什么 `ANY = 3` 正好等于 `SYNC | ASYNC`？**

不是巧合。`ANY`（3，二进制 011）在位运算上就是 `SYNC`（001）和 `ASYNC`（010）的按位或。所以当请求方说 `requestFusion(ANY)`（"我都行"），上游就用 `(mode & SYNC) != 0` 或 `(mode & ASYNC) != 0` 来判断该给哪种。位运算天然表达了"包含"关系。

**为什么 `THREAD_BARRIER` 单独占一个 bit（100）？**

因为它**不是一种熔合模式，而是一个附加标签**。它可以和别的模式"贴"在一起用。比如 publishOn 会请求 `ANY | THREAD_BARRIER`（= 111 = 7），意思是"我 SYNC/ASYNC 都行，但**提醒你一句：我会在别的线程上消费数据**"。因为它是独立 bit，所以擦掉它、只看基本模式非常容易：

```java
int basicMode = mode & ~THREAD_BARRIER;   // 擦掉第 3 位，只留低 2 位判断基本模式
```

标志位和模式位互不干扰，这是位运算设计的优雅之处。

---

## 四、SYNC 熔合的完整故事：以 FluxArray.map().filter() 为例

### 4.1 协商阶段：在 onSubscribe 时"对暗号"

熔合的协商发生在订阅时（`onSubscribe`）。核心方法叫 `requestFusion(int mode)`——请求方传入想要的模式，被请求方返回它实际能支持的模式。

我们看 `Flux.just(1,2,3,4,5).map(...).filter(...)` 的协商过程（记住订阅从下往上传）：

```
1. 最下游订阅者请求 SYNC 熔合
   → 问 FilterFuseable: requestFusion(SYNC)?
     → Filter 说"我这没有 THREAD_BARRIER，往上问问 Map"
     → 问 MapFuseable: requestFusion(SYNC)?
       → Map 也没 THREAD_BARRIER，继续往上问 ArraySubscription
       → 问 ArraySubscription: requestFusion(SYNC)?
         → 数组数据全在内存里，返回 SYNC ✓
       → Map 记下"我上游是 SYNC 模式"，返回 SYNC
     → Filter 记下"我上游是 SYNC 模式"，返回 SYNC
   → 协商成功，整条链进入 SYNC 熔合模式
```

**中间操作符（map、filter）的策略是"透传"**：只要请求里没有 `THREAD_BARRIER`，它们就把请求原样传给上游，上游支持什么它们就支持什么。看源码：

```java
// MapFuseableSubscriber.requestFusion
@Override
public int requestFusion(int requestedMode) {
    if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
        return Fuseable.NONE;   // 带线程边界标志？我拒绝熔合（下面第六节详解为什么）
    }
    m = s.requestFusion(requestedMode);   // 否则透传给上游
    sourceMode = m;   // 记住结果，后面 poll/onNext 根据它走不同分支
    return m;
}
```

而**源头**（FluxArray 的 `ArraySubscription`）实现了 `SynchronousSubscription`，它的默认逻辑是"只要你要 SYNC，我就给 SYNC"：

```java
default int requestFusion(int requestedMode) {
    if ((requestedMode & Fuseable.SYNC) != 0) {
        return Fuseable.SYNC;
    }
    return NONE;
}
```

### 4.2 数据阶段：一次 poll 拉通全链

协商成功后，消费者不再等着被 `onNext` 推数据，而是**主动 `poll()`**。这一个 poll 调用会沿着链条一路往上拉：

```java
subscriber.poll()
  → FilterFuseableSubscriber.poll()
    → for(;;) {                          // filter 会循环，直到找到符合条件的
        T v = s.poll();                  // 向上游 map 拉
        → MapFuseableSubscriber.poll()
          → T raw = s.poll();            // 向上游 array 拉
          → ArraySubscription.poll()     // return array[index++]  比如返回 1
          → return mapper.apply(1) = 2;  // map 就地加工成 2
        if (predicate.test(2)) → false   // filter 判断 2 > 4? 不满足
        continue;                         // 直接 continue 继续拉下一个，不用打电话给上游！
        // ... 继续 poll 到 3，map 成 6，filter 判断 6 > 4 满足，返回 6
      }
```

看这里的关键：**filter 过滤掉不满足的元素，只是一个 `continue`，在同一个循环里继续 poll**，完全不需要像正常模式那样调 `s.request(1)` 去跟上游"再要一个"。这就是省下几十万次 request 原子操作的地方。

各个工位的 `poll()` 实现：

```java
// 源头：数组迭代器
public T poll() {
    if (index != array.length) return array[index++];   // 返回下一个，推进指针
    return null;   // 到头了，返回 null 表示"没货了 = 完成"
}

// map：拉 + 加工
public R poll() {
    T v = s.poll();
    if (v != null) return mapper.apply(v);   // 拉一个，加工后返回
    return null;
}

// filter：拉 + 循环跳过不匹配
public T poll() {
    for(;;) {
        T v = s.poll();
        if (v == null || predicate.test(v)) return v;   // null(完成) 或 满足条件就返回
        Operators.onDiscard(v, ctx);   // 不满足就丢弃，继续循环
    }
}
```

> ⚠️ 小知识：SYNC 模式下源头的 `index` 字段**不是 volatile 的**，这是安全的。因为 SYNC 熔合下 `poll()` 永远由同一个线程调用（消费者的排空循环是单线程的），不存在多线程竞争。

---

## 五、ASYNC 熔合：publishOn 场景下的共享传送带

当链路里有 `publishOn` 时，情况变成 ASYNC 熔合。

```java
someAsyncSource   // 一个异步源，比如 UnicastProcessor
    .publishOn(scheduler)
    .subscribe(...);
```

ASYNC 熔合的核心是"**共享队列**"：

1. `publishOn` 问异步源：`requestFusion(ANY | THREAD_BARRIER)`；
2. 异步源支持 ASYNC，返回 `ASYNC`；
3. `publishOn` 于是把**异步源自己的队列**直接当成自己的队列用（`queue = asyncSource 的 QueueSubscription`），省掉了再分配一个中间队列；
4. 数据流转：异步源把数据放进它自己的队列 → 调 `onNext(null)` 通知 publishOn"有货了" → publishOn 的排空循环 `queue.poll()` 取数据 → 推给下游。

**ASYNC 和 SYNC 的关键区别**：

- SYNC：数据**已经全在**队列/源里了，poll 到 null 就代表完成；
- ASYNC：数据**陆续异步到达**，`onNext(null)` 是"来新货了"的通知信号，真正的完成靠 `onComplete()`。

在 ASYNC 模式下，中间的 map/filter 的 `onNext(t)` 变成了"转发通知"：

```java
// MapFuseableSubscriber.onNext 在 ASYNC 模式下
if (sourceMode == ASYNC) {
    actual.onNext(null);   // 只转发"有新数据"的通知，不处理 t（t 已在共享队列里）
}
```

而实际的加工（`mapper.apply`）延迟到下游 `poll()` 时才做。

**filter 在 ASYNC 模式下有个额外的 `dropped` 计数器**：因为异步源需要 request 记账，被过滤掉的元素得"补偿性地"向上游补 request，否则上游不知道该继续产多少：

```java
// FilterFuseableSubscriber.poll 在 ASYNC 模式下
long dropped = 0;
for (;;) {
    T v = s.poll();
    if (v == null || predicate.test(v)) {
        if (dropped != 0) request(dropped);   // 把丢弃的量补偿性地 request 回去
        return v;
    }
    dropped++;   // 每过滤一个就记一笔
}
```

SYNC 模式则不需要这个，因为 SYNC 源根本不用 request。

---

## 六、THREAD_BARRIER：熔合世界里的"安全护栏"

这是整篇最烧脑、但也最重要的一个概念。理解了它，你就理解了熔合和线程切换的交界处。

### 问题：熔合可能悄悄突破线程边界

看这个链路：

```java
asyncSource
    .map(expensiveMapper)    // 一个"重"的转换函数
    .publishOn(scheduler)    // 换线程
    .subscribe(...);
```

在**没有 publishOn** 时，`map` 的 `onNext` 是在 **asyncSource 的线程**上被调用的，所以 `expensiveMapper.apply()` 在 asyncSource 的线程执行。这是它"应该待的地方"。

现在加了 `publishOn`。如果**允许熔合**会发生什么？publishOn 的排空循环在 **scheduler 的 Worker 线程**上跑，它会调 `MapFuseableSubscriber.poll()`，而 `poll()` 里会执行 `mapper.apply()`。于是——

**`expensiveMapper` 被偷偷搬到了 scheduler 的 Worker 线程上执行！线程边界被熔合悄无声息地突破了。**

如果这个 mapper 是**线程不安全**的（比如它读写了 `ThreadLocal`、或者用了非线程安全的缓存），这就是一个隐蔽的并发 bug——而且是那种"偶尔出错、极难复现"的噩梦级 bug。

### 解决：用 THREAD_BARRIER 打招呼

`THREAD_BARRIER` 就是为这个问题设计的护栏。`publishOn` 请求熔合时会带上它：

```java
int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);
//                                      ↑ 相当于举手说："注意，我会换线程消费你的数据"
```

而 `map`、`filter` 这些"函数会被执行"的操作符，看到 `THREAD_BARRIER` 就**主动拒绝熔合**：

```java
if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
    return Fuseable.NONE;   // "既然你要换线程，那我不熔合了，避免我的函数跑错线程"
}
```

拒绝熔合后，publishOn 只能老老实实创建独立的中间队列，`expensiveMapper` 继续通过正常的 `onNext` 在 asyncSource 的线程上执行——**线程边界被守住了**。

### 但源头（FluxArray）为什么可以无视 THREAD_BARRIER？

```java
someArray.publishOn(scheduler)   // FluxArray 直接接 publishOn
```

这时 `ArraySubscription`（`SynchronousSubscription`）**完全无视 THREAD_BARRIER**，照样返回 SYNC。为什么它就安全？

因为**数组数据是静止地躺在内存里的，没有"原始线程"这个概念**。数组的元素 `array[i]`，你在哪个线程 `poll` 出来都是同一个值，不涉及任何线程相关的计算。所以 publishOn 在 Worker 线程上直接 poll 数组是完全安全的。

**一句话总结 THREAD_BARRIER 的判断标准**：
- 如果熔合会导致"某段用户代码（如 mapper）跑到别的线程" → 危险 → 中间操作符拒绝熔合；
- 如果熔合只是"从静止数据里拉值"（如数组、range） → 安全 → 源头无视 THREAD_BARRIER，照常熔合。

> ⚠️ 踩坑提醒：这解释了一个现象——`Flux.just(...).map(...).publishOn(...)` 里的 map **不会被熔合进 publishOn**（因为 map 拒绝了 THREAD_BARRIER），但 map 之前如果紧挨着的是源头之间的 map/filter 链，它们彼此之间是可以 SYNC 熔合的。熔合的边界正好卡在"跨线程"的地方，这是 Reactor 精心设计的安全底线。

---

## 七、ConditionalSubscriber：filter 的另一个提速小技巧

除了熔合，Reactor 还有一个针对 filter 的独立优化，叫 `ConditionalSubscriber`（条件订阅者）。它解决的是**非熔合模式下 filter 的 request 开销**。

### 问题

在**非熔合模式**下，filter 收到一个不匹配的元素时，得调 `s.request(1)` 向上游"再要一个"：

```java
// 普通 onNext
public void onNext(T t) {
    if (predicate.test(t)) {
        actual.onNext(t);
    } else {
        s.request(1);   // ← 不匹配就要"再要一个"，一次原子操作往返
        Operators.onDiscard(t, ctx);
    }
}
```

如果过滤率很高（比如 90% 被过滤），这就是 90% 的 `request(1)` 往返，开销可观。

### 解法：tryOnNext——"你要是不要，直接告诉我，我立刻给你下一个"

`ConditionalSubscriber` 多了一个方法 `tryOnNext(T t)`：

```java
interface ConditionalSubscriber<T> extends CoreSubscriber<T> {
    boolean tryOnNext(T t);   // 尝试消费；返回 false 表示"没要，你可以立刻发下一个"
}
```

filter 实现它：

```java
public boolean tryOnNext(T t) {
    boolean b = predicate.test(t);
    if (b) {
        actual.onNext(t);
        return true;    // 要了
    }
    Operators.onDiscard(t, ctx);
    return false;       // 没要——但注意，这里没有 s.request(1)！
}
```

**关键区别**：`tryOnNext` 返回 `false` 时**不调 `request(1)`**。上游看到 `false`，知道"这个没被要"，于是**立刻推送下一个元素**，不需要等 request 往返。

这就好比：正常模式是"我不要这个 → 挂电话 → 上游再打电话来 → 发下一个"；而 tryOnNext 是"我不要这个（直接摇头）→ 上游立刻递下一个"，省掉了一个来回的电话。在 90% 过滤率的场景下，能省掉 90% 的 request 原子操作。

在第 07 篇讲的 `publishOn` 里，你会看到它专门为 `ConditionalSubscriber` 准备了 `PublishOnConditionalSubscriber` 分支，就是为了让这个优化能贯穿整条链。

---

## 八、什么操作符不能熔合？（熔合的边界）

不是所有操作符都能熔合，强行熔合会破坏语义：

- **`flatMap` 不支持 SYNC 熔合**：因为它的输出是多个内部流异步交错出来的，根本不是"一个能顺序 poll 的队列"。
- **`publishOn` 不支持 SYNC 输出熔合**：因为它本身就是个异步边界，输出天然是异步的（只支持 ASYNC 输出熔合）。
- **带 THREAD_BARRIER 的 map/filter**：如前所述，为了守住线程边界，主动拒绝。

Reactor 只在"熔合了也不会出错"的地方启用它，绝不为了性能牺牲正确性。

---

## 九、归纳总表

### 三种熔合模式对比

| 维度 | NONE（不熔合） | SYNC 熔合 | ASYNC 熔合 |
|---|---|---|---|
| **流水线比喻** | 每工位签收+打电话 | 相邻工位合并成一个 | 工位间共享传送带 |
| **常量值** | 0 | 1 | 2 |
| **数据怎么传** | `onNext(t)` 推 | `poll()` 拉 | `onNext(null)` 通知 + `poll()` 拉 |
| **谁主动** | 上游推给下游 | 下游主动来拉 | 上游填队列，下游来拉 |
| **有独立中间队列吗** | 有（如 SpscArrayQueue） | 无，复用上游 | 无，复用上游 |
| **需要 request 记账吗** | 需要 | 不需要 | 需要（部分，含 dropped 补偿） |
| **完成信号** | `onComplete()` | `poll()` 返回 null | `onComplete()` |
| **典型源头** | 任意 Publisher | `FluxArray`、`FluxRange`、`MonoJust` | `UnicastProcessor`、异步操作符 |
| **性能收益** | 基线 | 省掉 onNext + request 开销（2~5倍） | 省掉中间队列分配 |

### 各操作符在协商中的行为

| 操作符/接口 | requestFusion 怎么回应 | poll() 怎么干 |
|---|---|---|
| `SynchronousSubscription`（源头基类） | 要 SYNC 就给 SYNC，否则 NONE；无视 THREAD_BARRIER | 从底层数据直接拉 |
| `MapFuseableSubscriber` | 带 THREAD_BARRIER→NONE；否则透传上游 | `s.poll()` 后 `mapper.apply(v)` |
| `FilterFuseableSubscriber` | 带 THREAD_BARRIER→NONE；否则透传上游 | 循环 `s.poll()` 直到匹配 |
| `PublishOnSubscriber`（作为下游问上游） | 请求 `ANY + THREAD_BARRIER` | 看 sourceMode 分支 |
| `PublishOnSubscriber`（作为上游被问） | 要 ASYNC 就给 ASYNC，否则 NONE | 从内部队列 poll + 补货 |

### 五个必记结论

| 结论 | 一句话 |
|---|---|
| 熔合把"推"改成"拉" | 消费者主动 poll，沿链同步拉数据，省掉 onNext/request |
| SYNC 用于静止数据 | 数据已就绪（数组、range），poll 到 null 即完成 |
| ASYNC 用于异步数据 | 共享队列，onNext(null) 是通知信号 |
| THREAD_BARRIER 是安全护栏 | 防止 mapper 等用户代码被熔合偷偷搬到别的线程执行 |
| ConditionalSubscriber 省 request | filter 拒绝元素时用 tryOnNext 返回 false，上游立刻发下一个，不用 request(1) 往返 |

---

## 结语

熔合（Fuseable）是 Reactor "看不见但一直在省钱"的性能引擎。它的核心思想很简单：**能"拉"就别"推"，能合并就别拆开，能共享队列就别各搞一套。** 但它又极其克制——一旦遇到 `THREAD_BARRIER`（可能跨线程执行用户代码），立刻退回安全的非熔合模式，绝不为性能牺牲正确性。

把这三篇连起来看：第 06 篇讲了线程池（Scheduler）这个"厨房"，第 07 篇讲了 `subscribeOn`/`publishOn` 这两个"换线程开关"，第 08 篇讲的熔合则揭示了 Reactor 在不换线程的地方如何把操作符压榨到极致。而 `THREAD_BARRIER` 正是这两个主题的交汇点——它保证了"性能优化"和"线程安全"这两件事能和平共处。这，就是 Reactor 工程设计的精妙之处。
