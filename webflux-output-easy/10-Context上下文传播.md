# Context 上下文传播（易懂版）

> **Reactor Core 源码解析系列 · 第 10 篇 · 易懂版**

---

## 一、从一个真实痛点说起：traceId 去哪了？

你正在开发一个 Spring WebFlux 应用。为了追踪请求链路，你在 Controller 里用 ThreadLocal 存了一个 traceId：

```java
// Spring MVC 时代的做法
@RestController
public class OrderController {
    
    @PostMapping("/orders")
    public Order createOrder(@RequestBody OrderRequest request) {
        // 从请求头里拿到 traceId，存到 ThreadLocal
        String traceId = request.getHeader("X-Trace-Id");
        MDC.put("traceId", traceId);
        
        // 后续的日志都会自动带上 traceId
        log.info("开始创建订单");    // [traceId=abc123] 开始创建订单
        Order order = orderService.create(request);
        log.info("订单创建完成");    // [traceId=abc123] 订单创建完成
        
        return order;
    }
}
```

在 Spring MVC 里这完全没问题——从请求进来到响应返回，始终在同一个线程上执行，ThreadLocal 一直都在。

**然后你切换到了 WebFlux：**

```java
@RestController
public class OrderController {
    
    @PostMapping("/orders")
    public Mono<Order> createOrder(@RequestBody Mono<OrderRequest> request) {
        String traceId = MDC.get("traceId");  // 这里还能拿到
        
        return request
            .flatMap(req -> orderService.create(req))  // 可能在另一个线程执行！
            .doOnNext(order -> {
                log.info("订单创建完成");  // traceId 呢？丢了！
                // MDC.get("traceId") → null
            });
    }
}
```

**问题出在 `flatMap` 可能会切换线程。** WebFlux 底层用的是 Netty 的 EventLoop 线程池，数据可能在线程 A 上开始处理，经过 `publishOn` 或 `flatMap` 后跑到线程 B 上继续。ThreadLocal 是跟线程绑定的，线程一换，值就丢了。

```
Thread-1 (Netty EventLoop):          Thread-2 (boundedElastic):
  ├── 设置 ThreadLocal("traceId")      
  ├── flatMap 切换线程 ────────────→    ├── 读 ThreadLocal("traceId")
  │                                     │   → null！！！丢了！
```

**这就是 Reactor Context 要解决的核心问题：在线程可能随时切换的响应式编程中，如何安全地传递上下文信息？**

---

## 二、Context 是什么？——不跟线程走，跟订阅者走

**Q：Context 到底是个什么东西？**

一句话：**Context 是一个不可变的键值对容器，它绑定在订阅者（Subscriber）上，而不是绑定在线程上。**

```java
// 写入 Context
Flux.just(1, 2, 3)
    .flatMap(i -> Mono.deferContextual(ctx -> {
        String traceId = ctx.get("traceId");  // 无论在哪个线程，都能拿到
        log.info("[{}] 处理数据: {}", traceId, i);
        return Mono.just(i * 10);
    }))
    .contextWrite(ctx -> ctx.put("traceId", "abc123"))  // 在这里写入
    .subscribe();
```

无论中间经过多少次线程切换，`ctx.get("traceId")` 都能拿到 `"abc123"`。因为 Context 不存在 ThreadLocal 里，而是存在订阅者对象的字段里——对象在哪个线程上被使用，就在哪个线程上读 Context。

**类比：ThreadLocal 像是工位上贴的便签——你离开工位（切换线程），便签就看不到了。Context 像是订单上写的备注——订单传到哪里，备注就跟到哪里。**

---

## 三、Context 的逆向传播：从下游到上游

**Q：`contextWrite` 明明写在 `flatMap` 后面，为什么 `flatMap` 里面能读到？**

这是 Context 最反直觉的地方——**Context 从下游向上游传播，和数据流方向相反！**

```java
Flux.just(1, 2, 3)          // ④ 数据源：能看到 {traceId: abc123}
    .map(i -> i * 2)         // ③ 能看到 {traceId: abc123}
    .contextWrite(ctx -> ctx.put("traceId", "abc123"))  // ② 修改 Context
    .filter(i -> i > 2)      // ① 看不到 traceId！只能看到空 Context
    .subscribe();            // ⓪ 起点：空 Context
```

**数据流方向：** 上游 → 下游（①②③④ → ⓪）

**Context 传播方向：** 下游 → 上游（⓪ → ① → ② → ③ → ④）

**类比：想象你在餐厅点餐——**

1. 你（订阅者/下游）在订单上写了一个备注"不要辣"（`contextWrite`）
2. 服务员把订单传给后厨（上游）
3. 订单经过每个工位时，厨师都能看到"不要辣"这个备注

顾客（下游）写备注，后厨（上游）读备注。备注是从顾客传向后厨的——和菜品出餐的方向相反。

**Q：为什么要从下游往上游传，而不是从上游往下游传？**

因为 Context 携带的是"消费者想要告诉生产者的信息"。比如：
- 消费者想说"我这个请求的 traceId 是 abc123"（日志追踪）
- 消费者想说"我这个请求用的是中文"（国际化）
- 消费者想说"如果数据丢弃了请用这个函数清理"（资源清理）

这些信息都是消费者定义的，需要被上游的操作符感知。如果从上游往下游传，上游操作符根本不知道下游需要什么上下文。

---

## 四、源码揭秘：Context 是怎么传播的？

### 第一步：每个操作符都有 `currentContext()` 方法

每个操作符（如 `map`、`filter`）内部的订阅者都实现了 `currentContext()` 方法。默认实现是**委托给下游**：

```java
// InnerOperator.java
default Context currentContext() {
    return actual().currentContext();  // 问下游要 Context
}
```

这形成了一个链式委托：

```
MapSubscriber.currentContext()       → 问 actual（下游）
  ↓ 委托
FilterSubscriber.currentContext()    → 问 actual（下游）
  ↓ 委托
LambdaSubscriber.currentContext()    → 返回 Context.empty()（终端订阅者）
```

### 第二步：`contextWrite` 打断委托链

`contextWrite()` 操作符创建一个 `ContextWriteSubscriber`，它不再委托下游，而是返回自己修改后的 Context：

```java
// FluxContextWrite.java 中的 ContextWriteSubscriber
public Context currentContext() {
    return this.context;  // 返回修改后的 Context，不再委托下游
}
```

所以完整的传播链变成：

```
Source 问 currentContext()
  ↓ 委托
MapSubscriber 问 currentContext()
  ↓ 委托
ContextWriteSubscriber 返回 {traceId: "abc123"}  ← 打断！插入新值
  ↓（如果继续委托的话就是 FilterSubscriber → LambdaSubscriber）
```

上游操作符（Source、MapSubscriber）看到的是经过 `contextWrite` 修改后的 Context。而 `contextWrite` 下方的操作符（FilterSubscriber）看到的是原始的空 Context。

### 第三步：`contextWrite` 怎么修改 Context

```java
// FluxContextWrite.java
public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
    // 1. 从下游获取当前 Context
    Context downstream = actual.currentContext();
    // 2. 用用户提供的函数修改
    Context modified = doOnContext.apply(downstream);
    // 3. 包装成新的订阅者
    return new ContextWriteSubscriber<>(actual, modified);
}
```

注意这个顺序：先拿到下游的 Context，再在其基础上修改。所以如果有多个 `contextWrite`，它们是"叠加"的：

```java
flux
    .contextWrite(ctx -> ctx.put("key1", "value1"))  // 后执行，在 {key2:v2} 基础上加 key1
    .contextWrite(ctx -> ctx.put("key2", "value2"))  // 先执行，在空 Context 上加 key2
    .subscribe();

// 上游看到的 Context: {key1: "value1", key2: "value2"}
```

⚠️ **踩坑提醒：`contextWrite` 的执行顺序和代码书写顺序是反的！** 离 `subscribe()` 越近的 `contextWrite` 越先执行，离数据源越近的越后执行。因为 Context 传播方向是下游 → 上游，所以下游的 `contextWrite` 先拿到空 Context 进行修改，上游的 `contextWrite` 在下游修改的基础上再修改。

---

## 五、Context 的不可变设计：为什么 `put` 返回新对象？

**Q：为什么不能直接修改 Context？**

```java
Context ctx1 = Context.of("key", "value1");
Context ctx2 = ctx1.put("key", "value2");  // 返回一个新对象！

System.out.println(ctx1.get("key"));  // "value1"  —— ctx1 没有被修改
System.out.println(ctx2.get("key"));  // "value2"  —— ctx2 是新的
```

**如果 Context 是可变的会怎样？**

想象两个操作符在不同线程上同时读写同一个 Context 对象：
- 线程 A（map 操作符）：正在读 `ctx.get("traceId")`
- 线程 B（某个回调）：同时修改 `ctx.put("userId", "user1")`

如果 Context 是可变的（像 HashMap），就会产生并发修改问题——读到不一致的数据，甚至 ConcurrentModificationException。

不可变设计让 Context 天生线程安全：
- 多个操作符可以同时读同一个 Context 实例，无需加锁
- `put` 返回新实例，原实例不受影响
- 不同操作符看到不同版本的 Context，互不干扰

**类比：Context 像是你拍的照片——你可以在照片上 P 图（`put`），但 P 出来的是一张新照片，原照片不变。其他看原照片的人不受影响。**

---

## 六、Context0 到 Context5：为几个键值对专门优化

**Q：一个 Map 就能存键值对，为什么搞出 Context0、Context1、...、Context5、ContextN 这么多类？**

答案是**性能**。Reactor 做了一个统计：实际使用中，90%+ 的 Context 只有 1-5 个键值对（比如一个 traceId、一个 userId、一个 locale，很少超过5个）。

对于这种少量键值对的场景，用 Map 是杀鸡用牛刀：

```java
// HashMap 存 1 个键值对的代价：
// - 1 个 HashMap 对象（48字节）
// - 1 个 Node[] 数组（至少 16 个槽位 = 80字节）
// - 1 个 Node 对象（32字节）
// 总计约 160 字节

// Context1 存 1 个键值对的代价：
// - 1 个 Context1 对象（对象头16字节 + 2个引用16字节）
// 总计约 32 字节  —— 节省 80%！
```

所以 Reactor 用 `final` 字段直接存储键值对：

```java
final class Context1 implements CoreContext {
    final Object key;    // 就两个字段，没有任何多余结构
    final Object value;
    
    public <T> T get(Object key) {
        if (this.key.equals(key)) return (T) this.value;
        throw new NoSuchElementException();
    }
    
    public Context put(Object key, Object value) {
        if (this.key.equals(key)) {
            return new Context1(key, value);    // 同 key，替换值
        }
        return new Context2(this.key, this.value, key, value);  // 新 key，升级
    }
}
```

**升级/降级规则：**

```
put 一个新 key：
  Context0 → Context1 → Context2 → Context3 → Context4 → Context5 → ContextN

delete 一个 key：
  ContextN → Context5 → Context4 → Context3 → Context2 → Context1 → Context0
```

**类比：用钱包装硬币。1-5个硬币直接放口袋（Context1-5），超过5个就拿个零钱包（ContextN/LinkedHashMap）。硬币花到只剩5个以下了，又放回口袋。**

### 为什么分界线是5？

这是 Reactor 团队实测得出的阈值：
- 5个以下：`final` 字段逐个 `equals` 比较的速度快于 `HashMap.get()` 的 `hashCode + 查表`
- 6个以上：`HashMap` 的 O(1) 查找开始优于 `final` 字段的 O(n) 遍历
- 同时，5个 `final` 字段的对象仍然很小，对 CPU 缓存友好

### ContextN：超过5个时的 Map 实现

```java
final class ContextN extends LinkedHashMap<Object, Object> implements CoreContext {
    
    // 外部 put：创建新实例（不可变语义）
    public Context put(Object key, Object value) {
        ContextN newContext = new ContextN(this);  // 复制当前 Map
        newContext.accept(key, value);             // 添加新条目
        return newContext;
    }
    
    // delete 如果剩5个，降级回 Context5
    public Context delete(Object key) {
        if (size() - 1 == 5) {
            // 降级回 Context5
            return new Context5(/* 剩下的5个键值对 */);
        }
        ContextN newInstance = new ContextN(this);
        newInstance.remove(key);
        return newInstance;
    }
}
```

⚠️ **踩坑提醒：每次 `put` 都会复制整个 Map（copy-on-write），所以不要在热路径上频繁修改大 Context。如果你的 Context 有很多键值对且经常变化，考虑把多个值合并成一个对象存储，减少 `put` 次数。**

---

## 七、读 Context vs 写 Context：接口分离

**Q：为什么有 `ContextView` 和 `Context` 两个接口？**

```java
// ContextView：只读接口
public interface ContextView {
    <T> T get(Object key);
    boolean hasKey(Object key);
    int size();
    // 没有 put/delete！
}

// Context：可写接口
public interface Context extends ContextView {
    Context put(Object key, Object value);    // 返回新实例
    Context delete(Object key);                // 返回新实例
}
```

**为什么分开？** 防止下游操作符意外修改 Context。

大多数操作符只需要**读**Context（拿 traceId 打日志），不需要修改。把只读接口 `ContextView` 暴露给它们，编译器就能防止意外调用 `put`：

```java
// deferContextual 给你的是 ContextView，只读
Mono.deferContextual(ctx -> {
    String traceId = ctx.get("traceId");  // 能读
    // ctx.put("key", "value");           // 编译错误！ContextView 没有 put 方法
    return Mono.just(traceId);
});
```

只有 `contextWrite` 操作符才接受一个 `Function<Context, Context>`，能读能写：

```java
.contextWrite(ctx -> ctx.put("traceId", "abc123"))  // 这里拿到的是 Context，能写
```

**类比：银行的查询窗口和业务窗口。查询窗口（ContextView）只能查余额，业务窗口（Context）才能转账。大部分人只需要查余额，让他们去查询窗口，安全且高效。**

---

## 八、实战：用 Context 解决 traceId 丢失问题

### 方案一：手动传播

```java
@RestController
public class OrderController {
    
    @PostMapping("/orders")
    public Mono<Order> createOrder(ServerHttpRequest request) {
        String traceId = request.getHeaders().getFirst("X-Trace-Id");
        
        return orderService.create(request.getBody())
            .doOnEach(signal -> {
                // 在每个信号（onNext/onError/onComplete）时读取 Context
                if (signal.isOnNext()) {
                    String tid = signal.getContextView().getOrDefault("traceId", "unknown");
                    log.info("[{}] 订单创建成功: {}", tid, signal.get());
                }
            })
            .contextWrite(ctx -> ctx.put("traceId", traceId));  // 写入 Context
    }
}
```

### 方案二：deferContextual（更优雅）

```java
@Service
public class OrderService {
    
    public Mono<Order> create(OrderRequest request) {
        return Mono.deferContextual(ctx -> {
            // 在这里可以安全地读取 Context
            String traceId = ctx.getOrDefault("traceId", "no-trace");
            log.info("[{}] 开始处理订单", traceId);
            
            return doCreate(request)
                .doOnSuccess(order -> log.info("[{}] 订单处理完成", traceId));
        });
    }
}
```

### 方案三：自动桥接 ThreadLocal（Reactor 3.5.3+）

如果你有大量依赖 ThreadLocal 的遗留代码（比如 MDC 日志、Spring Security），可以启用自动传播：

```java
// 应用启动时
Hooks.enableAutomaticContextPropagation();
```

这个功能需要额外依赖 `io.micrometer:context-propagation`。它的原理是：在 Scheduler 切换线程时，自动将 Context 中的值同步到 ThreadLocal，在任务执行完毕后恢复。这样即使线程切换了，`MDC.get("traceId")` 也能拿到正确的值。

⚠️ **踩坑提醒：`enableAutomaticContextPropagation` 有性能开销（每次线程切换都要读写 ThreadLocal），且需要配置 `ThreadLocalAccessor` 来指定哪些 ThreadLocal 需要桥接。不要指望它开箱即用——需要阅读 [context-propagation](https://github.com/micrometer-metrics/context-propagation) 的文档进行配置。**

---

## 九、多订阅者场景：Context 怎么合并？

**Q：`Sinks.many().multicast()` 有多个订阅者，每个订阅者的 Context 不一样怎么办？**

```java
Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

// 订阅者 A 带了 traceId
sink.asFlux()
    .contextWrite(ctx -> ctx.put("traceId", "req-1"))
    .subscribe(s -> log.info("A 收到: {}", s));

// 订阅者 B 带了 userId
sink.asFlux()
    .contextWrite(ctx -> ctx.put("userId", "user-2"))
    .subscribe(s -> log.info("B 收到: {}", s));
```

Sink 作为数据源，它的 `currentContext()` 会合并所有订阅者的 Context：

```java
// SinkManyEmitterProcessor.java
public Context currentContext() {
    return Operators.multiSubscribersContext(subscribers);
    // 合并结果: {traceId: "req-1", userId: "user-2"}
}
```

合并规则是 `putAll`——所有订阅者的键值对合在一起。

⚠️ **踩坑提醒：如果两个订阅者的 Context 有相同的 key 但不同的 value，合并结果取决于遍历顺序，是不确定的。** 最佳实践是：不同订阅者不要设置相同的 Context key。如果需要统一设置，在 Sink 上游用 `contextWrite` 设置，而不是在每个订阅者上分别设置。

---

## 十、Context vs ThreadLocal 全面对比

**Q：既然 Context 这么好，ThreadLocal 是不是就完全不要了？**

不是。两者各有适用场景：

| 维度 | ThreadLocal | Reactor Context |
|------|------------|----------------|
| **绑定对象** | 线程 | 订阅者（Subscription） |
| **线程切换** | 值丢失 | 值保留 |
| **传播方向** | 线程内隐式 | 操作符链显式（下游→上游） |
| **可变性** | 可变（`.set()` 直接修改） | 不可变（`.put()` 返回新实例） |
| **线程安全** | 天然安全（线程隔离） | 天然安全（不可变） |
| **生命周期** | 线程生命周期 | 订阅生命周期 |
| **清理** | 需要手动 `remove()` 防止泄漏 | 随订阅销毁自动回收 |
| **性能** | 极快（直接数组索引） | 较快（对象引用 + equals） |
| **兼容性** | 所有 Java 库都支持 | 只有 Reactor 生态支持 |

**选择建议：**
- 纯 WebFlux 项目且没有遗留依赖：优先用 Context
- 需要和 Spring Security、MDC、Hibernate 等基于 ThreadLocal 的库集成：考虑 `enableAutomaticContextPropagation()` 做桥接
- 同步代码块中的短期状态：ThreadLocal 更方便

---

## 十一、多个 contextWrite 的执行顺序

**Q：多个 `contextWrite` 叠在一起，上游看到的是哪个值？**

```java
Flux.deferContextual(ctx -> {
    System.out.println(ctx.get("key"));  // 输出什么？
    return Flux.just(1);
})
.contextWrite(ctx -> ctx.put("key", "A"))    // 离数据源最近
.contextWrite(ctx -> ctx.put("key", "B"))    // 中间
.contextWrite(ctx -> ctx.put("key", "C"))    // 离 subscribe 最近
.subscribe();
```

答案是输出 `"A"`。

**分析过程：**

Context 从下游到上游传播，所以执行顺序是：
1. `subscribe()` → 空 Context
2. 第三个 `contextWrite`（离 subscribe 最近）→ `{key: "C"}`
3. 第二个 `contextWrite` → 在 `{key: "C"}` 基础上 `put("key", "B")` → `{key: "B"}`（覆盖了 C）
4. 第一个 `contextWrite`（离数据源最近）→ 在 `{key: "B"}` 基础上 `put("key", "A")` → `{key: "A"}`（覆盖了 B）

**结论：离数据源最近的 `contextWrite` "赢"了。** 因为它在传播链上最后执行，覆盖了前面的值。

⚠️ **踩坑提醒：这个顺序和直觉相反！很多人以为离 `subscribe()` 最近的会生效，其实是离数据源最近的生效。记住口诀——"`contextWrite` 就近原则：离读取点（上游）越近越优先"。**

---

## 十二、Context 的工厂方法

```java
// 创建空 Context
Context ctx0 = Context.empty();  // 返回 Context0 单例

// 创建1-5个键值对
Context ctx1 = Context.of("key1", "value1");                    // Context1
Context ctx2 = Context.of("key1", "v1", "key2", "v2");         // Context2
// ...最多到5对

// 从 Map 创建
Map<String, String> map = Map.of("k1", "v1", "k2", "v2", "k3", "v3");
Context ctxFromMap = Context.of(map);  // 3个键值对 → Context3（不是 ContextN！）
```

注意 `Context.of(Map)` 会根据 Map 大小选择最优实现——3个条目的 Map 会创建 `Context3`，而不是直接用 `ContextN`。

---

## 十三、实战场景：用 Context 实现多租户隔离

```java
@Component
public class TenantFilter implements WebFilter {
    
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String tenantId = exchange.getRequest().getHeaders().getFirst("X-Tenant-Id");
        
        return chain.filter(exchange)
            .contextWrite(ctx -> ctx.put("tenantId", tenantId));
    }
}

@Repository
public class OrderRepository {
    
    public Flux<Order> findAll() {
        return Flux.deferContextual(ctx -> {
            String tenantId = ctx.get("tenantId");
            // 根据租户 ID 查不同的数据库 schema
            return r2dbcTemplate.select(
                "SELECT * FROM " + tenantId + ".orders", Order.class);
        });
    }
}
```

**这种模式的优势：**
- 租户信息不依赖 ThreadLocal，线程切换不会丢失
- 每个请求的 Context 是隔离的，不会互相污染
- 不需要在每个方法签名里传递 `tenantId` 参数

---

## 十四、归纳总结表格

### 表1：Context 实现类对照

| 实现类 | 键值对数 | 存储方式 | 查找效率 | 内存占用 | put 行为 | delete 行为 |
|--------|---------|---------|---------|---------|---------|------------|
| `Context0` | 0 | 无字段（单例） | - | 极小（共享实例） | → Context1 | 返回 this |
| `Context1` | 1 | 2个 final 字段 | 1次 equals | 约32字节 | 同key→新Context1；新key→Context2 | → Context0 |
| `Context2` | 2 | 4个 final 字段 | 最多2次 equals | 约48字节 | 同key→新Context2；新key→Context3 | → Context1 |
| `Context3` | 3 | 6个 final 字段 | 最多3次 equals | 约64字节 | 类推 | → Context2 |
| `Context4` | 4 | 8个 final 字段 | 最多4次 equals | 约80字节 | 类推 | → Context3 |
| `Context5` | 5 | 10个 final 字段 | 最多5次 equals | 约96字节 | 同key→新Context5；新key→ContextN | → Context4 |
| `ContextN` | 6+ | LinkedHashMap | O(1) hash 查找 | Map 开销 | copy-on-write 新 Map | 剩5个→Context5 |

### 表2：Context 操作 API 速查

| 操作 | 方法 | 说明 |
|------|------|------|
| 写入 | `contextWrite(ctx -> ctx.put(k, v))` | 在操作符链中插入/修改 Context |
| 读取（信号级） | `doOnEach(signal -> signal.getContextView())` | 在 onNext/onError/onComplete 信号中读取 |
| 读取（延迟） | `Mono.deferContextual(ctx -> ...)` | 在订阅时读取 Context 并创建流 |
| 读取（变换） | `transformDeferredContextual((flux, ctx) -> ...)` | 变换流的同时读取 Context |
| 删除 | `contextWrite(ctx -> ctx.delete(k))` | 从 Context 中移除一个 key |
| 合并 | `contextWrite(ctx -> ctx.putAll(otherCtx))` | 合并另一个 Context 的所有键值对 |

### 表3：ThreadLocal vs Context 场景选择

| 场景 | 推荐方案 | 原因 |
|------|---------|------|
| WebFlux 请求追踪 | Context | 线程切换不会丢失 |
| 多租户隔离 | Context | 跟请求走而非跟线程走 |
| 与 MDC 日志集成 | Context + 自动桥接 | 两者配合使用 |
| 与 Spring Security 集成 | Context + `ReactorContextAccessor` | Security 已支持 |
| 纯同步代码 | ThreadLocal | 更简单高效 |
| JDBC 事务管理 | ThreadLocal | JDBC 不支持响应式 |

### 表4：核心源码类对照

| 源码类 | 作用 | 关键方法 |
|--------|------|---------|
| `ContextView` | 只读接口 | `get()`, `hasKey()`, `size()`, `stream()` |
| `Context` | 可写接口（继承 ContextView） | `put()`, `delete()`, `putAll()`, `of()`, `empty()` |
| `CoreContext` | 内部优化接口 | `putAllInto()`, `unsafePutAllInto()` |
| `Context0` ~ `Context5` | 1-5个键值对的优化实现 | 自动升级/降级 |
| `ContextN` | 6+个键值对的 Map 实现 | copy-on-write 的 `put`/`delete` |
| `FluxContextWrite` | `contextWrite()` 操作符 | `subscribeOrReturn()` 创建 `ContextWriteSubscriber` |
| `ContextWriteSubscriber` | 修改后的 Context 持有者 | `currentContext()` 返回修改后的 Context |
| `InnerOperator` | 操作符默认行为 | `currentContext()` 委托给下游 |
| `CoreSubscriber` | 终端订阅者接口 | `currentContext()` 默认返回 `Context.empty()` |
