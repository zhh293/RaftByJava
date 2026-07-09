# 从 Reactor 视角理解 WebFlux 与 Spring 响应式架构

> **Reactor Core 源码解析系列 · 第 16 篇（易懂版）**
>
> 用"传统餐厅 vs 自助餐厅"的类比，搞懂 Spring MVC 和 WebFlux 的本质区别，以及一个完整的 WebFlux 请求是如何从 Netty 到你的业务代码再返回的。

---

## 一、传统餐厅 vs 自助餐厅：Spring MVC 与 WebFlux 的本质区别

### 1.1 传统餐厅（Spring MVC）

你去一家传统餐厅吃饭。坐下后，服务员过来点菜。你看了半天菜单，点了一个红烧肉。服务员记下后——**站在你桌旁等着**。等后厨做好红烧肉，服务员端给你，然后才去服务下一桌。

问题显而易见：如果每桌点菜后都要等 20 分钟才上菜，一个服务员一晚上只能服务几桌。餐厅要么雇大量服务员（大量线程），要么客户体验很差。

这就是 Spring MVC 的模型：

- **一个请求分配一个线程**（一个服务员盯一桌）
- 线程在等待 I/O 时被阻塞（服务员站在桌旁等菜）
- 200 个并发请求需要 200 个线程（200 个服务员）
- 每个线程约占 1MB 栈空间，200 个线程 = 200MB 内存

### 1.2 自助餐厅（WebFlux）

你去一家自助餐厅。坐下后，扫码点单。点完提交后，你跟朋友聊天。服务员（其实只有 4 个）在后台收到订单，传给后厨。后厨做好后，服务员端到你桌上，叫号通知你。

关键区别：**服务员不会站在你桌旁等菜**。他传完订单就去看下一桌的扫码点单。4 个服务员就能服务 200 桌。

这就是 WebFlux 的模型：

- **少量 EventLoop 线程处理大量连接**（4 个服务员管全场）
- 线程在等待 I/O 时不阻塞，继续处理其他连接（服务员传完订单就去忙别的）
- 200 个并发请求只需要 4 个线程
- 内存占用约 16MB（4 个线程 × 4MB 栈空间）

### 1.3 用数字对比

```
传统 Spring MVC (Tomcat):
  200 个请求 → 200 个工作线程 → 200 个栈帧（每个约 1MB）→ 200MB 内存
  每个线程在等待 I/O 时被阻塞，无法处理其他请求

WebFlux (Reactor Netty):
  200 个请求 → 4 个 EventLoop 线程 → 4 个栈帧 → ~16MB 内存
  每个线程在等待 I/O 时不阻塞，继续处理其他连接的请求
  I/O 完成后通过回调/信号唤醒对应的请求处理链
```

⚠️ **踩坑提醒**：WebFlux 不是"万能加速器"。如果你的业务逻辑主要是 CPU 密集型计算（如图像处理、加密解密），WebFlux 的线程模型优势不明显——因为 CPU 计算不能"等"，4 个线程就是 4 个线程的计算能力。WebFlux 的优势在 I/O 密集型场景：大量请求、每个请求的大部分时间在等 I/O（数据库、HTTP 调用）。

---

## 二、WebFlux 全景架构：从 HTTP 请求到你的 Controller

### 2.1 完整架构图

```
                           Spring WebFlux 响应式架构全景
═══════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                        客户端 (Browser / Mobile / CLI)               │
  └──────────────────────────────────┬──────────────────────────────────┘
                                     │ HTTP/HTTPS
                                     ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                     Reactor Netty (传输层)                           │
  │                                                                     │
  │  ┌─────────────────────────────────────────────────────────────┐   │
  │  │              Netty EventLoop Group (少量线程)                 │   │
  │  │   EventLoop-1   EventLoop-2   EventLoop-3   EventLoop-4      │   │
  │  │      │              │              │              │          │   │
  │  │   Channel-1..N   Channel-1..M   Channel-1..K   Channel-1..L  │   │
  │  └─────────────────────────────────────────────────────────────┘   │
  │                        │                                           │
  │           HTTP 请求解码 → HttpServerHandler                         │
  │                        │                                           │
  │           转换为 ServerHttpRequest / ServerHttpResponse             │
  └────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    WebFlux 核心处理层                                │
  │                                                                     │
  │  HttpHandler (适配传输层)                                            │
  │       │                                                             │
  │       ▼                                                             │
  │  WebHandlerChain (Filter 链)                                         │
  │       │                                                             │
  │       ▼                                                             │
  │  DispatcherHandler (前端控制器)                                       │
  │       │                                                             │
  │       ├─► HandlerMapping (路由匹配)                                   │
  │       │      ├─ RequestMappingHandlerMapping (@Controller 路由)      │
  │       │      └─ RouterFunctionMapping (函数式路由)                    │
  │       │                                                             │
  │       ├─► HandlerAdapter (处理器适配)                                 │
  │       │      └─ RequestMappingHandlerAdapter                         │
  │       │           ├─ 参数解析 (HandlerMethodArgumentResolver)        │
  │       │           └─ 返回值处理 (HandlerResultHandler)                │
  │       │                                                             │
  │       └─► HandlerResultHandler (结果处理)                              │
  │              ├─ ResponseBodyResultHandler (Mono/Flux → JSON)         │
  │              ├─ ViewResolutionResultHandler (模板渲染)               │
  │              └─ ServerSentEventResultHandler (SSE)                  │
  └────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    Controller 层 (用户代码)                           │
  │                                                                     │
  │  @RestController                                                     │
  │  class UserController {                                              │
  │      @GetMapping("/users/{id}")                                     │
  │      fun getUser(@PathVariable id: String): Mono<User> {            │
  │          return userRepository.findById(id)  // 返回 Mono<User>     │
  │      }                                                              │
  │  }                                                                  │
  └────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │              响应式数据访问层 (R2DBC / Reactive Redis)                 │
  │                                                                     │
  │  ReactiveCrudRepository.findById(id): Mono<T>                       │
  │      │                                                              │
  │      └─► R2DBC → Reactor Netty → MySQL/PostgreSQL (非阻塞驱动)      │
  └─────────────────────────────────────────────────────────────────────┘
```

关键在于：**每一层都返回 Reactor 的 `Mono` 或 `Flux`，整个调用链是异步非阻塞的，最终在 Netty 的少量 EventLoop 线程上完成所有 I/O 操作。**

### 2.2 一个请求的完整旅程

用一个真实场景走一遍：用户通过浏览器访问 `GET /users/123`，获取用户信息。

**第 1 步：HTTP 请求到达 Netty**

浏览器发送 HTTP 请求，到达 Reactor Netty 的 EventLoop 线程。EventLoop 线程解码 HTTP 请求，转换为 `ServerHttpRequest` 和 `ServerHttpResponse`。

**第 2 步：WebFlux 框架接管**

`HttpHandler` → `WebHandlerChain`（Filter 链）→ `DispatcherHandler`。每一层都返回 `Mono<Void>`，通过 `flatMap` 串联。此时 **EventLoop 线程没有阻塞**——它只是在构建操作符链（装配时）。

**第 3 步：路由匹配**

`HandlerMapping` 找到 `@GetMapping("/users/{id}")` 对应的 Controller 方法。匹配过程是异步的。

**第 4 步：调用 Controller 方法**

`HandlerAdapter` 调用你的 Controller 方法。你的方法返回 `Mono<User>`——注意，此时 **数据库查询还没有执行**！返回的只是一个"承诺"（装配时的操作符链）。

**第 5 步：HandlerResultHandler 订阅**

`ResponseBodyResultHandler` 拿到 Controller 返回的 `Mono<User>`，订阅它——这一刻，数据库查询才真正开始执行（执行时）。

**第 6 步：数据库查询（异步）**

R2DBC 驱动通过 Reactor Netty 发起非阻塞数据库查询。EventLoop 线程发送查询请求后不阻塞，继续处理其他连接。

**第 7 步：结果返回**

数据库返回结果，R2DBC 解析为 `User` 对象，通过 `onNext` 信号传递给操作符链。`ResponseBodyResultHandler` 将 `User` 序列化为 JSON，写入 Netty Channel，HTTP 响应发回浏览器。

整个过程中，EventLoop 线程从未阻塞等待。它在等数据库返回的间隙，处理了其他几十个请求。

---

## 三、HTTP 请求 = Mono：WebFlux 如何基于 Reactor 构建

### 3.1 核心设计理念

WebFlux 的核心设计思路就是：**将 HTTP 请求/响应建模为 Reactor 的 `Publisher`，将请求处理建模为操作符链。**

每一个 HTTP 请求被建模为一个 `Mono<Void>`——当响应完全写入时，该 Mono 完成：

```java
// WebFlux 的核心接口
public interface WebHandler {
    Mono<Void> handle(ServerWebExchange exchange);
}
```

从 Reactor 视角看：
- `Mono<Void>` 的 `onComplete` 信号 = HTTP 响应已完全写入
- `Mono<Void>` 的 `onError` 信号 = 处理过程中发生异常
- `Mono<Void>` 的 `onSubscribe` 信号 = 请求处理链已接通
- 处理过程中的异步 I/O（如数据库查询）= 操作符链中的 `flatMap` 订阅内部 `Mono`/`Flux`

### 3.2 Controller 返回 Mono/Flux 的适配机制

当 Controller 方法返回 `Mono<User>` 时，Spring WebFlux 通过两个组件完成适配：

**HandlerAdapter 负责调用 Controller 方法：**

```
RequestMappingHandlerAdapter.handle(exchange, handler)
    │
    ├─► 解析参数 (@PathVariable, @RequestBody 等)
    │      └─ 对于 @RequestBody，反序列化为 Mono<T> 或 T
    │
    ├─► 调用 Controller 方法
    │      └─ 返回 Mono<User> / Flux<User> / User / String 等
    │
    └─► 封装为 HandlerResult
           └─ HandlerResult 包含返回值和返回类型信息
```

**HandlerResultHandler 负责将返回值写入 HTTP 响应：**

```
ResponseBodyResultHandler.handleResult(exchange, result)
    │
    ├─► 获取返回值（Mono<User> 或 Flux<User>）
    │
    ├─► 使用 HttpMessageWriter 序列化
    │      └─ HttpMessageWriter.write(Publisher<T>, mediaType)
    │
    └─► 将序列化结果写入 Netty Channel
           └─ 通过 Reactor Netty 的 ByteBufMono/Flux 完成
```

关键在于 `HttpMessageWriter.write()` 接收的是 `Publisher<T>`——它直接订阅这个 Publisher，将产出的数据写入 HTTP 响应。这里完美利用了 Reactor 的背压机制：**Netty 的 Channel 在可写时才 request(1)，在不可写时（缓冲区满）暂停 request，实现 HTTP 层面的背压。**

### 3.3 WebClient：用 Reactor 封装 HTTP 请求

Spring WebClient 是 RestTemplate 的响应式替代品：

```java
WebClient client = WebClient.create("http://api.example.com");

Mono<User> user = client.get()
    .uri("/users/{id}", 123)
    .retrieve()
    .bodyToMono(User.class);
```

从 Reactor 视角看，`bodyToMono()` 就是一个异步数据源——类似于 `Mono.fromCallable()`，但底层是 Netty 的非阻塞 I/O 而非线程池的阻塞调用。

**装配时**：只构建请求配置（URL、Header 等），不执行任何网络 I/O。
**执行时**（subscribe 时）：Reactor Netty 发起 TCP 连接，EventLoop 线程处理连接和请求发送，响应到达后 ByteBuf 被解析为 User。

⚠️ **踩坑提醒**：很多人写了 `WebClient` 调用后忘记 subscribe（或没有在链路中触发订阅），结果发现 HTTP 请求根本没发出去。记住：**没有 subscribe，就没有执行。** 在 WebFlux 中，框架会帮你 subscribe Controller 返回的 Mono/Flux，但如果你在 Controller 内部单独创建了 WebClient 请求却没有串入返回链，那个请求就不会执行。

---

## 四、背压端到端传递：从客户端到数据库

### 4.1 HTTP 响应的背压链

WebFlux 的背压传递是一个从 Netty Channel 到 Reactor Publisher 的完整链路：

```
消费者端 (客户端读取响应慢)
    │
    ▼
TCP 滑动窗口缩小
    │
    ▼
Netty Channel 的 isWritable() 返回 false
    │
    ▼
Reactor Netty 的 ChannelOutboundHandler 暂停 request(n)
    │
    ▼
HttpMessageWriter 的 Publisher 被 request(0)（暂停）
    │
    ▼
HandlerResultHandler 的下游 Subscriber 停止 request
    │
    ▼
Controller 返回的 Flux<User> 停止产出数据
    │
    ▼
R2DBC 查询暂停读取数据库结果集
    │
    ▼
数据库 TCP 连接的滑动窗口缩小
    │
    ▼
数据库暂停发送数据
```

用餐厅类比：客人吃得很慢 → 服务员不急着端新菜 → 后厨不急着做新菜 → 采购不急着买新食材。整条链路自动减速，没有人在"等"，只是"慢一点"。

**如果去掉背压会怎样？** Controller 返回的 `Flux<User>` 会以最大速度产出数据，全部缓冲在内存中。对于大数据量流式响应（如导出百万行 CSV），这会导致 OOM。有了背压，数据产出速率严格受控于客户端的消费速率，内存使用保持恒定。

### 4.2 背压在 SSE（Server-Sent Events）中的作用

SSE 是 WebFlux 的典型应用场景：

```java
@GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<ServerSentEvent<Event>> streamEvents() {
    return Flux.interval(Duration.ofSeconds(1))
        .map(seq -> ServerSentEvent.<Event>builder()
            .id(String.valueOf(seq))
            .event("update")
            .data(new Event("tick-" + seq))
            .build());
}
```

如果客户端读取 SSE 数据的速度慢于服务器产出速度，背压机制自动调节：

1. 客户端 TCP 接收窗口缩小
2. Netty Channel 不可写
3. `ServerSentEventResultHandler` 停止 `request(n)`
4. `Flux.interval` 的 `request` 被暂停
5. 定时器虽然触发，但由于没有 `request`，不产出数据
6. 客户端消费后，TCP 窗口扩大，Netty 可写，`request` 恢复

这种自动调节完全基于 Reactor 的背压协议，无需任何额外代码。

---

## 五、为什么在 WebFlux 里用 JDBC 会出问题

### 5.1 问题复现

这是 WebFlux 中最常见的踩坑：

```java
@GetMapping("/users/{id}")
public Mono<User> getUser(@PathVariable String id) {
    // 错误！JDBC 是阻塞调用
    User user = jdbcTemplate.queryForObject(
        "SELECT * FROM users WHERE id = ?", 
        new Object[]{id}, 
        UserRowMapper.INSTANCE);
    return Mono.just(user);
}
```

这段代码的问题：

1. `jdbcTemplate.queryForObject()` 在 EventLoop 线程上阻塞执行
2. 阻塞期间，该 EventLoop 线程无法处理其他连接的请求
3. 如果 4 个 EventLoop 线程同时被阻塞，整个服务器就无法接受新连接
4. 相当于把 WebFlux 退化成了"4 线程的 Spring MVC"，吞吐量急剧下降

用餐厅类比：你只有 4 个服务员，其中 4 个都站在后厨等菜。新来的客人没人接待，餐厅瘫痪了。

### 5.2 正确做法

```java
// 方案1：使用 R2DBC（非阻塞驱动）—— 推荐做法
@GetMapping("/users/{id}")
public Mono<User> getUser(@PathVariable String id) {
    return r2dbcTemplate.queryForObject(
        "SELECT * FROM users WHERE id = $1", 
        User.class, id);
}

// 方案2：如果必须用 JDBC，在 boundedElastic 上执行
@GetMapping("/users/{id}")
public Mono<User> getUser(@PathVariable String id) {
    return Mono.fromCallable(() -> 
        jdbcTemplate.queryForObject("SELECT * FROM users WHERE id = ?", 
            new Object[]{id}, UserRowMapper.INSTANCE))
    .subscribeOn(Schedulers.boundedElastic());
}
```

方案 2 虽然可以工作，但 `boundedElastic()` 线程池本质上还是阻塞模型，只是把阻塞从 EventLoop 线程转移到了工作线程。这削弱了 WebFlux 的非阻塞优势。方案 1 使用 R2DBC 才是真正的响应式方案。

⚠️ **踩坑提醒**：如果你在 WebFlux 项目中发现性能没有提升甚至更差，第一个排查的就是有没有在 EventLoop 线程上做阻塞调用。常见阻塞调用包括：JDBC、`Thread.sleep()`、同步 HTTP 客户端（如 RestTemplate）、文件 I/O（非 NIO）。Reactor 提供了 `BlockHound` 工具可以检测阻塞调用：

```java
// 在启动时加入 BlockHound 检测
BlockHound.install();
```

---

## 六、WebFlux 的线程模型

### 6.1 EventLoop = Reactor 的 Scheduler

WebFlux 的少量 EventLoop 线程本质上是 Reactor 的 `Scheduler`。当 HTTP 请求到达时：

1. EventLoop 线程解码 HTTP 请求
2. EventLoop 线程调用 `WebHandler.handle(exchange)` 返回 `Mono<Void>`
3. EventLoop 线程 subscribe 这个 `Mono<Void>`，触发整个处理链
4. 如果处理链中包含异步 I/O（如数据库查询），`flatMap` 会订阅内部 Publisher
5. **EventLoop 线程不阻塞等待**，继续处理其他连接
6. 异步 I/O 完成后，回调在 EventLoop 线程（或指定的 Scheduler）上继续执行

### 6.2 与 Reactor Scheduler 的对应关系

| Reactor Scheduler | WebFlux 用途 | 对应组件 |
|---|---|---|
| Netty EventLoop Group | HTTP I/O 读写 | Reactor Netty HttpServer |
| `boundedElastic()` | 阻塞操作（应避免） | `subscribeOn(boundedElastic())` |
| `parallel()` | CPU 密集计算 | `publishOn(parallel())` |
| 自定义 Scheduler | 业务线程池 | `publishOn(myScheduler)` |

### 6.3 装配时/执行时分离在 WebFlux 中的体现

```java
@GetMapping("/users")
public Flux<User> listUsers() {
    // 装配时：构建操作符链，不执行任何 I/O
    return userRepository.findAll()
        .map(user -> {
            user.setName(user.getName().toUpperCase());
            return user;
        })
        .filter(user -> user.getAge() > 18);
        // 没有 subscribe()！返回的是未执行的操作符链
}
```

当 `DispatcherHandler` 调用这个 Controller 方法时，只触发了装配时——构建了 `FluxFilter → FluxMap → R2DBC Query` 的操作符链。直到 `HandlerResultHandler` subscribe 这个 Flux 时，才真正开始执行数据库查询。

**如果 WebFlux 不利用装配时/执行时分离会怎样？**

1. 数据库查询在调用线程（EventLoop）上同步执行，阻塞 EventLoop
2. 查询结果必须完整加载到内存，无法流式输出
3. 背压失效——数据在 subscribe 之前就已经产出了
4. 无法支持超时取消——subscribe 之前无法 cancel

---

## 七、WebFlux 的错误处理

### 7.1 两层错误处理

WebFlux 的错误处理结合了 Spring 的声明式异常处理和 Reactor 的响应式错误恢复：

**Spring 层面——@ExceptionHandler：**

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(UserNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(UserNotFoundException ex) {
        return ResponseEntity.status(404)
            .body(new ErrorResponse("USER_NOT_FOUND", ex.getMessage()));
    }
}
```

当 Controller 返回的 `Mono<User>` 产生 `onError(UserNotFoundException)` 信号时，`DispatcherHandler` 会捕获该错误，交给 `@ExceptionHandler` 方法处理。

**Reactor 层面——onErrorResume：**

```java
@GetMapping("/users/{id}")
public Mono<User> getUser(@PathVariable String id) {
    return userRepository.findById(id)
        .switchIfEmpty(Mono.error(new UserNotFoundException(id)))
        .onErrorResume(e -> {
            log.error("Failed to find user: {}", id, e);
            return Mono.error(new UserServiceException("Internal error", e));
        });
}
```

`onErrorResume` 是 Reactor 的错误恢复操作符，在装配时被添加到操作符链中。执行时，如果 `findById` 产生 `onError`，`onErrorResume` 的 lambda 被调用，返回替代的 `Mono`。

### 7.2 错误传播过程

1. Controller 内部的 `Mono` 产生 `onError` 信号
2. `onError` 沿操作符链向下游传播
3. `HandlerResultHandler` 接收到 `onError`，交给 `DispatcherHandler` 的错误处理链
4. `@ExceptionHandler` 方法被调用，返回替代响应
5. 替代响应被写回客户端

整个过程是异步非阻塞的——错误处理不需要额外的线程，在产生错误的 EventLoop 线程上直接完成。

---

## 八、Context 传播：WebFlux 的请求上下文

### 8.1 为什么不用 ThreadLocal

在 Spring MVC 中，请求上下文（如 requestId、用户认证信息）通常存在 `ThreadLocal` 中。一个请求从头到尾在一个线程上执行，`ThreadLocal` 自然可用。

但在 WebFlux 中，一个请求的处理可能跨越多个线程：

```
EventLoop线程 → 数据库回调线程 → 序列化线程
```

`ThreadLocal` 无法自动跨线程传递。Reactor 的 `Context` 与操作符链绑定，而非与线程绑定，天然解决了跨线程传播问题。

### 8.2 使用方式

```java
// 在 Filter 中将请求信息放入 Context
@Component
public class RequestIdFilter implements WebFilter {
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String requestId = exchange.getRequest().getHeaders().getFirst("X-Request-Id");
        if (requestId == null) {
            requestId = UUID.randomUUID().toString();
        }
        
        // 将 requestId 注入 Reactor Context
        return chain.filter(exchange)
            .contextWrite(Context.of("requestId", requestId));
    }
}

// 在 Controller 中从 Context 读取
@GetMapping("/users/{id}")
public Mono<User> getUser(@PathVariable String id, ContextView ctx) {
    String requestId = ctx.getOrDefault("requestId", "unknown");
    log.info("[{}] Finding user: {}", requestId, id);
    return userRepository.findById(id);
}
```

### 8.3 Context 传播方向

Reactor `Context` 的核心特性是**从下游向上游传播**。

在 WebFlux 中，`WebFilter` 链是最外层（下游），Controller 是内层（上游）。Filter 中的 `contextWrite` 将信息注入 Context，Controller 中的操作符可以读取这些信息。

这就像餐厅的"点单备注"——客人在最外层（下单时）写的备注，沿着订单传递到最内层（后厨），后厨能看到。

### 8.4 自动 Context 传播与 ThreadLocal 桥接

在 Reactor 3.5+ 中，`Hooks.enableAutomaticContextPropagation()` 可以自动将 Reactor `Context` 中的值同步到 `ThreadLocal`。这对于需要与使用 `ThreadLocal` 的传统库（如 MDC 日志、Spring Security）集成时非常有用：

```java
// 启用自动 Context 传播
Hooks.enableAutomaticContextPropagation();

// 现在 Reactor Context 中的值会自动同步到 ThreadLocal
// MDC.get("requestId") 在操作符链内部自动可用
```

---

## 九、Fuseable 在 WebFlux 中的应用

当 Controller 返回 `Flux<User>` 且数据源支持 `Fuseable`（如 R2DBC 的流式查询），WebFlux 的序列化过程可以受益于队列融合：

```
R2DBC Flux<User> (Fuseable)
    → map(user -> JSON bytes) (Fuseable, 因为上游是 Fuseable)
    → HttpMessageWriter.write (检查 Fuseable, 使用 poll() 而非 onNext())
```

融合模式下，`map` 操作符不创建独立的 Subscriber，而是直接使用上游的队列 `poll()` 方法。这减少了每次 `onNext` 的方法调用开销，在大数据量流式响应场景下可以显著提升吞吐量。

用工厂类比：正常模式下，工位A把半成品"递给"工位B（方法调用），工位B再"递给"工位C。融合模式下，三个工位共用一个传送带（共享队列），半成品直接在传送带上流动，省去了"递"的动作。

---

## 十、何时选择 WebFlux

### 10.1 WebFlux 的优势场景

- **微服务网关**：大量 HTTP 请求转发，WebClient 非阻塞调用下游服务
- **实时数据推送**：SSE、WebSocket，Flux 天然适合流式输出
- **高并发 API**：每秒数万请求，但每个请求的 CPU 计算量很小
- **流式数据处理**：大数据量查询结果以 Flux 流式返回，避免全量加载到内存

### 10.2 WebFlux 的劣势

- **学习曲线陡峭**：响应式编程思维与传统命令式编程差异大
- **调试困难**：调用栈不连续，错误堆栈难以追踪（需要 `checkpoint()` 辅助）
- **生态限制**：JDBC 不支持非阻塞，必须使用 R2DBC；某些第三方库不支持响应式
- **CPU 密集型任务不适用**：如果请求处理主要是 CPU 计算，线程模型优势不明显

⚠️ **踩坑提醒**：不要为了"技术先进"而盲目选择 WebFlux。如果你的团队对响应式编程不熟悉、项目大量依赖阻塞式库（JDBC、同步 HTTP 客户端），或者主要是 CPU 密集型业务，Spring MVC 可能是更好的选择。WebFlux 的价值在高并发 I/O 密集型场景才能真正体现。

---

## 十一、Spring MVC vs WebFlux 请求处理流程对比

### Spring MVC 的请求处理流程

```
客户端请求 → Tomcat Connector (NIO 接收) → Worker Thread Pool
    │
    ▼
DispatcherServlet.doDispatch()
    │
    ├─► HandlerMapping.findHandler() → HandlerExecutionChain
    ├─► HandlerAdapter.handle() → ModelAndView
    │      └─ Controller 方法在 Worker Thread 上同步执行
    │      └─ 如果调用 JDBC，Worker Thread 阻塞等待
    └─► ViewResolver.resolveViewName() → View → 渲染响应
    │
    ▼
Tomcat 将响应写回客户端
Worker Thread 归还到线程池
```

### WebFlux 的请求处理流程

```
客户端请求 → Reactor Netty EventLoop (NIO 接收)
    │
    ▼
HttpHandler.handle() → WebHandlerChain → DispatcherHandler
    │
    ├─► HandlerMapping.getHandler() → Mono<HandlerResult>
    │      └─ 路由匹配是异步的，不阻塞 EventLoop
    │
    ├─► HandlerAdapter.handle() → Mono<HandlerResult>
    │      └─ Controller 方法返回 Mono/Flux，不等待结果
    │
    └─► HandlerResultHandler.handleResult() → Mono<Void>
           └─ 订阅 Controller 返回的 Publisher
           └─ 数据产出后写入 Netty Channel
    │
    ▼
Mono<Void>.subscribe() 触发整个处理链
EventLoop 线程在 I/O 等待期间处理其他连接
```

---

## 十二、归纳表格

### Spring MVC vs WebFlux 对比表

| 维度 | Spring MVC（传统餐厅） | Spring WebFlux（自助餐厅） |
|------|-----------|----------------|
| **底层框架** | Servlet API | Reactive Streams + Reactor Core |
| **默认容器** | Tomcat (Servlet 容器) | Reactor Netty (非阻塞) |
| **线程模型** | 一请求一线程（200 线程） | 少量 EventLoop 线程（4-16 线程） |
| **I/O 模型** | 同步阻塞 I/O | 异步非阻塞 I/O |
| **请求处理** | `DispatcherServlet` 同步调用 | `DispatcherHandler` 异步链式调用 |
| **返回值类型** | `User` / `List<User>` / `ModelAndView` | `Mono<User>` / `Flux<User>` |
| **数据库访问** | JDBC (阻塞) | R2DBC (非阻塞) |
| **HTTP 客户端** | RestTemplate (阻塞) | WebClient (非阻塞) |
| **背压** | 无，全量缓冲 | 端到端背压（客户端→Netty→Reactor→DB） |
| **内存占用** | ~200MB (200线程 × 1MB栈) | ~16MB (4线程 × 4MB栈) |
| **装配时/执行时** | 不区分（方法调用即执行） | 严格分离（Controller 返回 Publisher，subscribe 才执行） |
| **Context 传播** | ThreadLocal | Reactor Context（跨线程安全） |
| **错误处理** | `@ExceptionHandler` + try-catch | `@ExceptionHandler` + `onErrorResume` / `onErrorMap` |
| **流式响应** | 需 Servlet 3.1 异步支持 | 原生支持（Flux + SSE/WebSocket） |
| **CPU 密集型** | 适合 | 需 `publishOn(parallel())` 切换 |
| **I/O 密集型** | 线程数受限 | 极大优势 |
| **学习曲线** | 低 | 高 |
| **调试难度** | 低（完整调用栈） | 高（需 `checkpoint()` 辅助） |
| **生态支持** | 最广泛 | 仍在发展（R2DBC 尚不如 JDBC 成熟） |

### WebFlux 核心组件与 Reactor 机制对应表

| WebFlux 组件 | Reactor 机制 | 对应关系 | 通俗解释 |
|-------------|------------|---------|---------|
| `WebHandler.handle()` | `Mono<Void>` | HTTP 请求处理 = Mono 的 onComplete | 做完菜叫号 |
| `DispatcherHandler` | 操作符链编排者 | 构建 `flatMap` 链串联各处理步骤 | 餐厅经理调度 |
| `HandlerAdapter` | 装配时构建 | 调用 Controller 方法，获取 Publisher（不 subscribe） | 把订单传给后厨 |
| `HandlerResultHandler` | `subscribe()` 触发 | 订阅 Controller 返回的 Publisher，写入响应 | 按下启动按钮 |
| `WebFilter` | `FluxOperator` 装饰器 | Filter 链 = 操作符链的 `flatMap` 嵌套 | 进门安检流程 |
| `WebClient` | `Mono.fromCallable()` 的异步变体 | 装配时构建请求配置，subscribe 时发起 I/O | 外卖下单 |
| `ServerWebExchange.attributes` | Reactor `Context` | 请求上下文传播 | 点单备注 |
| `@ExceptionHandler` | `onErrorResume` | 错误恢复操作符 | 投诉处理流程 |
| SSE 流式响应 | `Flux` + `onNext` 逐条推送 | 每收到 onNext 立即写入 HTTP 响应 | 菜做好一道端一道 |
| R2DBC 查询 | `Flux` (Fuseable) | 流式查询结果，支持队列融合 | 后厨流水线出菜 |
| EventLoop 线程 | `Schedulers.parallel()` | 少量线程处理大量连接 | 4 个服务员管全场 |
| 背压传递 | `Subscription.request(n)` | Netty Channel 可写性 → request(n) | 客人吃得慢，后厨自动减速 |

---

## 十三、总结

Spring WebFlux 不是一个独立的 Web 框架，而是 Reactor Core 在 Web 领域的应用延伸。用一句话概括：

**WebFlux 就是把 HTTP 请求/响应当作 Reactor 的 Mono/Flux 来处理——装配时构建操作符链，执行时 subscribe 触发异步处理，少量 EventLoop 线程通过回调机制处理大量并发连接。**

从 Reactor 视角看 WebFlux，四个核心对应关系：

1. **HTTP 请求/响应 = Mono**：WebFlux 将 HTTP 请求处理建模为 `Mono<Void>`，完美利用了 Reactor 的装配时/执行时分离——Controller 方法返回 Publisher 时不执行任何 I/O，只有 `HandlerResultHandler` subscribe 后才触发处理链。

2. **背压端到端传递**：WebFlux 的背压从客户端 TCP 窗口一路传递到数据库查询游标，每一层都基于 Reactor 的 `Subscription.request(n)` 协议。流式响应和大数据量查询能够自动调节速率，避免 OOM。

3. **线程模型 = Reactor Scheduler**：WebFlux 的少量 EventLoop 线程本质上是 Reactor 的 `Scheduler`，通过操作符链的异步回调机制处理大量并发连接，而非通过线程池的阻塞等待。

4. **Context 传播 = Reactor Context**：WebFlux 利用 Reactor 的 `Context` 机制在请求处理链中传播请求 ID、安全上下文等信息，天然解决了跨线程传播问题——`ThreadLocal` 在 WebFlux 中不可靠，因为一个请求可能跨越多个线程。

理解 WebFlux 的前提是深入理解 Reactor Core。前面 15 篇源码分析中揭示的所有机制——装配时/执行时分离、操作符链、背压协议、Scheduler 线程模型、Context 传播、Fuseable 融合、错误处理——都在 WebFlux 中发挥着关键作用。WebFlux 只是 Reactor 在 Web 场景下的一个具体应用，其架构设计完全建立在 Reactor 的核心机制之上。
