# 从 Reactor 视角理解 WebFlux 与 Spring 响应式架构

> **Reactor Core 源码深度研究系列 · 第 16 篇**
>
> 基于 Reactor Core 的核心机制（装配时/执行时分离、背压驱动、操作符链、Context 传播、Scheduler 线程模型），从 Reactor 的视角反向审视 Spring WebFlux 的架构设计，理解 WebFlux 如何在 Reactor 之上构建完整的响应式 Web 栈。

---

## 一、WebFlux 全景架构图

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
  │                                                                     │
  │      @GetMapping("/users")                                          │
  │      fun listUsers(): Flux<User> {                                  │
  │          return userRepository.findAll()      // 返回 Flux<User>    │
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
  │                                                                     │
  │  ReactiveCrudRepository.findAll(): Flux<T>                          │
  │      │                                                              │
  │      └─► R2DBC → Reactor Netty → MySQL/PostgreSQL (流式查询)        │
  └─────────────────────────────────────────────────────────────────────┘
```

这张图展示了 WebFlux 从传输层到用户代码的完整调用链。关键在于：**每一层都返回 Reactor 的 `Mono` 或 `Flux`，整个调用链是异步非阻塞的，最终在 Netty 的少量 EventLoop 线程上完成所有 I/O 操作。**

---

## 二、WebFlux 如何基于 Reactor 构建

### 2.1 从 Reactor 视角看 WebFlux 的本质

经过前 15 篇的源码分析，我们已经深入理解了 Reactor Core 的核心机制：

- **装配时与执行时分离**：操作符链构建阶段不执行任何代码，`subscribe()` 才触发数据流动
- **背压驱动**：消费者通过 `request(n)` 控制生产者速率
- **操作符链**：`map`、`filter`、`flatMap` 等操作符构建数据处理管道
- **Scheduler 线程模型**：`publishOn`、`subscribeOn` 切换执行线程
- **Context 传播**：`Context` 沿操作符链从下游向上游传播，`contextWrite` 注入键值对
- **Fuseable 融合**：相邻操作符之间通过队列融合避免数据拷贝

WebFlux 的核心设计思路就是：**将 HTTP 请求/响应建模为 Reactor 的 `Publisher`，将请求处理建模为操作符链。**

### 2.2 HTTP 请求 = Mono，HTTP 响应 = Mono

在 WebFlux 中，每一个 HTTP 请求被建模为一个 `Mono<Void>`——当响应完全写入时，该 Mono 完成。这不是巧合，而是精心设计：

```java
// WebFlux 的核心接口（概念模型）
public interface WebHandler {
    Mono<Void> handle(ServerWebExchange exchange);
}
```

`ServerWebExchange` 封装了 HTTP 请求和响应。`handle()` 返回 `Mono<Void>` 表示"处理完成"的信号。这个 `Mono<Void>` 被订阅后，整个处理链就开始执行。

从 Reactor 视角看：
- `Mono<Void>` 的 `onComplete` 信号表示 HTTP 响应已完全写入
- `Mono<Void>` 的 `onError` 信号表示处理过程中发生异常
- `Mono<Void>` 的 `onSubscribe` 信号表示请求处理链已接通
- 如果处理过程中需要异步 I/O（如数据库查询），操作符链中的 `flatMap` 会订阅内部的 `Mono`/`Flux`，在 I/O 完成后继续传播信号

### 2.3 Controller 返回 Mono/Flux 的适配机制

当 Controller 方法返回 `Mono<User>` 或 `Flux<User>` 时，Spring WebFlux 通过 `HandlerAdapter` 和 `HandlerResultHandler` 将其适配到响应式处理链：

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
           └─ 这一步通过 Reactor Netty 的 ByteBufMono/Flux 完成
```

关键在于 `HttpMessageWriter.write()` 接收的是 `Publisher<T>`——它直接订阅这个 Publisher，将产出的数据写入 HTTP 响应。这里完美利用了 Reactor 的背压机制：**Netty 的 Channel 在可写时才 request(1)，在不可写时（缓冲区满）暂停 request，实现 HTTP 层面的背压。**

### 2.4 WebClient：用 Reactor 封装 HTTP 请求

Spring WebClient 是 RestTemplate 的响应式替代品，它直接基于 Reactor Netty 构建：

```java
WebClient client = WebClient.create("http://api.example.com");

Mono<User> user = client.get()
    .uri("/users/{id}", 123)
    .retrieve()
    .bodyToMono(User.class);

// 等价于以下 Reactor 操作链（概念模型）：
// HttpClient.request(HttpMethod.GET, uri)
//     .flatMap(response -> response.receive()
//         .aggregate()
//         .map(byteBuf -> deserialize(byteBuf, User.class)))
//     .subscribeOn(reactor.netty.http.client.HttpClient)
```

WebClient 内部的调用链：

```
WebClient.get().uri().retrieve().bodyToMono(User.class)
    │
    ├─► 构建请求配置（装配时，不执行网络 I/O）
    │
    ├─► bodyToMono() 返回一个 Mono<User>
    │      这个 Mono 在被 subscribe 时才会发起 HTTP 请求
    │
    └─► subscribe 后：
           ├─ Reactor Netty HttpClient 发起 TCP 连接
           ├─ EventLoop 线程处理连接和请求发送
           ├─ 响应到达后，ByteBuf 被解析为 User
           └─ Mono<User>.onNext(user) → onComplete()
```

**从 Reactor 视角看**，WebClient 的 `bodyToMono()` 就是一个异步数据源——类似于 `Mono.fromCallable()`，但底层是 Netty 的非阻塞 I/O 而非线程池的阻塞调用。装配时只构建请求配置，执行时（subscribe 时）才真正发起网络请求。

---

## 三、WebFlux 的背压传递

### 3.1 HTTP 响应的背压链

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

这条背压链从客户端的 TCP 接收窗口一路传递到数据库的查询游标，每一层都是非阻塞的。这是 WebFlux 相对于 Spring MVC 最根本的架构优势——**背压是端到端的，从网络层到数据层一气呵成。**

### 3.2 与 Reactor 背压机制的对应

在前面的篇章中，我们分析了 Reactor 的背压机制：
- `Subscription.request(n)` 从下游向上游传播
- `Operators.addCap()` 使用 CAS 无锁方式累加需求
- `Fuseable` 队列融合避免了 request 的逐层传递

WebFlux 完全复用了这套机制。`HttpMessageWriter.write(Publisher<T>)` 返回的 `Mono<Void>` 内部会订阅传入的 Publisher，并在 Netty Channel 可写时 request(1)，不可写时暂停。这与 `Flux.range(1, 100).map(fn).subscribeOn(scheduler)` 中的背压传递本质相同。

**如果去掉背压会怎样？** 假设 WebFlux 不使用背压，Controller 返回的 `Flux<User>` 会以最大速度产出数据，全部缓冲在内存中。对于大数据量的流式响应（如导出百万行 CSV），这会导致 OOM。有了背压，数据产出速率严格受控于客户端的消费速率，内存使用保持恒定。

---

## 四、WebFlux 的线程模型

### 4.1 少量 EventLoop 线程处理大量连接

WebFlux 默认使用 Reactor Netty 作为 HTTP 服务器，其线程模型基于 Netty 的 EventLoop：

```
传统 Spring MVC (Tomcat):
  200 个请求 → 200 个工作线程 → 200 个栈帧（每个约 1MB）→ 200MB 内存
  每个线程在等待 I/O 时被阻塞，无法处理其他请求

WebFlux (Reactor Netty):
  200 个请求 → 4 个 EventLoop 线程 → 4 个栈帧 → ~4MB 内存
  每个线程在等待 I/O 时不阻塞，继续处理其他连接的请求
  I/O 完成后通过回调/信号唤醒对应的请求处理链
```

从 Reactor 视角看，EventLoop 线程就是 Reactor 的 `Scheduler`。Reactor Netty 的 `HttpServer` 默认使用 `Schedulers.boundedElastic()` 或 Netty 自身的 EventLoop Group 作为执行线程。当 HTTP 请求到达时：

1. EventLoop 线程解码 HTTP 请求
2. EventLoop 线程调用 `WebHandler.handle(exchange)` 返回 `Mono<Void>`
3. EventLoop 线程 subscribe 这个 `Mono<Void>`，触发整个处理链
4. 如果处理链中包含异步 I/O（如数据库查询），操作符链中的 `flatMap` 会订阅内部 Publisher
5. EventLoop 线程不阻塞等待，而是继续处理其他连接
6. 当异步 I/O 完成后，回调在 EventLoop 线程（或指定的 Scheduler）上继续执行操作符链

### 4.2 与 Reactor Scheduler 的对应

在前面的篇章中，我们分析了 Reactor 的 `Schedulers`：
- `parallel()`：CPU 密集型任务，线程数 = CPU 核心数
- `boundedElastic()`：I/O 密集型任务，有界弹性线程池
- `single()`：单线程调度器
- `immediate()`：在当前线程执行

WebFlux 的线程模型对应关系：

| Reactor Scheduler | WebFlux 用途 | 对应组件 |
|---|---|---|
| Netty EventLoop Group | HTTP I/O 读写 | Reactor Netty HttpServer |
| `boundedElastic()` | 阻塞操作（应避免） | `subscribeOn(boundedElastic())` |
| `parallel()` | CPU 密集计算 | `publishOn(parallel())` |
| 自定义 Scheduler | 业务线程池 | `publishOn(myScheduler)` |

### 4.3 反例：在 WebFlux 中使用阻塞调用

**如果 WebFlux 中调用 JDBC 会怎样？**

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

**正确做法：**

```java
// 方案1：使用 R2DBC（非阻塞驱动）
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

---

## 五、对比 Spring MVC 与 WebFlux

### 5.1 架构层面对比

**Spring MVC 的请求处理流程：**

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

**WebFlux 的请求处理流程：**

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

### 5.2 从 Reactor 视角的核心差异

| 差异点 | Spring MVC | WebFlux |
|--------|-----------|---------|
| 请求处理模型 | 同步阻塞，一请求一线程 | 异步非阻塞，少量线程处理大量连接 |
| 返回值类型 | `User` / `List<User>` / `ModelAndView` | `Mono<User>` / `Flux<User>` |
| I/O 模型 | 阻塞 I/O（JDBC、阻塞 HTTP 客户端） | 非阻塞 I/O（R2DBC、WebClient） |
| 背压 | 无，全量缓冲 | 端到端背压，从客户端到数据库 |
| 线程数 | 线程数 = 并发请求数（通常 200） | 线程数 = CPU 核心数 × 2（通常 4-16） |
| 内存模型 | 每线程 ~1MB 栈空间 | 固定少量线程，内存占用稳定 |
| Reactor 角色 | 不使用 Reactor | 核心依赖，一切基于 Mono/Flux |

### 5.3 何时选择 WebFlux

WebFlux 的优势在高并发 I/O 密集型场景最为明显：

- **微服务网关**：大量 HTTP 请求转发，WebClient 非阻塞调用下游服务
- **实时数据推送**：SSE（Server-Sent Events）、WebSocket，Flux 天然适合流式输出
- **高并发 API**：每秒数万请求，但每个请求的 CPU 计算量很小
- **流式数据处理**：大数据量查询结果以 Flux 流式返回，避免全量加载到内存

WebFlux 的劣势：
- **学习曲线陡峭**：响应式编程思维与传统命令式编程差异大
- **调试困难**：调用栈不连续，错误堆栈难以追踪（需要 Reactor 的 `checkpoint()` 辅助）
- **生态限制**：JDBC 不支持非阻塞，必须使用 R2DBC；某些第三方库不支持响应式
- **CPU 密集型任务不适用**：如果请求处理主要是 CPU 计算，线程模型优势不明显

---

## 六、WebFlux 的错误处理

### 6.1 @ExceptionHandler + Reactor onErrorResume

WebFlux 的错误处理结合了 Spring 的声明式异常处理和 Reactor 的响应式错误恢复：

**Spring 层面的错误处理：**

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

当 Controller 返回的 `Mono<User>` 产生 `onError(UserNotFoundException)` 信号时，`DispatcherHandler` 会捕获该错误，交给 `@ExceptionHandler` 方法处理。这在 WebFlux 内部是通过 `onErrorResume` 操作符实现的——`DispatcherHandler` 在处理链末端添加了一个错误恢复操作符，将异常映射为替代的 `Mono<Void>`。

**Reactor 层面的错误处理：**

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

这里 `onErrorResume` 是 Reactor 的错误恢复操作符，它在装配时被添加到操作符链中。执行时，如果 `findById` 产生 `onError`，`onErrorResume` 的 lambda 会被调用，返回一个替代的 `Mono`。

### 6.2 从 Reactor 视角看错误传播

在 Reactor 源码中，我们分析了 `onError` 信号从上游向下游传播的机制。WebFlux 的错误处理完全基于这一机制：

1. Controller 内部的 `Mono` 产生 `onError` 信号
2. `onError` 沿操作符链向下游传播
3. `HandlerResultHandler` 接收到 `onError`，将其交给 `DispatcherHandler` 的错误处理链
4. `@ExceptionHandler` 方法被调用，返回替代响应
5. 替代响应被写回客户端

整个过程是异步非阻塞的——错误处理不需要额外的线程，在产生错误的 EventLoop 线程上直接完成。

---

## 七、WebFlux 的 Context 传播

### 7.1 ServerWebExchange.attributes → Reactor Context

WebFlux 利用 Reactor 的 `Context` 机制在请求处理链中传播上下文信息。`ServerWebExchange` 的 `attributes` 可以与 Reactor `Context` 互转：

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

### 7.2 从 Reactor 视角理解 Context 传播方向

在前面的篇章中，我们分析了 Reactor `Context` 的核心特性：**Context 从下游向上游传播。**

这意味着：
- `contextWrite` 操作符将键值对注入到 Context 中
- 上游操作符可以通过 `currentContext()` 读取这些键值对
- Context 在 subscribe 时沿操作符链从下游向上游传递

在 WebFlux 中，`WebFilter` 链是最外层（下游），Controller 是内层（上游）。Filter 中的 `contextWrite` 将信息注入 Context，Controller 中的操作符可以读取这些信息。这与 Reactor 的 Context 传播方向完全一致。

**如果不用 Reactor Context 会怎样？** 替代方案是使用 `ThreadLocal`。但在 WebFlux 中，一个请求的处理可能跨越多个线程（EventLoop → 数据库回调 → 序列化线程），`ThreadLocal` 无法自动跨线程传递。Reactor 的 `Context` 与操作符链绑定，而非与线程绑定，天然解决了跨线程传播问题。

### 7.3 自动 Context 传播与 ThreadLocal 桥接

在 Reactor 3.5+ 中，`Hooks.enableAutomaticContextPropagation()` 可以自动将 Reactor `Context` 中的值同步到 `ThreadLocal`。这对于需要与使用 `ThreadLocal` 的传统库（如 MDC 日志、Spring Security 的 SecurityContext）集成时非常有用：

```java
// 启用自动 Context 传播
Hooks.enableAutomaticContextPropagation();

// 现在 Reactor Context 中的值会自动同步到 ThreadLocal
// 例如，MDC 日志可以自动获取 requestId
exchange.getResponse().getHeaders().add("X-Request-Id", requestId);
chain.filter(exchange).contextWrite(Context.of("requestId", requestId));
// MDC.get("requestId") 在操作符链内部自动可用
```

---

## 八、WebFlux 与 Reactor 的深层对应关系

### 8.1 装配时 vs 执行时在 WebFlux 中的体现

WebFlux 的 Controller 方法返回 `Mono`/`Flux` 时，正好体现了 Reactor 的装配时/执行时分离：

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

**如果 WebFlux 不利用装配时/执行时分离会怎样？** 假设 Controller 方法在返回前就执行了数据库查询，那么：
1. 数据库查询在调用线程（EventLoop）上同步执行，阻塞 EventLoop
2. 查询结果必须完整加载到内存，无法流式输出
3. 背压失效——数据在 subscribe 之前就已经产出了
4. 无法支持超时取消——subscribe 之前无法 cancel

### 8.2 操作符链在 WebFlux 中的角色

WebFlux 的整个请求处理链本质上是一条 Reactor 操作符链：

```
Mono<Void> requestProcessing = 
    WebFilter1.filter(exchange)              // 外层 Filter
    .flatMap(ex -> WebFilter2.filter(ex))    // 内层 Filter
    .flatMap(ex -> DispatcherHandler.handle(ex))
    .flatMap(ex -> HandlerAdapter.handle(ex, handler))
    .flatMap(result -> HandlerResultHandler.handleResult(ex, result))
    .flatMap(voidMono -> writeResponse(exchange));
```

每一层都返回 `Mono`/`Flux`，通过 `flatMap` 串联。整个链在 `subscribe` 后才开始执行。这与 Reactor 源码中 `InternalFluxOperator.subscribe()` 的优化循环完美配合——每一层操作符的 `subscribeOrReturn` 创建对应的 Subscriber，循环向上游遍历，最终订阅数据源。

### 8.3 Fuseable 在 WebFlux 中的应用

当 Controller 返回 `Flux<User>` 且数据源支持 `Fuseable`（如 R2DBC 的流式查询），WebFlux 的序列化过程可以受益于队列融合：

```
R2DBC Flux<User> (Fuseable)
    → map(user -> JSON bytes) (Fuseable, 因为上游是 Fuseable)
    → HttpMessageWriter.write (检查 Fuseable, 使用 poll() 而非 onNext())
```

融合模式下，`map` 操作符不创建独立的 Subscriber，而是直接使用上游的队列 `poll()` 方法。这减少了每次 `onNext` 的方法调用开销，在大数据量流式响应场景下可以显著提升吞吐量。

---

## 九、WebFlux 的 SSE 与流式响应

### 9.1 Server-Sent Events 的响应式实现

SSE（Server-Sent Events）是 WebFlux 的典型应用场景：

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

这个例子展示了 WebFlux 流式响应的精髓：

1. `Flux.interval(Duration.ofSeconds(1))` 每秒产出一个数据
2. `map` 将数据包装为 SSE 格式
3. `ServerSentEventResultHandler` 订阅这个 Flux，每收到一个 `onNext` 就立即写入 HTTP 响应
4. 连接保持打开，直到 Flux 完成或客户端断开

从 Reactor 视角看，这就是一个 `Flux` + `publishOn` 的操作符链。`interval` 操作符内部使用 `Schedulers.parallel()` 调度，每秒在 parallel 线程上产出数据，通过 `onNext` 传播到 `ServerSentEventResultHandler`，后者将数据写入 Netty Channel。

### 9.2 背压在 SSE 中的作用

如果客户端读取 SSE 数据的速度慢于服务器产出的速度，背压机制会自动调节：

1. 客户端 TCP 接收窗口缩小
2. Netty Channel 不可写
3. `ServerSentEventResultHandler` 停止 `request(n)`
4. `Flux.interval` 的 `request` 被暂停
5. 定时器虽然触发，但由于没有 `request`，不产出数据
6. 客户端消费后，TCP 窗口扩大，Netty 可写，`request` 恢复

这种自动调节机制完全基于 Reactor 的背压协议，无需任何额外代码。

---

## 十、归纳表格

### Spring MVC vs WebFlux 对比表

| 维度 | Spring MVC | Spring WebFlux |
|------|-----------|----------------|
| **底层框架** | Servlet API | Reactive Streams + Reactor Core |
| **默认容器** | Tomcat (Servlet 容器) | Reactor Netty (非阻塞) |
| **线程模型** | 一请求一线程（200 线程） | 少量 EventLoop 线程（4-16 线程） |
| **I/O 模型** | 同步阻塞 I/O | 异步非阻塞 I/O |
| **请求处理** | `DispatcherServlet` 同步调用 | `DispatcherHandler` 异步链式调用 |
| **返回值类型** | `User` / `List<User>` / `ModelAndView` | `Mono<User>` / `Flux<User>` |
| **数据库访问** | JDBC (阻塞) | R2DBC (非阻塞) |
| **HTTP 客户端** | RestTemplate (阻塞) | WebClient (非阻塞) |
| **背压** | 无 | 端到端背压（客户端→Netty→Reactor→DB） |
| **内存占用** | ~200MB (200线程 × 1MB栈) | ~16MB (4线程 × 4MB栈) |
| **装配时/执行时** | 不区分（方法调用即执行） | 严格分离（Controller 返回 Publisher，subscribe 才执行） |
| **Context 传播** | ThreadLocal | Reactor Context（跨线程安全） |
| **错误处理** | `@ExceptionHandler` + try-catch | `@ExceptionHandler` + `onErrorResume` / `onErrorMap` |
| **流式响应** | 需 Servlet 3.1 异步支持 | 原生支持（Flux + SSE/WebSocket） |
| **CPU 密集型** | 适合（线程模型天然适合） | 需 `publishOn(parallel())` 切换 |
| **I/O 密集型** | 线程数受限，可能不足 | 极大优势（少量线程处理大量连接） |
| **学习曲线** | 低（传统命令式） | 高（响应式编程） |
| **调试难度** | 低（完整调用栈） | 高（需 `checkpoint()` 辅助） |
| **生态支持** | 最广泛 | 仍在发展（R2DBC 尚不如 JDBC 成熟） |

### WebFlux 核心组件与 Reactor 机制对应表

| WebFlux 组件 | Reactor 机制 | 对应关系 |
|-------------|------------|---------|
| `WebHandler.handle()` | `Mono<Void>` | HTTP 请求处理 = Mono 的 onComplete |
| `DispatcherHandler` | 操作符链的编排者 | 构建 `flatMap` 链串联各处理步骤 |
| `HandlerAdapter` | 装配时构建 | 调用 Controller 方法，获取 Publisher（不 subscribe） |
| `HandlerResultHandler` | `subscribe()` 触发 | 订阅 Controller 返回的 Publisher，写入响应 |
| `WebFilter` | `FluxOperator` 装饰器 | Filter 链 = 操作符链的 `flatMap` 嵌套 |
| `WebClient` | `Mono.fromCallable()` 的异步变体 | 装配时构建请求配置，subscribe 时发起 I/O |
| `ServerWebExchange.attributes` | Reactor `Context` | 请求上下文传播 |
| `@ExceptionHandler` | `onErrorResume` | 错误恢复操作符 |
| SSE 流式响应 | `Flux` + `onNext` 逐条推送 | 每收到 onNext 立即写入 HTTP 响应 |
| R2DBC 查询 | `Flux` (Fuseable) | 流式查询结果，支持队列融合 |
| EventLoop 线程 | `Schedulers.parallel()` | 少量线程处理大量连接 |
| 背压传递 | `Subscription.request(n)` | Netty Channel 可写性 → request(n) |
| `contextWrite()` | Reactor `Context` | 从下游 Filter 向上游 Controller 传播 |

---

## 十一、总结

Spring WebFlux 不是一个独立的 Web 框架，而是 Reactor Core 在 Web 领域的应用延伸。从 Reactor 视角看 WebFlux，我们可以看到：

1. **HTTP 请求/响应 = Mono**：WebFlux 将 HTTP 请求处理建模为 `Mono<Void>`，完美利用了 Reactor 的装配时/执行时分离——Controller 方法返回 Publisher 时不执行任何 I/O，只有 `HandlerResultHandler` subscribe 后才触发处理链。

2. **背压端到端传递**：WebFlux 的背压从客户端 TCP 窗口一路传递到数据库查询游标，每一层都基于 Reactor 的 `Subscription.request(n)` 协议。这使得流式响应和大数据量查询能够自动调节速率，避免 OOM。

3. **线程模型 = Reactor Scheduler**：WebFlux 的少量 EventLoop 线程本质上是 Reactor 的 `Scheduler`，通过操作符链的异步回调机制处理大量并发连接，而非通过线程池的阻塞等待。

4. **Context 传播 = Reactor Context**：WebFlux 利用 Reactor 的 `Context` 机制在请求处理链中传播请求 ID、安全上下文等信息，天然解决了跨线程传播问题。

理解 WebFlux 的前提是深入理解 Reactor Core。前面 15 篇源码分析中揭示的所有机制——装配时/执行时分离、操作符链、背压协议、Scheduler 线程模型、Context 传播、Fuseable 融合、错误处理——都在 WebFlux 中发挥着关键作用。WebFlux 只是 Reactor 在 Web 场景下的一个具体应用，其架构设计完全建立在 Reactor 的核心机制之上。
