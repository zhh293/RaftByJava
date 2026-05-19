# Dubbo 高级配置深度分析（五）：RpcContext 与隐式传参

> 基于源码 `/Users/zhanghonghao/Desktop/dubbo` 分析

---

## 一、RpcContext 架构设计

### 源码分析

```java
// dubbo-rpc/dubbo-rpc-api/.../RpcContext.java
// RpcContext 是一个线程级别的临时状态持有者，每次请求发送或接收时状态都会变化。
// 共有四种 Context：

// 1. ClientAttachment - 消费端发送附件（A --> B，在 A 侧设置）
private static final InternalThreadLocal<RpcContextAttachment> CLIENT_ATTACHMENT = ...;

// 2. ServerAttachment - 提供端接收附件（A --> B，在 B 侧读取）
private static final InternalThreadLocal<RpcContextAttachment> SERVER_ATTACHMENT = ...;

// 3. ServerContext - 提供端返回附件（A <-- B，在 B 侧设置）
private static final InternalThreadLocal<RpcContextAttachment> SERVER_RESPONSE_LOCAL = ...;

// 4. ServiceContext - 整个调用链的环境参数（远程地址、方法名等）
private static final InternalThreadLocal<RpcServiceContext> SERVICE_CONTEXT = ...;

// 5. CancellationContext - 取消调用上下文
private static final InternalThreadLocal<CancellationContext> CANCELLATION_CONTEXT = ...;
```

### 数据流向图

```
Consumer (A)                          Provider (B)
    |                                      |
    | ClientAttachment.setAttachment()     |
    |  (设置要传递的附件)                    |
    |------- 请求 + attachments --------->|
    |                                      | ServerAttachment.getAttachment()
    |                                      |  (读取消费端传来的附件)
    |                                      |
    |                                      | ServerContext.setAttachment()
    |                                      |  (设置要返回的附件)
    |<------ 响应 + attachments ----------|
    | ClientResponseContext.getAttachment() |
    |  (读取提供端返回的附件)                 |
```

---

## 二、隐式传参（Attachment）

### 消费端设置附件

```java
// 方式一：通过 RpcContext（推荐）
RpcContext.getClientAttachment().setAttachment("traceId", "abc123");
RpcContext.getClientAttachment().setAttachment("userId", "10086");
demoService.sayHello("world");  // 附件随请求发送

// 方式二：通过 RpcContext 旧 API（兼容）
RpcContext.getContext().setAttachment("key", "value");

// 方式三：设置 Object 类型附件（实验性）
RpcContext.getClientAttachment().setObjectAttachment("complexData", myObject);
```

### 提供端读取附件

```java
@DubboService
public class DemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        // 从 ServerAttachment 读取消费端传来的附件
        String traceId = RpcContext.getServerAttachment().getAttachment("traceId");
        String userId = RpcContext.getServerAttachment().getAttachment("userId");

        // 获取调用方信息
        String remoteHost = RpcContext.getServiceContext().getRemoteHost();
        int remotePort = RpcContext.getServiceContext().getRemotePort();
        String remoteApp = RpcContext.getServiceContext().getRemoteApplicationName();

        return "Hello " + name + ", traceId=" + traceId;
    }
}
```

### 提供端返回附件

```java
@DubboService
public class DemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        // 设置返回给消费端的附件
        RpcContext.getServerContext().setAttachment("serverTime",
            String.valueOf(System.currentTimeMillis()));
        RpcContext.getServerContext().setAttachment("serverNode", "node-1");

        return "Hello " + name;
    }
}

// 消费端读取返回附件
String result = demoService.sayHello("world");
String serverTime = RpcContext.getClientResponseContext().getAttachment("serverTime");
```

---

## 三、附件的生命周期

### 重要特性：附件默认只传递一跳

```java
// RpcContext 注释说明：
// Note: RpcContext is a temporary state holder.
// States in RpcContext changes every time when request is sent or received.
```

**默认行为**：ClientAttachment 中的附件只会传递到下一跳（A→B），不会自动传递到 B→C。

### 跨多跳传递

如果需要附件在整个调用链中传递（如 traceId），需要实现 `PenetrateAttachmentSelector`：

```java
// dubbo-rpc/dubbo-rpc-api/.../PenetrateAttachmentSelector.java
// 选择哪些附件需要穿透传递
```

```java
// 自定义穿透附件选择器
public class TracePenetrateSelector implements PenetrateAttachmentSelector {
    @Override
    public Map<String, Object> select(Invocation invocation,
                                       Map<String, Object> clientAttachments) {
        Map<String, Object> result = new HashMap<>();
        // 只穿透 traceId 和 spanId
        if (clientAttachments.containsKey("traceId")) {
            result.put("traceId", clientAttachments.get("traceId"));
        }
        if (clientAttachments.containsKey("spanId")) {
            result.put("spanId", clientAttachments.get("spanId"));
        }
        return result;
    }
}

// 注册 SPI: META-INF/dubbo/org.apache.dubbo.rpc.PenetrateAttachmentSelector
// trace=com.example.TracePenetrateSelector
```

---

## 四、ServiceContext 环境信息

### 可获取的信息

```java
// 在 Provider 端
RpcServiceContext ctx = RpcContext.getServiceContext();

// 远程信息
ctx.getRemoteHost();              // 调用方 IP
ctx.getRemotePort();              // 调用方端口
ctx.getRemoteApplicationName();   // 调用方应用名
ctx.getRemoteAddress();           // 调用方完整地址

// 本地信息
ctx.getLocalHost();               // 本机 IP
ctx.getLocalPort();               // 本机端口

// 调用信息
ctx.getMethodName();              // 方法名
ctx.getParameterTypes();          // 参数类型
ctx.getArguments();               // 参数值
ctx.getInterfaceName();           // 接口名
ctx.getGroup();                   // 分组
ctx.getVersion();                 // 版本
ctx.getProtocol();                // 协议
ctx.getServiceKey();              // 服务 key
ctx.getUrl();                     // 完整 URL

// 判断角色
ctx.isProviderSide();             // 是否提供端
ctx.isConsumerSide();             // 是否消费端
```

---

## 五、异步调用上下文

### CompletableFuture 异步调用

```java
// 消费端异步调用
@DubboReference(async = true)
private DemoService demoService;

// 方式一：通过 RpcContext 获取 Future
demoService.sayHello("world");  // 立即返回 null
CompletableFuture<String> future = RpcContext.getServiceContext().getCompletableFuture();
future.whenComplete((result, exception) -> {
    if (exception == null) {
        System.out.println("Result: " + result);
    }
});

// 方式二：使用 asyncCall
CompletableFuture<String> future = RpcContext.getServiceContext().asyncCall(
    () -> demoService.sayHello("world")
);
```

### Provider 端异步执行

```java
@DubboService
public class AsyncDemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        // 开启异步上下文
        AsyncContext asyncContext = RpcContext.startAsync();

        // 在其他线程中完成处理
        CompletableFuture.supplyAsync(() -> {
            // 模拟耗时操作
            return "Hello " + name;
        }).whenComplete((result, ex) -> {
            if (ex == null) {
                asyncContext.write(result);  // 异步写回结果
            } else {
                asyncContext.write(new RuntimeException(ex));
            }
        });

        return null;  // 立即返回，实际结果通过 asyncContext 写回
    }
}
```

---

## 六、CancellationContext（调用取消）

```java
// 获取取消上下文
CancellationContext cancelCtx = RpcContext.getCancellationContext();

// 注册取消监听器
cancelCtx.addListener(reason -> {
    System.out.println("Call cancelled: " + reason);
    // 清理资源
});

// 取消调用
cancelCtx.cancel("timeout");
```

---

## 七、Context 保存与恢复

当需要在异步线程中使用 RpcContext 时，需要手动保存和恢复：

```java
// 保存当前上下文
RpcContext.RestoreContext restoreContext = RpcContext.storeContext();

CompletableFuture.runAsync(() -> {
    // 恢复上下文到新线程
    RpcContext.restoreContext(restoreContext);

    // 现在可以正常使用 RpcContext
    String traceId = RpcContext.getServerAttachment().getAttachment("traceId");
    // ...
});
```

---

## 八、ContextFilter 源码分析

```java
// dubbo-rpc/dubbo-rpc-api/.../filter/ContextFilter.java
// 这个 Filter 负责在 Provider 端设置 RpcContext
@Activate(group = CommonConstants.PROVIDER)
public class ContextFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 将请求中的 attachments 设置到 ServerAttachment
        // 设置远程地址、本地地址等信息到 ServiceContext
        // 调用完成后清理 Context
    }
}
```

---

## 九、Spring Boot 完整示例

### 分布式链路追踪场景

```java
// === 消费端 Filter：自动注入 traceId ===
@Activate(group = CommonConstants.CONSUMER)
public class TraceConsumerFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 从 MDC 或 ThreadLocal 获取 traceId
        String traceId = MDC.get("traceId");
        if (traceId == null) {
            traceId = UUID.randomUUID().toString();
        }
        // 通过 attachment 传递
        RpcContext.getClientAttachment().setAttachment("traceId", traceId);
        RpcContext.getClientAttachment().setAttachment("spanId", generateSpanId());

        return invoker.invoke(invocation);
    }
}

// === 提供端 Filter：自动提取 traceId ===
@Activate(group = CommonConstants.PROVIDER)
public class TraceProviderFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        String traceId = RpcContext.getServerAttachment().getAttachment("traceId");
        String spanId = RpcContext.getServerAttachment().getAttachment("spanId");

        // 设置到 MDC，方便日志输出
        MDC.put("traceId", traceId);
        MDC.put("spanId", spanId);

        try {
            return invoker.invoke(invocation);
        } finally {
            MDC.clear();
        }
    }
}

// SPI 注册文件: META-INF/dubbo/org.apache.dubbo.rpc.Filter
// traceConsumer=com.example.TraceConsumerFilter
// traceProvider=com.example.TraceProviderFilter
```

### application.yml 配置

```yaml
dubbo:
  consumer:
    filter: traceConsumer  # 激活消费端 trace filter
  provider:
    filter: traceProvider  # 激活提供端 trace filter
```

### 灰度路由场景

```java
// 消费端设置灰度标签
@Component
public class GrayInterceptor {
    public void beforeCall() {
        // 根据用户 ID 决定是否走灰度
        if (isGrayUser(getCurrentUserId())) {
            RpcContext.getClientAttachment().setAttachment("dubbo.tag", "gray");
        }
    }
}
```

---

## 十、注意事项

1. **ClientAttachment 每次调用后自动清除**：设置的附件只对下一次调用生效
2. **不要在 attachment 中传递大对象**：附件会序列化到网络中，影响性能
3. **异步场景注意 Context 传递**：异步线程中 RpcContext 为空，需要手动保存恢复
4. **attachment key 避免使用 dubbo 保留前缀**：如 `dubbo.tag`、`dubbo.force.tag` 等是框架内部使用的
5. **Object 类型附件是实验性功能**：跨语言调用时可能不兼容
