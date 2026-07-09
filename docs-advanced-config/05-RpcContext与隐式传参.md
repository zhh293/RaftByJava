# Dubbo RpcContext 与隐式传参 —— 源码全流程解析

> 基于源码项目 `/Users/zhanghonghao/Desktop/dubbo` 逐步分析，从 Consumer 设置 attachment 到 Provider 读取、再到响应 attachment 返回 Consumer，不跳步、不省略。

---

## 全局流转链路总览

先给你一张完整的 attachment 流转图，后面逐步展开每一层：

```
Consumer 业务代码
  │
  │  RpcContext.getClientAttachment().setAttachment("traceId", "abc123")
  │  userService.getUserById(1)    ← 触发远程调用
  │
  +-- 1. InvokerInvocationHandler.invoke()
  |     → 封装 RpcInvocation（此时 attachments 还是空的）
  |
  +-- 2. ConsumerContextFilter.invoke()  [ClusterFilter, order=Integer.MIN_VALUE]
  |     → 把 ServerAttachment 中的附件合并到 invocation（链式透传 A→B→C）
  |     → 把 ClientAttachment 中的附件合并到 invocation（用户设置的 traceId）
  |
  +-- 3. AbstractInvoker.invoke() → addInvocationAttachments()
  |     → 把 invoker 自带的默认附件（interface、version）合并到 invocation（IfAbsent）
  |     → 再次把 ClientAttachment 合并到 invocation（IfAbsent，兜底）
  |
  +-- 4. DubboInvoker.doInvoke()
  |     → 补充 path、version 到 invocation.attachment
  |
  +-- 5. DubboCodec.encodeRequestData()
  |     → out.writeAttachments(inv.getObjectAttachments())
  |     → attachments 随 Body 序列化写入 TCP 字节流
  |
  =========== 网络传输（TCP）===========
  |
  +-- 6. DecodeableRpcInvocation.decode()  [Provider 端]
  |     → in.readAttachments() → addObjectAttachments(map)
  |     → RpcInvocation 上现在有了 Consumer 传来的所有附件
  |
  +-- 7. ContextFilter.invoke()  [Provider Filter]
  |     → 从 invocation 取出 attachments
  |     → 过滤掉系统内部 key（path、interface、version、token、timeout 等）
  |     → 设置到 RpcContext.getServerAttachment()
  |
  +-- 8. Provider 业务代码执行
  |     → 读: RpcContext.getServerAttachment().getAttachment("traceId")  ← "abc123"
  |     → 写: RpcContext.getServerContext().setAttachment("serverTime", "172000000")
  |           （实际写入 SERVER_RESPONSE_LOCAL）
  |
  +-- 9. ContextFilter.onResponse()  [Provider 响应阶段]
  |     → 把 SERVER_RESPONSE_LOCAL 中的附件 merge 进 AppResponse
  |
  +-- 10. DubboCodec.encodeResponseData()  [Provider 端]
  |      → out.writeAttachments(result.getObjectAttachments())
  |      → 响应附件随 Response Body 序列化写入 TCP
  |
  =========== 网络传输（TCP）===========
  |
  +-- 11. DecodeableRpcResult.handleAttachment()  [Consumer 端]
  |      → in.readAttachments() → addObjectAttachments(map)
  |      → AppResponse 上现在有了 Provider 返回的附件
  |
  +-- 12. ConsumerContextFilter.onResponse()  [Consumer 端]
        → 把 AppResponse 中的附件设置到 RpcContext.getClientResponseContext()
        → 用户通过 RpcContext.getServerContext().getAttachment("serverTime") 读取
```

---

## 第一部分：RpcContext 的五个 ThreadLocal 上下文

### 为什么需要这么多 Context？

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/RpcContext.java`

一次 RPC 调用涉及**四个方向的数据传递**——Consumer 发给 Provider、Provider 读消费者的附件、Provider 返回附件给 Consumer、Consumer 读响应附件。如果只用一个 ThreadLocal，这四个方向的数据会互相覆盖。所以 Dubbo 设计了四个独立的 Context，各管各的方向：

```java
// 1. Consumer 侧：用户设置要发给 Provider 的附件
//    用法：RpcContext.getClientAttachment().setAttachment("traceId", "xxx")
private static final InternalThreadLocal<RpcContextAttachment> CLIENT_ATTACHMENT =
        new InternalThreadLocal<>() { ... };

// 2. Provider 侧：收到 Consumer 发来的附件
//    用法：RpcContext.getServerAttachment().getAttachment("traceId")
private static final InternalThreadLocal<RpcContextAttachment> SERVER_ATTACHMENT =
        new InternalThreadLocal<>() { ... };

// 3. Provider 侧：用户设置要返回给 Consumer 的附件
//    用法：通过 RpcContext.getServerContext().setAttachment("key", "val") 代理写入
private static final InternalThreadLocal<RpcContextAttachment> SERVER_RESPONSE_LOCAL =
        new InternalThreadLocal<>() { ... };

// 4. Consumer 侧：收到 Provider 返回的附件
//    用法：RpcContext.getServerContext().getAttachment("key")
private static final InternalThreadLocal<RpcContextAttachment> CLIENT_RESPONSE_LOCAL =
        new InternalThreadLocal<>() { ... };

// 5. 服务级上下文（Invoker、Invocation、远程地址等环境信息）
private static final InternalThreadLocal<RpcServiceContext> SERVICE_CONTEXT =
        new InternalThreadLocal<>() { ... };
```

**InternalThreadLocal 是什么？** 它是 Dubbo 自己实现的高性能 ThreadLocal，底层用数组替代了 JDK ThreadLocal 的 HashMap，性能更高。每个线程有自己独立的副本，线程之间互不干扰。

### 数据流向对照表

| 方向 | 设置方 | 读取方 | 使用的 Context |
|------|--------|--------|---------------|
| Consumer → Provider（请求附件） | Consumer 业务代码 | Provider 业务代码 | CLIENT_ATTACHMENT → SERVER_ATTACHMENT |
| Provider → Consumer（响应附件） | Provider 业务代码 | Consumer 业务代码 | SERVER_RESPONSE_LOCAL → CLIENT_RESPONSE_LOCAL |

### getServerContext() 的适配器设计

```java
public static RpcContextAttachment getServerContext() {
    return new RpcServerContextAttachment();
}
```

`RpcServerContextAttachment` 是一个适配器：
- **写操作**（`setAttachment`）→ 代理到 `SERVER_RESPONSE_LOCAL`（Provider 设置返回附件时用）
- **读操作**（`getAttachment`）→ 合并 `SERVER_RESPONSE_LOCAL` + `CLIENT_RESPONSE_LOCAL`（Consumer 读响应附件时用）

这样同一个 `getServerContext()` API，在 Provider 端用来写、在 Consumer 端用来读，上层代码无感知。

---

## 第二部分：Consumer 端 —— attachment 如何从 ThreadLocal 进入网络请求

### 2.1 用户设置 attachment

```java
// 你的业务代码
RpcContext.getClientAttachment().setAttachment("traceId", "abc123");
RpcContext.getClientAttachment().setAttachment("userId", "10086");
userService.getUserById(1L);  // ← 调用这行时，附件随请求发出去
```

调用 `setAttachment()` 只是把 key-value 存进当前线程的 `CLIENT_ATTACHMENT` 这个 ThreadLocal 里，还没有跟任何请求关联。真正把它们"搬运"到请求里的是后面的 Filter 链。

---

### 2.2 InvokerInvocationHandler.invoke() —— 封装 RpcInvocation

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/proxy/InvokerInvocationHandler.java`

当你调用 `userService.getUserById(1L)` 时，JDK 动态代理拦截方法调用，封装成 `RpcInvocation`：

```java
RpcInvocation rpcInvocation = new RpcInvocation(
        serviceModel,
        methodName,           // "getUserById"
        interfaceName,        // "com.example.UserService"
        protocolServiceKey,
        parameterTypes,       // [Long.class]
        args);                // [1L]
```

**注意**：此时 `rpcInvocation` 的 `attachments` 是空的！用户设置的 traceId、userId 还在 ThreadLocal 里，还没有被合并进来。合并是下一步 Filter 做的事。

---

### 2.3 ConsumerContextFilter.invoke() —— 核心搬运工

**源码位置**: `dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/filter/support/ConsumerContextFilter.java`

> **这个 Filter 在干什么？**
>
> 它是 Consumer 端 Filter 链中**最先执行的**（order=Integer.MIN_VALUE），职责就一个：**把各种 ThreadLocal 里的 attachment 搬运到 RpcInvocation 上**。搬运完之后，后续的 DubboInvoker、Codec 就能从 RpcInvocation 上取到所有附件并序列化发送了。
>
> 它做了两件"搬运"：
> 1. 搬运"链式透传附件"（ServerAttachment 里的，来自上游调用方传给你的，你需要继续传给下游）
> 2. 搬运"用户手动设置的附件"（ClientAttachment 里的，你自己 setAttachment 的）

```java
@Activate(group = CONSUMER, order = Integer.MIN_VALUE)
public class ConsumerContextFilter implements ClusterFilter, ClusterFilter.Listener {

    private List<PenetrateAttachmentSelector> supportedSelectors;

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        // ===== 第一步：搬运链式透传附件 =====
        // 场景：A 调 B，B 再调 C。A 传给 B 的 traceId 存在 B 的 ServerAttachment 里，
        // B 调 C 时需要把这个 traceId 继续传下去。
        if (CollectionUtils.isNotEmpty(supportedSelectors)) {
            // 有自定义的 PenetrateAttachmentSelector：由 selector 决定透传哪些
            for (PenetrateAttachmentSelector selector : supportedSelectors) {
                Map<String, Object> selected = selector.select(
                        invocation,
                        RpcContext.getClientAttachment(),
                        RpcContext.getServerAttachment());
                if (CollectionUtils.isNotEmptyMap(selected)) {
                    ((RpcInvocation) invocation).addObjectAttachments(selected);
                }
            }
        } else {
            // 没有自定义 selector：直接把 ServerAttachment 全部透传
            ((RpcInvocation) invocation)
                    .addObjectAttachments(RpcContext.getServerAttachment().getObjectAttachments());
        }

        // ===== 第二步：搬运用户手动设置的附件 =====
        // 就是你代码里 RpcContext.getClientAttachment().setAttachment("traceId", "xxx") 设置的
        Map<String, Object> contextAttachments =
                RpcContext.getClientAttachment().getObjectAttachments();
        if (CollectionUtils.isNotEmptyMap(contextAttachments)) {
            ((RpcInvocation) invocation).addObjectAttachments(contextAttachments);
        }

        // ===== 第三步：设置 ServiceContext（环境信息）=====
        RpcServiceContext serviceContext = RpcContext.getServiceContext();
        serviceContext.setInvocation(invocation);
        serviceContext.setInvoker(invoker);

        return invoker.invoke(invocation);
    }
}
```

**关键设计**：第一步用 `addObjectAttachments`（会覆盖同名 key），第二步也用 `addObjectAttachments`。所以如果 ServerAttachment 和 ClientAttachment 里有同名 key，**用户手动设置的优先**（因为第二步覆盖第一步）。

**做完这步之后**：`RpcInvocation` 的 `attachments` map 里已经有了 `{"traceId": "abc123", "userId": "10086", ...}`。

---

### 2.4 AbstractInvoker.invoke() —— 补充默认附件

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/protocol/AbstractInvoker.java`

```java
@Override
public Result invoke(Invocation inv) throws RpcException {
    RpcInvocation invocation = (RpcInvocation) inv;

    // 补充 Invoker 级别的默认附件
    addInvocationAttachments(invocation);
    // ...
    return doInvoke(invocation);
}

private void addInvocationAttachments(RpcInvocation invocation) {
    // 1. Invoker 自带的默认附件（interface、version、group 等 URL 上的参数）
    if (CollectionUtils.isNotEmptyMap(attachment)) {
        invocation.addObjectAttachmentsIfAbsent(attachment);
    }

    // 2. 再次从 ClientAttachment 获取（兜底，防止某些路径没经过 ConsumerContextFilter）
    Map<String, Object> clientContextAttachments =
            RpcContext.getClientAttachment().getObjectAttachments();
    if (CollectionUtils.isNotEmptyMap(clientContextAttachments)) {
        invocation.addObjectAttachmentsIfAbsent(clientContextAttachments);
    }
}
```

**注意用的是 `addObjectAttachmentsIfAbsent`**——不覆盖已有的 key。所以 ConsumerContextFilter 已经设置的用户附件不会被默认附件覆盖。

---

### 2.5 DubboInvoker.doInvoke() —— 补充 path 和 version

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboInvoker.java`

```java
@Override
protected Result doInvoke(final Invocation invocation) throws Throwable {
    RpcInvocation inv = (RpcInvocation) invocation;

    // 补充 path（接口路径）和 version（版本号）
    inv.setAttachment(PATH_KEY, getUrl().getPath());
    inv.setAttachment(VERSION_KEY, version);

    // 选连接、发请求...
    ExchangeClient currentClient = clients[...];
    CompletableFuture<AppResponse> appResponseFuture =
            currentClient.request(inv, timeout, executor).thenApply(...);
    return new AsyncRpcResult(appResponseFuture, inv);
}
```

---

### 2.6 DubboCodec.encodeRequestData() —— 序列化到网络

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboCodec.java`

```java
@Override
protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version)
        throws IOException {
    RpcInvocation inv = (RpcInvocation) data;

    // 写入协议字段
    out.writeUTF(version);                        // 框架版本
    out.writeUTF(inv.getAttachment(PATH_KEY));    // 接口路径
    out.writeUTF(inv.getAttachment(VERSION_KEY)); // 服务版本
    out.writeUTF(inv.getMethodName());            // 方法名
    out.writeUTF(inv.getParameterTypesDesc());    // 参数类型描述

    // 写入参数值
    Object[] args = inv.getArguments();
    for (int i = 0; i < args.length; i++) {
        out.writeObject(encodeInvocationArgument(channel, inv, i));
    }

    // ===== 核心：把 RpcInvocation 上的所有 attachments 序列化写入 =====
    out.writeAttachments(inv.getObjectAttachments());
    // 这里面就包含了你设置的 "traceId"="abc123"、"userId"="10086"
    // 以及系统附件 "path"、"interface"、"version" 等
}
```

**Dubbo 协议帧中 attachments 的位置：**

```
┌───────────────────────────────────────────────────┐
│  Header (16字节): Magic + Flag + RequestId + ...   │
├───────────────────────────────────────────────────┤
│  Body:                                             │
│    ├── 框架版本 (UTF)                               │
│    ├── 接口路径 (UTF)                               │
│    ├── 服务版本 (UTF)                               │
│    ├── 方法名 (UTF)                                 │
│    ├── 参数类型描述 (UTF)                            │
│    ├── 参数值 (Object...)                           │
│    └── ★ attachments (Map<String, Object>)         │  ← 你的隐式参数在这里!
└───────────────────────────────────────────────────┘
```

**至此，Consumer 端的工作完成**——用户设置的 attachment 已经从 ThreadLocal 搬运到 RpcInvocation，再序列化进 TCP 字节流，发往 Provider。

---

## 第三部分：Provider 端 —— attachment 如何从网络请求进入业务代码

### 3.1 DecodeableRpcInvocation.decode() —— 反序列化 attachments

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DecodeableRpcInvocation.java`

Provider 端 Netty 收到字节流后，DubboCodec 解码 Header，然后在业务线程中由 `DecodeableRpcInvocation.decode()` 反序列化 Body：

```java
@Override
public void decode() throws Exception {
    // 读取框架版本、接口路径、服务版本、方法名、参数类型...
    String dubboVersion = in.readUTF();
    String path = in.readUTF();
    String version = in.readUTF();
    setMethodName(in.readUTF());
    setParameterTypesDesc(in.readUTF());

    // 读取参数值...
    Object[] args = new Object[parameterTypes.length];
    for (int i = 0; i < args.length; i++) {
        args[i] = in.readObject(parameterTypes[i]);
    }
    setArguments(args);

    // ===== 核心：读取 attachments =====
    Map<String, Object> map = in.readAttachments();
    if (CollectionUtils.isNotEmptyMap(map)) {
        addObjectAttachments(map);
    }
    // 此时 RpcInvocation 上有了所有 Consumer 传来的附件
    // 包括 "traceId"="abc123"、"path"、"version" 等
}
```

**做完这步**：Provider 端的 `RpcInvocation` 对象上已经有了 Consumer 传来的全部 attachments。但此时业务代码还拿不到——需要 `ContextFilter` 把它设置到 `RpcContext` 里。

---

### 3.2 ContextFilter.invoke() —— 把 attachments 设置到 RpcContext

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/filter/ContextFilter.java`

> **这个 Filter 在干什么？**
>
> 它是 Provider 端 Filter 链中的关键一环。职责：
> 1. 从 RpcInvocation 的 attachments 中**过滤掉系统内部使用的 key**（path、interface、version、token、timeout 等——这些是框架路由用的，业务代码不需要看到）
> 2. 把**过滤后**的用户附件设置到 `RpcContext.getServerAttachment()` 中
> 3. 设置 ServiceContext 的环境信息（远端地址、方法名等）
>
> 过滤完之后，业务代码通过 `RpcContext.getServerAttachment().getAttachment("traceId")` 就能拿到 Consumer 传来的 traceId 了。

```java
@Activate(group = PROVIDER)
public class ContextFilter implements Filter, Filter.Listener {

    // 需要过滤掉的系统内部 key 集合
    private static final Set<String> UNLOADING_KEYS;
    static {
        UNLOADING_KEYS = new HashSet<>();
        UNLOADING_KEYS.add(PATH_KEY);           // "path"
        UNLOADING_KEYS.add(INTERFACE_KEY);      // "interface"
        UNLOADING_KEYS.add(VERSION_KEY);        // "version"
        UNLOADING_KEYS.add(GROUP_KEY);          // "group"
        UNLOADING_KEYS.add(TOKEN_KEY);          // "token"
        UNLOADING_KEYS.add(TIMEOUT_KEY);        // "timeout"
        UNLOADING_KEYS.add(ASYNC_KEY);          // "async"
        // ... 还有一些 HTTP header 相关的 key
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        // ===== 第一步：从 invocation 取出 attachments 并过滤 =====
        Map<String, Object> attachments = invocation.getObjectAttachments();
        Map<String, Object> newAttach = new HashMap<>(attachments.size());
        for (Map.Entry<String, Object> entry : attachments.entrySet()) {
            String key = entry.getKey();
            if (!UNLOADING_KEYS.contains(key)) {
                // 只保留非系统 key（如 traceId、userId 等用户设置的）
                newAttach.put(key, entry.getValue());
            }
        }

        // ===== 第二步：设置到 ServerAttachment =====
        RpcContextAttachment serverAttachment = RpcContext.getServerAttachment();
        if (CollectionUtils.isNotEmptyMap(newAttach)) {
            if (serverAttachment.getObjectAttachments().size() > 0) {
                serverAttachment.getObjectAttachments().putAll(newAttach);
            } else {
                serverAttachment.setObjectAttachments(newAttach);
            }
        }

        // ===== 第三步：设置 ServiceContext 环境信息 =====
        RpcServiceContext serviceContext = RpcContext.getServiceContext();
        serviceContext.setInvoker(invoker);
        serviceContext.setInvocation(invocation);
        serviceContext.setLocalAddress(invoker.getUrl().toInetSocketAddress());
        serviceContext.setRemoteAddress(
                (InetSocketAddress) invocation.get(REMOTE_ADDRESS_KEY));
        // 消费者应用名
        serviceContext.setRemoteApplicationName(
                invocation.getAttachment(REMOTE_APPLICATION_KEY));

        // ===== 第四步：继续执行 Filter 链 → 最终到达业务代码 =====
        return invoker.invoke(invocation);
    }
}
```

**做完这步**：`RpcContext.getServerAttachment()` 里有了过滤后的用户附件。你的业务代码现在可以通过 `RpcContext.getServerAttachment().getAttachment("traceId")` 拿到 "abc123" 了。

---

### 3.3 Provider 业务代码 —— 读写 attachment

```java
@DubboService
public class UserServiceImpl implements UserService {
    @Override
    public User getUserById(Long id) {
        // ===== 读取 Consumer 传来的附件 =====
        String traceId = RpcContext.getServerAttachment().getAttachment("traceId");
        // traceId = "abc123" ✓

        String userId = RpcContext.getServerAttachment().getAttachment("userId");
        // userId = "10086" ✓

        // ===== 获取调用者环境信息 =====
        String remoteHost = RpcContext.getServiceContext().getRemoteHost();
        // "192.168.1.200"（Consumer 的 IP）

        // ===== 设置要返回给 Consumer 的附件 =====
        RpcContext.getServerContext().setAttachment("serverTime",
                String.valueOf(System.currentTimeMillis()));
        RpcContext.getServerContext().setAttachment("serverNode", "node-1");
        // 实际写入的是 SERVER_RESPONSE_LOCAL 这个 ThreadLocal

        // 执行业务逻辑...
        return userRepository.findById(id);
    }
}
```

---

## 第四部分：Provider 端响应 —— attachment 如何从业务代码返回到网络

### 4.1 ContextFilter.onResponse() —— 把响应附件合并进 AppResponse

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/filter/ContextFilter.java`

> **这一步在干什么？**
>
> 业务代码执行完了，返回值被封装成了 `AppResponse`。但业务代码通过 `RpcContext.getServerContext().setAttachment()` 设置的响应附件还在 ThreadLocal 里，还没有进入 AppResponse。这一步就是把 ThreadLocal 里的响应附件"搬运"到 AppResponse 上，这样后续编码发送时就能把它带出去。

```java
@Override
public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
    // ===== 搬运响应附件到 AppResponse =====

    // 1. 如果有自定义 PenetrateAttachmentSelector：用 selectReverse 选择反向透传的附件
    if (CollectionUtils.isNotEmpty(supportedSelectors)) {
        for (PenetrateAttachmentSelector selector : supportedSelectors) {
            Map<String, Object> selected = selector.selectReverse(
                    invocation,
                    RpcContext.getClientResponseContext(),   // 来自更下游的响应附件
                    RpcContext.getServerResponseContext());  // 本节点设置的响应附件
            if (CollectionUtils.isNotEmptyMap(selected)) {
                appResponse.addObjectAttachments(selected);
            }
        }
    } else {
        // 没有 selector：直接把来自下游的附件透传
        appResponse.addObjectAttachments(
                RpcContext.getClientResponseContext().getObjectAttachments());
    }

    // 2. 始终把本节点设置的响应附件放入 response
    //    这就是你在业务代码里 RpcContext.getServerContext().setAttachment("serverTime", ...) 的那些
    appResponse.addObjectAttachments(
            RpcContext.getServerResponseContext().getObjectAttachments());

    // 3. 清理 ThreadLocal，防止线程复用时污染下次请求
    removeContext();
}
```

**做完这步**：AppResponse 的 `attachments` 里已经有了 `{"serverTime": "172000000", "serverNode": "node-1"}`。

---

### 4.2 DubboCodec.encodeResponseData() —— 序列化响应附件

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboCodec.java`

```java
@Override
protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version)
        throws IOException {
    Result result = (Result) data;

    // 写状态标识（正常/异常/有附件等）
    // ...

    if (result.hasException()) {
        out.writeObject(result.getException());
    } else {
        out.writeObject(result.getValue());  // 业务返回值（User 对象）
    }

    // ===== 核心：序列化响应附件 =====
    if (isAttachVersion) {
        result.getObjectAttachments().put(DUBBO_VERSION_KEY, Version.getProtocolVersion());
        out.writeAttachments(result.getObjectAttachments());
        // 包含 "serverTime"="172000000"、"serverNode"="node-1"、"dubbo"="2.0.2" 等
    }
}
```

**Response 帧中 attachments 的位置：**

```
┌───────────────────────────────────────────────────┐
│  Header (16字节): Magic + Flag + Status + Id + ... │
├───────────────────────────────────────────────────┤
│  Body:                                             │
│    ├── 状态标识 (byte)                              │
│    ├── 返回值 / 异常 (Object)                       │
│    └── ★ attachments (Map<String, Object>)         │  ← Provider 的响应附件在这里!
└───────────────────────────────────────────────────┘
```

---

## 第五部分：Consumer 端 —— 响应附件如何进入业务代码

### 5.1 DecodeableRpcResult.handleAttachment() —— 反序列化响应附件

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DecodeableRpcResult.java`

Consumer 端 Netty 收到 Provider 返回的 Response 字节流后，解码过程中会读取附件：

```java
private void handleAttachment(ObjectInput in) throws IOException {
    try {
        Map<String, Object> map = in.readAttachments();
        addObjectAttachments(map);
        // AppResponse 上现在有了 Provider 返回的所有附件
        // {"serverTime": "172000000", "serverNode": "node-1", "dubbo": "2.0.2"}
    } catch (ClassNotFoundException e) {
        rethrow(e);
    }
}
```

---

### 5.2 ConsumerContextFilter.onResponse() —— 设置到 ClientResponseContext

**源码位置**: `dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/filter/support/ConsumerContextFilter.java`

```java
@Override
public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
    // ===== 把响应中的 attachments 设置到 CLIENT_RESPONSE_LOCAL =====
    Map<String, Object> map = appResponse.getObjectAttachments();
    RpcContext.getClientResponseContext().setObjectAttachments(map);

    // 清理 ClientAttachment（防止下次调用时旧的附件还在）
    removeContext(invocation);
}
```

**做完这步**：用户代码可以通过 `RpcContext.getServerContext().getAttachment("serverTime")` 读到 Provider 返回的附件了（`getServerContext()` 的适配器会从 CLIENT_RESPONSE_LOCAL 读取）。

---

### 5.3 Consumer 业务代码 —— 读取响应附件

```java
// 调用远程服务
User user = userService.getUserById(1L);

// 读取 Provider 返回的响应附件
String serverTime = RpcContext.getServerContext().getAttachment("serverTime");
// serverTime = "172000000" ✓
String serverNode = RpcContext.getServerContext().getAttachment("serverNode");
// serverNode = "node-1" ✓
```

---

## 第六部分：attachment 的生命周期 —— 为什么只传一跳

### 6.1 调用后自动清理

`ConsumerContextFilter.onResponse()` 里调用了 `removeContext(invocation)`，这个方法会**清空 CLIENT_ATTACHMENT**。也就是说，你设置的 attachment 在请求发出去之后就被清掉了，下次再调另一个服务时不会携带上次设的值。

```java
private void removeContext(Invocation invocation) {
    // 清空 ClientAttachment，防止影响下次调用
    RpcContext.getClientAttachment().clearAttachments();
    // ...
}
```

**这意味着**：`RpcContext.getClientAttachment().setAttachment("traceId", "xxx")` 只对**紧接着的下一次** RPC 调用生效，调完就没了。

### 6.2 链式透传场景（A → B → C）

默认行为下，A 传给 B 的 traceId 不会自动传到 C。因为 B 调 C 时，B 的 ClientAttachment 已经被清空了。

但 `ConsumerContextFilter` 第一步做了一件事——把 `ServerAttachment` 的内容合并到 invocation 里。什么意思？

当 B 作为 Provider 收到 A 的请求时，traceId 被设进了 B 的 `ServerAttachment`。然后 B 的业务代码在同一个线程里接着调用 C（B 同时是 C 的 Consumer）。此时 B 作为 Consumer 发请求时，`ConsumerContextFilter` 第一步会把 B 的 `ServerAttachment`（里面有 A 传来的 traceId）搬到发给 C 的 invocation 里——**实现了自动透传**。

但注意：这只在**默认没有配置 PenetrateAttachmentSelector 时**生效（因为默认逻辑是把 ServerAttachment 全部透传）。如果配了 selector，就由 selector 决定透传哪些 key。

---

## 第七部分：完整源码调用链总结

### 请求方向（Consumer → Provider）

```
用户代码: RpcContext.getClientAttachment().setAttachment("traceId", "abc123")
    │
    │ 存入 ThreadLocal (CLIENT_ATTACHMENT)
    ▼
InvokerInvocationHandler.invoke()
    │ new RpcInvocation(methodName, args)  // attachments 为空
    ▼
ConsumerContextFilter.invoke()        [ClusterFilter, order=MIN_VALUE]
    │ invocation.addObjectAttachments(ServerAttachment)   // 链式透传
    │ invocation.addObjectAttachments(ClientAttachment)   // 用户设置
    ▼
AbstractInvoker.addInvocationAttachments()
    │ invocation.addObjectAttachmentsIfAbsent(defaultAttachments)  // interface/version
    │ invocation.addObjectAttachmentsIfAbsent(ClientAttachment)    // 兜底
    ▼
DubboInvoker.doInvoke()
    │ invocation.setAttachment(PATH_KEY, path)
    │ invocation.setAttachment(VERSION_KEY, version)
    ▼
HeaderExchangeChannel.request()
    │ new Request(requestId, invocation)
    ▼
DubboCodec.encodeRequestData()
    │ out.writeAttachments(inv.getObjectAttachments())
    │   → {"traceId":"abc123", "userId":"10086", "path":"com.example.UserService", ...}
    ▼
═══════ TCP 网络传输 ═══════
    ▼
DubboCodec.decodeBody() → DecodeableRpcInvocation.decode()
    │ map = in.readAttachments()
    │ invocation.addObjectAttachments(map)
    ▼
ContextFilter.invoke()            [Provider Filter]
    │ 过滤 UNLOADING_KEYS (path/interface/version/token/timeout)
    │ RpcContext.getServerAttachment().setObjectAttachments(filteredAttachments)
    ▼
Provider 业务代码
    │ RpcContext.getServerAttachment().getAttachment("traceId")  →  "abc123" ✓
```

### 响应方向（Provider → Consumer）

```
Provider 业务代码
    │ RpcContext.getServerContext().setAttachment("serverTime", "172000000")
    │   → 实际写入 SERVER_RESPONSE_LOCAL (ThreadLocal)
    │ return user;
    ▼
AbstractProxyInvoker.invoke()
    │ new AsyncRpcResult(CompletableFuture<AppResponse>, invocation)
    │   → AppResponse.value = user
    ▼
ContextFilter.onResponse()         [Provider Filter.Listener]
    │ appResponse.addObjectAttachments(ServerResponseContext)
    │   → AppResponse.attachments = {"serverTime":"172000000", "serverNode":"node-1"}
    │ removeContext()  // 清理 ThreadLocal
    ▼
HeaderExchangeHandler.handleRequest() → whenComplete 回调
    │ response.setResult(appResponse)
    │ channel.send(response)
    ▼
DubboCodec.encodeResponseData()
    │ out.writeObject(result.getValue())       // User 对象
    │ out.writeAttachments(result.getObjectAttachments())  // 响应附件
    ▼
═══════ TCP 网络传输 ═══════
    ▼
DubboCodec.decodeBody() → DecodeableRpcResult.handleAttachment()
    │ map = in.readAttachments()
    │ appResponse.addObjectAttachments(map)
    ▼
ConsumerContextFilter.onResponse()  [Consumer ClusterFilter.Listener]
    │ RpcContext.getClientResponseContext().setObjectAttachments(appResponse.attachments)
    │ removeContext()  // 清空 ClientAttachment，防止下次调用携带旧值
    ▼
Consumer 业务代码
    │ RpcContext.getServerContext().getAttachment("serverTime")  →  "172000000" ✓
```

---

## 第八部分：关键设计思想

### 1. 分离四个方向，避免互相覆盖

四个独立的 ThreadLocal 分别管"Consumer 发出""Provider 收到""Provider 返回""Consumer 收到"四个方向，任何时刻都不会混淆。这是相比老版本 Dubbo（只有一个 RpcContext）的重大改进。

### 2. Filter 做搬运，业务代码无感知

用户只需要跟 `RpcContext` 的 API 交互（get/set attachment），不需要关心 attachment 是怎么进入 RpcInvocation 的、怎么序列化到网络的。这些"脏活"全部由 `ConsumerContextFilter` 和 `ContextFilter` 在 Filter 链中透明完成。

### 3. 过滤系统 key，不污染业务

`ContextFilter` 用 `UNLOADING_KEYS` 集合把 path、interface、version、token 等系统内部使用的 key 过滤掉，只把用户自定义的 key 暴露给业务代码。业务代码看到的是一个干净的 attachment map。

### 4. 链式透传靠 ServerAttachment → ConsumerContextFilter

A→B→C 场景下，B 收到 A 的附件存在 ServerAttachment，B 再调 C 时 ConsumerContextFilter 会自动把 ServerAttachment 合并到发给 C 的请求里。不需要业务代码手动搬运。

### 5. 调用后自动清理，防止污染

每次调用完成后，ConsumerContextFilter 会清空 ClientAttachment。这保证了"附件只对下一次调用生效"，不会因为线程复用而泄漏到其他请求。

### 6. addObjectAttachments vs addObjectAttachmentsIfAbsent

ConsumerContextFilter 用 `add`（覆盖）合并用户附件，AbstractInvoker 用 `addIfAbsent`（不覆盖）补充默认附件。这个优先级设计保证了：**用户手动设置的 > 链式透传的 > 框架默认的**。

---

## 第九部分：实战场景 —— 分布式链路追踪的完整实现

### 9.1 原理

利用 Dubbo Filter + RpcContext attachment，在每次 RPC 调用时自动注入和传递 traceId、spanId，实现全链路追踪。

### 9.2 Consumer 端 Filter

```java
@Activate(group = CommonConstants.CONSUMER)
public class TraceConsumerFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 从 MDC 或 ThreadLocal 获取当前链路的 traceId
        String traceId = MDC.get("traceId");
        if (traceId == null) {
            traceId = UUID.randomUUID().toString();
        }

        // 通过 ClientAttachment 设置
        // ConsumerContextFilter 会自动搬运到 RpcInvocation → 序列化发送
        RpcContext.getClientAttachment().setAttachment("traceId", traceId);
        RpcContext.getClientAttachment().setAttachment("spanId", generateSpanId());

        return invoker.invoke(invocation);
    }
}
```

### 9.3 Provider 端 Filter

```java
@Activate(group = CommonConstants.PROVIDER)
public class TraceProviderFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // ContextFilter 已经把附件设置到 ServerAttachment 了
        // 这里直接从 ServerAttachment 读取
        String traceId = RpcContext.getServerAttachment().getAttachment("traceId");
        String spanId = RpcContext.getServerAttachment().getAttachment("spanId");

        // 设置到 MDC，方便日志框架输出
        MDC.put("traceId", traceId);
        MDC.put("spanId", spanId);

        try {
            return invoker.invoke(invocation);
        } finally {
            MDC.clear();
        }
    }
}
```

### 9.4 SPI 注册

```
# META-INF/dubbo/org.apache.dubbo.rpc.Filter
traceConsumer=com.example.TraceConsumerFilter
traceProvider=com.example.TraceProviderFilter
```

### 9.5 为什么不需要手动搬运到下游？

如果 B 收到 A 的请求后又调 C，traceId 会自动透传到 C，因为：
1. `ContextFilter` 把 traceId 设到了 B 的 `ServerAttachment`
2. B 调 C 时，`ConsumerContextFilter` 第一步会把 `ServerAttachment` 合并到发给 C 的 invocation
3. C 的 `ContextFilter` 再把它设到 C 的 `ServerAttachment`

全链路无感知透传，业务代码完全不需要关心。

---

## 第十部分：注意事项

### 10.1 attachment 只对下一次调用生效

```java
RpcContext.getClientAttachment().setAttachment("key", "value");
serviceA.method1();  // ← 带了 key=value
serviceA.method2();  // ← 不带了！因为 method1 调完后 ClientAttachment 被清空了
```

如果每次调用都需要带，要么在 Filter 里统一设置，要么每次调用前都 set 一次。

### 10.2 异步场景的 Context 丢失

RpcContext 基于 ThreadLocal，如果你在业务代码里切了线程（`CompletableFuture.supplyAsync()`），新线程里 RpcContext 是空的。需要手动保存和恢复：

```java
// 保存
RpcContext.RestoreContext snapshot = RpcContext.storeContext();

CompletableFuture.runAsync(() -> {
    // 恢复到新线程
    RpcContext.restoreContext(snapshot);
    // 现在可以正常读 ServerAttachment
    String traceId = RpcContext.getServerAttachment().getAttachment("traceId");
});
```

### 10.3 不要用 attachment 传大对象

Attachment 会被序列化到每一个请求/响应的 Body 里。如果你塞一个大 JSON 进去（比如几 MB），每次 RPC 调用都会多传这些数据，严重影响性能和带宽。只传 traceId、userId 这种短字符串。

### 10.4 避免使用框架保留的 key

以下 key 是 Dubbo 内部使用的，业务代码不要覆盖：`path`、`interface`、`version`、`group`、`token`、`timeout`、`async`、`dubbo.tag`、`dubbo.force.tag`。
