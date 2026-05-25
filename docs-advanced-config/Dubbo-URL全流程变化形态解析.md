# Dubbo URL 全流程变化形态解析

> 本文档从 URL 这一核心视角出发，完整梳理 Dubbo 中 Provider 暴露服务、Consumer 引用服务、以及请求/响应三大流程中，URL 在每一步的具体形态和变化逻辑。
>
> **核心思想**：在 Dubbo 内部，一切信息都用 URL 来传递。URL 是 Dubbo 的"统一数据总线"——协议类型、机器地址、接口名、配置参数全部编码在 URL 里。不同阶段的处理逻辑通过修改 URL 的协议头、host、port、参数来实现路由和控制。

---

## 一、Provider 暴露服务过程中的 URL 变化

假设我们有一个服务 `com.example.UserService`，实现类 `UserServiceImpl`，配置了 Nacos 注册中心，使用 dubbo 协议，部署在 `192.168.1.100` 机器上。

### 阶段 1：加载注册中心配置 - 生成注册中心 URL

**触发时机**：`ServiceConfig.doExportUrls()` 中调用 `ConfigValidationUtils.loadRegistries(this, true)`

**URL 形态**：

```
registry://127.0.0.1:8848/org.apache.dubbo.registry.RegistryService?application=my-app&registry=nacos&timestamp=1700000000000
```

**解读**：

| 部分 | 值 | 含义 |
|------|-----|------|
| 协议头 | `registry://` | 标识这是一个注册中心 URL，后续 Protocol SPI 会路由到 RegistryProtocol |
| host:port | `127.0.0.1:8848` | 注册中心的地址（Nacos 的地址） |
| path | `org.apache.dubbo.registry.RegistryService` | 固定的 path，表示注册中心服务 |
| 参数 registry | `nacos` | 真正的注册中心类型（后续会替换协议头） |
| 参数 application | `my-app` | 当前应用名 |

---

### 阶段 2：构建服务 URL（Provider URL）

**触发时机**：`ServiceConfig.doExportUrlsFor1Protocol()` 中调用 `buildUrl(protocolConfig, map)`

**URL 形态**：

```
dubbo://192.168.1.100:20880/com.example.UserService?anyhost=true&application=my-app&bind.ip=192.168.1.100&bind.port=20880&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=com.example.UserService&methods=getUserById,createUser,updateUser&pid=12345&release=3.2.0&revision=1.0.0&side=provider&timeout=3000&timestamp=1700000000000&version=1.0.0
```

**解读**：

| 部分 | 值 | 含义 |
|------|-----|------|
| 协议头 | `dubbo://` | 服务使用的 RPC 协议（也可能是 `tri://`） |
| host:port | `192.168.1.100:20880` | 本机 IP + 协议端口 |
| path | `com.example.UserService` | 服务接口全限定名 |
| interface | `com.example.UserService` | 接口名（冗余但必要，某些场景 path 可能被修改） |
| methods | `getUserById,createUser,updateUser` | 该接口所有方法名 |
| side | `provider` | 标识这是 Provider 端 |
| timeout | `3000` | 超时时间（毫秒） |
| version | `1.0.0` | 服务版本号 |
| anyhost | `true` | 监听所有网卡 |
| dynamic | `true` | 动态注册（服务下线自动注销） |
| generic | `false` | 非泛化服务 |
| pid | `12345` | 当前进程 ID |
| bind.ip / bind.port | 本机地址 | 实际绑定地址（可能和注册地址不同，比如 Docker 场景） |

**这是 Provider 最核心的 URL**，后续所有步骤都是对这个 URL 做变形。

---

### 阶段 3：本地暴露 - injvm URL

**触发时机**：`ServiceConfig.exportLocal(url)` — 默认情况下先做一次本地暴露

**URL 变化**：把阶段 2 的 URL 改三个东西：协议改为 `injvm`、host 改为 `127.0.0.1`、port 改为 `0`

**URL 形态**：

```
injvm://127.0.0.1/com.example.UserService?anyhost=true&application=my-app&bind.ip=192.168.1.100&bind.port=20880&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=com.example.UserService&methods=getUserById,createUser,updateUser&pid=12345&release=3.2.0&revision=1.0.0&side=provider&timeout=3000&timestamp=1700000000000&version=1.0.0
```

**和阶段 2 的区别**：

| 变化点 | 阶段 2 | 阶段 3 |
|--------|--------|--------|
| 协议头 | `dubbo://` | `injvm://` |
| host | `192.168.1.100` | `127.0.0.1` |
| port | `20880` | （无端口/0） |

**后续处理**：Protocol SPI 看到 `injvm://` 协议头 -> 路由到 `InjvmProtocol.export()` -> 不开任何网络端口，只在 JVM 内存中注册一个 Exporter。同 JVM 内的消费者可以直接从内存找到它调用。

---

### 阶段 4：远程暴露 - "URL 套娃"（registry URL 包含 provider URL）

**触发时机**：`ServiceConfig.exportRemote()` — 把 provider URL 作为属性塞进 registry URL

**URL 形态**（传给 `doExportUrl()` 的 URL）：

```
registry://127.0.0.1:8848/org.apache.dubbo.registry.RegistryService?application=my-app&registry=nacos&timestamp=1700000000000
```

**关键**：这个 URL 对象的 `attributes` 属性中有一个 key 为 `export` 的字段，值是阶段 2 的完整 provider URL：

```
attributes: {
  "export": "dubbo://192.168.1.100:20880/com.example.UserService?timeout=3000&..."
}
```

**为什么要套娃？**

Protocol SPI 根据 URL 协议头路由。如果直接传 `dubbo://` 进去，就直接走 `DubboProtocol` 只开端口不注册。套一层 `registry://` 后：

1. SPI 先路由到 `RegistryProtocol`
2. `RegistryProtocol` 从 attributes 中取出真正的 `dubbo://` URL，交给 `DubboProtocol` 开端口
3. `RegistryProtocol` 再向注册中心注册这个 provider URL

一箭双雕：既开了端口，又注册到了 Nacos。

---

### 阶段 5：RegistryProtocol 内部处理 - provider URL 被动态配置覆盖

**触发时机**：`RegistryProtocol.export()` 中 `overrideUrlWithConfig(providerUrl, ...)`

**URL 形态**（可能被配置中心推送的规则修改）：

```
dubbo://192.168.1.100:20880/com.example.UserService?anyhost=true&application=my-app&timeout=5000&...
```

注意 `timeout` 可能从 `3000` 被动态覆盖为 `5000`（如果配置中心推送了 override 规则的话）。如果没有 override 规则，URL 保持不变。

---

### 阶段 6：DubboProtocol 开端口时的 URL

**触发时机**：`DubboProtocol.export()` -> `openServer()` -> `createServer(url)`

**URL 形态**（增加了网络层参数）：

```
dubbo://192.168.1.100:20880/com.example.UserService?anyhost=true&application=my-app&bind.ip=192.168.1.100&bind.port=20880&channel.readonly.sent=true&codec=dubbo&heartbeat=60000&interface=com.example.UserService&methods=getUserById,createUser,updateUser&side=provider&timeout=3000&version=1.0.0&...
```

**新增/变化的参数**：

| 参数 | 值 | 来源 |
|------|-----|------|
| codec | `dubbo` | 编解码器名称（DubboCodec） |
| heartbeat | `60000` | 心跳间隔（默认 60 秒） |
| channel.readonly.sent | `true` | 只读事件发送标记 |

这个 URL 最终传给 Netty Server，Server 从中读取 bind 地址、心跳间隔等参数来配置自身。

---

### 阶段 7：注册到注册中心的 URL（精简版）

**触发时机**：`RegistryProtocol.export()` 中 `customizeURL(providerUrl, registryUrl)` — 精简参数后注册

**URL 形态**（写入 Nacos/Zookeeper 的 URL）：

```
dubbo://192.168.1.100:20880/com.example.UserService?application=my-app&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=com.example.UserService&methods=getUserById,createUser,updateUser&pid=12345&release=3.2.0&revision=1.0.0&side=provider&timeout=3000&timestamp=1700000000000&version=1.0.0
```

**和阶段 2 的区别**：注册到注册中心的 URL 会**去掉**一些纯本地参数（如 `bind.ip`、`bind.port` 等只对本机有意义的参数），保留对消费者有用的参数。消费者从注册中心拉到这个 URL 后就知道：去 `192.168.1.100:20880` 用 `dubbo` 协议调 `UserService` 就行了。

---

### Provider 暴露 URL 变化全景图

```
[阶段2] 构建 Provider URL
dubbo://192.168.1.100:20880/com.example.UserService?timeout=3000&side=provider&...
    |
    +---> [阶段3] 本地暴露（改协议头）
    |    injvm://127.0.0.1/com.example.UserService?timeout=3000&...
    |       -> InjvmProtocol.export() -> 内存注册，不开端口
    |
    +---> [阶段4] 远程暴露（套入 registry URL）
         registry://127.0.0.1:8848/...?registry=nacos
         attributes: { export: "dubbo://192.168.1.100:20880/..." }
            |
            +---> [阶段5] RegistryProtocol 取出 provider URL，可能被 override
                 dubbo://192.168.1.100:20880/...?timeout=5000（如有覆盖）
                    |
                    +---> [阶段6] DubboProtocol 补充网络参数后开端口
                    |    dubbo://192.168.1.100:20880/...?codec=dubbo&heartbeat=60000&...
                    |       -> Netty bootstrap.bind(192.168.1.100:20880)
                    |
                    +---> [阶段7] 精简后注册到 Nacos
                         dubbo://192.168.1.100:20880/...?timeout=3000&methods=...
                            -> Nacos 创建服务实例
```

---

## 二、Consumer 引用服务过程中的 URL 变化

假设消费者应用 `order-app` 引用 `com.example.UserService`，配置了 Nacos 注册中心。

### 阶段 1：加载注册中心 URL + 附加引用参数

**触发时机**：`ReferenceConfig.createProxy()` 中 `aggregateUrlFromRegistry(referenceParameters)`

**URL 形态**：

```
service-discovery-registry://127.0.0.1:8848/org.apache.dubbo.registry.RegistryService?application=order-app&dubbo=2.0.2&interface=com.example.UserService&pid=23456&refer=application%3Dorder-app%26dubbo%3D2.0.2%26interface%3Dcom.example.UserService%26methods%3DgetUserById%2CcreateUser%26side%3Dconsumer%26timeout%3D3000%26version%3D1.0.0&registry=nacos&timestamp=1700000001000
```

**解读**：

| 部分 | 值 | 含义 |
|------|-----|------|
| 协议头 | `service-discovery-registry://` | 应用级服务发现的注册中心协议（Dubbo 3.x 默认） |
| host:port | `127.0.0.1:8848` | 注册中心地址 |
| 参数 refer | URL 编码的引用参数 | 消费者的配置信息（超时、重试等），编码后作为参数附在 registry URL 上 |
| 参数 registry | `nacos` | 注册中心类型 |

**`refer` 参数解码后的内容**：

```
application=order-app&dubbo=2.0.2&interface=com.example.UserService&methods=getUserById,createUser&side=consumer&timeout=3000&version=1.0.0
```

这些就是消费者"我要什么"的描述。

---

### 阶段 2：RegistryProtocol.refer() - 构建消费者 URL

**触发时机**：`RegistryProtocol.doRefer()` 中构建 `consumerUrl`

**URL 形态**：

```
consumer://192.168.1.200/com.example.UserService?application=order-app&dubbo=2.0.2&interface=com.example.UserService&methods=getUserById,createUser&pid=23456&side=consumer&timeout=3000&version=1.0.0
```

**解读**：

| 部分 | 值 | 含义 |
|------|-----|------|
| 协议头 | `consumer://` | 标识这是消费者 URL（注册到注册中心时用这个让 Provider 知道谁在消费它） |
| host | `192.168.1.200` | 消费者自身的 IP |
| port | 无（0） | 消费者不监听端口 |
| path | `com.example.UserService` | 要消费的接口 |
| side | `consumer` | 消费端标识 |

这个 URL 会被注册到注册中心的 consumers 目录下，让 Provider 方知道有谁在消费它的服务。

---

### 阶段 3：订阅注册中心后收到的 Provider URL 列表

**触发时机**：`RegistryDirectory.notify()` 收到注册中心推送

**URL 形态**（注册中心推送过来的原始 Provider URL，就是 Provider 阶段 7 注册的那个）：

```
dubbo://192.168.1.100:20880/com.example.UserService?application=my-app&dubbo=2.0.2&interface=com.example.UserService&methods=getUserById,createUser,updateUser&side=provider&timeout=3000&version=1.0.0

dubbo://192.168.1.101:20880/com.example.UserService?application=my-app&dubbo=2.0.2&interface=com.example.UserService&methods=getUserById,createUser,updateUser&side=provider&timeout=3000&version=1.0.0
```

如果有多个 Provider 节点，会收到一个 URL 列表。

---

### 阶段 4：合并 Consumer 配置后的 URL

**触发时机**：`RegistryDirectory.refreshInvoker()` 中 `mergeUrl(providerUrl)` — 消费者参数覆盖 Provider 参数

**URL 形态**：

```
dubbo://192.168.1.100:20880/com.example.UserService?application=my-app&check=false&dubbo=2.0.2&interface=com.example.UserService&loadbalance=random&methods=getUserById,createUser,updateUser&pid=23456&retries=2&side=consumer&timeout=3000&version=1.0.0
```

**变化点**：

| 参数 | 来源 | 说明 |
|------|------|------|
| side | 从 `provider` 变为 `consumer` | 消费端覆盖 |
| retries | 新增（消费者配置） | 消费端的重试次数 |
| loadbalance | 新增（消费者配置） | 负载均衡策略 |
| check | 新增（消费者配置） | 启动时是否检查 Provider 可用 |
| timeout | `3000` | 如果消费者也配了 timeout，以消费者为准；否则用 Provider 的 |

**合并规则**：Consumer 配置 > Provider 配置。如果消费者配了 `timeout=5000`，那合并后 timeout 就是 5000，不管 Provider 配的是多少。

---

### 阶段 5：DubboProtocol.refer() 建连时的 URL

**触发时机**：`DubboProtocol.protocolBindingRefer()` -> `initClient(url)` — 创建到 Provider 的 TCP 连接

**URL 形态**（补充了网络层参数）：

```
dubbo://192.168.1.100:20880/com.example.UserService?application=order-app&codec=dubbo&heartbeat=60000&interface=com.example.UserService&loadbalance=random&methods=getUserById,createUser,updateUser&retries=2&side=consumer&timeout=3000&version=1.0.0
```

**新增参数**：

| 参数 | 值 | 说明 |
|------|-----|------|
| codec | `dubbo` | 编解码器（DubboCodec） |
| heartbeat | `60000` | 心跳间隔，和 Server 端保持一致 |

这个 URL 最终传给 NettyClient，Client 从中读取 `host:port` 去建 TCP 连接，读取 `heartbeat` 配置心跳。

---

### Consumer 引用 URL 变化全景图

```
[阶段1] 注册中心 URL + refer 参数
service-discovery-registry://127.0.0.1:8848/...?refer=interface%3DUserService%26timeout%3D3000%26...
    |
    +---> RegistryProtocol.refer()
         |
         +---> [阶段2] 构建消费者 URL（注册到注册中心 consumers 目录）
         |    consumer://192.168.1.200/com.example.UserService?side=consumer&...
         |
         +---> [阶段3] 订阅后收到 Provider URL 列表
         |    dubbo://192.168.1.100:20880/com.example.UserService?side=provider&...
         |    dubbo://192.168.1.101:20880/com.example.UserService?side=provider&...
         |
         +---> [阶段4] 合并消费者配置
              dubbo://192.168.1.100:20880/com.example.UserService?side=consumer&retries=2&loadbalance=random&...
                 |
                 +---> [阶段5] 补充网络参数后建连
                      dubbo://192.168.1.100:20880/...?codec=dubbo&heartbeat=60000&...
                         -> NettyClient.doConnect(192.168.1.100:20880)
```

---

## 三、RPC 请求/响应过程中的 URL 角色

请求和响应阶段，URL 不再发生"形变"，而是**作为参数来源被各层读取**。下面说清楚每一层从 URL 中读了什么。

### 请求发送阶段

#### 3.1 InvokerInvocationHandler（代理层）

**读取的 URL**：ClusterInvoker 持有的 URL（本质是 RegistryDirectory 的 consumerUrl）

```
dubbo://192.168.1.200/com.example.UserService?application=order-app&interface=com.example.UserService&timeout=3000&retries=2&version=1.0.0&...
```

**从中读取**：`serviceKey`（格式：`group/interface:version`）-> 设置到 RpcInvocation 的 targetServiceUniqueName

#### 3.2 FailoverClusterInvoker（集群层）

**读取的 URL**：Directory 的 consumerUrl

**从中读取**：

- `retries` -> 决定最多调几次（1 + retries）
- `loadbalance` -> 选哪个负载均衡算法（random/roundrobin/leastactive）

#### 3.3 DubboInvoker（协议层）

**读取的 URL**：该 DubboInvoker 对应的具体 Provider URL（阶段 4 合并后的 URL）

```
dubbo://192.168.1.100:20880/com.example.UserService?timeout=3000&version=1.0.0&...
```

**从中读取并塞入 RpcInvocation 的 attachments**：

- `path`（接口路径）-> `inv.setAttachment(PATH_KEY, url.getPath())` -> Provider 靠这个找 Exporter
- `version`（版本号）-> `inv.setAttachment(VERSION_KEY, version)` -> Provider 靠这个区分不同版本
- `timeout`（超时时间）-> `inv.setAttachment(TIMEOUT_KEY, timeout)` -> Provider 的 TimeoutFilter 靠这个判断是否已超时
- `group`（分组）-> Provider 靠这个区分不同分组

**这些 attachment 随请求一起序列化发给 Provider**，Provider 端靠它们来定位到具体的服务实例。

#### 3.4 HeaderExchangeChannel（交换层）

**不直接读 URL**，但构造 Request 对象时使用了 URL 相关信息：

```java
Request req = new Request();
req.setVersion(Version.getProtocolVersion());  // "2.0.2"
req.setTwoWay(true);
req.setData(rpcInvocation);  // 包含上面塞的所有 attachment
```

`Request` 对象被序列化后通过网络发出。

#### 3.5 Netty Pipeline（传输层）

DubboCodec 编码时参考 URL 的 serialization 参数决定序列化方式（Hessian2/Fastjson/Protobuf）。

---

### 网络上传输的"最终形态"

经过编码后，网络上传输的是 Dubbo 协议帧（不再是 URL 字符串，而是二进制帧）：

```
+--------------------------- 16 字节 Header ---------------------------+
|  0-1:  Magic Number = 0xdabb                                          |
|  2:    Flag = [请求][双向][Hessian2序列化]                              |
|  3:    Status = 0x00 (请求不用 Status)                                 |
|  4-11: Request ID = 1 (long类型，全局唯一递增)                          |
|  12-15: Body Length = 326 (Body 字节长度)                              |
+-----------------------------------------------------------------------+
+--------------------------- Body（Hessian2 序列化）---------------------+
|  dubbo version: "2.0.2"                                               |
|  path:          "com.example.UserService"                             |
|  version:       "1.0.0"                                               |
|  methodName:    "getUserById"                                         |
|  parameterTypes: "Ljava/lang/Long;"                                   |
|  arguments:     [1L]                                                  |
|  attachments:   {                                                     |
|      "path": "com.example.UserService",                               |
|      "interface": "com.example.UserService",                          |
|      "version": "1.0.0",                                             |
|      "timeout": "3000",                                               |
|      "group": null                                                    |
|  }                                                                    |
+-----------------------------------------------------------------------+
```

**注意**：网络帧里不再有完整的 URL 字符串！URL 的信息被拆散了——`path`、`version`、`group` 进了 Body 的 attachments；协议信息进了 Header 的 Flag 位；`requestId` 进了 Header。

---

### 响应返回阶段

#### Provider 端处理时如何通过 URL 信息定位服务

Provider 收到请求后，从 Body 中解析出 `RpcInvocation`，然后靠 attachment 信息构建 `serviceKey`：

```java
// serviceKey = "group/path:version:port"
// 例如: "com.example.UserService:1.0.0:20880"
String serviceKey = serviceKey(port, path, version, group);
```

然后从 `exporterMap` 中用这个 key 查找到对应的 Exporter -> 拿到 Invoker -> 调用你的业务代码。

**这就是为什么 DubboInvoker 发请求时要把 path、version、group 塞进 attachment——Provider 靠这些信息在同一端口上路由到正确的服务实现。**

#### 响应帧的形态

```
+--------------------------- 16 字节 Header ---------------------------+
|  0-1:  Magic Number = 0xdabb                                          |
|  2:    Flag = [响应][Hessian2序列化]                                    |
|  3:    Status = 20 (OK) / 30 (SERVICE_ERROR) / 31 (TIMEOUT)           |
|  4-11: Request ID = 1  <-- 和请求相同！Consumer 靠它匹配 DefaultFuture  |
|  12-15: Body Length = 156                                              |
+-----------------------------------------------------------------------+
+--------------------------- Body（Hessian2 序列化）---------------------+
|  如果 Status=OK:                                                      |
|    响应标志位: RESPONSE_VALUE (表示有正常返回值)                         |
|    返回值: User{id=1, name="张三", age=25}  <-- 你的业务返回值          |
|                                                                       |
|  如果 Status=OK 但有业务异常:                                          |
|    响应标志位: RESPONSE_WITH_EXCEPTION                                 |
|    异常对象: UserNotFoundException("用户不存在")                         |
|                                                                       |
|  如果 Status=SERVICE_ERROR:                                           |
|    错误信息字符串: "Server side thread pool is exhausted"               |
+-----------------------------------------------------------------------+
```

#### Consumer 收到响应后的匹配逻辑

```
Response.getId() = 1
    -> DefaultFuture.FUTURES.remove(1)  -> 找到对应的 Future
        -> future.complete(AppResponse{value=User对象})
            -> AsyncRpcResult.recreate()
                -> 返回 User 对象给业务代码
```

---

## 四、URL 全景对照表

| 阶段 | URL 协议头 | host:port | 主要用途 |
|------|-----------|-----------|----------|
| Provider 构建服务 URL | `dubbo://` | `本机IP:20880` | 描述"我是谁、在哪里、提供什么" |
| Provider 本地暴露 | `injvm://` | `127.0.0.1:0` | 同 JVM 内调用优化，不走网络 |
| Provider 远程暴露（套娃） | `registry://` | `注册中心IP:8848` | 触发 RegistryProtocol，先开端口再注册 |
| Provider 注册到注册中心 | `dubbo://` | `本机IP:20880` | 写入注册中心供消费者发现 |
| Consumer 订阅 URL | `consumer://` | `消费者IP:0` | 告诉注册中心"我要消费什么" |
| Consumer 收到 Provider 列表 | `dubbo://` | `ProviderIP:20880` | 注册中心推送的 Provider 地址 |
| Consumer 合并后的调用 URL | `dubbo://` | `ProviderIP:20880` | 合并了 Consumer 侧配置的最终调用 URL |
| 请求发送时 | Request 对象 | -- | URL 信息被拆散为 attachments（path/version/timeout） |
| 响应返回时 | Response 对象 | -- | 只携带 requestId + 业务结果 |

---

## 五、核心设计思想

### 5.1 URL 总线设计

Dubbo 把所有配置信息统一编码为 URL，在各层之间传递。URL 就像一条"信息总线"——任何一层拿到 URL 就能知道完整的上下文信息（去哪里、找什么服务、超时多少、重试几次）。这避免了在方法签名中传递大量零散参数。

### 5.2 URL 协议头驱动 SPI 路由

`protocolSPI.export(invoker)` 和 `protocolSPI.refer(type, url)` 都是根据 URL 的协议头做自适应路由的：

- `registry://` -> RegistryProtocol（先注册再暴露）
- `dubbo://` -> DubboProtocol（开 Netty 端口 / 建 Netty 连接）
- `tri://` -> TripleProtocol（HTTP/2）
- `injvm://` -> InjvmProtocol（内存调用）

**改变 URL 协议头 = 改变整个处理链路。** 这就是为什么本地暴露只需要把 `dubbo://` 改成 `injvm://` 就能走完全不同的逻辑。

### 5.3 URL 套娃（属性嵌套）

远程暴露时，把真正的服务 URL 作为属性塞进 `registry://` URL 里：

```
registry://... ?export=dubbo://...(URL编码后的服务地址)
```

消费者引用时，把引用参数作为属性塞进 `registry://` URL 里：

```
service-discovery-registry://... ?refer=timeout%3D3000%26...
```

这样 RegistryProtocol 收到 URL 后，能同时拿到注册中心地址（URL 主体）和服务/引用信息（URL 属性），实现"一个 URL 携带两层信息"。

### 5.4 URL 在请求时被"拆解"

一旦连接建立，实际发请求时 URL 不再以完整形式出现。URL 里的关键信息被拆散放入 `RpcInvocation` 的 attachments 中：

- `path` <-- URL 的 path
- `version` <-- URL 参数
- `timeout` <-- URL 参数
- `group` <-- URL 参数

Provider 端收到请求后，用 `path + version + group + port` 拼成 `serviceKey`，从 `exporterMap` 里找到对应的 Invoker。**请求阶段不传完整 URL，只传必要的路由信息，节省网络带宽。**

---

## 六、一图看懂 URL 生命周期

```
                    Provider 端                                    Consumer 端
                    =========                                     =========

[1] 配置拼装        dubbo://192.168.1.100:20880
                    /com.example.UserService
                    ?timeout=3000&retries=2...
                           |
                           v
[2] 本地暴露        injvm://127.0.0.1:0/...
    (分支)         (InjvmProtocol 处理)
                           |
                           v
[3] 远程暴露        registry://nacos:8848/...
    (套娃)         ?export=dubbo%3A%2F%2F192.168.1.100%3A20880...
                           |
                           v
[4] 开端口          dubbo://192.168.1.100:20880/...
    (从属性取出)    -> Netty bind(20880)
                           |
                           v
[5] 注册            dubbo://192.168.1.100:20880/...  ---推送--->   [6] 消费者订阅
                    -> 写入 Nacos 服务列表                          service-discovery-registry://
                                                                   nacos:8848/...?refer=timeout%3D3000
                                                                          |
                                                                          v
                                                                  [7] 收到 Provider 列表
                                                                   dubbo://192.168.1.100:20880/...
                                                                   dubbo://192.168.1.101:20880/...
                                                                          |
                                                                          v
                                                                  [8] 合并 URL（Consumer 配置覆盖）
                                                                   dubbo://192.168.1.100:20880/...
                                                                   ?timeout=3000(consumer)&...
                                                                          |
                                                                          v
                                                                  [9] 建立 TCP 连接
                                                                   Netty connect(192.168.1.100:20880)
                                                                          |
                                                                          v
                    +---------------------- RPC 调用 --------------------------+
                    |                                                          |
                    |  [10] 请求: URL 信息拆散为 attachments                    |
                    |       {path:"com.example.UserService",                   |
                    |        version:"1.0.0", timeout:"3000"}                  |
                    |       + requestId + 方法名 + 参数                         |
                    |                                                          |
                    |  [11] 响应: 只有 requestId + 业务结果/异常                 |
                    |       Response{id=123, status=OK, result=User{...}}       |
                    +----------------------------------------------------------+
```

---

## 七、总结

Dubbo 的 URL 就像一张"信息身份证"：

- **在配置阶段**：它是完整的，携带所有配置信息（协议、地址、接口、超时、重试等）。
- **在路由阶段**：它的协议头决定了走哪条处理链路（registry/dubbo/injvm/tri）。
- **在注册阶段**：它被精简后写入注册中心，供消费者发现。
- **在合并阶段**：Consumer 的配置会覆盖 Provider 的默认配置，生成最终的调用 URL。
- **在请求阶段**：它被"拆解"为 attachments，只传输必要的路由信息。
- **在响应阶段**：它不再出现，只有 requestId 和业务结果在网络上传输。

理解了 URL 在每个阶段的形态和变化，就理解了 Dubbo 框架是如何用一个统一的抽象贯穿整个 RPC 调用链路的。
