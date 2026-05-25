# Dubbo 服务提供者暴露服务 —— 源码全流程解析

> 基于源码项目 `/Users/zhanghonghao/Desktop/dubbo` 逐步分析，从 Spring 容器启动到 Netty 端口监听，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
Spring 容器启动
  |
  +-- 1. ServiceAnnotationPostProcessor 扫描 @DubboService 注解
  |     -> 为每个服务实现类注册一个 ServiceBean 的 BeanDefinition
  |
  +-- 2. ServiceBean.afterPropertiesSet()
  |     -> 将自身注册到 ModuleConfigManager
  |
  +-- 3. Spring ContextRefreshedEvent 触发
  |     -> DubboDeployApplicationListener.onContextRefreshedEvent()
  |         -> moduleModel.getDeployer().start()
  |
  +-- 4. DefaultModuleDeployer.startSync()
  |     -> exportServices()
  |         -> 遍历所有 ServiceConfig，调用 sc.export()
  |
  +-- 5. ServiceConfig.export()
  |     -> doExport()
  |         -> doExportUrls()
  |             -> doExportUrlsFor1Protocol()  (每个协议执行一次)
  |                 -> exportUrl()
  |                     +-- exportLocal()     <-- 本地暴露 (injvm)
  |                     +-- exportRemote()    <-- 远程暴露
  |                           -> doExportUrl()
  |
  +-- 6. doExportUrl() 内部做两件事：
  |     +-- proxyFactory.getInvoker(ref, interfaceClass, url)
  |     |     -> JavassistProxyFactory: Wrapper 字节码生成 + AbstractProxyInvoker 创建
  |     |
  |     +-- protocolSPI.export(invoker)
  |           -> 根据 URL 协议自适应路由：
  |               +-- registry:// -> RegistryProtocol.export()
  |               +-- dubbo://   -> DubboProtocol.export()
  |               +-- tri://     -> TripleProtocol.export()
  |
  +-- 7. RegistryProtocol.export()  (有注册中心时)
  |     +-- doLocalExport() -> 调用底层协议 (DubboProtocol/TripleProtocol) 的 export()
  |     +-- register() -> 向注册中心注册服务 URL
  |
  +-- 8. DubboProtocol.export()
  |     -> openServer() -> createServer()
  |         -> Exchangers.bind() -> HeaderExchanger.bind()
  |             -> Transporters.bind() -> NettyTransporter.bind()
  |                 -> new NettyServer(url, handler)
  |                     -> doOpen() -> bootstrap.bind(address)  <-- Netty 端口监听启动!
  |
  +-- 9. TripleProtocol.export()
        -> bindServerPort()
            -> PortUnificationExchanger.bind()
                -> new NettyPortUnificationServer(url, handler)
                    -> doOpen0() -> bootstrap.bind(address)  <-- Netty 端口监听启动!
```

---

## 第一阶段：Spring 容器扫描与注册

### 1.1 ServiceAnnotationPostProcessor —— 扫描 @DubboService

**源码位置**: `dubbo-config/dubbo-config-spring/src/main/java/org/apache/dubbo/config/spring/beans/factory/annotation/ServiceAnnotationPostProcessor.java`

这个类实现了 Spring 的 `BeanDefinitionRegistryPostProcessor` 接口，在 Spring 容器启动的**极早期**（Bean 定义注册阶段）就开始工作。

**触发时机**: Spring 容器调用所有 `BeanDefinitionRegistryPostProcessor` 的 `postProcessBeanDefinitionRegistry()` 方法时。

**核心逻辑**:

```java
@Override
public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
    this.registry = registry;
    scanServiceBeans(resolvedPackagesToScan, registry);
}
```

`scanServiceBeans()` 方法内部使用 `DubboClassPathBeanDefinitionScanner` 扫描指定包路径下所有带有 `@DubboService`（或旧版 `@Service`）注解的类。对于每个扫描到的类，调用 `buildServiceBeanDefinition()` 构建一个 `ServiceBean` 类型的 BeanDefinition，其中：

- `ref` 属性指向实际的服务实现 Bean（比如你写的 `UserServiceImpl`）
- `interface` 属性设为服务接口全限定名
- 注解上的 `timeout`、`retries`、`version`、`group` 等属性全部解析并设置进去

最终这个 `ServiceBean` 的 BeanDefinition 被注册到 Spring 容器中。

---

### 1.2 ServiceBean.afterPropertiesSet() —— 注册到 Dubbo 配置管理器

**源码位置**: `dubbo-config/dubbo-config-spring/src/main/java/org/apache/dubbo/config/spring/ServiceBean.java`

`ServiceBean` 继承自 `ServiceConfig`，同时实现了 Spring 的 `InitializingBean` 接口。当 Spring 完成这个 Bean 的属性注入后，会回调 `afterPropertiesSet()`：

```java
@Override
public void afterPropertiesSet() throws Exception {
    if (StringUtils.isEmpty(getPath())) {
        if (StringUtils.isNotEmpty(getInterface())) {
            setPath(getInterface());
        }
    }
    // 关键：将 ServiceBean 注册到 ModuleConfigManager
    ModuleModel moduleModel = DubboBeanUtils.getModuleModel(applicationContext);
    moduleModel.getConfigManager().addService(this);
    moduleModel.getDeployer().setPending();
}
```

这一步做了什么？把这个 `ServiceBean`（也就是 `ServiceConfig`）放进了 `ModuleConfigManager` 的服务列表里。后面 `DefaultModuleDeployer` 启动时，就是从这个列表里取出所有待暴露的服务。

---

### 1.3 DubboDeployApplicationListener —— 监听容器刷新事件

**源码位置**: `dubbo-config/dubbo-config-spring/src/main/java/org/apache/dubbo/config/spring/context/DubboDeployApplicationListener.java`

这个类监听 Spring 的 `ContextRefreshedEvent`（容器刷新完成事件）。当所有 Bean 都初始化完毕后，Spring 会发布这个事件，然后这个监听器触发 Dubbo 模块的启动：

```java
private void onContextRefreshedEvent(ContextRefreshedEvent event) {
    ModuleDeployer deployer = moduleModel.getDeployer();
    Future future = null;
    synchronized (singletonMutex) {
        future = deployer.start();  // <-- 触发 Dubbo 模块启动!
    }
    // 如果不是后台启动模式，同步等待完成
    if (!deployer.isBackground()) {
        future.get();
    }
}
```

**这就是服务暴露的真正触发点**：Spring 容器刷新完成 -> 监听器收到事件 -> 调用 `deployer.start()`。

---

## 第二阶段：DefaultModuleDeployer 编排服务导出

### 2.1 DefaultModuleDeployer.startSync()

**源码位置**: `dubbo-config/dubbo-config-api/src/main/java/org/apache/dubbo/config/deploy/DefaultModuleDeployer.java`

```java
private synchronized Future startSync() throws IllegalStateException {
    // 状态检查...
    onModuleStarting();
    initialize();

    // ===== 核心：导出所有服务 =====
    exportServices();

    // 准备应用实例
    applicationDeployer.prepareInternalModule();

    // 引用服务（消费端）
    referServices();

    // 后续处理...
    onModuleStarted();
    registerServices();  // 延迟注册到注册中心
    return startFuture;
}
```

### 2.2 exportServices() —— 遍历所有服务逐一导出

```java
private void exportServices() {
    for (ServiceConfigBase sc : configManager.getServices()) {
        exportServiceInternal(sc);
    }
}

private void exportServiceInternal(ServiceConfigBase sc) {
    ServiceConfig<?> serviceConfig = (ServiceConfig<?>) sc;
    if (!serviceConfig.isRefreshed()) {
        serviceConfig.refresh();  // 刷新配置（合并外部化配置）
    }
    if (sc.isExported()) {
        return;  // 已导出则跳过
    }

    if (exportAsync || sc.shouldExportAsync()) {
        // 异步导出
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            sc.export();
        }, executor);
        asyncExportingFutures.add(future);
    } else {
        // 同步导出（默认）
        sc.export(RegisterTypeEnum.AUTO_REGISTER_BY_DEPLOYER);
        exportedServices.add(sc);
    }
}
```

`configManager.getServices()` 返回的就是前面 `ServiceBean.afterPropertiesSet()` 中注册进去的所有 `ServiceConfig`。

---

## 第三阶段：ServiceConfig —— 服务暴露的核心编排

### 3.1 export() —— 入口方法

**源码位置**: `dubbo-config/dubbo-config-api/src/main/java/org/apache/dubbo/config/ServiceConfig.java`

```java
@Override
public void export(RegisterTypeEnum registerType) {
    if (this.exported) {
        return;
    }
    synchronized (this) {
        if (this.exported) {
            return;
        }
        if (!this.isRefreshed()) {
            this.refresh();  // 合并配置（优先级：JVM参数 > 外部化配置 > application.yml > 注解）
        }
        if (this.shouldExport()) {
            this.init();  // 初始化：解析接口方法、检查配置合法性

            if (shouldDelay()) {
                doDelayExport();  // 延迟暴露：启动定时任务，到时间再调 doExport
            } else {
                doExport(registerType);  // 正常暴露
            }
        }
    }
}
```

### 3.2 doExport() —— 状态检查后进入核心流程

```java
protected synchronized void doExport(RegisterTypeEnum registerType) {
    if (unexported) {
        throw new IllegalStateException("The service has already unexported!");
    }
    if (exported) {
        return;
    }
    if (StringUtils.isEmpty(path)) {
        path = interfaceName;  // 默认 path 就是接口名
    }
    doExportUrls(registerType);  // <-- 进入核心
    exported();  // 标记为已导出，发布事件
}
```

### 3.3 doExportUrls() —— 注册 ProviderModel + 遍历协议

> **这一步在干什么？**
>
> 前面 3.1 和 3.2 只是做了"能不能导出"的判断（配置合法性、是否已导出、是否延迟）。到了 3.3，才真正开始**准备导出所需的所有材料**。你可以把它理解成"出发前的打包"——告诉 Dubbo 框架"我有一个什么服务、它在哪些注册中心注册、用哪些协议暴露"。
>
> **做了什么事情？**
> 1. 把你的服务接口信息（有哪些方法、方法签名）注册到一个全局仓库里，后续收到 RPC 请求时能根据 serviceKey 找到对应的服务
> 2. 创建一个 `ProviderModel`——可以理解为你的服务在 Dubbo 内部的"身份证"，记录了"这个服务是谁、实现类是谁、接口长什么样"
> 3. 加载你配置的所有注册中心地址（比如 Nacos 在哪、Zookeeper 在哪）
> 4. 遍历你配置的所有协议（dubbo、tri），对每个协议分别走一遍完整的暴露流程
>
> **为什么需要这步？** 因为后面的步骤需要知道三个关键信息：服务元数据（方法签名等）、注册中心地址列表、协议列表。这一步就是把这三样东西准备好，交给下游。

```java
private void doExportUrls(RegisterTypeEnum registerType) {
    ModuleServiceRepository repository = getScopeModel().getServiceRepository();

    // 1. 注册服务描述符（方法签名等元数据）
    ServiceDescriptor serviceDescriptor = repository.registerService(getInterfaceClass());

    // 2. 创建 ProviderModel（服务提供者模型，包含 ref 实例引用）
    providerModel = new ProviderModel(
            serviceMetadata.getServiceKey(),
            ref,                    // <-- 你的服务实现类实例
            serviceDescriptor,
            getScopeModel(),
            serviceMetadata,
            interfaceClassLoader);
    repository.registerProvider(providerModel);

    // 3. 加载注册中心 URL 列表
    List<URL> registryURLs = ConfigValidationUtils.loadRegistries(this, true);
    // 例如: [registry://127.0.0.1:8848/...?registry=nacos]

    // 4. 遍历每个协议配置，逐一暴露
    for (ProtocolConfig protocolConfig : protocols) {
        doExportUrlsFor1Protocol(protocolConfig, registryURLs, registerType);
    }
}
```

**关键点**：如果你配置了多个协议（比如同时配了 dubbo 和 tri），这里会循环多次，每个协议都暴露一遍。

---

### 3.3.1 多协议多次暴露 —— 完整拆解每一次暴露发生了什么

> **为什么需要多次暴露？**
>
> 每种协议对应不同的网络传输方式和序列化格式，它们需要各自独立的端口监听和请求处理链路。dubbo 协议走自定义的二进制帧格式（默认端口 20880），Triple 协议走 gRPC/HTTP2（默认端口 50051），它们的字节解码、请求对象封装方式完全不同。所以一个服务如果想同时被这两种协议的消费者调用，就必须在两个端口上分别启动 Server，各自注册到 exporterMap 里。

假设你的配置如下：

```yaml
dubbo:
  protocols:
    dubbo:
      name: dubbo
      port: 20880
    tri:
      name: tri
      port: 50051
  registry:
    address: nacos://127.0.0.1:8848
```

服务接口为 `com.example.UserService`，版本 `1.0.0`。

那么 `doExportUrls()` 中的 `for (ProtocolConfig protocolConfig : protocols)` 循环会执行**两次**，每次走完整个暴露链路。下面逐次拆解：

---

#### 第一次循环：dubbo 协议暴露

**Step 1: doExportUrlsFor1Protocol(dubboProtocolConfig, registryURLs)**

构建出服务 URL：
```
dubbo://192.168.1.100:20880/com.example.UserService?version=1.0.0&timeout=3000&methods=getUserById,createUser&...
```

**Step 2: exportUrl() 内部处理**

(a) **本地暴露 exportLocal()**：

URL 变形为 `injvm://127.0.0.1:0/com.example.UserService?...`，走 `InjvmProtocol.export()`。在内存中的 exporterMap 注册一个 InjvmExporter，不开任何端口。

注意：本地暴露只会做一次。虽然循环执行了两次，但第二次 `exportLocal()` 检测到同接口已经做过本地暴露，会跳过（通过 `exportedLocalMap` 去重）。

(b) **远程暴露 exportRemote()**：

URL 套娃——把 `dubbo://192.168.1.100:20880/UserService?...` 塞进 `registry://127.0.0.1:8848/...` 的 `export` 属性里，然后调用 `doExportUrl()`。

**Step 3: doExportUrl() -> protocolSPI.export(invoker)**

SPI 根据 `registry://` 协议头路由到 `RegistryProtocol.export()`。

**Step 4: RegistryProtocol.export()**

- 从 registry URL 属性中取出真正的 provider URL：`dubbo://192.168.1.100:20880/UserService?...`
- 调用 `doLocalExport()`：委托给 `DubboProtocol.export()`

**Step 5: DubboProtocol.export()**

- 生成 serviceKey = `com.example.UserService:1.0.0:20880`
- 创建 `DubboExporter` 存入 `exporterMap`
- 调用 `openServer(url)`：发现 `192.168.1.100:20880` 还没有 Server
- 一路向下：`Exchangers.bind()` -> `HeaderExchanger.bind()` -> `Transporters.bind()` -> `NettyTransporter.bind()` -> `new NettyServer(url, handler)` -> `doOpen()` -> **bootstrap.bind(20880)**
- **Netty Server 在 20880 端口启动，开始监听 dubbo 协议请求**

**Step 6: 回到 RegistryProtocol.export()**

- 向 Nacos 注册服务实例：`dubbo://192.168.1.100:20880/com.example.UserService?version=1.0.0&...`
- Nacos 上出现一条服务实例记录

**第一次循环的最终效果：**

| 产物 | 内容 |
|------|------|
| 本地 exporterMap (InjvmProtocol) | `com.example.UserService` -> InjvmExporter |
| 远程 exporterMap (DubboProtocol) | `com.example.UserService:1.0.0:20880` -> DubboExporter |
| Netty Server | 监听 `0.0.0.0:20880`，处理 dubbo 二进制帧协议 |
| Nacos 注册中心 | 新增实例 `dubbo://192.168.1.100:20880/com.example.UserService` |

---

#### 第二次循环：Triple 协议暴露

**Step 1: doExportUrlsFor1Protocol(triProtocolConfig, registryURLs)**

构建出服务 URL：
```
tri://192.168.1.100:50051/com.example.UserService?version=1.0.0&timeout=3000&methods=getUserById,createUser&...
```

**Step 2: exportUrl() 内部处理**

(a) **本地暴露 exportLocal()**：

检测到同接口已做过本地暴露，**跳过**。

(b) **远程暴露 exportRemote()**：

URL 套娃——把 `tri://192.168.1.100:50051/UserService?...` 塞进 `registry://127.0.0.1:8848/...` 的 `export` 属性里。

**Step 3: doExportUrl() -> protocolSPI.export(invoker)**

SPI 路由到 `RegistryProtocol.export()`。

**Step 4: RegistryProtocol.export()**

- 取出 provider URL：`tri://192.168.1.100:50051/UserService?...`
- 调用 `doLocalExport()`：此时 providerUrlKey 与第一次不同（协议和端口都不同），所以不会命中引用计数复用，而是**创建新的 Exporter**
- 委托给 `TripleProtocol.export()`

**Step 5: TripleProtocol.export()**

- 生成 serviceKey = `com.example.UserService:1.0.0:50051`
- 创建 Exporter 存入 `exporterMap`
- 注册 gRPC 路径映射：`/com.example.UserService/getUserById`、`/com.example.UserService/createUser`
- 调用 `bindServerPort(url)`：发现 `192.168.1.100:50051` 还没有 Server
- 一路向下：`PortUnificationExchanger.bind()` -> `new NettyPortUnificationServer(url, handler)` -> `doOpen0()` -> **bootstrap.bind(50051)**
- **Netty Server 在 50051 端口启动，开始监听 HTTP/2 (gRPC) 请求**

**Step 6: 回到 RegistryProtocol.export()**

- 向 Nacos 注册服务实例：`tri://192.168.1.100:50051/com.example.UserService?version=1.0.0&...`
- Nacos 上出现第二条服务实例记录

**第二次循环的最终效果：**

| 产物 | 内容 |
|------|------|
| 远程 exporterMap (TripleProtocol) | `com.example.UserService:1.0.0:50051` -> TripleExporter |
| Netty Server | 监听 `0.0.0.0:50051`，处理 HTTP/2 (gRPC) 协议 |
| Nacos 注册中心 | 新增实例 `tri://192.168.1.100:50051/com.example.UserService` |
| gRPC 路径映射 | `/com.example.UserService/getUserById` -> Invoker |

---

#### 两次循环全部完成后的全局视图

```
                    ┌─────────────────────────────────────────────────────┐
                    │              Nacos 注册中心                           │
                    │                                                     │
                    │  实例1: dubbo://192.168.1.100:20880/UserService     │
                    │  实例2: tri://192.168.1.100:50051/UserService       │
                    └─────────────────────────────────────────────────────┘

                    ┌─────────────────────────────────────────────────────┐
                    │              Provider JVM                            │
                    │                                                     │
                    │  InjvmProtocol.exporterMap:                         │
                    │    "UserService" -> InjvmExporter (本地调用)          │
                    │                                                     │
                    │  DubboProtocol.exporterMap:                         │
                    │    "UserService:1.0.0:20880" -> DubboExporter       │
                    │                                                     │
                    │  TripleProtocol.exporterMap:                        │
                    │    "UserService:1.0.0:50051" -> TripleExporter      │
                    │                                                     │
                    │  NettyServer (port 20880) ← dubbo 协议消费者连接     │
                    │  NettyPortUnificationServer (port 50051) ← gRPC 消费者连接  │
                    │                                                     │
                    │  UserServiceImpl (唯一实例，被三个 Exporter 共享引用) │
                    └─────────────────────────────────────────────────────┘
```

**关键结论：**

1. **一份代码，多种接入方式**：你只写了一个 `UserServiceImpl`，但通过多协议暴露，dubbo 老客户端和 gRPC 新客户端都能调到你
2. **每种协议独立的 Server 和编解码**：dubbo 走 DubboCodec 二进制帧，Triple 走 HTTP/2 帧，互不干扰
3. **exporterMap 按协议隔离**：DubboProtocol 和 TripleProtocol 各自有独立的 exporterMap，通过不同的 serviceKey（端口不同）区分
4. **注册中心有两条记录**：消费者通过注册中心发现服务时，能看到两个地址，根据自己配置的协议选择对应的实例连接
5. **本地暴露只做一次**：不管配了几个协议，injvm 暴露只做一次，因为本地调用不走网络，跟协议无关

---

#### 补充：引用计数复用（多注册中心场景）

上面多协议的场景中，两次 `doLocalExport()` 的 providerUrlKey 不同（一个含 `dubbo:20880`，一个含 `tri:50051`），所以各自创建新的 Exporter。

但如果是**同一协议 + 多注册中心**的场景：

```yaml
dubbo:
  protocols:
    dubbo:
      port: 20880
  registries:
    nacos:
      address: nacos://127.0.0.1:8848
    zk:
      address: zookeeper://127.0.0.1:2181
```

那么对于 dubbo 协议，`exportRemote()` 中会遍历两个 registryURL，两次都调用 `RegistryProtocol.export()`。两次进入 `doLocalExport()` 时，providerUrlKey 相同（都是 `dubbo://192.168.1.100:20880/UserService?...`）。引用计数机制保证：

- 第一次：创建 DubboExporter，启动 NettyServer，引用计数 = 1
- 第二次：命中同一个 providerUrlKey，直接复用已有 Exporter，引用计数 = 2，**不会重复启动 Server**

两次的区别只在第 6 步：第一次注册到 Nacos，第二次注册到 Zookeeper。网络层只启动一次。

---

### 3.4 doExportUrlsFor1Protocol() —— 构建服务 URL

> **这一步在干什么？**
>
> 3.3 遍历每个协议时，会对每个协议调用这个方法。这一步的任务就一个：**把你的服务配置拼成一个 URL 字符串。**
>
> **为什么要搞成 URL？** 这是 Dubbo 的一个核心设计——在 Dubbo 内部，**所有信息都用 URL 来传递**。一个 URL 里包含了：用什么协议（dubbo/tri）、服务部署在哪台机器的哪个端口、接口名是什么、超时时间多少、重试几次等等。后续所有层拿到这个 URL 就知道该怎么处理了，不需要额外传一堆参数。
>
> **类比：** 就像你写一个 HTTP 请求 `http://192.168.1.100:8080/api/user?timeout=3000`，URL 本身就携带了"去哪里、找什么、怎么配置"的全部信息。Dubbo 的 URL 也是这个思路，只是协议头不是 http 而是 dubbo/tri。
>
> **做完之后：** 拿着这个拼好的 URL，进入下一步 `exportUrl()`，决定这个服务是本地暴露还是远程暴露。

```java
private void doExportUrlsFor1Protocol(
        ProtocolConfig protocolConfig, List<URL> registryURLs, RegisterTypeEnum registerType) {

    // 1. 构建 URL 参数 Map（包含 timeout、retries、methods 等所有配置）
    Map<String, String> map = buildAttributes(protocolConfig);

    // 2. 构建完整的服务 URL
    URL url = buildUrl(protocolConfig, map);
    // 例如: dubbo://192.168.1.100:20880/com.example.UserService?timeout=3000&...
    // 或者: tri://192.168.1.100:50051/com.example.UserService?timeout=3000&...

    // 3. 处理服务线程池
    processServiceExecutor(url);

    // 4. 进入暴露逻辑
    exportUrl(url, registryURLs, registerType);
}
```

### 3.5 exportUrl() —— 决定本地暴露还是远程暴露

> **这一步在干什么？**
>
> 3.4 把 URL 拼好了，现在要决定一个问题：**这个服务要暴露给谁用？**
>
> 有两种暴露方式：
> - **本地暴露（exportLocal）**：给同一个 JVM 里的消费者用。比如你的项目里既有 UserService 的实现，又有另一个模块调用 UserService——这时候走内存调用就行了，不用走网络，快得多。
> - **远程暴露（exportRemote）**：给其他机器上的消费者用。这才是真正要开端口、注册到 Nacos 的那条路。
>
> **为什么要分这两种？** 性能优化。如果调用方和提供方在同一个进程里，还走网络序列化反序列化一遍纯属浪费。本地暴露走 `injvm://` 协议，直接在内存里调用，延迟几乎为零。
>
> **默认行为：** 如果你没配 `scope` 参数，Dubbo 会**两种都做**——既做本地暴露（万一同 JVM 有消费者），又做远程暴露（给外部消费者）。

```java
private void exportUrl(URL url, List<URL> registryURLs, RegisterTypeEnum registerType) {
    String scope = url.getParameter(SCOPE_KEY);  // 获取 scope 参数

    if (!SCOPE_NONE.equalsIgnoreCase(scope)) {

        // ===== 本地暴露 =====
        // 除非 scope=remote，否则都会做本地暴露
        if (!SCOPE_REMOTE.equalsIgnoreCase(scope)) {
            exportLocal(url);
        }

        // ===== 远程暴露 =====
        // 除非 scope=local，否则都会做远程暴露
        if (!SCOPE_LOCAL.equalsIgnoreCase(scope)) {
            url = exportRemote(url, registryURLs, registerType);
            // 发布服务定义到元数据中心
            MetadataUtils.publishServiceDefinition(url, providerModel.getServiceModel(), getApplicationModel());
        }
    }
    this.urls.add(url);
}
```

### 3.6 exportLocal() —— 本地暴露（JVM 内部调用优化）

> **这一步在干什么？**
>
> 就是把 3.4 拼好的那个 URL（比如 `dubbo://192.168.1.100:20880/UserService?...`）**改头换面**——协议从 `dubbo` 改成 `injvm`，host 改成 `127.0.0.1`，端口改成 `0`。然后用这个改造后的 URL 去调 `doExportUrl()`。
>
> **为什么这么做？** 因为后面 `doExportUrl()` 里会根据 URL 的协议头来决定走哪个 Protocol 实现。协议头是 `injvm` 的话，就会走 `InjvmProtocol`——这个 Protocol 不会开任何网络端口，只是在内存里注册一下，同 JVM 的消费者直接从内存里找到这个 Invoker 就行了。
>
> **这步做完之后：** 本地暴露就结束了。简单、快速、不涉及网络。接下来 3.5 那个 if 判断会继续走远程暴露的逻辑。

```java
private void exportLocal(URL url) {
    URL local = URLBuilder.from(url)
            .setProtocol(LOCAL_PROTOCOL)  // 协议改为 injvm
            .setHost(LOCALHOST_VALUE)     // host 改为 127.0.0.1
            .setPort(0)                  // port 改为 0
            .build();
    doExportUrl(local, false, RegisterTypeEnum.NEVER_REGISTER);
}
```

### 3.7 exportRemote() —— 远程暴露（核心路径）

> **这一步在干什么？**
>
> 远程暴露意味着：你的服务要能被**其他机器上的进程**通过网络调用到。要实现这个目标，需要做两件事：
> 1. 在本机开一个网络端口，监听 RPC 请求
> 2. 把"我在哪个 IP、哪个端口、提供什么服务"这个信息注册到注册中心（Nacos/Zookeeper），这样消费者才能找到你
>
> **这步做了一个很精妙的操作：URL 套娃。** 它不是直接拿着 `dubbo://192.168.1.100:20880/UserService` 去调 `doExportUrl`，而是把这个 URL **塞进了** `registry://127.0.0.1:8848/...` 这个注册中心 URL 的属性里。
>
> **为什么要套娃？** 因为 Dubbo 的 SPI 路由机制是看 URL 协议头的。如果传 `dubbo://` 进去，就直接走 `DubboProtocol` 开端口了，不会注册到 Nacos。但是传 `registry://` 进去，就会先走 `RegistryProtocol`——这个 Protocol 会**先帮你开端口（从属性里取出真正的 dubbo:// URL），再帮你注册到注册中心**。一箭双雕。
>
> **如果没有注册中心呢？** 那就直连模式，直接传 `dubbo://` URL 去调 `doExportUrl`，只开端口不注册。消费者需要手动配置你的地址才能调到你。

```java
private URL exportRemote(URL url, List<URL> registryURLs, RegisterTypeEnum registerType) {
    if (CollectionUtils.isNotEmpty(registryURLs) && registerType != RegisterTypeEnum.NEVER_REGISTER) {
        // ===== 有注册中心的情况 =====
        for (URL registryURL : registryURLs) {
            // 将 provider URL 作为属性放入 registry URL
            // 这样 RegistryProtocol 就能从中取出真正的服务地址
            doExportUrl(registryURL.putAttribute(EXPORT_KEY, url), true, registerType);
        }
    } else {
        // ===== 无注册中心（直连模式）=====
        doExportUrl(url, true, registerType);
    }
    return url;
}
```

### 3.8 doExportUrl() —— 创建 Invoker + 调用 Protocol.export()

> **这一步在干什么？这是整个第三阶段的终点，也是最关键的一步。**
>
> 前面所有步骤都是准备工作：3.3 准备了材料（元数据、注册中心列表、协议列表），3.4 拼好了 URL，3.5 决定了暴露方式，3.6/3.7 做了 URL 的变形。到了 3.8，终于要**真正动手了**。
>
> **这步做了两件大事：**
>
> **第一件：把你的 `UserServiceImpl` 包装成 `Invoker`。**
>
> 问题是：Dubbo 框架下面那些层（Protocol、Exchange、Transport）不认识你的 `UserServiceImpl`，它们只认识一个统一的接口——`Invoker`。`Invoker` 就是一个"我能帮你调某个方法"的抽象。所以这里用 `proxyFactory.getInvoker()` 把你的实现类包装了一层。包装完之后，不管你的接口有多少方法、方法签名是什么，对框架来说都变成了统一的 `invoker.invoke(invocation)` 调用。
>
> 通俗比喻：你写了一个会做菜的厨师（UserServiceImpl），但餐厅的点单系统只认识标准化的"工位"接口（Invoker）。你需要让厨师坐到工位上，这样点单系统就能统一派单了。
>
> **第二件：把包装好的 Invoker 交给 Protocol 层去"暴露"。**
>
> `protocolSPI.export(invoker)` 这一行是**整个流程的分水岭**——从这里开始，控制权从 Config 层（配置编排）交给了 Protocol 层（网络协议处理）。Protocol 层会根据 URL 协议头做不同的事：
> - `registry://` → 走 `RegistryProtocol`：先委托底层协议开端口，再注册到注册中心
> - `dubbo://` → 走 `DubboProtocol`：启动 Netty Server，监听端口
> - `tri://` → 走 `TripleProtocol`：启动 HTTP/2 Server，监听端口
> - `injvm://` → 走 `InjvmProtocol`：什么网络都不开，直接在内存里注册
>
> **做完这步之后：** Config 层的活就全干完了。后面第四、五、六、七、八、九阶段都是 Protocol 层和更底下的层在干活。

```java
private void doExportUrl(URL url, boolean withMetaData, RegisterTypeEnum registerType) {
    // 注册类型处理...
    if (registerType == RegisterTypeEnum.NEVER_REGISTER || ...) {
        url = url.addParameter(REGISTER_KEY, false);
    }

    // ===== 第一件大事：创建 Invoker =====
    // proxyFactory 默认是 JavassistProxyFactory
    // ref 是你的服务实现类实例（如 UserServiceImpl）
    // interfaceClass 是服务接口（如 UserService.class）
    // url 是服务 URL
    Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, url);

    // 包装元数据
    if (withMetaData) {
        invoker = new DelegateProviderMetaDataInvoker(invoker, this);
    }

    // ===== 第二件大事：通过 Protocol SPI 暴露服务 =====
    // protocolSPI 是自适应扩展点，根据 URL 的 protocol 字段路由：
    //   registry:// -> RegistryProtocol.export()
    //   dubbo://   -> DubboProtocol.export()
    //   tri://     -> TripleProtocol.export()
    //   injvm://   -> InjvmProtocol.export()
    Exporter<?> exporter = protocolSPI.export(invoker);

    // 保存 exporter 引用
    exporters.computeIfAbsent(registerType, k -> new CopyOnWriteArrayList<>()).add(exporter);
}
```

---

## 第四阶段：ProxyFactory —— 将服务实现包装为 Invoker

### 4.1 为什么需要 Invoker？

Dubbo 的整个 RPC 框架都是围绕 `Invoker` 这个核心模型构建的。`Invoker` 是一个可执行体，代表"对某个接口的某次调用"。在 Provider 端，需要把你写的 `UserServiceImpl` 包装成一个 `Invoker`，这样上层的 Filter 链、Protocol 等组件就能统一处理。

### 4.2 JavassistProxyFactory.getInvoker()

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/proxy/javassist/JavassistProxyFactory.java`

```java
@Override
public <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) {
    try {
        // 1. 为服务实现类生成 Wrapper（字节码动态生成，避免反射）
        final Wrapper wrapper = Wrapper.getWrapper(
                proxy.getClass().getName().indexOf('$') < 0 ? proxy.getClass() : type);

        // 2. 创建 AbstractProxyInvoker，doInvoke 委托给 Wrapper
        return new AbstractProxyInvoker<T>(proxy, type, url) {
            @Override
            protected Object doInvoke(T proxy, String methodName,
                    Class<?>[] parameterTypes, Object[] arguments) throws Throwable {
                return wrapper.invokeMethod(proxy, methodName, parameterTypes, arguments);
            }
        };
    } catch (Throwable fromJavassist) {
        // Javassist 失败则降级为 JDK 反射方式
        return jdkProxyFactory.getInvoker(proxy, type, url);
    }
}
```

### 4.3 Wrapper —— 字节码生成避免反射

**源码位置**: `dubbo-common/src/main/java/org/apache/dubbo/common/bytecode/Wrapper.java`

`Wrapper.getWrapper(UserServiceImpl.class)` 会通过 Javassist 动态生成一个类，其 `invokeMethod()` 方法大致等价于：

```java
// 动态生成的伪代码（实际是字节码）
public Object invokeMethod(Object instance, String methodName,
        Class<?>[] paramTypes, Object[] args) throws InvocationTargetException {
    UserServiceImpl w;
    try {
        w = (UserServiceImpl) instance;
    } catch (Throwable e) {
        throw new IllegalArgumentException(e);
    }
    try {
        // 直接方法调用，不走反射!
        if ("getUserById".equals(methodName) && paramTypes.length == 1) {
            return w.getUserById((Long) args[0]);
        }
        if ("createUser".equals(methodName) && paramTypes.length == 1) {
            return w.createUser((UserDTO) args[0]);
        }
        // ... 其他方法
    } catch (Throwable e) {
        throw new InvocationTargetException(e);
    }
    throw new NoSuchMethodException("Not found method " + methodName);
}
```

**性能优势**：通过 if-else 直接调用目标方法，避免了 Java 反射的开销，性能接近原生方法调用。

### 4.4 AbstractProxyInvoker.invoke() —— RPC 调用的执行入口

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/proxy/AbstractProxyInvoker.java`

当网络层收到一个 RPC 请求后，最终会调用到这个 Invoker 的 `invoke()` 方法：

```java
@Override
public Result invoke(Invocation invocation) throws RpcException {
    try {
        // 调用子类的 doInvoke（即 Wrapper.invokeMethod）
        Object value = doInvoke(proxy, invocation.getMethodName(),
                invocation.getParameterTypes(), invocation.getArguments());

        // 处理异步返回值（CompletableFuture）
        CompletableFuture<Object> future = wrapWithFuture(value, invocation);

        // 包装为 AppResponse
        CompletableFuture<AppResponse> appResponseFuture = future.handle((obj, t) -> {
            AppResponse result = new AppResponse(invocation);
            if (t != null) {
                result.setException(t instanceof CompletionException ? t.getCause() : t);
            } else {
                result.setValue(obj);
            }
            return result;
        });

        return new AsyncRpcResult(appResponseFuture, invocation);
    } catch (InvocationTargetException e) {
        return AsyncRpcResult.newDefaultAsyncResult(null, e.getTargetException(), invocation);
    }
}
```

---

## 第五阶段：RegistryProtocol —— 注册中心协议暴露

### 5.1 什么时候走 RegistryProtocol？

当 `doExportUrl()` 传入的 URL 是 `registry://...` 协议时（即有注册中心的场景），Protocol SPI 自适应机制会路由到 `RegistryProtocol`。

### 5.2 RegistryProtocol.export()

**源码位置**: `dubbo-registry/dubbo-registry-api/src/main/java/org/apache/dubbo/registry/integration/RegistryProtocol.java`

```java
@Override
public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
    // 1. 从 Invoker 的 URL 中解析出注册中心地址和 Provider 地址
    URL registryUrl = getRegistryUrl(originInvoker);
    URL providerUrl = getProviderUrl(originInvoker);  // 从 EXPORT_KEY 属性中取出

    // 2. 订阅动态配置（override 规则）
    final URL overrideSubscribeUrl = getSubscribedOverrideUrl(providerUrl);
    final OverrideListener overrideSubscribeListener =
            new OverrideListener(overrideSubscribeUrl, originInvoker);

    // 3. 用动态配置覆盖 Provider URL（如配置中心推送了新的 timeout 值）
    providerUrl = overrideUrlWithConfig(providerUrl, overrideSubscribeListener);

    // ===== 4. 核心：执行本地暴露 -- 调用底层协议的 export =====
    final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker, providerUrl);

    // 5. 获取注册中心实例（如 NacosRegistry、ZookeeperRegistry）
    final Registry registry = getRegistry(registryUrl);

    // 6. 构建要注册到注册中心的 URL
    final URL registeredProviderUrl = customizeURL(providerUrl, registryUrl);

    // ===== 7. 向注册中心注册 =====
    boolean register = providerUrl.getParameter(REGISTER_KEY, true)
            && registryUrl.getParameter(REGISTER_KEY, true);
    if (register) {
        register(registry, registeredProviderUrl);
        // 例如：在 Nacos 中创建一个服务实例
        // 或在 Zookeeper 中创建一个临时节点
    }

    // 8. 订阅 override 规则（兼容 2.6.x）
    if (!registry.isServiceDiscovery()) {
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);
    }

    return new DestroyableExporter<>(exporter);
}
```

### 5.3 doLocalExport() —— 调用底层协议暴露

```java
private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker, URL providerUrl) {
    String providerUrlKey = getProviderUrlKey(originInvoker);

    // 创建 InvokerDelegate，将 URL 替换为真正的 provider URL
    Invoker<?> invokerDelegate = new InvokerDelegate<>(originInvoker, providerUrl);

    // 通过底层协议暴露（DubboProtocol 或 TripleProtocol）
    // 同一个 providerUrlKey 只会暴露一次（引用计数复用）
    ReferenceCountExporter<?> exporter =
            exporterFactory.createExporter(providerUrlKey, () -> protocol.export(invokerDelegate));

    return new ExporterChangeableWrapper<>(exporter, originInvoker);
}
```

**关键点**：`protocol.export(invokerDelegate)` 这里的 `protocol` 就是 `DubboProtocol` 或 `TripleProtocol`，因为 `providerUrl` 的协议是 `dubbo://` 或 `tri://`。

---

## 第六阶段：DubboProtocol —— Dubbo 协议暴露与 Server 启动

### 6.1 DubboProtocol.export()

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboProtocol.java`

```java
@Override
public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
    URL url = invoker.getUrl();

    // 1. 生成 serviceKey（如 "com.example.UserService:1.0.0:20880"）
    String key = serviceKey(url);

    // 2. 创建 DubboExporter 并放入 exporterMap
    //    后续收到请求时，通过 serviceKey 从 map 中找到对应的 Exporter/Invoker
    DubboExporter<T> exporter = new DubboExporter<>(invoker, key, exporterMap);

    // 3. 核心：打开服务端口
    openServer(url);

    // 4. 优化序列化
    optimizeSerialization(url);

    return exporter;
}
```

### 6.2 openServer() —— 确保同一地址只启动一个 Server

```java
private void openServer(URL url) {
    String key = url.getAddress();  // 如 "192.168.1.100:20880"
    boolean isServer = url.getParameter(IS_SERVER_KEY, true);
    if (isServer) {
        ProtocolServer server = serverMap.get(key);
        if (server == null) {
            synchronized (this) {
                server = serverMap.get(key);
                if (server == null) {
                    serverMap.put(key, createServer(url));  // 首次：创建新 Server
                    return;
                }
            }
        }
        server.reset(url);  // 非首次：复用已有 Server，只做 reset
    }
}
```

**设计要点**：用 `host:port` 做 key，双重检查锁保证同一地址只启动一个 Server。后续暴露到同一端口的服务直接复用。

### 6.3 createServer() —— 构建参数并进入 Exchange 层

```java
private ProtocolServer createServer(URL url) {
    url = URLBuilder.from(url)
            // 添加只读事件发送标记
            .addParameterIfAbsent(CHANNEL_READONLYEVENT_SENT_KEY, Boolean.TRUE.toString())
            // 添加心跳间隔（默认 60s）
            .addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT))
            // 设置编解码器为 DubboCodec
            .addParameter(CODEC_KEY, DubboCodec.NAME)
            .build();

    // 校验 transporter 扩展是否存在
    String transporter = url.getParameter(SERVER_KEY, DEFAULT_REMOTING_SERVER);
    if (StringUtils.isNotEmpty(transporter) && !url.getOrDefaultFrameworkModel()
            .getExtensionLoader(Transporter.class).hasExtension(transporter)) {
        throw new RpcException("Unsupported server type: " + transporter);
    }

    ExchangeServer server;
    try {
        // 核心：通过 Exchangers.bind 进入 Exchange 层
        server = Exchangers.bind(url, requestHandler);
    } catch (RemotingException e) {
        throw new RpcException("Fail to start server(url: " + url + ") " + e.getMessage(), e);
    }

    return new DefaultProtocolServer(server);
}
```

---

## 第七阶段：Exchange 层 —— 请求-响应语义封装

### 7.1 Exchangers.bind() —— Exchange 层入口

**源码位置**：`dubbo-remoting/dubbo-remoting-api/.../exchange/Exchangers.java`

```java
public static ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
    url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
    // 通过 SPI 获取 Exchanger 实现（默认为 "header"）
    return getExchanger(url).bind(url, handler);
}

public static Exchanger getExchanger(URL url) {
    String type = url.getParameter(Constants.EXCHANGER_KEY, Constants.DEFAULT_EXCHANGER);
    return url.getOrDefaultFrameworkModel()
            .getExtensionLoader(Exchanger.class)
            .getExtension(type);  // 默认加载 HeaderExchanger
}
```

### 7.2 HeaderExchanger.bind() —— 包装 Handler 链

**源码位置**：`dubbo-remoting/dubbo-remoting-api/.../exchange/support/header/HeaderExchanger.java`

```java
@Override
public ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
    // Handler 包装链：ExchangeHandler -> HeaderExchangeHandler -> DecodeHandler
    return new HeaderExchangeServer(
            Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler)))
    );
}
```

**Handler 包装链的作用**：

```
ExchangeHandler (DubboProtocol 的 requestHandler，处理业务逻辑)
    | 被包装为
HeaderExchangeHandler (处理请求-响应模型，匹配 Request/Response)
    | 被包装为
DecodeHandler (延迟解码，在业务线程中解码而非 IO 线程)
    | 传递给
Transport 层
```

---

## 第八阶段：Transport 层 —— 网络传输抽象

### 8.1 Transporters.bind() —— Transport 层入口

**源码位置**：`dubbo-remoting/dubbo-remoting-api/.../Transporters.java`

```java
public static RemotingServer bind(URL url, ChannelHandler... handlers) throws RemotingException {
    ChannelHandler handler;
    if (handlers.length == 1) {
        handler = handlers[0];
    } else {
        handler = new ChannelHandlerDispatcher(handlers);
    }
    // 通过 SPI @Adaptive 根据 URL 参数选择 Transporter 实现
    return getTransporter(url).bind(url, handler);
}

public static Transporter getTransporter(URL url) {
    return url.getOrDefaultFrameworkModel()
            .getExtensionLoader(Transporter.class)
            .getAdaptiveExtension();  // 默认为 netty -> NettyTransporter
}
```

### 8.2 NettyTransporter.bind() —— 创建 NettyServer

**源码位置**：`dubbo-remoting/dubbo-remoting-netty4/.../NettyTransporter.java`

```java
@Override
public RemotingServer bind(URL url, ChannelHandler handler) throws RemotingException {
    return new NettyServer(url, handler);  // 直接 new，构造函数中完成端口绑定
}
```

---

## 第九阶段：NettyServer —— 真正的端口绑定

### 9.1 AbstractServer 构造函数 —— 模板方法触发 doOpen()

**源码位置**：`dubbo-remoting/dubbo-remoting-api/.../transport/AbstractServer.java`

```java
public AbstractServer(URL url, ChannelHandler handler) throws RemotingException {
    super(url, handler);
    localAddress = getUrl().toInetSocketAddress();

    // 解析绑定地址
    String bindIp = getUrl().getParameter(Constants.BIND_IP_KEY, getUrl().getHost());
    int bindPort = getUrl().getParameter(Constants.BIND_PORT_KEY, getUrl().getPort());
    if (url.getParameter(ANYHOST_KEY, false) || NetUtils.isInvalidLocalHost(bindIp)) {
        bindIp = ANYHOST_VALUE;  // 0.0.0.0
    }
    bindAddress = new InetSocketAddress(bindIp, bindPort);
    this.accepts = url.getParameter(ACCEPTS_KEY, DEFAULT_ACCEPTS);

    try {
        doOpen();  // 模板方法! 子类实现真正的端口绑定
        logger.info("Start " + getClass().getSimpleName() + " bind " + getBindAddress());
    } catch (Throwable t) {
        throw new RemotingException(url.toInetSocketAddress(), null,
                "Failed to bind " + getClass().getSimpleName() + " on " + bindAddress, t);
    }

    // 创建业务线程池
    executors.add(executorRepository.createExecutorIfAbsent(
            ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME)));
}
```

### 9.2 NettyServer.doOpen() —— Netty Bootstrap 启动

**源码位置**：`dubbo-remoting/dubbo-remoting-netty4/.../NettyServer.java`

```java
@Override
protected void doOpen() throws Throwable {
    bootstrap = new ServerBootstrap();

    // 创建 Boss 线程组（1个线程，负责 accept 连接）
    bossGroup = NettyEventLoopFactory.eventLoopGroup(1, EVENT_LOOP_BOSS_POOL_NAME);
    // 创建 Worker 线程组（默认 CPU 核心数+1，负责 IO 读写）
    workerGroup = NettyEventLoopFactory.eventLoopGroup(
            getUrl().getPositiveParameter(IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS),
            EVENT_LOOP_WORKER_POOL_NAME);

    final NettyServerHandler nettyServerHandler = new NettyServerHandler(getUrl(), this);
    channels = nettyServerHandler.getChannels();

    bootstrap.group(bossGroup, workerGroup)
            .channel(NettyEventLoopFactory.serverSocketChannelClass())  // NioServerSocketChannel
            .option(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
            .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .childOption(ChannelOption.SO_KEEPALIVE, keepalive)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    // Netty Pipeline 配置
                    ch.pipeline()
                        .addLast("negotiation", new SslServerTlsHandler(getUrl()))  // SSL/TLS
                        .addLast("decoder", adapter.getDecoder())    // Dubbo 协议解码器
                        .addLast("encoder", adapter.getEncoder())    // Dubbo 协议编码器
                        .addLast("server-idle-handler",              // 空闲检测
                                new IdleStateHandler(0, 0, closeTimeout, MILLISECONDS))
                        .addLast("handler", nettyServerHandler);    // 业务处理器
                }
            });

    // ========== 真正的端口绑定! ==========
    ChannelFuture channelFuture = bootstrap.bind(getBindAddress());
    channelFuture.syncUninterruptibly();  // 同步等待绑定完成
    channel = channelFuture.channel();
}
```

**至此，Netty Server 启动完成，端口开始监听，可以接收消费者的连接请求了。**

---

## 第十阶段（并行）：注册中心注册

回到 `RegistryProtocol.export()` 方法，在 `doLocalExport()` 完成本地端口绑定后，紧接着执行注册中心注册：

```java
// 获取注册中心实例（如 NacosRegistry、ZookeeperRegistry）
final Registry registry = getRegistry(registryUrl);

// 定制注册 URL（简化参数）
final URL registeredProviderUrl = customizeURL(providerUrl, registryUrl);

// 向注册中心注册
boolean register = providerUrl.getParameter(REGISTER_KEY, true)
        && registryUrl.getParameter(REGISTER_KEY, true);
if (register) {
    register(registry, registeredProviderUrl);
}
```

注册的本质就是将 Provider 的 URL 信息写入注册中心（如 Zookeeper 的节点、Nacos 的服务实例），消费者通过订阅这些信息来发现服务。

---

## Triple 协议的差异路径

Triple 协议（基于 HTTP/2）的暴露路径与 Dubbo 协议有所不同：

### TripleProtocol.export()

**源码位置**: `dubbo-rpc/dubbo-rpc-triple/src/main/java/org/apache/dubbo/rpc/protocol/tri/TripleProtocol.java`

```java
@Override
public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
    URL url = invoker.getUrl();
    String key = serviceKey(url);

    // 1. 创建 Exporter
    AbstractExporter<T> exporter = new AbstractExporter<T>(invoker) { ... };
    exporterMap.put(key, exporter);

    // 2. 注册 gRPC 路径映射（如 /com.example.UserService/getUserById）
    pathResolver.register(invoker);

    // 3. 注册 REST 路径映射（如果启用了 REST）
    if (REST_ENABLED) {
        mappingRegistry.register(invoker);
    }

    // 4. 设置健康检查状态为 SERVING
    setServiceStatus(url, true);

    // 5. 创建业务线程池
    ExecutorRepository.getInstance(url.getOrDefaultApplicationModel())
            .createExecutorIfAbsent(ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME));

    // 6. 绑定端口（通过 PortUnificationExchanger）
    bindServerPort(url);

    return exporter;
}
```

### TripleProtocol.bindServerPort()

```java
private void bindServerPort(URL url) {
    boolean bindPort = true;
    // 检查是否在 Servlet 容器中运行
    if (ServletExchanger.isEnabled()) { ... }

    if (bindPort) {
        String addr = url.getAddress();
        ConcurrentHashMapUtils.computeIfAbsent(serverMap, addr, k -> {
            // 通过端口复用交换器绑定
            RemotingServer remotingServer = PortUnificationExchanger.bind(url, new DefaultPuHandler());
            return new DefaultProtocolServer(remotingServer);
        });
    }

    // 同时绑定 HTTP/3 (QUIC)
    Http3Exchanger.bind(url);
}
```

Triple 使用 `NettyPortUnificationServer`，它的 Pipeline 中有一个协议探测器（`PortUnificationHandler`），可以在同一端口上同时处理 HTTP/2、Dubbo 协议等多种协议。

---

## Server 类层次结构

```
Endpoint (接口)
  +-- RemotingServer (接口) - extends Endpoint
        +-- ExchangeServer (接口) - extends RemotingServer
        |     +-- HeaderExchangeServer (实现) - 装饰器，包装 RemotingServer
        +-- AbstractServer (抽象类) - extends AbstractEndpoint implements RemotingServer
              +-- NettyServer - 传统 Dubbo 协议的 Netty 服务端
              +-- AbstractPortUnificationServer - 端口复用服务端
                    +-- NettyPortUnificationServer - Triple 协议的 Netty 服务端
```

---

## Dubbo 协议 vs Triple 协议对比

| 维度 | Dubbo 协议 | Triple 协议 |
|------|-----------|-------------|
| 入口 | `DubboProtocol.export()` -> `openServer()` | `TripleProtocol.export()` -> `bindServerPort()` |
| Exchange层 | `Exchangers.bind()` -> `HeaderExchanger.bind()` | 不走 Exchanger，直接 `PortUnificationExchanger.bind()` |
| Transport层 | `Transporters.bind()` -> `NettyTransporter.bind()` | `PortUnificationTransporter.bind()` |
| Server实现 | `NettyServer` (固定编解码，单协议) | `NettyPortUnificationServer` (协议探测，多协议复用) |
| Netty Pipeline | SSL -> Decoder -> Encoder -> IdleState -> ServerHandler | ChannelHandler -> PortUnificationHandler (动态协议检测) |
| 端口复用 | 不支持（每个协议独立端口） | 支持（同一端口可同时处理 HTTP/2、Dubbo 等多协议） |
| 共同点 | 最终都调用 `bootstrap.bind(address).syncUninterruptibly()` 完成 Netty 端口绑定 |

---

## 关键设计思想总结

### 1. 分层架构

整个服务暴露过程严格分层：

| 层次 | 职责 | 核心类 |
|------|------|--------|
| Config 层 | 配置解析、流程编排 | ServiceConfig, DefaultModuleDeployer |
| Proxy 层 | 将实现类包装为 Invoker | JavassistProxyFactory, Wrapper |
| Protocol 层 | 协议暴露、注册中心交互 | RegistryProtocol, DubboProtocol, TripleProtocol |
| Exchange 层 | 请求-响应语义封装 | HeaderExchanger, HeaderExchangeServer |
| Transport 层 | 网络传输抽象 | Transporters, NettyTransporter |
| Netty 层 | 真正的网络 IO | NettyServer, ServerBootstrap |

### 2. SPI 自适应扩展

每一层都通过 SPI 机制实现可替换：
- `ProxyFactory` SPI: javassist / jdk
- `Protocol` SPI: registry / dubbo / tri / injvm
- `Exchanger` SPI: header
- `Transporter` SPI: netty

### 3. 模板方法模式

`AbstractServer` 的构造函数中调用 `doOpen()` 是典型的模板方法模式，子类（NettyServer、NettyPortUnificationServer）只需实现 `doOpen()` 即可。

### 4. 装饰器模式

Handler 链使用装饰器模式层层包装：
```
业务 Handler -> HeaderExchangeHandler -> DecodeHandler -> NettyServerHandler
```

### 5. 端口复用

同一地址只启动一个 Server（通过 `serverMap` 缓存），多个服务暴露到同一端口时复用已有 Server，通过 `serviceKey` 区分不同服务。

### 6. 双重暴露

默认情况下，服务同时进行本地暴露（injvm，同 JVM 内调用走内存，不走网络）和远程暴露（走网络），通过 `scope` 参数控制。

---

## 一句话串联全流程

> Spring 扫描到你的 `@DubboService` 注解类，注册为 `ServiceBean`；容器刷新完成后触发 `DefaultModuleDeployer.start()`；`ServiceConfig` 将你的服务实现通过 `JavassistProxyFactory` 包装为 `Invoker`（字节码生成避免反射）；然后通过 `RegistryProtocol` 先调用底层协议（`DubboProtocol`/`TripleProtocol`）的 `export()` 启动 Netty Server 监听端口，再向注册中心注册服务地址；至此，你的服务就对外可用了。

---

## 第十一阶段：Provider 接收请求并处理返回 —— 从字节流到业务方法调用

> 前面十个阶段讲的都是"服务怎么暴露出去"。现在服务已经暴露了，Netty Server 在监听端口。当消费者发起一次 RPC 调用，Provider 端收到 TCP 字节流后，经历了怎样的旅程才最终调用到你的 `UserServiceImpl.getUserById()` 并把结果返回？下面逐层拆解。

### 11.1 全局请求处理链路总览

```
Consumer 发送请求（TCP 字节流）
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  Netty IO 线程                                                    │
│                                                                   │
│  1. DubboCodec.decode()                                          │
│     → 解析 16 字节 Header + Body                                  │
│     → 得到 Request 对象（含 RpcInvocation：方法名、参数）          │
│                                                                   │
│  2. NettyServerHandler.channelRead(ctx, msg)                     │
│     → 触发 Dubbo Handler 链                                       │
│                                                                   │
│  3. AllChannelHandler.received(channel, message)                 │
│     → 将消息 dispatch 到业务线程池（保护 IO 线程）                 │
└────────────────────────────────┬──────────────────────────────────┘
                                 │ 线程切换
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  业务线程池                                                        │
│                                                                   │
│  4. DecodeHandler.received()                                     │
│     → 延迟解码（在业务线程中完成反序列化，避免阻塞 IO 线程）       │
│                                                                   │
│  5. HeaderExchangeHandler.received()                             │
│     → 识别消息类型（Request/Response/心跳）                        │
│     → 调用 handleRequest()                                        │
│                                                                   │
│  6. HeaderExchangeHandler.handleRequest()                        │
│     → 调用 DubboProtocol.requestHandler.reply()                  │
│     → 拿到异步 Future                                             │
│     → Future 完成后将 Response 写回 Channel                       │
│                                                                   │
│  7. DubboProtocol.requestHandler.reply()                         │
│     → 通过 serviceKey 从 exporterMap 找到 Exporter                │
│     → 拿到 Invoker                                                │
│     → 经过 Filter 链                                              │
│     → 调用 AbstractProxyInvoker.invoke()                         │
│       → Wrapper.invokeMethod()                                   │
│         → UserServiceImpl.getUserById(1)  ← 你的业务代码!          │
│                                                                   │
│  8. 结果返回                                                      │
│     → AppResponse（包含返回值或异常）                              │
│     → 封装为 Response 对象（带上 requestId）                       │
│     → DubboCodec.encode() 编码为字节流                            │
│     → Netty Channel.writeAndFlush() 发送回 Consumer               │
└───────────────────────────────────────────────────────────────────┘
```

### 11.2 DubboCodec.decode() —— 协议解码

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboCodec.java`

> **这一步在干什么？**
>
> Netty 收到的是原始字节流。DubboCodec 负责按照 Dubbo 协议的帧格式把字节流切割、解析成一个 `Request` 对象。回忆一下 Dubbo 协议帧格式（16 字节 Header + Body）：
>
> ```
> ┌──────────────────────────────────────────────────────────────┐
> │  0-1:  Magic Number (0xdabb) —— 魔数，标识这是 Dubbo 协议     │
> │  2:    Flag (请求/响应、单向/双向、心跳、序列化方式)             │
> │  3:    Status (仅响应有效)                                     │
> │  4-11: Request ID (8字节 long)                                 │
> │  12-15: Body Length (4字节 int)                                │
> │  16-...: Body (序列化后的 RpcInvocation)                       │
> └──────────────────────────────────────────────────────────────┘
> ```
>
> Header 解析在 IO 线程完成（很快，就是读几个字节）。Body 的反序列化（把字节变成 Java 对象）可以延迟到业务线程做（通过 DecodeHandler），避免阻塞 IO 线程。

```java
@Override
protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
    byte flag = header[2];
    byte proto = (byte) (flag & SERIALIZATION_MASK);  // 序列化方式（Hessian2/Fastjson等）

    // 获取 requestId
    long id = Bytes.bytes2long(header, 4);

    // 判断是请求还是响应
    if ((flag & FLAG_REQUEST) != 0) {
        // ===== 这是一个请求 =====
        Request req = new Request(id);
        req.setVersion(Version.getProtocolVersion());
        req.setTwoWay((flag & FLAG_TWOWAY) != 0);

        if ((flag & FLAG_EVENT) != 0) {
            req.setEvent(true);  // 心跳或事件
        }

        // 解码 Body → RpcInvocation（方法名、参数类型、参数值）
        ObjectInput in = CodecSupport.getSerialization(url, proto)
                .deserialize(url, is);
        // 延迟解码模式下只是包装一层，真正的反序列化在业务线程做
        DecodeableRpcInvocation inv = new DecodeableRpcInvocation(
                channel, req, is, proto);
        inv.decode();  // 或延迟到 DecodeHandler 中 decode

        req.setData(inv);
        return req;
    } else {
        // 这是一个响应（Provider 端通常不会收到响应，这里不展开）
        // ...
    }
}
```

**RpcInvocation 解码后包含的信息：**

```java
public class RpcInvocation {
    private String methodName;           // "getUserById"
    private Class<?>[] parameterTypes;   // [Long.class]
    private Object[] arguments;          // [1L]
    private String serviceName;          // "com.example.UserService"
    private String serviceVersion;       // "1.0.0"
    private Map<String, Object> attachments;  // 附加参数（timeout、token等）
}
```

### 11.3 NettyServerHandler.channelRead() —— Netty 入站处理

**源码位置**: `dubbo-remoting/dubbo-remoting-netty4/src/main/java/org/apache/dubbo/remoting/transport/netty4/NettyServerHandler.java`

> Netty Pipeline 中最后一个 Handler 是 `NettyServerHandler`，它负责把 Netty 的事件桥接到 Dubbo 的 Handler 链。

```java
@Override
public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), url, handler);
    // 触发 Dubbo Handler 链的 received 方法
    handler.received(channel, msg);
}
```

这里的 `handler` 就是在服务暴露时层层包装的 Handler 链。调用链如下：

```
NettyServerHandler.channelRead()
  → MultiMessageHandler.received()      // 处理批量消息
    → HeartbeatHandler.received()        // 过滤心跳消息
      → AllChannelHandler.received()     // 线程派发!
```

### 11.4 AllChannelHandler.received() —— IO 线程到业务线程的切换

**源码位置**: `dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/transport/dispatcher/all/AllChannelHandler.java`

> **这一步在干什么？**
>
> Netty 的 IO 线程（EventLoop）非常宝贵——一个 IO 线程管多个连接的读写。如果在 IO 线程上做耗时的业务处理（反序列化参数、调用业务方法、序列化响应），就会阻塞这个线程管的所有连接。所以这里必须切线程：把消息扔到业务线程池，IO 线程立刻回去继续处理网络 IO。

```java
@Override
public void received(Channel channel, Object message) throws RemotingException {
    // 获取业务线程池
    ExecutorService executor = getPreferredExecutorService(message);
    try {
        // 将消息封装成 Runnable 提交到业务线程池
        executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.RECEIVED, message));
    } catch (Throwable t) {
        // 线程池满时的处理（根据策略：丢弃/抛异常/调用方执行）
        if (message instanceof Request && t instanceof RejectedExecutionException) {
            sendFeedback(channel, (Request) message, t);
            return;
        }
        throw new ExecutionException(message, channel, getClass() + " error when process received event.", t);
    }
}
```

**线程池满时的降级处理：** 如果业务线程池满了（`RejectedExecutionException`），Dubbo 不会默默丢弃请求，而是构造一个"服务端线程池满"的错误 Response 返回给消费者，让消费者知道怎么回事（可以重试其他 Provider）。

```java
private void sendFeedback(Channel channel, Request request, Throwable t) {
    if (request.isTwoWay()) {
        Response response = new Response(request.getId(), request.getVersion());
        response.setStatus(Response.SERVER_THREADPOOL_EXHAUSTED_ERROR);
        response.setErrorMessage("Server side thread pool is exhausted...");
        channel.send(response);
    }
}
```

### 11.5 DecodeHandler.received() —— 延迟解码

**源码位置**: `dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/transport/DecodeHandler.java`

> **这一步在干什么？**
>
> 前面 DubboCodec 解码时，如果配置了延迟解码（`decode.in.io=false`，默认行为），Body 的反序列化不会在 IO 线程做，而是留到业务线程里做。DecodeHandler 就是在业务线程中完成这个"延迟的反序列化"。

```java
@Override
public void received(Channel channel, Object message) throws RemotingException {
    if (message instanceof Decodeable) {
        decode(message);  // 在业务线程中完成反序列化
    }
    if (message instanceof Request) {
        decode(((Request) message).getData());  // 反序列化 RpcInvocation 的参数
    }
    if (message instanceof Response) {
        decode(((Response) message).getResult());
    }
    handler.received(channel, message);  // 继续传递
}

private void decode(Object message) {
    if (message instanceof Decodeable) {
        try {
            ((Decodeable) message).decode();
            // 此时 RpcInvocation 里的 arguments 才真正变成 Java 对象
            // 比如 arguments[0] 从字节变成了 Long(1)
        } catch (Throwable e) {
            logger.warn("Decode message failed: " + e.getMessage(), e);
        }
    }
}
```

### 11.6 HeaderExchangeHandler.received() —— 识别消息类型

**源码位置**: `dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/exchange/support/header/HeaderExchangeHandler.java`

> **这一步在干什么？**
>
> TCP 连接上跑的消息有好几种：普通 RPC 请求、心跳请求、事件消息等。`HeaderExchangeHandler` 负责识别消息类型并分发处理。对于普通的 RPC 请求，它调用 `handleRequest()` 处理。

```java
@Override
public void received(Channel channel, Object message) throws RemotingException {
    final ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);

    if (message instanceof Request) {
        Request request = (Request) message;

        if (request.isEvent()) {
            // 事件消息（如只读事件）
            handlerEvent(channel, request);
        } else if (request.isTwoWay()) {
            // ===== 双向请求（需要返回响应）—— 绝大多数 RPC 调用走这里 =====
            handleRequest(exchangeChannel, request);
        } else {
            // 单向请求（不需要响应）
            handler.received(exchangeChannel, request.getData());
        }
    } else if (message instanceof Response) {
        handleResponse(channel, (Response) message);
    } else {
        handler.received(exchangeChannel, message);
    }
}
```

### 11.7 HeaderExchangeHandler.handleRequest() —— 调用业务逻辑并写回响应

> **这是请求处理的核心枢纽。** 它做三件事：
> 1. 调用上层 handler 的 `reply()` 方法，获得业务处理结果（异步 Future）
> 2. 当 Future 完成后，将结果封装为 Response
> 3. 通过 Channel 把 Response 写回给消费者

```java
void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
    // 1. 构造 Response（带上相同的 requestId，消费者靠这个匹配）
    Response res = new Response(req.getId(), req.getVersion());

    // 2. 检查请求合法性
    if (req.isBroken()) {
        // 请求解码就出错了（比如反序列化失败）
        res.setStatus(Response.BAD_REQUEST);
        res.setErrorMessage("Fail to decode request due to: " + req.getData());
        channel.send(res);
        return;
    }

    // 3. 取出 RpcInvocation
    Object msg = req.getData();

    try {
        // ===== 4. 核心：调用业务逻辑 =====
        // handler 就是 DubboProtocol 的 requestHandler
        // reply() 返回的是 CompletableFuture<Object>
        CompletableFuture<Object> future = handler.reply(channel, msg);

        // 5. 等 Future 完成后写回响应
        future.whenComplete((appResult, t) -> {
            try {
                if (t == null) {
                    // 正常完成
                    res.setStatus(Response.OK);
                    res.setResult(appResult);
                } else {
                    // 业务处理异常
                    res.setStatus(Response.SERVICE_ERROR);
                    res.setErrorMessage(StringUtils.toString(t));
                }
                // ===== 6. 写回 Response! =====
                channel.send(res);
            } catch (RemotingException e) {
                logger.warn("Send result to consumer failed for request: " + req + ", response: " + res);
            }
        });
    } catch (Throwable e) {
        // handler.reply() 本身就抛异常了
        res.setStatus(Response.SERVICE_ERROR);
        res.setErrorMessage(StringUtils.toString(e));
        channel.send(res);
    }
}
```

**关键点**：Response 里带的 `req.getId()` 就是那个 requestId——消费者收到 Response 后，靠这个 ID 从 `FUTURES` map 里找到对应的 `DefaultFuture` 并 complete。

### 11.8 DubboProtocol.requestHandler.reply() —— 找到 Invoker 并调用

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboProtocol.java`

> **这一步在干什么？**
>
> 请求到了这里，终于要找到你的 `UserServiceImpl` 并调用了。怎么找？还记得暴露阶段 `DubboProtocol.export()` 里把 `DubboExporter` 以 `serviceKey` 为 key 存进了 `exporterMap` 吗？现在就靠这个 key 去查：

```java
private ExchangeHandler requestHandler = new ExchangeHandlerAdapter() {
    @Override
    public CompletableFuture<Object> reply(ExchangeChannel channel, Object message) throws RemotingException {
        if (!(message instanceof Invocation)) {
            throw new RemotingException(channel, "Unsupported request: " + message);
        }
        Invocation inv = (Invocation) message;

        // ===== 1. 通过 serviceKey 找到对应的 Invoker =====
        // serviceKey 格式："group/com.example.UserService:version:port"
        // 这个 key 是从 RpcInvocation 的 attachments 中取的
        Invoker<?> invoker = getInvoker(channel, inv);

        // 2. 设置远端地址到上下文
        RpcContext.getServiceContext().setRemoteAddress(channel.getRemoteAddress());

        // ===== 3. 调用 Invoker 链! =====
        Result result = invoker.invoke(inv);

        // 4. 返回异步结果
        return result.thenApply(Function.identity());
    }
};
```

### 11.9 getInvoker() —— 从 exporterMap 查找

```java
Invoker<?> getInvoker(Channel channel, Invocation inv) throws RemotingException {
    int port = channel.getLocalAddress().getPort();

    // 构建 serviceKey
    String serviceKey = serviceKey(
            port,
            inv.getObjectAttachments().get(PATH_KEY).toString(),      // 接口路径
            inv.getObjectAttachments().get(VERSION_KEY).toString(),    // 版本号
            inv.getObjectAttachments().get(GROUP_KEY) != null ?
                    inv.getObjectAttachments().get(GROUP_KEY).toString() : null  // 分组
    );

    // 从 exporterMap 中查找
    DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);
    if (exporter == null) {
        throw new RemotingException(channel,
                "Not found exported service: " + serviceKey + " in " + exporterMap.keySet());
    }

    return exporter.getInvoker();
}
```

**为什么能找到？** 因为在暴露阶段（第六阶段 6.1），`DubboProtocol.export()` 已经把 `DubboExporter` 存入了这个 map：

```java
// 暴露时存入
DubboExporter<T> exporter = new DubboExporter<>(invoker, key, exporterMap);
// 请求时取出
DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);
```

### 11.10 Filter 链 —— 请求到达业务代码前的切面处理

> **这一步在干什么？**
>
> 拿到 Invoker 之后，调用 `invoker.invoke(inv)` 并不是直接到你的业务代码。中间还有一层 Filter 链（通过 Dubbo SPI 的 Wrapper 机制自动包装）。Provider 端常见的 Filter 包括：

```
请求进入
  → EchoFilter            // Echo 测试（消费者发 $echo 直接返回，不走业务）
    → ClassLoaderFilter   // 切换 ClassLoader 上下文
      → GenericFilter     // 泛化调用支持
        → ContextFilter   // 设置 RpcContext（远端IP、attachments等）
          → TimeoutFilter // 超时检测（如果请求在网络传输中已经超时了，直接返回）
            → ExceptionFilter  // 异常包装（把非声明异常包装为 RuntimeException）
              → AbstractProxyInvoker.invoke()  ← 真正调用业务代码
```

以 `ContextFilter` 为例：

```java
@Activate(group = PROVIDER)
public class ContextFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 把消费者传来的 attachments 设置到 RpcContext
        Map<String, Object> attachments = invocation.getObjectAttachments();
        RpcContext context = RpcContext.getServiceContext();
        context.setInvoker(invoker);
        context.setInvocation(invocation);
        context.setLocalAddress(invoker.getUrl().toInetSocketAddress());
        context.setRemoteAddress(
                (InetSocketAddress) invocation.get(REMOTE_ADDRESS_KEY));

        // 继续调用链
        return invoker.invoke(invocation);
    }
}
```

以 `TimeoutFilter` 为例（Provider 端的超时短路）：

```java
@Activate(group = PROVIDER)
public class TimeoutFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 检查请求是否已经在传输中超时了
        long timeout = Long.parseLong(
                invocation.getObjectAttachment(TIMEOUT_KEY, "0").toString());
        long startTime = Long.parseLong(
                invocation.getObjectAttachment(TIMEOUT_COUNTDOWN_KEY, "0").toString());

        if (timeout > 0 && startTime > 0) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > timeout) {
                // 请求已超时，消费者那边 Future 已经超时完成了
                // 即使执行了结果也没人要，直接返回，节省 Provider 资源
                logger.warn("invoke timeout, discard request...");
                return AsyncRpcResult.newDefaultAsyncResult(
                        new RpcException("Provider side timeout"), invocation);
            }
        }
        return invoker.invoke(invocation);
    }
}
```

### 11.11 AbstractProxyInvoker.invoke() —— 调用你的业务代码

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/proxy/AbstractProxyInvoker.java`

> **终于到了！** Filter 链走完之后，调用到达 `AbstractProxyInvoker`——这就是暴露阶段（第四阶段）用 `proxyFactory.getInvoker()` 创建的那个 Invoker。它内部持有你的 `UserServiceImpl` 实例和 Javassist 生成的 `Wrapper`。

```java
@Override
public Result invoke(Invocation invocation) throws RpcException {
    try {
        // ===== 调用你的业务代码! =====
        // doInvoke 内部是 wrapper.invokeMethod(proxy, methodName, parameterTypes, arguments)
        // 即：UserServiceImpl.getUserById(1L)
        Object value = doInvoke(proxy, invocation.getMethodName(),
                invocation.getParameterTypes(), invocation.getArguments());

        // 处理异步返回值
        CompletableFuture<Object> future = wrapWithFuture(value, invocation);

        // 封装为 AppResponse
        CompletableFuture<AppResponse> appResponseFuture = future.handle((obj, t) -> {
            AppResponse result = new AppResponse(invocation);
            if (t != null) {
                // 业务方法抛了异常
                if (t instanceof CompletionException) {
                    t = t.getCause();
                }
                result.setException(t);
            } else {
                // 正常返回
                result.setValue(obj);
            }
            return result;
        });

        return new AsyncRpcResult(appResponseFuture, invocation);
    } catch (InvocationTargetException e) {
        // 反射/字节码调用抛异常（InvocationTargetException 包装的是真正的业务异常）
        return AsyncRpcResult.newDefaultAsyncResult(null, e.getTargetException(), invocation);
    }
}
```

`doInvoke()` 内部就是 Wrapper 的字节码直接调用：

```java
// Wrapper 生成的字节码（伪代码）
Object invokeMethod(Object instance, String methodName, Class<?>[] types, Object[] args) {
    UserServiceImpl impl = (UserServiceImpl) instance;
    if ("getUserById".equals(methodName)) {
        return impl.getUserById((Long) args[0]);  // ← 直接调用，没有反射!
    }
    // ...
}
```

### 11.12 响应编码与发送 —— 从 Result 到字节流

> 业务方法执行完毕，返回值被封装成 `AppResponse`，通过 `AsyncRpcResult` 的 Future 传回到 `HeaderExchangeHandler.handleRequest()` 里的 `whenComplete` 回调。回调里把 `AppResponse` 塞进 `Response` 对象，然后调用 `channel.send(res)` 写回消费者。

```
AppResponse（value=User对象 或 exception=业务异常）
  │
  ▼
Response 对象
  ├── id = requestId（和请求相同，消费者靠它匹配 Future）
  ├── status = Response.OK（或 SERVICE_ERROR）
  └── result = AppResponse
  │
  ▼
channel.send(response)
  → NettyChannel.send(message)
    → NioSocketChannel.writeAndFlush(message)
      → Netty Pipeline 出站
        → DubboCodec.encode()
```

**DubboCodec.encode() 编码响应：**

```java
@Override
protected void encodeResponse(Channel channel, OutputStream os, Response res) throws IOException {
    // 1. 写 Header（16字节）
    byte[] header = new byte[HEADER_LENGTH];
    // Magic Number
    Bytes.short2bytes(MAGIC, header);
    // Flag: 标记为响应
    header[2] = res.isHeartbeat() ? (byte) (FLAG_EVENT | serialization.getContentTypeId())
            : serialization.getContentTypeId();
    // Status
    header[3] = res.getStatus();
    // Request ID（和请求相同!）
    Bytes.long2bytes(res.getId(), header, 4);

    // 2. 写 Body（序列化 AppResponse）
    ObjectOutput out = serialization.serialize(url, os);
    if (res.getStatus() == Response.OK) {
        if (res.isHeartbeat()) {
            out.writeObject(res.getResult());  // 心跳响应
        } else {
            // 序列化业务返回值
            AppResponse appResponse = (AppResponse) res.getResult();
            if (appResponse.hasException()) {
                out.writeObject(appResponse.getException());
            } else {
                out.writeObject(appResponse.getValue());  // 序列化 User 对象
            }
        }
    } else {
        out.writeUTF(res.getErrorMessage());  // 错误信息
    }

    // 3. 回填 Body Length 到 Header
    int bodyLength = out.getBufferSize();
    Bytes.int2bytes(bodyLength, header, 12);

    // 4. 先写 Header 再写 Body
    os.write(header);
    out.flushBuffer();
}
```

### 11.13 响应到达消费者 —— 闭环

响应字节流通过 TCP 发送到消费者端。消费者端的处理链路（详见消费者文档）：

```
TCP 字节流到达 Consumer
  → DubboCodec.decode() 解码为 Response 对象
    → NettyClientHandler.channelRead()
      → AllChannelHandler.received() → 切到业务线程
        → HeaderExchangeHandler.handleResponse()
          → DefaultFuture.received(channel, response)
            → FUTURES.remove(response.getId())  // 通过 requestId 找到 Future
            → future.complete(response.getResult())
              → CompletableFuture 完成
                → AsyncRpcResult.recreate()
                  → 返回 User 对象给调用者
```

至此，一次完整的 RPC 调用闭环完成。

---

## 11.14 关键设计总结：请求处理阶段

### 线程模型

```
IO 线程（Netty EventLoop）           业务线程池
     │                                    │
     │  1. 读取 TCP 字节流                 │
     │  2. Header 解码                     │
     │  3. 触发 Handler 链                 │
     │                                    │
     │── AllChannelHandler.received() ──→ │
     │   （dispatch 到业务线程池）          │
     │                                    │  4. Body 反序列化（延迟解码）
     │  返回继续处理其他连接的 IO           │  5. 查找 Invoker
     │                                    │  6. Filter 链
     │                                    │  7. 调用业务方法
     │                                    │  8. 封装 Response
     │                                    │
     │←── channel.send(response) ────────│
     │                                    │
     │  9. 编码 Response                   │
     │  10. TCP 发送                       │
```

### exporterMap —— 请求路由的核心

```java
// 暴露时：存入
exporterMap.put("com.example.UserService:1.0.0:20880", exporter);

// 请求时：取出
Invoker<?> invoker = exporterMap.get(serviceKey).getInvoker();
```

同一个端口可以暴露多个服务，靠 `serviceKey` 区分。这就是为什么 DubboProtocol 用 `host:port` 做 key 只启动一个 Server，但可以服务多个接口。

### 异步全链路

从 `invoker.invoke()` 返回 `AsyncRpcResult`（包含 CompletableFuture），到 `handleRequest()` 的 `whenComplete()` 回调写 Response——整个流程都是异步的。如果你的业务方法返回 `CompletableFuture<User>`，Dubbo 会直接把这个 Future 接上去，业务线程不会阻塞。只有同步方法才会在业务线程中阻塞等待方法执行完毕。

### Provider 端超时保护

`TimeoutFilter` 在调用业务方法之前会检查：请求从消费者发出到现在已经过了多久？如果已经超过了 timeout，说明消费者那边的 Future 已经超时完成了（抛了 TimeoutException），即使 Provider 执行了结果也没人要。这时 Provider 直接丢弃请求，节省线程资源。

### 线程池满的优雅降级

当业务线程池满了（`RejectedExecutionException`），Provider 不会默默丢弃请求，而是在 IO 线程上直接构造一个 `SERVER_THREADPOOL_EXHAUSTED_ERROR` 的 Response 返回给消费者。消费者收到后会重试其他 Provider（如果用的是 FailoverCluster）。
