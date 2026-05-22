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

### 3.4 doExportUrlsFor1Protocol() —— 构建服务 URL

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

**默认行为**：scope 为空时，**同时做本地暴露和远程暴露**。

### 3.6 exportLocal() —— 本地暴露（JVM 内部调用优化）

本地暴露的目的是：当同一个 JVM 内既有 Provider 又有 Consumer 时，调用走 JVM 内部直接调用，不走网络。

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

**关键设计**：有注册中心时，传给 `doExportUrl` 的 URL 是 `registry://...` 协议的，真正的 provider URL（如 `dubbo://...`）被放在了 `EXPORT_KEY` 属性里。这样 Protocol SPI 自适应机制就会路由到 `RegistryProtocol`。

### 3.8 doExportUrl() —— 创建 Invoker + 调用 Protocol.export()

**这是整个暴露流程中最关键的一步**，做了两件大事：

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
