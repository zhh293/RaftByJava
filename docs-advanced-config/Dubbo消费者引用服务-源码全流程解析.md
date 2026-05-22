# Dubbo 消费者引用服务 —— 源码全流程解析

> 基于源码项目 `/Users/zhanghonghao/Desktop/dubbo` 逐步分析，从 Spring 启动扫描 `@DubboReference` 注解，到代理注入、服务发现、RPC 调用、网络传输、响应返回，不跳步、不省略。

---

## 全局调用链总览

先给你一张完整的调用链路图，后面逐步展开每一层：

```
Spring 容器启动
  |
  +-- 1. ReferenceAnnotationBeanPostProcessor 扫描 @DubboReference 注解字段
  |     -> 为每个引用注册一个 ReferenceBean 的 BeanDefinition
  |
  +-- 2. ReferenceBean 作为 FactoryBean 被 Spring 实例化
  |     -> getObject() 返回一个懒代理（LazyProxy）
  |     -> 此时不会建立网络连接!
  |
  +-- 3. 懒代理被注入到你的 @Component 字段中
  |     -> 此时你的 Controller/Service 拿到的是一个代理对象
  |
  +-- 4. 第一次调用代理方法时触发真正的初始化
  |     -> ReferenceConfig.init()
  |         -> createProxy()
  |             -> createInvoker()
  |
  +-- 5. createInvoker() 内部流程：
  |     -> protocolSPI.refer(interfaceClass, registryUrl)
  |         -> RegistryProtocol.refer()
  |             +-- 创建 RegistryDirectory（服务目录）
  |             +-- 订阅注册中心（获取 provider 列表）
  |             +-- 对每个 provider URL 创建 DubboInvoker（建立 Netty 连接）
  |             +-- cluster.join(directory) → 创建 FailoverClusterInvoker
  |
  +-- 6. 代理方法被调用时的完整链路：
  |     -> InvokerInvocationHandler.invoke()          [代理层]
  |         -> FailoverClusterInvoker.invoke()        [集群层: 负载均衡 + 重试]
  |             -> Filter Chain                       [过滤器链]
  |                 -> DubboInvoker.doInvoke()        [协议层: 封装请求]
  |                     -> HeaderExchangeChannel.request()  [交换层: 请求-响应匹配]
  |                         -> NettyClient.send()    [传输层: 网络发送]
  |
  +-- 7. 响应返回：
        -> Netty 收到 Response 字节流
            -> DubboCodec 解码
                -> DefaultFuture.received()  [通过 requestId 匹配]
                    -> CompletableFuture.complete()
                        -> AsyncRpcResult.recreate()
                            -> 返回业务方法结果给调用者
```

---

## 第一阶段：Spring 容器扫描与 ReferenceBean 注册

### 1.1 ReferenceAnnotationBeanPostProcessor —— 扫描 @DubboReference

**源码位置**: `dubbo-config/dubbo-config-spring/src/main/java/org/apache/dubbo/config/spring/beans/factory/annotation/ReferenceAnnotationBeanPostProcessor.java`

> **这一步在干什么？**
>
> 跟 Provider 端的 `ServiceAnnotationPostProcessor` 扫描 `@DubboService` 类似，消费端需要找到你代码里所有标了 `@DubboReference` 的字段。比如：
>
> ```java
> @Component
> public class OrderController {
>     @DubboReference(timeout = 3000)
>     private UserService userService;  // ← 这个字段会被扫描到
> }
> ```
>
> 扫描到之后，为每个引用注册一个 `ReferenceBean` 的 BeanDefinition 到 Spring 容器里。

这个类同时实现了两个 Spring 扩展接口：

- `BeanFactoryPostProcessor`：在 Bean 定义注册阶段扫描所有 `@DubboReference` 字段
- `InstantiationAwareBeanPostProcessor`：在 Bean 属性注入阶段把代理对象注入到字段里

**核心扫描逻辑：**

```java
@Override
public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
    // 遍历所有 BeanDefinition
    String[] beanNames = beanFactory.getBeanDefinitionNames();
    for (String beanName : beanNames) {
        Class<?> beanType = beanFactory.getType(beanName);
        if (beanType != null) {
            // 找到这个 Bean 类中所有标注了 @DubboReference 的字段和方法
            AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, beanType, null);
            // 为每个引用注册 ReferenceBean
            prepareInjection(metadata);
        }
    }
}
```

**prepareInjection() 内部 —— 注册 ReferenceBean：**

```java
private void prepareInjection(AnnotatedInjectionMetadata metadata) {
    for (AnnotatedFieldElement fieldElement : metadata.getFieldElements()) {
        // 例如: 字段类型是 UserService.class，注解属性有 timeout=3000
        Class<?> injectedType = fieldElement.field.getType();
        AnnotationAttributes attributes = fieldElement.getAttributes();

        // 注册一个 ReferenceBean 到 Spring 容器
        registerReferenceBean(fieldElement.getPropertyName(), injectedType, attributes, fieldElement.field);
    }
}
```

**registerReferenceBean() —— 构建 BeanDefinition：**

```java
private String registerReferenceBean(String propertyName, Class<?> injectedType,
        Map<String, Object> attributes, Member member) {

    // 1. 生成唯一的 referenceKey，如 "com.example.UserService:1.0.0"
    String referenceKey = ReferenceBeanSupport.generateReferenceKey(attributes);

    // 2. 创建 ReferenceBean 的 BeanDefinition
    RootBeanDefinition beanDefinition = new RootBeanDefinition();
    beanDefinition.setBeanClassName(ReferenceBean.class.getName());

    // 3. 把注解上的属性（timeout、retries、version 等）存进去
    beanDefinition.setAttribute(REFERENCE_PROPS, attributes);
    beanDefinition.setAttribute(INTERFACE_CLASS, injectedType);
    beanDefinition.setAttribute(INTERFACE_NAME, injectedType.getName());

    // 4. 注册到 Spring 容器
    beanDefinitionRegistry.registerBeanDefinition(referenceBeanName, beanDefinition);

    // 5. 记录映射关系
    referenceBeanManager.registerReferenceKeyAndBeanName(referenceKey, referenceBeanName);

    return referenceBeanName;
}
```

**做完这步：** Spring 容器里多了一个 `ReferenceBean` 类型的 BeanDefinition。但此时还没有创建任何代理对象或网络连接。

---

### 1.2 ReferenceBean —— 懒代理工厂

**源码位置**: `dubbo-config/dubbo-config-spring/src/main/java/org/apache/dubbo/config/spring/ReferenceBean.java`

> **这一步在干什么？**
>
> `ReferenceBean` 实现了 Spring 的 `FactoryBean` 接口。当 Spring 容器需要注入 `UserService` 字段时，会调用 `ReferenceBean.getObject()` 获取实际要注入的对象。
>
> **关键设计：懒初始化。** `getObject()` 不会立刻去连注册中心、建 TCP 连接。它只是返回一个"懒代理"——一个空壳子。真正的初始化（连注册中心、建连接、创建 Invoker 链）在**第一次调用代理方法时**才触发。
>
> **为什么要懒？** 因为应用启动时，Provider 可能还没就绪。如果启动时就去连，可能连不上导致启动失败。懒初始化让应用先启动完成，等真正调用时再去建连，容错性更好。

```java
@Override
public Object getObject() {
    if (lazyProxy == null) {
        createLazyProxy();
    }
    return lazyProxy;
}

private void createLazyProxy() {
    // 1. 构建接口列表
    Set<Class<?>> interfaces = new LinkedHashSet<>();
    interfaces.add(interfaceClass);         // UserService.class
    interfaces.add(EchoService.class);      // Dubbo 内置的回声测试接口
    interfaces.add(Destroyable.class);      // 销毁接口

    // 2. 创建懒加载目标源（真正的初始化逻辑在这里面）
    DubboReferenceLazyInitTargetSource targetSource = new DubboReferenceLazyInitTargetSource();

    // 3. 创建懒代理
    //    这个代理对象被注入到你的字段里
    //    调用它的任何方法时，会先触发 targetSource.getTarget() 完成初始化
    lazyProxy = Proxy.newProxyInstance(
            interfaceClass.getClassLoader(),
            interfaces.toArray(new Class[0]),
            new LazyTargetInvocationHandler(targetSource)
    );
}
```

**LazyTargetInvocationHandler.invoke() —— 首次调用时触发初始化：**

```java
public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // 第一次调用时，getTarget() 会触发真正的初始化
    Object target = targetSource.getTarget();
    // target 是通过 ReferenceConfig 创建的真正代理对象
    return method.invoke(target, args);
}
```

**targetSource.getTarget() 内部：**

```java
public Object getTarget() {
    if (target == null) {
        synchronized (this) {
            if (target == null) {
                // 触发 ReferenceConfig 的完整初始化流程!
                referenceConfig.get();
                target = referenceConfig.getRef();  // 获取真正的代理对象
            }
        }
    }
    return target;
}
```

---

### 1.3 属性注入 —— 把懒代理注入到你的字段

**触发时机：** Spring 创建你的 `OrderController` Bean 时，回调 `ReferenceAnnotationBeanPostProcessor.postProcessProperties()`

```java
@Override
public PropertyValues postProcessProperties(PropertyValues pvs, Object bean, String beanName) {
    // 1. 找到这个 Bean 的注入元数据
    AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, bean.getClass(), pvs);

    // 2. 执行注入
    metadata.inject(bean, beanName, pvs);
    //   内部：field.set(bean, getBean(referenceBeanName))
    //         ↓
    //   Spring 容器调用 ReferenceBean.getObject() 获取懒代理
    //   然后把懒代理设置到你的 @DubboReference 字段上
    return pvs;
}
```

**做完这步：** 你的 `OrderController.userService` 字段已经有值了——一个懒代理对象。但此时没有任何网络连接，也没有订阅注册中心。一切都在等第一次方法调用。

---

## 第二阶段：ReferenceConfig —— 真正的初始化（首次调用时触发）

### 2.1 ReferenceConfig.get() —— 初始化入口

**源码位置**: `dubbo-config/dubbo-config-api/src/main/java/org/apache/dubbo/config/ReferenceConfig.java`

> **什么时候执行？** 第一次调用 `userService.getUserById(1)` 时，懒代理的 `getTarget()` 会调到这里。

```java
public T get(boolean check) {
    if (ref == null) {
        // 确保 Dubbo 模块已启动
        getScopeModel().getDeployer().start();

        // 初始化引用（创建代理、建立连接、订阅注册中心）
        init(check);
    }
    return ref;
}
```

### 2.2 ReferenceConfig.init() —— 核心初始化流程

```java
protected synchronized void init(boolean check) {
    if (initialized) {
        return;
    }
    initialized = true;

    // 1. 刷新配置（合并 JVM 参数、外部化配置、注解属性）
    this.refresh();
    checkAndUpdateSubConfigs();

    // 2. 初始化服务元数据
    initServiceMetadata(consumer);
    serviceMetadata.setServiceType(getServiceInterfaceClass());

    // 3. 收集所有引用参数（timeout、retries、loadbalance 等）
    Map<String, String> referenceParameters = appendConfig();

    // 4. 注册 ConsumerModel 到全局仓库
    //    （类似 Provider 端的 ProviderModel，是消费者在框架内的"身份证"）
    ModuleServiceRepository repository = getScopeModel().getServiceRepository();
    ServiceDescriptor serviceDescriptor = repository.registerService(interfaceClass);
    consumerModel = new ConsumerModel(
            serviceMetadata.getServiceKey(),
            "jdk",   // proxy type
            serviceDescriptor,
            getScopeModel(),
            serviceMetadata,
            null,
            classLoader);
    repository.registerConsumer(consumerModel);

    // ===== 5. 核心：创建代理对象 =====
    ref = createProxy(referenceParameters);

    // 6. 可用性检查（check=true 时会验证是否有可用 Provider）
    checkInvokerAvailable(0);
}
```

### 2.3 createProxy() —— 创建代理对象

> **这一步在干什么？**
>
> 把所有准备工作的成果——注册中心地址、引用参数、接口信息——汇总起来，创建一个真正能发起 RPC 调用的代理对象。分三步：
> 1. 收集注册中心 URL
> 2. 通过 Protocol SPI 创建 Invoker 链（涉及服务发现、连接建立）
> 3. 用 Invoker 生成 JDK 动态代理

```java
private T createProxy(Map<String, String> referenceParameters) {

    // 1. 收集注册中心 URL
    //    把引用参数（timeout、retries等）作为 attribute 放入 registry URL
    //    后续 RegistryProtocol 会从中取出这些参数
    aggregateUrlFromRegistry(referenceParameters);
    // urls 现在是: [registry://nacos-host:8848/...?refer=timeout%3D3000%26...]

    // 2. 创建 Invoker 链（最复杂的一步，后面详细展开）
    createInvoker();

    // 3. 用 Invoker 生成 JDK 动态代理
    //    proxyFactory 默认是 JavassistProxyFactory
    //    生成的代理对象实现了 UserService 接口
    //    所有方法调用都会转发到 InvokerInvocationHandler
    return (T) proxyFactory.getProxy(invoker, isGeneric(genericType));
}
```

### 2.4 createInvoker() —— 创建 Invoker 链（核心中的核心）

> **这一步在干什么？**
>
> 这是整个消费端初始化最复杂的一步。它要做的事情是：
> 1. 连到注册中心（Nacos/Zookeeper）
> 2. 从注册中心拿到所有 Provider 的地址列表
> 3. 对每个 Provider 建立 TCP 连接
> 4. 把所有连接包装成 Invoker，组装成一个带负载均衡和重试能力的 ClusterInvoker
>
> 做完之后，你就拿到了一个"可以调用远端服务的 Invoker"——调用它的 `invoke()` 方法，它会自动选一个 Provider、发请求、等响应。

```java
private void createInvoker() {
    if (urls.size() == 1) {
        // 单注册中心场景（最常见）
        URL curUrl = urls.get(0);
        // protocolSPI 根据 URL 协议头路由：
        //   service-discovery-registry:// → RegistryProtocol.refer()
        invoker = protocolSPI.refer(interfaceClass, curUrl);

    } else if (urls.size() > 1) {
        // 多注册中心场景：分别 refer，再用 Cluster 聚合
        List<Invoker<?>> invokers = new ArrayList<>();
        for (URL url : urls) {
            invokers.add(protocolSPI.refer(interfaceClass, url));
        }
        // 用 StaticDirectory 包装，再走一次 Cluster.join
        invoker = Cluster.getCluster(getScopeModel(), Cluster.DEFAULT)
                .join(new StaticDirectory(invokers), true);
    }

    // 包装 Filter 链（ConsumerContextFilter、MonitorFilter 等）
    if (this.bootstrap != null) {
        invoker = this.bootstrap.refer(invoker);
    }
}
```

---

## 第三阶段：RegistryProtocol.refer() —— 注册中心协议引用

### 3.1 RegistryProtocol.refer() —— 入口

**源码位置**: `dubbo-registry/dubbo-registry-api/src/main/java/org/apache/dubbo/registry/integration/RegistryProtocol.java`

> **这一步在干什么？**
>
> 上一步 `protocolSPI.refer()` 根据 URL 协议头路由到了 `RegistryProtocol`。这个 Protocol 是一个"中间人"——它不直接处理网络通信，而是：
> 1. 连接注册中心
> 2. 创建"服务目录"（Directory），用来维护 Provider 列表
> 3. 订阅注册中心的服务变更通知
> 4. 把 Directory 交给 Cluster 层，创建一个带重试和负载均衡能力的 ClusterInvoker

```java
@Override
public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
    // 1. 解析注册中心 URL（去掉外层包装，得到真正的注册中心地址）
    url = getRegistryUrl(url);

    // 2. 获取 Registry 实例（如 NacosRegistry、ZookeeperRegistry）
    Registry registry = getRegistry(url);

    // 3. 获取 Cluster 实现（默认 FailoverCluster）
    Map<String, String> qs = url.getParameters();
    Cluster cluster = Cluster.getCluster(url.getScopeModel(),
            qs.get(CLUSTER_KEY));  // 默认 "failover"

    // 4. 进入核心的 doRefer
    return doRefer(cluster, registry, type, url, qs);
}
```

### 3.2 doRefer() —— 创建目录 + 订阅 + 聚合

```java
private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type,
        URL url, Map<String, String> parameters) {

    // 1. 构建消费者 URL（描述"我是谁、我要消费什么"）
    URL consumerUrl = new ServiceConfigURL(
            parameters.get(PROTOCOL_KEY) == null ? DUBBO : parameters.get(PROTOCOL_KEY),
            null, null,
            parameters.get(REGISTER_IP_KEY), 0,
            getPath(parameters, type), parameters);

    // 2. 创建 MigrationInvoker（兼容接口级/应用级服务发现）
    ClusterInvoker<T> migrationInvoker = getMigrationInvoker(
            this, cluster, registry, type, url, consumerUrl);

    // 3. 拦截器处理（MigrationRuleListener 等）
    return interceptInvoker(migrationInvoker, url, consumerUrl);
}
```

### 3.3 doCreateInvoker() —— 真正的订阅和 Cluster 组装

> **这一步在干什么？**
>
> 这是注册中心引用的最后一步，也是最关键的一步。它会：
> 1. 创建一个 `RegistryDirectory`——可以理解为一个"服务地址簿"，实时维护所有可用 Provider 的 Invoker 列表
> 2. 向注册中心注册消费者信息（让 Provider 知道有哪些消费者在用它）
> 3. 订阅注册中心的 provider 列表变更——注册中心会推送所有 Provider 的地址过来
> 4. 收到 Provider 列表后，对每个 Provider 创建 DubboInvoker（建立 TCP 连接）
> 5. 用 Cluster.join() 把 Directory 包装成一个 ClusterInvoker（带负载均衡和重试）

```java
private <T> ClusterInvoker<T> doCreateInvoker(
        DynamicDirectory<T> directory, Cluster cluster, Registry registry, Class<T> type) {

    // 1. 设置 registry 和 protocol（后续创建 DubboInvoker 时要用）
    directory.setRegistry(registry);
    directory.setProtocol(protocol);

    // 2. 向注册中心注册消费者 URL
    //    例如在 Nacos 中创建一个消费者实例记录
    URL registeredConsumerUrl = getRegisteredConsumerUrl(directory.getConsumerUrl(),
            directory.getRegisteredConsumerUrl());
    registry.register(registeredConsumerUrl);

    // 3. 构建路由链（TagRouter、ConditionRouter 等）
    directory.buildRouterChain(directory.getRegisteredConsumerUrl());

    // ===== 4. 核心：订阅服务 =====
    //    注册中心会推送 provider URL 列表过来
    //    directory 收到通知后会创建对应的 DubboInvoker
    directory.subscribe(toSubscribeUrl(directory.getRegisteredConsumerUrl()));

    // ===== 5. 核心：Cluster 聚合 =====
    //    把 directory（持有一组 DubboInvoker）包装成 FailoverClusterInvoker
    //    FailoverClusterInvoker 具备负载均衡和失败重试能力
    return (ClusterInvoker<T>) cluster.join(directory, true);
}
```

---

## 第四阶段：服务发现 —— 从注册中心获取 Provider 列表并建立连接

### 4.1 RegistryDirectory.subscribe() —— 订阅注册中心

**源码位置**: `dubbo-registry/dubbo-registry-api/src/main/java/org/apache/dubbo/registry/integration/RegistryDirectory.java`

> **这一步在干什么？**
>
> 调用 `registry.subscribe(url, this)`，向注册中心订阅指定服务的 provider 列表。`RegistryDirectory` 自身实现了 `NotifyListener` 接口，当注册中心的 provider 列表发生变化时（新增节点、下线节点），注册中心会回调 `RegistryDirectory.notify()` 方法推送最新的 URL 列表。
>
> 第一次订阅时，注册中心会立即推送当前所有已注册的 Provider URL。

```java
public void subscribe(URL url) {
    setSubscribeUrl(url);
    // 向注册中心订阅，自身作为 listener
    registry.subscribe(url, this);
    // 例如 NacosRegistry：订阅 Nacos 服务的实例变更事件
}
```

### 4.2 RegistryDirectory.notify() —— 收到 Provider 列表通知

```java
@Override
public synchronized void notify(List<URL> urls) {
    // 按 category 分类
    Map<String, List<URL>> categoryUrls = urls.stream()
            .collect(Collectors.groupingBy(url -> {
                if (UrlUtils.isConfigurator(url)) return CONFIGURATORS_CATEGORY;
                if (UrlUtils.isRoute(url)) return ROUTERS_CATEGORY;
                return PROVIDERS_CATEGORY;
            }));

    // 处理 configurators（动态配置覆盖）
    List<URL> configuratorUrls = categoryUrls.getOrDefault(CONFIGURATORS_CATEGORY, Collections.emptyList());
    this.configurators = Configurator.toConfigurators(configuratorUrls).orElse(this.configurators);

    // 处理 routers（动态路由规则）
    List<URL> routerUrls = categoryUrls.getOrDefault(ROUTERS_CATEGORY, Collections.emptyList());
    toRouters(routerUrls).ifPresent(this::addRouters);

    // ===== 核心：处理 providers =====
    List<URL> providerUrls = categoryUrls.getOrDefault(PROVIDERS_CATEGORY, Collections.emptyList());
    refreshOverrideAndInvoker(providerUrls);
}
```

### 4.3 refreshInvoker() —— 为每个 Provider 创建 Invoker

> **这一步在干什么？**
>
> 收到 Provider URL 列表后（比如 `[dubbo://192.168.1.100:20880/UserService, dubbo://192.168.1.101:20880/UserService]`），需要对每个 URL 创建一个 `DubboInvoker`。每个 DubboInvoker 内部持有一条到对应 Provider 的 TCP 连接。
>
> 同时，如果某个 Provider 下线了（新列表里没有它了），要销毁对应的旧 Invoker 并关闭连接。

```java
private void refreshInvoker(List<URL> invokerUrls) {
    // 空协议 = 禁止调用
    if (invokerUrls.size() == 1
            && invokerUrls.get(0) != null
            && EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
        this.forbidden = true;
        this.invokers = Collections.emptyList();
        destroyAllInvokers();
        return;
    }

    this.forbidden = false;

    // 对比新老 URL 列表
    Map<URL, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap;
    Map<URL, Invoker<T>> newUrlInvokerMap = new ConcurrentHashMap<>();

    for (URL providerUrl : invokerUrls) {
        // 合并配置（consumer 的 timeout 覆盖 provider 的默认值等）
        URL url = mergeUrl(providerUrl);

        if (oldUrlInvokerMap != null && oldUrlInvokerMap.containsKey(url)) {
            // 旧的 Invoker 还能用，直接复用
            newUrlInvokerMap.put(url, oldUrlInvokerMap.get(url));
        } else {
            // ===== 新的 Provider，创建 DubboInvoker =====
            Invoker<T> invoker = protocol.refer(serviceType, url);
            // protocol.refer() 走到 DubboProtocol.refer()
            // 内部会建立 Netty TCP 连接
            newUrlInvokerMap.put(url, invoker);
        }
    }

    // 更新 Invoker 列表
    this.urlInvokerMap = newUrlInvokerMap;
    this.invokers = Collections.unmodifiableList(new ArrayList<>(newUrlInvokerMap.values()));

    // 销毁不再使用的旧 Invoker（关闭连接）
    destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);
}
```

---

## 第五阶段：DubboProtocol.refer() —— 创建 DubboInvoker 并建立连接

### 5.1 DubboProtocol.protocolBindingRefer() —— 创建 DubboInvoker

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboProtocol.java`

> **这一步在干什么？**
>
> 对于注册中心推送过来的每一个 Provider URL（如 `dubbo://192.168.1.100:20880/UserService`），都要创建一个 `DubboInvoker`。DubboInvoker 是消费端真正发起 RPC 调用的执行体，它内部持有到 Provider 的 TCP 连接（ExchangeClient）。
>
> 这一步做两件事：
> 1. 建立网络连接（TCP）
> 2. 把连接包装成 DubboInvoker

```java
@Override
public <T> Invoker<T> protocolBindingRefer(Class<T> serviceType, URL url) throws RpcException {
    // 序列化优化
    optimizeSerialization(url);

    // 1. 获取或创建到 Provider 的连接
    //    默认是共享连接（多个 DubboInvoker 共享同一个 TCP 连接）
    ExchangeClient[] clients = getClients(url);

    // 2. 创建 DubboInvoker
    DubboInvoker<T> invoker = new DubboInvoker<>(serviceType, url, clients, invokers);

    // 3. 加入 invoker 集合（便于后续统一管理/销毁）
    invokers.add(invoker);

    return invoker;
}
```

### 5.2 getClients() —— 获取或创建网络连接

```java
private ExchangeClient[] getClients(URL url) {
    // 获取连接数配置（默认共享 1 个连接）
    int connections = url.getParameter(CONNECTIONS_KEY, 0);

    // 共享连接模式（默认）
    if (connections == 0) {
        int shareConnections = getShareConnectionsCount(url);  // 默认1
        ExchangeClient[] clients = new ExchangeClient[shareConnections];
        for (int i = 0; i < shareConnections; i++) {
            clients[i] = getSharedClient(url, i);
        }
        return clients;
    }

    // 独立连接模式
    ExchangeClient[] clients = new ExchangeClient[connections];
    for (int i = 0; i < connections; i++) {
        clients[i] = initClient(url);
    }
    return clients;
}
```

### 5.3 getSharedClient() —— 连接复用

> **为什么要连接复用？** 如果你的应用引用了同一台机器上的 10 个服务（UserService、OrderService、PayService...），不需要建 10 条 TCP 连接。它们可以共享同一条连接——因为 Dubbo 协议是多路复用的（通过 requestId 区分不同请求）。

```java
private ExchangeClient getSharedClient(URL url, int connectNum) {
    String key = url.getAddress();  // "192.168.1.100:20880"

    // 从缓存中获取
    Object clients = referenceClientMap.get(key);
    if (clients instanceof List) {
        // 已有共享连接，引用计数+1
        ReferenceCountExchangeClient client = ((List<ReferenceCountExchangeClient>) clients).get(connectNum);
        if (client != null && !client.isClosed()) {
            client.incrementAndGetCount();
            return client;
        }
    }

    // 没有则创建新连接
    ExchangeClient exchangeClient = initClient(url);
    ReferenceCountExchangeClient client = new ReferenceCountExchangeClient(exchangeClient);
    // 放入缓存
    referenceClientMap.put(key, client);
    return client;
}
```

### 5.4 initClient() —— 建立 TCP 连接

```java
private ExchangeClient initClient(URL url) {
    // 设置编解码器
    url = url.addParameterIfAbsent(CODEC_KEY, DubboCodec.NAME);

    // 设置心跳间隔（默认 60s）
    url = url.addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT));

    // 通过 Exchangers.connect 进入 Exchange 层
    ExchangeClient client = Exchangers.connect(url, requestHandler);
    return client;
}
```

### 5.5 连接建立的完整调用链

```
initClient(url)
  → Exchangers.connect(url, handler)
    → HeaderExchanger.connect(url, handler)
      → new HeaderExchangeClient(
            Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler))),
            needHeartbeat)
        → NettyTransporter.connect(url, handler)
          → new NettyClient(url, handler)
            → doConnect()
              → bootstrap.connect(getConnectAddress())  ← Netty TCP 连接建立!
```

**NettyClient.doConnect() —— Netty 建立 TCP 连接：**

```java
@Override
protected void doConnect() throws Throwable {
    ChannelFuture future = bootstrap.connect(getConnectAddress());
    boolean ret = future.awaitUninterruptibly(getConnectTimeout(), MILLISECONDS);
    if (ret && future.isSuccess()) {
        Channel newChannel = future.channel();
        // 关闭旧连接（如果有）
        Channel oldChannel = this.channel;
        if (oldChannel != null) {
            oldChannel.close();
        }
        this.channel = newChannel;
    } else if (future.cause() != null) {
        throw new RemotingException("client connect to " + getRemoteAddress() + " failed", future.cause());
    } else {
        throw new RemotingException("client connect to " + getRemoteAddress() + " timed out");
    }
}
```

**做完这步：** TCP 连接建立完成。DubboInvoker 持有了一个到 Provider 的活跃连接，随时可以发送 RPC 请求。

---

## 第六阶段：Cluster 层 —— 负载均衡与失败重试

### 6.1 整体结构

> **这一层在干什么？**
>
> 经过前面的步骤，`RegistryDirectory` 里已经有了一组 `DubboInvoker`（每个对应一个 Provider 节点）。但调用者不应该直接跟某个具体的 Invoker 打交道——应该有一个中间层来做：
> - **负载均衡**：选哪个 Provider 发请求
> - **失败重试**：发失败了换一个再试
> - **路由过滤**：根据规则排除某些 Provider
>
> 这就是 Cluster 层的职责。`cluster.join(directory)` 返回一个 `FailoverClusterInvoker`，它把上面这些逻辑封装在 `invoke()` 方法里。

```
代理层调用 invoke(invocation)
         │
         ▼
┌─── FailoverClusterInvoker ────────────────────────────────┐
│                                                           │
│  1. directory.list(invocation)                            │
│     → RouterChain 过滤（TagRouter、ConditionRouter）       │
│     → 返回可用的 Invoker 列表                             │
│                                                           │
│  2. loadbalance.select(invokers, url, invocation)         │
│     → 从可用列表中选一个（Random/RoundRobin/LeastActive）  │
│                                                           │
│  3. invoker.invoke(invocation)                            │
│     → 调用选中的 DubboInvoker                             │
│                                                           │
│  4. 如果失败 && 重试次数未耗尽 → 回到步骤 1                 │
│     （选一个不同的 Invoker 重试）                           │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

### 6.2 FailoverClusterInvoker.doInvoke() —— 失败重试

**源码位置**: `dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/support/FailoverClusterInvoker.java`

```java
@Override
public Result doInvoke(Invocation invocation, List<Invoker<T>> invokers,
        LoadBalance loadbalance) throws RpcException {

    // 计算最大调用次数 = 1 + retries（默认 retries=2，即最多调 3 次）
    int len = calculateInvokeTimes(getUrl().getMethodParameter(
            invocation.getMethodName(), RETRIES_KEY, DEFAULT_RETRIES));

    RpcException lastException = null;
    List<Invoker<T>> invoked = new ArrayList<>(len);  // 记录已调用过的 Invoker

    for (int i = 0; i < len; i++) {
        // 重试时重新获取 Invoker 列表（可能有变化）
        if (i > 0) {
            checkWhetherDestroyed();
            invokers = list(invocation);
            checkInvokers(invokers, invocation);
        }

        // ===== 负载均衡选择 =====
        Invoker<T> invoker = select(loadbalance, invocation, invokers, invoked);
        invoked.add(invoker);

        boolean success = false;
        try {
            // ===== 调用选中的 Invoker =====
            Result result = invokeWithContext(invoker, invocation);
            success = true;
            return result;
        } catch (RpcException e) {
            if (e.isBiz()) {
                throw e;  // 业务异常不重试
            }
            lastException = e;
        } catch (Throwable e) {
            lastException = new RpcException(e.getMessage(), e);
        }
    }

    // 所有重试都失败了
    throw new RpcException("Failed to invoke " + invocation.getMethodName()
            + ". Tried " + len + " times. Last error: " + lastException.getMessage());
}
```

### 6.3 select() —— 负载均衡选择

```java
protected Invoker<T> select(LoadBalance loadbalance, Invocation invocation,
        List<Invoker<T>> invokers, List<Invoker<T>> selected) {

    // 用负载均衡算法选一个
    Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);

    // 如果选到的已经在已调用列表中（重试场景），或者不可用
    if (selected.contains(invoker) || !invoker.isAvailable()) {
        // 重新选择（排除已调用过的）
        invoker = reselect(loadbalance, invocation, invokers, selected);
    }

    return invoker;
}

private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation,
        List<Invoker<T>> invokers, List<Invoker<T>> selected) {

    // 只有一个 Invoker 时直接返回，不走负载均衡
    if (invokers.size() == 1) {
        return invokers.get(0);
    }

    // 调用负载均衡算法（默认 RandomLoadBalance：加权随机）
    return loadbalance.select(invokers, getUrl(), invocation);
}
```

---

## 第七阶段：DubboInvoker —— 封装 RPC 请求

### 7.1 DubboInvoker.doInvoke() —— 发送请求

**源码位置**: `dubbo-rpc/dubbo-rpc-dubbo/src/main/java/org/apache/dubbo/rpc/protocol/dubbo/DubboInvoker.java`

> **这一步在干什么？**
>
> 经过 Cluster 层选出了一个具体的 DubboInvoker，现在要真正把请求发出去了。这一步做的事情：
> 1. 往请求里塞附加信息（path、version、timeout）
> 2. 选一条连接（如果有多条的话轮询）
> 3. 根据调用方式（单向/双向、同步/异步）决定怎么发

```java
@Override
protected Result doInvoke(final Invocation invocation) throws Throwable {
    RpcInvocation inv = (RpcInvocation) invocation;

    // 1. 设置附加信息
    inv.setAttachment(PATH_KEY, getUrl().getPath());         // 接口路径
    inv.setAttachment(VERSION_KEY, version);                 // 版本号

    // 2. 选择一条连接（轮询）
    ExchangeClient currentClient;
    if (clients.length == 1) {
        currentClient = clients[0];
    } else {
        currentClient = clients[index.getAndIncrement() % clients.length];
    }

    // 3. 计算超时时间
    int timeout = RpcUtils.calculateTimeout(getUrl(), invocation,
            invocation.getMethodName(), DEFAULT_TIMEOUT);
    invocation.setAttachment(TIMEOUT_KEY, String.valueOf(timeout));

    // 4. 判断是否单向调用（不需要响应）
    boolean isOneway = RpcUtils.isOneway(getUrl(), invocation);

    if (isOneway) {
        // ===== 单向调用：发完就走，不等响应 =====
        boolean isSent = getUrl().getMethodParameter(invocation.getMethodName(), Constants.SENT_KEY, false);
        currentClient.send(inv, isSent);
        return AsyncRpcResult.newDefaultAsyncResult(invocation);
    } else {
        // ===== 双向调用（默认）：发请求 + 等响应 =====
        ExecutorService executor = getCallbackExecutor(getUrl(), inv);

        // 发送请求并获取 Future
        CompletableFuture<AppResponse> appResponseFuture =
                currentClient.request(inv, timeout, executor)
                        .thenApply(obj -> (AppResponse) obj);

        // 包装成 AsyncRpcResult 返回
        return new AsyncRpcResult(appResponseFuture, inv);
    }
}
```

---

## 第八阶段：Exchange 层 —— 请求-响应匹配

### 8.1 HeaderExchangeChannel.request() —— 创建 Future + 发送

**源码位置**: `dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/exchange/support/header/HeaderExchangeChannel.java`

> **这一步在干什么？**
>
> 这是 Exchange 层的核心。TCP 连接是双向的，请求和响应在同一条连接上传输，但它们是异步的——你发了请求 A，可能响应 B 先回来（如果有并发请求的话）。Exchange 层解决的就是**请求-响应匹配**问题：怎么知道收到的这个响应是对应哪个请求的？
>
> 答案是 **requestId**。每个请求有一个全局唯一的 ID，响应里也带着同样的 ID。发请求时，用 requestId 作为 key 把一个 `DefaultFuture` 存起来；收到响应时，用响应里的 ID 找到对应的 Future，然后 `complete()` 它。

```java
@Override
public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) {
    // 1. 构造 Request 对象
    Request req = new Request();
    req.setVersion(Version.getProtocolVersion());
    req.setTwoWay(true);
    req.setData(request);   // RpcInvocation（方法名、参数、附加信息）

    // 2. 创建 DefaultFuture
    //    以 requestId 为 key 存入全局 FUTURES map
    //    后续收到响应时通过 requestId 匹配到这个 Future
    DefaultFuture future = DefaultFuture.newFuture(channel, req, timeout, executor);

    // 3. 发送到网络!
    try {
        channel.send(req);
    } catch (RemotingException e) {
        future.cancel();
        throw e;
    }

    // 4. 返回 Future（调用者可以同步 get 或异步处理）
    return future;
}
```

### 8.2 DefaultFuture —— 请求-响应匹配的核心

**源码位置**: `dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/exchange/support/DefaultFuture.java`

```java
public class DefaultFuture extends CompletableFuture<Object> {

    // ===== 全局映射表 =====
    // requestId → Future（等待响应的请求）
    private static final Map<Long, DefaultFuture> FUTURES = new ConcurrentHashMap<>();
    // requestId → Channel（发送请求的连接）
    private static final Map<Long, Channel> CHANNELS = new ConcurrentHashMap<>();

    private final long id;       // requestId
    private final int timeout;   // 超时时间
    private final Request request;
    private volatile long sent;  // 实际发送时间戳

    // 创建新的 Future
    public static DefaultFuture newFuture(Channel channel, Request request,
            int timeout, ExecutorService executor) {
        final DefaultFuture future = new DefaultFuture(channel, request, timeout);

        // 放入全局 map
        FUTURES.put(request.getId(), future);
        CHANNELS.put(request.getId(), channel);

        // 启动超时检查（HashedWheelTimer 定时触发）
        timeoutCheck(future);

        return future;
    }

    // ===== 收到响应时调用 =====
    public static void received(Channel channel, Response response) {
        DefaultFuture future = FUTURES.remove(response.getId());
        if (future != null) {
            // 取消超时检查
            future.cancelTimeoutCheckTask();

            // 完成 Future
            future.doReceived(response);
        }
    }

    private void doReceived(Response res) {
        if (res.getStatus() == Response.OK) {
            // 正常响应 → 完成 Future
            this.complete(res.getResult());
        } else if (res.getStatus() == Response.CLIENT_TIMEOUT
                || res.getStatus() == Response.SERVER_TIMEOUT) {
            // 超时
            this.completeExceptionally(
                    new TimeoutException(res.getErrorMessage()));
        } else {
            // 其他错误
            this.completeExceptionally(
                    new RemotingException(channel, res.getErrorMessage()));
        }

        // 清理 CHANNELS map
        CHANNELS.remove(res.getId());
    }

    // 超时处理
    private static void timeoutCheck(DefaultFuture future) {
        TimeoutCheckTask task = new TimeoutCheckTask(future.getId());
        // 用 HashedWheelTimer 延迟 timeout 毫秒后执行检查
        future.timeoutCheckTask = TIME_OUT_TIMER.newTimeout(task, future.getTimeout(), MILLISECONDS);
    }
}
```

---

## 第九阶段：Transport 层 —— 网络编码与发送

### 9.1 从 Channel.send() 到 Netty 写入

```
HeaderExchangeChannel.request()
  → channel.send(req)                        // Channel 是 NettyChannel
    → NettyChannel.send(message, sent)
      → NioSocketChannel.writeAndFlush(message)  // Netty Channel
```

### 9.2 Netty Pipeline —— 编码

在 Netty 的 Pipeline 中，消息经过 `DubboCodec` 编码器编码为字节流：

```
Request 对象 → DubboCodec.encode() → 字节流 → TCP 发送

Dubbo 协议帧格式（16 字节 Header + Body）:
┌──────────────────────────────────────────────────────────────┐
│  0-1: Magic Number (0xdabb)                                  │
│  2:   Flag (请求/响应、单向/双向、心跳、序列化方式)             │
│  3:   Status (仅响应有效：OK/TIMEOUT/ERROR 等)               │
│  4-11: Request ID (8字节 long，用于匹配请求响应)              │
│  12-15: Body Length (4字节 int)                               │
│  16-...: Body (序列化后的 RpcInvocation / Response)           │
└──────────────────────────────────────────────────────────────┘
```

---

## 第十阶段：响应返回 —— 从网络到调用者

### 10.1 Provider 处理完毕，返回 Response

Provider 端收到请求后，通过自己的 Invoker 调用你的 `UserServiceImpl.getUserById(1)`，得到结果后封装为 `Response` 写回网络。

### 10.2 Consumer 端 Netty 收到响应

```
TCP 接收字节流
  → DubboCodec.decode()                      // 解码为 Response 对象
    → NettyClientHandler.channelRead()       // Netty 入站处理器
      → NettyChannel.received(message)
        → MultiMessageHandler.received()
          → HeartbeatHandler.received()      // 过滤心跳消息
            → AllChannelHandler.received()   // 分派到业务线程池!
```

### 10.3 AllChannelHandler —— 从 IO 线程切到业务线程

> **为什么要切线程？** Netty 的 IO 线程负责所有连接的网络读写，非常宝贵。如果在 IO 线程上做解码和业务处理，一旦某个响应处理慢了，会阻塞所有连接的 IO。所以 Dubbo 在这里把消息从 IO 线程 dispatch 到业务线程池处理。

```java
public class AllChannelHandler extends WrappedChannelHandler {
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        // 在业务线程池中执行后续处理
        ExecutorService executor = getPreferredExecutorService(message);
        executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.RECEIVED, message));
    }
}
```

### 10.4 HeaderExchangeHandler.received() —— 识别响应并匹配

```java
@Override
public void received(Channel channel, Object message) throws RemotingException {
    if (message instanceof Response) {
        // 这是一个 RPC 响应
        handleResponse(channel, (Response) message);
    } else if (message instanceof Request) {
        // 这是一个请求（回调场景）
        handleRequest(channel, (Request) message);
    }
}

static void handleResponse(Channel channel, Response response) {
    if (response != null && !response.isHeartbeat()) {
        // ===== 核心：通过 requestId 匹配 Future =====
        DefaultFuture.received(channel, response);
    }
}
```

### 10.5 DefaultFuture.received() —— Future 完成

```java
// 在 DefaultFuture 类中（前面已经详细展示过）
public static void received(Channel channel, Response response) {
    // 通过 response.getId() 从 FUTURES map 中取出对应的 Future
    DefaultFuture future = FUTURES.remove(response.getId());
    if (future != null) {
        future.doReceived(response);
        // → this.complete(response.getResult())
        // → CompletableFuture 完成!
    }
}
```

### 10.6 回到调用者 —— 从 Future 到返回值

```
CompletableFuture<Object> 完成（response.getResult() = AppResponse）
  │
  ▼
DubboInvoker 中的 appResponseFuture：
  currentClient.request(inv, timeout, executor).thenApply(obj -> (AppResponse) obj)
  → appResponseFuture 完成，值为 AppResponse
  │
  ▼
AsyncRpcResult 持有 appResponseFuture
  │
  ▼
InvocationUtil.invoke() 中：
  invoker.invoke(rpcInvocation).recreate()
  │  → AsyncRpcResult.recreate()
  │    → appResponseFuture.get()   [同步调用时在这里阻塞等待]
  │    → AppResponse.recreate()
  │      → 如果有异常：throw exception
  │      → 如果正常：return getValue()  // 你的业务返回值
  │
  ▼
InvokerInvocationHandler.invoke() 返回给调用者
  │
  ▼
你的代码收到返回值:
  User user = userService.getUserById(1);  // ← 拿到结果
```

---

## 第十一阶段：代理层 —— InvokerInvocationHandler

### 11.1 InvokerInvocationHandler.invoke() —— 最外层入口

**源码位置**: `dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/proxy/InvokerInvocationHandler.java`

> **这一步在干什么？**
>
> 这就是 JDK 动态代理的 InvocationHandler。当你调用 `userService.getUserById(1)` 时，实际调用的是代理对象，代理对象会把方法调用转发到这里。它负责：
> 1. 过滤掉 Object 的方法（toString、hashCode、equals）
> 2. 把方法调用信息（方法名、参数类型、参数值）封装成 `RpcInvocation`
> 3. 调用 Invoker 链的 `invoke()` 方法

```java
@Override
public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // 1. Object 方法直接处理，不走 RPC
    if (method.getDeclaringClass() == Object.class) {
        return method.invoke(invoker, args);
    }

    String methodName = method.getName();
    Class<?>[] parameterTypes = method.getParameterTypes();

    // toString / hashCode / equals 特殊处理
    if (parameterTypes.length == 0) {
        if ("toString".equals(methodName)) {
            return invoker.toString();
        } else if ("hashCode".equals(methodName)) {
            return invoker.hashCode();
        }
    } else if (parameterTypes.length == 1 && "equals".equals(methodName)) {
        return invoker.equals(args[0]);
    }

    // 2. 封装 RPC 调用信息
    RpcInvocation rpcInvocation = new RpcInvocation(
            serviceModel,
            methodName,
            interfaceName,
            protocolServiceKey,
            parameterTypes,
            args);

    // 3. 设置消费者模型上下文
    rpcInvocation.put(CONSUMER_MODEL, serviceModel);

    // 4. 调用 Invoker 链
    return InvocationUtil.invoke(invoker, rpcInvocation);
}
```

### 11.2 InvocationUtil.invoke() —— 设置上下文 + 触发调用

```java
public static Object invoke(Invoker<?> invoker, RpcInvocation rpcInvocation) throws Throwable {
    // 1. 存储 ServiceContext
    RpcContext.storeServiceContext();

    // 2. 设置服务唯一标识
    rpcInvocation.setTargetServiceUniqueName(
            invoker.getUrl().getServiceKey());

    // 3. 设置消费者 URL 到上下文
    RpcServiceContext serviceContext = RpcContext.getServiceContext();
    serviceContext.setConsumerUrl(invoker.getUrl());

    // 4. 调用 Invoker（进入 Cluster 层）并等待结果
    return invoker.invoke(rpcInvocation).recreate();
    //     ↓                              ↓
    //  Cluster → Filter → DubboInvoker   等待 Future 完成，返回业务值
}
```

---

## 整体分层架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│  你的代码: userService.getUserById(1)                                │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Proxy 层: InvokerInvocationHandler                                  │
│  职责: 把方法调用转换为 RpcInvocation，转发给 Invoker 链              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Cluster 层: FailoverClusterInvoker                                  │
│  职责: 从 Directory 获取 Invoker 列表 → 负载均衡选择 → 失败重试       │
│                                                                     │
│  内含:                                                               │
│    - RegistryDirectory: 持有所有可用 Provider 的 Invoker 列表        │
│    - RouterChain: 路由规则过滤（Tag/Condition/Script）                │
│    - LoadBalance: 负载均衡算法（Random/RoundRobin/LeastActive）       │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Filter 层: ConsumerContextFilter → MonitorFilter → ...              │
│  职责: 在调用前后做切面处理（设置上下文、记录监控、超时控制）          │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Protocol 层: DubboInvoker                                           │
│  职责: 封装 RPC 请求，选择连接，发送请求                              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Exchange 层: HeaderExchangeChannel                                   │
│  职责: 请求-响应匹配（通过 requestId + DefaultFuture）                │
│        超时检测（HashedWheelTimer）                                   │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Transport 层: NettyClient                                           │
│  职责: Netty 网络 IO、编解码（DubboCodec: 16字节Header + Body）       │
│        心跳保活、断线重连                                             │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
                          ┌─────────────┐
                          │   TCP 网络   │
                          └─────────────┘
```

---

## 关键设计思想总结

### 1. 懒初始化

`ReferenceBean.getObject()` 返回懒代理，第一次方法调用才触发 `ReferenceConfig.init()`。好处：应用启动快、不依赖 Provider 是否就绪。

### 2. 连接复用

同一个 `host:port` 上的多个服务共享 TCP 连接（通过 requestId 多路复用）。避免了"引用 100 个服务就建 100 条连接"的资源浪费。

### 3. 请求-响应匹配

`DefaultFuture` + 全局 `FUTURES` map + requestId。发请求时存入 map，收到响应时按 ID 取出并 complete。这是在一条 TCP 连接上支持并发请求的核心机制。

### 4. 异步模型

底层全部是异步的（CompletableFuture）。同步调用只是在最外层 `recreate()` 时调了 `future.get()` 阻塞等待。这意味着切换为异步调用（`@Async` 或返回 CompletableFuture）几乎零成本。

### 5. 目录 + 动态更新

`RegistryDirectory` 订阅注册中心，Provider 上下线时实时更新本地 Invoker 列表。调用时永远用最新的列表。

### 6. 分层解耦

每一层只做一件事：Proxy 层转换调用格式、Cluster 层做容错和均衡、Protocol 层封装请求、Exchange 层匹配请求响应、Transport 层管网络 IO。任何一层都可以通过 SPI 替换实现。

---

## 与 Provider 端的对照

| 维度 | Provider 端 | Consumer 端 |
|------|------------|-------------|
| 注解 | `@DubboService` | `@DubboReference` |
| 扫描器 | `ServiceAnnotationPostProcessor` | `ReferenceAnnotationBeanPostProcessor` |
| 配置类 | `ServiceConfig` | `ReferenceConfig` |
| Spring Bean | `ServiceBean`（普通 Bean） | `ReferenceBean`（FactoryBean，返回代理） |
| 核心操作 | `protocol.export(invoker)` → 开端口 | `protocol.refer(type, url)` → 建连接 |
| Protocol 层 | `DubboProtocol.export()` → 创建 Server | `DubboProtocol.refer()` → 创建 Client |
| 注册中心 | `register()` 注册自己 | `subscribe()` 订阅 Provider 列表 |
| 最终产物 | Netty Server 监听端口 | JDK 代理对象（内含 Invoker 链） |

---

## 一句话串联全流程

> Spring 扫描到你的 `@DubboReference` 字段，注册一个 `ReferenceBean`（FactoryBean）；Spring 注入时调用 `getObject()` 返回一个懒代理；第一次调用代理方法时触发 `ReferenceConfig.init()`——连接注册中心、订阅 Provider 列表、对每个 Provider 建立 Netty TCP 连接、用 Cluster 把所有连接聚合成一个带负载均衡和重试的 ClusterInvoker；后续每次方法调用通过 InvokerInvocationHandler 进入 Invoker 链，经过 Cluster（选节点+重试）→ Filter（切面）→ DubboInvoker（封装请求）→ HeaderExchangeChannel（创建 DefaultFuture + 发送）→ NettyClient（编码+网络发送）；响应返回时 Netty 收到字节流、解码、通过 requestId 匹配到 DefaultFuture 并 complete，最终 `recreate()` 把业务返回值交还给你的代码。
