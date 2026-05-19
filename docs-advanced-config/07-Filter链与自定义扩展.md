# Dubbo 高级配置深度分析（七）：Filter 链与自定义扩展

> 基于源码 `/Users/zhanghonghao/Desktop/dubbo` 分析

---

## 一、Filter 链架构

Dubbo 的 Filter 链是一个责任链模式的实现，分为 Provider 端 Filter 和 Consumer 端 Filter（ClusterFilter）。

### Provider 端内置 Filter

```
# dubbo-rpc/dubbo-rpc-api/src/main/resources/META-INF/dubbo/internal/org.apache.dubbo.rpc.Filter
echo=org.apache.dubbo.rpc.filter.EchoFilter
generic=org.apache.dubbo.rpc.filter.GenericFilter
genericimpl=org.apache.dubbo.rpc.filter.GenericImplFilter
token=org.apache.dubbo.rpc.filter.TokenFilter
accesslog=org.apache.dubbo.rpc.filter.AccessLogFilter
classloader=org.apache.dubbo.rpc.filter.ClassLoaderFilter
context=org.apache.dubbo.rpc.filter.ContextFilter
exception=org.apache.dubbo.rpc.filter.ExceptionFilter
executelimit=org.apache.dubbo.rpc.filter.ExecuteLimitFilter
deprecated=org.apache.dubbo.rpc.filter.DeprecatedFilter
timeout=org.apache.dubbo.rpc.filter.TimeoutFilter
tps=org.apache.dubbo.rpc.filter.TpsLimitFilter
profiler-server=org.apache.dubbo.rpc.filter.ProfilerServerFilter
adaptiveLoadBalance=org.apache.dubbo.rpc.filter.AdaptiveLoadBalanceFilter
active-limit=org.apache.dubbo.rpc.filter.ActiveLimitFilter
rpc-exception=org.apache.dubbo.rpc.filter.RpcExceptionFilter
```

### Consumer 端内置 ClusterFilter

```
# dubbo-cluster/src/main/resources/META-INF/dubbo/internal/org.apache.dubbo.rpc.cluster.filter.ClusterFilter
consumercontext=org.apache.dubbo.rpc.cluster.filter.support.ConsumerContextFilter
consumer-classloader=org.apache.dubbo.rpc.cluster.filter.support.ConsumerClassLoaderFilter
router-snapshot=org.apache.dubbo.rpc.cluster.router.RouterSnapshotFilter
metricsConsumerFilter=org.apache.dubbo.rpc.cluster.filter.support.MetricsConsumerFilter
```

---

## 二、Filter 激活机制（@Activate）

### 源码分析

```java
// @Activate 注解控制 Filter 的自动激活条件
@Activate(
    group = CommonConstants.PROVIDER,  // 在哪一端激活: PROVIDER / CONSUMER
    value = EXECUTES_KEY,              // 当 URL 中存在该参数时激活
    order = -1                         // 执行顺序（越小越先执行）
)
public class ExecuteLimitFilter implements Filter { }
```

**激活规则**：
- `group`：指定在 Provider 端还是 Consumer 端激活
- `value`：当 URL 参数中包含指定 key 时才激活（如 `executes` 参数存在时才激活 ExecuteLimitFilter）
- `order`：执行顺序，数值越小越先执行
- 无 `value` 的 Filter 默认激活（如 ContextFilter）

---

## 三、自定义 Filter 开发

### 完整示例：耗时统计 Filter

```java
package com.example.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务调用耗时统计 Filter
 * 同时在 Provider 和 Consumer 端激活
 */
@Activate(group = {CommonConstants.PROVIDER, CommonConstants.CONSUMER}, order = -10000)
public class CostTimeFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(CostTimeFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long startTime = System.currentTimeMillis();
        String side = invoker.getUrl().getParameter("side", "unknown");
        String service = invoker.getInterface().getSimpleName();
        String method = invocation.getMethodName();

        try {
            Result result = invoker.invoke(invocation);
            long costTime = System.currentTimeMillis() - startTime;

            if (costTime > 1000) {
                log.warn("[{}] Slow call: {}.{} cost {}ms",
                    side, service, method, costTime);
            } else {
                log.debug("[{}] {}.{} cost {}ms", side, service, method, costTime);
            }

            return result;
        } catch (RpcException e) {
            long costTime = System.currentTimeMillis() - startTime;
            log.error("[{}] Failed call: {}.{} cost {}ms, error: {}",
                side, service, method, costTime, e.getMessage());
            throw e;
        }
    }
}
```

### SPI 注册

```
# 文件: src/main/resources/META-INF/dubbo/org.apache.dubbo.rpc.Filter
costTime=com.example.filter.CostTimeFilter
```

---

### 完整示例：参数校验 Filter

```java
@Activate(group = CommonConstants.PROVIDER, order = -9000)
public class ParamValidationFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        Object[] args = invocation.getArguments();

        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                if (args[i] == null) {
                    // 检查参数是否允许为 null
                    // 可以结合 JSR-303 注解做更精细的校验
                }
                if (args[i] instanceof String && ((String) args[i]).length() > 10000) {
                    throw new RpcException("Parameter " + i + " is too long");
                }
            }
        }

        return invoker.invoke(invocation);
    }
}
```

---

### 完整示例：结果缓存 Filter

```java
@Activate(group = CommonConstants.CONSUMER, value = "cache")
public class ResultCacheFilter implements Filter {

    private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 只缓存查询方法
        if (invocation.getMethodName().startsWith("get")
            || invocation.getMethodName().startsWith("find")
            || invocation.getMethodName().startsWith("query")) {

            String cacheKey = buildCacheKey(invoker, invocation);
            Object cached = cache.get(cacheKey);
            if (cached != null) {
                return AsyncRpcResult.newDefaultAsyncResult(cached, invocation);
            }

            Result result = invoker.invoke(invocation);
            if (!result.hasException()) {
                cache.put(cacheKey, result.getValue());
            }
            return result;
        }

        return invoker.invoke(invocation);
    }

    private String buildCacheKey(Invoker<?> invoker, Invocation invocation) {
        return invoker.getInterface().getName() + "." + invocation.getMethodName()
            + ":" + Arrays.toString(invocation.getArguments());
    }
}
```

---

## 四、Filter 配置方式

### 方式一：通过 @Activate 自动激活（推荐）

```java
// 只要 classpath 中有这个类且注册了 SPI，就会自动激活
@Activate(group = CommonConstants.PROVIDER)
public class MyAutoFilter implements Filter { }
```

### 方式二：通过配置显式指定

```yaml
dubbo:
  provider:
    filter: costTime,paramValidation,default  # 自定义 + 默认 Filter
    # "default" 代表所有默认激活的 Filter
    # 不写 default 则只使用指定的 Filter

  consumer:
    filter: costTime,resultCache,default
```

### 方式三：排除特定 Filter

```yaml
dubbo:
  provider:
    filter: -accesslog,-token  # 用 "-" 前缀排除指定 Filter
```

### 方式四：服务级别配置

```java
@DubboService(filter = "costTime,validation")
public class MyServiceImpl implements MyService { }

@DubboReference(filter = "costTime,cache")
private MyService myService;
```

---

## 五、Filter.Listener 回调机制

```java
// Filter 可以实现 Listener 接口，在调用完成后回调
public class MyFilter implements Filter, Filter.Listener {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 前置处理
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // 调用成功后的回调
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        // 调用失败后的回调
    }
}
```

---

## 六、ExporterListener 与 InvokerListener

### 服务暴露/引用监听器

```java
// 监听服务暴露事件
public class MyExporterListener implements ExporterListener {
    @Override
    public void exported(Exporter<?> exporter) throws RpcException {
        // 服务暴露成功后的回调
        log.info("Service exported: {}", exporter.getInvoker().getUrl());
    }

    @Override
    public void unexported(Exporter<?> exporter) {
        // 服务取消暴露后的回调
        log.info("Service unexported: {}", exporter.getInvoker().getUrl());
    }
}

// 监听服务引用事件
public class MyInvokerListener implements InvokerListener {
    @Override
    public void referred(Invoker<?> invoker) throws RpcException {
        log.info("Service referred: {}", invoker.getUrl());
    }

    @Override
    public void destroyed(Invoker<?> invoker) {
        log.info("Service destroyed: {}", invoker.getUrl());
    }
}
```

### 配置

```yaml
dubbo:
  provider:
    listener: myExporterListener
  consumer:
    listener: myInvokerListener
```

---

## 七、内置 Filter 功能详解

| Filter | 端 | 功能 | 激活条件 |
|---|---|---|---|
| `EchoFilter` | Provider | 回声测试，用于检测服务是否可用 | 默认激活 |
| `GenericFilter` | Provider | 泛化调用支持 | 默认激活 |
| `TokenFilter` | Provider | Token 鉴权 | `token` 参数存在 |
| `AccessLogFilter` | Provider | 访问日志记录 | `accesslog` 参数存在 |
| `ContextFilter` | Provider | 设置 RpcContext | 默认激活 |
| `ExceptionFilter` | Provider | 异常处理（包装非声明异常） | 默认激活 |
| `ExecuteLimitFilter` | Provider | 并发执行数限制 | `executes` 参数存在 |
| `TimeoutFilter` | Provider | 超时检测（记录日志） | 默认激活 |
| `TpsLimitFilter` | Provider | TPS 限流 | `tps` 参数存在 |
| `ActiveLimitFilter` | Consumer | 并发调用数限制 | `actives` 参数存在 |
| `ConsumerContextFilter` | Consumer | 设置消费端 Context | 默认激活 |

---

## 八、HeaderFilter（Triple 协议专用）

Triple 协议有独立的 HeaderFilter 机制：

```java
// dubbo-rpc/dubbo-rpc-api/.../HeaderFilter.java
// 用于处理 HTTP/2 Header
```

```
# dubbo-rpc/dubbo-rpc-api/.../org.apache.dubbo.rpc.HeaderFilter
token=org.apache.dubbo.rpc.filter.TokenHeaderFilter
```

---

## 九、Spring Boot 综合配置示例

```yaml
dubbo:
  provider:
    # Filter 链配置
    filter: costTime,paramValidation,default,-deprecated
    # 服务监听器
    listener: myExporterListener
    # 限流配置
    executes: 200
    accesslog: /var/log/dubbo/access.log
    token: true

  consumer:
    # Filter 链配置
    filter: costTime,default
    # 引用监听器
    listener: myInvokerListener
    # 并发限制
    actives: 50
```

### 完整的自定义 Filter 项目结构

```
src/main/
├── java/com/example/filter/
│   ├── CostTimeFilter.java
│   ├── ParamValidationFilter.java
│   └── ResultCacheFilter.java
└── resources/META-INF/dubbo/
    └── org.apache.dubbo.rpc.Filter
        # 内容:
        # costTime=com.example.filter.CostTimeFilter
        # paramValidation=com.example.filter.ParamValidationFilter
        # resultCache=com.example.filter.ResultCacheFilter
```

---

## 十、最佳实践

1. **使用 @Activate 自动激活**：比手动配置更灵活，支持条件激活
2. **注意 Filter 顺序**：order 越小越先执行，安全类 Filter 应该最先执行
3. **Filter 中避免阻塞操作**：Filter 在调用链中同步执行，阻塞会影响性能
4. **异常处理要完善**：Filter 中的异常会中断整个调用链
5. **使用 Filter.Listener 做后置处理**：比在 invoke 中 try-catch 更优雅
6. **生产环境关闭不需要的 Filter**：用 `-filterName` 排除，减少开销
