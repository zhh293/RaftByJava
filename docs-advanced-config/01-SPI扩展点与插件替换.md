# Dubbo 高级配置深度分析（一）：SPI 扩展点与插件替换

> 基于源码 `/Users/zhanghonghao/Desktop/dubbo` 分析

---

## 一、SPI 机制核心原理

Dubbo 的 SPI（Service Provider Interface）机制是整个框架的灵魂。与 JDK 原生 SPI 不同，Dubbo SPI 支持按需加载、自适应扩展（Adaptive）、自动包装（Wrapper）和自动注入。

扩展点配置文件位于 `META-INF/dubbo/internal/` 目录下，以接口全限定名为文件名，内容为 `key=实现类全限定名` 的格式。

---

## 二、Transporter 传输层替换

### 源码定义

```java
// dubbo-remoting/dubbo-remoting-api/src/main/java/org/apache/dubbo/remoting/Transporter.java
@SPI(value = "netty", scope = ExtensionScope.FRAMEWORK)
public interface Transporter {

    @Adaptive({Constants.SERVER_KEY, Constants.TRANSPORTER_KEY})
    RemotingServer bind(URL url, ChannelHandler handler) throws RemotingException;

    @Adaptive({Constants.CLIENT_KEY, Constants.TRANSPORTER_KEY})
    Client connect(URL url, ChannelHandler handler) throws RemotingException;
}
```

**关键点**：`@SPI(value = "netty")` 表示默认使用 Netty 实现。`@Adaptive` 注解表示运行时根据 URL 参数 `server`/`client`/`transporter` 动态选择实现。

### 可用实现

项目中提供了两套 Netty 实现：
- `dubbo-remoting-netty`：基于 Netty3（旧版兼容）
- `dubbo-remoting-netty4`：基于 Netty4（默认，推荐）

```java
// dubbo-remoting/dubbo-remoting-netty4/.../NettyTransporter.java
public class NettyTransporter implements Transporter {
    public static final String NAME = "netty";

    @Override
    public RemotingServer bind(URL url, ChannelHandler handler) throws RemotingException {
        return new NettyServer(url, handler);
    }

    @Override
    public Client connect(URL url, ChannelHandler handler) throws RemotingException {
        return new NettyClient(url, handler);
    }
}
```

### Spring Boot 配置示例

```yaml
dubbo:
  protocol:
    name: dubbo
    port: 20880
    # 替换传输层实现（server端和client端可分别指定）
    transporter: netty    # 可选: netty (默认)
    server: netty         # 服务端传输层
    client: netty         # 客户端传输层
```

### 自定义 Transporter 扩展

```java
// 1. 实现 Transporter 接口
public class MyTransporter implements Transporter {
    @Override
    public RemotingServer bind(URL url, ChannelHandler handler) throws RemotingException {
        return new MyServer(url, handler);
    }

    @Override
    public Client connect(URL url, ChannelHandler handler) throws RemotingException {
        return new MyClient(url, handler);
    }
}

// 2. 在 META-INF/dubbo/org.apache.dubbo.remoting.Transporter 文件中注册
// mytransporter=com.example.MyTransporter

// 3. 配置使用
// dubbo.protocol.transporter=mytransporter
```

---

## 三、Protocol 协议层替换

### 源码定义

Protocol 是 Dubbo RPC 的核心扩展点，决定了服务暴露和引用的方式。

```
# dubbo-rpc/dubbo-rpc-api/src/main/resources/META-INF/dubbo/internal/org.apache.dubbo.rpc.Protocol
listener=org.apache.dubbo.rpc.protocol.ProtocolListenerWrapper
serializationwrapper=org.apache.dubbo.rpc.protocol.ProtocolSerializationWrapper
securitywrapper=org.apache.dubbo.rpc.protocol.ProtocolSecurityWrapper
invokercount=org.apache.dubbo.rpc.protocol.InvokerCountWrapper
mock=org.apache.dubbo.rpc.support.MockProtocol
```

项目中支持的协议：
- **dubbo**：Dubbo 私有协议（`dubbo-rpc-dubbo`）
- **tri / triple**：Triple 协议，兼容 gRPC（`dubbo-rpc-triple`）
- **injvm**：JVM 本地调用（`dubbo-rpc-injvm`）
- **rest**：RESTful HTTP 协议（`dubbo-rest-*`）

### Spring Boot 配置示例

```yaml
dubbo:
  protocol:
    name: tri          # 使用 Triple 协议（推荐，兼容 gRPC）
    port: 50051
  # 多协议支持
  protocols:
    dubbo:
      name: dubbo
      port: 20880
    triple:
      name: tri
      port: 50051
```

```java
// 服务暴露时指定协议
@DubboService(protocol = "dubbo,tri")  // 同时暴露 dubbo 和 triple 两种协议
public class DemoServiceImpl implements DemoService {
    // ...
}
```

---

## 四、Serialization 序列化替换

### 可用实现

```
# 来自各 serialization 模块的 SPI 配置
hessian2=org.apache.dubbo.common.serialize.hessian2.Hessian2Serialization
fastjson2=org.apache.dubbo.common.serialize.fastjson2.FastJson2Serialization
```

### 源码中的安全控制

```java
// dubbo-common/src/main/java/org/apache/dubbo/config/ApplicationConfig.java
private String serializeCheckStatus;      // 序列化检查状态: STRICT / WARN / DISABLE
private Boolean autoTrustSerializeClass;  // 是否自动信任序列化类
private Integer trustSerializeClassLevel; // 信任序列化类的级别
private Boolean checkSerializable;        // 是否检查 Serializable 接口
```

### Spring Boot 配置示例

```yaml
dubbo:
  protocol:
    name: tri
    port: 50051
    serialization: fastjson2           # 主序列化方式
    prefer-serialization: fastjson2,hessian2  # 优先序列化（按顺序尝试）
  application:
    serialize-check-status: STRICT     # STRICT(严格) / WARN(警告) / DISABLE(禁用)
    auto-trust-serialize-class: true   # 自动信任序列化类
    trust-serialize-class-level: 3     # 信任级别
    check-serializable: true           # 检查是否实现 Serializable
```

---

## 五、ProxyFactory 代理工厂替换

### 可用实现

```
# dubbo-rpc/dubbo-rpc-api/src/main/resources/META-INF/dubbo/internal/org.apache.dubbo.rpc.ProxyFactory
stub=org.apache.dubbo.rpc.proxy.wrapper.StubProxyFactoryWrapper
jdk=org.apache.dubbo.rpc.proxy.jdk.JdkProxyFactory
javassist=org.apache.dubbo.rpc.proxy.javassist.JavassistProxyFactory
nativestub=org.apache.dubbo.rpc.stub.StubProxyFactory
```

- **javassist**（默认）：使用 Javassist 字节码生成代理，性能最优
- **jdk**：使用 JDK 动态代理，兼容性好
- **stub**：Wrapper 模式，支持本地存根
- **nativestub**：原生 Stub 代理（用于 IDL 生成的代码）

### Spring Boot 配置示例

```yaml
dubbo:
  consumer:
    proxy: javassist   # 可选: javassist(默认), jdk
  provider:
    proxy: javassist
```

---

## 六、Compiler 编译器替换

### Spring Boot 配置示例

```yaml
dubbo:
  application:
    compiler: javassist  # 可选: javassist(默认), jdk
```

---

## 七、Logger 日志框架替换

### Spring Boot 配置示例

```yaml
dubbo:
  application:
    logger: slf4j  # 可选: slf4j, jcl, log4j, jdk, log4j2
```

---

## 八、Exchanger 信息交换层替换

### 可用实现

```
# dubbo-remoting/dubbo-remoting-api/.../org.apache.dubbo.remoting.exchange.Exchanger
header=org.apache.dubbo.remoting.exchange.support.header.HeaderExchanger
```

默认使用 `header` 实现（HeaderExchanger），负责封装 Request/Response 模型、心跳检测等。

### Spring Boot 配置示例

```yaml
dubbo:
  protocol:
    exchanger: header  # 默认 header，一般不需要修改
```

---

## 九、总结：SPI 扩展点一览表

| 扩展点接口 | 默认实现 | 配置 key | 说明 |
|---|---|---|---|
| `Transporter` | netty | `transporter`/`server`/`client` | 网络传输层 |
| `Protocol` | dubbo | `protocol.name` | RPC 协议 |
| `Serialization` | hessian2 | `serialization` | 序列化方式 |
| `ProxyFactory` | javassist | `proxy` | 代理生成方式 |
| `Compiler` | javassist | `compiler` | 动态编译器 |
| `LoggerAdapter` | slf4j | `logger` | 日志框架 |
| `Exchanger` | header | `exchanger` | 信息交换层 |
| `ThreadPool` | fixed | `threadpool` | 线程池策略 |
| `Dispatcher` | all | `dispatcher` | 消息派发策略 |
| `LoadBalance` | random | `loadbalance` | 负载均衡 |
| `Cluster` | failover | `cluster` | 集群容错 |
| `StateRouterFactory` | - | 动态路由 | 路由规则 |

所有 SPI 扩展点都可以通过自定义实现 + 配置文件注册的方式进行替换。
