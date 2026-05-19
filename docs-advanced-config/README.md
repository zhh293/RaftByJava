# Dubbo 高级配置深度分析文档

> 基于 Apache Dubbo 源码深度分析，涵盖所有可配置的高级特性，每篇均附 Spring Boot 使用示例。

---

## 文档目录

| 编号 | 文档 | 核心内容 |
|---|---|---|
| 01 | [SPI 扩展点与插件替换](./01-SPI扩展点与插件替换.md) | SPI 机制原理、Transporter/Protocol/Serialization/ProxyFactory/Compiler/Logger 等扩展点替换 |
| 02 | [线程池与消息派发配置](./02-线程池与消息派发配置.md) | fixed/cached/limited/eager/virtual 线程池、all/direct/message/execution/connection 派发策略 |
| 03 | [启动优化与连接管理](./03-启动优化与连接管理.md) | 延迟暴露、预热、懒连接、后台启动、心跳、重连、连接数控制、粘滞连接 |
| 04 | [集群容错与负载均衡](./04-集群容错与负载均衡.md) | failover/failfast/failsafe/failback/forking/broadcast/zone-aware 容错、random/roundrobin/leastactive/consistenthash/shortestresponse/adaptive 负载均衡 |
| 05 | [RpcContext 与隐式传参](./05-RpcContext与隐式传参.md) | ClientAttachment/ServerAttachment/ServerContext/ServiceContext/CancellationContext 五大 Context、隐式参数传递 |
| 06 | [服务治理与流量管控](./06-服务治理与流量管控.md) | 条件路由/标签路由/Mesh 路由、ExecuteLimit/ActiveLimit/TPS 限流、Token 鉴权、Triple 协议配置、SSL/TLS、Metrics、优雅停机 |
| 07 | [Filter 链与自定义扩展](./07-Filter链与自定义扩展.md) | Filter 链架构、@Activate 激活机制、自定义 Filter 开发、Filter.Listener、ExporterListener/InvokerListener |
| 08 | [配置中心、元数据、链路追踪与泛化调用](./08-配置中心-元数据-链路追踪-泛化调用.md) | ConfigCenter/MetadataReport/Tracing(Zipkin+OTLP)/GenericInvocation/ModuleConfig/MCP Server/OpenAPI/WebSocket/序列化安全 |

---

## 配置层次结构

```
Application (应用级)
├── Registry (注册中心)
├── ConfigCenter (配置中心)
├── MetadataReport (元数据中心)
├── Protocol (协议)
├── Metrics (指标)
├── Tracing (链路追踪)
├── SSL (安全)
├── Module (模块)
│   ├── Provider (服务提供者)
│   │   └── Service (具体服务)
│   │       └── Method (方法级)
│   └── Consumer (服务消费者)
│       └── Reference (具体引用)
│           └── Method (方法级)
└── Monitor (监控)
```

---

## 配置优先级（从高到低）

1. **方法级配置** > 接口级配置 > 全局配置
2. **Consumer 端配置** > Provider 端配置（超时等参数）
3. **JVM -D 参数** > 外部化配置（配置中心） > application.yml > API/注解

---

## 快速索引

### 按使用场景查找

| 场景 | 推荐文档 |
|---|---|
| 想替换底层网络框架 | 01 - Transporter SPI |
| 服务响应慢，想调优线程池 | 02 - 线程池配置 |
| 启动太慢，想优化启动速度 | 03 - 延迟暴露、后台启动 |
| 调用失败想自动重试/快速失败 | 04 - 集群容错 |
| 想在调用链中传递自定义参数 | 05 - RpcContext |
| 想做灰度发布/流量染色 | 06 - 标签路由 |
| 想加自定义拦截逻辑 | 07 - 自定义 Filter |
| 想接入配置中心动态管理 | 08 - 配置中心 |
| 想做分布式链路追踪 | 08 - Tracing |
| 网关层不想依赖接口 jar | 08 - 泛化调用 |
| 想暴露 AI Agent 可调用的服务 | 08 - MCP Server |
