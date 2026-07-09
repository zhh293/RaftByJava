# Netty 对 epoll 与 kqueue 原生传输的支持

> **Netty 源码深度研究系列 · 第 13 篇**
>
> 基于 Netty 主分支源码，深入剖析 Netty 如何通过 JNI 直接调用 Linux epoll 和 macOS/BSD kqueue 系统调用，突破 JDK NIO 的固有限制，获得更低延迟、更高吞吐和更多操作系统级特性。

---

## 一、为什么需要原生传输？JDK NIO 的局限

JDK 自带的 NIO（`java.nio.channels`）为 Java 提供了多路复用 IO 能力，但在高性能场景下存在多项结构性限制。Netty 选择通过 JNI 直接调用操作系统 API，正是为了绕过这些无法在 Java 层面修复的瓶颈。

### 1.1 空轮询 Bug（Epoll Spin Bug）

JDK NIO 中存在一个长期未修复的著名 Bug：在 Linux 上，`Selector.select()` 可能在没有任何就绪事件时立即返回，导致 CPU 100% 空转。JDK 社区从 2006 年（JDK-6670302）起就认识到这个问题，但由于涉及 JVM 底层实现，至今仍未彻底解决。

Netty 的 NioIoHandler 通过 `SELECTOR_AUTO_REBUILD_THRESHOLD`（默认 512 次）检测空轮询并重建 Selector 来规避此问题。但这本质上是一个"事后补救"策略——检测到问题后才修复，在重建的瞬间仍有性能抖动。

而 EpollIoHandler 直接调用 `epoll_wait`，完全绕过了 JDK Selector 的抽象层，从根本上消除了这个问题。

### 1.2 定时精度：毫秒 vs 纳秒

JDK 的 `Selector.select(long timeout)` 仅支持毫秒级超时。对于需要亚毫秒级响应的场景（如高频交易、实时音视频），这个精度远远不够。

Netty 的 epoll 原生传输通过两种方式实现纳秒级定时：

- **timerfd**：通过 `timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC|TFD_NONBLOCK)` 创建内核定时器 fd，注册到 epoll 实例中，由内核精确触发
- **epoll_pwait2**：Linux 5.11+ 引入的新系统调用，直接接受 `struct timespec` 纳秒级超时参数

### 1.3 唤醒机制的开销

当其他线程需要唤醒阻塞在 `select()` 上的 EventLoop 线程时，JDK 使用 `Selector.wakeup()`，其内部实现在 Linux 上涉及一次 pipe write。而 Netty 的 epoll 传输使用 `eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK)`——这是一个专为线程间通知设计的轻量级机制，只需一个原子写操作即可完成唤醒。

### 1.4 缺失的操作系统级特性

JDK NIO 的 `SocketOption` 仅暴露了最通用的 socket 选项。许多 Linux 特有的高性能选项（如 `SO_REUSEPORT`、`TCP_FASTOPEN`、`TCP_CORK`）和传输机制（如 Unix Domain Socket、`splice` 零拷贝、`sendmmsg` 批量发送）在 JDK 中完全不可用。

### 1.5 SelectionKey 集合的遍历效率

JDK 默认使用 `HashSet<SelectionKey>` 存放就绪事件。HashSet 的迭代涉及链表/红黑树遍历，对 CPU 缓存不友好。Netty 的 NioIoHandler 已经通过反射将其替换为数组实现（`SelectedSelectionKeySet`），但这属于 hack 手段，依赖 JDK 内部实现，在模块化 JDK 中可能被限制。

原生传输则完全不依赖 JDK 的 `SelectionKey` 体系，直接从内核获取 `struct epoll_event` 数组，天然是连续内存布局。

### 1.6 BUSY_WAIT 模式不可用

在极端低延迟场景下，需要让 EventLoop 线程持续轮询（`cpu_relax`）而不进入内核等待。JDK Selector 没有提供此能力。Netty 的 EpollIoHandler 支持 `BUSY_WAIT` 策略，通过 `epoll_wait(timeout=0)` + 自旋实现极致响应。

---

## 二、Netty Native Transport 架构：Java 层 + JNI 层

### 2.1 模块结构

Netty 的原生传输采用分层模块化设计：

```
transport-native-unix-common/        ← 共享层（31 个 Java 文件）
├── Unix Domain Socket 抽象
├── FileDescriptor、IovArray、NativeInetAddress
└── 跨平台通用的 native 工具类

transport-classes-epoll/             ← Java 层（37 个文件）
├── EpollIoHandler                   ← 核心事件循环
├── EpollSocketChannel / EpollServerSocketChannel
├── EpollDatagramChannel
├── EpollDomainSocketChannel
├── EpollChannelOption               ← Linux 特有选项
└── LinuxSocket                      ← 封装 Linux 特有 syscall

transport-native-epoll/              ← C JNI 层
├── netty_epoll_native.c             ← epoll_create/ctl/wait
├── netty_epoll_linuxsocket.c        ← setsockopt 各选项
└── Makefile / 编译脚本

transport-classes-kqueue/            ← Java 层（33 个文件）
├── KQueueIoHandler                  ← 核心事件循环
├── KQueueSocketChannel / KQueueServerSocketChannel
└── BsdSocket                        ← BSD 特有 syscall

transport-native-kqueue/             ← C JNI 层
├── netty_kqueue_native.c            ← kqueue/kevent
└── 编译脚本
```

### 2.2 分层设计哲学

这套架构的设计思想非常清晰：**Java 层负责 Channel 生命周期与事件驱动语义，JNI 层负责系统调用的薄封装**。两层之间通过 `FileDescriptor`（一个 int 值）连接。Java 层不持有任何 native 指针，不存在内存泄漏风险；JNI 层不包含任何业务逻辑，只做参数转换和 syscall 调用。

`transport-native-unix-common` 作为共享基础层，抽取了 epoll 和 kqueue 都需要的 Unix 域能力（地址解析、fd 操作、IO 向量等），避免代码重复。

---

## 三、EpollIoHandler.run() 与 NioIoHandler.run() 的差异

这是两种传输最核心的差异所在。两者虽然都实现了 `IoHandler` 接口的 `run(IoHandlerContext)` 方法，但内部实现路径截然不同。

### 3.1 NioIoHandler.run() 的执行路径

```
NioIoHandler.run(IoHandlerContext context)
│
├─ strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks)
│   ├─ 有任务？→ selectNow()（非阻塞）
│   └─ 无任务？→ SELECT（需要阻塞等待）
│
├─ select(context, oldWakenUp)
│   ├─ Selector.select(timeoutMillis)         ← 毫秒级精度
│   ├─ 空轮询检测：selectCnt++ > 512 ?
│   │   └─ rebuildSelector()                  ← 重建整个 Selector
│   └─ Selector.wakeup() 唤醒
│
├─ processSelectedKeys()
│   └─ selectedKeys.keys[i] → unsafe.read() / flush() / connect()
│       └─ 遍历 SelectedSelectionKeySet（数组优化后的 HashSet）
│
└─ runAllTasks(ioTime * (100 - ioRatio) / ioRatio)
```

关键特征：依赖 JDK Selector 抽象，通过反射优化 SelectionKey 集合，需要空轮询修复逻辑。

### 3.2 EpollIoHandler.run() 的执行路径

```
EpollIoHandler.run(IoHandlerContext context)
│
├─ strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks)
│   ├─ 有任务？→ epollWaitNow()（epoll_wait timeout=0）
│   ├─ BUSY_WAIT？→ epollBusyWait()（cpu_relax 自旋）
│   └─ SELECT？→ epollWait(context, deadlineNanos)
│
├─ epollWait 实现（4 种变体）
│   ├─ 毫秒超时：epoll_wait(epollFd, events, maxEvents, timeoutMs)
│   ├─ 纳秒超时+timerfd：timerfd_settime() + epoll_wait(-1)
│   ├─ busyWait0：循环调用 epoll_wait(timeout=0) + cpu_relax
│   └─ epoll_pwait2：直接传 struct timespec（Linux 5.11+）
│
├─ 唤醒机制
│   └─ eventfd_write(eventFd, 1)              ← 原子操作，极低开销
│
├─ 处理就绪事件
│   ├─ 遍历 epoll_event 数组（连续内存）
│   ├─ fd == eventFd？→ eventfd_read 消费唤醒信号
│   ├─ fd == timerFd？→ 忽略（仅用于打断等待）
│   └─ 用户 fd → channel.unsafe().epollInReady() / epollOutReady()
│
└─ runAllTasks(...)
```

### 3.3 核心差异对比

| 维度 | NioIoHandler | EpollIoHandler |
|------|-------------|----------------|
| IO 多路复用 | JDK Selector（间接） | 直接 JNI 调 epoll_wait |
| 文件描述符 | 1 个 Selector fd | 3 个：epollFd + eventFd + timerFd |
| 定时精度 | 毫秒 | 纳秒（timerfd / epoll_pwait2） |
| 空轮询修复 | 检测 + 重建 Selector | 无需（不存在此 Bug） |
| BUSY_WAIT | 不支持 | 支持（cpu_relax 自旋） |
| 唤醒方式 | Selector.wakeup()（pipe） | eventfd_write（原子写） |
| 事件遍历 | SelectedSelectionKeySet 数组 | struct epoll_event 原生数组 |
| JDK 依赖 | 强依赖 java.nio | 仅依赖 JNI |

### 3.4 三个原生 fd 的分工

EpollIoHandler 在初始化时创建三个文件描述符，各司其职：

```c
// epollFd：epoll 实例本身
int epollFd = epoll_create1(EPOLL_CLOEXEC);
// fallback: epoll_create(126) + fcntl(FD_CLOEXEC)

// eventFd：线程间唤醒
int eventFd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
// 注册到 epoll：EPOLLIN | EPOLLET（边缘触发）

// timerFd：纳秒级定时
int timerFd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
// 注册到 epoll：EPOLLIN | EPOLLET（边缘触发）
```

`eventFd` 和 `timerFd` 都使用边缘触发（ET），因为它们的语义是"通知一次即可"，不需要重复触发。而用户 Channel 对应的 fd 使用水平触发（LT），确保数据不会因为一次处理不完而丢失通知。

---

## 四、原生传输独有高级特性

这些特性是 JDK NIO 完全无法提供的，必须通过 JNI 直接调用 Linux/BSD 内核接口。

### 4.1 SO_REUSEPORT：多核端口复用

```java
ServerBootstrap b = new ServerBootstrap();
b.option(EpollChannelOption.SO_REUSEPORT, true);
```

`SO_REUSEPORT`（Linux 3.9+）允许多个 socket 绑定同一端口，由内核负责在它们之间均匀分发连接。这解决了传统 "thundering herd" 问题——当只有一个 accept socket 时，所有 worker 线程竞争同一个 accept 锁；而 `SO_REUSEPORT` 让每个线程拥有独立的 accept socket，内核通过一致性哈希分发，CPU 亲和性更优，减少锁竞争和缓存失效。

### 4.2 TCP_FASTOPEN：首包即数据

```java
// 服务端：在 SYN-ACK 阶段就携带数据
b.option(EpollChannelOption.TCP_FASTOPEN, 128); // 队列深度

// 客户端：在 SYN 阶段就发送数据
b.option(EpollChannelOption.TCP_FASTOPEN_CONNECT, true);
```

TCP Fast Open（RFC 7413）允许在 TCP 三次握手期间传输数据，消除了连接建立的一个 RTT 延迟。Netty 在 `doBind()` 中通过 `setsockopt(TCP_FASTOPEN)` 设置服务端队列深度，在 `doConnect0()` 中通过 `sendto(MSG_FASTOPEN)` 实现客户端 SYN+data。

### 4.3 Unix Domain Socket：进程间高速通信

```java
EventLoopGroup group = new MultiThreadIoEventLoopGroup(
    NThreads, EpollIoHandler.newFactory());

ServerBootstrap b = new ServerBootstrap();
b.group(group)
 .channel(EpollServerDomainSocketChannel.class)
 .childHandler(new MyHandler());
b.bind(new DomainSocketAddress("/tmp/my-app.sock"));
```

Unix Domain Socket 是同主机进程间通信的最佳选择：无需经过网络协议栈（无 TCP/IP 头部开销、无校验和计算、无拥塞控制），纯粹的内核内存拷贝。Netty 的实现还支持：

- **文件描述符传递**：通过 `FileDescriptorReadMode.FILE_DESCRIPTORS` 模式，进程间可以传递打开的 fd，实现真正的零拷贝文件传输
- **对端凭证获取**：`peerCredentials()` 返回连接对端的 uid/gid/pid，用于进程身份验证
- **VSock 支持**：虚拟机与宿主机之间的高效通信通道

### 4.4 splice：内核级零拷贝管道

`splice()` 系统调用在两个文件描述符之间移动数据，完全不经过用户空间。典型场景是代理服务器：从上游 socket 读取数据，直接 splice 到下游 socket，数据始终在内核 buffer 中流转。这比 `sendfile()` 更通用——sendfile 要求源必须是文件，而 splice 可以连接任意两个 fd（通过 pipe 中转）。

### 4.5 sendmmsg / recvmmsg：批量 UDP 收发

```java
// EpollDatagramChannel 内部实现
EpollChannelOption.MAX_DATAGRAM_PAYLOAD_SIZE
```

对于高吞吐 UDP 场景（DNS 服务器、游戏服务器），每个数据包一次系统调用的开销不可忽视。`sendmmsg` 允许一次系统调用发送多个数据报，`recvmmsg` 允许一次接收多个。Netty 的 `EpollDatagramChannel` 内部使用这些 API 实现批量收发，显著降低系统调用次数。

### 4.6 UDP GSO/GRO：硬件卸载加速

```java
b.option(EpollChannelOption.UDP_GRO, true);
// UDP_SEGMENT (GSO)：在发送侧将大 buffer 按 segment size 分片，由网卡硬件完成
```

Generic Segmentation Offload（GSO）和 Generic Receive Offload（GRO）将 UDP 分片工作下推到网卡硬件，减少 CPU 在协议栈中的处理时间。

### 4.7 其他 TCP 调优选项

| 选项 | 作用 |
|------|------|
| `TCP_CORK` | 将小包聚合，等待一段时间或 buffer 满后再发送（类似 Nagle 但更可控） |
| `TCP_QUICKACK` | 禁用延迟 ACK，立即确认（适合交互式应用） |
| `TCP_DEFER_ACCEPT` | 只有收到数据后才唤醒 accept，过滤空连接 |
| `TCP_USER_TIMEOUT` | 设置 TCP 重传超时，替代依赖 keepalive 的慢检测 |
| `TCP_KEEPIDLE/INTVL/CNT` | 精细控制 keepalive 探测间隔和次数 |
| `TCP_MD5SIG` | TCP MD5 签名，用于 BGP 路由器间防欺骗 |
| `IP_TRANSPARENT` | 透明代理，允许绑定非本机 IP |
| `IP_FREEBIND` | 允许绑定尚未分配给接口的 IP 地址 |

---

## 五、kqueue 与 epoll 差异及 Netty 抽象统一

### 5.1 系统调用层面的差异

epoll 和 kqueue 虽然都是高性能事件通知机制，但 API 设计理念差异显著：

```c
// --- Linux epoll ---
int epollFd = epoll_create1(EPOLL_CLOEXEC);
// 注册/修改/删除 分离的操作
epoll_ctl(epollFd, EPOLL_CTL_ADD, fd, &event);
epoll_ctl(epollFd, EPOLL_CTL_MOD, fd, &event);
epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, &event);
// 等待事件
int n = epoll_wait(epollFd, events, maxEvents, timeout);

// --- macOS/BSD kqueue ---
int kqFd = kqueue();
// 注册和等待使用同一个 API，通过 changelist 批量提交修改
int n = kevent(kqFd, changelist, nchanges, eventlist, nevents, &timeout);
// EV_ADD/EV_DELETE/EV_ENABLE/EV_DISABLE 在 flags 中指定
```

kqueue 的 `kevent()` 将"注册变更"和"等待事件"合并为一次系统调用，可以在等待的同时批量修改监听列表。而 epoll 需要分别调用 `epoll_ctl` 和 `epoll_wait`。

### 5.2 实现层面的差异对比

| 维度 | EpollIoHandler | KQueueIoHandler |
|------|---------------|-----------------|
| 创建 | `epoll_create1(EPOLL_CLOEXEC)` | `kqueue()` |
| 等待 | `epoll_wait(epollFd, ...)` | `kevent(kqFd, NULL, 0, events, ...)` |
| 唤醒 | `eventfd` 写入 | `EVFILT_USER` 触发 |
| 定时 | `timerfd` 注册到 epoll | `kevent` 的 timeout 参数 |
| BUSY_WAIT | 支持（cpu_relax 自旋） | 不支持 |
| 注册表 | `IntObjectHashMap<channel>`（fd→channel） | `LongObjectHashMap<channel>`（递增id→channel） |
| 最大超时 | 无限制（timerfd 独立管理） | 86399 秒（kevent timeout 限制） |
| 平台 | Linux 2.6+ | macOS、FreeBSD、OpenBSD |

### 5.3 唤醒机制的差异

epoll 使用 `eventfd`——一个专用的内核文件描述符，写入一个 8 字节整数即可唤醒阻塞的 `epoll_wait`。kqueue 使用 `EVFILT_USER`——一种用户自定义事件类型，通过 `kevent(kqFd, &userEvent, 1, NULL, 0, NULL)` 触发。两者都很高效，但 `EVFILT_USER` 不需要额外的文件描述符。

### 5.4 IoHandler 接口：统一抽象

Netty 通过 `IoHandler` 接口将 NIO、epoll、kqueue 三种实现统一在同一套编程模型下：

```java
public interface IoHandler {
    // 初始化
    void initialize();
    
    // 核心事件循环（由 EventLoop 线程调用）
    int run(IoHandlerContext context);
    
    // 注册 Channel（IoHandle）
    void register(IoHandle handle) throws Exception;
    
    // 注销 Channel
    void deregister(IoHandle handle) throws Exception;
    
    // 唤醒阻塞的 run()
    void wakeup();
    
    // 检查 IoHandle 兼容性
    boolean isCompatible(Class<? extends IoHandle> handleType);
    
    // 销毁
    void destroy();
}
```

每种传输只需实现这个接口，上层的 `SingleThreadIoEventLoop` 无需知道底层是 epoll、kqueue 还是 JDK NIO。工厂模式确保切换传输时只需替换工厂：

```java
// 三种实现，同一接口
IoHandlerFactory nioFactory = NioIoHandler.newFactory();
IoHandlerFactory epollFactory = EpollIoHandler.newFactory();
IoHandlerFactory kqueueFactory = KQueueIoHandler.newFactory();
```

### 5.5 注册表设计差异

EpollIoHandler 使用 `IntObjectHashMap<AbstractEpollChannel>`，key 是文件描述符（int），因为 Linux fd 是从 0 开始的小整数，直接用 fd 作为 key 效率最高。

KQueueIoHandler 使用 `LongObjectHashMap<AbstractKQueueChannel>`，key 是 Netty 内部分配的递增 id。这是因为 kqueue 的事件通过 `(ident, filter)` 二元组标识而非单一 fd，需要一个独立的映射键。

---

## 六、使用方式：只需替换三行代码

Netty 原生传输的设计目标之一是"对用户代码零侵入"。从 JDK NIO 切换到 epoll 原生传输，只需修改三处：

### 6.1 NIO 版本（标准写法）

```java
EventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(
    1, NioIoHandler.newFactory());
EventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(
    0, NioIoHandler.newFactory());

ServerBootstrap b = new ServerBootstrap();
b.group(bossGroup, workerGroup)
 .channel(NioServerSocketChannel.class)      // ← 第三处
 .childHandler(new ChannelInitializer<SocketChannel>() {
     @Override
     protected void initChannel(SocketChannel ch) {
         ch.pipeline().addLast(new MyHandler());
     }
 });
```

### 6.2 Epoll 版本（仅三行不同）

```java
EventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(
    1, EpollIoHandler.newFactory());           // ← 替换 1：IoHandler 工厂
EventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(
    0, EpollIoHandler.newFactory());           // ← 替换 2：IoHandler 工厂

ServerBootstrap b = new ServerBootstrap();
b.group(bossGroup, workerGroup)
 .channel(EpollServerSocketChannel.class)     // ← 替换 3：Channel 类型
 .childHandler(new ChannelInitializer<SocketChannel>() {
     @Override
     protected void initChannel(SocketChannel ch) {
         ch.pipeline().addLast(new MyHandler());
     }
 });
```

### 6.3 平台自适应模式

实际项目中通常通过条件判断实现跨平台兼容：

```java
IoHandlerFactory ioHandlerFactory;
Class<? extends ServerSocketChannel> channelClass;

if (Epoll.isAvailable()) {
    ioHandlerFactory = EpollIoHandler.newFactory();
    channelClass = EpollServerSocketChannel.class;
} else if (KQueue.isAvailable()) {
    ioHandlerFactory = KQueueIoHandler.newFactory();
    channelClass = KQueueServerSocketChannel.class;
} else {
    ioHandlerFactory = NioIoHandler.newFactory();
    channelClass = NioServerSocketChannel.class;
}

EventLoopGroup group = new MultiThreadIoEventLoopGroup(0, ioHandlerFactory);
ServerBootstrap b = new ServerBootstrap();
b.group(group).channel(channelClass);
```

### 6.4 Maven 依赖

```xml
<!-- Linux epoll -->
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-transport-native-epoll</artifactId>
    <classifier>linux-x86_64</classifier> <!-- 或 linux-aarch_64 -->
</dependency>

<!-- macOS kqueue -->
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-transport-native-kqueue</artifactId>
    <classifier>osx-x86_64</classifier> <!-- 或 osx-aarch_64 -->
</dependency>
```

原生传输以 classifier 区分平台架构，JNI .so/.dylib 文件打包在 JAR 中，运行时自动加载。

---

## 七、ET vs LT 设计决策

边缘触发（Edge-Triggered, ET）和水平触发（Level-Triggered, LT）是事件通知的两种模式，Netty 在 epoll 传输中做出了经过深思熟虑的混合选择。

### 7.1 两种模式的语义差异

- **LT（水平触发）**：只要 fd 处于就绪状态，每次 `epoll_wait` 都会返回该事件。类似"轮询检查"——数据在 buffer 中就持续通知
- **ET（边缘触发）**：仅在状态**变化**时通知一次。从未就绪变为就绪时触发，之后即使 buffer 中仍有数据也不再通知

### 7.2 Netty 的选择：用户 Channel 始终 LT

```java
// 用户 Channel 的事件掩码（无 EPOLLET 标志）
int events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLERR;
```

Netty 为用户 Channel 选择 LT 模式，核心原因是**安全性和简洁性**：

ET 模式要求每次通知后必须将数据完全读取干净（读到 `EAGAIN`），否则会永远错过后续通知。这带来几个问题：一是用户的 `channelRead()` 处理逻辑可能中途抛出异常导致读取不完整；二是如果单次读取的数据量很大，Pipeline 中可能需要多轮处理；三是背压（backpressure）机制变得复杂——LT 模式下只需移除 `EPOLLIN` 注册即可暂停读取，ET 模式下即使不注册也可能丢失事件。

Netty 早期版本曾有 `EpollMode` 枚举允许用户选择 ET/LT，但后来被标记为 `@Deprecated` 并移除了 ET 选项。这是实践验证后的决策：ET 模式带来的性能提升（减少 epoll_wait 返回次数）相比其引入的复杂性和潜在 bug 不值得。

### 7.3 内部 fd 使用 ET 的理由

```java
// eventFd 和 timerFd 注册时使用 EPOLLET
int internalEvents = EPOLLIN | EPOLLET;
```

`eventFd` 和 `timerFd` 使用 ET 模式则完全合理：

- **eventFd**：唤醒信号是"通知一次"语义，ET 天然匹配。即使多次写入也只需响应一次
- **timerFd**：定时器到期是一次性事件，触发后由 Netty 自己重设，不需要重复通知

这些内部 fd 的读取逻辑完全在 Netty 控制之内，不存在"读不干净"的风险。

### 7.4 性能影响分析

LT 模式在高并发下可能比 ET 多出一些 `epoll_wait` 返回（因为未读完的 fd 会重复就绪），但 Netty 通过两个机制缓解了这个问题：

- **autoRead 机制**：读取完成后如果 Channel 不想继续读，会主动 `epoll_ctl(MOD)` 移除 `EPOLLIN`，避免无意义的重复就绪
- **readMaxAttempts**：每次 read 事件最多读取固定次数，配合 LT 的重复通知自然实现分批读取

这种设计让用户代码不需要关心"一次性读完"的约束，大幅降低了使用门槛和 bug 可能性。

---

## 八、本篇涉及的设计模式

| 设计模式 | 应用场景 | 具体实现 |
|---------|---------|---------|
| **抽象工厂** | IoHandler 创建 | `EpollIoHandler.newFactory()` / `KQueueIoHandler.newFactory()` / `NioIoHandler.newFactory()` 返回统一的 `IoHandlerFactory`，客户端代码通过工厂创建具体实现，无需关心底层传输类型 |
| **策略模式** | 事件等待策略 | `SelectStrategy` 接口决定是 `SELECT`（阻塞等待）、`CONTINUE`（不阻塞直接返回）还是 `BUSY_WAIT`（自旋轮询），不同场景可插拔不同策略 |
| **模板方法** | Channel 生命周期 | `AbstractChannel` 定义了 `register → bind → read → write → close` 的骨架流程，`EpollSocketChannel` 和 `KQueueSocketChannel` 只需重写 JNI 调用的细节 |
| **适配器模式** | 统一 IO 模型 | `IoHandler` 接口将三种完全不同的系统 API（JDK Selector / epoll / kqueue）适配为统一的 `run() + register() + wakeup()` 协议 |
| **桥接模式** | 平台与传输分离 | Java 层（EpollSocketChannel）与 C 层（JNI native 方法）通过 `FileDescriptor` int 值桥接，平台实现变化不影响 Channel 接口 |
| **外观模式** | LinuxSocket 封装 | `LinuxSocket` 类将数十个 `setsockopt/getsockopt` JNI 调用封装为语义清晰的 Java 方法（如 `setTcpFastOpen()`、`setTcpCork()`），隐藏底层 C 结构体细节 |

---

## 九、本篇涉及的高性能并发技术

| 技术 | 应用位置 | 性能收益 |
|------|---------|---------|
| **JNI 直调系统调用** | EpollIoHandler / KQueueIoHandler 的所有 IO 操作 | 绕过 JDK 抽象层，消除 Selector 空轮询 bug、SelectionKey HashSet 开销、wakeup pipe 开销 |
| **eventfd 原子唤醒** | EpollIoHandler 的线程间通知 | 替代 pipe 或 Selector.wakeup()，单次原子写操作完成唤醒，无 pipe 缓冲区管理开销 |
| **timerfd 内核定时器** | EpollIoHandler 的纳秒级调度 | 利用内核高精度时钟，比用户态 sleep/轮询更精确且更节能 |
| **BUSY_WAIT + cpu_relax** | 极端低延迟场景 | 线程不进入内核等待，通过 `pause` 指令降低功耗同时保持亚微秒级响应 |
| **SO_REUSEPORT 多核分发** | 多 accept 线程场景 | 内核层面将连接分发到不同 socket，消除 accept 锁竞争，CPU 缓存亲和性提升 |
| **TCP_FASTOPEN 零 RTT** | 频繁短连接场景 | 三次握手期间即传输数据，每次连接节省一个 RTT 延迟 |
| **splice 零拷贝** | 代理/转发场景 | 数据在内核 buffer 间直接移动，不经过用户空间，消除两次 copy（kernel→user→kernel） |
| **sendmmsg/recvmmsg 批量 syscall** | 高吞吐 UDP 场景 | 一次系统调用处理多个数据报，将 syscall 开销从 O(n) 降至 O(1) |
| **UDP GSO/GRO 硬件卸载** | 大规模 UDP 发送/接收 | 协议分片工作下推到网卡硬件，释放 CPU 处理更多业务逻辑 |
| **IntObjectHashMap 替代 HashMap** | EpollIoHandler fd→channel 映射 | 避免 Integer 自动装箱，开放寻址法减少内存分配和 GC 压力，缓存友好的连续内存布局 |
| **连续内存事件数组** | epoll_event / kevent 结果遍历 | 就绪事件存储在连续 native 内存中，CPU 预取友好，比 JDK 的 SelectionKey 链表/树遍历效率高 |
| **ET + LT 混合触发策略** | 内部 fd 用 ET，用户 Channel 用 LT | 内部 fd 减少无意义通知，用户 Channel 确保数据不丢失，兼顾性能与正确性 |
