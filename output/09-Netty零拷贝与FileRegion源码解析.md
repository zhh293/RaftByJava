# Netty 零拷贝与 FileRegion 源码解析

> 基于 Netty 源码，深度解析零拷贝（Zero-Copy）技术在 Netty 中的两个层面实现。从操作系统层面的 sendfile 系统调用，到 Netty 应用层面的 CompositeByteBuf、slice()、duplicate() 等内存视图技术，逐层揭示 Netty 如何在文件传输和数据拼装两大场景下，将不必要的数据拷贝降到最低。全文以数据流图标注每次拷贝和上下文切换的位置，直观对比传统 IO、sendfile、sendfile+DMA gather 三种方案的差异。

---

## 一、什么是零拷贝

零拷贝并不是"完全不拷贝数据"，而是"避免 CPU 参与不必要的数据拷贝"。要理解零拷贝，必须先理解传统 IO 在发送文件时到底做了什么。

### 1.1 传统 IO 的 4 次拷贝 + 4 次上下文切换

当你用传统的 `read()` + `write()` 方式将一个文件内容发送到网络时，数据在内核态和用户态之间来回搬运，总共发生 4 次拷贝和 4 次上下文切换：

```
                     传统 IO 的数据拷贝路径
                     =====================

 应用程序            用户态缓冲区 (byte[])
    │                     ▲         │
    │ read()              │         │ write()
    │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
    │     上下文切换①②     │         │    上下文切换③④
    ▼                     │         ▼
 ┌─────────────┐   拷贝②  │  拷贝③  ┌─────────────────┐
 │ 内核读缓冲区 │──────────┘ ┌──────▶│  Socket发送缓冲区 │
 │ (PageCache) │            │      └────────┬────────┘
 └──────┬──────┘            │               │
        │                   │               │
  拷贝① │ DMA Copy          │         拷贝④ │ DMA Copy
        │                   │               │
 ┌──────┴──────┐            │      ┌────────┴────────┐
 │   磁盘控制器  │            │      │   网卡控制器(NIC) │
 └─────────────┘            │      └─────────────────┘
                            │
                    CPU 参与拷贝

 4次拷贝: ① 磁盘→内核缓冲区(DMA)  ② 内核缓冲区→用户缓冲区(CPU)
          ③ 用户缓冲区→Socket缓冲区(CPU)  ④ Socket缓冲区→网卡(DMA)
 4次上下文切换: read()调用→内核态, read()返回→用户态,
               write()调用→内核态, write()返回→用户态
```

其中，拷贝②和拷贝③完全是多余的——数据从内核读缓冲区拷贝到用户态，只是为了让应用程序"看一眼"，然后又原封不动地拷回内核的 Socket 发送缓冲区。CPU 在这两次拷贝中做的是纯搬运工作，毫无增值。

### 1.2 sendfile 的 2 次拷贝 + 2 次上下文切换

Linux 2.1 引入了 `sendfile()` 系统调用。它允许数据直接从内核的读缓冲区（PageCache）拷贝到 Socket 发送缓冲区，绕过用户态，省去了拷贝②和拷贝③：

```
                     sendfile 的数据拷贝路径
                     =======================

 应用程序
    │
    │ sendfile(out_fd, in_fd, offset, count)
    │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
    │        上下文切换①②（仅1次系统调用）
    ▼
 ┌─────────────┐         拷贝②        ┌─────────────────┐
 │ 内核读缓冲区 │───────(CPU拷贝)──────▶│  Socket发送缓冲区 │
 │ (PageCache) │                      └────────┬────────┘
 └──────┬──────┘                               │
        │                                      │
  拷贝① │ DMA Copy                       拷贝③ │ DMA Copy
        │                                      │
 ┌──────┴──────┐                      ┌────────┴────────┐
 │   磁盘控制器  │                      │   网卡控制器(NIC) │
 └─────────────┘                      └─────────────────┘

 3次拷贝: ① 磁盘→内核缓冲区(DMA)  ② 内核缓冲区→Socket缓冲区(CPU)
          ③ Socket缓冲区→网卡(DMA)
 2次上下文切换: sendfile()调用→内核态, sendfile()返回→用户态
```

严格来说，sendfile 将拷贝次数从 4 次减少到 3 次（2 次 DMA + 1 次 CPU），上下文切换从 4 次减少到 2 次。相比传统 IO，已经减少了一次 CPU 拷贝和两次上下文切换。

### 1.3 sendfile + DMA Gather 的真正零 CPU 拷贝

Linux 2.4 之后，如果网卡支持 DMA Gather（Scatter-Gather DMA），sendfile 可以进一步优化：内核不再将数据从 PageCache 拷贝到 Socket 缓冲区，而是只将文件描述符信息（内存地址、偏移量、长度）写入 Socket 缓冲区，网卡的 DMA 引擎直接从 PageCache 收集（gather）数据发送出去：

```
               sendfile + DMA Gather 的数据路径
               =================================

 应用程序
    │
    │ sendfile(out_fd, in_fd, offset, count)
    │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
    │        上下文切换①②（仅1次系统调用）
    ▼
 ┌─────────────┐   只传递fd/offset/len  ┌─────────────────┐
 │ 内核读缓冲区 │─────(描述符信息)───────▶│  Socket发送缓冲区 │
 │ (PageCache) │─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
 └──────┬──────┘                       └────────────────┘ │
        │                                                  │
  拷贝① │ DMA Copy                           DMA Gather拷贝②
        │                                                  │
 ┌──────┴──────┐                              ┌───────────┴─┐
 │   磁盘控制器  │                              │ 网卡控制器(NIC)│
 └─────────────┘                              └─────────────┘

 2次拷贝: ① 磁盘→内核缓冲区(DMA)  ② 内核缓冲区→网卡(DMA Gather)
 0次CPU拷贝！
 2次上下文切换: sendfile()调用→内核态, sendfile()返回→用户态
```

这才是真正意义上的"零拷贝"——CPU 全程不参与数据搬运，所有拷贝都由 DMA 引擎完成。CPU 只需要在 sendfile 系统调用时设置好描述符信息即可。

### 1.4 三种方案的对比

```
 ┌─────────────────┬──────────┬───────────┬──────────────┐
 │     方案         │ CPU拷贝  │ DMA拷贝   │ 上下文切换    │
 ├─────────────────┼──────────┼───────────┼──────────────┤
 │ 传统 read+write  │   2次    │   2次     │    4次       │
 │ sendfile         │   1次    │   2次     │    2次       │
 │ sendfile+Gather  │   0次    │   2次     │    2次       │
 └─────────────────┴──────────┴───────────┴──────────────┘
```

JDK 的 `FileChannel.transferTo()` 方法在底层就是通过 sendfile 系统调用实现的。如果操作系统和网卡支持 DMA Gather，则自动走最优路径。Netty 的 `FileRegion` 正是对 `FileChannel.transferTo()` 的封装。

---

## 二、Netty 零拷贝的两个层面

Netty 的零拷贝技术分为两个层面，理解这一点是掌握本章的关键：

```
 ┌───────────────────────────────────────────────────────────┐
 │                  Netty 的零拷贝体系                       │
 ├───────────────────────────────────────────────────────────┤
 │                                                           │
 │  层面一: OS 级零拷贝（文件传输）                            │
 │  ┌─────────────────────────────────────────────────────┐  │
 │  │  FileRegion / DefaultFileRegion                     │  │
 │  │       │                                             │  │
 │  │       ├─ NIO传输: FileChannel.transferTo() → sendfile│  │
 │  │       │    └─ NioSocketChannel.doWriteFileRegion()  │  │
 │  │       │                                             │  │
 │  │       └─ Epoll传输: JNI直接调 sendfile()             │  │
 │  │            └─ LinuxSocket.sendFile() → native C     │  │
 │  └─────────────────────────────────────────────────────┘  │
 │                                                           │
 │  层面二: 应用级零拷贝（内存操作）                            │
 │  ┌─────────────────────────────────────────────────────┐  │
 │  │  CompositeByteBuf — 逻辑合并多个 ByteBuf 无需拷贝     │  │
 │  │  slice()          — 共享底层内存的子视图              │  │
 │  │  duplicate()      — 共享底层内存的完整视图            │  │
 │  │  wrappedBuffer()  — 直接引用 byte[]/ByteBuffer       │  │
 │  │  Direct ByteBuffer — 避免 JVM 堆到直接内存的拷贝     │  │
 │  └─────────────────────────────────────────────────────┘  │
 │                                                           │
 └───────────────────────────────────────────────────────────┘
```

层面一解决的是**文件到网络**的传输效率问题，依赖操作系统内核的 sendfile 支持。层面二解决的是**内存到内存**的数据拼装效率问题，这是 Netty 在 JVM 层面独创的设计。

---

## 三、OS 级零拷贝：FileRegion 源码全流程

### 3.1 FileRegion 接口定义

FileRegion 是 Netty 对操作系统零拷贝能力的顶层抽象，它继承自 `ReferenceCounted`，表示一个可以通过零拷贝方式发送的文件区域：

```java
// FileRegion.java
public interface FileRegion extends ReferenceCounted {

    // 文件中开始传输的偏移量
    long position();

    // 已传输的字节数
    long transferred();

    // 待传输的总字节数
    long count();

    // 核心方法：将文件内容传输到目标 Channel
    // position 是相对于 position() 的偏移
    long transferTo(WritableByteChannel target, long position) throws IOException;
}
```

接口设计非常精简，只有四个核心方法。其中 `transferTo()` 是零拷贝的入口点——它的实现会最终调用操作系统的 sendfile 系统调用。值得注意的是，`position` 参数是**相对偏移**，实际的文件偏移是 `position() + position`，这在 DefaultFileRegion 的实现中会看到。

### 3.2 DefaultFileRegion：两种构造与懒加载

DefaultFileRegion 是 FileRegion 的默认实现，继承自 `AbstractReferenceCounted`。它提供了两种构造方式——直接传入 FileChannel 或传入 File 对象延迟打开：

```java
// DefaultFileRegion.java
public class DefaultFileRegion extends AbstractReferenceCounted implements FileRegion {

    private final File f;              // 文件引用（懒加载模式使用）
    private final long position;       // 起始偏移
    private final long count;          // 传输字节数
    private long transferred;          // 已传输字节数
    private FileChannel file;          // 底层 FileChannel

    // 构造方式一：直接传入已打开的 FileChannel
    public DefaultFileRegion(FileChannel fileChannel, long position, long count) {
        this.file = ObjectUtil.checkNotNull(fileChannel, "fileChannel");
        this.position = checkPositiveOrZero(position, "position");
        this.count = checkPositiveOrZero(count, "count");
        this.f = null;                 // 不需要 File 引用
    }

    // 构造方式二：传入 File，延迟到 transferTo() 时才打开
    public DefaultFileRegion(File file, long position, long count) {
        this.f = ObjectUtil.checkNotNull(file, "file");
        this.position = checkPositiveOrZero(position, "position");
        this.count = checkPositiveOrZero(count, "count");
        // 注意：this.file 此时为 null，延迟到 open() 时才赋值
    }
}
```

两种构造方式的区别在于 FileChannel 的生命周期管理。构造方式一适用于调用方已经持有 FileChannel 的场景，构造方式二适用于只知道文件路径的场景——FileChannel 会在第一次调用 `transferTo()` 时通过 `open()` 方法懒加载创建：

```java
// DefaultFileRegion.java
public void open() throws IOException {
    if (!isOpen() && refCnt() > 0) {
        // 只在尚未打开且引用计数大于0时打开
        file = new RandomAccessFile(f, "r").getChannel();
    }
}
```

这种懒加载设计的好处在于：如果你创建了一个 DefaultFileRegion 但在 write 之前就因为某种原因被释放了（比如连接断开），就不会浪费文件描述符资源。

### 3.3 transferTo()：零拷贝的核心方法

`transferTo()` 是整个零拷贝机制的核心。它将文件数据通过 JDK 的 `FileChannel.transferTo()` 方法传输到目标 Channel，底层触发操作系统的 sendfile 系统调用：

```java
// DefaultFileRegion.java
@Override
public long transferTo(WritableByteChannel target, long position) throws IOException {
    long count = this.count - position;
    if (count < 0 || position < 0) {
        throw new IllegalArgumentException(
                "position out of range: " + position +
                " (expected: 0 - " + (this.count - 1) + ')');
    }
    if (count == 0) {
        return 0L;
    }
    if (refCnt() == 0) {
        throw new IllegalReferenceCountException(0);
    }

    // 懒加载：确保 FileChannel 已打开
    open();

    // 核心调用！JDK FileChannel.transferTo() → 操作系统 sendfile()
    long written = file.transferTo(this.position + position, count, target);

    if (written > 0) {
        transferred += written;        // 累计已传输字节数
    } else if (written == 0) {
        // 防御性检查：如果写入0字节，检查文件是否被截断
        // See https://github.com/netty/netty/issues/8868
        validate(this, position);
    }
    return written;
}
```

这里有几个关键细节值得深入分析。首先是 `file.transferTo(this.position + position, count, target)` 这行代码：第一个参数 `this.position + position` 是文件的绝对偏移——`this.position` 是 DefaultFileRegion 创建时指定的起始位置，`position` 是本次传输的相对偏移（通常等于 `transferred()`）。

其次是 `written == 0` 时的防御性检查。这是针对一个真实 Bug（netty#8868）的修复：当文件在传输过程中被外部程序截断（truncate）时，`FileChannel.transferTo()` 不会抛异常，而是静默返回 0。如果不加检查，Netty 会陷入死循环——一直尝试传输，一直返回 0。validate() 方法通过比较文件实际大小来检测这种情况：

```java
// DefaultFileRegion.java
static void validate(DefaultFileRegion region, long position) throws IOException {
    long size = region.file.size();
    long count = region.count - position;
    if (region.position + count + position > size) {
        throw new IOException("Underlying file size " + size +
                " smaller then requested count " + region.count);
    }
}
```

最后是 `deallocate()` 方法，它在引用计数归零时被调用，负责关闭底层 FileChannel：

```java
// DefaultFileRegion.java
@Override
protected void deallocate() {
    FileChannel file = this.file;
    if (file == null) {
        return;
    }
    this.file = null;
    try {
        file.close();
    } catch (IOException e) {
        logger.warn("Failed to close a file.", e);
    }
}
```

### 3.4 NioSocketChannel 如何调用 FileRegion

理解了 DefaultFileRegion 的内部实现后，我们来看 NioSocketChannel 是如何驱动 FileRegion 完成零拷贝写入的。整个调用链如下：

```
 ctx.write(new DefaultFileRegion(file, 0, fileLength))
    │
    ▼
 HeadContext.write() → AbstractUnsafe.write()
    │
    ├─ filterOutboundMessage(msg)
    │   └─ msg instanceof FileRegion → 直接放行，不做转换
    │
    └─ outboundBuffer.addMessage(msg)  → 加入写缓冲区
    
 ctx.flush()
    │
    ▼
 AbstractUnsafe.flush() → flush0() → doWrite(outboundBuffer)
    │
    ▼
 NioSocketChannel.doWrite()
    │
    ├─ nioBuffers = in.nioBuffers()
    │   └─ nioBufferCnt = 0  （FileRegion 没有 NIO Buffer）
    │
    └─ switch (nioBufferCnt) {
           case 0:  // 非 ByteBuf 消息，走 doWrite0
               writeSpinCount -= doWrite0(in);
       }
    │
    ▼
 AbstractNioByteChannel.doWrite0(in)
    │
    └─ doWriteInternal(in, msg)
        │
        └─ msg instanceof FileRegion
            │
            ├─ region.transferred() >= region.count() → in.remove() (已完成)
            │
            └─ doWriteFileRegion(region)  ← 子类实现
    │
    ▼
 NioSocketChannel.doWriteFileRegion()
    │
    └─ region.transferTo(javaChannel(), position)
        │
        └─ FileChannel.transferTo() → sendfile()  ← 操作系统零拷贝！
```

关键在 `NioSocketChannel.doWrite()` 方法的 switch-case 分支。当 `nioBufferCnt == 0` 时，说明当前要写入的消息不是 ByteBuf（而是 FileRegion），此时走 `doWrite0()` 路径：

```java
// NioSocketChannel.java
@Override
protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    SocketChannel ch = javaChannel();
    int writeSpinCount = config().getWriteSpinCount();
    do {
        if (in.isEmpty()) {
            clearOpWrite();
            return;
        }
        int maxBytesPerGatheringWrite = ((NioSocketChannelConfig) config).getMaxBytesPerGatheringWrite();
        ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
        int nioBufferCnt = in.nioBufferCount();

        switch (nioBufferCnt) {
            case 0:
                // 非 ByteBuf 消息（如 FileRegion），走 doWrite0
                writeSpinCount -= doWrite0(in);
                break;
            case 1: { /* 单 ByteBuf 写入 */ break; }
            default: { /* gathering write 写入 */ break; }
        }
    } while (writeSpinCount > 0);
    incompleteWrite(writeSpinCount < 0);
}
```

`doWriteFileRegion()` 方法本身非常简洁——只有两行代码：

```java
// NioSocketChannel.java
@Override
protected long doWriteFileRegion(FileRegion region) throws Exception {
    final long position = region.transferred();
    return region.transferTo(javaChannel(), position);
}
```

它取出已传输的字节数作为位置参数，然后调用 `region.transferTo()`，将数据通过 JDK 的 NIO SocketChannel 传输。`javaChannel()` 返回的是底层 JDK 的 `java.nio.channels.SocketChannel`，它实现了 `WritableByteChannel` 接口，因此可以作为 `FileChannel.transferTo()` 的目标。

另外要注意 `filterOutboundMessage()` 对 FileRegion 的特殊处理——在 `AbstractNioByteChannel` 中，FileRegion 直接放行不做任何转换，而堆内存的 ByteBuf 会被转换为直接内存 ByteBuf：

```java
// AbstractNioByteChannel.java
@Override
protected final Object filterOutboundMessage(Object msg) {
    if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        if (buf.isDirect()) {
            return msg;
        }
        return newDirectBuffer(buf);     // 堆 ByteBuf → 直接 ByteBuf
    }
    if (msg instanceof FileRegion) {
        return msg;                      // FileRegion 直接放行
    }
    throw new UnsupportedOperationException(
            "unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
}
```

### 3.5 Epoll 传输：JNI 直接调用 sendfile()

NIO 传输通过 JDK 的 `FileChannel.transferTo()` 间接调用 sendfile，中间经过了 JDK 的一层封装。Netty 的 Epoll 原生传输则更加激进——通过 JNI 直接调用 Linux 的 sendfile() 系统调用，跳过 JDK 的中间层。

在 `AbstractEpollStreamChannel.doWriteSingle()` 中，Netty 对 DefaultFileRegion 和普通 FileRegion 做了区分处理：

```java
// AbstractEpollStreamChannel.java
@Override
protected int doWriteSingle(ChannelOutboundBuffer in) throws Exception {
    Object msg = in.current();
    if (msg instanceof ByteBuf) {
        return writeBytes(in, (ByteBuf) msg);
    } else if (msg instanceof DefaultFileRegion) {
        return writeDefaultFileRegion(in, (DefaultFileRegion) msg);   // 优化路径
    } else if (msg instanceof FileRegion) {
        return writeFileRegion(in, (FileRegion) msg);                 // 回退路径
    } else if (msg instanceof SpliceOutTask) {
        // ...
    }
    // ...
}
```

对于 DefaultFileRegion，走 `writeDefaultFileRegion()` 优化路径，直接通过 JNI 调用 sendfile：

```java
// AbstractEpollStreamChannel.java
private int writeDefaultFileRegion(ChannelOutboundBuffer in,
                                   DefaultFileRegion region) throws Exception {
    final long offset = region.transferred();
    final long regionCount = region.count();
    if (offset >= regionCount) {
        in.remove();
        return 0;
    }

    // 核心：通过 LinuxSocket.sendFile() 直接调用 sendfile 系统调用
    final long flushedAmount = socket.sendFile(region, region.position(), offset,
                                               regionCount - offset);
    if (flushedAmount > 0) {
        in.progress(flushedAmount);
        if (region.transferred() >= regionCount) {
            in.remove();
        }
        return 1;
    } else if (flushedAmount == 0) {
        validateFileRegion(region, offset);  // 同样的截断检查
    }
    return WRITE_STATUS_SNDBUF_FULL;
}
```

`LinuxSocket.sendFile()` 的 Java 侧代码会先调用 `open()` 确保 FileChannel 已打开，然后调用 JNI native 方法：

```java
// LinuxSocket.java
long sendFile(DefaultFileRegion src, long baseOffset,
              long offset, long length) throws IOException {
    src.open();    // 确保 FileChannel 已打开
    long res = sendFile(intValue(), src, baseOffset, offset, length);
    if (res >= 0) {
        return res;
    }
    return ioResult("sendfile", (int) res);
}

private static native long sendFile(int socketFd, DefaultFileRegion src,
                                    long baseOffset, long offset, long length)
                                    throws IOException;
```

在 C 层面，JNI 函数直接调用 Linux 的 sendfile 系统调用：

```c
// netty_epoll_linuxsocket.c
#include <sys/sendfile.h>

// JNI native 方法实现
ssize_t res;
off_t offset = base_off + off;
int err;
do {
    res = sendfile(fd, srcFd, &offset, (size_t) len);
} while (res == -1 && ((err = errno) == EINTR));
// EINTR 处理：被信号中断时自动重试

if (res < 0) {
    return -err;
}
if (res > 0) {
    // 直接通过 JNI 更新 DefaultFileRegion 的 transferred 字段
    (*env)->SetLongField(env, fileRegion, transferredFieldId, off + res);
}
return res;
```

这段 C 代码有两个值得注意的细节。第一，`do-while` 循环处理 `EINTR`（系统调用被信号中断）——这在高并发服务器中是常见的，信号处理（如 SIGPROF 用于 profiling）可能随时中断 sendfile。第二，通过 `SetLongField` 直接修改 Java 对象的 `transferred` 字段，避免了一次 JNI 回调的开销。

对于非 DefaultFileRegion 的实现（即自定义 FileRegion 子类），Epoll 传输会回退到通用的 `transferTo(WritableByteChannel)` 方式，通过 `EpollSocketWritableByteChannel` 桥接：

```java
// AbstractEpollStreamChannel.java
private int writeFileRegion(ChannelOutboundBuffer in, FileRegion region) throws Exception {
    if (region.transferred() >= region.count()) {
        in.remove();
        return 0;
    }
    if (byteChannel == null) {
        byteChannel = new EpollSocketWritableByteChannel();
    }
    final long flushedAmount = region.transferTo(byteChannel, region.transferred());
    // ...
}
```

两条路径的区别可以总结为：

```
 FileRegion 在 Epoll 传输中的调用路径
 ====================================

 msg instanceof DefaultFileRegion?
    │
    ├─ YES → writeDefaultFileRegion()
    │         └─ LinuxSocket.sendFile()
    │              └─ JNI: sendfile(socketFd, fileFd, &offset, len)
    │                   └─ 直接系统调用，零中间层
    │
    └─ NO  → writeFileRegion()
              └─ region.transferTo(EpollSocketWritableByteChannel)
                   └─ 走 JDK FileChannel.transferTo() 路径
                        └─ 间接调用 sendfile，多一层 JDK 封装
```

---

## 四、应用级零拷贝：内存视图技术

OS 级零拷贝解决的是文件到网络的传输问题。但在日常开发中，更常见的场景是在内存中拼装、切分、合并数据包——比如协议头+协议体的拼装、一个大包拆成多个小包等。Netty 通过一系列内存视图技术，让这些操作在无需内存拷贝的情况下完成。

### 4.1 CompositeByteBuf：逻辑合并无需拷贝

CompositeByteBuf 是 Netty 应用级零拷贝中最重要的设计。它将多个 ByteBuf 逻辑上组合成一个连续的 ByteBuf，但底层各个 ByteBuf 仍然保持独立，不发生任何数据拷贝：

```
                  传统方式：合并两个 ByteBuf
                  ==========================

 ByteBuf header (8字节)     ByteBuf body (1024字节)
 ┌────────────────────┐     ┌──────────────────────────────┐
 │ H E A D E R _ _    │     │ B O D Y . . . . . . . . .   │
 └────────────────────┘     └──────────────────────────────┘
          │                              │
          │        拷贝 (CPU参与)         │
          ▼                              ▼
 ┌──────────────────────────────────────────────────────────┐
 │ H E A D E R _ _ B O D Y . . . . . . . . .              │
 └──────────────────────────────────────────────────────────┘
 新分配 1032字节的 ByteBuf，两次 memcpy


                  CompositeByteBuf：零拷贝合并
                  ============================

 ByteBuf header (8字节)     ByteBuf body (1024字节)
 ┌────────────────────┐     ┌──────────────────────────────┐
 │ H E A D E R _ _    │     │ B O D Y . . . . . . . . .   │
 └─────────┬──────────┘     └──────────────┬───────────────┘
           │                               │
           │  Component[0]                 │  Component[1]
           │  offset=0                     │  offset=8
           │  endOffset=8                  │  endOffset=1032
           ▼                               ▼
 ┌──────────────────────────────────────────────────────────┐
 │               CompositeByteBuf (逻辑视图)                │
 │            capacity=1032, 不分配新内存                    │
 └──────────────────────────────────────────────────────────┘
```

#### 4.1.1 Component 内部类：桥接逻辑偏移与物理偏移

CompositeByteBuf 内部用 `Component` 数组维护每个子 ByteBuf 的元信息：

```java
// CompositeByteBuf.java 内部类
private static final class Component {
    final ByteBuf srcBuf;      // 原始添加的 ByteBuf
    final ByteBuf buf;         // srcBuf 解包后的底层 ByteBuf

    int srcAdjustment;         // CompositeByteBuf 索引 → srcBuf 索引的偏移量
    int adjustment;            // CompositeByteBuf 索引 → buf 索引的偏移量

    int offset;                // 此 Component 在 CompositeByteBuf 中的起始偏移
    int endOffset;             // 此 Component 在 CompositeByteBuf 中的结束偏移

    private ByteBuf slice;     // 缓存的 slice 视图

    Component(ByteBuf srcBuf, int srcOffset, ByteBuf buf, int bufOffset,
              int offset, int len, ByteBuf slice) {
        this.srcBuf = srcBuf;
        this.srcAdjustment = srcOffset - offset;   // 关键：偏移量差值
        this.buf = buf;
        this.adjustment = bufOffset - offset;
        this.offset = offset;
        this.endOffset = offset + len;
        this.slice = slice;
    }

    // 将 CompositeByteBuf 的索引转换为 srcBuf 的索引
    int srcIdx(int index) {
        return index + srcAdjustment;
    }

    // 将 CompositeByteBuf 的索引转换为底层 buf 的索引
    int idx(int index) {
        return index + adjustment;
    }

    int length() {
        return endOffset - offset;
    }
}
```

`srcAdjustment` 和 `adjustment` 的设计非常巧妙。它们将 CompositeByteBuf 的逻辑索引转换为底层 ByteBuf 的物理索引，转换公式为 `物理索引 = 逻辑索引 + adjustment`。这样读取 CompositeByteBuf 的第 N 个字节时，只需要先通过二分查找定位到第几个 Component，然后用 adjustment 计算出底层 ByteBuf 的真实索引，无需任何数据拷贝。

#### 4.1.2 newComponent()：解包外层包装

当向 CompositeByteBuf 添加子 ByteBuf 时，`newComponent()` 方法会逐层解包外部包装，直到拿到真正的底层 ByteBuf：

```java
// CompositeByteBuf.java
private Component newComponent(final ByteBuf buf, final int offset) {
    final int srcIndex = buf.readerIndex();
    final int len = buf.readableBytes();

    // 第一步：剥离 WrappedByteBuf、SwappedByteBuf 等装饰器
    ByteBuf unwrapped = buf;
    int unwrappedIndex = srcIndex;
    while (unwrapped instanceof WrappedByteBuf || unwrapped instanceof SwappedByteBuf) {
        unwrapped = unwrapped.unwrap();
    }

    // 第二步：剥离 SlicedByteBuf，累加偏移
    if (unwrapped instanceof AbstractUnpooledSlicedByteBuf) {
        unwrappedIndex += ((AbstractUnpooledSlicedByteBuf) unwrapped).idx(0);
        unwrapped = unwrapped.unwrap();
    } else if (unwrapped instanceof PooledSlicedByteBuf) {
        unwrappedIndex += ((PooledSlicedByteBuf) unwrapped).adjustment;
        unwrapped = unwrapped.unwrap();
    } else if (unwrapped instanceof DuplicatedByteBuf
               || unwrapped instanceof PooledDuplicatedByteBuf) {
        unwrapped = unwrapped.unwrap();
    }

    return new Component(buf.order(ByteOrder.BIG_ENDIAN), srcIndex,
            unwrapped.order(ByteOrder.BIG_ENDIAN), unwrappedIndex, offset, len, slice);
}
```

为什么要解包？因为 CompositeByteBuf 在做 nioBuffers()（gathering write）时需要拿到底层真正的 `ByteBuffer`，如果不解包，每一层包装都可能引入额外的间接调用开销。解包后，srcBuf 保留原始引用用于引用计数管理，buf 指向真正的底层 ByteBuf 用于数据访问。

#### 4.1.3 consolidateIfNeeded()：自动合并防碎片化

当 Component 数量超过 `maxNumComponents` 阈值时，CompositeByteBuf 会自动将所有 Component 合并（consolidate）为一个连续的 ByteBuf。这是一个权衡——过多的 Component 会增加每次读取时的二分查找开销和 gathering write 时的系统调用参数数量：

```java
// CompositeByteBuf.java
private void consolidateIfNeeded() {
    int size = componentCount;
    if (size > maxNumComponents) {
        consolidate0(0, size);    // 将所有 Component 合并为一个
    }
}
```

默认的 `maxNumComponents` 在 `ByteBufAllocator.compositeBuffer()` 中通常为 16。这意味着当你向一个 CompositeByteBuf 中添加超过 16 个子 ByteBuf 时，Netty 会分配一块连续内存，将所有数据拷贝过去——此时就不再是零拷贝了。这是性能权衡的体现：少量的 Component 使用逻辑视图更高效，大量的 Component 不如合并为一块连续内存。

#### 4.1.4 nioBuffers()：支持 Gathering Write

CompositeByteBuf 的 `nioBuffers()` 方法返回底层所有 Component 的 `ByteBuffer` 数组，直接用于 NIO 的 gathering write（`SocketChannel.write(ByteBuffer[])`），一次系统调用写入多个不连续的缓冲区：

```java
// CompositeByteBuf.java
@Override
public ByteBuffer[] nioBuffers(int index, int length) {
    checkIndex(index, length);
    if (length == 0) {
        return new ByteBuffer[] { EMPTY_NIO_BUFFER };
    }

    RecyclableArrayList buffers = RecyclableArrayList.newInstance(componentCount);
    try {
        int i = toComponentIndex0(index);   // 二分查找定位起始 Component
        while (length > 0) {
            Component c = components[i];
            ByteBuf s = c.buf;
            int localLength = Math.min(length, c.endOffset - index);
            switch (s.nioBufferCount()) {
                case 0:
                    throw new UnsupportedOperationException();
                case 1:
                    buffers.add(s.nioBuffer(c.idx(index), localLength));
                    break;
                default:
                    // 嵌套 CompositeByteBuf 的情况
                    Collections.addAll(buffers, s.nioBuffers(c.idx(index), localLength));
            }
            index += localLength;
            length -= localLength;
            i++;
        }
        return buffers.toArray(EmptyArrays.EMPTY_BYTE_BUFFERS);
    } finally {
        buffers.recycle();
    }
}
```

这个方法在 `NioSocketChannel.doWrite()` 中被调用：当 `nioBufferCnt > 1` 时，Netty 使用 `ch.write(nioBuffers, 0, nioBufferCnt)` 进行 gathering write，一次系统调用将多个 ByteBuffer 的数据写入 Socket——这本身就是操作系统层面的零拷贝技术（writev 系统调用）。

### 4.2 slice()：共享内存的子区域视图

`slice()` 创建一个共享底层内存的子视图 ByteBuf，它有独立的 readerIndex 和 writerIndex，但和原始 ByteBuf 共享同一块底层内存：

```
                    slice() 的内存模型
                    ==================

 原始 ByteBuf (capacity=1024)
 ┌─────────────────────────────────────────────────────┐
 │  0       128      256      512              1024    │
 │  ├────────┼────────┼────────┼─────────────────┤     │
 │  │ header │ body-1 │ body-2 │     unused      │     │
 └──┼────────┼────────┼────────┼─────────────────┼─────┘
    │        │                 │
    │        │   slice(128,384)│
    │        │                 │
    ▼        ▼                 ▼
 ┌───────────────────────────────┐
 │ SlicedByteBuf                │
 │ adjustment = 128             │
 │ readerIndex = 0              │  ← 独立的读写索引
 │ writerIndex = 384            │
 │ capacity = 384               │
 │ 底层内存 = 原始ByteBuf的内存   │  ← 共享内存！
 └───────────────────────────────┘

 修改 SlicedByteBuf[0] 等同于修改原始ByteBuf[128]
```

SlicedByteBuf 通过 `adjustment` 字段记录偏移，将自身的索引映射到底层 ByteBuf 的索引。嵌套 slice 时（对一个 SlicedByteBuf 再次 slice），Netty 会解包到最底层的 ByteBuf，避免多层间接访问的性能损耗。

### 4.3 duplicate()：共享内存的完整视图

`duplicate()` 和 `slice()` 类似，也是创建一个共享底层内存的视图，但它保留了原始 ByteBuf 的全部容量。它有独立的 readerIndex 和 writerIndex，没有偏移量（adjustment = 0）：

```
                    duplicate() 的内存模型
                    ======================

 原始 ByteBuf
 ┌────────────────────────────────────────────┐
 │  readerIndex=100     writerIndex=500       │
 │  ├──────────┼────────────┼─────────────┤   │
 │  │ discarded│  readable  │  writable   │   │
 └──┼──────────┼────────────┼─────────────┼───┘
    │                                      │
    │         duplicate()                  │
    │                                      │
    ▼                                      ▼
 ┌────────────────────────────────────────────┐
 │ DuplicatedByteBuf                         │
 │ readerIndex=100     writerIndex=500        │  ← 初始值相同
 │ 但可以独立修改 readerIndex/writerIndex      │
 │ 底层内存 = 原始ByteBuf的内存                │  ← 共享内存！
 └────────────────────────────────────────────┘
```

`duplicate()` 和 `slice()` 的区别在于：`slice()` 只暴露 readable 区域（readerIndex 到 writerIndex），`duplicate()` 暴露整个 ByteBuf 的全部容量。

### 4.4 Unpooled.wrappedBuffer()：直接引用不拷贝

`Unpooled.wrappedBuffer()` 提供了多种重载，核心思想是"包装而非拷贝"——直接引用传入的内存，不做任何数据复制：

```java
// Unpooled.java

// 包装 byte[]：直接引用，不拷贝
public static ByteBuf wrappedBuffer(byte[] array) {
    if (array.length == 0) {
        return EMPTY_BUFFER;
    }
    return new UnpooledHeapByteBuf(ALLOC, array, array.length);  // 直接引用 array
}

// 包装 ByteBuffer：按 direct/heap 选择不同实现
public static ByteBuf wrappedBuffer(ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
        return EMPTY_BUFFER;
    }
    if (!buffer.isDirect() && buffer.hasArray()) {
        return wrappedBuffer(buffer.array(),                    // 直接引用底层数组
                buffer.arrayOffset() + buffer.position(),
                buffer.remaining()).order(buffer.order());
    } else if (PlatformDependent.hasUnsafe()) {
        // Direct ByteBuffer，通过 Unsafe 直接访问内存地址
        // ...
    } else {
        return new UnpooledDirectByteBuf(ALLOC, buffer, buffer.remaining());
    }
}

// 包装多个 ByteBuf：组合为 CompositeByteBuf
public static ByteBuf wrappedBuffer(ByteBuf... buffers) {
    return wrappedBuffer(buffers.length, buffers);  // → CompositeByteBuf
}

// 包装 native 内存地址
public static ByteBuf wrappedBuffer(long memoryAddress, int size, boolean doFree) {
    return new WrappedUnpooledUnsafeDirectByteBuf(ALLOC, memoryAddress, size, doFree);
}
```

不同输入类型的处理策略可以总结为：

```
 Unpooled.wrappedBuffer() 的分发路径
 ====================================

 输入类型              输出类型                     是否拷贝
 ─────────────────────────────────────────────────────────
 byte[]           →  UnpooledHeapByteBuf            否
 ByteBuffer(heap) →  UnpooledHeapByteBuf.slice()    否
 ByteBuffer(direct)→ UnpooledDirectByteBuf          否
 ByteBuf          →  buf.slice()                    否
 ByteBuf[]        →  CompositeByteBuf               否
 long memoryAddr  →  WrappedUnpooledUnsafeDirectByteBuf  否
```

所有路径都不涉及数据拷贝——这就是"wrappedBuffer"名字的含义：包装（wrap），而非复制（copy）。

---

## 五、Direct ByteBuffer 的作用

Direct ByteBuffer 是 Java NIO 提供的堆外内存分配方式，它在 Netty 的零拷贝体系中扮演着一个经常被忽视但至关重要的角色——避免 JVM GC 导致的额外拷贝。

### 5.1 为什么堆内存需要额外拷贝

当你使用堆内存（HeapByteBuf）进行 Socket 写入时，JDK 的 NIO 实现会在内部分配一个临时的 Direct ByteBuffer，将堆内存数据拷贝到这个临时缓冲区，然后再执行系统调用。原因在于 JVM 的 GC 可能在系统调用执行期间移动堆内存中的对象（compaction），导致操作系统通过指针访问到错误的内存地址。Direct ByteBuffer 分配在 JVM 堆外，GC 不会移动它，因此可以安全地将内存地址传递给操作系统：

```
             堆 ByteBuf 写入 Socket 的数据路径
             ================================

 HeapByteBuf (JVM堆内存)
 ┌──────────────────┐
 │  data...         │  GC 可能移动此内存！
 └────────┬─────────┘
          │
          │ 拷贝 ← JDK 内部自动完成
          │       （避免 GC 移动导致指针失效）
          ▼
 DirectByteBuffer (堆外内存)
 ┌──────────────────┐
 │  data...         │  GC 不会移动，地址稳定
 └────────┬─────────┘
          │
          │ 系统调用 write()
          ▼
 ┌──────────────────┐
 │ Socket 发送缓冲区 │
 └──────────────────┘


             直接 ByteBuf 写入 Socket 的数据路径
             ==================================

 DirectByteBuf (堆外内存)
 ┌──────────────────┐
 │  data...         │  GC 不会移动，地址稳定
 └────────┬─────────┘
          │
          │ 系统调用 write()  ← 无需额外拷贝！
          ▼
 ┌──────────────────┐
 │ Socket 发送缓冲区 │
 └──────────────────┘
```

### 5.2 Netty 的 filterOutboundMessage() 自动转换

正是因为这个原因，Netty 在将消息写入 Socket 之前，会通过 `filterOutboundMessage()` 将堆 ByteBuf 自动转换为直接 ByteBuf：

```java
// AbstractNioByteChannel.java
@Override
protected final Object filterOutboundMessage(Object msg) {
    if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        if (buf.isDirect()) {
            return msg;           // 已经是直接内存，不转换
        }
        return newDirectBuffer(buf);  // 堆内存 → 直接内存
    }
    if (msg instanceof FileRegion) {
        return msg;               // FileRegion 不需要转换
    }
    throw new UnsupportedOperationException(...);
}
```

这个转换发生在 `AbstractUnsafe.write()` 中，即消息进入 ChannelOutboundBuffer 之前。也就是说，当你调用 `ctx.write(heapBuf)` 时，进入写缓冲区的已经是一个 Direct ByteBuf 了。

### 5.3 Netty 的直接内存管理

Netty 对 Direct ByteBuffer 做了两层优化，避免了 JDK 原生 Direct ByteBuffer 的高分配/释放成本。

对于池化场景（`PooledDirectByteBuf`），Netty 通过 jemalloc 风格的内存池（PoolChunk/PoolSubpage）管理大块直接内存，分配时从池中切割，释放时归还池中。同时通过 `Recycler` 对象池复用 PooledDirectByteBuf 对象本身，避免频繁的 GC。

对于非池化场景（`UnpooledDirectByteBuf`），Netty 使用 `ByteBuffer.allocateDirect()` 分配，通过 `PlatformDependent.freeDirectBuffer()` 在 `deallocate()` 时主动释放（而非等待 GC 的 Cleaner 回收），避免直接内存泄漏。

---

## 六、实际案例：HTTP 文件下载服务对比

通过一个完整的 HTTP 文件下载服务示例，对比使用 FileRegion 零拷贝和传统方式的区别：

### 6.1 零拷贝方式：FileRegion

```java
// 零拷贝文件下载 Handler
public class FileServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        File file = new File("/data/files/" + request.uri());
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        long fileLength = raf.length();

        // 1. 先发送 HTTP 响应头
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, fileLength);
        ctx.write(response);

        // 2. 通过 FileRegion 零拷贝发送文件内容
        //    数据路径: 磁盘 → PageCache → 网卡 (CPU 不参与数据拷贝)
        ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength));

        // 3. 发送 LastHttpContent 标记结束
        ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        future.addListener(ChannelFutureListener.CLOSE);
    }
}
```

数据流向：磁盘 → 内核 PageCache →（DMA Gather）→ 网卡。CPU 全程不参与文件数据的拷贝。

### 6.2 传统方式：读入 ByteBuf 再写出

```java
// 传统文件下载 Handler（无零拷贝）
public class TraditionalFileHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        File file = new File("/data/files/" + request.uri());
        byte[] bytes = Files.readAllBytes(file.toPath());  // 文件 → 堆内存（CPU拷贝）

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, bytes.length);
        ctx.write(response);

        // 数据路径: 磁盘 → 内核 → 用户态byte[] → DirectByteBuf → 内核Socket → 网卡
        ByteBuf content = Unpooled.wrappedBuffer(bytes);
        ctx.write(new DefaultLastHttpContent(content));
        ctx.flush();
    }
}
```

数据流向经过了用户态，CPU 参与了至少 2 次多余的拷贝。对于大文件传输，性能差距会非常明显。

### 6.3 性能差异分析

```
 1GB 文件下载的对比（示意）
 ===========================

 指标              FileRegion        传统方式
 ─────────────────────────────────────────────
 CPU拷贝次数        0次               2次
 内存占用           几乎为0            ≥1GB (堆内存)
 上下文切换         2次               4次
 GC压力            无                 高 (1GB堆对象)
 吞吐量            高                 低
```

FileRegion 的优势在大文件场景下尤为突出：它不需要在 JVM 堆中分配和文件一样大的内存，也不会因为频繁的大对象分配触发 GC。

---

## 七、SslHandler 与 FileRegion 不兼容的原因和解决方案

### 7.1 不兼容的根本原因

当 Pipeline 中包含 SslHandler 时，FileRegion 会抛出 `UnsupportedMessageTypeException`。这是一个设计上的必然限制，不是 Bug。

SslHandler 的 `write()` 方法只接受 ByteBuf 类型的消息：

```java
// SslHandler.java
@Override
public void write(final ChannelHandlerContext ctx, Object msg,
                  ChannelPromise promise) throws Exception {
    if (!(msg instanceof ByteBuf)) {
        UnsupportedMessageTypeException exception =
                new UnsupportedMessageTypeException(msg, ByteBuf.class);
        ReferenceCountUtil.safeRelease(msg);
        promise.setFailure(exception);
    } else if (pendingUnencryptedWrites == null) {
        ReferenceCountUtil.safeRelease(msg);
        promise.setFailure(newPendingWritesNullException());
    } else {
        pendingUnencryptedWrites.add((ByteBuf) msg, promise);
    }
}
```

原因在于 SSL/TLS 加密的本质：加密引擎（SSLEngine）需要读取明文数据，对其进行 wrap（加密），然后输出密文。这要求数据必须在用户态可访问。而 FileRegion 的 sendfile 系统调用恰恰是绕过用户态直接在内核中完成文件到网络的传输——数据根本不经过用户态，SslHandler 无法对数据进行加密：

```
                  FileRegion + SslHandler 的矛盾
                  ==============================

 FileRegion 的 sendfile 路径:
 磁盘 → 内核PageCache ──────────────────────────→ 网卡
                       ↑                    ↑
                       │ 数据不经过用户态！   │
                       │                    │
                       │   SslHandler在这里  │
                       │   无法加密数据      │
                       └────────────────────┘

 SSL 加密需要的路径:
 磁盘 → 内核 → 用户态(SslHandler.wrap加密) → 内核 → 网卡
                       ↑                ↑
                       │ 必须经过用户态  │
                       │ SSLEngine才能  │
                       │ 加密数据       │
                       └────────────────┘
```

简而言之，sendfile 的"零拷贝"优势——数据不经过用户态——恰恰是它与 SSL 不兼容的原因。SSL 加密必须在用户态完成，两者在数据路径上存在根本矛盾。

### 7.2 解决方案：ChunkedWriteHandler + ChunkedFile

Netty 提供了 `ChunkedWriteHandler` + `ChunkedFile` 的组合方案来替代 FileRegion，适用于需要 SSL 加密的文件传输场景：

```java
// SSL 场景的文件下载 Handler
public class SslFileServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        File file = new File("/data/files/" + request.uri());
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        long fileLength = raf.length();

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, fileLength);
        ctx.write(response);

        // 使用 ChunkedFile 替代 FileRegion
        // ChunkedFile 将文件分块读入 ByteBuf，SslHandler 可以逐块加密
        ctx.write(new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)));

        ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        future.addListener(ChannelFutureListener.CLOSE);
    }
}
```

ChunkedFile 的 `readChunk()` 方法每次从文件中读取一个固定大小（默认 8192 字节）的块到 ByteBuf 中：

```java
// ChunkedFile.java
public class ChunkedFile implements ChunkedInput<ByteBuf> {

    private final RandomAccessFile file;
    private final long endOffset;
    private final int chunkSize;
    private long offset;

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        long offset = this.offset;
        if (offset >= endOffset) {
            return null;
        }

        int chunkSize = (int) Math.min(this.chunkSize, endOffset - offset);
        ByteBuf buf = allocator.heapBuffer(chunkSize);
        boolean release = true;
        try {
            file.readFully(buf.array(), buf.arrayOffset(), chunkSize);
            buf.writerIndex(chunkSize);
            this.offset = offset + chunkSize;
            release = false;
            return buf;
        } finally {
            if (release) {
                buf.release();
            }
        }
    }
}
```

Pipeline 的配置方式：

```java
// SSL 场景的 Pipeline 配置
pipeline.addLast("ssl", new SslHandler(sslEngine));
pipeline.addLast("http-codec", new HttpServerCodec());
pipeline.addLast("chunked-writer", new ChunkedWriteHandler());  // 必须加这个
pipeline.addLast("file-handler", new SslFileServerHandler());
```

整个数据流转过程为：

```
 ChunkedFile + SslHandler 的数据流
 ==================================

 ChunkedFile.readChunk()
    │ 每次读取 8KB 到 HeapByteBuf
    ▼
 ChunkedWriteHandler.doFlush()
    │ 将 ByteBuf 写入 Pipeline
    ▼
 SslHandler.write()
    │ msg instanceof ByteBuf ✓
    │ SSLEngine.wrap(明文) → 密文
    ▼
 HeadContext.write()
    │ filterOutboundMessage(): 堆ByteBuf → 直接ByteBuf
    ▼
 NioSocketChannel.doWrite()
    │ SocketChannel.write(directBuf)
    ▼
 网卡发送密文
```

虽然 ChunkedFile 方式放弃了 sendfile 的零拷贝优势，但它带来了两个好处：第一，兼容 SSL 加密；第二，分块读取文件，内存占用可控（不需要将整个文件加载到内存中），适合大文件传输。

### 7.3 选型建议

```
 场景                       推荐方案                    原因
 ──────────────────────────────────────────────────────────────
 非SSL文件传输(HTTP)         DefaultFileRegion           零拷贝，性能最优
 SSL文件传输(HTTPS)          ChunkedFile + SslHandler    SSL需要用户态加密
 小文件/需要修改内容          直接读入ByteBuf              灵活，可修改数据
 Epoll传输 + 非SSL           DefaultFileRegion           JNI直调sendfile
```

---

## 八、本篇涉及的设计模式

**适配器模式（Adapter）**：`EpollSocketWritableByteChannel` 将 Epoll 的 Socket 包装为 JDK 的 `WritableByteChannel` 接口，使得非 DefaultFileRegion 的 FileRegion 实现可以通过标准的 `transferTo(WritableByteChannel)` 方法完成数据传输。这是将 Netty 特有的 Epoll Socket 适配为 JDK 标准接口的典型适配器。

**组合模式（Composite）**：CompositeByteBuf 是组合模式的经典应用。它将多个 ByteBuf（叶子节点）组合成一个逻辑上的大 ByteBuf（组合节点），客户端代码可以像操作单个 ByteBuf 一样操作 CompositeByteBuf，无需关心内部有多少个子 ByteBuf。Component 数组维护了组合关系，`nioBuffers()` 方法透明地遍历所有子节点。

**享元模式（Flyweight）**：slice() 和 duplicate() 创建的 ByteBuf 视图共享底层的内存数据（内在状态），只有 readerIndex、writerIndex、adjustment 等轻量级字段（外在状态）是每个视图独立持有的。通过共享不可变的底层内存，避免了大量数据拷贝。

**模板方法模式（Template Method）**：`AbstractNioByteChannel.doWrite()` 定义了写入的骨架流程（循环、spinCount 控制、incompleteWrite 处理），将 `doWriteFileRegion()` 和 `doWriteBytes()` 作为抽象方法留给子类 NioSocketChannel 实现。`AbstractReferenceCounted` 定义了引用计数的通用逻辑，将 `deallocate()` 留给 DefaultFileRegion 实现。

**策略模式（Strategy）**：在 Epoll 传输的 `doWriteSingle()` 中，根据消息类型（DefaultFileRegion vs 普通 FileRegion vs ByteBuf）选择不同的写入策略——JNI 直接 sendfile、JDK FileChannel.transferTo()、或普通的 Socket write。不同的策略封装了不同的系统调用路径。

---

## 九、本篇涉及的高性能并发技术

**操作系统 sendfile 零拷贝**：DefaultFileRegion 通过 `FileChannel.transferTo()` 触发操作系统的 sendfile 系统调用，避免了数据在用户态和内核态之间的来回拷贝。Epoll 传输更进一步，通过 JNI 直接调用 Linux sendfile()，跳过 JDK 的中间层。在支持 DMA Gather 的硬件上，CPU 全程不参与数据搬运。

**Gathering Write（writev）**：CompositeByteBuf 的 nioBuffers() 方法返回底层 ByteBuffer 数组，NioSocketChannel.doWrite() 使用 `SocketChannel.write(ByteBuffer[])` 进行 gathering write。这对应操作系统的 writev 系统调用，一次系统调用写入多个不连续的缓冲区，减少了系统调用次数和上下文切换开销。

**堆外直接内存（Off-Heap Memory）**：Direct ByteBuffer 分配在 JVM 堆外，GC 不会移动其内存地址，因此可以安全地将指针传递给操作系统进行 I/O 操作，避免了堆内存写入 Socket 时 JDK 内部的额外拷贝。Netty 通过 PooledDirectByteBuf + jemalloc 内存池管理直接内存，解决了 JDK 原生 Direct ByteBuffer 分配/释放成本高的问题。

**内存视图共享（Memory View Sharing）**：slice()、duplicate()、CompositeByteBuf 和 Unpooled.wrappedBuffer() 通过共享底层内存创建不同的视图，避免了协议拼装、拆包过程中的数据拷贝。这在高频的编解码场景中意义重大——一个 TCP 包可能被拆成多个协议帧，每个帧可以用 slice 直接引用原始数据的子区域。

**EINTR 信号中断重试**：Epoll 传输的 C 层 sendfile 调用使用 `do-while` 循环处理 EINTR 信号中断。在高并发服务器中，各种信号（SIGPROF、SIGALRM 等）可能在系统调用执行过程中到达，导致 sendfile 返回 -1 且 errno 为 EINTR。自动重试确保了系统调用的可靠完成。

**懒加载与资源延迟获取（Lazy Initialization）**：DefaultFileRegion 的 File 构造方式采用懒加载——FileChannel 延迟到第一次 transferTo() 调用时才通过 `open()` 方法创建。这避免了创建 FileRegion 后因连接断开等原因未使用时浪费文件描述符资源，在高并发场景下减少了不必要的系统资源占用。

**引用计数（Reference Counting）**：FileRegion 继承 ReferenceCounted，通过引用计数管理底层 FileChannel 的生命周期。当 refCnt 归零时自动调用 deallocate() 关闭 FileChannel，既避免了资源泄漏，又支持多个 Handler 共享同一个 FileRegion 对象。transferTo() 方法在执行前检查 refCnt，防止访问已释放的资源。