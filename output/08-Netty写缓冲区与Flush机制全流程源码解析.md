# Netty 写缓冲区与 Flush 机制全流程源码解析

> 基于 Netty 源码，深度解析 write() 和 flush() 的完整链路。从 ChannelOutboundBuffer 的 Entry 链表结构，到 AbstractNioByteChannel 的 writeSpinCount 循环写入，再到 NioSocketChannel 的 gathering write（writev）优化，逐层揭示 Netty 如何在高并发场景下高效地将数据写入操作系统 Socket 缓冲区，同时通过水位线机制实现背压（Backpressure）控制。

---

## 一、write() 和 flush() 的本质区别

在深入源码之前，必须先理解一个核心概念：Netty 的 write() 和 flush() 是两个完全独立的操作，它们分别对应数据生命周期的不同阶段。

**write() 的本质是"入队"**：当你调用 `ctx.write(msg)` 时，Netty 并没有将数据写入操作系统的 Socket，而是将消息封装成一个 Entry 对象，追加到 ChannelOutboundBuffer 的 unflushed 链表中。此时数据还停留在用户态内存中，对端完全看不到。

**flush() 的本质是"出队 + 写入"**：当你调用 `ctx.flush()` 时，Netty 会先将 unflushed 链表中的所有 Entry 标记为 flushed 状态（即把它们从 unflushed 区移动到 flushed 区），然后调用底层 NIO 的 `SocketChannel.write()` 将数据真正写入操作系统内核缓冲区。

这两步分离的设计带来一个关键优势：**批量写入**。你可以在一个 EventLoop 周期内多次调用 write() 积攒多条消息，然后一次 flush() 将它们通过 gathering write（writev）一次性写入 Socket，大幅减少系统调用次数。

下面这张图展示了 write() 和 flush() 在 ChannelOutboundBuffer 中的操作位置：

```
                    ChannelOutboundBuffer
 ┌──────────────────────────────────────────────────────────┐
 │                                                          │
 │  flushedEntry          unflushedEntry         tailEntry  │
 │      │                      │                     │      │
 │      ▼                      ▼                     ▼      │
 │   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐  │
 │   │Entry1│──▶│Entry2│──▶│Entry3│──▶│Entry4│──▶│Entry5│  │
 │   │(已flush)│ │(已flush)│ │(已flush)│ │(未flush)│ │(未flush)│ │
 │   └──────┘   └──────┘   └──────┘   └──────┘   └──────┘  │
 │                                                          │
 │   ←── flushed=3 ──→←── unflushed=2 ──→                  │
 │                                                          │
 └──────────────────────────────────────────────────────────┘

 write() 追加到 unflushed 链表尾部（tailEntry 之后）
 flush() 把 unflushed 链表整体移入 flushed 链表
```

从调用链来看，write() 和 flush() 都经过 ChannelPipeline 的 outbound 链传播，最终到达 AbstractUnsafe：

```
用户代码: ctx.write(msg, promise)
    │
    ▼
ChannelPipeline 出站传播（tail → head 方向）
    │
    ▼
HeadContext.write()
    │
    ▼
AbstractUnsafe.write()           ← write() 的终点
    ├─ filterOutboundMessage(msg)    ← 堆内存转直接内存
    ├─ estimatorHandle().size(msg)   ← 估算消息大小
    └─ outboundBuffer.addMessage()   ← 追加到 unflushed 链表

用户代码: ctx.flush()
    │
    ▼
ChannelPipeline 出站传播
    │
    ▼
HeadContext.flush()
    │
    ▼
AbstractUnsafe.flush()           ← flush() 的终点
    ├─ outboundBuffer.addFlush()     ← unflushed → flushed
    └─ flush0()
        └─ doWrite(outboundBuffer)  ← 真正写入 OS Socket
```

**write() 不会触发任何 I/O 操作**。它只是链表操作和内存计数。这就意味着，如果你只调用 write() 不调用 flush()，数据会永远停留在 ChannelOutboundBuffer 中，对端永远收不到数据——这是 Netty 初学者最常犯的错误之一。

---

## 二、ChannelOutboundBuffer 的数据结构（核心）

ChannelOutboundBuffer 是 Netty 写缓冲区的核心数据结构。理解它，就理解了 Netty 写操作的骨架。

### 2.1 三指针链表结构

ChannelOutboundBuffer 内部维护一个单向链表，通过三个指针将链表划分为两个区域：

```java
// ChannelOutboundBuffer.java

// Entry(flushedEntry) --> ... Entry(unflushedEntry) --> ... Entry(tailEntry)
private Entry flushedEntry;      // 第一个已 flush 但尚未写完的 Entry
private Entry unflushedEntry;    // 第一个未 flush 的 Entry
private Entry tailEntry;         // 链表尾部 Entry
private int flushed;             // 已 flush 但尚未写完的 Entry 数量
```

三个指针的含义如下：

`flushedEntry` 指向第一个"已经被 flush() 标记但还没被 doWrite() 完全写完"的 Entry。doWrite() 从这个位置开始读取数据写入 Socket。当 Entry 被完全写完并 remove() 后，flushedEntry 向后移动。

`unflushedEntry` 指向第一个"被 write() 追加但还没被 flush() 标记"的 Entry。write() 新增的 Entry 挂在 tailEntry 之后，如果 unflushedEntry 为 null（说明之前的数据都被 flush 了），则 unflushedEntry 指向新 Entry。flush() 时从 unflushedEntry 开始，把到 tailEntry 之间的所有 Entry 标记为 flushed。

`tailEntry` 始终指向链表的最后一个 Entry，write() 通过它实现 O(1) 尾部追加。

完整的链表状态可以用下图表示：

```
初始状态（无数据）：
    flushedEntry = null
    unflushedEntry = null
    tailEntry = null
    flushed = 0

write(msg1) 后：
    flushedEntry = null          unflushedEntry    tailEntry
         null                         │                │
                                      ▼                ▼
                                   ┌──────┐
                                   │Entry1│──▶ null
                                   └──────┘
    flushed = 0

write(msg2) 后：
    flushedEntry = null          unflushedEntry    tailEntry
         null                         │                │
                                      ▼                ▼
                                   ┌──────┐   ┌──────┐
                                   │Entry1│──▶│Entry2│──▶ null
                                   └──────┘   └──────┘
    flushed = 0

flush() 后：
    flushedEntry                   unflushedEntry    tailEntry
        │                               null             null
        ▼
     ┌──────┐   ┌──────┐
     │Entry1│──▶│Entry2│──▶ null
     └──────┘   └──────┘
    flushed = 2

doWrite 写完 Entry1 并 remove() 后：
    flushedEntry                   unflushedEntry    tailEntry
        │                               null             null
        ▼
     ┌──────┐
     │Entry2│──▶ null
     └──────┘
    flushed = 1

doWrite 写完 Entry2 并 remove() 后：
    flushedEntry = null
    unflushedEntry = null
    tailEntry = null
    flushed = 0
    （回到初始状态）
```

### 2.2 Entry 的结构与对象池化

Entry 是链表的节点，它通过 Recycler 对象池化，避免频繁创建 GC 压力：

```java
// ChannelOutboundBuffer.Entry

static final class Entry {
    private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
        @Override
        protected Entry newObject(Handle<Entry> handle) {
            return new Entry(handle);
        }
    };

    private final EnhancedHandle<Entry> handle;
    Entry next;              // 链表后继指针
    Object msg;              // 待写入的消息（ByteBuf / FileRegion）
    ByteBuffer[] bufs;       // 多 ByteBuffer 缓存（CompositeByteBuf 场景）
    ByteBuffer buf;          // 单 ByteBuffer 缓存（普通 ByteBuf 场景）
    ChannelPromise promise;  // 写完成回调
    long progress;           // 已写入进度
    long total;              // 消息总大小
    int pendingSize;         // 占用的待处理字节数（含 overhead）
    int count = -1;          // nioBuffer 数量（-1 表示未计算）
    boolean cancelled;       // 是否被取消
}
```

Entry 创建时通过 `Entry.newInstance()` 从对象池获取：

```java
static Entry newInstance(Object msg, int size, long total, ChannelPromise promise) {
    Entry entry = RECYCLER.get();    // 从对象池获取，避免 new
    entry.msg = msg;
    entry.pendingSize = size + CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD;
    entry.total = total;
    entry.promise = promise;
    return entry;
}
```

其中 `CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD` 是每个 Entry 的固定开销：

```java
// Assuming a 64-bit JVM:
//  - 16 bytes object header
//  - 6 reference fields
//  - 2 long fields
//  - 2 int fields
//  - 1 boolean field
//  - padding
static final int CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD =
        SystemPropertyUtil.getInt("io.netty.transport.outboundBufferEntrySizeOverhead", 96);
```

这个 96 字节是 Entry 对象本身在 64 位 JVM 上的内存占用估算。把它计入 pendingSize 是因为水位线机制需要统计所有待处理数据占用的内存总量，而不仅仅是消息数据本身的大小。如果只统计消息大小而不计 Entry 开销，在高频小消息场景下会严重低估实际内存占用。

Entry 用完后通过 `unguardedRecycle()` 回收到对象池：

```java
void unguardedRecycle() {
    next = null;
    bufs = null;
    buf = null;
    msg = null;
    promise = null;
    progress = 0;
    total = 0;
    pendingSize = 0;
    count = -1;
    cancelled = false;
    handle.unguardedRecycle(this);  // 归还到 Recycler 对象池
}
```

### 2.3 addMessage()：write() 时追加到 unflushed 链表

当 AbstractUnsafe.write() 调用 `outboundBuffer.addMessage(msg, size, promise)` 时，执行如下逻辑：

```java
// ChannelOutboundBuffer.java

public void addMessage(Object msg, int size, ChannelPromise promise) {
    // 1. 从对象池获取 Entry，封装消息
    Entry entry = Entry.newInstance(msg, size, total(msg), promise);

    // 2. 追加到链表尾部
    if (tailEntry == null) {
        // 链表为空，初始化
        flushedEntry = null;
    } else {
        // 链表非空，把新 Entry 挂到 tail 后面
        Entry tail = tailEntry;
        tail.next = entry;
    }
    tailEntry = entry;  // 更新 tail 指针

    // 3. 如果 unflushedEntry 为空，说明之前没有未 flush 的数据
    if (unflushedEntry == null) {
        unflushedEntry = entry;
    }

    // 4. 触摸消息辅助泄漏检测
    if (msg instanceof AbstractReferenceCountedByteBuf) {
        ((AbstractReferenceCountedByteBuf) msg).touch();
    } else {
        ReferenceCountUtil.touch(msg);
    }

    // 5. 增加待处理字节数，检查是否超过高水位线
    incrementPendingOutboundBytes(entry.pendingSize, false);
}
```

这个方法的执行流程可以用下图展示（假设链表已有 Entry1，现在 addMessage 写入 Entry2）：

```
addMessage 前：
    unflushedEntry    tailEntry
        │                │
        ▼                ▼
     ┌──────┐
     │Entry1│──▶ null
     └──────┘

addMessage(msg2) 执行步骤：
    ① Entry.newInstance(msg2, size, total, promise) → 从池中获取 Entry2
    ② tailEntry(Entry1).next = Entry2
    ③ tailEntry = Entry2
    ④ unflushedEntry 不变（已经是 Entry1）
    ⑤ incrementPendingOutboundBytes(Entry2.pendingSize)

addMessage 后：
    unflushedEntry         tailEntry
        │                     │
        ▼                     ▼
     ┌──────┐   ┌──────┐
     │Entry1│──▶│Entry2│──▶ null
     └──────┘   └──────┘
```

`total()` 方法用于计算消息的可读字节数，用于进度跟踪：

```java
private static long total(Object msg) {
    if (msg instanceof ByteBuf) {
        return ((ByteBuf) msg).readableBytes();
    }
    if (msg instanceof FileRegion) {
        return ((FileRegion) msg).count();
    }
    if (msg instanceof ByteBufHolder) {
        return ((ByteBufHolder) msg).content().readableBytes();
    }
    return -1;
}
```

### 2.4 addFlush()：flush() 时把 unflushed 移到 flushed

当 AbstractUnsafe.flush() 调用 `outboundBuffer.addFlush()` 时，执行如下逻辑：

```java
// ChannelOutboundBuffer.java

public void addFlush() {
    Entry entry = unflushedEntry;
    if (entry != null) {
        // 如果之前没有 flushed Entry，从 unflushedEntry 开始
        if (flushedEntry == null) {
            flushedEntry = entry;
        }
        do {
            flushed++;  // flushed 计数器 +1

            // 尝试设置为不可取消状态
            if (!entry.promise.setUncancellable()) {
                // Promise 已经被取消了，需要释放消息并归还内存
                int pending = entry.cancel();
                decrementPendingOutboundBytes(pending, false, true);
            }
            entry = entry.next;
        } while (entry != null);

        // 所有 unflushed Entry 都标记完了，重置 unflushedEntry
        unflushedEntry = null;
    }
}
```

addFlush() 的关键操作是遍历从 unflushedEntry 到链表尾部的所有 Entry，对每个执行两件事：将 flushed 计数器加 1，以及将 promise 标记为不可取消。如果 promise 在 flush 之前已经被用户取消（通过 `future.cancel()`），则调用 `entry.cancel()` 释放消息引用并扣减待处理字节数。

addFlush() 前后的链表状态变化：

```
addFlush 前：
    flushedEntry = null    unflushedEntry         tailEntry
                               │                     │
                               ▼                     ▼
                            ┌──────┐   ┌──────┐   ┌──────┐
                            │Entry1│──▶│Entry2│──▶│Entry3│──▶ null
                            └──────┘   └──────┘   └──────┘
    flushed = 0

addFlush 后：
    flushedEntry             unflushedEntry    tailEntry
        │                         null             null
        ▼
     ┌──────┐   ┌──────┐   ┌──────┐
     │Entry1│──▶│Entry2│──▶│Entry3│──▶ null
     └──────┘   └──────┘   └──────┘
    flushed = 3
```

注意：addFlush() 只是修改指针和计数器，并不移动 Entry 对象本身。Entry1/Entry2/Entry3 在内存中的位置不变，只是它们的归属从 unflushed 区变成了 flushed 区。这是一个 O(1) 的指针操作，非常高效。

### 2.5 remove()：写完后移除 Entry 并回收

当 doWrite() 成功将一个 Entry 的数据写入 Socket 后，调用 `remove()` 将其从链表中移除：

```java
// ChannelOutboundBuffer.java

public boolean remove() {
    Entry e = flushedEntry;
    if (e == null) {
        clearNioBuffers();
        return false;
    }
    Object msg = e.msg;
    ChannelPromise promise = e.promise;
    int size = e.pendingSize;

    // 从链表中移除
    removeEntry(e);

    if (!e.cancelled) {
        // 释放消息引用计数
        if (msg instanceof AbstractReferenceCountedByteBuf) {
            ((AbstractReferenceCountedByteBuf) msg).release();
        } else {
            ReferenceCountUtil.safeRelease(msg);
        }
        // 通知 Promise 成功
        safeSuccess(promise);
        // 扣减待处理字节数
        decrementPendingOutboundBytes(size, false, true);
    }

    // 回收到对象池
    e.unguardedRecycle();
    return true;
}
```

`removeEntry()` 的逻辑负责更新链表指针：

```java
private void removeEntry(Entry e) {
    if (--flushed == 0) {
        // 所有 flushed Entry 都处理完了
        flushedEntry = null;
        if (e == tailEntry) {
            // 链表全部清空
            tailEntry = null;
            unflushedEntry = null;
        }
    } else {
        // 还有 flushed Entry，指针后移
        flushedEntry = e.next;
    }
}
```

### 2.6 removeBytes()：批量移除已写入的字节

NioSocketChannel.doWrite() 使用 gathering write 一次性写入多个 ByteBuf 的数据后，需要根据实际写入的字节数批量移除已写完的 Entry，并更新部分写入的 Entry 的 readerIndex：

```java
// ChannelOutboundBuffer.java

public void removeBytes(long writtenBytes) {
    for (;;) {
        Object msg = current();  // 获取 flushedEntry.msg
        if (!(msg instanceof ByteBuf)) {
            assert writtenBytes == 0;
            break;
        }

        final ByteBuf buf = (ByteBuf) msg;
        final int readerIndex = buf.readerIndex();
        final int readableBytes = buf.writerIndex() - readerIndex;

        if (readableBytes <= writtenBytes) {
            // 这个 ByteBuf 的数据已经全部写完了
            if (writtenBytes != 0) {
                progress(readableBytes);        // 更新进度
                writtenBytes -= readableBytes;  // 扣减已写入字节数
            }
            remove();  // 移除并回收 Entry
        } else {
            // 这个 ByteBuf 只写了一部分
            if (writtenBytes != 0) {
                buf.readerIndex(readerIndex + (int) writtenBytes);
                progress(writtenBytes);
            }
            break;  // 还有数据没写完，退出循环
        }
    }
    clearNioBuffers();  // 清理 ThreadLocal 的 nioBuffers 数组
}
```

这个方法的设计很精妙：它不需要知道哪些 ByteBuf 被写完了，只需要按顺序遍历 flushed 链表，根据 `readableBytes` 和 `writtenBytes` 的大小关系决定是 remove 还是更新 readerIndex。例如：

```
假设 gathering write 写入了 150 字节，flushed 链表中有三个 ByteBuf：
    Entry1: 100 字节  → 全部写完，remove()
    Entry2: 80 字节   → 前 50 字节写完，更新 readerIndex += 50
    Entry3: 120 字节  → 还没轮到，不动

writtenBytes 变化：150 → 50 → 0
```

### 2.7 nioBuffers()：收集 ByteBuffer 数组供 gathering write 使用

这是 NioSocketChannel.doWrite() 实现 gathering write 的关键方法。它遍历 flushed 链表，将每个 ByteBuf 的内部 NIO ByteBuffer 提取出来，放入一个 ThreadLocal 的数组中：

```java
// ChannelOutboundBuffer.java

// 线程局部 ByteBuffer 数组，初始容量 1024
private static final FastThreadLocal<ByteBuffer[]> NIO_BUFFERS = new FastThreadLocal<ByteBuffer[]>() {
    @Override
    protected ByteBuffer[] initialValue() throws Exception {
        return new ByteBuffer[1024];
    }
};

public ByteBuffer[] nioBuffers(int maxCount, long maxBytes) {
    long nioBufferSize = 0;
    int nioBufferCount = 0;
    final InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
    ByteBuffer[] nioBuffers = NIO_BUFFERS.get(threadLocalMap);  // 从 ThreadLocal 获取数组
    Entry entry = flushedEntry;

    while (isFlushedEntry(entry) && entry.msg instanceof ByteBuf) {
        if (!entry.cancelled) {
            ByteBuf buf = (ByteBuf) entry.msg;
            final int readerIndex = buf.readerIndex();
            final int readableBytes = buf.writerIndex() - readerIndex;

            if (readableBytes > 0) {
                // 检查是否会超过 maxBytes 限制
                if (maxBytes - readableBytes < nioBufferSize && nioBufferCount != 0) {
                    break;  // 超过限制了，停止收集
                }
                nioBufferSize += readableBytes;

                // 计算这个 ByteBuf 包含几个 NIO ByteBuffer
                int count = entry.count;
                if (count == -1) {
                    entry.count = count = buf.nioBufferCount();
                }

                // 确保数组容量足够
                int neededSpace = min(maxCount, nioBufferCount + count);
                if (neededSpace > nioBuffers.length) {
                    nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                    NIO_BUFFERS.set(threadLocalMap, nioBuffers);
                }

                if (count == 1) {
                    // 普通 ByteBuf：只有一个 ByteBuffer，缓存到 entry.buf
                    ByteBuffer nioBuf = entry.buf;
                    if (nioBuf == null) {
                        entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
                    }
                    nioBuffers[nioBufferCount++] = nioBuf;
                } else {
                    // CompositeByteBuf：多个 ByteBuffer，缓存到 entry.bufs
                    nioBufferCount = nioBuffers(entry, buf, nioBuffers, nioBufferCount, maxCount);
                }

                if (nioBufferCount >= maxCount) {
                    break;  // 达到最大数量限制
                }
            }
        }
        entry = entry.next;
    }

    this.nioBufferCount = nioBufferCount;
    this.nioBufferSize = nioBufferSize;
    return nioBuffers;
}
```

这个方法有三个关键优化：

第一，使用 FastThreadLocal 缓存 ByteBuffer 数组，避免每次 gathering write 都创建新数组。初始容量 1024 足以覆盖绝大多数场景，不足时才扩容。

第二，将提取出的 ByteBuffer 缓存到 Entry 的 `buf` 或 `bufs` 字段中。因为 ByteBuf 的 `internalNioBuffer()` 方法可能创建新的 ByteBuffer 对象（对于 derived buffer），缓存后同一个 Entry 多次写入时不需要重复创建。

第三，maxBytes 参数限制了单次 gathering write 的最大字节数。这是为了避免在 BSD/macOS 上 writev 超过 `Integer.MAX_VALUE` 字节导致 `EINVAL` 错误。

`isFlushedEntry()` 方法用于判断遍历是否到达了 unflushed 区的边界：

```java
private boolean isFlushedEntry(Entry e) {
    return e != null && e != unflushedEntry;
}
```

当 `entry == unflushedEntry`（或 null）时，说明已经遍历完了所有 flushed Entry，应该停止。

---

## 三、write() 的完整源码链路

### 3.1 AbstractUnsafe.write()

当 write 操作沿 ChannelPipeline 传播到 HeadContext 后，调用 AbstractUnsafe.write()：

```java
// AbstractChannel.AbstractUnsafe

@Override
public final void write(Object msg, ChannelPromise promise) {
    assertEventLoop();  // 确保在 EventLoop 线程中执行

    ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
    if (outboundBuffer == null) {
        // Channel 已关闭，释放消息并失败
        try {
            ReferenceCountUtil.release(msg);
        } finally {
            safeSetFailure(promise, newClosedChannelException(initialCloseCause, "write(Object, ChannelPromise)"));
        }
        return;
    }

    int size;
    try {
        // 1. 过滤消息：堆内存 ByteBuf 转为直接内存
        msg = filterOutboundMessage(msg);
        // 2. 估算消息大小
        size = pipeline.estimatorHandle().size(msg);
        if (size < 0) {
            size = 0;
        }
    } catch (Throwable t) {
        try {
            ReferenceCountUtil.release(msg);
        } finally {
            safeSetFailure(promise, t);
        }
        return;
    }

    // 3. 追加到 ChannelOutboundBuffer 的 unflushed 链表
    outboundBuffer.addMessage(msg, size, promise);
}
```

### 3.2 filterOutboundMessage()：堆内存转直接内存

AbstractNioByteChannel 重写了 filterOutboundMessage，将堆内存 ByteBuf 转换为直接内存 ByteBuf：

```java
// AbstractNioByteChannel.java

@Override
protected final Object filterOutboundMessage(Object msg) {
    if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        if (buf.isDirect()) {
            // 已经是直接内存，无需转换
            return msg;
        }
        // 堆内存转直接内存
        return newDirectBuffer(buf);
    }

    if (msg instanceof FileRegion) {
        return msg;  // FileRegion 不需要转换
    }

    throw new UnsupportedOperationException(
            "unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
}
```

**为什么要转直接内存**：NIO 的 `SocketChannel.write(ByteBuffer)` 如果传入的是堆内存 ByteBuffer，JDK 内部会先分配一块临时直接内存，把堆内存数据拷贝过去，再写入 Socket。这个临时分配 + 拷贝在高并发场景下是严重的性能开销。Netty 提前在 filterOutboundMessage 中完成转换，并利用池化的直接内存，避免了 JDK 内部的临时分配。

转换通过 `newDirectBuffer(buf)` 完成，底层调用 `ByteBufAllocator` 的 `directBuffer()` 方法分配池化直接内存，然后将堆内存数据拷贝过去，并释放原始的堆内存 ByteBuf。

### 3.3 MessageSizeEstimator 估算消息大小

`pipeline.estimatorHandle().size(msg)` 用于估算消息的字节大小，这个值会用于水位线计算。DefaultMessageSizeEstimator 的实现：

```java
// DefaultMessageSizeEstimator.Handle

@Override
public int size(Object msg) {
    if (msg instanceof ByteBuf) {
        return ((ByteBuf) msg).readableBytes();
    }
    if (msg instanceof ByteBufHolder) {
        return ((ByteBufHolder) msg).content().readableBytes();
    }
    if (msg instanceof FileRegion) {
        return 0;  // FileRegion 不计入内存水位线
    }
    return -1;  // 未知大小，按 0 处理
}
```

FileRegion 返回 0 是因为它代表的是文件传输（零拷贝），不占用用户态堆外内存。

### 3.4 write() 完整链路图

```
ctx.write(msg, promise)
    │
    ▼
ChannelPipeline 出站传播（tail → head）
    │
    ▼
HeadContext.write(channel, msg, promise)
    │
    ▼
AbstractUnsafe.write(msg, promise)
    │
    ├─ assertEventLoop()
    │
    ├─ filterOutboundMessage(msg)
    │   └─ 堆内存 ByteBuf → newDirectBuffer() → 池化直接内存 ByteBuf
    │      （释放原始堆内存 ByteBuf 的引用）
    │
    ├─ pipeline.estimatorHandle().size(msg)
    │   └─ ByteBuf.readableBytes() → 估算字节数
    │
    └─ outboundBuffer.addMessage(msg, size, promise)
        ├─ Entry.newInstance(msg, size, total, promise)  ← 从 Recycler 获取
        ├─ tailEntry.next = entry; tailEntry = entry     ← 追加到链表尾部
        ├─ unflushedEntry ??= entry                       ← 更新 unflushed 指针
        ├─ msg.touch()                                    ← 泄漏检测辅助
        └─ incrementPendingOutboundBytes(pendingSize)
            └─ TOTAL_PENDING_SIZE_UPDATER.addAndGet()
                └─ if (newSize > highWaterMark) → setUnwritable()
                    └─ fireChannelWritabilityChanged()
```

---

## 四、flush() 的完整源码链路

### 4.1 AbstractUnsafe.flush()

```java
// AbstractChannel.AbstractUnsafe

@Override
public final void flush() {
    assertEventLoop();

    ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
    if (outboundBuffer == null) {
        return;
    }

    outboundBuffer.addFlush();  // unflushed → flushed
    flush0();                    // 执行真正的写入
}
```

flush() 方法非常简洁：先调用 addFlush() 将 unflushed 链表标记为 flushed，再调用 flush0() 执行真正的 I/O 写入。

### 4.2 flush0()：防止重入 + 调用 doWrite

```java
// AbstractChannel.AbstractUnsafe

@SuppressWarnings("deprecation")
protected void flush0() {
    if (inFlush0) {
        // 防止重入：如果已经在 flush 过程中，直接返回
        return;
    }

    final ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
    if (outboundBuffer == null || outboundBuffer.isEmpty()) {
        // 没有待写数据，直接返回
        return;
    }

    inFlush0 = true;  // 标记正在 flush

    // Channel 不活跃时，将所有 flushed 消息标记为失败
    if (!isActive()) {
        try {
            if (!outboundBuffer.isEmpty()) {
                if (isOpen()) {
                    outboundBuffer.failFlushed(new NotYetConnectedException(), true);
                } else {
                    outboundBuffer.failFlushed(newClosedChannelException(initialCloseCause, "flush0()"), false);
                }
            }
        } finally {
            inFlush0 = false;
        }
        return;
    }

    try {
        // 调用子类实现的 doWrite 执行真正的 I/O 写入
        doWrite(outboundBuffer);
    } catch (Throwable t) {
        handleWriteError(t);
    } finally {
        inFlush0 = false;
    }
}
```

`inFlush0` 标志位防止重入：在 flush0() 执行过程中，如果 doWrite() 触发了某些回调（比如 ChannelFutureListener），而回调中又调用了 flush()，就会形成递归调用。inFlush0 确保同一时间只有一个 flush0() 在执行，重入的 flush() 直接返回。

### 4.3 AbstractNioByteChannel.doWrite()：writeSpinCount 循环

AbstractNioByteChannel 提供了通用的 doWrite() 实现，NioSocketChannel 会重写它以支持 gathering write。我们先看通用实现：

```java
// AbstractNioByteChannel.java

@Override
protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    int writeSpinCount = config().getWriteSpinCount();
    do {
        Object msg = in.current();  // 获取 flushedEntry.msg
        if (msg == null) {
            // 所有消息都写完了，清除 OP_WRITE
            clearOpWrite();
            return;
        }
        // 写入单条消息，返回消耗的 spinCount
        writeSpinCount -= doWriteInternal(in, msg);
    } while (writeSpinCount > 0);

    // spinCount 用完了还没写完，需要处理未完成写入
    incompleteWrite(writeSpinCount < 0);
}
```

`writeSpinCount` 默认值是 16（来自 ChannelMetadata）：

```java
private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
```

这个值的含义是：doWrite() 最多尝试 16 次写入循环。如果 16 次之内所有数据都写完了，就正常返回；如果 16 次之后还有数据没写完，就调用 `incompleteWrite()` 处理。

### 4.4 doWriteInternal()：单条消息写入

```java
// AbstractNioByteChannel.java

private int doWriteInternal(ChannelOutboundBuffer in, Object msg) throws Exception {
    if (msg instanceof ByteBuf) {
        ByteBuf buf = (ByteBuf) msg;
        if (!buf.isReadable()) {
            // 空 ByteBuf，直接移除
            in.remove();
            return 0;
        }

        final int localFlushedAmount = doWriteBytes(buf);  // 调用子类的写入方法
        if (localFlushedAmount > 0) {
            in.progress(localFlushedAmount);  // 更新写入进度
            if (!buf.isReadable()) {
                in.remove();  // 写完了，移除 Entry
            }
            return 1;  // 消耗 1 个 spinCount
        }
    } else if (msg instanceof FileRegion) {
        FileRegion region = (FileRegion) msg;
        if (region.transferred() >= region.count()) {
            in.remove();
            return 0;
        }

        long localFlushedAmount = doWriteFileRegion(region);
        if (localFlushedAmount > 0) {
            in.progress(localFlushedAmount);
            if (region.transferred() >= region.count()) {
                in.remove();
            }
            return 1;
        }
    } else {
        throw new Error("Unexpected message type: " + className(msg));
    }
    // 写入 0 字节，说明 Socket 发送缓冲区满了
    return WRITE_STATUS_SNDBUF_FULL;  // Integer.MAX_VALUE
}
```

当 `doWriteBytes()` 返回 0（写入 0 字节）时，`doWriteInternal()` 返回 `WRITE_STATUS_SNDBUF_FULL`（即 `Integer.MAX_VALUE`）。回到 doWrite() 中，`writeSpinCount -= Integer.MAX_VALUE` 会让 writeSpinCount 变成一个很大的负数，然后 `writeSpinCount < 0` 为 true，`incompleteWrite(true)` 会被调用。

### 4.5 incompleteWrite()：处理未完成写入的两种策略

```java
// AbstractNioByteChannel.java

protected final void incompleteWrite(boolean setOpWrite) {
    if (setOpWrite) {
        // 策略一：Socket 发送缓冲区满了，注册 OP_WRITE 事件
        // 等 Socket 缓冲区有空间时，Selector 会通知我们
        setOpWrite();
    } else {
        // 策略二：writeSpinCount 用完了（16 次循环用尽），
        // 但 Socket 缓冲区可能还有空间
        clearOpWrite();
        // 提交 flushTask 到 EventLoop 任务队列，下一轮继续写
        eventLoop().execute(flushTask);
    }
}
```

两种策略的触发条件不同：

**策略一（setOpWrite = true）**：当 `writeSpinCount < 0` 时触发。这意味着 `doWriteInternal()` 返回了 `WRITE_STATUS_SNDBUF_FULL`，即 Socket 发送缓冲区已满，write() 系统调用返回 0。此时不应该空转浪费 CPU，而应该注册 OP_WRITE 事件，让 Selector 在 Socket 缓冲区有空间时通知我们。

```java
protected final void setOpWrite() {
    final IoRegistration registration = registration();
    if (!registration.isValid()) {
        return;
    }
    addAndSubmit(NioIoOps.WRITE);  // 注册 OP_WRITE 感兴趣事件
}
```

当 Selector 检测到 Socket 可写时，会触发 `NioByteUnsafe` 的 ready 处理逻辑，最终重新调用 `flush0()` 继续写入。

**策略二（setOpWrite = false）**：当 `writeSpinCount == 0` 时触发。这意味着 16 次循环用完了，但最后一次写入可能成功了（返回 1 而非 SNDBUF_FULL）。此时 Socket 缓冲区可能还有空间，但 Netty 不想让写操作占满整个 EventLoop 的时间（EventLoop 还需要处理其他 Channel 的 IO 和任务），所以先清除 OP_WRITE，然后提交一个 flushTask 到任务队列，等 EventLoop 处理完其他任务后再继续写。

```java
private final Runnable flushTask = new Runnable() {
    @Override
    public void run() {
        ((AbstractNioUnsafe) unsafe()).flush0();
    }
};
```

这两种策略的协作可以用下面的流程图表示：

```
doWrite() 开始
    │
    ▼
┌─── doWriteInternal() ─────────────────────────┐
│                                                │
│  写入成功?                                     │
│    ├─ 是 → return 1                            │
│    └─ 否（SNDBUF_FULL）→ return MAX_VALUE      │
│                                                │
└────────────────────────────────────────────────┘
    │
    ▼
writeSpinCount -= return_value
    │
    ├─ writeSpinCount > 0 → 继续循环 doWriteInternal()
    │
    ├─ writeSpinCount < 0（SNDBUF_FULL）→ incompleteWrite(true)
    │   └─ setOpWrite() → 注册 OP_WRITE
    │       └─ Selector 通知 Socket 可写 → 重新 flush0()
    │
    └─ writeSpinCount == 0（16 次用完）→ incompleteWrite(false)
        └─ clearOpWrite() + eventLoop.execute(flushTask)
            └─ EventLoop 下轮执行 flushTask → 重新 flush0()
```

---

## 五、NioSocketChannel.doWrite()：Gathering Write 实现

NioSocketChannel 重写了 doWrite()，使用 JDK 的 gathering write（`writev` 系统调用）一次性写入多个 ByteBuffer，大幅提升写入效率。

```java
// NioSocketChannel.java

@Override
protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    SocketChannel ch = javaChannel();
    int writeSpinCount = config().getWriteSpinCount();
    do {
        if (in.isEmpty()) {
            // 所有 flushed 消息都写完了
            clearOpWrite();
            return;
        }

        // 1. 收集 flushed 链表中的 ByteBuffer 数组
        int maxBytesPerGatheringWrite = ((NioSocketChannelConfig) config).getMaxBytesPerGatheringWrite();
        ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
        int nioBufferCnt = in.nioBufferCount();

        // 2. 根据 ByteBuffer 数量选择写入策略
        switch (nioBufferCnt) {
            case 0:
                // 有非 ByteBuf 的消息（如 FileRegion），退回单条写入
                writeSpinCount -= doWrite0(in);
                break;
            case 1: {
                // 只有一个 ByteBuffer，用普通的 write(ByteBuffer)
                ByteBuffer buffer = nioBuffers[0];
                int attemptedBytes = buffer.remaining();
                final int localWrittenBytes = ch.write(buffer);
                if (localWrittenBytes <= 0) {
                    // 写入 0 字节，Socket 缓冲区满了
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
                break;
            }
            default: {
                // 多个 ByteBuffer，用 gathering write: ch.write(ByteBuffer[], 0, cnt)
                long attemptedBytes = in.nioBufferSize();
                final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);
                if (localWrittenBytes <= 0) {
                    incompleteWrite(true);
                    return;
                }
                adjustMaxBytesPerGatheringWrite((int) attemptedBytes, (int) localWrittenBytes,
                        maxBytesPerGatheringWrite);
                in.removeBytes(localWrittenBytes);
                --writeSpinCount;
                break;
            }
        }
    } while (writeSpinCount > 0);

    incompleteWrite(writeSpinCount < 0);
}
```

### 5.1 三种写入路径

这个方法根据 `nioBufferCnt` 分为三条路径：

**case 0**：flushed 链表中有非 ByteBuf 消息（如 FileRegion），`nioBuffers()` 返回空数组。此时退回到 `doWrite0(in)`，走 AbstractNioByteChannel 的单条写入逻辑。

**case 1**：flushed 链表中只有一个可写的 ByteBuf（或第一个 ByteBuf 超过了 maxBytes 限制导致只收集了一个）。使用 `SocketChannel.write(ByteBuffer)` 单 Buffer 写入。

**default**：flushed 链表中有多个可写的 ByteBuf。使用 `SocketChannel.write(ByteBuffer[], 0, nioBufferCnt)` 进行 gathering write。底层调用操作系统的 `writev` 系统调用，将多个不连续的内存区域在一次系统调用中写入 Socket，避免了多次 write() 的系统调用开销。

### 5.2 adjustMaxBytesPerGatheringWrite：动态调整单次最大写入量

```java
// NioSocketChannel.java

private void adjustMaxBytesPerGatheringWrite(int attempted, int written, int oldMaxBytesPerGatheringWrite) {
    if (attempted == written) {
        // 全部写完了，尝试翻倍下次的最大写入量
        if (attempted << 1 > oldMaxBytesPerGatheringWrite) {
            ((NioSocketChannelConfig) config).setMaxBytesPerGatheringWrite(attempted << 1);
        }
    } else if (attempted > MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD && written < attempted >>> 1) {
        // 写不到一半，说明 Socket 缓冲区比较小，减半下次的最大写入量
        ((NioSocketChannelConfig) config).setMaxBytesPerGatheringWrite(attempted >>> 1);
    }
}
```

其中 `MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD = 4096`。

这个自适应策略的逻辑是：如果每次尝试的字节都全部写完了（`attempted == written`），说明 Socket 缓冲区还有余量，下次可以尝试翻倍，收集更多数据一次性写入。如果写入量不到尝试量的一半（`written < attempted >>> 1`），说明 Socket 缓冲区比较小或网络拥塞，下次应该减少尝试量，避免收集过多数据却写不出去。

初始值在 NioSocketChannelConfig 构造时设置：

```java
private void calculateMaxBytesPerGatheringWrite() {
    // 取 SO_SNDBUF × 2 作为初始值，给一些余量
    int newSendBufferSize = getSendBufferSize() << 1;
    if (newSendBufferSize > 0) {
        setMaxBytesPerGatheringWrite(newSendBufferSize);
    }
}
```

### 5.3 Gathering Write 的完整流程图

```
NioSocketChannel.doWrite(outboundBuffer)
    │
    ├─ in.isEmpty()? → clearOpWrite(); return
    │
    ├─ nioBuffers(1024, maxBytesPerGatheringWrite)
    │   │  遍历 flushed 链表，收集 ByteBuffer[]
    │   │  ┌──────┐  ┌──────┐  ┌──────┐
    │   │  │Buf 1 │  │Buf 2 │  │Buf 3 │  → nioBuffers[]
    │   │  │100B  │  │80B   │  │120B  │  总计 300B
    │   │  └──────┘  └──────┘  └──────┘
    │   └─ return nioBuffers[]（ThreadLocal 缓存）
    │
    ├─ nioBufferCnt = 0? → doWrite0(in)  [退回单条写入]
    │
    ├─ nioBufferCnt = 1? → ch.write(nioBuffers[0])
    │                       └─ 一次 write() 系统调用
    │
    └─ nioBufferCnt > 1? → ch.write(nioBuffers, 0, cnt)
                            └─ 一次 writev() 系统调用
                                一次性写入多个不连续内存区域

    写入结果:
    ├─ localWrittenBytes > 0 → removeBytes(localWrittenBytes)
    │   ├─ 完全写完的 ByteBuf → remove() + 回收 Entry
    │   └─ 部分写入的 ByteBuf → 更新 readerIndex
    │
    ├─ localWrittenBytes <= 0 → incompleteWrite(true)
    │   └─ setOpWrite() → 等 Selector 通知
    │
    └─ writeSpinCount 用完 → incompleteWrite(false)
        └─ eventLoop.execute(flushTask)
```

---

## 六、水位线机制（WriteBufferWaterMark）

### 6.1 水位线的设计目标

在网络通信中，如果生产者（写入端）的速度远大于消费者（网络传输 + 对端读取）的速度，写入端会不断往 ChannelOutboundBuffer 中堆积数据，最终导致 OOM。水位线机制就是 Netty 的背压（Backpressure）方案：当待处理数据量超过高水位线时，标记 Channel 为不可写状态，通知上游停止写入；当数据量降到低水位线以下时，标记为可写状态，通知上游可以继续写入。

### 6.2 WriteBufferWaterMark 的定义

```java
// WriteBufferWaterMark.java

public final class WriteBufferWaterMark {
    private static final int DEFAULT_LOW_WATER_MARK = 32 * 1024;   // 32 KB
    private static final int DEFAULT_HIGH_WATER_MARK = 64 * 1024;  // 64 KB

    public static final WriteBufferWaterMark DEFAULT =
            new WriteBufferWaterMark(DEFAULT_LOW_WATER_MARK, DEFAULT_HIGH_WATER_MARK, false);

    private final int low;
    private final int high;
}
```

默认低水位线 32KB，高水位线 64KB。高低水位之间有一个 32KB 的缓冲带，这是为了避免在临界点反复触发可写/不可写状态切换（抖动）。

### 6.3 totalPendingSize 的原子更新

ChannelOutboundBuffer 使用 AtomicLongFieldUpdater 来原子更新待处理字节数：

```java
// ChannelOutboundBuffer.java

private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER =
        AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");

private volatile long totalPendingSize;
```

之所以用 AtomicLongFieldUpdater 而不是 AtomicLong，是为了节省一个对象引用的内存开销——updater 是静态字段，所有 ChannelOutboundBuffer 实例共享一个 updater，而 `totalPendingSize` 直接是 long 字段。

### 6.4 incrementPendingOutboundBytes()：超过高水位线 → setUnwritable

```java
// ChannelOutboundBuffer.java

private void incrementPendingOutboundBytes(long size, boolean invokeLater) {
    if (size == 0) {
        return;
    }

    long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, size);
    if (newWriteBufferSize > channel.config().getWriteBufferHighWaterMark()) {
        setUnwritable(invokeLater);
    }
}
```

当 addMessage() 调用 `incrementPendingOutboundBytes(entry.pendingSize, false)` 时，先将 pendingSize 原子加到 totalPendingSize 上，然后检查是否超过高水位线。如果超过了，调用 `setUnwritable()` 标记 Channel 为不可写。

注意 `addMessage` 中传入的 `invokeLater = false`，意味着在 EventLoop 线程内直接触发 `fireChannelWritabilityChanged()` 事件，不需要延迟到任务队列。

### 6.5 decrementPendingOutboundBytes()：低于低水位线 → setWritable

```java
// ChannelOutboundBuffer.java

private void decrementPendingOutboundBytes(long size, boolean invokeLater, boolean notifyWritability) {
    if (size == 0) {
        return;
    }

    long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
    if (notifyWritability && newWriteBufferSize < channel.config().getWriteBufferLowWaterMark()) {
        setWritable(invokeLater);
    }
}
```

当 remove() 调用 `decrementPendingOutboundBytes(size, false, true)` 时，先原子减去 size，然后检查是否低于低水位线。注意这里用的是 `<` 而不是 `<=`，且阈值是 lowWaterMark 而不是 highWaterMark。

### 6.6 setUnwritable() / setWritable()：位运算 + CAS

这是水位线机制最精妙的部分。Netty 用一个 int 字段 `unwritable` 的不同位来表示不同的不可写原因：

```java
private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> UNWRITABLE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "unwritable");

private volatile int unwritable;
```

第 0 位（`1 << 0`）表示"因待处理数据超过高水位线而不可写"。第 1~31 位留给用户自定义的不可写原因（通过 `setUserDefinedWritability(int index, boolean writable)`）。

`isWritable()` 判断很简单——只要 unwritable 为 0 就是可写的：

```java
public boolean isWritable() {
    return unwritable == 0;
}
```

`setUnwritable()` 的实现：

```java
private void setUnwritable(boolean invokeLater) {
    for (;;) {
        final int oldValue = unwritable;
        final int newValue = oldValue | 1;  // 置第 0 位为 1
        if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
            // 只有从 0 变成非 0 时才触发事件（避免重复通知）
            if (oldValue == 0) {
                fireChannelWritabilityChanged(invokeLater);
            }
            break;
        }
    }
}
```

`setWritable()` 的实现：

```java
private void setWritable(boolean invokeLater) {
    for (;;) {
        final int oldValue = unwritable;
        final int newValue = oldValue & ~1;  // 清除第 0 位
        if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
            // 只有从非 0 变成 0 时才触发事件（避免重复通知）
            if (oldValue != 0 && newValue == 0) {
                fireChannelWritabilityChanged(invokeLater);
            }
            break;
        }
    }
}
```

关键设计：**只在状态切换时触发事件**。setUnwritable 只在 `oldValue == 0`（从可写变为不可写）时才触发 `fireChannelWritabilityChanged`；setWritable 只在 `oldValue != 0 && newValue == 0`（从不可写变为可写）时才触发。这避免了在高水位线附近反复触发事件的问题。

假设 totalPendingSize 从 60KB 增长到 70KB 再到 80KB：

```
60KB: totalPendingSize < 64KB → writable = 0 (可写)
70KB: totalPendingSize > 64KB → setUnwritable()
      oldValue=0, newValue=1 → 触发 fireChannelWritabilityChanged ✅
80KB: totalPendingSize > 64KB → setUnwritable()
      oldValue=1, newValue=1 → 不触发 ❌（已经是不可写状态了）

80KB → 50KB（remove 30KB）:
50KB: totalPendingSize > 32KB → 不触发 setWritable
50KB → 25KB（再 remove 25KB）:
25KB: totalPendingSize < 32KB → setWritable()
      oldValue=1, newValue=0 → 触发 fireChannelWritabilityChanged ✅
```

### 6.7 fireChannelWritabilityChanged() 事件传播

```java
private void fireChannelWritabilityChanged(boolean invokeLater) {
    final ChannelPipeline pipeline = channel.pipeline();
    if (invokeLater) {
        Runnable task = fireChannelWritabilityChangedTask;
        if (task == null) {
            fireChannelWritabilityChangedTask = task = new Runnable() {
                @Override
                public void run() {
                    pipeline.fireChannelWritabilityChanged();
                }
            };
        }
        channel.eventLoop().execute(task);
    } else {
        pipeline.fireChannelWritabilityChanged();
    }
}
```

当 `invokeLater = true` 时（比如从非 EventLoop 线程调用 `setUserDefinedWritability`），事件传播被封装成 Runnable 提交到 EventLoop 任务队列，确保在正确的线程中执行。`invokeLater = false` 时（比如在 EventLoop 线程内的 addMessage/remove 操作），直接同步传播。

用户可以在 ChannelHandler 中监听这个事件来实现背压：

```java
public class BackpressureHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable()) {
            // Channel 恢复可写，恢复读取上游数据
            ctx.channel().config().setAutoRead(true);
        } else {
            // Channel 不可写，暂停读取上游数据
            ctx.channel().config().setAutoRead(false);
        }
    }
}
```

### 6.8 水位线机制完整流程图

```
                    totalPendingSize
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
        0 KB          32 KB         64 KB
                     (low)          (high)
          │              │              │
          │   可写区域    │  缓冲带      │  不可写区域
          │              │              │
          │              │              │
    setWritable       不触发事件     setUnwritable
    (oldValue!=0      (在高低水位     (oldValue==0
     && newValue==0    之间不变化      → 触发事件)
     → 触发事件)       状态)          

  incrementPendingOutboundBytes(size)
    → newTotal = totalPendingSize + size
    → if (newTotal > highWaterMark) → setUnwritable()

  decrementPendingOutboundBytes(size)
    → newTotal = totalPendingSize - size
    → if (newTotal < lowWaterMark) → setWritable()
```

---

## 七、VoidPromise 优化

### 7.1 为什么需要 VoidPromise

每次 write() 都需要一个 ChannelPromise 来通知写入完成。对于高频写入场景（如每秒数万次 write），如果每次都创建 DefaultChannelPromise 对象，会产生大量短生命周期对象，增加 GC 压力。

更关键的是，很多场景下用户根本不关心写入是否完成——比如在 `channelActive()` 中写入欢迎消息，或者在 `channelRead()` 中写入响应后立即 flush。此时创建 Promise 纯属浪费。

VoidChannelPromise 就是为此设计的"空 Promise"：所有成功通知都是 no-op，不分配任何资源。

### 7.2 VoidChannelPromise 源码

```java
// VoidChannelPromise.java

public final class VoidChannelPromise extends AbstractFuture<Void> implements ChannelPromise {

    private final Channel channel;
    private final ChannelFutureListener fireExceptionListener;

    public VoidChannelPromise(final Channel channel, boolean fireException) {
        this.channel = channel;
        if (fireException) {
            // 如果需要传播异常，创建一个 listener
            fireExceptionListener = new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    Throwable cause = future.cause();
                    if (cause != null) {
                        fireException0(cause);
                    }
                }
            };
        } else {
            fireExceptionListener = null;
        }
    }

    // 成功通知：no-op，直接返回 this
    @Override
    public VoidChannelPromise setSuccess() {
        return this;
    }

    @Override
    public boolean trySuccess() {
        return false;  // 返回 false 表示"我不会被通知"
    }

    // 失败通知：通过 fireExceptionCaught 传播异常
    @Override
    public VoidChannelPromise setFailure(Throwable cause) {
        fireException0(cause);
        return this;
    }

    @Override
    public boolean tryFailure(Throwable cause) {
        fireException0(cause);
        return false;
    }

    // 添加 listener：直接抛异常！
    @Override
    public VoidChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        fail();  // throw new IllegalStateException("void future")
        return this;
    }

    // isDone 永远返回 false
    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    // 标记为 void promise
    @Override
    public boolean isVoid() {
        return true;
    }

    // unvoid() 创建一个真实的 Promise
    @Override
    public ChannelPromise unvoid() {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        if (fireExceptionListener != null) {
            promise.addListener(fireExceptionListener);
        }
        return promise;
    }
}
```

### 7.3 VoidPromise 在写入流程中的优化点

在 ChannelOutboundBuffer 中，多处代码对 VoidChannelPromise 做了特殊优化：

**safeSuccess / safeFail 的日志优化**：

```java
private static void safeSuccess(ChannelPromise promise) {
    // VoidChannelPromise 的 trySuccess 返回 false 是预期行为，不需要记录日志
    PromiseNotificationUtil.trySuccess(promise, null, promise instanceof VoidChannelPromise ? null : logger);
}
```

**progress 的快速路径**：

```java
public void progress(long amount) {
    Entry e = flushedEntry;
    ChannelPromise p = e.promise;
    // fast-path：VoidChannelPromise 和 DefaultChannelPromise 不支持进度通知，直接跳过
    final Class<?> promiseClass = p.getClass();
    if (promiseClass == VoidChannelPromise.class || promiseClass == DefaultChannelPromise.class) {
        return;
    }
    // 只有 ChannelProgressivePromise 才需要通知进度
    if (p instanceof DefaultChannelProgressivePromise) {
        ((DefaultChannelProgressivePromise) p).tryProgress(progress, e.total);
    } else if (p instanceof ChannelProgressivePromise) {
        ((ChannelProgressivePromise) p).tryProgress(progress, e.total);
    }
}
```

这里用 `promiseClass == VoidChannelPromise.class` 做精确类型匹配（而不是 instanceof），是因为 JDK 的类型污染问题（[JDK-8180450](https://bugs.openjdk.org/browse/JDK-8180450)）会导致 instanceof 检查在多态场景下性能下降。

### 7.4 何时使用 VoidPromise

Netty 内部在不需要通知写入完成的地方广泛使用 VoidPromise。例如：

```java
// AbstractChannel.java 中的 voidPromise() 方法
@Override
public ChannelFuture voidPromise() {
    return newVoidPromise(channel);
}

@Override
public ChannelPromise newVoidPromise(Channel channel) {
    return new VoidChannelPromise(channel, true);  // fireException=true
}
```

用户也可以主动使用：

```java
// 不关心写入结果，使用 voidPromise 避免创建 Promise 对象
channel.write(msg, channel.voidPromise());

// 或者更简单地使用 writeAndFlush 的 void 版本
channel.writeAndFlush(msg, channel.voidPromise());
```

但要注意：VoidPromise 不支持 `addListener()`，调用会直接抛异常。如果你需要知道写入是否完成，必须使用普通 Promise。

---

## 八、反面教材：常见误用与正确写法

### 8.1 只 write 不 flush

**错误写法**：

```java
public class MyHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // 处理请求后写入响应
        ByteBuf response = ctx.alloc().buffer();
        response.writeBytes("HTTP/1.1 200 OK\r\n\r\n".getBytes());
        ctx.write(response);  // ❌ 忘记 flush！
        // 对端永远收不到响应，数据卡在 ChannelOutboundBuffer 中
    }
}
```

**问题分析**：write() 只是把数据追加到 ChannelOutboundBuffer 的 unflushed 链表，不会触发任何 I/O 操作。数据会一直留在内存中，直到下一次 flush() 或 Channel 关闭。

**正确写法**：

```java
public class MyHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf response = ctx.alloc().buffer();
        response.writeBytes("HTTP/1.1 200 OK\r\n\r\n".getBytes());
        ctx.writeAndFlush(response);  // ✅ write + flush 一步到位
    }
}
```

或者分步调用：

```java
ctx.write(response);
ctx.flush();  // 显式 flush
```

或者使用 `writeAndFlush()` 的变体，在 pipeline 中传播 write 和 flush 两个事件。

### 8.2 写太快不检查 isWritable()

**错误写法**：

```java
public class FastWriter extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 疯狂写入，不检查 Channel 是否还能写
        for (int i = 0; i < 1000000; i++) {
            ByteBuf buf = ctx.alloc().buffer(1024);
            buf.writeBytes(("message-" + i + "\n").getBytes());
            ctx.writeAndFlush(buf);
        }
        // ❌ 如果网络速度跟不上写入速度，
        // ChannelOutboundBuffer 会无限膨胀，最终 OOM
    }
}
```

**问题分析**：当对端读取速度慢于写入速度时，数据会在 ChannelOutboundBuffer 中堆积。由于没有检查 isWritable()，totalPendingSize 会持续增长，最终耗尽内存。虽然水位线机制会触发 `channelWritabilityChanged()` 事件，但如果不监听和处理这个事件，写入不会停止。

**正确写法**：

```java
public class SafeWriter extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        writeBatch(ctx, 0);
    }

    private void writeBatch(ChannelHandlerContext ctx, int start) {
        if (!ctx.channel().isWritable()) {
            // Channel 不可写，暂停写入
            // 等 channelWritabilityChanged 事件恢复后再继续
            return;
        }

        int batchSize = Math.min(100, 1000000 - start);
        for (int i = 0; i < batchSize; i++) {
            ByteBuf buf = ctx.alloc().buffer(1024);
            buf.writeBytes(("message-" + (start + i) + "\n").getBytes());
            ctx.writeAndFlush(buf);
        }

        if (start + batchSize < 1000000) {
            // 通过 EventLoop 调度下一批
            final int nextStart = start + batchSize;
            ctx.channel().eventLoop().execute(() -> writeBatch(ctx, nextStart));
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable()) {
            // Channel 恢复可写，继续写入
            writeBatch(ctx, lastWrittenIndex);
        }
    }
}
```

更简洁的做法是利用 AutoRead 机制实现自动背压：

```java
public class AutoBackpressureHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable()) {
            // 恢复读取上游数据
            ctx.channel().config().setAutoRead(true);
        } else {
            // 暂停读取上游数据
            ctx.channel().config().setAutoRead(false);
        }
    }
}
```

当 AutoRead 为 false 时，Netty 不会注册 OP_READ 事件，上游数据停止流入。当 Channel 恢复可写时，重新开启 AutoRead，形成自然的背压链路。

### 8.3 在 EventLoop 线程外直接操作 ByteBuf

**错误写法**：

```java
public class WrongBufferUsage {
    public void sendMessage(Channel channel, byte[] data) {
        // 在业务线程中分配 ByteBuf
        ByteBuf buf = channel.alloc().buffer(data.length);  // ❌ 可能不是 EventLoop 线程
        buf.writeBytes(data);
        
        // 在业务线程中直接写入 ByteBuf
        channel.writeAndFlush(buf);  // ByteBuf 的操作可能不是线程安全的
    }
}
```

**问题分析**：ByteBuf 本身在未被加入 ChannelOutboundBuffer 之前不是线程安全的。在非 EventLoop 线程中分配和操作 ByteBuf 可能导致 PoolThreadCache 的线程绑定被破坏（PooledByteBufAllocator 的 PoolThreadCache 是 ThreadLocal 的，在非 EventLoop 线程中分配的 ByteBuf 使用的是当前线程的 PoolThreadCache，释放时却可能在 EventLoop 线程中执行，导致内存归还到错误的缓存）。更严重的是，如果在多线程中同时操作同一个 ByteBuf，会导致数据竞争。

**正确写法**：

```java
public class CorrectBufferUsage {
    public void sendMessage(Channel channel, byte[] data) {
        // 方式一：将操作提交到 EventLoop 线程执行
        channel.eventLoop().execute(() -> {
            ByteBuf buf = channel.alloc().buffer(data.length);
            buf.writeBytes(data);
            channel.writeAndFlush(buf);
        });

        // 方式二：如果必须在外部线程构造数据，先拷贝到不可变对象
        // 然后 writeAndFlush 会自动将操作转发到 EventLoop
        channel.writeAndFlush(Unpooled.wrappedBuffer(data));
        // writeAndFlush 内部会检查是否在 EventLoop 线程，
        // 如果不是，会创建一个 task 提交到 EventLoop
    }
}
```

实际上 `Channel.writeAndFlush()` 内部会检查当前线程是否是 EventLoop 线程：

```java
// AbstractChannelHandlerContext.java

private void write(Object msg, boolean flush, ChannelPromise promise) {
    // ... 省略找下一个节点的逻辑 ...
    if (executor.inEventLoop()) {
        if (flush) {
            next.invokeWriteAndFlush(m, promise);
        } else {
            next.invokeWrite(m, promise);
        }
    } else {
        // 不在 EventLoop 线程，封装成 task 提交
        final WriteTask task = WriteTask.newInstance(next, m, promise, flush);
        if (!safeExecuteAndSetCancellation(executor, task, promise, m)) {
            task.cancel();
        }
    }
}
```

所以 `channel.writeAndFlush()` 本身在非 EventLoop 线程调用是安全的（操作会被转发到 EventLoop），但 ByteBuf 的分配和填充操作应该尽量在 EventLoop 线程中进行，或者使用 `Unpooled.wrappedBuffer()` 等线程安全的方式。

### 8.4 忘记释放 ByteBuf 导致内存泄漏

**错误写法**：

```java
public class LeakHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        
        // ❌ 读取了 buf 的数据但没有释放 buf
        // 也没有调用 ctx.writeAndFlush(buf) 让 pipeline 后续处理释放
        
        process(data);  // 只处理了 byte[]，buf 泄漏了
    }
}
```

**正确写法**：

```java
public class CorrectHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        try {
            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            process(data);
        } finally {
            // ✅ 确保在任何情况下都释放 ByteBuf
            ReferenceCountUtil.release(msg);
        }
    }
}
```

或者使用 SimpleChannelInboundHandler，它会自动释放消息：

```java
public class AutoReleaseHandler extends SimpleChannelInboundHandler<ByteBuf> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) {
        // SimpleChannelInboundHandler 会在 channelRead0 返回后自动释放 buf
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        process(data);
    }
}
```

---

## 九、完整调用链总览

将前面所有环节串联起来，write() + flush() 的完整调用链如下：

```
┌─────────────────────────────────────────────────────────────────┐
│                        用户代码                                   │
│  ctx.write(msg, promise)    ctx.flush()                         │
└──────┬──────────────────────────┬───────────────────────────────┘
       │                          │
       ▼                          ▼
┌─────────────────┐    ┌──────────────────┐
│ ChannelPipeline │    │ ChannelPipeline  │
│ 出站传播         │    │ 出站传播          │
│ (tail → head)   │    │ (tail → head)    │
└──────┬──────────┘    └────────┬─────────┘
       │                        │
       ▼                        ▼
┌─────────────────┐    ┌──────────────────┐
│ HeadContext     │    │ HeadContext      │
│ .write()        │    │ .flush()         │
└──────┬──────────┘    └────────┬─────────┘
       │                        │
       ▼                        ▼
│ AbstractUnsafe.write() │  │ AbstractUnsafe.flush() │
│  ├─ filterOutboundMessage() │  │  ├─ addFlush()         │
│  │  └─ heap → direct     │  │  │  └─ unflushed → flushed │
│  ├─ estimatorHandle.size()  │  │  └─ flush0()           │
│  └─ addMessage()            │  │     └─ doWrite()       │
│     ├─ Entry.newInstance()  │  └───────────────────────┘
│     ├─ 追加到 tailEntry     │
│     ├─ touch() 泄漏检测     │
│     └─ incrementPending()   │
│        └─ > highWaterMark?  │
│           └─ setUnwritable()│
│              └─ fireChannel │
│                 Writability │
│                 Changed()   │
└─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│ NioSocketChannel.doWrite(outboundBuffer)        │
│  ├─ nioBuffers(1024, maxBytesPerGatheringWrite) │
│  │  └─ 遍历 flushed 链表收集 ByteBuffer[]        │
│  │     使用 ThreadLocal 缓存数组                  │
│  │                                               │
│  ├─ nioBufferCnt == 0 → doWrite0() [FileRegion] │
│  ├─ nioBufferCnt == 1 → ch.write(ByteBuffer)    │
│  └─ nioBufferCnt > 1 → ch.write(ByteBuffer[])   │
│                        └─ writev 系统调用        │
│  ├─ adjustMaxBytesPerGatheringWrite()            │
│  ├─ removeBytes(writtenBytes)                    │
│  │  ├─ 完全写完 → remove() + 回收 Entry           │
│  │  │  └─ decrementPendingOutboundBytes()        │
│  │  │     └─ < lowWaterMark? → setWritable()     │
│  │  │        └─ fireChannelWritabilityChanged()  │
│  │  └─ 部分写入 → 更新 readerIndex               │
│  │                                               │
│  ├─ writeSpinCount 用完?                         │
│  │  ├─ SNDBUF_FULL → setOpWrite() [注册 OP_WRITE]│
│  │  └─ spinCount==0 → eventLoop.execute(flushTask)│
│  └─ 全部写完 → clearOpWrite()                    │
└─────────────────────────────────────────────────┘
```

---

## 十、本篇涉及的设计模式

本篇涉及的写缓冲区与 Flush 机制中，大量运用了经典设计模式来解耦关注点、提升扩展性和性能。

**对象池模式（Object Pool）**贯穿了 ChannelOutboundBuffer 的整个设计。Entry 对象通过 Recycler 实现池化复用——addMessage 时从池中获取 Entry，remove 时归还到池中。在高频写入场景下，每秒可能创建和销毁数万个 Entry，对象池避免了这些短生命周期对象的 GC 开销。同样的思想也体现在 NIO_BUFFERS 的 FastThreadLocal 缓存上——ByteBuffer 数组被线程局部缓存，避免每次 gathering write 都分配新数组。这两个池化机制一前一后，一个复用 Java 对象，一个复用数组引用，共同构成了 Netty 写路径上零分配的基础。

**标记模式（Marker）**体现在 ChannelOutboundBuffer 的三指针链表设计上。flushedEntry、unflushedEntry、tailEntry 三个指针将同一个链表划分为两个逻辑区域，write() 操作 unflushed 区，doWrite() 操作 flushed 区，两套操作互不干扰。addFlush() 只需要移动指针（O(1) 操作）就能完成"区域迁移"，不需要拷贝或移动任何 Entry 对象。这种用指针标记区域边界的设计，比维护两个独立的链表更节省内存，且避免了链表合并时的指针修改开销。

**策略模式（Strategy）**体现在 incompleteWrite() 的两种处理策略上。当 Socket 缓冲区满时（writeSpinCount < 0），采用注册 OP_WRITE 事件的策略，让 Selector 在缓冲区有空闲时通知；当 writeSpinCount 耗尽但 Socket 可能还可用时（writeSpinCount == 0），采用提交 flushTask 到任务队列的策略，让 EventLoop 在处理完其他任务后继续写。两种策略根据不同的失败原因自动选择，调用方不需要关心具体实现。NioSocketChannel.doWrite() 中的 switch-case 也体现了策略模式——根据 nioBufferCnt 的值选择单 Buffer 写入、gathering write 或退回单条写入。

**观察者模式（Observer）**体现在水位线机制的事件通知上。totalPendingSize 的变化触发 setUnwritable/setWritable 状态切换，状态切换触发 fireChannelWritabilityChanged 事件，事件沿 ChannelPipeline 传播到所有注册了 channelWritabilityChanged 回调的 ChannelHandler。这种"数据变化 → 状态变化 → 事件通知"的链式反应是典型的观察者模式，实现了 ChannelOutboundBuffer（被观察者）与用户 Handler（观察者）之间的解耦。关键的是，事件只在状态真正变化时触发（0→非0 或 非0→0），避免了重复通知。

**模板方法模式（Template Method）**体现在 doWrite() 的继承层次上。AbstractNioByteChannel.doWrite() 提供了通用的单条写入循环框架（writeSpinCount 循环 + doWriteInternal + incompleteWrite），NioSocketChannel.doWrite() 重写它以支持 gathering write 优化。AbstractNioByteChannel.doWriteBytes() 是抽象方法，由 NioSocketChannel 实现为 `buf.readBytes(javaChannel())`。这种设计使得不同类型的 Channel（TCP、UDP、SCTP）可以复用通用的写入框架，同时在关键 I/O 方法上提供各自的高效实现。

---

## 十一、本篇涉及的高性能并发技术

本篇涉及的写缓冲区与 Flush 机制中，Netty 综合运用了多种高性能并发技术来保证在极高吞吐量下的稳定性和低延迟。

**无锁 CAS（Compare-And-Swap）**是水位线机制的并发基础。totalPendingSize 使用 AtomicLongFieldUpdater 进行 CAS 更新，unwritable 使用 AtomicIntegerFieldUpdater 进行 CAS 更新。选择 FieldUpdater 而不是 AtomicLong/AtomicInteger 是为了节省对象头开销——每个 ChannelOutboundBuffer 只需要一个 long 字段和一个 int 字段，而不是两个额外的 Atomic 对象。setUnwritable/setWritable 中的 CAS 自旋循环确保了在多线程环境下（比如 EventLoop 线程执行 addMessage 的同时，另一个线程调用 setUserDefinedWritability）状态更新的原子性。关键在于"只在状态翻转时触发事件"的设计——通过 `oldValue == 0` 和 `newValue == 0` 的判断，确保 fireChannelWritabilityChanged 只被调用一次，避免了高水位线附近的事件风暴。

**线程局部存储（Thread-Local Storage）**体现在 NIO_BUFFERS 的 FastThreadLocal 使用上。gathering write 需要一个 ByteBuffer 数组来收集多个 ByteBuf 的 NIO Buffer，如果每次都创建新数组会产生大量 GC 垃圾。FastThreadLocal（相比 JDK ThreadLocal 查找速度更快，直接用数组下标访问）为每个 EventLoop 线程缓存一个初始容量 1024 的 ByteBuffer 数组，多次 gathering write 共享同一个数组。这种设计的前提是 ChannelOutboundBuffer 的所有方法都在同一个 EventLoop 线程中调用（除了 isWritable 等少数方法），因此 ThreadLocal 缓存不会出现线程竞争。数组不足时通过 `expandNioBufferArray()` 扩容并更新 ThreadLocal，扩容后的数组对后续操作永久生效。

**Gathering Write（批量写入 / writev）**是 NioSocketChannel 性能优化的核心。当 flushed 链表中有多个 ByteBuf 时，Netty 不逐个调用 `SocketChannel.write(ByteBuffer)`（每次都是一次系统调用），而是将多个 ByteBuffer 收集到一个数组中，一次性调用 `SocketChannel.write(ByteBuffer[], 0, cnt)`。底层调用操作系统的 `writev` 系统调用，将多个不连续的内存区域在一次内核态切换中写入 Socket。在典型的 HTTP 响应场景（Header + Body 分属不同 ByteBuf），gathering write 将两次系统调用减少为一次，吞吐量提升显著。adjustMaxBytesPerGatheringWrite 的自适应调整策略进一步优化了 gathering write 的效率——全部写完则翻倍下次尝试量，写不到一半则减半，动态适应不同网络条件和 Socket 缓冲区大小。

**自旋写入与事件驱动协作（Spin-Write + Event-Driven Cooperation）**是 writeSpinCount 机制的核心设计。doWrite() 在一个循环中最多尝试 16 次写入（writeSpinCount），而不是写入一次就返回。这种自旋设计在 Socket 缓冲区有空间但单次 write 只写入部分数据时非常有效——通过连续多次写入可以快速清空 ChannelOutboundBuffer，减少 EventLoop 的调度次数。但自旋不能无限循环（会饿死其他 Channel 的 IO 处理），所以设置了 16 次上限。当 16 次用完但数据还没写完时，通过提交 flushTask 到任务队列让出 CPU；当 Socket 缓冲区满时，通过注册 OP_WRITE 事件切换到事件驱动模式。这种"自旋优先、事件兜底"的策略在低延迟和高吞吐之间取得了平衡。

**背压控制（Backpressure）**是水位线机制的核心价值。write() 路径上的 incrementPendingOutboundBytes 在数据量超过高水位线时触发 setUnwritable，使得 `channel.isWritable()` 返回 false；remove() 路径上的 decrementPendingOutboundBytes 在数据量低于低水位线时触发 setWritable，使得 `channel.isWritable()` 返回 true。高低水位线之间的缓冲带（默认 32KB）避免了状态抖动。配合 `channelWritabilityChanged()` 事件和 AutoRead 机制，Netty 构建了一条从"网络写入端"到"数据读取端"的完整背压链路：写入太快 → ChannelOutboundBuffer 膨胀 → 超过高水位 → isWritable()=false → 暂停 AutoRead → 停止读取上游数据 → 数据量下降 → 低于低水位 → isWritable()=true → 恢复 AutoRead → 继续读取。这种端到端的流控机制确保了在任何网络条件下都不会因写入速度过快而导致 OOM。

**对象池化与零分配（Object Pooling + Zero-Allocation）**是 Netty 写路径高性能的基础保障。Entry 对象通过 Recycler 池化，addMessage 时从池中获取、remove 时归还，整个写入和移除过程不创建任何新 Java 对象。ByteBuffer 数组通过 FastThreadLocal 缓存，nioBuffers() 方法复用同一个数组。ChannelOutboundBuffer 的 ByteBuffer 缓存（entry.buf / entry.bufs）避免了同一个 ByteBuf 多次提取 NIO ByteBuffer 的开销。这些零分配设计使得 Netty 在每秒数百万次写入的场景下，写路径上的 GC 压力趋近于零。VoidChannelPromise 进一步减少了不必要对象的创建——当不需要写入完成通知时，使用 voidPromise() 返回的共享 Promise 对象，所有成功通知都是 no-op，所有 addListener 调用直接抛异常（fast-fail），避免了 DefaultChannelPromise 中 listeners 列表的内存分配。
