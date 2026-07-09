# Netty HTTP 协议支持源码解析

> 基于 Netty 源码，深度解析 HTTP/1.1 协议在 Netty 中的完整实现。从 Pipeline 中 Handler 的组合策略，到 HttpObjectDecoder 的状态机解码过程，再到 HttpObjectAggregator 的聚合逻辑、Keep-Alive 机制、WebSocket 升级握手以及 HTTP/2 的简要概述。以一个完整的 HTTP 请求-响应报文作为贯穿案例，展示每一步解码和编码的输入输出。

---

## 一、HTTP 协议在 Pipeline 中的 Handler 组合

### 1.1 一个典型 HTTP Server 的 Pipeline 布局

当我们在 Netty 中搭建一个 HTTP 服务器时，最基础的 Pipeline 配置通常长这样：

```
ChannelPipeline:
  ┌──────────────────────────────────────────────────────────────────┐
  │  Head                                                            │
  │                                                                  │
  │  1. HttpServerCodec          ← 解码 HTTP 请求 + 编码 HTTP 响应    │
  │     ├─ HttpRequestDecoder    (Inbound, 字节 → HttpObject)        │
  │     └─ HttpResponseEncoder   (Outbound, HttpObject → 字节)       │
  │                                                                  │
  │  2. HttpObjectAggregator     ← 将分片的 HttpObject 聚合为 FullHttp│
  │                                                                  │
  │  3. HttpContentCompressor    ← 自动 Gzip 压缩响应体              │
  │                                                                  │
  │  4. HttpServerKeepAliveHandler ← 管理连接保活与关闭               │
  │                                                                  │
  │  5. BusinessHandler          ← 用户业务逻辑                      │
  │                                                                  │
  │  Tail                                                            │
  └──────────────────────────────────────────────────────────────────┘
```

这个 Pipeline 的每一层都有明确的职责边界。数据在 Inbound 方向（读取）自上而下流动，在 Outbound 方向（写入）自下而上流动。

以一个完整的 HTTP 请求-响应为例，数据流经 Pipeline 的过程如下：

```
客户端发送：
POST /api/upload HTTP/1.1
Host: example.com
Content-Type: text/plain
Content-Length: 26

Hello, Netty HTTP Server!

=== Inbound 方向（字节流 → 业务对象）===

[TCP 字节流到达]
    │
    ▼
┌─────────────────────────────────────────────┐
│ HttpServerCodec (内部 HttpRequestDecoder)   │
│                                             │
│ 字节流 → HttpRequest (headers 部分)          │
│       → HttpContent (body 部分, 可能有多个)   │
│       → LastHttpContent (最后一个 body 块)   │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│ HttpObjectAggregator                        │
│                                             │
│ HttpRequest + N×HttpContent + LastHttpContent│
│ → FullHttpRequest (完整请求, 含全部 body)    │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│ BusinessHandler                             │
│                                             │
│ 处理 FullHttpRequest, 生成 FullHttpResponse  │
└─────────────────────────────────────────────┘

=== Outbound 方向（业务对象 → 字节流）===

    │
    ▼
┌─────────────────────────────────────────────┐
│ HttpServerCodec (内部 HttpResponseEncoder)   │
│                                             │
│ FullHttpResponse → 字节流                    │
└─────────────────────────────────────────────┘
    │
    ▼
[TCP 字节流发出]

服务端响应：
HTTP/1.1 200 OK
Content-Type: text/plain
Content-Length: 13

Hello, World!
```

### 1.2 HttpServerCodec 的组合模式

`HttpServerCodec` 是 `CombinedChannelDuplexHandler` 的典型应用，它将请求解码器和响应编码器组合成一个双向 Handler：

```java
public final class HttpServerCodec
        extends CombinedChannelDuplexHandler<HttpRequestDecoder, HttpResponseEncoder>
        implements HttpServerUpgradeHandler.SourceCodec {

    public HttpServerCodec(HttpDecoderConfig config) {
        init(new HttpServerRequestDecoder(config), new HttpServerResponseEncoder());
    }
}
```

`CombinedChannelDuplexHandler` 的核心思想是：通过一个 `DelegatingChannelHandlerContext` 代理上下文，使得 Inbound 事件只传播给内部的 decoder，Outbound 事件只传播给内部的 encoder。这比 `ByteToMessageCodec` 更灵活，因为两个被组合的 Handler 可以是完全不相关的类型。

### 1.3 HttpServerCodec 的 bit-packed 请求方法队列

HttpServerCodec 内部有一个精巧的设计——它需要追踪每个请求的 HTTP 方法，以便在编码响应时正确处理 HEAD 和 CONNECT 方法的特殊行为。这个追踪机制使用了一个 bit-packed 的队列：

```java
private static final byte METHOD_FLAG_HEAD = 1;     // 01
private static final byte METHOD_FLAG_CONNECT = 2;   // 10
private static final byte METHOD_FLAG_OTHER = 3;      // 11
private static final int METHOD_FLAG_BITS = 2;
private static final int INLINE_QUEUE_CAPACITY = Long.SIZE / METHOD_FLAG_BITS; // 32
```

每个请求方法只需要 2 位（bit），一个 `long` 字段可以内联存储 32 个请求的方法标记。这是因为 HTTP 方法在此场景下只需要区分三种情况：HEAD、CONNECT 和其他。

当请求到达时，decoder 侧将方法入队：

```java
private void enqueueMethod(HttpMethod method) {
    final byte flag;
    if (HttpMethod.HEAD.equals(method)) {
        flag = METHOD_FLAG_HEAD;
    } else if (HttpMethod.CONNECT.equals(method)) {
        flag = METHOD_FLAG_CONNECT;
    } else {
        flag = METHOD_FLAG_OTHER;
    }

    if (methodQueueSize < INLINE_QUEUE_CAPACITY) {
        // 内联存储：用位运算将 flag 放入 long 的对应位置
        methodQueue |= (long) flag << (methodQueueSize << 1);
        methodQueueSize++;
    } else {
        // 溢出：创建 ArrayDeque 存储
        overflowQueue = new ArrayDeque<>(4);
        overflowQueue.add(flag);
        methodOverflowQueue = overflowQueue;
    }
}
```

当响应编码时，encoder 侧从队列头部取出方法标记：

```java
private byte pollMethod() {
    if (methodQueueSize != 0) {
        byte flag = (byte) (methodQueue & 0x3L);
        methodQueue >>>= METHOD_FLAG_BITS;
        methodQueueSize--;
        return flag;
    }
    // 溢出队列处理...
}
```

这个设计的关键优势在于：对于常见的 ≤32 个并发请求场景（HTTP pipelining），整个队列只是一个 `long` 字段的位操作，零分配、零 GC。只有当 pipelining 的请求超过 32 个时，才会回退到 `ArrayDeque`。

取出的方法标记在 `isContentAlwaysEmpty` 中决定响应编码行为：

```java
protected boolean isContentAlwaysEmpty(HttpResponse msg) {
    methodFlag = pollMethod();
    // HEAD 请求的响应没有 body
    return methodFlag == METHOD_FLAG_HEAD || super.isContentAlwaysEmpty(msg);
}
```

### 1.4 各 Handler 的数据类型契约

整个 HTTP Pipeline 中的数据类型流转遵循一条严格的契约链：

| Pipeline 位置 | Inbound 输入 | Inbound 输出 | Outbound 输入 | Outbound 输出 |
|---|---|---|---|---|
| HttpServerCodec (Decoder) | ByteBuf | HttpRequest, HttpContent, LastHttpContent | — | — |
| HttpObjectAggregator | HttpObject | FullHttpRequest / FullHttpResponse | — | — |
| HttpContentCompressor | HttpObject | HttpObject (压缩后) | — | — |
| HttpServerKeepAliveHandler | HttpObject | HttpObject (透传) | HttpObject | HttpObject (可能修改 Connection 头) |
| BusinessHandler | FullHttpRequest | — | FullHttpResponse | — |
| HttpServerCodec (Encoder) | — | — | HttpResponse, HttpContent | ByteBuf |

---

## 二、HttpObjectDecoder 的解码过程（状态机）

### 2.1 状态机总览

`HttpObjectDecoder` 是整个 HTTP 解码器的核心，继承自 `ByteToMessageDecoder`。它使用一个显式的状态机来驱动解码过程。以下是完整的状态定义：

```java
private enum State {
    SKIP_INITIAL_LINE_CHARS,    // 跳过初始行前的控制字符
    SKIP_CONTROL_CHARS,          // 跳过消息间的控制字符
    READ_INITIAL,                // 读取请求行/状态行
    READ_HEADER,                 // 读取头部
    READ_VARIABLE_LENGTH_CONTENT,// 读取变长内容（无 Content-Length，靠连接关闭）
    READ_FIXED_LENGTH_CONTENT,   // 读取定长内容
    READ_CHUNK_SIZE,             // 读取 chunk 大小
    READ_CHUNKED_CONTENT,        // 读取 chunk 数据
    READ_CHUNK_DELIMITER,        // 读取 chunk 后的 CRLF
    READ_CHUNK_FOOTER,           // 读取 trailer 头部
    BAD_MESSAGE,                 // 损坏消息，丢弃剩余数据
    UPGRADED                     // 协议升级，透传剩余字节
}
```

状态转换流程可以用下图表示：

```
                        ┌─────────────────────────┐
                        │ SKIP_INITIAL_LINE_CHARS  │
                        └────────────┬─────────────┘
                                     │ 跳过控制字符
                                     ▼
                        ┌─────────────────────────┐
                        │      READ_INITIAL        │
                        │  解析 "GET /path HTTP/1.1"│
                        └────────────┬─────────────┘
                                     │ 解析出 method, uri, version
                                     ▼
                        ┌─────────────────────────┐
                        │      READ_HEADER         │
                        │  逐行解析 Header 键值对    │
                        └────────────┬─────────────┘
                                     │ 空行结束 Header
                          ┌──────────┼──────────┐
                          │          │          │
                    chunked?   Content-Length?  都没有?
                          │          │          │
                          ▼          ▼          ▼
                   ┌──────────┐ ┌──────────┐ ┌─────────────┐
                   │READ_CHUNK│ │READ_FIXED│ │READ_VARIABLE│
                   │  _SIZE   │ │_LENGTH   │ │_LENGTH      │
                   └────┬─────┘ └────┬─────┘ └─────────────┘
                        │            │              │
                        ▼            ▼              ▼
                   ┌──────────┐ ┌──────────┐  读取到连接关闭
                   │READ_CHUNK│ │产出HttpContent│   │
                   │_CONTENT  │ │ +LastHttp    │   ▼
                   └────┬─────┘ └──────────┘  resetNow()
                        │
                        ▼
                   ┌──────────┐
                   │READ_CHUNK│
                   │_DELIMITER│
                   └────┬─────┘
                        │ chunk size == 0?
                   ┌────┴────┐
                   │No       │Yes
                   │         ▼
                   │    ┌──────────┐
                   │    │READ_CHUNK│
                   │    │ _FOOTER  │
                   │    └────┬─────┘
                   │         │ resetNow()
                   └─────────┘
                   回到 READ_CHUNK_SIZE
```

### 2.2 贯穿案例：完整 HTTP 请求的解码过程

我们用以下 POST 请求作为贯穿案例，展示状态机的每一步解码：

```
POST /api/upload HTTP/1.1\r\n
Host: example.com\r\n
Content-Type: text/plain\r\n
Content-Length: 26\r\n
\r\n
Hello, Netty HTTP Server!
```

#### 第一步：SKIP_INITIAL_LINE_CHARS → READ_INITIAL

初始状态为 `SKIP_INITIAL_LINE_CHARS`。`LineParser` 首先跳过前导控制字符（如前一个请求遗留的 CRLF），然后进入 `READ_INITIAL` 状态，用 `indexOf` 查找 LF 来定位一行：

```java
// LineParser.parse() 方法
public ByteBuf parse(ByteBuf buffer, Runnable strictCRLFCheck) {
    final int indexOfLf = buffer.indexOf(readerIndex, toIndexExclusive, HttpConstants.LF);
    if (indexOfLf == -1) {
        return null;  // 数据不完整，等待更多数据
    }
    // 检查是否有 CR（CRLF vs LF）
    final int endOfSeqIncluded;
    if (indexOfLf > readerIndex && buffer.getByte(indexOfLf - 1) == HttpConstants.CR) {
        endOfSeqIncluded = indexOfLf - 1;  // 去掉 CR
    } else {
        if (strictCRLFCheck != null) {
            strictCRLFCheck.run();
        }
        endOfSeqIncluded = indexOfLf;
    }
    // 将一行数据写入 scratch buffer
    seq.clear();
    seq.writeBytes(buffer, readerIndex, newSize);
    buffer.readerIndex(indexOfLf + 1);
    return seq;
}
```

对于我们的案例，第一次 `parse` 返回 `"POST /api/upload HTTP/1.1"`。`splitInitialLine` 将其按空格拆分为三部分：

```java
final String[] initialLine = splitInitialLine(line);
// initialLine = ["POST", "/api/upload", "HTTP/1.1"]

message = createMessage(initialLine);
// 创建 DefaultHttpRequest(method=POST, uri="/api/upload", version=HTTP_1_1)
currentState = State.READ_HEADER;
```

**HttpRequestDecoder 的快速路径优化**：对于常见的 HTTP 方法，`splitFirstWordInitialLine` 使用 int/long 比较替代字符串比较：

```java
private static final int GET_AS_INT = 'G' | 'E' << 8 | 'T' << 16;
private static final int POST_AS_INT = 'P' | 'O' << 8 | 'S' << 16 | 'T' << 24;
private static final long HTTP_1_1_AS_LONG = 'H' | 'T' << 8 | 'T' << 16 | 'P' << 24 |
        (long) '/' << 32 | (long) '1' << 40 | (long) '.' << 48 | (long) '1' << 56;

// 检查是否为 GET：一次 int 比较替代 String.equals
private static boolean isGetMethod(final byte[] sb, int start) {
    final int maybeGet = sb[start] | sb[start + 1] << 8 | sb[start + 2] << 16;
    return maybeGet == GET_AS_INT;
}
```

这意味着 `"GET "` 的识别只需要一次 int 比较（3 字节打包到一个 int），`"HTTP/1.1"` 的识别只需要一次 long 比较（8 字节打包到一个 long）。同理，`splitHeaderName` 对 `"Host"`、`"Content-Type"`、`"Content-Length"`、`"Connection"`、`"Accept"` 等常见头名也做了同样的 int/long 快速比较。

#### 第二步：READ_HEADER

进入 `READ_HEADER` 状态后，`readHeaders` 方法循环使用 `HeaderParser` 逐行读取头部，直到遇到空行（`lineLength == 0`）：

```java
private State readHeaders(ByteBuf buffer) {
    ByteBuf line = headerParser.parse(buffer, defaultStrictCRLFCheck);
    if (line == null) {
        return null;  // 数据不完整
    }
    int lineLength = line.readableBytes();
    while (lineLength > 0) {
        final byte[] lineContent = line.array();
        final int startLine = line.arrayOffset() + line.readerIndex();
        final byte firstChar = lineContent[startLine];
        
        if (name != null && (firstChar == ' ' || firstChar == '\t')) {
            // 拆行续接（obsolete line folding）
            String trimmedLine = langAsciiString(lineContent, startLine, lineLength).trim();
            value = value + ' ' + trimmedLine;
        } else {
            if (name != null) {
                headers.add(name, value);
            }
            splitHeader(lineContent, startLine, lineLength);
        }
        
        line = headerParser.parse(buffer, defaultStrictCRLFCheck);
        if (line == null) return null;
        lineLength = line.readableBytes();
    }
    // 空行 → header 解析完成
    // ...
}
```

对于我们的案例，Header 解析器依次处理：

```
第1行: "Host: example.com"          → name=Host, value=example.com
第2行: "Content-Type: text/plain"   → name=Content-Type, value=text/plain
第3行: "Content-Length: 26"         → name=Content-Length, value=26
第4行: "" (空行)                    → header 解析结束
```

`splitHeader` 方法手动逐字节遍历，而非使用正则表达式，这是出于性能考虑：

```java
private void splitHeader(byte[] line, int start, int length) {
    final int end = start + length;
    int nameEnd;
    final int nameStart = start;
    for (nameEnd = nameStart; nameEnd < end; nameEnd++) {
        byte ch = line[nameEnd];
        if (ch == ':' || (!isDecodingRequest && isOWS(ch))) {
            break;  // 找到冒号
        }
    }
    // 跳过冒号后的空格
    int colonEnd;
    for (colonEnd = nameEnd; colonEnd < end; colonEnd++) {
        if (line[colonEnd] == ':') {
            colonEnd++;
            break;
        }
    }
    name = splitHeaderName(line, nameStart, nameEnd - nameStart);
    final int valueStart = findNonWhitespace(line, colonEnd, end);
    value = langAsciiString(line, valueStart, valueEnd - valueStart);
}
```

#### 第三步：Content-Length vs chunked 的判断

Header 解析完成后，`readHeaders` 方法需要决定消息体的读取方式。判断逻辑的顺序非常重要——**Transfer-Encoding: chunked 优先于 Content-Length**：

```java
// 1. 检查 Content-Length
List<String> contentLengthFields = headers.getAll(HttpHeaderNames.CONTENT_LENGTH);
if (!contentLengthFields.isEmpty()) {
    contentLength = HttpUtil.normalizeAndGetContentLength(contentLengthFields, ...);
}

// 2. 检查是否内容永远为空（1xx, 204, 304 等）
if (isContentAlwaysEmpty(message)) {
    return State.SKIP_CONTROL_CHARS;
}

// 3. 检查 Transfer-Encoding: chunked（优先级高于 Content-Length）
if (HttpUtil.isTransferEncodingChunked(message)) {
    this.chunked = true;
    // 如果同时有 Content-Length，按 RFC 9112 拒绝或移除
    if (!contentLengthFields.isEmpty()) {
        handleTransferEncodingChunkedWithContentLength(message);
    }
    return State.READ_CHUNK_SIZE;
}

// 4. 有 Content-Length → 定长内容
if (contentLength >= 0) {
    return State.READ_FIXED_LENGTH_CONTENT;
}

// 5. 都没有 → 变长内容（靠连接关闭判断结束）
return State.READ_VARIABLE_LENGTH_CONTENT;
```

对于我们的案例（`Content-Length: 26`，无 `Transfer-Encoding`），状态进入 `READ_FIXED_LENGTH_CONTENT`，`chunkSize` 设为 26：

```java
case READ_FIXED_LENGTH_CONTENT: {
    int readLimit = buffer.readableBytes();
    if (readLimit == 0) return;
    
    int toRead = Math.min(readLimit, maxChunkSize);  // maxChunkSize 默认 8192
    if (toRead > chunkSize) {
        toRead = (int) chunkSize;
    }
    ByteBuf content = buffer.readRetainedSlice(toRead);
    chunkSize -= toRead;
    
    if (chunkSize == 0) {
        // 读完了所有内容
        out.add(new DefaultLastHttpContent(content, trailersFactory));
        resetNow();
    } else {
        out.add(new DefaultHttpContent(content));
    }
    return;
}
```

如果 buffer 中恰好有完整的 26 字节 `"Hello, Netty HTTP Server!"`，则直接产出 `DefaultLastHttpContent` 并 reset。如果只有部分数据（比如 10 字节），则先产出 `DefaultHttpContent`（10 字节），下次 `channelRead` 再继续读取剩余 16 字节。

#### 第四步：产出序列总结

对于完整的 `POST /api/upload` 请求，decoder 的产出序列是：

```
1. DefaultHttpRequest  (POST /api/upload HTTP/1.1 + headers)
2. DefaultLastHttpContent (26 bytes body + empty trailing headers)
```

如果是 chunked 请求（`Transfer-Encoding: chunked`），产出序列则不同。考虑如下 chunked 请求：

```
POST /api/upload HTTP/1.1\r\n
Host: example.com\r\n
Transfer-Encoding: chunked\r\n
\r\n
1a\r\n
abcdefghijklmnopqrstuvwxyz\r\n
10\r\n
1234567890abcdef\r\n
0\r\n
\r\n
```

产出序列为：

```
1. DefaultHttpRequest  (POST /api/upload HTTP/1.1 + headers)
2. DefaultHttpContent  (26 bytes: "abcdefghijklmnopqrstuvwxyz")
3. DefaultLastHttpContent (16 bytes: "1234567890abcdef" + empty trailing headers)
```

chunked 解码经过 `READ_CHUNK_SIZE → READ_CHUNKED_CONTENT → READ_CHUNK_DELIMITER` 循环，当 chunk size 为 0 时进入 `READ_CHUNK_FOOTER` 读取 trailer headers，最后产出 `LastHttpContent`。

### 2.3 LineParser 和 HeaderParser 的 ByteProcessor 机制

`LineParser` 和 `HeaderParser` 都继承自 `HeaderParser` 内部类。它们的核心查找逻辑是 `buffer.indexOf(readerIndex, toIndexExclusive, LF)`，底层使用 ByteProcessor 逐字节扫描。但 LineParser 在 `SKIP_INITIAL_LINE_CHARS` 状态下会使用特殊的 ByteProcessor 来跳过控制字符：

```java
private static final ByteProcessor SKIP_CONTROL_CHARS_BYTES = new ByteProcessor() {
    @Override
    public boolean process(byte value) {
        return ISO_CONTROL_OR_WHITESPACE[128 + value];
    }
};
```

这里使用了一个预计算的 `boolean[256]` 查找表，避免了对每个字节调用 `Character.isISOControl()` 的开销。`SP_LENIENT_BYTES` 和 `LATIN_WHITESPACE` 也是同样的预计算表优化。

### 2.4 resetNow 与状态重置

每当一条完整的 HTTP 消息解码完成后（通过 `LastHttpContent` 或空内容判定），`resetNow()` 被调用来重置所有状态，准备解码下一条消息：

```java
private void resetNow() {
    message = null;
    name = null;
    value = null;
    clearContentLength();  // contentLength = Long.MIN_VALUE
    chunked = false;
    lineParser.reset();
    headerParser.reset();
    trailer = null;
    
    if (isSwitchingToNonHttp1Protocol) {
        isSwitchingToNonHttp1Protocol = false;
        currentState = State.UPGRADED;  // 协议已升级，不再解码
        return;
    }
    
    resetRequested.lazySet(false);
    currentState = State.SKIP_INITIAL_LINE_CHARS;  // 回到初始状态
}
```

注意 `resetRequested` 使用 `AtomicBoolean` 和 `lazySet`，这是因为 `reset()` 方法可能从非 EventLoop 线程调用（例如用户在另一个线程中拒绝了大请求体），而 `resetNow()` 在 EventLoop 线程中执行。`lazySet` 提供了可见性保证而不付出 `compareAndSet` 的开销。

---

## 三、HttpObjectAggregator 的聚合逻辑

### 3.1 为什么需要将消息拆成多个对象？

HttpObjectDecoder 的设计哲学是**流式解码**：它不等待整个 HTTP 消息体到达才产出结果，而是将消息拆分成一个 `HttpMessage`（包含请求行/状态行和头部）加上零到多个 `HttpContent`（包含消息体分片），最后一个分片是 `LastHttpContent`。

这种拆分设计有三个关键原因：

第一，**内存效率**。考虑一个上传 1GB 文件的 HTTP 请求。如果 decoder 等待所有数据到达再产出完整消息，服务器需要分配 1GB 的连续内存。拆分后，每次只持有 `maxChunkSize`（默认 8KB）的数据，业务层可以流式处理——边接收边写入磁盘，峰值内存恒定。

第二，**背压控制**。流式分片使得业务层可以在处理完一个 chunk 后才继续读取下一个，天然实现了背压。如果聚合为一个完整消息，decoder 会一次性读取所有数据填满内存。

第三，**协议兼容**。HTTP chunked 编码本身就是流式的——发送方可能不知道总长度，chunk 一个一个到达。流式解码直接映射这种协议语义，不需要额外缓冲。

但流式 API 对业务开发者来说使用不便——需要处理 `HttpRequest`、`HttpContent`、`LastHttpContent` 三种对象，还要自己拼接 body。`HttpObjectAggregator` 就是解决这个便利性问题的：它将分片重新聚合为 `FullHttpRequest` / `FullHttpResponse`，以空间换便利。

### 3.2 MessageAggregator 的泛型框架

`HttpObjectAggregator` 继承自 `MessageAggregator<HttpObject, HttpMessage, HttpContent, FullHttpMessage>`。四个泛型参数的含义是：

| 参数 | 含义 | HTTP 中的具体类型 |
|---|---|---|
| I | 覆盖起始消息和内容消息的统一类型 | HttpObject |
| S | 起始消息类型 | HttpMessage (HttpRequest/HttpResponse) |
| C | 内容消息类型 | HttpContent |
| O | 聚合后的输出类型 | FullHttpMessage (FullHttpRequest/FullHttpResponse) |

`MessageAggregator` 的核心 `decode` 方法实现了聚合状态机：

```java
protected void decode(final ChannelHandlerContext ctx, I msg, List<Object> out) throws Exception {
    if (isStartMessage(msg)) {
        // 收到起始消息（HttpRequest/HttpResponse）
        aggregating = true;
        
        // 1. 检查 100-continue 期望
        Object continueResponse = newContinueResponse(m, maxContentLength, ctx.pipeline());
        if (continueResponse != null) {
            // 自动发送 100 Continue 或 417 Expectation Failed
            ctx.writeAndFlush(continueResponse);
            // ...
        } else if (isContentLengthInvalid(m, maxContentLength)) {
            // 2. Content-Length 超限 → 413
            invokeHandleOversizedMessage(ctx, m);
            return;
        }
        
        // 3. 创建聚合消息，初始内容用 CompositeByteBuf
        CompositeByteBuf content = ctx.alloc().compositeBuffer(maxCumulationBufferComponents);
        currentMessage = beginAggregation(m, content);
        
    } else if (isContentMessage(msg)) {
        // 收到内容分片（HttpContent）
        CompositeByteBuf content = (CompositeByteBuf) currentMessage.content();
        
        // 4. 检查是否超限
        if (content.readableBytes() > maxContentLength - m.content().readableBytes()) {
            invokeHandleOversizedMessage(ctx, s);
            return;
        }
        
        // 5. 追加内容到 CompositeByteBuf（零拷贝）
        appendPartialContent(content, m.content());
        
        // 6. 如果是最后一个分片，完成聚合
        if (isLastContentMessage(m)) {
            finishAggregation0(currentMessage);
            out.add(currentMessage);
            currentMessage = null;
        }
    }
}
```

`appendPartialContent` 使用 `CompositeByteBuf.addComponent` 将每个 chunk 的 ByteBuf 作为独立组件添加，避免了数据拷贝：

```java
private static void appendPartialContent(CompositeByteBuf content, ByteBuf partialContent) {
    if (partialContent.isReadable()) {
        content.addComponent(true, partialContent.retain());
    }
}
```

### 3.3 maxContentLength 超限处理

当聚合的内容超过 `maxContentLength` 时，`handleOversizedMessage` 被调用。HttpObjectAggregator 的实现根据请求类型和上下文返回不同的错误响应：

```java
protected void handleOversizedMessage(ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
    if (oversized instanceof HttpRequest) {
        // 请求体过大
        if (oversized instanceof FullHttpMessage || !ctx.channel().config().isAutoRead() ||
            !HttpUtil.is100ContinueExpected(oversized) && !HttpUtil.isKeepAlive(oversized)) {
            // 发送 413 + Connection: close，然后关闭连接
            ChannelFuture future = ctx.writeAndFlush(TOO_LARGE_CLOSE.retainedDuplicate());
            future.addListener(f -> ctx.close());
        } else {
            // 发送 413 但保持连接（可以继续处理下一个请求）
            ctx.writeAndFlush(TOO_LARGE.retainedDuplicate());
        }
    } else if (oversized instanceof HttpResponse) {
        ctx.close();
        throw new TooLongHttpContentException("Response entity too large: " + oversized);
    }
}
```

这里有两个预创建的静态响应对象，避免了每次超限都创建新对象：

```java
private static final FullHttpResponse TOO_LARGE_CLOSE = new DefaultFullHttpResponse(
    HttpVersion.HTTP_1_1, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, Unpooled.EMPTY_BUFFER);
private static final FullHttpResponse TOO_LARGE = new DefaultFullHttpResponse(
    HttpVersion.HTTP_1_1, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, Unpooled.EMPTY_BUFFER);

static {
    TOO_LARGE_CLOSE.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
}
```

### 3.4 100-continue 处理

HTTP/1.1 的 `Expect: 100-continue` 机制允许客户端在发送大请求体之前先询问服务器是否愿意接收。HttpObjectAggregator 自动处理这个流程：

```java
private Object continueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
    if (HttpUtil.isUnsupportedExpectation(start)) {
        // 不支持的 Expect 值 → 417 Expectation Failed
        pipeline.fireUserEventTriggered(HttpExpectationFailedEvent.INSTANCE);
        return EXPECTATION_FAILED.retainedDuplicate();
    } else if (HttpUtil.is100ContinueExpected(start)) {
        if (!isContentLengthInvalid(start, maxContentLength)) {
            // Content-Length 在限制内 → 100 Continue
            return CONTINUE.retainedDuplicate();
        }
        // Content-Length 超限 → 413
        pipeline.fireUserEventTriggered(HttpExpectationFailedEvent.INSTANCE);
        return TOO_LARGE.retainedDuplicate();
    }
    return null;
}
```

完整的 100-continue 交互流程：

```
客户端                                     服务端
  │                                          │
  │  POST /upload HTTP/1.1                   │
  │  Expect: 100-continue                    │
  │  Content-Length: 10485760                │
  │ ──────────────────────────────────────▶  │
  │                                          │
  │                          HttpObjectAggregator 检查:
  │                          - is100ContinueExpected? Yes
  │                          - isContentLengthInvalid? 
  │                            10MB > maxContentLength(1MB)? Yes
  │                          → 自动发送 413
  │                                          │
  │  HTTP/1.1 413 Request Entity Too Large   │
  │  Content-Length: 0                        │
  │  Connection: close                        │
  │ ◀──────────────────────────────────────  │
  │                                          │
  │  （客户端不发送 body，连接关闭）            │
```

如果 Content-Length 在限制内：

```
客户端                                     服务端
  │                                          │
  │  POST /upload HTTP/1.1                   │
  │  Expect: 100-continue                    │
  │  Content-Length: 512                      │
  │ ──────────────────────────────────────▶  │
  │                                          │
  │                          HttpObjectAggregator 检查:
  │                          - is100ContinueExpected? Yes
  │                          - isContentLengthInvalid? No
  │                          → 自动发送 100 Continue
  │                                          │
  │  HTTP/1.1 100 Continue                    │
  │ ◀──────────────────────────────────────  │
  │                                          │
  │  （客户端开始发送 body）                    │
  │  [512 bytes of body data]                │
  │ ──────────────────────────────────────▶  │
  │                                          │
  │                          聚合完成 → FullHttpRequest
  │                          传递给业务 Handler
```

关键细节：发送 continue 响应后，`newContinueResponse` 会从请求头中移除 `Expect` 头，防止它传递到业务层：

```java
protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
    Object response = continueResponse(start, maxContentLength, pipeline);
    if (response != null) {
        start.headers().remove(EXPECT);  // 移除 Expect 头
    }
    return response;
}
```

### 3.5 AggregatedFullHttpRequest/Response 装饰器

聚合后的完整消息使用装饰器模式包装原始的 HttpMessage 和聚合的 ByteBuf：

```java
private abstract static class AggregatedFullHttpMessage implements FullHttpMessage {
    protected final HttpMessage message;      // 原始的 HttpRequest/HttpResponse
    private final ByteBuf content;             // 聚合后的完整 body
    private HttpHeaders trailingHeaders;       // trailer 头部

    // 所有 HttpMessage 的方法委托给 message
    public HttpHeaders headers() { return message.headers(); }
    public HttpVersion protocolVersion() { return message.protocolVersion(); }
    public DecoderResult decoderResult() { return message.decoderResult(); }

    // ByteBufHolder 的方法委托给 content
    public ByteBuf content() { return content; }
    public int refCnt() { return content.refCnt(); }
    public FullHttpMessage retain() { content.retain(); return this; }
    public boolean release() { return content.release(); }
}
```

`AggregatedFullHttpRequest` 和 `AggregatedFullHttpResponse` 分别继承它并实现 `FullHttpRequest` / `FullHttpResponse` 接口。这种装饰器模式使得聚合后的对象对外暴露统一的 `FullHttpMessage` 接口，业务层无需关心内部是原始消息还是聚合消息。

`finishAggregation` 方法在聚合完成时设置 `Content-Length` 头（如果尚未设置）：

```java
protected void finishAggregation(FullHttpMessage aggregated) throws Exception {
    if (!HttpUtil.isContentLengthSet(aggregated)) {
        aggregated.headers().setInt(CONTENT_LENGTH, aggregated.content().readableBytes());
    }
}
```

---

## 四、HTTP Keep-Alive 处理

### 4.1 Keep-Alive 的核心挑战

HTTP/1.1 默认使用持久连接（Keep-Alive），多个请求-响应可以在同一个 TCP 连接上复用。这带来一个核心挑战：**服务器如何知道何时应该关闭连接？**

关闭连接的时机取决于多个因素：客户端是否请求了 Keep-Alive、响应是否设置了正确的消息长度头、是否出现了错误等。`HttpServerKeepAliveHandler` 就是为了自动管理这些逻辑而设计的。

### 4.2 pendingResponses 计数器

`HttpServerKeepAliveHandler` 维护一个 `pendingResponses` 计数器，跟踪当前有多少个请求尚未响应。这支持 HTTP pipelining——客户端可以在收到前一个响应之前就发送下一个请求：

```java
public class HttpServerKeepAliveHandler extends ChannelDuplexHandler {
    private boolean persistentConnection = true;
    private int pendingResponses;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            final HttpRequest request = (HttpRequest) msg;
            if (persistentConnection) {
                pendingResponses += 1;
                persistentConnection = isKeepAlive(request);
            }
        }
        super.channelRead(ctx, msg);
    }
}
```

每收到一个 `HttpRequest`，如果当前连接是持久的，`pendingResponses` 加 1。同时检查这个请求是否请求了 Keep-Alive（通过 `Connection: keep-alive` 头或 HTTP/1.1 默认行为）。

在 Outbound 方向，每发送一个非 1xx 的 `HttpResponse`，`pendingResponses` 减 1：

```java
@Override
public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpResponse) {
        final HttpResponse response = (HttpResponse) msg;
        trackResponse(response);
        if (!isKeepAlive(response) || !isSelfDefinedMessageLength(response)) {
            pendingResponses = 0;
            persistentConnection = false;
        }
        if (!shouldKeepAlive()) {
            setKeepAlive(response, false);
        }
    }
    if (msg instanceof LastHttpContent && !shouldKeepAlive()) {
        promise = promise.unvoid().addListener(ChannelFutureListener.CLOSE);
    }
    super.write(ctx, msg, promise);
}

private void trackResponse(HttpResponse response) {
    if (!isInformational(response)) {
        pendingResponses -= 1;  // 1xx 响应不算完整响应
    }
}
```

### 4.3 isSelfDefinedMessageLength 检查

Keep-Alive 的一个关键前提是：**客户端必须能不依赖连接关闭来判断消息结束**。也就是说，响应必须包含 `Content-Length` 或 `Transfer-Encoding: chunked`，否则客户端无法区分当前响应的结束和下一个响应的开始。

`isSelfDefinedMessageLength` 方法检查这个条件：

```java
private static boolean isSelfDefinedMessageLength(HttpResponse response) {
    return isContentLengthSet(response)            // 有 Content-Length
        || isTransferEncodingChunked(response)     // 有 Transfer-Encoding: chunked
        || isMultipart(response)                   // multipart 内容
        || isInformational(response)               // 1xx 信息响应（无 body）
        || response.status().code() == HttpResponseStatus.NO_CONTENT.code();  // 204 无内容
}
```

如果响应没有自定义消息长度，则必须关闭连接，否则客户端会无限等待更多数据。

### 4.4 完整的 Keep-Alive 决策流程

以一个典型的 Keep-Alive 交互为例：

```
时刻 T1: 客户端发送第1个请求
    channelRead: HttpRequest (Connection: keep-alive)
    pendingResponses: 0 → 1
    persistentConnection: true → true (isKeepAlive=true)

时刻 T2: 业务处理完成，发送第1个响应
    write: HttpResponse (200 OK, Content-Length: 13)
    trackResponse: pendingResponses: 1 → 0
    isKeepAlive(response)? Yes
    isSelfDefinedMessageLength? Yes (有 Content-Length)
    shouldKeepAlive()? pendingResponses(0) != 0? No; persistentConnection? Yes → true
    → 不关闭连接，继续保活

时刻 T3: 客户端发送第2个请求 (Connection: close)
    channelRead: HttpRequest (Connection: close)
    pendingResponses: 0 → 1
    persistentConnection: true → false (isKeepAlive=false)

时刻 T4: 业务处理完成，发送第2个响应
    write: HttpResponse (200 OK, Content-Length: 10)
    trackResponse: pendingResponses: 1 → 0
    shouldKeepAlive()? pendingResponses(0) != 0? No; persistentConnection? No → false
    → setKeepAlive(response, false)  ← 修改响应头
    → LastHttpContent + CLOSE listener
    → 响应发送完成后关闭连接
```

关键设计：`HttpServerKeepAliveHandler` 是一个 `ChannelDuplexHandler`，同时拦截 Inbound（读取请求）和 Outbound（写入响应）。它不需要修改消息内容（除了可能修改 `Connection` 头），只是添加 `CLOSE` listener 来控制连接生命周期。

---

## 五、WebSocket 升级过程

### 5.1 WebSocket 握手请求

WebSocket 协议的建立需要一个 HTTP 升级握手。客户端发送一个特殊的 HTTP 请求，服务器返回 101 Switching Protocols 响应，之后连接就从 HTTP 协议切换到 WebSocket 协议：

```
客户端请求：
GET /chat HTTP/1.1
Host: server.example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Origin: http://example.com
Sec-WebSocket-Protocol: chat, superchat
Sec-WebSocket-Version: 13

服务端响应：
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
Sec-WebSocket-Protocol: chat
```

### 5.2 WebSocketServerHandshakerFactory 的版本选择

`WebSocketServerHandshakerFactory` 通过 `Sec-WebSocket-Version` 头来选择对应版本的 Handshaker：

```java
private static WebSocketServerHandshaker resolveHandshaker0(HttpRequest req,
        String webSocketURL, String subprotocols, WebSocketDecoderConfig decoderConfig) {
    CharSequence version = req.headers().get(HttpHeaderNames.SEC_WEBSOCKET_VERSION);
    if (version != null) {
        if (version.equals(WebSocketVersion.V13.toHttpHeaderValue())) {
            // RFC 6455（最常用）
            return new WebSocketServerHandshaker13(webSocketURL, subprotocols, decoderConfig);
        } else if (version.equals(WebSocketVersion.V08.toHttpHeaderValue())) {
            return new WebSocketServerHandshaker08(webSocketURL, subprotocols, decoderConfig);
        } else if (version.equals(WebSocketVersion.V07.toHttpHeaderValue())) {
            return new WebSocketServerHandshaker07(webSocketURL, subprotocols, decoderConfig);
        } else {
            return null;  // 不支持的版本
        }
    } else {
        // 没有 Version 头 → 假设是 V00（Hixie 76 草案）
        return new WebSocketServerHandshaker00(webSocketURL, subprotocols, decoderConfig);
    }
}
```

### 5.3 V13 握手的 Sec-WebSocket-Accept 计算

`WebSocketServerHandshaker13` 的 `newHandshakeResponse` 方法实现了 RFC 6455 的握手响应：

```java
protected FullHttpResponse newHandshakeResponse(FullHttpRequest req, HttpHeaders headers) {
    // 1. 验证请求必须是 GET 方法
    if (!GET.equals(method)) {
        throw new WebSocketServerHandshakeException("Invalid WebSocket handshake method: " + method);
    }
    
    // 2. 验证 Connection 头包含 Upgrade
    if (!reqHeaders.containsValue(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE, true)) {
        throw new WebSocketServerHandshakeException("Connection header must include 'Upgrade'");
    }
    
    // 3. 验证 Upgrade 头是 websocket
    if (!reqHeaders.contains(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)) {
        throw new WebSocketServerHandshakeException("Upgrade header must be 'websocket'");
    }
    
    // 4. 获取 Sec-WebSocket-Key
    String key = reqHeaders.get(HttpHeaderNames.SEC_WEBSOCKET_KEY);
    
    // 5. 计算 Sec-WebSocket-Accept
    //    SHA-1(Sec-WebSocket-Key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11") → Base64
    MessageDigest digestSha1 = WebSocketUtil.sha1();
    digestSha1.update(key.getBytes(StandardCharsets.US_ASCII));
    digestSha1.update(GUID_BYTES);  // GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    String accept = WebSocketUtil.base64(digestSha1.digest());
    
    // 6. 构建响应
    FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.SWITCHING_PROTOCOLS, ...);
    res.headers()
       .set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET)
       .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE)
       .set(HttpHeaderNames.SEC_WEBSOCKET_ACCEPT, accept);
    
    // 7. 子协议协商
    String subprotocols = reqHeaders.get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL);
    if (subprotocols != null) {
        String selectedSubprotocol = selectSubprotocol(subprotocols);
        if (selectedSubprotocol != null) {
            res.headers().set(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, selectedSubprotocol);
        }
    }
    return res;
}
```

其中 GUID `"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"` 是 RFC 6455 规定的魔术字符串，它的存在确保了客户端和服务端都明确知道对方理解 WebSocket 协议——而不是一个不理解 Upgrade 的 HTTP 中间代理。

### 5.4 Pipeline 动态修改

握手成功后，`handshake` 方法需要动态修改 Pipeline，将 HTTP 编解码器替换为 WebSocket 帧编解码器。这是 Netty Pipeline 热插拔能力的典型应用：

```java
public final ChannelFuture handshake(Channel channel, FullHttpRequest req,
        HttpHeaders responseHeaders, final ChannelPromise promise) {
    
    // 1. 构建握手响应
    FullHttpResponse response = newHandshakeResponse(req, responseHeaders);
    ChannelPipeline p = channel.pipeline();
    
    // 2. 移除 HttpObjectAggregator（如果存在）
    if (p.get(HttpObjectAggregator.class) != null) {
        p.remove(HttpObjectAggregator.class);
    }
    
    // 3. 移除 HttpContentCompressor（如果存在）
    if (p.get(HttpContentCompressor.class) != null) {
        p.remove(HttpContentCompressor.class);
    }
    
    // 4. 查找 HttpRequestDecoder 或 HttpServerCodec
    ChannelHandlerContext ctx = p.context(HttpRequestDecoder.class);
    final String encoderName;
    if (ctx == null) {
        // 用户使用的是 HttpServerCodec
        ctx = p.context(HttpServerCodec.class);
        // 在 HttpServerCodec 之前添加 WebSocket 编解码器
        p.addBefore(ctx.name(), "wsencoder", newWebSocketEncoder());
        p.addBefore(ctx.name(), "wsdecoder", newWebsocketDecoder());
        encoderName = ctx.name();
    } else {
        // 用户分别使用了 HttpRequestDecoder + HttpResponseEncoder
        // 替换 decoder 为 WebSocket decoder
        p.replace(ctx.name(), "wsdecoder", newWebsocketDecoder());
        // 在 encoder 之前添加 WebSocket encoder
        encoderName = p.context(HttpResponseEncoder.class).name();
        p.addBefore(encoderName, "wsencoder", newWebSocketEncoder());
    }
    
    // 5. 发送握手响应，成功后移除 HTTP 编码器
    channel.writeAndFlush(response).addListener(future -> {
        if (future.isSuccess()) {
            channel.pipeline().remove(encoderName);  // 移除 HttpServerCodec/HttpResponseEncoder
            promise.setSuccess();
        } else {
            promise.setFailure(future.cause());
        }
    });
    return promise;
}
```

Pipeline 的变化过程可以用下图表示：

```
握手前:                                      握手后:
┌───────────────────────┐                  ┌───────────────────────┐
│ HttpServerCodec       │                  │ WebSocket13FrameDecoder│
│   ├ HttpRequestDecoder│   ────────▶      │ WebSocket13FrameEncoder│
│   └ HttpResponseEncoder│                 │ (HttpServerCodec 已移除)│
│ HttpObjectAggregator  │  (已移除)         │ BusinessHandler        │
│ BusinessHandler       │                  │                        │
└───────────────────────┘                  └───────────────────────┘
```

关键点：WebSocket encoder 和 decoder 在 HTTP 响应发送**之前**就被添加到 Pipeline 中。这是因为握手响应仍然需要通过 HTTP 编码器发送（它是 HTTP/1.1 格式的 101 响应），但之后的数据帧需要通过 WebSocket 编解码器处理。HTTP 编码器在响应发送完成后的 listener 中才被移除，确保握手响应的编码不受影响。

### 5.5 非 FullHttpRequest 的握手处理

如果用户没有使用 `HttpObjectAggregator`，收到的 `HttpRequest` 不是 `FullHttpRequest`。`WebSocketServerHandshaker` 提供了一个重载的 `handshake` 方法，它会动态添加一个临时的 `HttpObjectAggregator` 和一个 `ChannelInboundHandlerAdapter` 来等待请求被聚合完成：

```java
public final ChannelFuture handshake(final Channel channel, HttpRequest req,
        final HttpHeaders responseHeaders, final ChannelPromise promise) {
    if (req instanceof FullHttpRequest) {
        return handshake(channel, (FullHttpRequest) req, responseHeaders, promise);
    }
    
    // 动态添加临时 aggregator
    if (HttpUtil.isContentLengthSet(req) || HttpUtil.isTransferEncodingChunked(req) ||
        version == WebSocketVersion.V00) {
        aggregatorCtx = "httpAggregator";
        p.addAfter(ctx.name(), aggregatorCtx, new HttpObjectAggregator(8192));
    }
    
    // 添加临时 handshaker handler，等待 FullHttpRequest 到达
    p.addAfter(aggregatorCtx, "handshaker", new ChannelInboundHandlerAdapter() {
        private FullHttpRequest fullHttpRequest;
        
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof FullHttpRequest) {
                ctx.pipeline().remove(this);  // 移除自身
                handshake(channel, (FullHttpRequest) msg, responseHeaders, promise);
            }
            // ... 处理 HttpRequest + LastHttpContent 分片到达的情况
        }
    });
    
    // 重新触发当前请求的读取，使其通过新添加的 aggregator
    ctx.fireChannelRead(ReferenceCountUtil.retain(req));
}
```

这是一个非常精巧的设计——它在运行时动态组装了一个"等待聚合 → 握手"的临时 Pipeline 子链，完成后又自动拆除。

---

## 六、HttpObjectEncoder 的编码过程

### 6.1 编码状态机

`HttpObjectEncoder` 使用一个简单的状态机来管理编码过程中消息和内容的配对：

```java
private static final int ST_INIT = 0;              // 初始状态，等待 HttpMessage
private static final int ST_CONTENT_NON_CHUNK = 1; // 定长内容模式
private static final int ST_CONTENT_CHUNK = 2;     // chunked 内容模式
private static final int ST_CONTENT_ALWAYS_EMPTY = 3; // 无内容模式
```

当收到一个 `HttpMessage` 时，根据其头部决定进入哪种内容模式：

```java
private ByteBuf encodeInitHttpMessage(ChannelHandlerContext ctx, H m) throws Exception {
    assert state == ST_INIT;
    
    ByteBuf buf = ctx.alloc().buffer((int) headersEncodedSizeAccumulator);
    encodeInitialLine(buf, m);
    
    // 决定内容模式
    state = isContentAlwaysEmpty(m) ? ST_CONTENT_ALWAYS_EMPTY :
            HttpUtil.isTransferEncodingChunked(m) ? ST_CONTENT_CHUNK : ST_CONTENT_NON_CHUNK;
    
    sanitizeHeadersBeforeEncode(m, state == ST_CONTENT_ALWAYS_EMPTY);
    encodeHeaders(m.headers(), buf);
    ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
    
    // 更新 EMA 预测值
    headersEncodedSizeAccumulator = HEADERS_WEIGHT_NEW * padSizeForAccumulation(buf.readableBytes()) +
            HEADERS_WEIGHT_HISTORICAL * headersEncodedSizeAccumulator;
    return buf;
}
```

### 6.2 指数移动平均预测缓冲区大小

`headersEncodedSizeAccumulator` 是一个指数移动平均（EMA）预测器，用于预测下一次编码时 Header 部分的大小，从而预先分配合适大小的 ByteBuf：

```java
private static final float HEADERS_WEIGHT_NEW = 1 / 5f;       // 新数据权重 20%
private static final float HEADERS_WEIGHT_HISTORICAL = 1 - HEADERS_WEIGHT_NEW; // 历史权重 80%

// 初始值 256，每次编码后更新
headersEncodedSizeAccumulator = HEADERS_WEIGHT_NEW * padSizeForAccumulation(buf.readableBytes()) +
        HEADERS_WEIGHT_HISTORICAL * headersEncodedSizeAccumulator;

// padSizeForAccumulation 添加 33% 的 padding，宁愿多分配也不愿扩容拷贝
private static int padSizeForAccumulation(int readableBytes) {
    return (readableBytes << 2) / 3;  // readableBytes * 4 / 3
}
```

这种 EMA 预测的优势在于：对于 Header 大小相对稳定的场景（大多数 HTTP 服务的响应 Header 大小变化不大），预分配的缓冲区大小会收敛到实际值附近，既避免了过小导致扩容拷贝，也避免了过大导致内存浪费。20% 的新数据权重使得预测值能逐步适应 Header 大小的变化趋势。

### 6.3 小内容内联合并优化

对于 `FullHttpMessage`（已聚合的完整消息），如果内容较小，encoder 会将内容内联到 Header 缓冲区中，避免额外的 ByteBuf 分配：

```java
private void encodeFullHttpMessage(ChannelHandlerContext ctx, Object o, List<Object> out) {
    final FullHttpMessage msg = (FullHttpMessage) o;
    final int state = isContentAlwaysEmpty(m) ? ST_CONTENT_ALWAYS_EMPTY :
            HttpUtil.isTransferEncodingChunked(m) ? ST_CONTENT_CHUNK : ST_CONTENT_NON_CHUNK;
    
    ByteBuf content = msg.content();
    
    // 判断是否可以内联：内容 ≤ max(128, header预估大小的1/8)
    final boolean accountForContentSize = content.readableBytes() > 0 &&
        state == ST_CONTENT_NON_CHUNK &&
        content.readableBytes() <= Math.max(COPY_CONTENT_THRESHOLD, ((int) headersEncodedSizeAccumulator) / 8);
    
    final int headersAndContentSize = (int) headersEncodedSizeAccumulator +
        (accountForContentSize ? content.readableBytes() : 0);
    final ByteBuf buf = ctx.alloc().buffer(headersAndContentSize);
    
    encodeInitialLine(buf, m);
    encodeHeaders(m.headers(), buf);
    ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
    
    // 如果内联，直接将内容写入同一个 buf
    if (accountForContentSize && content.readableBytes() > 0) {
        buf.writeBytes(content);  // 合并到一个 ByteBuf
    }
}
```

`COPY_CONTENT_THRESHOLD = 128` 意味着对于 body ≤ 128 字节的小响应（如常见的 JSON API 响应 `{"status":"ok"}`），Header 和 Body 会被合并到一个 ByteBuf 中，减少一次 `ctx.write()` 调用和一次 ByteBuf 分配。

### 6.4 chunked 编码

对于 `Transfer-Encoding: chunked` 的响应，encoder 需要在每个 `HttpContent` 前后添加 chunk 大小行和 CRLF：

```java
private void encodeChunkedHttpContent(ChannelHandlerContext ctx, ByteBuf content,
        HttpHeaders trailingHeaders, List<Object> out) {
    final int contentLength = content.readableBytes();
    if (contentLength > 0) {
        // 写入 chunk 大小行：hex长度 + CRLF
        addEncodedLengthHex(ctx, contentLength, out);
        // 写入 chunk 数据
        out.add(content.retain());
        // 写入 chunk 后的 CRLF
        out.add(CRLF_BUF.duplicate());
    }
    if (trailingHeaders != null) {
        // 最后一个 chunk，写入 trailer
        encodeTrailingHeaders(ctx, trailingHeaders, out);
    } else if (contentLength == 0) {
        out.add(content.retain());  // 确保至少产出一个对象
    }
}
```

`addEncodedLengthHex` 直接将 long 转为十六进制 ASCII 写入 ByteBuf，避免了 `Long.toHexString()` 的字符串分配：

```java
private static void addEncodedLengthHex(ChannelHandlerContext ctx, long contentLength, List<Object> out) {
    int hexLen = contentLength == 0 ? 1 : (Long.SIZE - Long.numberOfLeadingZeros(contentLength) + 3) >>> 2;
    ByteBuf buf = ctx.alloc().buffer(hexLen + 2);  // +2 for CRLF
    writeHexAscii(buf, contentLength, hexLen);
    ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
    out.add(buf);
}

private static void writeHexAscii(ByteBuf out, long contentLength, int hexLen) {
    for (int shift = (hexLen - 1) << 2; shift >= 0; shift -= 4) {
        out.writeByte(HEX[(int) ((contentLength >>> shift) & 0xF)]);
    }
}
```

对于结束的 `LastHttpContent`，如果没有 trailer headers，则直接写入预创建的 `ZERO_CRLF_CRLF_BUF`（`"0\r\n\r\n"`），这是一个不可释放的只读 ByteBuf，零分配：

```java
private static final ByteBuf ZERO_CRLF_CRLF_BUF = unreleasableBuffer(
    directBuffer(ZERO_CRLF_CRLF.length).writeBytes(ZERO_CRLF_CRLF)).asReadOnly();
```

---

## 七、HTTP/2 简要概述

### 7.1 从 HTTP/1.1 到 HTTP/2

HTTP/2 的核心改进是多路复用——在一个 TCP 连接上可以同时传输多个请求/响应流（Stream），每个流由一个整数 ID 标识，流内的数据被分割为帧（Frame）。HTTP/2 还引入了 HPACK 头部压缩和服务器推送等特性。

Netty 的 HTTP/2 实现位于 `codec-http2` 模块，核心 Handler 包括：

| Handler | 职责 |
|---|---|
| Http2ConnectionHandler | 核心帧读写，管理连接生命周期 |
| Http2FrameCodec | 将帧事件映射为 Http2Frame 对象 |
| Http2MultiplexHandler | 为每个流创建子 Channel |
| HttpServerUpgradeHandler | 通用 HTTP/1.1 → HTTP/2 升级框架 |

### 7.2 Http2ConnectionHandler 的两阶段解码

`Http2ConnectionHandler` 继承 `ByteToMessageDecoder`，内部使用一个 `BaseDecoder` 抽象类实现两阶段解码：

```java
private abstract class BaseDecoder {
    public abstract void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception;
}

// 第一阶段：连接前奏验证
private final class PrefaceDecoder extends BaseDecoder {
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (readClientPrefaceString(in) && verifyFirstFrameIsSettings(in)) {
            // 前奏读取完成，切换到帧解码器
            byteDecoder = new FrameDecoder();
            byteDecoder.decode(ctx, in, out);
        }
    }
    
    private boolean readClientPrefaceString(ByteBuf in) throws Http2Exception {
        // 逐字节匹配 HTTP/2 连接前奏
        // "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
        if (!ByteBufUtil.equals(in, in.readerIndex(),
                clientPrefaceString, clientPrefaceString.readerIndex(), bytesRead)) {
            // 检查是否误用了 HTTP/1.x
            int http1Index = ByteBufUtil.indexOf(HTTP_1_X_BUF, in.slice(...));
            if (http1Index != -1) {
                throw connectionError(PROTOCOL_ERROR, "Unexpected HTTP/1.x request: %s", chunk);
            }
            throw connectionError(PROTOCOL_ERROR, "HTTP/2 client preface string missing or corrupt");
        }
        // ...
    }
    
    private boolean verifyFirstFrameIsSettings(ByteBuf in) throws Http2Exception {
        // 验证第一个帧必须是 SETTINGS 帧且不是 ACK
        short frameType = in.getUnsignedByte(in.readerIndex() + 3);
        if (frameType != SETTINGS) {
            throw connectionError(PROTOCOL_ERROR, "First received frame was not SETTINGS");
        }
        // ...
    }
}

// 第二阶段：帧解码
private final class FrameDecoder extends BaseDecoder {
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 持续读取和分发 HTTP/2 帧
        // 帧格式: [长度(3字节)][类型(1字节)][标志(1字节)][流ID(4字节)][载荷]
    }
}
```

连接前奏（Connection Preface）是 HTTP/2 协议的一个设计：每个 HTTP/2 连接都以一个固定的 24 字节魔法字符串开始（`"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"`），紧跟着一个 SETTINGS 帧。这个前奏的存在使得服务器可以明确区分 HTTP/1.x 和 HTTP/2 连接——如果收到的前缀不匹配，就说明客户端在使用错误的协议版本。

### 7.3 Http2MultiplexHandler 的子 Channel 模型

`Http2MultiplexHandler` 为每个 HTTP/2 流创建一个独立的子 Channel，使得每个流的业务处理可以完全隔离：

```
HTTP/2 连接 Channel
├── Stream 1 子 Channel → 子 Pipeline → StreamHandler A
├── Stream 3 子 Channel → 子 Pipeline → StreamHandler B
├── Stream 5 子 Channel → 子 Pipeline → StreamHandler C
└── ...
```

这种设计使得 HTTP/2 的多路复用对业务层透明——每个流看起来就像一个独立的连接，业务 Handler 不需要关心流 ID 和帧类型。

### 7.4 HttpServerUpgradeHandler 通用升级框架

`HttpServerUpgradeHandler` 提供了一个通用的 HTTP/1.1 → HTTP/2 升级框架。它拦截 HTTP 请求中的 `Upgrade: h2c` 头，在升级完成前用 HTTP/1.1 处理，升级完成后替换为 HTTP/2 Handler：

```
HTTP/1.1 请求 (Upgrade: h2c)
    │
    ▼
HttpServerUpgradeHandler
    ├─ 检查 Upgrade 头 → 匹配 HTTP/2 升级
    ├─ 创建 HTTP/2 连接，复用 HTTP/1.1 的 stream 1
    ├─ 从 Pipeline 移除 HTTP/1.1 编解码器 (HttpServerCodec.upgradeFrom)
    ├─ 添加 Http2ConnectionHandler
    └─ 返回 101 Switching Protocols
```

`HttpServerCodec` 实现了 `HttpServerUpgradeHandler.SourceCodec` 接口的 `upgradeFrom` 方法：

```java
@Override
public void upgradeFrom(ChannelHandlerContext ctx) {
    ctx.pipeline().remove(this);  // 从 Pipeline 移除自身
}
```

---

## 八、完整请求-响应的端到端流程

将前面的所有组件串联起来，以一个带有 `Content-Length` 的 POST 请求为例，展示完整的数据流转过程：

### 8.1 Inbound 方向（请求解码）

```
TCP 字节流:
POST /api/upload HTTP/1.1\r\nHost: example.com\r\nContent-Type: text/plain\r\nContent-Length: 26\r\n\r\nHello, Netty HTTP Server!

│
▼ HttpServerCodec.HttpServerRequestDecoder.decode()
│  [状态机: SKIP_INITIAL_LINE_CHARS → READ_INITIAL → READ_HEADER → READ_FIXED_LENGTH_CONTENT]
│  产出:
│    1. DefaultHttpRequest(POST, /api/upload, HTTP/1.1, headers)
│    2. DefaultLastHttpContent(26 bytes: "Hello, Netty HTTP Server!")
│
▼ HttpServerKeepAliveHandler.channelRead()
│  收到 HttpRequest → pendingResponses: 0 → 1, persistentConnection = isKeepAlive(req)
│  透传两个对象
│
▼ HttpObjectAggregator.decode()
│  收到 HttpRequest → isStartMessage? Yes
│    → isContentLengthInvalid? 26 > 1MB? No
│    → 创建 AggregatedFullHttpRequest, CompositeByteBuf
│  收到 LastHttpContent → isContentMessage? Yes, isLastContentMessage? Yes
│    → appendPartialContent(content, 26 bytes)
│    → finishAggregation: setInt(CONTENT_LENGTH, 26)
│    → out.add(AggregatedFullHttpRequest)
│  产出:
│    1. AggregatedFullHttpRequest (完整请求, body = CompositeByteBuf)
│
▼ BusinessHandler.channelRead0()
│  处理 FullHttpRequest, 构建响应
│  ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, OK, body))
```

### 8.2 Outbound 方向（响应编码）

```
DefaultFullHttpResponse(200 OK, Content-Length: 13, body: "Hello, World!")
│
▼ HttpServerKeepAliveHandler.write()
│  收到 HttpResponse → trackResponse: pendingResponses: 1 → 0
│  isKeepAlive(response)? Yes, isSelfDefinedMessageLength? Yes (有 Content-Length)
│  shouldKeepAlive()? pendingResponses(0)!=0? No; persistentConnection? Yes → true
│  → 不修改 Connection 头, 不添加 CLOSE listener
│  透传 FullHttpResponse
│
▼ HttpObjectEncoder.encode() → encodeFullHttpMessage()
│  state = ST_CONTENT_NON_CHUNK (有 Content-Length, 非 chunked)
│  buf = alloc.buffer(headersEncodedSizeAccumulator)  // EMA 预测大小
│  encodeInitialLine(buf, response)  → "HTTP/1.1 200 OK\r\n"
│  encodeHeaders(buf, headers)       → "Content-Type: text/plain\r\nContent-Length: 13\r\n"
│  writeShortBE(buf, CRLF_SHORT)     → "\r\n"
│  内容内联? 13 ≤ max(128, accumulator/8)? Yes → buf.writeBytes("Hello, World!")
│  产出:
│    1. ByteBuf [HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, World!]
│
▼ TCP 发送
```

### 8.3 对应的 chunked 响应编码

如果响应使用 chunked 编码，流程略有不同：

```
DefaultHttpResponse(200 OK, Transfer-Encoding: chunked)
  + DefaultHttpContent("Hello, ")
  + DefaultLastHttpContent("World!")

▼ HttpObjectEncoder:
  HttpMessage → state = ST_CONTENT_CHUNK
  buf1 = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
  
  HttpContent("Hello, ") →
  buf2 = "6\r\n"        // chunk size (6 bytes)
  [Hello, ]              // chunk data (retained slice)
  buf3 = "\r\n"          // CRLF
  
  LastHttpContent("World!") →
  buf4 = "6\r\n"        // chunk size (6 bytes)
  [World!]               // chunk data (retained slice)
  buf5 = "\r\n"          // CRLF
  buf6 = "0\r\n\r\n"     // 最后一个 chunk (ZERO_CRLF_CRLF_BUF)

最终 TCP 发送的 ByteBuf 序列:
  HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n
  6\r\nHello, \r\n
  6\r\nWorld!\r\n0\r\n\r\n
```

---

## 九、本篇涉及的设计模式

**状态机模式（State Machine）**贯穿了 HttpObjectDecoder 的整个设计。11 个显式状态（`SKIP_INITIAL_LINE_CHARS`、`READ_INITIAL`、`READ_HEADER`、`READ_FIXED_LENGTH_CONTENT` 等）通过 `switch(currentState)` 驱动解码流程。每个状态只关注自己的解析逻辑，状态转换由解析结果决定。这种设计将复杂的 HTTP 协议解析分解为若干个简单的、可独立测试的状态处理单元。HttpObjectEncoder 同样使用状态机（`ST_INIT` → `ST_CONTENT_NON_CHUNK` / `ST_CONTENT_CHUNK` / `ST_CONTENT_ALWAYS_EMPTY`）来管理编码过程中消息和内容的配对关系。Http2ConnectionHandler 的 `PrefaceDecoder` → `FrameDecoder` 两阶段切换也是状态机思想的体现——连接前奏验证完成后，`byteDecoder` 引用从 `PrefaceDecoder` 切换到 `FrameDecoder`，实现了状态转换。

**组合模式（Composite）**体现在 HttpServerCodec 的设计中。它继承 `CombinedChannelDuplexHandler<HttpRequestDecoder, HttpResponseEncoder>`，将一个 Inbound decoder 和一个 Outbound encoder 组合成一个双向 Handler。组合的关键是 `DelegatingChannelHandlerContext`——它作为代理上下文，确保 Inbound 事件只传播给 decoder，Outbound 事件只传播给 encoder，两者互不干扰。这种模式比 `ByteToMessageCodec` 更灵活，因为被组合的两个 Handler 可以是完全不相关的类型，各自独立管理状态。

**装饰器模式（Decorator）**体现在 `AggregatedFullHttpMessage` 及其子类 `AggregatedFullHttpRequest` / `AggregatedFullHttpResponse` 的设计中。聚合后的完整消息用装饰器包装原始的 `HttpMessage`（持有 headers 和 protocol version）和聚合后的 `ByteBuf`（持有完整 body）。所有 `HttpMessage` 接口的方法委托给内部的 `message` 字段，所有 `ByteBufHolder` 接口的方法委托给内部的 `content` 字段。这种设计使得聚合后的对象对外暴露统一的 `FullHttpMessage` 接口，同时避免了数据拷贝——原始的 headers 和 body buffer 被直接复用。`AggregatedFullHttpRequest` 进一步实现了 `FullHttpRequest` 接口，将 `method()` 和 `uri()` 方法委托给内部的 `HttpRequest` 对象。

**模板方法模式（Template Method）**体现在 HttpObjectDecoder 的类层次中。父类定义了完整的解码算法骨架（`decode` 方法中的 `switch(currentState)` 大循环），子类只需实现抽象方法 `createMessage`、`createInvalidMessage`、`isDecodingRequest` 以及可选的 `isContentAlwaysEmpty`。`HttpRequestDecoder` 和 `HttpResponseDecoder` 分别提供各自的实现。HttpObjectEncoder 同样使用模板方法——`encodeInitialLine` 是抽象方法，`HttpResponseEncoder` 将其实现为写入 `"HTTP/1.1 200 OK"` 格式的状态行，而 `HttpRequestEncoder` 将其实现为写入 `"GET /path HTTP/1.1"` 格式的请求行。`MessageAggregator` 的 `decode` 方法也定义了聚合算法骨架，子类通过实现 `isStartMessage`、`isContentMessage`、`beginAggregation` 等钩子方法来自定义行为。

**策略模式（Strategy）**体现在 HttpObjectEncoder 对不同内容类型的编码策略上。`ST_CONTENT_NON_CHUNK` 策略直接将内容追加到缓冲区；`ST_CONTENT_CHUNK` 策略在每个 chunk 前后添加大小行和 CRLF；`ST_CONTENT_ALWAYS_EMPTY` 策略跳过内容编码。策略的选择由 `isContentAlwaysEmpty` 和 `HttpUtil.isTransferEncodingChunked` 的判断结果决定，调用方（`encode` 方法）不需要关心具体的编码细节。`HttpResponseEncoder.sanitizeHeadersBeforeEncode` 也是策略模式的体现——对于 1xx、204、304 等状态码，自动移除 Content-Length 和 Transfer-Encoding 头，这是 RFC 7230 规定的特殊处理策略。

**工厂方法模式（Factory Method）**体现在 `WebSocketServerHandshakerFactory` 中。`resolveHandshaker0` 方法根据 `Sec-WebSocket-Version` 头的值，创建不同版本的 Handshaker（V13、V08、V07 或 V00）。客户端不需要知道具体使用哪个版本的实现，工厂方法封装了版本选择的逻辑。`WebSocketServerHandshaker` 的 `newWebsocketDecoder` 和 `newWebSocketEncoder` 也是抽象工厂方法，由各版本的子类（如 `WebSocketServerHandshaker13`）提供具体的编解码器实现。

---

## 十、本篇涉及的高性能并发技术

**Bit-packed 队列（位压缩队列）**是 HttpServerCodec 中最精巧的性能优化。请求方法队列使用一个 `long` 字段（64 位）存储 32 个请求的方法标记，每个标记占 2 位。入队操作是 `methodQueue |= (long) flag << (methodQueueSize << 1)`——一次位 OR 和左移；出队操作是 `flag = (byte)(methodQueue & 0x3L); methodQueue >>>= 2`——一次位 AND 和无符号右移。整个队列操作无分配、无 CAS、无锁，因为 HttpServerCodec 的所有方法都在同一个 EventLoop 线程中执行。当 pipelining 请求超过 32 个时，才回退到 `ArrayDeque<Byte>`，但溢出队列在排空后会立即被置 null（`methodOverflowQueue = null`），后续请求重新使用内联队列。这种设计在 99.9% 的场景下（≤32 个并发 pipelining 请求）实现了零分配。

**int/long 快速比较（Fast Integer Comparison）**是 HttpRequestDecoder 的关键优化。对于常见的 HTTP 方法（GET、POST）、协议版本（HTTP/1.1、HTTP/1.0）和头名（Host、Content-Type、Content-Length、Connection、Accept），它将 3-8 字节的字符串打包为一个 int 或 long，然后用一次整数比较替代 `String.equals`。例如，`"GET"` 的识别是 `sb[start] | sb[start+1] << 8 | sb[start+2] << 16 == GET_AS_INT`，只需 3 次数组访问、2 次移位、2 次 OR 和 1 次比较。`splitHeaderName` 方法在首字符快速分支的基础上，先检查字符串长度（`length == 4` for Host, `length == 12` for Content-Type），再进行 int/long 比较，避免了对不匹配头名的不必要比较。这种优化在 HTTP 服务器每秒处理数万请求的场景下，显著减少了字符串对象的创建和比较开销。

**预计算查找表（Precomputed Lookup Table）**贯穿了 HttpObjectDecoder 的字节处理逻辑。`ISO_CONTROL_OR_WHITESPACE[256]`、`SP_LENIENT_BYTES[256]`、`LATIN_WHITESPACE[256]` 三个 boolean 数组在类加载时预计算，将 `Character.isISOControl(b) || Character.isWhitespace(b)` 这样的多方法调用简化为一次数组下标访问。`LineParser` 的 `SKIP_CONTROL_CHARS_BYTES` ByteProcessor 使用这些查找表逐字节扫描，每个字节只需一次数组访问。`HEX[16]` 数组将十六进制数字到 ASCII 字符的转换从 `Character.forDigit` 简化为一次数组访问。这些预计算表虽然只占用几百字节的内存，但在高频解码路径上避免了大量的方法调用开销。

**指数移动平均预测（Exponential Moving Average Prediction）**体现在 HttpObjectEncoder 的 `headersEncodedSizeAccumulator` 和 `trailersEncodedSizeAccumulator` 中。这两个 float 字段以 20% 新数据 + 80% 历史数据的权重持续更新，预测下一次编码时 Header/Trailer 的大小。基于预测值预分配 ByteBuf，既避免了过小导致 ByteBuf 扩容（涉及内存拷贝），也避免了过大导致内存浪费。`padSizeForAccumulation` 在预测值基础上再添加 33% 的 padding（`readableBytes * 4 / 3`），进一步降低扩容概率。EMA 的优势在于它能自适应 Header 大小的变化趋势——如果服务端的响应 Header 逐渐增大（如 Cookie 越来越长），预测值会逐步跟上。20% 的新数据权重确保了适应性不会太慢也不会太抖动。

**CompositeByteBuf 零拷贝聚合（Zero-Copy Aggregation）**是 HttpObjectAggregator 的核心内存优化。聚合过程中，每个 `HttpContent` 的 ByteBuf 被作为一个独立组件添加到 `CompositeByteBuf` 中（`content.addComponent(true, partialContent.retain())`），不需要将数据拷贝到连续内存区域。`maxCumulationBufferComponents`（默认 1024）限制了组件数量，超过时会触发合并（consolidate）操作将多个组件拷贝为一个。这种设计在大多数场景下（chunk 数量 ≤ 1024）实现了零拷贝聚合，只有在极端情况下（超长 chunked 消息）才会触发一次合并操作。`readRetainedSlice` 在 HttpObjectDecoder 中也用于产出 chunk——它是 cumulation 的切片视图，共享底层内存，避免了 chunk 数据的拷贝。

**预创建静态响应对象（Pre-created Static Response Objects）**减少了 HttpObjectAggregator 在错误路径上的分配。`CONTINUE`（100）、`EXPECTATION_FAILED`（417）、`TOO_LARGE`（413）、`TOO_LARGE_CLOSE`（413 + Connection: close）四个 FullHttpResponse 对象在类加载时创建，使用时通过 `retainedDuplicate()` 获取引用。`retainedDuplicate` 会增加引用计数而不复制底层 buffer，是一个轻量级操作。HttpObjectEncoder 中的 `CRLF_BUF`、`ZERO_CRLF_CRLF_BUF` 也是预创建的不可释放只读 ByteBuf，在 chunked 编码中反复使用而不产生任何分配。这种"预分配 + 引用计数复用"的模式在 Netty 的热路径上随处可见。

**Pipeline 热插拔（Hot-swappable Pipeline）**是 WebSocket 升级握手的技术基础。`WebSocketServerHandshaker.handshake` 方法在运行时动态修改 Pipeline：移除 `HttpObjectAggregator` 和 `HttpContentCompressor`，替换 `HttpRequestDecoder` 为 `WebSocketFrameDecoder`，在 `HttpResponseEncoder` 前添加 `WebSocketFrameEncoder`，最后在握手响应发送完成后移除 HTTP 编码器。整个过程不需要关闭连接或中断数据流——`addBefore`、`remove`、`replace` 操作都是在线程安全的 Pipeline 修改机制下进行的。对于非 `FullHttpRequest` 的场景，还会动态添加临时 `HttpObjectAggregator` 和临时 `ChannelInboundHandlerAdapter`，完成后自动移除。这种运行时 Pipeline 重构能力是 Netty 区别于其他网络框架的重要特性——它使得协议升级（HTTP → WebSocket、HTTP/1.1 → HTTP/2）可以平滑进行，不需要重建连接。
