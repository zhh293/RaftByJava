# Sink过滤与Store环形缓冲区 —— 源码全流程解析

> 本文深入剖析 Canal 中 Filter（数据过滤）、Sink（数据投递）和 Store（环形缓冲区存储）三大模块的完整源码实现。
> 从 AviaterRegexFilter 的正则过滤引擎，到 EntryEventSink 的双级过滤+Handler责任链投递，再到 MemoryEventStoreWithBuffer 的三指针 RingBuffer 模型，
> 逐行解读每一个核心方法的实现细节与设计意图。

---

## 全局数据流定位

在 Canal 的整体架构中，数据从 MySQL Binlog 到客户端消费，经历四个核心阶段：

```
                    Parser阶段                    Sink阶段                    Store阶段                   Client阶段
                ┌─────────────┐              ┌──────────────┐           ┌────────────────┐          ┌──────────────┐
                │             │              │              │           │                │          │              │
  MySQL Binlog  │  模拟Slave   │  List<Event> │  Filter过滤   │  Events   │  RingBuffer    │  Events  │  CanalClient  │
  ────────────> │  解析Binlog  │ ──────────> │  Sink投递     │ ────────> │  三指针存储     │ ────────>│  消费&ACK     │
                │             │              │              │           │                │          │              │
                └─────────────┘              └──────────────┘           └────────────────┘          └──────────────┘
                   Parser线程                    Parser线程                  Store线程                  Client线程
```

详细的数据流转过程如下：

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              Canal Instance 内部数据流转全景                                         │
│                                                                                                     │
│  ┌───────────────┐     ┌─────────────────────────────────────┐     ┌──────────────────────────┐     │
│  │   Parser      │     │              Sink                    │     │          Store            │     │
│  │               │     │                                       │     │                            │     │
│  │  MysqlConnection  │     │  ┌─────────────────────┐    │     │  MemoryEventStoreWithBuffer │     │
│  │  ┌──────────┐  │     │  │  AviaterRegexFilter  │    │     │  ┌────────────────────────┐  │     │
│  │  │BinlogDump│  │     │  │  (正则过滤ROWDATA)    │    │     │  │                        │  │     │
│  │  └────┬─────┘  │     │  └─────────┬───────────┘    │     │  │   entries[] RingBuffer  │  │     │
│  │       │        │     │            │                 │     │  │                        │  │     │
│  │       v        │     │  ┌─────────v───────────┐    │     │  │  putSequence            │  │     │
│  │  ┌──────────┐  │     │  │  EntryEventSink      │    │     │  │  getSequence            │  │     │
│  │  │LogEvent  │  │     │  │  .sinkData()         │    │     │  │  ackSequence            │  │     │
│  │  │→Entry    │  │     │  │                      │    │     │  │                        │  │     │
│  │  └────┬─────┘  │     │  │  doFilter()          │    │     │  │  ReentrantLock +        │  │     │
│  │       │        │     │  │  doSink()             │    │     │  │  Condition(notEmpty/   │  │     │
│  │       v        │     │  │    └─Handler责任链    │    │     │  │  notFull)              │  │     │
│  │  ┌──────────┐  │     │  │    └─tryPut/put      │────│──>──│  │                        │  │     │
│  │  │List<Event>│  │     │  └─────────────────────┘    │     │  └────────────────────────┘  │     │
│  │  └────┬─────┘  │     │                               │     │                              │     │
│  │       │        │     │  ┌─────────────────────┐    │     │  ┌────────────────────────┐  │     │
│  └───────┼────────┘     │  │  HeartBeatHandler    │    │     │  │  CanalEventUtils        │  │     │
│          │              │  │  (心跳事件处理)       │    │     │  │  (位点计算工具)          │  │     │
│          │              │  └─────────────────────┘    │     │  └────────────────────────┘  │     │
│          │              │                               │     │                              │     │
│          │              │  ┌─────────────────────┐    │     │  BatchMode:                  │     │
│          │              │  │  GroupEventSink       │    │     │  ┌──────────────────────┐  │     │
│          │              │  │  + TimelineBarrier    │    │     │  │ ITEMSIZE (按条数)     │  │     │
│          │              │  │  + TimelineTxBarrier  │    │     │  │ MEMSIZE (按内存大小)   │  │     │
│          │              │  │  (多源归并排序)        │    │     │  └──────────────────────┘  │     │
│          │              │  └─────────────────────┘    │     │                              │     │
│          │              └───────────┬───────────────────┘     └──────────────┬───────────────┘     │
│          │                          │                                        │                     │
│          v                          v                                        v                     │
│   List<Event>                doSink → eventStore.put()                 get() → Events              │
│   (原始Entry)                (过滤后投递)                               (客户端批量获取)               │
│                                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

三个模块的职责分工：

| 模块 | 职责 | 核心类 | 线程模型 |
|------|------|--------|----------|
| Filter | 按 schema/table 正则规则过滤 ROWDATA 事件 | `AviaterRegexFilter` | Parser 线程 |
| Sink | 过滤空事务、Handler 责任链增强、自旋重试投递 Store | `EntryEventSink` | Parser 线程 |
| Store | 三指针 RingBuffer 存储，支持阻塞/超时/非阻塞三种语义 | `MemoryEventStoreWithBuffer` | Parser 线程 put + Client 线程 get |

---

## 第一章：Filter 模块 —— 数据过滤引擎

### 1.1 模块总览

Canal 的 Filter 模块提供了对 Binlog 事件的过滤能力，允许用户通过配置规则只订阅特定的 schema 和 table。Filter 模块的核心架构如下：

```
┌─────────────────────────────────────────────────────┐
│                   Filter 模块架构                     │
│                                                       │
│  ┌─────────────────────┐                             │
│  │  CanalEventFilter   │  ← 过滤器顶层接口             │
│  │  <T>                │                             │
│  └──────────┬──────────┘                             │
│             │                                         │
│     ┌───────┼───────────────────┐                   │
│     │       │                   │                   │
│     v       v                   v                   │
│  ┌──┴──────────────┐  ┌──────────────┐  ┌──────────┴──────┐
│  │AviaterRegex     │  │AviaterSimple  │  │AviaterEL        │
│  │Filter           │  │Filter         │  │Filter           │
│  │(正则匹配)        │  │(简单等值匹配)  │  │(EL表达式匹配)    │
│  └─────────────────┘  └──────────────┘  └─────────────────┘
│         │                                              │
│         │  依赖                                         │
│         v                                              │
│  ┌─────────────────┐  ┌──────────────┐                │
│  │  RegexFunction  │  │ PatternUtils │                │
│  │  (Aviator自定义  │  │ (正则缓存)    │                │
│  │   函数)          │  │              │                │
│  └─────────────────┘  └──────────────┘                │
└─────────────────────────────────────────────────────┘
```

### 1.2 CanalEventFilter 接口

```java
package com.alibaba.otter.canal.filter;

/**
 * canal过滤器接口
 * 
 * @author jianghang 2012-7-23 上午10:51:40
 * @version 1.0.0
 */
public interface CanalEventFilter<T> {

    /**
     * 过滤事件，返回true代表允许通过，false代表被过滤
     */
    boolean filter(T event);
}
```

**这一步在干什么？**

`CanalEventFilter` 是过滤模块的最顶层接口，泛型参数 `T` 代表被过滤的对象类型。在 Canal 中，`T` 通常是 `CanalEntry.Entry`。接口只定义了一个 `filter()` 方法，返回 `true` 表示事件允许通过，`false` 表示被过滤掉。这种简洁的单一职责设计使得过滤逻辑可以灵活组合。

### 1.3 AviaterRegexFilter 的完整实现

`AviaterRegexFilter` 是 Canal 中最重要、使用最广泛的过滤器。它利用 Aviator 表达式引擎和 ORO 正则库，实现了对 `schema.table` 格式的高效正则匹配。

#### 1.3.1 完整源码

```java
package com.alibaba.otter.canal.filter.aviater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.filter.exception.CanalFilterException;
import com.google.common.base.Function;
import com.google.common.collect.OdrIterables;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import com.googlecode.aviator.exception.ExpressionRuntimeException;

/**
 * 使用Aviator进行filter的正则匹配
 * 
 * @author jianghang 2012-7-23 上午10:33:52
 * @version 1.0.0
 */
public class AviaterRegexFilter implements CanalEventFilter<String> {

    private static final String             SPLITCHAR = ";";

    private final Pattern[]                 patterns; // 编译后的正则模式数组
    private final String                    pattern;  // 原始pattern字符串
    private final com.googlecode.aviator.Expression filterExpression; // Aviator表达式

    public AviaterRegexFilter(String pattern){
        this(pattern, false);
    }

    public AviaterRegexFilter(String pattern, boolean defaultEmptyValue){
        if (pattern == null || pattern.isEmpty()) {
            if (defaultEmptyValue) {
                pattern = ".*\\\\..*";
            } else {
                throw new IllegalArgumentException("pattern is null");
            }
        }
        this.pattern = pattern;
        List<String> patternList = splitPattern(pattern);
        // 对pattern按从长到短排序，防止通配符模式较短的正则先匹配成功，导致较长的正则无法生效
        Collections.sort(patternList, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o2.length() - o1.length();
            }
        });

        // 编译正则表达式
        List<Pattern> patternGroupList = new ArrayList<>();
        for (String p : patternList) {
            // 使用ORO的Perl5Compiler编译正则，添加^和$锚点确保全匹配
            Pattern compiledPattern = PatternUtils.getPattern(p);
            patternGroupList.add(compiledPattern);
        }
        this.patterns = patternGroupList.toArray(new Pattern[patternGroupList.size()]);

        // 构建Aviator表达式
        // 生成类似: regex(pattern0,regex(pattern1,...true)) 的嵌套表达式
        Map<String, Object> env = new HashMap<>();
        env.put("pattern", this.pattern);
        String expression = OdrIterables.transform(OdrIterables.from(patternList), new Function<String, String>() {
            @Override
            public String apply(String input) {
                return "regex(pattern,'" + input + "')";
            }
        }).join(" and ");

        try {
            this.filterExpression = AviatorEvaluator.compile(expression);
        } catch (CompileExpressionErrorException e) {
            throw new CanalFilterException(e);
        }
    }

    @Override
    public boolean filter(String event) throws CanalFilterException {
        if (event == null) {
            return false;
        }

        if (patterns.length == 0) {
            return true;
        }

        try {
            Map<String, Object> env = new HashMap<>();
            env.put("pattern", event);

            // 执行Aviator表达式
            Object result = filterExpression.execute(env);
            if (result instanceof Boolean) {
                return (Boolean) result;
            } else {
                throw new CanalFilterException("result is not boolean");
            }
        } catch (ExpressionRuntimeException e) {
            throw new CanalFilterException(e);
        }
    }

    /**
     * 拆分pattern字符串，按顶层逗号分割（感知括号深度），每个子pattern以分号分隔
     */
    private List<String> splitPattern(String pattern) {
        List<String> result = new ArrayList<>();
        if (pattern != null && !pattern.isEmpty()) {
            StringBuilder current = new StringBuilder();
            int depth = 0;
            for (int i = 0; i < pattern.length(); i++) {
                char c = pattern.charAt(i);
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                }

                if (c == ',' && depth == 0) {
                    // 顶层逗号，分割
                    result.add(current.toString().trim());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
            if (current.length() > 0) {
                result.add(current.toString().trim());
            }
        }
        return result;
    }
}
```

#### 1.3.2 构造函数详解 —— splitPattern 按顶层逗号拆分

**这一步在干什么？**

构造函数的核心任务是将用户传入的 pattern 字符串（如 `foo\\..*,bar\\..*`）拆分为多个独立的正则模式，然后编译并构建 Aviator 嵌套表达式。

`splitPattern` 方法的拆分逻辑非常精妙，它不是简单地用 `String.split(",")` 来分割，而是**感知括号深度**地拆分：

```
输入: foo\\..*,bar\\.(t1|t2),baz\\..*

拆分过程:
  字符: f  o  o  \  \  .  .  *  ,  b  a  r  \  .  (  t  1  |  t  2  )  ,  b  a  z  \  .  .  *
  深度: 0  0  0  0  0  0  0  0  0  0  0  0  0  0  1  1  1  1  1  0  0  0  0  0  0  0  0  0  0
  
  在 depth==0 的逗号处分割:
  结果: ["foo\\..*", "bar\\.(t1|t2)", "baz\\..*"]
```

如果用简单的 `split(",")` ，第二个 pattern 中的 `(t1|t2)` 会被错误地拆开。通过括号深度感知，确保只有在顶层（depth==0）的逗号才作为分隔符。

#### 1.3.3 从长到短排序 —— 防止误匹配

```java
Collections.sort(patternList, new Comparator<String>() {
    @Override
    public int compare(String o1, String o2) {
        return o2.length() - o1.length();
    }
});
```

**这一步在干什么？**

排序的目的是**防止通配符模式较短的正则先匹配成功**。考虑以下场景：

```
patterns: ["foo\\..*", "foo\\.bar"]
event:    "foo.bar"

如果不排序，先匹配 "foo\\..*" → 匹配成功 → 返回 true
但用户的意图可能是精确匹配 "foo.bar"，而 "foo\\..*" 只是通配

排序后: ["foo\\.bar", "foo\\..*"]  (从长到短)
先匹配 "foo\\.bar" → 匹配成功 → 更精确的规则优先
```

虽然在这个例子中两种方式结果相同，但在有排除规则（如 `!foo\\.bar`）的场景下，长模式优先匹配可以避免短通配模式"截断"精确规则的意图。

#### 1.3.4 Aviator 嵌套表达式构建

```java
String expression = OdrIterables.transform(OdrIterables.from(patternList), new Function<String, String>() {
    @Override
    public String apply(String input) {
        return "regex(pattern,'" + input + "')";
    }
}).join(" and ");
```

对于三个 pattern `["foo\\..*", "bar\\..*", "baz\\..*"]`，生成的 Aviator 表达式为：

```
regex(pattern,'foo\\..*') and regex(pattern,'bar\\..*') and regex(pattern,'baz\\..*')
```

这里用 `and` 连接所有子正则的匹配结果。注意：Aviator 中 `and` 是逻辑与，只有所有子正则都匹配成功时整体才返回 `true`。

> **等一下，这里用 `and` 而不是 `or`？**
> 
> 实际上，Canal 的过滤语义是：所有 pattern 以 `and` 连接，意味着**所有 pattern 都必须匹配**才算通过。这在配置排除规则时很有用，比如 `.*\\..*` (匹配所有) `and` `!foo\\..*` (排除 foo 库)。但通常用户配置的多个 schema/table 是用逗号分隔的，表示**或**的关系。这里的 `and` 连接配合 `regex` 函数的返回值实现了这种语义。

#### 1.3.5 filter() 方法执行流程

```java
@Override
public boolean filter(String event) throws CanalFilterException {
    if (event == null) {
        return false;
    }

    if (patterns.length == 0) {
        return true;
    }

    try {
        Map<String, Object> env = new HashMap<>();
        env.put("pattern", event);

        Object result = filterExpression.execute(env);
        if (result instanceof Boolean) {
            return (Boolean) result;
        } else {
            throw new CanalFilterException("result is not boolean");
        }
    } catch (ExpressionRuntimeException e) {
        throw new CanalFilterException(e);
    }
}
```

**这一步在干什么？**

`filter()` 方法的执行流程：

1. **空值防御**：如果 `event` 为 null，返回 false（不通过）
2. **空模式快速通过**：如果没有编译任何 pattern，返回 true（全部通过）
3. **Aviator 执行**：将 `event`（即 `schema.table` 字符串）作为变量 `pattern` 注入 Aviator 环境，执行预编译的嵌套表达式
4. **结果类型检查**：确保返回值为 Boolean 类型

执行链路：

```
filter("canal.test")
  → filterExpression.execute(env)  // env: {pattern: "canal.test"}
    → regex(pattern, 'canal\\..*')  // 调用 RegexFunction
      → Perl5Matcher.matches("canal.test", "^canal\\..*$")  // ORO 正则匹配
    → 返回 true
  → 返回 true
```

### 1.4 RegexFunction —— Aviator 自定义函数

```java
package com.alibaba.otter.canal.filter.aviater;

import java.util.Map;

import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Matcher;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;

/**
 * Aviator自定义函数，用于执行正则匹配
 */
public class RegexFunction extends AbstractFunction {

    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        // 获取待匹配的字符串（即schema.table）
        String pattern = (String) FunctionUtils.getStringValue(arg1, env);
        // 获取正则模式字符串
        String regex = (String) FunctionUtils.getStringValue(arg2, env);

        // 从缓存中获取编译好的Pattern
        Pattern compiledPattern = PatternUtils.getPattern(regex);
        // 执行匹配
        Perl5Matcher matcher = new Perl5Matcher();
        boolean result = matcher.matches(pattern, compiledPattern);

        return AviatorBoolean.valueOf(result);
    }

    @Override
    public String getName() {
        return "regex";
    }
}
```

**这一步在干什么？**

`RegexFunction` 继承自 Aviator 的 `AbstractFunction`，注册为 Aviator 运行时的自定义函数 `regex`。当 Aviator 表达式执行到 `regex(pattern, 'canal\\..*')` 时：

1. **参数提取**：`arg1` 是变量 `pattern`（即 `schema.table` 字符串），`arg2` 是正则模式字符串
2. **Pattern 获取**：通过 `PatternUtils.getPattern()` 获取编译好的正则 Pattern（带缓存）
3. **Perl5 匹配**：使用 ORO 的 `Perl5Matcher` 执行 `matches()` 全匹配
4. **返回 AviatorBoolean**：将 Java boolean 包装为 Aviator 类型返回

这里使用 ORO（Jakarta ORO）而不是 Java 原生 `java.util.regex`，是因为 Canal 早期设计中 ORO 对 Perl5 正则的兼容性更好，特别是在处理复杂正则表达式时性能更稳定。

### 1.5 PatternUtils —— 正则编译与软引用缓存

```java
package com.alibaba.otter.canal.filter;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;

/**
 * 正则Pattern工具类，使用WeakHashMap做缓存
 */
public class PatternUtils {

    private static final Map<String, Pattern> patterns = new WeakHashMap<>();

    /**
     * 编译正则表达式，添加^和$锚点确保全匹配
     */
    public static Pattern getPattern(String pattern) {
        Pattern result = patterns.get(pattern);
        if (result == null) {
            synchronized (PatternUtils.class) {
                result = patterns.get(pattern);
                if (result == null) {
                    try {
                        // 添加^和$锚点，确保全匹配
                        String anchoredPattern = "^" + pattern + "$";
                        Perl5Compiler compiler = new Perl5Compiler();
                        result = compiler.compile(anchoredPattern, Perl5Compiler.READ_ONLY_MASK);
                    } catch (MalformedPatternException e) {
                        throw new IllegalArgumentException("Illegal pattern: " + pattern, e);
                    }
                    patterns.put(pattern, result);
                }
            }
        }
        return result;
    }
}
```

**这一步在干什么？**

`PatternUtils` 提供了正则 Pattern 的编译和缓存能力：

1. **^$ 锚点全匹配**：在 pattern 前后添加 `^` 和 `$`，确保正则是全匹配而非部分匹配。例如 `canal\\..*` 变成 `^canal\\..*$`，只有完整匹配 `canal.` 开头的字符串才返回 true。
2. **WeakHashMap 软引用缓存**：使用 `WeakHashMap` 缓存编译后的 Pattern。当 JVM 内存不足时，缓存可以被 GC 回收，避免内存泄漏。
3. **双重检查锁定**：使用 synchronized + double-check 保证线程安全，避免并发重复编译。
4. **READ_ONLY_MASK**：编译时设置只读标记，编译后的 Pattern 不可变，可以被多线程安全共享。

缓存的工作流程：

```
getPattern("canal\\..*")
  → patterns.get("canal\\..*")  // 第一次为 null
  → synchronized:
    → patterns.get("canal\\..*")  // double-check，仍为 null
    → 编译: "^canal\\..*$" → Perl5Compiler.compile()
    → patterns.put("canal\\..*", compiledPattern)
  → 返回 compiledPattern

下次调用:
getPattern("canal\\..*")
  → patterns.get("canal\\..*")  // 命中缓存，直接返回
```

### 1.6 AviaterSimpleFilter 和 AviaterELFilter 简述

#### 1.6.1 AviaterSimpleFilter

```java
package com.alibaba.ototter.canal.filter.aviater;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.googlecode.aviator.AviatorEvaluator;

/**
 * 使用Aviator进行简单等值过滤
 */
public class AviaterSimpleFilter implements CanalEventFilter<String> {

    private final com.googlecode.aviator.Expression expression;

    public AviaterSimpleFilter(String expression){
        this.expression = AviatorEvaluator.compile(expression);
    }

    @Override
    public boolean filter(String event) throws CanalFilterException {
        try {
            return (Boolean) expression.execute("pattern", event);
        } catch (Exception e) {
            throw new CanalFilterException(e);
        }
    }
}
```

**这一步在干什么？**

`AviaterSimpleFilter` 用于简单的 Aviator 表达式过滤，用户直接传入 Aviator 表达式（如 `pattern == 'canal.test'`），不走正则匹配，适用于等值比较等简单场景。

#### 1.6.2 AviaterELFilter

```java
package com.alibaba.otter.canal.filter.aviater;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.googlecode.aviator.AviatorEvaluator;

/**
 * 使用Aviator进行EL表达式过滤
 */
public class AviaterELFilter implements CanalEventFilter<String> {

    private final com.googlecode.aviator.Expression expression;

    public AviaterELFilter(String expression){
        this.expression = AviatorEvaluator.compile(expression);
    }

    @Override
    public boolean filter(String event) throws CanalFilterException {
        try {
            return (Boolean) expression.execute("pattern", event);
        } catch (Exception e) {
            throw new CanalFilterException(e);
        }
    }
}
```

**这一步在干什么？**

`AviaterELFilter` 与 `AviaterSimpleFilter` 结构几乎相同，都直接编译 Aviator 表达式。区别在于语义层面：ELFilter 支持更复杂的表达式语法（如逻辑运算、函数调用），而 SimpleFilter 侧重简单等值判断。在实际使用中，`AviaterRegexFilter` 是最常用的实现。

### 1.7 Filter 模块设计总结

| 类名 | 匹配方式 | 正则引擎 | 缓存策略 | 典型场景 |
|------|---------|---------|---------|---------|
| `AviaterRegexFilter` | 正则全匹配 | ORO Perl5 | WeakHashMap | schema/table 过滤 |
| `AviaterSimpleFilter` | Aviator 表达式 | 无 | 无 | 简单等值比较 |
| `AviaterELFilter` | Aviator EL表达式 | 无 | 无 | 复杂逻辑表达式 |

```
┌──────────────────────────────────────────────────────────┐
│              AviaterRegexFilter 初始化流程                 │
│                                                            │
│  用户配置: "canal\\..*,test\\..*"                         │
│       │                                                    │
│       v                                                    │
│  splitPattern()  ──→  ["canal\\..*", "test\\..*"]        │
│       │                    │                               │
│       │                    v                               │
│       │          从长到短排序                                │
│       │                    │                               │
│       │                    v                               │
│       │    ["canal\\..*", "test\\..*"]  (长度相同，顺序不变) │
│       │                    │                               │
│       │                    v                               │
│       │    对每个pattern调用 PatternUtils.getPattern()      │
│       │         → 编译为 ^canal\\..*$ Perl5 Pattern        │
│       │         → 存入 WeakHashMap 缓存                     │
│       │                    │                               │
│       v                    v                               │
│  构建 Aviator 表达式:                                      │
│  "regex(pattern,'canal\\..*') and regex(pattern,'test\\..*')"│
│       │                                                    │
│       v                                                    │
│  AviatorEvaluator.compile() → filterExpression             │
│                                                            │
│  ─── 运行时 ───                                           │
│                                                            │
│  filter("canal.test")                                      │
│       │                                                    │
│       v                                                    │
│  filterExpression.execute({pattern: "canal.test"})        │
│       │                                                    │
│       v                                                    │
│  RegexFunction.call("canal.test", "canal\\..*")           │
│       │                                                    │
│       v                                                    │
│  Perl5Matcher.matches("canal.test", "^canal\\..*$")       │
│       │                                                    │
│       v                                                    │
│  → true                                                    │
└──────────────────────────────────────────────────────────┘
```

---

## 第二章：Sink 模块 —— 数据投递引擎

### 2.1 模块总览

Sink 模块是连接 Parser 和 Store 的桥梁。Parser 解析出的 `List<Event>` 交给 Sink 后，Sink 负责两件事：

1. **过滤**：调用 Filter 模块过滤不需要的 ROWDATA，并执行空事务过滤策略
2. **投递**：通过 Handler 责任链增强后，将事件写入 Store 的 RingBuffer

```
┌─────────────────────────────────────────────────────────────┐
│                      Sink 模块架构                            │
│                                                               │
│  ┌──────────────────┐                                        │
│  │ CanalEventSink   │  ← Sink 顶层接口                        │
│  │ <E>              │                                        │
│  └────────┬─────────┘                                        │
│           │                                                   │
│           v                                                   │
│  ┌──────────────────────┐                                    │
│  │ AbstractCanalEventSink│  ← 抽象基类，持有 eventStore 引用    │
│  │ <E>                   │     和 CanalEventDownStreamHandler  │
│  └────────┬─────────────┘                                    │
│           │                                                   │
│           v                                                   │
│  ┌──────────────────────┐        ┌──────────────────────┐   │
│  │ EntryEventSink       │        │ GroupEventSink        │   │
│  │ (单实例Sink)          │        │ (多源归并Sink)         │   │
│  │                      │        │ + TimelineBarrier     │   │
│  │  sink()              │        │ + TimelineTxBarrier   │   │
│  │  └─ sinkData()       │        └──────────────────────┘   │
│  │       ├─ doFilter()  │                                    │
│  │       └─ doSink()    │                                    │
│  │            ├─ before │                                    │
│  │            ├─ tryPut │                                    │
│  │            └─ after  │                                    │
│  └──────────────────────┘                                    │
│           │                                                   │
│           v                                                   │
│  ┌──────────────────────┐                                    │
│  │ HeartBeatEntryEvent  │                                    │
│  │ Handler              │  ← Handler 责任链中的一个节点        │
│  │ (心跳事件处理)        │                                    │
│  └──────────────────────┘                                    │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 CanalEventSink 接口

```java
package com.alibaba.otter.canal.sink;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

/**
 * canal sink接口
 * 
 * @author jianghang 2012-7-23 下午12:28:18
 * @version 1.0.0
 */
public interface CanalEventSink<E> {

    /**
     * 处理数据，返回是否成功
     */
    boolean sink(E event);

    /**
     * 处理数据，带超时
     */
    boolean sink(E event, long timeout) throws InterruptedException;

    /**
     * 中断处理（主备切换等场景）
     */
    void interrupt();

    /**
     * 启动
     */
    void start();

    /**
     * 停止
     */
    void stop();
}
```

**这一步在干什么？**

`CanalEventSink` 定义了 Sink 模块的顶层接口。泛型 `E` 在实际使用中为 `List<Event>`。接口定义了五个方法：
- `sink()`：处理一批事件，阻塞和非阻塞两个版本
- `interrupt()`：中断处理，用于主备切换等场景
- `start()`/`stop()`：生命周期管理

### 2.3 AbstractCanalEventSink 抽象基类

```java
package com.alibaba.otter.canal.sink;

import java.util.List;

import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * sink抽象实现
 * 
 * @author jianghang 2012-7-23 下午12:31:14
 * @version 1.0.0
 */
public abstract class AbstractCanalEventSink<E> implements CanalEventSink<E> {

    protected CanalEventStore<Event> eventStore;        // 关联的Store
    protected CanalEventDownStreamHandler handler;       // 下游处理器（责任链）
    protected volatile boolean running;                  // 运行状态标志

    public AbstractCanalEventSink(){
        this(null);
    }

    public AbstractCanalEventSink(CanalEventDownStreamHandler handler){
        this.handler = handler;
    }

    public void setEventStore(CanalEventStore<Event> eventStore){
        this.eventStore = eventStore;
    }

    public void start(){
        if (running) {
            return;
        }
        running = true;
    }

    public void stop(){
        if (!running) {
            return;
        }
        running = false;
    }

    public void interrupt(){
        // 默认空实现，子类可覆盖
    }

    public CanalEventStore<Event> getEventStore(){
        return eventStore;
    }

    public void setHandler(CanalEventDownStreamHandler handler){
        this.handler = handler;
    }

    public CanalEventDownStreamHandler getHandler(){
        return handler;
    }
}
```

**这一步在干什么？**

`AbstractCanalEventSink` 提供了 Sink 的骨架实现：
1. 持有 `eventStore` 引用 —— 这是 Sink 投递数据的目标
2. 持有 `handler` —— `CanalEventDownStreamHandler`，用于在投递前后做增强处理
3. 提供 `running` 状态标志和 `start()`/`stop()` 生命周期管理
4. `interrupt()` 默认空实现，由子类按需覆盖

### 2.4 CanalEventDownStreamHandler 责任链接口

```java
package com.alibaba.otter.canal.sink;

import java.util.List;

import com.alibaba.otter.canal.store.model.Event;

/**
 * 下游事件处理器，用于在sink投递前后做增强处理
 * 
 * @author jianghang 2012-9-14 上午10:11:54
 * @version 1.0.0
 */
public interface CanalEventDownStreamHandler {

    /**
     * 在sink之前做处理
     */
    List<Event> before(List<Event> events);

    /**
     * 在sink之后做处理
     */
    void after(List<Event> events);

    /**
     * sink失败时的重试处理
     */
    List<Event> retry(List<Event> events);
}
```

对应的抽象实现：

```java
package com.alibaba.otter.canal.sink;

import java.util.List;

import com.alibaba.otter.canal.store.model.Event;

/**
 * 下游事件处理器抽象实现
 */
public abstract class AbstractCanalEventDownStreamHandler implements CanalEventDownStreamHandler {

    @Override
    public List<Event> before(List<Event> events) {
        return events;
    }

    @Override
    public void after(List<Event> events) {
        // 默认空实现
    }

    @Override
    public List<Event> retry(List<Event> events) {
        return events;
    }
}
```

**这一步在干什么？**

`CanalEventDownStreamHandler` 定义了 Sink 投递过程中的三个切面：
1. `before()`：在投递 Store 之前调用，可以对事件列表做预处理（如过滤心跳）
2. `after()`：投递成功后调用，用于后置处理
3. `retry()`：投递失败重试时调用，可以做恢复处理

`AbstractCanalEventDownStreamHandler` 提供了默认实现：`before()` 直接返回原列表，`after()` 和 `retry()` 空操作。子类按需覆盖。

### 2.5 EntryEventSink —— 核心 Sink 实现

`EntryEventSink` 是 Sink 模块中最核心的类，它实现了完整的过滤和投递逻辑。

#### 2.5.1 完整源码

```java
package com.alibaba.otter.canal.sink.entry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.exception.CanalStoreException;

/**
 * 处理Entry的sink实现
 * 
 * @author jianghang 2012-7-23 下午01:04:36
 * @version 1.0.0
 */
public class EntryEventSink extends AbstractCanalEventSink<List<Event>> {

    private static final Logger            logger              = LoggerFactory.getLogger(EntryEventSink.class);
    private static final int               START_DETECTING_CAPACITY = 1;
    private static final int               WAIT_CYCLE          = 10;
    private static final long              WAIT_NANO           = 1000000L;  // 1ms in nanos

    private CanalEventFilter<CanalEntry.Entry> filter;              // 数据过滤器
    private boolean                        filterTransactionEntry = true;    // 是否过滤空事务
    private AtomicLong                     eventsSinkBlockingTime = new AtomicLong(); // sink阻塞时间统计

    public EntryEventSink(){
        this.handler = new HeartBeatEntryEventHandler(); // 默认使用心跳处理器
    }

    public EntryEventSink(CanalEventDownStreamHandler handler){
        super(handler);
    }

    public void start(){
        super.start();
    }

    public void stop(){
        super.stop();
    }

    /**
     * sink入口方法
     * 
     * @param events 待处理的事件列表
     * @return 是否成功
     */
    public boolean sink(List<Event> events) throws CanalSinkException {
        return sinkData(events);
    }

    /**
     * 核心处理逻辑
     */
    private boolean sinkData(List<Event> events) throws CanalSinkException {
        // 1. 过滤
        List<Event> filteredEvents = doFilter(events);
        if (filteredEvents == null || filteredEvents.isEmpty()) {
            return true; // 过滤后无数据，直接返回成功
        }

        // 2. 投递Store
        return doSink(filteredEvents);
    }

    /**
     * 过滤逻辑
     */
    private List<Event> doFilter(List<Event> events) {
        if (filter == null) {
            // 没有配置过滤器，不做过滤
            return events;
        }

        List<Event> result = new ArrayList<>();
        for (Event event : events) {
            Entry entry = event.getEntry();
            if (entry.getEntryType() == EntryType.ROWDATA) {
                // 只对ROWDATA类型做正则过滤
                try {
                    if (filter.filter(entry)) {
                        result.add(event);
                    }
                } catch (Exception e) {
                    throw new CanalSinkException(e);
                }
            } else {
                // 非ROWDATA类型（如事务头尾、心跳）直接保留
                result.add(event);
            }
        }

        // 空事务过滤
        if (filterTransactionEntry) {
            return filterTransactionEntry(result);
        } else {
            return filterEmtryTransactionEntry(result);
        }
    }

    /**
     * 过滤空事务 —— 第一级过滤策略
     * 
     * 如果一个事务中没有ROWDATA（只有事务头和事务尾），则过滤掉
     */
    private List<Event> filterTransactionEntry(List<Event> events) {
        if (events == null || events.isEmpty()) {
            return events;
        }

        List<Event> result = new ArrayList<>();
        boolean hasRowData = false;
        for (Event event : events) {
            EntryType entryType = event.getEntry().getEntryType();
            if (entryType == EntryType.TRANSACTIONEND) {
                // 事务结束
                if (hasRowData) {
                    result.add(event);
                }
                hasRowData = false; // 重置，准备下一个事务
            } else if (entryType == EntryType.ROWDATA) {
                hasRowData = true;
                result.add(event);
            } else {
                // TRANSACTIONBEGIN, HEARTBEAT等
                result.add(event);
            }
        }

        return result;
    }

    /**
     * 过滤空事务 —— 第二级过滤策略（保留事务结构）
     * 
     * 保留事务头和事务尾，但如果事务中没有ROWDATA，
     * 则只保留事务尾，过滤掉事务头
     */
    private List<Event> filterEmtryTransactionEntry(List<Event> events) {
        if (events == null || events.isEmpty()) {
            return events;
        }

        List<Event> result = new ArrayList<>();
        // 用于记录当前事务中是否有ROWDATA
        boolean hasRowData = false;
        // 用于暂存事务头
        Event transactionBegin = null;
        for (Event event : events) {
            EntryType entryType = event.getEntry().getEntryType();
            switch (entryType) {
                case TRANSACTIONBEGIN:
                    transactionBegin = event;
                    hasRowData = false;
                    break;
                case ROWDATA:
                    hasRowData = true;
                    if (transactionBegin != null) {
                        result.add(transactionBegin);
                        transactionBegin = null;
                    }
                    result.add(event);
                    break;
                case TRANSACTIONEND:
                    if (hasRowData) {
                        result.add(event);
                    } else {
                        // 空事务，只保留事务尾
                        result.add(event);
                    }
                    if (transactionBegin != null) {
                        // 如果事务头还没被添加，说明事务中没有ROWDATA
                        // 此时丢弃事务头
                        transactionBegin = null;
                    }
                    break;
                default:
                    result.add(event);
                    break;
            }
        }

        return result;
    }

    /**
     * 投递Store
     */
    private boolean doSink(List<Event> events) {
        // 1. before处理
        List<Event> processedEvents = handler.before(events);
        if (processedEvents == null || processedEvents.isEmpty()) {
            return true;
        }

        // 2. 自旋重试写入Store
        long blockingStart = System.nanoTime();
        try {
            // 自旋等待
            applyWait(processedEvents);

            // 3. tryPut / put
            boolean success = false;
            if (eventStore instanceof MemoryEventStoreWithBuffer) {
                // 先尝试tryPut（非阻塞）
                success = eventStore.tryPut(processedEvents);
            }
            if (!success) {
                // tryPut失败，使用阻塞put
                success = eventStore.put(processedEvents);
            }
            return success;
        } finally {
            long blockingTime = System.nanoTime() - blockingStart;
            eventsSinkBlockingTime.addAndGet(blockingTime);
            // 4. after处理
            handler.after(processedEvents);
        }
    }

    /**
     * 自旋等待 + 渐进式退避
     */
    private void applyWait(List<Event> events) {
        int retries = 0;
        while (true) {
            if (eventStore instanceof MemoryEventStoreWithBuffer) {
                MemoryEventStoreWithBuffer store = (MemoryEventStoreWithBuffer) eventStore;
                if (store.isRaw()) {
                    break; // 有空间，不需要等待
                }
            }

            if (retries <= START_DETECTING_CAPACITY) {
                // 前1次：直接yield
                Thread.yield();
            } else if (retries <= WAIT_CYCLE) {
                // 2~10次：parkNanos递增
                LockSupport.parkNanos(WAIT_NANO * retries);
            } else {
                // 超过10次：封顶10ms
                LockSupport.parkNanos(WAIT_NANO * WAIT_CYCLE);
            }

            retries++;

            // 安全检查
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    // getter/setter
    public CanalEventFilter<CanalEntry.Entry> getFilter() {
        return filter;
    }

    public void setFilter(CanalEventFilter<CanalEntry.Entry> filter) {
        this.filter = filter;
    }

    public boolean isFilterTransactionEntry() {
        return filterTransactionEntry;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public AtomicLong getEventsSinkBlockingTime() {
        return eventsSinkBlockingTime;
    }

    public void setEventsSinkBlockingTime(AtomicLong eventsSinkBlockingTime) {
        this.eventsSinkBlockingTime = eventsSinkBlockingTime;
    }
}
```

> **注意**：以上是 EntryEventSink 的核心逻辑结构展示。实际源码中，`doFilter` 的实现更加精细，包含了 issue #2616 的修复逻辑。下面我们逐方法详细解读。

#### 2.5.2 sink() 入口方法

```java
public boolean sink(List<Event> events) throws CanalSinkException {
    return sinkData(events);
}
```

**这一步在干什么？**

`sink()` 是 Sink 模块的入口方法。Parser 线程解析完一批 Binlog 事件后，将 `List<Event>` 交给 `sink()` 处理。这里直接委托给 `sinkData()`，体现了**模板方法**的思路——入口方法固定，核心逻辑在 `sinkData()` 中。

#### 2.5.3 sinkData() 的完整源码和逐行解读

```java
private boolean sinkData(List<Event> events) throws CanalSinkException {
    // 1. 过滤
    List<Event> filteredEvents = doFilter(events);
    if (filteredEvents == null || filteredEvents.isEmpty()) {
        return true; // 过滤后无数据，直接返回成功
    }

    // 2. 投递Store
    return doSink(filteredEvents);
}
```

**这一步在干什么？**

`sinkData()` 是 Sink 模块的核心方法，分为两个阶段：

1. **doFilter**：对事件列表进行过滤。先通过 AviaterRegexFilter 过滤掉不需要的 ROWDATA，再根据 `filterTransactionEntry` 配置执行空事务过滤策略。
2. **doSink**：将过滤后的事件列表通过 Handler 责任链增强后，投递到 Store 的 RingBuffer 中。

两个阶段的顺序很重要：**先过滤再投递**，避免将不需要的事件写入 Store 浪费 RingBuffer 空间。

#### 2.5.4 doFilter() 过滤逻辑 —— 只过滤 ROWDATA 类型

```java
private List<Event> doFilter(List<Event> events) {
    if (filter == null) {
        return events; // 没有配置过滤器，直接返回
    }

    List<Event> result = new ArrayList<>();
    for (Event event : events) {
        Entry entry = event.getEntry();
        if (entry.getEntryType() == EntryType.ROWDATA) {
            // 只对ROWDATA类型做正则过滤
            try {
                if (filter.filter(entry)) {
                    result.add(event);
                }
            } catch (Exception e) {
                throw new CanalSinkException(e);
            }
        } else {
            // 非ROWDATA类型（如事务头尾、心跳）直接保留
            result.add(event);
        }
    }

    // 空事务过滤
    if (filterTransactionEntry) {
        return filterTransactionEntry(result);
    } else {
        return filterEmtryTransactionEntry(result);
    }
}
```

**这一步在干什么？**

`doFilter()` 方法分两个层次进行过滤：

**第一层：ROWDATA 正则过滤**

只有 `EntryType.ROWDATA`（即 DML 数据变更记录）才会被正则过滤器处理。其他类型的事件（`TRANSACTIONBEGIN`、`TRANSACTIONEND`、`HEARTBEAT`）直接保留。

这是因为正则过滤的配置是 `schema.table` 格式，只对 DML 有意义。事务控制事件和心跳事件不属于任何表，不应被表级正则过滤。

**第二层：空事务过滤策略**

根据 `filterTransactionEntry` 配置选择两种策略：
- `true`：`filterTransactionEntry()` —— 过滤掉没有 ROWDATA 的空事务
- `false`：`filterEmtryTransactionEntry()` —— 保留事务结构，但做更精细的处理

过滤流程图：

```
events (原始事件列表)
    │
    ├── TRANSACTIONBEGIN → 保留（非ROWDATA）
    ├── ROWDATA(foo.bar) → filter.filter() → true → 保留
    ├── ROWDATA(foo.baz) → filter.filter() → false → 丢弃
    ├── TRANSACTIONEND   → 保留（非ROWDATA）
    ├── HEARTBEAT        → 保留（非ROWDATA）
    │
    v
第一层过滤后: [TRANSACTIONBEGIN, ROWDATA(foo.bar), TRANSACTIONEND, HEARTBEAT]
    │
    v
第二层过滤（filterTransactionEntry=true）:
    filterTransactionEntry() → 过滤掉没有ROWDATA的事务
    │
    v
最终结果: [ROWDATA(foo.bar), HEARTBEAT]
```

#### 2.5.5 空事务双级过滤策略

**第一级：filterTransactionEntry**

```java
private List<Event> filterTransactionEntry(List<Event> events) {
    if (events == null || events.isEmpty()) {
        return events;
    }

    List<Event> result = new ArrayList<>();
    boolean hasRowData = false;
    for (Event event : events) {
        EntryType entryType = event.getEntry().getEntryType();
        if (entryType == EntryType.TRANSACTIONEND) {
            // 事务结束
            if (hasRowData) {
                result.add(event);
            }
            hasRowData = false; // 重置，准备下一个事务
        } else if (entryType == EntryType.ROWDATA) {
            hasRowData = true;
            result.add(event);
        } else {
            // TRANSACTIONBEGIN, HEARTBEAT等
            result.add(event);
        }
    }

    return result;
}
```

**这一步在干什么？**

`filterTransactionEntry()` 是第一种空事务过滤策略（`filterTransactionEntry=true` 时使用）。

逻辑：遍历事件列表，跟踪每个事务是否有 ROWDATA：
- 遇到 `TRANSACTIONEND` 时，只有当 `hasRowData=true`（事务中有数据变更）时才保留事务尾
- 遇到 `TRANSACTIONBEGIN` 和 `HEARTBEAT` 时直接保留
- 遇到 `ROWDATA` 时设置 `hasRowData=true` 并保留

**关键设计：为什么只有 TRANSACTIONEND 才重置计数？**

这是 issue #2616 的修复。在修复之前，`TRANSACTIONBEGIN` 也会重置 `hasRowData`，导致一个问题：

```
场景：主备切换后，从备库拿到的第一个事件是 TRANSACTIONEND（而非 TRANSACTIONBEGIN）

修复前的行为:
  TRANSACTIONEND → hasRowData=false → 丢弃事务尾（错误！）
  ROWDATA → hasRowData=true → 保留
  TRANSACTIONEND → hasRowData=true → 保留事务尾

修复后的行为:
  TRANSACTIONEND → if(hasRowData) 保留 → hasRowData=false（重置）
  ROWDATA → hasRowData=true → 保留
  TRANSACTIONEND → hasRowData=true → 保留事务尾
```

如果 `TRANSACTIONBEGIN` 也重置 `hasRowData`，那么在事务头之后、事务尾之前如果出现 ROWDATA，`hasRowData` 会被正确设置。但如果没有 `TRANSACTIONBEGIN`（主备切换场景），`TRANSACTIONEND` 重置 `hasRowData` 就能正确处理跨事务边界。

修复后只在 `TRANSACTIONEND` 重置，确保事务边界的完整性不被 `TRANSACTIONBEGIN` 的缺失打乱。

**第二级：filterEmtryTransactionEntry**

```java
private List<Event> filterEmtryTransactionEntry(List<Event> events) {
    if (events == null || events.isEmpty()) {
        return events;
    }

    List<Event> result = new ArrayList<>();
    boolean hasRowData = false;
    Event transactionBegin = null;
    for (Event event : events) {
        EntryType entryType = event.getEntry().getEntryType();
        switch (entryType) {
            case TRANSACTIONBEGIN:
                transactionBegin = event;
                hasRowData = false;
                break;
            case ROWDATA:
                hasRowData = true;
                if (transactionBegin != null) {
                    result.add(transactionBegin);
                    transactionBegin = null;
                }
                result.add(event);
                break;
            case TRANSACTIONEND:
                if (hasRowData) {
                    result.add(event);
                } else {
                    // 空事务，只保留事务尾
                    result.add(event);
                }
                if (transactionBegin != null) {
                    // 事务头还没被添加，说明事务中没有ROWDATA
                    // 此时丢弃事务头
                    transactionBegin = null;
                }
                break;
            default:
                result.add(event);
                break;
        }
    }

    return result;
}
```

**这一步在干什么？**

`filterEmtryTransactionEntry()` 是第二种空事务过滤策略（`filterTransactionEntry=false` 时使用）。

与第一级策略的区别：**保留事务结构**，即保留事务头和事务尾，但对于没有 ROWDATA 的空事务，只保留事务尾、丢弃事务头。

逻辑：
1. 遇到 `TRANSACTIONBEGIN`：暂存到 `transactionBegin`，不立即加入结果
2. 遇到 `ROWDATA`：如果暂存了事务头，先加入结果再加入 ROWDATA。这保证了只有事务中有数据时才保留事务头
3. 遇到 `TRANSACTIONEND`：无论事务是否为空，都保留事务尾。如果事务头还没被加入（说明事务中没有ROWDATA），则丢弃事务头

**hasRowData / hasHeartBeat 分支处理**

在实际的 EntryEventSink 源码中，`doFilter` 方法还处理了心跳事件的特殊情况。当一批事件中只有心跳而没有 ROWDATA 时，心跳事件应该被保留以维持客户端的消费连接。这个逻辑在 `HeartBeatEntryEventHandler` 中进一步处理。

两种策略的对比：

| 特性 | filterTransactionEntry (第一级) | filterEmtryTransactionEntry (第二级) |
|------|------|------|
| 事务头(BEGIN) | 全部保留 | 只有事务有ROWDATA时才保留 |
| 事务尾(END) | 只有事务有ROWDATA时才保留 | 全部保留 |
| ROWDATA | 全部保留 | 全部保留 |
| HEARTBEAT | 全部保留 | 全部保留 |
| 配置 | filterTransactionEntry=true | filterTransactionEntry=false |
| 适用场景 | 不需要事务边界的场景 | 需要事务完整性保证的场景 |

#### 2.5.6 doSink() 投递 Store 的完整实现

```java
private boolean doSink(List<Event> events) {
    // 1. before处理
    List<Event> processedEvents = handler.before(events);
    if (processedEvents == null || processedEvents.isEmpty()) {
        return true;
    }

    // 2. 自旋重试写入Store
    long blockingStart = System.nanoTime();
    try {
        // 自旋等待
        applyWait(processedEvents);

        // 3. tryPut / put
        boolean success = false;
        if (eventStore instanceof MemoryEventStoreWithBuffer) {
            success = eventStore.tryPut(processedEvents);
        }
        if (!success) {
            success = eventStore.put(processedEvents);
        }
        return success;
    } finally {
        long blockingTime = System.nanoTime() - blockingStart;
        eventsSinkBlockingTime.addAndGet(blockingTime);
        // 4. after处理
        handler.after(processedEvents);
    }
}
```

**这一步在干什么？**

`doSink()` 是数据从 Sink 流向 Store 的关键方法，实现了 Handler 责任链三切面和自旋重试策略：

**Handler 责任链三切面：before → tryPut → after / retry**

```
doSink(events)
    │
    v
handler.before(events)     ← 切面1：前置处理（如心跳过滤）
    │
    v
processedEvents (可能被修改)
    │
    v
applyWait(processedEvents)  ← 自旋等待Store有空间
    │
    v
eventStore.tryPut()         ← 切面2：非阻塞写入尝试
    │
    ├─ 成功 → return true
    │
    └─ 失败
        │
        v
    eventStore.put()          ← 切面2：阻塞写入
        │
        v
    handler.after(events)    ← 切面3：后置处理
    │
    v
return success
```

**自旋重试 + 渐进式等待（applyWait）**

```java
private void applyWait(List<Event> events) {
    int retries = 0;
    while (true) {
        if (eventStore instanceof MemoryEventStoreWithBuffer) {
            MemoryEventStoreWithBuffer store = (MemoryEventStoreWithBuffer) eventStore;
            if (store.isRaw()) {
                break; // 有空间，不需要等待
            }
        }

        if (retries <= START_DETECTING_CAPACITY) {
            // 前1次：直接yield
            Thread.yield();
        } else if (retries <= WAIT_CYCLE) {
            // 2~10次：parkNanos递增
            LockSupport.parkNanos(WAIT_NANO * retries);
        } else {
            // 超过10次：封顶10ms
            LockSupport.parkNanos(WAIT_NANO * WAIT_CYCLE);
        }

        retries++;

        if (Thread.currentThread().isInterrupted()) {
            Thread.currentThread().interrupt();
            break;
        }
    }
}
```

**这一步在干什么？**

`applyWait()` 实现了渐进式退避策略，在 Store RingBuffer 满时等待空间释放：

```
重试次数    策略              等待时间
───────────────────────────────────────
0           Thread.yield()    ~0 (让出CPU时间片)
1           Thread.yield()    ~0
2           parkNanos(2ms)    2ms
3           parkNanos(3ms)    3ms
4           parkNanos(4ms)    4ms
5           parkNanos(5ms)    5ms
6           parkNanos(6ms)    6ms
7           parkNanos(7ms)    7ms
8           parkNanos(8ms)    8ms
9           parkNanos(9ms)    9ms
10          parkNanos(10ms)   10ms
11+         parkNanos(10ms)   10ms (封顶)
```

渐进式退避的设计意图：
1. **前1次 yield**：Store 可能只是瞬时满，yield 一次后消费者可能就读完了。避免过早进入 park。
2. **2~10次递增 parkNanos**：如果 yield 后仍然满，说明消费速度跟不上，逐步增加等待时间，给消费者更多时间。
3. **封顶 10ms**：避免等待时间无限增长，10ms 是一个合理的上限——既不频繁抢占 CPU，也不至于等待太久。

**阻塞时间统计（eventsSinkBlockingTime）**

```java
long blockingStart = System.nanoTime();
try {
    applyWait(processedEvents);
    // ... put逻辑
} finally {
    long blockingTime = System.nanoTime() - blockingStart;
    eventsSinkBlockingTime.addAndGet(blockingTime);
    handler.after(processedEvents);
}
```

`eventsSinkBlockingTime` 是一个 `AtomicLong`，统计了 Sink 在 `applyWait` + `put` 过程中的总阻塞时间。这个指标用于监控 Store 的消费速度是否跟得上生产速度。如果阻塞时间持续增长，说明 RingBuffer 频繁满了，消费者需要扩容或优化。

### 2.6 HeartBeatEntryEventHandler

```java
package com.alibaba.otter.canal.sink.entry;

import java.util.List;

import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 心跳事件处理器
 * 
 * @author jianghang 2012-9-14 上午10:15:16
 * @version 1.0.0
 */
public class HeartBeatEntryEventHandler extends AbstractCanalEventDownStreamHandler {

    @Override
    public List<Event> before(List<Event> events) {
        boolean existHeartBeat = false;
        for (Event event : events) {
            if (event.getEntryType() == EntryType.HEARTBEAT) {
                existHeartBeat = true;
                break;
            }
        }

        if (existHeartBeat) {
            // 过滤掉非心跳事件
            List<Event> result = new ArrayList<>();
            for (Event event : events) {
                if (event.getEntryType() == EntryType.HEARTBEAT) {
                    result.add(event);
                }
            }
            return result;
        }

        return events;
    }
}
```

**这一步在干什么？**

`HeartBeatEntryEventHandler` 是默认的 `CanalEventDownStreamHandler` 实现，在 `doSink()` 的 `before()` 切面中执行。

核心逻辑：**如果一批事件中包含心跳事件，则只保留心跳事件，过滤掉其他所有事件**。

这是一个防御性检查，确保心跳事件不被淹没在大量数据事件中。心跳事件的作用是维持客户端与 Canal Server 的连接活跃度，当 MySQL 没有 Binlog 变更时，Canal 会定期发送心跳事件。如果心跳和数据事件混在一起投递到 Store，客户端可能在消费完大量数据事件后才能看到心跳，导致心跳检测超时。

处理流程：

```
events = [ROWDATA, ROWDATA, HEARTBEAT, ROWDATA]
                │
                v
before() 检测到 existHeartBeat=true
                │
                v
过滤后: [HEARTBEAT]
                │
                v
只有心跳事件被投递到Store
```

### 2.7 GroupEventSink —— 多源归并

当 Canal 需要合并多个 MySQL 实例的数据时（如分库分表场景），使用 `GroupEventSink` 替代 `EntryEventSink`。`GroupEventSink` 继承自 `EntryEventSink`，在 `doSink()` 中增加了**时间线归并排序**逻辑。

#### 2.7.1 完整源码

```java
package com.alibaba.otter.canal.sink.entry.group;

import java.util.Arrays;
import java.util.List;

import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 基于归并排序的sink处理
 * 
 * <pre>
 * 几点设计说明：
 * 1. 多库合并时，需要控制不满足groupSize的条件，就会阻塞其他库的合并操作.
 *    (比如刚启动时会所有通道正常工作才开始合并，或者中间过程出现主备切换)
 * 2. 库解析出现问题，但没有进行主备切换，此时需要通过{@linkplain CanalEventDownStreamHandler}
 *    进行定时监听合并数据的产生时间间隔
 *    a. 因为一旦库解析异常，就不会再sink数据，此时groupSize就会一直缺少，就会阻塞其他库的合并，
 *       也就是不会有数据写入到store中
 * </pre>
 * 
 * @author jianghang 2012-10-15 下午09:54:18
 * @version 1.0.0
 */
public class GroupEventSink extends EntryEventSink {

    private int          groupSize;
    private GroupBarrier barrier;  // 归并排序需要预先知道组的大小

    public GroupEventSink(){
        this(1);
    }

    public GroupEventSink(int groupSize){
        super();
        this.groupSize = groupSize;
    }

    public void start() {
        super.start();

        if (filterTransactionEntry) {
            barrier = new TimelineBarrier(groupSize);
        } else {
            barrier = new TimelineTransactionBarrier(groupSize); // 支持事务保留
        }
    }

    protected boolean doSink(List<Event> events) {
        int size = events.size();
        for (int i = 0; i < events.size(); i++) {
            Event event = events.get(i);
            try {
                barrier.await(event); // 进行timeline的归并调度处理
                if (filterTransactionEntry) {
                    super.doSink(Arrays.asList(event));
                } else if (i == size - 1) {
                    // 针对事务数据，只有到最后一条数据都通过后，才进行sink操作，保证原子性
                    // 同时批量sink，也要保证在最后一条数据释放状态之前写出数据，否则就有并发问题
                    return super.doSink(events);
                }
            } catch (InterruptedException e) {
                return false;
            } finally {
                barrier.clear(event);
            }
        }

        return false;
    }

    public void interrupt() {
        super.interrupt();
        barrier.interrupt();
    }
}
```

#### 2.7.2 doSink() 重写 —— barrier.await → 事务原子写入 → barrier.clear

**这一步在干什么？**

`GroupEventSink.doSink()` 重写了父类的 `doSink()` 方法，核心变化是引入了 `GroupBarrier` 进行时间线归并排序：

```
doSink(events) [事件列表: BEGIN, ROWDATA1, ROWDATA2, END]
    │
    ├── event = BEGIN
    │   ├── barrier.await(BEGIN)    ← 等待时间线归并调度
    │   ├── filterTransactionEntry=false → 跳过（非最后一条）
    │   └── barrier.clear(BEGIN)    ← 清理barrier状态
    │
    ├── event = ROWDATA1
    │   ├── barrier.await(ROWDATA1)
    │   ├── 非最后一条 → 跳过
    │   └── barrier.clear(ROWDATA1)
    │
    ├── event = ROWDATA2
    │   ├── barrier.await(ROWDATA2)
    │   ├── 非最后一条 → 跳过
    │   └── barrier.clear(ROWDATA2)
    │
    └── event = END (最后一条)
        ├── barrier.await(END)      ← 最后一条通过barrier
        ├── i == size-1 → true
        │   └── super.doSink(events)  ← 整批写入Store（原子性）
        └── barrier.clear(END)
```

**关键设计：事务原子写入**

当 `filterTransactionEntry=false`（保留事务结构）时，`doSink()` 不是逐条投递，而是**等到最后一条事件通过 barrier 后，整批一次性调用 `super.doSink(events)`**。

这保证了事务的原子性——要么整个事务都写入 Store，要么都不写。如果逐条投递，可能出现事务头写入后、事务尾写入前发生异常，导致 Store 中存在不完整事务。

**关键设计：barrier.clear 的 finally 块**

`barrier.clear(event)` 放在 `finally` 块中，确保即使 `await` 或 `doSink` 抛出异常，barrier 状态也能被正确清理。这是防止死锁的关键——如果 `await` 后异常但没 `clear`，其他通道的线程会永远等待。

### 2.8 GroupBarrier 接口

```java
package com.alibaba.otter.canal.sink.entry.group;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 针对group合并的barrier接口，控制多个sink操作的合并处理
 * 
 * @author jianghang 2012-10-18 下午05:07:35
 * @version 1.0.0
 */
public interface GroupBarrier<T> {

    /**
     * 判断当前的数据对象是否允许通过
     */
    public void await(T event) throws InterruptedException;

    /**
     * 判断当前的数据对象是否允许通过，带超时控制
     */
    public void await(T event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * sink成功，清理对应barrier的状态
     */
    public void clear(T event);

    /**
     * 出现切换，发起interrupt，清理对应的上下文
     */
    public void interrupt();
}
```

**这一步在干什么？**

`GroupBarrier` 定义了多源归并排序的屏障接口，包含四个方法：
- `await()`：阻塞等待直到当前事件被允许通过（归并排序到最小时间戳）
- `await(timeout)`：带超时版本
- `clear()`：事件处理完成后清理 barrier 状态
- `interrupt()`：主备切换等场景下中断所有等待

### 2.9 TimelineBarrier —— 时间线归并排序

#### 2.9.1 完整源码

```java
package com.alibaba.otter.canal.sink.entry.group;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.otter.canal.store.model.Event;

/**
 * 时间归并控制
 * 
 * <pre>
 * 大致设计：
 *  1. 多个队列都提交一个timestamp，判断出最小的一个timestamp做为通过的条件，
 *     然后唤醒<=该最小时间的线程通过
 *  2. 只有当多个队列都提交了一个timestamp，缺少任何一个提交，都会阻塞其他队列通过。
 *     (解决当一个库启动过慢或者发生主备切换时出现延迟等问题)
 * 
 * 存在一个假定，认为提交的timestamp是一个顺序递增，但是在两种case下会出现时间回退
 * a. 大事务时，事务头的时间会晚于事务当中数据的时间，相当于出现一个时间回退
 * b. 出现主备切换，从备机上发过来的数据会回退几秒钟
 * </pre>
 * 
 * @author jianghang 2012-10-15 下午10:01:53
 * @version 1.0.0
 */
public class TimelineBarrier implements GroupBarrier<Event> {

    protected int                 groupSize;
    protected ReentrantLock       lock           = new ReentrantLock();
    protected Condition           condition      = lock.newCondition();
    protected volatile long       threshold;
    protected BlockingQueue<Long> lastTimestamps = new PriorityBlockingQueue<>();

    public TimelineBarrier(int groupSize){
        this.groupSize = groupSize;
        threshold = Long.MIN_VALUE;
    }

    public void await(Event event) throws InterruptedException {
        long timestamp = getTimestamp(event);
        try {
            lock.lockInterruptibly();
            single(timestamp);
            while (isPermit(event, timestamp) == false) {
                condition.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public void await(Event event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        long timestamp = getTimestamp(event);
        try {
            lock.lockInterruptibly();
            single(timestamp);
            while (isPermit(event, timestamp) == false) {
                condition.await(timeout, unit);
            }
        } finally {
            lock.unlock();
        }
    }

    public void clear(Event event) {
        lastTimestamps.remove(getTimestamp(event));
    }

    public void interrupt() {
        // do nothing
    }

    public long state() {
        return threshold;
    }

    protected boolean isPermit(Event event, long state) {
        return state <= state();
    }

    protected void notify(long minTimestamp) {
        condition.signalAll();
    }

    private void single(long timestamp) throws InterruptedException {
        lastTimestamps.add(timestamp);

        if (timestamp < state()) {
            threshold = timestamp;
        }

        if (lastTimestamps.size() >= groupSize) {
            Long minTimestamp = this.lastTimestamps.peek();
            if (minTimestamp != null) {
                threshold = minTimestamp;
                notify(minTimestamp);
            }
        } else {
            threshold = Long.MIN_VALUE;
        }
    }

    private Long getTimestamp(Event event) {
        return event.getExecuteTime();
    }
}
```

#### 2.9.2 PriorityBlockingQueue 实现多源最小时间选择

**这一步在干什么？**

`TimelineBarrier` 使用 `PriorityBlockingQueue<Long>` 来收集所有通道（group 中的每个 MySQL 实例对应一个通道）提交的时间戳。`PriorityBlockingQueue` 是一个最小堆，`peek()` 返回最小值。

归并排序的核心思路：

```
假设有3个MySQL实例（groupSize=3），各通道提交的时间戳：

通道1: timestamp=100  →  lastTimestamps = [100]
通道2: timestamp=200  →  lastTimestamps = [100, 200]
通道3: timestamp=150  →  lastTimestamps = [100, 150, 200]

当 lastTimestamps.size() >= groupSize (3):
  minTimestamp = peek() = 100
  threshold = 100
  notify → signalAll

各通道检查 isPermit:
  通道1: 100 <= 100 → true → 通过
  通道2: 200 <= 100 → false → 继续等待
  通道3: 150 <= 100 → false → 继续等待

通道1处理完成后 clear:
  lastTimestamps.remove(100) → [150, 200]
  size=2 < groupSize=3 → threshold = Long.MIN_VALUE
  → 通道2、3重新阻塞

通道1提交下一个时间戳:
  通道1: timestamp=180 → lastTimestamps = [150, 180, 200]
  size=3 >= groupSize → threshold = peek() = 150
  → 通道3通过（150 <= 150）
```

#### 2.9.3 single() 方法 —— groupSize 触发条件、时间回退处理、threshold 更新

```java
private void single(long timestamp) throws InterruptedException {
    lastTimestamps.add(timestamp);

    if (timestamp < state()) {
        // 时间回退处理
        threshold = timestamp;
    }

    if (lastTimestamps.size() >= groupSize) {
        Long minTimestamp = this.lastTimestamps.peek();
        if (minTimestamp != null) {
            threshold = minTimestamp;
            notify(minTimestamp);
        }
    } else {
        threshold = Long.MIN_VALUE;
    }
}
```

**这一步在干什么？**

`single()` 是 `TimelineBarrier` 的核心方法，处理三个关键逻辑：

**1. groupSize 触发条件**

只有当 `lastTimestamps.size() >= groupSize` 时，才会设置 `threshold` 并唤醒等待线程。这意味着**所有通道都必须提交了至少一个时间戳**，归并排序才能开始。

设计意图：防止某个通道启动慢或主备切换时，其他通道的数据"抢跑"。比如 3 个通道只有 2 个在运行，如果允许通过，可能导致时间顺序错乱——未启动的通道可能有更早时间戳的数据。

**2. 时间回退处理**

```java
if (timestamp < state()) {
    threshold = timestamp;
}
```

时间回退发生在两种场景（源码注释中提到）：

```
场景a：大事务时间跳跃
  事务头: 2012-08-08 16:24:26  (executeTime)
  变更1:  2012-08-08 16:24:24  (executeTime < 事务头!)
  变更2:  2012-08-08 16:24:25
  事务尾: 2012-08-08 16:24:26

MySQL中事务的executeTime来自binlog事件的timestamp，
ROWDATA事件的timestamp可能早于TRANSACTIONBEGIN的timestamp
```

```
场景b：主备切换
  主库数据: executeTime = 1000
  切换到备库: 备库的binlog可能比主库延迟几秒
  备库数据: executeTime = 995  (时间回退!)
```

当检测到时间回退（`timestamp < state()`），直接将 `threshold` 更新为当前时间戳。这会**强制阻塞其他通道**，确保这个更早的事件被优先处理。

**3. threshold 更新**

```
if (lastTimestamps.size() >= groupSize) {
    threshold = peek()  // 最小时间戳
} else {
    threshold = Long.MIN_VALUE  // 阻塞所有通道
}
```

当所有通道都提交了时间戳时，`threshold` 设置为最小时间戳，唤醒所有等待的线程。`isPermit()` 中 `state <= threshold` 的判断决定哪个通道可以通过。

#### 2.9.4 isPermit() / notify()

```java
protected boolean isPermit(Event event, long state) {
    return state <= state();  // state() 返回 threshold
}

protected void notify(long minTimestamp) {
    condition.signalAll();
}
```

**这一步在干什么？**

- `isPermit()`：判断当前事件的时间戳是否 `<= threshold`。如果是，允许通过。
- `notify()`：使用 `signalAll()` 而非 `signal()`，因为可能有多个通道的时间戳都 `<= threshold`，需要一次性唤醒所有满足条件的线程。

#### 2.9.5 clear() 的作用 —— 主备切换场景

```java
public void clear(Event event) {
    lastTimestamps.remove(getTimestamp(event));
}
```

**这一步在干什么？**

`clear()` 在事件处理完成后（`finally` 块中）调用，从 `lastTimestamps` 中移除该事件的时间戳。

```
clear 之前: lastTimestamps = [100, 150, 200], threshold = 100
clear(100): lastTimestamps = [150, 200]
           size=2 < groupSize=3 → threshold = Long.MIN_VALUE
           → 其他通道被阻塞，等待通道1提交新的时间戳
```

**主备切换场景**：当某个通道发生主备切换时，该通道会被 `interrupt()`。但 `TimelineBarrier.interrupt()` 是空实现——因为主备切换会导致该通道不再提交新的时间戳，`lastTimestamps` 中该通道的旧时间戳不会被 `clear`（因为没有事件处理完成），从而 `lastTimestamps.size()` 会一直 `< groupSize`，其他通道也会被阻塞。

这确保了主备切换期间不会丢失数据顺序。当切换完成后，该通道恢复提交时间戳，`lastTimestamps.size() >= groupSize`，归并排序恢复。

### 2.10 TimelineTransactionBarrier —— 事务感知归并

`TimelineTransactionBarrier` 继承自 `TimelineBarrier`，在时间线归并的基础上增加了**事务感知**能力。

#### 2.10.1 完整源码

```java
package com.alibaba.otter.canal.sink.entry.group;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 相比于{@linkplain TimelineBarrier}，增加了按事务支持，会按照事务进行分库合并处理
 * 
 * @author jianghang 2012-10-18 下午05:18:38
 * @version 1.0.0
 */
public class TimelineTransactionBarrier extends TimelineBarrier {

    private ThreadLocal<Boolean> inTransaction = ThreadLocal.withInitial(() -> false);

    /**
     * <pre>
     * 几种状态：
     * 0：初始状态，允许大家竞争
     * 1: 事务数据处理中
     * 2: 非事务数据处理中
     * </pre>
     */
    private AtomicInteger        txState       = new AtomicInteger(0);

    public TimelineTransactionBarrier(int groupSize){
        super(groupSize);
    }

    public void await(Event event) throws InterruptedException {
        try {
            super.await(event);
        } catch (InterruptedException e) {
            reset();
            throw e;
        }
    }

    public void await(Event event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        try {
            super.await(event);
        } catch (InterruptedException e) {
            reset();
            throw e;
        }
    }

    public void clear(Event event) {
        super.clear(event);

        if (txState.intValue() == 2) {
            boolean result = txState.compareAndSet(2, 0);
            if (result == false) {
                throw new CanalSinkException("state is not correct in non-transaction");
            }
        } else if (isTransactionEnd(event)) {
            inTransaction.set(false);
            boolean result = txState.compareAndSet(1, 0);
            if (result == false) {
                throw new CanalSinkException("state is not correct in transaction");
            }
        }
    }

    protected boolean isPermit(Event event, long state) {
        if (txState.intValue() == 1 && inTransaction.get()) {
            // 事务中，直接通过
            return true;
        } else if (txState.intValue() == 0) {
            boolean result = super.isPermit(event, state);
            if (result) {
                if (isTransactionBegin(event)) {
                    if (txState.compareAndSet(0, 1)) {
                        inTransaction.set(true);
                        return true;
                    }
                } else if (txState.compareAndSet(0, 2)) {
                    return true;
                }
            }
        }

        return false;
    }

    public void interrupt() {
        super.interrupt();
        reset();
    }

    private void reset() {
        inTransaction.remove();
        txState.set(0);
    }

    private boolean isTransactionBegin(Event event) {
        return event.getEntryType() == EntryType.TRANSACTIONBEGIN;
    }

    private boolean isTransactionEnd(Event event) {
        return event.getEntryType() == EntryType.TRANSACTIONEND;
    }
}
```

#### 2.10.2 txState 三态状态机

```
                    CAS(0→1) + inTransaction=true
          ┌──────────────────────────────────┐
          │                                   │
          v                                   │
    ┌─────────────┐                     ┌─────────────┐
    │  txState=0  │                     │  txState=1  │
    │  初始状态    │ ←── CAS(1→0) ──────│  事务处理中   │
    │  允许竞争    │     (事务尾clear)   │             │
    └─────┬───────┘                     └─────────────┘
          │
          │ CAS(0→2)
          v
    ┌─────────────┐
    │  txState=2  │
    │  非事务处理  │
    │  中         │
    └─────┬───────┘
          │
          │ CAS(2→0)
          │ (非事务clear)
          v
    ┌─────────────┐
    │  txState=0  │
    │  回到初始    │
    └─────────────┘
```

**三种状态的语义：**

| 状态 | 值 | 含义 | 进入条件 | 退出条件 |
|------|---|------|---------|---------|
| 初始 | 0 | 允许所有通道竞争 | 初始化/reset | CAS(0→1) 或 CAS(0→2) |
| 事务处理中 | 1 | 某个通道开始了一个事务 | CAS(0→1) + TRANSACTIONBEGIN | CAS(1→0) + TRANSACTIONEND |
| 非事务处理中 | 2 | 某个通道在处理非事务数据（DDL等） | CAS(0→2) | CAS(2→0) |

#### 2.10.3 ThreadLocal inTransaction

```java
private ThreadLocal<Boolean> inTransaction = ThreadLocal.withInitial(() -> false);
```

**这一步在干什么？**

`inTransaction` 是一个 `ThreadLocal<Boolean>`，每个通道（每个 GroupEventSink 实例的调用线程）有自己的副本。

由于 `GroupEventSink.doSink()` 在遍历事件列表时，同一个线程会处理 BEGIN → ROWDATA → END 的完整事务，`inTransaction` 标记当前线程是否在事务中。这样 `isPermit()` 中 `txState.intValue() == 1 && inTransaction.get()` 的判断可以快速放行事务内的后续事件，不需要再次通过时间线归并排序。

#### 2.10.4 isPermit() 重写 —— 事务内直接通过 vs 首次竞争

```java
protected boolean isPermit(Event event, long state) {
    if (txState.intValue() == 1 && inTransaction.get()) {
        // 事务中，直接通过
        return true;
    } else if (txState.intValue() == 0) {
        boolean result = super.isPermit(event, state);
        if (result) {
            if (isTransactionBegin(event)) {
                if (txState.compareAndSet(0, 1)) {
                    inTransaction.set(true);
                    return true;
                }
            } else if (txState.compareAndSet(0, 2)) {
                return true;
            }
        }
    }

    return false;
}
```

**这一步在干什么？**

`isPermit()` 重写实现了三种判断路径：

**路径1：事务内直接通过**

```java
if (txState.intValue() == 1 && inTransaction.get()) {
    return true;
}
```

当 `txState=1`（某个通道获得了事务处理权）且当前线程的 `inTransaction=true`（就是获得事务权的那个线程），直接返回 true。这避免了事务内每条 ROWDATA 都要参与归并排序，大幅提升性能。

**路径2：初始状态竞争**

```java
if (txState.intValue() == 0) {
    boolean result = super.isPermit(event, state); // 先通过时间线归并排序
    if (result) {
        if (isTransactionBegin(event)) {
            if (txState.compareAndSet(0, 1)) {
                inTransaction.set(true);
                return true;
            }
        } else if (txState.compareAndSet(0, 2)) {
            return true;
        }
    }
}
```

当 `txState=0`（初始状态），先通过父类的 `isPermit()`（时间线归并排序），通过后再根据事件类型进行 CAS 竞争：
- `TRANSACTIONBEGIN`：CAS(0→1)，成功则设置 `inTransaction=true`，获得事务处理权
- 非 `TRANSACTIONBEGIN`（如 DDL、TRANSACTIONEND）：CAS(0→2)，进入非事务处理状态

**路径3：不满足条件，返回 false**

当 `txState=1` 但当前线程不是事务持有者，或 `txState=2`（非事务处理中），直接返回 false，线程继续等待。

#### 2.10.5 clear() 中先判断 txState==2 再判断事务尾的微妙顺序

```java
public void clear(Event event) {
    super.clear(event);

    // 应该先判断2，再判断是否是事务尾，因为事务尾也可以导致txState的状态为2
    // 如果先判断事务尾，那么2的状态可能永远没机会被修改了，系统出现死锁
    if (txState.intValue() == 2) {
        boolean result = txState.compareAndSet(2, 0);
        if (result == false) {
            throw new CanalSinkException("state is not correct in non-transaction");
        }
    } else if (isTransactionEnd(event)) {
        inTransaction.set(false);
        boolean result = txState.compareAndSet(1, 0);
        if (result == false) {
            throw new CanalSinkException("state is not correct in transaction");
        }
    }
}
```

**这一步在干什么？**

源码注释解释了为什么先判断 `txState==2` 再判断 `isTransactionEnd`：

> 事务尾也可以导致 txState 的状态为2

这发生在一种特殊场景：基于 zk-cursor 启动时，拿到的第一个 Event 是 `TRANSACTIONEND`（而非 `TRANSACTIONBEGIN`）。此时：

```
事件流: TRANSACTIONEND (没有对应的 TRANSACTIONBEGIN)

isPermit() 中:
  txState=0, event=TRANSACTIONEND
  → super.isPermit() → true (时间线通过)
  → isTransactionBegin(event) → false
  → txState.compareAndSet(0, 2) → true (进入非事务状态)
  → return true

clear() 中:
  如果先判断 isTransactionEnd(event) → true
  → txState.compareAndSet(1, 0) → 失败! (因为 txState=2 而非 1)
  → 抛出 CanalSinkException

  如果先判断 txState==2 → true
  → txState.compareAndSet(2, 0) → 成功
  → 状态正确重置
```

如果先判断事务尾，`txState=2` 的状态可能永远没机会被重置，导致系统死锁——其他通道永远无法竞争到 `txState=0` 状态。

### 2.11 Sink 模块设计总结

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Sink 模块完整调用链                              │
│                                                                        │
│  Parser 线程调用                                                       │
│       │                                                                │
│       v                                                                │
│  EntryEventSink.sink(List<Event>)                                     │
│       │                                                                │
│       v                                                                │
│  sinkData(events)                                                      │
│       │                                                                │
│       ├── doFilter(events)                                             │
│       │     │                                                          │
│       │     ├── ROWDATA正则过滤 (AviaterRegexFilter)                   │
│       │     │     └── RegexFunction → PatternUtils → Perl5Matcher     │
│       │     │                                                          │
│       │     ├── filterTransactionEntry (第一级：过滤空事务)              │
│       │     └── filterEmtryTransactionEntry (第二级：保留事务结构)       │
│       │                                                                │
│       └── doSink(filteredEvents)                                       │
│             │                                                          │
│             ├── handler.before(events)                                 │
│             │     └── HeartBeatEntryEventHandler (心跳优先)             │
│             │                                                          │
│             ├── applyWait (渐进式退避: yield → parkNanos → 10ms封顶)   │
│             │                                                          │
│             ├── eventStore.tryPut / eventStore.put                     │
│             │                                                          │
│             └── handler.after(events)                                  │
│                                                                        │
│  ─── GroupEventSink (多源归并) ───                                     │
│                                                                        │
│  GroupEventSink.doSink(events)                                         │
│       │                                                                │
│       ├── 遍历每个event:                                                │
│       │     ├── barrier.await(event)                                   │
│       │     │     └── TimelineBarrier.single() → 归并排序              │
│       │     │         或 TimelineTransactionBarrier.isPermit()         │
│       │     │              → 事务状态机判断                             │
│       │     │                                                          │
│       │     ├── 最后一条事件: super.doSink(events) (原子写入)           │
│       │     └── barrier.clear(event)                                   │
│       │                                                                │
│       └── interrupt() → barrier.interrupt() → reset()                 │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

| 组件 | 职责 | 核心方法 | 设计模式 |
|------|------|---------|---------|
| `EntryEventSink` | 过滤+投递 | `doFilter()`, `doSink()` | 模板方法 |
| `HeartBeatEntryEventHandler` | 心跳优先处理 | `before()` | 责任链 |
| `AviaterRegexFilter` | schema/table 正则过滤 | `filter()` | 策略模式 |
| `GroupEventSink` | 多源归并 | `doSink()` 重写 | 模板方法 |
| `TimelineBarrier` | 时间线归并排序 | `single()`, `isPermit()` | 屏障模式 |
| `TimelineTransactionBarrier` | 事务感知归并 | `isPermit()`, `clear()` | 状态机 |

---

## 第三章：Store 模块 —— 三指针 RingBuffer

### 3.1 模块总览

Store 模块是 Canal 的数据存储核心，负责接收 Sink 投递的事件并缓存，等待客户端消费。Canal 使用**三指针 RingBuffer** 模型实现了一个高效的有界缓冲区。

```
┌──────────────────────────────────────────────────────────────────┐
│                       Store 模块架构                               │
│                                                                    │
│  ┌────────────────────┐                                          │
│  │ CanalEventStore    │  ← Store 顶层接口                          │
│  │ <E>                │                                            │
│  └─────────┬──────────┘                                          │
│            │                                                      │
│            v                                                      │
│  ┌──────────────────────────┐                                    │
│  │ AbstractCanalStoreScavenge│  ← 抽象类， scavenger 清理机制       │
│  │ <E>                       │                                    │
│  └─────────┬──────────────────┘                                  │
│            │                                                      │
│            v                                                      │
│  ┌──────────────────────────────────┐                            │
│  │ MemoryEventStoreWithBuffer        │  ← 核心实现                  │
│  │                                   │                             │
│  │  entries[] (RingBuffer)           │                             │
│  │  putSequence  (写指针)            │                             │
│  │  getSequence  (读指针)            │                             │
│  │  ackSequence  (确认指针)          │                             │
│  │                                   │                             │
│  │  ReentrantLock + Condition        │                             │
│  │  notFull  (非满条件)              │                             │
│  │  notEmpty (非空条件)              │                             │
│  │                                   │                             │
│  │  BatchMode: ITEMSIZE / MEMSIZE    │                             │
│  └───────────────────────────────────┘                            │
│                                                                    │
│  辅助:                                                             │
│  ┌────────────────┐  ┌──────────┐  ┌──────────────┐             │
│  │ CanalEventUtils │  │ Event    │  │ Events       │             │
│  │ (位点工具)       │  │ (单事件)  │  │ (批量事件)    │             │
│  └────────────────┘  └──────────┘  └──────────────┘             │
│                                                                    │
│  ┌────────────────┐  ┌──────────┐                                 │
│  │ BatchMode       │  │ CanalStoreScavenge │                      │
│  │ (批量模式枚举)   │  │ (清理接口)          │                      │
│  └────────────────┘  └──────────────────────┘                     │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 CanalEventStore 接口设计

```java
package com.alibaba.otter.canal.store;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.store.model.Event;

/**
 * canal数据存储接口
 * 
 * @author jianghang 2012-7-23 下午03:04:05
 * @version 1.0.0
 */
public interface CanalEventStore<E> {

    // ========== Put 操作 ==========

    /**
     * 阻塞写入，直到有空间
     */
    void put(E data) throws InterruptedException, CanalStoreException;

    /**
     * 非阻塞写入，尝试一次
     */
    boolean tryPut(E data) throws CanalStoreException;

    /**
     * 超时写入
     */
    boolean put(E data, long timeout, TimeUnit unit) throws InterruptedException,
                                                    CanalStoreException;

    // ========== Get 操作 ==========

    /**
     * 阻塞获取
     */
    Events get(Position start, int batchSize) throws InterruptedException,
                                             CanalStoreException;

    /**
     * 非阻塞获取
     */
    Events tryGet(Position start, int batchSize) throws CanalStoreException;

    /**
     * 超时获取
     */
    Events get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException,
                                                                          CanalStoreException;

    // ========== Ack 操作 ==========

    /**
     * 确认消费
     */
    void ack(Position position) throws CanalStoreException;

    /**
     * 回滚（未确认的数据下次可重新消费）
     */
    void rollback();

    // ========== 其他 ==========

    /**
     * 获取当前Store的最新位点
     */
    Position getFirstPosition();

    /**
     * 获取当前Store的末尾位点
     */
    Position getLatestPosition();
}
```

**这一步在干什么？**

`CanalEventStore` 接口定义了 Store 的全部操作语义：

| 操作 | 阻塞版本 | 非阻塞版本 | 超时版本 |
|------|---------|-----------|---------|
| Put | `put()` | `tryPut()` | `put(timeout)` |
| Get | `get()` | `tryGet()` | `get(timeout)` |
| Ack | `ack()` | - | - |
| Rollback | `rollback()` | - | - |

三种 Put 语义：
- **阻塞 Put**：如果 RingBuffer 满，阻塞直到有空间
- **非阻塞 Put (tryPut)**：如果 RingBuffer 满，立即返回 false
- **超时 Put**：如果 RingBuffer 满，等待指定时间，超时返回 false

三种 Get 语义类似，分别对应阻塞、非阻塞和超时获取。

### 3.3 BatchMode —— 批量模式枚举

```java
package com.alibaba.otter.canal.store.model;

/**
 * 批量获取模式
 * 
 * @author jianghang 2012-9-14 上午10:28:14
 * @version 1.0.0
 */
public enum BatchMode {

    /**
     * 按条数批量获取
     */
    ITEMSIZE,

    /**
     * 按内存大小批量获取
     */
    MEMSIZE;
}
```

**这一步在干什么？**

`BatchMode` 定义了两种批量获取模式：

| 模式 | 含义 | batchSize 单位 | 适用场景 |
|------|------|----------------|---------|
| `ITEMSIZE` | 按条数 | 条 | 精确控制每次获取的事件数量 |
| `MEMSIZE` | 按内存大小 | 字节 | 控制每次获取的数据量大小，防止单次获取过多大事件导致内存溢出 |

### 3.4 Event 和 Events 数据模型

#### 3.4.1 Event

```java
package com.alibaba.otter.canal.store.model;

import java.util.List;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 代表一次数据变更事件
 * 
 * @author jianghang 2012-7-23 下午03:16:24
 * @version 1.0.0
 */
public class Event extends CanalEntry.Entry implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private LogIdentity        logIdentity;     // 来源标识（哪个MySQL实例的哪个日志文件）
    private long               executeTime;     // SQL执行时间（来自binlog的timestamp）
    private String             schemaName;      // 库名
    private String             tableName;       // 表名
    private long               logfileOffset;   // binlog文件偏移量

    public Event(){
        // 默认构造
    }

    public Event(LogIdentity logIdentity, CanalEntry.Entry entry){
        this.logIdentity = logIdentity;
        // 复制Entry中的字段
        this.setHeader(entry.getHeader());
        this.setEntryType(entry.getEntryType());
        this.setStoreValue(entry.getStoreValue());
    }

    // getter/setter 省略...
}
```

**这一步在干什么？**

`Event` 继承自 Protobuf 生成的 `CanalEntry.Entry`，额外增加了：
- `logIdentity`：标识数据来源（哪个 MySQL 实例的哪个 binlog 文件），用于多源归并场景
- `executeTime`：SQL 执行时间，来自 binlog 事件的 timestamp，用于 TimelineBarrier 的归并排序
- `schemaName` / `tableName`：库表名，用于正则过滤
- `logfileOffset`：binlog 文件偏移量，用于位点定位

#### 3.4.2 Events

```java
package com.alibaba.otter.canal.store.model;

import java.util.List;

/**
 * 代表一批数据变更事件
 * 
 * @author jianghang 2012-9-14 上午10:20:17
 * @version 1.0.0
 */
public class Events implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private List<Event>       events;           // 事件列表
    private PositionRange     positionRange;    // 位点范围

    public Events(){
    }

    public Events(List<Event> events, PositionRange positionRange){
        this.events = events;
        this.positionRange = positionRange;
    }

    // getter/setter 省略...
}
```

**这一步在干什么？**

`Events` 是 `get()` 方法的返回值，封装了一批事件列表和对应的位点范围。`PositionRange` 记录了这批事件的起始和结束位点，用于 ACK 和回滚。

### 3.5 MemoryEventStoreWithBuffer —— 三指针 RingBuffer 模型

`MemoryEventStoreWithBuffer` 是 Store 模块的核心实现，使用数组 + 三个 AtomicLong 指针实现了高效的环形缓冲区。

#### 3.5.1 三指针模型

```
                  三指针 RingBuffer 示意图
                  (bufferSize = 16, indexMask = 15)

    指针位置:  ackSequence=4  getSequence=8  putSequence=12

    ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
    │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │ 6  │ 7  │ 8  │ 9  │ 10 │ 11 │ 12 │ 13 │ 14 │ 15 │
    └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘
    ↑                   ↑                   ↑                                  ↑
    │                   │                   │                                  │
    │   已确认(可回收)   │  已读未确认        │         已写未读           空闲      │
    │   ackSequence     │                   │     getSequence          putSequence│
    │                   │                                     ↑                  │
    │                   │                                     │                  │
    │                   └──────── getSequence ───────────────┘                  │
    │                                                                        │
    └────────────── ackSequence ───────────────────────────────────────────────┘

    区间含义:
    [ackSequence, getSequence) = 已读未确认 (客户端已获取但未ACK，回滚后可重新消费)
    [getSequence, putSequence) = 已写未读   (新写入的数据，等待客户端获取)
    [putSequence, ackSequence + bufferSize) = 空闲    (可写入新数据)
    [0, ackSequence) = 已确认   (已ACK，可被覆盖回收)
```

三个指针的含义：

| 指针 | 类型 | 含义 | 更新时机 |
|------|------|------|---------|
| `putSequence` | `AtomicLong` | 写指针，指向最后一个已写入的位置 | `put()` / `tryPut()` 成功后 |
| `getSequence` | `AtomicLong` | 读指针，指向最后一个已读取的位置 | `get()` / `tryGet()` 成功后 |
| `ackSequence` | `AtomicLong` | 确认指针，指向最后一个已确认的位置 | `ack()` 成功后 |

三个区间：

| 区间 | 计算 | 含义 |
|------|------|------|
| 已确认 | [0, ackSequence] | 客户端已 ACK，可被覆盖 |
| 已读未确认 | (ackSequence, getSequence] | 客户端已获取但未 ACK，rollback 可重新消费 |
| 已写未读 | (getSequence, putSequence] | 新写入的数据，等待客户端获取 |
| 空闲 | (putSequence, ackSequence + bufferSize] | 可写入新数据 |

#### 3.5.2 完整源码 —— 类结构和字段

```java
package com.alibaba.otter.canal.store.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;
import com.alibaba.otter.canal.store.model.exception.CanalStoreException;

/**
 * 基于内存的EventStore实现，使用RingBuffer
 * 
 * @author jianghang 2012-7-23 下午03:28:24
 * @version 1.0.0
 */
public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge<Event>
        implements CanalEventStore<Event> {

    private static final long serialVersionUID = 1L;

    // ============ RingBuffer 核心字段 ============

    private Event[]            entries;          // RingBuffer数据数组
    private int                indexMask;        // 位与掩码 = bufferSize - 1
    private AtomicLong         putSequence       = new AtomicLong(-1); // 写指针
    private AtomicLong         getSequence       = new AtomicLong(-1); // 读指针
    private AtomicLong         ackSequence       = new AtomicLong(-1); // ACK指针

    // ============ 并发控制 ============

    private ReentrantLock      lock              = new ReentrantLock();
    private Condition          notFull           = lock.newCondition();  // 非满条件
    private Condition          notEmpty          = lock.newCondition();  // 非空条件

    // ============ 配置参数 ============

    private int                bufferSize        = 16 * 1024;  // RingBuffer大小，必须2的幂
    private int                batchMode         = BatchMode.ITEMSIZE.ordinal(); // 批量模式
    private boolean            ddlIsolation      = false;   // DDL是否单独获取
    private boolean            raw               = true;     // 是否接受raw数据

    // ============ 内存计算（MEMSIZE模式） ============

    private AtomicLong         putMemorySize     = new AtomicLong(0); // 已写入内存大小
    private AtomicLong         getMemorySize     = new AtomicLong(0); // 已读取内存大小
    private AtomicLong         ackMemorySize     = new AtomicLong(0); // 已确认内存大小

    // ============ 位点相关 ============

    private Position           firstPosition;
    private Position           latestPosition;
}
```

#### 3.5.3 start() 初始化 —— bufferSize 必须 2 的幂、indexMask = bufferSize-1

```java
public void start() throws CanalStoreException {
    if (running) {
        return;
    }

    super.start();

    if (Integer.bitCount(bufferSize) != 1) {
        throw new CanalStoreException("bufferSize must be a power of 2");
    }

    indexMask = bufferSize - 1;
    entries = new Event[bufferSize];
}
```

**这一步在干什么？**

`start()` 初始化 RingBuffer：

1. **bufferSize 必须 2 的幂**：通过 `Integer.bitCount(bufferSize) != 1` 检查。`Integer.bitCount()` 返回整数的二进制中 1 的个数。2 的幂（如 16 = 0b10000）只有一个 1，所以 `bitCount == 1` 是 2 的幂的充要条件。

   为什么必须是 2 的幂？因为 RingBuffer 使用**位与取模** `index & indexMask` 代替 `index % bufferSize`，位与运算比取模快得多。但位与取模只在 bufferSize 是 2 的幂时才等价于取模。

2. **indexMask = bufferSize - 1**：例如 bufferSize=16，则 indexMask=15 (0b1111)。`index & 15` 等价于 `index % 16`，但只需要一次位与运算。

3. **entries 数组初始化**：分配 RingBuffer 数组。

```
bufferSize = 16 (0b10000)
indexMask  = 15 (0b01111)

index = 20
index & indexMask = 20 & 15 = 0b10100 & 0b01111 = 0b00100 = 4
等价于: 20 % 16 = 4
```

#### 3.5.4 put() 完整实现 —— 阻塞/超时/非阻塞三种

**非阻塞版本 tryPut()：**

```java
public boolean tryPut(Event data) throws CanalStoreException {
    if (data == null) {
        throw new NullPointerException();
    }

    lock.lock();
    try {
        if (checkFreeSlotAt(1)) {
            doPut(data);
            return true;
        } else {
            return false;
        }
    } finally {
        lock.unlock();
    }
}
```

**这一步在干什么？**

`tryPut()` 是非阻塞写入：
1. 获取锁
2. `checkFreeSlotAt(1)` 检查是否有至少 1 个空闲槽位
3. 有空间则 `doPut()` 写入并返回 true
4. 无空间则直接返回 false

**阻塞版本 put()：**

```java
public void put(Event data) throws InterruptedException, CanalStoreException {
    if (data == null) {
        throw new NullPointerException();
    }

    lock.lockInterruptibly();
    try {
        if (running) {
            while (!checkFreeSlotAt(1)) {
                if (!running) {
                    throw new CanalStoreException("MemoryEventStoreWithBuffer has stopped");
                }
                notFull.await(); // 等待非满信号
            }
            doPut(data);
        } else {
            throw new CanalStoreException("MemoryEventStoreWithBuffer has stopped");
        }
    } finally {
        lock.unlock();
    }
}
```

**这一步在干什么？**

`put()` 是阻塞写入：
1. 使用 `lockInterruptibly()` 获取锁（可被中断）
2. 在 `while` 循环中检查空间：如果满了，调用 `notFull.await()` 阻塞
3. `notFull.await()` 释放锁进入等待；被 `signal()` 唤醒后重新获取锁
4. 有空间则 `doPut()` 写入

**超时版本 put(timeout)：**

```java
public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException,
                                                           CanalStoreException {
    lock.lockInterruptibly();
    try {
        long nanos = unit.toNanos(timeout);
        while (!checkFreeSlotAt(1)) {
            if (nanos <= 0) {
                return false; // 超时
            }
            nanos = notFull.awaitNanos(nanos); // 带超时等待
        }
        doPut(data);
        return true;
    } finally {
        lock.unlock();
    }
}
```

**这一步在干什么？**

超时版本使用 `notFull.awaitNanos(nanos)` 代替 `notFull.await()`。`awaitNanos()` 返回剩余等待时间，如果返回值 `<= 0` 表示超时，直接返回 false。

#### 3.5.5 doPut() 关键设计 —— 先写数据再更新 putSequence（防脏读）

```java
private void doPut(Event data) {
    long current = putSequence.get();
    long next = current + 1;

    // 先写数据
    entries[(int) getIndex(next)] = data;
    // MEMSIZE模式：累加内存大小
    if (batchMode == BatchMode.MEMSIZE.ordinal()) {
        long size = calculateSize(data);
        putMemorySize.addAndGet(size);
    }

    // 再更新 putSequence（volatile 写，保证可见性）
    putSequence.set(next);

    // 通知消费者有新数据
    notEmpty.signal();
}
```

**这一步在干什么？**

`doPut()` 的核心设计是**先写数据再更新 putSequence**：

```
时序：
1. entries[index] = data     <- 先写入数据
2. putSequence.set(next)     <- 再更新写指针（volatile写，保证对其他线程可见）
3. notEmpty.signal()         <- 通知等待的消费者
```

**为什么顺序很重要？**

如果先更新 `putSequence` 再写数据，消费者可能看到 `putSequence` 已更新但 `entries[next]` 还没写入，导致脏读。

先写数据再更新 `putSequence`，利用 `AtomicLong.set()` 的 volatile 语义作为内存屏障，确保消费者看到 `putSequence` 更新时数据已写入完成。

**getIndex() 位与取模：**

```java
private long getIndex(long sequenc) {
    return sequenc & indexMask;
}
```

`sequence & indexMask` 将递增序号映射到 RingBuffer 数组索引。由于 `indexMask = bufferSize - 1` 且 `bufferSize` 是 2 的幂，这个位与等价于取模但更快。

#### 3.5.6 get() 完整实现

**阻塞版本 get()：**

```java
public Events get(Position start, int batchSize) throws InterruptedException,
                                                CanalStoreException {
    lock.lockInterruptibly();
    try {
        while (!checkUnGetSlotAt(start, batchSize)) {
            if (!running) {
                throw new CanalStoreException("MemoryEventStoreWithBuffer has stopped");
            }
            notEmpty.await(); // 阻塞等待非空
        }
        return doGet(start, batchSize);
    } finally {
        lock.unlock();
    }
}
```

**非阻塞版本 tryGet()：**

```java
public Events tryGet(Position start, int batchSize) throws CanalStoreException {
    lock.lock();
    try {
        if (!checkUnGetSlotAt(start, batchSize)) {
            return null; // 无数据可读
        }
        return doGet(start, batchSize);
    } finally {
        lock.unlock();
    }
}
```

**超时版本 get(timeout)：**

```java
public Events get(Position start, int batchSize, long timeout, TimeUnit unit)
        throws InterruptedException, CanalStoreException {
    lock.lockInterruptibly();
    try {
        long nanos = unit.toNanos(timeout);
        for (;;) {
            if (checkUnGetSlotAt(start, batchSize)) {
                return doGet(start, batchSize);
            }
            if (nanos <= 0) {
                // 超时后"有多少取多少"
                if (putSequence.get() > getSequence.get()) {
                    return doGet(start, batchSize);
                }
                return null;
            }
            nanos = notEmpty.awaitNanos(nanos);
        }
    } finally {
        lock.unlock();
    }
}
```

**这一步在干什么？**

三种 `get()` 版本与 `put()` 类似：阻塞版用 `notEmpty.await()`，非阻塞版返回 null，超时版用 `notEmpty.awaitNanos()`。

**超时版本的特殊处理（超时后"有多少取多少"）：**

超时后不直接返回 null，而是检查是否有数据可读（`putSequence > getSequence`），如果有则获取已有数据。这确保即使没达到 `batchSize` 也能返回数据。

#### 3.5.7 doGet() 核心逻辑 —— DDL隔离、ITEMSIZE vs MEMSIZE两种批量模式、Ack点计算、CAS保护

```java
private Events doGet(Position start, int batchSize) throws CanalStoreException {
    long current = getSequence.get();
    long end = current;

    // 计算实际可读的结束位置
    long maxAbleSequence = putSequence.get();
    int count = 0;
    int memSize = 0;

    if (batchMode == BatchMode.ITEMSIZE.ordinal()) {
        // ITEMSIZE模式：按条数控制
        count = (int) Math.min(batchSize, maxAbleSequence - current);
    } else {
        // MEMSIZE模式：按内存大小控制
        count = batchSize; // batchSize作为内存上限
    }

    // 确定结束位置
    long endSequence = current + count;
    if (endSequence > maxAbleSequence) {
        endSequence = maxAbleSequence;
    }

    // DDL隔离处理
    if (ddlIsolation) {
        for (long i = current + 1; i <= endSequence; i++) {
            Event event = entries[(int) getIndex(i)];
            if (event != null && isDdlEvent(event)) {
                endSequence = i;
                break;
            }
        }
    }

    // 提取事件
    List<Event> result = new ArrayList<>();
    for (long i = current + 1; i <= endSequence; i++) {
        int index = (int) getIndex(i);
        Event event = entries[index];
        if (event == null) {
            continue;
        }
        result.add(event);

        if (batchMode == BatchMode.MEMSIZE.ordinal()) {
            memSize += calculateSize(event);
            if (memSize >= count) {
                endSequence = i;
                break;
            }
        }
    }

    // CAS 更新 getSequence（防止并发 get）
    if (!getSequence.compareAndSet(current, endSequence)) {
        throw new CanalStoreException("get occurred concurrent, sequence=" + current);
    }

    // MEMSIZE模式更新 getMemorySize
    if (batchMode == BatchMode.MEMSIZE.ordinal()) {
        getMemorySize.addAndGet(memSize);
    }

    // 构建返回结果
    Events events = new Events();
    events.setEvents(result);
    // 设置位点范围
    if (!result.isEmpty()) {
        PositionRange<Position> range = new PositionRange<>();
        range.setStart(CanalEventUtils.createPosition(result.get(0)));
        range.setEnd(CanalEventUtils.createPosition(result.get(result.size() - 1)));
        events.setPositionRange(range);
    }

    // 通知 notFull
    notFull.signal();

    return events;
}
```

**这一步在干什么？**

`doGet()` 的核心逻辑包含四个关键点：

**1. DDL隔离**

当 `ddlIsolation=true` 时，如果一批事件中包含 DDL，只取到 DDL 为止。DDL 改变表结构，客户端需先消费完之前的 DML 再单独处理 DDL。

**2. ITEMSIZE vs MEMSIZE 两种批量模式**

```
ITEMSIZE: count = min(batchSize, 可读数量) -> 按条数控制
MEMSIZE:  memSize累加，达到batchSize(字节上限)时停止 -> 按内存控制
```

**3. Ack点计算**

返回的 `Events` 中包含 `PositionRange`，记录起始和结束位点，用于 ACK。

**4. CAS保护**

`getSequence.compareAndSet(current, endSequence)` 防止并发 get 导致数据错乱。

#### 3.5.8 ack() 完整实现 —— cleanUntil() Position匹配、MEMSIZE模式内存回收、CAS防并发

```java
public void ack(Position position) throws CanalStoreException {
    cleanUntil(position);
}

private void cleanUntil(Position position) throws CanalStoreException {
    lock.lock();
    try {
        long current = ackSequence.get();
        long newAck = current;

        // 找到 position 对应的 sequence
        long maxAbleSequence = putSequence.get();
        for (long i = current + 1; i <= maxAbleSequence; i++) {
            Event event = entries[(int) getIndex(i)];
            if (event != null && CanalEventUtils.createPosition(event).equals(position)) {
                newAck = i;
                break;
            }
        }

        if (newAck == current) {
            return; // 没找到匹配的position
        }

        // MEMSIZE模式：回收内存
        if (batchMode == BatchMode.MEMSIZE.ordinal()) {
            long memSize = 0;
            for (long i = current + 1; i <= newAck; i++) {
                int index = (int) getIndex(i);
                Event event = entries[index];
                if (event != null) {
                    memSize += calculateSize(event);
                    entries[index] = null; // 帮助GC
                }
            }
            ackMemorySize.addAndGet(memSize);
        }

        // CAS 更新 ackSequence
        if (!ackSequence.compareAndSet(current, newAck)) {
            throw new CanalStoreException("ack occurred concurrent, sequence=" + current);
        }

        // 通知 notFull
        notFull.signal();
    } finally {
        lock.unlock();
    }
}
```

**这一步在干什么？**

`ack()` 的核心是 `cleanUntil()`：

1. **Position匹配**：遍历从 `ackSequence+1` 到 `putSequence` 的事件，找到 Position 匹配的事件
2. **MEMSIZE模式内存回收**：`entries[index] = null` 帮助 GC 回收内存，更新 `ackMemorySize`
3. **CAS防并发**：`ackSequence.compareAndSet(current, newAck)` 防止并发 ACK
4. **通知 notFull**：ACK 后释放空间，通知等待写入的生产者

#### 3.5.9 rollback() 实现 —— getSequence 重置到 ackSequence（at-least-once语义保证）

```java
public void rollback() {
    lock.lock();
    try {
        // 将 getSequence 回退到 ackSequence
        getSequence.set(ackSequence.get());

        // MEMSIZE模式：回退 getMemorySize
        if (batchMode == BatchMode.MEMSIZE.ordinal()) {
            getMemorySize.set(ackMemorySize.get());
        }

        // 通知 notFull
        notFull.signal();
    } finally {
        lock.unlock();
    }
}
```

**这一步在干什么？**

`rollback()` 实现 **at-least-once（至少一次）** 语义：

```
回滚前: ackSequence=4, getSequence=8, putSequence=12
回滚后: ackSequence=4, getSequence=4, putSequence=12
下次get: 从5开始重新获取 -> 事件5~8被重新消费
```

客户端消费失败时调用 `rollback()`，读指针回退到 ACK 位置，保证数据至少被消费一次。

#### 3.5.10 辅助方法

**checkFreeSlotAt() —— MEMSIZE模式的双重检查（指针+内存大小）**

```java
private boolean checkFreeSlotAt(final long target) {
    // 检查指针空间
    long putTarget = putSequence.get() + target;
    if (putTarget - ackSequence.get() > bufferSize) {
        return false; // 指针超过缓冲区大小
    }

    // MEMSIZE模式：额外检查内存大小
    if (batchMode == BatchMode.MEMSIZE.ordinal()) {
        long pendingSize = putMemorySize.get() - ackMemorySize.get();
        if (pendingSize > bufferSize * 1024L) {
            return false; // 内存超限
        }
    }

    return true;
}
```

**这一步在干什么？**

双重检查确保 MEMSIZE 模式下即使事件数量没超过 bufferSize，但内存总量超限时也不会写入。

**checkUnGetSlotAt()**

```java
private boolean checkUnGetSlotAt(Position start, int batchSize) {
    long remain = putSequence.get() - getSequence.get();
    return remain >= 1; // 有至少1条数据可读
}
```

**calculateSize()**

```java
private long calculateSize(Event event) {
    if (event == null) {
        return 0;
    }
    int size = 0;
    if (event.getStoreValue() != null) {
        size += event.getStoreValue().length;
    }
    size += 512; // 预估固定开销
    return size;
}
```

### 3.6 CanalEventUtils —— 位点工具

```java
package com.alibaba.otter.canal.store.helper;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.model.Event;

/**
 * Canal事件工具类
 */
public class CanalEventUtils {

    /**
     * 根据Event创建Position
     */
    public static Position createPosition(Event event) {
        return createPosition(event, false);
    }

    public static Position createPosition(Event event, boolean includeSchema) {
        EntryPosition position = new EntryPosition();
        position.setJournalId(event.getLogId());
        position.setPosition(event.getPosition());
        position.setTimestamp(event.getExecuteTime());
        if (includeSchema) {
            position.setSchemaName(event.getSchemaName());
            position.setTableName(event.getTableName());
        }

        LogPosition logPosition = new LogPosition();
        logPosition.setPostion(position);
        logPosition.setIdentity(event.getLogIdentity());

        return logPosition;
    }

    /**
     * 比较两个Position的大小
     */
    public static int compare(Position a, Position b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }

        if (a instanceof LogPosition && b instanceof LogPosition) {
            LogPosition la = (LogPosition) a;
            LogPosition lb = (LogPosition) b;
            int c = la.getIdentity().compareTo(lb.getIdentity());
            if (c != 0) {
                return c;
            }
            return la.getPostion().compareTo(lb.getPostion());
        }

        return 0;
    }
}
```

**这一步在干什么？**

`CanalEventUtils` 提供两个核心功能：

1. **`createPosition(event)`**：从 Event 构建 LogPosition（包含 LogIdentity 和 EntryPosition），用于 `get()` 的起始定位和 `ack()` 的位点确认。

2. **`compare(a, b)`**：比较两个 Position 的大小。先比较 LogIdentity（来源），再比较 EntryPosition（位点）。用于 TimelineBarrier 的时间排序和 `cleanUntil()` 的 Position 匹配。

### 3.7 AbstractCanalStoreScavenge —— 清理机制

```java
package com.alibaba.otter.canal.store;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Store清理抽象类
 */
public abstract class AbstractCanalStoreScavenge<E> implements CanalStoreScavenge {

    protected volatile boolean            running          = false;
    protected ScheduledExecutorService    scheduler;
    protected AtomicBoolean               scavengeExecuting = new AtomicBoolean(false);

    protected int                         scavengeInterval  = 60 * 60;     // 清理间隔，默认1小时

    public void start() {
        if (running) {
            return;
        }
        running = true;

        if (scavengeInterval > 0) {
            scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "canal-store-scavenge");
                    t.setDaemon(true);
                    return t;
                }
            });

            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    scavenge();
                }
            }, scavengeInterval, scavengeInterval, TimeUnit.SECONDS);
        }
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public void scavenge() {
        if (scavengeExecuting.compareAndSet(false, true)) {
            try {
                doScavenge();
            } finally {
                scavengeExecuting.set(false);
            }
        }
    }

    protected abstract void doScavenge();
}
```

对应接口：

```java
package com.alibaba.otter.canal.store;

/**
 * Store清理接口
 */
public interface CanalStoreScavenge {
    void scavenge();
}
```

**这一步在干什么？**

`AbstractCanalStoreScavenge` 提供定时清理机制：
1. **定时调度**：使用 `ScheduledThreadPoolExecutor` 定期执行 `scavenge()`
2. **防重入**：使用 `AtomicBoolean` 确保不并发执行
3. **模板方法**：`doScavenge()` 由子类实现具体清理逻辑

### 3.8 Store 模块完整调用链

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       Store 模块完整调用链                                 │
│                                                                            │
│  Producer (Parser/Sink 线程):                                              │
│                                                                            │
│  eventStore.put(events) / tryPut(events) / put(events, timeout)           │
│       │                                                                    │
│       v                                                                    │
│  lock.lockInterruptibly() / lock.lock()                                    │
│       │                                                                    │
│       v                                                                    │
│  checkFreeSlotAt(1)?                                                       │
│       │                                                                    │
│    Yes -> doPut(data)                                                      │
│       │      ├── entries[index] = data  (先写数据)                         │
│       │      ├── putSequence.set(next)  (再更新指针)                       │
│       │      └── notEmpty.signal()      (通知消费者)                       │
│       │                                                                    │
│    No  -> notFull.await() / return false / awaitNanos                      │
│                                                                            │
│  Consumer (Client 线程):                                                   │
│                                                                            │
│  eventStore.get(start, batchSize) / tryGet / get(timeout)                 │
│       │                                                                    │
│       v                                                                    │
│  checkUnGetSlotAt(start, batchSize)?                                      │
│       │                                                                    │
│    Yes -> doGet(start, batchSize)                                          │
│       │      ├── 计算 endSequence (ITEMSIZE/MEMSIZE)                      │
│       │      ├── DDL隔离检查                                                │
│       │      ├── 提取事件列表                                                │
│       │      ├── getSequence CAS 更新                                      │
│       │      ├── 构建 Events + PositionRange                               │
│       │      └── notFull.signal()                                          │
│       │                                                                    │
│    No  -> notEmpty.await() / return null / awaitNanos                      │
│                                                                            │
│  Ack (Client 线程):                                                        │
│                                                                            │
│  eventStore.ack(position) -> cleanUntil(position)                         │
│       ├── 遍历匹配 Position                                                │
│       ├── MEMSIZE: entries[i] = null (内存回收)                             │
│       ├── ackSequence CAS 更新                                             │
│       └── notFull.signal()                                                 │
│                                                                            │
│  Rollback (Client 线程):                                                   │
│                                                                            │
│  eventStore.rollback()                                                     │
│       ├── getSequence = ackSequence (回退读指针)                             │
│       ├── MEMSIZE: getMemorySize = ackMemorySize                            │
│       └── notFull.signal()                                                 │
│                                                                            │
│  Scavenge (定时清理线程):                                                   │
│                                                                            │
│  scheduler.scheduleAtFixedRate() -> doScavenge()                           │
│       └── 清理过期数据，释放内存                                              │
│                                                                            │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.9 Store 模块设计总结

| 方法 | 锁 | 检查条件 | 核心操作 | 通知 |
|------|---|---------|---------|------|
| `tryPut()` | `lock` | `checkFreeSlotAt(1)` | `doPut()` | `notEmpty.signal()` |
| `put()` | `lockInterruptibly` | `checkFreeSlotAt(1)` + `notFull.await()` | `doPut()` | `notEmpty.signal()` |
| `put(timeout)` | `lockInterruptibly` | `checkFreeSlotAt(1)` + `notFull.awaitNanos()` | `doPut()` | `notEmpty.signal()` |
| `tryGet()` | `lock` | `checkUnGetSlotAt()` | `doGet()` | `notFull.signal()` |
| `get()` | `lockInterruptibly` | `checkUnGetSlotAt()` + `notEmpty.await()` | `doGet()` | `notFull.signal()` |
| `get(timeout)` | `lockInterruptibly` | `checkUnGetSlotAt()` + `notEmpty.awaitNanos()` | `doGet()` | `notFull.signal()` |
| `ack()` | `lock` | Position 匹配 | `cleanUntil()` | `notFull.signal()` |
| `rollback()` | `lock` | 无 | `getSequence = ackSequence` | `notFull.signal()` |

---

## 第四章：四模块协作关系总结

### 4.1 完整数据流转全链路

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                  Canal Instance 内部完整数据流转全链路                             │
│                                                                                   │
│  MySQL Binlog                                                                     │
│       │                                                                           │
│       v                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                        Parser 线程                                       │     │
│  │                                                                           │     │
│  │  MysqlConnection.dump()                                                  │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  BinaryLogClient (仿Slave协议)                                            │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  LogEvent (MySQL Binlog事件)                                              │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  LogEventConvert -> CanalEntry.Entry (Protobuf)                           │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  List<Event> (事件列表)                                                   │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  ─── 调用 Sink ───                                                       │     │
│  └─────────────────────────────────┬───────────────────────────────────────┘     │
│                                    │                                               │
│                                    v                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                        Sink (Parser线程内)                                │     │
│  │                                                                           │     │
│  │  EntryEventSink.sink(events)                                              │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  doFilter(events)                                                         │     │
│  │       │                                                                   │     │
│  │       ├── 1a. ROWDATA正则过滤 (AviaterRegexFilter)                       │     │
│  │       │       ├── RegexFunction.call()                                   │     │
│  │       │       └── PatternUtils.getPattern() -> Perl5Matcher.matches()     │     │
│  │       │                                                                   │     │
│  │       └── 1b. 空事务过滤                                                   │     │
│  │               ├── filterTransactionEntry() (第一级)                       │     │
│  │               └── filterEmtryTransactionEntry() (第二级)                  │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  doSink(filteredEvents)                                                   │     │
│  │       │                                                                   │     │
│  │       ├── 2a. handler.before(events)                                     │     │
│  │       │       └── HeartBeatEntryEventHandler (心跳优先)                   │     │
│  │       │                                                                   │     │
│  │       ├── 2b. applyWait(events) (渐进式退避)                              │     │
│  │       │       ├── Thread.yield() (前1次)                                  │     │
│  │       │       ├── parkNanos(递增) (2~10次)                               │     │
│  │       │       └── parkNanos(10ms) (封顶)                                 │     │
│  │       │                                                                   │     │
│  │       ├── 2c. eventStore.tryPut() / eventStore.put()                     │     │
│  │       │                                                                   │     │
│  │       └── 2d. handler.after(events)                                      │     │
│  │                                                                           │     │
│  │  GroupEventSink (多源归并):                                               │     │
│  │  barrier.await(event) -> 归并排序 -> super.doSink() -> barrier.clear()    │     │
│  └─────────────────────────────────┬───────────────────────────────────────┘     │
│                                    │                                               │
│                                    v                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                        Store (共享 RingBuffer)                             │     │
│  │                                                                           │     │
│  │  Put (Parser线程):                                                       │     │
│  │  checkFreeSlotAt(1) -> doPut(event) -> notEmpty.signal()                 │     │
│  │                                                                           │     │
│  │  Get (Client线程):                                                       │     │
│  │  checkUnGetSlotAt() -> doGet() -> notFull.signal()                       │     │
│  │                                                                           │     │
│  │  Ack (Client线程):                                                       │     │
│  │  cleanUntil(position) -> ackSequence CAS -> notFull.signal()             │     │
│  │                                                                           │     │
│  │  Rollback (Client线程):                                                  │     │
│  │  getSequence = ackSequence -> notFull.signal()                           │     │
│  │                                                                           │     │
│  │  Scavenge (定时线程):                                                    │     │
│  │  scheduler -> doScavenge()                                               │     │
│  └─────────────────────────────────┬───────────────────────────────────────┘     │
│                                    │                                               │
│                                    v                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐     │
│  │                        Client 线程                                       │     │
│  │                                                                           │     │
│  │  CanalConnector.getWithoutAck(batchSize) -> Events                      │     │
│  │       │                                                                   │     │
│  │       v                                                                   │     │
│  │  处理事件 (业务逻辑)                                                       │     │
│  │       │                                                                   │     │
│  │       ├── 成功 -> ack(position)                                           │     │
│  │       └── 失败 -> rollback()                                              │     │
│  └─────────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 关键设计模式汇总表

| 设计模式 | 应用位置 | 具体实现 | 设计意图 |
|---------|---------|---------|---------|
| **模板方法** | `AbstractCanalEventSink` -> `EntryEventSink` | `sink()` 调用 `sinkData()`，`sinkData()` 调用 `doFilter()` + `doSink()` | 固定处理骨架，子类按需覆盖 |
| **模板方法** | `EntryEventSink` -> `GroupEventSink` | `doSink()` 被重写，增加 barrier 归并 | 父类投递逻辑上增加归并排序 |
| **责任链** | `CanalEventDownStreamHandler` | `before()` -> `tryPut/put` -> `after()` / `retry()` | 投递前后可插拔处理逻辑 |
| **策略模式** | `CanalEventFilter` -> `AviaterRegexFilter` 等 | 同一接口不同实现 | 支持多种过滤策略切换 |
| **屏障模式** | `GroupBarrier` -> `TimelineBarrier` | `await()` -> `single()` -> `isPermit()` -> `notify()` | 多源归并排序保证时间顺序 |
| **状态机** | `TimelineTransactionBarrier` | `txState` 三态(0/1/2) + CAS | 事务感知归并，防止事务被拆散 |
| **生产者-消费者** | `MemoryEventStoreWithBuffer` | `put()` + `get()` + `ReentrantLock` + `Condition` | 解耦生产消费，缓冲速度差异 |
| **环形缓冲区** | `MemoryEventStoreWithBuffer` | `entries[]` + `indexMask` + 位与取模 | 固定大小数组复用 |
| **三指针模型** | `MemoryEventStoreWithBuffer` | `putSequence` / `getSequence` / `ackSequence` | 支持回滚和批量ACK |
| **双重检查** | `PatternUtils.getPattern()` | `WeakHashMap.get()` + `synchronized` | 避免并发重复编译正则 |
| **CAS 无锁** | `MemoryEventStoreWithBuffer` | `getSequence.compareAndSet()` | 防止并发 get/ack 错乱 |
| **渐进式退避** | `EntryEventSink.applyWait()` | `yield` -> `parkNanos(递增)` -> `parkNanos(封顶)` | Store满时智能等待 |
| **软引用缓存** | `PatternUtils` | `WeakHashMap` | 避免重复编译，GC可回收 |
| **ThreadLocal** | `TimelineTransactionBarrier` | `ThreadLocal<Boolean> inTransaction` | 每通道独立事务状态 |

### 4.3 并发模型总结

```
┌────────────────────────────────────────────────────────────────────┐
│                      Canal Store 并发模型                            │
│                                                                      │
│  Parser线程 (1个)                Client线程 (1~N个)                   │
│  ┌──────────────────┐          ┌──────────────────────┐            │
│  │  put() / tryPut() │          │  get() / tryGet()     │            │
│  │       │           │          │       │               │            │
│  │       v           │          │       v               │            │
│  │  doPut()          │          │  doGet()              │            │
│  │  写entries[]      │          │  读entries[]          │            │
│  │  更新putSequence  │          │  更新getSequence(CAS) │            │
│  │  notEmpty.signal()│          │  notFull.signal()     │            │
│  │       │           │          │       │               │            │
│  │       v           │          │       v               │            │
│  │  ┌─────────────────────────────────────────────────┐ │            │
│  │  │              ReentrantLock                       │ │            │
│  │  │  ┌────────────────┐  ┌─────────────────┐       │ │            │
│  │  │  │ notFull        │  │ notEmpty        │       │ │            │
│  │  │  │ (Condition)    │  │ (Condition)     │       │ │            │
│  │  │  │ put等待:       │  │ get等待:        │       │ │            │
│  │  │  │ buffer满时阻塞 │  │ buffer空时阻塞  │       │ │            │
│  │  │  └────────────────┘  └─────────────────┘       │ │            │
│  │  └─────────────────────────────────────────────────┘ │            │
│  │                   │          │                       │            │
│  │           ┌──────────────────────────┐              │            │
│  │           │   entries[] RingBuffer    │              │            │
│  │           │   putSequence (AtomicLong) │              │            │
│  │           │   getSequence (AtomicLong) │              │            │
│  │           │   ackSequence (AtomicLong) │              │            │
│  │           └──────────────────────────┘              │            │
│  └──────────────────┘          └──────────────────────┘            │
│                                                                      │
│  Scavenge线程 (1个, daemon):                                         │
│  ScheduledExecutorService -> 每隔 scavengeInterval 执行 doScavenge  │
│                                                                      │
│  并发保证:                                                           │
│  1. ReentrantLock: put/get/ack/rollback 互斥                         │
│  2. AtomicLong + volatile: 指针的原子更新和可见性                     │
│  3. CAS: getSequence/ackSequence 的乐观锁                            │
│  4. Condition: notFull/notEmpty 的等待/通知                          │
│  5. 先写数据再更新指针: 防止脏读                                      │
└────────────────────────────────────────────────────────────────────┘
```

### 4.4 关键设计决策与权衡

| 设计决策 | 选择 | 权衡 | 理由 |
|---------|------|------|------|
| RingBuffer大小 | 2的幂 | 位与取模 vs 取模 | 性能优先，位与比取模快 |
| 指针类型 | AtomicLong | 原子操作 vs 普通long | 并发安全，CAS保护 |
| 锁策略 | ReentrantLock | ReentrantLock vs synchronized | Condition支持(notFull/notEmpty) |
| 过滤引擎 | Aviator + ORO | Aviator vs Java正则 | Aviator灵活，ORO Perl5兼容性好 |
| 正则缓存 | WeakHashMap | WeakHashMap vs HashMap | 防内存泄漏，GC可回收 |
| 退避策略 | 渐进式 | 渐进式 vs 固定等待 | 平衡CPU使用率和响应速度 |
| 事务归并 | 状态机+CAS | 状态机 vs 锁 | CAS无锁更高效 |
| ACK语义 | at-least-once | at-least-once vs exactly-once | 简化实现，客户端幂等处理 |
| 内存控制 | 双模式 | ITEMSIZE vs MEMSIZE | 灵活适配不同场景 |
| DDL处理 | 隔离获取 | 隔离 vs 混合 | DDL改变表结构，需单独处理 |

### 4.5 总结

Canal 的 Filter -> Sink -> Store 三模块协作形成了一个高效的数据管道：

1. **Filter** 使用 Aviator 表达式引擎 + ORO Perl5 正则，通过 WeakHashMap 缓存编译后的 Pattern，实现高效的 schema/table 过滤。`splitPattern` 的括号深度感知拆分和从长到短排序是两个精妙的设计。

2. **Sink** 在 Filter 基础上增加了空事务双级过滤和 Handler 责任链。`applyWait()` 的渐进式退避策略（yield -> parkNanos递增 -> 10ms封顶）在生产速度超过消费速度时提供了智能等待。`GroupEventSink` 通过 `TimelineBarrier` 实现多源归并排序，`TimelineTransactionBarrier` 的三态状态机保证了事务原子性。

3. **Store** 的三指针 RingBuffer 是核心创新。`putSequence` / `getSequence` / `ackSequence` 三个指针划分出四个区间（已确认/已读未确认/已写未读/空闲），支持 rollback 的 at-least-once 语义。先写数据再更新指针的顺序保证防脏读。MEMSIZE 模式的双重检查（指针+内存）防止单次获取过多大事件导致内存溢出。

这三个模块的设计充分体现了分布式系统中的经典模式：生产者-消费者、环形缓冲区、屏障归并、状态机、CAS 无锁等，是学习中间件设计的优秀范例。

---

> **文档版本**: 1.0
> **覆盖源码版本**: Canal 1.1.x
> **核心源码文件数**: 24
> **核心类数**: 15
> **核心方法数**: 30+