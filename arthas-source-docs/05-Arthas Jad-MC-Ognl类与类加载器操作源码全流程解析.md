# Arthas Jad/MC/Ognl 类与类加载器操作源码全流程解析

> 本文基于 Arthas 开源项目源码进行分析，源码根目录为 `/Users/zhanghonghao/Desktop/RaftByJava/tmp-source-reading/arthas`。本文将深入剖析 Arthas 中与**类操作**和**类加载器**相关的全部核心命令——`jad`（反编译）、`mc`（内存编译）、`ognl`（表达式执行）、`sc`（类搜索）、`dump`（字节码导出）、`redefine`（热替换）、`classloader`（类加载器查看）、`getstatic`（静态字段查看）的源码实现，覆盖从命令入口到底层引擎的每一步调用。

---

## 全局调用链总览

在深入分析每个命令之前，先给出一张完整的 ASCII 调用链路总览图，展示各命令从用户输入到底层 JVM 操作的完整流程：

```
用户输入命令
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                          Arthas Shell 命令分发层                                      │
│  CommandProcess.process() ──→ AnnotatedCommand.process()                              │
└──────────┬───────────┬──────────┬──────────┬──────────┬──────────┬───────────┬────────┘
           │           │          │          │          │          │           │
     ┌─────▼────┐ ┌────▼───┐ ┌───▼──┐ ┌────▼───┐ ┌───▼────┐ ┌──▼────────┐ ┌▼──────────┐
     │ jad      │ │ mc     │ │ ognl │ │ sc     │ │ dump   │ │ redefine  │ │classloader│
     │ Command  │ │Command │ │Cmd   │ │Command │ │ClassCmd│ │ Command   │ │  Command  │
     └────┬─────┘ └───┬────┘ └──┬───┘ └───┬────┘ └───┬────┘ └─────┬─────┘ └─────┬─────┘
          │           │         │         │          │            │              │
          ▼           │         │         │          │            │              │
  ┌───────────────┐   │         │         │          │            │              │
  │ SearchUtils   │◄──┼─────────┼─────────┼──────────┤            │              │
  │ .searchClass()│   │         │         │          │            │              │
  └───────┬───────┘   │         │         │          │            │              │
          │           │         │         │          │            │              │
          ▼           │         │         ▼          │            │              │
  ┌───────────────┐   │         │  ┌────────────┐    │            │              │
  │ClassDump      │   │         │  │Instrument- │    │            │              │
  │Transformer    │◄──┼─────────┼──┤ation.get   │◄───┤            │              │
  │ (字节码dump)  │   │         │  │AllLoaded   │    │            │              │
  └───────┬───────┘   │         │  │Classes()   │    │            │              │
          │           │         │  └────────────┘    │            │              │
          ▼           │         │                    │            │              │
  ┌───────────────┐   │         │                    │            │              │
  │Instrumentation│   │         │                    │            │              │
  │Utils.retrans  │◄──┼─────────┼────────────────────┤            │              │
  │formClasses()  │   │         │                    │            │              │
  └───────┬───────┘   │         │                    │            │              │
          │           │         │                    │            │              │
          ▼           │         ▼                    │            ▼              │
  ┌───────────────┐   │  ┌─────────────┐             │   ┌──────────────┐       │
  │ Decompiler    │   │  │ExpressFactory│             │   │Instrumentation│      │
  │ (cfr反编译)   │   │  │.unpooled    │             │   │.redefine     │       │
  │ .decompile    │   │  │Express()    │             │   │Classes()     │       │
  │ WithMappings()│   │  └──────┬──────┘             │   └──────────────┘       │
  └───────────────┘   │         │                    │                          │
                      │         ▼                    │                          │
                      │  ┌─────────────┐             │                          │
                      │  │OgnlExpress  │             │                          │
                      │  │.get(expr)   │             │                          │
                      │  └──────┬──────┘             │                          │
                      │         │                    │                          │
                      │         ▼                    │                          │
                      │  ┌─────────────┐             │                          │
                      │  │Ognl.getValue│             │                          ▼
                      │  │(OGNL引擎)   │             │                  ┌──────────────┐
                      │  └─────────────┘             │                  │ClassLoader   │
                      │                              │                  │Utils         │
                      ▼                              │                  │.getAllClass   │
               ┌─────────────┐                       │                  │Loader()      │
               │DynamicCompiler│                     │                  └──────────────┘
               │ (javax.tools │                      │
               │  JavaCompiler)│                     │
               └──────┬───────┘                      │
                      │                              │
                      ▼                              │
               ┌─────────────┐                       │
               │DynamicClass │                       │
               │Loader       │                       │
               │(内存类加载)  │                       │
               └─────────────┘                       │
                                                     │
           ┌─────────────────────────────────────────┘
           │
           ▼
    ┌──────────────────────────────────────────────────┐
    │         ClassLoaderUtils 工具层                    │
    │  getClassLoader() / getClassLoaderByClassName()   │
    │  getAllClassLoader() / getUrls()                   │
    └──────────────────────────────────────────────────┘
           │
           ▼
    ┌──────────────────────────────────────────────────┐
    │         Instrumentation API (JVM层)               │
    │  getAllLoadedClasses() / retransformClasses()      │
    │  redefineClasses() / addTransformer()              │
    └──────────────────────────────────────────────────┘
```

**调用链说明**：

1. 所有命令都从 `AnnotatedCommand.process(CommandProcess)` 入口开始
2. 类搜索类命令（`jad`、`sc`、`dump`、`getstatic`）共享 `SearchUtils.searchClass()` 搜索引擎
3. 字节码获取类命令（`jad`、`dump`）共享 `ClassDumpTransformer` + `InstrumentationUtils.retransformClasses()`
4. 表达式类命令（`ognl`、`getstatic`）共享 `ExpressFactory` + `OgnlExpress` 引擎
5. 所有需要指定 ClassLoader 的命令共享 `ClassLoaderUtils` 工具类
6. `mc` 命令走独立的 `DynamicCompiler` 内存编译链路
7. `redefine` 命令直接调用 `Instrumentation.redefineClasses()`

---

## 第一阶段：公共基础设施 —— 类搜索与ClassLoader工具

在分析各个具体命令之前，我们先剖析它们共同依赖的基础设施层。这就像建房子之前先打地基——`SearchUtils`、`ClassLoaderUtils`、`ClassUtils` 这三个工具类是所有类操作命令的基石。

### 1.1 SearchUtils —— 类搜索引擎

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/util/SearchUtils.java`

SearchUtils 是 Arthas 中最核心的工具类之一，几乎所有与类相关的命令都要用它来搜索 JVM 中已加载的类。它的核心思路非常直接：**遍历 `Instrumentation.getAllLoadedClasses()` 返回的全部已加载类，逐个进行名称匹配**。

#### 1.1.1 基础搜索方法 searchClass(Instrumentation, Matcher, int)

```java
public static Set<Class<?>> searchClass(Instrumentation inst, 
        Matcher<String> classNameMatcher, int limit) {
    if (classNameMatcher == null) {
        return Collections.emptySet();
    }
    final Set<Class<?>> matches = new HashSet<Class<?>>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        if (clazz == null) {
            continue;   
        }
        if (classNameMatcher.matching(clazz.getName())) {
            matches.add(clazz);
        }
        if (matches.size() >= limit) {
            break;
        }
    }
    return matches;
}
```

**逐行解释**：

- 第1行：方法接收三个参数——`Instrumentation` 实例（JVM提供的字节码操作接口）、`Matcher<String>` 类名匹配器、`int limit` 最大匹配数量
- 第2-4行：空匹配器校验，直接返回空集合
- 第5行：创建 `HashSet` 存放匹配结果，用 Set 保证不重复
- 第6行：**关键调用** `inst.getAllLoadedClasses()` —— 这个 JVM API 返回当前 JVM 中**所有已加载的类**，包括系统类、框架类、业务类，数量可能达到数万甚至数十万
- 第7-8行：空指针防护，JVM 在特殊情况下可能返回 null 元素
- 第9-10行：用 Matcher 对类的全限定名进行匹配
- 第12-14行：达到上限后提前退出，防止匹配过多导致性能问题

**它为什么存在？** 在运行中的 JVM 里，我们无法像在文件系统中那样按路径找到一个类。类一旦被加载，就只存在于 JVM 的内存中。`Instrumentation.getAllLoadedClasses()` 是 JDK 提供的唯一能获取所有已加载类的 API，因此所有类搜索都必须基于它实现。

#### 1.1.2 带ClassLoader过滤的搜索

```java
public static Set<Class<?>> searchClass(Instrumentation inst, 
        String classPattern, boolean isRegEx, String code) {
    Set<Class<?>> matchedClasses = searchClass(inst, classPattern, isRegEx);
    return filter(matchedClasses, code);
}
```

这个重载版本在类名匹配的基础上，增加了 ClassLoader 的 hashCode 过滤。参数 `code` 是 ClassLoader 的十六进制 hashCode 字符串。

```java
private static Set<Class<?>> filter(Set<Class<?>> matchedClasses, String code) {
    if (code == null) {
        return matchedClasses;
    }
    Set<Class<?>> result = new HashSet<Class<?>>();
    if (matchedClasses != null) {
        for (Class<?> c : matchedClasses) {
            if (c.getClassLoader() != null && 
                Integer.toHexString(c.getClassLoader().hashCode()).equals(code)) {
                result.add(c);
            }
        }
    }
    return result;
}
```

**逐行解释**：

- 第1-3行：如果没有指定 ClassLoader hashCode，直接返回所有匹配结果
- 第7-10行：遍历匹配结果，只保留 ClassLoader 的 hashCode 与指定值相同的类
- 注意第8行：`c.getClassLoader() != null` —— BootstrapClassLoader 加载的类返回 null，会被过滤掉

**为什么需要按 ClassLoader 过滤？** 在复杂应用中（如 OSGI、热部署容器），同一个类名可能被不同的 ClassLoader 加载了多次，形成多个不同的 Class 对象。通过 ClassLoader hashCode 过滤，用户可以精确定位到特定 ClassLoader 下的那个类。

#### 1.1.3 searchClassOnly vs searchClass 的区别

```java
public static Set<Class<?>> searchClass(Instrumentation inst, 
        String classPattern, boolean isRegEx) {
    Matcher<String> classNameMatcher = classNameMatcher(classPattern, isRegEx);
    return GlobalOptions.isDisableSubClass 
        ? searchClass(inst, classNameMatcher) 
        : searchSubClass(inst, searchClass(inst, classNameMatcher));
}

public static Set<Class<?>> searchClassOnly(Instrumentation inst, 
        String classPattern, boolean isRegEx) {
    Matcher<String> classNameMatcher = classNameMatcher(classPattern, isRegEx);
    return searchClass(inst, classNameMatcher);
}
```

**关键区别**：

| 方法 | 是否搜索子类 | 使用场景 |
|------|-------------|---------|
| `searchClass()` | 是（默认），受 `GlobalOptions.isDisableSubClass` 控制 | `sc`、`dump` 等需要看到继承体系的命令 |
| `searchClassOnly()` | 否，只匹配精确类名 | `jad`、`ognl`、`getstatic` 等只需要精确类的命令 |

`jad` 命令使用 `searchClassOnly()` 是因为反编译只需要精确的一个类，不需要把它的所有子类也找出来。

#### 1.1.4 子类搜索 searchSubClass

```java
public static Set<Class<?>> searchSubClass(Instrumentation inst, 
        Set<Class<?>> classSet) {
    final Set<Class<?>> matches = new HashSet<Class<?>>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        if (clazz == null) {
            continue;   
        }
        for (Class<?> superClass : classSet) {
            if (superClass.isAssignableFrom(clazz)) {
                matches.add(clazz);
                break;
            }
        }
    }
    return matches;
}
```

**逐行解释**：

- 再次遍历 JVM 中所有已加载的类
- 对每个类，检查它是否是目标类集合中任意一个类的子类（`isAssignableFrom` 判断继承/实现关系）
- 这是一个 O(N*M) 的操作，N 是 JVM 中所有类的数量，M 是目标类集合大小

#### 1.1.5 类名匹配器构造

```java
public static Matcher<String> classNameMatcher(String classPattern, 
        boolean isRegEx) {
    if (StringUtils.isEmpty(classPattern)) {
        classPattern = isRegEx ? ".*" : "*";
    }
    if (!classPattern.contains("$$Lambda")) {
        classPattern = StringUtils.replace(classPattern, "/", ".");
    }
    return isRegEx 
        ? new RegexMatcher(classPattern) 
        : new WildcardMatcher(classPattern);
}
```

**逐行解释**：

- 第2-4行：空模式的默认处理——正则模式默认匹配所有 `.*`，通配符模式默认 `*`
- 第5-7行：将路径分隔符 `/` 转换为包分隔符 `.`，但 Lambda 类名中的 `$$Lambda` 不做转换（因为 Lambda 类名本身包含 `$` 等特殊字符）
- 第8-10行：根据 `isRegEx` 标志选择正则匹配器或通配符匹配器

**为什么要支持 `/` 分隔符？** 因为在 JVM 字节码层面，类名使用 `/` 作为分隔符（如 `java/lang/String`），用户可能习惯用这种格式输入，所以需要自动转换。

### 1.2 ClassLoaderUtils —— 类加载器工具

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/util/ClassLoaderUtils.java`

ClassLoaderUtils 提供了一系列查找和操作 ClassLoader 的工具方法。在 Arthas 中，几乎所有需要指定 ClassLoader 的命令都依赖这个工具类。

#### 1.2.1 获取所有ClassLoader

```java
public static Set<ClassLoader> getAllClassLoader(Instrumentation inst) {
    Set<ClassLoader> classLoaderSet = new HashSet<ClassLoader>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        ClassLoader classLoader = clazz.getClassLoader();
        if (classLoader != null) {
            classLoaderSet.add(classLoader);
        }
    }
    return classLoaderSet;
}
```

**这一步做了什么？** 遍历所有已加载的类，收集它们的 ClassLoader。由于用的是 `HashSet`，重复的 ClassLoader 引用会自动去重。BootstrapClassLoader 加载的类返回 `null`，被跳过。

**为什么不直接有一个 API 获取所有 ClassLoader？** JDK 的 `Instrumentation` 接口只提供了 `getAllLoadedClasses()` 方法，没有直接获取所有 ClassLoader 的 API。所以必须通过"遍历所有类→收集其ClassLoader"的间接方式来实现。

#### 1.2.2 按hashCode查找ClassLoader

```java
public static ClassLoader getClassLoader(Instrumentation inst, String hashCode) {
    if (hashCode == null || hashCode.isEmpty()) {
        return null;
    }
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        ClassLoader classLoader = clazz.getClassLoader();
        if (classLoader != null) {
            if (Integer.toHexString(classLoader.hashCode()).equals(hashCode)) {
                return classLoader;
            }
        }
    }
    return null;
}
```

**逐行解释**：

- 遍历所有已加载类（又一次完整遍历）
- 对每个类的 ClassLoader，计算其 hashCode 的十六进制表示
- 与用户传入的 hashCode 字符串比较，匹配则返回

**为什么用 hashCode 而不是类名来标识 ClassLoader？** 因为同一个 ClassLoader 类（如 `URLClassLoader`）可能有多个实例，类名无法区分。而每个 ClassLoader 实例的 `hashCode()` 通常是唯一的（除非 hashCode 冲突），可以精确标识特定实例。

#### 1.2.3 按类名查找ClassLoader

```java
public static List<ClassLoader> getClassLoaderByClassName(
        Instrumentation inst, String classLoaderClassName) {
    if (classLoaderClassName == null || classLoaderClassName.isEmpty()) {
        return null;
    }
    Set<ClassLoader> classLoaderSet = getAllClassLoader(inst);
    List<ClassLoader> matchClassLoaders = new ArrayList<ClassLoader>();
    for (ClassLoader classLoader : classLoaderSet) {
        if (classLoader.getClass().getName().equals(classLoaderClassName)) {
            matchClassLoaders.add(classLoader);
        }
    }
    return matchClassLoaders;
}
```

**这一步做了什么？** 先获取所有 ClassLoader 实例，然后按 ClassLoader 自身的类名进行过滤。返回的是 List 而非单个对象，因为同一个 ClassLoader 类可能有多个实例。

这个方法被 `--classLoaderClass` 选项使用。当用户通过 `--classLoaderClass` 指定 ClassLoader 时：
- 如果找到唯一一个实例，直接使用
- 如果找到多个实例，提示用户用 `-c` 指定具体的 hashCode
- 如果找不到，报错

#### 1.2.4 获取ClassLoader的URL列表

```java
public static URL[] getUrls(ClassLoader classLoader) {
    if (classLoader instanceof URLClassLoader) {
        try {
            return ((URLClassLoader) classLoader).getURLs();
        } catch (Throwable e) {
            logger.error("classLoader: {} getUrls error", classLoader, e);
        }
    }
    // jdk9
    if (classLoader.getClass().getName()
            .startsWith("jdk.internal.loader.ClassLoaders$")) {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            sun.misc.Unsafe unsafe = (sun.misc.Unsafe) field.get(null);
            // ... 通过 Unsafe 反射获取 ucp 字段中的 URL 列表
        } catch (Throwable e) {
            return null;
        }
    }
    return null;
}
```

**这一步做了什么？** 获取一个 ClassLoader 的 classpath URL 列表。分两种情况：

1. **JDK 8 及之前**：ClassLoader 通常是 `URLClassLoader` 的子类，直接调用 `getURLs()` 即可
2. **JDK 9+**：AppClassLoader 不再继承 URLClassLoader，需要通过 `sun.misc.Unsafe` 来强行读取内部的 `ucp` 字段

这个方法主要被 `classloader` 命令的 `-u`（URL统计）选项使用。

### 1.3 ClassUtils —— 类信息工具

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/util/ClassUtils.java`

ClassUtils 主要负责两件事：一是从 `Class` 对象中提取各种元信息构造视图对象（VO），二是提供一些类相关的辅助方法。

#### 1.3.1 getCodeSource —— 获取类的代码来源

```java
public static String getCodeSource(final CodeSource cs) {
    if (null == cs || null == cs.getLocation() 
            || null == cs.getLocation().getFile()) {
        return Constants.EMPTY_STRING;
    }
    return cs.getLocation().getFile();
}
```

**这一步做了什么？** 从 `CodeSource` 对象中提取类所在的 jar 包或目录路径。这就是 `jad` 命令输出中 `Location` 信息的来源。比如一个来自 Spring 的类，其 CodeSource 可能是 `/path/to/spring-core-5.3.jar`。

#### 1.3.2 isLambdaClass —— Lambda类判断

```java
public static boolean isLambdaClass(Class<?> clazz) {
    return clazz.getName().contains("$$Lambda");
}
```

Lambda 类是 JDK 8+ 中由 `LambdaMetafactory` 在运行时动态生成的，其类名包含 `$$Lambda` 标记。这个判断用于在 `retransformClasses` 时跳过 Lambda 类（JDK 不支持对 Lambda 类进行 retransform）。

#### 1.3.3 createClassInfo —— 构造详细类信息

```java
public static ClassDetailVO createClassInfo(Class clazz, 
        boolean withFields, Integer expand) {
    CodeSource cs = clazz.getProtectionDomain().getCodeSource();
    ClassDetailVO classInfo = new ClassDetailVO();
    classInfo.setName(StringUtils.classname(clazz));
    classInfo.setClassInfo(StringUtils.classname(clazz));
    classInfo.setCodeSource(ClassUtils.getCodeSource(cs));
    classInfo.setInterface(clazz.isInterface());
    classInfo.setAnnotation(clazz.isAnnotation());
    classInfo.setEnum(clazz.isEnum());
    classInfo.setAnonymousClass(clazz.isAnonymousClass());
    classInfo.setArray(clazz.isArray());
    classInfo.setLocalClass(clazz.isLocalClass());
    classInfo.setMemberClass(clazz.isMemberClass());
    classInfo.setPrimitive(clazz.isPrimitive());
    classInfo.setSynthetic(clazz.isSynthetic());
    classInfo.setSimpleName(clazz.getSimpleName());
    classInfo.setModifier(StringUtils.modifier(clazz.getModifiers(), ','));
    classInfo.setAnnotations(TypeRenderUtils.getAnnotations(clazz));
    classInfo.setInterfaces(TypeRenderUtils.getInterfaces(clazz));
    classInfo.setSuperClass(TypeRenderUtils.getSuperClass(clazz));
    classInfo.setClassloader(TypeRenderUtils.getClassloader(clazz));
    classInfo.setClassLoaderHash(StringUtils.classLoaderHash(clazz));
    if (withFields) {
        classInfo.setFields(TypeRenderUtils.getFields(clazz, expand));
    }
    return classInfo;
}
```

**这一步做了什么？** 通过 Java 反射 API，从一个 `Class` 对象中提取出所有元信息，包括类名、代码来源、是否接口/枚举/注解/匿名类/数组等、修饰符、注解列表、接口列表、父类链、ClassLoader 信息等。这个方法被 `sc -d` 命令使用，用于展示类的详细信息。

### 1.4 InstrumentationUtils —— 字节码操作工具

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/util/InstrumentationUtils.java`

这个工具类封装了 `Instrumentation.retransformClasses()` 的调用，是 `jad` 和 `dump` 命令获取运行时字节码的关键。

```java
public static void retransformClasses(Instrumentation inst, 
        ClassFileTransformer transformer, Set<Class<?>> classes) {
    try {
        inst.addTransformer(transformer, true);
        for (Class<?> clazz : classes) {
            if (ClassUtils.isLambdaClass(clazz)) {
                logger.info("ignore lambda class: {}, because jdk do not "
                    + "support retransform lambda class", clazz.getName());
                continue;
            }
            try {
                inst.retransformClasses(clazz);
            } catch (Throwable e) {
                String errorMsg = "retransformClasses class error, name: " 
                    + clazz.getName();
                logger.error(errorMsg, e);
            }
        }
    } finally {
        inst.removeTransformer(transformer);
    }
}
```

**逐行解释**：

- 第3行：`inst.addTransformer(transformer, true)` —— 注册一个 `ClassFileTransformer`，第二个参数 `true` 表示这个 transformer 可以用于 retransform 操作
- 第4-14行：遍历目标类集合，逐个调用 `inst.retransformClasses(clazz)`
- 第5-8行：跳过 Lambda 类，因为 JDK 不支持对 Lambda 类进行 retransform（参见 [Arthas Issue #1512](https://github.com/alibaba/arthas/issues/1512)）
- 第16行：**关键！** 在 `finally` 块中移除 transformer。这确保 transformer 只在本次操作期间生效，不会影响后续的类加载或 retransform

**这一步的流程可以类比为"快递取件"**：
1. 在快递柜前放一个"取件机器人"（addTransformer）
2. 让快递员把包裹重新投递一次（retransformClasses）
3. 机器人拦截到包裹，记录下包裹内容（transformer.transform）
4. 把机器人撤走（removeTransformer）

调用 `retransformClasses` 后，JVM 会触发所有已注册的 `ClassFileTransformer` 的 `transform()` 方法，并将当前类的字节码作为参数传入。`ClassDumpTransformer` 就是在这个回调中拿到了运行时字节码。

---

## 第二阶段：JadCommand —— 反编译命令

### 2.1 JadCommand —— 命令入口

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/JadCommand.java`

`jad` 是 Arthas 中最常用的命令之一，它可以将 JVM 中已加载的类**反编译为 Java 源代码**。这在排查线上问题时极其有用——你可以直接看到运行中的代码到底是什么版本，是否被热替换过。

#### 2.1.1 命令注解与参数定义

```java
@Name("jad")
@Summary("Decompile class")
@Description(Constants.EXAMPLE +
        "  jad java.lang.String\n" +
        "  jad java.lang.String toString\n" +
        "  jad java.lang.String -d /tmp/jad/dump\n" +
        "  jad --source-only java.lang.String\n" +
        "  jad -c 39eb305e org/apache/log4j/Logger\n" +
        "  jad -c 39eb305e -E org\\\\.apache\\\\.*\\\\.StringUtils\n" +
        Constants.WIKI + Constants.WIKI_HOME + "jad")
public class JadCommand extends AnnotatedCommand {
```

**注解说明**：

| 注解 | 作用 |
|------|------|
| `@Name("jad")` | 命令名称，用户在终端输入 `jad` 触发 |
| `@Summary("Decompile class")` | 命令简要描述 |
| `@Description(...)` | 包含使用示例和 Wiki 链接的详细说明 |

命令继承 `AnnotatedCommand`，这是 Arthas 的注解驱动命令框架，通过注解自动完成参数解析。

#### 2.1.2 参数字段与setter

```java
private String classPattern;        // 类名模式
private String methodName;          // 方法名（可选，反编译特定方法）
private String code = null;         // ClassLoader hashCode
private String classLoaderClass;    // ClassLoader 类名
private boolean isRegEx = false;    // 是否使用正则表达式
private boolean hideUnicode = false;// 是否隐藏 Unicode
private boolean lineNumber;         // 是否显示行号
private String directory;           // dump 目录
private boolean sourceOnly = false; // 仅输出源代码

@Argument(argName = "class-pattern", index = 0)
@Description("Class name pattern, use either '.' or '/' as separator")
public void setClassPattern(String classPattern) {
    this.classPattern = StringUtils.normalizeClassName(classPattern);
}

@Argument(argName = "method-name", index = 1, required = false)
@Description("Method name pattern, decompile a specific method")
public void setMethodName(String methodName) {
    this.methodName = methodName;
}

@Option(shortName = "c", longName = "code")
@Description("The hash code of the special class's classLoader")
public void setCode(String code) {
    this.code = code;
}
```

**参数对照表**：

| 参数 | 短选项 | 长选项 | 类型 | 说明 |
|------|--------|--------|------|------|
| class-pattern | 位置参数0 | — | String | 类名模式，支持通配符和正则 |
| method-name | 位置参数1 | — | String | 方法名（可选） |
| -c | -c | --code | String | ClassLoader 的 hashCode |
| — | — | --classLoaderClass | String | ClassLoader 的类名 |
| -E | -E | --regex | boolean | 启用正则匹配 |
| — | — | --hideUnicode | boolean | 隐藏 Unicode 字符 |
| — | — | --source-only | boolean | 仅输出源码 |
| — | — | --lineNumber | boolean | 显示行号（默认true） |
| -d | -d | --directory | String | 指定 dump 目录 |

#### 2.1.3 process() 主流程

```java
@Override
public void process(CommandProcess process) {
    if (directory != null && !FileUtils.isDirectoryOrNotExist(directory)) {
        process.end(-1, directory 
            + " :is not a directory, please check it");
        return;
    }
    Instrumentation inst = process.session().getInstrumentation();

    if (code == null && classLoaderClass != null) {
        List<ClassLoader> matchedClassLoaders = 
            ClassLoaderUtils.getClassLoaderByClassName(inst, classLoaderClass);
        if (matchedClassLoaders.size() == 1) {
            code = Integer.toHexString(
                matchedClassLoaders.get(0).hashCode());
        } else if (matchedClassLoaders.size() > 1) {
            Collection<ClassLoaderVO> classLoaderVOList = 
                ClassUtils.createClassLoaderVOList(matchedClassLoaders);
            JadModel jadModel = new JadModel()
                    .setClassLoaderClass(classLoaderClass)
                    .setMatchedClassLoaders(classLoaderVOList);
            process.appendResult(jadModel);
            process.end(-1, "Found more than one classloader by class name,"
                + " please specify classloader with '-c <classloader hash>'");
            return;
        } else {
            process.end(-1, "Can not find classloader by class name: " 
                + classLoaderClass + ".");
            return;
        }
    }
    
    Set<Class<?>> matchedClasses = SearchUtils.searchClassOnly(
        inst, classPattern, isRegEx, code);

    try {
        final RowAffect affect = new RowAffect();
        final ExitStatus status;
        if (matchedClasses == null || matchedClasses.isEmpty()) {
            status = processNoMatch(process);
        } else if (matchedClasses.size() > 1) {
            status = processMatches(process, matchedClasses);
        } else {
            Set<Class<?>> withInnerClasses = SearchUtils.searchClassOnly(
                inst, matchedClasses.iterator().next().getName() + "$*", 
                false, code);
            if(withInnerClasses.isEmpty()) {
                withInnerClasses = matchedClasses;
            }
            status = processExactMatch(
                process, affect, inst, matchedClasses, withInnerClasses);
        }
        if (!this.sourceOnly) {
            process.appendResult(new RowAffectModel(affect));
        }
        CommandUtils.end(process, status);
    } catch (Throwable e){
        logger.error("processing error", e);
        process.end(-1, "processing error");
    }
}
```

**逐段解析**：

**第一段：目录校验（第1-5行）**

如果用户通过 `-d` 指定了输出目录，先校验该路径是否是一个目录（或不存在，不存在会自动创建）。这是因为反编译时需要先将字节码 dump 到文件，cfr 反编译器需要从文件读取。

**第二段：ClassLoader 解析（第7-22行）**

这段代码处理 `--classLoaderClass` 选项。当用户只提供了 ClassLoader 类名而非 hashCode 时：
1. 通过 `ClassLoaderUtils.getClassLoaderByClassName()` 查找所有该类名的 ClassLoader 实例
2. 如果恰好一个，自动转换为 hashCode 赋值给 `code`
3. 如果多于一个，列出所有匹配的 ClassLoader 及其 hashCode，提示用户用 `-c` 精确指定
4. 如果一个都没有，报错

这种"先尝试自动解析，歧义时提示用户"的设计模式在 Arthas 中非常常见，每个命令都有类似的逻辑。

**第三段：类搜索（第24-25行）**

```java
Set<Class<?>> matchedClasses = SearchUtils.searchClassOnly(
    inst, classPattern, isRegEx, code);
```

注意这里用的是 `searchClassOnly` 而非 `searchClass`——`jad` 命令不需要搜索子类，只需要精确匹配的类。

**第四段：分流处理（第28-43行）**

根据搜索结果分三种情况：

1. **没有匹配**（`matchedClasses` 为空）→ `processNoMatch()`：提示"No class found"
2. **多个匹配**（`matchedClasses.size() > 1`）→ `processMatches()`：列出所有匹配类，提示用户精确指定
3. **恰好一个匹配** → `processExactMatch()`：执行反编译

**第五段：内部类搜索（第35-39行）**

```java
Set<Class<?>> withInnerClasses = SearchUtils.searchClassOnly(
    inst, matchedClasses.iterator().next().getName() + "$*", 
    false, code);
```

这一步非常巧妙：当找到目标类后，额外搜索它的所有内部类（通过 `类名$*` 模式匹配）。内部类需要和外部类一起 dump，因为 cfr 反编译时需要看到完整的类结构。

**为什么需要内部类？** Java 编译器会将内部类编译为独立的 `.class` 文件（如 `Outer$Inner.class`）。反编译时，cfr 需要读取这些内部类的字节码才能正确恢复出内部类的声明和引用关系。

#### 2.1.4 processExactMatch() —— 核心反编译流程

```java
private ExitStatus processExactMatch(CommandProcess process, 
        RowAffect affect, Instrumentation inst, 
        Set<Class<?>> matchedClasses, Set<Class<?>> withInnerClasses) {
    Class<?> c = matchedClasses.iterator().next();
    Set<Class<?>> allClasses = new HashSet<>(withInnerClasses);
    allClasses.add(c);
    try {
        final ClassDumpTransformer transformer;
        if (directory == null) {
            transformer = new ClassDumpTransformer(allClasses);
        } else {
            transformer = new ClassDumpTransformer(allClasses, new File(directory));
        }
        InstrumentationUtils.retransformClasses(inst, transformer, allClasses);

        Map<Class<?>, File> classFiles = transformer.getDumpResult();
        if (classFiles == null || classFiles.isEmpty()) {
            return ExitStatus.failure(-1, 
                "jad: fail to dump class file for decompiler, "
                + "make sure you have write permission of the directory \""
                + transformer.dumpDir() + "\"");
        }
        File classFile = classFiles.get(c);
        Pair<String, NavigableMap<Integer, Integer>> decompileResult = 
            Decompiler.decompileWithMappings(
                classFile.getAbsolutePath(), methodName, 
                hideUnicode, lineNumber);
        String source = decompileResult.getFirst();
        if (source != null) {
            source = pattern.matcher(source).replaceAll("");
        } else {
            source = "unknown";
        }
        JadModel jadModel = new JadModel();
        jadModel.setSource(source);
        jadModel.setMappings(decompileResult.getSecond());
        if (!this.sourceOnly) {
            jadModel.setClassInfo(ClassUtils.createSimpleClassInfo(c));
            jadModel.setLocation(ClassUtils.getCodeSource(
                c.getProtectionDomain().getCodeSource()));
        }
        process.appendResult(jadModel);
        affect.rCnt(classFiles.keySet().size());
        return ExitStatus.success();
    } catch (Throwable t) {
        logger.error("jad: fail to decompile class: " + c.getName(), t);
        return ExitStatus.failure(-1, 
            "jad: fail to decompile class: " + c.getName());
    }
}
```

**逐段详解**：

**步骤1：合并目标类和内部类（第1-3行）**

将主类和内部类合并到一个 `allClasses` 集合中，后续一起 dump。

**步骤2：创建 ClassDumpTransformer（第5-9行）**

`ClassDumpTransformer` 是一个实现了 `ClassFileTransformer` 接口的类，它的作用是在 retransform 时拦截目标类的字节码并保存到文件。如果用户指定了 `-d` 目录，使用指定目录；否则使用默认的 arthas 日志目录下的 `classdump` 子目录。

**步骤3：触发 retransform 以获取字节码（第10行）**

```java
InstrumentationUtils.retransformClasses(inst, transformer, allClasses);
```

这是整个 `jad` 命令中最关键的一步。调用 `retransformClasses` 后：
1. JVM 会重新对目标类执行所有已注册的 transformer 链
2. `ClassDumpTransformer.transform()` 被回调，拿到类的当前字节码
3. 字节码被写入临时文件

**步骤4：获取 dump 结果并校验（第12-17行）**

`transformer.getDumpResult()` 返回一个 `Map<Class<?>, File>`，key 是类，value 是对应的 `.class` 文件。如果为空说明 dump 失败（通常是权限问题）。

**步骤5：调用 cfr 反编译（第18-22行）**

```java
Pair<String, NavigableMap<Integer, Integer>> decompileResult = 
    Decompiler.decompileWithMappings(
        classFile.getAbsolutePath(), methodName, 
        hideUnicode, lineNumber);
```

将 `.class` 文件路径传给 `Decompiler.decompileWithMappings()`，cfr 引擎读取字节码并输出 Java 源代码。返回值是一个 `Pair`，包含：
- 反编译后的 Java 源码字符串
- 行号映射表（反编译后的行号 → 原始字节码中的行号）

**步骤6：清理空注释（第23-26行）**

```java
private static Pattern pattern = Pattern.compile(
    "(?m)^/\\*\\s*\\*/\\s*$" + System.getProperty("line.separator"));
// ...
source = pattern.matcher(source).replaceAll("");
```

cfr 反编译器有时会生成空注释 `/* */`，这个正则表达式将它们清除，让输出更整洁。

**步骤7：构造输出模型（第27-35行）**

将源码、行号映射、类信息、代码来源位置封装到 `JadModel` 中返回给前端展示。`--source-only` 模式下不包含类信息和位置信息，只输出纯源码。

#### 2.1.5 processMatches() —— 多类匹配处理

```java
private ExitStatus processMatches(CommandProcess process, 
        Set<Class<?>> matchedClasses) {
    String usage = "jad -c <hashcode> " + classPattern;
    String msg = " Found more than one class for: " + classPattern 
        + ", Please use " + usage;
    process.appendResult(new MessageModel(msg));

    List<ClassVO> classVOs = ClassUtils.createClassVOList(matchedClasses);
    JadModel jadModel = new JadModel();
    jadModel.setMatchedClasses(classVOs);
    process.appendResult(jadModel);

    return ExitStatus.failure(-1, msg);
}
```

当搜索到多个类时，列出所有匹配的类及其 ClassLoader 的 hashCode，提示用户使用 `jad -c <hashcode> 类名` 来精确指定。这在多 ClassLoader 环境下很常见。

### 2.2 ClassDumpTransformer —— 字节码拦截器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/ClassDumpTransformer.java`

这个类是 `jad` 和 `dump` 命令的核心组件——它通过 JVM 的 `ClassFileTransformer` 接口拦截类的字节码并保存到文件。

#### 2.2.1 类定义和构造

```java
class ClassDumpTransformer implements ClassFileTransformer {
    private Set<Class<?>> classesToEnhance;
    private Map<Class<?>, File> dumpResult;
    private File arthasLogHome;
    private File directory;

    public ClassDumpTransformer(Set<Class<?>> classesToEnhance) {
        this(classesToEnhance, null);
    }

    public ClassDumpTransformer(Set<Class<?>> classesToEnhance, 
            File directory) {
        this.classesToEnhance = classesToEnhance;
        this.dumpResult = new HashMap<Class<?>, File>();
        this.arthasLogHome = new File(LogUtil.loggingDir());
        this.directory = directory;
    }
}
```

**字段说明**：
- `classesToEnhance`：目标类集合，只拦截这些类的字节码
- `dumpResult`：dump 结果映射，key 是类，value 是保存的 `.class` 文件
- `arthasLogHome`：Arthas 日志主目录
- `directory`：用户指定的输出目录

#### 2.2.2 transform() —— 字节码拦截

```java
@Override
public byte[] transform(ClassLoader loader, String className, 
        Class<?> classBeingRedefined,
        ProtectionDomain protectionDomain, byte[] classfileBuffer)
        throws IllegalClassFormatException {
    if (classesToEnhance.contains(classBeingRedefined)) {
        dumpClassIfNecessary(classBeingRedefined, classfileBuffer);
    }
    return null;
}
```

**逐行解释**：

- 这个方法是 `ClassFileTransformer` 接口的回调，JVM 在 retransform 时调用
- 参数 `classBeingRedefined` 是当前正在被重新定义的类
- 参数 `classfileBuffer` 是该类的当前字节码（可能已经被其他 transformer 修改过）
- 第3行：检查当前类是否在目标集合中
- 第4行：如果是目标类，将字节码保存到文件
- **关键：返回 `null`**。返回 null 表示不修改字节码，类保持原样。如果返回了修改后的字节码，类的行为就会改变

**这是 `jad` 和 `dump` 命令不会修改类行为的保证**——transform 方法始终返回 null。

#### 2.2.3 dumpClassIfNecessary() —— 保存字节码到文件

```java
private void dumpClassIfNecessary(Class<?> clazz, byte[] data) {
    String className = clazz.getName();
    ClassLoader classLoader = clazz.getClassLoader();

    File dumpDir = dumpDir();
    if (!dumpDir.mkdirs() && !dumpDir.exists()) {
        logger.warn("create dump directory:{} failed.", 
            dumpDir.getAbsolutePath());
        return;
    }

    String fileName;
    if (classLoader != null) {
        fileName = classLoader.getClass().getName() + "-" 
            + Integer.toHexString(classLoader.hashCode())
            + File.separator 
            + className.replace(".", File.separator) + ".class";
    } else {
        fileName = className.replace(".", File.separator) + ".class";
    }

    File dumpClassFile = new File(dumpDir, fileName);

    try {
        FileUtils.writeByteArrayToFile(dumpClassFile, data);
        dumpResult.put(clazz, dumpClassFile);
    } catch (IOException e) {
        logger.warn("dump class:{} to file {} failed.", 
            className, dumpClassFile, e);
    }
}
```

**逐行解释**：

- 第5-9行：确保 dump 目录存在
- 第11-18行：构造文件名。**注意文件名格式**：如果类有 ClassLoader，文件路径包含 ClassLoader 的信息（`ClassLoaderName-hashCode/包路径/类名.class`）；如果是 BootstrapClassLoader 加载的，直接用包路径
- 第20行：创建目标文件对象
- 第22-23行：将字节码写入文件，并记录到 `dumpResult` 映射中

**为什么文件名要包含 ClassLoader 信息？** 因为同一个类名可能被不同的 ClassLoader 加载，如果只用类名作为文件路径，后 dump 的会覆盖先 dump 的。加上 ClassLoader 信息就能区分。

### 2.3 Decompiler —— cfr 反编译引擎封装

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/util/Decompiler.java`

Decompiler 类封装了 cfr（Class File Reader）反编译库的调用。cfr 是一个功能强大的 Java 反编译器，能够处理 Java 5 到 Java 14 的字节码。

#### 2.3.1 decompileWithMappings() —— 带行号映射的反编译

```java
public static Pair<String, NavigableMap<Integer, Integer>> 
        decompileWithMappings(String classFilePath,
        String methodName, boolean hideUnicode, 
        boolean printLineNumber) {
    final StringBuilder sb = new StringBuilder(8192);
    final NavigableMap<Integer, Integer> lineMapping = 
        new TreeMap<Integer, Integer>();

    OutputSinkFactory mySink = new OutputSinkFactory() {
        @Override
        public List<SinkClass> getSupportedSinks(SinkType sinkType, 
                Collection<SinkClass> collection) {
            return Arrays.asList(
                SinkClass.STRING, 
                SinkClass.DECOMPILED, 
                SinkClass.DECOMPILED_MULTIVER,
                SinkClass.EXCEPTION_MESSAGE, 
                SinkClass.LINE_NUMBER_MAPPING);
        }

        @Override
        public <T> Sink<T> getSink(final SinkType sinkType, 
                final SinkClass sinkClass) {
            return new Sink<T>() {
                @Override
                public void write(T sinkable) {
                    if (sinkType == SinkType.PROGRESS) {
                        return;
                    }
                    if (sinkType == SinkType.LINENUMBER) {
                        LineNumberMapping mapping = 
                            (LineNumberMapping) sinkable;
                        NavigableMap<Integer, Integer> classFileMappings = 
                            mapping.getClassFileMappings();
                        NavigableMap<Integer, Integer> mappings = 
                            mapping.getMappings();
                        if (classFileMappings != null 
                                && mappings != null) {
                            for (Entry<Integer, Integer> entry 
                                    : mappings.entrySet()) {
                                Integer srcLineNumber = 
                                    classFileMappings.get(entry.getKey());
                                lineMapping.put(
                                    entry.getValue(), srcLineNumber);
                            }
                        }
                        return;
                    }
                    sb.append(sinkable);
                }
            };
        }
    };
```

**逐段详解**：

**OutputSinkFactory 是什么？** 这是 cfr 提供的回调接口，用于接收反编译输出。Arthas 通过自定义 Sink 来收集反编译结果，而不是让 cfr 直接输出到控制台。

**getSupportedSinks()** 告诉 cfr 我们对哪些类型的输出感兴趣：
- `STRING`：反编译后的字符串
- `DECOMPILED`：已反编译的内容
- `DECOMPILED_MULTIVER`：多版本反编译内容
- `EXCEPTION_MESSAGE`：异常信息
- `LINE_NUMBER_MAPPING`：行号映射

**getSink().write()** 的分流处理：
- `PROGRESS` 类型：跳过（这是 cfr 的进度信息，如"Analysing type demo.MathGame"）
- `LINENUMBER` 类型：处理行号映射。cfr 提供了两层映射——`classFileMappings`（字节码行号→源码行号）和 `mappings`（反编译后行号→字节码行号）。Arthas 将两层映射合并为"反编译后行号→源码行号"的直接映射
- 其他类型：直接追加到 StringBuilder 中

#### 2.3.2 配置 cfr 选项

```java
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("showversion", "false");
    options.put("hideutf", String.valueOf(hideUnicode));
    options.put("trackbytecodeloc", "true");
    if (!StringUtils.isBlank(methodName)) {
        options.put("methodname", methodName);
    }
```

| 选项 | 值 | 作用 |
|------|----|------|
| `showversion` | `false` | 不显示 cfr 版本号（因为打包后的版本号不准确） |
| `hideutf` | 用户指定 | 是否隐藏 Unicode 字符 |
| `trackbytecodeloc` | `true` | 启用字节码位置跟踪，这是行号映射的前提 |
| `methodname` | 用户指定 | 只反编译指定方法 |

#### 2.3.3 执行反编译

```java
    CfrDriver driver = new CfrDriver.Builder()
        .withOptions(options)
        .withOutputSink(mySink)
        .build();
    List<String> toAnalyse = new ArrayList<String>();
    toAnalyse.add(classFilePath);
    driver.analyse(toAnalyse);

    String resultCode = sb.toString();
    if (printLineNumber && !lineMapping.isEmpty()) {
        resultCode = addLineNumber(resultCode, lineMapping);
    }

    return Pair.make(resultCode, lineMapping);
}
```

**逐行解释**：

- 第1-4行：构建 CfrDriver，传入配置选项和自定义的 OutputSink
- 第5-7行：将 `.class` 文件路径加入待分析列表，调用 `analyse()` 开始反编译
- 第9-11行：如果需要打印行号且映射表不为空，调用 `addLineNumber()` 在源码中插入行号注释

#### 2.3.4 addLineNumber() —— 行号注释插入

```java
private static String addLineNumber(String src, 
        Map<Integer, Integer> lineMapping) {
    int maxLineNumber = 0;
    for (Integer value : lineMapping.values()) {
        if (value != null && value > maxLineNumber) {
            maxLineNumber = value;
        }
    }

    String formatStr = "/*%2d*/ ";
    String emptyStr = "       ";

    if (maxLineNumber >= 1000) {
        formatStr = "/*%4d*/ ";
        emptyStr = "         ";
    } else if (maxLineNumber >= 100) {
        formatStr = "/*%3d*/ ";
        emptyStr = "        ";
    }

    StringBuilder sb = new StringBuilder();
    List<String> lines = StringUtils.toLines(src);
    int index = 0;
    for (String line : lines) {
        Integer srcLineNumber = lineMapping.get(index + 1);
        if (srcLineNumber != null) {
            sb.append(String.format(formatStr, srcLineNumber));
        } else {
            sb.append(emptyStr);
        }
        sb.append(line).append("\n");
        index++;
    }

    return sb.toString();
}
```

**这一步做了什么？** 在反编译后的每一行前面加上原始行号注释。例如：

```java
/*10*/ public class Demo {
/*11*/     private int count;
       
/*13*/     public void run() {
/*14*/         System.out.println("hello");
/*15*/     }
/*16*/ }
```

没有映射到原始行号的行（如空行、闭合大括号）只添加空格占位，保持缩进对齐。行号格式会根据最大行号自动调整宽度。

---

## 第三阶段：MemoryCompilerCommand —— 内存编译命令

### 3.1 MemoryCompilerCommand —— 命令入口

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/MemoryCompilerCommand.java`

`mc`（Memory Compiler）命令允许用户在运行中的 JVM 内**直接将 Java 源码编译为字节码**，无需安装 JDK 或使用 `javac`。这个命令与 `jad` 和 `redefine` 配合使用，形成"反编译→修改→重新编译→热替换"的完整链路。

#### 3.1.1 命令注解与参数定义

```java
@Name("mc")
@Summary("Memory compiler, compiles java files into bytecode "
    + "and class files in memory.")
@Description(Constants.EXAMPLE 
    + "  mc /tmp/Test.java\n" 
    + "  mc -c 327a647b /tmp/Test.java\n"
    + "  mc -d /tmp/output /tmp/ClassA.java /tmp/ClassB.java\n" 
    + Constants.WIKI + Constants.WIKI_HOME + "mc")
public class MemoryCompilerCommand extends AnnotatedCommand {
```

**参数对照表**：

| 参数 | 短选项 | 长选项 | 类型 | 说明 |
|------|--------|--------|------|------|
| sourcefiles | 位置参数0 | — | List<String> | Java 源文件路径列表 |
| -c | -c | --classloader | String | ClassLoader 的 hashCode |
| — | — | --classLoaderClass | String | ClassLoader 的类名 |
| — | — | --encoding | String | 源文件编码 |
| -d | -d | --directory | String | 输出目录 |

#### 3.1.2 process() 主流程

```java
@Override
public void process(final CommandProcess process) {
    RowAffect affect = new RowAffect();
    try {
        Instrumentation inst = process.session().getInstrumentation();

        if (hashCode == null && classLoaderClass != null) {
            List<ClassLoader> matchedClassLoaders = 
                ClassLoaderUtils.getClassLoaderByClassName(
                    inst, classLoaderClass);
            if (matchedClassLoaders.size() == 1) {
                hashCode = Integer.toHexString(
                    matchedClassLoaders.get(0).hashCode());
            } else if (matchedClassLoaders.size() > 1) {
                // ... 提示用户用 -c 精确指定
                return;
            } else {
                process.end(-1, "Can not find classloader");
                return;
            }
        }
        
        ClassLoader classloader = null;
        if (hashCode == null) {
            classloader = ClassLoader.getSystemClassLoader();
        } else {
            classloader = ClassLoaderUtils.getClassLoader(
                inst, hashCode);
            if (classloader == null) {
                process.end(-1, "Can not find classloader "
                    + "with hashCode: " + hashCode + ".");
                return;
            }
        }

        DynamicCompiler dynamicCompiler = 
            new DynamicCompiler(classloader);

        Charset charset = Charset.defaultCharset();
        if (encoding != null) {
            charset = Charset.forName(encoding);
        }

        for (String sourceFile : sourcefiles) {
            String sourceCode = FileUtils.readFileToString(
                new File(sourceFile), charset);
            String name = new File(sourceFile).getName();
            if (name.endsWith(".java")) {
                name = name.substring(0, 
                    name.length() - ".java".length());
            }
            dynamicCompiler.addSource(name, sourceCode);
        }

        Map<String, byte[]> byteCodes = 
            dynamicCompiler.buildByteCodes();

        File outputDir = null;
        if (this.directory != null) {
            outputDir = new File(this.directory);
        } else {
            outputDir = new File("").getAbsoluteFile();
        }

        List<String> files = new ArrayList<String>();
        for (Entry<String, byte[]> entry : byteCodes.entrySet()) {
            File byteCodeFile = new File(outputDir, 
                entry.getKey().replace('.', '/') + ".class");
            FileUtils.writeByteArrayToFile(
                byteCodeFile, entry.getValue());
            files.add(byteCodeFile.getAbsolutePath());
            affect.rCnt(1);
        }
        process.appendResult(new MemoryCompilerModel(files));
        process.appendResult(new RowAffectModel(affect));
        process.end();
    } catch (Throwable e) {
        logger.warn("Memory compiler error", e);
        process.end(-1, "Memory compiler error, exception message: " 
            + e.getMessage());
    }
}
```

**逐段详解**：

**步骤1：ClassLoader 解析（第5-20行）**

与 `jad` 命令完全相同的 ClassLoader 解析模式：
1. 如果指定了 `--classLoaderClass`，先查找对应的 ClassLoader 实例
2. 如果匹配唯一一个，自动转换为 hashCode
3. 多个匹配或找不到，报错

**步骤2：获取目标 ClassLoader（第22-32行）**

```java
ClassLoader classloader = null;
if (hashCode == null) {
    classloader = ClassLoader.getSystemClassLoader();
} else {
    classloader = ClassLoaderUtils.getClassLoader(inst, hashCode);
}
```

**为什么 `mc` 需要指定 ClassLoader？** 编译 Java 代码时，编译器需要解析代码中引用的所有类。这些类必须通过某个 ClassLoader 才能找到。如果目标类引用了特定 ClassLoader 下的类（如 Spring 容器中的类），就必须指定该 ClassLoader。

如果用户没有指定，默认使用 `SystemClassLoader`，它只能看到 classpath 下的类。

**步骤3：创建 DynamicCompiler（第34-35行）**

```java
DynamicCompiler dynamicCompiler = new DynamicCompiler(classloader);
```

将目标 ClassLoader 传入 DynamicCompiler。后面会详细分析 DynamicCompiler 的实现。

**步骤4：读取源文件并添加到编译器（第37-48行）**

```java
for (String sourceFile : sourcefiles) {
    String sourceCode = FileUtils.readFileToString(
        new File(sourceFile), charset);
    String name = new File(sourceFile).getName();
    if (name.endsWith(".java")) {
        name = name.substring(0, name.length() - ".java".length());
    }
    dynamicCompiler.addSource(name, sourceCode);
}
```

遍历用户指定的 Java 源文件，读取内容并加入编译器。文件名去掉 `.java` 后缀作为类名。

**步骤5：执行编译（第50-51行）**

```java
Map<String, byte[]> byteCodes = dynamicCompiler.buildByteCodes();
```

调用 `buildByteCodes()` 执行编译，返回 `Map<String, byte[]>`，key 是全限定类名，value 是编译后的字节码。如果编译失败会抛出 `DynamicCompilerException`。

**步骤6：保存编译结果到文件（第53-65行）**

```java
for (Entry<String, byte[]> entry : byteCodes.entrySet()) {
    File byteCodeFile = new File(outputDir, 
        entry.getKey().replace('.', '/') + ".class");
    FileUtils.writeByteArrayToFile(byteCodeFile, entry.getValue());
    files.add(byteCodeFile.getAbsolutePath());
}
```

将编译后的字节码写入 `.class` 文件。文件路径按 Java 的包结构组织，例如 `com.example.Test` 会保存到 `com/example/Test.class`。

### 3.2 DynamicCompiler —— 内存编译器核心

**源码位置**: `arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/DynamicCompiler.java`

DynamicCompiler 是基于 `javax.tools.JavaCompiler` API 实现的内存编译器。它不需要将源码写入文件系统，而是直接在内存中完成从 Java 源码到字节码的编译。

#### 3.2.1 类定义和构造

```java
public class DynamicCompiler {
    private final JavaCompiler javaCompiler = 
        ToolProvider.getSystemJavaCompiler();
    private final StandardJavaFileManager standardFileManager;
    private final List<String> options = new ArrayList<String>();
    private final DynamicClassLoader dynamicClassLoader;
    private final Collection<JavaFileObject> compilationUnits = 
        new ArrayList<JavaFileObject>();
    private final List<Diagnostic<? extends JavaFileObject>> errors = 
        new ArrayList<Diagnostic<? extends JavaFileObject>>();
    private final List<Diagnostic<? extends JavaFileObject>> warnings = 
        new ArrayList<Diagnostic<? extends JavaFileObject>>();

    public DynamicCompiler(ClassLoader classLoader) {
        if (javaCompiler == null) {
            throw new IllegalStateException(
                "Can not load JavaCompiler from "
                + "javax.tools.ToolProvider#getSystemJavaCompiler(),"
                + " please confirm the application running in JDK "
                + "not JRE.");
        }
        standardFileManager = 
            javaCompiler.getStandardFileManager(null, null, null);
        options.add("-Xlint:unchecked");
        options.add("-g");
        dynamicClassLoader = new DynamicClassLoader(classLoader);
    }
}
```

**逐行解释**：

- `ToolProvider.getSystemJavaCompiler()`：获取 JDK 自带的 Java 编译器。**注意：这只在 JDK 环境中有效，JRE 中返回 null**。这就是为什么 `mc` 命令需要目标 JVM 运行在 JDK 上
- `standardFileManager`：标准的文件管理器，用于处理磁盘上的类文件
- `options.add("-Xlint:unchecked")`：启用未检查类型的警告
- `options.add("-g")`：生成所有调试信息（包括行号、变量名等）
- `DynamicClassLoader(classLoader)`：创建自定义的 ClassLoader，以用户指定的 ClassLoader 为父加载器

**类比理解**：DynamicCompiler 就像一个"随身携带的 javac 编译器"。它不需要命令行，不需要磁盘上的源文件，只需要给它字符串形式的 Java 代码，它就能在内存中编译出字节码。

#### 3.2.2 addSource() —— 添加源码

```java
public void addSource(String className, String source) {
    addSource(new StringSource(className, source));
}

public void addSource(JavaFileObject javaFileObject) {
    compilationUnits.add(javaFileObject);
}
```

`addSource` 将 Java 源码封装为 `StringSource`（一个 `JavaFileObject` 实现），加入编译单元列表。

#### 3.2.3 buildByteCodes() —— 执行编译并返回字节码

```java
public Map<String, byte[]> buildByteCodes() {
    errors.clear();
    warnings.clear();

    JavaFileManager fileManager = new DynamicJavaFileManager(
        standardFileManager, dynamicClassLoader);

    DiagnosticCollector<JavaFileObject> collector = 
        new DiagnosticCollector<JavaFileObject>();
    JavaCompiler.CompilationTask task = javaCompiler.getTask(
        null, fileManager, collector, options, null, compilationUnits);

    try {
        if (!compilationUnits.isEmpty()) {
            boolean result = task.call();

            if (!result || collector.getDiagnostics().size() > 0) {
                for (Diagnostic<? extends JavaFileObject> diagnostic 
                        : collector.getDiagnostics()) {
                    switch (diagnostic.getKind()) {
                    case NOTE:
                    case MANDATORY_WARNING:
                    case WARNING:
                        warnings.add(diagnostic);
                        break;
                    case OTHER:
                    case ERROR:
                    default:
                        errors.add(diagnostic);
                        break;
                    }
                }
                if (!errors.isEmpty()) {
                    throw new DynamicCompilerException(
                        "Compilation Error", errors);
                }
            }
        }
        return dynamicClassLoader.getByteCodes();
    } catch (ClassFormatError e) {
        throw new DynamicCompilerException(e, errors);
    } finally {
        compilationUnits.clear();
    }
}
```

**逐段详解**：

**步骤1：创建自定义文件管理器（第5-6行）**

```java
JavaFileManager fileManager = new DynamicJavaFileManager(
    standardFileManager, dynamicClassLoader);
```

`DynamicJavaFileManager` 是关键的桥接组件。它重写了 `getJavaFileForOutput()` 方法，使得编译器输出的 `.class` 字节码不是写入磁盘文件，而是保存到 `DynamicClassLoader` 的内存中。

**步骤2：创建编译任务（第8-11行）**

```java
JavaCompiler.CompilationTask task = javaCompiler.getTask(
    null,           // Writer out: 输出流（null=标准输出）
    fileManager,    // JavaFileManager: 自定义文件管理器
    collector,      // DiagnosticListener: 诊断信息收集器
    options,        // Iterable<String>: 编译选项
    null,           // Iterable<String>: 需要参与注解处理的类名
    compilationUnits // Iterable<JavaFileObject>: 编译单元
);
```

**步骤3：执行编译（第14行）**

```java
boolean result = task.call();
```

`task.call()` 是真正触发编译的方法。编译器会：
1. 解析所有源码中的类引用
2. 通过 `DynamicJavaFileManager` 查找这些类（先找内存中的，再找 ClassLoader 能加载的）
3. 执行语法检查和语义分析
4. 生成字节码，通过 `DynamicJavaFileManager.getJavaFileForOutput()` 保存到内存

**步骤4：诊断信息处理（第16-32行）**

将编译过程中的诊断信息（错误、警告等）分类收集。如果有编译错误，抛出 `DynamicCompilerException`。

**步骤5：返回字节码（第35行）**

```java
return dynamicClassLoader.getByteCodes();
```

从 `DynamicClassLoader` 中取出编译好的字节码（`Map<String, byte[]>`）。

### 3.3 DynamicClassLoader —— 内存字节码容器

**源码位置**: `arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/DynamicClassLoader.java`

```java
public class DynamicClassLoader extends ClassLoader {
    private final Map<String, MemoryByteCode> byteCodes = 
        new HashMap<String, MemoryByteCode>();

    public DynamicClassLoader(ClassLoader classLoader) {
        super(classLoader);
    }

    public void registerCompiledSource(MemoryByteCode byteCode) {
        byteCodes.put(byteCode.getClassName(), byteCode);
    }

    @Override
    protected Class<?> findClass(String name) 
            throws ClassNotFoundException {
        MemoryByteCode byteCode = byteCodes.get(name);
        if (byteCode == null) {
            return super.findClass(name);
        }
        return super.defineClass(name, byteCode.getByteCode(), 0, 
            byteCode.getByteCode().length);
    }

    public Map<String, byte[]> getByteCodes() {
        Map<String, byte[]> result = 
            new HashMap<String, byte[]>(byteCodes.size());
        for (Entry<String, MemoryByteCode> entry 
                : byteCodes.entrySet()) {
            result.put(entry.getKey(), 
                entry.getValue().getByteCode());
        }
        return result;
    }
}
```

**逐行解释**：

- `DynamicClassLoader extends ClassLoader`：继承标准 ClassLoader
- 构造函数 `super(classLoader)`：以用户指定的 ClassLoader 为父加载器。这意味着编译时需要的依赖类可以通过父加载器找到
- `registerCompiledSource()`：由 `DynamicJavaFileManager` 调用，在编译完成后注册编译结果
- `findClass()`：标准的类加载方法。先从内存中找，找不到委托给父加载器
- `getByteCodes()`：提取所有编译好的字节码

**它为什么存在？** 它充当了一个"内存仓库"，编译器往里面存字节码，外部从中取字节码。同时它作为 ClassLoader，还能让编译器在编译过程中加载已编译的类（处理类之间的互相引用）。

### 3.4 DynamicJavaFileManager —— 内存文件管理器

**源码位置**: `arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/DynamicJavaFileManager.java`

```java
public class DynamicJavaFileManager 
        extends ForwardingJavaFileManager<JavaFileManager> {
    private final DynamicClassLoader classLoader;
    private final List<MemoryByteCode> byteCodes = 
        new ArrayList<MemoryByteCode>();

    @Override
    public JavaFileObject getJavaFileForOutput(
            JavaFileManager.Location location, String className,
            JavaFileObject.Kind kind, FileObject sibling) 
            throws IOException {
        for (MemoryByteCode byteCode : byteCodes) {
            if (byteCode.getClassName().equals(className)) {
                return byteCode;
            }
        }
        MemoryByteCode innerClass = new MemoryByteCode(className);
        byteCodes.add(innerClass);
        classLoader.registerCompiledSource(innerClass);
        return innerClass;
    }

    @Override
    public ClassLoader getClassLoader(
            JavaFileManager.Location location) {
        return classLoader;
    }

    @Override
    public Iterable<JavaFileObject> list(Location location, 
            String packageName, Set<JavaFileObject.Kind> kinds,
            boolean recurse) throws IOException {
        if (location == StandardLocation.CLASS_PATH 
                && kinds.contains(JavaFileObject.Kind.CLASS)) {
            return new IterableJoin<JavaFileObject>(
                super.list(location, packageName, kinds, recurse),
                finder.find(packageName));
        }
        return super.list(location, packageName, kinds, recurse);
    }
}
```

**逐段详解**：

**getJavaFileForOutput()** 是核心方法。当编译器编译完一个类需要输出 `.class` 文件时，会调用这个方法。正常情况下会返回一个磁盘文件对象，但这里返回的是 `MemoryByteCode`——一个基于内存的 `JavaFileObject`，编译器会把字节码写入它的内存缓冲区，而不是磁盘。

**getClassLoader()** 返回自定义的 `DynamicClassLoader`，让编译器在需要加载类时使用我们的 ClassLoader。

**list()** 重写了类查找逻辑。当编译器在 CLASS_PATH 位置查找类时，除了搜索标准位置，还会通过 `PackageInternalsFinder` 搜索用户指定的 ClassLoader 能看到的类。这保证了编译时能找到目标 JVM 中已加载的类。

### 3.5 StringSource —— 字符串源码封装

**源码位置**: `arthas/memorycompiler/src/main/java/com/taobao/arthas/compiler/StringSource.java`

```java
public class StringSource extends SimpleJavaFileObject {
    private final String contents;

    public StringSource(String className, String contents) {
        super(URI.create("string:///" 
            + className.replace('.', '/') 
            + Kind.SOURCE.extension), Kind.SOURCE);
        this.contents = contents;
    }

    @Override
    public CharSequence getCharContent(
            boolean ignoreEncodingErrors) throws IOException {
        return contents;
    }
}
```

**这一步做了什么？** 将 Java 源码字符串包装为 `JavaFileObject`，让 Java 编译器可以像读取文件一样读取它。URI 格式为 `string:///包路径/类名.java`，这只是一个虚拟 URI，实际内容来自内存中的字符串。

---

## 第四阶段：OgnlCommand —— OGNL 表达式执行命令

### 4.1 OgnlCommand —— 命令入口

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/OgnlCommand.java`

`ognl` 命令允许用户在目标 JVM 中**执行任意 OGNL 表达式**。OGNL（Object-Graph Navigation Language）是一种功能强大的表达式语言，可以访问和操作 Java 对象。通过这个命令，用户可以在线调用静态方法、访问静态字段、创建对象、调用方法等。

#### 4.1.1 命令注解与参数定义

```java
@Name("ognl")
@Summary("Execute ognl expression.")
@Description(Constants.EXAMPLE
    + "  ognl '@java.lang.System@out.println("
    + "\"hello \\u4e2d\\u6587\")' \n"
    + "  ognl -x 2 '@Singleton@getInstance()' \n"
    + "  ognl '@Demo@staticFiled' \n"
    + "  ognl '#value1=@System@getProperty(\"java.home\"), "
    + "#value2=@System@getProperty(\"java.runtime.name\"), "
    + "{#value1, #value2}'\n"
    + "  ognl -c 5d113a51 "
    + "'@com.taobao.arthas.core.GlobalOptions@isDump' \n"
    + Constants.WIKI + Constants.WIKI_HOME + "ognl\n"
    + "  https://commons.apache.org/proper/commons-ognl/"
    + "language-guide.html")
public class OgnlCommand extends AnnotatedCommand {
```

**参数对照表**：

| 参数 | 短选项 | 长选项 | 类型 | 说明 |
|------|--------|--------|------|------|
| express | 位置参数0 | — | String | OGNL 表达式（必填） |
| -c | -c | --classLoader | String | ClassLoader 的 hashCode |
| — | — | --classLoaderClass | String | ClassLoader 的类名 |
| -x | -x | --expand | int | 对象展开层级（默认1） |

#### 4.1.2 process() 主流程

```java
@Override
public void process(CommandProcess process) {
    Instrumentation inst = process.session().getInstrumentation();
    ClassLoader classLoader = null;
    if (hashCode != null) {
        classLoader = ClassLoaderUtils.getClassLoader(inst, hashCode);
        if (classLoader == null) {
            process.end(-1, "Can not find classloader "
                + "with hashCode: " + hashCode + ".");
            return;
        }
    } else if (classLoaderClass != null) {
        List<ClassLoader> matchedClassLoaders = 
            ClassLoaderUtils.getClassLoaderByClassName(
                inst, classLoaderClass);
        if (matchedClassLoaders.size() == 1) {
            classLoader = matchedClassLoaders.get(0);
        } else if (matchedClassLoaders.size() > 1) {
            // ... 提示用户精确指定
            return;
        } else {
            process.end(-1, "Can not find classloader.");
            return;
        }
    } else {
        classLoader = ClassLoader.getSystemClassLoader();
    }

    Express unpooledExpress = 
        ExpressFactory.unpooledExpress(classLoader);
    try {
        Object value = unpooledExpress.bind(new Object()).get(express);
        OgnlModel ognlModel = new OgnlModel()
                .setValue(new ObjectVO(value, expand));
        process.appendResult(ognlModel);
        process.end();
    } catch (ExpressException e) {
        logger.warn("ognl: failed execute express: " 
            + express, e);
        process.end(-1, "Failed to execute ognl, "
            + "exception message: " + e.getMessage());
    }
}
```

**逐段详解**：

**步骤1：ClassLoader 解析（第3-22行）**

`ognl` 命令的 ClassLoader 解析与其他命令略有不同：
- 如果指定了 `-c`（hashCode），直接通过 hashCode 查找
- 如果指定了 `--classLoaderClass`，通过类名查找
- **如果都没指定，使用 `SystemClassLoader`**

注意这里的顺序与 `jad` 不同：`ognl` 先检查 hashCode，再检查 classLoaderClass。而且 `ognl` 不会自动将 classLoaderClass 转换为 hashCode，而是直接获取 ClassLoader 实例。

**步骤2：创建 OGNL 表达式执行器（第24-25行）**

```java
Express unpooledExpress = ExpressFactory.unpooledExpress(classLoader);
```

`unpooledExpress` 创建一个**非池化的** Express 实例（每次调用创建新的，不复用）。这里传入 ClassLoader 是因为 OGNL 表达式中可能引用用户的类，需要通过正确的 ClassLoader 来解析。

**为什么用 unpooled 而不是 ThreadLocal 的？** 因为 `ognl` 命令是一次性执行，不需要复用。ThreadLocal 版本主要用于 `watch`、`trace` 等持续执行的命令中，避免频繁创建对象。

**步骤3：执行表达式（第27行）**

```java
Object value = unpooledExpress.bind(new Object()).get(express);
```

这一行的流程：
1. `bind(new Object())`：绑定一个空对象作为 OGNL 的根对象。对于 `ognl` 命令来说，用户通常通过 `@ClassName@field` 语法访问静态成员，根对象不太重要
2. `get(express)`：执行用户输入的 OGNL 表达式，返回结果

**为什么要 bind 一个 new Object() 而不是 null？** 这是为了解决 [Arthas Issue #2892](https://github.com/alibaba/arthas/issues/2892)。OGNL 引擎在根对象为 null 时可能抛出 NullPointerException，绑定一个空对象可以避免这个问题。

**步骤4：输出结果（第28-30行）**

```java
OgnlModel ognlModel = new OgnlModel()
        .setValue(new ObjectVO(value, expand));
process.appendResult(ognlModel);
```

将执行结果封装为 `ObjectVO`，带上用户指定的展开层级 `expand`。展开层级控制对象序列化时递归到几层。例如 `-x 1` 只显示对象的第一层属性，`-x 3` 则展开到第三层。

### 4.2 ExpressFactory —— 表达式工厂

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/express/ExpressFactory.java`

```java
public class ExpressFactory {
    private static final ThreadLocal<WeakReference<Express>> expressRef = 
        ThreadLocal.withInitial(
            () -> new WeakReference<Express>(new OgnlExpress()));

    public static Express threadLocalExpress(Object object) {
        WeakReference<Express> reference = expressRef.get();
        Express express = reference == null ? null : reference.get();
        if (express == null) {
            express = new OgnlExpress();
            expressRef.set(new WeakReference<Express>(express));
        }
        return express.reset().bind(object);
    }

    public static Express unpooledExpress(ClassLoader classloader) {
        if (classloader == null) {
            classloader = ClassLoader.getSystemClassLoader();
        }
        return new OgnlExpress(
            new ClassLoaderClassResolver(classloader));
    }
}
```

**两种创建方式对比**：

| 方法 | 用途 | ClassResolver | 生命周期 |
|------|------|---------------|----------|
| `threadLocalExpress()` | watch/trace 等高频命令 | `CustomClassResolver`（使用线程上下文ClassLoader） | ThreadLocal 复用 |
| `unpooledExpress()` | ognl 等一次性命令 | `ClassLoaderClassResolver`（使用指定ClassLoader） | 每次新建 |

**为什么 ThreadLocal 要用 WeakReference？** 代码注释中有详细说明：Express 对象由 ArthasClassLoader 加载，如果在 ThreadLocalMap 中强引用它，当 Arthas 执行 stop/detach 时，业务线程仍然持有对 Express 的强引用，导致 ArthasClassLoader 无法被 GC 回收。使用 WeakReference 打断了这条引用链。

### 4.3 OgnlExpress —— OGNL 表达式执行器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/express/OgnlExpress.java`

```java
public class OgnlExpress implements Express {
    private static final MemberAccess MEMBER_ACCESS = 
        new DefaultMemberAccess(true);
    private static final ArthasObjectPropertyAccessor 
        OBJECT_PROPERTY_ACCESSOR = 
            new ArthasObjectPropertyAccessor();

    private Object bindObject;
    private final OgnlContext context;

    public OgnlExpress() {
        this(CustomClassResolver.customClassResolver);
    }

    public OgnlExpress(ClassResolver classResolver) {
        OgnlRuntime.setPropertyAccessor(
            Object.class, OBJECT_PROPERTY_ACCESSOR);
        context = new OgnlContext(
            MEMBER_ACCESS, classResolver, null, null);
    }

    @Override
    public Object get(String express) throws ExpressException {
        try {
            return Ognl.getValue(express, context, bindObject);
        } catch (Exception e) {
            logger.error("Error during evaluating "
                + "the expression:", e);
            throw new ExpressException(express, e);
        }
    }

    @Override
    public Express bind(Object object) {
        this.bindObject = object;
        return this;
    }

    @Override
    public Express bind(String name, Object value) {
        context.put(name, value);
        return this;
    }

    @Override
    public Express reset() {
        context.clear();
        return this;
    }
}
```

**逐段详解**：

**构造函数中的三个关键配置**：

1. **MemberAccess**：`new DefaultMemberAccess(true)` —— 允许访问 private/protected/package-private 成员。这是 Arthas 能访问任意对象内部状态的关键
2. **PropertyAccessor**：`ArthasObjectPropertyAccessor` —— 自定义的属性访问器，在 strict 模式下禁止设置属性（安全限制）
3. **ClassResolver**：负责在 OGNL 表达式中解析类名

**get() 方法**：

```java
return Ognl.getValue(express, context, bindObject);
```

这一行就是整个 OGNL 执行的核心。`Ognl.getValue()` 是 Apache OGNL 库的入口方法，它：
1. 解析 OGNL 表达式为 AST（抽象语法树）
2. 在 context 上下文中求值
3. 以 bindObject 作为根对象进行导航

### 4.4 ClassLoaderClassResolver —— 类解析器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/express/ClassLoaderClassResolver.java`

```java
public class ClassLoaderClassResolver implements ClassResolver {
    private ClassLoader classLoader;
    private Map<String, Class<?>> classes = 
        new ConcurrentHashMap<String, Class<?>>(101);

    public ClassLoaderClassResolver(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public Class classForName(String className, Map context) 
            throws ClassNotFoundException {
        Class<?> result = null;
        if ((result = classes.get(className)) == null) {
            try {
                result = classLoader.loadClass(className);
            } catch (ClassNotFoundException ex) {
                if (className.indexOf('.') == -1) {
                    result = Class.forName(
                        "java.lang." + className);
                    classes.put(
                        "java.lang." + className, result);
                }
            }
            if (result == null) {
                return null;
            }
            classes.put(className, result);
        }
        return result;
    }
}
```

**逐行解释**：

- 使用用户指定的 ClassLoader 来加载类
- `ConcurrentHashMap` 作为缓存，避免重复加载
- 如果类名不包含 `.`（如 `String` 而非 `java.lang.String`），尝试在 `java.lang` 包下查找
- 这个 ClassResolver 被 `unpooledExpress()` 使用，保证 OGNL 表达式中引用的类通过正确的 ClassLoader 加载

### 4.5 CustomClassResolver —— 默认类解析器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/express/CustomClassResolver.java`

```java
public class CustomClassResolver implements ClassResolver {
    public static final CustomClassResolver customClassResolver = 
        new CustomClassResolver();

    private Map<String, Class<?>> classes = 
        new ConcurrentHashMap<String, Class<?>>(101);

    private CustomClassResolver() {}

    @Override
    public Class classForName(String className, Map context) 
            throws ClassNotFoundException {
        Class<?> result = null;
        if ((result = classes.get(className)) == null) {
            try {
                ClassLoader classLoader = Thread.currentThread()
                    .getContextClassLoader();
                if (classLoader != null) {
                    result = classLoader.loadClass(className);
                } else {
                    result = Class.forName(className);
                }
            } catch (ClassNotFoundException ex) {
                if (className.indexOf('.') == -1) {
                    result = Class.forName(
                        "java.lang." + className);
                    classes.put(
                        "java.lang." + className, result);
                }
            }
            classes.put(className, result);
        }
        return result;
    }
}
```

**与 ClassLoaderClassResolver 的区别**：

| 属性 | ClassLoaderClassResolver | CustomClassResolver |
|------|--------------------------|---------------------|
| ClassLoader 来源 | 构造函数传入（用户指定） | 当前线程的上下文 ClassLoader |
| 实例化方式 | 每次新建 | 单例模式 |
| 使用场景 | `ognl` 命令（需要指定ClassLoader） | `watch`/`trace` 等（在业务线程中执行） |

`CustomClassResolver` 使用线程上下文 ClassLoader 是因为 `watch`/`trace` 的表达式是在**业务线程中执行**的，此时线程的上下文 ClassLoader 就是业务代码的 ClassLoader，可以正确解析业务类。

### 4.6 DefaultMemberAccess —— 成员访问控制

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/express/DefaultMemberAccess.java`

```java
public class DefaultMemberAccess implements MemberAccess {
    public boolean allowPrivateAccess = false;
    public boolean allowProtectedAccess = false;
    public boolean allowPackageProtectedAccess = false;

    public DefaultMemberAccess(boolean allowAllAccess) {
        this(allowAllAccess, allowAllAccess, allowAllAccess);
    }

    @Override
    public Object setup(Map context, Object target, 
            Member member, String propertyName) {
        Object result = null;
        if (isAccessible(context, target, member, propertyName)) {
            AccessibleObject accessible = (AccessibleObject) member;
            if (!accessible.isAccessible()) {
                result = Boolean.TRUE;
                accessible.setAccessible(true);
            }
        }
        return result;
    }

    @Override
    public void restore(Map context, Object target, 
            Member member, String propertyName, Object state) {
        if (state != null) {
            ((AccessibleObject) member).setAccessible((Boolean) state);
        }
    }

    @Override
    public boolean isAccessible(Map context, Object target, 
            Member member, String propertyName) {
        int modifiers = member.getModifiers();
        boolean result = Modifier.isPublic(modifiers);
        if (!result) {
            if (Modifier.isPrivate(modifiers)) {
                result = getAllowPrivateAccess();
            } else if (Modifier.isProtected(modifiers)) {
                result = getAllowProtectedAccess();
            } else {
                result = getAllowPackageProtectedAccess();
            }
        }
        return result;
    }
}
```

**核心机制**：

- `setup()` 方法在访问一个成员之前被调用。如果成员不可访问（private/protected），通过 `setAccessible(true)` 临时打开访问权限
- `restore()` 方法在访问完成后被调用，恢复原来的访问权限
- `isAccessible()` 判断一个成员是否可以被访问

Arthas 中使用 `new DefaultMemberAccess(true)` 创建，即**允许访问所有级别的成员**（private、protected、package-private）。这是 Arthas 能深入查看对象内部状态的基础。

### 4.7 ArthasObjectPropertyAccessor —— 安全属性访问器

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/express/ArthasObjectPropertyAccessor.java`

```java
public class ArthasObjectPropertyAccessor 
        extends ObjectPropertyAccessor {

    @Override
    public Object setPossibleProperty(Map context, Object target, 
            String name, Object value) throws OgnlException {
        if (GlobalOptions.strict) {
            throw new IllegalAccessError(
                GlobalOptions.STRICT_MESSAGE);
        }
        return super.setPossibleProperty(
            context, target, name, value);
    }
}
```

**这一步做了什么？** 重写了 OGNL 的属性设置方法。当 `GlobalOptions.strict` 为 true 时，禁止通过 OGNL 表达式修改对象属性。这是一个安全机制——在严格模式下，OGNL 只能读取不能修改，防止用户误操作破坏运行中的系统。

### 4.8 Express 接口 —— 表达式抽象

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/express/Express.java`

```java
public interface Express {
    Object get(String express) throws ExpressException;
    boolean is(String express) throws ExpressException;
    Express bind(Object object);
    Express bind(String name, Object value);
    Express reset();
}
```

这个接口定义了表达式引擎的核心能力：

| 方法 | 作用 | 使用示例 |
|------|------|----------|
| `get(express)` | 执行表达式并返回结果 | `get("@System@getProperty('java.home')")` |
| `is(express)` | 执行表达式并返回布尔结果 | `is("#this instanceof String")` |
| `bind(object)` | 绑定根对象 | `bind(targetObject)` |
| `bind(name, value)` | 绑定命名变量 | `bind("cost", 100)` |
| `reset()` | 重置上下文 | 清除所有绑定 |

---

## 第五阶段：SearchClassCommand —— 类搜索命令

### 5.1 SearchClassCommand —— sc 命令

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/SearchClassCommand.java`

`sc`（Search Class）命令用于搜索 JVM 中已加载的类。它是最基础的类查看命令，可以展示类的详细信息、字段信息等。

#### 5.1.1 命令注解与参数

```java
@Name("sc")
@Summary("Search all the classes loaded by JVM")
@Description(Constants.EXAMPLE +
        "  sc -d org.apache.commons.lang.StringUtils\n" +
        "  sc -d org/apache/commons/lang/StringUtils\n" +
        "  sc -d *StringUtils\n" +
        "  sc -d -f org.apache.commons.lang.StringUtils\n" +
        "  sc -E org\\\\.apache\\\\.commons\\\\.lang\\\\.StringUtils\n" +
        Constants.WIKI + Constants.WIKI_HOME + "sc")
public class SearchClassCommand extends AnnotatedCommand {
```

**参数对照表**：

| 参数 | 短选项 | 长选项 | 类型 | 说明 |
|------|--------|--------|------|------|
| class-pattern | 位置参数0 | — | String | 类名模式 |
| -d | -d | --details | boolean | 显示详细信息 |
| -f | -f | --field | boolean | 显示成员变量 |
| -E | -E | --regex | boolean | 启用正则匹配 |
| -x | -x | --expand | Integer | 对象展开层级 |
| -c | -c | --classloader | String | ClassLoader hashCode |
| — | — | --classLoaderClass | String | ClassLoader 类名 |
| -n | -n | --limits | int | 最大展示类数（默认100） |
| -cs | -cs | --classLoaderStr | String | ClassLoader toString() 值 |

#### 5.1.2 process() 主流程

```java
@Override
public void process(final CommandProcess process) {
    RowAffect affect = new RowAffect();
    Instrumentation inst = process.session().getInstrumentation();

    if (hashCode == null && (classLoaderClass != null 
            || classLoaderToString != null)) {
        List<ClassLoader> matchedClassLoaders = 
            ClassLoaderUtils.getClassLoader(
                inst, classLoaderClass, classLoaderToString);
        // ... 与其他命令相同的 ClassLoader 解析逻辑
    }

    List<Class<?>> matchedClasses = new ArrayList<Class<?>>(
        SearchUtils.searchClass(inst, classPattern, isRegEx, hashCode));
    Collections.sort(matchedClasses, new Comparator<Class<?>>() {
        @Override
        public int compare(Class<?> c1, Class<?> c2) {
            return StringUtils.classname(c1)
                .compareTo(StringUtils.classname(c2));
        }
    });

    if (isDetail) {
        if (numberOfLimit > 0 
                && matchedClasses.size() > numberOfLimit) {
            process.end(-1, "The number of matching classes is "
                + "greater than : " + numberOfLimit);
            return;
        }
        for (Class<?> clazz : matchedClasses) {
            ClassDetailVO classInfo = ClassUtils.createClassInfo(
                clazz, isField, expand);
            process.appendResult(
                new SearchClassModel(classInfo, isDetail, isField));
        }
    } else {
        int pageSize = 256;
        ResultUtils.processClassNames(matchedClasses, pageSize, 
            new ResultUtils.PaginationHandler<List<String>>() {
                @Override
                public boolean handle(List<String> classNames, 
                        int segment) {
                    process.appendResult(
                        new SearchClassModel(classNames, segment));
                    return true;
                }
            });
    }

    affect.rCnt(matchedClasses.size());
    process.appendResult(new RowAffectModel(affect));
    process.end();
}
```

**核心流程**：

1. **ClassLoader 解析**：`sc` 命令支持三种 ClassLoader 指定方式——hashCode、classLoaderClass、classLoaderToString（`-cs` 选项通过 ClassLoader 的 `toString()` 返回值匹配）

2. **类搜索**：注意使用的是 `SearchUtils.searchClass()` 而非 `searchClassOnly()`。这意味着 `sc` 命令**默认会搜索子类**（除非 `GlobalOptions.isDisableSubClass` 为 true）

3. **排序**：搜索结果按类名字母序排列

4. **输出分为两种模式**：
   - **详细模式**（`-d`）：为每个类调用 `ClassUtils.createClassInfo()` 生成完整信息
   - **简单模式**（默认）：只输出类名列表，分批（每批256个）输出

5. **数量限制**：详细模式下有 100 个类的默认限制（可通过 `-n` 调整），防止输出过多导致客户端卡顿

**sc 与 jad 在类搜索上的区别**：

| 维度 | sc | jad |
|------|-------|------|
| 搜索方法 | `searchClass()`（含子类） | `searchClassOnly()`（不含子类） |
| 多匹配处理 | 直接列出所有匹配 | 提示用户精确指定 |
| 单匹配处理 | 展示类信息 | 反编译该类 |

---

## 第六阶段：DumpClassCommand —— 类字节码导出命令

### 6.1 DumpClassCommand —— dump 命令

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/DumpClassCommand.java`

`dump` 命令将 JVM 中已加载类的字节码保存到文件。它与 `jad` 的区别在于：`dump` 只保存原始字节码（`.class` 文件），不进行反编译。

#### 6.1.1 参数对照表

| 参数 | 短选项 | 长选项 | 类型 | 说明 |
|------|--------|--------|------|------|
| class-pattern | 位置参数0 | — | String | 类名模式 |
| -c | -c | --code | String | ClassLoader hashCode |
| — | — | --classLoaderClass | String | ClassLoader 类名 |
| -E | -E | --regex | boolean | 启用正则匹配 |
| -d | -d | --directory | String | 输出目录 |
| -l | -l | --limit | int | 最大 dump 类数（默认50） |

#### 6.1.2 process() 核心流程

```java
@Override
public void process(CommandProcess process) {
    try {
        if (directory != null 
                && !FileUtils.isDirectoryOrNotExist(directory)) {
            process.end(-1, directory 
                + " :is not a directory, please check it");
            return;
        }
        Instrumentation inst = process.session().getInstrumentation();
        // ... ClassLoader 解析（省略，与其他命令相同）
        
        Set<Class<?>> matchedClasses = SearchUtils.searchClass(
            inst, classPattern, isRegEx, code);
        final RowAffect effect = new RowAffect();
        final ExitStatus status;
        if (matchedClasses == null || matchedClasses.isEmpty()) {
            status = processNoMatch(process);
        } else if (matchedClasses.size() > limit) {
            status = processMatches(process, matchedClasses);
        } else {
            status = processMatch(process, effect, inst, matchedClasses);
        }
        process.appendResult(new RowAffectModel(effect));
        CommandUtils.end(process, status);
    } catch (Throwable e){
        logger.error("processing error", e);
        process.end(-1, "processing error");
    }
}
```

**注意 `dump` 与 `jad` 在类搜索上的区别**：`dump` 使用 `SearchUtils.searchClass()`（含子类搜索），而 `jad` 使用 `searchClassOnly()`（不含子类）。这意味着 `dump *StringUtils` 会 dump 所有 StringUtils 及其子类，匹配范围更广。因此 `dump` 有一个 `limit` 参数（默认50）来防止一次 dump 太多类。

#### 6.1.3 dump() 方法 —— 字节码导出

```java
private Map<Class<?>, File> dump(Instrumentation inst, 
        Set<Class<?>> classes) throws UnmodifiableClassException {
    ClassDumpTransformer transformer = null;
    if (directory != null) {
        transformer = new ClassDumpTransformer(
            classes, new File(directory));
    } else {
        transformer = new ClassDumpTransformer(classes);
    }
    InstrumentationUtils.retransformClasses(
        inst, transformer, classes);
    return transformer.getDumpResult();
}
```

**与 jad 共享的核心机制**：`dump` 和 `jad` 都使用 `ClassDumpTransformer` + `InstrumentationUtils.retransformClasses()` 来获取字节码。区别在于 `jad` 获取字节码后会进一步调用 cfr 反编译，而 `dump` 直接将字节码保存。

#### 6.1.4 processMatch() —— 处理匹配结果

```java
private ExitStatus processMatch(CommandProcess process, 
        RowAffect effect, Instrumentation inst, 
        Set<Class<?>> matchedClasses) {
    try {
        Map<Class<?>, File> classFiles = dump(inst, matchedClasses);
        List<DumpClassVO> dumpedClasses = 
            new ArrayList<DumpClassVO>(classFiles.size());
        for (Map.Entry<Class<?>, File> entry 
                : classFiles.entrySet()) {
            Class<?> clazz = entry.getKey();
            File file = entry.getValue();
            DumpClassVO dumpClassVO = new DumpClassVO();
            dumpClassVO.setLocation(file.getCanonicalPath());
            ClassUtils.fillSimpleClassVO(clazz, dumpClassVO);
            dumpedClasses.add(dumpClassVO);
        }
        process.appendResult(
            new DumpClassModel().setDumpedClasses(dumpedClasses));
        effect.rCnt(classFiles.keySet().size());
        return ExitStatus.success();
    } catch (Throwable t) {
        logger.error("dump: fail to dump classes: " 
            + matchedClasses, t);
        return ExitStatus.failure(-1, "dump: fail to dump classes");
    }
}
```

对每个 dump 成功的类，记录其保存路径和类信息，返回给用户。

---

## 第七阶段：RedefineCommand —— 类重定义命令

### 7.1 RedefineCommand —— redefine 命令

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/RedefineCommand.java`

`redefine` 命令用于将新的 `.class` 文件加载到 JVM 中，替换已有类的定义。它与 `mc` 命令配合使用，形成"修改→编译→替换"的热修复流程。

#### 7.1.1 参数定义

```java
@Name("redefine")
@Summary("Redefine classes. @see Instrumentation#redefineClasses(ClassDefinition...)")
@Description(Constants.EXAMPLE +
    "  redefine /tmp/Test.class\n" +
    "  redefine -c 327a647b /tmp/Test.class /tmp/Test\\$Inner.class \n" +
    "  redefine --classLoaderClass "
    + "'sun.misc.Launcher$AppClassLoader' /tmp/Test.class \n" +
    Constants.WIKI + Constants.WIKI_HOME + "redefine")
public class RedefineCommand extends AnnotatedCommand {
    private static final int MAX_FILE_SIZE = 10 * 1024 * 1024;
    private String hashCode;
    private String classLoaderClass;
    private List<String> paths;
}
```

**参数对照表**：

| 参数 | 短选项 | 长选项 | 类型 | 说明 |
|------|--------|--------|------|------|
| classfilePaths | 位置参数0 | — | List<String> | .class 文件路径列表 |
| -c | -c | --classloader | String | ClassLoader hashCode |
| — | — | --classLoaderClass | String | ClassLoader 类名 |

注意 `MAX_FILE_SIZE = 10 * 1024 * 1024`（10MB），超过这个大小的 `.class` 文件会被拒绝。

#### 7.1.2 process() 主流程

```java
@Override
public void process(CommandProcess process) {
    RedefineModel redefineModel = new RedefineModel();
    Instrumentation inst = process.session().getInstrumentation();
    
    // 步骤1：校验所有文件
    for (String path : paths) {
        File file = new File(path);
        if (!file.exists()) {
            process.end(-1, "file does not exist, path:" + path);
            return;
        }
        if (!file.isFile()) {
            process.end(-1, "not a normal file, path:" + path);
            return;
        }
        if (file.length() >= MAX_FILE_SIZE) {
            process.end(-1, "file size: " + file.length() 
                + " >= " + MAX_FILE_SIZE);
            return;
        }
    }

    // 步骤2：读取所有 .class 文件并解析类名
    Map<String, byte[]> bytesMap = new HashMap<String, byte[]>();
    for (String path : paths) {
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(path, "r");
            final byte[] bytes = new byte[(int) f.length()];
            f.readFully(bytes);
            final String clazzName = readClassName(bytes);
            bytesMap.put(clazzName, bytes);
        } catch (Exception e) {
            // ... 错误处理
        } finally {
            // ... 关闭文件
        }
    }

    if (bytesMap.size() != paths.size()) {
        process.end(-1, "paths may contains same class name!");
        return;
    }

    // 步骤3：在 JVM 中查找对应的已加载类
    List<ClassDefinition> definitions = 
        new ArrayList<ClassDefinition>();
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        if (bytesMap.containsKey(clazz.getName())) {
            // ... ClassLoader 解析（省略）
            ClassLoader classLoader = clazz.getClassLoader();
            if (classLoader != null && hashCode != null 
                    && !Integer.toHexString(
                        classLoader.hashCode()).equals(hashCode)) {
                continue;
            }
            definitions.add(
                new ClassDefinition(clazz, 
                    bytesMap.get(clazz.getName())));
            redefineModel.addRedefineClass(clazz.getName());
        }
    }

    // 步骤4：执行 redefine
    try {
        if (definitions.isEmpty()) {
            process.end(-1, "These classes are not found in the JVM "
                + "and may not be loaded: " + bytesMap.keySet());
            return;
        }
        inst.redefineClasses(
            definitions.toArray(new ClassDefinition[0]));
        process.appendResult(redefineModel);
        process.end();
    } catch (Throwable e) {
        String message = "redefine error! " + e.toString();
        logger.error(message, e);
        process.end(-1, message);
    }
}
```

**逐步详解**：

**步骤1：文件校验**

三重校验：文件是否存在、是否是普通文件（非目录）、大小是否超限。这些校验在读取文件之前完成，fail-fast 策略。

**步骤2：读取字节码并解析类名**

```java
final String clazzName = readClassName(bytes);

private static String readClassName(final byte[] bytes) {
    return new ClassReader(bytes).getClassName().replace("/", ".");
}
```

使用 ASM 库的 `ClassReader` 从字节码中解析出类的全限定名。这里不依赖文件名来确定类名，而是直接从字节码中读取——这更准确、更可靠。

`bytesMap.size() != paths.size()` 的检查是为了检测是否有多个文件包含同一个类名的情况。

**步骤3：查找已加载的类**

遍历 JVM 中所有已加载的类，找到与 `.class` 文件中类名匹配的类。如果指定了 ClassLoader hashCode，还要匹配 ClassLoader。

**步骤4：执行 redefine**

```java
inst.redefineClasses(definitions.toArray(new ClassDefinition[0]));
```

这是核心 API 调用。`Instrumentation.redefineClasses()` 将新的字节码应用到已加载的类上。

**redefine 的限制条件**：

| 限制 | 说明 |
|------|------|
| 不能添加/删除字段 | 类的字段结构不能改变 |
| 不能添加/删除方法 | 类的方法签名不能改变 |
| 不能改变方法签名 | 参数类型、返回类型不能变 |
| 不能改变类的继承关系 | 父类和接口不能变 |
| 只能修改方法体 | 方法内部的实现可以随意修改 |
| 不能改变类的修饰符 | public/private 等不能变 |

这些限制是 JVM 规范的硬性要求，违反会抛出 `UnsupportedOperationException`。

---

## 第八阶段：ClassLoaderCommand —— 类加载器命令

### 8.1 ClassLoaderCommand —— classloader 命令

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/ClassLoaderCommand.java`

`classloader` 命令提供了对 JVM 中所有 ClassLoader 的全方位查看能力。它是最复杂的类操作命令之一，支持多种查看模式。

#### 8.1.1 命令参数总览

| 参数 | 短选项 | 长选项 | 类型 | 说明 |
|------|--------|--------|------|------|
| -t | -t | --tree | boolean | 树形展示 ClassLoader 继承关系 |
| -c | -c | --classloader | String | 指定 ClassLoader 的 hashCode |
| — | — | --classLoaderClass | String | 指定 ClassLoader 的类名 |
| -a | -a | --all | boolean | 显示所有已加载的类 |
| -r | -r | --resource | String | 通过 ClassLoader 查找资源 |
| -i | -i | --include-reflection-classloader | boolean | 包含反射用 ClassLoader |
| -l | -l | --list-classloader | boolean | 按实例列出 ClassLoader |
| — | — | --load | String | 用 ClassLoader 加载类 |
| -u | -u | --url-stat | boolean | 显示 ClassLoader URL 统计 |
| — | — | --url-classes | boolean | 显示 jar 与类的对应关系 |
| -d | -d | --details | boolean | 显示每个 jar 的类列表 |
| -E | -E | --regex | boolean | 启用正则过滤 |
| -n | -n | --limit | int | 详情模式下每个 jar 最大类数 |
| — | — | --jar | String | 按 jar 名过滤 |
| — | — | --class | String | 按类名过滤 |

#### 8.1.2 process() 路由分发

```java
@Override
public void process(CommandProcess process) {
    process.interruptHandler(
        new ClassLoaderInterruptHandler(this));
    // ...
    
    if (urlStat) {
        // 模式1：URL 统计
        Map<ClassLoaderVO, ClassLoaderUrlStat> urlStats = 
            this.urlStats(inst);
        // ...
    } else if (urlClasses) {
        // 模式2：jar 与类的对应关系
        processUrlClasses(process, inst, targetClassLoader);
    } else if (all) {
        // 模式3：显示所有类
        processAllClasses(process, inst, hashCode);
    } else if (classLoaderSpecified && resource != null) {
        // 模式4：查找资源
        processResources(process, inst, targetClassLoader);
    } else if (classLoaderSpecified && loadClass != null) {
        // 模式5：加载类
        processLoadClass(process, inst, targetClassLoader);
    } else if (classLoaderSpecified) {
        // 模式6：显示 ClassLoader 的 URL 列表
        processClassLoader(process, inst, targetClassLoader);
    } else if (listClassLoader || isTree) {
        // 模式7：列表或树形展示
        processClassLoaders(process, inst);
    } else {
        // 模式8：默认统计视图
        processClassLoaderStats(process, inst);
    }
}
```

八种模式通过 if-else 链路由分发。注意第一行注册了 `ClassLoaderInterruptHandler`，支持 Ctrl-C 中断。

#### 8.1.3 processClassLoaderStats() —— 默认统计视图

```java
private void processClassLoaderStats(CommandProcess process, 
        Instrumentation inst) {
    RowAffect affect = new RowAffect();
    List<ClassLoaderInfo> classLoaderInfos = 
        getAllClassLoaderInfo(inst);
    Map<String, ClassLoaderStat> classLoaderStats = 
        new HashMap<String, ClassLoaderStat>();
    for (ClassLoaderInfo info: classLoaderInfos) {
        String name = info.classLoader == null 
            ? "BootstrapClassLoader" 
            : info.classLoader.getClass().getName();
        ClassLoaderStat stat = classLoaderStats.get(name);
        if (null == stat) {
            stat = new ClassLoaderStat();
            classLoaderStats.put(name, stat);
        }
        stat.addLoadedCount(info.loadedClassCount);
        stat.addNumberOfInstance(1);
    }
    
    TreeMap<String, ClassLoaderStat> sorted =
        new TreeMap<String, ClassLoaderStat>(
            new ValueComparator(classLoaderStats));
    sorted.putAll(classLoaderStats);
    process.appendResult(
        new ClassLoaderModel().setClassLoaderStats(sorted));
}
```

**这一步做了什么？** 按 ClassLoader 的类名分组统计，计算每种 ClassLoader 的实例数和总加载类数。输出格式类似：

```
名称                                        实例数  加载类数
BootstrapClassLoader                          1     2346
sun.misc.Launcher$AppClassLoader              1      587
sun.misc.Launcher$ExtClassLoader              1       52
sun.reflect.DelegatingClassLoader           120      120
```

排序按加载类数降序排列（通过 `ValueComparator` 实现）。

#### 8.1.4 getAllClassLoaderInfo() —— 收集 ClassLoader 信息

```java
private static List<ClassLoaderInfo> getAllClassLoaderInfo(
        Instrumentation inst, Filter... filters) {
    ClassLoaderInfo bootstrapInfo = new ClassLoaderInfo(null);
    Map<ClassLoader, ClassLoaderInfo> loaderInfos = 
        new HashMap<ClassLoader, ClassLoaderInfo>();

    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        ClassLoader classLoader = clazz.getClassLoader();
        if (classLoader == null) {
            bootstrapInfo.increase();
        } else {
            if (shouldInclude(classLoader, filters)) {
                ClassLoaderInfo loaderInfo = 
                    loaderInfos.get(classLoader);
                if (loaderInfo == null) {
                    loaderInfo = new ClassLoaderInfo(classLoader);
                    loaderInfos.put(classLoader, loaderInfo);
                    ClassLoader parent = classLoader.getParent();
                    while (parent != null) {
                        ClassLoaderInfo parentLoaderInfo = 
                            loaderInfos.get(parent);
                        if (parentLoaderInfo == null) {
                            parentLoaderInfo = 
                                new ClassLoaderInfo(parent);
                            loaderInfos.put(parent, parentLoaderInfo);
                        }
                        parent = parent.getParent();
                    }
                }
                loaderInfo.increase();
            }
        }
    }
    // ... 排序处理
}
```

**关键细节**：

1. BootstrapClassLoader 特殊处理：`classLoader == null` 的类被统计到 `bootstrapInfo` 中
2. 当发现一个新的 ClassLoader 时，会沿着 parent 链向上遍历，确保所有祖先 ClassLoader 都被记录（即使它们没有直接加载任何类）
3. 使用 Filter 机制过滤不需要的 ClassLoader（如 `SunReflectionClassLoaderFilter`）

#### 8.1.5 SunReflectionClassLoaderFilter —— 反射类加载器过滤

```java
private static class SunReflectionClassLoaderFilter 
        implements Filter {
    private static final List<String> REFLECTION_CLASSLOADERS = 
        Arrays.asList(
            "sun.reflect.DelegatingClassLoader",
            "jdk.internal.reflect.DelegatingClassLoader");

    @Override
    public boolean accept(ClassLoader classLoader) {
        return !REFLECTION_CLASSLOADERS.contains(
            classLoader.getClass().getName());
    }
}
```

**为什么要过滤反射类加载器？** JVM 在使用反射调用方法时，会为每个被反射调用的方法创建一个独立的 `DelegatingClassLoader` 实例。在大型应用中，可能有成百上千个这样的 ClassLoader，它们会干扰 ClassLoader 的统计和展示。默认不包含它们（但可以通过 `-i` 选项包含）。

#### 8.1.6 processClassLoaderTree() —— 树形展示

```java
private static List<ClassLoaderVO> processClassLoaderTree(
        List<ClassLoaderVO> classLoaders) {
    List<ClassLoaderVO> rootClassLoaders = new ArrayList<>();
    Map<String, List<ClassLoaderVO>> childMap = new HashMap<>();

    for (ClassLoaderVO classLoaderVO : classLoaders) {
        if (classLoaderVO.getParent() == null) {
            rootClassLoaders.add(classLoaderVO);
        } else {
            childMap.computeIfAbsent(
                classLoaderVO.getParent(), 
                k -> new ArrayList<>()).add(classLoaderVO);
        }
    }

    for (ClassLoaderVO root : rootClassLoaders) {
        buildTree(root, childMap);
    }

    return rootClassLoaders;
}
```

**这一步做了什么？** 将扁平的 ClassLoader 列表构建为树形结构。算法：
1. 遍历所有 ClassLoader，分为根节点（parent == null）和非根节点
2. 非根节点按 parent 分组存入 childMap
3. 从根节点开始递归构建树

#### 8.1.7 urlStats() —— URL 使用统计

```java
private Map<ClassLoaderVO, ClassLoaderUrlStat> urlStats(
        Instrumentation inst) {
    Map<ClassLoaderVO, ClassLoaderUrlStat> urlStats = 
        new HashMap<>();
    Map<ClassLoader, Set<String>> usedUrlsMap = new HashMap<>();
    
    for (Class<?> clazz : inst.getAllLoadedClasses()) {
        ClassLoader classLoader = clazz.getClassLoader();
        if (classLoader != null) {
            ProtectionDomain protectionDomain = 
                clazz.getProtectionDomain();
            CodeSource codeSource = 
                protectionDomain.getCodeSource();
            if (codeSource != null) {
                URL location = codeSource.getLocation();
                if (location != null) {
                    Set<String> urls = usedUrlsMap.get(classLoader);
                    if (urls == null) {
                        urls = new HashSet<String>();
                        usedUrlsMap.put(classLoader, urls);
                    }
                    urls.add(location.toString());
                }
            }
        }
    }
    
    for (Entry<ClassLoader, Set<String>> entry 
            : usedUrlsMap.entrySet()) {
        ClassLoader loader = entry.getKey();
        Set<String> usedUrls = entry.getValue();
        URL[] allUrls = ClassLoaderUtils.getUrls(loader);
        List<String> unusedUrls = new ArrayList<String>();
        if (allUrls != null) {
            for (URL url : allUrls) {
                String urlStr = url.toString();
                if (!usedUrls.contains(urlStr)) {
                    unusedUrls.add(urlStr);
                }
            }
        }
        urlStats.put(
            ClassUtils.createClassLoaderVO(loader), 
            new ClassLoaderUrlStat(usedUrls, unusedUrls));
    }
    return urlStats;
}
```

**这一步做了什么？** 分析每个 ClassLoader 的 classpath URL 使用情况：
1. 遍历所有类，通过 `CodeSource.getLocation()` 收集每个 ClassLoader "实际用到"的 URL
2. 通过 `ClassLoaderUtils.getUrls()` 获取每个 ClassLoader "声明的"所有 URL
3. 将"声明的 URL" - "实际用到的 URL" = "未使用的 URL"

这个功能用于发现 classpath 中的冗余 jar 包——如果一个 jar 包在 classpath 中但没有被加载任何类，说明它可能是多余的，可以考虑移除以减少启动时间和内存占用。

#### 8.1.8 processUrlClasses() —— jar 与类的对应关系

```java
private void processUrlClasses(CommandProcess process, 
        Instrumentation inst, ClassLoader targetClassLoader) {
    // ... 参数校验
    
    Map<String, UrlClassStatBuilder> statsMap = new HashMap<>();
    Class<?>[] allLoadedClasses = inst.getAllLoadedClasses();
    for (int i = 0; i < allLoadedClasses.length; i++) {
        if ((i & 0x3FFF) == 0 && checkInterrupted(process)) {
            return;
        }
        Class<?> clazz = allLoadedClasses[i];
        if (clazz == null) continue;
        if (clazz.getClassLoader() != targetClassLoader) continue;

        String url = codeSourceLocation(clazz);
        if (!matchJarFilter(url, jarPattern)) continue;

        UrlClassStatBuilder builder = statsMap.get(url);
        if (builder == null) {
            builder = new UrlClassStatBuilder(url, 
                classFilter != null, 
                urlClassesDetail ? urlClassesLimit : 0);
            statsMap.put(url, builder);
        }
        builder.increaseLoadedCount();

        if (classFilter != null) {
            if (matchClassFilter(clazz.getName(), classPattern)) {
                builder.increaseMatchedCount();
                builder.tryAddClass(clazz.getName());
            }
        } else {
            builder.tryAddClass(clazz.getName());
        }
    }
    // ... 排序和输出
}
```

**关键设计细节**：

1. **中断检测**：`(i & 0x3FFF) == 0` 每隔 16384 个类检查一次是否被用户中断（Ctrl-C）。这个检查间隔通过位运算实现，非常高效
2. **jar 过滤**：支持通过 `--jar` 选项按 jar 名过滤
3. **类名过滤**：支持通过 `--class` 选项按类名过滤
4. **限流**：通过 `urlClassesLimit`（默认100）限制每个 jar 下最多展示的类数量

#### 8.1.9 ClassLoaderInterruptHandler —— 中断处理

```java
private static class ClassLoaderInterruptHandler 
        implements Handler<Void> {
    private ClassLoaderCommand command;

    public ClassLoaderInterruptHandler(
            ClassLoaderCommand command) {
        this.command = command;
    }

    @Override
    public void handle(Void event) {
        command.isInterrupted = true;
    }
}
```

当用户按下 Ctrl-C 时，框架调用 `handle()` 方法设置 `isInterrupted = true`。循环中的 `checkInterrupted()` 检查到这个标志后，停止遍历并结束命令。这是 Arthas 中典型的协作式中断模式。

---

## 第九阶段：GetStaticCommand —— 获取静态字段命令

### 9.1 GetStaticCommand —— getstatic 命令

**源码位置**: `arthas/core/src/main/java/com/taobao/arthas/core/command/klass100/GetStaticCommand.java`

`getstatic` 命令用于查看类的静态字段值。虽然 `ognl` 命令也可以做到这一点（通过 `@ClassName@fieldName` 语法），但 `getstatic` 提供了更直接的方式。

#### 9.1.1 核心逻辑

```java
private ExitStatus processExactMatch(CommandProcess process, 
        RowAffect affect, Instrumentation inst,
        Set<Class<?>> matchedClasses) {
    Matcher<String> fieldNameMatcher = fieldNameMatcher();
    Class<?> clazz = matchedClasses.iterator().next();
    boolean found = false;

    for (Field field : clazz.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers()) 
                || !fieldNameMatcher.matching(field.getName())) {
            continue;
        }
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        try {
            Object value = field.get(null);
            if (!StringUtils.isEmpty(express)) {
                value = ExpressFactory.threadLocalExpress(value)
                    .get(express);
            }
            process.appendResult(
                new GetStaticModel(field.getName(), value, expand));
            affect.rCnt(1);
        } catch (IllegalAccessException e) {
            // ... 错误处理
        }
        found = true;
    }
    // ...
}
```

**逐行解释**：

1. 遍历类的所有声明字段
2. 过滤非静态字段和不匹配名称模式的字段
3. 通过 `setAccessible(true)` 突破访问限制
4. `field.get(null)` —— null 表示获取静态字段的值（静态字段不属于任何实例）
5. 如果用户还提供了 OGNL 表达式（第三个参数），对获取到的值再执行表达式求值

**getstatic 支持 OGNL 表达式的意义**：可以对获取到的静态字段值进行进一步操作。例如 `getstatic com.example.Config cache size` 先获取 `cache` 静态字段，再对其执行 `size` 表达式（相当于调用 `cache.size()`）。

---

## 第十阶段：三大核心命令对比分析

### 10.1 jad / mc / ognl 功能对比

| 维度 | jad（反编译） | mc（内存编译） | ognl（表达式执行） |
|------|-------------|---------------|------------------|
| **功能定位** | 将字节码反编译为源码 | 将源码编译为字节码 | 在 JVM 中执行表达式 |
| **输入** | 类名模式 | Java 源文件路径 | OGNL 表达式字符串 |
| **输出** | Java 源代码文本 | .class 文件 | 表达式执行结果 |
| **底层引擎** | cfr 反编译器 | javax.tools.JavaCompiler | Apache OGNL |
| **字节码操作** | 读取（通过retransform） | 生成（通过JavaCompiler） | 无 |
| **ClassLoader** | 用于过滤类 | 用于解析编译依赖 | 用于解析表达式中的类 |
| **默认ClassLoader** | 无（按类匹配） | SystemClassLoader | SystemClassLoader |
| **是否修改JVM状态** | 否 | 否（只生成文件） | 可能（可以调用方法） |
| **典型使用场景** | 查看运行中的代码版本 | 热修复前编译新代码 | 调用方法、读写字段 |

### 10.2 dump / jad / redefine 操作链

这三个命令形成一个完整的"类操作闭环"：

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│          │     │          │     │          │
│   dump   │────>│   修改   │────>│ redefine │
│ (导出)   │     │ (编辑)   │     │ (替换)   │
│          │     │          │     │          │
└──────────┘     └──────────┘     └──────────┘
      │                                 ▲
      │                                 │
      │          ┌──────────┐           │
      └─────────>│   jad    │           │
                 │ (反编译) │           │
                 └────┬─────┘           │
                      │                 │
                      ▼                 │
                 ┌──────────┐           │
                 │   修改   │           │
                 │ (编辑)   │           │
                 └────┬─────┘           │
                      │                 │
                      ▼                 │
                 ┌──────────┐           │
                 │   mc     │───────────┘
                 │ (编译)   │
                 └──────────┘
```

**两条路径**：

1. **字节码路径**：`dump`（导出.class）→ 外部工具修改字节码 → `redefine`（替换）
2. **源码路径**：`jad`（反编译）→ 编辑 Java 源码 → `mc`（编译）→ `redefine`（替换）

### 10.3 所有命令的 ClassLoader 处理方式对比

| 命令 | -c (hashCode) | --classLoaderClass | 默认行为 | classLoaderToString |
|------|:---:|:---:|---|:---:|
| jad | 支持 | 支持（转hashCode） | 无ClassLoader过滤 | 不支持 |
| mc | 支持 | 支持（转hashCode） | SystemClassLoader | 不支持 |
| ognl | 支持 | 支持（直接获取实例） | SystemClassLoader | 不支持 |
| sc | 支持 | 支持（转hashCode） | 无ClassLoader过滤 | 支持(-cs) |
| dump | 支持 | 支持（转hashCode） | 无ClassLoader过滤 | 不支持 |
| redefine | 支持 | 支持（转hashCode） | 不过滤 | 不支持 |
| classloader | 支持 | 支持（直接获取实例） | N/A | 不支持 |
| getstatic | 支持 | 支持（转hashCode） | 无ClassLoader过滤 | 不支持 |

**公共模式**：几乎所有命令都遵循相同的 ClassLoader 解析模式：

```java
// 伪代码：所有命令共享的 ClassLoader 解析模式
if (hashCode == null && classLoaderClass != null) {
    List<ClassLoader> matched = ClassLoaderUtils
        .getClassLoaderByClassName(inst, classLoaderClass);
    if (matched.size() == 1) {
        hashCode = toHex(matched.get(0).hashCode());
    } else if (matched.size() > 1) {
        // 列出所有匹配的 ClassLoader，提示用户用 -c 精确指定
        return;
    } else {
        // 报错：找不到
        return;
    }
}
```

---

## 第十一阶段：关键设计问题深入分析

### 11.1 jad 为什么要用 retransform 来获取字节码，不能直接读 .class 文件吗？

这是一个非常好的问题，理解它需要知道 JVM 中类的字节码经历了什么。

**直接读 .class 文件的问题**：

1. **找不到文件**：类可能从网络加载（`URLClassLoader`）、从加密的 jar 中加载、或由动态生成器（如 CGLIB、Javassist）在内存中直接生成，根本没有对应的磁盘文件

2. **文件内容不准确**：即使找到了 .class 文件，JVM 中运行的字节码可能已经与文件不同：
   - 其他 Java Agent 的 `ClassFileTransformer` 可能在加载时修改了字节码（如 AOP 框架的字节码增强）
   - `redefine` 命令可能已经替换了类的字节码
   - Spring AOP、Hibernate 等框架的动态代理也会修改字节码

3. **BootstrapClassLoader 的类**：`rt.jar` 中的类在某些 JDK 版本中路径不好确定

**retransform 方案的优势**：

通过 `retransformClasses()`，JVM 会将类的**当前实际字节码**传递给 `ClassFileTransformer.transform()` 回调。这个字节码是"活的"——它反映了类在 JVM 中的真实状态，包括所有 Agent 的修改。

**类比理解**：
- 直接读 .class 文件 ≈ 看一本书的初版印刷
- 通过 retransform 获取字节码 ≈ 看这本书的最新修订版（包含所有勘误和修改）

### 11.2 mc 如何选择目标 ClassLoader 进行编译？

`mc` 命令选择 ClassLoader 的逻辑：

```
用户指定了 -c？ ──是──→ 通过 hashCode 查找 ClassLoader
    │
    否
    │
用户指定了 --classLoaderClass？ ──是──→ 通过类名查找 ClassLoader
    │
    否
    │
    └──→ 使用 SystemClassLoader
```

**为什么 ClassLoader 的选择如此重要？**

编译 Java 代码时，编译器需要解析代码中引用的所有类。例如：

```java
import com.example.service.UserService;

public class UserController {
    private UserService service = new UserService();
}
```

要编译这段代码，编译器需要找到 `UserService` 类。如果 `UserService` 是由自定义 ClassLoader（如 Spring 的 `LaunchedURLClassLoader`）加载的，那么必须指定该 ClassLoader，否则编译器找不到 `UserService` 类，编译会失败。

`DynamicCompiler` 通过 `DynamicClassLoader(classLoader)` 将用户指定的 ClassLoader 设为父加载器，这样编译器在解析类引用时就会通过这个父加载器来查找类。

### 11.3 ognl 如何安全地在目标 JVM 中执行任意代码？

OGNL 表达式的执行涉及多层安全机制：

**第一层：ClassLoader 隔离**

```java
Express unpooledExpress = ExpressFactory.unpooledExpress(classLoader);
```

通过 `ClassLoaderClassResolver` 限定表达式中能引用的类范围——只能使用指定 ClassLoader 可见的类。

**第二层：成员访问控制**

```java
private static final MemberAccess MEMBER_ACCESS = 
    new DefaultMemberAccess(true);
```

虽然 `DefaultMemberAccess(true)` 允许访问所有成员（包括 private），但 `ArthasObjectPropertyAccessor` 在 strict 模式下会阻止属性设置：

```java
if (GlobalOptions.strict) {
    throw new IllegalAccessError(GlobalOptions.STRICT_MESSAGE);
}
```

**第三层：异常隔离**

```java
try {
    Object value = unpooledExpress.bind(new Object()).get(express);
} catch (ExpressException e) {
    // 异常被捕获，不会传播到业务代码
}
```

表达式执行过程中的任何异常都被捕获，不会影响目标 JVM 的正常运行。

**潜在风险**：

尽管有这些安全机制，`ognl` 命令仍然有一定的风险：
- 可以调用任意方法（包括 `System.exit()`）
- 可以修改静态字段值
- 可以通过反射绕过访问控制

因此在生产环境使用时需要谨慎，只在排查问题时临时使用。

### 11.4 redefine 和 Instrumentation.redefineClasses 的限制

`Instrumentation.redefineClasses()` 的限制来自 JVM 规范（JVMS），主要包括：

| 限制项 | 说明 | 违反后果 |
|--------|------|----------|
| 不能增删字段 | 类的字段数量和名称不能改变 | UnsupportedOperationException |
| 不能增删方法 | 类的方法签名集合不能改变 | UnsupportedOperationException |
| 不能修改方法签名 | 方法的参数类型、返回类型、异常类型不能变 | UnsupportedOperationException |
| 不能修改继承关系 | 父类和实现的接口不能变 | UnsupportedOperationException |
| 不能修改类修饰符 | public/abstract/final 等不能变 | UnsupportedOperationException |
| 可以修改方法体 | 方法内部的实现代码可以自由修改 | 正常 |
| 可以修改常量池 | 可以添加/修改字符串常量等 | 正常 |

**为什么有这些限制？**

JVM 在类加载后会创建各种元数据结构（vtable、itable、字段偏移量表等）来支持高效的方法分派和字段访问。如果允许改变类的结构（添加字段/方法），就需要重新计算所有这些元数据，并更新所有引用了该类的代码——这在运行中的 JVM 里几乎不可能安全地完成。

**retransform 与 redefine 的区别**：

| 维度 | retransform | redefine |
|------|-------------|----------|
| API | `Instrumentation.retransformClasses()` | `Instrumentation.redefineClasses()` |
| 字节码来源 | JVM 中的当前字节码 | 用户提供的新字节码 |
| 主要用途 | 触发 transformer 链（如 dump 字节码） | 替换类的实现 |
| 可逆性 | 可以通过移除 transformer 后再次 retransform 来"撤销" | 不可逆（除非再次 redefine） |
| Arthas 使用场景 | `jad`、`dump`（只读） | `redefine`（修改） |

### 11.5 多 ClassLoader 环境下如何精确定位类？

在复杂的 Java 应用中（如 OSGI、Spring Boot、Tomcat 多应用部署），同一个类名可能被多个 ClassLoader 加载。Arthas 提供了三种方式来精确定位：

**方式一：通过 ClassLoader hashCode（-c）**

```bash
# 1. 先用 sc 找到所有同名类及其 ClassLoader hashCode
sc -d com.example.UserService

# 输出：
# class-info     com.example.UserService
# classLoaderHash 327a647b
# class-info     com.example.UserService
# classLoaderHash 5d113a51

# 2. 用 -c 指定精确的 ClassLoader
jad -c 327a647b com.example.UserService
```

**方式二：通过 ClassLoader 类名（--classLoaderClass）**

```bash
jad --classLoaderClass org.springframework.boot.loader.LaunchedURLClassLoader com.example.UserService
```

如果该类名的 ClassLoader 只有一个实例，会自动选中；如果有多个实例，提示用户用 `-c`。

**方式三：通过 ClassLoader 的 toString()（仅 sc 支持 -cs）**

```bash
sc -cs "WebappClassLoader context=/myapp" com.example.UserService
```

**Arthas 内部实现**：所有这些方式最终都归结为调用 `SearchUtils` 的 `filter()` 方法，通过 ClassLoader 的 hashCode 进行过滤：

```java
private static Set<Class<?>> filter(
        Set<Class<?>> matchedClasses, String code) {
    Set<Class<?>> result = new HashSet<>();
    for (Class<?> c : matchedClasses) {
        if (c.getClassLoader() != null 
            && Integer.toHexString(
                c.getClassLoader().hashCode()).equals(code)) {
            result.add(c);
        }
    }
    return result;
}
```

### 11.6 dump 和 jad 的区别和联系

**联系**：两者共享相同的底层字节码获取机制——`ClassDumpTransformer` + `InstrumentationUtils.retransformClasses()`。

**区别**：

| 维度 | dump | jad |
|------|------|-----|
| **最终输出** | .class 字节码文件 | Java 源代码文本 |
| **后续处理** | 无 | 调用 cfr 反编译 |
| **类搜索** | `searchClass()`（含子类） | `searchClassOnly()`（不含子类） |
| **内部类处理** | 不特殊处理 | 自动搜索并一起 dump |
| **数量限制** | 有（默认50） | 无（只处理一个精确匹配） |
| **行号映射** | 无 | 有 |
| **方法过滤** | 无 | 支持只反编译特定方法 |
| **使用场景** | 导出字节码供外部工具分析 | 在线查看代码 |

**使用建议**：
- 如果只是想看代码，用 `jad`
- 如果需要用 ASM/Javassist 等工具分析字节码结构，用 `dump`
- 如果要做热替换，先 `jad` 看代码 → 修改 → `mc` 编译 → `redefine` 替换

---

## 第十二阶段：反编译引擎 cfr 使用详解

### 12.1 cfr 的工作原理

cfr（Class File Reader）是 Arthas 使用的 Java 反编译器。它的工作流程如下：

```
.class 字节码文件
    │
    ▼
┌─────────────────────────────┐
│     字节码解析层              │
│  读取常量池、方法表、字段表    │
│  解析字节码指令序列           │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│     控制流分析层              │
│  构建控制流图(CFG)           │
│  识别循环、分支、异常处理     │
│  恢复 if/for/while/try      │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│     类型推断层               │
│  推断局部变量类型            │
│  恢复泛型信息               │
│  处理类型擦除               │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│     代码生成层               │
│  生成 Java 源代码            │
│  处理缩进和格式化            │
│  生成行号映射               │
└─────────────────────────────┘
```

### 12.2 cfr 在 Arthas 中的配置

Arthas 通过 `CfrDriver.Builder` 配置 cfr：

```java
HashMap<String, String> options = new HashMap<>();
options.put("showversion", "false");     // 不显示版本号
options.put("hideutf", "true/false");    // 是否隐藏 Unicode
options.put("trackbytecodeloc", "true"); // 跟踪字节码位置（行号映射必须）
options.put("methodname", "xxx");        // 只反编译指定方法

CfrDriver driver = new CfrDriver.Builder()
    .withOptions(options)
    .withOutputSink(mySink)
    .build();
```

### 12.3 行号映射的实现

cfr 提供了两层行号映射：

1. **classFileMappings**：字节码中的行号表（`LineNumberTable` 属性）→ 原始源码行号
2. **mappings**：反编译后的输出行号 → 字节码中的行号

Arthas 将两层映射合并为：**反编译后的行号 → 原始源码行号**

```java
for (Entry<Integer, Integer> entry : mappings.entrySet()) {
    Integer srcLineNumber = classFileMappings.get(entry.getKey());
    lineMapping.put(entry.getValue(), srcLineNumber);
}
```

这样用户在 `jad` 输出中看到的行号就是原始源码中的行号，方便与 IDE 中的代码对照。

### 12.4 cfr 的局限性

| 场景 | cfr 表现 |
|------|---------|
| 普通 Java 代码 | 非常好 |
| Lambda 表达式 | 良好 |
| 泛型代码 | 较好（可恢复大部分泛型信息） |
| 混淆后的代码 | 可反编译但可读性差 |
| Kotlin 编译的字节码 | 可反编译但有些 Kotlin 特有语法无法恢复 |
| 动态生成的字节码（CGLIB） | 可反编译但代码可能不直观 |

---

## 第十三阶段：内存编译核心组件深入分析

### 13.1 javax.tools.JavaCompiler API 概述

`mc` 命令的核心依赖是 JDK 提供的 `javax.tools.JavaCompiler` API。这个 API 自 JDK 6 引入，允许 Java 程序在运行时调用编译器。

```
┌────────────────────────────────────────┐
│           DynamicCompiler               │
│                                         │
│  ┌─────────────┐   ┌────────────────┐  │
│  │ StringSource │   │ DynamicJava    │  │
│  │ (源码输入)   │──>│ FileManager    │  │
│  └─────────────┘   │ (IO 拦截)      │  │
│                     └───────┬────────┘  │
│                             │           │
│                     ┌───────▼────────┐  │
│                     │ JavaCompiler   │  │
│                     │ .getTask()     │  │
│                     │ .call()        │  │
│                     └───────┬────────┘  │
│                             │           │
│                     ┌───────▼────────┐  │
│                     │ DynamicClass   │  │
│                     │ Loader         │  │
│                     │ (字节码存储)    │  │
│                     └────────────────┘  │
└────────────────────────────────────────┘
```

### 13.2 编译流程中的类解析

当编译器遇到 `import com.example.Foo` 时，它需要找到 `Foo` 类的定义来检查类型正确性。查找过程：

1. 编译器调用 `DynamicJavaFileManager.list(CLASS_PATH, "com.example", {CLASS}, false)`
2. `DynamicJavaFileManager` 先委托给 `StandardFileManager` 在标准位置查找
3. 如果找不到，通过 `PackageInternalsFinder` 在用户指定的 ClassLoader 可见的类中查找
4. 如果是刚编译的类（同批次的其他源文件），通过 `DynamicClassLoader.findClass()` 查找

这个多层查找机制保证了：
- 标准库的类可以找到
- 目标 JVM 中已加载的类可以找到
- 同批次编译的类之间的互相引用可以解析

### 13.3 编译产物的保存

编译完成后，字节码的保存路径由 `mc` 命令控制：

```java
File outputDir = null;
if (this.directory != null) {
    outputDir = new File(this.directory);
} else {
    outputDir = new File("").getAbsoluteFile();
}

for (Entry<String, byte[]> entry : byteCodes.entrySet()) {
    // entry.getKey() 是全限定类名，如 "com.example.Test"
    // 转换为路径格式 "com/example/Test.class"
    File byteCodeFile = new File(outputDir, 
        entry.getKey().replace('.', '/') + ".class");
    FileUtils.writeByteArrayToFile(byteCodeFile, entry.getValue());
}
```

如果用户没有指定 `-d` 目录，`.class` 文件会保存到 Arthas 进程的当前工作目录（通常是 `~`）。

---

## 第十四阶段：总结与架构设计精要

### 14.1 架构分层

Arthas 的类操作命令体系采用了清晰的三层架构：

```
┌──────────────────────────────────────────┐
│        命令层（Command Layer）             │
│  JadCommand / McCommand / OgnlCommand    │
│  ScCommand / DumpCommand / RedefineCmd   │
│  ClassLoaderCommand / GetStaticCommand   │
│                                           │
│  职责：参数解析、流程编排、结果输出        │
└──────────────────┬───────────────────────┘
                   │
┌──────────────────▼───────────────────────┐
│        工具层（Utility Layer）             │
│  SearchUtils / ClassLoaderUtils          │
│  ClassUtils / InstrumentationUtils       │
│  ExpressFactory / Decompiler             │
│                                           │
│  职责：封装公共操作、提供复用能力          │
└──────────────────┬───────────────────────┘
                   │
┌──────────────────▼───────────────────────┐
│        引擎层（Engine Layer）             │
│  ClassDumpTransformer (字节码拦截)        │
│  DynamicCompiler (内存编译)               │
│  OgnlExpress (表达式求值)                 │
│  CfrDriver (反编译)                       │
│  Instrumentation API (JVM 操作)           │
│                                           │
│  职责：提供核心能力实现                    │
└──────────────────────────────────────────┘
```

### 14.2 核心设计原则

**1. 只读优先原则**

`jad` 和 `dump` 的 `ClassDumpTransformer.transform()` 始终返回 `null`，保证不修改类行为。只有 `redefine` 命令才会修改类定义，且需要用户显式操作。

**2. 优雅降级原则**

所有命令在遇到多 ClassLoader 匹配时，不会随意选择一个，而是列出所有候选并提示用户精确指定。这避免了操作错误的类。

**3. 资源安全原则**

`InstrumentationUtils.retransformClasses()` 在 `finally` 块中移除 transformer，保证不会留下"幽灵 transformer"影响后续的类加载。

`ExpressFactory` 使用 `WeakReference` 持有 Express 实例，避免 Arthas 卸载后的内存泄漏。

**4. 失败快速原则**

所有命令在执行核心操作之前，先进行参数校验（目录是否存在、文件大小是否超限、ClassLoader 是否找到等），fail-fast 避免无效操作。

### 14.3 命令协同使用的完整工作流

**场景：线上热修复一个 Bug**

```bash
# 步骤1：反编译查看当前代码
jad --source-only com.example.service.OrderService > /tmp/OrderService.java

# 步骤2：编辑源码修复 Bug
# （用户在 /tmp/OrderService.java 中修改代码）

# 步骤3：查找目标类的 ClassLoader
sc -d com.example.service.OrderService
# 输出 classLoaderHash: 39eb305e

# 步骤4：用正确的 ClassLoader 编译
mc -c 39eb305e -d /tmp/output /tmp/OrderService.java

# 步骤5：用编译后的字节码替换
redefine -c 39eb305e /tmp/output/com/example/service/OrderService.class

# 步骤6：验证修改生效
jad com.example.service.OrderService
```

这个工作流展示了 Arthas 各命令如何紧密协作，形成一套完整的在线诊断和修复方案。

### 14.4 性能考量

| 操作 | 性能影响 | 原因 |
|------|---------|------|
| `sc` 搜索 | 较慢（大型JVM） | 遍历所有已加载类（可能数万） |
| `jad` 反编译 | 中等 | retransform + cfr 反编译 |
| `dump` 导出 | 较快 | 只有 retransform，无反编译 |
| `mc` 编译 | 中等 | 调用 JavaCompiler |
| `redefine` 替换 | 较快 | 直接调用 JVM API |
| `ognl` 执行 | 很快 | 只是表达式求值 |
| `classloader` 统计 | 较慢（大型JVM） | 遍历所有已加载类 |

所有涉及 `inst.getAllLoadedClasses()` 的操作在大型 JVM（加载了数十万个类）中都可能比较耗时。`classloader --url-classes` 命令为此特别实现了中断检测机制。

### 14.5 源码文件速查表

| 文件 | 路径 | 行数 | 核心功能 |
|------|------|------|----------|
| JadCommand.java | core/.../klass100/ | ~256 | jad 命令入口 |
| MemoryCompilerCommand.java | core/.../klass100/ | ~169 | mc 命令入口 |
| OgnlCommand.java | core/.../klass100/ | ~117 | ognl 命令入口 |
| SearchClassCommand.java | core/.../klass100/ | ~183 | sc 命令入口 |
| DumpClassCommand.java | core/.../klass100/ | ~195 | dump 命令入口 |
| RedefineCommand.java | core/.../klass100/ | ~183 | redefine 命令入口 |
| ClassLoaderCommand.java | core/.../klass100/ | ~1067 | classloader 命令入口 |
| GetStaticCommand.java | core/.../klass100/ | ~209 | getstatic 命令入口 |
| ClassDumpTransformer.java | core/.../klass100/ | ~96 | 字节码拦截与dump |
| Decompiler.java | core/.../util/ | ~143 | cfr 反编译封装 |
| DynamicCompiler.java | memorycompiler/.../compiler/ | ~173 | 内存编译器 |
| DynamicClassLoader.java | memorycompiler/.../compiler/ | ~63 | 编译字节码容器 |
| DynamicJavaFileManager.java | memorycompiler/.../compiler/ | ~126 | 内存文件管理器 |
| StringSource.java | memorycompiler/.../compiler/ | ~40 | 字符串源码封装 |
| SearchUtils.java | core/.../util/ | ~145 | 类搜索工具 |
| ClassLoaderUtils.java | core/.../util/ | ~185 | ClassLoader 工具 |
| ClassUtils.java | core/.../util/ | ~248 | 类信息工具 |
| InstrumentationUtils.java | core/.../util/ | ~55 | 字节码操作工具 |
| ExpressFactory.java | core/.../express/ | ~43 | 表达式工厂 |
| OgnlExpress.java | core/.../express/ | ~67 | OGNL 执行器 |
| Express.java | core/.../express/ | ~52 | 表达式接口 |
| ClassLoaderClassResolver.java | core/.../express/ | ~42 | ClassLoader 类解析器 |
| CustomClassResolver.java | core/.../express/ | ~44 | 默认类解析器 |
| DefaultMemberAccess.java | core/.../express/ | ~111 | 成员访问控制 |
| ArthasObjectPropertyAccessor.java | core/.../express/ | ~23 | 安全属性访问器 |

---

> 至此，我们完成了对 Arthas 中 jad/mc/ognl/sc/dump/redefine/classloader/getstatic 八大类操作命令的完整源码分析。从命令入口到底层引擎，从参数解析到 JVM API 调用，每一步都追踪到底。希望这篇分析能帮助你深入理解 Arthas 的类操作体系，以及 Java Agent 技术在实际工程中的应用。
