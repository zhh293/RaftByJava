# AI 驱动开发完整流程指南（Spec Coding / SDD）

> 本文档整理自美团内部学城多篇实践文档，结合 GitHub spec-kit、Amazon Kiro、MDP AI-SDLC 等工具的实际应用经验，系统性地梳理了"如何用 AI 按企业级流程独立开发一个项目"的全貌。

---

## 一、两种 AI 编程范式的本质区别

在动手之前，先理解两条路线的差异：

**Vibe Coding（直觉驱动开发）**——"拿起锤子就是干"。开发者用自然语言描述想法，AI 直接生成代码，开发者接受并继续。门槛极低，适合快速原型和创意验证，但在复杂项目中往往导致代码质量失控、上下文丢失、技术债务悄悄累积。典型体验是"永远在调试"。

**Spec Coding（规范驱动开发，SDD）**——"计划后执行"。在让 AI 写代码之前，先通过结构化的规格文档完整显性化"要做什么"和"怎么做"，再驱动 AI 按规格实现。前期投入多，但总交付时间反而更短，质量可控，知识可沉淀。

两者的核心对比：

| 维度 | Vibe Coding | Spec Coding |
|------|-------------|-------------|
| 上下文管理 | 仅存于对话框，多人协作易丢失 | 结构化文档沉淀，团队共享上下文 |
| 质量控制 | 事后被动修复，返工成本高 | 前置规范评审，早期发现问题 |
| 知识传承 | 隐性知识依赖个人，难以交接 | 显性文档标准化，新人可快速接手 |
| 适合场景 | 2PD 以内、1-3 个文件的简单改动 | 跨模块、多文件、有架构决策的正经项目 |

**结论：对于你独立开发一个有一定复杂度的项目（比如 Raft 实现），Spec Coding 是正确的选择。**

---

## 二、SDD 全流程概览（七步法）

Spec Coding 的精髓是"重设计、轻编码"的倒金字塔结构——前 6 步全是思考与规划，只有最后 1 步是"执行"：

```
┌─────────────────────────────────────────────────────┐
│  Step 1: Bootstrap（项目初始化 + AI 行为规范）         │
├─────────────────────────────────────────────────────┤
│  Step 2: Specify（需求定义 → requirements.md）        │
├─────────────────────────────────────────────────────┤
│  Step 3: Design（方案设计 → design.md）               │
├─────────────────────────────────────────────────────┤
│  Step 4: Plan / Tasks（任务拆解 → tasks.md）          │
├─────────────────────────────────────────────────────┤
│  Step 5: Review（人工评审设计与任务）                   │
├─────────────────────────────────────────────────────┤
│  Step 6: Test Spec（测试规范定义）                     │
├─────────────────────────────────────────────────────┤
│  Step 7: Implement（逐任务生成代码 + 验证）            │
└─────────────────────────────────────────────────────┘
```

下面逐步详解。

---

## 三、Step 1：Bootstrap —— 项目初始化与 AI 行为规范

### 3.1 做什么

在写任何业务代码之前，先把项目的"地基"打好：目录结构、构建工具、以及最重要的——**CLAUDE.md**（AI 行为操作手册）。

### 3.2 为什么 CLAUDE.md 是最高优先级

CLAUDE.md 不执行代码，但它约束 AI 如何生成代码。它类似于 `checkstyle.xml` + 架构决策记录（ADR）+ Runbook 的综合体。实际案例表明，一份写好的 CLAUDE.md 能让 AI 生成代码的 Code Review 通过率从 60% 跃升到 93%。

### 3.3 CLAUDE.md 应该包含什么

```markdown
# CLAUDE.md

## 项目概述
一两句话说清楚项目是什么、解决什么问题。

## 技术栈与构建
- 语言：Java 17
- 构建工具：Maven
- 框架：无框架 / Spring Boot / Netty（按实际填写）
- 测试：JUnit 5 + Mockito

## 编码规范
- 命名约定（类名大驼峰、方法名小驼峰、常量全大写下划线）
- 异常处理策略
- 日志格式
- 注释要求

## 架构约定
- 包结构说明（哪个包放什么）
- 核心设计模式
- 模块间依赖规则

## 禁止行为
- 不允许使用 System.out.println 代替日志框架
- 不允许在 Controller 层直接操作数据库
- ...

## 实现顺序 / Checklist
- 建议的开发优先级
```

### 3.4 推荐的项目目录结构

```
项目根目录/
├── CLAUDE.md                        # AI 行为规范（最核心）
├── .claude/
│   └── rules/                       # 条件规则（可按文件类型匹配不同规范）
├── .specify/                        # Spec Coding 产物目录
│   ├── memory/
│   │   └── constitution.md          # 项目章程和原则
│   ├── specs/
│   │   ├── requirements.md          # 需求规范
│   │   └── design.md                # 系统设计
│   ├── tasks/
│   │   └── tasks.md                 # 任务拆解
│   └── templates/                   # 各类模板
├── src/                             # 源代码
├── tests/                           # 测试代码
└── docs/                            # 补充文档
```

### 3.5 实际案例：本项目的 Bootstrap

本项目（RaftByJava）已有的 `CLAUDE.md` 就是一个很好的例子：

- 明确了项目是"Raft 分布式共识算法的 Java 实现"
- 指定了技术栈（Maven）
- 描述了架构参考（三种角色、核心数据结构、关键 RPC）
- 给出了建议实现顺序（10 步 checklist）
- 约定了语言策略（讨论用中文，代码标识符用英文）

---

## 四、Step 2：Specify —— 需求定义

### 4.1 做什么

将模糊的想法转化为结构化的需求文档（`requirements.md`），明确"做什么"和"为谁做"，刻意避免"怎么做"。

### 4.2 需求文档应该包含什么

```markdown
# Requirements: [项目/功能名称]

## 1. 项目目标
- 一句话描述核心价值

## 2. 用户角色
- 谁会使用这个系统？

## 3. 功能需求（按优先级排列）
### FR-1: [功能名称]
- 描述：...
- 验收标准：Given ... When ... Then ...
- 优先级：P0/P1/P2

### FR-2: ...

## 4. 非功能需求
- 性能：...
- 可用性：...
- 安全性：...

## 5. 约束与假设
- 已知技术约束
- 业务假设

## 6. 边界（不做什么）
- 明确 out of scope 的内容
```

### 4.3 实际案例：Raft 项目的需求规范

```markdown
# Requirements: Raft 分布式共识算法 Java 实现

## 1. 项目目标
实现一个功能完整、可用于学习和实验的 Raft 共识算法库，
支持 Leader 选举、日志复制、成员变更三大核心能力。

## 2. 用户角色
- 开发者：将此库集成到自己的分布式系统中
- 学习者：通过阅读代码理解 Raft 算法原理

## 3. 功能需求
### FR-1: Leader 选举
- 描述：节点能在 Leader 失联后自动发起选举，选出新 Leader
- 验收标准：
  - Given 3 节点集群且 Leader 宕机
  - When 选举超时触发
  - Then 剩余节点在 1 个选举周期内选出新 Leader
- 优先级：P0

### FR-2: 日志复制
- 描述：Leader 接收客户端写请求，将日志复制到多数节点后提交
- 验收标准：
  - Given Leader 收到写请求
  - When 多数节点确认 AppendEntries
  - Then commitIndex 推进，状态机应用该命令
- 优先级：P0

### FR-3: 线性一致性读
- 描述：提供可选的强一致性读取能力
- 优先级：P1

### FR-4: 成员变更
- 描述：支持在线添加/移除节点
- 优先级：P2

## 4. 非功能需求
- 性能：单 Leader 写入吞吐 > 1000 ops/s（3 节点本地）
- 可用性：少数派故障不影响集群可用性
- 可测试性：核心逻辑可脱离网络进行单元测试

## 5. 约束
- Java 8+
- 网络通信基于 Netty
- 不依赖第三方共识库

## 6. 不做什么
- 不实现 Multi-Raft（单 Raft Group）
- 不实现生产级持久化（WAL 用简单文件即可）
- 不实现客户端 SDK（只提供服务端库）
```

---

## 五、Step 3：Design —— 方案设计

### 5.1 做什么

基于需求文档，输出系统设计文档（`design.md`），回答"怎么做"：架构选型、模块划分、数据结构、接口定义、关键流程的时序。

### 5.2 设计文档应该包含什么

```markdown
# Design: [项目/功能名称]

## 1. 架构概览
- 整体架构图（用 Mermaid/ASCII 表示）
- 核心组件及其职责

## 2. 模块划分
### 模块 A: [名称]
- 职责：...
- 对外接口：...
- 依赖关系：...

## 3. 核心数据结构
- 结构定义（字段、类型、约束）

## 4. 接口设计
### RPC/API 1: [名称]
- 请求参数：...
- 响应参数：...
- 错误处理：...

## 5. 关键流程（时序图）
- 写请求流程
- 选举流程
- ...

## 6. 持久化方案
- 什么数据需要持久化
- 存储格式

## 7. 错误处理与边界情况
- 网络分区时的行为
- 节点崩溃恢复策略
```

### 5.3 实际案例：Raft 项目的设计（摘要）

```markdown
# Design: Raft Java Implementation

## 1. 架构概览

┌───────────────────────────────────────────────┐
│                  RaftNode                       │
├───────────┬───────────┬───────────┬───────────┤
│ Election  │    Log    │Replication│   State   │
│  Manager  │  Manager  │  Manager  │  Machine  │
├───────────┴───────────┴───────────┴───────────┤
│              RPC Layer (Netty)                  │
└───────────────────────────────────────────────┘

## 2. 模块划分

| 模块 | 类 | 职责 |
|------|------|------|
| 核心状态 | NodeState, NodeRole | 维护 term、votedFor、角色 |
| 选举 | ElectionManager | 超时触发选举、处理投票 |
| 日志 | LogManager, LogEntry | 日志存储、冲突检测、截断修复 |
| 复制 | ReplicationManager | Leader 向 Follower 同步日志 |
| 状态机 | StateMachine | 按序应用已提交日志 |
| 网络 | RaftNettyServer/Client | 节点间 RPC 通信 |
| 持久化 | PersistenceManager | WAL 写入与恢复 |
| 定时器 | TimerManager | 心跳、选举超时管理 |

## 3. 核心数据结构

LogEntry:
  - term: int          // 日志所属任期
  - index: long        // 全局递增索引
  - command: byte[]    // 状态机命令

NodeState:
  - currentTerm: int
  - votedFor: String
  - commitIndex: long
  - lastApplied: long

## 4. 关键 RPC

AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)
  → Response(term, success)

RequestVote(term, candidateId, lastLogIndex, lastLogTerm)
  → Response(term, voteGranted)
```

---

## 六、Step 4：Plan / Tasks —— 任务拆解

### 6.1 做什么

把设计方案拆成可独立实现和测试的原子任务。每个任务的粒度应该是"一个 AI 对话能完成的量"。

### 6.2 任务拆解的原则

- 每个任务可独立编译和测试
- 任务之间的依赖关系清晰（标明前置任务）
- 粒度适中：太粗（"实现选举"）AI 容易跑偏，太细（"写第 3 行代码"）没有意义
- 包含明确的完成标准（Done Criteria）

### 6.3 实际案例：Raft 项目的任务拆解

```markdown
# Tasks: Raft Java Implementation

## Phase 1: 基础骨架（无需网络）

### Task 1.1: 节点状态与角色定义
- 创建 NodeRole 枚举（LEADER, FOLLOWER, CANDIDATE）
- 创建 NodeState 类（currentTerm, votedFor, commitIndex, lastApplied）
- 完成标准：单元测试验证状态转换合法性
- 依赖：无

### Task 1.2: 日志条目与日志管理器
- 创建 LogEntry 类
- 创建 LogManager（append、get、getLastIndex/Term、truncate）
- 完成标准：测试 append 后能正确读取，truncate 能正确截断
- 依赖：无

### Task 1.3: 定时器管理器
- 实现选举超时（随机化 150-300ms）
- 实现心跳定时器（固定间隔）
- 完成标准：超时回调能正确触发
- 依赖：无

## Phase 2: 选举逻辑

### Task 2.1: RequestVote RPC 消息定义
- 定义 RequestVoteRequest / Response
- 依赖：Task 1.1

### Task 2.2: 选举发起逻辑
- ElectionManager.startElection()
- 转为 Candidate，自增 term，投票给自己，广播 RequestVote
- 完成标准：模拟 3 节点，验证能成功选出 Leader
- 依赖：Task 2.1, Task 1.3

### Task 2.3: 投票处理逻辑
- 收到 RequestVote 时的判断逻辑（term 比较、日志新旧比较）
- 完成标准：测试各种边界情况（term 落后、日志更旧等）
- 依赖：Task 2.1, Task 1.2

## Phase 3: 日志复制

### Task 3.1: AppendEntries RPC 消息定义
- 定义 AppendEntriesRequest / Response
- 依赖：Task 1.2

### Task 3.2: Leader 日志复制逻辑
- ReplicationManager: 维护 nextIndex[]、matchIndex[]
- 发送 AppendEntries、处理响应、推进 commitIndex
- 完成标准：3 节点写入后 commitIndex 正确推进
- 依赖：Task 3.1, Task 2.2

### Task 3.3: Follower 日志接收逻辑
- prevLogIndex/prevLogTerm 一致性检查
- 冲突时截断 + 追加
- 完成标准：测试日志冲突修复场景
- 依赖：Task 3.1, Task 1.2

## Phase 4: 状态机与客户端交互
...（继续拆解）
```

---

## 七、Step 5：Review —— 人工评审

### 7.1 做什么

在 AI 开始写代码之前，**人工评审** requirements、design、tasks 三份文档，确认：

- 需求是否遗漏了关键场景？
- 设计是否有明显的技术风险？
- 任务拆解是否合理（粒度、依赖关系、优先级）？

### 7.2 为什么这步不可跳过

AI 生成的 spec 可能看起来很完整，但它无法判断业务上下文中"哪些是真正重要的"。spec-kit 的设计中也强调，每个阶段产物都需要 **Human in the loop** 确认后才进入下一步。这是 Spec Coding 和 Vibe Coding 的关键分水岭。

---

## 八、Step 6：Test Spec —— 测试规范定义

### 8.1 做什么

在写实现代码之前，先定义测试策略和关键测试用例。这不是 TDD 的"先写测试代码"，而是先明确"怎么验证正确性"。

### 8.2 测试规范模板

```markdown
# Test Specification

## 测试策略
- 单元测试：核心逻辑（选举、日志复制）脱离网络测试
- 集成测试：3/5 节点集群的端到端场景
- 混沌测试：随机杀节点、网络分区模拟

## 关键测试场景

### TS-1: Leader 选举基本流程
- 前置条件：3 节点集群，无 Leader
- 操作：等待选举超时
- 期望结果：恰好一个节点成为 Leader，其余为 Follower

### TS-2: Leader 宕机后重新选举
- 前置条件：3 节点集群，Node-1 为 Leader
- 操作：Kill Node-1
- 期望结果：Node-2 或 Node-3 在选举超时后成为新 Leader

### TS-3: 日志冲突修复
- 前置条件：Follower 的日志与 Leader 在 index=5 处有冲突
- 操作：Leader 发送 AppendEntries
- 期望结果：Follower 截断 index>=5 的日志，接受 Leader 的版本

### TS-4: 网络分区恢复
- 前置条件：5 节点集群，2 个节点被隔离
- 操作：恢复网络
- 期望结果：被隔离节点回退到 Follower，同步 Leader 的日志
```

---

## 九、Step 7：Implement —— 逐任务生成代码

### 9.1 做什么

按照 tasks.md 的顺序，逐个任务让 AI 生成代码。每完成一个任务就验证（运行测试），确认无误后再进入下一个。

### 9.2 最佳实践

- **一次只做一个任务**：给 AI 的上下文越聚焦，产出质量越高
- **每个任务的 prompt 结构**：引用 spec + 当前任务描述 + 约束条件
- **完成后立即测试**：不要累积多个任务再测，那样定位问题会很痛苦
- **迭代修正 spec**：实现过程中发现 spec 有遗漏，先更新 spec 再继续

### 9.3 示例 Prompt 结构

```
请实现 Task 2.2（选举发起逻辑）。

相关 spec 参考：
- requirements.md 中 FR-1（Leader 选举）
- design.md 中"选举流程"时序

具体要求：
1. 在 ElectionManager 类中实现 startElection() 方法
2. 逻辑：转 Candidate → term+1 → 投票给自己 → 广播 RequestVote → 收集响应
3. 收到多数票则转 Leader，否则回退 Follower
4. 遵循 CLAUDE.md 中的编码规范

完成后请同时编写对应的单元测试。
```

---

## 十、工具链配置

### 10.1 核心编码工具

| 工具 | 安装方式 | 适合场景 |
|------|----------|----------|
| CatPaw CLI + Claude Code | `bash -c "$(curl -sL https://s3plus.sankuai.com/mcopilot-cli/install.sh)"` + `mc --code` | 命令行重度用户，后台长任务 |
| CatDesk | 桌面 App 下载安装 | 喜欢 GUI 交互、文件预览 |
| Cursor | 官网下载 | VSCode 系用户 |

### 10.2 Spec Coding 工具

**MDP Spec Kit**（美团版 spec-kit）：

```bash
# 安装
uv tool install mdp-specify-cli --from git+ssh://git.sankuai.com/hbar/mdp-spec.git --reinstall

# 在项目中初始化
mdp_specify init .

# 核心命令
mdp_specify specify   # 需求定义
mdp_specify design    # 方案设计
mdp_specify tasks     # 任务拆解
```

**GitHub 原版 spec-kit**（开源版，无内网依赖）：

```bash
# 安装后在 Claude Code 中使用内建命令
/specify    # 想法 → PRD
/design     # PRD → 技术方案
/tasks      # 方案 → 任务拆解
/implement  # 逐任务实现
```

### 10.3 上下文工程工具

**MDP Context**——为 AI 补充项目知识：

```bash
# 添加规则仓库
git remote add mdp-rules ssh://git@git.sankuai.com/hbar/mdp-rule.git
git fetch mdp-rules
git merge mdp-rules/master --allow-unrelated-histories

# 生成项目初始上下文
@init.md
```

### 10.4 推荐 Skill 配置

| Skill 名称 | 用途 | 何时使用 |
|-------------|------|----------|
| brainstorming | 写代码前规划需求和方案 | Step 2-3 阶段 |
| backend-development | 后端 API 设计、数据库架构、TDD | Step 3-7 |
| frontend-design | 前端界面设计 | 有 UI 的项目 |
| skill-creator | 沉淀自己的工作流为可复用 Skill | 流程成熟后 |
| catdesk-browser | 浏览器自动化测试 | 验收测试阶段 |

---

## 十一、AI-Ready 仓库的评估标准

美团内部总结了"AI-Ready 仓库"的概念——AI 对仓库的可理解性、可测性、可持续性。一个 AI-Ready 的仓库应该满足：

| 维度 | 关键指标 |
|------|----------|
| 知识完备性 | 有 CLAUDE.md / AGENTS.md，关键模块有注释和 README |
| 代码可读性 | 命名规范、职责单一、方法长度适中 |
| 可测试性 | 核心逻辑有单元测试，测试覆盖率 > 60% |
| 结构清晰性 | 包结构合理，模块边界清楚 |
| 上下文可获取性 | 有 MCP/知识库接入，AI 能获取框架文档和组件用法 |

实践数据：经过 AI-Ready 改造的仓库，AI Coding 的代码采纳率从 60%+ 提升至 90%+。

---

## 十二、完整工作流时间线（以本项目为例）

```
Day 1: Bootstrap
  ✓ 建好目录结构
  ✓ 写好 CLAUDE.md（已完成）
  ✓ 配置构建工具 pom.xml（已完成）

Day 2: Specify
  → 与 AI 对话，产出 requirements.md
  → 人工评审，确认功能边界和验收标准

Day 3: Design
  → 基于需求产出 design.md（模块划分、接口定义、时序图）
  → 人工评审，确认架构合理性
  → （本项目的"Raft算法-写代码前研究提纲.md"已覆盖大部分内容）

Day 4: Tasks
  → 自动拆解成原子任务
  → 人工评审任务粒度和依赖关系
  → 定义测试规范

Day 5-N: Implement（逐任务）
  → 每个任务：AI 生成代码 → 运行测试 → Review → 合入
  → 发现 spec 遗漏时回溯更新
```

---

## 十三、常见误区与避坑建议

**误区 1：Spec 写一次就够了**
事实：Spec 是活文档。实现过程中一定会发现遗漏或设计不合理的地方，应该及时回溯更新 spec，保持 spec 与代码同步。

**误区 2：AI 生成的 spec 可以直接用**
事实：AI 生成的 spec 看起来很完整，但它不理解你的业务上下文。每个阶段产物必须经过人工评审，这是 Spec Coding 区别于 Vibe Coding 的核心——Human in the loop。

**误区 3：任务越细越好**
事实：过细的任务会导致上下文碎片化，AI 每次只看到一小块拼图，反而写不出连贯的代码。建议每个任务是"一个有意义的功能单元"，通常对应一个类或一个方法族。

**误区 4：写了 CLAUDE.md 就万事大吉**
事实：CLAUDE.md 需要随项目演进持续维护。新增的架构约定、踩过的坑、发现的最佳实践都应该及时补充进去。

**误区 5：Spec Coding 适合所有场景**
事实：改个按钮颜色、修个错别字、加个日志——这些用 Vibe Coding 即可。Spec Coding 的投入适合有一定复杂度的新功能或重构场景。判断标准：如果你需要想超过 5 分钟"这东西该怎么做"，就值得走 Spec 流程。

---

## 七、参考资料

以下均为美团内部学城文档，可直接访问：

- [Spec Coding 研发范式应用（spec-kit、BMAD-METHOD、OpenSpec）](https://km.sankuai.com/collabpage/2728645670)
- [CatPaw - Spec Coding 实践指南](https://km.sankuai.com/collabpage/2750916977)
- [AI-SDLC 全流程管理 - MDP 官方文档](https://km.sankuai.com/collabpage/2751093669)
- [MDP Spec Kit 使用文档](https://km.sankuai.com/collabpage/2742178451)
- [CLAUDE.md 与 Rules：给 AI 定制操作手册](https://km.sankuai.com/collabpage/2754517929)
- [AI-Ready 仓库改造及实验实践](https://km.sankuai.com/collabpage/2759533082)
- [AI 编程基础环境搭建指南（Claude Code + CatPaw + Skills）](https://km.sankuai.com/collabpage/2763112528)
- [【总览】MDP For AI Coding](https://km.sankuai.com/collabpage/2745984738)
- [业务需求 Spec Coding 实践路径](https://km.sankuai.com/collabpage/2721460410)
- [Spec Driven Development 使用](https://km.sankuai.com/collabpage/2733836617)
