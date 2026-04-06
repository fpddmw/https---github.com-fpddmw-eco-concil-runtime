# OpenClaw Agent 权责与协作边界技术报告

## 1. 报告定位

本报告说明项目中不同 agent 角色各自负责什么、能写什么、彼此如何协作，以及 runtime 为什么不直接替代 agent 做调查判断。

## 2. 角色设计的核心原则

当前项目对 agent 体系的判断很明确：

1. 角色不能只是标签。
2. 角色应逐步成为独立工作单元。
3. 不同角色要有不同的默认写边界。
4. runtime 负责治理，不负责替 agent 下调查结论。

因此，agent 设计不是为了“拟人化”，而是为了把调查分工、写权限和责任归属做清楚。

## 3. 五类核心角色

### 3.1 sociologist

默认职责：

1. 查询 public evidence。
2. 做 narrative grouping。
3. 生成 claim-like result sets。
4. 补公共叙事侧证据。

默认可写对象：

1. public analysis result sets。
2. board notes。
3. claim-oriented hypothesis proposals。

它更偏向“社会叙事、政策讨论、舆论表达、公共话语”的分析者。

### 3.2 environmentalist

默认职责：

1. 查询 environment evidence。
2. 生成 observation / pattern result sets。
3. 补时空环境证据。
4. 做指标、空间、时间层面的环境解释。

默认可写对象：

1. environment analysis result sets。
2. board notes。
3. observation-oriented hypothesis proposals。

它更偏向“环境观测、污染指标、物理背景、环境机制”的分析者。

### 3.3 challenger

默认职责：

1. contradiction scan。
2. source overlap 检查。
3. sample-and-review。
4. rebuttal 与 falsification。

默认可写对象：

1. challenge tickets。
2. probes。
3. rebuttal result sets。

它的价值不在“补更多信息”，而在“阻止系统过早相信一个顺滑叙事”。

### 3.4 moderator

默认职责：

1. 读取多方结果。
2. 推进议会状态。
3. 决定继续当前轮、开启下一轮还是请求 freeze。
4. 协调 notes、tasks、challenges、probes 和 round transition。

关键点在于：

`moderator 是议会状态推进者，不是唯一调查执行者。`

也就是说，moderator 不是全能代理，而是负责协调、裁剪和推进状态。

### 3.5 runtime

runtime 只负责治理，不负责调查判断。

runtime 的职责包括：

1. admission
2. sandbox
3. timeout / retry
4. side effect approval
5. ledger / receipt / dead-letter
6. archive / replay / benchmark
7. promotion / publication gate

它更像“制度与基础设施”，而不是“内容判断者”。

## 4. 当前 agent 设计的现实状态

需要在汇报中明确区分“已实现”和“目标形态”。

### 4.1 已实现部分

当前已实现的，不是完整的多 session agent 社会，而是：

1. 角色语义已经明确。
2. 很多 skill 已经带有 `assigned_role_hint` 或 owner_role 语义。
3. board、task、challenge、probe 等写对象已有明确分类。
4. moderator control surface 已具备 DB-backed 恢复能力。
5. runtime 已经具备对 skill contract、执行策略、side effect 的治理能力。

### 4.2 尚未完全实现的部分

当前蓝图也明确承认，角色仍有不足：

1. 仍不是完全独立 session。
2. 还没有成熟的独立工作记忆边界。
3. 写权限边界更多体现在对象类别和 skill 契约上，而不是硬隔离的 agent workspace。
4. 多 agent turn loop 还没有完全进入主运行路径。

因此，当前系统更准确的描述是：

`已经完成角色化的状态面与技能面设计，但尚未完全完成多 agent 会话化。`

## 5. 为什么必须做角色分工

项目之所以强调 agent 权责，不是为了抽象得更复杂，而是因为调查本身存在天然张力：

1. 一类角色擅长发现主叙事。
2. 一类角色擅长发现环境证据结构。
3. 一类角色必须专门负责否证、挑错和施压。
4. 一类角色必须站在流程与状态推进视角，决定“现在该继续查，还是已经足够冻结”。

如果不把这些职责拆开，就会出现两个问题：

1. controller 或单个 summary skill 过早替系统做判断。
2. 线性流水线会把早期启发式错误一路传到最终报告。

## 6. 当前协作协议

按照现有蓝图和实现，角色协作大致遵循以下协议：

1. sociologist 和 environmentalist 从 evidence / analysis plane 读取证据与分析结果。
2. challenger 对已形成的叙事与证据关系做反驳、重检和 probe 设计。
3. moderator 综合多方结果，调用 state-change skill 修改 board。
4. runtime 对状态修改、外部副作用、发布和归档进行治理约束。

这其实形成了一种“分析权、质疑权、推进权、治理权”分离的结构：

1. 分析权由 domain agents 持有。
2. 质疑权由 challenger 持有。
3. 推进权由 moderator 持有。
4. 治理权由 runtime 持有。

## 7. 写边界为何重要

项目中特别强调“默认可写对象”，是因为一旦没有写边界，系统会出现三个风险：

1. 任意角色都能直接改 board 状态，导致责任不可审计。
2. 调查角色与治理角色混淆，导致 runtime 失去约束力。
3. 质疑方和裁决方混为一体，导致反证机制失效。

因此，当前设计至少已经建立了一种清晰趋势：

1. analysis result sets 由分析角色主写。
2. deliberation state 由 moderator 和 state-change skills 主写。
3. promotion / publication 仍由 runtime gate 和 reporting chain 共同约束。

## 8. 课题组汇报时的推荐说法

可以用下面的表达：

1. 这个项目并不是把一个大模型包成“万能助手”，而是在构造一个有分工、有权限边界、有治理约束的多角色调查系统。
2. sociologist、environmentalist、challenger 和 moderator 分别承担叙事分析、环境分析、否证施压和状态推进四类职责。
3. runtime 不直接决定调查结论，而是负责审批、账本、回放、发布等高风险治理边界。

## 9. 后续演化方向

后续真正值得投入的 agent 方向主要有四个：

1. 独立 session / workspace / permission boundary。
2. 更清晰的 per-role memory 与 context surface。
3. moderator、challenger、domain agents 的并行 turn loop。
4. governance bridge，使 agent 的高风险动作显式提交给 runtime 审核。

这也是为什么 `A4 Agent Entry Gate` 被放在后续路线中，它不是简单做个入口，而是要在治理稳定后，真正把 agent route 接通。

