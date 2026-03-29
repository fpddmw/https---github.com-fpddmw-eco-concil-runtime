# OpenClaw-First 重构蓝图

## 1. 文档目的

本文用于统一 `openclaw-first` 重构方向，并替代此前同时描述“旧版 runtime 目录形态”和“新 skill-first 形态”的混合表述。

本蓝图只保留一个明确结论：**后续系统应当是 `skills/` 驱动的调查框架，`eco-concil-runtime/` 只保留最小运行时内核。**

---

## 2. 核心决策

1. OpenClaw agent 是主调查者，不再只是 JSON 表单填写器。
2. 业务能力以原子 skill 暴露，agent 通过 skill 调查、比对、挑战、汇总。
3. runtime 只负责不变式：权限、预算、审计、黑板、存储、promotion。
4. 规则从“替 agent 推理”退回到“约束 agent 的安全边界和数据边界”。
5. 数据默认入库并可查询，token 控制主要依靠局部查询、board delta 和 compact receipt，而不是预先把问题压成大 packet。

这意味着未来主路径不再是：

- packet-heavy 的 `source_selection/report/decision` 表单链
- stage-driven 的单轮 JSON 导入链
- 由 supervisor 硬编码完成主要业务判断

而应当是：

- `agent -> skill -> data/board -> promotion`

---

## 3. 需求边界

### 3.1 必须满足的需求

1. OpenClaw 必须成为主调查执行者，而不是被动消费者。
2. 必须支持多 agent 并行、challenge、alternative hypothesis 和 falsification。
3. 数据必须进入可查询的数据面，agent 通过 skill 获取局部证据，而不是长期依赖强制浓缩后的 packet。
4. 必须保留可追溯、可审计、可回放、可 promotion 的正式产物链。
5. 必须尽量复用现有数据库、归档、schema、audit 和路径规范能力。

### 3.2 明确的非目标

1. 不是取消所有规则，仍然需要权限、预算、白名单和 promotion gate。
2. 不是给 agent 开放任意 shell、任意 SQL、任意文件写入。
3. 不是删除 canonical artifact；正式产物仍然要保留，只是不再是 agent 的主工作对象。

---

## 4. 单一目标架构

### 4.1 最终项目形态

```text
skills/
  eco-fetch-.../
  eco-normalize-.../
  eco-query-.../
  eco-lookup-.../
  eco-extract-.../
  eco-link-.../
  eco-board-.../
  eco-investigate-.../
  eco-archive-.../
  eco-promote-.../

eco-concil-runtime/
  src/eco_concil_runtime/
    kernel/
    board/
    storage/
    audit/
    promotion/
    adapters/openclaw/

tests/
```

解释如下：

- `skills/` 是主要业务能力层，也是 OpenClaw 真正工作的接口面。
- 当前仓库 `skills/` 与 detached `/home/fpddmw/projects/skills` 中的技能，最终都应被投影到 OpenClaw 的统一 managed skill surface，而不是长期分裂成“runtime 直调本地脚本”和“外部仓库孤立 skill”两套面。
- `eco-concil-runtime/` 是最小运行时内核，只保留系统不变式。
- 旧的 `controller/application/...` 结构不再作为目标形态描述，只能作为迁移来源、兼容层或 legacy fallback。

### 4.2 runtime 内核职责

runtime 只保留以下能力：

- skill registry、调用调度与 receipt 记录
- append-only audit ledger 与调用事件落盘
- investigation board / workspace DB 管理
- signal plane、archive、raw artifact 的存储适配
- agent 权限、预算、并发和锁
- promotion gate、快照冻结、canonical artifact materialize
- 与 OpenClaw 的最小适配层

这里的“与 OpenClaw 的最小适配层”应优先包括：

- role-agent workspace / session provisioning
- managed skill projection / refresh
- board-driven turn orchestration 接口

凡是“具体调查业务判断”，都不应继续堆在 runtime 内核里。

### 4.3 skill 层职责

所有面向调查的业务能力都应当技能化，包括：

- 拉取或读取数据
- source-specific normalize
- canonical query / lookup
- claim / observation 抽取
- evidence linking 与 hypothesis comparison
- board 写入与任务推进
- archive 检索与历史对照
- promotion 前的汇总与审计

---

## 5. Skill 设计原则

### 5.1 原子化原则

每个 skill 应满足：

- 单一目标明确
- 输入输出 schema 明确
- side effect 可声明
- 能生成 receipt
- 能返回 compact result 和 board handoff
- 自身不是新的“小 supervisor”

命名建议统一为：

- `eco-<verb>-<object>`
- `eco-<verb>-<source>-<object>`

skill 名称表达能力，不表达角色。角色只影响调用权限和默认优先级，不应拥有各自一套独立流程代码。

### 5.2 Normalize 与 Query 的边界

这是后续实现里最需要明确的地方。

1. **Normalize 应当按数据源拆分。**
   不同上游数据源的 payload 结构、可靠字段、时间语义、去重规则、provenance 记录方式都不同，因此 normalize 不应强行共享成一个大 skill。更合理的做法是：每个数据源拥有自己的原子 normalize skill，把原始数据写入统一的 canonical signal plane。

2. **Query/Lookup 应当尽量面向统一数据面共享。**
   一旦数据已经进入 canonical signal plane，下游 agent 不应继续感知每个来源的细节，而应主要通过共享 query/lookup skill 读取公共结构化结果。只有在法证追踪、原文复核或 source-specific 特殊字段不可避免时，才回到 raw lookup。

3. **不要再用规则把信息不可逆地压扁。**
   原始 artifact 和 normalized rows 应保存到文件或数据库，agent 通过 query/lookup 局部读取。规则可以负责查询边界、返回上限和安全约束，但不应把证据长期压成不可复查的 summary。

### 5.3 推荐技能组

建议保留以下技能组，但它们都应以并列 atomic skill 的方式存在，而不是变成新的层级式大模块：

- `fetch`：外部数据源读取与 raw artifact 落盘
- `normalize`：source-specific raw -> canonical signal
- `query/lookup`：统一信号平面与 raw record 查询
- `extract`：claim / observation / scope 候选抽取
- `evidence`：匹配、链接、support / contradiction 建模
- `board`：黑板读写、任务状态、notes、tickets
- `investigate`：probe、challenge、alternative、next action
- `archive`：case library / corpus 检索
- `promote`：从工作态产出正式 artifact
- `audit`：coverage、gap、readiness 与 receipt 汇总

---

## 6. 数据与工作流

### 6.1 统一数据流

推荐主链如下：

```text
外部数据源
  -> fetch skill（可选）
  -> raw artifact
  -> source-specific normalize skill
  -> unified signal plane DB
  -> shared query / lookup skills
  -> extract / evidence / investigation skills
  -> investigation board
  -> promote / audit skills
  -> canonical artifacts / report / decision
```

这条链路比旧式 packet 流程更清晰，因为它明确区分了四种对象：

- `raw artifact`：原始载荷，保留细节与 provenance
- `canonical signal`：统一数据面，供查询与抽取
- `board state`：多 agent 工作态，不等于最终产物
- `promoted artifact`：冻结后的正式结果

### 6.2 理想 agent 工作流

推荐工作流如下：

1. `moderator` 在 board 上创建 round card、预算、核心问题和候选假说。
2. `archivist` 先读取历史案例和已归档模式，写入 board 的对照提示。
3. `sociologist` 与 `environmentalist` 读取 board delta，并通过 query/lookup skill 获取当前证据面。
4. 如果关键数据缺失，再由 agent 主动调用 fetch / normalize skill，把新增数据写回 signal plane。
5. `extract` 与 `evidence` 类 skill 生成 claim、observation、support、contradiction 和 gap hint。
6. `challenger` 基于 board 当前主解释，主动创建 alternative hypothesis 和 falsification task。
7. 各 agent 只把 refs、notes、ticket、status 和 compact summary 写回 board，不直接手工维护最终 report JSON。
8. `moderator` 根据 board、receipts 和 challenge 状态决定：进入 promotion，或继续下一轮 probe。
9. `promote` 类 skill 将已批准的 board refs 冻结为 canonical artifact、report section 和 decision basis。

### 6.3 治理约束与推理自由

原子化 skill 不意味着取消议会流程，也不意味着无法保留轮次编排。

后续应采用的是 **轻编排 + 强 gate**，而不是旧式的僵硬表单流水线。

应当继续强制的内容包括：

- round 的开启、关闭和快照冻结
- agent 角色权限、预算和并发约束
- challenge / falsification 的最低覆盖要求
- promotion 的进入条件和审批点
- skill receipt、artifact ref、board handoff 的记录义务

不应继续强制的内容包括：

- 每一轮必须填写固定 JSON 表单
- 所有 agent 必须走同一条 `candidate -> match -> report` 流水线
- 所有调查结论都必须先经过单一路径的 supervisor 规则筛选

换句话说，**议会流程仍然存在，但它应当约束治理边界，而不是替 agent 规定唯一推理路径。**

### 6.4 匹配与候选机制的定位

为避免后续再出现“候选和匹配是规则硬编码还是由 LLM 自由组合”的歧义，这里明确：

- 候选生成本身应由 `extract` 类 skill 完成，输入是统一数据面中的 canonical signals。
- 匹配、链接、support/contradiction 计算应由 `evidence` 类 skill 完成，必要时可以结合确定性规则和 LLM 辅助，但对外暴露为独立 skill，而不是藏在 supervisor 主流程里。
- LLM 的职责是决定**何时调用哪些 skill、如何比较结果、是否继续 challenge**；不是把所有判断重新内嵌回一个大 prompt。
- 确定性规则仍然重要，但它们应该以 skill backend 或 guardrail 的形式存在，而不是继续充当系统主推理器。

更进一步说，`candidate/match` 在 v2 中应被明确视为**分析工具**，而不是强制漏斗：

- 它们输出的是 `proposal`、`candidate set`、`match hypothesis`，不是系统唯一真值。
- 允许多种实现并存，例如 rule-based、LLM-assisted、hybrid 版本同时存在。
- board 上应记录每个候选或匹配结果的 `method`、`params`、`receipt`、`source refs` 和状态。
- agent 应可以接受、拆分、驳回、重开或绕过某个 `candidate/match` 结果，而不是只能继承它。
- promotion 的门槛应是证据链是否充分、challenge 是否处理完成，而不是某个单一匹配分数是否过线。

---

## 7. 对现有代码的处理原则

### 7.1 直接保留

以下部分应尽量保留，作为 v2 的基础设施或 backend：

- signal plane / analytics SQLite
- raw artifact 存储约定
- case library / signal corpus
- audit、snapshot、receipt 机制
- schema / contract 校验
- 路径、IO、manifest 等适配层

### 7.2 优先技能化

以下部分适合保留实现，但应改造成 atomic skill 或 skill backend：

- 各类 normalize adapter
- candidate extraction
- evidence linking / matching
- archive retrieval
- investigation action planning
- report / decision 的部分中间汇总逻辑

### 7.3 降级为 legacy fallback

以下部分不应再作为未来主路径扩张：

- packet-heavy 的 source selection / report packet
- stage-driven 的 agent JSON turn
- `run-agent-step` 风格的表单导入主循环
- supervisor 中承担主要业务推理的硬编码链条

这些能力可以保留为：

- baseline benchmark
- 回归测试路径
- fallback 兼容方案

但不再作为目标架构中心。

---

## 8. 当前已实现的方向

当前仓库已经出现了与本蓝图一致的第一批技能化成果，说明方向不再只是概念设想。

### 8.1 已完成的 source-specific normalize skill

- `eco-normalize-gdelt-doc-public-signals`
- `eco-normalize-youtube-video-public-signals`
- `eco-normalize-bluesky-cascade-public-signals`
- `eco-normalize-openaq-observation-signals`
- `eco-normalize-airnow-observation-signals`
- `eco-normalize-open-meteo-historical-signals`

这说明 normalize 层已经开始按数据源拆分，而不是继续维护一个过粗的统一 normalize 流程。

### 8.2 已完成的共享 query / lookup skill

- `eco-query-public-signals`
- `eco-query-environment-signals`
- `eco-lookup-normalized-signal`
- `eco-lookup-raw-record`

这说明下游调查已经开始面向统一 signal plane，而不是强依赖 packet。

### 8.3 已完成的 extract / audit skill

- `eco-extract-claim-candidates`
- `eco-extract-observation-candidates`
- `eco-build-normalization-audit`

这说明当前已经具备了从 canonical signal 继续向候选对象和审核摘要推进的雏形。

### 8.4 这意味着什么

当前最合理的判断不是“再回头设计一套新的统一 normalize 大模块”，而是：

- 继续沿着“source-specific normalize + shared canonical query + extraction/audit”这条线推进
- 把剩余的 evidence、board、investigation、promotion 继续补齐为 skill
- 让 runtime 收缩成真正的最小内核

---

## 9. 重构路线

### Phase 1：统一 contract 与命名

目标：先把 skill 的输入输出、receipt、board handoff 和命名规则统一下来。

产出：

- 统一 skill contract
- 统一 receipt / artifact ref 结构
- 统一 audit event / append-only ledger 结构
- 统一 signal plane 与 board 的接口边界

### Phase 2：建立 board 与 workspace DB

目标：让 agent 主路径从 packet 切到 board delta。

产出：

- investigation board schema
- board read/write skills
- workspace DB 与快照机制

### Phase 3：补齐 evidence / investigation / promotion skill

目标：把目前仍留在 runtime 或脚本中的核心业务逻辑继续拆成 atomic skill。

产出：

- evidence linking skills
- challenge / falsification skills
- board action skills
- promote / audit skills

### Phase 4：切换主工作流

目标：让 `agent + board + skill mesh` 成为默认主路径，旧表单链退为 fallback。

产出：

- 新 orchestrate loop
- 多 agent 并行与挑战回路
- legacy baseline 对照测试

---

## 10. 风险与控制

### 10.1 主要风险

- skill 设计过粗，重新长成新的“大流程函数”
- agent 自由度过高，导致查询漫游和 token 失控
- board 工作态与最终 promoted artifact 脱节
- 过早删除 legacy baseline，导致无法比较重构收益

### 10.2 控制方式

- 采用参数化 query、字段白名单、返回上限和时间窗口约束
- 强制记录 receipt、artifact ref 和 board handoff
- 强制记录 skill 调用事件、读写对象和快照哈希
- 只允许从已批准的 board refs promotion 到正式产物
- 保留 legacy baseline 作为 benchmark，而不是立即删除

---

## 11. 验收标准

当以下条件成立时，才说明本次重构真正成功：

1. OpenClaw agent 已经通过 skill 直接工作，而不是主要填写 JSON 表单。
2. 主要业务推理已经迁移到 `agent + skill + board`，而不是 supervisor 硬编码链。
3. promoted artifact 可以追溯到 board refs、skill receipts 和 raw artifacts。
4. challenger 与 alternative hypothesis 已经成为常态流程，而不是可有可无的附加项。
5. v2 在至少一批真实任务上，相比 legacy baseline 同时获得更好的调查覆盖度、反证能力和 token 效率。

---

## 12. 最终定义

本项目的最终理想形态应当是：

**一个以 OpenClaw 多智能体协作为核心、以原子 skill mesh 为主要行动接口、以 unified signal plane 和 investigation board 为工作介质、以 promotion 机制保证审计与归档的调查型运行时。**

更简化地说：

- agent 负责调查与判断
- skill 负责操作数据与能力复用
- runtime 负责边界、不变式和审计
- board 负责协作
- promotion 负责把工作态冻结成正式结果

这就是后续重构应当收敛到的唯一目标形态。
