# OpenClaw 数据层级技术报告

## 1. 文档目的

这份文档只讲一件事：

`当前项目的数据到底分成哪几层；未来理想蓝图里，这些层又应该怎样重组。`

重点不是“顺序跑了哪些步骤”，而是：

1. 原始数据放在哪
2. 归一化数据放在哪
3. `claim` 和 `observation` 处在什么层
4. 更上层的 `card`、board、reporting 又处在什么层

全文按两个部分组织：

1. 当前阶段的真实数据层级
2. 蓝图阶段的理想数据层级

## 2. 先给结论

当前项目已经形成了一条相对清晰的数据上升路径：

1. 治理元数据
2. mission / round planning
3. raw artifacts
4. normalized signals
5. claim / observation candidates
6. cluster / merge / link / coverage 等分析中间物
7. board state 与各种 `card`
8. promotion / reporting / archive

但当前它还有一个明显特点：

`下层的证据底座已经开始 DB 化，上层的大量分析与议事对象仍主要以 JSON / Markdown 工件存在。`

蓝图阶段要做的，不是推翻这个层级，而是把它改造成：

1. `raw` 和 `normalized` 进入统一 evidence plane
2. `claim / observation / support / contradiction` 降级为 analysis result sets
3. `card` 与 board state 进入独立 deliberation plane
4. JSON / Markdown 退回到 export 和 snapshot 角色

## 3. 当前阶段：真实数据层级

### 3.1 第 0 层：治理元数据层

这层不直接承载调查内容，而是承载运行系统本身的治理状态。

当前主要对象包括：

1. `runtime/run_manifest.json`
2. `runtime/round_cursor.json`
3. `runtime/audit_ledger.jsonl`
4. `runtime/skill_registry.json`
5. `runtime/admission_policy.json`
6. `runtime/runtime_health.json`
7. `dead_letters/`
8. `receipts/`

这层回答的是：

1. 这次 run 叫什么
2. 当前 round 是哪一轮
3. 哪个 skill 被执行过
4. 副作用是否被批准
5. 有没有失败、重试、dead-letter

这层不属于业务证据，但它决定了全系统的可审计性。

### 3.2 第 1 层：mission 与 round 规划层

这层描述“这一轮要做什么”，而不是“这一轮已经看到了什么”。

当前主要对象包括：

1. `mission.json`
2. `investigation/round_tasks_<round_id>.json`
3. `runtime/source_selection_<role>_<round_id>.json`
4. `runtime/fetch_plan_<round_id>.json`
5. `runtime/round_transition_<round_id>.json`

它们的职责分别是：

1. `mission.json`
   冻结调查目标、时间窗、地区、数据源入口
2. `round_tasks`
   把一轮工作拆成角色化任务
3. `source_selection`
   决定本轮允许选哪些 source
4. `fetch_plan`
   把取数动作显式化为可执行步骤
5. `round_transition`
   把“从上一轮切到下一轮”这件事显式化

这层还是“规划层”，不是“证据层”。

### 3.3 第 2 层：原始证据层 `raw`

这层是最接近外部世界的一层。

当前它主要由文件系统中的 artifacts 组成，包括：

1. import 进来的原始 JSON
2. detached fetch 得到的原始输出
3. runtime 为各 source 落盘的原始文件

这层的特点是：

1. 形状不统一
2. 强依赖具体 source
3. 仍保留最丰富、最少压缩的信息

它的价值不是方便 agent 直接消费，而是：

1. 保留回溯源
2. 作为 normalize 的输入
3. 作为争议时的最终原始证据

### 3.4 第 3 层：归一化证据层 `normalized signals`

这是当前项目最关键的一层。

当前所有 source 的 normalize 结果会进入：

`analytics/signal_plane.sqlite`

其中核心表是：

1. `normalized_signals`

这一层的特点是：

1. 已经统一为同一张结构化表
2. 仍保留 `artifact_path`、`record_locator`、`raw_json`
3. 每条记录都带 `run_id`、`round_id`
4. 现在已经支持跨轮读取

这层回答的是：

1. 这条标准化信号是什么
2. 它来自哪个 source
3. 它属于 public 还是 environment
4. 它对应哪个 raw artifact
5. 它属于哪一轮调查

这是当前项目最接近“统一证据面”的部分。

### 3.5 第 4 层：候选抽取层 `claim / observation`

这层是当前项目第一个明显的语义压缩层。

#### `claim` 侧

当前 public signals 会被进一步压成：

1. `claim_candidates_<round_id>.json`
2. `clustered_claim_candidates_<round_id>.json`

这意味着系统会把文本性、叙事性信号先压成可讨论的 claim 对象。

#### `observation` 侧

当前 environment signals 会被进一步压成：

1. `observation_candidates_<round_id>.json`
2. `merged_observation_candidates_<round_id>.json`

这意味着系统会把观测记录压成更适合比较和链接的 observation 对象。

所以目前的 `claim` 和 `observation` 所在层级可以总结为：

`它们位于 normalized signals 之上、board state 之下，是当前系统的语义候选层。`

### 3.6 第 5 层：关系分析层

在 `claim` 和 `observation` 之上，当前项目还会继续生成更高一层的中间分析物，例如：

1. `claim_observation_links_<round_id>.json`
2. `claim_scope_proposals_<round_id>.json`
3. `observation_scope_proposals_<round_id>.json`
4. `evidence_coverage_<round_id>.json`
5. `normalization_audit_<round_id>.json`

这层做的是：

1. 把 public 与 environment 两个平面关联起来
2. 给出 support / contradiction 倾向
3. 给出 coverage 和 readiness 倾向

所以如果用“抽象层级”来描述，当前系统其实是：

1. raw
2. normalized signal
3. claim / observation
4. link / scope / coverage

### 3.7 第 6 层：议事对象层 `card / board`

这是你特别关心的一层。

当前项目里，严格来说已经存在两种不同含义的 `card`：

#### 第一类：board card

例如：

1. hypothesis card
2. challenge ticket
3. task card
4. probe
5. board note

其中最明确写成 `card` 的，是 hypothesis card。

这些对象当前被保存在：

1. `board/investigation_board.json`

它们的作用不是保存原始证据，而是把证据转化成：

1. 可讨论对象
2. 可挑战对象
3. 可推进对象

#### 第二类：archive / reporting 层的 evidence card

当前在 case archive 相关逻辑里，已经有 `evidence-card` 这种更高层的摘要对象类型。

它本质上是：

1. 从 promotion / reporting 结果中抽取出的证据摘要卡片
2. 供 archive / retrieval / history context 使用

所以如果按层级看：

1. `claim / observation` 还是分析候选层
2. `card` 已经更接近议事层和知识封装层

### 3.8 第 7 层：冻结、报告与归档层

这是当前数据层级的顶部。

当前主要对象包括：

1. `promotion/promotion_gate_<round_id>.json`
2. `promotion/promoted_evidence_basis_<round_id>.json`
3. `reporting/reporting_handoff_<round_id>.json`
4. `reporting/expert_report_*_<round_id>.json`
5. `reporting/council_decision*.json`
6. `reporting/final_publication_<round_id>.json`
7. case library / signal corpus archive

这一层的共同特点是：

1. 面向冻结、对外表达、历史沉淀
2. 不是调查内循环的主工作面

## 4. 当前阶段：一张总层级图

可以把当前项目的数据层级画成下面这样：

```text
治理元数据
  -> mission / round planning
    -> raw artifacts
      -> normalized signals
        -> claim / observation candidates
          -> link / scope / coverage / audit
            -> board cards / council state
              -> promotion / reporting / archive
```

其中最关键的三个判断是：

1. `raw` 是原始事实层
2. `normalized signals` 是统一证据层
3. `claim / observation` 还不是最终议事对象，而是中间语义层

## 5. 当前阶段：数据层级问题

### 5.1 当前最强的一层是 `normalized signals`

它已经开始具备 DB-first 味道：

1. 有统一表结构
2. 有 provenance
3. 有跨轮字段
4. 有 query skill 直接读取

### 5.2 当前最脆弱的一层是 `claim / observation -> link / coverage`

原因不是这些层没用，而是：

1. 它们现在太容易被当成唯一主链
2. 它们大多还是 JSON 工件
3. 它们还没有进入统一 analysis plane

### 5.3 当前 `card` 层还没有完全独立出来

现在的 board card 已经存在，但仍然有两个问题：

1. 还主要寄存在单一 `investigation_board.json`
2. 与下层分析物的关系还没有被建成真正可查询的关系面

## 6. 蓝图阶段：理想数据层级

蓝图里的理想状态，不是简单把当前层次原样搬进数据库，而是重新分成五个大平面。

### 6.1 Governance Plane

保存：

1. run metadata
2. runtime events
3. skill invocations
4. agent sessions
5. agent turns

它相当于把当前第 0 层提升成正式的 DB 平面。

### 6.2 Evidence Plane

保存：

1. fetch requests
2. fetch executions
3. raw artifacts
4. normalized signals
5. signal text FTS

这意味着蓝图里：

1. `raw`
2. `normalized`

都会成为统一 evidence plane 的一部分。

### 6.3 Analysis Plane

这是蓝图中最关键的新层。

保存：

1. `analysis_result_sets`
2. `analysis_result_rows`
3. `analysis_lineage`

这意味着未来的：

1. claim grouping
2. observation grouping
3. support / contradiction ranking
4. coverage scoring
5. history similarity retrieval

都不应再首先表现为一串固定 JSON 工件，而应表现为：

`一个可重复、可比较、可追溯的结果集系统`

### 6.4 Deliberation Plane

保存：

1. `board_entities`
2. `board_relations`
3. `board_events`

这就是未来 `card` 真正应该落的地方。

到那时：

1. hypothesis card
2. challenge card
3. task card
4. probe card
5. evidence pack
6. round transition state

都应成为 deliberation plane 的显式对象，而不是散落在 JSON 里的状态块。

### 6.5 Publication Plane

保存：

1. promotion freezes
2. report exports
3. final publication exports

这层仍然是最上层，但不再承载主分析逻辑。

## 7. 蓝图阶段：`claim`、`observation`、`card` 的理想位置

### 7.1 `claim`

理想状态下，`claim` 不应再被视为唯一固定层，而应是：

1. 一种 analysis result row entity
2. 一种 narrative grouping 或 candidate ranking 的结果
3. 可被不同工具重复生成的中间对象

换句话说：

`claim` 在理想态里仍然重要，但它应属于 analysis plane，而不是唯一官定骨架。

### 7.2 `observation`

`observation` 的理想位置与 `claim` 类似：

1. 它是环境信号的中间抽象
2. 它可来自不同 grouping / merge / pattern 工具
3. 它也是 analysis plane 的结果对象

### 7.3 `card`

`card` 在理想态里应明显高于 `claim / observation`。

可以这样理解：

1. `claim / observation` 是分析对象
2. `card` 是议会对象

也就是说：

1. 一个 hypothesis card 可以引用多个 claim-like results
2. 一个 challenge card 可以引用多个 contradiction rows
3. 一个 evidence pack card 可以引用多个 support / coverage / sample rows

所以 `card` 的理想层级是：

`它位于 analysis plane 之上，属于 deliberation plane。`

## 8. 当前实现 vs 理想蓝图：简单对比

### 8.1 分层方式对比

| 主题 | 当前实现 | 理想状态 |
| --- | --- | --- |
| 原始层 | 文件系统 raw artifacts | evidence plane 中的 raw_artifacts |
| 归一化层 | `signal_plane.sqlite` + `normalized_signals` | evidence plane 中的核心统一表 |
| claim / observation | 固定 JSON 中间层 | analysis result sets / rows |
| support / contradiction / coverage | 固定 JSON 分析物 | 可重复生成的 analysis capabilities |
| card / board | `investigation_board.json` 为主 | `board_entities / relations / events` |
| reporting | JSON / Markdown 导出物 | publication plane 导出物 |

### 8.2 `claim / observation / card` 位置对比

| 抽象对象 | 当前实现 | 理想状态 |
| --- | --- | --- |
| `claim` | 候选抽取层 | analysis plane |
| `observation` | 候选抽取层 | analysis plane |
| `card` | board / archive 上层对象，但还分散 | deliberation plane 中的标准对象 |

## 9. 给老师或评委的稳妥说法

如果你要对外解释当前数据层级，最稳妥的说法是：

1. 当前系统已经完成了从 raw 到 normalized 的统一证据底座。
2. 在此之上，系统还能生成 claim、observation、link、coverage 等分析中间物。
3. 再往上，系统通过 hypothesis、challenge、task 等 board card 进入议事层。
4. 最上层才是 promotion、reporting 和 archive。

如果你要解释蓝图目标，最稳妥的说法是：

1. 未来要把 `claim / observation` 从“固定流水线步骤”改成“analysis plane 的结果集对象”。
2. 要把 `card` 和 board state 从 JSON 提升成 deliberation plane 的正式状态对象。
3. 要让 JSON / Markdown 重新退回 export 和 snapshot 的位置。

## 10. 近期最关键的数据层演化方向

如果只看数据层，不看 agent，我建议后续优先级是：

1. 保住 `raw -> normalized` 这条证据底座。
2. 把 `claim / observation / coverage` 从固定 JSON 主链降级为 analysis result sets。
3. 把 board card 层从单文件状态提升为可查询状态面。
4. 把 reporting / archive 层继续保留为冻结与导出边界。

一句话总结：

`当前项目已经有了清晰的数据上升路径，但只有下层证据面真正开始 DB 化；蓝图要做的，是把中上层的分析面和议事面也从工件链提升为真正的状态平面。`
