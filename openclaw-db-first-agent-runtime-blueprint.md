# OpenClaw Agent Mode / DB-First Runtime Blueprint

## 1. 文档定位

这份蓝图描述下一阶段的目标形态：

1. 保留当前 `runtime route` 作为治理执行面。
2. 新建一条真正适配 OpenClaw 的 `agent mode` 主工作面。
3. 让 agent 直接围绕同一 `run` 的持久化数据库开展多轮调查，而不是被固定的 JSON / Markdown 工件链牵着走。

这不是对当前 runtime 的否定。

当前 runtime 已经较好地完成了它应该承担的职责：

1. admission
2. sandbox
3. side effect 治理
4. ledger / receipt / dead-letter
5. archive / replay / benchmark
6. promotion / publication 边界

下一阶段真正要重构的，不是这些治理能力，而是上层“如何让 agent 使用这些能力并自由调查”的工作面。

## 2. 为什么它天然适配 OpenClaw Agent Mode

`DB-first agent runtime` 与 OpenClaw agent mode 天然契合，原因很直接。

### 2.1 OpenClaw 的强项本来就不是固定表单流程

OpenClaw 的价值在于：

1. 自主决定下一步调用什么工具。
2. 在调查中途改变路径。
3. 根据新证据补抓数据、回查历史、重新构造假设。
4. 让多个 agent 在共享状态面上分工与对抗。

如果系统仍然要求：

1. 先 `candidate`
2. 再 `cluster`
3. 再 `link`
4. 再 `coverage`
5. 再 `readiness`

而且每一步都只能消费上一步压缩好的产物，那么 OpenClaw 只是一个被规则驱动的表单填写器。

### 2.2 数据库能把长上下文从 token 面转移到工具面

agent 调查的核心矛盾不是“信息太少”，而是“信息太多，不能每次都塞进上下文”。

DB-first 的好处是：

1. 大量数据持久化在库里，不进入 prompt。
2. agent 只在需要时取局部切片、统计摘要、采样结果。
3. 分析工具返回 `result_set_id` 而不是整批数据正文。
4. agent 可以多次追问同一批数据，而不需要反复重载整份 JSON。

这比把海量 normalize 结果持续压成一层又一层中间文件，更适合控制 token。

### 2.3 多 agent 协作需要共享、可查询、可写边界清晰的状态面

OpenClaw agent mode 真正需要的是：

1. 一个共享证据面
2. 一个共享分析面
3. 一个共享议事面
4. 一组受控写边界

数据库比“几十个一步一文件的中间产物”更适合承担这个角色。

## 3. 当前结构的核心问题

当前项目最主要的问题，不是 skill 不够多，而是很多 skill 承担了错误的职责。

### 3.1 过早语义压缩

目前很多 skill 会把 `normalized_signals` 过早压缩成：

1. claim candidates
2. observation candidates
3. clusters / merges
4. links
5. coverage
6. readiness

这类压缩本身不是错。

问题在于它们被当成主链必经步骤，结果是：

1. agent 先看到压缩结论，再看到原始证据。
2. agent 很难绕开某个启发式。
3. 一个早期启发式错误会层层放大。
4. 很多替代解释在进入 board 前就被过滤掉。

### 3.2 基于文件的工件链过于线性

当前大量流程依赖：

1. `claim_candidates_<round>.json`
2. `merged_observation_candidates_<round>.json`
3. `claim_observation_links_<round>.json`
4. `evidence_coverage_<round>.json`
5. `board_brief_<round>.md`
6. `round_readiness_<round>.json`

这些工件用于审计和导出没有问题，但如果它们承担主分析平面，就会带来明显缺点：

1. 一步写死下一步的数据形状。
2. 很难做高频迭代查询。
3. 很难保存多套候选解释。
4. 很难支持多 agent 同时工作。
5. 很难进行灵活的 branch / compare / merge。

### 3.3 强 stage / 强 skill 绑定压低 agent 自主性

当前 `controller` / `phase2_contract` 的问题不在于“有 contract”，而在于：

1. exact stage name 被写死。
2. exact skill name 被写死。
3. exact dependency order 被写死。
4. planner queue 默认拥有主导权。

这会把规划权从 agent 手里夺走。

### 3.4 角色目前只是标签，不是实体 agent 边界

现在的 `sociologist`、`environmentalist`、`moderator`、`challenger` 更多是：

1. source 选择标签
2. board owner 标签
3. 报告视角标签

它们还不是：

1. 有独立会话的 agent
2. 有独立工作记忆的 agent
3. 有独立写权限边界的 agent
4. 有共享协作协议的 agent

## 4. 目标形态

一句话概括：

`数据库做主工作面，skill 做分析工具，文件做快照与导出物，runtime 做治理边界，OpenClaw 做主调查者。`

### 4.1 运行时总分层

目标系统分成五层。

#### A. Governance Plane

由 runtime 继续负责：

1. admission
2. sandbox
3. timeout / retry
4. side effect approval
5. ledger / receipt / dead-letter
6. archive / replay / benchmark
7. promotion / publication gate

#### B. Evidence Plane

存放：

1. raw artifacts
2. normalized signals
3. 历史 case / corpus 检索索引

#### C. Analysis Plane

存放：

1. agent 调查中形成的各类结果集
2. 筛选结果
3. 分布统计
4. 聚类结果
5. 相互支持 / 相互矛盾评分
6. 采样与抽检结果

#### D. Deliberation Plane

存放：

1. board notes
2. hypotheses
3. challenge tickets
4. tasks
5. probes
6. moderator 决策对象

#### E. Export Plane

只负责对外物：

1. board brief
2. history context
3. reporting handoff
4. expert reports
5. council decision
6. final publication

### 4.2 主工作流应当如何变化

未来主工作流不应再是：

`fetch -> normalize -> candidate -> cluster -> link -> coverage -> readiness -> report`

而应变成：

1. `fetch / import / normalize`
2. agent 直接查询 `normalized_signals`
3. agent 根据需要调用分析工具生成若干 `result sets`
4. agent 依据 `result sets` 写入 board / hypothesis / challenge
5. moderator 或其他角色 agent 决定是否继续取证、是否收敛
6. 只有在准备 promotion / publication 时才进入严格 gate
7. 由导出类 skill 从数据库和 board 中生成汇报与发布物

也就是说：

1. 调查内循环以 DB + tools 为中心
2. 发布外循环以 gate + export 为中心

当前过渡实现也应向这个方向靠拢：

1. 是否开启下一轮，应由 moderator 调用显式的 council-state skill 决定，而不是藏在 controller 内部。
2. 跨轮查询应允许 agent 在 `round-003` 回看 `round-001`，而不是默认只能读“当前轮压缩结果”。
3. `round_id` 仍然保留，但它应该是查询维度与治理边界，而不是记忆封锁线。

## 5. 存储模型

### 5.1 一 run 一主库

理想形态建议采用：

1. 每个 `run` 一个主数据库
2. 以 SQLite 为默认实现
3. 继续保留原始文件目录

建议目录形态：

```text
run_dir/
  artifacts/
    raw/
    imports/
    detached-fetch/
  exports/
    board/
    reporting/
    publication/
  state/
    run_state.sqlite
  runtime/
    operator_runbook.md
    manifest_snapshots/
```

说明：

1. 原始抓取文件仍保留在文件系统。
2. 数据库保存元数据、索引、引用、分析结果、board 状态。
3. Markdown / JSON 主要作为 export，不再充当主分析内存。

### 5.2 为什么当前项目适合 SQLite

本项目当前更适合继续用 SQLite，而不是立即切换到外部数据库。

原因：

1. per-run 本身就天然分片。
2. 本地开发和 benchmark 更方便。
3. 审计与归档容易复制和封存。
4. 可以直接利用 JSON 列和 FTS。
5. 后续若要迁移 PostgreSQL，可以基于相同逻辑抽象上移。

### 5.3 核心表设计

下面是建议的主表分组。

### A. Runtime / Governance

1. `run_metadata`
   保存 run、mission、window、region、mode 等基础信息。
2. `runtime_events`
   保存 runtime event、ledger mirror、gate 结果。
3. `skill_invocations`
   保存每次 skill 调用的输入摘要、输出摘要、时长、状态。
4. `agent_sessions`
   保存 agent 身份、role profile、权限边界、会话状态。
5. `agent_turns`
   保存 agent 的 turn 级操作记录、使用的工具、引用的结果集。

### B. Evidence

1. `fetch_requests`
   保存 fetch 请求、来源、审批、追加方式、父请求。
2. `fetch_executions`
   保存 detached-fetch / import 的执行记录。
3. `raw_artifacts`
   保存 artifact 元数据、sha256、存储路径、source skill、capture mode。
4. `normalized_signals`
   继续沿用现有 signal plane 的主体结构。
5. `signal_text_fts`
   为 public / textual signals 提供全文检索。

### C. Analysis Result Sets

1. `analysis_result_sets`
   保存每次分析工具运行生成的一个结果集。
2. `analysis_result_rows`
   保存结果集中的逐行结果。
3. `analysis_lineage`
   保存结果集之间、结果行之间的血缘关系。

`analysis_result_sets` 应至少包含：

1. `result_set_id`
2. `run_id`
3. `round_id`
4. `tool_name`
5. `capability_name`
6. `status`
7. `summary_json`
8. `input_params_json`
9. `source_tables_json`
10. `parent_result_set_ids_json`
11. `created_by_agent_id`
12. `created_at_utc`

`analysis_result_rows` 应至少包含：

1. `result_set_id`
2. `row_id`
3. `rank`
4. `score`
5. `entity_kind`
6. `entity_id`
7. `payload_json`
8. `evidence_refs_json`
9. `parent_row_refs_json`
10. `flags_json`

这层是整个新架构的关键。

它的目标不是定义“唯一官方候选对象”，而是给 agent 一个可以不断创建、比较、筛选、合并的分析工作面。

### D. Deliberation / Board

1. `board_entities`
   保存 note、hypothesis、challenge、task、probe 等对象。
2. `board_relations`
   保存 entity 之间的关系。
3. `board_events`
   保存 board 写入历史。
4. round opening / round carryover
   应记录为显式 council-state mutation，而不是隐藏流程状态。

建议不要再让 `investigation_board.json` 成为唯一真相源。

未来应当改成：

1. 数据库表是主状态
2. `investigation_board.json` 是可再生导出物

### E. Publication

1. `promotion_freezes`
   保存 promotion gate 之后冻结的 evidence basis。
2. `report_exports`
   保存 expert report / decision / final publication 的导出记录。

## 6. Skill 体系重构

### 6.1 Skill 应分成四类

### 第一类：Ingress / Governance Skills

这些 skill 继续强治理：

1. fetch
2. import
3. normalize
4. archive
5. publish
6. gate

它们负责：

1. 边界控制
2. 规范化落库
3. 风险阻断
4. 对外冻结

### 第二类：Query / Lookup Skills

这些 skill 是 agent 默认主入口：

1. 查询某类 signals
2. lookup 某条 normalized signal
3. lookup 原始 record
4. query case library
5. query signal corpus
6. query board entities
7. query agent turns / prior result sets

这类 skill 应当尽量：

1. 轻量
2. 快速
3. 可组合
4. 可分页

### 第三类：Analysis Tool Skills

这些是未来最重要的 skill 类别。

它们不应再输出“下一步必须消费的官方文件”，而应输出 `result_set_id`。

适合进入这一层的能力包括：

1. 数据分布统计
2. 时间分桶
3. 地理聚合
4. source overlap 分析
5. narrative clustering
6. outlier detection
7. contradiction scan
8. support / contradiction score
9. sample-and-review
10. candidate ranking
11. history similarity retrieval

### 第四类：State-Change / Council-State Skills

这类 skill 必须显式、可审计：

1. post board note
2. open / close challenge
3. update hypothesis
4. claim task
5. open next investigation round
6. freeze promotion basis
7. publish report / decision

这类 skill 不应被隐式触发。

特别是：

1. `open-next-round` 不应被写成一个模糊的 “continue-investigation” 状态。
2. 它应当成为 moderator 可调用、会留下 board event 和 transition artifact 的显式动作。
3. 未来即使 board 主状态迁到 DB，这个动作也应保留为一个明确的 council-state skill。

### 6.2 统一的分析工具契约

未来所有 analysis tool skill 都应遵守统一约定。

输入至少包括：

1. `run_dir`
2. `run_id`
3. `round_id`
4. `source_result_set_id` 或显式过滤条件
5. `persist=true/false`
6. `limit / sampling / ranking` 参数

输出至少包括：

1. `result_set_id`
2. `row_count`
3. `sample_rows`
4. `summary`
5. `evidence_refs`
6. `suggested_next_capabilities`

核心原则：

1. 输出是“分析结果集”，不是“官方结论”。
2. 必须保留可回溯 evidence refs。
3. 必须保留输入参数摘要。
4. 必须支持重复调用和多版本并存。

### 6.3 强规则启发式 skill 如何降级为工具

以下 skill 不应再作为主链必经步骤，而应改造成 agent 可调用的分析工具。

1. `eco-extract-claim-candidates`
2. `eco-cluster-claim-candidates`
3. `eco-extract-observation-candidates`
4. `eco-merge-observation-candidates`
5. `eco-link-claims-to-observations`
6. `eco-derive-claim-scope`
7. `eco-derive-observation-scope`
8. `eco-score-evidence-coverage`
9. `eco-propose-next-actions`
10. `eco-summarize-round-readiness`
11. `eco-materialize-history-context`
12. `eco-materialize-board-brief`

改造目标不是删除这些能力，而是改变它们的定位：

1. 从“唯一通路”变成“可选透镜”
2. 从“强制压缩器”变成“辅助分析器”
3. 从“下一步控制器”变成“建议生成器”

### 6.4 应新增的 DB-first 工具型 skill

建议优先补下面这些能力。

### A. 数据面工具

1. `eco-profile-signal-distribution`
   统计数量、时间分布、来源分布、地理分布、语言分布。
2. `eco-sample-signals`
   从某个过滤结果中抽样，便于 agent 低成本人工复核。
3. `eco-find-signal-outliers`
   查极端值、罕见源、异常时间点。
4. `eco-compare-source-overlap`
   看多个 source family 是否相互支持还是高度重复。

### B. 语义面工具

1. `eco-group-public-narratives`
2. `eco-group-environment-patterns`
3. `eco-rank-cross-plane-support`
4. `eco-detect-contradiction-clusters`
5. `eco-build-evidence-pack`

### C. 检索面工具

1. `eco-query-run-sql-readonly`
   只读 SQL 查询，服务高级 agent。
2. `eco-query-board-entities`
3. `eco-query-analysis-result-sets`
4. `eco-query-analysis-rows`

注意：

1. 可以提供只读 SQL 工具。
2. 不应提供任意写 SQL 工具。
3. 写操作仍应通过受控 state-change skills 完成。

## 7. Agent 模型与协作

### 7.1 角色应从“硬编码分类”变成“默认 role profiles”

当前 `sociologist / environmentalist / moderator / challenger` 可以保留，但应降级为默认角色画像，而不是系统死规则。

建议：

1. 角色画像决定默认工具偏好。
2. 角色画像决定默认关注面。
3. 角色画像决定默认输出风格。
4. 角色画像不决定唯一可调用工具。

也就是说：

1. `sociologist` 默认更关注 public signals 与 narrative tools。
2. `environmentalist` 默认更关注 environment signals 与 physical pattern tools。
3. `challenger` 默认更关注 contradiction、sampling、falsification。
4. `moderator` 默认更关注 board 汇总、冲突裁决、freeze / publish。

但任何 agent 仍可调用跨域工具。

### 7.2 应建立明确的 agent 边界

目标不是让 agent 随意改库，而是让 agent 拥有清晰而可审计的边界。

每个 agent 至少应有：

1. `agent_id`
2. `role_profile`
3. `session_state`
4. `tool_invocation_history`
5. `owned_board_entities`
6. `visibility_scope`
7. `write_permissions`

### 7.3 协作方式

多 agent 协作不应依赖 prompt 里口头约定，而应依赖共享状态面。

建议的协作方式：

1. 所有 agent 共享同一个 run DB。
2. 所有 agent 可读 evidence plane 与 analysis plane。
3. board 写入必须带 `actor_agent_id`。
4. challenge / rebuttal / support 都应成为显式关系，而不是文本埋点。
5. moderator 负责收敛，而不是独占全部调查。

### 7.4 理想的 OpenClaw agent mode 工作流

一轮理想工作流应当更像这样：

1. `init-run` 建立 `run_state.sqlite`、artifact 目录、runtime 治理对象。
2. `fetch / import / normalize` 将 raw 与 normalized 数据落到 evidence plane。
3. `sociologist` 先调用查询和 narrative 工具，生成若干 public result sets。
4. `environmentalist` 调用环境分布和模式工具，生成若干 environment result sets。
5. `challenger` 对已有 result sets 做抽样复核、矛盾扫描、source overlap 检查。
6. `moderator` 不直接接管全部推理，只消费上述结果集并决定是否写 board、是否追加取数、是否准备 freeze。
7. 若证据仍不足，moderator 可选择继续在本轮调查，或显式调用 `eco-open-investigation-round` 开启下一轮。
8. 新轮 agent 查询时应允许 `round_scope=up-to-current`，这样 `round-003` 可以直接回看 `round-001` 与 `round-002` 的证据。
9. 只有当要进入 reporting / publication 时，runtime 才启动严格 gate 和导出流程。

这里没有强制单一顺序。

唯一严格的，是写边界和发布边界。

## 8. 当前代码如何取舍

### 8.1 应保留的部分

这些代码大部分可以继续复用。

1. `kernel/operations.py`
2. `kernel/executor.py`
3. `kernel/ledger.py`
4. `kernel/post_round.py`
5. `kernel/benchmark.py`
6. detached-fetch admission / dead-letter 相关逻辑
7. 当前各 source 的 normalizer skills
8. 当前 query / lookup 类 skills
9. archive / publish 类 skills

### 8.2 应重构的部分

这些能力保留，但要改定位与输出面。

1. `source_queue_*`
   从默认主路降级为 strict runtime mode / benchmark mode 的专用能力。
2. `eco-plan-round-orchestration`
   从 queue owner 改成 advisory planner。
3. `controller.py`
   从 exact stage executor 改成 capability / gate orchestrator，或仅保留给 strict mode。
4. `supervisor.py`
   保留为治理与冻结判断，不再主导调查。
5. candidate / cluster / merge / link / scope / coverage 相关 skills
   改写为 `result_set` 型分析工具。
6. board brief / history context / readiness / next-actions
   改成从 DB 生成的 advisory exports。

### 8.3 应放弃作为“主逻辑”的部分

下面这些思路不应再继续扩大：

1. exact stage 绑定 exact skill
2. 让 planner queue 统领开放式调查
3. 让 JSON 工件承担主分析内存
4. 让早期启发式压缩结果成为唯一事实骨架

## 9. JSON / Markdown 工件的新定位

未来仍然可以保留很多 JSON / Markdown 文件，但它们的地位必须变化。

应保留为：

1. export
2. operator snapshot
3. reporting handoff
4. benchmark fixture
5. archive 封存物

不应继续作为：

1. 主分析存储
2. 唯一中间状态
3. 强制下游输入格式

建议原则：

1. 任何关键 JSON / Markdown 都应能从 DB 再生。
2. 如果某个工件不能再生，它就不是导出物，而是隐藏状态源。
3. 隐藏状态源应尽量搬回数据库。

## 10. 实施路线

### 阶段 A：建立统一 run 主库

目标：

1. 从当前 `signal_plane.sqlite` 过渡到 `run_state.sqlite`
2. 先不改变业务行为
3. 先把 state 边界收拢

交付：

1. 主库 schema
2. evidence tables
3. runtime / invocation tables
4. 与现有 signal plane 的兼容视图或迁移层

完成判据：

1. fetch / normalize 后的数据都能进入主库
2. query / lookup 技能默认读主库
3. 不影响现有回归

### 阶段 B：引入 result-set 平面

目标：

1. 建立 analysis result set 通用层
2. 让工具型 skill 不再强依赖文件产物

交付：

1. `analysis_result_sets`
2. `analysis_result_rows`
3. `analysis_lineage`
4. 第一批 DB-first 分析工具

完成判据：

1. agent 可以创建多个并存结果集
2. agent 可以基于结果集继续筛选和分析
3. 不需要先生成官方候选 JSON 才能继续工作

### 阶段 C：board 主状态入库

目标：

1. 让 board 从单文件状态迁移到可查询表结构
2. 保留 board JSON 作为导出物

交付：

1. `board_entities`
2. `board_relations`
3. `board_events`
4. 从 DB 导出 `investigation_board.json`

完成判据：

1. 多 agent 能共享 board 状态
2. board 写入可追踪到具体 agent 与具体 evidence refs
3. board 快照不再是唯一真相源

### 阶段 D：将强规则 skill 工具化

目标：

1. candidate / cluster / link / coverage 不再是强制流水线
2. readiness / next-actions / history context 不再是隐性控制器

交付：

1. 旧 skill 的 DB-first 重写版或适配版
2. `result_set` 输出契约
3. 旧 JSON 导出兼容层

完成判据：

1. agent 可以跳过任意压缩工具
2. agent 可以直接从 query / lookup 层工作
3. 强规则 skill 只作为分析透镜存在

### 阶段 E：建立 OpenClaw agent mode

目标：

1. 把主调查权真正交给 agent
2. 让 runtime 回到治理边界

交付：

1. `agent_sessions` / `agent_turns`
2. appendable fetch request 机制
3. capability-based tool dispatch
4. 角色画像与协作协议

完成判据：

1. agent 能自主决定工具组合
2. agent 能在 run 中追加取数动作
3. 多 agent 能共享同一 run DB 分工调查
4. 仍然不破坏审计与治理

### 阶段 F：发布与归档收口

目标：

1. 导出与发布完全从 DB 和 board 再生
2. runtime 继续承担 archive / replay / benchmark

交付：

1. DB-backed reporting handoff
2. DB-backed expert reports
3. DB-backed final publication
4. archive / replay 适配

完成判据：

1. 发布物不再依赖脆弱的中间文件链
2. benchmark / replay 仍可稳定复现

## 11. 完成定义

只有当下面条件同时满足，才算真正完成这条蓝图。

1. agent 默认直接面向 DB 工作，而不是默认面向压缩文件工作。
2. 分析型 skill 默认输出 `result_set`，而不是唯一强制结论文件。
3. board 状态已入库，且写边界清晰可审计。
4. 多 agent 可以共享 run DB 协作，而不是只共享文本上下文。
5. runtime 只在治理、freeze、publish、archive 边界保持硬控制。
6. strict runtime mode 仍然存在，用于 benchmark / replay / nightly。

## 12. 最终项目形态

最终理想项目应当呈现为：

1. 一个稳定的治理执行内核
2. 一个 run 级持久化数据库工作面
3. 一组原子化的 query / analysis / state-change / export skills
4. 一个真正能发挥 OpenClaw 多 agent 自主调查能力的 agent mode

它不应再是：

1. controller-heavy 的第二套业务系统
2. 用规则硬编排 OpenClaw 的表单流水线
3. 用文件链伪装成智能体协作

它应当是：

`OpenClaw 负责调查，runtime 负责边界，数据库负责记忆，skill 负责操作。`
