# OpenClaw 数据层级约定与 DB-First 设计技术报告

## 1. 报告定位

本报告说明项目当前的数据分层、命名约定、数据库平面设计，以及“文件工件”和“数据库工作面”之间的关系。

## 2. 数据层级的总体原则

当前项目已经形成较清晰的数据主干：

`raw -> normalized -> analytics -> board -> reporting -> archive/history`

但更重要的是，这条链不再被理解为“必须严格按文件串起来的流水线”，而是：

1. 文件用于保留原始证据与导出物。
2. SQLite 用于提供统一查询、恢复和追踪工作面。
3. JSON / Markdown 逐步从主状态载体降级为 snapshot / export / handoff。

## 3. 目录与层级约定

### 3.1 runtime 层

`runtime/` 主要承载治理与控制状态，例如：

1. `run_manifest.json`
2. `round_cursor.json`
3. `skill_registry.json`
4. `admission_policy.json`
5. `orchestration_plan_<round>.json`
6. `round_controller_<round>.json`
7. `promotion_gate_<round>.json`
8. `supervisor_state_<round>.json`

这层不是证据层，而是运行时治理层。

### 3.2 raw 层

`raw/public` 和 `raw/environment` 代表原始抓取层。

这里保存的是外部来源的原始结果，如：

1. API JSON
2. CSV
3. ZIP 导出
4. manifest

保留 raw 层的意义是：任何上层分析都必须能回到原始证据。

### 3.3 normalized 层

归一化层并不要求单独存在一个大量 JSON 的目录，它的核心是 `normalized_signals` 表。

其关键字段包括：

1. `signal_id`
2. `run_id`
3. `round_id`
4. `plane`
5. `source_skill`
6. `signal_kind`
7. `title / body_text / url`
8. `metric / numeric_value / unit`
9. `published_at_utc / observed_at_utc`
10. `latitude / longitude`
11. `metadata_json / raw_json`
12. `artifact_path / record_locator / artifact_sha256`

这说明 normalized 层同时承担三件事：

1. 统一检索接口。
2. 统一信号语义。
3. 到原始工件的可逆追踪。

### 3.4 analytics 层

analytics 层的核心不是某一个 JSON 文件，而是 `Analysis Plane`。

当前主要表包括：

1. `analysis_result_sets`
2. `analysis_result_items`
3. `analysis_result_lineage`

其中：

1. `result_sets` 表示一次分析结果集合。
2. `result_items` 表示结果集合中的具体条目。
3. `lineage` 表示 query basis、parent ids、artifact refs、parent result sets 等可追溯关系。

当前已纳入 analysis plane 的对象包括：

1. `claim-candidate`
2. `observation-candidate`
3. `claim-observation-link`
4. `claim-scope`
5. `observation-scope`
6. `evidence-coverage`

### 3.5 board 层

board 层的核心是 `Deliberation Plane`。

当前主要表包括：

1. `board_runs`
2. `board_events`
3. `board_notes`
4. `hypothesis_cards`
5. `challenge_tickets`
6. `board_tasks`
7. `round_transitions`
8. `promotion_freezes`
9. `moderator_action_snapshots`
10. `falsification_probe_snapshots`
11. `round_task_snapshots`

这意味着：

1. board 已经不仅是 `investigation_board.json`。
2. round 的推进、挑战、任务、notes、freeze 都有结构化状态面。
3. moderator 的核心控制对象已有 DB-backed 恢复面。

### 3.6 reporting 层

reporting 层当前仍然以导出物为主，包括：

1. board brief
2. reporting handoff
3. decision draft
4. expert report draft
5. canonical decision
6. canonical expert report
7. final publication

但 reporting 链已经完成 trace contract 规范化，因此它虽然是导出层，却不再是“不可回溯摘要层”。

### 3.7 archive/history 层

archive/history 层负责跨 run 复用，主要承载：

1. case library
2. signal corpus
3. history context

这一层的意义在于把一次 run 的调查结果转化成后续 run 的先验资源，而不只是生成一次性报告。

## 4. DB-first 的核心约定

### 4.1 文件不删除，但降级

项目并没有把文件工件完全删除，而是明确重新定位：

1. 文件保原始证据。
2. 文件保兼容导出。
3. 文件保人类可读 handoff。
4. 数据库保主查询面与状态面。

这是一种很重要的工程折中：

1. 保留透明性。
2. 保留兼容性。
3. 同时摆脱线性 JSON 依赖。

### 4.2 source of truth 的迁移原则

当前迁移遵循的不是一次性重写，而是：

1. 先保持 JSON 导出兼容。
2. 再把读路径迁到 DB-first。
3. 最后再把写路径迁到 DB-first。

这也是为什么很多里程碑会强调：

1. `analysis-plane-first with JSON fallback`
2. `deliberation-plane-first with JSON export`

### 4.3 过渡性 SQLite 复用

当前 `normalized`、`analysis`、`deliberation` 三个工作面，默认都复用同一个 SQLite 文件：

`analytics/signal_plane.sqlite`

这不是最终理想形态，但它有明确工程价值：

1. 降低迁移成本，不需要一次性拆出多套独立 DB。
2. 让 analysis plane 和 deliberation plane 可以快速接入现有 run 目录。
3. 方便在保持本地可运行性的前提下，逐步把 JSON-first 读写面迁到 DB-first。

因此，这一设计更适合被表述为：

`过渡性统一数据底盘，而不是最终的物理拆库方案。`

### 4.4 trace contract 必须统一

为了防止跨层漂移，当前项目已经重点统一如下字段：

1. `analysis_sync`
2. `deliberation_sync`
3. `board_state_source`
4. `coverage_source`
5. `observed_inputs`
6. 各输入的 `*_artifact_present`
7. 各输入的 `*_present`

这套约定的意义在于：

1. 能区分“文件在不在”和“内容是否已被恢复/物化”。
2. 能区分当前对象来自 artifact 还是 DB snapshot。
3. 能让 reporting/export 继续保留跨平面的来源链。

### 4.5 lineage 必须显式化

analysis plane 当前特别强化了 lineage 语义，包括：

1. `query_basis`
2. parent artifact refs
3. parent result-set resolution
4. item-level parent ids
5. item-level artifact refs

这意味着项目正在从“结果文件仓库”转向“可解释分析图谱”。

## 5. 当前最值得强调的数据层创新

### 5.1 `normalized_signals`

它统一承接了 public 与 environment 两类信号，是证据底座。

### 5.2 `analysis_result_*`

它把 candidates、scope、link、coverage 这类分析对象从临时 JSON 变成可查询结果集。

### 5.3 `deliberation_plane`

它把 board、task、challenge、round transition、freeze 等 moderator 工作面结构化。

### 5.4 latest-snapshot recovery surfaces

`moderator_action_snapshots`、`falsification_probe_snapshots`、`round_task_snapshots` 等表，解决了“JSON 一删，运行就断”的问题。

## 6. 当前约定的边界与不足

需要在汇报里明确指出，这些设计仍处于“过渡到 DB-first”的中后段，而非最终形态。

当前主要不足包括：

1. analysis plane 仍是 runtime-local Python/SQLite 表面，尚未形成正式的非 Python 查询接口。
2. candidate/cluster/merge 家族还没有全部进入统一 result-set contract。
3. `promotion_freezes` 与各类 snapshot 表当前更偏 latest-snapshot，而不是完整历史。
4. 仍有部分 round lineage discovery 依赖 runtime/artifact footprint，而不是完全 DB-native。

## 7. 对课题组的表达建议

可以将数据层设计总结为三句话：

1. 这个项目的数据结构不再是单一流水线，而是 evidence、analysis、deliberation、export 四个平面协同。
2. 文件没有被废弃，但已逐步从主状态面降级为原始证据和兼容导出。
3. 真正的新能力来自数据库工作面，因为只有 DB-first，系统才具备可查询、可恢复、可追踪和可审计的运行能力。
