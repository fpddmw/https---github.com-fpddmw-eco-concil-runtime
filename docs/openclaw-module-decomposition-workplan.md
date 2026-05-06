# OpenClaw 大模块彻底拆分工作计划

## 1. 文档定位

本文是大模块拆分的持久化执行计划。它不是全局总计划，也不包含真实案例 case 工作。

目标是在不改变外部行为、skill id、CLI 命令、canonical contract、DB schema 和 artifact shape 的前提下，把当前过大的 Python 模块拆成可维护、可测试、可逐步迁移的模块族。

本计划以 2026-05-06 的仓库状态为基线。当前已验证：syntax 覆盖 219 个 Python 文件，P3 targeted gate 组合通过 34 tests，runtime/reporting 组合通过 95 tests，默认 targeted gates 通过 125 tests，full gate 通过 268 tests。schema migration 硬化已完成第一块代码，模块拆分 P0 保护网已落地，P1/P2 已完成 deliberation plane 的 facade 化拆分，P3 已完成 analysis plane 的 facade 化拆分，并已完成一轮拆分文件数量收敛。

### 当前落地状态

已完成 P0 拆分准备与保护网，P1/P2 已交付 deliberation plane 全量拆分，P3 已交付 analysis plane 全量拆分，并在 2026-05-06 进行了一轮文件数量收敛：

1. 新增 `tools/module_size_report.py`。
   - 输出 `module-size-report-v1`。
   - 固定当前 runtime/module 拆分候选清单。
   - skill scripts 不再因行数进入拆分压力清单；skill 是否拆分只由原子能力边界决定。
   - 支持 JSON 与 Markdown 报告。
   - 目前仅作为报告工具，不阻断 CI。
2. 新增 `tests/test_module_decomposition_contracts.py`。
   - 固定 facade public import compatibility。
   - 固定关键 CLI command `--help` smoke。
   - 固定 module size report 的目标清单和基础行为。
3. `tools/quality_gate.py` 新增 `module-decomposition` targeted suite。
   - 已纳入默认 targeted gates。
   - 已与 `schema-migration`、`runtime-governance` 组合验证通过。
4. `.github/workflows/quality-gates.yml` 已把 `module-decomposition` 纳入 targeted CI 命令。
5. 新增 `eco_concil_runtime/kernel/deliberation_plane_schema.py`。
   - 承载 `SCHEMA_SQL`、DB path resolution、`connect_db`、schema migration 与 schema status。
   - 不改变 SQLite 表、列、index 或 migration id。
6. 新增 `eco_concil_runtime/kernel/deliberation_plane_rows.py`。
   - 承载通用 JSON/row helper、`payload_from_db_row`、基础 `write_*_row`、board/transition row conversion 与 DB-backed record query helpers。
7. `eco_concil_runtime/kernel/deliberation_plane.py` 保持兼容入口。
   - 继续 re-export 原 public names。
   - 当前约 285 行，已从 P0 基线约 8158 行下降。
8. 新增 `eco_concil_runtime/kernel/deliberation_board_state.py`。
   - 承载 board path、JSON export、board bootstrap、board sync、round snapshot、board mutation 与 round transition store/load。
9. 新增 `eco_concil_runtime/kernel/deliberation_actions.py`。
   - 承载 moderator actions、falsification probes、round readiness 的 normalization、store/load 与 snapshot wrapper。
10. 新增 `eco_concil_runtime/kernel/deliberation_reporting_records.py`。
   - 承载 report basis freeze、reporting handoff、council decision、expert report、final publication，以及 reporting record 共用 canonical default helper。
11. 新增 `eco_concil_runtime/kernel/deliberation_runtime_control.py`。
   - 承载 runtime control freeze、controller/gate/supervisor snapshots、governed_execution control state、moderator work surface、orchestration plan/step 与 round task snapshot。
12. 新增 `eco_concil_runtime/kernel/analysis_plane_schema.py`。
   - 承载 analysis DB schema、path resolution、connect 与 legacy column ensure。
13. 新增 `eco_concil_runtime/kernel/analysis_plane_contracts.py`。
   - 承载 analysis plane 共用 JSON/text/hash/time helper、analysis kind constants、kind config、governance metadata 与 kind registry。
14. 新增 `eco_concil_runtime/kernel/analysis_plane_support.py`。
   - 承载 artifact path/ref normalization、dedupe、artifact presence helper、result contract、lineage、parent result set 与 parent artifact refs 的 build/load helper。
15. 新增 `eco_concil_runtime/kernel/analysis_plane_results.py`。
   - 承载 generic sync/load result set persistence 与 context loading。
16. 新增 `eco_concil_runtime/kernel/analysis_plane_queries.py`。
   - 承载 result set/item 查询、分页、serialization 与 spatiotemporal relation cue 专用查询。
17. 新增 `eco_concil_runtime/kernel/analysis_plane_contexts.py`。
   - 承载 typed `sync_*_result_set` 与 `load_*_context` compatibility wrappers。
18. `eco_concil_runtime/kernel/analysis_plane.py` 保持兼容入口。
   - 继续 re-export 原 public names。
   - 当前约 242 行，已从 P0 基线约 3697 行下降。

当前已验证命令：

1. `python3 tools/quality_gate.py syntax`，219 个 Python 文件通过，无重复字面量 dict key。
2. `python3 -m unittest tests.test_module_decomposition_contracts`，7 tests 通过。
3. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_optional_analysis_guardrails tests.test_spatiotemporal_relation_taxonomy`，33 tests 通过。
4. `python3 -m unittest tests.test_runtime_kernel`，47 tests 通过。
5. `python3 tools/quality_gate.py test relation-taxonomy optional-guardrails module-decomposition`，34 tests 通过。
6. `python3 tools/quality_gate.py test module-decomposition schema-migration db-recovery runtime-governance reporting`，95 tests 通过。
7. `python3 tools/quality_gate.py test relation-taxonomy optional-guardrails db-recovery schema-migration module-decomposition runtime-governance reporting case-study`，125 tests 通过。
8. `python3 tools/quality_gate.py full`，268 tests 通过。
9. `git diff --check` 通过。

## 2. 拆分原则

1. 保留兼容入口。
   - 现有 public import path 不立即删除。
   - `kernel/deliberation_plane.py`、`kernel/analysis_plane.py`、`optional_analysis_helpers.py`、`kernel/cli.py` 等先变成 facade/re-export 层。
2. 不混入语义重写。
   - 不改 canonical object 字段。
   - 不改 receipt、ledger、approval、transition、reporting、relation 等数据契约。
   - 不改 SQLite 表、列、index，除非另走 schema migration 计划。
3. 命名遵守现有风格。
   - runtime/kernel 内继续使用 snake_case 文件名。
   - 已有概念继续使用现有词根：`*_objects.py`、`*_semantics.py`、`*_status.py`、`*_exports.py`、`*_support.py`、`*_requests.py`、`*_approvals.py`、`*_state_surfaces.py`。
   - skill 目录继续使用 kebab-case；script 文件继续使用 snake_case。
4. 限制文件数量。
   - 拆分目标不是制造更多文件，而是形成少数稳定的中等模块。
   - 单个原大模块默认最多拆成 4 到 6 个主模块；超过该数量必须先说明为什么不能合并。
   - 小 helper 不单独成文件，除非它有独立 public contract、独立测试边界或明确跨模块复用价值。
5. 以数据面和契约边界拆分。
   - schema / row conversion / payload normalization / store-load / query / operator view 分开。
   - 分层边界优先于函数长度边界。
   - 行数是观察信号，不是拆分依据；行数偏大但职责原子、入口清晰时可以保留。
6. 每一步都必须可回滚、可测试。
   - 每个阶段后运行 targeted gates。
   - 每个大阶段后运行 full gate。
   - 不在一个提交里同时拆多个大面。
7. skill 只按原子能力边界拆分。
   - skill 是否拆分只看它是否包含多个可独立执行、独立契约、独立 artifact 语义的能力。
   - 单个 skill 脚本很长但仍是一个原子采集、转换或发布动作时，不因为行数拆分。
   - skill 内部可以整理 helper，但不得把一个原子 skill 切成多个 skill id。

## 3. 当前必须拆分的文件清单

### 3.1 核心 runtime/kernel 必拆

这些文件体积大、调用面广、承载多个数据契约，是大模块拆分的 P0/P1 范围。

1. `eco-concil-runtime/src/eco_council_runtime/kernel/deliberation_plane.py`
   - P0 基线约 8158 行，P2 交付后当前约 285 行。
   - 原先同时承载 schema、migration、row conversion、board sync、governed_execution control、moderator actions、falsification probes、reporting records、round transitions、round snapshots。
   - 已拆为 facade/re-export 入口，后续只保留 compatibility glue。
2. `eco-concil-runtime/src/eco_council_runtime/kernel/cli.py`
   - 当前约 3864 行。
   - 同时承载 parser、command dispatch、run state view、operator views、analysis/council/reporting/control query commands。
   - 必须彻底拆分，但外部命令名必须保持不变。
3. `eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py`
   - P0 基线约 3697 行，P3 交付后当前约 248 行。
   - 原先同时承载 analysis schema、result set persistence、artifact refs、query paging、relation query、typed sync/load wrappers。
   - 已拆为 facade/re-export 入口，后续只保留 compatibility glue。
4. `eco-concil-runtime/src/eco_council_runtime/optional_analysis_helpers.py`
   - 当前约 2671 行。
   - 同时承载 helper common、signal query、evidence aggregation、scope review、research issue、formal/public taxonomy、relation cues、alternative review。
   - 必须拆分。
5. `eco-concil-runtime/src/eco_council_runtime/council_objects.py`
   - 当前约 2279 行。
   - 同时承载 council schema、payload normalization、append/store/query、bundle helpers、decision trace。
   - 必须拆分。
6. `eco-concil-runtime/src/eco_council_runtime/analysis_objects.py`
   - 当前约 2005 行。
   - 同时承载多类 analysis canonical object normalization。
   - 必须拆分或至少按 object family 分组。
7. `eco-concil-runtime/src/eco_council_runtime/canonical_contracts.py`
   - 当前约 1524 行。
   - 作为 central contract registry 保留，但各 plane 的 contract definitions 应拆入独立文件。
   - 必须谨慎拆分，保留 `canonical_contracts.py` facade。

### 3.2 runtime 治理与控制面应拆

这些文件未必都超过 2000 行，但已经承担多个职责。它们应在核心 plane 拆分后继续拆。

1. `eco-concil-runtime/src/eco_council_runtime/kernel/benchmark.py`
   - 当前约 1511 行。
   - 拆分 scenario fixture、benchmark manifest、compare、replay。
2. `eco-concil-runtime/src/eco_council_runtime/kernel/skill_approvals.py`
   - 当前约 1395 行。
   - 拆分 payload、row conversion、request/approve/reject/consume store-load。
3. `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
   - 当前约 1316 行。
   - 拆分 controller state transitions、stage execution、controller event materialization。
4. `eco-concil-runtime/src/eco_council_runtime/kernel/transition_requests.py`
   - 当前约 1187 行。
   - 拆分 transition payloads、approval/rejection payloads、store-load、execution resolver。
5. `eco-concil-runtime/src/eco_council_runtime/kernel/runtime_state_surfaces.py`
   - 当前约 1040 行。
   - 拆分 controller/gate/supervisor/reporting/fallback wrapper readers。
6. `eco-concil-runtime/src/eco_council_runtime/kernel/signal_plane_normalizer.py`
   - 当前约 1032 行。
   - 拆分 schema/indexing、signal payload normalization、metadata indexing、row persistence。
7. `eco-concil-runtime/src/eco_council_runtime/kernel/operations.py`
   - 当前约 1015 行。
   - 拆分 admission policy、dead letters、runtime health、operator runbook。
8. `eco-concil-runtime/src/eco_council_runtime/kernel/executor.py`
   - 当前约 960 行。
   - 拆分 command building、attempt execution、structured failure, receipt/postflight handling。
9. `eco-concil-runtime/src/eco_council_runtime/kernel/post_round.py`
   - 当前约 926 行。
   - 拆分 close round、archive handling、history bootstrap。

### 3.3 profile/config 大文件应整理

这些文件多为规则/配置集合，拆分方式应避免改变 runtime 语义。

1. `eco-concil-runtime/src/eco_council_runtime/agent_entry_profile.py`
   - 当前约 1222 行。
   - 拆为 gate profile、role expectations、entry chain profile。
2. `eco-concil-runtime/src/eco_council_runtime/fallback_agenda_profile.py`
   - 当前约 755 行。
   - 可在 governed_execution profile 家族中整理。
3. `eco-concil-runtime/src/eco_council_runtime/formal_signal_semantics.py`
   - 当前约 762 行。
   - 可按 issue/stance/citation/actor facets 拆分，但优先级低于 runtime/kernel。

### 3.4 Runtime 结构与旧 phase 命名清理

当前 `eco_council_runtime/kernel/` 文件数量已经偏多。旧 `phase2` 命名已经不再作为兼容入口保留；当前代码、测试、CLI 和 skill 脚本统一以 governed execution 命名表达“受治理的运行时执行链”。

本轮命名清理后的最新命名基线：

1. `kernel/runtime_state_surfaces.py`
   - 原旧相位状态 surface 名称已替换为 runtime state surface。
2. `agent_entry_profile.py`、`agent_entry_handoff.py`
   - agent entry 不再挂在旧相位名下。
3. `runtime_gate_profile.py`、`runtime_gate_handlers.py`
   - gate profile / handler 归入 runtime gate 概念。
4. `runtime_planning_profile.py`、`runtime_posture_profile.py`、`runtime_stage_profile.py`、`runtime_round_profile.py`
   - planning、posture、stage、round profile 使用 runtime 语义。
5. `governed_execution_controller_state.py`、`governed_execution_exports.py`
   - 仅在确实表达“受治理执行链”整体产物时使用 governed execution。
6. CLI 命令使用 `run-governed-execution-round`、`resume-governed-execution-round`、`restart-governed-execution-round`、`materialize-governed-execution-exports`。
7. 旧 `phase2_*` import path、函数名、CLI 命令和测试 patch path 不再保留。

后续 runtime 重组不再继续在 `kernel/` 根目录横向增加文件，而应逐步收敛为少数子包：

1. `kernel/planes/`
   - 承载 signal、analysis、deliberation 等数据面 facade 和持久化模块。
   - 现有 import path 继续通过兼容 facade 暴露。
2. `kernel/governance/`
   - 承载 transition request、skill approval、access/admission policy、operator approval。
3. `kernel/execution/`
   - 承载 controller、executor、runtime attempts、receipt/postflight、failure handling。
4. `kernel/reporting/`
   - 承载 reporting handoff、report basis、publication/export 相关 runtime surface。
5. `kernel/operator/`
   - 承载 run state view、operator views、health、dead letters、runbook。
6. `kernel/archive/`
   - 承载 post-round close、history bootstrap、benchmark fixture/manifest/replay。

下一步目录重组时，优先把这些最新命名迁入更合适的包，而不是继续改名：

1. `kernel/runtime_state_surfaces.py` 后续迁入 `kernel/operator/state_surfaces.py` 或 `kernel/operator/runtime_state_view.py`。
2. `agent_entry_profile.py`、`agent_entry_handoff.py` 后续迁入 `kernel/governance/agent_entry_*` 或顶层 governance profile 区。
3. `fallback_agenda.py`、`fallback_agenda_profile.py` 后续迁入 `kernel/governance/fallback_agenda.py` 或 runtime governance profile 区。
4. controller、executor、gate、supervisor 后续迁入 `kernel/execution/`。

### 3.5 Skill scripts 原子性审查

这些脚本大多是 provider 或工作流专用。后续不再因为行数将它们列为“必须拆分”。skill 是否拆分只看它是否仍是原子能力。

原子 skill 判定标准：

1. 只有一个用户可理解的动词目标。
2. 只有一个主要输入契约和一个主要输出 artifact 家族。
3. 失败、重试、receipt、provenance 能作为一个动作解释。
4. skill id 与 `SKILL.md` 描述没有混合多个独立能力。

只有当一个 skill 同时包含多个独立能力时才考虑拆分 skill id。整理 skill 内部实现时必须保留：

1. skill id 不变。
2. `SKILL.md` contract 不变。
3. 原脚本文件仍作为 CLI wrapper 存在。
4. stdout JSON shape、receipt id、artifact refs、canonical ids 不变。

### 3.6 测试文件也需要配套拆分

生产模块拆分到一定阶段后，测试也要拆，否则单个测试文件会成为新的维护瓶颈。

1. `tests/test_runtime_kernel.py`，当前约 3282 行。
2. `tests/test_board_workflow.py`，当前约 1825 行。
3. `tests/test_spatiotemporal_relation_taxonomy.py`，当前约 1535 行。
4. `tests/test_archive_history_workflow.py`，当前约 1141 行。

测试拆分必须滞后于生产模块拆分一小步执行，避免同时失去行为锚点。

## 4. 拆分后的理想形态

### 4.1 Deliberation Plane

保留兼容入口：

1. `eco_concil_runtime/kernel/deliberation_plane.py`
   - 保留原 public API。
   - 只做 re-export / facade。
   - 暂不删除旧函数名。

目标模块：

1. `eco_concil_runtime/kernel/deliberation_plane_schema.py`
   - `SCHEMA_SQL`
   - `connect_db`
   - `default_db_path`
   - `resolve_db_path`
   - `ensure_schema_migrations`
   - `load_schema_status`
2. `eco_concil_runtime/kernel/deliberation_plane_rows.py`
   - `payload_from_db_row`
   - `*_row_from_payload`
   - `write_*_row`
   - shared JSON/row conversion helpers。
   - shared DB-backed query helpers。
3. `eco_concil_runtime/kernel/deliberation_board_state.py`
   - board run/event/note/hypothesis/challenge/task store-load。
   - `sync_board_to_deliberation_plane`
   - `bootstrap_board_state`
   - `commit_board_mutation`
   - `load_round_snapshot`
   - `fetch_round_events`
   - `fetch_round_state`
   - `store_round_transition_record`
   - `load_round_transition_record`
4. `eco_concil_runtime/kernel/deliberation_actions.py`
   - moderator action records。
   - falsification probe records。
   - readiness assessment records。
5. `eco_concil_runtime/kernel/deliberation_runtime_control.py`
   - runtime control freeze。
   - controller/gate/supervisor snapshots。
   - `load_governed_execution_control_state`。
   - orchestration plan/steps。
   - round task snapshot。
   - planner-backed governed_execution plan persistence。
6. `eco_concil_runtime/kernel/deliberation_reporting_records.py`
   - reporting roles。
   - shared reporting canonical default helpers。
   - nested evidence/text extraction。
   - report basis freeze records/items。
   - reporting handoff。
   - council decision record。
   - expert report record。
   - final publication record。

命名说明：

1. 使用 `deliberation_*` 前缀，延续 `deliberation_plane.py` 的概念边界。
2. `*_records.py` 用于 DB-backed object records，符合现有 reporting/council 命名。
3. `*_state.py` 用于 board / runtime state surface，不和 canonical object registry 混淆。
4. 不再为 small helper 单独保留文件；helper 应合并进同族中等模块。

### 4.2 Analysis Plane

保留兼容入口：

1. `eco_concil_runtime/kernel/analysis_plane.py`
   - 保留 `sync_*_result_set`、`load_*_context`、`query_*` 等现有 public names。

目标模块：

1. `eco_concil_runtime/kernel/analysis_plane_schema.py`
   - analysis DB schema 和 connect/ensure。
2. `eco_concil_runtime/kernel/analysis_plane_contracts.py`
   - common JSON/text/hash/time helper。
   - `analysis_config`
   - governance metadata。
   - analysis kind registry。
3. `eco_concil_runtime/kernel/analysis_plane_support.py`
   - artifact refs。
   - path resolution。
   - artifact presence checks。
   - result contract building/loading。
   - lineage entries。
   - parent result set/artifact refs。
4. `eco_concil_runtime/kernel/analysis_plane_results.py`
   - result set and item persistence。
   - result wrapper loading。
   - generic sync/load wrapper shared implementation。
5. `eco_concil_runtime/kernel/analysis_plane_queries.py`
   - result set query。
   - item query。
   - paging/serialization。
   - `query_spatiotemporal_relation_cues`
   - relation-specific filters and serialization。
6. `eco_concil_runtime/kernel/analysis_plane_contexts.py`
   - typed `sync_*_result_set` compatibility wrappers。
   - typed `load_*_context` compatibility wrappers。

命名说明：

1. 保留 `analysis_plane_*`，对应现有 plane 名称。
2. relation 查询保留在 `analysis_plane_queries.py`，避免为单个查询族增加额外小文件。
3. support 文件只收纳 artifact/result-contract 这类共享支持能力，不继续拆成更细 helper 文件。

### 4.3 Optional Analysis Helpers

保留兼容入口：

1. `eco_concil_runtime/optional_analysis_helpers.py`
   - 保留所有 `run_*` helper 名称。
   - 作为 facade/re-export。

目标模块：

1. `eco_concil_runtime/optional_analysis_common.py`
   - JSON/path/text/hash/common helper。
   - helper metadata。
   - board handoff helper。
2. `eco_concil_runtime/optional_analysis_signal_queries.py`
   - signal DB connect/query。
   - row-to-signal。
   - evidence refs。
3. `eco_concil_runtime/optional_analysis_environment_evidence.py`
   - `run_aggregate_environment_evidence`
   - source/metric distribution。
4. `eco_concil_runtime/optional_analysis_scope_review.py`
   - fact-check evidence scope。
   - structured verification scope。
5. `eco_concil_runtime/optional_analysis_research_issues.py`
   - discourse issue discovery。
   - evidence lanes。
   - research issue surface/views/map。
6. `eco_concil_runtime/optional_analysis_formal_public.py`
   - approved taxonomy labels。
   - formal/public footprints。
   - representation audit cues。
7. `eco_concil_runtime/optional_analysis_relations.py`
   - temporal cooccurrence。
   - structured spatiotemporal relation cues。
   - relation alternative review。

命名说明：

1. 使用 `optional_analysis_*` 前缀，保持与 layer 名称一致。
2. 不把 optional helper 变成 phase gate 或 report basis，这一点必须在拆分后继续由 tests 固定。

### 4.4 Runtime CLI

保留兼容入口：

1. `eco_concil_runtime/kernel/cli.py`
   - 保留 `build_parser` 和 `main`。
   - 可继续作为 console script 入口。

目标模块：

1. `eco_concil_runtime/kernel/cli_parser.py`
   - subcommand parser construction。
   - shared arg helpers。
2. `eco_concil_runtime/kernel/cli_runtime_commands.py`
   - `init-run`
   - `run-skill`
   - `preflight-skill`
   - admission/health/runbook/dead-letter/schema-status commands。
3. `eco_concil_runtime/kernel/cli_control_commands.py`
   - phase transition request/approve/reject。
   - skill approval request/approve/reject。
   - control object queries。
4. `eco_concil_runtime/kernel/cli_council_commands.py`
   - finding/discussion/review/evidence/report-section commands。
   - council object queries。
5. `eco_concil_runtime/kernel/cli_analysis_commands.py`
   - analysis result set/item queries。
   - relation query。
   - canonical contracts list。
6. `eco_concil_runtime/kernel/cli_reporting_commands.py`
   - reporting object query。
   - reporting exports。
   - show reporting state。
7. `eco_concil_runtime/kernel/cli_execution_commands.py`
   - gate/apply。
   - run/resume/restart governed execution。
   - supervisor/controller execution commands。
8. `eco_concil_runtime/kernel/cli_post_round_commands.py`
   - close round。
   - history bootstrap。
   - benchmark fixture/manifest/compare/replay。
9. `eco_concil_runtime/kernel/run_state_view.py`
   - `show_run_state`
   - operations state。
   - transition state。
   - benchmark/post-round/reporting/governed execution state composition。
10. `eco_concil_runtime/kernel/operator_views.py`
   - governed execution operator view。
   - `reporting_operator_view`
   - `post_round_operator_view`
   - `benchmark_operator_view`

命名说明：

1. `cli_*_commands.py` 保持现有 `kernel/cli.py` 的命令语义。
2. CLI 执行链命名以 governed execution 为准，不再保留旧 phase 命令。
3. 中长期可把 command handlers 迁入 `kernel/operator/`、`kernel/governance/`、`kernel/execution/` 子包，`kernel/cli.py` 仍作为入口 facade。

### 4.5 Council Objects

保留兼容入口：

1. `eco_concil_runtime/council_objects.py`
   - 继续导出现有 append/store/query API。

目标模块：

1. `eco_concil_runtime/council_objects_schema.py`
   - council object schema and DB connect。
2. `eco_concil_runtime/council_object_payloads.py`
   - finding/evidence/discussion/review/proposal/readiness/decision trace payload normalization。
3. `eco_concil_runtime/council_object_rows.py`
   - row conversion and row writes。
4. `eco_concil_runtime/council_object_store.py`
   - append/store operations。
5. `eco_concil_runtime/council_object_query.py`
   - query config and query surface。
6. `eco_concil_runtime/council_decision_traces.py`
   - decision trace bundles and trace-specific helpers。

命名说明：

1. 保留 `council_object_*` 单数前缀用于 helpers。
2. 保留 `council_objects.py` plural facade，因为现有 public module 就是 plural。

### 4.6 Analysis Objects

保留兼容入口：

1. `eco_concil_runtime/analysis_objects.py`
   - 继续导出所有 existing normalized payload builders。

目标模块：

1. `eco_concil_runtime/analysis_object_common.py`
   - common normalization helpers。
2. `eco_concil_runtime/analysis_signal_objects.py`
   - claim/observation candidates and scopes。
3. `eco_concil_runtime/analysis_issue_objects.py`
   - issue cluster、stance group、concern facet、actor profile。
4. `eco_concil_runtime/analysis_relation_objects.py`
   - formal-public link、diffusion edge、spatiotemporal relation cue、representation gap。
5. `eco_concil_runtime/analysis_verification_objects.py`
   - verifiability assessment、verification route、evidence citation type。

### 4.7 Canonical Contracts

保留兼容入口：

1. `eco_concil_runtime/canonical_contracts.py`
   - 保留 `canonical_contract`、`canonical_contracts_for_plane`、`validate_canonical_payload`。
   - 保留 constants re-export。

目标模块：

1. `eco_concil_runtime/canonical_contract_types.py`
   - `CanonicalContract` dataclass。
   - registration helpers。
2. `eco_concil_runtime/canonical_signal_contracts.py`
   - signal contracts and signal metadata constants。
3. `eco_concil_runtime/canonical_analysis_contracts.py`
   - analysis object contracts。
4. `eco_concil_runtime/canonical_deliberation_contracts.py`
   - council/board/round/probe/readiness contracts。
5. `eco_concil_runtime/canonical_runtime_contracts.py`
   - transition request/approval, skill approval, controller/gate/supervisor/orchestration contracts。
6. `eco_concil_runtime/canonical_reporting_contracts.py`
   - report section, reporting handoff, council decision, expert report, final publication contracts。
7. `eco_concil_runtime/canonical_contract_registry.py`
   - merged registry and lookup functions。

命名说明：

1. Use plane names exactly matching `PLANE_SIGNAL`、`PLANE_ANALYSIS`、`PLANE_DELIBERATION`、`PLANE_RUNTIME`、`PLANE_REPORTING`。
2. Registry remains central, definitions become plane-scoped.

### 4.8 Runtime Governance Modules

目标结构：

1. `kernel/governance/skill_approvals.py`
2. `kernel/governance/transition_requests.py`
3. `kernel/governance/admission_policy.py`
4. `kernel/operator/dead_letters.py`
5. `kernel/operator/runtime_health.py`
6. `kernel/operator/runbook.py`
7. `kernel/execution/attempts.py`
8. `kernel/execution/failures.py`
9. `kernel/execution/controller.py`
10. `kernel/execution/executor.py`

兼容入口继续保留：

1. `kernel/skill_approvals.py`
2. `kernel/transition_requests.py`
3. `kernel/operations.py`
4. `kernel/executor.py`

### 4.9 Benchmark / Post-round

目标模块：

1. `kernel/benchmark_fixtures.py`
2. `kernel/benchmark_manifests.py`
3. `kernel/benchmark_compare.py`
4. `kernel/benchmark_replay.py`
5. `kernel/post_round_close.py`
6. `kernel/post_round_archives.py`
7. `kernel/post_round_history.py`

兼容入口继续保留：

1. `kernel/benchmark.py`
2. `kernel/post_round.py`

### 4.10 Skill Scripts

原则：

1. skill 是能力边界，不是代码体积边界。
2. 行数不作为 skill 拆分理由。
3. 原子 skill 可以保留单脚本实现；只有当脚本内部混合多个独立用户动作、独立输入契约或独立 artifact 家族时，才考虑拆成多个 skill。
4. skill 内部实现整理可以使用 helper，但 helper 应优先保持 skill-local，避免制造 runtime shared dependency。
5. provider/client/pagination/rate-limit/normalization/write-output 可以作为内部组织线索，但不能自动推出多个 skill id。
6. 对 GDELT / YouTube / Open-Meteo 这类多 skill 家族，只有在多个 skill 已经重复同一 provider 协议实现时，才考虑抽 provider helper；抽 helper 不改变 skill 原子能力。

命名说明：

1. Shared source helpers 使用 `source_*` 前缀，贴近现有 `source_queue_*`。
2. Skill-local helper 使用 provider + object 名称，如 `youtube_comments_client.py`。
3. 任何 skill 拆分都必须先写明原子性判断，而不是引用文件行数。

## 5. 执行顺序

### P0：拆分准备与保护网

目标：

1. 为 facade 模块补 `__all__` 或 explicit re-export 清单。
2. 固定 import compatibility smoke tests。
3. 固定 CLI help smoke tests。
4. 固定 module-size check，但先作为报告，不阻断 CI。

验收：

1. `python3 tools/quality_gate.py syntax`
2. `python3 tools/quality_gate.py test module-decomposition schema-migration runtime-governance`
3. 关键 public imports 在测试中显式覆盖。

当前状态：已完成。

### P1：Deliberation Plane 第一阶段

目标：

1. 抽 `deliberation_plane_schema.py`。
2. 抽 `deliberation_plane_rows.py`。
3. 保持 `deliberation_plane.py` re-export。
4. 不移动 store/load 业务函数。

验收：

1. `python3 tools/quality_gate.py test schema-migration db-recovery`
2. `python3 -m unittest tests.test_board_workflow`
3. `python3 tools/quality_gate.py test runtime-governance reporting`

当前状态：已完成第一阶段代码交付。

当前结果：

1. `deliberation_plane_schema.py` 已承载 schema、connect、migration 和 schema status。
2. `deliberation_plane_rows.py` 已承载通用 row helper、基础 write row helper、board/transition row conversion。
3. `deliberation_plane.py` 在 P1 后仍保留后续 store/load 业务函数与兼容导出；这些业务函数已在 P2 继续拆出。

### P2：Deliberation Plane 第二阶段

目标：

1. 抽 board state、round snapshot、round transition。
2. 抽 moderator actions、probes、readiness。
3. 抽 reporting records 与 runtime control records。
4. 让 `deliberation_plane.py` 降到 facade + compatibility glue。

当前状态：已完成。

当前结果：

1. `deliberation_board_state.py` 已承载 board bootstrap、sync、mutation、round snapshot 与 round transition artifact import/store/load。
2. `deliberation_plane_rows.py` 已承载 shared DB record query helpers，避免额外 query helper 文件。
3. `deliberation_actions.py` 已承载 moderator actions、falsification probes、round readiness。
4. `deliberation_reporting_records.py` 已承载 reporting 记录共用 canonical default、角色与嵌套字段 helper，以及 report basis freeze、reporting handoff、council decision、expert report、final publication。
5. `deliberation_runtime_control.py` 已承载 runtime control freeze、controller/gate/supervisor snapshots、governed_execution control state、moderator work surface、orchestration plan/step 与 round task snapshot。
6. `deliberation_plane.py` 当前约 285 行，只保留 import/re-export facade。

验收：

1. `python3 tools/quality_gate.py test db-recovery runtime-governance reporting`
2. `python3 -m unittest tests.test_board_workflow tests.test_council_submission_workflow tests.test_council_query_surface`
3. `python3 tools/quality_gate.py full`

本阶段实测：

1. `python3 tools/quality_gate.py syntax`，219 个 Python 文件通过。
2. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_schema_migrations tests.test_board_workflow tests.test_runtime_kernel`，72 tests 通过。
3. `python3 tools/quality_gate.py test module-decomposition schema-migration db-recovery runtime-governance reporting`，95 tests 通过。
4. `python3 tools/quality_gate.py full`，268 tests 通过。
5. `git diff --check` 通过。

### P3：Analysis Plane

目标：

1. 抽 schema/contracts/artifacts/results/queries。
2. relation query 收敛进 `analysis_plane_queries.py`。
3. 保持所有 `sync_*_result_set` 与 `load_*_context` names 不变。

验收：

1. `python3 tools/quality_gate.py test relation-taxonomy optional-guardrails`
2. `python3 tools/quality_gate.py test db-recovery`
3. `python3 tools/quality_gate.py full`

当前状态：已完成。

当前结果：

1. `analysis_plane_schema.py` 已承载 analysis schema、DB path resolution、connect 与 schema ensure。
2. `analysis_plane_contracts.py` 已承载共用 JSON/text/hash/time helper、analysis kind constants、kind config、governance metadata 与 kind registry。
3. `analysis_plane_support.py` 已承载 artifact path/ref normalization、dedupe、presence helper、result contract、lineage、parent result set 与 parent artifact ref build/load。
4. `analysis_plane_results.py` 已承载 generic result set sync/load 与 analysis context loading。
5. `analysis_plane_queries.py` 已承载 result set/item query、paging、serialization 与 spatiotemporal relation cue 专用查询。
6. `analysis_plane_contexts.py` 已承载 typed sync/load compatibility wrappers。
7. `analysis_plane.py` 当前约 242 行，只保留 import/re-export facade。

本阶段实测：

1. `python3 tools/quality_gate.py syntax`，219 个 Python 文件通过。
2. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_optional_analysis_guardrails tests.test_spatiotemporal_relation_taxonomy`，33 tests 通过。
3. `python3 -m unittest tests.test_runtime_kernel`，47 tests 通过。
4. `python3 tools/quality_gate.py test relation-taxonomy optional-guardrails module-decomposition`，34 tests 通过。
5. `python3 tools/quality_gate.py test module-decomposition schema-migration db-recovery runtime-governance reporting`，95 tests 通过。
6. `python3 tools/quality_gate.py test relation-taxonomy optional-guardrails db-recovery schema-migration module-decomposition runtime-governance reporting case-study`，125 tests 通过。
7. `python3 tools/quality_gate.py full`，268 tests 通过。
8. `git diff --check` 通过。

### P4：Optional Analysis Helpers

目标：

1. 按 helper family 拆分。
2. 保留 `optional_analysis_helpers.py` re-export。
3. 不改变 helper governance metadata。

验收：

1. `python3 tools/quality_gate.py test optional-guardrails relation-taxonomy`
2. `python3 -m unittest tests.test_spatiotemporal_relation_taxonomy`
3. `python3 tools/quality_gate.py full`

### P5：CLI 和 Operator Views

目标：

1. 抽 parser 与 command handlers。
2. 抽 `show_run_state` 到 `run_state_view.py`。
3. 抽 operator views。
4. 保留 `kernel/cli.py main`。

验收：

1. CLI smoke tests 覆盖 `--help`、`show-run-state`、`show-schema-status`、`query-*`、approval commands。
2. `python3 tools/quality_gate.py test runtime-governance reporting schema-migration`
3. `python3 tools/quality_gate.py full`

### P6：Council / Analysis Object Registries

目标：

1. 拆 `council_objects.py`。
2. 拆 `analysis_objects.py`。
3. 拆 `canonical_contracts.py` 为 plane-scoped definitions + registry。

验收：

1. `python3 -m unittest tests.test_canonical_contracts tests.test_council_query_surface tests.test_reporting_query_surface`
2. `python3 tools/quality_gate.py test relation-taxonomy reporting`
3. `python3 tools/quality_gate.py full`

### P7：Runtime Governance 支撑模块

目标：

1. 拆 skill approval 与 transition request store/payload。
2. 拆 operations/executor。
3. 保留原模块 facade。

验收：

1. `python3 tools/quality_gate.py test runtime-governance`
2. `python3 tools/quality_gate.py test runtime-governance reporting`
3. `python3 tools/quality_gate.py full`

### P8：Benchmark / Post-round

目标：

1. 拆 benchmark。
2. 拆 post-round。
3. 不改变 replay/case fixture semantics。

验收：

1. `python3 tools/quality_gate.py test case-study`
2. `python3 -m unittest tests.test_archive_history_workflow`
3. `python3 tools/quality_gate.py full`

### P9：大型 Skill Scripts

目标：

1. 逐个拆 provider-heavy skill。
2. wrapper scripts 保持原路径。
3. 对每个 skill 增加至少一个 command-level fixture smoke test。

优先顺序：

1. YouTube comments / video search。
2. Bluesky cascade。
3. Open-Meteo family。
4. GDELT family。
5. NASA FIRMS / USGS。
6. open-investigation-round / freeze-report-basis / reporting handoff。

验收：

1. 对应 skill workflow tests。
2. `python3 tools/quality_gate.py full`。

### P10：测试文件拆分

目标：

1. 拆 `tests/test_runtime_kernel.py`。
2. 拆 `tests/test_board_workflow.py`。
3. 拆 `tests/test_spatiotemporal_relation_taxonomy.py`。
4. 更新 `tools/quality_gate.py` suite composition。

建议目标：

1. `tests/test_runtime_kernel_receipts.py`
2. `tests/test_runtime_kernel_approvals.py`
3. `tests/test_runtime_kernel_operator_surfaces.py`
4. `tests/test_runtime_kernel_controller.py`
5. `tests/test_board_round_workflow.py`
6. `tests/test_board_recovery_workflow.py`
7. `tests/test_relation_contracts.py`
8. `tests/test_relation_workflow.py`

验收：

1. targeted suite 数量和覆盖面不下降。
2. `python3 tools/quality_gate.py ci` 通过。

## 6. 禁止事项

1. 不恢复旧 phase 命名或旧 phase 兼容入口。
2. 不把 schema migration 混入模块拆分。
3. 后续不再无计划改变最新 CLI command names。
4. 不改变 skill ids。
5. 不把 optional-analysis helper 升级为 authoritative conclusion。
6. 不移动 artifact output paths。
7. 不把真实案例 case 运行混入本计划。

## 7. 完成定义

大模块拆分完成时应满足：

1. `deliberation_plane.py` 低于 600 行，且只包含 facade/re-export 与少量 compatibility glue。
2. `analysis_plane.py` 低于 500 行，且只包含 facade/re-export。
3. `optional_analysis_helpers.py` 低于 400 行，且只包含 facade/re-export。
4. `kernel/cli.py` 低于 800 行，主要保留 `main`、`build_parser` glue。
5. 任一生产 Python 文件超过 1500 行时，必须有明确保留理由或后续拆分 issue。
6. 全部现有 targeted gates 与 full gate 通过。
7. 文档列出的最新 public import、CLI command、skill id、canonical contract 和 DB schema 均保持稳定。
