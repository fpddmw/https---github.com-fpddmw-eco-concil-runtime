# OpenClaw 大模块彻底拆分工作计划

## 1. 文档定位

本文是大模块拆分的持久化执行计划。它不是全局总计划，也不包含真实案例 case 工作。

目标是在不改变 runtime 行为、skill id、CLI 命令、canonical contract、DB schema 和 artifact shape 的前提下，把当前过大的 Python 模块拆成可维护、可测试、可逐步迁移的模块族。Python import path 以本文记录的最新包结构为准，不保留旧 phase 或旧 flat kernel 路径。

本计划以 2026-05-06 的仓库状态为基线。当前已验证：syntax 覆盖 285 个 Python 文件，module-decomposition gate 通过，P6 相关 canonical/council/reporting/relation/optional 组合测试通过，P7/P7.5/P7.6 runtime governance/reporting/signal/package targeted gate 通过 113 tests，P7.7 governance package targeted gate 通过 86 tests，full gate 通过 268 tests。schema migration 硬化已完成第一块代码，模块拆分 P0 保护网已落地，P1/P2 已完成 deliberation plane 的 facade 化拆分，P3 已完成 analysis plane 的 facade 化拆分，P4 已完成 optional analysis helper family 全量拆分与包级收敛，并已完成旧 phase 命名清理、runtime/kernel 浅层包结构整理，P5 CLI/operator view、CLI parser 与 runtime command handler 拆分，P6 council/analysis/canonical object registry 拆分与包级收敛，P7 runtime governance 两轮支撑模块拆分，P7.5 顶层 `src` 暴露面收敛，P7.6 kernel 内部包级收敛第一轮，以及 P7.7 governance package 收敛。

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
5. 新增 `eco_concil_runtime/kernel/planes/deliberation_plane_schema.py`。
   - 承载 `SCHEMA_SQL`、DB path resolution、`connect_db`、schema migration 与 schema status。
   - 不改变 SQLite 表、列、index 或 migration id。
6. 新增 `eco_concil_runtime/kernel/planes/deliberation_plane_rows.py`。
   - 承载通用 JSON/row helper、`payload_from_db_row`、基础 `write_*_row`、board/transition row conversion 与 DB-backed record query helpers。
7. `eco_concil_runtime/kernel/planes/deliberation_plane.py` 保持当前 public 入口。
   - 继续 re-export 原 public names。
   - 当前约 285 行，已从 P0 基线约 8158 行下降。
8. 新增 `eco_concil_runtime/kernel/planes/deliberation_board_state.py`。
   - 承载 board path、JSON export、board bootstrap、board sync、round snapshot、board mutation 与 round transition store/load。
9. 新增 `eco_concil_runtime/kernel/planes/deliberation_actions.py`。
   - 承载 moderator actions、falsification probes、round readiness 的 normalization、store/load 与 snapshot wrapper。
10. 新增 `eco_concil_runtime/kernel/planes/deliberation_reporting_records.py`。
   - 承载 report basis freeze、reporting handoff、council decision、expert report、final publication，以及 reporting record 共用 canonical default helper。
11. 新增 `eco_concil_runtime/kernel/planes/deliberation_runtime_control.py`。
   - 承载 runtime control freeze、controller/gate/supervisor snapshots、governed_execution control state、moderator work surface、orchestration plan/step 与 round task snapshot。
12. 新增 `eco_concil_runtime/kernel/planes/analysis_plane_schema.py`。
   - 承载 analysis DB schema、path resolution、connect 与 legacy column ensure。
13. 新增 `eco_concil_runtime/kernel/planes/analysis_plane_contracts.py`。
   - 承载 analysis plane 共用 JSON/text/hash/time helper、analysis kind constants、kind config、governance metadata 与 kind registry。
14. 新增 `eco_concil_runtime/kernel/planes/analysis_plane_support.py`。
   - 承载 artifact path/ref normalization、dedupe、artifact presence helper、result contract、lineage、parent result set 与 parent artifact refs 的 build/load helper。
15. 新增 `eco_concil_runtime/kernel/planes/analysis_plane_results.py`。
   - 承载 generic sync/load result set persistence 与 context loading。
16. 新增 `eco_concil_runtime/kernel/planes/analysis_plane_queries.py`。
   - 承载 result set/item 查询、分页、serialization 与 spatiotemporal relation cue 专用查询。
17. 新增 `eco_concil_runtime/kernel/planes/analysis_plane_contexts.py`。
   - 承载 typed `sync_*_result_set` 与 `load_*_context` compatibility wrappers。
18. `eco_concil_runtime/kernel/planes/analysis_plane.py` 保持当前 public 入口。
   - 继续 re-export 原 public names。
   - 当前约 242 行，已从 P0 基线约 3697 行下降。
19. 新增 `eco_concil_runtime/kernel/operator/run_state_view.py`。
   - 承载 `show_run_state`、transition state、operations state、reporting state composition，以及 governed/reporting/post-round/benchmark operator views。
   - `eco_concil_runtime/kernel/cli.py` 保留 `build_parser`、`main` 与上述状态视图符号的 public import。
   - `cli.py` 已从约 3869 行下降到约 2469 行。
20. 新增 `eco_concil_runtime/kernel/operator/cli_parser.py`。
   - 承载 subcommand parser construction 与 shared arg helpers。
   - `eco_concil_runtime/kernel/cli.py` 继续导出 `build_parser`。
   - `cli.py` 已继续下降到约 1815 行。
21. 新增 `eco_concil_runtime/kernel/operator/cli_runtime_commands.py`。
   - 承载 CLI JSON/output helper、`init_run`、early runtime commands、`run-skill`、`preflight-skill`、admission policy、runtime health、operator runbook 和 dead-letter command handlers。
   - `eco_concil_runtime/kernel/cli.py` 继续导出 `init_run`、`pretty_json` 等既有 helper symbols。
   - `cli.py` 已继续下降到约 1575 行。
22. 新增 `eco_concil_runtime/optional_analysis/support.py`。
   - 承载 optional-analysis 共用 helper governance metadata、signal DB query、时间/空间过滤、artifact refs、lineage 与 board handoff helper。
   - `eco_concil_runtime/optional_analysis/__init__.py` 承载当前 package public API。
23. 新增 `eco_concil_runtime/optional_analysis/relations.py`。
   - 承载 spatiotemporal relation cue construction 与 relation alternative review helper family。
   - `run_detect_temporal_cooccurrence_cues`、`run_review_spatiotemporal_relation_alternatives` 继续从 `optional_analysis` package API 导出。
24. 新增 `eco_concil_runtime/optional_analysis/environment_evidence.py`。
   - 承载 environment evidence aggregation helper family。
25. 新增 `eco_concil_runtime/optional_analysis/scope_review.py`。
   - 承载 structured verification scope 与 fact-check evidence scope review helper family。
26. 新增 `eco_concil_runtime/optional_analysis/research_issues.py`。
   - 承载 discourse issue discovery、evidence lanes、research issue surface/views/map helper family。
27. 新增 `eco_concil_runtime/optional_analysis/formal_public.py`。
   - 承载 approved formal/public taxonomy labels、formal/public footprints、representation audit helper family。
   - 顶层 `optional_analysis_*` 平铺文件已收敛到 package 目录。
28. 新增 `eco_concil_runtime/objects/analysis/common.py`、`signals.py`、`issues.py`、`verification.py`、`relations.py`。
   - `eco_concil_runtime/objects/analysis/__init__.py` 承载当前 package public API。
   - signal/issue/verification/relation family 分别承载对应 canonical analysis object normalization。
29. 新增 `eco_concil_runtime/objects/council/schema.py`、`payloads.py`、`rows.py`、`store.py`、`query.py`、`decision_traces.py`。
   - `eco_concil_runtime/objects/council/__init__.py` 承载当前 package public API。
   - 不改变 council SQLite schema、append/store/query result shape 或 decision trace contract。
30. 新增 `eco_concil_runtime/contracts/types.py`、`signal.py`、`analysis.py`、`deliberation.py`、`runtime.py`、`reporting.py`、`registry.py`。
   - `eco_concil_runtime/contracts/__init__.py` 承载当前 package public API。
   - canonical definitions 按 plane 分组，registry 继续合并为 50 个 canonical contracts。
31. 新增 `eco_concil_runtime/kernel/governance/transition_requests/` package。
   - `transition_requests/__init__.py` 为当前 public API。
   - `common.py`、`payloads.py`、`rows.py`、`store.py` 分别承载 transition kind/spec、payload、row conversion/write、store/load/approve/reject/commit/resolve。
   - transition kind/spec、payload、row conversion/write、store/load/approve/reject/commit/resolve 已分离。
32. 新增 `eco_concil_runtime/kernel/governance/skill_approvals/` package。
   - `skill_approvals/__init__.py` 为当前 public API。
   - `common.py`、`payloads.py`、`rows.py`、`store.py` 分别承载 skill approval request/approval/rejection/consumption 的 payload、row conversion/write、store/load/approve/reject/consume/resolve。
   - skill approval request/approval/rejection/consumption 的 payload、row conversion/write、store/load/approve/reject/consume/resolve 已分离。
33. 新增 `eco_concil_runtime/kernel/operator/operations_common.py`、`admission_policy.py`、`dead_letters.py`、`runtime_health.py`、`runbook.py`。
   - `operator/operations.py` 保留 facade/re-export，当前约 43 行。
   - admission policy、dead letter、runtime health 与 operator runbook 已分离。
34. 新增 `eco_concil_runtime/kernel/execution/executor_common.py`、`executor_command_hints.py`、`executor_failures.py`。
   - `execution/executor.py` 保留 `run_skill` 主流程，当前约 882 行。
   - common hashing/time/error、command hints、structured failure/dead-letter extraction/health refresh helper 已分离；attempt loop 和 receipt/postflight 主流程后续视风险继续拆。
35. 新增 `eco_concil_runtime/kernel/operator/surfaces/` package。
   - `operator/surfaces/__init__.py` 为当前 public API。
   - `common.py`、`reporting.py`、`investigation.py`、`execution.py`、`publication.py` 按 operator-facing runtime surface 边界组织。
   - runtime surface common helper、reporting gate enrichment、investigation wrappers、controller/gate/supervisor/orchestration wrappers、reporting publication wrappers 已分离。
36. 新增 `eco_concil_runtime/kernel/planes/signal/` package。
   - `planes/signal/__init__.py` 为当前 public API。
   - `common.py`、`schema.py`、`metadata.py`、`store.py`、`finalize.py`、`evidence.py` 按 signal plane normalization/evidence 边界组织。
   - signal normalizer 的 common helper、schema migration/connect、taxonomy metadata enrichment、store/index write、streaming finalize 已分离。
37. 新增 `eco_concil_runtime/kernel/execution/controller/` package。
   - `execution/controller/__init__.py` 为当前 controller public API。
   - `artifacts.py`、`planning_adapters.py`、`transition_planning.py` 按 controller support 边界组织。
   - artifact path/state persistence、planning adapter wrappers、transition-executor planning 与 stage approval guard 已分离；controller 主执行循环仍保留在入口模块。
38. 完成 P7.5 顶层 package encapsulation。
   - 顶层 `eco_council_runtime/` `.py` 文件从 39 个收敛到 11 个。
   - 当前最新 public import path 为 `eco_council_runtime.contracts`、`eco_council_runtime.objects.analysis`、`eco_council_runtime.objects.council`、`eco_council_runtime.optional_analysis`。
   - 旧 `canonical_*`、`analysis_*`、`council_*`、`optional_analysis_*` 顶层平铺路径不再保留。
39. 完成 P7.6 kernel package encapsulation 第一轮。
   - `kernel/operator/` 顶层 `.py` 文件从 18 个收敛到 12 个。
   - `kernel/planes/` 顶层 `.py` 文件从 23 个收敛到 16 个。
   - `kernel/execution/` 顶层 `.py` 文件从 20 个收敛到 16 个。
   - 当前最新 public import path 为 `eco_council_runtime.kernel.operator.surfaces`、`eco_council_runtime.kernel.planes.signal`、`eco_council_runtime.kernel.execution.controller`。
40. 完成 P7.7 governance package encapsulation。
   - `kernel/governance/` 顶层 `.py` 文件从 26 个收敛到 7 个。
   - 当前最新 public import path 为 `eco_council_runtime.kernel.governance.skill_approvals`、`eco_council_runtime.kernel.governance.transition_requests`、`eco_council_runtime.kernel.governance.agent_entry`、`eco_council_runtime.kernel.governance.fallback`。

当前已验证命令：

1. `python3 tools/quality_gate.py syntax`，285 个 Python 文件通过，无重复字面量 dict key。
2. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_spatiotemporal_relation_taxonomy tests.test_canonical_contracts`，32 tests 通过。
3. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_council_query_surface tests.test_council_submission_workflow tests.test_canonical_contracts tests.test_reporting_query_surface`，26 tests 通过。
4. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_canonical_contracts tests.test_council_query_surface tests.test_reporting_query_surface tests.test_spatiotemporal_relation_taxonomy tests.test_optional_analysis_guardrails`，47 tests 通过。
5. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting case-study`，89 tests 通过。
6. `python3 tools/quality_gate.py test relation-taxonomy reporting module-decomposition optional-guardrails`，57 tests 通过。
7. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting`，86 tests 通过。
8. `python3 tools/quality_gate.py full`，268 tests 通过。
9. `git diff --check` 通过。
10. `python3 -m unittest tests.test_optional_analysis_guardrails tests.test_spatiotemporal_relation_taxonomy`，27 tests 通过。
11. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_runtime_kernel`，54 tests 通过。
12. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting relation-taxonomy optional-guardrails`，113 tests 通过。
13. `python3 tools/quality_gate.py full`，268 tests 通过；P7.5 package encapsulation 后耗时约 8 分 58 秒。
14. `python3 tools/quality_gate.py full`，268 tests 通过；P7.7 governance package encapsulation 后耗时约 7 分 58 秒。

## 2. 拆分原则

1. 保留最新入口。
   - 最新 public import path 以浅层包结构为准。
   - 旧 phase 命名和旧 flat kernel 路径不再保留。
   - `kernel/planes/deliberation_plane.py`、`kernel/planes/analysis_plane.py`、`optional_analysis/__init__.py`、`objects/*/__init__.py`、`contracts/__init__.py`、`kernel/cli.py` 等作为当前 public entry。
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

1. `eco-concil-runtime/src/eco_council_runtime/kernel/planes/deliberation_plane.py`
   - P0 基线约 8158 行，P2 交付后当前约 285 行。
   - 原先同时承载 schema、migration、row conversion、board sync、governed_execution control、moderator actions、falsification probes、reporting records、round transitions、round snapshots。
   - 已拆为 facade/re-export 入口，后续只保留 compatibility glue。
2. `eco-concil-runtime/src/eco_council_runtime/kernel/cli.py`
   - 当前约 3864 行。
   - 同时承载 parser、command dispatch、run state view、operator views、analysis/council/reporting/control query commands。
   - 必须彻底拆分，但外部命令名必须保持不变。
3. `eco-concil-runtime/src/eco_council_runtime/kernel/planes/analysis_plane.py`
   - P0 基线约 3697 行，P3 交付后当前约 248 行。
   - 原先同时承载 analysis schema、result set persistence、artifact refs、query paging、relation query、typed sync/load wrappers。
   - 已拆为 facade/re-export 入口，后续只保留 compatibility glue。
4. `eco-concil-runtime/src/eco_council_runtime/optional_analysis/__init__.py`
   - 当前约 2671 行。
   - 同时承载 helper common、signal query、evidence aggregation、scope review、research issue、formal/public taxonomy、relation cues、alternative review。
   - 必须拆分。
5. `eco-concil-runtime/src/eco_council_runtime/objects/council/__init__.py`
   - 当前约 2279 行。
   - 同时承载 council schema、payload normalization、append/store/query、bundle helpers、decision trace。
   - 必须拆分。
6. `eco-concil-runtime/src/eco_council_runtime/objects/analysis/__init__.py`
   - 当前约 2005 行。
   - 同时承载多类 analysis canonical object normalization。
   - 必须拆分或至少按 object family 分组。
7. `eco-concil-runtime/src/eco_council_runtime/contracts/__init__.py`
   - 当前约 1524 行。
   - 作为 central contract registry 保留，但各 plane 的 contract definitions 应拆入独立文件。
   - 必须谨慎拆分，保留 `contracts/__init__.py` facade。

### 3.2 runtime 治理与控制面应拆

这些文件未必都超过 2000 行，但已经承担多个职责。它们应在核心 plane 拆分后继续拆。

1. `eco-concil-runtime/src/eco_council_runtime/kernel/archive/benchmark/__init__.py`
   - 已收敛为 package public API。
   - `common.py`、`manifest.py`、`compare.py`、`replay.py` 分别承载 artifact digest/snapshot、benchmark manifest/scenario fixture、manifest comparison、scenario replay。
2. `eco-concil-runtime/src/eco_council_runtime/kernel/governance/skill_approvals/__init__.py`
   - 已收敛为 package public API。
   - `common.py`、`payloads.py`、`rows.py`、`store.py` 分别承载 approval constants/policy/id、canonical payload、row conversion/write、store/load/approve/reject/consume/resolve。
3. `eco-concil-runtime/src/eco_council_runtime/kernel/execution/controller/__init__.py`
   - 当前约 1316 行。
   - 拆分 controller state transitions、stage execution、controller event materialization。
4. `eco-concil-runtime/src/eco_council_runtime/kernel/governance/transition_requests/__init__.py`
   - 已收敛为 package public API。
   - `common.py`、`payloads.py`、`rows.py`、`store.py` 分别承载 transition kind/spec、canonical payload、row conversion/write、store/load/approve/reject/commit/resolve。
5. `eco-concil-runtime/src/eco_council_runtime/kernel/operator/surfaces/__init__.py`
   - 当前约 1040 行。
   - 拆分 controller/gate/supervisor/reporting/fallback wrapper readers。
6. `eco-concil-runtime/src/eco_council_runtime/kernel/planes/signal/__init__.py`
   - 当前约 1032 行。
   - 拆分 schema/indexing、signal payload normalization、metadata indexing、row persistence。
7. `eco-concil-runtime/src/eco_council_runtime/kernel/operator/operations.py`
   - 当前约 1015 行。
   - 拆分 admission policy、dead letters、runtime health、operator runbook。
8. `eco-concil-runtime/src/eco_council_runtime/kernel/execution/executor.py`
   - 当前约 960 行。
   - 拆分 command building、attempt execution、structured failure, receipt/postflight handling。
9. `eco-concil-runtime/src/eco_council_runtime/kernel/archive/post_round/__init__.py`
   - 已收敛为 package public API。
   - `common.py`、`close.py`、`history.py` 分别承载 close/history 共享状态、round close workflow、history bootstrap workflow。

### 3.3 profile/config 大文件应整理

这些文件多为规则/配置集合，拆分方式应避免改变 runtime 语义。

1. `eco-concil-runtime/src/eco_council_runtime/kernel/governance/agent_entry/profile.py`
   - 当前约 1222 行。
   - 拆为 gate profile、role expectations、entry chain profile。
2. `eco-concil-runtime/src/eco_council_runtime/kernel/governance/fallback/agenda_profile.py`
   - 当前约 755 行。
   - 可在 governed_execution profile 家族中整理。
3. `eco-concil-runtime/src/eco_council_runtime/formal_signal_semantics.py`
   - 当前约 762 行。
   - 可按 issue/stance/citation/actor facets 拆分，但优先级低于 runtime/kernel。

### 3.4 Runtime 结构与旧 phase 命名清理

当前 `eco_council_runtime/kernel/` 文件数量已经偏多。旧 `phase2` 命名已经不再作为兼容入口保留；当前代码、测试、CLI 和 skill 脚本统一以 governed execution 命名表达“受治理的运行时执行链”。

本轮命名清理后的最新命名基线：

1. `kernel/operator/surfaces/__init__.py`
   - 原旧相位状态 surface 名称已替换为 runtime state surface。
2. `agent_entry/profile.py`、`agent_entry/handoff.py`
   - agent entry 不再挂在旧相位名下。
3. `runtime_gate_profile.py`、`runtime_gate_handlers.py`
   - gate profile / handler 归入 runtime gate 概念。
4. `runtime_planning_profile.py`、`runtime_posture_profile.py`、`runtime_stage_profile.py`、`runtime_round_profile.py`
   - planning、posture、stage、round profile 使用 runtime 语义。
5. `governed_execution_controller_state.py`、`governed_execution_exports.py`
   - 仅在确实表达“受治理执行链”整体产物时使用 governed execution。
6. CLI 命令使用 `run-governed-execution-round`、`resume-governed-execution-round`、`restart-governed-execution-round`、`materialize-governed-execution-exports`。
7. 旧 `phase2_*` import path、函数名、CLI 命令和测试 patch path 不再保留。

本轮已完成 runtime/kernel 浅层包结构迁移，后续不再继续在 `kernel/` 根目录横向增加文件。当前包边界如下：

1. `kernel/planes/`
   - 承载 signal、analysis、deliberation 等数据面 facade 和持久化模块。
   - 现有 public package API 继续按最新命名暴露。
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

1. `kernel/operator/surfaces/__init__.py` 当前保留原名；它负责读取/构造 runtime surface wrapper，不与 `kernel/operator/run_state_view.py` 的 operator-facing 汇总视图混淆。
2. `kernel/governance/agent_entry/profile.py`、`kernel/governance/agent_entry/handoff.py` 已纳入 agent entry package。
3. `kernel/governance/fallback/agenda.py`、`kernel/governance/fallback/agenda_profile.py` 已纳入 fallback package。
4. controller、executor、gate、supervisor 已纳入 `kernel/execution/`。

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

当前 public 入口：

1. `eco_concil_runtime/kernel/planes/deliberation_plane.py`
   - 保留原 public API。
   - 只做 re-export / facade。
   - 暂不删除旧函数名。

目标模块：

1. `eco_concil_runtime/kernel/planes/deliberation_plane_schema.py`
   - `SCHEMA_SQL`
   - `connect_db`
   - `default_db_path`
   - `resolve_db_path`
   - `ensure_schema_migrations`
   - `load_schema_status`
2. `eco_concil_runtime/kernel/planes/deliberation_plane_rows.py`
   - `payload_from_db_row`
   - `*_row_from_payload`
   - `write_*_row`
   - shared JSON/row conversion helpers。
   - shared DB-backed query helpers。
3. `eco_concil_runtime/kernel/planes/deliberation_board_state.py`
   - board run/event/note/hypothesis/challenge/task store-load。
   - `sync_board_to_deliberation_plane`
   - `bootstrap_board_state`
   - `commit_board_mutation`
   - `load_round_snapshot`
   - `fetch_round_events`
   - `fetch_round_state`
   - `store_round_transition_record`
   - `load_round_transition_record`
4. `eco_concil_runtime/kernel/planes/deliberation_actions.py`
   - moderator action records。
   - falsification probe records。
   - readiness assessment records。
5. `eco_concil_runtime/kernel/planes/deliberation_runtime_control.py`
   - runtime control freeze。
   - controller/gate/supervisor snapshots。
   - `load_governed_execution_control_state`。
   - orchestration plan/steps。
   - round task snapshot。
   - planner-backed governed_execution plan persistence。
6. `eco_concil_runtime/kernel/planes/deliberation_reporting_records.py`
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

当前 public 入口：

1. `eco_concil_runtime/kernel/planes/analysis_plane.py`
   - 保留 `sync_*_result_set`、`load_*_context`、`query_*` 等现有 public names。

目标模块：

1. `eco_concil_runtime/kernel/planes/analysis_plane_schema.py`
   - analysis DB schema 和 connect/ensure。
2. `eco_concil_runtime/kernel/planes/analysis_plane_contracts.py`
   - common JSON/text/hash/time helper。
   - `analysis_config`
   - governance metadata。
   - analysis kind registry。
3. `eco_concil_runtime/kernel/planes/analysis_plane_support.py`
   - artifact refs。
   - path resolution。
   - artifact presence checks。
   - result contract building/loading。
   - lineage entries。
   - parent result set/artifact refs。
4. `eco_concil_runtime/kernel/planes/analysis_plane_results.py`
   - result set and item persistence。
   - result wrapper loading。
   - generic sync/load wrapper shared implementation。
5. `eco_concil_runtime/kernel/planes/analysis_plane_queries.py`
   - result set query。
   - item query。
   - paging/serialization。
   - `query_spatiotemporal_relation_cues`
   - relation-specific filters and serialization。
6. `eco_concil_runtime/kernel/planes/analysis_plane_contexts.py`
   - typed `sync_*_result_set` compatibility wrappers。
   - typed `load_*_context` compatibility wrappers。

命名说明：

1. 保留 `analysis_plane_*`，对应现有 plane 名称。
2. relation 查询保留在 `analysis_plane_queries.py`，避免为单个查询族增加额外小文件。
3. support 文件只收纳 artifact/result-contract 这类共享支持能力，不继续拆成更细 helper 文件。

### 4.3 Optional Analysis Helpers

当前 public 入口：

1. `eco_concil_runtime/optional_analysis/__init__.py`
   - 保留所有 `run_*` helper 名称。
   - 作为 facade/re-export。

目标模块：

1. `eco_concil_runtime/optional_analysis/support.py`
   - JSON/path/text/hash/common helper。
   - helper metadata。
   - board handoff helper。
   - signal DB connect/query、row-to-signal、evidence refs。
   - relation/time/space filter support。
2. `eco_concil_runtime/optional_analysis/environment_evidence.py`
   - `run_aggregate_environment_evidence`
   - source/metric distribution。
3. `eco_concil_runtime/optional_analysis/scope_review.py`
   - fact-check evidence scope。
   - structured verification scope。
4. `eco_concil_runtime/optional_analysis/research_issues.py`
   - discourse issue discovery。
   - evidence lanes。
   - research issue surface/views/map。
5. `eco_concil_runtime/optional_analysis/formal_public.py`
   - approved taxonomy labels。
   - formal/public footprints。
   - representation audit cues。
6. `eco_concil_runtime/optional_analysis/relations.py`
   - temporal cooccurrence。
   - structured spatiotemporal relation cues。
   - relation alternative review。

命名说明：

1. 使用 `optional_analysis_*` 前缀，保持与 layer 名称一致。
2. 不把 optional helper 变成 phase gate 或 report basis，这一点必须在拆分后继续由 tests 固定。

### 4.4 Runtime CLI

当前 public 入口：

1. `eco_concil_runtime/kernel/cli.py`
   - 保留 `build_parser` 和 `main`。
   - 继续 re-export `show_run_state`、`reporting_state_for_round` 等状态视图 public symbols。
   - 可继续作为 console script 入口。

已落地模块：

1. `eco_concil_runtime/kernel/operator/run_state_view.py`
   - `show_run_state`
   - operations state。
   - transition state。
   - benchmark/post-round/reporting/governed execution state composition。
   - governed execution operator view。
   - `reporting_operator_view`
   - `post_round_operator_view`
   - `benchmark_operator_view`
2. `eco_concil_runtime/kernel/operator/cli_parser.py`
   - subcommand parser construction。
   - shared arg helpers。
3. `eco_concil_runtime/kernel/operator/cli_runtime_commands.py`
   - CLI JSON/output helper。
   - `init-run`、`run-skill`、`preflight-skill`。
   - admission/health/runbook/dead-letter/schema-status commands。

后续候选模块：

1. `eco_concil_runtime/kernel/governance/cli_approval_commands.py`
   - transition request/approve/reject。
   - skill approval request/approve/reject。
2. `eco_concil_runtime/kernel/planes/cli_query_commands.py`
   - analysis/council/reporting/control object query commands。
   - canonical contracts list。
3. `eco_concil_runtime/kernel/execution/cli_execution_commands.py`
   - gate/apply。
   - run/resume/restart governed execution。
   - supervisor/controller execution commands。
4. `eco_concil_runtime/kernel/reporting/cli_reporting_commands.py`
   - reporting exports。
   - show reporting state。
5. `eco_concil_runtime/kernel/archive/cli_archive_commands.py`
   - close round。
   - history bootstrap。
   - benchmark fixture/manifest/compare/replay。

命名说明：

1. 后续 CLI command handler 不再放入 `kernel/` 根目录，优先放入命令所属的浅层包。
2. CLI 执行链命名以 governed execution 为准，不再保留旧 phase 命令。
3. `cli_*_commands.py` 保持现有 `kernel/cli.py` 的命令语义，`kernel/cli.py` 仍作为入口 facade。

### 4.5 Council Objects

当前 public 入口：

1. `eco_concil_runtime/objects/council/__init__.py`
   - 继续导出现有 append/store/query API。

目标模块：

1. `eco_concil_runtime/objects/council/schema.py`
   - council object schema and DB connect。
2. `eco_concil_runtime/objects/council/payloads.py`
   - finding/evidence/discussion/review/proposal/readiness/decision trace payload normalization。
3. `eco_concil_runtime/objects/council/rows.py`
   - row conversion and row writes。
4. `eco_concil_runtime/objects/council/store.py`
   - append/store operations。
5. `eco_concil_runtime/objects/council/query.py`
   - query config and query surface。
6. `eco_concil_runtime/objects/council/decision_traces.py`
   - decision trace bundles and trace-specific helpers。

命名说明：

1. 保留 `council_object_*` 单数前缀用于 helpers。
2. 保留 `objects/council/__init__.py` plural facade，因为现有 public module 就是 plural。

### 4.6 Analysis Objects

当前 public 入口：

1. `eco_concil_runtime/objects/analysis/__init__.py`
   - 继续导出所有 existing normalized payload builders。

目标模块：

1. `eco_concil_runtime/objects/analysis/common.py`
   - common normalization helpers。
2. `eco_concil_runtime/objects/analysis/signals.py`
   - claim/observation candidates and scopes。
3. `eco_concil_runtime/objects/analysis/issues.py`
   - issue cluster、stance group、concern facet、actor profile。
4. `eco_concil_runtime/objects/analysis/relations.py`
   - formal-public link、diffusion edge、spatiotemporal relation cue、representation gap。
5. `eco_concil_runtime/objects/analysis/verification.py`
   - verifiability assessment、verification route、evidence citation type。

### 4.7 Canonical Contracts

当前 public 入口：

1. `eco_concil_runtime/contracts/__init__.py`
   - 保留 `canonical_contract`、`canonical_contracts_for_plane`、`validate_canonical_payload`。
   - 保留 constants re-export。

目标模块：

1. `eco_concil_runtime/contracts/types.py`
   - `CanonicalContract` dataclass。
   - registration helpers。
2. `eco_concil_runtime/contracts/signal.py`
   - signal contracts and signal metadata constants。
3. `eco_concil_runtime/contracts/analysis.py`
   - analysis object contracts。
4. `eco_concil_runtime/contracts/deliberation.py`
   - council/board/round/probe/readiness contracts。
5. `eco_concil_runtime/contracts/runtime.py`
   - transition request/approval, skill approval, controller/gate/supervisor/orchestration contracts。
6. `eco_concil_runtime/contracts/reporting.py`
   - report section, reporting handoff, council decision, expert report, final publication contracts。
7. `eco_concil_runtime/contracts/registry.py`
   - merged registry and lookup functions。

命名说明：

1. Use plane names exactly matching `PLANE_SIGNAL`、`PLANE_ANALYSIS`、`PLANE_DELIBERATION`、`PLANE_RUNTIME`、`PLANE_REPORTING`。
2. Registry remains central, definitions become plane-scoped.

### 4.8 Runtime Governance Modules

目标结构：

1. `kernel/governance/skill_approvals/__init__.py`
2. `kernel/governance/transition_requests/__init__.py`
3. `kernel/governance/admission_policy.py`
4. `kernel/operator/dead_letters.py`
5. `kernel/operator/runtime_health.py`
6. `kernel/operator/runbook.py`
7. `kernel/execution/attempts.py`
8. `kernel/execution/failures.py`
9. `kernel/execution/controller/__init__.py`
10. `kernel/execution/executor.py`

当前 public package 入口：

1. `kernel/governance/skill_approvals/__init__.py`
2. `kernel/governance/transition_requests/__init__.py`
3. `kernel/operator/operations.py`
4. `kernel/execution/executor.py`

### 4.9 Benchmark / Post-round

目标模块：

1. `kernel/archive/benchmark/__init__.py`
2. `kernel/archive/benchmark/common.py`
3. `kernel/archive/benchmark/manifest.py`
4. `kernel/archive/benchmark/compare.py`
5. `kernel/archive/benchmark/replay.py`
6. `kernel/archive/post_round/__init__.py`
7. `kernel/archive/post_round/common.py`
8. `kernel/archive/post_round/close.py`
9. `kernel/archive/post_round/history.py`

当前 public 入口：

1. `kernel/archive/benchmark/__init__.py`
2. `kernel/archive/post_round/__init__.py`

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
3. `deliberation_plane.py` 在 P1 后仍保留后续 store/load 业务函数与public 导出；这些业务函数已在 P2 继续拆出。

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
2. 保留 `optional_analysis/__init__.py` re-export。
3. 不改变 helper governance metadata。

验收：

1. `python3 tools/quality_gate.py test optional-guardrails relation-taxonomy`
2. `python3 -m unittest tests.test_spatiotemporal_relation_taxonomy`
3. `python3 tools/quality_gate.py full`

当前状态：已完成并完成包级收敛。`optional_analysis/support.py` 已承载共用 helper/signal/lineage 支撑逻辑；`optional_analysis/environment_evidence.py`、`optional_analysis/scope_review.py`、`optional_analysis/research_issues.py`、`optional_analysis/formal_public.py`、`optional_analysis/relations.py` 已按 helper family 承载实现；`optional_analysis/__init__.py` 为当前 public package API。

### P5：CLI 和 Operator Views

目标：

1. 抽 parser 与 command handlers。
2. 抽 `show_run_state` 到 `kernel/operator/run_state_view.py`。
3. 抽 operator views 到同一状态视图模块，避免为 view 逻辑继续增加细碎文件。
4. 保留 `kernel/cli.py main`。

验收：

1. CLI smoke tests 覆盖 `--help`、`show-run-state`、`show-schema-status`、`query-*`、approval commands。
2. `python3 tools/quality_gate.py test runtime-governance reporting schema-migration`
3. `python3 tools/quality_gate.py full`

当前状态：已完成第三块代码交付。`kernel/operator/run_state_view.py` 已承载运行状态与 operator view；`kernel/operator/cli_parser.py` 已承载 parser construction 与 shared arg helpers；`kernel/operator/cli_runtime_commands.py` 已承载 runtime command handlers；`kernel/cli.py` 保留 main 和 public import。

### P6：Council / Analysis Object Registries

目标：

1. 拆 `objects/council/__init__.py`。
2. 拆 `objects/analysis/__init__.py`。
3. 拆 `contracts/__init__.py` 为 plane-scoped definitions + registry。

当前状态：已完成。

当前结果：

1. `objects/analysis/__init__.py` 为 analysis object package API；`objects/analysis/common.py` 承载共用 constants/helper/provenance/evidence refs/confidence helpers；`signals.py`、`issues.py`、`verification.py`、`relations.py` 按 canonical analysis object family 承载 normalization。
2. `objects/council/__init__.py` 为 council object package API；schema/connect、payload normalization、row conversion/write、append/store、query surface 与 decision trace family 已分别落入 `objects/council/schema.py`、`payloads.py`、`rows.py`、`store.py`、`query.py`、`decision_traces.py`。
3. `contracts/__init__.py` 为 canonical contract package API；`contracts/types.py` 承载 dataclass、plane constants 与 `_contract` helper；signal/analysis/deliberation/runtime/reporting contract definitions 已按 plane 拆分；`contracts/registry.py` 继续提供 merged registry、lookup、plane query 与 payload validation。
4. 本阶段不改变 canonical object 字段、contract schema_version、council DB schema、append/store/query result shape。

验收：

1. `python3 -m unittest tests.test_canonical_contracts tests.test_council_query_surface tests.test_reporting_query_surface`
2. `python3 tools/quality_gate.py test relation-taxonomy reporting`
3. `python3 tools/quality_gate.py full`

本阶段实测：

1. `python3 tools/quality_gate.py syntax`，254 个 Python 文件通过。
2. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_canonical_contracts tests.test_council_query_surface tests.test_reporting_query_surface tests.test_spatiotemporal_relation_taxonomy tests.test_optional_analysis_guardrails`，47 tests 通过。
3. `python3 tools/quality_gate.py test relation-taxonomy reporting module-decomposition optional-guardrails`，57 tests 通过。
4. `python3 tools/quality_gate.py full`，268 tests 通过。

### P7：Runtime Governance 支撑模块

目标：

1. 拆 skill approval 与 transition request store/payload。
2. 拆 operations/executor。
3. 保留原模块 facade。

当前状态：第二轮已完成，controller/executor 主循环未继续硬拆。

当前结果：

1. `transition_requests/__init__.py` 为 package public API；common、payloads、rows、store 分别承载 transition kind/spec、canonical payload、row conversion/write、store/load/approve/reject/commit/resolve。
2. `skill_approvals/__init__.py` 为 package public API；common、payloads、rows、store 分别承载 approval constants/policy/id、canonical payload、row conversion/write、store/load/approve/reject/consume/resolve。
3. `operator/operations.py` 已收缩为 facade；admission policy、dead letters、runtime health、operator runbook 和 common helper 已分离。
4. `execution/executor.py` 已抽出 common、command hints 与 failure helpers；`run_skill` 主流程仍保留在 executor 入口，后续若继续拆，应以 attempt loop、receipt/postflight、skill approval consumption 为边界，不按行数拆。
5. `operator/surfaces/__init__.py` 为 runtime surface package public API；common/reporting/investigation/execution/publication wrappers 已按 operator-facing surface 边界拆分。
6. `planes/signal/__init__.py` 为 signal plane package public API；schema、metadata enrichment、store/index、finalize streaming、evidence helper 已按 signal plane pipeline 边界拆分。
7. `execution/controller/__init__.py` 为 controller package public API；artifact/state persistence、planning adapters、transition-executor planning 与 stage approval guard 已分离；主执行循环仍留在 package 入口。
8. 本阶段不改变 approval status、transition kind、runtime admission decision、dead letter payload、health surface、runbook markdown、signal normalized row/index shape 或 skill execution result shape。

验收：

1. `python3 tools/quality_gate.py test runtime-governance`
2. `python3 tools/quality_gate.py test runtime-governance reporting`
3. `python3 tools/quality_gate.py full`

本阶段当前实测：

1. `python3 tools/quality_gate.py syntax`，284 个 Python 文件通过。
2. `python3 -m unittest tests.test_module_decomposition_contracts`，7 tests 通过。
3. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting`，86 tests 通过。
4. `python3 tools/quality_gate.py full`，268 tests 通过。
5. `python3 -m unittest tests.test_optional_analysis_guardrails tests.test_spatiotemporal_relation_taxonomy`，27 tests 通过。
6. `python3 -m unittest tests.test_module_decomposition_contracts tests.test_runtime_kernel`，54 tests 通过。
7. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting relation-taxonomy optional-guardrails`，113 tests 通过。
8. `python3 tools/quality_gate.py full`，268 tests 通过；P7.5 package encapsulation 后耗时约 8 分 58 秒。

### P7.5：Package Encapsulation / File Surface Consolidation

目标：

1. 收敛顶层 `eco_council_runtime/` 平铺文件。
2. 将 canonical contracts、analysis objects、council objects、optional analysis helpers 转为包级 public API。
3. 不保留旧顶层平铺 import path，以最新 package path 为准。

当前状态：已完成。

当前结果：

1. `eco_council_runtime/contracts/` 承载 canonical contract package。
   - `contracts/__init__.py` 为 public API。
   - `types.py`、`signal.py`、`analysis.py`、`deliberation.py`、`runtime.py`、`reporting.py`、`registry.py` 按 contract plane 与 registry 边界组织。
2. `eco_council_runtime/objects/analysis/` 承载 analysis object package。
   - `objects/analysis/__init__.py` 为 public API。
   - `common.py`、`signals.py`、`issues.py`、`verification.py`、`relations.py` 按 canonical object family 组织。
3. `eco_council_runtime/objects/council/` 承载 council object package。
   - `objects/council/__init__.py` 为 public API。
   - `schema.py`、`payloads.py`、`rows.py`、`store.py`、`query.py`、`decision_traces.py` 按 persistence/query/trace 边界组织。
4. `eco_council_runtime/optional_analysis/` 承载 optional analysis package。
   - `optional_analysis/__init__.py` 为 public API。
   - `support.py`、`environment_evidence.py`、`scope_review.py`、`research_issues.py`、`formal_public.py`、`relations.py` 按 helper family 组织。
5. 顶层 `eco_council_runtime/` `.py` 文件从 39 个降到 11 个。

验收：

1. `python3 tools/quality_gate.py syntax`
2. `python3 -m unittest tests.test_module_decomposition_contracts`
3. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting relation-taxonomy optional-guardrails`
4. `python3 tools/quality_gate.py full`

本阶段实测：

1. `python3 tools/quality_gate.py syntax`，284 个 Python 文件通过。
2. `python3 -m unittest tests.test_module_decomposition_contracts`，7 tests 通过。
3. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting relation-taxonomy optional-guardrails`，113 tests 通过。
4. `python3 tools/quality_gate.py full`，268 tests 通过。

### P7.6：Kernel Package Encapsulation 第一轮

目标：

1. 收敛 `kernel/operator/` 内 runtime surface 文件族。
2. 收敛 `kernel/planes/` 内 signal plane 文件族。
3. 收敛 `kernel/execution/` 内 controller support 文件族。
4. 不改变 runtime 行为、CLI 命令、signal DB schema、controller result shape 或 reporting surface shape。

当前状态：已完成第一轮。

当前结果：

1. `eco_council_runtime/kernel/operator/surfaces/` 承载 runtime surface package。
   - `surfaces/__init__.py` 为 public API。
   - `common.py`、`reporting.py`、`investigation.py`、`execution.py`、`publication.py` 按 surface family 组织。
2. `eco_council_runtime/kernel/planes/signal/` 承载 signal plane package。
   - `signal/__init__.py` 为 public API。
   - `common.py`、`schema.py`、`metadata.py`、`store.py`、`finalize.py`、`evidence.py` 按 signal normalization/evidence pipeline 组织。
3. `eco_council_runtime/kernel/execution/controller/` 承载 governed execution controller package。
   - `controller/__init__.py` 为 public API。
   - `artifacts.py`、`planning_adapters.py`、`transition_planning.py` 按 controller support 边界组织。

验收：

1. `python3 tools/quality_gate.py syntax`
2. `python3 -m unittest tests.test_module_decomposition_contracts`
3. `python3 -m unittest tests.test_operator_surfaces tests.test_spatiotemporal_relation_taxonomy tests.test_optional_analysis_guardrails`
4. `python3 -m unittest tests.test_runtime_kernel`
5. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting relation-taxonomy optional-guardrails`
6. `python3 tools/quality_gate.py full`

本阶段实测：

1. `python3 tools/quality_gate.py syntax`，284 个 Python 文件通过。
2. `python3 -m unittest tests.test_module_decomposition_contracts`，7 tests 通过。
3. `python3 -m unittest tests.test_operator_surfaces tests.test_spatiotemporal_relation_taxonomy tests.test_optional_analysis_guardrails`，31 tests 通过。
4. `python3 -m unittest tests.test_runtime_kernel`，47 tests 通过。
5. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting relation-taxonomy optional-guardrails`，113 tests 通过。
6. `python3 tools/quality_gate.py full`，268 tests 通过；P7.6 kernel package encapsulation 第一轮后耗时约 8 分 23 秒。

### P7.7：Governance Package Encapsulation

目标：

1. 收敛 `kernel/governance/` 内 transition request 文件族。
2. 收敛 `kernel/governance/` 内 skill approval 文件族。
3. 收敛 agent entry profile/handoff 文件族。
4. 收敛 fallback agenda/context/contract/policy 文件族。
5. 不保留旧 helper module import path；以最新 package 命名为准。

当前状态：已完成并通过 full gate 复验。

当前结果：

1. `eco_council_runtime/kernel/governance/transition_requests/` 承载 transition request package。
   - `__init__.py` 为 public API。
   - `common.py`、`payloads.py`、`rows.py`、`store.py` 按 transition request pipeline 组织。
2. `eco_council_runtime/kernel/governance/skill_approvals/` 承载 skill approval package。
   - `__init__.py` 为 public API。
   - `common.py`、`payloads.py`、`rows.py`、`store.py` 按 approval pipeline 组织。
3. `eco_council_runtime/kernel/governance/agent_entry/` 承载 agent entry package。
   - `__init__.py` 为 public API。
   - `handoff.py`、`profile.py` 按 handoff command builders 与 profile builders 组织。
4. `eco_council_runtime/kernel/governance/fallback/` 承载 fallback package。
   - `common.py`、`context.py`、`contracts.py`、`agenda.py`、`agenda_profile.py`、`policy.py` 按 fallback support boundary 组织。

验收：

1. `python3 tools/quality_gate.py syntax`
2. Governance package smoke import。
3. `python3 -m unittest tests.test_module_decomposition_contracts`
4. `python3 -m unittest tests.test_skill_approval_workflow tests.test_agent_entry_gate tests.test_runtime_kernel`
5. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting`
6. `python3 tools/quality_gate.py full`

本阶段实测：

1. `python3 tools/quality_gate.py syntax`，285 个 Python 文件通过。
2. Governance package smoke import 通过。
3. `python3 -m unittest tests.test_module_decomposition_contracts`，7 tests 通过。
4. `python3 -m unittest tests.test_skill_approval_workflow tests.test_agent_entry_gate tests.test_runtime_kernel`，62 tests 通过。
5. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting`，86 tests 通过。
6. `python3 tools/quality_gate.py full`，268 tests 通过；P7.7 governance package encapsulation 后耗时约 7 分 58 秒。

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
3. `optional_analysis/__init__.py` 低于 400 行，且只包含 facade/re-export。
4. `kernel/cli.py` 低于 800 行，主要保留 `main`、`build_parser` glue。
5. 任一生产 Python 文件超过 1500 行时，必须有明确保留理由或后续拆分 issue。
6. 全部现有 targeted gates 与 full gate 通过。
7. 文档列出的最新 public import、CLI command、skill id、canonical contract 和 DB schema 均保持稳定。
