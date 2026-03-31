# Runtime Source-Queue 路线全量开发计划

## 1. 路线定位

这条路线是第二编排面，不是主智能体面。

它解决的是：

1. 固定场景执行
2. benchmark / replay / nightly run
3. 治理约束
4. 输入冻结
5. ledger / gate / archive
6. 可复现的批处理调查

它不解决的是：

1. 开放式调查推理
2. 多 agent 辩论与挑战
3. 动态工具组合策略
4. 主动探索式取证

这些属于 OpenClaw multi-agent 路线。

## 2. 当前代码状态

### 2.1 已经可运行的主链

当前 runtime route 已具备一条闭环主链：

1. `eco-scaffold-mission-run`
   - 写入 `mission.json`
   - 写入 `round_tasks`
   - 初始化 board
2. `eco-prepare-round`
   - 根据 mission / tasks / source selection 生成 role 级治理对象
   - 写入 `source_selection_<role>_<round_id>.json`
   - 写入 `fetch_plan_<round_id>.json`
3. `eco-import-fetch-execution`
   - 执行 `import`
   - 执行 `detached-fetch`
   - 调用 normalizer
4. normalize / analysis chain
   - signal plane、claim、observation、scope、coverage
5. `eco-plan-round-orchestration`
   - 生成 phase-2 orchestration plan
6. `controller`
   - 执行 planner-backed phase-2 queue
7. `supervisor`
   - 物化 promotion / reporting 准入状态
8. archive / history
   - case library
   - signal corpus
   - history context

### 2.2 已经落实的关键治理能力

当前已完成：

1. `source-selection governance`
2. `family_plans / layer_plans / source_decisions`
3. `prior-round family memory`
4. `fetch plan input snapshot`
5. `drift detection`
6. `detached fetch execution policy`
7. `declared_side_effects / requested_side_effect_approvals`
8. `runtime admission / dead-letter / operator surface`
9. `registry + source_queue_profile`
10. `artifact_imports + source_requests` 双入口接入
11. `source family / layer / anchor` 编排
12. `l2 source depends_on + anchor_artifact_paths` 自动生成
13. `missing normalizer -> raw-only ingest` 降级保底

截至 `2026-03-31`，已验证：

1. `python3 -m unittest discover -s tests` 共 `75` 项测试通过
2. 新迁移 source 已能进入 `SOURCE_CATALOG`
3. `youtube-video-search -> youtube-comments-fetch` 链路可执行
4. `regulationsgov-comments-fetch -> regulationsgov-comment-detail-fetch` 链路可执行

### 2.2.1 当前已接入的 source 组

公共舆情面：

1. `bluesky-cascade-fetch`
2. `gdelt-doc-search`
3. `gdelt-events-fetch`
4. `gdelt-mentions-fetch`
5. `gdelt-gkg-fetch`
6. `youtube-video-search`
7. `youtube-comments-fetch`
8. `regulationsgov-comments-fetch`
9. `regulationsgov-comment-detail-fetch`

物理环境面：

1. `airnow-hourly-obs-fetch`
2. `openaq-data-fetch`
3. `open-meteo-historical-fetch`
4. `open-meteo-air-quality-fetch`
5. `open-meteo-flood-fetch`
6. `usgs-water-iv-fetch`
7. `nasa-firms-fire-fetch`

其中：

1. `youtube-comments-fetch` 依赖 `youtube-video-search`
2. `regulationsgov-comment-detail-fetch` 依赖 `regulationsgov-comments-fetch`

### 2.3 R6 之后剩余的维护项

当前缺的已经不是新的 runtime 大阶段，而是少量生产维护项：

1. detached fetch 仍缺少更正式的 credential / env 注入策略
2. detached fetch artifact 还缺少 quarantine / provenance 扩展
3. `gdelt-events / gdelt-mentions / gdelt-gkg` 目前是 manifest 级 normalize，不是 zip 内全表行级 normalize
4. history retrieval 仍是辅助检索层，不是更强的调查型检索层
5. replay / benchmark / nightly 的 corpus 编排仍可继续完善，但已经不再是阻塞主链的缺口
6. 新增 normalizer 目录当前主要是 runtime 可调用脚本，尚未全部补齐 `SKILL.md / agents/openai.yaml`

## 3. 路线完成定义

只有下面五个条件都满足，才能说 runtime route 完成：

1. 固定场景能稳定从 mission 跑到 archive
2. queue governance、drift detection、execution policy 都能阻断违规执行
3. replay / benchmark / nightly run 有统一入口
4. 所有关键状态物都有 ledger、gate、archive 对应物
5. route 明确保持“治理执行面”，不再重新吞回业务推理

截至 `2026-03-29`，这条路线的主完成条件已经基本满足；后续工作应视为维护与交接，而不是继续扩张新的 runtime 阶段。

## 4. 开发原则

1. 只增强 runtime 的治理、执行、存储、回放能力。
2. 不把调查判断塞回 controller。
3. 不再引入第二套业务逻辑实现。
4. 能做成 skill 的能力，优先做成 skill。
5. runtime 内核只承载：
   - contract
   - execution
   - gate
   - ledger
   - registry
   - cursor / manifest

## 5. 全量开发阶段

### 阶段 R0: 基线锁定

状态：`已完成`

目标：

1. 保证现有 preview 主链始终可运行
2. 让 regression 成为后续所有改动的前置条件

当前已有产物：

1. `kernel/cli.py`
2. `kernel/controller.py`
3. `kernel/supervisor.py`
4. `tests/test_runtime_kernel.py`
5. `tests/test_orchestration_ingress_workflow.py`
6. `tests/test_reporting_workflow.py`

完成判据：

1. 全量测试持续通过
2. ingress 到 supervisor 的链路不回退

### 阶段 R1: source governance 收口

状态：`已完成`

目标：

1. 让 source selection 成为完整治理对象
2. 把 prior-round family memory 收进 active kernel

当前已有产物：

1. `kernel/source_queue_contract.py`
2. `kernel/source_queue_selection.py`
3. `kernel/source_queue_history.py`
4. `tests/test_source_queue_governance.py`
5. `tests/test_source_queue_family_memory.py`

后续只保留维护性工作：

1. 收紧 schema
2. 补更细的 negative tests
3. 补 role-specific override 规则

### 阶段 R2: fetch plan 与执行边界收口

状态：`已完成并完成第一轮 migrated source 接入`

目标：

1. 让 `fetch_plan` 成为可审计 step graph
2. 让 detached fetch 具备独立治理边界

当前已有产物：

1. `kernel/source_queue_planner.py`
2. `kernel/source_queue_execution.py`
3. `skills/eco-prepare-round`
4. `skills/eco-import-fetch-execution`
5. `tests/test_source_queue_rebuild.py`
6. `kernel/signal_plane_normalizer.py`
7. `tests/test_migrated_source_runtime_integration.py`

下一步要补的不是更多字段，而是更硬的执行控制：

1. credential / env 注入策略
2. detached fetch request contract 的 provider 级标准化
3. detached fetch artifact quarantine / provenance 规则
4. 更多 admission negative tests 与 provider fixture coverage
5. GDELT 全量导出类 source 的深度解包与行级 normalize

完成判据：

1. 所有 fetch step 都能在 plan 中声明执行政策
2. 所有执行失败都有结构化状态和重试语义
3. 所有 raw artifacts 都有统一落盘与引用方式
4. l2 source 可以通过 anchor 依赖同轮或前轮上游 artifact
5. 当 normalizer 尚未实现时，runtime 至少能 raw-only 保留证据，不因单源缺口打断整轮

### 阶段 R3: phase-2 控制面硬化

状态：`已完成`

目标：

1. 让 planner / controller / supervisor 的边界固定
2. 让 queue mode 拥有稳定的 phase-2 运行面

当前已有产物：

1. `eco-plan-round-orchestration`
2. `controller.py`
3. `supervisor.py`
4. `phase2_contract.py`
5. `tests/test_orchestration_planner_workflow.py`
6. `tests/test_supervisor_simulation_regression.py`
7. `tests/test_runtime_kernel.py`

本轮已交付：

1. 为 controller 引入明确的 phase-2 stage contract，并对 planner queue 做顺序和 skill 绑定校验
2. 将 `round_controller_<round_id>.json` 升级为增量写入的控制面状态物，而不是仅最终摘要
3. 为 controller 增加失败快照、resume / restart 语义，以及基于已完成 stage 的跳过恢复
4. 为 supervisor 增加更清晰的 promote / hold / failed 终态分类和 operator action 提示
5. 将 `show-run-state` 提升为 round 运维入口，并补充 `resume-phase2-round` / `restart-phase2-round`
6. 补齐 R3 回归测试；截至 `2026-03-29`，全量 `56` 项测试通过

完成判据：

1. phase-2 每一步都有明确输入物、输出物、失败语义
2. resume 一次中断 round 不需要人工猜测状态
3. operator 可以仅通过 runtime artifacts 判断 round posture

### 阶段 R4: archive / history 纳入正式 runtime 流程

状态：`已完成`

目标：

1. 让 archive / history 不再是“有技能但靠手工调用”
2. 让 post-round closure 成为 runtime route 的标准尾部

当前已有产物：

1. `eco-archive-case-library`
2. `eco-archive-signal-corpus`
3. `eco-query-case-library`
4. `eco-query-signal-corpus`
5. `eco-materialize-history-context`
6. `kernel/post_round.py`
7. `kernel/cli.py`
8. `tests/test_archive_history_workflow.py`
9. `tests/test_runtime_kernel.py`

本轮已交付：

1. 增加 `close-round`，将 archive write 固定为 runtime 的标准 post-round closeout 入口
2. 增加 `bootstrap-history-context`，将 history context 正式纳入 runtime 的 next-round bootstrap 路径
3. 为 round close 增加显式 state artifact、ledger event、step 状态与失败语义
4. 明确 archive failure policy：默认 `block`，并支持 `warn` 退化关闭
5. 明确 archive compaction policy：以 `replace-per-run-snapshot` 作为 nightly / benchmark 的稳定替换策略
6. 将 `show-run-state` 扩展到 post-round 状态面，可直接查看 round close 与 history bootstrap 结果
7. 补齐 R4 workflow / kernel 回归；截至 `2026-03-29`，全量 `60` 项测试通过

完成判据：

1. round close 时 archive 行为稳定、可预期
2. next round 可以稳定读取历史而无需人工拼装

### 阶段 R5: replay / benchmark / nightly tooling

状态：`已完成`

目标：

1. 让 runtime route 真正承担第二编排面职责
2. 让它能用于回放、基准对比、固定数据集验证

当前已有产物：

1. `kernel/benchmark.py`
2. `kernel/cli.py`
3. `tests/test_benchmark_replay_workflow.py`
4. `tests/test_runtime_kernel.py`

本轮已交付：

1. benchmark run manifest
2. scenario fixture contract
3. replay command
4. stable output comparison
5. per-skill / per-round timing and failure summary
6. `show-run-state` benchmark state 面板
7. 基于 runtime artifacts 的 compare / replay / regression report 落盘
8. 补齐 R5 workflow / kernel 回归；截至 `2026-03-29`，全量 `63` 项测试通过

当前实现形态：

1. `materialize-benchmark-manifest`
2. `materialize-scenario-fixture`
3. `compare-benchmark-manifests`
4. `replay-runtime-scenario`
5. `scenario_fixture_<round_id>.json`
6. `benchmark_manifest_<round_id>.json`
7. `benchmark_compare_<round_id>.json`
8. `replay_report_<round_id>.json`

完成判据：

1. 能基于同一 scenario 重复跑出稳定 artifacts
2. benchmark 结果能自动比较并暴露回归

### 阶段 R6: 生产化执行边界

状态：`已完成`

目标：

1. 把当前 preview route 提升为可长期运行的受控系统

当前已有产物：

1. `kernel/operations.py`
2. `kernel/executor.py`
3. `kernel/source_queue_execution.py`
4. `kernel/cli.py`
5. `skills/eco-import-fetch-execution`
6. `tests/test_runtime_kernel.py`
7. `tests/test_source_queue_rebuild.py`

本轮已交付：

1. permission model
2. sandbox boundary
3. approval / admission policy
4. rollback / retry / dead-letter policy
5. operator runbook
6. alert / observability surface
7. `run-skill` runtime admission、dead-letter、health / runbook surface 自动接线
8. detached fetch admission、ledger event、dead-letter、失败状态物落盘
9. `show-run-state` 扩展为包含 operations control plane 的运维视图
10. 新增 `materialize-admission-policy` / `materialize-runtime-health` / `materialize-operator-runbook` / `show-dead-letters`
11. detached fetch side-effect contract 拆分为 `declared_side_effects / requested_side_effect_approvals`
12. 默认 sandbox root 收紧为“读可到 workspace，写只限 run_dir / archives”
13. 补齐 R6 回归；截至 `2026-03-29`，全量 `69` 项测试通过

完成判据：

1. 任何 step 的副作用边界都能提前判断
2. 任何失败类型都有确定处理策略
3. operator 不依赖读代码就能处理常见故障

## 6. 当前维护顺序

接下来不再继续新增 runtime 大阶段，而按下面顺序做维护收口：

1. 完成 `R6.1`：credential / env policy、artifact quarantine / provenance、detached fetch provider contract 细化
2. 只在需要 benchmark / nightly corpus 时继续补 replay fixture 与 archive pinning
3. runtime 收口后，把主要研发重心切换到 agent 松绑与 OpenClaw 主调查路线

不要再把新的业务推理能力塞进 runtime。

## 7. 当前建议的具体 backlog

### Backlog A: phase-2 maintenance（R3 已交付）

1. 如果 planner queue schema 继续扩展，保持 `phase2_contract.py` 与 planner skill 同步
2. 继续补 controller / supervisor 的 negative tests，尤其是更复杂的 gate / resume 失败场景
3. 保持 operator hints 与真实 CLI 子命令一致，避免文档与运行面脱节

### Backlog B: detached fetch 生产化

1. 把 detached fetch 的执行输入从 ad hoc argv 继续收敛成 provider-aware request contract
2. 增加 credential mount / env policy
3. 增加 quarantine / checksum / provenance 扩展
4. 保持 `declared_side_effects / requested_side_effect_approvals` 语义稳定，不再退回单字段混用

### Backlog C: archive / history maintenance（R4 已交付）

1. 继续增强 history retrieval 的检索策略，但保持它是分析辅助层而不是主调查面
2. 如果后续需要多轮同 run 复用历史，可再补 case_id / round_id 级索引拆分
3. 保持 archive schema 演进与 close-round state artifact 同步

### Backlog D: benchmark / replay maintenance（R5 已交付）

1. 如果要做 nightly corpus，继续补 round template 与 regression corpus 编排
2. 如果要跨机器复放，补更强的 fixture dataset pinning 和 archive snapshot pinning
3. 如果要做趋势分析，再补 diffable summary export 与 timing trend report

## 8. 路线风险

当前这条路线最大的风险不是“代码太少”，而是“职责滑坡”：

1. controller 再次吞回业务推理
2. queue 为了兼容 agent 场景而变成第二套 agent 系统
3. history retrieval 被误当成强调查能力本身
4. 生产化工作过早开始，导致结构僵化

必须持续抵抗这四个风险。

## 9. 路线完成后的定位

这条路线完成后，它应该是：

1. 可靠的 benchmark / replay / governed batch surface
2. OpenClaw agent 路线的受控执行后盾
3. 审计、存储、回放和固定流程验证的基础设施

它不应该成为：

1. 主调查智能体
2. controller-heavy 的第二套业务系统
3. 依赖规则堆叠取代 agent 判断的“伪智能”路线
