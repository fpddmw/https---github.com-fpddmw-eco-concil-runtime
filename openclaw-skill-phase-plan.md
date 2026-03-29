# OpenClaw Skill 分阶段交付计划

## 1. 规划依据

本计划以当前的 [openclaw-first-refactor-blueprint.md](openclaw-first-refactor-blueprint.md) 为唯一架构基线，执行上遵守以下约束：

- `skills/` 是主要业务能力面，所有新增能力都以 atomic skill 方式交付
- `eco-concil-runtime/` 仅保留最小运行时内核，不承担新业务实现
- 数据主链继续沿用 `raw artifact -> source normalize -> signal plane -> candidate/evidence -> board -> promote`
- 所有新增 skill 必须保持自包含，不允许重新引入根级共享 runtime 包装层

文档分工补充：

- 本文件只描述阶段性交付面与下一步开发顺序。
- 当前完成度见 [openclaw-collaboration-status.md](openclaw-collaboration-status.md)。
- 到生产环境的开发路线见 [openclaw-production-development-plan.md](openclaw-production-development-plan.md)。

## 2. 当前完成面

当前已交付的 skill 面可分为数层：

### 2.1 source-specific normalize

- `eco-normalize-gdelt-doc-public-signals`
- `eco-normalize-youtube-video-public-signals`
- `eco-normalize-bluesky-cascade-public-signals`
- `eco-normalize-openaq-observation-signals`
- `eco-normalize-airnow-observation-signals`
- `eco-normalize-open-meteo-historical-signals`

### 2.2 shared query / lookup

- `eco-query-public-signals`
- `eco-query-environment-signals`
- `eco-lookup-normalized-signal`
- `eco-lookup-raw-record`

### 2.3 candidate / audit 雏形

- `eco-extract-claim-candidates`
- `eco-extract-observation-candidates`
- `eco-build-normalization-audit`

### 2.4 evidence 中间层

- `eco-cluster-claim-candidates`
- `eco-merge-observation-candidates`
- `eco-link-claims-to-observations`
- `eco-derive-claim-scope`
- `eco-derive-observation-scope`
- `eco-score-evidence-coverage`

### 2.5 board 第一批

- `eco-read-board-delta`
- `eco-post-board-note`
- `eco-open-challenge-ticket`
- `eco-update-hypothesis-status`

### 2.6 board 第二批

- `eco-claim-board-task`
- `eco-close-challenge-ticket`
- `eco-summarize-board-state`
- `eco-materialize-board-brief`

### 2.7 investigation / promotion 层

- `eco-propose-next-actions`
- `eco-open-falsification-probe`
- `eco-summarize-round-readiness`
- `eco-promote-evidence-basis`

### 2.8 最小 runtime kernel

- 新增 `eco-concil-runtime/` 最小内核包
- 已落地 `run_manifest.json`、`round_cursor.json`、`skill_registry.json`、`audit_ledger.jsonl`、`receipts/`
- 已落地 kernel CLI：`eco-concil-runtime/scripts/eco_runtime_kernel.py`
- 已补齐第 2 阶段产物：`promotion_gate_<round_id>.json`、`round_controller_<round_id>.json`、`supervisor_state_<round_id>.json`
- 已补齐 phase-2 CLI 入口：`apply-promotion-gate`、`run-phase2-round`、`supervise-round`

这说明当前主链已经不只停留在“候选对象生成”，而是已经延伸到 evidence bridge、board organize / brief、investigation / readiness / promotion，以及最小 runtime kernel 的第 2 阶段。

### 2.9 reporting / decision 第一批

- `eco-materialize-reporting-handoff`
- `eco-draft-council-decision`

这说明当前主链已经开始消费 `promoted_evidence_basis_<round_id>.json`，而不是把下游 reporting / decision 永久留在 legacy runtime 里。

### 2.10 orchestration / planning

- `eco-plan-round-orchestration`

这说明当前 phase-2 controller 已经不再只依赖固定硬编码队列，而是先落出 `orchestration_plan_<round_id>.json` 再执行 controller queue。

## 3. 分阶段目标

## Phase A：收紧现有 contract

目标：统一现有 skill 的返回契约、receipt 语义和 handoff 风格。

工作内容：

1. 统一 `status/summary/receipt_id/batch_id/artifact_refs/canonical_ids/warnings/board_handoff` 返回字段。
2. 统一 `board_handoff` 中的 `candidate_ids/evidence_refs/gap_hints/challenge_hints/suggested_next_skills`。
3. 补齐空输入、空结果、路径覆盖等专项测试。

说明：

- 这一阶段是持续性收口，不阻塞后续批次推进。

## Phase B：补齐 evidence 中间层

目标：在 claim / observation candidates 与未来 board / investigation skill 之间补上一层稳定的分析对象。

### B1：当前对话交付批次

本次直接交付以下 3 个 atomic skill：

1. `eco-cluster-claim-candidates`
   - 输入：`claim_candidates_<round_id>.json`
   - 输出：`claim_candidate_clusters_<round_id>.json`
   - 作用：将重复叙事或高度相近的 claim candidate 聚成 board 可审阅的 cluster

2. `eco-merge-observation-candidates`
   - 输入：`observation_candidates_<round_id>.json`
   - 输出：`merged_observation_candidates_<round_id>.json`
   - 作用：将时间、空间、metric 相近的 observation candidate 归并为更稳定的 observation group

3. `eco-link-claims-to-observations`
   - 输入：claim candidates / claim clusters 与 observation candidates / merged observations
   - 输出：`claim_observation_links_<round_id>.json`
   - 作用：给出 support / contradiction / contextual 级别的候选链接，为 board 和 challenge 提供中间证据对象

### B2：当前对话已完成

本轮已补齐：

1. `eco-derive-claim-scope`
2. `eco-derive-observation-scope`
3. `eco-score-evidence-coverage`

这三项已经把当前较弱的空间/范围语义和 claim-level coverage summary 补齐。

## Phase C：board 操作层

目标：把现在仅存在于 `board_handoff` 里的协作建议真正写入 board 可消费的工作态对象。

### C1：当前对话交付批次

本轮直接交付以下 4 个 skill：

1. `eco-read-board-delta`
2. `eco-post-board-note`
3. `eco-open-challenge-ticket`
4. `eco-update-hypothesis-status`

这些 skill 的共同目标是：先用一个最小可运行的 `investigation_board.json` 作为 board artifact，把 note、hypothesis、challenge ticket 和 event cursor 这些最基础的协作对象跑通。

### C2：当前对话交付批次

本轮继续补齐：

1. `eco-claim-board-task`
2. `eco-close-challenge-ticket`
3. `eco-summarize-board-state`
4. `eco-materialize-board-brief`

这一小批已经把 board 从“记录工作态”推进到“整理工作态”，现在 board 不只可写入 note / hypothesis / challenge，也可以显式声明 follow-up task、关闭 challenge、沉淀 summary，并产出 board brief。

## Phase D：investigation / promotion 层

目标：让多 agent 不只会提取和链接，还能推进 probe、challenge 与正式冻结。

说明：这部分不是从零开始。旧的 abandoned runtime 中已经存在 investigation / readiness / promotion 原型，但它们仍然是 runtime-first 形态，尚未被重铸为当前仓库的 atomic skill 面。对应线索主要在：

- `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/investigation/actions.py`
- `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/reporting/readiness.py`
- `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/reporting/promotion.py`

因此，Phase D 的工作重点不是“发明全新概念”，而是把这些旧 runtime 原型重新切成 skill-first 交付面，并只把真正需要的调度与状态保持收回最小 runtime 内核。

### D1：当前对话已完成

1. `eco-propose-next-actions`
   - 输入：board summary / board brief + evidence coverage + challenge state
   - 输出：`next_actions_<round_id>.json`
   - 作用：把当前 board working-state 转成可执行的 next-action queue

2. `eco-open-falsification-probe`
   - 输入：challenge ticket / hypothesis / evidence refs
   - 输出：`falsification_probes_<round_id>.json`
   - 作用：把“需要继续挑战”落成显式 probe 对象，而不是继续停留在 note 或 ticket 层

### D2：当前对话已完成

3. `eco-summarize-round-readiness`
   - 输入：board brief + next action queue + evidence coverage
   - 输出：`round_readiness_<round_id>.json`
   - 作用：判断当前轮次是 ready、needs-more-data 还是 blocked

4. `eco-promote-evidence-basis`
   - 输入：round readiness + board brief + selected evidence refs
   - 输出：`promoted_evidence_basis_<round_id>.json`
   - 作用：把 working-state 冻结成可被后续报告与 decision 层消费的正式 basis artifact

## Phase E：reporting / decision 层

目标：把 promotion 后的 basis artifact 接成可被下游消费的 reporting / decision 对象，而不是继续依赖 legacy runtime 的大 reporting 模块。

### E1：当前对话已完成

1. `eco-materialize-reporting-handoff`
   - 输入：promotion basis + readiness + board brief + supervisor state
   - 输出：`reporting_handoff_<round_id>.json`
   - 作用：把 promotion 阶段的分散 artifact 整成一个紧凑、可审计、可下游消费的 reporting handoff

2. `eco-draft-council-decision`
   - 输入：reporting handoff + promotion basis
   - 输出：`council_decision_draft_<round_id>.json`
   - 作用：把当前轮次显式落成 `finalize` 或 `continue` 的决策草案

### E2：当前对话已完成

3. `eco-draft-expert-report`
   - 输入：reporting handoff + council decision draft + board brief
   - 输出：`expert_report_draft_<role>_<round_id>.json`
   - 作用：把 reporting handoff 落成 role-specific expert report draft，并保持 reporting 语义留在 skill 面

4. `eco-publish-expert-report`
   - 输入：expert report draft
   - 输出：`expert_report_<role>_<round_id>.json`
   - 作用：把 role-specific draft 提升为 canonical expert report，并显式加入 overwrite guard

5. `eco-publish-council-decision`
   - 输入：council decision draft + canonical expert reports
   - 输出：`council_decision_<round_id>.json`
   - 作用：把 decision draft 提升为 canonical decision，并在 ready 路径上要求 canonical expert report 先存在

### E3：当前对话已完成

6. `eco-materialize-final-publication`
   - 输入：`reporting_handoff_<round_id>.json`、`expert_report_<role>_<round_id>.json`、`council_decision_<round_id>.json`、`promoted_evidence_basis_<round_id>.json`、`supervisor_state_<round_id>.json`
   - 输出：`final_publication_<round_id>.json`
   - 作用：把 canonical report / decision 收敛成一个最终发布对象，固定 publication summary、published sections、upstream audit refs、release posture 与 operator review hints

7. final publication regression
   - 目标：覆盖 promoted / withheld 两条路径、缺失 canonical inputs 的阻断行为，以及 publication artifact 对上游 audit refs 的绑定完整性

## Phase F：control-plane hardening

目标：把当前 skill-first deterministic pipeline 补成更可信的单机协作与审计底座，同时明确它还不是 board-driven orchestration runtime。

### F1：当前对话已完成

1. board 单机并发写入安全
   - `eco-post-board-note`、`eco-update-hypothesis-status`、`eco-open-challenge-ticket`、`eco-claim-board-task`、`eco-close-challenge-ticket` 现在统一使用 filesystem lock + atomic replace + `board_revision`
   - 作用：避免同机多进程写 `investigation_board.json` 时静默丢更新

2. runtime 审计链增强
   - runtime event id 已从低熵时间片拼接改成低碰撞事件标识
   - ledger 现在记录 `skill_args`、命令快照、registry entry、声明式 read/write contract、解析后的路径、输入哈希与 payload 哈希

3. registry metadata snapshot
   - runtime registry 现在会消费 `SKILL.md` 的 read/write contract 与 `agents/openai.yaml` 的 interface 元数据
   - 作用：为下一步 contract-aware / permission-aware 执行治理打基础

### F2：当前对话已完成

1. contract-aware execution preflight
   - 第一步只做 metadata-to-runtime 的 preflight，不立即假装已经具备完整 enforcement
   - `run-skill` 在执行前对 declared inputs、declared reads / writes、CLI args、artifact_refs 进行预解析与一致性检查
   - 初始阶段至少支持 `warn` 与 `strict` 两种模式

2. contract-aware execution enforcement
   - 在 preflight 成立后，再逐步把未声明输出、越界写路径、缺失 required input 变成真正的 allow/deny 决策
   - 这一阶段要区分 `runtime governance` 与 `business semantics`，不能把业务判断塞回 kernel

### F3：当前对话已完成

3. board-driven orchestration planner
   - 第一阶段不直接替换现有 phase-2 controller，而是先产出 `orchestration_plan_<round_id>.json`
   - planner 读取 board summary、board brief、challenge state、next actions、readiness，并显式给出下一步 skill queue、角色建议、停止条件和 fallback path

4. planner-backed controller cutover
   - 当前已完成 phase-2 preview cutover：controller 先执行 planner skill，再按 plan queue 执行 stage
   - 这一版仍只覆盖 phase-2 内部调度，不应被等同于 full board-driven、agent-decided runtime 已完成

## 4. 本批次交付标准

当前阶段交付必须满足：

1. 每个 skill 仍然是目录内自包含实现。
2. board 写入路径至少要保证同机多进程下的 filesystem lock + atomic replace，不再允许静默丢更新。
3. runtime 只允许承接 manifest、cursor、registry、ledger、receipt、executor 与治理逻辑，不把业务判断塞回 runtime。
4. ledger 至少记录命令快照、skill_args、声明式 contract、解析后的路径和输入/输出哈希，不能只做“结果日志”。
5. registry 至少消费 `SKILL.md` 与 `agents/openai.yaml` 的可机读元数据，为 contract-aware 执行做准备。
6. 必须补上脚本级集成测试，覆盖并发 board 写入、reporting publish 与 runtime 审计链回归。

## 5. 从当前状态继续推进的顺序

从当前状态到更完整的可运行系统，建议按下面顺序推进：

1. 把 orchestration / contract scaffold 接回新主链
2. 恢复 archive / history context / richer simulation
3. 做剩余 runtime hardening 与生产前准入验证

### 5.1 直接下一批的交付清单

如果只看“下一次编码批次”，建议严格收敛成下面 3 项：

1. mission scaffold / prepare-round 最小闭环
   - 先把真实任务入口重新接回 run contract，而不是继续停留在 seeded local workflow

2. fetch-plan / import execution 最小闭环
   - 输出真实 fetch / import artifact，而不是只在 phase-2 内部继续优化 controller queue

3. orchestration integration regression
   - 覆盖 mission -> prepare -> fetch/import -> normalize -> phase-2 -> reporting 的最小集成链路

### 5.2 下一批验收条件

下一批如果要算完成，至少要满足：

1. 真实 mission 输入至少能落成 prepare/fetch/import 所需 contract artifact
2. 最小真实 orchestration 链路能接到现有 normalize / phase-2 / reporting 主链
3. planner-backed controller 继续保持 preview 状态，不在这一批次扩张成 full board-driven runtime
4. unittest 需要新增 orchestration integration regression，而不是只更新文档口径

## 6. runtime 当前边界

runtime 在当前阶段仍不应承担新的业务推理，推荐继续维持下面边界：

1. `next_actions_<round_id>.json`、`falsification_probes_<round_id>.json`、`round_readiness_<round_id>.json`、`promoted_evidence_basis_<round_id>.json`、`reporting_handoff_<round_id>.json`、`council_decision_draft_<round_id>.json`、`expert_report_draft_<role>_<round_id>.json`、`expert_report_<role>_<round_id>.json`、`council_decision_<round_id>.json` 的契约应继续保持稳定。
2. 最小 runtime kernel 负责 run manifest、artifact path resolver、receipt/event ledger、skill executor wrapper、round cursor、promotion gate、round controller、supervisor state，以及后续 contract-aware / permission-aware 治理。
3. reporting / decision 仍然优先以 atomic skill 方式推进，而不是把新业务逻辑塞回 runtime。

换句话说，runtime 现在已经形成了一个 planner-backed 的 phase-2 preview，但仍不是蓝图里的 full board-driven、agent-decided orchestration runtime。

## 7. 当前补充状态

- `run-phase2-round` 现在可以把 `board -> D1 -> D2 -> promotion` 串成单命令流程。
- `supervise-round` 现在会在 controller 结果上额外落出 operator 视角的 `supervisor_state_<round_id>.json`。
- `show-run-state` 现在会同时回显最新 round 的 gate / controller / supervisor 快照。
- `show-run-state` 现在也会回显最新 round 的 `orchestration_plan_<round_id>.json`。
- `eco-materialize-reporting-handoff` 与 `eco-draft-council-decision` 已经把 promotion basis 接到 reporting / decision 第一批下游对象。
- `eco-draft-expert-report`、`eco-publish-expert-report`、`eco-publish-council-decision` 已经把 role draft、canonical report 与 canonical decision 接回 skill-first 主链。
- `eco-materialize-final-publication` 已经把 reporting 主链从 canonical report / decision 收敛到最终发布对象。
- `eco-plan-round-orchestration` 已经把 phase-2 controller 先前的固定 queue 显式化为一个 planner artifact，并驱动 planner-backed cutover。
- board 写入路径现在已切到 filesystem lock + atomic replace + `board_revision`，当前目标是先保证同机多进程安全，而不是宣称分布式协作已经完成。
- runtime registry 现在会快照 skill contract 与 agent metadata，ledger 也会记录命令快照、skill_args、解析路径和输入/输出哈希。
- runtime 现在已经具备 contract-aware preflight 与 enforcement baseline：支持 `preflight-skill`、`run-skill --contract-mode off|warn|strict`，并能阻断缺失 required inputs、未声明 path override、undeclared summary path 与 artifact_ref mismatch。
- 当前完整 unittest 集会继续扩展，用于覆盖并发写、reporting publish、planner cutover 与后续 orchestration integration 回归。

## 8. 面向生产的开发指引

- 生产路线、环境准入条件、shadow test 与 pilot 条件统一收敛在 [openclaw-production-development-plan.md](openclaw-production-development-plan.md)。
- 本计划只继续追踪“做哪一批能力”，不重复维护生产准入细则。

这份文档是当前蓝图下的执行型阶段计划，后续批次应在此基础上继续推进。