# OpenClaw 项目整体工作流技术报告

## 1. 报告定位

本报告面向课题组说明：当前项目是如何把一次调查任务，从任务初始化一路推进到证据分析、议会判断、冻结发布与归档复用的。

一句话概括：

`runtime 管治理，数据库管状态，skill 管分析，moderator 管议会推进，OpenClaw 管调查。`

## 2. 系统目标

项目当前的核心目标，不是构造一条更长的固定流水线，而是把原先依赖大量 JSON 串联的调查流程，逐步迁移为：

1. 以数据库为主工作面。
2. 以 skill 为原子分析工具。
3. 以 moderator 驱动 round 与 board 的推进。
4. 以 runtime 负责审批、执行、账本和治理边界。

这意味着项目的重点，已经从“生成更多中间产物”，转向“让系统能围绕共享状态持续调查、恢复、回放和审计”。

## 3. 当前端到端工作流

### 3.1 Run 初始化

第一步是初始化一次调查运行。

运行时会创建：

1. `runtime/run_manifest.json`
2. `runtime/round_cursor.json`
3. `runtime/skill_registry.json`
4. `runtime/admission_policy.json`
5. `runtime/runtime_health.json`
6. `runtime/operator_runbook.md`

这里体现的是治理优先原则：先把执行边界、技能注册、运行账本和操作入口建好，再进入调查本体。

### 3.2 任务脚手架与轮次准备

当前运行入口主要围绕两类 skill：

1. `eco-scaffold-mission-run`
   - 初始化 mission、首轮任务和种子 board。
2. `eco-open-investigation-round`
   - 开启新一轮 round，并继承上一轮未解决的任务、假设、挑战和调查上下文。

随后由：

1. `eco-prepare-round`
   - 将 mission、round task 和 source governance 编译为 `fetch_plan`。
2. `eco-import-fetch-execution`
   - 执行 fetch plan，完成抓取、导入和后续 normalizer 入库。

这一步体现的是：源接入仍然受 runtime 治理，但它已经不再是唯一的智能决定点。

### 3.3 证据接入与信号归一化

外部数据源目前分成两类：

1. `raw/public`
   - 例如 GDELT、YouTube、Bluesky、Regulations.gov。
2. `raw/environment`
   - 例如 OpenAQ、AirNow、USGS、Open-Meteo、NASA FIRMS。

这些原始抓取结果经过 `eco-normalize-*` 系列技能，统一写入 `normalized_signals` 表。

这一步的设计重点有三点：

1. 原始文件保留，作为证据原件。
2. 数据库中保留统一的查询面。
3. 每条 normalized signal 都保留 `artifact_path + record_locator + raw_json`，便于反查原始记录。

### 3.4 分析链构建

当前分析链大体为：

1. 公共信号抽取 `claim candidates`
2. 环境信号抽取 `observation candidates`
3. claim 聚类与 observation 合并
4. claim 与 observation 建 link
5. 生成 claim / observation scope
6. 计算 evidence coverage

当前这条链已经不再只是“生成 JSON 文件”，而是会把结果同步到 `Analysis Plane`：

1. `analysis_result_sets`
2. `analysis_result_items`
3. `analysis_result_lineage`

因此，后续消费者即使在 JSON 导出物缺失时，也能继续从数据库恢复分析上下文。

### 3.5 议会状态推进

议会推进围绕 `Deliberation Plane` 展开，主要对象包括：

1. `board_notes`
2. `hypothesis_cards`
3. `challenge_tickets`
4. `board_tasks`
5. `round_transitions`
6. `promotion_freezes`

对应的状态修改 skill 包括：

1. `eco-post-board-note`
2. `eco-update-hypothesis-status`
3. `eco-open-challenge-ticket`
4. `eco-close-challenge-ticket`
5. `eco-claim-board-task`
6. `eco-open-investigation-round`

截至当前阶段，这些关键 board 变更已经从“JSON first -> DB sync”迁移到“DB first -> JSON export”。这意味着 board 不再只是快照文件，而是可查询、可恢复的结构化状态。

### 3.6 Moderator phase-2 控制流程

当前 round 的 phase-2 控制链主要包括：

1. `orchestration-planner`
2. `next-actions`
3. `falsification-probes`
4. `round-readiness`
5. `promotion-gate`
6. `promotion-basis`

其中：

1. `board-summary` 和 `board-brief` 已被降级为 derived exports，不再是硬前置。
2. `next_actions`、`falsification_probes`、`round_tasks` 都已有 deliberation-plane-backed 恢复面。
3. `promotion_gate`、`controller_state`、`supervisor_state` 也能从 deliberation plane 恢复，不再完全依赖 JSON 文件是否还在。

这说明 moderator loop 的主控制面已经基本从线性导出物链转到了数据库快照面。

### 3.7 Promotion、Reporting 与 Final Publication

当 round readiness 满足条件后，系统进入 promotion 和 reporting 链：

1. `eco-promote-evidence-basis`
2. `eco-materialize-reporting-handoff`
3. `eco-draft-council-decision`
4. `eco-draft-expert-report`
5. `eco-publish-expert-report`
6. `eco-publish-council-decision`
7. `eco-materialize-final-publication`

这一层目前已经完成统一的 trace contract 规范化，能够显式携带：

1. `board_state_source`
2. `coverage_source`
3. `deliberation_sync`
4. `analysis_sync`
5. `observed_inputs`
6. 各输入的 `*_artifact_present` 与 `*_present`

因此，最终报告和发布物已经不再是“黑盒摘要”，而是带有可追踪来源链的导出结果。

### 3.8 归档、历史与治理闭环

在 round 结束或 run 收束之后，系统还支持：

1. `eco-archive-case-library`
2. `eco-archive-signal-corpus`
3. `eco-materialize-history-context`
4. replay / benchmark / operator state inspection

这使项目不只是“一次性报告系统”，而是有机会形成：

1. 历史案例库
2. 跨 run 信号语料库
3. 可回放、可比较的治理闭环

## 4. 当前工作流的关键变化

相较于早期结构，当前工作流发生了四个本质变化：

1. `board` 从 JSON 快照集合，迁移为 deliberation-plane-first 的共享状态面。
2. `coverage / scope / links / candidates` 从 JSON 导出物，迁移为 analysis-plane-first 的可查询结果集。
3. `board_summary / board_brief` 从控制前置，降级为可选导出物。
4. `next_actions / probes / round_tasks / controller snapshots` 从单一文件依赖，迁移为 DB-backed 恢复面。

## 5. 当前成熟度判断

截至 `2026-04-06`：

1. 总阶段完成度为 `20 / 25`。
2. `Route B` 已经 `8 / 8` 完成，说明 moderator loop 的主要 DB-first 迁移已完成。
3. `Route C` 已完成 `5 / 7`，分析面已经成型，但 candidate/cluster 家族尚未完全纳入统一 result-set。
4. `Route A` 已完成 `4 / 6`，运行时治理契约已经硬化，但还需要 `A3` 做回归硬化。

## 6. 汇报时建议强调

如果需要在 3 分钟内讲清整体工作流，建议重点讲三句话：

1. 这个项目已经不是简单的“抓数据再写报告”，而是在构造一个带治理边界的多 agent 调查运行时。
2. 当前最重要的技术进展，是分析面和议会面都已经有数据库主工作面，JSON/Markdown 逐步降级为导出物。
3. 这让系统从一次性流水线，转向可恢复、可追踪、可审计、可继续调查的长期运行结构。

