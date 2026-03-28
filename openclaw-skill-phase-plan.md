# OpenClaw Skill 分阶段交付计划

## 1. 规划依据

本计划以当前的 [openclaw-first-refactor-blueprint.md](openclaw-first-refactor-blueprint.md) 为唯一架构基线，执行上遵守以下约束：

- `skills/` 是主要业务能力面，所有新增能力都以 atomic skill 方式交付
- `eco-concil-runtime/` 仅保留最小运行时内核，不承担新业务实现
- 数据主链继续沿用 `raw artifact -> source normalize -> signal plane -> candidate/evidence -> board -> promote`
- 所有新增 skill 必须保持自包含，不允许重新引入根级共享 runtime 包装层

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

这说明当前主链已经不只停留在“候选对象生成”，而是已经延伸到 evidence bridge、board working-state，以及 board organize / brief 这一层。

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

### D1：investigation 行动层

1. `eco-propose-next-actions`
   - 输入：board summary / board brief + evidence coverage + challenge state
   - 输出：`next_actions_<round_id>.json`
   - 作用：把当前 board working-state 转成可执行的 next-action queue

2. `eco-open-falsification-probe`
   - 输入：challenge ticket / hypothesis / evidence refs
   - 输出：`falsification_probes_<round_id>.json`
   - 作用：把“需要继续挑战”落成显式 probe 对象，而不是继续停留在 note 或 ticket 层

### D2：readiness / promotion 层

3. `eco-summarize-round-readiness`
   - 输入：board brief + next action queue + evidence coverage
   - 输出：`round_readiness_<round_id>.json`
   - 作用：判断当前轮次是 ready、needs-more-data 还是 blocked

4. `eco-promote-evidence-basis`
   - 输入：round readiness + board brief + selected evidence refs
   - 输出：`promoted_evidence_basis_<round_id>.json`
   - 作用：把 working-state 冻结成可被后续报告与 decision 层消费的正式 basis artifact

## 4. 本批次交付标准

本轮 C2 批次必须满足：

1. 每个 skill 仍然是目录内自包含实现。
2. 继续默认从现有 board JSON 产物读取，不要求引入新的共享 runtime。
3. 新增对象只允许扩展 `investigation_board.json`，不能把 board 业务逻辑回塞到 runtime。
4. 输出继续保持 compact artifact + receipt + board handoff 的风格，其中 summary 与 brief 允许分别落成 JSON / Markdown。
5. 必须补上脚本级集成测试，验证 challenge -> task -> close -> summary -> brief 可以串联。
6. 现有 C1 board skill 的 `suggested_next_skills` 要衔接到本轮新增 skill。

## 5. 当前执行顺序

本次对话的延续顺序按下面推进：

1. 新增 C2 的 4 个 board organize skill
2. 调整现有 C1 board handoff
3. 补充 board workflow 集成测试
4. 对 D 层做 skill-first 规划
5. 生成当前仓库状态清单
6. 运行测试并确认当前 skill 面可用

## 6. runtime 动工窗口

runtime 在当前阶段仍然不宜提前扩张，推荐按下面窗口动工：

1. C2 完成后，不立即重建完整 runtime，只继续推进 D1 skill，把 `next_actions` 和 `falsification_probes` 的 artifact 契约先稳定下来。
2. 当 D1 交付并经过一轮脚本级集成测试后，再启动最小 runtime kernel：只负责 run manifest、artifact path resolver、receipt/event ledger、skill executor wrapper 和 round cursor。
3. 等 D2 的 readiness / promotion skill 落地后，runtime 才补第 2 步：把 promote/freeze 命令、round gate、以及 supervisor 入口接回来。

换句话说，runtime 不是当前批次的主战场；它应该在 D1 稳定之后动工，而且只做“编排与落盘”，不再承载业务语义。

这份文档是当前蓝图下的执行型阶段计划，后续批次应在此基础上继续推进。