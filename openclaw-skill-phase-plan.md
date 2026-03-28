# OpenClaw Skill 分阶段交付计划

## 1. 规划依据

本计划以当前的 [openclaw-first-refactor-blueprint.md](openclaw-first-refactor-blueprint.md) 为唯一架构基线，执行上遵守以下约束：

- `skills/` 是主要业务能力面，所有新增能力都以 atomic skill 方式交付
- `eco-concil-runtime/` 仅保留最小运行时内核，不承担新业务实现
- 数据主链继续沿用 `raw artifact -> source normalize -> signal plane -> candidate/evidence -> board -> promote`
- 所有新增 skill 必须保持自包含，不允许重新引入根级共享 runtime 包装层

## 2. 当前完成面

当前已交付的 skill 面可分为三层：

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

这说明当前主链已经走通到了“候选对象生成”，但在候选对象和未来 board / promotion 之间仍缺少稳定的 evidence 中间层。

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

### B2：下一小批

在 B1 稳定后继续补：

1. `eco-derive-claim-scope`
2. `eco-derive-observation-scope`
3. `eco-score-evidence-coverage`

这三项会把当前仍较弱的空间/范围语义补齐。

## Phase C：board 操作层

目标：把现在仅存在于 `board_handoff` 里的协作建议真正写入 board 可消费的工作态对象。

优先 skill：

1. `eco-read-board-delta`
2. `eco-post-board-note`
3. `eco-open-challenge-ticket`
4. `eco-update-hypothesis-status`

## Phase D：investigation / promotion 层

目标：让多 agent 不只会提取和链接，还能推进 probe、challenge 与正式冻结。

优先 skill：

1. `eco-propose-next-actions`
2. `eco-open-falsification-probe`
3. `eco-summarize-round-readiness`
4. `eco-promote-evidence-basis`

## 4. 本批次交付标准

本次 B1 evidence 批次必须满足：

1. 每个 skill 仍然是目录内自包含实现。
2. 默认从现有 analytics JSON 产物读取，不要求引入新的共享 runtime。
3. 输出继续保持 compact JSON artifact + receipt + board handoff 的风格。
4. 必须补上至少一组脚本级集成测试，验证这 3 个新 skill 可以与当前 N1/N2 结果串联。
5. 现有 extract / audit skill 的 `suggested_next_skills` 要衔接到本批次新增 skill。

## 5. 当前执行顺序

本次对话按下面顺序推进：

1. 写入本计划文档
2. 新增 B1 的 3 个 evidence skill
3. 调整现有 extract / audit handoff 指向
4. 新增 evidence 批次集成测试
5. 运行测试并确认当前 skill 面可用

这份文档是当前蓝图下的执行型阶段计划，后续批次应在此基础上继续推进。