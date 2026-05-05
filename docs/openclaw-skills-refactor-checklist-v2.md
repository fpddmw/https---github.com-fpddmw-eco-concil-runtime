# OpenClaw Skills 架构与扩展计划

## 1. 文档定位

本文描述当前 skills 体系、分层、治理边界和后续扩展计划。文件名保留历史路径，但本文不再是迁移清单。

当前 active skills：`82`。

分层统计：

1. `fetch`: 16
2. `normalize`: 17
3. `query`: 9
4. `optional-analysis`: 17
5. `deliberation-write`: 9
6. `reporting`: 8
7. `state-transition`: 4
8. `runtime-admin`: 2

## 2. Skill 设计原则

1. skill 是原子工具，不是默认结论链。
2. fetch 只抓取或导入，不解释。
3. normalize 只归一化，不做事实判断。
4. query 必须返回 item-level evidence basis。
5. optional-analysis 默认是 approval-gated helper。
6. deliberation-write 才能把调查判断写成议会对象。
7. reporting 只能消费 frozen/reporting basis，不回写调查状态。
8. state-transition 必须经 moderator request 和 runtime-operator approval。

## 3. Skill 分层

### 3.1 Fetch

代表技能：

1. `fetch-airnow-hourly-observations`
2. `fetch-openaq`
3. `fetch-open-meteo-historical`
4. `fetch-open-meteo-air-quality`
5. `fetch-open-meteo-flood`
6. `fetch-usgs-water-iv`
7. `fetch-nasa-firms-fire`
8. `fetch-gdelt-doc-search`
9. `fetch-gdelt-events`
10. `fetch-gdelt-mentions`
11. `fetch-gdelt-gkg`
12. `fetch-youtube-video-search`
13. `fetch-youtube-comments`
14. `fetch-bluesky-cascade`
15. `fetch-regulationsgov-comments`
16. `fetch-regulationsgov-comment-detail`

边界：

1. 输出 raw provider payload 或 runtime-captured artifact。
2. 不写 board judgement。
3. 不判断 claim true/false。
4. 受 source governance、side-effect policy 和 admission 控制。

### 3.2 Normalize

职责：

1. 读取 raw artifact。
2. 写入 `normalized_signals`。
3. 保留 provenance、artifact ref、record locator、quality flags、metadata。
4. 为 public/formal/environment query 提供统一 DB surface。

边界：

1. 不做研究结论。
2. 不给出 readiness。
3. 不把 taxonomy label 变成事实判断。

### 3.3 Query

代表技能：

1. `query-public-signals`
2. `query-formal-signals`
3. `query-environment-signals`
4. `query-normalized-signal`
5. `query-raw-record`
6. `query-signal-corpus`
7. `query-board-delta`
8. `query-case-library`
9. `query-spatiotemporal-relations`

要求：

1. 返回 item-level `evidence_refs`。
2. 返回 `evidence_basis` 和 source provenance。
3. 支持 investigator 从 query result 提交 finding/evidence bundle。
4. public/environment query 支持 `round_scope=current|up-to-current|all`。

### 3.4 Optional Analysis

定位：

`approval-gated advisory helper view`

用途：

1. 帮 agent 审视证据覆盖。
2. 帮 moderator 组织议题和下一步。
3. 帮 challenger 找到可质疑点。
4. 帮 report editor 形成 appendix/audit/uncertainty 材料。

边界：

1. 不直接作为 phase gate。
2. 不直接作为 report basis。
3. 不输出 claim truth。
4. 不输出强因果。
5. 不绕过 finding/evidence bundle/proposal/report basis。

### 3.5 Deliberation Write

代表技能：

1. `submit-council-proposal`
2. `submit-readiness-opinion`
3. `post-board-note`
4. `update-hypothesis-status`
5. `open-challenge-ticket`
6. `close-challenge-ticket`
7. `claim-board-task`
8. `summarize-board-state`
9. `materialize-board-brief`

职责：

1. 把 agent 判断落成 DB council object。
2. 将 evidence refs、lineage、provenance 固化。
3. 支持 controller/gate/supervisor 从 DB 读取议会状态。

### 3.6 Reporting

代表技能：

1. `materialize-reporting-handoff`
2. `materialize-spatiotemporal-relation-evidence-packet`
3. `draft-council-decision`
4. `draft-expert-report`
5. `publish-expert-report`
6. `publish-council-decision`
7. `materialize-final-publication`
8. `materialize-history-context`

职责：

1. 消费 frozen report basis。
2. 生成 evidence packet、decision packet、report packet。
3. 保留 uncertainty register、residual disputes、policy recommendations。
4. 通过 operator approval 控制发布。

### 3.7 State Transition / Runtime Admin

代表技能：

1. `scaffold-mission-run`
2. `prepare-round`
3. `open-investigation-round`
4. `freeze-report-basis`
5. `archive-signal-corpus`
6. `archive-case-library`

职责：

1. 打开 run/round。
2. 准备 source plan。
3. 开启 follow-up investigation round。
4. 冻结报告依据。
5. 关闭轮次后归档。

## 4. 当前 Skills 的有效性判断

### 已足够支撑的能力

1. 多源数据进入统一 signal plane。
2. query 到 finding/evidence bundle 的调查闭环。
3. council proposal/readiness/challenge 的议会协作。
4. report basis freeze 到 final publication 的报告链。
5. 多轮 round carryover 与 cross-round query。
6. optional-analysis 的审计与降权机制。

### 不应过度宣称的能力

1. 不具备全领域环境报告自动生成能力。
2. 不具备强污染归因模型。
3. 不具备完整健康风险、生态风险、司法鉴定或 EIA 模型。
4. optional-analysis 多数是启发式视图，不是专业模型结论。

## 5. 新增 Skill 规范

新增 skill 必须同时定义：

1. `SKILL.md`
   - core goal、triggering conditions、read/write contract、required input、output contract、scripts。
2. `scripts/*.py`
   - 可 CLI 执行，stdout 输出 JSON object。
3. `agents/openai.yaml`
   - 如果需要 agent prompt。
4. `kernel/skill_registry.py` policy
   - skill layer、allowed roles、capabilities、write scope、approval requirement。
5. canonical object 或 result shape
   - 对判断型输出必须有 evidence refs、lineage、provenance。
6. tests
   - 至少覆盖 successful output、missing input、governance boundary。

## 6. 通用时空关系扩展计划

近期建议聚焦 `spatiotemporal-relation` 基础设施，而不是同时补全所有生态环境领域或新增 `transport-investigation` 窄 skill 包。

### 6.1 Relation Infrastructure Skill Set

建议补强：

1. `detect-temporal-cooccurrence-cues`
   - 保留 legacy same-day cue。
   - 在显式 source/target/scope 参数下输出 structured relation cue。
2. `query-spatiotemporal-relations`
   - 支持按 relation_id、relation_status、source_signal_id、target_signal_id、source_role、target_role 查询。
3. `review-spatiotemporal-relation-alternatives`
   - 输出 objection candidates，由 challenge/probe/review comment 承接。
4. `materialize-spatiotemporal-relation-evidence-packet`
   - 默认写入 relation evidence packet artifact。
   - 仅在显式 `--write-basis-objects` 下把 relation cues、rejections、challenger objections、uncertainty 转成 finding/evidence bundle/report section 可引用 basis。

### 6.2 Relation Canonical Objects

建议新增或规范化对象：

1. `spatiotemporal-relation-cue`
2. `spatiotemporal-relation-evidence-packet`
3. relation-oriented `challenge` / `probe` fields
4. structured `verification_scope`

每个对象必须带：

1. spatial scope
2. temporal scope
3. data sources
4. evidence refs
5. lineage
6. uncertainty / limitations
7. challenger objections

## 7. Demo 推荐链路

建议论文/demo 使用以下链路：

1. `scaffold-mission-run`
2. `prepare-round`
3. fetch/import public + environment + fire/weather
4. normalize
5. query signals
6. investigator submit finding/evidence bundle
7. optional `aggregate-environment-evidence`
8. optional `discover-discourse-issues`
9. challenger open challenge
10. readiness opinion = needs-more-data
11. `open-investigation-round`
12. 第二轮补 structured relation cue 或 relation evidence packet
13. proposal/readiness
14. `freeze-report-basis`
15. reporting handoff / decision / expert reports / final publication

## 8. 后续清理

1. 保留现有 skill ids，论文前不要做大规模 breaking rename。
2. 将 legacy analysis kind query compatibility 标为 query-only。
3. 将 relation cue / evidence packet 输出优先接入 DB basis。
4. 后续再清理 `report_basis_*` 命名债、历史 artifact trace 字段和旧 helper module 名称。
