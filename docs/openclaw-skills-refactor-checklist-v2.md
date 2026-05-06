# OpenClaw Skills 架构与治理边界

## 1. 文档定位

本文描述当前 skills 体系、分层和治理边界。文件名保留历史路径，但本文不再是迁移清单或统一扩展计划。

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
6. reporting draft/publish/finalize 中声明 `requires_operator_approval=True` 的 skill 必须经 skill approval request/approval 后由 runtime 执行。
7. deliberation-write 才能把调查判断写成议会对象。
8. reporting 只能消费 frozen/reporting basis，不回写调查状态。
9. state-transition 必须经 moderator phase transition request 和 runtime-operator approval，不走 skill approval request。

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
4. 通过 skill approval request/approval 控制正式 runtime 发布。

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
6. phase transition 类动作通过 transition request/approval 治理，不复用 skill approval。

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

## 6. 通用时空关系基础设施当前状态

`spatiotemporal-relation` 基础设施已作为当前 skills baseline，而不是后续扩展计划。基础能力包括：

1. `detect-temporal-cooccurrence-cues`
   - 保留 legacy same-day cue，并在显式 source/target/scope 参数下输出 structured relation cue。
2. `query-spatiotemporal-relations`
   - 支持按 relation_id、relation_status、source_signal_id、target_signal_id、source_role、target_role 查询。
3. `review-spatiotemporal-relation-alternatives`
   - 输出 objection candidates，由 challenge/probe/review comment 承接。
4. `materialize-spatiotemporal-relation-evidence-packet`
   - 默认写入 relation evidence packet artifact。
   - 仅在显式 `--write-basis-objects` 下把 relation cues、rejections、challenger objections、uncertainty 转成 finding/evidence bundle/report section 可引用 basis。

该能力仍遵守 optional-analysis 降权原则：relation cue 是候选线索，不是传播证明、污染源归因或报告结论。

## 7. 真实案例评测链路

真实案例评测链路不在本文中继续展开，独立计划见 `docs/openclaw-case-study-evaluation-workplan.md`。

以下是评测时需要观察的能力节点，不是固定脚本，也不要求每次真实案例运行都按相同顺序成功出现：

1. `scaffold-mission-run` 能初始化受治理 run/round。
2. `prepare-round` 能生成可审计 source plan。
3. fetch/import public + environment + fire/weather 能进入 raw/normalized surfaces。
4. normalize 能写出可查询 signals 和 metadata。
5. query signals 能返回 item-level evidence refs。
6. investigator 能提交 finding/evidence bundle。
7. optional helper 只能输出审计线索。
8. challenger 能 open challenge 或 falsification probe。
9. readiness opinion 能在证据不足时标记 `needs-more-data`。
10. `open-investigation-round` 能从 gap/challenge 派生 follow-up round。
11. structured relation cue 或 relation evidence packet 能暴露候选关系和不确定性。
12. proposal/readiness 能承接下一步行动或阻断。
13. `freeze-report-basis` 只能消费被承接过的 DB basis。
14. reporting handoff / decision / expert reports / final publication 能保留 evidence index 和 residual disputes。

## 8. 后续清理入口

后续清理不在本文维护成总计划，按独立工作面跟踪：

1. `docs/openclaw-module-decomposition-workplan.md`
   - 拆分大模块，保留现有 skill ids 和 CLI 兼容。
2. `docs/openclaw-schema-migration-hardening-workplan.md`
   - 将 schema 变更纳入 version 和 migration ledger。
3. `docs/openclaw-ci-quality-gates-workplan.md`
   - 固定 skills 与 runtime 的 targeted 回归门。
4. `docs/openclaw-runtime-governed-execution-workplan.md`
   - 将正式运行入口收束到 runtime-governed execution。
