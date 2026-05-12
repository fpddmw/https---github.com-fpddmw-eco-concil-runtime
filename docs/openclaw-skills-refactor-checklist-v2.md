# OpenClaw Skills 架构与治理边界

## 1. 文档定位

本文描述当前 skills 体系、分层和治理边界。文件名保留历史路径，但本文不再是迁移清单或统一扩展计划。

当前 active skills：`94`。

分层统计：

1. `fetch`: 16
2. `normalize`: 17
3. `query`: 9
4. `optional-analysis`: 17
5. `deliberation-write`: 21
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
5. 普通合法 fetch 不强制要求先有 proposal；proposal 只用于跨 agent 协调、审批、跨轮承接或显式议会记录。

### 3.2 Normalize

职责：

1. 读取 raw artifact。
2. 写入 `normalized_signals`。
3. 保留 provenance、artifact ref、record locator、quality flags、metadata。
4. 为 public/formal/environment query 提供统一 DB surface。
5. `normalize-fetch-execution` 负责把已执行 fetch plan 中可归一化的 receipt/artifact 写入 signal plane；无法归一化时必须保持 `receipt-only` 语义。

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
7. `submit-evidence-request`
8. `submit-investigation-plan`
9. `submit-investigation-scope`
10. `submit-round-brief`
11. `submit-agent-position`
12. `submit-challenge-disposition`
13. `claim-board-task`
14. `summarize-board-state`
15. `materialize-board-brief`
16. `materialize-context-packet`
17. `submit-source-acquisition-proposal`
18. `update-source-acquisition-proposal-status`
19. `submit-round-synthesis`
20. `link-source-acquisition-execution`
21. `open-followup-from-review-comment`

职责：

1. 把 agent 判断落成 DB council object。
2. 将 evidence refs、lineage、provenance 固化。
3. 支持 controller/gate/supervisor 从 DB 读取议会状态。
4. source-acquisition proposal 是薄议会对象，不是 source 推荐系统；只记录 source skill、query parameters、side effects、target refs、rationale、evidence refs、lineage、provenance 和生命周期状态。
5. round synthesis 是 moderator 阶段性记录，不是 agenda scheduler；只记录阶段结论、已覆盖 refs、未解决 refs、证据缺口和候选 continuation refs。
6. source acquisition execution link 只把 proposal、fetch receipt、normalization receipt、normalized signal refs 和 artifact refs 串成 lineage，不执行取证、不归一化、不判断采信。

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
7. state-transition request 一旦 committed，只能重放到同一 committed object，不能复用到另一个 round transition 或 report basis。
8. target 已存在时，state-transition skill 应优先 no-op 并保留 canonical id / warning，而不是再次写入业务对象。

## 4. 当前 Skills 的有效性判断

### 已足够支撑的能力

1. 多源数据进入统一 signal plane。
2. query 到 finding/evidence bundle 的调查闭环。
3. council proposal/readiness/challenge 的议会协作。
4. report basis freeze 到 final publication 的报告链。
5. 多轮 round carryover 与 cross-round query。
6. optional-analysis 的审计与降权机制。
7. agent-led source acquisition、proposal lifecycle、round liveness handoff 和 archive/history evidence-ref reuse。

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
4. `kernel/governance/skill_registry.py` policy
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

## 7. Agent 自主调查链路

agent 自主取证、finding 后续议程承接、多轮 continuation 和 archive/history 复用的详细进展见 `docs/openclaw-agent-autonomy-archive-workplan.md`。skills 层只提供原子能力和 role-governed surfaces，不负责把开放型 mission 直接硬编码成固定调查剧本，也不负责替 agent 排序 source 或采信证据。

以下是后续真实案例需要观察的 skills 能力节点，不是固定脚本，也不要求每次运行都按相同顺序成功出现：

1. `scaffold-mission-run` 能初始化受治理 run/round。
2. moderator 规划类 write surface 能把开放型 mission 转成 investigation plan、subissues、round briefs。
3. evidence request 表达“需要什么信息”，source acquisition 由 agent 自主提出或执行。
4. `prepare-round` 能消费 round-brief，但不把 source plan 变成 agent 议程锁。
5. fetch/import public + environment + fire/weather 能进入 raw/normalized surfaces。
6. normalize 能写出可查询 signals 和 metadata。
7. query signals 能返回 item-level evidence refs。
8. investigator 能提交 finding/evidence bundle/hypothesis/proposal。
9. optional helper 只能输出审计线索。
10. challenger 能 open challenge 或 approval-gated falsification probe。
11. readiness opinion 能在证据不足时标记 `needs-more-data`。
12. `open-investigation-round` 能从 gap/challenge/finding 派生 follow-up 或 supplemental round。
13. structured relation cue 或 relation evidence packet 能暴露候选关系和不确定性。
14. archive/history 能在 checkpoint 或 closeout 后提供历史 evidence refs。
15. `freeze-report-basis` 只能消费被承接过的 DB basis。
16. reporting handoff / decision / expert reports / final publication 能保留 evidence index 和 residual disputes。

当前已进入基线的链路：

1. `submit-source-acquisition-proposal` 记录 agent 自主选择的 source skill 和 query parameters。
2. `update-source-acquisition-proposal-status` 记录 `proposed|approved-for-execution|executed|withdrawn|rejected` 生命周期，不执行取证、不判断采信。
3. `show-source-acquisition-intents` 暴露 preflight/fetch/status-update command templates，执行命令只携带 proposal 明确请求的 side-effect approvals。
4. `open-investigation-round` carry `primary_focus_refs` 到下一轮；这些 refs 是 handoff context，不限制 agent read/write/source surface。
5. `materialize-history-context`、`query-case-library`、`query-signal-corpus` 暴露历史 evidence refs 和 match surfaces；history 不生成当前 run 结论。
6. fetch receipt 在未归一化前保持 `receipt-only`，并通过 status surface 显示 normalizer 和后续 query hints。
7. `submit-round-synthesis` 提供 moderator 阶段性结论入口；`round_liveness.closing_checklist` 只列 observed gaps 和 copyable commands，不排序、不固定下一轮议程。
8. `link-source-acquisition-execution` 将 agent-authored source proposal 与 receipt / normalized signal refs 建立审计 lineage，不把 linked refs 提升为 accepted evidence。

## 8. 当前结论

skills 当前形态可以接受，不进入批量拆分。

后续判断标准：

1. 不按行数拆 skill。
2. 不因 provider 脚本较长而拆 skill。
3. 只有当一个 skill 混入多个独立用户能力、多个输入契约、多个输出 artifact 家族，或跨越 fetch/normalize/query/write/reporting 层边界时，才考虑拆成多个 skill。
4. skill 内部可以继续整理 helper，但优先保持 skill-local，避免把 provider 细节扩散到 runtime shared dependency。
5. optional-analysis helper 继续保持 advisory/audit 定位；报告正文必须由 finding、evidence bundle、proposal、review comment、report basis 或 reporting object 显式承接。

质量门基线命令保留在 `docs/openclaw-project-overview.md` 和 `docs/openclaw-refactor-overall-notes.md`。
