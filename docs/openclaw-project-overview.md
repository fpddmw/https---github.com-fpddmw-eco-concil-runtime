# OpenClaw 项目架构总览与后续开发计划

## 1. 项目定位

OpenClaw 是一个面向生态环境争议调查的 DB-first 多 agent 议会运行时。它的目标不是让模型直接给出不可审计的结论，而是把模型调查、证据抓取、反证挑战、阶段推进、报告冻结和最终发布组织成一条可追溯、可复核、可恢复的工作流。

一句话定位：

`一个受治理、DB-first、证据可审计的生态环境调查议会系统。`

核心分工：

1. `runtime kernel` 负责执行、权限、审批、ledger、receipt、replay、DB 持久化和 operator 可见状态。
2. `agent council` 负责实质性调查判断，包括 proposal、finding、evidence bundle、challenge、readiness opinion。
3. `database` 是议会状态、调查对象和报告依据的主要状态源。
4. `artifact` 是导出、handoff、调试和人类阅读材料，不作为唯一事实源。

## 2. 架构总览

系统按工作面分为七层：

1. `runtime / governance`
   - run、round、skill registry、actor role、admission、side effect、operator approval、receipt、ledger、dead letter、health、replay。
2. `source / ingestion`
   - source catalog、source governance、source selection、fetch/import plan、detached fetch、raw artifact 管理。
3. `signal plane`
   - 将 public、formal、environment 三类输入统一归一化为 `normalized_signals` 和索引字段。
4. `analysis plane`
   - 承载 approval-gated 的 optional analysis helper 输出，以及 DB-backed result set / lineage。
5. `deliberation plane`
   - 承载议会对象：finding、evidence bundle、proposal、hypothesis、challenge、board task、probe、readiness、round transition、report-basis-freeze。
6. `control plane`
   - controller、gate、supervisor、runtime-control-freeze、orchestration step 等运行状态。
7. `reporting / archive`
   - reporting handoff、council decision、expert report、final publication、case library、signal archive、history context。

## 3. 主工作流

当前主干流程：

`mission -> run/round -> source governance -> fetch/import -> normalize -> query/analysis -> council deliberation -> readiness/gate -> report basis freeze -> reporting -> archive/history`

细化为：

1. `scaffold-mission-run`
   - 创建 run/round、mission、初始 board、round task scaffold。
2. `prepare-round`
   - 根据 mission、round tasks、source governance 生成 source selections 和 fetch plan。
3. `fetch/import + normalize`
   - 抓取或导入 raw artifact，并通过对应 normalizer 写入 signal plane。
4. `query`
   - investigator 通过 public/formal/environment/raw/normalized query surfaces 获取 item-level evidence basis。
5. `council write`
   - agent 提交 finding、evidence bundle、proposal、review comment、challenge、hypothesis、readiness opinion。
6. `optional analysis`
   - 经 operator approval 后运行 helper，输出审计视图、证据覆盖摘要、议题线索、footprint、temporal cue、sufficiency note 等。
7. `phase control`
   - controller/gate/supervisor 读取 DB council objects，判断是否继续调查、打开新 round、冻结 report basis 或阻断报告。
8. `reporting`
   - report editor 基于 frozen report basis 和 DB reporting objects 生成 handoff、decision、expert report、final publication。
9. `archive/history`
   - close-round 后归档 signal/case，并可为新 run 物化 history context。

## 4. 多轮调查能力

系统支持分批取证。若议会认为证据不足，moderator 可以发起 `open-investigation-round` transition request，经 runtime-operator 批准后打开 follow-up round。

新 round 会保留：

1. source round 的历史引用。
2. active hypotheses。
3. open challenges 转成 follow-up tasks。
4. 未完成 board tasks。
5. next actions 转成下一轮任务。
6. cross-round query hints。
7. round transition record 和 round task snapshot。

public/environment query 支持 `round_scope=current|up-to-current|all`，因此第二轮可以读取第一轮和当前轮的 normalized signals。source queue 也会记录 prior-round family memory，并支持受治理的 prior-round anchor。

## 5. Agent 权责

核心角色：

1. `moderator`
   - 主持议程、协调 board、提交 proposal/readiness、请求 phase transition。
2. `environmental-investigator`
   - 抓取、归一化、查询、分析环境与物理证据，提交 finding/proposal/readiness。
3. `public-discourse-investigator`
   - 调查公共讨论、媒体、社区表达与公众证据。
4. `formal-record-investigator`
   - 调查正式记录、监管材料、政策文本和 docket/comment。
5. `challenger`
   - 提交反证、开启 challenge/probe、质疑证据范围、taxonomy、时空匹配和结论表述。
6. `report-editor`
   - 基于 frozen basis 写报告，不改变调查状态。
7. `runtime-operator`
   - 管理审批、运行边界、归档、审计、恢复和重放，不做实质议会判断。

协作原则：

1. 自由文本可以解释理由，但权威状态必须落到 DB council object。
2. proposal/readiness/challenge/finding/evidence bundle 是议会推进主路径。
3. helper 输出默认不能直接成为报告结论，必须被 DB council/reporting basis 显式引用。
4. phase transition 由 moderator 请求，runtime-operator 批准，runtime kernel 执行。

## 6. 数据契约

主要 canonical planes：

1. `signal`
   - `public-discourse-signal`、`formal-comment-signal`、`environment-observation-signal`。
2. `analysis`
   - optional-analysis result sets、issue surfaces、typed projections、footprints、audit cues、sufficiency reviews。
3. `deliberation`
   - `finding`、`evidence-bundle`、`proposal`、`hypothesis`、`challenge`、`board-task`、`probe`、`readiness-opinion`、`readiness-assessment`、`report-basis-freeze`。
4. `runtime/control`
   - `transition-request`、`skill-approval`、`controller-state`、`gate-state`、`supervisor-state`、`runtime-control-freeze`、`orchestration-plan-step`。
5. `reporting`
   - `reporting-handoff`、`report-section-draft`、`council-decision`、`expert-report`、`final-publication`。

关键字段原则：

1. 每个重要对象必须能定位 `run_id`、`round_id`、object id。
2. 每个判断型对象必须保留 `evidence_refs`、`lineage`、`provenance`。
3. 每个 helper 输出必须标注 decision source、rule id、caveats、audit status。
4. 报告正文必须引用 finding/evidence bundle/proposal/report section/report basis 等 DB-backed basis。

## 7. 当前能力边界

已具备：

1. 受治理 run/round 生命周期。
2. 多源抓取、导入、归一化和 DB query。
3. 多 agent 议会对象写入与跨轮持久化。
4. proposal-authoritative 的议会推进路径。
5. report basis gate、freeze、reporting、archive/history。
6. approval-gated optional-analysis helper governance。

仍有限：

1. 专业环境模型较少，当前更适合“证据组织和审议”，不适合直接做强因果归因。
2. 部分历史命名仍保留，如 `report_basis_*`、legacy analysis kind query compatibility。
3. source queue 的 family memory 和 prior-round anchor 仍依赖部分 runtime artifact。
4. optional-analysis helper 多为启发式视图，默认 `audit-pending`，不是专业结论模型。

## 8. 后续开发计划

### 8.1 近期展示增强

优先做一个专业深案例，而不是扩展很多浅层 skill。

推荐 benchmark case：

`跨区域烟霾 / PM2.5 时空关系争议的多轮议会调查`

目标演示：

1. 第一轮收集公众讨论、正式记录、受体区空气质量和火点/气象背景。
2. 议会判断证据不足，提交 readiness opinion 和 follow-up proposal。
3. moderator 打开第二轮，补充上风向、下风向、时滞和候选源区证据。
4. challenger 质疑本地源、站点代表性、时间窗错配、气象条件不足。
5. 最终输出 withhold 或 cautious release 的报告，展示 evidence index、uncertainty、residual disputes。

### 8.2 通用关系基础设施

近期优先补 `spatiotemporal-relation` 基础设施，而不是新增 `transport-investigation` 窄 skill 包：

1. `spatiotemporal-relation-cue`
   - 作为 analysis plane canonical object，表达候选 source-target relation、lag window、spatial rule、rejection reason。
2. `signal_role` / `environment_signal_class`
   - 在 normalized environment signal metadata 和 DB index 中区分 source-event、receptor-observation、context-observation。
3. `detect-temporal-cooccurrence-cues`
   - 保留旧 same-day cue，同时在显式 scope 下输出 structured relation cue。
4. `query-spatiotemporal-relations`
   - 支持 relation_id、relation_status、source_signal_id、target_signal_id、source_role、target_role 查询。
5. relation-oriented challenger inputs
   - `open-challenge-ticket`、`open-falsification-probe`、`post-review-comment` 支持 relation_id、objection_code、challenged_rule、alternative_explanation、required_followup_evidence、report_risk。
6. `review-spatiotemporal-relation-alternatives`
   - 输出 relation objection candidates，必须经 challenge/probe/review comment 承接后才能进入后续报告链。
7. `materialize-spatiotemporal-relation-evidence-packet`
   - 默认把 relation cues、rejections、challenger objections 和 uncertainty register 写成 packet artifact。
   - 仅在显式 `--write-basis-objects` 下写入 finding/evidence bundle/report section draft 承接对象。

### 8.3 工程硬化

1. 将 spatiotemporal relation 相关对象纳入 canonical contract。
2. 为新 skill 建立最小端到端 fixture。
3. 补齐 DB-only recovery 测试。
4. 保持 optional-analysis helper 不直通 report basis。
5. 整理 legacy naming debt，但不要在论文展示前做大规模 breaking migration。

## 9. 文档地图

1. `docs/openclaw-project-overview.md`
   - 项目总览、主工作流、能力边界、后续计划。
2. `docs/openclaw-runtime-kernel-agent-refactor-checklist.md`
   - runtime kernel 与 agent council 架构。
3. `docs/openclaw-skills-refactor-checklist-v2.md`
   - skills 分层、能力矩阵和扩展规范。
4. `docs/openclaw-optional-analysis-skills-refactor-workplan.md`
   - optional-analysis helper 治理与可靠性边界。
5. `docs/openclaw-refactor-overall-notes.md`
   - 工程原则、运行护栏和论文展示建议。
