# OpenClaw 项目架构总览

## 1. 项目定位

OpenClaw 是一个面向生态环境争议调查的 DB-first 议会框架与运行时，支撑多 council-agent 协作。它的目标不是让模型直接给出不可审计的结论，而是把模型调查、证据抓取、反证挑战、阶段推进、报告冻结和最终发布组织成一条可追溯、可复核、可恢复的工作流。

一句话定位：

`一个受治理、DB-first、证据可审计的生态环境调查议会系统。`

核心分工：

1. `runtime kernel` 负责执行、权限、审批、ledger、receipt、replay、DB 持久化和 operator 可见状态。
2. `agent council` 负责实质性调查判断，包括 proposal、finding、evidence bundle、challenge、readiness opinion。
3. `database` 是议会状态、调查对象和报告依据的主要状态源。
4. `artifact` 是导出、handoff、调试和人类阅读材料，不作为唯一事实源。

### 概念模型和代码角色模型

必须显式区分两层，避免把 runtime 的治理主体误读为议会 agent：

1. 概念模型中，`runtime` 是维持议会可运行、可编排、可审计的框架。它提供给 human/operator 和被授权的顶层智能体使用，但自身不参与议会推理。
2. 概念模型中，`moderator` 才是议会的真正组织者，负责议题边界、board 协调、proposal/readiness 汇总和 phase transition 请求。
3. 代码角色模型中，`runtime-operator` 是 `actor_role`、审批主体和审计归因主体，用于授权 transition、skill approval、archive/replay/export 等运行面动作。它不是 council agent，也不做实质调查判断。
4. 代码角色模型现在只保留一个 `social-investigator` council agent 来承接公共讨论、社区表达、正式记录和政策材料。`public-discourse-signal` 与 `formal-comment-signal` 仍是不同数据类型，但不再对应两个独立 agent。

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

## 5. Council Agent 与 Runtime Principal

概念模型中的 council agents：

1. `moderator`
   - 议会组织者，主持议程、协调 board、提交 proposal/readiness、请求 phase transition。
2. `environmental-investigator`
   - 抓取、归一化、查询、分析环境与物理证据，提交 finding/proposal/readiness。
3. `social-investigator`
   - 调查公共讨论、媒体、社区表达、正式记录和政策材料。当前代码角色模型不再保留历史 `sociologist`、`public-discourse-investigator` 或 `formal-record-investigator` 入口。
4. `challenger`
   - 提交反证、开启 challenge/probe、质疑证据范围、taxonomy、时空匹配和结论表述。
5. `report-editor`
   - 基于 frozen basis 写报告，不改变调查状态。

代码模型中的 runtime principal：

1. `runtime-operator`
   - 管理审批、运行边界、归档、审计、恢复和重放；它是 runtime/control-plane 的授权主体和 ledger 归因主体，不是 council agent，也不做实质议会判断。

协作原则：

1. 自由文本可以解释理由，但权威状态必须落到 DB council object。
2. proposal/readiness/challenge/finding/evidence bundle 是议会推进主路径。
3. helper 输出默认不能直接成为报告结论，必须被 DB council/reporting basis 显式引用。
4. phase transition 由 moderator 请求，runtime-operator 批准，runtime kernel 执行；operator 批准程序性授权，不替代 moderator 的议会组织职责。

实用 runtime CLI 封装：

1. `start-council-run`
   - 由 `runtime-operator` 调用，一次完成 `init-run`、moderator 身份的 `scaffold-mission-run`、moderator 身份的 `prepare-round`、`materialize-agent-entry-gate`，并默认生成 OpenClaw agent 注册计划。
2. `materialize-openclaw-agent-registration`
   - 从当前 agent entry gate 生成 `openclaw agents add ...` 注册命令和 per-role workspace，不执行外部 agent turn。
3. 这些封装属于 runtime/kernel 操作，不封装为 skill；skill 继续用于有明确角色、输入输出契约和领域产物的工作单元。

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
3. 多 council-agent 议会对象写入与跨轮持久化。
4. proposal-authoritative 的议会推进路径。
5. report basis gate、freeze、reporting、archive/history。
6. approval-gated optional-analysis helper governance。

仍有限：

1. 专业环境模型较少，当前更适合“证据组织和审议”，不适合直接做强因果归因。
2. 部分历史命名仍保留，如 `report_basis_*`、legacy analysis kind query compatibility。
3. source queue 的 family memory 和 prior-round anchor 仍依赖部分 runtime artifact。
4. optional-analysis helper 多为启发式视图，默认 `audit-pending`，不是专业结论模型。

## 8. 当前收口状态

本轮重构已收口。已完成的工程面包括：

1. CI/quality gate 基线、targeted suites 和 full gate。
2. DB-only recovery、schema migration hardening 和 runtime-governed execution 的第一轮硬化。
3. runtime/kernel 命名清理、浅层包结构整理和大模块 package 化。
4. optional-analysis、spatiotemporal relation、canonical contracts、council/analysis objects 的 baseline 化。
5. archive/benchmark/replay 与 post-round/history bootstrap 的 package 化。

skills 当前形态可接受，不进入 P9 拆分。后续只在发现某个 skill 混入多个独立能力、输入契约或 artifact 家族时，才重新评估是否拆分；不会按行数拆 skill。

## 9. 文档地图

最终保留 5 个文档：

1. `docs/openclaw-project-overview.md`
   - 项目总览、主工作流、能力边界和当前收口状态。
2. `docs/openclaw-refactor-overall-notes.md`
   - 工程原则、重构收口摘要、剩余风险和论文展示建议。
3. `docs/openclaw-ci-quality-gates-workplan.md`
   - CI 与质量门的当前状态、命令和未闭环项。
4. `docs/openclaw-skills-refactor-checklist-v2.md`
   - skills 分层、原子能力边界、optional-analysis 降权和 relation baseline。
5. `docs/openclaw-case-study-evaluation-workplan.md`
   - 真实案例议会能力评测和可回放轨迹沉淀。
