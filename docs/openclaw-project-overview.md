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

### 工程护栏

1. `DB-first`
   - 议会状态、调查对象、报告依据优先写 DB；artifact 只作为导出、handoff 和调试材料。
2. `governed autonomy`
   - agent 可以自主调查和提议，但高影响执行必须受 role、approval、side-effect policy 约束。
3. `evidence-backed judgement`
   - finding、proposal、hypothesis、challenge、readiness、report section 必须引用 evidence refs。
4. `helper demotion`
   - optional-analysis/helper 默认是 audit/advisory，不是结论、证据权重或 phase gate。
5. `challenger first-class`
   - 反证、替代解释和不确定性必须是一等议会对象。
6. `report basis freeze`
   - 报告正文必须来自 frozen/reporting basis，而不是临时 helper artifact。
7. `thin runtime surfaces`
   - runtime 只展示权限、对象状态、refs、receipt、ledger 和可执行命令模板；不把 agenda、source 选择、证据权重或结论采信写成运行时规则。
8. `report-driven investigation`
   - 调查工作应由报告需要回答的 mission-driven claim slots 和 evidence contract 牵引，而不是先堆积材料再由 report-editor 拼接。报告蓝图约束证据需求，但不预设结论，不提供固定题型模板。
9. `program-aware rounds`
   - round 不应只是泛泛的 `round-001`、`round-002`。面向复杂形势分析时，前置 framing/scope council 应先形成 `council-investigation-program`，再把后续 round 组织成带有 `round_mode`、`round_category`、`round_title`、问题式 `round_subtitle_question`、active themes、agent responsibility boundaries、internal phases、exit criteria 和 supplemental continuation criteria 的议会程序。机器可读 `round_id` 保持文件系统安全；语义标题和轮次目的写入 round brief / transition / program 对象。数据获取和数据分析/综合通常是 issue council round 内不同的 agent work turns；只有当议题边界、责任边界或议会采信需要重新组织时，才升级为 supplemental council round。

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
   - source catalog、source governance、agent-led source acquisition proposal、fetch/import plan、detached fetch、raw artifact 管理。
3. `signal plane`
   - 将 public、formal、environment 三类输入统一归一化为 `normalized_signals` 和索引字段。
   - `formal` 不是“官方来源”的总称，而是政策、规制、许可、docket、agency notice、public comment 等正式记录文本。
   - `environment` 是物理环境观测或模型数据，如空气质量、气象、火点、水文；AirNow 这类官方监测仍归 `environment`，可在 metadata/provenance 中标注 official/provider。
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

`mission -> run/round -> framing/scope council -> council investigation program -> issue council rounds with acquisition/analysis turns -> in-round coverage feedback -> council deliberation -> supplemental issue rounds or readiness/gate -> report basis freeze -> reporting -> archive/history`

细化为：

1. `scaffold-mission-run`
   - 创建 run/round、mission、初始 board、round task scaffold。
2. `report framing / scoping`
   - 在开放型 mission 缺少完整 window/region/source requests 时，moderator 先提交 investigation plan、candidate scope、round brief 和 evidence request。面向形势分析报告的 run 还应形成 `report_blueprint`、`claim_slots`、`investigation_themes` 和 claim-basis 边界；report-editor 可参与定义报告问题，但不得提前写结论，也不得把 claim slots 固化成领域模板。
   - 前置 framing/scope council 不能只做粗粒度 claim 拆分。它应组织 report-editor、environmental-investigator、social-investigator、challenger 和 moderator 的 positions，形成一个可审计的 `council-investigation-program`：后续有哪些问题式议题、哪些 round 承接哪些议题、各 agent 在每轮的责任边界是什么、每轮内部有哪些 acquisition / analysis / progress review / moderator synthesis turns、何时满足、何时降级、何时开启 supplemental issue council。
3. `agent-led evidence acquisition`
   - investigator 根据 evidence request、report claim slots、finding、challenge 和自身判断提出或执行取证动作；runtime 负责权限、side-effect approval、receipt 和 ledger，不替 agent 排序 source 或采信证据。moderator/report-editor 可以提出主题和 evidence need；前置 theme plan 只能记录证据义务、分母义务、成功条件、恢复路径和降级边界，不能以任何形式预填 source family、source skill、query variant、query parameters 或 route ranking。具体取证路线只能由 investigator 在正式 acquisition turn 中自主形成。
4. `prepare-round`
   - 根据 mission、round tasks、source governance 和已有 coordination context 生成可审计 fetch plan。该 plan 是运行面材料，不替 agent 采信证据。
5. `fetch/import + normalize`
   - 抓取或导入 raw artifact，并通过对应 normalizer 写入 signal plane。
6. `query + in-round coverage feedback`
   - investigator 通过 public/formal/environment/raw/normalized query surfaces 获取 item-level evidence basis。
   - 对舆情、正式记录、政策动作和大体量环境数据，agent 应在当前 round 内进行小批量抓取、coverage checkpoint、query/窗口/来源恢复和 challenger 快速质疑；只有达到可讨论状态或明确 source-limit 后再提交议会收口。checkpoint 只在结果影响 claim strength、降级表述或恢复路径时记录，不能变成每次 tool call 的表单负担。
7. `council write`
   - agent 提交 finding、evidence bundle、proposal、review comment、challenge、hypothesis、readiness opinion。
8. `optional analysis`
   - 经 operator approval 后运行 helper，输出审计视图、证据覆盖摘要、议题线索、footprint、temporal cue、sufficiency note 等。
9. `phase control`
   - controller/gate/supervisor 读取 DB council objects 和 round liveness surface，判断是否继续调查、打开新 round、冻结 report basis 或阻断报告。
10. `reporting`
   - report editor 基于 frozen report basis 和 DB reporting objects 生成 handoff、decision、expert report、final publication。
11. `archive/history`
   - close-round 或 checkpoint 后归档 signal/case，并可为新 run / 新 round 物化 history context；history 只提供 archived evidence refs 和 match surfaces，不生成当前 run 结论。

## 4. 多轮调查能力

系统支持分批取证和补充议题轮。若议会认为证据不足，moderator 可以发起 `open-investigation-round` transition request，经 runtime-operator 批准后打开 follow-up round 或 supplemental issue council round。普通 query repair、同源补采或小批量分析优先作为当前 issue council round 内的 agent work turn，而不是自动升级为新 round。

新 round 会保留：

1. source round 的历史引用。
2. active hypotheses。
3. open challenges 转成 follow-up tasks。
4. 未完成 board tasks。
5. next actions 转成下一轮任务。
6. cross-round query hints。
7. round transition record 和 round task snapshot。

public/environment query 支持 `round_scope=current|up-to-current|all`，因此第二轮可以读取第一轮和当前轮的 normalized signals。source queue 也会记录 prior-round family memory，并支持受治理的 prior-round anchor。

Round 是议会形成阶段性判断的单位，不是 API 重试、query variant 扩展或低量恢复的单位。若一个主题在当前 round 内经过初次抓取后明显不足，优先走 in-round 反馈闭环：source owner 修正查询和来源路径，challenger 快速审查覆盖和外推风险，moderator 只在需要改变主题边界、接受 source-limit 或打开新调查主题时介入。只有当当前 round 内没有合理恢复路径、或主题需要跨 agent 重定边界时，才打开 follow-up round。sufficiency review 只说明可支撑、必须降级和缺 basis 的 claim，不是 runtime 判真、证据打分或自动 report-ready 机制。

程序化 round 命名采用多层语义：

1. `round_id`
   - 文件系统和 DB 友好的稳定 id，例如 `round-001-framing-scope`、`round-002-public-semantic`、`round-003-public-semantic-supplement-01`。
2. `round_title` / `round_subtitle_question`
   - 写入 round brief、transition 和 program。subtitle 优先采用问题形式，例如“哪些证据需要被取得？”、“已取得的数据能支持哪些 claim？”。
3. `round_mode` / `round_category`
   - 写入 round brief、transition 和 program，例如 `framing-scope-council`、`issue-council`、`supplemental-issue-council`、`report-writing`。
4. `round_internal_phases`
   - 写入 round brief 或 program，用于区分 acquisition turn、analysis turn、progress review 和 moderator synthesis。数据获取和数据分析/综合不应被伪装成同一种“调查轮”；它们通常是 issue council round 内不同组织环节。

`open-investigation-round` 已支持 `round_mode`、`program_id`、active themes、`round_category`、`round_internal_phases`、agent responsibility boundaries、parent theme-progress review refs、`primary_focus_refs`、`context_packet_id` 和 `round_brief_id` 作为 continuation context；`prepare-round` 和 agent entry gate 会把这些字段合并为 program-aware coordination context。它们只作为 handoff surface，不得变成 source family / source skill / query / route ranking / scheduler queue 或 evidence uptake gate。

当前已完成的基线是：开放型 mission 可以保持 `scoping-required`，moderator 可以在 scoping round 中提交 investigation plan、candidate scope、round brief、round synthesis、evidence request 和 context packet；framing/scope round 可通过 report blueprint、agent positions 和 `synthesize-council-investigation-program` 生成 DB-backed `council-investigation-program`、projected round briefs 和 operator-facing `human_review_packet`，用于人工检查 mission question、缺席角色、issue-round sequence、下一轮建议和非 scheduler 边界；investigator 可以通过薄 `source-acquisition-proposal` 自主记录取证意图，并通过 execution lineage helper 把 proposal 与 receipt / normalized signal refs 串联；agent entry 和 source surfaces 会暴露同一信源家族内的可选多层工作流（例如 GDELT DOC recon 到 Events/Mentions/GKG，YouTube video search 到 comments，Regulations.gov list 到 detail），并在 role entry 中暴露当前 program question、active themes、role responsibility boundaries、theme-progress disposition 和 supplemental parent refs，但这些 workflow 和 context 不排序、不评分、不固定议程、不限制 investigator 路线；round liveness 可把 unresolved refs 带入 continuation round，并提供不排序的 closing checklist；失败、blocked、receipt-only、executed-without-normalized-refs 或 zero-signal acquisition attempt 必须被 source owner 反思后，moderator 才能把 `no-actionable-path` 作为非继续理由；弱报告允许生成，但必须显式记录 claim strength、limitations、unresolved refs 和不继续调查的理由，不能把检索失败当成过早收口的依据；archive/history 可在 checkpoint 后暴露历史 evidence refs。source-family workflow 的常驻说明见 `docs/openclaw-source-family-workflows.md`；claim-strength 收口义务见 `docs/openclaw-claim-strength-obligations.md`。

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
   - 参与 report framing，帮助把 mission 拆成报告问题和 claim slots；在 reporting 阶段基于 frozen basis、agent section brief 和 DB reporting objects 写报告，不改变调查状态。

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
   - `formal-comment-signal` 面向正式程序/政策记录；`environment-observation-signal` 面向物理观测。官方环境监测不因“官方”而进入 formal plane。
2. `analysis`
   - optional-analysis result sets、issue surfaces、typed projections、footprints、audit cues、sufficiency reviews。
3. `deliberation`
   - `finding`、`evidence-bundle`、`proposal`、`hypothesis`、`challenge`、`board-task`、`probe`、`readiness-opinion`、`readiness-assessment`、`report-basis-freeze`。
4. `runtime/control`
   - `transition-request`、`skill-approval`、`controller-state`、`gate-state`、`supervisor-state`、`runtime-control-freeze`、`orchestration-plan-step`。
5. `reporting`
   - `agent-section-brief`、`situation-analysis-brief`、`reporting-handoff`、`report-section-draft`、`council-decision`、`expert-report`、`final-publication`。
   - `situation-analysis-brief` 位于 agent section brief 和 narrative report 之间，只综合已经被 section brief、frozen basis、council object、accepted review 或 challenger boundary 承接的材料；它不是新数据源、调查轮、runtime gate 或报告正文。

关键字段原则：

1. 每个重要对象必须能定位 `run_id`、`round_id`、object id。
2. 每个判断型对象必须保留 `evidence_refs`、`lineage`、`provenance`。
3. 每个 helper 输出必须标注 source/provenance、caveats、audit status；helper 不提供强制采信结论。
4. 报告正文必须引用 finding/evidence bundle/proposal/report section/report basis 等 DB-backed basis。

### Skill 分层与治理边界

当前 skills 按职责分为：

1. `fetch`
   - 只抓取或导入 raw provider payload / runtime-captured artifact；不写 board judgement、不判断 claim true/false。
2. `normalize`
   - 只把 raw artifact 转为 `normalized_signals`，保留 provenance、artifact ref、record locator、quality flags 和 metadata；不做事实判断或 readiness。
3. `query`
   - 返回 item-level `evidence_refs`、`evidence_basis` 和 source provenance，供 investigator 写 finding/evidence bundle。
4. `optional-analysis`
   - approval-gated advisory helper view，可帮助审视覆盖、议题、footprint、temporal cue、sufficiency note；默认不能直接作为 phase gate 或 report basis。
5. `deliberation-write`
   - 才能把调查判断写成议会对象，例如 finding、evidence request、source-acquisition proposal、challenge、readiness opinion。
6. `reporting`
   - 只能消费 frozen/reporting basis，不回写调查状态。
7. `state-transition`
   - 必须经 moderator transition request 和 runtime-operator approval，不走普通 skill approval。

普通合法 fetch 不强制先有 proposal；`source-acquisition-proposal` 只在需要跨 agent 协调、审批、跨轮承接或显式议会记录时使用。source catalog 和 role source surface 是权限面，不是 runtime-selected source queue。

### 公共舆情深化 Lane

公共舆情深化是 `optional deepening lane`，不是新的 round type。它可在 round brief、round synthesis、evidence request 或 continuation 的 `primary_focus_refs` 中被 moderator 或 investigator 显式提出，语义是从已采集或待采集的公共文本样本中分析样本内议题、情绪、媒体 tone 和公共来源叙事。

该 lane 的治理边界：

1. YouTube comments、Bluesky posts/replies、formal comments 等文本样本可用于 `social_sample_affect`，但只能声明样本内结果。
2. GDELT DOC Search 有两种不同表面：`artlist` 等文章结果属于 `gdelt_doc_recon`，用于发现新闻/公共记录线索；`tone` / `toneabs` 查询、`timelinetone`、`tonechart` 和 tone 排序属于 `gdelt_doc_tone_aggregate`。GDELT Events/Mentions/GKG 的数值 tone 属于 `gdelt_media_tone`。两类 GDELT tone 都只能描述媒体/文档语气，不能直接代表公众情绪。
3. GDELT GKG `V2Tone` 已拆出 tone、positive、negative、polarity、activity density、self/group reference density、word count 等分量，并保留结构化 `GCAM` cue；这些仍是 provider/media-document cue，不是公众表达情绪。
4. 公共来源叙事和物理来源归因必须分开；前者由 `social-investigator` 调查，后者必须由 `environmental-investigator` 验证。
5. 情感/立场标签由 `classify-public-discourse-affect` 这样的 bounded annotation worker 产出；`social-investigator` 负责样本范围、annotation basis 和议会采信，不直接承担逐条情感标签作者身份。
6. `challenger` 对舆情标注的默认职责是样本边界、taxonomy fit、ambiguous clusters、outlier examples 和报告措辞审计；不要求逐条复核所有非 GDELT 情感标签。只有报告要引用争议样本、强影响结论或可见讽刺/转述/翻译歧义时，才需要局部 item-level 复核。
7. 当前已具备 corpus materialization、coverage audit、annotation worker、annotation aggregation、GDELT tone enrichment、cross-source comparison 和 sample summary 的初步闭环；这些输出都保持 optional-analysis/advisory 属性，必须由议会对象承接后才能进入 report basis。

### 公共政策形势分析 Lane

面向毕业设计和公共政策形势分析的报告链采用四条主 lane 和一个报告综合层。这些 lane 是 agent 可调用、可组合的能力链，不是 runtime 固定议程。

1. `fact / official action lane`
   - 获取、归一化、查询和聚合环境事实、运行记录、官方行动和正式政策节点。
   - 输出事实/官方动作时间线、环境过程摘要和 claim boundary。
   - 环境指标不能直接推出政策成败、责任归属或公众态度；官方记录能证明制度动作存在，不自动证明政策有效性。
2. `public-policy corpus lane`
   - 为媒体材料、公众样本、正式政策文本和正式评论建立 corpus。
   - 记录 query variants、source family、window、eligible count、dedup count、failed/zero/low-volume rationale。
   - 没有 corpus 和 coverage audit 的公共语义只能作为个例或线索，不能写成样本结构判断。
3. `semantic perception lane`
   - 对公众样本、媒体文本和正式政策文本做 bounded semantic labels。
   - 聚合样本内议题、情绪、来源叙事、责任归因、政策诉求、信任/不信任和不确定性表达。
   - GDELT tone 是媒体/文档语气，不是公众情绪；source narrative 是公共来源叙事，不是物理来源归因。
4. `interaction timeline lane`
   - 将事实节点、官方动作、媒体可见性和公众/政策语义放入同一时间坐标。
   - 互动判断必须先有各 lane 自己整理的 episode cards，再做时间编排；同日可见不能写成因果、政策效果或公众反应归因。
5. `policy evaluation basis`
   - 不是独立调查 lane，而是 moderator、challenger 和 report-editor 基于前四条 lane 与 frozen basis 形成的综合产物。
   - 它只能说明哪些材料可作为政策沟通、公众参与、风险治理或政策回应评估的依据，不能无证据给政策成败评分，也不能被实现成单独抓取一种“政策评估数据”的 lane。

报告 claim 的最低 basis 采用软义务：如果报告要写公众比例，必须有 corpus、coverage audit、annotation、aggregation、denominator 和非代表性边界；如果要写事实/政策/公共语义互动，必须有 interaction timeline、lane episode cards 和至少两类 evidence refs；如果要写政策评估依据，必须有事实/行动证据、公共-政策语义证据、claim boundary 和 challenger review。弱报告允许生成，但必须降级表述并说明缺失 basis。

### Report Framing 与 Agent Section Brief

面向形势分析的调查应从 `report_blueprint` 开始。该 blueprint 不预设答案，而是把 mission 拆成可调查的 `claim_slots` 和 `investigation_themes`。claim slots 是 mission-driven 的待回答问题槽，不是固定题型模板。典型主题包括事实过程、官方/政策行动、公共舆情语义、媒体/政策框架和互动时间线；政策评估依据属于报告综合层，不是单独 acquisition theme。

各 agent 在报告前应提交或参与生成自己的 section brief：

1. `section_role`
2. `main_claims`
3. `evidence_refs`
4. `source_families`
5. `claim_strength`
6. `denominators`
7. `limitations`
8. `recommended_report_use`
9. `blocked_phrases`

report-editor 可以组织语言、调整结构和提升可读性，但新增的实质 claim 必须能回溯到 section brief、frozen basis 或 council object。challenger 标记为 unsupported 的句子必须修改、降级、删除，或由 moderator 决定是否继续调查。

### 环境证据压缩 Lane

环境证据压缩是 `optional analysis/helper` 能力，不是新的 round type，也不是专业环境模型。它用于把 AirNow、OpenAQ、Open-Meteo、USGS、NASA FIRMS 等 normalized environment signals 压成可读的覆盖和统计摘要，帮助 agent 避免直接读取百万级原始记录。

该 lane 的治理边界：

1. `query-environment-signals` 返回 item-level 环境证据 rows 和 evidence refs，适合抽查和写 finding 时引用。
2. `aggregate-environment-evidence` 负责描述性覆盖和统计摘要；下一阶段面向 runtime 议会程序加固的工程计划见 `docs/openclaw-runtime-council-program-upgrade-plan.md`。
3. 环境聚合只输出 source/metric/time/spatial coverage、数值 min/max/mean、bucket counts、缺测和样本 refs。
4. 时序观测和点事件应按数据形态处理，而不是按 provider 写厚规则。
5. 环境聚合不得输出风险等级、source 排序、证据权重、健康暴露评估、水资源短缺严重性、火源证明或输送/归因结论。
6. 聚合摘要必须由 `environmental-investigator` 的 finding、evidence bundle、readiness opinion 或 moderator synthesis 显式承接后，才能进入报告依据。

## 7. 当前能力边界

已具备：

1. 受治理 run/round 生命周期。
2. 多源抓取、导入、归一化和 DB query。
3. 多 council-agent 议会对象写入与跨轮持久化。
4. proposal-authoritative 的议会推进路径。
5. report basis gate、freeze、reporting、archive/history。
6. approval-gated optional-analysis helper governance。
7. 环境侧已有 `query-environment-signals` 和基础 `aggregate-environment-evidence`，可做 item-level 查询和描述性覆盖摘要。
8. 已有 claim-gap action cards、公共语义 corpus/coverage/annotation/aggregation、interaction timeline、reporting handoff 和 narrative validator 的初步 typed artifact 链路。narrative validator 会检查 situation-analysis brief 是否存在、是否保留 mission-answerable question、central bounded judgement、report spine、事实/官方行动/公共语义/政策语义/互动链条索引、正文是否可见消费 brief 主线、未完成 claim boundary 索引、指令残留、runtime 元话语密度、强 claim 上游 basis 和政策评估显式 basis。

仍有限：

1. 专业环境模型较少，当前更适合“证据组织和审议”，不适合直接做强因果归因。
2. 部分历史命名仍保留，如 `report_basis_*`、legacy analysis kind query compatibility。
3. 部分历史命名和 artifact 仍保留旧语义，如运行记录里的 `source_selection_*`；当前能力入口以 role allowed source surface + agent-led source acquisition proposal 为准。
4. finding 到 evidence bundle / hypothesis / proposal / next round 的承接已有对象局部 command templates，但真实 agent uptake 仍需更多案例验证。
5. archive/history 已能 checkpoint 并提供历史 evidence refs；更大规模 raw receipt cache 和跨案例复用策略仍需后续设计。
6. optional-analysis helper 多为启发式视图，默认 `audit-pending`，不是专业结论模型。
7. 现有环境聚合层仍偏覆盖摘要；对百万级时序数据需要补齐全量统计、时序 bucket、点事件摘要和小型 evidence-ref sample 输出。
8. report-driven investigation 已具备 framing/program/round brief、theme progress review、supplemental transition context、agent section brief、situation-analysis brief、reporting handoff、narrative draft 和 validator 的最小代码链路。当前工程验收只使用 fixture、backtest 和 smoke harness，不以生产方式运行真实议会；真实 agent uptake 和更宽案例覆盖仍需后续非生产回归。

## 8. 当前收口状态

本轮重构已收口。已完成的工程面包括：

1. CI/quality gate 基线、targeted suites 和 full gate。
2. DB-only recovery、schema migration hardening 和 runtime-governed execution 的第一轮硬化。
3. runtime/kernel 命名清理、浅层包结构整理和大模块 package 化。
4. optional-analysis、spatiotemporal relation、canonical contracts、council/analysis objects 的 baseline 化。
5. archive/benchmark/replay 与 post-round/history bootstrap 的 package 化。
6. agent-led source acquisition、source execution lineage、round synthesis、round liveness continuation / closing checklist、claim-strength obligation、approval handoff、archive checkpoint/history context 和 receipt-only normalization hints 已进入运行面基线；runtime 只展示对象、refs、权限和命令模板，不生成 source 排序、evidence 权重或固定调查剧本。

skills 不按行数或目录数量机械拆分，但当前平铺目录已经造成检索和理解成本。下一阶段会在不改变公共 `skill_name` 的前提下，为 registry 增加 category/family/stage metadata 和递归 discovery，再按 planning、runtime-state、fetch、normalize、query、optional-analysis、deliberation-write、reporting、archive-history 等职责整理物理目录。只有当某个 skill 混入多个独立 side-effect、输入契约、DB plane 或 artifact 家族时，才进一步合并或拆分能力。

## 9. 文档地图

当前 docs 以基础文档为主；历史工作计划、迁移清单、真实 run timeline 和临时审计计划的有效常驻内容已并入基础文档。当前代码层工程收口以 runtime 议会程序加固工作计划为准；实验和案例安排单独记录，不混入代码开发计划。

1. `docs/openclaw-project-overview.md`
   - 项目定位、概念模型/代码角色模型、主工作流、多轮能力、agent/runtime principal、数据契约、skill 分层、公共舆情深化 lane、当前能力边界和质量门。
2. `docs/openclaw-source-family-workflows.md`
   - fetch skill 的多层工作流说明；用于帮助 agent 理解同一信源家族内的 search/detail/table/backfill 关系，不作为 source 排序或议程脚本。
3. `docs/openclaw-claim-strength-obligations.md`
   - 弱报告、强 claim 和 unresolved refs 收口边界；用于防止过早放弃调查，同时不引入议题模板或证据打分。
4. `docs/openclaw-experiment-case-plan.md`
   - 毕业设计实验与案例计划；记录两主案例、两轻量验证、第二主案例风险对比、降级策略和展示材料安排。
5. `docs/openclaw-runtime-council-program-upgrade-plan.md`
   - 当前工程工作计划；聚焦 framing/scope council、council investigation program、program-aware issue round brief、supplemental issue council、theme progress review、agent section brief、situation-analysis brief 和报告回归验收。
6. `docs/frozen-case-packages/nyc-smoke-20230607/baseline-case-package.md`
   - NYC smoke 第一主案例冻结包；汇总 mission、最终报告、证据链、claim boundary、公共舆情样本结构和答辩可引用的基线结论。
7. `docs/frozen-case-packages/nyc-smoke-20230607/defense-onepager.md`
   - NYC smoke 答辩一页纸；用于快速展示案例定位、议会流程、环境证据、公共语义样本和边界表述。
8. `docs/frozen-case-packages/colorado-river-glen-canyon-2023/baseline-case-package.md`
   - Colorado River / Glen Canyon 第二主案例冻结包；汇总 mission、治理记录、水文/运行信号、公共语义和报告边界。
9. `docs/frozen-case-packages/colorado-river-glen-canyon-2023/defense-onepager.md`
   - Colorado River / Glen Canyon 答辩一页纸；用于展示慢变量环境治理争议案例。

质量门基线命令：

1. `python3 tools/quality_gate.py syntax`
2. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting case-study`
3. `python3 tools/quality_gate.py full`

质量门边界：

1. `tools/quality_gate.py` 提供 `syntax`、`test`、`full`、`ci`、`list` 子命令。
2. `syntax` gate 使用 AST parse，并阻断重复字面量 dict key。
3. 默认质量门不依赖真实外部 API 或 secrets。
4. case-study replay 只在真实运行抽取稳定 fixture 后作为固定回放门。
