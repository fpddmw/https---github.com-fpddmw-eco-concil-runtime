# OpenClaw 公共政策形势分析升级计划

文档性质：本文是 OpenClaw 毕业设计提交前的工程升级计划，目标是把系统从“证据可审计的环境事件报告链路”升级为“面向生态环境形势分析的事实核查、舆情语义感知、政策语义互动和政策评估依据生成链路”。

本文不是固定调查脚本，不是 runtime gate，不是 source 排序规则，也不要求所有 case 走同一套议程。它定义的是：当报告想提出某类 claim 时，系统应如何帮助 agent 找到可行动证据路径，并在报告层阻止没有 basis 的越界表述。

## 1. 核心目标

升级后的报告应围绕用户 mission 形成一条可审计论证链：

`事实核查 -> 事实/官方动作时间线 -> 公共/政策语义样本 -> 事实、政策与公共语义互动 -> 政策评估依据 -> 可审计中文报告`

报告需要回答：

1. 事件或治理争议发生了什么，哪些事实可核查。
2. 环境数据、正式政策记录、媒体材料和公众样本分别能证明什么、不能证明什么。
3. 媒体、公众样本、正式政策记录如何描述问题、责任、风险、政策选择和不确定性。
4. 公共语义和正式政策语义在哪里一致、错位或互补。
5. 这些证据能为风险沟通、公众参与、政策回应和政策有效性评估提供哪些依据。
6. 哪些结论仍不能被当前证据支持，尤其不能把样本内表达写成总体民意。

## 2. 不变的治理边界

1. `runtime kernel` 只负责执行、权限、审批、receipt、ledger、DB 持久化和 operator 可见状态。
2. `runtime` 不选择 source，不固定议程，不给证据排序、打分或设置权重。
3. `moderator` 是议会组织者，负责议题边界、round synthesis、readiness、continuation / closure 判断。
4. `investigators` 保留自主调查权；系统只能暴露可行动路径和 claim gap，不替 agent 采信证据。
5. `challenger` 审查外推、归因、代表性、政策责任和 unsupported claim。
6. `report-editor` 只能消费 frozen/reporting basis、section brief 或 council object，不新增事实。
7. optional-analysis/helper artifact 默认是 advisory，必须被 finding、evidence bundle、readiness opinion、round synthesis、reporting handoff 或 report basis 承接后才能进入报告主文。

## 3. 能力架构：四条主 Lane + 报告综合层

Lane 是 agent 可调用和推进的能力链，不是 runtime 固定议程。每条 lane 可以被 moderator 或 investigator 在 round brief、evidence request、finding、challenge、readiness 或 continuation 中显式提出。

### 3.1 Fact / Official Action Lane

职责：

1. 获取、归一化、查询和聚合环境事实、运行记录、官方行动和正式政策节点。
2. 构建事实与官方动作时间线。
3. 说明事实支持范围和不能升级的 claim。

典型材料：

1. AirNow、OpenAQ、Open-Meteo、USGS、USBR RISE、NASA FIRMS 等环境/运行数据。
2. Federal Register、USBR、DOI、EPA、agency notice、public involvement page 等正式记录。
3. 官方健康建议、风险沟通公告、政策动作、会议和程序节点。

关键输出：

1. `verified_fact_timeline`
2. `environment_process_summary`
3. `official_action_timeline`
4. `fact_claim_boundary`

边界：

1. 环境指标不能直接推出政策成败、责任归属或公众态度。
2. 官方记录能证明制度动作或程序入口存在，不自动证明政策有效性或共识。
3. 对来源归因、传输链、政策责任等强 claim，必须保留替代解释和未证边界。

### 3.2 Public-Policy Corpus Lane

职责：

1. 为公共讨论、媒体材料、正式政策文本和正式评论材料建立 corpus。
2. 记录 query variants、source family、window、eligible count、dedup count、failed/zero/low-volume rationale。
3. 形成覆盖审计，说明样本能代表什么、不能代表什么。

关键输出：

1. `public_policy_corpus_plan`
2. `query_variant_pack`
3. `materialized_public_policy_corpus`
4. `public_policy_corpus_coverage_audit`
5. `source_limit_rationale`

边界：

1. 没有 corpus audit 的公共语义只能作为个例或线索，不能进入报告主文的样本结构判断。
2. GDELT、YouTube、Bluesky、formal comments 不得混用 denominator。
3. API 不足时，系统输出 source-limit rationale，不能编造宏观舆情。

### 3.3 Semantic Perception Lane

职责：

1. 对公众样本、媒体文本和正式政策文本分别做 bounded semantic labels。
2. 聚合样本内议题、情绪、来源叙事、责任归因、政策诉求、信任/不信任、不确定性表达。
3. 保持 source family、sample definition 和 denominator 隔离。

关键输出：

1. `public_semantic_annotations`
2. `policy_semantic_annotations`
3. `media_tone_summary`
4. `semantic_aggregate`
5. `cross_source_semantic_comparison`

边界：

1. GDELT tone 是媒体/文档语气，不是公众情绪。
2. YouTube / Bluesky / comments 等平台样本只能声明样本内结构。
3. 非互斥标签必须说明，不能相加解释为 100% 的意见组成。
4. source narrative 是公共来源叙事，不是物理来源归因。

### 3.4 Interaction Timeline Lane

职责：

1. 将事实节点、官方动作、媒体可见性和公众/政策语义放在同一时间线上。
2. 分析语义如何随事件事实、政策动作和信息发布发生阶段性变化。
3. 标出事实语义错位、政策回应缺口、风险沟通节点和不确定性。

关键输出：

1. `fact_policy_public_interaction_timeline`
2. `semantic_shift_events`
3. `communication_gap_notes`
4. `misalignment_and_uncertainty_register`

边界：

1. 时间相关性不能写成因果。
2. 每个“互动”判断至少应引用事实/政策侧和公共/媒体侧证据。
3. 若只有单侧证据，只能写成待验证线索或报告限制。

### 3.5 报告综合层：Policy Evaluation Basis

`policy_evaluation_basis` 不作为独立调查 lane。它是 moderator / challenger / report-editor 基于前四条 lane 和 frozen basis 形成的综合产物。

允许输出：

1. 哪些证据可用于评估政策沟通、公众参与、风险治理或政策回应。
2. 哪些评估维度已有材料支持。
3. 哪些维度只能作为后续调查方向。
4. 哪些 claim 不能写成政策有效/无效或责任结论。

禁止输出：

1. 无证据的政策成败评分。
2. 总体公众态度断言。
3. 把样本内情绪、媒体 tone 或正式记录直接写成政策效果。

## 4. Claim-Basis Soft Obligations

以下是报告 claim 的软义务，不是固定调查议程。若报告不写对应 claim，可以不运行对应分析；若报告要写对应 claim，则必须有 basis，否则 report validator 应要求降级、删除或补证。

| 报告 claim | 需要的最低 basis | 不满足时的写法 |
| --- | --- | --- |
| 公众样本中某类情绪/议题/来源叙事比例 | corpus、coverage audit、annotation、aggregation、denominator、非代表性边界 | 只能写个例或小样本线索 |
| 正式评论主要争点/关切/立场线索 | candidate audit、可读 comment/detail/attachment text、issue classification、aggregation | 只能写找到了候选评论入口 |
| 环境趋势/峰值/运行状态 | `aggregate-environment-evidence` 或等价统计摘要；否则 item-level boundary | 只能写具体证据例子 |
| 事实、政策动作和公共语义互动 | interaction timeline，且每个节点有至少两类 refs | 只能写并列时间线或待验证线索 |
| 来源归因、因果、传输链、政策责任 | normalized refs、relation/fact-check/challenger review、alternatives/limitations | 写成相容性、线索或仍需验证 |
| 政策沟通/参与/回应评估依据 | fact/action evidence、public-policy semantic evidence、claim boundary、challenger review | 只能写后续可评估维度 |

## 5. Skill 推荐机制：Claim-Gap Action Cards

当前问题不是缺少 `Skill.md`，而是 agent 很难稳定理解“什么时候用哪个 skill、用完接什么、失败后怎么恢复、不能证明什么”。因此推荐机制应从自然语言文档升级为机器可读 advisory surface。

推荐机制名称建议：

`materialize-claim-gap-action-cards`

它不是 scheduler，不是 gate，不排序，不打分，不自动执行。它只输出可行动卡片，帮助 moderator 和 investigators 看见 claim gap。

输入：

1. mission focus。
2. 当前 round / prior round council objects。
3. normalized signal counts。
4. existing helper artifacts。
5. failed / zero / low-volume / receipt-only acquisition attempts。
6. open challenges。
7. report readiness gaps。

输出 action card：

1. `claim_gap`
2. `why_it_matters`
3. `candidate_skills`
4. `required_inputs`
5. `expected_artifacts`
6. `if_not_done_report_boundary`
7. `owner_role_suggestions`

验收：

1. 输出多个并列行动卡，不输出优先级排名。
2. 不自动触发 skill。
3. operator 只批准高影响动作和记录边界，不手动告诉 agent 查什么。
4. investigator / moderator 可以选择采纳、拒绝或改写 action card，并应写入 council object。

## 6. Skill 契约字段

每个关键 skill 应逐步声明以下 metadata。该 metadata 用于 action cards、agent entry surface、report validation 和文档生成，不用于 runtime source 排序。

1. `observes`：该 skill 能观察什么。
2. `cannot_prove`：该 skill 不能证明什么。
3. `requires`：前置输入、corpus、artifact 或 DB state。
4. `produces`：结构化 artifact kind、DB object、normalized signal 或 advisory summary。
5. `followups`：正常输出后的候选后续 skill。
6. `failure_recovery`：zero / failed / low-volume / receipt-only 后的恢复路径。
7. `claim_limits`：报告中允许和禁止的 claim。
8. `report_uses`：输出可支持哪些报告用途。
9. `owner_roles`：哪些 agent 可主导使用。

验收：

1. Agent entry surface 能展示当前 skill 的 followups 和 failure recovery。
2. fetch skill 的 zero-result 不直接终止 source family，至少产生 recovery action card 或 source-limit record。
3. public-policy 相关 helper 输出 typed artifacts，不能只输出自由文本。

## 7. Advisory Lane State

Lane state 只帮助 agent 理解当前缺口，不作为 runtime hard gate。

建议状态：

1. `unscoped`
2. `corpus-planned`
3. `acquisition-attempted`
4. `corpus-materialized`
5. `coverage-audited`
6. `annotated`
7. `aggregated`
8. `interaction-timeline-built`
9. `section-brief-ready`
10. `report-basis-carried`

使用规则：

1. 如果报告要写样本内语义比例，状态应至少达到 `aggregated`，否则必须降级表述。
2. 如果报告要写事实/政策/公共语义互动，状态应达到 `interaction-timeline-built`，否则只能写并列时间线或待验证关系。
3. 如果 helper 结果未被 council object 或 report basis 承接，状态不能视为 `report-basis-carried`。
4. 弱报告允许生成，但必须说明缺少哪些 state 和对应 claim 限制。

## 8. Skill 调整范围

原则：能重构现有 skill 就不新增平行 skill。新增只用于确实没有现有职责承接的新能力。

### 8.1 优先重构现有 Skills

| 现有 skill | 重构方向 | 备注 |
| --- | --- | --- |
| `materialize-public-discourse-corpus` | 扩展为 public-policy corpus 的物化基础，支持媒体、公众样本、正式政策文本的 source-family metadata | 不急于新增 `materialize-public-policy-corpus` |
| `audit-public-discourse-sample-coverage` | 扩展 query variants、failed/zero/low-volume rationale、source family coverage、denominator | 作为 corpus coverage 主路径 |
| `classify-public-discourse-affect` | 扩展为 bounded semantic labels：affect、issue frame、source narrative、policy demand、trust/confidence、uncertainty、responsibility attribution | 可保留旧名，也可加 alias |
| `aggregate-public-discourse-annotations` | 扩展 denominator 隔离、source-family 分组、非互斥标签说明 | 支撑样本内比例 |
| `compare-formal-public-footprints` | 扩展为 formal-public semantic comparison | 不新增平行 comparison skill |
| `aggregate-environment-evidence` | 增强事实/官方动作 lane 的环境和运行摘要支持 | 仍保持描述性，不能做风险等级或归因 |
| `draft-narrative-report` | 重构为 situation-analysis report 的主写作路径 | 不新增平行 compose skill |
| `validate-narrative-report` | 扩展 claim-basis validation：denominator、formal comment corpus、environment aggregate、interaction timeline、policy basis | 不新增平行 validator |
| `materialize-reporting-handoff` | 承接 section brief、claim gap、helper carried status | 保持 report basis 边界 |

### 8.2 建议新增 Skills

| 新 skill | 理由 | 边界 |
| --- | --- | --- |
| `materialize-claim-gap-action-cards` | 跨 lane advisory surface，现有 skill 无法自然承接 | 不排序、不调度、不自动执行 |
| `build-fact-policy-public-interaction-timeline` | 新的核心分析能力，用于事实、政策动作、媒体/公众语义互动 | 只输出时间线、相邻关系和 limitations，不做因果断言 |
| `draft-agent-section-brief` | 让各 agent 向 report-editor 提供可审计 brief，减少报告胡编 | 可先作为 reporting handoff 扩展；若实现成本低再新增 |

### 8.3 暂缓新增 Skills

以下名字暂缓，不作为第一阶段新增，避免平行架构膨胀：

1. `plan-public-policy-analysis-lane`
2. `expand-eco-public-queries`
3. `plan-public-corpus-acquisition`
4. `synthesize-policy-evaluation-basis`
5. `compose-situation-analysis-report`
6. `validate-situation-analysis-report`

这些能力优先并入 action cards、corpus coverage、semantic aggregation、reporting handoff、draft narrative report 和 validate narrative report。

## 9. 报告组织轮改造

报告组织轮从“把材料交给 report-editor 自行组织”改为“agent brief + frozen basis + report-editor compose”。

### 9.1 Agent Brief

每个 agent section brief 建议包含：

1. `section_role`
2. `main_claims`
3. `evidence_refs`
4. `source_families`
5. `claim_strength`
6. `denominators`
7. `limitations`
8. `recommended_report_use`
9. `blocked_phrases`

### 9.2 进入报告的规则

1. report-editor 可以组织语言、调整结构和提升可读性。
2. report-editor 新增的实质 claim 必须能回溯到 section brief、frozen basis 或 council object。
3. challenger 标记为 unsupported 的句子必须修改、降级、删除，或由 moderator 决定开 continuation round。
4. 没有 section brief 的材料不绝对禁止进入报告，但必须来自 frozen/reporting basis，并通过 validator 检查。
5. 报告主文围绕 mission 问题、事件/治理过程、证据链、语义互动和结论边界展开，不写 runtime 日志。

## 10. 案例目标

### 10.1 NYC Smoke

目标报告主线：

`加拿大野火烟霾输送与纽约空气质量恶化事实核查、风险沟通语义变化、公众样本反应和政策沟通评估依据分析`

报告应包含：

1. AirNow / Open-Meteo / NASA FIRMS / 风场分别证明什么。
2. 可以支持区域输送相容性，不能证明具体源火场责任。
3. 事件前兆、高峰、缓解和后续解释阶段。
4. 官方健康建议、学校/户外活动调整、交通/工作生活影响、媒体关注高峰等 policy communication anchors。
5. GDELT 作为媒体/文档 tone 和可见性，不能代表公众情绪。
6. YouTube / Bluesky / comments 作为平台样本，必须有样本定义、query variants、window、denominator 和边界。
7. 事实、官方沟通和公众样本语义之间的互动分析。

数据目标不设置硬数量阈值。若评论样本量低，必须通过 coverage audit 说明 query、window、quota、视频选择和 API 限制。

### 10.2 Colorado River / Glen Canyon

目标报告主线：

`科罗拉多河水资源短缺与格伦峡谷大坝运行争议中的水文压力、联邦治理过程、公共与政策语义互动及政策评估依据分析`

报告应包含：

1. USBR RISE / USGS / Lake Powell / Glen Canyon releases 等数据说明 reservoir elevation、storage、inflow、release 和 powerplant release 的变化。
2. 只描述运行事实和压力背景，不把单一水文指标写成政策责任或政策成败。
3. DOI / USBR / Federal Register / SEIS / post-2026 guidelines / public involvement / Adaptive Management Work Group 等治理节点。
4. 正式政策语义、媒体语义和公众样本语义的差异。
5. 水位、release、治理动作和公共叙事变化之间的互动线索。
6. 政策评估依据：参与机制覆盖、政策文本回应范围、公众关注与正式议程错位、风险沟通充分性、长期治理不确定性表达。

## 11. 开发步骤

### Phase 0: 文档与边界清理

任务：

1. 将本计划作为毕业提交前代码层工程收口文档。
2. 更新 `openclaw-project-overview.md` 中对本计划的引用。
3. 清理或归档过时工作计划，保留 frozen case package 和必要 timeline。

验收：

1. 文档地图清晰。
2. 旧计划不再与当前架构冲突。

### Phase 1: Skill Contract Metadata 与 Action Cards

任务：

1. 为核心 fetch / normalize / query / optional-analysis / reporting skills 添加或补齐 contract metadata。
2. 实现 `materialize-claim-gap-action-cards`。
3. 将 action cards 暴露到 agent entry surface 和 operator 可见 artifact。

验收：

1. Action cards 不排序、不自动执行。
2. failed / zero / low-volume / receipt-only attempt 能产生 recovery card 或 source-limit card。
3. agent 能看到 followups、failure recovery 和 claim limits。

### Phase 2: Corpus 与 Coverage 重构

任务：

1. 扩展 corpus materialization。
2. 扩展 coverage audit。
3. 将 GDELT DOC/tone/table、YouTube video/comments、Bluesky false-zero、formal record/comment coverage 纳入 source-family 审计。

验收：

1. 每个 corpus 有 source family、sample definition、window、query variants、eligible count、dedup count、failure rationale。
2. 不同 source family denominator 不混用。

### Phase 3: Semantic Perception 重构

任务：

1. 扩展 semantic taxonomy。
2. 扩展 annotation worker。
3. 扩展 aggregation。
4. 支持 formal policy semantic labels 和 public/media semantic labels 的分离。

验收：

1. 任一比例都有 denominator 和 sample definition。
2. GDELT tone、YouTube comments、formal comments 不会混成同一个情绪比例。
3. public source narrative 不会被写成 physical source attribution。

### Phase 4: Interaction Timeline 与 Section Brief

任务：

1. 实现 `build-fact-policy-public-interaction-timeline`。
2. 实现或扩展 `draft-agent-section-brief`。
3. 将 interaction timeline 和 section brief 接入 reporting handoff。

验收：

1. 每个互动节点引用事实/政策侧和公共/媒体侧证据。
2. section brief 带 refs、claim strength、denominator、limitations。

### Phase 5: Report 与 Validator 重构

任务：

1. 重构 `draft-narrative-report`，使其成为 situation-analysis report 主路径。
2. 重构 `validate-narrative-report`，加入 claim-basis 检查。
3. validator 识别公众比例、正式评论争点、环境趋势、互动判断、政策评估依据、强归因/责任 claim 的 basis 缺口。

验收：

1. 报告像专业调研/学术汇报，不像 runtime 日志。
2. 没有 basis 的强 claim 被阻断、降级或要求补证。
3. helper artifact 未被 council/report basis 承接时不能直接进入主文。

### Phase 6: 双案例补跑与冻结

任务：

1. NYC smoke：补齐 action cards、corpus audit、semantic aggregation、interaction timeline、section brief、report rewrite。
2. Colorado River：补齐环境聚合、治理记录承接、public-policy semantic comparison、interaction timeline、section brief、report rewrite。
3. 冻结 case package、report basis、narrative report、defense onepager。

验收：

1. 两个报告均通过 validator。
2. 报告能展示事实核查、舆情语义感知、政策语义互动和政策评估依据。
3. 答辩材料能说明系统不是泛化民意预测，而是可审计的生态环境形势分析与样本内语义感知框架。

## 12. 总体验收条件

工程验收：

1. Action cards 稳定减少 operator 手动引导，但不替 agent 决策。
2. 核心 skill contract metadata 可被 agent entry surface 和 report validator 使用。
3. Public-policy corpus、semantic aggregate、interaction timeline、section brief 都有 typed artifacts。
4. 旧 narrative report path 不再是主写作风格，但 report publication 能力保留。

数据验收：

1. 公共/政策 corpus 的低量、失败和偏差被显式审计。
2. GDELT 媒体 tone 与公众样本情绪严格分离。
3. API 不足时，系统输出 source-limit rationale，而不是空泛舆情结论。

报告验收：

1. 报告有主线、时间线和互动分析。
2. 报告能为政策评估提供证据依据，但不越权给政策成败评分。
3. 所有 public semantic claims 都有 source family、sample definition 和 denominator。
4. 读者可以看出事实核查、舆情语义感知和政策分析之间的互动关系。

论文验收：

1. 方法章节可以描述多 agent、DB-first、skill graph、action cards、public-policy corpus、semantic perception 和 report validation。
2. 实验章节可以展示 NYC smoke 和 Colorado River 两个不同类型案例。
3. 讨论章节可以诚实说明 API 覆盖和代表性限制，同时证明系统具备可审计、可扩展、可复核的舆情语义感知能力。

## 13. 截止期执行顺序

5 月 29 日提交前，按风险压缩执行：

1. 先完成 skill contract metadata、action cards、section brief 和 report validator。
2. 再完成 corpus coverage 和 semantic aggregation 的重构。
3. 再完成 interaction timeline。
4. 最后补跑 NYC 与 Colorado 并冻结材料。

如果时间不足，不退回旧报告链路；保留已完成 typed artifacts、validator 结果和 source-limit rationale，并在论文中把未完成部分写成受审计的能力边界。

