# OpenClaw 公共舆情深化能力工作计划

文档性质：本文是公共舆情深化能力的持久化设计和实施计划。它不是固定议程脚本，也不是 source 排序规则；它用于说明当 mission、moderator 或 challenger 需要把“公共可见性”推进到“样本内意见/情绪/叙事分布”时，系统应如何提供可审计、可边界化的调查能力。

## 1. 背景问题

NYC smoke 实案报告目前只能稳妥说明：

1. 事件在公开视频和新闻渠道中具有公共可见性。
2. 已归一化的 YouTube video / GDELT DOC 信号可支持“存在同时期公开讨论”的有限描述。
3. 当前证据不能支持公众意见比例、情绪分布或来源归因。

这个边界是正确的，但也暴露了一个能力缺口：当议会希望进一步回答“公众如何反应”“样本内情绪如何分布”“公共叙事如何归因”时，当前主流程缺少一条完整、可选、可审计的深化路径。

目标不是让 runtime 自动判断民意，也不是让 fetch/normalize skill 直接做 sentiment，而是增加一条由 agent 主导、skill 辅助的公共舆情智能分析能力。该能力应同时覆盖社交样本情绪、媒体/公共记录 tone、公共来源叙事、样本覆盖审计和跨来源比较。

## 2. 设计原则

1. 舆情深化是 `optional deepening lane`，不是新的 round type。
2. `open-investigation-round` 仍然是唯一普通 continuation round 机制；舆情深化只作为 round brief / synthesis / primary focus refs 中的目标或上下文。
3. Fetch skill 只负责取证，normalize skill 只负责 lineage-preserving translation，analysis skill 只输出样本内候选统计和审计材料。
4. 任何“公众总体意见比例”都不能由平台样本直接推出；默认只能声明“样本内比例”。
5. GDELT tone 是媒体/文档语气指标，不等同于公众情绪；YouTube / Bluesky / formal comments 的文本样本才可用于样本内公众表达分析。
6. 公共来源归因叙事和物理来源归因必须分开。前者由 `social-investigator` 调查，后者必须由 `environmental-investigator` 验证。
7. Challenger 必须检查外推风险、样本选择偏差、误入样本、平台覆盖限制和分类边界。
8. Runtime 只暴露对象、refs、权限和命令模板，不生成舆情结论、不排序 source、不固定议程。

## 3. 可行性基础

### 3.1 数据源基础

当前已有可接入的公共/正式文本来源：

| 数据线 | 已有 fetch / normalize | 适合回答 | 边界 |
| --- | --- | --- | --- |
| YouTube 视频与评论 | `fetch-youtube-video-search`、`fetch-youtube-comments`、`normalize-youtube-video-public-signals`、`normalize-youtube-comments-public-signals` | 视频候选、评论反应、样本内情绪和问题表达 | 不是公开视频宇宙，也不是公众总体样本；comment-disabled、选择视频、时间窗都会影响结果 |
| Bluesky 讨论链 | `fetch-bluesky-cascade`、`normalize-bluesky-cascade-public-signals` | 社交讨论、回复语义、扩散链条、平台内叙事 | 搜索高度依赖 query / mode / access；不能代表总体社媒 |
| GDELT 新闻/公共记录 | `fetch-gdelt-doc-search`、`fetch-gdelt-events`、`fetch-gdelt-mentions`、`fetch-gdelt-gkg` 及对应 normalizer | 媒体叙事、新闻时间线、官方/媒体用语、来源假说发现 | GDELT DOC 是 recon，不是完整主表；新闻叙事不是公众意见比例 |
| Regulations.gov 正式评论 | `fetch-regulationsgov-comments`、`fetch-regulationsgov-comment-detail` 及对应 normalizer | 政策/规制议题中的正式公众评论、提交人类型、政策 concern | 对即时公共事件通常不适用，除非 mission 转向政策 docket |

NYC smoke 当前 run 中已有 GDELT DOC article 和 YouTube video 信号，但缺少 YouTube comments / Bluesky 这类可直接分析公众语义的文本样本。因此现有报告只能写“公共可见性”，不能写“情绪分布”。

### 3.2 GDELT Tone 现状与边界

GDELT 已经提供情感极性或语气相关字段，当前项目也已有部分归一化支持，但尚未形成完整的舆情分析能力：

1. `normalize-gdelt-events-public-signals`
   - 读取 `AvgTone`。
   - 写入 `metric="avg_tone"` 和 `numeric_value=<AvgTone>`。
   - 语义是事件相关报道集合的平均 tone。
2. `normalize-gdelt-mentions-public-signals`
   - 读取 `MentionDocTone`。
   - 写入 `metric="mention_doc_tone"` 和 `numeric_value=<MentionDocTone>`。
   - 语义是 mention 所在文档的 tone。
3. `normalize-gdelt-gkg-public-signals`
   - 读取 `V2Tone`。
   - 当前只抽取第一个 tone score，写入 `metric="v2_tone"` 和 `numeric_value=<tone>`。
   - 尚未拆出 `positive score`、`negative score`、`polarity`、`activity density`、`self/group reference density`、`word count` 等 `V2Tone` 分量。
   - 尚未解析或结构化 `GCAM` 的更细情绪/心理维度。

因此，当前 GDELT normalizers 不是完全没有情感字段，而是只保留了最低限度的 tone 数值。后续公共舆情深化应把 GDELT tone 作为 `media/public-record narrative tone`，不能把它直接当成 `public response sentiment`。

GDELT tone 的正确用途：

1. 描述媒体/公共记录报道语气。
2. 观察某时间窗内新闻叙事 tone 的变化。
3. 与 YouTube / Bluesky / formal comments 的样本内公众表达做对照。
4. 辅助发现公共来源叙事或报道框架。

GDELT tone 不应单独用于：

1. 推断公众总体情绪。
2. 替代 YouTube comments / Bluesky posts / formal comments 的公众表达样本。
3. 证明物理来源归因。
4. 声明社会共识或公众意见比例。

### 3.3 现有分析基础

当前已有一些 optional-analysis 辅助面：

1. `discover-discourse-issues`：从 DB public/formal signals 中生成可逆议题线索。
2. `apply-approved-formal-public-taxonomy`：在批准的 mission-scoped taxonomy 下打候选标签。
3. `compare-formal-public-footprints`：比较正式记录和公共讨论 footprint。
4. `identify-representation-audit-cues`：提示覆盖和代表性审计问题。
5. `query-public-signals`：读取 normalized public rows，并返回 item-level evidence refs。

缺口是：还没有一个正式的、样本内的公共文本语义分析闭环，能够把评论/帖子/文章标题正文转成：

1. 样本定义和 coverage summary。
2. item-level annotation refs。
3. issue / affect / source narrative 的样本内计数。
4. GDELT media tone 与 social sample affect 的分离展示。
5. 跨来源叙事比较。
6. 典型样本引用和限制说明。

### 3.4 Agent 基础

现有角色可以承接该能力：

1. `moderator`
   - 在 scoping、round brief 或 round synthesis 中记录是否启用公共舆情深化方向。
   - 判断是否需要 continuation round，或是否在当前主调查轮内继续深化。
2. `social-investigator`
   - 自主选择公共/正式信源，提交 source acquisition proposals。
   - 查询 normalized signals，生成样本内语义 finding。
3. `environmental-investigator`
   - 对“公共叙事中的来源假说”进行物理证据验证。
   - 不让舆论归因替代环境归因。
4. `challenger`
   - 审查样本外推、分类 taxonomy、误入样本、平台覆盖和结论措辞。
5. `report-editor`
   - 只在 frozen basis 允许的边界内写入样本内结果。

## 4. 建议能力形态

### 4.1 Optional Deepening Lane

新增或约定一个可选深化方向标识：

`public-discourse-sample-analysis`

可出现在：

1. `round brief` 的 investigation goals。
2. `round synthesis` 的 candidate continuation refs / optional deepening notes。
3. `evidence request` 的 requested evidence description。
4. `open-investigation-round` 的 `primary_focus_refs` 或 `round_mode` 附带说明。

它的语义是：议会希望从已采集或待采集的公共文本样本中，分析样本内议题、情绪和公共归因叙事。

该标识不应：

1. 自动选择 source。
2. 自动触发 fetch。
3. 自动决定分类 taxonomy。
4. 自动推进到 report-ready。

### 4.2 完整公共舆情智能分析 Lane

公共舆情深化不应只新增一个摘要工具，而应形成一组可组合的 optional-analysis 能力：

1. `materialize-public-discourse-corpus`
   - 从 normalized public/formal signals 物化可审计文本样本。
   - 记录 source family、query、时间窗、平台、语言、去重、误入样本标记和 evidence refs。
2. `audit-public-discourse-sample-coverage`
   - 输出样本覆盖审计。
   - 记录抓取失败、zero rows、comment-disabled、blocked、filter 过窄、source-family 缺口等。
3. `enrich-gdelt-tone-signals`
   - 或直接扩展 GDELT normalizers。
   - 拆解 `AvgTone`、`MentionDocTone`、`V2Tone` 分量，并尽量保留结构化 `GCAM` cue。
4. `aggregate-public-discourse-annotations`
   - 汇总 agent 或 approved taxonomy 生成的 item-level annotations。
   - 输出样本内 issue / affect / source narrative / actor responsibility / action orientation 分布。
5. `compare-public-media-narratives`
   - 比较 social sample affect、GDELT media tone、formal public comments 的叙事差异。
   - 只输出对照线索和 evidence refs，不生成采信结论。
6. `summarize-public-discourse-sample`
   - 面向议会和报告的汇总出口。
   - 读取 corpus、coverage、annotation aggregation、GDELT tone 和 cross-source comparison 结果。

这些 skill 都应保持 optional-analysis 属性：需要被 agent 或 moderator 明确请求和解释，输出默认是 advisory/audit material，不自动成为 report basis。

### 4.3 汇总 Skill：`summarize-public-discourse-sample`

建议保留一个汇总型 optional-analysis skill：

`summarize-public-discourse-sample`

核心职责：

1. 读取 `signal_plane.sqlite` 中的 public/formal text-like signals。
2. 读取 corpus / coverage / annotation / GDELT tone / comparison artifacts。
3. 按 run/round/source/window/keyword/ref 过滤样本。
4. 生成样本摘要、样本内分类计数、GDELT media tone 摘要、典型 evidence refs、coverage limitations。
5. 不判断真实世界公众总体，不判断物理来源真伪，不生成 report-ready 结论。

输出建议：

1. `status`
2. `summary`
3. `sample_definition`
4. `sample_count`
5. `source_family_counts`
6. `issue_distribution`
7. `social_affect_distribution`
8. `gdelt_media_tone_summary`
9. `source_narrative_distribution`
10. `cross_source_comparison`
11. `representativeness_limits`
12. `evidence_refs`
13. `example_refs`
14. `warnings`
15. `board_handoff`

### 4.4 Taxonomy / Annotation 边界

第一版不应使用全局默认 sentiment taxonomy。应采用 mission-scoped taxonomy 或 agent-authored annotation basis。

建议第一版标签族：

1. `issue_facets`
   - health-risk
   - visibility/orange-sky
   - mask/protection
   - school/work/disruption
   - travel/flight-disruption
   - government-response
   - climate-change
   - source-origin-question
   - information-seeking
2. `affect_labels`
   - concern
   - fear
   - anger
   - frustration
   - sarcasm/humor
   - sympathy
   - neutral-reporting
   - uncertainty
3. `source_narrative_labels`
   - canada-wildfires
   - quebec-wildfires
   - nova-scotia-wildfires
   - regional-wildfire-smoke
   - climate-change-frame
   - local-pollution
   - unknown-or-not-mentioned
4. `actor_responsibility_labels`
   - government-response
   - agency-warning
   - individual-protection
   - platform/media-amplification
   - natural-hazard
   - regulatory-failure
   - no-responsibility-frame
5. `action_orientation_labels`
   - seeking-information
   - protective-action
   - policy-demand
   - fact-checking
   - sharing-experience
   - humor/reaction
   - uncertainty

这些标签只描述文本样本表达，不验证事实真伪。

### 4.5 GDELT Tone 与 Social Sentiment 分工

报告和 finding 必须显式区分：

1. `gdelt_media_tone`
   - 来源：GDELT Events / Mentions / GKG / DOC timelinetone。
   - 描述对象：媒体报道、文档、公共记录的语气和叙事框架。
   - 允许说：新闻样本 tone 偏负/偏正/波动，报道叙事集中在某些主题。
2. `social_sample_affect`
   - 来源：YouTube comments、Bluesky posts/replies、formal public comments 等文本样本。
   - 描述对象：样本内公众表达。
   - 允许说：样本内担忧、愤怒、信息求助、讽刺等标签的计数或比例。
3. `source_narrative`
   - 来源：GDELT、YouTube、Bluesky、formal comments 中出现的来源叙事。
   - 描述对象：文本如何解释或指称来源。
   - 允许说：样本内哪些来源叙事反复出现。
4. `physical_source_attribution`
   - 来源：环境证据、轨迹、烟羽、火点、气象、受体观测等。
   - 描述对象：现实物理来源。
   - 必须由环境线验证，不能由 public discourse lane 独立给出强归因。

### 4.6 Claim Strength 约束

允许的表述：

1. “在本轮 YouTube comments 样本内，健康风险和空气安全是高频 concern。”
2. “在本轮 Bluesky query 样本内，加拿大野火是反复出现的来源叙事。”
3. “GDELT 新闻样本显示，多家媒体将事件框定为加拿大野火烟雾影响。”
4. “GDELT Events / Mentions / GKG 的 tone 指标显示，本轮媒体/文档样本整体语气偏负，但这不是公众情绪比例。”
5. “YouTube / Bluesky 样本中的 affect 标签显示，健康担忧和信息求助在样本内较常见。”

不允许的表述：

1. “纽约公众大多数认为……”
2. “公众情绪比例为……”
3. “舆论证明烟霾来自某地……”
4. “没有抓到某类说法，所以公众没有这种担忧。”
5. “GDELT tone 证明公众情绪为负面。”

需要环境线验证后才可升级的表述：

1. “来源归因为加拿大/魁北克某区域火点。”
2. “公共叙事中的来源假说与物理输送证据一致。”
3. “某个具体火场导致纽约受体峰值。”

## 5. 建议议程流程

### 5.1 主调查轮内启用

当 mission 明确要求公众反应、情绪、意见比例、叙事归因时：

1. Moderator 在 round brief 中记录 `public-discourse-sample-analysis`。
2. Social-investigator 自主提出 source proposals。
3. Operator 执行批准后的 fetch/normalize/link。
4. Social-investigator 查询 normalized rows，必要时使用 analysis skill。
5. Analysis lane 输出 corpus、coverage audit、annotation aggregation、GDELT tone summary 和 cross-source comparison。
6. Challenger 审查样本边界。
7. Moderator 在 round synthesis 中记录是否足够支持样本内结论。

### 5.2 Continuation Round 中启用

当报告草稿或 challenger 指出“当前公共证据只能支持可见性，不能支持比例/情绪/叙事分布”时：

1. Moderator 请求普通 `open-investigation-round`。
2. `primary_focus_refs` 指向相关 finding / report limitation / challenge。
3. `round_mode` 仍可为 `continuation`，不新增 `sentiment-round`。
4. 新轮中 social-investigator 负责深化公共文本样本。
5. 若出现物理来源假说，environmental-investigator 同步验证。

## 6. 实施阶段

### P1：GDELT Tone Enrichment

1. 扩展 GDELT GKG normalizer，完整拆解 `V2Tone` 分量。
2. 在 metadata 中保留结构化 tone parts，例如 tone、positive、negative、polarity、activity density、self/group reference density、word count。
3. 评估并结构化保留 `GCAM` 中可审计的情绪/心理维度 cue。
4. 明确 `AvgTone`、`MentionDocTone`、`V2Tone` 属于 media/public-record tone，不属于 public response sentiment。

### P2：公共文本样本 Corpus 与 Coverage Audit

1. 新增 `materialize-public-discourse-corpus`。
2. 新增 `audit-public-discourse-sample-coverage`。
3. 支持 YouTube comments、Bluesky、GDELT DOC/GKG/Mentions/Events、Regulations.gov comments/detail 的 source-family 分组。
4. 记录 source-family coverage、query/window、去重、误入样本、失败尝试和不可外推边界。

### P3：Annotation Aggregation 与 Cross-Source Comparison

1. 新增 `aggregate-public-discourse-annotations`。
2. 支持 agent-authored annotation JSONL 或 approved mission taxonomy。
3. 输出 issue / affect / source narrative / actor responsibility / action orientation 的样本内分布。
4. 新增 `compare-public-media-narratives`，比较 GDELT media tone、social sample affect 和 formal comment footprint。

### P4：样本摘要与报告交接

1. 新增 `summarize-public-discourse-sample`。
2. 支持 DB-backed query、corpus artifact、coverage audit、annotation aggregation、GDELT tone summary 和 cross-source comparison 输入。
3. 输出样本定义、分布、GDELT media tone、social sample affect、example refs、warnings、board handoff。
4. 不写 deliberation conclusion，只写 optional-analysis artifact。

### P5：议会对象承接

1. Social-investigator 把样本摘要写入 finding / evidence bundle。
2. Challenger 对 finding 发起 challenge 或 readiness boundary。
3. Moderator 在 synthesis 中记录是否继续深化或允许弱收口。

### P6：真实案例回归

用 NYC smoke 作为第一轮验证：

1. 从 YouTube video candidates 中选择与事件强相关的视频。
2. 抓取 comments。
3. 归一化到 public signals。
4. 拉取 GDELT Events / Mentions / GKG 时间窗，归一化并检查 tone fields。
5. 运行 corpus、coverage audit、annotation aggregation、cross-source comparison 和样本摘要。
6. 检查是否能生成“样本内健康担忧/防护/来源叙事”和“媒体 tone / 公众样本 affect 分离”的有限结论。
7. 检查 challenger 是否阻止外推成公众总体。

