# OpenClaw 正式评论与舆情调查能力升级工作计划

文档性质：本文是针对 PM2.5 NAAQS public comment smoke run 暴露问题形成的代码侧工作计划。它聚焦正式公众评论、公共媒体/平台语义、样本设计、附件文本实体化、语义标注与报告前质量控制。本文不是案例结论，不是固定议会议程，也不要求 runtime 代替 agent 选择 source。

## 1. 背景问题

PM2.5 NAAQS run 暴露出的问题不是单纯“缺一个附件下载 skill”，而是正式评论和舆情调查链条整体过薄。

已观察到的具体问题：

1. Regulations.gov 只做了 broad list discovery 和单条 comment detail，没有先构建可靠候选评论集合。
2. 从 125 条 broad comment-listing 中只确认 1 条相关 seed 后，议会过早下钻到单个 PDF 附件，导致样本基础过窄。
3. 单个附件即使能读取，也可能是封面、图片、模板信或低相关内容，不能代表 formal comment corpus。
4. 公共侧只有少量 GDELT/YouTube/Bluesky 样本，无法支撑公众意见比例、舆情分布或主要治理争议的稳定判断。
5. agent 对“候选集合是否足够”“是否存在样本漂移”“是否需要批量 detail/分层抽样”的反思不充分。
6. moderator 能识别 capability boundary，但当前缺少更强的“不能用单条 seed 收口 formal-public 语义任务”的质量约束。

因此，本计划的目标是补齐一条完整的 formal-public discourse pipeline：

`source family discovery -> candidate corpus construction -> list quality audit -> batch detail -> attachment/text materialization -> semantic annotation -> aggregation -> council uptake -> report boundary check`

## 2. 不变原则

升级过程中必须保持以下原则：

1. `runtime` 不选择 source、不固定议程、不判断证据充分性。
2. `moderator` 负责组织议会、判断 continuation 和收口，不直接替 investigator 做证据采信。
3. `social-investigator` 保留自主调查权，但必须对样本设计、漂移和代表性边界负责。
4. `skill` 保持原子化能力：fetch 只取证，normalize 只归一化，analysis 只做可审计辅助标注或聚合。
5. 不引入 source 权重、证据打分、排序或非必要启发式规则。
6. 可以报告样本内比例，但不能把样本内比例说成总体公众意见，除非 mission 明确提供代表性抽样设计。
7. `receipt-only`、`failed`、`zero-signal`、`blocked` 都是执行状态，不是现实世界证据缺席。

## 3. 目标能力

升级完成后，议会应能做到：

1. 从 Federal Register / Regulations.gov 规则锚点反推或构建目标 docket / document / commentOn 入口。
2. 对正式评论先形成候选集合，而不是从单条 seed 直接下钻。
3. 区分 list discovery、comment detail、attachment content 三层证据。
4. 对 inline comment 与 attachment comment 分别处理。
5. 批量读取一批评论 detail，并保留抽样/筛选理由。
6. 对评论正文、附件文本、公共媒体/平台文本进行 issue / stance / concern / affect / source narrative 标注。
7. 聚合样本内标签分布，并明确分母、过滤条件、不可代表性限制。
8. 让 report-editor 生成有边界但内容充分的决策者报告，而不是基于少量 seed 的运行日志。

## 4. 实施路线

### 4.1 Regulations.gov 候选集合构建

新增或增强能力：

1. 增强 `fetch-regulationsgov-comments` 的可用查询路线。
2. 支持从 known docket/document/commentOn 信息构建候选集合。
3. 增加对结果漂移的结构化输出，而不是让 agent 靠人工扫标题。

需要支持的输入维度：

1. `docket_id`
2. `comment_on_document_id`
3. `agency_id`
4. `posted_date` / `received_date`
5. `search_term`
6. `document_type`
7. `subtype`

输出要求：

1. `candidate_comment_count`
2. `candidate_ids`
3. `query_parameters`
4. `source_limitations`
5. `field_coverage`
6. `likely_drift_indicators`
7. `sample_ref_limit`

注意：漂移检测只能输出提示，例如标题不含主题词、docket 缺失、commentOn 缺失、日期不匹配；不能给 source 排序或证据打分。

### 4.2 候选集合质量审计

新增 skill：`audit-formal-comment-candidate-corpus`

输入：

1. Regulations.gov list artifact 或 normalized comment-listing refs。
2. 目标 docket/document/commentOn 约束。
3. 可选关键词集，由 agent 提供。

输出：

1. `eligible_count`
2. `excluded_count`
3. `missing_docket_count`
4. `missing_comment_on_count`
5. `exact_docket_match_count`
6. `exact_document_match_count`
7. `title_keyword_match_count`
8. `duplicate_or_mass_campaign_count`
9. `candidate_id_samples`
10. `exclusion_reason_samples`
11. `warnings`

边界：

1. 不判断评论立场。
2. 不判断评论重要性。
3. 不自动决定下一步是否收口。
4. 只帮助 agent 判断当前候选集合是否适合进入 batch detail 或需要重新查询。

### 4.3 批量 comment detail

增强使用 `fetch-regulationsgov-comment-detail`：

1. 支持从 candidate corpus 审计结果直接读取 comment IDs。
2. 支持 batch detail 输出更清晰的字段摘要。
3. 确保 artifacts 写入 run raw 目录，不进入 repo-root `data/` 或 manual 目录。

需要改进 normalizer：

1. `normalize-fetch-execution` 或专用 normalizer 应能识别 comment detail payload。
2. 将 detail 归一化为 `formal` plane signal。
3. 保留以下字段：
   - `comment_id`
   - `docket_id`
   - `comment_on_document_id`
   - `title`
   - `submitter_name` / `organization`
   - `document_type`
   - `subtype`
   - `posted_date`
   - `receive_date`
   - `comment_text`
   - `attachment_ids`
   - `attachment_count`
   - `has_inline_comment_text`
   - `requires_attachment_text`

验收标准：

1. 单条 comment detail 不应再出现 `Expected payload.records to be a list` 这种 receipt-only 失败。
2. detail signal 可以被 `query-formal-signals` 查到。
3. 若 comment 字段只有 `See Attached`，quality flags 必须明确 `requires-attachment-text`。

### 4.4 附件下载与文本抽取

新增 skill：`fetch-regulationsgov-attachments`

职责：

1. 输入 comment ID 或 attachment IDs。
2. 调 `/comments/{commentId}/attachments` 或 `/attachments/{attachmentId}`。
3. 获取 attachment metadata、fileUrl、format、size、title。
4. 下载 PDF/HTML/TXT 等附件到 run raw 目录。
5. 记录 sha256、content-type、byte size、download URL、source comment linkage。

新增或复用通用 skill：`extract-document-text`

职责：

1. 输入本地 PDF/HTML/TXT artifact。
2. 输出文本 artifact。
3. 输出页数、提取成功页、空白页、OCR/扫描疑似标记。
4. 不做语义判断。

新增 normalizer：`normalize-regulationsgov-attachment-text`

职责：

1. 将附件文本归一化为 formal signal。
2. 关联 comment detail signal、attachment ID、docket ID、document ID。
3. 保留附件标题、页码、文本片段和 artifact refs。

验收标准：

1. 对 `EPA-HQ-OAR-2015-0072-5836` 能下载 `attachment_1.pdf` 和 `attachment_2.pdf`。
2. 能识别封面页与正文附件。
3. 若 PDF 是图片或不可抽取文本，应输出 `text-extraction-limited`，而不是误称无内容。
4. 附件文本 signal 可被 formal query skill 查询。

### 4.5 语义标注与聚合

新增或完善 analysis lane：

1. `classify-formal-comment-issues`
2. `classify-public-discourse-affect`
3. `aggregate-public-discourse-annotations`
4. `compare-formal-public-discourse`

标注维度：

1. `issue_label`
   - health benefit
   - compliance cost
   - monitoring / implementation
   - environmental justice
   - scientific basis
   - legal authority
   - economic burden
   - industry impact
   - state/local implementation
2. `stance_hint`
   - support
   - oppose
   - mixed
   - procedural / unclear
3. `concern_facet`
   - health
   - cost
   - feasibility
   - uncertainty
   - equity
   - federalism
4. `affect_label`
   - alarm / concern
   - relief / support
   - distrust / anger
   - technocratic / neutral
   - uncertainty
5. `source_narrative`
   - how actors frame cause, responsibility, benefit, harm, or burden.

输出要求：

1. 每条标注必须有 source signal ref。
2. 必须写明 `label_method` 和 prompt/template version。
3. 聚合必须写明 sample denominator。
4. 标签默认非互斥，不强行相加为 100%。
5. 输出样本内结构，不输出总体公众意见。

### 4.6 Public/media 样本扩展

公共侧不能只依赖 7 条 GDELT 和 4 条 YouTube。

GDELT：

1. DOC search 用于 recon 和媒体可见性。
2. Events / Mentions / GKG 用于窗口内结构化补充。
3. DOC tone / timeline tone 用于媒体/document tone，不等同公众情绪。
4. 遇到 429 时应记录限流并允许延迟重试或降低 max records，不应判定无数据。

YouTube：

1. Search 只发现视频候选。
2. 若 mission 涉及 public reaction，应进入 comments fetch。
3. 视频样本应记录频道、发布时间、标题、查询词和 off-topic 样本。

Bluesky：

1. Search 失败或 0 seed 时，应允许 alternate terms / hashtag / author-feed / thread mode。
2. 不应把 0 seed 当作平台没有讨论。

验收标准：

1. 至少能形成一个公共样本 corpus，而不是孤立若干条结果。
2. public discourse summary 必须包含 source-family counts、样本定义、时间窗口、off-topic 风险。
3. report 只能写样本内结构，不能写总体民意。

### 4.7 Moderator 和 agent 提示词修正

需要更新 `social-investigator`：

1. 不能从单条 seed 直接推 formal-comment 结构。
2. 在追 detail/attachment 前，应先说明候选集合是否足够。
3. 必须区分：
   - discovery set
   - candidate corpus
   - enriched detail set
   - readable text corpus
   - annotated corpus
4. 若样本过小，应提出扩样或明确 capability boundary。

需要更新 `moderator`：

1. 当 mission 要求 public comment / public discourse 结构时，不能接受单条 comment seed 作为充分 formal basis。
2. continuation 不应只追一条附件，除非 agent 已说明这是 exploratory trace 而非 corpus conclusion。
3. 收口前必须检查：
   - 是否有 candidate corpus。
   - 是否有 readable text corpus。
   - 是否有 annotation/aggregation basis。
   - 是否有 sample boundary。
4. 如果缺口是能力边界，可以收成 capability-bounded report，但报告必须明确“不是议题调查完成”。

需要更新 `challenger`：

1. 重点质疑样本量、样本漂移、单条 seed 外推、附件不可读、公共样本代表性。
2. 不要求逐条复核所有标签，但要抽查 taxonomy 是否合理。

需要更新 `report-editor`：

1. 不得把 capability-bounded run 写成实质结论完整报告。
2. 必须明确区分：
   - 议题锚点成立。
   - 样本内公共语义初探。
   - formal comment corpus 未完成。
   - 能力缺口。

### 4.8 报告前质量检查

新增或增强 reporting validation：

检查项：

1. 若报告写“正式评论主要争议”，必须存在 readable formal comment corpus 和 annotation/aggregation basis。
2. 若只有 comment listing 或单条 detail，不得写 formal comment stance distribution。
3. 若 public sample 少于 agent 自定阈值，必须写 sample-boundary warning。
4. 若 GDELT / YouTube / Bluesky 混合统计，必须分 source-family denominator。
5. 若附件不可读取，必须写 capability limitation。
6. 若报告写“公众意见比例”，必须有 explicit sample denominator 和 representativeness limits。

输出：

1. `pass`
2. `warning`
3. `block`

注意：quality check 可以 block 报告措辞，但不决定是否继续 round。

## 5. 推荐实施批次

### Batch 1：正式评论集合与 detail normalizer

优先级最高。

任务：

1. 增强 Regulations.gov list 查询与 candidate corpus 输出。
2. 实现 `audit-formal-comment-candidate-corpus`。
3. 修复 comment detail normalize，使 detail 不再 receipt-only。
4. 更新 social/moderator 提示词，禁止单条 seed 外推。

验收：

1. PM2.5 NAAQS run 能构建一批相关候选 comments。
2. 能批量 detail 一组 comment IDs。
3. detail rows 能进入 formal signal plane。

### Batch 2：附件下载与文本抽取

任务：

1. 实现 `fetch-regulationsgov-attachments`。
2. 实现或复用 `extract-document-text`。
3. 实现 `normalize-regulationsgov-attachment-text`。
4. 对不可抽取 PDF 输出明确 quality flags。

验收：

1. 能读取 `EPA-HQ-OAR-2015-0072-5836` 的 attachment metadata 和 PDF。
2. 能将正文附件转为 text artifact。
3. 能产生 formal attachment text signals。

### Batch 3：语义标注与样本聚合

任务：

1. 实现 formal/public discourse 标注 worker。
2. 实现 annotation aggregation。
3. 输出样本内比例和边界说明。
4. 更新 challenger/report-editor 使用方式。

验收：

1. 对一批 formal comment text 可产出 issue/stance/concern 标签。
2. 对 YouTube/Bluesky/GDELT 文本可产出 affect/source narrative/media tone 边界化摘要。
3. 报告可写样本内结构，但不会误写总体民意。

### Batch 4：报告质量与 case rerun

任务：

1. 增强 report validation。
2. 重跑 PM2.5 NAAQS case。
3. 生成 bounded substantive report。
4. 对比旧 run：说明从 capability smoke 变为 formal-public discourse case 的改进。

验收：

1. 报告有足够正文材料，不是运行日志。
2. 结论围绕用户 mission，而不是只解释议会做了什么。
3. 所有强 claim 都有明确 evidence basis 和 boundary。

## 6. PM2.5 NAAQS 重跑标准

重跑前必须满足：

1. 能构建候选评论集合，而不是单条 seed。
2. 能批量 detail 至少一批候选评论。
3. 能处理 inline comment 与 attachment comment。
4. 能将附件文本进入 formal signal plane。
5. 能做至少一轮 formal comment issue/stance/concern 标注。
6. 能做公共样本扩展或明确公共样本边界。
7. report validation 能阻止单条 seed 外推。

重跑后期望输出：

1. 官方规则锚点。
2. 正式公众评论样本结构。
3. 公共媒体/平台语义结构。
4. formal-public 对照。
5. 样本限制和能力限制。
6. 决策者可读的有边界结论。

## 7. 非目标

本计划不做：

1. 代表性民意调查。
2. 社会科学严格抽样推断。
3. 法律合规结论。
4. source 权重和排序。
5. 自动证据充分性评分。
6. runtime 固定议程。
7. 将 GDELT tone 当作公众情绪。
8. 将单条正式评论当作 formal comment universe。

## 8. 当前 run 的正确定位

当前 PM2.5 NAAQS run 应被标记为：

`formal-public discourse capability smoke / failure-driven upgrade trigger`

它可以证明：

1. 官方规则锚点能抓取。
2. broad comment list 能跑通但漂移严重。
3. comment detail 能抓取。
4. 附件层是正式评论语义分析的真实瓶颈。
5. 当前议会对公共评论集合构建和样本设计仍不足。

它不能证明：

1. PM2.5 NAAQS 正式公众评论主要争议已经识别完成。
2. 各立场比例或公众意见分布。
3. 单条 Forestry Association of South Carolina comment 的实质立场。
4. 媒体和公共平台整体语义结构。

后续只有完成上述能力升级后，PM2.5 NAAQS 才适合作为毕业展示中的“正式评论与公共舆情治理争议案例”。
