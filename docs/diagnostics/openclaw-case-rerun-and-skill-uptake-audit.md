# OpenClaw 三案例重跑必要性与 Skill Uptake 审计重写

文档性质：诊断与重写文件。本文基于三个已归档/阶段性归档 case 的运行产物、signal plane、skill usage matrix、claim-strength 文档与 source-family workflow 文档，给出是否重跑、如何重跑、哪些 skill 未用属于正常、哪些未用暴露能力缺口的审计结论。

本文不新增案例事实，不替代议会结论，不把 runtime 改造成议程编排者。它的目标是把“为什么很多 skill 没被用”“哪些 case 还值得展示”“下一步怎么修”说清楚。

## 一句话结论

不建议三个 case 全部重跑。

| Case | 是否完整重跑 | 推荐动作 | 核心理由 |
| --- | --- | --- | --- |
| NYC smoke | 不需要 | 保留已冻结 case；最多补做环境聚合/事实边界复核/报告重写 | 数据链和舆情分析链相对完整，已有 33,975 条 normalized signals，并形成 public discourse 深化产物。问题主要在报告组织和部分可选分析未纳入。 |
| Colorado River / Glen Canyon | 不建议从头重跑 | 不重抓大规模原始数据；从已有 signal plane 做环境聚合、治理记录整合和报告重写 | 已有 1,278,468 条 normalized signals，重抓成本高且意义不大。真正缺口是百万级水文/气象数据没有被充分压缩进入 report basis。 |
| PM2.5 NAAQS | 需要重跑或从正式评论语料阶段重开 | 将当前 run 定位为 failure-driven smoke；重新跑正式评论语料构建、样本审计、正文/附件材料化、语义标注、聚合和报告 | 当前只有 165 条 normalized signals，其中 125 条是 comment listing，不是可读评论正文；没有 formal candidate audit、formal issue classification、public discourse aggregation。不能作为最终展示案例。 |

## 审计口径

本文采用三层判断：

1. `normalized_signals`：说明数据是否进入 signal plane。
2. `analysis_result_sets` / `analytics/*`：说明是否形成可引用的分析产物。
3. council objects / report basis / narrative report：说明分析是否被议会采信并进入报告链。

很多 skill “没有被使用”不是问题。问题只在于：当报告想要提出某类 claim 时，是否缺少对应的证据组织和分析基础。

## Case 级审计

### NYC Smoke

运行目录：

`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512`

Signal plane 概况：

| Plane | Kind | Source skill | Count |
| --- | --- | --- | ---: |
| environment | fire-detection | `fetch-nasa-firms-fire` | 31,227 |
| environment | observation | `fetch-airnow-hourly-observations` | 2,032 |
| environment | hourly-observation | `fetch-open-meteo-historical` | 240 |
| public | comment | `fetch-youtube-comments` | 100 |
| public | event-row | `fetch-gdelt-events` | 100 |
| public | gkg-row | `fetch-gdelt-gkg` | 100 |
| public | mention-row | `fetch-gdelt-mentions` | 100 |
| public | article | `fetch-gdelt-doc-search` | 51 |
| public | video | `fetch-youtube-video-search` | 25 |
| total |  |  | 33,975 |

已形成的关键链路：

1. 环境证据：AirNow PM2.5、Open-Meteo wind、NASA FIRMS fire detections 已进入 signal plane，可支撑“纽约受体污染峰值与区域输送背景相容”的有边界描述。
2. 舆情证据：GDELT、YouTube video、YouTube comments 已进入 public plane。
3. 舆情深化：已形成 public discourse corpus、coverage audit、affect classification、annotation aggregation、public/media narrative comparison、sample summary。
4. 报告链：已有 round synthesis、report basis freeze、narrative report 和 frozen package。

主要缺口：

1. `aggregate-environment-evidence` 未被使用。对于 31,227 条 FIRMS、2,032 条 AirNow 和 240 条风场记录，报告应尽量引用聚合后的覆盖、时间峰值、空间范围和样本 refs，而不是只依赖 agent 自行概括。
2. `detect-temporal-cooccurrence-cues`、`materialize-spatiotemporal-relation-evidence-packet`、`review-spatiotemporal-relation-alternatives` 未被使用。由于报告保留了“无法证明具体源头”的边界，这不是硬伤；但如果希望报告显得更专业，这些 helper 可以让“相容但不构成完整归因”的论证更清晰。
3. 叙事报告曾多次重写，说明报告模板/提示词的问题比取证问题更大。

重跑判定：

不需要完整重跑。若用于毕业设计展示，建议只做“分析补强 + 报告重写”：

1. 从已有 signal plane 运行 `aggregate-environment-evidence`。
2. 可选运行 `detect-temporal-cooccurrence-cues` 和 `review-fact-check-evidence-scope`。
3. 将这些 helper 产物由 agent 或 moderator 以 finding / evidence bundle / report-basis item 的形式采纳。
4. 重新执行报告撰写轮，不重新抓取原始数据。

### Colorado River / Glen Canyon

运行目录：

`runs/openclaw-realcase-colorado-river-glen-canyon-governance-rerun-round005-20260515`

Signal plane 概况：

| Plane | Kind | Source skill | Count |
| --- | --- | --- | ---: |
| environment | instantaneous-value | `fetch-usgs-water-iv` | 1,135,507 |
| environment | hourly-observation | `fetch-open-meteo-historical` | 78,912 |
| public | mention-row | `fetch-gdelt-mentions` | 29,946 |
| public | gkg-row | `fetch-gdelt-gkg` | 27,382 |
| environment | usbr-rise-result | `fetch-usbr-rise` | 5,480 |
| public | comment | `fetch-youtube-comments` | 544 |
| environment | daily-observation | `fetch-open-meteo-flood` | 365 |
| public | article | `fetch-gdelt-doc-search` | 210 |
| formal | comment-listing | `fetch-regulationsgov-comments` | 41 |
| formal | official-governance-record | `fetch-usbr-project-records` | 40 |
| public | video | `fetch-youtube-video-search` | 21 |
| formal | official-governance-record | `fetch-federal-register-documents` | 20 |
| total |  |  | 1,278,468 |

已形成的关键链路：

1. 环境/运行数据体量充足：USGS IV、Open-Meteo historical、USBR RISE 都已进入 signal plane。
2. 治理记录有所补足：Federal Register、USBR project records 和 Regulations.gov comment listing 已进入 formal plane。
3. 公共叙事材料较丰富：GDELT GKG/Mentions/DOC、YouTube video/comments 已进入 public plane。
4. 已有 public discourse corpus、coverage audit、affect annotations、aggregation、comparison、sample summary。
5. 已形成 round-005 freeze 与 round-006 narrative report。

主要缺口：

1. `aggregate-environment-evidence` 未被使用。这是 Colorado 最大问题。百万级 USGS 记录和 78,912 条气象记录不应该直接靠报告 agent 阅读或抽象概括；必须先压缩成覆盖范围、关键指标、时间窗、站点/变量分布、极值/均值/缺口、样本 refs。
2. 治理记录与环境运行数据之间的关系没有形成足够清晰的 relation packet。可使用 `materialize-spatiotemporal-relation-evidence-packet`、`review-fact-check-evidence-scope` 或 `review-spatiotemporal-relation-alternatives` 辅助表达“运行记录能说明什么、不能说明什么”。
3. 现有报告曾出现过模板过度领域化问题，例如空气质量/烟霾措辞误入水资源治理报告。这个问题不应通过重抓数据解决，而应通过报告模板和 claim-sensitive validation 解决。

重跑判定：

不建议完整重跑，也不建议重抓百万级数据。建议做“既有数据再分析 + 报告重写”：

1. 从现有 DB 运行 `aggregate-environment-evidence`，按 USGS IV、Open-Meteo、USBR RISE 分开形成压缩摘要。
2. 对治理记录运行 formal footprint / relation scope review，而不是再盲目扩大抓取。
3. 要求 report editor 使用 mission 问题组织报告，而不是写运行日志。
4. 若最终报告仍不能回答 mission，再考虑有针对性的补抓，不应从头重跑。

### PM2.5 NAAQS

运行目录：

`runs/openclaw-realcase-pm25-naaqs-public-comment-smoke-20260519`

Signal plane 概况：

| Plane | Kind | Source skill | Count |
| --- | --- | --- | ---: |
| formal | comment-listing | `fetch-regulationsgov-comments` | 125 |
| formal | official-governance-record | `fetch-federal-register-documents` | 24 |
| public | article | `fetch-gdelt-doc-search` | 7 |
| public | video | `fetch-youtube-video-search` | 4 |
| public | comment | `fetch-youtube-comments` | 3 |
| formal | attachment-text | `fetch-regulationsgov-attachments` | 2 |
| total |  |  | 165 |

关键事实：

1. 125 条 `comment-listing` 只是候选列表/元数据，不等同于可读正式评论语料。
2. 只有 1 条 detail route 实质性尝试，附件文本抽取仍存在 `attachment-file-not-downloaded`、`metadata-only-no-local-file`、`text-extraction-limited` 等质量限制。
3. 没有 `audit-formal-comment-candidate-corpus`。
4. 没有 `classify-formal-comment-issues`。
5. 没有 public discourse corpus、coverage audit、affect classification、aggregation、sample summary。
6. 没有 formal/public footprint comparison 能支持“正式评论和公共讨论如何对应”的判断。

主要缺口：

PM2.5 的问题不是“数据少”这么简单，而是“语料链没有成立”。comment listing 可以证明系统找到了一批 Regulations.gov 候选记录，但不能证明议会已经阅读、归纳和分析了正式公众评论内容。

重跑判定：

需要重跑，或至少从正式评论语料阶段重开。当前 run 可以保留为“能力 smoke / failure-driven upgrade trigger”，但不应作为最终展示 case。

建议重跑边界：

1. 保留 Federal Register 规则锚点。
2. 对 Regulations.gov listing 做 `audit-formal-comment-candidate-corpus`。
3. 批量拉取 detail，而不是只取一条。
4. 对 “See Attached” 或正文缺失项批量拉取附件并 `extract-document-text`。
5. 用 `normalize-regulationsgov-comment-detail-public-signals` 和 `normalize-regulationsgov-attachment-text` 建立可读语料。
6. 用 `classify-formal-comment-issues` 做样本内议题/关切/立场线索标注。
7. 用 `compare-formal-public-footprints` 比较正式评论与 GDELT/YouTube/Bluesky 的覆盖差异。
8. 只有在这些产物进入 finding / evidence bundle / round synthesis / report basis 后，再开报告轮。

## Skill 未使用情况的重新分类

### 未使用但正常

这些 skill 没在三个 case 中使用，并不意味着系统失败：

| Skill 类型 | 代表 skill | 为什么未用正常 |
| --- | --- | --- |
| 特定数据源 fetch | `fetch-epa-eis-records`, `fetch-openaq`, `fetch-open-meteo-air-quality` | 三个 case 未必需要 EIS、OpenAQ 或 Open-Meteo AQ；NYC 已使用 AirNow 作为受体监测，Colorado 是水资源/运行治理。 |
| 特定 normalize | 各数据源 normalize skill | 如果对应 fetch 没触发，normalize 不触发是正常的。 |
| Board 深度管理 | `open-challenge-ticket`, `close-challenge-ticket`, `update-hypothesis-status`, `summarize-board-state` | 当前 case 的 board 使用较轻。除非有显式挑战、假说跟踪、反事实线路，否则这些 skill 可不使用。 |
| 专题导出/展示 | `export-research-issue-map`, `project-research-issue-views` | 更适合论文展示/答辩可视化，不是每轮调查必需。 |
| 显式分类法应用 | `apply-approved-formal-public-taxonomy` | 只有 mission 或议会批准了固定 taxonomy 时才应使用；否则容易压制 agent 自主发现议题。 |
| Advisor 类提示 | `suggest-evidence-lanes` | 这是辅助发现，不应成为所有 case 的固定入口。 |

### 未使用且应优先补入的能力

这些不是硬 gate，但一旦报告想提出相应 claim，就应成为“软义务”：

| Claim 族 | 应优先出现的 skill / artifact | 缺失后果 |
| --- | --- | --- |
| 大规模环境状态描述 | `aggregate-environment-evidence` | 百万级数据无法进入人类可读报告，agent 容易只抽取零散证据或泛泛概括。 |
| 事件时间关系 / 空间关系 | `detect-temporal-cooccurrence-cues`, `materialize-spatiotemporal-relation-evidence-packet`, `review-spatiotemporal-relation-alternatives` | 报告只能说“有若干证据”，难以组织成“时间链/空间链/反证边界”。 |
| 正式评论议题结构 | `audit-formal-comment-candidate-corpus`, `fetch-regulationsgov-comment-detail`, `fetch-regulationsgov-attachments`, `extract-document-text`, `normalize-regulationsgov-attachment-text`, `classify-formal-comment-issues` | 只能说明“找到了评论列表”，不能说明正式评论争点、关切和样本内分布。 |
| 公共讨论样本结构 | `materialize-public-discourse-corpus`, `audit-public-discourse-sample-coverage`, `classify-public-discourse-affect`, `aggregate-public-discourse-annotations`, `summarize-public-discourse-sample` | 不能报告样本内情绪/议题/叙事线索比例，只能列举单条材料。 |
| Formal-public 对照 | `compare-formal-public-footprints`, `compare-public-media-narratives` | 难以回答“正式制度化参与”和“公共传播/社交讨论”之间是否一致、错位或互补。 |
| 报告 claim 边界 | `review-evidence-sufficiency`, `review-fact-check-evidence-scope`, `validate-narrative-report` | 容易把弱证据写成强结论，或反过来把可成立的样本内结论写得过弱。 |

## 重写后的 Skill Uptake 机制

不建议通过硬编写议程、固定 gate 或 runtime 强制调用所有 skill 来解决 uptake 问题。这会破坏 agent 自主调查权，也会让 runtime 重新变成议程制定者。

推荐采用三层软机制。

### 第一层：Source-family workflow card

每类 source family 明确“常见链路”，但不规定必须走完。

示例：

| Source family | 推荐理解 |
| --- | --- |
| GDELT | DOC search 是 recon 和文章/tone 聚合入口；Events/Mentions/GKG 是行级扩展入口。DOC 失败不能直接说明没有媒体记录。 |
| YouTube | video search 是候选发现；comments 才是公共响应语料。只抓 video 不足以分析公众情绪。 |
| Regulations.gov | comments listing 是候选列表；detail/attachment/text 才形成可读 formal comment corpus。 |
| Environmental observations | fetch 产生原始信号；大规模数据应先 aggregate，再由 agent 解释。 |
| Governance records | Federal Register/USBR/EIS 等记录提供制度链条，但不自动证明政策责任、争议强度或公众立场。 |

### 第二层：Claim-sensitive soft obligations

不是“开这个 case 必须调用哪个 skill”，而是“如果报告要写这种 claim，就需要相应 basis”。

| 报告想写的内容 | 最低 basis |
| --- | --- |
| “样本内某类公众情绪/关切占比” | corpus + coverage audit + annotation + aggregation + denominator |
| “正式评论中主要争点包括...” | candidate audit + readable text corpus + issue classification + aggregation |
| “环境数据呈现某种阶段性变化” | environment aggregation + sample refs + time/space/metric coverage |
| “A 与 B 在时间上相互对应” | temporal co-occurrence cues + relation packet 或清楚的 fact-check scope |
| “某来源/原因得到支持” | normalized refs + challenger/review + alternatives/limitations |
| “可以收口成报告” | round synthesis + readiness opinion + unresolved refs/non-continuation rationale + report validation |

### 第三层：Report validation 反向约束

报告 validator 不应要求固定议程，但应检查报告是否越过已有 basis。

应新增或强化的 validator 语义：

1. 报告出现“公众意见比例/情绪分布/主要议题”时，必须能找到 sample denominator 和 annotation/aggregation artifact。
2. 报告出现“正式评论争议/正式公众意见”时，必须能找到 candidate audit、可读 comment text 或 attachment text、formal issue classification。
3. 报告出现“环境趋势/峰值/运行状态”时，必须能找到 aggregation artifact，或显式说明只是 item-level 示例。
4. 报告出现“来源归因/因果/影响链”时，必须有 relation/fact-check/challenger review basis，或降级为“相容/线索/仍需验证”。
5. 报告不应把 GDELT tone 当作公众情绪，也不应把 YouTube/Bluesky/Regulations.gov 样本当作总体民意。

这套机制不会要求 runtime 决定证据够不够，只要求 report editor 不要写出没有 basis 的 claim。

## 三个 Case 的重写后处置

### NYC Smoke：保留为主展示 case

定位：

突发环境事件的“环境受体 + 区域输送线索 + 公共语义响应”案例。

处置：

1. 不完整重跑。
2. 可补做环境聚合和关系边界复核。
3. 重新生成中文叙事报告，重点不是“议会做了什么”，而是回答用户问题：纽约烟霾如何发展、环境证据如何支撑、公众语义如何显现、结论边界在哪里。

### Colorado River：保留为第二展示 case，但不重抓原始大数据

定位：

慢变量环境治理争议案例，展示“水文/运行数据 + 治理记录 + 公共叙事”的不同调查模式。

处置：

1. 不从头重跑。
2. 先补 `aggregate-environment-evidence`，将 USGS/Open-Meteo/USBR RISE 压缩为可报告结构。
3. 再做治理记录与公共叙事的 report-basis 重组织。
4. 报告不得套用烟霾/空气质量模板。

### PM2.5 NAAQS：不作为当前形态的展示 case

定位：

正式公众评论管线的失败驱动测试，不是成品案例。

处置：

1. 需要重跑或至少从 formal comment corpus 阶段重开。
2. 不能再接受 125 条 listing + 1 条 detail + 2 条附件元数据作为“正式评论语义分析”基础。
3. 如果时间有限，可把 PM2.5 降级为“能力改进说明”，主展示仍放在 NYC 与 Colorado。

## 对“强结论”的重写

当前几个 case 经常生成“有边界报告”，这并不必然失败。真正的问题是报告没有清楚地区分三种强度：

1. 数据内强结论：在明确样本、时间窗、平台、字段范围内可以比较强。
2. 机制性中等结论：多源证据相容，但缺少专业模型或反事实验证。
3. 总体性弱结论：不能推广到总体公众意见、完整因果链或政策责任归因。

因此，未来报告不应简单写“证据不足”。更好的写法是：

1. 在样本内明确给出可成立的强描述。
2. 在跨源关系上说明相互支撑与仍需验证的断点。
3. 对总体民意、政策因果、物理归因保持边界。

这样既不夸大，也不会让所有结论都显得无力。

## 下一步执行顺序

1. 保留 `docs/diagnostics/openclaw-skill-usage-matrix.md` 作为原始 usage 台账。
2. 使用本文作为三案例重跑与补强决策依据。
3. 若继续改代码，优先改 report validation 与 role prompt：
   - 报告 claim 触发 basis 检查。
   - agent role surface 明确“本地集成 skills 优先；web search 不作为默认证据入口”。
   - source-family workflow card 明确 list/detail、search/comments、fetch/aggregate 的链路关系。
4. 若继续跑案例：
   - 先补 Colorado 环境聚合与报告重写。
   - 保持 NYC 为已完成主案例，只做必要报告重写。
   - PM2.5 只有在正式评论语料链修好后再重跑。

## 不应采取的方案

1. 不要为了提高 skill 使用率而强制每轮调用大量 skill。
2. 不要让 runtime 根据 topic 固定议程或决定 source order。
3. 不要把 helper artifact 自动升级成议会结论。
4. 不要把零结果、失败结果、receipt-only、listing-only 当作现实世界不存在证据。
5. 不要把报告写成运行日志。报告应回答 mission，而不是解释议会框架。

