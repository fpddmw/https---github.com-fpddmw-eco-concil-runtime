# 2023年6月纽约烟霾事件：环境证据与公共讨论语义结构的有界报告

## 摘要

本报告只消费 round-002 的 frozen/canonical report basis（report-basis:report-basis-a3644c654c90；artifact: report_basis/frozen_report_basis_round-002_v2.json），并以 round-003 形成中文读者版叙述。报告回答四个问题：2023 年 6 月纽约烟霾发生了什么，环境证据能支持到什么层级，公共讨论与媒体文本呈现怎样的语义结构，以及哪些结论必须保持受限。

有界结论是：受体侧 PM2.5 观测清楚显示纽约在 2023-06-06 至 2023-06-08 出现显著污染升高，2023-06-07 达到本轮材料中的最高受体侧记录，2023-06-09 后明显回落。风场记录与候选源区火点记录提供“相容背景”，但本报告不得把它写成强来源归因、烟羽输送证明、化学因果链或责任判断。

公共讨论与媒体证据显示的不是代表性公众意见占比，而是样本内语义结构：媒体/文档材料反复把事件框定为空气质量、野火烟雾、危险烟霾、口罩、防护、航班或户外影响以及烟雾持续/改善；YouTube 评论与 Bluesky 样本呈现感官描述、防护健康、气候/野火解释、怀疑或阴谋式反应、以及西海岸/加州/西雅图烟霾经验比较等线索。旧的 lang=en Bluesky zero 只能作为路线参数诊断，不能作为“无人讨论”的负面发现。

审计引用（节选）：report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63, finding:finding-589da494e256, finding:finding-41d0055c332e, finding:finding-0063344c52a7, review-comment:review-comment-c4b1fe6e6922；另 16 条见 JSON。

## 要点

事件发生了什么：在当前报告基础中，纽约烟霾事件表现为 2023-06-06 开始明显升高、2023-06-07 达峰、2023-06-08 仍高、2023-06-09 至 2023-06-10 回落的受体侧 PM2.5 过程。AirNow 在纽约 bbox 内归一化 2,265 条 PM2.5 信号；日最大值为 Jun 5: 28.1 µg/m³、Jun 6: 201.9、Jun 7: 412.0、Jun 8: 274.6、Jun 9: 122.0、Jun 10: 56.0，其中 Queens 在 2023-06-07T18:00Z 记录 412.0 µg/m³。Open-Meteo 空气质量交叉检查提供 144 个小时值，2023-06-08T00:00 最大为 99.9 µg/m³。

环境证据支持什么：它支持“纽约受体侧确有显著 PM2.5 升高，并与同时段局地风场和候选源区活跃火点记录具有背景相容性”的描述性判断。Open-Meteo 历史天气提供 Jun 5-10 的 288 个风速/风向信号；在 AirNow 峰值附近记录 298°、5.10 m/s（2023-06-07T18:00）、301°、5.83 m/s（19:00）和 302°、3.55 m/s（2023-06-08T00:00）。FIRMS 在候选源区 bbox（约 lat 45.024–58.529、lon -84.830–-61.051）于 Jun 1-10 归一化 70,392 条火点信号，日计数在 Jun 6 为 14,940。

公共语义结构是什么：GDELT DOC 的 500 条文章信号与 YouTube 标题是媒体/文档叙事线索；YouTube 的 439 条评论/回复和 Bluesky 的 71 条无语言过滤样本是平台样本内公共反应线索。它们可以说明“看起来像夜晚、黄天、异味”、N95/KN95 或健康防护、加拿大野火/野火烟雾叙事、气候框架、怀疑反应、以及西海岸经验比较等语义簇，但不能转写为公众占比、平台总体情绪或纽约居民代表性意见。

审计引用（节选）：report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63, finding:finding-589da494e256, finding:finding-41d0055c332e, finding:finding-0063344c52a7, review-comment:review-comment-c4b1fe6e6922；另 16 条见 JSON。

## 引言：事件问题与可回答范围

2023 年 6 月纽约烟霾在本轮材料中首先是一个“受体侧空气污染过程”：报告基础不需要、也不允许新增外部事实来证明它，而是从已归一化的环境信号出发，描述纽约地区 PM2.5 的时间结构。可回答的问题是污染升高的时段、峰值、回落，以及哪些背景线索与之同时出现；不可回答的问题是精确来源、烟羽路径、化学成分、责任主体或完整健康影响链。

本报告采用学术/专业调研文章的组织方式，而不是复述运行日志。读者应把它理解为一个有审计索引的阶段性证据综合：它能为后续归因建模、健康影响研究、风险沟通或制度复盘划定问题和证据边界，但不把尚未完成的专业归因或代表性民意调查伪装成结论。

审计引用（节选）：report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63, finding:finding-589da494e256, finding:finding-41d0055c332e, finding:finding-0063344c52a7, review-comment:review-comment-c4b1fe6e6922；另 5 条见 JSON。

## 材料与方法

材料基础为当前 run 的 frozen/canonical report basis：frozen_report_basis_round-002_v2.json、round_readiness_round-002_after_dispositions.json，以及被报告基础携带的环境聚合 artifact environment_evidence_aggregation_round-002_metric_unit_v2.json。环境材料包括 AirNow 小时 PM2.5、Open-Meteo 空气质量 PM2.5、Open-Meteo 历史风速/风向，以及 NASA FIRMS VIIRS 活跃火点记录。公共/媒体材料包括 GDELT DOC、YouTube 发现元数据、YouTube 评论/回复，以及经 challenger 修正边界后的 Bluesky 无语言过滤样本。

方法上，本报告只做“证据角色综合”：受体侧 PM2.5 用于描述纽约污染时序和强度；风场记录用于局地气象相容性背景；FIRMS 火点用于候选源区活跃火点背景；GDELT 和 YouTube 标题用于媒体/文档叙事线索；YouTube 评论与 Bluesky 帖文用于样本内公共反应语义。环境聚合 artifact 的角色是压缩已入库环境信号的覆盖、时间序列和点事件统计，不用于证据排序、来源排序、风险评分或归因判定。

所有公共讨论材料均按样本处理：GDELT 不等于公众情绪，YouTube 发现只是选取候选视频的元数据，YouTube 评论和 Bluesky 帖文不构成代表性抽样。旧 Bluesky lang=en zero 在本报告中只保留为路线诊断：同查询在无 search-lang 条件下可见 25 条，而 search-lang=en 为 0，说明早先零结果与历史语言元数据/路线参数有关，不能写成讨论缺席。

审计引用（节选）：report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63, finding:finding-589da494e256, finding:finding-41d0055c332e, finding:finding-0063344c52a7, review-comment:review-comment-c4b1fe6e6922；另 15 条见 JSON。

## 结果一：环境证据显示的时序与相容背景

AirNow 与 Open-Meteo 的共同作用是确认受体侧污染升高。AirNow 记录显示 Jun 6 开始出现高值、Jun 7 达到最高、Jun 8 仍保持高水平、Jun 9 后下降；Open-Meteo 空气质量记录也在 Jun 6-8 给出升高的小时值和日均背景。两套材料来源、测量/模型属性不同，因此本报告把它们作为交叉描述线索，而不是互相替代。

局地风记录提供的是相容性语境：在 AirNow 峰值附近，风向约 298°–302°、风速约 3.55–5.83 m/s。该信息说明峰值时段有可讨论的局地气象背景，但单一受体风时间序列不是烟羽轨迹模型，也不是来源归因模型。

FIRMS 火点记录提供候选源区背景：Jun 1-10 共 70,392 条归一化火点信号，Jun 3、Jun 5、Jun 6 等日计数较高，其中 Jun 6 为 14,940。该记录说明候选源区在事件窗口前后存在大量活跃火点，但本报告不得把火点记录直接改写成“这些火导致纽约 PM2.5 峰值”的因果证明。

审计引用（节选）：report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63, finding:finding-589da494e256, finding:finding-41d0055c332e, finding:finding-0063344c52a7, review-comment:review-comment-c4b1fe6e6922；另 5 条见 JSON。

## 结果二：公共讨论与媒体语义结构

媒体/文档语义层面，GDELT DOC 文章记录和 YouTube 标题反复出现“野火烟雾、空气质量警示、危险烟霾、口罩、防护、航班或户外影响、烟雾持续或改善”等框架。这些材料说明事件在公开文本中被组织为空气质量与野火烟雾相关的风险议题；但它们不是公众情绪测量，也不能证明物理来源。

平台评论语义层面，YouTube 的 439 条评论/回复显示若干样本内主题：感官异常（像夜晚、天空发黄、异味）、健康防护与口罩、气候/野火解释、怀疑或阴谋式反应、以及与 California/West Coast/Seattle 烟霾经验的比较。Bluesky 的 71 条无语言过滤样本也提供空气质量、N95/KN95、橙色/烟雾天空和气味、加拿大野火叙事、西海岸比较和气候框架等例子。

这些主题只能称为“样本内语义簇”或“可见讨论线索”。本报告不报告公众意见占比，不把非代表性样本写成纽约公众或平台总体态度，不把公共 source narrative cues 写成物理 source attribution。若未来要讨论公众态度结构，需要独立的样本设计、覆盖审计、标注体系、聚合方法和明确分母。

审计引用（节选）：evidence-bundle:evidence-bundle-c7fd0fd58728, evidence-bundle:evidence-bundle-921dc877cdf0, finding:finding-0e69dd8d8a2e, finding:finding-01ca5b9d9b40, review-comment:review-comment-1738755d2c75, review-comment:review-comment-30d295ffa17e；另 4 条见 JSON。

## 讨论：证据链如何支持有界结论

将环境证据和公共语义放在同一条论证链中，可以得到一个稳健但受限的解释：纽约在 Jun 6-8 经历了明显 PM2.5 污染升高，媒体和公众样本将其理解为空气质量和烟霾/野火烟雾风险事件，环境背景中同时存在候选源区活跃火点和局地风场相容线索。这个解释足以回答“发生了什么”和“公共讨论如何组织”，但不足以回答“精确来源是什么”或“谁应负责”。

challenger/moderator 的边界在这里不是形式约束，而是结论质量的一部分。环境 bundle 只能支持 AirNow/Open-Meteo 的受体侧 PM2.5 描述、Open-Meteo 风场背景和 FIRMS 活跃火点背景；公共/媒体 bundle 只能支持媒体文档叙事线索和平台样本内公共反应；Bluesky 修正只改变路线诊断和样本可见性，不提供代表性。

因此，本文采用“相容性”和“背景线索”而非强归因语言。若要从本报告升级到来源、输送、化学成因或责任判断，需要轨迹/烟羽模型、化学组成或源解析、替代解释评估、以及相应的专业审查；这些均不在 frozen basis 中形成可发布强结论。

审计引用（节选）：report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63, finding:finding-589da494e256, finding:finding-41d0055c332e, finding:finding-0063344c52a7, review-comment:review-comment-c4b1fe6e6922；另 16 条见 JSON。

## 局限性

第一，环境证据的主要强项是受体侧 PM2.5 时序和强度；它不提供强 source attribution、plume transport、chemical causation 或 responsibility 判断。FIRMS 火点和风场记录是相容背景，不是烟羽轨迹或源解析。

第二，公共讨论材料没有代表性抽样设计。YouTube 评论、Bluesky 帖文和媒体/文档记录只能说明样本内可见语义；不得写为公众意见占比、纽约居民总体态度、平台总体情绪或议题 prevalence。GDELT DOC 是媒体/文档叙事线索，不是 public sentiment。

第三，旧 lang=en Bluesky zero 只应作为 route diagnostic：它说明带语言过滤的历史检索可能因语言元数据而出现假零，不说明相关讨论不存在。修正后的无语言样本可用于例示语义主题，但仍不得外推代表性。

审计引用（节选）：review-comment:review-comment-c4b1fe6e6922, review-comment:review-comment-1738755d2c75, review-comment:review-comment-30d295ffa17e, challenge-disposition:challenge-disposition-f7c4f598eef5, report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63；另 16 条见 JSON。

## 结论与使用建议

本报告的可发布结论是：2023 年 6 月纽约烟霾在 frozen basis 中表现为 Jun 6-8 的显著受体侧 PM2.5 升高，Jun 7 达到最高记录，Jun 9 后回落；媒体/公共文本把它组织为空气质量、野火烟雾、防护、感官异常、气候/野火解释、怀疑反应和跨地区经验比较等语义结构；环境背景与候选源区活跃火点和局地风场具有相容性，但不构成强物理归因。

适当用途包括：为决策者、研究者或传播人员提供事件时序、证据角色和公共语义结构的审计化摘要；为后续归因建模、健康影响评估、风险沟通研究和代表性公众调查提出证据需求。

不适当用途包括：证明具体野火或区域导致了纽约 PM2.5 峰值，判定烟羽传输路径或化学成因，给出责任归属，或声称公众中有多少占比持有某种意见。

审计引用（节选）：report-basis-freeze:report-basis-freeze, evidence-bundle:evidence-bundle-b6db51775c63, finding:finding-589da494e256, finding:finding-41d0055c332e, finding:finding-0063344c52a7, review-comment:review-comment-c4b1fe6e6922；另 16 条见 JSON。

## 参考文献与审计索引

本报告的“参考文献”以审计索引形式保存，而非新增外部书目。核心材料包括：frozen_report_basis_round-002_v2.json；round_readiness_round-002_after_dispositions.json；environment_evidence_aggregation_round-002_metric_unit_v2.json；environmental evidence bundle b6db51775c63；public/media semantics bundle c7fd0fd58728；supplemental Bluesky bundle 921dc877cdf0；以及相关 finding、review-comment、challenge-disposition 和 current-run signal-set refs。

主要信号索引包括：AirNow PM2.5 2,265 条；Open-Meteo AQ PM2.5 144 条；Open-Meteo historical wind 288 条；FIRMS fire_detection_count 70,392 条；GDELT DOC 500 条；YouTube comments 439 条；Bluesky no-language 71 条。完整 116 条审计引用保留在 JSON 产物中，用于复核证据对象、报告基础和样本边界。

### 完整审计引用

- report-basis-freeze:report-basis-freeze
- finding:finding-01ca5b9d9b40
- finding:finding-0e69dd8d8a2e
- finding:finding-cabc71dba6ec
- finding:finding-41d0055c332e
- finding:finding-0063344c52a7
- finding:finding-589da494e256
- evidence-bundle:evidence-bundle-921dc877cdf0
- evidence-bundle:evidence-bundle-c7fd0fd58728
- evidence-bundle:evidence-bundle-b6db51775c63
- readiness-opinion:readiness-opinion-163c747bfe63
- readiness-opinion:readiness-opinion-99708bd51d96
- readiness-opinion:readiness-opinion-df01a96235ba
- readiness-opinion:readiness-opinion-49c8cfff9312
- review-comment:review-comment-30d295ffa17e
- review-comment:review-comment-c4b1fe6e6922
- review-comment:review-comment-1738755d2c75
- round-synthesis:round-synthesis-70eabb1c435e
- agent-position:agent-position-6acf3967bfdf
- current-run-signal-set:airnow:round-002:fetch-airnow-hourly-observations:pm2_5:2265
- current-run-signal-set:open-meteo-aq:round-002:pm2_5:144
- current-run-signal-set:open-meteo-historical:round-002:wind:288
- current-run-signal-set:firms:round-002:fire_detection_count:70392
- current-run-signal-set:gdelt-doc:round-002:articles:500
- current-run-signal-set:youtube-comments:round-002:comments:439
- current-run-signal-set:bluesky:round-002:no-language:71
- source-acquisition-execution-link-receipt-51451a1312a292dcbadc
- runtime-receipt-087da36bdb2118d89ca4
- source-acquisition-execution-link-receipt-87ae5c0f2c81bb42dc21
- source-acquisition-execution-link-receipt-ab9b74ba0fc07aea6570
- source-acquisition-execution-link-receipt-ea302778ad52cf626e52
- evidence-bundle-b6db51775c63
- evidence-bundle-921dc877cdf0
- source-acquisition-proposal-85bc1c931e91
- sig-8a8e93e917255d53
- sig-e9c3d00c8b119e4d
- sig-2328cc4ee5e0aaa8
- sig-7f0f871062bb427c
- sig-f6bad0178dbbc1bd
- sig-3f6c9a8444df67ae
- sig-453a53065cd93241
- sig-db1447fff226c1af
- evidence-bundle-c7fd0fd58728
- evidence-route-assessment-52e1450809d3
- source-acquisition-proposal-37efd02a8e5f
- source-acquisition-proposal-d8c1bb50d598
- sig-9981801ca3094e70
- sig-b66cdc03e7853762
- sig-9e80973ae217bf9a
- sig-3350860d2e9a2500
- sig-8843c118a821fd76
- sig-6fac1f7278b98134
- sig-9bd2476c406eb0c7
- sig-a967f9bf17122875
- sig-1c4a40ead06ea428
- sig-27a6ec52d44b563a
- runtimeevt-c30d1e4f72cfabbb18b2
- runtimeevt-8fb3b950678e3bf5ee06
- source-acquisition-proposal-785fdbda01c5
- evidence-route-assessment-1d649f166dcb
- sig-48e61603ac7b9e13
- sig-d8b3f17e83266faa
- sig-b6f9d6cf1dbf1044
- sig-392a265b094e840c
- runtimeevt-a37782160792de8d58c6
- runtimeevt-57ccdad13b67686f87dc
- runtimeevt-2149ea86fabcc7c4cbdb
- runtimeevt-85d1ea2d172bc4eb9910
- source-acquisition-proposal-d9bc889a7457
- sig-cf4deb5ec850cf19
- sig-c556784289a09442
- sig-1009fcbd6be33df5
- sig-b6de5b2dd288e972
- runtimeevt-56c689927f22be3f99d8
- runtimeevt-a435663bac7a1074bc49
- runtimeevt-3eae90fa724970feb295
- runtimeevt-6683702e9c65b357b6fe
- source-acquisition-proposal-2203dfbea040
- source-acquisition-proposal-d2c690227ae1
- sig-8668dab0196a97d8
- sig-c6746d64e9407913
- sig-1e10bb002bd54dde
- sig-4cbb05b4c010dc3e
- runtime-receipt-9a9a82570b54b4757a87
- runtime-receipt-a3b792a5fcc463936d47
- runtime-receipt-caa238f51ab4e20bebdc
- normalize-receipt-f1ba5ecdb1750434ba45
- normalize-receipt-5e09b0f6b790aae4e84c
- normalize-receipt-a0007e1bca60e4855336
- runtime-receipt-555d428b0cf782c5741f
- normalize-receipt-7a6fccae202552b34147
- runtime-receipt-46e9763b524b87d9b8be
- normalize-receipt-d9b34309a159df3bb270
- runtime-receipt-f98c872a91c831127b2c
- normalize-receipt-8f84f47d63346f269bad
- runtime-receipt-bcbd4a414aebec640a2e
- normalize-receipt-f1e26692e08229a03983
- source-acquisition-proposal-0cf67a529271
- sig-4569ac9d8d600a9a
- /home/fpddmw/projects/openclaw-eco-concil_v1/runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/raw/round-002/environmental-investigator/link_round002_airnow_execution.json
- /home/fpddmw/projects/openclaw-eco-concil_v1/runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/raw/round-002/environmental-investigator/link_round002_open_meteo_air_quality_execution.json
- /home/fpddmw/projects/openclaw-eco-concil_v1/runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/raw/round-002/environmental-investigator/link_round002_open_meteo_historical_execution.json
- /home/fpddmw/projects/openclaw-eco-concil_v1/runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/raw/round-002/environmental-investigator/link_round002_firms_execution.json
- finding-589da494e256
- finding-41d0055c332e
- finding-0063344c52a7
- finding-0e69dd8d8a2e
- finding-01ca5b9d9b40
- readiness-opinion-49c8cfff9312
- readiness-opinion-99708bd51d96
- review-comment-c4b1fe6e6922
- review-comment-1738755d2c75
- review-comment-30d295ffa17e
- challenge-disposition:challenge-disposition-f7c4f598eef5
- envagg-receipt-22c329b7b5238af42a69
- /home/fpddmw/projects/openclaw-eco-concil_v1/runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/analytics/environment_evidence_aggregation_round-002_metric_unit_v2.json:$.aggregation