# NYC Smoke Fresh Rerun GPT-5.5 案例包

文档性质：本文是 2026-05-20 fresh rerun 的冻结案例包摘要。它记录本次从头运行 NYC smoke 议会的关键产物、数据范围、报告边界和展示路径，不新增事实、不替代 run 内 DB/ledger/receipt。

## 1. 案例身份

| 字段 | 内容 |
| --- | --- |
| Run ID | `openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520` |
| Agent 模型 | OpenClaw council agents 使用 `openai-codex/gpt-5.5` |
| 主题 | `2023 年 6 月纽约市烟霾事件` |
| Mission | 说明发生了什么、环境证据支持哪些解释、公共讨论和媒体语义呈现什么结构、哪些结论受证据限制，并生成中文专业调研报告 |
| 最终状态 | 已发布中文报告，validation valid；runtime health 已刷新为无 open dead letters |

## 2. 最终产物

| 产物 | 路径 / ID | 状态 |
| --- | --- | --- |
| 发布版中文报告 | `runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/reporting/narrative_report_round-003.md` | 已发布 |
| Docs 留档报告 | `docs/frozen-case-packages/nyc-smoke-20230607/narrative_report_round-003_fresh-rerun-gpt55-20260520.md` | 已复制 |
| 报告 JSON | `runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/reporting/narrative_report_round-003.json` | 已发布 |
| Draft / Validation | `narrative_report_draft_round-003.*` / `narrative_report_validation_round-003.json` | `valid` |
| Publication ID | `narrative-report-97cd0794b009` | 已发布 |
| Frozen report basis | `report-basis-a3644c654c90` / `report_basis/frozen_report_basis_round-002_v2.json` | `frozen` |
| Round synthesis | `round-synthesis-70eabb1c435e` | 已提交 |
| Case run package | `runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/runtime/case_run_package_round-003.json` | 已生成 |
| Runtime health | `runs/openclaw-realcase-nyc-smoke-fresh-rerun-gpt55-20260520/runtime/runtime_health.json` | `open_dead_letter_count=0` |

Validation 状态：`status=valid`，`error_count=0`，`publish_allowed=true`；仍有 1 个非阻断 warning：`public-summary-path-missing`，表示公共讨论 addendum metadata 没有独立 helper artifact path。报告没有使用该 warning 去外推代表性舆情。

## 3. 议会流程摘要

| 阶段 | 主要动作 | 结果 |
| --- | --- | --- |
| round-001 scoping | moderator 建立 mission scaffold、证据请求和 source proposal 路线 | environmental / social investigators 明确本地 skill 路线，不以 web search 为入口 |
| round-002 investigation | 执行 AirNow、Open-Meteo AQ、Open-Meteo wind、FIRMS、GDELT DOC、YouTube、Bluesky 等本地 skills | 形成 environmental bundle、social bundle、Bluesky corrective bundle、findings 和 readiness opinions |
| Bluesky corrective follow-up | 发现 `--search-lang en` 造成历史检索假零；改用 no-language A/B 路线 | 获得 71 条 Bluesky sample signals；旧 zero route 被 challenger 降级为 route diagnostic |
| challenger review | 审查环境归因、公民/媒体语义、Bluesky 路线边界 | 3 条 review comment + challenge disposition；限制被 moderator 逐条接受为 report limitation |
| moderator synthesis | 汇总 facts、limitations、non-continuation rationale | 决定不再开调查轮，进入 bounded report basis |
| report basis / reporting | 生成 readiness、freeze report basis，打开 report-editor-only round-003 | report-editor 起草、修订、validate、publish 中文报告 |

## 4. 抓取与归一化范围

### 环境线

| Source family / skill | 地点 / 时间 | 归一化结果 |
| --- | --- | --- |
| `fetch-airnow-hourly-observations` | NYC bbox `-74.30,40.45,-73.65,40.95`；`2023-06-05T00:00Z` 至 `2023-06-10T12:00Z` | 2,265 条 PM2.5/AQI 相关环境信号；报告使用按单位分组后的 PM2.5 浓度摘要 |
| `fetch-open-meteo-air-quality` | `40.7128,-74.0060`；`2023-06-05` 至 `2023-06-10` | 144 条 PM2.5 小时信号 |
| `fetch-open-meteo-historical` | `40.7128,-74.0060`；`2023-06-05` 至 `2023-06-10` | 288 条风速/风向小时信号 |
| `fetch-nasa-firms-fire` | 候选源区 bbox `-85,45,-55,60`；`2023-06-01` 至 `2023-06-10`；VIIRS SNPP/NOAA20 | 70,392 条 active fire detections |
| `aggregate-environment-evidence` | round-002 current-run environment signals | `environment_evidence_aggregation_round-002_metric_unit_v2.json`，73,089 条环境信号描述性压缩 |

### 公共 / 媒体线

| Source family / skill | 地点 / 时间 / 查询边界 | 归一化结果 |
| --- | --- | --- |
| `fetch-gdelt-doc-search` | NYC smoke / wildfire smoke event window | 500 条媒体/文档 article signals |
| `fetch-youtube-video-search` | `2023-06-05T00:00Z` 至 `2023-06-20T00:00Z` | 25 条候选视频 metadata，作为 discovery 而非评论语料 |
| `fetch-youtube-comments` | 前 10 个候选视频；comments window `2023-06-05T00:00Z` 至 `2023-07-15T00:00Z` | 439 条 comments/replies |
| `fetch-bluesky-cascade` corrective no-language | `2023-06-05T00:00Z` 至 `2023-06-10T12:00Z`；`NYC smoke wildfire air quality`、`New York wildfire smoke`、`orange sky NYC` | 71 条 provider-visible sample social-post signals |

## 5. 结论边界

本次可发布结论：

1. 纽约在 2023-06-06 至 2023-06-08 出现显著受体侧 PM2.5 升高，2023-06-07 达到本轮材料最高记录，2023-06-09 后回落。
2. Open-Meteo wind 与 FIRMS source-region active fire detections 提供与区域烟雾背景相容的上下文。
3. GDELT / YouTube / Bluesky 样本显示事件被组织为空气质量、野火烟雾、防护、感官异常、气候/野火解释、怀疑反应和跨地区经验比较等语义结构。

本次不得升级的结论：

1. 不得断言具体火场、具体输送路径、化学因果链或责任主体。
2. 不得把 FIRMS + local wind + PM2.5 写成 plume/source attribution proof。
3. 不得把 GDELT tone 写成 public sentiment。
4. 不得把 YouTube / Bluesky 样本写成代表性公众意见比例。
5. 不得把旧 Bluesky `lang=en` zero route 写成“无讨论”或“route exhausted”。

## 6. 本次程序性修复

1. 补充 Bluesky skill 文档：历史 search 零结果必须进行 no-language A/B 检查，避免 `--search-lang en` 假零。
2. 修复 `aggregate-environment-evidence` 的 coverage metric distribution：按 `metric + unit` 分组，避免 PM2.5 浓度与 AQI 混合均值。
3. 修复 `draft-narrative-report` 对 frozen basis 变体路径的读取：当有效 basis 使用 `_v2` 等变体路径时，report skill 可自动消费。

质量门：

1. `python3 tools/quality_gate.py syntax`
2. `python3 tools/quality_gate.py test runtime-governance reporting`
3. `python3 -m unittest tests.test_signal_plane_workflow tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles`
4. `python3 tools/quality_gate.py test module-decomposition`

上述质量门在修复后已通过；另有针对 `aggregate-environment-evidence` 的单位分组单测通过。

## 7. 展示价值

本案例比旧 NYC baseline 更适合展示最近的 skill uptake 修复：

1. investigators 使用本地 source-family skills，而不是平台 web search。
2. Bluesky 假零被识别为参数/metadata route 问题，并通过同族 follow-up 修正。
3. 大规模环境数据进入了 `aggregate-environment-evidence` 压缩摘要。
4. helper artifact 被 moderator synthesis / report basis 承接后才进入报告。
5. validator 成功阻止了样本比例越界，report-editor 修订后才发布。
6. 最终报告是中文专业调研文章，而不是 runtime 日志。
