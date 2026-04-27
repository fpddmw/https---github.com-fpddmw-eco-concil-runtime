# OpenClaw Skills 总表

## 说明

这份文档现在只承担一个角色：

`skills 附录索引。`

路线说明、当前实现说明、未来蓝图说明，都已经收敛到另外两份文档：

1. `../openclaw-project-overview.md`
2. `../openclaw-next-phase-development-plan.md`

当前工作区共有 `73` 个 skill 目录。这里按“运行时编排、数据抓取、归一化、检索回溯、分析加工、议会状态、报告发布”七类整理。

这里的“层级”使用统一约定：

- `runtime`：控制运行、轮次、执行计划。
- `raw/public`、`raw/environment`：原始抓取层。
- `normalized/public`、`normalized/environment`：归一化后的统一信号层。
- `analytics`：候选提取、范围推导、覆盖度评分、证据融合等分析层。
- `board`：议会状态、任务、挑战、假设、下一步动作。
- `reporting`：对外报告、内部交接、最终发布。
- `archive/history`：跨 run 归档与历史复用。

## Runtime 编排

| 名称 | 用途（简要概括） | 分类 | 层级 | 关键字段 | 主要产物 |
| --- | --- | --- | --- | --- | --- |
| `scaffold-mission-run` | 根据 mission 初始化 run、首轮任务和初始 board | runtime 编排 | `runtime` | `mission`、`source_requests`、`artifact_imports`、初始假设 | `mission.json`、`round_tasks`、种子 board |
| `open-investigation-round` | 开启新一轮调查并继承未解决状态 | runtime 编排 | `runtime` | 当前 round、来源 round、board 状态、未解决对象 | 新 round 任务骨架、轮次状态更新 |
| `plan-round-orchestration` | 基于 board 和阶段产物生成 runtime orchestration plan | runtime 编排 | `runtime` | board、D1/D2 产物、readiness posture | 轮次 orchestration plan JSON |
| `prepare-round` | 将 mission、round task 和 source governance 编译成 fetch plan | runtime 编排 | `runtime` | `mission.json`、`round_tasks`、选源结果、导入请求 | `runtime/fetch_plan_<round>.json` |
| `normalize-fetch-execution` | 执行 fetch plan，调度抓取/导入，并调用 normalizer 入库 | runtime 执行 | `runtime` | fetch plan、raw artifact、normalizer 映射 | `runtime/import_execution_<round>.json`、信号库写入 |

## 数据抓取 Skills

| 名称 | 用途（简要概括） | 分类 | 层级 | 关键字段 | 主要产物 |
| --- | --- | --- | --- | --- | --- |
| `fetch-airnow-hourly-observations` | 拉取 AirNow 小时空气监测值 | 数据抓取 | `raw/environment` | 时间窗、bbox、污染物参数 | AirNow 原始观测 JSON |
| `fetch-bluesky-cascade` | 拉取 Bluesky 种子帖及回复串联 | 数据抓取 | `raw/public` | 查询源、作者/列表/关键词、时间窗 | Bluesky 级联帖子 JSON/JSONL |
| `fetch-gdelt-doc-search` | 通过 GDELT DOC API 查文章、时间线或聚合 | 数据抓取 | `raw/public` | `query`、`mode`、`format`、时间参数 | GDELT DOC 查询 JSON |
| `fetch-gdelt-events` | 下载 GDELT Events 导出快照 | 数据抓取 | `raw/public` | 时间范围、最大文件数、输出目录 | manifest JSON + `.export.CSV.zip` |
| `fetch-gdelt-gkg` | 下载 GDELT GKG 导出快照 | 数据抓取 | `raw/public` | 时间范围、最大文件数、输出目录 | manifest JSON + `.gkg.csv.zip` |
| `fetch-gdelt-mentions` | 下载 GDELT Mentions 导出快照 | 数据抓取 | `raw/public` | 时间范围、最大文件数、输出目录 | manifest JSON + `.mentions.CSV.zip` |
| `fetch-nasa-firms-fire` | 拉取 NASA FIRMS 活跃火点数据 | 数据抓取 | `raw/environment` | 日期范围、bbox、数据源/卫星 | 火点 CSV/JSON |
| `fetch-open-meteo-air-quality` | 拉取 Open-Meteo 空气质量背景场 | 数据抓取 | `raw/environment` | 坐标、日期范围、污染物变量 | Open-Meteo 空气质量 JSON |
| `fetch-open-meteo-flood` | 拉取 Open-Meteo 洪水或流量数据 | 数据抓取 | `raw/environment` | 坐标、日期范围、日尺度变量 | Flood API JSON |
| `fetch-open-meteo-historical` | 拉取 Open-Meteo 历史气象和浅层土壤数据 | 数据抓取 | `raw/environment` | 坐标、日期范围、气象/土壤变量 | Historical API JSON |
| `fetch-openaq` | 通过 API 或 S3 路由获取 OpenAQ 数据 | 数据抓取 | `raw/environment` | source mode、参数、位置、时间窗 | OpenAQ 原始 JSON |
| `fetch-regulationsgov-comment-detail` | 根据 comment id 拉取 Regulations.gov 评论详情 | 数据抓取 | `raw/public` | `comment_ids` 或 anchor 文件 | 评论详情 JSON |
| `fetch-regulationsgov-comments` | 拉取 Regulations.gov 评论列表 | 数据抓取 | `raw/public` | docket、agency、query、时间窗 | 评论列表 JSON/JSONL |
| `fetch-usgs-water-iv` | 拉取 USGS 即时水文观测 | 数据抓取 | `raw/environment` | bbox/站点、参数码、时间窗 | 水文原始 JSON |
| `fetch-youtube-comments` | 基于 video id 拉取 YouTube 评论与回复 | 数据抓取 | `raw/public` | `video_ids`、anchor 文件、时间窗 | 评论线程 JSON |
| `fetch-youtube-video-search` | 搜索 YouTube 视频候选 | 数据抓取 | `raw/public` | `query`、频道过滤、发布时间窗、数量限制 | 视频列表 JSON |

## 归一化 Skills

| 名称 | 用途（简要概括） | 分类 | 层级 | 关键字段 | 主要产物 |
| --- | --- | --- | --- | --- | --- |
| `normalize-airnow-observation-signals` | AirNow 小时观测转统一环境信号 | 数据归一化 | `normalized/environment` | `run_dir`、`run_id`、`round_id`、`artifact_path` | `signal_plane.sqlite` 中环境信号、`artifact_refs` |
| `normalize-bluesky-cascade-public-signals` | Bluesky 帖子串转统一公共信号 | 数据归一化 | `normalized/public` | `run_dir`、`artifact_path`、帖子级联结构 | 公共信号行、`canonical_ids` |
| `normalize-gdelt-doc-public-signals` | GDELT DOC 文章结果转统一公共信号 | 数据归一化 | `normalized/public` | `run_dir`、`artifact_path`、DOC 查询结果 | 公共信号行、`artifact_refs` |
| `normalize-gdelt-events-public-signals` | GDELT Events zip 逐行转统一公共信号 | 数据归一化 | `normalized/public` | `artifact_path`、zip manifest、`max_rows_per_download`、`max_total_rows` | 公共信号行、zip 级 provenance |
| `normalize-gdelt-gkg-public-signals` | GDELT GKG zip 逐行转统一公共信号 | 数据归一化 | `normalized/public` | `artifact_path`、zip manifest、行数限制参数 | 公共信号行、zip 级 provenance |
| `normalize-gdelt-mentions-public-signals` | GDELT Mentions zip 逐行转统一公共信号 | 数据归一化 | `normalized/public` | `artifact_path`、zip manifest、行数限制参数 | 公共信号行、zip 级 provenance |
| `normalize-nasa-firms-fire-observation-signals` | NASA FIRMS 火点转统一环境信号 | 数据归一化 | `normalized/environment` | `run_dir`、`artifact_path`、火点表字段 | 环境信号行、`canonical_ids` |
| `normalize-open-meteo-air-quality-signals` | Open-Meteo 空气质量转统一环境信号 | 数据归一化 | `normalized/environment` | `run_dir`、`artifact_path`、小时模型字段 | 环境信号行、`artifact_refs` |
| `normalize-open-meteo-flood-signals` | Open-Meteo 洪水或流量转统一环境信号 | 数据归一化 | `normalized/environment` | `run_dir`、`artifact_path`、日尺度流量字段 | 环境信号行、`artifact_refs` |
| `normalize-open-meteo-historical-signals` | Open-Meteo 历史气象转统一环境信号 | 数据归一化 | `normalized/environment` | `run_dir`、`artifact_path`、天气/土壤时间序列 | 环境信号行、`canonical_ids` |
| `normalize-openaq-observation-signals` | OpenAQ 观测转统一环境信号 | 数据归一化 | `normalized/environment` | `run_dir`、`artifact_path`、provider/parameter 观测字段 | 环境信号行、`artifact_refs` |
| `normalize-regulationsgov-comment-detail-public-signals` | Regulations.gov 评论详情转统一公共信号 | 数据归一化 | `normalized/public` | `run_dir`、`artifact_path`、detail payload | 公共信号行、`canonical_ids` |
| `normalize-regulationsgov-comments-public-signals` | Regulations.gov 评论列表转统一公共信号 | 数据归一化 | `normalized/public` | `run_dir`、`artifact_path`、comment list | 公共信号行、`artifact_refs` |
| `normalize-usgs-water-observation-signals` | USGS 水文观测转统一环境信号 | 数据归一化 | `normalized/environment` | `run_dir`、`artifact_path`、站点/参数/时间点 | 环境信号行、`canonical_ids` |
| `normalize-youtube-comments-public-signals` | YouTube 评论转统一公共信号 | 数据归一化 | `normalized/public` | `run_dir`、`artifact_path`、评论线程结构 | 公共信号行、`artifact_refs` |
| `normalize-youtube-video-public-signals` | YouTube 视频结果转统一公共信号 | 数据归一化 | `normalized/public` | `run_dir`、`artifact_path`、视频元数据 | 公共信号行、`canonical_ids` |

## 检索、回溯与归档 Skills

| 名称 | 用途（简要概括） | 分类 | 层级 | 关键字段 | 主要产物 |
| --- | --- | --- | --- | --- | --- |
| `query-case-library` | 查询历史案例库中的相似调查 | 检索回溯 | `archive/history` | case library DB、主题词、过滤条件 | 历史案例摘要 JSON |
| `query-environment-signals` | 查询统一环境信号层 | 信号检索 | `normalized/environment` | metric、时间窗、空间范围、来源过滤 | 环境信号列表 JSON |
| `query-public-signals` | 查询统一公共信号层 | 信号检索 | `normalized/public` | 关键词、来源、时间窗、来源 skill | 公共信号列表 JSON |
| `query-signal-corpus` | 查询跨 run 历史信号库 | 检索回溯 | `archive/history` | signal corpus DB、主题词、过滤条件 | 历史信号引用 JSON |
| `query-normalized-signal` | 按 `signal_id` 精确查看规范化信号 | 取证回查 | `normalized` | `signal_id` | 单条 canonical signal 详情 |
| `query-raw-record` | 从规范化信号反查原始记录 | 取证回查 | `raw -> normalized` | `signal_id` 或 `artifact_path + record_locator` | 原始记录详情与 provenance |
| `materialize-history-context` | 将历史案例和历史信号整理成当前 round 可消费上下文 | 历史上下文 | `archive/history` | case library、signal corpus、当前主题 | 历史上下文 Markdown/JSON |
| `archive-case-library` | 将一次 run 的议会和汇报产物归档入案例库 | 归档 | `archive/history` | board、reporting、promotion 产物 | case library SQLite 记录 |
| `archive-signal-corpus` | 将一次 run 的规范化信号归档入跨 run 信号库 | 归档 | `archive/history` | `normalized_signals`、run 元数据 | signal corpus SQLite 记录 |

## 分析与证据加工 Skills

| 名称 | 用途（简要概括） | 分类 | 层级 | 关键字段 | 主要产物 |
| --- | --- | --- | --- | --- | --- |
| `build-normalization-audit` | 从 claim/observation 候选构建归一化审计视图 | 分析加工 | `analytics` | claim 候选、observation 候选、coverage 状态 | normalization audit JSON |
| `cluster-claim-candidates` | 聚合相似 claim 候选为可审查群组 | 分析加工 | `analytics` | claim candidates、语义指纹、provenance | claim cluster JSON |
| `derive-claim-scope` | 从 claim 侧推导匹配范围 | 分析加工 | `analytics` | claim evidence、地理和主题标签 | claim scope JSON |
| `derive-observation-scope` | 从 observation 侧推导匹配范围 | 分析加工 | `analytics` | observation evidence、metric/place 标签 | observation scope JSON |
| `extract-claim-candidates` | 从公共信号抽取待议 claim 候选 | 分析加工 | `analytics` | public signals、聚类阈值、文本字段 | claim candidates JSON |
| `extract-observation-candidates` | 从环境信号抽取 observation 候选 | 分析加工 | `analytics` | environment signals、metric/time/geo 字段 | observation candidates JSON |
| `link-claims-to-observations` | 将 claim 与 observation 建立支持/反驳关联 | 分析加工 | `analytics` | claim groups、observation groups、启发式分数 | claim-observation links JSON |
| `merge-observation-candidates` | 合并邻近或同类 observation 候选 | 分析加工 | `analytics` | observation candidates、空间/时间/指标相似度 | merged observations JSON |
| `promote-evidence-basis` | 将当前 round 提升为正式 evidence basis | 证据提升 | `analytics` | readiness、board brief、coverage 对象 | evidence basis JSON |
| `score-evidence-coverage` | 计算证据覆盖度与未解决缺口 | 分析加工 | `analytics` | links、scope proposals、coverage 指标 | coverage score JSON |

## 议会状态与看板 Skills

| 名称 | 用途（简要概括） | 分类 | 层级 | 关键字段 | 主要产物 |
| --- | --- | --- | --- | --- | --- |
| `claim-board-task` | 领取或更新 board 跟进任务 | 议会状态 | `board` | `task_id`、owner、source ids | board task 事件 |
| `close-challenge-ticket` | 关闭 challenge ticket 并记录结论 | 议会状态 | `board` | ticket id、resolution、linked refs | challenge closure 事件 |
| `open-challenge-ticket` | 为争议对象创建 challenge ticket | 议会状态 | `board` | target ids、evidence refs、reason | challenge ticket 事件 |
| `open-falsification-probe` | 从下一步动作中开启证伪 probe | 议会状态 | `board` | next-action queue、target、probe reason | falsification probe JSON |
| `post-board-note` | 向 board 追加调查笔记 | 议会状态 | `board` | note、linked refs、author role | board note 事件 |
| `query-board-delta` | 读取 board 增量事件和当前活跃对象 | 议会状态 | `board` | round id、cursor、事件范围 | board delta JSON |
| `summarize-board-state` | 汇总当前 board 状态为简洁快照 | 议会状态 | `board` | board artifact、事件、活跃对象 | board summary JSON |
| `summarize-round-readiness` | 判断当前轮是否可继续、阻塞或可提升 | 议会状态 | `board` | board、probe、coverage、next actions | readiness summary JSON |
| `update-hypothesis-status` | 创建或更新假设卡片及置信度 | 议会状态 | `board` | hypothesis id、claim ids、confidence | hypothesis card 事件 |
| `propose-next-actions` | 生成排序后的下一步调查动作队列 | 议会状态 | `board` | board summary、brief、coverage gaps | next-action queue JSON |

## 报告与发布 Skills

| 名称 | 用途（简要概括） | 分类 | 层级 | 关键字段 | 主要产物 |
| --- | --- | --- | --- | --- | --- |
| `draft-council-decision` | 起草 council decision 草案 | 报告发布 | `reporting` | reporting handoff、evidence basis | decision draft JSON |
| `draft-expert-report` | 起草角色化 expert report 草案 | 报告发布 | `reporting` | handoff、decision draft、role | expert report draft |
| `materialize-board-brief` | 将 board 状态整理成可读 brief | 报告发布 | `reporting` | board、readiness、重点对象 | board brief Markdown |
| `materialize-final-publication` | 汇总 decision/report/handoff 生成最终发布包 | 报告发布 | `reporting` | canonical decision、expert reports、audit refs | final publication artifact |
| `materialize-reporting-handoff` | 将证据、board、readiness 整理为统一交接对象 | 报告发布 | `reporting` | evidence basis、board brief、readiness | reporting handoff JSON |
| `publish-council-decision` | 发布正式 council decision | 报告发布 | `reporting` | decision draft、轮次约束、overwrite guard | canonical decision |
| `publish-expert-report` | 发布正式 expert report | 报告发布 | `reporting` | expert draft、role、overwrite guard | canonical expert report |

## 备注

1. 目前数据主干已经基本形成 `raw -> normalized -> analytics -> board -> reporting -> archive` 的层次。
2. 新迁移的数据源型 skills 已全部补齐 `SKILL.md` 与 `agents/openai.yaml` 元数据。
3. GDELT `events / mentions / gkg` 现已进入“zip 行级归一化”路线，不再只是 manifest 级占位。
