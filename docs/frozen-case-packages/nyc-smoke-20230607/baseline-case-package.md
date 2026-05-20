# NYC Smoke 基线案例包

文档性质：本文是毕业设计展示用的 NYC smoke 冻结基线案例包。它整理既有 run 产物，不新增事实、不重跑议会、不改变 report basis。跨案例运行与 skill 使用诊断见 `docs/diagnostics/openclaw-skill-usage-matrix.md` 和 `docs/diagnostics/openclaw-case-rerun-and-skill-uptake-audit.md`。

## 1. 案例身份

| 字段 | 内容 |
| --- | --- |
| 案例类型 | 突发环境事件复盘 |
| Run ID | `openclaw-realcase-nyc-smoke-skillguidance-validation-20260512` |
| 主题 | `2023 New York City smoke haze event` |
| 核心问题 | 2023 年纽约烟霾事件发生了什么、环境证据能支持什么解释、公共讨论呈现出什么语义结构、哪些结论仍不能升级 |
| 当前状态 | 可作为冻结基线案例展示 |

## 2. Mission

Mission 文件：

1. `runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/input/mission.json`
2. `runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/mission.json`

用户请求原文：

> 请调查 2023 年纽约烟霾事件：说明发生了什么、可能原因是什么、哪些证据支持或限制这些判断，并给出证据支持的结论。

Mission 边界：

1. mission 是用户面向议会的请求信封，不是 moderator 的调查计划。
2. mission 没有预设来源地、具体火场、必须使用的 source 或固定 round 数。
3. 缺少完整 window / region 时，runtime 保持 `scoping-required`，由 moderator 和 agents 自主提交 scope、round brief、evidence request 和 source proposals。

## 3. 最终产物

| 产物 | 路径 / ID | 状态 |
| --- | --- | --- |
| 最终中文叙事报告 | `runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/reporting/narrative_report_round-003.md` | 已发布 |
| 报告 JSON | `runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/reporting/narrative_report_round-003.json` | 已发布 |
| Publication ID | `narrative-report-d5171532a6c7` | 已发布 |
| Draft ID | `narrative-report-draft-d8fc098b2dd3` | 已验证 |
| Validation ID | `narrative-report-validation-4d43d94b836f` | `valid` |
| Runtime health | `runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/runtime/runtime_health.json` | `green` |

报告校验状态：

1. `status=valid`
2. `error_count=0`
3. `warning_count=0`
4. `publish_allowed=true`

Runtime 状态：

1. `alert_status=green`
2. `blocked_event_count=0`
3. `failed_event_count=0`
4. `open_dead_letter_count=0`
5. `receipt_conflict_count=0`
6. `runtime_lock_state=released`

## 4. 议会流程摘要

| 阶段 | 主要动作 | 角色分工 | 结果 |
| --- | --- | --- | --- |
| Scoping / round-001 | mission scaffold、初步环境和公共信号抓取、证据请求和 finding 形成 | moderator 组织；environmental-investigator / social-investigator 取证；challenger 检查边界 | 形成环境受体、风场、火点、公共可见性等初步证据，但仍需补充公共讨论线 |
| Continuation / round-002 | 承接第一轮问题，补充 YouTube public-discourse lane，完成 readiness、round synthesis、report basis freeze | social-investigator 补充公开视频证据；environmental-investigator 复核环境链；challenger 接受有限用法；moderator 决定冻结报告基础 | `round-synthesis-a7f75f034c1b` 记录非继续理由和报告边界 |
| Reporting / round-003 | 公共舆情深化摘要进入报告补充，report-editor 起草、校验、发布中文叙事报告 | report-editor 只消费 frozen basis 和已承接的舆情摘要；runtime-operator 审批发布 | 生成 `narrative-report-d5171532a6c7` |

关键 round synthesis：

`round-synthesis-a7f75f034c1b`

综合判断：

1. 没有开放证据请求、待执行 source-acquisition proposal、not-ready readiness opinion 或开放 challenge。
2. 剩余 unresolved ref 是已被 challenger 接受有限用法的 social finding，不是未审查 blocker。
3. 正确动作是冻结 report basis，而不是继续开启调查轮。
4. 报告边界必须停留在 bounded descriptive + limited relational context，不做 causal/source attribution。

## 5. 关键证据链

### 5.1 环境证据线

| 证据层 | 来源/能力 | 报告中可支持的判断 | 不可升级为 |
| --- | --- | --- | --- |
| 受体端 PM2.5 | AirNow hourly observations | 纽约地区 PM2.5 在 2023-06-06 上升，2023-06-07 达到高位，2023-06-08 仍严重，2023-06-09 快速回落 | 单站点异常值、完整暴露评估 |
| 风场背景 | Open-Meteo historical | 核心窗口纽约上空风向约 298-330 度，与北至西北方向输送相容 | 反向轨迹或烟羽路径证明 |
| 火点背景 | NASA FIRMS | 加拿大东部在事件前后存在大量火点背景 | 某个具体火场产生纽约烟霾负荷 |

环境线中心表述：

> 现有环境证据支持纽约烟霾过程与区域烟雾输送相容，但不足以把烟霾负荷归因到某一个具体火场。

### 5.2 公共讨论与舆情语义线

| 证据层 | 来源/能力 | 报告中可支持的判断 | 不可升级为 |
| --- | --- | --- | --- |
| 公开视频可见性 | YouTube video search / normalize | 2023-06-07 至 2023-06-08 存在同时期公开可见的纽约烟霾视频记录 | 全平台覆盖、代表性舆情 |
| 评论/公共文本样本 | YouTube comments + GDELT public records | 样本内可见健康风险、来源疑问、信息求助等语义结构 | 受影响人群总体观点 |
| GDELT media tone | Events / Mentions / GKG numeric tone | 媒体/文档语气整体偏负 | 公众情绪 |
| 来源叙事 | annotation worker + aggregation | 样本中出现“区域野火烟雾”“加拿大野火”等来源叙事 | 物理来源归因 |

公共语料规模：

1. 总计 476 条已归一化公共语料。
2. GDELT 公共记录 351 条。
3. YouTube 公共样本 125 条。

样本内标签结构：

| 标签族 | 样本内主要结果 | 边界 |
| --- | --- | --- |
| affect cues | 中性报道/转述 123 条，约 90.4%；不确定/疑问 15 条，约 11.0%；担忧 10 条，约 7.4% | 标签可能非互斥，不代表总体公众情绪 |
| issue cues | 来源/起因疑问 69 条，约 67.0%；健康风险 45 条，约 43.7%；信息求助/询问 28 条，约 27.2% | 样本内议题结构，不是社会总体议题比例 |
| source narrative cues | 未说明或未提及来源 76 条，约 93.8%；区域野火烟雾 60 条，约 74.1%；加拿大野火 56 条，约 69.1% | 公共文本如何谈论来源，不是环境归因 |

GDELT tone：

1. Events AvgTone 平均 `-0.472585`。
2. Mentions 文档 tone 平均 `-0.828918`。
3. GKG V2Tone 平均 `-0.209294`。
4. 这些是媒体/文档语气，不是公众情绪。

## 6. Claim Boundary 表

| 报告可以说 | 报告不能说 | 升级所需证据 |
| --- | --- | --- |
| 纽约在 2023-06-06 至 2023-06-09 经历了一次时间边界清楚、强度突出的 PM2.5 烟霾过程 | 已经证明某个具体源火场 | 反向轨迹、烟羽影像、化学指纹、专业归因模型或等价证据 |
| 受体峰值、风向和加拿大东部火点背景与区域输送相容 | 已证明完整烟羽输送路径 | 轨迹/烟羽产品和更强时空匹配 |
| 公开视频渠道中存在同时期可见的纽约烟霾记录 | 公众总体如何看待事件 | 代表性抽样设计或可信民意数据 |
| 样本内可见健康风险、来源疑问、信息求助等语义结构 | 某类情绪在受影响人群中的比例 | 明确抽样框、去重、分层、加权和不确定性表达 |
| 样本内存在加拿大野火等来源叙事 | 舆情来源叙事证明物理来源 | 环境线独立验证 |

## 7. 答辩展示要点

建议在答辩中用本案例说明四件事：

1. OpenClaw 能把环境数据、公共讨论和议会推理组织成可审计链路。
2. 议会没有把“可见公共讨论”误写成“代表性公众情绪”。
3. 议会没有把“区域输送相容性”误写成“具体源火场归因”。
4. 报告能提供决策者可读的有边界结论，而不是运行日志。

建议展示图：

`mission -> scoping -> environmental/public fetch -> normalize -> signal DB -> council objects -> report basis freeze -> public discourse summary -> narrative report`

## 8. 冻结校验清单

| 检查项 | 结果 |
| --- | --- |
| 最终中文报告存在 | 通过 |
| 报告 validation valid | 通过 |
| validation error/warning 为 0 | 通过 |
| runtime health green | 通过 |
| 无 open dead letter | 通过 |
| 报告区分样本内结构和总体民意 | 通过 |
| 报告区分公共来源叙事和物理来源归因 | 通过 |
| 报告保留审计 refs | 通过 |

## 9. 可引用路径

1. 最终报告：`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/reporting/narrative_report_round-003.md`
2. 报告 JSON：`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/reporting/narrative_report_round-003.json`
3. 报告校验：`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/reporting/narrative_report_validation_round-003.json`
4. 公共舆情摘要：`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/analytics/public_discourse_sample_summary_round-003.json`
5. 冻结报告基础：`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/report_basis/frozen_report_basis_round-002.json`
6. Runtime health：`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/runtime/runtime_health.json`
