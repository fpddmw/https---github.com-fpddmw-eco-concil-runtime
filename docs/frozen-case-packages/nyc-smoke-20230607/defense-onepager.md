# 答辩展示页：NYC Smoke 基线案例

## 题目

`2023 年纽约烟霾事件的环境受体信号、区域输送相容性与公共舆情语义感知分析`

## 案例定位

本案例展示 OpenClaw 对“突发环境事件复盘”的处理能力：议会从用户 mission 出发，组织环境观测、公共讨论、舆情语义和报告边界，生成一份可审计的有边界叙事报告。

## 一句话结论

纽约在 2023 年 6 月 6 日至 9 日经历了一次时间边界清楚、强度突出的 PM2.5 烟霾过程；现有环境证据支持其与区域烟雾输送相容，但不足以证明具体源火场。公共舆情线可呈现样本内语义结构，但不能外推为总体民意。

## 议会流程

```text
用户 mission
  -> runtime scaffold + scoping-required
  -> moderator 组织调查范围和证据请求
  -> environmental-investigator 获取/复核环境证据
  -> social-investigator 获取/复核公共讨论证据
  -> challenger 审查来源归因、代表性和报告边界
  -> moderator 记录 round synthesis 并冻结 report basis
  -> report-editor 生成、校验、发布中文叙事报告
```

## 环境信号线

| 环节 | 发现 | 报告边界 |
| --- | --- | --- |
| AirNow PM2.5 | 6 月 6 日上升，6 月 7 日高峰，6 月 8 日仍严重，6 月 9 日回落 | 支持烟霾过程时序 |
| Open-Meteo 风场 | 核心窗口约 298-330 度方向风 | 支持北至西北方向输送相容 |
| NASA FIRMS 火点 | 加拿大东部存在区域火点背景 | 不能证明具体火场 |

## 舆情语义线

样本基础：

1. 476 条归一化公共语料。
2. GDELT 公共记录 351 条。
3. YouTube 公共样本 125 条。

样本内结构：

| 标签族 | 主要结果 | 解释边界 |
| --- | --- | --- |
| affect cues | 中性报道/转述约 90.4%，不确定/疑问约 11.0%，担忧约 7.4% | 样本内出现率，不是总体情绪 |
| issue cues | 来源/起因疑问约 67.0%，健康风险约 43.7%，信息求助/询问约 27.2% | 样本内议题结构 |
| source narrative | 区域野火烟雾约 74.1%，加拿大野火约 69.1% | 公共来源叙事，不是物理归因 |

GDELT tone 只作为媒体/文档语气，不作为公众情绪。

## Claim Boundary

| 能说 | 不能说 |
| --- | --- |
| 纽约烟霾过程具有清楚时间边界 | 已锁定具体起火点 |
| 环境证据与区域烟雾输送相容 | 已证明完整烟羽路径 |
| 公开视频和公共文本中存在事件可见性 | 公众总体如何看待事件 |
| 样本内存在健康风险、来源疑问、信息求助等语义结构 | 这些比例代表纽约公众或受影响人群 |

## 展示价值

1. 展示 DB-first、agent council、skill 和 runtime operator 的边界。
2. 展示环境信号与舆情语义如何在同一报告中协同。
3. 展示系统如何避免两类常见越界：把相容性写成归因、把样本内结构写成总体民意。
4. 展示最终报告可以面向决策者阅读，而不是运行日志。

## 冻结产物

1. 基线案例包：`docs/frozen-case-packages/nyc-smoke-20230607/baseline-case-package.md`
2. 最终报告：`runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/reporting/narrative_report_round-003.md`
3. 报告校验：`status=valid`，`error_count=0`，`warning_count=0`
4. Runtime health：`green`
