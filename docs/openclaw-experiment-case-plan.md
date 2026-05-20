# OpenClaw 毕业设计实验与案例计划

文档性质：本文只记录毕业设计展示用实验/案例安排，不规定代码开发任务，不作为 runtime 议程脚本，也不要求 agent 固定使用某个 source。代码层架构基线见 `docs/openclaw-project-overview.md`；毕业提交前的彻底升级路径见 `docs/openclaw-public-policy-situation-analysis-upgrade-plan.md`。

## 1. 实验目标

实验目标是证明 OpenClaw 不只是单一事件脚本，而是一个可用于生态环境形势分析的议会式调查框架。案例应覆盖不同任务驱动：

1. 事件型调查：围绕已发生环境事件，组织环境观测、公共讨论和证据边界。
2. 治理争议型调查：围绕政策/治理争议，组织环境背景、正式记录、公共叙事和多主体语义。

展示重点：

1. 环境侧数据不是装饰，而是约束语义分析和报告边界的证据线。
2. 舆情分析不是总体民意估计，而是样本内公共语义结构感知。
3. 议会流程不是固定脚本，而是由 moderator 和 investigators 自主推进。
4. 报告能明确区分环境事实、公共来源叙事、媒体 tone、公众样本表达和未证事项。

## 2. 案例组合

推荐采用“两主案例 + 两轻量验证案例”的组合。

主案例必须 end-to-end 运行并生成叙事报告。轻量验证案例只用于说明 source-family 能力覆盖，不要求完整议会报告。

### 2.1 主案例一：NYC Smoke

题目：

`2023 年纽约烟霾事件的环境受体信号、区域输送相容性与公共舆情语义感知分析`

任务类型：

`突发环境事件复盘`

展示价值：

1. 环境受体端：AirNow / Open-Meteo / NASA FIRMS 等环境证据组织。
2. 公共讨论端：GDELT / YouTube 等公开视频和媒体/公共记录语义。
3. 议会边界：可以说明区域输送相容性，不能证明具体源火场。
4. 舆情边界：可以说明样本内情绪、议题和来源叙事结构，不能推断总体民意。

当前状态：

1. 已完成多轮真实 run。
2. 已生成中文叙事报告。
3. 已纳入样本内公共舆情结构。
4. 已生成冻结基线案例包和答辩一页纸，可作为第一主案例冻结。

冻结材料：

1. `docs/frozen-case-packages/nyc-smoke-20230607/baseline-case-package.md`
2. `docs/frozen-case-packages/nyc-smoke-20230607/defense-onepager.md`
3. `runs/openclaw-realcase-nyc-smoke-skillguidance-validation-20260512/reporting/narrative_report_round-003.md`

### 2.2 主案例二：Colorado River / Glen Canyon

题目：

`科罗拉多河水资源短缺与格伦峡谷大坝运行争议中的水文信号、治理叙事与公共舆情语义感知分析`

任务类型：

`治理争议型调查`

推荐理由：

1. 与 NYC smoke 差异显著：不是突发空气质量事件，而是水资源治理争议。
2. 能使用项目中尚未充分展示的水文数据能力。
3. 能同时体现环境背景、正式记录、媒体叙事、公众讨论和多主体利益表达。
4. 议会任务更复杂：不是回答“发生了什么”，而是组织“水文压力、治理选择、利益主体和公众叙事如何相互作用”。

可用数据线：

1. 环境/水文：
   - `fetch-usgs-water-iv`
   - Colorado River / Lees Ferry / Glen Canyon 附近 streamgage。
   - Open-Meteo historical 作为气象/干旱背景辅助。
2. 正式/政策记录：
   - Bureau of Reclamation / DOI 近程 Colorado River operations、SEIS、public involvement materials。
   - 若可通过 Regulations.gov 抓取 docket/comment，则作为正式公众评论线；若抓取受限，则作为覆盖边界记录。
3. 公共/媒体：
   - GDELT DOC / Events / Mentions / GKG。
   - YouTube video/comments。
   - Bluesky search / cascade，如可取得相关讨论。

可分析语义：

1. 水资源短缺。
2. Glen Canyon Dam / Lake Powell 运行风险。
3. 下游供水、农业用水、城市供水。
4. 水电、生态流量、Grand Canyon ecosystem。
5. 部落权益、州际分配、联邦治理。
6. 气候变化、长期干旱、基础设施风险。

主要风险：

1. 范围较宽，mission 必须限定时间和议题边界。
2. 正式评论抓取不一定稳定。
3. 水文数据和政策争议之间不能写成强因果，只能作为环境压力背景。
4. 多主体叙事复杂，报告需要严格控制 claim boundary。

推荐 mission：

`请调查 2023 年前后科罗拉多河水资源短缺与格伦峡谷大坝运行争议，综合水文背景、公开报道、正式记录和公众讨论，分析该议题中的环境压力信号、主要治理争议、公共舆情语义结构和证据边界，并生成一份适合决策者阅读的有边界报告。`

当前准备状态：

1. 已创建最小用户面向 mission：`runs/openclaw-realcase-colorado-river-glen-canyon-governance-20260514/input/mission.json`。
2. 已通过 `start-council-run` 初始化 `round-001`，run 健康状态为 `green`。
3. 当前入口为 `scoping-required`：mission 未预设 window、region、source 或 hypotheses，后续由 moderator 和 investigators 自主提出调查边界、source proposal 和证据路线。
4. 已生成 5 个 agent registration surfaces：`moderator`、`environmental-investigator`、`social-investigator`、`challenger`、`report-editor`。
5. 初始 fetch plan step count 为 0，这是预期状态；不把实验计划中的可用数据线转化为 runtime source 队列。

降级策略：

1. 若正式评论抓取失败，保留 Reclamation / Federal Register / public involvement 页面作为正式记录线，明确 Regulations.gov coverage gap。
2. 若水文数据范围过宽，限定到 Lees Ferry / Glen Canyon / Lake Powell 相关站点和 2023 年关键政策窗口。
3. 若公众讨论样本稀疏，使用 GDELT 媒体/公共记录 tone 和 YouTube/Bluesky 样本内讨论作为有限公共语义线。

参考依据：

1. Bureau of Reclamation public involvement 页面记录，2023 年 Revised Draft SEIS comment period 收到 596 submissions，包括 public、Tribes、states、federal agencies、NGOs 等。
2. AP 等公开报道记录，低水位和 Glen Canyon Dam outlet works 风险引发水资源输送和基础设施关注。

参考链接：

1. https://usbr.gov/ColoradoRiverBasin/interimguidelines/seis/publicinvolvement.html
2. https://apnews.com/article/25901fb7e9f6896a27c1493a4bbf22f3

## 3. 轻量验证案例

轻量验证案例不要求完整 end-to-end 报告。目标是用较低成本展示 source-family 覆盖面和系统泛化潜力。

### 3.1 轻量验证一：PM2.5 NAAQS

题目：

`2024 年 EPA PM2.5 NAAQS 修订中的正式公众评论与媒体/公共语义结构 smoke test`

任务类型：

`政策/规则评论型验证`

用途：

1. 验证 Regulations.gov comments / detail 工作流。
2. 验证 GDELT 媒体 tone 与正式评论主题的对照。
3. 作为主案例二的备选方案。

优点：

1. 稳定性高。
2. EPA docket、final rule、response-to-comments 资料清楚。
3. 正式评论量大，适合展示 comment analysis。

限制：

1. 与 NYC smoke 都属于空气质量大类，展示差异弱于 Colorado River。
2. 环境数据主要作为背景，不如 Colorado River 对水文 source-family 的展示价值高。

参考链接：

1. https://www.epa.gov/pm-pollution/final-reconsideration-national-ambient-air-quality-standards-particulate-matter-pm
2. https://www.epa.gov/system/files/documents/2024-02/pm-naaqs_response-to-comments-document_final.pdf

### 3.2 轻量验证二：USGS 水文数据 smoke test

题目：

`USGS 水文观测 fetch/normalize/query 能力 smoke test`

任务类型：

`环境数据源能力验证`

用途：

1. 只验证 `fetch-usgs-water-iv`、normalize 和 query 是否能稳定形成 evidence refs。
2. 可选择 Colorado River 相关站点，也可选择 Vermont flood 相关站点。
3. 不要求完整舆情分析和最终报告。

限制：

1. 不作为毕业答辩主案例。
2. 不写强水文归因。
3. 只用于证明水文 source-family 可接入、可归一化、可被议会引用。

## 4. PM2.5 NAAQS 与 Colorado River 的选择比较

| 维度 | PM2.5 NAAQS | Colorado River / Glen Canyon |
| --- | --- | --- |
| 任务类型 | 政策/规则评论 | 水资源治理争议 |
| 与 NYC 差异 | 调查模式不同，但仍是空气质量 | 数据域和任务模式都不同 |
| 稳定性 | 高 | 中 |
| 环境侧展示 | AirNow/OpenAQ 背景，较弱 | USGS 水文和水资源背景，较强 |
| 正式记录 | EPA docket/response-to-comments 稳定 | Reclamation/SEIS/public involvement 稳定，Regulations.gov 抓取不确定 |
| 舆情语义 | 正式评论、媒体、社媒 | 多主体治理叙事、媒体、社媒 |
| 风险 | 低 | 中高 |
| 架构展示价值 | 稳妥 | 更强 |

结论：

1. 若时间和运行稳定性优先，PM2.5 NAAQS 是更稳备选。
2. 若希望展示 OpenClaw 的泛化能力和水文 source-family，Colorado River 更适合作为第二主案例。
3. 当前建议：Colorado River 作为第二主案例；PM2.5 NAAQS 作为轻量验证和失败降级备选。

## 5. 实验执行原则

执行案例 run 时应沿用此前测试要求：

1. 只在执行议会流程命令时向用户摘抄和说明命令用途。
2. 代码调试、测试、修 bug 的命令不作为议会流程汇报。
3. 若 moderator 发起审批，由 runtime-operator 执行授权。
4. 只修程序性 blocker，不显式指导议会调查方向或阶段结论。
5. mission 面向用户表达，不预设调查答案、不指定必须使用的 source、不规定 round 数。
6. 若 agent 检索失败，只能修工具/程序问题，不能替 agent 填写调查结论。

## 6. 展示材料安排

最终答辩建议展示：

1. 两个主案例：
   - NYC smoke：事件型调查。
   - Colorado River / Glen Canyon：治理争议型调查。
2. 两个轻量验证：
   - PM2.5 NAAQS：正式公众评论/政策语义。
   - USGS 水文：环境数据源接入。
3. 一张案例矩阵图：
   - 横轴：事件型 / 治理争议型。
   - 纵轴：环境信号 / 舆情语义。
4. 一张系统流程图：
   - mission -> agents -> fetch/normalize -> signal DB -> optional analysis -> council objects -> report basis -> narrative report。
5. 一张 claim boundary 表：
   - 环境事实、公共叙事、媒体 tone、样本内公众表达、未证事项。

## 7. 完成标准

主案例完成标准：

1. 有 mission。
2. 有至少一轮真实议会运行。
3. 有环境侧 evidence refs。
4. 有公共/正式语义 evidence refs。
5. 有 report basis / synthesis / decision 或等价收口对象。
6. 有中文最终叙事报告。
7. 报告明确写出 claim boundary。

轻量验证完成标准：

1. 能运行对应 source-family 的 fetch / normalize / query。
2. 能输出 evidence refs 或明确记录程序性失败原因。
3. 不要求完整报告。
4. 不推动代码架构扩张。
