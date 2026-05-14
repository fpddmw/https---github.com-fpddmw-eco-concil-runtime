# OpenClaw 毕业提交前最终升级路径工作计划

文档性质：本文是 2026-05-29 毕业论文提交前的最终代码层工程收口计划。它整合此前公共舆情深化、source-family workflow、claim-strength obligations、NYC smoke run 复盘和报告质量讨论；它不是固定议程脚本，不规定 agent 必须选择的 source，不引入证据权重或排序。实验/案例选择另见 `docs/openclaw-experiment-case-plan.md`。

## 1. 时间约束和目标定位

当前目标不是继续扩大系统边界，而是在保留 agent 自主调查权的前提下，把系统稳定成可用于论文和答辩展示的研究原型。

时间约束：

1. 论文提交日期：2026-05-29。
2. 论文撰写至少需要一周多时间，工程侧应在 2026-05-20 前进入展示冻结状态。
3. 2026-05-20 后只做文档、报告措辞、截图和演示材料修订，不再做大规模 runtime 重构。

答辩定位：

`面向生态环境形势分析大模型的舆情分析与语义感知研究`

系统展示重点：

1. 生态环境事件中的环境信号组织。
2. 公共舆情与媒体语义感知。
3. 多 agent 议会式证据讨论。
4. 报告生成中的 claim boundary 控制。
5. 可审计、可复核、可恢复的运行过程。

## 2. 不变原则

以下原则不得在本轮收口中破坏：

1. `runtime` 不编排议会议程、不选择 source、不判断证据充分性。
2. `moderator` 是议会组织者，负责 round brief、synthesis、transition 和收口判断。
3. `investigator` 保留自主调查权，自主提出 source acquisition、选择 fetch/normalize/query/analysis 能力组合。
4. `skill` 提供取证、归一化、辅助分析、报告生成能力；默认不替 agent 采信证据。
5. 公共舆情深化是 optional capability lane，不是新的固定 round type。
6. 报告前质量检查只检查“表述是否有证据基础”，不替议会决定结论。
7. 不引入 source 排序、证据打分、非必要启发式权重或固定议题模板。

## 3. 最终升级范围

本轮升级应聚焦“展示稳定性”和“报告可信度”，不做架构性扩张。

### 3.1 报告前质量检查

目标：防止报告写出超出 frozen basis 的结论，同时不干预 investigator 的调查自由。

质量检查应覆盖：

1. 若报告要写样本内情绪、议题、来源叙事比例，必须有 public discourse summary、annotation aggregation 或等价 DB-backed / approved helper basis。
2. 若报告要写总体民意、受影响人群整体观点、平台整体情绪，必须阻止或强警告，除非 mission 明确提供代表性抽样设计。
3. 若报告要写公共来源叙事，必须明确它是 `source narrative cue`，不能替代 `physical source attribution`。
4. 若报告要写环境来源、输送、因果归因，必须检查是否存在环境证据 bundle / finding / hypothesis / challenger review；缺少专业归因模型时只能写相容性或描述性关系。
5. 若存在 zero-signal、receipt-only、failed、blocked 或 executed-without-normalized-refs 的 acquisition attempt，报告收口必须说明是否仍有可行动调查路径。
6. 若报告使用 optional-analysis helper 输出，必须检查该输出是否被 agent position、finding、readiness、synthesis 或 report basis 显式承接。

实现边界：

1. 质量检查可以是 reporting skill / validation skill 的检查项。
2. 质量检查输出 warning/error 和可读解释。
3. 质量检查不决定是否开新 round，只把风险暴露给 moderator / report-editor。

### 3.2 最终报告模板优化

目标：让报告适合决策者、毕业论文案例展示和答辩阅读，而不是 runtime 日志。

模板应稳定保留：

1. `结论先行`：一句中心判断 + 一句证据链 + 一句边界。
2. `一页要点`：4-6 条可展示 bullet。
3. `事情如何发展`：按事件时间线和议会调查逻辑叙述。
4. `环境信号与舆情语义双线结构`：明确环境 evidence lane 和 public discourse lane 各自贡献。
5. `公共舆情深化补充`：样本量、source-family 构成、样本内标签结构、GDELT media tone、边界。
6. `还不能证明什么`：把限制放在正文，而不是只放审计索引。
7. `决策使用建议`：说明报告可用于什么、不应用于什么、若需强结论需要补什么。
8. `审计索引`：保留 refs，但不让 refs 支配正文。

舆情表述标准：

1. 可以写“本轮样本内出现率约 X%”。
2. 必须写明标签可能非互斥，不应相加为 100% 意见构成。
3. 必须写明样本内出现率不是受影响人群总体比例。
4. GDELT tone 只能写媒体/文档语气，不写公众情绪。

### 3.3 Agent 提示词和 skill 文档收口

目标：让 agent 正确理解已有能力，但不把 prompt 写成固定调查剧本。

需要收口的提示要求：

1. `social-investigator`
   - 看到公共文本样本时，应考虑 public visibility、issue cues、affect cues、source narrative cues、sample boundary。
   - 不直接手写逐条情感标签；情感/议题/来源叙事标签应由 bounded annotation worker 或 approved taxonomy 产生。
   - 对公共来源叙事必须提示 environmental-investigator 做物理验证。
2. `environmental-investigator`
   - 面对公共来源假说时，负责判断已有环境证据能支持“相容性”“时间空间关系”还是“强归因”。
   - 在缺少轨迹、烟羽、化学或专业归因模型时，不把环境关系写成具体源头证明。
3. `challenger`
   - 默认审查 claim boundary、sample boundary、taxonomy fit、误入样本、外推风险和报告措辞。
   - 不要求逐条复核所有非 GDELT 情绪标签。
4. `report-editor`
   - 只消费 frozen / reporting basis。
   - 必须把样本内结构和总体民意区分开。
   - 必须把公共来源叙事和物理来源归因区分开。

需要收口的 skill 文档：

1. `draft-narrative-report`
   - 已加入样本内结构与非代表性边界要求；后续只做措辞精修。
2. public discourse optional-analysis skills
   - 保持 advisory/helper 属性。
   - 输出应包含 sample definition、source-family counts、distribution、warnings、evidence refs。
3. source-family workflows
   - 保持为“如何理解同一信源家族多层能力”的说明，不作为 source 排序或议程脚本。

### 3.4 可复用 case-run 操作面

目标：降低第二案例执行成本，但不把 case-run 流程变成 agent 议程模板。

允许形成：

1. human/operator runbook：如何 scaffold mission、启动 run、注册 agents、查看状态、处理 approval、生成报告。
2. 报告发布 checklist：草稿、校验、审批、发布、runtime health。
3. 案例归档 checklist：保存 mission、timeline、最终报告、关键 artifacts、截图。

禁止形成：

1. 规定某案例必须查哪些 source。
2. 规定 investigator 必须按固定顺序执行 fetch。
3. 规定必须开几轮或必须得出某结论。
4. 把 runbook 写进 runtime 作为 agenda rule。

### 3.5 实验支撑与展示冻结的代码侧要求

本文不规定具体案例；案例安排见 `docs/openclaw-experiment-case-plan.md`。代码侧只需保证实验/展示可复用：

1. 同一报告链路可服务事件型调查和治理争议型调查。
2. case-run 操作面可以生成 mission、启动 run、处理 approval、生成报告、导出 runtime health。
3. 报告模板能展示 `environment evidence lane`、`public discourse lane`、`formal/policy record lane`、`claim boundary`。
4. public discourse summary 可以作为样本内结构写入报告，但不外推为总体民意。
5. source-family workflow 文档能帮助 agent 理解多层 fetch 能力，但不固定 source 或议程。

## 4. 近期执行顺序

建议执行顺序：

1. 完成报告质量检查和报告模板最后收口。
2. 检查 agent prompt / skill docs 是否仍有“总体民意”“source 排序”“固定议程”类误导表述。
3. 冻结 NYC smoke 最终报告和展示摘要。
4. 按实验计划启动第二主案例真实 run。
5. 只修程序性 blocker，不在 run 中显式指导 agent 调查方向。
6. 生成第二主案例最终叙事报告。
7. 进入论文写作和答辩材料制作。

## 5. 不做事项

提交前不做：

1. 大规模 runtime kernel 重构。
2. 新增专业水文、烟羽、化学或归因模型 skill。
3. 代表性民意估计。
4. source 排序、证据权重、固定议题模板。
5. 新增复杂 continuation round 类型。
6. 重写已有 fetch/normalize 架构。
7. 为兼容旧架构保留大量冗余代码。

## 6. 完成标准

工程完成标准：

1. 报告质量检查能阻止或警告主要越界表述。
2. 报告模板能清楚呈现环境信号、舆情语义、样本内结构和 claim boundary。
3. 至少两个主案例都能复用同一报告链路。
4. 实验计划中的轻量验证不需要推动代码架构扩张。
5. runtime health 绿色，关键测试通过。
6. docs 保留基础文档、timeline、本代码升级计划和实验/案例计划，删除过时专项工作计划。

论文展示完成标准：

1. 两个案例能说明系统不是单一事件脚本。
2. 两个案例都体现环境侧与舆情侧结合。
3. 报告明确区分样本内结构与总体民意。
4. 报告明确区分公共来源叙事与物理来源归因。
5. 能用图示说明 DB-first、agent council、skill、runtime operator 的边界。
