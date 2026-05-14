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
8. 环境数据压缩只能做描述性 coverage/statistics/index view，不替 agent 判断风险、归因或证据充分性。

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

### 3.4 环境证据压缩层升级

目标：让百万级环境 normalized signals 可以被 agent 安全使用，避免把原始时序或点事件全量塞入上下文；同时保留原始 evidence refs 和可复核链路。

当前问题：

1. `query-environment-signals` 已能做 item-level 查询，但不适合承载百万级全量阅读。
2. `aggregate-environment-evidence` 已有描述性聚合能力，但当前实现默认通过 `limit=500` 读取信号；对 USGS 这类百万级时序而言，它只是抽样覆盖摘要，不是全量压缩。
3. AirNow / OpenAQ / USGS / Open-Meteo / FIRMS 的关键字段差异很大，不能通过厚配置和 provider 专用规则堆出一个“统一智能分析器”。
4. 议会需要的是“可读的数据面摘要”，不是 runtime 或 skill 自动替 agent 做风险判断、来源归因、证据排序。

设计原则：

1. 保留 `aggregate-environment-evidence` 的原子能力定义：`descriptive environment evidence aggregation`。
2. 不按 provider 编排议程，不规定某案例必须使用哪种 source。
3. 不新增 source 权重、风险评分、严重性排序或 claim sufficiency 结论。
4. 不做专业水文、烟羽、化学、暴露或归因模型。
5. 只基于 normalized signal 共通字段和少量形态字段工作：
   - `source_skill`
   - `metric`
   - `numeric_value`
   - `observed_at_utc`
   - `latitude` / `longitude`
   - `evidence_refs`
   - `record_locator`
   - `metadata`
6. provider 差异只允许作为薄字段别名或 metadata 展示，例如 station/site id、satellite/instrument、confidence、FRP；这些字段不得驱动结论。

实现方案：

1. 扩展现有 `aggregate-environment-evidence`，不新建厚 skill。
2. 将内部代码拆成小模块，避免单文件继续膨胀：
   - `environment_evidence/common.py`
   - `environment_evidence/coverage.py`
   - `environment_evidence/timeseries.py`
   - `environment_evidence/point_events.py`
   - `environment_evidence/output.py`
3. `aggregate_environment_evidence.py` 只保留 CLI wrapper。
4. `--aggregation-method` 保持为形态方法，而不是 provider 方法：
   - `coverage-summary`
   - `time-series-summary`
   - `point-event-summary`
   - `auto-summary`
5. `--limit` 语义调整为输出/样本限制；全量统计应尽量通过 SQLite aggregation 或 chunked scan 完成。若无法全量统计，输出必须显式写 `sampling_status` 和 `sample_limit`。
6. 增加可选过滤参数，但不引入 agenda：
   - `--round-scope current|up-to-current|all`
   - `--source-skill`
   - `--metric`
   - `--observed-after-utc`
   - `--observed-before-utc`
   - `--bbox`
   - `--group-limit`
   - `--sample-ref-limit`

输出要求：

1. 所有方法都输出：
   - `sample_definition`
   - `aggregation_method`
   - `signal_count`
   - `source_distribution`
   - `metric_distribution`
   - `time_coverage`
   - `spatial_coverage`
   - `quality_or_metadata_limitations`
   - `evidence_ref_samples`
   - `source_signal_ref_samples`
   - `warnings`
2. `time-series-summary` 额外输出：
   - 按 `source_skill + station/site/location + metric` 分组的序列摘要。
   - 每组 `count`、`first_observed_at`、`last_observed_at`、`min`、`max`、`mean`。
   - 可选日/小时 bucket 的 `count/min/max/mean`。
   - 极值窗口只作为 descriptive extrema，不写风险等级。
3. `point-event-summary` 额外输出：
   - 日期 bucket 计数。
   - 空间包络 / bbox。
   - point density 只能作为记录密度描述，不能写风险等级。
   - 若存在 `frp`、`confidence`、`brightness` 等 numeric metadata，则输出 min/max/mean 和 missing count。
   - 若存在 satellite/instrument/provider 字段，则输出分布。
4. `auto-summary` 只根据 normalized signal 的形态选择 coverage/time-series/point-event 输出组合，不做案例语义判断。

Agent 使用边界：

1. `environmental-investigator` 可以读取环境聚合摘要来决定是否写 finding、evidence bundle、readiness 或继续提交 source acquisition proposal。
2. 聚合摘要不能直接等同于 finding；必须由 agent 显式承接。
3. `challenger` 应审查聚合摘要是否被过度解释，例如把点事件密度写成火源证明、把 PM2.5 峰值写成暴露评估、把 USGS downstream proxy 写成 direct operations record。
4. `moderator` 可以在 round synthesis 中引用聚合摘要的覆盖边界，但不能把聚合结果当成自动收口条件。

NYC smoke / Colorado River 使用建议：

1. NYC smoke 已封口，不重开调查结论；可补跑环境聚合摘要作为展示材料，说明 PM2.5、风场、FIRMS 的数据覆盖和边界。
2. 若补跑 NYC 聚合，报告措辞只可增强“证据面可读性”，不得升级为具体源火场或输送证明。
3. Colorado River 当前 USGS 数据量大，应优先用 `time-series-summary` 把百万级记录压成站点/指标/时间覆盖和极值窗口，再由 environmental-investigator 判断它是否仍只是 downstream/tributary context。
4. FIRMS 应使用 `point-event-summary`，不应走 time-series 逻辑。

测试要求：

1. 添加小型 fixture：AirNow/OpenAQ/USGS/Open-Meteo time-series，NASA FIRMS point-event。
2. 添加混合 source fixture，验证 `auto-summary` 不把 point-event 当作连续时序。
3. 添加大样本 synthetic fixture 或 SQLite fixture，验证全量统计不被 `limit` 截断。
4. 验证输出不包含 claim judgement、risk score、source ranking、readiness decision。
5. 验证 evidence refs/sample refs 被限制数量，避免生成超大 artifact。
6. 运行：
   - `python3 tools/quality_gate.py syntax`
   - `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting case-study`

完成标准：

1. 百万级环境信号可以生成小型可读摘要。
2. agent 不需要全量读取原始 JSON / receipt / normalized rows。
3. 聚合层保持 advisory/helper 属性。
4. 报告可引用 agent 承接后的聚合摘要，但不能直接外推风险或归因。
5. 代码未出现新的超大纠缠文件；若 environment aggregation 文件超过合理边界，应按上述内部模块拆分。

### 3.5 可复用 case-run 操作面

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

### 3.6 实验支撑与展示冻结的代码侧要求

本文不规定具体案例；案例安排见 `docs/openclaw-experiment-case-plan.md`。代码侧只需保证实验/展示可复用：

1. 同一报告链路可服务事件型调查和治理争议型调查。
2. case-run 操作面可以生成 mission、启动 run、处理 approval、生成报告、导出 runtime health。
3. 报告模板能展示 `environment evidence lane`、`public discourse lane`、`formal/policy record lane`、`claim boundary`。
4. public discourse summary 可以作为样本内结构写入报告，但不外推为总体民意。
5. source-family workflow 文档能帮助 agent 理解多层 fetch 能力，但不固定 source 或议程。
6. environment aggregation summary 可以作为环境证据覆盖和数值压缩面进入 agent deliberation，但不直接成为风险或归因结论。

## 4. 近期执行顺序

建议执行顺序：

1. 完成报告质量检查和报告模板最后收口。
2. 升级 `aggregate-environment-evidence`，补齐 time-series / point-event descriptive summary。
3. 检查 agent prompt / skill docs 是否仍有“总体民意”“source 排序”“固定议程”“环境聚合自动给出风险结论”类误导表述。
4. 冻结 NYC smoke 最终报告和展示摘要；如有必要，仅补充环境聚合展示材料，不升级结论。
5. 按实验计划继续第二主案例真实 run。
6. 只修程序性 blocker，不在 run 中显式指导 agent 调查方向。
7. 生成第二主案例最终叙事报告。
8. 进入论文写作和答辩材料制作。

## 5. 不做事项

提交前不做：

1. 大规模 runtime kernel 重构。
2. 新增专业水文、烟羽、化学或归因模型 skill。
3. 代表性民意估计。
4. source 排序、证据权重、固定议题模板。
5. 新增复杂 continuation round 类型。
6. 重写已有 fetch/normalize 架构。
7. 为兼容旧架构保留大量冗余代码。
8. 为每个 provider 编写独立厚配置或专用结论解释器。
9. 在环境聚合层输出 wildfire risk、health exposure、water-shortage severity、transport/source attribution 等结论。

## 6. 完成标准

工程完成标准：

1. 报告质量检查能阻止或警告主要越界表述。
2. 报告模板能清楚呈现环境信号、舆情语义、样本内结构和 claim boundary。
3. 环境聚合层能把 AirNow/USGS/OpenAQ/Open-Meteo 这类时序数据和 FIRMS 这类点事件数据压成可读摘要。
4. 至少两个主案例都能复用同一报告链路。
5. 实验计划中的轻量验证不需要推动代码架构扩张。
6. runtime health 绿色，关键测试通过。
7. docs 保留基础文档、timeline、本代码升级计划和实验/案例计划，删除过时专项工作计划。

论文展示完成标准：

1. 两个案例能说明系统不是单一事件脚本。
2. 两个案例都体现环境侧与舆情侧结合。
3. 报告明确区分样本内结构与总体民意。
4. 报告明确区分公共来源叙事与物理来源归因。
5. 能用图示说明 DB-first、agent council、skill、runtime operator 的边界。
