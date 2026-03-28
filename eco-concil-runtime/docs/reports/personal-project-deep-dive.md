# 生态议会运行时项目深度评估报告（个人版）

## 1. 先给结论

如果只用一句话概括当前项目，我的判断是：

这是一个“监督器驱动、文件边界清晰、审计意识较强”的调查运行时，其工程骨架已经基本成立，但调查能力本身仍偏启发式和规则化，离真正强调查、强反证、强竞争性推理还有明显距离。

更具体地说：

- 结构设计总体合理，而且比早期原型成熟得多。
- 可追溯、可复盘、可审计性已经是项目的突出长板。
- 调查与推理能力有雏形，但目前更像“受约束的证据流水线”，不是“开放式调查智能体”。
- 下一阶段最值得投入的，不是继续加更多角色，而是补强调查能力的内核：假说竞争、反证优先、检索排序、调查动作生成和跨轮记忆利用。

## 2. 系统定位：它到底是什么，不是什么

它是什么：

- 一个围绕环境事件调查构建的运行时。
- 一个把主持人、社会学专家、环境学专家放进固定工位的多角色流程系统。
- 一个把原始数据采集、规范化、匹配、报告、决策拆成可落盘阶段的工程框架。

它不是什么：

- 不是一个可以自由浏览、自由分解问题、自由调用任意工具的开放式 agent。
- 不是一个主要依赖 LLM 自主思考完成所有调查的系统。
- 不是“问一句就直接给结论”的黑盒问答器。

这点必须讲清楚，因为对项目的优劣判断几乎都取决于此。当前版本的优势，不在于“模型自己非常聪明”，而在于“模型活动被嵌入了一个受治理、可回放、可审计的流程框架”。

## 3. 端到端工作流：一轮调查到底怎么跑

当前工作流可以概括为六段。

### 3.1 主持人定任务

输入通常是 `mission.json` 和本轮 `tasks.json`。

主持人并不直接规定“抓哪个源”，而是描述证据需求、验证目标和本轮优先事项。这个边界很重要，因为系统想把“任务需求”和“数据源选择”分开。

### 3.2 两个专家做 source selection

社会学专家负责公众信息侧，环境学专家负责物理观测侧。

这一阶段是 LLM 参与的，但不是自由发挥。系统会先构造 `source_selection_packet.json`，其中包含：

- mission
- 当前调查计划
- 当前角色的 causal focus
- 任务列表
- evidence requirements
- allowed sources
- source governance
- family memory
- 当前已有 source selection
- 已存在的 override requests

模型只是在这个包里做“受约束选择”，最终必须产出满足合同的 `source_selection.json`。

### 3.3 系统根据选源结果生成 fetch plan

这一步不再依赖 LLM。它是本地确定性的计划合成。

系统读取：

- mission
- tasks
- sociologist/environmentalist 的 source selection

然后经过治理校验，拼出一个 `fetch_plan.json`。同时还记录输入快照，后续如果 `tasks.json` 或 `source_selection.json` 发生变化，就会拒绝继续执行旧计划，要求重新 prepare。

### 3.4 执行抓取并落原始数据

这一阶段强调“只写 raw artifacts”，采集与分析严格分开。这样做的意义是后续任何规范化、匹配和判断都可以回放到相同的原始输入上。

### 3.5 确定性数据平面

这是项目当前最硬核、也最能体现工程性的部分。它把原始数据变成：

- claim candidates
- observation candidates
- curated claims / observations
- matching result
- evidence cards
- isolated entries
- remands
- investigation state
- investigation actions

这里面有少量人工审阅节点，但“候选如何形成、匹配如何发生、证据如何生成”的主逻辑基本是确定性的。

### 3.6 专家报告与主持人决策

后半段又回到 LLM 参与，但仍是 packet + prompt + contract 的模式。

系统会先生成各种 packet 和 draft scaffold，然后模型补齐：

- claim curation
- observation curation
- readiness report
- matching authorization
- matching adjudication
- investigation review
- expert report
- council decision

模型输出不是直接进入系统，而是必须满足 JSON contract，并且与既有 run_id、round_id、authorization_id 等标识一致。

## 4. 规则、LLM、合同三者的边界

这是你最需要在个人版里讲透的部分。

### 4.1 确定性规则主导的环节

- 路径与目录规范。
- round 和 role 的文件边界。
- fetch plan 合成。
- 原始信号到 claim candidates / observation candidates 的生成。
- claim 与 observation 的匹配过滤。
- support / contradiction 分数累计。
- evidence card、isolated、remand、investigation state、investigation actions 的生成。
- 历史检索查询对象的构造与检索结果排序。

这些步骤的好处是可复现、可测试、可审计，坏处是灵活性和泛化能力受限。

### 4.2 LLM 参与但被强约束的环节

- source selection
- claim curation
- observation curation
- readiness report
- matching authorization
- matching adjudication
- investigation review
- expert report
- council decision

这些环节不是随便输出文本，而是基于 packet、prompt、validation command 和 contract schema 生成受约束 JSON。

### 4.3 JSON 合同约束了什么

它至少约束了三类东西：

- 结构：字段必须存在，层级必须正确。
- 身份一致性：run_id、round_id、agent_role、authorization_id 等必须与当前上下文一致。
- 选择范围：只能引用 packet 中已有的 claim_id、observation_id、evidence_id，不能凭空捏造。

但也必须清醒地看到，合同主要解决“格式和引用边界正确”，并不自动保证“语义判断真的对”。也就是说，模型不能乱写字段，但仍可能在允许范围内做出不够好的判断。

### 4.4 一张表看全流程主驱动

| 阶段 | 主要输入 | 主驱动机制 | 是否允许 LLM 自行组合 |
| --- | --- | --- | --- |
| 任务设定 | mission、tasks | 主持人审阅 + 合同化任务文件 | 允许，但输出被任务结构限制 |
| source selection | task packet、governance、allowed sources | LLM 受治理约束选择 | 允许在治理边界内组合 |
| fetch plan | mission、tasks、validated source selection | 本地确定性合成 | 不允许 |
| raw fetch | fetch plan | 本地执行器 / 外部命令 | 不允许 |
| claim candidate | public signals | 关键词规则 + 文本指纹聚类 | 不允许 |
| observation candidate | environment signals | 指标归一 + 分桶聚合 + 规则打标 | 不允许 |
| curation | candidate pool、investigation plan | LLM 受 packet 和 contract 约束筛选 / 组合 | 允许，但只能引用已有候选 |
| matching | curated claims、curated observations | 规则过滤 + 阈值评分 | 不允许 |
| evidence / remand | matches | 本地确定性物化 | 不允许 |
| investigation state / actions / history | round state、case library | 规则汇总 + 启发式排序 | 不允许 |
| reports / decision | state packet、draft scaffold | LLM 受 packet 和 contract 约束撰写 | 允许，但不能越权发明事实 |

## 5. Source Selection 机制：到底是怎么“选源”的

这一步容易被说得太抽象，实际代码里的逻辑比“让 LLM 自己想想”要硬得多。

### 5.1 输入是什么

系统在 `controller/source_selection.py` 中构造 source-selection packet。packet 会把以下内容一次性交给角色：

- mission
- 当前 round 的任务
- role-specific governance
- 当前角色允许使用的 sources
- evidence requirements
- family memory
- 现有 selection 和 override request

### 5.2 模型可以决定什么

模型需要决定：

- 哪些 family 选，哪些不选
- 每个 family 下哪些 layer 选，哪些不选
- 每个 layer 对应哪些 source skill 被选中
- 对于 L2 layer，anchor_refs 用什么
- 是否提出 override request

### 5.3 模型不能决定什么

模型不能：

- 越过 allowed_sources
- 自己发明新的 source skill
- 跳过 family_plans / layer_plans 这些强制字段
- 对未批准且非 auto-selectable 的 layer 直接开闸
- 用 moderator override 私自突破治理边界

### 5.4 真正的控制点在哪里

控制点不在 prompt 文案本身，而在治理校验。

`application/orchestration/governance.py` 会检查：

- family_plans 是否与治理定义一一对应
- layer_plans 是否与该 family 下的 layer 一一对应
- 选中的技能是否都属于该 layer 允许的 skill 集
- 是否超出 max_selected_sources_per_role
- 是否超出 max_active_families_per_role
- 是否超出 max_non_entry_layers_per_role
- L2 layer 是否满足 anchor 要求
- authorization_basis 是否和治理状态一致

所以这一步的本质是：

“LLM 负责在被允许的空间内做组合决策，系统负责把可行动空间收窄到治理允许的集合。”

这也是项目里一个比较成熟的机制。

## 6. Fetch Plan 机制：到底是如何由任务和选源结果合成命令的

这一段是确定性的，不是 LLM 自己拼命令。

### 6.1 输入

- mission
- round tasks
- sociologist source selection
- environmentalist source selection

### 6.2 合成逻辑

`build_fetch_plan()` 会分别调用社会学侧和环境学侧的 step synthesis，把当前任务目标、时间窗口、地域范围和已选择 source skills 变成一组具体 steps。

这里真正重要的点不是“命令能拼出来”，而是它先做了治理收口：

- 先由 `role_selected_sources()` 根据 governance 验证 source selection
- 再把被允许的 selected sources 送入 step builder
- 最后把输入快照写进 plan

因此 fetch plan 的来源是：

任务需求 + mission 约束 + 已验证的选源结果 + 本地 step synthesis 规则

不是：

让模型临场决定下一条 shell command

## 7. Claim Candidate 机制：公众侧候选到底怎么形成

这部分是纯确定性的，目前没有 LLM。

### 7.1 哪些信号会被排除

如果 public signal 的 `signal_kind` 属于非主张型记录，例如 coverage 或 manifest 类元数据，就不会进入 claim candidate 流。

### 7.2 如何识别 claim type

系统把标题和正文拼成文本，再用 `claim_type_from_text()` 走关键词规则。

例如：

- 出现 smoke / haze / smog 会偏向 `smoke`
- 出现 flood / flooding 会偏向 `flood`
- 出现 heat / heatwave 会偏向 `heat`

这一步的本质是词表匹配，不是语义模型分类。

### 7.3 如何把多条 public signal 聚成一个候选 claim

系统先对文本做 `semantic_fingerprint()`：

- 全部小写
- 只保留字母数字 token
- 去掉 stopwords
- 截取前 12 个 token

然后按：

- `claim_type`
- `semantic_fingerprint`

组合分组。

也就是说，当前 claim candidate 的聚合逻辑，本质是“关键词判类 + 轻量文本指纹聚类”。这很稳定，但也很依赖文本表面相似性。

### 7.4 如何形成 claim scope

对每个分组，系统会调用 `build_public_claim_scope()`，尝试从信号中提取：

- 局部时间范围
- 局部地点范围

如果提取不到，就回退到 mission window 或 mission region。

但这里有一个非常关键的设计：

只有当时间和地点都不是 mission fallback 时，`usable_for_matching` 才为真。否则该 claim 不会被当作可直接用于物理匹配的强证据。

这意味着项目明确区分：

- “这个说法和任务主题有关”
- “这个说法已经有足够局部化信息，可拿去和观测做直接匹配”

这个区分是合理的，而且很重要。

### 7.5 如何把候选 claim 指派给 hypothesis

这里也不是 LLM。`best_public_claim_hypothesis_id()` 会做：

- 提取 claim statement 的 token set
- 提取每个 hypothesis 的 statement / summary token set
- 计算 token overlap
- 如果某个 hypothesis 的 `public_interpretation` leg 显式包含该 claim_type，再加分

最后选分数最高的 hypothesis。

所以 claim 到 hypothesis 的映射，本质是“词项重叠启发式 + leg claim_type 奖励”，不是深层推理。

## 8. Observation Candidate 机制：环境侧候选到底怎么形成

这一部分同样是确定性的。

### 8.1 如何分组

环境信号会先按 `observation_group_key()` 分组。核心维度是：

- source_skill
- canonical metric
- 位置键

如果缺少经纬度，会退回 mission_scope 的稳定哈希。

这意味着 observation candidate 更像“同源、同指标、同地点桶”的聚合摘要。

### 8.2 如何形成观测值

每组里：

- 如果只有一个值，就保留 point observation
- 如果有多个值，就做 window summary
- 如果是 `nasa-firms-fire-fetch` 且 metric 为 `fire_detection`，会转换为 `fire_detection_count`

对于多值组，还会计算：

- mean
- median
- p05 / p25 / p75 / p95
- stddev

并保留 distribution summary 与 compact audit。

### 8.3 时间和地点如何确定

- 时间优先用组内最早/最晚观测时间
- 地点由组内观测推导 place scope
- 提取失败时回退 mission scope / mission window

### 8.4 如何给 observation 打调查标签

`infer_observation_investigation_tags()` 会把 observation 去和 investigation plan 里的物理 causal legs 比较。

它不是 LLM 判断，而是：

- 先看该 observation 的 metric family
- 再看与 mission scope 是否重叠
- 再看 leg 的 scope mode
- 对每个 leg 打分
- 选得分最高的 leg

如果只有一个最佳 leg，就写入：

- `leg_id`
- `hypothesis_id`

所以 observation 到 causal leg 的链接，本质是“metric family + scope overlap + 规则打分”，而不是语义理解。

## 9. Matching 机制：主张与观测到底怎样匹配

这是你上次写得不够清楚的部分，这次必须明确。

### 9.1 先决条件

claim 必须有可用的 matching scope。

如果 `claim_scope.usable_for_matching` 为假，`claim_matching_scope()` 会直接返回 `None`，该 claim 不会进入正常的直接匹配流程，而是留下 gap 说明。

### 9.2 候选 observation 是怎么被筛出来的

对每个 claim，系统只保留同时满足以下条件的 observation：

- `metric_relevant(claim_type, observation.metric)` 为真
- `time_windows_overlap(claim.time_window, observation.time_window)` 为真
- `geometry_overlap(claim.place_scope.geometry, observation.place_scope.geometry)` 为真

也就是说，匹配不是“所有 observation 和 claim 全量比对”，而是先做三层硬过滤：

- 指标相关
- 时间重叠
- 空间重叠

### 9.3 support / contradiction 分数怎么来的

每个通过过滤的 observation 会调用 `assess_observation_against_claim()`。

核心依据是 `CLAIM_METRIC_RULES`，里面为不同 claim_type 预设了：

- support thresholds
- contradict thresholds

例如对 smoke / air-pollution，会重点看：

- pm2_5
- pm10
- us_aqi
- fire_detection_count

系统还会结合 `evidence_role` 或 `component_roles` 区分：

- primary
- contextual
- contradictory
- mixed

然后累计：

- `support_score`
- `contradict_score`
- `primary_support_hits`
- `contradict_hits`
- `contextual_hits`

### 9.4 最终 verdict 怎么判

规则非常明确：

- 有匹配，且 `support_score > 0`、`contradict_score == 0`、并且至少有 primary support hit，则判 `supports`
- 有匹配，且 `support_score == 0`、`contradict_score > 0`，则判 `contradicts`
- 同时有支持和矛盾，则判 `mixed`
- 没有匹配，或者只有背景性上下文而没有直接支持，则判 `insufficient`

confidence 也是规则化给出，不是模型主观写的。

### 9.5 特定 claim type 的附加 gap

例如 smoke / air-pollution 还有额外规则：

- 如果缺少 `openaq-data-fetch` 这类 station-grade corroboration，会追加 gap
- 如果命中了 `modeled-background` 质量标记，也会提醒要交叉验证

这说明系统已经开始在部分场景中把“证据质量意识”编码进匹配逻辑，但仍是手工规则，不是统一框架。

## 10. 证据卡、isolated、remand：匹配结果如何转成调查状态

### 10.1 Evidence Card

凡是 claim 至少匹配到一个 observation，就会生成 evidence card。

它包含：

- claim_id
- observation_ids
- verdict
- confidence
- summary
- public_refs
- gaps
- 可选的 hypothesis_id / leg_id / matching_scope

这是当前系统最重要的证据对象之一，因为后续报告、调查状态和决策都会围绕它组织。

### 10.2 Isolated

如果允许 isolated evidence，那么：

- 没有匹配到观测的 claim，会变成 isolated claim
- 没有匹配到 claim 的 observation，会变成 isolated observation

这类对象表达的是“现在这份证据还孤立着”，而不是它没有价值。

### 10.3 Remand

如果 verdict 是 `mixed` 或 `insufficient`，或者未授权 isolated evidence，则会形成 remand。

remand 的作用是把“还没有解决的地方”结构化地留到下一轮，而不是在文本报告里口头说一句“还需要更多数据”。

## 11. Investigation State / Actions / History：现在的“调查能力”到底有多强

这一部分决定了项目是不是真的在做“调查”，还是只是“匹配完就写报告”。

### 11.1 Investigation State

`investigation_state.json` 是一个确定性汇总层。它会把：

- claims
- observations
- evidence cards
- isolated
- remands
- matching / adjudication / review

重新投影到 hypothesis 和 causal leg 上，产出每条 leg 的：

- status
- support count
- contradiction count
- pending refs
- remaining gaps
- uncertainty level

这个设计是对的，因为它把“散乱证据对象”转成了“围绕假说结构组织的调查状态”。

但它依然是规则汇总，不是新推理。

### 11.2 Investigation Actions

`investigation_actions.json` 的生成也是确定性的。

它会根据：

- unresolved required legs
- contradiction-active paths
- alternative hypotheses
- gap types
- 当前 governance 下可用 source options

去构造下一步候选动作。

动作评分会综合：

- expected_evidence_gain
- contradiction_resolution_value
- coverage_gain
- audit_clarity
- token_cost_penalty

最后给出预算内排序。

这一步很有价值，因为它让下一轮任务不再完全拍脑袋。

但也必须实话实说：当前 action planning 还是“规则评分器”，不是“会自己做探索式研究设计的 planner”。

### 11.3 History Retrieval

历史检索也比表面看起来更扎实一些，但仍主要是启发式。

它会先从 mission 和 investigation plan 构造 history query，包括：

- query
- region_label
- profile_id
- claim_types
- metric_families
- gap_types
- source_skills
- priority_leg_ids
- alternative_hypotheses

然后去 case library 做检索，并对 case/excerpt 按以下因素打分：

- 结构化字段重叠
- profile 是否一致
- gap type / claim type / metric family overlap
- region 匹配层级
- lexical overlap
- excerpt kind bonus

最终选出少量历史案例和摘录。

这能提供“有历史可参考”的能力，但还没有达到真正高质量类比推理。问题在于：

- 检索 query 仍来自固定计划结构
- 相关性打分仍是手工拼装
- 历史案例并未深度参与 hypothesis 竞争与反证

## 12. 可追溯与可审计性：当前做得怎么样

这是当前版本最值得肯定的部分。

### 12.1 已经做得好的地方

- 路径有统一规范，`controller/paths.py` 基本定义了 canonical artifact 的位置。
- 关键阶段存在 append-only audit chain。
- 回执里保留 artifact path、snapshot path、sha256、size、phase kind、event kind、prev receipt sha256。
- 审计对象不是只记结果文件路径，而是会把快照 blob 留下来。
- fetch plan 输入快照可以防止“任务变了但还用旧计划”。
- supervisor 把本地 shell 阶段和 agent 阶段严格拆开。

从工程角度看，这一套已经不只是“日志”，而是接近一个可验证的调查账本。

### 12.2 还不够的地方

主流程的审计强，辅助流程的审计弱。

当前审计链明确覆盖的 phase 是：

- import
- fetch
- normalize
- match
- decision

但还存在几个薄弱点：

- history retrieval 的候选、得分和最终摘录没有被统一纳入同等级 receipt 模型
- investigation actions 的排序依据虽然落成 JSON，但缺少和主审计链同级的快照化证明
- 一些 LLM 审阅节点虽然有 packet、draft、target 文件，但“为什么接受某个改写”仍主要靠结果文件本身反推

所以现在的状态可以评价为：

“主证据链条已经比较可审计，辅助调查链条还没有完全同强度审计化。”

## 13. 结构设计是否合理：客观评价

### 13.1 合理之处

- 已经从单体脚本式原型进化成 supervisor-driven staged runtime。
- 根入口是 thin facade，这很好，说明公开接口和内部实现开始分离。
- `application / domain / adapters` 的目标分层清楚。
- 第二阶段 package map 已经明确指出热点模块的去向，不是盲目重构。
- 包拓扑和导入边界有测试锁定，说明结构不是只靠口头约定。

### 13.2 仍然不够合理之处

- `controller/` 仍是明显的过渡枢纽，职责偏重。
- 一些大模块虽然已经拆过一轮，但仍带有“二代热点文件”倾向。
- 领域语义、应用编排、审计适配之间还有部分历史包袱，未完全剥离干净。

所以我的结论不是“架构已经很好”，而是：

“架构方向对，当前版本可用，但仍处于向稳定架构收敛的过渡段。”

## 14. 当前调查和推理能力的真实上限

如果必须客观评价，不要把它说成强智能系统。更准确的说法是：

它已经有调查流程能力，但还没有强调查智能。

### 14.1 目前真正擅长的

- 把一次调查拆成可执行阶段
- 管理多角色协作
- 让关键中间产物稳定落盘
- 用规则做基础匹配与核验
- 在证据不足时保留 gap 和下一轮任务

### 14.2 目前真正不擅长的

- 自主发现新的调查方向
- 深度比较多个竞争性假说
- 系统性寻找反证而不是补证
- 面对陌生事件类型时快速自适应
- 用历史案例做高质量类比推理

### 14.3 造成这个上限的根本原因

- 候选生成依赖关键词与文本指纹
- 假说绑定依赖 token overlap
- 匹配依赖手工阈值和 metric rules
- 动作生成依赖 gap-to-source 映射和手工评分
- 历史检索依赖结构化重叠和词项分数

这些都不是坏事，它们带来了稳定性。但它们决定了当前系统更偏“工程上可靠”，而不是“认知上很强”。

## 15. 风险与隐患

### 15.1 规则脆弱性

事件类型一多，`CLAIM_KEYWORDS`、`CLAIM_METRIC_RULES`、gap type 到 source skill 的映射都可能快速膨胀，维护成本会上升。

### 15.2 启发式偏置

当前很多“看起来像推理”的步骤，本质上是启发式排序。这在 benchmark 上可以有效，但遇到边界案例时容易产生系统性偏差。

### 15.3 语义正确性缺口

合同和校验器解决的是结构正确，不是判断正确。尤其在 curation、authorization、adjudication、review 这些节点，模型仍可能在格式完全合法的情况下做出欠佳决策。

### 15.4 竞争性假说利用不足

alternative hypotheses 已经存在，但更多像“被保留的对象”，而不是“被认真拿来竞争当前主解释的对象”。

### 15.5 审计深浅不一致

如果未来项目声称“全过程都可审计”，目前还不够严谨。准确表述应是“主流程关键节点较可审计，辅助调查逻辑尚需补齐”。

## 16. 改进方向：尤其是如何强化调查能力

这一部分是最关键的。下面这些建议我按“最值得做”排序。

### 16.1 把“补证”升级为“验证 + 反证”双轨调查

现在的下一步动作更偏向补齐缺失证据。后续应把动作显式区分为：

- verify current hypothesis
- falsify current hypothesis
- discriminate between alternatives

只有这样，系统才会从“把已有解释补完整”进化成“主动检验解释是否站得住”。

### 16.2 提升 alternative hypotheses 的制度地位

建议让 alternative 不只是 investigation state 里的挂件，而是：

- 必须拥有独立 evidence budget
- 必须有最小测试动作配额
- 在 decision gating 中有更强权重

否则“有 alternative 字段”和“真的在做竞争性推理”不是一回事。

### 16.3 重做 investigation actions 的生成逻辑

当前动作排序已经比“人工拍脑袋”好，但还不够。

下一步可以做：

- 引入更显式的 utility of information 估计
- 把 action 的收益拆成对多个 hypothesis 的区分度，而不是只看补当前 gap
- 增加 negative evidence preference，对可能推翻当前解释的动作提高优先级
- 记录动作被采纳后的真实收益，用于后续迭代打分器

### 16.4 强化 history retrieval 的真正用途

现在历史检索更像“提供参考案例片段”。建议下一步让它更直接服务调查：

- 用历史案例指导 alternative generation
- 用历史案例指导缺失证据类型判断
- 用历史案例提示反证路径
- 用历史案例校正 action ranking

否则 history context 只是“背景材料”，不是调查引擎的一部分。

### 16.5 从规则识别升级到混合候选生成

候选生成没必要完全放弃规则，但可以升级为“规则 + 受约束模型”的混合机制。

例如：

- 规则负责保守召回
- 小模型或受限 LLM 负责 claim merge / split 建议
- 再由合同和审计机制把最终候选固定下来

这样既不丢可控性，也能减少仅靠文本指纹带来的误聚类和漏聚类。

### 16.6 把匹配从单点阈值扩展为证据图

当前 matching 更像 claim 对 observation 的局部打分。下一步可以考虑：

- 明确 source / mechanism / impact / public interpretation 四段证据链
- 让证据关系从一对多扩展为可审计的小型证据图
- 显式记录“支持的是哪一段 causal leg，而不是只支持整个 claim”

这会让 investigation state 更扎实，也更符合“调查”而不是“简单匹配”。

### 16.7 扩展审计链到调查辅助层

建议给以下对象增加与主流程同级的 receipt 或快照机制：

- history_retrieval.json
- history_context.json
- investigation_actions.json
- investigation_review 的关键引用快照

这样之后你可以更有底气说，系统不仅结果可审查，连“为什么建议下一步做这个动作”也可审查。

### 16.8 建立更强的调查 benchmark

现有 benchmark 已经是好起点，但下一步要更有针对性。

建议新增三类评测：

- 反证型 benchmark：主假说看似合理，但应被替代解释击败
- 低局部化 benchmark：公众说法相关但不能直接匹配，考验 system 是否保守
- 历史借鉴型 benchmark：历史案例能显著提升动作选择质量

这样你后续论文里才能真正回答“调查能力是否增强”，而不只是“流程是否更完整”。

## 17. 论文写作时建议如何表述

为了避免表述过满，我建议你在论文中把项目定位成：

“一个面向环境事件核验的、监督器驱动的多角色调查运行时，其创新点主要在于把角色化 LLM 决策、确定性证据加工、治理边界与审计留痕结合进同一工作流。”

而不要直接表述成：

“一个已经具备强自主调查推理能力的智能体系统。”

前者是准确且有说服力的，后者容易被评委追问后暴露短板。

## 18. 最终判断

结构设计是否合理？

合理，且方向正确，但仍未完全收敛，`controller/` 和部分热点模块仍是后续风险点。

可追溯可审计性做得怎么样？

主链条做得相当不错，已经形成项目特色；辅助调查链条还需要补齐。

调查和推理能力如何？

已经有“调查流程能力”，但主要靠规则、启发式和受约束的模型审阅来支撑，距离强调查智能仍有明显差距。

最大的风险是什么？

容易把“流程完整”误当成“调查能力很强”；实际上当前最强的是工程约束，不是开放式认知能力。

最重要的改进方向是什么？

把系统从“会补证的证据流水线”推进到“会做假说竞争、主动反证、历史借鉴和收益导向行动规划的调查系统”。
