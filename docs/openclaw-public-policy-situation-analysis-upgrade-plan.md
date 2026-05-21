# OpenClaw 报告主导调查与及时反馈工作计划

文档性质：本文是当前工程收口计划。旧版公共政策形势分析升级计划中的长期架构原则已经并入 `docs/openclaw-project-overview.md` 和 `docs/openclaw-source-family-workflows.md`；本文只保留下一阶段需要实现、回归和验收的任务。

本文不是 runtime 固定议程，不是 source 排序规则，不是硬数据量规则。它的目标是把 OpenClaw 从“先调查、后拼报告”的链路改成“报告 claim slots 和 evidence contract 前置，agents 围绕分主题调查，并在 round 内获得及时反馈”的链路。

## 1. 当前问题

NYC smoke backtest 已经证明 `lane_episode_cards -> interaction nodes -> section brief -> narrative report -> validator` 的基本链路可跑通，但报告本身仍不令人满意：

1. `report-editor` 仍是主要写作者，其它 agents 只通过既有 artifacts 间接参与报告。
2. 调查轮次没有由最终报告 claim slots 反推 evidence contract。
3. 舆情和政策数据不足时，系统多是 advisory 提醒，不会形成及时补采和充分性讨论闭环。
4. interaction timeline 有 typed structure，但节点缺少可读的逐日分析摘要。
5. policy / official action lane 容易为空，导致报告不能支撑政策沟通、政策回应或政策有效性依据。
6. validator 能阻断一部分无 basis claim，但还不能稳定检查报告叙事质量、policy lane absence、重复标签和分母异常。

## 2. 目标链路

目标链路：

`mission -> report-framing round -> claim slots -> investigation themes -> theme acquisition plans -> in-round acquisition checkpoints -> sufficiency review -> agent section briefs -> report synthesis -> validator/backtest`

核心原则：

1. 报告内容主导调查工作，但不预设调查结论。
2. claim slots 是 mission-driven 的待回答问题槽，不是固定题型模板、领域模板或预设结论。
3. report-editor 参与前置 framing，负责提出报告问题、claim slots 和写作所需 evidence shape。
4. moderator 把 claim slots 拆成可分配、可讨论的 investigation themes。
5. investigators 对各自 theme 自主完成取证路线选择；路线选择发生在正式 acquisition turn，而不是前置拆分对象里。
6. theme acquisition plan 必须由 investigator 自己撰写，或由 investigator 显式采纳、修改后使用；它只能记录证据义务、成功条件、分母义务、失败恢复和降级边界。
7. moderator/report-editor 可以提出主题和 evidence need，但不能替 investigator 决定或预填 source、query、skill 路线；前置计划也不能以任何形式指定 source family、source skill、query variant 或 route ranking。
8. checkpoint 只在抓取结果会影响 claim strength、source-limit 或报告降级时记录，不能变成每次 tool call 的表单负担。
9. sufficiency review 不是 runtime 判真机制，只回答“哪些 claim 能支撑、哪些必须降级、哪些缺 basis”；最终采信仍由议会通过 finding、evidence bundle、synthesis、readiness 或 report basis 完成。
10. `policy_evaluation_basis` 是报告综合层产物，由事实、官方行动、公众语义、治理记录共同支撑，不是独立数据 lane。
11. challenger 在 round 内及时审查覆盖、外推、分母、policy lane absence 和 unsupported wording。
12. 数据量是否足够由 agents 协作形成 sufficiency review，不靠全局硬阈值。
13. 弱报告仍允许生成，但必须明确 claim strength、缺失 basis、source-limit rationale 和降级表述。

## 2.1 不可误读的设计边界

1. `claim_slots` 不是固定题型模板。它们必须从 mission、用户关注、已有议会对象和 report framing 现场生成，表达“本报告要回答什么”，而不是表达“系统一定要证明什么”。
2. `investigation_theme` 不是 source queue。theme 只定义问题边界和 claim-basis 需求，不规定必须使用哪个 source family、query 或 skill。
3. `theme_acquisition_plan` 必须 agent-authored 或 agent-adopted。moderator/report-editor 可以提出主题和证据缺口，但 plan 本身不得包含 source family、source skill、query variant、query parameters 或 route ranking；investigator 的路线选择权保留到正式 acquisition turn。
4. `acquisition_checkpoint` 是及时反馈，不是合规表单。只有当结果会改变 claim strength、报告降级、source-limit rationale 或下一步恢复选择时才需要记录。
5. `theme_sufficiency_review` 不是 runtime 判真、证据打分或自动放行。它只把可支撑 claim、必须降级 claim 和缺失 basis 显式化，供议会采信或挑战。
6. `policy_evaluation_basis` 不是“政策评估数据”抓取 lane。它只能由 fact / official action、public-policy corpus、semantic perception 和 interaction timeline 的已承接材料综合而来。

## 3. 新增或重构对象

### 3.1 `report_blueprint`

用途：定义报告最终要回答的问题，不写结论，不生成固定题型模板。

字段建议：

1. `report_questions`
2. `claim_slots`
3. `required_evidence_families`
4. `forbidden_claims_without_basis`
5. `expected_sections`
6. `policy_evaluation_boundaries`

验收：

1. NYC smoke blueprint 能基于 mission 明确事实核查、官方/政策动作、舆情语义、互动时间线和政策评估依据五类待回答问题。
2. Colorado River blueprint 能基于 mission 明确水文压力、治理节点、正式记录、公共语义、多主体叙事和政策评估依据。
3. blueprint 不能写成“所有环境事件都套同一组问题”的领域模板。

### 3.2 `investigation_theme`

用途：把 report claim slots 拆成天然适配议会结构的分主题。

典型 theme：

1. `fact_event_process`
2. `official_policy_action`
3. `public_semantic_perception`
4. `media_policy_framing`
5. `interaction_timeline`

`policy_evaluation_basis` 不列为普通 acquisition theme。它可以作为报告综合问题或 synthesis target 出现，但其材料必须来自事实、官方行动、公共/媒体语义、治理记录和互动时间线等上游 theme。

验收：

1. 每个 theme 有 owner role、claim boundary、expected artifacts 和 completion criteria。
2. theme 不固定 source，不预设结论，不携带 query 或 skill 路线。

### 3.3 `theme_acquisition_plan`

用途：让 investigator 在抓取前说明当前 theme 需要回答什么、什么证据形态才允许支撑 claim、需要怎样的分母/覆盖边界，以及失败或低量时如何恢复或降级。该计划必须由 investigator 撰写，或由 investigator 明确采纳并可修改；moderator/report-editor 不能替 investigator 决定 source、query 或 skill 路线，plan 本身也不能预填这些路线。

字段建议：

1. `theme_id`
2. `claim_slots_supported`
3. `evidence_obligations`
4. `success_criteria`
5. `denominator_obligations`
6. `time_window`
7. `sample_unit`
8. `failure_recovery_plan`
9. `forbidden_precommitments`
10. `downgrade_boundary`

验收：

1. public / policy theme 不允许没有证据义务、分母义务和降级边界就直接进入报告。
2. plan 是 agent-authored 或 agent-adopted，不是 runtime 自动排序，也不是 moderator 指派的 source/script。
3. plan 不允许出现 source-family candidates、query variants、source skills、query parameters 或 route ranking。source-family workflow 只能在正式 acquisition turn 中由 investigator 自主使用、拒绝或改写。

### 3.4 `acquisition_checkpoint`

用途：在当前 round 内做及时反馈，避免“抓一次数据、开一次新 round”的低效循环。checkpoint 只在抓取、查询或归一化结果会影响 claim strength、source-limit、降级表述或恢复路径时记录；不为每次 tool call 生成表单。

字段建议：

1. `theme_id`
2. `source_family_counts`
3. `query_variant_hits`
4. `zero_low_volume_or_failed_attempts`
5. `visible_denominators`
6. `coverage_risks`
7. `challenger_quick_review`
8. `next_recovery_choice`
9. `stop_or_continue_reason`

验收：

1. checkpoint 能在同一 round 内触发 query 修正、same-family follow-up、source switch 或 source-limit rationale。
2. checkpoint 不要求长表单，不为每个 tool call 生成官僚记录，只在结果将影响 claim strength 时触发。
3. checkpoint 不替代 finding、evidence bundle、sufficiency review 或 readiness opinion。

### 3.5 `theme_sufficiency_review`

用途：由 source owner、challenger、moderator 和必要时 report-editor 协作判断“当前数据能支撑哪些 claim，不能支撑哪些 claim”。它不是 runtime 判真机制，不自动决定证据采信、报告通过或调查结束。

字段建议：

1. `supported_claim_slots`
2. `unsupported_claim_slots`
3. `valid_denominators`
4. `source_family_limits`
5. `representativeness_limits`
6. `required_downgrades`
7. `recommended_section_brief_inputs`

验收：

1. 舆情数据量不再只靠硬规则；必须通过 sufficiency review 说明样本内结构是否足够。
2. policy lane 为空时，review 必须显式阻止政策有效性或政策回应 claim。
3. review 的结论必须由议会对象或 report basis 承接后，才能成为报告依据。

### 3.6 `agent_section_brief`

用途：让其它 agents 在报告撰写前提交可审计 brief，而不是只把事实材料交给 report-editor。

字段建议：

1. `section_role`
2. `main_claims`
3. `evidence_refs`
4. `source_families`
5. `claim_strength`
6. `denominators`
7. `limitations`
8. `recommended_report_use`
9. `blocked_phrases`

验收：

1. environmental-investigator 至少提交事实过程 brief。
2. social-investigator 至少提交公共语义/媒体语义 brief。
3. policy/formal material 由 social-investigator 或 moderator 形成 official action / policy record brief。
4. report-editor 新增实质 claim 必须能回溯到 brief、frozen basis 或 council object。

## 4. 开发步骤

### Phase 1: 文档与对象契约收口

任务：

1. 以本文替换旧升级计划。
2. 在常驻文档中保留 report-driven investigation、in-round feedback、四条主 lane 和 section brief 规则。
3. 为 `report_blueprint`、`investigation_theme`、`theme_acquisition_plan`、`acquisition_checkpoint`、`theme_sufficiency_review`、`agent_section_brief` 定义 canonical schema 或 typed artifact shape。

验收：

1. docs 不再重复旧 phase 列表。
2. 新对象 shape 可被 tests 构造 fixture。
3. 旧 action cards / interaction timeline / reporting handoff 的语义与新对象不冲突。

### Phase 2: Report-Framing Round

任务：

1. 增加 framing 产物生成路径，可由 moderator/report-editor 基于 mission 生成 `report_blueprint`。
2. 将 blueprint 拆成 `investigation_themes`。
3. 将 themes 暴露给 agent entry surface 和 round brief。
4. 明确 claim slots 的 mission-driven 属性，防止生成固定题型模板。

验收：

1. NYC smoke 自动或半自动生成五类 claim slots：事实核查、官方行动、舆情语义、互动时间线、政策评估依据。
2. Colorado River 自动或半自动生成水文、治理记录、公共/媒体/正式语义、互动、政策评估依据 themes。
3. framing 不触发 fetch，不选择 source，不写结论。
4. 相同框架在不同 mission 下生成的问题槽应明显不同，不能只是复用领域模板。

### Phase 3: Theme Acquisition 与 In-Round Feedback

任务：

1. 为 public / policy / environment themes 生成或接收不含 source/query/skill 路线的 `theme_acquisition_plan`。
2. 在 corpus materialization、coverage audit、claim-gap action cards 周围增加 `acquisition_checkpoint` 输出。
3. 让 checkpoint 能承接 failed / zero / low-volume / receipt-only attempts，并提出恢复路径。
4. challenger 能对 checkpoint 做快速审查，不必等报告 validator 才发现问题。
5. 保证 acquisition plan 只表达证据义务和降级边界；source / query / skill 路线只能由 investigator 在正式 acquisition turn 中自主形成。

验收：

1. 舆情低量时，系统能在当前 round 内暴露 query variant、source-family 和 denominator 问题。
2. GDELT tone、YouTube comments、Bluesky posts、formal comments 的 denominator 不混合。
3. policy lane 为空时，checkpoint 明确提示 official action / governance record 补采或 report downgrade。
4. 不通过全局硬阈值决定是否足够。
5. checkpoint 数量与 claim-impact 绑定，不因普通成功 tool call 膨胀。

### Phase 4: Sufficiency Review 与 Agent Section Brief

任务：

1. 新增或重构 `draft-agent-section-brief` 路径。
2. 将 `theme_sufficiency_review` 接入 reporting handoff。
3. 让 environmental、social、policy/formal 相关主题都能产出 brief。
4. section brief 必须携带 refs、claim strength、denominator、limitations、blocked phrases。
5. 将 `policy_evaluation_basis` 保持为 report synthesis 层，不新增独立“政策评估数据”采集 lane。

验收：

1. report handoff 不再只由脚本从 artifacts 合成单一 section brief。
2. 其它 agents 可以实质参与报告内容组织。
3. 没有 policy brief 时，报告不能写政策有效性或政策回应结论，只能写缺口和后续评估维度。
4. sufficiency review 不直接让 runtime 判定 report-ready；report-ready 仍由议会和 gate/freeze 链路承接。

### Phase 5: Interaction Timeline 和报告可读性

任务：

1. interaction timeline 节点必须从 lane episode cards 生成可读 `node_summary`。
2. interaction timeline 区分事实侧、政策侧、公共/媒体侧。
3. narrative report 渲染逐日或阶段性互动线，而不是只写 artifact counts。
4. 公共语义 section 清理重复标签、分母异常和“100% 中性报道”这类不可读输出。

验收：

1. NYC smoke 报告能讲清楚 6 月 6 日、6 月 7 日、6 月 8 日事实变化、公共语义变化和官方/政策动作是否缺失。
2. interaction section 不再只写“49 个 lane episode cards、6 个节点”。
3. validator 能识别缺少 node summary、policy lane absence 和 denominator 异常。

### Phase 6: Policy / Official Action Lane 补强

任务：

1. 为突发环境事件补齐 official action acquisition routes，例如 agency alerts、health guidance、school/outdoor activity changes、public service advisories。
2. 为治理争议案例补齐 Federal Register、agency pages、public involvement、EIS/SEIS、USBR/DOI project records。
3. normalize-official-governance-records 输出要能形成 policy lane episode cards。

验收：

1. NYC smoke full run 至少形成 official action / risk communication episodes，若确实未抓到则输出 source-limit rationale。
2. Colorado River run 至少形成 governance record / public involvement / policy process episodes。
3. policy lane 不再默默为空。

### Phase 7: Validator 与回归

任务：

1. 扩展 validator，检查 report claim 是否有对应 section brief、sufficiency review 或 frozen basis。
2. 检查 public semantic percentages 的 denominator 和 source family。
3. 检查 interaction claims 是否有 lane episode cards、node summaries 和至少两类 evidence refs。
4. 检查 policy evaluation wording 是否有 policy/official action basis。
5. 建立旧 run report-chain backtest 和完整 run 验收标准。

验收：

1. NYC old-run backtest 能明确指出 policy lane 缺失导致的降级，而不是简单 valid。
2. 新 full run 报告应通过 validator，并在人工阅读上具备事实核查、语义变化、互动时间线和政策评估依据主线。
3. 测试覆盖新增对象、checkpoint、brief、validator failure cases 和报告渲染。

## 5. 非目标

1. 不写固定 source 队列。
2. 不引入全局样本量硬阈值。
3. 不把 action cards 变成 scheduler。
4. 不让 runtime 排序 source、证据或 claim。
5. 不新增平行 report composer / validator，优先增强现有 `draft-narrative-report` 和 `validate-narrative-report`。
6. 不把 GDELT media tone 写成 public sentiment。
7. 不把 sample-internal semantic structure 写成总体民意。
8. 不把 claim slots 做成固定题型模板。
9. 不让 moderator 替 investigator 决定 acquisition plan。
10. 不把 checkpoint 做成每次工具调用都要填写的表单。
11. 不把 sufficiency review 做成 runtime 判真或自动采信机制。
12. 不把 `policy_evaluation_basis` 做成独立数据 lane。

## 6. 回归案例

### 6.1 NYC Smoke

必须验证：

1. 事实核查仍保留 AirNow / Open-Meteo / FIRMS / wind 的边界。
2. public discourse corpus 有 source family、query variants、coverage audit、annotation aggregation 和 denominator。
3. official action / policy lane 有 episode 或 source-limit rationale。
4. interaction timeline 能形成阶段性叙事。
5. report 不再只给 artifact counts，而能解释事实、语义和政策沟通之间的关系。

### 6.2 Colorado River / Glen Canyon

必须验证：

1. 水文/运行数据形成环境压力背景，不越界写政策责任。
2. Reclamation / DOI / Federal Register / public involvement 形成治理记录 lane。
3. 公共/媒体/正式记录语义分开聚合。
4. interaction timeline 能连接水文压力、治理动作、公共/政策语义。
5. policy evaluation basis 只写依据、缺口和后续评估维度，不写无证据的政策成败评分。

## 7. 完成标准

工程完成标准：

1. 新对象链路可被生成、读取、写入 handoff，并有 tests。
2. in-round checkpoint 能减少明显低效的跨 round 补采。
3. action cards 仍保持 advisory，但能被 theme plan、checkpoint 和 sufficiency review 承接。
4. agent section brief 成为 report-editor 的主要输入之一。

数据完成标准：

1. 舆情数据质量由 source-family coverage、query variants、denominator 和 sufficiency review 共同说明。
2. policy / official action 数据不会默默缺席。
3. failed / zero / low-volume / receipt-only attempts 都能成为 recovery choice 或 source-limit rationale。

报告完成标准：

1. 报告有明确主线、时间线和互动分析。
2. 报告能说明事实核查、舆情语义感知和政策分析如何互相约束。
3. 报告能为政策评估提供依据，但不越权给政策成败评分。
4. 所有强 claim 都能回溯到 brief、sufficiency review、frozen basis 或 council object。

论文完成标准：

1. 方法章节可以描述 DB-first 议会、report-driven investigation、in-round feedback、public-policy corpus、semantic perception、interaction timeline 和 validator。
2. 实验章节可以展示 NYC smoke 和 Colorado River 两类案例。
3. 讨论章节可以诚实说明 API 覆盖、非代表性样本和政策评估边界，同时证明系统具备可审计、可扩展、可复核的舆情语义感知能力。
