# OpenClaw Runtime 议会程序加固工作计划

文档性质：本文是当前工程工作计划。旧版公共政策形势分析升级计划已删除；其中需要长期保留的原则已并入 `docs/openclaw-project-overview.md`、`docs/openclaw-source-family-workflows.md` 和 `docs/openclaw-claim-strength-obligations.md`。

本文的目的不是只植入目标拆分，而是把现有 runtime 议会结构升级为 report-driven、program-aware、theme-aware、supplement-capable 的议会程序。核心目标是让 OpenClaw 从“单轮或多轮调查后拼报告”升级为“前置议会把报告需求转化为后续议会议题问题，每个 council round 围绕一个或一组问题式议题组织 agent responsibility boundaries，并在不足时通过同轮获取/分析 turn 或针对性补充轮继续推进”。

## 1. 当前判断

现有结构已经有关键骨架：

1. `round-brief`
   - 可记录 round mode、focus refs、open questions、invited roles、requested outputs 和 boundary notes。
2. `open-investigation-round`
   - 可从 source round 打开 follow-up round，携带 `round_mode`、`primary_focus_refs`、`context_packet_id`、`round_brief_id`，并生成下一轮 task scaffold。
3. `prepare-round`
   - 会读取最新 round brief，把它作为 coordination context 暴露给 source planning / agent entry。
4. `agent-position`
   - 可让 agents 对 report blueprint、theme、round 等对象表达采纳、反对、修正或边界意见。
5. `review-theme-sufficiency`、`summarize-round-readiness`、`round_liveness`
   - 已能给出 advisory review、readiness summary 和 continuation hints。
6. `skills/` 当前有大量平铺 skill 目录。
   - 这让 agent、operator 和开发者很难快速判断某个 skill 属于规划、fetch、normalize、query、deliberation-write、optional-analysis、reporting 还是 state-transition。当前 runtime registry 和部分测试默认扫描 `skills/<skill-name>`，因此物理目录整理必须先升级 registry，而不能直接移动。

但这些还是松散组件。当前系统缺少一个一等的 `council-investigation-program` 来回答：

1. 本次报告到底要回答哪些 mission-driven questions。
2. 这些问题如何拆成 investigation themes。
3. 后续有哪些 council agenda questions，每个 round 围绕哪些问题式议题、责任边界和退出条件组织讨论。
4. 某个议题目标未满足时，是当前 round 内增加 acquisition turn / analysis turn，降级，scope out，还是打开哪个 supplemental council round。
5. 报告撰写轮如何接收各 agent 的 section brief，而不是只接收事实材料。

## 2. 不可违反的边界

1. `claim_slots` 是 mission-driven 的待回答问题槽，不是固定题型模板、领域模板或预设结论。
2. `investigation_theme` 是问题边界和 claim-basis 需求，不是 source queue。
3. `council-investigation-program` 和 program-aware `round-brief` 可以规划主题、round、agent responsibility boundaries、exit criteria 和 downgrade conditions，但不能指定 source family、source skill、query variant、query parameters 或 route ranking。
4. `council-investigation-program` 的 schema 和测试必须显式拒绝 source family、source skill、query、query parameters、route ranking、priority score、automatic execution 或 scheduler queue 字段；否则 program 会滑向隐性 scheduler。
5. `agent_obligations` 或后续命名必须表达“本轮责任边界”，而不是机械任务清单。它应说明某 role 对哪个 claim boundary、denominator、limitation 或 review responsibility 负责，不强制具体动作、source、query 或 skill。
6. `theme_evidence_boundary_plan` 是当前一等对象。它必须 agent-authored 或 agent-adopted，只记录 claim-basis、证据义务、分母义务、成功条件、恢复路径和降级边界。它不是 acquisition route plan；真正 source/query/skill route 只能出现在 investigator 的 acquisition turn、source-acquisition-proposal 或 route assessment 中。
7. source family workflow 是能力地图，不是议会程序。正式 acquisition turn 中，investigator 自主选择、拒绝或改写 source 路线。
8. checkpoint 只在结果影响 claim strength、source-limit、报告降级或恢复路径时记录，不能成为每次 tool call 的表单负担。
9. sufficiency/progress review 不是 runtime 判真机制，不给证据打分，不自动决定 report-ready；它只建议 disposition，最终仍要通过 council object、moderator synthesis、readiness opinion、report-basis gate 或 transition approval 承接。
10. `policy_evaluation_basis` 是报告综合层产物，由事实、官方行动、公众/媒体/正式语义、治理记录和 challenger limitations 共同支撑；它不是独立数据 lane。
11. runtime 只负责对象、权限、审批、ledger、transition 和可执行命令模板；不替 moderator 组织议程，不替 agents 判断证据。

## 3. 目标工作流

目标链路：

`mission -> round-001-framing-scope -> report_blueprint -> agent positions -> council-investigation-program -> program-aware issue round briefs -> issue council rounds with acquisition/analysis turns -> in-round progress feedback -> supplemental issue rounds when needed -> agent section briefs -> report writing round -> validator/backtest`

推荐 round 语义：

1. `round-001-framing-scope`
   - `round_mode`: `framing-scope-council`
   - `round_category`: `planning`
   - `round_subtitle_question`: `本次报告需要回答哪些问题，议会应如何分轮次调查？`
   - 目标：由 report-editor 提出报告问题框架，各 investigator 和 challenger 自由讨论并提交 positions，moderator 综合成 council investigation program。
2. `round-002-<issue-or-theme>`
   - `round_mode`: `issue-council`
   - `round_category`: `issue-deliberation`
   - `round_subtitle_question`: `围绕本议题，哪些事实、语义和政策边界需要被回答？`
   - 目标：承接 program 中的一组 issue questions / active themes。议会 round 本身负责问题讨论、责任边界、证据承接、充分性讨论和 synthesis；数据获取与数据分析作为本 round 内的 agent work turns，而不是独立把 round 变成抓取流水线。
   - `round_internal_phases`: `agenda-question`、`agent-acquisition-turns`、`agent-analysis-turns`、`progress-review`、`moderator-synthesis`
3. `round-003-<issue-or-theme>-supplement-01`
   - `round_mode`: `supplemental-issue-council`
   - `round_category`: `supplemental-issue-deliberation`
   - `round_subtitle_question`: `哪个未满足议题边界仍需要补充获取、补充分析或重新讨论？`
   - 目标：只处理未满足的 theme responsibility boundary、challenger concern、denominator/source-limit dispute 或 policy lane absence。若只是同一 agent 的 query repair，优先作为上一 round 内 acquisition turn，不单独开 council round。
   - `round_internal_phases`: `supplemental-acquisition-turns`、`supplemental-analysis-turns`、`progress-review`、`moderator-synthesis`
4. `round-N-report-writing`
   - `round_mode`: `report-writing`
   - `round_category`: `reporting`
   - `round_subtitle_question`: `哪些议会已采信材料可以进入报告，哪些只能作为限制或后续工作？`
   - 目标：基于 frozen/reporting basis、agent section briefs 和 sufficiency/progress review 写报告。

`round_id` 采用文件系统安全短 id；可读标题写入 `round_title`，副标题优先采用问题形式写入 `round_subtitle_question`。不要把冒号、长中文标题或不可控字符放进 `round_id`。数据获取和数据分析/综合不应被伪装成同一种“调查轮”；它们通常是 issue council round 内不同的 agent work turns，只有当议题边界、责任边界或议会采信需要重新组织时，才升级为 supplemental council round。

## 4. 新增和重构对象

### 4.1 `council-investigation-program`

用途：前置议会轮次的总纲。它不是报告模板、source plan 或 runtime scheduler。

建议字段：

1. `program_id`
2. `mission_question`
3. `report_blueprint_ref`
4. `agent_position_refs`
5. `program_questions`
6. `theme_threads`
7. `council_agenda_questions`
8. `agent_responsibility_boundaries`
9. `round_sequence`
10. `round_internal_phase_model`
11. `round_exit_criteria`
12. `downgrade_conditions`
13. `supplemental_round_triggers`
14. `source_autonomy_boundary`
15. `policy_evaluation_boundary`
16. `adoption_status`
17. `forbidden_scheduler_fields`

验收：

1. program 能列出后续 round，而不是只列 4 到 5 个宽泛主题。
2. 每个 theme thread 能追溯到 claim slots，但不预设结论。
3. 每个 round 有 `round_category`、问题式 `round_subtitle_question`、active themes、agent responsibility boundaries、internal phase model、exit criteria 和 continuation criteria。
4. program 不包含 source family、source skill、query variants、query parameters 或 route ranking。
5. 至少 environmental-investigator、social-investigator、challenger 的 positions 被读取或明确记录缺席。
6. schema 和 tests 拒绝 `source_family`、`source_families`、`source_skill`、`query`、`query_variants`、`query_parameters`、`route_ranking`、`source_priority`、`scheduler_queue`、`auto_execute` 等隐性 scheduler 字段。
7. `agent_responsibility_boundaries` 只能描述 role 的责任边界、证据边界、分母边界、limitation 或 review duty；不能描述必须执行的 source/query/skill/task sequence。
8. `round_internal_phases` 是描述性组织提示，不是 runtime 状态机；runtime 不按这些 phase 自动推进议程或拒绝 agent 行动。

### 4.2 Program-aware `round-brief`

用途：把 `council-investigation-program` 的某一轮计划投射到现有 round 结构，继续沿用既有 `submit-round-brief`、`prepare-round`、`open-investigation-round` 和 agent entry surfaces。

建议新增或规范字段：

1. `program_id`
2. `round_title`
3. `round_subtitle_question`
4. `round_mode`
5. `round_category`
6. `active_theme_ids`
7. `agent_responsibility_boundaries`
8. `round_internal_phases`
9. `expected_council_objects`
10. `round_exit_criteria`
11. `in_round_feedback_triggers`
12. `supplemental_round_policy`
13. `forbidden_source_precommitments`

验收：

1. `prepare-round` 能读取并暴露这些字段，但不把它们转成 source 排序或硬 source filter。
2. agent entry 能让每个 agent 看见本轮主题、责任边界和退出条件。
3. round brief 仍是 coordination context；agents 可以提交 position 修正、挑战或 scope change。
4. acquisition turn、analysis/synthesis turn、progress review、moderator synthesis 和 reporting round 可由 `round_internal_phases` / `round_category` 明确区分，避免所有工作都被泛称为 investigation。
5. `round_subtitle_question` 默认采用问题形式，帮助 agents 围绕待回答问题组织讨论，而不是围绕陈述句预设结论。
6. 数据获取不会自动成为 council round；只有当获取不足改变议题边界、责任边界、采信边界或需要 moderator synthesis / transition approval 时，才打开 supplemental council round。

### 4.3 Program-aware `theme_evidence_boundary_plan`

用途：让 investigator 针对 active theme 说明 claim-basis 边界、证据义务、分母义务、成功条件、恢复路径和降级边界。当前实现使用一等对象名 `theme_evidence_boundary_plan` / `theme-evidence-boundary-plan`；它是 evidence boundary / claim basis plan，不是 acquisition route plan。

保留当前边界：

1. 必须 `agent-authored` 或 `agent-adopted`。
2. 不得含 source family、source skill、query variants、query parameters、route ranking。
3. 不得把 `policy_evaluation_basis` 当作 acquisition lane。
4. 真正的 source/query/skill route 只能出现在 investigator 的 acquisition turn、source-acquisition-proposal 或 evidence route assessment 中。

验收：

1. moderator/report-editor 不能提交代替 investigator 的 theme evidence boundary plan。
2. plan 验证器能拒绝任何 route precommitment。
3. plan 与 program/round brief 的 theme 和 claim slot 对齐。
4. 文档、schema 和测试应逐步迁移到 `theme_evidence_boundary_plan` 语义；若暂时保留旧名，必须在 skill docs、contract 和 validator 中明确“不是 route plan”。

### 4.4 `theme-progress-review`

用途：比当前 `theme_sufficiency_review` 更贴近 round 执行。它不判真，只说明本轮 active themes 的 obligations 状态。

建议字段：

1. `program_id`
2. `round_brief_id`
3. `active_theme_id`
4. `agent_responsibility_status`
5. `available_basis_refs`
6. `denominator_status`
7. `coverage_or_policy_lane_limits`
8. `in_round_recovery_options`
9. `recommended_disposition`
10. `supplemental_round_recommendation`

推荐 disposition：

1. `satisfied-for-current-claim-strength`
2. `needs-in-round-recovery`
3. `needs-supplemental-round`
4. `downgrade-required`
5. `scope-out-with-rationale`
6. `blocked-by-program-mismatch`

验收：

1. review 能指出哪个 active theme 未满足，而不是泛泛说 round needs-more-data。
2. review 能区分“当前 round 内恢复”和“需要 supplemental round”。
3. review 不自动打开 round，不自动允许报告；它只生成议会可采信的 advisory object。
4. review 的 `recommended_disposition` 必须由后续 council object、moderator synthesis、readiness opinion、report-basis gate 或 transition approval 承接后，才会改变议会状态。

### 4.5 Agent section brief

用途：报告撰写前让各 agent 参与内容组织，而不是只向 report-editor 提供材料。

建议字段：

1. `program_id`
2. `theme_ids`
3. `section_role`
4. `main_claims`
5. `evidence_refs`
6. `basis_object_ids`
7. `claim_strength`
8. `denominators`
9. `limitations`
10. `recommended_report_use`
11. `blocked_phrases`

验收：

1. environmental-investigator 提交事实/环境过程 brief。
2. social-investigator 提交公共语义、媒体语义、正式记录或政策动作 brief。
3. challenger 提交 unsupported wording、denominator、causal overreach 和 policy-evaluation boundary brief。
4. report-editor 新增实质 claim 必须能回溯到 section brief、frozen basis 或 council object。

### 4.6 Skill taxonomy and directory layout

用途：降低 skill 数量带来的认知负担，让议会程序和 runtime surfaces 能按职责发现 skill，而不是面对一个 100+ 项平铺列表。

当前约束：

1. `skill_registry.available_skill_names()` 只扫描 `skills_root.iterdir()`。
2. `kernel/core/registry.py` 只从 `skills/<skill-name>` 构造 script/doc/config 路径。
3. 部分测试直接读取 `skills/<skill-name>/SKILL.md`。
4. 因此不能先移动目录再修 registry；必须先实现递归 discovery 或 category manifest。

建议目标目录：

1. `skills/planning-program/`
   - report blueprint、council program、round brief、theme evidence boundary plan、theme progress review。
2. `skills/runtime-state/`
   - scaffold、prepare round、open investigation/report-writing round、freeze/gate/transition-adjacent skills。
3. `skills/deliberation-write/`
   - finding/proposal/readiness/challenge/position/synthesis/evidence request/task 类写入。
4. `skills/source-fetch/`
   - provider fetch 和 artifact import。
5. `skills/source-normalize/`
   - raw artifact 到 signal plane 的 normalize。
6. `skills/query/`
   - public/formal/environment/raw/normalized/archive/case query。
7. `skills/optional-analysis/`
   - coverage audit、corpus、annotation、aggregation、timeline、action cards、evidence sufficiency、representation audit。
8. `skills/reporting/`
   - section brief、handoff、draft、publish、validator-adjacent reporting skills。
9. `skills/archive-history/`
   - archive、history context、case library。

整理策略：

1. 先给每个 skill 增加 registry-level metadata：`skill_category`、`skill_family`、`workflow_stage`、`physical_path`。
2. 让 registry 支持递归发现 `**/SKILL.md`，但继续以 `skill_name` 而不是路径作为公共标识。
3. 更新 tests 和 helper，禁止新代码硬编码 `skills/<skill-name>`。
4. 迁移物理目录后，保留一个短期兼容 map 或 generated manifest，直到所有调用点都走 registry。
5. 最后再评估合并重复 skill。不要因为数量多就合并有不同 side-effect、role policy、DB plane 或 evidence contract 的 skill。

可优先合并或收敛的区域：

1. report/program planning 相关 helper：避免 `materialize-report-blueprint`、program synthesis、theme planning 和 round brief 出现互相平行的提示词逻辑。
2. public discourse optional-analysis：corpus、coverage、annotation、aggregation、summary 可以共享输入 shape、denominator semantics 和 source-limit vocabulary。
3. dynamic deliberation submissions：继续复用 `dynamic_investigation_submission_support`，避免每个 thin submit skill 自己维护 parser。
4. reporting validation：增强现有 report draft/validator，不新增平行 composer/validator。

不宜合并的区域：

1. fetch 与 normalize 不合并，因为 side effects、provider payload 和 DB write surface 不同。
2. query 与 deliberation-write 不合并，因为 query 只读，finding/evidence bundle/proposal 是议会判断写入。
3. state transition 与 optional analysis 不合并，因为 transition 需要 moderator request 和 runtime-operator approval。

验收：

1. skill registry 能递归发现分类目录下的所有 skill。
2. `resolve_skill_policy(<skill_name>)`、`run-skill --skill-name <skill_name>` 和 agent entry surfaces 不受物理路径影响。
3. registry snapshot 能按 category/family/stage 汇总，减少 agent 面向 100+ 平铺 skill 的负担。
4. 迁移后 full tests 中所有直接路径假设都已改为 registry helper。
5. skill 数量可以仍多，但列表必须按职责可读；后续合并只基于职责重复和契约重叠，不按行数或目录数量合并。

## 5. 开发步骤

### Phase 1: 文档和契约收口

任务：

1. 删除旧公共政策形势分析工作计划。
2. 更新常驻文档中的工作流、round 命名、source autonomy、claim-strength 和 sufficiency 边界。
3. 新增 `council-investigation-program` canonical contract。
4. 让 program-aware `round-brief` 的 typed payload 可被 tests 构造和验证。
5. 为 program 和 round brief 增加 forbidden scheduler/source-route field validation。
6. 明确 `agent_responsibility_boundaries` 的语义是责任边界，不是 action queue。

验收：

1. docs 中不再引用旧计划路径。
2. `claim_slots`、theme evidence boundary plan、checkpoint、sufficiency 和 `policy_evaluation_basis` 的边界在常驻文档中一致。
3. 新对象契约不引入 source route precommitment。
4. schema/tests 能拒绝 source family、source skill、query、route ranking、priority score、scheduler queue、auto execute 等隐性 scheduler 字段。
5. schema/tests 能拒绝把 agent responsibility 写成必须执行的 source/query/skill/task sequence。

### Phase 2: Framing/Scope Council

任务：

1. 让 `round-001-framing-scope` 成为正式 council planning round。
2. `materialize-report-blueprint` 只生成 report questions、claim slots 和 investigation theme candidates。
3. 各 agent 通过 `submit-agent-position` 对 blueprint/themes 表态：采纳、修正、质疑、补充主题、边界提醒。
4. 新增或重构 `synthesize-council-investigation-program`，读取 blueprint 和 agent positions，输出 program。

验收：

1. NYC smoke 能生成程序化调查计划，包含事实过程、官方/政策行动、公共语义、互动时间线和报告综合目标。
2. Colorado River 能生成不同的程序化调查计划，包含水文背景、治理记录、公共/媒体/正式语义、多主体叙事和政策评估依据边界。
3. 计划中的 round sequence 明确，不是 4 个宽泛 theme 的静态列表。
4. 计划不触发 fetch，不选 source，不写结论。

### Phase 3: Program-aware Round Execution

任务：

1. 扩展 `submit-round-brief` / dynamic payload support，使 round brief 能携带 program fields。
2. 扩展 `open-investigation-round`，让 supplemental council round 继承 `program_id`、active theme、unresolved responsibility boundary 和 parent review refs。
3. 扩展 agent entry surfaces，让 agents 看见当前 round 的 program context、active themes、internal phases 和 exit criteria。
4. 保持 `prepare-round` 对 round brief 的读取为 coordination context，不变成 source filter。
5. 确保 `round_internal_phases` 只是描述性组织提示，不成为 runtime hard gate 或自动状态机。

验收：

1. `round-001-framing-scope` 后能创建 issue council round，而不是创建抓取流水线 round。
2. issue council round 内能表达 acquisition turn、analysis turn、progress review 和 moderator synthesis 的不同组织方式。
3. 若 `theme-progress-review` 建议补充，`open-investigation-round` 能创建 `round-003-<issue-or-theme>-supplement-01` 并携带 focus refs、未满足责任边界和建议的 internal phases。
4. 旧的泛泛 continuation 仍可工作，但新的 program-aware path 是默认推荐路径。
5. round brief 和 transition artifact 都能保留 `round_category`、问题式 `round_subtitle_question` 和 `round_internal_phases`。

### Phase 4: Agent-led Acquisition Turns 和及时反馈

任务：

1. 保留 investigator 对 source family、query、skill route 的自主选择权。
2. 将 `theme_evidence_boundary_plan` 与 active theme/program 对齐，但不包含 route；若代码仍使用 `theme_evidence_boundary_plan`，必须保留同一语义边界。
3. 将 source acquisition proposal、coverage audit、checkpoint 和 route assessment 串成 issue council round 内的 acquisition turn feedback。
4. challenger 对 checkpoint-level risks 做快速审查：denominator mixing、GDELT tone misuse、policy lane absence、unsupported absence claim、causal overreach。

验收：

1. public/policy 低量时，系统能在当前 issue council round 内提出 query/source recovery 或 source-limit rationale，而不是直接等下一轮。
2. checkpoint 只在影响 claim strength 时产生，不随普通 tool call 膨胀。
3. source family workflows 只作为 agent 能力地图出现，不出现在 program 或 theme evidence boundary plan 的 route precommitment 字段里。
4. acquisition turn 不自动改变议会状态；只有被 finding、evidence bundle、checkpoint、route assessment、review 或 moderator synthesis 承接后，才影响本议题的进展判断。
5. 低量、zero result、query variant 尝试和 same-family follow-up 不应自动触发 supplemental round。

### Phase 5: Analysis Turns / Progress Review 和补充轮

任务：

1. 新增 `review-round-theme-progress` 或重构 `review-theme-sufficiency` 为 program-aware。
2. 对每个 active theme 输出 responsibility status、basis refs、denominator status、analysis status、recovery options 和 recommended disposition。
3. 当 disposition 是 `needs-supplemental-round` 时，生成 `open-investigation-round` 可消费的 focus refs / transition payload 建议。
4. 明确该 review 只能建议 disposition，不能替 moderator synthesis、readiness opinion、report-basis gate 或 transition approval 做决定。

验收：

1. review 能说清“哪个 theme 的哪个 obligation 不足”。
2. review 能区分“当前 round 内增加 acquisition turn”、“当前 round 内增加 analysis turn”与“开启 supplemental council round”。
3. supplemental round 的 focus refs 指向 theme/review/evidence request，而不是只写 `round-002`。
4. 任何 recommended disposition 改变议会状态前，必须被 council object、moderator synthesis、readiness opinion、report-basis gate 或 transition approval 承接。
5. supplemental round 触发必须克制：普通 query repair、zero-result 诊断、query variant 扩展、same-family follow-up 优先留在当前 issue council round 内。

### Phase 6: Reporting Integration

任务：

1. 让 `draft-agent-section-brief` 读取 program/theme/progress review。
2. reporting handoff 汇总 agent section briefs、theme progress、frozen basis 和 unresolved limitations。
3. narrative report 渲染事实核查、公共/政策语义、互动时间线和政策评估依据边界。
4. validator 检查实质 claim 是否有 section brief、frozen basis、council object 或 accepted sufficiency/progress review 支撑。

验收：

1. 其它 agents 实质参与报告内容组织。
2. policy / official action lane 缺失时，报告只能写缺口、source-limit 或后续评估维度，不能写政策有效性结论。
3. interaction claim 必须能回溯到 fact/policy/public 至少两类 refs 和 timeline node summary。
4. 强 claim、政策评估、公众比例、因果/归因表述必须能看到 challenger 的边界审查痕迹；否则 validator 或 handoff 应标记为 unsupported / downgrade-required。

### Phase 7: Skill Registry 和目录治理

任务：

1. 第一阶段只增加 skill category/family/stage metadata 和分类 manifest，避免阻塞 program-aware council flow 的最小闭环。
2. 修改 runtime skill registry 和 core registry，使其支持递归发现分类目录中的 `SKILL.md`，但物理迁移放在 program-aware flow 跑通之后。
3. 更新测试与 helper，禁止直接假设 `skills/<skill-name>` 是唯一物理路径。
4. 先生成分类 manifest / registry snapshot，再执行物理目录迁移。
5. 按职责整理 skill 目录；只在契约高度重叠时合并 skill。

验收：

1. 123 个现有 skill 均能被 registry 发现，skill name 和 policy resolution 不变。
2. 所有 run-skill、approval、agent entry、source queue、quality gate 均通过 registry path 运行。
3. 目录结构能让 agent 快速区分 planning、runtime-state、fetch、normalize、query、optional-analysis、deliberation-write、reporting、archive-history。
4. 没有因为移动目录破坏现有脚本路径、agent config 路径或 skill policy。
5. 物理目录迁移不是 Phase 2-6 的 blocker；program-aware council 最小闭环可先依赖 metadata / manifest。

### Phase 8: 回归和真实 run 验收

任务：

1. 单元测试覆盖 program contract、round brief payload、source precommitment rejection、implicit scheduler field rejection、responsibility-boundary validation、supplemental transition context、theme progress review disposition。
2. 做 NYC smoke 旧 run report-chain backtest，验证报告缺口被明确指出。
3. 新建 fresh run，先跑到 `round-001-framing-scope` 结束并停在正式调查前，人工验收 program 拆分。
4. 验收通过后再挂机跑完整 run。

验收：

1. 新 framing 输出不再只是“事实/政策/公众/互动”粗分，而是具体规划后续 issue questions、issue council rounds、internal acquisition/analysis turns、supplemental policy 和 agent responsibility boundaries。
2. 不出现 moderator/report-editor 预填 source family 或 query route 的问题。
3. full run 报告具备事实核查、舆情/政策语义主线、互动时间线和政策评估依据边界。

## 6. 非目标

1. 不写固定 source 队列。
2. 不引入全局样本量硬阈值。
3. 不把 action cards 变成 scheduler。
4. 不让 runtime 排序 source、证据或 claim。
5. 不新增平行 report composer / validator，优先增强现有报告链。
6. 不把 GDELT media/document tone 写成 public sentiment。
7. 不把 sample-internal semantic structure 写成总体民意。
8. 不把 claim slots 做成固定题型模板。
9. 不让 moderator 替 investigator 决定 acquisition route。
10. 不把 checkpoint 做成每次工具调用都要填写的表单。
11. 不把 sufficiency/progress review 做成 runtime 判真或自动采信机制。
12. 不把 `policy_evaluation_basis` 做成独立数据 lane。
13. 不把 `council-investigation-program` 做成 scheduler、source planner、query planner 或 action queue。
14. 不把数据获取和数据分析/综合混为同一种泛化 investigation round；也不把普通数据获取 turn 自动升级为 council round。

## 7. 最终完成标准

工程完成标准：

1. `council-investigation-program`、program-aware `round-brief`、theme progress review 和 supplemental round context 均可写入、查询、回归。
2. `open-investigation-round` 与现有 transition request / approval / ledger 结构深度衔接。
3. agent entry、prepare-round、reporting handoff 和 validator 都能读取 program context。
4. skill registry 支持分类目录和递归 discovery；agent/operator 不再面对无分类平铺 skill 列表。

议会流程完成标准：

1. 前置 framing/scope council 让 report-editor、environmental-investigator、social-investigator、challenger 和 moderator 都有可见贡献。
2. 后续 round 有 round category、问题式 subtitle、active themes、agent responsibility boundaries、internal phases、exit criteria 和 supplement policy。
3. acquisition turn、analysis/synthesis turn、progress review、moderator synthesis 和 reporting round 的组织边界清楚。
4. 补充轮针对具体未满足目标，不再泛泛重复一轮；普通 query repair 优先留在当前 issue council round 内处理。

报告完成标准：

1. 报告有事实核查、舆情与政策语义、互动时间线和政策评估依据边界。
2. 报告能说明哪些 claim 被支撑、哪些被降级、哪些缺 basis。
3. 所有强 claim 都能回溯到 section brief、frozen basis、council object 或 accepted progress/sufficiency review。
