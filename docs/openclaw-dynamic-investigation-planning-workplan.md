# OpenClaw 动态多轮调查治理开发计划

## 1. 文档定位

本文是下一阶段核心开发计划，目标是补齐 `moderator-led dynamic investigation planning`，但该能力必须保持为薄 runtime 治理与 agent 自主调查的组合。

本计划不让 runtime 变成事实判断者、调查议程制定者或证据采信模型。runtime 只负责让议会可运行、可审计、可恢复、可授权；moderator 是议会组织者；investigators、challenger、report-editor 保留自主调查、证据组合和结论表达权。

本计划要解决的治理缺口：

1. mission 可以是开放型问题，不一定自带明确时空域、对象范围或指标范围。
2. 复合型问题不应在初始化时被强行压成固定 verification scope。
3. moderator 应能提出调查拆分和 round brief，但这些对象是议会工作材料，不是 runtime 对 agent 的推理脚本。
4. challenger 应能阻断具体对象进入 synthesis，并触发有目标的补充调查。
5. 多 round 场景需要 context packet，避免每轮重复加载全量历史。

## 2. 不可违反的治理边界

1. `runtime is thin`
   - runtime 只维护权限、ledger、artifact refs、状态转移、上下文分发和可恢复执行。
   - runtime 不判断证据强弱，不给结论打分，不替 agent 决定调查方向。
2. `contracts are envelopes, not agendas`
   - contract 只规定对象 envelope：身份、作者、时间、目标引用、证据引用、provenance、状态。
   - 不引入厚字段来规定调查步骤、证据质量等级、采信标准、议程顺序或结论模板。
3. `agents own investigation judgment`
   - agent 可以接受、修改、拒绝或扩展 moderator 的 round brief，只要写明 rationale 和 evidence refs。
   - investigator 自主决定如何组合证据、哪些证据可采信、哪些证据只能作为限制条件。
   - report-editor 只消费 council 提交的显式对象，不从 helper artifact 自动推断结论。
4. `skills provide evidence, not judgment`
   - fetch/normalize/query/analysis skills 只提供 raw artifact、normalized signals、检索结果、可复现派生结果和 source/provenance notes。
   - skill 不输出“推荐结论”“证据权重”“可信度排序”“最佳 source 组合”。
   - 如需列表顺序，只允许使用确定性展示顺序，例如输入顺序、时间顺序、artifact 生成顺序；不得包装成调查建议。
5. `no hidden heuristics`
   - 不新增非必要启发式规则、权值计算、自动排序或自动筛选。
   - 必要的 runtime gate 只能检查程序性条件：权限、provenance、对象是否存在、blocking challenge 是否有 disposition、operator 是否已授权。
6. `moderator organizes, does not over-script`
   - moderator 可以提出 plan、scope hints、round brief 和 open questions。
   - 这些对象是协调材料，不得把 agent 限死在唯一子议题、唯一 source family 或唯一输出格式上。

## 3. 目标工作流

### 3.1 Intake / Scoping Round

输入：

1. 原始 mission。
2. 可选 policy profile。
3. 可选用户边界，例如“不做健康风险评估”。

moderator 可输出：

1. `investigation-plan`
2. `subissue` 草案
3. `scope-hint` 或 `candidate-scope`
4. `evidence-request`
5. 下一轮 round brief 草案

边界：

1. 该阶段不要求产生实质结论。
2. 该阶段不要求抓取完整证据。
3. plan 不得包含 runtime 强制执行的优先级、权重或 source 排序。
4. investigator 和 challenger 可以提出新增、合并、拆分或拒绝 subissue/scope 的意见。

验收重点：

1. 没有明确时空域的 mission 也能进入 scoping。
2. 不把开放型 mission 强行编译成单一 region/window。
3. plan 只记录 moderator 的组织意图和 rationale，不形成 runtime 议程锁。

### 3.2 Investigation Round

每轮 round 可以有一个 primary focus，但不强制 “one round, one subissue”。agent 可以记录与 primary focus 相关的旁支发现，并建议后续 round。

输入：

1. `round-brief`
2. primary focus refs，例如 subissue/scope/challenge/evidence-request
3. context packet
4. 可用 skill surface
5. 上轮 delta refs

investigator 输出：

1. raw artifact receipts
2. normalized signals
3. finding / evidence bundle / uncertainty note
4. unresolved evidence gaps
5. 自主提出的 follow-up question 或 scope revision

moderator 输出：

1. round state note
2. 是否需要补充 round
3. 是否邀请其他角色介入

边界：

1. round-brief 是协调输入，不是强制脚本。
2. `expected_outputs` 如存在，只能是提示，不得作为 runtime 拒绝 agent 输出的依据。
3. prepare-round 不得根据启发式权重替 agent 自动挑选“最佳”来源。

### 3.3 Agent Position / Subissue Position

子议题完成后可以形成 `subissue-position`，但它不是每轮硬性产物。若证据不足，agent 可以只提交 finding、gap note 或 challenge。

最小字段：

1. `position_id`
2. `target_ref`
3. `claim_summary`
4. `evidence_refs`
5. `limitations`
6. `open_challenge_refs`
7. `author_role`
8. `rationale`

允许状态：

1. `proposed-for-synthesis`
2. `needs-more-evidence`
3. `blocked-by-challenge`
4. `withheld-by-author`

边界：

1. 不设置 `support_level`、score、rank 或 weight。
2. 采信理由由 author_role 以自然语言和 evidence refs 表达。
3. synthesis 是否采纳由后续 council/reporting 对象显式记录。

### 3.4 Challenger Review

challenger 的对象必须绑定具体目标：

1. `target_kind=subissue|position|finding|evidence-bundle|scope|report-section`
2. `target_id`
3. `objection_kind`
4. `challenge_text`
5. `evidence_refs`
6. `blocking=true|false`

challenge disposition 最小记录：

1. `disposition_status=resolved|requires-followup|accepted-as-limitation|excluded-from-synthesis|withdrawn`
2. `rationale`
3. `evidence_refs`
4. `decided_by_role`

边界：

1. blocking challenge 没有 disposition 时，不能进入 synthesis/report basis。
2. disposition 不得被当作形式动作；它必须说明 agent/council 如何处理该 objection。
3. runtime 只检查 disposition 是否存在和是否引用目标，不判断 disposition 是否“正确”。

### 3.5 Supplemental Round

补充轮不是完整重跑。

输入通常包括：

1. 原 mission 摘要。
2. target refs。
3. 被阻断的 challenge。
4. related evidence refs。
5. 上轮 delta refs。

输出目标：

1. 回应 challenge。
2. 提交新增证据或说明无法补足。
3. 提出 scope/subissue 修订建议。

边界：

1. 默认不加载全量 board history。
2. 默认不加载全量 raw records。
3. 默认不暴露 report drafting surface。
4. agent 如认为必须扩展范围，可以提交理由和新的 evidence-request，而不是被 runtime 拦截。

### 3.6 Synthesis / Reporting Round

moderator 请求 synthesis/report-basis transition 时，只提交显式 council objects：

1. agent positions 或 subissue positions
2. findings
3. evidence bundles
4. accepted limitations
5. resolved/unresolved challenges
6. selected evidence refs

report-editor 只消费这些显式对象。helper artifact、context packet、source selection 或 query result 本身不得被自动当作结论。

## 4. 薄治理对象

### 4.1 `investigation-plan`

用途：记录 moderator 的调查组织意图。

最小字段：

1. `plan_id`
2. `mission_ref`
3. `author_role`
4. `planning_round_id`
5. `plan_status=draft|active|superseded|withdrawn`
6. `proposed_subissue_refs`
7. `scope_hint_refs`
8. `open_questions`
9. `rationale`
10. `supersedes_plan_id`

禁止字段：

1. numeric priority
2. weight
3. score
4. automatic source ranking
5. mandatory agenda order

### 4.2 `subissue`

用途：表达一个可被调查、挑战和归纳的问题对象。

最小字段：

1. `subissue_id`
2. `title`
3. `question`
4. `parent_plan_id`
5. `status=proposed|active|closed|withheld`
6. `rationale`
7. `created_by_role`

边界：

1. 不设置 numeric priority。
2. 不设置 required_roles。
3. 不设置 synthesis criticality 分数。
4. 是否进入 synthesis 由显式 position/report-basis 对象决定。

### 4.3 `investigation-scope`

用途：表达候选或激活的对象范围、时空范围、指标范围。

最小字段：

1. `scope_id`
2. `scope_kind`
3. `status=candidate|active|retired|rejected`
4. `spatial_scope`
5. `temporal_scope`
6. `object_scope`
7. `metric_scope`
8. `comparison_frame`
9. `rationale`

边界：

1. scope 可以为空或局部未知。
2. scope 是 agent/council 可修订对象，不是 mission 编译后的硬约束。

### 4.4 `round-brief`

用途：给本轮 agent turn 提供协调材料。

最小字段：

1. `round_id`
2. `round_mode=scoping|investigation|supplemental|synthesis`
3. `primary_focus_refs`
4. `context_packet_id`
5. `open_questions`
6. `source_boundary_notes`
7. `invited_roles`
8. `requested_outputs`

边界：

1. `invited_roles` 不是权限表；真实权限仍由 role contracts 和 access policy 控制。
2. `requested_outputs` 是提示，不是 runtime hard gate。
3. agent 可以提交 brief revision request。

### 4.5 `evidence-request`

用途：表达 agent/council 想寻找什么证据，而不是写死抓取参数或 source 排序。

最小字段：

1. `evidence_request_id`
2. `target_ref`
3. `question`
4. `desired_evidence_type`
5. `source_hints`
6. `boundary_notes`
7. `rationale`
8. `created_by_role`

禁止字段：

1. `minimum_coverage`
2. `quality_score`
3. `blocking_if_missing`
4. `candidate_source_weight`
5. `recommended_source_rank`

### 4.6 `agent-position`

用途：agent 对某个 target 的阶段性立场或结论候选。

最小字段：

1. `position_id`
2. `target_ref`
3. `author_role`
4. `claim_summary`
5. `evidence_refs`
6. `limitations`
7. `open_challenge_refs`
8. `rationale`
9. `status=proposed|withheld|needs-more-evidence`

### 4.7 `context-packet`

用途：为 agent turn 提供压缩上下文。

最小字段：

1. `context_packet_id`
2. `packet_profile`
3. `target_round_id`
4. `included_object_refs`
5. `excluded_object_refs`
6. `summary_text`
7. `raw_data_policy`
8. `source_refs`

边界：

1. packet 不做 salience ranking。
2. packet 不根据权重自动删除反证。
3. packet 只说明 included/excluded 的 provenance 和范围理由。
4. token/字符预算来自 operator 配置或 human request，不写死 25% 之类启发式阈值。

## 5. Runtime 与 Skill 修改计划

### P0：薄对象 envelope 与 query surface

修改范围：

1. `eco-concil-runtime/src/eco_council_runtime/contracts/deliberation.py`
2. `eco-concil-runtime/src/eco_council_runtime/contracts/reporting.py`
3. `eco-concil-runtime/src/eco_council_runtime/kernel/planes/...`
4. migration 测试

验收：

1. 新对象只引入薄 envelope 和必要索引。
2. DB schema 不引入 score/weight/rank/priority_order。
3. query surface 能按 kind、author_role、target_ref、round_id 查询对象。
4. 旧 run 可迁移或作为历史 fixture 被读取，不伪装成新角色模型。

### P1：Moderator coordination write surface

新增或扩展 skills：

1. `submit-investigation-plan`
2. `submit-investigation-scope`
3. `submit-round-brief`
4. `submit-evidence-request`
5. `submit-agent-position`

验收：

1. moderator 可以在没有明确时空域的 mission 上发布 plan。
2. plan 可表达多个 proposed subissues 或 scope hints。
3. operator 不参与 plan 内容。
4. investigator/challenger 可以提交 plan 或 scope revision request。

### P2：Skill boundary audit

修改重点：

1. fetch skills 只输出 raw artifact、receipt、source metadata。
2. normalize skills 只输出 normalized signals 和转换 provenance。
3. query skills 只输出匹配结果和查询条件。
4. analysis skills 只输出可复现派生对象，不输出采信排序。

验收：

1. skill payload 不包含 evidence score、weight、rank、recommended conclusion。
2. 如存在列表顺序，文档必须说明只是 deterministic display order。
3. evidence bundle 的组合由 agent 提交，不由 skill 自动合成。

### P3：Round lifecycle 改造

修改：

1. `open-investigation-round`
2. `prepare-round`
3. `materialize-agent-entry-gate`
4. `materialize-openclaw-agent-registration`

行为：

1. 若存在 `round-brief`，prepare-round 将其作为 context hint。
2. 若无 round-brief，继续允许 agent 从 mission/context 自主提出首轮调查动作。
3. `open-investigation-round` 支持 `round_mode`、`primary_focus_refs`、`target_challenge_id`、`context_packet_id`。

验收：

1. round brief 不会隐藏 agent 可用的合法 write surface。
2. supplemental round 默认给最小上下文，但允许 agent 请求扩展。
3. round transition request 由 moderator 发起，operator 只审批程序性动作。

### P4：Challenger blocking 与 disposition

修改：

1. challenge ticket envelope。
2. challenge disposition envelope。
3. readiness gate。
4. report basis gate。

验收：

1. blocking challenge 会阻断对应 target 进入 synthesis/report basis。
2. 未 disposition 的 blocking challenge 不允许 report basis freeze。
3. resolved/accepted-as-limitation challenge 可以恢复 target 的 synthesis eligibility。
4. runtime 不判断 challenge 内容真伪，只检查对象状态和引用完整性。

### P5：Context packet 与压缩

新增 skill：

1. `materialize-context-packet`

packet profiles：

1. `scoping`
2. `investigation`
3. `supplemental`
4. `synthesis`

验收：

1. packet 默认不包含 raw records。
2. packet 包含 target refs、evidence refs、delta refs 和 excluded refs。
3. packet 不进行权重筛选或 salience 排序。
4. agent workspace 中默认暴露 context packet 指针，而不是全量历史。

### P6：Synthesis 与 reporting gate

修改：

1. `materialize-reporting-handoff`
2. `draft-council-decision`
3. `publish-council-decision`
4. report basis freeze gate

验收：

1. reporting handoff 只消费显式 council objects。
2. accepted limitations 与 unresolved challenges 分开展示。
3. report 不从 helper artifact 直接摘取结论。
4. final publication 不采纳没有 author_role/provenance/evidence_refs 的结论。

## 6. 验收指标

### 6.1 开放型 mission 验收

测试输入：

1. “对比美国多条河流的污染情况。”
2. “调查中国主要沿海城市的生态情况。”

必须满足：

1. 初始化不要求单一 region/window。
2. moderator 可生成 investigation-plan，但 plan 不锁定唯一议程。
3. investigator/challenger 可提出 scope 或 subissue revision。
4. 第一轮不会直接执行全量 fetch。
5. operator trace 中没有调查方向内容。

### 6.2 Agent 自主调查验收

必须满足：

1. agent 可以提交未在 round brief 中列出的相关 finding 或 evidence-request。
2. agent 可以说明某个 source hint 不适用。
3. agent 可以把证据作为 limitation 而非 supporting evidence。
4. runtime 不因 agent 输出不符合 `requested_outputs` 而拒绝对象。

### 6.3 Skill 边界验收

必须满足：

1. skill 不输出 score/weight/rank/recommended conclusion。
2. skill 不自动把 query results 合成为 evidence bundle。
3. skill 输出必须保留 provenance、query/source parameters、artifact refs。
4. skill 的缺证说明只能描述缺口，不能裁定结论不成立。

### 6.4 Challenger 验收

必须满足：

1. challenger challenge 绑定 target object。
2. blocking challenge 阻断对应 target 的 synthesis/report basis。
3. moderator 能基于 challenge 发布 supplemental-round request。
4. supplemental round 完成后由 agent/council 写 disposition。

### 6.5 Context Packet 验收

必须满足：

1. packet 不包含 raw record 全文，除非 human/operator 明确授权。
2. packet 中 evidence 以 refs 和摘要呈现。
3. packet 记录 included/excluded refs。
4. packet 不使用固定百分比阈值或自动 salience ranking。
5. agent prompt 不注入 unrelated history，但 agent 可请求扩展上下文。

### 6.6 Reporting 验收

必须满足：

1. unresolved blocking challenge 时不能 freeze report basis。
2. final report 能列出 positions、limitations、resolved/unresolved challenges。
3. final report 的每个结论都有 author_role 和 evidence_refs。
4. final publication 不从 runtime helper artifact 自动生成结论。

## 7. 回归测试计划

新增测试建议：

1. `tests/test_dynamic_investigation_planning.py`
   - broad mission -> plan/subissue/scope hints。
2. `tests/test_agent_autonomy_round_lifecycle.py`
   - round-brief -> prepare-round -> agent 提交 brief 外相关 finding。
3. `tests/test_skill_evidence_only_boundary.py`
   - skill payload 不包含 score/weight/rank/recommended conclusion。
4. `tests/test_challenger_supplemental_round.py`
   - challenge -> supplemental round request -> disposition。
5. `tests/test_context_packet_workflow.py`
   - packet profile、raw data exclusion、no salience ranking。
6. `tests/test_dynamic_reporting_gate.py`
   - blocking challenge 与 explicit council objects 控制 report basis gate。

质量门：

1. `python3 tools/quality_gate.py syntax`
2. targeted suites：runtime-governance、reporting、case-study、dynamic-planning。
3. `python3 tools/quality_gate.py full`

## 8. 非目标

本计划不实现：

1. 专业污染源解析模型。
2. 自动确定“真实世界最佳调查范围”。
3. 自动替代专家判断。
4. operator 主导调查方向。
5. runtime 主导证据采信。
6. skill 自动生成结论或证据权重。
7. 为特定案例硬编码河流、城市、火点或指标列表。

## 9. 风险与边界

主要风险：

1. moderator 过度规划，导致 agent 被隐性议程锁定。
2. context packet 过度压缩，丢失关键反证。
3. challenge disposition 被当成形式动作。
4. report gate 从非显式 council object 取结论。
5. skill 逐步滑向推荐系统或证据打分系统。

对应约束：

1. plan/round brief 只能作为 council coordination object。
2. packet 必须保留 included/excluded refs 和范围理由。
3. challenge disposition 必须带 rationale 和 target refs。
4. report basis 只接受 agent/council 显式提交的 positions、findings、evidence bundles、limitations、challenge dispositions。
5. 任何 score/weight/rank/heuristic selection 字段都必须作为架构异味处理，除非 human 明确要求且另行设计治理边界。
