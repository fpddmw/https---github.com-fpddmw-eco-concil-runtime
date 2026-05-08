# OpenClaw 动态多轮调查治理开发计划

## 1. 文档定位

本文是下一阶段核心开发计划，目标是补齐 `moderator-led dynamic investigation planning`。

本计划解决的不是某个单一案例的 follow-up bug，而是 OpenClaw 当前更深层的治理缺口：

1. mission 不一定自带明确时空域、对象范围或指标范围。
2. 复合型问题不能在初始化时被强行压成一个固定 verification scope。
3. moderator 应主动拆分调查、组织多 round、发布子议题和子目标，而不是只在 challenger 质询后被动补证。
4. challenger 对一个子结论的反对意见，应阻断该子议题进入 synthesis，并触发轻量补充轮。
5. 多 round 场景必须有 context 压缩机制，不能每轮重复加载全量议程和全量历史。

## 2. 目标架构原则

1. `mission can be broad`
   - mission 可以是开放型问题，例如“对比美国多条河流污染情况”或“调查中国主要沿海城市生态情况”。
   - mission 不要求预置单一时空域。
2. `scope is a council object`
   - 时空域、对象集合、指标范围、比较口径应由 moderator 在 scoping round 中产出，并可被后续 round 修订。
3. `one round, one subissue`
   - 每个调查 round 应围绕一个明确子议题展开，产出一个子结论或明确的 evidence gap。
4. `challenger blocks subissue, not the whole project by default`
   - challenger 可以阻断某个 subissue 进入 synthesis。
   - 只有核心 subissue 被阻断时，整体 reporting 才被阻断。
5. `operator does not steer investigation`
   - runtime-operator 只批准 moderator 发出的 transition/approval 请求、修运行故障、维护流程完整性。
   - 调查方向、sub-target、source scope 由 council objects 承载。
6. `context is packetized`
   - 每轮 agent 不应读取全量历史。
   - agent 读取当前 round 的 context packet、evidence refs、delta summary 和必要 lineage。

## 3. 目标工作流

### 3.1 Intake / Scoping Round

输入：

1. 原始 mission。
2. 可选的 policy profile。
3. 可选的用户边界，例如“不做健康风险评估”。

moderator 输出：

1. `investigation-plan`
2. `subissue` 列表
3. `candidate-scope` 列表
4. `evidence-need` 列表
5. 初始 round sequence

该阶段不要求产生实质结论，也不要求抓取完整证据。

验收重点：

1. 没有明确时空域的 mission 也能进入 scoping。
2. 不把开放型 mission 强行编译成单一 region/window。
3. 每个 subissue 都有可审计的 rationale 和预期输出。

### 3.2 Subissue Round

每轮 round 只处理一个主子议题。

输入：

1. `round-brief`
2. 当前 `subissue`
3. 当前 active scope 或 candidate scopes
4. evidence needs
5. 上轮 delta packet

investigator 输出：

1. raw artifact receipts
2. normalized signals
3. finding / evidence bundle / uncertainty note
4. unresolved evidence gaps

moderator 输出：

1. round state note
2. 是否进入 sub-conclusion
3. 是否需要补充 round

### 3.3 Sub-conclusion

每个子议题完成后必须有 `sub-conclusion`。

最小字段：

1. `subissue_id`
2. `claim_summary`
3. `support_level`
4. `evidence_refs`
5. `coverage_limits`
6. `open_challenges`
7. `synthesis_status`

允许状态：

1. `ready-for-synthesis`
2. `needs-more-evidence`
3. `blocked-by-challenge`
4. `withheld`

### 3.4 Challenger Review

challenger 的对象必须绑定具体目标：

1. `target_kind=subissue|sub-conclusion|finding|evidence-bundle|scope`
2. `target_id`
3. `objection_kind`
4. `required_followup_evidence`
5. `blocking_scope`

challenge disposition 必须显式记录：

1. `resolved-by-followup`
2. `requires-followup`
3. `accepted-as-limitation`
4. `excluded-from-synthesis`
5. `waived-by-challenger`

没有 disposition 的 blocking challenge 不能进入 synthesis。

### 3.5 Supplemental Round

补充轮不是完整重跑。

输入仅包括：

1. 原 mission 摘要。
2. 目标 subissue。
3. 被阻断的 challenge。
4. required follow-up evidence。
5. 上一轮相关 evidence refs。

输出仅解决该 challenge 或明确无法解决。

默认不加载：

1. 全量 board history。
2. 全量 raw records。
3. 与 target subissue 无关的其他子议题。
4. report drafting surface。

### 3.6 Synthesis / Reporting Round

只有在必要 subissues 完成 disposition 后，moderator 才能请求 synthesis/report-basis transition。

report-editor 只消费：

1. frozen sub-conclusions
2. accepted limitations
3. resolved challenges
4. selected evidence bundles
5. explicit report basis

## 4. 新增或强化的治理对象

### 4.1 `investigation-plan`

用途：记录 moderator 对开放 mission 的调查拆分。

关键字段：

1. `plan_id`
2. `mission_id`
3. `planning_round_id`
4. `planning_status`
5. `subissue_ids`
6. `scope_ids`
7. `priority_order`
8. `rationale`
9. `revision_history`

### 4.2 `subissue`

用途：一个可独立调查、推理、挑战和归纳的子议题。

关键字段：

1. `subissue_id`
2. `title`
3. `question`
4. `parent_plan_id`
5. `priority`
6. `required_roles`
7. `evidence_need_ids`
8. `status`
9. `synthesis_criticality`

### 4.3 `investigation-scope`

用途：表达候选或激活的对象范围、时空范围、指标范围。

关键字段：

1. `scope_id`
2. `scope_kind`
3. `status=candidate|active|retired|rejected`
4. `spatial_scope`
5. `temporal_scope`
6. `object_scope`
7. `metric_scope`
8. `comparison_frame`
9. `rationale`

### 4.4 `round-brief`

用途：每轮 round 的机器可读任务说明。

关键字段：

1. `round_id`
2. `round_mode=scoping|subissue|supplemental|synthesis`
3. `target_subissue_id`
4. `target_challenge_id`
5. `active_scope_ids`
6. `evidence_need_ids`
7. `allowed_roles`
8. `expected_outputs`
9. `context_packet_id`

### 4.5 `evidence-need`

用途：表达当前子议题需要什么类型证据，而不是直接写死抓取参数。

关键字段：

1. `evidence_need_id`
2. `subissue_id`
3. `need_kind`
4. `minimum_coverage`
5. `candidate_source_families`
6. `quality_requirements`
7. `blocking_if_missing`

### 4.6 `sub-conclusion`

用途：每个子议题的阶段性结论。

关键字段：

1. `sub_conclusion_id`
2. `subissue_id`
3. `round_id`
4. `claim_summary`
5. `support_level`
6. `evidence_refs`
7. `limitations`
8. `challenge_ids`
9. `synthesis_status`

### 4.7 `context-packet`

用途：为 agent turn 提供压缩上下文。

关键字段：

1. `context_packet_id`
2. `packet_profile`
3. `target_round_id`
4. `included_object_refs`
5. `excluded_object_refs`
6. `summary_text`
7. `token_budget_estimate`
8. `raw_data_policy`

## 5. Runtime 与 Skill 修改计划

### P0：契约与 schema 设计

修改范围：

1. `eco-concil-runtime/src/eco_council_runtime/contracts/deliberation.py`
2. `eco-concil-runtime/src/eco_council_runtime/contracts/reporting.py`
3. `eco-concil-runtime/src/eco_council_runtime/kernel/planes/...`
4. 新增 migration 测试。

验收：

1. 新对象都有 canonical contract。
2. DB schema 可从旧 run 迁移。
3. query surface 能查询 plan/subissue/scope/round-brief/sub-conclusion/context-packet。

### P1：Moderator planning write surface

新增或扩展 skills：

1. `draft-investigation-plan`
2. `submit-investigation-plan`
3. `update-investigation-scope`
4. `submit-round-brief`
5. `submit-sub-conclusion`

验收：

1. moderator 可以在没有明确时空域的 mission 上发布 plan。
2. plan 中至少能表达多个 subissues。
3. operator 不参与 plan 内容。

### P2：Round lifecycle 改造

修改：

1. `open-investigation-round`
2. `prepare-round`
3. `materialize-agent-entry-gate`
4. `materialize-openclaw-agent-registration`

行为：

1. 若存在 `round-brief`，prepare-round 以 round-brief 为主。
2. 若无 round-brief，才回退到 mission scaffold。
3. `open-investigation-round` 支持 `round_mode`、`target_subissue_id`、`target_challenge_id`、`context_packet_id`。

验收：

1. subissue round 不读取其他 subissue 的任务面。
2. supplemental round 只给相关角色开放必要 read/write/fetch surface。
3. round transition request 由 moderator 发起，operator 只审批。

### P3：Challenger blocking 与 disposition

修改：

1. challenge ticket schema。
2. readiness gate。
3. report basis gate。
4. challenger constraints。

验收：

1. blocking challenge 会阻断 target subissue 的 synthesis status。
2. 未 disposition 的 blocking challenge 不允许 report basis freeze。
3. resolved challenge 可以恢复 target subissue 的 synthesis eligibility。

### P4：Context packet 与 token 压缩

新增 skill：

1. `materialize-context-packet`

packet profiles：

1. `scoping-full`
2. `subissue-standard`
3. `supplemental-minimal`
4. `synthesis-basis`

验收：

1. supplemental packet 不包含 raw records。
2. supplemental packet 只包含 target challenge/subissue/evidence refs/delta。
3. packet 有字符数或 token estimate。
4. agent workspace 中默认暴露 context packet 指针，而不是全量历史。

### P5：Synthesis 与 reporting gate

修改：

1. `materialize-reporting-handoff`
2. `draft-council-decision`
3. `publish-council-decision`
4. report basis freeze gate。

验收：

1. reporting handoff 只消费 ready/frozen sub-conclusions。
2. accepted limitations 与 unresolved challenges 分开展示。
3. report 不从 helper artifact 直接摘取结论。

## 6. 验收指标

### 6.1 开放型 mission 验收

测试输入：

1. “对比美国多条河流的污染情况。”
2. “调查中国主要沿海城市的生态情况。”

必须满足：

1. 初始化不要求单一 region/window。
2. moderator 生成 investigation-plan。
3. plan 中存在多个 subissues 或 candidate scopes。
4. 第一轮不会直接执行全量 fetch。
5. operator trace 中没有调查方向内容。

### 6.2 子议题 round 验收

必须满足：

1. 每个 evidence round 有且只有一个 primary subissue。
2. 每个 subissue round 有 round-brief。
3. fetch/normalize/query surface 来自 round-brief 和 role surface。
4. 每轮产出 sub-conclusion 或 needs-more-evidence 状态。

### 6.3 Challenger 验收

必须满足：

1. challenger challenge 绑定 target object。
2. blocking challenge 阻断对应 subissue。
3. moderator 能基于 challenge 发布 supplemental-round request。
4. supplemental round 完成后必须写 disposition。

### 6.4 Token 压缩验收

必须满足：

1. supplemental context packet 不超过 full scoping packet 的 25% 字符数，或低于项目设置的硬阈值。
2. packet 不包含 raw record 全文。
3. packet 中 evidence 以 refs 和摘要呈现。
4. agent prompt 不注入 unrelated subissue history。

### 6.5 Reporting 验收

必须满足：

1. 未完成 critical subissue 时不能 final publication。
2. unresolved blocking challenge 时不能 freeze report basis。
3. final report 能列出 sub-conclusions、limitations、resolved/unresolved challenges。

## 7. 回归测试计划

新增测试建议：

1. `tests/test_dynamic_investigation_planning.py`
   - broad mission -> plan/subissue/scope。
2. `tests/test_subissue_round_lifecycle.py`
   - round-brief -> prepare-round -> role surface。
3. `tests/test_challenger_supplemental_round.py`
   - challenge -> supplemental round request -> disposition。
4. `tests/test_context_packet_workflow.py`
   - packet profile、压缩、raw data exclusion。
5. `tests/test_dynamic_reporting_gate.py`
   - subissue readiness 与 report basis gate。

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
5. 为特定案例硬编码河流、城市、火点或指标列表。

## 9. 风险与边界

主要风险：

1. moderator 过度规划，产生过多 subissues。
2. context packet 过度压缩，丢失关键反证。
3. challenge disposition 被当成形式动作。
4. report gate 仍可能从非 frozen 对象取结论。

对应约束：

1. plan 必须有 priority 和 synthesis criticality。
2. packet 必须保留 evidence refs 和 excluded refs。
3. challenge disposition 必须带 rationale 和 evidence refs。
4. report basis 只接受 frozen/ready sub-conclusions 与 selected evidence bundles。

