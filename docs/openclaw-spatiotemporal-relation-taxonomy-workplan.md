# OpenClaw 时空关系与环境信号 Taxonomy 基础设施工作计划

## 1. 文档定位

本文保留 `spatiotemporal-relation` 基础设施的设计、实施记录和验收边界。目标不是新增大气传输模型或污染物扩散模拟，而是修正通用 skill 层对“时空匹配 / 关系线索”的表达不足。

核心判断：

`时空匹配是 OpenClaw 基础分析层应该胜任的通用任务，不应为 NASA FIRMS 或 PM2.5 单独创建窄 skill。`

烟气传输议题只作为压力测试场景，用来检验该基础设施是否能表达：

1. 候选源事件。
2. 受体观测。
3. 背景上下文观测。
4. 时空窗口关系。
5. 反证和替代解释。
6. 报告中可审计的 evidence packet。

## 1.1 当前验收状态

截至 2026-05-05，本文 P0-P5 的基础设施验收通过，作为当前 baseline 维护。验收覆盖：

1. `spatiotemporal-relation-cue` canonical contract。
2. `signal_role` 与 `environment_signal_class` metadata/index。
3. structured relation cue 与 legacy same-day cue 兼容。
4. `query-spatiotemporal-relations` 查询面。
5. relation-oriented challenge/probe/review comment 字段。
6. `review-spatiotemporal-relation-alternatives`。
7. `materialize-spatiotemporal-relation-evidence-packet` 默认 artifact 与显式 basis handoff。

验收命令：

1. `python3 -m unittest tests.test_spatiotemporal_relation_taxonomy`
2. `python3 -m unittest discover -s tests`

本文后续只记录 taxonomy/contract 相关 bugfix 或边界修订。真实案例评测、DB-only recovery、runtime-governed execution、schema migration、模块拆分和 CI 质量门分别由独立计划跟踪。

## 2. 当前 Taxonomy 落地状态

本节保留历史问题和当前处理状态，便于审阅后续变更是否偏离原始边界。

已经具备：

1. `apply-approved-formal-public-taxonomy`
   - 对 public/formal signals 应用显式 approved mission-scoped taxonomy。
   - 无 taxonomy approval ref 时不输出强标签。
   - 输出 candidate labels，而不是报告结论。
2. `formal_signal_semantics.py`
   - 包含 formal/public issue、concern、citation、stance 等规则族。
   - 有测试覆盖 taxonomy freeze metadata、candidate-only 边界。
3. `normalized_signals.canonical_object_kind`
   - 已区分 `public-discourse-signal`、`formal-comment-signal`、`environment-observation-signal`。
4. optional-analysis helper governance
   - 支持 `taxonomy_version`、`rubric_version`、`approval_ref`、`audit_status` 等治理字段。

原始缺口与当前状态：

1. 环境信号内部 taxonomy 过粗。
   - 多数环境输入都落在 `environment-observation-signal`。
   - 当前已通过 `metadata.signal_role`、`metadata.environment_signal_class` 和 DB metadata index 提供可查询分类。
2. 时空关系 taxonomy 缺位。
   - 原始状态中 `detect-temporal-cooccurrence-cues` 只做 same-day multi-plane co-occurrence cue。
   - 当前已保留 same-day 兼容，并在 structured mode 下表达 source-target relation、lag window、spatial rule、distance、rejection reason。
3. 关系结果未成为 DB-backed canonical object。
   - helper artifact 可以输出线索，但缺少可被 finding、evidence bundle、challenge、report basis 稳定引用的 relation row。
   - 当前以 analysis plane result set 承载 `spatiotemporal-relation-cue`，并提供 relation query 与 evidence packet handoff。
4. verification scope 仍偏文本化。
   - `review-fact-check-evidence-scope` 要求 scope 字段，但没有统一结构化 scope contract。
   - 当前已有 structured scope/relation helper 路径；剩余风险是各任务是否都显式提供 scope。
5. challenger 缺少通用关系质疑对象。
   - 现有 challenge/probe 能承接质疑，但没有专门针对时空关系的标准 objection taxonomy。
   - 当前 challenge/probe/review comment 已支持 relation_id、objection_code、challenged_rule、alternative_explanation、required_followup_evidence、report_risk。

## 3. 设计原则

1. 保留通用性。
   - PM2.5、烟气传输、上游排污、降雨洪水、事故泄漏、水质异常都应使用同一关系基础设施。
2. 不做强因果。
   - 输出 relation cue 或 candidate relation，不输出 transport proof、diffusion proof、source attribution proof。
3. DB-first。
   - 重要关系对象必须能写入 DB，artifact 只作为导出和调试材料。
4. Approved taxonomy only。
   - 任务级 taxonomy 或 relation rule 必须带 approval/version/audit metadata。
5. Challenger first-class。
   - 每个关系对象都必须能被 challenger 以结构化理由质疑。
6. Report basis mediated。
   - helper 输出不能直接进入报告正文，必须由 finding、evidence bundle、proposal、review comment、report section 或 report basis 承接。

## 4. 目标能力

目标不是“证明烟气传输”，而是让系统能审计地表达：

1. 某个候选源事件是否落在指定时间窗口内。
2. 某个受体观测是否落在指定 lag window 内。
3. 两个信号是否满足给定空间规则。
4. 哪些候选关系因缺时间、缺坐标、超窗口、超距离、scope 不一致而被拒绝。
5. 哪些关系只是弱线索，需要下一轮补证。
6. challenger 为什么认为该关系不足以支持报告表述。

## 5. Canonical Taxonomy 扩展

### 5.1 Signal Role Taxonomy

新增或规范化 `metadata.signal_role`：

1. `source-event`
   - 火点、排放事件、事故、上游排污、施工扰动、异常排放记录。
2. `receptor-observation`
   - PM2.5、水质、水位、生物指标、空气质量、土壤指标等受体观测。
3. `context-observation`
   - 风、降雨、温度、流量、湿度、土壤湿度、背景环境条件。
4. `claim-or-report-signal`
   - public/formal 文本中的事件陈述、影响报告、投诉、监管记录。
5. `unknown-environment-signal-role`
   - normalizer 无法可靠判断时使用，不允许静默默认到 source 或 receptor。

### 5.2 Environmental Signal Class

新增或规范化 `metadata.environment_signal_class`：

1. `air-quality`
2. `fire-detection`
3. `meteorology`
4. `hydrology`
5. `water-quality`
6. `soil`
7. `ecology`
8. `emission-or-release-event`
9. `infrastructure-or-operations-event`
10. `unknown-environment-class`

### 5.3 Relation Type Taxonomy

新增 `spatiotemporal_relation.relation_type`：

1. `temporal-window-candidate`
2. `spatial-window-candidate`
3. `spatiotemporal-window-candidate`
4. `same-day-cooccurrence`
5. `lag-window-candidate`
6. `context-window-candidate`
7. `scope-overlap-candidate`
8. `rejected-by-temporal-rule`
9. `rejected-by-spatial-rule`
10. `insufficient-basis`

### 5.4 Relation Status Taxonomy

新增 `relation_status`：

1. `candidate`
2. `weak-candidate`
3. `insufficient-basis`
4. `rejected-by-rule`
5. `needs-human-review`
6. `deprecated-legacy-cue`

### 5.5 Objection Taxonomy

Challenger 使用通用 objection codes：

1. `temporal-window-mismatch`
2. `lag-assumption-unsupported`
3. `spatial-scope-overbroad`
4. `spatial-scope-too-narrow`
5. `coordinate-missing`
6. `timestamp-missing`
7. `source-event-background-noise`
8. `local-alternative-source`
9. `receptor-coverage-gap`
10. `context-variable-missing`
11. `provider-quality-limitation`
12. `taxonomy-misclassification`
13. `report-overclaim-risk`

## 6. DB 对象与持久化设计

### 6.1 新增 Analysis Object

新增 canonical analysis object：

`spatiotemporal-relation-cue`

最低字段：

1. `relation_id`
2. `run_id`
3. `round_id`
4. `relation_type`
5. `relation_status`
6. `source_signal_id`
7. `target_signal_id`
8. `context_signal_ids`
9. `source_role`
10. `target_role`
11. `temporal_rule`
12. `spatial_rule`
13. `lag_window`
14. `time_delta`
15. `distance`
16. `spatial_basis`
17. `temporal_basis`
18. `rejection_reasons`
19. `caveats`
20. `evidence_refs`
21. `lineage`
22. `provenance`
23. `helper_governance`

### 6.2 Analysis Plane Storage

优先写入现有 analysis plane result set，而不是先新增独立物理表。原因：

1. 现有 optional-analysis 查询和治理链路已覆盖 result set。
2. 可减少 schema migration 风险。
3. 论文前更容易做端到端测试。

后续若关系查询成为高频能力，再考虑独立表：

`spatiotemporal_relation_cues`

### 6.3 Artifact 导出

导出路径：

`analytics/spatiotemporal_relation_cues_<round_id>.json`

artifact 只作为导出，不是唯一事实源。

## 7. Skill 调整计划

### 7.1 重构现有 Skill

保留 skill id：

`detect-temporal-cooccurrence-cues`

调整定位：

1. 兼容旧 same-day co-occurrence 输出。
2. 新增 structured relation cue 输出。
3. 新增参数：
   - `--source-role`
   - `--target-role`
   - `--source-class`
   - `--target-class`
   - `--observed-after-utc`
   - `--observed-before-utc`
   - `--lag-min-hours`
   - `--lag-max-hours`
   - `--bbox`
   - `--max-distance-km`
   - `--spatial-rule`
   - `--taxonomy-version`
4. 默认仍只输出低风险 cue，不输出因果方向。
5. 若未提供结构化 scope，则降级到 legacy same-day cue，并标记 `deprecated-legacy-cue` 或 `legacy-compatible`.

### 7.2 不新增窄 Skill

不新增：

1. `match-nasa-firms-window`
2. `match-fire-or-emission-source-window`
3. `prove-smoke-transport`

原因：

1. 会把通用基础设施误做成大气场景适配。
2. NASA FIRMS 只是 source-event 的一种。
3. 专业大气传输模型应单独立项，并基于文献和行业标准。

### 7.3 可选新增薄封装

如演示需要，可新增 thin wrapper：

`detect-spatiotemporal-relation-cues`

但它应调用同一底层实现，并在 registry 中标注为 successor / alias，而不是另起独立逻辑。

## 8. Query Surface 补强

### 8.1 `query-environment-signals`

新增过滤：

1. `--signal-role`
2. `--environment-signal-class`
3. `--metric-family`
4. `--has-coordinates`
5. `--has-timestamp`

### 8.2 `query-normalized-signal`

支持按 metadata index 查询：

1. `signal_role`
2. `environment_signal_class`
3. `relation_candidate_role`

### 8.3 新增或扩展 Relation Query

优先扩展 `query-normalized-signal` 或 `query-signal-corpus`，查询：

1. source event 周边目标观测。
2. receptor window 内候选 source events。
3. 已生成 relation cue。
4. relation cue 的 source/target evidence refs。

如果现有 query surface 过于拥挤，再新增：

`query-spatiotemporal-relations`

## 9. Verification Scope 结构化

新增 mission/hypothesis 可选对象：

`verification_scope`

字段：

1. `verification_question`
2. `receptor_scope`
3. `candidate_source_scope`
4. `study_period`
5. `evidence_window`
6. `lag_window`
7. `spatial_rule`
8. `required_source_roles`
9. `required_target_roles`
10. `required_context_classes`
11. `excluded_inferences`

`review-fact-check-evidence-scope` 的目标状态是从字符串检查升级为结构化 scope review：

1. 缺字段时输出 `scope-required`。
2. 有字段时输出 `scope-reviewed-with-caveats`。
3. 不输出 factual outcome。
4. 不输出 readiness score。

## 10. Challenger 工作面

现有 `open-challenge-ticket` 和 `open-falsification-probe` 继续使用，并支持 relation-oriented 输入字段：

1. `relation_id`
2. `objection_code`
3. `challenged_rule`
4. `alternative_explanation`
5. `required_followup_evidence`
6. `report_risk`

当前 helper：

`review-spatiotemporal-relation-alternatives`

定位：

1. optional-analysis helper。
2. approval-gated。
3. 只生成 objection candidates。
4. 必须由 challenge/probe/review comment 承接后才能进入报告链。

## 11. Reporting 与 Evidence Packet

当前 reporting/council 可引用 packet：

`spatiotemporal-relation-evidence-packet`

最低内容：

1. relation cues summary。
2. accepted relation cue ids。
3. rejected or weak relation cue ids。
4. challenger objections。
5. uncertainty register。
6. report-use constraints。
7. evidence refs。
8. lineage。

报告中允许的表述：

1. “候选时空关系线索”
2. “与指定窗口一致 / 不一致”
3. “需要进一步调查”
4. “无法单独支持因果或归因”

报告中禁止的表述：

1. “证明传输”
2. “确认污染源”
3. “排除本地源”
4. “模型归因成立”

## 12. 测试计划

### 12.1 Contract Tests

1. taxonomy metadata 必须包含 version、approval/audit status。
2. `spatiotemporal-relation-cue` 缺 evidence refs 时拒绝或警告。
3. relation status 不允许输出强因果枚举。
4. legacy same-day cue 不得被当作 report basis。

### 12.2 Skill Tests

1. source event 与 receptor observation 在 lag window 内，输出 candidate。
2. 时间超窗，输出 rejected-by-temporal-rule。
3. 坐标缺失，输出 insufficient-basis。
4. 超出距离阈值，输出 rejected-by-spatial-rule。
5. 未提供 structured scope，保持 legacy 兼容输出。

### 12.3 Workflow Tests

1. normalized environment signals -> relation cue -> finding -> evidence bundle。
2. relation cue -> challenger objection -> readiness needs-more-data。
3. follow-up round carryover relation gap。
4. report basis freeze 不直接消费 helper artifact。

### 12.4 Regression Tests

1. 现有 `detect-temporal-cooccurrence-cues` 测试继续通过。
2. formal/public taxonomy helper 行为不变。
3. query public/formal/environment 不因 metadata 扩展破坏旧输出。

## 13. 实施顺序

本节保留为实施记录。P0-P5 已按第 1.1 节命令完成基础验收；后续不在本文追加新的工程硬化主题。

### P0：契约设计

1. 在 canonical contracts 中定义 `spatiotemporal-relation-cue` shape。
2. 定义 signal role taxonomy 和 environment signal class taxonomy。
3. 更新 helper governance freeze-line。

验收：

1. contract tests 通过。
2. 文档列出允许和禁止语义。

### P1：Normalizer Metadata 补齐

1. NASA FIRMS normalizer 标注 `signal_role=source-event`、`environment_signal_class=fire-detection`。
2. AirNow/OpenAQ/Open-Meteo air-quality 标注 `signal_role=receptor-observation`、`environment_signal_class=air-quality`。
3. Open-Meteo historical wind/precipitation 标注 `signal_role=context-observation`、`environment_signal_class=meteorology`。
4. USGS water 标注 hydrology / receptor 或 context，按 metric 判断。

验收：

1. DB metadata index 可查 signal role 和 environment class。
2. 旧 query 输出兼容。

### P2：重构 `detect-temporal-cooccurrence-cues`

1. 保留旧同日共现。
2. 新增 structured relation mode。
3. 写入 analysis plane result set。
4. 导出 relation cue artifact。

验收：

1. lag window candidate 测试通过。
2. spatial rejection 测试通过。
3. 不输出 causality / transport proof 字段。

### P3：Query 与 Challenger 补强

当前基础实现：

1. `query-spatiotemporal-relations` 已提供 relation_id、relation_status、source_signal_id、target_signal_id、source_role、target_role 查询。
2. `open-challenge-ticket`、`open-falsification-probe`、`post-review-comment` 已承载 relation_id、objection_code、challenged_rule、alternative_explanation、required_followup_evidence、report_risk。
3. `review-spatiotemporal-relation-alternatives` 已输出 objection candidates artifact；该 helper 不直接写 report basis。

原计划任务：

1. 增加 relation-oriented query。
2. 扩展 challenge/probe/review comment 对 relation_id 和 objection_code 的支持。
3. 增加 `review-spatiotemporal-relation-alternatives` 或等价 helper。

验收：

1. challenger 可阻断 relation overclaim。
2. readiness 能把 relation gap 转为 follow-up task。

### P4：Evidence Packet 与案例评测

当前基础实现：

1. `materialize-spatiotemporal-relation-evidence-packet` 已生成 `spatiotemporal-relation-evidence-packet-v1` artifact。
2. packet 汇总 relation cues、accepted/rejected-or-weak cue ids、challenger objections、uncertainty register、report use constraints、evidence refs、lineage 和 `board_handoff.gap_hints`。
3. 默认模式只写 artifact；显式 `--write-basis-objects` 时写入 finding、evidence bundle 和 report section draft。
4. report section draft 文本只表述 candidate relation cue、weak/rejected cue、challenger objection 和不确定性约束，不写 causality、transport proof、source attribution 或 local source exclusion。

原计划任务：

1. 生成 relation evidence packet。
2. 接入 finding/evidence bundle/report section basis。
3. 以烟气传输议题作为案例评测场景，但不预设固定流程或固定结论。

验收：

1. 报告只表述候选关系和不确定性。
2. helper artifact 不直通 report basis。
3. 多轮调查能 carry over relation gap。

### P5：验收准备

本文档此前没有单独定义 P5 能力范围；本次 P5 只作为验收准备，不扩展第 14 节排除的专业模型能力。

当前验收面：

1. `tests/test_spatiotemporal_relation_taxonomy.py` 覆盖 structured relation cue、relation query、relation objection、relation probe、evidence packet artifact、显式 basis handoff。
2. 验收命令：`python3 -m unittest tests.test_spatiotemporal_relation_taxonomy`。
3. 全量回归命令：`python3 -m unittest discover -s tests`。
4. `materialize-spatiotemporal-relation-evidence-packet` 可作为案例评测/验收链路的一个 reporting 边界检查点；它不运行 HYSPLIT、WRF-Chem、Gaussian plume、化学传输模型、污染源解析、健康风险或合规裁决。

## 14. 与专业化模型的边界

本计划不做：

1. HYSPLIT。
2. WRF-Chem。
3. Gaussian plume。
4. 化学传输模型。
5. 专业污染源解析。
6. 健康风险或合规裁决。

这些应作为后续专业化 skill 包，且必须单独查阅文献、行业标准和模型适用边界。

本计划只做：

1. 时空窗口候选关系。
2. 数据覆盖和 scope 可审计。
3. Challenger 质疑路径。
4. Report basis 可引用的关系证据包。

## 15. 论文表述建议

建议表述：

`OpenClaw 通过显式 taxonomy、结构化 scope、DB-backed 时空关系线索、challenger objection 和 report-basis freeze，把环境争议中的“可能相关”与“可报告结论”分离，从而降低多 agent 系统在证据不足时过度归因的风险。`

避免表述：

1. “系统完成烟气传输模拟。”
2. “系统证明污染源。”
3. “时空匹配等于因果关系。”
4. “PM2.5 是系统唯一或主要适用领域。”

## 16. 收尾与后续独立计划

本文不再维护统一后续计划。相关后续工作按独立文档跟踪：

1. `docs/openclaw-case-study-evaluation-workplan.md`
   - 将 PM2.5 / 烟霾议题作为真实案例评测场景，修复问题后再沉淀可回放轨迹。
2. `docs/openclaw-db-only-recovery-hardening-workplan.md`
   - 验证 relation cue、packet、board、reporting 在 artifact 缺失时的 DB-first 恢复。
3. `docs/openclaw-runtime-governed-execution-workplan.md`
   - 验证正式 relation helper 和 reporting 链路走 runtime-governed execution。
4. `docs/openclaw-schema-migration-hardening-workplan.md`
   - 将 relation metadata/index 和后续 schema 变化纳入 version/migration。
5. `docs/openclaw-module-decomposition-workplan.md`
   - 拆分 relation helper 所在大模块，保持行为不变。
6. `docs/openclaw-ci-quality-gates-workplan.md`
   - 将 relation taxonomy、helper guardrail 和 case-study replay 纳入回归门。
