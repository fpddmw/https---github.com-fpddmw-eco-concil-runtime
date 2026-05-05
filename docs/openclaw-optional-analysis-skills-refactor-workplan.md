# OpenClaw Optional-Analysis Skills 治理架构

## 1. 文档定位

本文描述 optional-analysis skills 的当前架构、用途、可靠性边界和后续补强方向。文件名保留历史路径，但本文不再是历史施工记录。

## 2. 核心原则

optional-analysis helper 的定位是：

`approval-gated helper view, not default judgement`

硬边界：

1. 不作为 workflow 必经层。
2. 不作为 phase gate。
3. 不直接成为 report basis。
4. 不输出 claim true/false。
5. 不输出强因果、传播方向、代表性充分等高风险结论。
6. 进入报告正文前，必须被 finding、evidence bundle、proposal、review comment、report section draft 或 report basis 显式承接。
7. helper 输出默认可进入 appendix、audit、uncertainty、challenge context。

## 3. 治理字段

每个 optional helper 必须带：

1. `decision_source`
   - 默认 `approved-helper-view`。
2. `rule_id`
   - 例如 `HEUR-ENV-AGGREGATE-001`。
3. `rule_version`
   - 当前统一使用 freeze-line version。
4. `taxonomy_version` 或 `rubric_version`
   - 适用于 taxonomy/rubric helper。
5. `approval_ref`
   - 来自 skill approval flow 或显式输入。
6. `audit_ref`
   - 指向审计说明或文档。
7. `rule_trace`
   - 标明启发式来源。
8. `caveats`
   - 明确不可做何种解释。
9. `audit_status`
   - 当前多数为 `default-frozen; approval-required; audit-pending`。
10. `helper_status`
   - 默认 `approval-gated-helper-view`。

## 4. Optional Skills 分组

### 4.1 环境证据辅助

`aggregate-environment-evidence`

用途：

1. 汇总 normalized environment signals。
2. 输出 source、metric、spatial、temporal 分布。
3. 输出 coverage limitations。

边界：

1. 不做 claim matching。
2. 不做 readiness scoring。
3. 不判断污染来源。

`review-fact-check-evidence-scope`

用途：

1. 要求显式 verification question。
2. 检查地理范围、研究期、证据窗口、lag assumptions、metric/source requirements 是否明确。
3. 输出 scope caveats。

边界：

1. 不输出 true/false。
2. 不输出 support/contradiction。
3. 不输出 phase-gate posture。

`review-evidence-sufficiency`

用途：

1. 读取 finding、evidence bundle、report section draft、review comments。
2. 输出 sufficiency notes、gaps、counter-evidence cues、uncertainty notes。

边界：

1. 不输出 numeric readiness score。
2. 不证明 claim。
3. 不自动允许 freeze。

### 4.2 公共争议与研究议题辅助

`discover-discourse-issues`

用途：

1. 从 public/formal signals 发现叙事、议题、stakeholder 表达。
2. 输出 discourse issue hints。

边界：

1. hint 不是 fact claim。
2. mentioned scope 不是 study scope。

`suggest-evidence-lanes`

用途：

1. 对 approved hints 或 findings 提供 evidence-lane tags。

边界：

1. 不分配 owner。
2. 不驱动 source queue。
3. 不推进 phase。

`materialize-research-issue-surface`

用途：

1. 将 approved discovery、mission question、DB evidence basis 组织为 research issue surface。

边界：

1. 默认 appendix/audit only。
2. 进入报告正文前必须被 DB basis 引用。

`project-research-issue-views`

用途：

1. 从 research issue surface 投影 actor、stance、concern、citation 等 typed views。

边界：

1. 不验证身份。
2. 不裁决代表性。
3. 不重新编码证据。

`export-research-issue-map`

用途：

1. 输出 issue/view traceability map。

边界：

1. edge 是导航线索，不是因果或影响关系。

### 4.3 Formal / Public 对照辅助

`apply-approved-formal-public-taxonomy`

用途：

1. 对 formal/public signals 应用显式 approved taxonomy。

边界：

1. 无 taxonomy approval ref 时不得输出强标签。
2. label 是 candidate label。

`compare-formal-public-footprints`

用途：

1. 对比 formal record 与 public discourse 是否都有 source footprint。
2. 输出 overlap terms 和 coverage caveats。

边界：

1. 不表示观点一致。
2. 不输出 alignment score。
3. 不判定代表性充分。

`identify-representation-audit-cues`

用途：

1. 输出 source/stakeholder coverage audit prompts。

边界：

1. 不输出 representation gap finding。
2. 不输出 severity score。

`detect-temporal-cooccurrence-cues`

用途：

1. 检测 public/formal/environment signals 的时间邻近或同日共现。
2. 在显式 source/target/scope 参数下输出 `spatiotemporal-relation-cue`。

边界：

1. 不推断传播、影响、因果或方向。
2. timestamp 缺失时输出 insufficient temporal basis。

`review-spatiotemporal-relation-alternatives`

用途：

1. 查询 analysis plane 中的 `spatiotemporal-relation-cue`。
2. 生成 relation objection candidates。
3. 为 `open-challenge-ticket`、`open-falsification-probe`、`post-review-comment` 提供 relation_id / objection_code / challenged_rule / follow-up evidence 输入。

边界：

1. 不关闭、证明或否定 relation cue。
2. 不直接写 readiness 或 report basis。
3. 报告链使用前必须由 DB council/reporting object 显式承接。

### 4.4 流程辅助

`plan-round-orchestration`

1. 为 moderator 生成 advisory plan。
2. 不拥有阶段推进权。

`propose-next-actions`

1. 生成 optional next-action suggestions。
2. 不成为默认 investigator queue。

`open-falsification-probe`

1. 将 probe-worthy actions 转为 falsification probes。
2. 服务 challenger/moderator。

`summarize-round-readiness`

1. 汇总 readiness evidence。
2. 不提交正式 phase transition。

## 5. 可靠性判断

工程可靠性：

1. 运行前需要 skill approval。
2. registry 强制标注 helper governance。
3. 测试覆盖旧入口移除、helper 非 gate、非 report basis、无 alignment/severity/diffusion 语义。
4. 输出包含 caveats 和 audit status。

专业可靠性：

1. 当前 helper 多数是启发式、描述性、审计型。
2. 不应被论文或案例展示表述为专业环境模型。
3. 它们可靠地完成“辅助组织证据和暴露不确定性”，不可靠地完成“污染归因或事实裁决”。

## 6. Challenger 检查点

challenger 应重点质疑：

1. scope 是否明确。
2. time window 是否匹配。
3. spatial scope 是否过窄。
4. source coverage 是否偏斜。
5. taxonomy 是否带入价值判断。
6. aggregation 是否抹平异质性。
7. temporal cue 是否被误读为传播因果。
8. helper cue 是否被错误放入报告正文。

## 7. 后续补强入口

近期不建议扩大 optional helper 数量。`spatiotemporal-relation` structured relation mode、relation cue、relation query、challenger objection 和 evidence packet 已进入当前 baseline。

后续重点是硬化运行、恢复和展示边界，分别由独立计划跟踪：

1. `docs/openclaw-db-only-recovery-hardening-workplan.md`
   - 验证 helper artifact 缺失时，analysis/result set、council object 和 reporting basis 能从 DB 恢复。
2. `docs/openclaw-runtime-governed-execution-workplan.md`
   - 验证 optional-analysis helper 在正式运行中必须经过 approval、receipt 和 ledger。
3. `docs/openclaw-case-study-evaluation-workplan.md`
   - 在真实案例评测中验证报告只使用 cautious/withheld 表述，不把 helper cue 误写成结论。
4. `docs/openclaw-ci-quality-gates-workplan.md`
   - 将 helper-not-report-basis、approval-gated 和 relation overclaim guardrail 纳入回归门。

验收标准：

1. 新 helper 仍 approval-gated。
2. 输出必须带 evidence refs、lineage、provenance。
3. 输出必须带 uncertainty/caveats。
4. 不得直接写 readiness_score 或 freeze_allowed。
5. 报告正文引用必须经过 DB council/reporting basis。
