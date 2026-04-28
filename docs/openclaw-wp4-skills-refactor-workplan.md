# OpenClaw WP4 Skills Refactor Workplan

日期：2026-04-28

状态：WP4 后续唯一施工清单。本文件取代此前分散的 WP4 静态审计稿、批次裁决回写和 challenger 补强过程记录。后续 WP4 代码、测试和文档交付均以本文件为准。

## 1. 总裁决

WP4 的核心结论：

1. 所有规则化 / 启发式 skill 默认都是 approval-gated helper view。
2. helper 不得作为 workflow 必经层、phase gate、默认调查方向、报告结论或 evidence basis 的直接来源。
3. helper 输出进入后续研究链前，必须被 agent 明确引用，并通过 finding / evidence bundle / proposal / review comment / challenge / report basis 等 DB council objects 承接。
4. moderator 仍是唯一阶段推进者；helper 不得通过 route、readiness、severity、alignment、suggested next skill 等字段隐式推进流程。
5. challenger 必须能质疑 scope、taxonomy、rubric、source coverage、aggregation、framing、timestamp quality、participation frame 和 report usage。
6. 报告正文默认只消费 finding / evidence bundle / approved research issue surface / report basis；helper cue 默认进入 appendix / audit / uncertainty，除非 moderator 或 report basis 明确引用。

## 2. 裁决过程摘要

本轮裁决按风险从高到低收口：

1. 废弃 `claim -> observation -> support / contradiction` 主轴，禁止 claim truth 语义。
2. 废弃 coverage/readiness 公式，重建为 evidence sufficiency review。
3. 废弃 observation candidate 双层压缩链，重建为环境证据发现/聚合视图。
4. 废弃 claim/scope/route 主链，重建为 discourse / issue discovery 和 evidence-lane advisory。
5. 区分 `public discourse issue hints` 与 `research issue / evidence issue surface`；前者是线索，后者必须全证据 DB-backed。
6. typed issue decomposition 只保留为 approved research issue surface 的 projection/view。
7. formal/public linkage 重建为 footprint comparison，不输出 alignment/agreement。
8. representation gap 与 diffusion edge 降级为 audit cues / temporal co-occurrence cues。

## 3. Skill 重构总表

| 原 skill / helper | 原职责与主要问题 | WP4 操作 | 重构后 skill / helper | 重构后职责 | 来源关系与硬边界 |
| --- | --- | --- | --- | --- | --- |
| `link-claims-to-observations` | 把 claim 文本映射到环境指标、时间窗和阈值，输出 `support / contradiction / contextual`。风险是复活 claim truth matching。 | 废弃当前形态；短期只可作为 deprecated alias 阻断旧调用。 | `review-fact-check-evidence-scope` | 在 agent 显式提交核验问题、地理范围、研究期、证据匹配窗口、传播/滞后假设、metric/source 要求后，检查环境证据是否覆盖这些范围。 | 由 `link-claims-to-observations` 重建；不得输出 support / contradiction / true / false；不得默认进入报告 basis。 |
| `score-evidence-coverage` | 用 claim-observation link、scope、support/contradiction 计数和 confidence 计算 coverage/readiness。 | 废弃当前公式。 | `review-evidence-sufficiency` | 基于 finding / evidence bundle / report section basis 做结构化 sufficiency review，指出覆盖维度、缺口、counter-evidence、不确定性。 | 由 `score-evidence-coverage` 重建；默认不使用 numeric readiness score；不得成为 phase gate。 |
| `extract-observation-candidates` | 从 environment normalized signals 按 source/metric/rounded point 聚合 observation candidates。 | 与 merge/scope 合并重建。 | `aggregate-environment-evidence` | 从 DB normalized environment signals 生成环境证据集合/聚合视图，含统计、空间/时间/指标/source 分布和 coverage limitations。 | 由 observation 三件套合并而成；不再生成 observation candidate；只描述证据覆盖与限制。 |
| `merge-observation-candidates` | 对 observation candidates 做第二级 point/date bucket 合并。风险是抹平 provider、空间、时间、质量差异。 | 与 extract/scope 合并重建。 | `aggregate-environment-evidence` | 同上；聚合必须由 caller 显式给出 region/time/metric/source/quality/aggregation method。 | 不再保留 `merged observation candidate` 抽象；输出必须记录 bucket/aggregation basis 与 caveats。 |
| `derive-observation-scope` | 有 geometry 即 `usable_for_matching=True`，按 metric 打 matching tags。 | 废弃 matching scope；职责并入环境证据视图。 | `aggregate-environment-evidence` | 输出中性 metadata：spatial/temporal/metric/source scope status、resolution、coverage limitations。 | `usable_for_matching` 直接删除；不得建议 coverage scoring 或 claim linking。 |
| `extract-claim-candidates` | 从 public signals 用 issue/concern/actor/citation/stance 词典抽 claim candidates。 | 废弃 claim 主语义。 | `discover-discourse-issues` | 发现公共叙事、议题线索、stakeholder 表达和 text evidence snippets。 | 由 claim extraction/cluster/scope 部分职责重建；输出是 discourse issue hints，不是事实 claim。 |
| `cluster-claim-candidates` | 按 claim type、issue、stance、concern signature 聚合 claim clusters。 | 废弃 claim cluster；改为可逆 discourse grouping。 | `discover-discourse-issues` | 对 discourse signals 做可逆 grouping，记录成员 source ids、合并规则、差异和未合并原因。 | clustering confidence 只能表示 extraction/grouping confidence，不表示真实性、重要性或代表性。 |
| `derive-claim-scope` | 从文本推 city/regional/national、metric tags、evidence lane。风险是把文本提及当研究范围。 | 降级为 discovery metadata。 | `discover-discourse-issues` | 输出 `mentioned_places`、`mentioned_time_refs`、`mentioned_metrics`、`mentioned_policy_objects`、`mentioned_actors`。 | 所有字段必须表达 mentioned；不得定义 study / fact-check / investigation scope。 |
| `classify-claim-verifiability` | 将 verifiability kind 映射到 lane，并判断 matching readiness。 | 废弃 claim verifiability classifier 形态。 | `suggest-evidence-lanes` | 对 approved discovery hints 或 research issue surface 提供 optional evidence-lane suggestions。 | 不得输出 route owner、matching readiness 或强制调查路径；只做 advisory tags。 |
| `route-verification-lane` | 按 lane 输出 route status 和 suggested next skills，environmental lane 指向 legacy link/coverage。 | 废弃 workflow route 语义。 | `suggest-evidence-lanes` | 同上；可被 agent/moderator 显式调用做证据类型提示。 | 不得驱动 source queue、phase、default investigator loop；删除 legacy handoff。 |
| `extract-issue-candidates` | claim clusters 缺失时从 claim scopes 生成 issue candidates。风险是从 claim fallback 再生 issue layer。 | 废弃旧 claim-derived issue candidate 链；按输入分流。 | `discover-discourse-issues` 或 `materialize-research-issue-surface` | 文本聚合输出 public discourse issue hints；研究组织输出 research issue surface。 | 不得从未批准 claim/scope/route fallback 静默生成 issue。 |
| `cluster-issue-candidates` | 从 claim clusters/scopes/routes 生成 canonical issue clusters。 | 废弃旧自动二次抽象链；重建 research issue surface。 | `materialize-research-issue-surface` | 基于 mission/moderator question、approved discovery、formal/environment summaries、findings、evidence bundles、challenger objections 形成研究问题面。 | research issue surface 必须全证据 DB-backed；不是启发式直接结论。 |
| `extract-stance-candidates` | 将 issue stance distribution 投影成 stance groups。 | 保留为 projection/view。 | `project-research-issue-views` | 对 approved research issue surface 输出 stance view。 | 不重新编码证据；不得把复杂立场压成 support/oppose；允许 mixed/conditional/unclear。 |
| `extract-concern-facets` | 将 issue concern labels 投影成 concern facets。 | 保留为 projection/view。 | `project-research-issue-views` | 输出 concern facets view。 | taxonomy 必须 approved/versioned；允许 unknown/ambiguous/other。 |
| `extract-actor-profiles` | 将 actor hints 投影成 actor profiles。 | 保留为高敏 projection/view。 | `project-research-issue-views` | 输出 actor label candidates、source-claimed actor、submitter metadata basis。 | 不得暗示身份已验证、组织身份已确认或代表性成立。 |
| `extract-evidence-citation-types` | 将 citation labels 投影成 evidence citation type objects。 | 保留为 projection/view。 | `project-research-issue-views` | 输出 citation type view。 | 不得把 citation label 当证据质量裁决；保留 taxonomy/caveats。 |
| `materialize-controversy-map` | 聚合 issue/typed surfaces；缺失时 inline fallback 重跑上游规则。 | 改为只读 DB view/export。 | `export-research-issue-map` | 从 approved research issue surfaces 与 typed projections 生成报告/审议 view。 | 禁止 inline fallback；不得生成 actionable gaps、agenda、readiness 或 policy recommendation。 |
| `formal_signal_semantics.py` | 内含 submitter/issue/concern/citation/stance/route taxonomy。风险是伪装成 normalizer 附属实现。 | 拆成 approved taxonomy helper families。 | `apply-approved-formal-public-taxonomy` | 对 formal/public records 输出 candidate labels/cues。 | 不得默认套用全局 taxonomy；只可 mission-scoped 或 approved versioned taxonomy。 |
| `link-formal-comments-to-public-discourse` | 按 issue token score 链 formal/public，并输出 `aligned` 和 alignment score。 | 废弃 alignment/link 语义。 | `compare-formal-public-footprints` | 比较 formal record 与 public discourse 在 approved issue/question basis 上是否都有 source footprint。 | 不表示观点一致、实质对齐、代表性充分或因果影响；删除 claim-side support bonus。 |
| `identify-representation-gaps` | 用 public-only/formal-only/count ratio/route mismatch 输出 representation gap 和 severity。 | 降级为 audit cue。 | `identify-representation-audit-cues` | 提示可能需要人工检查的 source/stakeholder coverage 不均衡。 | 不输出已证实 participation gap、underrepresentation、severity 或 legitimacy judgement。 |
| `detect-cross-platform-diffusion` | 用 issue/platform/first-seen time 生成 diffusion edges 和 confidence。 | 降级为 temporal cue。 | `detect-temporal-cooccurrence-cues` | 输出同一 issue/question basis 上跨平台/跨 source 的时间邻近或共现线索。 | 不输出 influence、causality、传播方向已确定；缺失 timestamp 不得 fallback 到 1970。 |
| `plan-round-orchestration` | 从 DB-backed council state 生成 advisory plan。 | 保留但继续 approval-gated。 | `plan-round-orchestration` | optional planning advice。 | 不拥有阶段推进权；不得替代 moderator transition。 |
| `propose-next-actions` | 排序 next-action candidates。 | 保留但降权为 advisory helper。 | `propose-next-actions` | optional next-action suggestions。 | 不得成为默认 investigator loop；action 必须经 DB council object 承接。 |
| `open-falsification-probe` | 从 challenge/proposal context 打开 falsification probe。 | 保留为 challenger/moderator helper。 | `open-falsification-probe` | approved probe support。 | 只对明确 challenge/proposal/framing/rubric/scope 问题服务。 |
| `summarize-round-readiness` | 聚合 readiness signals。 | 保留但不得做 gate。 | `summarize-round-readiness` | optional readiness opinion / summary。 | 不得自动推进阶段；正式 transition 仍由 moderator 发起。 |

## 4. 批次执行清单

### WP4.0 规则元数据与总护栏

- [x] 在 shared helper / canonical contract 中补齐 helper metadata：`decision_source`、`rule_id`、`rule_version`、`taxonomy_version` 或 `rubric_version`、`approval_ref`、`audit_ref`、`rule_trace`、`caveats`。
- [x] 定义允许的 `decision_source`：`approved-helper-view`、`deprecated-legacy-helper`、`manual-or-moderator-defined`、`agent-submitted-finding`、`scenario`。
- [ ] 所有 optional-analysis 输出必须带 item-level `evidence_refs`、`lineage`、`provenance`。
- [ ] 清理所有 `board_handoff.suggested_next_skills` 中的旧链路提示。
- [x] runtime registry / source queue / agent prompt 中继续保证 heuristic helper approval-gated，不进默认链。

验收：

- [x] 任一 WP4 helper 无 approval/audit metadata 时测试失败。
- [x] 默认 investigator loop 不调用 WP4 helper。
- [ ] helper 输出不能直接进入 report basis，除非被 finding / evidence bundle / proposal / report basis 引用。

### WP4.1 Legacy link 与 coverage 替换

- [x] 废弃或阻断 `link-claims-to-observations` 当前入口。
- [ ] 新增或重建 `review-fact-check-evidence-scope`。
- [x] 废弃 `score-evidence-coverage` 当前公式。
- [ ] 新增或重建 `review-evidence-sufficiency`。
- [x] 移除 support / contradiction / claim_true / claim_false / readiness score 默认输出。

验收：

- [ ] 事实核验 helper 必须要求显式核验问题、地理范围、研究期、证据匹配窗口、传播/滞后假设、metric/source 要求。
- [ ] evidence sufficiency review 默认输出 notes，不输出 numeric readiness score。
- [ ] 旧 coverage/link 测试改写为 scope/sufficiency/caveat 测试。

### WP4.2 Environment evidence aggregation

- [ ] 将 `extract-observation-candidates`、`merge-observation-candidates`、`derive-observation-scope` 收敛为 `aggregate-environment-evidence`。
- [ ] 删除 `observation candidate / merged observation / usable_for_matching` 主语义。
- [ ] 输出 statistics summary、spatial distribution、temporal distribution、metric distribution、source distribution、coverage limitations、metadata tags。
- [ ] 保留 source signal ids、provider/source_skill、artifact refs、record locator。

验收：

- [ ] 不再输出 `usable_for_matching`。
- [ ] 不再建议 `link-claims-to-observations` 或 `score-evidence-coverage`。
- [ ] 聚合方法、bucket、source distribution 和 caveats 可从 DB 复原。

### WP4.3 Discourse discovery 与 evidence-lane advisory

- [ ] 将 `extract-claim-candidates`、`cluster-claim-candidates`、`derive-claim-scope` 收敛为 `discover-discourse-issues`。
- [ ] 将 `classify-claim-verifiability`、`route-verification-lane` 收敛为 `suggest-evidence-lanes`。
- [ ] 删除 claim candidate / claim cluster / claim scope / route owner / matching readiness 主语义。
- [ ] 输出 public discourse issue hints、text evidence snippets、source distribution、taxonomy labels、mentioned_scope_metadata、coverage caveats。

验收：

- [ ] discovery 输出不得称为事实 claim。
- [ ] `mentioned_*` 不得被提升为 study scope。
- [ ] evidence-lane suggestion 不得驱动 workflow、source queue 或 phase。

### WP4.4 Research issue surface 与 typed projections

- [ ] 将旧 `extract-issue-candidates` / `cluster-issue-candidates` 重建为 `materialize-research-issue-surface` 或废弃为 discovery alias。
- [ ] research issue surface 只消费 mission/moderator question、approved discovery、formal typed signals、environment summaries、findings、evidence bundles、challenger objections。
- [ ] typed issue skills 收敛为 `project-research-issue-views`。
- [ ] `materialize-controversy-map` 收敛为 `export-research-issue-map`。

验收：

- [ ] 不得从未批准 claim/scope/route fallback 生成 issue。
- [ ] typed projections 不重新编码证据。
- [ ] controversy map 禁止 inline fallback，缺输入时报告 missing inputs。

### WP4.5 Formal/Public footprint 与 taxonomy

- [ ] 将 `formal_signal_semantics.py` 拆为 approved taxonomy family records。
- [ ] 将 `link-formal-comments-to-public-discourse` 重建为 `compare-formal-public-footprints`。
- [ ] 删除 `aligned`、alignment score、claim-side-only、claim support bonus。
- [ ] 输出 formal footprint、public footprint、overlap status、descriptive balance metrics、coverage caveats。

验收：

- [ ] footprint comparison 不表示观点一致或代表性充分。
- [ ] submitter type 只作为 candidate label，并保留 metadata/text basis。
- [ ] taxonomy 未审批时不能输出强标签。

### WP4.6 Representation 与 temporal co-occurrence cues

- [ ] 将 `identify-representation-gaps` 重建为 `identify-representation-audit-cues`。
- [ ] 将 `detect-cross-platform-diffusion` 重建为 `detect-temporal-cooccurrence-cues`。
- [ ] 删除 gap/severity/influence/causality/spillover 确定语义。
- [ ] timestamp 缺失时输出 insufficient temporal basis，不得 fallback 到 1970。

验收：

- [ ] representation cue 不输出已证实 underrepresentation。
- [ ] temporal cue 不输出传播方向已确定。
- [ ] 报告正文默认不消费 cue，除非 report basis 明确引用。

### WP4.7 Audit records、docs、tests 收口

- [ ] 将本文件第 `8` 节 freeze-line placeholder rows 替换为最终 versioned audit records。
- [ ] 更新所有触达 skill 的 `SKILL.md`、agent prompts、runtime registry description。
- [ ] 重写依赖 coverage/linkage/route/map 的旧测试。
- [ ] 增加 approval-gated helper、deprecated alias、DB-only recovery、report basis 引用测试。
- [ ] 更新本文件的完成状态、未完成项、新发现问题、是否影响后续计划。

## 5. 必须删除或替换的旧语义

以下字段、标签或行为不得在新默认语义中继续出现：

1. `support` / `contradiction` / `claim_true` / `claim_false`
2. `usable_for_matching`
3. `matching-ready`
4. `route owner` / workflow route
5. `aligned` 表示观点一致
6. alignment score 表示 agreement strength
7. representation severity 表示现实缺口强度
8. diffusion / spillover / influence 表示因果传播
9. timestamp 缺失 fallback 到 1970
10. helper 内部 `suggested_next_skills` 引回 legacy chain

## 6. Challenger 要求

WP4 helper 必须支持 challenger 对以下内容提交 review comment / challenge：

1. scope：地理范围、研究期、证据匹配窗口、传播/滞后假设。
2. taxonomy：issue / concern / actor / citation / submitter type 是否过窄或带结论导向。
3. rubric：evidence sufficiency rubric 是否经过批准，是否隐藏权重。
4. source coverage：平台/API/docket/source family 覆盖是否足以支撑报告用途。
5. aggregation：bucket、统计摘要、source mixing 是否抹平异质性。
6. framing：helper 是否把线索包装成结论。
7. report usage：helper cue 是否被错误放入正文结论。

## 7. 后续工作基准

后续 WP4 工作只以本文件为基准。若代码实施中需要调整命名或兼容策略，必须在最终交付时回写本文件，并说明：

1. 已完成。
2. 未完成。
3. 新发现的问题。
4. 是否影响后续计划。
5. 实际运行的最小测试集合与结果。

## 7.1 2026-04-28 本批收口（WP4.0 / WP4.1 起步）

已完成：

1. `eco_council_runtime.analysis_objects` 新增 WP4 helper metadata helper 和允许的 `decision_source` 集合：`approved-helper-view / deprecated-legacy-helper / manual-or-moderator-defined / agent-submitted-finding / scenario`。
2. `kernel/skill_registry.py` 已为 WP4 optional-analysis / advisory helper 补齐 freeze-line metadata：`rule_id / rule_version / audit_status / wp4_destination / caveats`，并通过 registry snapshot 暴露。
3. `link-claims-to-observations` 默认入口已改为 `deprecated-blocked`，只写 deprecated-helper stop artifact；不再加载旧输入、不再输出 link rows、不再 sync analysis result、不再通过 `board_handoff.suggested_next_skills` 指向旧链。
4. `score-evidence-coverage` 默认入口已改为 `deprecated-blocked`，只写 deprecated-helper stop artifact；不再运行旧 coverage 公式、不再输出 numeric gate posture、不再 sync analysis result、不再返回旧链路建议。
5. 已更新上述两个 skill 的 `SKILL.md` 与 OpenAI agent prompt，明确它们是 WP4 deprecated alias，不应用于新调查链。
6. 新增 `tests/test_wp4_helper_guardrails.py`，覆盖 registry freeze metadata、两个 deprecated alias 不输出旧默认语义、`board_handoff.suggested_next_skills` 为空。

未完成：

1. 尚未新增 `review-fact-check-evidence-scope` 与 `review-evidence-sufficiency` successor skills。
2. 尚未把全部 optional-analysis skill 的 item-level 输出统一补齐 `evidence_refs / lineage / provenance / wp4_helper_metadata`。
3. 尚未清理所有 helper 的旧 `board_handoff.suggested_next_skills`；本批只阻断了 `link-claims-to-observations` 与 `score-evidence-coverage` 两个 WP4.1 高风险入口。
4. 尚未批量改写依赖旧 coverage/link/map 的历史 workflow 测试；本批新增的是 guardrail 回归。

新发现的问题：

1. 旧 workflow 测试和若干历史链路仍直接调用 `link-claims-to-observations` / `score-evidence-coverage` 作为中间产物生成器；本批阻断默认语义后，这些旧测试需要按 WP4 successor helper 与 DB council-object 承接模式重写。
2. runtime 的审批链已经存在，但直接运行 skill 脚本仍可得到 deprecated stop artifact；这符合本批“阻断旧语义”的目标，但 successor helper 落地前不会产出可替代的 scope/sufficiency review。
3. `build-normalization-audit` 原本不在本文件 freeze-line 表中；本批在 registry metadata 中补了 `HEUR-NORMALIZATION-AUDIT-001`，后续应决定是否把它正式纳入第 8 节审计表。

是否影响后续计划：

1. 不阻塞后续 WP4.2-WP4.7，但会使依赖旧 link/coverage 产物的旧测试或 demo 需要重写，不能再用旧语义维持兼容。
2. 下一批建议优先实现 `review-evidence-sufficiency` 的 DB-backed notes/caveats 输出，再处理 `aggregate-environment-evidence`，这样可先恢复报告 evidence sufficiency review 的新链路。

本批实际运行测试：

1. `.venv/bin/python -m unittest tests.test_wp4_helper_guardrails tests.test_skill_approval_workflow`：8 项通过。
2. `.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate`：10 项通过。

## 8. 规则审计 freeze line

本节收敛原独立规则审计台账。当前记录只是冻结线，不表示任何规则已被审计通过。

最终 versioned audit record 至少需要包含：

1. `rule_id`
2. `skill`
3. `rule_text`
4. `trigger_conditions`
5. `exception_conditions`
6. `known_biases`
7. `representativeness_risks`
8. `sample_input`
9. `sample_output`
10. `audit_status`
11. `effective_version`

状态含义：

1. `default-frozen`：不得进入默认 investigator、moderator、source queue 或 controller chain。
2. `approval-required`：执行必须经过 `skill_approval_requests -> approval -> run-skill --skill-approval-request-id`。
3. `audit-pending`：仍待详细人工审计；该行只是 freeze-line placeholder。
4. `legacy-isolated`：当前语义属于 legacy 或 deprecated，重建前不得作为新链路使用。

| rule_id | skill | current status | WP4 destination |
| --- | --- | --- | --- |
| `HEUR-NORMALIZATION-AUDIT-001` | `build-normalization-audit` | `legacy-isolated; default-frozen; approval-required; audit-pending` | operator QA export or removal |
| `HEUR-CLAIM-EXTRACT-001` | `extract-claim-candidates` | `default-frozen; approval-required; audit-pending` | `discover-discourse-issues` |
| `HEUR-CLAIM-CLUSTER-001` | `cluster-claim-candidates` | `default-frozen; approval-required; audit-pending` | `discover-discourse-issues` |
| `HEUR-CLAIM-SCOPE-001` | `derive-claim-scope` | `default-frozen; approval-required; audit-pending` | `discover-discourse-issues` |
| `HEUR-VERIFY-001` | `classify-claim-verifiability` | `default-frozen; approval-required; audit-pending` | `suggest-evidence-lanes` |
| `HEUR-ROUTE-001` | `route-verification-lane` | `default-frozen; approval-required; audit-pending` | `suggest-evidence-lanes` |
| `HEUR-ISSUE-EXTRACT-001` | `extract-issue-candidates` | `default-frozen; approval-required; audit-pending` | `discover-discourse-issues` or `materialize-research-issue-surface` |
| `HEUR-ISSUE-CLUSTER-001` | `cluster-issue-candidates` | `default-frozen; approval-required; audit-pending` | `materialize-research-issue-surface` |
| `HEUR-STANCE-001` | `extract-stance-candidates` | `default-frozen; approval-required; audit-pending` | `project-research-issue-views` |
| `HEUR-CONCERN-001` | `extract-concern-facets` | `default-frozen; approval-required; audit-pending` | `project-research-issue-views` |
| `HEUR-ACTOR-001` | `extract-actor-profiles` | `default-frozen; approval-required; audit-pending` | `project-research-issue-views` |
| `HEUR-CITATION-001` | `extract-evidence-citation-types` | `default-frozen; approval-required; audit-pending` | `project-research-issue-views` |
| `HEUR-MAP-001` | `materialize-controversy-map` | `default-frozen; approval-required; audit-pending` | `export-research-issue-map` |
| `HEUR-OBS-EXTRACT-001` | `extract-observation-candidates` | `default-frozen; approval-required; audit-pending` | `aggregate-environment-evidence` |
| `HEUR-OBS-MERGE-001` | `merge-observation-candidates` | `default-frozen; approval-required; audit-pending` | `aggregate-environment-evidence` |
| `HEUR-OBS-SCOPE-001` | `derive-observation-scope` | `default-frozen; approval-required; audit-pending` | `aggregate-environment-evidence` |
| `HEUR-LEGACY-LINK-001` | `link-claims-to-observations` | `legacy-isolated; default-frozen; approval-required; audit-pending` | `review-fact-check-evidence-scope` |
| `HEUR-COVERAGE-001` | `score-evidence-coverage` | `default-frozen; approval-required; audit-pending` | `review-evidence-sufficiency` |
| `HEUR-FORMAL-PUBLIC-001` | `link-formal-comments-to-public-discourse` | `default-frozen; approval-required; audit-pending` | `compare-formal-public-footprints` |
| `HEUR-REP-GAP-001` | `identify-representation-gaps` | `default-frozen; approval-required; audit-pending` | `identify-representation-audit-cues` |
| `HEUR-DIFFUSION-001` | `detect-cross-platform-diffusion` | `default-frozen; approval-required; audit-pending` | `detect-temporal-cooccurrence-cues` |
| `HEUR-AGENDA-001` | `plan-round-orchestration` | `default-frozen; approval-required; audit-pending` | approval-gated advisory helper |
| `HEUR-NEXT-ACTION-001` | `propose-next-actions` | `default-frozen; approval-required; audit-pending` | approval-gated advisory helper |
| `HEUR-PROBE-001` | `open-falsification-probe` | `default-frozen; approval-required; audit-pending` | challenger/moderator helper |
| `HEUR-READINESS-001` | `summarize-round-readiness` | `default-frozen; approval-required; audit-pending` | optional readiness opinion |

冻结线规则：

1. 上表 skill 不得出现在默认 agent entry recommendations 或 source-queue downstream hints。
2. 上表 skill 不得绕过已批准且未消费的 skill approval request 执行。
3. 上表输出在完成后续审计前只能视为 derived advisory 或 legacy helper surface。
