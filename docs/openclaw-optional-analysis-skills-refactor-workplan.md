# OpenClaw Optional-Analysis Skills Refactor Workplan

日期：2026-04-28

状态：optional-analysis helper governance 后续唯一施工清单。本文件取代此前分散的静态审计稿、批次裁决回写和 challenger 补强过程记录。后续 optional-analysis 代码、测试和文档交付均以本文件为准。

## 1. 总裁决

optional-analysis helper governance 的核心结论：

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

| 原 skill / helper | 原职责与主要问题 | optional-analysis helper governance 操作 | 重构后 skill / helper | 重构后职责 | 来源关系与硬边界 |
| --- | --- | --- | --- | --- | --- |
| `link-claims-to-observations` | 把 claim 文本映射到环境指标、时间窗和阈值，输出 `support / contradiction / contextual`。风险是复活 claim truth matching。 | 删除旧入口；不保留 alias 或兼容脚本。 | `review-fact-check-evidence-scope` | 在 agent 显式提交核验问题、地理范围、研究期、证据匹配窗口、传播/滞后假设、metric/source 要求后，检查环境证据是否覆盖这些范围。 | 由 `link-claims-to-observations` 重建；不得输出 support / contradiction / true / false；不得默认进入报告 basis。 |
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

### Optional-analysis 0 规则元数据与总护栏

- [x] 在 shared helper / canonical contract 中补齐 helper metadata：`decision_source`、`rule_id`、`rule_version`、`taxonomy_version` 或 `rubric_version`、`approval_ref`、`audit_ref`、`rule_trace`、`caveats`。
- [x] 定义允许的 `decision_source`：`approved-helper-view`、`manual-or-moderator-defined`、`agent-submitted-finding`、`scenario`。
- [ ] 所有 optional-analysis 输出必须带 item-level `evidence_refs`、`lineage`、`provenance`。
- [x] 清理所有 `board_handoff.suggested_next_skills` 中的旧链路提示。
- [x] runtime registry / source queue / agent prompt 中继续保证 heuristic helper approval-gated，不进默认链。

验收：

- [x] 任一 optional-analysis helper 无 approval/audit metadata 时测试失败。
- [x] 默认 investigator loop 不调用 optional-analysis helper。
- [ ] helper 输出不能直接进入 report basis，除非被 finding / evidence bundle / proposal / report basis 引用。

### Optional-analysis 1 Legacy link 与 coverage 替换

- [x] 废弃或阻断 `link-claims-to-observations` 当前入口。
- [x] 新增或重建 `review-fact-check-evidence-scope`。
- [x] 废弃 `score-evidence-coverage` 当前公式。
- [x] 新增或重建 `review-evidence-sufficiency`。
- [x] 移除 support / contradiction / claim_true / claim_false / readiness score 默认输出。

验收：

- [x] 事实核验 helper 必须要求显式核验问题、地理范围、研究期、证据匹配窗口、传播/滞后假设、metric/source 要求。
- [x] evidence sufficiency review 默认输出 notes，不输出 numeric readiness score。
- [x] 旧 coverage/link 测试改写为 scope/sufficiency/caveat 测试。

### Optional-analysis 2 Environment evidence aggregation

- [x] 将 `extract-observation-candidates`、`merge-observation-candidates`、`derive-observation-scope` 收敛为 `aggregate-environment-evidence`。
- [x] 删除 `observation candidate / merged observation / usable_for_matching` 主语义。
- [x] 输出 statistics summary、spatial distribution、temporal distribution、metric distribution、source distribution、coverage limitations、metadata tags。
- [x] 保留 source signal ids、provider/source_skill、artifact refs、record locator。

验收：

- [x] 不再输出 `usable_for_matching`。
- [x] 不再建议 `link-claims-to-observations` 或 `score-evidence-coverage`。
- [x] 聚合方法、bucket、source distribution 和 caveats 可从 DB 复原。

### Optional-analysis 3 Discourse discovery 与 evidence-lane advisory

- [x] 将 `extract-claim-candidates`、`cluster-claim-candidates`、`derive-claim-scope` 收敛为 `discover-discourse-issues`。
- [x] 将 `classify-claim-verifiability`、`route-verification-lane` 收敛为 `suggest-evidence-lanes`。
- [x] 删除 claim candidate / claim cluster / claim scope / route owner / matching readiness 主语义。
- [x] 输出 public discourse issue hints、text evidence snippets、source distribution、taxonomy labels、mentioned_scope_metadata、coverage caveats。

验收：

- [x] discovery 输出不得称为事实 claim。
- [x] `mentioned_*` 不得被提升为 study scope。
- [x] evidence-lane suggestion 不得驱动 workflow、source queue 或 phase。

### Optional-analysis 4 Research issue surface 与 typed projections

- [x] 将旧 `extract-issue-candidates` / `cluster-issue-candidates` 重建为 `materialize-research-issue-surface`；删除旧入口，不保留 discovery alias。
- [ ] research issue surface 只消费 mission/moderator question、approved discovery、formal typed signals、environment summaries、findings、evidence bundles、challenger objections。
- [x] typed issue skills 收敛为 `project-research-issue-views`。
- [x] `materialize-controversy-map` 收敛为 `export-research-issue-map`。

验收：

- [x] 不得从未批准 claim/scope/route fallback 生成 issue。
- [x] typed projections 不重新编码证据。
- [x] controversy map 禁止 inline fallback，缺输入时报告 missing inputs。

### Optional-analysis 5 Formal/Public footprint 与 taxonomy

- [ ] 将 `formal_signal_semantics.py` 拆为 approved taxonomy family records。
- [x] 将 `link-formal-comments-to-public-discourse` 重建为 `compare-formal-public-footprints`。
- [x] 删除 `aligned`、alignment score、claim-side-only、claim support bonus。
- [x] 输出 formal footprint、public footprint、overlap status、descriptive balance metrics、coverage caveats。

验收：

- [x] footprint comparison 不表示观点一致或代表性充分。
- [x] submitter type 只作为 candidate label，并保留 metadata/text basis。
- [x] taxonomy 未审批时不能输出强标签。

### Optional-analysis 6 Representation 与 temporal co-occurrence cues

- [x] 将 `identify-representation-gaps` 重建为 `identify-representation-audit-cues`。
- [x] 将 `detect-cross-platform-diffusion` 重建为 `detect-temporal-cooccurrence-cues`。
- [x] 删除 gap/severity/influence/causality/spillover 确定语义。
- [x] timestamp 缺失时输出 insufficient temporal basis，不得 fallback 到 1970。

验收：

- [x] representation cue 不输出已证实 underrepresentation。
- [x] temporal cue 不输出传播方向已确定。
- [x] 报告正文默认不消费 cue，除非 report basis 明确引用。

### Optional-analysis 7 Audit records、docs、tests 收口

- [ ] 将本文件第 `8` 节 freeze-line placeholder rows 替换为最终 versioned audit records。
- [x] 更新所有触达 skill 的 `SKILL.md`、agent prompts、runtime registry description。
- [x] 重写依赖 coverage/linkage/route/map 的旧测试。
- [x] 增加 approval-gated helper、旧入口移除、DB-only recovery、report basis 引用测试。
- [x] 更新本文件的完成状态、未完成项、新发现问题、是否影响后续计划。

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

optional-analysis helper 必须支持 challenger 对以下内容提交 review comment / challenge：

1. scope：地理范围、研究期、证据匹配窗口、传播/滞后假设。
2. taxonomy：issue / concern / actor / citation / submitter type 是否过窄或带结论导向。
3. rubric：evidence sufficiency rubric 是否经过批准，是否隐藏权重。
4. source coverage：平台/API/docket/source family 覆盖是否足以支撑报告用途。
5. aggregation：bucket、统计摘要、source mixing 是否抹平异质性。
6. framing：helper 是否把线索包装成结论。
7. report usage：helper cue 是否被错误放入正文结论。

## 7. 后续工作基准

后续 optional-analysis helper governance 工作只以本文件为基准。若代码实施中需要调整命名或兼容策略，必须在最终交付时回写本文件，并说明：

1. 已完成。
2. 未完成。
3. 新发现的问题。
4. 是否影响后续计划。
5. 实际运行的最小测试集合与结果。

## 7.1 2026-04-28 本批交付（optional-analysis skills 无兼容收口）

已完成：

1. 新增共享实现 `eco_council_runtime.optional_analysis_helpers`，统一 optional-analysis helper governance、DB signal 查询、artifact refs、lineage、provenance、safe board handoff 与命名规则。
2. 新增 successor skills：`aggregate-environment-evidence`、`review-fact-check-evidence-scope`、`review-evidence-sufficiency`、`discover-discourse-issues`、`suggest-evidence-lanes`、`materialize-research-issue-surface`、`project-research-issue-views`、`export-research-issue-map`、`apply-approved-formal-public-taxonomy`、`compare-formal-public-footprints`、`identify-representation-audit-cues`、`detect-temporal-cooccurrence-cues`。
3. 删除 20 个旧 skill 目录和脚本：observation、claim、issue、typed issue、controversy map、formal/public link、representation gap、diffusion、link、coverage 系列均不再作为可执行 skill 存在。
4. 删除不再有调用者的旧 issue / typed issue runner 与旧 controversy issue surface 构造模块，避免继续保留旧启发式引擎。
5. `kernel/skill_registry.py` 与 `source_queue_profile.py` 只登记 successor / advisory helpers；所有 optional-analysis helper 仍为 approval-gated optional-analysis，不进入默认 source queue chain。
6. `analysis_objects.py` 与 `skill_registry.py` 已移除旧 helper 决策源；允许的 optional-analysis helper governance `decision_source` 只剩 `approved-helper-view`、`manual-or-moderator-defined`、`agent-submitted-finding`、`scenario`。
7. `analysis_plane.py` 中残留的 `default_source_skill` 已全部指向 successor skill 名称，不再指向已删除旧 skill。
8. 清理 `board_handoff.suggested_next_skills / recommended_next_skills / withheld_next_skills` 中残留的旧 skill 名称。
9. 更新 focused tests：旧 skill 在 registry 与脚本路径上都必须不存在；source queue profile 对旧 skill 名称也必须不存在。

未完成：

1. `formal_signal_semantics.py` 尚未物理拆分为 versioned taxonomy family records；当前由 `apply-approved-formal-public-taxonomy` 承接显式 approved taxonomy 输入。
2. 全仓历史 workflow 测试仍有大量旧脚本直接调用，需要逐步改写为 successor helper 与 DB council-object 承接模式；这些测试不应通过恢复旧入口解决。
3. 所有 optional-analysis 的 item-level `evidence_refs / lineage / provenance` 尚未逐一做全仓验收；本批覆盖 successor helpers 与当前 guardrail 路径。
4. `analysis_plane.py` 仍保留若干历史 analysis kind 名称用于 DB result-set 查询结构；本批已移除旧 skill 来源，但尚未重命名这些 DB/query object kind。

新发现的问题：

1. 多个历史 workflow 测试仍直接调用已删除旧脚本；全量测试在完成测试迁移前会按预期暴露缺失入口。
2. `analysis_plane.py` 与部分 DB query 表面仍保留历史 analysis result kind 名称；它们不再指向旧 skill，但后续若要彻底消除 claim/route/coverage 命名债，需要另立 DB schema/query migration。
3. 旧测试 fixture 曾把 skill 链当作中间产物生成器；按 optional-analysis helper governance 原则，后续 fixture 应从 normalized DB signals、finding/evidence bundle/proposal/report basis 等 DB objects 构造输入。

是否影响后续计划：

1. 会影响所有依赖旧 skill 名称的外部调用、demo 和历史测试；失败是有意行为，不做兼容修复。
2. 不阻塞后续 successor helper 扩展，但 full regression 必须先完成旧 workflow 测试迁移。
3. 后续优先级应是：taxonomy family records、历史 workflow 测试迁移、analysis kind 命名迁移、report basis 显式引用链。

本批实际运行测试：

1. `PYTHONPATH=eco-concil-runtime/src .venv/bin/python - <<'PY' ... validate_skill_registry()`：通过；当前 registry 识别 `80` 个 skill、`24` 个需要 operator approval、`17` 个 optional-analysis skill。
2. `.venv/bin/python -m py_compile eco-concil-runtime/src/eco_council_runtime/optional_analysis_helpers.py eco-concil-runtime/src/eco_council_runtime/analysis_objects.py eco-concil-runtime/src/eco_council_runtime/kernel/skill_registry.py eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_profile.py eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py tests/_workflow_support.py tests/test_optional_analysis_guardrails.py tests/test_runtime_source_queue_profiles.py tests/test_skill_approval_workflow.py`：通过。
3. `.venv/bin/python -m unittest tests.test_optional_analysis_guardrails tests.test_skill_approval_workflow tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate`：21 项通过。

## 7.2 2026-04-28 本批交付（历史 workflow 测试迁移与旧入口清零）

已完成：

1. `tests/_workflow_support.py` 新增 successor-helper 测试辅助：从 `research_issue_surface` 取 research issue id、从 successor artifacts 取 evidence ref，并用 `submit-council-proposal` / `submit-readiness-opinion` / `summarize-round-readiness` 构造 DB council-object 就绪依据。
2. 迁移 reporting / publication / decision trace / archive / benchmark / orchestration planner / orchestration ingress / supervisor / runtime kernel / board / investigation / council autonomy 测试，不再用旧 coverage、claim cluster、route 或 controversy map 脚本作为 fixture 生成器。
3. 重写 `test_analysis_workflow.py`、`test_formal_public_workflow.py`、`test_diffusion_workflow.py`、`test_controversy_workflow.py`、`test_deliberation_agenda_workflow.py`：测试目标改为 discourse issue discovery、environment aggregation、evidence lane advisory、research issue surface/view/map、formal/public footprint、representation audit cues、temporal co-occurrence cues。
4. `tests` 中对旧 skill 的 `script_path(...)` 直接调用已清零；旧 skill 名称只保留在 guardrail/negative assertion 中，用于证明 registry、source queue 和 board handoff 不再包含旧入口。
5. 历史 DB query fallback 测试改为显式断言“不做 inline fallback”：没有旧 analysis result-set 时返回空结果，而不是静默重建 claim cluster / controversy map。
6. readiness / report basis 测试改为 council judgement basis：没有旧 coverage artifact 时，report basis 不再伪造 selected coverage；ready/frozen 必须来自 DB council proposal + readiness opinion + moderator transition。

未完成：

1. `formal_signal_semantics.py` 仍未拆成 versioned taxonomy family records。
2. `analysis_plane.py` 仍保留历史 analysis kind/query object 命名，用于旧 DB 查询表面；本批只清理旧 skill 入口和测试 fixture，不做 DB schema / query kind 重命名。
3. 本批未运行全仓所有测试；只运行了 optional-analysis helper governance 旧入口迁移直接相关的最小集合。

新发现的问题：

1. 无旧 coverage 时，`propose-next-actions` 对干净 ready board 不再生成 heuristic action，这是期望行为；相关测试已改为断言 action count 为 `0`。
2. `freeze-report-basis` 在 council proposal / readiness opinion 明确支持时使用 `council-judgement-freeze-v1`，不会生成 `selected_coverages` 或 `selected_basis_object_ids`；测试已改为检查 supporting proposal/opinion ids。
3. `run-phase2-round` / `supervise-round` 在 approved transition 路径下不再物化默认 `orchestration_plan` 文件；controller/supervisor 只记录 expected path，并以 `transition-executor` 模式执行。
4. 报告 handoff 在缺少旧 coverage result-set 时会记录 `missing-coverage`，key findings 可能为空；这符合“helper 不能直接成为报告 basis”的原则。

是否影响后续计划：

1. 正向影响：旧入口、旧 fixture 生成器和旧 coverage/readiness 语义已经从测试主路径移除，后续 optional-analysis helper governance 开发不会被兼容测试拖回旧架构。
2. 后续若迁移 `analysis_plane` 历史 kind 名称，需要单独设计 DB query/schema 兼容或迁移策略；不应通过恢复旧 skill 实现。
3. 后续 full regression 若失败，应优先判断是否仍有历史 analysis object 命名债，而不是恢复 claim matching / coverage formula。

本批实际运行测试：

1. `.venv/bin/python -m unittest tests.test_reporting_publish_workflow tests.test_decision_trace_workflow tests.test_reporting_query_surface tests.test_archive_history_workflow tests.test_benchmark_replay_workflow tests.test_orchestration_planner_workflow`：39 项通过。
2. `.venv/bin/python -m unittest tests.test_analysis_workflow tests.test_formal_public_workflow tests.test_diffusion_workflow tests.test_controversy_workflow tests.test_deliberation_agenda_workflow`：11 项通过。
3. `.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_investigation_workflow tests.test_board_workflow tests.test_council_autonomy_flow tests.test_supervisor_simulation_regression tests.test_orchestration_ingress_workflow`：41 项通过。
4. `.venv/bin/python -m unittest tests.test_runtime_kernel`：42 项通过。
5. `.venv/bin/python -m unittest tests.test_optional_analysis_guardrails tests.test_skill_approval_workflow tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate`：21 项通过。
6. `.venv/bin/python -m py_compile ...` 覆盖本批修改的测试文件：通过。
7. `rg` 扫描 `tests` 中旧 skill 的 `script_path(...)` 直接调用：无匹配。
8. `git diff --check`：通过。

## 7.3 2026-04-28 针对性检阅修正（旧语义再入口收口）

已完成：

1. `phase2_fallback_context.py` 不再读取或消费 legacy coverage、claim-verifiability、verification-route、formal-public-link、representation-gap、diffusion-edge、controversy-map、typed issue analysis contexts；兼容字段保留为空，并在 `analysis_sync / observed_inputs` 标记 `legacy_optional_analysis_quarantined`。
2. `materialize-reporting-handoff` 不再把旧 `selected_coverages`、coverage score、claim readiness、verification route、formal/public link、representation gap、diffusion edge 转成 `key_findings`；若历史 report basis 中仍有这些行，只记录 `legacy-optional-analysis-reporting-basis-ignored` warning。
3. `suggest-evidence-lanes`、`materialize-research-issue-surface`、`project-research-issue-views`、`export-research-issue-map` 的 `input_path` 只接受带 optional-analysis helper governance 的 approved-helper-view artifact；裸 JSON 或不带规则元数据的 artifact 会被忽略并写入 `unapproved-input-artifact` warning。
4. `apply-approved-formal-public-taxonomy` 现在要求显式 `approval_ref`、`approval_ref` 字段或 `approved_taxonomy_ref`；只有 taxonomy 文件但没有具体审批引用时，状态为 `taxonomy-approval-required`，不输出 taxonomy labels。
5. 新增 guardrail 测试：未批准 issue hint artifact 在无 DB basis 时不能生成 research issue surface / views；taxonomy 文件缺审批引用时不能输出标签。

未完成：

1. `analysis_plane.py` 的历史 analysis kind / query object 命名仍未迁移；本批只阻断 phase-2 fallback 与 reporting handoff 消费旧对象。
2. `formal_signal_semantics.py` 仍未拆为 versioned taxonomy family records。
3. 当前 artifact gate 验证的是 optional-analysis helper governance 与 approval-gated 声明，不等价于完整 DB council object 承接；后续应把真实 skill approval request、finding/evidence bundle/report basis 引用写入 helper artifact 或统一从 DB 读取。

新发现的问题：

1. reporting workflow 仍读取 `analysis_sync.status` 兼容字段；本批以 `missing-coverage` 保留状态兼容，但不恢复 legacy coverage 读取。
2. 已有 successor helper 测试链会通过 helper artifact 串联 surface/views/map；该链必须带 optional-analysis governance metadata，后续若推进 DB-only，应改为通过 DB council object 或 approved research issue surface record 传递。
3. 报告 `key_findings` 现在不会由旧 helper basis 生成；后续需要通过明确的 finding / evidence bundle / report basis 设计提供正文 finding，而不是恢复 coverage finding。

是否影响后续计划：

1. 不阻塞后续 optional-analysis helper governance；相反，它把旧 claim/coverage/linkage 语义从 phase-2 与 reporting 再入口中隔离出来。
2. 后续优先级不变：taxonomy family records、analysis kind 命名迁移、helper artifact 到 DB council object 承接、report basis 显式引用链。
3. 若后续测试需要报告正文 finding，应补 DB-backed report basis fixture，不应重新依赖 `coverage_score` 或 `support_links / contradiction_links`。

本批实际运行测试：

1. `.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_optional_analysis_guardrails tests.test_formal_public_workflow tests.test_analysis_workflow tests.test_controversy_workflow`：20 项通过。
2. `.venv/bin/python -m py_compile eco-concil-runtime/src/eco_council_runtime/phase2_fallback_context.py eco-concil-runtime/src/eco_council_runtime/optional_analysis_helpers.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py tests/test_optional_analysis_guardrails.py tests/test_formal_public_workflow.py`：通过。
3. `git diff --check`：通过。

## 7.4 2026-04-29 decision-maker reporting pipeline track reporting packet 化对 optional-analysis helper governance 护栏的跟进

已完成：

1. `materialize-reporting-handoff` 现在输出 `evidence_packet / decision_packet / report_packet`，并把旧 helper cue 与 DB report basis 分开；helper cue 不能直接成为 final report finding。
2. `materialize-final-publication` 现在输出 `decision_maker_report / evidence_index / uncertainty_register / residual_disputes / policy_recommendations`，将 citation index 与 uncertainty 明确放入报告结构。
3. reporting skill 的 handoff suggested skills 已移除 `propose-next-actions / open-falsification-probe / post-board-note` 作为报告链默认后续动作，hold path 改为 finding / evidence bundle / proposal / readiness opinion 等 DB basis 写入面。

未完成：

1. reporting contract 仍保留 `coverage_source` 等历史 trace 字段；本批没有做 DB/query contract 改名迁移。
2. ready-round fixture 仍可能缺少 `finding-record / evidence-bundle / report-section-draft`，因此 `key_findings` 为空时只能输出 report-basis gap。

新发现的问题：

1. 如果不补 DB-backed report basis fixture，final publication 虽能生成结构化决策者报告，但正文 finding 与 recommendations 会保持保守，不能用旧 coverage/helper 结果填充。
2. optional-analysis helper artifact 到 DB council/reporting object 的承接仍是后续重点，尤其是 report section draft 与 evidence bundle 的显式引用链。

是否影响后续计划：

1. 不阻塞 optional-analysis helper governance 后续；它把 reporting 再入口中的 helper 直通风险进一步压低。
2. Policy research case fixture 应优先补 finding / evidence bundle / report-section-draft，而不是恢复旧 helper basis。

本批实际运行测试：

1. `.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface`：21 项通过。
2. `.venv/bin/python -m py_compile ...reporting 相关脚本与 canonical normalizer`：通过。
3. `.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate`：10 项通过。

## 7.5 2026-04-29 Policy research case fixture、回归与交付收口

已完成：

1. 新增 `tests/test_policy_research_case_fixtures.py`，覆盖政策争议、正式记录/舆情混合、可核实经验事件三类 case fixture。
2. 三类 fixture 均走 `fetch/import -> normalize -> query -> finding -> evidence bundle -> review/challenge -> transition request/approval -> report` 路径；其中 fetch 使用本地 detached fetch runner，避免引入外部网络依赖。
3. 新增 `submit_report_basis_records(...)` 测试辅助，显式提交 `finding-record / evidence-bundle / report-section-draft`，使 reporting handoff 与 final publication 的 key findings 来自 DB canonical report basis。
4. 修复 `materialize-reporting-handoff` 的 DB report basis 读取：内部以 canonical query kind `finding` 读取 finding rows，再在 evidence index 中以 `finding-record` report-basis role 暴露。
5. Policy research fixture 中 `summarize-round-readiness` 只通过 `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` 执行，并断言 consumption control object 存在。
6. 删除 reporting / report basis / supervisor 导出物后，policy research final publication 仍能从 DB 恢复 handoff、decision、role reports、report basis 和 supervisor state。
7. 补充 skill approval 不可复用回归：已消费的 optional-analysis approval request 会被 preflight 阻断，不能重复执行。
8. `scaffold-mission-run` source task 文案已从 claim/observation matching 口径改为 investigator query、finding、evidence-bundle 口径，避免 operator/agent runbook 继续暗示旧主链。

未完成：

1. `formal_signal_semantics.py` 仍未拆成 versioned taxonomy family records。
2. `analysis_plane.py` 历史 analysis kind / query object 命名仍未迁移。
3. `freeze-report-basis / report_basis_status` 等历史命名仍保留，尚未做 breaking CLI/schema 改名。
4. 本批未运行全仓所有测试；仅运行 policy research case fixture 和直接相关 reporting / approval / source queue / agent entry 最小集合。

新发现的问题：

1. `materialize-reporting-handoff` 此前查询 `finding-record`，但 council query surface 的 canonical kind 是 `finding`；这会造成 DB finding 不能进入 `key_findings`。本批已修复。
2. `scaffold-mission-run` 虽已不支配 phase-2 默认判断，但 task 文案仍会影响 operator/agent 理解；旧 claim/observation 文案应视为架构风险而不是普通措辞问题。
3. Policy research fixture 证明 final report finding 可以完全由 DB basis 承接；后续不需要、也不应恢复旧 coverage/helper basis 直通。
4. reporting-ready 判定仍需要 approved readiness summary；该 summary 必须保持为显式审批、一次性消费的 optional-analysis，而不是恢复默认主链。

是否影响后续计划：

1. 不阻塞后续整批交付；policy research targeted case fixture 已可作为最终验收入口。
2. 整批交付前应把 taxonomy family records、analysis kind 命名迁移、report basis 命名债、full regression 未跑完列为残留风险。
3. 后续新增 case 应沿用本批模式：helper cue 默认只进 audit/appendix，报告正文必须引用 DB `finding / evidence-bundle / report-section-draft / proposal / readiness` basis。

本批实际运行测试：

1. `.venv/bin/python -m py_compile tests/_workflow_support.py tests/test_policy_research_case_fixtures.py tests/test_skill_approval_workflow.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py skills/scaffold-mission-run/scripts/scaffold_mission_run.py`：通过。
2. `.venv/bin/python -m unittest tests.test_policy_research_case_fixtures -v`：1 项通过，覆盖 3 个 subTest case。
3. `.venv/bin/python -m unittest tests.test_skill_approval_workflow tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate -v`：15 项通过。
4. `.venv/bin/python -m unittest tests.test_source_queue_rebuild -v`：8 项通过。
5. `.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface -v`：21 项通过。
6. `git diff --check`：通过。

## 7.6 2026-04-29 final acceptance hardening track 最终验收硬化与残留项收口

已完成：

1. `analysis_plane.py` 已为所有 analysis kind 输出 `analysis_kind_governance`；高风险旧对象 `evidence-coverage / claim-observation-link / observation-candidate / merged-observation / formal-public-link / representation-gap / diffusion-edge` 被标记为 `legacy-frozen-compatibility-query-only`。
2. analysis query surface 即使没有返回 result set，也会暴露该 analysis kind 的治理元数据：不可进入默认链、不可作为 phase gate、不可作为 report basis、执行来源必须是 approval-gated helper。
3. `formal_signal_semantics.py` 已补 versioned taxonomy family records，覆盖 issue、concern、citation、stance、submitter type、route hint；所有 family 均带 `formal-public-taxonomy-freeze-2026-04-29`、approval/audit refs、candidate-only 与非 report/phase basis 标记。
4. `apply-approved-formal-public-taxonomy` 的 registry helper metadata 已同步 taxonomy freeze version，便于 operator 审计和后续迁移。
5. 默认 agent entry 已移除旧 analysis query commands；角色入口只保留 DB query、DB council/reporting write surface，以及 optional-analysis approval/run 模板。
6. `open-investigation-round` fallback task 已去掉 `claim-candidates / observation-candidates` 输出，并将历史 source task 中的旧 output kind 映射为 `public-discourse-evidence / environment-evidence`。
7. 新增 final guardrail 回归，覆盖 analysis kind freeze、taxonomy family records、agent entry 默认入口、open-round fallback 文案与输出。

未完成：

1. `freeze-report-basis / report_basis_status / report_basis_path` 等历史命名仍未做 breaking rename；继续作为非阻塞命名债。
2. `build-normalization-audit` 仍保留读取历史 claim/observation candidate result set 的兼容参数；它只属于 runtime-operator QA optional-analysis，仍需 approval，不进入默认主链。
3. 旧 canonical analysis contract 未物理删除；本批选择冻结 query/governance 语义，保留历史 replay/query 能力。
4. 未运行全仓所有测试；本批运行最终验收直接相关 targeted regression。

新发现的问题：

1. `open-investigation-round` fallback 仍会在缺少 source task artifact 时重建旧 output kind；这会污染后续 operator/agent runbook，本批已修复。
2. default agent entry 中列出旧 analysis kind 查询命令，会把 approval-gated helper surface 暗示为默认角色入口；本批已清空默认 analysis commands。
3. 对 analysis kind 的彻底重命名不是小补丁：它牵涉 DB replay、kernel query、benchmark artifact 与历史报告 trace，后续应单独做 breaking migration。

是否影响后续计划：

1. 不阻塞整批重构验收；policy research fixture 中列为阻塞风险的 taxonomy family records 与 analysis kind 边界已转成可测试的冻结/审计面。
2. 后续交付文档应把剩余项列为 schema/CLI/历史兼容迁移债，而不是默认主链或报告证据风险。
3. 若后续新增 helper 或 taxonomy family，必须继承本批字段：version、approval/audit refs、candidate-only、非 phase/report basis、DB council/reporting basis 才能进入报告正文。

本批实际运行测试：

1. `.venv/bin/python -m py_compile eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py eco-concil-runtime/src/eco_council_runtime/formal_signal_semantics.py eco-concil-runtime/src/eco_council_runtime/kernel/skill_registry.py eco-concil-runtime/src/eco_council_runtime/phase2_agent_entry_profile.py skills/open-investigation-round/scripts/open_investigation_round.py skills/scaffold-mission-run/scripts/scaffold_mission_run.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py tests/test_optional_analysis_guardrails.py tests/test_agent_entry_gate.py tests/test_board_workflow.py tests/test_policy_research_case_fixtures.py tests/test_skill_approval_workflow.py`：通过。
2. `.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails -v`：19 项通过。
3. `.venv/bin/python -m unittest tests.test_board_workflow.BoardWorkflowTests.test_open_investigation_round_preserves_prior_round_and_carries_state_from_db tests.test_board_workflow.BoardWorkflowTests.test_open_investigation_round_fallback_uses_shared_source_role_catalog tests.test_board_workflow.BoardWorkflowTests.test_open_investigation_round_reads_db_backed_actions_when_export_is_missing -v`：3 项通过。
4. `.venv/bin/python -m unittest tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_lists_no_legacy_claim_cluster_result_sets_after_successor_helpers tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_queries_no_legacy_claim_cluster_items_after_successor_helpers tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_does_not_inline_legacy_controversy_map_fallback tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_does_not_inline_legacy_issue_cluster_fallback tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_analysis_query_reports_invalid_analysis_kind -v`：5 项通过。
5. `.venv/bin/python -m unittest tests.test_policy_research_case_fixtures -v`：1 项通过，覆盖 3 个 subTest case。
6. `.venv/bin/python -m unittest tests.test_skill_approval_workflow tests.test_source_queue_rebuild -v`：13 项通过。
7. `.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface -v`：21 项通过。
8. `git diff --check`：通过。

## 7.7 2026-04-29 验收审阅回写

已完成：

1. optional-analysis 默认链退出目标通过验收：旧 claim/observation/link/coverage skill 不再是 registry active path，successor helper 均按 approval-gated optional-analysis 处理。
2. legacy analysis kind query surface 已带 `analysis_kind_governance`，明确 `default_chain_eligible=false / phase_gate_eligible=false / report_basis_eligible=false`。
3. formal/public taxonomy cue 已带 versioned family records、approval/audit refs、candidate-only、非 phase/report basis 标记。
4. 默认 agent entry 不再暴露旧 analysis query commands；open-round fallback 与 scaffold task 输出已改为 `public-discourse-evidence / environment-evidence`。
5. reporting handoff/final publication 已验证 helper/legacy cue 不直通报告正文；报告 finding 来自 DB `finding / evidence-bundle / report-section-draft` 等 basis。

未完成：

1. 第 `8` 节 freeze line 仍为 `audit-pending`，尚未替换为完整人工审计批准记录。
2. `build-normalization-audit` 仍保留历史 claim/observation candidate 兼容参数，但它是 operator QA optional-analysis，需要审批。
3. 旧 analysis contract 未物理删除；当前冻结为 compatibility query/replay surface。
4. 未运行全仓 discover。

新发现的问题：

1. 第 `4` 节仍有若干未勾选项，主要属于完整审计、物理拆分和破坏性迁移，不应再被理解为默认链阻塞。
2. `freeze-report-basis` 仍保留 legacy coverage helper 函数；当前 shared context 已 quarantine legacy optional-analysis analysis，reporting handoff 也忽略 legacy optional-analysis basis，但后续 breaking cleanup 应物理移除。

是否影响后续计划：

1. 不阻塞 optional-analysis hard acceptance；optional-analysis 默认链、报告 basis 和 agent entry 风险已经收口。
2. 后续应继续推进完整审计记录、schema/CLI rename、legacy contract 删除和全仓回归。

本次验收实际运行测试：

1. `.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_skill_approval_workflow tests.test_source_queue_rebuild tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface tests.test_runtime_kernel tests.test_board_workflow -v`：`111` 项通过。
2. `git diff --check`：通过。

## 7.8 2026-04-29 测试命名清理

已完成：

1. 测试模块从 work-package 编号命名改为功能命名：
   - `tests/test_optional_analysis_guardrails.py`
   - `tests/test_policy_research_case_fixtures.py`
2. 测试类、测试函数、共享 helper、fixture source/provenance 已改成 `optional-analysis / policy-research / successor-helper / research-issue` 等语义命名。
3. 文档中的测试模块路径和精确测试函数名已同步更新。

未完成：

1. 本节原先未覆盖 runtime contract breaking rename；该项已在 7.9 收口。

新发现的问题：

1. 旧测试 helper 名称会把 work package 编号误当成行为语义；后续新增测试应避免此类命名。

是否影响后续计划：

1. 不阻塞 optional-analysis hard acceptance。
2. runtime contract breaking rename 已在 7.9 统一回写。

本批实际运行测试：

1. `.venv/bin/python -m py_compile tests/_workflow_support.py tests/test_optional_analysis_guardrails.py tests/test_policy_research_case_fixtures.py tests/test_analysis_workflow.py tests/test_runtime_kernel.py tests/test_investigation_workflow.py tests/test_board_workflow.py tests/test_reporting_workflow.py tests/test_reporting_publish_workflow.py tests/test_reporting_query_surface.py tests/test_decision_trace_workflow.py tests/test_archive_history_workflow.py tests/test_benchmark_replay_workflow.py tests/test_council_autonomy_flow.py tests/test_orchestration_ingress_workflow.py tests/test_orchestration_planner_workflow.py tests/test_supervisor_simulation_regression.py`：通过。
2. `.venv/bin/python -m unittest tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_lists_no_legacy_claim_cluster_result_sets_after_successor_helpers tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_queries_no_legacy_claim_cluster_items_after_successor_helpers tests.test_analysis_workflow.AnalysisWorkflowTests.test_successor_analysis_chain_materializes_db_backed_surfaces tests.test_investigation_workflow -v`：`22` 项通过。
3. import smoke：`16` 个受影响测试模块可正常 import。
4. `git diff --check`：通过。

## 7.9 2026-04-29 runtime / skill / docs 命名同步

已完成：

1. 运行时代码从编号命名同步为功能命名：`eco_council_runtime.optional_analysis_helpers` 取代旧 helper 模块名，`helper_governance` 取代旧 artifact / policy metadata 字段，`helper_destination` 取代旧 destination 字段。
2. `analysis_objects.py` 与 `skill_registry.py` 的 helper decision source、freeze line、governance metadata 函数和 registry policy 字段已改为 optional-analysis 语义。
3. 所有 optional-analysis skill wrapper、agent 描述、schema version、warning code、rubric version 与测试断言已同步为功能命名。
4. 本文件已从旧编号 workplan 路径重命名为 `docs/openclaw-optional-analysis-skills-refactor-workplan.md`，并同步相关文档引用。

未完成：

1. 未做全仓 discover；本批只运行命名同步直接相关的 targeted regression。
2. 旧 analysis kind / report basis contract 的业务语义迁移仍是独立 schema/CLI 迁移项，不在本次命名同步范围内。

新发现的问题：

1. runtime contract 字段此前仍保留编号历史，这会继续把 optional-analysis governance 误读为阶段编号语义；本批已做 breaking rename。
2. 历史验收文本中有较多阶段编号表述，已改为功能轨道名；后续新增文档不得再用编号代替行为或能力命名。

是否影响后续计划：

1. 不影响当前重构验收目标；这次同步降低了测试、runtime API 和 skill 文档之间的命名偏差。
2. 外部若仍消费旧 helper module 或旧 artifact 字段，需要迁移到 `optional_analysis_helpers` 与 `helper_governance`。

本批实际运行测试：

1. `.venv/bin/python -m py_compile eco-concil-runtime/src/eco_council_runtime/analysis_objects.py eco-concil-runtime/src/eco_council_runtime/optional_analysis_helpers.py eco-concil-runtime/src/eco_council_runtime/kernel/skill_registry.py eco-concil-runtime/src/eco_council_runtime/phase2_fallback_context.py eco-concil-runtime/src/eco_council_runtime/formal_signal_semantics.py skills/aggregate-environment-evidence/scripts/aggregate_environment_evidence.py skills/review-fact-check-evidence-scope/scripts/review_fact_check_evidence_scope.py skills/discover-discourse-issues/scripts/discover_discourse_issues.py skills/suggest-evidence-lanes/scripts/suggest_evidence_lanes.py skills/materialize-research-issue-surface/scripts/materialize_research_issue_surface.py skills/project-research-issue-views/scripts/project_research_issue_views.py skills/export-research-issue-map/scripts/export_research_issue_map.py skills/apply-approved-formal-public-taxonomy/scripts/apply_approved_formal_public_taxonomy.py skills/compare-formal-public-footprints/scripts/compare_formal_public_footprints.py skills/identify-representation-audit-cues/scripts/identify_representation_audit_cues.py skills/detect-temporal-cooccurrence-cues/scripts/detect_temporal_cooccurrence_cues.py skills/review-evidence-sufficiency/scripts/review_evidence_sufficiency.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py tests/test_optional_analysis_guardrails.py tests/test_analysis_workflow.py tests/test_formal_public_workflow.py tests/test_policy_research_case_fixtures.py tests/test_skill_approval_workflow.py`：通过。
2. `.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_skill_approval_workflow tests.test_source_queue_rebuild tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface tests.test_runtime_kernel tests.test_board_workflow tests.test_analysis_workflow tests.test_formal_public_workflow -v`：`117` 项通过。
3. `rg` 扫描 阶段编号样式、旧 helper module 和旧 metadata 字段：无残留命中。
4. `git diff --check`：通过。

## 7.10 2026-04-29 残留风险收尾与全量验收

已完成：

1. `freeze-report-basis` 已成为公开 transition skill；旧 `freeze report basis-evidence-basis` 目录和脚本入口已移除。
2. `phase2_report_basis_resolution.py` 取代旧 report basis resolution 入口，report-basis judgement 字段与历史 `report_basis_*` 兼容字段并行写出。
3. `runtime-control-freeze` 已从 `report-basis-freeze` 中拆出，runtime/control query surface 不再和 deliberation report basis 共用 object kind。
4. `query_runtime_control_freeze_command` 与 `query_report_basis_freeze_command` 分别指向 control plane 和 council plane，避免 operator view 中同名覆盖。

未完成：

1. `report_basis_status / report_basis_gate_path / report-basis-gate` 仍是历史 DB/CLI/stage 兼容字段，不在本轮做破坏性 schema 迁移。
2. optional-analysis frozen legacy query kinds 仍保留 query-only 兼容面，后续删除必须带 DB replay 迁移计划。

新发现的问题：

1. runtime control freeze 与 report basis freeze 曾发生 canonical object kind 撞名，测试能通过但 registry 实际覆盖；本轮已补 `runtime-control-freeze` 并用 full discover 验证。
2. 缺失 archive master-plan 文档会让 dashboard/package repo-level tests 依赖过时 fixture；本轮已改为 synthetic docs regression。

是否影响后续计划：

1. 不阻塞当前验收；剩余风险已降级为 breaking migration 债务。
2. 后续新增 skill / query / report basis 逻辑必须明确区分 `report-basis-freeze` 与 `runtime-control-freeze`。

本批实际运行测试：

1. `.venv/bin/python -m unittest discover -s tests -v`：`235` 项通过，用时 `420.703s`。
2. 相关 targeted regression 覆盖 canonical contracts、control query surface、phase2 state surface、decision trace、investigation、reporting、publication、gate handler。

## 7.11 2026-04-29 report-basis gate 命名收口

已完成：

1. 默认 gate stage / handler 改为 `report-basis-gate`，并新增 `apply-report-basis-gate` operator command。
2. reporting handoff、decision draft、expert report、final publication 和 runtime/control surfaces 已输出 `report_basis_*` 字段；`report_basis_*` 只作为 DB/replay 兼容双写。
3. `runtime/report_basis_gate_<round>.json` 成为唯一默认 gate artifact；旧 promotion gate artifact fallback 已在 report-basis-only 收尾中删除。
4. control query 增加 `--report-basis-status`，避免 operator surface 必须使用旧 report basis wording。

未完成：

1. 该条已被 7.12 覆盖：旧 promotion DB/CLI/schema 命名已删除，当前只保留 `report_basis_*` 架构字段。
2. optional-analysis legacy query kinds 仍是 frozen compatibility surface，未物理删除。

新发现的问题：

1. 全量 discover 首轮暴露一个旧测试仍读取 `report_basis_gate_*.json`；已改为新 artifact path。
2. 中途补 alias 时误插入 `empty_round_state()` 的局部变量已修复。

是否影响后续计划：

1. 不阻塞。默认链和报告 basis 语义已按 report-basis gate 收口。
2. 后续仅剩显式 DB/schema/replay migration 类工作，不应再影响 investigator/helper governance 主线。

本批实际运行测试：

1. `.venv/bin/python -m unittest tests.test_phase2_gate_handlers tests.test_phase2_contracts tests.test_direct_council_advisory tests.test_orchestration_planner_workflow tests.test_control_query_surface tests.test_phase2_state_surfaces tests.test_runtime_kernel tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_supervisor_simulation_regression -v`：`84` 项通过。
2. `.venv/bin/python -m unittest discover -s tests -v`：`235` 项通过，用时 `216.605s`。

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

| rule_id | skill | current status | helper destination |
| --- | --- | --- | --- |
| `HEUR-NORMALIZATION-AUDIT-001` | `build-normalization-audit` | `default-frozen; approval-required; audit-pending` | operator QA export |
| `HEUR-ENV-AGGREGATE-001` | `aggregate-environment-evidence` | `default-frozen; approval-required; audit-pending` | DB-backed environment evidence aggregation helper |
| `HEUR-FACT-SCOPE-001` | `review-fact-check-evidence-scope` | `default-frozen; approval-required; audit-pending` | explicit fact-check scope review helper |
| `HEUR-DISCOURSE-DISCOVERY-001` | `discover-discourse-issues` | `default-frozen; approval-required; audit-pending` | DB-backed public/formal discourse issue hints |
| `HEUR-EVIDENCE-LANE-001` | `suggest-evidence-lanes` | `default-frozen; approval-required; audit-pending` | advisory evidence-lane tags |
| `HEUR-RESEARCH-ISSUE-SURFACE-001` | `materialize-research-issue-surface` | `default-frozen; approval-required; audit-pending` | candidate research issue surface helper |
| `HEUR-RESEARCH-ISSUE-PROJECTION-001` | `project-research-issue-views` | `default-frozen; approval-required; audit-pending` | typed research issue cue projections |
| `HEUR-RESEARCH-ISSUE-MAP-001` | `export-research-issue-map` | `default-frozen; approval-required; audit-pending` | research issue navigation export |
| `HEUR-TAXONOMY-APPLY-001` | `apply-approved-formal-public-taxonomy` | `default-frozen; approval-required; audit-pending` | approved formal/public taxonomy label cues |
| `HEUR-FORMAL-PUBLIC-FOOTPRINT-001` | `compare-formal-public-footprints` | `default-frozen; approval-required; audit-pending` | formal/public footprint comparison helper |
| `HEUR-REPRESENTATION-AUDIT-001` | `identify-representation-audit-cues` | `default-frozen; approval-required; audit-pending` | representation audit cue helper |
| `HEUR-TEMPORAL-COOCCURRENCE-001` | `detect-temporal-cooccurrence-cues` | `default-frozen; approval-required; audit-pending` | temporal co-occurrence cue helper |
| `HEUR-SUFFICIENCY-REVIEW-001` | `review-evidence-sufficiency` | `default-frozen; approval-required; audit-pending` | DB-backed sufficiency notes and caveats |
| `HEUR-AGENDA-001` | `plan-round-orchestration` | `default-frozen; approval-required; audit-pending` | approval-gated advisory helper |
| `HEUR-NEXT-ACTION-001` | `propose-next-actions` | `default-frozen; approval-required; audit-pending` | approval-gated advisory helper |
| `HEUR-PROBE-001` | `open-falsification-probe` | `default-frozen; approval-required; audit-pending` | challenger/moderator helper |
| `HEUR-READINESS-001` | `summarize-round-readiness` | `default-frozen; approval-required; audit-pending` | optional readiness opinion |

冻结线规则：

1. 上表 skill 不得出现在默认 agent entry recommendations 或 source-queue downstream hints。
2. 上表 skill 不得绕过已批准且未消费的 skill approval request 执行。
3. 上表输出在完成后续审计前只能视为 derived advisory helper surface。
4. 第 3 节列出的原 skill 只作为历史映射说明；它们不再是 active freeze-line rows，也不得重新作为 skill registry entry 出现。

## 7.12 2026-04-29 report-basis-only 破坏性收尾

- 已完成：
  - promotion 兼容命名已从 runtime、skills、tests、reporting/publication surface 中删除，当前 report-basis 链只使用 `report_basis_*`。
  - `promotion/` artifact 目录改为 `report_basis/`；旧 gate/field/status alias 不再保留。
  - optional-analysis 输出仍被隔离为 approval-gated helper，不会因本次命名收尾进入主链或报告 basis。

- 未完成：
  - optional-analysis helper 的逐项人工审计仍未完成，继续保留 `audit-pending` freeze line。

- 新发现的问题：
  - 旧文档段落中“兼容保留”的结论已过时；后续验收应以本节和 skills checklist 第 24 节为准。

- 是否影响后续计划：
  - 不阻塞；后续只需继续推进启发式审计，不再需要旧 promotion 兼容迁移。

- 本次实际运行：
  - `compileall` 通过。
  - 目标回归 `84` 项通过。
  - 全量 `unittest discover`：`235` 项通过，用时 `222.805s`。
