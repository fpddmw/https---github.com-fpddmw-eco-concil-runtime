# OpenClaw NYC Smoke Real-Case Run Diagnostics

## 中文摘要

本轮 run 的核心问题不是单一 bug，而是 mission 表述、证据 lane 编排、skill 触发规则、治理状态传播、报告门禁共同造成的系统性偏窄。

主要结论：

1. 当前 mission 实际是在调查「公开报道 + 本地环境观测是否足以形成受限报告」，不是在完整调查「纽约烟霾事件的异常、始源地、传输路径、影响和处置建议」。
2. `source_requests` 在运行中成为事实上的固定议程，导致 council 只围绕 GDELT、Open-Meteo 空气质量、Open-Meteo 历史天气展开，没有自动扩展到火点、烟羽、轨迹或健康/响应证据。
3. NASA FIRMS 火点能力在底层 source catalog 中存在，但没有被 mission/lane 编排激活；因此没有显式加拿大数据获取，也没有火点数据调用。
4. 时空关系 skill 已存在，但定位为可选分析；当前编排没有在「wildfire smoke / source / origin / transport」这类任务中自动触发，所以没有形成空间中心和时间中心转移的证据包。
5. 报告链路过早给出 `ready-for-release`：部分 canonical report 仍带有 stale blocker / stale supervisor note，final publication 对 section-level 的 `basis-required`、`needs-explicit-moderator-text` 没有形成硬阻断。
6. 治理层存在明确 bug 和契约缺口，包括 `runtime-operator` 曾被允许尝试执行 normalize、单一 investigator 曾能执行跨角色 fetch/normalize plan、dead letter 缺少关闭/解决命令、reporting contract 声明不完整、preflight 误解析 prose 造成 false missing input。

建议修复顺序：

1. 先固化本轮 run 为回归 fixture，避免修复时丢失真实失败样本。
2. 修 `normalize-fetch-execution` 角色边界、dead letter resolution、report readiness/supervisor 状态传播等硬 bug。
3. 加严 final publication 门禁，禁止 stale blocker、false open risk 和 section-level missing basis 进入 release。
4. 引入 mission-to-lane 编译：把 broad mission 自动展开为 receptor、fire-origin、smoke-plume、transport、impact、response、uncertainty lanes。
5. 后续只推进治理完整性；凡涉及证据权重、排除、排序、专业阈值或领域判断的规则，先与项目 owner 确认，不在 skill 中内置。

## 0. 修复进度快照（2026-05-08）

状态说明：

1. `done`: 已有代码修复和定向回归验证。
2. `partial`: 已修住本轮暴露的一部分失败面，但仍有架构或能力缺口。
3. `pending`: 仍未落地。
4. `deferred`: 建议保留为后续能力建设。

| 问题/阶段 | 当前状态 | 已落地修复 | 剩余验收点 |
| --- | --- | --- | --- |
| Config: split skill API key inheritance | done | `fetch-regulationsgov-comments`、`fetch-regulationsgov-comment-detail`、`fetch-openaq` 的 skill 文档改为加载真实 `assets/config.env`；配置检查已通过。 | 不暴露密钥；后续 CI 可加 no-config-env-doc-mismatch 检查。 |
| G-001 runtime-operator data-execution boundary | done | 已回滚“给 `runtime-operator` 补 normalize 能力”的错误方向：`runtime-operator` 不再拥有 `normalize` capability，`normalize-fetch-execution.allowed_roles` 移除 `runtime-operator`；新增测试确认 operator preflight 会被 `actor-role-not-allowed` 阻断。 | operator 只保留审批、准入、runtime/admin/archive/export 类职责；若未来引入独立 executor role，必须使用独立能力而非 investigator-facing `normalize`。 |
| G-006 role-owned fetch/normalize execution | done | `normalize-fetch-execution` 现在要求 `--actor-role` 或 kernel 注入的 `OPENCLAW_ACTOR_ROLE`，并只执行该 actor 对应 plan role 的 steps；执行结果和 detached fetch ledger 记录 `actor_role`/`resolved_actor_role`。`prepare-round` 输出 role-owned `suggested_next_skill_runs`，提示 social-investigator/public-discourse 与 environmental investigator 分别执行自己的 fetch/normalize。 | 下一步可把 controller/runbook 中的推荐命令进一步 UI 化，避免人工只运行其中一个 role slice。 |
| G-007 operator boundary audit | done | 检查 mission/source planning、data execution 和 reporting/content 主链路：`scaffold-mission-run`、`prepare-round`、`normalize-fetch-execution`、`materialize-reporting-handoff`、relation evidence packet、decision draft、expert report draft/publish、council decision publish、final publication 均不允许 `runtime-operator` 执行；controller 即使由 operator 监督启动，也按 stage `assigned_role_hint`/skill 默认角色执行内容 skill，不继承 operator 身份；新增测试冻结该边界。 | operator 仍可执行审批、准入、runtime admin、archive/export、read-only query；这些不是 moderator/investigator/report-editor 内容生成职责。 |
| G-002 dead-letter lifecycle | done | 新增 `resolve-dead-letter` 命令、访问策略、CLI 解析、ledger 事件和 health 刷新；测试覆盖 dead letter 关闭后 health 消警。 | 后续可补 superseded/accepted-risk 的更细状态。 |
| C-001 contract prose parsing | done | required input 解析不再把 `Optional:`/`Recommended:` 当硬要求；能从 prose 中提取 backtick identifier；测试覆盖 proposal 输入解析。 | 仍建议把 skill I/O contract 迁到机器可读 schema。 |
| C-002/C-003 reporting contract cleanup | partial | 去重 `report_basis`，移除 no-op condition，过滤 `db_path` 类 summary 噪声。 | 仍缺完整机器可读 reporting contract 和 contract fixture。 |
| R-001 false open risks | done | reporting handoff 不再把正向 gate reason / operator note 全部转成风险；仅在 readiness/supervisor 非 ready 时转为风险。 | 需要用真实 run fixture 防回归。 |
| R-002 arbitrary decision lead basis | done | council decision 不再直接拿 `key_findings[0]` 作为任意 lead basis；decision summary 只报告显式选中 finding 的数量；reporting handoff 只把 evidence refs 或 basis object ids 已被 freeze 显式选中的 finding 写入 `key_findings`。 | lead basis 只能来自 agent/council 显式 lead-basis 对象；没有显式 lead-basis 对象时，decision 只列 evidence coverage/count，不由 skill 排序或选择。 |
| R-003 role-specific expert reports | partial | expert report 增加基于 role 的 finding 过滤，减少 social-investigator 直接继承环境证据的问题。 | 仍需 role-specific synthesis 模板和 cross-role evidence 引用规则。 |
| R-004 draft/canonical/publication status split | done | `publish-expert-report` 不再把 draft 的 `ready-to-publish` 原样带入 canonical；ready draft 发布后为 `canonical-published`，hold draft 发布后为 `canonical-needs-more-evidence`；final publication gate 接受 canonical published 状态。 | 后续可把状态枚举抽到共享 reporting status module。 |
| R-005 report status propagation | done | expert report payload 写入 `readiness_status` 和 `supervisor_status`。 | 后续应与 publication gate 共用同一状态枚举。 |
| R-006 final publication hard gate | partial | final publication 增加 release blockers；readiness、handoff、open risk、supervisor、report status、required section 状态、unresolved challenger constraints、显式 report-claim/lead-basis structural violations 可阻断 release；空风险/不确定性不再被误判为 `basis-gap`，open risks 会进入 uncertainty rows。 | 仍需补 section 结构完整性和未显式 claim-tagged 正文的治理策略；不能让 publication skill 自行判断 recommendation 或 claim 的专业支撑强度。 |
| M-002/S-001/T-001 mission-to-lane trigger | partial | mission intent 可派生 receptor-air-quality、fire-origin、public-discourse、local-weather-context、spatiotemporal-relation-review、impact、response lanes；source selection 会把 FIRMS、Open-Meteo、GDELT 纳入候选；prepare-round 只建议 role-owned fetch/normalize，不再自动推荐 spatiotemporal relation helper。 | 自动 fetch 参数生成、候选源区发现、烟羽/轨迹数据源仍缺；这些候选 lane/source 只作为议会审阅脚手架，不能标为完整 source attribution。 |
| M-003 verification scope | done | scaffold 阶段写入轻量 `verification_scope`，只包含 receptor region、study window、mission-derived lane/source candidates、lag-window cue、显式 required evidence lanes/source skills；round task 和 scaffold summary 均携带该对象；`summarize-round-readiness` 只把显式 `required_source_skills` 缺失作为 source-import gate，mission-derived source 只记录为 `candidate_source_skills`。 | final report 的 claim 支持关系仍只能由 agent/council/report-section 对象显式表达；任何 excluded inference、source equivalence、lane blocker 必须由 agent/council 对象或确认规则显式给出。 |
| S-003 lane-aware source budget | partial | fetch planner 增加 `source_step_budget`：当 mission-derived lane source candidates 多于全局 `max_source_steps_per_round` 时，提升 effective budget 并写出 warning，避免候选 lane source 被全局步数压掉；readiness 只对显式 `required_source_skills` 做 required source import gate。硬编码公共/空气质量 source 等价类已回收，只有 `verification_scope.source_skill_equivalents` 显式声明时才允许替代。 | 仍需真正的 per-lane min/max budget、must-select lane 对象化；任何 source 等价规则需由 mission/agent/council 显式声明。 |
| G-003 readiness-before-freeze | done | `freeze-report-basis` 在没有 DB-backed readiness assessment 时返回 `blocked`，不写 withheld freeze artifact，也不提交 transition；handoff 建议先运行 `summarize-round-readiness`。同时修复 `publish-council-decision` 在缺 report-basis payload 时的空值崩溃。 | controller plan 仍应在更上游自动提示 readiness materialization。 |
| G-004 governance freshness | done | Controller 记录 adopted transition request；completed controller 遇到更新的 approved freeze-report-basis request 会自动 stale restart；supervisor 传播 adopted transition 字段；reporting handoff 比对 frozen report-basis 与 supervisor transition id，不一致时以 `stale-controller` 阻断 reporting-ready。 | 后续可把 freshness 检查推广到 close-round / open-next-round 等其它 transition kind。 |
| G-005 challenger follow-up | partial | `summarize-round-readiness` 现在读取 DB-backed `review-comment`；未关闭且带 `report_risk` 或 required follow-up evidence 的 challenger review comment 会把 readiness 从 `ready` 降为 `needs-more-data`，并建议 `open-followup-from-review-comment`、`open-challenge-ticket`、`claim-board-task`、`submit-readiness-opinion`。新增 `open-followup-from-review-comment`，可把严重 review comment 显式转成 challenge ticket 和 claimed board task。 | 后续若要把某类 challenger 评论自动转成某类专业 follow-up，必须先确认规则；当前只做结构化 follow-up 与 gate。 |
| G-008 challenger constraint disposition | partial | 新增 `challenger_constraints` runtime helper；`post-review-comment` 支持 `--constraint-disposition`；readiness 不再把 challenger readiness opinion 的引用当 waiver，只有显式 disposition comment 才能解除 unresolved blocker；freeze、reporting handoff、decision draft、final publication 传播 `challenger_constraints` / `basis_use_constraints`，unresolved constraints 阻断 freeze/reporting/release。新增最小显式 report-claim/lead-basis 结构：`submit-report-section-draft` 只消费 claim text、evidence refs、constraint links、basis-use/lead-basis 等必要字段，freeze 会阻断缺最低引用结构、缺 challenger disposition chain 或与 challenger basis-use constraint 冲突的显式 claim/lead basis。 | 仍不从正文自动抽取 claim；未显式 claim-tagged 的 report text 需要后续由 report-editor/agent/council 显式提交，不由 runtime 建模板。处置是 agent/council 对象，不由 skill 判断专业对错。 |
| G-009 challenger independent agent entry | partial | 复核发现第一轮 run 只有 `actor_role=challenger` 的对象，没有独立 challenger 外部 agent/session；根因是测试只覆盖 role/access policy 和 operator command template，没有断言 challenger role entry 自己具备 review/comment 工作入口。现已把 `post-review-comment`、`post-board-note` 暴露到 challenger role entry，并用测试锁定 challenger 可提交 review comment、counter-finding、evidence bundle、challenge/probe，但不能发起 phase transition。新增 preflight run `openclaw-realcase-nyc-smoke-20230607-preflight`，宽口径 mission 为调查 2023 New York City smoke event；entry gate 生成 6 个 role surface，其中 challenger 有 4 个 read commands、10 个 write commands、0 个 transition commands；外部 OpenClaw registry 已创建 `openclaw-realcase-nyc-smoke-20230607-preflight-challenger`。 | 本仓库仍只生成 agent-entry capability surface，不自动创建外部 agents；本次是显式 CLI provisioning。尚未启动 challenger agent turn，因此没有 session 文件，也没有进入调查执行。若要把 role roster provisioning 自动化，应单独实现并测试。 |
| R-007 explicit evidence selection integrity | done | 已回收自动 evidence expansion：`freeze-report-basis` 不再把 referenced evidence bundle 内全部 refs 自动加入 `selected_evidence_refs`，只把它们暴露为 `candidate_bundle_evidence_refs` 和 `unselected_candidate_bundle_evidence_refs`；`selected_evidence_refs` 仅保留 agent/council 显式 evidence refs，并消费带 evidence refs 的 DB `report-section-draft` 作为显式 agent 选择。 | 后续如需证据排除、权重、排序或选择理由字段，必须先确认规则口径；skill 只做显式性和 lineage 检查。 |
| R-008 evidence-bound recommendations | partial | reporting-ready handoff 不再把 generic reporting/audit actions 写成 `policy_recommendations`；没有证据绑定的 recommendations section 会保持 `not-in-scope`。`summarize-round-readiness` 现在只记录 `response-recommendation-boundary` 是否有带 evidence refs 的 DB report-section-draft，不再因为缺失该 section 自动降级 readiness。 | 是否要求 recommendations lane 阻断 release 属于规则口径，需确认后再做；当前只记录显式证据存在性。 |
| C-004 shared minimal schema contract layer | done | 在现有 `eco_council_runtime.contracts` 层新增共享 `ContractFieldGroup`：object identity、evidence/lineage、provenance、governance target、basis linkage、challenger constraint state、report claim linkage；`CanonicalContract` 支持 optional text/list/dict/number/bool fields 和 `field_groups`，`validate_canonical_payload` 会校验 optional list/dict/number/bool 的结构类型；report-section-draft、review-comment、readiness-assessment、report-basis-freeze、reporting handoff、council decision、expert/final publication 均挂接相关最小治理字段。 | 这不是每个 skill 的独立 schema，也不表达“证据支持什么 claim”；它只描述可机器校验的共享治理/引用结构。skill I/O contract 若后续迁移，应复用该层而非另起模板。 |
| T-002 smoke/transport model capability | deferred | 当前不自动触发 transport/smoke helper；只保留候选 lane/source 与显式 follow-up/governance 对象。 | NOAA HMS smoke、trajectory/lag cue、transport alternatives 等能力属于专业 skill/规则，后续需先确认再实现或接入。 |
| T-003 required lane evidence review | done | 已回收 `specialist_method_gate`：`summarize-round-readiness` 改为 `required_lane_evidence_review`，且只评估 lane 内显式 `evidence_requirements`；没有显式 requirement 的 lane 记为 `not-evaluated`。缺少 relation packet 不再自动降级 readiness，也不再把 relation helper 注入 top-level recommended skills。 | 是否把某 lane 缺 evidence 作为硬 blocker，必须由 agent/council 明确对象、review comment、readiness opinion 或项目确认规则表达。 |
| Phase 0 regression fixture | done | 新增 `tests/fixtures/openclaw-realcase-nyc-smoke-phase0.json` 和 `tests.test_realcase_nyc_smoke_phase0_fixture`，以离线方式冻结本轮 run 的 mission/source/execution/reporting 关键问题签名。 | 后续真实重跑仍需更宽 mission 和 source/transport 能力。 |

当前判断：

1. 已完成的多为治理硬 bug、报告门禁 bug、配置 bug，以及 mission-to-lane 的第一层编排。
2. 仍不能宣布“真实烟霾调查流程完整可用”，因为 source-origin 目前只是候选 source/lane 编排加显式 required-source gate，transport/plume 的数据能力仍未闭环。
3. 已回收“专业/非专业 skill”分类和 lane 缺证据自动硬阻断；skill 只记录显式证据、候选缺口、lineage 和 open blocker。
4. 下一阶段只做治理完整性：显式证据选择、逐声明 report-claim structural gate、final publication 硬门禁和机器可读 contract；证据权重、排序、source 等价、lane blocker 等规则需先确认。

### 0.1 修正计划复核：需要回收或改写的越权点

本次复核结论：此前修正计划中仍混入了一些会让 skill 替 agent/council 做判断的表述，需要回收。

1. `R-002` 的“语义排序 / evidence strength ranking”已回收。正确做法是只接受 agent/council 显式 lead-basis 选择、显式顺序或显式 priority；没有这些对象时，decision 不生成 lead basis，只列 evidence coverage/count。
2. “证据可支持什么 claim”不能由 fetch、normalize、analysis/helper、reporting skill 判断。正确做法是 agent/council 提交 claim/finding/proposal/report-section 对象，显式写出 claim text 与 evidence_refs；治理 skill 只检查最低结构引用链和未解决 challenger 约束。
3. `G-005` 的 challenger caveat 处理不够强，且“引用 caveat 即 waiver”不成立。引用只表示 council 知道该问题；是否接受为 limitation、是否要求 follow-up、是否排除证据，必须由显式 disposition 对象表达。
4. mission-derived lane/source 只能作为候选脚手架；除非 mission、agent/council 对象或确认规则显式声明 required，否则不能自动把缺失 lane/source 变成 readiness/freeze/release blocker。
5. `R-006` final publication gate 应检查结构完整性、状态 freshness、section 状态、unresolved challenger constraints、显式 claim/basis-use constraints；不检查 claim 是否“专业上充分”或 recommendation 是否“实质正确”。
6. recommendation 的 evidence-bound 含义应改为“有显式 agent/council recommendation/section 对象和 evidence refs”，不是由 reporting skill 从证据中推导处理建议。

最新验收记录：

1. 2026-05-07: `tests.test_runtime_kernel` 通过，覆盖 controller transition freshness 重启和既有 runtime kernel 回归。
2. 2026-05-07: `tests.test_reporting_workflow` 通过，覆盖 stale supervisor handoff 阻断和 reporting handoff/decision 主路径。
3. 2026-05-07: `tests.test_reporting_publish_workflow`、`tests.test_decision_trace_workflow`、`tests.test_investigation_workflow`、`tests.test_reporting_query_surface`、`tests.test_control_query_surface` 通过。
4. 2026-05-07: `tests.test_source_queue_governance`、`tests.test_orchestration_ingress_workflow`、`tests.test_db_only_recovery` 通过。
5. 2026-05-07: 修改文件语法检查和 `git diff --check` 通过。
6. 2026-05-07: `tests.test_council_autonomy_flow` 通过，覆盖 report-risk review comment 阻断 readiness；后续第 32 条已把 waiver 语义改为显式 constraint disposition。
7. 2026-05-07: `tests.test_council_submission_workflow`、`tests.test_spatiotemporal_relation_taxonomy` 通过，确认 review-comment 写入与 relation objection 字段未回归。
8. 2026-05-07: `tests.test_council_autonomy_flow.CouncilAutonomyFlowTests.test_open_followup_from_review_comment_creates_challenge_and_task` 通过，覆盖严重 review comment 到 challenge/task 的显式 follow-up。
9. 2026-05-07: `tests.test_runtime_source_queue_profiles`、`tests.test_optional_analysis_guardrails` 和 runtime skill policy capability 检查通过，确认新增 skill 注册、角色能力和 source-queue profile 未破坏全局契约。
10. 2026-05-07: follow-up skill 合入后重跑 `tests.test_council_submission_workflow`、`tests.test_spatiotemporal_relation_taxonomy`，24 tests OK。
11. 2026-05-07: follow-up skill 合入后重跑 `tests.test_investigation_workflow`、`tests.test_reporting_workflow`、`tests.test_decision_trace_workflow`，20 tests OK。
12. 2026-05-07: `tests.test_realcase_nyc_smoke_phase0_fixture` 通过，5 tests OK；确认 NYC smoke Phase 0 fixture 可离线锁定原始问题签名。
13. 2026-05-07: `tests.test_council_autonomy_flow.CouncilAutonomyFlowTests.test_verification_scope_required_sources_hold_ready_opinion` 通过，确认 verification scope 缺失 required source import 时会覆盖 ready opinion。
14. 2026-05-07: `tests.test_policy_research_case_fixtures` 通过，确认 final publication 空风险语义未误伤通用 policy fixture。注：后续已回收硬编码 source 等价类，改为显式 scope 配置。
15. 2026-05-07: 重跑 `tests.test_reporting_publish_workflow`、`tests.test_reporting_workflow`、`tests.test_orchestration_ingress_workflow`、`tests.test_runtime_source_queue_profiles`、`tests.test_source_queue_governance`，34 tests OK。
16. 2026-05-07: required-source readiness gate 合入后重跑 `tests.test_council_submission_workflow`、`tests.test_spatiotemporal_relation_taxonomy`，24 tests OK；重跑 `tests.test_investigation_workflow`、`tests.test_decision_trace_workflow`，14 tests OK；`git diff --check` 通过。
17. 2026-05-07: R-007 合入后 `tests.test_policy_research_case_fixtures` 通过，确认 frozen report basis 会从 cross-plane evidence bundle 展开 public/formal/environment evidence refs。
18. 2026-05-07: R-007 合入后重跑 `tests.test_reporting_workflow`、`tests.test_decision_trace_workflow`、`tests.test_reporting_publish_workflow`，23 tests OK；重跑 `tests.test_council_submission_workflow`、`tests.test_council_autonomy_flow`，16 tests OK。
19. 2026-05-07: R-008 保守修复后重跑 `tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`，19 tests OK；确认未证据绑定的 generic reporting/audit actions 不再进入 policy recommendations。
20. 2026-05-08: 曾合入 `specialist_method_gate` 并通过 `tests.test_council_autonomy_flow`；后续复核认为“专业方法门槛”和 lane 缺证据自动阻断属于规则越权，已在第 24 条回收。同步保留 `submit-report-section-draft` CLI 返回空 `canonical_ids` 的 ID 传播 bug 修复。
21. 2026-05-08: 重跑 `tests.test_realcase_nyc_smoke_phase0_fixture`、`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`，24 tests OK；`tests.test_orchestration_ingress_workflow`、`tests.test_source_queue_governance`、`tests.test_spatiotemporal_relation_taxonomy`，30 tests OK；`tests.test_reporting_query_surface`，4 tests OK；`git diff --check` 通过。`empirical-event` policy fixture 现按 response lane 要求提交带 evidence refs 的 recommendations section，不再绕过 response gate。
22. 2026-05-08: 按“skill 只提供证据，证据选择交给 agent/council”边界，回收 R-007 的自动 bundle evidence expansion。`freeze-report-basis` 现在记录候选 bundle refs 和未显式选择 refs，但不自动加入 selected evidence；同时把带 evidence refs 的 DB `report-section-draft` 作为显式 agent 证据选择输入。`tests.test_policy_research_case_fixtures.PolicyResearchCaseFixtureTests.test_policy_research_cases_generate_db_backed_decision_reports` 通过；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`，18 tests OK。
23. 2026-05-08: R-007 边界回收后重跑 `tests.test_realcase_nyc_smoke_phase0_fixture`、`tests.test_council_autonomy_flow`，19 tests OK；`tests.test_orchestration_ingress_workflow`、`tests.test_source_queue_governance`，12 tests OK；`git diff --check` 通过。
24. 2026-05-08: 复核此前修复的越权点：回收 `specialist_method_gate`，改为非阻断的 `required_lane_evidence_review`；回收硬编码 source skill 等价类，默认精确匹配 required source，只有 `verification_scope.source_skill_equivalents` 显式声明时才替代；`derive_verification_scope` 优先尊重 mission 中显式 `source_selections`。`tests.test_council_autonomy_flow` 和 policy fixture 目标测试通过；`tests.test_source_queue_governance`、`tests.test_orchestration_ingress_workflow`，12 tests OK。
25. 2026-05-08: 越权回收后重跑 `tests.test_realcase_nyc_smoke_phase0_fixture`、`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`，24 tests OK；`tests.test_spatiotemporal_relation_taxonomy`、`tests.test_reporting_query_surface`，22 tests OK；`git diff --check` 通过。
26. 2026-05-08: 继续回收残留规则语义：mission-derived source 只进入 `candidate_source_skills`，不再自动写成 `required_source_skills`；`required_lane_evidence_review` 只评估显式 `evidence_requirements`；source queue lane summary 改为候选审阅表述，避免把脚手架写成归因规则。`tests.test_source_queue_governance`、`tests.test_orchestration_ingress_workflow`、`tests.test_council_autonomy_flow`，27 tests OK。
27. 2026-05-08: 第 26 条后续回归：`tests.test_realcase_nyc_smoke_phase0_fixture`、`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`，24 tests OK；`tests.test_spatiotemporal_relation_taxonomy`、`tests.test_reporting_query_surface`，22 tests OK；`git diff --check` 与 `git diff --cached --check` 均通过。
28. 2026-05-08: 修正 G-001 错误修复方向：原 run 中 `runtime-operator` 执行 normalize 被阻断是正确行为；现已移除 operator 的 normalize 权限，并把 `normalize-fetch-execution` 改为 role-owned fanout；同时冻结 mission/source planning 和 downstream reporting/content skills 不允许 operator 执行。`tests.test_runtime_kernel.RuntimeKernelTests.test_runtime_operator_cannot_run_fetch_normalize_bridge`、`tests.test_runtime_kernel.RuntimeKernelTests.test_runtime_operator_cannot_run_reporting_content_skills`、`tests.test_runtime_kernel.RuntimeKernelTests.test_runtime_operator_cannot_run_mission_or_source_planning_skills`、`tests.test_runtime_kernel.RuntimeKernelTests.test_registered_skill_allowed_roles_have_required_capabilities`、`tests.test_runtime_source_queue_profiles`、`tests.test_agent_entry_gate`，14 tests OK；`tests.test_source_queue_rebuild`、`tests.test_migrated_source_runtime_integration`、`tests.test_orchestration_ingress_workflow`、`tests.test_policy_research_case_fixtures`、`tests.test_realcase_nyc_smoke_phase0_fixture`，26 tests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow` 和 4 个 runtime boundary 目标测试，22 tests OK。
29. 2026-05-08: 继续检查后续步骤是否有 operator 代跑内容 skill 的同类问题：operator 保留 `approve-*`、`supervise-*`、`apply-report-basis-gate`、runtime health/export/archive/query 等治理与只读/派生面；mission/source planning、fetch/normalize、reporting handoff、decision/report drafting、publish/final publication 均由 moderator/investigator/report-editor 执行。新增 controller dispatch 断言，确认 operator 启动 controller 时内容 stage 仍用 `moderator` 等 stage role。`tests.test_runtime_kernel.RuntimeKernelTests.test_runtime_operator_cannot_run_fetch_normalize_bridge`、`test_runtime_operator_cannot_run_reporting_content_skills`、`test_runtime_operator_cannot_run_mission_or_source_planning_skills`、`test_registered_skill_allowed_roles_have_required_capabilities`、`test_controller_forwards_execution_policy_and_records_it`，5 tests OK；`py_compile`、`git diff --check`、`git diff --cached --check` 通过。
30. 2026-05-08: 重跑 role-owned source queue 回归：`tests.test_runtime_source_queue_profiles`、`tests.test_source_queue_rebuild`，12 tests OK；`tests.test_migrated_source_runtime_integration`、`tests.test_orchestration_ingress_workflow`、`tests.test_policy_research_case_fixtures`，13 tests OK。
31. 2026-05-08: 复核此前修正计划中的越权风险：回收 R-002 的 semantic/evidence-strength ranking 计划，改为显式 lead-basis 结构；确认 claim 支持关系只能由 agent/council 对象表达；将 G-005 从 done 调整为 partial，并新增 G-008 challenger constraint disposition；将 Phase 3/4/5 与架构建议改写为候选 lane、显式 required、结构性 gate，不让 skill 判断 claim/recommendation 专业充分性。
32. 2026-05-08: 落地 G-008 第一阶段：新增 runtime `challenger_constraints` 结构模块；`post-review-comment` 增加 `--constraint-disposition`；`summarize-round-readiness` 移除“readiness opinion 引用即 waiver”，改为显式 disposition comment 才解除 unresolved constraint；`freeze-report-basis`、`materialize-reporting-handoff`、`draft-council-decision`、`materialize-final-publication` 传播 challenger/basis-use constraints，并让 unresolved constraint 阻断 freeze/reporting/release。`py_compile` 通过；`tests.test_council_autonomy_flow` 15 tests OK；`tests.test_policy_research_case_fixtures` 1 test/3 subtests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_reporting_query_surface`、`tests.test_realcase_nyc_smoke_phase0_fixture` 共 27 tests/18 subtests OK。
33. 2026-05-08: 落地显式 lead-basis 结构第一阶段：新增 runtime `report_claim_structure`，`submit-report-section-draft` 可写入 `claim_id`、`claim_text`、`basis_use`、`lead_basis`、`claim_constraint_ids` 与 `evidence_refs`；`freeze-report-basis` 会从显式 report-section-draft 识别 lead-basis 对象，并在其缺最低结构字段或与 `lead_basis_allowed=false` 的 challenger constraint 冲突时 withheld；handoff、decision、final publication 传播 `explicit_lead_basis_objects` 和 `lead_basis_constraint_violations`。新增测试覆盖 accepted limitation 解除 readiness 后仍禁止该对象作为 lead basis。`tests.test_council_autonomy_flow` 16 tests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures` 共 19 tests/3 subtests OK。
34. 2026-05-08: 扩展显式 report-claim 最小 structural contract：显式声明 claim 的 `report-section-draft` 只要求 `claim_text` 和 `evidence_refs`；若该 claim 使用了受 challenger constraint 限制的对象或 evidence ref，还必须在 `claim_constraint_ids` 中显式挂接 disposition chain。`freeze-report-basis` 对 `report_claim_structural_violations` withheld，handoff/decision/final publication 传播并阻断 release。新增测试覆盖“readiness ready 但显式 claim 缺 claim_text 时 freeze withheld”。`tests.test_council_autonomy_flow` 17 tests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`、`tests.test_reporting_query_surface`、`tests.test_realcase_nyc_smoke_phase0_fixture` 共 28 tests/21 subtests OK。
35. 2026-05-08: 回收第 34 条中仍偏厚的 report-claim/verification-scope 契约：移除 `claim_scope`、`claim_boundary`、`claim_limitations` CLI 字段和 structural/advisory 检查；`report_claim_structure` 不再传播 advisory missing fields；mission-derived `verification_scope` 不再写入 `claim_boundary_notes`、`excluded_inferences`、`reportable_claim_boundary`。后续 runtime 只保留必要 governance linkage，不提供报告模板字段。`tests.test_council_autonomy_flow` 18 tests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`、`tests.test_realcase_nyc_smoke_phase0_fixture` 共 24 tests/21 subtests OK；`tests.test_runtime_kernel`、`tests.test_source_queue_governance`、`tests.test_orchestration_ingress_workflow`、`tests.test_runtime_source_queue_profiles` 共 70 tests/9 subtests OK；`tests.test_canonical_contracts`、`tests.test_reporting_contracts` 9 tests OK；`py_compile`、`git diff --check`、`git diff --cached --check` 通过。
36. 2026-05-08: 继续回收自动议程引导：`summarize-round-readiness`、`prepare-round`、`plan-round-orchestration` 不再根据 relation/representation/diffusion gap 自动推荐 `query-spatiotemporal-relations`、`review-spatiotemporal-relation-alternatives`、`compare-formal-public-footprints`、`identify-representation-audit-cues`、`detect-temporal-cooccurrence-cues` 或 `suggest-evidence-lanes`。这些 gap 仍记录在 readiness/plan 的 counts、gate reasons 和 action objects 中；后续具体专业工具必须由 agent/council 显式 proposal、probe 或审批流程承载。`tests.test_spatiotemporal_relation_taxonomy`、`tests.test_council_autonomy_flow` 共 36 tests OK；`tests.test_orchestration_planner_workflow`、`tests.test_orchestration_ingress_workflow`、`tests.test_source_queue_governance`、`tests.test_runtime_source_queue_profiles` 共 24 tests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`、`tests.test_realcase_nyc_smoke_phase0_fixture` 共 24 tests/21 subtests OK；`py_compile`、`git diff --check`、`git diff --cached --check` 通过。
37. 2026-05-08: 完成 R-002 显式 key-finding/lead-basis 边界：`materialize-reporting-handoff` 只把与 frozen `selected_evidence_refs` 相交或位于 `selected_basis_object_ids` 的 finding 写入 `key_findings`；`draft-council-decision` 不再拼接前三条 finding 标题作为 evidence basis，只报告显式选中 finding 数量。`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures` 共 19 tests/3 subtests OK；`tests.test_realcase_nyc_smoke_phase0_fixture`、`tests.test_council_autonomy_flow`、`tests.test_spatiotemporal_relation_taxonomy` 共 41 tests/18 subtests OK；`py_compile`、`git diff --check`、`git diff --cached --check` 通过。
38. 2026-05-08: 继续回收 probe 内部自动专业工具选择：`open-falsification-probe` 的 `requested_skills` 不再根据 action kind 自动写入 query/analysis helper，只保留 `submit-council-proposal`、`submit-readiness-opinion`、`close-challenge-ticket`、`post-review-comment`、`open-challenge-ticket`、`update-hypothesis-status` 等治理动作；skill 文档和 agent prompt 同步改为“governance follow-up actions”。`tests.test_investigation_workflow`、`tests.test_spatiotemporal_relation_taxonomy`、`tests.test_council_autonomy_flow` 共 45 tests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_policy_research_case_fixtures`、`tests.test_realcase_nyc_smoke_phase0_fixture`、`tests.test_orchestration_planner_workflow`、`tests.test_orchestration_ingress_workflow`、`tests.test_source_queue_governance` 共 44 tests/21 subtests OK；`py_compile` 通过。
39. 2026-05-08: 落地共享最小 schema contract 层：扩展 `CanonicalContract`，新增 `ContractFieldGroup` registry 与 optional field 类型校验；report claim linkage、challenger constraint state、basis linkage 等治理字段进入共享 contract 元数据，不再散落为每个 skill 的独立约定。新增测试覆盖 field group 暴露和 malformed optional field 拦截。`python3 -m py_compile` 通过；`tests.test_canonical_contracts`、`tests.test_module_decomposition_contracts` 共 16 tests OK；`tests.test_reporting_workflow`、`tests.test_reporting_publish_workflow`、`tests.test_reporting_query_surface` 共 22 tests OK；`tests.test_council_autonomy_flow`、`tests.test_policy_research_case_fixtures`、`tests.test_realcase_nyc_smoke_phase0_fixture` 共 24 tests OK。
40. 2026-05-08: 修复 challenger 独立 entry 能力缺口：`DEFAULT_AGENT_ENTRY_ROLE_DEFINITIONS` 为 challenger 增加 `post-board-note`、`post-review-comment`；`default_role_entry_points` 为 `post-review-comment` 生成 challenger 自己的 kernel command，而非只依赖 operator template。新增回归断言 challenger role entry 包含 review comment、finding、evidence bundle、board note、challenge/probe，且不包含 phase transition；新增 kernel access 测试确认 challenger 可 `post-review-comment` 但不能 `request-phase-transition`。`python3 -m py_compile` 通过；`tests.test_agent_entry_gate` 6 tests OK；`tests.test_runtime_kernel.RuntimeKernelTests.test_challenger_can_review_but_not_transition`、`test_registered_skill_allowed_roles_have_required_capabilities` 2 tests OK；`tests.test_council_submission_workflow.CouncilSubmissionWorkflowTests.test_investigator_query_to_finding_bundle_proposal_and_review_loop` 1 test OK。另有一次错误测试名调用失败，已用正确测试名重跑通过。
41. 2026-05-08: 执行下一轮 run 的 preflight/provisioning 检查，不进入议会执行。新增 `runs/openclaw-realcase-nyc-smoke-20230607-preflight/input/mission-preflight.json`，mission objective 为调查 2023 New York City smoke event，不固定加拿大、传输路径或结论；`scaffold-mission-run --orchestration-mode openclaw-agent` 完成；`materialize-agent-entry-gate --actor-role runtime-operator` 完成，summary 为 `entry_status=ready`、`role_count=6`。只读检查 challenger entry：`read_command_count=4`、`write_command_count=10`、`transition_command_count=0`，且包含 `post-review-comment`、`open-falsification-probe`、`open-challenge-ticket`、`submit-finding-record`、`submit-evidence-bundle`。通过 `openclaw agents add` 显式创建外部 agent `openclaw-realcase-nyc-smoke-20230607-preflight-challenger`，workspace 为本 run 的 `supervisor/openclaw-workspaces/challenger`，identity 为 `Eco Council Challenger`；未启动 agent turn，所以未生成 session，也未触发 fetch/normalize/report。

### 0.2 Shared Minimal Schema Contract Layer

本次新增的 schema contract 层只服务三个目的：

1. 让 canonical object 的最低结构可机器验证，例如 `evidence_refs` 必须是 list、`lead_basis` 若出现必须是 bool、`report_claim_structure` 若出现必须是 dict。
2. 把跨 skill 重复出现的治理字段收敛为共享 field group，例如 `challenger-constraint-state`、`basis-linkage`、`report-claim-linkage`。
3. 让 query surface / contract list / tests 能看到对象支持哪些共享结构，减少 CLI、payload normalization、fixture 各写一套隐含契约。

明确不做的事：

1. 不为每个 skill 设计独立 schema。
2. 不判断证据是否足以支持 claim。
3. 不引入专业阈值、证据权重、证据排序或 source 等价规则。
4. 不把 reporting section 写成固定模板；agent/council/report-editor 仍需显式提交 claim text、evidence refs 和 constraint links。

### 0.3 Challenger Agent Coverage Gap

为何没有尽早发现：

1. 第一轮 run 的 ledger 中存在 `author_role=challenger` / `actor_role=challenger` 的 review comment，因此对象层看起来有 challenger 参与。
2. 当时测试覆盖了 role contract、skill/access policy、operator command template，但没有覆盖“外部独立 challenger agent/session 是否存在”和“challenger role entry 自己是否暴露 review command”。
3. `post-review-comment` 只在 operator command template 中出现；operator 可以用模板代填 `challenger`，这掩盖了 challenger 没有独立入口的问题。

当前边界：

1. challenger 是正式 role，不是 moderator 的附属状态；它有 query/analysis/finding/evidence-bundle/review/challenge/probe/readiness/proposal 等权限。
2. challenger 现在能从 role entry 中独立读取 public/formal/environment/board surfaces，提交 review comment、counter-finding、evidence bundle、board note，开启/关闭 challenge，申请 falsification probe。
3. challenger 不能发起 phase transition，也不能代替 moderator/requester 决定流程进入 freeze/report/release。
4. 本仓库目前只负责生成 agent-entry capability surface；外部 `.openclaw/agents/...-challenger` 的自动创建仍未实现。
5. 本次 preflight 已用 OpenClaw CLI 显式创建 challenger agent registry/workspace；这证明外部 agent 可落地，但不等于完整 run 会自动 provision 全 role roster。

## 1. Scope

This document reviews run `openclaw-realcase-nyc-smoke-20230607` after the first real-case council execution.

Run directory:

`runs/openclaw-realcase-nyc-smoke-20230607`

Primary final artifacts:

1. `reporting/final_publication_round-001.json`
2. `reporting/council_decision_round-001.json`
3. `reporting/expert_report_social_investigator_round-001.json`
4. `reporting/expert_report_environmental_investigator_round-001.json`
5. `report_basis/frozen_report_basis_round-001.json`
6. `runtime/audit_ledger.jsonl`
7. `runtime/runtime_health.json`

The run completed a governed path from mission scaffold to final publication. It also exposed several issues that should be fixed before treating the system as ready for unattended real-case evaluation.

## 2. Run Outcome Summary

The run produced a final publication with:

1. `publication_status=ready-for-release`
2. `publication_posture=release`
3. 3 selected evidence families in the broader evidence set:
   - GDELT public article signal.
   - Open-Meteo modelled PM2.5 signal.
   - Open-Meteo historical wind-speed context signal.
4. 2 canonical expert reports.
5. 1 canonical council decision.
6. 1 frozen report basis.

Runtime ledger summary:

1. 58 audit ledger events.
2. 22 `skill-execution` events.
3. 8 skill approval requests, all approved and consumed.
4. 2 transition approvals, both committed.
5. 3 findings, 1 evidence bundle, 1 challenger review.
6. 1 open dead letter remains.

The core report conclusion is bounded:

> NYC had local PM2.5 evidence, public-discourse evidence, and weather context in the selected time window. The run did not verify wildfire source origin, Canadian fire contribution, smoke-plume transport, or response recommendations beyond generic reporting/audit actions.

## 3. Highest-Level Diagnosis

The run succeeded as a narrow evidence-basis report. It failed as a full real-case investigation of a smoke event.

The primary cause is mission and orchestration framing:

1. The mission was written as a governed investigation over `public-report and environmental observation records`.
2. It did not ask the council to identify candidate source regions, smoke transport, health impact, or response options.
3. `prepare-round` did not infer additional evidence lanes from the issue text; it only planned from explicit `source_requests`.
4. `fetch-nasa-firms-fire` existed in the catalog, but was not requested and therefore was skipped.

This means the run naturally answered:

> Can the selected public and local environmental records support a bounded report?

It did not answer:

> What caused the NYC smoke episode, where did it originate, how did it move, who was affected, and what should be done?

## 4. Evidence From This Run

### 4.1 Mission Framing

Current mission objective:

> Run a governed council investigation over public-report and environmental observation records for the June 2023 New York City smoke episode, and produce a DB-backed report without pre-assigning causal conclusions or policy direction.

This is too narrow for a source/transport investigation.

Seeded hypothesis:

> Public-report records and environmental observations around the June 2023 New York City smoke episode may provide a bounded evidence basis for council reporting, subject to source limitations.

This hypothesis checks report-basis sufficiency. It does not ask the council to verify source origin or transport.

### 4.2 Sources Actually Selected

Actual source counts in `analytics/signal_plane.sqlite`:

1. `fetch-gdelt-doc-search`: 50 public signals.
2. `fetch-open-meteo-air-quality`: 288 environment signals.
3. `fetch-open-meteo-historical`: 291 environment signals.

`fetch-nasa-firms-fire` was available in the environmental source catalog but not selected:

1. `source_selection_environmental-investigator_round-001.json` marks `fetch-nasa-firms-fire` as `selected=false`.
2. `nasa-firms:active-fire` is explicitly skipped.

### 4.3 Selected Evidence Signals

Selected report evidence included:

1. PM2.5: `65.9 ug/m3`, `2023-06-07T17:00`, near NYC.
2. Wind speed: `21.3 km/h`, `2023-06-07T22:00`, near NYC.
3. GDELT article: `NY air quality : How asthma ER visits spiked amid wildfire smoke`, published `20230609T230000Z`.

These support a local episode description. They do not establish source origin or transport.

## 5. Issues And Root Causes

### M-001: Mission Was Too Narrow

Severity: high

Symptom:

The council did not request Canadian data, fire-origin data, smoke-plume data, trajectory evidence, health impact evidence, or response planning evidence.

Root cause:

The objective constrained the run to public-report and environmental observation records. It also explicitly avoided pre-assigning causal conclusions or policy direction. That is appropriate for avoiding overclaiming, but it also removed the investigation mandate for source attribution and recommendations.

Fix direction:

For this class of case, write the mission as:

> Investigate the June 2023 New York City smoke episode, identify pollution anomalies, candidate source regions, possible transport pathways, public impacts, uncertainties, and evidence-bounded response recommendations.

Add a separate prohibition:

> Ask the council to state source-attribution and transport-causality claim boundaries, including what evidence it used and what uncertainty remains.

### M-002: Source Requests Became The De Facto Agenda

Severity: high

Symptom:

Although the user requested no fixed agenda, the mission file carried three concrete `source_requests`. `prepare-round` planned only those sources.

Root cause:

`build_source_selection` first reads explicit source selections, then falls back to `infer_selected_sources(mission, role)`. That inference uses mission `artifact_imports` and `source_requests`; it does not expand from the investigation question.

Fix direction:

Split mission input into:

1. `seed_sources`: optional starting sources.
2. `required_evidence_lanes`: explicit verification lanes.
3. `source_requests`: direct operator-specified sources, only when the operator intends to constrain the source set.

Add a mission compiler that turns broad questions into evidence lanes before source selection.

### M-003: Verification Scope Was Missing

Severity: high

Symptom:

No object captured receptor, candidate source region, lag window, spatial review scope, transport evidence review needs, or report decision boundary.

Root cause:

The current scaffold only seeded a broad hypothesis and role tasks. It did not create a structured verification scope object.

Fix direction:

Add a `define-verification-scope` or `derive-investigation-lanes` step before source planning. It should create DB-backed objects such as:

1. `receptor_region`
2. `study_window`
3. `candidate_source_region_policy`
4. `lag_window`
5. `required_evidence_lanes`
6. optional project-rule references, only if explicitly supplied by agent/council or confirmed project rules

Implemented fix:

1. `scaffold-mission-run` writes a structured `verification_scope` into the mission artifact and round tasks.
2. `prepare-round` carries the same scope into fetch planning and records mission-derived lane source candidates.
3. `summarize-round-readiness` now consumes `verification_scope.required_source_skills` only when they are explicit.
4. If explicit required source imports are not completed, readiness is downgraded from `ready` to `needs-more-data`, even when council readiness opinions are ready.
5. The readiness payload records `verification_scope_gate`, including required, selected, completed, missing-required, and missing-selected source skills.
6. Mission-derived source suggestions are recorded as `candidate_source_skills`; they do not become readiness blockers unless later adopted explicitly.
7. Source equivalence is not hardcoded. It is only applied when `verification_scope.source_skill_equivalents` is explicitly supplied.

Residual risk:

This is a source-import gate, not full evidence-lane adjudication. It does not prove that imported fire-origin or weather data supports a transport claim. The remaining governance work is structural: every explicit report claim should point to an explicit agent/council/report-section object with claim text, evidence refs, and any relevant challenger disposition state.

### S-001: Fire-Origin Lane Was Available But Not Activated

Severity: high

Symptom:

`fetch-nasa-firms-fire` was in the source catalog and skill registry, but was skipped.

Root cause:

No source request or evidence requirement asked for active-fire data. The environmental-investigator task was generic `environment-signal-import`, which was satisfied by Open-Meteo local air quality and weather.

Fix direction:

Add lane-aware source selection:

1. If mission contains `wildfire smoke`, `smoke episode`, `source`, `origin`, `transport`, or equivalent, create a `fire-origin` lane.
2. Map `fire-origin` to `fetch-nasa-firms-fire` and the FIRMS normalizer.
3. Require challenger review before using fire-origin data as attribution.

### S-002: Environmental Role Was Weighted Toward Receptor Evidence

Severity: medium

Symptom:

The environmental side collected local PM2.5 and local weather only.

Root cause:

The `environmental-investigator` / `environmental-investigator` role currently covers all physical evidence. There is no separate role or lane weight for source-origin, smoke plume, trajectory, or transport validation.

Fix direction:

Either:

1. Add a separate `source-attribution-investigator` role, or
2. Keep one environmental role but add sub-lanes:
   - `receptor-air-quality`
   - `local-weather-context`
   - `fire-origin`
   - `smoke-plume`
   - `transport-pathway`
   - `alternative-local-sources`

### S-003: Source Step Budget Can Suppress Necessary Lanes

Severity: medium

Symptom:

The run used `max_source_steps_per_round=3`. A source-origin investigation would require more than three source steps.

Root cause:

The budget is global and source-count oriented, not lane oriented. It does not preserve minimal coverage per critical lane.

Fix direction:

Replace a single max-source budget with lane budgets:

1. `min_sources_per_required_lane`
2. `max_sources_per_lane`
3. `total_source_budget`
4. `must_select_lanes`

### T-001: No Spatiotemporal Relation Flow Was Triggered

Severity: high

Symptom:

No `detect-temporal-cooccurrence-cues`, `query-spatiotemporal-relations`, `review-spatiotemporal-relation-alternatives`, or `materialize-spatiotemporal-relation-evidence-packet` ran.

Root cause:

Spatiotemporal relation is correctly downgraded to optional-analysis, but no orchestration rule promotes it when the mission contains a transport or source-origin question, or when challenger flags attribution risk.

Fix direction:

Add a governed trigger:

1. If a mission has `transport`, `origin`, `source`, `smoke`, `wildfire`, or cross-region terms, record candidate relation review work as council-visible lane/action context without automatically recommending a specific professional helper.
2. If challenger flags transport-attribution risk, surface a review item with candidate follow-up options; readiness impact requires an explicit review comment, readiness opinion, transition request, or confirmed project rule.

Implemented fix, then corrected:

1. `scaffold-mission-run` / mission-to-lane 编译可把 broad smoke/source/transport mission 派生为 `spatiotemporal-relation-review` lane。
2. `prepare-round` 曾在该 lane 出现时建议 relation helper；该做法已回收。
3. 先前 `summarize-round-readiness` 曾新增 `specialist_method_gate` 并在缺 relation packet 时自动降级 readiness；该做法已判定为越权，因为它把 lane evidence 缺口直接解释为硬 blocker。
4. 现在改为 `required_lane_evidence_review`：只评估 lane 内显式 `evidence_requirements`；没有显式 requirement 的 lane 记为 `not-evaluated`。该 review 不会自动改变 readiness，也不会把支持 skill 注入 top-level recommended list。
5. `summarize-round-readiness`、`prepare-round`、`plan-round-orchestration` 不再从 relation/representation/diffusion gap 自动推荐专业 helper；只保留 governance follow-up skills。

Residual risk:

是否要求某个 lane 的 evidence object 作为 freeze/release 硬条件，必须由 agent/council readiness opinion、review comment、transition request，或经确认的项目规则表达。

### T-002: Relation Infrastructure Is Not A Transport Model

Severity: medium

Symptom:

Even if relation helper had run, current baseline is cue-oriented and cannot establish smoke transport.

Root cause:

Current skills support temporal co-occurrence cues and relation packets. They do not ingest plume polygons, trajectory model output, upper-air wind fields, or smoke dispersion products.

Fix direction:

Add optional, explicitly bounded capabilities:

1. `fetch-noaa-hms-smoke` or equivalent smoke-plume polygon source.
2. `normalize-smoke-plume-signals`.
3. `query-fire-signals`.
4. `detect-source-receptor-lag-cues`.
5. `review-transport-attribution-alternatives`.

Keep these as evidence support and uncertainty surfaces, not as strong attribution models.

Current boundary:

本轮修复没有新增烟羽、轨迹或传输 skill，也不再用“专业/非专业 skill”分类决定证据资格。系统只记录缺少哪些显式 lane evidence object；是否阻断、是否采用、如何解释，交由 agent/council 显式对象或已确认规则处理。

### G-001: Runtime Operator Was Correctly Blocked From Normalize

Severity: high

Symptom:

The first `normalize-fetch-execution` attempt with `runtime-operator` was blocked and created an open dead letter. The retry with `environmental-investigator` succeeded.

Root cause:

The first block was not a bug. It correctly enforced that `runtime-operator` is an approval/admin role, not a data collection or normalization actor. The actual bug was that `skill_registry.py` still listed `runtime-operator` as allowed for `normalize-fetch-execution`, creating a misleading runnable surface.

The retry exposed a second bug: `environmental-investigator` executed the whole fetch plan, including social-investigator/public-discourse steps. That let one investigator role perform another role's data collection and normalization.

Fix direction:

1. Remove `runtime-operator` from `normalize-fetch-execution.allowed_roles`.
2. Remove `normalize` capability from `runtime-operator`.
3. Require `normalize-fetch-execution` to know the executing actor role.
4. Filter fetch-plan steps so each actor only executes steps owned by its role.
5. Preserve actor role lineage on import execution statuses and detached fetch ledger events.

Implemented fix:

1. `runtime-operator` no longer has `CAPABILITY_NORMALIZE`.
2. `normalize-fetch-execution.allowed_roles` is now limited to investigator roles.
3. Runtime `run-skill` injects `OPENCLAW_ACTOR_ROLE` and `OPENCLAW_RESOLVED_ACTOR_ROLE`; direct script use may also pass `--actor-role`.
4. `normalize-fetch-execution` filters plan steps by actor role aliases: `social-investigator` owns `social-investigator` steps, and `environmental-investigator` owns `environmental-investigator` steps.
5. Existing partial import execution receipts are merged, so public and environmental roles can run the same plan in separate role-owned slices.
6. `prepare-round` emits `suggested_next_skill_runs` with role-owned `normalize-fetch-execution` runs.
7. Tests confirm operator preflight is blocked and role-owned fetch/normalize statuses preserve actor lineage.

Residual risk:

The current CLI still relies on the caller/controller to run each role slice. A later UI/controller improvement should render or execute the `suggested_next_skill_runs` fanout explicitly so a mixed source plan cannot be accidentally left half-normalized.

### G-002: Dead Letter Has No Resolution Command

Severity: high

Symptom:

The run finished successfully, but `runtime_health.json` stayed red because `deadletter-f6db1d1237f8dd8b0713` remained open.

Root cause:

The dead-letter surface can materialize and list dead letters. There is no governed command to close, supersede, or mark a dead letter as recovered after a successful retry.

Fix direction:

Add `resolve-dead-letter` or `mark-dead-letter-resolved` with:

1. Required `dead_letter_id`.
2. Required `resolution_status=closed|superseded|accepted-risk`.
3. Required `resolution_reason`.
4. Optional `superseding_receipt_id`.
5. Runtime ledger event.

Health should exclude resolved dead letters.

### G-003: Readiness Was Not Automatically Materialized Before Freeze

Severity: high

Symptom:

The first report-basis freeze was withheld because no canonical `round_readiness` artifact / DB assessment existed. A separate approved `summarize-round-readiness` run was needed.

Root cause:

The transition-executor plan includes a `report-basis-gate`, but not a required materialization step for round readiness when readiness is missing.

Fix direction:

Before `report-basis-gate`, the controller should:

1. Detect missing readiness.
2. Surface a required readiness materialization step.
3. Stop before freeze if approval is required and absent.
4. Re-run the gate after readiness is materialized.

### G-004: Supervisor And Controller State Could Be Stale

Severity: high

Symptom:

After a second freeze transition was approved, `supervise-round` reused a stale controller snapshot until `restart-governed-execution-round` was run. Reporting handoff also had to be rerun after refreshing supervisor state.

Root cause:

Controller/supervisor surfaces did not consistently detect that the latest approved transition request was newer than the persisted controller state.

Fix direction:

Add freshness checks:

1. Controller snapshot should record the transition request id it adopted.
2. `supervise-round` should compare that id with the latest approved transition request.
3. If stale, it should replan/restart automatically or return an explicit `stale-controller` blocker.
4. `materialize-reporting-handoff` should require a supervisor snapshot whose input controller/gate/freeze ids match the current latest records.

Implemented fix:

1. `run_governed_execution_round_with_contract_mode` now records `adopted_transition_request_id`, `adopted_transition_request_status`, and `adopted_transition_kind` on controller payloads.
2. In default transition-executor mode, a completed controller is not blindly reused if a newer approved `freeze-report-basis` request exists; it restarts with `resume_status=restart-stale-transition` and records a `controller-freshness` planning attempt.
3. `supervise-round` copies the controller's adopted transition fields into the supervisor snapshot, so downstream reporting can audit which approved transition was supervised.
4. `materialize-reporting-handoff` compares `frozen_report_basis.transition_request_id` with `supervisor.adopted_transition_request_id`; a mismatch adds `stale-supervisor-state`, sets `supervisor_status=stale-controller`, and keeps `handoff_status=investigation-open`.
5. Regression coverage:
   - `tests.test_runtime_kernel.RuntimeKernelTests.test_controller_restarts_completed_default_path_for_newer_approved_transition_request`
   - `tests.test_reporting_workflow.ReportingWorkflowTests.test_reporting_handoff_blocks_stale_supervisor_transition_request`

Residual risk:

This fix covers the freeze-report-basis path exposed by the NYC smoke run. Other phase transitions, especially round close and next-round opening, should use the same adopted-transition freshness pattern before being treated as unattended-safe.

### G-005: Challenger Review Did Not Force Follow-Up Or Readiness Hold

Severity: high

Symptom:

The challenger correctly warned that GDELT is not representative, PM2.5 is modelled, and weather context is not transport attribution. The council still reached ready/finalize without a follow-up round.

Root cause:

Challenger review is currently preserved as context, but it is not converted into a structural constraint on the challenged claim/finding/evidence/ref. The first repair also treated a challenger readiness opinion that merely references the comment as a waiver. That is too weak: citation is not disposition.

Corrected fix direction:

Add a bridge from serious review comments to explicit disposition objects:

1. `review_comment.report_risk`, `target_kind`, `target_id`, `evidence_refs`, and `required_followup_evidence` should create a pending challenger constraint record.
2. The constraint must be explicitly disposed as `accepted_as_limitation`, `requires_followup`, `excluded_from_report_basis`, `resolved_by_followup`, or `waived_by_challenger`.
3. A moderator/readiness opinion may cite the constraint, but that only proves awareness; it does not waive or resolve it.
4. Readiness/freeze/publication gates should check unresolved constraint state and basis-use constraints, not infer whether the challenger is substantively right.
5. Provide `open-followup-from-review-comment`.

Implemented fix after G-008 first-stage repair:

1. `summarize-round-readiness` now queries DB-backed `review-comment` objects for the round.
2. Open/submitted challenger review comments with a non-empty `report_risk` or required follow-up evidence create structural `challenger_constraints`.
3. A challenger `readiness-opinion` that cites the review comment no longer resolves the constraint. It only proves awareness.
4. Resolution now requires an explicit `review-comment` response carrying `constraint_disposition`: `accepted_as_limitation`, `requires_followup`, `excluded_from_report_basis`, `resolved_by_followup`, or `waived_by_challenger`.
5. Blocked readiness records `challenger_constraint_count`, `unresolved_challenger_constraint_count`, `challenger_constraints`, `unresolved_challenger_constraints`, and `basis_use_constraints`; legacy `blocking_review_comment_*` fields remain as compatibility aliases for unresolved constraints.
6. Blocked readiness recommends `open-followup-from-review-comment`, `open-challenge-ticket`, `claim-board-task`, and `submit-readiness-opinion` without adding domain-specific helper skills based on comment text.
7. `freeze-report-basis`, `materialize-reporting-handoff`, `draft-council-decision`, and `materialize-final-publication` propagate challenger/basis-use constraints; unresolved constraints block freeze/reporting/release.
8. `post-review-comment` now accepts `--status` and `--constraint-disposition` so disposition stays in council objects instead of DB edits or runtime inference.
9. `open-followup-from-review-comment` turns one report-risk review comment into a linked challenge ticket and claimed board task, preserving comment id, target, report risk, evidence refs, and relation objection fields in lineage/provenance.
10. `submit-report-section-draft` can now carry explicit report-claim and lead-basis structure.
11. `freeze-report-basis` blocks explicit report claims that lack claim text/evidence refs, lack a required challenger disposition chain, or conflict with `lead_basis_allowed=false` when used as lead basis.
12. Regression coverage:
   - `tests.test_council_autonomy_flow.CouncilAutonomyFlowTests.test_report_risk_review_comment_blocks_readiness_until_explicit_disposition`
   - `tests.test_council_autonomy_flow.CouncilAutonomyFlowTests.test_lead_basis_conflicting_with_constraint_withholds_freeze`
   - `tests.test_council_autonomy_flow.CouncilAutonomyFlowTests.test_minimal_explicit_report_claim_freezes_without_template_fields`
   - `tests.test_council_autonomy_flow.CouncilAutonomyFlowTests.test_explicit_report_claim_without_text_withholds_freeze`
   - `tests.test_council_autonomy_flow.CouncilAutonomyFlowTests.test_open_followup_from_review_comment_creates_challenge_and_task`

Residual risk:

This now closes the “challenger comment is only a soft caveat” failure for gate transitions and blocks explicitly declared report claims/lead basis when their structural contract or challenger disposition chain is incomplete. The remaining gap is not substantive judgement, but coverage: report text that is not explicitly claim-tagged is not parsed or treated as a claim by runtime.

### R-001: Final Open Risks Were False Positives

Severity: high

Symptom:

Final publication carried positive notes as open risks:

1. The round is ready for downstream reporting handoff.
2. No blocking board or probe objects remain.
3. Council submitted 1 readiness opinion and all support freeze.

Root cause:

`build_open_risks` in `materialize-reporting-handoff` turns all supervisor `operator_notes` and readiness `gate_reasons` into risks, regardless of polarity.

Fix direction:

Classify gate reasons and operator notes:

1. `supporting_reason`
2. `blocking_reason`
3. `warning`
4. `operator_note`

Only blocking reasons and explicit warnings should become `open_risks` or `uncertainty_register`.

### R-002: Decision Lead Basis Was Arbitrary

Severity: medium

Symptom:

Decision summary used wind speed as the lead basis, although PM2.5 was the central receptor anomaly.

Root cause:

`draft-council-decision` used `key_findings[0].summary` as the lead basis. A later plan proposed semantic ranking, but that would also be wrong because the skill would be judging evidence priority.

Corrected fix direction:

Do not rank lead basis semantically inside the skill.

1. If agent/council submits an explicit lead-basis object, explicit priority, or explicit report-section order, the decision may preserve that order.
2. If no explicit lead-basis disposition exists, the decision summary should describe evidence coverage without naming a lead basis.
3. If a challenger constraint limits a finding/evidence ref, that object cannot be selected as lead basis unless the limitation disposition explicitly allows that use.
4. For this run, wind speed should not have become lead basis merely because it appeared first.

### R-003: Expert Reports Are Not Role-Specific Enough

Severity: medium

Symptom:

Both expert reports include the same three findings, only changing the `focus` field.

Root cause:

`draft-expert-report` maps all `key_findings` into each role report. A role-expertise mapping would reduce leakage, but hardcoding domain expertise into the reporting skill would again risk moving judgement into infrastructure.

Corrected fix direction:

Add role-specific report assembly using explicit structure only:

1. Use `finding.agent_role`, `report-section-draft.role`, explicit `target_role`, or explicit cross-role citation fields.
2. Cross-role evidence may appear only as cited context, not as that role's own finding, unless an agent/council object explicitly assigns it.
3. Do not infer role expertise from domain keywords inside the reporting skill.

### R-004: Published Expert Reports Retain Ambiguous Status

Severity: medium

Symptom:

Canonical expert reports still have `status=ready-to-publish`.

Root cause:

`publish-expert-report` creates canonical stage but does not change report status to `published` or `canonical-published`.

Fix direction:

Separate:

1. `report_stage=draft|canonical`
2. `draft_status=ready-to-publish|needs-more-evidence`
3. `publication_status=published|not-published`

### R-005: Expert Report Status Fields Are Internally Inconsistent

Severity: high

Symptom:

Expert reports were `ready-to-publish`, but included stale blockers such as `readiness-missing` and `supervisor-missing`.

Root cause:

`draft-expert-report` does not put `readiness_status` and `supervisor_status` into the payload even when the handoff is reporting-ready. Downstream normalization can re-run reporting gate defaults and produce missing-status blockers.

Fix direction:

Propagate:

1. `readiness_status`
2. `supervisor_status`
3. `report_basis_status`
4. `handoff_status`
5. `reporting_ready`

from handoff/decision into expert report payload and storage row. Add tests asserting no blockers when handoff is reporting-ready.

### R-006: Final Publication Can Be Ready While Sections Still Say Basis Required

Severity: medium

Symptom:

Final publication is `ready-for-release`, while generated sections include states such as `needs-explicit-moderator-text` or `basis-required`.

Root cause:

`materialize-final-publication` gates release mostly on council decision readiness and canonical report presence. Section readiness is not part of publication gating.

Fix direction:

Add publication quality gate:

1. Release may proceed only if required sections are `included`, `explicitly-scoped-out`, or `appendix-only`.
2. `basis-required` and `needs-explicit-moderator-text` should either block release or downgrade posture to `hold-release`.

Implemented fix:

1. `materialize-final-publication` now computes `release_blockers` from decision readiness, handoff readiness, open risks, supervisor status, role report status, and required section statuses.
2. If `open_risks` exist and no uncertainty register was materialized, final publication converts those open risks into uncertainty rows.
3. If no open risks or uncertainty rows exist, the risks/uncertainties section is marked `no-open-risks-recorded` rather than `basis-gap`, so an empty risk set does not falsely block release.
4. Unresolved challenger constraints now propagate through handoff and become release blockers.
5. Explicit lead-basis constraint violations now propagate through handoff and become release blockers.

Residual risk:

Final publication still needs recommendation section object completeness. Explicit lead-basis objects are checked, explicit report claims carry minimum structure, and key findings are now limited to explicitly selected frozen evidence/basis objects. The publication gate must not decide whether claims or recommendations are professionally sufficient; it should only require explicit agent/council/report-section objects, evidence refs, and relevant challenger disposition chains.

### R-007: Frozen Selected Evidence Was Too Narrow, Then Too Implicit

Severity: medium

Symptom:

The frozen report basis `selected_evidence_refs` contained only the PM2.5 evidence ref, while the evidence bundle held three refs.

Root cause:

The freeze transition and proposal evidence refs centered on PM2.5, and `freeze-report-basis` selected those refs rather than expanding through accepted evidence bundle members.

Initial fix direction, now rejected:

When a report-basis proposal targets an evidence bundle, freeze should include all accepted bundle evidence refs unless explicitly excluded.

Updated governance boundary:

This introduced an implicit selection rule. It treated a referenced evidence bundle as permission to select every evidence ref inside it. That crosses the governance boundary: evidence selection, exclusion, ranking, and weighing must be explicit agent/council judgement, not skill behavior.

Correct fix direction:

1. `freeze-report-basis` may expose all evidence refs from referenced bundles as candidates.
2. `freeze-report-basis` must not add those candidate refs to `selected_evidence_refs`.
3. `selected_evidence_refs` should contain only evidence refs explicitly supplied by agent/council objects, approved transition requests, readiness opinions, proposals, report sections, or other accepted DB-backed judgement objects.
4. If candidate refs exist but were not explicitly selected, the freeze artifact should record that gap for review.
5. Any future fields such as `excluded_evidence_refs`, `selection_rationale`, or evidence weights require explicit rule confirmation before implementation.

Implemented correction:

1. `freeze-report-basis` now queries DB-backed `evidence-bundle` objects for the round.
2. It identifies selected/supported bundle ids from selected basis object ids, supporting proposals, and supporting readiness opinions.
3. It records `candidate_evidence_bundle_ids`, `candidate_bundle_evidence_refs`, and `unselected_candidate_bundle_evidence_refs`.
4. It sets `evidence_selection_policy=explicit-agent-council-evidence-refs-only-v1`.
5. It consumes accepted DB `report-section-draft` evidence refs as explicit agent selections and records `explicit_report_section_draft_ids` plus `explicit_report_section_draft_evidence_ref_count`.
6. It leaves legacy `expanded_evidence_bundle_ids=[]` and `expanded_evidence_bundle_ref_count=0` to make clear no automatic expansion occurred.
7. Regression coverage asserts that a policy fixture cross-plane bundle exposes public, formal, and environmental refs as candidates; those refs enter `selected_evidence_refs` only when a DB report section explicitly cites them.

Residual risk:

This still does not implement `excluded_evidence_refs`, evidence weights, or ranking. Those are rule-bearing fields and require explicit confirmation before implementation.

### R-008: Report Recommendations Were Generic

Severity: medium

Symptom:

Policy recommendations were limited to generic reporting/audit actions, not smoke-event handling recommendations.

Root cause:

The mission avoided policy direction, no response-planning evidence lane existed, and reporting skills do not derive public-health or emergency response recommendations from evidence.

Fix direction:

Add a response lane only when mission asks for recommendations:

1. `formal-response-records`
2. `public-health-guidance`
3. `emergency-operations-actions`
4. `recommended-response-options`

Keep recommendations evidence-bounded and explicitly uncertain.

Implemented fix:

1. `materialize-reporting-handoff` no longer emits generic reporting/audit actions as `policy_recommendations` when the round is reporting-ready.
2. `materialize-final-publication` therefore marks recommendations as `not-in-scope` unless DB-backed policy recommendations are present.
3. 先前 `summarize-round-readiness` 曾把 `response-recommendation-boundary` 缺少带 `evidence_refs` 的 recommendations/response 类 DB `report-section-draft` 作为 readiness blocker；该做法已回收。
4. 当前只记录该 lane 的 evidence-referenced report section 是否存在；不自动判定它是否必须阻断 freeze/release。
5. Regression coverage asserts that generic report-writing actions do not appear in the final decision-maker report as policy recommendations, and that response lane evidence presence is recorded without overriding readiness opinions.

Residual risk:

This prevents generic reporting actions from being silently converted into recommendations, but it does not decide whether missing response recommendations should block release. That blocker rule requires explicit confirmation.

### C-001: Contract Parsing Produces False Missing Inputs

Severity: medium

Symptom:

`submit-council-proposal` preflight reported missing inputs parsed from prose, such as `Recommended` and fragments of Markdown text.

Root cause:

The runtime contract parser is reading non-structured `SKILL.md` language as required fields.

Fix direction:

Require machine-readable contract metadata and stop parsing prose as authoritative input schema.

### C-002: Reporting Contracts Are Incomplete

Severity: medium

Symptom:

Several reporting skills emitted `undeclared-summary-path` for `db_path`. Some skills wrote DB records or artifacts not fully reflected in declared contracts.

Root cause:

Skill contracts and actual write behavior diverged as reporting DB-backed outputs evolved.

Fix direction:

Update each reporting skill contract to list:

1. DB read/write side effects.
2. Artifact reads.
3. Artifact writes.
4. Summary paths that resolve outside declared writes.

### C-003: Reporting Contract Helper Contains Duplicates And No-Op Conditions

Severity: low

Symptom:

`reporting_contracts.py` includes duplicated entries such as `report_basis`, and conditions like `if "report_basis_artifact_present" in source and "report_basis_artifact_present" not in source`.

Root cause:

Mechanical edits accumulated without cleanup tests for helper normalization.

Fix direction:

Clean the helper and add tests for observed input propagation.

## 6. Repair Plan

### Phase 0: Preserve This Run As A Regression Fixture

Goal:

Make the observed failures reproducible.

Tasks:

1. Add a minimal fixture derived from this run's mission, source selections, audit ledger, and reporting outputs.
2. Add tests that assert the current failures are detectable:
   - FIRMS not selected under the narrow mission.
   - Dead letter remains open after successful retry.
   - Positive gate reasons are carried as open risks.
   - Expert reports can be ready while carrying missing readiness/supervisor blockers.

Acceptance:

1. The fixture can be used without network calls.
2. Each issue has at least one failing or diagnostic assertion before code fixes.

Implemented fix:

1. Added `tests/fixtures/openclaw-realcase-nyc-smoke-phase0.json`.
2. The fixture freezes a minimal subset of the real run's mission, source selections, execution/dead-letter state, reporting handoff, expert reports, and final publication posture.
3. Added `tests.test_realcase_nyc_smoke_phase0_fixture`.
4. The test suite asserts that the fixture is offline, points to local run artifacts, and preserves the original issue signatures:
   - no fire-origin source selection;
   - no smoke plume or transport source selection;
   - successful import with an open dead letter;
   - positive readiness/gate notes carried as open risks;
   - expert reports marked `ready-to-publish` while carrying blocked readiness/unavailable supervisor status;
   - final publication released with `missing-coverage`.

### Phase 1: Fix Runtime/Governance Bugs

Priority: highest

Tasks:

1. Fix `normalize-fetch-execution` role policy mismatch and role-owned fetch/normalize fanout.
2. Add a governed dead-letter resolution command.
3. Make report-basis freeze require a canonical readiness assessment or stop with an actionable approval request.
4. Add controller/supervisor freshness checks for transition request adoption.
5. Require reporting handoff to use a fresh supervisor state.

Acceptance:

1. No red runtime health remains after a successful governed retry and explicit dead-letter resolution.
2. `supervise-round` cannot silently use an older approved transition when a newer one exists.
3. Freeze cannot proceed without explicit readiness state.

### Phase 2: Fix Reporting Correctness

Priority: high

Tasks:

1. Stop converting positive operator notes and gate reasons into risks.
2. Remove automatic lead-basis selection; preserve only agent/council explicit lead-basis or priority order.
3. Propagate readiness and supervisor statuses into expert reports.
4. Introduce distinct draft/canonical/publication statuses.
5. Make section readiness part of final publication gating.
6. Expose evidence refs from referenced bundles as candidates, but require agent/council explicit evidence refs before they enter `selected_evidence_refs`.
7. Add challenger constraint disposition and basis-use constraint checks before decision/final publication can present a challenged object as core basis.

Acceptance:

1. A ready publication has no false open risks.
2. Expert reports have no stale blockers when handoff is reporting-ready.
3. A final publication cannot be `ready-for-release` while required sections remain `basis-required`.
4. No decision/final publication can name a lead basis unless that lead basis is explicit and has no unresolved challenger constraint.

### Phase 3: Add Mission-To-Lane Orchestration

Priority: high

Tasks:

1. Add a mission compiler or scope derivation step.
2. Represent candidate evidence lanes as DB-backed objects, with `required` only when mission text, agent/council object, or confirmed rule explicitly says so.
3. Change `prepare-round` to consume explicit required lanes and surface candidate lanes without treating them as blockers.
4. Treat explicit source requests as seed inputs unless the mission says they are exhaustive.
5. Add lane budgets and source coverage metadata, avoiding hardcoded sufficiency or source-equivalence rules.

Acceptance:

1. A mission phrased as `investigate NYC smoke event and provide response recommendations` creates lanes for receptor air quality, public discourse, fire-origin, weather/transport context, source limitations, and response guidance.
2. If fire-origin/transport lanes are explicitly required and unmet, readiness can be `needs-more-data`; mission-derived candidate lanes alone should not override council readiness.

### Phase 4: Add Or Wire Missing Skills

Priority: medium

Tasks:

1. Wire existing `fetch-nasa-firms-fire` into lane-aware source planning.
2. Add `query-fire-signals` if current environment query is not enough for fire-origin use.
3. Add optional smoke-plume ingestion, for example NOAA HMS smoke if available.
4. Add `detect-source-receptor-lag-cues` as a bounded optional-analysis helper.
5. Add `review-transport-attribution-alternatives`.
6. Add response recommendation drafting that consumes formal/public-health/emergency records.

Acceptance:

1. Transport claims require explicit agent/council claim/finding/report-section objects with cited source/time/space/transport evidence; the skill layer does not decide sufficiency.
2. The council can explicitly report "source not verified" instead of silently omitting the question.

### Phase 5: Add CI Quality Gates

Priority: medium

Tasks:

1. Source lane selection tests.
2. Runtime role/capability consistency tests.
3. Dead-letter lifecycle tests.
4. Reporting freshness tests.
5. Final publication structural gate tests.
6. Regression test using the NYC smoke fixture.

Acceptance:

1. The current run's known failures cannot reappear silently.
2. A broad smoke-event mission no longer collapses into only local PM2.5 plus public discourse.

## 7. Architecture Recommendations

### 7.1 Treat Mission As Investigation Intent, Not Source Plan

The mission should define the question and boundaries. Source planning should be derived from that question.

Recommended additions:

1. `mission_intent`
2. `verification_scope`
3. `evidence_lanes`
4. explicit project-rule references, only when supplied by agent/council or confirmed project rules
5. `report_objective`

### 7.2 Promote Evidence Lanes To First-Class Objects

Current role/source selection is too coarse. It should represent evidence lanes without turning candidate lanes into implicit conclusions or blockers:

1. Local receptor evidence.
2. Fire-origin evidence.
3. Smoke/plume evidence.
4. Weather/transport context.
5. Public discourse.
6. Formal response records.
7. Health/community impact.
8. Alternative explanations.

### 7.3 Separate Receptor Description From Attribution

A smoke report should distinguish:

1. Receptor anomaly: what happened in NYC.
2. Candidate source: where smoke may have originated.
3. Transport plausibility: whether time/space/path evidence is consistent.
4. Attribution claim: stronger claim, only if an agent/council/report-section object explicitly states the claim text and cites evidence refs.

Current run only satisfied the first item.

### 7.4 Make Challenger Findings Operational

Challenger reviews should be able to create structural constraints when they identify scope-critical gaps.

Recommended mechanism:

1. Review comment constraint id.
2. Target claim/lane.
3. Required follow-up evidence.
4. Basis-use disposition: `accepted_as_limitation`, `requires_followup`, `excluded_from_report_basis`, `resolved_by_followup`, or `waived_by_challenger`.
5. Readiness/freeze/release impact derived from unresolved disposition state, not from the skill judging the technical correctness of the comment.

### 7.5 Add Report Quality Gates

Final publication should not be just an artifact-completeness check. It should validate:

1. Required sections resolved.
2. Evidence refs preserved.
3. Open risks are real risks.
4. Challenger constraints and source limitations are carried when explicitly submitted.
5. Every explicit report claim has an explicit agent/council/report-section source object, claim text, and evidence refs.
6. Recommendations come from explicit agent/council recommendation or report-section objects with evidence refs; the publication skill does not derive or validate substantive policy advice.

## 8. Suggested Next Mission For Re-Run

Use a broader but still evidence-bounded mission:

> Investigate the June 2023 New York City smoke episode. Identify the local pollution anomaly, candidate wildfire source regions, possible smoke transport pathway, public and health/community impact signals, formal response records, unresolved uncertainties, and evidence-bounded handling recommendations. Ask the council to explicitly state claim boundaries, cited evidence, and unresolved uncertainty before any causal, transport, or policy conclusion.

Expected minimum evidence lanes:

1. NYC receptor PM2.5/AQI.
2. Fire-origin candidates.
3. Weather and transport context.
4. Smoke plume or satellite cue, if available.
5. Public discourse and community impact.
6. Formal/public-health response record.
7. Challenger alternative-explanation review.

## 9. Immediate Fix Order

Recommended order:

1. Fix runtime role/capability mismatch, role-owned fetch/normalize fanout, and dead-letter lifecycle.
2. Fix readiness-before-freeze and stale controller/supervisor behavior.
3. Fix reporting false risks and stale blockers.
4. Add mission-to-lane derivation.
5. Wire fire-origin and spatiotemporal follow-up lanes.
6. Re-run the NYC smoke case with the broader mission.

This order avoids building new analysis skills on top of unstable governance/reporting surfaces.
