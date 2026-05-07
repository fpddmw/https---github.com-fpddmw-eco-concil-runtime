# OpenClaw NYC Smoke Real-Case Run Diagnostics

## 中文摘要

本轮 run 的核心问题不是单一 bug，而是 mission 表述、证据 lane 编排、skill 触发规则、治理状态传播、报告门禁共同造成的系统性偏窄。

主要结论：

1. 当前 mission 实际是在调查「公开报道 + 本地环境观测是否足以形成受限报告」，不是在完整调查「纽约烟霾事件的异常、始源地、传输路径、影响和处置建议」。
2. `source_requests` 在运行中成为事实上的固定议程，导致 council 只围绕 GDELT、Open-Meteo 空气质量、Open-Meteo 历史天气展开，没有自动扩展到火点、烟羽、轨迹或健康/响应证据。
3. NASA FIRMS 火点能力在底层 source catalog 中存在，但没有被 mission/lane 编排激活；因此没有显式加拿大数据获取，也没有火点数据调用。
4. 时空关系 skill 已存在，但定位为可选分析；当前编排没有在「wildfire smoke / source / origin / transport」这类任务中自动触发，所以没有形成空间中心和时间中心转移的证据包。
5. 报告链路过早给出 `ready-for-release`：部分 canonical report 仍带有 stale blocker / stale supervisor note，final publication 对 section-level 的 `basis-required`、`needs-explicit-moderator-text` 没有形成硬阻断。
6. 治理层存在明确 bug 和契约缺口，包括 `runtime-operator` 被允许执行 normalize 但角色能力不含 `normalize`、dead letter 缺少关闭/解决命令、reporting contract 声明不完整、preflight 误解析 prose 造成 false missing input。

建议修复顺序：

1. 先固化本轮 run 为回归 fixture，避免修复时丢失真实失败样本。
2. 修 `runtime-operator normalize`、dead letter resolution、report readiness/supervisor 状态传播等硬 bug。
3. 加严 final publication 门禁，禁止 stale blocker、false open risk 和 section-level missing basis 进入 release。
4. 引入 mission-to-lane 编译：把 broad mission 自动展开为 receptor、fire-origin、smoke-plume、transport、impact、response、uncertainty lanes。
5. 为 wildfire smoke 类任务补齐 source-attribution/transport 相关 skill 和 CI fixture。

## 1. Scope

This document reviews run `openclaw-realcase-nyc-smoke-20230607` after the first real-case council execution.

Run directory:

`runs/openclaw-realcase-nyc-smoke-20230607`

Primary final artifacts:

1. `reporting/final_publication_round-001.json`
2. `reporting/council_decision_round-001.json`
3. `reporting/expert_report_sociologist_round-001.json`
4. `reporting/expert_report_environmentalist_round-001.json`
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

1. `source_selection_environmentalist_round-001.json` marks `fetch-nasa-firms-fire` as `selected=false`.
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

> Do not assert source attribution or transport causality unless supported by explicit source, timing, spatial, and transport evidence.

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

No object captured receptor, candidate source region, lag window, spatial rule, transport evidence requirements, excluded inferences, or report decision boundary.

Root cause:

The current scaffold only seeded a broad hypothesis and role tasks. It did not create a structured verification scope object.

Fix direction:

Add a `define-verification-scope` or `derive-investigation-lanes` step before source planning. It should create DB-backed objects such as:

1. `receptor_region`
2. `study_window`
3. `candidate_source_region_policy`
4. `lag_window`
5. `required_evidence_lanes`
6. `excluded_inferences`
7. `reportable_claim_boundary`

### S-001: Fire-Origin Lane Was Available But Not Activated

Severity: high

Symptom:

`fetch-nasa-firms-fire` was in the source catalog and skill registry, but was skipped.

Root cause:

No source request or evidence requirement asked for active-fire data. The environmentalist task was generic `environment-signal-import`, which was satisfied by Open-Meteo local air quality and weather.

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

The `environmentalist` / `environmental-investigator` role currently covers all physical evidence. There is no separate role or lane weight for source-origin, smoke plume, trajectory, or transport validation.

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

1. If a mission has `transport`, `origin`, `source`, `smoke`, `wildfire`, or cross-region terms, recommend relation helper approval before readiness.
2. If challenger flags `weather context must not be treated as transport attribution`, automatically create a follow-up task or `needs-more-data` readiness path unless relation evidence is explicitly absent by scope.

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

### G-001: Runtime Operator Was Allowed But Lacked Normalize Capability

Severity: high

Symptom:

The first `normalize-fetch-execution` attempt with `runtime-operator` was blocked and created an open dead letter. The retry with `environmental-investigator` succeeded.

Root cause:

`skill_registry.py` allows `runtime-operator` for `normalize-fetch-execution`, but `role_contracts.py` does not give `runtime-operator` the `normalize` capability.

Fix direction:

Choose one policy:

1. Remove `runtime-operator` from `normalize-fetch-execution.allowed_roles`, or
2. Add a distinct `runtime-execute-normalize` capability to `runtime-operator` and use that instead of the investigator-facing `normalize` capability.

Preferred:

Keep `runtime-operator` as approval/admin authority and run normalization under an investigator role. Remove `runtime-operator` from the skill's allowed roles unless a separate executor role is introduced.

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

### G-005: Challenger Review Did Not Force Follow-Up Or Readiness Hold

Severity: high

Symptom:

The challenger correctly warned that GDELT is not representative, PM2.5 is modelled, and weather context is not transport attribution. The council still reached ready/finalize without a follow-up round.

Root cause:

Challenger review is currently preserved as context, but does not automatically affect readiness unless an agent submits a blocking readiness opinion, challenge ticket, task, or proposal.

Fix direction:

Add a bridge from serious review comments to action/readiness:

1. `review_comment.report_risk=source-limitations` plus mission requiring source/transport should create a `needs-more-data` default unless explicitly waived.
2. Provide `open-followup-from-review-comment`.
3. Let readiness summarizer count unresolved review comments by severity and target kind.

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

`draft-council-decision` uses `key_findings[0].summary` as the lead basis. The ordering is not semantically ranked.

Fix direction:

Rank lead basis by:

1. Mission objective and issue type.
2. Finding kind.
3. Evidence role.
4. Source limitations.
5. Whether the finding supports the main receptor anomaly.

For this case, PM2.5 should be primary, public discourse secondary, wind/weather contextual.

### R-003: Expert Reports Are Not Role-Specific Enough

Severity: medium

Symptom:

Both expert reports include the same three findings, only changing the `focus` field.

Root cause:

`draft-expert-report` maps all `key_findings` into each role report. It does not filter or transform findings by role expertise.

Fix direction:

Add role-specific mapping:

1. Sociologist: public discourse, community impact, representativeness limitations.
2. Environmentalist: PM2.5, fire, plume, weather, exposure uncertainty.
3. Formal/policy role if present: official notices, emergency actions, regulatory obligations.

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

### R-007: Frozen Selected Evidence Was Too Narrow

Severity: medium

Symptom:

The frozen report basis `selected_evidence_refs` contained only the PM2.5 evidence ref, while the evidence bundle held three refs.

Root cause:

The freeze transition and proposal evidence refs centered on PM2.5, and `freeze-report-basis` selected those refs rather than expanding through accepted evidence bundle members.

Fix direction:

When a report-basis proposal targets an evidence bundle, freeze should include all accepted bundle evidence refs unless explicitly excluded.

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

### Phase 1: Fix Runtime/Governance Bugs

Priority: highest

Tasks:

1. Fix `normalize-fetch-execution` role policy mismatch.
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
2. Rank lead basis semantically rather than by list order.
3. Propagate readiness and supervisor statuses into expert reports.
4. Introduce distinct draft/canonical/publication statuses.
5. Make section readiness part of final publication gating.
6. Expand selected evidence refs from accepted evidence bundles.

Acceptance:

1. A ready publication has no false open risks.
2. Expert reports have no stale blockers when handoff is reporting-ready.
3. A final publication cannot be `ready-for-release` while required sections remain `basis-required`.

### Phase 3: Add Mission-To-Lane Orchestration

Priority: high

Tasks:

1. Add a mission compiler or scope derivation step.
2. Represent required evidence lanes as DB-backed objects.
3. Change `prepare-round` to consume required lanes, not only explicit source requests.
4. Treat explicit source requests as seed inputs unless the mission says they are exhaustive.
5. Add lane budgets and source coverage rules.

Acceptance:

1. A mission phrased as `investigate NYC smoke event and provide response recommendations` creates lanes for receptor air quality, public discourse, fire-origin, weather/transport context, source limitations, and response guidance.
2. If fire-origin/transport lanes are unmet, readiness should be `needs-more-data` unless the council explicitly scopes them out.

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

1. Transport claims remain non-causal unless supported by source, time, space, and transport evidence.
2. The council can explicitly report "source not verified" instead of silently omitting the question.

### Phase 5: Add CI Quality Gates

Priority: medium

Tasks:

1. Source lane selection tests.
2. Runtime role/capability consistency tests.
3. Dead-letter lifecycle tests.
4. Reporting freshness tests.
5. Final publication semantic gate tests.
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
4. `excluded_inferences`
5. `report_objective`

### 7.2 Promote Evidence Lanes To First-Class Objects

Current role/source selection is too coarse. It should reason about evidence lanes:

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
4. Attribution claim: stronger claim, only if supported.

Current run only satisfied the first item.

### 7.4 Make Challenger Findings Operational

Challenger reviews should be able to affect readiness when they identify scope-critical gaps.

Recommended mechanism:

1. Review comment severity.
2. Target claim/lane.
3. Required follow-up evidence.
4. Readiness impact.
5. Explicit waiver path if moderator scopes the question down.

### 7.5 Add Report Quality Gates

Final publication should not be just an artifact-completeness check. It should validate:

1. Required sections resolved.
2. Evidence refs preserved.
3. Open risks are real risks.
4. Source limitations are carried.
5. Claims do not exceed evidence basis.
6. Recommendations match mission scope and evidence support.

## 8. Suggested Next Mission For Re-Run

Use a broader but still evidence-bounded mission:

> Investigate the June 2023 New York City smoke episode. Identify the local pollution anomaly, candidate wildfire source regions, possible smoke transport pathway, public and health/community impact signals, formal response records, unresolved uncertainties, and evidence-bounded handling recommendations. Do not assert causal attribution or policy conclusions unless supported by explicit source, timing, spatial, and transport evidence.

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

1. Fix runtime role/capability mismatch and dead-letter lifecycle.
2. Fix readiness-before-freeze and stale controller/supervisor behavior.
3. Fix reporting false risks and stale blockers.
4. Add mission-to-lane derivation.
5. Wire fire-origin and spatiotemporal follow-up lanes.
6. Re-run the NYC smoke case with the broader mission.

This order avoids building new analysis skills on top of unstable governance/reporting surfaces.
