# Runtime Task Board

## Board Scope

- Persist forward execution planning in this file.
- Keep this board focused on open work only; do not keep old completed slice detail here.
- Historical baseline for completed `T01` to `T06` now lives in `docs/architecture/runtime-task-board-history-t01-t06.md`.

## Working Rules

- Keep at most one sub-slice in `in_progress` at a time.
- Finish one sub-slice end-to-end before opening the next one.
- A sub-slice is only `completed` when code, schema or contract updates, tests, and a short outcome note land together.
- If a change spans `schema + runtime + tests`, land it as one sub-slice rather than splitting it across partially merged tasks.
- Any new investigation feature must declare its token budget, deterministic inputs, emitted artifacts, and audit boundary before implementation starts.
- Any planner, retrieval, or review upgrade must remain replayable from canonical run artifacts; do not depend on hidden model-only memory.

## Status Legend

- `completed`: implemented and validated
- `in_progress`: the only active task
- `planned`: approved backlog, not yet active
- `blocked`: cannot proceed without a prior task or a design decision

## Current Baseline

- First-stage layered extraction is complete: `domain/`, `application/`, `adapters/`, and `cli/` all exist and are now the main long-lived destinations.
- The current structure is materially better than the original monolith, but `application/` now contains second-generation hotspots:
  - `application/simulation_workflow.py` (`2164` lines)
  - `application/normalize_sources.py` (`2154` lines)
  - `application/reporting_drafts.py` (`1677` lines)
  - `application/reporting_artifacts.py` (`1644` lines)
  - `application/orchestration_planning.py` (`1582` lines)
- Legacy root facades are thinner but still not fully settled as final public API surfaces:
  - `contract.py` (`2793` lines)
  - `normalize.py` (`1502` lines)
  - `reporting.py` (`902` lines)
  - `supervisor.py` (`839` lines)
  - `orchestrate.py` (`726` lines)
  - `simulate.py` (`68` lines)
- Strongest current capability: stage-machine discipline, run orchestration, and local tamper-evident audit flow.
- Weakest current capability: investigation planning and reasoning still rely mainly on static profiles, fixed gap mappings, deterministic matching heuristics, and compact status aggregation.
- Baseline regression note: the repository was green at `110` tests when this board was reset.

## Execution Order

- Recommended order:
  - `T07.1`
  - `T07.2`
  - `T08.1`
  - `T08.2`
  - `T08.3`
  - `T07.3`
  - `T08.4`
  - `T07.4`
  - `T08.5`
  - `T07.5`
  - `T07.6`
  - `T08.6`
- Rationale:
  - do the minimum structural cleanup first so new investigation work does not land in unstable module boundaries
  - introduce deterministic investigation state before changing retrieval, review, or action selection
  - keep benchmark and audit gates until the end of each major capability block

## Task List

### T07 Structure Optimization And Second-Stage Decomposition

- Status: `planned`
- Goal: move from first-stage extraction into a stable subdomain package layout, remove residual compatibility cycles, and prevent `application/` from becoming a new monolith layer.
- Non-goals:
  - dependency risk cleanup
  - detached skills repository redesign
  - major runtime behavior changes unless required to complete the structural split safely
- Target end state:
  - `src/eco_council_runtime/application/` gains subdomain ownership instead of flat file accumulation:
    - `application/contract/`
    - `application/normalize/`
    - `application/reporting/`
    - `application/orchestration/`
    - `application/simulation/`
    - `application/supervisor/`
    - `application/archive/`
    - `application/investigation/`
  - `src/eco_council_runtime/domain/` grows explicit semantic homes where needed:
    - `domain/investigation/`
    - `domain/evidence/`
    - `domain/matching/`
    - `domain/mission/`
  - `src/eco_council_runtime/adapters/` becomes the permanent home for audit, OpenClaw, storage, and archive bridges rather than leaving those responsibilities under `controller/`
  - root modules become intentionally thin public facades or CLI entrypoints only
  - `controller/` shrinks toward compatibility shims and narrow workflow internals instead of long-lived ownership
- Acceptance criteria:
  - every remaining hotspot has an explicit destination and owner
  - targeted compatibility cycles are removed
  - no newly created extracted module should exceed roughly `1000` lines without an explicit exception note in this board
  - public entrypoints and benchmarked behavior remain stable

#### T07.1 Structural Target Map And Ownership Freeze

- Status: `completed`
- Scope:
  - define the second-stage package map under `application/`, `domain/`, and `adapters/`
  - assign each current hotspot file to concrete destination modules
  - define which root modules remain public facades and which imports are transitional only
  - define the controller exit map for `audit_chain`, OpenClaw helpers, archive import helpers, and supervisor internals
- Acceptance:
  - no hotspot remains without a target destination
  - import ownership is unambiguous enough to guide later slices without re-planning
- Outcome:
  - added `docs/architecture/second-stage-package-map.md` as the concrete `T07.1` ownership freeze for second-stage extraction, including hotspot-to-target mapping, public-facade policy, and controller exit mapping
  - updated `docs/architecture/standalone-controller-target.md` and `src/eco_council_runtime/controller/__init__.py` so the structural target is no longer described as a `T06`-only transitional state
  - added importable second-stage package skeletons under `application/`, `domain/`, and `adapters/`, giving later `T07` slices concrete destinations instead of ad hoc flat-file growth
  - extended package-topology regression coverage to lock the new second-stage package skeleton and keep `controller/` explicitly transitional, and targeted `unittest` regressions passed with `4` tests

#### T07.2 Compatibility-Cycle And Boundary Cleanup

- Status: `completed`
- Scope:
  - remove or isolate remaining compatibility back-import patterns, especially:
    - `application.contract_runtime -> contract`
    - `contract -> application.contract_runtime`
    - `signal_corpus -> supervisor`
    - `case_library -> supervisor`
  - clean stale README or architecture references that still describe transitional boundaries as final
  - ensure new structural work does not need lazy root-module imports to function
- Acceptance:
  - targeted modules import final package homes instead of bouncing through root facades
  - structural docs and runtime imports describe the same boundary model
- Outcome:
  - added `application/archive/runtime_state.py` as the shared archive snapshot helper and rewired `signal_corpus.py` plus `case_library.py` to stop loading the root `supervisor.py` facade for run state, round summaries, and shared payload access
  - added `application/contract/runtime_support.py` so `application/contract_runtime.py` now owns contract assets and round helpers through final package homes, while the remaining root `contract.py` dependency is isolated to narrow lazy validation and source-governance bridge functions
  - cleaned stale CLI boundary wording in `README.md` and `controller/cli.py`, added `tests/test_import_boundaries.py`, and passed targeted `unittest` regressions with `13` tests covering contract runtime, CLI imports, package topology, and the new boundary guards

#### T07.3 Reporting And Orchestration Hotspot Split

- Status: `completed`
- Scope:
  - split `application/reporting_drafts.py` into subdomain modules such as readiness, expert reports, investigation review, and council decision
  - split `application/reporting_artifacts.py` into packet building, prompt rendering, artifact persistence, and promotion flows
  - split `application/orchestration_planning.py` into governance checks, query builders, geometry or window helpers, and step synthesis
- Acceptance:
  - reporting and orchestration services stop depending on second-generation mega-modules
  - direct regression coverage moves with the extracted submodules
- Outcome:
  - `application/reporting_drafts.py`, `application/reporting_artifacts.py`, and `application/orchestration_planning.py` were all reduced to compatibility shells, with owned logic moved into `application/reporting/`, `application/investigation/`, and `application/orchestration/`
  - `T07.3` now validates both extracted-module homes and legacy entrypoints, and the full repository regression baseline advanced from the earlier `130`-test pass to a green `138`-test discovery pass

##### T07.3a Reporting Readiness Extraction

- Status: `completed`
- Scope:
  - move readiness-specific helpers and draft construction out of `application/reporting_drafts.py` into `application/reporting/readiness.py`
  - keep `application/reporting_drafts.py` as a compatibility shell for the extracted readiness entrypoints
  - add direct regressions for the new readiness submodule rather than testing only through the old mega-module
- Acceptance:
  - readiness flows no longer require ownership by `reporting_drafts.py`
  - at least one focused test module imports the new readiness package home directly
- Outcome:
  - added `application/reporting/readiness.py` as the owned readiness home, rewired `application/reporting_drafts.py` to import those builders instead of defining duplicate implementations, and promoted the readiness exports through `application/reporting/__init__.py`
  - added `tests/test_reporting_readiness.py` so readiness flows are now exercised directly through `eco_council_runtime.application.reporting` instead of only through the old mega-module
  - targeted `unittest` regressions for readiness/reporting compatibility stayed green, and the later full `130`-test discovery pass remained green after the follow-on `T07.3b` extraction

##### T07.3b Expert Report, Investigation Review, And Decision Split

- Status: `completed`
- Scope:
  - move expert-report, investigation-review, and council-decision builders from `application/reporting_drafts.py` into owned `application/reporting/` or `application/investigation/` modules
  - keep compatibility re-exports in the old file until downstream imports have settled
- Acceptance:
  - `application/reporting_drafts.py` shrinks to a thin compatibility shell over extracted reporting builders
- Outcome:
  - added `application/reporting/common.py`, `application/reporting/recommendations.py`, `application/reporting/expert_reports.py`, `application/reporting/council_decision.py`, and `application/investigation/review.py` so expert reporting, review, and decision logic now live in owned second-stage module homes
  - reduced `application/reporting_drafts.py` to a `111`-line compatibility shell, and updated `application/reporting/__init__.py` plus `application/investigation/__init__.py` so callers can use the new package homes directly
  - added `tests/test_reporting_extracted_modules.py`; a focused `39`-test `unittest` regression set passed, and `python3 -m unittest discover -s tests` passed with `130` tests

##### T07.3c Reporting Artifact Pipeline Split

- Status: `completed`
- Scope:
  - split `application/reporting_artifacts.py` into packet loading, prompt rendering, artifact persistence, and promotion flows under `application/reporting/`
  - move direct regressions with those extracted homes
- Acceptance:
  - packet or prompt services stop depending on a single reporting-artifacts mega-module
- Outcome:
  - added `application/reporting/artifact_support.py`, `application/reporting/packets.py`, `application/reporting/prompts.py`, `application/reporting/promotion.py`, and `application/reporting/artifact_pipeline.py`, then reduced `application/reporting_artifacts.py` to a thin compatibility shell while moving owned logic into second-stage package homes
  - added `tests/test_reporting_artifact_modules.py` and kept `tests/test_reporting_artifacts.py` as the compatibility regression surface so packet, prompt, promotion, and artifact-pipeline behavior are now exercised both directly and through the legacy entrypoint
  - reporting-focused `unittest` regressions passed with `20` tests, and the later combined `53`-test `T07.3` regression set plus full `138`-test discovery pass stayed green

##### T07.3d Orchestration Planning Split

- Status: `completed`
- Scope:
  - split `application/orchestration_planning.py` into governance checks, query builders, geometry helpers, and step synthesis under `application/orchestration/`
  - keep fetch-plan behavior stable while moving ownership to second-stage package homes
- Acceptance:
  - orchestration planning no longer depends on a single second-generation mega-module
- Outcome:
  - added `application/orchestration/governance.py`, `application/orchestration/query_builders.py`, `application/orchestration/geometry.py`, `application/orchestration/step_synthesis.py`, and `application/orchestration/fetch_plan_builder.py`, then reduced `application/orchestration_planning.py` to a compatibility shell and rewired `application/orchestration_execution.py` to import second-stage package homes directly
  - added `tests/test_orchestration_extracted_modules.py` and updated planning regressions so the extracted orchestration modules are tested directly while legacy import surfaces remain covered
  - resolved the post-split `application.reporting` package import cycle by converting `application/reporting/__init__.py` to lazy exports, after which orchestration-focused `unittest` regressions passed with `15` tests, the combined `T07.3` regression set passed with `53` tests, and `python3 -m unittest discover -s tests` passed with `138` tests

#### T07.4 Normalize, Simulation, And Archive Hotspot Split

- Status: `planned`
- Scope:
  - split `application/normalize_sources.py` into public-source, environment-source, and cache-wrapper or ingestion helper modules
  - split `application/simulation_workflow.py` into preset loading, raw artifact generation, synthetic payload builders, and workflow runners
  - move cross-run archive responsibilities toward `application/archive/` plus dedicated archive adapters so `signal_corpus.py` and `case_library.py` stop depending on `supervisor.py`
- Acceptance:
  - normalize, simulation, and archive flows have stable subdomain boundaries
  - archive import code no longer requires root `supervisor` module loading

#### T07.5 Root Facade Contraction And Controller Retirement Pass

- Status: `planned`
- Scope:
  - shrink root facades to intentional public API wrappers only
  - reduce `controller/` to documented compatibility shims or clearly bounded workflow internals
  - update README and architecture docs to reflect the final package layout after the second-stage split
- Acceptance:
  - root modules are visibly thin
  - `controller/` is no longer treated as a permanent home for new work
  - docs reflect the actual production boundaries

#### T07.6 Structural Regression Gate

- Status: `planned`
- Scope:
  - add topology or import-boundary regressions where useful
  - lock compatibility entrypoints that must remain stable
  - add guardrails against reintroducing the cleaned compatibility cycles
- Acceptance:
  - the second-stage structure is test-guarded rather than documented only
  - future refactors cannot easily drift back into flat mega-modules

### T08 Investigation Capability Upgrade Under Controlled Token And Auditable Flow

- Status: `planned`
- Goal: improve investigation quality from static heuristic routing toward evidence-state-guided planning, while keeping token use bounded and the full process replayable and auditable.
- Hard constraints:
  - deterministic state first, optional model assistance second
  - every new investigation artifact must be persisted to canonical run paths before it can influence later stages
  - every new planner or retrieval step must expose its inputs, selection reasons, and budget counters
  - source governance remains authoritative; the planner may recommend only mission-governed families, layers, or explicitly permitted skills
  - prompt-facing investigation context must be compact structured summaries rather than raw artifact dumps
- Initial budget targets:
  - max primary hypotheses materialized per round: `3`
  - max alternative hypotheses per primary hypothesis: `2`
  - max ranked next actions per planning pass: `6`
  - max historical cases surfaced to a role in one pass: `3`
  - max excerpt blocks per retrieved case: `2`
  - new retrieval or planning packets must emit a bounded token estimate or size counter alongside content
- Acceptance criteria:
  - planning no longer depends only on fixed profile and gap mappings
  - investigation review compares competing explanations rather than only aggregating leg statuses
  - retrieval becomes more useful without allowing uncontrolled context growth
  - all new state is auditable, replayable, and benchmarked

#### T08.1 Investigation State Artifact And Replay Model

- Status: `completed`
- Scope:
  - add a deterministic `shared/investigation_state.json`
  - derive it from canonical artifacts such as `investigation_plan`, matching outputs, evidence adjudication, curated evidence, and moderator review artifacts
  - track per hypothesis, per leg, and per alternative:
    - support
    - contradiction
    - coverage
    - remaining gaps
    - uncertainty
    - latest evidence refs
    - last update stage or round
- Acceptance:
  - investigation state can be rebuilt from canonical artifacts without hidden memory
  - later planners and reviews read this state instead of inferring everything from scratch each time
- Outcome:
  - added `application/investigation/state.py` plus canonical `controller.paths.investigation_state_path`, and scaffold or runtime refresh paths now materialize `shared/investigation_state.json` deterministically from canonical round artifacts
  - round-state loading, compact context views, investigation-review packets, and decision packets now read the persisted investigation state first instead of relying only on ad hoc recomputation
  - import, normalize, and match audit receipts now snapshot `investigation_state` at the points where it becomes materially influential for later review or planning
  - added focused regression coverage for scaffold persistence, investigation-state assembly, reporting-state exposure, packet loading, and audit-chain coverage; targeted `unittest` regressions passed with `29` tests

#### T08.2 Budgeted Next-Action Planner

- Status: `completed`
- Scope:
  - add a machine-readable `shared/investigation_actions.json`
  - derive candidate actions from unresolved required legs, contradiction signals, alternative hypotheses, and governed source options
  - score actions using explicit components such as:
    - expected evidence gain
    - contradiction resolution value
    - coverage gain
    - token or context cost penalty
    - audit clarity
  - keep the output bounded to the configured top actions only
- Acceptance:
  - the runtime can explain why one next action outranks another
  - action selection becomes evidence-state-aware instead of only gap-template-driven
- Outcome:
  - added deterministic `investigation_actions` planning, canonical persistence, receipt or packet exposure, and recommendation-flow migration so review, expert-report, and decision drafting now prefer persisted ranked actions before falling back to flat gap templates
  - completed focused plus cross-module regressions for planner scoring, runtime materialization, report or decision consumption, audit coverage, import boundaries, and CLI stability; final `unittest` regression passed with `41` tests

##### T08.2a Action Artifact Contract And Deterministic Scoring

- Status: `completed`
- Scope:
  - define the canonical `shared/investigation_actions.json` structure and `controller.paths` home
  - build a deterministic planner that consumes `investigation_state` plus mission governance and emits at most `6` ranked actions with explicit score components and compact budget counters
  - keep the first slice read-only with respect to later runtime behavior: produce the planner artifact and focused tests before replacing existing recommendation flows
- Acceptance:
  - one pure planner path can be exercised from local test fixtures without OpenClaw or hidden state
  - every returned action includes enough fields to audit ranking inputs, governed source options, and budget penalties
- Outcome:
  - added `application/investigation/actions.py`, canonical `controller.paths.investigation_actions_path`, deterministic ranking logic, governed source-option expansion, and focused planner regressions for scoring, truncation, governance, and budget counters

##### T08.2b Runtime And Packet Integration

- Status: `completed`
- Scope:
  - materialize `investigation_actions.json` at the canonical round write points that already refresh `investigation_state`
  - expose compact action summaries through round context and relevant review or decision packets
  - keep packet-facing action context bounded and source-governed
- Acceptance:
  - later planners, moderator review, and decision drafting can read the persisted action artifact without re-ranking from scratch
- Outcome:
  - scaffold, normalize refresh, stage imports, audit receipts, reporting state or views, and reporting artifacts now materialize or expose `investigation_actions` alongside `investigation_state` so later stages consume the persisted artifact rather than recomputing hidden planning state

##### T08.2c Recommendation-Flow Migration And Regression Gate

- Status: `completed`
- Scope:
  - replace the most important `combine_recommendations`-only fallbacks with action-artifact consumption where appropriate
  - add regression coverage for ranking explanations, top-k truncation, governance filtering, and budget counters
  - ensure audit-chain coverage remains valid when `investigation_actions.json` becomes influential
- Acceptance:
  - the runtime no longer depends only on flat missing-type templates when proposing next actions
  - ranking drift, governance drift, or budget drift fail deterministically in tests
- Outcome:
  - `application/reporting_drafts.py` now prioritizes persisted `investigation_actions` for investigation review, expert reports, and decision next-round planning, while keeping report-derived and missing-type recommendations as auditable fallback layers
  - added regression coverage in `tests/test_reporting_drafts.py` for review, report, and decision consumption of persisted actions, and confirmed the broader `41`-test `unittest` regression set remains green

#### T08.3 Retrieval V2 With Compact History Evidence

- Status: `completed`
- Scope:
  - upgrade historical retrieval from case-level overlap scoring toward compact artifact-level support
  - keep structured filters, but add bounded retrieval of report, decision, evidence-card, or curated-summary snippets
  - persist a machine-readable retrieval snapshot alongside the current human-readable history context so retrieval remains auditable
  - ensure weak lexical overlap cannot dominate retrieval when structured mismatch is strong
- Acceptance:
  - retrieved history is more directly useful for investigation planning
  - retrieval remains compact, explainable, and safe under the budget caps
- Outcome:
  - added `application/investigation/history_context.py` as the snapshot-backed retrieval service, with canonical `shared/history_retrieval.json`, bounded case or excerpt budgets, and compact moderator-facing rendering built from persisted retrieval state rather than an ephemeral CLI response
  - upgraded case-library search ranking so structured overlap tiers are explicit and strong structured matches outrank weak lexical matches, then added artifact-level excerpt selection for decision, round, report, evidence-card, and curated-summary snippets
  - routed supervisor history-context generation through the new service, exposed the snapshot path in operator status, and passed focused plus cross-module `unittest` regressions with `51` tests

##### T08.3a Retrieval Snapshot Contract And Budget Counters

- Status: `completed`
- Scope:
  - define a canonical machine-readable retrieval snapshot for each round alongside the human-readable history context output
  - record deterministic query inputs, selected cases, selection reasons, per-case rank or score, and bounded size or token counters
  - keep the first slice read-only with respect to downstream prompt wording beyond switching the writer to the new snapshot-backed path
- Acceptance:
  - retrieval decisions become replayable from persisted artifacts instead of only a rendered text file
  - size, count, and truncation behavior are explicit in the emitted snapshot
- Outcome:
  - added canonical `shared/history_retrieval.json` with deterministic query inputs, case ranking details, per-case excerpt budgets, and snapshot-level token or size counters, plus round-path support in `controller.paths`

##### T08.3b Artifact-Level Excerpt Retrieval

- Status: `completed`
- Scope:
  - add bounded retrieval of compact decision, report, evidence, or round-summary excerpts for each matched historical case
  - cap surfaced cases to `3` and excerpt blocks per case to `2`, with explicit relevance reasons
  - make structured overlap outweigh weak lexical similarity when selecting both cases and excerpts
- Acceptance:
  - retrieved history includes directly useful artifact snippets rather than only case-level metadata
  - irrelevant lexical matches cannot dominate strong structured matches
- Outcome:
  - added deterministic excerpt candidate assembly and ranking for decision summaries, round summaries, report summaries, evidence cards, and curated claim summaries, with at most `2` excerpts per case and explicit relevance reasons or token estimates
  - strengthened case-ranking output with `score_components.match_tier` and structured-score-first ordering so weak lexical overlap no longer outranks materially stronger structured matches

##### T08.3c Runtime Integration And Regression Gate

- Status: `completed`
- Scope:
  - route supervisor history-context generation through the new snapshot-backed retrieval service
  - keep moderator prompt context compact while preserving the machine-readable snapshot for audit and replay
  - add regression coverage for snapshot persistence, excerpt bounding, and degraded-match filtering
- Acceptance:
  - history context and audit snapshot stay consistent
  - retrieval regressions fail deterministically in local tests
- Outcome:
  - rewired `controller/state_config.write_history_context_file` and `controller/operator_surface` to materialize and expose the snapshot-backed history context, while keeping moderator prompt ingestion unchanged at the text-file boundary
  - extended `tests/test_investigation_retrieval_upgrade.py` to cover structured-overlap ranking, snapshot persistence, excerpt caps, and rendered context output, and confirmed the broader `51`-test regression set stays green

#### T08.4 Competing-Hypothesis Review And Decision Gating

- Status: `completed`
- Scope:
  - upgrade investigation review so it compares primary and alternative explanations explicitly
  - surface contradiction paths, not just support paths
  - make review outputs explain whether another round is needed because of unresolved required legs, unresolved alternatives, or contradictory evidence
- Acceptance:
  - moderator review becomes a comparative causal assessment rather than mostly a leg-status rollup
  - decision drafting can consume clearer uncertainty and contradiction signals
- Outcome:
  - upgraded `application/investigation/review.py` so hypothesis review now emits comparative assessment fields, contradiction paths, alternative-review state, and explicit `decision_gating` / `another_round_required` signals instead of relying only on causal-leg status rollups
  - updated `application/reporting/council_decision.py` so moderator decisions now honor persisted review gating when deciding whether another round is required, including contradiction and alternative-pressure cases that do not surface as remands or flat missing-type lists
  - fixed the latent `application/investigation/state.py` compatibility-shell import cycle while keeping review/state semantics aligned, and added focused regressions in `tests/test_reporting_drafts.py` plus `tests/test_reporting_extracted_modules.py` for competing-hypothesis review and no-remand decision gating; targeted `20`-test regressions and full `142`-test `unittest` discovery both passed

#### T08.5 Governance-Aware Discovery Or Probe Mode

- Status: `planned`
- Scope:
  - allow limited exploratory investigation when current evidence is insufficient or atypical
  - constrain exploration by mission governance, explicit budgets, and auditable reason codes
  - keep discovery outputs as recommendations or probe requests rather than free-form uncontrolled source execution
- Acceptance:
  - recall improves on non-template missions without opening an unbounded fetch surface
  - every exploratory step remains reviewable and replayable

#### T08.6 Evaluation, Token-Budget, And Audit Regression Gate

- Status: `planned`
- Scope:
  - expand benchmarks for ambiguous attribution, false causal chains, contradictory evidence, low-evidence rounds, and atypical missions
  - add regression checks for token-budget envelopes, retrieval compactness, and deterministic replay of investigation artifacts
  - ensure any new investigation artifacts are included in validation and audit-chain coverage where appropriate
- Acceptance:
  - new investigation behavior has scenario-level regression coverage
  - budget drift and audit regressions fail deterministically

## Current Task Notes

- Active task: none
- Next planned task: `T07.4 Normalize, Simulation, And Archive Hotspot Split`
- Working rule reaffirmed: after a sub-slice passes acceptance, persist its outcome here and then explicitly open the next sub-slice before coding continues.
