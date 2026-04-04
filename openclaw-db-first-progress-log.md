# OpenClaw DB-First Progress Log

This log records independently deliverable increments that move the project from the current runtime route toward the `db first / openclaw agent mode` target.

Planning source of truth:
- `openclaw-db-first-master-plan.md`

Use this log only for delivered increments and historical detail.
Use the master plan for:
- route definitions
- normalized stage numbering
- future sequencing
- historical crosswalks

Normalization note:
- Historical `B2 / B2.1` entries remain unchanged in this log, but they are treated as `C1 / C1.1` in the master plan because they belong to the analysis-plane route semantically.

Reference blueprints:
- `openclaw-first-refactor-blueprint.md`
- `openclaw-db-first-agent-runtime-blueprint.md`
- `openclaw-db-first-master-plan.md`

## 2026-04-02 A1: Review Fix Pack

Status: completed

Objective:
- Remove source-role drift in round opening.
- Enforce `max_source_steps_per_round` where fetch plans are actually built.
- Restore the board-delta contract expected by board workflow consumers.

Implementation:
- `skills/eco-open-investigation-round/scripts/eco_open_investigation_round.py`
  - Replaced hardcoded source-role grouping with the shared source catalog via `source_role(...)`.
- `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_planner.py`
  - Applied `max_source_steps_per_round` during fetch-plan construction instead of leaving it as passive governance metadata.
- `skills/eco-read-board-delta/scripts/eco_read_board_delta.py`
  - Restored cursor-safe delta reads and expanded the payload with active round state fields.

Validation:
- `python3 -m unittest tests/test_source_queue_governance.py -q`
- `python3 -m unittest tests/test_board_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_source_queue_governance.py`
  - Verifies `build_fetch_plan(...)` respects `max_source_steps_per_round`.
- `tests/test_board_workflow.py`
  - Verifies board delta reads expose active round state.
  - Verifies `eco-open-investigation-round` fallback task generation uses the shared source-role catalog.

Known limitations:
- `eco-read-board-delta` still syncs from board JSON first; it does not yet read from a DB-first write path.

Next:
- Move more board-facing consumers onto the deliberation plane without breaking current JSON compatibility.

## 2026-04-02 B1: Deliberation Plane Bootstrap

Status: completed

Objective:
- Create the first runtime-visible deliberation plane so board state can be queried as structured DB state instead of only JSON snapshots.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/deliberation_plane.py`
  - Added SQLite-backed deliberation tables for `board_events`, `board_notes`, `hypothesis_cards`, `challenge_tickets`, `board_tasks`, and `round_transitions`.
  - Added sync helpers to project existing board JSON into the deliberation plane.
  - Added round event and round state read helpers for board consumers.
  - Added an `event_index` migration and ordering rule so cursor reads preserve board append order even when multiple events share the same timestamp.
- Synced board mutations into the deliberation plane after JSON writes in:
  - `skills/eco-post-board-note/scripts/eco_post_board_note.py`
  - `skills/eco-update-hypothesis-status/scripts/eco_update_hypothesis_status.py`
  - `skills/eco-open-challenge-ticket/scripts/eco_open_challenge_ticket.py`
  - `skills/eco-close-challenge-ticket/scripts/eco_close_challenge_ticket.py`
  - `skills/eco-claim-board-task/scripts/eco_claim_board_task.py`
  - `skills/eco-open-investigation-round/scripts/eco_open_investigation_round.py`
- `skills/eco-read-board-delta/scripts/eco_read_board_delta.py`
  - Now returns `round_state` and `deliberation_sync` in addition to the event slice and board handoff payload.

Validation:
- `python3 -m unittest tests/test_board_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Known limitations:
- Board JSON remains the current source of truth.
- The deliberation plane reuses `analytics/signal_plane.sqlite` as a transitional local SQLite surface instead of introducing a separate dedicated DB file.
- Sync is currently full-run rebuild by `run_id`, which is acceptable for local runs but not yet optimized for large boards.

Next:
- Convert selected board readers and planners to query the deliberation plane directly before moving board writes to DB-first primary mutations.

## 2026-04-02 B1.1: Board Read Path Migration

Status: completed

Objective:
- Move board-facing read consumers from direct JSON-only reads to a shared deliberation-plane snapshot path while preserving compatible fallbacks.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/deliberation_plane.py`
  - Added `load_round_snapshot(...)` so board readers can share one sync-plus-read path for round state and event history.
- `skills/eco-summarize-board-state/scripts/eco_summarize_board_state.py`
  - Now summarizes the round from the deliberation plane instead of manually slicing board JSON.
  - Emits `state_source`, `db_path`, and `deliberation_sync` for artifact traceability.
- `skills/eco-plan-round-orchestration/scripts/eco_plan_round_orchestration.py`
  - Now derives board posture from the deliberation plane first.
  - Keeps `board_state_summary` as a compatible advisory fallback instead of the primary state source.
  - Emits `board_state_source`, `db_path`, and `deliberation_sync` in planner outputs.

Validation:
- `python3 -m unittest tests/test_board_workflow.py -q`
- `python3 -m unittest tests/test_orchestration_planner_workflow.py -q`
- `python3 -m unittest tests/test_investigation_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_board_workflow.py`
  - Verifies board summary payloads are marked as `deliberation-plane` backed.
- `tests/test_orchestration_planner_workflow.py`
  - Verifies planner outputs remain `deliberation-plane` backed even when no board summary artifact exists yet.

Known limitations:
- `eco-materialize-board-brief` still consumes the board summary artifact rather than reading the deliberation plane directly.
- Planner still reads `next_actions`, `probes`, and `readiness` from file artifacts; this increment only migrates the board-state read path.

Next:
- Migrate `eco-materialize-board-brief` and then one more planner-side consumer so more of the moderator loop can read from the deliberation plane without depending on precomputed JSON snapshots.

## 2026-04-02 B1.2: Moderator Handoff And Readiness Migration

Status: completed

Objective:
- Move the remaining moderator handoff and readiness readers off precomputed board-summary dependence and onto the shared deliberation-plane snapshot path.

Implementation:
- `skills/eco-materialize-board-brief/scripts/eco_materialize_board_brief.py`
  - Now materializes the board brief from deliberation-plane round state.
  - Keeps `board_state_summary` only as a compatible fallback when board state is unavailable.
  - Emits `state_source`, `db_path`, and `deliberation_sync`.
- `skills/eco-summarize-round-readiness/scripts/eco_summarize_round_readiness.py`
  - Now evaluates board readiness from deliberation-plane state first.
  - Keeps `board_state_summary` as an advisory fallback instead of a required primary input.
  - Emits `board_state_source`, `db_path`, `deliberation_sync`, and input-presence flags for traceability.

Validation:
- `python3 -m unittest tests/test_board_workflow.py -q`
- `python3 -m unittest tests/test_investigation_workflow.py -q`
- `python3 -m unittest tests/test_orchestration_planner_workflow.py -q`
- `python3 -m unittest tests/test_supervisor_simulation_regression.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_board_workflow.py`
  - Verifies board brief generation is `deliberation-plane` backed.
  - Verifies board brief generation works without a precomputed board summary artifact.
- `tests/test_investigation_workflow.py`
  - Verifies round readiness can return `ready` from deliberation-plane state even when `board_summary` and `board_brief` artifacts are absent.

Known limitations:
- `eco-propose-next-actions` still consumes board summary and board brief artifacts as its primary board context.
- `eco-summarize-round-readiness` still reads `next_actions`, `probes`, and `coverage` from file artifacts; only the board-state side is migrated in this increment.

Next:
- Migrate `eco-propose-next-actions` to deliberation-plane-first board reads, then revisit whether `board_summary` should remain a generated convenience artifact rather than a planning dependency.

## 2026-04-02 B1.3: Next-Action Deliberation Migration

Status: completed

Objective:
- Remove `eco-propose-next-actions` from primary dependence on precomputed board summary artifacts so the D1 planning surface can read shared deliberation state directly.

Implementation:
- `skills/eco-propose-next-actions/scripts/eco_propose_next_actions.py`
  - Now reads board posture from the deliberation plane first through `load_round_snapshot(...)`.
  - Keeps `board_state_summary` as a compatible advisory fallback instead of the primary board source.
  - Emits `board_state_source`, `db_path`, `deliberation_sync`, and input-presence flags in the action artifact for traceability.
- No change was needed in `eco-open-falsification-probe`; the existing next-actions artifact contract remained stable for downstream consumers.

Validation:
- `python3 -m unittest tests/test_investigation_workflow.py -q`
- `python3 -m unittest tests/test_supervisor_simulation_regression.py -q`
- `python3 -m unittest tests/test_orchestration_planner_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_investigation_workflow.py`
  - Verifies next-action generation is marked as `deliberation-plane` backed.
  - Verifies `next_actions -> falsification_probes` still works when `board_summary` and `board_brief` artifacts do not exist.

Known limitations:
- `eco-propose-next-actions` still optionally uses `board_brief` text when present as concise human context.
- The D1 surface still reads evidence coverage from file artifacts; only the board-state side is migrated in this increment.

Next:
- Decide whether `eco-open-falsification-probe` should stay artifact-driven or grow a direct deliberation/analysis-plane query path.
- Revisit whether `board_brief` should remain optional context only, instead of an expected upstream step in planner-oriented flows.

## 2026-04-02 B1.4: Probe Source Decoupling

Status: completed

Objective:
- Remove the hard runtime dependence of `eco-open-falsification-probe` on a preexisting `next_actions` artifact while keeping probe generation semantics aligned with the D1 action planner.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/investigation_planning.py`
  - Added a shared runtime helper for D1 action planning.
  - Centralized board-state snapshot fallback, action ranking, and action-context loading from deliberation-plane state plus evidence coverage.
- `skills/eco-propose-next-actions/scripts/eco_propose_next_actions.py`
  - Now delegates action ranking to the shared runtime helper instead of maintaining an isolated local implementation.
- `skills/eco-open-falsification-probe/scripts/eco_open_falsification_probe.py`
  - Still consumes `next_actions` when the artifact exists.
  - Rebuilds ranked actions from the shared helper when the artifact is absent.
  - Emits `action_source`, `board_state_source`, `db_path`, `deliberation_sync`, and input-presence flags for traceability.

Validation:
- `python3 -m unittest tests/test_investigation_workflow.py -q`
- `python3 -m unittest tests/test_supervisor_simulation_regression.py -q`
- `python3 -m unittest tests/test_orchestration_planner_workflow.py -q`
- `python3 -m unittest tests/test_board_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_investigation_workflow.py`
  - Verifies probe generation still records `next-actions-artifact` as the action source on the normal path.
  - Verifies probe generation can rebuild candidates directly from deliberation-plane state when the `next_actions` artifact is missing.

Known limitations:
- `eco-open-falsification-probe` still consumes evidence coverage through the file artifact layer rather than querying an analysis plane directly.
- The shared D1 planning helper currently lives in runtime Python only; it is not yet surfaced as a first-class reusable contract for non-Python tooling.

Next:
- Decide whether to move D1 coverage reads off file artifacts and into a dedicated analysis-plane query surface.
- Revisit whether `next_actions` should remain a durable artifact only, rather than an expected operational prerequisite for downstream probe generation.

## 2026-04-02 D1: Documentation Traceability Pack

Status: completed

Objective:
- Keep each delivered increment traceable in-repo and remove blueprint reference drift from skill docs.

Implementation:
- Added this file: `openclaw-db-first-progress-log.md`
- Replaced stale `../../openclaw-skill-phase-plan.md` references in skill docs with `../../openclaw-db-first-agent-runtime-blueprint.md`
- Updated `skills/eco-read-board-delta/SKILL.md` to match the current output contract.

Validation:
- `rg -n "openclaw-skill-phase-plan\\.md" skills -g 'SKILL.md'`

Next:
- Keep appending one section per independent delivery so blueprint work remains auditable across turns.

## 2026-04-02 B2: Coverage Analysis Plane Query Surface

Status: completed

Objective:
- Move evidence coverage off direct JSON-only consumption for the D1 and promotion path while preserving the existing artifact contract as a compatible export.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py`
  - Added a transitional analysis-plane surface in `analytics/signal_plane.sqlite`.
  - Added `analysis_result_sets` and `analysis_result_items` tables plus helpers to sync and query `evidence_coverage` results.
- `skills/eco-score-evidence-coverage/scripts/eco_score_evidence_coverage.py`
  - Now syncs the coverage artifact into the analysis plane after JSON export.
  - Emits `db_path` and `analysis_sync` for traceability.
- `eco-concil-runtime/src/eco_council_runtime/kernel/investigation_planning.py`
  - Replaced direct coverage-file reads with the shared analysis-plane helper.
  - Emits `coverage_source`, `coverage_file`, and `analysis_sync` in shared D1 action context.
- `skills/eco-propose-next-actions/scripts/eco_propose_next_actions.py`
  - Now exposes coverage trace fields from the shared analysis-plane-backed action context.
- `skills/eco-open-falsification-probe/scripts/eco_open_falsification_probe.py`
  - Now preserves `coverage_source` and `analysis_sync` when probes are rebuilt from shared D1 context.
- `skills/eco-summarize-round-readiness/scripts/eco_summarize_round_readiness.py`
  - Now loads coverage posture from the shared analysis plane first.
  - Continues to work when the `evidence_coverage` JSON artifact is absent but the synced result set exists.
- `skills/eco-promote-evidence-basis/scripts/eco_promote_evidence_basis.py`
  - Now selects promotion-ready coverage objects from the shared analysis plane first.
  - Emits `coverage_source`, `db_path`, and `analysis_sync`.

Validation:
- `python3 -m unittest tests/test_analysis_workflow.py -q`
- `python3 -m unittest tests/test_investigation_workflow.py -q`
- `python3 -m unittest discover -s tests -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_analysis_workflow.py`
  - Verifies `eco-score-evidence-coverage` syncs into `analysis_result_sets` and `analysis_result_items`.
  - Verifies custom coverage output paths are also recorded in the analysis plane.
- `tests/test_investigation_workflow.py`
  - Verifies probe fallback can rebuild from deliberation state when both `next_actions` and the coverage JSON artifact are absent.
  - Verifies D1 action planning, readiness, and promotion continue from the analysis plane when `analytics/evidence_coverage_<round_id>.json` is missing.

Known limitations:
- Only `evidence_coverage` is surfaced through the new analysis plane; other analysis artifacts still remain JSON-first.
- The analysis-plane result-set contract is still runtime-local Python infrastructure and is not yet exposed as a formal cross-tooling API.

Next:
- Extend the same analysis-plane query surface to other high-value analysis artifacts such as links, scopes, or promotion-ready result sets.
- Decide whether additional reporting/export consumers should stop reading coverage-export JSON directly and rely on analysis-plane queries instead.

## 2026-04-03 B2.1: Coverage Upstream Analysis Migration

Status: completed

Objective:
- Move `eco-score-evidence-coverage` off direct JSON-only dependence on upstream links and scope artifacts so coverage scoring can continue from the shared analysis plane.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py`
  - Generalized the transitional analysis-plane helper beyond coverage.
  - Added shared sync/load wrappers for:
    - `claim-observation-link`
    - `claim-scope`
    - `observation-scope`
    - existing `evidence-coverage`
- `skills/eco-link-claims-to-observations/scripts/eco_link_claims_to_observations.py`
  - Now syncs link results into the analysis plane after JSON export.
  - Emits `db_path` and `analysis_sync`.
- `skills/eco-derive-claim-scope/scripts/eco_derive_claim_scope.py`
  - Now syncs claim-scope results into the analysis plane after JSON export.
  - Emits `db_path` and `analysis_sync`.
- `skills/eco-derive-observation-scope/scripts/eco_derive_observation_scope.py`
  - Now syncs observation-scope results into the analysis plane after JSON export.
  - Emits `db_path` and `analysis_sync`.
- `skills/eco-score-evidence-coverage/scripts/eco_score_evidence_coverage.py`
  - Now loads links, claim scopes, and observation scopes from the shared analysis plane first.
  - Falls back to the JSON artifacts only when no synced result set is available.
  - Emits upstream source trace fields, input-presence flags, and `input_analysis_sync`.

Validation:
- `python3 -m unittest tests/test_analysis_workflow.py -q`
- `python3 -m unittest tests/test_investigation_workflow.py -q`

Tests added or extended:
- `tests/test_analysis_workflow.py`
  - Verifies link, claim-scope, observation-scope, and coverage result sets are all present in `analysis_result_sets`.
  - Verifies coverage can still run after deleting `claim_observation_links`, `claim_scope_proposals`, and `observation_scope_proposals` JSON artifacts.
- Existing custom-path coverage checks now also confirm custom claim-scope and observation-scope artifact paths are recorded in the analysis plane.

Known limitations:
- `eco-materialize-history-context` and `eco-archive-case-library` still read claim/observation scope artifacts directly instead of querying the shared analysis plane.
- The analysis-plane result-set helper is still local runtime infrastructure rather than a formal public contract for non-Python tooling.

Next:
- Migrate history/archive consumers that still read scope artifacts directly onto the shared analysis-plane helper.
- Decide whether `claim_observation_links` itself should become an operational query surface for more downstream reasoning beyond coverage scoring.

## 2026-04-03 D2: Master Plan And Route Normalization

Status: completed

Objective:
- Stop using the progress log itself as the place where route semantics are inferred.
- Define one stable A/B/C/D route taxonomy and a near-term development sequence that can be used to coordinate future work.

Implementation:
- Added `openclaw-db-first-master-plan.md`
  - Defined the meaning of `A / B / C / D`.
  - Defined normalized stage numbering and stage status semantics.
  - Added a historical crosswalk so early log items can be interpreted consistently.
  - Added a full route-level development plan plus the recommended next several increments.
- Updated this file:
  - Added a planning-source notice that points future sequencing and route meaning to the master plan.
  - Added a normalization note explaining why historical `B2 / B2.1` should now be interpreted as `C1 / C1.1`.

Validation:
- `rg -n "Route Legend|历史编号归一化说明|推荐的未来数次开发顺序" openclaw-db-first-master-plan.md`
- `rg -n "Planning source of truth|Normalization note" openclaw-db-first-progress-log.md`

Known limitations:
- The master plan is still a planning/control document; it does not replace the detailed implementation history kept in this log.
- Some future stages are intentionally broad and will still need to be split into turn-sized independent deliveries when work starts.

Next:
- Use `openclaw-db-first-master-plan.md` as the only source for route definitions and forward scheduling.
- Continue the next implementation increment from `C1.2`, then append the delivery detail here after completion.

## 2026-04-03 C1.2: History / Archive Read Migration

Status: completed

Objective:
- Move history/archive consumers off direct JSON-first reads for scopes and coverage so they can continue from the shared analysis plane.

Implementation:
- `skills/eco-materialize-history-context/scripts/eco_materialize_history_context.py`
  - Replaced direct claim/observation scope JSON reads with analysis-plane-first loading via the shared runtime helper.
  - Added `claim_scope_source`, `observation_scope_source`, `analysis_db_path`, `observed_inputs`, and `input_analysis_sync` to the retrieval artifact and skill summary for traceability.
- `skills/eco-archive-case-library/scripts/eco_archive_case_library.py`
  - Replaced direct claim-scope, observation-scope, and coverage JSON reads with analysis-plane-first loading via the shared runtime helper.
  - Added source-trace fields, input-presence flags, and `input_analysis_sync` to the archive snapshot and skill summary while preserving the existing archive DB contract.
- `tests/test_archive_history_workflow.py`
  - Extended archive/history regression coverage to delete scope and coverage JSON artifacts before execution and verify both skills continue from analysis-plane state.

Validation:
- `python3 -m unittest tests/test_archive_history_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_archive_history_workflow.py`
  - Verifies `eco-archive-case-library` continues from analysis-plane state after deleting `claim_scope_proposals`, `observation_scope_proposals`, and `evidence_coverage` JSON artifacts.
  - Verifies `eco-materialize-history-context` continues from analysis-plane state after deleting current-round claim/observation scope JSON artifacts.

Known limitations:
- `eco-materialize-history-context` and `eco-archive-case-library` still assemble the rest of their context from board/reporting/promotion artifacts; this increment only migrates the analysis-side scope/coverage reads.
- The analysis-plane helper is still runtime-local Python infrastructure rather than a formal cross-tool query contract.

Next:
- Continue with `C1.3` by migrating remaining reporting/export consumers that still read analysis JSON as their primary input.
- Revisit whether the analysis-plane helper should expose a more formal query surface for non-Python tooling.

## 2026-04-03 C1.3: Normalization Audit Analysis Read Migration

Status: completed

Objective:
- Remove the remaining board-facing normalization audit dependence on candidate-export JSON so audit generation can continue from shared analysis-plane state.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py`
  - Added transitional result-set support for:
    - `claim-candidate`
    - `observation-candidate`
  - Added shared sync/load wrappers for claim and observation candidate results.
- `skills/eco-extract-claim-candidates/scripts/eco_extract_claim_candidates.py`
  - Now syncs claim candidates into the shared analysis plane after JSON export.
  - Emits `db_path` and `analysis_sync`.
- `skills/eco-extract-observation-candidates/scripts/eco_extract_observation_candidates.py`
  - Now syncs observation candidates into the shared analysis plane after JSON export.
  - Emits `db_path` and `analysis_sync`.
- `skills/eco-build-normalization-audit/scripts/eco_build_normalization_audit.py`
  - Now loads claim and observation candidates from the shared analysis plane first.
  - Falls back to candidate JSON artifacts only when no synced result set is available.
  - Emits candidate source trace fields, input-presence flags, `db_path`, and `input_analysis_sync`.

Validation:
- `python3 -m unittest tests/test_analysis_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_analysis_workflow.py`
  - Verifies claim-candidate and observation-candidate result sets are present in `analysis_result_sets`.
  - Verifies `eco-build-normalization-audit` continues from analysis-plane state after deleting `claim_candidates` and `observation_candidates` JSON artifacts.

Known limitations:
- Clustered and merged candidate-family objects still remain outside the shared analysis plane; this increment only migrates raw claim/observation candidate results plus the audit consumer.
- The shared result-set contract still lacks richer lineage semantics for non-Python tooling.

Next:
- Shift the next independent increment to `B2` now that the major read-side DB-first migration tranche is complete.
- Revisit `C2` and `C2.1` later for broader result-set contract hardening and cluster/merge object migration.

## 2026-04-03 B2: Core Board Mutation DB-First

Status: completed

Objective:
- Move the core board mutation skills off `JSON first -> DB sync` and onto `deliberation-plane first -> JSON export`.
- Remove the hard requirement that board readers must see `investigation_board.json` before they can continue from existing DB state.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/deliberation_plane.py`
  - Added `board_runs` metadata to track board revision and export path in the DB.
  - Added bootstrap logic that imports board JSON only when the DB is missing or stale.
  - Added a shared DB-first mutation commit path that writes deliberation rows first, then exports `investigation_board.json`, and backfills stable record locators.
  - Updated `load_round_snapshot(...)` so readers can continue from DB-only state when the board JSON export is temporarily absent.
- Migrated core board mutation skills to `deliberation-plane` primary writes:
  - `skills/eco-post-board-note/scripts/eco_post_board_note.py`
  - `skills/eco-update-hypothesis-status/scripts/eco_update_hypothesis_status.py`
  - `skills/eco-open-challenge-ticket/scripts/eco_open_challenge_ticket.py`
  - `skills/eco-close-challenge-ticket/scripts/eco_close_challenge_ticket.py`
  - `skills/eco-claim-board-task/scripts/eco_claim_board_task.py`
  - These skills now read existing board state from the deliberation plane, write DB rows first, export the board JSON for compatibility, and emit `summary.db_path` plus `summary.write_surface`.
- `skills/eco-read-board-delta/scripts/eco_read_board_delta.py`
  - Now reads through `load_round_snapshot(...)` so delta reads can continue from deliberation-plane state even when the board JSON export is missing.

Validation:
- `python3 -m unittest tests/test_board_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_board_workflow.py`
  - Verifies a second DB-first board mutation can recreate `board/investigation_board.json` after the file has been deleted, while preserving earlier board state from the DB.
  - Verifies board delta and board brief readers continue from `deliberation-plane` state when the board JSON export is absent.

Known limitations:
- `eco-open-investigation-round` still performs a JSON-first board mutation and then syncs back into the deliberation plane.
- `eco-scaffold-mission-run` still seeds the initial board through a JSON artifact; this increment relies on DB bootstrap to import that scaffold on the first DB-first mutation.

Next:
- Continue `B2` by migrating `eco-open-investigation-round` and any remaining round-transition mutation paths onto the same deliberation-plane-first surface.
- Then move to `B2.1` so `board_summary` and `board_brief` can be treated as derived exports rather than operational prerequisites.

## 2026-04-03 B2: Round Transition Write-Path Migration

Status: completed

Objective:
- Finish `B2` by moving `eco-open-investigation-round` off `JSON first -> DB sync` and onto `deliberation-plane first -> JSON export`.
- Keep follow-up round transition artifacts queryable in the deliberation plane while preserving the existing JSON exports and task scaffold contract.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/deliberation_plane.py`
  - Added `store_round_transition_record(...)` so `round_transitions` can be written directly into SQLite without requiring a full board re-sync.
- `skills/eco-open-investigation-round/scripts/eco_open_investigation_round.py`
  - Replaced in-place board JSON mutation with `load_round_snapshot(...)` plus `commit_board_mutation(...)`.
  - Source and target round existence now resolve from deliberation-plane state, so follow-up round opening continues even when `board/investigation_board.json` is temporarily absent.
  - Added `summary.db_path` and `summary.write_surface`, and now writes the `round_transition` row directly into the deliberation plane after exporting `runtime/round_transition_<round_id>.json`.
- `eco-concil-runtime/src/eco_council_runtime/kernel/registry.py`
  - Extended contract parsing so DB-first board skills that declare compatibility exports still register their resolved write paths for runtime governance and ledger metadata.
- `skills/eco-open-investigation-round/SKILL.md`
  - Updated the read/write contract wording to match deliberation-plane-first behavior.

Validation:
- `python3 -m unittest tests/test_board_workflow.py -q`
- `python3 -m unittest tests/test_investigation_workflow.py -q`
- `python3 -m unittest tests/test_runtime_kernel.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_board_workflow.py`
  - Verifies `eco-open-investigation-round` can reopen a follow-up round after deleting `board/investigation_board.json`, recreate the compatibility export from DB state, and persist the round transition into `round_transitions`.

Known limitations:
- `eco-scaffold-mission-run` still seeds the initial board through a JSON artifact before DB bootstrap takes over on the first DB-first mutation.
- `runtime/round_transition_<round_id>.json` and `investigation/round_tasks_<round_id>.json` remain compatibility exports written after the DB mutation rather than transactional DB-only surfaces.

Next:
- Move to `B2.1` so `board_summary` and `board_brief` are treated strictly as derived exports instead of operational prerequisites.
- Follow with `A2` shared contract hardening so board / analysis trace fields and runtime governance metadata stop drifting independently.

## 2026-04-03 B2.1: Phase-2 Board Export Demotion

Status: completed

Objective:
- Stop treating `board_summary` and `board_brief` as hard phase-2 controller prerequisites.
- Keep these artifacts available as derived exports while letting planner/controller proceed directly from deliberation-plane-backed board state.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
  - Removed `board-summary` and `board-brief` from the default phase-2 execution queue so runtime fallback control no longer requires them before `next-actions` and `round-readiness`.
- `eco-concil-runtime/src/eco_council_runtime/kernel/phase2_contract.py`
  - Reclassified `board-summary` and `board-brief` as non-blocking export-side stages.
  - Relaxed `next-actions` so it depends on `orchestration-planner` directly instead of `board-brief`.
- `skills/eco-plan-round-orchestration/scripts/eco_plan_round_orchestration.py`
  - Planner-backed execution queues now start from `next-actions`.
  - `board-summary` and `board-brief` are emitted under `derived_exports` rather than `execution_queue`.
  - Added `observed_state.board_exports_are_derived` and `summary.derived_export_count` for traceability.
- Updated skill docs to match the new semantics:
  - `skills/eco-plan-round-orchestration/SKILL.md`
  - `skills/eco-propose-next-actions/SKILL.md`
  - `skills/eco-summarize-round-readiness/SKILL.md`

Validation:
- `python3 -m unittest tests/test_orchestration_planner_workflow.py -q`
- `python3 -m unittest tests/test_supervisor_simulation_regression.py -q`
- `python3 -m unittest tests/test_runtime_kernel.py -q`

Tests added or extended:
- `tests/test_orchestration_planner_workflow.py`
  - Verifies planner-backed execution queues omit `board-summary` and `board-brief`.
  - Verifies both artifacts remain visible as `derived_exports`.
- `tests/test_supervisor_simulation_regression.py`
  - Verifies real controller runs no longer execute `board-summary` or `board-brief` as phase-2 stages.

Known limitations:
- Reporting/archive skills still consume `board_brief` or `board_summary` as optional human-facing context in some paths; this increment only demotes them from runtime control prerequisites.
- The planner still records summary/brief paths in the plan payload for export discoverability even when controller execution does not materialize them.

Next:
- Move to `A2` shared contract hardening so `analysis_sync / deliberation_sync / observed_inputs / *_source` fields stop drifting between skills and runtime wrappers.
- Revisit `B3` later to consolidate more moderator control transitions onto the DB work surface now that summary-first ordering is no longer required.

## 2026-04-03 A2.1: D1 Contract Metadata Normalization

Status: completed

Objective:
- Stop D1 contract metadata from drifting independently between `eco-propose-next-actions`, `eco-open-falsification-probe`, and `eco-summarize-round-readiness`.
- Make artifact presence and materialized input presence explicit in `observed_inputs` while preserving compatibility with older payloads.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/investigation_planning.py`
  - Added shared helpers for D1 contract hardening:
    - `normalize_d1_observed_inputs(...)`
    - `d1_contract_fields(...)`
    - `d1_contract_fields_from_payload(...)`
    - `load_d1_shared_context(...)`
  - Generalized `board_snapshot(...)` so D1 consumers can share one board-state summarization path, including readiness-side note counts when needed.
  - Normalized D1 trace metadata around:
    - `board_state_source`
    - `coverage_source`
    - `deliberation_sync`
    - `analysis_sync`
    - explicit `*_artifact_present` and `*_present` flags inside `observed_inputs`
- `skills/eco-propose-next-actions/scripts/eco_propose_next_actions.py`
  - Now serializes D1 contract metadata through the shared runtime helper instead of hand-copying trace fields.
- `skills/eco-open-falsification-probe/scripts/eco_open_falsification_probe.py`
  - Now normalizes contract metadata both when consuming an existing `next_actions` artifact and when rebuilding from shared D1 context.
  - Adds explicit `next_actions_artifact_present` tracking without losing compatibility with the older `next_actions_present` flag.
- `skills/eco-summarize-round-readiness/scripts/eco_summarize_round_readiness.py`
  - Replaced duplicated deliberation/analysis trace assembly with the shared D1 context helper.
  - Now emits the same normalized input-presence contract as the D1 action/probe artifacts.

Validation:
- `python3 -m unittest tests/test_investigation_contracts.py -q`
- `python3 -m unittest tests/test_investigation_workflow.py -q`
- `python3 -m unittest tests/test_runtime_kernel.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_investigation_contracts.py`
  - Verifies explicit D1 artifact-presence flags can diverge from materialized-presence flags when provided directly.
  - Verifies legacy D1 payloads are backfilled into the new normalized contract shape.
- `tests/test_investigation_workflow.py`
  - Verifies next-actions, probes, and readiness artifacts all expose the new explicit `*_artifact_present` flags on both normal and fallback paths.

Known limitations:
- This increment hardens the D1 investigation chain only; archive/history/promotion/reporting consumers still have similar trace metadata that should be normalized under the remaining `A2` work.
- Output schema versions remain unchanged because this delivery is a compatible contract normalization rather than a breaking payload rewrite.

Next:
- Continue `A2` by extending the same explicit contract surface to remaining cross-plane consumers such as archive/reporting/promotion skills.
- Then revisit `C2` once contract drift is reduced enough to harden result-set lineage semantics on top of a more stable metadata base.

## 2026-04-03 A2.2: Promotion / Reporting Trace Contract Adoption

Status: completed

Objective:
- Extend the normalized cross-plane trace contract from D1 into the promotion and reporting draft chain.
- Make artifact presence versus materialized presence explicit for promotion, reporting handoff, council decision draft, and expert report draft inputs without breaking existing workflow contracts.

Implementation:
- Added `eco-concil-runtime/src/eco_council_runtime/kernel/reporting_contracts.py`
  - Introduced shared helpers for the reporting chain:
    - `normalize_reporting_observed_inputs(...)`
    - `reporting_contract_fields(...)`
    - `reporting_contract_fields_from_payload(...)`
  - Reused the D1 contract surface so downstream reporting artifacts can carry forward:
    - `board_state_source`
    - `coverage_source`
    - `deliberation_sync`
    - `analysis_sync`
    - normalized `observed_inputs`
  - Added reporting-side source and presence normalization for:
    - `readiness`
    - `promotion`
    - `supervisor_state`
    - `reporting_handoff`
    - `decision`
- `skills/eco-promote-evidence-basis/scripts/eco_promote_evidence_basis.py`
  - Now emits shared trace metadata through the reporting contract helper.
  - Preserves readiness-origin deliberation/analysis provenance while making `readiness`, `board_brief`, and `next_actions` artifact-versus-materialized presence explicit.
- `skills/eco-materialize-reporting-handoff/scripts/eco_materialize_reporting_handoff.py`
  - Now carries forward normalized promotion/readiness contract metadata instead of emitting a reporting-only summary object with no shared trace surface.
  - Adds explicit `promotion_source`, `readiness_source`, `board_brief_source`, `supervisor_state_source`, `deliberation_sync`, `analysis_sync`, and normalized `observed_inputs`.
- `skills/eco-draft-council-decision/scripts/eco_draft_council_decision.py`
  - Now preserves normalized reporting/promotion trace metadata in the decision draft so downstream consumers can still see board/evidence provenance.
- `skills/eco-draft-expert-report/scripts/eco_draft_expert_report.py`
  - Now preserves normalized reporting/decision trace metadata in role report drafts, including explicit handoff/decision/board-brief presence flags.
- Updated skill docs to match the new output contract:
  - `skills/eco-promote-evidence-basis/SKILL.md`
  - `skills/eco-materialize-reporting-handoff/SKILL.md`
  - `skills/eco-draft-council-decision/SKILL.md`
  - `skills/eco-draft-expert-report/SKILL.md`

Validation:
- `python3 -m unittest tests/test_reporting_contracts.py -q`
- `python3 -m unittest tests/test_reporting_workflow.py -q`
- `python3 -m unittest tests/test_reporting_publish_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- Added `tests/test_reporting_contracts.py`
  - Verifies reporting-side explicit artifact flags can diverge from materialized-presence flags.
  - Verifies reporting contracts can merge fallback upstream payload metadata and local overrides.
- `tests/test_reporting_workflow.py`
  - Verifies promotion, reporting handoff, and council decision artifacts expose normalized cross-plane trace metadata on both promoted and withheld paths.
  - Verifies normal runtime paths explicitly record `board_brief` as absent when the derived export is not materialized.
- `tests/test_reporting_publish_workflow.py`
  - Verifies expert report drafts preserve reporting-handoff, decision-draft, board-state, and coverage provenance.

Known limitations:
- The canonical publish/final-publication layer still consumes upstream reporting artifacts without yet emitting the same fully normalized trace contract; that remains part of the unfinished `A2.2` surface.
- This increment intentionally preserves existing schema versions and payload shapes, so some older upstream artifacts may still rely on fallback merge behavior rather than natively carrying every normalized source field.

Next:
- Continue `A2.2` into `eco-publish-expert-report`, `eco-publish-council-decision`, and `eco-materialize-final-publication` so the rest of the export plane emits the same explicit trace contract.
- Then revisit `C2` once the remaining cross-plane metadata drift is further reduced.

## 2026-04-04 A2.2: Publish / Final Publication Trace Contract Adoption

Status: completed

Objective:
- Finish the remaining `A2.2` publish/export consumers so canonical report publication, canonical decision publication, and final publication all emit the same normalized cross-plane trace contract already used by the reporting draft chain.
- Preserve optional-input absence explicitly in final export artifacts instead of relying on warnings alone.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/reporting_contracts.py`
  - Extended the shared reporting helper to cover publish/export input families:
    - `expert_report_draft`
    - `sociologist_report`
    - `environmentalist_report`
  - Added matching normalized source fields:
    - `expert_report_draft_source`
    - `sociologist_report_source`
    - `environmentalist_report_source`
- `skills/eco-publish-expert-report/scripts/eco_publish_expert_report.py`
  - Canonical expert reports now preserve reporting-chain provenance from the draft instead of only adding publish metadata.
  - Adds explicit `expert_report_draft_source`, normalized `observed_inputs`, and shared `deliberation_sync` / `analysis_sync` passthrough.
- `skills/eco-publish-council-decision/scripts/eco_publish_council_decision.py`
  - Canonical council decisions now preserve draft-side board/evidence provenance while explicitly recording the presence state of canonical role reports.
  - Adds `decision_source`, `sociologist_report_source`, `environmentalist_report_source`, normalized `observed_inputs`, and shared sync metadata.
- `skills/eco-materialize-final-publication/scripts/eco_materialize_final_publication.py`
  - Final publication artifacts now preserve shared board/evidence provenance from upstream decision/handoff payloads instead of collapsing to publish-only metadata.
  - Explicitly records direct input sources and input presence for:
    - `reporting_handoff`
    - `decision`
    - `promotion`
    - `supervisor_state`
    - both canonical role reports
  - Makes absent optional canonical reports visible in the final artifact on hold paths through normalized `*_source` and `observed_inputs` fields.
- Updated skill docs to match the new output contract:
  - `skills/eco-publish-expert-report/SKILL.md`
  - `skills/eco-publish-council-decision/SKILL.md`
  - `skills/eco-materialize-final-publication/SKILL.md`

Validation:
- `python3 -m unittest tests/test_reporting_contracts.py -q`
- `python3 -m unittest tests/test_reporting_publish_workflow.py -q`
- `python3 -m unittest tests/test_reporting_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_reporting_contracts.py`
  - Extended explicit-flag coverage to the publish-layer `expert_report_draft` input family.
- `tests/test_reporting_publish_workflow.py`
  - Verifies canonical expert reports preserve draft/reporting provenance.
  - Verifies canonical council decisions preserve draft provenance and explicit role-report presence flags on both ready and hold paths.
  - Verifies final publication artifacts preserve reporting/promotion/decision/supervisor/role-report provenance, including explicit absence on hold paths.

Known limitations:
- The normalized reporting contract is still runtime-local Python infrastructure; it is not yet surfaced as a formal non-Python query or schema package.
- Payload schema versions remain stable for compatibility, so older artifacts may still rely on fallback merge behavior when they predate the new normalized fields.

Next:
- Shift the next implementation priority to `C2` so analysis-plane result sets gain stronger lineage semantics on top of the now more stable cross-plane contract base.
- After that, revisit `D3` or `B3` depending whether program-control visibility or moderator control consolidation is the more immediate bottleneck.

## 2026-04-04 C2: Generic Result-Set Lineage Contract

Status: completed

Objective:
- Harden the shared analysis-plane contract so result sets are no longer just JSON mirrors in SQLite.
- Make `result_sets / result_items` traceable by `query_basis`, parent result sets, parent ids, and artifact refs through one consistent runtime surface.

Implementation:
- `eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py`
  - Added `analysis_result_lineage` as a shared SQLite lineage table for analysis result sets and items.
  - Added generic extraction helpers for:
    - `query_basis`
    - set-level parent artifact refs
    - set-level parent result-set resolution by input artifact path
    - item-level parent ids
    - item-level artifact refs
  - `sync_analysis_result_set(...)` now persists lineage rows alongside `analysis_result_sets` and `analysis_result_items`.
  - `load_analysis_result_context(...)` and all typed analysis-context loaders now return a normalized `result_contract`, and `analysis_sync` now carries the same lineage summary fields.
- Added explicit `query_basis` emission to current analysis-plane producers:
  - `skills/eco-extract-claim-candidates/scripts/eco_extract_claim_candidates.py`
  - `skills/eco-extract-observation-candidates/scripts/eco_extract_observation_candidates.py`
  - `skills/eco-link-claims-to-observations/scripts/eco_link_claims_to_observations.py`
  - `skills/eco-derive-claim-scope/scripts/eco_derive_claim_scope.py`
  - `skills/eco-derive-observation-scope/scripts/eco_derive_observation_scope.py`
  - `skills/eco-score-evidence-coverage/scripts/eco_score_evidence_coverage.py`
- `tests/test_analysis_workflow.py`
  - Added lineage persistence coverage for both DB rows and analysis-plane load helpers.
- `openclaw-db-first-master-plan.md`
  - Marked `C2` as completed and shifted the recommended next queue forward.

Validation:
- `python3 -m unittest tests/test_analysis_workflow.py -q`
- `python3 -m unittest discover -s tests -q`

Tests added or extended:
- `tests/test_analysis_workflow.py`
  - Verifies coverage result sets persist `query-basis`, parent result-set, and parent artifact-ref lineage rows in `analysis_result_lineage`.
  - Verifies claim-candidate items persist parent-id and artifact-ref lineage rows.
  - Verifies `load_evidence_coverage_context(...)` can recover the same lineage contract from DB-only state after deleting the coverage JSON artifact.

Known limitations:
- The lineage contract is still a runtime-local Python/SQLite surface; `C2.2` is still needed before non-Python tooling can query it through a more formal interface.
- Clustered and merged candidate-family objects still remain outside the shared analysis plane; this increment hardens the contract for already-migrated result kinds, but does not yet finish `C2.1`.

Next:
- Move to `D3` so the enlarged route map gains a stronger progress dashboard and blocking view.
- Then continue either `B3` moderator control consolidation or `C2.1` candidate/cluster migration according to the updated master plan order.
