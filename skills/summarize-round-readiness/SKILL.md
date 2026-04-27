---
name: summarize-round-readiness
description: Optional moderator advisory skill for summarizing readiness evidence from DB-backed council state. It requires operator-approved skill approval and cannot commit phase movement.
---

# Eco Summarize Round Readiness

## Core Goal
- Compile a reviewable readiness assessment only when a moderator requests optional advisory support.
- Prefer explicit council readiness opinions, findings, evidence bundles, probes, and unresolved blockers stored in DB.
- Label heuristic aggregation as advisory; formal phase movement still requires moderator transition request and operator approval.
- Require `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` before execution.

## Triggering Conditions
- A moderator needs an auditable readiness summary before deciding whether to request a phase transition.
- The operator has approved this optional-analysis run for the current round and actor role.
- Need to aggregate existing DB-native council objects without letting heuristic readiness become the phase owner.

## Read/Write Contract
- Syncs the round into the run-local deliberation plane and prefers that state for readiness evaluation.
- Reads canonical `readiness-opinion` objects from the shared deliberation plane when present.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default as a compatible advisory fallback.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present as a compatible advisory fallback.
- Reads optional next-action and probe exports when present, but does not require them.
- Reads evidence coverage from the run-local analysis plane first.
- Falls back to `run_dir/analytics/evidence_coverage_<round_id>.json` when the synced result set is unavailable.
- Writes `run_dir/reporting/round_readiness_<round_id>.json` as an optional advisory export and syncs canonical readiness rows where supported.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_summary_path`
  - `board_brief_path`
  - `next_actions_path`
  - `probes_path`
  - `coverage_path`
  - `output_path`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `deliberation_sync`
- `analysis_sync`
- `board_handoff`
- The emitted artifact also carries normalized D1/D2 trace metadata in `board_state_source`, `coverage_source`, `db_path`, and `observed_inputs`, including explicit `*_artifact_present` and `*_present` flags for board, action, probe, and coverage inputs.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/summarize_round_readiness.py`
