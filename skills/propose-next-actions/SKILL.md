---
name: propose-next-actions
description: Optional moderator advisory skill for proposing possible next actions from DB-backed council state. It requires operator-approved skill approval and never owns the default investigation sequence.
---

# Eco Propose Next Actions

## Core Goal
- Suggest possible follow-up actions when a moderator asks for advisory help.
- Label ranking logic as heuristic and keep it subordinate to DB-native findings, evidence bundles, proposals, and moderator judgement.
- Emit a durable optional-analysis artifact that can be audited, consumed, or ignored without changing phase state.
- Require `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` before execution.

## Triggering Conditions
- A moderator has identified a planning question that would benefit from optional ranking support.
- The operator has approved this optional-analysis run for the specific round and actor role.
- Need advisory candidates for future investigation, not a required queue before probes, readiness, report basis, or reporting.

## Read/Write Contract
- Syncs the round into the run-local deliberation plane and prefers that state for action ranking.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default as a compatible advisory fallback.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Reads evidence coverage from the run-local analysis plane first.
- Falls back to `run_dir/analytics/evidence_coverage_<round_id>.json` when the synced result set is unavailable.
- Writes `run_dir/investigation/next_actions_<round_id>.json` as an optional advisory export and syncs canonical rows where supported.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_summary_path`
  - `board_brief_path`
  - `coverage_path`
  - `output_path`
  - `max_actions`

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
- The emitted artifact also carries normalized D1 trace metadata in `board_state_source`, `coverage_source`, `db_path`, and `observed_inputs`, including explicit `*_artifact_present` and `*_present` flags.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/propose_next_actions.py`
