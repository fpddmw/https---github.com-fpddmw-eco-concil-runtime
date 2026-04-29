---
name: promote-evidence-basis
description: Freeze the current round into a DB-backed report evidence basis after an approved transition request; retained skill name is a compatibility label, not runtime research judgement.
---

# Promote Evidence Basis

## Core Goal
- Freeze the current DB-backed evidence basis for downstream reporting after moderator request and operator approval.
- Preserve explicit council proposal/readiness judgement metadata without making the runtime a research decision maker.
- Consume explicit `proposal / readiness-opinion` judgements from the deliberation DB rather than inferring promotion support from legacy proposal names.
- Emit a compact report-basis artifact that later reporting and decision layers can consume.

## Triggering Conditions
- An approved `promote-evidence-basis` transition request exists and the operator has approved committing the freeze.
- Council proposals or readiness opinions already express report-basis posture and need to be preserved explicitly.
- Need to freeze DB evidence refs and basis object ids for reporting.
- Need a durable basis artifact for the eventual canonical reporting layer.

## Read/Write Contract
- Reads DB wrappers for readiness, board state, next actions, and promotion basis context; artifacts are export-only compatibility surfaces.
- Reads canonical `proposal` and `readiness-opinion` objects from the shared deliberation plane.
- Reads selected evidence/basis refs from the deliberation and analysis planes when present.
- Writes `run_dir/promotion/promoted_evidence_basis_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `transition_request_id`
- Optional:
  - `readiness_path`
  - `board_brief_path`
  - `coverage_path`
  - `next_actions_path`
  - `output_path`
  - `allow_non_ready`
  - `max_coverages`

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
- The emitted artifact also carries normalized cross-plane trace metadata in `board_state_source`, `coverage_source`, `readiness_source`, `board_brief_source`, `next_actions_source`, `db_path`, and `observed_inputs`, including explicit `*_artifact_present` and `*_present` flags for promotion inputs.
- The emitted artifact also records `basis_selection_mode`, `basis_counts`, `selected_basis_object_ids`, and `frozen_basis` so downstream layers can read controversy objects directly instead of only reading coverage rows.
- The emitted artifact also records `basis_object_kind=report-basis-freeze`, `transition_semantics=freeze-report-basis`, and `report_basis_selection_mode=freeze-report-basis-v1`.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/promote_evidence_basis.py`
