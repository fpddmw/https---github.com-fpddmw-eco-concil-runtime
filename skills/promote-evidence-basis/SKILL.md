---
name: promote-evidence-basis
description: Promote the current round into a compact controversy-basis artifact by combining explicit council promotion judgements, readiness state, controversy agenda objects, and strongest coverage objects.
---

# Eco Promote Evidence Basis

## Core Goal
- Freeze the strongest round evidence and controversy objects into a promotion-ready basis artifact.
- Respect round-readiness gate results while still producing an auditable promotion decision.
- Consume explicit `proposal / readiness-opinion` judgements from the deliberation DB rather than inferring promotion support from legacy proposal names.
- Emit a compact artifact that later reporting and decision layers can consume.

## Triggering Conditions
- A round-readiness artifact exists and needs to be turned into a promotion decision.
- Council proposals or readiness opinions already express promotion posture and need to be resolved explicitly.
- Need to freeze strongest coverage refs together with issue clusters, routing posture, formal/public linkage, representation gaps, and diffusion edges when available.
- Need a durable basis artifact for the eventual canonical reporting layer.

## Read/Write Contract
- Reads `run_dir/reporting/round_readiness_<round_id>.json` by default.
- Reads `run_dir/board/board_brief_<round_id>.md` by default.
- Reads canonical `proposal` and `readiness-opinion` objects from the shared deliberation plane.
- Reads evidence coverage from the run-local analysis plane first.
- Falls back to `run_dir/analytics/evidence_coverage_<round_id>.json` when the synced result set is unavailable.
- Reads `run_dir/investigation/next_actions_<round_id>.json` by default when present.
- Writes `run_dir/promotion/promoted_evidence_basis_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
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

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/promote_evidence_basis.py`
