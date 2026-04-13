---
name: eco-promote-evidence-basis
description: Promote the current round into a compact evidence-basis artifact by combining round readiness, board brief, and strongest coverage objects.
---

# Eco Promote Evidence Basis

## Core Goal
- Freeze the strongest round evidence into a promotion-ready basis artifact.
- Respect round-readiness gate results while still producing an auditable promotion decision.
- Emit a compact artifact that later reporting and decision layers can consume.

## Triggering Conditions
- A round-readiness artifact exists and needs to be turned into a promotion decision.
- Need to freeze strongest coverage refs together with board context.
- Need a durable basis artifact for the eventual canonical reporting layer.

## Read/Write Contract
- Reads `run_dir/reporting/round_readiness_<round_id>.json` by default.
- Reads `run_dir/board/board_brief_<round_id>.md` by default.
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

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/eco_promote_evidence_basis.py`
