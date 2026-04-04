---
name: eco-draft-council-decision
description: Draft a compact council decision object from the reporting handoff so the system can either finalize the round or explicitly continue investigation.
---

# Eco Draft Council Decision

## Core Goal
- Turn the reporting handoff into one auditable council decision draft.
- Make the decision posture explicit: finalize the round or require another round.
- Preserve references back to the reporting, readiness, promotion, and supervisor artifacts.

## Triggering Conditions
- A compact reporting handoff already exists for the round.
- Need a downstream decision object instead of re-deriving final posture from multiple upstream artifacts.
- Need a draft that can later be consumed by richer report or publish workflows.

## Read/Write Contract
- Reads `run_dir/reporting/reporting_handoff_<round_id>.json` by default.
- Reads `run_dir/promotion/promoted_evidence_basis_<round_id>.json` by default when present.
- Writes `run_dir/reporting/council_decision_draft_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `reporting_handoff_path`
  - `promotion_path`
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
- The emitted artifact also carries normalized cross-plane trace metadata in `board_state_source`, `coverage_source`, `reporting_handoff_source`, `promotion_source`, `db_path`, and `observed_inputs`, preserving upstream trace fields from the reporting chain.

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_draft_council_decision.py`
