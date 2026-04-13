---
name: eco-materialize-reporting-handoff
description: Materialize a compact reporting handoff from promoted evidence basis, readiness, board, and supervisor artifacts so downstream report and decision layers can consume one durable object.
---

# Eco Materialize Reporting Handoff

## Core Goal
- Turn promotion-stage artifacts into one compact reporting handoff.
- Preserve evidence basis, gate posture, operator notes, and next-round focus in one auditable object.
- Provide a stable bridge between promotion and downstream reporting or decision skills.

## Triggering Conditions
- A round already has readiness and promotion artifacts.
- Need one downstream handoff instead of forcing reporting logic to re-read every upstream artifact separately.
- Need a compact object that can drive either report drafting or another-round decisions.

## Read/Write Contract
- Reads `run_dir/promotion/promoted_evidence_basis_<round_id>.json` by default.
- Reads `run_dir/reporting/round_readiness_<round_id>.json` by default.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Reads `run_dir/runtime/supervisor_state_<round_id>.json` by default when present.
- Writes `run_dir/reporting/reporting_handoff_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `promotion_path`
  - `readiness_path`
  - `board_brief_path`
  - `supervisor_state_path`
  - `output_path`
  - `max_findings`

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
- The emitted artifact also carries normalized cross-plane trace metadata in `board_state_source`, `coverage_source`, `promotion_source`, `readiness_source`, `board_brief_source`, `supervisor_state_source`, `db_path`, and `observed_inputs`, including explicit artifact-versus-materialized flags for each upstream input.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/eco_materialize_reporting_handoff.py`
