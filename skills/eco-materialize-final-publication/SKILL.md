---
name: eco-materialize-final-publication
description: Materialize one final publication artifact from the canonical decision, canonical expert reports, reporting handoff, and promotion-stage audit refs.
---

# Eco Materialize Final Publication

## Core Goal
- Collapse canonical reporting outputs into one final publication artifact.
- Preserve release posture, published sections, and upstream audit refs in one durable object.
- Keep publication semantics in a skill rather than pushing them into runtime.

## Triggering Conditions
- A canonical council decision already exists for the round.
- Need one final publication object rather than forcing downstream consumers to re-open each canonical report separately.
- Need explicit release vs withhold posture with audit completeness.

## Read/Write Contract
- Reads `run_dir/reporting/reporting_handoff_<round_id>.json` by default.
- Reads `run_dir/reporting/council_decision_<round_id>.json` by default.
- Reads `run_dir/reporting/expert_report_sociologist_<round_id>.json` by default when present.
- Reads `run_dir/reporting/expert_report_environmentalist_<round_id>.json` by default when present.
- Reads `run_dir/promotion/promoted_evidence_basis_<round_id>.json` by default.
- Reads `run_dir/runtime/supervisor_state_<round_id>.json` by default when present.
- Writes `run_dir/reporting/final_publication_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `reporting_handoff_path`
  - `decision_path`
  - `sociologist_report_path`
  - `environmentalist_report_path`
  - `promotion_path`
  - `supervisor_state_path`
  - `output_path`
  - `allow_overwrite`

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
- The emitted final publication preserves normalized cross-plane trace metadata in `board_state_source`, `coverage_source`, `reporting_handoff_source`, `decision_source`, `promotion_source`, `supervisor_state_source`, role-report sources, `db_path`, and `observed_inputs`.

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_materialize_final_publication.py`
