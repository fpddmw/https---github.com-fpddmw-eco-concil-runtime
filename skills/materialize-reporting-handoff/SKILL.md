---
name: materialize-reporting-handoff
description: Materialize a DB-backed reporting handoff from frozen evidence basis, readiness, board, and supervisor state. Exports are compatibility views, not the source of reporting truth.
---

# Eco Materialize Reporting Handoff

## Core Goal
- Turn DB-backed frozen evidence basis and reporting gate state into one compact reporting handoff.
- Preserve evidence basis, gate posture, operator notes, and next-round focus in one auditable object.
- Provide a stable bridge between promotion and downstream reporting or decision skills.
- Treat JSON/Markdown artifacts as export-only compatibility inputs when DB rows are available.

## Triggering Conditions
- A round has a frozen DB evidence basis or an explicitly withheld reporting state that must be explained.
- Need one downstream handoff instead of forcing reporting logic to re-read every upstream DB object separately.
- Need a compact object that can drive either report drafting or another-round decisions.

## Read/Write Contract
- Reads promotion-basis, readiness, board, and supervisor state through DB-first wrappers.
- Reads `run_dir/promotion/promoted_evidence_basis_<round_id>.json`, `run_dir/reporting/round_readiness_<round_id>.json`, `run_dir/board/board_brief_<round_id>.md`, and `run_dir/runtime/supervisor_state_<round_id>.json` only as compatible exports when needed.
- Writes canonical reporting handoff rows and `run_dir/reporting/reporting_handoff_<round_id>.json` as a rebuildable export.

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
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/materialize_reporting_handoff.py`
