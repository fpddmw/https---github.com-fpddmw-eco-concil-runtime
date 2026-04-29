---
name: materialize-reporting-handoff
description: Materialize DB-backed reporting packets from frozen evidence basis and reporting gate state. Exports are compatibility views, not the source of reporting truth.
---

# Materialize Reporting Handoff

## Core Goal
- Turn DB-backed frozen evidence basis and reporting gate state into explicit `evidence_packet`, `decision_packet`, and `report_packet` objects inside one canonical reporting handoff.
- Preserve evidence index, uncertainty register, residual disputes, policy recommendations, operator notes, and next-round focus in one auditable object.
- Provide a stable handoff for decision memo drafting and decision-maker report assembly.
- Treat JSON/Markdown artifacts as export-only compatibility inputs when DB rows are available.

## Triggering Conditions
- A round has a frozen DB evidence basis or an explicitly withheld reporting state that must be explained.
- Need one downstream handoff without letting reporting logic re-read heuristic helper artifacts as report basis.
- Need a clear packet boundary between evidence citation, decision posture, and final report structure.

## Read/Write Contract
- Reads report-basis-freeze, readiness, board, and supervisor state through DB-first wrappers.
- Reads `run_dir/report_basis/frozen_report_basis_<round_id>.json`, `run_dir/reporting/round_readiness_<round_id>.json`, `run_dir/board/board_brief_<round_id>.md`, and `run_dir/runtime/supervisor_state_<round_id>.json` only as compatible exports when needed.
- Writes canonical reporting handoff rows and `run_dir/reporting/reporting_handoff_<round_id>.json` as a rebuildable export.
- Helper or heuristic cues remain audit material unless a DB finding, evidence bundle, proposal, readiness opinion, report section draft, or report basis explicitly cites them.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `report_basis_path`
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
- The emitted artifact also carries normalized cross-plane trace metadata in `board_state_source`, `coverage_source`, `report_basis_source`, `readiness_source`, `board_brief_source`, `supervisor_state_source`, `db_path`, and `observed_inputs`, including explicit artifact-versus-materialized flags for each upstream input.
- The emitted handoff includes `evidence_packet`, `decision_packet`, `report_packet`, `evidence_index`, `uncertainty_register`, `residual_disputes`, and `policy_recommendations`.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/materialize_reporting_handoff.py`
