---
name: materialize-reporting-handoff
description: Materialize DB-backed reporting packets from frozen evidence basis and reporting gate state. Exports are compatibility views, not the source of reporting truth.
---

# Materialize Reporting Handoff

## Core Goal
- Turn DB-backed frozen evidence basis and reporting gate state into explicit `evidence_packet`, `decision_packet`, and `report_packet` objects inside one canonical reporting handoff.
- Preserve evidence index, uncertainty register, residual disputes, policy recommendations, operator notes, and next-round focus in one auditable object.
- Carry optional claim-gap cards, interaction timeline nodes, and derived section briefs as advisory report_packet context, not direct report basis.
- Provide a stable handoff for decision memo drafting and decision-maker report assembly.
- Treat JSON/Markdown artifacts as export-only compatibility inputs when DB rows are available.

## Triggering Conditions
- A round has a frozen DB evidence basis or an explicitly withheld reporting state that must be explained.
- Need one downstream handoff without letting reporting logic re-read optional helper artifacts as report basis.
- Need a clear packet boundary between evidence citation, decision posture, and final report structure.

## Read/Write Contract
- Reads `run_dir/report_basis/frozen_report_basis_<round_id>.json`.
- Reads `run_dir/reporting/round_readiness_<round_id>.json`.
- Reads `run_dir/board/board_brief_<round_id>.md`.
- Reads `run_dir/runtime/supervisor_state_<round_id>.json`.
- Writes `run_dir/reporting/reporting_handoff_<round_id>.json`.
- Writes canonical reporting handoff rows through the reporting plane.
- Helper and fallback cues remain audit material unless a DB finding, evidence bundle, proposal, readiness opinion, report section draft, or report basis explicitly cites them.

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
- The emitted handoff includes `evidence_packet`, `decision_packet`, `report_packet`, `evidence_index`, `uncertainty_register`, `residual_disputes`, `policy_recommendations`, optional `claim_gap_action_cards`, optional `interaction_timeline_nodes`, and optional `section_briefs`.

## Agent Reasoning Guide
- Treat reporting handoff as a rebuildable packet view over DB-backed frozen or
  withheld reporting state. It does not reopen investigation, add new evidence,
  or promote optional helper cues into report basis.
- Missing packet sections or withheld posture should preserve blockers,
  uncertainty, and residual disputes instead of being smoothed into readiness.
- Downstream decision/report skills must keep evidence refs, basis ids, and
  uncertainty boundaries visible.
- Section briefs expose refs, claim strength, denominators, and limitations for
  report-editor judgement; they do not create a parallel report path or validate
  public policy situation analysis by themselves.
- If no `situation-analysis-brief` is present, expose
  `materialize-situation-analysis-brief` as the recommended pre-narrative next
  skill. This is a coordination hint only, not a runtime gate or report approval
  decision.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/materialize_reporting_handoff.py`
