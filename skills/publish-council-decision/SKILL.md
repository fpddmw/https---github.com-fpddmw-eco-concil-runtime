---
name: publish-council-decision
description: Publish a canonical council decision from the current decision draft while enforcing report prerequisites and overwrite guards.
---

# Publish Council Decision

## Core Goal
- Promote the current council decision draft into a canonical decision artifact.
- Enforce overwrite protection and, when appropriate, require canonical expert reports.
- Keep publish semantics outside the runtime kernel.
- Require explicit operator approval for governed publish execution; this skill does not advance investigation state.

## Triggering Conditions
- A council decision draft already exists.
- Need a canonical decision artifact with publish guards.
- Need final decision semantics to stay skill-first rather than move into runtime.

## Read/Write Contract
- Reads `run_dir/reporting/council_decision_draft_<round_id>.json` by default.
- Reads `run_dir/reporting/expert_report_sociologist_<round_id>.json` by default when decision publication_readiness is `ready`.
- Reads `run_dir/reporting/expert_report_environmentalist_<round_id>.json` by default when decision publication_readiness is `ready`.
- Writes `run_dir/reporting/council_decision_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `draft_path`
  - `sociologist_report_path`
  - `environmentalist_report_path`
  - `output_path`
  - `allow_overwrite`
  - `skip_report_check`

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
- The emitted canonical decision preserves normalized reporting-chain trace metadata in `board_state_source`, `coverage_source`, `reporting_handoff_source`, `report_basis_source`, `decision_source`, `sociologist_report_source`, `environmentalist_report_source`, `db_path`, and `observed_inputs`.
- The canonical decision preserves `decision_packet`, memo sections, report refs, evidence refs, and decision trace ids for final report assembly.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/publish_council_decision.py`
