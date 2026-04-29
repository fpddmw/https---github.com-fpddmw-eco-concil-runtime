---
name: publish-expert-report
description: Publish a canonical expert report from a role-specific draft while enforcing round and overwrite guards.
---

# Publish Expert Report

## Core Goal
- Promote a role-specific expert report draft into a canonical report artifact.
- Keep publish semantics and overwrite guards in a skill instead of runtime.
- Allow deterministic no-op republish when the canonical artifact already matches the draft.
- Require explicit operator approval for governed publish execution; this skill does not advance investigation state.

## Triggering Conditions
- A role-specific expert report draft already exists.
- Need a canonical report artifact with overwrite protection.
- Need report publish semantics outside the runtime kernel.

## Read/Write Contract
- Reads `run_dir/reporting/expert_report_draft_<role>_<round_id>.json` by default.
- Writes `run_dir/reporting/expert_report_<role>_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `role`
- Optional:
  - `draft_path`
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
- The emitted canonical report preserves normalized reporting-chain trace metadata in `board_state_source`, `coverage_source`, `reporting_handoff_source`, `decision_source`, `expert_report_draft_source`, `db_path`, and `observed_inputs`.
- The canonical report preserves `report_packet`, section draft refs, evidence index, uncertainty register, residual disputes, and policy recommendations from the draft.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/publish_expert_report.py`
