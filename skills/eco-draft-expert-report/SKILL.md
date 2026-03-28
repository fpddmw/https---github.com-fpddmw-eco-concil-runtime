---
name: eco-draft-expert-report
description: Draft a compact role-specific expert report from the reporting handoff and council decision draft so downstream publish steps stay outside the runtime kernel.
---

# Eco Draft Expert Report

## Core Goal
- Materialize one role-specific expert report draft as an atomic skill.
- Keep reporting semantics outside the runtime kernel.
- Preserve role focus, promoted evidence basis, open risks, and next actions in one durable artifact.

## Triggering Conditions
- A reporting handoff already exists for the round.
- Need a sociologist or environmentalist report draft before canonical publish.
- Need downstream report logic to stay skill-first instead of moving into runtime.

## Read/Write Contract
- Reads `run_dir/reporting/reporting_handoff_<round_id>.json` by default.
- Reads `run_dir/reporting/council_decision_draft_<round_id>.json` by default when present.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Writes `run_dir/reporting/expert_report_draft_<role>_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `role`
- Optional:
  - `reporting_handoff_path`
  - `decision_path`
  - `board_brief_path`
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
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-skill-phase-plan.md`

## Scripts
- `scripts/eco_draft_expert_report.py`