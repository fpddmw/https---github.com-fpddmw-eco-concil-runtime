---
name: draft-expert-report
description: Draft a chapterized role-specific report from reporting packets, section drafts, and a decision memo.
---

# Draft Expert Report

## Core Goal
- Materialize one role-specific, chapterized report draft as an atomic skill.
- Keep reporting semantics outside the runtime kernel.
- Preserve role focus, DB evidence index, report section draft refs, uncertainty register, residual disputes, and next actions in one durable artifact.

## Triggering Conditions
- A reporting handoff already exists for the round.
- Need a public-discourse/community-impact or environmental-evidence report draft before canonical publish.
- Need downstream report logic to stay skill-first instead of moving into runtime.

## Read/Write Contract
- Reads `run_dir/reporting/reporting_handoff_<round_id>.json` by default.
- Reads `run_dir/reporting/council_decision_draft_<round_id>.json` by default when present.
- Reads canonical `report-section-draft` rows for the requested role when available.
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
- `deliberation_sync`
- `analysis_sync`
- `board_handoff`
- The emitted artifact also carries normalized cross-plane trace metadata in `board_state_source`, `coverage_source`, `reporting_handoff_source`, `decision_source`, `board_brief_source`, `db_path`, and `observed_inputs`, preserving upstream trace fields from the reporting chain.
- The emitted draft includes `report_packet`, `report_sections`, `section_draft_refs`, `evidence_index`, `uncertainty_register`, `residual_disputes`, and `policy_recommendations`.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/draft_expert_report.py`
