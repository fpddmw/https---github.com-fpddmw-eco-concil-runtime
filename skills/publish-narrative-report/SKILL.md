---
name: publish-narrative-report
description: Publish a validated narrative report artifact from a report-editor draft.
---

# Publish Narrative Report

## Core Goal
- Promote a validated narrative draft to a canonical report artifact.
- Preserve Markdown and JSON outputs for human reading and audit.
- Keep publication governed by operator approval through runtime policy.

## Triggering Conditions
- `draft-narrative-report` has produced a draft.
- `validate-narrative-report` has produced a valid validation artifact.
- Runtime policy has allowed publication.

## Read/Write Contract
- Reads `run_dir/reporting/narrative_report_draft_<round_id>.json`.
- Reads `run_dir/reporting/narrative_report_draft_<round_id>.md` when present.
- Reads `run_dir/reporting/narrative_report_validation_<round_id>.json`.
- Writes `run_dir/reporting/narrative_report_<round_id>.json`.
- Writes `run_dir/reporting/narrative_report_<round_id>.md`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `draft_path`
  - `draft_markdown_path`
  - `validation_path`
  - `output_path`
  - `markdown_output_path`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- Publication preserves the validated draft and does not add facts or upgrade
  claim strength.
- Operator approval is required by runtime policy because this is a publication
  action, not because the runtime decides the report's truth.
- Publication should preserve the reader-facing Markdown exactly as validated;
  it should not rewrite the report, add claims, or hide limitations.

## Scripts
- `scripts/publish_narrative_report.py`
