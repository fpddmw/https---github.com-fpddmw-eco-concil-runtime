---
name: open-report-writing-round
description: Open a reporting-only council round after moderator closeout so report-editor can draft, validate, and publish narrative reporting from existing council basis.
---

# Open Report Writing Round

## Core Goal
- Open a governed continuation round for report writing only.
- Register report-editor work from frozen or canonical council/reporting basis.
- Keep investigation agents, source selection, fetch, normalize, and evidence adoption out of the report-writing round.

## Triggering Conditions
- Moderator has judged the investigation ready for report production, or has explicitly bounded the report as weak/limited.
- Runtime-operator has approved an `open-report-writing-round` transition request.
- The round needs a narrative report artifact without reopening investigation.

## Read/Write Contract
- Reads the approved `transition-request` from the deliberation plane.
- Reads source round board state and optional `run_dir/reporting/*_<source_round_id>.json` artifacts.
- Writes `run_dir/runtime/round_transition_<round_id>.json`.
- Writes `run_dir/investigation/round_tasks_<round_id>.json` with a report-editor-only task.
- Writes a deliberation-plane round transition and task snapshot.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `source_round_id`
- `transition_request_id`
- Optional:
  - `board_path`
  - `output_path`
  - `basis_round_id`
  - `reporting_basis_ref`
  - `report_language`
  - `author_role`
  - `transition_note`

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
- This skill creates a report-writing container, not a new investigation phase.
- The generated task is a handoff surface for report-editor only; it must not
  rank evidence, assign truth, or add source-selection requirements.
- If the report editor finds a material evidence gap, it should record a report
  limitation or ask the moderator for a separate continuation investigation
  round. It should not silently fetch new evidence inside the report-writing
  round.
- The report-writing task should ask for a human-readable narrative report:
  conclusion first, then reasoning, evidence lanes, limitations, decision use,
  and audit trail. It should not ask the report editor to merely concatenate
  council object summaries.

## Scripts
- `scripts/open_report_writing_round.py`
