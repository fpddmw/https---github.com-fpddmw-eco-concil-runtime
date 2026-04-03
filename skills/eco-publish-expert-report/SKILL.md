---
name: eco-publish-expert-report
description: Publish a canonical expert report from a role-specific draft while enforcing round and overwrite guards.
---

# Eco Publish Expert Report

## Core Goal
- Promote a role-specific expert report draft into a canonical report artifact.
- Keep publish semantics and overwrite guards in a skill instead of runtime.
- Allow deterministic no-op republish when the canonical artifact already matches the draft.

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
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_publish_expert_report.py`