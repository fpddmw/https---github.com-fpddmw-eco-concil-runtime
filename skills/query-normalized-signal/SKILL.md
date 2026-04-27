---
name: query-normalized-signal
description: Look up one normalized signal by signal_id from the unified signal plane and return compact details, item-level evidence refs, and evidence basis. Use when an investigator needs precise inspection of a single canonical signal before filing a finding, evidence bundle, challenge, or proposal.
---

# Query Normalized Signal

## Core Goal
- Resolve one normalized signal by `signal_id`.
- Return compact canonical fields, `evidence_refs`, and `evidence_basis`.
- Optionally include raw JSON only when explicitly requested.

## Triggering Conditions
- Need to verify one exact public or environment signal.
- Need to inspect the canonical row behind a board ref.
- Need a targeted review step before finding, evidence-bundle, review-comment, challenge, or proposal submission.

## Read/Write Contract
- Read only.
- Reads from `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not write to the database.

## Required Input
- `run_dir`
- `signal_id`
- Optional:
  - `db_path`
  - `include_raw_json`

## Output Contract
- `status`
- `summary`
- `result_count`
- `results`
  - Each result includes `evidence_refs` and `evidence_basis`.
- `artifact_refs`
- `warnings`
- `board_handoff`
  - Suggested next steps are raw lookup, finding, evidence bundle, or discussion writes.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_normalized_signal.py`
