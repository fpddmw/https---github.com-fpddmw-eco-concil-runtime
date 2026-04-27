---
name: query-raw-record
description: Look up one raw record by signal_id or artifact path and record locator using the unified signal plane provenance model. Use when an investigator needs high-fidelity source inspection, item-level evidence refs, and evidence basis for auditing, challenge work, or provenance verification.
---

# Query Raw Record

## Core Goal
- Resolve one raw source record through signal provenance.
- Support lookup by `signal_id` or by `artifact_path` plus `record_locator`.
- Return the smallest raw payload slice plus `evidence_refs` and `evidence_basis` needed for auditing or challenge work.

## Triggering Conditions
- Need to verify what the normalizer actually read.
- Need to inspect ambiguous, contradictory, or low-confidence records.
- Need a provenance-preserving raw fallback without loading a whole artifact into context.

## Read/Write Contract
- Read only.
- Reads from `normalized_signals` and `signal_artifacts` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not write to the database.

## Required Input
- `run_dir`
- One of:
  - `signal_id`
  - `artifact_path`
  - `artifact_path` plus `record_locator`
- Optional:
  - `db_path`

## Output Contract
- `status`
- `summary`
- `result_count`
- `results`
  - Each result includes `raw_record`, `evidence_refs`, and `evidence_basis`.
- `artifact_refs`
- `warnings`
- `board_handoff`

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_raw_record.py`
