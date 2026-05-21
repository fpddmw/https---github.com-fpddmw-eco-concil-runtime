---
name: query-normalized-signal
description: Look up normalized signals by signal_id or metadata index from the unified signal plane and return compact details, item-level evidence refs, and evidence basis. Use when an investigator needs precise inspection of canonical signals before filing a finding, evidence bundle, challenge, or proposal.
---

# Query Normalized Signal

## Core Goal
- Resolve one normalized signal by `signal_id`, or query metadata-indexed signals by role/class fields.
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
- Either `signal_id` or at least one metadata filter:
  - `signal_role`
  - `environment_signal_class`
  - `relation_candidate_role`
  - `metadata_field` plus `metadata_value`
- Optional:
  - `db_path`
  - `run_id`
  - `round_id`
  - `plane`
  - `limit`
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

## Agent Reasoning Guide
- Treat this as targeted inspection of rows already present in the signal plane.
  It does not re-fetch, normalize, or judge whether a signal supports a claim.
- Empty lookup can reflect a wrong `signal_id`, wrong metadata filter, missing
  normalization, wrong DB path, or round/run mismatch. It does not prove that the
  underlying source record is absent.
- Use `include_raw_json` sparingly. Prefer `query-raw-record` for provenance
  review when the raw provider slice matters.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_normalized_signal.py`
