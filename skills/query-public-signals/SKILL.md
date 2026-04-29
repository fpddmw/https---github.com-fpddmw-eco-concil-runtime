---
name: query-public-signals
description: Query compact public-signal rows from the unified signal plane database with run, round, round-scope, source, kind, time, and keyword filters. Use when an investigator needs DB-backed public evidence refs and item-level evidence basis for findings or evidence bundles.
---

# Query Public Signals

## Core Goal
- Read compact public signal rows from the unified signal plane database.
- Filter by run, round or cross-round scope, source skill, signal kind, publication window, and keywords.
- Return short results with item-level `evidence_refs` and `evidence_basis`.

## Triggering Conditions
- Need public narrative evidence without reopening raw artifacts.
- Need to validate whether a public source has already been normalized.
- Need compact references for public-discourse investigator, moderator, report-editor, or challenger work.
- Need to reopen prior-round public evidence while staying inside the same run.

## Read/Write Contract
- Read only.
- Reads from `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite` with `plane = public`.
- Does not write to the database.
- Does not derive claims, routes, coverage scores, readiness, or report basis posture.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `db_path`
  - `round_scope`
- Optional filters:
  - `source_skill`
  - `signal_kind`
  - `published_after_utc`
  - `published_before_utc`
  - `keyword_any`
  - `limit`

## Output Contract
- `status`
- `summary`
- `result_count`
- `results`
  - Each result includes `evidence_refs` and `evidence_basis`.
- `artifact_refs`
- `warnings`
- `board_handoff`
  - Suggested next steps are lookup, finding, evidence bundle, or discussion writes.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_public_signals.py`
