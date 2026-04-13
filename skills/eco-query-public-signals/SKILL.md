---
name: eco-query-public-signals
 description: Query compact public-signal rows from the unified signal plane database with run, round, round-scope, source, kind, time, and keyword filters. Use when an agent needs board-ready public evidence refs instead of raw artifacts or packet-heavy context.
---

# Eco Query Public Signals

## Core Goal
- Read compact public signal rows from the unified signal plane database.
- Filter by run, round or cross-round scope, source skill, signal kind, publication window, and keywords.
- Return short results with provenance refs and board-ready hints.

## Triggering Conditions
- Need public narrative evidence without reopening raw artifacts.
- Need to validate whether a public source has already been normalized.
- Need compact references for sociologist, moderator, or challenger work.
- Need to reopen prior-round public evidence while staying inside the same run.

## Read/Write Contract
- Read only.
- Reads from `public_signals_vw` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not write to the database.

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
- `artifact_refs`
- `warnings`
- `board_handoff`

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/eco_query_public_signals.py`
