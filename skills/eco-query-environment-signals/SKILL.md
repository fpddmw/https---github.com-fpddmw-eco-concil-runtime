---
name: eco-query-environment-signals
 description: Query compact environment-signal rows from the unified signal plane database with run, round, round-scope, metric, source, time, bbox, and quality filters. Use when an agent needs board-ready physical evidence refs without reopening raw model or station artifacts.
---

# Eco Query Environment Signals

## Core Goal
- Read compact environment signal rows from the unified signal plane database.
- Filter by run, round or cross-round scope, metric, source, time window, location, and quality flags.
- Return short results with provenance refs for environmentalist, moderator, and challenger use.

## Triggering Conditions
- Need physical observations without reading raw provider payloads.
- Need mission-window filtering for air, weather, hydrology, or fire signals.
- Need compact evidence refs for board or challenge work.
- Need to reopen prior-round physical evidence while staying inside the same run.

## Read/Write Contract
- Read only.
- Reads from `environment_signals_vw` in `runs/<run_id>/analytics/signal_plane.sqlite`.
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
  - `metric`
  - `observed_after_utc`
  - `observed_before_utc`
  - `bbox`
  - `quality_flag_any`
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
- `scripts/eco_query_environment_signals.py`
