---
name: query-environment-signals
description: Query compact environment-signal rows from the unified signal plane database with run, round, round-scope, metric, source, time, bbox, and quality filters. Use when an investigator needs DB-backed physical evidence refs and item-level evidence basis without reopening raw model or station artifacts.
---

# Query Environment Signals

## Core Goal
- Read compact environment signal rows from the unified signal plane database.
- Filter by run, round or cross-round scope, metric, source, time window, location, and quality flags.
- Return short results with item-level `evidence_refs` and `evidence_basis` for investigator, moderator, report-editor, and challenger use.

## Triggering Conditions
- Need physical observations without reading raw provider payloads.
- Need mission-window filtering for air, weather, hydrology, or fire signals.
- Need compact evidence refs for findings, evidence bundles, board review, or challenge work.
- Need to reopen prior-round physical evidence while staying inside the same run.

## Read/Write Contract
- Read only.
- Reads from `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite` with `plane = environment`.
- Does not write to the database.
- Does not infer exposure, representativeness, coverage sufficiency, readiness, or policy conclusions.

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
  - Each result includes `evidence_refs` and `evidence_basis`.
- `artifact_refs`
- `warnings`
- `board_handoff`
  - Suggested next steps are lookup, finding, evidence bundle, or discussion writes.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_environment_signals.py`
