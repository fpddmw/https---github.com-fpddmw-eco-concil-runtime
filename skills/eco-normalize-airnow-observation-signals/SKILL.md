---
name: eco-normalize-airnow-observation-signals
description: Normalize airnow-hourly-obs-fetch results into unified environment signals and write them into the signal plane database. Use when tasks need canonical AirNow observation rows, artifact refs, and board-ready hints for downstream observation extraction.
---

# Eco Normalize AirNow Observation Signals

## Core Goal
- Read one `airnow-hourly-obs-fetch` raw artifact.
- Convert station observations into canonical environment signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and board handoff hints.

## Triggering Conditions
- A fetch step already produced AirNow hourly observations.
- The council needs station-level physical evidence instead of raw file products.
- Later observation extraction should operate from the unified signal plane.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes to `signal_artifacts`, `signal_ingest_batches`, and `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate observation candidates directly.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `artifact_path`
- Optional:
  - `db_path`
  - `metric_allowlist`

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

## Scripts
- `scripts/eco_normalize_airnow_observation_signals.py`