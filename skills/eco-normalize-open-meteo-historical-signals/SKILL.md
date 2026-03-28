---
name: eco-normalize-open-meteo-historical-signals
description: Normalize open-meteo-historical-fetch results into unified environment signals and write them into the signal plane database. Use when tasks need canonical model or reanalysis time-series rows, artifact refs, and board-ready hints for downstream observation extraction.
---

# Eco Normalize Open Meteo Historical Signals

## Core Goal
- Read one `open-meteo-historical-fetch` raw artifact.
- Convert hourly or daily series into canonical environment signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and board handoff hints.

## Triggering Conditions
- A fetch step already produced Open-Meteo historical output.
- The council needs model-backed physical context instead of raw provider payloads.
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
  - `section_allowlist`

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
- `scripts/eco_normalize_open_meteo_historical_signals.py`