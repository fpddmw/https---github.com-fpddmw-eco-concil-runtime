---
name: normalize-open-meteo-flood-signals
description: Normalize fetch-open-meteo-flood results into unified environment signals and write them into the signal plane database. Use when investigators need canonical flood or discharge rows, artifact refs, provenance, quality flags, temporal/spatial scope, and coverage limitations.
---

# Eco Normalize Open-Meteo Flood Signals

## Core Goal
- Read one `fetch-open-meteo-flood` raw artifact.
- Convert daily flood or river-discharge records into canonical environment signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced Open-Meteo flood output.
- The council needs canonical flood or discharge observations instead of provider-native payloads.
- Investigator query, finding, and evidence-bundle work should operate from normalized signals.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes normalized rows into `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate observation candidates directly.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `artifact_path`
- Optional:
  - `db_path`

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
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/normalize_open_meteo_flood_signals.py`
