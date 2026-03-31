---
name: eco-normalize-usgs-water-observation-signals
description: Normalize usgs-water-iv-fetch results into unified environment signals and write them into the signal plane database. Use when tasks need canonical hydrology observation rows, artifact refs, and board-ready hints for downstream observation extraction.
---

# Eco Normalize USGS Water Observation Signals

## Core Goal
- Read one `usgs-water-iv-fetch` raw artifact.
- Convert hydrology station observations into canonical environment signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and board-ready hints.

## Triggering Conditions
- A fetch step already produced USGS instantaneous-value output.
- The council needs canonical hydrology observations instead of provider-native rows.
- Downstream observation extraction should operate from normalized signals.

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
- `../../openclaw-first-refactor-blueprint.md`

## Scripts
- `scripts/eco_normalize_usgs_water_observation_signals.py`
