---
name: eco-normalize-open-meteo-flood-signals
description: Normalize open-meteo-flood-fetch results into unified environment signals and write them into the signal plane database. Use when tasks need canonical flood or discharge rows, artifact refs, and board-ready hints for downstream observation extraction.
---

# Eco Normalize Open-Meteo Flood Signals

## Core Goal
- Read one `open-meteo-flood-fetch` raw artifact.
- Convert daily flood or river-discharge records into canonical environment signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and board-ready hints.

## Triggering Conditions
- A fetch step already produced Open-Meteo flood output.
- The council needs canonical flood or discharge observations instead of provider-native payloads.
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
- `scripts/eco_normalize_open_meteo_flood_signals.py`
