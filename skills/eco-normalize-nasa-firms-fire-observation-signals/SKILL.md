---
name: eco-normalize-nasa-firms-fire-observation-signals
description: Normalize nasa-firms-fire-fetch results into unified environment signals and write them into the signal plane database. Use when tasks need canonical active-fire observation rows, artifact refs, and board-ready hints for downstream observation extraction.
---

# Eco Normalize NASA FIRMS Fire Observation Signals

## Core Goal
- Read one `nasa-firms-fire-fetch` raw artifact.
- Convert fire-detection rows into canonical environment signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and board-ready hints.

## Triggering Conditions
- A fetch step already produced NASA FIRMS active-fire output.
- The council needs canonical fire observations instead of provider-native rows.
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
- `scripts/eco_normalize_nasa_firms_fire_observation_signals.py`
