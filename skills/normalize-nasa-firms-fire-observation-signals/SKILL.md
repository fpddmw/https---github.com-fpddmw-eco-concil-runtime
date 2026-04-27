---
name: normalize-nasa-firms-fire-observation-signals
description: Normalize fetch-nasa-firms-fire results into unified environment signals and write them into the signal plane database. Use when investigators need canonical active-fire observation rows, artifact refs, provenance, quality flags, temporal/spatial scope, and coverage limitations.
---

# Eco Normalize NASA FIRMS Fire Observation Signals

## Core Goal
- Read one `fetch-nasa-firms-fire` raw artifact.
- Convert fire-detection rows into canonical environment signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced NASA FIRMS active-fire output.
- The council needs canonical fire observations instead of provider-native rows.
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
- `scripts/normalize_nasa_firms_fire_observation_signals.py`
