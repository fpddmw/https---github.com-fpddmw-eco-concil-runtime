---
name: eco-lookup-normalized-signal
description: Look up one normalized signal by signal_id from the unified signal plane and return compact details plus provenance. Use when an agent needs precise inspection of a single canonical signal before promoting, challenging, or linking it.
---

# Eco Lookup Normalized Signal

## Core Goal
- Resolve one normalized signal by `signal_id`.
- Return compact canonical fields and provenance.
- Optionally include raw JSON only when explicitly requested.

## Triggering Conditions
- Need to verify one exact public or environment signal.
- Need to inspect the canonical row behind a board ref.
- Need a targeted review step before challenge or evidence linking.

## Read/Write Contract
- Read only.
- Reads from `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not write to the database.

## Required Input
- `run_dir`
- `signal_id`
- Optional:
  - `db_path`
  - `include_raw_json`

## Output Contract
- `status`
- `summary`
- `result_count`
- `results`
- `artifact_refs`
- `warnings`
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`

## Scripts
- `scripts/eco_lookup_normalized_signal.py`