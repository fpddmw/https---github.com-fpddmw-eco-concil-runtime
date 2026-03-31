---
name: eco-normalize-gdelt-events-public-signals
description: Normalize gdelt-events-fetch export snapshots into unified public signals and write them into the signal plane database. Use when tasks need row-level GDELT event evidence, artifact refs, and board-ready hints from zipped export files.
---

# Eco Normalize GDELT Events Public Signals

## Core Goal
- Read one `gdelt-events-fetch` manifest artifact plus referenced zip exports.
- Convert zipped event rows into canonical public signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and board-ready hints.

## Triggering Conditions
- A fetch step already produced a GDELT Events export manifest.
- The council needs row-level event evidence instead of only manifest metadata.
- Downstream claim extraction should operate from canonical signal rows.

## Read/Write Contract
- Reads one manifest artifact and its referenced zip outputs from disk.
- Writes normalized rows into `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate claim candidates directly.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `artifact_path`
- Optional:
  - `db_path`
  - `max_rows_per_download`
  - `max_total_rows`
  - `artifact_ref_limit`
  - `canonical_id_limit`

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
- `scripts/eco_normalize_gdelt_events_public_signals.py`
