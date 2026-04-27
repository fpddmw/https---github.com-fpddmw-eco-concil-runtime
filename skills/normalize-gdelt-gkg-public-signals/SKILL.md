---
name: normalize-gdelt-gkg-public-signals
description: Normalize fetch-gdelt-gkg export snapshots into unified public signals and write them into the signal plane database. Use when investigators need row-level GDELT GKG evidence, artifact refs, provenance, quality flags, and coverage limitations from zipped export files.
---

# Eco Normalize GDELT GKG Public Signals

## Core Goal
- Read one `fetch-gdelt-gkg` manifest artifact plus referenced zip exports.
- Convert zipped GKG rows into canonical public signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced a GDELT GKG export manifest.
- The council needs row-level theme or knowledge-graph evidence instead of only manifest metadata.
- Investigator query, finding, and evidence-bundle work should operate from canonical signal rows.

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
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/normalize_gdelt_gkg_public_signals.py`
