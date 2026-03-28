---
name: eco-normalize-bluesky-cascade-public-signals
description: Normalize bluesky-cascade-fetch thread and post results into unified public signals and write them into the signal plane database. Use when tasks need canonical social-thread evidence rows, artifact refs, and board-ready challenge hints from Bluesky cascade output.
---

# Eco Normalize Bluesky Cascade Public Signals

## Core Goal
- Read one `bluesky-cascade-fetch` raw artifact.
- Convert posts and reply nodes into canonical public signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and board handoff hints.

## Triggering Conditions
- A fetch step already produced Bluesky cascade output.
- The council needs canonical social-thread evidence instead of raw thread payloads.
- Later claim clustering or challenge work should read from the unified signal plane.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes to `signal_artifacts`, `signal_ingest_batches`, and `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate claim candidates directly.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `artifact_path`
- Optional:
  - `db_path`
  - `dedupe_by_uri`

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
- `scripts/eco_normalize_bluesky_cascade_public_signals.py`