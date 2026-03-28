---
name: eco-normalize-youtube-video-public-signals
description: Normalize youtube-video-search results into unified public signals and write them into the signal plane database. Use when tasks need canonical video-level public evidence rows, artifact refs, and board-ready hints from YouTube discovery output.
---

# Eco Normalize YouTube Video Public Signals

## Core Goal
- Read one `youtube-video-search` raw artifact.
- Convert video records into canonical public signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and board handoff hints.

## Triggering Conditions
- A fetch step already produced YouTube video search output.
- The council needs video-level public evidence instead of raw platform payloads.
- Later claim extraction should operate from the unified signal plane.

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
  - `query_text_override`

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
- `scripts/eco_normalize_youtube_video_public_signals.py`