---
name: eco-normalize-youtube-comments-public-signals
description: Normalize youtube-comments-fetch results into unified public signals and write them into the signal plane database. Use when tasks need canonical YouTube comment rows, artifact refs, and board-ready hints for downstream claim extraction.
---

# Eco Normalize YouTube Comments Public Signals

## Core Goal
- Read one `youtube-comments-fetch` raw artifact.
- Convert comment-thread records into canonical public signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and board-ready hints.

## Triggering Conditions
- A fetch step already produced YouTube comments output.
- The council needs canonical public-language comment evidence instead of provider-native payloads.
- Downstream claim extraction should operate from normalized signals.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes normalized rows into `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate claim candidates directly.

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
- `scripts/eco_normalize_youtube_comments_public_signals.py`
