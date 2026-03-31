---
name: eco-normalize-regulationsgov-comments-public-signals
description: Normalize regulationsgov-comments-fetch results into unified public signals and write them into the signal plane database. Use when tasks need canonical policy-comment rows, artifact refs, and board-ready hints for downstream claim extraction.
---

# Eco Normalize Regulations.gov Comments Public Signals

## Core Goal
- Read one `regulationsgov-comments-fetch` raw artifact.
- Convert comment-list records into canonical public signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and board-ready hints.

## Triggering Conditions
- A fetch step already produced Regulations.gov comments output.
- The council needs canonical policy-comment evidence instead of provider-native payloads.
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
- `../../openclaw-first-refactor-blueprint.md`

## Scripts
- `scripts/eco_normalize_regulationsgov_comments_public_signals.py`
