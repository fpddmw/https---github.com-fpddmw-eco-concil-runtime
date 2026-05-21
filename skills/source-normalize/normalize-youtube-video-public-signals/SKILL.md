---
name: normalize-youtube-video-public-signals
description: Normalize fetch-youtube-video-search results into unified public signals and write them into the signal plane database. Use when investigators need canonical video-level public evidence rows, artifact refs, provenance, platform quality flags, and coverage limitations from YouTube discovery output.
---

# Eco Normalize YouTube Video Public Signals

## Core Goal
- Read one `fetch-youtube-video-search` raw artifact.
- Convert video records into canonical public signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced YouTube video search output.
- The council needs video-level public evidence instead of raw platform payloads.
- Investigator query, finding, and evidence-bundle work should operate from the unified signal plane.

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

## Agent Reasoning Guide
- Treat normalization as lineage-preserving translation from one raw artifact
  into canonical signal-plane rows. It does not infer claims, public consensus,
  representativeness, issue salience, or readiness.
- Zero `canonical_ids` or warnings can reflect artifact shape, unsupported
  provider fields, parser coverage, query/export shape, duplicate replacement,
  or a fetch/normalizer mismatch. It is not proof that public signals are absent.
- Before using a no-row result as a limitation, inspect `warnings`,
  `artifact_refs`, source skill pairing, and record locators; rerun the correct
  normalizer or fetch path when the evidence need remains live.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/normalize_youtube_video_public_signals.py`
