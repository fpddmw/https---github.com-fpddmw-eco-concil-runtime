---
name: normalize-gdelt-doc-public-signals
description: Normalize fetch-gdelt-doc-search article results into unified public signals and write them into the signal plane database. Use when investigators need canonical public-signal rows, artifact refs, provenance, source quality flags, and coverage limitations for article-like GDELT discovery output.
---

# Eco Normalize GDELT Doc Public Signals

## Core Goal
- Read one `fetch-gdelt-doc-search` raw artifact.
- Convert article-like results into canonical public signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced a GDELT doc artifact.
- The council needs article-level public signals instead of raw search payloads.
- Investigator query, finding, and evidence-bundle work should operate from the unified signal plane rather than files.

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
  - `max_records`

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
- `scripts/normalize_gdelt_doc_public_signals.py`
