---
name: normalize-gdelt-doc-public-signals
description: Normalize fetch-gdelt-doc-search article and DOC tone aggregate results into unified public signals and write them into the signal plane database. Use when investigators need canonical public-signal rows, artifact refs, provenance, source quality flags, article discovery refs, DOC timelinetone refs, or DOC tonechart refs.
---

# Eco Normalize GDELT Doc Public Signals

## Core Goal
- Read one `fetch-gdelt-doc-search` raw artifact.
- Convert article-like results, DOC `timelinetone`, and DOC `tonechart`
  results into canonical public signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced a GDELT doc artifact.
- The council needs article-level public signals or DOC-level media/document
  tone aggregates instead of raw search payloads.
- Investigator query, finding, and evidence-bundle work should operate from the unified signal plane rather than files.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes to `signal_artifacts`, `signal_ingest_batches`, and `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate claim candidates directly.
- DOC `articles` normalize as `gdelt_doc_recon`.
- DOC `timelinetone` normalizes as `metric="doc_timeline_tone"` with
  `gdelt_doc_kind="gdelt_doc_tone_aggregate"`.
- DOC `tonechart` normalizes as `metric="doc_tonechart_count"` with
  `gdelt_doc_kind="gdelt_doc_tone_distribution"` and `metadata.tone_bin`.
  Here `numeric_value` is article count in that tone bin, not the tone value.

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

## Agent Reasoning Guide
- Treat normalization as lineage-preserving translation from one raw artifact
  into canonical signal-plane rows. It does not infer claims, public consensus,
  representativeness, issue salience, or readiness.
- GDELT DOC tone outputs describe indexed document/media tone, not public
  response sentiment. Keep them separate from YouTube/Bluesky/formal-comment
  affect or stance labels.
- Zero `canonical_ids` or warnings can reflect artifact shape, unsupported
  provider fields, parser coverage, query/export shape, duplicate replacement,
  or a fetch/normalizer mismatch. It is not proof that public signals are absent.
- Before using a no-row result as a limitation, inspect `warnings`,
  `artifact_refs`, source skill pairing, and record locators; rerun the correct
  normalizer or fetch path when the evidence need remains live.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/normalize_gdelt_doc_public_signals.py`
