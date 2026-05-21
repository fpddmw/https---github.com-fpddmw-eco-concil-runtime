---
name: normalize-gdelt-mentions-public-signals
description: Normalize fetch-gdelt-mentions export snapshots into unified public signals and write them into the signal plane database. Use when investigators need row-level GDELT mention evidence, artifact refs, provenance, quality flags, and coverage limitations from zipped export files.
---

# Eco Normalize GDELT Mentions Public Signals

## Core Goal
- Read one `fetch-gdelt-mentions` manifest artifact plus referenced zip exports.
- Convert zipped mention rows into canonical public signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced a GDELT Mentions export manifest.
- The council needs row-level mention evidence instead of only manifest metadata.
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
- `scripts/normalize_gdelt_mentions_public_signals.py`
