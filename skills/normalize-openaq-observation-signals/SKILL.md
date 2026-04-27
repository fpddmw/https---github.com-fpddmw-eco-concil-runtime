---
name: normalize-openaq-observation-signals
description: Normalize fetch-openaq raw measurement artifacts into environment signals and write them into the signal plane database. Use when investigators need canonical OpenAQ observation rows, artifact refs, provenance, quality flags, temporal/spatial scope, and coverage limitations without board judgement.
---

# Eco Normalize OpenAQ Observation Signals

## Core Goal
- Read one `fetch-openaq` raw artifact.
- Convert station measurements into canonical environment signals.
- Preserve provider, station, metric, value, timestamp, coordinate metadata, fetch contract, data quality, and coverage limitations.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and query-oriented handoff only.
- Do not infer exposure, compliance, causation, readiness, or policy conclusions.

## Triggering Conditions
- A fetch step already produced OpenAQ API or archive output.
- An environmental investigator needs station-level evidence rows instead of raw provider payloads.
- Later investigation should start from DB query surfaces, not from a default observation-extraction chain.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes to `signal_artifacts`, `signal_ingest_batches`, and `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate observation candidates directly.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `artifact_path`
- Optional:
  - `db_path`
  - `source_mode`

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
- `scripts/normalize_openaq_observation_signals.py`
