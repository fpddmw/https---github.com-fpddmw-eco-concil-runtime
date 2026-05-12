---
name: normalize-airnow-observation-signals
description: Normalize fetch-airnow-hourly-observations results into unified environment signals and write them into the signal plane database. Use when investigators need canonical AirNow observation rows, artifact refs, provenance, quality flags, temporal/spatial scope, and coverage limitations.
---

# Eco Normalize AirNow Observation Signals

## Core Goal
- Read one `fetch-airnow-hourly-observations` raw artifact.
- Convert station observations into canonical environment signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced AirNow hourly observations.
- The council needs station-level physical evidence instead of raw file products.
- Investigator query, finding, and evidence-bundle work should operate from the unified signal plane.

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
  - `metric_allowlist`

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
  into canonical signal-plane rows. It does not decide whether an environmental
  condition happened, mattered, or supports a council claim.
- Zero `canonical_ids` or warnings can reflect artifact shape, unsupported
  provider fields, metric allowlists, parser coverage, duplicate replacement, or
  a fetch/normalizer mismatch. It is not proof that observations do not exist.
- Before using a no-row result as a limitation, inspect `warnings`,
  `artifact_refs`, source skill pairing, and record locators; rerun the correct
  normalizer or fetch path when the evidence need remains live.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/normalize_airnow_observation_signals.py`
