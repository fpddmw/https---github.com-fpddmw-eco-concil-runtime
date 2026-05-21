---
name: normalize-open-meteo-air-quality-signals
description: Normalize fetch-open-meteo-air-quality results into unified environment signals and write them into the signal plane database. Use when investigators need canonical modeled air-quality rows, artifact refs, provenance, quality flags, temporal/spatial scope, and coverage limitations.
---

# Eco Normalize Open-Meteo Air Quality Signals

## Core Goal
- Read one `fetch-open-meteo-air-quality` raw artifact.
- Convert hourly modeled air-quality records into canonical environment signals.
- Write normalized rows into the unified signal plane.
- Return compact receipts, artifact refs, signal ids, and query-oriented handoff.

## Triggering Conditions
- A fetch step already produced Open-Meteo air-quality output.
- The council needs canonical modeled air-quality observations instead of provider-native payloads.
- Investigator query, finding, and evidence-bundle work should operate from normalized signals.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes normalized rows into `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Does not generate observation candidates directly.

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
- `scripts/normalize_open_meteo_air_quality_signals.py`
