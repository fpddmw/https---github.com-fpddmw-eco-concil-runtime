---
name: eco-normalize-openaq-observation-signals
description: Normalize openaq-data-fetch results into unified environment signals and write them into the signal plane database. Use when tasks need canonical OpenAQ observation rows, artifact refs, and board-ready hints for downstream observation extraction.
---

# Eco Normalize OpenAQ Observation Signals

## Core Goal
- Read one `openaq-data-fetch` raw artifact.
- Convert station measurements into canonical environment signals.
- Write artifact, ingest-batch, and signal rows into the unified signal plane.
- Return compact summary, receipt id, batch id, artifact refs, signal ids, and board handoff hints.

## Triggering Conditions
- A fetch step already produced OpenAQ API or archive output.
- The council needs station-level physical evidence instead of raw provider payloads.
- Later observation extraction should operate from the unified signal plane.

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
- `scripts/eco_normalize_openaq_observation_signals.py`