---
name: aggregate-environment-evidence
description: WP4 optional helper for DB-backed environment signal aggregation. It summarizes source, metric, spatial, and temporal coverage without claim matching or readiness scoring.
---

# Aggregate Environment Evidence

## Core Goal
- Read normalized environment signals from the signal-plane DB.
- Produce descriptive source, metric, spatial, and temporal aggregation.
- Preserve source signal ids, artifact refs, record locators, lineage, provenance, and WP4 helper metadata.
- Avoid claim matching, readiness scores, phase gates, or workflow suggestions.

## Triggering Conditions
- An approved optional-analysis request asks for an environment evidence aggregation view.
- Investigators need a human-auditable coverage summary before writing findings or evidence bundles.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/environment_evidence_aggregation_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `aggregation_method`
- `output_path`
- `limit`

## References
- `../../docs/openclaw-wp4-skills-refactor-workplan.md`

## Scripts
- `scripts/aggregate_environment_evidence.py`
