---
name: aggregate-environment-evidence
description: Optional-analysis helper for DB-backed environment signal aggregation. It summarizes source, metric, spatial, and temporal coverage without claim matching or readiness scoring.
---

# Aggregate Environment Evidence

## Core Goal
- Read normalized environment signals from the signal-plane DB.
- Produce descriptive source, metric, spatial, and temporal aggregation.
- Preserve source signal ids, artifact refs, record locators, lineage, provenance, and optional-analysis helper governance.
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

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. It summarizes visible
  normalized environment rows; it does not match claims, rank sources, score
  sufficiency, or decide readiness.
- Empty or narrow aggregation can reflect missing normalization, filters,
  `round_id`, DB path, or source coverage. It is not proof that environmental
  evidence is absent.
- A council agent must carry useful cues into a finding, evidence bundle,
  challenge, proposal, readiness opinion, or synthesis before downstream use.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/aggregate_environment_evidence.py`
