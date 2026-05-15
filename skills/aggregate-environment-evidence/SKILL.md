---
name: aggregate-environment-evidence
description: Optional-analysis helper for DB-backed environment signal aggregation. It summarizes source, metric, spatial, and temporal coverage without claim matching, risk scoring, source ranking, source attribution, or readiness scoring.
---

# Aggregate Environment Evidence

## Core Goal
- Read normalized environment signals from the signal-plane DB.
- Produce descriptive coverage, time-series, and point-event summaries.
- Preserve source signal ids, artifact refs, record locators, lineage, provenance, and optional-analysis helper governance.
- Avoid claim matching, source ranking, evidence weighting, risk scoring, physical source attribution, readiness decisions, phase gates, or workflow suggestions.

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
- `aggregation_method`: `coverage-summary`, `time-series-summary`, `point-event-summary`, or `auto-summary`; the legacy `source-metric-day-summary` is accepted as a compatibility alias.
- `round_scope`: `current`, `up-to-current`, or `all`
- `source_skill`
- `metric`
- `observed_after_utc`
- `observed_before_utc`
- `bbox`
- `output_path`
- `limit`: output/sample row limit only; statistics are computed across matched DB rows.
- `group_limit`
- `sample_ref_limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. It summarizes visible
  normalized environment rows; it does not match claims, rank sources, score
  sufficiency, score risk, assign physical source attribution, or decide
  readiness.
- `time-series-summary` reports descriptive count/min/max/mean by
  source/location/metric, plus date buckets with count/min/max/mean. Point-event
  records such as FIRMS detections stay out of this continuous-series grouping.
  These extrema are not exposure, severity, transport, or attribution findings.
- `point-event-summary` reports date buckets, spatial envelope, provider
  metadata distribution, and numeric metadata statistics such as FIRMS FRP when
  present. Point density is only normalized row density.
- Empty or narrow aggregation can reflect missing normalization, filters,
  `round_id`, DB path, or source coverage. It is not proof that environmental
  evidence is absent.
- A council agent must carry useful cues into a finding, evidence bundle,
  challenge, proposal, readiness opinion, or synthesis before downstream use.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/aggregate_environment_evidence.py`
