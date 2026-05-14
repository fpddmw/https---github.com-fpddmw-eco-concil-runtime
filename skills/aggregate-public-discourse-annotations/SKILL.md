---
name: aggregate-public-discourse-annotations
description: Optional-analysis helper for aggregating sample-level public discourse annotation labels without generating sentiment or public-opinion conclusions.
---

# Aggregate Public Discourse Annotations

## Core Goal
- Read a DB-backed public discourse corpus and approved taxonomy cues, annotation-worker output, or agent-authored annotation JSON/JSONL.
- Aggregate issue, affect, source narrative, actor responsibility, and action orientation labels inside the selected sample.
- Avoid global default sentiment taxonomies, source ranking, public-opinion inference, or report conclusions.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Optionally reads `public_discourse_corpus_<round_id>.json`
- Optionally reads approved taxonomy label cues, `classify-public-discourse-affect` output, or agent-authored annotation JSON/JSONL with an annotation basis ref
- Writes `run_dir/analytics/public_discourse_annotation_aggregation_<round_id>.json`
- Defaults to `--round-scope current`; use `--round-scope run` when the
  approved corpus intentionally spans multiple rounds in the same run.

## Agent Reasoning Guide
- Treat annotation distributions as advisory, approval-scoped sample descriptors.
  They describe only annotated items in the selected corpus and do not prove
  prevalence outside that corpus.
- Annotation-worker output may carry its own `annotation_basis_ref`; manual
  agent-authored annotations require an explicit annotation basis ref. Approved
  taxonomy cues remain candidate labels for human review and are not findings by
  themselves.
- A council agent must carry useful distributions into a finding, evidence
  bundle, challenge, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/aggregate_public_discourse_annotations.py`
