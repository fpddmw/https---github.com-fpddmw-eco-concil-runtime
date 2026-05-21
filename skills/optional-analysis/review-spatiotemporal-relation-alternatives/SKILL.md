---
name: review-spatiotemporal-relation-alternatives
description: Optional-analysis helper that turns DB-backed spatiotemporal relation cues into structured challenger objection candidates.
---

# Review Spatiotemporal Relation Alternatives

## Core Goal
- Read `spatiotemporal-relation-cue` analysis-plane result items.
- Emit objection candidates using the approved relation objection taxonomy.
- Keep outputs approval-gated and advisory; do not open or close challenges directly.

## Read/Write Contract
- Reads analysis result tables in `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/spatiotemporal_relation_alternative_reviews_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `relation_id`
- `relation_status`
- `output_path`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory material for challengers. Objection
  candidates do not open or close challenges and do not disprove a relation by
  themselves.
- Empty or sparse alternatives can reflect missing relation cues, filters, or
  analysis-plane import scope. They do not prove that no counter-explanation
  exists.
- A council agent must carry useful alternatives into a challenge, probe, review
  comment, finding, or evidence packet before downstream use.

## Scripts
- `scripts/review_spatiotemporal_relation_alternatives.py`
