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

## Scripts
- `scripts/review_spatiotemporal_relation_alternatives.py`
