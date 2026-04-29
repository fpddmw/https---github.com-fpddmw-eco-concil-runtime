---
name: review-fact-check-evidence-scope
description: Optional-analysis helper for explicit fact-check evidence scope review. It requires question, place, period, window, lag, metric, and source requirements and emits scope caveats only.
---

# Review Fact Check Evidence Scope

## Core Goal
- Require explicit verification scope before any environment-evidence review.
- Read DB-backed environment signals and emit scope coverage notes.
- Avoid factual outcome labels, claim matching, route assignment, readiness scores, or report conclusions.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/fact_check_evidence_scope_review_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `verification_question`
- `geographic_scope`
- `study_period`
- `evidence_window`
- `lag_assumptions`
- `metric_requirements`
- `source_requirements`

## References
- `../../docs/openclaw-optional-analysis-skills-refactor-workplan.md`

## Scripts
- `scripts/review_fact_check_evidence_scope.py`
