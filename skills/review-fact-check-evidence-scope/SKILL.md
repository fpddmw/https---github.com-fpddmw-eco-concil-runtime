---
name: review-fact-check-evidence-scope
description: Optional-analysis helper for structured verification scope review. It requires a verification question, receptor/source scope, study/evidence windows, lag/spatial rules, required roles/classes, and excluded inferences; it emits scope caveats only.
---

# Review Fact Check Evidence Scope

## Core Goal
- Require explicit structured `verification_scope` before any environment-evidence review.
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
- `receptor_scope`
- `candidate_source_scope`
- `study_period`
- `evidence_window`
- `lag_window`
- `spatial_rule`
- `required_source_roles`
- `required_target_roles`
- `required_context_classes`
- `excluded_inferences`

## Compatibility Inputs
- `verification_scope_json` may provide the structured scope as a JSON object or path.
- Legacy `geographic_scope`, `lag_assumptions`, `metric_requirements`, and `source_requirements` are accepted only as compatibility inputs and do not replace required role/class fields.

## Agent Reasoning Guide
- Treat output as approval-scoped scope review. It emits caveats about whether
  available evidence matches the requested verification scope; it does not decide
  factual outcome, route assignment, readiness, or report conclusions.
- Empty or weak coverage can reflect the explicit scope, missing normalization,
  filters, or excluded inference rules. It is not proof that the fact question is
  false or unanswerable.
- A council agent must carry useful caveats into a finding, evidence bundle,
  challenge, proposal, readiness opinion, or synthesis before downstream use.

## References
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/review_fact_check_evidence_scope.py`
