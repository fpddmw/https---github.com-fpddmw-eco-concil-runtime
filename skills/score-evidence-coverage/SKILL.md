---
name: score-evidence-coverage
description: Deprecated WP4 legacy alias. It now blocks the old empirical coverage formula and points operators to DB-backed sufficiency review.
---

# Eco Score Evidence Coverage

## Core Goal
- This skill is a WP4 deprecated alias for the old empirical coverage formula.
- Default execution no longer loads legacy link/scope inputs, emits formula outputs, syncs coverage result sets, or suggests follow-on legacy helpers.
- The replacement direction is `review-evidence-sufficiency`, which must review DB-backed findings, evidence bundles, report section basis, and challenger review comments.
- Any successor helper output must remain advisory until a DB council object explicitly cites it.

## Triggering Conditions
- Existing callers need a governed, auditable stop instead of silently running the removed legacy formula.
- Operator approval is still required at runtime because the registry classifies this as optional analysis.

## Read/Write Contract
- Writes `runs/<run_id>/analytics/evidence_coverage_<round_id>.json` as a deprecated-helper stop artifact.
- Does not write analysis-plane coverage rows.
- Does not emit candidate ids for board use.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `links_path`
  - `claim_scope_path`
  - `observation_scope_path`
  - `output_path`

## Output Contract
- `status` is `deprecated-blocked`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids` is empty
- `warnings`
- `analysis_sync.status` is `skipped`
- `board_handoff.suggested_next_skills` is empty
- `wp4_helper_metadata` is written into the stop artifact

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`
- `../../docs/openclaw-wp4-skills-refactor-workplan.md`

## Scripts
- `scripts/score_evidence_coverage.py`
