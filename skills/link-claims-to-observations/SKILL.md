---
name: link-claims-to-observations
description: Deprecated WP4 legacy alias. It now blocks old claim/observation matching and points operators to the scoped successor helper.
---

# Eco Link Claims To Observations

## Core Goal
- This skill is a WP4 deprecated alias for the old claim/observation matching path.
- Default execution no longer loads legacy inputs, emits empirical relation objects, syncs link result sets, or suggests follow-on legacy helpers.
- The replacement direction is `review-fact-check-evidence-scope`, which must require an explicit verification question, geographic scope, study period, evidence window, lag assumptions, and metric/source requirements.
- Any successor helper output must remain advisory until a DB council object explicitly cites it.

## Triggering Conditions
- Existing callers need a governed, auditable stop instead of silently running the removed legacy path.
- Operator approval is still required at runtime because the registry classifies this as optional analysis.

## Read/Write Contract
- Writes `runs/<run_id>/analytics/claim_observation_links_<round_id>.json` as a deprecated-helper stop artifact.
- Does not write analysis-plane link rows.
- Does not emit candidate ids for board use.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_cluster_path`
  - `claim_candidates_path`
  - `merged_observations_path`
  - `observation_candidates_path`
  - `output_path`
  - `min_score`
  - `top_links_per_claim`

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
- `scripts/link_claims_to_observations.py`
