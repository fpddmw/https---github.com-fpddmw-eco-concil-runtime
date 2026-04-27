---
name: link-claims-to-observations
description: Legacy optional empirical-link helper for explicitly routed observation questions. It requires operator-approved skill approval and must not be a default report evidence basis.
---

# Eco Link Claims To Observations

## Core Goal
- Read claim-side and observation-side candidate or grouped result sets from the analysis plane, with artifact-compatible fallback behavior.
- Propose support, contradiction, or contextual links only for issues explicitly routed to an empirical observation lane.
- Persist a link artifact with rule traces and provenance for audit or challenge work.
- Keep claim-observation matching out of the default policy research workflow and out of reporting basis unless explicitly selected and approved.
- Require `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` before execution.

## Triggering Conditions
- A moderator or investigator has an approved empirical question with time/place/source scope.
- The operator has approved this optional-analysis run for the current round and actor role.
- Need claim-to-observation evidence hypotheses as audit material, not as the default investigation frame.
- Need stable link ids, rule traces, and provenance refs for auditability.

## Read/Write Contract
- Loads `claim-cluster` result sets from the analysis plane first and falls back to `claim-candidate` results only when the preferred claim-side grouping is missing.
- Loads `merged-observation` result sets from the analysis plane first and falls back to `observation-candidate` results only when the preferred observation-side grouping is missing.
- Uses `claim_candidate_clusters_<round_id>.json`, `claim_candidates_<round_id>.json`, `merged_observation_candidates_<round_id>.json`, and `observation_candidates_<round_id>.json` as the default artifact paths behind those result kinds.
- Writes `runs/<run_id>/analytics/claim_observation_links_<round_id>.json` by default.
- Syncs the same link result set into `runs/<run_id>/analytics/signal_plane.sqlite` as analysis-plane state.

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
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `analysis_sync`
- `input_analysis_sync`
- `board_handoff`

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/link_claims_to_observations.py`
