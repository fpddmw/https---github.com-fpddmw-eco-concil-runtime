---
name: eco-derive-observation-scope
description: Derive matching-oriented observation scope proposals from observation-side evidence objects, normalize place and metric tags, and persist a compact scope artifact for downstream evidence coverage and board work.
---

# Eco Derive Observation Scope

## Core Goal
- Read merged observations from the analysis plane when available, otherwise fall back to observation candidates.
- Infer compact observation scope proposals for matching and challenge review.
- Persist a scope artifact for evidence coverage and board workflows.

## Triggering Conditions
- Observation-side evidence exists but matching-oriented scope fields are still uneven.
- Need stable point or metric tags before evidence coverage scoring or board use.
- Need a bridge artifact between evidence linking and board review.

## Read/Write Contract
- Loads `merged-observation` result sets from the analysis plane first and falls back to `observation-candidate` results only when the preferred merged result is missing.
- Uses `merged_observation_candidates_<round_id>.json` / `observation_candidates_<round_id>.json` as the default artifact paths behind those result kinds.
- Writes `runs/<run_id>/analytics/observation_scope_proposals_<round_id>.json` by default.
- Syncs the same observation-scope result set into `runs/<run_id>/analytics/signal_plane.sqlite` as analysis-plane state.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `merged_observations_path`
  - `observation_candidates_path`
  - `output_path`

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
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/eco_derive_observation_scope.py`
