---
name: eco-derive-observation-scope
description: Derive matching-oriented observation scope proposals from observation-side evidence objects, normalize place and metric tags, and persist a compact scope artifact for downstream evidence coverage and board work.
---

# Eco Derive Observation Scope

## Core Goal
- Read merged observations if available, otherwise fall back to observation candidates.
- Infer compact observation scope proposals for matching and challenge review.
- Persist a scope artifact for evidence coverage and board workflows.

## Triggering Conditions
- Observation-side evidence exists but matching-oriented scope fields are still uneven.
- Need stable point or metric tags before evidence coverage scoring or board use.
- Need a bridge artifact between evidence linking and board review.

## Read/Write Contract
- Reads `merged_observation_candidates_<round_id>.json` by default and falls back to `observation_candidates_<round_id>.json`.
- Writes `runs/<run_id>/analytics/observation_scope_proposals_<round_id>.json` by default.

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
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_derive_observation_scope.py`