---
name: eco-merge-observation-candidates
description: Merge nearby observation candidates into stable observation groups, retain multi-source provenance, and persist a compact merged-observation artifact for downstream evidence linking and board review.
---

# Eco Merge Observation Candidates

## Core Goal
- Read the current round's observation candidate artifact.
- Merge nearby candidates that describe the same physical window.
- Persist a merged-observation artifact for downstream evidence linking and moderation.

## Triggering Conditions
- Need a lighter review surface than a flat observation-candidate list.
- Need to collapse cross-source or near-duplicate observation candidates before linking.
- Need stable merged observation ids and provenance refs for board work.

## Read/Write Contract
- Reads `observation_candidates_<round_id>.json` by default.
- Writes `runs/<run_id>/analytics/merged_observation_candidates_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `input_path`
  - `output_path`
  - `metric`
  - `source_skill`
  - `point_precision`
  - `max_groups`

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
- `scripts/eco_merge_observation_candidates.py`