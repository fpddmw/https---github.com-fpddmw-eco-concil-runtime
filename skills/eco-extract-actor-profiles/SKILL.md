---
name: eco-extract-actor-profiles
description: Derive canonical actor-profile objects from canonical issue-cluster rows so actor posture becomes a first-class DB-native issue surface.
---

# Eco Extract Actor Profiles

## Core Goal
- Project canonical `issue-cluster` rows into canonical `actor-profile` objects.
- Make actor-centric controversy context reusable outside the monolithic map skill.

## Triggering Conditions
- Canonical issue clusters already exist.
- Need explicit actor posture context for board, agenda, or reporting decisions.

## Read/Write Contract
- Reads issue-cluster results from the run-local analysis plane first.
- Falls back to `run_dir/analytics/issue_clusters_<round_id>.json` when needed.
- Writes `run_dir/analytics/actor_profiles_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `actor-profile`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `issue_clusters_path`
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
- `board_handoff`

## References
- `../../docs/openclaw-next-phase-development-plan.md`
- `../../docs/openclaw-skill-refactor-checklist.md`

## Scripts
- `scripts/eco_extract_actor_profiles.py`
