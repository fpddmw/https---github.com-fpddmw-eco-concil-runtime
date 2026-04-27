---
name: extract-stance-candidates
description: Derive canonical stance-group objects from canonical issue-cluster rows so typed controversy posture no longer depends on one monolithic map artifact.
---

# Eco Extract Stance Candidates

## Core Goal
- Project canonical `issue-cluster` rows into canonical `stance-group` objects.
- Make stance decomposition queryable and auditable without reopening the whole controversy-map skill.

## Triggering Conditions
- Canonical issue clusters already exist.
- Need explicit stance breakdowns for board, planning, or reporting.

## Read/Write Contract
- Reads issue-cluster results from the run-local analysis plane first.
- Falls back to `run_dir/analytics/issue_clusters_<round_id>.json` when needed.
- Writes `run_dir/analytics/stance_groups_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `stance-group`.

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
- `scripts/extract_stance_candidates.py`
