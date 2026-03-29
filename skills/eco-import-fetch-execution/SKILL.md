---
name: eco-import-fetch-execution
description: Execute one local fetch-plan by copying raw artifacts into the run, calling the mapped normalizer skills, and writing an import execution snapshot.
---

# Eco Import Fetch Execution

## Core Goal
- Read the prepared fetch plan for the current round.
- Copy local raw artifacts into the current run raw store.
- Invoke the matching normalizer skills.
- Write one auditable import execution snapshot.

## Triggering Conditions
- `eco-prepare-round` already wrote `fetch_plan_<round_id>.json`.
- The current ingress path is local artifact import rather than remote fetch execution.
- Downstream extraction should start from normalized signal-plane data rather than direct seed helpers.

## Read/Write Contract
- Reads `run_dir/runtime/fetch_plan_<round_id>.json`.
- Writes `run_dir/raw/<round_id>`.
- Writes `run_dir/analytics/signal_plane.sqlite`.
- Writes `run_dir/runtime/import_execution_<round_id>.json`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

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
- `../../openclaw-skill-phase-plan.md`

## Scripts
- `scripts/eco_import_fetch_execution.py`