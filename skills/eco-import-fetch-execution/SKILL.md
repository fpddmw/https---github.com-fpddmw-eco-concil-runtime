---
name: eco-import-fetch-execution
description: Execute one prepared fetch-plan by running mixed local-import and detached-fetch steps, invoking the mapped normalizer skills, and writing an execution snapshot.
---

# Eco Import Fetch Execution

## Core Goal
- Read the prepared fetch plan for the current round.
- Execute mixed import and detached-fetch steps into the current run raw store.
- Invoke the matching normalizer skills.
- Write one auditable execution snapshot.

## Triggering Conditions
- `eco-prepare-round` already wrote `fetch_plan_<round_id>.json`.
- The current ingress path may contain both local artifact imports and detached fetch requests.
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
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/eco_import_fetch_execution.py`