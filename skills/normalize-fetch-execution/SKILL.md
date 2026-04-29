---
name: normalize-fetch-execution
description: Execute one prepared fetch-plan through explicit queue-runner, normalizer-runner, and execution-receipt components. Use when investigators or operators need raw artifacts copied/fetched, normalized into DB signal rows, and recorded without selecting downstream analysis conclusions.
---

# Eco Import Fetch Execution

## Core Goal
- Read the prepared fetch plan for the current round.
- Run the `queue_runner` component: copy local imports or execute approved detached-fetch steps into the current run raw store.
- Run the `normalizer_runner` component: invoke mapped normalizer skills or keep a raw-only receipt when no normalizer exists.
- Run the `execution_receipt` component: write one auditable execution snapshot with step status, raw artifact refs, normalizer receipts, and warnings.
- Do not choose claim extraction, observation extraction, coverage scoring, readiness, report basis, or any other analysis chain.

## Triggering Conditions
- `prepare-round` already wrote `fetch_plan_<round_id>.json`.
- The current ingress path may contain both local artifact imports and detached fetch requests.
- Downstream extraction should start from normalized signal-plane data rather than direct seed helpers.

## Read/Write Contract
- Reads `run_dir/runtime/fetch_plan_<round_id>.json`.
- Writes `run_dir/raw/<round_id>`.
- Writes `run_dir/analytics/signal_plane.sqlite`.
- Writes `run_dir/runtime/import_execution_<round_id>.json`.
- Output `board_handoff.suggested_next_skills` is limited to DB query surfaces.

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
- `execution_components`

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/normalize_fetch_execution.py`
