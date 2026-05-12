---
name: normalize-fetch-execution
description: Execute the current actor's slice of one prepared fetch-plan through explicit queue-runner, normalizer-runner, and execution-receipt components. Use when an investigator needs their assigned raw artifacts copied/fetched, normalized into DB signal rows, and recorded without selecting downstream analysis conclusions.
---

# Eco Import Fetch Execution

## Core Goal
- Read the prepared fetch plan for the current round.
- Resolve the executing actor role from `--actor-role` or the runtime-injected `OPENCLAW_ACTOR_ROLE`.
- Run only fetch-plan steps owned by that actor's role, for example `social-investigator` for `social_investigator` steps and `environmental-investigator` for `environmental_investigator` steps.
- Run the `queue_runner` component for owned steps: copy local imports or execute approved detached-fetch steps into the current run raw store.
- Run the `normalizer_runner` component for owned steps: invoke mapped normalizer skills or keep a raw-only receipt when no normalizer exists.
- In recovery mode, accept explicit `--receipt-id` or `--receipt-path` values, materialize a raw artifact from the runtime receipt when needed, and invoke the mapped normalizer without requiring a fetch-plan step.
- Run the `execution_receipt` component: write one auditable execution snapshot with step status, raw artifact refs, normalizer receipts, and warnings.
- Do not choose claim extraction, observation extraction, coverage scoring, readiness, report basis, or any other analysis chain.

## Triggering Conditions
- `prepare-round` already wrote `fetch_plan_<round_id>.json`.
- The current ingress path may contain both local artifact imports and detached fetch requests.
- Each investigator runs their own role slice; runtime-operator approves/governs but does not execute data fetch or normalization.
- A governed fetch receipt exists but did not enter `normalized_signals`, especially when an agent executed a fetch directly instead of through `fetch_plan_<round_id>.json`.
- Downstream extraction should start from normalized signal-plane data rather than direct seed helpers.

## Read/Write Contract
- Reads `run_dir/runtime/fetch_plan_<round_id>.json`.
- May read `run_dir/runtime/receipts/<receipt_id>.json` in receipt-driven recovery mode.
- Writes `run_dir/raw/<round_id>`.
- Writes `run_dir/analytics/signal_plane.sqlite`.
- Writes `run_dir/runtime/import_execution_<round_id>.json`.
- Output `board_handoff.suggested_next_skills` is limited to DB query surfaces.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `actor_role` through `--actor-role` or runtime `OPENCLAW_ACTOR_ROLE`
- Optional:
  - `receipt_id`
  - `receipt_path`

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
