---
name: eco-prepare-round
description: Build one governed fetch plan from mission.json, round tasks, and source selections so runtime queue mode can mix local imports with detached fetch requests for the current round.
---

# Eco Prepare Round

## Core Goal
- Read the scaffolded mission and round task inputs.
- Materialize source-selection snapshots for the current round.
- Build one auditable fetch plan for the current round.
- Map each selected source to either an import step or a detached-fetch step plus the correct normalizer skill.

## Triggering Conditions
- A mission has already been scaffolded into the current run.
- The next step should create a governed fetch plan before normalization starts.
- The workflow should not depend on direct ad hoc local seed execution.

## Read/Write Contract
- Reads `run_dir/mission.json`.
- Reads `run_dir/investigation/round_tasks_<round_id>.json`.
- Writes `run_dir/runtime/source_selection_<role>_<round_id>.json`.
- Writes `run_dir/runtime/fetch_plan_<round_id>.json`.

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
- `scripts/eco_prepare_round.py`