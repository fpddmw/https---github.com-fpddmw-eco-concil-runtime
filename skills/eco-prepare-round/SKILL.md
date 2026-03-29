---
name: eco-prepare-round
description: Build one minimal fetch plan from mission.json and round tasks so local artifact import can call the correct normalizer skills for the current round.
---

# Eco Prepare Round

## Core Goal
- Read the scaffolded mission and round task inputs.
- Build one auditable fetch plan for the current round.
- Map each artifact import to the correct raw contract path and normalizer skill.

## Triggering Conditions
- A mission has already been scaffolded into the current run.
- The next step should create a stable import plan before normalization starts.
- The workflow should not depend on direct ad hoc local seed execution.

## Read/Write Contract
- Reads `run_dir/mission.json`.
- Reads `run_dir/investigation/round_tasks_<round_id>.json`.
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
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-skill-phase-plan.md`

## Scripts
- `scripts/eco_prepare_round.py`