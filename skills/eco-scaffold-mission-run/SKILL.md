---
name: eco-scaffold-mission-run
description: Scaffold one mission-driven run by materializing mission.json, first-round task inputs, and seeded board hypotheses before prepare-round executes.
---

# Eco Scaffold Mission Run

## Core Goal
- Read one mission contract.
- Materialize the active `mission.json` for the run.
- Seed first-round task inputs for prepare-round.
- Seed initial board hypotheses so downstream board and readiness skills do not start from an empty round.

## Triggering Conditions
- A new run needs to be created from a mission contract.
- The workflow should start from mission input instead of direct test seeding.
- The next step will be `eco-prepare-round`.

## Read/Write Contract
- Reads `<mission_path>`.
- Writes `run_dir/mission.json`.
- Writes `run_dir/investigation/round_tasks_<round_id>.json`.
- Writes `run_dir/board/investigation_board.json`.
- Writes `run_dir/runtime/mission_scaffold_<round_id>.json`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `mission_path`
- Optional:
  - `hypothesis_confidence`
  - `orchestration_mode`

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
- `scripts/eco_scaffold_mission_run.py`