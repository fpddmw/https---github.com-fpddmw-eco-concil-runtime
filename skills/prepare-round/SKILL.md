---
name: prepare-round
description: Build one governed fetch plan from mission.json, round tasks, and source selections so runtime queue mode can mix local imports with detached fetch requests for the current round.
---

# Eco Prepare Round

## Core Goal
- Read the scaffolded mission and round task inputs.
- Read the latest `round-brief` as optional coordination context when one exists.
- Materialize source-selection snapshots for the current round.
- Build one auditable fetch plan for the current round.
- Map each selected source to either an import step or a detached-fetch step plus the correct normalizer skill.
- Keep round-brief content as agent-visible context only; it must not become a hard agenda, ranking rule, source filter, or evidence-admission rule.

## Triggering Conditions
- A mission has already been scaffolded into the current run.
- The next step should create a governed fetch plan before normalization starts.
- The workflow should not depend on direct ad hoc local seed execution.

## Read/Write Contract
- Reads `run_dir/mission.json`.
- Reads `run_dir/investigation/round_tasks_<round_id>.json`.
- Reads latest `round-brief` from the deliberation plane when present.
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

## Scripts
- `scripts/prepare_round.py`
