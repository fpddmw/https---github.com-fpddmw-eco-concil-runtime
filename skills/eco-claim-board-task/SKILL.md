---
name: eco-claim-board-task
description: Claim or upsert a board follow-up task on the local investigation board, preserve owner and source ids, and emit an auditable task event for board organization workflows.
---

# Eco Claim Board Task

## Core Goal
- Claim one board follow-up task on the current round's board state.
- Preserve owner role, source ticket or hypothesis ids, and linked evidence refs.
- Emit an auditable board event for downstream organization and review work.

## Triggering Conditions
- Need to turn an open challenge or hypothesis into an explicitly owned follow-up task.
- Need to claim an existing board task for a specific role.
- Need a durable task object before summarizing or briefing the board state.

## Read/Write Contract
- Reads and writes `run_dir/board/investigation_board.json` by default.
- Appends or updates one board task and one board event.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `title`
- Optional:
  - `board_path`
  - `task_id`
  - `task_text`
  - `task_type`
  - `status`
  - `owner_role`
  - `priority`
  - `source_ticket_id`
  - `source_hypothesis_id`
  - `linked_artifact_ref`
  - `related_id`

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
- `scripts/eco_claim_board_task.py`