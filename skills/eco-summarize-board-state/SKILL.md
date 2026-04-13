---
name: eco-summarize-board-state
description: Summarize the current round's investigation board into a compact JSON snapshot with counts, active items, and next-step hints for organization and Phase D handoff.
---

# Eco Summarize Board State

## Core Goal
- Summarize one round's board state into a compact artifact.
- Count active hypotheses, challenge tickets, tasks, notes, and recent events.
- Emit next-step hints that tell the workflow whether the board still needs organization or can move toward Phase D.

## Triggering Conditions
- Need a compact board snapshot before briefing or readiness planning.
- Need to know whether open challenges still lack claimed tasks.
- Need a durable artifact summarizing the current working state.

## Read/Write Contract
- Reads `run_dir/board/investigation_board.json` by default.
- Syncs the round into the run-local deliberation plane before summarizing.
- Writes `run_dir/board/board_state_summary_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_path`
  - `summary_path`
  - `recent_event_limit`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `deliberation_sync`
- `board_handoff`

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/eco_summarize_board_state.py`
