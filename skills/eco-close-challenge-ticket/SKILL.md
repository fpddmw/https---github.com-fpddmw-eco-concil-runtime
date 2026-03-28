---
name: eco-close-challenge-ticket
description: Close a challenge ticket on the local investigation board, preserve a compact resolution trail, and emit an auditable closure event for board organization workflows.
---

# Eco Close Challenge Ticket

## Core Goal
- Close one challenge ticket on the current round's board state.
- Preserve a compact resolution note and related task ids.
- Emit an auditable board event for downstream summary and briefing work.

## Triggering Conditions
- A challenge ticket has been reviewed and no longer needs to stay open.
- A claimed board task produced enough outcome to resolve the challenge.
- Need to reduce board noise before summarizing readiness.

## Read/Write Contract
- Reads and writes `run_dir/board/investigation_board.json` by default.
- Updates one challenge ticket and appends one board event.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `ticket_id`
- Optional:
  - `board_path`
  - `resolution`
  - `resolution_note`
  - `closing_role`
  - `related_task_id`

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
- `scripts/eco_close_challenge_ticket.py`