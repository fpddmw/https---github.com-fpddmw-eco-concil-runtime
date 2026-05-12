---
name: close-challenge-ticket
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
- Reads the shared deliberation plane first and exports `run_dir/board/investigation_board.json` for compatibility.
- Bootstraps existing board JSON into the deliberation plane when needed.
- Updates one challenge ticket and appends one board event on the deliberation plane.

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
  - Includes `db_path`
  - Includes `write_surface`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- Closing a challenge is a council workflow action, not proof that the challenged
  claim is true or false.
- The resolution should cite the finding, evidence bundle, disposition, or
  readiness object that actually carries the reasoning.
- If the issue is only being deferred or bounded out of the report, record that
  limitation instead of treating closure as substantive resolution.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/close_challenge_ticket.py`
