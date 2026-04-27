---
name: open-challenge-ticket
description: Open a challenge ticket on the local investigation board, preserve target ids and linked evidence refs, and emit an auditable board event for challenger workflows.
---

# Eco Open Challenge Ticket

## Core Goal
- Open one challenge ticket on the current round's board state.
- Preserve target claim or hypothesis ids and linked evidence refs.
- Emit an auditable board event for downstream challenger and moderator work.

## Triggering Conditions
- Need to turn a contradiction or uncertainty into an explicit review ticket.
- Need a board-visible object for alternative hypothesis or falsification work.
- Need a durable challenger queue item rather than an ad hoc note.

## Read/Write Contract
- Reads the shared deliberation plane first and exports `run_dir/board/investigation_board.json` for compatibility.
- Bootstraps existing board JSON into the deliberation plane when needed.
- Appends one challenge ticket and one board event on the deliberation plane.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `title`
- `challenge_statement`
- Optional:
  - `board_path`
  - `target_claim_id`
  - `target_hypothesis_id`
  - `priority`
  - `owner_role`
  - `linked_artifact_ref`

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

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/open_challenge_ticket.py`
