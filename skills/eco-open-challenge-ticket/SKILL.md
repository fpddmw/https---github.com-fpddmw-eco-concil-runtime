---
name: eco-open-challenge-ticket
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
- Reads and writes `run_dir/board/investigation_board.json` by default.
- Appends one challenge ticket and one board event.

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
- `scripts/eco_open_challenge_ticket.py`