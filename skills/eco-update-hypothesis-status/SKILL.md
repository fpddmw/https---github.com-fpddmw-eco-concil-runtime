---
name: eco-update-hypothesis-status
description: Create or update a hypothesis card on the local investigation board, preserve linked claim ids and confidence, and emit an auditable board event for downstream multi-agent review.
---

# Eco Update Hypothesis Status

## Core Goal
- Create or update one hypothesis card on the current round's board state.
- Preserve linked claim ids, owner role, and confidence.
- Emit an auditable board event for downstream review.

## Triggering Conditions
- Need a durable hypothesis card rather than a freeform note.
- Need to update status as evidence strengthens or challenge work proceeds.
- Need a board-visible object for moderator and challenger workflows.

## Read/Write Contract
- Reads and writes `run_dir/board/investigation_board.json` by default.
- Upserts one hypothesis card and appends one board event.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `title`
- `status`
- Optional:
  - `board_path`
  - `hypothesis_id`
  - `statement`
  - `owner_role`
  - `linked_claim_id`
  - `confidence`

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
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_update_hypothesis_status.py`