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
- Reads the shared deliberation plane first and exports `run_dir/board/investigation_board.json` for compatibility.
- Bootstraps existing board JSON into the deliberation plane when needed.
- Upserts one hypothesis card and appends one board event on the deliberation plane.

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
- `scripts/eco_update_hypothesis_status.py`
