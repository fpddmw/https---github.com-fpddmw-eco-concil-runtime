---
name: eco-post-board-note
description: Append a compact investigation note to the local board artifact, preserve linked evidence refs, and emit an auditable board event for downstream moderator and challenger work.
---

# Eco Post Board Note

## Core Goal
- Append one compact note to the current round's board state.
- Preserve linked evidence refs and related ids.
- Emit an auditable board event for downstream review.

## Triggering Conditions
- Need to capture an analysis note or working conclusion on the board.
- Need to anchor compact evidence refs to a board-visible note.
- Need to initialize board activity for a run or round.

## Read/Write Contract
- Reads and writes `run_dir/board/investigation_board.json` by default.
- Appends one note and one board event.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `author_role`
- `note_text`
- Optional:
  - `board_path`
  - `category`
  - `tag`
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
- `scripts/eco_post_board_note.py`