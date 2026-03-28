---
name: eco-read-board-delta
description: Read compact round-scoped investigation board deltas, return event slices plus active hypotheses and challenge tickets, and provide a cursor for continued multi-agent work.
---

# Eco Read Board Delta

## Core Goal
- Read the current round's board events and working state.
- Return a compact delta slice plus active hypotheses and challenge tickets.
- Provide an event cursor for continued multi-agent work.

## Triggering Conditions
- Need current board activity without loading the entire board into context.
- Need active hypotheses or challenge tickets for the next agent move.
- Need a cursor-based delta view for moderator or specialist loops.

## Read/Write Contract
- Read only.
- Reads `run_dir/board/investigation_board.json` by default.
- Does not mutate board state.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_path`
  - `after_event_id`
  - `event_limit`
  - `include_closed`

## Output Contract
- `status`
- `summary`
- `result_count`
- `results`
- `artifact_refs`
- `warnings`
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-skill-phase-plan.md`

## Scripts
- `scripts/eco_read_board_delta.py`