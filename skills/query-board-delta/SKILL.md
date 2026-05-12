---
name: query-board-delta
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
- Reads the run-local deliberation plane first.
- Uses `run_dir/board/investigation_board.json` as a bootstrap import when it exists.
- Falls back to DB-only reads when the board JSON export is temporarily absent.
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
- `round_state`
- `deliberation_sync`
- `board_handoff`

## Agent Reasoning Guide
- Treat this as a compact read surface over visible board state. It is not a
  canonical finding, readiness decision, source selector, or phase gate.
- Empty or short deltas can mean the board has not been initialized, the cursor
  is too recent, the round filter is wrong, or DB-only recovery is active. It
  does not mean the investigation has no evidence or no live issues.
- Use write skills such as `submit-agent-position`, `open-challenge-ticket`, or
  `submit-round-synthesis` when an agent needs to turn board context into an
  auditable council judgement.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_board_delta.py`
