---
name: materialize-board-brief
description: Materialize the current round's investigation board into a compact human-readable markdown brief that is ready for moderator review or Phase D handoff.
---

# Eco Materialize Board Brief

## Core Goal
- Materialize one round's board state into a concise markdown brief.
- Surface active hypotheses, open challenges, active tasks, and immediate next moves.
- Provide a human-readable handoff artifact for moderator review and Phase D planning.

## Triggering Conditions
- Need a concise board brief after the board has been organized.
- Need a durable summary to hand off into action planning or readiness review.
- Need to inspect open tasks and remaining risks without re-reading raw board JSON.

## Read/Write Contract
- Reads `run_dir/board/investigation_board.json` by default.
- Syncs the round into the run-local deliberation plane and prefers that state for the brief.
- Optionally reads `run_dir/board/board_state_summary_<round_id>.json` as a compatible advisory fallback.
- Writes `run_dir/board/board_brief_<round_id>.md` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_path`
  - `summary_path`
  - `brief_path`
  - `max_items`

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
- `scripts/materialize_board_brief.py`
