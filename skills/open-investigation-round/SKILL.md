---
name: open-investigation-round
description: Moderator-controlled council-state skill that opens one follow-up investigation round on the shared board, preserves the source round as history, carries forward unresolved state, and materializes the next round task scaffold for governed or agent-led continuation.
---

# Eco Open Investigation Round

## Core Goal
- Act as one explicit council-state mutation under moderator control.
- Open one follow-up round without overwriting prior round state.
- Preserve the source round as historical context.
- Carry forward active hypotheses and unresolved follow-up work into the new round.
- Materialize `round_tasks_<round_id>.json` so the next round can immediately continue through `prepare-round` or agent-led investigation.

## Triggering Conditions
- The current round is evidence-insufficient and the moderator decides to continue in a new round.
- The board still has active hypotheses, open challenges, or unfinished tasks that should survive into a follow-up round.
- A run needs an explicit round transition object rather than continuing to mutate the prior round forever.

## Read/Write Contract
- Reads board state from the shared deliberation plane first and exports `run_dir/board/investigation_board.json` for compatibility.
- Reads `run_dir/investigation/round_tasks_<source_round_id>.json` when present.
- Reads `run_dir/investigation/next_actions_<source_round_id>.json` when present.
- Reads `run_dir/mission.json` when present.
- Writes `run_dir/investigation/round_tasks_<round_id>.json`.
- Writes `run_dir/runtime/round_transition_<round_id>.json`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `source_round_id`
- Optional:
  - `board_path`
  - `source_task_path`
  - `source_next_actions_path`
  - `output_path`
  - `author_role`
  - `transition_note`
  - `action_limit`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Scripts
- `scripts/open_investigation_round.py`
