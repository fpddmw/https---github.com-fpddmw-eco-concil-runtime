---
name: eco-plan-round-orchestration
description: Build one runtime orchestration plan from the current board, D1/D2 artifacts, and readiness posture before the phase-2 controller executes.
---

# Eco Plan Round Orchestration

## Core Goal
- Materialize one explicit orchestration plan artifact before controller execution.
- Turn board state, D1 actions, and readiness posture into a stable skill queue.
- Keep planning semantics in a skill instead of hardcoding all phase-2 flow in runtime.
- In OpenClaw agent mode, downgrade this plan to an advisory planner rather than the only controller authority.

## Triggering Conditions
- A round is ready for phase-2 controller execution.
- Need an auditable plan object instead of an implicit fixed controller queue.
- Need to decide whether probe-opening should remain in the queue for the current round.

## Read/Write Contract
- Reads `run_dir/board/investigation_board.json` by default.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default when present.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Reads `run_dir/investigation/next_actions_<round_id>.json` by default when present.
- Reads `run_dir/investigation/falsification_probes_<round_id>.json` by default when present.
- Reads `run_dir/reporting/round_readiness_<round_id>.json` by default when present.
- Writes `run_dir/runtime/orchestration_plan_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_path`
  - `board_summary_path`
  - `board_brief_path`
  - `next_actions_path`
  - `probes_path`
  - `readiness_path`
  - `output_path`
  - `planner_mode`

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
- `scripts/eco_plan_round_orchestration.py`