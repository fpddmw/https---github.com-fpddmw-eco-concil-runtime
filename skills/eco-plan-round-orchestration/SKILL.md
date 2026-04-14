---
name: eco-plan-round-orchestration
description: Build one runtime orchestration plan from shared board state and controversy-agenda artifacts before the phase-2 controller executes, while treating board summary and brief as derived exports rather than hard controller prerequisites.
---

# Eco Plan Round Orchestration

## Core Goal
- Materialize one explicit orchestration plan artifact before controller execution.
- Turn board state, D1 actions, and readiness posture into a stable skill queue.
- Decide whether falsification probes stay in the queue from agenda artifacts first, with board heuristics only kept as a compatibility fallback.
- Keep planning semantics in a skill instead of hardcoding all phase-2 flow in runtime.
- In OpenClaw agent mode, downgrade this plan to an advisory planner rather than the only controller authority.

## Triggering Conditions
- A round is ready for phase-2 controller execution.
- Need an auditable plan object instead of an implicit fixed controller queue.
- Need to decide whether probe-opening should remain in the queue for the current round.
- Need an explicit `phase_decision_basis` that explains why the plan is holding or skipping probe work.

## Read/Write Contract
- Reads `run_dir/board/investigation_board.json` by default.
- Syncs the round into the run-local deliberation plane and prefers that state for planning.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default when present as a compatible advisory artifact.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present as a compatible advisory artifact.
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
- `deliberation_sync`
- `board_handoff`
- The emitted plan artifact also records `phase_decision_basis`, including agenda counts, controversy-gap counts, and probe-stage / posture reason codes.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/eco_plan_round_orchestration.py`
