---
name: plan-round-orchestration
description: Optional moderator advisory skill for materializing an auditable orchestration plan from DB-backed council state. It requires operator-approved skill approval and is not part of the default controller path.
---

# Eco Plan Round Orchestration

## Core Goal
- Materialize an explicit advisory plan only when a moderator requests optional planning help.
- Read DB-backed council state and compatible exports without turning them into a runtime-owned default queue.
- Record fallback planning basis as audit material, including which state gaps or caveats led to each advisory item.
- Keep this skill outside the default controller path; controller execution remains transition-request driven.
- Require `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` before execution.

## Triggering Conditions
- A moderator explicitly asks for advisory planning after reviewing DB-native findings, evidence bundles, proposals, or readiness opinions.
- The operator approves this optional-analysis run for the current round and requested actor role.
- Need a reviewable planning suggestion, not a committed phase transition or controller plan.
- Need an explicit `phase_decision_basis` that labels fallback assumptions and missing-input caveats.

## Read/Write Contract
- Reads `run_dir/board/investigation_board.json` by default.
- Syncs the round into the run-local deliberation plane and prefers that state for planning.
- Reads board, next-action, probe, and readiness exports only as derived advisory context.
- Writes `run_dir/runtime/orchestration_plan_<round_id>.json` as an advisory export, not canonical phase ownership.

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

## Agent Reasoning Guide
- Treat output as optional moderator advisory material. It is not the controller's
  default plan, a source queue, a phase transition, or report-basis authority.
- Missing-input caveats and fallback assumptions are part of the result. Do not
  convert a sparse plan into `no-actionable-path`, readiness, or phase movement.
- Moderator and runtime-operator actions remain explicit: transition requests,
  approvals, source proposals, findings, challenges, and readiness opinions must
  carry any plan item before downstream use.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/plan_round_orchestration.py`
