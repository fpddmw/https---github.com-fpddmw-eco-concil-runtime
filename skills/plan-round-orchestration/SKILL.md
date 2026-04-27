---
name: plan-round-orchestration
description: Optional moderator advisory skill for materializing an auditable orchestration plan from DB-backed council state. It requires operator-approved skill approval and is not part of the default controller path.
---

# Eco Plan Round Orchestration

## Core Goal
- Materialize an explicit advisory plan only when a moderator requests optional planning help.
- Read DB-backed council state and compatible exports without turning them into a runtime-owned default queue.
- Record any heuristic planning basis as audit material, including why a probe or readiness suggestion is proposed.
- Keep this skill outside the default controller path; controller execution remains transition-request driven.
- Require `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` before execution.

## Triggering Conditions
- A moderator explicitly asks for advisory planning after reviewing DB-native findings, evidence bundles, proposals, or readiness opinions.
- The operator approves this optional-analysis run for the current round and requested actor role.
- Need a reviewable planning suggestion, not a committed phase transition or controller plan.
- Need an explicit `phase_decision_basis` that labels heuristic assumptions and compatibility fallbacks.

## Read/Write Contract
- Reads `run_dir/board/investigation_board.json` by default.
- Syncs the round into the run-local deliberation plane and prefers that state for planning.
- Reads compatible board, next-action, probe, and readiness exports only as derived advisory context.
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

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/plan_round_orchestration.py`
