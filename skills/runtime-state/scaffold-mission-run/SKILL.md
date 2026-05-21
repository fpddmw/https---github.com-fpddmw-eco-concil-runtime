---
name: scaffold-mission-run
description: Scaffold one mission-driven run by materializing mission.json, first-round task inputs, and seeded board hypotheses before prepare-round executes.
---

# Eco Scaffold Mission Run

## Core Goal
- Read one mission input envelope.
- Materialize the active `mission.json` for the run.
- Seed first-round task inputs for prepare-round.
- Seed initial board hypotheses so downstream board and readiness skills do not start from an empty round.
- If the mission lacks a complete window or region, keep the run in scoping mode instead of forcing a verification scope.

## Mission Semantics
- A mission is the user-facing request envelope that starts a council run.
- It is not the moderator's investigation plan, not an evidence bundle, not a report basis, and not a factual attribution.
- Required fields are `schema_version`, `run_id`, `topic`, and `objective`.
- `request_text` should preserve the user's natural-language request when available; `objective` may mirror it for legacy compatibility.
- Optional seed fields such as `window`, `region`, `artifact_imports`, `source_requests`, `hypotheses`, or `source_governance` are starting context only. They must not be treated as agent instructions, evidence acceptance, or a hard council agenda.
- If the user prompt is open-ended, keep the mission minimal and let moderator/agents submit `investigation-plan`, `investigation-scope`, `round-brief`, and `evidence-request` objects after ingress.

## Triggering Conditions
- A new run needs to be created from a user mission input.
- The workflow should start from mission input instead of direct test seeding.
- The next step will be `prepare-round`.

## Read/Write Contract
- Reads `<mission_path>`.
- Writes `run_dir/mission.json`.
- Writes `run_dir/investigation/round_tasks_<round_id>.json`.
- Writes `run_dir/board/investigation_board.json`.
- Writes `run_dir/runtime/mission_scaffold_<round_id>.json`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `mission_path`
- Optional:
  - `hypothesis_confidence`
  - `orchestration_mode`

## Scoping Boundary
- `window` and `region` are optional at ingress.
- Missing `window.start_utc`, `window.end_utc`, `region.label`, or `region.geometry` sets `mission_scope_status.scoping_required=true`.
- Scoping mode does not auto-select intent sources; moderator and agents should submit investigation plan, scope, round brief, or evidence-request objects.
- Existing `artifact_imports` and `source_requests` may still seed tasks when explicitly provided.

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- Treat the mission as the user's request envelope. It is not a moderator plan,
  source selection, evidence bundle, or factual conclusion.
- Seeded hypotheses, source requests, artifact imports, window, and region are
  starting context only. Agents may revise, narrow, reject, or extend them with
  explicit rationale.
- Missing scope means the run needs scoping work; it does not mean the mission
  is invalid or that no evidence exists.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/scaffold_mission_run.py`
