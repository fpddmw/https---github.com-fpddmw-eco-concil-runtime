---
name: eco-propose-next-actions
description: Propose a ranked next-action queue from the current board summary, board brief, and evidence coverage so the council can move from organized board state into explicit investigation work.
---

# Eco Propose Next Actions

## Core Goal
- Convert the current round's board state into a compact next-action queue.
- Rank actions by board urgency, contradiction pressure, and coverage readiness.
- Emit a durable investigation artifact that downstream probe and readiness skills can consume.

## Triggering Conditions
- Board state has reached summary / brief level and needs explicit follow-up actions.
- Need a deterministic queue before opening falsification probes.
- Need an intermediate artifact between board organization and readiness / promotion.

## Read/Write Contract
- Syncs the round into the run-local deliberation plane and prefers that state for action ranking.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default as a compatible advisory fallback.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Reads evidence coverage from the run-local analysis plane first.
- Falls back to `run_dir/analytics/evidence_coverage_<round_id>.json` when the synced result set is unavailable.
- Writes `run_dir/investigation/next_actions_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_summary_path`
  - `board_brief_path`
  - `coverage_path`
  - `output_path`
  - `max_actions`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `deliberation_sync`
- `analysis_sync`
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_propose_next_actions.py`
