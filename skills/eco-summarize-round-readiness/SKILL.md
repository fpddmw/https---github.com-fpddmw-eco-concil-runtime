---
name: eco-summarize-round-readiness
description: Summarize round-level readiness from board, next-action, probe, and evidence-coverage artifacts so the council can decide whether the round is blocked, needs more data, or is ready for promotion.
---

# Eco Summarize Round Readiness

## Core Goal
- Turn D1 and board artifacts into a compact round-readiness gate.
- Explain whether the round is blocked, needs more data, or is ready.
- Emit a durable reporting artifact for promotion and runtime gate logic.

## Triggering Conditions
- Need a round-level gate after board organization and D1 planning.
- Need to know whether open tasks or probes still block promotion.
- Need a compact readiness artifact before freezing evidence basis.

## Read/Write Contract
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Reads `run_dir/investigation/next_actions_<round_id>.json` by default when present.
- Reads `run_dir/investigation/falsification_probes_<round_id>.json` by default when present.
- Reads `run_dir/analytics/evidence_coverage_<round_id>.json` by default when present.
- Writes `run_dir/reporting/round_readiness_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `board_summary_path`
  - `board_brief_path`
  - `next_actions_path`
  - `probes_path`
  - `coverage_path`
  - `output_path`

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
- `scripts/eco_summarize_round_readiness.py`