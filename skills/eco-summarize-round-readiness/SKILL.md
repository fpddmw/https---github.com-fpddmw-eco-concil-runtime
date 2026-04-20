---
name: eco-summarize-round-readiness
description: Summarize round-level readiness from deliberation-plane state, council readiness opinions, next actions, probes, and evidence coverage so the council can decide whether the round is blocked, needs more data, or is ready for promotion.
---

# Eco Summarize Round Readiness

## Core Goal
- Turn deliberation-plane state plus council readiness opinions into a compact round-readiness gate.
- Explain whether the round is blocked, needs more data, or is ready.
- Emit a durable reporting artifact for promotion and runtime gate logic.

## Triggering Conditions
- Need a round-level gate after board organization and D1 planning.
- Need to aggregate or override heuristic readiness with explicit readiness opinions already written by the council.
- Need to know whether open tasks or probes still block promotion.
- Need a compact readiness artifact before freezing evidence basis.

## Read/Write Contract
- Syncs the round into the run-local deliberation plane and prefers that state for readiness evaluation.
- Reads canonical `readiness-opinion` objects from the shared deliberation plane when present.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default as a compatible advisory fallback.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present as a compatible advisory fallback.
- Reads `run_dir/investigation/next_actions_<round_id>.json` by default when present.
- Reads `run_dir/investigation/falsification_probes_<round_id>.json` by default when present.
- Reads evidence coverage from the run-local analysis plane first.
- Falls back to `run_dir/analytics/evidence_coverage_<round_id>.json` when the synced result set is unavailable.
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
- `deliberation_sync`
- `analysis_sync`
- `board_handoff`
- The emitted artifact also carries normalized D1/D2 trace metadata in `board_state_source`, `coverage_source`, `db_path`, and `observed_inputs`, including explicit `*_artifact_present` and `*_present` flags for board, action, probe, and coverage inputs.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/eco_summarize_round_readiness.py`
