---
name: eco-open-falsification-probe
description: Open compact falsification probes from the next-action queue so contradiction-heavy or low-confidence board targets become explicit probe objects.
---

# Eco Open Falsification Probe

## Core Goal
- Convert probe-worthy next actions into explicit falsification probes.
- Preserve the target ids, linked evidence refs, and requested follow-up skills.
- Emit a durable investigation artifact for challenger and moderator workflows.

## Triggering Conditions
- A ranked action queue contains contradiction-heavy or low-confidence targets.
- Need explicit probe objects instead of leaving falsification work implicit in notes or challenges.
- Need a bridge artifact between D1 action planning and D2 readiness gating.

## Read/Write Contract
- Reads `run_dir/investigation/next_actions_<round_id>.json` by default.
- If the next-actions artifact is absent, rebuilds probe candidates from the run-local deliberation plane plus analysis-plane-backed coverage context.
- Writes `run_dir/investigation/falsification_probes_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `next_actions_path`
  - `board_summary_path`
  - `board_brief_path`
  - `coverage_path`
  - `output_path`
  - `action_id`
  - `max_probes`
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
- The emitted artifact also carries normalized D1 trace metadata in `action_source`, `board_state_source`, `coverage_source`, `db_path`, and `observed_inputs`, including explicit `next_actions_artifact_present` handling on both artifact and fallback paths.

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_open_falsification_probe.py`
