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
- Writes `run_dir/investigation/falsification_probes_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `next_actions_path`
  - `output_path`
  - `action_id`
  - `max_probes`

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
- `scripts/eco_open_falsification_probe.py`