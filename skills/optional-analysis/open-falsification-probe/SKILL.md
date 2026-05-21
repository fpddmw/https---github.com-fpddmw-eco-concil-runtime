---
name: open-falsification-probe
description: Open compact falsification probes from next-action candidates so contradiction-heavy or low-confidence board targets become explicit probe objects.
---

# Eco Open Falsification Probe

## Core Goal
- Convert probe-worthy next actions into explicit falsification probes.
- Preserve the target ids, linked evidence refs, and governance follow-up actions.
- Do not select professional analysis, query, transport, representation, or diffusion tools for the council.
- Emit a durable investigation artifact for challenger and moderator workflows.

## Triggering Conditions
- Next-action candidates contain contradiction-heavy or low-confidence targets.
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

## Agent Reasoning Guide
- Treat a falsification probe as an explicit test target, not as proof that a
  claim is false, weak, or ready to close.
- The skill does not select professional tools or run the investigation. It
  preserves target ids, evidence refs, and governance follow-up so challengers
  and moderators can act.
- Empty or sparse probes can reflect missing next-action inputs, filters, DB
  sync gaps, or lack of contradiction-heavy candidates. They do not close live
  challenges or unresolved hypotheses.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/open_falsification_probe.py`
