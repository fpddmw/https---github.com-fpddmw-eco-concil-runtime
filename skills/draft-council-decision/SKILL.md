---
name: draft-council-decision
description: Draft a decision memo from reporting packets so the moderator can finalize the round or explicitly continue investigation.
---

# Draft Council Decision

## Core Goal
- Turn the reporting handoff's `decision_packet` into one auditable decision memo draft.
- Make the decision posture explicit: finalize the round or require another round.
- Preserve DB evidence basis, uncertainty, residual disputes, reporting blockers, and packet lineage.

## Triggering Conditions
- A compact reporting handoff already exists for the round.
- Need a downstream decision object instead of re-deriving final posture from multiple upstream objects.
- Need a draft that can later be consumed by report drafting, publish, and final publication workflows.

## Read/Write Contract
- Reads canonical reporting handoff rows by default; JSON artifacts are rebuildable exports.
- Reads frozen report-basis state only through DB-first wrappers when packet fields are missing.
- Writes `run_dir/reporting/council_decision_draft_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `reporting_handoff_path`
  - `promotion_path`
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
- The emitted artifact also carries normalized cross-plane trace metadata in `board_state_source`, `coverage_source`, `reporting_handoff_source`, `promotion_source`, `db_path`, and `observed_inputs`, preserving upstream trace fields from the reporting chain.
- The emitted decision draft includes `decision_packet` and `memo_sections` for decision-maker report assembly.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/draft_council_decision.py`
