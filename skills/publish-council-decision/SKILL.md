---
name: publish-council-decision
description: Publish a canonical council decision from the current decision draft while enforcing report prerequisites and overwrite guards.
---

# Publish Council Decision

## Core Goal
- Promote the current council decision draft into a canonical decision artifact.
- Enforce overwrite protection and, when appropriate, require canonical expert reports.
- Keep publish semantics outside the runtime kernel.
- Require explicit operator approval for governed publish execution; this skill does not advance investigation state.

## Triggering Conditions
- A council decision draft already exists.
- Need a canonical decision artifact with publish guards.
- Need final decision semantics to stay skill-first rather than move into runtime.

## Read/Write Contract
- Reads `run_dir/reporting/council_decision_draft_<round_id>.json` by default.
- Reads `run_dir/reporting/expert_report_social_investigator_<round_id>.json` by default when decision publication_readiness is `ready`.
- Reads `run_dir/reporting/expert_report_environmental_investigator_<round_id>.json` by default when decision publication_readiness is `ready`.
- Writes `run_dir/reporting/council_decision_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `draft_path`
  - `social_investigator_report_path`
  - `environmental_investigator_report_path`
  - `output_path`
  - `allow_overwrite`
  - `skip_report_check`

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
- The emitted canonical decision preserves normalized reporting-chain trace metadata in `board_state_source`, `coverage_source`, `reporting_handoff_source`, `report_basis_source`, `decision_source`, `social_investigator_report_source`, `environmental_investigator_report_source`, `db_path`, and `observed_inputs`.
- The canonical decision preserves `decision_packet`, memo sections, report refs, evidence refs, and decision trace ids for final report assembly.

## Agent Reasoning Guide
- Treat publish as promotion of an existing decision draft into a canonical
  decision artifact. It does not reopen investigation, add findings, or bypass
  report prerequisites.
- Preserve overwrite guards, operator approval, evidence refs, decision trace
  ids, and any continue/withhold posture from the draft.
- If reports are required but missing, keep the prerequisite failure visible
  rather than weakening the publication boundary.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/publish_council_decision.py`
