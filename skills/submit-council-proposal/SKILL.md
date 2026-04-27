---
name: submit-council-proposal
description: Submit one structured council proposal directly into the deliberation DB, preserve explicit judgement fields, evidence refs, provenance, and target anchors, and emit a queryable proposal artifact for downstream council execution.
---

# Eco Submit Council Proposal

## Core Goal
- Submit one structured council proposal as a canonical `proposal` object.
- Preserve explicit judgement metadata, target anchors, evidence refs, response links, and provenance.
- Make the proposal immediately queryable from the deliberation DB for next-action, probe, readiness, and promotion workflows.

## Triggering Conditions
- Need to express a council judgement without hiding it inside board notes or implicit artifact wrappers.
- Need a proposal that downstream skills can consume directly from the DB.
- Need to attach explicit promotion, publication, or handoff posture to one proposal.

## Read/Write Contract
- Reads only its direct inputs.
- Appends one canonical `proposal` row to the shared deliberation plane.
- Writes one runtime-local proposal submission artifact for auditability.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `proposal_kind`
- `agent_role`
- `rationale`
- `confidence`
- `target_kind`
- `target_id`
- At least one `evidence_ref`
- `provenance_json`
- Optional:
  - `proposal_id`
  - `decision_source`
  - `status`
  - `target_json`
  - `action_kind`
  - `assigned_role`
  - `objective`
  - `summary`
  - `response_to_id`
  - `lineage_id`
  - `extra_json`
  - `promotion_disposition`
  - `promote_allowed`
  - `publication_readiness`
  - `handoff_status`
  - `moderator_status`

## Output Contract
- `status`
- `summary`
  - Includes `proposal_id`
  - Includes `db_path`
  - Includes `output_path`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-next-phase-development-plan.md`

## Scripts
- `scripts/submit_council_proposal.py`
