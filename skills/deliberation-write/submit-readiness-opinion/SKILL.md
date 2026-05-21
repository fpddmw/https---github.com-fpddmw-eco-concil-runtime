---
name: submit-readiness-opinion
description: Submit one structured readiness opinion directly into the deliberation DB, preserve basis object ids, evidence refs, provenance, and explicit readiness posture, and emit a queryable opinion artifact for downstream report-basis review.
---

# Eco Submit Readiness Opinion

## Core Goal
- Submit one structured readiness judgement as a canonical `readiness-opinion` object.
- Preserve basis object ids, evidence refs, lineage, provenance, and explicit readiness posture.
- Make the opinion immediately queryable from the deliberation DB for readiness aggregation and report-basis review.

## Triggering Conditions
- Need to record that a round is ready, blocked, or still needs more data.
- Need a council-visible readiness object instead of relying on fallback readiness summaries alone.
- Need to preserve which basis objects support the readiness posture.

## Read/Write Contract
- Reads only its direct inputs.
- Appends one canonical `readiness-opinion` row to the shared deliberation plane.
- Writes one runtime-local readiness submission artifact for auditability.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `agent_role`
- `readiness_status`
- `rationale`
- Optional:
  - `opinion_id`
  - `decision_source`
  - `opinion_status`
  - `sufficient_for_report_basis`
  - `confidence`
  - `basis_object_id`
  - `evidence_ref`
  - `lineage_id`
  - `provenance_json`
  - `extra_json`

## Output Contract
- `status`
- `summary`
  - Includes `opinion_id`
  - Includes `db_path`
  - Includes `output_path`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- A readiness opinion is one role's stated posture about whether a claim,
  round, or report basis can move forward. It does not move the council phase
  by itself.
- `blocked` or `needs-more-data` is a gap declaration, not proof that evidence
  does not exist. It should name the missing route or unresolved refs whenever
  possible.
- If the blocker is that the current source surface cannot answer the evidence
  need, submit `evidence-route-assessment` as the route object and cite it from
  the readiness opinion rather than hiding route mismatch inside readiness text.
- `ready` should cite basis objects and limits. It should not hide unresolved
  challenges or unnormalized fetch receipts.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/submit_readiness_opinion.py`
