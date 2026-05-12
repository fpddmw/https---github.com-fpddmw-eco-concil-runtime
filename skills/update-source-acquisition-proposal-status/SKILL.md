---
name: update-source-acquisition-proposal-status
description: Update the lifecycle status of an existing source-acquisition proposal after execution, approval, withdrawal, or rejection while preserving agent-authored evidence and provenance without source ranking or evidence weighting.
---

# Update Source Acquisition Proposal Status

## Core Goal
- Update one existing `source-acquisition-proposal` deliberation object.
- Record lifecycle status, updater role, rationale, evidence refs, lineage, and provenance.
- Keep the proposal descriptive: execution status does not select a source for the council or force evidence acceptance.

## Boundaries
- Do not include source rank, source weight, score, priority, recommended conclusion, or evidence sufficiency fields.
- Do not infer whether the fetched evidence should be adopted.
- Do not mutate fetch artifacts or normalized signals; this skill only updates the council proposal envelope.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `object_id`
- `status`
- `actor_role`

Supported source-acquisition statuses are:
- `proposed`
- `approved-for-execution`
- `fetched`
- `normalized`
- `receipt-only`
- `failed`
- `blocked`
- `executed` for legacy records that do not distinguish fetch vs normalization
- `withdrawn`
- `rejected`

## Useful Optional Input
- `status_rationale`
- `evidence_ref`
- `lineage_id`
- `provenance_json`

## Output Contract
- Rewrites the canonical `source-acquisition-proposal` row with the updated status.
- Writes one runtime-local status update artifact.
- Returns `canonical_ids`, `artifact_refs`, and query handoff commands.

## Agent Reasoning Guide
- This skill updates lifecycle state on a proposal envelope. It does not judge
  the substantive value of the source or normalize raw evidence.
- `failed`, `blocked`, or `receipt-only` is not proof that the source has no
  relevant data. It usually means the query, permission, fetch, or
  normalization path needs inspection.
- Use status updates with lineage refs so later agents can decide whether to
  retry, revise parameters, switch skills, or cite normalized signals.

## Scripts
- `scripts/update_source_acquisition_proposal_status.py`
