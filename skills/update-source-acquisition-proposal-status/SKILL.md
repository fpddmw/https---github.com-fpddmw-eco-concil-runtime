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

## Useful Optional Input
- `status_rationale`
- `evidence_ref`
- `lineage_id`
- `provenance_json`

## Output Contract
- Rewrites the canonical `source-acquisition-proposal` row with the updated status.
- Writes one runtime-local status update artifact.
- Returns `canonical_ids`, `artifact_refs`, and query handoff commands.

## Scripts
- `scripts/update_source_acquisition_proposal_status.py`
