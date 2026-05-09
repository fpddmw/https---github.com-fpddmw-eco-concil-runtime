---
name: submit-evidence-request
description: Submit a thin evidence-request coordination object into the deliberation DB to express what evidence an agent wants to seek, without minimum coverage, quality scores, source weights, or ranked source recommendations.
---

# Submit Evidence Request

## Core Goal
- Record one `evidence-request` from a moderator, investigator, or challenger.
- Preserve question, desired evidence type, source hints, boundary notes, target ref, rationale, provenance, and evidence refs when supplied.
- Keep the object as a request for investigation, not an instruction that evidence must exist or be accepted.

## Boundaries
- Do not include `minimum_coverage`, `quality_score`, `blocking_if_missing`, source weight, rank, score, or recommended source rank.
- Source hints are hints only; agents may accept, reject, or revise them with rationale.
- Runtime does not judge whether the requested evidence is sufficient or decisive.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role`

## Useful Optional Input
- `question`
- `desired_evidence_type`
- `source_hint`
- `boundary_note`
- `target_kind`
- `target_id`
- `evidence_ref`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `evidence-request` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Scripts
- `scripts/submit_evidence_request.py`
