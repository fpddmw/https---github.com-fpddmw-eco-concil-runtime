---
name: submit-round-brief
description: Submit a thin round-brief coordination object into the deliberation DB as a context hint for agents, without turning requested outputs or focus refs into runtime hard gates.
---

# Submit Round Brief

## Core Goal
- Record one `round-brief` for a council round.
- Preserve round mode, primary focus refs, context packet refs, open questions, source boundary notes, invited roles, requested outputs, rationale, provenance, and evidence refs when supplied.
- Make the brief queryable as coordination context.

## Boundaries
- `requested_outputs` are hints only and must not be used as runtime rejection criteria.
- `invited_roles` is not a permission table; role contracts and access policy still govern permissions.
- Do not include priority, rank, weight, score, or source ordering fields.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role` defaults to `moderator`

## Useful Optional Input
- `round_mode`
- `primary_focus_ref`
- `context_packet_id`
- `open_question`
- `boundary_note`
- `source_boundary_note`
- `invited_role`
- `requested_output`
- `target_kind`
- `target_id`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `round-brief` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Scripts
- `scripts/submit_round_brief.py`
