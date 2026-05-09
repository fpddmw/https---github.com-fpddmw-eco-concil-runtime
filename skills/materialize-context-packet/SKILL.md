---
name: materialize-context-packet
description: Materialize a refs-only context-packet coordination object for an agent turn, containing target refs, evidence refs, delta refs, excluded refs, and provenance without raw records, source weighting, or salience ranking.
---

# Materialize Context Packet

## Core Goal
- Create one `context-packet` for the current round.
- Include compact object references, target refs, evidence refs, delta refs, excluded refs, source refs, and provenance.
- Give agents a small, auditable pointer surface without copying full raw records or full history.

## Boundaries
- Do not include raw record payloads or full source artifacts.
- Do not rank, score, weight, prioritize, or suppress contradictory objects by salience.
- Object order is deterministic query order only; agents decide what to read, combine, accept, or reject.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`

## Useful Optional Input
- `packet_profile`
- `target_ref`
- `include_object_ref`
- `excluded_object_ref`
- `source_ref`
- `max_objects_per_kind`
- `object_kind`
- `summary_text`

## Output Contract
- Appends one canonical `context-packet` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, `context_packet`, and DB query handoff.

## Scripts
- `scripts/materialize_context_packet.py`
