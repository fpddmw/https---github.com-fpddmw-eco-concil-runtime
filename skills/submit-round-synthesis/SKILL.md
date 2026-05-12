---
name: submit-round-synthesis
description: Submit a thin moderator-authored round synthesis that records stage conclusions, open gaps, and candidate continuation refs without ranking work or deciding evidence acceptance.
---

# Submit Round Synthesis

## Core Goal
- Record one `round-synthesis` object for a council round.
- Capture the moderator's stage conclusion, known facts, unresolved refs, evidence gaps, and candidate continuation refs.
- Make the synthesis queryable as coordination context before a closeout or continuation round.

## Boundaries
- This is a synthesis note, not a scheduler, source selector, evidence weighting tool, or report-basis freeze.
- Do not include priority, rank, weight, score, or source ordering fields.
- Candidate continuation refs remain optional handoff context; agents and the moderator decide how to use them.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role` defaults to `moderator`

## Useful Optional Input
- `synthesis_text`
- `stage_conclusion`
- `known_fact`
- `covered_object_ref`
- `resolved_object_ref`
- `unresolved_object_ref`
- `evidence_gap_ref`
- `next_round_candidate_ref`
- `open_question`
- `limitation`
- `target_kind`
- `target_id`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `round-synthesis` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Agent Reasoning Guide
- A round synthesis is moderator coordination about what is known, unresolved,
  and available for continuation. It is not source ranking or evidence
  acceptance.
- Known facts should point to cited refs. Unresolved refs and evidence gaps
  should remain visible so continuation can act on them.
- Candidate continuation refs are live routes, not a fixed agenda. If useful
  routes remain, the moderator should decide whether to open a continuation
  round instead of silently closing.

## Scripts
- `scripts/submit_round_synthesis.py`
