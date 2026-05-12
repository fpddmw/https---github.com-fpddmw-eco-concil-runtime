---
name: submit-agent-position
description: Submit a thin agent-position coordination object into the deliberation DB so an agent can state a provisional position, limitation, or need-more-evidence posture using natural-language rationale and evidence refs without support scores or weights.
---

# Submit Agent Position

## Core Goal
- Record one `agent-position` for a target object.
- Preserve claim summary, limitations, open challenge refs, rationale, provenance, and evidence refs.
- Let the authoring agent own evidence combination and caveats in explicit text.

## Boundaries
- Do not include support level, score, rank, confidence, weight, priority, or automatic evidence quality fields.
- A position may be proposed, withheld, or marked needs-more-evidence.
- Synthesis/reporting must consume this object explicitly; the skill does not decide adoption.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role`
- `target_kind`
- `target_id`

## Useful Optional Input
- `claim_summary`
- `limitation`
- `open_challenge_ref`
- `evidence_ref`
- `status`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `agent-position` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Agent Reasoning Guide
- Use this skill to make an agent's provisional stance visible to the council.
  It is not a finding, report basis, or automatic adoption of the cited
  evidence.
- A `needs-more-evidence` or withheld position is a limitation statement, not
  proof that evidence does not exist.
- State what the agent believes, what remains unresolved, and which refs should
  be inspected next; downstream synthesis must cite this object explicitly
  before it affects conclusions.

## Scripts
- `scripts/submit_agent_position.py`
