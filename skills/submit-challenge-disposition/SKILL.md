---
name: submit-challenge-disposition
description: Submit a thin challenge-disposition coordination object into the deliberation DB so a moderator or challenger can record how an explicit challenge/review constraint should affect readiness and report-basis use without judging the underlying evidence truth.
---

# Submit Challenge Disposition

## Core Goal
- Record one `challenge-disposition` for a target challenge or challenger review comment.
- Preserve disposition status, disposition text, target ref, response refs, rationale, provenance, and evidence refs.
- Keep the disposition as an explicit council record that downstream gates can inspect by status and object references.

## Boundaries
- Do not include support level, score, rank, confidence, weight, priority, or automatic evidence quality fields.
- This skill does not decide whether a challenge is true, false, strong, weak, or persuasive.
- Runtime may use the disposition only as an explicit governance status envelope, not as evidence weighting.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role`
- `target_kind`
- `target_id`
- `disposition_status`

## Useful Optional Input
- `disposition_text`
- `decided_by_role`
- `response_to_id`
- `source_review_comment_id`
- `challenge_id`
- `evidence_ref`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `challenge-disposition` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Agent Reasoning Guide
- Use this skill to record how the council treats a challenge procedurally. It
  does not prove the challenged claim true or false by itself.
- `upheld`, `resolved`, `unresolved`, or `rejected` are governance states. They
  constrain later readiness/report-basis use only when downstream objects cite
  the disposition and its evidence refs.
- If a challenge remains unresolved, carry it into synthesis or follow-up work
  rather than treating it as absence of data.

## Scripts
- `scripts/submit_challenge_disposition.py`
