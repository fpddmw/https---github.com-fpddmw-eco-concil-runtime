---
name: submit-investigation-plan
description: Submit a thin investigation-plan coordination object into the deliberation DB without creating agenda locks, evidence scores, source weights, or runtime-controlled investigation order.
---

# Submit Investigation Plan

## Core Goal
- Record a moderator-authored investigation organization plan as a queryable `investigation-plan`.
- Keep the plan as council coordination material, not a runtime agenda or evidence judgement.
- Preserve open questions, proposed subissue refs, scope hint refs, rationale, provenance, and evidence refs when supplied.

## Boundaries
- Do not include score, weight, rank, priority, minimum coverage, support level, or source ranking fields.
- Do not use this skill to decide which evidence agents must accept.
- Agents may submit follow-up `subissue`, `investigation-scope`, `evidence-request`, or `agent-position` objects that revise or reject the plan.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role` defaults to `moderator`

## Useful Optional Input
- `mission_ref`
- `planning_round_id`
- `proposed_subissue_ref`
- `scope_hint_ref`
- `open_question`
- `supersedes_plan_id`
- `target_kind`
- `target_id`
- `evidence_ref`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `investigation-plan` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Agent Reasoning Guide
- Treat the plan as coordination material, not a fixed agenda or runtime
  command sequence.
- Agents may refine the plan when retrieved evidence, failed queries, or new
  hypotheses expose a better route.
- Open questions are investigation routes, not conclusions. If routes remain
  live, carry them into round synthesis or a continuation round.

## Scripts
- `scripts/submit_investigation_plan.py`
