---
name: submit-investigation-scope
description: Submit a thin investigation-scope coordination object into the deliberation DB so agents can propose, revise, activate, retire, or reject scope boundaries without runtime scoring or hard mission compilation.
---

# Submit Investigation Scope

## Core Goal
- Record a candidate or active `investigation-scope` as a queryable council object.
- Preserve scope text, spatial/temporal/object/metric/comparison fields, rationale, provenance, and evidence refs when supplied.
- Keep scope as revisable council material, not a hard runtime constraint.

## Boundaries
- Do not include score, weight, rank, priority, source ranking, or required coverage fields.
- Scope may be incomplete or unknown; agents can refine it in later objects.
- Runtime stores and queries the object but does not decide whether the scope is substantively correct.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role`

## Useful Optional Input
- `scope_kind`
- `scope_text`
- `spatial_scope`
- `temporal_scope`
- `object_scope`
- `metric_scope`
- `comparison_frame`
- `target_kind`
- `target_id`
- `evidence_ref`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `investigation-scope` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Agent Reasoning Guide
- Scope records what the council currently thinks should be bounded. It is
  revisable and must not be treated as the whole truth of the mission.
- Unknown or partial spatial, temporal, object, metric, or comparison scope is
  a reason to continue scoping, not a reason to stop investigation.
- Later findings, evidence requests, or synthesis objects should cite scope
  refs when they rely on them.

## Scripts
- `scripts/submit_investigation_scope.py`
