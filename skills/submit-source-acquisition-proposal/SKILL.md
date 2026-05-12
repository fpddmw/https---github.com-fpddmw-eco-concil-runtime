---
name: submit-source-acquisition-proposal
description: Submit a thin source-acquisition proposal so an investigator or challenger can record a planned fetch source, query parameters, side-effect declarations, and rationale without runtime source ranking or evidence weighting.
---

# Submit Source Acquisition Proposal

## Core Goal
- Record one `source-acquisition-proposal` as a deliberation object.
- Let the author declare the fetch skill, query parameters, target request or challenge, side effects, rationale, provenance, and status.
- Keep the proposal optional: legal fetch skills may still be run directly through role permission and runtime admission.

## Boundaries
- Do not include source rank, source weight, score, priority, recommended conclusion, or evidence sufficiency fields.
- The proposal does not select a source for the council and does not force evidence acceptance.
- Runtime validates source skill existence, author role permission, and declared approval shape only.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `author_role`
- `source_skill`
- `rationale`

## Useful Optional Input
- `query_parameters_json`
- `target_kind`
- `target_id`
- `target_evidence_request_id`
- `declared_side_effect`
- `requested_side_effect_approval`
- `evidence_ref`
- `provenance_json`

## Output Contract
- Appends one canonical `source-acquisition-proposal` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and query handoff commands.

## Agent Reasoning Guide
- A source-acquisition proposal records an investigator's intended fetch route.
  It is not source selection by the runtime and not evidence acceptance.
- Query parameters should be concrete enough for later execution and lineage
  linking. If a first query fails, revise parameters or use a complementary
  skill before treating the route as exhausted.
- Fetch and normalization results must later be linked by
  `link-source-acquisition-execution` or another explicit lineage object before
  downstream agents rely on them.

## Scripts
- `scripts/submit_source_acquisition_proposal.py`
