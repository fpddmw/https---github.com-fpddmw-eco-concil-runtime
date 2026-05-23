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

`requested_side_effect_approval` is a list of side-effect names that must be a
subset of `declared_side_effect`. For a non-executing proposal, omit
`requested_side_effect_approval`; do not use it as a boolean. Runtime treats
common false-like values such as `false`, `no`, `none`, and `0` as "no approval
requested" for compatibility with agent-generated command repairs.

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
- Choose the source surface before writing the command:
  - Federal Register is appropriate for federal rulemaking, notices, and
    presidential/executive-document metadata. It is usually a poor first route
    for local emergency advisories, school closures, transit actions, or city
    health notices unless the claim is explicitly about federal publication.
  - GDELT DOC Search is indexed-web reconnaissance. It can find official-domain
    or media documents, but exact-domain zero rows are a query/indexing result,
    not proof that official records or public discussion are absent.
  - AirNow station observations are environmental receptor-side measurements.
    They do not belong in the formal/official-action lane merely because AirNow
    is an official provider.
- For network fetch routes, include `declared_side_effect=network-external`.
  Omit `requested_side_effect_approval` unless the route truly needs explicit
  operator approval; never pass booleans such as `false`.
- Fetch and normalization results must later be linked by
  `link-source-acquisition-execution` or another explicit lineage object before
  downstream agents rely on them.

## Scripts
- `scripts/submit_source_acquisition_proposal.py`
