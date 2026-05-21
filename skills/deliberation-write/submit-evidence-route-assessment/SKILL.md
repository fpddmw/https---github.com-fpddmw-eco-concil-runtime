---
name: submit-evidence-route-assessment
description: Record an investigator or challenger assessment that a live evidence need should be pursued through a different route, cannot be answered by the currently visible source surface, or needs moderator acknowledgement before the same request is repeated.
---

# Submit Evidence Route Assessment

## Core Goal
- Record one `evidence-route-assessment` for an evidence request, source attempt, round focus, or challenge.
- Make route mismatch, capability gap, source-surface limits, same-family follow-up checks, and recommended procedural next step visible to the council.
- Preserve investigator autonomy: the author states why the current route is or is not actionable; the moderator decides whether to continue, re-route, pause for capability work, or proceed with a bounded report.

## Boundaries
- Do not include source ranking, priority, scores, weights, support levels, or automatic readiness gates.
- Do not use this object to prove that evidence does not exist in the world.
- Do not use this object to close an issue by itself; moderator synthesis must explicitly cite it before it affects continuation or reporting.
- Do not make runtime select a replacement source. The assessment may name candidate alternate routes, but agents still own source choice and query design.
- This is not source ranking and not a runtime-owned source-selection mechanism.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `rationale`
- `author_role`
- `target_kind`
- `target_id`

## Useful Optional Input
- `assessment_type`: for example `source-surface-mismatch`, `capability-gap`, `route-discovery-needed`, `no-actionable-current-route`, `same-family-followup-needed`.
- `route_judgment`: concise judgement about the current route.
- `source_surface_status`: for example `insufficient-current-surface`, `wrong-source-family`, `needs-followup-skill`, `external-capability-gap`.
- `evidence_need_summary`
- `current_surface_summary`
- `recommended_next_step`: for example `route-discovery-continuation`, `capability-gap-human-pause`, `bounded-report-with-limitation`, `revise-request`.
- `continuation_mode`
- `capability_gap_kind`
- `target_evidence_request_id`
- `considered_source_family`
- `considered_source_skill`
- `same_family_followup_considered`
- `alternate_route`
- `capability_gap_ref`
- `rejected_route_ref`
- `evidence_ref`
- `payload_json`
- `provenance_json`

## Output Contract
- Appends one canonical `evidence-route-assessment` row to the deliberation DB.
- Writes one runtime-local submission artifact.
- Returns `canonical_ids`, `artifact_refs`, and a DB query handoff.

## Agent Reasoning Guide
- Use this when a requested evidence need is real but the currently visible source surface cannot answer it, or when a fetch/query path failed enough that repeating the same route would be a non-move.
- Before recording a route mismatch, state what you considered: same-family follow-up skills, revised query/window/parameters, alternate providers, and whether the missing need is outside the current skill surface.
- A route assessment is not refusal to investigate. It is the council-visible reason to re-route, open route-discovery continuation, pause for capability work, or generate only a bounded report.
- If the moderator wants to repeat the same evidence request after this object is recorded, the moderator should explicitly acknowledge or disagree with the assessment in `round-synthesis`.

## Scripts
- `scripts/submit_evidence_route_assessment.py`
