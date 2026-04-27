---
name: route-verification-lane
description: Route claim-side controversy objects into environmental verification, formal-record review, discourse analysis, or mixed review by consuming claim-verifiability assessments and emitting one explicit routing artifact.
---

# Eco Route Verification Lane

## Core Goal
- Decide which claims should enter the environmental verification lane.
- Prevent procedural or representational controversies from defaulting into observation matching.
- Emit one routing artifact that board, planning, and controversy-map steps can consume.

## Triggering Conditions
- Verifiability assessments already exist, or claim scopes exist and need explicit routing.
- A moderator needs a deterministic answer to "what should happen next for each issue?"
- Need a machine-readable route summary before next actions or reporting.

## Read/Write Contract
- Reads claim-verifiability results from the run-local analysis plane first.
- Falls back to `run_dir/analytics/claim_verifiability_assessments_<round_id>.json` when needed.
- Can derive minimal routes from claim scopes as a compatible fallback.
- Writes `run_dir/investigation/verification_routes_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `verification-route`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_verifiability_path`
  - `claim_scope_path`
  - `output_path`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `analysis_sync`
- `board_handoff`

## References
- `../../docs/openclaw-next-phase-development-plan.md`
- `../../docs/openclaw-skill-refactor-checklist.md`

## Scripts
- `scripts/route_verification_lane.py`
