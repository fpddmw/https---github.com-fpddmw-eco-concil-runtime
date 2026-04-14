---
name: eco-materialize-controversy-map
description: Materialize a compact controversy map by combining issue clusters, claim scope proposals, verifiability assessments, and verification routes into one board-consumable artifact.
---

# Eco Materialize Controversy Map

## Core Goal
- Turn the current public-side analysis chain into one explicit controversy-map artifact.
- Summarize issue clusters, dominant stances, concern facets, actors, and routing posture.
- Provide a board-ready object that explains what the controversy is before further verification work.

## Triggering Conditions
- Claim clusters and claim scopes already exist.
- Need a compact issue-level picture rather than raw candidate / cluster / scope lists.
- Need an artifact that can support board notes, next actions, and reporting.

## Read/Write Contract
- Reads claim clusters from the run-local analysis plane first.
- Reads claim scopes, claim-verifiability assessments, and verification routes from the run-local analysis plane when present.
- Falls back to compatible artifact paths when synced result sets are unavailable.
- Writes `run_dir/analytics/controversy_map_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `controversy-map`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_cluster_path`
  - `claim_scope_path`
  - `claim_verifiability_path`
  - `verification_route_path`
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
- `scripts/eco_materialize_controversy_map.py`
