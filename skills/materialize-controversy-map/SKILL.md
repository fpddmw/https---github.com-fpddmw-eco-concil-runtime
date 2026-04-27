---
name: materialize-controversy-map
description: Materialize a compact controversy map by aggregating DB-native issue-cluster and typed controversy surfaces into one board-consumable artifact.
---

# Eco Materialize Controversy Map

## Core Goal
- Turn typed issue surfaces into one explicit board-facing controversy-map artifact.
- Aggregate issue clusters, dominant stances, concern facets, actors, and routing posture without making the map skill own all typed extraction.
- Provide a board-ready object that explains what the controversy is after typed issue surfaces are already queryable on their own.

## Triggering Conditions
- Canonical issue clusters already exist, or need to be inlined as a compatibility fallback.
- Need a compact issue-level picture rather than raw typed result sets.
- Need an artifact that can support board notes, next actions, and reporting.

## Read/Write Contract
- Reads `issue-cluster / stance-group / concern-facet / actor-profile / evidence-citation-type` results from the run-local analysis plane first.
- Falls back to compatible artifact paths when synced typed result sets are unavailable.
- Can inline regenerate missing issue-cluster and typed issue surfaces from claim-side inputs as an explicit compatibility fallback.
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
  - `issue_clusters_path`
  - `stance_groups_path`
  - `concern_facets_path`
  - `actor_profiles_path`
  - `evidence_citation_types_path`
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
- `scripts/materialize_controversy_map.py`
