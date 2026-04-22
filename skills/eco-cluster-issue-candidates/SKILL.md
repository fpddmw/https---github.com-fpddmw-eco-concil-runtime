---
name: eco-cluster-issue-candidates
description: Cluster claim-side controversy inputs into canonical issue-cluster objects by merging claim clusters, claim scopes, verifiability assessments, and verification routes into one DB-native issue surface.
---

# Eco Cluster Issue Candidates

## Core Goal
- Turn the claim-side chain into canonical `issue-cluster` objects.
- Stop treating `controversy-map` as the only typed issue surface.
- Give board, agenda, and reporting code a DB-native issue layer before any high-level map aggregation.

## Triggering Conditions
- Claim clusters, claim scopes, verifiability assessments, and routes already exist.
- Need canonical issue objects rather than one board-facing wrapper.
- Need typed controversy state that can be queried or reused without rerunning map aggregation.

## Read/Write Contract
- Reads claim clusters, claim scopes, claim-verifiability assessments, and verification routes from the run-local analysis plane first.
- Falls back to compatible artifact paths when synced result sets are unavailable.
- Writes `run_dir/analytics/issue_clusters_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `issue-cluster`.

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
- `scripts/eco_cluster_issue_candidates.py`
