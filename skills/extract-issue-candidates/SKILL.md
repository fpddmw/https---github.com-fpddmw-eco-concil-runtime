---
name: extract-issue-candidates
description: Derive scope-level canonical issue-cluster candidates directly from claim scopes, verifiability assessments, and verification routes before any claim-cluster merge happens.
---

# Eco Extract Issue Candidates

## Core Goal
- Materialize pre-merge `issue-cluster` candidates from the claim-side chain.
- Expose a candidate-level issue surface that is queryable and auditable before later clustering compresses related claims together.
- Keep the canonical issue object shape stable while separating extraction from clustering.

## Triggering Conditions
- Claim scopes, verifiability assessments, and verification routes already exist.
- Need an issue-candidate view before `cluster-issue-candidates` merges related claims.
- Need a DB-native issue surface that can drive typed decomposition or board review even when claim-cluster merge is deferred.

## Read/Write Contract
- Reads claim scopes, verifiability assessments, verification routes, and any available claim clusters from the run-local analysis plane first.
- Uses claim scopes as the primary candidate source, even when merged claim clusters already exist.
- Writes `run_dir/analytics/issue_candidates_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as canonical `issue-cluster` rows.

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
- `scripts/extract_issue_candidates.py`
