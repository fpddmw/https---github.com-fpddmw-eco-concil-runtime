---
name: eco-derive-claim-scope
description: Derive matching-oriented claim scope proposals from claim-side evidence objects, infer geographic and thematic scope hints, and persist a compact scope artifact for downstream evidence coverage and board work.
---

# Eco Derive Claim Scope

## Core Goal
- Read claim clusters if available, otherwise fall back to claim candidates.
- Infer compact claim scope proposals for matching and challenge review.
- Persist a scope artifact for evidence coverage and board workflows.

## Triggering Conditions
- Claim-side evidence exists but claim scope is still underspecified.
- Matching and challenge review need explicit scope labels and tags.
- Need a bridge artifact before evidence coverage scoring or board work.

## Read/Write Contract
- Reads `claim_candidate_clusters_<round_id>.json` by default and falls back to `claim_candidates_<round_id>.json`.
- Writes `runs/<run_id>/analytics/claim_scope_proposals_<round_id>.json` by default.
- Syncs the same claim-scope result set into `runs/<run_id>/analytics/signal_plane.sqlite` as analysis-plane state.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_cluster_path`
  - `claim_candidates_path`
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
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_derive_claim_scope.py`
