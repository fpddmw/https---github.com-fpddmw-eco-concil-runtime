---
name: eco-cluster-claim-candidates
description: Cluster previously extracted claim candidates into board-reviewable claim groups, keep shared provenance refs, and persist a compact claim-cluster artifact for downstream evidence linking and challenge work.
---

# Eco Cluster Claim Candidates

## Core Goal
- Read the current round's claim candidate artifact.
- Group repeated or highly similar claim candidates into compact claim clusters.
- Persist a claim-cluster artifact for downstream evidence linking and board review.

## Triggering Conditions
- Need a lighter review surface than a flat claim-candidate list.
- Need to collapse repeated public narratives before evidence linking.
- Need stable cluster ids and provenance refs for board and challenge workflows.

## Read/Write Contract
- Reads `claim_candidates_<round_id>.json` by default.
- Writes `runs/<run_id>/analytics/claim_candidate_clusters_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `input_path`
  - `output_path`
  - `claim_type`
  - `keyword_any`
  - `min_member_count`
  - `max_clusters`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_cluster_claim_candidates.py`