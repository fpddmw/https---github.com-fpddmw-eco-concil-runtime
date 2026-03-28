---
name: eco-link-claims-to-observations
description: Link claim-side evidence objects to observation-side evidence objects, score support or contradiction heuristically, and persist a compact claim-observation link artifact for board review and challenge work.
---

# Eco Link Claims To Observations

## Core Goal
- Read claim-side and observation-side candidate or grouped artifacts.
- Propose support, contradiction, or contextual links between them.
- Persist a link artifact for board review, challenge work, and later promotion gates.

## Triggering Conditions
- Need claim-to-observation evidence hypotheses instead of isolated candidate lists.
- Need a compact link layer before board, challenge, or promotion skills.
- Need stable link ids, rule traces, and provenance refs for auditability.

## Read/Write Contract
- Reads claim clusters if available, otherwise falls back to claim candidates.
- Reads merged observations if available, otherwise falls back to observation candidates.
- Writes `runs/<run_id>/analytics/claim_observation_links_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_cluster_path`
  - `claim_candidates_path`
  - `merged_observations_path`
  - `observation_candidates_path`
  - `output_path`
  - `min_score`
  - `top_links_per_claim`

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
- `../../openclaw-skill-phase-plan.md`

## Scripts
- `scripts/eco_link_claims_to_observations.py`