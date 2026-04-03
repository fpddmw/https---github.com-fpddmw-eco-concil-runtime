---
name: eco-extract-claim-candidates
description: Extract board-ready public claim candidates from normalized public signals in the unified signal plane, cluster repeated narratives by semantic fingerprint, and persist a compact analytics artifact for downstream council work.
---

# Eco Extract Claim Candidates

## Core Goal
- Read normalized public signals from the unified signal plane.
- Collapse repeated public narratives into compact claim candidates.
- Persist a candidate artifact for downstream clustering, scope derivation, and audit steps.

## Triggering Conditions
- Need a compact claim layer instead of raw public-signal rows.
- Need board-discussable public narratives with evidence refs.
- Need a stable candidate artifact for moderator, sociologist, or challenger workflows.

## Read/Write Contract
- Reads from `normalized_signals` where `plane = public` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Writes `runs/<run_id>/analytics/claim_candidates_<round_id>.json` by default.
- Syncs the emitted claim-candidate result set into the shared analysis-plane tables in `runs/<run_id>/analytics/signal_plane.sqlite`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `db_path`
  - `source_skill`
  - `claim_type`
  - `keyword_any`
  - `max_candidates`
  - `output_path`

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

## Scripts
- `scripts/eco_extract_claim_candidates.py`
