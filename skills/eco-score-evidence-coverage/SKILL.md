---
name: eco-score-evidence-coverage
description: Score claim-side evidence coverage using links and scope proposals, summarize readiness and unresolved gaps, and persist a compact coverage artifact for board review and challenge routing.
---

# Eco Score Evidence Coverage

## Core Goal
- Read claim-observation links and available scope proposals.
- Score claim-side evidence coverage and readiness.
- Persist a compact coverage artifact for board review and challenge routing.

## Triggering Conditions
- Need a compact readiness view before board writeback or challenge work.
- Need claim-level coverage scores rather than raw link lists.
- Need a summary object that can guide moderator and challenger behavior.

## Read/Write Contract
- Reads claim-observation links from the run-local analysis plane first.
- Reads claim and observation scope proposals from the run-local analysis plane first.
- Falls back to the corresponding JSON artifacts when the synced result sets are unavailable.
- Writes `runs/<run_id>/analytics/evidence_coverage_<round_id>.json` by default.
- Syncs the same coverage result set into `runs/<run_id>/analytics/signal_plane.sqlite` as analysis-plane state.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `links_path`
  - `claim_scope_path`
  - `observation_scope_path`
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
- `input_analysis_sync`
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-db-first-agent-runtime-blueprint.md`

## Scripts
- `scripts/eco_score_evidence_coverage.py`
