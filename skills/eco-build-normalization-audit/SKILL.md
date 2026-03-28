---
name: eco-build-normalization-audit
description: Build a board-facing normalization audit from previously extracted claim and observation candidate artifacts, summarize candidate coverage and gaps, and persist a compact audit artifact for downstream moderation.
---

# Eco Build Normalization Audit

## Core Goal
- Read the current round's claim and observation candidate artifacts.
- Summarize coverage, diversity, and unresolved matching gaps.
- Persist a normalization audit artifact for board review.

## Triggering Conditions
- Need a compact readiness check after candidate extraction.
- Need counts, distribution summaries, and gap hints before promotion.
- Need a shared audit artifact for moderator, challenger, or orchestrator workflows.

## Read/Write Contract
- Reads `claim_candidates_<round_id>.json` and `observation_candidates_<round_id>.json`.
- Writes `runs/<run_id>/analytics/normalization_audit_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_candidates_path`
  - `observation_candidates_path`
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
- `scripts/eco_build_normalization_audit.py`