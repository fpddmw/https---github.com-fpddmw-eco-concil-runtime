---
name: build-normalization-audit
description: Build a board-facing normalization audit from previously extracted claim and observation candidate artifacts, summarize candidate coverage and gaps, and persist a compact audit artifact for downstream moderation.
---

# Eco Build Normalization Audit

## Core Goal
- Read the current round's claim and observation candidate results.
- Summarize coverage, diversity, and unresolved matching gaps.
- Persist a normalization audit artifact for board review.

## Triggering Conditions
- Need a compact readiness check after candidate extraction.
- Need counts, distribution summaries, and gap hints before report-basis freeze.
- Need a shared audit artifact for moderator, challenger, or orchestrator workflows.

## Read/Write Contract
- Reads shared claim/observation candidate result sets from `runs/<run_id>/analytics/signal_plane.sqlite` first when available.
- Reads `claim_candidates_<round_id>.json` and `observation_candidates_<round_id>.json` as compatible artifact paths when present.
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
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/build_normalization_audit.py`
