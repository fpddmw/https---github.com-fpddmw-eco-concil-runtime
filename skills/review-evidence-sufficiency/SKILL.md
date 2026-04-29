---
name: review-evidence-sufficiency
description: Optional-analysis DB-backed evidence sufficiency review helper. It emits notes and caveats from findings, evidence bundles, report section basis, and challenger review comments; it is not a readiness gate.
---

# Eco Review Evidence Sufficiency

## Core Goal
- Review DB-backed investigation findings, evidence bundles, report section drafts, and challenger review comments.
- Emit structured sufficiency notes, gaps, counter-evidence cues, uncertainty notes, and report-use caveats.
- Avoid numeric readiness scores, coverage formulas, claim truth labels, or phase-gate semantics.
- Keep helper output advisory until a finding, evidence bundle, proposal, review comment, or report basis explicitly cites it.

## Triggering Conditions
- A moderator, investigator, or challenger has an approved optional-analysis request for evidence sufficiency review.
- The round already has DB council objects or report section basis to review.
- Need an audit/support artifact before a moderator decides whether to request a phase transition or a report editor cites a basis.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/evidence_sufficiency_review_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `target_kind`
  - `target_id`
  - `rubric_version`
  - `output_path`
  - `limit`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `review`
- `board_handoff`

## Side Effects
- `db-read`
- `writes-artifacts`

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-optional-analysis-skills-refactor-workplan.md`

## Scripts
- `scripts/review_evidence_sufficiency.py`
