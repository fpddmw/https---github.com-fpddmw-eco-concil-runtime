---
name: eco-classify-claim-verifiability
description: Classify claim-side controversy objects into empirical, procedural, representational, or mixed verifiability lanes by reading claim scope proposals from the analysis plane and emitting one reusable assessment artifact.
---

# Eco Classify Claim Verifiability

## Core Goal
- Turn claim scope outputs into explicit verifiability assessments.
- Decide whether each controversy point belongs in environmental verification, formal record review, discourse analysis, or mixed review.
- Emit one compact artifact that later routing and controversy-map skills can reuse.

## Triggering Conditions
- Claim scope proposals already exist.
- Need an explicit answer to "should this issue go to observation matching?"
- Need a reusable routing precursor before next actions or controversy mapping.

## Read/Write Contract
- Reads claim scope results from the run-local analysis plane first.
- Falls back to `run_dir/analytics/claim_scope_proposals_<round_id>.json` when needed.
- Writes `run_dir/analytics/claim_verifiability_assessments_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `claim-verifiability`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_scope_path`
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
- `scripts/eco_classify_claim_verifiability.py`
