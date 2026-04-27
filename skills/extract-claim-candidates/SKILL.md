---
name: extract-claim-candidates
description: Optional audited helper for extracting public narrative seed candidates from normalized public signals. It requires operator-approved skill approval and is not a default investigation chain.
---

# Eco Extract Claim Candidates

## Core Goal
- Read normalized public signals from the unified signal plane.
- Collapse repeated public narratives into compact claim-candidate objects for optional review.
- Persist candidate rows with evidence refs, provenance, and rule metadata for audit.
- Keep this output as a derived narrative surface, not the required ontology for every policy investigation.
- Require `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` before execution.

## Triggering Conditions
- A moderator or investigator explicitly needs optional public narrative seeds after direct DB query is insufficient.
- The operator has approved this optional-analysis run for the current round and actor role.
- Need a stable candidate artifact for audit, challenge, or later optional clustering.

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
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/extract_claim_candidates.py`
