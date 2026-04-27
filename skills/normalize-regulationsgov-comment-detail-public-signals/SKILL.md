---
name: normalize-regulationsgov-comment-detail-public-signals
description: Normalize fetch-regulationsgov-comment-detail results into provider-field formal-comment signals and write them into the signal plane database. Use when investigators need DB-backed policy-comment detail rows, artifact refs, provenance, validation metadata, and quality flags without heuristic issue, stance, concern, citation, or route typing.
---

# Eco Normalize Regulations.gov Comment Detail Public Signals

## Core Goal
- Read one `fetch-regulationsgov-comment-detail` raw artifact.
- Convert enriched comment-detail records into canonical formal-comment signals.
- Preserve provider fields such as docket, agency, comment id, submitter name, dates, detail validation, artifact provenance, and source quality flags.
- Write normalized rows into the unified signal plane without deriving issue, stance, concern, citation, submitter type, or route judgements.
- Return compact receipts, artifact refs, signal ids, and provenance-aware normalization hints.

## Triggering Conditions
- A fetch step already produced Regulations.gov comment-detail output.
- The council needs canonical policy-comment detail evidence instead of provider-native payloads.
- Downstream formal/public linkage, representation review, or policy-record analysis should operate from DB-backed normalized signals.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes normalized rows into `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite` with `plane = formal` and `canonical_object_kind = formal-comment-signal`.
- Writes DB index rows only for provider fields such as `docket_id`, `agency_id`, `submitter_name`, and `decision_source`.
- Does not generate claim candidates, issue labels, stance hints, concern facets, citation types, submitter type, route hints, board judgements, or report conclusions.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `artifact_path`
- Optional:
  - `db_path`

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
- `scripts/normalize_regulationsgov_comment_detail_public_signals.py`
