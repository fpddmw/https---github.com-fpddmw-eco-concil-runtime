---
name: query-formal-signals
description: Query compact formal-signal rows from the unified signal plane database with run, round, round-scope, source, kind, publication window, docket, agency, submitter, optional typed metadata, and keyword filters. Use when an investigator or operator needs DB-backed formal-record evidence refs and item-level evidence basis without reopening raw Regulations.gov artifacts.
---

# Query Formal Signals

## Core Goal
- Read compact formal signal rows from the unified signal plane database.
- Filter by run, round or cross-round scope, source skill, signal kind, publication window, docket, agency, submitter, keyword, and optional typed metadata if an approved parser or analysis skill has written it.
- Return short results with item-level `evidence_refs` and `evidence_basis`.

## Triggering Conditions
- Need formal-record evidence without reopening raw comment artifacts.
- Need docket-, agency-, submitter-, or keyword-scoped formal input for linkage, representation, or policy-record review.
- Need issue-, stance-, concern-, citation-, or route-scoped formal input only when those optional typed fields already exist in DB.
- Need compact references for moderator, investigator, report-editor, or challenger work.
- Need to reopen prior-round formal evidence while staying inside the same run.

## Read/Write Contract
- Read only.
- Reads from `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite` with `plane = formal`.
- Does not write to the database.
- Does not derive missing typed metadata while querying.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `db_path`
  - `round_scope`
- Optional filters:
  - `source_skill`
  - `signal_kind`
  - `published_after_utc`
  - `published_before_utc`
  - `docket_id`
  - `agency_id`
  - `submitter_type`
  - `issue_label`
  - `concern_facet`
  - `citation_type`
  - `stance_hint`
  - `route_hint`
  - `keyword_any`
  - `limit`

## Output Contract
- `status`
- `summary`
- `result_count`
- `results`
  - Each result includes `evidence_refs` and `evidence_basis`.
- `artifact_refs`
- `warnings`
- `board_handoff`
  - Suggested next steps are lookup, finding, evidence bundle, or discussion writes.

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/query_formal_signals.py`
