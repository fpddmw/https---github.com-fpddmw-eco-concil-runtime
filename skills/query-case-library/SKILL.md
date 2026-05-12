---
name: query-case-library
description: Query archived historical cases from the shared case library so current rounds can retrieve evidence refs and match surfaces without ranking or recommending conclusions.
---

# Eco Query Case Library

## Core Goal
- Filter archived cases by structured overlap and lexical cues.
- Return compact case matches with retrieval reasons and overlap metadata only.
- Materialize one reusable archive query artifact for downstream history context.
- Do not score, rank, tier, or recommend archived cases; agents decide relevance and use.

## Triggering Conditions
- A case library archive database already exists.
- Need cross-run precedent before moderator or investigator planning.
- Need a stable query surface before rendering history context.

## Read/Write Contract
- Reads `run_dir/../archives/eco_case_library.sqlite` by default.
- Writes `run_dir/archive/case_library_query_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `db_path`
  - `output_path`
  - `query_text`
  - `region_label`
  - `profile_id`
  - `claim_type`
  - `metric_family`
  - `gap_type`
  - `source_skill`
  - `exclude_case_id`
  - `limit`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- Treat returned cases as historical hints with evidence refs, not as current-run
  proof, source ranking, or recommended conclusions.
- Empty or sparse matches can reflect missing archives, filters that are too
  narrow, lexical mismatch, excluded case ids, or archive import gaps. It does
  not prove that no analogous case exists.
- When reusing historical context, record which current-run evidence need it
  informs and keep the current run's findings tied to current-run evidence refs.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_case_library.py`
