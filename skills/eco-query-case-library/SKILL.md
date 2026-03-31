---
name: eco-query-case-library
description: Query archived historical cases from the shared case library so current rounds can retrieve analogous investigations and compact precedent summaries.
---

# Eco Query Case Library

## Core Goal
- Search archived cases by structured overlap and lexical cues.
- Return compact case matches with retrieval reasons and overlap metadata.
- Materialize one reusable archive query artifact for downstream history context.

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

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-first-refactor-blueprint.md`

## Scripts
- `scripts/eco_query_case_library.py`
