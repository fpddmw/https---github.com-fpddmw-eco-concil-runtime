---
name: eco-extract-concern-facets
description: Derive canonical concern-facet objects from canonical issue-cluster rows so controversy concerns become a first-class DB surface.
---

# Eco Extract Concern Facets

## Core Goal
- Project canonical `issue-cluster` rows into canonical `concern-facet` objects.
- Separate concern decomposition from the high-level controversy-map wrapper.

## Triggering Conditions
- Canonical issue clusters already exist.
- Need explicit concern facets for planning, challenge opening, or reporting.

## Read/Write Contract
- Reads issue-cluster results from the run-local analysis plane first.
- Falls back to `run_dir/analytics/issue_clusters_<round_id>.json` when needed.
- Writes `run_dir/analytics/concern_facets_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `concern-facet`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `issue_clusters_path`
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
- `scripts/eco_extract_concern_facets.py`
