---
name: eco-extract-evidence-citation-types
description: Derive canonical evidence-citation-type objects from canonical issue-cluster rows so citation posture becomes a first-class DB surface instead of an embedded map field.
---

# Eco Extract Evidence Citation Types

## Core Goal
- Project canonical `issue-cluster` rows into canonical `evidence-citation-type` objects.
- Separate citation posture from the high-level controversy-map wrapper.

## Triggering Conditions
- Canonical issue clusters already exist.
- Need explicit citation posture for planning, review, or reporting.

## Read/Write Contract
- Reads issue-cluster results from the run-local analysis plane first.
- Falls back to `run_dir/analytics/issue_clusters_<round_id>.json` when needed.
- Writes `run_dir/analytics/evidence_citation_types_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `evidence-citation-type`.

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
- `scripts/eco_extract_evidence_citation_types.py`
