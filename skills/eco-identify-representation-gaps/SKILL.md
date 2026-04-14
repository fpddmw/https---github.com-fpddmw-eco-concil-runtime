---
name: eco-identify-representation-gaps
description: Identify formal-underrepresentation, public-underrepresentation, attention imbalance, and route mismatch by consuming formal-public linkage artifacts for environmental controversy issues.
---

# Eco Identify Representation Gaps

## Core Goal
- Turn formal/public linkage output into explicit representation-gap objects.
- Identify which issues are missing from formal participation, missing from open discourse, or disproportionately concentrated in one arena.
- Produce one board-consumable artifact for action planning and challenge opening.

## Triggering Conditions
- Formal/public linkage artifacts already exist.
- Need to answer “what is missing or imbalanced between formal participation and public discourse?”
- Need a machine-readable gap list before next actions or board review.

## Read/Write Contract
- Reads formal-public linkage results from the run-local analysis plane first.
- Falls back to `run_dir/analytics/formal_public_links_<round_id>.json` when needed.
- Writes `run_dir/analytics/representation_gaps_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `representation-gap`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `formal_public_links_path`
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
- `scripts/eco_identify_representation_gaps.py`
