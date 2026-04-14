---
name: eco-detect-cross-platform-diffusion
description: Detect cross-platform diffusion and formal-public spillover around environmental controversy issues by consuming formal-public linkage artifacts and normalized public signals.
---

# Eco Detect Cross Platform Diffusion

## Core Goal
- Detect whether an issue remains confined to one platform or appears across multiple public and formal arenas.
- Infer cross-public diffusion and public-to-formal or formal-to-public spillover from issue-level signal timing and platform overlap.
- Produce one reusable diffusion-edge artifact for board planning and reporting.

## Triggering Conditions
- Formal/public linkage artifacts already exist.
- Normalized public signals are present in the signal plane.
- Need to answer whether an issue spreads across platforms and how it likely travels.

## Read/Write Contract
- Reads formal-public linkage results from the run-local analysis plane first.
- Reads normalized signals from `run_dir/analytics/signal_plane.sqlite`.
- Falls back to `run_dir/analytics/formal_public_links_<round_id>.json` when needed.
- Writes `run_dir/analytics/diffusion_edges_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `diffusion-edge`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `formal_public_links_path`
  - `db_path`
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
- `scripts/eco_detect_cross_platform_diffusion.py`
