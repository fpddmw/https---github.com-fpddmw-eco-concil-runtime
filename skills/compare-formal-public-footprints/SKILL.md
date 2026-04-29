---
name: compare-formal-public-footprints
description: Optional-analysis helper for comparing formal-record and public-discourse footprints. It describes overlap and absence cues without alignment scoring.
---

# Compare Formal Public Footprints

## Core Goal
- Read public/formal normalized signals and optional taxonomy label cues.
- Emit footprint summaries, overlap terms, and source-family caveats.
- Avoid paired discourse links, alignment scores, representation findings, or conclusions.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/formal_public_footprints_<round_id>.json`

## Scripts
- `scripts/compare_formal_public_footprints.py`
