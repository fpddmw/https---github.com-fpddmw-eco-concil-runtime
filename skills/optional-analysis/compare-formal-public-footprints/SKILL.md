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

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `taxonomy_labels_path`
- `output_path`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. Footprint overlap or
  absence cues are not alignment scores, paired discourse links,
  representation findings, or report conclusions.
- Empty or narrow comparison can reflect missing normalization, source-family
  coverage, filters, or archive/import scope. It is not proof that a public or
  formal footprint is absent.
- A council agent must carry useful cues into a finding, evidence bundle,
  challenge, proposal, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/compare_formal_public_footprints.py`
