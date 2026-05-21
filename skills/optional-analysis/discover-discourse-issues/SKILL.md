---
name: discover-discourse-issues
description: Optional-analysis helper for DB-backed public/formal discourse issue hints. It emits reversible issue hints, not claim candidates or report conclusions.
---

# Discover Discourse Issues

## Core Goal
- Read normalized public and formal signals from the DB.
- Emit reversible discourse issue hints with source signal ids, evidence refs, lineage, and caveats.
- Avoid claim extraction, truth assessment, source queue routing, or workflow advancement.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/discourse_issue_discovery_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `output_path`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. Issue hints are
  reversible labels for review, not claim candidates, public consensus,
  controversy conclusions, or report prose.
- Empty or sparse hints can reflect missing normalized rows, filters, keyword
  scope, parser limits, or source coverage. It is not proof that issues are
  absent from public/formal evidence.
- A council agent must carry useful hints into a finding, evidence bundle,
  challenge, proposal, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/discover_discourse_issues.py`
