---
name: discover-discourse-issues
description: WP4 optional helper for DB-backed public/formal discourse issue hints. It emits reversible issue hints, not claim candidates or report conclusions.
---

# Discover Discourse Issues

## Core Goal
- Read normalized public and formal signals from the DB.
- Emit reversible discourse issue hints with source signal ids, evidence refs, lineage, and caveats.
- Avoid claim extraction, truth assessment, source queue routing, or workflow advancement.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/discourse_issue_discovery_<round_id>.json`

## Scripts
- `scripts/discover_discourse_issues.py`
