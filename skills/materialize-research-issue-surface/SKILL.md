---
name: materialize-research-issue-surface
description: Optional-analysis helper for DB-backed research issue surfaces. It emits candidate issue records for human review, not controversy conclusions.
---

# Materialize Research Issue Surface

## Core Goal
- Convert DB public/formal signals or approved hints into candidate research issue surfaces.
- Preserve evidence refs, lineage, provenance, and optional-analysis governance metadata.
- Keep issue records appendix/audit only until DB basis objects cite them.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite` and optional approved input artifact.
- Writes `run_dir/analytics/research_issue_surface_<round_id>.json`

## Scripts
- `scripts/materialize_research_issue_surface.py`
