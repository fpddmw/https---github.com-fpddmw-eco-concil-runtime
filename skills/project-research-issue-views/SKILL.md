---
name: project-research-issue-views
description: Optional-analysis helper for typed actor, concern, citation, and stance cues from DB-backed issue surfaces.
---

# Project Research Issue Views

## Core Goal
- Read candidate issue surfaces and DB discourse signals.
- Emit typed cue projections for human audit.
- Avoid report writing, taxonomy defaulting, and conclusion scoring.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite` and optional issue-surface artifact.
- Writes `run_dir/analytics/research_issue_views_<round_id>.json`

## Scripts
- `scripts/project_research_issue_views.py`
