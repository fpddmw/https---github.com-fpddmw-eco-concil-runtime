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

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `input_path`
- `output_path`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. Typed actor, concern,
  citation, and stance cues are projections for review, not conclusions, report
  prose, or taxonomy defaults.
- Empty or sparse projections can reflect missing issue surfaces, normalized
  rows, filters, or metadata coverage. They do not prove that actors, concerns,
  citations, or stances are absent.
- A council agent must carry useful cues into a finding, evidence bundle,
  challenge, proposal, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/project_research_issue_views.py`
