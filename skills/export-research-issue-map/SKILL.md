---
name: export-research-issue-map
description: Optional-analysis export helper for research issue navigation maps. It emits traceability nodes and edges, not a controversy conclusion graph.
---

# Export Research Issue Map

## Core Goal
- Build a human-readable navigation export from issue surfaces and typed issue views.
- Keep edges as traceability cues only.
- Avoid controversy conclusions, influence claims, or phase movement.

## Read/Write Contract
- Reads issue surface/view artifacts.
- Writes `run_dir/analytics/research_issue_map_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `issue_surface_path`
- `issue_views_path`
- `output_path`

## Agent Reasoning Guide
- Treat output as approval-scoped navigation material. Nodes and edges are not a
  controversy conclusion graph, influence model, phase decision, or report
  basis.
- Empty or sparse maps can reflect missing issue-surface inputs, filters, or
  projection scope. They do not prove that research issues are absent.
- A council agent must carry useful map cues into a finding, challenge,
  proposal, readiness opinion, synthesis, or report-basis object before
  downstream use.

## Scripts
- `scripts/export_research_issue_map.py`
