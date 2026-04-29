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

## Scripts
- `scripts/export_research_issue_map.py`
