---
name: suggest-evidence-lanes
description: WP4 optional helper for advisory evidence-lane tags. It does not route workflow, assign owners, or advance phases.
---

# Suggest Evidence Lanes

## Core Goal
- Read approved discovery hints or DB findings.
- Emit advisory evidence-lane tags for human review.
- Avoid route assignment, source queue decisions, readiness posture, or default investigator loops.

## Read/Write Contract
- Reads an optional discovery artifact or DB findings.
- Writes `run_dir/analytics/evidence_lane_suggestions_<round_id>.json`

## Scripts
- `scripts/suggest_evidence_lanes.py`
