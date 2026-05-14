---
name: suggest-evidence-lanes
description: Optional-analysis helper for advisory evidence-lane tags. It does not route workflow, assign owners, or advance phases.
---

# Suggest Evidence Lanes

## Core Goal
- Read approved discovery hints or DB findings.
- Emit advisory evidence-lane tags for human review.
- Avoid route assignment, source queue decisions, readiness posture, or default investigator loops.

## Read/Write Contract
- Reads an optional discovery artifact or DB findings.
- Writes `run_dir/analytics/evidence_lane_suggestions_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `input_path`
- `output_path`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory tags. Lane suggestions do not route
  workflow, assign owners, drive source queues, set priorities, or advance
  phases.
- Empty or sparse tags can reflect missing discovery inputs, filters, or DB
  coverage. They do not prove that no evidence lane is available.
- A council agent or moderator must carry useful tags into a source acquisition
  proposal, evidence request, investigation plan, readiness opinion, or synthesis
  before downstream use.

## Scripts
- `scripts/suggest_evidence_lanes.py`
