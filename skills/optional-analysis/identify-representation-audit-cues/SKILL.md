---
name: identify-representation-audit-cues
description: Optional-analysis helper for representation audit cues. It emits human-review prompts, not representation gap findings or severity scores.
---

# Identify Representation Audit Cues

## Core Goal
- Read DB-backed public and formal source families.
- Emit audit cues about source-family presence and participant-name coverage.
- Avoid severity scores, representation findings, or report conclusions.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/representation_audit_cues_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `output_path`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. Representation cues
  are prompts for human or council review, not severity scores, representation
  findings, or report conclusions.
- Empty or sparse cues can reflect missing normalized rows, filters, source
  coverage, or unavailable metadata. They do not prove that representation gaps
  are absent.
- A council agent must carry useful cues into a finding, evidence bundle,
  challenge, proposal, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/identify_representation_audit_cues.py`
