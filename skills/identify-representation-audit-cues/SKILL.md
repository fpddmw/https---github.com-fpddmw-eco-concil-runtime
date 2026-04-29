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

## Scripts
- `scripts/identify_representation_audit_cues.py`
