---
name: detect-temporal-cooccurrence-cues
description: Optional-analysis helper for temporal co-occurrence cues across source families. It never infers influence, causality, spread, or direction.
---

# Detect Temporal Cooccurrence Cues

## Core Goal
- Read DB-backed public, formal, and environment signal timestamps.
- Emit same-day co-occurrence cues and timestamp limitations.
- When explicit relation scope flags are supplied, emit structured `spatiotemporal-relation-cue` rows.
- Avoid timestamp fallback defaults, influence claims, causality, spread, or direction.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/temporal_cooccurrence_cues_<round_id>.json`
- With relation scope flags, also writes `run_dir/analytics/spatiotemporal_relation_cues_<round_id>.json` and syncs it into the analysis plane.

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. Temporal co-occurrence
  is a cue, not influence, causality, transport, spread, direction, or source
  attribution.
- Empty or sparse cues can reflect window choice, timestamp availability, signal
  filters, missing normalization, or relation input scope. It is not proof that
  no relationship exists.
- A council agent must carry useful cues into a finding, evidence bundle,
  challenge, probe, review comment, or evidence packet before downstream use.

## Scripts
- `scripts/detect_temporal_cooccurrence_cues.py`
