---
name: detect-temporal-cooccurrence-cues
description: Optional-analysis helper for temporal co-occurrence cues across source families. It never infers influence, causality, spread, or direction.
---

# Detect Temporal Cooccurrence Cues

## Core Goal
- Read DB-backed public, formal, and environment signal timestamps.
- Emit same-day co-occurrence cues and timestamp limitations.
- Avoid timestamp fallback defaults, influence claims, causality, spread, or direction.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/temporal_cooccurrence_cues_<round_id>.json`

## Scripts
- `scripts/detect_temporal_cooccurrence_cues.py`
