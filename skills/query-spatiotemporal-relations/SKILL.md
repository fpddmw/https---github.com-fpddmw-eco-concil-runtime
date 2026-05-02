---
name: query-spatiotemporal-relations
description: Query DB-backed spatiotemporal relation cues from the analysis plane by relation id, source/target signal, role, type, or status.
---

# Query Spatiotemporal Relations

## Core Goal
- Read `spatiotemporal-relation-cue` analysis result items.
- Return relation cue rows with item-level evidence refs and lineage.
- Do not infer causality, transport, source attribution, or report readiness.

## Read/Write Contract
- Reads analysis result tables in `run_dir/analytics/signal_plane.sqlite`
- Writes no DB rows.

## Scripts
- `scripts/query_spatiotemporal_relations.py`
