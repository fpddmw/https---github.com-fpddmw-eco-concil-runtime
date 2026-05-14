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

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `result_set_id`
- `relation_id`
- `relation_type`
- `relation_status`
- `source_signal_id`
- `target_signal_id`
- `source_role`
- `target_role`
- `latest_only`
- `include_result_sets`
- `include_contract`
- `limit`
- `offset`
- `db_path`

## Agent Reasoning Guide
- Treat returned rows as candidate relation cues that must be carried by a
  challenge, finding, evidence packet, or review note before downstream use.
- Empty results can reflect missing cue materialization, wrong relation id,
  source/target filters, role filters, or analysis-plane import gaps. They do
  not prove that no spatiotemporal relationship exists.
- Do not infer causality, transport, source attribution, readiness, or report
  basis from this query surface alone.

## Scripts
- `scripts/query_spatiotemporal_relations.py`
