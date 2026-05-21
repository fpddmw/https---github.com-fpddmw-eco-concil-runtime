---
name: materialize-spatiotemporal-relation-evidence-packet
description: Build a report-basis-mediated packet from DB-backed spatiotemporal relation cues, challenger objections, and uncertainty constraints.
---

# Materialize Spatiotemporal Relation Evidence Packet

## Core Goal
- Read `spatiotemporal-relation-cue` analysis result items.
- Read relation-oriented challenge, probe, and review-comment objects.
- Write a cautious `spatiotemporal-relation-evidence-packet` artifact that can be cited by finding, evidence bundle, or report section draft.
- Do not treat the helper artifact itself as direct frozen report basis.
- Do not assert causality, transport proof, source attribution, or exclusion of local alternatives.

## Read/Write Contract
- Reads analysis and deliberation tables in `run_dir/analytics/signal_plane.sqlite`.
- Writes `run_dir/reporting/spatiotemporal_relation_evidence_packet_<round_id>.json`.
- With `--write-basis-objects`, also writes DB-backed finding, evidence bundle, and report section draft records.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `relation_id`
- `relation_status`
- `output_path`
- `write_basis_objects`
- `report_id`
- `section_key`
- `agent_role`
- `report_agent_role`
- `confidence`
- `limit`

## Agent Reasoning Guide
- Treat the packet as cautious evidence organization around relation cues,
  objections, and uncertainty. It is not transport proof, causality proof, source
  attribution, or exclusion of local alternatives.
- Without `--write-basis-objects`, the artifact remains an export and should not
  be treated as direct frozen report basis.
- If written into basis objects, preserve challenger objections, rejected
  alternatives, uncertainty, and item-level evidence refs.

## Scripts
- `scripts/materialize_spatiotemporal_relation_evidence_packet.py`
