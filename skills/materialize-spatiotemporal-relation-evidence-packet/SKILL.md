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

## Scripts
- `scripts/materialize_spatiotemporal_relation_evidence_packet.py`
