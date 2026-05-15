---
name: normalize-official-governance-records
description: Normalize fetch-federal-register-documents or fetch-usbr-project-records artifacts into provider-field formal signal-plane rows. Use when investigators need DB-backed official governance records with provenance and quality flags, without legal interpretation, issue labels, source ranking, or conclusions.
---

# Normalize Official Governance Records

## Core Goal
- Read one `official-governance-record-fetch-v1` raw artifact.
- Convert EPA EIS records, Federal Register documents, or USBR project records into formal signal-plane rows.
- Preserve provider fields such as agency, docket IDs, publication date, URL, record type, document type, artifact path, and record locator.
- Avoid deriving legal meaning, issue labels, stance, concern, citation type, evidence sufficiency, or report conclusions.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes rows into `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Uses `plane = formal` and `canonical_object_kind = formal-comment-signal` for the current formal-record surface.
- Emits compact normalization receipts, artifact refs, canonical ids, warnings, and board handoff hints.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `artifact_path`
- Optional `db_path`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- Treat this as provider-field translation only. The normalizer does not decide whether a document is legally controlling, comprehensive, relevant, sufficient, or report-ready.
- Zero `canonical_ids` or warnings can reflect unsupported artifact shape, missing `records`, unsupported source skill, or parser coverage. It is not proof that official records are absent.
- Before using a no-row result as a limitation, inspect `warnings`, the source artifact shape, and the paired fetch skill; rerun the correct fetch or normalizer when the evidence need remains live.
- Report-facing use requires explicit council uptake through findings, evidence bundles, review comments, or report basis objects.

## Script
- `scripts/normalize_official_governance_records.py`
