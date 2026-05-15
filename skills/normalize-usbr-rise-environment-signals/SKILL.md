---
name: normalize-usbr-rise-environment-signals
description: Normalize fetch-usbr-rise artifacts into provider-field environment signal-plane rows for USBR operational time-series records. Use when investigators need DB-backed reservoir, release, storage, or elevation observations with provenance and quality flags, without severity scoring, compliance judgement, source ranking, or conclusions.
---

# Normalize USBR RISE Environment Signals

## Core Goal
- Read one `fetch-usbr-rise-v1` raw artifact.
- Convert RISE result records into environment signal-plane rows.
- Preserve item, location, parameter, unit, timestamp, value, provider disclaimer, artifact path, and record locator.
- Avoid deriving shortage severity, operating compliance, governance responsibility, risk score, or report conclusions.

## Read/Write Contract
- Reads one raw artifact from disk.
- Writes rows into `normalized_signals` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Uses `plane = environment` and `canonical_object_kind = environment-observation-signal`.
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
- Treat normalization as provider-field translation only. It does not determine whether a reservoir operation is severe, compliant, sufficient, or report-ready.
- Zero `canonical_ids` or warnings can reflect artifact shape, missing `records`, unsupported fetch output, or metadata gaps. It is not proof that operational records are absent.
- Before using a no-row result as a limitation, inspect `warnings`, source artifact shape, item IDs, date filters, and record locators.
- Report-facing use requires explicit council uptake through findings, evidence bundles, review comments, readiness opinions, synthesis, or report basis objects.

## Script
- `scripts/normalize_usbr_rise_environment_signals.py`
