---
name: apply-approved-formal-public-taxonomy
description: Optional-analysis helper that applies an explicit approved mission-scoped taxonomy to public/formal DB signals and emits candidate labels for audit.
---

# Apply Approved Formal Public Taxonomy

## Core Goal
- Require an approved taxonomy file or record reference before labeling public/formal records.
- Emit candidate labels with signal evidence refs and taxonomy approval metadata.
- Avoid global default taxonomies or report-ready interpretations.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite` and an approved taxonomy artifact.
- Writes `run_dir/analytics/formal_public_taxonomy_labels_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `taxonomy_path`
- `taxonomy_version`
- `approval_ref`
- `output_path`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. Candidate taxonomy
  labels do not become findings, issue truth, representation conclusions, or
  report categories by themselves.
- Empty or sparse labels can reflect the approved taxonomy scope, missing
  normalized rows, filters, or parser limits. It is not proof that issues or
  stances are absent.
- A council agent must cite and interpret candidate labels through a finding,
  evidence bundle, challenge, proposal, readiness opinion, or synthesis before
  downstream use.

## Scripts
- `scripts/apply_approved_formal_public_taxonomy.py`
