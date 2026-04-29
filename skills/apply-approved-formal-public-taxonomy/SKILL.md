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

## Scripts
- `scripts/apply_approved_formal_public_taxonomy.py`
