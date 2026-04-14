---
name: eco-link-formal-comments-to-public-discourse
description: Link Regulations.gov-style formal comments and open-platform public discourse around shared environmental controversy issues by consuming claim clusters, claim candidates, verification routes, and normalized public signals.
---

# Eco Link Formal Comments To Public Discourse

## Core Goal
- Connect formal participation traces and open-platform public discourse at the issue level.
- Distinguish aligned, formal-only, and public-only controversy footprints.
- Produce one reusable linkage artifact for representation-gap analysis and board planning.

## Triggering Conditions
- Claim clusters or claim candidates already exist.
- Formal comments and public-platform signals have been normalized into the signal plane.
- Need to know whether an issue is discussed only in formal channels, only in open discourse, or in both.

## Read/Write Contract
- Reads claim clusters, claim candidates, and verification routes from the run-local analysis plane first.
- Reads normalized signals from `run_dir/analytics/signal_plane.sqlite`, treating Regulations.gov rows as formal signals and other public rows as open-platform public signals.
- Falls back to compatible artifact paths when synced result sets are unavailable.
- Writes `run_dir/analytics/formal_public_links_<round_id>.json` by default.
- Syncs the emitted artifact into the run-local analysis plane as `formal-public-link`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `claim_cluster_path`
  - `claim_candidates_path`
  - `verification_route_path`
  - `output_path`
  - `db_path`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `analysis_sync`
- `board_handoff`

## References
- `../../docs/openclaw-next-phase-development-plan.md`
- `../../docs/openclaw-skill-refactor-checklist.md`

## Scripts
- `scripts/eco_link_formal_comments_to_public_discourse.py`
