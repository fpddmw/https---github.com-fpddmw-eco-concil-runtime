---
name: materialize-acquisition-checkpoints
description: Optional-analysis helper that records lightweight in-round acquisition checkpoints only when acquisition state can affect claim strength, source limits, report downgrade, or recovery choice.
---

# Materialize Acquisition Checkpoints

## Purpose

Create `acquisition-checkpoint` artifacts from public corpus, coverage audit, action cards, source-attempt outcomes, and visible signal counts. Checkpoints are intentionally lightweight and claim-impact scoped.

## Read/Write Contract

- Reads `run_dir/analytics/public_discourse_coverage_audit_<round_id>.json`
- Reads `run_dir/analytics/public_discourse_corpus_<round_id>.json`
- Reads `run_dir/analytics/claim_gap_action_cards_<round_id>.json`
- Reads `run_dir/analytics/fact_policy_public_interaction_timeline_<round_id>.json`
- Writes `run_dir/analytics/acquisition_checkpoints_<round_id>.json`

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `theme_id`

## Outputs

- `run_dir/analytics/acquisition_checkpoints_<round_id>.json`

## Agent Reasoning Guide

This helper is advisory and approval-scoped. It should be explicitly cited before downstream use, and only when the observed state affects claim strength, source-limit rationale, downgrade wording, or a recovery choice.

Do not run it as a form for every tool call. It does not prove claim truth, does not schedule sources, does not rank routes, and does not replace findings, evidence bundles, sufficiency review, or report basis.
