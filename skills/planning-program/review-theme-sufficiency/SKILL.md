---
name: review-theme-sufficiency
description: Optional-analysis helper that states which theme claim slots are supported, unsupported, denominator-valid, source-limited, or downgrade-required without becoming a runtime gate.
---

# Review Theme Sufficiency

## Purpose

Materialize `theme-sufficiency-review` artifacts for report-chain handoff. The review states support and downgrade boundaries for claim slots; it does not decide truth, readiness, or final adoption.

## Read/Write Contract

- Reads `run_dir/reporting/report_blueprint_<round_id>.json`
- Reads `run_dir/analytics/acquisition_checkpoints_<round_id>.json`
- Reads `run_dir/analytics/public_discourse_coverage_audit_<round_id>.json`
- Reads `run_dir/analytics/public_discourse_corpus_<round_id>.json`
- Reads `run_dir/analytics/fact_policy_public_interaction_timeline_<round_id>.json`
- Writes `run_dir/analytics/theme_sufficiency_review_<round_id>.json`

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `theme_id`
  - `checkpoint_path`

## Outputs

- `run_dir/analytics/theme_sufficiency_review_<round_id>.json`

## Agent Reasoning Guide

This helper is advisory and must be explicitly cited before downstream use. It helps agents and report-editor see supported claim slots, unsupported claim slots, valid denominators, source-family limits, and required downgrades.

Do not treat the review as a phase gate, score, automatic report-ready decision, or evidence truth mechanism. Council objects, section briefs, frozen basis, and report basis still carry final uptake.

Theme progress review may summarize evidence planes, basis refs, denominator
status, recovery options, and advisory disposition. It must not expose source
skill counts, query variants, route ranking, scheduler queues, or auto-execute
fields as progress-review output.

When a theme progress review recommends `needs-supplemental-round`, the skill
may emit a supplemental transition payload suggestion and request template. This
is advisory context only: it does not open a round, does not authorize report
use, and still requires moderator transition request plus runtime-operator
approval before `open-investigation-round` can carry the context forward.
