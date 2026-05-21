---
name: build-fact-policy-public-interaction-timeline
description: Optional-analysis helper that aligns fact/policy-side and public/media-side evidence refs into a descriptive interaction timeline without causality, response-attribution, ranking, or scheduling.
---

# Build Fact Policy Public Interaction Timeline

## Core Goal
- Read DB-backed environment, formal, and public signal timestamps for one run/round.
- Read available public discourse helper artifacts as advisory denominator and semantic context.
- First synthesize date-scoped `lane_episode_cards` for fact, policy, and public/media lanes.
- Emit interaction nodes only when a date bucket has fact/policy-side lane episodes and public/media-side lane episodes.
- Emit one-sided chronology nodes as limitations, not interaction claims.
- Avoid causality, policy-impact, response-attribution, representative-public, source-ranking, or execution recommendations.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Reads optional helper artifacts from `run_dir/analytics/public_discourse_*_<round_id>.json`
- Writes `run_dir/analytics/fact_policy_public_interaction_timeline_<round_id>.json`
- Writes `lane_episode_cards`, `interaction_nodes`, one-sided context nodes, semantic-shift candidates, and boundary metadata.
- Syncs `interaction_nodes` into the analysis plane as `fact-policy-public-interaction-node`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `output_path`
- `max_nodes`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory context. A timeline node is chronology
  with refs, not a finding about why public/media discourse changed.
- Each interaction node must keep fact/policy-side refs separate from
  public/media-side refs before downstream use.
- Empty or one-sided output is visibility context only and does not prove that no
  interaction, public response, official action, or source evidence exists.
- Timeline nodes must be composed from lane episode cards, not raw same-date signal co-visibility alone.
- A council agent or report editor must explicitly carry useful nodes into a
  finding, evidence bundle, round synthesis, report section draft, or reporting
  basis before downstream use.

## Scripts
- `scripts/build_fact_policy_public_interaction_timeline.py`
