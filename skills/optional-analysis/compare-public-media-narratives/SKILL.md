---
name: compare-public-media-narratives
description: Optional-analysis helper for comparing sampled social affect, GDELT media tone, formal comments, and source narratives without producing alignment or attribution conclusions.
---

# Compare Public Media Narratives

## Core Goal
- Read a DB-backed public discourse corpus and, optionally, an approved annotation aggregation artifact.
- Compare sampled discourse lanes such as social sample affect, GDELT media tone, formal comments, and source narrative labels.
- Avoid alignment scoring, public-opinion inference, physical source attribution, or report conclusions.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Optionally reads `public_discourse_corpus_<round_id>.json`
- Optionally reads `public_discourse_annotation_aggregation_<round_id>.json`
- Writes `run_dir/analytics/public_media_narrative_comparison_<round_id>.json`
- Defaults to `--round-scope current`; use `--round-scope run` only for
  approved same-run comparisons across rounds.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `round_scope`
- `source_round_id` (use when no corpus artifact is supplied and the analysis reads a specific prior acquisition round)
- `corpus_path`
- `aggregation_path`
- `output_path`

## Agent Reasoning Guide
- Treat cross-source cues as advisory review material. Similar labels across
  sampled lanes are not proof that a narrative is true, representative, or
  physically attributable.
- Keep GDELT DOC tone aggregates and GDELT row-level media/document tone
  separate from social sample affect and formal comment samples. This helper can
  expose contrasts and gaps, but it cannot decide source truth or public
  opinion.
- A council agent must explicitly cite useful cues in a finding, evidence
  bundle, challenge, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/compare_public_media_narratives.py`
