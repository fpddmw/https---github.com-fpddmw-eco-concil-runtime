---
name: materialize-public-discourse-corpus
description: Optional-analysis helper for materializing a DB-backed public/formal discourse text corpus with explicit sample boundaries.
---

# Materialize Public Discourse Corpus

## Core Goal
- Read normalized public/formal text-like signals from `signal_plane.sqlite`.
- Emit a bounded corpus with source family, discourse lane, item refs, and sample limitations.
- Avoid public-opinion inference, source ranking, sentiment judgement, or report conclusions.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Writes `run_dir/analytics/public_discourse_corpus_<round_id>.json`
- Defaults to `--round-scope current`; use `--round-scope run` only when an
  approved regression or continuation review needs same-run rows across rounds.

## Agent Reasoning Guide
- Treat the corpus as approval-scoped advisory/audit material. It defines which
  DB-visible text rows were available under the supplied filters; it does not
  make a finding about public opinion or issue importance.
- YouTube comments and Bluesky posts can support `social_sample_affect` only
  within their sampled platform/query/window. GDELT DOC tone aggregates and
  GDELT Events/Mentions/GKG rows belong to `gdelt_doc_tone_aggregate` or
  `gdelt_media_tone`, not public sentiment.
- Empty or narrow output can reflect missing fetches, unnormalized artifacts,
  source-family gaps, API limits, or filters. A council agent must carry useful
  refs into finding, evidence bundle, challenge, readiness, or synthesis before
  downstream use.

## Scripts
- `scripts/materialize_public_discourse_corpus.py`
