---
name: audit-formal-comment-candidate-corpus
description: Optional-analysis helper for auditing a Regulations.gov formal-comment candidate corpus against docket, document, and keyword constraints without judging stance, importance, or evidence sufficiency.
---

# Audit Formal Comment Candidate Corpus

## Core Goal
- Read a Regulations.gov comment-list artifact or DB-backed formal comment listing rows.
- Emit candidate-corpus coverage counts, field gaps, drift cues, duplicate or mass-campaign cues, and bounded samples.
- Avoid stance labels, importance judgements, source ranking, report conclusions, or continuation decisions.

## Read/Write Contract
- Reads a `fetch-regulationsgov-comments` JSON/JSONL artifact and/or `run_dir/analytics/signal_plane.sqlite`.
- Writes `run_dir/analytics/formal_comment_candidate_corpus_audit_<round_id>.json`.
- Defaults to `--round-scope current`; use `--round-scope up-to-current` when a continuation round intentionally relies on prior candidate rows.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `artifact_path`
- `docket_id`
- `comment_on_document_id`
- `agency_id`
- `keyword`
- `round_scope`
- `output_path`
- `sample_ref_limit`

## Agent Reasoning Guide
- Treat this as a sample-shape audit for formal comment candidates. It helps decide whether a list result is coherent enough for batch detail, but it does not decide whether investigation should close.
- Treat the output as advisory helper material, not a council finding or report basis.
- `likely_drift_indicators` are cues such as missing docket fields, wrong agency, commentOn mismatch, or title/body keyword miss. They are not source scores.
- Candidate counts are corpus-local. They must not be written as formal comment stance distribution or public opinion distribution.
- A council agent must carry useful audit cues into an evidence request, source-acquisition proposal, challenge, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/audit_formal_comment_candidate_corpus.py`
