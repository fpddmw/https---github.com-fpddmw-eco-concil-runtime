---
name: audit-public-discourse-sample-coverage
description: Optional-analysis helper for auditing public discourse sample coverage across source families without producing representation findings.
---

# Audit Public Discourse Sample Coverage

## Core Goal
- Read normalized public/formal signals and, optionally, an approved public discourse corpus artifact.
- Emit source-family coverage cues, missing-layer warnings, and representativeness limits.
- Avoid representation findings, absence claims, source ranking, or report conclusions.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Optionally reads `public_discourse_corpus_<round_id>.json`
- Writes `run_dir/analytics/public_discourse_coverage_audit_<round_id>.json`
- Defaults to `--round-scope current`; use `--round-scope run` for same-run
  regression checks where public-discourse source rows span multiple rounds.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `round_scope`
- `corpus_path`
- `output_path`

## Agent Reasoning Guide
- Treat coverage cues as human-review prompts. They help identify missing
  source-family layers such as YouTube comments after video discovery, GDELT DOC
  `timelinetone` / `tonechart`, or GDELT Events/Mentions/GKG after DOC recon,
  but they remain advisory and do not prove a concern or narrative is absent.
- Keep `social_sample_affect`, `gdelt_doc_tone_aggregate`, `gdelt_media_tone`,
  formal comments, and physical source attribution separate. This helper audits
  sample shape only.
- A council agent must carry any useful gap cue into an evidence request,
  source-acquisition proposal, challenge, readiness opinion, or synthesis before
  it affects investigation posture.

## Scripts
- `scripts/audit_public_discourse_sample_coverage.py`
