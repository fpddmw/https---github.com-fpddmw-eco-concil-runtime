---
name: summarize-public-discourse-sample
description: Optional-analysis helper for summarizing a public discourse sample from approved corpus, coverage, annotation, and comparison artifacts without writing deliberation conclusions.
---

# Summarize Public Discourse Sample

## Core Goal
- Read DB-backed public/formal signals and approved public discourse helper artifacts.
- Emit a compact sample summary with sample definition, distributions, media tone, example refs, warnings, and board handoff.
- Avoid report-ready conclusions, public-opinion inference, source attribution, source ranking, or phase-gate posture.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`
- Optionally reads approved public discourse corpus, coverage audit, annotation aggregation, and public/media narrative comparison artifacts
- Writes `run_dir/analytics/public_discourse_sample_summary_<round_id>.json`
- Defaults to `--round-scope current`; use `--round-scope run` only when the
  council-approved sample intentionally spans multiple rounds in one run.

## Output Contract
- Always carries `sample_definition`, `sample_count`, `source_family_counts`,
  `source_skill_counts`, `discourse_lane_counts`, `warnings`, and `evidence_refs`.
- Carries annotation distributions when an approved aggregation artifact is
  supplied: `issue_distribution`, `social_affect_distribution`,
  `source_narrative_distribution`, `actor_responsibility_distribution`, and
  `action_orientation_distribution`.
- Carries `distribution_use_policy` so report writers preserve the sample-local
  boundary: labels may be non-exclusive, sample fractions are not population
  estimates, GDELT tone is media/document tone, and source-narrative labels are
  cues for environmental verification rather than physical attribution.
- Carries `sample_internal_distribution`, `what_this_sample_can_support`,
  `what_this_sample_cannot_support`, `recommended_report_language`, and
  `forbidden_report_language` so report-editor can use bounded wording without
  turning helper output into a conclusion.
- The summary is advisory helper output until a council object explicitly cites it.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `round_scope`
- `source_round_id` (use when no corpus artifact is supplied and the analysis reads a specific prior acquisition round)
- `corpus_path`
- `coverage_audit_path`
- `aggregation_path`
- `comparison_path`
- `output_path`

## Agent Reasoning Guide
- Treat the summary as advisory handoff material. It assembles approved helper
  outputs and DB refs, but it does not create findings, evidence bundles,
  readiness decisions, or report-basis objects.
- Keep sample boundaries visible: YouTube/Bluesky affect is sample-local, GDELT
  DOC tone aggregates and row-level tone are media/document tone, formal
  comments are policy-record samples, and physical source attribution belongs to
  the environmental evidence lane.
- A council agent must cite the returned refs in a finding, evidence bundle,
  challenge, readiness opinion, or synthesis before downstream use.

## Scripts
- `scripts/summarize_public_discourse_sample.py`
