---
name: materialize-research-issue-surface
description: Optional-analysis helper for DB-backed research issue surfaces. It emits candidate issue records for human review, not controversy conclusions.
---

# Materialize Research Issue Surface

## Core Goal
- Convert DB public/formal signals or approved hints into candidate research issue surfaces.
- Preserve evidence refs, lineage, provenance, and optional-analysis governance metadata.
- Keep issue records appendix/audit only until DB basis objects cite them.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite` and optional approved input artifact.
- Writes `run_dir/analytics/research_issue_surface_<round_id>.json`

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `input_path`
- `output_path`
- `limit`

## Agent Reasoning Guide
- Treat output as approval-scoped advisory/audit material. Candidate issue
  records are review surfaces, not controversy conclusions, claim candidates, or
  report prose.
- Empty or sparse surfaces can reflect missing normalized rows, filters, source
  coverage, or upstream issue-hint scope. They do not prove that research issues
  are absent.
- A council agent must carry useful issue candidates into a finding, evidence
  bundle, challenge, proposal, readiness opinion, or synthesis before downstream
  use.

## Scripts
- `scripts/materialize_research_issue_surface.py`
