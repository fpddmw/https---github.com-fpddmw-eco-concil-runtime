---
name: materialize-situation-analysis-brief
description: Materialize a report-facing situation-analysis brief from agent section briefs, theme progress reviews, frozen basis refs, interaction context, and challenger boundaries.
---

## Purpose

Create a `situation-analysis-brief` before narrative report writing. The brief
organizes already-carried material into a bounded analysis spine for the
report-editor.

## Boundary

- Does not fetch, query, schedule, or choose sources.
- Does not create new facts, certify evidence, open rounds, or act as a runtime
  gate.
- May only synthesize material already carried by agent section briefs, frozen
  basis refs, council objects, accepted progress/sufficiency review, interaction
  context, or challenger boundaries.
- Policy evaluation is a report synthesis boundary, not an acquisition lane.

## Required Input

- `run_dir`
- `run_id`
- `round_id`

Optional:

- `basis_round_id`
- `program_id`
- `mission_text`
- `output_path`

## Read/Write Contract

- Reads reporting objects and council objects from the run-local DB.
- Reads optional analysis artifacts under `analytics/`.
- Writes `reporting/situation_analysis_brief_<round_id>.json`.
- Writes one `situation-analysis-brief` reporting object.

## Runtime Policy

- `timeout_seconds`: 30
- `retry_budget`: 0
