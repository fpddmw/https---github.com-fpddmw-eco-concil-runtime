---
name: draft-agent-section-brief
description: Draft and store an agent-authored section brief for report-editor consumption.
---

# Draft Agent Section Brief

## Purpose

Create a DB-backed `agent-section-brief` with claims, refs, source families, claim strength, denominators, limitations, recommended report use, and blocked phrases. This lets investigators and moderator contribute report-ready boundaries before synthesis.

The brief also carries program-aware context when available: mission question,
current round question, active themes, role responsibility boundaries,
theme-progress advisory dispositions, and round exit criteria. These fields are
handoff context only. They do not choose sources, skills, queries, route
rankings, scheduler queues, or automatic execution.

## Read/Write Contract

- Reads `run_dir/analytics/theme_sufficiency_review_<round_id>.json`
- Reads DB `council-investigation-program` when `program_id` is supplied or a
  program is visible for the run.
- Writes `run_dir/reporting/agent_section_brief_<agent_role>_<section_key>_<round_id>.json`

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- `agent_role`
- Optional:
  - `section_key`
  - `sufficiency_review_path`

## Outputs

- `run_dir/reporting/agent_section_brief_<agent_role>_<section_key>_<round_id>.json`
- Reporting DB object `agent-section-brief`

## Agent Reasoning Guide

Use this after theme sufficiency review or equivalent council uptake. The brief is not a new investigation, not a replacement for frozen basis, and not a license for report-editor to add unsupported claims.

Report-editor may consume it before downstream report drafting only with its limitations, denominators, blocked phrases, and evidence refs intact.

Do not write `agent_responsibility_boundaries` as a mechanical task list. They
must remain role/report responsibility boundaries: what claim basis, denominator,
limitation, or review duty this role is carrying into reporting.
