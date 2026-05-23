---
name: materialize-report-blueprint
description: Materialize a mission-driven report blueprint and investigation themes without fetching data, choosing source routes, or writing conclusions.
---

# Materialize Report Blueprint

## Purpose

Create a report-framing artifact and DB-backed `report-blueprint`,
`report-outcome-contract`, and `investigation-theme` objects from the mission.
The output defines report questions, required theme reports, quality gates,
claim slots, and theme boundaries only.

The blueprint is not complete council framing by itself. Before formal acquisition starts, relevant roles should adopt, narrow, or challenge the split with council objects such as `agent-position`.

## Read/Write Contract

- Reads `run_dir/mission.json`
- Writes `run_dir/reporting/report_blueprint_<round_id>.json`

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `mission_text`
  - `author_role`

## Outputs

- `run_dir/reporting/report_blueprint_<round_id>.json`
- Dynamic council objects for `report-blueprint`, `report-outcome-contract`,
  and `investigation-theme`

## Agent Reasoning Guide

Use this at the beginning of report-driven investigation. The blueprint and
outcome contract are not a source plan, not a fixed topic template, and not a
conclusion list. They should be cited before downstream use as framing context
only.

Do not add source family choices, query strings, skill routes, source ranking, or policy evaluation findings here. Downstream theme plans should state obligations and downgrade boundaries, not route precommitments.
