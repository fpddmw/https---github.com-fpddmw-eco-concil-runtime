---
name: materialize-report-blueprint
description: Materialize a mission-driven report blueprint and investigation themes without fetching data, choosing source routes, or writing conclusions.
---

# Materialize Report Blueprint

## Purpose

Create a report-framing artifact and DB-backed `report-blueprint` / `investigation-theme` objects from the mission. The output defines report questions, claim slots, and theme boundaries only.

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
- Dynamic council objects for `report-blueprint` and `investigation-theme`

## Agent Reasoning Guide

Use this at the beginning of report-driven investigation. The blueprint is not a source plan, not a fixed topic template, and not a conclusion list. It should be cited before downstream use as framing context only.

Do not add source family choices, query strings, skill routes, source ranking, or policy evaluation findings here. Investigators must author or explicitly adopt their own acquisition plans.
