---
name: submit-theme-acquisition-plan
description: Submit an investigator-authored or investigator-adopted acquisition plan for one investigation theme.
---

# Submit Theme Acquisition Plan

## Purpose

Record the responsible investigator's claim-facing acquisition obligations for one investigation theme. The plan states what must be answerable, what evidence shape is required, how denominator/coverage boundaries will be handled, and how report wording must downgrade if the obligation is not met.

It must not name data sources, source families, source skills, query variants, query parameters, or route rankings. Those choices stay inside later investigator acquisition work.

## Read/Write Contract

- Writes `run_dir/runtime/theme-acquisition-plan_<object_id>.json`

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- `author_role`
- `theme_id`
- `claim_slots_supported`
- `evidence_obligations`
- `success_criteria`
- `denominator_obligations`
- `failure_recovery_plan`
- `forbidden_precommitments`
- `downgrade_boundary`
- Optional:
  - `payload_json`

## Outputs

- Dynamic council object `theme-acquisition-plan`

## Agent Reasoning Guide

Use this before acquisition affects a report claim slot. This is not moderator source selection, not source ranking, not query planning, and not evidence sufficiency. It preserves agent autonomy by making obligations and downgrade boundaries explicit while leaving concrete route choice to later acquisition turns.

Do not treat the plan as proof that a route will work or that an untried source is irrelevant. Failed or low-volume attempts still need checkpoint or source-limit reflection before report wording changes.
