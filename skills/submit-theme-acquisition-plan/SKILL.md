---
name: submit-theme-acquisition-plan
description: Submit an investigator-authored or investigator-adopted acquisition plan for one investigation theme.
---

# Submit Theme Acquisition Plan

## Purpose

Record how the responsible investigator plans to acquire evidence for an investigation theme. The plan may reference action cards or source-family workflows, but the investigator must choose, reject, or rewrite the route.

## Read/Write Contract

- Writes `run_dir/runtime/theme-acquisition-plan_<object_id>.json`

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- `author_role`
- `theme_id`
- `claim_slots_supported`
- `source_family_candidates`
- `query_variant_plan`
- `expected_denominators`
- `failure_recovery_plan`
- `downgrade_boundary`
- Optional:
  - `payload_json`

## Outputs

- Dynamic council object `theme-acquisition-plan`

## Agent Reasoning Guide

Use this before acquisition affects a report claim slot. This is not moderator source selection, not source ranking, and not evidence sufficiency. It preserves agent autonomy by making source, query, and recovery choices explicit before downstream use.

Do not treat the plan as proof that a route will work or that an untried source is irrelevant. Failed or low-volume attempts still need checkpoint or source-limit reflection before report wording changes.
