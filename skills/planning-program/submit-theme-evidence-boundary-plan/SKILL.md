---
name: submit-theme-evidence-boundary-plan
description: Submit an investigator-authored or investigator-adopted evidence boundary plan for one investigation theme.
---

# Submit Theme Evidence Boundary Plan

## Purpose

Record the responsible investigator's claim-facing evidence obligations for one investigation theme. The plan states what must be answerable, what evidence shape is required, how denominator/coverage boundaries will be handled, and how report wording must downgrade if the obligation is not met.

It must not name data sources, source families, source skills, query variants, query parameters, or route rankings. Those choices stay inside later investigator acquisition work.

## Read/Write Contract

- Writes `run_dir/runtime/theme-evidence-boundary-plan_<object_id>.json`

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- `author_role`
- `theme_id`
- `authoring_mode` (`agent-authored` or `agent-adopted`)
- `sample_unit`
- `target_kind`
- `target_id`
- `claim_slots_supported`
- `evidence_obligations`
- `success_criteria`
- `denominator_obligations`
- `failure_recovery_plan`
- `forbidden_precommitments`
- `downgrade_boundary`
- Optional:
  - `payload_json`
  - `temporal_scope`
  - `time_window` in `payload_json`

The canonical object includes a structured `time_window` dictionary. If
`payload_json.time_window` is omitted, runtime derives a non-claiming
`time_window` from `temporal_scope`; if both are absent, runtime records an
explicit "not specified by author" time-window placeholder. The placeholder is
only schema metadata and must not be treated as evidence coverage.

When invoking through a shell, use a compact identifier such as
`official/governance-record` for `sample_unit`, or quote the full value. Unquoted
multi-word values are parsed as separate CLI arguments and will be rejected
before any council object is created.

Example:

```bash
python3 eco-concil-runtime/scripts/eco_runtime_kernel.py run-skill \
  --run-dir "$RUN_DIR" --run-id "$RUN_ID" --round-id "$ROUND_ID" \
  --skill-name submit-theme-evidence-boundary-plan \
  --actor-role social-investigator --contract-mode warn -- \
  --author-role social-investigator \
  --authoring-mode agent-authored \
  --target-kind investigation-theme \
  --target-id theme-official-policy-action \
  --theme-id theme-official-policy-action \
  --sample-unit official/governance-record \
  --temporal-scope "Acute June 2023 NYC smoke episode" \
  --claim-slot-supported official-action-record-presence \
  --evidence-obligation "Record issuer, date, venue, and wording scope." \
  --success-criterion "Supports only descriptive official-record claims." \
  --denominator-obligation "Denominator is acquired visible records only." \
  --failure-recovery-plan "Downgrade to route-specific coverage limitation." \
  --forbidden-precommitment "Do not infer public uptake or effectiveness." \
  --downgrade-boundary "No completeness/effectiveness claim from this plan alone." \
  --rationale "Boundary plan authored by the responsible investigator."
```

## Outputs

- Dynamic council object `theme-evidence-boundary-plan`

## Agent Reasoning Guide

Use this before acquisition affects a report claim slot. This is not moderator source selection, not source ranking, not query planning, and not evidence sufficiency. It preserves agent autonomy by making obligations and downgrade boundaries explicit while leaving concrete route choice to later acquisition turns.

Do not treat the plan as proof that a route will work or that an untried source is irrelevant. Failed or low-volume attempts still need checkpoint or source-limit reflection before report wording changes.
