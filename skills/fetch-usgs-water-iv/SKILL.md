---
name: fetch-usgs-water-iv
description: Fetch USGS Water Services Instantaneous Values JSON for one bounding box or explicit site list, then return structured hydrology observations with site metadata, parameter codes, timestamps, qualifiers, and validation output. Use when tasks need deterministic station-based streamflow or gage-height evidence from USGS for flood, runoff, river, or water-level verification in the United States.
---

# USGS Water IV Fetch

## Core Goal
- Fetch USGS Water Services `iv` data for one bounding box or explicit site list.
- Keep the call atomic: one request, one structured JSON payload, no follow-up site-discovery chain required.
- Return site metadata plus flattened time-series records that downstream normalization can map into hydrology observations.
- Keep runtime deterministic with retries, throttling, response-size caps, and validation summaries.

## Repository Policy
- This is the canonical USGS station-hydrology skill in this repository.
- When eco-council or OpenClaw assigns a raw artifact path, write this skill's full JSON payload to that exact path with `--output`.
- Do not treat dry-run output as collected evidence.

## Artifact Placement
- In council/runtime runs, write fetched artifacts under the assigned run path, usually `runs/<run-id>/raw/<round-id>/direct-fetch/`, using `--output`, `--output-dir`, and related path flags.
- `./runs/manual-fetch-artifacts/...` examples are for ad-hoc manual probes only.
- Never write fetch outputs to repo-root `data/`.

## Required Environment
- Configure runtime by environment variables in `references/env.md`.
- Start from `assets/config.example.env`.
- Load env values before running commands:

```bash
set -a
source assets/config.example.env
set +a
```

## Workflow
1. Validate effective configuration.

```bash
python3 scripts/fetch_usgs_water_iv.py check-config --pretty
```

2. Dry-run the query plan first.

```bash
python3 scripts/fetch_usgs_water_iv.py fetch \
  --bbox=-77.3,38.8,-77.0,39.1 \
  --period P1D \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --dry-run \
  --pretty
```

3. Run one time-window fetch and write the payload.

```bash
python3 scripts/fetch_usgs_water_iv.py fetch \
  --bbox=-77.3,38.8,-77.0,39.1 \
  --start-datetime 2026-03-21T00:00:00Z \
  --end-datetime 2026-03-22T23:59:59Z \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --output ./runs/manual-fetch-artifacts/usgs-water-iv.json \
  --pretty
```

4. Optional explicit-site fetch when the mission already knows site numbers.

```bash
python3 scripts/fetch_usgs_water_iv.py fetch \
  --site 01646500 \
  --site 01646000 \
  --period P1D \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --output ./runs/manual-fetch-artifacts/usgs-water-iv-sites.json \
  --pretty
```

## Output Record Shape
Each item in `records` is one site-parameter-timestamp observation with fields:
- `site_number`, `site_name`, `agency_code`
- `site_type`, `state_code`, `county_code`, `huc_code`
- `latitude`, `longitude`
- `parameter_code`, `variable_name`, `variable_description`, `unit`
- `observed_at_utc`, `value`
- `qualifiers`, `provisional`
- `source_query_url`

## Scope Boundaries
- This skill targets the USGS Water Services `Instantaneous Values` endpoint only.
- This skill does not infer flood thresholds, return periods, or policy meaning.
- This skill does not geocode place names.
- This skill does not need an API key.
- This skill supports `file://` base URLs for deterministic local fixture testing.

## Agent Reasoning Guide
- Use this skill for USGS station-based instantaneous hydrology observations
  inside one bbox or explicit site list. It observes available USGS-served
  sites and requested parameter codes, not all rivers or all flood conditions.
- Keep the major filter explicit: bbox and site-list modes answer different
  acquisition questions. Use site lists when the relevant gauge is already
  known.
- A zero or sparse result can reflect bbox size, site status, parameter
  availability, time range, provisional data limits, or lack of USGS coverage.
  Do not treat it as proof that streamflow or flood impacts were absent.
- The upstream Water Services platform is scheduled for decommissioning in early
  2027. Treat endpoint failures or migration warnings as provider-interface
  issues before converting them into council evidence limits.

## References
- `references/env.md`
- `references/usgs-water-iv-api-notes.md`
- `references/usgs-water-iv-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/fetch_usgs_water_iv.py`
