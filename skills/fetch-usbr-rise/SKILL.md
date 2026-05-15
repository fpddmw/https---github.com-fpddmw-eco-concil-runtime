---
name: fetch-usbr-rise
description: Fetch Bureau of Reclamation RISE JSON:API time-series result rows for explicit item IDs, optional location/parameter filters, and bounded date windows. Use when investigators need direct USBR operational or water-environment records such as reservoir elevation, storage, or release data without interpreting shortage severity, compliance, or governance responsibility.
---

# USBR RISE Fetch

## Core Goal
- Fetch RISE `result` records from `https://data.usbr.gov/rise/api`.
- Require explicit `itemId` input rather than automatic catalog search.
- Support bounded result fetches with `locationId`, `parameterId`, `dateTime` filters, page limits, and output caps.
- Optionally fetch item metadata or accept operator-supplied metadata overrides for title, location, parameter, unit, and coordinates.

## Artifact Placement
- In runtime runs, write artifacts under the assigned run path, usually `runs/<run-id>/raw/<round-id>/`.
- Use `--output` when the source queue expects direct-file capture.
- Do not write fetch outputs to repo-root `data/`.

## Workflow
1. Validate runtime settings.

```bash
python3 scripts/fetch_usbr_rise.py check-config --pretty
```

2. Dry-run an explicit item request.

```bash
python3 scripts/fetch_usbr_rise.py fetch \
  --item-id 10835 \
  --after-utc 2023-01-01T00:00:00Z \
  --before-utc 2023-12-31T23:59:59Z \
  --max-pages 1 \
  --dry-run \
  --pretty
```

3. Fetch and save operational result rows.

```bash
python3 scripts/fetch_usbr_rise.py fetch \
  --item-id 10835 \
  --after-utc 2023-01-01T00:00:00Z \
  --before-utc 2023-12-31T23:59:59Z \
  --max-pages 2 \
  --max-records 500 \
  --include-item-metadata \
  --output ./runs/manual-fetch-artifacts/usbr-rise-results.json \
  --pretty
```

## Output Contract
- `schema_version = fetch-usbr-rise-v1`
- `source_skill = fetch-usbr-rise`
- `source_parameters`
- `query_parameters`
- `item_metadata`
- `records`
- `page_summaries`
- `warnings`
- `provenance`
- `artifact_refs` when `--output` is used

## Agent Reasoning Guide
- This skill fetches direct RISE operational/environment rows. It does not decide shortage severity, operating compliance, governance responsibility, attribution, or report readiness.
- Missing, sparse, or failed rows may reflect wrong `itemId`, date filters, page caps, API availability, metadata fetch failure, or provider latency. It is not proof that USBR operational records are absent.
- If item metadata fetch fails but result rows exist, keep the result artifact and record the metadata limitation; do not convert that limitation into a negative claim.
- Normalize with `$normalize-usbr-rise-environment-signals` before DB-backed environment queries or aggregation.

## Script
- `scripts/fetch_usbr_rise.py`
