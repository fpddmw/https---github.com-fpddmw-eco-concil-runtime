---
name: fetch-usbr-rise
description: Discover Bureau of Reclamation RISE catalog item IDs and fetch JSON:API time-series result rows for explicit item IDs, optional location/parameter filters, and bounded date windows. Use when investigators need direct USBR operational or water-environment records such as reservoir elevation, storage, or release data without interpreting shortage severity, compliance, or governance responsibility.
---

# USBR RISE Fetch

## Core Goal
- Discover candidate RISE catalog item IDs from the public `catalog-item`
  endpoint when the investigator has a place/parameter phrase but not a
  grounded item ID yet.
- Fetch RISE `result` records from `https://data.usbr.gov/rise/api`.
- Require explicit `itemId` input for result fetches. If item IDs are missing,
  run `discover-items` first and cite that artifact before proposing a bounded
  result fetch.
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

2. Discover candidate item IDs when the evidence need names a place/parameter
   but no item ID is grounded yet.

```bash
python3 scripts/fetch_usbr_rise.py discover-items \
  --query "Glen Canyon Lake Powell release storage elevation" \
  --max-pages 20 \
  --max-records 25 \
  --output ./runs/manual-fetch-artifacts/usbr-rise-catalog-candidates.json \
  --pretty
```

This produces a discovery artifact with `candidate_item_ids` and catalog item
metadata. Candidate order is provider/page scan order after client-side
filtering; it is not source ranking or evidence weighting.
For broad place names, a first discovery attempt may hit the page cap before
the relevant catalog page. In that case revise terms or rerun with a higher
approved `--max-pages-per-run` / `--max-pages`; do not treat an incomplete
catalog scan as evidence absence.

3. Dry-run an explicit item request.

```bash
python3 scripts/fetch_usbr_rise.py fetch \
  --item-id 10835 \
  --after-utc 2023-01-01T00:00:00Z \
  --before-utc 2023-12-31T23:59:59Z \
  --max-pages 1 \
  --dry-run \
  --pretty
```

4. Fetch and save operational result rows.

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
- `source = usbr-rise-catalog-items` for `discover-items`, with
  `candidate_item_ids`, `records`, `page_summaries`, and `list_semantics`
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
- No item ID is not a reason to decline investigation. Use `discover-items` as
  the same-family route-grounding step, then submit/fetch explicit item IDs if
  the candidate artifact supports a bounded proposal.
- Missing, sparse, or failed rows may reflect wrong `itemId`, date filters, page caps, API availability, metadata fetch failure, or provider latency. It is not proof that USBR operational records are absent.
- If item metadata fetch fails but result rows exist, keep the result artifact and record the metadata limitation; do not convert that limitation into a negative claim.
- Normalize with `$normalize-usbr-rise-environment-signals` before DB-backed environment queries or aggregation.

## Script
- `scripts/fetch_usbr_rise.py`
