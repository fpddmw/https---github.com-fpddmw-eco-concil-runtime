---
name: fetch-epa-eis-records
description: Fetch official EPA EIS Database result tables from common-search pages or explicit search URLs and emit governance-record metadata. Use when investigators need NEPA/EIS records such as title, CEQ number, document type, Federal Register date, lead agency, state, detail URL, and document availability cues without judging EIS adequacy or policy responsibility.
---

# EPA EIS Records Fetch

## Core Goal
- Fetch EPA EIS Database HTML result tables from the official EIS Database.
- Support official common-search pages such as open-comment EISs and recently published EISs.
- Support explicit `--search-url` when an operator or investigator has already created a specific EIS Database search URL in the browser.
- Emit `official-governance-record-fetch-v1` records that can be normalized by `$normalize-official-governance-records`.

## Artifact Placement
- In runtime runs, write artifacts under the assigned run path, usually `runs/<run-id>/raw/<round-id>/`.
- Use `--output` when the source queue expects direct-file capture.
- Do not write fetch outputs to repo-root `data/`.

## Workflow
1. Validate runtime settings.

```bash
python3 scripts/fetch_epa_eis_records.py check-config --pretty
```

2. Dry-run a common official search page.

```bash
python3 scripts/fetch_epa_eis_records.py fetch \
  --common-search openComment \
  --dry-run \
  --pretty
```

3. Fetch and save EIS records.

```bash
python3 scripts/fetch_epa_eis_records.py fetch \
  --common-search last30Published \
  --max-records 100 \
  --output ./runs/manual-fetch-artifacts/epa-eis-last30.json \
  --pretty
```

## Output Contract
- `schema_version = official-governance-record-fetch-v1`
- `source_skill = fetch-epa-eis-records`
- `source_parameters`
- `query_parameters`
- `records`
- `page_summaries`
- `warnings`
- `provenance`
- `artifact_refs` when `--output` is used

## Agent Reasoning Guide
- This skill parses official EPA EIS Database result rows. It does not decide EIS adequacy, legal sufficiency, policy responsibility, environmental effects, or report conclusions.
- Empty or sparse output may reflect the selected common-search page, a stale explicit search URL, provider HTML changes, pagination, or the database 500-record result surface. It is not proof that no EIS records exist.
- If a precise historical/project query is needed, create the search in the EPA EIS Database UI and pass the resulting official search URL with `--search-url`.
- Normalize with `$normalize-official-governance-records` before using the records through DB-backed formal-signal queries.

## Script
- `scripts/fetch_epa_eis_records.py`
