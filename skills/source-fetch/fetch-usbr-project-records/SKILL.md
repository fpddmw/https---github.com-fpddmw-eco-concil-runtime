---
name: fetch-usbr-project-records
description: Fetch Bureau of Reclamation project pages and same-domain linked official records from supplied URLs. Use when investigators already have an official USBR project surface and need a raw governance-record inventory for formal-plane normalization without page-ranking, legal interpretation, or conclusions.
---

# USBR Project Records Fetch

## Core Goal
- Fetch supplied Bureau of Reclamation project URLs.
- Extract page title, description, response metadata, and same-domain linked document URLs.
- Return one compact `official-governance-record-fetch-v1` artifact.
- Avoid pretending USBR has one uniform project-record search API; this skill is direct URL acquisition.

## Artifact Placement
- In runtime runs, write artifacts under the assigned run path, usually `runs/<run-id>/raw/<round-id>/`.
- Use `--output` when the source queue expects direct-file capture.
- Do not write fetch outputs to repo-root `data/`.

## Workflow
1. Validate runtime settings.

```bash
python3 scripts/fetch_usbr_project_records.py check-config --pretty
```

2. Dry-run the URL list.

```bash
python3 scripts/fetch_usbr_project_records.py fetch \
  --url "https://www.usbr.gov/ColoradoRiverBasin/interimguidelines/seis/publicinvolvement.html" \
  --max-linked-records 25 \
  --dry-run \
  --pretty
```

3. Fetch and save the artifact.

```bash
python3 scripts/fetch_usbr_project_records.py fetch \
  --url "https://www.usbr.gov/ColoradoRiverBasin/interimguidelines/seis/publicinvolvement.html" \
  --max-linked-records 25 \
  --output ./runs/manual-fetch-artifacts/usbr-colorado-project-records.json \
  --pretty
```

## Output Contract
- `schema_version = official-governance-record-fetch-v1`
- `source_skill = fetch-usbr-project-records`
- `source_parameters`
- `query_parameters`
- `records`
- `page_summaries`
- `warnings`
- `provenance`
- `artifact_refs` when `--output` is used

## Agent Reasoning Guide
- This skill inventories supplied official project pages and links. It does not search the full USBR site, rank documents, decide completeness, classify policy meaning, or make report conclusions.
- A page with few links can reflect URL choice, page design, extraction limits, or same-domain filtering. It is not proof that official records are absent.
- If the evidence need remains live, revise the supplied URL, add related official project URLs, or record a source-limit rationale with the exact fetched URL list.
- Normalize with `$normalize-official-governance-records` before using the records through DB-backed formal-signal queries.

## Script
- `scripts/fetch_usbr_project_records.py`
