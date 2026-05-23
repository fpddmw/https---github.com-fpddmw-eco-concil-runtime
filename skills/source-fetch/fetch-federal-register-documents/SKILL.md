---
name: fetch-federal-register-documents
description: Fetch FederalRegister.gov published-document metadata from the official API by term, agency, document type, and publication-date filters. Use when investigators need official governance or rulemaking records as raw artifacts for formal-plane normalization without deriving policy meaning, stance, or evidence weight.
---

# Federal Register Documents Fetch

## Core Goal
- Fetch `documents.json` records from the FederalRegister.gov API.
- Filter by search term, agency slug, document type, and publication date.
- Return one compact `official-governance-record-fetch-v1` artifact with provider metadata and provenance.
- Keep the skill limited to raw acquisition; downstream meaning belongs to council objects and normalizers.

## Artifact Placement
- In runtime runs, write artifacts under the assigned run path, usually `runs/<run-id>/raw/<round-id>/`.
- Use `--output` when the source queue expects direct-file capture.
- Do not write fetch outputs to repo-root `data/`.

## Workflow
1. Validate configuration.

```bash
python3 scripts/fetch_federal_register_documents.py check-config --pretty
```

2. Dry-run a scoped request.

```bash
python3 scripts/fetch_federal_register_documents.py fetch \
  --term "Colorado River" \
  --agency "reclamation-bureau" \
  --publication-date-gte 2023-01-01 \
  --publication-date-lte 2023-12-31 \
  --max-pages 1 \
  --dry-run \
  --pretty
```

3. Fetch and save the raw artifact.

```bash
python3 scripts/fetch_federal_register_documents.py fetch \
  --term "Colorado River" \
  --agency "reclamation-bureau" \
  --publication-date-gte 2023-01-01 \
  --publication-date-lte 2023-12-31 \
  --max-pages 2 \
  --max-records 100 \
  --output ./runs/manual-fetch-artifacts/federal-register-colorado.json \
  --pretty
```

## Output Contract
- `schema_version = official-governance-record-fetch-v1`
- `source_skill = fetch-federal-register-documents`
- `source_parameters`
- `query_parameters`
- `records`
- `page_summaries`
- `warnings`
- `provenance`
- `artifact_refs` when `--output` is used

## Agent Reasoning Guide
- This skill fetches official published-document metadata only. It does not classify legal significance, policy stance, public sentiment, source sufficiency, or report conclusions.
- Use this skill when the evidence question is about federal publication,
  federal rules/notices, agency notices, or executive documents. Do not use it
  as the default route for local or state episode response records such as NYC
  advisories, school operational decisions, transit alerts, or health guidance;
  those need an official-domain/page route or a clearly bounded reconnaissance
  route.
- Zero rows may reflect term choice, agency slug, publication date, document type, page caps, or Federal Register indexing limits. It is not proof that official records are absent.
- If the route remains live, revise the term/window/agency or use another official project-record source before recording a source-limit rationale.
- Normalize with `$normalize-official-governance-records` before using the records through DB-backed formal-signal queries.

## Script
- `scripts/fetch_federal_register_documents.py`
