---
name: fetch-gdelt-doc-search
description: Query GDELT DOC 2.0 API using explicit query/mode/format/time parameters with retry, throttling, and detailed logs. Use when tasks need topical/domain retrieval, article lists, or timeline aggregates without pulling raw events files.
---

# GDELT DOC Search

## Core Goal
- Execute atomic DOC API searches against `https://api.gdeltproject.org/api/v2/doc/doc`.
- Support topic/domain retrieval via explicit query syntax.
- Support both relative windows (`timespan`) and absolute UTC windows (`STARTDATETIME`, `ENDDATETIME`).
- Return structured JSON envelopes and optionally write raw response bytes.
- Keep runtime observable with structured logs and optional log file.

## Artifact Placement
- In council/runtime runs, write fetched artifacts under the assigned run path, usually `runs/<run-id>/raw/<round-id>/direct-fetch/`, using `--output`, `--output-dir`, and related path flags.
- `./runs/manual-fetch-artifacts/...` examples are for ad-hoc manual probes only.
- Never write fetch outputs to repo-root `data/`.

## Required Environment
- Configure runtime by environment variables (see `references/env.md`).
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
python3 scripts/fetch_gdelt_doc_search.py check-config --pretty
```

2. Run a relative-window DOC search.

```bash
python3 scripts/fetch_gdelt_doc_search.py search \
  --query '("climate change" OR pollution)' \
  --mode artlist \
  --format json \
  --timespan 1day \
  --max-records 50 \
  --pretty
```

3. Run an absolute-window timeline query.

```bash
python3 scripts/fetch_gdelt_doc_search.py search \
  --query '("climate change" OR pollution) sourcecountry:us' \
  --mode timelinevolraw \
  --format json \
  --start-datetime 20260301000000 \
  --end-datetime 20260308000000 \
  --timeline-smooth 5 \
  --pretty
```

4. Run DOC-level media/document tone queries when the evidence need is about
   news/document tone rather than article discovery.

```bash
python3 scripts/fetch_gdelt_doc_search.py search \
  --query '("New York City" OR NYC) smoke wildfire tone<-3' \
  --mode timelinetone \
  --format json \
  --start-datetime 20230605000000 \
  --end-datetime 20230609000000 \
  --timeline-smooth 0 \
  --pretty
```

```bash
python3 scripts/fetch_gdelt_doc_search.py search \
  --query '("New York City" OR NYC) smoke wildfire' \
  --mode tonechart \
  --format json \
  --start-datetime 20230605000000 \
  --end-datetime 20230609000000 \
  --pretty
```

5. Lint a query before a remote call, especially when an agent authored it.

```bash
python3 scripts/fetch_gdelt_doc_search.py lint-query \
  --query 'site:airnow.gov smoke' \
  --pretty
```

6. Search exact official/news domains with GDELT's `domainis:` operator. Do not use `site:`.

```bash
python3 scripts/fetch_gdelt_doc_search.py search \
  --query 'smoke wildfire "New York City"' \
  --domain-is airnow.gov \
  --domain-is epa.gov \
  --mode artlist \
  --format json \
  --start-datetime 20230605000000 \
  --end-datetime 20230609000000 \
  --max-records 50 \
  --output ./runs/manual-fetch-artifacts/gdelt-doc/official-smoke.json \
  --pretty
```

7. Persist raw API payload to a file for downstream tools.

```bash
python3 scripts/fetch_gdelt_doc_search.py search \
  --query '(wildfire OR drought)' \
  --mode artlist \
  --format json \
  --timespan 1week \
  --output ./runs/manual-fetch-artifacts/gdelt-doc/wildfire.json \
  --pretty
```

## Built-in Robustness
- Apply retry with exponential backoff on transient HTTP/network failures.
- Respect `Retry-After` when present on retriable responses.
- Throttle request frequency with a minimum interval between requests.
- Validate query/time parameter combinations before remote calls.
- Lint provider-specific query syntax before remote calls, including blocking unsupported `site:` usage.
- Support repeated `--domain` / `--domain-is` filters by running one query per domain and merging JSON article results.
- Validate DOC constraints (`MAXRECORDS<=250`, `TIMELINESMOOTH<=30`).
- Emit JSON results while writing operational logs to stderr and optional log file.

## Scope Decision
- Keep only DOC API retrieval in this skill.
- Keep atomic operations only; do not add internal scheduler/polling loops.

## Agent Reasoning Guide
- This skill is for GDELT DOC API article lists and timeline-style
  reconnaissance over indexed web documents. It is not the raw
  Events/Mentions/GKG export-table layer.
- DOC Search also supports media/document tone operations: query operators such
  as `tone>5`, `tone<-5`, and `toneabs>10`; modes such as `timelinetone` and
  `tonechart`; and tone sorting such as `sort=toneasc` / `sort=tonedesc`.
  These describe GDELT-indexed document tone, not public response sentiment.
- Normalize DOC `artlist` output as `gdelt_doc_recon`; normalize DOC
  `timelinetone` / `tonechart` output as `gdelt_doc_tone_aggregate`.
- `domain:` and `domainis:` are URL-domain filters. They are useful for exact
  source slices, but they are not official-record categories and should not be
  the only way an agent distinguishes official, media, or community material.
- A zero DOC result can mean the query, date window, domain filter, language, or
  DOC index path was too narrow. It does not mean GDELT Events, Mentions, GKG,
  or non-GDELT sources have no relevant records.
- If DOC output is zero, narrow, or only reconnaissance-level while the evidence
  need remains live, revise/lint the query or use same-family follow-up skills:
  `fetch-gdelt-events`, `fetch-gdelt-mentions`, and `fetch-gdelt-gkg`.
- Avoid turning a provider query failure into a council conclusion. Record the
  query/window/domain limits before saying a public-record route is exhausted.

## References
- `references/env.md`
- `references/gdelt-data-sources.md`
- `references/gdelt-doc-search.md`
- `references/gdelt-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/fetch_gdelt_doc_search.py`

## OpenClaw Invocation Compatibility
- Keep skill trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke in prompts with `$fetch-gdelt-doc-search`.
- Keep the skill atomic: one query execution per invocation.
- Use script parameters for retrieval conditions (`--query`, `--mode`, `--format`, `--timespan` or `--start-datetime/--end-datetime`).
- If you need polling, let OpenClaw agent orchestrate repeated invocations externally (scheduler/loop), not inside this skill.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (config and endpoint check)

```text
Use $fetch-gdelt-doc-search.
Run:
python3 scripts/fetch_gdelt_doc_search.py check-config --pretty
Return only the JSON result.
```

2. Search (relative window)

```text
Use $fetch-gdelt-doc-search.
Run:
python3 scripts/fetch_gdelt_doc_search.py search \
  --query '[QUERY_EXPRESSION]' \
  --mode [MODE] \
  --format json \
  --timespan [TIMESPAN] \
  --max-records [N] \
  --pretty
Return only the JSON result.
```

3. Validate (absolute window and output persistence)

```text
Use $fetch-gdelt-doc-search.
Run:
python3 scripts/fetch_gdelt_doc_search.py search \
  --query '[QUERY_EXPRESSION]' \
  --mode [MODE] \
  --format json \
  --start-datetime [YYYYMMDDHHMMSS] \
  --end-datetime [YYYYMMDDHHMMSS] \
  --output [OUTPUT_FILE] \
  --pretty
Check command exit code and bytes_written > 0.
Return JSON plus one-line pass/fail verdict.
```
