---
name: fetch-gdelt-gkg
description: Fetch GDELT 2.0 GKG export snapshots from lastupdate/masterfilelist with retry, throttling, transport validation, and structure validation. Use when tasks need latest or time-range GKG files (*.gkg.csv.zip) for deterministic ingestion and machine-readable manifests.
---

# GDELT GKG Fetch

## Core Goal
- Fetch GDELT 2.0 `GKG` table exports (`*.gkg.csv.zip`) from official public endpoints.
- Resolve latest available snapshot via `lastupdate.txt`.
- Resolve historical snapshots in a UTC range via `masterfilelist.txt`.
- Persist downloaded files and return machine-readable JSON manifest.
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
python3 scripts/fetch_gdelt_gkg.py check-config --pretty
```

2. Inspect the latest available GKG snapshot.

```bash
python3 scripts/fetch_gdelt_gkg.py resolve-latest --pretty
```

3. Dry-run a historical range selection before downloading.

```bash
python3 scripts/fetch_gdelt_gkg.py fetch \
  --mode range \
  --start-datetime 20260301000000 \
  --end-datetime 20260301120000 \
  --max-files 3 \
  --dry-run \
  --pretty
```

4. Fetch files with transport and structure validation.

```bash
python3 scripts/fetch_gdelt_gkg.py fetch \
  --mode latest \
  --max-files 1 \
  --output-dir ./runs/manual-fetch-artifacts/gdelt-gkg \
  --preview-lines 2 \
  --validate-structure \
  --expected-columns 27 \
  --quarantine-dir ./runs/manual-fetch-artifacts/gdelt-gkg-quarantine \
  --log-level INFO \
  --log-file ./logs/fetch-gdelt-gkg.log \
  --pretty
```

## Built-in Robustness
- Apply retry with exponential backoff on transient HTTP/network failures.
- Respect `Retry-After` when present on retriable responses.
- Throttle request frequency with a minimum interval between requests.
- Enforce `--max-files` safety cap (`GDELT_MAX_FILES_PER_RUN`) to prevent accidental bulk pulls.
- Validate datetime format and range boundaries before remote calls.
- Validate transport and structure after download:
  - ZIP CRC/integrity check
  - UTF-8 strict decoding check
  - Tab column-count check (default 27)
  - Optional bad-line issue quarantine (`--quarantine-dir`)
- Emit JSON results while writing operational logs to stderr and optional log file.

## Scope Decision
- Keep one concrete file-table fetch implementation: `GKG` (`*.gkg.csv.zip`).
- Keep atomic operations only; do not add internal scheduler/polling loops.

## Agent Reasoning Guide
- This skill retrieves GDELT GKG export files for a UTC snapshot range. It is a
  row-level knowledge-graph surface, not the DOC article API and not an issue or
  report-conclusion generator.
- Use it after a scoped time window exists, after DOC reconnaissance, or when DOC
  query sensitivity/caps make article search insufficient for discovering
  topical public signals.
- Dry-run historical ranges and keep `--max-files` bounded. A failed or sparse
  pull is an acquisition/window/cap limitation, not proof that no topical public
  signal exists.
- Pair this skill with `normalize-gdelt-gkg-public-signals` before relying on
  DB-backed public-signal queries.

## References
- `references/gdelt-data-sources.md`
- `references/gdelt-limitations.md`
- `references/gdelt-schema.md`
- `references/env.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/fetch_gdelt_gkg.py`

## OpenClaw Invocation Compatibility
- Keep skill trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke in prompts with `$fetch-gdelt-gkg`.
- Keep the skill atomic: only resolve/fetch on demand.
- Use script parameters for fetch conditions (`--mode range --start-datetime --end-datetime`).
- If you need polling, let OpenClaw orchestrate repeated invocations externally, not inside this skill.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (latest availability)

```text
Use $fetch-gdelt-gkg.
Run:
python3 scripts/fetch_gdelt_gkg.py resolve-latest --pretty
Return only the JSON result.
```

2. Fetch (historical window, dry-run first)

```text
Use $fetch-gdelt-gkg.
Run:
python3 scripts/fetch_gdelt_gkg.py fetch \
  --mode range \
  --start-datetime [YYYYMMDDHHMMSS] \
  --end-datetime [YYYYMMDDHHMMSS] \
  --max-files [N] \
  --dry-run \
  --pretty

Then run without --dry-run using:
  --output-dir [OUTPUT_DIR]
  --validate-structure
  --expected-columns 27
  --quarantine-dir [QUARANTINE_DIR]
Return only the JSON result.
```

3. Validate (download quality gate)

```text
Use $fetch-gdelt-gkg.
Run:
python3 scripts/fetch_gdelt_gkg.py fetch \
  --mode latest \
  --max-files 1 \
  --output-dir [OUTPUT_DIR] \
  --validate-structure \
  --expected-columns 27 \
  --quarantine-dir [QUARANTINE_DIR] \
  --pretty
Check validation.issue_count, decode_error_count, column_mismatch_count.
Return JSON plus one-line pass/fail verdict.
```
