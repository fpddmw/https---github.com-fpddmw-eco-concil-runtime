---
name: fetch-openaq
description: Retrieve OpenAQ raw artifacts through explicit metadata discovery, measurement fetch, or archive backfill commands. Use when environmental investigators need provider-backed OpenAQ evidence inputs with provenance, quality, temporal scope, spatial scope, and coverage limitations, without research conclusions.
---

# OpenAQ Data Fetch

## Core Goal
- Query OpenAQ API v3 metadata endpoints with an explicit `fetch-metadata` command.
- Query OpenAQ API v3 measurements with an explicit `fetch-measurements` command.
- List or download public OpenAQ S3 archive partitions with an explicit `fetch-archive-backfill` command.
- Keep the compatibility `fetch` router for older run plans, but do not treat automatic routing as a research decision.
- Keep credentials and region configuration in environment variables.
- Return raw provider payloads plus `fetch_contract` metadata only; this skill does not normalize, analyze, route, score, or write board judgement.

## Required Environment
- `OPENAQ_API_KEY` (required for API requests).
- `OPENAQ_REGION` (optional for S3 endpoint selection, defaults to `us-east-1` when unset).
- Optional:
  - `OPENAQ_S3_BUCKET` (defaults to `openaq-data-archive`).

Reference:
- `references/env.md`
- `assets/config.example.env`

## Workflow
1. Validate environment and defaults.

```bash
python3 scripts/openaq_api_client.py check-config
python3 scripts/openaq_s3_fetch.py check-config
```

2. Choose exactly one raw fetch operation:
- `fetch-metadata`: metadata discovery for countries/providers/locations/sensors/parameters.
- `fetch-measurements`: measurement rows for an explicit API path or sensor endpoint.
- `fetch-archive-backfill`: S3 archive listing or direct-key download for historical partitions.

3. Fetch data with one of the commands below. Each command emits `fetch_contract.source_provenance`, `data_quality`, `temporal_scope`, `spatial_scope`, and `coverage_limitations`.

## Split Commands
Discover metadata:

```bash
python3 scripts/openaq_router.py fetch-metadata \
  --entity locations \
  --api-query countries_id=9 \
  --api-query limit=100 \
  --pretty
```

Fetch measurements:

```bash
python3 scripts/openaq_router.py fetch-measurements \
  --sensor-id 2178 \
  --aggregation hourly \
  --datetime-from 2023-06-01T00:00:00Z \
  --datetime-to 2023-06-02T00:00:00Z \
  --pretty
```

Plan or run an archive backfill:

```bash
python3 scripts/openaq_router.py fetch-archive-backfill \
  --location-id 2178 \
  --year 2022 \
  --month 5 \
  --dry-run \
  --pretty
```

## API Usage
Rate limits (API only, scoped to API key):
- `60 / minute`
- `2,000 / hour`
- On exceed: `429 Too Many Requests`.
- S3 archive access does not use these OpenAQ API key rate limits.

Query any v3 path with optional query parameters:

```bash
python3 scripts/openaq_api_client.py request \
  --path /v3/locations \
  --query countries_id=9 \
  --query limit=100 \
  --pretty
```

Fetch across pages and merge `results`:

```bash
python3 scripts/openaq_api_client.py request \
  --path /v3/locations \
  --query countries_id=9 \
  --all-pages \
  --max-pages 20 \
  --pretty
```

## S3 Usage
List partition prefixes:

```bash
python3 scripts/openaq_s3_fetch.py ls \
  --prefix records/csv.gz/locationid=2178/ \
  --delimiter / \
  --max-keys 200
```

Build a Hive-style prefix for partition traversal:

```bash
python3 scripts/openaq_s3_fetch.py build-prefix \
  --location-id 2178 \
  --year 2022 \
  --month 5
```

Download one archived file by key:

```bash
python3 scripts/openaq_s3_fetch.py download \
  --key records/csv.gz/locationid=2178/year=2022/month=05/location-2178-20220503.csv.gz \
  --output ./data/location-2178-20220503.csv.gz
```

## Auto Routing
Compatibility only. Let the router choose API vs S3 for older fetch plans:

```bash
python3 scripts/openaq_router.py fetch \
  --source-mode auto \
  --volume-profile interactive \
  --api-path /v3/providers \
  --api-query limit=50 \
  --pretty
```

Force S3 path via full-backfill profile:

```bash
python3 scripts/openaq_router.py fetch \
  --source-mode auto \
  --volume-profile full-backfill \
  --s3-action build-prefix \
  --location-id 2178 \
  --year 2022 \
  --month 5 \
  --pretty
```

## References
- `references/openaq-api-endpoints.md`
- `references/openaq-s3-layout.md`
- `references/openaq-limitations.md`

## Scripts
- `scripts/openaq_api_client.py`
- `scripts/openaq_s3_fetch.py`
- `scripts/openaq_router.py`

## Boundary
- Fetch writes or emits raw artifacts/receipts only.
- Normalization belongs to `normalize-openaq-observation-signals`.
- Measurement interpretation, station representativeness, exposure inference, and policy conclusions belong to investigator findings or approved optional-analysis, not this fetch skill.
