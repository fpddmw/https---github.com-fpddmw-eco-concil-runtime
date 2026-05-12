# GDELT DOC API Constraints and Safety Notes

This skill uses GDELT DOC 2.0 API for query-driven retrieval.

## Constraints Confirmed from Official Documentation

- DOC API is query-driven and mode-dependent, not raw table-file export.
- Time windows can be set by relative `timespan` or absolute UTC datetime bounds.

## DOC API Limits (Reference)

Official DOC API documentation notes:

- `timespan` minimum unit is 15 minutes.
- Monitoring mode can scan up to 3 months in one call.
- `MAXRECORDS` applies to `artlist` and image collage modes:
  - default `75`
  - maximum `250`
- `TIMELINESMOOTH` maximum is `30`.
- Timeline modes have bin limits (documentation references cap behavior around 500 bins in specific modes).
- 429 responses can occur when request cadence is too high; use slower polling and retries.
- Search-engine operators such as `site:` are not DOC query operators. Use `domain:` or exact `domainis:` filters.
- Long queries or several OR blocks joined together can be rejected by the provider. Prefer several compact invocations, or repeated `--domain-is` values that the skill executes as split queries.

## Skill-Side Safety Measures

The official DOC page does not publish a clear numeric QPS quota.
This skill applies client-side protections:

- Configurable timeout (`GDELT_TIMEOUT_SECONDS`)
- Retries with exponential backoff (`GDELT_MAX_RETRIES`, `GDELT_RETRY_BACKOFF_*`)
- Request throttling (`GDELT_MIN_REQUEST_INTERVAL_SECONDS`, default `5.0`)
- Parameter validation before request dispatch
- Query linting for unsupported operators and fragile long/compound query shapes
- Exact-domain split execution through repeated `--domain-is` values
- Atomic invocation design: no internal polling loop in this skill

Reference:

- `https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/`
