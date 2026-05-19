# Regulations.gov Comments API Notes

## Endpoint

- `GET /comments`
- Base URL: `https://api.regulations.gov/v4`
- Authentication: `X-Api-Key: <REGGOV_API_KEY>`

## Filters Used by This Skill

- `filter[lastModifiedDate][ge]`
- `filter[lastModifiedDate][le]`
  - Format: `yyyy-MM-dd HH:mm:ss`
- `filter[postedDate][ge]`
- `filter[postedDate][le]`
  - Format: `yyyy-MM-dd`
- Optional:
  - `filter[docketId]`
  - `filter[agencyId]`
  - `filter[commentOnId]`
  - `filter[searchTerm]`
  - `filter[documentType]`
  - `filter[subtype]`

`--comment-on-document-id` is accepted as a clearer OpenClaw-facing alias and is
sent as `filter[commentOnId]`.

## Pagination

- `page[size]`: accepted `5` to `250`
- `page[number]`: starts at `1`
- Response `meta` includes pagination state:
  - `hasNextPage`
  - `pageNumber`
  - `pageSize`
  - `totalElements`
  - `totalPages`

## Sorting

- Common values for comments endpoint:
  - `postedDate`
  - `lastModifiedDate`
  - `documentId`
- Descending sort: prefix `-` (example: `-lastModifiedDate`)

## Response Shape (List)

- Top-level object with:
  - `data`: array of comment resources
  - `meta`: pagination + filters + aggregations
- Each `data[]` item generally contains:
  - `id`
  - `type` (expected `comments`)
  - `attributes`
  - `links`

The skill validates this structure and records validation issues when present.

## Candidate Corpus Summary

Fetch results include `candidate_corpus_summary` when records are included. This
summary reports candidate IDs, field coverage, source limitations, and likely
drift indicators such as docket mismatch, commentOn mismatch, agency mismatch,
keyword miss, and duplicate or mass-campaign cues.

These are sample-shape cues only. They do not judge comment stance, importance,
evidence sufficiency, or public-opinion distribution.
