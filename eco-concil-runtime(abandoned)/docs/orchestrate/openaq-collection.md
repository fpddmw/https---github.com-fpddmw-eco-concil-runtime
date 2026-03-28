# OpenAQ Collection

## Why This Wrapper Exists

`openaq-data-fetch` is intentionally generic, but eco-council rounds usually need one station-measurement artifact that downstream normalization can consume directly.

`collect-openaq` closes that gap by chaining:

1. `/v3/locations`
2. `/v3/locations/{id}/sensors`
3. `/v3/sensors/{id}/measurements`

and then writing one JSON file with:

- mission geometry and window request metadata
- discovery summary
- flattened measurement `records`

## Geometry Rules

- `BBox` missions use `bbox=...` for location discovery.
- `Point` missions use `coordinates=lat,lon` plus `radius`.

## Output Contract

The output file is shaped for `$eco-council-normalize`:

```json
{
  "source_skill": "openaq-data-fetch",
  "request": {},
  "discovery_summary": [],
  "record_count": 0,
  "records": []
}
```

`records` must stay a list of measurement-like JSON objects so the normalizer can map them into `station-observation` environment signals.

## Tuning Knobs

Use these CLI flags when coverage is too wide or too sparse:

- `--max-locations`
- `--max-sensors-per-location`
- `--max-pages`
- `--radius-meters`
- `--parameter-name`

Default behavior is intentionally conservative to avoid exploding API calls in a single round.
