# Normalization Roadmap

## Recommended Data Plane

Use a four-stage deterministic pipeline:

1. `landing`
   - persist raw JSON, JSONL, CSV, or ZIP outputs exactly as returned by each fetch skill
2. `normalize`
   - map source-specific fields into canonical council objects
3. `link`
   - associate claims, observations, and evidence cards
4. `derive`
   - build compact round context for moderator and expert prompts

Keep OpenClaw in the control plane, not the data plane.

## Storage Pattern

Use this split:

- `runs/<run_id>/raw/...`
  - immutable source artifacts
- `runs/<run_id>/normalized/...`
  - canonical JSON or JSONL objects
- `runs/<run_id>/shared/...`
  - linked cross-role payloads such as `claims.json`, `observations.json`, `evidence_cards.json`
- `SQLite`
  - run state, round state, canonical object registry, file provenance
- `DuckDB`
  - analytical joins, aggregations, export to compact context sets

Keep SQLite as the control-plane truth store and DuckDB as the analytical workbench.

## Deterministic Operations

Implement these without any LLM:

- datetime parsing and UTC normalization
- schema validation
- enum normalization
- unit normalization
- ID assignment
- duplicate suppression
- coordinate range checks
- window clipping
- raw-to-canonical field mapping
- evidence-path hashing and provenance logging

## Recommended Module Split

Start with four deterministic modules:

1. `normalize_listener_outputs`
   - convert GDELT, Bluesky, YouTube, and Regulations.gov outputs into stable public-signal records and `claim` candidates
2. `normalize_environment_outputs`
   - convert OpenAQ, Open-Meteo, and NASA FIRMS outputs into `observation` records
3. `link_claims_to_evidence`
   - link canonical claims to matching observations by time window, geometry overlap, and topic rules
4. `build_round_context`
   - produce compact JSON for moderator, sociologist, environmentalist, and later historian prompts

## Where LLMs Help

Use an LLM only in a narrow semantic lane:

- claim extraction from text-heavy public signals
- ambiguous place-name disambiguation when deterministic geocoding fails
- rhetorical clustering of near-duplicate public claims
- explanation of why evidence is mixed or insufficient
- historical analog comparison

Do not use an LLM for base cleaning, typing, or de-duplication.

## Geocoding Strategy

Current physical skills require coordinates or bounding boxes, so add a deterministic pre-step:

- if the mission already provides coordinates, trust them
- if not, resolve place text once and store the result in the mission or claim scope
- keep resolved geometry separate from raw text mention

Do not let moderator or expert prompts silently infer coordinates.

## Suggested Next Build Order

1. finalize this shared contract
2. create deterministic normalizers for sociologist outputs
3. create deterministic normalizers for environmentalist outputs
4. add a linking layer that creates `evidence-card`
5. add moderator round context builder
6. add historian case-support contract and retrieval layer
