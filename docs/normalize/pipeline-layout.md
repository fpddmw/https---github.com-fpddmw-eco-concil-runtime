# Pipeline Layout

## Directory Contract

This runtime expects a scaffolded run directory from the packaged eco-council contract CLI.

Recommended layout after initialization:

```text
runs/<run_id>/
├── mission.json
├── analytics/
│   ├── public_signals.sqlite
│   └── environment_signals.sqlite
├── run_manifest.json
└── round_001/
    ├── moderator/
    │   └── derived/
    │       └── context_moderator.json
    ├── sociologist/
    │   ├── raw/
    │   ├── normalized/
    │   │   ├── public_signals.jsonl
    │   │   └── claim_candidates.json
    │   └── derived/
    │       └── context_sociologist.json
    ├── environmentalist/
    │   ├── raw/
    │   ├── normalized/
    │   │   ├── environment_signals.jsonl
    │   │   ├── observations.json
    │   │   └── evidence_cards.json
    │   └── derived/
    │       └── context_environmentalist.json
    └── shared/
        ├── claims.json
        ├── observations.json
        ├── evidence_cards.json
        └── contexts/
            └── round_snapshot.json
```

## Storage Semantics

- `raw/`
  - immutable upstream artifacts from fetch skills
- `normalized/`
  - role-owned deterministic outputs
- `shared/`
  - canonical exchange files for cross-role coordination
- `analytics/*.sqlite`
  - staging/aggregation workbenches

## Backend Note

This first implementation uses separate SQLite staging databases because they are available in the standard library and fit the current repository style.

The generated JSONL, canonical JSON, and staging schemas are intentionally DuckDB-friendly so the analytics layer can be swapped later without changing the role-facing contract.
