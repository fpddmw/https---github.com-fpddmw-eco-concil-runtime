# Context Format

`build-round-context` writes compact JSON payloads for role prompts.

## Files

- `moderator/derived/context_moderator.json`
- `sociologist/derived/context_sociologist.json`
- `environmentalist/derived/context_environmentalist.json`
- `shared/contexts/round_snapshot.json`

## Top-Level Shape

```json
{
  "run": {
    "run_id": "eco-20260320-chiangmai-smoke",
    "round_id": "round-001",
    "topic": "Chiang Mai smoke verification",
    "objective": "Determine whether public smoke claims are supported by physical evidence."
  },
  "dataset": {
    "generated_at_utc": "2026-03-21T08:00:00Z",
    "claim_count": 3,
    "observation_count": 6,
    "evidence_count": 3
  },
  "aggregates": {
    "claim_type_counts": {
      "smoke": 2
    },
    "observation_metric_counts": {
      "pm2_5": 1,
      "fire_detection_count": 1
    },
    "evidence_verdict_counts": {
      "supports": 1,
      "insufficient": 2
    }
  },
  "claims": [],
  "observations": [],
  "evidence_cards": []
}
```

## Role Differences

- Moderator context:
  - includes all aggregates and all current canonical objects
- Sociologist context:
  - emphasizes claims, unresolved evidence gaps, and mission/task framing
- Environmentalist context:
  - emphasizes claims needing physical validation, available observations, and matching gaps

## Prompting Guidance

- Use `claims`, `observations`, and `evidence_cards` as primary evidence.
- Treat `aggregates` only as compression aids.
- Do not let agents infer coordinates or time windows not present in the payload.
