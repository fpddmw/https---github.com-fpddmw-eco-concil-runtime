# Integration Boundary

## Position in the Stack

`eco-council-simulate` sits between:

1. `prepare-round`
2. `run-data-plane`

It is an alternative raw-artifact producer, not a replacement control plane.

## Invocation Boundary

- Invoke it explicitly from a human operator, eval harness, or test script.
- Do not inject it implicitly into moderator, sociologist, or environmentalist runtime prompts.
- Do not let it participate in task review, source selection, reporting, or council decision drafting.
- Treat it as a low-cost external fetch runner for replay, smoke, and fault-injection work only.

## Reads

- `mission.json`
- `round_xxx/moderator/tasks.json`
- `round_xxx/moderator/derived/fetch_plan.json`
- optional custom scenario JSON

## Writes

- `round_xxx/<role>/raw/*`
- `round_xxx/<role>/raw/_meta/*.stdout.json`
- `round_xxx/<role>/raw/_meta/*.stderr.log`
- `round_xxx/moderator/derived/fetch_execution.json`

The canonical `fetch_execution.json` should describe the full current fetch plan, not a partial subset. It should remain importable by the supervisor without relaxing any current-round step checks.

## Does Not Write

- `round_xxx/shared/claims.json`
- `round_xxx/shared/observations.json`
- `round_xxx/shared/evidence_cards.json`
- `round_xxx/*/derived/report_packet.json`
- `round_xxx/*/*_report.json`
- `round_xxx/moderator/council_decision.json`

## Supervisor Integration

When a run is stage-gated by `$eco-council-supervisor`, the simulator should not edit supervisor state directly.

Instead:

1. run the simulator
2. import the resulting canonical `fetch_execution.json` with:
   - `python3 eco-council-supervisor/scripts/eco_council_supervisor.py import-fetch-execution ...`
3. continue to the data plane

This keeps simulation as one external fetch runner among many possible runners.

## Safety Rules

- Respect the same `fetch.lock` location as live fetch execution.
- Re-check `fetch_plan.input_snapshot` before writing outputs.
- Simulate only the steps already present in `fetch_plan.json`.
- Keep source semantics deterministic and reproducible from `scenario_id`, `seed`, and `source_skill`.
- Keep all synthetic artifacts downstream-compatible, but clearly non-authoritative as real-world evidence.
- Include the current `plan_sha256` in `fetch_execution.json` so the supervisor can reject stale imports after `prepare-round` changes.
