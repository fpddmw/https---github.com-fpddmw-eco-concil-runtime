# Eco Council Reporting OpenClaw Templates

## Template A: Expert Report Drafting

Use when `report_packet.json` already exists and the sociologist or environmentalist should produce one `expert-report`.

### Sociologist

```text
Use the provisioned sociologist OpenClaw agent for this run.
Read and follow the current report outbox prompt:
[RUN_DIR]/supervisor/outbox/sociologist_report.txt
Return only JSON.
```

### Environmentalist

```text
Use the provisioned environmentalist OpenClaw agent for this run.
Read and follow the current report outbox prompt:
[RUN_DIR]/supervisor/outbox/environmentalist_report.txt
Return only JSON.
```

## Template B: Moderator Decision Drafting

Use when expert-report drafts already exist and the moderator should decide whether to continue, complete, or block the round.

```text
Use the provisioned moderator OpenClaw agent for this run.
Read and follow the current decision outbox prompt:
[RUN_DIR]/supervisor/outbox/moderator_decision.txt
Return only JSON.
```

## Template C: Promote Approved Drafts

Use after the returned JSON has been reviewed and imported into the canonical draft paths by the supervisor CLI.

### Promote everything

```text
Run:
PYTHONPATH=src python3 -m eco_council_runtime.supervisor continue-run \
  --run-dir [RUN_DIR] \
  --pretty
Return only JSON.
```

### Promote one report only

```text
Run:
PYTHONPATH=src python3 -m eco_council_runtime.reporting promote-report-draft \
  --run-dir [RUN_DIR] \
  --round-id round-001 \
  --role sociologist \
  --pretty
Return only JSON.
```

### Promote one moderator decision only

```text
Run:
PYTHONPATH=src python3 -m eco_council_runtime.reporting promote-decision-draft \
  --run-dir [RUN_DIR] \
  --round-id round-001 \
  --pretty
Return only JSON.
```

## Orchestration Rules

- Build packets before rendering prompts.
- Let expert agents revise only draft objects, not canonical outputs.
- Promote drafts only after validation and review.
- Rebuild packets for a round before asking the moderator for a new decision if upstream canonical objects changed.
