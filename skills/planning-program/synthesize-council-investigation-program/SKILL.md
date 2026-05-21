---
name: synthesize-council-investigation-program
description: Synthesize a program-aware council investigation flow from a report blueprint and framing agent positions.
---

# synthesize-council-investigation-program

Create a DB-backed `council-investigation-program` after the framing/scope
council. The program turns report questions and agent positions into later
council agenda questions, issue-round boundaries, descriptive internal phases,
exit criteria, downgrade conditions, and restrained supplemental-round policy.

This skill must not choose source families, source skills, query variants,
query parameters, route ranking, priorities, scheduler queues, or automatic
execution. Investigator acquisition routes belong in acquisition turns,
`source-acquisition-proposal`, or route assessment.

Typical use:

```bash
python3 skills/planning-program/synthesize-council-investigation-program/scripts/synthesize_council_investigation_program.py \
  --run-dir runs/<run_id> \
  --run-id <run_id> \
  --round-id round-001-framing-scope \
  --author-role moderator \
  --pretty
```

Outputs:

- Deliberation DB object `council-investigation-program`
- Artifact `runtime/council_investigation_program_<round_id>.json`

## Agent Reasoning Guide

- Treat the output as a council program contract: agenda questions, claim-basis
  boundaries, role responsibility boundaries, and descriptive round organization.
- Do not turn report questions into fixed report sections, executable tasks, or a
  hidden scheduler queue.
- Do not preselect source families, source skills, queries, query parameters,
  route rankings, source priorities, or automatic execution. Those choices stay
  with investigator acquisition turns, source-acquisition proposals, and route
  assessment.
- `round_internal_phases` are descriptive prompts for agent reasoning. They are
  not a runtime state machine, hard gate, or automatic transition mechanism.
- Supplemental-round triggers must stay restrained. Ordinary query repair, zero
  results, same-family follow-up, or query variant expansion should remain in
  the current issue council round unless a later council object approves a
  stronger transition.
