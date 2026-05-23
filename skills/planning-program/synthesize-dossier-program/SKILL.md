---
name: synthesize-dossier-program
description: Synthesize a dossier-first council program from a report outcome contract, investigation themes, and role-separated framing positions.
---

# synthesize-dossier-program

Create a DB-backed `dossier-program` after the framing/scope council. The
program decomposes the `report-outcome-contract` into reviewable theme cycles
that must each produce a theme dossier, theme report, challenger review, and
moderator adoption before final report composition.

This skill is program planning only. It must not choose provider classes, source
skills, queries, query parameters, route rankings, scheduler queues, or automatic
execution. Investigator acquisition choices remain in later work turns,
`source-acquisition-proposal`, or route assessment.

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `author_role`
  - `contract_id`
  - `program_id`
  - `output_path`

Typical use:

```bash
python3 skills/planning-program/synthesize-dossier-program/scripts/synthesize_dossier_program.py \
  --run-dir runs/<run_id> \
  --run-id <run_id> \
  --round-id round-001-framing-scope \
  --author-role moderator \
  --pretty
```

Outputs:

- Deliberation DB object `dossier-program`
- DB-backed projected `round-brief` objects for each planned dossier cycle
- Artifact `runtime/dossier_program_<round_id>.json`
- `human_review_packet` showing required theme reports, theme cycles, missing
  framing roles, and future composition-planning boundary

## Agent Reasoning Guide

- Treat the dossier program as a contract for later council work, not as a route
  plan.
- Each theme cycle must preserve acquisition, structuring, analysis,
  theme-report, review, and adoption as distinct responsibilities.
- Final report writing is outside this skill. It can start only after adopted
  theme reports and a final composition plan exist.
