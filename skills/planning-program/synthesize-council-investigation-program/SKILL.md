---
name: synthesize-council-investigation-program
description: Synthesize a program-aware council investigation flow from a report blueprint and framing agent positions.
---

# synthesize-council-investigation-program

Create a DB-backed `council-investigation-program` after the framing/scope
council. The preferred path is agent-authored planning: agents submit
`agent-position` objects whose `payload_json` contains `proposed_program_rounds`
or equivalent round proposals, and this skill adopts those proposals into later
council agenda questions, round boundaries, descriptive internal phases, exit
criteria, downgrade conditions, and restrained supplemental-round policy.

If no agent-authored round proposals are visible, the skill can fall back to a
blueprint-derived conservative program. That fallback is an operator recovery
path, not the intended deliberative planning path.

This skill must not choose source families, source skills, query variants,
query parameters, route ranking, priorities, scheduler queues, or automatic
execution. Investigator acquisition routes belong in acquisition turns,
`source-acquisition-proposal`, or route assessment.

## Required Input

- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `author_role`
  - `blueprint_id`
  - `program_id`
  - `output_path`

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
- Prefer agent-authored or agent-adopted program proposals over generated
  templates. A useful `agent-position.payload_json` may contain
  `proposed_program_rounds`, where each item has a question-form
  `round_subtitle_question`, optional `program_order`, `round_category`, `round_mode`,
  `active_theme_ids`, `agent_responsibility_boundaries`, and
  `round_internal_phases`.
- Agents decide whether acquisition, analysis, interaction synthesis,
  policy-basis review, or other issue rounds should be separate. This skill
  should preserve those choices unless they violate the source/query/route
  autonomy boundary.
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
