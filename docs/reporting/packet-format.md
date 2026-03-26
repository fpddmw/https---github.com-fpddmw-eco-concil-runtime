# Packet Format

## Expert Report Packet

`build-report-packets` writes one packet per expert role:

- `round_001/sociologist/derived/report_packet.json`
- `round_001/environmentalist/derived/report_packet.json`

Each packet contains:

- `packet_kind`
- `generated_at_utc`
- `run`
- `role`
- `task_scope`
- `context`
- `instructions`
- `validation`
- `existing_report`
- `draft_report`

`draft_report` is already a valid `expert-report` object. Treat it as the editable baseline, not as the final report.

## Moderator Decision Packet

`build-decision-packet` writes:

- `round_001/moderator/derived/decision_packet.json`

The decision packet contains:

- `packet_kind`
- `generated_at_utc`
- `run`
- `round_context`
- `reports`
- `report_sources`
- `unresolved_claims`
- `missing_evidence_types`
- `proposed_next_round_tasks`
- `instructions`
- `validation`
- `draft_decision`

`draft_decision` is already a valid `council-decision` object and may be promoted after moderator review.

## Promotion Rule

Keep derived packets and canonical outputs separate:

- Drafts live under `*/derived/`
- Final contract objects stay in:
  - `sociologist/sociologist_report.json`
  - `environmentalist/environmentalist_report.json`
  - `moderator/council_decision.json`
