---
name: open-challenge-ticket
description: Open a challenge ticket on the local investigation board, preserve target ids, linked evidence refs, evidence bundle ids, and emit an auditable board event for challenger workflows.
---

# Open Challenge Ticket

## Core Goal
- Open one challenge ticket on the current round's board state.
- Preserve target ids, linked evidence refs, and evidence bundle cross-references.
- Emit an auditable board event for downstream challenger and moderator work.

## Triggering Conditions
- Need to turn a contradiction or uncertainty into an explicit review ticket.
- Need a board-visible object for alternative hypothesis or falsification work.
- Need a durable challenger queue item rather than an ad hoc note.
- Need to challenge a finding or evidence bundle while keeping the bundle queryable by moderator and report-editor.

## Read/Write Contract
- Reads the shared deliberation plane first and exports `run_dir/board/investigation_board.json` for compatibility.
- Bootstraps existing board JSON into the deliberation plane when needed.
- Appends one challenge ticket and one board event on the deliberation plane.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `title`
- `challenge_statement`
- Optional:
  - `board_path`
  - `target_claim_id`
  - `target_hypothesis_id`
  - `priority`
  - `owner_role`
  - `linked_artifact_ref`
  - `evidence_bundle_id`

## Output Contract
- `status`
- `summary`
  - Includes `db_path`
  - Includes `write_surface`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-investigator-role-runbook.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/open_challenge_ticket.py`
