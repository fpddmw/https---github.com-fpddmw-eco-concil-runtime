---
name: post-board-note
description: Append a compact human-readable investigation note to the local board export, preserve linked evidence refs, and emit an auditable board event without creating canonical judgement.
---

# Post Board Note

## Core Goal
- Append one compact note to the current round's board state.
- Preserve linked evidence refs and related ids.
- Emit an auditable board event for downstream review.
- Keep the note human-readable; it is not a finding, hypothesis judgement, readiness opinion, or report basis.

## Triggering Conditions
- Need to capture a human-readable note on the board.
- Need to anchor compact evidence refs to a board-visible note.
- Need to initialize board activity for a run or round.

## Read/Write Contract
- Reads the shared deliberation plane first and exports `run_dir/board/investigation_board.json` for compatibility.
- Bootstraps existing board JSON into the deliberation plane when needed.
- Appends one note and one board event on the deliberation plane.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `author_role`
- `note_text`
- Optional:
  - `board_path`
  - `category`
  - `tag`
  - `linked_artifact_ref`
  - `related_id`

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

## Agent Reasoning Guide
- A board note is human-readable context only. It is not a canonical finding,
  hypothesis judgement, readiness opinion, challenge disposition, or report
  basis.
- Use canonical write skills when the council needs a judgement, evidence
  request, proposal, challenge, or readiness posture.
- Empty or informal notes should never be used as proof that an issue has been
  investigated or resolved.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/post_board_note.py`
