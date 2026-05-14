---
name: open-followup-from-review-comment
description: Convert one DB-backed report-risk review comment into an explicit challenge ticket and claimed board follow-up task.
---

# Open Follow-Up From Review Comment

Use this skill when a `review-comment` with `report_risk` or required follow-up evidence should become board-visible follow-up work before readiness can proceed.

This skill:

1. Reads the target review comment from the deliberation DB.
2. Blocks if the comment is closed, missing, or not a report-risk/follow-up comment.
3. Opens a challenge ticket that preserves the comment target, report risk, relation objection fields, evidence refs, and lineage.
4. Creates a claimed board task linked to that challenge ticket and review comment.

It does not close or waive the review comment. A later challenger readiness opinion or explicit resolved comment status must handle waiver.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `review_comment_id`

## Optional Input
- `board_path`
- `owner_role`
- `priority`
- `task_status`

## Agent Reasoning Guide
- This skill converts a report-risk comment into visible follow-up work. It does
  not decide that the review comment is correct, resolved, or waived.
- The generated challenge/task remains procedural scaffolding until an agent
  writes findings, dispositions, or readiness opinions that cite the relevant
  evidence.
- If the source review comment lacks DB-backed evidence refs, preserve that as a
  limitation instead of treating it as substantive proof.
