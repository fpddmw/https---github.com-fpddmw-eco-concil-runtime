---
name: score-evidence-coverage
description: Optional audited empirical evidence sufficiency helper for explicitly routed observation questions. It requires operator-approved skill approval and is not a global readiness gate.
---

# Eco Score Evidence Coverage

## Core Goal
- Read empirical link and scope proposals only for issues explicitly routed to an observation lane.
- Score evidence sufficiency and unresolved gaps as heuristic audit material.
- Persist a compact coverage artifact for challenge or review without making it the default readiness basis.
- Require `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id` before execution.

## Triggering Conditions
- A moderator or investigator has an approved empirical question with explicit time, place, and source scope.
- The operator has approved this optional-analysis run for the current round and actor role.
- Need a compact sufficiency review for empirical evidence, not a system-wide promotion or reporting gate.

## Read/Write Contract
- Reads claim-observation links from the run-local analysis plane first.
- Reads claim and observation scope proposals from the run-local analysis plane first.
- Falls back to the corresponding JSON artifacts when the synced result sets are unavailable.
- Writes `runs/<run_id>/analytics/evidence_coverage_<round_id>.json` by default.
- Syncs the same coverage result set into `runs/<run_id>/analytics/signal_plane.sqlite` as analysis-plane state.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `links_path`
  - `claim_scope_path`
  - `observation_scope_path`
  - `output_path`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `analysis_sync`
- `input_analysis_sync`
- `board_handoff`

## References
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-refactor-overall-notes.md`
- `../../docs/openclaw-skills-refactor-checklist-v2.md`

## Scripts
- `scripts/score_evidence_coverage.py`
