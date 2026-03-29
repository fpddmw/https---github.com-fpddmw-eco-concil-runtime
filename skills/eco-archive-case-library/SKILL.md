---
name: eco-archive-case-library
description: Archive one run's canonical board, reporting, and promotion artifacts into a compact case library SQLite store for future historical retrieval.
---

# Eco Archive Case Library

## Core Goal
- Compress one run's canonical investigation state into a reusable case library entry.
- Preserve compact excerpts, profile tags, structured overlap fields, and final posture.
- Emit one auditable archive import snapshot for later history retrieval.

## Triggering Conditions
- The run already produced board, promotion, or reporting artifacts.
- Need cross-run case reuse instead of only within-run evidence review.
- Need later history-context assembly to retrieve prior analogous cases.

## Read/Write Contract
- Reads `run_dir/mission.json` by default when present.
- Reads `run_dir/board/investigation_board.json` by default when present.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default when present.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Reads `run_dir/analytics/claim_scope_proposals_<round_id>.json` by default when present.
- Reads `run_dir/analytics/observation_scope_proposals_<round_id>.json` by default when present.
- Reads `run_dir/analytics/evidence_coverage_<round_id>.json` by default when present.
- Reads `run_dir/investigation/next_actions_<round_id>.json` by default when present.
- Reads `run_dir/investigation/falsification_probes_<round_id>.json` by default when present.
- Reads `run_dir/reporting/round_readiness_<round_id>.json` by default when present.
- Reads `run_dir/promotion/promoted_evidence_basis_<round_id>.json` by default when present.
- Reads `run_dir/reporting/reporting_handoff_<round_id>.json` by default when present.
- Reads `run_dir/reporting/council_decision_<round_id>.json` by default when present.
- Reads `run_dir/reporting/council_decision_draft_<round_id>.json` by default when present.
- Reads `run_dir/reporting/final_publication_<round_id>.json` by default when present.
- Reads `run_dir/reporting/expert_report_sociologist_<round_id>.json` by default when present.
- Reads `run_dir/reporting/expert_report_environmentalist_<round_id>.json` by default when present.
- Reads `run_dir/analytics/signal_plane.sqlite` by default when present.
- Writes `run_dir/../archives/eco_case_library.sqlite` by default.
- Writes `run_dir/archive/case_library_import_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `db_path`
  - `output_path`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## References
- `../../openclaw-first-refactor-blueprint.md`
- `../../openclaw-full-development-report.md`

## Scripts
- `scripts/eco_archive_case_library.py`