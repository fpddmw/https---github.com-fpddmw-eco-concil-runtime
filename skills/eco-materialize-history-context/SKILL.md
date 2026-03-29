---
name: eco-materialize-history-context
description: Build retrieval-ready history context for the current round by querying the archived case library and signal corpus, selecting compact excerpts, and rendering one markdown context object.
---

# Eco Materialize History Context

## Core Goal
- Build one structured history query from current run artifacts.
- Retrieve analogous archived cases and historical signal hints.
- Render one retrieval snapshot and one compact markdown history context.

## Triggering Conditions
- Archive stores already contain historical cases or signals.
- A moderator or investigator needs historical precedent before the next planning step.
- Need compact context rather than raw archive DB browsing.

## Read/Write Contract
- Reads `run_dir/mission.json` by default when present.
- Reads `run_dir/board/board_state_summary_<round_id>.json` by default when present.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Reads `run_dir/reporting/round_readiness_<round_id>.json` by default when present.
- Reads `run_dir/investigation/next_actions_<round_id>.json` by default when present.
- Reads `run_dir/investigation/falsification_probes_<round_id>.json` by default when present.
- Reads `run_dir/promotion/promoted_evidence_basis_<round_id>.json` by default when present.
- Reads `run_dir/analytics/claim_scope_proposals_<round_id>.json` by default when present.
- Reads `run_dir/analytics/observation_scope_proposals_<round_id>.json` by default when present.
- Reads `run_dir/analytics/signal_plane.sqlite` by default when present.
- Reads `run_dir/../archives/eco_case_library.sqlite` by default.
- Reads `run_dir/../archives/eco_signal_corpus.sqlite` by default.
- Writes `run_dir/archive/case_library_query_<round_id>.json` by default.
- Writes `run_dir/archive/signal_corpus_query_<round_id>.json` by default.
- Writes `run_dir/investigation/history_retrieval_<round_id>.json` by default.
- Writes `run_dir/investigation/history_context_<round_id>.md` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `case_library_db_path`
  - `signal_corpus_db_path`
  - `case_query_path`
  - `signal_query_path`
  - `retrieval_path`
  - `context_path`
  - `max_cases`
  - `max_excerpts_per_case`
  - `max_signals`

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
- `scripts/eco_materialize_history_context.py`