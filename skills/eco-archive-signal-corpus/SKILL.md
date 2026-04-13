---
name: eco-archive-signal-corpus
description: Archive one run's normalized signal-plane rows into a cross-run signal corpus SQLite store so later rounds can reuse historical public and environment signals.
---

# Eco Archive Signal Corpus

## Core Goal
- Read the current run's normalized signal-plane rows.
- Import them into one cross-run signal corpus database.
- Emit one auditable import snapshot for the current round.

## Triggering Conditions
- The run already contains normalized signal-plane data.
- Need cross-run signal reuse rather than only per-run analytics access.
- Need history-context assembly to search historical signals later.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`.
- Reads `run_dir/mission.json` by default when present.
- Reads `run_dir/board/board_brief_<round_id>.md` by default when present.
- Writes `run_dir/../archives/eco_signal_corpus.sqlite` by default.
- Writes `run_dir/archive/signal_corpus_import_<round_id>.json` by default.

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
- `../../docs/openclaw-project-overview.md`
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/eco_archive_signal_corpus.py`
