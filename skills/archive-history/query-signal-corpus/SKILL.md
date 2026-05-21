---
name: query-signal-corpus
description: Query archived cross-run signals from the signal corpus so current investigation rounds can inspect historical signal references without ranking or recommending conclusions.
---

# Eco Query Signal Corpus

## Core Goal
- Filter archived historical signals across prior runs.
- Return compact reusable signal matches instead of forcing direct archive DB inspection.
- Produce one stable query artifact for downstream history-context assembly.
- Do not score, rank, tier, or recommend signals; agents decide relevance and use.

## Triggering Conditions
- A signal corpus archive database already exists.
- Need historical public or environment signal hints for the current round.
- Need a reusable query surface before assembling full history context.

## Read/Write Contract
- Reads `run_dir/../archives/eco_signal_corpus.sqlite` by default.
- Writes `run_dir/archive/signal_corpus_query_<round_id>.json` by default.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `db_path`
  - `output_path`
  - `query_text`
  - `region_label`
  - `plane`
  - `metric_family`
  - `source_skill`
  - `exclude_run_id`
  - `limit`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- Treat returned historical signals as reusable hints with archived refs, not as
  current-run proof, source ranking, or recommended conclusions.
- Empty or sparse matches can reflect missing archives, filters that are too
  narrow, lexical mismatch, excluded run ids, or archive import gaps. It does not
  prove that no analogous signal exists.
- When reusing historical signal context, keep current-run findings tied to
  current-run evidence refs and document any history-derived parameter choices.

## References
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/query_signal_corpus.py`
