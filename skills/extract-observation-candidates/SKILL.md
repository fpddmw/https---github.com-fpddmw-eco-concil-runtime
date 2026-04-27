---
name: extract-observation-candidates
description: Extract board-ready environmental observation candidates from normalized environment signals in the unified signal plane, aggregate signal clusters into compact observations, and persist a compact analytics artifact for downstream council work.
---

# Eco Extract Observation Candidates

## Core Goal
- Read normalized environment signals from the unified signal plane.
- Aggregate signal clusters into compact observation candidates.
- Persist a candidate artifact for downstream matching, merging, and audit steps.

## Triggering Conditions
- Need observation objects instead of raw environment-signal rows.
- Need stable summaries of pm, weather, fire, or other environment metrics.
- Need a candidate artifact for environmentalist, moderator, or challenger workflows.

## Read/Write Contract
- Reads from `normalized_signals` where `plane = environment` in `runs/<run_id>/analytics/signal_plane.sqlite`.
- Writes `runs/<run_id>/analytics/observation_candidates_<round_id>.json` by default.
- Syncs the emitted observation-candidate result set into the shared analysis-plane tables in `runs/<run_id>/analytics/signal_plane.sqlite`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `db_path`
  - `source_skill`
  - `metric`
  - `quality_flag_any`
  - `max_candidates`
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

## Scripts
- `scripts/extract_observation_candidates.py`
