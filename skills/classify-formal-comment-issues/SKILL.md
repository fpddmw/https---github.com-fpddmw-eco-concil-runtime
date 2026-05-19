---
name: classify-formal-comment-issues
description: Optional-analysis annotation-worker skill for labeling DB-visible formal comment text with sample-local issue, stance, and concern cues without making findings or public-opinion claims.
---

# Classify Formal Comment Issues

## Core Goal
- Read normalized formal comment text signals from the signal-plane DB.
- Produce item-level annotation rows for:
  - `formal_issue_labels`
  - `formal_stance_hints`
  - `formal_concern_facets`
- Cover comment listings, comment detail records, and extracted attachment text.
- Avoid findings, source ranking, stance distribution conclusions, evidence sufficiency, or report conclusions.

## Governance Boundary
- This skill acts as a bounded formal-comment annotation worker, not as a council agent.
- Output is optional-analysis/advisory material until a council object explicitly cites it.
- Labels are sample-local candidate cues. They are not representative public-opinion estimates.
- Challenger review should focus on sample boundary, taxonomy fit, ambiguous clusters, outliers, and report wording before downstream use.

## Read/Write Contract
- Reads `run_dir/analytics/signal_plane.sqlite`.
- Reads only `formal` plane records from:
  - `fetch-regulationsgov-comments`
  - `fetch-regulationsgov-comment-detail`
  - `fetch-regulationsgov-attachments`
- Writes `run_dir/analytics/formal_comment_issue_annotations_<round_id>.json`.
- Output can be passed to `aggregate-public-discourse-annotations` for sample-local aggregation.

## Required Input
- `run_dir`
- `run_id`
- `round_id`

## Optional Input
- `round_scope`
- `source_skill`
- `signal_kind`
- `annotation_basis_ref`
- `output_path`
- `max_items`
- `max_labels_per_family`

## Workflow

```bash
python3 scripts/classify_formal_comment_issues.py \
  --run-dir [RUN_DIR] \
  --run-id [RUN_ID] \
  --round-id [ROUND_ID] \
  --annotation-basis-ref formal-comment-issue-worker-v1 \
  --pretty
```

Then aggregate with the same sample boundary:

```bash
python3 ../aggregate-public-discourse-annotations/scripts/aggregate_public_discourse_annotations.py \
  --run-dir [RUN_DIR] \
  --run-id [RUN_ID] \
  --round-id [ROUND_ID] \
  --annotations-path [RUN_DIR]/analytics/formal_comment_issue_annotations_[ROUND_ID].json \
  --pretty
```

## Agent Reasoning Guide
- Use this skill after readable formal comment text exists in the signal-plane DB. Do not run it directly on raw fetch artifacts.
- Treat labels as sample-local semantic annotations. They are not evidence sufficiency decisions, stance distribution findings, or general public-opinion estimates.
- Preserve the three evidence layers: list discovery, detail text, and attachment text. Do not collapse a single seed or attachment into the formal comment universe.
- If extracted attachment text is missing or limited, cite that limitation before it affects report wording.
- If a report will cite a controversial comment example, ask challenger to review that example or cluster; do not require challenger review for every low-stakes annotation row.

## Scripts
- `scripts/classify_formal_comment_issues.py`
