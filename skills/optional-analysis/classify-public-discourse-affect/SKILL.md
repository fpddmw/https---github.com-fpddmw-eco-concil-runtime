---
name: classify-public-discourse-affect
description: Optional-analysis annotation-worker skill for labeling a materialized public discourse corpus with sample-local affect, stance, issue, and source-narrative labels without making findings or public-opinion claims.
---

# Classify Public Discourse Affect

## Core Goal
- Read an approved `materialize-public-discourse-corpus` artifact.
- Produce item-level annotation rows for:
  - `affect_labels`
  - `issue_facets`
  - `source_narrative_labels`
  - `actor_responsibility_labels`
  - `responsibility_attribution_labels`
  - `action_orientation_labels`
  - `policy_demand_labels`
  - `trust_confidence_labels`
  - `uncertainty_labels`
  - `formal_policy_semantic_labels`
- Keep the annotation worker separate from `social-investigator` judgement.
- Avoid findings, source ranking, public-opinion inference, physical source attribution, or report conclusions.

## Governance Boundary
- This skill acts as a bounded public-discourse annotation worker, not as a council agent.
- `social-investigator` owns sample selection, annotation-basis choice, and council uptake; it does not personally author every affect label.
- `challenger` does not need to re-label every item. Challenger review should focus on sample boundary, taxonomy fit, ambiguous clusters, outliers, and report wording.
- The output remains optional-analysis/advisory material until a council object explicitly cites it.

## Read/Write Contract
- Reads `public_discourse_corpus_<round_id>.json`.
- Writes `run_dir/analytics/public_discourse_affect_annotations_<round_id>.json`.
- Output can be passed to `aggregate-public-discourse-annotations`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `corpus_path`

## Optional Input
- `annotation_basis_ref`
- `output_path`
- `max_items`
- `max_labels_per_family`

## Workflow

```bash
python3 scripts/classify_public_discourse_affect.py \
  --run-dir [RUN_DIR] \
  --run-id [RUN_ID] \
  --round-id [ROUND_ID] \
  --corpus-path [CORPUS_JSON] \
  --annotation-basis-ref public-discourse-affect-worker-v1 \
  --pretty
```

Then aggregate:

```bash
python3 ../aggregate-public-discourse-annotations/scripts/aggregate_public_discourse_annotations.py \
  --run-dir [RUN_DIR] \
  --run-id [RUN_ID] \
  --round-id [ROUND_ID] \
  --corpus-path [CORPUS_JSON] \
  --annotations-path [RUN_DIR]/analytics/public_discourse_affect_annotations_[ROUND_ID].json \
  --pretty
```

## Agent Reasoning Guide
- Use this skill after the corpus is fixed. Do not run it directly on raw fetch artifacts.
- Treat labels as sample-local semantic annotations. They are not public-opinion estimates.
- Do not apply affect labels to GDELT provider tone rows. GDELT DOC/row tone remains media/document tone.
- Keep sarcasm/humor and uncertainty separate from positive/negative simplifications.
- Keep policy demand, trust/confidence, uncertainty, responsibility attribution,
  and formal-policy semantic labels source-family local. Formal comment and
  formal record labels are not public sentiment.
- If a report will cite a controversial example, ask challenger to review that cited example or cluster; do not require challenger review for every low-stakes item.

## Scripts
- `scripts/classify_public_discourse_affect.py`
