---
name: validate-narrative-report
description: Validate narrative report draft structure, claim-boundary presence, and citation/audit visibility without ranking evidence.
---

# Validate Narrative Report

## Core Goal
- Check that a narrative report draft has required sections, visible claim boundaries, and citation/audit refs.
- Require the report template sections through `Audit Trail`, so JSON and
  Markdown publications both keep a stable traceability index.
- Check that the report is reader-facing enough to avoid a pure object-summary
  dump.
- Check common claim-boundary hazards before publication: representative public
  opinion upgrades, sample percentages without public-discourse basis, GDELT tone
  treated as public sentiment, public source narratives used as physical
  attribution, attribution claims without visible basis, and advisory helper
  output that has not been carried by council/report-basis objects.
- Allow general public-opinion wording only when the mission itself records an
  explicit representative sampling or survey design.
- Warn when public-discourse percentages omit sample-local and non-exclusive
  label boundaries, even when an approved summary exists.
- Warn when any optional-analysis helper artifact, including environment
  aggregation, appears in report prose or refs without a visible council or
  report-basis carrier.
- Keep validation structural and procedural.
- Avoid scoring, weighting, ranking, or deciding evidentiary truth.

## Triggering Conditions
- `draft-narrative-report` has produced a draft artifact.
- Report-editor needs a lightweight release-readiness check before publish.

## Read/Write Contract
- Reads `run_dir/reporting/narrative_report_draft_<round_id>.json`.
- Writes `run_dir/reporting/narrative_report_validation_<round_id>.json`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `draft_path`
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

## Agent Reasoning Guide
- This skill validates report form and traceability only. It does not certify
  truth, sufficiency, source priority, or conclusion strength.
- Claim-boundary errors mean the draft wording exceeds its declared basis. They
  do not decide whether the underlying claim is true; they require report-editor
  revision or a moderator-visible decision about the boundary.
- A validation warning should become a report limitation or a moderator decision
  about a separate investigation continuation; it should not be hidden by prose.
- The validator should warn when the draft leads with runtime object labels
  instead of conclusions, key points, reasoning, limitations, and decision use.
- Reader-facing quality checks are procedural checks, not style scoring or
  evidence ranking.

## Scripts
- `scripts/validate_narrative_report.py`
