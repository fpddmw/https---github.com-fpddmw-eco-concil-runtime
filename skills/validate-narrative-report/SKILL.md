---
name: validate-narrative-report
description: Validate narrative report draft structure, claim-boundary presence, and citation/audit visibility without ranking evidence.
---

# Validate Narrative Report

## Core Goal
- Check that a narrative report draft has required sections, visible claim boundaries, and citation/audit refs.
- Check that the report is reader-facing enough to avoid a pure object-summary
  dump.
- Check common claim-boundary hazards before publication: representative public
  opinion upgrades, sample percentages without public-discourse basis, GDELT tone
  treated as public sentiment, public source narratives used as physical
  attribution, attribution claims without visible basis, and advisory helper
  output that has not been carried by council/report-basis objects.
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
