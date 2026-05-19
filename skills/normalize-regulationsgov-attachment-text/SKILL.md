---
name: normalize-regulationsgov-attachment-text
description: Normalize extracted Regulations.gov attachment text manifests into formal-comment signal rows with comment and attachment linkage, extraction caveats, provenance, and quality flags without semantic issue or stance labels.
---

# Normalize Regulations.gov Attachment Text

## Core Goal
- Read an `extract-document-text` manifest derived from Regulations.gov attachments.
- Write formal `attachment-text` signals to the signal plane database.
- Preserve comment ID, attachment ID, file URL, text extraction status, artifact refs, and caveats.
- Keep normalization provider-field scoped; the normalizer does not derive
  semantic labels or claim strength.
- Avoid issue labels, stance, concern, citation typing, source ranking, or readiness decisions.

## Read/Write Contract
- Reads one extraction manifest.
- Writes `formal` rows to `run_dir/analytics/signal_plane.sqlite`.

## Agent Reasoning Guide
- Normalized attachment text is item-level formal evidence only. It is not proof
  of comment stance, corpus representativeness, or evidence sufficiency.
- `text-extraction-limited` and OCR/scanned flags must be preserved; no-row or
  limited-text output does not mean the attachment lacks relevant content.
- Use `query-formal-signals` to inspect rows and carry any judgement through a
  finding, evidence bundle, challenge, readiness opinion, or synthesis.

## Script
- `scripts/normalize_regulationsgov_attachment_text.py`
