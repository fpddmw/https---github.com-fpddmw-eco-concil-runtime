---
name: extract-document-text
description: Extract bounded text artifacts from local TXT, HTML, and PDF documents while preserving extraction limits, page counts, and OCR/scanned-file caveats without semantic classification.
---

# Extract Document Text

## Core Goal
- Read local document files directly or from a fetch manifest.
- Extract plain text into text artifacts for TXT/HTML and PDF when a local PDF reader is available.
- Emit extraction status, page counts, empty-page counts, quality flags, and source linkage.
- Preserve metadata-only attachment records from fetch manifests when the file
  URL was resolved but the local download failed, marking them as limited rather
  than dropping the attachment route.
- Avoid issue labels, stance, concern, policy conclusions, or evidence sufficiency judgements.

## Read/Write Contract
- Reads local files or manifest downloads.
- Writes text files under `--output-dir`.
- Writes a JSON manifest to `--manifest-output` or `document_text_extraction_manifest.json`.

## Required Input
- At least one `--input-path` or `--input-manifest`.

## Agent Reasoning Guide
- Treat extraction output as a readability artifact. It is not a formal comment
  finding, not a stance label, and not proof of content absence.
- `text-extraction-limited`, `pdf-reader-unavailable`, or `scanned-or-ocr-suspected`
  means the council may need an OCR/manual route before drawing conclusions.
- `metadata-only-no-local-file` or `attachment-download-failed` means the
  upstream attachment route was found but text was not available locally; do not
  treat this as absence of attachment content.
- Preserve upstream fetch/detail/audit refs before downstream use so attachment
  text remains traceable to selected comments.

## Script
- `scripts/extract_document_text.py`
