---
name: fetch-regulationsgov-attachments
description: Fetch Regulations.gov attachment metadata and downloadable files for selected comments or attachment IDs, preserving comment linkage, file metadata, sha256, and source limitations without extracting text or judging content.
---

# Regulations.gov Attachments Fetch

## Core Goal
- Read comment IDs, attachment IDs, or `fetch-regulationsgov-comment-detail` artifacts.
- Query Regulations.gov attachment metadata endpoints when needed.
- Download available attachment files to a run raw directory.
- Emit machine-readable metadata, download records, sha256, byte size, content type, and source comment linkage.

## Artifact Placement
- In council/runtime runs, write downloaded files under the assigned run path,
  usually `runs/<run-id>/raw/<round-id>/direct-fetch/regulationsgov-attachments/`.
- Use `--output-dir` and `--manifest-output` for deterministic artifact paths.
- Never write fetch outputs to repo-root `data/`.

## Required Environment
- Configure `REGGOV_API_KEY` or pass `--api-key`.
- Public attachment file URLs may not require the API key, but metadata endpoints do.

## Workflow
1. Dry-run from a detail artifact.

```bash
python3 scripts/fetch_regulationsgov_attachments.py fetch \
  --input-artifact ./runs/<run-id>/raw/round-001/direct-fetch/comment-details.jsonl \
  --max-attachments 20 \
  --dry-run \
  --pretty
```

2. Fetch metadata and files.

```bash
python3 scripts/fetch_regulationsgov_attachments.py fetch \
  --input-artifact ./runs/<run-id>/raw/round-001/direct-fetch/comment-details.jsonl \
  --max-attachments 20 \
  --output-dir ./runs/<run-id>/raw/round-001/direct-fetch/regulationsgov-attachments \
  --manifest-output ./runs/<run-id>/raw/round-001/direct-fetch/regulationsgov-attachments/manifest.json \
  --pretty
```

## Agent Reasoning Guide
- This skill fetches attachment metadata and files only. It does not extract PDF
  text, classify issues, infer stance, judge importance, or decide whether a
  formal comment corpus is sufficient.
- A missing file URL, failed download, or scanned/opaque file is not proof that
  the attachment has no relevant content. Record the limitation and use a text
  extraction or OCR route if the council needs readable text.
- Preserve the upstream comment detail or candidate audit artifact reference so
  later agents can audit why these attachments were selected.

## References
- `references/regulationsgov-attachments-api-notes.md`

## Script
- `scripts/fetch_regulationsgov_attachments.py`
