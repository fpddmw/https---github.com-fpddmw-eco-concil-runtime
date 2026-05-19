#!/usr/bin/env python3
"""Normalize extracted Regulations.gov attachment text into formal signals."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.planes.signal import (  # noqa: E402
    base_signal,
    file_sha256,
    finalize_normalization,
    maybe_text,
    pretty_json,
    read_json,
    stable_hash,
    utc_now_iso,
)

SKILL_NAME = "normalize-regulationsgov-attachment-text"
SOURCE_SKILL = "fetch-regulationsgov-attachments"
PLANE = "formal"


def dict_items(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def list_items(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def metadata_attributes(record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    attachment_metadata = dict_items(record.get("metadata"))
    attachment_attrs = dict_items(attachment_metadata.get("attributes"))
    comment_attrs = dict_items(record.get("comment_attributes"))
    return attachment_attrs, comment_attrs


def read_text_artifact(path_text: str) -> str:
    if not path_text:
        return ""
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def is_document_text_extraction_manifest(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if maybe_text(payload.get("schema_version")) == "document-text-extraction-v1":
        return True
    if maybe_text(payload.get("skill")) == "extract-document-text":
        return True
    records = payload.get("records")
    if not isinstance(records, list) or not records:
        return False
    return all(
        isinstance(record, dict)
        and (
            maybe_text(record.get("text_extraction_status"))
            or maybe_text(record.get("output_text_path"))
            or maybe_text(record.get("extracted_text_char_count"))
        )
        for record in records
    )


def build_signals(payload: Any, run_id: str, round_id: str, artifact_file: Path, artifact_sha256: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    if not is_document_text_extraction_manifest(payload):
        warnings.append(
            {
                "code": "expected-document-text-extraction-manifest",
                "message": "Expected an extract-document-text manifest. Run extract-document-text on the attachment fetch manifest before normalization.",
            }
        )
        return [], warnings
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        warnings.append({"code": "missing-records", "message": "Expected payload.records to be a list."})
        return [], warnings
    captured_at = utc_now_iso()
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        attachment_attrs, comment_attrs = metadata_attributes(record)
        attachment_id = maybe_text(record.get("attachment_id")) or maybe_text(dict_items(record.get("metadata")).get("id"))
        comment_id = maybe_text(record.get("comment_id"))
        external_id = attachment_id or maybe_text(record.get("input_path")) or f"attachment-text-{index}"
        title = maybe_text(attachment_attrs.get("title")) or Path(maybe_text(record.get("input_path"))).name or f"Regulations.gov attachment {external_id}"
        body_text = read_text_artifact(maybe_text(record.get("output_text_path")))
        quality_flags = ["formal-record", "provider-field-normalized", "attachment-text"]
        for flag in list_items(record.get("quality_flags")):
            text = maybe_text(flag)
            if text and text not in quality_flags:
                quality_flags.append(text)
        if maybe_text(record.get("text_extraction_status")) != "completed":
            if "text-extraction-limited" not in quality_flags:
                quality_flags.append("text-extraction-limited")
        metadata = {
            "decision_source": "provider-field-normalization",
            "normalization_scope": "attachment-text-only",
            "typed_metadata_status": "not-derived-by-normalizer",
            "comment_id": comment_id,
            "attachment_id": attachment_id,
            "file_url": maybe_text(record.get("file_url")),
            "input_path": maybe_text(record.get("input_path")),
            "output_text_path": maybe_text(record.get("output_text_path")),
            "text_extraction_status": maybe_text(record.get("text_extraction_status")),
            "page_count": record.get("page_count"),
            "extracted_page_count": record.get("extracted_page_count"),
            "empty_page_count": record.get("empty_page_count"),
            "extracted_text_char_count": record.get("extracted_text_char_count"),
            "docket_id": maybe_text(comment_attrs.get("docketId")),
            "comment_on_id": maybe_text(comment_attrs.get("commentOnId")),
            "comment_on_document_id": maybe_text(comment_attrs.get("commentOnDocumentId")) or maybe_text(comment_attrs.get("commentOnId")),
            "agency_id": maybe_text(comment_attrs.get("agencyId")),
            "submitter_name": maybe_text(comment_attrs.get("submitterName")) or maybe_text(comment_attrs.get("organizationName")),
            "provider": "Regulations.gov",
            "source_provenance": {
                "source_skill": SOURCE_SKILL,
                "provider": "Regulations.gov",
                "artifact_sha256": artifact_sha256,
            },
        }
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, external_id)[:16]
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=SOURCE_SKILL,
                signal_kind="attachment-text",
                canonical_object_kind="formal-comment-signal",
                external_id=external_id,
                dedupe_key=external_id,
                title=title,
                body_text=body_text,
                url=maybe_text(record.get("file_url")),
                author_name=metadata["submitter_name"],
                channel_name=metadata["agency_id"] or metadata["docket_id"],
                language="",
                query_text="",
                metric="",
                numeric_value=None,
                unit="",
                published_at_utc="",
                observed_at_utc="",
                window_start_utc="",
                window_end_utc="",
                captured_at_utc=captured_at,
                latitude=None,
                longitude=None,
                quality_flags=quality_flags,
                engagement={},
                metadata=metadata,
                raw_record=record,
                artifact_path=artifact_file,
                record_locator=f"$.records[{index}]",
                artifact_sha256=artifact_sha256,
            )
        )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No attachment text rows produced normalized signals."})
    return signals, warnings


def normalize_attachment_text(run_dir: str, run_id: str, round_id: str, artifact_path: str, db_path: str) -> dict[str, Any]:
    artifact_file = Path(artifact_path).expanduser().resolve()
    payload = read_json(artifact_file)
    artifact_sha256 = file_sha256(artifact_file)
    signals, warnings = build_signals(payload, run_id, round_id, artifact_file, artifact_sha256)
    return finalize_normalization(
        skill_name=SKILL_NAME,
        source_skill=SOURCE_SKILL,
        plane=PLANE,
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_file=artifact_file,
        db_path=db_path,
        signals=signals,
        warnings=warnings,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize Regulations.gov attachment text extraction manifests.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = normalize_attachment_text(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        artifact_path=args.artifact_path,
        db_path=args.db_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
