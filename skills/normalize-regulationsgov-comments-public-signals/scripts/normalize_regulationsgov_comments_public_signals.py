#!/usr/bin/env python3
"""Normalize fetch-regulationsgov-comments artifacts into formal signal-plane rows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    base_signal,
    file_sha256,
    finalize_normalization,
    maybe_text,
    pretty_json,
    read_json,
    stable_hash,
    utc_now_iso,
)

SKILL_NAME = "normalize-regulationsgov-comments-public-signals"
SOURCE_SKILL = "fetch-regulationsgov-comments"
PLANE = "formal"


def provider_metadata(attributes: dict[str, Any], *, artifact_sha256: str) -> dict[str, Any]:
    submitter_name = (
        maybe_text(attributes.get("submitterName"))
        or maybe_text(attributes.get("organization"))
        or maybe_text(attributes.get("organizationName"))
    )
    return {
        "decision_source": "provider-field-normalization",
        "normalization_scope": "provider-fields-only",
        "typed_metadata_status": "not-derived-by-normalizer",
        "docket_id": maybe_text(attributes.get("docketId")),
        "comment_on_id": maybe_text(attributes.get("commentOnId")),
        "last_modified_date": maybe_text(attributes.get("lastModifiedDate")),
        "agency_id": maybe_text(attributes.get("agencyId")),
        "submitter_name": submitter_name,
        "submitter_organization": maybe_text(attributes.get("organization"))
        or maybe_text(attributes.get("organizationName")),
        "provider": "Regulations.gov",
        "source_provenance": {
            "source_skill": SOURCE_SKILL,
            "provider": "Regulations.gov",
            "artifact_sha256": artifact_sha256,
        },
    }


def quality_flags_for_record(attributes: dict[str, Any], *, body_text: str, author_name: str) -> list[str]:
    flags = ["formal-record", "provider-field-normalized"]
    if not body_text:
        flags.append("missing-comment-text")
    if not maybe_text(attributes.get("docketId")):
        flags.append("missing-docket-id")
    if not maybe_text(attributes.get("agencyId")):
        flags.append("missing-agency-id")
    if not author_name:
        flags.append("missing-submitter-name")
    return flags


def build_signals(payload: Any, run_id: str, round_id: str, artifact_file: Path, artifact_sha256: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        warnings.append({"code": "missing-records", "message": "Expected payload.records to be a list."})
        return [], warnings
    captured_at = utc_now_iso()
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        comment_id = maybe_text(record.get("id")) or f"reggov-comment-{index}"
        attributes = record.get("attributes") if isinstance(record.get("attributes"), dict) else {}
        title = maybe_text(attributes.get("title")) or f"Regulations.gov comment {comment_id}"
        body_text = (
            maybe_text(attributes.get("comment"))
            or maybe_text(attributes.get("commentText"))
            or maybe_text(attributes.get("commentOnDocumentTitle"))
        )
        published_at = maybe_text(attributes.get("postedDate")) or maybe_text(attributes.get("lastModifiedDate"))
        author_name = (
            maybe_text(attributes.get("submitterName"))
            or maybe_text(attributes.get("organization"))
            or maybe_text(attributes.get("organizationName"))
        )
        url = f"https://www.regulations.gov/comment/{comment_id}" if comment_id else ""
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, comment_id)[:16]
        metadata = provider_metadata(attributes, artifact_sha256=artifact_sha256)
        quality_flags = quality_flags_for_record(
            attributes,
            body_text=body_text,
            author_name=author_name,
        )
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=SOURCE_SKILL,
                signal_kind="comment-listing",
                canonical_object_kind="formal-comment-signal",
                external_id=comment_id,
                dedupe_key=comment_id,
                title=title,
                body_text=body_text,
                url=url,
                author_name=author_name,
                channel_name=maybe_text(attributes.get("agencyId")) or maybe_text(attributes.get("docketId")),
                language="",
                query_text="",
                metric="",
                numeric_value=None,
                unit="",
                published_at_utc=published_at,
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
        warnings.append({"code": "no-signals", "message": "No Regulations.gov comment rows produced normalized signals."})
    return signals, warnings


def normalize_regulationsgov_comments(run_dir: str, run_id: str, round_id: str, artifact_path: str, db_path: str) -> dict[str, Any]:
    artifact_file = Path(artifact_path).expanduser().resolve()
    artifact_payload = read_json(artifact_file)
    artifact_sha256 = file_sha256(artifact_file)
    signals, warnings = build_signals(artifact_payload, run_id, round_id, artifact_file, artifact_sha256)
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize fetch-regulationsgov-comments artifacts into formal signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_regulationsgov_comments(
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
