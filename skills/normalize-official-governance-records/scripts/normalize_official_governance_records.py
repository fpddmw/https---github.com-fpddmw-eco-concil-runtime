#!/usr/bin/env python3
"""Normalize official governance-record fetch artifacts into formal signal rows."""

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

SKILL_NAME = "normalize-official-governance-records"
PLANE = "formal"
SUPPORTED_SOURCE_SKILLS = {
    "fetch-epa-eis-records",
    "fetch-federal-register-documents",
    "fetch-usbr-project-records",
}


def read_json_or_jsonl(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".jsonl":
        records: list[Any] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if text:
                    records.append(json.loads(text))
        return {"records": records, "artifact_format": "jsonl"}
    payload = read_json(path)
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"records": payload, "artifact_format": "json-array"}
    return {"records": [], "artifact_format": "unsupported"}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [maybe_text(item) for item in value if maybe_text(item)]
    text = maybe_text(value)
    return [text] if text else []


def first_text(value: Any) -> str:
    values = text_list(value)
    return values[0] if values else ""


def record_source_skill(record: dict[str, Any]) -> str:
    source_skill = maybe_text(record.get("source_skill"))
    return source_skill if source_skill in SUPPORTED_SOURCE_SKILLS else ""


def source_skill_for_payload(payload: dict[str, Any], records: list[Any]) -> tuple[str, list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    source_skill = maybe_text(payload.get("source_skill"))
    if source_skill in SUPPORTED_SOURCE_SKILLS:
        return source_skill, warnings

    observed = sorted(
        {
            record_source_skill(record)
            for record in records
            if isinstance(record, dict) and record_source_skill(record)
        }
    )
    if len(observed) == 1:
        return observed[0], warnings
    if len(observed) > 1:
        warnings.append(
            {
                "code": "mixed-source-skills",
                "message": "Artifact contains multiple supported source_skill values; normalized rows will use the first observed source skill for cleanup scope.",
            }
        )
        return observed[0], warnings

    warnings.append(
        {
            "code": "unknown-source-skill",
            "message": "Artifact did not declare a supported official governance source skill.",
        }
    )
    return "fetch-official-governance-records", warnings


def provider_name(record: dict[str, Any], source_skill: str) -> str:
    provider = maybe_text(record.get("record_source"))
    if provider:
        return provider
    if source_skill == "fetch-federal-register-documents":
        return "FederalRegister.gov"
    if source_skill == "fetch-epa-eis-records":
        return "EPA EIS Database"
    if source_skill == "fetch-usbr-project-records":
        return "Bureau of Reclamation"
    return "Official governance record"


def agency_id(record: dict[str, Any], source_skill: str) -> str:
    explicit = maybe_text(record.get("agency_id"))
    if explicit:
        return explicit
    if source_skill == "fetch-usbr-project-records":
        return "USBR"
    if source_skill == "fetch-epa-eis-records":
        return "EPA-EIS"
    return first_text(record.get("agency_names")) or maybe_text(record.get("agency"))


def docket_id(record: dict[str, Any]) -> str:
    return first_text(record.get("docket_ids")) or maybe_text(record.get("docket_id"))


def published_date(record: dict[str, Any]) -> str:
    return (
        maybe_text(record.get("publication_date"))
        or maybe_text(record.get("published_at"))
        or maybe_text(record.get("updated_at"))
    )


def record_url(record: dict[str, Any]) -> str:
    return (
        maybe_text(record.get("url"))
        or maybe_text(record.get("document_url"))
        or maybe_text(record.get("pdf_url"))
        or maybe_text(record.get("raw_text_url"))
    )


def body_text(record: dict[str, Any]) -> str:
    parts = [
        maybe_text(record.get("summary")),
        maybe_text(record.get("citation")),
        maybe_text(record.get("document_type")),
    ]
    return "\n".join(part for part in parts if part)


def quality_flags(record: dict[str, Any], source_skill: str) -> list[str]:
    flags = ["formal-record", "official-governance-record", "provider-field-normalized"]
    if not body_text(record):
        flags.append("missing-record-summary")
    if not docket_id(record):
        flags.append("missing-docket-id")
    if not agency_id(record, source_skill):
        flags.append("missing-agency-id")
    if not record_url(record):
        flags.append("missing-record-url")
    if maybe_text(record.get("record_type")) == "usbr_linked_document":
        flags.append("linked-record-not-fetched")
    return flags


def provider_metadata(record: dict[str, Any], source_skill: str, artifact_sha256: str) -> dict[str, Any]:
    source_provider = provider_name(record, source_skill)
    provider_record = record.get("provider_record") if isinstance(record.get("provider_record"), dict) else {}
    return {
        "decision_source": "provider-field-normalization",
        "normalization_scope": "official-governance-record-metadata-only",
        "typed_metadata_status": "not-derived-by-normalizer",
        "provider": source_provider,
        "agency_id": agency_id(record, source_skill),
        "agency_names": text_list(record.get("agency_names")),
        "docket_id": docket_id(record),
        "docket_ids": text_list(record.get("docket_ids")),
        "record_type": maybe_text(record.get("record_type")),
        "document_type": maybe_text(record.get("document_type")),
        "citation": maybe_text(record.get("citation")),
        "publication_date": maybe_text(record.get("publication_date")),
        "comment_period": record.get("comment_period") if isinstance(record.get("comment_period"), dict) else {},
        "ceq_number": maybe_text(record.get("ceq_number")),
        "unique_identification_number": maybe_text(record.get("unique_identification_number")),
        "lead_agency": maybe_text(record.get("lead_agency")),
        "federal_cooperating_agencies": maybe_text(record.get("federal_cooperating_agencies")),
        "epa_comment_letter_date": maybe_text(record.get("epa_comment_letter_date")),
        "source_page_url": maybe_text(record.get("source_page_url")),
        "provider_record_keys": sorted(provider_record.keys()),
        "source_provenance": {
            "source_skill": source_skill,
            "provider": source_provider,
            "artifact_sha256": artifact_sha256,
        },
    }


def build_signals(
    payload: dict[str, Any],
    run_id: str,
    round_id: str,
    artifact_file: Path,
    artifact_sha256: str,
) -> tuple[str, list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    records = payload.get("records") if isinstance(payload.get("records"), list) else []
    source_skill, source_warnings = source_skill_for_payload(payload, records)
    warnings.extend(source_warnings)
    captured_at = utc_now_iso()
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        record_id = maybe_text(record.get("record_id")) or maybe_text(record.get("id")) or f"official-governance-record-{index}"
        title = maybe_text(record.get("title")) or record_id
        url = record_url(record)
        dedupe_key = f"{source_skill}:{record_id}"
        signal_id = "sig-" + stable_hash(run_id, round_id, source_skill, artifact_sha256, record_id)[:16]
        signals.append(
            base_signal(
                signal_id=signal_id,
                run_id=run_id,
                round_id=round_id,
                plane=PLANE,
                source_skill=source_skill,
                signal_kind="official-governance-record",
                canonical_object_kind="formal-comment-signal",
                external_id=record_id,
                dedupe_key=dedupe_key,
                title=title,
                body_text=body_text(record),
                url=url,
                author_name=agency_id(record, source_skill),
                channel_name=provider_name(record, source_skill),
                language="",
                query_text="",
                metric="",
                numeric_value=None,
                unit="",
                published_at_utc=published_date(record),
                observed_at_utc="",
                window_start_utc="",
                window_end_utc="",
                captured_at_utc=captured_at,
                latitude=None,
                longitude=None,
                quality_flags=quality_flags(record, source_skill),
                engagement={},
                metadata=provider_metadata(record, source_skill, artifact_sha256),
                raw_record=record,
                artifact_path=artifact_file,
                record_locator=f"$.records[{index}]",
                artifact_sha256=artifact_sha256,
            )
        )
    if not signals:
        warnings.append(
            {
                "code": "no-signals",
                "message": "No official governance records produced normalized signals.",
            }
        )
    return source_skill, signals, warnings


def normalize_official_governance_records(
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_path: str,
    db_path: str,
) -> dict[str, Any]:
    artifact_file = Path(artifact_path).expanduser().resolve()
    artifact_payload = read_json_or_jsonl(artifact_file)
    artifact_sha256 = file_sha256(artifact_file)
    source_skill, signals, warnings = build_signals(
        artifact_payload,
        run_id,
        round_id,
        artifact_file,
        artifact_sha256,
    )
    return finalize_normalization(
        skill_name=SKILL_NAME,
        source_skill=source_skill,
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
    parser = argparse.ArgumentParser(description="Normalize official governance-record artifacts into formal signal rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_official_governance_records(
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
