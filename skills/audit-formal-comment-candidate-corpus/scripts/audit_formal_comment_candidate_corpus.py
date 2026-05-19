#!/usr/bin/env python3
"""Audit a formal comment candidate corpus without judging stance or sufficiency."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.optional_analysis import pretty_json  # noqa: E402
from eco_council_runtime.optional_analysis.support import (  # noqa: E402
    artifact_ref,
    helper_metadata,
    resolve_output_path,
    resolve_run_dir,
    stable_hash,
    utc_now_iso,
    write_json,
)

SKILL_NAME = "audit-formal-comment-candidate-corpus"
SOURCE_SKILL = "fetch-regulationsgov-comments"


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def list_items(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def read_json_or_jsonl(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".jsonl":
        records: list[Any] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if text:
                    records.append(json.loads(text))
        return {"records": records, "artifact_format": "jsonl"}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"records": payload, "artifact_format": "json-array"}
    return {"records": [], "artifact_format": "unsupported"}


def normalize_keywords(values: list[str]) -> list[str]:
    results: list[str] = []
    for value in values:
        for token in str(value).split(","):
            text = maybe_text(token).lower()
            if text and text not in results:
                results.append(text)
    return results


def comment_id(record: dict[str, Any], index: int) -> str:
    return maybe_text(record.get("id")) or maybe_text(record.get("comment_id")) or f"candidate-{index}"


def attributes_for_record(record: dict[str, Any]) -> dict[str, Any]:
    if isinstance(record.get("attributes"), dict):
        return record["attributes"]
    detail = dict_items(record.get("detail"))
    return dict_items(detail.get("attributes"))


def text_fields(attributes: dict[str, Any]) -> str:
    return " ".join(
        maybe_text(attributes.get(key))
        for key in ("title", "comment", "commentText", "summary", "commentOnDocumentTitle")
        if maybe_text(attributes.get(key))
    ).lower()


def record_snapshot(record: dict[str, Any], index: int) -> dict[str, Any]:
    attributes = attributes_for_record(record)
    return {
        "comment_id": comment_id(record, index),
        "title": maybe_text(attributes.get("title")),
        "docket_id": maybe_text(attributes.get("docketId")),
        "comment_on_document_id": maybe_text(attributes.get("commentOnDocumentId"))
        or maybe_text(attributes.get("commentOnId")),
        "agency_id": maybe_text(attributes.get("agencyId")),
        "posted_date": maybe_text(attributes.get("postedDate")),
        "receive_date": maybe_text(attributes.get("receiveDate")),
    }


def load_artifact_records(path: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, str]]]:
    if not path:
        return [], {}, []
    artifact_path = Path(path).expanduser().resolve()
    payload = read_json_or_jsonl(artifact_path)
    warnings: list[dict[str, str]] = []
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        return [], payload, [{"code": "missing-records", "message": "Expected artifact records to be a list."}]
    records = [record for record in raw_records if isinstance(record, dict)]
    if len(records) != len(raw_records):
        warnings.append({"code": "non-object-records-skipped", "message": "Some artifact records were not JSON objects."})
    return records, payload, warnings


def round_ids_for_scope(connection: sqlite3.Connection, run_id: str, round_id: str, round_scope: str) -> list[str]:
    if round_scope == "current":
        return [round_id]
    rows = connection.execute(
        """
        SELECT DISTINCT round_id
        FROM normalized_signals
        WHERE run_id = ? AND round_id <= ?
        ORDER BY round_id
        """,
        (run_id, round_id),
    ).fetchall()
    return [str(row[0]) for row in rows] or [round_id]


def load_db_records(run_dir: Path, run_id: str, round_id: str, round_scope: str) -> tuple[list[dict[str, Any]], str, list[str]]:
    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    if not db_path.exists():
        return [], str(db_path), [round_id]
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        selected_round_ids = round_ids_for_scope(connection, run_id, round_id, round_scope)
        placeholders = ",".join("?" for _ in selected_round_ids)
        rows = connection.execute(
            f"""
            SELECT signal_id, external_id, title, body_text, published_at_utc, metadata_json
            FROM normalized_signals
            WHERE run_id = ?
              AND round_id IN ({placeholders})
              AND plane = 'formal'
              AND source_skill = ?
              AND signal_kind = 'comment-listing'
            ORDER BY published_at_utc, signal_id
            """,
            [run_id, *selected_round_ids, SOURCE_SKILL],
        ).fetchall()
    records: list[dict[str, Any]] = []
    for row in rows:
        metadata = json.loads(row["metadata_json"] or "{}")
        records.append(
            {
                "id": row["external_id"] or row["signal_id"],
                "attributes": {
                    "title": row["title"],
                    "comment": row["body_text"],
                    "postedDate": row["published_at_utc"],
                    "docketId": metadata.get("docket_id"),
                    "commentOnId": metadata.get("comment_on_id"),
                    "agencyId": metadata.get("agency_id"),
                    "submitterName": metadata.get("submitter_name"),
                },
                "signal_id": row["signal_id"],
            }
        )
    return records, str(db_path), selected_round_ids


def audit_records(
    records: list[dict[str, Any]],
    *,
    docket_id: str,
    comment_on_document_id: str,
    agency_id: str,
    keywords: list[str],
    sample_ref_limit: int,
) -> dict[str, Any]:
    missing_docket_count = 0
    missing_comment_on_count = 0
    exact_docket_match_count = 0
    exact_document_match_count = 0
    agency_match_count = 0
    title_keyword_match_count = 0
    eligible: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    seen_fingerprints: dict[str, int] = {}

    for index, record in enumerate(records):
        attributes = attributes_for_record(record)
        snapshot = record_snapshot(record, index)
        reasons: list[str] = []
        if not snapshot["docket_id"]:
            missing_docket_count += 1
            if docket_id:
                reasons.append("missing-docket-id")
        elif docket_id and snapshot["docket_id"] == docket_id:
            exact_docket_match_count += 1
        elif docket_id:
            reasons.append("docket-mismatch")

        if not snapshot["comment_on_document_id"]:
            missing_comment_on_count += 1
            if comment_on_document_id:
                reasons.append("missing-comment-on-document-id")
        elif comment_on_document_id and snapshot["comment_on_document_id"] == comment_on_document_id:
            exact_document_match_count += 1
        elif comment_on_document_id:
            reasons.append("comment-on-document-mismatch")

        if agency_id:
            if snapshot["agency_id"] == agency_id:
                agency_match_count += 1
            else:
                reasons.append("agency-mismatch")

        searchable = text_fields(attributes)
        if keywords:
            if any(keyword in searchable for keyword in keywords):
                title_keyword_match_count += 1
            else:
                reasons.append("keyword-miss")

        fingerprint = "|".join(
            [
                maybe_text(attributes.get("title")).lower(),
                maybe_text(attributes.get("comment") or attributes.get("commentText")).lower()[:200],
            ]
        )
        if fingerprint:
            seen_fingerprints[fingerprint] = seen_fingerprints.get(fingerprint, 0) + 1

        if reasons:
            excluded.append({"candidate": snapshot, "reasons": reasons})
        else:
            eligible.append(snapshot)

    duplicate_count = sum(count for count in seen_fingerprints.values() if count > 1)
    drift_indicators: list[dict[str, Any]] = []
    if docket_id and missing_docket_count:
        drift_indicators.append({"code": "missing-docket-id", "count": missing_docket_count})
    if docket_id and exact_docket_match_count < len(records):
        drift_indicators.append({"code": "docket-mismatch-or-missing", "count": len(records) - exact_docket_match_count})
    if comment_on_document_id and exact_document_match_count < len(records):
        drift_indicators.append({"code": "comment-on-document-mismatch-or-missing", "count": len(records) - exact_document_match_count})
    if keywords and title_keyword_match_count < len(records):
        drift_indicators.append({"code": "keyword-miss", "count": len(records) - title_keyword_match_count})
    if duplicate_count:
        drift_indicators.append({"code": "duplicate-or-mass-campaign-cue", "count": duplicate_count})

    return {
        "candidate_comment_count": len(records),
        "eligible_count": len(eligible),
        "excluded_count": len(excluded),
        "missing_docket_count": missing_docket_count,
        "missing_comment_on_count": missing_comment_on_count,
        "exact_docket_match_count": exact_docket_match_count,
        "exact_document_match_count": exact_document_match_count,
        "agency_match_count": agency_match_count,
        "title_keyword_match_count": title_keyword_match_count,
        "duplicate_or_mass_campaign_count": duplicate_count,
        "candidate_ids": [item["comment_id"] for item in eligible[:sample_ref_limit]],
        "candidate_id_samples": eligible[:sample_ref_limit],
        "exclusion_reason_samples": excluded[:sample_ref_limit],
        "field_coverage": {
            "records_with_docket_id": len(records) - missing_docket_count,
            "records_with_comment_on_document_id": len(records) - missing_comment_on_count,
            "records_with_agency_id": sum(1 for index, record in enumerate(records) if record_snapshot(record, index)["agency_id"]),
            "records_with_title": sum(1 for record in records if maybe_text(attributes_for_record(record).get("title"))),
            "records_with_comment_text": sum(
                1
                for record in records
                if maybe_text(attributes_for_record(record).get("comment") or attributes_for_record(record).get("commentText"))
            ),
        },
        "likely_drift_indicators": drift_indicators,
        "sample_ref_limit": sample_ref_limit,
    }


def audit_formal_comment_candidate_corpus(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_path: str = "",
    docket_id: str = "",
    comment_on_document_id: str = "",
    agency_id: str = "",
    keywords: list[str] | None = None,
    round_scope: str = "current",
    output_path: str = "",
    sample_ref_limit: int = 25,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(
        run_dir_path,
        output_path,
        f"formal_comment_candidate_corpus_audit_{round_id}.json",
    )
    artifact_records, artifact_payload, warnings = load_artifact_records(artifact_path)
    db_records: list[dict[str, Any]] = []
    db_path = str(run_dir_path / "analytics" / "signal_plane.sqlite")
    selected_round_ids = [round_id]
    if not artifact_records:
        db_records, db_path, selected_round_ids = load_db_records(run_dir_path, run_id, round_id, round_scope)
    records = artifact_records or db_records
    normalized_keywords = normalize_keywords(keywords or [])
    audit = audit_records(
        records,
        docket_id=maybe_text(docket_id),
        comment_on_document_id=maybe_text(comment_on_document_id),
        agency_id=maybe_text(agency_id),
        keywords=normalized_keywords,
        sample_ref_limit=max(0, sample_ref_limit),
    )
    metadata = helper_metadata(
        skill_name=SKILL_NAME,
        rule_trace=["formal-comment-candidate-corpus-audit"],
        caveats=[
            "Candidate corpus audit describes sample shape only.",
            "It does not judge stance, importance, sufficiency, source ranking, or whether to close investigation.",
            "Formal comment samples must not be converted into public-opinion distributions.",
        ],
    )
    audit_id = "formalcand-" + stable_hash(run_id, round_id, artifact_path, audit["candidate_comment_count"])[:12]
    payload = {
        "schema_version": "optional-analysis-formal-comment-candidate-corpus-audit-v1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "audit": {
            "audit_id": audit_id,
            "run_id": run_id,
            "round_id": round_id,
            "helper_governance": metadata,
            "query_parameters": {
                "docket_id": maybe_text(docket_id),
                "comment_on_document_id": maybe_text(comment_on_document_id),
                "agency_id": maybe_text(agency_id),
                "keywords": normalized_keywords,
                "round_scope": round_scope,
            },
            "source_parameters": {
                "artifact_path": str(Path(artifact_path).expanduser().resolve()) if artifact_path else "",
                "db_path": db_path,
                "queried_round_ids": selected_round_ids,
                "input_source": "artifact" if artifact_records else "signal-plane-db",
                "artifact_source": artifact_payload.get("source") if isinstance(artifact_payload, dict) else "",
            },
            "source_limitations": [
                "Regulations.gov list rows may omit full text and attachment content.",
                "Candidate corpus audit cannot establish stance distribution or representativeness.",
                "Formal comment samples must not be converted into general public-opinion distributions.",
                "Zero or excluded rows may reflect filters, API limits, list-surface drift, or normalization scope.",
            ],
            **audit,
            "warnings": warnings,
        },
        "observed_inputs": {
            "artifact_record_count": len(artifact_records),
            "db_record_count": len(db_records),
            "candidate_comment_count": audit["candidate_comment_count"],
        },
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "audit_id": audit_id,
            "candidate_comment_count": audit["candidate_comment_count"],
            "eligible_count": audit["eligible_count"],
            "excluded_count": audit["excluded_count"],
            "likely_drift_indicator_count": len(audit["likely_drift_indicators"]),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "artifact_refs": [artifact_ref(output_file, "$.audit")],
        "canonical_ids": [audit_id],
        "warnings": warnings,
        "audit": payload["audit"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit a formal comment candidate corpus.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", default="")
    parser.add_argument("--docket-id", default="")
    parser.add_argument("--comment-on-document-id", default="")
    parser.add_argument("--agency-id", default="")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--round-scope", choices=["current", "up-to-current"], default="current")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--sample-ref-limit", type=int, default=25)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    payload = audit_formal_comment_candidate_corpus(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        artifact_path=args.artifact_path,
        docket_id=args.docket_id,
        comment_on_document_id=args.comment_on_document_id,
        agency_id=args.agency_id,
        keywords=args.keyword,
        round_scope=args.round_scope,
        output_path=args.output_path,
        sample_ref_limit=args.sample_ref_limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
