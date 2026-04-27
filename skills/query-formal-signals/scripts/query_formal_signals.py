#!/usr/bin/env python3
"""Query compact formal signals from a local signal-plane SQLite file."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "query-formal-signals"
VALID_ROUND_SCOPES = ("current", "up-to-current", "all")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.source_queue_history import discovered_round_ids  # noqa: E402
from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    ensure_signal_plane_schema,
    resolved_canonical_object_kind,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def truncate_text(value: Any, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return run_dir / "analytics" / "signal_plane.sqlite"
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str) -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    ensure_signal_plane_schema(connection)
    return connection, file_path


def normalize_round_scope(round_scope: str) -> str:
    scope = maybe_text(round_scope) or "current"
    if scope not in VALID_ROUND_SCOPES:
        raise ValueError(f"Unsupported --round-scope {scope!r}. Expected one of {', '.join(VALID_ROUND_SCOPES)}.")
    return scope


def observed_round_ids(connection: sqlite3.Connection, *, run_id: str, plane: str) -> list[str]:
    rows = connection.execute(
        """
        SELECT
            round_id,
            MIN(
                COALESCE(
                    NULLIF(captured_at_utc, ''),
                    NULLIF(published_at_utc, ''),
                    NULLIF(observed_at_utc, ''),
                    NULLIF(window_start_utc, ''),
                    NULLIF(window_end_utc, ''),
                    signal_id
                )
            ) AS first_seen
        FROM normalized_signals
        WHERE run_id = ? AND plane = ?
        GROUP BY round_id
        ORDER BY first_seen, round_id
        """,
        (run_id, plane),
    ).fetchall()
    return [maybe_text(row["round_id"]) for row in rows if maybe_text(row["round_id"])]


def ordered_round_ids(run_dir: Path, connection: sqlite3.Connection, *, run_id: str, plane: str, current_round_id: str) -> list[str]:
    ordered = discovered_round_ids(run_dir)
    for round_id in observed_round_ids(connection, run_id=run_id, plane=plane):
        if round_id not in ordered:
            ordered.append(round_id)
    current = maybe_text(current_round_id)
    if current and current not in ordered:
        ordered.append(current)
    return ordered


def query_round_ids(
    run_dir: Path,
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plane: str,
    current_round_id: str,
    round_scope: str,
) -> tuple[str, list[str]]:
    scope = normalize_round_scope(round_scope)
    current = maybe_text(current_round_id)
    if scope == "current":
        return scope, [current]
    ordered = ordered_round_ids(run_dir, connection, run_id=run_id, plane=plane, current_round_id=current)
    if scope == "all":
        return scope, ordered
    if current not in ordered:
        ordered.append(current)
    return scope, ordered[: ordered.index(current) + 1]


def fetch_rows(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plane: str,
    round_ids: list[str],
    signal_ids: set[str] | None = None,
) -> list[sqlite3.Row]:
    selected_round_ids = [round_id for round_id in round_ids if maybe_text(round_id)]
    if not selected_round_ids:
        return []
    clauses = ["plane = ?", "run_id = ?"]
    params: list[Any] = [plane, run_id]
    round_placeholders = ",".join("?" for _ in selected_round_ids)
    clauses.append(f"round_id IN ({round_placeholders})")
    params.extend(selected_round_ids)
    if signal_ids is not None:
        selected_signal_ids = sorted(
            signal_id for signal_id in signal_ids if maybe_text(signal_id)
        )
        if not selected_signal_ids:
            return []
        signal_placeholders = ",".join("?" for _ in selected_signal_ids)
        clauses.append(f"signal_id IN ({signal_placeholders})")
        params.extend(selected_signal_ids)
    return connection.execute(
        f"""
        SELECT *
        FROM normalized_signals
        WHERE {" AND ".join(clauses)}
        ORDER BY COALESCE(published_at_utc, captured_at_utc) DESC, signal_id
        """,
        params,
    ).fetchall()


def metadata_dict(row: sqlite3.Row) -> dict[str, Any]:
    try:
        payload = json.loads(row["metadata_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def metadata_field(row: sqlite3.Row, key: str) -> str:
    return maybe_text(metadata_dict(row).get(key))


def metadata_list_field(row: sqlite3.Row, key: str) -> list[str]:
    values = metadata_dict(row).get(key)
    if not isinstance(values, list):
        return []
    return unique_texts(values)


def indexed_field_filters(
    *,
    docket_id: str,
    agency_id: str,
    submitter_type: str,
    issue_labels: list[str],
    concern_facets: list[str],
    citation_types: list[str],
    stance_hint: str,
    route_hint: str,
) -> dict[str, list[str]]:
    filters: dict[str, list[str]] = {}
    if maybe_text(docket_id):
        filters["docket_id"] = [maybe_text(docket_id)]
    if maybe_text(agency_id):
        filters["agency_id"] = [maybe_text(agency_id)]
    if maybe_text(submitter_type):
        filters["submitter_type"] = [maybe_text(submitter_type)]
    if issue_labels:
        filters["issue_labels"] = unique_texts(issue_labels)
    if concern_facets:
        filters["concern_facets"] = unique_texts(concern_facets)
    if citation_types:
        filters["evidence_citation_types"] = unique_texts(citation_types)
    if maybe_text(stance_hint):
        filters["stance_hint"] = [maybe_text(stance_hint)]
    if maybe_text(route_hint):
        filters["route_hint"] = [maybe_text(route_hint)]
    return filters


def query_signal_ids_from_index(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    plane: str,
    round_ids: list[str],
    field_filters: dict[str, list[str]],
) -> set[str] | None:
    selected_round_ids = [round_id for round_id in round_ids if maybe_text(round_id)]
    if not selected_round_ids or not field_filters:
        return None

    round_placeholders = ",".join("?" for _ in selected_round_ids)
    matched_signal_ids: set[str] | None = None
    for field_name, values in sorted(field_filters.items()):
        selected_values = unique_texts(values)
        if not selected_values:
            continue
        value_placeholders = ",".join("?" for _ in selected_values)
        rows = connection.execute(
            f"""
            SELECT DISTINCT signal_id
            FROM normalized_signal_index
            WHERE run_id = ?
              AND plane = ?
              AND round_id IN ({round_placeholders})
              AND field_name = ?
              AND field_value IN ({value_placeholders})
            """,
            (run_id, plane, *selected_round_ids, field_name, *selected_values),
        ).fetchall()
        signal_ids = {
            maybe_text(row["signal_id"])
            for row in rows
            if maybe_text(row["signal_id"])
        }
        matched_signal_ids = (
            signal_ids
            if matched_signal_ids is None
            else matched_signal_ids & signal_ids
        )
        if not matched_signal_ids:
            return set()
    return matched_signal_ids


def metadata_contains_any(row: sqlite3.Row, key: str, values: list[str]) -> bool:
    expected = {maybe_text(value) for value in values if maybe_text(value)}
    if not expected:
        return True
    return bool(expected & set(metadata_list_field(row, key)))


def keyword_match(row: sqlite3.Row, keywords: list[str]) -> bool:
    if not keywords:
        return True
    metadata = metadata_dict(row)
    haystack = " ".join(
        part
        for part in (
            maybe_text(row["title"]),
            maybe_text(row["body_text"]),
            maybe_text(row["url"]),
            maybe_text(row["author_name"]),
            maybe_text(row["channel_name"]),
            maybe_text(metadata.get("docket_id")),
            maybe_text(metadata.get("agency_id")),
            maybe_text(metadata.get("submitter_name")),
            maybe_text(metadata.get("submitter_type")),
            maybe_text(metadata.get("stance_hint")),
            maybe_text(metadata.get("route_hint")),
            " ".join(unique_texts(metadata.get("issue_labels", [])))
            if isinstance(metadata.get("issue_labels"), list)
            else "",
            " ".join(unique_texts(metadata.get("issue_terms", [])))
            if isinstance(metadata.get("issue_terms"), list)
            else "",
            " ".join(unique_texts(metadata.get("concern_facets", [])))
            if isinstance(metadata.get("concern_facets"), list)
            else "",
            " ".join(unique_texts(metadata.get("evidence_citation_types", [])))
            if isinstance(metadata.get("evidence_citation_types"), list)
            else "",
        )
        if part
    ).casefold()
    return any(keyword.casefold() in haystack for keyword in keywords if maybe_text(keyword))


def compact_result(row: sqlite3.Row) -> dict[str, Any]:
    artifact_ref = f"{row['artifact_path']}:{row['record_locator']}"
    metadata = metadata_dict(row)
    return {
        "signal_id": row["signal_id"],
        "round_id": maybe_text(row["round_id"]),
        "source_skill": maybe_text(row["source_skill"]),
        "signal_kind": maybe_text(row["signal_kind"]),
        "canonical_object_kind": resolved_canonical_object_kind(
            plane="formal",
            source_skill=maybe_text(row["source_skill"]),
            signal_kind=maybe_text(row["signal_kind"]),
            canonical_object_kind=maybe_text(row["canonical_object_kind"]),
        ),
        "title": maybe_text(row["title"]),
        "snippet": truncate_text(row["body_text"], 240),
        "author_name": maybe_text(row["author_name"]),
        "submitter_name": maybe_text(metadata.get("submitter_name"))
        or maybe_text(row["author_name"]),
        "submitter_type": maybe_text(metadata.get("submitter_type")),
        "agency_id": maybe_text(metadata.get("agency_id")),
        "docket_id": maybe_text(metadata.get("docket_id")),
        "issue_labels": unique_texts(metadata.get("issue_labels", []))
        if isinstance(metadata.get("issue_labels"), list)
        else [],
        "issue_terms": unique_texts(metadata.get("issue_terms", []))
        if isinstance(metadata.get("issue_terms"), list)
        else [],
        "stance_hint": maybe_text(metadata.get("stance_hint")),
        "concern_facets": unique_texts(metadata.get("concern_facets", []))
        if isinstance(metadata.get("concern_facets"), list)
        else [],
        "evidence_citation_types": unique_texts(
            metadata.get("evidence_citation_types", [])
        )
        if isinstance(metadata.get("evidence_citation_types"), list)
        else [],
        "route_hint": maybe_text(metadata.get("route_hint")),
        "route_status_hint": maybe_text(metadata.get("route_status_hint")),
        "decision_source": maybe_text(metadata.get("decision_source")),
        "typing_method": maybe_text(metadata.get("typing_method")),
        "published_at_utc": maybe_text(row["published_at_utc"]),
        "artifact_ref": artifact_ref,
    }


def artifact_ref(row: sqlite3.Row) -> dict[str, str]:
    return {
        "signal_id": row["signal_id"],
        "round_id": maybe_text(row["round_id"]),
        "artifact_path": maybe_text(row["artifact_path"]),
        "record_locator": maybe_text(row["record_locator"]),
        "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}",
    }


def query_formal_signals_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str,
    db_path: str,
    source_skill: str,
    signal_kind: str,
    published_after_utc: str,
    published_before_utc: str,
    docket_id: str,
    agency_id: str,
    submitter_type: str,
    issue_labels: list[str],
    concern_facets: list[str],
    citation_types: list[str],
    stance_hint: str,
    route_hint: str,
    keyword_any: list[str],
    limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        resolved_round_scope, selected_round_ids = query_round_ids(
            run_dir_path,
            connection,
            run_id=run_id,
            plane="formal",
            current_round_id=round_id,
            round_scope=round_scope,
        )
        field_filters = indexed_field_filters(
            docket_id=docket_id,
            agency_id=agency_id,
            submitter_type=submitter_type,
            issue_labels=issue_labels,
            concern_facets=concern_facets,
            citation_types=citation_types,
            stance_hint=stance_hint,
            route_hint=route_hint,
        )
        matched_signal_ids = query_signal_ids_from_index(
            connection,
            run_id=run_id,
            plane="formal",
            round_ids=selected_round_ids,
            field_filters=field_filters,
        )
        rows = fetch_rows(
            connection,
            run_id=run_id,
            plane="formal",
            round_ids=selected_round_ids,
            signal_ids=matched_signal_ids,
        )
    finally:
        connection.close()
    filtered: list[sqlite3.Row] = []
    for row in rows:
        if maybe_text(source_skill) and maybe_text(row["source_skill"]) != maybe_text(source_skill):
            continue
        if maybe_text(signal_kind) and maybe_text(row["signal_kind"]) != maybe_text(signal_kind):
            continue
        published = maybe_text(row["published_at_utc"])
        if maybe_text(published_after_utc) and published and published < maybe_text(published_after_utc):
            continue
        if maybe_text(published_before_utc) and published and published > maybe_text(published_before_utc):
            continue
        if maybe_text(docket_id) and metadata_field(row, "docket_id") != maybe_text(docket_id):
            continue
        if maybe_text(agency_id) and metadata_field(row, "agency_id") != maybe_text(agency_id):
            continue
        if maybe_text(submitter_type) and metadata_field(row, "submitter_type") != maybe_text(submitter_type):
            continue
        if issue_labels and not metadata_contains_any(row, "issue_labels", issue_labels):
            continue
        if concern_facets and not metadata_contains_any(row, "concern_facets", concern_facets):
            continue
        if citation_types and not metadata_contains_any(row, "evidence_citation_types", citation_types):
            continue
        if maybe_text(stance_hint) and metadata_field(row, "stance_hint") != maybe_text(stance_hint):
            continue
        if maybe_text(route_hint) and metadata_field(row, "route_hint") != maybe_text(route_hint):
            continue
        if not keyword_match(row, keyword_any):
            continue
        filtered.append(row)
    limited = filtered[: max(1, limit)]
    refs = [artifact_ref(row) for row in limited]
    matched_round_ids = unique_texts([row["round_id"] for row in filtered])
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "round_scope": resolved_round_scope,
            "queried_round_ids": selected_round_ids,
            "matched_round_ids": matched_round_ids,
            "result_count": len(limited),
            "indexed_filter_fields": sorted(field_filters.keys()),
            "db_path": str(db_file),
        },
        "result_count": len(limited),
        "results": [compact_result(row) for row in limited],
        "artifact_refs": refs,
        "warnings": [] if limited else [{"code": "no-results", "message": "No formal signals matched the supplied filters."}],
        "board_handoff": {
            "candidate_ids": [row["signal_id"] for row in limited],
            "evidence_refs": refs,
            "gap_hints": [] if limited else ["Formal record coverage is empty for the current filter set."],
            "challenge_hints": ["Check whether docket-level formal submissions are being conflated with broader public discourse patterns."] if limited else [],
            "suggested_next_skills": [
                "query-normalized-signal",
                "link-formal-comments-to-public-discourse",
                "identify-representation-gaps",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query compact formal signals from a local signal-plane SQLite file.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--round-scope", default="current", choices=VALID_ROUND_SCOPES)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--signal-kind", default="")
    parser.add_argument("--published-after-utc", default="")
    parser.add_argument("--published-before-utc", default="")
    parser.add_argument("--docket-id", default="")
    parser.add_argument("--agency-id", default="")
    parser.add_argument("--submitter-type", default="")
    parser.add_argument("--issue-label", action="append", default=[])
    parser.add_argument("--concern-facet", action="append", default=[])
    parser.add_argument("--citation-type", action="append", default=[])
    parser.add_argument("--stance-hint", default="")
    parser.add_argument("--route-hint", default="")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = query_formal_signals_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        round_scope=args.round_scope,
        db_path=args.db_path,
        source_skill=args.source_skill,
        signal_kind=args.signal_kind,
        published_after_utc=args.published_after_utc,
        published_before_utc=args.published_before_utc,
        docket_id=args.docket_id,
        agency_id=args.agency_id,
        submitter_type=args.submitter_type,
        issue_labels=args.issue_label,
        concern_facets=args.concern_facet,
        citation_types=args.citation_type,
        stance_hint=args.stance_hint,
        route_hint=args.route_hint,
        keyword_any=args.keyword,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
