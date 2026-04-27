#!/usr/bin/env python3
"""Query compact environment signals from a local signal-plane SQLite file."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "query-environment-signals"
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
from eco_council_runtime.kernel.signal_evidence import (  # noqa: E402
    signal_artifact_ref,
    with_signal_evidence_fields,
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    external_id TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    author_name TEXT NOT NULL DEFAULT '',
    channel_name TEXT NOT NULL DEFAULT '',
    language TEXT NOT NULL DEFAULT '',
    query_text TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL DEFAULT '',
    numeric_value REAL,
    unit TEXT NOT NULL DEFAULT '',
    published_at_utc TEXT NOT NULL DEFAULT '',
    observed_at_utc TEXT NOT NULL DEFAULT '',
    window_start_utc TEXT NOT NULL DEFAULT '',
    window_end_utc TEXT NOT NULL DEFAULT '',
    captured_at_utc TEXT NOT NULL DEFAULT '',
    latitude REAL,
    longitude REAL,
    bbox_json TEXT NOT NULL DEFAULT '{}',
    quality_flags_json TEXT NOT NULL DEFAULT '[]',
    engagement_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT 'null',
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL DEFAULT ''
);
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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
                    NULLIF(observed_at_utc, ''),
                    NULLIF(window_start_utc, ''),
                    NULLIF(window_end_utc, ''),
                    NULLIF(published_at_utc, ''),
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


def fetch_rows(connection: sqlite3.Connection, *, run_id: str, plane: str, round_ids: list[str]) -> list[sqlite3.Row]:
    selected_round_ids = [round_id for round_id in round_ids if maybe_text(round_id)]
    if not selected_round_ids:
        return []
    placeholders = ",".join("?" for _ in selected_round_ids)
    return connection.execute(
        f"""
        SELECT *
        FROM normalized_signals
        WHERE plane = ? AND run_id = ? AND round_id IN ({placeholders})
        ORDER BY COALESCE(observed_at_utc, window_start_utc, captured_at_utc) DESC, signal_id
        """,
        (plane, run_id, *selected_round_ids),
    ).fetchall()


def within_bbox(row: sqlite3.Row, bbox: dict[str, float] | None) -> bool:
    if bbox is None:
        return True
    latitude = row["latitude"]
    longitude = row["longitude"]
    if latitude is None or longitude is None:
        return False
    return bbox["south"] <= latitude <= bbox["north"] and bbox["west"] <= longitude <= bbox["east"]


def quality_match(row: sqlite3.Row, wanted: list[str]) -> bool:
    if not wanted:
        return True
    try:
        flags = {maybe_text(flag).casefold() for flag in json.loads(row["quality_flags_json"] or "[]")}
    except json.JSONDecodeError:
        flags = set()
    wanted_flags = {maybe_text(flag).casefold() for flag in wanted if maybe_text(flag)}
    return bool(flags.intersection(wanted_flags))


def compact_result(row: sqlite3.Row) -> dict[str, Any]:
    return with_signal_evidence_fields(
        {
            "signal_id": row["signal_id"],
            "round_id": maybe_text(row["round_id"]),
            "source_skill": maybe_text(row["source_skill"]),
            "signal_kind": maybe_text(row["signal_kind"]),
            "canonical_object_kind": resolved_canonical_object_kind(
                plane="environment",
                source_skill=maybe_text(row["source_skill"]),
                signal_kind=maybe_text(row["signal_kind"]),
                canonical_object_kind=maybe_text(row["canonical_object_kind"]),
            ),
            "metric": maybe_text(row["metric"]),
            "value": row["numeric_value"],
            "unit": maybe_text(row["unit"]),
            "observed_at_utc": maybe_text(row["observed_at_utc"]),
            "location": {"latitude": row["latitude"], "longitude": row["longitude"]},
            "quality_flags": json.loads(row["quality_flags_json"] or "[]"),
        },
        row,
        plane="environment",
    )


def artifact_ref(row: sqlite3.Row) -> dict[str, str]:
    return signal_artifact_ref(row, plane="environment")


def query_environment_signals_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str,
    db_path: str,
    source_skill: str,
    metric: str,
    observed_after_utc: str,
    observed_before_utc: str,
    bbox: dict[str, float] | None,
    quality_flag_any: list[str],
    limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        resolved_round_scope, selected_round_ids = query_round_ids(
            run_dir_path,
            connection,
            run_id=run_id,
            plane="environment",
            current_round_id=round_id,
            round_scope=round_scope,
        )
        rows = fetch_rows(connection, run_id=run_id, plane="environment", round_ids=selected_round_ids)
    finally:
        connection.close()
    filtered: list[sqlite3.Row] = []
    for row in rows:
        if maybe_text(source_skill) and maybe_text(row["source_skill"]) != maybe_text(source_skill):
            continue
        if maybe_text(metric) and maybe_text(row["metric"]) != maybe_text(metric):
            continue
        observed = maybe_text(row["observed_at_utc"] or row["window_start_utc"])
        if maybe_text(observed_after_utc) and observed and observed < maybe_text(observed_after_utc):
            continue
        if maybe_text(observed_before_utc) and observed and observed > maybe_text(observed_before_utc):
            continue
        if not within_bbox(row, bbox):
            continue
        if not quality_match(row, quality_flag_any):
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
            "db_path": str(db_file),
        },
        "result_count": len(limited),
        "results": [compact_result(row) for row in limited],
        "artifact_refs": refs,
        "warnings": [] if limited else [{"code": "no-results", "message": "No environment signals matched the supplied filters."}],
        "board_handoff": {
            "candidate_ids": [row["signal_id"] for row in limited],
            "evidence_refs": refs,
            "gap_hints": [] if limited else ["No environment signal rows matched this DB query; adjust source, metric, time, bbox, or quality filters before filing a finding."],
            "challenge_hints": ["When using environment rows, attach the returned item-level evidence_refs and state provider/model/station limitations."] if limited else [],
            "suggested_next_skills": [
                "query-normalized-signal",
                "query-raw-record",
                "submit-finding-record",
                "submit-evidence-bundle",
                "post-discussion-message",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query compact environment signals from a local signal-plane SQLite file.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--round-scope", default="current", choices=VALID_ROUND_SCOPES)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--metric", default="")
    parser.add_argument("--observed-after-utc", default="")
    parser.add_argument("--observed-before-utc", default="")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    parser.add_argument("--quality-flag", action="append", default=[])
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bbox = None
    if args.bbox is not None:
        bbox = {"west": args.bbox[0], "south": args.bbox[1], "east": args.bbox[2], "north": args.bbox[3]}
    payload = query_environment_signals_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        round_scope=args.round_scope,
        db_path=args.db_path,
        source_skill=args.source_skill,
        metric=args.metric,
        observed_after_utc=args.observed_after_utc,
        observed_before_utc=args.observed_before_utc,
        bbox=bbox,
        quality_flag_any=args.quality_flag,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
