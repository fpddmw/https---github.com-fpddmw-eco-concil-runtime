#!/usr/bin/env python3
"""Look up one normalized signal from a local signal-plane SQLite file."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any
import sys

SKILL_NAME = "query-normalized-signal"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    ensure_signal_plane_schema,
    resolved_canonical_object_kind,
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


def decode_json(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default


def row_to_result(row: sqlite3.Row, include_raw_json: bool) -> dict[str, Any]:
    result = {
        "signal_id": row["signal_id"],
        "run_id": row["run_id"],
        "round_id": row["round_id"],
        "plane": row["plane"],
        "source_skill": row["source_skill"],
        "signal_kind": row["signal_kind"],
        "canonical_object_kind": resolved_canonical_object_kind(
            plane=maybe_text(row["plane"]),
            source_skill=maybe_text(row["source_skill"]),
            signal_kind=maybe_text(row["signal_kind"]),
            canonical_object_kind=maybe_text(row["canonical_object_kind"]),
        ),
        "title": maybe_text(row["title"]),
        "text": maybe_text(row["body_text"]),
        "url": maybe_text(row["url"]),
        "metric": maybe_text(row["metric"]),
        "value": row["numeric_value"],
        "unit": maybe_text(row["unit"]),
        "published_at_utc": maybe_text(row["published_at_utc"]),
        "observed_at_utc": maybe_text(row["observed_at_utc"]),
        "window_start_utc": maybe_text(row["window_start_utc"]),
        "window_end_utc": maybe_text(row["window_end_utc"]),
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "quality_flags": decode_json(row["quality_flags_json"], []),
        "engagement": decode_json(row["engagement_json"], {}),
        "metadata": decode_json(row["metadata_json"], {}),
        "artifact_path": maybe_text(row["artifact_path"]),
        "record_locator": maybe_text(row["record_locator"]),
        "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}",
    }
    if include_raw_json:
        result["raw_json"] = decode_json(row["raw_json"], None)
    return result


def lookup_normalized_signal_skill(run_dir: str, signal_id: str, db_path: str, include_raw_json: bool) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        row = connection.execute("SELECT * FROM normalized_signals WHERE signal_id = ?", (signal_id,)).fetchone()
    finally:
        connection.close()
    if row is None:
        return {
            "status": "completed",
            "summary": {"skill": SKILL_NAME, "signal_id": signal_id, "result_count": 0, "db_path": str(db_file)},
            "result_count": 0,
            "results": [],
            "artifact_refs": [],
            "warnings": [{"code": "not-found", "message": "The requested signal_id was not found."}],
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": ["No normalized signal was found for the requested id."], "challenge_hints": [], "suggested_next_skills": ["query-public-signals", "query-environment-signals"]},
        }
    result = row_to_result(row, include_raw_json)
    ref = {"signal_id": row["signal_id"], "artifact_path": maybe_text(row["artifact_path"]), "record_locator": maybe_text(row["record_locator"]), "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}"}
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "signal_id": signal_id, "result_count": 1, "db_path": str(db_file)},
        "result_count": 1,
        "results": [result],
        "artifact_refs": [ref],
        "warnings": [],
        "board_handoff": {"candidate_ids": [signal_id], "evidence_refs": [ref], "gap_hints": [], "challenge_hints": [], "suggested_next_skills": ["query-raw-record"]},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Look up one normalized signal from a local signal-plane SQLite file.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--signal-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--include-raw-json", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = lookup_normalized_signal_skill(
        run_dir=args.run_dir,
        signal_id=args.signal_id,
        db_path=args.db_path,
        include_raw_json=args.include_raw_json,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
