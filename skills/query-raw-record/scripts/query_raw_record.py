#!/usr/bin/env python3
"""Look up one raw record through signal-plane provenance stored in local SQLite."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "query-raw-record"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.signal_evidence import (  # noqa: E402
    signal_artifact_ref,
    signal_evidence_basis,
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
    connection.executescript(SCHEMA_SQL)
    return connection, file_path


def decode_json(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default


def lookup_raw_record_skill(run_dir: str, signal_id: str, artifact_path: str, record_locator: str, db_path: str) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        if maybe_text(signal_id):
            rows = connection.execute("SELECT * FROM normalized_signals WHERE signal_id = ?", (signal_id,)).fetchall()
        elif maybe_text(record_locator):
            rows = connection.execute(
                "SELECT * FROM normalized_signals WHERE artifact_path = ? AND record_locator = ? ORDER BY signal_id",
                (str(Path(artifact_path).expanduser().resolve()), record_locator),
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT * FROM normalized_signals WHERE artifact_path = ? ORDER BY signal_id LIMIT 20",
                (str(Path(artifact_path).expanduser().resolve()),),
            ).fetchall()
    finally:
        connection.close()
    results = [
        {
            "signal_id": row["signal_id"],
            "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}",
            "evidence_refs": [signal_artifact_ref(row)],
            "evidence_basis": signal_evidence_basis(row),
            "raw_record": decode_json(row["raw_json"], None),
        }
        for row in rows
    ]
    refs = [signal_artifact_ref(row) for row in rows]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "lookup_mode": "signal-id" if maybe_text(signal_id) else "artifact-ref",
            "result_count": len(results),
            "db_path": str(db_file),
        },
        "result_count": len(results),
        "results": results,
        "artifact_refs": refs,
        "warnings": [] if results else [{"code": "not-found", "message": "No raw record matched the supplied locator."}],
        "board_handoff": {
            "candidate_ids": [item["signal_id"] for item in results],
            "evidence_refs": refs,
            "gap_hints": [] if results else ["No provenance-linked raw record was found."],
            "challenge_hints": [],
            "suggested_next_skills": [
                "query-normalized-signal",
                "submit-finding-record",
                "submit-evidence-bundle",
                "post-discussion-message",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Look up one raw record through signal-plane provenance stored in local SQLite.")
    parser.add_argument("--run-dir", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--signal-id", default="")
    group.add_argument("--artifact-path", default="")
    parser.add_argument("--record-locator", default="")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = lookup_raw_record_skill(
        run_dir=args.run_dir,
        signal_id=args.signal_id,
        artifact_path=args.artifact_path,
        record_locator=args.record_locator,
        db_path=args.db_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
