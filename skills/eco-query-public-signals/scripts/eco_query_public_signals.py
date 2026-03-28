#!/usr/bin/env python3
"""Query compact public signals from a local signal-plane SQLite file."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-query-public-signals"

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


def truncate_text(value: Any, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


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


def keyword_match(row: sqlite3.Row, keywords: list[str]) -> bool:
    if not keywords:
        return True
    haystack = " ".join(
        part
        for part in (
            maybe_text(row["title"]),
            maybe_text(row["body_text"]),
            maybe_text(row["url"]),
        )
        if part
    ).casefold()
    return any(keyword.casefold() in haystack for keyword in keywords if maybe_text(keyword))


def compact_result(row: sqlite3.Row) -> dict[str, Any]:
    artifact_ref = f"{row['artifact_path']}:{row['record_locator']}"
    return {
        "signal_id": row["signal_id"],
        "source_skill": maybe_text(row["source_skill"]),
        "signal_kind": maybe_text(row["signal_kind"]),
        "title": maybe_text(row["title"]),
        "snippet": truncate_text(row["body_text"], 240),
        "published_at_utc": maybe_text(row["published_at_utc"]),
        "artifact_ref": artifact_ref,
    }


def artifact_ref(row: sqlite3.Row) -> dict[str, str]:
    return {
        "signal_id": row["signal_id"],
        "artifact_path": maybe_text(row["artifact_path"]),
        "record_locator": maybe_text(row["record_locator"]),
        "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}",
    }


def query_public_signals_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    source_skill: str,
    signal_kind: str,
    published_after_utc: str,
    published_before_utc: str,
    keyword_any: list[str],
    limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        rows = connection.execute(
            "SELECT * FROM normalized_signals WHERE plane = 'public' AND run_id = ? AND round_id = ? ORDER BY COALESCE(published_at_utc, captured_at_utc) DESC, signal_id",
            (run_id, round_id),
        ).fetchall()
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
        if not keyword_match(row, keyword_any):
            continue
        filtered.append(row)
    limited = filtered[: max(1, limit)]
    refs = [artifact_ref(row) for row in limited]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "result_count": len(limited),
            "db_path": str(db_file),
        },
        "result_count": len(limited),
        "results": [compact_result(row) for row in limited],
        "artifact_refs": refs,
        "warnings": [] if limited else [{"code": "no-results", "message": "No public signals matched the supplied filters."}],
        "board_handoff": {
            "candidate_ids": [row["signal_id"] for row in limited],
            "evidence_refs": refs,
            "gap_hints": [] if limited else ["Public evidence coverage is empty for the current filter set."],
            "challenge_hints": ["Check whether repeated public narratives are being overread as independent evidence."] if limited else [],
            "suggested_next_skills": ["eco-lookup-normalized-signal", "eco-extract-claim-candidates"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query compact public signals from a local signal-plane SQLite file.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--signal-kind", default="")
    parser.add_argument("--published-after-utc", default="")
    parser.add_argument("--published-before-utc", default="")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = query_public_signals_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        db_path=args.db_path,
        source_skill=args.source_skill,
        signal_kind=args.signal_kind,
        published_after_utc=args.published_after_utc,
        published_before_utc=args.published_before_utc,
        keyword_any=args.keyword,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())