#!/usr/bin/env python3
"""Query compact historical signals from the cross-run signal corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-query-signal-corpus"
SEARCH_TOKEN_RE = re.compile(r"[a-z0-9_]{2,}")
SEARCH_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "that",
    "this",
    "these",
    "those",
    "issue",
    "case",
    "archive",
    "signal",
    "signals",
    "round",
    "historical",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS corpus_runs (
    run_id TEXT PRIMARY KEY,
    run_dir TEXT NOT NULL,
    topic TEXT NOT NULL DEFAULT '',
    objective TEXT NOT NULL DEFAULT '',
    region_label TEXT NOT NULL DEFAULT '',
    current_round_id TEXT NOT NULL DEFAULT '',
    round_count INTEGER NOT NULL DEFAULT 0,
    public_signal_count INTEGER NOT NULL DEFAULT 0,
    environment_signal_count INTEGER NOT NULL DEFAULT 0,
    metric_families_json TEXT NOT NULL DEFAULT '[]',
    source_skills_json TEXT NOT NULL DEFAULT '[]',
    mission_json TEXT NOT NULL DEFAULT '{}',
    imported_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corpus_signals (
    archived_signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    metric TEXT NOT NULL DEFAULT '',
    metric_family TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    snippet TEXT NOT NULL DEFAULT '',
    query_text TEXT NOT NULL DEFAULT '',
    numeric_value REAL,
    unit TEXT NOT NULL DEFAULT '',
    published_at_utc TEXT NOT NULL DEFAULT '',
    observed_at_utc TEXT NOT NULL DEFAULT '',
    captured_at_utc TEXT NOT NULL DEFAULT '',
    latitude REAL,
    longitude REAL,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    artifact_ref TEXT NOT NULL DEFAULT '',
    region_label TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}'
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


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_archive_db_path(run_dir: Path) -> Path:
    return (run_dir / ".." / "archives" / "eco_signal_corpus.sqlite").resolve()


def default_output_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "archive" / f"signal_corpus_query_{round_id}.json").resolve()


def resolve_path(run_dir: Path, override: str, default_path: Path) -> Path:
    text = maybe_text(override)
    if not text:
        return default_path
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def connect_archive_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    return connection


def search_terms(*values: Any) -> list[str]:
    tokens: set[str] = set()
    for value in values:
        text = maybe_text(value).casefold()
        if not text:
            continue
        for token in SEARCH_TOKEN_RE.findall(text):
            if token not in SEARCH_STOPWORDS:
                tokens.add(token)
    return sorted(tokens)


def lexical_overlap_score(row: sqlite3.Row, query_terms: list[str]) -> tuple[float, list[str]]:
    haystack = " ".join(
        [
            maybe_text(row["title"]),
            maybe_text(row["snippet"]),
            maybe_text(row["query_text"]),
            maybe_text(row["topic"]),
        ]
    ).casefold()
    hits = [term for term in query_terms if term and term in haystack]
    if not hits:
        return 0.0, []
    return min(2.0, 0.4 * len(hits)), [f"lexical:{','.join(hits[:4])}"]


def overlap_score(query_values: list[str], row_value: str, label: str, base: float, per_value: float) -> tuple[float, list[str]]:
    values = {maybe_text(value).casefold() for value in query_values if maybe_text(value)}
    normalized = maybe_text(row_value).casefold()
    if not values or not normalized or normalized not in values:
        return 0.0, []
    return base + per_value, [f"{label}:{normalized}"]


def region_score(query_region: str, row_region: str) -> tuple[float, list[str]]:
    query_text = maybe_text(query_region).casefold()
    row_text = maybe_text(row_region).casefold()
    if not query_text or not row_text:
        return 0.0, []
    if query_text == row_text:
        return 1.4, [f"region:{row_text}"]
    if query_text in row_text or row_text in query_text:
        return 0.8, [f"region-partial:{row_text}"]
    return 0.0, []


def compact_result(row: sqlite3.Row, score: float, reasons: list[str]) -> dict[str, Any]:
    return {
        "run_id": maybe_text(row["run_id"]),
        "round_id": maybe_text(row["round_id"]),
        "signal_id": maybe_text(row["signal_id"]),
        "plane": maybe_text(row["plane"]),
        "source_skill": maybe_text(row["source_skill"]),
        "signal_kind": maybe_text(row["signal_kind"]),
        "metric": maybe_text(row["metric"]),
        "metric_family": maybe_text(row["metric_family"]),
        "title": maybe_text(row["title"]),
        "snippet": maybe_text(row["snippet"]),
        "region_label": maybe_text(row["region_label"]),
        "topic": maybe_text(row["topic"]),
        "score": round(score, 3),
        "match_reasons": reasons,
        "artifact_ref": maybe_text(row["artifact_ref"]),
    }


def archive_ref_for_output(path: Path) -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": "$",
        "artifact_ref": f"{path}:$",
    }


def query_signal_corpus_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    output_path: str,
    query_text: str,
    region_label: str,
    plane: str,
    metric_families: list[str],
    source_skills: list[str],
    exclude_run_id: str,
    limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    archive_db = resolve_path(run_dir_path, db_path, default_archive_db_path(run_dir_path))
    output_file = resolve_path(run_dir_path, output_path, default_output_path(run_dir_path, round_id))
    exclusion = maybe_text(exclude_run_id) or run_id
    query_terms = search_terms(query_text, region_label, plane, *metric_families, *source_skills)

    connection = connect_archive_db(archive_db)
    try:
        rows = connection.execute(
            "SELECT * FROM corpus_signals WHERE run_id != ? ORDER BY captured_at_utc DESC, run_id, signal_id",
            (exclusion,),
        ).fetchall()
    finally:
        connection.close()

    scored_rows: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        reasons: list[str] = []
        score = 0.0
        score_part, reason_part = overlap_score([plane], row["plane"], "plane", 1.2, 0.0)
        score += score_part
        reasons.extend(reason_part)
        score_part, reason_part = overlap_score(metric_families, row["metric_family"], "metric_family", 1.6, 0.4)
        score += score_part
        reasons.extend(reason_part)
        score_part, reason_part = overlap_score(source_skills, row["source_skill"], "source_skill", 1.0, 0.3)
        score += score_part
        reasons.extend(reason_part)
        score_part, reason_part = region_score(region_label, row["region_label"])
        score += score_part
        reasons.extend(reason_part)
        score_part, reason_part = lexical_overlap_score(row, query_terms)
        score += score_part
        reasons.extend(reason_part)
        if score <= 0 and query_terms:
            continue
        scored_rows.append((score, compact_result(row, score, unique_texts(reasons))))

    scored_rows.sort(key=lambda item: (-item[0], item[1]["run_id"], item[1]["signal_id"]))
    results = [item[1] for item in scored_rows[: max(1, limit)]]
    warnings = [] if results else [{"code": "no-results", "message": "No historical corpus signals matched the supplied filters."}]
    payload = {
        "schema_version": "archive-signal-query-v1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "query": {
            "query_text": maybe_text(query_text),
            "region_label": maybe_text(region_label),
            "plane": maybe_text(plane),
            "metric_families": unique_texts(metric_families),
            "source_skills": unique_texts(source_skills),
            "exclude_run_id": exclusion,
        },
        "db_path": str(archive_db),
        "result_count": len(results),
        "results": results,
    }
    write_json_file(output_file, payload)

    artifact_refs = [archive_ref_for_output(output_file)]
    for result in results[:20]:
        artifact_ref = maybe_text(result.get("artifact_ref"))
        if not artifact_ref or ":" not in artifact_ref:
            continue
        artifact_path, record_locator = artifact_ref.split(":", 1)
        artifact_refs.append(
            {
                "signal_id": maybe_text(result.get("signal_id")),
                "artifact_path": artifact_path,
                "record_locator": record_locator,
                "artifact_ref": artifact_ref,
            }
        )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "db_path": str(archive_db),
            "output_path": str(output_file),
            "result_count": len(results),
        },
        "receipt_id": "archive-receipt-" + hashlib.sha256(f"{SKILL_NAME}|{run_id}|{round_id}|{output_file}".encode("utf-8")).hexdigest()[:20],
        "batch_id": "archivebatch-" + hashlib.sha256(f"{SKILL_NAME}|{run_id}|{round_id}|{len(results)}".encode("utf-8")).hexdigest()[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [maybe_text(result.get("signal_id")) for result in results if maybe_text(result.get("signal_id"))],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [maybe_text(result.get("signal_id")) for result in results if maybe_text(result.get("signal_id"))],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if results else [warnings[0]["message"]],
            "challenge_hints": ["Historical signal matches still need claim-side validation before they are treated as supporting evidence."] if results else [],
            "suggested_next_skills": ["eco-materialize-history-context", "eco-lookup-normalized-signal", "eco-lookup-raw-record"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query compact historical signals from the cross-run signal corpus.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--query-text", default="")
    parser.add_argument("--region-label", default="")
    parser.add_argument("--plane", default="")
    parser.add_argument("--metric-family", action="append", default=[])
    parser.add_argument("--source-skill", action="append", default=[])
    parser.add_argument("--exclude-run-id", default="")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = query_signal_corpus_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        db_path=args.db_path,
        output_path=args.output_path,
        query_text=args.query_text,
        region_label=args.region_label,
        plane=args.plane,
        metric_families=args.metric_family,
        source_skills=args.source_skill,
        exclude_run_id=args.exclude_run_id,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())