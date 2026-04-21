#!/usr/bin/env python3
"""Normalize Open-Meteo historical artifacts into a local signal-plane SQLite file."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    ensure_signal_plane_schema,
)

SKILL_NAME = "eco-normalize-open-meteo-historical-signals"
SOURCE_SKILL = "open-meteo-historical-fetch"
PLANE = "environment"
CANONICAL_OBJECT_KIND = "environment-observation-signal"

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
CREATE INDEX IF NOT EXISTS idx_normalized_signals_round_plane ON normalized_signals(run_id, round_id, plane);
CREATE INDEX IF NOT EXISTS idx_normalized_signals_artifact ON normalized_signals(artifact_path, record_locator);
"""

INSERT_SQL = """
INSERT OR REPLACE INTO normalized_signals (
    signal_id, run_id, round_id, plane, batch_id, source_skill, signal_kind,
    canonical_object_kind,
    external_id, dedupe_key, title, body_text, url, author_name, channel_name,
    language, query_text, metric, numeric_value, unit, published_at_utc,
    observed_at_utc, window_start_utc, window_end_utc, captured_at_utc,
    latitude, longitude, bbox_json, quality_flags_json, engagement_json,
    metadata_json, raw_json, artifact_path, record_locator, artifact_sha256
) VALUES (
    :signal_id, :run_id, :round_id, :plane, :batch_id, :source_skill, :signal_kind,
    :canonical_object_kind,
    :external_id, :dedupe_key, :title, :body_text, :url, :author_name, :channel_name,
    :language, :query_text, :metric, :numeric_value, :unit, :published_at_utc,
    :observed_at_utc, :window_start_utc, :window_end_utc, :captured_at_utc,
    :latitude, :longitude, :bbox_json, :quality_flags_json, :engagement_json,
    :metadata_json, :raw_json, :artifact_path, :record_locator, :artifact_sha256
)
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def canonical_metric(value: Any) -> str:
    text = maybe_text(value).casefold().replace(".", "_").replace("-", "_")
    if text in {"pm25", "pm2_5", "pm2_5_", "pm2_5m"}:
        return "pm2_5"
    if text in {"pm10", "pm_10"}:
        return "pm10"
    return text


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "signal_plane.sqlite"


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return default_db_path(run_dir)
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


def artifact_ref(signal: dict[str, Any]) -> dict[str, str]:
    artifact_path = maybe_text(signal.get("artifact_path"))
    record_locator = maybe_text(signal.get("record_locator"))
    return {
        "signal_id": signal["signal_id"],
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": f"{artifact_path}:{record_locator}",
    }


def delete_existing_rows(connection: sqlite3.Connection, run_id: str, round_id: str, artifact_path: str) -> None:
    connection.execute(
        "DELETE FROM normalized_signals WHERE run_id = ? AND round_id = ? AND source_skill = ? AND artifact_path = ?",
        (run_id, round_id, SOURCE_SKILL, artifact_path),
    )


def insert_signals(connection: sqlite3.Connection, signals: list[dict[str, Any]]) -> None:
    for signal in signals:
        connection.execute(INSERT_SQL, signal)


def environment_gap_hints(signals: list[dict[str, Any]]) -> list[str]:
    if not signals:
        return ["No environment signals were normalized from the provided artifact."]
    hints: list[str] = []
    missing_coords = sum(1 for signal in signals if signal.get("latitude") is None or signal.get("longitude") is None)
    if missing_coords:
        hints.append(f"{missing_coords} Open-Meteo signals are missing coordinates.")
    return hints


def environment_challenge_hints() -> list[str]:
    return ["Historical weather rows may be model-backed or reanalysis-backed rather than direct station observations."]


def append_section_signals(
    signals: list[dict[str, Any]],
    section_name: str,
    record_index: int,
    record: dict[str, Any],
    run_id: str,
    round_id: str,
    artifact_path: Path,
    artifact_sha256: str,
    captured_at: str,
    warnings: list[dict[str, str]],
) -> None:
    section = record.get(section_name) if isinstance(record.get(section_name), dict) else {}
    units = record.get(f"{section_name}_units") if isinstance(record.get(f"{section_name}_units"), dict) else {}
    time_values = section.get("time") if isinstance(section.get("time"), list) else []
    latitude = maybe_number(record.get("latitude"))
    longitude = maybe_number(record.get("longitude"))
    for key, values in section.items():
        if key == "time" or not isinstance(values, list):
            continue
        if len(values) != len(time_values):
            warnings.append(
                {"code": "length-mismatch", "message": f"Skipped {section_name}.{key} because value count did not match time count."}
            )
            continue
        metric = canonical_metric(key)
        for index, raw_value in enumerate(values):
            value = maybe_number(raw_value)
            if value is None:
                continue
            observed_at = maybe_text(time_values[index])
            signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, section_name, key, index, value)[:16]
            signal_kind = f"{section_name}-observation"
            signals.append(
                {
                    "signal_id": signal_id,
                    "run_id": run_id,
                    "round_id": round_id,
                    "plane": PLANE,
                    "batch_id": "",
                    "source_skill": SOURCE_SKILL,
                    "signal_kind": signal_kind,
                    "canonical_object_kind": CANONICAL_OBJECT_KIND,
                    "external_id": f"{record_index}:{section_name}:{key}:{index}",
                    "dedupe_key": f"{metric}:{latitude}:{longitude}:{observed_at}",
                    "title": f"{metric} at {latitude},{longitude}" if latitude is not None and longitude is not None else metric,
                    "body_text": "",
                    "url": "",
                    "author_name": "",
                    "channel_name": maybe_text(record.get("timezone")),
                    "language": "",
                    "query_text": "",
                    "metric": metric,
                    "numeric_value": value,
                    "unit": maybe_text(units.get(key)),
                    "published_at_utc": "",
                    "observed_at_utc": observed_at,
                    "window_start_utc": observed_at if section_name == "daily" else "",
                    "window_end_utc": observed_at if section_name == "daily" else "",
                    "captured_at_utc": captured_at,
                    "latitude": latitude,
                    "longitude": longitude,
                    "bbox_json": json.dumps({}, ensure_ascii=True, sort_keys=True),
                    "quality_flags_json": json.dumps(["reanalysis-or-model"], ensure_ascii=True, sort_keys=True),
                    "engagement_json": json.dumps({}, ensure_ascii=True, sort_keys=True),
                    "metadata_json": json.dumps({"section": section_name, "timezone": maybe_text(record.get("timezone"))}, ensure_ascii=True, sort_keys=True),
                    "raw_json": json.dumps({"time": observed_at, "metric": key, "value": raw_value}, ensure_ascii=True, sort_keys=True),
                    "artifact_path": str(artifact_path),
                    "record_locator": f"$.records[{record_index}].{section_name}.{key}[{index}]",
                    "artifact_sha256": artifact_sha256,
                }
            )


def build_signals(
    payload: Any,
    run_id: str,
    round_id: str,
    artifact_path: Path,
    artifact_sha256: str,
    section_allowlist: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        warnings.append({"code": "missing-records", "message": "Expected payload.records to be a list."})
        return [], warnings
    allowed = {maybe_text(item).casefold() for item in section_allowlist if maybe_text(item)}
    if not allowed:
        allowed = {"hourly", "daily"}
    captured_at = utc_now_iso()
    signals: list[dict[str, Any]] = []
    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        if "hourly" in allowed:
            append_section_signals(signals, "hourly", record_index, record, run_id, round_id, artifact_path, artifact_sha256, captured_at, warnings)
        if "daily" in allowed:
            append_section_signals(signals, "daily", record_index, record, run_id, round_id, artifact_path, artifact_sha256, captured_at, warnings)
    if not signals:
        warnings.append({"code": "no-signals", "message": "No Open-Meteo values produced normalized signals."})
    return signals, warnings


def normalize_open_meteo_historical_signals(
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_path: str,
    db_path: str,
    section_allowlist: list[str],
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    artifact_file = Path(artifact_path).expanduser().resolve()
    payload = read_json(artifact_file)
    artifact_sha256 = file_sha256(artifact_file)
    signals, warnings = build_signals(payload, run_id, round_id, artifact_file, artifact_sha256, section_allowlist)
    batch_id = "sigbatch-" + stable_hash(SKILL_NAME, run_id, round_id, artifact_sha256)[:16]
    for signal in signals:
        signal["batch_id"] = batch_id
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        delete_existing_rows(connection, run_id, round_id, str(artifact_file))
        insert_signals(connection, signals)
        connection.commit()
    finally:
        connection.close()
    artifact_refs = [artifact_ref(signal) for signal in signals]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "plane": PLANE,
            "source_skill": SOURCE_SKILL,
            "signal_count": len(signals),
            "warning_count": len(warnings),
            "db_path": str(db_file),
        },
        "receipt_id": "normalize-receipt-" + stable_hash(SKILL_NAME, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": artifact_refs,
        "canonical_ids": [signal["signal_id"] for signal in signals],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [signal["signal_id"] for signal in signals],
            "evidence_refs": artifact_refs[:20],
            "gap_hints": environment_gap_hints(signals),
            "challenge_hints": environment_challenge_hints(),
            "suggested_next_skills": ["eco-query-environment-signals", "eco-extract-observation-candidates"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize Open-Meteo historical artifacts into a local signal-plane SQLite file.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--section", action="append", default=[])
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_open_meteo_historical_signals(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        artifact_path=args.artifact_path,
        db_path=args.db_path,
        section_allowlist=args.section,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
