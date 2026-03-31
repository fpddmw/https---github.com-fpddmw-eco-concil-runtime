from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
    external_id, dedupe_key, title, body_text, url, author_name, channel_name,
    language, query_text, metric, numeric_value, unit, published_at_utc,
    observed_at_utc, window_start_utc, window_end_utc, captured_at_utc,
    latitude, longitude, bbox_json, quality_flags_json, engagement_json,
    metadata_json, raw_json, artifact_path, record_locator, artifact_sha256
) VALUES (
    :signal_id, :run_id, :round_id, :plane, :batch_id, :source_skill, :signal_kind,
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
    connection.executescript(SCHEMA_SQL)
    return connection, file_path


def delete_existing_rows(connection: sqlite3.Connection, run_id: str, round_id: str, source_skill: str, artifact_path: str) -> None:
    connection.execute(
        "DELETE FROM normalized_signals WHERE run_id = ? AND round_id = ? AND source_skill = ? AND artifact_path = ?",
        (run_id, round_id, source_skill, artifact_path),
    )


def delete_existing_rows_for_artifacts(
    connection: sqlite3.Connection,
    run_id: str,
    round_id: str,
    source_skill: str,
    artifact_paths: list[str],
) -> None:
    for artifact_path in sorted({maybe_text(item) for item in artifact_paths if maybe_text(item)}):
        delete_existing_rows(connection, run_id, round_id, source_skill, artifact_path)


def insert_signals(connection: sqlite3.Connection, signals: list[dict[str, Any]]) -> None:
    for signal in signals:
        connection.execute(INSERT_SQL, signal)


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def base_signal(
    *,
    signal_id: str,
    run_id: str,
    round_id: str,
    plane: str,
    source_skill: str,
    signal_kind: str,
    external_id: str,
    dedupe_key: str,
    title: str,
    body_text: str,
    url: str,
    author_name: str,
    channel_name: str,
    language: str,
    query_text: str,
    metric: str,
    numeric_value: float | None,
    unit: str,
    published_at_utc: str,
    observed_at_utc: str,
    window_start_utc: str,
    window_end_utc: str,
    captured_at_utc: str,
    latitude: float | None,
    longitude: float | None,
    quality_flags: list[Any],
    engagement: dict[str, Any],
    metadata: dict[str, Any],
    raw_record: Any,
    artifact_path: Path,
    record_locator: str,
    artifact_sha256: str,
) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "run_id": run_id,
        "round_id": round_id,
        "plane": plane,
        "batch_id": "",
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "external_id": external_id,
        "dedupe_key": dedupe_key,
        "title": title,
        "body_text": body_text,
        "url": url,
        "author_name": author_name,
        "channel_name": channel_name,
        "language": language,
        "query_text": query_text,
        "metric": metric,
        "numeric_value": numeric_value,
        "unit": unit,
        "published_at_utc": published_at_utc,
        "observed_at_utc": observed_at_utc,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "captured_at_utc": captured_at_utc,
        "latitude": latitude,
        "longitude": longitude,
        "bbox_json": json_text({}),
        "quality_flags_json": json_text(quality_flags),
        "engagement_json": json_text(engagement),
        "metadata_json": json_text(metadata),
        "raw_json": json.dumps(raw_record, ensure_ascii=True, sort_keys=True),
        "artifact_path": str(artifact_path),
        "record_locator": record_locator,
        "artifact_sha256": artifact_sha256,
    }


def artifact_ref(signal: dict[str, Any]) -> dict[str, str]:
    return {
        "signal_id": maybe_text(signal.get("signal_id")),
        "artifact_path": maybe_text(signal.get("artifact_path")),
        "record_locator": maybe_text(signal.get("record_locator")),
        "artifact_ref": f"{maybe_text(signal.get('artifact_path'))}:{maybe_text(signal.get('record_locator'))}",
    }


def plane_gap_hints(plane: str, signals: list[dict[str, Any]]) -> list[str]:
    if signals:
        return []
    if plane == "public":
        return ["No public signals were normalized from the provided artifact."]
    return ["No environment signals were normalized from the provided artifact."]


def plane_challenge_hints(plane: str) -> list[str]:
    if plane == "public":
        return ["Check whether normalization kept enough text context for downstream claim clustering and challenge review."]
    return ["Check whether provider-specific observation rows still need spatial or metric-family harmonization before promotion."]


def normalize_limit(value: int | None) -> int | None:
    if value is None:
        return None
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return None
    if resolved <= 0:
        return None
    return resolved


def finalize_normalization_streaming(
    *,
    skill_name: str,
    source_skill: str,
    plane: str,
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_file: Path,
    db_path: str,
    signals: Iterable[dict[str, Any]],
    warnings: list[dict[str, str]],
    cleanup_artifact_paths: list[str] | None = None,
    artifact_ref_limit: int | None = None,
    canonical_id_limit: int | None = None,
    chunk_size: int = 1000,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    batch_id = "sigbatch-" + stable_hash(skill_name, run_id, round_id, artifact_file.name, utc_now_iso())[:16]
    connection, db_file = connect_db(run_dir_path, db_path)
    returned_artifact_refs: list[dict[str, str]] = []
    returned_canonical_ids: list[str] = []
    signal_count = 0
    artifact_limit = normalize_limit(artifact_ref_limit)
    canonical_limit = normalize_limit(canonical_id_limit)
    resolved_chunk_size = max(1, int(chunk_size))
    buffer: list[dict[str, Any]] = []

    try:
        delete_existing_rows_for_artifacts(
            connection,
            run_id,
            round_id,
            source_skill,
            cleanup_artifact_paths or [str(artifact_file)],
        )
        for signal in signals:
            signal["batch_id"] = batch_id
            buffer.append(signal)
            signal_count += 1

            if artifact_limit is None or len(returned_artifact_refs) < artifact_limit:
                returned_artifact_refs.append(artifact_ref(signal))
            if canonical_limit is None or len(returned_canonical_ids) < canonical_limit:
                signal_id = maybe_text(signal.get("signal_id"))
                if signal_id:
                    returned_canonical_ids.append(signal_id)

            if len(buffer) >= resolved_chunk_size:
                insert_signals(connection, buffer)
                buffer.clear()

        if buffer:
            insert_signals(connection, buffer)
        connection.commit()
    finally:
        connection.close()

    if artifact_limit is not None and signal_count > artifact_limit:
        warnings.append(
            {
                "code": "artifact-refs-truncated",
                "message": f"Returned artifact_refs were truncated to {artifact_limit} while {signal_count} signals were normalized.",
            }
        )
    if canonical_limit is not None and signal_count > canonical_limit:
        warnings.append(
            {
                "code": "canonical-ids-truncated",
                "message": f"Returned canonical_ids were truncated to {canonical_limit} while {signal_count} signals were normalized.",
            }
        )

    suggested_next_skills = ["eco-query-public-signals", "eco-extract-claim-candidates"] if plane == "public" else ["eco-query-environment-signals", "eco-extract-observation-candidates"]
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "plane": plane,
            "source_skill": source_skill,
            "signal_count": signal_count,
            "warning_count": len(warnings),
            "returned_artifact_ref_count": len(returned_artifact_refs),
            "returned_canonical_id_count": len(returned_canonical_ids),
            "db_path": str(db_file),
        },
        "receipt_id": "normalize-receipt-" + stable_hash(skill_name, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": returned_artifact_refs,
        "canonical_ids": returned_canonical_ids,
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": returned_canonical_ids,
            "evidence_refs": returned_artifact_refs[:20],
            "gap_hints": [] if signal_count else plane_gap_hints(plane, []),
            "challenge_hints": plane_challenge_hints(plane),
            "suggested_next_skills": suggested_next_skills,
        },
    }


def finalize_normalization(
    *,
    skill_name: str,
    source_skill: str,
    plane: str,
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_file: Path,
    db_path: str,
    signals: list[dict[str, Any]],
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    return finalize_normalization_streaming(
        skill_name=skill_name,
        source_skill=source_skill,
        plane=plane,
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_file=artifact_file,
        db_path=db_path,
        signals=signals,
        warnings=warnings,
    )


__all__ = [
    "artifact_ref",
    "base_signal",
    "connect_db",
    "default_db_path",
    "delete_existing_rows",
    "delete_existing_rows_for_artifacts",
    "file_sha256",
    "finalize_normalization",
    "finalize_normalization_streaming",
    "insert_signals",
    "json_text",
    "maybe_number",
    "maybe_text",
    "normalize_space",
    "pretty_json",
    "read_json",
    "resolve_db_path",
    "resolve_run_dir",
    "stable_hash",
    "utc_now_iso",
]
