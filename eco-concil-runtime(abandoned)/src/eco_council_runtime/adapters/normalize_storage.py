"""Storage and manifest adapters for normalize artifacts."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from eco_council_runtime.adapters.filesystem import load_json_if_exists
from eco_council_runtime.domain.text import maybe_text
from eco_council_runtime.layout import (
    NORMALIZE_ENVIRONMENT_DDL_PATH,
    NORMALIZE_PUBLIC_DDL_PATH,
)


def default_public_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "public_signals.sqlite"


def default_environment_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "environment_signals.sqlite"


def run_manifest_path(run_dir: Path) -> Path:
    return run_dir / "run_manifest.json"


def load_or_build_manifest(
    run_dir: Path,
    *,
    run_id: str,
    public_db_path: Path | None = None,
    environment_db_path: Path | None = None,
) -> dict[str, Any]:
    manifest_file = run_manifest_path(run_dir)
    payload = load_json_if_exists(manifest_file)
    if isinstance(payload, dict):
        return payload
    return {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "analytics_backend": "sqlite",
        "databases": {
            "public_signals": str(public_db_path or default_public_db_path(run_dir)),
            "environment_signals": str(environment_db_path or default_environment_db_path(run_dir)),
        },
    }


def resolve_manifest_db_path(run_dir: Path, value: Any, fallback: Path) -> Path:
    text = maybe_text(value)
    if not text:
        return fallback.expanduser().resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_analytics_db_paths(run_dir: Path) -> tuple[Path, Path]:
    manifest = load_json_if_exists(run_manifest_path(run_dir))
    dbs = manifest.get("databases") if isinstance(manifest, dict) else {}
    public_value = dbs.get("public_signals") if isinstance(dbs, dict) else ""
    environment_value = dbs.get("environment_signals") if isinstance(dbs, dict) else ""
    return (
        resolve_manifest_db_path(run_dir, public_value, default_public_db_path(run_dir)),
        resolve_manifest_db_path(run_dir, environment_value, default_environment_db_path(run_dir)),
    )


def _load_ddl(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _init_sqlite_db(path: Path, ddl_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ddl = _load_ddl(ddl_path)
    with sqlite3.connect(path) as conn:
        conn.executescript(ddl)
        conn.commit()


def _insert_many(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    data = list(rows)
    if not data:
        return
    conn.executemany(sql, data)
    conn.commit()


def save_public_db(db_path: Path, signals: list[dict[str, Any]], claims: list[dict[str, Any]]) -> None:
    _init_sqlite_db(db_path, NORMALIZE_PUBLIC_DDL_PATH)
    with sqlite3.connect(db_path) as conn:
        _insert_many(
            conn,
            """
            INSERT OR REPLACE INTO public_signals (
                signal_id, run_id, round_id, source_skill, signal_kind, external_id, title, text,
                url, author_name, channel_name, language, query_text, published_at_utc,
                captured_at_utc, engagement_json, metadata_json, artifact_path, record_locator,
                sha256, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    signal["signal_id"],
                    signal["run_id"],
                    signal["round_id"],
                    signal["source_skill"],
                    signal["signal_kind"],
                    signal["external_id"],
                    signal["title"],
                    signal["text"],
                    signal["url"],
                    signal["author_name"],
                    signal["channel_name"],
                    signal["language"],
                    signal["query_text"],
                    signal["published_at_utc"],
                    signal["captured_at_utc"],
                    json.dumps(signal.get("engagement", {}), ensure_ascii=True, sort_keys=True),
                    json.dumps(signal.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    signal["artifact_path"],
                    signal["record_locator"],
                    signal["sha256"],
                    json.dumps(signal.get("raw_json"), ensure_ascii=True, sort_keys=True),
                )
                for signal in signals
            ),
        )
        _insert_many(
            conn,
            """
            INSERT OR REPLACE INTO claim_candidates (
                claim_id, run_id, round_id, claim_type, priority, summary, statement,
                source_signal_ids_json, claim_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    claim["claim_id"],
                    claim["run_id"],
                    claim["round_id"],
                    claim["claim_type"],
                    claim["priority"],
                    claim["summary"],
                    claim["statement"],
                    json.dumps(
                        [ref.get("external_id") or ref.get("record_locator") for ref in claim.get("public_refs", [])],
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                    json.dumps(claim, ensure_ascii=True, sort_keys=True),
                )
                for claim in claims
            ),
        )


def save_environment_db(db_path: Path, signals: list[dict[str, Any]], observations: list[dict[str, Any]]) -> None:
    _init_sqlite_db(db_path, NORMALIZE_ENVIRONMENT_DDL_PATH)
    with sqlite3.connect(db_path) as conn:
        _insert_many(
            conn,
            """
            INSERT OR REPLACE INTO environment_signals (
                signal_id, run_id, round_id, source_skill, signal_kind, metric, value, unit,
                observed_at_utc, window_start_utc, window_end_utc, latitude, longitude,
                bbox_json, quality_flags_json, metadata_json, artifact_path, record_locator,
                sha256, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    signal["signal_id"],
                    signal["run_id"],
                    signal["round_id"],
                    signal["source_skill"],
                    signal["signal_kind"],
                    signal["metric"],
                    signal["value"],
                    signal["unit"],
                    signal["observed_at_utc"],
                    signal["window_start_utc"],
                    signal["window_end_utc"],
                    signal["latitude"],
                    signal["longitude"],
                    json.dumps(signal.get("bbox"), ensure_ascii=True, sort_keys=True) if signal.get("bbox") is not None else None,
                    json.dumps(signal.get("quality_flags", []), ensure_ascii=True, sort_keys=True),
                    json.dumps(signal.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    signal["artifact_path"],
                    signal["record_locator"],
                    signal["sha256"],
                    json.dumps(signal.get("raw_json"), ensure_ascii=True, sort_keys=True),
                )
                for signal in signals
            ),
        )
        _insert_many(
            conn,
            """
            INSERT OR REPLACE INTO observation_summaries (
                observation_id, run_id, round_id, metric, source_skill, observation_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    observation["observation_id"],
                    observation["run_id"],
                    observation["round_id"],
                    observation["metric"],
                    observation["source_skill"],
                    json.dumps(observation, ensure_ascii=True, sort_keys=True),
                )
                for observation in observations
            ),
        )


__all__ = [
    "default_environment_db_path",
    "default_public_db_path",
    "load_or_build_manifest",
    "resolve_analytics_db_paths",
    "resolve_manifest_db_path",
    "run_manifest_path",
    "save_environment_db",
    "save_public_db",
]
