from __future__ import annotations

import sqlite3
from pathlib import Path

from eco_council_runtime.kernel.core.schema_migrations import (
    apply_schema_migration,
    ensure_schema_migration_tables,
    set_schema_version,
)
from eco_council_runtime.kernel.planes.signal.common import maybe_text

SIGNAL_PLANE_SCHEMA_NAME = "signal-plane"
SIGNAL_PLANE_SCHEMA_VERSION = "2026.05.06.1"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    canonical_object_kind TEXT NOT NULL DEFAULT '',
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
CREATE INDEX IF NOT EXISTS idx_normalized_signals_round_kind ON normalized_signals(run_id, round_id, canonical_object_kind);
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

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    canonical_object_kind TEXT NOT NULL DEFAULT '',
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
)
"""

INDEX_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signal_index (
    signal_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    canonical_object_kind TEXT NOT NULL DEFAULT '',
    field_name TEXT NOT NULL,
    field_value TEXT NOT NULL,
    PRIMARY KEY (signal_id, field_name, field_value)
)
"""

INDEX_INSERT_SQL = """
INSERT OR REPLACE INTO normalized_signal_index (
    signal_id,
    run_id,
    round_id,
    plane,
    source_skill,
    canonical_object_kind,
    field_name,
    field_value
) VALUES (
    :signal_id,
    :run_id,
    :round_id,
    :plane,
    :source_skill,
    :canonical_object_kind,
    :field_name,
    :field_value
)
"""


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


def table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {maybe_text(row[1]) for row in rows if len(row) > 1}


def default_canonical_object_kind(*, plane: str) -> str:
    resolved_plane = maybe_text(plane)
    if resolved_plane == "formal":
        return "formal-comment-signal"
    if resolved_plane == "environment":
        return "environment-observation-signal"
    if resolved_plane == "public":
        return "public-discourse-signal"
    return ""


def resolved_canonical_object_kind(
    *,
    plane: str,
    source_skill: str = "",
    signal_kind: str = "",
    canonical_object_kind: str = "",
) -> str:
    explicit_kind = maybe_text(canonical_object_kind)
    if explicit_kind:
        return explicit_kind
    if maybe_text(source_skill) in FORMAL_SOURCE_SKILLS:
        return "formal-comment-signal"
    return default_canonical_object_kind(plane=plane)


def ensure_signal_plane_schema(connection: sqlite3.Connection) -> None:
    ensure_schema_migration_tables(connection)
    apply_schema_migration(
        connection,
        schema_name=SIGNAL_PLANE_SCHEMA_NAME,
        migration_id="0001-signal-plane-schema-baseline",
        target_version=SIGNAL_PLANE_SCHEMA_VERSION,
        description="Record the current normalized signal-plane schema baseline.",
        operation=lambda: None,
    )
    apply_schema_migration(
        connection,
        schema_name=SIGNAL_PLANE_SCHEMA_NAME,
        migration_id="0002-signal-plane-normalized-indexes-and-kind",
        target_version=SIGNAL_PLANE_SCHEMA_VERSION,
        description="Backfill normalized signal canonical object kind support and query indexes.",
        operation=lambda: apply_signal_plane_legacy_schema_migrations(connection),
    )
    set_schema_version(
        connection,
        schema_name=SIGNAL_PLANE_SCHEMA_NAME,
        current_version=SIGNAL_PLANE_SCHEMA_VERSION,
    )


def apply_signal_plane_legacy_schema_migrations(connection: sqlite3.Connection) -> None:
    connection.execute(TABLE_SQL)
    connection.execute(INDEX_TABLE_SQL)
    columns = table_columns(connection, "normalized_signals")
    if "canonical_object_kind" not in columns:
        connection.execute(
            "ALTER TABLE normalized_signals ADD COLUMN canonical_object_kind TEXT NOT NULL DEFAULT ''"
        )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_normalized_signals_round_plane ON normalized_signals(run_id, round_id, plane)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_normalized_signals_round_kind ON normalized_signals(run_id, round_id, canonical_object_kind)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_normalized_signals_artifact ON normalized_signals(artifact_path, record_locator)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_normalized_signal_index_scope ON normalized_signal_index(run_id, round_id, plane, field_name, field_value)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_normalized_signal_index_signal ON normalized_signal_index(signal_id)"
    )


def connect_db(run_dir: Path, db_path: str) -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    ensure_signal_plane_schema(connection)
    connection.commit()
    return connection, file_path


__all__ = [
    "INDEX_INSERT_SQL",
    "INSERT_SQL",
    "SCHEMA_SQL",
    "SIGNAL_PLANE_SCHEMA_NAME",
    "SIGNAL_PLANE_SCHEMA_VERSION",
    "TABLE_SQL",
    "apply_signal_plane_legacy_schema_migrations",
    "connect_db",
    "default_db_path",
    "ensure_signal_plane_schema",
    "resolve_db_path",
    "table_columns",
]
