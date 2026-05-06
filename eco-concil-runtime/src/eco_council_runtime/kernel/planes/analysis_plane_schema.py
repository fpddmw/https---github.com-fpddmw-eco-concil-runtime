from __future__ import annotations

import sqlite3
from pathlib import Path

from eco_council_runtime.kernel.planes.analysis_plane_contracts import maybe_text

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS analysis_result_sets (
    result_set_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    source_skill TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    generated_at_utc TEXT NOT NULL DEFAULT '',
    item_count INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_sets_round_kind
ON analysis_result_sets(run_id, round_id, analysis_kind, generated_at_utc, result_set_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_sets_artifact
ON analysis_result_sets(artifact_path, analysis_kind, generated_at_utc, result_set_id);

CREATE TABLE IF NOT EXISTS analysis_result_items (
    item_id TEXT PRIMARY KEY,
    result_set_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    source_skill TEXT NOT NULL DEFAULT '',
    item_index INTEGER NOT NULL DEFAULT 0,
    subject_id TEXT NOT NULL DEFAULT '',
    readiness TEXT NOT NULL DEFAULT '',
    decision_source TEXT NOT NULL DEFAULT '',
    score REAL,
    related_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    lineage_json TEXT NOT NULL DEFAULT '[]',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    item_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    generated_at_utc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_result_set
ON analysis_result_items(result_set_id, item_index, item_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_round_kind
ON analysis_result_items(run_id, round_id, analysis_kind, item_index, item_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_subject
ON analysis_result_items(subject_id, analysis_kind, readiness);

CREATE TABLE IF NOT EXISTS analysis_result_lineage (
    lineage_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    result_set_id TEXT NOT NULL,
    item_id TEXT NOT NULL DEFAULT '',
    lineage_scope TEXT NOT NULL DEFAULT '',
    lineage_type TEXT NOT NULL DEFAULT '',
    relation TEXT NOT NULL DEFAULT '',
    value_text TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    source_analysis_kind TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_lineage_result_set
ON analysis_result_lineage(result_set_id, item_id, lineage_scope, lineage_type, relation, lineage_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_lineage_value
ON analysis_result_lineage(value_text, lineage_type, result_set_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_lineage_artifact
ON analysis_result_lineage(artifact_path, lineage_type, result_set_id, item_id);
"""

def resolve_run_dir(run_dir: str | Path) -> Path:
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

def connect_db(run_dir: Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    ensure_analysis_plane_schema(connection)
    return connection, file_path

def ensure_analysis_plane_schema(connection: sqlite3.Connection) -> None:
    item_columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(analysis_result_items)")
    }
    if "decision_source" not in item_columns:
        connection.execute(
            "ALTER TABLE analysis_result_items ADD COLUMN decision_source TEXT NOT NULL DEFAULT ''"
        )
    if "lineage_json" not in item_columns:
        connection.execute(
            "ALTER TABLE analysis_result_items ADD COLUMN lineage_json TEXT NOT NULL DEFAULT '[]'"
        )
    if "provenance_json" not in item_columns:
        connection.execute(
            "ALTER TABLE analysis_result_items ADD COLUMN provenance_json TEXT NOT NULL DEFAULT '{}'"
        )

__all__ = [
    "SCHEMA_SQL",
    "resolve_run_dir",
    "default_db_path",
    "resolve_db_path",
    "connect_db",
    "ensure_analysis_plane_schema",
]
