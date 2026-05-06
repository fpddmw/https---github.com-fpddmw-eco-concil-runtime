from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

SCHEMA_METADATA_TABLE = "schema_metadata"
SCHEMA_MIGRATIONS_TABLE = "schema_migrations"
SCHEMA_METADATA_SCHEMA = "schema-metadata-v1"
SCHEMA_MIGRATION_RECORD_SCHEMA = "schema-migration-record-v1"


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def migration_checksum(*, schema_name: str, migration_id: str, target_version: str, description: str) -> str:
    return stable_hash(schema_name, migration_id, target_version, description)


def ensure_schema_migration_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_METADATA_TABLE} (
            schema_name TEXT PRIMARY KEY,
            current_version TEXT NOT NULL DEFAULT '',
            updated_at_utc TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{{}}'
        )
        """
    )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
            migration_id TEXT PRIMARY KEY,
            schema_name TEXT NOT NULL,
            target_version TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            checksum TEXT NOT NULL DEFAULT '',
            applied_at_utc TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            error_message TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{{}}'
        )
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_schema_migrations_schema
        ON {SCHEMA_MIGRATIONS_TABLE}(schema_name, target_version, migration_id)
        """
    )


def set_schema_version(
    connection: sqlite3.Connection,
    *,
    schema_name: str,
    current_version: str,
) -> dict[str, Any]:
    ensure_schema_migration_tables(connection)
    now = utc_now_iso()
    payload = {
        "schema_version": SCHEMA_METADATA_SCHEMA,
        "schema_name": maybe_text(schema_name),
        "current_version": maybe_text(current_version),
        "updated_at_utc": now,
    }
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {SCHEMA_METADATA_TABLE} (
            schema_name, current_version, updated_at_utc, raw_json
        ) VALUES (?, ?, ?, ?)
        """,
        (
            payload["schema_name"],
            payload["current_version"],
            payload["updated_at_utc"],
            json_text(payload),
        ),
    )
    return payload


def schema_migration_row(
    connection: sqlite3.Connection,
    *,
    migration_id: str,
) -> dict[str, Any] | None:
    ensure_schema_migration_tables(connection)
    row = connection.execute(
        f"""
        SELECT *
        FROM {SCHEMA_MIGRATIONS_TABLE}
        WHERE migration_id = ?
        """,
        (maybe_text(migration_id),),
    ).fetchone()
    if row is None:
        return None
    keys = row.keys() if hasattr(row, "keys") else ()
    if keys:
        return {key: row[key] for key in keys}
    columns = [description[0] for description in connection.execute(
        f"SELECT * FROM {SCHEMA_MIGRATIONS_TABLE} LIMIT 0"
    ).description]
    return dict(zip(columns, row))


def record_schema_migration(
    connection: sqlite3.Connection,
    *,
    schema_name: str,
    migration_id: str,
    target_version: str,
    description: str,
    checksum: str,
    status: str,
    error_message: str = "",
) -> dict[str, Any]:
    ensure_schema_migration_tables(connection)
    now = utc_now_iso()
    payload = {
        "schema_version": SCHEMA_MIGRATION_RECORD_SCHEMA,
        "schema_name": maybe_text(schema_name),
        "migration_id": maybe_text(migration_id),
        "target_version": maybe_text(target_version),
        "description": maybe_text(description),
        "checksum": maybe_text(checksum),
        "applied_at_utc": now,
        "status": maybe_text(status),
        "error_message": maybe_text(error_message),
    }
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {SCHEMA_MIGRATIONS_TABLE} (
            migration_id, schema_name, target_version, description, checksum,
            applied_at_utc, status, error_message, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["migration_id"],
            payload["schema_name"],
            payload["target_version"],
            payload["description"],
            payload["checksum"],
            payload["applied_at_utc"],
            payload["status"],
            payload["error_message"],
            json_text(payload),
        ),
    )
    return payload


def apply_schema_migration(
    connection: sqlite3.Connection,
    *,
    schema_name: str,
    migration_id: str,
    target_version: str,
    description: str,
    operation: Callable[[], None],
) -> dict[str, Any]:
    ensure_schema_migration_tables(connection)
    checksum = migration_checksum(
        schema_name=schema_name,
        migration_id=migration_id,
        target_version=target_version,
        description=description,
    )
    existing = schema_migration_row(connection, migration_id=migration_id)
    if existing and maybe_text(existing.get("status")) == "applied":
        if maybe_text(existing.get("checksum")) != checksum:
            raise ValueError(
                f"Schema migration {migration_id} was already applied with a different checksum."
            )
        return {**existing, "write_status": "unchanged"}

    try:
        operation()
    except Exception as exc:  # noqa: BLE001
        record = record_schema_migration(
            connection,
            schema_name=schema_name,
            migration_id=migration_id,
            target_version=target_version,
            description=description,
            checksum=checksum,
            status="failed",
            error_message=str(exc),
        )
        connection.commit()
        raise

    record = record_schema_migration(
        connection,
        schema_name=schema_name,
        migration_id=migration_id,
        target_version=target_version,
        description=description,
        checksum=checksum,
        status="applied",
    )
    return {**record, "write_status": "created"}


def load_schema_status(connection: sqlite3.Connection) -> dict[str, Any]:
    ensure_schema_migration_tables(connection)
    metadata_rows = connection.execute(
        f"""
        SELECT schema_name, current_version, updated_at_utc, raw_json
        FROM {SCHEMA_METADATA_TABLE}
        ORDER BY schema_name
        """
    ).fetchall()
    migration_rows = connection.execute(
        f"""
        SELECT migration_id, schema_name, target_version, description, checksum,
               applied_at_utc, status, error_message, raw_json
        FROM {SCHEMA_MIGRATIONS_TABLE}
        ORDER BY schema_name, migration_id
        """
    ).fetchall()
    metadata = [
        {
            "schema_name": row["schema_name"],
            "current_version": row["current_version"],
            "updated_at_utc": row["updated_at_utc"],
        }
        for row in metadata_rows
    ]
    migrations = [
        {
            "migration_id": row["migration_id"],
            "schema_name": row["schema_name"],
            "target_version": row["target_version"],
            "description": row["description"],
            "checksum": row["checksum"],
            "applied_at_utc": row["applied_at_utc"],
            "status": row["status"],
            "error_message": row["error_message"],
        }
        for row in migration_rows
    ]
    failed_migrations = [
        migration for migration in migrations if maybe_text(migration.get("status")) == "failed"
    ]
    return {
        "schema_version": "schema-status-v1",
        "status": "failed" if failed_migrations else "completed",
        "generated_at_utc": utc_now_iso(),
        "summary": {
            "schema_count": len(metadata),
            "migration_count": len(migrations),
            "failed_migration_count": len(failed_migrations),
        },
        "metadata": metadata,
        "migrations": migrations,
        "failed_migrations": failed_migrations,
    }
