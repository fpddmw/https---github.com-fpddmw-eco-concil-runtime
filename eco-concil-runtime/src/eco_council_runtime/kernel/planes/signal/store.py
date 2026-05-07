from __future__ import annotations

import sqlite3
from typing import Any

from eco_council_runtime.kernel.planes.signal.common import maybe_text
from eco_council_runtime.kernel.planes.signal.metadata import (
    enrich_signal_metadata_fields,
    indexed_signal_rows,
)
from eco_council_runtime.kernel.planes.signal.schema import (
    INDEX_INSERT_SQL,
    INSERT_SQL,
)


def delete_existing_rows(connection: sqlite3.Connection, run_id: str, round_id: str, source_skill: str, artifact_path: str) -> None:
    connection.execute(
        """
        DELETE FROM normalized_signal_index
        WHERE signal_id IN (
            SELECT signal_id
            FROM normalized_signals
            WHERE run_id = ? AND round_id = ? AND source_skill = ? AND artifact_path = ?
        )
        """,
        (run_id, round_id, source_skill, artifact_path),
    )
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
        enriched_signal = enrich_signal_metadata_fields(signal)
        connection.execute(INSERT_SQL, enriched_signal)
        replace_signal_index_rows(connection, enriched_signal)


def replace_signal_index_rows(
    connection: sqlite3.Connection,
    signal: dict[str, Any],
) -> None:
    signal_id = maybe_text(signal.get("signal_id"))
    if not signal_id:
        return
    connection.execute(
        "DELETE FROM normalized_signal_index WHERE signal_id = ?",
        (signal_id,),
    )
    for row in indexed_signal_rows(signal):
        connection.execute(INDEX_INSERT_SQL, row)


__all__ = [
    "delete_existing_rows",
    "delete_existing_rows_for_artifacts",
    "insert_signals",
    "replace_signal_index_rows",
]
