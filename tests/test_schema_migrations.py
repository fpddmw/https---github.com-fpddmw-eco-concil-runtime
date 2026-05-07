from __future__ import annotations

import io
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))


def column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def migration_statuses(connection: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT migration_id, status FROM schema_migrations"
        ).fetchall()
    }


def create_legacy_deliberation_tables(db_file: Path) -> None:
    db_file.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_file)
    try:
        connection.executescript(
            """
            CREATE TABLE board_events (
                event_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                round_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                created_at_utc TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                board_revision INTEGER NOT NULL DEFAULT 0,
                artifact_path TEXT NOT NULL DEFAULT '',
                record_locator TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}'
            );
            INSERT INTO board_events (
                event_id, run_id, round_id, event_type, created_at_utc,
                payload_json, board_revision, artifact_path, record_locator, raw_json
            ) VALUES (
                'event-legacy-001', 'run-schema-001', 'round-schema-001',
                'legacy-event', '2026-05-06T00:00:00Z', '{}', 1, '', '$', '{}'
            );
            CREATE TABLE report_basis_freezes (
                freeze_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                round_id TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL DEFAULT '',
                gate_status TEXT NOT NULL DEFAULT '',
                readiness_status TEXT NOT NULL DEFAULT '',
                report_basis_status TEXT NOT NULL DEFAULT '',
                controller_status TEXT NOT NULL DEFAULT '',
                supervisor_status TEXT NOT NULL DEFAULT '',
                planning_mode TEXT NOT NULL DEFAULT '',
                report_basis_freeze_allowed INTEGER NOT NULL DEFAULT 0,
                gate_reasons_json TEXT NOT NULL DEFAULT '[]',
                recommended_next_skills_json TEXT NOT NULL DEFAULT '[]',
                controller_artifact_path TEXT NOT NULL DEFAULT '',
                gate_artifact_path TEXT NOT NULL DEFAULT '',
                supervisor_artifact_path TEXT NOT NULL DEFAULT '',
                record_locator TEXT NOT NULL DEFAULT '$',
                raw_json TEXT NOT NULL DEFAULT '{}'
            );
            """
        )
        connection.commit()
    finally:
        connection.close()


def create_legacy_signal_table(db_file: Path) -> None:
    connection = sqlite3.connect(db_file)
    try:
        connection.executescript(
            """
            CREATE TABLE normalized_signals (
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
            INSERT INTO normalized_signals (
                signal_id, run_id, round_id, plane, batch_id, source_skill,
                signal_kind, artifact_path, record_locator
            ) VALUES (
                'signal-legacy-001', 'run-schema-001', 'round-schema-001',
                'environment', 'batch-legacy-001', 'fetch-openaq',
                'observation', '/tmp/legacy.json', '$.results[0]'
            );
            """
        )
        connection.commit()
    finally:
        connection.close()


class SchemaMigrationTests(unittest.TestCase):
    def test_legacy_run_db_upgrades_columns_indexes_and_migration_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            db_file = run_dir / "analytics" / "signal_plane.sqlite"
            create_legacy_deliberation_tables(db_file)
            create_legacy_signal_table(db_file)

            from eco_council_runtime.kernel.planes.deliberation_plane import (
                connect_db as connect_deliberation_db,
                load_schema_status,
            )
            from eco_council_runtime.kernel.planes.signal import (
                connect_db as connect_signal_db,
            )

            connection, _ = connect_deliberation_db(run_dir)
            try:
                self.assertIn("event_index", column_names(connection, "board_events"))
                self.assertIn(
                    "reporting_ready",
                    column_names(connection, "report_basis_freezes"),
                )
                first_statuses = migration_statuses(connection)
            finally:
                connection.close()

            connection, _ = connect_signal_db(run_dir, "")
            try:
                self.assertIn(
                    "canonical_object_kind",
                    column_names(connection, "normalized_signals"),
                )
                self.assertIn("normalized_signal_index", {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                })
                second_statuses = migration_statuses(connection)
            finally:
                connection.close()

            status_payload = load_schema_status(run_dir)
            schema_versions = {
                item["schema_name"]: item["current_version"]
                for item in status_payload["metadata"]
            }
            migration_ids = {
                item["migration_id"]
                for item in status_payload["migrations"]
            }

            self.assertEqual(
                "applied",
                first_statuses["0002-deliberation-legacy-columns-and-indexes"],
            )
            self.assertEqual(
                "applied",
                second_statuses["0002-signal-plane-normalized-indexes-and-kind"],
            )
            self.assertEqual("2026.05.06.1", schema_versions["deliberation-plane"])
            self.assertEqual("2026.05.06.1", schema_versions["signal-plane"])
            self.assertIn("0001-deliberation-schema-baseline", migration_ids)
            self.assertIn("0001-signal-plane-schema-baseline", migration_ids)
            self.assertEqual("completed", status_payload["status"])

            connection, _ = connect_deliberation_db(run_dir)
            try:
                third_statuses = migration_statuses(connection)
            finally:
                connection.close()

            self.assertEqual(second_statuses, third_statuses)

    def test_failed_schema_migration_records_failure_and_can_be_retried(self) -> None:
        from eco_council_runtime.kernel.core.schema_migrations import (
            apply_schema_migration,
            load_schema_status,
        )

        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        try:
            with self.assertRaises(RuntimeError):
                apply_schema_migration(
                    connection,
                    schema_name="unit-test-schema",
                    migration_id="0001-unit-test-failure",
                    target_version="unit-test-v1",
                    description="Exercise failed migration status.",
                    operation=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
                )
            failed_status = load_schema_status(connection)

            apply_schema_migration(
                connection,
                schema_name="unit-test-schema",
                migration_id="0001-unit-test-failure",
                target_version="unit-test-v1",
                description="Exercise failed migration status.",
                operation=lambda: connection.execute(
                    "CREATE TABLE unit_test_migrated (id TEXT PRIMARY KEY)"
                ),
            )
            recovered_status = load_schema_status(connection)
        finally:
            connection.close()

        self.assertEqual("failed", failed_status["status"])
        self.assertEqual(1, failed_status["summary"]["failed_migration_count"])
        self.assertEqual("completed", recovered_status["status"])
        self.assertEqual(0, recovered_status["summary"]["failed_migration_count"])

    def test_show_schema_status_cli_reports_migration_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            from eco_council_runtime.kernel.cli import main

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                status_code = main(
                    [
                        "show-schema-status",
                        "--run-dir",
                        str(run_dir),
                    ]
                )

            payload = json.loads(stdout.getvalue())
            schema_names = {
                item["schema_name"]
                for item in payload["metadata"]
                if isinstance(item, dict)
            }
            migration_ids = {
                item["migration_id"]
                for item in payload["migrations"]
                if isinstance(item, dict)
            }

            self.assertEqual(0, status_code)
            self.assertEqual("schema-status-v1", payload["schema_version"])
            self.assertEqual("completed", payload["status"])
            self.assertIn("deliberation-plane", schema_names)
            self.assertIn("0001-deliberation-schema-baseline", migration_ids)


if __name__ == "__main__":
    unittest.main()
