from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS board_runs (
    run_id TEXT PRIMARY KEY,
    board_revision INTEGER NOT NULL DEFAULT 0,
    updated_at_utc TEXT NOT NULL DEFAULT '',
    board_path TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS board_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    event_index INTEGER NOT NULL DEFAULT 0,
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_board_events_round_created
ON board_events(run_id, round_id, created_at_utc, event_id);
CREATE INDEX IF NOT EXISTS idx_board_events_round_sequence
ON board_events(run_id, round_id, event_index, event_id);

CREATE TABLE IF NOT EXISTS board_notes (
    note_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    author_role TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    note_text TEXT NOT NULL DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    linked_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
    related_ids_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_board_notes_round_created
ON board_notes(run_id, round_id, created_at_utc, note_id);

CREATE TABLE IF NOT EXISTS hypothesis_cards (
    hypothesis_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    statement TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    owner_role TEXT NOT NULL DEFAULT '',
    linked_claim_ids_json TEXT NOT NULL DEFAULT '[]',
    confidence REAL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    updated_at_utc TEXT NOT NULL DEFAULT '',
    carryover_from_round_id TEXT NOT NULL DEFAULT '',
    carryover_from_hypothesis_id TEXT NOT NULL DEFAULT '',
    history_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_hypothesis_cards_round_status
ON hypothesis_cards(run_id, round_id, status, updated_at_utc, hypothesis_id);

CREATE TABLE IF NOT EXISTS challenge_tickets (
    ticket_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    priority TEXT NOT NULL DEFAULT '',
    owner_role TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    challenge_statement TEXT NOT NULL DEFAULT '',
    target_claim_id TEXT NOT NULL DEFAULT '',
    target_hypothesis_id TEXT NOT NULL DEFAULT '',
    linked_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
    related_task_ids_json TEXT NOT NULL DEFAULT '[]',
    closed_at_utc TEXT NOT NULL DEFAULT '',
    closed_by_role TEXT NOT NULL DEFAULT '',
    resolution TEXT NOT NULL DEFAULT '',
    resolution_note TEXT NOT NULL DEFAULT '',
    history_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_challenge_tickets_round_status
ON challenge_tickets(run_id, round_id, status, created_at_utc, ticket_id);

CREATE TABLE IF NOT EXISTS board_tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    task_text TEXT NOT NULL DEFAULT '',
    task_type TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    owner_role TEXT NOT NULL DEFAULT '',
    priority TEXT NOT NULL DEFAULT '',
    source_round_id TEXT NOT NULL DEFAULT '',
    source_ticket_id TEXT NOT NULL DEFAULT '',
    source_hypothesis_id TEXT NOT NULL DEFAULT '',
    carryover_from_round_id TEXT NOT NULL DEFAULT '',
    carryover_from_task_id TEXT NOT NULL DEFAULT '',
    linked_artifact_refs_json TEXT NOT NULL DEFAULT '[]',
    related_ids_json TEXT NOT NULL DEFAULT '[]',
    created_at_utc TEXT NOT NULL DEFAULT '',
    updated_at_utc TEXT NOT NULL DEFAULT '',
    claimed_at_utc TEXT NOT NULL DEFAULT '',
    history_json TEXT NOT NULL DEFAULT '[]',
    board_revision INTEGER NOT NULL DEFAULT 0,
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_board_tasks_round_status
ON board_tasks(run_id, round_id, status, updated_at_utc, task_id);

CREATE TABLE IF NOT EXISTS round_transitions (
    transition_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    source_round_id TEXT NOT NULL DEFAULT '',
    generated_at_utc TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    event_id TEXT NOT NULL DEFAULT '',
    board_revision INTEGER NOT NULL DEFAULT 0,
    prior_round_ids_json TEXT NOT NULL DEFAULT '[]',
    cross_round_query_hints_json TEXT NOT NULL DEFAULT '{}',
    counts_json TEXT NOT NULL DEFAULT '{}',
    warnings_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_round_transitions_round
ON round_transitions(run_id, round_id, generated_at_utc, transition_id);

CREATE TABLE IF NOT EXISTS promotion_freezes (
    freeze_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL DEFAULT '',
    gate_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    promotion_status TEXT NOT NULL DEFAULT '',
    controller_status TEXT NOT NULL DEFAULT '',
    supervisor_status TEXT NOT NULL DEFAULT '',
    planning_mode TEXT NOT NULL DEFAULT '',
    promote_allowed INTEGER NOT NULL DEFAULT 0,
    gate_reasons_json TEXT NOT NULL DEFAULT '[]',
    recommended_next_skills_json TEXT NOT NULL DEFAULT '[]',
    controller_artifact_path TEXT NOT NULL DEFAULT '',
    gate_artifact_path TEXT NOT NULL DEFAULT '',
    supervisor_artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_promotion_freezes_round_updated
ON promotion_freezes(run_id, round_id, updated_at_utc, freeze_id);
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    import hashlib

    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def decode_json(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default


def coerce_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_db_path(run_dir: Path) -> Path:
    # Transitional bootstrap: reuse the existing run-local SQLite surface.
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
    ensure_schema_migrations(connection)
    return connection, file_path


def ensure_schema_migrations(connection: sqlite3.Connection) -> None:
    ensure_column(
        connection,
        "board_events",
        "event_index",
        "INTEGER NOT NULL DEFAULT 0",
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_board_events_round_sequence
        ON board_events(run_id, round_id, event_index, event_id)
        """
    )


def ensure_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    if any(maybe_text(row["name"]) == column_name for row in rows):
        return
    connection.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"
    )


def resolve_board_path(run_dir: Path, board_path: str | Path = "") -> Path:
    if isinstance(board_path, Path):
        return board_path.expanduser().resolve()
    text = maybe_text(board_path)
    if not text:
        return (run_dir / "board" / "investigation_board.json").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temp_path, path)


def empty_round_state() -> dict[str, list[dict[str, Any]]]:
    return {
        "notes": [],
        "challenge_tickets": [],
        "hypotheses": [],
        "tasks": [],
    }


def ensure_round_state(rounds: dict[str, dict[str, list[dict[str, Any]]]], round_id: str) -> dict[str, list[dict[str, Any]]]:
    round_key = maybe_text(round_id)
    state = rounds.get(round_key)
    if not isinstance(state, dict):
        state = empty_round_state()
        rounds[round_key] = state
    state.setdefault("notes", [])
    state.setdefault("challenge_tickets", [])
    state.setdefault("hypotheses", [])
    state.setdefault("tasks", [])
    return state


def board_has_state(connection: sqlite3.Connection, *, run_id: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM (
            SELECT run_id FROM board_events WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM board_notes WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM hypothesis_cards WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM challenge_tickets WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM board_tasks WHERE run_id = ?
            UNION ALL
            SELECT run_id FROM round_transitions WHERE run_id = ?
        )
        LIMIT 1
        """,
        (run_id, run_id, run_id, run_id, run_id, run_id),
    ).fetchone()
    return row is not None


def infer_board_revision(connection: sqlite3.Connection, *, run_id: str) -> int:
    revisions = [
        coerce_int(
            connection.execute(
                f"SELECT COALESCE(MAX(board_revision), 0) AS value FROM {table_name} WHERE run_id = ?",
                (run_id,),
            ).fetchone()["value"]
        )
        for table_name in (
            "board_events",
            "board_notes",
            "hypothesis_cards",
            "challenge_tickets",
            "board_tasks",
            "round_transitions",
        )
    ]
    return max(revisions) if revisions else 0


def infer_board_path(connection: sqlite3.Connection, *, run_id: str) -> str:
    for table_name in (
        "board_events",
        "board_notes",
        "hypothesis_cards",
        "challenge_tickets",
        "board_tasks",
    ):
        row = connection.execute(
            f"""
            SELECT artifact_path
            FROM {table_name}
            WHERE run_id = ? AND artifact_path != ''
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        if row is not None and maybe_text(row["artifact_path"]):
            return maybe_text(row["artifact_path"])
    return ""


def fetch_board_run(connection: sqlite3.Connection, *, run_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT run_id, board_revision, updated_at_utc, board_path
        FROM board_runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is not None:
        return {
            "run_id": maybe_text(row["run_id"]),
            "board_revision": coerce_int(row["board_revision"]),
            "updated_at_utc": maybe_text(row["updated_at_utc"]),
            "board_path": maybe_text(row["board_path"]),
        }
    if not board_has_state(connection, run_id=run_id):
        return None
    return {
        "run_id": run_id,
        "board_revision": infer_board_revision(connection, run_id=run_id),
        "updated_at_utc": "",
        "board_path": infer_board_path(connection, run_id=run_id),
    }


def upsert_board_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    board_revision: int,
    updated_at_utc: str,
    board_path: str,
) -> None:
    connection.execute(
        """
        INSERT INTO board_runs (run_id, board_revision, updated_at_utc, board_path)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            board_revision = excluded.board_revision,
            updated_at_utc = excluded.updated_at_utc,
            board_path = excluded.board_path
        """,
        (run_id, coerce_int(board_revision), maybe_text(updated_at_utc), maybe_text(board_path)),
    )


def next_event_index(connection: sqlite3.Connection, *, run_id: str) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(event_index), -1) AS max_event_index
        FROM board_events
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    return coerce_int(row["max_event_index"]) + 1 if row is not None else 0


def load_raw_board_record(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    id_column: str,
    record_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        f"SELECT raw_json FROM {table_name} WHERE {id_column} = ?",
        (maybe_text(record_id),),
    ).fetchone()
    if row is None:
        return None
    payload = decode_json(maybe_text(row["raw_json"]), {})
    return payload if isinstance(payload, dict) else None


def write_board_event_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO board_events (
            event_id, run_id, round_id, event_type, created_at_utc, payload_json,
            event_index, board_revision, artifact_path, record_locator, raw_json
        ) VALUES (
            :event_id, :run_id, :round_id, :event_type, :created_at_utc, :payload_json,
            :event_index, :board_revision, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_board_note_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO board_notes (
            note_id, run_id, round_id, created_at_utc, author_role, category, note_text,
            tags_json, linked_artifact_refs_json, related_ids_json, board_revision,
            artifact_path, record_locator, raw_json
        ) VALUES (
            :note_id, :run_id, :round_id, :created_at_utc, :author_role, :category, :note_text,
            :tags_json, :linked_artifact_refs_json, :related_ids_json, :board_revision,
            :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_hypothesis_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO hypothesis_cards (
            hypothesis_id, run_id, round_id, title, statement, status, owner_role,
            linked_claim_ids_json, confidence, created_at_utc, updated_at_utc,
            carryover_from_round_id, carryover_from_hypothesis_id, history_json,
            board_revision, artifact_path, record_locator, raw_json
        ) VALUES (
            :hypothesis_id, :run_id, :round_id, :title, :statement, :status, :owner_role,
            :linked_claim_ids_json, :confidence, :created_at_utc, :updated_at_utc,
            :carryover_from_round_id, :carryover_from_hypothesis_id, :history_json,
            :board_revision, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_challenge_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO challenge_tickets (
            ticket_id, run_id, round_id, created_at_utc, status, priority, owner_role,
            title, challenge_statement, target_claim_id, target_hypothesis_id,
            linked_artifact_refs_json, related_task_ids_json, closed_at_utc, closed_by_role,
            resolution, resolution_note, history_json, board_revision, artifact_path,
            record_locator, raw_json
        ) VALUES (
            :ticket_id, :run_id, :round_id, :created_at_utc, :status, :priority, :owner_role,
            :title, :challenge_statement, :target_claim_id, :target_hypothesis_id,
            :linked_artifact_refs_json, :related_task_ids_json, :closed_at_utc, :closed_by_role,
            :resolution, :resolution_note, :history_json, :board_revision, :artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def write_board_task_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO board_tasks (
            task_id, run_id, round_id, title, task_text, task_type, status, owner_role,
            priority, source_round_id, source_ticket_id, source_hypothesis_id,
            carryover_from_round_id, carryover_from_task_id, linked_artifact_refs_json,
            related_ids_json, created_at_utc, updated_at_utc, claimed_at_utc, history_json,
            board_revision, artifact_path, record_locator, raw_json
        ) VALUES (
            :task_id, :run_id, :round_id, :title, :task_text, :task_type, :status, :owner_role,
            :priority, :source_round_id, :source_ticket_id, :source_hypothesis_id,
            :carryover_from_round_id, :carryover_from_task_id, :linked_artifact_refs_json,
            :related_ids_json, :created_at_utc, :updated_at_utc, :claimed_at_utc, :history_json,
            :board_revision, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_round_transition_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO round_transitions (
            transition_id, run_id, round_id, source_round_id, generated_at_utc, operation,
            event_id, board_revision, prior_round_ids_json, cross_round_query_hints_json,
            counts_json, warnings_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :transition_id, :run_id, :round_id, :source_round_id, :generated_at_utc, :operation,
            :event_id, :board_revision, :prior_round_ids_json, :cross_round_query_hints_json,
            :counts_json, :warnings_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def write_promotion_freeze_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO promotion_freezes (
            freeze_id, run_id, round_id, updated_at_utc, gate_status, readiness_status,
            promotion_status, controller_status, supervisor_status, planning_mode,
            promote_allowed, gate_reasons_json, recommended_next_skills_json,
            controller_artifact_path, gate_artifact_path, supervisor_artifact_path,
            record_locator, raw_json
        ) VALUES (
            :freeze_id, :run_id, :round_id, :updated_at_utc, :gate_status, :readiness_status,
            :promotion_status, :controller_status, :supervisor_status, :planning_mode,
            :promote_allowed, :gate_reasons_json, :recommended_next_skills_json,
            :controller_artifact_path, :gate_artifact_path, :supervisor_artifact_path,
            :record_locator, :raw_json
        )
        """,
        row,
    )


def event_row_from_payload(
    event: dict[str, Any],
    *,
    event_index: int,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "event_id": maybe_text(event.get("event_id")),
        "run_id": maybe_text(event.get("run_id")),
        "round_id": maybe_text(event.get("round_id")),
        "event_type": maybe_text(event.get("event_type")),
        "created_at_utc": maybe_text(event.get("created_at_utc")),
        "payload_json": json_text(event.get("payload", {})),
        "event_index": coerce_int(event_index),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(event),
    }


def note_row_from_payload(
    note: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "note_id": maybe_text(note.get("note_id")),
        "run_id": maybe_text(note.get("run_id")),
        "round_id": maybe_text(note.get("round_id")),
        "created_at_utc": maybe_text(note.get("created_at_utc")),
        "author_role": maybe_text(note.get("author_role")),
        "category": maybe_text(note.get("category")),
        "note_text": maybe_text(note.get("note_text")),
        "tags_json": json_text(note.get("tags", [])),
        "linked_artifact_refs_json": json_text(note.get("linked_artifact_refs", [])),
        "related_ids_json": json_text(note.get("related_ids", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(note),
    }


def hypothesis_row_from_payload(
    hypothesis: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
        "run_id": maybe_text(hypothesis.get("run_id")),
        "round_id": maybe_text(hypothesis.get("round_id")),
        "title": maybe_text(hypothesis.get("title")),
        "statement": maybe_text(hypothesis.get("statement")),
        "status": maybe_text(hypothesis.get("status")),
        "owner_role": maybe_text(hypothesis.get("owner_role")),
        "linked_claim_ids_json": json_text(hypothesis.get("linked_claim_ids", [])),
        "confidence": hypothesis.get("confidence"),
        "created_at_utc": maybe_text(hypothesis.get("created_at_utc")),
        "updated_at_utc": maybe_text(hypothesis.get("updated_at_utc")),
        "carryover_from_round_id": maybe_text(hypothesis.get("carryover_from_round_id")),
        "carryover_from_hypothesis_id": maybe_text(hypothesis.get("carryover_from_hypothesis_id")),
        "history_json": json_text(hypothesis.get("history", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(hypothesis),
    }


def challenge_row_from_payload(
    ticket: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "ticket_id": maybe_text(ticket.get("ticket_id")),
        "run_id": maybe_text(ticket.get("run_id")),
        "round_id": maybe_text(ticket.get("round_id")),
        "created_at_utc": maybe_text(ticket.get("created_at_utc")),
        "status": maybe_text(ticket.get("status")),
        "priority": maybe_text(ticket.get("priority")),
        "owner_role": maybe_text(ticket.get("owner_role")),
        "title": maybe_text(ticket.get("title")),
        "challenge_statement": maybe_text(ticket.get("challenge_statement")),
        "target_claim_id": maybe_text(ticket.get("target_claim_id")),
        "target_hypothesis_id": maybe_text(ticket.get("target_hypothesis_id")),
        "linked_artifact_refs_json": json_text(ticket.get("linked_artifact_refs", [])),
        "related_task_ids_json": json_text(ticket.get("related_task_ids", [])),
        "closed_at_utc": maybe_text(ticket.get("closed_at_utc")),
        "closed_by_role": maybe_text(ticket.get("closed_by_role")),
        "resolution": maybe_text(ticket.get("resolution")),
        "resolution_note": maybe_text(ticket.get("resolution_note")),
        "history_json": json_text(ticket.get("history", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(ticket),
    }


def board_task_row_from_payload(
    task: dict[str, Any],
    *,
    board_revision: int,
    board_path: Path,
    record_locator: str = "",
) -> dict[str, Any]:
    return {
        "task_id": maybe_text(task.get("task_id")),
        "run_id": maybe_text(task.get("run_id")),
        "round_id": maybe_text(task.get("round_id")),
        "title": maybe_text(task.get("title")),
        "task_text": maybe_text(task.get("task_text")),
        "task_type": maybe_text(task.get("task_type")),
        "status": maybe_text(task.get("status")),
        "owner_role": maybe_text(task.get("owner_role")),
        "priority": maybe_text(task.get("priority")),
        "source_round_id": maybe_text(task.get("source_round_id")),
        "source_ticket_id": maybe_text(task.get("source_ticket_id")),
        "source_hypothesis_id": maybe_text(task.get("source_hypothesis_id")),
        "carryover_from_round_id": maybe_text(task.get("carryover_from_round_id")),
        "carryover_from_task_id": maybe_text(task.get("carryover_from_task_id")),
        "linked_artifact_refs_json": json_text(task.get("linked_artifact_refs", [])),
        "related_ids_json": json_text(task.get("related_ids", [])),
        "created_at_utc": maybe_text(task.get("created_at_utc")),
        "updated_at_utc": maybe_text(task.get("updated_at_utc")),
        "claimed_at_utc": maybe_text(task.get("claimed_at_utc")),
        "history_json": json_text(task.get("history", [])),
        "board_revision": coerce_int(board_revision),
        "artifact_path": str(board_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(task),
    }


def round_transition_row_from_payload(
    transition: dict[str, Any],
    *,
    board_revision: int,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "transition_id": maybe_text(transition.get("transition_id")),
        "run_id": maybe_text(transition.get("run_id")),
        "round_id": maybe_text(transition.get("round_id")),
        "source_round_id": maybe_text(transition.get("source_round_id")),
        "generated_at_utc": maybe_text(transition.get("generated_at_utc")),
        "operation": maybe_text(transition.get("operation")),
        "event_id": maybe_text(transition.get("event_id")),
        "board_revision": coerce_int(
            transition.get("board_revision") or board_revision
        ),
        "prior_round_ids_json": json_text(transition.get("prior_round_ids", [])),
        "cross_round_query_hints_json": json_text(
            transition.get("cross_round_query_hints", {})
        ),
        "counts_json": json_text(transition.get("counts", {})),
        "warnings_json": json_text(transition.get("warnings", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(transition),
    }


def promotion_freeze_record_id(run_id: str, round_id: str) -> str:
    return "freeze-" + stable_hash("promotion-freeze", run_id, round_id)[:12]


def promotion_freeze_row_from_payload(
    freeze_record: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    artifacts = freeze_record.get("artifacts", {}) if isinstance(freeze_record.get("artifacts"), dict) else {}
    return {
        "freeze_id": maybe_text(freeze_record.get("freeze_id")),
        "run_id": maybe_text(freeze_record.get("run_id")),
        "round_id": maybe_text(freeze_record.get("round_id")),
        "updated_at_utc": maybe_text(freeze_record.get("updated_at_utc")),
        "gate_status": maybe_text(freeze_record.get("gate_status")),
        "readiness_status": maybe_text(freeze_record.get("readiness_status")),
        "promotion_status": maybe_text(freeze_record.get("promotion_status")),
        "controller_status": maybe_text(freeze_record.get("controller_status")),
        "supervisor_status": maybe_text(freeze_record.get("supervisor_status")),
        "planning_mode": maybe_text(freeze_record.get("planning_mode")),
        "promote_allowed": 1 if bool(freeze_record.get("promote_allowed")) else 0,
        "gate_reasons_json": json_text(freeze_record.get("gate_reasons", [])),
        "recommended_next_skills_json": json_text(freeze_record.get("recommended_next_skills", [])),
        "controller_artifact_path": maybe_text(artifacts.get("controller_state_path")),
        "gate_artifact_path": maybe_text(artifacts.get("promotion_gate_path")),
        "supervisor_artifact_path": maybe_text(artifacts.get("supervisor_state_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(freeze_record),
    }


def fetch_promotion_freeze(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any] | None:
    normalized_run_id = maybe_text(run_id)
    normalized_round_id = maybe_text(round_id)
    clauses: list[str] = []
    params: list[str] = []
    if normalized_run_id:
        clauses.append("run_id = ?")
        params.append(normalized_run_id)
    if normalized_round_id:
        clauses.append("round_id = ?")
        params.append(normalized_round_id)
    if not clauses:
        return None
    row = connection.execute(
        f"""
        SELECT raw_json
        FROM promotion_freezes
        WHERE {' AND '.join(clauses)}
        ORDER BY updated_at_utc DESC, freeze_id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    payload = decode_json(maybe_text(row["raw_json"]), {})
    return payload if isinstance(payload, dict) else None


def resolved_promotion_freeze_artifacts(
    existing_record: dict[str, Any],
    *,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, str]:
    existing_artifacts = (
        existing_record.get("artifacts", {})
        if isinstance(existing_record.get("artifacts"), dict)
        else {}
    )
    controller_artifacts = (
        controller_snapshot.get("artifacts", {})
        if isinstance(controller_snapshot, dict)
        and isinstance(controller_snapshot.get("artifacts"), dict)
        else {}
    )
    supervisor_inspection = (
        supervisor_snapshot.get("inspection_paths", {})
        if isinstance(supervisor_snapshot, dict)
        and isinstance(supervisor_snapshot.get("inspection_paths"), dict)
        else {}
    )
    explicit = artifact_paths if isinstance(artifact_paths, dict) else {}
    gate_output_path = (
        maybe_text(gate_snapshot.get("output_path"))
        if isinstance(gate_snapshot, dict)
        else ""
    )
    supervisor_gate_path = (
        maybe_text(supervisor_snapshot.get("promotion_gate_path"))
        if isinstance(supervisor_snapshot, dict)
        else ""
    )
    supervisor_path = (
        maybe_text(supervisor_snapshot.get("supervisor_path"))
        if isinstance(supervisor_snapshot, dict)
        else ""
    )
    return {
        "controller_state_path": maybe_text(explicit.get("controller_state_path"))
        or maybe_text(controller_artifacts.get("controller_state_path"))
        or maybe_text(existing_artifacts.get("controller_state_path")),
        "promotion_gate_path": maybe_text(explicit.get("promotion_gate_path"))
        or gate_output_path
        or maybe_text(controller_artifacts.get("promotion_gate_path"))
        or supervisor_gate_path
        or maybe_text(supervisor_inspection.get("gate_path"))
        or maybe_text(existing_artifacts.get("promotion_gate_path")),
        "supervisor_state_path": maybe_text(explicit.get("supervisor_state_path"))
        or supervisor_path
        or maybe_text(existing_artifacts.get("supervisor_state_path")),
    }


def merged_promotion_freeze_record(
    *,
    run_id: str,
    round_id: str,
    existing_record: dict[str, Any] | None = None,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = dict(existing_record) if isinstance(existing_record, dict) else {}
    normalized_run_id = maybe_text(run_id) or maybe_text(record.get("run_id"))
    normalized_round_id = maybe_text(round_id) or maybe_text(record.get("round_id"))
    record["schema_version"] = "deliberation-promotion-freeze-v1"
    record["freeze_id"] = maybe_text(record.get("freeze_id")) or promotion_freeze_record_id(
        normalized_run_id,
        normalized_round_id,
    )
    record["run_id"] = normalized_run_id
    record["round_id"] = normalized_round_id
    if isinstance(controller_snapshot, dict) and controller_snapshot:
        record["controller_snapshot"] = controller_snapshot
    if isinstance(gate_snapshot, dict) and gate_snapshot:
        record["gate_snapshot"] = gate_snapshot
    if isinstance(supervisor_snapshot, dict) and supervisor_snapshot:
        record["supervisor_snapshot"] = supervisor_snapshot

    resolved_controller = (
        record.get("controller_snapshot", {})
        if isinstance(record.get("controller_snapshot"), dict)
        else {}
    )
    resolved_gate = (
        record.get("gate_snapshot", {})
        if isinstance(record.get("gate_snapshot"), dict)
        else {}
    )
    resolved_supervisor = (
        record.get("supervisor_snapshot", {})
        if isinstance(record.get("supervisor_snapshot"), dict)
        else {}
    )
    record["updated_at_utc"] = (
        maybe_text(resolved_supervisor.get("generated_at_utc"))
        or maybe_text(resolved_controller.get("generated_at_utc"))
        or maybe_text(resolved_gate.get("generated_at_utc"))
        or maybe_text(record.get("updated_at_utc"))
        or utc_now_iso()
    )
    record["gate_status"] = (
        maybe_text(resolved_supervisor.get("gate_status"))
        or maybe_text(resolved_controller.get("gate_status"))
        or maybe_text(resolved_gate.get("gate_status"))
        or maybe_text(record.get("gate_status"))
    )
    record["readiness_status"] = (
        maybe_text(resolved_supervisor.get("readiness_status"))
        or maybe_text(resolved_controller.get("readiness_status"))
        or maybe_text(resolved_gate.get("readiness_status"))
        or maybe_text(record.get("readiness_status"))
    )
    record["promotion_status"] = (
        maybe_text(resolved_supervisor.get("promotion_status"))
        or maybe_text(resolved_controller.get("promotion_status"))
        or maybe_text(record.get("promotion_status"))
    )
    record["controller_status"] = (
        maybe_text(resolved_controller.get("controller_status"))
        or maybe_text(record.get("controller_status"))
    )
    record["supervisor_status"] = (
        maybe_text(resolved_supervisor.get("supervisor_status"))
        or maybe_text(record.get("supervisor_status"))
    )
    record["planning_mode"] = (
        maybe_text(resolved_supervisor.get("planning_mode"))
        or maybe_text(resolved_controller.get("planning_mode"))
        or maybe_text(
            resolved_controller.get("planning", {}).get("planning_mode")
            if isinstance(resolved_controller.get("planning"), dict)
            else ""
        )
        or maybe_text(record.get("planning_mode"))
    )
    gate_present = isinstance(resolved_gate, dict) and bool(resolved_gate)
    promote_allowed = (
        bool(resolved_gate.get("promote_allowed"))
        if gate_present
        else bool(record.get("promote_allowed"))
    )
    if record["gate_status"] == "allow-promote":
        promote_allowed = True
    record["promote_allowed"] = promote_allowed
    record["gate_reasons"] = unique_texts(
        (
            resolved_supervisor.get("gate_reasons", [])
            if isinstance(resolved_supervisor.get("gate_reasons"), list)
            else []
        )
        + (
            resolved_controller.get("gate_reasons", [])
            if isinstance(resolved_controller.get("gate_reasons"), list)
            else []
        )
        + (
            resolved_gate.get("gate_reasons", [])
            if isinstance(resolved_gate.get("gate_reasons"), list)
            else []
        )
        + (
            record.get("gate_reasons", [])
            if isinstance(record.get("gate_reasons"), list)
            else []
        )
    )
    record["recommended_next_skills"] = unique_texts(
        (
            resolved_supervisor.get("recommended_next_skills", [])
            if isinstance(resolved_supervisor.get("recommended_next_skills"), list)
            else []
        )
        + (
            resolved_controller.get("recommended_next_skills", [])
            if isinstance(resolved_controller.get("recommended_next_skills"), list)
            else []
        )
        + (
            resolved_gate.get("recommended_next_skills", [])
            if isinstance(resolved_gate.get("recommended_next_skills"), list)
            else []
        )
        + (
            record.get("recommended_next_skills", [])
            if isinstance(record.get("recommended_next_skills"), list)
            else []
        )
    )
    record["artifacts"] = resolved_promotion_freeze_artifacts(
        record,
        controller_snapshot=resolved_controller,
        gate_snapshot=resolved_gate,
        supervisor_snapshot=resolved_supervisor,
        artifact_paths=artifact_paths,
    )
    return record


def store_promotion_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            existing_record = fetch_promotion_freeze(
                connection,
                run_id=run_id,
                round_id=round_id,
            )
            freeze_record = merged_promotion_freeze_record(
                run_id=run_id,
                round_id=round_id,
                existing_record=existing_record,
                controller_snapshot=controller_snapshot,
                gate_snapshot=gate_snapshot,
                supervisor_snapshot=supervisor_snapshot,
                artifact_paths=artifact_paths,
            )
            write_promotion_freeze_row(
                connection,
                promotion_freeze_row_from_payload(freeze_record),
            )
    finally:
        connection.close()
    return freeze_record


def load_promotion_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_promotion_freeze(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()


def load_phase2_control_state(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    freeze_record = load_promotion_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    return {
        "promotion_freeze": freeze_record,
        "controller": (
            freeze_record.get("controller_snapshot", {})
            if isinstance(freeze_record.get("controller_snapshot"), dict)
            else {}
        ),
        "promotion_gate": (
            freeze_record.get("gate_snapshot", {})
            if isinstance(freeze_record.get("gate_snapshot"), dict)
            else {}
        ),
        "supervisor": (
            freeze_record.get("supervisor_snapshot", {})
            if isinstance(freeze_record.get("supervisor_snapshot"), dict)
            else {}
        ),
    }


def iter_round_transition_rows(run_dir: Path, *, run_id: str) -> list[dict[str, Any]]:
    runtime_dir = run_dir / "runtime"
    if not runtime_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for file_path in sorted(runtime_dir.glob("round_transition_*.json")):
        payload = load_json_if_exists(file_path)
        if not isinstance(payload, dict):
            continue
        payload_run_id = maybe_text(payload.get("run_id"))
        if run_id and payload_run_id and payload_run_id != run_id:
            continue
        transition_id = maybe_text(payload.get("transition_id"))
        round_id = maybe_text(payload.get("round_id"))
        if not transition_id or not round_id:
            continue
        rows.append(
            round_transition_row_from_payload(
                payload,
                board_revision=coerce_int(payload.get("board_revision")),
                artifact_path=str(file_path.resolve()),
                record_locator="$",
            )
        )
    return rows


def sync_board_to_deliberation_plane(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    board_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    db_file = resolve_db_path(run_dir_path, db_path)
    board_payload = load_json_if_exists(board_file)
    if not isinstance(board_payload, dict):
        return {
            "status": "missing-board",
            "run_id": maybe_text(expected_run_id),
            "board_path": str(board_file),
            "db_path": str(db_file),
            "board_revision": 0,
            "event_count": 0,
            "round_count": 0,
            "note_count": 0,
            "hypothesis_count": 0,
            "challenge_ticket_count": 0,
            "task_count": 0,
            "round_transition_count": 0,
        }

    run_id = maybe_text(board_payload.get("run_id")) or maybe_text(expected_run_id)
    if maybe_text(expected_run_id) and run_id and run_id != maybe_text(expected_run_id):
        raise ValueError(
            f"Board run_id mismatch: board has {run_id!r} but expected {maybe_text(expected_run_id)!r}."
        )
    if not run_id:
        raise ValueError(f"Board artifact is missing run_id: {board_file}")

    board_revision = coerce_int(board_payload.get("board_revision"))
    updated_at_utc = maybe_text(board_payload.get("updated_at_utc"))
    rounds = board_payload.get("rounds", {}) if isinstance(board_payload.get("rounds"), dict) else {}
    events = board_payload.get("events", []) if isinstance(board_payload.get("events"), list) else []

    event_rows: list[dict[str, Any]] = []
    note_rows: list[dict[str, Any]] = []
    hypothesis_rows: list[dict[str, Any]] = []
    challenge_rows: list[dict[str, Any]] = []
    task_rows: list[dict[str, Any]] = []

    for index, event in enumerate(events):
        if not isinstance(event, dict):
            continue
        event_id = maybe_text(event.get("event_id"))
        round_id = maybe_text(event.get("round_id"))
        if not event_id or not round_id:
            continue
        resolved_event = {
            **event,
            "event_id": event_id,
            "run_id": maybe_text(event.get("run_id")) or run_id,
            "round_id": round_id,
        }
        event_rows.append(
            event_row_from_payload(
                resolved_event,
                event_index=index,
                board_revision=board_revision,
                board_path=board_file,
                record_locator=f"$.events[{index}]",
            )
        )

    for round_id, round_state in rounds.items():
        if not isinstance(round_state, dict):
            continue
        notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
        hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
        challenges = (
            round_state.get("challenge_tickets")
            if isinstance(round_state.get("challenge_tickets"), list)
            else []
        )
        tasks = round_state.get("tasks", []) if isinstance(round_state.get("tasks"), list) else []

        for index, note in enumerate(notes):
            if not isinstance(note, dict):
                continue
            note_id = maybe_text(note.get("note_id"))
            if not note_id:
                continue
            resolved_note = {
                **note,
                "note_id": note_id,
                "run_id": maybe_text(note.get("run_id")) or run_id,
                "round_id": maybe_text(note.get("round_id")) or maybe_text(round_id),
            }
            note_rows.append(
                note_row_from_payload(
                    resolved_note,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.notes[{index}]",
                )
            )

        for index, hypothesis in enumerate(hypotheses):
            if not isinstance(hypothesis, dict):
                continue
            hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
            if not hypothesis_id:
                continue
            resolved_hypothesis = {
                **hypothesis,
                "hypothesis_id": hypothesis_id,
                "run_id": maybe_text(hypothesis.get("run_id")) or run_id,
                "round_id": maybe_text(hypothesis.get("round_id")) or maybe_text(round_id),
            }
            hypothesis_rows.append(
                hypothesis_row_from_payload(
                    resolved_hypothesis,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.hypotheses[{index}]",
                )
            )

        for index, ticket in enumerate(challenges):
            if not isinstance(ticket, dict):
                continue
            ticket_id = maybe_text(ticket.get("ticket_id"))
            if not ticket_id:
                continue
            resolved_ticket = {
                **ticket,
                "ticket_id": ticket_id,
                "run_id": maybe_text(ticket.get("run_id")) or run_id,
                "round_id": maybe_text(ticket.get("round_id")) or maybe_text(round_id),
            }
            challenge_rows.append(
                challenge_row_from_payload(
                    resolved_ticket,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.challenge_tickets[{index}]",
                )
            )

        for index, task in enumerate(tasks):
            if not isinstance(task, dict):
                continue
            task_id = maybe_text(task.get("task_id"))
            if not task_id:
                continue
            resolved_task = {
                **task,
                "task_id": task_id,
                "run_id": maybe_text(task.get("run_id")) or run_id,
                "round_id": maybe_text(task.get("round_id")) or maybe_text(round_id),
            }
            task_rows.append(
                board_task_row_from_payload(
                    resolved_task,
                    board_revision=board_revision,
                    board_path=board_file,
                    record_locator=f"$.rounds.{round_id}.tasks[{index}]",
                )
            )

    round_transition_rows = iter_round_transition_rows(run_dir_path, run_id=run_id)

    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            for table_name in (
                "board_events",
                "board_notes",
                "hypothesis_cards",
                "challenge_tickets",
                "board_tasks",
                "round_transitions",
            ):
                connection.execute(f"DELETE FROM {table_name} WHERE run_id = ?", (run_id,))

            for row in event_rows:
                write_board_event_row(connection, row)
            for row in note_rows:
                write_board_note_row(connection, row)
            for row in hypothesis_rows:
                write_hypothesis_row(connection, row)
            for row in challenge_rows:
                write_challenge_row(connection, row)
            for row in task_rows:
                write_board_task_row(connection, row)
            for row in round_transition_rows:
                write_round_transition_row(connection, row)

            upsert_board_run(
                connection,
                run_id=run_id,
                board_revision=board_revision,
                updated_at_utc=updated_at_utc,
                board_path=str(board_file),
            )
    finally:
        connection.close()

    return {
        "status": "completed",
        "sync_mode": "json-import",
        "run_id": run_id,
        "board_path": str(board_file),
        "db_path": str(db_file),
        "board_revision": board_revision,
        "event_count": len(event_rows),
        "round_count": len(
            [round_id for round_id, round_state in rounds.items() if isinstance(round_state, dict)]
        ),
        "note_count": len(note_rows),
        "hypothesis_count": len(hypothesis_rows),
        "challenge_ticket_count": len(challenge_rows),
        "task_count": len(task_rows),
        "round_transition_count": len(round_transition_rows),
    }


def bootstrap_board_state(
    run_dir: str | Path,
    *,
    expected_run_id: str,
    board_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        board_run = fetch_board_run(connection, run_id=expected_run_id)
    finally:
        connection.close()

    board_payload = load_json_if_exists(board_file)
    if isinstance(board_payload, dict):
        board_run_id = maybe_text(board_payload.get("run_id")) or maybe_text(expected_run_id)
        if maybe_text(expected_run_id) and board_run_id and board_run_id != maybe_text(expected_run_id):
            raise ValueError(
                f"Board run_id mismatch: board has {board_run_id!r} but expected {maybe_text(expected_run_id)!r}."
            )
        file_revision = coerce_int(board_payload.get("board_revision"))
        current_revision = coerce_int(board_run.get("board_revision")) if isinstance(board_run, dict) else -1
        if board_run is None or file_revision > current_revision:
            sync_summary = sync_board_to_deliberation_plane(
                run_dir_path,
                expected_run_id=expected_run_id,
                board_path=board_file,
                db_path=db_path,
            )
            sync_summary["sync_mode"] = "json-import"
            return sync_summary
        return {
            "status": "completed",
            "sync_mode": "db-current",
            "run_id": maybe_text(expected_run_id),
            "board_path": str(board_file),
            "db_path": str(db_file),
            "board_revision": current_revision,
        }

    if isinstance(board_run, dict):
        return {
            "status": "completed",
            "sync_mode": "db-only",
            "run_id": maybe_text(expected_run_id),
            "board_path": maybe_text(board_run.get("board_path")) or str(board_file),
            "db_path": str(db_file),
            "board_revision": coerce_int(board_run.get("board_revision")),
        }

    return {
        "status": "missing-board",
        "sync_mode": "missing-board",
        "run_id": maybe_text(expected_run_id),
        "board_path": str(board_file),
        "db_path": str(db_file),
        "board_revision": 0,
    }


def export_board_from_connection(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    board_path: Path,
) -> dict[str, Any]:
    board_run = fetch_board_run(connection, run_id=run_id) or {
        "run_id": run_id,
        "board_revision": infer_board_revision(connection, run_id=run_id),
        "updated_at_utc": "",
        "board_path": str(board_path),
    }
    board_revision = coerce_int(board_run.get("board_revision"))
    updated_at_utc = maybe_text(board_run.get("updated_at_utc"))
    rounds: dict[str, dict[str, list[dict[str, Any]]]] = {}

    event_rows = connection.execute(
        """
        SELECT event_id, raw_json
        FROM board_events
        WHERE run_id = ?
        ORDER BY event_index, event_id
        """,
        (run_id,),
    ).fetchall()
    note_rows = connection.execute(
        """
        SELECT note_id, round_id, raw_json
        FROM board_notes
        WHERE run_id = ?
        ORDER BY round_id, created_at_utc, note_id
        """,
        (run_id,),
    ).fetchall()
    hypothesis_rows = connection.execute(
        """
        SELECT hypothesis_id, round_id, raw_json
        FROM hypothesis_cards
        WHERE run_id = ?
        ORDER BY round_id, updated_at_utc, hypothesis_id
        """,
        (run_id,),
    ).fetchall()
    challenge_rows = connection.execute(
        """
        SELECT ticket_id, round_id, raw_json
        FROM challenge_tickets
        WHERE run_id = ?
        ORDER BY round_id, created_at_utc, ticket_id
        """,
        (run_id,),
    ).fetchall()
    task_rows = connection.execute(
        """
        SELECT task_id, round_id, raw_json
        FROM board_tasks
        WHERE run_id = ?
        ORDER BY round_id, updated_at_utc, task_id
        """,
        (run_id,),
    ).fetchall()

    event_locators: dict[str, str] = {}
    note_locators: dict[str, str] = {}
    hypothesis_locators: dict[str, str] = {}
    challenge_locators: dict[str, str] = {}
    task_locators: dict[str, str] = {}
    events: list[dict[str, Any]] = []

    for row in event_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        event_id = maybe_text(payload.get("event_id")) or maybe_text(row["event_id"])
        round_id = maybe_text(payload.get("round_id"))
        if not event_id or not round_id:
            continue
        resolved = {
            **payload,
            "event_id": event_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        events.append(resolved)
        event_locators[event_id] = f"$.events[{len(events) - 1}]"
        ensure_round_state(rounds, round_id)

    for row in note_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        note_id = maybe_text(payload.get("note_id")) or maybe_text(row["note_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not note_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "note_id": note_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["notes"].append(resolved)
        note_locators[note_id] = f"$.rounds.{round_id}.notes[{len(state['notes']) - 1}]"

    for row in hypothesis_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        hypothesis_id = maybe_text(payload.get("hypothesis_id")) or maybe_text(row["hypothesis_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not hypothesis_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "hypothesis_id": hypothesis_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["hypotheses"].append(resolved)
        hypothesis_locators[hypothesis_id] = (
            f"$.rounds.{round_id}.hypotheses[{len(state['hypotheses']) - 1}]"
        )

    for row in challenge_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        ticket_id = maybe_text(payload.get("ticket_id")) or maybe_text(row["ticket_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not ticket_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "ticket_id": ticket_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["challenge_tickets"].append(resolved)
        challenge_locators[ticket_id] = (
            f"$.rounds.{round_id}.challenge_tickets[{len(state['challenge_tickets']) - 1}]"
        )

    for row in task_rows:
        payload = decode_json(maybe_text(row["raw_json"]), {})
        if not isinstance(payload, dict):
            continue
        task_id = maybe_text(payload.get("task_id")) or maybe_text(row["task_id"])
        round_id = maybe_text(payload.get("round_id")) or maybe_text(row["round_id"])
        if not task_id or not round_id:
            continue
        state = ensure_round_state(rounds, round_id)
        resolved = {
            **payload,
            "task_id": task_id,
            "run_id": maybe_text(payload.get("run_id")) or run_id,
            "round_id": round_id,
        }
        state["tasks"].append(resolved)
        task_locators[task_id] = f"$.rounds.{round_id}.tasks[{len(state['tasks']) - 1}]"

    if not updated_at_utc and events:
        updated_at_utc = maybe_text(events[-1].get("created_at_utc"))
    if not updated_at_utc:
        updated_at_utc = utc_now_iso()

    ordered_rounds = {
        round_id: ensure_round_state(rounds, round_id)
        for round_id in sorted(rounds)
    }
    board_payload = {
        "schema_version": "board-v1",
        "run_id": run_id,
        "board_revision": board_revision,
        "updated_at_utc": updated_at_utc,
        "events": events,
        "rounds": ordered_rounds,
    }

    upsert_board_run(
        connection,
        run_id=run_id,
        board_revision=board_revision,
        updated_at_utc=updated_at_utc,
        board_path=str(board_path),
    )
    for event_id, locator in event_locators.items():
        connection.execute(
            """
            UPDATE board_events
            SET artifact_path = ?, record_locator = ?
            WHERE event_id = ?
            """,
            (str(board_path), locator, event_id),
        )
    for note_id, locator in note_locators.items():
        connection.execute(
            """
            UPDATE board_notes
            SET artifact_path = ?, record_locator = ?
            WHERE note_id = ?
            """,
            (str(board_path), locator, note_id),
        )
    for hypothesis_id, locator in hypothesis_locators.items():
        connection.execute(
            """
            UPDATE hypothesis_cards
            SET artifact_path = ?, record_locator = ?
            WHERE hypothesis_id = ?
            """,
            (str(board_path), locator, hypothesis_id),
        )
    for ticket_id, locator in challenge_locators.items():
        connection.execute(
            """
            UPDATE challenge_tickets
            SET artifact_path = ?, record_locator = ?
            WHERE ticket_id = ?
            """,
            (str(board_path), locator, ticket_id),
        )
    for task_id, locator in task_locators.items():
        connection.execute(
            """
            UPDATE board_tasks
            SET artifact_path = ?, record_locator = ?
            WHERE task_id = ?
            """,
            (str(board_path), locator, task_id),
        )

    write_json_atomic(board_path, board_payload)
    return {
        "status": "completed",
        "run_id": run_id,
        "board_path": str(board_path),
        "board_revision": board_revision,
        "event_count": len(events),
        "round_count": len(ordered_rounds),
        "note_count": sum(len(state.get("notes", [])) for state in ordered_rounds.values()),
        "hypothesis_count": sum(len(state.get("hypotheses", [])) for state in ordered_rounds.values()),
        "challenge_ticket_count": sum(
            len(state.get("challenge_tickets", []))
            for state in ordered_rounds.values()
        ),
        "task_count": sum(len(state.get("tasks", [])) for state in ordered_rounds.values()),
        "record_locators": {
            "events": event_locators,
            "notes": note_locators,
            "hypotheses": hypothesis_locators,
            "challenge_tickets": challenge_locators,
            "tasks": task_locators,
        },
    }


def commit_board_mutation(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    board_path: str | Path = "",
    db_path: str = "",
    note_records: list[dict[str, Any]] | None = None,
    hypothesis_records: list[dict[str, Any]] | None = None,
    challenge_records: list[dict[str, Any]] | None = None,
    task_records: list[dict[str, Any]] | None = None,
    round_transition_records: list[dict[str, Any]] | None = None,
    event_type: str,
    event_payload: dict[str, Any],
    event_created_at_utc: str = "",
    event_discriminator: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    bootstrap_board_state(
        run_dir_path,
        expected_run_id=run_id,
        board_path=board_file,
        db_path=db_path,
    )
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            board_run = fetch_board_run(connection, run_id=run_id) or {
                "run_id": run_id,
                "board_revision": 0,
                "updated_at_utc": "",
                "board_path": str(board_file),
            }
            next_revision = coerce_int(board_run.get("board_revision")) + 1
            event_timestamp = maybe_text(event_created_at_utc) or utc_now_iso()
            event_index = next_event_index(connection, run_id=run_id)
            event_id = "boardevt-" + stable_hash(
                run_id,
                round_id,
                event_type,
                event_index,
                event_timestamp,
                event_discriminator,
            )[:12]
            event = {
                "event_id": event_id,
                "run_id": run_id,
                "round_id": round_id,
                "event_type": maybe_text(event_type),
                "created_at_utc": event_timestamp,
                "payload": event_payload,
            }
            for note in note_records or []:
                write_board_note_row(
                    connection,
                    note_row_from_payload(
                        note,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for hypothesis in hypothesis_records or []:
                write_hypothesis_row(
                    connection,
                    hypothesis_row_from_payload(
                        hypothesis,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for ticket in challenge_records or []:
                write_challenge_row(
                    connection,
                    challenge_row_from_payload(
                        ticket,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for task in task_records or []:
                write_board_task_row(
                    connection,
                    board_task_row_from_payload(
                        task,
                        board_revision=next_revision,
                        board_path=board_file,
                        record_locator="",
                    ),
                )
            for transition in round_transition_records or []:
                artifact_path = maybe_text(transition.get("artifact_path"))
                write_round_transition_row(
                    connection,
                    round_transition_row_from_payload(
                        transition,
                        board_revision=next_revision,
                        artifact_path=artifact_path,
                        record_locator=maybe_text(transition.get("record_locator")) or "$",
                    ),
                )
            write_board_event_row(
                connection,
                event_row_from_payload(
                    event,
                    event_index=event_index,
                    board_revision=next_revision,
                    board_path=board_file,
                    record_locator="",
                ),
            )
            upsert_board_run(
                connection,
                run_id=run_id,
                board_revision=next_revision,
                updated_at_utc=event_timestamp,
                board_path=str(board_file),
            )
            export_summary = export_board_from_connection(
                connection,
                run_id=run_id,
                board_path=board_file,
            )
    finally:
        connection.close()
    return {
        "status": "completed",
        "write_surface": "deliberation-plane",
        "run_id": run_id,
        "round_id": round_id,
        "board_path": str(board_file),
        "db_path": str(db_file),
        "board_revision": coerce_int(export_summary.get("board_revision")),
        "event_id": event_id,
        "event": event,
        "board_export": export_summary,
        "record_locators": export_summary.get("record_locators", {}),
    }


def store_round_transition_record(
    run_dir: str | Path,
    *,
    transition_record: dict[str, Any],
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_round_transition_row(
                connection,
                round_transition_row_from_payload(
                    transition_record,
                    board_revision=coerce_int(transition_record.get("board_revision")),
                    artifact_path=maybe_text(transition_record.get("artifact_path")),
                    record_locator=maybe_text(transition_record.get("record_locator"))
                    or "$",
                ),
            )
    finally:
        connection.close()
    return {
        "status": "completed",
        "run_id": maybe_text(transition_record.get("run_id")),
        "round_id": maybe_text(transition_record.get("round_id")),
        "transition_id": maybe_text(transition_record.get("transition_id")),
        "db_path": str(db_file),
        "board_revision": coerce_int(transition_record.get("board_revision")),
        "artifact_path": maybe_text(transition_record.get("artifact_path")),
        "record_locator": maybe_text(transition_record.get("record_locator")) or "$",
    }


def fetch_round_events(connection: sqlite3.Connection, *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT event_id, run_id, round_id, event_type, created_at_utc, payload_json, event_index
        FROM board_events
        WHERE run_id = ? AND round_id = ?
        ORDER BY event_index, event_id
        """,
        (run_id, round_id),
    ).fetchall()
    return [
        {
            "event_id": maybe_text(row["event_id"]),
            "run_id": maybe_text(row["run_id"]),
            "round_id": maybe_text(row["round_id"]),
            "event_type": maybe_text(row["event_type"]),
            "created_at_utc": maybe_text(row["created_at_utc"]),
            "payload": decode_json(maybe_text(row["payload_json"]), {}),
        }
        for row in rows
    ]


def fetch_round_state(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    round_id: str,
    include_closed: bool,
) -> dict[str, Any]:
    note_rows = connection.execute(
        """
        SELECT note_id, created_at_utc, author_role, category, note_text,
               tags_json, linked_artifact_refs_json, related_ids_json
        FROM board_notes
        WHERE run_id = ? AND round_id = ?
        ORDER BY created_at_utc, note_id
        """,
        (run_id, round_id),
    ).fetchall()
    hypothesis_sql = """
        SELECT hypothesis_id, title, statement, status, owner_role, linked_claim_ids_json,
               confidence, created_at_utc, updated_at_utc,
               carryover_from_round_id, carryover_from_hypothesis_id
        FROM hypothesis_cards
        WHERE run_id = ? AND round_id = ?
    """
    challenge_sql = """
        SELECT ticket_id, created_at_utc, status, priority, owner_role, title,
               challenge_statement, target_claim_id, target_hypothesis_id,
               linked_artifact_refs_json, related_task_ids_json,
               closed_at_utc, closed_by_role, resolution, resolution_note
        FROM challenge_tickets
        WHERE run_id = ? AND round_id = ?
    """
    task_sql = """
        SELECT task_id, title, task_text, task_type, status, owner_role, priority,
               source_round_id, source_ticket_id, source_hypothesis_id,
               carryover_from_round_id, carryover_from_task_id,
               linked_artifact_refs_json, related_ids_json,
               created_at_utc, updated_at_utc, claimed_at_utc
        FROM board_tasks
        WHERE run_id = ? AND round_id = ?
    """
    params: tuple[Any, ...] = (run_id, round_id)
    if not include_closed:
        hypothesis_sql += " AND status NOT IN ('closed', 'rejected')"
        challenge_sql += " AND status != 'closed'"
        task_sql += " AND status NOT IN ('completed', 'closed', 'cancelled')"
    hypothesis_sql += " ORDER BY updated_at_utc, hypothesis_id"
    challenge_sql += " ORDER BY created_at_utc, ticket_id"
    task_sql += " ORDER BY updated_at_utc, task_id"

    hypothesis_rows = connection.execute(hypothesis_sql, params).fetchall()
    challenge_rows = connection.execute(challenge_sql, params).fetchall()
    task_rows = connection.execute(task_sql, params).fetchall()

    return {
        "include_closed": include_closed,
        "note_count": len(note_rows),
        "hypothesis_count": len(hypothesis_rows),
        "challenge_ticket_count": len(challenge_rows),
        "task_count": len(task_rows),
        "notes": [
            {
                "note_id": maybe_text(row["note_id"]),
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "author_role": maybe_text(row["author_role"]),
                "category": maybe_text(row["category"]),
                "note_text": maybe_text(row["note_text"]),
                "tags": decode_json(maybe_text(row["tags_json"]), []),
                "linked_artifact_refs": decode_json(
                    maybe_text(row["linked_artifact_refs_json"]), []
                ),
                "related_ids": decode_json(maybe_text(row["related_ids_json"]), []),
            }
            for row in note_rows
        ],
        "hypotheses": [
            {
                "hypothesis_id": maybe_text(row["hypothesis_id"]),
                "title": maybe_text(row["title"]),
                "statement": maybe_text(row["statement"]),
                "status": maybe_text(row["status"]),
                "owner_role": maybe_text(row["owner_role"]),
                "linked_claim_ids": decode_json(
                    maybe_text(row["linked_claim_ids_json"]), []
                ),
                "confidence": row["confidence"],
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "updated_at_utc": maybe_text(row["updated_at_utc"]),
                "carryover_from_round_id": maybe_text(row["carryover_from_round_id"]),
                "carryover_from_hypothesis_id": maybe_text(
                    row["carryover_from_hypothesis_id"]
                ),
            }
            for row in hypothesis_rows
        ],
        "challenge_tickets": [
            {
                "ticket_id": maybe_text(row["ticket_id"]),
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "status": maybe_text(row["status"]),
                "priority": maybe_text(row["priority"]),
                "owner_role": maybe_text(row["owner_role"]),
                "title": maybe_text(row["title"]),
                "challenge_statement": maybe_text(row["challenge_statement"]),
                "target_claim_id": maybe_text(row["target_claim_id"]),
                "target_hypothesis_id": maybe_text(row["target_hypothesis_id"]),
                "linked_artifact_refs": decode_json(
                    maybe_text(row["linked_artifact_refs_json"]), []
                ),
                "related_task_ids": decode_json(
                    maybe_text(row["related_task_ids_json"]), []
                ),
                "closed_at_utc": maybe_text(row["closed_at_utc"]),
                "closed_by_role": maybe_text(row["closed_by_role"]),
                "resolution": maybe_text(row["resolution"]),
                "resolution_note": maybe_text(row["resolution_note"]),
            }
            for row in challenge_rows
        ],
        "tasks": [
            {
                "task_id": maybe_text(row["task_id"]),
                "title": maybe_text(row["title"]),
                "task_text": maybe_text(row["task_text"]),
                "task_type": maybe_text(row["task_type"]),
                "status": maybe_text(row["status"]),
                "owner_role": maybe_text(row["owner_role"]),
                "priority": maybe_text(row["priority"]),
                "source_round_id": maybe_text(row["source_round_id"]),
                "source_ticket_id": maybe_text(row["source_ticket_id"]),
                "source_hypothesis_id": maybe_text(row["source_hypothesis_id"]),
                "carryover_from_round_id": maybe_text(row["carryover_from_round_id"]),
                "carryover_from_task_id": maybe_text(row["carryover_from_task_id"]),
                "linked_artifact_refs": decode_json(
                    maybe_text(row["linked_artifact_refs_json"]), []
                ),
                "related_ids": decode_json(maybe_text(row["related_ids_json"]), []),
                "created_at_utc": maybe_text(row["created_at_utc"]),
                "updated_at_utc": maybe_text(row["updated_at_utc"]),
                "claimed_at_utc": maybe_text(row["claimed_at_utc"]),
            }
            for row in task_rows
        ],
    }


def load_round_snapshot(
    run_dir: str | Path,
    *,
    expected_run_id: str,
    round_id: str,
    board_path: str | Path = "",
    include_closed: bool = True,
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    sync_summary = bootstrap_board_state(
        run_dir_path,
        expected_run_id=expected_run_id,
        board_path=board_file,
        db_path=db_path,
    )
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        board_run = fetch_board_run(connection, run_id=expected_run_id)
        if maybe_text(sync_summary.get("status")) != "completed" and board_run is None:
            return {
                "status": "missing-board",
                "run_id": maybe_text(expected_run_id),
                "round_id": maybe_text(round_id),
                "board_path": str(board_file),
                "db_path": maybe_text(sync_summary.get("db_path")) or str(db_file),
                "state_source": "missing-board",
                "round_events": [],
                "round_state": {
                    "include_closed": bool(include_closed),
                    "note_count": 0,
                    "hypothesis_count": 0,
                    "challenge_ticket_count": 0,
                    "task_count": 0,
                    "notes": [],
                    "hypotheses": [],
                    "challenge_tickets": [],
                    "tasks": [],
                },
                "deliberation_sync": sync_summary,
            }
        round_events = fetch_round_events(
            connection,
            run_id=expected_run_id,
            round_id=round_id,
        )
        round_state = fetch_round_state(
            connection,
            run_id=expected_run_id,
            round_id=round_id,
            include_closed=include_closed,
        )
    finally:
        connection.close()
    return {
        "status": "completed",
        "run_id": maybe_text(expected_run_id),
        "round_id": maybe_text(round_id),
        "board_path": str(board_file),
        "db_path": str(db_file),
        "state_source": "deliberation-plane",
        "round_events": round_events,
        "round_state": round_state,
        "deliberation_sync": sync_summary,
    }


__all__ = [
    "bootstrap_board_state",
    "commit_board_mutation",
    "connect_db",
    "decode_json",
    "default_db_path",
    "fetch_round_events",
    "fetch_round_state",
    "load_raw_board_record",
    "load_round_snapshot",
    "maybe_text",
    "resolve_board_path",
    "resolve_db_path",
    "resolve_run_dir",
    "store_round_transition_record",
    "sync_board_to_deliberation_plane",
]
