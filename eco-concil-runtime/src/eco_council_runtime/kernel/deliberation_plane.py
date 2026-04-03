from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_SQL = """
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
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
            {
                "transition_id": transition_id,
                "run_id": payload_run_id or run_id,
                "round_id": round_id,
                "source_round_id": maybe_text(payload.get("source_round_id")),
                "generated_at_utc": maybe_text(payload.get("generated_at_utc")),
                "operation": maybe_text(payload.get("operation")),
                "event_id": maybe_text(payload.get("event_id")),
                "board_revision": coerce_int(payload.get("board_revision")),
                "prior_round_ids_json": json_text(payload.get("prior_round_ids", [])),
                "cross_round_query_hints_json": json_text(payload.get("cross_round_query_hints", {})),
                "counts_json": json_text(payload.get("counts", {})),
                "warnings_json": json_text(payload.get("warnings", [])),
                "artifact_path": str(file_path.resolve()),
                "record_locator": "$",
                "raw_json": json_text(payload),
            }
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
        event_rows.append(
            {
                "event_id": event_id,
                "run_id": maybe_text(event.get("run_id")) or run_id,
                "round_id": round_id,
                "event_type": maybe_text(event.get("event_type")),
                "created_at_utc": maybe_text(event.get("created_at_utc")),
                "payload_json": json_text(event.get("payload", {})),
                "event_index": index,
                "board_revision": board_revision,
                "artifact_path": str(board_file),
                "record_locator": f"$.events[{index}]",
                "raw_json": json_text(event),
            }
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
            note_rows.append(
                {
                    "note_id": note_id,
                    "run_id": maybe_text(note.get("run_id")) or run_id,
                    "round_id": maybe_text(note.get("round_id")) or maybe_text(round_id),
                    "created_at_utc": maybe_text(note.get("created_at_utc")),
                    "author_role": maybe_text(note.get("author_role")),
                    "category": maybe_text(note.get("category")),
                    "note_text": maybe_text(note.get("note_text")),
                    "tags_json": json_text(note.get("tags", [])),
                    "linked_artifact_refs_json": json_text(note.get("linked_artifact_refs", [])),
                    "related_ids_json": json_text(note.get("related_ids", [])),
                    "board_revision": board_revision,
                    "artifact_path": str(board_file),
                    "record_locator": f"$.rounds.{round_id}.notes[{index}]",
                    "raw_json": json_text(note),
                }
            )

        for index, hypothesis in enumerate(hypotheses):
            if not isinstance(hypothesis, dict):
                continue
            hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
            if not hypothesis_id:
                continue
            hypothesis_rows.append(
                {
                    "hypothesis_id": hypothesis_id,
                    "run_id": maybe_text(hypothesis.get("run_id")) or run_id,
                    "round_id": maybe_text(hypothesis.get("round_id")) or maybe_text(round_id),
                    "title": maybe_text(hypothesis.get("title")),
                    "statement": maybe_text(hypothesis.get("statement")),
                    "status": maybe_text(hypothesis.get("status")),
                    "owner_role": maybe_text(hypothesis.get("owner_role")),
                    "linked_claim_ids_json": json_text(hypothesis.get("linked_claim_ids", [])),
                    "confidence": hypothesis.get("confidence"),
                    "created_at_utc": maybe_text(hypothesis.get("created_at_utc")),
                    "updated_at_utc": maybe_text(hypothesis.get("updated_at_utc")),
                    "carryover_from_round_id": maybe_text(hypothesis.get("carryover_from_round_id")),
                    "carryover_from_hypothesis_id": maybe_text(
                        hypothesis.get("carryover_from_hypothesis_id")
                    ),
                    "history_json": json_text(hypothesis.get("history", [])),
                    "board_revision": board_revision,
                    "artifact_path": str(board_file),
                    "record_locator": f"$.rounds.{round_id}.hypotheses[{index}]",
                    "raw_json": json_text(hypothesis),
                }
            )

        for index, ticket in enumerate(challenges):
            if not isinstance(ticket, dict):
                continue
            ticket_id = maybe_text(ticket.get("ticket_id"))
            if not ticket_id:
                continue
            challenge_rows.append(
                {
                    "ticket_id": ticket_id,
                    "run_id": maybe_text(ticket.get("run_id")) or run_id,
                    "round_id": maybe_text(ticket.get("round_id")) or maybe_text(round_id),
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
                    "board_revision": board_revision,
                    "artifact_path": str(board_file),
                    "record_locator": f"$.rounds.{round_id}.challenge_tickets[{index}]",
                    "raw_json": json_text(ticket),
                }
            )

        for index, task in enumerate(tasks):
            if not isinstance(task, dict):
                continue
            task_id = maybe_text(task.get("task_id"))
            if not task_id:
                continue
            task_rows.append(
                {
                    "task_id": task_id,
                    "run_id": maybe_text(task.get("run_id")) or run_id,
                    "round_id": maybe_text(task.get("round_id")) or maybe_text(round_id),
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
                    "board_revision": board_revision,
                    "artifact_path": str(board_file),
                    "record_locator": f"$.rounds.{round_id}.tasks[{index}]",
                    "raw_json": json_text(task),
                }
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
            for row in note_rows:
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
            for row in hypothesis_rows:
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
            for row in challenge_rows:
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
            for row in task_rows:
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
            for row in round_transition_rows:
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
    finally:
        connection.close()

    return {
        "status": "completed",
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
    sync_summary = sync_board_to_deliberation_plane(
        run_dir_path,
        expected_run_id=expected_run_id,
        board_path=board_file,
        db_path=db_path,
    )
    if maybe_text(sync_summary.get("status")) != "completed":
        return {
            "status": "missing-board",
            "run_id": maybe_text(expected_run_id),
            "round_id": maybe_text(round_id),
            "board_path": str(board_file),
            "db_path": maybe_text(sync_summary.get("db_path")),
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
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
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
    "connect_db",
    "decode_json",
    "default_db_path",
    "fetch_round_events",
    "fetch_round_state",
    "maybe_text",
    "resolve_board_path",
    "resolve_db_path",
    "resolve_run_dir",
    "sync_board_to_deliberation_plane",
]
