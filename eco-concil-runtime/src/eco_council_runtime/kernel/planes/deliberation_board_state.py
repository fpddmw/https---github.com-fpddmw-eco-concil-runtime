from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane_rows import (
    board_task_row_from_payload,
    challenge_row_from_payload,
    coerce_int,
    decode_json,
    event_row_from_payload,
    hypothesis_row_from_payload,
    json_text,
    maybe_text,
    note_row_from_payload,
    payload_from_db_row,
    round_transition_row_from_payload,
    stable_hash,
    utc_now_iso,
    write_board_event_row,
    write_board_note_row,
    write_board_task_row,
    write_challenge_row,
    write_hypothesis_row,
    write_round_transition_row,
)
from eco_council_runtime.kernel.planes.deliberation_plane_schema import connect_db, resolve_db_path, resolve_run_dir


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
               decision_source, evidence_refs_json, source_ids_json, provenance_json, lineage_json,
               confidence, created_at_utc, updated_at_utc,
               carryover_from_round_id, carryover_from_hypothesis_id
        FROM hypothesis_cards
        WHERE run_id = ? AND round_id = ?
    """
    challenge_sql = """
        SELECT ticket_id, created_at_utc, status, priority, owner_role, title,
               challenge_statement, target_claim_id, target_hypothesis_id,
               decision_source, evidence_refs_json, source_ids_json, provenance_json, lineage_json,
               linked_artifact_refs_json, related_task_ids_json,
               closed_at_utc, closed_by_role, resolution, resolution_note
        FROM challenge_tickets
        WHERE run_id = ? AND round_id = ?
    """
    task_sql = """
        SELECT task_id, title, task_text, task_type, status, owner_role, priority,
               source_round_id, source_ticket_id, source_hypothesis_id,
               carryover_from_round_id, carryover_from_task_id,
               decision_source, evidence_refs_json, source_ids_json,
               provenance_json, lineage_json, linked_artifact_refs_json, related_ids_json,
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
                "decision_source": maybe_text(row["decision_source"]),
                "evidence_refs": decode_json(
                    maybe_text(row["evidence_refs_json"]), []
                ),
                "source_ids": decode_json(
                    maybe_text(row["source_ids_json"]), []
                ),
                "provenance": decode_json(
                    maybe_text(row["provenance_json"]), {}
                ),
                "lineage": decode_json(maybe_text(row["lineage_json"]), []),
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
                "decision_source": maybe_text(row["decision_source"]),
                "evidence_refs": decode_json(
                    maybe_text(row["evidence_refs_json"]), []
                ),
                "source_ids": decode_json(
                    maybe_text(row["source_ids_json"]), []
                ),
                "provenance": decode_json(
                    maybe_text(row["provenance_json"]), {}
                ),
                "lineage": decode_json(maybe_text(row["lineage_json"]), []),
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
                "decision_source": maybe_text(row["decision_source"]),
                "evidence_refs": decode_json(
                    maybe_text(row["evidence_refs_json"]), []
                ),
                "source_ids": decode_json(
                    maybe_text(row["source_ids_json"]), []
                ),
                "provenance": decode_json(
                    maybe_text(row["provenance_json"]), {}
                ),
                "lineage": decode_json(maybe_text(row["lineage_json"]), []),
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

def iter_round_transition_rows(run_dir: Path, *, run_id: str) -> list[dict[str, Any]]:
    runtime_dir = run_dir / "runtime"
    if not runtime_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for file_path in sorted(runtime_dir.glob("round_transition_*.json")):
        payload = json.loads(file_path.read_text(encoding="utf-8"))
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


def load_round_transition_record(
    run_dir: str | Path,
    *,
    transition_id: str = "",
    run_id: str = "",
    round_id: str = "",
    source_round_id: str = "",
    transition_request_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    requested_transition_id = maybe_text(transition_id)
    requested_run_id = maybe_text(run_id)
    requested_round_id = maybe_text(round_id)
    if not requested_transition_id and not (requested_run_id and requested_round_id):
        return None

    where_clauses: list[str] = []
    params: list[Any] = []
    if requested_transition_id:
        where_clauses.append("transition_id = ?")
        params.append(requested_transition_id)
    else:
        where_clauses.append("run_id = ?")
        params.append(requested_run_id)
        where_clauses.append("round_id = ?")
        params.append(requested_round_id)
        if maybe_text(source_round_id):
            where_clauses.append("source_round_id = ?")
            params.append(maybe_text(source_round_id))

    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        query = (
            "SELECT * FROM round_transitions WHERE "
            + " AND ".join(where_clauses)
            + " ORDER BY generated_at_utc DESC, transition_id DESC LIMIT ?"
        )
        rows = connection.execute(query, (*params, 200)).fetchall()
        requested_request_id = maybe_text(transition_request_id)
        for row in rows:
            payload = payload_from_db_row(row)
            if requested_request_id and maybe_text(
                payload.get("transition_request_id")
            ) != requested_request_id:
                continue
            return payload
    finally:
        connection.close()
    return None


__all__ = [
    "board_has_state",
    "bootstrap_board_state",
    "commit_board_mutation",
    "empty_round_state",
    "ensure_round_state",
    "export_board_from_connection",
    "fetch_board_run",
    "fetch_round_events",
    "fetch_round_state",
    "infer_board_path",
    "infer_board_revision",
    "load_json_if_exists",
    "load_raw_board_record",
    "load_round_snapshot",
    "next_event_index",
    "resolve_board_path",
    "sync_board_to_deliberation_plane",
    "upsert_board_run",
    "write_json_atomic",
    "iter_round_transition_rows",
    "store_round_transition_record",
    "load_round_transition_record",
]
