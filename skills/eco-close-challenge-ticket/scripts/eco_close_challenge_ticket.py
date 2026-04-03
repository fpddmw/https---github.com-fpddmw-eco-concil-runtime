#!/usr/bin/env python3
"""Close challenge tickets on a local board artifact."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-close-challenge-ticket"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    bootstrap_board_state,
    commit_board_mutation,
    connect_db,
    load_raw_board_record,
)


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


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_board_path(run_dir: Path, board_path: str) -> Path:
    text = maybe_text(board_path)
    if not text:
        return (run_dir / "board" / "investigation_board.json").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


@contextmanager
def locked_board(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def missing_ticket_payload(run_id: str, round_id: str, board_file: Path, ticket_id: str) -> dict[str, Any]:
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "board_path": str(board_file), "ticket_id": ticket_id, "operation": "missing-ticket"},
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, ticket_id, "missing")[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, ticket_id, "missing")[:16],
        "artifact_refs": [],
        "canonical_ids": [],
        "warnings": [{"code": "missing-ticket", "message": f"Challenge ticket `{ticket_id}` was not found on the board."}],
        "board_handoff": {
            "candidate_ids": [],
            "evidence_refs": [],
            "gap_hints": ["The requested challenge ticket could not be closed because it does not exist on the board."],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-read-board-delta", "eco-summarize-board-state"],
        },
    }


def close_challenge_ticket_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    ticket_id: str,
    resolution: str,
    resolution_note: str,
    closing_role: str,
    related_task_ids: list[str],
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    with locked_board(board_file):
        bootstrap_board_state(
            run_dir_path,
            expected_run_id=run_id,
            board_path=board_file,
        )
        connection, _ = connect_db(run_dir_path)
        try:
            resolved_ticket_id = maybe_text(ticket_id)
            ticket = load_raw_board_record(
                connection,
                table_name="challenge_tickets",
                id_column="ticket_id",
                record_id=resolved_ticket_id,
            )
        finally:
            connection.close()
        resolved_ticket_id = maybe_text(ticket_id)
        if ticket is None:
            return missing_ticket_payload(run_id, round_id, board_file, resolved_ticket_id)

        timestamp = utc_now_iso()
        previous_status = maybe_text(ticket.get("status")) or "open"
        ticket["status"] = "closed"
        ticket["closed_at_utc"] = timestamp
        ticket["closed_by_role"] = maybe_text(closing_role) or "moderator"
        ticket["resolution"] = maybe_text(resolution) or "resolved"
        ticket["resolution_note"] = maybe_text(resolution_note)
        ticket["related_task_ids"] = unique_texts(related_task_ids + (ticket.get("related_task_ids") if isinstance(ticket.get("related_task_ids"), list) else []))
        history = ticket.get("history") if isinstance(ticket.get("history"), list) else []
        history.append({"status": "closed", "updated_at_utc": timestamp, "closing_role": ticket["closed_by_role"], "resolution": ticket["resolution"]})
        ticket["history"] = history

        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            challenge_records=[ticket],
            event_type="challenge-closed",
            event_payload={
                "ticket_id": resolved_ticket_id,
                "resolution": ticket["resolution"],
                "closed_by_role": ticket["closed_by_role"],
                "previous_status": previous_status,
            },
            event_created_at_utc=timestamp,
            event_discriminator=resolved_ticket_id,
        )
        event_id = maybe_text(write_summary.get("event_id"))
        record_locators = (
            write_summary.get("record_locators", {})
            if isinstance(write_summary.get("record_locators"), dict)
            else {}
        )
        ticket_locator = (
            record_locators.get("challenge_tickets", {})
            if isinstance(record_locators.get("challenge_tickets"), dict)
            else {}
        ).get(resolved_ticket_id, f"$.rounds.{round_id}.challenge_tickets[0]")
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": ticket_locator, "artifact_ref": f"{board_file}:{ticket_locator}"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "board_path": str(board_file),
            "board_revision": max(0, int(write_summary.get("board_revision") or 0)),
            "event_id": event_id,
            "ticket_id": resolved_ticket_id,
            "operation": "closed",
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, resolved_ticket_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [resolved_ticket_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": unique_texts([resolved_ticket_id] + ticket.get("related_task_ids", [])),
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-summarize-board-state", "eco-materialize-board-brief", "eco-post-board-note"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Close challenge tickets on a local board artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--ticket-id", required=True)
    parser.add_argument("--resolution", default="resolved")
    parser.add_argument("--resolution-note", default="")
    parser.add_argument("--closing-role", default="moderator")
    parser.add_argument("--related-task-id", action="append", default=[])
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = close_challenge_ticket_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        ticket_id=args.ticket_id,
        resolution=args.resolution,
        resolution_note=args.resolution_note,
        closing_role=args.closing_role,
        related_task_ids=args.related_task_id,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
