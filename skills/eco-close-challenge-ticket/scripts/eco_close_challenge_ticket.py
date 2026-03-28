#!/usr/bin/env python3
"""Close challenge tickets on a local board artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-close-challenge-ticket"


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


def load_or_init_board(path: Path, run_id: str) -> dict[str, Any]:
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    return {"schema_version": "board-v1", "run_id": run_id, "updated_at_utc": utc_now_iso(), "events": [], "rounds": {}}


def ensure_round(board: dict[str, Any], round_id: str) -> dict[str, Any]:
    rounds = board.setdefault("rounds", {})
    if not isinstance(rounds, dict):
        rounds = {}
        board["rounds"] = rounds
    round_state = rounds.setdefault(round_id, {"notes": [], "challenge_tickets": [], "hypotheses": [], "tasks": []})
    if not isinstance(round_state, dict):
        round_state = {"notes": [], "challenge_tickets": [], "hypotheses": [], "tasks": []}
        rounds[round_id] = round_state
    round_state.setdefault("notes", [])
    round_state.setdefault("challenge_tickets", [])
    round_state.setdefault("hypotheses", [])
    round_state.setdefault("tasks", [])
    return round_state


def append_event(board: dict[str, Any], run_id: str, round_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    events = board.setdefault("events", [])
    if not isinstance(events, list):
        events = []
        board["events"] = events
    timestamp = utc_now_iso()
    event_id = "boardevt-" + stable_hash(run_id, round_id, event_type, len(events), timestamp, payload.get("ticket_id", ""))[:12]
    event = {"event_id": event_id, "run_id": run_id, "round_id": round_id, "event_type": event_type, "created_at_utc": timestamp, "payload": payload}
    events.append(event)
    board["updated_at_utc"] = timestamp
    return event


def write_board(path: Path, board: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(board, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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
    board = load_or_init_board(board_file, run_id)
    round_state = ensure_round(board, round_id)
    tickets = round_state["challenge_tickets"] if isinstance(round_state.get("challenge_tickets"), list) else []
    round_state["challenge_tickets"] = tickets
    resolved_ticket_id = maybe_text(ticket_id)
    ticket = next((item for item in tickets if isinstance(item, dict) and maybe_text(item.get("ticket_id")) == resolved_ticket_id), None)
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

    ticket_index = next(index for index, item in enumerate(tickets) if isinstance(item, dict) and maybe_text(item.get("ticket_id")) == resolved_ticket_id)
    event = append_event(
        board,
        run_id,
        round_id,
        "challenge-closed",
        {"ticket_id": resolved_ticket_id, "resolution": ticket["resolution"], "closed_by_role": ticket["closed_by_role"], "previous_status": previous_status},
    )
    write_board(board_file, board)
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}.challenge_tickets[{ticket_index}]", "artifact_ref": f"{board_file}:$.rounds.{round_id}.challenge_tickets[{ticket_index}]"}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "board_path": str(board_file), "event_id": event["event_id"], "ticket_id": resolved_ticket_id, "operation": "closed"},
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, resolved_ticket_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event["event_id"])[:16],
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