#!/usr/bin/env python3
"""Append compact notes to a local board artifact."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-post-board-note"


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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
    return {"schema_version": "board-v1", "run_id": run_id, "board_revision": 0, "updated_at_utc": utc_now_iso(), "events": [], "rounds": {}}


def board_revision(board: dict[str, Any]) -> int:
    try:
        return max(0, int(board.get("board_revision") or 0))
    except (TypeError, ValueError):
        return 0


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
    event_id = "boardevt-" + stable_hash(run_id, round_id, event_type, len(events), timestamp, payload.get("note_id", ""))[:12]
    event = {"event_id": event_id, "run_id": run_id, "round_id": round_id, "event_type": event_type, "created_at_utc": timestamp, "payload": payload}
    events.append(event)
    board["updated_at_utc"] = timestamp
    return event


def write_board(path: Path, board: dict[str, Any], next_revision: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    board["board_revision"] = next_revision
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{next_revision}.tmp")
    temp_path.write_text(json.dumps(board, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp_path, path)


@contextmanager
def locked_board(path: Path, run_id: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield load_or_init_board(path, run_id)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def post_board_note_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    author_role: str,
    note_text: str,
    category: str,
    tags: list[str],
    linked_artifact_refs: list[str],
    related_ids: list[str],
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    with locked_board(board_file, run_id) as board:
        next_revision = board_revision(board) + 1
        round_state = ensure_round(board, round_id)
        notes = round_state["notes"] if isinstance(round_state.get("notes"), list) else []
        round_state["notes"] = notes
        created_at = utc_now_iso()
        note_id = "boardnote-" + stable_hash(run_id, round_id, author_role, note_text, len(notes), next_revision)[:12]
        note = {
            "note_id": note_id,
            "run_id": run_id,
            "round_id": round_id,
            "created_at_utc": created_at,
            "author_role": maybe_text(author_role),
            "category": maybe_text(category) or "analysis",
            "note_text": maybe_text(note_text),
            "tags": [maybe_text(tag) for tag in tags if maybe_text(tag)],
            "linked_artifact_refs": [maybe_text(ref) for ref in linked_artifact_refs if maybe_text(ref)],
            "related_ids": [maybe_text(item) for item in related_ids if maybe_text(item)],
        }
        notes.append(note)
        event = append_event(board, run_id, round_id, "note-posted", {"note_id": note_id, "category": note["category"], "author_role": note["author_role"]})
        write_board(board_file, board, next_revision)
        note_index = len(notes) - 1
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}.notes[{note_index}]", "artifact_ref": f"{board_file}:$.rounds.{round_id}.notes[{note_index}]"}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "board_path": str(board_file), "board_revision": next_revision, "event_id": event["event_id"], "note_id": note_id},
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, note_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event["event_id"])[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [note_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [note_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-update-hypothesis-status", "eco-open-challenge-ticket", "eco-claim-board-task"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append compact notes to a local board artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--author-role", required=True)
    parser.add_argument("--note-text", required=True)
    parser.add_argument("--category", default="analysis")
    parser.add_argument("--tag", action="append", default=[])
    parser.add_argument("--linked-artifact-ref", action="append", default=[])
    parser.add_argument("--related-id", action="append", default=[])
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = post_board_note_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        author_role=args.author_role,
        note_text=args.note_text,
        category=args.category,
        tags=args.tag,
        linked_artifact_refs=args.linked_artifact_ref,
        related_ids=args.related_id,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())