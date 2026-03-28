#!/usr/bin/env python3
"""Open challenge tickets on a local board artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-open-challenge-ticket"


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
    return {"schema_version": "board-v1", "run_id": run_id, "updated_at_utc": utc_now_iso(), "events": [], "rounds": {}}


def ensure_round(board: dict[str, Any], round_id: str) -> dict[str, Any]:
    rounds = board.setdefault("rounds", {})
    if not isinstance(rounds, dict):
        rounds = {}
        board["rounds"] = rounds
    round_state = rounds.setdefault(round_id, {"notes": [], "challenge_tickets": [], "hypotheses": []})
    if not isinstance(round_state, dict):
        round_state = {"notes": [], "challenge_tickets": [], "hypotheses": []}
        rounds[round_id] = round_state
    round_state.setdefault("notes", [])
    round_state.setdefault("challenge_tickets", [])
    round_state.setdefault("hypotheses", [])
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


def open_challenge_ticket_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    title: str,
    challenge_statement: str,
    target_claim_id: str,
    target_hypothesis_id: str,
    priority: str,
    owner_role: str,
    linked_artifact_refs: list[str],
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    board = load_or_init_board(board_file, run_id)
    round_state = ensure_round(board, round_id)
    tickets = round_state["challenge_tickets"] if isinstance(round_state.get("challenge_tickets"), list) else []
    round_state["challenge_tickets"] = tickets
    created_at = utc_now_iso()
    ticket_id = "challenge-" + stable_hash(run_id, round_id, title, challenge_statement, len(tickets))[:12]
    ticket = {
        "ticket_id": ticket_id,
        "run_id": run_id,
        "round_id": round_id,
        "created_at_utc": created_at,
        "status": "open",
        "priority": maybe_text(priority) or "medium",
        "owner_role": maybe_text(owner_role) or "challenger",
        "title": maybe_text(title),
        "challenge_statement": maybe_text(challenge_statement),
        "target_claim_id": maybe_text(target_claim_id),
        "target_hypothesis_id": maybe_text(target_hypothesis_id),
        "linked_artifact_refs": [maybe_text(ref) for ref in linked_artifact_refs if maybe_text(ref)],
    }
    tickets.append(ticket)
    event = append_event(board, run_id, round_id, "challenge-opened", {"ticket_id": ticket_id, "priority": ticket["priority"], "target_claim_id": ticket["target_claim_id"], "target_hypothesis_id": ticket["target_hypothesis_id"]})
    write_board(board_file, board)
    ticket_index = len(tickets) - 1
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}.challenge_tickets[{ticket_index}]", "artifact_ref": f"{board_file}:$.rounds.{round_id}.challenge_tickets[{ticket_index}]"}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "board_path": str(board_file), "event_id": event["event_id"], "ticket_id": ticket_id},
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, ticket_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event["event_id"])[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [ticket_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [ticket_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": ["This challenge ticket should be assigned a follow-up note or hypothesis update."],
            "suggested_next_skills": ["eco-read-board-delta", "eco-post-board-note", "eco-update-hypothesis-status"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open challenge tickets on a local board artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--title", required=True)
    parser.add_argument("--challenge-statement", required=True)
    parser.add_argument("--target-claim-id", default="")
    parser.add_argument("--target-hypothesis-id", default="")
    parser.add_argument("--priority", default="medium")
    parser.add_argument("--owner-role", default="challenger")
    parser.add_argument("--linked-artifact-ref", action="append", default=[])
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = open_challenge_ticket_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        title=args.title,
        challenge_statement=args.challenge_statement,
        target_claim_id=args.target_claim_id,
        target_hypothesis_id=args.target_hypothesis_id,
        priority=args.priority,
        owner_role=args.owner_role,
        linked_artifact_refs=args.linked_artifact_ref,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())