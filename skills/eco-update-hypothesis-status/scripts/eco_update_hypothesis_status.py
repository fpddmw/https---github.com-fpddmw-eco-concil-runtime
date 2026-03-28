#!/usr/bin/env python3
"""Create or update hypothesis cards on a local board artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-update-hypothesis-status"


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    event_id = "boardevt-" + stable_hash(run_id, round_id, event_type, len(events), timestamp, payload.get("hypothesis_id", ""))[:12]
    event = {"event_id": event_id, "run_id": run_id, "round_id": round_id, "event_type": event_type, "created_at_utc": timestamp, "payload": payload}
    events.append(event)
    board["updated_at_utc"] = timestamp
    return event


def write_board(path: Path, board: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(board, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def update_hypothesis_status_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    hypothesis_id: str,
    title: str,
    statement: str,
    status: str,
    owner_role: str,
    linked_claim_ids: list[str],
    confidence: float | None,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    board = load_or_init_board(board_file, run_id)
    round_state = ensure_round(board, round_id)
    hypotheses = round_state["hypotheses"] if isinstance(round_state.get("hypotheses"), list) else []
    round_state["hypotheses"] = hypotheses
    resolved_hypothesis_id = maybe_text(hypothesis_id) or ("hypothesis-" + stable_hash(run_id, round_id, title, statement)[:12])
    existing = next((item for item in hypotheses if isinstance(item, dict) and maybe_text(item.get("hypothesis_id")) == resolved_hypothesis_id), None)
    timestamp = utc_now_iso()
    payload = {
        "hypothesis_id": resolved_hypothesis_id,
        "run_id": run_id,
        "round_id": round_id,
        "title": maybe_text(title),
        "statement": maybe_text(statement),
        "status": maybe_text(status),
        "owner_role": maybe_text(owner_role),
        "linked_claim_ids": [maybe_text(item) for item in linked_claim_ids if maybe_text(item)],
        "confidence": maybe_number(confidence),
        "updated_at_utc": timestamp,
    }
    operation = "updated"
    if existing is None:
        payload["created_at_utc"] = timestamp
        payload["history"] = [{"status": payload["status"], "updated_at_utc": timestamp, "confidence": payload["confidence"]}]
        hypotheses.append(payload)
        existing = payload
        operation = "created"
    else:
        existing.update(payload)
        history = existing.get("history") if isinstance(existing.get("history"), list) else []
        history.append({"status": payload["status"], "updated_at_utc": timestamp, "confidence": payload["confidence"]})
        existing["history"] = history
    hypothesis_index = next(index for index, item in enumerate(hypotheses) if isinstance(item, dict) and maybe_text(item.get("hypothesis_id")) == resolved_hypothesis_id)
    event = append_event(board, run_id, round_id, "hypothesis-updated", {"hypothesis_id": resolved_hypothesis_id, "status": maybe_text(status), "operation": operation})
    write_board(board_file, board)
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": f"$.rounds.{round_id}.hypotheses[{hypothesis_index}]", "artifact_ref": f"{board_file}:$.rounds.{round_id}.hypotheses[{hypothesis_index}]"}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "board_path": str(board_file), "event_id": event["event_id"], "hypothesis_id": resolved_hypothesis_id, "operation": operation},
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, resolved_hypothesis_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event["event_id"])[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [resolved_hypothesis_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [resolved_hypothesis_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": ["Hypotheses with low confidence should usually be paired with a challenge ticket."] if maybe_number(confidence) is not None and float(maybe_number(confidence) or 0.0) < 0.6 else [],
            "suggested_next_skills": ["eco-read-board-delta", "eco-open-challenge-ticket", "eco-post-board-note"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update hypothesis cards on a local board artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--hypothesis-id", default="")
    parser.add_argument("--title", required=True)
    parser.add_argument("--statement", default="")
    parser.add_argument("--status", required=True)
    parser.add_argument("--owner-role", default="moderator")
    parser.add_argument("--linked-claim-id", action="append", default=[])
    parser.add_argument("--confidence", type=float)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = update_hypothesis_status_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        hypothesis_id=args.hypothesis_id,
        title=args.title,
        statement=args.statement,
        status=args.status,
        owner_role=args.owner_role,
        linked_claim_ids=args.linked_claim_id,
        confidence=args.confidence,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())