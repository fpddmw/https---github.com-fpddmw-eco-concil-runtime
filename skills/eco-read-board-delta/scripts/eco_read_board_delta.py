#!/usr/bin/env python3
"""Read compact investigation board deltas from a local board artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-read-board-delta"


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


def load_board_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def read_board_delta_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    after_event_id: str,
    event_limit: int,
    include_closed: bool,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    payload = load_board_if_exists(board_file)
    if not isinstance(payload, dict):
        return {
            "status": "completed",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "result_count": 0, "board_path": str(board_file), "event_cursor": ""},
            "result_count": 0,
            "results": [],
            "artifact_refs": [],
            "warnings": [{"code": "missing-board", "message": f"No board artifact was found at {board_file}."}],
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": ["Board state has not been initialized for this run yet."], "challenge_hints": [], "suggested_next_skills": ["eco-post-board-note", "eco-update-hypothesis-status"]},
        }
    events = payload.get("events", []) if isinstance(payload.get("events"), list) else []
    round_events = [event for event in events if isinstance(event, dict) and maybe_text(event.get("round_id")) == round_id]
    if maybe_text(after_event_id):
        index = next((position for position, event in enumerate(round_events) if maybe_text(event.get("event_id")) == maybe_text(after_event_id)), -1)
        round_events = round_events[index + 1 :] if index >= 0 else round_events
    limited_events = round_events[: max(1, event_limit)]
    rounds = payload.get("rounds", {}) if isinstance(payload.get("rounds"), dict) else {}
    round_state = rounds.get(round_id, {}) if isinstance(rounds.get(round_id), dict) else {}
    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    challenge_tickets = round_state.get("challenge_tickets", []) if isinstance(round_state.get("challenge_tickets"), list) else []
    hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
    if not include_closed:
        challenge_tickets = [ticket for ticket in challenge_tickets if isinstance(ticket, dict) and maybe_text(ticket.get("status")) != "closed"]
        hypotheses = [hypothesis for hypothesis in hypotheses if isinstance(hypothesis, dict) and maybe_text(hypothesis.get("status")) not in {"closed", "rejected"}]
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": "$.events", "artifact_ref": f"{board_file}:$.events"}]
    event_cursor = maybe_text(limited_events[-1].get("event_id")) if limited_events else ""
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "result_count": len(limited_events), "board_path": str(board_file), "event_cursor": event_cursor},
        "result_count": len(limited_events),
        "results": limited_events,
        "artifact_refs": artifact_refs,
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [maybe_text(event.get("event_id")) for event in limited_events if maybe_text(event.get("event_id"))],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if limited_events or notes or hypotheses or challenge_tickets else ["Board exists but no round activity has been recorded yet."],
            "challenge_hints": [f"{len(challenge_tickets)} open challenge tickets need review."] if challenge_tickets else [],
            "suggested_next_skills": ["eco-post-board-note", "eco-update-hypothesis-status", "eco-open-challenge-ticket"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read compact investigation board deltas from a local board artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--after-event-id", default="")
    parser.add_argument("--event-limit", type=int, default=50)
    parser.add_argument("--include-closed", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = read_board_delta_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        after_event_id=args.after_event_id,
        event_limit=args.event_limit,
        include_closed=args.include_closed,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())