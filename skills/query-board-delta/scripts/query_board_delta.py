#!/usr/bin/env python3
"""Read compact investigation board deltas from a local board artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "query-board-delta"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    load_round_snapshot,
    resolve_board_path as runtime_resolve_board_path,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def unique_texts(values: list[Any]) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        items.append(text)
    return items


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_board_path(run_dir: Path, board_path: str) -> Path:
    return runtime_resolve_board_path(run_dir, board_path)


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
    round_snapshot = load_round_snapshot(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        board_path=board_file,
        include_closed=include_closed,
    )
    sync_summary = (
        round_snapshot.get("deliberation_sync")
        if isinstance(round_snapshot.get("deliberation_sync"), dict)
        else {}
    )
    if maybe_text(round_snapshot.get("status")) != "completed":
        return {
            "status": "completed",
            "summary": {
                "skill": SKILL_NAME,
                "run_id": run_id,
                "round_id": round_id,
                "result_count": 0,
                "board_path": str(board_file),
                "db_path": maybe_text(sync_summary.get("db_path")),
                "event_cursor": "",
                "note_count": 0,
                "hypothesis_count": 0,
                "challenge_ticket_count": 0,
                "task_count": 0,
            },
            "result_count": 0,
            "results": [],
            "artifact_refs": [],
            "warnings": [{"code": "missing-board", "message": f"No board artifact was found at {board_file}."}],
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
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": ["Board state has not been initialized for this run yet."], "challenge_hints": [], "suggested_next_skills": ["post-board-note", "update-hypothesis-status"]},
        }
    db_file = Path(maybe_text(round_snapshot.get("db_path")))
    round_events = (
        round_snapshot.get("round_events", [])
        if isinstance(round_snapshot.get("round_events"), list)
        else []
    )
    round_state = (
        round_snapshot.get("round_state", {})
        if isinstance(round_snapshot.get("round_state"), dict)
        else {}
    )
    if maybe_text(after_event_id):
        index = next((position for position, event in enumerate(round_events) if maybe_text(event.get("event_id")) == maybe_text(after_event_id)), -1)
        round_events = round_events[index + 1 :] if index >= 0 else round_events
    limited_events = round_events[: max(1, event_limit)]
    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    challenge_tickets = round_state.get("challenge_tickets", []) if isinstance(round_state.get("challenge_tickets"), list) else []
    hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
    tasks = round_state.get("tasks", []) if isinstance(round_state.get("tasks"), list) else []
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": "$.events", "artifact_ref": f"{board_file}:$.events"}]
    event_cursor = maybe_text(limited_events[-1].get("event_id")) if limited_events else ""
    candidate_ids = unique_texts(
        [
            *[
                maybe_text(event.get("event_id"))
                for event in limited_events
                if isinstance(event, dict)
            ],
            *[
                maybe_text(item.get("hypothesis_id"))
                for item in hypotheses
                if isinstance(item, dict)
            ],
            *[
                maybe_text(item.get("ticket_id"))
                for item in challenge_tickets
                if isinstance(item, dict)
            ],
            *[
                maybe_text(item.get("task_id"))
                for item in tasks
                if isinstance(item, dict)
            ],
        ]
    )
    empty_round = not limited_events and not notes and not hypotheses and not challenge_tickets and not tasks
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "result_count": len(limited_events),
            "board_path": str(board_file),
            "db_path": str(db_file),
            "event_cursor": event_cursor,
            "note_count": len(notes),
            "hypothesis_count": len(hypotheses),
            "challenge_ticket_count": len(challenge_tickets),
            "task_count": len(tasks),
        },
        "result_count": len(limited_events),
        "results": limited_events,
        "artifact_refs": artifact_refs,
        "round_state": round_state,
        "deliberation_sync": sync_summary,
        "warnings": [],
        "board_handoff": {
            "candidate_ids": candidate_ids,
            "evidence_refs": artifact_refs,
            "gap_hints": [] if not empty_round else ["Board exists but no round activity has been recorded yet."],
            "challenge_hints": [f"{len(challenge_tickets)} open challenge tickets need review."] if challenge_tickets else ([f"{len(tasks)} claimed or open tasks are still in flight."] if tasks else []),
            "suggested_next_skills": ["claim-board-task", "close-challenge-ticket", "summarize-board-state"],
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
