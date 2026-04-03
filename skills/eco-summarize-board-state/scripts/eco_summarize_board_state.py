#!/usr/bin/env python3
"""Summarize the current round's local board state into a compact artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-summarize-board-state"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import load_round_snapshot  # noqa: E402


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


def resolve_summary_path(run_dir: Path, summary_path: str, round_id: str) -> Path:
    text = maybe_text(summary_path)
    if not text:
        return (run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def active_hypotheses(hypotheses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in hypotheses if maybe_text(item.get("status")) not in {"closed", "rejected"}]


def open_challenges(challenges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in challenges if maybe_text(item.get("status")) != "closed"]


def open_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in tasks if maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}]


def low_confidence_count(hypotheses: list[dict[str, Any]]) -> int:
    return len([item for item in hypotheses if (maybe_number(item.get("confidence")) or 0.0) < 0.6])


def board_rollup(active_hypothesis_count: int, open_challenge_count: int, open_task_count: int, note_count: int) -> str:
    if active_hypothesis_count == 0 and open_challenge_count == 0 and open_task_count == 0 and note_count == 0:
        return "empty"
    if open_challenge_count > 0 and open_task_count == 0:
        return "needs-triage"
    if open_challenge_count > 0 or open_task_count > 0:
        return "in-flight"
    return "organized"


def next_skill_hints(active_hypothesis_count: int, open_challenge_count: int, open_task_count: int, note_count: int) -> list[str]:
    suggestions: list[str] = []
    if open_challenge_count > 0 and open_task_count == 0:
        suggestions.append("eco-claim-board-task")
    if open_challenge_count > 0:
        suggestions.append("eco-close-challenge-ticket")
    if active_hypothesis_count == 0:
        suggestions.append("eco-update-hypothesis-status")
    if note_count == 0:
        suggestions.append("eco-post-board-note")
    suggestions.append("eco-materialize-board-brief")
    if open_challenge_count == 0 and active_hypothesis_count > 0:
        suggestions.append("eco-propose-next-actions")
    return unique_texts(suggestions)


def summarize_board_state_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    summary_path: str,
    recent_event_limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    summary_file = resolve_summary_path(run_dir_path, summary_path, round_id)
    round_snapshot = load_round_snapshot(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        board_path=board_file,
        include_closed=True,
    )

    round_state = round_snapshot.get("round_state", {}) if isinstance(round_snapshot.get("round_state"), dict) else {}
    events = round_snapshot.get("round_events", []) if isinstance(round_snapshot.get("round_events"), list) else []
    state_source = maybe_text(round_snapshot.get("state_source")) or "missing-board"
    db_path = maybe_text(round_snapshot.get("db_path"))
    deliberation_sync = (
        round_snapshot.get("deliberation_sync")
        if isinstance(round_snapshot.get("deliberation_sync"), dict)
        else {}
    )
    warnings: list[dict[str, Any]] = []
    if maybe_text(round_snapshot.get("status")) != "completed":
        warnings.append({"code": "missing-board", "message": f"No board artifact was found at {board_file}."})

    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    challenges = round_state.get("challenge_tickets", []) if isinstance(round_state.get("challenge_tickets"), list) else []
    hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
    tasks = round_state.get("tasks", []) if isinstance(round_state.get("tasks"), list) else []

    active_hypothesis_items = active_hypotheses([item for item in hypotheses if isinstance(item, dict)])
    open_challenge_items = open_challenges([item for item in challenges if isinstance(item, dict)])
    open_task_items = open_tasks([item for item in tasks if isinstance(item, dict)])
    recent_events = events[-max(1, recent_event_limit) :]

    counts = {
        "notes_total": len(notes),
        "hypotheses_total": len(hypotheses),
        "hypotheses_active": len(active_hypothesis_items),
        "hypotheses_low_confidence": low_confidence_count(active_hypothesis_items),
        "challenge_total": len(challenges),
        "challenge_open": len(open_challenge_items),
        "challenge_closed": max(0, len(challenges) - len(open_challenge_items)),
        "tasks_total": len(tasks),
        "tasks_open": len(open_task_items),
        "tasks_claimed": len([item for item in open_task_items if maybe_text(item.get("status")) == "claimed"]),
        "tasks_in_progress": len([item for item in open_task_items if maybe_text(item.get("status")) == "in_progress"]),
        "tasks_completed": len([item for item in tasks if isinstance(item, dict) and maybe_text(item.get("status")) in {"completed", "closed"}]),
        "recent_event_count": len(recent_events),
    }
    status_rollup = board_rollup(counts["hypotheses_active"], counts["challenge_open"], counts["tasks_open"], counts["notes_total"])
    suggested_next_skills = next_skill_hints(counts["hypotheses_active"], counts["challenge_open"], counts["tasks_open"], counts["notes_total"])

    summary_payload = {
        "schema_version": "board-summary-v1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "board_path": str(board_file),
        "db_path": db_path,
        "state_source": state_source,
        "deliberation_sync": deliberation_sync,
        "status_rollup": status_rollup,
        "counts": counts,
        "active_hypotheses": [
            {
                "hypothesis_id": maybe_text(item.get("hypothesis_id")),
                "title": maybe_text(item.get("title")),
                "status": maybe_text(item.get("status")),
                "owner_role": maybe_text(item.get("owner_role")),
                "confidence": maybe_number(item.get("confidence")),
                "linked_claim_ids": unique_texts(item.get("linked_claim_ids", []) if isinstance(item.get("linked_claim_ids"), list) else []),
            }
            for item in active_hypothesis_items[:10]
        ],
        "open_challenges": [
            {
                "ticket_id": maybe_text(item.get("ticket_id")),
                "title": maybe_text(item.get("title")),
                "priority": maybe_text(item.get("priority")),
                "owner_role": maybe_text(item.get("owner_role")),
                "target_claim_id": maybe_text(item.get("target_claim_id")),
                "target_hypothesis_id": maybe_text(item.get("target_hypothesis_id")),
                "related_task_ids": unique_texts(item.get("related_task_ids", []) if isinstance(item.get("related_task_ids"), list) else []),
            }
            for item in open_challenge_items[:10]
        ],
        "open_tasks": [
            {
                "task_id": maybe_text(item.get("task_id")),
                "title": maybe_text(item.get("title")),
                "status": maybe_text(item.get("status")),
                "owner_role": maybe_text(item.get("owner_role")),
                "priority": maybe_text(item.get("priority")),
                "source_ticket_id": maybe_text(item.get("source_ticket_id")),
                "source_hypothesis_id": maybe_text(item.get("source_hypothesis_id")),
            }
            for item in open_task_items[:10]
        ],
        "recent_events": [
            {
                "event_id": maybe_text(item.get("event_id")),
                "event_type": maybe_text(item.get("event_type")),
                "created_at_utc": maybe_text(item.get("created_at_utc")),
            }
            for item in recent_events
        ],
        "recommended_next_skills": suggested_next_skills,
    }
    write_json_file(summary_file, summary_payload)

    summary_id = "board-summary-" + stable_hash(run_id, round_id, status_rollup)[:12]
    artifact_refs = [{"signal_id": "", "artifact_path": str(summary_file), "record_locator": "$", "artifact_ref": f"{summary_file}:$"}]
    gap_hints: list[str] = []
    if counts["hypotheses_active"] == 0:
        gap_hints.append("No active hypotheses are recorded on the board yet.")
    if counts["challenge_open"] > 0 and counts["tasks_open"] == 0:
        gap_hints.append("Open challenge tickets still have no claimed follow-up task.")
    if counts["notes_total"] == 0:
        gap_hints.append("No board notes capture moderator reasoning for this round yet.")
    challenge_hints: list[str] = []
    if counts["challenge_open"] > 0:
        challenge_hints.append(f"{counts['challenge_open']} challenge tickets remain open.")
    if counts["hypotheses_low_confidence"] > 0 and counts["challenge_open"] == 0:
        challenge_hints.append(f"{counts['hypotheses_low_confidence']} active hypotheses remain low confidence without an open challenge ticket.")
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "summary_path": str(summary_file),
            "status_rollup": status_rollup,
            "summary_id": summary_id,
            "state_source": state_source,
            "db_path": db_path,
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, summary_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, summary_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [summary_id],
        "warnings": warnings,
        "deliberation_sync": deliberation_sync,
        "board_handoff": {
            "candidate_ids": unique_texts(
                [item.get("hypothesis_id") for item in active_hypothesis_items[:8]]
                + [item.get("ticket_id") for item in open_challenge_items[:8]]
                + [item.get("task_id") for item in open_task_items[:8]]
            ),
            "evidence_refs": artifact_refs,
            "gap_hints": gap_hints,
            "challenge_hints": challenge_hints,
            "suggested_next_skills": suggested_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize the current round's local board state into a compact artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--summary-path", default="")
    parser.add_argument("--recent-event-limit", type=int, default=5)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = summarize_board_state_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        summary_path=args.summary_path,
        recent_event_limit=args.recent_event_limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
