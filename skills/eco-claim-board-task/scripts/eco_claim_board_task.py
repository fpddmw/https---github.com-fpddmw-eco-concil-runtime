#!/usr/bin/env python3
"""Claim or upsert follow-up tasks on a local board artifact."""

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

SKILL_NAME = "eco-claim-board-task"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    bootstrap_board_state,
    commit_board_mutation,
    connect_db,
    fetch_round_state,
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


def keep_existing_text(new_value: str, existing_value: Any) -> str:
    text = maybe_text(new_value)
    if text:
        return text
    return maybe_text(existing_value)


def keep_existing_list(new_values: list[str], existing_values: Any) -> list[str]:
    values = unique_texts(new_values)
    if values:
        return values
    if isinstance(existing_values, list):
        return unique_texts(existing_values)
    return []


def claim_board_task_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    task_id: str,
    title: str,
    task_text: str,
    task_type: str,
    status: str,
    owner_role: str,
    priority: str,
    source_ticket_id: str,
    source_hypothesis_id: str,
    linked_artifact_refs: list[str],
    related_ids: list[str],
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    with locked_board(board_file):
        bootstrap_summary = bootstrap_board_state(
            run_dir_path,
            expected_run_id=run_id,
            board_path=board_file,
        )
        connection, _ = connect_db(run_dir_path)
        try:
            round_state = fetch_round_state(
                connection,
                run_id=run_id,
                round_id=round_id,
                include_closed=True,
            )
            tasks = (
                round_state.get("tasks", [])
                if isinstance(round_state.get("tasks"), list)
                else []
            )
            next_revision = max(0, int(bootstrap_summary.get("board_revision") or 0)) + 1
            resolved_task_id = maybe_text(task_id) or (
                "boardtask-" + stable_hash(run_id, round_id, title, task_text, len(tasks), next_revision)[:12]
            )
            existing = load_raw_board_record(
                connection,
                table_name="board_tasks",
                id_column="task_id",
                record_id=resolved_task_id,
            )
        finally:
            connection.close()
        timestamp = utc_now_iso()
        resolved_status = maybe_text(status) or "claimed"
        operation = "claimed"
        if existing is None:
            payload = {
                "task_id": resolved_task_id,
                "run_id": run_id,
                "round_id": round_id,
                "title": maybe_text(title),
                "task_text": maybe_text(task_text),
                "task_type": maybe_text(task_type) or "board-follow-up",
                "status": resolved_status,
                "owner_role": maybe_text(owner_role) or "moderator",
                "priority": maybe_text(priority) or "medium",
                "source_ticket_id": maybe_text(source_ticket_id),
                "source_hypothesis_id": maybe_text(source_hypothesis_id),
                "linked_artifact_refs": unique_texts(linked_artifact_refs),
                "related_ids": unique_texts(related_ids),
                "created_at_utc": timestamp,
                "updated_at_utc": timestamp,
                "history": [{"status": resolved_status, "owner_role": maybe_text(owner_role) or "moderator", "updated_at_utc": timestamp, "operation": "created"}],
            }
            if resolved_status in {"claimed", "in_progress"}:
                payload["claimed_at_utc"] = timestamp
            existing = payload
            operation = "created"
        else:
            existing["title"] = keep_existing_text(title, existing.get("title"))
            existing["task_text"] = keep_existing_text(task_text, existing.get("task_text"))
            existing["task_type"] = keep_existing_text(task_type, existing.get("task_type")) or "board-follow-up"
            existing["status"] = resolved_status or maybe_text(existing.get("status")) or "claimed"
            existing["owner_role"] = keep_existing_text(owner_role, existing.get("owner_role")) or "moderator"
            existing["priority"] = keep_existing_text(priority, existing.get("priority")) or "medium"
            existing["source_ticket_id"] = keep_existing_text(source_ticket_id, existing.get("source_ticket_id"))
            existing["source_hypothesis_id"] = keep_existing_text(source_hypothesis_id, existing.get("source_hypothesis_id"))
            existing["linked_artifact_refs"] = keep_existing_list(linked_artifact_refs, existing.get("linked_artifact_refs"))
            existing["related_ids"] = keep_existing_list(related_ids, existing.get("related_ids"))
            existing["updated_at_utc"] = timestamp
            if maybe_text(existing.get("status")) in {"claimed", "in_progress"} and not maybe_text(existing.get("claimed_at_utc")):
                existing["claimed_at_utc"] = timestamp
            history = existing.get("history") if isinstance(existing.get("history"), list) else []
            history.append({"status": maybe_text(existing.get("status")), "owner_role": maybe_text(existing.get("owner_role")), "updated_at_utc": timestamp, "operation": "claimed"})
            existing["history"] = history
            operation = "claimed"

        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            task_records=[existing],
            event_type="task-claimed",
            event_payload={
                "task_id": resolved_task_id,
                "status": maybe_text(existing.get("status")),
                "owner_role": maybe_text(existing.get("owner_role")),
                "source_ticket_id": maybe_text(existing.get("source_ticket_id")),
                "source_hypothesis_id": maybe_text(existing.get("source_hypothesis_id")),
                "operation": operation,
            },
            event_created_at_utc=timestamp,
            event_discriminator=resolved_task_id,
        )
        event_id = maybe_text(write_summary.get("event_id"))
        record_locators = (
            write_summary.get("record_locators", {})
            if isinstance(write_summary.get("record_locators"), dict)
            else {}
        )
        task_locator = (
            record_locators.get("tasks", {})
            if isinstance(record_locators.get("tasks"), dict)
            else {}
        ).get(resolved_task_id, f"$.rounds.{round_id}.tasks[0]")
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": task_locator, "artifact_ref": f"{board_file}:{task_locator}"}]
    challenge_hints: list[str] = []
    if maybe_text(existing.get("source_ticket_id")) and maybe_text(existing.get("status")) not in {"completed", "closed"}:
        challenge_hints.append("Linked challenge tickets should be closed only after the claimed task produces a review outcome.")
    gap_hints: list[str] = []
    if not maybe_text(existing.get("task_text")):
        gap_hints.append("This claimed task still has no detail text describing the expected follow-up.")
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "board_path": str(board_file),
            "board_revision": max(0, int(write_summary.get("board_revision") or 0)),
            "event_id": event_id,
            "task_id": resolved_task_id,
            "operation": operation,
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, resolved_task_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [resolved_task_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": unique_texts([resolved_task_id, existing.get("source_ticket_id"), existing.get("source_hypothesis_id")]),
            "evidence_refs": artifact_refs,
            "gap_hints": gap_hints,
            "challenge_hints": challenge_hints,
            "suggested_next_skills": ["eco-post-board-note", "eco-summarize-board-state", "eco-close-challenge-ticket"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claim or upsert follow-up tasks on a local board artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--task-id", default="")
    parser.add_argument("--title", required=True)
    parser.add_argument("--task-text", default="")
    parser.add_argument("--task-type", default="board-follow-up")
    parser.add_argument("--status", default="claimed")
    parser.add_argument("--owner-role", default="moderator")
    parser.add_argument("--priority", default="medium")
    parser.add_argument("--source-ticket-id", default="")
    parser.add_argument("--source-hypothesis-id", default="")
    parser.add_argument("--linked-artifact-ref", action="append", default=[])
    parser.add_argument("--related-id", action="append", default=[])
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = claim_board_task_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        task_id=args.task_id,
        title=args.title,
        task_text=args.task_text,
        task_type=args.task_type,
        status=args.status,
        owner_role=args.owner_role,
        priority=args.priority,
        source_ticket_id=args.source_ticket_id,
        source_hypothesis_id=args.source_hypothesis_id,
        linked_artifact_refs=args.linked_artifact_ref,
        related_ids=args.related_id,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
