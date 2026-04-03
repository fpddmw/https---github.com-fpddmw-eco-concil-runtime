#!/usr/bin/env python3
"""Open challenge tickets on a local board artifact."""

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

SKILL_NAME = "eco-open-challenge-ticket"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    bootstrap_board_state,
    commit_board_mutation,
    connect_db,
    fetch_round_state,
)


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
            tickets = (
                round_state.get("challenge_tickets", [])
                if isinstance(round_state.get("challenge_tickets"), list)
                else []
            )
        finally:
            connection.close()
        created_at = utc_now_iso()
        next_revision = max(0, int(bootstrap_summary.get("board_revision") or 0)) + 1
        ticket_id = "challenge-" + stable_hash(
            run_id,
            round_id,
            title,
            challenge_statement,
            len(tickets),
            next_revision,
        )[:12]
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
        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            challenge_records=[ticket],
            event_type="challenge-opened",
            event_payload={
                "ticket_id": ticket_id,
                "priority": ticket["priority"],
                "target_claim_id": ticket["target_claim_id"],
                "target_hypothesis_id": ticket["target_hypothesis_id"],
            },
            event_created_at_utc=created_at,
            event_discriminator=ticket_id,
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
        ).get(ticket_id, f"$.rounds.{round_id}.challenge_tickets[0]")
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
            "ticket_id": ticket_id,
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, ticket_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [ticket_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [ticket_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": ["This challenge ticket should be assigned a follow-up note or hypothesis update."],
            "suggested_next_skills": ["eco-claim-board-task", "eco-post-board-note", "eco-summarize-board-state"],
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
