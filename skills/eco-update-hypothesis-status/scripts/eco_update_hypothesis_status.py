#!/usr/bin/env python3
"""Create or update hypothesis cards on a local board artifact."""

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

SKILL_NAME = "eco-update-hypothesis-status"
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
    with locked_board(board_file):
        bootstrap_summary = bootstrap_board_state(
            run_dir_path,
            expected_run_id=run_id,
            board_path=board_file,
        )
        next_revision = max(0, int(bootstrap_summary.get("board_revision") or 0)) + 1
        resolved_hypothesis_id = maybe_text(hypothesis_id) or (
            "hypothesis-" + stable_hash(run_id, round_id, title, statement, next_revision)[:12]
        )
        connection, _ = connect_db(run_dir_path)
        try:
            existing = load_raw_board_record(
                connection,
                table_name="hypothesis_cards",
                id_column="hypothesis_id",
                record_id=resolved_hypothesis_id,
            )
        finally:
            connection.close()
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
            existing = payload
            operation = "created"
        else:
            existing.update(payload)
            history = existing.get("history") if isinstance(existing.get("history"), list) else []
            history.append({"status": payload["status"], "updated_at_utc": timestamp, "confidence": payload["confidence"]})
            existing["history"] = history
        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            hypothesis_records=[existing],
            event_type="hypothesis-updated",
            event_payload={
                "hypothesis_id": resolved_hypothesis_id,
                "status": maybe_text(status),
                "operation": operation,
            },
            event_created_at_utc=timestamp,
            event_discriminator=resolved_hypothesis_id,
        )
        event_id = maybe_text(write_summary.get("event_id"))
        record_locators = (
            write_summary.get("record_locators", {})
            if isinstance(write_summary.get("record_locators"), dict)
            else {}
        )
        hypothesis_locator = (
            record_locators.get("hypotheses", {})
            if isinstance(record_locators.get("hypotheses"), dict)
            else {}
        ).get(resolved_hypothesis_id, f"$.rounds.{round_id}.hypotheses[0]")
    artifact_refs = [{"signal_id": "", "artifact_path": str(board_file), "record_locator": hypothesis_locator, "artifact_ref": f"{board_file}:{hypothesis_locator}"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "board_path": str(board_file),
            "board_revision": max(0, int(write_summary.get("board_revision") or 0)),
            "event_id": event_id,
            "hypothesis_id": resolved_hypothesis_id,
            "operation": operation,
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, resolved_hypothesis_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [resolved_hypothesis_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [resolved_hypothesis_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": ["Hypotheses with low confidence should usually be paired with a challenge ticket."] if maybe_number(confidence) is not None and float(maybe_number(confidence) or 0.0) < 0.6 else [],
            "suggested_next_skills": ["eco-open-challenge-ticket", "eco-claim-board-task", "eco-summarize-board-state"],
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
