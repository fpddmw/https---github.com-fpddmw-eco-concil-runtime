#!/usr/bin/env python3
"""Open explicit board follow-up from a report-risk review comment."""

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

SKILL_NAME = "open-followup-from-review-comment"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.contracts import (  # noqa: E402
    SPATIOTEMPORAL_OBJECTION_CODE_VALUES,
    validate_canonical_payload,
)
from eco_council_runtime.objects.council import query_council_objects  # noqa: E402
from eco_council_runtime.kernel.planes.deliberation_plane import (  # noqa: E402
    bootstrap_board_state,
    commit_board_mutation,
)


OPEN_REVIEW_COMMENT_STATUSES = {"", "open", "submitted"}
NON_BLOCKING_REPORT_RISKS = {"", "none", "no-risk", "no-report-risk", "informational"}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


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


def blocked_payload(
    *,
    run_id: str,
    round_id: str,
    board_file: Path,
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": "blocked",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "board_path": str(board_file),
            "operation": "blocked",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "blocked")[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "blocked")[:16],
        "artifact_refs": [],
        "canonical_ids": [],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [],
            "evidence_refs": [],
            "gap_hints": [item["message"] for item in warnings],
            "challenge_hints": [],
            "suggested_next_skills": [
                "query-council-objects",
                "post-review-comment",
                "summarize-round-readiness",
            ],
        },
    }


def load_review_comment(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    comment_id: str,
) -> dict[str, Any] | None:
    payload = query_council_objects(
        run_dir,
        object_kind="review-comment",
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    for comment in list_items(payload.get("objects")):
        if isinstance(comment, dict) and maybe_text(comment.get("comment_id")) == comment_id:
            return comment
    return None


def review_comment_requires_followup(comment: dict[str, Any]) -> bool:
    status = maybe_text(comment.get("status")).casefold()
    if status not in OPEN_REVIEW_COMMENT_STATUSES:
        return False
    report_risk = maybe_text(comment.get("report_risk")).casefold()
    if report_risk and report_risk not in NON_BLOCKING_REPORT_RISKS:
        return True
    return any(maybe_text(item) for item in list_items(comment.get("required_followup_evidence")))


def comment_target_label(comment: dict[str, Any]) -> str:
    target_kind = maybe_text(comment.get("target_kind")) or "round"
    target_id = maybe_text(comment.get("target_id")) or maybe_text(comment.get("round_id"))
    if target_id:
        return f"{target_kind}:{target_id}"
    return target_kind


def build_challenge_statement(comment: dict[str, Any]) -> str:
    parts = [
        maybe_text(comment.get("comment_text")),
        f"Report risk: {maybe_text(comment.get('report_risk'))}" if maybe_text(comment.get("report_risk")) else "",
    ]
    followup = unique_texts(list_items(comment.get("required_followup_evidence")))
    if followup:
        parts.append("Required follow-up evidence: " + "; ".join(followup))
    return " ".join(part for part in parts if part)


def open_followup_from_review_comment_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    review_comment_id: str,
    owner_role: str,
    priority: str,
    task_status: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_file = resolve_board_path(run_dir_path, board_path)
    comment_id = maybe_text(review_comment_id)
    warnings: list[dict[str, Any]] = []
    if not comment_id:
        return blocked_payload(
            run_id=run_id,
            round_id=round_id,
            board_file=board_file,
            warnings=[
                {
                    "code": "missing-review-comment-id",
                    "message": "A review_comment_id is required to open follow-up.",
                }
            ],
        )
    comment = load_review_comment(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        comment_id=comment_id,
    )
    if not isinstance(comment, dict):
        return blocked_payload(
            run_id=run_id,
            round_id=round_id,
            board_file=board_file,
            warnings=[
                {
                    "code": "missing-review-comment",
                    "message": f"No DB-backed review-comment `{comment_id}` was found for this round.",
                }
            ],
        )
    if not review_comment_requires_followup(comment):
        return blocked_payload(
            run_id=run_id,
            round_id=round_id,
            board_file=board_file,
            warnings=[
                {
                    "code": "review-comment-does-not-require-followup",
                    "message": (
                        f"Review comment `{comment_id}` is not an open report-risk "
                        "or required-follow-up comment."
                    ),
                }
            ],
        )
    objection_code = maybe_text(comment.get("objection_code"))
    if objection_code and objection_code not in SPATIOTEMPORAL_OBJECTION_CODE_VALUES:
        return blocked_payload(
            run_id=run_id,
            round_id=round_id,
            board_file=board_file,
            warnings=[
                {
                    "code": "invalid-objection-code",
                    "message": f"Unsupported relation objection_code: {objection_code}.",
                }
            ],
        )

    timestamp = utc_now_iso()
    resolved_owner_role = maybe_text(owner_role) or "challenger"
    resolved_priority = maybe_text(priority) or "high"
    resolved_task_status = maybe_text(task_status) or "claimed"
    target_label = comment_target_label(comment)
    evidence_refs = unique_texts(list_items(comment.get("evidence_refs")))
    response_to_ids = unique_texts(list_items(comment.get("response_to_ids")))
    relation_id = maybe_text(comment.get("relation_id"))
    report_risk = maybe_text(comment.get("report_risk"))
    challenge_id = "challenge-" + stable_hash(
        run_id,
        round_id,
        comment_id,
        target_label,
        report_risk,
    )[:12]
    task_id = "boardtask-" + stable_hash(
        run_id,
        round_id,
        comment_id,
        "review-comment-follow-up",
    )[:12]
    title = f"Follow up review risk on {target_label}"
    challenge_statement = build_challenge_statement(comment)
    if not challenge_statement:
        challenge_statement = f"Review comment `{comment_id}` requires follow-up before report-basis freeze."

    challenge = {
        "ticket_id": challenge_id,
        "run_id": run_id,
        "round_id": round_id,
        "created_at_utc": timestamp,
        "status": "open",
        "priority": resolved_priority,
        "owner_role": resolved_owner_role,
        "title": title,
        "challenge_statement": challenge_statement,
        "target_claim_id": "",
        "target_hypothesis_id": "",
        "target_kind": maybe_text(comment.get("target_kind")),
        "target_id": maybe_text(comment.get("target_id")),
        "relation_id": relation_id,
        "objection_code": objection_code,
        "challenged_rule": maybe_text(comment.get("challenged_rule")),
        "alternative_explanation": maybe_text(comment.get("alternative_explanation")),
        "required_followup_evidence": unique_texts(list_items(comment.get("required_followup_evidence"))),
        "report_risk": report_risk,
        "source_review_comment_id": comment_id,
        "linked_artifact_refs": evidence_refs,
        "decision_source": "review-comment-followup",
        "evidence_refs": evidence_refs,
        "source_ids": unique_texts([comment_id, relation_id, maybe_text(comment.get("target_id")), *response_to_ids]),
        "response_to_ids": unique_texts([comment_id, *response_to_ids]),
        "provenance": {
            "source_skill": SKILL_NAME,
            "source_review_comment_id": comment_id,
        },
        "lineage": unique_texts([comment_id, relation_id, maybe_text(comment.get("target_id")), *response_to_ids]),
        "history": [
            {
                "status": "open",
                "updated_at_utc": timestamp,
                "owner_role": resolved_owner_role,
                "decision_source": "review-comment-followup",
                "source_ids": unique_texts([comment_id]),
            }
        ],
    }
    task = validate_canonical_payload(
        "board-task",
        {
            "task_id": task_id,
            "run_id": run_id,
            "round_id": round_id,
            "title": f"Resolve follow-up for {comment_id}",
            "task_text": challenge_statement,
            "task_type": "review-comment-follow-up",
            "status": resolved_task_status,
            "owner_role": resolved_owner_role,
            "priority": resolved_priority,
            "source_ticket_id": challenge_id,
            "source_hypothesis_id": "",
            "linked_artifact_refs": evidence_refs,
            "related_ids": unique_texts([comment_id, maybe_text(comment.get("target_id")), relation_id]),
            "decision_source": "review-comment-followup",
            "evidence_refs": evidence_refs,
            "source_ids": unique_texts([comment_id, challenge_id, relation_id, maybe_text(comment.get("target_id"))]),
            "response_to_ids": unique_texts([comment_id, challenge_id, *response_to_ids]),
            "provenance": {
                "source_skill": SKILL_NAME,
                "source_review_comment_id": comment_id,
            },
            "lineage": unique_texts([comment_id, challenge_id, relation_id, maybe_text(comment.get("target_id")), *response_to_ids]),
            "created_at_utc": timestamp,
            "updated_at_utc": timestamp,
        },
    )
    task["history"] = [
        {
            "status": resolved_task_status,
            "owner_role": resolved_owner_role,
            "updated_at_utc": timestamp,
            "operation": "created",
            "decision_source": "review-comment-followup",
            "source_ids": unique_texts([comment_id, challenge_id]),
        }
    ]
    if resolved_task_status in {"claimed", "in_progress"}:
        task["claimed_at_utc"] = timestamp

    with locked_board(board_file):
        bootstrap_board_state(
            run_dir_path,
            expected_run_id=run_id,
            board_path=board_file,
        )
        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            challenge_records=[challenge],
            task_records=[task],
            event_type="review-comment-followup-opened",
            event_payload={
                "review_comment_id": comment_id,
                "ticket_id": challenge_id,
                "task_id": task_id,
                "target_kind": maybe_text(comment.get("target_kind")),
                "target_id": maybe_text(comment.get("target_id")),
                "report_risk": report_risk,
            },
            event_created_at_utc=timestamp,
            event_discriminator=comment_id,
        )
    record_locators = (
        write_summary.get("record_locators", {})
        if isinstance(write_summary.get("record_locators"), dict)
        else {}
    )
    challenge_locator = (
        record_locators.get("challenge_tickets", {})
        if isinstance(record_locators.get("challenge_tickets"), dict)
        else {}
    ).get(challenge_id, f"$.rounds.{round_id}.challenge_tickets[0]")
    task_locator = (
        record_locators.get("tasks", {})
        if isinstance(record_locators.get("tasks"), dict)
        else {}
    ).get(task_id, f"$.rounds.{round_id}.tasks[0]")
    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(board_file),
            "record_locator": challenge_locator,
            "artifact_ref": f"{board_file}:{challenge_locator}",
        },
        {
            "signal_id": "",
            "artifact_path": str(board_file),
            "record_locator": task_locator,
            "artifact_ref": f"{board_file}:{task_locator}",
        },
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "board_path": str(board_file),
            "board_revision": int(write_summary.get("board_revision") or 0),
            "event_id": maybe_text(write_summary.get("event_id")),
            "review_comment_id": comment_id,
            "ticket_id": challenge_id,
            "task_id": task_id,
            "report_risk": report_risk,
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, comment_id)[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, maybe_text(write_summary.get("event_id")))[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [challenge_id, task_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [comment_id, challenge_id, task_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": [
                "Review-comment follow-up is now board-visible; readiness should remain open until the task is completed or a challenger waiver is filed."
            ],
            "suggested_next_skills": [
                "query-board-delta",
                "post-board-note",
                "summarize-round-readiness",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open board follow-up from one review comment.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-path", default="")
    parser.add_argument("--review-comment-id", required=True)
    parser.add_argument("--owner-role", default="challenger")
    parser.add_argument("--priority", default="high")
    parser.add_argument("--task-status", default="claimed")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = open_followup_from_review_comment_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_path=args.board_path,
        review_comment_id=args.review_comment_id,
        owner_role=args.owner_role,
        priority=args.priority,
        task_status=args.task_status,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
