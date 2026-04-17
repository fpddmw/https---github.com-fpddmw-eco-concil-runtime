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

from eco_council_runtime.board_proposal_support import (  # noqa: E402
    CLAIM_BOARD_TASK_PROPOSAL_KINDS,
    board_judgement_metadata,
    load_council_proposals,
    proposal_target,
    resolved_task_id_from_proposal,
    select_council_proposal,
)
from eco_council_runtime.canonical_contracts import (  # noqa: E402
    validate_canonical_payload,
)
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
        "receipt_id": "board-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, "blocked")[:20],
        "batch_id": "boardbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, "blocked")[:16],
        "artifact_refs": [],
        "canonical_ids": [],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [],
            "evidence_refs": [],
            "gap_hints": [item["message"] for item in warnings],
            "challenge_hints": [],
            "suggested_next_skills": [
                "eco-read-board-delta",
                "eco-post-board-note",
                "eco-claim-board-task",
            ],
        },
    }


def proposal_task_status(proposal: dict[str, Any]) -> str:
    for key in ("task_status", "proposed_status", "status"):
        text = maybe_text(proposal.get(key))
        if text:
            return text
    proposal_kind = maybe_text(proposal.get("proposal_kind"))
    action_kind = maybe_text(proposal.get("action_kind"))
    if proposal_kind in {"create-board-task", "open-board-task", "claim-board-task"}:
        return "claimed"
    if action_kind in {"create-board-task", "open-board-task", "claim-board-task"}:
        return "claimed"
    return ""


def claim_board_task_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    proposal_id: str,
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
    proposals = load_council_proposals(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    selected_proposal = select_council_proposal(
        proposals,
        proposal_id=proposal_id,
        accepted_kinds=CLAIM_BOARD_TASK_PROPOSAL_KINDS,
    )
    selected_proposal_id = (
        maybe_text(selected_proposal.get("proposal_id"))
        if isinstance(selected_proposal, dict)
        else ""
    )
    proposal_target_payload = (
        proposal_target(selected_proposal)
        if isinstance(selected_proposal, dict)
        else {}
    )
    proposal_target_kind = maybe_text(proposal_target_payload.get("object_kind"))
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
            resolved_task_id = (
                maybe_text(task_id)
                or (
                    resolved_task_id_from_proposal(selected_proposal)
                    if isinstance(selected_proposal, dict)
                    else ""
                )
                or (
                    "boardtask-"
                    + stable_hash(
                        run_id,
                        round_id,
                        title,
                        task_text,
                        maybe_text(selected_proposal.get("title"))
                        if isinstance(selected_proposal, dict)
                        else "",
                        len(tasks),
                        next_revision,
                    )[:12]
                )
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
        existing_evidence_refs = (
            existing.get("evidence_refs", [])
            if isinstance(existing, dict)
            and isinstance(existing.get("evidence_refs"), list)
            else []
        )
        existing_source_ids = (
            existing.get("source_ids", [])
            if isinstance(existing, dict)
            and isinstance(existing.get("source_ids"), list)
            else []
        )
        existing_lineage = (
            existing.get("lineage", [])
            if isinstance(existing, dict)
            and isinstance(existing.get("lineage"), list)
            else []
        )
        existing_linked_refs = (
            existing.get("linked_artifact_refs", [])
            if isinstance(existing, dict)
            and isinstance(existing.get("linked_artifact_refs"), list)
            else []
        )
        resolved_title = (
            maybe_text(title)
            or (
                maybe_text(selected_proposal.get("title"))
                or maybe_text(selected_proposal.get("summary"))
                or maybe_text(selected_proposal.get("objective"))
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (maybe_text(existing.get("title")) if isinstance(existing, dict) else "")
        resolved_task_text = (
            maybe_text(task_text)
            or (
                maybe_text(selected_proposal.get("task_text"))
                or maybe_text(selected_proposal.get("task_description"))
                or maybe_text(selected_proposal.get("summary"))
                or maybe_text(selected_proposal.get("rationale"))
                or maybe_text(selected_proposal.get("objective"))
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (maybe_text(existing.get("task_text")) if isinstance(existing, dict) else "")
        resolved_task_type = (
            maybe_text(task_type)
            or (
                maybe_text(selected_proposal.get("task_type"))
                or maybe_text(selected_proposal.get("proposed_task_type"))
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (
            maybe_text(existing.get("task_type")) if isinstance(existing, dict) else ""
        ) or "board-follow-up"
        resolved_status = (
            maybe_text(status)
            or (
                proposal_task_status(selected_proposal)
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (
            maybe_text(existing.get("status")) if isinstance(existing, dict) else ""
        ) or "claimed"
        resolved_owner_role = (
            maybe_text(owner_role)
            or (
                maybe_text(selected_proposal.get("assigned_role"))
                or maybe_text(selected_proposal.get("agent_role"))
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (
            maybe_text(existing.get("owner_role")) if isinstance(existing, dict) else ""
        ) or "moderator"
        resolved_priority = (
            maybe_text(priority)
            or (
                maybe_text(selected_proposal.get("priority"))
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (
            maybe_text(existing.get("priority")) if isinstance(existing, dict) else ""
        ) or "medium"
        resolved_source_ticket_id = (
            maybe_text(source_ticket_id)
            or (
                maybe_text(selected_proposal.get("source_ticket_id"))
                or maybe_text(selected_proposal.get("target_ticket_id"))
                or maybe_text(proposal_target_payload.get("ticket_id"))
                or (
                    maybe_text(proposal_target_payload.get("object_id"))
                    if proposal_target_kind in {"challenge-ticket", "ticket"}
                    else ""
                )
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (
            maybe_text(existing.get("source_ticket_id")) if isinstance(existing, dict) else ""
        )
        resolved_source_hypothesis_id = (
            maybe_text(source_hypothesis_id)
            or (
                maybe_text(selected_proposal.get("source_hypothesis_id"))
                or maybe_text(selected_proposal.get("target_hypothesis_id"))
                or maybe_text(proposal_target_payload.get("hypothesis_id"))
                or (
                    maybe_text(proposal_target_payload.get("object_id"))
                    if proposal_target_kind in {"hypothesis", "hypothesis-card"}
                    else ""
                )
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (
            maybe_text(existing.get("source_hypothesis_id"))
            if isinstance(existing, dict)
            else ""
        )
        proposal_related_ids = (
            (
                selected_proposal.get("related_ids", [])
                if isinstance(selected_proposal, dict)
                and isinstance(selected_proposal.get("related_ids"), list)
                else []
            )
            + (
                selected_proposal.get("linked_claim_ids", [])
                if isinstance(selected_proposal, dict)
                and isinstance(selected_proposal.get("linked_claim_ids"), list)
                else []
            )
            + [
                maybe_text(selected_proposal.get("target_claim_id"))
                if isinstance(selected_proposal, dict)
                else "",
                maybe_text(proposal_target_payload.get("claim_id")),
                (
                    maybe_text(proposal_target_payload.get("object_id"))
                    if proposal_target_kind
                    not in {
                        "",
                        "task",
                        "board-task",
                        "challenge-ticket",
                        "ticket",
                        "hypothesis",
                        "hypothesis-card",
                    }
                    else ""
                ),
            ]
        )
        resolved_related_ids = unique_texts(
            related_ids
            + proposal_related_ids
            + (
                existing.get("related_ids", [])
                if isinstance(existing, dict)
                and isinstance(existing.get("related_ids"), list)
                else []
            )
        )
        judgement = board_judgement_metadata(
            selected_proposal,
            source_skill=SKILL_NAME,
            default_decision_source="operator-command",
            base_evidence_refs=unique_texts(
                linked_artifact_refs + existing_linked_refs + existing_evidence_refs
            ),
            base_lineage=[
                resolved_task_id,
                resolved_source_ticket_id,
                resolved_source_hypothesis_id,
                *resolved_related_ids,
                *existing_lineage,
            ],
            base_source_ids=[
                resolved_task_id,
                resolved_source_ticket_id,
                resolved_source_hypothesis_id,
                *resolved_related_ids,
                *existing_source_ids,
            ],
        )
        resolved_linked_artifact_refs = unique_texts(
            linked_artifact_refs + existing_linked_refs + judgement["evidence_refs"]
        )
        if not resolved_title:
            return blocked_payload(
                run_id=run_id,
                round_id=round_id,
                board_file=board_file,
                warnings=[
                    {
                        "code": "missing-task-title",
                        "message": (
                            "Board task title is missing, and no matching council "
                            "proposal supplied enough data to create or update the task."
                        ),
                    }
                ],
            )
        if existing is None:
            payload = validate_canonical_payload(
                "board-task",
                {
                    "task_id": resolved_task_id,
                    "run_id": run_id,
                    "round_id": round_id,
                    "title": resolved_title,
                    "task_text": resolved_task_text,
                    "task_type": resolved_task_type,
                    "status": resolved_status,
                    "owner_role": resolved_owner_role,
                    "priority": resolved_priority,
                    "source_ticket_id": resolved_source_ticket_id,
                    "source_hypothesis_id": resolved_source_hypothesis_id,
                    "linked_artifact_refs": resolved_linked_artifact_refs,
                    "related_ids": resolved_related_ids,
                    "decision_source": judgement["decision_source"],
                    "evidence_refs": judgement["evidence_refs"],
                    "source_ids": judgement["source_ids"],
                    "response_to_ids": judgement["response_to_ids"],
                    "provenance": judgement["provenance"],
                    "lineage": judgement["lineage"],
                    "created_at_utc": timestamp,
                    "updated_at_utc": timestamp,
                },
            )
            payload["history"] = [
                {
                    "status": resolved_status,
                    "owner_role": resolved_owner_role,
                    "updated_at_utc": timestamp,
                    "operation": "created",
                    "decision_source": judgement["decision_source"],
                    "source_ids": judgement["source_ids"],
                }
            ]
            if resolved_status in {"claimed", "in_progress"}:
                payload["claimed_at_utc"] = timestamp
            existing = payload
            operation = "created"
        else:
            existing["title"] = resolved_title
            existing["task_text"] = resolved_task_text
            existing["task_type"] = resolved_task_type
            existing["status"] = resolved_status
            existing["owner_role"] = resolved_owner_role
            existing["priority"] = resolved_priority
            existing["source_ticket_id"] = resolved_source_ticket_id
            existing["source_hypothesis_id"] = resolved_source_hypothesis_id
            existing["linked_artifact_refs"] = resolved_linked_artifact_refs
            existing["related_ids"] = resolved_related_ids
            existing["decision_source"] = judgement["decision_source"]
            existing["evidence_refs"] = judgement["evidence_refs"]
            existing["source_ids"] = judgement["source_ids"]
            existing["response_to_ids"] = judgement["response_to_ids"]
            existing["provenance"] = judgement["provenance"]
            existing["lineage"] = judgement["lineage"]
            existing["updated_at_utc"] = timestamp
            if resolved_status in {"claimed", "in_progress"} and not maybe_text(
                existing.get("claimed_at_utc")
            ):
                existing["claimed_at_utc"] = timestamp
            existing = validate_canonical_payload("board-task", existing)
            history = existing.get("history") if isinstance(existing.get("history"), list) else []
            operation = "claimed" if resolved_status in {"claimed", "in_progress"} else "updated"
            history.append(
                {
                    "status": resolved_status,
                    "owner_role": resolved_owner_role,
                    "updated_at_utc": timestamp,
                    "operation": operation,
                    "decision_source": judgement["decision_source"],
                    "source_ids": judgement["source_ids"],
                }
            )
            existing["history"] = history

        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            task_records=[existing],
            event_type="task-claimed" if operation in {"created", "claimed"} else "task-updated",
            event_payload={
                "task_id": resolved_task_id,
                "status": maybe_text(existing.get("status")),
                "owner_role": maybe_text(existing.get("owner_role")),
                "source_ticket_id": maybe_text(existing.get("source_ticket_id")),
                "source_hypothesis_id": maybe_text(existing.get("source_hypothesis_id")),
                "operation": operation,
                "decision_source": maybe_text(existing.get("decision_source")),
                "proposal_id": selected_proposal_id,
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
            "decision_source": maybe_text(existing.get("decision_source")),
            "proposal_id": selected_proposal_id,
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
    parser.add_argument("--proposal-id", default="")
    parser.add_argument("--task-id", default="")
    parser.add_argument("--title", default="")
    parser.add_argument("--task-text", default="")
    parser.add_argument("--task-type", default="")
    parser.add_argument("--status", default="")
    parser.add_argument("--owner-role", default="")
    parser.add_argument("--priority", default="")
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
        proposal_id=args.proposal_id,
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
