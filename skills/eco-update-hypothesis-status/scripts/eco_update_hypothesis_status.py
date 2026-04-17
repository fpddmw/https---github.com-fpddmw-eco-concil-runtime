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

from eco_council_runtime.board_proposal_support import (  # noqa: E402
    UPDATE_HYPOTHESIS_PROPOSAL_KINDS,
    UPDATE_HYPOTHESIS_TARGET_KINDS,
    board_judgement_metadata,
    load_council_proposals,
    proposal_target,
    resolved_hypothesis_id_from_proposal,
    select_council_proposal,
)
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
                "eco-update-hypothesis-status",
            ],
        },
    }


def proposal_hypothesis_status(proposal: dict[str, Any]) -> str:
    for key in (
        "hypothesis_status",
        "proposed_status",
        "desired_status",
        "target_status",
    ):
        text = maybe_text(proposal.get(key))
        if text:
            return text
    proposal_kind = maybe_text(proposal.get("proposal_kind"))
    action_kind = maybe_text(proposal.get("action_kind"))
    if proposal_kind in {"retire-hypothesis"} or action_kind in {"retire-hypothesis"}:
        return "retired"
    if proposal_kind in {
        "open-hypothesis",
        "create-hypothesis",
        "reopen-hypothesis",
        "stabilize-hypothesis",
    } or action_kind in {
        "open-hypothesis",
        "create-hypothesis",
        "reopen-hypothesis",
        "stabilize-hypothesis",
    }:
        return "active"
    return ""


def update_hypothesis_status_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    proposal_id: str,
    hypothesis_id: str,
    title: str,
    statement: str,
    status: str,
    owner_role: str,
    linked_claim_ids: list[str],
    linked_artifact_refs: list[str],
    confidence: float | None,
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
        accepted_kinds=UPDATE_HYPOTHESIS_PROPOSAL_KINDS,
        accepted_target_kinds=UPDATE_HYPOTHESIS_TARGET_KINDS,
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
        next_revision = max(0, int(bootstrap_summary.get("board_revision") or 0)) + 1
        resolved_hypothesis_id = maybe_text(hypothesis_id) or (
            resolved_hypothesis_id_from_proposal(selected_proposal)
            if isinstance(selected_proposal, dict)
            else ""
        ) or (
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
        resolved_title = (
            maybe_text(title)
            or maybe_text(selected_proposal.get("title"))
            or maybe_text(selected_proposal.get("summary"))
            or maybe_text(selected_proposal.get("objective"))
            if isinstance(selected_proposal, dict)
            else maybe_text(title)
        ) or (maybe_text(existing.get("title")) if isinstance(existing, dict) else "")
        resolved_statement = (
            maybe_text(statement)
            or maybe_text(selected_proposal.get("statement"))
            or maybe_text(selected_proposal.get("rationale"))
            or maybe_text(selected_proposal.get("summary"))
            if isinstance(selected_proposal, dict)
            else maybe_text(statement)
        ) or (
            maybe_text(existing.get("statement")) if isinstance(existing, dict) else ""
        )
        resolved_status = (
            maybe_text(status)
            or (
                proposal_hypothesis_status(selected_proposal)
                if isinstance(selected_proposal, dict)
                else ""
            )
        ) or (maybe_text(existing.get("status")) if isinstance(existing, dict) else "")
        resolved_owner_role = (
            maybe_text(owner_role)
            or maybe_text(selected_proposal.get("assigned_role"))
            or maybe_text(selected_proposal.get("agent_role"))
            if isinstance(selected_proposal, dict)
            else maybe_text(owner_role)
        ) or (
            maybe_text(existing.get("owner_role")) if isinstance(existing, dict) else "moderator"
        )
        resolved_linked_claim_ids = unique_texts(
            linked_claim_ids
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
                    if proposal_target_kind in {"claim", "claim-candidate", "claim-cluster"}
                    else ""
                ),
            ]
            + (
                existing.get("linked_claim_ids", [])
                if isinstance(existing, dict)
                and isinstance(existing.get("linked_claim_ids"), list)
                else []
            )
        )
        resolved_confidence = (
            confidence
            if confidence is not None
            else (
                maybe_number(selected_proposal.get("confidence"))
                if isinstance(selected_proposal, dict)
                else None
            )
        )
        if resolved_confidence is None and isinstance(existing, dict):
            resolved_confidence = maybe_number(existing.get("confidence"))
        judgement = board_judgement_metadata(
            selected_proposal,
            source_skill=SKILL_NAME,
            default_decision_source="operator-command",
            base_evidence_refs=linked_artifact_refs,
            base_lineage=[resolved_hypothesis_id, *resolved_linked_claim_ids],
            base_source_ids=[resolved_hypothesis_id, *resolved_linked_claim_ids],
        )
        if not resolved_title or not resolved_status:
            return blocked_payload(
                run_id=run_id,
                round_id=round_id,
                board_file=board_file,
                warnings=[
                    {
                        "code": "missing-hypothesis-inputs",
                        "message": (
                            "Hypothesis title or status is missing, and no matching "
                            "council proposal supplied enough data to create or update "
                            "the card."
                        ),
                    }
                ],
            )
        payload = {
            "hypothesis_id": resolved_hypothesis_id,
            "run_id": run_id,
            "round_id": round_id,
            "title": resolved_title,
            "statement": resolved_statement,
            "status": resolved_status,
            "owner_role": resolved_owner_role,
            "linked_claim_ids": resolved_linked_claim_ids,
            "confidence": resolved_confidence,
            "updated_at_utc": timestamp,
            "decision_source": judgement["decision_source"],
            "evidence_refs": judgement["evidence_refs"],
            "source_ids": judgement["source_ids"],
            "response_to_ids": judgement["response_to_ids"],
            "provenance": judgement["provenance"],
            "lineage": judgement["lineage"],
        }
        operation = "updated"
        if existing is None:
            payload["created_at_utc"] = timestamp
            payload["history"] = [
                {
                    "status": payload["status"],
                    "updated_at_utc": timestamp,
                    "confidence": payload["confidence"],
                    "decision_source": judgement["decision_source"],
                    "source_ids": judgement["source_ids"],
                }
            ]
            existing = payload
            operation = "created"
        else:
            existing.update(payload)
            history = existing.get("history") if isinstance(existing.get("history"), list) else []
            history.append(
                {
                    "status": payload["status"],
                    "updated_at_utc": timestamp,
                    "confidence": payload["confidence"],
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
            hypothesis_records=[existing],
            event_type="hypothesis-updated",
            event_payload={
                "hypothesis_id": resolved_hypothesis_id,
                "status": payload["status"],
                "operation": operation,
                "decision_source": judgement["decision_source"],
                "proposal_id": selected_proposal_id,
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
            "decision_source": judgement["decision_source"],
            "proposal_id": selected_proposal_id,
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
    parser.add_argument("--proposal-id", default="")
    parser.add_argument("--hypothesis-id", default="")
    parser.add_argument("--title", default="")
    parser.add_argument("--statement", default="")
    parser.add_argument("--status", default="")
    parser.add_argument("--owner-role", default="")
    parser.add_argument("--linked-claim-id", action="append", default=[])
    parser.add_argument("--linked-artifact-ref", action="append", default=[])
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
        proposal_id=args.proposal_id,
        hypothesis_id=args.hypothesis_id,
        title=args.title,
        statement=args.statement,
        status=args.status,
        owner_role=args.owner_role,
        linked_claim_ids=args.linked_claim_id,
        linked_artifact_refs=args.linked_artifact_ref,
        confidence=args.confidence,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
