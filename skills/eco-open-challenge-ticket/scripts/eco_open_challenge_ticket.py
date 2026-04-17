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

from eco_council_runtime.board_proposal_support import (  # noqa: E402
    OPEN_CHALLENGE_PROPOSAL_KINDS,
    OPEN_CHALLENGE_TARGET_KINDS,
    board_judgement_metadata,
    load_council_proposals,
    proposal_target,
    select_council_proposal,
)
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
                "eco-open-challenge-ticket",
            ],
        },
    }


def open_challenge_ticket_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_path: str,
    proposal_id: str,
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
    proposals = load_council_proposals(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    selected_proposal = select_council_proposal(
        proposals,
        proposal_id=proposal_id,
        accepted_kinds=OPEN_CHALLENGE_PROPOSAL_KINDS,
        accepted_target_kinds=OPEN_CHALLENGE_TARGET_KINDS,
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
    resolved_title = (
        maybe_text(title)
        or maybe_text(selected_proposal.get("title"))
        or maybe_text(selected_proposal.get("summary"))
        or maybe_text(selected_proposal.get("objective"))
        if isinstance(selected_proposal, dict)
        else maybe_text(title)
    )
    resolved_statement = (
        maybe_text(challenge_statement)
        or maybe_text(selected_proposal.get("challenge_statement"))
        or maybe_text(selected_proposal.get("rationale"))
        or maybe_text(selected_proposal.get("summary"))
        if isinstance(selected_proposal, dict)
        else maybe_text(challenge_statement)
    )
    resolved_target_claim_id = (
        maybe_text(target_claim_id)
        or (
            maybe_text(selected_proposal.get("target_claim_id"))
            or maybe_text(proposal_target_payload.get("claim_id"))
            or (
                maybe_text(proposal_target_payload.get("object_id"))
                if proposal_target_kind in {"claim", "claim-candidate", "claim-cluster"}
                else ""
            )
            if isinstance(selected_proposal, dict)
            else ""
        )
    )
    resolved_target_hypothesis_id = (
        maybe_text(target_hypothesis_id)
        or (
            maybe_text(selected_proposal.get("target_hypothesis_id"))
            or maybe_text(proposal_target_payload.get("hypothesis_id"))
            or (
                maybe_text(proposal_target_payload.get("object_id"))
                if proposal_target_kind in {"hypothesis", "hypothesis-card"}
                else ""
            )
            if isinstance(selected_proposal, dict)
            else ""
        )
    )
    resolved_priority = (
        maybe_text(priority)
        or maybe_text(selected_proposal.get("priority"))
        if isinstance(selected_proposal, dict)
        else maybe_text(priority)
    ) or "medium"
    resolved_owner_role = (
        maybe_text(owner_role)
        or maybe_text(selected_proposal.get("assigned_role"))
        or maybe_text(selected_proposal.get("agent_role"))
        if isinstance(selected_proposal, dict)
        else maybe_text(owner_role)
    ) or "challenger"
    judgement = board_judgement_metadata(
        selected_proposal,
        source_skill=SKILL_NAME,
        default_decision_source="operator-command",
        base_evidence_refs=linked_artifact_refs,
        base_lineage=[resolved_target_claim_id, resolved_target_hypothesis_id],
        base_source_ids=[resolved_target_claim_id, resolved_target_hypothesis_id],
    )
    resolved_linked_refs = unique_texts(
        linked_artifact_refs + judgement["evidence_refs"]
    )
    warnings: list[dict[str, Any]] = []
    if not resolved_title or not resolved_statement:
        warnings.append(
            {
                "code": "missing-challenge-inputs",
                "message": (
                    "Challenge title or challenge statement is missing, and no "
                    "matching council proposal supplied enough data to open a "
                    "ticket."
                ),
            }
        )
        return blocked_payload(
            run_id=run_id,
            round_id=round_id,
            board_file=board_file,
            warnings=warnings,
        )
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
            maybe_text(selected_proposal.get("proposed_ticket_id"))
            if isinstance(selected_proposal, dict)
            else "",
            resolved_title,
            resolved_statement,
            len(tickets),
            next_revision,
        )[:12]
        ticket = {
            "ticket_id": (
                maybe_text(selected_proposal.get("proposed_ticket_id"))
                or maybe_text(selected_proposal.get("ticket_id"))
                if isinstance(selected_proposal, dict)
                else ""
            )
            or ticket_id,
            "run_id": run_id,
            "round_id": round_id,
            "created_at_utc": created_at,
            "status": "open",
            "priority": resolved_priority,
            "owner_role": resolved_owner_role,
            "title": resolved_title,
            "challenge_statement": resolved_statement,
            "target_claim_id": resolved_target_claim_id,
            "target_hypothesis_id": resolved_target_hypothesis_id,
            "linked_artifact_refs": resolved_linked_refs,
            "decision_source": judgement["decision_source"],
            "evidence_refs": resolved_linked_refs,
            "source_ids": judgement["source_ids"],
            "response_to_ids": judgement["response_to_ids"],
            "provenance": judgement["provenance"],
            "lineage": judgement["lineage"],
            "history": [
                {
                    "status": "open",
                    "updated_at_utc": created_at,
                    "owner_role": resolved_owner_role,
                    "decision_source": judgement["decision_source"],
                    "source_ids": judgement["source_ids"],
                }
            ],
        }
        write_summary = commit_board_mutation(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_path=board_file,
            challenge_records=[ticket],
            event_type="challenge-opened",
            event_payload={
                "ticket_id": ticket["ticket_id"],
                "priority": ticket["priority"],
                "target_claim_id": ticket["target_claim_id"],
                "target_hypothesis_id": ticket["target_hypothesis_id"],
                "decision_source": ticket["decision_source"],
                "proposal_id": selected_proposal_id,
            },
            event_created_at_utc=created_at,
            event_discriminator=ticket["ticket_id"],
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
        ).get(ticket["ticket_id"], f"$.rounds.{round_id}.challenge_tickets[0]")
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
            "ticket_id": ticket["ticket_id"],
            "decision_source": ticket["decision_source"],
            "proposal_id": selected_proposal_id,
            "db_path": maybe_text(write_summary.get("db_path")),
            "write_surface": maybe_text(write_summary.get("write_surface")) or "deliberation-plane",
        },
        "receipt_id": "board-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, ticket["ticket_id"])[:20],
        "batch_id": "boardbatch-" + stable_hash(SKILL_NAME, run_id, round_id, event_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [ticket["ticket_id"]],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [ticket["ticket_id"]],
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
    parser.add_argument("--proposal-id", default="")
    parser.add_argument("--title", default="")
    parser.add_argument("--challenge-statement", default="")
    parser.add_argument("--target-claim-id", default="")
    parser.add_argument("--target-hypothesis-id", default="")
    parser.add_argument("--priority", default="")
    parser.add_argument("--owner-role", default="")
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
        proposal_id=args.proposal_id,
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
