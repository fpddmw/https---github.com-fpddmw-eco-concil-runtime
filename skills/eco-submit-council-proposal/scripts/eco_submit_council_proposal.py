#!/usr/bin/env python3
"""Submit one structured council proposal directly into the deliberation DB."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-submit-council-proposal"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import append_council_proposal_record  # noqa: E402
from eco_council_runtime.council_submission_support import (  # noqa: E402
    council_proposal_payload,
    maybe_text,
    resolve_path,
    resolve_run_dir,
    unique_texts,
    write_json_file,
)
from eco_council_runtime.kernel.deliberation_plane import stable_hash  # noqa: E402
from eco_council_runtime.phase2_promotion_resolution import proposal_explicit_signals  # noqa: E402
from eco_council_runtime.phase2_proposal_actions import proposal_drives_phase2_action_queue  # noqa: E402

OPEN_CHALLENGE_KINDS = {
    "challenge-claim",
    "challenge-hypothesis",
    "open-challenge",
    "open-challenge-ticket",
}
CLOSE_CHALLENGE_KINDS = {
    "close-challenge",
    "close-challenge-ticket",
    "dismiss-challenge",
    "resolve-challenge",
}
HYPOTHESIS_KINDS = {
    "create-hypothesis",
    "open-hypothesis",
    "reopen-hypothesis",
    "retire-hypothesis",
    "stabilize-hypothesis",
    "update-hypothesis-status",
}
BOARD_TASK_KINDS = {
    "assign-board-task",
    "board-follow-up-task",
    "claim-board-task",
    "create-board-task",
    "follow-up-task",
    "open-board-task",
    "update-board-task",
}


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def proposal_follow_up_skills(proposal: dict[str, Any]) -> list[str]:
    proposal_kind = maybe_text(proposal.get("proposal_kind"))
    action_kind = maybe_text(proposal.get("action_kind"))
    target_kind = maybe_text(proposal.get("target_kind"))
    operation_kinds = {proposal_kind, action_kind}
    suggestions = ["eco-read-board-delta"]
    if proposal_drives_phase2_action_queue(proposal):
        suggestions.append("eco-propose-next-actions")
    if bool(proposal.get("probe_candidate")):
        suggestions.append("eco-open-falsification-probe")
    if proposal_explicit_signals(proposal):
        suggestions.extend(
            [
                "eco-submit-readiness-opinion",
                "eco-promote-evidence-basis",
            ]
        )
    if target_kind in {"hypothesis", "hypothesis-card"} or operation_kinds.intersection(HYPOTHESIS_KINDS):
        suggestions.append("eco-update-hypothesis-status")
    if target_kind in {"challenge-ticket", "ticket"} or operation_kinds.intersection(OPEN_CHALLENGE_KINDS):
        suggestions.append("eco-open-challenge-ticket")
    if target_kind in {"challenge-ticket", "ticket"} or operation_kinds.intersection(CLOSE_CHALLENGE_KINDS):
        suggestions.append("eco-close-challenge-ticket")
    if target_kind in {"board-task", "task"} or operation_kinds.intersection(BOARD_TASK_KINDS):
        suggestions.append("eco-claim-board-task")
    return unique_texts(suggestions)


def proposal_gap_hints(proposal: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    if not maybe_text(proposal.get("target_kind")) or not maybe_text(proposal.get("target_id")):
        hints.append("Proposal still lacks an explicit target anchor; downstream execution may need human disambiguation.")
    if not isinstance(proposal.get("evidence_refs"), list) or not proposal.get("evidence_refs"):
        hints.append("Proposal does not cite any evidence refs yet.")
    if proposal_explicit_signals(proposal) and not maybe_text(proposal.get("publication_readiness")):
        hints.append("Promotion-oriented proposal should normally state publication_readiness explicitly for downstream promotion review.")
    return hints[:3]


def proposal_challenge_hints(proposal: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    explicit_signals = proposal_explicit_signals(proposal)
    reject_signals = [
        signal
        for signal in explicit_signals
        if maybe_text(signal.get("disposition")) == "reject"
    ]
    if reject_signals:
        hints.append(
            "This proposal carries an explicit withholding signal and can veto promotion until the council resolves it."
        )
    if bool(proposal.get("probe_candidate")):
        hints.append(
            "Proposal marks the target as probe-worthy, so contradiction review should stay visible in the round queue."
        )
    return hints[:2]


def submit_council_proposal_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    proposal_id: str,
    proposal_kind: str,
    agent_role: str,
    rationale: str,
    decision_source: str,
    status: str,
    confidence: str,
    target_kind: str,
    target_id: str,
    target_claim_id: str,
    target_hypothesis_id: str,
    target_ticket_id: str,
    target_task_id: str,
    target_json: str,
    action_kind: str,
    assigned_role: str,
    objective: str,
    summary: str,
    evidence_refs: list[str],
    evidence_refs_json: str,
    response_to_ids: list[str],
    response_to_ids_json: str,
    lineage: list[str],
    lineage_json: str,
    provenance_json: str,
    extra_json: str,
    promotion_disposition: str,
    promote_allowed: str,
    publication_readiness: str,
    handoff_status: str,
    moderator_status: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    proposal_payload = council_proposal_payload(
        run_id=run_id,
        round_id=round_id,
        proposal_id=proposal_id,
        proposal_kind=proposal_kind,
        agent_role=agent_role,
        rationale=rationale,
        decision_source=decision_source,
        status=status,
        confidence=confidence,
        target_kind=target_kind,
        target_id=target_id,
        target_claim_id=target_claim_id,
        target_hypothesis_id=target_hypothesis_id,
        target_ticket_id=target_ticket_id,
        target_task_id=target_task_id,
        target_json=target_json,
        action_kind=action_kind,
        assigned_role=assigned_role,
        objective=objective,
        summary=summary,
        evidence_refs=evidence_refs,
        evidence_refs_json=evidence_refs_json,
        response_to_ids=response_to_ids,
        response_to_ids_json=response_to_ids_json,
        lineage=lineage,
        lineage_json=lineage_json,
        provenance_json=provenance_json,
        extra_json=extra_json,
        source_skill=SKILL_NAME,
        promotion_disposition=promotion_disposition,
        promote_allowed=promote_allowed,
        publication_readiness=publication_readiness,
        handoff_status=handoff_status,
        moderator_status=moderator_status,
    )
    append_result = append_council_proposal_record(
        run_dir_path,
        proposal_payload=proposal_payload,
    )
    proposal = (
        append_result.get("proposal", {})
        if isinstance(append_result.get("proposal"), dict)
        else {}
    )
    proposal_identifier = maybe_text(proposal.get("proposal_id"))
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"runtime/council_proposal_{proposal_identifier}.json",
    )
    artifact = {
        "schema_version": "council-proposal-submission-v1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "db_path": maybe_text(append_result.get("db_path")),
        "proposal": proposal,
    }
    write_json_file(output_file, artifact)
    artifact_ref = {
        "signal_id": "",
        "artifact_path": str(output_file),
        "record_locator": "$.proposal",
        "artifact_ref": f"{output_file}:$.proposal",
    }
    gap_hints = proposal_gap_hints(proposal)
    warnings = [
        {
            "code": "proposal-target-unspecified",
            "message": gap_hints[0],
        }
        for _hint in gap_hints[:1]
        if "target anchor" in gap_hints[0]
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "proposal_id": proposal_identifier,
            "proposal_kind": maybe_text(proposal.get("proposal_kind")),
            "decision_source": maybe_text(proposal.get("decision_source")),
            "target_kind": maybe_text(proposal.get("target_kind")),
            "target_id": maybe_text(proposal.get("target_id")),
            "output_path": str(output_file),
            "db_path": maybe_text(append_result.get("db_path")),
        },
        "receipt_id": "council-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, proposal_identifier)[:20],
        "batch_id": "councilbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": [artifact_ref],
        "canonical_ids": [proposal_identifier],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": unique_texts(
                [proposal_identifier, proposal.get("target_id")]
            ),
            "evidence_refs": [artifact_ref],
            "gap_hints": gap_hints,
            "challenge_hints": proposal_challenge_hints(proposal),
            "suggested_next_skills": proposal_follow_up_skills(proposal),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit one structured council proposal into the deliberation DB."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--proposal-id", default="")
    parser.add_argument("--proposal-kind", required=True)
    parser.add_argument("--agent-role", required=True)
    parser.add_argument("--rationale", required=True)
    parser.add_argument("--decision-source", default="agent-council")
    parser.add_argument("--status", default="open")
    parser.add_argument("--confidence", default="")
    parser.add_argument("--target-kind", default="")
    parser.add_argument("--target-id", default="")
    parser.add_argument("--target-claim-id", default="")
    parser.add_argument("--target-hypothesis-id", default="")
    parser.add_argument("--target-ticket-id", default="")
    parser.add_argument("--target-task-id", default="")
    parser.add_argument("--target-json", default="")
    parser.add_argument("--action-kind", default="")
    parser.add_argument("--assigned-role", default="")
    parser.add_argument("--objective", default="")
    parser.add_argument("--summary", default="")
    parser.add_argument("--evidence-ref", action="append", default=[])
    parser.add_argument("--evidence-refs-json", default="")
    parser.add_argument("--response-to-id", action="append", default=[])
    parser.add_argument("--response-to-ids-json", default="")
    parser.add_argument("--lineage-id", action="append", default=[])
    parser.add_argument("--lineage-json", default="")
    parser.add_argument("--provenance-json", default="")
    parser.add_argument("--extra-json", default="")
    parser.add_argument("--promotion-disposition", default="")
    parser.add_argument("--promote-allowed", default="")
    parser.add_argument("--publication-readiness", default="")
    parser.add_argument("--handoff-status", default="")
    parser.add_argument("--moderator-status", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = submit_council_proposal_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        proposal_id=args.proposal_id,
        proposal_kind=args.proposal_kind,
        agent_role=args.agent_role,
        rationale=args.rationale,
        decision_source=args.decision_source,
        status=args.status,
        confidence=args.confidence,
        target_kind=args.target_kind,
        target_id=args.target_id,
        target_claim_id=args.target_claim_id,
        target_hypothesis_id=args.target_hypothesis_id,
        target_ticket_id=args.target_ticket_id,
        target_task_id=args.target_task_id,
        target_json=args.target_json,
        action_kind=args.action_kind,
        assigned_role=args.assigned_role,
        objective=args.objective,
        summary=args.summary,
        evidence_refs=args.evidence_ref,
        evidence_refs_json=args.evidence_refs_json,
        response_to_ids=args.response_to_id,
        response_to_ids_json=args.response_to_ids_json,
        lineage=args.lineage_id,
        lineage_json=args.lineage_json,
        provenance_json=args.provenance_json,
        extra_json=args.extra_json,
        promotion_disposition=args.promotion_disposition,
        promote_allowed=args.promote_allowed,
        publication_readiness=args.publication_readiness,
        handoff_status=args.handoff_status,
        moderator_status=args.moderator_status,
        output_path=args.output_path,
    )
    sys.stdout.write(pretty_json(payload, args.pretty))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
