from __future__ import annotations

from pathlib import Path
from typing import Any

from .council_objects import query_council_objects

OPEN_CHALLENGE_PROPOSAL_KINDS = {
    "open-challenge",
    "open-challenge-ticket",
    "challenge-claim",
    "challenge-hypothesis",
}
OPEN_CHALLENGE_TARGET_KINDS = {"claim", "claim-cluster", "hypothesis", "hypothesis-card"}
CLOSE_CHALLENGE_PROPOSAL_KINDS = {
    "close-challenge",
    "close-challenge-ticket",
    "resolve-challenge",
    "dismiss-challenge",
}
CLOSE_CHALLENGE_TARGET_KINDS = {"challenge-ticket", "ticket"}
UPDATE_HYPOTHESIS_PROPOSAL_KINDS = {
    "update-hypothesis-status",
    "stabilize-hypothesis",
    "create-hypothesis",
    "open-hypothesis",
    "reopen-hypothesis",
    "retire-hypothesis",
}
UPDATE_HYPOTHESIS_TARGET_KINDS = {"hypothesis", "hypothesis-card"}


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


def proposal_target(proposal: dict[str, Any]) -> dict[str, Any]:
    target = proposal.get("target", {})
    if isinstance(target, dict) and target:
        return dict(target)
    target_kind = maybe_text(proposal.get("target_kind"))
    target_id = maybe_text(proposal.get("target_id"))
    resolved: dict[str, Any] = {}
    if target_kind:
        resolved["object_kind"] = target_kind
    if target_id:
        resolved["object_id"] = target_id
    if maybe_text(proposal.get("target_claim_id")):
        resolved["claim_id"] = maybe_text(proposal.get("target_claim_id"))
    if maybe_text(proposal.get("target_hypothesis_id")):
        resolved["hypothesis_id"] = maybe_text(proposal.get("target_hypothesis_id"))
    if maybe_text(proposal.get("target_ticket_id")):
        resolved["ticket_id"] = maybe_text(proposal.get("target_ticket_id"))
    if target_kind in {"claim", "claim-candidate", "claim-cluster"} and target_id:
        resolved.setdefault("claim_id", target_id)
    if target_kind in {"hypothesis", "hypothesis-card"} and target_id:
        resolved.setdefault("hypothesis_id", target_id)
    if target_kind in {"challenge-ticket", "ticket"} and target_id:
        resolved.setdefault("ticket_id", target_id)
    return resolved


def proposal_operation_kinds(proposal: dict[str, Any]) -> set[str]:
    return {
        text
        for text in unique_texts(
            [
                proposal.get("proposal_kind"),
                proposal.get("action_kind"),
                proposal.get("proposed_action_kind"),
            ]
        )
        if text
    }


def load_council_proposals(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = query_council_objects(
        run_dir,
        object_kind="proposal",
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    proposals = (
        payload.get("objects", [])
        if isinstance(payload.get("objects"), list)
        else []
    )
    return [
        proposal
        for proposal in proposals
        if isinstance(proposal, dict)
        and maybe_text(proposal.get("status")) not in {"rejected", "withdrawn", "closed"}
    ]


def select_council_proposal(
    proposals: list[dict[str, Any]],
    *,
    proposal_id: str = "",
    accepted_kinds: set[str] | None = None,
    accepted_target_kinds: set[str] | None = None,
) -> dict[str, Any] | None:
    accepted_kind_set = accepted_kinds or set()
    accepted_target_kind_set = accepted_target_kinds or set()
    requested_proposal_id = maybe_text(proposal_id)
    for proposal in proposals:
        if not isinstance(proposal, dict):
            continue
        current_id = maybe_text(proposal.get("proposal_id"))
        if requested_proposal_id and current_id != requested_proposal_id:
            continue
        kinds = proposal_operation_kinds(proposal)
        target_kind = maybe_text(proposal_target(proposal).get("object_kind"))
        kind_match = bool(accepted_kind_set.intersection(kinds))
        target_kind_match = target_kind in accepted_target_kind_set
        if accepted_kind_set or accepted_target_kind_set:
            if not kind_match and not target_kind_match:
                continue
        return proposal
    return None


def resolved_hypothesis_id_from_proposal(proposal: dict[str, Any]) -> str:
    target = proposal_target(proposal)
    return (
        maybe_text(proposal.get("hypothesis_id"))
        or maybe_text(proposal.get("proposed_hypothesis_id"))
        or maybe_text(proposal.get("target_hypothesis_id"))
        or maybe_text(target.get("hypothesis_id"))
        or (
            maybe_text(target.get("object_id"))
            if maybe_text(target.get("object_kind")) in UPDATE_HYPOTHESIS_TARGET_KINDS
            else ""
        )
    )


def resolved_ticket_id_from_proposal(proposal: dict[str, Any]) -> str:
    target = proposal_target(proposal)
    return (
        maybe_text(proposal.get("ticket_id"))
        or maybe_text(proposal.get("proposed_ticket_id"))
        or maybe_text(proposal.get("target_ticket_id"))
        or maybe_text(target.get("ticket_id"))
        or (
            maybe_text(target.get("object_id"))
            if maybe_text(target.get("object_kind")) in CLOSE_CHALLENGE_TARGET_KINDS
            else ""
        )
    )


def board_judgement_metadata(
    proposal: dict[str, Any] | None,
    *,
    source_skill: str,
    default_decision_source: str,
    base_evidence_refs: list[Any] | None = None,
    base_lineage: list[Any] | None = None,
    base_source_ids: list[Any] | None = None,
) -> dict[str, Any]:
    proposal_payload = proposal if isinstance(proposal, dict) else {}
    proposal_id = maybe_text(proposal_payload.get("proposal_id"))
    decision_source = (
        maybe_text(proposal_payload.get("decision_source")) or default_decision_source
    )
    response_to_ids = unique_texts(list_items(proposal_payload.get("response_to_ids")))
    source_ids = unique_texts(
        [
            proposal_id,
            maybe_text(proposal_payload.get("target_id")),
            *(base_source_ids or []),
        ]
    )
    lineage = unique_texts(
        [
            proposal_id,
            *response_to_ids,
            *list_items(proposal_payload.get("lineage")),
            *(base_lineage or []),
            *source_ids,
        ]
    )
    evidence_refs = unique_texts(
        list_items(proposal_payload.get("evidence_refs")) + list(base_evidence_refs or [])
    )
    provenance_value = proposal_payload.get("provenance")
    provenance = dict(provenance_value) if isinstance(provenance_value, dict) else {}
    if source_skill and "source_skill" not in provenance:
        provenance["source_skill"] = source_skill
    if decision_source and "decision_source" not in provenance:
        provenance["decision_source"] = decision_source
    if proposal_id and "proposal_id" not in provenance:
        provenance["proposal_id"] = proposal_id
    return {
        "decision_source": decision_source,
        "evidence_refs": evidence_refs,
        "lineage": lineage,
        "source_ids": source_ids,
        "response_to_ids": response_to_ids,
        "provenance": provenance,
    }
