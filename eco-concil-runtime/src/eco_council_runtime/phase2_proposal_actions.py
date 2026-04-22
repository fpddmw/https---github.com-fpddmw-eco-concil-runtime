from __future__ import annotations

from typing import Any

from .deliberation_target_semantics import proposal_target_from_payload
from .phase2_fallback_common import maybe_number, maybe_text, stable_hash, unique_texts

COUNCIL_PROPOSAL_POLICY_PROFILE = "agent-council-proposal-v1"
COUNCIL_PROPOSAL_POLICY_OWNER = "agent-council"
NON_EXECUTION_PROPOSAL_KINDS = {
    "claim-board-task",
    "close-challenge",
    "create-board-task",
    "create-hypothesis",
    "dismiss-challenge",
    "open-board-task",
    "open-challenge",
}
NON_EXECUTION_ACTION_KINDS = {
    "claim-board-task",
    "close-challenge-ticket",
    "create-board-task",
    "create-hypothesis",
    "dismiss-challenge",
    "dismiss-challenge-ticket",
    "open-board-task",
    "open-challenge-ticket",
}
DEFAULT_PROBE_ACTION_KINDS = {
    "advance-empirical-verification",
    "clarify-verification-route",
    "open-probe",
    "resolve-challenge",
    "resolve-contradiction",
}


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def council_proposal_annotation(
    *,
    decision_source: str,
    policy_profile: str = COUNCIL_PROPOSAL_POLICY_PROFILE,
) -> dict[str, str]:
    return {
        "policy_profile": maybe_text(policy_profile) or COUNCIL_PROPOSAL_POLICY_PROFILE,
        "policy_source": maybe_text(decision_source) or "agent-council",
        "policy_owner": COUNCIL_PROPOSAL_POLICY_OWNER,
    }


def proposal_target(proposal: dict[str, Any]) -> dict[str, Any]:
    return proposal_target_from_payload(proposal)


def action_signature(action: dict[str, Any]) -> str:
    target = action.get("target", {}) if isinstance(action.get("target"), dict) else {}
    return "|".join(
        [
            maybe_text(action.get("action_kind")),
            maybe_text(action.get("assigned_role")),
            maybe_text(target.get("object_kind")),
            maybe_text(target.get("object_id")),
            maybe_text(target.get("claim_id")),
            maybe_text(target.get("hypothesis_id")),
            maybe_text(target.get("ticket_id")),
            maybe_text(action.get("issue_label")),
        ]
    )


def proposal_drives_phase2_action_queue(
    proposal: dict[str, Any],
    *,
    non_execution_proposal_kinds: set[str] | None = None,
    non_execution_action_kinds: set[str] | None = None,
) -> bool:
    proposal_kind = maybe_text(proposal.get("proposal_kind"))
    action_kind = (
        maybe_text(proposal.get("action_kind"))
        or maybe_text(proposal.get("proposed_action_kind"))
        or proposal_kind
    )
    blocked_proposals = (
        non_execution_proposal_kinds
        if isinstance(non_execution_proposal_kinds, set)
        else NON_EXECUTION_PROPOSAL_KINDS
    )
    blocked_actions = (
        non_execution_action_kinds
        if isinstance(non_execution_action_kinds, set)
        else NON_EXECUTION_ACTION_KINDS
    )
    if proposal_kind in blocked_proposals or action_kind in blocked_actions:
        return False
    target_kind = maybe_text(proposal.get("target_kind"))
    return bool(action_kind or proposal_kind) and (
        bool(proposal.get("probe_candidate"))
        or bool(proposal.get("readiness_blocker"))
        or bool(maybe_text(proposal.get("recommended_lane")))
        or bool(maybe_text(proposal.get("controversy_gap")))
        or target_kind
        in {
            "issue-cluster",
            "claim",
            "claim-candidate",
            "claim-cluster",
            "challenge-ticket",
            "ticket",
            "round",
        }
    )


def action_from_council_proposal(
    proposal: dict[str, Any],
    *,
    default_assigned_role: str = "moderator",
    agenda_source: str = "agent-proposal",
    action_id_namespace: str = "council-proposal-action",
    probe_action_kinds: set[str] | None = None,
) -> dict[str, Any]:
    proposal_id = maybe_text(proposal.get("proposal_id"))
    target = proposal_target(proposal)
    response_to_ids = unique_texts(list_items(proposal.get("response_to_ids")))
    decision_source = maybe_text(proposal.get("decision_source")) or "agent-council"
    action_kind = (
        maybe_text(proposal.get("action_kind"))
        or maybe_text(proposal.get("proposed_action_kind"))
        or maybe_text(proposal.get("proposal_kind"))
        or "follow-council-proposal"
    )
    objective = (
        maybe_text(proposal.get("objective"))
        or maybe_text(proposal.get("summary"))
        or maybe_text(proposal.get("rationale"))
        or f"Execute council proposal {proposal_id or 'for this round'}."
    )
    reason = (
        maybe_text(proposal.get("rationale"))
        or maybe_text(proposal.get("summary"))
        or f"Council proposal {proposal_id or '<missing>'} requested this action."
    )
    confidence = maybe_number(proposal.get("confidence"))
    pressure_score = 0.95 if confidence is None else max(0.55, min(1.0, float(confidence)))
    probe_kinds = probe_action_kinds if isinstance(probe_action_kinds, set) else DEFAULT_PROBE_ACTION_KINDS
    return {
        "action_id": (
            maybe_text(proposal.get("proposed_action_id"))
            or maybe_text(proposal.get("action_id"))
            or "action-"
            + stable_hash(
                action_id_namespace,
                proposal_id,
                action_kind,
                maybe_text(proposal.get("agent_role")),
                maybe_text(target.get("object_id")),
                maybe_text(target.get("claim_id")),
                maybe_text(target.get("hypothesis_id")),
                maybe_text(target.get("ticket_id")),
            )[:12]
        ),
        "action_kind": action_kind,
        "priority": maybe_text(proposal.get("priority")) or "high",
        "assigned_role": (
            maybe_text(proposal.get("assigned_role"))
            or maybe_text(proposal.get("agent_role"))
            or default_assigned_role
        ),
        "objective": objective,
        "reason": reason,
        "source_ids": unique_texts(
            [proposal_id, maybe_text(proposal.get("target_id")), *response_to_ids]
        ),
        "target": target,
        "controversy_gap": maybe_text(proposal.get("controversy_gap")),
        "recommended_lane": maybe_text(proposal.get("recommended_lane")),
        "expected_outcome": (
            maybe_text(proposal.get("expected_outcome"))
            or maybe_text(proposal.get("desired_outcome"))
            or "Execute the council-proposed next step."
        ),
        "evidence_refs": unique_texts(list_items(proposal.get("evidence_refs"))),
        "probe_candidate": bool(proposal.get("probe_candidate")) or action_kind in probe_kinds,
        "contradiction_link_count": int(proposal.get("contradiction_link_count") or 0),
        "coverage_score": float(proposal.get("coverage_score") or 0.0),
        "confidence": confidence,
        "brief_context": reason,
        "agenda_source": maybe_text(agenda_source) or "agent-proposal",
        "issue_label": (
            maybe_text(proposal.get("issue_label"))
            or maybe_text(target.get("issue_label"))
            or maybe_text(target.get("map_issue_id"))
        ),
        "pressure_score": pressure_score,
        "readiness_blocker": bool(proposal.get("readiness_blocker", True)),
        "decision_source": decision_source,
        "lineage": unique_texts(
            [proposal_id, *response_to_ids, *list_items(proposal.get("lineage"))]
        ),
        "provenance": (
            proposal.get("provenance")
            if isinstance(proposal.get("provenance"), dict)
            else {
                "source_skill": "council-proposal",
                "proposal_id": proposal_id,
                "decision_source": decision_source,
            }
        ),
        "source_proposal_id": proposal_id,
        **council_proposal_annotation(decision_source=decision_source),
    }
