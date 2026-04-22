from __future__ import annotations

from typing import Any

from .phase2_fallback_common import maybe_text

CLAIM_TARGET_KINDS = {"claim", "claim-candidate", "claim-cluster"}
HYPOTHESIS_TARGET_KINDS = {"hypothesis", "hypothesis-card"}
CHALLENGE_TARGET_KINDS = {"challenge", "challenge-ticket", "ticket"}
TASK_TARGET_KINDS = {"task", "board-task"}
ISSUE_TARGET_KINDS = {"controversy-map", "issue", "issue-cluster"}
ROUTE_TARGET_KINDS = {"route", "verification-route"}
ASSESSMENT_TARGET_KINDS = {
    "assessment",
    "claim-assessment",
    "verifiability-assessment",
}
LINKAGE_TARGET_KINDS = {"formal-public-link", "linkage"}
GAP_TARGET_KINDS = {"gap", "representation-gap"}
EDGE_TARGET_KINDS = {"diffusion-edge", "edge"}
COVERAGE_TARGET_KINDS = {"coverage", "evidence-coverage"}


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def canonical_target_kind(value: Any) -> str:
    normalized = maybe_text(value)
    if normalized in CLAIM_TARGET_KINDS:
        return "claim"
    if normalized in HYPOTHESIS_TARGET_KINDS:
        return "hypothesis"
    if normalized in CHALLENGE_TARGET_KINDS:
        return "challenge"
    if normalized in TASK_TARGET_KINDS:
        return "board-task"
    if normalized in ISSUE_TARGET_KINDS:
        return "issue-cluster"
    if normalized in ROUTE_TARGET_KINDS:
        return "verification-route"
    if normalized in ASSESSMENT_TARGET_KINDS:
        return "verifiability-assessment"
    if normalized in LINKAGE_TARGET_KINDS:
        return "formal-public-link"
    if normalized in GAP_TARGET_KINDS:
        return "representation-gap"
    if normalized in EDGE_TARGET_KINDS:
        return "diffusion-edge"
    if normalized in COVERAGE_TARGET_KINDS:
        return "evidence-coverage"
    return normalized


def infer_target_object_kind(target: Any) -> str:
    normalized = dict_items(target)
    explicit_kind = canonical_target_kind(
        maybe_text(normalized.get("object_kind")) or maybe_text(normalized.get("kind"))
    )
    if explicit_kind:
        return explicit_kind
    if maybe_text(normalized.get("route_id")):
        return "verification-route"
    if maybe_text(normalized.get("assessment_id")):
        return "verifiability-assessment"
    if maybe_text(normalized.get("linkage_id")):
        return "formal-public-link"
    if maybe_text(normalized.get("gap_id")):
        return "representation-gap"
    if maybe_text(normalized.get("edge_id")):
        return "diffusion-edge"
    if maybe_text(normalized.get("map_issue_id")) or maybe_text(normalized.get("cluster_id")):
        return "issue-cluster"
    if maybe_text(normalized.get("coverage_id")):
        return "evidence-coverage"
    if maybe_text(normalized.get("task_id")):
        return "board-task"
    if maybe_text(normalized.get("ticket_id")):
        return "challenge"
    if maybe_text(normalized.get("hypothesis_id")):
        return "hypothesis"
    if maybe_text(normalized.get("claim_id")):
        return "claim"
    if maybe_text(normalized.get("proposal_id")):
        return "proposal"
    if maybe_text(normalized.get("action_id")):
        return "next-action"
    if maybe_text(normalized.get("round_id")):
        return "round"
    return ""


def infer_target_object_id(target: Any, object_kind: str = "") -> str:
    normalized = dict_items(target)
    explicit_id = maybe_text(normalized.get("object_id")) or maybe_text(
        normalized.get("id")
    )
    if explicit_id:
        return explicit_id
    kind = canonical_target_kind(object_kind) or infer_target_object_kind(normalized)
    if kind == "verification-route":
        return maybe_text(normalized.get("route_id"))
    if kind == "verifiability-assessment":
        return maybe_text(normalized.get("assessment_id"))
    if kind == "formal-public-link":
        return maybe_text(normalized.get("linkage_id"))
    if kind == "representation-gap":
        return maybe_text(normalized.get("gap_id"))
    if kind == "diffusion-edge":
        return maybe_text(normalized.get("edge_id"))
    if kind == "issue-cluster":
        return maybe_text(normalized.get("map_issue_id")) or maybe_text(
            normalized.get("cluster_id")
        )
    if kind == "evidence-coverage":
        return maybe_text(normalized.get("coverage_id"))
    if kind == "board-task":
        return maybe_text(normalized.get("task_id"))
    if kind == "challenge":
        return maybe_text(normalized.get("ticket_id"))
    if kind == "hypothesis":
        return maybe_text(normalized.get("hypothesis_id"))
    if kind == "claim":
        return maybe_text(normalized.get("claim_id"))
    if kind == "proposal":
        return maybe_text(normalized.get("proposal_id"))
    if kind == "next-action":
        return maybe_text(normalized.get("action_id"))
    if kind == "round":
        return maybe_text(normalized.get("round_id"))
    return ""


def normalized_deliberation_target(
    target: Any,
    *,
    object_kind: str = "",
    object_id: str = "",
    issue_label: str = "",
    claim_id: str = "",
    hypothesis_id: str = "",
    ticket_id: str = "",
    task_id: str = "",
    coverage_id: str = "",
    route_id: str = "",
    assessment_id: str = "",
    linkage_id: str = "",
    gap_id: str = "",
    edge_id: str = "",
    map_issue_id: str = "",
    cluster_id: str = "",
    claim_cluster_id: str = "",
    round_id: str = "",
    action_id: str = "",
    proposal_id: str = "",
) -> dict[str, Any]:
    normalized = dict_items(target)
    explicit_values = {
        "object_kind": canonical_target_kind(object_kind),
        "object_id": maybe_text(object_id),
        "issue_label": maybe_text(issue_label),
        "claim_id": maybe_text(claim_id),
        "hypothesis_id": maybe_text(hypothesis_id),
        "ticket_id": maybe_text(ticket_id),
        "task_id": maybe_text(task_id),
        "coverage_id": maybe_text(coverage_id),
        "route_id": maybe_text(route_id),
        "assessment_id": maybe_text(assessment_id),
        "linkage_id": maybe_text(linkage_id),
        "gap_id": maybe_text(gap_id),
        "edge_id": maybe_text(edge_id),
        "map_issue_id": maybe_text(map_issue_id),
        "cluster_id": maybe_text(cluster_id),
        "claim_cluster_id": maybe_text(claim_cluster_id),
        "round_id": maybe_text(round_id),
        "action_id": maybe_text(action_id),
        "proposal_id": maybe_text(proposal_id),
    }
    for field_name, field_value in explicit_values.items():
        if field_value and not maybe_text(normalized.get(field_name)):
            normalized[field_name] = field_value

    resolved_kind = infer_target_object_kind(normalized)
    if not resolved_kind and maybe_text(action_id):
        resolved_kind = "next-action"
    if not resolved_kind and maybe_text(round_id):
        resolved_kind = "round"
    if resolved_kind:
        normalized["object_kind"] = resolved_kind

    resolved_id = infer_target_object_id(normalized, resolved_kind)
    if not resolved_id and resolved_kind == "next-action":
        resolved_id = maybe_text(action_id)
    if not resolved_id and resolved_kind == "round":
        resolved_id = maybe_text(round_id)
    if resolved_id:
        normalized["object_id"] = resolved_id

    if resolved_kind == "claim" and resolved_id and not maybe_text(normalized.get("claim_id")):
        normalized["claim_id"] = resolved_id
    if (
        resolved_kind == "hypothesis"
        and resolved_id
        and not maybe_text(normalized.get("hypothesis_id"))
    ):
        normalized["hypothesis_id"] = resolved_id
    if (
        resolved_kind == "challenge"
        and resolved_id
        and not maybe_text(normalized.get("ticket_id"))
    ):
        normalized["ticket_id"] = resolved_id
    if (
        resolved_kind == "board-task"
        and resolved_id
        and not maybe_text(normalized.get("task_id"))
    ):
        normalized["task_id"] = resolved_id
    if (
        resolved_kind == "evidence-coverage"
        and resolved_id
        and not maybe_text(normalized.get("coverage_id"))
    ):
        normalized["coverage_id"] = resolved_id
    if (
        resolved_kind == "verification-route"
        and resolved_id
        and not maybe_text(normalized.get("route_id"))
    ):
        normalized["route_id"] = resolved_id
    if (
        resolved_kind == "verifiability-assessment"
        and resolved_id
        and not maybe_text(normalized.get("assessment_id"))
    ):
        normalized["assessment_id"] = resolved_id
    if (
        resolved_kind == "formal-public-link"
        and resolved_id
        and not maybe_text(normalized.get("linkage_id"))
    ):
        normalized["linkage_id"] = resolved_id
    if (
        resolved_kind == "representation-gap"
        and resolved_id
        and not maybe_text(normalized.get("gap_id"))
    ):
        normalized["gap_id"] = resolved_id
    if (
        resolved_kind == "diffusion-edge"
        and resolved_id
        and not maybe_text(normalized.get("edge_id"))
    ):
        normalized["edge_id"] = resolved_id
    if (
        resolved_kind == "issue-cluster"
        and resolved_id
        and not maybe_text(normalized.get("map_issue_id"))
    ):
        normalized["map_issue_id"] = resolved_id
    if (
        resolved_kind == "proposal"
        and resolved_id
        and not maybe_text(normalized.get("proposal_id"))
    ):
        normalized["proposal_id"] = resolved_id
    if (
        resolved_kind == "next-action"
        and resolved_id
        and not maybe_text(normalized.get("action_id"))
    ):
        normalized["action_id"] = resolved_id

    resolved_issue_label = maybe_text(normalized.get("issue_label"))
    if not resolved_issue_label:
        resolved_issue_label = (
            maybe_text(issue_label)
            or maybe_text(normalized.get("map_issue_id"))
            or maybe_text(normalized.get("claim_id"))
        )
    if resolved_issue_label:
        normalized["issue_label"] = resolved_issue_label
    return normalized


def proposal_target_from_payload(proposal: dict[str, Any]) -> dict[str, Any]:
    payload = dict_items(proposal)
    return normalized_deliberation_target(
        payload.get("target"),
        object_kind=maybe_text(payload.get("target_kind")),
        object_id=maybe_text(payload.get("target_id")),
        issue_label=maybe_text(payload.get("issue_label")),
        claim_id=maybe_text(payload.get("target_claim_id")),
        hypothesis_id=maybe_text(payload.get("target_hypothesis_id")),
        ticket_id=maybe_text(payload.get("target_ticket_id")),
        task_id=maybe_text(payload.get("target_task_id")),
        coverage_id=maybe_text(payload.get("target_coverage_id"))
        or maybe_text(payload.get("coverage_id")),
        route_id=maybe_text(payload.get("target_route_id"))
        or maybe_text(payload.get("route_id")),
        assessment_id=maybe_text(payload.get("target_assessment_id"))
        or maybe_text(payload.get("assessment_id")),
        linkage_id=maybe_text(payload.get("target_linkage_id"))
        or maybe_text(payload.get("linkage_id")),
        gap_id=maybe_text(payload.get("target_gap_id"))
        or maybe_text(payload.get("gap_id")),
        edge_id=maybe_text(payload.get("target_edge_id"))
        or maybe_text(payload.get("edge_id")),
        map_issue_id=maybe_text(payload.get("target_map_issue_id"))
        or maybe_text(payload.get("map_issue_id")),
        cluster_id=maybe_text(payload.get("cluster_id")),
        claim_cluster_id=maybe_text(payload.get("claim_cluster_id")),
        round_id=maybe_text(payload.get("round_id")),
        proposal_id=maybe_text(payload.get("proposal_id")),
    )


def source_proposal_id_from_payload(payload: dict[str, Any]) -> str:
    normalized = dict_items(payload)
    for candidate in (
        normalized.get("source_proposal_id"),
        dict_items(normalized.get("provenance")).get("proposal_id"),
        dict_items(normalized.get("target")).get("proposal_id"),
    ):
        text = maybe_text(candidate)
        if text:
            return text
    for candidate in list_items(normalized.get("source_ids")) + list_items(
        normalized.get("lineage")
    ):
        text = maybe_text(candidate)
        if text.startswith("proposal-"):
            return text
    text = maybe_text(normalized.get("proposal_id"))
    return text if text.startswith("proposal-") else ""


def deliberation_anchor_fields(
    target: Any,
    *,
    source_proposal_id: str = "",
) -> dict[str, str]:
    normalized = normalized_deliberation_target(target)
    return {
        "target_object_kind": maybe_text(normalized.get("object_kind")),
        "target_object_id": maybe_text(normalized.get("object_id")),
        "issue_label": maybe_text(normalized.get("issue_label")),
        "target_route_id": maybe_text(normalized.get("route_id")),
        "target_assessment_id": maybe_text(normalized.get("assessment_id")),
        "target_linkage_id": maybe_text(normalized.get("linkage_id")),
        "target_gap_id": maybe_text(normalized.get("gap_id")),
        "source_proposal_id": maybe_text(source_proposal_id),
    }


__all__ = [
    "canonical_target_kind",
    "deliberation_anchor_fields",
    "infer_target_object_id",
    "infer_target_object_kind",
    "normalized_deliberation_target",
    "proposal_target_from_payload",
    "source_proposal_id_from_payload",
]
