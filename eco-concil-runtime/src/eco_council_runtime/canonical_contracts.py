from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


PLANE_SIGNAL = "signal"
PLANE_ANALYSIS = "analysis"
PLANE_DELIBERATION = "deliberation"


@dataclass(frozen=True)
class CanonicalContract:
    object_kind: str
    plane: str
    schema_version: str
    id_field: str
    required_text_fields: tuple[str, ...]
    required_list_fields: tuple[str, ...]
    required_dict_fields: tuple[str, ...]
    item_level_query: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _contract(
    object_kind: str,
    *,
    plane: str,
    schema_version: str,
    id_field: str,
    required_text_fields: tuple[str, ...],
    required_list_fields: tuple[str, ...] = ("evidence_refs", "lineage"),
    required_dict_fields: tuple[str, ...] = ("provenance",),
    item_level_query: bool = True,
) -> CanonicalContract:
    return CanonicalContract(
        object_kind=object_kind,
        plane=plane,
        schema_version=schema_version,
        id_field=id_field,
        required_text_fields=("run_id", "round_id", id_field, *required_text_fields),
        required_list_fields=required_list_fields,
        required_dict_fields=required_dict_fields,
        item_level_query=item_level_query,
    )


CANONICAL_CONTRACTS: dict[str, CanonicalContract] = {
    "formal-comment-signal": _contract(
        "formal-comment-signal",
        plane=PLANE_SIGNAL,
        schema_version="formal-comment-signal-v1",
        id_field="signal_id",
        required_text_fields=("decision_source", "docket_id", "agency_id"),
    ),
    "public-discourse-signal": _contract(
        "public-discourse-signal",
        plane=PLANE_SIGNAL,
        schema_version="public-discourse-signal-v1",
        id_field="signal_id",
        required_text_fields=("decision_source", "source_skill"),
    ),
    "environment-observation-signal": _contract(
        "environment-observation-signal",
        plane=PLANE_SIGNAL,
        schema_version="environment-observation-signal-v1",
        id_field="signal_id",
        required_text_fields=("decision_source", "source_skill"),
    ),
    "issue-cluster": _contract(
        "issue-cluster",
        plane=PLANE_ANALYSIS,
        schema_version="issue-cluster-v1",
        id_field="cluster_id",
        required_text_fields=("decision_source", "issue_label"),
    ),
    "stance-group": _contract(
        "stance-group",
        plane=PLANE_ANALYSIS,
        schema_version="stance-group-v1",
        id_field="stance_group_id",
        required_text_fields=("decision_source", "issue_label", "stance_label"),
    ),
    "concern-facet": _contract(
        "concern-facet",
        plane=PLANE_ANALYSIS,
        schema_version="concern-facet-v1",
        id_field="concern_id",
        required_text_fields=("decision_source", "issue_label", "concern_label"),
    ),
    "actor-profile": _contract(
        "actor-profile",
        plane=PLANE_ANALYSIS,
        schema_version="actor-profile-v1",
        id_field="actor_id",
        required_text_fields=("decision_source", "display_name"),
    ),
    "evidence-citation-type": _contract(
        "evidence-citation-type",
        plane=PLANE_ANALYSIS,
        schema_version="evidence-citation-type-v1",
        id_field="citation_type_id",
        required_text_fields=("decision_source", "citation_type"),
    ),
    "verifiability-assessment": _contract(
        "verifiability-assessment",
        plane=PLANE_ANALYSIS,
        schema_version="verifiability-assessment-v1",
        id_field="assessment_id",
        required_text_fields=("decision_source", "recommended_lane"),
    ),
    "verification-route": _contract(
        "verification-route",
        plane=PLANE_ANALYSIS,
        schema_version="verification-route-v1",
        id_field="route_id",
        required_text_fields=("decision_source", "recommended_lane", "route_status"),
    ),
    "formal-public-link": _contract(
        "formal-public-link",
        plane=PLANE_ANALYSIS,
        schema_version="formal-public-link-v1",
        id_field="linkage_id",
        required_text_fields=("decision_source", "issue_label", "link_status"),
    ),
    "representation-gap": _contract(
        "representation-gap",
        plane=PLANE_ANALYSIS,
        schema_version="representation-gap-v1",
        id_field="gap_id",
        required_text_fields=("decision_source", "issue_label", "gap_type"),
    ),
    "diffusion-edge": _contract(
        "diffusion-edge",
        plane=PLANE_ANALYSIS,
        schema_version="diffusion-edge-v1",
        id_field="edge_id",
        required_text_fields=("decision_source", "issue_label", "edge_type"),
    ),
    "controversy-map": _contract(
        "controversy-map",
        plane=PLANE_ANALYSIS,
        schema_version="controversy-map-v1",
        id_field="map_issue_id",
        required_text_fields=("decision_source", "issue_label", "route_status"),
    ),
    "proposal": _contract(
        "proposal",
        plane=PLANE_DELIBERATION,
        schema_version="council-proposal-v1",
        id_field="proposal_id",
        required_text_fields=(
            "decision_source",
            "proposal_kind",
            "agent_role",
            "status",
            "rationale",
        ),
        required_list_fields=("evidence_refs", "lineage", "response_to_ids"),
    ),
    "hypothesis": _contract(
        "hypothesis",
        plane=PLANE_DELIBERATION,
        schema_version="hypothesis-card-v1",
        id_field="hypothesis_id",
        required_text_fields=("decision_source", "status", "title", "owner_role"),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "linked_claim_ids",
            "source_ids",
        ),
    ),
    "challenge": _contract(
        "challenge",
        plane=PLANE_DELIBERATION,
        schema_version="challenge-ticket-v1",
        id_field="ticket_id",
        required_text_fields=(
            "decision_source",
            "status",
            "title",
            "challenge_statement",
            "owner_role",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "source_ids",
            "linked_artifact_refs",
            "related_task_ids",
        ),
    ),
    "board-task": _contract(
        "board-task",
        plane=PLANE_DELIBERATION,
        schema_version="board-task-v1",
        id_field="task_id",
        required_text_fields=("decision_source", "status", "title", "owner_role"),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "source_ids",
            "linked_artifact_refs",
            "related_ids",
        ),
    ),
    "next-action": _contract(
        "next-action",
        plane=PLANE_DELIBERATION,
        schema_version="next-action-v1",
        id_field="action_id",
        required_text_fields=("decision_source", "action_kind", "reason"),
    ),
    "probe": _contract(
        "probe",
        plane=PLANE_DELIBERATION,
        schema_version="probe-v1",
        id_field="probe_id",
        required_text_fields=("decision_source", "probe_type", "falsification_question"),
    ),
    "readiness-opinion": _contract(
        "readiness-opinion",
        plane=PLANE_DELIBERATION,
        schema_version="readiness-opinion-v1",
        id_field="opinion_id",
        required_text_fields=(
            "decision_source",
            "agent_role",
            "opinion_status",
            "readiness_status",
            "rationale",
        ),
        required_list_fields=("evidence_refs", "lineage", "basis_object_ids"),
    ),
    "readiness-assessment": _contract(
        "readiness-assessment",
        plane=PLANE_DELIBERATION,
        schema_version="readiness-assessment-v1",
        id_field="readiness_id",
        required_text_fields=("decision_source", "readiness_status"),
    ),
    "promotion-basis": _contract(
        "promotion-basis",
        plane=PLANE_DELIBERATION,
        schema_version="promotion-basis-v1",
        id_field="basis_id",
        required_text_fields=("decision_source", "promotion_status", "readiness_status"),
    ),
    "decision-trace": _contract(
        "decision-trace",
        plane=PLANE_DELIBERATION,
        schema_version="decision-trace-v1",
        id_field="trace_id",
        required_text_fields=("decision_source", "decision_id", "decision_kind", "status", "rationale"),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "accepted_object_ids",
            "rejected_object_ids",
        ),
    ),
}


def canonical_contract(object_kind: str) -> CanonicalContract:
    contract = CANONICAL_CONTRACTS.get(maybe_text(object_kind))
    if contract is None:
        raise ValueError(f"Unknown canonical object kind: {object_kind!r}")
    return contract


def canonical_contract_kinds(*, plane: str = "") -> list[str]:
    normalized_plane = maybe_text(plane)
    return sorted(
        object_kind
        for object_kind, contract in CANONICAL_CONTRACTS.items()
        if not normalized_plane or contract.plane == normalized_plane
    )


def canonical_contracts_for_plane(*, plane: str = "") -> list[dict[str, Any]]:
    return [
        canonical_contract(object_kind).as_dict()
        for object_kind in canonical_contract_kinds(plane=plane)
    ]


def validate_canonical_payload(
    object_kind: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    contract = canonical_contract(object_kind)
    normalized = dict(payload)
    normalized["schema_version"] = (
        maybe_text(normalized.get("schema_version")) or contract.schema_version
    )

    missing_text_fields = [
        field_name
        for field_name in contract.required_text_fields
        if not maybe_text(normalized.get(field_name))
    ]
    invalid_list_fields = [
        field_name
        for field_name in contract.required_list_fields
        if not isinstance(normalized.get(field_name), list)
    ]
    invalid_dict_fields = [
        field_name
        for field_name in contract.required_dict_fields
        if not isinstance(normalized.get(field_name), dict)
    ]
    if (
        missing_text_fields
        or invalid_list_fields
        or invalid_dict_fields
    ):
        problems: list[str] = []
        if missing_text_fields:
            problems.append(
                "missing text fields: " + ", ".join(sorted(missing_text_fields))
            )
        if invalid_list_fields:
            problems.append(
                "list fields required: " + ", ".join(sorted(invalid_list_fields))
            )
        if invalid_dict_fields:
            problems.append(
                "dict fields required: " + ", ".join(sorted(invalid_dict_fields))
            )
        raise ValueError(
            f"Invalid canonical payload for {object_kind}: " + "; ".join(problems)
        )
    return normalized
