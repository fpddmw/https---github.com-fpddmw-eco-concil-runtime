from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


PLANE_SIGNAL = "signal"
PLANE_ANALYSIS = "analysis"
PLANE_DELIBERATION = "deliberation"
PLANE_REPORTING = "reporting"


@dataclass(frozen=True)
class CanonicalContract:
    object_kind: str
    plane: str
    schema_version: str
    id_field: str
    required_text_fields: tuple[str, ...]
    required_list_fields: tuple[str, ...]
    required_dict_fields: tuple[str, ...]
    required_number_fields: tuple[str, ...]
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
    required_number_fields: tuple[str, ...] = (),
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
        required_number_fields=required_number_fields,
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
        required_text_fields=(
            "decision_source",
            "map_issue_id",
            "claim_cluster_id",
            "issue_label",
            "claim_type",
            "dominant_stance",
            "verifiability_kind",
            "dispute_type",
            "recommended_lane",
            "route_status",
            "controversy_posture",
            "issue_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "claim_ids",
            "source_signal_ids",
            "stance_distribution",
            "stance_group_ids",
            "concern_ids",
            "actor_ids",
            "citation_type_ids",
            "concern_facets",
            "actor_hints",
            "evidence_citation_types",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=(
            "member_count",
            "aggregate_source_signal_count",
            "confidence",
        ),
    ),
    "stance-group": _contract(
        "stance-group",
        plane=PLANE_ANALYSIS,
        schema_version="stance-group-v1",
        id_field="stance_group_id",
        required_text_fields=(
            "decision_source",
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "issue_label",
            "claim_type",
            "stance_label",
            "recommended_lane",
            "route_status",
            "controversy_posture",
            "stance_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "claim_ids",
            "source_signal_ids",
            "concern_facets",
            "actor_hints",
            "evidence_citation_types",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=("member_count", "share_ratio", "confidence"),
    ),
    "concern-facet": _contract(
        "concern-facet",
        plane=PLANE_ANALYSIS,
        schema_version="concern-facet-v1",
        id_field="concern_id",
        required_text_fields=(
            "decision_source",
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "issue_label",
            "claim_type",
            "concern_label",
            "priority",
            "recommended_lane",
            "route_status",
            "concern_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "claim_ids",
            "source_signal_ids",
            "actor_hints",
            "evidence_citation_types",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=(
            "affected_claim_count",
            "source_signal_count",
            "confidence",
        ),
    ),
    "actor-profile": _contract(
        "actor-profile",
        plane=PLANE_ANALYSIS,
        schema_version="actor-profile-v1",
        id_field="actor_id",
        required_text_fields=(
            "decision_source",
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "issue_label",
            "claim_type",
            "display_name",
            "actor_label",
            "dominant_stance",
            "recommended_lane",
            "route_status",
            "profile_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "claim_ids",
            "source_signal_ids",
            "concern_facets",
            "evidence_citation_types",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=("claim_count", "source_signal_count", "confidence"),
    ),
    "evidence-citation-type": _contract(
        "evidence-citation-type",
        plane=PLANE_ANALYSIS,
        schema_version="evidence-citation-type-v1",
        id_field="citation_type_id",
        required_text_fields=(
            "decision_source",
            "cluster_id",
            "map_issue_id",
            "claim_cluster_id",
            "issue_label",
            "claim_type",
            "citation_type",
            "dominant_stance",
            "recommended_lane",
            "route_status",
            "citation_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "claim_ids",
            "source_signal_ids",
            "concern_facets",
            "actor_hints",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=("claim_count", "source_signal_count", "confidence"),
    ),
    "claim-candidate": _contract(
        "claim-candidate",
        plane=PLANE_ANALYSIS,
        schema_version="claim-candidate-v1",
        id_field="claim_id",
        required_text_fields=(
            "decision_source",
            "agent_role",
            "claim_type",
            "status",
            "summary",
            "statement",
            "issue_hint",
            "stance_hint",
            "verifiability_hint",
            "dispute_type",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "issue_terms",
            "concern_facets",
            "actor_hints",
            "evidence_citation_types",
            "source_signal_ids",
            "public_refs",
        ),
        required_dict_fields=(
            "provenance",
            "controversy_seed",
            "time_window",
            "place_scope",
            "claim_scope",
            "compact_audit",
        ),
        required_number_fields=("confidence", "source_signal_count"),
    ),
    "claim-cluster": _contract(
        "claim-cluster",
        plane=PLANE_ANALYSIS,
        schema_version="claim-cluster-v1",
        id_field="cluster_id",
        required_text_fields=(
            "decision_source",
            "claim_type",
            "status",
            "cluster_label",
            "representative_statement",
            "semantic_fingerprint",
            "issue_label",
            "dominant_stance",
            "verifiability_posture",
            "dispute_type",
            "controversy_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "issue_terms",
            "concern_facets",
            "actor_hints",
            "evidence_citation_types",
            "member_claim_ids",
            "source_signal_ids",
            "stance_distribution",
            "member_summaries",
            "public_refs",
        ),
        required_dict_fields=("provenance", "time_window", "compact_audit"),
        required_number_fields=(
            "confidence",
            "member_count",
            "aggregate_source_signal_count",
            "unique_source_signal_count",
        ),
    ),
    "claim-scope": _contract(
        "claim-scope",
        plane=PLANE_ANALYSIS,
        schema_version="claim-scope-v1",
        id_field="claim_scope_id",
        required_text_fields=(
            "decision_source",
            "claim_id",
            "claim_type",
            "issue_hint",
            "scope_label",
            "scope_kind",
            "verifiability_kind",
            "dispute_type",
            "required_evidence_lane",
            "matching_eligibility_reason",
            "method",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "matching_tags",
            "issue_terms",
            "concern_facets",
            "actor_hints",
            "evidence_citation_types",
        ),
        required_dict_fields=("provenance", "claim_scope", "place_scope"),
        required_number_fields=("confidence",),
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
        required_text_fields=(
            "decision_source",
            "issue_label",
            "link_status",
            "recommended_lane",
            "route_status",
            "linkage_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "issue_terms",
            "concern_facets",
            "actor_hints",
            "cluster_ids",
            "claim_ids",
            "claim_scope_ids",
            "assessment_ids",
            "route_ids",
            "formal_signal_ids",
            "public_signal_ids",
            "formal_source_skills",
            "public_source_skills",
            "formal_examples",
            "public_examples",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=(
            "alignment_score",
            "formal_signal_count",
            "public_signal_count",
        ),
    ),
    "representation-gap": _contract(
        "representation-gap",
        plane=PLANE_ANALYSIS,
        schema_version="representation-gap-v1",
        id_field="gap_id",
        required_text_fields=(
            "decision_source",
            "linkage_id",
            "issue_label",
            "gap_type",
            "severity",
            "link_status",
            "recommended_lane",
            "route_status",
            "recommended_action",
            "gap_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "cluster_ids",
            "claim_ids",
            "claim_scope_ids",
            "assessment_ids",
            "route_ids",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=(
            "severity_score",
            "formal_signal_count",
            "public_signal_count",
        ),
    ),
    "diffusion-edge": _contract(
        "diffusion-edge",
        plane=PLANE_ANALYSIS,
        schema_version="diffusion-edge-v1",
        id_field="edge_id",
        required_text_fields=(
            "decision_source",
            "issue_label",
            "source_platform",
            "target_platform",
            "source_plane",
            "target_plane",
            "edge_type",
            "temporal_relation",
            "source_first_seen_utc",
            "target_first_seen_utc",
            "recommended_lane",
            "route_status",
            "edge_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "linkage_ids",
            "cluster_ids",
            "claim_ids",
            "claim_scope_ids",
            "assessment_ids",
            "route_ids",
            "source_signal_ids",
            "target_signal_ids",
            "source_source_skills",
            "target_source_skills",
            "source_examples",
            "target_examples",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=(
            "source_signal_count",
            "target_signal_count",
            "time_delta_hours",
            "confidence",
        ),
    ),
    "controversy-map": _contract(
        "controversy-map",
        plane=PLANE_ANALYSIS,
        schema_version="controversy-map-v1",
        id_field="map_issue_id",
        required_text_fields=(
            "decision_source",
            "cluster_id",
            "issue_label",
            "claim_type",
            "dominant_stance",
            "verifiability_kind",
            "dispute_type",
            "recommended_lane",
            "route_status",
            "controversy_posture",
            "controversy_summary",
            "rationale",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "claim_ids",
            "source_signal_ids",
            "stance_distribution",
            "concern_facets",
            "actor_hints",
            "evidence_citation_types",
        ),
        required_dict_fields=("provenance",),
        required_number_fields=(
            "member_count",
            "aggregate_source_signal_count",
            "confidence",
        ),
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
    "reporting-handoff": _contract(
        "reporting-handoff",
        plane=PLANE_REPORTING,
        schema_version="reporting-handoff-v1",
        id_field="handoff_id",
        required_text_fields=(
            "decision_source",
            "handoff_status",
            "promotion_status",
            "readiness_status",
            "supervisor_status",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "reporting_blockers",
            "selected_basis_object_ids",
            "selected_evidence_refs",
            "supporting_proposal_ids",
            "rejected_proposal_ids",
            "supporting_opinion_ids",
            "rejected_opinion_ids",
            "recommended_next_actions",
            "key_findings",
            "open_risks",
        ),
        required_dict_fields=(
            "provenance",
            "observed_inputs",
            "analysis_sync",
            "deliberation_sync",
            "council_input_counts",
        ),
    ),
    "council-decision": _contract(
        "council-decision",
        plane=PLANE_REPORTING,
        schema_version="council-decision-v1",
        id_field="record_id",
        required_text_fields=(
            "decision_source",
            "decision_id",
            "decision_stage",
            "moderator_status",
            "publication_readiness",
            "decision_summary",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "reporting_blockers",
            "selected_basis_object_ids",
            "selected_evidence_refs",
            "supporting_proposal_ids",
            "rejected_proposal_ids",
            "supporting_opinion_ids",
            "rejected_opinion_ids",
            "decision_trace_ids",
            "published_report_refs",
            "recommended_next_actions",
            "key_findings",
            "open_risks",
        ),
        required_dict_fields=(
            "provenance",
            "observed_inputs",
            "analysis_sync",
            "deliberation_sync",
            "decision_gating",
            "council_input_counts",
        ),
    ),
    "expert-report": _contract(
        "expert-report",
        plane=PLANE_REPORTING,
        schema_version="expert-report-v1",
        id_field="record_id",
        required_text_fields=(
            "decision_source",
            "report_id",
            "report_stage",
            "agent_role",
            "status",
            "handoff_status",
            "publication_readiness",
            "summary",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "reporting_blockers",
            "findings",
            "open_questions",
            "recommended_next_actions",
            "report_sections",
        ),
        required_dict_fields=(
            "provenance",
            "observed_inputs",
            "analysis_sync",
            "deliberation_sync",
            "audit_refs",
        ),
    ),
    "final-publication": _contract(
        "final-publication",
        plane=PLANE_REPORTING,
        schema_version="final-publication-v1",
        id_field="publication_id",
        required_text_fields=(
            "decision_source",
            "publication_status",
            "publication_posture",
            "publication_summary",
        ),
        required_list_fields=(
            "evidence_refs",
            "lineage",
            "published_sections",
            "decision_trace_ids",
            "decision_traces",
            "role_reports",
            "published_report_refs",
            "key_findings",
            "open_risks",
            "recommended_next_actions",
            "selected_evidence_refs",
            "operator_review_hints",
        ),
        required_dict_fields=(
            "provenance",
            "observed_inputs",
            "analysis_sync",
            "deliberation_sync",
            "decision",
            "audit_refs",
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
    invalid_number_fields = [
        field_name
        for field_name in contract.required_number_fields
        if not isinstance(normalized.get(field_name), (int, float))
        or isinstance(normalized.get(field_name), bool)
    ]
    if (
        missing_text_fields
        or invalid_list_fields
        or invalid_dict_fields
        or invalid_number_fields
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
        if invalid_number_fields:
            problems.append(
                "number fields required: " + ", ".join(sorted(invalid_number_fields))
            )
        raise ValueError(
            f"Invalid canonical payload for {object_kind}: " + "; ".join(problems)
        )
    return normalized
