from __future__ import annotations

from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from .common import (
    OBJECT_KIND_ACTOR_PROFILE,
    OBJECT_KIND_CONCERN_FACET,
    OBJECT_KIND_CONTROVERSY_MAP,
    OBJECT_KIND_EVIDENCE_CITATION_TYPE,
    OBJECT_KIND_ISSUE_CLUSTER,
    OBJECT_KIND_STANCE_GROUP,
    actor_profile_confidence,
    citation_type_confidence,
    concern_facet_confidence,
    controversy_map_confidence,
    controversy_posture_from_route,
    list_items,
    maybe_number,
    maybe_text,
    merged_lineage,
    normalized_provenance,
    stance_group_confidence,
    unique_artifact_refs,
    unique_texts,
)


def normalize_issue_cluster_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    cluster_id = (
        maybe_text(normalized.get("cluster_id"))
        or maybe_text(normalized.get("map_issue_id"))
        or maybe_text(normalized.get("claim_cluster_id"))
    )
    map_issue_id = maybe_text(normalized.get("map_issue_id")) or cluster_id
    claim_cluster_id = (
        maybe_text(normalized.get("claim_cluster_id"))
        or maybe_text(normalized.get("source_cluster_id"))
        or maybe_text(normalized.get("parent_cluster_id"))
        or cluster_id
    )
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    member_count = int(
        maybe_number(normalized.get("member_count")) or len(claim_ids) or 1
    )
    aggregate_source_signal_count = int(
        maybe_number(normalized.get("aggregate_source_signal_count"))
        or len(source_signal_ids)
    )
    recommended_lane = maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    route_status = maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    controversy_posture = (
        maybe_text(normalized.get("controversy_posture"))
        or controversy_posture_from_route(route_status, recommended_lane)
    )
    confidence = maybe_number(normalized.get("confidence"))
    stance_distribution = list_items(normalized.get("stance_distribution"))
    if not stance_distribution and maybe_text(normalized.get("dominant_stance")):
        stance_distribution = [
            {
                "stance": maybe_text(normalized.get("dominant_stance")),
                "count": member_count,
            }
        ]
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_ISSUE_CLUSTER
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["cluster_id"] = cluster_id
    normalized["map_issue_id"] = map_issue_id
    normalized["claim_cluster_id"] = claim_cluster_id
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["claim_type"] = maybe_text(normalized.get("claim_type")) or "public-claim"
    normalized["dominant_stance"] = (
        maybe_text(normalized.get("dominant_stance")) or "unclear"
    )
    normalized["verifiability_kind"] = (
        maybe_text(normalized.get("verifiability_kind")) or "mixed-public-claim"
    )
    normalized["dispute_type"] = (
        maybe_text(normalized.get("dispute_type")) or "mixed-controversy"
    )
    normalized["recommended_lane"] = recommended_lane
    normalized["route_status"] = route_status
    normalized["controversy_posture"] = controversy_posture
    normalized["claim_ids"] = claim_ids
    normalized["source_signal_ids"] = source_signal_ids
    normalized["stance_distribution"] = stance_distribution
    normalized["stance_group_ids"] = unique_texts(
        list_items(normalized.get("stance_group_ids"))
    )
    normalized["concern_ids"] = unique_texts(list_items(normalized.get("concern_ids")))
    normalized["actor_ids"] = unique_texts(list_items(normalized.get("actor_ids")))
    normalized["citation_type_ids"] = unique_texts(
        list_items(normalized.get("citation_type_ids"))
    )
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["evidence_citation_types"] = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    normalized["member_count"] = member_count
    normalized["aggregate_source_signal_count"] = aggregate_source_signal_count
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else controversy_map_confidence(
            member_count=member_count,
            aggregate_source_signal_count=aggregate_source_signal_count,
            route_status=route_status,
            controversy_posture=controversy_posture,
        )
    )
    normalized["issue_summary"] = maybe_text(normalized.get("issue_summary")) or (
        maybe_text(normalized.get("controversy_summary"))
        or (
            f"Issue {normalized['issue_label']} is represented as one typed issue-cluster "
            f"under {route_status.replace('-', ' ')}."
        )
    )
    normalized["controversy_summary"] = (
        maybe_text(normalized.get("controversy_summary"))
        or normalized["issue_summary"]
    )
    normalized["should_query_environment"] = bool(
        normalized.get("should_query_environment")
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=20,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        map_issue_id,
        claim_cluster_id,
        maybe_text(normalized.get("claim_scope_id")),
        maybe_text(normalized.get("assessment_id")),
        maybe_text(normalized.get("route_id")),
        claim_ids,
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Projected the controversy-map issue into a canonical issue-cluster "
        f"object for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "claim_scope_id": maybe_text(normalized.get("claim_scope_id")),
            "assessment_id": maybe_text(normalized.get("assessment_id")),
            "route_id": maybe_text(normalized.get("route_id")),
            "parent_object_kind": "controversy-map",
        },
    )
    return validate_canonical_payload(OBJECT_KIND_ISSUE_CLUSTER, normalized)


def normalize_stance_group_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    cluster_id = (
        maybe_text(normalized.get("cluster_id"))
        or maybe_text(normalized.get("map_issue_id"))
        or maybe_text(normalized.get("claim_cluster_id"))
    )
    map_issue_id = maybe_text(normalized.get("map_issue_id")) or cluster_id
    claim_cluster_id = (
        maybe_text(normalized.get("claim_cluster_id")) or cluster_id
    )
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    member_count = int(
        maybe_number(normalized.get("member_count")) or len(claim_ids) or 1
    )
    total_member_count = int(
        maybe_number(normalized.get("total_member_count")) or member_count or 1
    )
    share_ratio = maybe_number(normalized.get("share_ratio"))
    if share_ratio is None:
        share_ratio = round(member_count / max(total_member_count, 1), 3)
    recommended_lane = maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    route_status = maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    controversy_posture = (
        maybe_text(normalized.get("controversy_posture"))
        or controversy_posture_from_route(route_status, recommended_lane)
    )
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_STANCE_GROUP
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["cluster_id"] = cluster_id
    normalized["map_issue_id"] = map_issue_id
    normalized["claim_cluster_id"] = claim_cluster_id
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["claim_type"] = maybe_text(normalized.get("claim_type")) or "public-claim"
    normalized["stance_label"] = (
        maybe_text(normalized.get("stance_label"))
        or maybe_text(normalized.get("dominant_stance"))
        or "unclear"
    )
    normalized["recommended_lane"] = recommended_lane
    normalized["route_status"] = route_status
    normalized["controversy_posture"] = controversy_posture
    normalized["claim_ids"] = claim_ids
    normalized["source_signal_ids"] = source_signal_ids
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["evidence_citation_types"] = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    normalized["member_count"] = member_count
    normalized["share_ratio"] = round(max(0.0, min(1.0, float(share_ratio))), 3)
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else stance_group_confidence(
            member_count=member_count,
            share_ratio=normalized["share_ratio"],
        )
    )
    normalized["stance_summary"] = maybe_text(normalized.get("stance_summary")) or (
        f"Issue {normalized['issue_label']} retains a {normalized['stance_label']} "
        f"stance bloc covering {member_count} claims."
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        cluster_id,
        map_issue_id,
        claim_cluster_id,
        maybe_text(normalized.get("claim_scope_id")),
        maybe_text(normalized.get("assessment_id")),
        maybe_text(normalized.get("route_id")),
        claim_ids,
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Projected the issue-level stance distribution into a canonical "
        f"stance-group object for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "total_member_count": total_member_count,
            "claim_scope_id": maybe_text(normalized.get("claim_scope_id")),
            "assessment_id": maybe_text(normalized.get("assessment_id")),
            "route_id": maybe_text(normalized.get("route_id")),
            "parent_object_kind": "issue-cluster",
        },
    )
    return validate_canonical_payload(OBJECT_KIND_STANCE_GROUP, normalized)


def normalize_concern_facet_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    cluster_id = (
        maybe_text(normalized.get("cluster_id"))
        or maybe_text(normalized.get("map_issue_id"))
        or maybe_text(normalized.get("claim_cluster_id"))
    )
    map_issue_id = maybe_text(normalized.get("map_issue_id")) or cluster_id
    claim_cluster_id = (
        maybe_text(normalized.get("claim_cluster_id")) or cluster_id
    )
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    affected_claim_count = int(
        maybe_number(normalized.get("affected_claim_count")) or len(claim_ids) or 1
    )
    source_signal_count = int(
        maybe_number(normalized.get("source_signal_count")) or len(source_signal_ids)
    )
    priority = maybe_text(normalized.get("priority")) or "supporting"
    recommended_lane = maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    route_status = maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_CONCERN_FACET
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["cluster_id"] = cluster_id
    normalized["map_issue_id"] = map_issue_id
    normalized["claim_cluster_id"] = claim_cluster_id
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["claim_type"] = maybe_text(normalized.get("claim_type")) or "public-claim"
    normalized["concern_label"] = (
        maybe_text(normalized.get("concern_label")) or "general-concern"
    )
    normalized["priority"] = priority
    normalized["recommended_lane"] = recommended_lane
    normalized["route_status"] = route_status
    normalized["claim_ids"] = claim_ids
    normalized["source_signal_ids"] = source_signal_ids
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["evidence_citation_types"] = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    normalized["affected_claim_count"] = affected_claim_count
    normalized["source_signal_count"] = source_signal_count
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else concern_facet_confidence(
            affected_claim_count=affected_claim_count,
            source_signal_count=source_signal_count,
            priority=priority,
        )
    )
    normalized["concern_summary"] = maybe_text(normalized.get("concern_summary")) or (
        f"Issue {normalized['issue_label']} carries the {normalized['concern_label']} "
        f"concern as a {priority} controversy facet."
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        cluster_id,
        map_issue_id,
        claim_cluster_id,
        maybe_text(normalized.get("claim_scope_id")),
        maybe_text(normalized.get("assessment_id")),
        maybe_text(normalized.get("route_id")),
        claim_ids,
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Projected the issue-level concern set into a canonical concern-facet "
        f"object for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "claim_scope_id": maybe_text(normalized.get("claim_scope_id")),
            "assessment_id": maybe_text(normalized.get("assessment_id")),
            "route_id": maybe_text(normalized.get("route_id")),
            "parent_object_kind": "issue-cluster",
        },
    )
    return validate_canonical_payload(OBJECT_KIND_CONCERN_FACET, normalized)


def normalize_actor_profile_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    cluster_id = (
        maybe_text(normalized.get("cluster_id"))
        or maybe_text(normalized.get("map_issue_id"))
        or maybe_text(normalized.get("claim_cluster_id"))
    )
    map_issue_id = maybe_text(normalized.get("map_issue_id")) or cluster_id
    claim_cluster_id = (
        maybe_text(normalized.get("claim_cluster_id")) or cluster_id
    )
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    claim_count = int(maybe_number(normalized.get("claim_count")) or len(claim_ids) or 1)
    source_signal_count = int(
        maybe_number(normalized.get("source_signal_count")) or len(source_signal_ids)
    )
    confidence = maybe_number(normalized.get("confidence"))
    display_name = maybe_text(normalized.get("display_name"))
    actor_label = maybe_text(normalized.get("actor_label")) or display_name
    if not display_name:
        display_name = actor_label
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_ACTOR_PROFILE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["cluster_id"] = cluster_id
    normalized["map_issue_id"] = map_issue_id
    normalized["claim_cluster_id"] = claim_cluster_id
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["claim_type"] = maybe_text(normalized.get("claim_type")) or "public-claim"
    normalized["display_name"] = display_name or "unspecified-actor"
    normalized["actor_label"] = actor_label or normalized["display_name"]
    normalized["dominant_stance"] = (
        maybe_text(normalized.get("dominant_stance")) or "unclear"
    )
    normalized["recommended_lane"] = (
        maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    )
    normalized["route_status"] = (
        maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    )
    normalized["claim_ids"] = claim_ids
    normalized["source_signal_ids"] = source_signal_ids
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["evidence_citation_types"] = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    normalized["claim_count"] = claim_count
    normalized["source_signal_count"] = source_signal_count
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else actor_profile_confidence(
            claim_count=claim_count,
            source_signal_count=source_signal_count,
        )
    )
    normalized["profile_summary"] = maybe_text(normalized.get("profile_summary")) or (
        f"Actor {normalized['display_name']} appears in the {normalized['issue_label']} "
        f"issue cluster with a dominant {normalized['dominant_stance']} stance."
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        cluster_id,
        map_issue_id,
        claim_cluster_id,
        maybe_text(normalized.get("claim_scope_id")),
        maybe_text(normalized.get("assessment_id")),
        maybe_text(normalized.get("route_id")),
        claim_ids,
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Projected the actor hints on one issue-cluster into a canonical "
        f"actor-profile object for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "claim_scope_id": maybe_text(normalized.get("claim_scope_id")),
            "assessment_id": maybe_text(normalized.get("assessment_id")),
            "route_id": maybe_text(normalized.get("route_id")),
            "parent_object_kind": "issue-cluster",
        },
    )
    return validate_canonical_payload(OBJECT_KIND_ACTOR_PROFILE, normalized)


def normalize_evidence_citation_type_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    cluster_id = (
        maybe_text(normalized.get("cluster_id"))
        or maybe_text(normalized.get("map_issue_id"))
        or maybe_text(normalized.get("claim_cluster_id"))
    )
    map_issue_id = maybe_text(normalized.get("map_issue_id")) or cluster_id
    claim_cluster_id = (
        maybe_text(normalized.get("claim_cluster_id")) or cluster_id
    )
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    claim_count = int(maybe_number(normalized.get("claim_count")) or len(claim_ids) or 1)
    source_signal_count = int(
        maybe_number(normalized.get("source_signal_count")) or len(source_signal_ids)
    )
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_EVIDENCE_CITATION_TYPE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["cluster_id"] = cluster_id
    normalized["map_issue_id"] = map_issue_id
    normalized["claim_cluster_id"] = claim_cluster_id
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["claim_type"] = maybe_text(normalized.get("claim_type")) or "public-claim"
    normalized["citation_type"] = (
        maybe_text(normalized.get("citation_type")) or "uncategorized-citation"
    )
    normalized["dominant_stance"] = (
        maybe_text(normalized.get("dominant_stance")) or "unclear"
    )
    normalized["recommended_lane"] = (
        maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    )
    normalized["route_status"] = (
        maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    )
    normalized["claim_ids"] = claim_ids
    normalized["source_signal_ids"] = source_signal_ids
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["claim_count"] = claim_count
    normalized["source_signal_count"] = source_signal_count
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else citation_type_confidence(
            claim_count=claim_count,
            source_signal_count=source_signal_count,
        )
    )
    normalized["citation_summary"] = maybe_text(
        normalized.get("citation_summary")
    ) or (
        f"Issue {normalized['issue_label']} currently leans on "
        f"{normalized['citation_type']} evidence."
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        cluster_id,
        map_issue_id,
        claim_cluster_id,
        maybe_text(normalized.get("claim_scope_id")),
        maybe_text(normalized.get("assessment_id")),
        maybe_text(normalized.get("route_id")),
        claim_ids,
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Projected the issue-level evidence basis into a canonical "
        f"evidence-citation-type object for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "claim_scope_id": maybe_text(normalized.get("claim_scope_id")),
            "assessment_id": maybe_text(normalized.get("assessment_id")),
            "route_id": maybe_text(normalized.get("route_id")),
            "parent_object_kind": "issue-cluster",
        },
    )
    return validate_canonical_payload(OBJECT_KIND_EVIDENCE_CITATION_TYPE, normalized)


def normalize_controversy_map_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    member_count = int(
        maybe_number(normalized.get("member_count")) or len(claim_ids) or 1
    )
    aggregate_source_signal_count = int(
        maybe_number(normalized.get("aggregate_source_signal_count"))
        or len(source_signal_ids)
    )
    recommended_lane = maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    route_status = maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    controversy_posture = (
        maybe_text(normalized.get("controversy_posture"))
        or controversy_posture_from_route(route_status, recommended_lane)
    )
    cluster_id = maybe_text(normalized.get("cluster_id")) or (
        claim_ids[0] if claim_ids else maybe_text(normalized.get("map_issue_id"))
    )
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_CONTROVERSY_MAP
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["cluster_id"] = cluster_id
    normalized["claim_ids"] = claim_ids
    normalized["source_signal_ids"] = source_signal_ids
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["claim_type"] = maybe_text(normalized.get("claim_type")) or "public-claim"
    normalized["dominant_stance"] = (
        maybe_text(normalized.get("dominant_stance")) or "unclear"
    )
    normalized["stance_distribution"] = list_items(
        normalized.get("stance_distribution")
    )
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["evidence_citation_types"] = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    normalized["verifiability_kind"] = (
        maybe_text(normalized.get("verifiability_kind")) or "mixed-public-claim"
    )
    normalized["dispute_type"] = (
        maybe_text(normalized.get("dispute_type")) or "mixed-controversy"
    )
    normalized["claim_scope_id"] = maybe_text(normalized.get("claim_scope_id"))
    normalized["assessment_id"] = maybe_text(normalized.get("assessment_id"))
    normalized["route_id"] = maybe_text(normalized.get("route_id"))
    normalized["recommended_lane"] = recommended_lane
    normalized["route_status"] = route_status
    normalized["controversy_posture"] = controversy_posture
    normalized["member_count"] = member_count
    normalized["aggregate_source_signal_count"] = aggregate_source_signal_count
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else controversy_map_confidence(
            member_count=member_count,
            aggregate_source_signal_count=aggregate_source_signal_count,
            route_status=route_status,
            controversy_posture=controversy_posture,
        )
    )
    normalized["controversy_summary"] = maybe_text(
        normalized.get("controversy_summary")
    ) or (
        f"Issue {normalized['issue_label']} is currently routed as "
        f"{route_status.replace('-', ' ')} via {recommended_lane.replace('-', ' ')}."
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=20,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        cluster_id,
        normalized["claim_scope_id"],
        normalized["assessment_id"],
        normalized["route_id"],
        claim_ids,
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Merged claim clusters, scope, verifiability, and routing into one "
        f"controversy-map issue object for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "claim_scope_id": normalized["claim_scope_id"],
            "assessment_id": normalized["assessment_id"],
            "route_id": normalized["route_id"],
        },
    )
    return validate_canonical_payload(OBJECT_KIND_CONTROVERSY_MAP, normalized)


__all__ = (
    "normalize_issue_cluster_payload",
    "normalize_stance_group_payload",
    "normalize_concern_facet_payload",
    "normalize_actor_profile_payload",
    "normalize_evidence_citation_type_payload",
    "normalize_controversy_map_payload",
)
