from __future__ import annotations

from typing import Any

from eco_council_runtime.contracts import (
    ENVIRONMENT_SIGNAL_TAXONOMY_APPROVAL_REF,
    ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
    SIGNAL_ROLE_VALUES,
    SPATIOTEMPORAL_RELATION_STATUS_VALUES,
    SPATIOTEMPORAL_RELATION_TYPE_VALUES,
    canonical_contract,
    validate_canonical_payload,
)
from .common import (
    HELPER_DECISION_SOURCE_APPROVED_VIEW,
    OBJECT_KIND_DIFFUSION_EDGE,
    OBJECT_KIND_FORMAL_PUBLIC_LINK,
    OBJECT_KIND_REPRESENTATION_GAP,
    OBJECT_KIND_SPATIOTEMPORAL_RELATION_CUE,
    dict_items,
    diffusion_edge_confidence,
    formal_public_link_alignment_score,
    helper_governance_metadata,
    list_items,
    maybe_number,
    maybe_text,
    merged_lineage,
    normalized_provenance,
    representation_gap_severity_score,
    severity_from_score,
    unique_artifact_refs,
    unique_texts,
)


def normalize_spatiotemporal_relation_cue_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source"))
        or HELPER_DECISION_SOURCE_APPROVED_VIEW
    )
    relation_type = maybe_text(normalized.get("relation_type")) or "insufficient-basis"
    relation_status = (
        maybe_text(normalized.get("relation_status")) or "insufficient-basis"
    )
    if relation_type not in SPATIOTEMPORAL_RELATION_TYPE_VALUES:
        raise ValueError(f"Unsupported spatiotemporal relation_type: {relation_type}")
    if relation_status not in SPATIOTEMPORAL_RELATION_STATUS_VALUES:
        raise ValueError(
            f"Unsupported spatiotemporal relation_status: {relation_status}"
        )

    source_role = (
        maybe_text(normalized.get("source_role"))
        or "unknown-environment-signal-role"
    )
    target_role = (
        maybe_text(normalized.get("target_role"))
        or "unknown-environment-signal-role"
    )
    if source_role not in SIGNAL_ROLE_VALUES:
        raise ValueError(f"Unsupported source_role: {source_role}")
    if target_role not in SIGNAL_ROLE_VALUES:
        raise ValueError(f"Unsupported target_role: {target_role}")

    source_signal_id = maybe_text(normalized.get("source_signal_id"))
    target_signal_id = maybe_text(normalized.get("target_signal_id"))
    context_signal_ids = unique_texts(list_items(normalized.get("context_signal_ids")))
    evidence_refs = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=20,
    )
    helper_governance = dict_items(normalized.get("helper_governance"))
    if not helper_governance:
        helper_governance = helper_governance_metadata(
            skill_name=source_skill or "detect-temporal-cooccurrence-cues",
            rule_id="HEUR-SPATIOTEMPORAL-RELATION-001",
            destination="spatiotemporal-relation-cue",
            taxonomy_version=ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
            approval_ref=ENVIRONMENT_SIGNAL_TAXONOMY_APPROVAL_REF,
            rule_trace=["spatiotemporal-relation-taxonomy"],
            caveats=[
                "Relation cues are candidate evidence organization objects only.",
                "Relation cues do not prove causality, transport, source attribution, or exclusion of alternatives.",
            ],
        )

    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_SPATIOTEMPORAL_RELATION_CUE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["relation_id"] = maybe_text(normalized.get("relation_id")) or (
        "strel-"
        + maybe_text(source_signal_id or "missing-source")
        + "-"
        + maybe_text(target_signal_id or "missing-target")
    )
    normalized["relation_type"] = relation_type
    normalized["relation_status"] = relation_status
    normalized["source_signal_id"] = source_signal_id
    normalized["target_signal_id"] = target_signal_id
    normalized["context_signal_ids"] = context_signal_ids
    normalized["source_role"] = source_role
    normalized["target_role"] = target_role
    normalized["temporal_rule"] = dict_items(normalized.get("temporal_rule"))
    normalized["spatial_rule"] = dict_items(normalized.get("spatial_rule"))
    normalized["lag_window"] = dict_items(normalized.get("lag_window"))
    normalized["time_delta"] = dict_items(normalized.get("time_delta"))
    normalized["distance"] = dict_items(normalized.get("distance"))
    normalized["spatial_basis"] = dict_items(normalized.get("spatial_basis"))
    normalized["temporal_basis"] = dict_items(normalized.get("temporal_basis"))
    normalized["rejection_reasons"] = unique_texts(
        list_items(normalized.get("rejection_reasons"))
    )
    normalized["caveats"] = unique_texts(list_items(normalized.get("caveats")))
    normalized["evidence_refs"] = evidence_refs
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        source_signal_id,
        target_signal_id,
        context_signal_ids,
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={"canonical_object_kind": OBJECT_KIND_SPATIOTEMPORAL_RELATION_CUE},
    )
    normalized["helper_governance"] = helper_governance
    return validate_canonical_payload(
        OBJECT_KIND_SPATIOTEMPORAL_RELATION_CUE,
        normalized,
    )


def normalize_formal_public_link_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    cluster_ids = unique_texts(list_items(normalized.get("cluster_ids")))
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    claim_scope_ids = unique_texts(list_items(normalized.get("claim_scope_ids")))
    assessment_ids = unique_texts(list_items(normalized.get("assessment_ids")))
    route_ids = unique_texts(list_items(normalized.get("route_ids")))
    formal_signal_ids = unique_texts(list_items(normalized.get("formal_signal_ids")))
    public_signal_ids = unique_texts(list_items(normalized.get("public_signal_ids")))
    formal_signal_count = int(
        maybe_number(normalized.get("formal_signal_count")) or len(formal_signal_ids)
    )
    public_signal_count = int(
        maybe_number(normalized.get("public_signal_count")) or len(public_signal_ids)
    )
    alignment_score = maybe_number(normalized.get("alignment_score"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_FORMAL_PUBLIC_LINK
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["issue_terms"] = unique_texts(list_items(normalized.get("issue_terms")))
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["cluster_ids"] = cluster_ids
    normalized["claim_ids"] = claim_ids
    normalized["claim_scope_ids"] = claim_scope_ids
    normalized["assessment_ids"] = assessment_ids
    normalized["route_ids"] = route_ids
    normalized["formal_signal_ids"] = formal_signal_ids
    normalized["public_signal_ids"] = public_signal_ids
    normalized["formal_signal_count"] = formal_signal_count
    normalized["public_signal_count"] = public_signal_count
    normalized["formal_source_skills"] = unique_texts(
        list_items(normalized.get("formal_source_skills"))
    )
    normalized["public_source_skills"] = unique_texts(
        list_items(normalized.get("public_source_skills"))
    )
    normalized["formal_examples"] = unique_texts(
        list_items(normalized.get("formal_examples"))
    )
    normalized["public_examples"] = unique_texts(
        list_items(normalized.get("public_examples"))
    )
    normalized["link_status"] = maybe_text(normalized.get("link_status")) or "unlinked"
    normalized["recommended_lane"] = (
        maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    )
    normalized["route_status"] = (
        maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    )
    normalized["alignment_score"] = (
        alignment_score
        if alignment_score is not None
        else formal_public_link_alignment_score(
            formal_signal_count=formal_signal_count,
            public_signal_count=public_signal_count,
            claim_count=len(claim_ids),
        )
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=20,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        cluster_ids,
        claim_ids,
        claim_scope_ids,
        assessment_ids,
        route_ids,
        formal_signal_ids,
        public_signal_ids,
    )
    normalized["linkage_summary"] = maybe_text(normalized.get("linkage_summary")) or (
        f"Issue {normalized['issue_label']} is marked as "
        f"{normalized['link_status']} using {formal_signal_count} formal and "
        f"{public_signal_count} public signals."
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Collapsed formal-comment and public-discourse evidence into one "
        f"issue-level linkage object for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "source_plane": "formal-public",
        },
    )
    return validate_canonical_payload(OBJECT_KIND_FORMAL_PUBLIC_LINK, normalized)


def normalize_representation_gap_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    cluster_ids = unique_texts(list_items(normalized.get("cluster_ids")))
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    claim_scope_ids = unique_texts(list_items(normalized.get("claim_scope_ids")))
    assessment_ids = unique_texts(list_items(normalized.get("assessment_ids")))
    route_ids = unique_texts(list_items(normalized.get("route_ids")))
    formal_signal_count = int(maybe_number(normalized.get("formal_signal_count")) or 0)
    public_signal_count = int(maybe_number(normalized.get("public_signal_count")) or 0)
    severity_text = maybe_text(normalized.get("severity"))
    severity_score = maybe_number(normalized.get("severity_score"))
    if severity_score is None:
        severity_score = representation_gap_severity_score(
            link_status=maybe_text(normalized.get("link_status")),
            severity=severity_text,
            formal_signal_count=formal_signal_count,
            public_signal_count=public_signal_count,
        )
    if not severity_text:
        severity_text = severity_from_score(severity_score)
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_REPRESENTATION_GAP
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["linkage_id"] = maybe_text(normalized.get("linkage_id"))
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["gap_type"] = maybe_text(normalized.get("gap_type")) or "route-mismatch"
    normalized["severity"] = severity_text
    normalized["severity_score"] = severity_score
    normalized["link_status"] = maybe_text(normalized.get("link_status")) or "unlinked"
    normalized["recommended_lane"] = (
        maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    )
    normalized["route_status"] = (
        maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    )
    normalized["formal_signal_count"] = formal_signal_count
    normalized["public_signal_count"] = public_signal_count
    normalized["cluster_ids"] = cluster_ids
    normalized["claim_ids"] = claim_ids
    normalized["claim_scope_ids"] = claim_scope_ids
    normalized["assessment_ids"] = assessment_ids
    normalized["route_ids"] = route_ids
    normalized["recommended_action"] = maybe_text(
        normalized.get("recommended_action")
    ) or (
        f"Review whether {normalized['issue_label']} needs routing changes to close the "
        f"{normalized['gap_type']} gap."
    )
    normalized["gap_summary"] = maybe_text(normalized.get("gap_summary")) or (
        f"Issue {normalized['issue_label']} remains exposed to a "
        f"{normalized['gap_type']} gap under the {normalized['recommended_lane']} lane."
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        normalized["linkage_id"],
        cluster_ids,
        claim_ids,
        claim_scope_ids,
        assessment_ids,
        route_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Materialized a representation-gap object from issue-level "
        f"formal/public linkage evidence for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "gap_type": normalized["gap_type"],
        },
    )
    return validate_canonical_payload(OBJECT_KIND_REPRESENTATION_GAP, normalized)


def normalize_diffusion_edge_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "runtime-fallback"
    )
    linkage_ids = unique_texts(list_items(normalized.get("linkage_ids")))
    cluster_ids = unique_texts(list_items(normalized.get("cluster_ids")))
    claim_ids = unique_texts(list_items(normalized.get("claim_ids")))
    claim_scope_ids = unique_texts(list_items(normalized.get("claim_scope_ids")))
    assessment_ids = unique_texts(list_items(normalized.get("assessment_ids")))
    route_ids = unique_texts(list_items(normalized.get("route_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    target_signal_ids = unique_texts(list_items(normalized.get("target_signal_ids")))
    source_signal_count = int(
        maybe_number(normalized.get("source_signal_count")) or len(source_signal_ids)
    )
    target_signal_count = int(
        maybe_number(normalized.get("target_signal_count")) or len(target_signal_ids)
    )
    time_delta_hours = maybe_number(normalized.get("time_delta_hours")) or 0.0
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_DIFFUSION_EDGE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label")) or "general-public-controversy"
    )
    normalized["linkage_ids"] = linkage_ids
    normalized["cluster_ids"] = cluster_ids
    normalized["claim_ids"] = claim_ids
    normalized["claim_scope_ids"] = claim_scope_ids
    normalized["assessment_ids"] = assessment_ids
    normalized["route_ids"] = route_ids
    normalized["source_platform"] = maybe_text(normalized.get("source_platform"))
    normalized["target_platform"] = maybe_text(normalized.get("target_platform"))
    normalized["source_plane"] = maybe_text(normalized.get("source_plane"))
    normalized["target_plane"] = maybe_text(normalized.get("target_plane"))
    normalized["source_signal_ids"] = source_signal_ids
    normalized["target_signal_ids"] = target_signal_ids
    normalized["source_signal_count"] = source_signal_count
    normalized["target_signal_count"] = target_signal_count
    normalized["source_source_skills"] = unique_texts(
        list_items(normalized.get("source_source_skills"))
    )
    normalized["target_source_skills"] = unique_texts(
        list_items(normalized.get("target_source_skills"))
    )
    normalized["source_examples"] = unique_texts(
        list_items(normalized.get("source_examples"))
    )
    normalized["target_examples"] = unique_texts(
        list_items(normalized.get("target_examples"))
    )
    normalized["edge_type"] = (
        maybe_text(normalized.get("edge_type")) or "cross-public-diffusion"
    )
    normalized["temporal_relation"] = (
        maybe_text(normalized.get("temporal_relation")) or "same-window"
    )
    normalized["time_delta_hours"] = time_delta_hours
    normalized["source_first_seen_utc"] = maybe_text(
        normalized.get("source_first_seen_utc")
    )
    normalized["target_first_seen_utc"] = maybe_text(
        normalized.get("target_first_seen_utc")
    )
    normalized["recommended_lane"] = (
        maybe_text(normalized.get("recommended_lane")) or "mixed-review"
    )
    normalized["route_status"] = (
        maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    )
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else diffusion_edge_confidence(
            source_signal_count=source_signal_count,
            target_signal_count=target_signal_count,
            time_delta_hours=time_delta_hours,
            claim_count=len(claim_ids),
            cluster_count=len(cluster_ids),
        )
    )
    normalized["edge_summary"] = maybe_text(normalized.get("edge_summary")) or (
        f"Issue {normalized['issue_label']} shows {normalized['edge_type']} "
        f"between {normalized['source_platform']} and {normalized['target_platform']}."
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=20,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        linkage_ids,
        cluster_ids,
        claim_ids,
        claim_scope_ids,
        assessment_ids,
        route_ids,
        source_signal_ids,
        target_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Collapsed ordered platform observations into a diffusion-edge object "
        f"for {normalized['issue_label']}."
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "selection_mode": maybe_text(normalized.get("selection_mode")),
            "source_plane": normalized["source_plane"],
            "target_plane": normalized["target_plane"],
        },
    )
    return validate_canonical_payload(OBJECT_KIND_DIFFUSION_EDGE, normalized)


__all__ = (
    "normalize_spatiotemporal_relation_cue_payload",
    "normalize_formal_public_link_payload",
    "normalize_representation_gap_payload",
    "normalize_diffusion_edge_payload",
)
