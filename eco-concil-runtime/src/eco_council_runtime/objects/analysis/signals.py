from __future__ import annotations

from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from .common import (
    RUNTIME_FALLBACK_DECISION_SOURCE,
    LEGACY_PUBLIC_REFS_FIELD,
    OBJECT_KIND_CLAIM_CANDIDATE,
    OBJECT_KIND_CLAIM_CLUSTER,
    OBJECT_KIND_CLAIM_SCOPE,
    canonical_evidence_refs,
    claim_candidate_confidence,
    claim_cluster_confidence,
    dict_items,
    list_items,
    maybe_number,
    maybe_text,
    merged_lineage,
    normalized_provenance,
    unique_artifact_refs,
    unique_texts,
)


def normalize_claim_candidate_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or RUNTIME_FALLBACK_DECISION_SOURCE
    )
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    evidence_citation_types = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    evidence_refs, evidence_ref_source = canonical_evidence_refs(
        normalized,
        limit=12,
    )
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_CLAIM_CANDIDATE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "social-investigator"
    normalized["status"] = maybe_text(normalized.get("status")) or "candidate"
    normalized["issue_hint"] = (
        maybe_text(normalized.get("issue_hint"))
        or maybe_text(normalized.get("claim_type"))
        or "general-public-controversy"
    )
    normalized["summary"] = maybe_text(normalized.get("summary"))
    normalized["statement"] = maybe_text(normalized.get("statement"))
    normalized["stance_hint"] = maybe_text(normalized.get("stance_hint")) or "unclear"
    normalized["verifiability_hint"] = (
        maybe_text(normalized.get("verifiability_hint")) or "mixed-public-claim"
    )
    normalized["dispute_type"] = (
        maybe_text(normalized.get("dispute_type")) or "mixed-controversy"
    )
    normalized["issue_terms"] = unique_texts(list_items(normalized.get("issue_terms")))
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["evidence_citation_types"] = evidence_citation_types
    normalized["source_signal_ids"] = source_signal_ids
    normalized["source_signal_count"] = int(
        maybe_number(normalized.get("source_signal_count")) or len(source_signal_ids)
    )
    normalized["evidence_refs"] = evidence_refs
    legacy_public_refs = unique_artifact_refs(
        list_items(normalized.get(LEGACY_PUBLIC_REFS_FIELD)),
        limit=12,
    )
    if legacy_public_refs:
        normalized[LEGACY_PUBLIC_REFS_FIELD] = legacy_public_refs
    else:
        normalized.pop(LEGACY_PUBLIC_REFS_FIELD, None)
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Collapsed repeated public-discourse signals into one board-ready claim "
        f"candidate for {normalized['issue_hint']} with a dominant "
        f"{normalized['stance_hint']} posture."
    )
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else claim_candidate_confidence(
            source_signal_count=normalized["source_signal_count"],
            evidence_citation_types=evidence_citation_types,
            verifiability_hint=normalized["verifiability_hint"],
        )
    )
    normalized["controversy_seed"] = dict_items(normalized.get("controversy_seed"))
    normalized["time_window"] = dict_items(normalized.get("time_window"))
    normalized["place_scope"] = dict_items(normalized.get("place_scope"))
    normalized["claim_scope"] = dict_items(normalized.get("claim_scope"))
    normalized["compact_audit"] = dict_items(normalized.get("compact_audit"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "claim_type": maybe_text(normalized.get("claim_type")),
            "source_plane": "public",
            "evidence_ref_source": evidence_ref_source,
            "selection_mode": maybe_text(
                dict_items(normalized.get("compact_audit")).get("selection_mode")
            ),
        },
    )
    return validate_canonical_payload(OBJECT_KIND_CLAIM_CANDIDATE, normalized)


def normalize_claim_cluster_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or RUNTIME_FALLBACK_DECISION_SOURCE
    )
    member_claim_ids = unique_texts(list_items(normalized.get("member_claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    evidence_refs, evidence_ref_source = canonical_evidence_refs(
        normalized,
        limit=16,
    )
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_CLAIM_CLUSTER
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["status"] = maybe_text(normalized.get("status")) or "cluster-candidate"
    normalized["cluster_label"] = maybe_text(normalized.get("cluster_label"))
    normalized["representative_statement"] = maybe_text(
        normalized.get("representative_statement")
    )
    normalized["semantic_fingerprint"] = (
        maybe_text(normalized.get("semantic_fingerprint")) or "empty"
    )
    normalized["issue_label"] = (
        maybe_text(normalized.get("issue_label"))
        or maybe_text(normalized.get("claim_type"))
        or "general-public-controversy"
    )
    normalized["dominant_stance"] = (
        maybe_text(normalized.get("dominant_stance")) or "unclear"
    )
    normalized["verifiability_posture"] = (
        maybe_text(normalized.get("verifiability_posture")) or "mixed-public-claim"
    )
    normalized["dispute_type"] = (
        maybe_text(normalized.get("dispute_type")) or "mixed-controversy"
    )
    normalized["issue_terms"] = unique_texts(list_items(normalized.get("issue_terms")))
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["evidence_citation_types"] = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    normalized["member_claim_ids"] = member_claim_ids
    normalized["source_signal_ids"] = source_signal_ids
    normalized["member_count"] = int(
        maybe_number(normalized.get("member_count")) or len(member_claim_ids)
    )
    normalized["aggregate_source_signal_count"] = int(
        maybe_number(normalized.get("aggregate_source_signal_count"))
        or len(source_signal_ids)
    )
    normalized["unique_source_signal_count"] = int(
        maybe_number(normalized.get("unique_source_signal_count"))
        or len(source_signal_ids)
    )
    normalized["stance_distribution"] = list_items(
        normalized.get("stance_distribution")
    )
    normalized["member_summaries"] = unique_texts(
        list_items(normalized.get("member_summaries"))
    )
    normalized["evidence_refs"] = evidence_refs
    legacy_public_refs = unique_artifact_refs(
        list_items(normalized.get(LEGACY_PUBLIC_REFS_FIELD)),
        limit=16,
    )
    if legacy_public_refs:
        normalized[LEGACY_PUBLIC_REFS_FIELD] = legacy_public_refs
    else:
        normalized.pop(LEGACY_PUBLIC_REFS_FIELD, None)
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        member_claim_ids,
        source_signal_ids,
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Grouped aligned claim candidates into one board-reviewable cluster "
        f"for {normalized['issue_label']}."
    )
    normalized["confidence"] = (
        confidence
        if confidence is not None
        else claim_cluster_confidence(
            member_count=normalized["member_count"],
            unique_source_signal_count=normalized["unique_source_signal_count"],
            verifiability_posture=normalized["verifiability_posture"],
        )
    )
    normalized["time_window"] = dict_items(normalized.get("time_window"))
    normalized["compact_audit"] = dict_items(normalized.get("compact_audit"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "claim_type": maybe_text(normalized.get("claim_type")),
            "evidence_ref_source": evidence_ref_source,
            "selection_mode": "group-claim-candidates-by-issue-stance-concern",
        },
    )
    return validate_canonical_payload(OBJECT_KIND_CLAIM_CLUSTER, normalized)


def normalize_claim_scope_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or RUNTIME_FALLBACK_DECISION_SOURCE
    )
    claim_id = maybe_text(normalized.get("claim_id"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_CLAIM_SCOPE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["issue_hint"] = (
        maybe_text(normalized.get("issue_hint"))
        or maybe_text(normalized.get("claim_type"))
        or "general-public-controversy"
    )
    normalized["scope_label"] = maybe_text(normalized.get("scope_label"))
    normalized["scope_kind"] = maybe_text(normalized.get("scope_kind")) or "unknown"
    normalized["verifiability_kind"] = (
        maybe_text(normalized.get("verifiability_kind")) or "mixed-public-claim"
    )
    normalized["dispute_type"] = (
        maybe_text(normalized.get("dispute_type")) or "mixed-controversy"
    )
    normalized["required_evidence_lane"] = (
        maybe_text(normalized.get("required_evidence_lane")) or "route-before-matching"
    )
    normalized["matching_eligibility_reason"] = maybe_text(
        normalized.get("matching_eligibility_reason")
    )
    normalized["method"] = maybe_text(normalized.get("method")) or "fallback-scope"
    normalized["matching_tags"] = unique_texts(
        list_items(normalized.get("matching_tags"))
    )
    normalized["issue_terms"] = unique_texts(list_items(normalized.get("issue_terms")))
    normalized["concern_facets"] = unique_texts(
        list_items(normalized.get("concern_facets"))
    )
    normalized["actor_hints"] = unique_texts(list_items(normalized.get("actor_hints")))
    normalized["evidence_citation_types"] = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        claim_id,
        list_items(normalized.get("basis_claim_ids")),
        list_items(normalized.get("source_signal_ids")),
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale")) or (
        "Derived a claim-scope proposal from upstream claim-side evidence and "
        f"routed it toward {normalized['required_evidence_lane']}."
    )
    normalized["confidence"] = maybe_number(normalized.get("confidence")) or 0.5
    normalized["claim_scope"] = dict_items(normalized.get("claim_scope"))
    normalized["place_scope"] = dict_items(normalized.get("place_scope"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "claim_input_kind": maybe_text(normalized.get("claim_input_kind")),
            "claim_object_id": maybe_text(normalized.get("claim_object_id")),
            "method": normalized["method"],
        },
    )
    return validate_canonical_payload(OBJECT_KIND_CLAIM_SCOPE, normalized)


__all__ = (
    "normalize_claim_candidate_payload",
    "normalize_claim_cluster_payload",
    "normalize_claim_scope_payload",
)
