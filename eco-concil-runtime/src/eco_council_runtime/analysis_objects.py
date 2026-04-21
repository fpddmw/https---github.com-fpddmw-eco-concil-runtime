from __future__ import annotations

from typing import Any

from .canonical_contracts import canonical_contract, validate_canonical_payload


OBJECT_KIND_CLAIM_CANDIDATE = "claim-candidate"
OBJECT_KIND_CLAIM_CLUSTER = "claim-cluster"
OBJECT_KIND_CLAIM_SCOPE = "claim-scope"
OBJECT_KIND_VERIFIABILITY_ASSESSMENT = "verifiability-assessment"
OBJECT_KIND_VERIFICATION_ROUTE = "verification-route"


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def maybe_number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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


def parse_artifact_ref_text(value: Any) -> tuple[str, str]:
    text = maybe_text(value)
    if not text:
        return "", ""
    marker = text.find(":$")
    if marker >= 0:
        return text[:marker], text[marker + 1 :]
    return text, ""


def normalized_artifact_ref(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        artifact_path = maybe_text(value.get("artifact_path"))
        record_locator = maybe_text(value.get("record_locator"))
        artifact_ref = maybe_text(value.get("artifact_ref"))
        if not artifact_path and artifact_ref:
            artifact_path, parsed_locator = parse_artifact_ref_text(artifact_ref)
            if not record_locator:
                record_locator = parsed_locator
        if artifact_path and not artifact_ref:
            artifact_ref = (
                artifact_path
                if not record_locator
                else f"{artifact_path}:{record_locator}"
            )
        if not artifact_path:
            return {}
        return {
            "signal_id": maybe_text(value.get("signal_id")),
            "artifact_path": artifact_path,
            "record_locator": record_locator,
            "artifact_ref": artifact_ref or artifact_path,
        }
    artifact_path, record_locator = parse_artifact_ref_text(value)
    if not artifact_path:
        return {}
    artifact_ref = (
        artifact_path if not record_locator else f"{artifact_path}:{record_locator}"
    )
    return {
        "signal_id": "",
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": artifact_ref,
    }


def unique_artifact_refs(values: list[Any], *, limit: int = 0) -> list[dict[str, str]]:
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    for value in values:
        ref = normalized_artifact_ref(value)
        artifact_ref = maybe_text(ref.get("artifact_ref"))
        if not artifact_ref or artifact_ref in seen:
            continue
        seen.add(artifact_ref)
        results.append(ref)
        if limit > 0 and len(results) >= limit:
            break
    return results


def normalized_provenance(
    value: Any,
    *,
    source_skill: str = "",
    decision_source: str = "",
    artifact_path: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict_items(value)
    if source_skill and "source_skill" not in normalized:
        normalized["source_skill"] = source_skill
    if decision_source and "decision_source" not in normalized:
        normalized["decision_source"] = decision_source
    if artifact_path and "artifact_path" not in normalized:
        normalized["artifact_path"] = artifact_path
    if isinstance(extra, dict):
        for key, raw_value in extra.items():
            key_text = maybe_text(key)
            if (
                not key_text
                or key_text in normalized
                or raw_value in (None, "", [], {})
            ):
                continue
            normalized[key_text] = raw_value
    return normalized


def merged_lineage(existing: Any, *sources: Any) -> list[str]:
    values = list_items(existing)
    for source in sources:
        if isinstance(source, list):
            values.extend(source)
            continue
        values.append(source)
    return unique_texts(values)


def claim_candidate_confidence(
    *,
    source_signal_count: int,
    evidence_citation_types: list[str],
    verifiability_hint: str,
) -> float:
    confidence = 0.5
    confidence += min(max(source_signal_count - 1, 0), 4) * 0.06
    if "official-document" in evidence_citation_types:
        confidence += 0.08
    if "scientific-study" in evidence_citation_types:
        confidence += 0.06
    if verifiability_hint == "empirical-observable":
        confidence += 0.04
    return round(min(confidence, 0.88), 3)


def claim_cluster_confidence(
    *,
    member_count: int,
    unique_source_signal_count: int,
    verifiability_posture: str,
) -> float:
    confidence = 0.54
    confidence += min(max(member_count - 1, 0), 5) * 0.05
    confidence += min(unique_source_signal_count, 6) * 0.02
    if verifiability_posture == "empirical-observable":
        confidence += 0.04
    return round(min(confidence, 0.91), 3)


def normalize_claim_candidate_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    evidence_citation_types = unique_texts(
        list_items(normalized.get("evidence_citation_types"))
    )
    evidence_refs = unique_artifact_refs(
        list_items(normalized.get("evidence_refs"))
        or list_items(normalized.get("public_refs")),
        limit=12,
    )
    confidence = maybe_number(normalized.get("confidence"))
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_CLAIM_CANDIDATE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "sociologist"
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
    normalized["public_refs"] = unique_artifact_refs(
        list_items(normalized.get("public_refs")) or evidence_refs,
        limit=12,
    )
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
    decision_source = maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
    member_claim_ids = unique_texts(list_items(normalized.get("member_claim_ids")))
    source_signal_ids = unique_texts(list_items(normalized.get("source_signal_ids")))
    evidence_refs = unique_artifact_refs(
        list_items(normalized.get("evidence_refs"))
        or list_items(normalized.get("public_refs")),
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
    normalized["public_refs"] = unique_artifact_refs(
        list_items(normalized.get("public_refs")) or evidence_refs,
        limit=16,
    )
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
    decision_source = maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
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
    normalized["method"] = maybe_text(normalized.get("method")) or "heuristic-scope"
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


def normalize_verifiability_assessment_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_VERIFIABILITY_ASSESSMENT
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["recommended_lane"] = (
        maybe_text(normalized.get("recommended_lane")) or "route-before-matching"
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        maybe_text(normalized.get("claim_id")),
        maybe_text(normalized.get("claim_scope_id")),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "assessment_summary": maybe_text(normalized.get("assessment_summary")),
        },
    )
    return validate_canonical_payload(OBJECT_KIND_VERIFIABILITY_ASSESSMENT, normalized)


def normalize_verification_route_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_VERIFICATION_ROUTE
    ).schema_version
    normalized["decision_source"] = decision_source
    normalized["recommended_lane"] = (
        maybe_text(normalized.get("recommended_lane")) or "route-before-matching"
    )
    normalized["route_status"] = (
        maybe_text(normalized.get("route_status")) or "mixed-routing-review"
    )
    normalized["evidence_refs"] = unique_artifact_refs(
        list_items(normalized.get("evidence_refs")),
        limit=16,
    )
    normalized["lineage"] = merged_lineage(
        normalized.get("lineage"),
        maybe_text(normalized.get("claim_id")),
        maybe_text(normalized.get("assessment_id")),
        maybe_text(normalized.get("claim_scope_id")),
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        source_skill=source_skill,
        decision_source=decision_source,
        artifact_path=artifact_path,
        extra={
            "method": maybe_text(normalized.get("method")),
            "route_reason": maybe_text(normalized.get("route_reason")),
        },
    )
    return validate_canonical_payload(OBJECT_KIND_VERIFICATION_ROUTE, normalized)


__all__ = [
    "OBJECT_KIND_CLAIM_CANDIDATE",
    "OBJECT_KIND_CLAIM_CLUSTER",
    "OBJECT_KIND_CLAIM_SCOPE",
    "OBJECT_KIND_VERIFIABILITY_ASSESSMENT",
    "OBJECT_KIND_VERIFICATION_ROUTE",
    "maybe_number",
    "maybe_text",
    "merged_lineage",
    "normalize_claim_candidate_payload",
    "normalize_claim_cluster_payload",
    "normalize_claim_scope_payload",
    "normalize_verifiability_assessment_payload",
    "normalize_verification_route_payload",
    "unique_artifact_refs",
    "unique_texts",
]
