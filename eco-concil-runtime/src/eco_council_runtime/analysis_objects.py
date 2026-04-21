from __future__ import annotations

from typing import Any

from .canonical_contracts import canonical_contract, validate_canonical_payload


OBJECT_KIND_CLAIM_CANDIDATE = "claim-candidate"
OBJECT_KIND_CLAIM_CLUSTER = "claim-cluster"
OBJECT_KIND_CLAIM_SCOPE = "claim-scope"
OBJECT_KIND_CONTROVERSY_MAP = "controversy-map"
OBJECT_KIND_DIFFUSION_EDGE = "diffusion-edge"
OBJECT_KIND_FORMAL_PUBLIC_LINK = "formal-public-link"
OBJECT_KIND_REPRESENTATION_GAP = "representation-gap"
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


def formal_public_link_alignment_score(
    *,
    formal_signal_count: int,
    public_signal_count: int,
    claim_count: int,
) -> float:
    if formal_signal_count > 0 and public_signal_count > 0:
        balance = min(formal_signal_count, public_signal_count) / max(
            formal_signal_count,
            public_signal_count,
        )
        score = 0.55 + 0.35 * balance
        if claim_count > 0:
            score += 0.1
        return round(min(score, 1.0), 3)
    if formal_signal_count > 0 or public_signal_count > 0:
        score = 0.25 + min(max(formal_signal_count, public_signal_count), 3) * 0.04
        if claim_count > 0:
            score += 0.05
        return round(min(score, 0.45), 3)
    if claim_count > 0:
        return 0.15
    return 0.0


def representation_gap_severity_score(
    *,
    link_status: str,
    severity: str,
    formal_signal_count: int,
    public_signal_count: int,
) -> float:
    score = 0.42
    if link_status in {"public-only", "formal-only"}:
        score += 0.22
    elif link_status == "claim-side-only":
        score += 0.18
    score += min(abs(formal_signal_count - public_signal_count), 4) * 0.04
    if severity in {"high", "critical"}:
        score += 0.08
    if severity == "critical":
        score += 0.05
    return round(min(score, 0.95), 3)


def severity_from_score(score: float) -> str:
    if score >= 0.9:
        return "critical"
    if score >= 0.75:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


def diffusion_edge_confidence(
    *,
    source_signal_count: int,
    target_signal_count: int,
    time_delta_hours: float,
    claim_count: int,
    cluster_count: int,
) -> float:
    score = 0.52
    score += min(source_signal_count, 3) * 0.05
    score += min(target_signal_count, 3) * 0.05
    if time_delta_hours <= 48.0:
        score += 0.1
    if time_delta_hours <= 6.0:
        score += 0.05
    if claim_count > 0:
        score += 0.06
    if cluster_count > 0:
        score += 0.04
    return round(min(score, 0.96), 3)


def controversy_posture_from_route(route_status: str, recommended_lane: str) -> str:
    if recommended_lane == "environmental-observation":
        return "empirical-issue"
    if route_status in {
        "route-to-formal-record-review",
        "keep-in-public-discourse-analysis",
        "keep-in-stakeholder-deliberation",
    }:
        return "non-empirical-issue"
    return "mixed-issue"


def controversy_map_confidence(
    *,
    member_count: int,
    aggregate_source_signal_count: int,
    route_status: str,
    controversy_posture: str,
) -> float:
    score = 0.5
    score += min(max(member_count - 1, 0), 5) * 0.04
    score += min(aggregate_source_signal_count, 6) * 0.02
    if route_status == "route-to-verification-lane":
        score += 0.08
    elif route_status in {
        "route-to-formal-record-review",
        "keep-in-public-discourse-analysis",
        "keep-in-stakeholder-deliberation",
    }:
        score += 0.05
    if controversy_posture == "empirical-issue":
        score += 0.04
    return round(min(score, 0.93), 3)


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


def normalize_formal_public_link_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
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
        maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
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
        maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
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


def normalize_controversy_map_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = (
        maybe_text(normalized.get("decision_source")) or "heuristic-fallback"
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


__all__ = [
    "OBJECT_KIND_CLAIM_CANDIDATE",
    "OBJECT_KIND_CLAIM_CLUSTER",
    "OBJECT_KIND_CLAIM_SCOPE",
    "OBJECT_KIND_CONTROVERSY_MAP",
    "OBJECT_KIND_DIFFUSION_EDGE",
    "OBJECT_KIND_FORMAL_PUBLIC_LINK",
    "OBJECT_KIND_REPRESENTATION_GAP",
    "OBJECT_KIND_VERIFIABILITY_ASSESSMENT",
    "OBJECT_KIND_VERIFICATION_ROUTE",
    "maybe_number",
    "maybe_text",
    "merged_lineage",
    "normalize_claim_candidate_payload",
    "normalize_claim_cluster_payload",
    "normalize_claim_scope_payload",
    "normalize_controversy_map_payload",
    "normalize_diffusion_edge_payload",
    "normalize_formal_public_link_payload",
    "normalize_representation_gap_payload",
    "normalize_verifiability_assessment_payload",
    "normalize_verification_route_payload",
    "unique_artifact_refs",
    "unique_texts",
]
