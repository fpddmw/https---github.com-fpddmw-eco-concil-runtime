from __future__ import annotations

from typing import Any


OBJECT_KIND_ACTOR_PROFILE = "actor-profile"
OBJECT_KIND_CLAIM_CANDIDATE = "claim-candidate"
OBJECT_KIND_CLAIM_CLUSTER = "claim-cluster"
OBJECT_KIND_CLAIM_SCOPE = "claim-scope"
OBJECT_KIND_CONCERN_FACET = "concern-facet"
OBJECT_KIND_CONTROVERSY_MAP = "controversy-map"
OBJECT_KIND_DIFFUSION_EDGE = "diffusion-edge"
OBJECT_KIND_EVIDENCE_CITATION_TYPE = "evidence-citation-type"
OBJECT_KIND_FORMAL_PUBLIC_LINK = "formal-public-link"
OBJECT_KIND_ISSUE_CLUSTER = "issue-cluster"
OBJECT_KIND_REPRESENTATION_GAP = "representation-gap"
OBJECT_KIND_SPATIOTEMPORAL_RELATION_CUE = "spatiotemporal-relation-cue"
OBJECT_KIND_STANCE_GROUP = "stance-group"
OBJECT_KIND_VERIFIABILITY_ASSESSMENT = "verifiability-assessment"
OBJECT_KIND_VERIFICATION_ROUTE = "verification-route"

HEURISTIC_DECISION_SOURCE = "heuristic-fallback"
HELPER_DECISION_SOURCE_APPROVED_VIEW = "approved-helper-view"
HELPER_DECISION_SOURCE_MANUAL_OR_MODERATOR_DEFINED = "manual-or-moderator-defined"
HELPER_DECISION_SOURCE_AGENT_SUBMITTED_FINDING = "agent-submitted-finding"
HELPER_DECISION_SOURCE_SCENARIO = "scenario"
ALLOWED_HELPER_DECISION_SOURCES = {
    HELPER_DECISION_SOURCE_APPROVED_VIEW,
    HELPER_DECISION_SOURCE_MANUAL_OR_MODERATOR_DEFINED,
    HELPER_DECISION_SOURCE_AGENT_SUBMITTED_FINDING,
    HELPER_DECISION_SOURCE_SCENARIO,
}
LEGACY_PUBLIC_REFS_FIELD = "public_refs"


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


def canonical_evidence_refs(
    payload: dict[str, Any],
    *,
    limit: int = 0,
) -> tuple[list[dict[str, str]], str]:
    evidence_refs = unique_artifact_refs(
        list_items(payload.get("evidence_refs")),
        limit=limit,
    )
    if evidence_refs:
        return evidence_refs, "evidence_refs"
    legacy_public_refs = unique_artifact_refs(
        list_items(payload.get(LEGACY_PUBLIC_REFS_FIELD)),
        limit=limit,
    )
    if legacy_public_refs:
        return legacy_public_refs, "legacy-public-refs"
    return [], "missing-evidence-refs"


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


def helper_governance_metadata(
    *,
    skill_name: str,
    rule_id: str,
    destination: str,
    decision_source: str = HELPER_DECISION_SOURCE_APPROVED_VIEW,
    rule_version: str = "optional-analysis-freeze-line-2026-04-28",
    taxonomy_version: str = "",
    rubric_version: str = "",
    approval_ref: str = "",
    audit_ref: str = "",
    rule_trace: list[Any] | None = None,
    caveats: list[Any] | None = None,
    audit_status: str = "default-frozen; approval-required; audit-pending",
    helper_status: str = "approval-gated-helper-view",
) -> dict[str, Any]:
    normalized_decision_source = maybe_text(decision_source)
    if normalized_decision_source not in ALLOWED_HELPER_DECISION_SOURCES:
        raise ValueError(
            f"Unsupported optional helper decision_source: {normalized_decision_source}"
        )
    return {
        "decision_source": normalized_decision_source,
        "rule_id": maybe_text(rule_id),
        "rule_version": maybe_text(rule_version),
        "taxonomy_version": maybe_text(taxonomy_version),
        "rubric_version": maybe_text(rubric_version),
        "approval_ref": maybe_text(approval_ref),
        "audit_ref": maybe_text(audit_ref),
        "rule_trace": unique_texts(list(rule_trace or [])),
        "caveats": unique_texts(list(caveats or [])),
        "audit_status": maybe_text(audit_status),
        "helper_status": maybe_text(helper_status),
        "skill": maybe_text(skill_name),
        "helper_destination": maybe_text(destination),
    }


def build_heuristic_wrapper_provenance(
    *,
    skill_name: str,
    output_path: str,
    method: str,
    selection_mode: str,
    canonical_object_kind: str,
    parent_object_kind: str = "",
    parent_artifact_path: str = "",
    parent_source: str = "",
    extra: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    decision_source = HEURISTIC_DECISION_SOURCE
    return decision_source, normalized_provenance(
        {},
        source_skill=skill_name,
        decision_source=decision_source,
        artifact_path=output_path,
        extra={
            "method": maybe_text(method),
            "selection_mode": maybe_text(selection_mode),
            "canonical_object_kind": maybe_text(canonical_object_kind),
            "authoritative_surface": "analysis-plane",
            "parent_object_kind": maybe_text(parent_object_kind),
            "parent_artifact_path": maybe_text(parent_artifact_path),
            "parent_source": maybe_text(parent_source),
            **(extra if isinstance(extra, dict) else {}),
        },
    )


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


def stance_group_confidence(
    *,
    member_count: int,
    share_ratio: float,
) -> float:
    score = 0.42
    score += min(max(member_count - 1, 0), 6) * 0.04
    score += max(0.0, min(1.0, share_ratio)) * 0.28
    return round(min(score, 0.92), 3)


def concern_facet_confidence(
    *,
    affected_claim_count: int,
    source_signal_count: int,
    priority: str,
) -> float:
    score = 0.41
    score += min(max(affected_claim_count - 1, 0), 6) * 0.03
    score += min(source_signal_count, 6) * 0.025
    if priority == "primary":
        score += 0.08
    return round(min(score, 0.9), 3)


def actor_profile_confidence(
    *,
    claim_count: int,
    source_signal_count: int,
) -> float:
    score = 0.43
    score += min(max(claim_count - 1, 0), 6) * 0.04
    score += min(source_signal_count, 6) * 0.025
    return round(min(score, 0.91), 3)


def citation_type_confidence(
    *,
    claim_count: int,
    source_signal_count: int,
) -> float:
    score = 0.44
    score += min(max(claim_count - 1, 0), 6) * 0.035
    score += min(source_signal_count, 6) * 0.025
    return round(min(score, 0.9), 3)


__all__ = (
    "OBJECT_KIND_ACTOR_PROFILE",
    "OBJECT_KIND_CLAIM_CANDIDATE",
    "OBJECT_KIND_CLAIM_CLUSTER",
    "OBJECT_KIND_CLAIM_SCOPE",
    "OBJECT_KIND_CONCERN_FACET",
    "OBJECT_KIND_CONTROVERSY_MAP",
    "OBJECT_KIND_DIFFUSION_EDGE",
    "OBJECT_KIND_EVIDENCE_CITATION_TYPE",
    "OBJECT_KIND_FORMAL_PUBLIC_LINK",
    "OBJECT_KIND_ISSUE_CLUSTER",
    "OBJECT_KIND_REPRESENTATION_GAP",
    "OBJECT_KIND_SPATIOTEMPORAL_RELATION_CUE",
    "OBJECT_KIND_STANCE_GROUP",
    "OBJECT_KIND_VERIFIABILITY_ASSESSMENT",
    "OBJECT_KIND_VERIFICATION_ROUTE",
    "HEURISTIC_DECISION_SOURCE",
    "HELPER_DECISION_SOURCE_APPROVED_VIEW",
    "HELPER_DECISION_SOURCE_MANUAL_OR_MODERATOR_DEFINED",
    "HELPER_DECISION_SOURCE_AGENT_SUBMITTED_FINDING",
    "HELPER_DECISION_SOURCE_SCENARIO",
    "ALLOWED_HELPER_DECISION_SOURCES",
    "LEGACY_PUBLIC_REFS_FIELD",
    "maybe_text",
    "maybe_number",
    "list_items",
    "dict_items",
    "unique_texts",
    "parse_artifact_ref_text",
    "normalized_artifact_ref",
    "unique_artifact_refs",
    "canonical_evidence_refs",
    "normalized_provenance",
    "helper_governance_metadata",
    "build_heuristic_wrapper_provenance",
    "merged_lineage",
    "claim_candidate_confidence",
    "claim_cluster_confidence",
    "formal_public_link_alignment_score",
    "representation_gap_severity_score",
    "severity_from_score",
    "diffusion_edge_confidence",
    "controversy_posture_from_route",
    "controversy_map_confidence",
    "stance_group_confidence",
    "concern_facet_confidence",
    "actor_profile_confidence",
    "citation_type_confidence",
)
