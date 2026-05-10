from __future__ import annotations

from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from .common import (
    OBJECT_KIND_VERIFIABILITY_ASSESSMENT,
    OBJECT_KIND_VERIFICATION_ROUTE,
    list_items,
    maybe_text,
    merged_lineage,
    normalized_provenance,
    unique_artifact_refs,
)


def normalize_verifiability_assessment_payload(
    payload: dict[str, Any],
    *,
    source_skill: str = "",
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(payload)
    decision_source = maybe_text(normalized.get("decision_source")) or "runtime-fallback"
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
    decision_source = maybe_text(normalized.get("decision_source")) or "runtime-fallback"
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


__all__ = (
    "normalize_verifiability_assessment_payload",
    "normalize_verification_route_payload",
)
