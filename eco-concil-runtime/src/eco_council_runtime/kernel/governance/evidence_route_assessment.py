from __future__ import annotations

from typing import Any


ROUTE_ASSESSMENT_TERMINAL_STATUSES = {
    "accepted",
    "closed",
    "resolved",
    "retired",
    "superseded",
}
ROUTE_ASSESSMENT_ATTENTION_VALUES = {
    "capability-gap",
    "external-capability-gap",
    "insufficient-current-surface",
    "needs-followup-skill",
    "no-actionable-current-route",
    "route-discovery-needed",
    "same-family-followup-needed",
    "source-surface-mismatch",
    "wrong-source-family",
}

COMPACT_TEXT_FIELDS = (
    "assessment_type",
    "evidence_need_summary",
    "current_surface_summary",
    "route_judgment",
    "recommended_next_step",
    "source_surface_status",
    "continuation_mode",
    "capability_gap_kind",
    "moderator_response_required",
)
COMPACT_LIST_FIELDS = (
    "target_evidence_request_ids",
    "considered_source_families",
    "considered_source_skills",
    "same_family_followups_considered",
    "alternate_routes",
    "capability_gap_refs",
    "rejected_route_refs",
    "route_assessment_refs",
)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = maybe_text(value).lower()
    return text in {"1", "true", "yes", "y", "required"}


def route_assessment_needs_moderator_response(
    payload: dict[str, Any],
    *,
    terminal_statuses: set[str],
) -> bool:
    status = maybe_text(payload.get("status"))
    if status in terminal_statuses or status in ROUTE_ASSESSMENT_TERMINAL_STATUSES:
        return False
    if truthy_flag(payload.get("moderator_response_required")):
        return True
    for field_name in (
        "assessment_type",
        "route_judgment",
        "source_surface_status",
        "recommended_next_step",
        "continuation_mode",
        "capability_gap_kind",
    ):
        text = maybe_text(payload.get(field_name)).lower()
        if text in ROUTE_ASSESSMENT_ATTENTION_VALUES:
            return True
        if any(marker in text for marker in ROUTE_ASSESSMENT_ATTENTION_VALUES):
            return True
    return not status or status not in terminal_statuses


def route_assessment_closing_item(route_assessment_refs: list[str]) -> dict[str, Any]:
    return {
        "item_id": "respond-to-evidence-route-assessments",
        "state": "response-required" if route_assessment_refs else "observed-clear",
        "route_assessment_refs": route_assessment_refs[:20],
        "moderator_required_action": (
            "Acknowledge the route assessment before repeating the same "
            "evidence request. Choose route-discovery continuation, "
            "capability-gap/human pause, bounded report with limitation, "
            "request revision, or explicit disagreement. If the assessment "
            "names a same-family follow-up skill or discovery mode, do not "
            "open another generic continuation without recording why that "
            "discovery will be executed, deferred, or rejected."
            if route_assessment_refs
            else ""
        ),
        "response_boundary": (
            "This does not rank sources or force a route; it prevents "
            "unresolved refs from producing repeated non-move rounds when "
            "investigators have already recorded a source-surface mismatch "
            "or a missing route-grounding step."
            if route_assessment_refs
            else ""
        ),
        "response_surfaces": [
            "submit-round-synthesis",
            "submit-evidence-request",
            "submit-investigation-scope",
            "request-phase-transition",
        ],
    }


__all__ = [
    "COMPACT_LIST_FIELDS",
    "COMPACT_TEXT_FIELDS",
    "route_assessment_closing_item",
    "route_assessment_needs_moderator_response",
]
