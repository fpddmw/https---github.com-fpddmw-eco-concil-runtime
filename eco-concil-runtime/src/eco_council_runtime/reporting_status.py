from __future__ import annotations

from typing import Any

from .phase2_action_semantics import maybe_bool
from .phase2_fallback_common import maybe_text, unique_texts

REPORTING_READY_STATUS = "reporting-ready"
REPORTING_HOLD_STATUS = "investigation-open"
SUPERVISOR_REPORTING_READY_STATUS = "reporting-ready"
SUPERVISOR_HOLD_STATUS = "hold-investigation-open"

LEGACY_REPORTING_READY_STATUS = "ready-for-reporting"
LEGACY_REPORTING_HOLD_STATUS = "pending-more-investigation"
REPORTING_BLOCKER_SUMMARY_MAP = {
    "promotion-withheld": "Promotion is still withheld, so reporting cannot proceed.",
    "readiness-missing": "No readiness assessment is available yet for reporting handoff.",
    "readiness-blocked": "The readiness assessment is explicitly blocked.",
    "readiness-needs-more-data": "The readiness assessment still needs more data.",
    "supervisor-missing": "No supervisor state is available yet for reporting handoff.",
    "supervisor-unavailable": "The supervisor state is unavailable, so reporting cannot proceed.",
    "supervisor-controller-failed": "The supervisor recorded a controller failure, so reporting cannot proceed.",
    "supervisor-investigation-open": "The supervisor still keeps investigation open, so reporting cannot proceed.",
}


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def normalize_supervisor_status(value: Any) -> str:
    text = maybe_text(value)
    if text == LEGACY_REPORTING_READY_STATUS:
        return SUPERVISOR_REPORTING_READY_STATUS
    return text


def normalize_reporting_handoff_status(value: Any) -> str:
    text = maybe_text(value)
    if text == LEGACY_REPORTING_READY_STATUS:
        return REPORTING_READY_STATUS
    if text == LEGACY_REPORTING_HOLD_STATUS:
        return REPORTING_HOLD_STATUS
    return text


def reporting_blockers(
    *,
    promotion_status: Any,
    readiness_status: Any,
    supervisor_status: Any,
    require_supervisor: bool = False,
    extra_blockers: list[Any] | None = None,
) -> list[str]:
    blockers: list[str] = []
    normalized_promotion_status = maybe_text(promotion_status) or "withheld"
    normalized_readiness_status = maybe_text(readiness_status)
    normalized_supervisor_status = normalize_supervisor_status(supervisor_status)
    if normalized_promotion_status != "promoted":
        blockers.append("promotion-withheld")
    if not normalized_readiness_status:
        blockers.append("readiness-missing")
    elif normalized_readiness_status != "ready":
        blockers.append(f"readiness-{normalized_readiness_status}")
    if require_supervisor:
        if not normalized_supervisor_status:
            blockers.append("supervisor-missing")
        elif normalized_supervisor_status == "unavailable":
            blockers.append("supervisor-unavailable")
        elif normalized_supervisor_status == "controller-failed":
            blockers.append("supervisor-controller-failed")
        elif normalized_supervisor_status == SUPERVISOR_HOLD_STATUS:
            blockers.append("supervisor-investigation-open")
        elif normalized_supervisor_status != SUPERVISOR_REPORTING_READY_STATUS:
            blockers.append(f"supervisor-{normalized_supervisor_status}")
    else:
        if normalized_supervisor_status == "controller-failed":
            blockers.append("supervisor-controller-failed")
        elif normalized_supervisor_status == SUPERVISOR_HOLD_STATUS:
            blockers.append("supervisor-investigation-open")
    blockers.extend(maybe_text(item) for item in list_items(extra_blockers) if maybe_text(item))
    return unique_texts(blockers)


def reporting_gate_state(
    *,
    promotion_status: Any,
    readiness_status: Any,
    supervisor_status: Any,
    require_supervisor: bool = False,
    reporting_ready: Any = None,
    reporting_blockers_value: Any = None,
    handoff_status: Any = "",
) -> dict[str, Any]:
    explicit_ready = maybe_bool(reporting_ready)
    normalized_handoff_status = normalize_reporting_handoff_status(handoff_status)
    blockers = reporting_blockers(
        promotion_status=promotion_status,
        readiness_status=readiness_status,
        supervisor_status=supervisor_status,
        require_supervisor=require_supervisor,
        extra_blockers=list_items(reporting_blockers_value),
    )
    ready = explicit_ready if explicit_ready is not None and not blockers else not blockers
    if ready:
        status = REPORTING_READY_STATUS
    elif normalized_handoff_status and normalized_handoff_status != REPORTING_READY_STATUS:
        status = normalized_handoff_status
    else:
        status = REPORTING_HOLD_STATUS
    return {
        "reporting_ready": ready,
        "reporting_blockers": blockers,
        "handoff_status": status,
        "promotion_status": maybe_text(promotion_status) or "withheld",
        "readiness_status": maybe_text(readiness_status) or "blocked",
        "supervisor_status": normalize_supervisor_status(supervisor_status)
        or ("unavailable" if require_supervisor else ""),
    }


def reporting_blocker_summaries(blockers: list[Any]) -> list[str]:
    results: list[str] = []
    for blocker in list_items(blockers):
        code = maybe_text(blocker)
        if not code:
            continue
        results.append(
            REPORTING_BLOCKER_SUMMARY_MAP.get(
                code,
                f"Reporting remains blocked by {code}.",
            )
        )
    return unique_texts(results)


__all__ = [
    "LEGACY_REPORTING_HOLD_STATUS",
    "LEGACY_REPORTING_READY_STATUS",
    "REPORTING_HOLD_STATUS",
    "REPORTING_READY_STATUS",
    "SUPERVISOR_HOLD_STATUS",
    "SUPERVISOR_REPORTING_READY_STATUS",
    "normalize_reporting_handoff_status",
    "normalize_supervisor_status",
    "reporting_blocker_summaries",
    "reporting_blockers",
    "reporting_gate_state",
]
