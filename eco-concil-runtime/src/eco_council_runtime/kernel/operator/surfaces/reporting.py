from __future__ import annotations

from typing import Any

from eco_council_runtime.reporting_status import reporting_gate_state
from eco_council_runtime.kernel.operator.surfaces.common import (
    list_items,
    maybe_text,
)


def extra_reporting_blockers(payload: dict[str, Any]) -> list[Any]:
    return list_items(payload.get("reporting_blockers"))


def enrich_supervisor_reporting_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    gate_state = reporting_gate_state(
        report_basis_status=normalized.get("report_basis_status"),
        readiness_status=normalized.get("readiness_status"),
        supervisor_status=normalized.get("supervisor_status"),
        require_supervisor=True,
        reporting_ready=normalized.get("reporting_ready"),
        reporting_blockers_value=extra_reporting_blockers(normalized),
        handoff_status=normalized.get("reporting_handoff_status"),
    )
    normalized["report_basis_status"] = maybe_text(gate_state.get("report_basis_status"))
    normalized["readiness_status"] = maybe_text(gate_state.get("readiness_status"))
    normalized["supervisor_status"] = maybe_text(gate_state.get("supervisor_status"))
    normalized["reporting_ready"] = bool(gate_state.get("reporting_ready"))
    normalized["reporting_blockers"] = list_items(gate_state.get("reporting_blockers"))
    normalized["reporting_handoff_status"] = maybe_text(
        gate_state.get("handoff_status")
    )
    normalized["handoff_status"] = normalized["reporting_handoff_status"]
    return normalized


def enrich_reporting_record_payload(
    payload: dict[str, Any],
    *,
    default_report_basis_status: Any = "",
    default_readiness_status: Any = "",
    default_supervisor_status: Any = "",
    require_supervisor: bool = True,
) -> dict[str, Any]:
    normalized = dict(payload)
    explicit_report_basis_status = maybe_text(normalized.get("report_basis_status"))
    explicit_readiness_status = maybe_text(normalized.get("readiness_status"))
    explicit_supervisor_status = maybe_text(normalized.get("supervisor_status"))
    explicit_handoff_status = maybe_text(normalized.get("handoff_status"))
    explicit_blockers = extra_reporting_blockers(normalized)
    if (
        not explicit_report_basis_status
        and not explicit_readiness_status
        and not explicit_supervisor_status
        and not maybe_text(default_report_basis_status)
        and not maybe_text(default_readiness_status)
        and not maybe_text(default_supervisor_status)
    ):
        if "reporting_blockers" in normalized and not isinstance(
            normalized.get("reporting_blockers"), list
        ):
            normalized["reporting_blockers"] = []
        return normalized
    gate_state = reporting_gate_state(
        report_basis_status=explicit_report_basis_status or maybe_text(default_report_basis_status),
        readiness_status=explicit_readiness_status or maybe_text(default_readiness_status),
        supervisor_status=explicit_supervisor_status or maybe_text(default_supervisor_status),
        require_supervisor=require_supervisor,
        reporting_ready=normalized.get("reporting_ready"),
        reporting_blockers_value=explicit_blockers,
        handoff_status=explicit_handoff_status,
    )
    normalized["report_basis_status"] = maybe_text(gate_state.get("report_basis_status"))
    normalized["readiness_status"] = maybe_text(gate_state.get("readiness_status"))
    normalized["supervisor_status"] = maybe_text(gate_state.get("supervisor_status"))
    normalized["reporting_ready"] = bool(gate_state.get("reporting_ready"))
    normalized["reporting_blockers"] = list_items(gate_state.get("reporting_blockers"))
    normalized["handoff_status"] = maybe_text(gate_state.get("handoff_status"))
    return normalized


def build_reporting_surface(
    *,
    supervisor_payload: dict[str, Any] | None = None,
    handoff_payload: dict[str, Any] | None = None,
    decision_draft_payload: dict[str, Any] | None = None,
    decision_payload: dict[str, Any] | None = None,
    expert_report_payloads: dict[str, dict[str, Any]] | None = None,
    final_publication_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    supervisor = (
        enrich_supervisor_reporting_payload(supervisor_payload)
        if isinstance(supervisor_payload, dict) and supervisor_payload
        else {}
    )
    handoff = (
        enrich_reporting_record_payload(
            handoff_payload,
            default_report_basis_status=supervisor.get("report_basis_status"),
            default_readiness_status=supervisor.get("readiness_status"),
            default_supervisor_status=supervisor.get("supervisor_status"),
            require_supervisor=True,
        )
        if isinstance(handoff_payload, dict) and handoff_payload
        else {}
    )
    decision_draft = (
        enrich_reporting_record_payload(
            decision_draft_payload,
            default_report_basis_status=handoff.get("report_basis_status")
            or supervisor.get("report_basis_status"),
            default_readiness_status=handoff.get("readiness_status")
            or supervisor.get("readiness_status"),
            default_supervisor_status=handoff.get("supervisor_status")
            or supervisor.get("supervisor_status"),
            require_supervisor=True,
        )
        if isinstance(decision_draft_payload, dict) and decision_draft_payload
        else {}
    )
    decision = (
        enrich_reporting_record_payload(
            decision_payload,
            default_report_basis_status=handoff.get("report_basis_status")
            or supervisor.get("report_basis_status"),
            default_readiness_status=handoff.get("readiness_status")
            or supervisor.get("readiness_status"),
            default_supervisor_status=handoff.get("supervisor_status")
            or supervisor.get("supervisor_status"),
            require_supervisor=True,
        )
        if isinstance(decision_payload, dict) and decision_payload
        else {}
    )
    anchor_source = "missing"
    anchor_payload: dict[str, Any] = {}
    for source_name, candidate in (
        ("council-decision", decision),
        ("council-decision-draft", decision_draft),
        ("reporting-handoff", handoff),
        ("supervisor", supervisor),
    ):
        if candidate:
            anchor_source = source_name
            anchor_payload = candidate
            break
    publication = (
        dict(final_publication_payload)
        if isinstance(final_publication_payload, dict) and final_publication_payload
        else {}
    )
    report_statuses: dict[str, str] = {}
    for role, payload in (
        expert_report_payloads.items()
        if isinstance(expert_report_payloads, dict)
        else []
    ):
        if isinstance(payload, dict) and payload:
            report_statuses[role] = maybe_text(payload.get("status"))
    return {
        "surface_source": anchor_source,
        "reporting_ready": bool(anchor_payload.get("reporting_ready")),
        "reporting_blockers": list_items(anchor_payload.get("reporting_blockers")),
        "handoff_status": maybe_text(anchor_payload.get("handoff_status"))
        or maybe_text(anchor_payload.get("reporting_handoff_status")),
        "report_basis_status": maybe_text(anchor_payload.get("report_basis_status")),
        "readiness_status": maybe_text(anchor_payload.get("readiness_status")),
        "supervisor_status": maybe_text(anchor_payload.get("supervisor_status")),
        "publication_readiness": maybe_text(
            decision.get("publication_readiness")
        )
        or maybe_text(decision_draft.get("publication_readiness")),
        "publication_status": maybe_text(publication.get("publication_status")),
        "publication_posture": maybe_text(publication.get("publication_posture")),
        "handoff_present": bool(handoff),
        "decision_draft_present": bool(decision_draft),
        "decision_present": bool(decision),
        "final_publication_present": bool(publication),
        "expert_report_statuses": report_statuses,
    }


__all__ = [
    "build_reporting_surface",
    "enrich_reporting_record_payload",
    "enrich_supervisor_reporting_payload",
    "extra_reporting_blockers",
]
