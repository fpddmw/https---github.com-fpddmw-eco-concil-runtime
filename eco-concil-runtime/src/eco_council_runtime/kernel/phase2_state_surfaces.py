from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..reporting_status import reporting_gate_state
from .deliberation_plane import (
    build_falsification_probe_payload,
    build_moderator_action_payload,
    load_council_decision_record,
    load_expert_report_record,
    load_falsification_probe_records,
    load_falsification_probe_snapshot,
    load_final_publication_record,
    load_moderator_action_records,
    load_moderator_action_snapshot,
    load_promotion_freeze_record,
    load_promotion_basis_record,
    load_reporting_handoff_record,
    load_round_readiness_assessment,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def extra_reporting_blockers(payload: dict[str, Any]) -> list[Any]:
    return list_items(payload.get("reporting_blockers"))


def enrich_supervisor_reporting_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    gate_state = reporting_gate_state(
        promotion_status=normalized.get("promotion_status"),
        readiness_status=normalized.get("readiness_status"),
        supervisor_status=normalized.get("supervisor_status"),
        require_supervisor=True,
        reporting_ready=normalized.get("reporting_ready"),
        reporting_blockers_value=extra_reporting_blockers(normalized),
        handoff_status=normalized.get("reporting_handoff_status"),
    )
    normalized["promotion_status"] = maybe_text(gate_state.get("promotion_status"))
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
    default_promotion_status: Any = "",
    default_readiness_status: Any = "",
    default_supervisor_status: Any = "",
    require_supervisor: bool = True,
) -> dict[str, Any]:
    normalized = dict(payload)
    explicit_promotion_status = maybe_text(normalized.get("promotion_status"))
    explicit_readiness_status = maybe_text(normalized.get("readiness_status"))
    explicit_supervisor_status = maybe_text(normalized.get("supervisor_status"))
    explicit_handoff_status = maybe_text(normalized.get("handoff_status"))
    explicit_blockers = extra_reporting_blockers(normalized)
    if (
        not explicit_promotion_status
        and not explicit_readiness_status
        and not explicit_supervisor_status
        and not maybe_text(default_promotion_status)
        and not maybe_text(default_readiness_status)
        and not maybe_text(default_supervisor_status)
    ):
        if "reporting_blockers" in normalized and not isinstance(
            normalized.get("reporting_blockers"), list
        ):
            normalized["reporting_blockers"] = []
        return normalized
    gate_state = reporting_gate_state(
        promotion_status=explicit_promotion_status or maybe_text(default_promotion_status),
        readiness_status=explicit_readiness_status or maybe_text(default_readiness_status),
        supervisor_status=explicit_supervisor_status or maybe_text(default_supervisor_status),
        require_supervisor=require_supervisor,
        reporting_ready=normalized.get("reporting_ready"),
        reporting_blockers_value=explicit_blockers,
        handoff_status=explicit_handoff_status,
    )
    normalized["promotion_status"] = maybe_text(gate_state.get("promotion_status"))
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
            default_promotion_status=supervisor.get("promotion_status"),
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
            default_promotion_status=handoff.get("promotion_status")
            or supervisor.get("promotion_status"),
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
            default_promotion_status=handoff.get("promotion_status")
            or supervisor.get("promotion_status"),
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
        "promotion_status": maybe_text(anchor_payload.get("promotion_status")),
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


def load_next_actions_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    next_actions_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    next_actions_file = resolve_path(
        run_dir_path,
        next_actions_path,
        f"investigation/next_actions_{round_id}.json",
    )
    record_payload = load_moderator_action_records(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    snapshot_payload = load_moderator_action_snapshot(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if record_payload or isinstance(snapshot_payload, dict):
        payload = build_moderator_action_payload(
            record_payload,
            snapshot_payload=snapshot_payload if isinstance(snapshot_payload, dict) else None,
            run_id=run_id,
            round_id=round_id,
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-actions",
            "artifact_path": str(next_actions_file),
            "artifact_present": next_actions_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(next_actions_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("action_source"))
            or "next-actions-artifact",
            "artifact_path": str(next_actions_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-next-actions",
        "artifact_path": str(next_actions_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_falsification_probe_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    probes_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    probes_file = resolve_path(
        run_dir_path,
        probes_path,
        f"investigation/falsification_probes_{round_id}.json",
    )
    record_payload = load_falsification_probe_records(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    snapshot_payload = load_falsification_probe_snapshot(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if record_payload or isinstance(snapshot_payload, dict):
        payload = build_falsification_probe_payload(
            record_payload,
            snapshot_payload=snapshot_payload if isinstance(snapshot_payload, dict) else None,
            run_id=run_id,
            round_id=round_id,
        )
        payload["action_source"] = (
            maybe_text(payload.get("action_source"))
            or "deliberation-plane-probes"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-probes",
            "artifact_path": str(probes_file),
            "artifact_present": probes_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(probes_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("action_source"))
            or "falsification-probes-artifact",
            "artifact_path": str(probes_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-probes",
        "artifact_path": str(probes_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_round_readiness_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    readiness_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    readiness_file = resolve_path(
        run_dir_path,
        readiness_path,
        f"reporting/round_readiness_{round_id}.json",
    )
    readiness_payload = load_round_readiness_assessment(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(readiness_payload, dict):
        payload = dict(readiness_payload)
        payload["readiness_source"] = (
            maybe_text(payload.get("readiness_source"))
            or "deliberation-plane-readiness"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-readiness",
            "artifact_path": str(readiness_file),
            "artifact_present": readiness_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(readiness_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("readiness_source"))
            or "round-readiness-artifact",
            "artifact_path": str(readiness_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-readiness",
        "artifact_path": str(readiness_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_promotion_basis_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    promotion_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    promotion_file = resolve_path(
        run_dir_path,
        promotion_path,
        f"promotion/promoted_evidence_basis_{round_id}.json",
    )
    promotion_payload = load_promotion_basis_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(promotion_payload, dict):
        payload = dict(promotion_payload)
        payload["promotion_source"] = (
            maybe_text(payload.get("promotion_source"))
            or "deliberation-plane-promotion-basis"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-promotion-basis",
            "artifact_path": str(promotion_file),
            "artifact_present": promotion_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(promotion_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("promotion_source"))
            or "promotion-artifact",
            "artifact_path": str(promotion_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-promotion",
        "artifact_path": str(promotion_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_supervisor_state_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    supervisor_state_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    supervisor_file = resolve_path(
        run_dir_path,
        supervisor_state_path,
        f"runtime/supervisor_state_{round_id}.json",
    )
    freeze_payload = load_promotion_freeze_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(freeze_payload, dict):
        supervisor_payload = (
            freeze_payload.get("supervisor_snapshot", {})
            if isinstance(freeze_payload.get("supervisor_snapshot"), dict)
            else {}
        )
        if isinstance(supervisor_payload, dict) and supervisor_payload:
            payload = enrich_supervisor_reporting_payload(supervisor_payload)
            return {
                "payload": payload,
                "source": "deliberation-plane-supervisor",
                "artifact_path": str(supervisor_file),
                "artifact_present": supervisor_file.exists(),
                "payload_present": True,
            }
        if maybe_text(freeze_payload.get("supervisor_status")):
            payload = enrich_supervisor_reporting_payload(
                {
                    "run_id": maybe_text(freeze_payload.get("run_id")) or run_id,
                    "round_id": maybe_text(freeze_payload.get("round_id"))
                    or round_id,
                    "generated_at_utc": maybe_text(
                        freeze_payload.get("updated_at_utc")
                    ),
                    "supervisor_status": maybe_text(
                        freeze_payload.get("supervisor_status")
                    ),
                    "readiness_status": maybe_text(
                        freeze_payload.get("readiness_status")
                    ),
                    "gate_status": maybe_text(freeze_payload.get("gate_status")),
                    "promotion_status": maybe_text(
                        freeze_payload.get("promotion_status")
                    ),
                    "planning_mode": maybe_text(
                        freeze_payload.get("planning_mode")
                    ),
                    "recommended_next_skills": (
                        freeze_payload.get("recommended_next_skills", [])
                        if isinstance(
                            freeze_payload.get("recommended_next_skills"), list
                        )
                        else []
                    ),
                    "supervisor_path": str(supervisor_file),
                }
            )
            return {
                "payload": payload,
                "source": "deliberation-plane-promotion-freeze",
                "artifact_path": str(supervisor_file),
                "artifact_present": supervisor_file.exists(),
                "payload_present": True,
            }
    artifact_payload = load_json_if_exists(supervisor_file)
    if isinstance(artifact_payload, dict):
        payload = enrich_supervisor_reporting_payload(artifact_payload)
        return {
            "payload": payload,
            "source": "supervisor-state-artifact",
            "artifact_path": str(supervisor_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-supervisor-state",
        "artifact_path": str(supervisor_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_reporting_handoff_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    reporting_handoff_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    handoff_file = resolve_path(
        run_dir_path,
        reporting_handoff_path,
        f"reporting/reporting_handoff_{round_id}.json",
    )
    handoff_payload = load_reporting_handoff_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(handoff_payload, dict):
        payload = enrich_reporting_record_payload(handoff_payload)
        return {
            "payload": payload,
            "source": "deliberation-plane-reporting-handoff",
            "artifact_path": str(handoff_file),
            "artifact_present": handoff_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(handoff_file)
    if isinstance(artifact_payload, dict):
        payload = enrich_reporting_record_payload(artifact_payload)
        return {
            "payload": payload,
            "source": "reporting-handoff-artifact",
            "artifact_path": str(handoff_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-reporting-handoff",
        "artifact_path": str(handoff_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_council_decision_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    decision_stage: str = "canonical",
    decision_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    normalized_stage = (
        "draft" if maybe_text(decision_stage) == "draft" else "canonical"
    )
    default_relative = (
        f"reporting/council_decision_draft_{round_id}.json"
        if normalized_stage == "draft"
        else f"reporting/council_decision_{round_id}.json"
    )
    decision_file = resolve_path(
        run_dir_path,
        decision_path,
        default_relative,
    )
    record_payload = load_council_decision_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        decision_stage=normalized_stage,
    )
    if isinstance(record_payload, dict):
        payload = enrich_reporting_record_payload(record_payload)
        payload.pop("record_id", None)
        payload.pop("decision_stage", None)
        return {
            "payload": payload,
            "source": (
                "deliberation-plane-council-decision-draft"
                if normalized_stage == "draft"
                else "deliberation-plane-council-decision"
            ),
            "artifact_path": str(decision_file),
            "artifact_present": decision_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(decision_file)
    if isinstance(artifact_payload, dict):
        payload = enrich_reporting_record_payload(artifact_payload)
        return {
            "payload": payload,
            "source": (
                "council-decision-draft-artifact"
                if normalized_stage == "draft"
                else "council-decision-artifact"
            ),
            "artifact_path": str(decision_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": (
            "missing-council-decision-draft"
            if normalized_stage == "draft"
            else "missing-council-decision"
        ),
        "artifact_path": str(decision_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_expert_report_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    agent_role: str,
    report_stage: str = "canonical",
    report_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    normalized_role = maybe_text(agent_role)
    normalized_stage = "draft" if maybe_text(report_stage) == "draft" else "canonical"
    default_relative = (
        f"reporting/expert_report_draft_{normalized_role}_{round_id}.json"
        if normalized_stage == "draft"
        else f"reporting/expert_report_{normalized_role}_{round_id}.json"
    )
    report_file = resolve_path(
        run_dir_path,
        report_path,
        default_relative,
    )
    record_payload = load_expert_report_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        report_stage=normalized_stage,
        agent_role=normalized_role,
    )
    if isinstance(record_payload, dict):
        payload = enrich_reporting_record_payload(record_payload)
        payload.pop("record_id", None)
        payload.pop("report_stage", None)
        return {
            "payload": payload,
            "source": (
                "deliberation-plane-expert-report-draft"
                if normalized_stage == "draft"
                else "deliberation-plane-expert-report"
            ),
            "artifact_path": str(report_file),
            "artifact_present": report_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(report_file)
    if isinstance(artifact_payload, dict):
        payload = enrich_reporting_record_payload(artifact_payload)
        return {
            "payload": payload,
            "source": (
                "expert-report-draft-artifact"
                if normalized_stage == "draft"
                else "expert-report-artifact"
            ),
            "artifact_path": str(report_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": (
            "missing-expert-report-draft"
            if normalized_stage == "draft"
            else f"missing-{normalized_role}-report"
        ),
        "artifact_path": str(report_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_final_publication_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    output_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    publication_file = resolve_path(
        run_dir_path,
        output_path,
        f"reporting/final_publication_{round_id}.json",
    )
    publication_payload = load_final_publication_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(publication_payload, dict):
        return {
            "payload": publication_payload,
            "source": "deliberation-plane-final-publication",
            "artifact_path": str(publication_file),
            "artifact_present": publication_file.exists(),
            "payload_present": True,
        }
    artifact_payload = load_json_if_exists(publication_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": "final-publication-artifact",
            "artifact_path": str(publication_file),
            "artifact_present": True,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-final-publication",
        "artifact_path": str(publication_file),
        "artifact_present": False,
        "payload_present": False,
    }


__all__ = [
    "build_reporting_surface",
    "load_council_decision_wrapper",
    "load_expert_report_wrapper",
    "load_final_publication_wrapper",
    "load_falsification_probe_wrapper",
    "load_next_actions_wrapper",
    "load_promotion_basis_wrapper",
    "load_reporting_handoff_wrapper",
    "load_round_readiness_wrapper",
    "load_supervisor_state_wrapper",
]
