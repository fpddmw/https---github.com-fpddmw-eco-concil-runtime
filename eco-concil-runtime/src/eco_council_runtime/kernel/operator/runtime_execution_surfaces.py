from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane import (
    load_controller_snapshot_record,
    load_gate_snapshot_record,
    load_orchestration_plan_record,
    load_orchestration_plan_steps,
    load_runtime_control_freeze_record,
    load_supervisor_snapshot_record,
)
from eco_council_runtime.kernel.operator.runtime_reporting_surfaces import (
    enrich_supervisor_reporting_payload,
)
from eco_council_runtime.kernel.operator.runtime_surface_common import (
    maybe_text,
    orphaned_artifact_wrapper,
    resolve_path,
)


def plan_wrapper_kind(plan_file: Path) -> tuple[str, str]:
    return "queue-owner", "orchestration-plan"


def load_orchestration_plan_wrapper(
    run_dir: str | Path,
    *,
    round_id: str,
    run_id: str = "",
    orchestration_plan_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    plan_file = resolve_path(
        run_dir_path,
        orchestration_plan_path,
        f"runtime/orchestration_plan_{round_id}.json",
    )
    controller_authority, source_suffix = plan_wrapper_kind(plan_file)
    plan_payload = load_orchestration_plan_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        artifact_path=str(plan_file),
        controller_authority=controller_authority,
        allow_latest_fallback=False,
    )
    if isinstance(plan_payload, dict):
        step_rows = load_orchestration_plan_steps(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            plan_id=maybe_text(plan_payload.get("plan_id")),
        )
        return {
            "payload": plan_payload,
            "step_rows": step_rows,
            "step_row_count": len(step_rows),
            "source": f"deliberation-plane-{source_suffix}",
            "artifact_path": str(plan_file),
            "artifact_present": plan_file.exists(),
            "payload_present": True,
        }
    if plan_file.exists():
        return orphaned_artifact_wrapper(
            plan_file,
            source=f"orphaned-{source_suffix}-artifact",
        )
    return {
        "payload": None,
        "step_rows": [],
        "step_row_count": 0,
        "source": f"missing-{source_suffix}",
        "artifact_path": str(plan_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_controller_state_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    controller_state_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    controller_file = resolve_path(
        run_dir_path,
        controller_state_path,
        f"runtime/controller_state_{round_id}.json",
    )
    controller_payload = load_controller_snapshot_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(controller_payload, dict):
        return {
            "payload": controller_payload,
            "source": "deliberation-plane-controller",
            "artifact_path": str(controller_file),
            "artifact_present": controller_file.exists(),
            "payload_present": True,
        }
    freeze_payload = load_runtime_control_freeze_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(freeze_payload, dict):
        controller_snapshot = (
            freeze_payload.get("controller_snapshot", {})
            if isinstance(freeze_payload.get("controller_snapshot"), dict)
            else {}
        )
        if controller_snapshot:
            return {
                "payload": controller_snapshot,
                "source": "deliberation-plane-report-basis-freeze",
                "artifact_path": str(controller_file),
                "artifact_present": controller_file.exists(),
                "payload_present": True,
            }
        if maybe_text(freeze_payload.get("controller_status")):
            artifacts = (
                freeze_payload.get("artifacts", {})
                if isinstance(freeze_payload.get("artifacts"), dict)
                else {}
            )
            return {
                "payload": {
                    "run_id": maybe_text(freeze_payload.get("run_id")) or run_id,
                    "round_id": maybe_text(freeze_payload.get("round_id"))
                    or round_id,
                    "generated_at_utc": maybe_text(freeze_payload.get("updated_at_utc")),
                    "controller_status": maybe_text(
                        freeze_payload.get("controller_status")
                    ),
                    "planning_mode": maybe_text(freeze_payload.get("planning_mode")),
                    "readiness_status": maybe_text(
                        freeze_payload.get("readiness_status")
                    ),
                    "gate_status": maybe_text(freeze_payload.get("gate_status")),
                    "report_basis_status": maybe_text(
                        freeze_payload.get("report_basis_status")
                    ),
                    "resume_status": "",
                    "current_stage": "",
                    "failed_stage": "",
                    "resume_from_stage": "",
                    "resume_recommended": False,
                    "restart_recommended": False,
                    "completed_stage_names": [],
                    "pending_stage_names": [],
                    "gate_reasons": (
                        freeze_payload.get("gate_reasons", [])
                        if isinstance(freeze_payload.get("gate_reasons"), list)
                        else []
                    ),
                    "recommended_next_skills": (
                        freeze_payload.get("recommended_next_skills", [])
                        if isinstance(
                            freeze_payload.get("recommended_next_skills"), list
                        )
                        else []
                    ),
                    "execution_policy": {},
                    "progress": {},
                    "recovery": {},
                    "planning": {},
                    "planning_attempts": [],
                    "stage_contracts": {},
                    "steps": [],
                    "artifacts": artifacts,
                    "failure": {},
                },
                "source": "deliberation-plane-report-basis-freeze-summary",
                "artifact_path": str(controller_file),
                "artifact_present": controller_file.exists(),
                "payload_present": True,
            }
    if controller_file.exists():
        return orphaned_artifact_wrapper(
            controller_file,
            source="orphaned-controller-state-artifact",
        )
    return {
        "payload": None,
        "source": "missing-controller-state",
        "artifact_path": str(controller_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_report_basis_gate_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    report_basis_gate_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    gate_file = resolve_path(
        run_dir_path,
        report_basis_gate_path,
        f"runtime/report_basis_gate_{round_id}.json",
    )
    artifact_file = gate_file
    gate_payload = load_gate_snapshot_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(gate_payload, dict):
        return {
            "payload": gate_payload,
            "source": "deliberation-plane-gate",
            "artifact_path": str(artifact_file),
            "artifact_present": artifact_file.exists(),
            "payload_present": True,
        }
    freeze_payload = load_runtime_control_freeze_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(freeze_payload, dict):
        gate_snapshot = (
            freeze_payload.get("gate_snapshot", {})
            if isinstance(freeze_payload.get("gate_snapshot"), dict)
            else {}
        )
        if gate_snapshot:
            return {
                "payload": gate_snapshot,
                "source": "deliberation-plane-report-basis-freeze",
                "artifact_path": str(artifact_file),
                "artifact_present": artifact_file.exists(),
                "payload_present": True,
            }
        if maybe_text(freeze_payload.get("gate_status")):
            return {
                "payload": {
                    "run_id": maybe_text(freeze_payload.get("run_id")) or run_id,
                    "round_id": maybe_text(freeze_payload.get("round_id"))
                    or round_id,
                    "generated_at_utc": maybe_text(freeze_payload.get("updated_at_utc")),
                    "stage_name": "report-basis-gate",
                    "gate_handler": "report-basis-gate",
                    "gate_semantics": "report-basis-gate",
                    "gate_status": maybe_text(freeze_payload.get("gate_status")),
                    "report_basis_gate_status": maybe_text(
                        freeze_payload.get("report_basis_gate_status")
                    ),
                    "readiness_status": maybe_text(
                        freeze_payload.get("readiness_status")
                    ),
                    "report_basis_freeze_allowed": bool(
                        freeze_payload.get("report_basis_freeze_allowed")
                    ),
                    "report_basis_status": maybe_text(
                        freeze_payload.get("report_basis_status")
                    ),
                    "decision_source": "",
                    "report_basis_resolution_mode": "",
                    "gate_reasons": (
                        freeze_payload.get("gate_reasons", [])
                        if isinstance(freeze_payload.get("gate_reasons"), list)
                        else []
                    ),
                    "supporting_proposal_ids": [],
                    "rejected_proposal_ids": [],
                    "supporting_opinion_ids": [],
                    "rejected_opinion_ids": [],
                    "council_input_counts": {},
                    "recommended_next_skills": (
                        freeze_payload.get("recommended_next_skills", [])
                        if isinstance(
                            freeze_payload.get("recommended_next_skills"), list
                        )
                        else []
                    ),
                    "warnings": [],
                    "readiness_path": "",
                    "output_path": str(artifact_file),
                },
                "source": "deliberation-plane-report-basis-freeze-summary",
                "artifact_path": str(artifact_file),
                "artifact_present": artifact_file.exists(),
                "payload_present": True,
            }
    if artifact_file.exists():
        return orphaned_artifact_wrapper(
            artifact_file,
            source="orphaned-report-basis-gate-artifact",
        )
    return {
        "payload": None,
        "source": "missing-report-basis-gate",
        "artifact_path": str(artifact_file),
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
    supervisor_payload = load_supervisor_snapshot_record(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(supervisor_payload, dict):
        payload = enrich_supervisor_reporting_payload(supervisor_payload)
        return {
            "payload": payload,
            "source": "deliberation-plane-supervisor",
            "artifact_path": str(supervisor_file),
            "artifact_present": supervisor_file.exists(),
            "payload_present": True,
        }
    freeze_payload = load_runtime_control_freeze_record(
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
                    "report_basis_status": maybe_text(
                        freeze_payload.get("report_basis_status")
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
                "source": "deliberation-plane-report-basis-freeze",
                "artifact_path": str(supervisor_file),
                "artifact_present": supervisor_file.exists(),
                "payload_present": True,
            }
    if supervisor_file.exists():
        return orphaned_artifact_wrapper(
            supervisor_file,
            source="orphaned-supervisor-state-artifact",
        )
    return {
        "payload": None,
        "source": "missing-supervisor-state",
        "artifact_path": str(supervisor_file),
        "artifact_present": False,
        "payload_present": False,
    }


__all__ = [
    "load_controller_state_wrapper",
    "load_orchestration_plan_wrapper",
    "load_report_basis_gate_wrapper",
    "load_supervisor_state_wrapper",
    "plan_wrapper_kind",
]
