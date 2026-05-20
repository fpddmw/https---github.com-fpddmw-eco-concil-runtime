from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.contracts import validate_canonical_payload
from eco_council_runtime.kernel.execution.governed_execution_action_semantics import maybe_bool
from eco_council_runtime.reporting_status import normalize_reporting_handoff_status, reporting_gate_state
from eco_council_runtime.kernel.planes.deliberation_actions import (
    build_falsification_probe_payload,
    build_moderator_action_payload,
    load_falsification_probe_records,
    load_falsification_probe_snapshot,
    load_moderator_action_records,
    load_moderator_action_snapshot,
    load_round_readiness_assessment,
)
from eco_council_runtime.kernel.planes.deliberation_plane_rows import (
    coerce_int,
    dict_items,
    fetch_runtime_control_freeze,
    fetch_snapshot_payload,
    json_text,
    latest_json_row,
    latest_json_row_where,
    list_items,
    maybe_text,
    payload_from_db_row,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_controller_snapshot_row,
    write_gate_snapshot_row,
    write_orchestration_plan_row,
    write_orchestration_plan_step_row,
    write_report_basis_freeze_row,
    write_round_task_snapshot_row,
    write_supervisor_snapshot_row,
)
from eco_council_runtime.kernel.planes.deliberation_plane_schema import connect_db, resolve_run_dir
from eco_council_runtime.kernel.planes.deliberation_reporting_records import (
    REPORT_AGENT_ROLES,
    load_council_decision_record,
    load_expert_report_record,
    load_final_publication_record,
    load_report_basis_freeze_record,
    load_report_basis_freeze_items,
    load_reporting_handoff_record,
)
def runtime_control_freeze_id(run_id: str, round_id: str) -> str:
    return "control-freeze-" + stable_hash("runtime-control-freeze", run_id, round_id)[:12]

def controller_snapshot_object_id(run_id: str, round_id: str) -> str:
    return "controller-" + stable_hash("controller-state", run_id, round_id)[:12]

def gate_snapshot_object_id(
    run_id: str,
    round_id: str,
    stage_name: str,
    gate_handler: str,
) -> str:
    return "gate-" + stable_hash(
        "gate-state",
        run_id,
        round_id,
        stage_name,
        gate_handler,
    )[:12]

def supervisor_snapshot_object_id(run_id: str, round_id: str) -> str:
    return "supervisor-" + stable_hash("supervisor-state", run_id, round_id)[:12]

def runtime_control_freeze_row_from_payload(
    freeze_record: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    artifacts = freeze_record.get("artifacts", {}) if isinstance(freeze_record.get("artifacts"), dict) else {}
    return {
        "freeze_id": maybe_text(freeze_record.get("freeze_id")),
        "run_id": maybe_text(freeze_record.get("run_id")),
        "round_id": maybe_text(freeze_record.get("round_id")),
        "updated_at_utc": maybe_text(freeze_record.get("updated_at_utc")),
        "gate_status": maybe_text(freeze_record.get("gate_status")),
        "readiness_status": maybe_text(freeze_record.get("readiness_status")),
        "report_basis_status": maybe_text(freeze_record.get("report_basis_status")),
        "controller_status": maybe_text(freeze_record.get("controller_status")),
        "supervisor_status": maybe_text(freeze_record.get("supervisor_status")),
        "planning_mode": maybe_text(freeze_record.get("planning_mode")),
        "report_basis_freeze_allowed": 1 if bool(freeze_record.get("report_basis_freeze_allowed")) else 0,
        "gate_reasons_json": json_text(freeze_record.get("gate_reasons", [])),
        "recommended_next_skills_json": json_text(freeze_record.get("recommended_next_skills", [])),
        "reporting_ready": 1 if bool(freeze_record.get("reporting_ready")) else 0,
        "reporting_handoff_status": maybe_text(
            freeze_record.get("reporting_handoff_status")
        ),
        "reporting_blockers_json": json_text(
            freeze_record.get("reporting_blockers", [])
        ),
        "controller_artifact_path": maybe_text(artifacts.get("controller_state_path")),
        "gate_artifact_path": maybe_text(artifacts.get("report_basis_gate_path")),
        "supervisor_artifact_path": maybe_text(artifacts.get("supervisor_state_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(freeze_record),
    }

def control_snapshot_row_id(
    prefix: str,
    payload: dict[str, Any],
    *parts: Any,
) -> str:
    snapshot_payload = dict(payload)
    snapshot_payload.pop("snapshot_id", None)
    return prefix + "-" + stable_hash(prefix, *parts, json_text(snapshot_payload))[:20]

def normalized_controller_snapshot_payload(
    controller_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(controller_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["controller_id"] = maybe_text(
        normalized.get("controller_id")
    ) or controller_snapshot_object_id(
        normalized["run_id"],
        normalized["round_id"],
    )
    normalized["controller_status"] = (
        maybe_text(normalized.get("controller_status")) or "running"
    )
    normalized["planning_mode"] = (
        maybe_text(normalized.get("planning_mode")) or "planner-backed"
    )
    normalized["current_stage"] = maybe_text(normalized.get("current_stage"))
    normalized["failed_stage"] = maybe_text(normalized.get("failed_stage"))
    normalized["resume_status"] = maybe_text(normalized.get("resume_status"))
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "pending"
    )
    normalized["gate_status"] = (
        maybe_text(normalized.get("gate_status")) or "not-evaluated"
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status")) or "not-evaluated"
    )
    normalized["resume_recommended"] = bool(
        maybe_bool(normalized.get("resume_recommended"))
    )
    normalized["restart_recommended"] = bool(
        maybe_bool(normalized.get("restart_recommended"))
    )
    normalized["completed_stage_names"] = unique_texts(
        list_items(normalized.get("completed_stage_names"))
    )
    normalized["pending_stage_names"] = unique_texts(
        list_items(normalized.get("pending_stage_names"))
    )
    normalized["gate_reasons"] = unique_texts(list_items(normalized.get("gate_reasons")))
    normalized["recommended_next_skills"] = unique_texts(
        list_items(normalized.get("recommended_next_skills"))
    )
    normalized["execution_policy"] = dict_items(normalized.get("execution_policy"))
    normalized["progress"] = dict_items(normalized.get("progress"))
    normalized["recovery"] = dict_items(normalized.get("recovery"))
    normalized["resume_from_stage"] = (
        maybe_text(normalized.get("resume_from_stage"))
        or maybe_text(normalized["recovery"].get("resume_from_stage"))
    )
    normalized["planning"] = dict_items(normalized.get("planning"))
    normalized["planning_attempts"] = list_items(normalized.get("planning_attempts"))
    normalized["stage_contracts"] = dict_items(normalized.get("stage_contracts"))
    normalized["steps"] = list_items(normalized.get("steps"))
    normalized["artifacts"] = dict_items(normalized.get("artifacts"))
    normalized["failure"] = dict_items(normalized.get("failure"))
    normalized["snapshot_id"] = maybe_text(
        normalized.get("snapshot_id")
    ) or maybe_text(normalized.get("controller_id"))
    return validate_canonical_payload("controller-state", normalized)

def controller_snapshot_row_from_payload(
    controller_payload: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_controller_snapshot_payload(controller_payload)
    return {
        "snapshot_id": maybe_text(normalized.get("snapshot_id")),
        "controller_id": maybe_text(normalized.get("controller_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "controller_status": maybe_text(normalized.get("controller_status")),
        "planning_mode": maybe_text(normalized.get("planning_mode")),
        "current_stage": maybe_text(normalized.get("current_stage")),
        "failed_stage": maybe_text(normalized.get("failed_stage")),
        "resume_status": maybe_text(normalized.get("resume_status")),
        "readiness_status": maybe_text(normalized.get("readiness_status")),
        "gate_status": maybe_text(normalized.get("gate_status")),
        "report_basis_status": maybe_text(normalized.get("report_basis_status")),
        "resume_recommended": 1 if bool(normalized.get("resume_recommended")) else 0,
        "restart_recommended": 1
        if bool(normalized.get("restart_recommended"))
        else 0,
        "resume_from_stage": maybe_text(normalized.get("resume_from_stage")),
        "completed_stage_names_json": json_text(
            normalized.get("completed_stage_names", [])
        ),
        "pending_stage_names_json": json_text(
            normalized.get("pending_stage_names", [])
        ),
        "gate_reasons_json": json_text(normalized.get("gate_reasons", [])),
        "recommended_next_skills_json": json_text(
            normalized.get("recommended_next_skills", [])
        ),
        "execution_policy_json": json_text(normalized.get("execution_policy", {})),
        "progress_json": json_text(normalized.get("progress", {})),
        "recovery_json": json_text(normalized.get("recovery", {})),
        "planning_json": json_text(normalized.get("planning", {})),
        "planning_attempts_json": json_text(
            normalized.get("planning_attempts", [])
        ),
        "stage_contracts_json": json_text(normalized.get("stage_contracts", {})),
        "steps_json": json_text(normalized.get("steps", [])),
        "artifacts_json": json_text(normalized.get("artifacts", {})),
        "failure_json": json_text(normalized.get("failure", {})),
        "artifact_path": maybe_text(normalized.get("artifacts", {}).get("controller_state_path"))
        or maybe_text(normalized.get("artifact_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }

def normalized_gate_snapshot_payload(
    gate_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(gate_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["stage_name"] = (
        maybe_text(normalized.get("stage_name"))
        or maybe_text(normalized.get("gate_handler"))
        or "report-basis-gate"
    )
    normalized["gate_handler"] = (
        maybe_text(normalized.get("gate_handler"))
        or maybe_text(normalized.get("stage_name"))
        or "report-basis-gate"
    )
    normalized["gate_semantics"] = (
        maybe_text(normalized.get("gate_semantics"))
        or normalized["stage_name"]
    )
    normalized["gate_id"] = maybe_text(
        normalized.get("gate_id")
    ) or gate_snapshot_object_id(
        normalized["run_id"],
        normalized["round_id"],
        normalized["stage_name"],
        normalized["gate_handler"],
    )
    normalized["gate_status"] = (
        maybe_text(normalized.get("gate_status")) or "not-evaluated"
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "pending"
    )
    normalized["report_basis_freeze_allowed"] = bool(
        maybe_bool(normalized.get("report_basis_freeze_allowed"))
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status"))
        or ("frozen" if normalized["report_basis_freeze_allowed"] else "withheld")
    )
    normalized["report_basis_gate_status"] = (
        maybe_text(normalized.get("report_basis_gate_status"))
        or (
            "report-basis-freeze-allowed"
            if normalized["report_basis_freeze_allowed"]
            else "report-basis-freeze-withheld"
        )
    )
    normalized["decision_source"] = (
        maybe_text(normalized.get("decision_source")) or "policy-fallback"
    )
    normalized["report_basis_resolution_mode"] = maybe_text(
        normalized.get("report_basis_resolution_mode")
    )
    normalized["report_basis_resolution_mode"] = (
        maybe_text(normalized.get("report_basis_resolution_mode"))
        or normalized["report_basis_resolution_mode"]
    )
    normalized["gate_reasons"] = unique_texts(list_items(normalized.get("gate_reasons")))
    normalized["supporting_proposal_ids"] = unique_texts(
        list_items(normalized.get("supporting_proposal_ids"))
    )
    normalized["rejected_proposal_ids"] = unique_texts(
        list_items(normalized.get("rejected_proposal_ids"))
    )
    normalized["supporting_opinion_ids"] = unique_texts(
        list_items(normalized.get("supporting_opinion_ids"))
    )
    normalized["rejected_opinion_ids"] = unique_texts(
        list_items(normalized.get("rejected_opinion_ids"))
    )
    normalized["council_input_counts"] = dict_items(
        normalized.get("council_input_counts")
    )
    normalized["recommended_next_skills"] = unique_texts(
        list_items(normalized.get("recommended_next_skills"))
    )
    normalized["warnings"] = list_items(normalized.get("warnings"))
    normalized["readiness_path"] = maybe_text(normalized.get("readiness_path"))
    normalized["output_path"] = (
        maybe_text(normalized.get("output_path"))
        or maybe_text(normalized.get("artifact_path"))
    )
    normalized["snapshot_id"] = maybe_text(
        normalized.get("snapshot_id")
    ) or maybe_text(normalized.get("gate_id"))
    return validate_canonical_payload("gate-state", normalized)

def gate_snapshot_row_from_payload(
    gate_payload: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_gate_snapshot_payload(gate_payload)
    return {
        "snapshot_id": maybe_text(normalized.get("snapshot_id")),
        "gate_id": maybe_text(normalized.get("gate_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "stage_name": maybe_text(normalized.get("stage_name")),
        "gate_handler": maybe_text(normalized.get("gate_handler")),
        "gate_status": maybe_text(normalized.get("gate_status")),
        "readiness_status": maybe_text(normalized.get("readiness_status")),
        "report_basis_freeze_allowed": 1 if bool(normalized.get("report_basis_freeze_allowed")) else 0,
        "decision_source": maybe_text(normalized.get("decision_source")),
        "report_basis_resolution_mode": maybe_text(
            normalized.get("report_basis_resolution_mode")
        ),
        "gate_reasons_json": json_text(normalized.get("gate_reasons", [])),
        "supporting_proposal_ids_json": json_text(
            normalized.get("supporting_proposal_ids", [])
        ),
        "rejected_proposal_ids_json": json_text(
            normalized.get("rejected_proposal_ids", [])
        ),
        "supporting_opinion_ids_json": json_text(
            normalized.get("supporting_opinion_ids", [])
        ),
        "rejected_opinion_ids_json": json_text(
            normalized.get("rejected_opinion_ids", [])
        ),
        "council_input_counts_json": json_text(
            normalized.get("council_input_counts", {})
        ),
        "recommended_next_skills_json": json_text(
            normalized.get("recommended_next_skills", [])
        ),
        "warnings_json": json_text(normalized.get("warnings", [])),
        "readiness_path": maybe_text(normalized.get("readiness_path")),
        "output_path": maybe_text(normalized.get("output_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }

def normalized_supervisor_snapshot_payload(
    supervisor_payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(supervisor_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["supervisor_id"] = maybe_text(
        normalized.get("supervisor_id")
    ) or supervisor_snapshot_object_id(
        normalized["run_id"],
        normalized["round_id"],
    )
    normalized["supervisor_status"] = (
        maybe_text(normalized.get("supervisor_status")) or "unavailable"
    )
    normalized["supervisor_substatus"] = (
        maybe_text(normalized.get("supervisor_substatus")) or "unclassified"
    )
    normalized["governed_execution_posture"] = (
        maybe_text(normalized.get("governed_execution_posture"))
        or normalized["supervisor_status"]
    )
    normalized["terminal_state"] = (
        maybe_text(normalized.get("terminal_state"))
        or normalized["governed_execution_posture"]
    )
    normalized["recovery_posture"] = (
        maybe_text(normalized.get("recovery_posture")) or "terminal"
    )
    normalized["operator_action"] = (
        maybe_text(normalized.get("operator_action")) or "inspect-runtime"
    )
    normalized["controller_status"] = (
        maybe_text(normalized.get("controller_status")) or "missing"
    )
    normalized["planning_mode"] = (
        maybe_text(normalized.get("planning_mode")) or "planner-backed"
    )
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "pending"
    )
    normalized["gate_status"] = (
        maybe_text(normalized.get("gate_status")) or "not-evaluated"
    )
    normalized["report_basis_status"] = (
        maybe_text(normalized.get("report_basis_status")) or "not-evaluated"
    )
    reporting_blockers = unique_texts(list_items(normalized.get("reporting_blockers")))
    reporting_ready = maybe_bool(normalized.get("reporting_ready"))
    if reporting_ready is None:
        reporting_ready = bool(
            reporting_gate_state(
                report_basis_status=normalized["report_basis_status"],
                readiness_status=normalized["readiness_status"],
                supervisor_status=normalized["supervisor_status"],
                require_supervisor=True,
                reporting_blockers_value=reporting_blockers,
                handoff_status=maybe_text(
                    normalized.get("reporting_handoff_status")
                )
                or maybe_text(normalized.get("handoff_status")),
            ).get("reporting_ready")
        )
    normalized["reporting_ready"] = bool(reporting_ready)
    normalized["reporting_handoff_status"] = normalize_reporting_handoff_status(
        maybe_text(normalized.get("reporting_handoff_status"))
        or maybe_text(normalized.get("handoff_status"))
    ) or ("reporting-ready" if normalized["reporting_ready"] else "investigation-open")
    normalized["resume_status"] = maybe_text(normalized.get("resume_status"))
    normalized["current_stage"] = maybe_text(normalized.get("current_stage"))
    normalized["failed_stage"] = maybe_text(normalized.get("failed_stage"))
    normalized["resume_recommended"] = bool(
        maybe_bool(normalized.get("resume_recommended"))
    )
    normalized["restart_recommended"] = bool(
        maybe_bool(normalized.get("restart_recommended"))
    )
    normalized["resume_from_stage"] = maybe_text(normalized.get("resume_from_stage"))
    normalized["reporting_blockers"] = reporting_blockers
    normalized["recommended_next_skills"] = unique_texts(
        list_items(normalized.get("recommended_next_skills"))
    )
    normalized["execution_policy"] = dict_items(normalized.get("execution_policy"))
    normalized["round_transition"] = dict_items(normalized.get("round_transition"))
    normalized["top_actions"] = list_items(normalized.get("top_actions"))
    normalized["operator_notes"] = list_items(normalized.get("operator_notes"))
    normalized["inspection_paths"] = dict_items(normalized.get("inspection_paths"))
    normalized["supervisor_path"] = (
        maybe_text(normalized.get("supervisor_path"))
        or maybe_text(normalized.get("artifact_path"))
    )
    normalized["snapshot_id"] = maybe_text(
        normalized.get("snapshot_id")
    ) or maybe_text(normalized.get("supervisor_id"))
    return validate_canonical_payload("supervisor-state", normalized)

def supervisor_snapshot_row_from_payload(
    supervisor_payload: dict[str, Any],
    *,
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_supervisor_snapshot_payload(supervisor_payload)
    return {
        "snapshot_id": maybe_text(normalized.get("snapshot_id")),
        "supervisor_id": maybe_text(normalized.get("supervisor_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "supervisor_status": maybe_text(normalized.get("supervisor_status")),
        "supervisor_substatus": maybe_text(normalized.get("supervisor_substatus")),
        "governed_execution_posture": maybe_text(normalized.get("governed_execution_posture")),
        "terminal_state": maybe_text(normalized.get("terminal_state")),
        "recovery_posture": maybe_text(normalized.get("recovery_posture")),
        "operator_action": maybe_text(normalized.get("operator_action")),
        "controller_status": maybe_text(normalized.get("controller_status")),
        "planning_mode": maybe_text(normalized.get("planning_mode")),
        "readiness_status": maybe_text(normalized.get("readiness_status")),
        "gate_status": maybe_text(normalized.get("gate_status")),
        "report_basis_status": maybe_text(normalized.get("report_basis_status")),
        "reporting_ready": 1 if bool(normalized.get("reporting_ready")) else 0,
        "reporting_handoff_status": maybe_text(
            normalized.get("reporting_handoff_status")
        ),
        "resume_status": maybe_text(normalized.get("resume_status")),
        "current_stage": maybe_text(normalized.get("current_stage")),
        "failed_stage": maybe_text(normalized.get("failed_stage")),
        "resume_recommended": 1 if bool(normalized.get("resume_recommended")) else 0,
        "restart_recommended": 1
        if bool(normalized.get("restart_recommended"))
        else 0,
        "resume_from_stage": maybe_text(normalized.get("resume_from_stage")),
        "reporting_blockers_json": json_text(
            normalized.get("reporting_blockers", [])
        ),
        "recommended_next_skills_json": json_text(
            normalized.get("recommended_next_skills", [])
        ),
        "execution_policy_json": json_text(normalized.get("execution_policy", {})),
        "round_transition_json": json_text(
            normalized.get("round_transition", {})
        ),
        "top_actions_json": json_text(normalized.get("top_actions", [])),
        "operator_notes_json": json_text(normalized.get("operator_notes", [])),
        "inspection_paths_json": json_text(
            normalized.get("inspection_paths", {})
        ),
        "artifact_path": maybe_text(normalized.get("supervisor_path"))
        or maybe_text(normalized.get("artifact_path")),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }

def resolved_runtime_control_freeze_artifacts(
    existing_record: dict[str, Any],
    *,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, str]:
    existing_artifacts = (
        existing_record.get("artifacts", {})
        if isinstance(existing_record.get("artifacts"), dict)
        else {}
    )
    controller_artifacts = (
        controller_snapshot.get("artifacts", {})
        if isinstance(controller_snapshot, dict)
        and isinstance(controller_snapshot.get("artifacts"), dict)
        else {}
    )
    supervisor_inspection = (
        supervisor_snapshot.get("inspection_paths", {})
        if isinstance(supervisor_snapshot, dict)
        and isinstance(supervisor_snapshot.get("inspection_paths"), dict)
        else {}
    )
    explicit = artifact_paths if isinstance(artifact_paths, dict) else {}
    gate_output_path = (
        maybe_text(gate_snapshot.get("output_path"))
        if isinstance(gate_snapshot, dict)
        else ""
    )
    supervisor_gate_path = (
        maybe_text(supervisor_snapshot.get("report_basis_gate_path"))
        if isinstance(supervisor_snapshot, dict)
        else ""
    )
    supervisor_path = (
        maybe_text(supervisor_snapshot.get("supervisor_path"))
        if isinstance(supervisor_snapshot, dict)
        else ""
    )
    report_basis_gate_artifact_path = (
        maybe_text(explicit.get("report_basis_gate_path"))
        or gate_output_path
        or maybe_text(controller_artifacts.get("report_basis_gate_path"))
        or supervisor_gate_path
        or maybe_text(supervisor_inspection.get("gate_path"))
        or maybe_text(existing_artifacts.get("report_basis_gate_path"))
    )
    return {
        "controller_state_path": maybe_text(explicit.get("controller_state_path"))
        or maybe_text(controller_artifacts.get("controller_state_path"))
        or maybe_text(existing_artifacts.get("controller_state_path")),
        "report_basis_gate_path": report_basis_gate_artifact_path,
        "supervisor_state_path": maybe_text(explicit.get("supervisor_state_path"))
        or supervisor_path
        or maybe_text(existing_artifacts.get("supervisor_state_path")),
    }

def merged_runtime_control_freeze_record(
    *,
    run_id: str,
    round_id: str,
    existing_record: dict[str, Any] | None = None,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = dict(existing_record) if isinstance(existing_record, dict) else {}
    normalized_run_id = maybe_text(run_id) or maybe_text(record.get("run_id"))
    normalized_round_id = maybe_text(round_id) or maybe_text(record.get("round_id"))
    record["schema_version"] = "runtime-control-freeze-v1"
    record["freeze_id"] = maybe_text(record.get("freeze_id")) or runtime_control_freeze_id(
        normalized_run_id,
        normalized_round_id,
    )
    record["run_id"] = normalized_run_id
    record["round_id"] = normalized_round_id
    if isinstance(controller_snapshot, dict) and controller_snapshot:
        record["controller_snapshot"] = normalized_controller_snapshot_payload(
            {
                **controller_snapshot,
                "run_id": maybe_text(controller_snapshot.get("run_id"))
                or normalized_run_id,
                "round_id": maybe_text(controller_snapshot.get("round_id"))
                or normalized_round_id,
            }
        )
    if isinstance(gate_snapshot, dict) and gate_snapshot:
        record["gate_snapshot"] = normalized_gate_snapshot_payload(
            {
                **gate_snapshot,
                "run_id": maybe_text(gate_snapshot.get("run_id"))
                or normalized_run_id,
                "round_id": maybe_text(gate_snapshot.get("round_id"))
                or normalized_round_id,
            }
        )
    if isinstance(supervisor_snapshot, dict) and supervisor_snapshot:
        record["supervisor_snapshot"] = normalized_supervisor_snapshot_payload(
            {
                **supervisor_snapshot,
                "run_id": maybe_text(supervisor_snapshot.get("run_id"))
                or normalized_run_id,
                "round_id": maybe_text(supervisor_snapshot.get("round_id"))
                or normalized_round_id,
            }
        )

    resolved_controller = (
        record.get("controller_snapshot", {})
        if isinstance(record.get("controller_snapshot"), dict)
        else {}
    )
    resolved_gate = (
        record.get("gate_snapshot", {})
        if isinstance(record.get("gate_snapshot"), dict)
        else {}
    )
    resolved_supervisor = (
        record.get("supervisor_snapshot", {})
        if isinstance(record.get("supervisor_snapshot"), dict)
        else {}
    )
    record["updated_at_utc"] = (
        maybe_text(resolved_supervisor.get("generated_at_utc"))
        or maybe_text(resolved_controller.get("generated_at_utc"))
        or maybe_text(resolved_gate.get("generated_at_utc"))
        or maybe_text(record.get("updated_at_utc"))
        or utc_now_iso()
    )
    record["gate_status"] = (
        maybe_text(resolved_supervisor.get("gate_status"))
        or maybe_text(resolved_controller.get("gate_status"))
        or maybe_text(resolved_gate.get("gate_status"))
        or maybe_text(record.get("gate_status"))
    )
    record["readiness_status"] = (
        maybe_text(resolved_supervisor.get("readiness_status"))
        or maybe_text(resolved_controller.get("readiness_status"))
        or maybe_text(resolved_gate.get("readiness_status"))
        or maybe_text(record.get("readiness_status"))
    )
    record["report_basis_status"] = (
        maybe_text(resolved_supervisor.get("report_basis_status"))
        or maybe_text(resolved_controller.get("report_basis_status"))
        or maybe_text(resolved_gate.get("report_basis_status"))
        or maybe_text(record.get("report_basis_status"))
    )
    record["controller_status"] = (
        maybe_text(resolved_controller.get("controller_status"))
        or maybe_text(record.get("controller_status"))
    )
    record["supervisor_status"] = (
        maybe_text(resolved_supervisor.get("supervisor_status"))
        or maybe_text(record.get("supervisor_status"))
    )
    record["planning_mode"] = (
        maybe_text(resolved_supervisor.get("planning_mode"))
        or maybe_text(resolved_controller.get("planning_mode"))
        or maybe_text(
            resolved_controller.get("planning", {}).get("planning_mode")
            if isinstance(resolved_controller.get("planning"), dict)
            else ""
        )
        or maybe_text(resolved_gate.get("planning_mode"))
        or maybe_text(record.get("planning_mode"))
        or "planner-backed"
    )
    gate_present = isinstance(resolved_gate, dict) and bool(resolved_gate)
    report_basis_freeze_allowed = (
        bool(resolved_gate.get("report_basis_freeze_allowed"))
        if gate_present
        else bool(record.get("report_basis_freeze_allowed"))
    )
    if record["gate_status"] == "report-basis-freeze-allowed":
        report_basis_freeze_allowed = True
    record["report_basis_freeze_allowed"] = report_basis_freeze_allowed
    record["report_basis_gate_status"] = (
        maybe_text(resolved_gate.get("report_basis_gate_status"))
        or maybe_text(record.get("report_basis_gate_status"))
        or (
            "report-basis-freeze-allowed"
            if report_basis_freeze_allowed
            else "report-basis-freeze-withheld"
        )
    )
    record["gate_reasons"] = unique_texts(
        (
            resolved_supervisor.get("gate_reasons", [])
            if isinstance(resolved_supervisor.get("gate_reasons"), list)
            else []
        )
        + (
            resolved_controller.get("gate_reasons", [])
            if isinstance(resolved_controller.get("gate_reasons"), list)
            else []
        )
        + (
            resolved_gate.get("gate_reasons", [])
            if isinstance(resolved_gate.get("gate_reasons"), list)
            else []
        )
        + (
            record.get("gate_reasons", [])
            if isinstance(record.get("gate_reasons"), list)
            else []
        )
    )
    record["recommended_next_skills"] = unique_texts(
        (
            resolved_supervisor.get("recommended_next_skills", [])
            if isinstance(resolved_supervisor.get("recommended_next_skills"), list)
            else []
        )
        + (
            resolved_controller.get("recommended_next_skills", [])
            if isinstance(resolved_controller.get("recommended_next_skills"), list)
            else []
        )
        + (
            resolved_gate.get("recommended_next_skills", [])
            if isinstance(resolved_gate.get("recommended_next_skills"), list)
            else []
        )
        + (
            record.get("recommended_next_skills", [])
            if isinstance(record.get("recommended_next_skills"), list)
            else []
        )
    )
    report_ready_value = maybe_bool(resolved_supervisor.get("reporting_ready"))
    if report_ready_value is None:
        report_ready_value = maybe_bool(record.get("reporting_ready"))
    record["reporting_ready"] = bool(report_ready_value)
    record["reporting_handoff_status"] = normalize_reporting_handoff_status(
        maybe_text(resolved_supervisor.get("reporting_handoff_status"))
        or maybe_text(resolved_supervisor.get("handoff_status"))
        or maybe_text(record.get("reporting_handoff_status"))
    ) or ("reporting-ready" if record["reporting_ready"] else "investigation-open")
    record["reporting_blockers"] = unique_texts(
        (
            resolved_supervisor.get("reporting_blockers", [])
            if isinstance(resolved_supervisor.get("reporting_blockers"), list)
            else []
        )
        + (
            record.get("reporting_blockers", [])
            if isinstance(record.get("reporting_blockers"), list)
            else []
        )
    )
    record["artifacts"] = resolved_runtime_control_freeze_artifacts(
        record,
        controller_snapshot=resolved_controller,
        gate_snapshot=resolved_gate,
        supervisor_snapshot=resolved_supervisor,
        artifact_paths=artifact_paths,
    )
    return validate_canonical_payload("runtime-control-freeze", record)

def store_runtime_control_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    controller_snapshot: dict[str, Any] | None = None,
    gate_snapshot: dict[str, Any] | None = None,
    supervisor_snapshot: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            existing_record = fetch_runtime_control_freeze(
                connection,
                run_id=run_id,
                round_id=round_id,
            )
            freeze_record = merged_runtime_control_freeze_record(
                run_id=run_id,
                round_id=round_id,
                existing_record=existing_record,
                controller_snapshot=controller_snapshot,
                gate_snapshot=gate_snapshot,
                supervisor_snapshot=supervisor_snapshot,
                artifact_paths=artifact_paths,
            )
            write_report_basis_freeze_row(
                connection,
                runtime_control_freeze_row_from_payload(freeze_record),
            )
            normalized_controller = (
                freeze_record.get("controller_snapshot", {})
                if isinstance(freeze_record.get("controller_snapshot"), dict)
                else {}
            )
            normalized_gate = (
                freeze_record.get("gate_snapshot", {})
                if isinstance(freeze_record.get("gate_snapshot"), dict)
                else {}
            )
            normalized_supervisor = (
                freeze_record.get("supervisor_snapshot", {})
                if isinstance(freeze_record.get("supervisor_snapshot"), dict)
                else {}
            )
            if normalized_controller:
                write_controller_snapshot_row(
                    connection,
                    controller_snapshot_row_from_payload(normalized_controller),
                )
            if normalized_gate:
                write_gate_snapshot_row(
                    connection,
                    gate_snapshot_row_from_payload(normalized_gate),
                )
            if normalized_supervisor:
                write_supervisor_snapshot_row(
                    connection,
                    supervisor_snapshot_row_from_payload(normalized_supervisor),
                )
    finally:
        connection.close()
    return freeze_record

def load_moderator_work_surface(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    next_action_rows = load_moderator_action_records(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    next_actions_snapshot = load_moderator_action_snapshot(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    next_actions = build_moderator_action_payload(
        next_action_rows,
        snapshot_payload=next_actions_snapshot,
        run_id=run_id,
        round_id=round_id,
    ) if next_action_rows or isinstance(next_actions_snapshot, dict) else {}
    probe_rows = load_falsification_probe_records(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    probes_snapshot = load_falsification_probe_snapshot(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    probes = build_falsification_probe_payload(
        probe_rows,
        snapshot_payload=probes_snapshot,
        run_id=run_id,
        round_id=round_id,
    ) if probe_rows or isinstance(probes_snapshot, dict) else {}
    round_tasks = load_round_task_snapshot(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    return {
        "next_actions": next_actions,
        "probes": probes,
        "round_tasks": round_tasks,
    }

def load_runtime_control_freeze_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_runtime_control_freeze(
            connection,
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_controller_snapshot_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return latest_json_row(
            connection,
            table_name="controller_snapshots",
            id_column="snapshot_id",
            timestamp_column="generated_at_utc",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_gate_snapshot_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    stage_name: str = "",
    gate_handler: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    filters = {
        "run_id": run_id,
        "round_id": round_id,
        "stage_name": stage_name,
        "gate_handler": gate_handler,
    }
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        if maybe_text(stage_name) or maybe_text(gate_handler):
            return latest_json_row_where(
                connection,
                table_name="gate_snapshots",
                id_column="snapshot_id",
                timestamp_column="generated_at_utc",
                filters=filters,
            )
        return latest_json_row(
            connection,
            table_name="gate_snapshots",
            id_column="snapshot_id",
            timestamp_column="generated_at_utc",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_supervisor_snapshot_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return latest_json_row(
            connection,
            table_name="supervisor_snapshots",
            id_column="snapshot_id",
            timestamp_column="generated_at_utc",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_governed_execution_control_state(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    runtime_control_freeze_record = load_runtime_control_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    readiness_record = load_round_readiness_assessment(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    report_basis_freeze_record = load_report_basis_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    reporting_handoff_record = load_reporting_handoff_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    decision_draft_record = load_council_decision_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
        db_path=db_path,
    ) or {}
    decision_record = load_council_decision_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="canonical",
        db_path=db_path,
    ) or {}
    expert_report_drafts = {
        role: (
            load_expert_report_record(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                report_stage="draft",
                agent_role=role,
                db_path=db_path,
            )
            or {}
        )
        for role in REPORT_AGENT_ROLES
    }
    expert_reports = {
        role: (
            load_expert_report_record(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                report_stage="canonical",
                agent_role=role,
                db_path=db_path,
            )
            or {}
        )
        for role in REPORT_AGENT_ROLES
    }
    final_publication_record = load_final_publication_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    controller_record = load_controller_snapshot_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    gate_record = load_gate_snapshot_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    supervisor_record = load_supervisor_snapshot_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    ) or {}
    orchestration_plan_record = load_orchestration_plan_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_path=(
            maybe_text(controller_record.get("planning", {}).get("plan_path"))
            if isinstance(controller_record.get("planning"), dict)
            else ""
        ),
        db_path=db_path,
    ) or {}
    orchestration_plan_steps = (
        load_orchestration_plan_steps(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            plan_id=maybe_text(orchestration_plan_record.get("plan_id")),
            db_path=db_path,
        )
        if orchestration_plan_record
        else []
    )
    resolved_gate_record = gate_record or (
        runtime_control_freeze_record.get("gate_snapshot", {})
        if isinstance(runtime_control_freeze_record.get("gate_snapshot"), dict)
        else {}
    )
    return {
        "orchestration_plan": orchestration_plan_record,
        "orchestration_plan_steps": orchestration_plan_steps,
        "runtime_control_freeze": runtime_control_freeze_record,
        "round_readiness": readiness_record,
        "report_basis_freeze": report_basis_freeze_record,
        "reporting_handoff": reporting_handoff_record,
        "decision_draft": decision_draft_record,
        "decision": decision_record,
        "expert_report_drafts": expert_report_drafts,
        "expert_reports": expert_reports,
        "final_publication": final_publication_record,
        "controller": controller_record
        or (
            runtime_control_freeze_record.get("controller_snapshot", {})
            if isinstance(runtime_control_freeze_record.get("controller_snapshot"), dict)
            else {}
        ),
        "report_basis_gate": resolved_gate_record,
        "supervisor": supervisor_record
        or (
            runtime_control_freeze_record.get("supervisor_snapshot", {})
            if isinstance(runtime_control_freeze_record.get("supervisor_snapshot"), dict)
            else {}
        ),
    }

def orchestration_plan_object_id(
    run_id: str,
    round_id: str,
    plan_source: str,
    artifact_path: str,
) -> str:
    return "orchestration-plan-" + stable_hash(
        "orchestration-plan",
        run_id,
        round_id,
        plan_source,
        artifact_path,
    )[:12]

def orchestration_plan_step_object_id(
    plan_id: str,
    plan_step_group: str,
    step_index: int,
    stage_name: str,
    skill_name: str,
) -> str:
    return "orchestration-step-" + stable_hash(
        "orchestration-plan-step",
        plan_id,
        plan_step_group,
        step_index,
        stage_name,
        skill_name,
    )[:12]

def round_task_snapshot_id(run_id: str, round_id: str) -> str:
    return "round-tasks-" + stable_hash("round-task-snapshot", run_id, round_id)[:12]

def planning_source_from_runtime_plan(plan_payload: dict[str, Any]) -> str:
    explicit_source = maybe_text(plan_payload.get("plan_source"))
    if explicit_source:
        return explicit_source
    return "runtime-planner"

def normalized_orchestration_plan_payload(
    plan_payload: dict[str, Any],
    *,
    artifact_path: str = "",
) -> dict[str, Any]:
    normalized = dict(plan_payload)
    normalized["run_id"] = maybe_text(normalized.get("run_id"))
    normalized["round_id"] = maybe_text(normalized.get("round_id"))
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["planning_status"] = (
        maybe_text(normalized.get("planning_status")) or "ready-for-controller"
    )
    normalized["planning_mode"] = (
        maybe_text(normalized.get("planning_mode")) or "planner-backed-governed-execution"
    )
    normalized["controller_authority"] = (
        maybe_text(normalized.get("controller_authority"))
        or "queue-owner"
    )
    normalized["plan_source"] = planning_source_from_runtime_plan(normalized)
    normalized["council_execution_mode"] = maybe_text(
        normalized.get("council_execution_mode")
    )
    normalized["downstream_posture"] = (
        maybe_text(normalized.get("downstream_posture"))
        or "hold-investigation-open"
    )
    normalized["probe_stage_included"] = bool(
        maybe_bool(normalized.get("probe_stage_included"))
    )
    normalized["assigned_role_hints"] = unique_texts(
        list_items(normalized.get("assigned_role_hints"))
    )
    normalized["phase_decision_basis"] = dict_items(
        normalized.get("phase_decision_basis")
    )
    normalized["agent_turn_hints"] = dict_items(normalized.get("agent_turn_hints"))
    normalized["observed_state"] = dict_items(normalized.get("observed_state"))
    normalized["inputs"] = dict_items(normalized.get("inputs"))
    normalized["execution_queue"] = [
        dict(item)
        for item in list_items(normalized.get("execution_queue"))
        if isinstance(item, dict)
    ]
    normalized["gate_steps"] = [
        dict(item)
        for item in list_items(normalized.get("gate_steps"))
        if isinstance(item, dict)
    ]
    normalized["derived_exports"] = [
        dict(item)
        for item in list_items(normalized.get("derived_exports"))
        if isinstance(item, dict)
    ]
    normalized["post_gate_steps"] = [
        dict(item)
        for item in list_items(normalized.get("post_gate_steps"))
        if isinstance(item, dict)
    ]
    normalized["stop_conditions"] = [
        dict(item)
        for item in list_items(normalized.get("stop_conditions"))
        if isinstance(item, dict)
    ]
    normalized["fallback_path"] = [
        dict(item)
        for item in list_items(normalized.get("fallback_path"))
        if isinstance(item, dict)
    ]
    normalized["planning_notes"] = [
        maybe_text(item)
        for item in list_items(normalized.get("planning_notes"))
        if maybe_text(item)
    ]
    normalized["deliberation_sync"] = dict_items(normalized.get("deliberation_sync"))
    resolved_artifact_path = (
        maybe_text(artifact_path)
        or maybe_text(normalized.get("artifact_path"))
        or maybe_text(normalized.get("output_path"))
    )
    normalized["artifact_path"] = resolved_artifact_path
    normalized["step_counts"] = {
        "execution_queue_count": len(normalized["execution_queue"]),
        "gate_step_count": len(normalized["gate_steps"]),
        "derived_export_count": len(normalized["derived_exports"]),
        "post_gate_step_count": len(normalized["post_gate_steps"]),
        "planned_stage_count": len(normalized["execution_queue"])
        + len(normalized["gate_steps"])
        + len(normalized["derived_exports"])
        + len(normalized["post_gate_steps"]),
    }
    normalized["plan_id"] = maybe_text(normalized.get("plan_id")) or (
        orchestration_plan_object_id(
            normalized["run_id"],
            normalized["round_id"],
            normalized["plan_source"],
            normalized["artifact_path"],
        )
    )
    return validate_canonical_payload("orchestration-plan", normalized)

def normalized_orchestration_plan_step_payload(
    step_payload: dict[str, Any],
    *,
    plan_id: str,
    run_id: str,
    round_id: str,
    generated_at_utc: str,
    planning_mode: str,
    controller_authority: str,
    plan_source: str,
    plan_step_group: str,
    step_index: int,
    artifact_path: str,
) -> dict[str, Any]:
    normalized = dict(step_payload)
    skill_name = maybe_text(normalized.get("skill_name"))
    stage_name = (
        maybe_text(normalized.get("stage_name"))
        or maybe_text(normalized.get("stage"))
        or skill_name
        or maybe_text(normalized.get("gate_handler"))
    )
    default_stage_kind = "gate" if plan_step_group == "gate-step" else "skill"
    stage_kind = (
        maybe_text(normalized.get("stage_kind") or normalized.get("kind"))
        or default_stage_kind
    )
    default_phase_group = (
        "gate"
        if plan_step_group == "gate-step"
        else "exports"
        if plan_step_group == "derived-export"
        else "execution"
    )
    expected_output_path = (
        maybe_text(normalized.get("expected_output_path"))
        or maybe_text(normalized.get("output_path"))
        or artifact_path
    )
    required_previous_stages = [
        maybe_text(value)
        for value in list_items(normalized.get("required_previous_stages"))
        if maybe_text(value)
    ]
    skill_args = [
        maybe_text(value)
        for value in list_items(normalized.get("skill_args"))
        if maybe_text(value)
    ]
    blocking_value = maybe_bool(normalized.get("blocking"))
    if blocking_value is None:
        blocking_value = plan_step_group != "derived-export"
    required_for_controller_value = maybe_bool(
        normalized.get("required_for_controller")
    )
    if required_for_controller_value is None:
        required_for_controller_value = plan_step_group != "derived-export"
    normalized.update(
        {
            "run_id": run_id,
            "round_id": round_id,
            "plan_id": plan_id,
            "generated_at_utc": generated_at_utc,
            "plan_step_group": plan_step_group,
            "planning_mode": planning_mode,
            "controller_authority": controller_authority,
            "plan_source": plan_source,
            "phase_group": maybe_text(normalized.get("phase_group"))
            or default_phase_group,
            "stage_name": stage_name,
            "stage_kind": stage_kind,
            "skill_name": skill_name,
            "expected_skill_name": maybe_text(normalized.get("expected_skill_name"))
            or skill_name,
            "assigned_role_hint": maybe_text(normalized.get("assigned_role_hint")),
            "blocking": bool(blocking_value),
            "resume_policy": maybe_text(normalized.get("resume_policy"))
            or "skip-if-completed",
            "gate_handler": maybe_text(normalized.get("gate_handler")),
            "readiness_stage_name": maybe_text(
                normalized.get("readiness_stage_name")
            ),
            "reason": maybe_text(normalized.get("reason")),
            "operator_summary": maybe_text(normalized.get("operator_summary")),
            "expected_output_path": expected_output_path,
            "required_for_controller": bool(required_for_controller_value),
            "export_mode": maybe_text(normalized.get("export_mode")),
            "required_previous_stages": required_previous_stages,
            "skill_args": skill_args,
            "artifact_path": artifact_path,
        }
    )
    normalized["step_id"] = maybe_text(normalized.get("step_id")) or (
        orchestration_plan_step_object_id(
            plan_id,
            plan_step_group,
            step_index,
            stage_name,
            skill_name,
        )
    )
    return validate_canonical_payload("orchestration-plan-step", normalized)

def orchestration_plan_row_from_payload(
    plan_payload: dict[str, Any],
    *,
    artifact_path: str = "",
    record_locator: str = "$",
) -> dict[str, Any]:
    normalized = normalized_orchestration_plan_payload(
        plan_payload,
        artifact_path=artifact_path,
    )
    step_counts = (
        normalized.get("step_counts", {})
        if isinstance(normalized.get("step_counts"), dict)
        else {}
    )
    return {
        "plan_id": maybe_text(normalized.get("plan_id")),
        "run_id": maybe_text(normalized.get("run_id")),
        "round_id": maybe_text(normalized.get("round_id")),
        "generated_at_utc": maybe_text(normalized.get("generated_at_utc")),
        "planning_status": maybe_text(normalized.get("planning_status")),
        "planning_mode": maybe_text(normalized.get("planning_mode")),
        "controller_authority": maybe_text(
            normalized.get("controller_authority")
        ),
        "plan_source": maybe_text(normalized.get("plan_source")),
        "council_execution_mode": maybe_text(
            normalized.get("council_execution_mode")
        ),
        "downstream_posture": maybe_text(normalized.get("downstream_posture")),
        "probe_stage_included": 1
        if bool(normalized.get("probe_stage_included"))
        else 0,
        "artifact_path": maybe_text(normalized.get("artifact_path")),
        "execution_queue_count": coerce_int(
            step_counts.get("execution_queue_count")
        ),
        "gate_step_count": coerce_int(step_counts.get("gate_step_count")),
        "derived_export_count": coerce_int(step_counts.get("derived_export_count")),
        "post_gate_step_count": coerce_int(
            step_counts.get("post_gate_step_count")
        ),
        "planned_stage_count": coerce_int(step_counts.get("planned_stage_count")),
        "assigned_role_hints_json": json_text(
            normalized.get("assigned_role_hints", [])
        ),
        "phase_decision_basis_json": json_text(
            normalized.get("phase_decision_basis", {})
        ),
        "agent_turn_hints_json": json_text(
            normalized.get("agent_turn_hints", {})
        ),
        "observed_state_json": json_text(normalized.get("observed_state", {})),
        "inputs_json": json_text(normalized.get("inputs", {})),
        "execution_queue_json": json_text(normalized.get("execution_queue", [])),
        "gate_steps_json": json_text(normalized.get("gate_steps", [])),
        "derived_exports_json": json_text(normalized.get("derived_exports", [])),
        "post_gate_steps_json": json_text(normalized.get("post_gate_steps", [])),
        "stop_conditions_json": json_text(normalized.get("stop_conditions", [])),
        "fallback_path_json": json_text(normalized.get("fallback_path", [])),
        "planning_notes_json": json_text(normalized.get("planning_notes", [])),
        "deliberation_sync_json": json_text(
            normalized.get("deliberation_sync", {})
        ),
        "step_counts_json": json_text(step_counts),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(normalized),
    }

def iter_orchestration_plan_step_rows(
    plan_payload: dict[str, Any],
    *,
    artifact_path: str = "",
) -> list[dict[str, Any]]:
    normalized = normalized_orchestration_plan_payload(
        plan_payload,
        artifact_path=artifact_path,
    )
    rows: list[dict[str, Any]] = []
    sections = (
        ("execution_queue", "execution-queue"),
        ("gate_steps", "gate-step"),
        ("derived_exports", "derived-export"),
        ("post_gate_steps", "post-gate-step"),
    )
    for section_key, step_group in sections:
        steps = (
            normalized.get(section_key, [])
            if isinstance(normalized.get(section_key), list)
            else []
        )
        for step_index, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            step_payload = normalized_orchestration_plan_step_payload(
                step,
                plan_id=maybe_text(normalized.get("plan_id")),
                run_id=maybe_text(normalized.get("run_id")),
                round_id=maybe_text(normalized.get("round_id")),
                generated_at_utc=maybe_text(normalized.get("generated_at_utc")),
                planning_mode=maybe_text(normalized.get("planning_mode")),
                controller_authority=maybe_text(
                    normalized.get("controller_authority")
                ),
                plan_source=maybe_text(normalized.get("plan_source")),
                plan_step_group=step_group,
                step_index=step_index,
                artifact_path=maybe_text(normalized.get("artifact_path")),
            )
            rows.append(
                {
                    "step_id": maybe_text(step_payload.get("step_id")),
                    "plan_id": maybe_text(step_payload.get("plan_id")),
                    "run_id": maybe_text(step_payload.get("run_id")),
                    "round_id": maybe_text(step_payload.get("round_id")),
                    "generated_at_utc": maybe_text(
                        step_payload.get("generated_at_utc")
                    ),
                    "plan_step_group": maybe_text(
                        step_payload.get("plan_step_group")
                    ),
                    "step_index": step_index,
                    "planning_mode": maybe_text(
                        step_payload.get("planning_mode")
                    ),
                    "controller_authority": maybe_text(
                        step_payload.get("controller_authority")
                    ),
                    "plan_source": maybe_text(step_payload.get("plan_source")),
                    "phase_group": maybe_text(step_payload.get("phase_group")),
                    "stage_name": maybe_text(step_payload.get("stage_name")),
                    "stage_kind": maybe_text(step_payload.get("stage_kind")),
                    "skill_name": maybe_text(step_payload.get("skill_name")),
                    "expected_skill_name": maybe_text(
                        step_payload.get("expected_skill_name")
                    ),
                    "assigned_role_hint": maybe_text(
                        step_payload.get("assigned_role_hint")
                    ),
                    "blocking": 1 if bool(step_payload.get("blocking")) else 0,
                    "resume_policy": maybe_text(step_payload.get("resume_policy")),
                    "gate_handler": maybe_text(step_payload.get("gate_handler")),
                    "readiness_stage_name": maybe_text(
                        step_payload.get("readiness_stage_name")
                    ),
                    "reason": maybe_text(step_payload.get("reason")),
                    "operator_summary": maybe_text(
                        step_payload.get("operator_summary")
                    ),
                    "expected_output_path": maybe_text(
                        step_payload.get("expected_output_path")
                    ),
                    "required_for_controller": 1
                    if bool(step_payload.get("required_for_controller"))
                    else 0,
                    "export_mode": maybe_text(step_payload.get("export_mode")),
                    "required_previous_stages_json": json_text(
                        step_payload.get("required_previous_stages", [])
                    ),
                    "skill_args_json": json_text(
                        step_payload.get("skill_args", [])
                    ),
                    "artifact_path": maybe_text(step_payload.get("artifact_path")),
                    "record_locator": f"$.{section_key}[{step_index}]",
                    "raw_json": json_text(step_payload),
                }
            )
    return rows

def round_task_snapshot_row_from_payload(
    snapshot_payload: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str = "$",
) -> dict[str, Any]:
    return {
        "snapshot_id": maybe_text(snapshot_payload.get("snapshot_id")),
        "run_id": maybe_text(snapshot_payload.get("run_id")),
        "round_id": maybe_text(snapshot_payload.get("round_id")),
        "generated_at_utc": maybe_text(snapshot_payload.get("generated_at_utc")),
        "task_source": maybe_text(snapshot_payload.get("task_source"))
        or "round-tasks-artifact",
        "task_count": coerce_int(
            snapshot_payload.get("task_count")
            or (
                len(snapshot_payload.get("tasks", []))
                if isinstance(snapshot_payload.get("tasks"), list)
                else 0
            )
        ),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator) or "$",
        "raw_json": json_text(snapshot_payload),
    }

def store_round_task_snapshot(
    run_dir: str | Path,
    *,
    task_snapshot: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    snapshot_payload = dict(task_snapshot) if isinstance(task_snapshot, dict) else {}
    run_id = maybe_text(snapshot_payload.get("run_id"))
    round_id = maybe_text(snapshot_payload.get("round_id"))
    snapshot_payload["snapshot_id"] = (
        maybe_text(snapshot_payload.get("snapshot_id"))
        or round_task_snapshot_id(run_id, round_id)
    )
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_round_task_snapshot_row(
                connection,
                round_task_snapshot_row_from_payload(
                    snapshot_payload,
                    artifact_path=artifact_path,
                ),
            )
    finally:
        connection.close()
    return snapshot_payload

def load_round_task_snapshot(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        return fetch_snapshot_payload(
            connection,
            table_name="round_task_snapshots",
            run_id=run_id,
            round_id=round_id,
        )
    finally:
        connection.close()

def load_orchestration_plan_record(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    artifact_path: str = "",
    controller_authority: str = "",
    allow_latest_fallback: bool = True,
    db_path: str = "",
) -> dict[str, Any] | None:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        normalized_run_id = maybe_text(run_id)
        normalized_round_id = maybe_text(round_id)
        if not normalized_run_id and not normalized_round_id:
            return None
        normalized_artifact_path = maybe_text(artifact_path)
        normalized_controller_authority = maybe_text(controller_authority)
        if normalized_artifact_path:
            record = latest_json_row_where(
                connection,
                table_name="orchestration_plans",
                id_column="plan_id",
                timestamp_column="generated_at_utc",
                filters={
                    "run_id": normalized_run_id,
                    "round_id": normalized_round_id,
                    "artifact_path": normalized_artifact_path,
                },
            )
            if record is not None:
                return record
        if normalized_controller_authority:
            record = latest_json_row_where(
                connection,
                table_name="orchestration_plans",
                id_column="plan_id",
                timestamp_column="generated_at_utc",
                filters={
                    "run_id": normalized_run_id,
                    "round_id": normalized_round_id,
                    "controller_authority": normalized_controller_authority,
                },
            )
            if record is not None:
                return record
        if not allow_latest_fallback:
            return None
        return latest_json_row_where(
            connection,
            table_name="orchestration_plans",
            id_column="plan_id",
            timestamp_column="generated_at_utc",
            filters={
                "run_id": normalized_run_id,
                "round_id": normalized_round_id,
            },
        )
    finally:
        connection.close()

def load_orchestration_plan_steps(
    run_dir: str | Path,
    *,
    run_id: str = "",
    round_id: str = "",
    plan_id: str = "",
    plan_step_group: str = "",
    stage_name: str = "",
    skill_name: str = "",
    db_path: str = "",
) -> list[dict[str, Any]]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, _db_file = connect_db(run_dir_path, db_path)
    try:
        clauses: list[str] = []
        params: list[str] = []
        for column_name, value in (
            ("run_id", run_id),
            ("round_id", round_id),
            ("plan_id", plan_id),
            ("plan_step_group", plan_step_group),
            ("stage_name", stage_name),
            ("skill_name", skill_name),
        ):
            text = maybe_text(value)
            if not text:
                continue
            clauses.append(f"{column_name} = ?")
            params.append(text)
        if not clauses:
            return []
        rows = connection.execute(
            f"""
            SELECT *
            FROM orchestration_plan_steps
            WHERE {' AND '.join(clauses)}
            ORDER BY generated_at_utc DESC, plan_step_group, step_index, step_id
            """,
            tuple(params),
        ).fetchall()
    finally:
        connection.close()
    return [payload_from_db_row(row) for row in rows]

def store_orchestration_plan_record(
    run_dir: str | Path,
    *,
    plan_payload: dict[str, Any],
    artifact_path: str = "",
    run_id: str = "",
    round_id: str = "",
    controller_authority: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    resolved_payload = dict(plan_payload)
    resolved_payload["run_id"] = (
        maybe_text(resolved_payload.get("run_id")) or maybe_text(run_id)
    )
    resolved_payload["round_id"] = (
        maybe_text(resolved_payload.get("round_id")) or maybe_text(round_id)
    )
    if not maybe_text(resolved_payload.get("controller_authority")):
        resolved_payload["controller_authority"] = maybe_text(controller_authority)
    row = orchestration_plan_row_from_payload(
        resolved_payload,
        artifact_path=artifact_path,
    )
    step_rows = iter_orchestration_plan_step_rows(
        resolved_payload,
        artifact_path=maybe_text(row.get("artifact_path")),
    )
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            write_orchestration_plan_row(connection, row)
            connection.execute(
                "DELETE FROM orchestration_plan_steps WHERE plan_id = ?",
                (maybe_text(row.get("plan_id")),),
            )
            for step_row in step_rows:
                write_orchestration_plan_step_row(connection, step_row)
    finally:
        connection.close()
    return {
        "status": "completed",
        "run_id": maybe_text(row.get("run_id")),
        "round_id": maybe_text(row.get("round_id")),
        "plan_id": maybe_text(row.get("plan_id")),
        "artifact_path": maybe_text(row.get("artifact_path")),
        "db_path": str(db_file),
        "planned_stage_count": coerce_int(row.get("planned_stage_count")),
        "step_row_count": len(step_rows),
    }


__all__ = [
    "runtime_control_freeze_id",
    "controller_snapshot_object_id",
    "gate_snapshot_object_id",
    "supervisor_snapshot_object_id",
    "runtime_control_freeze_row_from_payload",
    "control_snapshot_row_id",
    "normalized_controller_snapshot_payload",
    "controller_snapshot_row_from_payload",
    "normalized_gate_snapshot_payload",
    "gate_snapshot_row_from_payload",
    "normalized_supervisor_snapshot_payload",
    "supervisor_snapshot_row_from_payload",
    "resolved_runtime_control_freeze_artifacts",
    "merged_runtime_control_freeze_record",
    "store_runtime_control_freeze_record",
    "load_moderator_work_surface",
    "load_runtime_control_freeze_record",
    "load_controller_snapshot_record",
    "load_gate_snapshot_record",
    "load_supervisor_snapshot_record",
    "load_governed_execution_control_state",
    "orchestration_plan_object_id",
    "orchestration_plan_step_object_id",
    "round_task_snapshot_id",
    "planning_source_from_runtime_plan",
    "normalized_orchestration_plan_payload",
    "normalized_orchestration_plan_step_payload",
    "orchestration_plan_row_from_payload",
    "iter_orchestration_plan_step_rows",
    "round_task_snapshot_row_from_payload",
    "store_round_task_snapshot",
    "load_round_task_snapshot",
    "load_orchestration_plan_record",
    "load_orchestration_plan_steps",
    "store_orchestration_plan_record",
]
