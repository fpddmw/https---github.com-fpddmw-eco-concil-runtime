from __future__ import annotations

import hashlib
from pathlib import Path
import re
from typing import Any

from .deliberation_plane import store_promotion_freeze_record
from .executor import SkillExecutionError
from .controller import run_phase2_round, run_phase2_round_with_contract_mode
from .executor import maybe_text, new_runtime_event_id, utc_now_iso
from .ledger import append_ledger_event
from .manifest import load_json_if_exists, write_json
from .paths import supervisor_state_path
from .source_queue_history import discovered_round_ids

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
OPEN_ROUND_SCRIPT = WORKSPACE_ROOT / "skills" / "eco-open-investigation-round" / "scripts" / "eco_open_investigation_round.py"
ROUND_SUFFIX_PATTERN = re.compile(r"^(?P<prefix>.*?)(?P<number>\d+)$")


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


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


def top_actions(next_actions: dict[str, Any]) -> list[dict[str, str]]:
    ranked_actions = next_actions.get("ranked_actions", []) if isinstance(next_actions.get("ranked_actions"), list) else []
    results: list[dict[str, str]] = []
    for action in ranked_actions[:3]:
        if not isinstance(action, dict):
            continue
        results.append(
            {
                "action_id": maybe_text(action.get("action_id")),
                "action_kind": maybe_text(action.get("action_kind")),
                "assigned_role": maybe_text(action.get("assigned_role")),
                "priority": maybe_text(action.get("priority")),
                "objective": maybe_text(action.get("objective")),
            }
        )
    return results


def operator_notes(*, promotion_status: str, gate_status: str, gate_reasons: list[Any], top_action_rows: list[dict[str, str]]) -> list[str]:
    if promotion_status == "promoted":
        return [
            "Round promotion succeeded and the evidence basis is now ready for downstream reporting.",
            "No blocking board or probe objects remain in the current controller snapshot.",
        ]
    notes = [f"Promotion is withheld because gate={gate_status}."]
    notes.extend(maybe_text(reason) for reason in gate_reasons if maybe_text(reason))
    if top_action_rows:
        top_action_kind = top_action_rows[0].get("action_kind") or "unspecified"
        notes.append(f"Highest-priority follow-up remains {top_action_kind}.")
    return notes[:4]


def operator_commands(*, run_id: str, round_id: str, run_dir: Path) -> dict[str, str]:
    base = f"--run-dir {run_dir} --run-id {run_id} --round-id {round_id}"
    return {
        "resume_command": f"resume-phase2-round {base}",
        "restart_command": f"restart-phase2-round {base}",
        "inspect_command": f"show-run-state --run-dir {run_dir} --round-id {round_id} --tail 20",
    }


def increment_round_id(round_id: str) -> str:
    normalized = maybe_text(round_id)
    match = ROUND_SUFFIX_PATTERN.match(normalized)
    if match is None:
        return f"{normalized}-next"
    prefix = maybe_text(match.group("prefix"))
    number_text = maybe_text(match.group("number"))
    return f"{prefix}{int(number_text) + 1:0{len(number_text)}d}"


def suggest_next_round_id(run_dir: Path, current_round_id: str) -> str:
    observed = set(discovered_round_ids(run_dir))
    candidate = increment_round_id(current_round_id)
    while candidate in observed:
        candidate = increment_round_id(candidate)
    return candidate


def round_transition_hint(run_dir: Path, *, run_id: str, round_id: str, supervisor_status: str) -> dict[str, str]:
    if supervisor_status != "hold-investigation-open":
        return {}
    suggested_round_id = suggest_next_round_id(run_dir, round_id)
    return {
        "skill_name": "eco-open-investigation-round",
        "source_round_id": round_id,
        "suggested_round_id": suggested_round_id,
        "command": (
            f"python3 {OPEN_ROUND_SCRIPT} --run-dir {run_dir} --run-id {run_id} "
            f"--round-id {suggested_round_id} --source-round-id {round_id}"
        ),
    }


def classify_supervisor_state(controller: dict[str, Any]) -> dict[str, str]:
    controller_status = maybe_text(controller.get("controller_status")) or "completed"
    readiness_status = maybe_text(controller.get("readiness_status")) or "blocked"
    gate_status = maybe_text(controller.get("gate_status")) or "freeze-withheld"
    promotion_status = maybe_text(controller.get("promotion_status")) or "withheld"
    if controller_status == "failed":
        return {
            "supervisor_status": "controller-failed",
            "supervisor_substatus": "phase2-recovery-required",
            "phase2_posture": "controller-failed",
            "terminal_state": "recovery-required",
            "recovery_posture": "resume-controller",
            "operator_action": "resume-phase2",
        }
    if promotion_status == "promoted":
        return {
            "supervisor_status": "ready-for-reporting",
            "supervisor_substatus": "promotion-complete",
            "phase2_posture": "reporting-ready",
            "terminal_state": "reporting-ready",
            "recovery_posture": "terminal",
            "operator_action": "handoff-reporting",
        }
    substatus = "promotion-withheld"
    if readiness_status == "blocked":
        substatus = "blocked-before-promotion"
    elif readiness_status == "needs-more-data":
        substatus = "investigation-open"
    elif gate_status == "freeze-withheld":
        substatus = "gate-withheld"
    return {
        "supervisor_status": "hold-investigation-open",
        "supervisor_substatus": substatus,
        "phase2_posture": "investigation-hold",
        "terminal_state": "investigation-open",
        "recovery_posture": "continue-investigation",
        "operator_action": "continue-investigation",
    }


def supervise_round(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    return supervise_round_with_contract_mode(run_dir, run_id=run_id, round_id=round_id, contract_mode="warn")


def supervise_round_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
) -> dict[str, Any]:
    started_at = utc_now_iso()
    execution_policy = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects or [],
    }
    command_hints = operator_commands(run_id=run_id, round_id=round_id, run_dir=run_dir)
    try:
        controller_result = run_phase2_round_with_contract_mode(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            timeout_seconds=timeout_seconds,
            retry_budget=retry_budget,
            retry_backoff_ms=retry_backoff_ms,
            allow_side_effects=allow_side_effects,
        )
    except SkillExecutionError as exc:
        controller = exc.payload.get("controller", {}) if isinstance(exc.payload.get("controller"), dict) else {}
        artifacts = controller.get("artifacts", {}) if isinstance(controller.get("artifacts"), dict) else {}
        classification = classify_supervisor_state(controller or {"controller_status": "failed"})
        finished_at = utc_now_iso()
        payload = {
            "schema_version": "runtime-supervisor-v3",
            "generated_at_utc": finished_at,
            "run_id": run_id,
            "round_id": round_id,
            "supervisor_path": "",
            "supervisor_status": classification["supervisor_status"],
            "supervisor_substatus": classification["supervisor_substatus"],
            "phase2_posture": classification["phase2_posture"],
            "terminal_state": classification["terminal_state"],
            "recovery_posture": classification["recovery_posture"],
            "operator_action": classification["operator_action"],
            "controller_status": maybe_text(controller.get("controller_status")) or "failed",
            "resume_status": maybe_text(controller.get("resume_status")),
            "current_stage": maybe_text(controller.get("current_stage")),
            "failed_stage": maybe_text(controller.get("failed_stage")),
            "resume_recommended": bool(controller.get("resume_recommended")),
            "restart_recommended": bool(controller.get("restart_recommended")),
            "resume_from_stage": maybe_text(controller.get("recovery", {}).get("resume_from_stage"))
            if isinstance(controller.get("recovery"), dict)
            else "",
            "readiness_status": maybe_text(controller.get("readiness_status")) or "pending",
            "gate_status": maybe_text(controller.get("gate_status")) or "not-evaluated",
            "promotion_status": maybe_text(controller.get("promotion_status")) or "not-evaluated",
            "execution_policy": execution_policy,
            "planning_mode": maybe_text(controller.get("planning_mode")) or "planner-backed",
            "orchestration_plan_path": artifacts.get("orchestration_plan_path", ""),
            "controller_path": artifacts.get("controller_state_path", ""),
            "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
            "promotion_basis_path": artifacts.get("promotion_basis_path", ""),
            "recommended_next_skills": controller.get("recommended_next_skills", []),
            "round_transition": {},
            "top_actions": [],
            "operator_notes": [
                f"Controller failed at stage {maybe_text(controller.get('failed_stage')) or maybe_text(controller.get('current_stage')) or 'unknown'}."
            ],
            "resume_command": command_hints["resume_command"],
            "restart_command": command_hints["restart_command"],
            "inspect_command": command_hints["inspect_command"],
            "inspection_paths": {
                "controller_path": artifacts.get("controller_state_path", ""),
                "plan_path": artifacts.get("orchestration_plan_path", ""),
                "gate_path": artifacts.get("promotion_gate_path", ""),
            },
        }
        output_file = supervisor_state_path(run_dir, round_id)
        payload["supervisor_path"] = str(output_file)
        write_json(output_file, payload)
        store_promotion_freeze_record(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            supervisor_snapshot=payload,
            artifact_paths={
                "controller_state_path": artifacts.get("controller_state_path", ""),
                "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
                "supervisor_state_path": str(output_file),
            },
        )
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "supervisor", started_at, finished_at, contract_mode, "failed"),
                "event_type": "supervisor",
                "run_id": run_id,
                "round_id": round_id,
                "started_at_utc": started_at,
                "completed_at_utc": finished_at,
                "status": "failed",
                "contract_mode": contract_mode,
                "execution_policy": execution_policy,
                "planning_mode": payload["planning_mode"],
                "supervisor_status": payload["supervisor_status"],
                "readiness_status": payload["readiness_status"],
                "gate_status": payload["gate_status"],
                "promotion_status": payload["promotion_status"],
                "supervisor_path": str(output_file),
            },
        )
        failure_payload = dict(exc.payload)
        failure_payload["supervisor"] = payload
        failure_payload["summary"] = {
            "run_id": run_id,
            "round_id": round_id,
            "supervisor_status": payload["supervisor_status"],
            "planning_mode": payload["planning_mode"],
            "supervisor_path": str(output_file),
            "promotion_status": payload["promotion_status"],
        }
        raise SkillExecutionError(failure_payload.get("message", str(exc)), failure_payload)
    controller = controller_result.get("controller", {}) if isinstance(controller_result.get("controller"), dict) else {}
    artifacts = controller.get("artifacts", {}) if isinstance(controller.get("artifacts"), dict) else {}
    next_actions_path = maybe_text(artifacts.get("next_actions_path"))
    next_actions = load_json_if_exists(Path(next_actions_path)) if next_actions_path else {}
    next_actions = next_actions or {}
    top_action_rows = top_actions(next_actions)
    gate_reasons = controller.get("gate_reasons", []) if isinstance(controller.get("gate_reasons"), list) else []
    promotion_status = maybe_text(controller.get("promotion_status")) or "withheld"
    gate_status = maybe_text(controller.get("gate_status")) or "freeze-withheld"
    classification = classify_supervisor_state(controller)
    round_transition = round_transition_hint(run_dir, run_id=run_id, round_id=round_id, supervisor_status=classification["supervisor_status"])
    recommended_next_skills = unique_texts(
        [
            *(
                ["eco-open-investigation-round"]
                if classification["supervisor_status"] == "hold-investigation-open"
                else []
            ),
            *(
                controller.get("recommended_next_skills", [])
                if isinstance(controller.get("recommended_next_skills"), list)
                else []
            ),
        ]
    )
    resolved_operator_notes = operator_notes(
        promotion_status=promotion_status,
        gate_status=gate_status,
        gate_reasons=gate_reasons,
        top_action_rows=top_action_rows,
    )
    if round_transition:
        resolved_operator_notes = [
            *resolved_operator_notes,
            f"Moderator may open {round_transition['suggested_round_id']} via eco-open-investigation-round to continue the case with a clean round boundary.",
        ][:4]
    finished_at = utc_now_iso()

    payload = {
        "schema_version": "runtime-supervisor-v3",
        "generated_at_utc": finished_at,
        "run_id": run_id,
        "round_id": round_id,
        "supervisor_path": "",
        "supervisor_status": classification["supervisor_status"],
        "supervisor_substatus": classification["supervisor_substatus"],
        "phase2_posture": classification["phase2_posture"],
        "terminal_state": classification["terminal_state"],
        "recovery_posture": classification["recovery_posture"],
        "operator_action": classification["operator_action"],
        "controller_status": maybe_text(controller.get("controller_status")) or "completed",
        "resume_status": maybe_text(controller.get("resume_status")),
        "current_stage": maybe_text(controller.get("current_stage")),
        "failed_stage": maybe_text(controller.get("failed_stage")),
        "resume_recommended": bool(controller.get("resume_recommended")),
        "restart_recommended": bool(controller.get("restart_recommended")),
        "resume_from_stage": maybe_text(controller.get("recovery", {}).get("resume_from_stage"))
        if isinstance(controller.get("recovery"), dict)
        else "",
        "readiness_status": maybe_text(controller.get("readiness_status")) or "blocked",
        "gate_status": gate_status,
        "promotion_status": promotion_status,
        "execution_policy": execution_policy,
        "planning_mode": maybe_text(controller.get("planning_mode")) or maybe_text(controller.get("planning", {}).get("planning_mode") if isinstance(controller.get("planning"), dict) else "") or "planner-backed",
        "orchestration_plan_path": artifacts.get("orchestration_plan_path", ""),
        "controller_path": artifacts.get("controller_state_path", ""),
        "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
        "promotion_basis_path": artifacts.get("promotion_basis_path", ""),
        "recommended_next_skills": recommended_next_skills,
        "round_transition": round_transition,
        "top_actions": top_action_rows,
        "operator_notes": resolved_operator_notes,
        "resume_command": command_hints["resume_command"],
        "restart_command": command_hints["restart_command"],
        "inspect_command": command_hints["inspect_command"],
        "inspection_paths": {
            "controller_path": artifacts.get("controller_state_path", ""),
            "plan_path": artifacts.get("orchestration_plan_path", ""),
            "gate_path": artifacts.get("promotion_gate_path", ""),
            "promotion_basis_path": artifacts.get("promotion_basis_path", ""),
        },
    }
    output_file = supervisor_state_path(run_dir, round_id)
    payload["supervisor_path"] = str(output_file)
    write_json(output_file, payload)
    store_promotion_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        supervisor_snapshot=payload,
        artifact_paths={
            "controller_state_path": artifacts.get("controller_state_path", ""),
            "promotion_gate_path": artifacts.get("promotion_gate_path", ""),
            "supervisor_state_path": str(output_file),
        },
    )

    event_id = new_runtime_event_id("runtimeevt", run_id, round_id, "supervisor", started_at, finished_at, contract_mode)
    append_ledger_event(
        run_dir,
        {
            "schema_version": "runtime-event-v3",
            "event_id": event_id,
            "event_type": "supervisor",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": started_at,
            "completed_at_utc": finished_at,
            "status": "completed",
            "contract_mode": contract_mode,
            "execution_policy": execution_policy,
            "planning_mode": payload["planning_mode"],
            "supervisor_status": payload["supervisor_status"],
            "readiness_status": payload["readiness_status"],
            "gate_status": gate_status,
            "promotion_status": promotion_status,
            "supervisor_path": str(output_file),
        },
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "supervisor_status": payload["supervisor_status"],
            "planning_mode": payload["planning_mode"],
            "supervisor_path": str(output_file),
            "promotion_status": promotion_status,
        },
        "supervisor": payload,
        "controller": controller,
    }
