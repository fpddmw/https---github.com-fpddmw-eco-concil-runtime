from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.governed_execution_controller_state import (
    adopted_planner_stage_summary,
    append_planning_attempt,
    base_controller_payload,
    controller_failure_payload,
    controller_planning_state,
    controller_result_payload,
    gate_stage_summary,
    merge_existing_steps,
    planner_stage_summary_from_result,
    planning_attempt_record,
    refresh_controller_payload,
    round_controller_event,
    stage_blueprint,
    stage_blueprints,
    stage_contracts_from_blueprints,
    stage_summary_from_result,
    step_index,
    unique_texts,
)
from eco_council_runtime.kernel.execution.runtime_posture_profile import (
    posture_profile_callable,
    resolve_runtime_posture_profile,
)
from eco_council_runtime.kernel.execution.runtime_planning_profile import (
    agent_orchestration_requested as agent_orchestration_requested_from_profile,
    ensure_executable_planning as ensure_executable_planning_from_profile,
    normalized_controller_planning_mode as normalized_controller_planning_mode_from_profile,
    planner_skill_args_for_source,
    planning_bundle as planning_bundle_from_result,
    planning_from_controller as planning_from_controller_from_profile,
    planning_source_output_path,
    resolve_runtime_planning_sources,
)
from eco_council_runtime.kernel.execution.runtime_stage_profile import (
    DEFAULT_RUNTIME_PLANNER_SKILL_NAME,
    resolve_stage_definitions,
)
from eco_council_runtime.kernel.planes.deliberation_plane import (
    load_governed_execution_control_state,
    store_orchestration_plan_record,
    store_runtime_control_freeze_record,
)
from eco_council_runtime.kernel.execution.executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill, utc_now_iso
from eco_council_runtime.kernel.execution.gate import GateHandler, execute_gate_step as execute_runtime_gate_step
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import init_round_cursor, init_run_manifest, load_json_if_exists, write_json
from eco_council_runtime.kernel.core.paths import (
    agent_entry_gate_path,
    controller_state_path,
    ensure_runtime_dirs,
    mission_scaffold_path,
    orchestration_plan_path,
    report_basis_gate_path,
)
from eco_council_runtime.kernel.core.registry import write_registry
from eco_council_runtime.kernel.governance.role_contracts import ROLE_MODERATOR
from eco_council_runtime.kernel.governance.skill_registry import default_actor_role_hint
from eco_council_runtime.kernel.governance.transition_requests import (
    REQUEST_STATUS_APPROVED,
    REQUEST_STATUS_COMMITTED,
    REQUEST_STATUS_PENDING,
    TRANSITION_KIND_FREEZE_REPORT_BASIS,
    latest_transition_request,
)


from eco_council_runtime.kernel.execution.controller.artifacts import (
    governed_execution_artifact_paths,
    persist_controller_state,
)
from eco_council_runtime.kernel.execution.controller.planning_adapters import (
    agent_orchestration_requested,
    ensure_executable_planning,
    execute_gate_step,
    normalized_controller_planning_mode,
    planning_bundle,
    planning_from_controller,
)
from eco_council_runtime.kernel.execution.controller.transition_planning import (
    PLANNER_SKILL_NAME,
    TRANSITION_EXECUTOR_AUTHORITY,
    TRANSITION_EXECUTOR_INSPECTION_SOURCE,
    TRANSITION_EXECUTOR_PLAN_SOURCE,
    TRANSITION_EXECUTOR_PLANNING_MODE,
    approved_transition_request_planning,
    controller_stage_skill_args,
    inspection_only_planning,
    skill_actor_role_hint,
)


def adopted_transition_request_id(controller_payload: dict[str, Any]) -> str:
    direct = maybe_text(controller_payload.get("adopted_transition_request_id"))
    if direct:
        return direct
    planning = controller_payload.get("planning") if isinstance(controller_payload.get("planning"), dict) else {}
    for key in ("transition_request_id", "adopted_transition_request_id"):
        value = maybe_text(planning.get(key))
        if value:
            return value
    phase_decision_basis = planning.get("phase_decision_basis") if isinstance(planning.get("phase_decision_basis"), dict) else {}
    value = maybe_text(phase_decision_basis.get("transition_request_id"))
    if value:
        return value
    plan_id = maybe_text(planning.get("plan_id"))
    prefix = "transition-executor-"
    if plan_id.startswith(prefix):
        return plan_id[len(prefix):]
    return ""


def latest_approved_transition_request(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any] | None:
    return latest_transition_request(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind=TRANSITION_KIND_FREEZE_REPORT_BASIS,
        request_status=REQUEST_STATUS_APPROVED,
    )


def stale_completed_controller_reason(
    existing_controller: dict[str, Any],
    latest_approved_request: dict[str, Any] | None,
) -> dict[str, str]:
    if not isinstance(latest_approved_request, dict):
        return {}
    latest_request_id = maybe_text(latest_approved_request.get("request_id"))
    if not latest_request_id:
        return {}
    adopted_request_id = adopted_transition_request_id(existing_controller)
    if not adopted_request_id:
        return {}
    if adopted_request_id == latest_request_id:
        return {}
    return {
        "reason": "newer-approved-transition-request",
        "adopted_transition_request_id": adopted_request_id,
        "latest_approved_transition_request_id": latest_request_id,
    }


def run_governed_execution_round(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    actor_role: str = "runtime-operator",
    gate_handlers: dict[str, GateHandler] | None,
    posture_profile: dict[str, Any] | None = None,
    planning_sources: list[dict[str, Any]] | None = None,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return run_governed_execution_round_with_contract_mode(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        actor_role=actor_role,
        contract_mode="warn",
        gate_handlers=gate_handlers,
        posture_profile=posture_profile,
        planning_sources=planning_sources,
        stage_definitions=stage_definitions,
    )

def run_governed_execution_round_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    actor_role: str = "runtime-operator",
    contract_mode: str,
    gate_handlers: dict[str, GateHandler] | None,
    posture_profile: dict[str, Any] | None = None,
    planning_sources: list[dict[str, Any]] | None = None,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
    force_restart: bool = False,
) -> dict[str, Any]:
    profile = resolve_runtime_posture_profile(posture_profile)
    controller_completion_builder = posture_profile_callable(
        profile,
        "controller_completion_builder",
    )
    ensure_runtime_dirs(run_dir)
    write_registry(run_dir)
    init_run_manifest(run_dir, run_id)
    init_round_cursor(run_dir, run_id)
    artifacts = governed_execution_artifact_paths(run_dir, round_id)
    execution_policy = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects or [],
    }
    execution_kwargs = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects,
    }
    resolved_planning_sources = resolve_runtime_planning_sources(planning_sources)
    default_transition_execution_mode = not bool(resolved_planning_sources)
    resolved_stage_definitions = resolve_stage_definitions(stage_definitions)

    governed_execution_control_state = load_governed_execution_control_state(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    existing_controller = (
        governed_execution_control_state.get("controller", {})
        if isinstance(governed_execution_control_state.get("controller"), dict)
        else {}
    ) or load_json_if_exists(controller_state_path(run_dir, round_id)) or {}
    existing_gate = (
        governed_execution_control_state.get("report_basis_gate", {})
        if isinstance(governed_execution_control_state.get("report_basis_gate"), dict)
        else governed_execution_control_state.get("report_basis_gate", {})
        if isinstance(governed_execution_control_state.get("report_basis_gate"), dict)
        else {}
    ) or load_json_if_exists(Path(artifacts["report_basis_gate_path"])) or load_json_if_exists(Path(artifacts["report_basis_gate_path"])) or {}
    existing_status = maybe_text(existing_controller.get("controller_status"))
    latest_approved_request = (
        latest_approved_transition_request(
            run_dir,
            run_id=run_id,
            round_id=round_id,
        )
        if default_transition_execution_mode
        else None
    )
    stale_controller = (
        stale_completed_controller_reason(existing_controller, latest_approved_request)
        if default_transition_execution_mode
        else {}
    )
    if not force_restart and existing_status == "completed" and not stale_controller:
        return controller_result_payload(existing_controller, existing_gate)

    started_at = utc_now_iso()
    resume_status = (
        "restart-forced"
        if force_restart
        else "restart-stale-transition"
        if stale_controller
        else "fresh-run"
    )
    resume_count = 0
    planning: dict[str, Any] = {}
    blueprints: list[dict[str, Any]] = []
    controller_payload = base_controller_payload(
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
        execution_policy=execution_policy,
        artifacts=artifacts,
        started_at=started_at,
        resume_status=resume_status,
        resume_count=resume_count,
    )
    controller_payload["requested_by_role"] = maybe_text(actor_role)
    controller_payload["planning_mode"] = (
        TRANSITION_EXECUTOR_PLANNING_MODE
        if default_transition_execution_mode
        else "planner-pending"
    )
    if stale_controller:
        controller_payload["stale_controller"] = stale_controller
        append_planning_attempt(
            controller_payload,
            planning_attempt_record(
                source="controller-freshness",
                status="stale-restart",
                planning_mode=TRANSITION_EXECUTOR_PLANNING_MODE,
                controller_authority=TRANSITION_EXECUTOR_AUTHORITY,
                message=(
                    "Existing completed controller adopted transition request "
                    f"`{stale_controller.get('adopted_transition_request_id') or '<none>'}`, "
                    "but a newer approved freeze-report-basis request "
                    f"`{stale_controller.get('latest_approved_transition_request_id')}` is available."
                ),
            ),
        )

    if not force_restart and not stale_controller and existing_status in {"running", "failed"}:
        recovered_planning = planning_from_controller(run_dir, round_id, existing_controller)
        if recovered_planning:
            ensure_executable_planning(recovered_planning)
            planning = recovered_planning
            blueprints = stage_blueprints(
                planning,
                artifacts,
                planner_skill_name=(
                    maybe_text(recovered_planning.get("planner_skill_name"))
                    or PLANNER_SKILL_NAME
                ),
                stage_definitions=resolved_stage_definitions,
            )
            controller_payload = {
                **existing_controller,
                "schema_version": "runtime-controller-v3",
                "run_id": run_id,
                "round_id": round_id,
                "contract_mode": contract_mode,
                "requested_by_role": maybe_text(actor_role),
                "execution_policy": execution_policy,
                "controller_status": "running",
                "resume_status": "resumed",
                "resume_count": int(existing_controller.get("resume_count") or 0) + 1,
                "started_at_utc": maybe_text(existing_controller.get("started_at_utc")) or started_at,
                "failure": {},
                "planning_mode": normalized_controller_planning_mode(
                    maybe_text(existing_controller.get("planning_mode"))
                    or maybe_text(recovered_planning.get("planning_mode"))
                ),
                "planning": controller_planning_state(recovered_planning, blueprints),
                "stage_contracts": stage_contracts_from_blueprints(blueprints),
                "steps": merge_existing_steps(blueprints, existing_controller.get("steps")),
                "artifacts": artifacts,
            }
            persist_controller_state(run_dir, round_id, controller_payload)

    planner_stage_ran = False

    def start_planner_attempt(output_path: Path) -> int:
        planner_blueprint = stage_blueprint(
            "orchestration-planner",
            skill_name=PLANNER_SKILL_NAME,
            artifacts=artifacts,
            explicit_output_path=str(output_path.resolve()),
            stage_definitions=resolved_stage_definitions,
        )
        controller_payload["steps"] = merge_existing_steps(
            [planner_blueprint],
            controller_payload.get("steps"),
        )
        controller_payload["stage_contracts"] = stage_contracts_from_blueprints(
            [planner_blueprint]
        )
        controller_payload["planning_mode"] = "planner-pending"
        controller_payload["controller_status"] = "running"
        planner_index = step_index(
            controller_payload["steps"],
            "orchestration-planner",
        )
        controller_payload["steps"][planner_index]["status"] = "running"
        controller_payload["steps"][planner_index]["started_at_utc"] = (
            maybe_text(
                controller_payload["steps"][planner_index].get("started_at_utc")
            )
            or started_at
        )
        persist_controller_state(run_dir, round_id, controller_payload)
        return planner_index

    def reset_planner_attempt(planner_index: int) -> None:
        controller_payload["steps"][planner_index].update(
            {
                "status": "pending",
                "event_id": "",
                "receipt_id": "",
                "started_at_utc": "",
                "completed_at_utc": "",
            }
        )

    def adopt_planning(
        selected_planning: dict[str, Any],
        *,
        source_name: str,
        status: str,
        message: str,
        planner_result: dict[str, Any] | None = None,
    ) -> None:
        nonlocal planning, blueprints, planner_stage_ran
        ensure_executable_planning(selected_planning)
        plan_payload = (
            selected_planning.get("plan_payload", {})
            if isinstance(selected_planning.get("plan_payload"), dict)
            else {}
        )
        if plan_payload:
            store_orchestration_plan_record(
                run_dir,
                plan_payload=plan_payload,
                artifact_path=maybe_text(selected_planning.get("plan_path")),
                run_id=run_id,
                round_id=round_id,
                controller_authority=maybe_text(
                    selected_planning.get("controller_authority")
                ),
            )
        planning = selected_planning
        resolved_planner_skill_name = (
            maybe_text(selected_planning.get("planner_skill_name"))
            or PLANNER_SKILL_NAME
        )
        blueprints = stage_blueprints(
            selected_planning,
            artifacts,
            planner_skill_name=resolved_planner_skill_name,
            stage_definitions=resolved_stage_definitions,
        )
        controller_payload["planning_mode"] = (
            maybe_text(selected_planning.get("planning_mode")) or "planner-backed"
        )
        controller_payload["planning"] = controller_planning_state(
            selected_planning,
            blueprints,
        )
        phase_decision_basis = (
            selected_planning.get("phase_decision_basis")
            if isinstance(selected_planning.get("phase_decision_basis"), dict)
            else {}
        )
        transition_request = (
            selected_planning.get("transition_request")
            if isinstance(selected_planning.get("transition_request"), dict)
            else {}
        )
        adopted_request_id = maybe_text(phase_decision_basis.get("transition_request_id")) or maybe_text(transition_request.get("request_id"))
        if adopted_request_id:
            controller_payload["adopted_transition_request_id"] = adopted_request_id
            controller_payload["adopted_transition_request_status"] = maybe_text(
                phase_decision_basis.get("transition_request_status")
            ) or maybe_text(transition_request.get("request_status"))
            controller_payload["adopted_transition_kind"] = maybe_text(
                phase_decision_basis.get("transition_kind")
            ) or maybe_text(transition_request.get("transition_kind"))
        controller_payload["stage_contracts"] = stage_contracts_from_blueprints(
            blueprints
        )
        controller_payload["steps"] = merge_existing_steps(
            blueprints,
            controller_payload.get("steps"),
        )
        include_planner_stage = bool(selected_planning.get("include_planner_stage", True))
        if include_planner_stage:
            planner_step_index = step_index(
                controller_payload["steps"],
                "orchestration-planner",
            )
            if planner_result is None:
                controller_payload["steps"][planner_step_index] = adopted_planner_stage_summary(
                    run_id=run_id,
                    round_id=round_id,
                    blueprint=blueprints[0],
                    planning=selected_planning,
                    started_at=started_at,
                    completed_at=utc_now_iso(),
                )
            else:
                controller_payload["steps"][planner_step_index] = planner_stage_summary_from_result(
                    planner_result,
                    blueprints[0],
                    selected_planning,
                )
        append_planning_attempt(
            controller_payload,
            planning_attempt_record(
                source=source_name,
                status=status,
                plan_path=maybe_text(selected_planning.get("plan_path")),
                planning_mode=maybe_text(selected_planning.get("planning_mode")),
                controller_authority=maybe_text(
                    selected_planning.get("controller_authority")
                ),
                receipt_id=maybe_text(planner_result.get("summary", {}).get("receipt_id"))
                if isinstance(planner_result, dict)
                and isinstance(planner_result.get("summary"), dict)
                else "",
                event_id=maybe_text(planner_result.get("summary", {}).get("event_id"))
                if isinstance(planner_result, dict)
                and isinstance(planner_result.get("summary"), dict)
                else "",
                message=message,
            ),
        )
        persist_controller_state(run_dir, round_id, controller_payload)
        planner_stage_ran = include_planner_stage

    def fail_runtime_planner(
        planner_index: int,
        plan_path: str,
        message: str,
        stage_failure: dict[str, Any] | None = None,
        *,
        retryable: bool = False,
    ) -> None:
        append_planning_attempt(
            controller_payload,
            planning_attempt_record(
                source="runtime-planner",
                status="failed",
                plan_path=plan_path,
                message=message,
            ),
        )
        controller_payload["controller_status"] = "failed"
        controller_payload["steps"][planner_index].update(
            {
                "status": "failed",
                "started_at_utc": maybe_text(
                    controller_payload["steps"][planner_index].get("started_at_utc")
                )
                or started_at,
                "completed_at_utc": utc_now_iso(),
            }
        )
        failure_payload = controller_failure_payload(
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            stage_name="orchestration-planner",
            message=message,
            controller_payload=persist_controller_state(
                run_dir,
                round_id,
                controller_payload,
            ),
            stage_failure=stage_failure,
            retryable=retryable,
        )
        append_ledger_event(
            run_dir,
            round_controller_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                contract_mode=contract_mode,
                controller_payload=controller_payload,
                status="failed",
                failure=failure_payload.get("failure", {}),
            ),
        )
        raise SkillExecutionError(failure_payload["message"], failure_payload)

    if not planning and default_transition_execution_mode:
        transition_planning, latest_transition = approved_transition_request_planning(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            artifacts=artifacts,
        )
        if transition_planning:
            adopt_planning(
                transition_planning,
                source_name=TRANSITION_EXECUTOR_PLAN_SOURCE,
                status="adopted",
                message="Controller adopted the latest approved freeze-report-basis transition request.",
            )
        else:
            inspection_planning = inspection_only_planning(artifacts=artifacts)
            controller_payload["planning_mode"] = TRANSITION_EXECUTOR_PLANNING_MODE
            controller_payload["planning"] = controller_planning_state(
                inspection_planning,
                [],
            )
            controller_payload["readiness_status"] = "needs-more-data"
            controller_payload["gate_status"] = "not-evaluated"
            controller_payload["report_basis_status"] = "withheld"
            controller_payload["recommended_next_skills"] = []
            controller_payload["controller_status"] = "completed"
            if isinstance(latest_transition, dict):
                latest_status = maybe_text(latest_transition.get("request_status"))
                reason = "Await moderator transition request."
                if latest_status == REQUEST_STATUS_PENDING:
                    reason = "Await runtime-operator approval for the latest transition request."
                elif latest_status:
                    reason = (
                        f"Latest freeze-report-basis request is `{latest_status}`; "
                        "no default kernel execution path is available."
                    )
                append_planning_attempt(
                    controller_payload,
                    planning_attempt_record(
                        source=TRANSITION_EXECUTOR_INSPECTION_SOURCE,
                        status="unavailable",
                        plan_path="",
                        planning_mode=TRANSITION_EXECUTOR_PLANNING_MODE,
                        controller_authority=TRANSITION_EXECUTOR_AUTHORITY,
                        message=reason,
                    ),
                )
            persisted_controller = persist_controller_state(
                run_dir,
                round_id,
                controller_payload,
            )
            finished_at = utc_now_iso()
            append_ledger_event(
                run_dir,
                round_controller_event(
                    run_id=run_id,
                    round_id=round_id,
                    started_at=started_at,
                    completed_at=finished_at,
                    contract_mode=contract_mode,
                    controller_payload=persisted_controller,
                    status="completed",
                ),
            )
            return controller_result_payload(persisted_controller, {})

    if not planning:
        for source_spec in resolved_planning_sources:
            source_name = maybe_text(source_spec.get("source_name"))
            source_kind = maybe_text(source_spec.get("source_kind"))
            if (
                bool(source_spec.get("requires_agent_orchestration"))
                and not agent_orchestration_requested(run_dir, round_id)
            ):
                continue
            output_path = planning_source_output_path(
                run_dir,
                round_id,
                artifacts,
                source_spec,
            )
            planner_index = start_planner_attempt(output_path)
            if source_kind != "planner-skill":
                continue
            skill_name = maybe_text(source_spec.get("planner_skill_name")) or PLANNER_SKILL_NAME
            skill_args = planner_skill_args_for_source(
                run_dir,
                source_spec,
                output_path,
            )
            planner_actor_role = skill_actor_role_hint(
                skill_name,
                preferred_role_hint=source_spec.get("assigned_role_hint"),
            )
            try:
                planner_result = run_skill(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    skill_name=skill_name,
                    actor_role=planner_actor_role,
                    skill_args=skill_args,
                    contract_mode=contract_mode,
                    **execution_kwargs,
                )
            except SkillExecutionError as exc:
                if source_name == "runtime-planner":
                    fail_runtime_planner(
                        planner_index,
                        maybe_text(artifacts.get("orchestration_plan_path")),
                        exc.payload.get("message", str(exc)),
                        exc.payload,
                        retryable=bool(exc.payload.get("failure", {}).get("retryable"))
                        if isinstance(exc.payload.get("failure"), dict)
                        else False,
                    )
                reset_planner_attempt(planner_index)
                append_planning_attempt(
                    controller_payload,
                    planning_attempt_record(
                        source=source_name,
                        status="failed",
                        plan_path=str(output_path.resolve()),
                        message=(
                            maybe_text(source_spec.get("failed_message"))
                            or exc.payload.get("message", str(exc))
                        ),
                    ),
                )
                persist_controller_state(run_dir, round_id, controller_payload)
                continue

            selected_planning = planning_bundle(run_dir, round_id, planner_result)
            if selected_planning:
                adopt_planning(
                    selected_planning,
                    source_name=(
                        maybe_text(selected_planning.get("plan_source")) or source_name
                    ),
                    status="materialized",
                    message=(
                        maybe_text(source_spec.get("materialized_message"))
                        or "Controller materialized a governed-execution planning source."
                    ),
                    planner_result=planner_result,
                )
                break
            if source_name == "runtime-planner":
                fail_runtime_planner(
                    planner_index,
                    str(output_path.resolve()),
                    maybe_text(source_spec.get("failed_message"))
                    or "Runtime planner completed without producing a usable execution_queue.",
                    {"status": "failed", "source": source_name},
                )
            reset_planner_attempt(planner_index)
            append_planning_attempt(
                controller_payload,
                planning_attempt_record(
                    source=source_name,
                    status="failed",
                    plan_path=str(output_path.resolve()),
                    message=(
                        maybe_text(source_spec.get("failed_message"))
                        or "Planning source completed but did not produce a usable execution_queue."
                    ),
                ),
            )
            persist_controller_state(run_dir, round_id, controller_payload)

    if not planning:
        controller_payload["controller_status"] = "failed"
        failure_payload = controller_failure_payload(
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            stage_name="orchestration-planner",
            message="No injected governed-execution planning source produced a usable execution_queue.",
            controller_payload=persist_controller_state(run_dir, round_id, controller_payload),
            stage_failure={
                "status": "failed",
                "planning_sources": resolved_planning_sources,
            },
        )
        append_ledger_event(
            run_dir,
            round_controller_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                contract_mode=contract_mode,
                controller_payload=controller_payload,
                status="failed",
                failure=failure_payload.get("failure", {}),
            ),
        )
        raise SkillExecutionError(failure_payload["message"], failure_payload)

    try:
        if not blueprints:
            blueprints = stage_blueprints(
                planning,
                artifacts,
                planner_skill_name=(
                    maybe_text(planning.get("planner_skill_name"))
                    or PLANNER_SKILL_NAME
                ),
                stage_definitions=resolved_stage_definitions,
            )
            controller_payload["stage_contracts"] = stage_contracts_from_blueprints(blueprints)
            controller_payload["steps"] = merge_existing_steps(blueprints, controller_payload.get("steps"))
            persist_controller_state(run_dir, round_id, controller_payload)

        for blueprint in blueprints:
            stage_name = maybe_text(blueprint.get("stage"))
            if planner_stage_ran and stage_name == "orchestration-planner":
                continue
            step_pos = step_index(controller_payload["steps"], stage_name)
            existing_step = controller_payload["steps"][step_pos]
            if maybe_text(existing_step.get("status")) == "completed":
                continue
            controller_payload["controller_status"] = "running"
            controller_payload["failure"] = {}
            controller_payload["steps"][step_pos]["status"] = "running"
            controller_payload["steps"][step_pos]["started_at_utc"] = maybe_text(existing_step.get("started_at_utc")) or utc_now_iso()
            persist_controller_state(run_dir, round_id, controller_payload)

            if maybe_text(blueprint.get("stage_kind")) == "gate":
                gate_started_at = utc_now_iso()
                gate_result = execute_gate_step(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    blueprint=blueprint,
                    gate_handlers=gate_handlers,
                    stage_contracts=controller_payload.get("stage_contracts", {})
                    if isinstance(controller_payload.get("stage_contracts"), dict)
                    else {},
                )
                gate_handler = maybe_text(gate_result.get("gate_handler")) or maybe_text(blueprint.get("gate_handler")) or stage_name
                readiness_stage_name = maybe_text(gate_result.get("readiness_stage_name"))
                gate_payload = gate_result.get("gate_payload", {}) if isinstance(gate_result.get("gate_payload"), dict) else {}
                if gate_payload:
                    gate_payload["stage_name"] = (
                        maybe_text(gate_payload.get("stage_name")) or stage_name
                    )
                    gate_payload["gate_handler"] = (
                        maybe_text(gate_payload.get("gate_handler")) or gate_handler
                    )
                    if readiness_stage_name:
                        gate_payload["readiness_stage_name"] = readiness_stage_name
                gate_updates = gate_result.get("controller_updates", {}) if isinstance(gate_result.get("controller_updates"), dict) else {}
                gate_event_id = new_runtime_event_id(
                    "runtimeevt",
                    run_id,
                    round_id,
                    gate_handler,
                    gate_started_at,
                    gate_payload.get("generated_at_utc"),
                    controller_payload.get("resume_status"),
                )
                append_ledger_event(
                    run_dir,
                    {
                        "schema_version": "runtime-event-v3",
                        "event_id": gate_event_id,
                        "event_type": gate_handler,
                        "run_id": run_id,
                        "round_id": round_id,
                        "started_at_utc": gate_started_at,
                        "completed_at_utc": gate_payload.get("generated_at_utc"),
                        "status": "completed",
                        "contract_mode": contract_mode,
                        "planning_mode": controller_payload.get("planning_mode"),
                        "plan_id": controller_payload.get("planning", {}).get("plan_id", "")
                        if isinstance(controller_payload.get("planning"), dict)
                        else "",
                        "plan_path": controller_payload.get("planning", {}).get("plan_path", "")
                        if isinstance(controller_payload.get("planning"), dict)
                        else "",
                        "gate_status": gate_payload.get("gate_status"),
                        "report_basis_gate_status": gate_payload.get("report_basis_gate_status"),
                        "readiness_status": gate_payload.get("readiness_status"),
                        "report_basis_freeze_allowed": bool(
                            gate_payload.get("report_basis_freeze_allowed")
                        ),
                        "gate_path": gate_payload.get("output_path"),
                        "readiness_stage_name": readiness_stage_name,
                    },
                )
                controller_payload["steps"][step_pos] = gate_stage_summary(blueprint, gate_payload, gate_event_id, gate_started_at)
                controller_payload["readiness_status"] = maybe_text(gate_updates.get("readiness_status")) or "blocked"
                controller_payload["gate_status"] = maybe_text(gate_updates.get("gate_status")) or "report-basis-freeze-withheld"
                controller_payload["report_basis_gate_status"] = maybe_text(
                    gate_updates.get("report_basis_gate_status")
                )
                controller_payload["report_basis_status"] = maybe_text(
                    gate_updates.get("report_basis_status")
                ) or maybe_text(gate_updates.get("report_basis_status")) or "withheld"
                controller_payload["report_basis_status"] = maybe_text(
                    gate_updates.get("report_basis_status")
                ) or controller_payload["report_basis_status"]
                controller_payload["report_basis_freeze_allowed"] = bool(
                    gate_updates.get("report_basis_freeze_allowed")
                )
                controller_payload["gate_reasons"] = (
                    gate_updates.get("gate_reasons", [])
                    if isinstance(gate_updates.get("gate_reasons"), list)
                    else []
                )
                controller_payload["recommended_next_skills"] = (
                    gate_updates.get("recommended_next_skills", [])
                    if isinstance(gate_updates.get("recommended_next_skills"), list)
                    else []
                )
                persist_controller_state(run_dir, round_id, controller_payload, gate_payload=gate_payload)
                continue

            stage_actor_role = skill_actor_role_hint(
                maybe_text(blueprint.get("skill_name")),
                preferred_role_hint=blueprint.get("assigned_role_hint"),
            )
            resolved_skill_args = controller_stage_skill_args(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                stage_name=stage_name,
                skill_name=maybe_text(blueprint.get("skill_name")),
                skill_args=blueprint.get("skill_args", []),
            )
            execution_blueprint = {
                **blueprint,
                "skill_args": resolved_skill_args,
            }
            skill_result = run_skill(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name=maybe_text(blueprint.get("skill_name")),
                actor_role=stage_actor_role,
                skill_args=resolved_skill_args,
                contract_mode=contract_mode,
                **execution_kwargs,
            )
            controller_payload["steps"][step_pos] = stage_summary_from_result(
                stage_name,
                skill_result,
                execution_blueprint,
            )
            if stage_name == "round-readiness":
                readiness_summary = skill_result.get("skill_payload", {}) if isinstance(skill_result.get("skill_payload"), dict) else {}
                controller_payload["readiness_status"] = maybe_text(readiness_summary.get("summary", {}).get("readiness_status"))
            if stage_name == "report-basis-freeze":
                report_basis_payload = skill_result.get("skill_payload", {}) if isinstance(skill_result.get("skill_payload"), dict) else {}
                report_basis_summary = report_basis_payload.get("summary", {}) if isinstance(report_basis_payload.get("summary"), dict) else {}
                controller_payload["report_basis_status"] = maybe_text(report_basis_summary.get("report_basis_status")) or "withheld"
            persist_controller_state(run_dir, round_id, controller_payload)
    except SkillExecutionError as exc:
        failed_stage = controller_payload.get("current_stage", "")
        if not failed_stage:
            failed_stage = controller_payload.get("recovery", {}).get("resume_from_stage", "") if isinstance(controller_payload.get("recovery"), dict) else ""
        if not failed_stage and isinstance(controller_payload.get("steps"), list):
            for step in controller_payload.get("steps", []):
                if maybe_text(step.get("status")) == "running":
                    failed_stage = maybe_text(step.get("stage"))
                    break
        if failed_stage:
            controller_payload["steps"][step_index(controller_payload["steps"], failed_stage)].update(
                {
                    "status": "failed",
                    "completed_at_utc": utc_now_iso(),
                }
            )
        controller_payload["controller_status"] = "failed"
        controller_payload["failure"] = exc.payload
        failure_payload = controller_failure_payload(
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            stage_name=failed_stage,
            message=exc.payload.get("message", str(exc)),
            controller_payload=persist_controller_state(run_dir, round_id, controller_payload),
            stage_failure=exc.payload,
            retryable=bool(exc.payload.get("failure", {}).get("retryable")) if isinstance(exc.payload.get("failure"), dict) else False,
        )
        append_ledger_event(
            run_dir,
            round_controller_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                contract_mode=contract_mode,
                controller_payload=controller_payload,
                status="failed",
                failure=failure_payload.get("failure", {}),
            ),
        )
        raise SkillExecutionError(failure_payload["message"], failure_payload)
    except Exception as exc:
        failed_stage = controller_payload.get("current_stage", "")
        if failed_stage and isinstance(controller_payload.get("steps"), list):
            controller_payload["steps"][step_index(controller_payload["steps"], failed_stage)].update(
                {
                    "status": "failed",
                    "completed_at_utc": utc_now_iso(),
                }
            )
        controller_payload["controller_status"] = "failed"
        controller_payload["failure"] = {"message": str(exc)}
        failure_payload = controller_failure_payload(
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            stage_name=failed_stage,
            message=str(exc),
            controller_payload=persist_controller_state(run_dir, round_id, controller_payload),
            stage_failure={"message": str(exc)},
            retryable=False,
        )
        append_ledger_event(
            run_dir,
            round_controller_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                contract_mode=contract_mode,
                controller_payload=controller_payload,
                status="failed",
                failure=failure_payload.get("failure", {}),
            ),
        )
        raise SkillExecutionError(failure_payload["message"], failure_payload)

    governed_execution_control_state = load_governed_execution_control_state(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    gate_payload = (
        governed_execution_control_state.get("report_basis_gate", {})
        if isinstance(governed_execution_control_state.get("report_basis_gate"), dict)
        else governed_execution_control_state.get("report_basis_gate", {})
        if isinstance(governed_execution_control_state.get("report_basis_gate"), dict)
        else {}
    ) or load_json_if_exists(Path(artifacts["report_basis_gate_path"])) or load_json_if_exists(Path(artifacts["report_basis_gate_path"])) or {}
    controller_payload["controller_status"] = "completed"
    controller_payload["planning_mode"] = normalized_controller_planning_mode(
        maybe_text(planning.get("planning_mode"))
        or maybe_text(controller_payload.get("planning_mode"))
    )
    controller_payload["readiness_status"] = maybe_text(gate_payload.get("readiness_status")) or maybe_text(controller_payload.get("readiness_status")) or "blocked"
    controller_payload["gate_status"] = maybe_text(gate_payload.get("gate_status")) or maybe_text(controller_payload.get("gate_status")) or "report-basis-freeze-withheld"
    if maybe_text(controller_payload.get("report_basis_status")) in {"", "not-evaluated"}:
        report_basis_freeze_payload = (
            governed_execution_control_state.get("report_basis_freeze", {})
            if isinstance(governed_execution_control_state.get("report_basis_freeze"), dict)
            else {}
        )
        controller_payload["report_basis_status"] = (
            maybe_text(report_basis_freeze_payload.get("report_basis_status"))
            or maybe_text(report_basis_freeze_payload.get("report_basis_status"))
            or maybe_text(gate_payload.get("report_basis_status"))
            or maybe_text(gate_payload.get("report_basis_status"))
            or "withheld"
        )
    if maybe_text(controller_payload.get("report_basis_status")) in {"", "not-evaluated"}:
        controller_payload["report_basis_status"] = controller_payload["report_basis_status"]
    completion_updates = controller_completion_builder(
        controller_payload=controller_payload,
        gate_payload=gate_payload,
        planning=planning,
    )
    controller_payload["recommended_next_skills"] = (
        unique_texts(completion_updates.get("recommended_next_skills", []))
        if isinstance(completion_updates.get("recommended_next_skills"), list)
        else []
    )
    controller_payload["gate_reasons"] = gate_payload.get("gate_reasons", []) if isinstance(gate_payload.get("gate_reasons"), list) else []
    persist_controller_state(run_dir, round_id, controller_payload, gate_payload=gate_payload)

    finished_at = utc_now_iso()
    append_ledger_event(
        run_dir,
        round_controller_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=finished_at,
            contract_mode=contract_mode,
            controller_payload=controller_payload,
            status="completed",
        ),
    )
    return controller_result_payload(controller_payload, gate_payload)
