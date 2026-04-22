from __future__ import annotations

from pathlib import Path
from typing import Any

from ..phase2_controller_state import (
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
from ..phase2_direct_advisory import materialize_direct_council_advisory_plan
from ..phase2_posture_profile import (
    posture_profile_callable,
    resolve_phase2_posture_profile,
)
from ..phase2_planning_profile import (
    advisory_planning_bundle as advisory_planning_bundle_from_profile,
    agent_orchestration_requested as agent_orchestration_requested_from_profile,
    ensure_executable_planning as ensure_executable_planning_from_profile,
    normalized_controller_planning_mode as normalized_controller_planning_mode_from_profile,
    planner_skill_args_for_source,
    planning_bundle as planning_bundle_from_result,
    planning_from_controller as planning_from_controller_from_profile,
    planning_source_output_path,
    relative_runtime_path as relative_runtime_path_from_profile,
    resolve_phase2_planning_sources,
)
from ..phase2_stage_profile import (
    DEFAULT_PHASE2_PLANNER_SKILL_NAME,
    resolve_stage_definitions,
)
from .deliberation_plane import (
    load_phase2_control_state,
    store_orchestration_plan_record,
    store_promotion_freeze_record,
)
from .executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill, utc_now_iso
from .gate import GateHandler, execute_gate_step as execute_runtime_gate_step
from .ledger import append_ledger_event
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists, write_json
from .paths import (
    agent_advisory_plan_path,
    agent_entry_gate_path,
    controller_state_path,
    ensure_runtime_dirs,
    mission_scaffold_path,
    orchestration_plan_path,
    promotion_gate_path,
)
from .registry import write_registry

PLANNER_SKILL_NAME = DEFAULT_PHASE2_PLANNER_SKILL_NAME


def phase2_artifact_paths(run_dir: Path, round_id: str) -> dict[str, str]:
    return {
        "agent_advisory_plan_path": str(agent_advisory_plan_path(run_dir, round_id).resolve()),
        "agent_entry_gate_path": str(agent_entry_gate_path(run_dir, round_id).resolve()),
        "board_summary_path": str((run_dir / "board" / f"board_state_summary_{round_id}.json").resolve()),
        "board_brief_path": str((run_dir / "board" / f"board_brief_{round_id}.md").resolve()),
        "mission_scaffold_path": str(mission_scaffold_path(run_dir, round_id).resolve()),
        "next_actions_path": str((run_dir / "investigation" / f"next_actions_{round_id}.json").resolve()),
        "probes_path": str((run_dir / "investigation" / f"falsification_probes_{round_id}.json").resolve()),
        "readiness_path": str((run_dir / "reporting" / f"round_readiness_{round_id}.json").resolve()),
        "orchestration_plan_path": str(orchestration_plan_path(run_dir, round_id).resolve()),
        "promotion_gate_path": str(promotion_gate_path(run_dir, round_id).resolve()),
        "promotion_basis_path": str((run_dir / "promotion" / f"promoted_evidence_basis_{round_id}.json").resolve()),
        "controller_state_path": str(controller_state_path(run_dir, round_id).resolve()),
    }


def normalized_controller_planning_mode(value: Any, *, default: str = "planner-backed") -> str:
    return normalized_controller_planning_mode_from_profile(value, default=default)


def relative_runtime_path(run_dir: Path, path: Path) -> str:
    return relative_runtime_path_from_profile(run_dir, path)


def agent_orchestration_requested(run_dir: Path, round_id: str) -> bool:
    return agent_orchestration_requested_from_profile(run_dir, round_id)


def planning_bundle(run_dir: Path, round_id: str, planner_result: dict[str, Any]) -> dict[str, Any]:
    return planning_bundle_from_result(
        run_dir,
        round_id,
        planner_result,
        planner_skill_name=PLANNER_SKILL_NAME,
    )


def advisory_planning_bundle(run_dir: Path, round_id: str) -> dict[str, Any]:
    return advisory_planning_bundle_from_profile(
        run_dir,
        round_id,
        planner_skill_name=PLANNER_SKILL_NAME,
    )


def planning_from_controller(run_dir: Path, round_id: str, controller_payload: dict[str, Any]) -> dict[str, Any]:
    return planning_from_controller_from_profile(
        run_dir,
        round_id,
        controller_payload,
        planner_skill_name=PLANNER_SKILL_NAME,
    )


def ensure_executable_planning(planning: dict[str, Any]) -> None:
    ensure_executable_planning_from_profile(planning)


def persist_controller_state(
    run_dir: Path,
    round_id: str,
    controller_payload: dict[str, Any],
    *,
    gate_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    controller_payload["artifacts"] = phase2_artifact_paths(run_dir, round_id)
    refreshed_payload = refresh_controller_payload(controller_payload)
    write_json(controller_state_path(run_dir, round_id), refreshed_payload)
    store_promotion_freeze_record(
        run_dir,
        run_id=maybe_text(refreshed_payload.get("run_id")),
        round_id=round_id,
        controller_snapshot=refreshed_payload,
        gate_snapshot=gate_payload,
        artifact_paths=refreshed_payload.get("artifacts", {})
        if isinstance(refreshed_payload.get("artifacts"), dict)
        else {},
    )
    return refreshed_payload


def execute_gate_step(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    blueprint: dict[str, Any],
    stage_contracts: dict[str, Any] | None = None,
    gate_handlers: dict[str, GateHandler] | None = None,
) -> dict[str, Any]:
    return execute_runtime_gate_step(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        blueprint=blueprint,
        stage_contracts=stage_contracts,
        gate_handlers=gate_handlers,
    )


def run_phase2_round(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    gate_handlers: dict[str, GateHandler] | None,
    posture_profile: dict[str, Any] | None = None,
    planning_sources: list[dict[str, Any]] | None = None,
    stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return run_phase2_round_with_contract_mode(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode="warn",
        gate_handlers=gate_handlers,
        posture_profile=posture_profile,
        planning_sources=planning_sources,
        stage_definitions=stage_definitions,
    )


def run_phase2_round_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
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
    profile = resolve_phase2_posture_profile(posture_profile)
    controller_completion_builder = posture_profile_callable(
        profile,
        "controller_completion_builder",
    )
    ensure_runtime_dirs(run_dir)
    write_registry(run_dir)
    init_run_manifest(run_dir, run_id)
    init_round_cursor(run_dir, run_id)
    artifacts = phase2_artifact_paths(run_dir, round_id)
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
    resolved_planning_sources = resolve_phase2_planning_sources(planning_sources)
    resolved_stage_definitions = resolve_stage_definitions(stage_definitions)

    phase2_control_state = load_phase2_control_state(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    existing_controller = (
        phase2_control_state.get("controller", {})
        if isinstance(phase2_control_state.get("controller"), dict)
        else {}
    ) or load_json_if_exists(controller_state_path(run_dir, round_id)) or {}
    existing_gate = (
        phase2_control_state.get("promotion_gate", {})
        if isinstance(phase2_control_state.get("promotion_gate"), dict)
        else {}
    ) or load_json_if_exists(Path(artifacts["promotion_gate_path"])) or {}
    existing_status = maybe_text(existing_controller.get("controller_status"))
    if not force_restart and existing_status == "completed":
        return controller_result_payload(existing_controller, existing_gate)

    started_at = utc_now_iso()
    resume_status = "restart-forced" if force_restart else "fresh-run"
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

    if not force_restart and existing_status in {"running", "failed"}:
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
        controller_payload["stage_contracts"] = stage_contracts_from_blueprints(
            blueprints
        )
        controller_payload["steps"] = merge_existing_steps(
            blueprints,
            controller_payload.get("steps"),
        )
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
        planner_stage_ran = True

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

    if not planning:
        for source_spec in resolved_planning_sources:
            source_name = maybe_text(source_spec.get("source_name"))
            source_kind = maybe_text(source_spec.get("source_kind"))
            if (
                bool(source_spec.get("requires_agent_orchestration"))
                and not agent_orchestration_requested(run_dir, round_id)
            ):
                continue
            if source_kind == "existing-advisory":
                advisory_planning = advisory_planning_bundle(run_dir, round_id)
                if not advisory_planning:
                    continue
                adopted_source = (
                    maybe_text(advisory_planning.get("plan_source")) or source_name
                )
                adopt_planning(
                    advisory_planning,
                    source_name=adopted_source,
                    status="adopted",
                    message=(
                        maybe_text(source_spec.get("adopted_message"))
                        or "Controller adopted a pre-materialized advisory plan."
                    ),
                )
                break

            output_path = planning_source_output_path(
                run_dir,
                round_id,
                artifacts,
                source_spec,
            )
            planner_index = start_planner_attempt(output_path)
            if source_kind == "direct-council-advisory":
                try:
                    direct_result = materialize_direct_council_advisory_plan(
                        run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        output_path=relative_runtime_path(run_dir, output_path),
                        contract_mode=contract_mode,
                    )
                except Exception as exc:
                    append_planning_attempt(
                        controller_payload,
                        planning_attempt_record(
                            source=source_name,
                            status="failed",
                            plan_path=str(output_path.resolve()),
                            message=str(exc),
                        ),
                    )
                    persist_controller_state(run_dir, round_id, controller_payload)
                    continue
                selected_planning = advisory_planning_bundle(run_dir, round_id)
                if direct_result and selected_planning:
                    adopt_planning(
                        selected_planning,
                        source_name=(
                            maybe_text(selected_planning.get("plan_source"))
                            or source_name
                        ),
                        status="materialized",
                        message=(
                            maybe_text(source_spec.get("materialized_message"))
                            or "Controller compiled and selected a direct council advisory plan."
                        ),
                        planner_result=direct_result,
                    )
                    break
                append_planning_attempt(
                    controller_payload,
                    planning_attempt_record(
                        source=source_name,
                        status="failed" if direct_result else "unavailable",
                        plan_path=str(output_path.resolve()),
                        message=(
                            maybe_text(source_spec.get("failed_message"))
                            if direct_result
                            else maybe_text(source_spec.get("unavailable_message"))
                        ),
                    ),
                )
                persist_controller_state(run_dir, round_id, controller_payload)
                continue

            if source_kind != "planner-skill":
                continue
            skill_name = maybe_text(source_spec.get("planner_skill_name")) or PLANNER_SKILL_NAME
            skill_args = planner_skill_args_for_source(
                run_dir,
                source_spec,
                output_path,
            )
            try:
                planner_result = run_skill(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    skill_name=skill_name,
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

            selected_planning = (
                advisory_planning_bundle(run_dir, round_id)
                if maybe_text(source_spec.get("output_path_key"))
                == "agent_advisory_plan_path"
                else planning_bundle(run_dir, round_id, planner_result)
            )
            if selected_planning:
                adopt_planning(
                    selected_planning,
                    source_name=(
                        maybe_text(selected_planning.get("plan_source")) or source_name
                    ),
                    status="materialized",
                    message=(
                        maybe_text(source_spec.get("materialized_message"))
                        or "Controller materialized a phase-2 planning source."
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
            message="No injected phase-2 planning source produced a usable execution_queue.",
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
                        "readiness_status": gate_payload.get("readiness_status"),
                        "promote_allowed": bool(gate_payload.get("promote_allowed")),
                        "gate_path": gate_payload.get("output_path"),
                        "readiness_stage_name": readiness_stage_name,
                    },
                )
                controller_payload["steps"][step_pos] = gate_stage_summary(blueprint, gate_payload, gate_event_id, gate_started_at)
                controller_payload["readiness_status"] = maybe_text(gate_updates.get("readiness_status")) or "blocked"
                controller_payload["gate_status"] = maybe_text(gate_updates.get("gate_status")) or "freeze-withheld"
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

            skill_result = run_skill(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name=maybe_text(blueprint.get("skill_name")),
                skill_args=blueprint.get("skill_args", []) if isinstance(blueprint.get("skill_args"), list) else [],
                contract_mode=contract_mode,
                **execution_kwargs,
            )
            controller_payload["steps"][step_pos] = stage_summary_from_result(stage_name, skill_result, blueprint)
            if stage_name == "round-readiness":
                readiness_summary = skill_result.get("skill_payload", {}) if isinstance(skill_result.get("skill_payload"), dict) else {}
                controller_payload["readiness_status"] = maybe_text(readiness_summary.get("summary", {}).get("readiness_status"))
            if stage_name == "promotion-basis":
                promotion_payload = skill_result.get("skill_payload", {}) if isinstance(skill_result.get("skill_payload"), dict) else {}
                promotion_summary = promotion_payload.get("summary", {}) if isinstance(promotion_payload.get("summary"), dict) else {}
                controller_payload["promotion_status"] = maybe_text(promotion_summary.get("promotion_status")) or "withheld"
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

    phase2_control_state = load_phase2_control_state(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    gate_payload = (
        phase2_control_state.get("promotion_gate", {})
        if isinstance(phase2_control_state.get("promotion_gate"), dict)
        else {}
    ) or load_json_if_exists(Path(artifacts["promotion_gate_path"])) or {}
    controller_payload["controller_status"] = "completed"
    controller_payload["planning_mode"] = normalized_controller_planning_mode(
        maybe_text(planning.get("planning_mode"))
        or maybe_text(controller_payload.get("planning_mode"))
    )
    controller_payload["readiness_status"] = maybe_text(gate_payload.get("readiness_status")) or maybe_text(controller_payload.get("readiness_status")) or "blocked"
    controller_payload["gate_status"] = maybe_text(gate_payload.get("gate_status")) or maybe_text(controller_payload.get("gate_status")) or "freeze-withheld"
    if maybe_text(controller_payload.get("promotion_status")) in {"", "not-evaluated"}:
        promotion_basis_payload = (
            phase2_control_state.get("promotion_basis", {})
            if isinstance(phase2_control_state.get("promotion_basis"), dict)
            else {}
        )
        controller_payload["promotion_status"] = maybe_text(promotion_basis_payload.get("promotion_status")) or "withheld"
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
