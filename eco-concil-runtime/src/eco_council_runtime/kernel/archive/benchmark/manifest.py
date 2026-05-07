from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.runtime_command_hints import kernel_command
from eco_council_runtime.kernel.planes.deliberation_plane import load_governed_execution_control_state
from eco_council_runtime.kernel.execution.executor import maybe_text, new_runtime_event_id, utc_now_iso
from eco_council_runtime.kernel.operator.surfaces import (
    build_reporting_surface,
    load_council_decision_wrapper,
    load_final_publication_wrapper,
    load_orchestration_plan_wrapper,
    load_reporting_handoff_wrapper,
    load_supervisor_state_wrapper,
)
from eco_council_runtime.kernel.core.ledger import append_ledger_event, load_ledger_tail
from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.core.paths import (
    benchmark_manifest_path,
    controller_state_path,
    ensure_runtime_dirs,
    history_bootstrap_state_path,
    orchestration_plan_path,
    report_basis_gate_path,
    round_close_state_path,
    scenario_baseline_manifest_path,
    scenario_fixture_path,
    supervisor_state_path,
)
from eco_council_runtime.kernel.archive.benchmark.common import (
    BENCHMARK_EVENT_TYPES,
    INPUT_ARTIFACT_SPECS,
    OUTPUT_ARTIFACT_SPECS,
    artifact_hash_lookup,
    artifact_rows,
    comparison_artifact_rows,
    comparison_step_rows,
    duration_seconds,
    json_hash,
    rounded_number,
    stable_hash,
    summarized_step_rows,
    unique_texts,
)

def governed_execution_state_snapshot(
    run_dir: Path,
    run_id: str,
    round_id: str,
    artifact_hashes: dict[str, str],
) -> dict[str, Any]:
    control_state = load_governed_execution_control_state(run_dir, run_id=run_id, round_id=round_id)
    plan_context = load_orchestration_plan_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        orchestration_plan_path=str(orchestration_plan_path(run_dir, round_id).resolve()),
    )
    plan = (
        plan_context.get("payload")
        if isinstance(plan_context.get("payload"), dict)
        else {}
    )
    gate = (
        control_state.get("report_basis_gate", {})
        if isinstance(control_state.get("report_basis_gate"), dict)
        else control_state.get("report_basis_gate", {})
        if isinstance(control_state.get("report_basis_gate"), dict)
        else {}
    ) or load_json_if_exists(report_basis_gate_path(run_dir, round_id)) or load_json_if_exists(report_basis_gate_path(run_dir, round_id)) or {}
    controller = (
        control_state.get("controller", {})
        if isinstance(control_state.get("controller"), dict)
        else {}
    ) or load_json_if_exists(controller_state_path(run_dir, round_id)) or {}
    supervisor_context = load_supervisor_state_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        supervisor_state_path=str(supervisor_state_path(run_dir, round_id).resolve()),
    )
    supervisor = (
        supervisor_context.get("payload")
        if isinstance(supervisor_context.get("payload"), dict)
        else {}
    )
    handoff_context = load_reporting_handoff_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    decision_draft_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
    )
    decision_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="canonical",
    )
    final_publication_context = load_final_publication_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    reporting_surface = build_reporting_surface(
        supervisor_payload=supervisor,
        handoff_payload=handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else {},
        decision_draft_payload=decision_draft_context.get("payload")
        if isinstance(decision_draft_context.get("payload"), dict)
        else {},
        decision_payload=decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else {},
        final_publication_payload=final_publication_context.get("payload")
        if isinstance(final_publication_context.get("payload"), dict)
        else {},
    )
    steps = summarized_step_rows(run_dir, round_id, controller.get("steps"), artifact_hashes=artifact_hashes)
    summary = {
        "planning_mode": maybe_text(controller.get("planning_mode")) or maybe_text(supervisor.get("planning_mode")) or "missing",
        "controller_status": maybe_text(controller.get("controller_status")) or "missing",
        "supervisor_status": maybe_text(supervisor.get("supervisor_status")) or "missing",
        "supervisor_substatus": maybe_text(supervisor.get("supervisor_substatus")),
        "governed_execution_posture": maybe_text(supervisor.get("governed_execution_posture")),
        "terminal_state": maybe_text(supervisor.get("terminal_state")),
        "readiness_status": maybe_text(supervisor.get("readiness_status")) or maybe_text(controller.get("readiness_status")) or maybe_text(gate.get("readiness_status")) or "unknown",
        "gate_status": maybe_text(supervisor.get("gate_status")) or maybe_text(controller.get("gate_status")) or maybe_text(gate.get("gate_status")) or "unknown",
        "report_basis_status": maybe_text(supervisor.get("report_basis_status")) or maybe_text(controller.get("report_basis_status")) or "unknown",
        "reporting_ready": bool(reporting_surface.get("reporting_ready")),
        "reporting_blockers": (
            reporting_surface.get("reporting_blockers", [])
            if isinstance(reporting_surface.get("reporting_blockers"), list)
            else []
        ),
        "reporting_handoff_status": maybe_text(reporting_surface.get("handoff_status")),
        "reporting_surface_source": maybe_text(reporting_surface.get("surface_source")),
        "publication_status": maybe_text(reporting_surface.get("publication_status")),
        "publication_posture": maybe_text(reporting_surface.get("publication_posture")),
        "resume_status": maybe_text(controller.get("resume_status")),
        "failed_stage": maybe_text(controller.get("failed_stage")),
        "completed_stage_names": [maybe_text(item) for item in controller.get("completed_stage_names", []) if maybe_text(item)]
        if isinstance(controller.get("completed_stage_names"), list)
        else [],
        "pending_stage_names": [maybe_text(item) for item in controller.get("pending_stage_names", []) if maybe_text(item)]
        if isinstance(controller.get("pending_stage_names"), list)
        else [],
        "gate_reasons": [maybe_text(item) for item in controller.get("gate_reasons", []) if maybe_text(item)]
        if isinstance(controller.get("gate_reasons"), list)
        else [],
        "recommended_next_skills": [maybe_text(item) for item in controller.get("recommended_next_skills", []) if maybe_text(item)]
        if isinstance(controller.get("recommended_next_skills"), list)
        else [],
        "planned_stage_sequence": [maybe_text(item) for item in controller.get("planning", {}).get("stage_sequence", []) if maybe_text(item)]
        if isinstance(controller.get("planning"), dict) and isinstance(controller.get("planning", {}).get("stage_sequence"), list)
        else [],
        "planner_probe_stage_included": bool(plan.get("probe_stage_included")),
        "step_count": len(steps),
    }
    comparison = {
        "planning_mode": summary["planning_mode"],
        "controller_status": summary["controller_status"],
        "supervisor_status": summary["supervisor_status"],
        "supervisor_substatus": summary["supervisor_substatus"],
        "governed_execution_posture": summary["governed_execution_posture"],
        "terminal_state": summary["terminal_state"],
        "readiness_status": summary["readiness_status"],
        "gate_status": summary["gate_status"],
        "report_basis_status": summary["report_basis_status"],
        "reporting_ready": summary["reporting_ready"],
        "reporting_blockers": summary["reporting_blockers"],
        "reporting_handoff_status": summary["reporting_handoff_status"],
        "reporting_surface_source": summary["reporting_surface_source"],
        "publication_status": summary["publication_status"],
        "publication_posture": summary["publication_posture"],
        "failed_stage": summary["failed_stage"],
        "completed_stage_names": summary["completed_stage_names"],
        "pending_stage_names": summary["pending_stage_names"],
        "gate_reasons": summary["gate_reasons"],
        "recommended_next_skills": summary["recommended_next_skills"],
        "planned_stage_sequence": summary["planned_stage_sequence"],
        "planner_probe_stage_included": summary["planner_probe_stage_included"],
        "steps": comparison_step_rows(steps),
    }
    return {"summary": summary, "comparison": comparison, "steps": steps}

def post_round_state_snapshot(run_dir: Path, round_id: str, artifact_hashes: dict[str, str]) -> dict[str, Any]:
    round_close = load_json_if_exists(round_close_state_path(run_dir, round_id)) or {}
    history_bootstrap = load_json_if_exists(history_bootstrap_state_path(run_dir, round_id)) or {}
    round_close_steps = summarized_step_rows(run_dir, round_id, round_close.get("steps"), artifact_hashes=artifact_hashes)
    history_steps = summarized_step_rows(run_dir, round_id, history_bootstrap.get("steps"), artifact_hashes=artifact_hashes)
    steps = round_close_steps + history_steps
    round_close_next_skills = (
        [maybe_text(item) for item in round_close.get("recommended_next_skills", []) if maybe_text(item)]
        if isinstance(round_close.get("recommended_next_skills"), list)
        else []
    )
    history_next_skills = (
        [maybe_text(item) for item in history_bootstrap.get("recommended_next_skills", []) if maybe_text(item)]
        if isinstance(history_bootstrap.get("recommended_next_skills"), list)
        else []
    )
    summary = {
        "close_status": maybe_text(round_close.get("close_status")) or "missing",
        "archive_status": maybe_text(round_close.get("archive_status")) or "missing",
        "close_posture": maybe_text(round_close.get("close_posture")),
        "publication_status": maybe_text(round_close.get("publication_status")),
        "publication_posture": maybe_text(round_close.get("publication_posture")),
        "bootstrap_status": maybe_text(history_bootstrap.get("bootstrap_status")) or "missing",
        "selected_case_count": int(history_bootstrap.get("selected_case_count") or 0),
        "selected_signal_count": int(history_bootstrap.get("selected_signal_count") or 0),
        "failed_stage": maybe_text(round_close.get("failed_stage")) or maybe_text(history_bootstrap.get("failed_stage")),
        "recommended_next_skills": unique_texts(round_close_next_skills + history_next_skills),
        "warning_count": len(round_close.get("warnings", [])) if isinstance(round_close.get("warnings"), list) else 0,
        "step_count": len(steps),
    }
    comparison = {
        "close_status": summary["close_status"],
        "archive_status": summary["archive_status"],
        "close_posture": summary["close_posture"],
        "publication_status": summary["publication_status"],
        "publication_posture": summary["publication_posture"],
        "bootstrap_status": summary["bootstrap_status"],
        "selected_case_count": summary["selected_case_count"],
        "selected_signal_count": summary["selected_signal_count"],
        "failed_stage": summary["failed_stage"],
        "recommended_next_skills": summary["recommended_next_skills"],
        "warning_count": summary["warning_count"],
        "steps": comparison_step_rows(steps),
    }
    return {"summary": summary, "comparison": comparison, "steps": steps}

def core_ledger_events(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    events = load_ledger_tail(run_dir, 1_000_000)
    return [
        event
        for event in events
        if isinstance(event, dict)
        and maybe_text(event.get("round_id")) == round_id
        and maybe_text(event.get("event_type")) not in BENCHMARK_EVENT_TYPES
    ]

def skill_timing_summary(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        skill_name = maybe_text(event.get("skill_name"))
        if not skill_name:
            continue
        bucket = buckets.setdefault(
            skill_name,
            {
                "skill_name": skill_name,
                "event_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "blocked_count": 0,
                "total_duration_seconds": 0.0,
                "max_duration_seconds": 0.0,
                "total_attempt_count": 0,
                "recovered_after_retry_count": 0,
            },
        )
        bucket["event_count"] += 1
        status = maybe_text(event.get("status"))
        if status == "completed":
            bucket["completed_count"] += 1
        elif status == "failed":
            bucket["failed_count"] += 1
        elif status == "blocked":
            bucket["blocked_count"] += 1
        duration = duration_seconds(event.get("started_at_utc"), event.get("completed_at_utc"))
        if duration is not None:
            bucket["total_duration_seconds"] += duration
            bucket["max_duration_seconds"] = max(bucket["max_duration_seconds"], duration)
        bucket["total_attempt_count"] += int(event.get("attempt_count") or 0)
        if bool(event.get("recovered_after_retry")):
            bucket["recovered_after_retry_count"] += 1
    rows: list[dict[str, Any]] = []
    for skill_name in sorted(buckets):
        bucket = buckets[skill_name]
        event_count = int(bucket["event_count"] or 0)
        rows.append(
            {
                "skill_name": skill_name,
                "event_count": event_count,
                "completed_count": int(bucket["completed_count"] or 0),
                "failed_count": int(bucket["failed_count"] or 0),
                "blocked_count": int(bucket["blocked_count"] or 0),
                "total_duration_seconds": rounded_number(bucket["total_duration_seconds"]),
                "average_duration_seconds": rounded_number(bucket["total_duration_seconds"] / event_count) if event_count else 0.0,
                "max_duration_seconds": rounded_number(bucket["max_duration_seconds"]),
                "total_attempt_count": int(bucket["total_attempt_count"] or 0),
                "recovered_after_retry_count": int(bucket["recovered_after_retry_count"] or 0),
            }
        )
    return rows

def round_event_summary(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        event_type = maybe_text(event.get("event_type"))
        if not event_type:
            continue
        bucket = buckets.setdefault(
            event_type,
            {
                "event_type": event_type,
                "event_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "blocked_count": 0,
                "degraded_count": 0,
                "total_duration_seconds": 0.0,
                "max_duration_seconds": 0.0,
            },
        )
        bucket["event_count"] += 1
        status = maybe_text(event.get("status"))
        if status == "completed":
            bucket["completed_count"] += 1
        elif status == "failed":
            bucket["failed_count"] += 1
        elif status == "blocked":
            bucket["blocked_count"] += 1
        elif status == "completed-with-warnings":
            bucket["degraded_count"] += 1
        duration = duration_seconds(event.get("started_at_utc"), event.get("completed_at_utc"))
        if duration is not None:
            bucket["total_duration_seconds"] += duration
            bucket["max_duration_seconds"] = max(bucket["max_duration_seconds"], duration)
    rows: list[dict[str, Any]] = []
    for event_type in sorted(buckets):
        bucket = buckets[event_type]
        event_count = int(bucket["event_count"] or 0)
        rows.append(
            {
                "event_type": event_type,
                "event_count": event_count,
                "completed_count": int(bucket["completed_count"] or 0),
                "failed_count": int(bucket["failed_count"] or 0),
                "blocked_count": int(bucket["blocked_count"] or 0),
                "degraded_count": int(bucket["degraded_count"] or 0),
                "total_duration_seconds": rounded_number(bucket["total_duration_seconds"]),
                "average_duration_seconds": rounded_number(bucket["total_duration_seconds"] / event_count) if event_count else 0.0,
                "max_duration_seconds": rounded_number(bucket["max_duration_seconds"]),
            }
        )
    return rows

def failure_summary(
    events: list[dict[str, Any]],
    *,
    governed_execution: dict[str, Any],
    post_round: dict[str, Any],
) -> dict[str, Any]:
    event_failures: list[dict[str, Any]] = []
    failed_event_types: list[str] = []
    failed_skills: list[str] = []
    blocked_event_count = 0
    failed_event_count = 0
    degraded_event_count = 0
    for event in events:
        status = maybe_text(event.get("status"))
        if status == "failed":
            failed_event_count += 1
            failed_event_types.append(maybe_text(event.get("event_type")))
            failed_skills.append(maybe_text(event.get("skill_name")))
        elif status == "blocked":
            blocked_event_count += 1
            failed_event_types.append(maybe_text(event.get("event_type")))
            failed_skills.append(maybe_text(event.get("skill_name")))
        elif status == "completed-with-warnings":
            degraded_event_count += 1
        if status not in {"failed", "blocked", "completed-with-warnings"}:
            continue
        failure = event.get("failure", {}) if isinstance(event.get("failure"), dict) else {}
        event_failures.append(
            {
                "event_type": maybe_text(event.get("event_type")),
                "skill_name": maybe_text(event.get("skill_name")),
                "status": status,
                "failed_stage": maybe_text(event.get("failed_stage")),
                "error_code": maybe_text(failure.get("error_code")),
                "message": maybe_text(failure.get("message")),
            }
        )
    failed_stage_names = unique_texts(
        [governed_execution.get("summary", {}).get("failed_stage"), post_round.get("summary", {}).get("failed_stage")]
        + [event.get("failed_stage") for event in event_failures]
    )
    return {
        "failed_event_count": failed_event_count,
        "blocked_event_count": blocked_event_count,
        "degraded_event_count": degraded_event_count,
        "failing_event_types": unique_texts(failed_event_types),
        "failing_skills": unique_texts(failed_skills),
        "failed_stage_names": failed_stage_names,
        "event_failures": event_failures,
    }

def benchmark_manifest_payload(run_dir: Path, run_id: str, round_id: str) -> dict[str, Any]:
    input_artifacts = artifact_rows(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        specs=INPUT_ARTIFACT_SPECS,
        category="input",
    )
    output_artifacts = artifact_rows(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        specs=OUTPUT_ARTIFACT_SPECS,
        category="output",
    )
    output_hashes = artifact_hash_lookup(output_artifacts)
    governed_execution = governed_execution_state_snapshot(run_dir, run_id, round_id, output_hashes)
    post_round = post_round_state_snapshot(run_dir, round_id, output_hashes)
    events = core_ledger_events(run_dir, round_id)
    failure = failure_summary(events, governed_execution=governed_execution, post_round=post_round)
    comparison_inputs = comparison_artifact_rows(input_artifacts)
    comparison_outputs = comparison_artifact_rows(output_artifacts)
    comparison_basis = {
        "scenario_fingerprint": json_hash(comparison_inputs),
        "governed_execution": governed_execution["comparison"],
        "post_round": post_round["comparison"],
        "artifact_outputs": comparison_outputs,
    }
    output_fingerprint = json_hash(comparison_basis)
    summary = {
        "scenario_input_count": len(input_artifacts),
        "output_artifact_count": len(output_artifacts),
        "present_output_artifact_count": len([row for row in output_artifacts if bool(row.get("payload_present"))]),
        "artifact_file_output_count": len([row for row in output_artifacts if bool(row.get("artifact_present"))]),
        "failed_event_count": failure["failed_event_count"],
        "blocked_event_count": failure["blocked_event_count"],
        "controller_status": governed_execution["summary"]["controller_status"],
        "supervisor_status": governed_execution["summary"]["supervisor_status"],
        "reporting_ready": governed_execution["summary"]["reporting_ready"],
        "close_status": post_round["summary"]["close_status"],
        "bootstrap_status": post_round["summary"]["bootstrap_status"],
    }
    return {
        "schema_version": "runtime-benchmark-manifest-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "scenario_fingerprint": comparison_basis["scenario_fingerprint"],
        "output_fingerprint": output_fingerprint,
        "summary": summary,
        "scenario_inputs": input_artifacts,
        "artifact_outputs": output_artifacts,
        "governed_execution_summary": governed_execution["summary"],
        "post_round_summary": post_round["summary"],
        "round_step_summary": {
            "governed_execution": governed_execution["steps"],
            "post_round": post_round["steps"],
        },
        "skill_timing_summary": skill_timing_summary(events),
        "round_event_summary": round_event_summary(events),
        "failure_summary": failure,
        "comparison_basis": comparison_basis,
    }

def benchmark_manifest_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "benchmark-manifest", started_at, completed_at),
        "event_type": "benchmark-manifest",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "scenario_fingerprint": payload.get("scenario_fingerprint"),
        "output_fingerprint": payload.get("output_fingerprint"),
        "benchmark_manifest_path": str(output_path),
    }

def materialize_benchmark_manifest(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    payload = benchmark_manifest_payload(run_dir, run_id, round_id)
    output_path = benchmark_manifest_path(run_dir, round_id)
    write_json(output_path, payload)
    append_ledger_event(
        run_dir,
        benchmark_manifest_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            payload=payload,
            output_path=output_path,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "benchmark_manifest_path": str(output_path),
            "scenario_fingerprint": payload["scenario_fingerprint"],
            "output_fingerprint": payload["output_fingerprint"],
            "failed_event_count": payload["summary"]["failed_event_count"],
            "blocked_event_count": payload["summary"]["blocked_event_count"],
        },
        "benchmark_manifest": payload,
    }

def scenario_fixture_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "scenario-fixture", started_at, completed_at),
        "event_type": "scenario-fixture",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "scenario_id": payload.get("scenario_id"),
        "scenario_fingerprint": payload.get("scenario_fingerprint"),
        "fixture_path": str(output_path),
    }

def materialize_scenario_fixture(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    scenario_id: str = "",
    baseline_manifest_override: str = "",
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    if maybe_text(baseline_manifest_override):
        baseline_manifest_path_value = Path(baseline_manifest_override).expanduser().resolve()
        if not baseline_manifest_path_value.exists():
            raise ValueError(f"Missing benchmark manifest for scenario fixture: {baseline_manifest_path_value}")
        baseline_payload = load_json_if_exists(baseline_manifest_path_value) or {}
    else:
        baseline_manifest_path_value = benchmark_manifest_path(run_dir, round_id)
        if not baseline_manifest_path_value.exists():
            baseline_payload = materialize_benchmark_manifest(run_dir, run_id=run_id, round_id=round_id)["benchmark_manifest"]
        else:
            baseline_payload = load_json_if_exists(baseline_manifest_path_value) or {}
    if not baseline_payload:
        raise ValueError(f"Missing benchmark manifest for scenario fixture: {baseline_manifest_path_value}")
    fixture_path = scenario_fixture_path(run_dir, round_id)
    frozen_baseline_path = scenario_baseline_manifest_path(run_dir, round_id)
    write_json(frozen_baseline_path, baseline_payload)
    resolved_scenario_id = maybe_text(scenario_id) or f"scenario-{stable_hash(run_id, round_id, baseline_payload.get('scenario_fingerprint'))[:12]}"
    payload = {
        "schema_version": "runtime-scenario-fixture-v1",
        "generated_at_utc": utc_now_iso(),
        "scenario_id": resolved_scenario_id,
        "run_id": run_id,
        "round_id": round_id,
        "scenario_fingerprint": baseline_payload.get("scenario_fingerprint", ""),
        "scenario_identity": {
            "run_id": run_id,
            "round_id": round_id,
            "identity_policy": "benchmark-replay-must-preserve-run-and-round-ids",
        },
        "scenario_inputs": baseline_payload.get("scenario_inputs", []),
        "expected_terminal_posture": {
            "governed_execution": baseline_payload.get("governed_execution_summary", {}),
            "post_round": baseline_payload.get("post_round_summary", {}),
        },
        "expected_artifacts": baseline_payload.get("comparison_basis", {}).get("artifact_outputs", []),
        "baseline_manifest": {
            "path": str(frozen_baseline_path),
            "source_path": str(baseline_manifest_path_value),
            "output_fingerprint": maybe_text(baseline_payload.get("output_fingerprint")),
            "scenario_fingerprint": maybe_text(baseline_payload.get("scenario_fingerprint")),
        },
        "replay_contract": {
            "benchmark_command_template": kernel_command(
                "materialize-benchmark-manifest",
                "--run-dir",
                "<candidate-run-dir>",
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            ),
            "compare_command_template": kernel_command(
                "compare-benchmark-manifests",
                "--run-dir",
                "<candidate-run-dir>",
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--left-manifest-path",
                str(frozen_baseline_path),
                "--right-manifest-path",
                f"<candidate-run-dir>/runtime/benchmark_manifest_{round_id}.json",
            ),
            "replay_command_template": kernel_command(
                "replay-runtime-scenario",
                "--run-dir",
                "<candidate-run-dir>",
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--fixture-path",
                str(fixture_path.resolve()),
            ),
            "replay_steps": [
                "Re-run the fixed scenario with the same run_id and round_id.",
                "Materialize the candidate benchmark manifest if needed.",
                "Run replay-runtime-scenario against this fixture to compare outputs.",
            ],
        },
    }
    write_json(fixture_path, payload)
    append_ledger_event(
        run_dir,
        scenario_fixture_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            payload=payload,
            output_path=fixture_path,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "scenario_id": resolved_scenario_id,
            "scenario_fixture_path": str(fixture_path),
            "scenario_fingerprint": payload["scenario_fingerprint"],
        },
        "scenario_fixture": payload,
    }
