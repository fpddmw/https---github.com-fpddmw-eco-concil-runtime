from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .benchmark import (
    compare_benchmark_manifests,
    materialize_benchmark_manifest,
    materialize_scenario_fixture,
    replay_runtime_scenario,
)
from .controller import run_phase2_round, run_phase2_round_with_contract_mode
from .executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill
from .governance import CONTRACT_MODES, preflight_skill_execution
from .gate import apply_promotion_gate
from .ledger import append_ledger_event, load_ledger_tail
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists
from .operations import (
    PERMISSION_PROFILES,
    load_admission_policy,
    load_dead_letters,
    materialize_admission_policy,
    materialize_operator_runbook,
    materialize_runtime_health,
    runtime_health_payload,
)
from .post_round import (
    ARCHIVE_FAILURE_POLICIES,
    bootstrap_history_context_with_contract_mode,
    close_round_with_contract_mode,
)
from .paths import (
    admission_policy_path,
    benchmark_compare_path,
    benchmark_manifest_path,
    controller_state_path,
    cursor_path,
    ensure_runtime_dirs,
    history_bootstrap_state_path,
    ledger_path,
    manifest_path,
    orchestration_plan_path,
    operator_runbook_path,
    promotion_gate_path,
    replay_report_path,
    registry_path,
    resolve_run_dir,
    round_close_state_path,
    runtime_health_path,
    scenario_fixture_path,
    supervisor_state_path,
)
from .registry import write_registry
from .supervisor import supervise_round, supervise_round_with_contract_mode


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def add_execution_policy_args(command: argparse.ArgumentParser) -> None:
    command.add_argument("--timeout-seconds", type=float, default=None)
    command.add_argument("--retry-budget", type=int, default=None)
    command.add_argument("--retry-backoff-ms", type=int, default=None)
    command.add_argument("--allow-side-effect", action="append", default=[])


def add_admission_policy_args(command: argparse.ArgumentParser) -> None:
    command.add_argument("--permission-profile", default="standard", choices=PERMISSION_PROFILES)
    command.add_argument("--max-timeout-seconds", type=float, default=None)
    command.add_argument("--max-retry-budget", type=int, default=None)
    command.add_argument("--max-retry-backoff-ms", type=int, default=None)
    command.add_argument("--default-allow-side-effect", action="append", default=[])
    command.add_argument("--approval-required-side-effect", action="append", default=[])
    command.add_argument("--blocked-side-effect", action="append", default=[])
    command.add_argument("--allowed-read-root", action="append", default=[])
    command.add_argument("--allowed-write-root", action="append", default=[])
    command.add_argument("--allowed-cwd-root", action="append", default=[])


def init_run(run_dir: Path, run_id: str) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    registry = write_registry(run_dir)
    manifest = init_run_manifest(run_dir, run_id)
    cursor = init_round_cursor(run_dir, run_id)
    if not admission_policy_path(run_dir).exists():
        materialize_admission_policy(run_dir, run_id=run_id)
    if not runtime_health_path(run_dir).exists():
        materialize_runtime_health(run_dir)
    if not operator_runbook_path(run_dir).exists():
        materialize_operator_runbook(run_dir)
    return {
        "status": "completed",
        "summary": {"run_id": run_id, "run_dir": str(run_dir), "skill_count": int(registry.get("skill_count") or 0)},
        "manifest": manifest,
        "cursor": cursor,
        "registry": registry,
        "paths": {
            "manifest_path": str(manifest_path(run_dir)),
            "cursor_path": str(cursor_path(run_dir)),
            "ledger_path": str(ledger_path(run_dir)),
            "registry_path": str(registry_path(run_dir)),
            "admission_policy_path": str(admission_policy_path(run_dir)),
            "runtime_health_path": str(runtime_health_path(run_dir)),
            "operator_runbook_path": str(operator_runbook_path(run_dir)),
        },
    }


def phase2_operator_view(run_dir: Path, round_id: str, phase2_state: dict[str, Any]) -> dict[str, Any]:
    plan = phase2_state.get("plan", {}) if isinstance(phase2_state.get("plan"), dict) else {}
    gate = phase2_state.get("promotion_gate", {}) if isinstance(phase2_state.get("promotion_gate"), dict) else {}
    controller = phase2_state.get("controller", {}) if isinstance(phase2_state.get("controller"), dict) else {}
    supervisor = phase2_state.get("supervisor", {}) if isinstance(phase2_state.get("supervisor"), dict) else {}
    run_id = (
        maybe_text(supervisor.get("run_id"))
        or maybe_text(controller.get("run_id"))
        or maybe_text(plan.get("run_id"))
        or maybe_text(gate.get("run_id"))
    )
    resume_command = maybe_text(supervisor.get("resume_command")) or (
        f"resume-phase2-round --run-dir {run_dir} --run-id {run_id} --round-id {round_id}" if round_id and run_id else ""
    )
    restart_command = maybe_text(supervisor.get("restart_command")) or (
        f"restart-phase2-round --run-dir {run_dir} --run-id {run_id} --round-id {round_id}" if round_id and run_id else ""
    )
    inspect_command = maybe_text(supervisor.get("inspect_command")) or (
        f"show-run-state --run-dir {run_dir} --round-id {round_id} --tail 20" if round_id else ""
    )
    return {
        "round_id": round_id,
        "planning_mode": maybe_text(controller.get("planning_mode")) or maybe_text(supervisor.get("planning_mode")),
        "controller_status": maybe_text(controller.get("controller_status")) or "missing",
        "supervisor_status": maybe_text(supervisor.get("supervisor_status")),
        "supervisor_substatus": maybe_text(supervisor.get("supervisor_substatus")),
        "phase2_posture": maybe_text(supervisor.get("phase2_posture")),
        "terminal_state": maybe_text(supervisor.get("terminal_state")),
        "readiness_status": maybe_text(controller.get("readiness_status")) or maybe_text(supervisor.get("readiness_status")),
        "gate_status": maybe_text(controller.get("gate_status")) or maybe_text(gate.get("gate_status")),
        "promotion_status": maybe_text(controller.get("promotion_status")) or maybe_text(supervisor.get("promotion_status")),
        "current_stage": maybe_text(controller.get("current_stage")),
        "failed_stage": maybe_text(controller.get("failed_stage")),
        "completed_stage_names": controller.get("completed_stage_names", []) if isinstance(controller.get("completed_stage_names"), list) else [],
        "pending_stage_names": controller.get("pending_stage_names", []) if isinstance(controller.get("pending_stage_names"), list) else [],
        "resume_recommended": bool(controller.get("resume_recommended")) or bool(supervisor.get("resume_recommended")),
        "restart_recommended": bool(controller.get("restart_recommended")) or bool(supervisor.get("restart_recommended")),
        "resume_from_stage": maybe_text(controller.get("recovery", {}).get("resume_from_stage"))
        if isinstance(controller.get("recovery"), dict)
        else maybe_text(supervisor.get("resume_from_stage")),
        "resume_command": resume_command,
        "restart_command": restart_command,
        "inspect_command": inspect_command,
        "inspection_paths": {
            "plan_path": maybe_text(controller.get("artifacts", {}).get("orchestration_plan_path"))
            if isinstance(controller.get("artifacts"), dict)
            else maybe_text(supervisor.get("orchestration_plan_path")),
            "controller_path": maybe_text(controller.get("artifacts", {}).get("controller_state_path"))
            if isinstance(controller.get("artifacts"), dict)
            else maybe_text(supervisor.get("controller_path")),
            "gate_path": maybe_text(controller.get("artifacts", {}).get("promotion_gate_path"))
            if isinstance(controller.get("artifacts"), dict)
            else maybe_text(supervisor.get("promotion_gate_path")),
            "supervisor_path": str(supervisor_state_path(run_dir, round_id).resolve()) if round_id else "",
        },
        "operator_notes": supervisor.get("operator_notes", []) if isinstance(supervisor.get("operator_notes"), list) else [],
    }


def post_round_operator_view(run_dir: Path, round_id: str, post_round_state: dict[str, Any]) -> dict[str, Any]:
    round_close = post_round_state.get("round_close", {}) if isinstance(post_round_state.get("round_close"), dict) else {}
    history_bootstrap = post_round_state.get("history_bootstrap", {}) if isinstance(post_round_state.get("history_bootstrap"), dict) else {}
    run_id = maybe_text(round_close.get("run_id")) or maybe_text(history_bootstrap.get("run_id"))
    return {
        "round_close_status": maybe_text(round_close.get("close_status")),
        "archive_status": maybe_text(round_close.get("archive_status")),
        "close_posture": maybe_text(round_close.get("close_posture")),
        "history_bootstrap_status": maybe_text(history_bootstrap.get("bootstrap_status")),
        "selected_case_count": int(history_bootstrap.get("selected_case_count") or 0),
        "selected_signal_count": int(history_bootstrap.get("selected_signal_count") or 0),
        "close_command": f"close-round --run-dir {run_dir} --run-id {run_id} --round-id {round_id}" if run_id and round_id else "",
        "history_command": f"bootstrap-history-context --run-dir {run_dir} --run-id {run_id} --round-id {round_id}" if run_id and round_id else "",
        "round_close_path": str(round_close_state_path(run_dir, round_id).resolve()) if round_id else "",
        "history_bootstrap_path": str(history_bootstrap_state_path(run_dir, round_id).resolve()) if round_id else "",
    }


def benchmark_operator_view(run_dir: Path, round_id: str, benchmark_state: dict[str, Any]) -> dict[str, Any]:
    fixture = benchmark_state.get("scenario_fixture", {}) if isinstance(benchmark_state.get("scenario_fixture"), dict) else {}
    manifest = benchmark_state.get("benchmark_manifest", {}) if isinstance(benchmark_state.get("benchmark_manifest"), dict) else {}
    compare = benchmark_state.get("benchmark_compare", {}) if isinstance(benchmark_state.get("benchmark_compare"), dict) else {}
    replay = benchmark_state.get("replay_report", {}) if isinstance(benchmark_state.get("replay_report"), dict) else {}
    run_id = (
        maybe_text(manifest.get("run_id"))
        or maybe_text(fixture.get("run_id"))
        or maybe_text(replay.get("run_id"))
    )
    fixture_path = str(scenario_fixture_path(run_dir, round_id).resolve()) if round_id else ""
    benchmark_path = str(benchmark_manifest_path(run_dir, round_id).resolve()) if round_id else ""
    compare_path = str(benchmark_compare_path(run_dir, round_id).resolve()) if round_id else ""
    replay_path = str(replay_report_path(run_dir, round_id).resolve()) if round_id else ""
    baseline_manifest_path = maybe_text(fixture.get("baseline_manifest", {}).get("path")) if isinstance(fixture.get("baseline_manifest"), dict) else ""
    compare_command = ""
    replay_command = ""
    if round_id and run_id and baseline_manifest_path:
        compare_command = (
            f"compare-benchmark-manifests --run-dir {run_dir} --run-id {run_id} --round-id {round_id} "
            f"--left-manifest-path {baseline_manifest_path} --right-manifest-path {benchmark_path}"
        )
        replay_command = (
            f"replay-runtime-scenario --run-dir {run_dir} --run-id {run_id} --round-id {round_id} "
            f"--fixture-path {fixture_path}"
        )
    return {
        "scenario_id": maybe_text(fixture.get("scenario_id")),
        "scenario_fingerprint": maybe_text(manifest.get("scenario_fingerprint")) or maybe_text(fixture.get("scenario_fingerprint")),
        "fixture_materialized": bool(fixture),
        "benchmark_materialized": bool(manifest),
        "compare_verdict": maybe_text(compare.get("verdict")),
        "replay_verdict": maybe_text(replay.get("replay_verdict")),
        "fixture_command": f"materialize-scenario-fixture --run-dir {run_dir} --run-id {run_id} --round-id {round_id}" if run_id and round_id else "",
        "benchmark_command": f"materialize-benchmark-manifest --run-dir {run_dir} --run-id {run_id} --round-id {round_id}" if run_id and round_id else "",
        "compare_command": compare_command,
        "replay_command": replay_command,
        "fixture_path": fixture_path,
        "benchmark_manifest_path": benchmark_path,
        "benchmark_compare_path": compare_path,
        "replay_report_path": replay_path,
    }


def operations_state(run_dir: Path, selected_round_id: str) -> dict[str, Any]:
    admission_policy = load_admission_policy(run_dir)
    runtime_health = runtime_health_payload(run_dir, round_id=selected_round_id)
    dead_letters = load_dead_letters(run_dir, round_id=selected_round_id, limit=20)
    runbook_path = operator_runbook_path(run_dir, selected_round_id) if selected_round_id else operator_runbook_path(run_dir)
    run_id = maybe_text(admission_policy.get("run_id"))
    materialize_policy_command = (
        f"materialize-admission-policy --run-dir {run_dir} --run-id {run_id}"
        if run_id
        else ""
    )
    return {
        "admission_policy": admission_policy,
        "runtime_health": runtime_health,
        "dead_letters": dead_letters,
        "operator": {
            "permission_profile": maybe_text(admission_policy.get("permission_profile")) or "standard",
            "alert_status": maybe_text(runtime_health.get("alert_status")) or "green",
            "admission_policy_path": str(admission_policy_path(run_dir).resolve()),
            "runtime_health_path": str(runtime_health_path(run_dir).resolve()),
            "operator_runbook_path": str(runbook_path.resolve()),
            "materialize_admission_policy_command": materialize_policy_command,
            "materialize_runtime_health_command": f"materialize-runtime-health --run-dir {run_dir}{f' --round-id {selected_round_id}' if selected_round_id else ''}",
            "materialize_operator_runbook_command": f"materialize-operator-runbook --run-dir {run_dir}{f' --round-id {selected_round_id}' if selected_round_id else ''}",
            "show_dead_letters_command": f"show-dead-letters --run-dir {run_dir}{f' --round-id {selected_round_id}' if selected_round_id else ''}",
            "open_dead_letter_count": int(runtime_health.get("summary", {}).get("open_dead_letter_count") or 0),
        },
    }


def show_run_state(run_dir: Path, tail: int, round_id: str = "") -> dict[str, Any]:
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    cursor = load_json_if_exists(cursor_path(run_dir)) or {}
    registry = load_json_if_exists(registry_path(run_dir)) or {}
    current_round_id = str(cursor.get("current_round_id") or "")
    selected_round_id = maybe_text(round_id) or current_round_id
    phase2_state: dict[str, Any] = {}
    post_round_state: dict[str, Any] = {}
    benchmark_state: dict[str, Any] = {}
    if selected_round_id:
        phase2_state = {
            "plan": load_json_if_exists(orchestration_plan_path(run_dir, selected_round_id)) or {},
            "promotion_gate": load_json_if_exists(promotion_gate_path(run_dir, selected_round_id)) or {},
            "controller": load_json_if_exists(controller_state_path(run_dir, selected_round_id)) or {},
            "supervisor": load_json_if_exists(supervisor_state_path(run_dir, selected_round_id)) or {},
        }
        phase2_state["operator"] = phase2_operator_view(run_dir, selected_round_id, phase2_state)
        post_round_state = {
            "round_close": load_json_if_exists(round_close_state_path(run_dir, selected_round_id)) or {},
            "history_bootstrap": load_json_if_exists(history_bootstrap_state_path(run_dir, selected_round_id)) or {},
        }
        post_round_state["operator"] = post_round_operator_view(run_dir, selected_round_id, post_round_state)
        benchmark_state = {
            "scenario_fixture": load_json_if_exists(scenario_fixture_path(run_dir, selected_round_id)) or {},
            "benchmark_manifest": load_json_if_exists(benchmark_manifest_path(run_dir, selected_round_id)) or {},
            "benchmark_compare": load_json_if_exists(benchmark_compare_path(run_dir, selected_round_id)) or {},
            "replay_report": load_json_if_exists(replay_report_path(run_dir, selected_round_id)) or {},
        }
        benchmark_state["operator"] = benchmark_operator_view(run_dir, selected_round_id, benchmark_state)
    operations = operations_state(run_dir, selected_round_id)
    return {
        "status": "completed",
        "summary": {
            "run_dir": str(run_dir),
            "current_round_id": current_round_id,
            "selected_round_id": selected_round_id,
            "ledger_events": len(load_ledger_tail(run_dir, 1000000)) if ledger_path(run_dir).exists() else 0,
            "alert_status": maybe_text(operations.get("runtime_health", {}).get("alert_status")) if isinstance(operations.get("runtime_health"), dict) else "",
            "open_dead_letter_count": int(operations.get("runtime_health", {}).get("summary", {}).get("open_dead_letter_count") or 0)
            if isinstance(operations.get("runtime_health"), dict)
            else 0,
        },
        "manifest": manifest,
        "cursor": cursor,
        "registry": registry,
        "operations": operations,
        "phase2": phase2_state,
        "post_round": post_round_state,
        "benchmark": benchmark_state,
        "ledger_tail": load_ledger_tail(run_dir, tail),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal runtime kernel for skill-first investigation runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser("init-run", help="Initialize runtime manifest, cursor, and registry for a run.")
    init_cmd.add_argument("--run-dir", required=True)
    init_cmd.add_argument("--run-id", required=True)
    init_cmd.add_argument("--pretty", action="store_true")

    run_cmd = sub.add_parser("run-skill", help="Execute one skill through the runtime kernel and append a ledger event.")
    run_cmd.add_argument("--run-dir", required=True)
    run_cmd.add_argument("--run-id", required=True)
    run_cmd.add_argument("--round-id", required=True)
    run_cmd.add_argument("--skill-name", required=True)
    run_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    run_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(run_cmd)
    run_cmd.add_argument("skill_args", nargs=argparse.REMAINDER)

    preflight_cmd = sub.add_parser("preflight-skill", help="Resolve one skill contract and report governance issues without executing the skill.")
    preflight_cmd.add_argument("--run-dir", required=True)
    preflight_cmd.add_argument("--run-id", required=True)
    preflight_cmd.add_argument("--round-id", required=True)
    preflight_cmd.add_argument("--skill-name", required=True)
    preflight_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    preflight_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(preflight_cmd)
    preflight_cmd.add_argument("skill_args", nargs=argparse.REMAINDER)

    gate_cmd = sub.add_parser("apply-promotion-gate", help="Evaluate round readiness and write a promote-or-freeze gate artifact.")
    gate_cmd.add_argument("--run-dir", required=True)
    gate_cmd.add_argument("--run-id", required=True)
    gate_cmd.add_argument("--round-id", required=True)
    gate_cmd.add_argument("--pretty", action="store_true")

    phase2_cmd = sub.add_parser("run-phase2-round", help="Run the board -> D1 -> D2 -> promotion phase-2 chain in one command.")
    phase2_cmd.add_argument("--run-dir", required=True)
    phase2_cmd.add_argument("--run-id", required=True)
    phase2_cmd.add_argument("--round-id", required=True)
    phase2_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    phase2_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(phase2_cmd)

    resume_phase2_cmd = sub.add_parser("resume-phase2-round", help="Resume one interrupted phase-2 round from the persisted controller state.")
    resume_phase2_cmd.add_argument("--run-dir", required=True)
    resume_phase2_cmd.add_argument("--run-id", required=True)
    resume_phase2_cmd.add_argument("--round-id", required=True)
    resume_phase2_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    resume_phase2_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(resume_phase2_cmd)

    restart_phase2_cmd = sub.add_parser("restart-phase2-round", help="Force a fresh phase-2 controller run and overwrite any resumable state.")
    restart_phase2_cmd.add_argument("--run-dir", required=True)
    restart_phase2_cmd.add_argument("--run-id", required=True)
    restart_phase2_cmd.add_argument("--round-id", required=True)
    restart_phase2_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    restart_phase2_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(restart_phase2_cmd)

    close_round_cmd = sub.add_parser("close-round", help="Run the standard post-round archive closeout for one terminal round.")
    close_round_cmd.add_argument("--run-dir", required=True)
    close_round_cmd.add_argument("--run-id", required=True)
    close_round_cmd.add_argument("--round-id", required=True)
    close_round_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    close_round_cmd.add_argument("--archive-failure-policy", default="block", choices=ARCHIVE_FAILURE_POLICIES)
    close_round_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(close_round_cmd)

    bootstrap_history_cmd = sub.add_parser("bootstrap-history-context", help="Materialize one runtime-managed history context bundle for the selected round.")
    bootstrap_history_cmd.add_argument("--run-dir", required=True)
    bootstrap_history_cmd.add_argument("--run-id", required=True)
    bootstrap_history_cmd.add_argument("--round-id", required=True)
    bootstrap_history_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    bootstrap_history_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(bootstrap_history_cmd)

    scenario_fixture_cmd = sub.add_parser("materialize-scenario-fixture", help="Freeze one benchmarkable scenario contract for the selected round.")
    scenario_fixture_cmd.add_argument("--run-dir", required=True)
    scenario_fixture_cmd.add_argument("--run-id", required=True)
    scenario_fixture_cmd.add_argument("--round-id", required=True)
    scenario_fixture_cmd.add_argument("--scenario-id", default="")
    scenario_fixture_cmd.add_argument("--baseline-manifest-path", default="")
    scenario_fixture_cmd.add_argument("--pretty", action="store_true")

    benchmark_manifest_cmd = sub.add_parser("materialize-benchmark-manifest", help="Write one stable runtime benchmark manifest for the selected round.")
    benchmark_manifest_cmd.add_argument("--run-dir", required=True)
    benchmark_manifest_cmd.add_argument("--run-id", required=True)
    benchmark_manifest_cmd.add_argument("--round-id", required=True)
    benchmark_manifest_cmd.add_argument("--pretty", action="store_true")

    compare_manifest_cmd = sub.add_parser("compare-benchmark-manifests", help="Compare two benchmark manifests and materialize one drift report.")
    compare_manifest_cmd.add_argument("--run-dir", required=True)
    compare_manifest_cmd.add_argument("--run-id", required=True)
    compare_manifest_cmd.add_argument("--round-id", required=True)
    compare_manifest_cmd.add_argument("--left-manifest-path", required=True)
    compare_manifest_cmd.add_argument("--right-manifest-path", required=True)
    compare_manifest_cmd.add_argument("--pretty", action="store_true")

    replay_cmd = sub.add_parser("replay-runtime-scenario", help="Materialize a candidate benchmark manifest and compare it against one frozen scenario fixture.")
    replay_cmd.add_argument("--run-dir", required=True)
    replay_cmd.add_argument("--run-id", required=True)
    replay_cmd.add_argument("--round-id", required=True)
    replay_cmd.add_argument("--fixture-path", default="")
    replay_cmd.add_argument("--baseline-manifest-path", default="")
    replay_cmd.add_argument("--pretty", action="store_true")

    supervisor_cmd = sub.add_parser("supervise-round", help="Run the phase-2 controller and materialize a compact supervisor state.")
    supervisor_cmd.add_argument("--run-dir", required=True)
    supervisor_cmd.add_argument("--run-id", required=True)
    supervisor_cmd.add_argument("--round-id", required=True)
    supervisor_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    supervisor_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(supervisor_cmd)

    admission_policy_cmd = sub.add_parser("materialize-admission-policy", help="Write one runtime admission policy for permission and sandbox enforcement.")
    admission_policy_cmd.add_argument("--run-dir", required=True)
    admission_policy_cmd.add_argument("--run-id", required=True)
    admission_policy_cmd.add_argument("--pretty", action="store_true")
    add_admission_policy_args(admission_policy_cmd)

    runtime_health_cmd = sub.add_parser("materialize-runtime-health", help="Write one runtime health and alert snapshot.")
    runtime_health_cmd.add_argument("--run-dir", required=True)
    runtime_health_cmd.add_argument("--round-id", default="")
    runtime_health_cmd.add_argument("--pretty", action="store_true")

    operator_runbook_cmd = sub.add_parser("materialize-operator-runbook", help="Write one operator runbook markdown surface for the runtime.")
    operator_runbook_cmd.add_argument("--run-dir", required=True)
    operator_runbook_cmd.add_argument("--round-id", default="")
    operator_runbook_cmd.add_argument("--pretty", action="store_true")

    dead_letters_cmd = sub.add_parser("show-dead-letters", help="Show open runtime dead letters for the selected run or round.")
    dead_letters_cmd.add_argument("--run-dir", required=True)
    dead_letters_cmd.add_argument("--round-id", default="")
    dead_letters_cmd.add_argument("--limit", type=int, default=20)
    dead_letters_cmd.add_argument("--pretty", action="store_true")

    show_cmd = sub.add_parser("show-run-state", help="Show manifest, cursor, registry, and a tail of runtime ledger events.")
    show_cmd.add_argument("--run-dir", required=True)
    show_cmd.add_argument("--round-id", default="")
    show_cmd.add_argument("--tail", type=int, default=10)
    show_cmd.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    run_dir = resolve_run_dir(args.run_dir)

    if args.command == "init-run":
        payload = init_run(run_dir, args.run_id)
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "run-skill":
        init_run(run_dir, args.run_id)
        skill_args = list(args.skill_args or [])
        if skill_args and skill_args[0] == "--":
            skill_args = skill_args[1:]
        try:
            payload = run_skill(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                skill_name=args.skill_name,
                skill_args=skill_args,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"skill_name": args.skill_name, "run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "preflight-skill":
        init_run(run_dir, args.run_id)
        skill_args = list(args.skill_args or [])
        if skill_args and skill_args[0] == "--":
            skill_args = skill_args[1:]
        preflight = preflight_skill_execution(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            skill_name=args.skill_name,
            skill_args=skill_args,
            contract_mode=args.contract_mode,
            timeout_seconds=args.timeout_seconds,
            retry_budget=args.retry_budget,
            retry_backoff_ms=args.retry_backoff_ms,
            allow_side_effects=args.allow_side_effect,
        )
        payload = {
            "status": "blocked" if bool(preflight.get("block_execution")) else "completed",
            "summary": {
                "skill_name": args.skill_name,
                "run_id": args.run_id,
                "round_id": args.round_id,
                "contract_mode": args.contract_mode,
                "issue_count": preflight.get("issue_count", 0),
                "blocking_issue_count": preflight.get("blocking_issue_count", 0),
                "timeout_seconds": preflight.get("execution_policy", {}).get("timeout_seconds"),
                "retry_budget": preflight.get("execution_policy", {}).get("retry_budget"),
            },
            "preflight": preflight,
        }
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, args.skill_name, "preflight-only", args.contract_mode),
                "event_type": "skill-preflight",
                "run_id": args.run_id,
                "round_id": args.round_id,
                "skill_name": args.skill_name,
                "status": payload["status"],
                "contract_mode": args.contract_mode,
                "execution_policy": preflight.get("execution_policy", {}),
                "preflight": preflight,
            },
        )
        print(pretty_json(payload, args.pretty))
        return 0 if payload["status"] != "blocked" else 1

    if args.command == "apply-promotion-gate":
        init_run(run_dir, args.run_id)
        payload = apply_promotion_gate(run_dir, run_id=args.run_id, round_id=args.round_id)
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v2",
                "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, "promotion-gate", payload.get("generated_at_utc")),
                "event_type": "promotion-gate",
                "run_id": args.run_id,
                "round_id": args.round_id,
                "started_at_utc": payload.get("generated_at_utc"),
                "completed_at_utc": payload.get("generated_at_utc"),
                "status": "completed",
                "gate_status": payload.get("gate_status"),
                "readiness_status": payload.get("readiness_status"),
                "promote_allowed": bool(payload.get("promote_allowed")),
                "gate_path": payload.get("output_path"),
            },
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "run-phase2-round":
        try:
            payload = run_phase2_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "resume-phase2-round":
        try:
            payload = run_phase2_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
                force_restart=False,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "restart-phase2-round":
        try:
            payload = run_phase2_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
                force_restart=True,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "close-round":
        try:
            payload = close_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
                archive_failure_policy=args.archive_failure_policy,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "bootstrap-history-context":
        try:
            payload = bootstrap_history_context_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-scenario-fixture":
        try:
            payload = materialize_scenario_fixture(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                scenario_id=args.scenario_id,
                baseline_manifest_override=args.baseline_manifest_path,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-benchmark-manifest":
        try:
            payload = materialize_benchmark_manifest(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "compare-benchmark-manifests":
        try:
            payload = compare_benchmark_manifests(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                left_manifest_path=args.left_manifest_path,
                right_manifest_path=args.right_manifest_path,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "replay-runtime-scenario":
        try:
            payload = replay_runtime_scenario(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                fixture_path_override=args.fixture_path,
                baseline_manifest_override=args.baseline_manifest_path,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "supervise-round":
        try:
            payload = supervise_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-admission-policy":
        payload = materialize_admission_policy(
            run_dir,
            run_id=args.run_id,
            permission_profile=args.permission_profile,
            max_timeout_seconds=args.max_timeout_seconds,
            max_retry_budget=args.max_retry_budget,
            max_retry_backoff_ms=args.max_retry_backoff_ms,
            default_allow_side_effects=args.default_allow_side_effect,
            approval_required_side_effects=args.approval_required_side_effect,
            blocked_side_effects=args.blocked_side_effect,
            allowed_read_roots=args.allowed_read_root,
            allowed_write_roots=args.allowed_write_root,
            allowed_cwd_roots=args.allowed_cwd_root,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-runtime-health":
        payload = materialize_runtime_health(run_dir, round_id=args.round_id)
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-operator-runbook":
        payload = {
            "status": "completed",
            "summary": {
                "run_dir": str(run_dir),
                "round_id": args.round_id,
            },
            "operator_runbook_path": materialize_operator_runbook(run_dir, round_id=args.round_id),
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-dead-letters":
        dead_letters = load_dead_letters(run_dir, round_id=args.round_id, limit=args.limit)
        payload = {
            "status": "completed",
            "summary": {
                "run_dir": str(run_dir),
                "round_id": args.round_id,
                "dead_letter_count": len(dead_letters),
            },
            "dead_letters": dead_letters,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-run-state":
        payload = show_run_state(run_dir, args.tail, args.round_id)
        print(pretty_json(payload, args.pretty))
        return 0

    return 1
