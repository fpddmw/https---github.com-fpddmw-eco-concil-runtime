from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..canonical_contracts import (
    PLANE_ANALYSIS,
    PLANE_DELIBERATION,
    PLANE_REPORTING,
    PLANE_RUNTIME,
    PLANE_SIGNAL,
    canonical_contracts_for_plane,
)
from ..control_objects import (
    control_queryable_object_kinds,
    query_control_objects,
)
from ..council_objects import (
    council_queryable_object_kinds,
    query_council_objects,
)
from ..phase2_exports import materialize_phase2_exports
from ..reporting_objects import (
    query_reporting_objects,
    reporting_queryable_object_kinds,
)
from ..reporting_exports import materialize_reporting_exports
from ..phase2_agent_handoff import EntryChainBuilder, HardGateCommandBuilder
from ..runtime_command_hints import run_skill_command
from .analysis_plane import (
    analysis_kind_names,
    query_analysis_result_items,
    query_analysis_result_sets,
)
from .agent_entry import agent_entry_state, materialize_agent_entry_gate
from .benchmark import (
    compare_benchmark_manifests,
    materialize_benchmark_manifest,
    materialize_scenario_fixture,
    replay_runtime_scenario,
)
from .controller import run_phase2_round_with_contract_mode
from .deliberation_plane import load_phase2_control_state
from .executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill
from .gate import GateHandler
from .governance import CONTRACT_MODES, preflight_skill_execution
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
from .phase2_state_surfaces import (
    build_reporting_surface,
    load_controller_state_wrapper,
    load_council_decision_wrapper,
    load_expert_report_wrapper,
    load_final_publication_wrapper,
    load_promotion_gate_wrapper,
    load_reporting_handoff_wrapper,
    load_supervisor_state_wrapper,
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


def phase2_operator_view(
    run_dir: Path,
    round_id: str,
    phase2_state: dict[str, Any],
    reporting_surface: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = phase2_state.get("plan", {}) if isinstance(phase2_state.get("plan"), dict) else {}
    gate = phase2_state.get("promotion_gate", {}) if isinstance(phase2_state.get("promotion_gate"), dict) else {}
    controller = phase2_state.get("controller", {}) if isinstance(phase2_state.get("controller"), dict) else {}
    supervisor = phase2_state.get("supervisor", {}) if isinstance(phase2_state.get("supervisor"), dict) else {}
    reporting = reporting_surface if isinstance(reporting_surface, dict) else {}
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
    query_public_signals_command = (
        run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-public-signals",
            contract_mode="warn",
        )
        if round_id and run_id
        else ""
    )
    query_formal_signals_command = (
        run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-formal-signals",
            contract_mode="warn",
        )
        if round_id and run_id
        else ""
    )
    query_environment_signals_command = (
        run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-environment-signals",
            contract_mode="warn",
        )
        if round_id and run_id
        else ""
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
        "reporting_ready": bool(reporting.get("reporting_ready")),
        "reporting_blockers": reporting.get("reporting_blockers", [])
        if isinstance(reporting.get("reporting_blockers"), list)
        else [],
        "reporting_handoff_status": maybe_text(reporting.get("handoff_status")),
        "reporting_surface_source": maybe_text(reporting.get("surface_source")),
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
        "show_reporting_state_command": (
            f"show-reporting-state --run-dir {run_dir} --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_controller_state_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind controller-state --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_gate_state_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind gate-state --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_supervisor_state_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind supervisor-state --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_promotion_freeze_command": (
            f"query-control-objects --run-dir {run_dir} --object-kind promotion-freeze --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "materialize_phase2_exports_command": (
            f"materialize-phase2-exports --run-dir {run_dir} --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_public_signals_command": query_public_signals_command,
        "query_formal_signals_command": query_formal_signals_command,
        "query_environment_signals_command": query_environment_signals_command,
        "query_next_actions_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind next-action --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_probes_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind probe --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_readiness_assessments_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind readiness-assessment --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_promotion_basis_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind promotion-basis --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_readiness_blockers_command": (
            f"query-council-objects --run-dir {run_dir} --object-kind next-action --run-id {run_id} --round-id {round_id} --readiness-blocker-only"
            if round_id and run_id
            else ""
        ),
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
            "supervisor_path": (
                maybe_text(supervisor.get("supervisor_path"))
                or str(supervisor_state_path(run_dir, round_id).resolve())
            )
            if round_id
            else "",
        },
        "recommended_next_skills": supervisor.get("recommended_next_skills", []) if isinstance(supervisor.get("recommended_next_skills"), list) else [],
        "round_transition": supervisor.get("round_transition", {}) if isinstance(supervisor.get("round_transition"), dict) else {},
        "operator_notes": supervisor.get("operator_notes", []) if isinstance(supervisor.get("operator_notes"), list) else [],
    }


def reporting_operator_view(
    run_dir: Path,
    round_id: str,
    run_id: str,
    reporting_state: dict[str, Any],
) -> dict[str, Any]:
    surface = (
        reporting_state.get("surface", {})
        if isinstance(reporting_state.get("surface"), dict)
        else {}
    )
    handoff = (
        reporting_state.get("handoff", {})
        if isinstance(reporting_state.get("handoff"), dict)
        else {}
    )
    decision_draft = (
        reporting_state.get("decision_draft", {})
        if isinstance(reporting_state.get("decision_draft"), dict)
        else {}
    )
    decision = (
        reporting_state.get("decision", {})
        if isinstance(reporting_state.get("decision"), dict)
        else {}
    )
    final_publication = (
        reporting_state.get("final_publication", {})
        if isinstance(reporting_state.get("final_publication"), dict)
        else {}
    )
    return {
        "round_id": round_id,
        "reporting_ready": bool(surface.get("reporting_ready")),
        "reporting_blockers": surface.get("reporting_blockers", [])
        if isinstance(surface.get("reporting_blockers"), list)
        else [],
        "handoff_status": maybe_text(surface.get("handoff_status")),
        "surface_source": maybe_text(surface.get("surface_source")),
        "publication_status": maybe_text(surface.get("publication_status")),
        "publication_posture": maybe_text(surface.get("publication_posture")),
        "handoff_present": bool(handoff),
        "decision_draft_present": bool(decision_draft),
        "decision_present": bool(decision),
        "final_publication_present": bool(final_publication),
        "query_reporting_handoff_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind reporting-handoff --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "query_council_decision_drafts_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind council-decision --run-id {run_id} --round-id {round_id} --stage draft"
            if round_id and run_id
            else ""
        ),
        "query_council_decisions_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind council-decision --run-id {run_id} --round-id {round_id} --stage canonical"
            if round_id and run_id
            else ""
        ),
        "query_expert_report_drafts_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind expert-report --run-id {run_id} --round-id {round_id} --stage draft"
            if round_id and run_id
            else ""
        ),
        "query_expert_reports_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind expert-report --run-id {run_id} --round-id {round_id} --stage canonical"
            if round_id and run_id
            else ""
        ),
        "query_final_publications_command": (
            f"query-reporting-objects --run-dir {run_dir} --object-kind final-publication --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "materialize_reporting_exports_command": (
            f"materialize-reporting-exports --run-dir {run_dir} --run-id {run_id} --round-id {round_id}"
            if round_id and run_id
            else ""
        ),
        "materialize_reporting_handoff_command": (
            f"run-skill --run-dir {run_dir} --run-id {run_id} --round-id {round_id} --skill-name eco-materialize-reporting-handoff"
            if round_id and run_id
            else ""
        ),
        "draft_council_decision_command": (
            f"run-skill --run-dir {run_dir} --run-id {run_id} --round-id {round_id} --skill-name eco-draft-council-decision"
            if round_id and run_id
            else ""
        ),
        "draft_sociologist_report_command": (
            f"run-skill --run-dir {run_dir} --run-id {run_id} --round-id {round_id} --skill-name eco-draft-expert-report -- --role sociologist"
            if round_id and run_id
            else ""
        ),
        "draft_environmentalist_report_command": (
            f"run-skill --run-dir {run_dir} --run-id {run_id} --round-id {round_id} --skill-name eco-draft-expert-report -- --role environmentalist"
            if round_id and run_id
            else ""
        ),
        "publish_council_decision_command": (
            f"run-skill --run-dir {run_dir} --run-id {run_id} --round-id {round_id} --skill-name eco-publish-council-decision"
            if round_id and run_id
            else ""
        ),
        "publish_sociologist_report_command": (
            f"run-skill --run-dir {run_dir} --run-id {run_id} --round-id {round_id} --skill-name eco-publish-expert-report -- --role sociologist"
            if round_id and run_id
            else ""
        ),
        "publish_environmentalist_report_command": (
            f"run-skill --run-dir {run_dir} --run-id {run_id} --round-id {round_id} --skill-name eco-publish-expert-report -- --role environmentalist"
            if round_id and run_id
            else ""
        ),
        "show_run_state_command": (
            f"show-run-state --run-dir {run_dir} --round-id {round_id} --tail 20"
            if round_id
            else ""
        ),
        "inspection_paths": {
            "handoff_path": maybe_text(handoff.get("output_path")),
            "decision_draft_path": maybe_text(decision_draft.get("output_path")),
            "decision_path": maybe_text(decision.get("output_path")),
            "final_publication_path": maybe_text(final_publication.get("output_path")),
        },
    }


def reporting_state_for_round(run_dir: Path, run_id: str, round_id: str) -> dict[str, Any]:
    supervisor_context = load_supervisor_state_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
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
    expert_report_drafts = {
        role: (
            load_expert_report_wrapper(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                agent_role=role,
                report_stage="draft",
            ).get("payload", {})
        )
        for role in ("sociologist", "environmentalist")
    }
    expert_reports = {
        role: (
            load_expert_report_wrapper(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                agent_role=role,
                report_stage="canonical",
            ).get("payload", {})
        )
        for role in ("sociologist", "environmentalist")
    }
    final_publication_context = load_final_publication_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    surface = build_reporting_surface(
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
        expert_report_payloads={
            role: payload
            for role, payload in expert_reports.items()
            if isinstance(payload, dict)
        },
        final_publication_payload=final_publication_context.get("payload")
        if isinstance(final_publication_context.get("payload"), dict)
        else {},
    )
    reporting_state = {
        "supervisor": supervisor,
        "handoff": handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else {},
        "decision_draft": decision_draft_context.get("payload")
        if isinstance(decision_draft_context.get("payload"), dict)
        else {},
        "decision": decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else {},
        "expert_report_drafts": expert_report_drafts,
        "expert_reports": expert_reports,
        "final_publication": final_publication_context.get("payload")
        if isinstance(final_publication_context.get("payload"), dict)
        else {},
        "surface": surface,
    }
    reporting_state["operator"] = reporting_operator_view(
        run_dir,
        round_id,
        run_id,
        reporting_state,
    )
    return reporting_state


def post_round_operator_view(run_dir: Path, round_id: str, post_round_state: dict[str, Any]) -> dict[str, Any]:
    round_close = post_round_state.get("round_close", {}) if isinstance(post_round_state.get("round_close"), dict) else {}
    history_bootstrap = post_round_state.get("history_bootstrap", {}) if isinstance(post_round_state.get("history_bootstrap"), dict) else {}
    run_id = maybe_text(round_close.get("run_id")) or maybe_text(history_bootstrap.get("run_id"))
    return {
        "round_close_status": maybe_text(round_close.get("close_status")),
        "archive_status": maybe_text(round_close.get("archive_status")),
        "close_posture": maybe_text(round_close.get("close_posture")),
        "reporting_ready": bool(round_close.get("reporting_ready")),
        "reporting_blockers": round_close.get("reporting_blockers", [])
        if isinstance(round_close.get("reporting_blockers"), list)
        else [],
        "reporting_handoff_status": maybe_text(
            round_close.get("reporting_handoff_status")
        ),
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
        "reporting_ready": bool(manifest.get("phase2_summary", {}).get("reporting_ready"))
        if isinstance(manifest.get("phase2_summary"), dict)
        else False,
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


def show_run_state(
    run_dir: Path,
    tail: int,
    round_id: str = "",
    *,
    agent_entry_profile: dict[str, Any] | None = None,
    hard_gate_command_builder: HardGateCommandBuilder | None = None,
) -> dict[str, Any]:
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    cursor = load_json_if_exists(cursor_path(run_dir)) or {}
    registry = load_json_if_exists(registry_path(run_dir)) or {}
    current_round_id = str(cursor.get("current_round_id") or "")
    selected_round_id = maybe_text(round_id) or current_round_id
    resolved_run_id = maybe_text(manifest.get("run_id")) or maybe_text(cursor.get("run_id"))
    phase2_state: dict[str, Any] = {}
    reporting_state: dict[str, Any] = {}
    post_round_state: dict[str, Any] = {}
    benchmark_state: dict[str, Any] = {}
    if selected_round_id:
        control_state = load_phase2_control_state(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
        )
        controller_context = load_controller_state_wrapper(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            controller_state_path=str(
                controller_state_path(run_dir, selected_round_id).resolve()
            ),
        )
        gate_context = load_promotion_gate_wrapper(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            promotion_gate_path=str(
                promotion_gate_path(run_dir, selected_round_id).resolve()
            ),
        )
        supervisor_context = load_supervisor_state_wrapper(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            supervisor_state_path=str(
                supervisor_state_path(run_dir, selected_round_id).resolve()
            ),
        )
        phase2_state = {
            "plan": load_json_if_exists(orchestration_plan_path(run_dir, selected_round_id)) or {},
            "promotion_gate": gate_context.get("payload", {})
            if isinstance(gate_context.get("payload"), dict)
            else {},
            "controller": controller_context.get("payload", {})
            if isinstance(controller_context.get("payload"), dict)
            else {},
            "supervisor": supervisor_context.get("payload", {})
            if isinstance(supervisor_context.get("payload"), dict)
            else {},
            "promotion_freeze": control_state.get("promotion_freeze", {})
            if isinstance(control_state.get("promotion_freeze"), dict)
            else {},
            "control_contexts": {
                "controller": controller_context,
                "promotion_gate": gate_context,
                "supervisor": supervisor_context,
            },
        }
        reporting_state = reporting_state_for_round(
            run_dir,
            resolved_run_id,
            selected_round_id,
        )
        phase2_state["operator"] = phase2_operator_view(
            run_dir,
            selected_round_id,
            phase2_state,
            reporting_state.get("surface", {})
            if isinstance(reporting_state.get("surface"), dict)
            else {},
        )
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
        "agent_entry": agent_entry_state(
            run_dir,
            run_id=resolved_run_id,
            round_id=selected_round_id,
            agent_entry_profile=agent_entry_profile,
            hard_gate_command_builder=hard_gate_command_builder,
        ),
        "phase2": phase2_state,
        "reporting": reporting_state,
        "post_round": post_round_state,
        "benchmark": benchmark_state,
        "ledger_tail": load_ledger_tail(run_dir, tail),
    }


def add_analysis_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(analysis_kind_names())
    command.add_argument("--run-dir", required=True)
    command.add_argument("--result-set-id", default="")
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument(
        "--analysis-kind",
        default="",
        help=f"Optional analysis kind filter. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--source-skill", default="")
    command.add_argument("--artifact-path", default="")
    command.add_argument("--latest-only", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


def add_council_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(council_queryable_object_kinds())
    command.add_argument("--run-dir", required=True)
    command.add_argument(
        "--object-kind",
        required=True,
        help=f"Canonical deliberation object kind. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument("--agent-role", default="")
    command.add_argument("--status", default="")
    command.add_argument("--decision-id", default="")
    command.add_argument("--target-kind", default="")
    command.add_argument("--target-id", default="")
    command.add_argument("--issue-label", default="")
    command.add_argument("--route-id", default="")
    command.add_argument("--assessment-id", default="")
    command.add_argument("--linkage-id", default="")
    command.add_argument("--gap-id", default="")
    command.add_argument("--source-proposal-id", default="")
    command.add_argument("--readiness-blocker-only", action="store_true")
    command.add_argument("--include-contract", action="store_true")
    command.add_argument("--include-items", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


def add_reporting_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(reporting_queryable_object_kinds())
    command.add_argument("--run-dir", required=True)
    command.add_argument(
        "--object-kind",
        required=True,
        help=f"Canonical reporting object kind. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument("--agent-role", default="")
    command.add_argument("--status", default="")
    command.add_argument("--decision-id", default="")
    command.add_argument("--stage", default="")
    command.add_argument("--include-contract", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


def add_control_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(control_queryable_object_kinds())
    command.add_argument("--run-dir", required=True)
    command.add_argument(
        "--object-kind",
        required=True,
        help=f"Canonical control object kind. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument("--status", default="")
    command.add_argument("--controller-status", default="")
    command.add_argument("--gate-status", default="")
    command.add_argument("--promotion-status", default="")
    command.add_argument("--supervisor-status", default="")
    command.add_argument("--planning-mode", default="")
    command.add_argument("--readiness-status", default="")
    command.add_argument("--current-stage", default="")
    command.add_argument("--failed-stage", default="")
    command.add_argument("--resume-status", default="")
    command.add_argument("--stage-name", default="")
    command.add_argument("--gate-handler", default="")
    command.add_argument("--decision-source", default="")
    command.add_argument("--supervisor-substatus", default="")
    command.add_argument("--phase2-posture", default="")
    command.add_argument("--terminal-state", default="")
    command.add_argument("--reporting-handoff-status", default="")
    command.add_argument("--reporting-ready-only", action="store_true")
    command.add_argument("--include-contract", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


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

    agent_entry_cmd = sub.add_parser("materialize-agent-entry-gate", help="Write one operator-visible agent entry gate contract for the selected round.")
    agent_entry_cmd.add_argument("--run-dir", required=True)
    agent_entry_cmd.add_argument("--run-id", required=True)
    agent_entry_cmd.add_argument("--round-id", required=True)
    agent_entry_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    agent_entry_cmd.add_argument("--refresh-advisory-plan", action="store_true")
    agent_entry_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(agent_entry_cmd)

    dead_letters_cmd = sub.add_parser("show-dead-letters", help="Show open runtime dead letters for the selected run or round.")
    dead_letters_cmd.add_argument("--run-dir", required=True)
    dead_letters_cmd.add_argument("--round-id", default="")
    dead_letters_cmd.add_argument("--limit", type=int, default=20)
    dead_letters_cmd.add_argument("--pretty", action="store_true")

    list_analysis_cmd = sub.add_parser(
        "list-analysis-result-sets",
        help="List analysis-plane result sets from the shared SQLite query surface.",
    )
    add_analysis_query_args(list_analysis_cmd)
    list_analysis_cmd.add_argument("--include-contract", action="store_true")
    list_analysis_cmd.add_argument("--include-items", action="store_true")

    query_items_cmd = sub.add_parser(
        "query-analysis-result-items",
        help="Query analysis-plane result items from the shared SQLite query surface.",
    )
    add_analysis_query_args(query_items_cmd)
    query_items_cmd.add_argument("--subject-id", default="")
    query_items_cmd.add_argument("--readiness", default="")
    query_items_cmd.add_argument("--include-result-sets", action="store_true")
    query_items_cmd.add_argument("--include-contract", action="store_true")

    council_query_cmd = sub.add_parser(
        "query-council-objects",
        help="Query canonical deliberation objects from the shared SQLite query surface.",
    )
    add_council_query_args(council_query_cmd)

    reporting_query_cmd = sub.add_parser(
        "query-reporting-objects",
        help="Query canonical reporting-plane objects from the shared SQLite query surface.",
    )
    add_reporting_query_args(reporting_query_cmd)

    control_query_cmd = sub.add_parser(
        "query-control-objects",
        help="Query runtime control objects from the shared SQLite query surface.",
    )
    add_control_query_args(control_query_cmd)

    phase2_export_cmd = sub.add_parser(
        "materialize-phase2-exports",
        help="Rebuild phase-2 investigation/promotion/runtime exports from canonical DB state.",
    )
    phase2_export_cmd.add_argument("--run-dir", required=True)
    phase2_export_cmd.add_argument("--run-id", required=True)
    phase2_export_cmd.add_argument("--round-id", required=True)
    phase2_export_cmd.add_argument("--pretty", action="store_true")

    reporting_export_cmd = sub.add_parser(
        "materialize-reporting-exports",
        help="Rebuild reporting/*.json exports from canonical reporting-plane DB records.",
    )
    reporting_export_cmd.add_argument("--run-dir", required=True)
    reporting_export_cmd.add_argument("--run-id", required=True)
    reporting_export_cmd.add_argument("--round-id", required=True)
    reporting_export_cmd.add_argument("--pretty", action="store_true")

    contract_list_cmd = sub.add_parser(
        "list-canonical-contracts",
        help="List target canonical contracts for the selected plane or all planes.",
    )
    contract_list_cmd.add_argument(
        "--plane",
        default="",
        choices=[
            "",
            PLANE_SIGNAL,
            PLANE_ANALYSIS,
            PLANE_DELIBERATION,
            PLANE_REPORTING,
            PLANE_RUNTIME,
        ],
    )
    contract_list_cmd.add_argument("--pretty", action="store_true")

    show_cmd = sub.add_parser("show-run-state", help="Show manifest, cursor, registry, and a tail of runtime ledger events.")
    show_cmd.add_argument("--run-dir", required=True)
    show_cmd.add_argument("--round-id", default="")
    show_cmd.add_argument("--tail", type=int, default=10)
    show_cmd.add_argument("--pretty", action="store_true")

    reporting_cmd = sub.add_parser(
        "show-reporting-state",
        help="Show the DB-first reporting surface for one round, including handoff, decision, and publication gate state.",
    )
    reporting_cmd.add_argument("--run-dir", required=True)
    reporting_cmd.add_argument("--run-id", default="")
    reporting_cmd.add_argument("--round-id", required=True)
    reporting_cmd.add_argument("--pretty", action="store_true")
    return parser


def main(
    argv: list[str] | None = None,
    *,
    default_gate_handlers: dict[str, GateHandler] | None = None,
    default_agent_entry_profile: dict[str, Any] | None = None,
    default_posture_profile: dict[str, Any] | None = None,
    hard_gate_command_builder: HardGateCommandBuilder | None = None,
    entry_chain_builder: EntryChainBuilder | None = None,
    default_planning_sources: list[dict[str, Any]] | None = None,
    default_stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    gate_handlers = default_gate_handlers if isinstance(default_gate_handlers, dict) else None
    agent_entry_profile = (
        default_agent_entry_profile
        if isinstance(default_agent_entry_profile, dict)
        else None
    )
    posture_profile = (
        default_posture_profile
        if isinstance(default_posture_profile, dict)
        else None
    )
    planning_sources = (
        default_planning_sources
        if isinstance(default_planning_sources, list)
        else None
    )
    stage_definitions = (
        default_stage_definitions
        if isinstance(default_stage_definitions, dict)
        else None
    )

    if args.command == "list-canonical-contracts":
        contracts = canonical_contracts_for_plane(plane=args.plane)
        payload = {
            "schema_version": "canonical-contract-list-v1",
            "status": "completed",
            "plane": args.plane or "all",
            "contracts": contracts,
            "summary": {
                "plane": args.plane or "all",
                "contract_count": len(contracts),
            },
        }
        print(pretty_json(payload, args.pretty))
        return 0

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
        if not gate_handlers or "promotion-gate" not in gate_handlers:
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id},
                "message": "No default phase-2 gate handler registry was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        payload = gate_handlers["promotion-gate"](
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
        )
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
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No phase-2 posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = run_phase2_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
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
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No phase-2 posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = run_phase2_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
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
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No phase-2 posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = run_phase2_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
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
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No phase-2 posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = supervise_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
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

    if args.command == "materialize-agent-entry-gate":
        init_run(run_dir, args.run_id)
        if (
            not isinstance(agent_entry_profile, dict)
            or not callable(hard_gate_command_builder)
            or not callable(entry_chain_builder)
        ):
            failure = {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "contract_mode": args.contract_mode,
                },
                "message": "No agent entry profile or agent handoff profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = materialize_agent_entry_gate(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                agent_entry_profile=agent_entry_profile,
                hard_gate_command_builder=hard_gate_command_builder,
                entry_chain_builder=entry_chain_builder,
                contract_mode=args.contract_mode,
                refresh_advisory_plan=args.refresh_advisory_plan,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "contract_mode": args.contract_mode,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
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

    if args.command == "list-analysis-result-sets":
        try:
            payload = query_analysis_result_sets(
                run_dir,
                result_set_id=args.result_set_id,
                run_id=args.run_id,
                round_id=args.round_id,
                analysis_kind=args.analysis_kind,
                source_skill=args.source_skill,
                artifact_path=args.artifact_path,
                latest_only=args.latest_only,
                include_contract=args.include_contract,
                include_items=args.include_items,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "analysis_kind": args.analysis_kind,
                    "result_set_id": args.result_set_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-analysis-result-items":
        try:
            payload = query_analysis_result_items(
                run_dir,
                result_set_id=args.result_set_id,
                run_id=args.run_id,
                round_id=args.round_id,
                analysis_kind=args.analysis_kind,
                source_skill=args.source_skill,
                artifact_path=args.artifact_path,
                subject_id=args.subject_id,
                readiness=args.readiness,
                latest_only=args.latest_only,
                include_result_sets=args.include_result_sets,
                include_contract=args.include_contract,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "analysis_kind": args.analysis_kind,
                    "result_set_id": args.result_set_id,
                    "subject_id": args.subject_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-council-objects":
        try:
            payload = query_council_objects(
                run_dir,
                object_kind=args.object_kind,
                run_id=args.run_id,
                round_id=args.round_id,
                agent_role=args.agent_role,
                status=args.status,
                decision_id=args.decision_id,
                target_kind=args.target_kind,
                target_id=args.target_id,
                issue_label=args.issue_label,
                route_id=args.route_id,
                assessment_id=args.assessment_id,
                linkage_id=args.linkage_id,
                gap_id=args.gap_id,
                source_proposal_id=args.source_proposal_id,
                readiness_blocker_only=args.readiness_blocker_only,
                include_contract=args.include_contract,
                include_items=args.include_items,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "object_kind": args.object_kind,
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-reporting-objects":
        try:
            payload = query_reporting_objects(
                run_dir,
                object_kind=args.object_kind,
                run_id=args.run_id,
                round_id=args.round_id,
                agent_role=args.agent_role,
                status=args.status,
                decision_id=args.decision_id,
                stage=args.stage,
                include_contract=args.include_contract,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "object_kind": args.object_kind,
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-control-objects":
        try:
            payload = query_control_objects(
                run_dir,
                object_kind=args.object_kind,
                run_id=args.run_id,
                round_id=args.round_id,
                status=args.status,
                controller_status=args.controller_status,
                gate_status=args.gate_status,
                promotion_status=args.promotion_status,
                supervisor_status=args.supervisor_status,
                planning_mode=args.planning_mode,
                readiness_status=args.readiness_status,
                current_stage=args.current_stage,
                failed_stage=args.failed_stage,
                resume_status=args.resume_status,
                stage_name=args.stage_name,
                gate_handler=args.gate_handler,
                decision_source=args.decision_source,
                supervisor_substatus=args.supervisor_substatus,
                phase2_posture=args.phase2_posture,
                terminal_state=args.terminal_state,
                reporting_handoff_status=args.reporting_handoff_status,
                reporting_ready_only=args.reporting_ready_only,
                include_contract=args.include_contract,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "object_kind": args.object_kind,
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-phase2-exports":
        payload = materialize_phase2_exports(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-reporting-exports":
        payload = materialize_reporting_exports(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-reporting-state":
        resolved_run_id = maybe_text(args.run_id)
        if not resolved_run_id:
            manifest = load_json_if_exists(manifest_path(run_dir)) or {}
            cursor = load_json_if_exists(cursor_path(run_dir)) or {}
            resolved_run_id = maybe_text(manifest.get("run_id")) or maybe_text(cursor.get("run_id"))
        payload = reporting_state_for_round(run_dir, resolved_run_id, args.round_id)
        output = {
            "status": "completed",
            "summary": {
                "run_dir": str(run_dir),
                "run_id": resolved_run_id,
                "round_id": args.round_id,
                "reporting_ready": bool(payload.get("surface", {}).get("reporting_ready"))
                if isinstance(payload.get("surface"), dict)
                else False,
                "reporting_blocker_count": len(payload.get("surface", {}).get("reporting_blockers", []))
                if isinstance(payload.get("surface", {}).get("reporting_blockers"), list)
                else 0,
                "surface_source": maybe_text(payload.get("surface", {}).get("surface_source"))
                if isinstance(payload.get("surface"), dict)
                else "",
                "publication_status": maybe_text(payload.get("surface", {}).get("publication_status"))
                if isinstance(payload.get("surface"), dict)
                else "",
            },
            **payload,
        }
        print(pretty_json(output, args.pretty))
        return 0

    if args.command == "show-run-state":
        if not isinstance(agent_entry_profile, dict):
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "round_id": args.round_id,
                },
                "message": "No agent entry profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        payload = show_run_state(
            run_dir,
            args.tail,
            args.round_id,
            agent_entry_profile=agent_entry_profile,
            hard_gate_command_builder=hard_gate_command_builder,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    return 1
