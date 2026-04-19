from __future__ import annotations

from pathlib import Path
from typing import Any

from ..phase2_agent_entry_profile import materialize_agent_entry_advisory_plan
from ..phase2_agent_handoff import EntryChainBuilder, HardGateCommandBuilder
from .analysis_plane import query_analysis_result_sets
from .deliberation_plane import load_round_snapshot
from .executor import (
    maybe_text,
    new_runtime_event_id,
    utc_now_iso,
)
from .ledger import append_ledger_event
from .manifest import load_json_if_exists, write_json
from .operations import load_admission_policy, runtime_health_payload
from .paths import (
    agent_advisory_plan_path,
    agent_entry_gate_path,
    mission_scaffold_path,
    resolve_run_dir,
)


def board_counts(round_state: dict[str, Any]) -> dict[str, int]:
    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
    challenges = round_state.get("challenge_tickets", []) if isinstance(round_state.get("challenge_tickets"), list) else []
    tasks = round_state.get("tasks", []) if isinstance(round_state.get("tasks"), list) else []
    return {
        "note_count": len(notes),
        "hypothesis_count": len(hypotheses),
        "active_hypothesis_count": len(
            [item for item in hypotheses if isinstance(item, dict) and maybe_text(item.get("status")) not in {"closed", "rejected"}]
        ),
        "challenge_ticket_count": len(challenges),
        "open_challenge_count": len(
            [item for item in challenges if isinstance(item, dict) and maybe_text(item.get("status")) != "closed"]
        ),
        "task_count": len(tasks),
        "open_task_count": len(
            [
                item
                for item in tasks
                if isinstance(item, dict)
                and maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}
            ]
        ),
    }


def round_surface(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    snapshot = load_round_snapshot(
        run_dir,
        expected_run_id=run_id,
        round_id=round_id,
        include_closed=True,
    )
    round_state = snapshot.get("round_state", {}) if isinstance(snapshot.get("round_state"), dict) else {}
    counts = board_counts(round_state) if maybe_text(snapshot.get("status")) == "completed" else {
        "note_count": 0,
        "hypothesis_count": 0,
        "active_hypothesis_count": 0,
        "challenge_ticket_count": 0,
        "open_challenge_count": 0,
        "task_count": 0,
        "open_task_count": 0,
    }
    return {
        "status": maybe_text(snapshot.get("status")) or "missing-board",
        "state_source": maybe_text(snapshot.get("state_source")) or "missing-board",
        "board_path": maybe_text(snapshot.get("board_path")),
        "db_path": maybe_text(snapshot.get("db_path")),
        "counts": counts,
        "deliberation_sync": snapshot.get("deliberation_sync", {}) if isinstance(snapshot.get("deliberation_sync"), dict) else {},
    }


def analysis_surface(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    try:
        payload = query_analysis_result_sets(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            latest_only=True,
            limit=200,
            offset=0,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "db_path": "",
            "matching_result_set_count": 0,
            "analysis_kind_count": 0,
            "available_analysis_kinds": [],
            "warnings": [
                {
                    "code": "analysis-query-failed",
                    "message": str(exc),
                }
            ],
        }
    rows = payload.get("result_sets", []) if isinstance(payload.get("result_sets"), list) else []
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        analysis_kind = maybe_text(row.get("analysis_kind"))
        if not analysis_kind:
            continue
        summary = grouped.setdefault(
            analysis_kind,
            {
                "analysis_kind": analysis_kind,
                "result_set_count": 0,
                "item_count": 0,
                "artifact_missing_count": 0,
                "latest_generated_at_utc": "",
            },
        )
        summary["result_set_count"] += 1
        summary["item_count"] += int(row.get("item_count") or 0)
        if not bool(row.get("artifact_present")):
            summary["artifact_missing_count"] += 1
        generated_at = maybe_text(row.get("generated_at_utc"))
        if generated_at and generated_at > maybe_text(summary.get("latest_generated_at_utc")):
            summary["latest_generated_at_utc"] = generated_at
    return {
        "status": "completed",
        "db_path": maybe_text(payload.get("summary", {}).get("db_path"))
        if isinstance(payload.get("summary"), dict)
        else "",
        "matching_result_set_count": int(
            payload.get("summary", {}).get("matching_result_set_count") or 0
        )
        if isinstance(payload.get("summary"), dict)
        else 0,
        "analysis_kind_count": len(grouped),
        "available_analysis_kinds": sorted(
            grouped.values(),
            key=lambda item: (
                -int(item.get("result_set_count") or 0),
                maybe_text(item.get("analysis_kind")),
            ),
        ),
        "warnings": payload.get("warnings", []) if isinstance(payload.get("warnings"), list) else [],
    }


def governance_surface(run_dir: Path, *, round_id: str) -> dict[str, Any]:
    policy = load_admission_policy(run_dir)
    health = runtime_health_payload(run_dir, round_id=round_id)
    return {
        "permission_profile": maybe_text(policy.get("permission_profile")) or "standard",
        "approval_authority": maybe_text(policy.get("approval_authority")) or "runtime-operator",
        "rollback_mode": maybe_text(policy.get("rollback_policy", {}).get("mode"))
        if isinstance(policy.get("rollback_policy"), dict)
        else "operator-mediated",
        "alert_status": maybe_text(health.get("alert_status")) or "green",
        "open_dead_letter_count": int(health.get("summary", {}).get("open_dead_letter_count") or 0)
        if isinstance(health.get("summary"), dict)
        else 0,
        "admission_policy_path": maybe_text(policy.get("policy_path")) or "",
        "runtime_health_path": maybe_text(health.get("output_path")) or "",
    }


def mission_surface(run_dir: Path, round_id: str) -> dict[str, Any]:
    payload = load_json_if_exists(mission_scaffold_path(run_dir, round_id)) or {}
    return {
        "present": bool(payload),
        "path": str(mission_scaffold_path(run_dir, round_id).resolve()),
        "orchestration_mode": maybe_text(payload.get("orchestration_mode")),
        "scaffold_id": maybe_text(payload.get("scaffold_id")),
        "task_count": int(payload.get("task_count") or 0),
        "import_source_count": int(payload.get("import_source_count") or 0),
        "request_source_count": int(payload.get("request_source_count") or 0),
    }


def advisory_plan_surface(run_dir: Path, round_id: str) -> dict[str, Any]:
    payload = load_json_if_exists(agent_advisory_plan_path(run_dir, round_id)) or {}
    return {
        "present": bool(payload),
        "path": str(agent_advisory_plan_path(run_dir, round_id).resolve()),
        "planning_mode": maybe_text(payload.get("planning_mode")),
        "controller_authority": maybe_text(payload.get("controller_authority")),
        "plan_source": maybe_text(payload.get("plan_source")),
        "downstream_posture": maybe_text(payload.get("downstream_posture")),
        "direct_council_queue": bool(payload.get("observed_state", {}).get("direct_council_queue"))
        if isinstance(payload.get("observed_state"), dict)
        else False,
        "recommended_skill_sequence": payload.get("agent_turn_hints", {}).get("recommended_skill_sequence", [])
        if isinstance(payload.get("agent_turn_hints"), dict)
        and isinstance(payload.get("agent_turn_hints", {}).get("recommended_skill_sequence"), list)
        else [],
        "primary_role": maybe_text(payload.get("agent_turn_hints", {}).get("primary_role"))
        if isinstance(payload.get("agent_turn_hints"), dict)
        else "",
        "support_roles": payload.get("agent_turn_hints", {}).get("support_roles", [])
        if isinstance(payload.get("agent_turn_hints"), dict)
        and isinstance(payload.get("agent_turn_hints", {}).get("support_roles"), list)
        else [],
    }


def resolved_agent_entry_profile(agent_entry_profile: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(agent_entry_profile, dict):
        raise ValueError("No agent entry profile was injected into kernel.agent_entry.")
    return agent_entry_profile


def profile_callable(agent_entry_profile: dict[str, Any], key: str) -> Any:
    candidate = agent_entry_profile.get(key)
    if not callable(candidate):
        raise ValueError(f"Agent entry profile is missing callable: {key}")
    return candidate


def profile_list(agent_entry_profile: dict[str, Any], key: str) -> list[Any]:
    candidate = agent_entry_profile.get(key)
    if not isinstance(candidate, list):
        raise ValueError(f"Agent entry profile is missing list: {key}")
    return candidate


def build_agent_entry_payload(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    agent_entry_profile: dict[str, Any],
    hard_gate_command_builder: HardGateCommandBuilder,
    entry_chain_builder: EntryChainBuilder,
) -> dict[str, Any]:
    profile = resolved_agent_entry_profile(agent_entry_profile)
    status_evaluator = profile_callable(profile, "status_evaluator")
    next_round_id_builder = profile_callable(profile, "next_round_id_builder")
    role_entry_builder = profile_callable(profile, "role_entry_builder")
    recommended_skills_builder = profile_callable(profile, "recommended_skills_builder")
    operator_notes_builder = profile_callable(profile, "operator_notes_builder")
    role_definitions = profile_list(profile, "role_definitions")
    governance = governance_surface(run_dir, round_id=round_id)
    mission = mission_surface(run_dir, round_id)
    advisory_plan = advisory_plan_surface(run_dir, round_id)
    round_state = round_surface(run_dir, run_id=run_id, round_id=round_id)
    analysis = analysis_surface(run_dir, run_id=run_id, round_id=round_id)
    next_round_id = maybe_text(
        next_round_id_builder(
            run_dir=run_dir,
            current_round_id=round_id,
        )
    )
    status, warnings = status_evaluator(
        governance=governance,
        mission=mission,
        round_surface_payload=round_state,
        analysis=analysis,
    )
    role_entries = role_entry_builder(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
        next_round_id=next_round_id,
        role_definitions=role_definitions,
    )
    recommended_skills = recommended_skills_builder(advisory_plan=advisory_plan)
    payload = {
        "schema_version": "runtime-agent-entry-gate-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "entry_id": "agent-entry-" + new_runtime_event_id("gate", run_id, round_id, status).split("-", 1)[1],
        "entry_status": status,
        "orchestration_mode": maybe_text(mission.get("orchestration_mode")) or "openclaw-agent-compatible",
        "contract_mode": contract_mode,
        "output_path": str(agent_entry_gate_path(run_dir, round_id).resolve()),
        "mission": mission,
        "governance": governance,
        "round_surface": round_state,
        "analysis_surface": analysis,
        "advisory_plan": advisory_plan,
        "recommended_entry_skills": recommended_skills,
        "role_entry_points": role_entries,
        "entry_chain": entry_chain_builder(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            next_round_id=next_round_id,
        ),
        "hard_gate_commands": hard_gate_command_builder(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            contract_mode=contract_mode,
        ),
        "operator_notes": operator_notes_builder(
            status=status,
            mission=mission,
            advisory_plan=advisory_plan,
            round_surface_payload=round_state,
            analysis=analysis,
        ),
        "warnings": warnings
        + (
            analysis.get("warnings", [])
            if isinstance(analysis.get("warnings"), list)
            else []
        ),
    }
    return payload


def agent_entry_operator_view(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    gate_payload: dict[str, Any] | None,
    contract_mode: str = "warn",
    agent_entry_profile: dict[str, Any] | None = None,
    hard_gate_command_builder: HardGateCommandBuilder | None = None,
) -> dict[str, Any]:
    profile = resolved_agent_entry_profile(agent_entry_profile)
    operator_commands_builder = profile_callable(profile, "operator_commands_builder")
    next_round_id_builder = profile_callable(profile, "next_round_id_builder")
    gate = gate_payload if isinstance(gate_payload, dict) else {}
    next_round_id = (
        maybe_text(
            next_round_id_builder(
                run_dir=run_dir,
                current_round_id=round_id,
            )
        )
        if round_id
        else ""
    )
    handoff_commands = (
        hard_gate_command_builder(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            contract_mode=contract_mode,
        )
        if callable(hard_gate_command_builder) and run_id and round_id
        else {}
    )
    entry_commands = (
        operator_commands_builder(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
        )
        if run_id and round_id
        else {}
    )
    return {
        "entry_gate_present": bool(gate),
        "entry_status": maybe_text(gate.get("entry_status")) or "",
        "orchestration_mode": maybe_text(gate.get("orchestration_mode")) or "",
        "entry_gate_path": str(agent_entry_gate_path(run_dir, round_id).resolve()) if round_id else "",
        "mission_scaffold_path": str(mission_scaffold_path(run_dir, round_id).resolve()) if round_id else "",
        "agent_advisory_plan_path": str(agent_advisory_plan_path(run_dir, round_id).resolve()) if round_id else "",
        "recommended_entry_skills": gate.get("recommended_entry_skills", []) if isinstance(gate.get("recommended_entry_skills"), list) else [],
        "materialize_agent_entry_gate_command": maybe_text(entry_commands.get("materialize_agent_entry_gate_command")),
        "refresh_agent_entry_gate_command": maybe_text(entry_commands.get("refresh_agent_entry_gate_command")),
        "materialize_agent_advisory_plan_command": maybe_text(entry_commands.get("materialize_agent_advisory_plan_command")),
        "read_board_delta_command": maybe_text(entry_commands.get("read_board_delta_command")),
        "query_public_signals_command": maybe_text(entry_commands.get("query_public_signals_command")),
        "query_environment_signals_command": maybe_text(entry_commands.get("query_environment_signals_command")),
        "list_claim_cluster_result_sets_command": maybe_text(entry_commands.get("list_claim_cluster_result_sets_command")),
        "query_claim_cluster_items_command_template": maybe_text(entry_commands.get("query_claim_cluster_items_command_template")),
        "open_next_round_command_template": maybe_text(handoff_commands.get("open_next_round")),
        "return_to_supervisor_command": maybe_text(handoff_commands.get("supervise_round")),
    }


def agent_entry_state(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str = "warn",
    agent_entry_profile: dict[str, Any] | None = None,
    hard_gate_command_builder: HardGateCommandBuilder | None = None,
) -> dict[str, Any]:
    if not round_id:
        return {}
    gate = load_json_if_exists(agent_entry_gate_path(run_dir, round_id)) or {}
    return {
        "gate": gate,
        "operator": agent_entry_operator_view(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            gate_payload=gate,
            contract_mode=contract_mode,
            agent_entry_profile=agent_entry_profile,
            hard_gate_command_builder=hard_gate_command_builder,
        ),
    }


def materialize_agent_entry_gate(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    agent_entry_profile: dict[str, Any],
    hard_gate_command_builder: HardGateCommandBuilder,
    entry_chain_builder: EntryChainBuilder,
    contract_mode: str = "warn",
    refresh_advisory_plan: bool = False,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
) -> dict[str, Any]:
    profile = resolved_agent_entry_profile(agent_entry_profile)
    resolved_run_dir = resolve_run_dir(run_dir)
    initial_payload = build_agent_entry_payload(
        resolved_run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
        agent_entry_profile=profile,
        hard_gate_command_builder=hard_gate_command_builder,
        entry_chain_builder=entry_chain_builder,
    )
    advisory_plan_file = agent_advisory_plan_path(resolved_run_dir, round_id)
    advisory_plan_materialized = False
    advisory_plan_receipt_id = ""
    if maybe_text(initial_payload.get("entry_status")) != "blocked" and (
        refresh_advisory_plan or not advisory_plan_file.exists()
    ):
        plan_result = materialize_agent_entry_advisory_plan(
            resolved_run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            advisory_sources=profile_list(profile, "advisory_sources"),
            timeout_seconds=timeout_seconds,
            retry_budget=retry_budget,
            retry_backoff_ms=retry_backoff_ms,
            allow_side_effects=allow_side_effects,
        )
        advisory_plan_materialized = bool(plan_result.get("materialized"))
        advisory_plan_receipt_id = maybe_text(plan_result.get("receipt_id"))
    payload = build_agent_entry_payload(
        resolved_run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
        agent_entry_profile=profile,
        hard_gate_command_builder=hard_gate_command_builder,
        entry_chain_builder=entry_chain_builder,
    )
    output_file = agent_entry_gate_path(resolved_run_dir, round_id)
    write_json(output_file, payload)
    append_ledger_event(
        resolved_run_dir,
        {
            "schema_version": "runtime-event-v3",
            "event_id": new_runtime_event_id(
                "runtimeevt",
                run_id,
                round_id,
                "agent-entry-gate",
                payload.get("generated_at_utc"),
                payload.get("entry_status"),
            ),
            "event_type": "agent-entry-gate",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": payload.get("generated_at_utc"),
            "completed_at_utc": payload.get("generated_at_utc"),
            "status": "completed",
            "entry_status": payload.get("entry_status"),
            "orchestration_mode": payload.get("orchestration_mode"),
            "agent_entry_gate_path": str(output_file.resolve()),
            "agent_advisory_plan_path": str(advisory_plan_file.resolve()),
            "advisory_plan_materialized": advisory_plan_materialized,
            "advisory_plan_receipt_id": advisory_plan_receipt_id,
            "advisory_plan_source": maybe_text(payload.get("advisory_plan", {}).get("plan_source"))
            if isinstance(payload.get("advisory_plan"), dict)
            else "",
        },
    )
    return {
        "status": "completed",
        "summary": {
            "run_dir": str(resolved_run_dir),
            "run_id": run_id,
            "round_id": round_id,
            "entry_status": maybe_text(payload.get("entry_status")),
            "orchestration_mode": maybe_text(payload.get("orchestration_mode")),
            "output_path": str(output_file.resolve()),
            "advisory_plan_path": str(advisory_plan_file.resolve()),
            "advisory_plan_present": bool(payload.get("advisory_plan", {}).get("present"))
            if isinstance(payload.get("advisory_plan"), dict)
            else False,
            "advisory_plan_materialized": advisory_plan_materialized,
            "advisory_plan_source": maybe_text(payload.get("advisory_plan", {}).get("plan_source"))
            if isinstance(payload.get("advisory_plan"), dict)
            else "",
            "analysis_kind_count": int(payload.get("analysis_surface", {}).get("analysis_kind_count") or 0)
            if isinstance(payload.get("analysis_surface"), dict)
            else 0,
            "recommended_skill_count": len(payload.get("recommended_entry_skills", []))
            if isinstance(payload.get("recommended_entry_skills"), list)
            else 0,
            "role_count": len(payload.get("role_entry_points", []))
            if isinstance(payload.get("role_entry_points"), list)
            else 0,
        },
        "agent_entry": payload,
    }


__all__ = [
    "agent_entry_state",
    "materialize_agent_entry_gate",
]
