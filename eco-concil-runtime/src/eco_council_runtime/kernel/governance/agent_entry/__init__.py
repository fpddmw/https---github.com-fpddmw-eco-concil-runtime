from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.governance.agent_entry.handoff import EntryChainBuilder, HardGateCommandBuilder
from eco_council_runtime.kernel.governance.round_liveness import build_round_liveness_surface
from eco_council_runtime.kernel.governance.role_contracts import normalize_actor_role
from eco_council_runtime.kernel.planes.analysis_plane import query_analysis_result_sets
from eco_council_runtime.kernel.planes.deliberation_plane import load_round_snapshot
from eco_council_runtime.kernel.execution.executor import (
    maybe_text,
    new_runtime_event_id,
    utc_now_iso,
)
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.operator.operations import load_admission_policy, runtime_health_payload
from eco_council_runtime.kernel.core.paths import (
    agent_entry_gate_path,
    mission_scaffold_path,
    resolve_run_dir,
)
from eco_council_runtime.kernel.source_queue.source_queue_contract import mission_input_semantics
from eco_council_runtime.objects.council import query_council_objects
from eco_council_runtime.runtime_command_hints import kernel_command


COORDINATION_OBJECT_KINDS = (
    "investigation-plan",
    "subissue",
    "investigation-scope",
    "round-brief",
    "round-synthesis",
    "evidence-request",
    "source-acquisition-proposal",
    "agent-position",
    "context-packet",
    "challenge-disposition",
)

COORDINATION_HINT_SEMANTICS = (
    "Optional council coordination context only; it does not restrict agent "
    "write surfaces, source selection, evidence acceptance, or investigator autonomy."
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
    mission_path = run_dir / "mission.json"
    mission_payload = load_json_if_exists(mission_path) or {}
    semantics = (
        mission_payload.get("mission_input_semantics")
        if isinstance(mission_payload.get("mission_input_semantics"), dict)
        else mission_input_semantics()
    )
    scope_status = (
        mission_payload.get("mission_scope_status")
        if isinstance(mission_payload.get("mission_scope_status"), dict)
        else {}
    )
    verification_scope = (
        mission_payload.get("verification_scope")
        if isinstance(mission_payload.get("verification_scope"), dict)
        else {}
    )
    return {
        "present": bool(payload or mission_payload),
        "path": str(mission_scaffold_path(run_dir, round_id).resolve()),
        "scaffold_path": str(mission_scaffold_path(run_dir, round_id).resolve()),
        "mission_path": str(mission_path.resolve()),
        "topic": maybe_text(mission_payload.get("topic") or payload.get("topic")),
        "objective": maybe_text(mission_payload.get("objective") or payload.get("objective")),
        "request_text": maybe_text(
            mission_payload.get("request_text")
            or payload.get("request_text")
            or mission_payload.get("objective")
            or payload.get("objective")
        ),
        "policy_profile": maybe_text(mission_payload.get("policy_profile")),
        "mission_input_semantics": semantics,
        "mission_scope_status": scope_status,
        "verification_scope_mode": maybe_text(verification_scope.get("scope_mode")),
        "orchestration_mode": maybe_text(payload.get("orchestration_mode")),
        "scaffold_id": maybe_text(payload.get("scaffold_id")),
        "task_count": int(payload.get("task_count") or 0),
        "import_source_count": int(payload.get("import_source_count") or 0),
        "request_source_count": int(payload.get("request_source_count") or 0),
    }


def text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [maybe_text(item) for item in value if maybe_text(item)]


def coordination_query_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    object_kind: str,
) -> str:
    return kernel_command(
        "query-council-objects",
        "--run-dir",
        str(run_dir),
        "--object-kind",
        object_kind,
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--include-contract",
        "--pretty",
    )


def compact_coordination_object(payload: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {
        "object_kind": maybe_text(payload.get("object_kind")),
        "object_id": maybe_text(payload.get("object_id")),
        "author_role": maybe_text(payload.get("author_role")),
        "status": maybe_text(payload.get("status")),
        "generated_at_utc": maybe_text(payload.get("generated_at_utc")),
        "target_kind": maybe_text(payload.get("target_kind")),
        "target_id": maybe_text(payload.get("target_id")),
        "rationale": maybe_text(payload.get("rationale")),
    }
    for field_name in (
        "title",
        "question",
        "summary",
        "summary_text",
        "brief_text",
        "request_text",
        "scope_text",
        "claim_summary",
        "round_mode",
        "context_packet_id",
        "desired_evidence_type",
        "packet_profile",
        "target_round_id",
        "raw_data_policy",
        "scope_kind",
        "spatial_scope",
        "temporal_scope",
        "object_scope",
        "metric_scope",
        "comparison_frame",
        "ordering_semantics",
        "compression_policy",
    ):
        value = maybe_text(payload.get(field_name))
        if value:
            compact[field_name] = value
    for field_name in (
        "proposed_subissue_refs",
        "scope_hint_refs",
        "open_questions",
        "source_hints",
        "boundary_notes",
        "source_boundary_notes",
        "limitations",
        "open_challenge_refs",
        "primary_focus_refs",
        "requested_outputs",
        "invited_roles",
        "included_object_refs",
        "excluded_object_refs",
        "target_refs",
        "delta_refs",
        "source_refs",
        "evidence_refs",
        "lineage",
    ):
        values = text_list(payload.get(field_name))
        if values:
            compact[field_name] = values
    return {key: value for key, value in compact.items() if value not in ("", [], {})}


def coordination_object_set(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    object_kind: str,
    limit: int,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    try:
        result = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except ValueError as exc:
        warnings.append(
            {
                "code": "coordination-object-query-failed",
                "message": str(exc),
            }
        )
        return {
            "object_kind": object_kind,
            "matching_object_count": 0,
            "returned_object_count": 0,
            "objects": [],
            "query_command": coordination_query_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                object_kind=object_kind,
            ),
        }, warnings
    objects = [
        compact_coordination_object(item)
        for item in result.get("objects", [])
        if isinstance(item, dict)
    ]
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    return {
        "object_kind": object_kind,
        "matching_object_count": int(summary.get("matching_object_count") or len(objects)),
        "returned_object_count": len(objects),
        "objects": objects,
        "query_command": coordination_query_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind=object_kind,
        ),
    }, warnings


def coordination_object_ref(payload: dict[str, Any]) -> dict[str, str]:
    return {
        "object_kind": maybe_text(payload.get("object_kind")),
        "object_id": maybe_text(payload.get("object_id")),
    }


def round_opening_context_surface(run_dir: Path, *, round_id: str) -> dict[str, Any]:
    transition_path = run_dir / "runtime" / f"round_transition_{round_id}.json"
    task_path = run_dir / "investigation" / f"round_tasks_{round_id}.json"
    transition = load_json_if_exists(transition_path) or {}
    tasks_payload = load_json_if_exists(task_path) or []
    task_context: dict[str, Any] = {}
    if isinstance(tasks_payload, list):
        for task in tasks_payload:
            if not isinstance(task, dict):
                continue
            inputs = task.get("inputs", {}) if isinstance(task.get("inputs"), dict) else {}
            candidate = (
                inputs.get("round_coordination_context")
                if isinstance(inputs.get("round_coordination_context"), dict)
                else {}
            )
            if candidate:
                task_context = candidate
                break
    if not isinstance(transition, dict):
        transition = {}
    primary_focus_refs = text_list(transition.get("primary_focus_refs")) or text_list(
        task_context.get("primary_focus_refs")
    )
    context_packet_id = maybe_text(transition.get("context_packet_id")) or maybe_text(
        task_context.get("context_packet_id")
    )
    round_brief_id = maybe_text(transition.get("round_brief_id")) or maybe_text(
        task_context.get("round_brief_id")
    )
    return {
        key: value
        for key, value in {
            "present": bool(transition or task_context),
            "source": "round-transition-artifact"
            if transition
            else "round-task-context"
            if task_context
            else "missing",
            "transition_artifact_path": str(transition_path.resolve()),
            "task_artifact_path": str(task_path.resolve()),
            "transition_id": maybe_text(transition.get("transition_id")),
            "transition_request_id": maybe_text(
                transition.get("transition_request_id")
            ),
            "source_round_id": maybe_text(transition.get("source_round_id"))
            or maybe_text(task_context.get("source_round_id")),
            "round_mode": maybe_text(transition.get("round_mode"))
            or maybe_text(task_context.get("round_mode")),
            "primary_focus_refs": primary_focus_refs,
            "target_challenge_id": maybe_text(transition.get("target_challenge_id"))
            or maybe_text(task_context.get("target_challenge_id")),
            "context_packet_id": context_packet_id,
            "round_brief_id": round_brief_id,
            "semantics": (
                "Round opening context is a handoff surface only; it does not "
                "restrict agent write surfaces, source selection, evidence "
                "acceptance, or investigator autonomy."
            ),
        }.items()
        if value not in ("", [], {})
    }


def coordination_surface(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    object_sets: dict[str, Any] = {}
    warnings: list[dict[str, str]] = []
    for object_kind in COORDINATION_OBJECT_KINDS:
        object_set, object_warnings = coordination_object_set(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind=object_kind,
            limit=20,
        )
        object_sets[object_kind] = object_set
        warnings.extend(object_warnings)

    round_briefs = object_sets.get("round-brief", {}).get("objects", [])
    latest_round_brief = (
        round_briefs[0]
        if isinstance(round_briefs, list) and round_briefs and isinstance(round_briefs[0], dict)
        else {}
    )
    context_packet_id = maybe_text(latest_round_brief.get("context_packet_id"))
    context_packets = object_sets.get("context-packet", {}).get("objects", [])
    context_packet = {}
    if isinstance(context_packets, list):
        for packet in context_packets:
            if not isinstance(packet, dict):
                continue
            if context_packet_id and maybe_text(packet.get("object_id")) != context_packet_id:
                continue
            context_packet = packet
            break
    object_refs = [
        coordination_object_ref(item)
        for object_set in object_sets.values()
        if isinstance(object_set, dict)
        for item in object_set.get("objects", [])
        if isinstance(item, dict) and maybe_text(item.get("object_id"))
    ]
    return {
        "schema_version": "agent-entry-coordination-surface-v1",
        "run_id": run_id,
        "round_id": round_id,
        "semantics": COORDINATION_HINT_SEMANTICS,
        "ordering_semantics": "Objects are exposed in deterministic generated_at order only, not salience or evidence strength order.",
        "round_opening_context": round_opening_context_surface(
            run_dir,
            round_id=round_id,
        ),
        "latest_round_brief": latest_round_brief,
        "context_packet": context_packet
        or (
            {
                "object_kind": "context-packet",
                "object_id": context_packet_id,
                "resolution_status": "referenced-not-loaded",
            }
            if context_packet_id
            else {}
        ),
        "object_refs": object_refs,
        "object_sets": object_sets,
        "query_commands": {
            object_kind: maybe_text(object_set.get("query_command"))
            for object_kind, object_set in object_sets.items()
            if isinstance(object_set, dict)
        },
        "warnings": warnings,
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
    actor_role: str,
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
    round_state = round_surface(run_dir, run_id=run_id, round_id=round_id)
    analysis = analysis_surface(run_dir, run_id=run_id, round_id=round_id)
    coordination = coordination_surface(run_dir, run_id=run_id, round_id=round_id)
    next_round_id = maybe_text(
        next_round_id_builder(
            run_dir=run_dir,
            current_round_id=round_id,
        )
    )
    round_liveness = build_round_liveness_surface(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        next_round_id=next_round_id,
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
    recommended_skills = recommended_skills_builder()
    requested_by_role = maybe_text(actor_role)
    payload = {
        "schema_version": "runtime-agent-entry-gate-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "requested_by_role": requested_by_role,
        "resolved_requested_by_role": normalize_actor_role(requested_by_role),
        "entry_id": "agent-entry-" + new_runtime_event_id("gate", run_id, round_id, status).split("-", 1)[1],
        "entry_status": status,
        "orchestration_mode": maybe_text(mission.get("orchestration_mode")) or "openclaw-agent",
        "contract_mode": contract_mode,
        "output_path": str(agent_entry_gate_path(run_dir, round_id).resolve()),
        "mission": mission,
        "governance": governance,
        "round_surface": round_state,
        "analysis_surface": analysis,
        "coordination_surface": coordination,
        "round_liveness_surface": round_liveness,
        "capability_surface": role_entries,
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
            round_surface_payload=round_state,
            analysis=analysis,
        ),
        "warnings": warnings
        + (
            analysis.get("warnings", [])
            if isinstance(analysis.get("warnings"), list)
            else []
        )
        + (
            coordination.get("warnings", [])
            if isinstance(coordination.get("warnings"), list)
            else []
        )
        + (
            round_liveness.get("warnings", [])
            if isinstance(round_liveness.get("warnings"), list)
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
    current_round_liveness: dict[str, Any] | None = None,
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
    coordination = (
        gate.get("coordination_surface")
        if isinstance(gate.get("coordination_surface"), dict)
        else {}
    )
    latest_round_brief = (
        coordination.get("latest_round_brief")
        if isinstance(coordination.get("latest_round_brief"), dict)
        else {}
    )
    context_packet = (
        coordination.get("context_packet")
        if isinstance(coordination.get("context_packet"), dict)
        else {}
    )
    round_opening_context = (
        coordination.get("round_opening_context")
        if isinstance(coordination.get("round_opening_context"), dict)
        else {}
    )
    gate_round_liveness = (
        gate.get("round_liveness_surface")
        if isinstance(gate.get("round_liveness_surface"), dict)
        else {}
    )
    round_liveness = (
        current_round_liveness
        if isinstance(current_round_liveness, dict)
        else build_round_liveness_surface(run_dir, run_id=run_id, round_id=round_id)
        if run_id and round_id
        else {}
    )
    gate_unresolved_refs = (
        gate_round_liveness.get("unresolved_refs", [])
        if isinstance(gate_round_liveness.get("unresolved_refs"), list)
        else []
    )
    current_unresolved_refs = (
        round_liveness.get("unresolved_refs", [])
        if isinstance(round_liveness.get("unresolved_refs"), list)
        else []
    )
    continuation = (
        round_liveness.get("continuation")
        if isinstance(round_liveness.get("continuation"), dict)
        else {}
    )
    closing_checklist = (
        round_liveness.get("closing_checklist")
        if isinstance(round_liveness.get("closing_checklist"), dict)
        else {}
    )
    gate_closing_checklist = (
        gate_round_liveness.get("closing_checklist")
        if isinstance(gate_round_liveness.get("closing_checklist"), dict)
        else {}
    )
    gate_continuation = (
        gate_round_liveness.get("continuation")
        if isinstance(gate_round_liveness.get("continuation"), dict)
        else {}
    )
    continuation_decision_required = bool(
        continuation.get("continuation_decision_required")
    ) or bool(closing_checklist.get("continuation_decision_required"))
    round_synthesis_required = bool(
        continuation.get("round_synthesis_required_before_continuation_decision")
    ) or bool(closing_checklist.get("round_synthesis_required"))
    gate_continuation_decision_required = bool(
        gate_continuation.get("continuation_decision_required")
    ) or bool(gate_closing_checklist.get("continuation_decision_required"))
    gate_round_synthesis_required = bool(
        gate_continuation.get("round_synthesis_required_before_continuation_decision")
    ) or bool(gate_closing_checklist.get("round_synthesis_required"))
    gate_liveness_stale = bool(
        gate
        and gate_round_liveness
        and (
            maybe_text(gate_continuation.get("status"))
            != maybe_text(continuation.get("status"))
            or set(gate_unresolved_refs) != set(current_unresolved_refs)
            or gate_continuation_decision_required != continuation_decision_required
            or gate_round_synthesis_required != round_synthesis_required
        )
    )
    return {
        "entry_gate_present": bool(gate),
        "entry_status": maybe_text(gate.get("entry_status")) or "",
        "orchestration_mode": maybe_text(gate.get("orchestration_mode")) or "",
        "coordination_surface_present": bool(coordination),
        "round_opening_context_present": bool(
            round_opening_context.get("present")
        ),
        "round_opening_source_round_id": maybe_text(
            round_opening_context.get("source_round_id")
        ),
        "round_opening_round_mode": maybe_text(
            round_opening_context.get("round_mode")
        ),
        "round_opening_primary_focus_refs": round_opening_context.get(
            "primary_focus_refs",
            [],
        )
        if isinstance(round_opening_context.get("primary_focus_refs"), list)
        else [],
        "latest_round_brief_id": maybe_text(latest_round_brief.get("object_id")),
        "active_context_packet_id": maybe_text(context_packet.get("object_id")),
        "round_liveness_source": "live-deliberation-plane" if run_id and round_id else "",
        "entry_gate_round_liveness_status": maybe_text(gate_continuation.get("status")),
        "entry_gate_unresolved_ref_count": int(
            gate_round_liveness.get("unresolved_ref_count") or 0
        )
        if gate_round_liveness
        else 0,
        "entry_gate_liveness_stale": gate_liveness_stale,
        "round_liveness_status": maybe_text(continuation.get("status")),
        "round_unresolved_ref_count": int(round_liveness.get("unresolved_ref_count") or 0),
        "round_unresolved_refs": current_unresolved_refs,
        "continuation_decision_required": continuation_decision_required,
        "round_synthesis_required_before_continuation_decision": round_synthesis_required,
        "entry_gate_path": str(agent_entry_gate_path(run_dir, round_id).resolve()) if round_id else "",
        "mission_scaffold_path": str(mission_scaffold_path(run_dir, round_id).resolve()) if round_id else "",
        "recommended_entry_skills": gate.get("recommended_entry_skills", []) if isinstance(gate.get("recommended_entry_skills"), list) else [],
        "show_run_state_command": maybe_text(entry_commands.get("show_run_state_command")),
        "show_council_status_command": maybe_text(entry_commands.get("show_council_status_command")),
        "materialize_agent_entry_gate_command": maybe_text(entry_commands.get("materialize_agent_entry_gate_command")),
        "refresh_agent_entry_gate_command": maybe_text(entry_commands.get("refresh_agent_entry_gate_command")),
        "read_board_delta_command": maybe_text(entry_commands.get("read_board_delta_command")),
        "query_public_signals_command": maybe_text(entry_commands.get("query_public_signals_command")),
        "query_formal_signals_command": maybe_text(entry_commands.get("query_formal_signals_command")),
        "query_environment_signals_command": maybe_text(entry_commands.get("query_environment_signals_command")),
        "query_council_proposals_command": maybe_text(entry_commands.get("query_council_proposals_command")),
        "query_finding_records_command": maybe_text(entry_commands.get("query_finding_records_command")),
        "query_discussion_messages_command": maybe_text(entry_commands.get("query_discussion_messages_command")),
        "query_review_comments_command": maybe_text(entry_commands.get("query_review_comments_command")),
        "query_evidence_bundles_command": maybe_text(entry_commands.get("query_evidence_bundles_command")),
        "query_readiness_opinions_command": maybe_text(entry_commands.get("query_readiness_opinions_command")),
        "query_investigation_plans_command": maybe_text(entry_commands.get("query_investigation_plans_command")),
        "query_subissues_command": maybe_text(entry_commands.get("query_subissues_command")),
        "query_investigation_scopes_command": maybe_text(entry_commands.get("query_investigation_scopes_command")),
        "query_round_briefs_command": maybe_text(entry_commands.get("query_round_briefs_command")),
        "query_round_syntheses_command": maybe_text(entry_commands.get("query_round_syntheses_command")),
        "query_evidence_requests_command": maybe_text(entry_commands.get("query_evidence_requests_command")),
        "query_source_acquisition_proposals_command": maybe_text(entry_commands.get("query_source_acquisition_proposals_command")),
        "update_source_acquisition_proposal_status_command_template": maybe_text(entry_commands.get("update_source_acquisition_proposal_status_command_template")),
        "link_source_acquisition_execution_command_template": maybe_text(entry_commands.get("link_source_acquisition_execution_command_template")),
        "query_agent_positions_command": maybe_text(entry_commands.get("query_agent_positions_command")),
        "query_context_packets_command": maybe_text(entry_commands.get("query_context_packets_command")),
        "query_report_section_drafts_command": maybe_text(entry_commands.get("query_report_section_drafts_command")),
        "query_transition_requests_command": maybe_text(entry_commands.get("query_transition_requests_command")),
        "query_skill_approval_requests_command": maybe_text(entry_commands.get("query_skill_approval_requests_command")),
        "query_skill_approvals_command": maybe_text(entry_commands.get("query_skill_approvals_command")),
        "query_skill_approval_consumptions_command": maybe_text(entry_commands.get("query_skill_approval_consumptions_command")),
        "request_optional_analysis_approval_command_template": maybe_text(entry_commands.get("request_optional_analysis_approval_command_template")),
        "request_falsification_probe_approval_command_template": maybe_text(entry_commands.get("request_falsification_probe_approval_command_template")),
        "approve_skill_approval_command_template": maybe_text(entry_commands.get("approve_skill_approval_command_template")),
        "reject_skill_approval_command_template": maybe_text(entry_commands.get("reject_skill_approval_command_template")),
        "run_approved_optional_analysis_command_template": maybe_text(entry_commands.get("run_approved_optional_analysis_command_template")),
        "request_report_basis_transition_command": maybe_text(entry_commands.get("request_report_basis_transition_command")),
        "request_report_writing_round_command_template": maybe_text(entry_commands.get("request_report_writing_round_command_template")),
        "open_report_writing_round_after_approval_command_template": maybe_text(entry_commands.get("open_report_writing_round_after_approval_command_template")),
        "approve_transition_request_command_template": maybe_text(entry_commands.get("approve_transition_request_command_template")),
        "reject_transition_request_command_template": maybe_text(entry_commands.get("reject_transition_request_command_template")),
        "submit_council_proposal_command_template": maybe_text(entry_commands.get("submit_council_proposal_command_template")),
        "submit_finding_record_command_template": maybe_text(entry_commands.get("submit_finding_record_command_template")),
        "post_discussion_message_command_template": maybe_text(entry_commands.get("post_discussion_message_command_template")),
        "post_review_comment_command_template": maybe_text(entry_commands.get("post_review_comment_command_template")),
        "submit_evidence_bundle_command_template": maybe_text(entry_commands.get("submit_evidence_bundle_command_template")),
        "update_hypothesis_from_finding_command_template": maybe_text(entry_commands.get("update_hypothesis_from_finding_command_template")),
        "open_challenge_on_hypothesis_or_bundle_command_template": maybe_text(entry_commands.get("open_challenge_on_hypothesis_or_bundle_command_template")),
        "submit_report_section_draft_command_template": maybe_text(entry_commands.get("submit_report_section_draft_command_template")),
        "submit_readiness_opinion_command_template": maybe_text(entry_commands.get("submit_readiness_opinion_command_template")),
        "request_continuation_round_command_template": maybe_text(continuation.get("request_open_round_command_template")),
        "open_continuation_round_after_approval_command_template": maybe_text(continuation.get("open_round_after_approval_command_template")),
        "open_next_round_command_template": maybe_text(handoff_commands.get("open_next_round")),
        "request_open_report_writing_round_command_template": maybe_text(handoff_commands.get("request_open_report_writing_round")),
        "open_report_writing_round_command_template": maybe_text(handoff_commands.get("open_report_writing_round")),
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
    current_round_liveness = (
        build_round_liveness_surface(run_dir, run_id=run_id, round_id=round_id)
        if run_id and round_id
        else {}
    )
    return {
        "gate": gate,
        "current_round_liveness_surface": current_round_liveness,
        "operator": agent_entry_operator_view(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            gate_payload=gate,
            contract_mode=contract_mode,
            agent_entry_profile=agent_entry_profile,
            hard_gate_command_builder=hard_gate_command_builder,
            current_round_liveness=current_round_liveness,
        ),
    }


def materialize_agent_entry_gate(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    actor_role: str = "runtime-operator",
    agent_entry_profile: dict[str, Any],
    hard_gate_command_builder: HardGateCommandBuilder,
    entry_chain_builder: EntryChainBuilder,
    contract_mode: str = "warn",
) -> dict[str, Any]:
    profile = resolved_agent_entry_profile(agent_entry_profile)
    resolved_run_dir = resolve_run_dir(run_dir)
    payload = build_agent_entry_payload(
        resolved_run_dir,
        run_id=run_id,
        round_id=round_id,
        actor_role=actor_role,
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
            "actor_role": maybe_text(actor_role),
            "resolved_actor_role": normalize_actor_role(actor_role),
            "started_at_utc": payload.get("generated_at_utc"),
            "completed_at_utc": payload.get("generated_at_utc"),
            "status": "completed",
            "entry_status": payload.get("entry_status"),
            "orchestration_mode": payload.get("orchestration_mode"),
            "agent_entry_gate_path": str(output_file.resolve()),
        },
    )
    return {
        "status": "completed",
        "summary": {
            "run_dir": str(resolved_run_dir),
            "run_id": run_id,
            "round_id": round_id,
            "requested_by_role": maybe_text(actor_role),
            "resolved_requested_by_role": normalize_actor_role(actor_role),
            "entry_status": maybe_text(payload.get("entry_status")),
            "orchestration_mode": maybe_text(payload.get("orchestration_mode")),
            "output_path": str(output_file.resolve()),
            "coordination_object_count": len(payload.get("coordination_surface", {}).get("object_refs", []))
            if isinstance(payload.get("coordination_surface"), dict)
            and isinstance(payload.get("coordination_surface", {}).get("object_refs"), list)
            else 0,
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
    "coordination_surface",
    "agent_entry_state",
    "materialize_agent_entry_gate",
]
