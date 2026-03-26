"""Operator-facing state, recovery, and current-step helpers."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from eco_council_runtime.controller.common import maybe_int, unique_strings
from eco_council_runtime.controller.constants import (
    DEFAULT_HISTORY_TOP_K,
    DEFAULT_SCHEMA_VERSION,
    LAST_FAILURE_ERROR_LIMIT,
    READINESS_ROLES,
    REPORT_ROLES,
    ROLES,
    SOURCE_SELECTION_ROLES,
    STAGE_AWAITING_DATA_READINESS,
    STAGE_AWAITING_DECISION,
    STAGE_AWAITING_EVIDENCE_CURATION,
    STAGE_AWAITING_INVESTIGATION_REVIEW,
    STAGE_AWAITING_MATCHING_ADJUDICATION,
    STAGE_AWAITING_MATCHING_AUTHORIZATION,
    STAGE_AWAITING_REPORTS,
    STAGE_AWAITING_SOURCE_SELECTION,
    STAGE_AWAITING_TASK_REVIEW,
    STAGE_COMPLETED,
    STAGE_READY_ADVANCE,
    STAGE_READY_DATA_PLANE,
    STAGE_READY_FETCH,
    STAGE_READY_MATCHING_ADJUDICATION,
    STAGE_READY_PREPARE,
    STAGE_READY_PROMOTE,
)
from eco_council_runtime.controller.io import (
    load_json_if_exists,
    maybe_text,
    truncate_text,
    utc_now_iso,
)
from eco_council_runtime.controller.openclaw import (
    ensure_openclaw_config,
    installed_openclaw_skills,
    normalize_agent_prefix,
    openclaw_skill_runtime_section,
    supervisor_status_command,
)
from eco_council_runtime.controller.paths import (
    claim_curation_path,
    claim_curation_prompt_path,
    data_plane_execution_path,
    data_readiness_prompt_path,
    data_readiness_report_path,
    decision_draft_path,
    decision_prompt_path,
    decision_target_path,
    evidence_adjudication_path,
    fetch_execution_path,
    fetch_plan_path,
    history_context_path,
    investigation_review_path,
    investigation_review_prompt_path,
    matching_adjudication_path,
    matching_adjudication_prompt_path,
    matching_authorization_path,
    matching_authorization_prompt_path,
    matching_candidate_set_path,
    matching_execution_path,
    matching_result_path,
    mission_path,
    observation_curation_path,
    observation_curation_prompt_path,
    outbox_message_path,
    override_requests_path,
    report_draft_path,
    report_prompt_path,
    reporting_handoff_path,
    session_prompt_path,
    source_selection_path,
    source_selection_prompt_path,
    supervisor_current_step_path,
    supervisor_dir,
    tasks_path,
    task_review_prompt_path,
)
from eco_council_runtime.controller.policy import (
    effective_constraints,
    load_override_requests,
    policy_profile_summary,
    resolve_schema_version,
)
from eco_council_runtime.controller.run_summary import recommended_commands_for_stage
from eco_council_runtime.controller.state_config import (
    default_case_library_db_path,
    default_signal_corpus_db_path,
    ensure_case_library_archive_config,
    ensure_history_context_config,
    ensure_signal_corpus_config,
    normalize_history_top_k,
)
from eco_council_runtime.layout import SUPERVISOR_SCRIPT_PATH

SCHEMA_VERSION = resolve_schema_version(DEFAULT_SCHEMA_VERSION)


def normalize_last_failure(value: Any, *, round_id: str, stage: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    raw_commands = value.get("recommended_commands")
    raw_paths = value.get("related_paths")
    raw_notes = value.get("notes")
    record = {
        "round_id": maybe_text(value.get("round_id")),
        "stage": maybe_text(value.get("stage")),
        "action": maybe_text(value.get("action")),
        "failed_at_utc": maybe_text(value.get("failed_at_utc")),
        "error": maybe_text(value.get("error")),
        "recoverable": bool(value.get("recoverable")),
        "attempt_count": max(1, maybe_int(value.get("attempt_count"))),
        "recommended_commands": unique_strings(
            [
                maybe_text(item)
                for item in (raw_commands if isinstance(raw_commands, list) else [])
                if maybe_text(item)
            ]
        ),
        "related_paths": unique_strings(
            [
                maybe_text(item)
                for item in (raw_paths if isinstance(raw_paths, list) else [])
                if maybe_text(item)
            ]
        ),
        "notes": unique_strings(
            [
                maybe_text(item)
                for item in (raw_notes if isinstance(raw_notes, list) else [])
                if maybe_text(item)
            ]
        ),
    }
    if record["round_id"] != round_id or record["stage"] != stage:
        return {}
    if not record["action"] or not record["failed_at_utc"] or not record["error"]:
        return {}
    return record


def clear_last_failure(state: dict[str, Any]) -> None:
    state["last_failure"] = {}


def prune_last_failure(state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    record = normalize_last_failure(state.get("last_failure"), round_id=round_id, stage=stage)
    state["last_failure"] = record
    return record


def stage_failure_related_paths(run_dir: Path, state: dict[str, Any], action_name: str) -> list[str]:
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    paths: list[str] = [str(supervisor_current_step_path(run_dir))]
    if not round_id:
        return unique_strings(paths)
    if action_name == "prepare-round":
        paths.extend(
            [
                str(tasks_path(run_dir, round_id)),
                str(fetch_plan_path(run_dir, round_id)),
            ]
        )
    elif action_name == "execute-fetch-plan":
        paths.extend(
            [
                str(fetch_plan_path(run_dir, round_id)),
                str(fetch_execution_path(run_dir, round_id)),
            ]
        )
    elif action_name == "run-data-plane":
        paths.extend(
            [
                str(fetch_execution_path(run_dir, round_id)),
                str(data_plane_execution_path(run_dir, round_id)),
                str(claim_curation_prompt_path(run_dir, round_id)),
                str(observation_curation_prompt_path(run_dir, round_id)),
                str(reporting_handoff_path(run_dir, round_id)),
            ]
        )
    elif action_name == "run-matching-adjudication":
        paths.extend(
            [
                str(matching_authorization_path(run_dir, round_id)),
                str(matching_adjudication_path(run_dir, round_id)),
                str(matching_execution_path(run_dir, round_id)),
                str(investigation_review_prompt_path(run_dir, round_id)),
                str(reporting_handoff_path(run_dir, round_id)),
            ]
        )
    elif action_name == "promote-all":
        paths.extend(
            [
                str(report_draft_path(run_dir, round_id, "sociologist")),
                str(report_draft_path(run_dir, round_id, "environmentalist")),
                str(decision_draft_path(run_dir, round_id)),
            ]
        )
    elif action_name == "advance-round" or stage == STAGE_READY_ADVANCE:
        paths.extend(
            [
                str(decision_target_path(run_dir, round_id)),
            ]
        )
    return unique_strings(paths)


def stage_failure_notes(stage: str) -> list[str]:
    if stage == STAGE_READY_FETCH:
        return [
            "Supervisor keeps the run at ready-to-execute-fetch-plan until a valid canonical fetch_execution.json is available.",
            "Retries reuse existing valid artifacts and rerun only missing or malformed JSON artifacts.",
            "You can also repair raw outputs externally and import fetch_execution.json.",
        ]
    if stage == STAGE_READY_DATA_PLANE:
        return [
            "Supervisor keeps the run at ready-to-run-data-plane until normalization and readiness generation complete.",
            "Inspect data_plane_execution.json to find the failed substep, fix the local issue, then rerun continue-run.",
        ]
    if stage == STAGE_READY_MATCHING_ADJUDICATION:
        return [
            "Supervisor keeps the run at ready-to-run-matching-adjudication until the imported moderator adjudication is materialized and investigation-review packet generation completes.",
            "continue-run will reuse an existing valid matching_adjudication_execution.json; otherwise it regenerates the stage.",
            "Inspect matching_adjudication_execution.json to find the failed substep, fix the local issue, then rerun continue-run.",
        ]
    if stage == STAGE_READY_PREPARE:
        return [
            "The run stays at ready-to-prepare-round until prepare-round succeeds.",
        ]
    if stage == STAGE_READY_PROMOTE:
        return [
            "The run stays at ready-to-promote until promote-all succeeds.",
        ]
    if stage == STAGE_READY_ADVANCE:
        return [
            "The run stays at ready-to-advance-round until next-round scaffolding succeeds.",
        ]
    return []


def failure_recovery_commands(run_dir: Path, state: dict[str, Any], action_name: str) -> list[str]:
    commands = list(recommended_commands_for_stage(run_dir, state))
    commands.append(supervisor_status_command(run_dir))
    if action_name == "execute-fetch-plan":
        commands.append(
            shlex.join(
                [
                    "python3",
                    str(SUPERVISOR_SCRIPT_PATH),
                    "import-fetch-execution",
                    "--run-dir",
                    str(run_dir),
                    "--pretty",
                ]
            )
        )
    return unique_strings(commands)


def record_continue_run_failure(run_dir: Path, state: dict[str, Any], action_name: str, exc: Exception) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    previous = normalize_last_failure(state.get("last_failure"), round_id=round_id, stage=stage)
    attempt_count = 1
    if previous and maybe_text(previous.get("action")) == action_name:
        attempt_count = maybe_int(previous.get("attempt_count")) + 1
    failure = {
        "round_id": round_id,
        "stage": stage,
        "action": action_name,
        "failed_at_utc": utc_now_iso(),
        "error": truncate_text(str(exc), LAST_FAILURE_ERROR_LIMIT),
        "recoverable": True,
        "attempt_count": attempt_count,
        "recommended_commands": failure_recovery_commands(run_dir, state, action_name),
        "related_paths": stage_failure_related_paths(run_dir, state, action_name),
        "notes": stage_failure_notes(stage),
    }
    state["last_failure"] = failure
    return failure


def openclaw_skill_runtime_lines(state: dict[str, Any]) -> list[str]:
    projected_skills = installed_openclaw_skills(state)
    skill_runtime = openclaw_skill_runtime_section(state)
    if not projected_skills:
        return []
    lines = [
        "Projected OpenClaw-managed eco-council skills:",
        "- " + ", ".join(projected_skills),
    ]
    skills_root = maybe_text(skill_runtime.get("skills_root"))
    if skills_root:
        lines.append(f"- detached skills root: {skills_root}")
    managed_dir = maybe_text(skill_runtime.get("managed_skills_dir"))
    if managed_dir:
        lines.append(f"- managed skills dir: {managed_dir}")
    lines.append("")
    return lines


def manual_agent_handoff_lines(
    *,
    run_dir: Path,
    role: str,
    outbox_name: str,
    import_command: str,
    artifact_label: str,
) -> list[str]:
    return [
        "Manual handoff through the provisioned OpenClaw agent:",
        "1. Review the role session prompt in the run workspace:",
        str(session_prompt_path(run_dir, role)),
        "",
        f"2. Send this {artifact_label} prompt to the {role} agent:",
        str(outbox_message_path(run_dir, outbox_name)),
        "",
        "3. Save the returned JSON locally, then import it:",
        import_command,
    ]


def build_current_step_text(run_dir: Path, state: dict[str, Any]) -> str:
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    script_path = str(SUPERVISOR_SCRIPT_PATH)
    lines = [
        f"Current round: {round_id}",
        f"Current stage: {stage}",
        "",
    ]
    lines.extend(openclaw_skill_runtime_lines(state))
    pending_override_requests = load_override_requests(run_dir, round_id) if round_id else []
    if pending_override_requests:
        lines.extend(
            [
                "Pending override requests:",
                *[
                    (
                        f"- {maybe_text(item.get('request_id'))}: {maybe_text(item.get('target_path'))} "
                        f"requested by {maybe_text(item.get('agent_role'))} from {maybe_text(item.get('request_origin_kind'))}"
                    )
                    for item in pending_override_requests
                ],
                "- These requests are advisory only. The active mission envelope does not change until an upstream human/bot edits mission.json and regenerates the relevant stage artifacts.",
                "",
            ]
        )
    last_failure = normalize_last_failure(state.get("last_failure"), round_id=round_id, stage=stage)
    if last_failure:
        lines.extend(
            [
                "Last recorded failure:",
                f"- failed_at_utc: {maybe_text(last_failure.get('failed_at_utc'))}",
                f"- action: {maybe_text(last_failure.get('action'))}",
                f"- recoverable: {'yes' if bool(last_failure.get('recoverable')) else 'no'}",
                f"- attempt_count: {maybe_int(last_failure.get('attempt_count'))}",
                f"- error: {maybe_text(last_failure.get('error'))}",
            ]
        )
        related_paths = [maybe_text(item) for item in last_failure.get("related_paths", []) if maybe_text(item)]
        if related_paths:
            lines.extend(["", "Relevant files:", *[f"- {path}" for path in related_paths]])
        recommended_commands = [maybe_text(item) for item in last_failure.get("recommended_commands", []) if maybe_text(item)]
        if recommended_commands:
            lines.extend(["", "Suggested recovery commands:", *recommended_commands])
        notes = [maybe_text(item) for item in last_failure.get("notes", []) if maybe_text(item)]
        if notes:
            lines.extend(["", "Recovery notes:", *[f"- {note}" for note in notes]])
        lines.append("")
    if stage == STAGE_AWAITING_TASK_REVIEW:
        lines.extend(
            [
                "Preferred: run the moderator turn automatically:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="moderator",
                    outbox_name="moderator_task_review",
                    import_command="python3 " + script_path + " import-task-review --run-dir " + str(run_dir) + " --input /path/to/moderator_tasks.json --pretty",
                    artifact_label="task-review",
                ),
            ]
        )
    elif stage == STAGE_AWAITING_SOURCE_SELECTION:
        lines.extend(
            [
                "Preferred: run the two expert source-selection turns automatically, one by one:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role sociologist --pretty",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role environmentalist --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="sociologist",
                    outbox_name="sociologist_source_selection",
                    import_command="python3 " + script_path + " import-source-selection --run-dir " + str(run_dir) + " --role sociologist --input /path/to/sociologist_source_selection.json --pretty",
                    artifact_label="source-selection",
                ),
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="environmentalist",
                    outbox_name="environmentalist_source_selection",
                    import_command="python3 " + script_path + " import-source-selection --run-dir " + str(run_dir) + " --role environmentalist --input /path/to/environmentalist_source_selection.json --pretty",
                    artifact_label="source-selection",
                ),
            ]
        )
    elif stage == STAGE_READY_PREPARE:
        lines.extend(
            [
                "Run the next approved shell stage:",
                "python3 " + script_path + " continue-run --run-dir " + str(run_dir) + " --pretty",
            ]
        )
    elif stage == STAGE_READY_FETCH:
        lines.extend(
            [
                "Run the local raw-data fetch plan:",
                "python3 " + script_path + " continue-run --run-dir " + str(run_dir) + " --pretty",
                "",
                "External/manual alternative:",
                "1. Materialize the raw artifacts and canonical fetch_execution.json with an external runner.",
                "2. Import that fetch execution result:",
                "python3 " + script_path + " import-fetch-execution --run-dir " + str(run_dir) + " --pretty",
            ]
        )
    elif stage == STAGE_READY_DATA_PLANE:
        lines.extend(
            [
                "Run normalization and evidence-curation packet generation:",
                "python3 " + script_path + " continue-run --run-dir " + str(run_dir) + " --pretty",
            ]
        )
    elif stage == STAGE_AWAITING_EVIDENCE_CURATION:
        lines.extend(
            [
                "Preferred: run the two expert curation turns automatically, one by one:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role sociologist --pretty",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role environmentalist --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="sociologist",
                    outbox_name="sociologist_claim_curation",
                    import_command="python3 " + script_path + " import-claim-curation --run-dir " + str(run_dir) + " --input /path/to/claim_curation.json --pretty",
                    artifact_label="claim-curation",
                ),
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="environmentalist",
                    outbox_name="environmentalist_observation_curation",
                    import_command="python3 " + script_path + " import-observation-curation --run-dir " + str(run_dir) + " --input /path/to/observation_curation.json --pretty",
                    artifact_label="observation-curation",
                ),
            ]
        )
    elif stage == STAGE_AWAITING_DATA_READINESS:
        lines.extend(
            [
                "Preferred: run the two expert data-readiness turns automatically, one by one:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role sociologist --pretty",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role environmentalist --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="sociologist",
                    outbox_name="sociologist_data_readiness",
                    import_command="python3 " + script_path + " import-data-readiness --run-dir " + str(run_dir) + " --role sociologist --input /path/to/sociologist_data_readiness.json --pretty",
                    artifact_label="data-readiness",
                ),
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="environmentalist",
                    outbox_name="environmentalist_data_readiness",
                    import_command="python3 " + script_path + " import-data-readiness --run-dir " + str(run_dir) + " --role environmentalist --input /path/to/environmentalist_data_readiness.json --pretty",
                    artifact_label="data-readiness",
                ),
            ]
        )
    elif stage == STAGE_AWAITING_MATCHING_AUTHORIZATION:
        lines.extend(
            [
                "Preferred: run the moderator matching-authorization turn automatically:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role moderator --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="moderator",
                    outbox_name="moderator_matching_authorization",
                    import_command="python3 " + script_path + " import-matching-authorization --run-dir " + str(run_dir) + " --input /path/to/matching_authorization.json --pretty",
                    artifact_label="matching-authorization",
                ),
            ]
        )
    elif stage == STAGE_AWAITING_MATCHING_ADJUDICATION:
        lines.extend(
            [
                "Preferred: run the moderator matching-adjudication turn automatically:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role moderator --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="moderator",
                    outbox_name="moderator_matching_adjudication",
                    import_command="python3 " + script_path + " import-matching-adjudication --run-dir " + str(run_dir) + " --input /path/to/matching_adjudication.json --pretty",
                    artifact_label="matching-adjudication",
                ),
            ]
        )
    elif stage == STAGE_READY_MATCHING_ADJUDICATION:
        lines.extend(
            [
                "Run the matching materialization stage and build the investigation-review packet:",
                "python3 " + script_path + " continue-run --run-dir " + str(run_dir) + " --pretty",
            ]
        )
    elif stage == STAGE_AWAITING_INVESTIGATION_REVIEW:
        lines.extend(
            [
                "Preferred: run the moderator investigation-review turn automatically:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role moderator --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="moderator",
                    outbox_name="moderator_investigation_review",
                    import_command="python3 " + script_path + " import-investigation-review --run-dir " + str(run_dir) + " --input /path/to/investigation_review.json --pretty",
                    artifact_label="investigation-review",
                ),
            ]
        )
    elif stage == STAGE_AWAITING_REPORTS:
        lines.extend(
            [
                "Preferred: run the two expert report turns automatically, one by one:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role sociologist --pretty",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role environmentalist --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="sociologist",
                    outbox_name="sociologist_report",
                    import_command="python3 " + script_path + " import-report --run-dir " + str(run_dir) + " --role sociologist --input /path/to/sociologist_report.json --pretty",
                    artifact_label="report",
                ),
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="environmentalist",
                    outbox_name="environmentalist_report",
                    import_command="python3 " + script_path + " import-report --run-dir " + str(run_dir) + " --role environmentalist --input /path/to/environmentalist_report.json --pretty",
                    artifact_label="report",
                ),
            ]
        )
    elif stage == STAGE_AWAITING_DECISION:
        lines.extend(
            [
                "Preferred: run the moderator decision turn automatically:",
                "python3 " + script_path + " run-agent-step --run-dir " + str(run_dir) + " --role moderator --pretty",
                "",
                *manual_agent_handoff_lines(
                    run_dir=run_dir,
                    role="moderator",
                    outbox_name="moderator_decision",
                    import_command="python3 " + script_path + " import-decision --run-dir " + str(run_dir) + " --input /path/to/council_decision.json --pretty",
                    artifact_label="decision",
                ),
            ]
        )
    elif stage == STAGE_READY_PROMOTE:
        lines.extend(
            [
                "Promote the approved drafts into canonical files:",
                "python3 " + script_path + " continue-run --run-dir " + str(run_dir) + " --pretty",
            ]
        )
    elif stage == STAGE_READY_ADVANCE:
        lines.extend(
            [
                "Open the next round after approval:",
                "python3 " + script_path + " continue-run --run-dir " + str(run_dir) + " --pretty",
            ]
        )
    else:
        lines.append("Run completed. No further action is required.")
    return "\n".join(lines)


def build_state_payload(*, run_dir: Path, round_id: str, agent_prefix: str) -> dict[str, Any]:
    prefix = normalize_agent_prefix(agent_prefix or run_dir.name)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "current_round_id": round_id,
        "stage": STAGE_AWAITING_TASK_REVIEW,
        "fetch_execution": "supervisor-local-shell",
        "imports": {
            "task_review_received": False,
            "source_selection_roles_received": [],
            "curation_roles_received": [],
            "data_readiness_roles_received": [],
            "matching_authorization_received": False,
            "matching_adjudication_received": False,
            "investigation_review_received": False,
            "report_roles_received": [],
            "decision_received": False,
        },
        "last_failure": {},
        "openclaw": {
            "agent_prefix": prefix,
            "workspace_root": str(supervisor_dir(run_dir) / "openclaw-workspaces"),
            "skill_runtime": {
                "skills_root": "",
                "managed_skills_dir": "",
                "manifest_path": "",
                "projected_skills": [],
                "recognized_skills": [],
                "projection_signature": "",
                "projected_at_utc": "",
            },
            "agents": {
                role: {
                    "id": f"{prefix}-{role}",
                    "workspace": str((supervisor_dir(run_dir) / "openclaw-workspaces" / role).resolve()),
                }
                for role in ROLES
            },
        },
        "history_context": {
            "db": "",
            "top_k": DEFAULT_HISTORY_TOP_K,
        },
        "case_library_archive": {
            "db": default_case_library_db_path(),
            "auto_import": True,
            "last_imported_round_id": "",
            "last_imported_at_utc": "",
            "last_import": {},
        },
        "signal_corpus": {
            "db": default_signal_corpus_db_path(),
            "auto_import": True,
            "last_imported_round_id": "",
            "last_imported_at_utc": "",
            "last_import": {},
        },
        "updated_at_utc": utc_now_iso(),
    }


def build_status_payload(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    run_dir = run_dir.expanduser().resolve()
    ensure_openclaw_config(run_dir, state)
    round_id = maybe_text(state.get("current_round_id"))
    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    stage = maybe_text(state.get("stage"))
    last_failure = normalize_last_failure(state.get("last_failure"), round_id=round_id, stage=stage)
    mission_payload = load_json_if_exists(mission_path(run_dir))
    mission = mission_payload if isinstance(mission_payload, dict) else {}
    pending_override_requests = load_override_requests(run_dir, round_id) if round_id else []
    stage_outboxes = {
        STAGE_AWAITING_TASK_REVIEW: ("moderator_task_review",),
        STAGE_AWAITING_SOURCE_SELECTION: ("sociologist_source_selection", "environmentalist_source_selection"),
        STAGE_AWAITING_EVIDENCE_CURATION: ("sociologist_claim_curation", "environmentalist_observation_curation"),
        STAGE_AWAITING_DATA_READINESS: ("sociologist_data_readiness", "environmentalist_data_readiness"),
        STAGE_AWAITING_MATCHING_AUTHORIZATION: ("moderator_matching_authorization",),
        STAGE_AWAITING_MATCHING_ADJUDICATION: ("moderator_matching_adjudication",),
        STAGE_AWAITING_INVESTIGATION_REVIEW: ("moderator_investigation_review",),
        STAGE_AWAITING_REPORTS: ("sociologist_report", "environmentalist_report"),
        STAGE_AWAITING_DECISION: ("moderator_decision",),
    }.get(stage, ())

    outbox_paths: dict[str, str] = {}
    for name in stage_outboxes:
        path = outbox_message_path(run_dir, name)
        if path.exists():
            outbox_paths[name] = str(path)

    session_paths = {role: str(session_prompt_path(run_dir, role)) for role in ROLES}
    history = ensure_history_context_config(state)
    case_library_archive = ensure_case_library_archive_config(state)
    signal_corpus = ensure_signal_corpus_config(state)
    history_file = history_context_path(run_dir, round_id) if round_id else None
    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "current_round_id": round_id,
        "stage": stage,
        "policy_profile": policy_profile_summary(mission) if mission else {},
        "effective_constraints": effective_constraints(mission) if mission else {},
        "fetch_execution": maybe_text(state.get("fetch_execution")),
        "imports": {
            "task_review_received": bool(imports.get("task_review_received")),
            "source_selection_roles_received": sorted(
                {maybe_text(role) for role in imports.get("source_selection_roles_received", []) if maybe_text(role)}
            ),
            "curation_roles_received": sorted(
                {maybe_text(role) for role in imports.get("curation_roles_received", []) if maybe_text(role)}
            ),
            "data_readiness_roles_received": sorted(
                {maybe_text(role) for role in imports.get("data_readiness_roles_received", []) if maybe_text(role)}
            ),
            "matching_authorization_received": bool(imports.get("matching_authorization_received")),
            "matching_adjudication_received": bool(imports.get("matching_adjudication_received")),
            "investigation_review_received": bool(imports.get("investigation_review_received")),
            "report_roles_received": sorted(
                {maybe_text(role) for role in imports.get("report_roles_received", []) if maybe_text(role)}
            ),
            "decision_received": bool(imports.get("decision_received")),
        },
        "task_review_prompt_path": str(task_review_prompt_path(run_dir, round_id)),
        "source_selection_paths": {
            role: str(source_selection_path(run_dir, round_id, role))
            for role in SOURCE_SELECTION_ROLES
        },
        "source_selection_prompt_paths": {
            role: str(source_selection_prompt_path(run_dir, round_id, role))
            for role in SOURCE_SELECTION_ROLES
        },
        "curation_paths": {
            "sociologist": str(claim_curation_path(run_dir, round_id)),
            "environmentalist": str(observation_curation_path(run_dir, round_id)),
        },
        "curation_prompt_paths": {
            "sociologist": str(claim_curation_prompt_path(run_dir, round_id)),
            "environmentalist": str(observation_curation_prompt_path(run_dir, round_id)),
        },
        "data_readiness_paths": {
            role: str(data_readiness_report_path(run_dir, round_id, role))
            for role in READINESS_ROLES
        },
        "data_readiness_prompt_paths": {
            role: str(data_readiness_prompt_path(run_dir, round_id, role))
            for role in READINESS_ROLES
        },
        "matching_authorization_path": str(matching_authorization_path(run_dir, round_id)),
        "matching_authorization_prompt_path": str(matching_authorization_prompt_path(run_dir, round_id)),
        "matching_candidate_set_path": str(matching_candidate_set_path(run_dir, round_id)),
        "matching_adjudication_path": str(matching_adjudication_path(run_dir, round_id)),
        "matching_adjudication_prompt_path": str(matching_adjudication_prompt_path(run_dir, round_id)),
        "investigation_review_path": str(investigation_review_path(run_dir, round_id)),
        "investigation_review_prompt_path": str(investigation_review_prompt_path(run_dir, round_id)),
        "report_prompt_paths": {
            role: str(report_prompt_path(run_dir, round_id, role))
            for role in REPORT_ROLES
        },
        "decision_prompt_path": str(decision_prompt_path(run_dir, round_id)),
        "override_request_paths": {role: str(override_requests_path(run_dir, round_id, role)) for role in ROLES},
        "pending_override_request_count": len(pending_override_requests),
        "pending_override_requests": pending_override_requests,
        "fetch_plan_path": str(fetch_plan_path(run_dir, round_id)),
        "fetch_execution_path": str(fetch_execution_path(run_dir, round_id)),
        "data_plane_execution_path": str(data_plane_execution_path(run_dir, round_id)),
        "matching_execution_path": str(matching_execution_path(run_dir, round_id)),
        "matching_result_path": str(matching_result_path(run_dir, round_id)),
        "evidence_adjudication_path": str(evidence_adjudication_path(run_dir, round_id)),
        "session_prompt_paths": session_paths,
        "outbox_paths": outbox_paths,
        "current_step_path": str(supervisor_current_step_path(run_dir)),
        "recommended_commands": recommended_commands_for_stage(run_dir, state),
        "last_failure": last_failure,
        "openclaw": state.get("openclaw", {}),
        "history_context": {
            "db": maybe_text(history.get("db")),
            "top_k": normalize_history_top_k(history.get("top_k")),
            "context_path": str(history_file) if history_file is not None and history_file.exists() else "",
        },
        "case_library_archive": {
            "db": maybe_text(case_library_archive.get("db")),
            "auto_import": bool(case_library_archive.get("auto_import")),
            "last_imported_round_id": maybe_text(case_library_archive.get("last_imported_round_id")),
            "last_imported_at_utc": maybe_text(case_library_archive.get("last_imported_at_utc")),
            "last_import": case_library_archive.get("last_import", {}),
        },
        "signal_corpus": {
            "db": maybe_text(signal_corpus.get("db")),
            "auto_import": bool(signal_corpus.get("auto_import")),
            "last_imported_round_id": maybe_text(signal_corpus.get("last_imported_round_id")),
            "last_imported_at_utc": maybe_text(signal_corpus.get("last_imported_at_utc")),
            "last_import": signal_corpus.get("last_import", {}),
        },
    }
