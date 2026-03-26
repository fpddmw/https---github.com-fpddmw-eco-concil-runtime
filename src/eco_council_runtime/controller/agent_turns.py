"""Agent-turn resolution and OpenClaw execution helpers."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.controller.constants import (
    CURATION_ROLES,
    READINESS_ROLES,
    REPORT_ROLES,
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
)
from eco_council_runtime.controller.io import (
    atomic_write_text_file,
    extract_json_suffix,
    load_text,
    maybe_text,
    write_json,
)
from eco_council_runtime.controller.openclaw import (
    ensure_openclaw_agent,
    maybe_compact_openclaw_message,
    openclaw_cli_env,
)
from eco_council_runtime.controller.paths import (
    claim_curation_packet_path,
    claim_curation_prompt_path,
    data_readiness_packet_path,
    data_readiness_prompt_path,
    decision_packet_path,
    decision_prompt_path,
    history_context_path,
    investigation_review_packet_path,
    investigation_review_prompt_path,
    matching_adjudication_packet_path,
    matching_adjudication_prompt_path,
    matching_authorization_packet_path,
    matching_authorization_prompt_path,
    mission_path,
    observation_curation_packet_path,
    observation_curation_prompt_path,
    report_packet_path,
    report_prompt_path,
    response_base_path,
    session_prompt_path,
    source_selection_packet_path,
    source_selection_prompt_path,
    task_review_prompt_path,
    tasks_path,
)
from eco_council_runtime.layout import PROJECT_DIR


def current_agent_turn(*, state: dict[str, Any], requested_role: str) -> tuple[str, str, str]:
    stage = maybe_text(state.get("stage"))
    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    requested = maybe_text(requested_role)

    if stage == STAGE_AWAITING_TASK_REVIEW:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "task-review", "round-task")

    if stage == STAGE_AWAITING_SOURCE_SELECTION:
        missing = [
            role
            for role in SOURCE_SELECTION_ROLES
            if role not in {maybe_text(item) for item in imports.get("source_selection_roles_received", [])}
        ]
        if requested:
            if requested not in SOURCE_SELECTION_ROLES:
                raise ValueError("Source-selection stage requires role=sociologist or role=environmentalist.")
            if requested not in missing:
                raise ValueError(f"Role {requested} has already been imported for this round.")
            return (requested, "source-selection", "source-selection")
        if len(missing) == 1:
            return (missing[0], "source-selection", "source-selection")
        raise ValueError("Current stage needs --role sociologist or --role environmentalist.")

    if stage == STAGE_AWAITING_EVIDENCE_CURATION:
        received = {maybe_text(item) for item in imports.get("curation_roles_received", []) if maybe_text(item)}
        missing = [role for role in CURATION_ROLES if role not in received]
        if requested:
            if requested not in CURATION_ROLES:
                raise ValueError("Evidence-curation stage requires role=sociologist or role=environmentalist.")
            if requested not in missing:
                raise ValueError(f"Role {requested} has already been imported for this round.")
        elif len(missing) != 1:
            raise ValueError("Current stage needs --role sociologist or --role environmentalist.")
        selected_role = requested or missing[0]
        if selected_role == "sociologist":
            return ("sociologist", "claim-curation", "claim-curation")
        return ("environmentalist", "observation-curation", "observation-curation")

    if stage == STAGE_AWAITING_DATA_READINESS:
        missing = [
            role
            for role in READINESS_ROLES
            if role not in {maybe_text(item) for item in imports.get("data_readiness_roles_received", [])}
        ]
        if requested:
            if requested not in READINESS_ROLES:
                raise ValueError("Data-readiness stage requires role=sociologist or role=environmentalist.")
            if requested not in missing:
                raise ValueError(f"Role {requested} has already been imported for this round.")
            return (requested, "data-readiness", "data-readiness-report")
        if len(missing) == 1:
            return (missing[0], "data-readiness", "data-readiness-report")
        raise ValueError("Current stage needs --role sociologist or --role environmentalist.")

    if stage == STAGE_AWAITING_MATCHING_AUTHORIZATION:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "matching-authorization", "matching-authorization")

    if stage == STAGE_AWAITING_MATCHING_ADJUDICATION:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "matching-adjudication", "matching-adjudication")

    if stage == STAGE_AWAITING_INVESTIGATION_REVIEW:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "investigation-review", "investigation-review")

    if stage == STAGE_AWAITING_DECISION:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "decision", "council-decision")

    if stage == STAGE_AWAITING_REPORTS:
        missing = [role for role in REPORT_ROLES if role not in {maybe_text(item) for item in imports.get("report_roles_received", [])}]
        if requested:
            if requested not in REPORT_ROLES:
                raise ValueError("Report stage requires role=sociologist or role=environmentalist.")
            if requested not in missing:
                raise ValueError(f"Role {requested} has already been imported for this round.")
            return (requested, "report", "expert-report")
        if len(missing) == 1:
            return (missing[0], "report", "expert-report")
        raise ValueError("Current stage needs --role sociologist or --role environmentalist.")

    raise ValueError(f"Current stage does not accept agent turns: {stage}")


def build_agent_message(*, run_dir: Path, state: dict[str, Any], role: str, turn_kind: str) -> str:
    round_id = maybe_text(state.get("current_round_id"))
    session_text = load_text(session_prompt_path(run_dir, role))
    history_text = ""
    history_path: Path | None = None
    if role == "moderator":
        history_path = history_context_path(run_dir, round_id)
        if history_path.exists():
            history_text = load_text(history_path)

    if turn_kind == "task-review":
        prompt_text = load_text(task_review_prompt_path(run_dir, round_id))
        mission_text = load_text(mission_path(run_dir))
        tasks_text = load_text(tasks_path(run_dir, round_id))
        sections = [
            session_text,
            (
                f"Current automated turn: moderator task review for {round_id}.\n"
                "All referenced file contents are embedded below. Do not ask for filesystem access. "
                "Return only the final JSON list."
            ),
            "=== TASK REVIEW PROMPT ===\n" + prompt_text,
            "=== MISSION.JSON ===\n" + mission_text,
            "=== CURRENT TASKS.JSON ===\n" + tasks_text,
        ]
        if history_text:
            sections.append("=== HISTORICAL CASE CONTEXT ===\n" + history_text)
        return maybe_compact_openclaw_message(
            inline_message="\n\n".join(sections),
            session_text=session_text,
            role=role,
            round_id=round_id,
            prompt_path=task_review_prompt_path(run_dir, round_id),
            history_path=history_path if history_text else None,
        )

    prompt_map: dict[str, tuple[Path, Path]] = {
        "source-selection": (
            source_selection_prompt_path(run_dir, round_id, role),
            source_selection_packet_path(run_dir, round_id, role),
        ),
        "claim-curation": (
            claim_curation_prompt_path(run_dir, round_id),
            claim_curation_packet_path(run_dir, round_id),
        ),
        "observation-curation": (
            observation_curation_prompt_path(run_dir, round_id),
            observation_curation_packet_path(run_dir, round_id),
        ),
        "data-readiness": (
            data_readiness_prompt_path(run_dir, round_id, role),
            data_readiness_packet_path(run_dir, round_id, role),
        ),
        "report": (
            report_prompt_path(run_dir, round_id, role),
            report_packet_path(run_dir, round_id, role),
        ),
        "matching-authorization": (
            matching_authorization_prompt_path(run_dir, round_id),
            matching_authorization_packet_path(run_dir, round_id),
        ),
        "matching-adjudication": (
            matching_adjudication_prompt_path(run_dir, round_id),
            matching_adjudication_packet_path(run_dir, round_id),
        ),
        "investigation-review": (
            investigation_review_prompt_path(run_dir, round_id),
            investigation_review_packet_path(run_dir, round_id),
        ),
        "decision": (
            decision_prompt_path(run_dir, round_id),
            decision_packet_path(run_dir, round_id),
        ),
    }
    if turn_kind not in prompt_map:
        raise ValueError(f"Unsupported agent turn kind: {turn_kind}")

    prompt_path, packet_path = prompt_map[turn_kind]
    prompt_text = load_text(prompt_path)
    packet_text = load_text(packet_path)
    action_text = {
        "source-selection": f"{role} source selection",
        "claim-curation": "sociologist claim curation",
        "observation-curation": "environmentalist observation curation",
        "data-readiness": f"{role} data-readiness auditing",
        "report": f"{role} report drafting",
        "matching-authorization": "moderator matching authorization",
        "matching-adjudication": "moderator matching adjudication",
        "investigation-review": "moderator investigation review",
        "decision": "moderator decision drafting",
    }[turn_kind]
    heading_text = {
        "source-selection": "SOURCE SELECTION",
        "claim-curation": "CLAIM CURATION",
        "observation-curation": "OBSERVATION CURATION",
        "data-readiness": "DATA READINESS",
        "report": "REPORT",
        "matching-authorization": "MATCHING AUTHORIZATION",
        "matching-adjudication": "MATCHING ADJUDICATION",
        "investigation-review": "INVESTIGATION REVIEW",
        "decision": "DECISION",
    }[turn_kind]
    sections = [
        session_text,
        (
            f"Current automated turn: {action_text} for {round_id}.\n"
            "The required packet content is embedded below. Do not ask for filesystem access. "
            "Return only the final JSON object."
        ),
        f"=== {heading_text} PROMPT ===\n" + prompt_text,
        f"=== {heading_text} PACKET.JSON ===\n" + packet_text,
    ]
    if history_text and role == "moderator":
        sections.append("=== HISTORICAL CASE CONTEXT ===\n" + history_text)
    return maybe_compact_openclaw_message(
        inline_message="\n\n".join(sections),
        session_text=session_text,
        role=role,
        round_id=round_id,
        prompt_path=prompt_path,
        history_path=history_path if history_text else None,
    )


def run_openclaw_agent_turn(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    turn_kind: str,
    schema_kind: str,
    message: str,
    timeout_seconds: int,
    thinking: str,
    normalize_payload: Callable[..., Any],
    validate_input_file: Callable[[str, Path], None],
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_openclaw_agent(run_dir, role=role, state=state)
    agent_id = maybe_text(state.get("openclaw", {}).get("agents", {}).get(role, {}).get("id"))
    if not agent_id:
        raise ValueError(f"No configured OpenClaw agent id for role={role}")

    base_path = response_base_path(run_dir, round_id, role, turn_kind)
    stdout_path = base_path.with_suffix(".stdout.txt")
    stderr_path = base_path.with_suffix(".stderr.txt")
    json_path = base_path.with_suffix(".json")
    stdout_path.parent.mkdir(parents=True, exist_ok=True)

    argv = [
        "openclaw",
        "--no-color",
        "agent",
        "--agent",
        agent_id,
        "--local",
        "--message",
        message,
        "--timeout",
        str(timeout_seconds),
    ]
    if thinking:
        argv.extend(["--thinking", thinking])

    completed = subprocess.run(
        argv,
        cwd=str(PROJECT_DIR),
        capture_output=True,
        env=openclaw_cli_env(run_dir),
        text=True,
        check=False,
    )
    atomic_write_text_file(stdout_path, completed.stdout)
    atomic_write_text_file(stderr_path, completed.stderr)
    if completed.returncode != 0:
        raise RuntimeError(
            f"OpenClaw agent turn failed for role={role}. "
            f"See {stdout_path} and {stderr_path}."
        )

    payload = normalize_payload(
        schema_kind=schema_kind,
        payload=extract_json_suffix(completed.stdout),
        run_dir=run_dir,
        round_id=round_id,
        role=role,
    )
    write_json(json_path, payload, pretty=True)
    validate_input_file(schema_kind, json_path)
    return {
        "agent_id": agent_id,
        "role": role,
        "turn_kind": turn_kind,
        "schema_kind": schema_kind,
        "response_json_path": str(json_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "payload": payload,
    }
