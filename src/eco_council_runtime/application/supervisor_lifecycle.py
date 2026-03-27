"""Supervisor lifecycle services for stage persistence and transitions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.cli_invocation import runtime_module_argv
from eco_council_runtime.controller.constants import (
    CURATION_ROLES,
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
)
from eco_council_runtime.controller.execution_artifacts import ensure_matching_execution_matches
from eco_council_runtime.controller.io import maybe_text, read_json, run_json_command, utc_now_iso, write_json, write_text
from eco_council_runtime.controller.openclaw import (
    ensure_openclaw_config,
    role_prompt_outbox_text,
    session_prompt_text,
    write_openclaw_workspace_files,
)
from eco_council_runtime.controller.operator_surface import (
    build_current_step_text,
    build_status_payload,
    prune_last_failure,
)
from eco_council_runtime.controller.paths import (
    claim_curation_prompt_path,
    data_plane_execution_path,
    data_readiness_prompt_path,
    decision_prompt_path,
    decision_target_path,
    fetch_execution_path,
    investigation_review_prompt_path,
    latest_round_id,
    matching_adjudication_prompt_path,
    matching_execution_path,
    matching_authorization_prompt_path,
    observation_curation_prompt_path,
    outbox_message_path,
    report_prompt_path,
    session_prompt_path,
    supervisor_current_step_path,
    supervisor_outbox_dir,
    supervisor_state_path,
    task_review_prompt_path,
)
from eco_council_runtime.controller.source_selection import render_source_selection_prompt
from eco_council_runtime.controller.stage_imports import (
    import_data_plane_execution_payload,
    import_fetch_execution_payload,
)
from eco_council_runtime.controller.state_config import (
    ensure_case_library_archive_config,
    ensure_signal_corpus_config,
    write_history_context_file,
)
from eco_council_runtime.layout import PROJECT_DIR

REPO_DIR = PROJECT_DIR


def case_library_argv(*args: object) -> list[str]:
    return runtime_module_argv("case_library", *args)


def orchestrate_argv(*args: object) -> list[str]:
    return runtime_module_argv("orchestrate", *args)


def reporting_argv(*args: object) -> list[str]:
    return runtime_module_argv("reporting", *args)


def signal_corpus_argv(*args: object) -> list[str]:
    return runtime_module_argv("signal_corpus", *args)


def load_state(run_dir: Path) -> dict[str, Any]:
    path = supervisor_state_path(run_dir)
    if not path.exists():
        raise ValueError(f"Supervisor state not found: {path}")
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Supervisor state is not a JSON object: {path}")
    return payload


def save_state(run_dir: Path, state: dict[str, Any]) -> None:
    prune_last_failure(state)
    state["updated_at_utc"] = utc_now_iso()
    refresh_supervisor_files(run_dir, state)
    write_json(supervisor_state_path(run_dir), state, pretty=True)


def stage_import_context() -> dict[str, Any]:
    return {
        "save_state": save_state,
        "status_builder": build_status_payload,
    }


def refresh_supervisor_files(run_dir: Path, state: dict[str, Any]) -> None:
    run_dir = run_dir.expanduser().resolve()
    current_round_id = maybe_text(state.get("current_round_id"))
    if not current_round_id:
        return

    openclaw_section = ensure_openclaw_config(run_dir, state)
    agents = openclaw_section.setdefault("agents", {})

    for role in ROLES:
        role_agent = agents[role]
        agent_id = maybe_text(role_agent.get("id"))
        if not agent_id:
            raise ValueError(f"Missing OpenClaw agent id for role={role}")
        write_openclaw_workspace_files(run_dir=run_dir, state=state, role=role, agent_id=agent_id)
        write_text(
            session_prompt_path(run_dir, role),
            session_prompt_text(run_dir=run_dir, state=state, role=role, agent_id=agent_id),
        )

    history_path = write_history_context_file(run_dir, state, current_round_id)

    outbox_dir = supervisor_outbox_dir(run_dir)
    outbox_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "moderator_task_review",
        "sociologist_source_selection",
        "environmentalist_source_selection",
        "sociologist_claim_curation",
        "environmentalist_observation_curation",
        "sociologist_data_readiness",
        "environmentalist_data_readiness",
        "moderator_matching_authorization",
        "moderator_matching_adjudication",
        "moderator_investigation_review",
        "sociologist_report",
        "environmentalist_report",
        "moderator_decision",
    ):
        path = outbox_message_path(run_dir, name)
        if path.exists():
            path.unlink()

    stage = maybe_text(state.get("stage"))
    if stage == STAGE_AWAITING_TASK_REVIEW:
        write_text(
            outbox_message_path(run_dir, "moderator_task_review"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=task_review_prompt_path(run_dir, current_round_id),
                history_path=history_path,
            ),
        )
    if stage == STAGE_AWAITING_SOURCE_SELECTION:
        for role in SOURCE_SELECTION_ROLES:
            prompt_path = render_source_selection_prompt(run_dir, current_round_id, role)
            write_text(
                outbox_message_path(run_dir, f"{role}_source_selection"),
                role_prompt_outbox_text(
                    role=role,
                    round_id=current_round_id,
                    prompt_path=prompt_path,
                ),
            )
    if stage == STAGE_AWAITING_EVIDENCE_CURATION:
        write_text(
            outbox_message_path(run_dir, "sociologist_claim_curation"),
            role_prompt_outbox_text(
                role="sociologist",
                round_id=current_round_id,
                prompt_path=claim_curation_prompt_path(run_dir, current_round_id),
            ),
        )
        write_text(
            outbox_message_path(run_dir, "environmentalist_observation_curation"),
            role_prompt_outbox_text(
                role="environmentalist",
                round_id=current_round_id,
                prompt_path=observation_curation_prompt_path(run_dir, current_round_id),
            ),
        )
    if stage == STAGE_AWAITING_DATA_READINESS:
        for role in READINESS_ROLES:
            write_text(
                outbox_message_path(run_dir, f"{role}_data_readiness"),
                role_prompt_outbox_text(
                    role=role,
                    round_id=current_round_id,
                    prompt_path=data_readiness_prompt_path(run_dir, current_round_id, role),
                ),
            )
    if stage == STAGE_AWAITING_MATCHING_AUTHORIZATION:
        write_text(
            outbox_message_path(run_dir, "moderator_matching_authorization"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=matching_authorization_prompt_path(run_dir, current_round_id),
                history_path=history_path,
            ),
        )
    if stage == STAGE_AWAITING_MATCHING_ADJUDICATION:
        write_text(
            outbox_message_path(run_dir, "moderator_matching_adjudication"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=matching_adjudication_prompt_path(run_dir, current_round_id),
                history_path=history_path,
            ),
        )
    if stage == STAGE_AWAITING_INVESTIGATION_REVIEW:
        write_text(
            outbox_message_path(run_dir, "moderator_investigation_review"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=investigation_review_prompt_path(run_dir, current_round_id),
                history_path=history_path,
            ),
        )
    if stage == STAGE_AWAITING_REPORTS:
        for role in REPORT_ROLES:
            write_text(
                outbox_message_path(run_dir, f"{role}_report"),
                role_prompt_outbox_text(
                    role=role,
                    round_id=current_round_id,
                    prompt_path=report_prompt_path(run_dir, current_round_id, role),
                ),
            )
    if stage == STAGE_AWAITING_DECISION:
        write_text(
            outbox_message_path(run_dir, "moderator_decision"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=decision_prompt_path(run_dir, current_round_id),
                history_path=history_path,
            ),
        )

    write_text(supervisor_current_step_path(run_dir), build_current_step_text(run_dir, state))


def round_start_imports() -> dict[str, Any]:
    return {
        "task_review_received": False,
        "source_selection_roles_received": [],
        "curation_roles_received": [],
        "data_readiness_roles_received": [],
        "matching_authorization_received": False,
        "matching_adjudication_received": False,
        "investigation_review_received": False,
        "report_roles_received": [],
        "decision_received": False,
    }


def data_plane_imports() -> dict[str, Any]:
    return {
        "task_review_received": True,
        "source_selection_roles_received": list(SOURCE_SELECTION_ROLES),
        "curation_roles_received": [],
        "data_readiness_roles_received": [],
        "matching_authorization_received": False,
        "matching_adjudication_received": False,
        "investigation_review_received": False,
        "report_roles_received": [],
        "decision_received": False,
    }


def reports_imports() -> dict[str, Any]:
    return {
        "task_review_received": True,
        "source_selection_roles_received": list(SOURCE_SELECTION_ROLES),
        "curation_roles_received": list(CURATION_ROLES),
        "data_readiness_roles_received": list(READINESS_ROLES),
        "matching_authorization_received": True,
        "matching_adjudication_received": True,
        "investigation_review_received": True,
        "report_roles_received": [],
        "decision_received": False,
    }


def continue_prepare_round(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        orchestrate_argv("prepare-round", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty"),
        cwd=REPO_DIR,
    )
    state["stage"] = STAGE_READY_FETCH
    save_state(run_dir, state)
    return {"action": "prepare-round", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_execute_fetch(run_dir: Path, state: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        orchestrate_argv(
            "execute-fetch-plan",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--continue-on-error",
            "--skip-existing",
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    execution_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    failures = [
        item
        for item in execution_payload.get("statuses", [])
        if isinstance(item, dict) and maybe_text(item.get("status")) == "failed"
    ]
    state["stage"] = STAGE_READY_DATA_PLANE
    save_state(run_dir, state)
    result = {
        "action": "execute-fetch-plan",
        "payload": payload,
        "state": build_status_payload(run_dir, state),
    }
    if failures:
        result["warnings"] = [
            {
                "kind": "fetch-partial-failure",
                "round_id": round_id,
                "failed_step_ids": [
                    maybe_text(item.get("step_id"))
                    for item in failures
                    if maybe_text(item.get("step_id"))
                ],
                "failed_count": len(failures),
                "message": (
                    "Fetch plan completed with partial failures. "
                    "Downstream normalization will use only the successfully materialized artifacts."
                ),
            }
        ]
    return result


def continue_recover_or_execute_fetch(run_dir: Path, state: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    canonical_execution_path = fetch_execution_path(run_dir, round_id)
    if canonical_execution_path.exists():
        try:
            payload = read_json(canonical_execution_path)
            recovered = import_fetch_execution_payload(
                run_dir=run_dir,
                state=state,
                payload=payload,
                source_path=canonical_execution_path,
                **stage_import_context(),
            )
            recovered["action"] = "reuse-fetch-execution"
            recovered["reused_existing_execution"] = True
            return recovered
        except Exception as exc:  # noqa: BLE001
            return {
                **continue_execute_fetch(run_dir, state, timeout_seconds),
                "recovery_skipped_reason": str(exc),
            }
    return continue_execute_fetch(run_dir, state, timeout_seconds)


def maybe_auto_import_signal_corpus(run_dir: Path, state: dict[str, Any], round_id: str) -> dict[str, Any] | None:
    signal_corpus = ensure_signal_corpus_config(state)
    db_text = maybe_text(signal_corpus.get("db"))
    if not db_text or not bool(signal_corpus.get("auto_import")):
        return {
            "enabled": bool(db_text),
            "attempted": False,
        }
    attempted_at_utc = utc_now_iso()
    try:
        payload = run_json_command(
            signal_corpus_argv("import-run", "--db", db_text, "--run-dir", str(run_dir), "--overwrite", "--pretty"),
            cwd=REPO_DIR,
        )
        result = {
            "enabled": True,
            "attempted": True,
            "ok": True,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "import_result": payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload,
        }
        signal_corpus["last_imported_round_id"] = round_id
        signal_corpus["last_imported_at_utc"] = attempted_at_utc
    except Exception as exc:  # noqa: BLE001
        result = {
            "enabled": True,
            "attempted": True,
            "ok": False,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "error": str(exc),
        }
    signal_corpus["last_import"] = result
    state["signal_corpus"] = signal_corpus
    return result


def maybe_auto_import_case_library(run_dir: Path, state: dict[str, Any], round_id: str) -> dict[str, Any] | None:
    archive = ensure_case_library_archive_config(state)
    db_text = maybe_text(archive.get("db"))
    if not db_text or not bool(archive.get("auto_import")):
        return {
            "enabled": bool(db_text),
            "attempted": False,
        }
    attempted_at_utc = utc_now_iso()
    try:
        payload = run_json_command(
            case_library_argv("import-run", "--db", db_text, "--run-dir", str(run_dir), "--overwrite", "--pretty"),
            cwd=REPO_DIR,
        )
        result = {
            "enabled": True,
            "attempted": True,
            "ok": True,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "import_result": payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload,
        }
        archive["last_imported_round_id"] = round_id
        archive["last_imported_at_utc"] = attempted_at_utc
    except Exception as exc:  # noqa: BLE001
        result = {
            "enabled": True,
            "attempted": True,
            "ok": False,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "error": str(exc),
        }
    archive["last_import"] = result
    state["case_library_archive"] = archive
    return result


def continue_run_data_plane(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        orchestrate_argv("run-data-plane", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty"),
        cwd=REPO_DIR,
    )
    signal_corpus_import = maybe_auto_import_signal_corpus(run_dir, state, round_id)
    if signal_corpus_import is not None and isinstance(payload, dict):
        payload["signal_corpus_import"] = signal_corpus_import
    state["stage"] = STAGE_AWAITING_EVIDENCE_CURATION
    state["imports"] = data_plane_imports()
    save_state(run_dir, state)
    return {"action": "run-data-plane", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_recover_or_run_data_plane(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    canonical_execution_path = data_plane_execution_path(run_dir, round_id)
    if canonical_execution_path.exists():
        try:
            payload = read_json(canonical_execution_path)
            recovered = import_data_plane_execution_payload(
                run_dir=run_dir,
                state=state,
                payload=payload,
                source_path=canonical_execution_path,
                signal_corpus_importer=maybe_auto_import_signal_corpus,
                **stage_import_context(),
            )
            recovered["action"] = "reuse-data-plane-execution"
            recovered["reused_existing_execution"] = True
            return recovered
        except Exception as exc:  # noqa: BLE001
            return {
                **continue_run_data_plane(run_dir, state),
                "recovery_skipped_reason": str(exc),
            }
    return continue_run_data_plane(run_dir, state)


def continue_run_matching_adjudication(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        orchestrate_argv("run-matching-adjudication", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty"),
        cwd=REPO_DIR,
    )
    state["stage"] = STAGE_AWAITING_REPORTS
    state["imports"] = reports_imports()
    save_state(run_dir, state)
    return {"action": "run-matching-adjudication", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_recover_or_run_matching_adjudication(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    canonical_execution_path = matching_execution_path(run_dir, round_id)
    if canonical_execution_path.exists():
        try:
            payload = read_json(canonical_execution_path)
            ensure_matching_execution_matches(payload, run_dir=run_dir, round_id=round_id, source_path=canonical_execution_path)
            state["stage"] = STAGE_AWAITING_REPORTS
            state["imports"] = reports_imports()
            save_state(run_dir, state)
            return {
                "action": "reuse-matching-adjudication-execution",
                "reused_existing_execution": True,
                "payload": payload,
                "state": build_status_payload(run_dir, state),
            }
        except Exception as exc:  # noqa: BLE001
            return {
                **continue_run_matching_adjudication(run_dir, state),
                "recovery_skipped_reason": str(exc),
            }
    return continue_run_matching_adjudication(run_dir, state)


def continue_promote(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        reporting_argv(
            "promote-all",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--allow-overwrite",
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    decision_payload = read_json(decision_target_path(run_dir, round_id))
    if not isinstance(decision_payload, dict):
        raise ValueError("Canonical moderator decision is not a JSON object after promote-all.")
    if bool(decision_payload.get("next_round_required")):
        state["stage"] = STAGE_READY_ADVANCE
    else:
        state["stage"] = STAGE_COMPLETED
    save_state(run_dir, state)
    case_library_import = maybe_auto_import_case_library(run_dir, state, round_id)
    save_state(run_dir, state)
    if case_library_import is not None and isinstance(payload, dict):
        payload["case_library_import"] = case_library_import
    return {"action": "promote-all", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_advance_round(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        orchestrate_argv("advance-round", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty"),
        cwd=REPO_DIR,
    )
    new_round_id = latest_round_id(run_dir)
    state["current_round_id"] = new_round_id
    state["stage"] = STAGE_AWAITING_TASK_REVIEW
    state["imports"] = round_start_imports()
    save_state(run_dir, state)
    return {"action": "advance-round", "payload": payload, "state": build_status_payload(run_dir, state)}
