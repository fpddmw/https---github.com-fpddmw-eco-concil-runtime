#!/usr/bin/env python3
"""Run eco-council stages with approval gates and fixed agent handoffs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from eco_council_runtime.controller.agent_turns import (
    build_agent_message,
    current_agent_turn,
    run_openclaw_agent_turn,
)
from eco_council_runtime.controller.agent_ingest import (
    normalize_agent_payload_for_schema,
    normalize_matching_authorization_payload,
    validate_input_file,
)
from eco_council_runtime.controller.artifact_builders import (
    build_data_readiness_artifacts_for_supervisor,
    build_decision_artifacts_for_supervisor,
    build_matching_adjudication_artifacts_for_supervisor,
    build_matching_authorization_artifacts_for_supervisor,
    build_report_artifacts_for_supervisor,
    materialize_curations_for_supervisor,
)
from eco_council_runtime.controller.cli import build_supervisor_parser
from eco_council_runtime.controller.common import first_nonempty
from eco_council_runtime.controller.constants import (
    CURATION_ROLES,
    DEFAULT_SCHEMA_VERSION,
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
    exclusive_file_lock,
    maybe_text,
    pretty_json,
    read_json,
    run_json_command,
    utc_now_iso,
    write_json,
    write_text,
)
from eco_council_runtime.controller.execution_artifacts import ensure_matching_execution_matches
from eco_council_runtime.controller.openclaw import (
    ensure_openclaw_config,
    provision_openclaw_agents_for_run,
    role_prompt_outbox_text,
    session_prompt_text,
    write_openclaw_workspace_files,
)
from eco_council_runtime.controller.operator_surface import (
    build_current_step_text,
    build_state_payload,
    build_status_payload,
    prune_last_failure,
    record_continue_run_failure,
)
from eco_council_runtime.controller.paths import (
    claim_curation_prompt_path,
    data_plane_execution_path,
    data_readiness_prompt_path,
    decision_prompt_path,
    discover_round_ids,
    fetch_execution_path,
    fetch_lock_path,
    investigation_review_prompt_path,
    latest_round_id,
    matching_adjudication_prompt_path,
    matching_authorization_prompt_path,
    matching_execution_path,
    mission_path,
    observation_curation_prompt_path,
    outbox_message_path,
    report_prompt_path,
    require_round_id,
    round_dir,
    session_prompt_path,
    supervisor_current_step_path,
    supervisor_outbox_dir,
    supervisor_state_lock_path,
    supervisor_state_path,
    task_review_prompt_path,
)
from eco_council_runtime.controller.policy import (
    resolve_schema_version,
)
from eco_council_runtime.controller.run_summary import (
    collect_round_summary,
    default_summary_output_path,
    render_run_summary_markdown,
    stage_label_zh,
)
from eco_council_runtime.controller.source_selection import (
    build_source_selection_packet,
    render_source_selection_prompt,
)
from eco_council_runtime.controller.stage_imports import (
    import_claim_curation_payload,
    import_data_plane_execution_payload,
    import_data_readiness_payload,
    import_decision_payload,
    import_fetch_execution_payload,
    import_investigation_review_payload,
    import_matching_adjudication_payload,
    import_matching_authorization_payload,
    import_observation_curation_payload,
    import_report_payload,
    import_source_selection_payload,
    import_task_review_payload,
)
from eco_council_runtime.controller.state_config import (
    apply_case_library_archive_cli_config,
    apply_history_cli_config,
    apply_signal_corpus_cli_config,
    case_library_archive_cli_updates_requested,
    ensure_case_library_archive_config,
    ensure_signal_corpus_config,
    history_cli_updates_requested,
    signal_corpus_cli_updates_requested,
    write_history_context_file,
)
from eco_council_runtime.layout import (
    CASE_LIBRARY_SCRIPT_PATH,
    ORCHESTRATE_SCRIPT_PATH,
    PROJECT_DIR,
    REPORTING_SCRIPT_PATH,
    SIGNAL_CORPUS_SCRIPT_PATH,
)
REPO_DIR = PROJECT_DIR

CASE_LIBRARY_SCRIPT = CASE_LIBRARY_SCRIPT_PATH
ORCHESTRATE_SCRIPT = ORCHESTRATE_SCRIPT_PATH
REPORTING_SCRIPT = REPORTING_SCRIPT_PATH
SIGNAL_CORPUS_SCRIPT = SIGNAL_CORPUS_SCRIPT_PATH
SCHEMA_VERSION = resolve_schema_version(DEFAULT_SCHEMA_VERSION)


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
def ask_for_approval(summary: str, assume_yes: bool = False) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        raise ValueError("Approval is required. Rerun in a terminal or pass --yes.")
    reply = input(f"{summary}\nContinue? [y/N]: ").strip().lower()
    return reply in {"y", "yes"}


def command_init_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    mission_input = Path(args.mission_input).expanduser().resolve()
    run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "bootstrap-run",
            "--run-dir",
            str(run_dir),
            "--mission-input",
            str(mission_input),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    round_id = latest_round_id(run_dir)
    state = build_state_payload(run_dir=run_dir, round_id=round_id, agent_prefix=args.agent_prefix)
    apply_history_cli_config(state, args)
    apply_case_library_archive_cli_config(state, args)
    apply_signal_corpus_cli_config(state, args)
    ensure_openclaw_config(
        run_dir,
        state,
        workspace_root_text=args.workspace_root,
        skills_root_text=args.skills_root,
    )
    provision_result: dict[str, Any]
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        save_state(run_dir, state)
        if args.no_provision_openclaw:
            provision_result = {
                "approved": False,
                "skipped": True,
                "workspace_root": maybe_text(state.get("openclaw", {}).get("workspace_root")),
                "created_agents": [],
            }
        else:
            try:
                provision_result = provision_openclaw_agents_for_run(
                    run_dir,
                    state=state,
                    workspace_root_text=args.workspace_root,
                    skills_root_text=args.skills_root,
                    assume_yes=args.yes,
                    approval_callback=ask_for_approval,
                    require_approval=True,
                    mission=read_json(mission_path(run_dir)),
                )
            except Exception as exc:
                raise RuntimeError(
                    "init-run now provisions OpenClaw agents by default. "
                    "Install/configure OpenClaw, pass --yes in non-interactive mode, or use --no-provision-openclaw to scaffold without agents. "
                    f"Underlying error: {exc}"
                ) from exc
            save_state(run_dir, state)
    payload = build_status_payload(run_dir, state)
    payload["openclaw_provision"] = provision_result
    return payload


def command_status(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    if (
        history_cli_updates_requested(args)
        or case_library_archive_cli_updates_requested(args)
        or signal_corpus_cli_updates_requested(args)
    ):
        with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
            state = load_state(run_dir)
            apply_history_cli_config(state, args)
            apply_case_library_archive_cli_config(state, args)
            apply_signal_corpus_cli_config(state, args)
            save_state(run_dir, state)
        return build_status_payload(run_dir, state)
    state = load_state(run_dir)
    return build_status_payload(run_dir, state)


def command_summarize_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    mission = read_json(mission_path(run_dir))
    if not isinstance(mission, dict):
        raise ValueError(f"Mission payload is not a JSON object: {mission_path(run_dir)}")

    if args.round_id:
        normalized_round_id = require_round_id(args.round_id)
        target_round_dir = round_dir(run_dir, normalized_round_id)
        if not target_round_dir.exists():
            raise ValueError(f"Round directory does not exist: {target_round_dir}")
        round_ids = [normalized_round_id]
    else:
        round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}")

    round_summaries = [collect_round_summary(run_dir, state, round_id) for round_id in round_ids]
    report_text = render_run_summary_markdown(
        run_dir=run_dir,
        state=state,
        mission=mission,
        round_summaries=round_summaries,
        lang=args.lang,
    )

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = default_summary_output_path(run_dir, round_ids[-1] if len(round_ids) == 1 else "", args.lang)
    write_text(output_path, report_text)

    latest_decision_round = first_nonempty(
        [summary["round_id"] for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)]
    )
    latest_decision = next(
        (summary.get("decision") for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)),
        None,
    )
    return {
        "ok": True,
        "run_dir": str(run_dir),
        "output_path": str(output_path),
        "lang": args.lang,
        "current_round_id": maybe_text(state.get("current_round_id")),
        "stage": maybe_text(state.get("stage")),
        "stage_label": stage_label_zh(maybe_text(state.get("stage"))) if args.lang == "zh" else maybe_text(state.get("stage")),
        "round_count": len(round_summaries),
        "round_ids": [summary["round_id"] for summary in round_summaries],
        "latest_decision_round_id": latest_decision_round,
        "latest_decision_requires_next_round": bool(latest_decision.get("next_round_required")) if isinstance(latest_decision, dict) else None,
        "preview": "\n".join(report_text.splitlines()[:20]),
    }


def continue_prepare_round(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "prepare-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    state["stage"] = STAGE_READY_FETCH
    save_state(run_dir, state)
    return {"action": "prepare-round", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_execute_fetch(run_dir: Path, state: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
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
        ],
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
            # If an existing fetch_execution.json is stale or invalid, fall back to a fresh execution.
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
            [
                "python3",
                str(SIGNAL_CORPUS_SCRIPT),
                "import-run",
                "--db",
                db_text,
                "--run-dir",
                str(run_dir),
                "--overwrite",
                "--pretty",
            ],
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
            [
                "python3",
                str(CASE_LIBRARY_SCRIPT),
                "import-run",
                "--db",
                db_text,
                "--run-dir",
                str(run_dir),
                "--overwrite",
                "--pretty",
            ],
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
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "run-data-plane",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    signal_corpus_import = maybe_auto_import_signal_corpus(run_dir, state, round_id)
    if signal_corpus_import is not None and isinstance(payload, dict):
        payload["signal_corpus_import"] = signal_corpus_import
    state["stage"] = STAGE_AWAITING_EVIDENCE_CURATION
    state["imports"] = {
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
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "run-matching-adjudication",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    state["stage"] = STAGE_AWAITING_INVESTIGATION_REVIEW
    state["imports"] = {
        "task_review_received": True,
        "source_selection_roles_received": list(SOURCE_SELECTION_ROLES),
        "curation_roles_received": list(CURATION_ROLES),
        "data_readiness_roles_received": list(READINESS_ROLES),
        "matching_authorization_received": True,
        "matching_adjudication_received": True,
        "investigation_review_received": False,
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    return {"action": "run-matching-adjudication", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_recover_or_run_matching_adjudication(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    canonical_execution_path = matching_execution_path(run_dir, round_id)
    if canonical_execution_path.exists():
        try:
            payload = read_json(canonical_execution_path)
            ensure_matching_execution_matches(payload, run_dir=run_dir, round_id=round_id, source_path=canonical_execution_path)
            state["stage"] = STAGE_AWAITING_INVESTIGATION_REVIEW
            state["imports"] = {
                "task_review_received": True,
                "source_selection_roles_received": list(SOURCE_SELECTION_ROLES),
                "curation_roles_received": list(CURATION_ROLES),
                "data_readiness_roles_received": list(READINESS_ROLES),
                "matching_authorization_received": True,
                "matching_adjudication_received": True,
                "investigation_review_received": False,
                "report_roles_received": [],
                "decision_received": False,
            }
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
        [
            "python3",
            str(REPORTING_SCRIPT),
            "promote-all",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--allow-overwrite",
            "--pretty",
        ],
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
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "advance-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    new_round_id = latest_round_id(run_dir)
    state["current_round_id"] = new_round_id
    state["stage"] = STAGE_AWAITING_TASK_REVIEW
    state["imports"] = {
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
    save_state(run_dir, state)
    return {"action": "advance-round", "payload": payload, "state": build_status_payload(run_dir, state)}


def command_continue_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    stage = maybe_text(state.get("stage"))
    action_map = {
        STAGE_READY_PREPARE: ("prepare-round", continue_prepare_round),
        STAGE_READY_FETCH: ("execute-fetch-plan", lambda d, s: continue_recover_or_execute_fetch(d, s, args.timeout_seconds)),
        STAGE_READY_DATA_PLANE: ("run-data-plane", continue_recover_or_run_data_plane),
        STAGE_READY_MATCHING_ADJUDICATION: ("run-matching-adjudication", continue_recover_or_run_matching_adjudication),
        STAGE_READY_PROMOTE: ("promote-all", continue_promote),
        STAGE_READY_ADVANCE: ("advance-round", continue_advance_round),
    }
    action = action_map.get(stage)
    if action is None:
        raise ValueError(f"Current stage does not accept continue-run: {stage}")
    action_name, handler = action
    approved = ask_for_approval(
        f"About to run stage {action_name} for {maybe_text(state.get('current_round_id'))}.",
        assume_yes=args.yes,
    )
    if not approved:
        return {
            "approved": False,
            "stage": stage,
            "state": build_status_payload(run_dir, state),
        }
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        locked_state = load_state(run_dir)
        locked_stage = maybe_text(locked_state.get("stage"))
        if locked_stage != stage:
            raise ValueError(f"Stage changed during approval window: expected {stage}, found {locked_stage}. Rerun continue-run.")
        try:
            result = handler(run_dir, locked_state)
        except Exception as exc:  # noqa: BLE001
            failure = record_continue_run_failure(run_dir, locked_state, action_name, exc)
            save_state(run_dir, locked_state)
            return {
                "ok": False,
                "approved": True,
                "action": action_name,
                "recoverable": True,
                "error": maybe_text(failure.get("error")),
                "failure": failure,
                "state": build_status_payload(run_dir, locked_state),
            }
    result["approved"] = True
    result["ok"] = True
    return result


def command_import_task_review(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    validate_input_file("round-task", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_TASK_REVIEW:
            raise ValueError("import-task-review is only allowed while waiting for moderator task review.")
        return import_task_review_payload(
            run_dir=run_dir,
            state=state,
            payload=payload,
            source_path=input_path,
            **stage_import_context(),
        )


def command_import_source_selection(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    role = args.role
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_SOURCE_SELECTION:
            raise ValueError("import-source-selection is only allowed while waiting for expert source selection.")
        round_id = maybe_text(state.get("current_round_id"))
        original_payload = read_json(input_path)
        payload = normalize_agent_payload_for_schema(
            schema_kind="source-selection",
            payload=original_payload,
            run_dir=run_dir,
            round_id=round_id,
            role=role,
        )
        if payload != original_payload:
            write_json(input_path, payload, pretty=True)
        validate_input_file("source-selection", input_path)
        return import_source_selection_payload(
            run_dir=run_dir,
            state=state,
            role=role,
            payload=payload,
            source_path=input_path,
            source_selection_packet_builder=build_source_selection_packet,
            **stage_import_context(),
        )


def command_import_claim_curation(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_EVIDENCE_CURATION:
            raise ValueError("import-claim-curation is only allowed while waiting for expert evidence curation.")
        round_id = maybe_text(state.get("current_round_id"))
        original_payload = read_json(input_path)
        payload = normalize_agent_payload_for_schema(
            schema_kind="claim-curation",
            payload=original_payload,
            run_dir=run_dir,
            round_id=round_id,
            role="sociologist",
        )
        if payload != original_payload:
            write_json(input_path, payload, pretty=True)
        validate_input_file("claim-curation", input_path)
        return import_claim_curation_payload(
            run_dir=run_dir,
            state=state,
            payload=payload,
            source_path=input_path,
            materialize_curations=materialize_curations_for_supervisor,
            build_data_readiness_artifacts=build_data_readiness_artifacts_for_supervisor,
            **stage_import_context(),
        )


def command_import_observation_curation(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    validate_input_file("observation-curation", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_EVIDENCE_CURATION:
            raise ValueError("import-observation-curation is only allowed while waiting for expert evidence curation.")
        return import_observation_curation_payload(
            run_dir=run_dir,
            state=state,
            payload=payload,
            source_path=input_path,
            materialize_curations=materialize_curations_for_supervisor,
            build_data_readiness_artifacts=build_data_readiness_artifacts_for_supervisor,
            **stage_import_context(),
        )


def command_import_data_readiness(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    role = args.role
    validate_input_file("data-readiness-report", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_DATA_READINESS:
            raise ValueError("import-data-readiness is only allowed while waiting for expert data-readiness reports.")
        return import_data_readiness_payload(
            run_dir=run_dir,
            state=state,
            role=role,
            payload=payload,
            source_path=input_path,
            build_matching_authorization_artifacts=build_matching_authorization_artifacts_for_supervisor,
            **stage_import_context(),
        )


def command_import_matching_authorization(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    original_payload = read_json(input_path)
    payload = normalize_matching_authorization_payload(original_payload)
    if payload != original_payload:
        write_json(input_path, payload, pretty=True)
    validate_input_file("matching-authorization", input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_MATCHING_AUTHORIZATION:
            raise ValueError("import-matching-authorization is only allowed while waiting for moderator matching authorization.")
        return import_matching_authorization_payload(
            run_dir=run_dir,
            state=state,
            payload=payload,
            source_path=input_path,
            build_matching_adjudication_artifacts=build_matching_adjudication_artifacts_for_supervisor,
            build_decision_artifacts=build_decision_artifacts_for_supervisor,
            **stage_import_context(),
        )


def command_import_matching_adjudication(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_MATCHING_ADJUDICATION:
            raise ValueError("import-matching-adjudication is only allowed while waiting for moderator matching adjudication.")
        round_id = maybe_text(state.get("current_round_id"))
        original_payload = read_json(input_path)
        payload = normalize_agent_payload_for_schema(
            schema_kind="matching-adjudication",
            payload=original_payload,
            run_dir=run_dir,
            round_id=round_id,
            role="moderator",
        )
        if payload != original_payload:
            write_json(input_path, payload, pretty=True)
        validate_input_file("matching-adjudication", input_path)
        return import_matching_adjudication_payload(
            run_dir=run_dir,
            state=state,
            payload=payload,
            source_path=input_path,
            **stage_import_context(),
        )


def command_import_investigation_review(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_INVESTIGATION_REVIEW:
            raise ValueError("import-investigation-review is only allowed while waiting for moderator investigation review.")
        round_id = maybe_text(state.get("current_round_id"))
        original_payload = read_json(input_path)
        payload = normalize_agent_payload_for_schema(
            schema_kind="investigation-review",
            payload=original_payload,
            run_dir=run_dir,
            round_id=round_id,
            role="moderator",
        )
        if payload != original_payload:
            write_json(input_path, payload, pretty=True)
        validate_input_file("investigation-review", input_path)
        return import_investigation_review_payload(
            run_dir=run_dir,
            state=state,
            payload=payload,
            source_path=input_path,
            build_report_artifacts=build_report_artifacts_for_supervisor,
            **stage_import_context(),
        )


def command_import_report(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    role = args.role
    validate_input_file("expert-report", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) not in {STAGE_AWAITING_REPORTS, STAGE_AWAITING_DECISION}:
            raise ValueError("import-report is only allowed while waiting for expert reports.")
        return import_report_payload(
            run_dir=run_dir,
            state=state,
            role=role,
            payload=payload,
            source_path=input_path,
            build_decision_artifacts=build_decision_artifacts_for_supervisor,
            **stage_import_context(),
        )


def command_import_decision(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    validate_input_file("council-decision", input_path)
    payload = read_json(input_path)
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_AWAITING_DECISION:
            raise ValueError("import-decision is only allowed while waiting for the moderator decision.")
        return import_decision_payload(
            run_dir=run_dir,
            state=state,
            payload=payload,
            source_path=input_path,
            **stage_import_context(),
        )


def command_import_fetch_execution(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        state = load_state(run_dir)
        if maybe_text(state.get("stage")) != STAGE_READY_FETCH:
            raise ValueError("import-fetch-execution is only allowed while waiting for fetch execution.")
        round_id = maybe_text(state.get("current_round_id"))
        with exclusive_file_lock(fetch_lock_path(run_dir, round_id)):
            input_path = (
                Path(args.input).expanduser().resolve()
                if args.input
                else fetch_execution_path(run_dir, round_id).expanduser().resolve()
            )
            if not input_path.exists():
                raise ValueError(f"Fetch execution input file does not exist: {input_path}")
            payload = read_json(input_path)
            return import_fetch_execution_payload(
                run_dir=run_dir,
                state=state,
                payload=payload,
                source_path=input_path,
                **stage_import_context(),
            )


def command_run_agent_step(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    role, turn_kind, schema_kind = current_agent_turn(state=state, requested_role=args.role)
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    approved = ask_for_approval(
        f"About to run OpenClaw agent turn {turn_kind} for role={role} in {round_id}.",
        assume_yes=args.yes,
    )
    if not approved:
        return {
            "approved": False,
            "state": build_status_payload(run_dir, state),
        }

    with exclusive_file_lock(supervisor_state_lock_path(run_dir)):
        locked_state = load_state(run_dir)
        locked_round_id = maybe_text(locked_state.get("current_round_id"))
        locked_stage = maybe_text(locked_state.get("stage"))
        if locked_round_id != round_id or locked_stage != stage:
            raise ValueError(
                f"Supervisor state changed during approval window: expected round={round_id}, stage={stage}; "
                f"found round={locked_round_id}, stage={locked_stage}. Rerun run-agent-step."
            )
        locked_role, locked_turn_kind, locked_schema_kind = current_agent_turn(state=locked_state, requested_role=args.role)
        if (locked_role, locked_turn_kind, locked_schema_kind) != (role, turn_kind, schema_kind):
            raise ValueError(
                "Requested agent turn is no longer current. "
                f"Expected {(role, turn_kind, schema_kind)!r}, found {(locked_role, locked_turn_kind, locked_schema_kind)!r}."
            )

        message = build_agent_message(run_dir=run_dir, state=locked_state, role=role, turn_kind=turn_kind)
        result = run_openclaw_agent_turn(
            run_dir=run_dir,
            state=locked_state,
            role=role,
            turn_kind=turn_kind,
            schema_kind=schema_kind,
            message=message,
            timeout_seconds=args.timeout_seconds,
            thinking=args.thinking,
            normalize_payload=normalize_agent_payload_for_schema,
            validate_input_file=validate_input_file,
        )
        response_path = Path(result["response_json_path"]).resolve()
        payload = result["payload"]
        if schema_kind == "round-task":
            imported = import_task_review_payload(
                run_dir=run_dir,
                state=locked_state,
                payload=payload,
                source_path=response_path,
                **stage_import_context(),
            )
        elif schema_kind == "source-selection":
            imported = import_source_selection_payload(
                run_dir=run_dir,
                state=locked_state,
                role=role,
                payload=payload,
                source_path=response_path,
                source_selection_packet_builder=build_source_selection_packet,
                **stage_import_context(),
            )
        elif schema_kind == "claim-curation":
            imported = import_claim_curation_payload(
                run_dir=run_dir,
                state=locked_state,
                payload=payload,
                source_path=response_path,
                materialize_curations=materialize_curations_for_supervisor,
                build_data_readiness_artifacts=build_data_readiness_artifacts_for_supervisor,
                **stage_import_context(),
            )
        elif schema_kind == "observation-curation":
            imported = import_observation_curation_payload(
                run_dir=run_dir,
                state=locked_state,
                payload=payload,
                source_path=response_path,
                materialize_curations=materialize_curations_for_supervisor,
                build_data_readiness_artifacts=build_data_readiness_artifacts_for_supervisor,
                **stage_import_context(),
            )
        elif schema_kind == "data-readiness-report":
            imported = import_data_readiness_payload(
                run_dir=run_dir,
                state=locked_state,
                role=role,
                payload=payload,
                source_path=response_path,
                build_matching_authorization_artifacts=build_matching_authorization_artifacts_for_supervisor,
                **stage_import_context(),
            )
        elif schema_kind == "matching-authorization":
            imported = import_matching_authorization_payload(
                run_dir=run_dir,
                state=locked_state,
                payload=payload,
                source_path=response_path,
                build_matching_adjudication_artifacts=build_matching_adjudication_artifacts_for_supervisor,
                build_decision_artifacts=build_decision_artifacts_for_supervisor,
                **stage_import_context(),
            )
        elif schema_kind == "matching-adjudication":
            imported = import_matching_adjudication_payload(
                run_dir=run_dir,
                state=locked_state,
                payload=payload,
                source_path=response_path,
                **stage_import_context(),
            )
        elif schema_kind == "investigation-review":
            imported = import_investigation_review_payload(
                run_dir=run_dir,
                state=locked_state,
                payload=payload,
                source_path=response_path,
                build_report_artifacts=build_report_artifacts_for_supervisor,
                **stage_import_context(),
            )
        elif schema_kind == "expert-report":
            imported = import_report_payload(
                run_dir=run_dir,
                state=locked_state,
                role=role,
                payload=payload,
                source_path=response_path,
                build_decision_artifacts=build_decision_artifacts_for_supervisor,
                **stage_import_context(),
            )
        elif schema_kind == "council-decision":
            imported = import_decision_payload(
                run_dir=run_dir,
                state=locked_state,
                payload=payload,
                source_path=response_path,
                **stage_import_context(),
            )
        else:
            raise ValueError(f"Unsupported schema kind: {schema_kind}")

    return {
        "approved": True,
        "agent_turn": {
            "agent_id": result["agent_id"],
            "role": role,
            "turn_kind": turn_kind,
            "response_json_path": result["response_json_path"],
            "stdout_path": result["stdout_path"],
            "stderr_path": result["stderr_path"],
        },
        "import_result": imported,
    }


def command_provision_openclaw_agents(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    result = provision_openclaw_agents_for_run(
        run_dir,
        state=state,
        workspace_root_text=args.workspace_root,
        skills_root_text=args.skills_root,
        assume_yes=args.yes,
        approval_callback=ask_for_approval,
        mission=read_json(mission_path(run_dir)),
    )
    if not result["approved"]:
        return {
            "approved": False,
            "state": build_status_payload(run_dir, state),
        }
    save_state(run_dir, state)
    return {
        "approved": True,
        "workspace_root": result["workspace_root"],
        "created_agents": result["created_agents"],
        "state": build_status_payload(run_dir, state),
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_supervisor_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-run": command_init_run,
        "provision-openclaw-agents": command_provision_openclaw_agents,
        "status": command_status,
        "summarize-run": command_summarize_run,
        "continue-run": command_continue_run,
        "run-agent-step": command_run_agent_step,
        "import-task-review": command_import_task_review,
        "import-source-selection": command_import_source_selection,
        "import-claim-curation": command_import_claim_curation,
        "import-observation-curation": command_import_observation_curation,
        "import-data-readiness": command_import_data_readiness,
        "import-matching-authorization": command_import_matching_authorization,
        "import-matching-adjudication": command_import_matching_adjudication,
        "import-investigation-review": command_import_investigation_review,
        "import-report": command_import_report,
        "import-decision": command_import_decision,
        "import-fetch-execution": command_import_fetch_execution,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(pretty_json({"ok": False, "error": str(exc)}, pretty=True))
        return 1
    print(pretty_json(payload, pretty=bool(getattr(args, "pretty", False))))
    if isinstance(payload, dict) and payload.get("ok") is False:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
