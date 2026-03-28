"""Execution-stage orchestration helpers for runtime workflows."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import (
    exclusive_file_lock,
    file_sha256,
    load_json_if_exists,
    read_json,
    utc_now_iso,
    write_json,
)
from eco_council_runtime.adapters.run_paths import load_mission
from eco_council_runtime.application import orchestration_prepare
from eco_council_runtime.application.orchestration.fetch_plan_builder import ensure_fetch_plan_inputs_match
from eco_council_runtime.application.orchestration.governance import ENVIRONMENT_SOURCES, PUBLIC_SOURCES
from eco_council_runtime.application.orchestration.step_synthesis import (
    REPO_DIR,
    contract_argv,
    contract_command,
    normalize_argv,
    normalize_command,
    orchestrate_command,
    reporting_argv,
    reporting_command,
    shell_join,
    skill_workdir,
)
from eco_council_runtime.controller.audit_chain import record_fetch_phase_receipt, record_normalize_phase_receipt
from eco_council_runtime.controller.paths import (
    claim_curation_draft_path,
    claim_curation_prompt_path,
    data_plane_execution_path,
    data_readiness_draft_path,
    data_readiness_prompt_path,
    decision_draft_path,
    decision_prompt_path,
    evidence_adjudication_path,
    fetch_execution_path,
    fetch_lock_path,
    fetch_plan_path,
    investigation_review_draft_path,
    investigation_review_path,
    investigation_review_prompt_path,
    matching_adjudication_path,
    matching_adjudication_prompt_path,
    matching_authorization_path,
    matching_authorization_prompt_path,
    matching_candidate_set_path,
    matching_execution_path,
    matching_result_path,
    observation_curation_draft_path,
    observation_curation_prompt_path,
    report_draft_path,
    report_prompt_path,
    reporting_handoff_path,
    round_dir,
)
from eco_council_runtime.domain.contract_bridge import effective_matching_authorization
from eco_council_runtime.domain.text import maybe_text, truncate_text

ENV_ASSIGNMENT_PATTERN = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")

resolve_round_id = orchestration_prepare.resolve_round_id
load_tasks = orchestration_prepare.load_tasks
tasks_for_role = orchestration_prepare.tasks_for_role
ensure_object = orchestration_prepare.ensure_object
ensure_object_list = orchestration_prepare.ensure_object_list


def strip_inline_comment(text: str) -> str:
    chars: list[str] = []
    in_single = False
    in_double = False
    escape = False
    for char in text:
        if escape:
            chars.append(char)
            escape = False
            continue
        if char == "\\" and not in_single:
            chars.append(char)
            escape = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            break
        chars.append(char)
    return "".join(chars).strip()


def parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise ValueError(f"Environment file does not exist: {path}")
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        clean = strip_inline_comment(line)
        if not clean:
            continue
        match = ENV_ASSIGNMENT_PATTERN.match(clean)
        if match is None:
            continue
        key, raw_value = match.groups()
        value = raw_value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]
        elif value.startswith("'") and value.endswith("'") and len(value) >= 2:
            value = value[1:-1]
        env[key] = value
    return env


def run_json_cli(argv: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        detail = stderr or stdout or f"exit={completed.returncode}"
        raise RuntimeError(f"Command failed: {shell_join(argv)} :: {detail}")
    stdout_text = completed.stdout.strip()
    if not stdout_text:
        raise RuntimeError(f"Command produced no JSON output: {shell_join(argv)}")
    try:
        payload = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Command did not emit valid JSON: {shell_join(argv)}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Command JSON output must be an object: {shell_join(argv)}")
    return payload


def materialize_json_artifact_from_stdout(*, artifact_path: Path, stdout_path: Path) -> bool:
    if not stdout_path.exists():
        return False
    try:
        payload = read_json(stdout_path)
    except Exception:
        return False
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(artifact_path, payload, pretty=True)
    return artifact_path.exists()


def validate_json_artifact_if_applicable(artifact_path: Path | None) -> str:
    if artifact_path is None:
        return ""
    if artifact_path.suffix.casefold() != ".json":
        return ""
    try:
        read_json(artifact_path)
    except Exception as exc:  # noqa: BLE001
        return str(exc)
    return ""


def build_fetch_execution_payload(
    *,
    run_dir: Path,
    round_id: str,
    plan_path: Path,
    plan_sha256: str,
    step_count: int,
    statuses: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "updated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "plan_path": str(plan_path),
        "plan_sha256": plan_sha256,
        "step_count": step_count,
        "completed_count": sum(1 for status in statuses if status.get("status") == "completed"),
        "failed_count": sum(1 for status in statuses if status.get("status") == "failed"),
        "statuses": statuses,
    }


def write_fetch_execution_snapshot(
    *,
    execution_path: Path,
    run_dir: Path,
    round_id: str,
    plan_path: Path,
    plan_sha256: str,
    step_count: int,
    statuses: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = build_fetch_execution_payload(
        run_dir=run_dir,
        round_id=round_id,
        plan_path=plan_path,
        plan_sha256=plan_sha256,
        step_count=step_count,
        statuses=statuses,
    )
    write_json(execution_path, payload, pretty=True)
    payload["execution_path"] = str(execution_path)
    return payload


def build_data_plane_execution_payload(
    *,
    run_dir: Path,
    round_id: str,
    public_inputs: list[str],
    environment_inputs: list[str],
    step_count: int,
    statuses: list[dict[str, Any]],
    reporting_handoff_path_value: Path | None,
) -> dict[str, Any]:
    payload = {
        "updated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "step_count": step_count,
        "completed_count": sum(1 for status in statuses if status.get("status") == "completed"),
        "failed_count": sum(1 for status in statuses if status.get("status") == "failed"),
        "public_inputs": public_inputs,
        "environment_inputs": environment_inputs,
        "statuses": statuses,
        "reporting_handoff_path": str(reporting_handoff_path_value) if reporting_handoff_path_value is not None else "",
    }
    first_failed = next((status for status in statuses if status.get("status") == "failed"), None)
    if isinstance(first_failed, dict):
        payload["failed_step_id"] = maybe_text(first_failed.get("step_id"))
    return payload


def write_data_plane_execution_snapshot(
    *,
    execution_path: Path,
    run_dir: Path,
    round_id: str,
    public_inputs: list[str],
    environment_inputs: list[str],
    step_count: int,
    statuses: list[dict[str, Any]],
    reporting_handoff_path_value: Path | None,
) -> dict[str, Any]:
    payload = build_data_plane_execution_payload(
        run_dir=run_dir,
        round_id=round_id,
        public_inputs=public_inputs,
        environment_inputs=environment_inputs,
        step_count=step_count,
        statuses=statuses,
        reporting_handoff_path_value=reporting_handoff_path_value,
    )
    write_json(execution_path, payload, pretty=True)
    payload["execution_path"] = str(execution_path)
    return payload


def ensure_ok_envelope(payload: dict[str, Any], label: str) -> dict[str, Any]:
    if payload.get("ok") is False:
        raise RuntimeError(f"{label} returned ok=false: {payload}")
    result = payload.get("payload")
    if isinstance(result, dict):
        return result
    return payload


def execute_fetch_plan(
    *,
    run_dir: Path,
    round_id: str,
    continue_on_error: bool,
    skip_existing: bool,
    timeout_seconds: int,
) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    with exclusive_file_lock(fetch_lock_path(run_path, current_round_id)):
        plan_path = fetch_plan_path(run_path, current_round_id)
        plan = ensure_object(read_json(plan_path), f"{plan_path}")
        ensure_fetch_plan_inputs_match(run_dir=run_path, round_id=current_round_id, plan=plan)
        steps = ensure_object_list(plan.get("steps"), f"{plan_path}.steps")
        execution_path = fetch_execution_path(run_path, current_round_id)
        plan_sha256 = file_sha256(plan_path)

        statuses: list[dict[str, Any]] = []
        succeeded: set[str] = set()

        def snapshot() -> dict[str, Any]:
            return write_fetch_execution_snapshot(
                execution_path=execution_path,
                run_dir=run_path,
                round_id=current_round_id,
                plan_path=plan_path,
                plan_sha256=plan_sha256,
                step_count=len(steps),
                statuses=statuses,
            )

        snapshot()
        for step in steps:
            step_id = maybe_text(step.get("step_id"))
            role = maybe_text(step.get("role"))
            artifact_text = str(step.get("artifact_path") or "").strip()
            artifact_path = Path(artifact_text).expanduser().resolve() if artifact_text else None
            stdout_path = Path(maybe_text(step.get("stdout_path"))).expanduser().resolve()
            stderr_path = Path(maybe_text(step.get("stderr_path"))).expanduser().resolve()
            cwd = Path(maybe_text(step.get("cwd")) or str(REPO_DIR)).expanduser().resolve()
            depends_on = [maybe_text(item) for item in step.get("depends_on", []) if maybe_text(item)]
            artifact_capture = maybe_text(step.get("artifact_capture"))
            raw_command = step.get("command")
            if not isinstance(raw_command, str) or not raw_command.strip():
                raise ValueError(f"Fetch step {step_id} is missing a shell command.")
            command = raw_command.strip()

            if any(dep not in succeeded for dep in depends_on):
                status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "skipped",
                    "reason": f"Unmet dependencies: {depends_on}",
                }
                statuses.append(status)
                snapshot()
                if not continue_on_error:
                    break
                continue

            existing_artifact_invalid = ""
            if artifact_path is not None and artifact_path.exists():
                existing_artifact_invalid = validate_json_artifact_if_applicable(artifact_path)

            if skip_existing and artifact_path is not None and artifact_path.exists() and not existing_artifact_invalid:
                statuses.append(
                    {
                        "step_id": step_id,
                        "role": role,
                        "source_skill": maybe_text(step.get("source_skill")),
                        "status": "skipped",
                        "reason": "artifact_exists",
                        "artifact_path": str(artifact_path),
                    }
                )
                succeeded.add(step_id)
                snapshot()
                continue

            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
                try:
                    completed = subprocess.run(
                        ["/bin/bash", "-lc", command],
                        cwd=str(cwd),
                        check=False,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                        text=True,
                        timeout=timeout_seconds,
                    )
                    returncode = completed.returncode
                    timed_out = False
                except subprocess.TimeoutExpired:
                    returncode = 124
                    timed_out = True
            artifact_materialized = False
            if returncode == 0 and artifact_path is not None and artifact_capture == "stdout-json":
                artifact_materialized = materialize_json_artifact_from_stdout(
                    artifact_path=artifact_path,
                    stdout_path=stdout_path,
                )
            artifact_missing = artifact_path is not None and not artifact_path.exists()
            artifact_invalid_json = validate_json_artifact_if_applicable(artifact_path) if returncode == 0 and not artifact_missing else ""
            if returncode == 0 and not artifact_missing and not artifact_invalid_json:
                succeeded.add(step_id)
                completed_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "completed",
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                }
                if artifact_path is not None:
                    completed_status["artifact_path"] = str(artifact_path)
                if artifact_materialized:
                    completed_status["artifact_materialized"] = True
                statuses.append(completed_status)
                snapshot()
                continue

            if artifact_missing:
                failure_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "failed",
                    "reason": "artifact_missing",
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "returncode": returncode,
                    "timed_out": timed_out,
                }
                if artifact_path is not None:
                    failure_status["artifact_path"] = str(artifact_path)
                statuses.append(failure_status)
                snapshot()
                if not continue_on_error:
                    break
                continue

            if artifact_invalid_json:
                failure_status = {
                    "step_id": step_id,
                    "role": role,
                    "source_skill": maybe_text(step.get("source_skill")),
                    "status": "failed",
                    "reason": "artifact_invalid_json",
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "returncode": returncode,
                    "timed_out": timed_out,
                    "artifact_validation_error": artifact_invalid_json,
                }
                if artifact_path is not None:
                    failure_status["artifact_path"] = str(artifact_path)
                statuses.append(failure_status)
                snapshot()
                if not continue_on_error:
                    break
                continue

            failure_status = {
                "step_id": step_id,
                "role": role,
                "source_skill": maybe_text(step.get("source_skill")),
                "status": "failed",
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "returncode": returncode,
                "timed_out": timed_out,
            }
            if artifact_path is not None:
                failure_status["artifact_path"] = str(artifact_path)
            statuses.append(failure_status)
            snapshot()
            if not continue_on_error:
                break

        final_payload = snapshot()
        record_fetch_phase_receipt(run_dir=run_path, round_id=current_round_id, payload=final_payload)
        return final_payload


def fetch_status_role(status: dict[str, Any]) -> str:
    role = maybe_text(status.get("role")) or maybe_text(status.get("assigned_role"))
    if role:
        return role
    step_id = maybe_text(status.get("step_id"))
    match = re.match(r"^step-([a-z]+)-", step_id)
    if match is None:
        return ""
    return maybe_text(match.group(1))


def fetch_status_has_usable_artifact(status: dict[str, Any]) -> bool:
    state = maybe_text(status.get("status"))
    if state == "completed":
        return True
    return state == "skipped" and maybe_text(status.get("reason")) == "artifact_exists"


def usable_fetch_artifacts(run_dir: Path, round_id: str, *, role: str) -> tuple[dict[str, Path], bool]:
    payload = load_json_if_exists(fetch_execution_path(run_dir, round_id))
    if not isinstance(payload, dict):
        return {}, False
    statuses = payload.get("statuses")
    if not isinstance(statuses, list):
        return {}, False
    artifacts: dict[str, Path] = {}
    for status in statuses:
        if not isinstance(status, dict):
            continue
        if fetch_status_role(status) != role or not fetch_status_has_usable_artifact(status):
            continue
        source_skill = maybe_text(status.get("source_skill"))
        artifact_text = maybe_text(status.get("artifact_path"))
        if not source_skill or not artifact_text:
            continue
        artifacts[source_skill] = Path(artifact_text).expanduser().resolve()
    return artifacts, True


def discover_normalize_inputs(run_dir: Path, round_id: str, *, role: str, sources: tuple[str, ...]) -> list[str]:
    input_specs: list[str] = []
    usable_artifacts, _has_execution_record = usable_fetch_artifacts(run_dir, round_id, role=role)
    for source_skill in sources:
        artifact_path = usable_artifacts.get(source_skill)
        if artifact_path is None:
            continue
        if artifact_path.exists():
            input_specs.append(f"{source_skill}={artifact_path}")
    return input_specs


def build_reporting_handoff(*, run_dir: Path, round_id: str) -> Path:
    def existing_path(path: Path) -> str:
        return str(path) if path.exists() else ""

    materialize_curations_command = normalize_command(
        "materialize-curations",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    build_data_readiness_packets_command = reporting_command(
        "build-data-readiness-packets",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    promote_all_command = reporting_command("promote-all", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty")
    validate_bundle_command = contract_command("validate-bundle", "--run-dir", str(run_dir), "--pretty")
    build_decision_packet_command = reporting_command(
        "build-decision-packet",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--prefer-draft-reports",
        "--pretty",
    )
    build_matching_authorization_packet_command = reporting_command(
        "build-matching-authorization-packet",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    prepare_matching_adjudication_command = normalize_command(
        "prepare-matching-adjudication",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    build_matching_adjudication_packet_command = reporting_command(
        "build-matching-adjudication-packet",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    build_investigation_review_packet_command = reporting_command(
        "build-investigation-review-packet",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    build_report_packets_command = reporting_command(
        "build-report-packets",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    render_openclaw_prompts_command = reporting_command(
        "render-openclaw-prompts",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    promote_matching_authorization_command = reporting_command(
        "promote-matching-authorization-draft",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    promote_matching_adjudication_command = reporting_command(
        "promote-matching-adjudication-draft",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    promote_investigation_review_command = reporting_command(
        "promote-investigation-review-draft",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    run_matching_adjudication_command = orchestrate_command(
        "run-matching-adjudication",
        "--run-dir",
        str(run_dir),
        "--round-id",
        round_id,
        "--pretty",
    )
    advance_round_command = orchestrate_command("advance-round", "--run-dir", str(run_dir), "--round-id", round_id, "--pretty")
    handoff = {
        "handoff_kind": "eco-council-reporting-handoff",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "curation_prompt_paths": {
            "sociologist": existing_path(claim_curation_prompt_path(run_dir, round_id)),
            "environmentalist": existing_path(observation_curation_prompt_path(run_dir, round_id)),
        },
        "data_readiness_prompt_paths": {
            "sociologist": existing_path(data_readiness_prompt_path(run_dir, round_id, "sociologist")),
            "environmentalist": existing_path(data_readiness_prompt_path(run_dir, round_id, "environmentalist")),
        },
        "matching_authorization_prompt_path": existing_path(matching_authorization_prompt_path(run_dir, round_id)),
        "matching_adjudication_prompt_path": existing_path(matching_adjudication_prompt_path(run_dir, round_id)),
        "investigation_review_prompt_path": existing_path(investigation_review_prompt_path(run_dir, round_id)),
        "expert_report_prompt_paths": {
            "sociologist": existing_path(report_prompt_path(run_dir, round_id, "sociologist")),
            "environmentalist": existing_path(report_prompt_path(run_dir, round_id, "environmentalist")),
        },
        "decision_prompt_path": existing_path(decision_prompt_path(run_dir, round_id)),
        "canonical_paths": {
            "matching_authorization": existing_path(matching_authorization_path(run_dir, round_id)),
            "matching_adjudication": existing_path(matching_adjudication_path(run_dir, round_id)),
            "investigation_review": existing_path(investigation_review_path(run_dir, round_id)),
            "matching_result": existing_path(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": existing_path(evidence_adjudication_path(run_dir, round_id)),
        },
        "draft_paths": {
            "sociologist_claim_curation": existing_path(claim_curation_draft_path(run_dir, round_id)),
            "environmentalist_observation_curation": existing_path(observation_curation_draft_path(run_dir, round_id)),
            "sociologist_data_readiness": existing_path(data_readiness_draft_path(run_dir, round_id, "sociologist")),
            "environmentalist_data_readiness": existing_path(data_readiness_draft_path(run_dir, round_id, "environmentalist")),
            "matching_authorization": existing_path(round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_authorization_draft.json"),
            "matching_adjudication": existing_path(round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_adjudication_draft.json"),
            "investigation_review": existing_path(investigation_review_draft_path(run_dir, round_id)),
            "sociologist_report": existing_path(report_draft_path(run_dir, round_id, "sociologist")),
            "environmentalist_report": existing_path(report_draft_path(run_dir, round_id, "environmentalist")),
            "moderator_decision": existing_path(decision_draft_path(run_dir, round_id)),
        },
        "matching_candidate_set_path": existing_path(matching_candidate_set_path(run_dir, round_id)),
        "promotion_commands": {
            "materialize_curations": materialize_curations_command,
            "build_data_readiness_packets": build_data_readiness_packets_command,
            "build_matching_authorization_packet": build_matching_authorization_packet_command,
            "prepare_matching_adjudication": prepare_matching_adjudication_command,
            "build_matching_adjudication_packet": build_matching_adjudication_packet_command,
            "build_investigation_review_packet": build_investigation_review_packet_command,
            "build_report_packets": build_report_packets_command,
            "render_openclaw_prompts": render_openclaw_prompts_command,
            "promote_matching_authorization": promote_matching_authorization_command,
            "promote_matching_adjudication": promote_matching_adjudication_command,
            "promote_investigation_review": promote_investigation_review_command,
            "promote_all": promote_all_command,
            "build_decision_packet": build_decision_packet_command,
            "run_matching_adjudication": run_matching_adjudication_command,
            "validate_bundle": validate_bundle_command,
            "advance_round": advance_round_command,
        },
    }
    output_path = reporting_handoff_path(run_dir, round_id)
    write_json(output_path, handoff, pretty=True)
    return output_path


def run_data_plane_json_step(
    *,
    step_id: str,
    label: str,
    argv: list[str],
    cwd: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    started_at_utc = utc_now_iso()
    try:
        result = ensure_ok_envelope(run_json_cli(argv, cwd=cwd), label)
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "completed",
                "command": shell_join(argv),
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "result": result,
            },
            result,
        )
    except Exception as exc:  # noqa: BLE001
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "failed",
                "command": shell_join(argv),
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "error": truncate_text(str(exc), 4000),
            },
            None,
        )


def run_data_plane_callable_step(
    *,
    step_id: str,
    label: str,
    callback: Any,
) -> tuple[dict[str, Any], Any | None]:
    started_at_utc = utc_now_iso()
    try:
        result = callback()
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "completed",
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "result": result,
            },
            result,
        )
    except Exception as exc:  # noqa: BLE001
        return (
            {
                "step_id": step_id,
                "label": label,
                "status": "failed",
                "started_at_utc": started_at_utc,
                "finished_at_utc": utc_now_iso(),
                "error": truncate_text(str(exc), 4000),
            },
            None,
        )


def run_data_plane(*, run_dir: Path, round_id: str) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    public_inputs = discover_normalize_inputs(run_path, current_round_id, role="sociologist", sources=PUBLIC_SOURCES)
    environment_inputs = discover_normalize_inputs(run_path, current_round_id, role="environmentalist", sources=ENVIRONMENT_SOURCES)
    execution_path = data_plane_execution_path(run_path, current_round_id)
    reporting_handoff: Path | None = None
    statuses: list[dict[str, Any]] = []
    step_count = 8

    def snapshot() -> dict[str, Any]:
        return write_data_plane_execution_snapshot(
            execution_path=execution_path,
            run_dir=run_path,
            round_id=current_round_id,
            public_inputs=public_inputs,
            environment_inputs=environment_inputs,
            step_count=step_count,
            statuses=statuses,
            reporting_handoff_path_value=reporting_handoff,
        )

    snapshot()

    def append_status_or_raise(status: dict[str, Any], result: Any | None) -> Any:
        statuses.append(status)
        snapshot()
        if result is None:
            raise RuntimeError(
                f"Data plane failed at {maybe_text(status.get('step_id'))}. "
                f"Inspect {execution_path}. {maybe_text(status.get('error'))}"
            )
        return result

    init_status, init_payload = run_data_plane_json_step(
        step_id="normalize-init-run",
        label="normalize init-run",
        argv=normalize_argv("init-run", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    init_payload = append_status_or_raise(init_status, init_payload)

    normalize_public_cmd = normalize_argv("normalize-public", "--run-dir", str(run_path), "--round-id", current_round_id)
    for input_spec in public_inputs:
        normalize_public_cmd.extend(["--input", input_spec])
    public_status, public_payload = run_data_plane_json_step(
        step_id="normalize-public",
        label="normalize public",
        argv=normalize_public_cmd,
    )
    public_payload = append_status_or_raise(public_status, public_payload)

    normalize_environment_cmd = normalize_argv(
        "normalize-environment",
        "--run-dir",
        str(run_path),
        "--round-id",
        current_round_id,
    )
    for input_spec in environment_inputs:
        normalize_environment_cmd.extend(["--input", input_spec])
    environment_status, environment_payload = run_data_plane_json_step(
        step_id="normalize-environment",
        label="normalize environment",
        argv=normalize_environment_cmd,
    )
    environment_payload = append_status_or_raise(environment_status, environment_payload)

    context_status, context_payload = run_data_plane_json_step(
        step_id="build-round-context",
        label="build round context",
        argv=normalize_argv("build-round-context", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    context_payload = append_status_or_raise(context_status, context_payload)

    curation_status, curation_payload = run_data_plane_json_step(
        step_id="reporting-build-curation-packets",
        label="reporting build curation packets",
        argv=reporting_argv("build-curation-packets", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    curation_payload = append_status_or_raise(curation_status, curation_payload)

    prompt_status, prompt_payload = run_data_plane_json_step(
        step_id="render-openclaw-prompts",
        label="render openclaw prompts",
        argv=reporting_argv("render-openclaw-prompts", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    prompt_payload = append_status_or_raise(prompt_status, prompt_payload)

    bundle_status, bundle_payload = run_data_plane_json_step(
        step_id="validate-bundle",
        label="validate bundle",
        argv=contract_argv("validate-bundle", "--run-dir", str(run_path)),
    )
    bundle_payload = append_status_or_raise(bundle_status, bundle_payload)

    handoff_status, handoff_payload = run_data_plane_callable_step(
        step_id="build-reporting-handoff",
        label="build reporting handoff",
        callback=lambda: {"reporting_handoff_path": str(build_reporting_handoff(run_dir=run_path, round_id=current_round_id))},
    )
    handoff_payload = append_status_or_raise(handoff_status, handoff_payload)
    reporting_handoff = Path(maybe_text(handoff_payload.get("reporting_handoff_path"))).expanduser().resolve()
    execution_payload = snapshot()
    record_normalize_phase_receipt(run_dir=run_path, round_id=current_round_id, payload=execution_payload)

    return {
        "run_dir": str(run_path),
        "round_id": current_round_id,
        "public_inputs": public_inputs,
        "environment_inputs": environment_inputs,
        "normalize_init": init_payload,
        "normalize_public": public_payload,
        "normalize_environment": environment_payload,
        "build_context": context_payload,
        "curation": curation_payload,
        "prompt_render": prompt_payload,
        "bundle_validation": bundle_payload,
        "reporting_handoff_path": str(reporting_handoff),
        "execution_path": str(execution_path),
    }


def run_matching_adjudication(*, run_dir: Path, round_id: str) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    mission = load_mission(run_path)
    authorization = load_json_if_exists(matching_authorization_path(run_path, current_round_id))
    if not isinstance(authorization, dict):
        raise ValueError(
            f"Matching/adjudication requires canonical moderator matching_authorization.json: "
            f"{matching_authorization_path(run_path, current_round_id)}"
        )
    authorization = effective_matching_authorization(
        mission=mission,
        round_id=current_round_id,
        authorization=authorization,
    )
    if maybe_text(authorization.get("authorization_status")) != "authorized":
        raise ValueError(
            "Matching/adjudication is only allowed after moderator authorization_status=authorized. "
            f"Current status={authorization.get('authorization_status')!r}."
        )
    adjudication_input = matching_adjudication_path(run_path, current_round_id)
    if not adjudication_input.exists():
        raise ValueError(
            "Matching/adjudication materialization requires canonical moderator matching_adjudication.json: "
            f"{adjudication_input}"
        )
    execution_path = matching_execution_path(run_path, current_round_id)
    reporting_handoff: Path | None = None
    statuses: list[dict[str, Any]] = []
    step_count = 8

    def snapshot() -> dict[str, Any]:
        return write_data_plane_execution_snapshot(
            execution_path=execution_path,
            run_dir=run_path,
            round_id=current_round_id,
            public_inputs=[],
            environment_inputs=[],
            step_count=step_count,
            statuses=statuses,
            reporting_handoff_path_value=reporting_handoff,
        )

    snapshot()

    def append_status_or_raise(status: dict[str, Any], result: Any | None) -> Any:
        statuses.append(status)
        snapshot()
        if result is None:
            raise RuntimeError(
                f"Matching/adjudication failed at {maybe_text(status.get('step_id'))}. "
                f"Inspect {execution_path}. {maybe_text(status.get('error'))}"
            )
        return result

    evidence_status, evidence_payload = run_data_plane_json_step(
        step_id="apply-matching-adjudication",
        label="apply matching adjudication",
        argv=normalize_argv("apply-matching-adjudication", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    evidence_payload = append_status_or_raise(evidence_status, evidence_payload)

    context_status, context_payload = run_data_plane_json_step(
        step_id="build-round-context",
        label="build round context",
        argv=normalize_argv("build-round-context", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    context_payload = append_status_or_raise(context_status, context_payload)

    reporting_status, reporting_payload = run_data_plane_json_step(
        step_id="reporting-build-investigation-review-packet",
        label="reporting build investigation review packet",
        argv=reporting_argv("build-investigation-review-packet", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    reporting_payload = append_status_or_raise(reporting_status, reporting_payload)

    promote_review_status, promote_review_payload = run_data_plane_json_step(
        step_id="reporting-promote-investigation-review-draft",
        label="promote investigation review draft",
        argv=reporting_argv("promote-investigation-review-draft", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    promote_review_payload = append_status_or_raise(promote_review_status, promote_review_payload)

    report_status, report_payload = run_data_plane_json_step(
        step_id="reporting-build-report-packets",
        label="reporting build report packets",
        argv=reporting_argv("build-report-packets", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    report_payload = append_status_or_raise(report_status, report_payload)

    prompt_status, prompt_payload = run_data_plane_json_step(
        step_id="render-openclaw-prompts",
        label="render openclaw prompts",
        argv=reporting_argv("render-openclaw-prompts", "--run-dir", str(run_path), "--round-id", current_round_id),
    )
    prompt_payload = append_status_or_raise(prompt_status, prompt_payload)

    bundle_status, bundle_payload = run_data_plane_json_step(
        step_id="validate-bundle",
        label="validate bundle",
        argv=contract_argv("validate-bundle", "--run-dir", str(run_path)),
    )
    bundle_payload = append_status_or_raise(bundle_status, bundle_payload)

    handoff_status, handoff_payload = run_data_plane_callable_step(
        step_id="build-reporting-handoff",
        label="build reporting handoff",
        callback=lambda: {"reporting_handoff_path": str(build_reporting_handoff(run_dir=run_path, round_id=current_round_id))},
    )
    handoff_payload = append_status_or_raise(handoff_status, handoff_payload)
    reporting_handoff = Path(maybe_text(handoff_payload.get("reporting_handoff_path"))).expanduser().resolve()
    snapshot()

    return {
        "run_dir": str(run_path),
        "round_id": current_round_id,
        "apply_matching_adjudication": evidence_payload,
        "build_context": context_payload,
        "investigation_review": reporting_payload,
        "investigation_review_promotion": promote_review_payload,
        "report_packets": report_payload,
        "prompt_render": prompt_payload,
        "bundle_validation": bundle_payload,
        "reporting_handoff_path": str(reporting_handoff),
        "execution_path": str(execution_path),
    }
