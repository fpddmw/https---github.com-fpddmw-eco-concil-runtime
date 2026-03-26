"""Validation helpers for fetch, data-plane, and matching execution artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.controller.common import maybe_int
from eco_council_runtime.controller.constants import DATA_PLANE_STEP_IDS, MATCHING_ADJUDICATION_STEP_IDS
from eco_council_runtime.controller.io import file_sha256, load_json_if_exists, maybe_text, read_json
from eco_council_runtime.controller.paths import (
    claim_curation_packet_path,
    claim_curation_prompt_path,
    evidence_adjudication_path,
    fetch_execution_path,
    fetch_plan_path,
    load_mission,
    matching_adjudication_path,
    matching_authorization_path,
    matching_result_path,
    observation_curation_packet_path,
    observation_curation_prompt_path,
    report_packet_path,
    report_prompt_path,
    require_round_id,
)
from eco_council_runtime.controller.policy import effective_matching_authorization_payload


def fetch_status_has_usable_artifact(status: dict[str, Any]) -> bool:
    status_value = maybe_text(status.get("status"))
    if status_value == "completed":
        return True
    return status_value == "skipped" and maybe_text(status.get("reason")) == "artifact_exists"


def fetch_plan_steps(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    payload = read_json(fetch_plan_path(run_dir, round_id))
    if not isinstance(payload, dict):
        raise ValueError("Fetch plan must be a JSON object.")
    steps = payload.get("steps")
    if not isinstance(steps, list):
        raise ValueError("Fetch plan must include a steps list.")
    if not all(isinstance(step, dict) for step in steps):
        raise ValueError("Fetch plan steps must be JSON objects.")
    return [step for step in steps if isinstance(step, dict)]


def resolved_required_path(value: Any, *, label: str) -> Path:
    text = maybe_text(value)
    if not text:
        raise ValueError(f"{label} is missing.")
    return Path(text).expanduser().resolve()


def optional_resolved_path(value: Any) -> Path | None:
    text = maybe_text(value)
    if not text:
        return None
    return Path(text).expanduser().resolve()


def validate_json_artifact_if_applicable(path: Path) -> None:
    if path.suffix.casefold() != ".json":
        return
    try:
        read_json(path)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Artifact is not valid JSON: {path} ({exc})") from exc


def ensure_fetch_execution_matches(payload: Any, *, run_dir: Path, round_id: str, source_path: Path) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Fetch execution payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_round_id = maybe_text(payload.get("round_id"))
    if not payload_round_id:
        raise ValueError("Fetch execution payload must include round_id.")
    if require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Fetch execution round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    payload_run_dir = maybe_text(payload.get("run_dir"))
    expected_run_dir = run_dir.expanduser().resolve()
    if payload_run_dir:
        found_run_dir = Path(payload_run_dir).expanduser().resolve()
        if found_run_dir != expected_run_dir:
            raise ValueError(f"Fetch execution run_dir mismatch: expected {expected_run_dir}, got {found_run_dir}")
    payload_plan_path = maybe_text(payload.get("plan_path"))
    expected_plan_path = fetch_plan_path(run_dir, round_id).expanduser().resolve()
    if not payload_plan_path:
        raise ValueError("Fetch execution payload must include plan_path.")
    found_plan_path = Path(payload_plan_path).expanduser().resolve()
    if found_plan_path != expected_plan_path:
        raise ValueError(f"Fetch execution plan_path mismatch: expected {expected_plan_path}, got {found_plan_path}")
    expected_plan_sha256 = file_sha256(expected_plan_path)
    payload_plan_sha256 = maybe_text(payload.get("plan_sha256"))
    if payload_plan_sha256:
        if payload_plan_sha256 != expected_plan_sha256:
            raise ValueError("Fetch execution plan_sha256 does not match the current fetch_plan.json.")
    elif source_path.exists() and source_path.stat().st_mtime_ns < expected_plan_path.stat().st_mtime_ns:
        raise ValueError("Fetch execution appears older than the current fetch_plan.json. Regenerate it from the current round inputs.")
    expected_steps = fetch_plan_steps(run_dir, round_id)
    expected_by_step: dict[str, dict[str, Any]] = {}
    for step in expected_steps:
        step_id = maybe_text(step.get("step_id"))
        if not step_id:
            raise ValueError("Fetch plan contains a step without step_id.")
        if step_id in expected_by_step:
            raise ValueError(f"Fetch plan contains duplicate step_id: {step_id}")
        expected_by_step[step_id] = step
    statuses = payload.get("statuses")
    if not isinstance(statuses, list):
        raise ValueError("Fetch execution payload must include a statuses list.")
    expected_step_count = len(expected_steps)
    payload_step_count = maybe_int(payload.get("step_count"))
    if payload_step_count != expected_step_count:
        raise ValueError(f"Fetch execution step_count mismatch: expected {expected_step_count}, got {payload_step_count}")
    if len(statuses) != expected_step_count:
        raise ValueError(f"Fetch execution statuses length mismatch: expected {expected_step_count}, got {len(statuses)}")
    actual_completed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "completed")
    actual_failed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "failed")
    if maybe_int(payload.get("completed_count")) != actual_completed:
        raise ValueError(
            f"Fetch execution completed_count mismatch: expected {actual_completed}, got {maybe_int(payload.get('completed_count'))}"
        )
    if maybe_int(payload.get("failed_count")) != actual_failed:
        raise ValueError(
            f"Fetch execution failed_count mismatch: expected {actual_failed}, got {maybe_int(payload.get('failed_count'))}"
        )
    seen_step_ids: set[str] = set()
    for status in statuses:
        if not isinstance(status, dict):
            raise ValueError("Fetch execution statuses must be JSON objects.")
        step_id = maybe_text(status.get("step_id"))
        if not step_id:
            raise ValueError(f"Fetch execution status is missing step_id: {status}")
        if step_id in seen_step_ids:
            raise ValueError(f"Fetch execution contains duplicate status for step_id: {step_id}")
        seen_step_ids.add(step_id)
        expected_step = expected_by_step.get(step_id)
        if expected_step is None:
            raise ValueError(f"Fetch execution contains unexpected step_id: {step_id}")
        expected_role = maybe_text(expected_step.get("role"))
        if maybe_text(status.get("role")) != expected_role:
            raise ValueError(f"Fetch execution role mismatch for {step_id}: expected {expected_role}, got {maybe_text(status.get('role'))}")
        expected_source_skill = maybe_text(expected_step.get("source_skill"))
        if maybe_text(status.get("source_skill")) != expected_source_skill:
            raise ValueError(
                f"Fetch execution source_skill mismatch for {step_id}: "
                f"expected {expected_source_skill}, got {maybe_text(status.get('source_skill'))}"
            )
        if not fetch_status_has_usable_artifact(status):
            raise ValueError(f"Fetch execution step {step_id} is not usable for downstream data-plane import: {status}")
        artifact_path = resolved_required_path(status.get("artifact_path"), label=f"fetch execution {step_id} artifact_path")
        expected_artifact_path = resolved_required_path(expected_step.get("artifact_path"), label=f"fetch plan {step_id} artifact_path")
        if artifact_path != expected_artifact_path:
            raise ValueError(
                f"Fetch execution artifact_path mismatch for {step_id}: expected {expected_artifact_path}, got {artifact_path}"
            )
        if not artifact_path.exists():
            raise ValueError(f"Fetch execution artifact_path does not exist: {artifact_path}")
        validate_json_artifact_if_applicable(artifact_path)
        status_state = maybe_text(status.get("status"))
        expected_stdout_path = resolved_required_path(expected_step.get("stdout_path"), label=f"fetch plan {step_id} stdout_path")
        expected_stderr_path = resolved_required_path(expected_step.get("stderr_path"), label=f"fetch plan {step_id} stderr_path")
        actual_stdout_path = optional_resolved_path(status.get("stdout_path"))
        actual_stderr_path = optional_resolved_path(status.get("stderr_path"))
        if status_state == "completed":
            if actual_stdout_path is None or actual_stderr_path is None:
                raise ValueError(f"Fetch execution completed step {step_id} must include stdout_path and stderr_path.")
            if actual_stdout_path != expected_stdout_path:
                raise ValueError(
                    f"Fetch execution stdout_path mismatch for {step_id}: expected {expected_stdout_path}, got {actual_stdout_path}"
                )
            if actual_stderr_path != expected_stderr_path:
                raise ValueError(
                    f"Fetch execution stderr_path mismatch for {step_id}: expected {expected_stderr_path}, got {actual_stderr_path}"
                )
            if not actual_stdout_path.exists():
                raise ValueError(f"Fetch execution stdout_path does not exist: {actual_stdout_path}")
            if not actual_stderr_path.exists():
                raise ValueError(f"Fetch execution stderr_path does not exist: {actual_stderr_path}")
        else:
            if actual_stdout_path is not None and actual_stdout_path != expected_stdout_path:
                raise ValueError(
                    f"Fetch execution stdout_path mismatch for skipped step {step_id}: expected {expected_stdout_path}, got {actual_stdout_path}"
                )
            if actual_stderr_path is not None and actual_stderr_path != expected_stderr_path:
                raise ValueError(
                    f"Fetch execution stderr_path mismatch for skipped step {step_id}: expected {expected_stderr_path}, got {actual_stderr_path}"
                )
            if actual_stdout_path is not None and not actual_stdout_path.exists():
                raise ValueError(f"Fetch execution stdout_path does not exist: {actual_stdout_path}")
            if actual_stderr_path is not None and not actual_stderr_path.exists():
                raise ValueError(f"Fetch execution stderr_path does not exist: {actual_stderr_path}")
    missing_step_ids = sorted(set(expected_by_step) - seen_step_ids)
    if missing_step_ids:
        raise ValueError(f"Fetch execution is missing statuses for steps: {missing_step_ids}")


def ensure_data_plane_execution_matches(payload: Any, *, run_dir: Path, round_id: str, source_path: Path) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Data-plane execution payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_round_id = maybe_text(payload.get("round_id"))
    if not payload_round_id:
        raise ValueError("Data-plane execution payload must include round_id.")
    if require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Data-plane execution round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    payload_run_dir = maybe_text(payload.get("run_dir"))
    expected_run_dir = run_dir.expanduser().resolve()
    if payload_run_dir:
        found_run_dir = Path(payload_run_dir).expanduser().resolve()
        if found_run_dir != expected_run_dir:
            raise ValueError(f"Data-plane execution run_dir mismatch: expected {expected_run_dir}, got {found_run_dir}")
    statuses = payload.get("statuses")
    if not isinstance(statuses, list):
        raise ValueError("Data-plane execution payload must include a statuses list.")
    expected_step_count = len(DATA_PLANE_STEP_IDS)
    payload_step_count = maybe_int(payload.get("step_count"))
    if payload_step_count != expected_step_count:
        raise ValueError(f"Data-plane execution step_count mismatch: expected {expected_step_count}, got {payload_step_count}")
    if len(statuses) != expected_step_count:
        raise ValueError(f"Data-plane execution statuses length mismatch: expected {expected_step_count}, got {len(statuses)}")
    actual_completed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "completed")
    actual_failed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "failed")
    if actual_failed:
        raise ValueError(f"Data-plane execution still contains failed steps: {statuses}")
    if maybe_int(payload.get("completed_count")) != actual_completed:
        raise ValueError(
            f"Data-plane execution completed_count mismatch: expected {actual_completed}, got {maybe_int(payload.get('completed_count'))}"
        )
    if maybe_int(payload.get("failed_count")) != actual_failed:
        raise ValueError(
            f"Data-plane execution failed_count mismatch: expected {actual_failed}, got {maybe_int(payload.get('failed_count'))}"
        )
    seen_step_ids: set[str] = set()
    for status in statuses:
        if not isinstance(status, dict):
            raise ValueError("Data-plane execution statuses must be JSON objects.")
        step_id = maybe_text(status.get("step_id"))
        if not step_id:
            raise ValueError(f"Data-plane execution status is missing step_id: {status}")
        if step_id in seen_step_ids:
            raise ValueError(f"Data-plane execution contains duplicate status for step_id: {step_id}")
        seen_step_ids.add(step_id)
        if step_id not in DATA_PLANE_STEP_IDS:
            raise ValueError(f"Data-plane execution contains unexpected step_id: {step_id}")
        if maybe_text(status.get("status")) != "completed":
            raise ValueError(f"Data-plane execution step {step_id} is not completed: {status}")
    missing_step_ids = sorted(set(DATA_PLANE_STEP_IDS) - seen_step_ids)
    if missing_step_ids:
        raise ValueError(f"Data-plane execution is missing statuses for steps: {missing_step_ids}")
    handoff = resolved_required_path(payload.get("reporting_handoff_path"), label="data-plane execution reporting_handoff_path")
    required_paths = [
        handoff,
        claim_curation_packet_path(run_dir, round_id),
        observation_curation_packet_path(run_dir, round_id),
        claim_curation_prompt_path(run_dir, round_id),
        observation_curation_prompt_path(run_dir, round_id),
    ]
    for path in required_paths:
        if not path.exists():
            raise ValueError(f"Data-plane execution required output does not exist: {path}")
    canonical_fetch_execution = fetch_execution_path(run_dir, round_id)
    if (
        source_path.exists()
        and canonical_fetch_execution.exists()
        and source_path.stat().st_mtime_ns < canonical_fetch_execution.stat().st_mtime_ns
    ):
        raise ValueError("Data-plane execution appears older than the current fetch_execution.json. Regenerate the data plane.")


def ensure_matching_execution_matches(payload: Any, *, run_dir: Path, round_id: str, source_path: Path) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Matching/adjudication execution payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_round_id = maybe_text(payload.get("round_id"))
    if not payload_round_id:
        raise ValueError("Matching/adjudication execution payload must include round_id.")
    if require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Matching/adjudication execution round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    payload_run_dir = maybe_text(payload.get("run_dir"))
    expected_run_dir = run_dir.expanduser().resolve()
    if payload_run_dir:
        found_run_dir = Path(payload_run_dir).expanduser().resolve()
        if found_run_dir != expected_run_dir:
            raise ValueError(f"Matching/adjudication execution run_dir mismatch: expected {expected_run_dir}, got {found_run_dir}")
    statuses = payload.get("statuses")
    if not isinstance(statuses, list):
        raise ValueError("Matching/adjudication execution payload must include a statuses list.")
    expected_step_count = len(MATCHING_ADJUDICATION_STEP_IDS)
    payload_step_count = maybe_int(payload.get("step_count"))
    if payload_step_count != expected_step_count:
        raise ValueError(f"Matching/adjudication execution step_count mismatch: expected {expected_step_count}, got {payload_step_count}")
    if len(statuses) != expected_step_count:
        raise ValueError(f"Matching/adjudication execution statuses length mismatch: expected {expected_step_count}, got {len(statuses)}")
    actual_completed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "completed")
    actual_failed = sum(1 for item in statuses if isinstance(item, dict) and maybe_text(item.get("status")) == "failed")
    if actual_failed:
        raise ValueError(f"Matching/adjudication execution still contains failed steps: {statuses}")
    if maybe_int(payload.get("completed_count")) != actual_completed:
        raise ValueError(
            f"Matching/adjudication execution completed_count mismatch: expected {actual_completed}, got {maybe_int(payload.get('completed_count'))}"
        )
    if maybe_int(payload.get("failed_count")) != actual_failed:
        raise ValueError(
            f"Matching/adjudication execution failed_count mismatch: expected {actual_failed}, got {maybe_int(payload.get('failed_count'))}"
        )
    seen_step_ids: set[str] = set()
    for status in statuses:
        if not isinstance(status, dict):
            raise ValueError("Matching/adjudication execution statuses must be JSON objects.")
        step_id = maybe_text(status.get("step_id"))
        if not step_id:
            raise ValueError(f"Matching/adjudication execution status is missing step_id: {status}")
        if step_id in seen_step_ids:
            raise ValueError(f"Matching/adjudication execution contains duplicate status for step_id: {step_id}")
        seen_step_ids.add(step_id)
        if step_id not in MATCHING_ADJUDICATION_STEP_IDS:
            raise ValueError(f"Matching/adjudication execution contains unexpected step_id: {step_id}")
        if maybe_text(status.get("status")) != "completed":
            raise ValueError(f"Matching/adjudication execution step {step_id} is not completed: {status}")
    missing_step_ids = sorted(set(MATCHING_ADJUDICATION_STEP_IDS) - seen_step_ids)
    if missing_step_ids:
        raise ValueError(f"Matching/adjudication execution is missing statuses for steps: {missing_step_ids}")
    handoff = resolved_required_path(payload.get("reporting_handoff_path"), label="matching execution reporting_handoff_path")
    required_paths = [
        handoff,
        matching_adjudication_path(run_dir, round_id),
        report_packet_path(run_dir, round_id, "sociologist"),
        report_packet_path(run_dir, round_id, "environmentalist"),
        report_prompt_path(run_dir, round_id, "sociologist"),
        report_prompt_path(run_dir, round_id, "environmentalist"),
        matching_result_path(run_dir, round_id),
        evidence_adjudication_path(run_dir, round_id),
    ]
    for path in required_paths:
        if not path.exists():
            raise ValueError(f"Matching/adjudication execution required output does not exist: {path}")
    authorization_payload = load_json_if_exists(matching_authorization_path(run_dir, round_id))
    mission_payload = load_mission(run_dir)
    if isinstance(authorization_payload, dict):
        authorization_payload = effective_matching_authorization_payload(
            mission=mission_payload,
            round_id=round_id,
            payload=authorization_payload,
        )
    if not isinstance(authorization_payload, dict) or maybe_text(authorization_payload.get("authorization_status")) != "authorized":
        raise ValueError("Matching/adjudication execution requires canonical matching_authorization.json with authorization_status=authorized.")
    canonical_authorization = matching_authorization_path(run_dir, round_id)
    if (
        source_path.exists()
        and canonical_authorization.exists()
        and source_path.stat().st_mtime_ns < canonical_authorization.stat().st_mtime_ns
    ):
        raise ValueError("Matching/adjudication execution appears older than the current matching_authorization.json. Regenerate it.")
    canonical_adjudication = matching_adjudication_path(run_dir, round_id)
    if (
        source_path.exists()
        and canonical_adjudication.exists()
        and source_path.stat().st_mtime_ns < canonical_adjudication.stat().st_mtime_ns
    ):
        raise ValueError("Matching/adjudication execution appears older than the current matching_adjudication.json. Regenerate it.")
