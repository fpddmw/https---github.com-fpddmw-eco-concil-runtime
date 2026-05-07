from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.executor import maybe_text, new_runtime_event_id, utc_now_iso
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.core.paths import benchmark_compare_path, ensure_runtime_dirs
from eco_council_runtime.kernel.archive.benchmark.common import rounded_number

def diff_values(left: Any, right: Any, *, path: str = "$", limit: int = 200) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    if type(left) is not type(right):
        return [{"path": path, "left": left, "right": right}]
    if isinstance(left, dict):
        changes: list[dict[str, Any]] = []
        for key in sorted(set(left) | set(right)):
            next_path = f"{path}.{key}"
            if key not in left:
                changes.append({"path": next_path, "left": None, "right": right[key]})
            elif key not in right:
                changes.append({"path": next_path, "left": left[key], "right": None})
            else:
                changes.extend(diff_values(left[key], right[key], path=next_path, limit=limit - len(changes)))
            if len(changes) >= limit:
                return changes[:limit]
        return changes
    if isinstance(left, list):
        if len(left) != len(right):
            return [{"path": path, "left": left, "right": right}]
        changes: list[dict[str, Any]] = []
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            changes.extend(diff_values(left_item, right_item, path=f"{path}[{index}]", limit=limit - len(changes)))
            if len(changes) >= limit:
                return changes[:limit]
        return changes
    if left != right:
        return [{"path": path, "left": left, "right": right}]
    return []

def compare_named_rows(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    *,
    key_field: str,
) -> list[dict[str, Any]]:
    left_map = {maybe_text(item.get(key_field)): item for item in left_rows if maybe_text(item.get(key_field))}
    right_map = {maybe_text(item.get(key_field)): item for item in right_rows if maybe_text(item.get(key_field))}
    drift: list[dict[str, Any]] = []
    for key in sorted(set(left_map) | set(right_map)):
        left_item = left_map.get(key)
        right_item = right_map.get(key)
        if left_item is None or right_item is None:
            drift.append({"key": key, "left": left_item, "right": right_item})
            continue
        if left_item != right_item:
            drift.append({"key": key, "left": left_item, "right": right_item})
    return drift

def compare_artifact_outputs(left_rows: list[dict[str, Any]], right_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return compare_named_rows(left_rows, right_rows, key_field="artifact_key")

def compare_failure_summary(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_failed = int(left.get("failed_event_count") or 0)
    right_failed = int(right.get("failed_event_count") or 0)
    left_blocked = int(left.get("blocked_event_count") or 0)
    right_blocked = int(right.get("blocked_event_count") or 0)
    return {
        "left_failed_event_count": left_failed,
        "right_failed_event_count": right_failed,
        "failed_event_delta": right_failed - left_failed,
        "left_blocked_event_count": left_blocked,
        "right_blocked_event_count": right_blocked,
        "blocked_event_delta": right_blocked - left_blocked,
        "new_failing_skills": [
            skill_name
            for skill_name in right.get("failing_skills", [])
            if skill_name not in set(left.get("failing_skills", []))
        ]
        if isinstance(right.get("failing_skills"), list) and isinstance(left.get("failing_skills"), list)
        else [],
        "new_failed_stage_names": [
            stage_name
            for stage_name in right.get("failed_stage_names", [])
            if stage_name not in set(left.get("failed_stage_names", []))
        ]
        if isinstance(right.get("failed_stage_names"), list) and isinstance(left.get("failed_stage_names"), list)
        else [],
    }

def compare_skill_timing(left_rows: list[dict[str, Any]], right_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    left_map = {maybe_text(item.get("skill_name")): item for item in left_rows if maybe_text(item.get("skill_name"))}
    right_map = {maybe_text(item.get("skill_name")): item for item in right_rows if maybe_text(item.get("skill_name"))}
    deltas: list[dict[str, Any]] = []
    for skill_name in sorted(set(left_map) | set(right_map)):
        left_item = left_map.get(skill_name, {})
        right_item = right_map.get(skill_name, {})
        left_duration = rounded_number(left_item.get("total_duration_seconds"))
        right_duration = rounded_number(right_item.get("total_duration_seconds"))
        left_attempts = int(left_item.get("total_attempt_count") or 0)
        right_attempts = int(right_item.get("total_attempt_count") or 0)
        if left_item == right_item:
            continue
        deltas.append(
            {
                "skill_name": skill_name,
                "left_total_duration_seconds": left_duration,
                "right_total_duration_seconds": right_duration,
                "duration_delta_seconds": rounded_number(right_duration - left_duration),
                "left_total_attempt_count": left_attempts,
                "right_total_attempt_count": right_attempts,
                "attempt_delta": right_attempts - left_attempts,
            }
        )
    return deltas

def benchmark_compare_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "benchmark-compare", started_at, completed_at, payload.get("verdict")),
        "event_type": "benchmark-compare",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "scenario_match": bool(payload.get("scenario_match")),
        "output_match": bool(payload.get("output_match")),
        "verdict": payload.get("verdict"),
        "benchmark_compare_path": str(output_path),
    }

def compare_benchmark_manifests(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    left_manifest_path: str,
    right_manifest_path: str,
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    left_path = Path(left_manifest_path).expanduser().resolve()
    right_path = Path(right_manifest_path).expanduser().resolve()
    left_payload = load_json_if_exists(left_path) or {}
    right_payload = load_json_if_exists(right_path) or {}
    if not left_payload:
        raise ValueError(f"Missing left benchmark manifest: {left_path}")
    if not right_payload:
        raise ValueError(f"Missing right benchmark manifest: {right_path}")
    left_basis = left_payload.get("comparison_basis", {}) if isinstance(left_payload.get("comparison_basis"), dict) else {}
    right_basis = right_payload.get("comparison_basis", {}) if isinstance(right_payload.get("comparison_basis"), dict) else {}
    changed_fields = diff_values(left_basis, right_basis)
    artifact_drift = compare_artifact_outputs(
        left_basis.get("artifact_outputs", []) if isinstance(left_basis.get("artifact_outputs"), list) else [],
        right_basis.get("artifact_outputs", []) if isinstance(right_basis.get("artifact_outputs"), list) else [],
    )
    governed_execution_step_drift = compare_named_rows(
        left_basis.get("governed_execution", {}).get("steps", []) if isinstance(left_basis.get("governed_execution"), dict) else [],
        right_basis.get("governed_execution", {}).get("steps", []) if isinstance(right_basis.get("governed_execution"), dict) else [],
        key_field="stage",
    )
    post_round_step_drift = compare_named_rows(
        left_basis.get("post_round", {}).get("steps", []) if isinstance(left_basis.get("post_round"), dict) else [],
        right_basis.get("post_round", {}).get("steps", []) if isinstance(right_basis.get("post_round"), dict) else [],
        key_field="stage",
    )
    failure_delta = compare_failure_summary(
        left_payload.get("failure_summary", {}) if isinstance(left_payload.get("failure_summary"), dict) else {},
        right_payload.get("failure_summary", {}) if isinstance(right_payload.get("failure_summary"), dict) else {},
    )
    timing_deltas = compare_skill_timing(
        left_payload.get("skill_timing_summary", []) if isinstance(left_payload.get("skill_timing_summary"), list) else [],
        right_payload.get("skill_timing_summary", []) if isinstance(right_payload.get("skill_timing_summary"), list) else [],
    )
    scenario_match = maybe_text(left_payload.get("scenario_fingerprint")) == maybe_text(right_payload.get("scenario_fingerprint"))
    output_match = maybe_text(left_payload.get("output_fingerprint")) == maybe_text(right_payload.get("output_fingerprint"))
    failure_regression = bool(failure_delta["failed_event_delta"] > 0 or failure_delta["blocked_event_delta"] > 0)
    verdict = "match"
    if not scenario_match:
        verdict = "scenario-mismatch"
    elif not output_match or failure_regression:
        verdict = "regression"
    elif timing_deltas:
        verdict = "match-with-timing-delta"
    payload = {
        "schema_version": "runtime-benchmark-compare-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "left_manifest": {
            "path": str(left_path),
            "run_id": maybe_text(left_payload.get("run_id")),
            "round_id": maybe_text(left_payload.get("round_id")),
            "scenario_fingerprint": maybe_text(left_payload.get("scenario_fingerprint")),
            "output_fingerprint": maybe_text(left_payload.get("output_fingerprint")),
        },
        "right_manifest": {
            "path": str(right_path),
            "run_id": maybe_text(right_payload.get("run_id")),
            "round_id": maybe_text(right_payload.get("round_id")),
            "scenario_fingerprint": maybe_text(right_payload.get("scenario_fingerprint")),
            "output_fingerprint": maybe_text(right_payload.get("output_fingerprint")),
        },
        "scenario_match": scenario_match,
        "output_match": output_match,
        "failure_regression": failure_regression,
        "verdict": verdict,
        "changed_field_count": len(changed_fields),
        "timing_delta_count": len(timing_deltas),
        "artifact_drift": artifact_drift,
        "governed_execution_step_drift": governed_execution_step_drift,
        "post_round_step_drift": post_round_step_drift,
        "failure_delta": failure_delta,
        "timing_deltas": timing_deltas,
        "changed_fields": changed_fields,
    }
    output_path = benchmark_compare_path(run_dir, round_id)
    write_json(output_path, payload)
    append_ledger_event(
        run_dir,
        benchmark_compare_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            payload=payload,
            output_path=output_path,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "benchmark_compare_path": str(output_path),
            "verdict": verdict,
            "scenario_match": scenario_match,
            "output_match": output_match,
            "changed_field_count": len(changed_fields),
        },
        "benchmark_compare": payload,
    }
