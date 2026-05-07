from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.executor import maybe_text, new_runtime_event_id, utc_now_iso
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.core.paths import (
    benchmark_compare_path,
    benchmark_manifest_path,
    ensure_runtime_dirs,
    replay_report_path,
    scenario_fixture_path,
)
from eco_council_runtime.kernel.archive.benchmark.compare import compare_benchmark_manifests
from eco_council_runtime.kernel.archive.benchmark.manifest import materialize_benchmark_manifest

def replay_report_event(
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
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "scenario-replay", started_at, completed_at, payload.get("replay_verdict")),
        "event_type": "scenario-replay",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "replay_verdict": payload.get("replay_verdict"),
        "scenario_id": payload.get("scenario_id"),
        "replay_report_path": str(output_path),
    }

def replay_verdict(compare_verdict: str) -> str:
    if compare_verdict == "scenario-mismatch":
        return "fixture-mismatch"
    if compare_verdict == "regression":
        return "regression-detected"
    if compare_verdict == "match-with-timing-delta":
        return "matched-with-timing-delta"
    return "matched"

def replay_runtime_scenario(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    fixture_path_override: str = "",
    baseline_manifest_override: str = "",
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    fixture_path_value = Path(fixture_path_override).expanduser().resolve() if maybe_text(fixture_path_override) else scenario_fixture_path(run_dir, round_id)
    fixture_payload = load_json_if_exists(fixture_path_value) or {}
    if not fixture_payload:
        raise ValueError(f"Missing scenario fixture: {fixture_path_value}")
    baseline_manifest_path_value = (
        Path(baseline_manifest_override).expanduser().resolve()
        if maybe_text(baseline_manifest_override)
        else Path(maybe_text(fixture_payload.get("baseline_manifest", {}).get("path"))).expanduser().resolve()
    )
    if not baseline_manifest_path_value.exists():
        raise ValueError(f"Missing baseline benchmark manifest for replay: {baseline_manifest_path_value}")
    benchmark_result = materialize_benchmark_manifest(run_dir, run_id=run_id, round_id=round_id)
    current_manifest_path = benchmark_manifest_path(run_dir, round_id)
    compare_result = compare_benchmark_manifests(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        left_manifest_path=str(baseline_manifest_path_value),
        right_manifest_path=str(current_manifest_path),
    )
    compare_payload = compare_result["benchmark_compare"]
    payload = {
        "schema_version": "runtime-replay-report-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "scenario_id": maybe_text(fixture_payload.get("scenario_id")),
        "fixture_path": str(fixture_path_value),
        "baseline_manifest_path": str(baseline_manifest_path_value),
        "current_manifest_path": str(current_manifest_path),
        "benchmark_compare_path": str(benchmark_compare_path(run_dir, round_id)),
        "scenario_match": bool(compare_payload.get("scenario_match")),
        "output_match": bool(compare_payload.get("output_match")),
        "compare_verdict": maybe_text(compare_payload.get("verdict")),
        "replay_verdict": replay_verdict(maybe_text(compare_payload.get("verdict"))),
        "expected_output_fingerprint": maybe_text(fixture_payload.get("baseline_manifest", {}).get("output_fingerprint")),
        "current_output_fingerprint": maybe_text(benchmark_result.get("benchmark_manifest", {}).get("output_fingerprint")),
        "artifact_drift_count": len(compare_payload.get("artifact_drift", []))
        if isinstance(compare_payload.get("artifact_drift"), list)
        else 0,
        "changed_field_count": int(compare_payload.get("changed_field_count") or 0),
        "timing_delta_count": int(compare_payload.get("timing_delta_count") or 0),
        "failure_delta": compare_payload.get("failure_delta", {}) if isinstance(compare_payload.get("failure_delta"), dict) else {},
        "replay_contract": fixture_payload.get("replay_contract", {}) if isinstance(fixture_payload.get("replay_contract"), dict) else {},
    }
    output_path = replay_report_path(run_dir, round_id)
    write_json(output_path, payload)
    append_ledger_event(
        run_dir,
        replay_report_event(
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
            "replay_report_path": str(output_path),
            "compare_verdict": payload["compare_verdict"],
            "replay_verdict": payload["replay_verdict"],
        },
        "replay_report": payload,
    }
