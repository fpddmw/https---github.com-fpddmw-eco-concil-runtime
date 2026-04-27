#!/usr/bin/env python3
"""Run approved fetch-plan steps, normalize raw artifacts, and write an execution receipt."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "normalize-fetch-execution"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.source_queue_contract import (  # noqa: E402
    file_sha256,
    maybe_text,
    read_json_object,
    resolve_run_dir,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json_file,
)
from eco_council_runtime.kernel.source_queue_execution import (  # noqa: E402
    DetachedFetchExecutionError,
    copy_import_artifact,
    execute_detached_fetch_step,
    resolved_artifact_path,
)
from eco_council_runtime.kernel.source_queue_planner import ensure_fetch_plan_inputs_match  # noqa: E402


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def normalizer_script_path(skill_name: str) -> Path:
    return WORKSPACE_ROOT / "skills" / skill_name / "scripts" / f"{skill_name.replace('-', '_')}.py"


def run_json_script(script_path: Path, *args: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=str(WORKSPACE_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit={completed.returncode}"
        raise RuntimeError(f"Script failed: {script_path.name}: {detail}")
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Script did not emit valid JSON: {script_path.name}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Script did not emit a JSON object: {script_path.name}")
    return payload


def raw_only_normalization_payload(*, source_skill: str, run_id: str, round_id: str, artifact_path: Path, reason: str) -> dict[str, Any]:
    artifact_ref = {
        "signal_id": "",
        "artifact_path": str(artifact_path),
        "record_locator": "$",
        "artifact_ref": f"{artifact_path}:$",
    }
    raw_receipt_id = "raw-receipt-" + stable_hash(SKILL_NAME, source_skill, run_id, round_id, artifact_path.name)[:20]
    raw_batch_id = "rawbatch-" + stable_hash(SKILL_NAME, source_skill, artifact_path)[:16]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "mode": "raw-only",
            "run_id": run_id,
            "round_id": round_id,
            "source_skill": source_skill,
            "artifact_path": str(artifact_path),
        },
        "receipt_id": raw_receipt_id,
        "batch_id": raw_batch_id,
        "artifact_refs": [artifact_ref],
        "canonical_ids": [],
        "warnings": [{"code": "raw-only-ingest", "message": reason}],
    }


def execute_queue_step(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    step: dict[str, Any],
) -> tuple[Path, dict[str, Any] | None, dict[str, Any]]:
    step_id = maybe_text(step.get("step_id"))
    step_kind = maybe_text(step.get("step_kind")) or "import"
    source_skill = maybe_text(step.get("source_skill"))
    raw_artifact_path = resolved_artifact_path(step)
    fetch_details: dict[str, Any] | None = None

    if step_kind == "import":
        raw_artifact_path = copy_import_artifact(step)
    elif step_kind == "detached-fetch":
        raw_artifact_path, fetch_details = execute_detached_fetch_step(step, run_dir=run_dir, run_id=run_id, round_id=round_id)
    else:
        raise RuntimeError(f"Unsupported step_kind: {step_kind}")

    raw_sha256 = file_sha256(raw_artifact_path)
    fetch_contract = {
        "source_skill": source_skill,
        "operation_kind": step_kind,
        "output_object_kind": "raw-artifact",
        "research_judgement": "none",
        "source_provenance": {
            "source_skill": source_skill,
            "family_id": maybe_text(step.get("family_id")),
            "layer_id": maybe_text(step.get("layer_id")),
            "artifact_path": str(raw_artifact_path),
            "artifact_sha256": raw_sha256,
        },
        "data_quality": {
            "quality_flags": ["raw-artifact", step_kind],
            "normalization_scope": "not-normalized",
        },
        "coverage_limitations": [
            "Raw fetch/import artifacts reflect provider availability and the selected source request only.",
            "No research judgement, representativeness claim, readiness status, or policy conclusion is produced by the queue runner.",
        ],
    }
    queue_status: dict[str, Any] = {
        "step_id": step_id,
        "step_kind": step_kind,
        "status": "completed",
        "component": "queue-runner",
        "role": maybe_text(step.get("role")),
        "source_skill": source_skill,
        "artifact_path": str(raw_artifact_path),
        "artifact_dir": maybe_text(step.get("artifact_dir")),
        "artifact_sha256": raw_sha256,
        "fetch_contract": fetch_contract,
    }
    if step_kind == "import":
        queue_status["source_artifact_path"] = maybe_text(step.get("source_artifact_path"))
    if fetch_details is not None:
        queue_status["detached_fetch"] = fetch_details
    return raw_artifact_path, fetch_details, queue_status


def run_normalizer_for_step(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    step: dict[str, Any],
    raw_artifact_path: Path,
) -> dict[str, Any]:
    source_skill = maybe_text(step.get("source_skill"))
    normalizer_skill = maybe_text(step.get("normalizer_skill"))
    normalizer_args = [maybe_text(item) for item in step.get("normalizer_args", []) if maybe_text(item)] if isinstance(step.get("normalizer_args"), list) else []
    if not normalizer_skill:
        return raw_only_normalization_payload(
            source_skill=source_skill,
            run_id=run_id,
            round_id=round_id,
            artifact_path=raw_artifact_path,
            reason=f"{source_skill} has no mapped normalizer skill yet; raw artifact was kept for later processing.",
        )
    else:
        script_path = normalizer_script_path(normalizer_skill)
        if not script_path.exists():
            return raw_only_normalization_payload(
                source_skill=source_skill,
                run_id=run_id,
                round_id=round_id,
                artifact_path=raw_artifact_path,
                reason=f"Normalizer script {normalizer_skill} is not present; raw artifact was kept for later processing.",
            )
        return run_json_script(
            script_path,
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--artifact-path",
            str(raw_artifact_path),
            *normalizer_args,
        )


def execute_import_step(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    step: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_artifact_path, fetch_details, queue_status = execute_queue_step(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        step=step,
    )
    payload = run_normalizer_for_step(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        step=step,
        raw_artifact_path=raw_artifact_path,
    )
    status = {
        "step_id": maybe_text(step.get("step_id")),
        "step_kind": maybe_text(step.get("step_kind")) or "import",
        "status": "completed",
        "components": {
            "queue_runner": "completed",
            "normalizer_runner": "completed",
            "execution_receipt": "pending",
        },
        "role": maybe_text(step.get("role")),
        "source_skill": maybe_text(step.get("source_skill")),
        "normalizer_skill": maybe_text(step.get("normalizer_skill")),
        "artifact_path": str(raw_artifact_path),
        "artifact_dir": maybe_text(step.get("artifact_dir")),
        "artifact_sha256": maybe_text(queue_status.get("artifact_sha256")),
        "receipt_id": maybe_text(payload.get("receipt_id")),
        "batch_id": maybe_text(payload.get("batch_id")),
        "canonical_count": len(payload.get("canonical_ids", [])) if isinstance(payload.get("canonical_ids"), list) else 0,
        "artifact_ref_count": len(payload.get("artifact_refs", [])) if isinstance(payload.get("artifact_refs"), list) else 0,
        "warning_count": len(payload.get("warnings", [])) if isinstance(payload.get("warnings"), list) else 0,
        "queue_runner": queue_status,
        "normalizer_runner": {
            "status": maybe_text(payload.get("status")) or "completed",
            "receipt_id": maybe_text(payload.get("receipt_id")),
            "batch_id": maybe_text(payload.get("batch_id")),
            "canonical_count": len(payload.get("canonical_ids", [])) if isinstance(payload.get("canonical_ids"), list) else 0,
            "artifact_ref_count": len(payload.get("artifact_refs", [])) if isinstance(payload.get("artifact_refs"), list) else 0,
        },
    }
    if maybe_text(step.get("step_kind")) == "import":
        status["source_artifact_path"] = maybe_text(step.get("source_artifact_path"))
    if fetch_details is not None:
        status["detached_fetch"] = fetch_details
    return status, payload


def build_execution_payload(
    *,
    run_id: str,
    round_id: str,
    plan_path: Path,
    plan_sha256: str,
    statuses: list[dict[str, Any]],
    normalized_receipt_ids: list[str],
    normalized_artifact_refs: list[dict[str, Any]],
    failure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": "ingress-import-v2",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "execution_id": "import-execution-" + stable_hash(run_id, round_id, plan_sha256, len(statuses))[:12],
        "plan_path": str(plan_path),
        "plan_sha256": plan_sha256,
        "step_count": len(statuses),
        "completed_count": len([item for item in statuses if maybe_text(item.get("status")) == "completed"]),
        "failed_count": len([item for item in statuses if maybe_text(item.get("status")) == "failed"]),
        "statuses": statuses,
        "normalized_receipt_ids": unique_texts(normalized_receipt_ids),
        "normalized_artifact_refs": normalized_artifact_refs,
        "execution_components": {
            "queue_runner": {
                "status": "completed" if all(maybe_text(item.get("status")) == "completed" for item in statuses) else "failed",
                "completed_count": len([item for item in statuses if maybe_text(item.get("queue_runner", {}).get("status") if isinstance(item.get("queue_runner"), dict) else item.get("status")) == "completed"]),
            },
            "normalizer_runner": {
                "status": "completed" if all(maybe_text(item.get("status")) == "completed" for item in statuses) else "failed",
                "receipt_count": len(unique_texts(normalized_receipt_ids)),
            },
            "execution_receipt": {
                "status": "completed" if failure is None else "failed",
                "plan_sha256": plan_sha256,
            },
        },
    }
    if failure is not None:
        payload["failure"] = failure
    return payload


def import_fetch_execution_skill(run_dir: str, run_id: str, round_id: str) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    plan_path = (run_dir_path / "runtime" / f"fetch_plan_{round_id}.json").resolve()
    output_path = (run_dir_path / "runtime" / f"import_execution_{round_id}.json").resolve()

    plan = read_json_object(plan_path)
    ensure_fetch_plan_inputs_match(run_dir=run_dir_path, round_id=round_id, plan=plan)
    steps = [item for item in plan.get("steps", []) if isinstance(item, dict)] if isinstance(plan.get("steps"), list) else []
    plan_sha256 = file_sha256(plan_path)

    statuses: list[dict[str, Any]] = []
    normalized_receipt_ids: list[str] = []
    normalized_artifact_refs: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []

    for step in steps:
        try:
            status, payload = execute_import_step(run_dir=run_dir_path, run_id=run_id, round_id=round_id, step=step)
            statuses.append(status)
            normalized_receipt_ids.append(maybe_text(payload.get("receipt_id")))
            if isinstance(payload.get("artifact_refs"), list):
                normalized_artifact_refs.extend(item for item in payload["artifact_refs"] if isinstance(item, dict))
            if isinstance(payload.get("warnings"), list):
                warnings.extend(item for item in payload["warnings"] if isinstance(item, dict) and maybe_text(item.get("message")))
        except Exception as exc:  # noqa: BLE001
            step_id = maybe_text(step.get("step_id")) or "unknown-step"
            failed_status = {
                "step_id": step_id,
                "step_kind": maybe_text(step.get("step_kind")) or "import",
                "status": "failed",
                "role": maybe_text(step.get("role")),
                "source_skill": maybe_text(step.get("source_skill")),
                "normalizer_skill": maybe_text(step.get("normalizer_skill")),
                "reason": str(exc),
            }
            failure_payload: dict[str, Any] = {
                "step_id": step_id,
                "message": str(exc),
            }
            if isinstance(exc, DetachedFetchExecutionError):
                failed_status["detached_fetch"] = exc.payload
                failure_payload["detached_fetch"] = exc.payload
            statuses.append(failed_status)
            partial_payload = build_execution_payload(
                run_id=run_id,
                round_id=round_id,
                plan_path=plan_path,
                plan_sha256=plan_sha256,
                statuses=statuses,
                normalized_receipt_ids=normalized_receipt_ids,
                normalized_artifact_refs=normalized_artifact_refs,
                failure=failure_payload,
            )
            write_json_file(output_path, partial_payload)
            raise RuntimeError(f"Import execution failed at {step_id}: {exc}") from exc

    payload = build_execution_payload(
        run_id=run_id,
        round_id=round_id,
        plan_path=plan_path,
        plan_sha256=plan_sha256,
        statuses=statuses,
        normalized_receipt_ids=normalized_receipt_ids,
        normalized_artifact_refs=normalized_artifact_refs,
    )
    for status in statuses:
        components = status.get("components") if isinstance(status.get("components"), dict) else {}
        if components and maybe_text(status.get("status")) == "completed":
            components["execution_receipt"] = "completed"
    write_json_file(output_path, payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_path), "record_locator": "$", "artifact_ref": f"{output_path}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_path),
            "execution_id": payload["execution_id"],
            "normalized_step_count": payload["completed_count"],
            "failed_step_count": payload["failed_count"],
        },
        "receipt_id": "ingress-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, payload["execution_id"])[:20],
        "batch_id": "ingressbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_path.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [payload["execution_id"]],
        "warnings": warnings,
        "execution_components": payload.get("execution_components", {}),
        "board_handoff": {
            "candidate_ids": [payload["execution_id"]],
            "evidence_refs": artifact_refs,
            "gap_hints": [item.get("message", "") for item in warnings[:3] if maybe_text(item.get("message"))],
            "challenge_hints": [],
            "suggested_next_skills": [
                "query-public-signals",
                "query-formal-signals",
                "query-environment-signals",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fetch-plan queue, normalization, and execution receipt components.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = import_fetch_execution_skill(run_dir=args.run_dir, run_id=args.run_id, round_id=args.round_id)
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
