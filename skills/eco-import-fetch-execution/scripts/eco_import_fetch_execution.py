#!/usr/bin/env python3
"""Execute one prepared fetch plan with mixed import and detached-fetch steps."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-import-fetch-execution"
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


def render_fetch_argv(step: dict[str, Any], *, run_dir: Path, run_id: str, round_id: str) -> list[str]:
    artifact_path = maybe_text(step.get("artifact_path"))
    substitutions = {
        "artifact_path": artifact_path,
        "run_dir": str(run_dir),
        "run_id": run_id,
        "round_id": round_id,
        "source_skill": maybe_text(step.get("source_skill")),
    }
    argv = step.get("fetch_argv") if isinstance(step.get("fetch_argv"), list) else []
    return [maybe_text(arg).format(**substitutions) for arg in argv if maybe_text(arg)]


def materialize_detached_fetch_artifact(step: dict[str, Any], *, run_dir: Path, run_id: str, round_id: str) -> Path:
    artifact_path = Path(maybe_text(step.get("artifact_path"))).expanduser().resolve()
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    argv = render_fetch_argv(step, run_dir=run_dir, run_id=run_id, round_id=round_id)
    if not argv:
        raise RuntimeError(f"Detached fetch step has no fetch_argv: {maybe_text(step.get('step_id'))}")
    fetch_cwd = Path(maybe_text(step.get("fetch_cwd")) or str(WORKSPACE_ROOT)).expanduser().resolve()
    completed = subprocess.run(argv, cwd=str(fetch_cwd), capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit={completed.returncode}"
        raise RuntimeError(f"Detached fetch command failed: {detail}")

    capture_mode = maybe_text(step.get("artifact_capture")) or "stdout-json"
    if capture_mode == "stdout-json":
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Detached fetch stdout was not valid JSON.") from exc
        artifact_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    elif capture_mode == "stdout-text":
        artifact_path.write_text(completed.stdout, encoding="utf-8")
    elif capture_mode == "direct-file":
        if not artifact_path.exists():
            raise RuntimeError(f"Detached fetch expected a direct-file artifact at {artifact_path}")
    else:
        raise RuntimeError(f"Unsupported detached fetch artifact_capture: {capture_mode}")
    return artifact_path


def execute_import_step(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    step: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    step_id = maybe_text(step.get("step_id"))
    step_kind = maybe_text(step.get("step_kind")) or "import"
    source_skill = maybe_text(step.get("source_skill"))
    normalizer_skill = maybe_text(step.get("normalizer_skill"))
    raw_artifact_path = Path(maybe_text(step.get("artifact_path"))).expanduser().resolve()

    if step_kind == "import":
        source_artifact_path = Path(maybe_text(step.get("source_artifact_path"))).expanduser().resolve()
        if not source_artifact_path.exists():
            raise FileNotFoundError(f"Source artifact is missing for {step_id}: {source_artifact_path}")
        raw_artifact_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_artifact_path, raw_artifact_path)
    elif step_kind == "detached-fetch":
        source_artifact_path = materialize_detached_fetch_artifact(step, run_dir=run_dir, run_id=run_id, round_id=round_id)
        raw_artifact_path = source_artifact_path
    else:
        raise RuntimeError(f"Unsupported step_kind: {step_kind}")

    raw_sha256 = file_sha256(raw_artifact_path)
    script_path = normalizer_script_path(normalizer_skill)
    if not script_path.exists():
        raise FileNotFoundError(f"Normalizer script is missing for {normalizer_skill}: {script_path}")

    normalizer_args = [maybe_text(item) for item in step.get("normalizer_args", []) if maybe_text(item)] if isinstance(step.get("normalizer_args"), list) else []
    payload = run_json_script(
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
    status = {
        "step_id": step_id,
        "step_kind": step_kind,
        "status": "completed",
        "role": maybe_text(step.get("role")),
        "source_skill": source_skill,
        "normalizer_skill": normalizer_skill,
        "artifact_path": str(raw_artifact_path),
        "artifact_sha256": raw_sha256,
        "receipt_id": maybe_text(payload.get("receipt_id")),
        "batch_id": maybe_text(payload.get("batch_id")),
        "canonical_count": len(payload.get("canonical_ids", [])) if isinstance(payload.get("canonical_ids"), list) else 0,
        "artifact_ref_count": len(payload.get("artifact_refs", [])) if isinstance(payload.get("artifact_refs"), list) else 0,
        "warning_count": len(payload.get("warnings", [])) if isinstance(payload.get("warnings"), list) else 0,
    }
    if step_kind == "import":
        status["source_artifact_path"] = maybe_text(step.get("source_artifact_path"))
    return status, payload


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
            statuses.append(
                {
                    "step_id": step_id,
                    "step_kind": maybe_text(step.get("step_kind")) or "import",
                    "status": "failed",
                    "role": maybe_text(step.get("role")),
                    "source_skill": maybe_text(step.get("source_skill")),
                    "normalizer_skill": maybe_text(step.get("normalizer_skill")),
                    "reason": str(exc),
                }
            )
            raise RuntimeError(f"Import execution failed at {step_id}: {exc}") from exc

    payload = {
        "schema_version": "ingress-import-v2",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "execution_id": "import-execution-" + stable_hash(run_id, round_id, plan_sha256, len(statuses))[:12],
        "plan_path": str(plan_path),
        "plan_sha256": plan_sha256,
        "step_count": len(steps),
        "completed_count": len([item for item in statuses if maybe_text(item.get("status")) == "completed"]),
        "failed_count": len([item for item in statuses if maybe_text(item.get("status")) == "failed"]),
        "statuses": statuses,
        "normalized_receipt_ids": unique_texts(normalized_receipt_ids),
        "normalized_artifact_refs": normalized_artifact_refs,
    }
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
        "board_handoff": {
            "candidate_ids": [payload["execution_id"]],
            "evidence_refs": artifact_refs,
            "gap_hints": [item.get("message", "") for item in warnings[:3] if maybe_text(item.get("message"))],
            "challenge_hints": [],
            "suggested_next_skills": [
                "eco-build-normalization-audit",
                "eco-extract-claim-candidates",
                "eco-extract-observation-candidates",
                "eco-cluster-claim-candidates",
                "eco-merge-observation-candidates",
                "eco-link-claims-to-observations",
                "eco-derive-claim-scope",
                "eco-derive-observation-scope",
                "eco-score-evidence-coverage",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute one prepared fetch plan with mixed import and detached-fetch steps.")
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
