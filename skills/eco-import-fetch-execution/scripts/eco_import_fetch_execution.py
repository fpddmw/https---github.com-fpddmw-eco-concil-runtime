#!/usr/bin/env python3
"""Execute one local fetch-plan by importing raw artifacts and invoking normalizers."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-import-fetch-execution"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return payload


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


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


def validate_plan_inputs(plan: dict[str, Any]) -> None:
    snapshot = plan.get("input_snapshot") if isinstance(plan.get("input_snapshot"), dict) else {}
    for key in ("mission", "tasks"):
        entry = snapshot.get(key) if isinstance(snapshot.get(key), dict) else {}
        path_text = maybe_text(entry.get("path"))
        expected_sha = maybe_text(entry.get("sha256"))
        if not path_text or not expected_sha:
            raise ValueError(f"fetch plan is missing input_snapshot.{key} metadata")
        current_path = Path(path_text).expanduser().resolve()
        if not current_path.exists():
            raise ValueError(f"fetch plan input is missing on disk: {current_path}")
        if file_sha256(current_path) != expected_sha:
            raise RuntimeError(f"fetch plan input changed after prepare-round: {current_path}")


def execute_import_step(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    step: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    step_id = maybe_text(step.get("step_id"))
    source_skill = maybe_text(step.get("source_skill"))
    normalizer_skill = maybe_text(step.get("normalizer_skill"))
    source_artifact_path = Path(maybe_text(step.get("source_artifact_path"))).expanduser().resolve()
    raw_artifact_path = Path(maybe_text(step.get("artifact_path"))).expanduser().resolve()
    if not source_artifact_path.exists():
        raise FileNotFoundError(f"Source artifact is missing for {step_id}: {source_artifact_path}")

    raw_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_artifact_path, raw_artifact_path)
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
        "status": "completed",
        "source_skill": source_skill,
        "normalizer_skill": normalizer_skill,
        "source_artifact_path": str(source_artifact_path),
        "artifact_path": str(raw_artifact_path),
        "artifact_sha256": raw_sha256,
        "receipt_id": maybe_text(payload.get("receipt_id")),
        "batch_id": maybe_text(payload.get("batch_id")),
        "canonical_count": len(payload.get("canonical_ids", [])) if isinstance(payload.get("canonical_ids"), list) else 0,
        "artifact_ref_count": len(payload.get("artifact_refs", [])) if isinstance(payload.get("artifact_refs"), list) else 0,
        "warning_count": len(payload.get("warnings", [])) if isinstance(payload.get("warnings"), list) else 0,
    }
    return status, payload


def import_fetch_execution_skill(run_dir: str, run_id: str, round_id: str) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    plan_path = (run_dir_path / "runtime" / f"fetch_plan_{round_id}.json").resolve()
    output_path = (run_dir_path / "runtime" / f"import_execution_{round_id}.json").resolve()

    plan = read_json_object(plan_path)
    validate_plan_inputs(plan)
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
            message = str(exc)
            statuses.append(
                {
                    "step_id": step_id,
                    "status": "failed",
                    "source_skill": maybe_text(step.get("source_skill")),
                    "normalizer_skill": maybe_text(step.get("normalizer_skill")),
                    "reason": message,
                }
            )
            raise RuntimeError(f"Import execution failed at {step_id}: {message}") from exc

    execution_id = "import-execution-" + stable_hash(run_id, round_id, plan_sha256, len(statuses))[:12]
    payload = {
        "schema_version": "ingress-import-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "execution_id": execution_id,
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
            "execution_id": execution_id,
            "normalized_step_count": payload["completed_count"],
            "failed_step_count": payload["failed_count"],
        },
        "receipt_id": "ingress-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, execution_id)[:20],
        "batch_id": "ingressbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_path.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [execution_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [execution_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item.get("message", "") for item in warnings[:3] if maybe_text(item.get("message"))],
            "challenge_hints": [],
            "suggested_next_skills": [
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
    parser = argparse.ArgumentParser(description="Execute one local fetch-plan by importing raw artifacts and invoking normalizers.")
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