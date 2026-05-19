#!/usr/bin/env python3
"""Run approved fetch-plan steps, normalize raw artifacts, and write an execution receipt."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "normalize-fetch-execution"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.source_queue.source_queue_contract import (  # noqa: E402
    file_sha256,
    maybe_text,
    read_json_object,
    resolve_run_dir,
    source_normalizer_skill,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json_file,
)
from eco_council_runtime.kernel.source_queue.source_queue_execution import (  # noqa: E402
    DetachedFetchExecutionError,
    copy_import_artifact,
    execute_detached_fetch_step,
    resolved_artifact_path,
)
from eco_council_runtime.kernel.source_queue.source_queue_planner import ensure_fetch_plan_inputs_match  # noqa: E402
from eco_council_runtime.kernel.governance.role_contracts import (  # noqa: E402
    normalize_actor_role,
    role_contract,
)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def kernel_run_skill_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    skill_name: str,
    actor_role: str,
    skill_args: list[str] | None = None,
) -> str:
    command = [
        "python3",
        "eco-concil-runtime/scripts/eco_runtime_kernel.py",
        "run-skill",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--skill-name",
        skill_name,
        "--actor-role",
        actor_role,
        "--contract-mode",
        "warn",
    ]
    if skill_args:
        command.extend(["--", *skill_args])
    return shlex.join(command)


def signal_query_command_hints(*, run_dir: Path, run_id: str, round_id: str, actor_role: str) -> dict[str, str]:
    return {
        "public": kernel_run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-public-signals",
            actor_role=actor_role,
        ),
        "formal": kernel_run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-formal-signals",
            actor_role=actor_role,
        ),
        "environment": kernel_run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-environment-signals",
            actor_role=actor_role,
        ),
    }


def normalize_execution_command_hint(*, run_dir: Path, run_id: str, round_id: str, actor_role: str) -> str:
    return kernel_run_skill_command(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=SKILL_NAME,
        actor_role=actor_role,
    )


def normalize_receipt_command_hint(*, run_dir: Path, run_id: str, round_id: str, actor_role: str, receipt_ref: str = "") -> str:
    skill_args: list[str] = []
    if receipt_ref:
        option = "--receipt-path" if "/" in receipt_ref or receipt_ref.endswith(".json") else "--receipt-id"
        skill_args.extend([option, receipt_ref])
    return kernel_run_skill_command(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=SKILL_NAME,
        actor_role=actor_role,
        skill_args=skill_args,
    )


def next_step_hints(*, run_dir: Path, run_id: str, round_id: str, actor_role: str) -> dict[str, Any]:
    return {
        "normalize_fetch_execution_command": normalize_execution_command_hint(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
        ),
        "normalize_runtime_receipt_command": normalize_receipt_command_hint(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
            receipt_ref="<runtime_receipt_id_or_path>",
        ),
        "query_commands": signal_query_command_hints(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
        ),
        "archive_checkpoint_command": kernel_run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="archive-signal-corpus",
            actor_role="moderator",
        ),
    }


def status_normalization_state(statuses: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_count = 0
    receipt_only_count = 0
    receipt_only_sources: list[str] = []
    for status in statuses:
        if not isinstance(status, dict) or maybe_text(status.get("status")) != "completed":
            continue
        state = maybe_text(status.get("normalization_status"))
        canonical_count = int(status.get("canonical_count") or 0)
        if state == "receipt-only" or canonical_count == 0:
            receipt_only_count += 1
            receipt_only_sources.append(maybe_text(status.get("source_skill")))
        else:
            normalized_count += 1
    if normalized_count and receipt_only_count:
        normalization_status = "mixed-normalized-and-receipt-only"
    elif normalized_count:
        normalization_status = "normalized-signal-plane"
    elif receipt_only_count:
        normalization_status = "receipt-only"
    else:
        normalization_status = "no-completed-steps"
    return {
        "normalization_status": normalization_status,
        "normalized_signal_step_count": normalized_count,
        "receipt_only_step_count": receipt_only_count,
        "receipt_only_sources": unique_texts(receipt_only_sources),
    }


def normalizer_script_path(skill_name: str) -> Path:
    return WORKSPACE_ROOT / "skills" / skill_name / "scripts" / f"{skill_name.replace('-', '_')}.py"


def attachment_text_extraction_manifest_path(raw_artifact_path: Path) -> tuple[Path, Path]:
    output_dir = raw_artifact_path.with_suffix("").with_name(f"{raw_artifact_path.stem}-text")
    manifest_path = output_dir / "document_text_extraction_manifest.json"
    return output_dir, manifest_path


def run_attachment_text_extraction(raw_artifact_path: Path) -> tuple[Path, dict[str, Any]]:
    script_path = normalizer_script_path("extract-document-text")
    if not script_path.exists():
        raise RuntimeError("Script failed: extract-document-text normalizer bridge is not present.")
    output_dir, manifest_path = attachment_text_extraction_manifest_path(raw_artifact_path)
    payload = run_json_script(
        script_path,
        "--input-manifest",
        str(raw_artifact_path),
        "--output-dir",
        str(output_dir),
        "--manifest-output",
        str(manifest_path),
        "--overwrite",
    )
    if not manifest_path.exists():
        raise RuntimeError(f"extract-document-text did not write expected manifest: {manifest_path}")
    return manifest_path, payload


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


def actor_role_labels(actor_role: str) -> list[str]:
    resolved = normalize_actor_role(actor_role) or maybe_text(actor_role)
    contract = role_contract(resolved)
    aliases = contract.get("aliases", []) if isinstance(contract.get("aliases"), list) else []
    return unique_texts([actor_role, resolved, *aliases])


def actor_owns_step(actor_role: str, step: dict[str, Any]) -> bool:
    step_role = maybe_text(step.get("role"))
    if not step_role:
        return False
    return normalize_actor_role(step_role) == (normalize_actor_role(actor_role) or maybe_text(actor_role))


def load_existing_execution(output_path: Path) -> dict[str, Any]:
    if not output_path.exists():
        return {}
    try:
        payload = read_json_object(output_path)
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def existing_execution_statuses(existing_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    statuses = (
        existing_payload.get("statuses", [])
        if isinstance(existing_payload.get("statuses"), list)
        else []
    )
    by_step: dict[str, dict[str, Any]] = {}
    for status in statuses:
        if not isinstance(status, dict):
            continue
        step_id = maybe_text(status.get("step_id"))
        if step_id:
            by_step[step_id] = status
    return by_step


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
            "normalization_status": "receipt-only",
            "query_status": "raw-artifact-only",
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
    actor_role: str,
    resolved_actor_role: str,
) -> tuple[Path, dict[str, Any] | None, dict[str, Any]]:
    step_id = maybe_text(step.get("step_id"))
    step_kind = maybe_text(step.get("step_kind")) or "import"
    source_skill = maybe_text(step.get("source_skill"))
    raw_artifact_path = resolved_artifact_path(step)
    fetch_details: dict[str, Any] | None = None

    if step_kind == "import":
        raw_artifact_path = copy_import_artifact(step)
    elif step_kind == "detached-fetch":
        raw_artifact_path, fetch_details = execute_detached_fetch_step(
            step,
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
            resolved_actor_role=resolved_actor_role,
        )
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
        "actor_role": actor_role,
        "resolved_actor_role": resolved_actor_role,
        "source_skill": source_skill,
        "artifact_path": str(raw_artifact_path),
        "artifact_dir": maybe_text(step.get("artifact_dir")),
        "artifact_sha256": raw_sha256,
        "fetch_contract": fetch_contract,
        "next_step_hints": next_step_hints(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
        ),
    }
    if step_kind == "import":
        queue_status["source_artifact_path"] = maybe_text(step.get("source_artifact_path"))
    if fetch_details is not None:
        fetch_details["next_step_hints"] = next_step_hints(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
        )
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
        normalizer_artifact_path = raw_artifact_path
        pre_normalization: dict[str, Any] = {}
        if source_skill == "fetch-regulationsgov-attachments" and normalizer_skill == "normalize-regulationsgov-attachment-text":
            extraction_manifest_path, extraction_payload = run_attachment_text_extraction(raw_artifact_path)
            normalizer_artifact_path = extraction_manifest_path
            pre_normalization = {
                "skill": "extract-document-text",
                "input_artifact_path": str(raw_artifact_path),
                "output_manifest_path": str(extraction_manifest_path),
                "status": maybe_text(extraction_payload.get("status")),
                "record_count": extraction_payload.get("record_count"),
                "completed_count": extraction_payload.get("completed_count"),
                "limited_count": extraction_payload.get("limited_count"),
            }
            normalizer_args = [
                item
                for item in normalizer_args
                if item != str(raw_artifact_path)
            ]
        payload = run_json_script(
            script_path,
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--artifact-path",
            str(normalizer_artifact_path),
            *normalizer_args,
        )
        if pre_normalization:
            payload["pre_normalization"] = pre_normalization
        return payload


def receipt_step_id(receipt_ref: str) -> str:
    return "receipt-normalize-" + stable_hash(receipt_ref)[:16]


def strip_json_ref(value: str) -> str:
    text = maybe_text(value)
    if ".json:" in text:
        return text.split(".json:", 1)[0] + ".json"
    return text


def resolve_receipt_path(run_dir: Path, receipt_ref: str) -> Path:
    ref = strip_json_ref(receipt_ref)
    candidate = Path(ref).expanduser()
    if candidate.is_absolute() or candidate.suffix == ".json" or "/" in ref:
        if not candidate.is_absolute():
            candidate = run_dir / candidate
        return candidate.resolve()
    return (run_dir / "runtime" / "receipts" / f"{ref}.json").resolve()


def artifact_path_candidates_from_receipt(receipt: dict[str, Any]) -> list[str]:
    candidates: list[Any] = []

    def collect_ref(value: Any) -> None:
        if isinstance(value, dict):
            candidates.append(value.get("artifact_path"))
            candidates.append(value.get("path"))
            candidates.append(value.get("output_path"))
            candidates.append(value.get("artifact_ref"))
        elif isinstance(value, str):
            candidates.append(value)

    for ref in receipt.get("artifact_refs", []) if isinstance(receipt.get("artifact_refs"), list) else []:
        collect_ref(ref)

    summary = receipt.get("summary") if isinstance(receipt.get("summary"), dict) else {}
    collect_ref(summary)

    skill_payload = receipt.get("skill_payload") if isinstance(receipt.get("skill_payload"), dict) else {}
    collect_ref(skill_payload.get("output_path"))
    collect_ref(skill_payload.get("output_file"))
    for ref in skill_payload.get("artifact_refs", []) if isinstance(skill_payload.get("artifact_refs"), list) else []:
        collect_ref(ref)
    collect_ref(skill_payload.get("summary") if isinstance(skill_payload.get("summary"), dict) else {})

    for artifacts in (
        skill_payload.get("artifacts"),
        skill_payload.get("payload", {}).get("artifacts")
        if isinstance(skill_payload.get("payload"), dict)
        else None,
    ):
        if isinstance(artifacts, dict):
            candidates.extend(artifacts.values())
        elif isinstance(artifacts, list):
            candidates.extend(artifacts)

    return unique_texts([strip_json_ref(item) for item in candidates if maybe_text(item)])


def existing_artifact_path_from_receipt(receipt: dict[str, Any], *, run_dir: Path) -> Path | None:
    for raw_candidate in artifact_path_candidates_from_receipt(receipt):
        candidate = Path(raw_candidate).expanduser()
        if not candidate.is_absolute():
            candidate = run_dir / candidate
        candidate = candidate.resolve()
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def normalizable_payload_from_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    skill_payload = receipt.get("skill_payload") if isinstance(receipt.get("skill_payload"), dict) else {}
    nested_payload = skill_payload.get("payload") if isinstance(skill_payload.get("payload"), dict) else {}
    data_payload = skill_payload.get("data") if isinstance(skill_payload.get("data"), dict) else {}
    for candidate in (nested_payload, data_payload, skill_payload):
        if not isinstance(candidate, dict):
            continue
        for key in ("records", "articles", "downloads"):
            if key in candidate:
                return candidate
    return skill_payload if isinstance(skill_payload, dict) else dict(receipt)


def materialize_receipt_artifact(
    *,
    run_dir: Path,
    round_id: str,
    receipt: dict[str, Any],
    receipt_path: Path,
    source_skill: str,
) -> tuple[Path, dict[str, Any]]:
    existing_path = existing_artifact_path_from_receipt(receipt, run_dir=run_dir)
    if existing_path is not None:
        return existing_path, {
            "mode": "existing-artifact",
            "receipt_path": str(receipt_path),
            "artifact_path": str(existing_path),
        }

    receipt_id = maybe_text(receipt.get("receipt_id")) or receipt_path.stem
    safe_source = "".join(char if char.isalnum() else "-" for char in source_skill).strip("-") or "source"
    artifact_path = (
        run_dir
        / "raw"
        / round_id
        / "receipt-materialized"
        / f"{receipt_id}-{safe_source}.json"
    ).resolve()
    payload = normalizable_payload_from_receipt(receipt)
    write_json_file(artifact_path, payload)
    return artifact_path, {
        "mode": "materialized-from-receipt-payload",
        "receipt_path": str(receipt_path),
        "artifact_path": str(artifact_path),
    }


def execute_receipt_normalization(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    receipt_ref: str,
    actor_role: str,
    resolved_actor_role: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    receipt_path = resolve_receipt_path(run_dir, receipt_ref)
    receipt = read_json_object(receipt_path)
    receipt_id = maybe_text(receipt.get("receipt_id")) or receipt_path.stem
    skill_payload = receipt.get("skill_payload") if isinstance(receipt.get("skill_payload"), dict) else {}
    source_skill = maybe_text(receipt.get("skill_name")) or maybe_text(skill_payload.get("source_skill"))
    if not source_skill:
        nested = skill_payload.get("payload")
        if isinstance(nested, dict):
            source_skill = maybe_text(nested.get("source_skill"))
    if not source_skill:
        raise RuntimeError(f"Could not determine source skill for receipt {receipt_ref}.")

    normalizer_skill = source_normalizer_skill(source_skill)
    raw_artifact_path, materialization = materialize_receipt_artifact(
        run_dir=run_dir,
        round_id=round_id,
        receipt=receipt,
        receipt_path=receipt_path,
        source_skill=source_skill,
    )
    step = {
        "step_id": receipt_step_id(receipt_id),
        "step_kind": "receipt-normalize",
        "role": resolved_actor_role,
        "source_skill": source_skill,
        "normalizer_skill": normalizer_skill,
        "normalizer_args": [],
    }
    payload = run_normalizer_for_step(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        step=step,
        raw_artifact_path=raw_artifact_path,
    )
    canonical_ids = (
        [maybe_text(item) for item in payload.get("canonical_ids", []) if maybe_text(item)]
        if isinstance(payload.get("canonical_ids"), list)
        else []
    )
    artifact_refs = (
        [item for item in payload.get("artifact_refs", []) if isinstance(item, dict)]
        if isinstance(payload.get("artifact_refs"), list)
        else []
    )
    canonical_count = len(canonical_ids)
    artifact_ref_count = len(artifact_refs)
    normalization_status = "normalized-signal-plane" if canonical_count > 0 else "receipt-only"
    raw_sha256 = file_sha256(raw_artifact_path)
    warnings = [
        item
        for item in payload.get("warnings", [])
        if isinstance(item, dict) and maybe_text(item.get("message"))
    ] if isinstance(payload.get("warnings"), list) else []
    if materialization.get("mode") == "materialized-from-receipt-payload":
        warnings.append(
            {
                "code": "receipt-payload-materialized",
                "message": (
                    "No existing raw artifact path was found in the receipt; "
                    "the receipt payload was materialized as a raw artifact before normalization."
                ),
            }
        )
    status = {
        "step_id": maybe_text(step.get("step_id")),
        "step_kind": "receipt-normalize",
        "status": "completed",
        "components": {
            "queue_runner": "receipt-reused",
            "normalizer_runner": "completed",
            "execution_receipt": "pending",
        },
        "role": resolved_actor_role,
        "actor_role": actor_role,
        "resolved_actor_role": resolved_actor_role,
        "source_skill": source_skill,
        "normalizer_skill": normalizer_skill,
        "artifact_path": str(raw_artifact_path),
        "artifact_sha256": raw_sha256,
        "source_receipt_id": receipt_id,
        "source_receipt_ref": str(receipt_path),
        "receipt_id": maybe_text(payload.get("receipt_id")),
        "batch_id": maybe_text(payload.get("batch_id")),
        "canonical_count": canonical_count,
        "artifact_ref_count": artifact_ref_count,
        "normalization_status": normalization_status,
        "normalized_signal_refs": canonical_ids,
        "normalized_batch_ref": maybe_text(payload.get("batch_id")),
        "warning_count": len(warnings),
        "receipt_materialization": materialization,
        "queue_runner": {
            "status": "completed",
            "component": "receipt-reuse",
            "source_receipt_id": receipt_id,
            "source_receipt_ref": str(receipt_path),
            "artifact_path": str(raw_artifact_path),
            "artifact_sha256": raw_sha256,
        },
        "normalizer_runner": {
            "status": maybe_text(payload.get("status")) or "completed",
            "normalization_status": normalization_status,
            "receipt_id": maybe_text(payload.get("receipt_id")),
            "batch_id": maybe_text(payload.get("batch_id")),
            "actor_role": actor_role,
            "resolved_actor_role": resolved_actor_role,
            "canonical_count": canonical_count,
            "artifact_ref_count": artifact_ref_count,
        },
        "next_step_hints": next_step_hints(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
        ),
    }
    return status, {**payload, "warnings": warnings}


def execute_import_step(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    step: dict[str, Any],
    actor_role: str,
    resolved_actor_role: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_artifact_path, fetch_details, queue_status = execute_queue_step(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        step=step,
        actor_role=actor_role,
        resolved_actor_role=resolved_actor_role,
    )
    payload = run_normalizer_for_step(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        step=step,
        raw_artifact_path=raw_artifact_path,
    )
    canonical_count = len(payload.get("canonical_ids", [])) if isinstance(payload.get("canonical_ids"), list) else 0
    artifact_ref_count = len(payload.get("artifact_refs", [])) if isinstance(payload.get("artifact_refs"), list) else 0
    normalization_status = "normalized-signal-plane" if canonical_count > 0 else "receipt-only"
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
        "actor_role": actor_role,
        "resolved_actor_role": resolved_actor_role,
        "source_skill": maybe_text(step.get("source_skill")),
        "normalizer_skill": maybe_text(step.get("normalizer_skill")),
        "artifact_path": str(raw_artifact_path),
        "artifact_dir": maybe_text(step.get("artifact_dir")),
        "artifact_sha256": maybe_text(queue_status.get("artifact_sha256")),
        "receipt_id": maybe_text(payload.get("receipt_id")),
        "batch_id": maybe_text(payload.get("batch_id")),
        "canonical_count": canonical_count,
        "artifact_ref_count": artifact_ref_count,
        "normalization_status": normalization_status,
        "normalized_signal_refs": [
            maybe_text(item)
            for item in payload.get("canonical_ids", [])
            if maybe_text(item)
        ]
        if isinstance(payload.get("canonical_ids"), list)
        else [],
        "normalized_batch_ref": maybe_text(payload.get("batch_id")),
        "warning_count": len(payload.get("warnings", [])) if isinstance(payload.get("warnings"), list) else 0,
        "queue_runner": queue_status,
        "normalizer_runner": {
            "status": maybe_text(payload.get("status")) or "completed",
            "normalization_status": normalization_status,
            "receipt_id": maybe_text(payload.get("receipt_id")),
            "batch_id": maybe_text(payload.get("batch_id")),
            "actor_role": actor_role,
            "resolved_actor_role": resolved_actor_role,
            "canonical_count": canonical_count,
            "artifact_ref_count": artifact_ref_count,
        },
        "next_step_hints": next_step_hints(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=actor_role,
        ),
    }
    if maybe_text(step.get("step_kind")) == "import":
        status["source_artifact_path"] = maybe_text(step.get("source_artifact_path"))
    if fetch_details is not None:
        status["detached_fetch"] = fetch_details
    if isinstance(payload.get("pre_normalization"), dict):
        status["normalizer_runner"]["pre_normalization"] = payload["pre_normalization"]
    return status, payload


def build_execution_payload(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    actor_role: str,
    resolved_actor_role: str,
    actor_plan_roles: list[str],
    plan_path: Path,
    plan_sha256: str,
    statuses: list[dict[str, Any]],
    normalized_receipt_ids: list[str],
    normalized_artifact_refs: list[dict[str, Any]],
    failure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalization_state = status_normalization_state(statuses)
    payload = {
        "schema_version": "ingress-import-v2",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "last_actor_role": actor_role,
        "last_resolved_actor_role": resolved_actor_role,
        "last_actor_plan_roles": actor_plan_roles,
        "execution_id": "import-execution-" + stable_hash(run_id, round_id, plan_sha256, len(statuses))[:12],
        "plan_path": str(plan_path),
        "plan_sha256": plan_sha256,
        "step_count": len(statuses),
        "completed_count": len([item for item in statuses if maybe_text(item.get("status")) == "completed"]),
        "failed_count": len([item for item in statuses if maybe_text(item.get("status")) == "failed"]),
        "statuses": statuses,
        "normalized_receipt_ids": unique_texts(normalized_receipt_ids),
        "normalized_artifact_refs": normalized_artifact_refs,
        **normalization_state,
        "next_step_hints": next_step_hints(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            actor_role=resolved_actor_role or actor_role,
        ),
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


def ordered_statuses_for_plan(
    steps: list[dict[str, Any]],
    statuses_by_step: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for step in steps:
        step_id = maybe_text(step.get("step_id"))
        if step_id and step_id in statuses_by_step:
            ordered.append(statuses_by_step[step_id])
            seen.add(step_id)
    for step_id, status in statuses_by_step.items():
        if step_id not in seen:
            ordered.append(status)
    return ordered


def import_fetch_execution_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    *,
    actor_role: str,
    receipt_refs: list[str] | None = None,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    plan_path = (run_dir_path / "runtime" / f"fetch_plan_{round_id}.json").resolve()
    output_path = (run_dir_path / "runtime" / f"import_execution_{round_id}.json").resolve()
    receipt_refs = unique_texts(list(receipt_refs or []))

    normalized_actor_role = normalize_actor_role(actor_role) or maybe_text(actor_role)
    if not normalized_actor_role:
        raise RuntimeError(
            "normalize-fetch-execution requires --actor-role or OPENCLAW_ACTOR_ROLE "
            "so fetch and normalization can be limited to the actor's assigned steps."
        )
    plan_role_labels = actor_role_labels(normalized_actor_role)
    plan_missing = not plan_path.exists()
    if plan_missing:
        plan = {
            "schema_version": "fetch-plan-missing-placeholder-v1",
            "run_id": run_id,
            "round_id": round_id,
            "steps": [],
        }
    else:
        plan = read_json_object(plan_path)
        ensure_fetch_plan_inputs_match(run_dir=run_dir_path, round_id=round_id, plan=plan)
    steps = [item for item in plan.get("steps", []) if isinstance(item, dict)] if isinstance(plan.get("steps"), list) else []
    owned_steps = [step for step in steps if actor_owns_step(normalized_actor_role, step)]
    plan_sha256 = file_sha256(plan_path) if plan_path.exists() else stable_hash("missing-fetch-plan", run_id, round_id)

    existing_payload = load_existing_execution(output_path)
    statuses_by_step = existing_execution_statuses(existing_payload)
    normalized_receipt_ids: list[str] = unique_texts(
        existing_payload.get("normalized_receipt_ids", [])
        if isinstance(existing_payload.get("normalized_receipt_ids"), list)
        else []
    )
    normalized_artifact_refs: list[dict[str, Any]] = [
        item
        for item in (
            existing_payload.get("normalized_artifact_refs", [])
            if isinstance(existing_payload.get("normalized_artifact_refs"), list)
            else []
        )
        if isinstance(item, dict)
    ]
    warnings: list[dict[str, str]] = []
    if plan_missing and receipt_refs:
        warnings.append(
            {
                "code": "receipt-driven-normalization-without-fetch-plan",
                "message": (
                    "No prepared fetch plan was found; only the supplied runtime receipts "
                    "were normalized."
                ),
            }
        )
    if not owned_steps and not receipt_refs:
        warnings.append(
            {
                "code": "no-owned-fetch-plan-steps",
                "message": (
                    f"Actor role {normalized_actor_role} has no owned fetch-plan steps "
                    f"for plan roles {', '.join(plan_role_labels)}."
                ),
            }
        )

    newly_completed_step_ids: list[str] = []
    skipped_step_ids: list[str] = []
    newly_completed_receipt_refs: list[str] = []
    skipped_receipt_refs: list[str] = []
    for step in owned_steps:
        step_id = maybe_text(step.get("step_id")) or "unknown-step"
        existing_status = statuses_by_step.get(step_id)
        if isinstance(existing_status, dict) and maybe_text(existing_status.get("status")) == "completed":
            skipped_step_ids.append(step_id)
            continue
        try:
            status, payload = execute_import_step(
                run_dir=run_dir_path,
                run_id=run_id,
                round_id=round_id,
                step=step,
                actor_role=actor_role,
                resolved_actor_role=normalized_actor_role,
            )
            statuses_by_step[step_id] = status
            newly_completed_step_ids.append(step_id)
            normalized_receipt_ids.append(maybe_text(payload.get("receipt_id")))
            if isinstance(payload.get("artifact_refs"), list):
                normalized_artifact_refs.extend(item for item in payload["artifact_refs"] if isinstance(item, dict))
            if isinstance(payload.get("warnings"), list):
                warnings.extend(item for item in payload["warnings"] if isinstance(item, dict) and maybe_text(item.get("message")))
        except Exception as exc:  # noqa: BLE001
            failed_status = {
                "step_id": step_id,
                "step_kind": maybe_text(step.get("step_kind")) or "import",
                "status": "failed",
                "role": maybe_text(step.get("role")),
                "actor_role": actor_role,
                "resolved_actor_role": normalized_actor_role,
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
            statuses_by_step[step_id] = failed_status
            statuses = ordered_statuses_for_plan(steps, statuses_by_step)
            partial_payload = build_execution_payload(
                run_dir=run_dir_path,
                run_id=run_id,
                round_id=round_id,
                actor_role=actor_role,
                resolved_actor_role=normalized_actor_role,
                actor_plan_roles=plan_role_labels,
                plan_path=plan_path,
                plan_sha256=plan_sha256,
                statuses=statuses,
                normalized_receipt_ids=normalized_receipt_ids,
                normalized_artifact_refs=normalized_artifact_refs,
                failure=failure_payload,
            )
            write_json_file(output_path, partial_payload)
            raise RuntimeError(f"Import execution failed at {step_id}: {exc}") from exc

    for receipt_ref in receipt_refs:
        try:
            receipt_path = resolve_receipt_path(run_dir_path, receipt_ref)
            receipt_payload = read_json_object(receipt_path)
            receipt_id = maybe_text(receipt_payload.get("receipt_id")) or receipt_path.stem
            step_id = receipt_step_id(receipt_id)
            existing_status = statuses_by_step.get(step_id)
            if (
                isinstance(existing_status, dict)
                and maybe_text(existing_status.get("status")) == "completed"
                and int(existing_status.get("canonical_count") or 0) > 0
            ):
                skipped_receipt_refs.append(receipt_ref)
                continue
            status, payload = execute_receipt_normalization(
                run_dir=run_dir_path,
                run_id=run_id,
                round_id=round_id,
                receipt_ref=str(receipt_path),
                actor_role=actor_role,
                resolved_actor_role=normalized_actor_role,
            )
            statuses_by_step[step_id] = status
            newly_completed_receipt_refs.append(receipt_ref)
            normalized_receipt_ids.append(maybe_text(payload.get("receipt_id")))
            if isinstance(payload.get("artifact_refs"), list):
                normalized_artifact_refs.extend(item for item in payload["artifact_refs"] if isinstance(item, dict))
            if isinstance(payload.get("warnings"), list):
                warnings.extend(item for item in payload["warnings"] if isinstance(item, dict) and maybe_text(item.get("message")))
        except Exception as exc:  # noqa: BLE001
            step_id = receipt_step_id(receipt_ref)
            failed_status = {
                "step_id": step_id,
                "step_kind": "receipt-normalize",
                "status": "failed",
                "role": normalized_actor_role,
                "actor_role": actor_role,
                "resolved_actor_role": normalized_actor_role,
                "source_receipt_ref": receipt_ref,
                "reason": str(exc),
            }
            statuses_by_step[step_id] = failed_status
            statuses = ordered_statuses_for_plan(steps, statuses_by_step)
            partial_payload = build_execution_payload(
                run_dir=run_dir_path,
                run_id=run_id,
                round_id=round_id,
                actor_role=actor_role,
                resolved_actor_role=normalized_actor_role,
                actor_plan_roles=plan_role_labels,
                plan_path=plan_path,
                plan_sha256=plan_sha256,
                statuses=statuses,
                normalized_receipt_ids=normalized_receipt_ids,
                normalized_artifact_refs=normalized_artifact_refs,
                failure={"step_id": step_id, "receipt_ref": receipt_ref, "message": str(exc)},
            )
            write_json_file(output_path, partial_payload)
            raise RuntimeError(f"Receipt-driven normalization failed at {receipt_ref}: {exc}") from exc

    statuses = ordered_statuses_for_plan(steps, statuses_by_step)
    payload = build_execution_payload(
        run_dir=run_dir_path,
        run_id=run_id,
        round_id=round_id,
        actor_role=actor_role,
        resolved_actor_role=normalized_actor_role,
        actor_plan_roles=plan_role_labels,
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
    summary = {
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "output_path": str(output_path),
        "execution_id": payload["execution_id"],
        "normalized_step_count": payload["completed_count"],
        "normalization_status": payload.get("normalization_status"),
        "normalized_signal_step_count": payload.get("normalized_signal_step_count"),
        "receipt_only_step_count": payload.get("receipt_only_step_count"),
        "failed_step_count": payload["failed_count"],
        "actor_role": actor_role,
        "resolved_actor_role": normalized_actor_role,
        "actor_plan_roles": plan_role_labels,
        "owned_step_count": len(owned_steps),
        "receipt_ref_count": len(receipt_refs),
        "newly_completed_step_count": len(newly_completed_step_ids),
        "newly_completed_receipt_count": len(newly_completed_receipt_refs),
        "skipped_completed_step_count": len(skipped_step_ids),
        "skipped_completed_receipt_count": len(skipped_receipt_refs),
        "total_plan_step_count": len(steps),
    }
    board_handoff = {
        "candidate_ids": [payload["execution_id"]],
        "evidence_refs": artifact_refs,
        "gap_hints": [item.get("message", "") for item in warnings[:3] if maybe_text(item.get("message"))],
        "challenge_hints": [],
        "next_query_commands": payload.get("next_step_hints", {}).get("query_commands", {})
        if isinstance(payload.get("next_step_hints"), dict)
        else {},
        "suggested_next_skills": [
            "query-public-signals",
            "query-formal-signals",
            "query-environment-signals",
        ],
    }
    result_payload = {
        "status": "completed",
        "summary": summary,
        "batch_id": "ingressbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_path.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [payload["execution_id"]],
        "warnings": warnings,
        "execution_components": payload.get("execution_components", {}),
        "next_step_hints": payload.get("next_step_hints", {}),
        "board_handoff": board_handoff,
    }
    result_payload["receipt_id"] = (
        "ingress-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, pretty_json(result_payload, pretty=False))[:20]
    )
    return result_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fetch-plan queue, normalization, and execution receipt components.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--actor-role", default="", help="Actor executing this role-owned fetch/normalize slice.")
    parser.add_argument(
        "--receipt-id",
        action="append",
        default=[],
        help="Runtime receipt id to normalize without requiring a fetch-plan step.",
    )
    parser.add_argument(
        "--receipt-path",
        action="append",
        default=[],
        help="Runtime receipt path to normalize without requiring a fetch-plan step.",
    )
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    actor_role = maybe_text(args.actor_role) or maybe_text(os.environ.get("OPENCLAW_RESOLVED_ACTOR_ROLE")) or maybe_text(os.environ.get("OPENCLAW_ACTOR_ROLE"))
    payload = import_fetch_execution_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        actor_role=actor_role,
        receipt_refs=[*args.receipt_id, *args.receipt_path],
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
