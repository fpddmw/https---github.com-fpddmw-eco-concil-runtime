"""Append-only tamper-evident audit receipts for eco-council rounds."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from eco_council_runtime.controller.io import exclusive_file_lock, file_sha256, maybe_text, utc_now_iso
from eco_council_runtime.controller.paths import (
    audit_chain_ledger_path,
    audit_chain_objects_dir,
    cards_active_path,
    claim_candidates_path,
    claim_submissions_path,
    claims_active_path,
    current_run_id,
    data_plane_execution_path,
    decision_draft_path,
    decision_target_path,
    evidence_adjudication_path,
    fetch_execution_path,
    fetch_plan_path,
    investigation_actions_path,
    investigation_state_path,
    isolated_active_path,
    matching_adjudication_path,
    matching_result_path,
    observation_candidates_path,
    observation_submissions_path,
    observations_active_path,
    remands_open_path,
    report_target_path,
    require_round_id,
    reporting_handoff_path,
    shared_claims_path,
    shared_evidence_path,
    shared_observations_path,
)

AUDIT_CHAIN_SCHEMA_VERSION = "1.0.0"
VALID_PHASE_KINDS = {"import", "fetch", "normalize", "match", "decision"}


def stable_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_sha256(data: Any) -> str:
    return hashlib.sha256(stable_json(data).encode("utf-8")).hexdigest()


def read_jsonl(path: Path) -> list[Any]:
    if not path.exists():
        return []
    rows: list[Any] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def path_is_within(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def display_path_for_artifact(*, run_dir: Path, artifact_path: Path) -> str:
    artifact_path = artifact_path.expanduser().resolve()
    run_dir = run_dir.expanduser().resolve()
    if path_is_within(run_dir, artifact_path):
        return str(artifact_path.relative_to(run_dir))
    return str(artifact_path)


def resolved_path_from_display(*, run_dir: Path, display_path: str) -> Path:
    candidate = Path(display_path)
    if candidate.is_absolute():
        return candidate.expanduser().resolve()
    return (run_dir.expanduser().resolve() / candidate).resolve()


def safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def ensure_audit_chain_ready(run_dir: Path, round_id: str) -> dict[str, Path]:
    normalized_round_id = require_round_id(round_id)
    ledger_path = audit_chain_ledger_path(run_dir, normalized_round_id)
    objects_dir = audit_chain_objects_dir(run_dir, normalized_round_id)
    objects_dir.mkdir(parents=True, exist_ok=True)
    if not ledger_path.exists():
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text("", encoding="utf-8")
    return {
        "ledger_path": ledger_path,
        "objects_dir": objects_dir,
    }


def write_snapshot_blob(objects_dir: Path, sha256: str, source_path: Path) -> Path:
    target_path = objects_dir / f"{sha256}.blob"
    if target_path.exists():
        return target_path
    fd, temp_path = tempfile.mkstemp(prefix=f".{sha256}.tmp-", dir=str(objects_dir))
    try:
        with os.fdopen(fd, "wb") as handle, source_path.open("rb") as source:
            shutil.copyfileobj(source, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target_path)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise
    return target_path


def build_artifact_ref(*, run_dir: Path, objects_dir: Path, spec: dict[str, Any]) -> dict[str, Any] | None:
    path_value = spec.get("path")
    if path_value is None:
        return None
    artifact_path = Path(str(path_value)).expanduser().resolve()
    if not artifact_path.exists() or not artifact_path.is_file():
        if bool(spec.get("optional")):
            return None
        raise ValueError(f"Audit artifact path is missing or not a file: {artifact_path}")
    sha256 = file_sha256(artifact_path)
    snapshot_path = write_snapshot_blob(objects_dir, sha256, artifact_path)
    required_current = spec.get("required_current")
    if required_current is None:
        required_current = path_is_within(run_dir.expanduser().resolve(), artifact_path)
    return {
        "label": maybe_text(spec.get("label")) or artifact_path.name,
        "artifact_kind": maybe_text(spec.get("artifact_kind")) or "file",
        "artifact_path": display_path_for_artifact(run_dir=run_dir, artifact_path=artifact_path),
        "snapshot_path": display_path_for_artifact(run_dir=run_dir, artifact_path=snapshot_path),
        "sha256": sha256,
        "size_bytes": int(artifact_path.stat().st_size),
        "required_current": bool(required_current),
    }


def receipt_key_for(*, phase_kind: str, event_kind: str, round_id: str, artifact_refs: list[dict[str, Any]], details: dict[str, Any]) -> str:
    return stable_sha256(
        {
            "phase_kind": maybe_text(phase_kind),
            "event_kind": maybe_text(event_kind),
            "round_id": maybe_text(round_id),
            "artifacts": [
                {
                    "artifact_path": maybe_text(item.get("artifact_path")),
                    "sha256": maybe_text(item.get("sha256")),
                    "label": maybe_text(item.get("label")),
                }
                for item in artifact_refs
                if isinstance(item, dict)
            ],
            "details": details,
        }
    )


def append_audit_receipt(
    *,
    run_dir: Path,
    round_id: str,
    phase_kind: str,
    event_kind: str,
    artifact_specs: list[dict[str, Any]],
    details: dict[str, Any] | None = None,
    actor_role: str = "",
) -> dict[str, Any]:
    normalized_round_id = require_round_id(round_id)
    if phase_kind not in VALID_PHASE_KINDS:
        raise ValueError(f"Unsupported audit phase_kind: {phase_kind}")
    paths = ensure_audit_chain_ready(run_dir, normalized_round_id)
    ledger_path = paths["ledger_path"]
    objects_dir = paths["objects_dir"]
    details_payload = details if isinstance(details, dict) else {}
    artifact_refs = [
        artifact_ref
        for artifact_ref in (
            build_artifact_ref(run_dir=run_dir, objects_dir=objects_dir, spec=spec)
            for spec in artifact_specs
            if isinstance(spec, dict)
        )
        if isinstance(artifact_ref, dict)
    ]
    if not artifact_refs:
        raise ValueError(f"Audit receipt {event_kind} does not have any materialized artifacts.")
    receipt_key = receipt_key_for(
        phase_kind=phase_kind,
        event_kind=event_kind,
        round_id=normalized_round_id,
        artifact_refs=artifact_refs,
        details=details_payload,
    )
    with exclusive_file_lock(ledger_path):
        existing = [item for item in read_jsonl(ledger_path) if isinstance(item, dict)]
        duplicate = next((item for item in existing if maybe_text(item.get("receipt_key")) == receipt_key), None)
        if isinstance(duplicate, dict):
            return {
                "recorded": False,
                "receipt": duplicate,
                "ledger_path": str(ledger_path),
            }
        previous_sha256 = maybe_text(existing[-1].get("receipt_sha256")) if existing else ""
        chain_index = len(existing) + 1
        receipt = {
            "schema_version": AUDIT_CHAIN_SCHEMA_VERSION,
            "receipt_id": f"audit-receipt-{normalized_round_id}-{chain_index:04d}",
            "receipt_key": receipt_key,
            "run_id": current_run_id(run_dir),
            "round_id": normalized_round_id,
            "phase_kind": phase_kind,
            "event_kind": maybe_text(event_kind),
            "actor_role": maybe_text(actor_role),
            "recorded_at_utc": utc_now_iso(),
            "chain_index": chain_index,
            "prev_receipt_sha256": previous_sha256,
            "artifact_refs": artifact_refs,
            "details": details_payload,
        }
        receipt["receipt_sha256"] = stable_sha256(receipt)
        with ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(stable_json(receipt) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    return {
        "recorded": True,
        "receipt": receipt,
        "ledger_path": str(ledger_path),
    }


def record_import_receipt(
    *,
    run_dir: Path,
    round_id: str,
    imported_kind: str,
    source_path: Path,
    target_path: Path,
    role: str = "",
    stage_after_import: str = "",
    derived_artifact_specs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_required = path_is_within(run_dir.expanduser().resolve(), source_path.expanduser().resolve())
    artifact_specs: list[dict[str, Any]] = [
        {
            "path": source_path,
            "label": "import-source",
            "artifact_kind": "input",
            "required_current": source_required,
        },
        {
            "path": target_path,
            "label": "canonical-target",
            "artifact_kind": "canonical",
            "required_current": True,
        },
    ]
    if isinstance(derived_artifact_specs, list):
        artifact_specs.extend(item for item in derived_artifact_specs if isinstance(item, dict))
    return append_audit_receipt(
        run_dir=run_dir,
        round_id=round_id,
        phase_kind="import",
        event_kind=f"{maybe_text(imported_kind)}-import",
        actor_role=role,
        artifact_specs=artifact_specs,
        details={
            "imported_kind": maybe_text(imported_kind),
            "stage_after_import": maybe_text(stage_after_import),
            "source_equals_target": source_path.expanduser().resolve() == target_path.expanduser().resolve(),
            "derived_artifact_count": len(artifact_specs) - 2,
        },
    )


def fetch_artifact_specs_from_payload(*, run_dir: Path, round_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = [
        {"path": fetch_plan_path(run_dir, round_id), "label": "fetch-plan", "artifact_kind": "plan", "required_current": True},
        {
            "path": fetch_execution_path(run_dir, round_id),
            "label": "fetch-execution",
            "artifact_kind": "execution",
            "required_current": True,
        },
    ]
    statuses = payload.get("statuses") if isinstance(payload.get("statuses"), list) else []
    for status in statuses:
        if not isinstance(status, dict):
            continue
        step_id = maybe_text(status.get("step_id")) or "fetch-step"
        for field_name, label_prefix, artifact_kind in (
            ("artifact_path", "artifact", "raw-artifact"),
            ("stdout_path", "stdout", "stdout-log"),
            ("stderr_path", "stderr", "stderr-log"),
        ):
            value = maybe_text(status.get(field_name))
            if not value:
                continue
            specs.append(
                {
                    "path": Path(value),
                    "label": f"{step_id}-{label_prefix}",
                    "artifact_kind": artifact_kind,
                    "required_current": field_name == "artifact_path",
                    "optional": field_name != "artifact_path",
                }
            )
    return specs


def record_fetch_phase_receipt(*, run_dir: Path, round_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return append_audit_receipt(
        run_dir=run_dir,
        round_id=round_id,
        phase_kind="fetch",
        event_kind="fetch-execution-materialized",
        artifact_specs=fetch_artifact_specs_from_payload(run_dir=run_dir, round_id=round_id, payload=payload),
        details={
            "step_count": int(payload.get("step_count") or 0),
            "completed_count": int(payload.get("completed_count") or 0),
            "failed_count": int(payload.get("failed_count") or 0),
            "plan_sha256": maybe_text(payload.get("plan_sha256")),
        },
    )


def record_normalize_phase_receipt(*, run_dir: Path, round_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return append_audit_receipt(
        run_dir=run_dir,
        round_id=round_id,
        phase_kind="normalize",
        event_kind="data-plane-materialized",
        artifact_specs=[
            {
                "path": data_plane_execution_path(run_dir, round_id),
                "label": "data-plane-execution",
                "artifact_kind": "execution",
                "required_current": True,
            },
            {
                "path": reporting_handoff_path(run_dir, round_id),
                "label": "reporting-handoff",
                "artifact_kind": "handoff",
                "required_current": True,
                "optional": True,
            },
            {"path": claim_candidates_path(run_dir, round_id), "label": "claim-candidates", "artifact_kind": "normalized", "required_current": True},
            {
                "path": observation_candidates_path(run_dir, round_id),
                "label": "observation-candidates",
                "artifact_kind": "normalized",
                "required_current": True,
            },
            {"path": claim_submissions_path(run_dir, round_id), "label": "claim-submissions", "artifact_kind": "canonical", "required_current": True},
            {
                "path": observation_submissions_path(run_dir, round_id),
                "label": "observation-submissions",
                "artifact_kind": "canonical",
                "required_current": True,
            },
            {"path": shared_claims_path(run_dir, round_id), "label": "shared-claims", "artifact_kind": "canonical", "required_current": True},
            {
                "path": shared_observations_path(run_dir, round_id),
                "label": "shared-observations",
                "artifact_kind": "canonical",
                "required_current": True,
            },
            {"path": claims_active_path(run_dir, round_id), "label": "claims-active", "artifact_kind": "library-view", "required_current": True},
            {
                "path": observations_active_path(run_dir, round_id),
                "label": "observations-active",
                "artifact_kind": "library-view",
                "required_current": True,
            },
            {
                "path": investigation_state_path(run_dir, round_id),
                "label": "investigation-state",
                "artifact_kind": "derived-state",
                "required_current": True,
            },
            {
                "path": investigation_actions_path(run_dir, round_id),
                "label": "investigation-actions",
                "artifact_kind": "derived-state",
                "required_current": True,
            },
        ],
        details={
            "step_count": int(payload.get("step_count") or 0),
            "completed_count": int(payload.get("completed_count") or 0),
            "failed_count": int(payload.get("failed_count") or 0),
        },
    )


def record_match_phase_receipt(
    *,
    run_dir: Path,
    round_id: str,
    evidence_count: int,
    isolated_count: int,
    remand_count: int,
) -> dict[str, Any]:
    return append_audit_receipt(
        run_dir=run_dir,
        round_id=round_id,
        phase_kind="match",
        event_kind="matching-adjudication-applied",
        artifact_specs=[
            {
                "path": matching_adjudication_path(run_dir, round_id),
                "label": "matching-adjudication",
                "artifact_kind": "canonical",
                "required_current": True,
            },
            {"path": shared_evidence_path(run_dir, round_id), "label": "shared-evidence", "artifact_kind": "canonical", "required_current": True},
            {"path": matching_result_path(run_dir, round_id), "label": "matching-result", "artifact_kind": "canonical", "required_current": True},
            {
                "path": evidence_adjudication_path(run_dir, round_id),
                "label": "evidence-adjudication",
                "artifact_kind": "canonical",
                "required_current": True,
            },
            {"path": cards_active_path(run_dir, round_id), "label": "cards-active", "artifact_kind": "library-view", "required_current": True},
            {
                "path": isolated_active_path(run_dir, round_id),
                "label": "isolated-active",
                "artifact_kind": "library-view",
                "required_current": True,
            },
            {"path": remands_open_path(run_dir, round_id), "label": "remands-open", "artifact_kind": "library-view", "required_current": True},
            {
                "path": investigation_state_path(run_dir, round_id),
                "label": "investigation-state",
                "artifact_kind": "derived-state",
                "required_current": True,
            },
            {
                "path": investigation_actions_path(run_dir, round_id),
                "label": "investigation-actions",
                "artifact_kind": "derived-state",
                "required_current": True,
            },
        ],
        details={
            "evidence_count": int(evidence_count),
            "isolated_count": int(isolated_count),
            "remand_count": int(remand_count),
        },
    )


def record_decision_phase_receipt(
    *,
    run_dir: Path,
    round_id: str,
    decision_payload: dict[str, Any],
) -> dict[str, Any]:
    artifact_specs: list[dict[str, Any]] = [
        {
            "path": decision_target_path(run_dir, round_id),
            "label": "council-decision",
            "artifact_kind": "canonical",
            "required_current": True,
        },
        {
            "path": decision_draft_path(run_dir, round_id),
            "label": "decision-draft",
            "artifact_kind": "draft",
            "required_current": False,
            "optional": True,
        },
        {
            "path": report_target_path(run_dir, round_id, "sociologist"),
            "label": "sociologist-report",
            "artifact_kind": "canonical",
            "required_current": True,
            "optional": True,
        },
        {
            "path": report_target_path(run_dir, round_id, "environmentalist"),
            "label": "environmentalist-report",
            "artifact_kind": "canonical",
            "required_current": True,
            "optional": True,
        },
        {
            "path": matching_result_path(run_dir, round_id),
            "label": "matching-result",
            "artifact_kind": "canonical",
            "required_current": True,
            "optional": True,
        },
    ]
    return append_audit_receipt(
        run_dir=run_dir,
        round_id=round_id,
        phase_kind="decision",
        event_kind="council-decision-promoted",
        actor_role="moderator",
        artifact_specs=artifact_specs,
        details={
            "decision_id": maybe_text(decision_payload.get("decision_id")),
            "moderator_status": maybe_text(decision_payload.get("moderator_status")),
            "evidence_sufficiency": maybe_text(decision_payload.get("evidence_sufficiency")),
            "next_round_required": bool(decision_payload.get("next_round_required")),
        },
    )


def validate_round_audit_chain(run_dir: Path, round_id: str, *, require_exists: bool = False) -> dict[str, Any]:
    normalized_round_id = require_round_id(round_id)
    ledger_path = audit_chain_ledger_path(run_dir, normalized_round_id)
    if not ledger_path.exists():
        issues = []
        if require_exists:
            issues.append(
                {
                    "path": str(ledger_path),
                    "message": "Audit-chain ledger is missing.",
                }
            )
        return {
            "kind": "audit-chain",
            "path": str(ledger_path),
            "round_id": normalized_round_id,
            "receipt_count": 0,
            "latest_artifact_checks": 0,
            "validation": {"ok": not issues, "issues": issues},
        }
    try:
        receipts = [item for item in read_jsonl(ledger_path) if isinstance(item, dict)]
    except Exception as exc:  # noqa: BLE001
        return {
            "kind": "audit-chain",
            "path": str(ledger_path),
            "round_id": normalized_round_id,
            "receipt_count": 0,
            "latest_artifact_checks": 0,
            "validation": {
                "ok": False,
                "issues": [
                    {
                        "path": str(ledger_path),
                        "message": f"Audit-chain ledger is not valid JSONL: {exc}",
                    }
                ],
            },
        }
    issues: list[dict[str, Any]] = []
    previous_sha256 = ""
    latest_current_refs: dict[str, dict[str, Any]] = {}
    for index, receipt in enumerate(receipts, start=1):
        expected_index = index
        actual_index = safe_int(receipt.get("chain_index"))
        if actual_index is None:
            issues.append(
                {
                    "path": f"{ledger_path}:{index}",
                    "message": "chain_index must be an integer.",
                }
            )
        elif actual_index != expected_index:
            issues.append(
                {
                    "path": f"{ledger_path}:{index}",
                    "message": f"Expected chain_index={expected_index}, got {actual_index}.",
                }
            )
        if maybe_text(receipt.get("round_id")) != normalized_round_id:
            issues.append(
                {
                    "path": f"{ledger_path}:{index}.round_id",
                    "message": f"Receipt round_id does not match expected round {normalized_round_id}.",
                }
            )
        phase_kind = maybe_text(receipt.get("phase_kind"))
        if phase_kind not in VALID_PHASE_KINDS:
            issues.append(
                {
                    "path": f"{ledger_path}:{index}.phase_kind",
                    "message": f"Unsupported phase_kind in receipt: {phase_kind or '<missing>'}.",
                }
            )
        if maybe_text(receipt.get("prev_receipt_sha256")) != previous_sha256:
            issues.append(
                {
                    "path": f"{ledger_path}:{index}",
                    "message": "prev_receipt_sha256 does not match the previous receipt hash.",
                }
            )
        candidate = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
        expected_sha256 = stable_sha256(candidate)
        actual_sha256 = maybe_text(receipt.get("receipt_sha256"))
        if actual_sha256 != expected_sha256:
            issues.append(
                {
                    "path": f"{ledger_path}:{index}",
                    "message": "receipt_sha256 does not match the canonical receipt content.",
                }
            )
        previous_sha256 = actual_sha256
        artifact_refs = receipt.get("artifact_refs")
        if not isinstance(artifact_refs, list) or not artifact_refs:
            issues.append(
                {
                    "path": f"{ledger_path}:{index}.artifact_refs",
                    "message": "Receipt must contain at least one artifact_ref.",
                }
            )
            continue
        for ref_index, artifact_ref in enumerate(artifact_refs):
            if not isinstance(artifact_ref, dict):
                issues.append(
                    {
                        "path": f"{ledger_path}:{index}.artifact_refs[{ref_index}]",
                        "message": "artifact_ref must be an object.",
                    }
                )
                continue
            snapshot_display = maybe_text(artifact_ref.get("snapshot_path"))
            if not snapshot_display:
                issues.append(
                    {
                        "path": f"{ledger_path}:{index}.artifact_refs[{ref_index}].snapshot_path",
                        "message": "artifact_ref snapshot_path is missing.",
                    }
                )
            else:
                snapshot_path = resolved_path_from_display(run_dir=run_dir, display_path=snapshot_display)
                if not snapshot_path.exists() or not snapshot_path.is_file():
                    issues.append(
                        {
                            "path": f"{ledger_path}:{index}.artifact_refs[{ref_index}].snapshot_path",
                            "message": f"Snapshot is missing or not a file: {snapshot_path}",
                        }
                    )
                else:
                    expected_snapshot_sha256 = maybe_text(artifact_ref.get("sha256"))
                    if not expected_snapshot_sha256:
                        issues.append(
                            {
                                "path": f"{ledger_path}:{index}.artifact_refs[{ref_index}].sha256",
                                "message": "artifact_ref sha256 is missing.",
                            }
                        )
                    else:
                        try:
                            actual_snapshot_sha256 = file_sha256(snapshot_path)
                        except Exception as exc:  # noqa: BLE001
                            issues.append(
                                {
                                    "path": f"{ledger_path}:{index}.artifact_refs[{ref_index}].snapshot_path",
                                    "message": f"Unable to read snapshot for digest verification: {exc}",
                                }
                            )
                        else:
                            if actual_snapshot_sha256 != expected_snapshot_sha256:
                                issues.append(
                                    {
                                        "path": f"{ledger_path}:{index}.artifact_refs[{ref_index}].snapshot_path",
                                        "message": "Snapshot digest does not match recorded sha256.",
                                    }
                                )
            artifact_display = maybe_text(artifact_ref.get("artifact_path"))
            if bool(artifact_ref.get("required_current")) and not artifact_display:
                issues.append(
                    {
                        "path": f"{ledger_path}:{index}.artifact_refs[{ref_index}].artifact_path",
                        "message": "artifact_ref artifact_path is missing for a required_current artifact.",
                    }
                )
            if bool(artifact_ref.get("required_current")) and artifact_display:
                latest_current_refs[artifact_display] = artifact_ref
    for display_path, artifact_ref in latest_current_refs.items():
        current_path = resolved_path_from_display(run_dir=run_dir, display_path=display_path)
        if not current_path.exists() or not current_path.is_file():
            issues.append(
                {
                    "path": display_path,
                    "message": "Latest required canonical artifact is missing or not a file.",
                }
            )
            continue
        try:
            current_sha256 = file_sha256(current_path)
        except Exception as exc:  # noqa: BLE001
            issues.append(
                {
                    "path": display_path,
                    "message": f"Latest canonical artifact could not be hashed: {exc}",
                }
            )
            continue
        if current_sha256 != maybe_text(artifact_ref.get("sha256")):
            issues.append(
                {
                    "path": display_path,
                    "message": "Latest canonical artifact digest no longer matches the newest recorded receipt.",
                }
            )
    return {
        "kind": "audit-chain",
        "path": str(ledger_path),
        "round_id": normalized_round_id,
        "receipt_count": len(receipts),
        "latest_artifact_checks": len(latest_current_refs),
        "validation": {
            "ok": not issues,
            "issues": issues,
        },
    }
