from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import ledger_path, receipt_path

RUNTIME_RECEIPT_SCHEMA = "runtime-receipt-v2"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(*parts: Any) -> str:
    import hashlib

    joined = "||".join("" if part is None else " ".join(str(part).split()) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def json_hash(payload: Any) -> str:
    return stable_hash(json.dumps(payload, ensure_ascii=True, sort_keys=True))


def load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def existing_receipt_payload_hash(existing: dict[str, Any]) -> str:
    if not existing:
        return ""
    payload_hash = existing.get("payload_hash")
    if isinstance(payload_hash, str) and payload_hash:
        return payload_hash
    skill_payload = existing.get("skill_payload")
    if isinstance(skill_payload, dict):
        return json_hash(skill_payload)
    return json_hash(existing)


def runtime_receipt_envelope(
    *,
    receipt_id: str,
    skill_payload: dict[str, Any],
    runtime_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = runtime_context if isinstance(runtime_context, dict) else {}
    payload_hash = json_hash(skill_payload)
    return {
        "schema_version": RUNTIME_RECEIPT_SCHEMA,
        "generated_at_utc": utc_now_iso(),
        "receipt_id": receipt_id,
        "status": skill_payload.get("status", "completed"),
        "run_id": context.get("run_id", ""),
        "round_id": context.get("round_id", ""),
        "skill_name": context.get("skill_name", ""),
        "event_id": context.get("event_id", ""),
        "payload_hash": payload_hash,
        "summary": skill_payload.get("summary", {})
        if isinstance(skill_payload.get("summary"), dict)
        else {},
        "artifact_refs": skill_payload.get("artifact_refs", [])
        if isinstance(skill_payload.get("artifact_refs"), list)
        else [],
        "canonical_ids": skill_payload.get("canonical_ids", [])
        if isinstance(skill_payload.get("canonical_ids"), list)
        else [],
        "runtime": context,
        "skill_payload": skill_payload,
    }


def write_receipt(
    run_dir: Path,
    receipt_id: str,
    payload: dict[str, Any],
    *,
    runtime_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path = receipt_path(run_dir, receipt_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    envelope = runtime_receipt_envelope(
        receipt_id=receipt_id,
        skill_payload=payload,
        runtime_context=runtime_context,
    )
    existing = load_json_if_exists(path)
    previous_payload_hash = existing_receipt_payload_hash(existing)
    payload_hash = envelope["payload_hash"]
    if previous_payload_hash == payload_hash:
        write_status = "unchanged"
    elif previous_payload_hash:
        write_status = "replaced"
    else:
        write_status = "created"

    if write_status != "unchanged":
        path.write_text(
            json.dumps(envelope, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    receipt_hash = json_hash(load_json_if_exists(path) or envelope)
    return {
        "schema_version": "runtime-receipt-write-v1",
        "receipt_id": receipt_id,
        "receipt_path": str(path),
        "write_status": write_status,
        "payload_hash": payload_hash,
        "previous_payload_hash": previous_payload_hash,
        "receipt_hash": receipt_hash,
    }


def append_ledger_event(run_dir: Path, event: dict[str, Any]) -> None:
    path = ledger_path(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=True, sort_keys=True) + "\n")


def load_ledger_tail(run_dir: Path, limit: int = 10) -> list[dict[str, Any]]:
    path = ledger_path(run_dir)
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    results: list[dict[str, Any]] = []
    for line in lines[-max(1, limit) :]:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            results.append(payload)
    return results
