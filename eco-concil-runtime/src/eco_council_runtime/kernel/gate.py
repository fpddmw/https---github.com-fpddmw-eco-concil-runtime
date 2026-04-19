from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Callable

from .deliberation_plane import store_promotion_freeze_record
from .manifest import write_json
from .paths import promotion_gate_path
from .phase2_state_surfaces import load_round_readiness_wrapper

GateHandler = Callable[..., dict[str, Any]]


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_path(run_dir: Path, override: str, default_path: Path) -> Path:
    text = maybe_text(override)
    if not text:
        return default_path.resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_readiness_stage_name(blueprint: dict[str, Any]) -> str:
    readiness_stage_name = maybe_text(blueprint.get("readiness_stage_name"))
    if readiness_stage_name:
        return readiness_stage_name
    required_previous_stages = blueprint.get("required_previous_stages")
    if isinstance(required_previous_stages, list):
        normalized = [maybe_text(item) for item in required_previous_stages if maybe_text(item)]
        if normalized:
            return normalized[-1]
    return "round-readiness"


def apply_promotion_gate(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    readiness_path_override: str = "",
    output_path_override: str = "",
) -> dict[str, Any]:
    readiness_file = resolve_path(run_dir, readiness_path_override, run_dir / "reporting" / f"round_readiness_{round_id}.json")
    output_file = resolve_path(run_dir, output_path_override, promotion_gate_path(run_dir, round_id))

    readiness_context = load_round_readiness_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        readiness_path=readiness_path_override,
    )
    readiness = (
        readiness_context.get("payload")
        if isinstance(readiness_context.get("payload"), dict)
        else {}
    )
    readiness_status = maybe_text(readiness.get("readiness_status")) or "blocked"
    gate_reasons = readiness.get("gate_reasons", []) if isinstance(readiness.get("gate_reasons"), list) else []
    recommended_next_skills = (
        readiness.get("recommended_next_skills", []) if isinstance(readiness.get("recommended_next_skills"), list) else []
    )
    warnings: list[dict[str, str]] = []
    if not readiness:
        warnings.append({"code": "missing-readiness", "message": f"No round readiness artifact or DB assessment was found at {readiness_file}."})
    promote_allowed = readiness_status == "ready"
    gate_status = "allow-promote" if promote_allowed else "freeze-withheld"

    payload = {
        "schema_version": "runtime-gate-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "readiness_path": str(readiness_file),
        "readiness_status": readiness_status,
        "promote_allowed": promote_allowed,
        "gate_status": gate_status,
        "gate_reasons": [maybe_text(item) for item in gate_reasons if maybe_text(item)],
        "recommended_next_skills": [maybe_text(item) for item in recommended_next_skills if maybe_text(item)],
        "warnings": warnings,
    }
    write_json(output_file, payload)
    payload["output_path"] = str(output_file)
    store_promotion_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        gate_snapshot=payload,
        artifact_paths={"promotion_gate_path": str(output_file)},
    )
    return payload


def gate_controller_updates(gate_payload: dict[str, Any]) -> dict[str, Any]:
    embedded_updates = (
        gate_payload.get("controller_updates")
        if isinstance(gate_payload.get("controller_updates"), dict)
        else None
    )
    if isinstance(embedded_updates, dict):
        return dict(embedded_updates)
    return {
        "readiness_status": maybe_text(gate_payload.get("readiness_status")) or "blocked",
        "gate_status": maybe_text(gate_payload.get("gate_status")) or "freeze-withheld",
        "gate_reasons": gate_payload.get("gate_reasons", []) if isinstance(gate_payload.get("gate_reasons"), list) else [],
        "recommended_next_skills": (
            gate_payload.get("recommended_next_skills", [])
            if isinstance(gate_payload.get("recommended_next_skills"), list)
            else []
        ),
    }


def gate_handler_registry(
    gate_handlers: dict[str, GateHandler] | None = None,
    *,
    promotion_gate_handler: GateHandler | None = None,
) -> dict[str, GateHandler]:
    registry: dict[str, GateHandler] = {
        "promotion-gate": promotion_gate_handler or apply_promotion_gate,
    }
    if isinstance(gate_handlers, dict):
        for name, handler in gate_handlers.items():
            normalized_name = maybe_text(name)
            if normalized_name and callable(handler):
                registry[normalized_name] = handler
    return registry


def execute_gate_step(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    blueprint: dict[str, Any],
    stage_contracts: dict[str, Any] | None = None,
    gate_handlers: dict[str, GateHandler] | None = None,
    promotion_gate_handler: GateHandler | None = None,
) -> dict[str, Any]:
    gate_handler = maybe_text(blueprint.get("gate_handler")) or maybe_text(blueprint.get("stage")) or "promotion-gate"
    readiness_stage_name = resolve_readiness_stage_name(blueprint)
    contracts = stage_contracts if isinstance(stage_contracts, dict) else {}
    readiness_path_override = (
        maybe_text(contracts.get(readiness_stage_name, {}).get("expected_output_path"))
        if isinstance(contracts.get(readiness_stage_name), dict)
        else ""
    )
    output_path_override = maybe_text(blueprint.get("expected_output_path"))
    registry = gate_handler_registry(
        gate_handlers,
        promotion_gate_handler=promotion_gate_handler,
    )
    handler = registry.get(gate_handler)
    if handler is None:
        available = ", ".join(sorted(registry))
        raise ValueError(
            f"Unsupported runtime gate handler: {gate_handler}. Available: {available}"
        )
    gate_payload = handler(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        readiness_path_override=readiness_path_override,
        output_path_override=output_path_override,
    )
    return {
        "gate_handler": gate_handler,
        "readiness_stage_name": readiness_stage_name,
        "gate_payload": gate_payload,
        "controller_updates": gate_controller_updates(gate_payload),
    }
