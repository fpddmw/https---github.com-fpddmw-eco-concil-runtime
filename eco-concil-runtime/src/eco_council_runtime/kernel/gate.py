from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Callable

GateHandler = Callable[..., dict[str, Any]]


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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
        "gate_status": maybe_text(gate_payload.get("gate_status")) or "report-basis-freeze-withheld",
        "report_basis_gate_status": maybe_text(gate_payload.get("report_basis_gate_status")),
        "report_basis_status": maybe_text(gate_payload.get("report_basis_status")),
        "report_basis_freeze_allowed": bool(
            gate_payload.get("report_basis_freeze_allowed")
        )
        or bool(gate_payload.get("report_basis_freeze_allowed")),
        "gate_reasons": gate_payload.get("gate_reasons", []) if isinstance(gate_payload.get("gate_reasons"), list) else [],
        "recommended_next_skills": (
            gate_payload.get("recommended_next_skills", [])
            if isinstance(gate_payload.get("recommended_next_skills"), list)
            else []
        ),
    }


def gate_handler_registry(
    gate_handlers: dict[str, GateHandler] | None = None,
) -> dict[str, GateHandler]:
    registry: dict[str, GateHandler] = {}
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
) -> dict[str, Any]:
    gate_handler = maybe_text(blueprint.get("gate_handler")) or maybe_text(blueprint.get("stage")) or "report-basis-gate"
    readiness_stage_name = resolve_readiness_stage_name(blueprint)
    contracts = stage_contracts if isinstance(stage_contracts, dict) else {}
    readiness_path_override = (
        maybe_text(contracts.get(readiness_stage_name, {}).get("expected_output_path"))
        if isinstance(contracts.get(readiness_stage_name), dict)
        else ""
    )
    output_path_override = maybe_text(blueprint.get("expected_output_path"))
    registry = gate_handler_registry(gate_handlers)
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
