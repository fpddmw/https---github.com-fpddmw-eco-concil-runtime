from __future__ import annotations

from typing import Any
from typing import Callable

from . import phase2_gate_handlers

GateHandler = Callable[..., dict[str, Any]]


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def phase2_gate_handler_registry(
    gate_handlers: dict[str, GateHandler] | None = None,
) -> dict[str, GateHandler]:
    registry: dict[str, GateHandler] = {
        "report-basis-gate": phase2_gate_handlers.apply_report_basis_gate,
    }
    if isinstance(gate_handlers, dict):
        for name, handler in gate_handlers.items():
            normalized_name = maybe_text(name)
            if normalized_name and callable(handler):
                registry[normalized_name] = handler
    return registry
