"""Application services for cross-run archive workflows and imports."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "case_library_argv": "eco_council_runtime.application.archive.importers",
    "maybe_auto_import_case_library": "eco_council_runtime.application.archive.importers",
    "maybe_auto_import_signal_corpus": "eco_council_runtime.application.archive.importers",
    "signal_corpus_argv": "eco_council_runtime.application.archive.importers",
    "collect_run_snapshot": "eco_council_runtime.application.archive.runtime_state",
    "load_fetch_execution": "eco_council_runtime.application.archive.runtime_state",
    "load_state": "eco_council_runtime.application.archive.runtime_state",
    "round_payload_lists": "eco_council_runtime.application.archive.runtime_state",
    "state_for_run": "eco_council_runtime.application.archive.runtime_state",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS)