
"""Application services for supervisor lifecycle and import workflows."""

from __future__ import annotations

from importlib import import_module

_EXPORT_TO_MODULE = {
	"command_continue_run": "eco_council_runtime.application.supervisor.runtime_cli",
	"command_init_run": "eco_council_runtime.application.supervisor.runtime_cli",
	"command_run_agent_step": "eco_council_runtime.application.supervisor.runtime_cli",
	"command_status": "eco_council_runtime.application.supervisor.runtime_cli",
	"continue_recover_or_run_matching_adjudication": "eco_council_runtime.application.supervisor.runtime_cli",
	"continue_run_matching_adjudication": "eco_council_runtime.application.supervisor.runtime_cli",
	"load_state": "eco_council_runtime.application.supervisor.runtime_cli",
	"main": "eco_council_runtime.application.supervisor.runtime_cli",
	"refresh_supervisor_files": "eco_council_runtime.application.supervisor.runtime_cli",
	"save_state": "eco_council_runtime.application.supervisor.runtime_cli",
}

__all__ = list(_EXPORT_TO_MODULE)


def __getattr__(name: str) -> object:
	module_name = _EXPORT_TO_MODULE.get(name)
	if module_name is None:
		raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
	module = import_module(module_name)
	value = getattr(module, name)
	globals()[name] = value
	return value


def __dir__() -> list[str]:
	return sorted(set(globals()) | set(__all__))

__all__: list[str] = []
