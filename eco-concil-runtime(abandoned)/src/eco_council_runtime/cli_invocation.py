"""Helpers for invoking eco-council runtime modules without script wrappers."""

from __future__ import annotations

import shlex
import sys


def runtime_module_name(module_name: str) -> str:
    return f"eco_council_runtime.{module_name}"


def runtime_module_argv(module_name: str, *args: object) -> list[str]:
    return [sys.executable, "-m", runtime_module_name(module_name), *(str(arg) for arg in args)]


def runtime_module_command(module_name: str, *args: object) -> str:
    return shlex.join(runtime_module_argv(module_name, *args))


__all__ = [
    "runtime_module_argv",
    "runtime_module_command",
    "runtime_module_name",
]
