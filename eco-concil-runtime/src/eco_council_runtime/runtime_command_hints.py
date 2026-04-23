from __future__ import annotations

import shlex
from pathlib import Path

from .kernel.access_policy import (
    command_requires_explicit_actor_role,
    kernel_command_actor_role_hint,
)
from .kernel.executor import skill_command_hint
from .kernel.skill_registry import default_actor_role_hint


def kernel_command(command_name: str, *args: str, actor_role: str = "") -> str:
    command_args = list(args)
    if command_requires_explicit_actor_role(command_name) and "--actor-role" not in command_args:
        command_args = [
            "--actor-role",
            actor_role or kernel_command_actor_role_hint(command_name),
            *command_args,
        ]
    return shlex.join(
        [
            "python3",
            "eco-concil-runtime/scripts/eco_runtime_kernel.py",
            command_name,
            *command_args,
        ]
    )


def run_skill_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    skill_name: str,
    contract_mode: str,
    skill_args: list[str] | None = None,
    actor_role: str = "",
) -> str:
    return (
        "python3 eco-concil-runtime/scripts/eco_runtime_kernel.py "
        + skill_command_hint(
            "run-skill",
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=skill_name,
            actor_role=actor_role or default_actor_role_hint(skill_name),
            contract_mode=contract_mode,
            skill_args=skill_args or [],
        )
    )
