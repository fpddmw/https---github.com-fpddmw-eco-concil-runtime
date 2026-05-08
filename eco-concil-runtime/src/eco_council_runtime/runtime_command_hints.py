from __future__ import annotations

import shlex
from pathlib import Path

from eco_council_runtime.kernel.governance.access_policy import (
    command_requires_explicit_actor_role,
    kernel_command_actor_role_hint,
)
from eco_council_runtime.kernel.execution.executor import skill_command_hint
from eco_council_runtime.kernel.governance.skill_registry import default_actor_role_hint


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
    skill_approval_request_id: str = "",
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
) -> str:
    command_args = shlex.split(
        skill_command_hint(
            "run-skill",
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=skill_name,
            actor_role=actor_role or default_actor_role_hint(skill_name),
            contract_mode=contract_mode,
            skill_args=[],
            skill_approval_request_id=skill_approval_request_id,
        )
    )
    if timeout_seconds is not None:
        command_args.extend(["--timeout-seconds", str(timeout_seconds)])
    if retry_budget is not None:
        command_args.extend(["--retry-budget", str(retry_budget)])
    if retry_backoff_ms is not None:
        command_args.extend(["--retry-backoff-ms", str(retry_backoff_ms)])
    for side_effect in allow_side_effects or []:
        command_args.extend(["--allow-side-effect", side_effect])
    if skill_args:
        command_args.extend(["--", *skill_args])
    return (
        "python3 eco-concil-runtime/scripts/eco_runtime_kernel.py "
        + shlex.join(command_args)
    )
