from __future__ import annotations

import shlex
from pathlib import Path

from .kernel.executor import skill_command_hint


def kernel_command(command_name: str, *args: str) -> str:
    return shlex.join(
        [
            "python3",
            "eco-concil-runtime/scripts/eco_runtime_kernel.py",
            command_name,
            *args,
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
) -> str:
    return (
        "python3 eco-concil-runtime/scripts/eco_runtime_kernel.py "
        + skill_command_hint(
            "run-skill",
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=skill_name,
            contract_mode=contract_mode,
            skill_args=skill_args or [],
        )
    )
