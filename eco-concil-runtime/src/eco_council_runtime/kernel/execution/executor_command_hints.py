from __future__ import annotations

import shlex
from pathlib import Path

from eco_council_runtime.kernel.execution.executor_common import maybe_text

def skill_command_hint(
    command_name: str,
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    skill_name: str,
    actor_role: str,
    contract_mode: str,
    skill_args: list[str],
    skill_approval_request_id: str = "",
) -> str:
    command = [
        command_name,
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--skill-name",
        skill_name,
        "--actor-role",
        actor_role,
        "--contract-mode",
        contract_mode,
    ]
    if maybe_text(skill_approval_request_id):
        command.extend(["--skill-approval-request-id", maybe_text(skill_approval_request_id)])
    if skill_args:
        command.extend(["--", *skill_args])
    return shlex.join(command)


__all__ = (
    "skill_command_hint",
)
