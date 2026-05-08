from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import load_json_if_exists, utc_now_iso, write_json
from eco_council_runtime.kernel.core.paths import agent_entry_gate_path, resolve_run_dir
from eco_council_runtime.kernel.execution.executor import maybe_text


def role_identity(role: str) -> str:
    return "Eco Council " + maybe_text(role).replace("-", " ").title()


def role_agent_name(prefix: str, role: str) -> str:
    normalized_prefix = maybe_text(prefix)
    if normalized_prefix:
        return f"{normalized_prefix}-{role}"
    return role


def resolve_workspace_root(run_dir: Path, workspace_root: str) -> Path:
    text = maybe_text(workspace_root)
    if not text:
        return (run_dir / "supervisor" / "openclaw-workspaces").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def shell_command(parts: list[str]) -> str:
    return shlex.join([part for part in parts if maybe_text(part)])


def materialize_openclaw_agent_registration_plan(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    actor_role: str,
    agent_entry_gate: dict[str, Any] | None = None,
    agent_name_prefix: str = "",
    workspace_root: str = "",
    create_workspaces: bool = True,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    gate_payload = agent_entry_gate if isinstance(agent_entry_gate, dict) else load_json_if_exists(agent_entry_gate_path(run_dir_path, round_id))
    gate_payload = gate_payload if isinstance(gate_payload, dict) else {}
    workspace_root_path = resolve_workspace_root(run_dir_path, workspace_root)
    if create_workspaces:
        workspace_root_path.mkdir(parents=True, exist_ok=True)

    role_entries = (
        gate_payload.get("role_entry_points", [])
        if isinstance(gate_payload.get("role_entry_points"), list)
        else []
    )
    registrations: list[dict[str, Any]] = []
    for entry in role_entries:
        if not isinstance(entry, dict):
            continue
        if maybe_text(entry.get("role_kind")) != "council-agent":
            continue
        role = maybe_text(entry.get("role"))
        if not role:
            continue
        workspace = (workspace_root_path / role).resolve()
        if create_workspaces:
            workspace.mkdir(parents=True, exist_ok=True)
        agent_name = role_agent_name(agent_name_prefix or run_id, role)
        identity = role_identity(role)
        command = shell_command(
            [
                "openclaw",
                "agents",
                "add",
                agent_name,
                "--workspace",
                str(workspace),
                "--identity",
                identity,
            ]
        )
        registrations.append(
            {
                "role": role,
                "agent_name": agent_name,
                "identity": identity,
                "workspace": str(workspace),
                "registration_command": command,
            }
        )

    output_path = run_dir_path / "runtime" / f"openclaw_agent_registration_{round_id}.json"
    payload = {
        "schema_version": "openclaw-agent-registration-plan-v1",
        "generated_at_utc": utc_now_iso(),
        "status": "completed" if registrations else "needs-agent-entry-gate",
        "run_id": run_id,
        "round_id": round_id,
        "requested_by_role": maybe_text(actor_role),
        "source_agent_entry_gate": str(agent_entry_gate_path(run_dir_path, round_id).resolve()),
        "workspace_root": str(workspace_root_path),
        "create_workspaces": bool(create_workspaces),
        "registration_count": len(registrations),
        "registrations": registrations,
        "register_all_command": " && ".join(item["registration_command"] for item in registrations),
        "output_path": str(output_path.resolve()),
    }
    write_json(output_path, payload)
    return payload


__all__ = [
    "materialize_openclaw_agent_registration_plan",
    "role_agent_name",
    "role_identity",
]
