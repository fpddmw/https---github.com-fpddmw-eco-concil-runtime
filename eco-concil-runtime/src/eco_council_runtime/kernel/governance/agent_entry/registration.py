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


def role_surface_for(agent_entry_gate: dict[str, Any], role: str) -> dict[str, Any]:
    role_entries = (
        agent_entry_gate.get("role_entry_points", [])
        if isinstance(agent_entry_gate.get("role_entry_points"), list)
        else []
    )
    for entry in role_entries:
        if isinstance(entry, dict) and maybe_text(entry.get("role")) == role:
            return dict(entry)
    return {}


def write_role_workspace_context(
    workspace: Path,
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    role: str,
    agent_entry_gate: dict[str, Any],
) -> dict[str, str]:
    context_dir = workspace / "council_runtime"
    context_dir.mkdir(parents=True, exist_ok=True)

    role_surface_path = context_dir / "role_surface.json"
    runtime_context_path = context_dir / "runtime_context.json"
    source_selection_path = run_dir / "runtime" / f"source_selection_{role}_{round_id}.json"
    role_surface = role_surface_for(agent_entry_gate, role)
    write_json(role_surface_path, role_surface)

    context_payload = {
        "schema_version": "openclaw-role-workspace-context-v1",
        "run_id": run_id,
        "round_id": round_id,
        "role": role,
        "run_dir": str(run_dir.resolve()),
        "mission_path": str((run_dir / "mission.json").resolve()),
        "agent_entry_gate_path": str(agent_entry_gate_path(run_dir, round_id).resolve()),
        "role_surface_path": str(role_surface_path.resolve()),
        "source_selection_path": str(source_selection_path.resolve()),
        "runtime_health_path": str((run_dir / "runtime" / "runtime_health.json").resolve()),
        "investigation_board_path": str((run_dir / "board" / "investigation_board.json").resolve()),
    }
    write_json(runtime_context_path, context_payload)

    overview_path = workspace / "COUNCIL_RUNTIME.md"
    overview_path.write_text(
        "\n".join(
            [
                "# Council Runtime Context",
                "",
                f"- Run id: `{run_id}`",
                f"- Round id: `{round_id}`",
                f"- Role: `{role}`",
                f"- Runtime context: `{runtime_context_path.resolve()}`",
                f"- Role surface: `{role_surface_path.resolve()}`",
                f"- Agent entry gate: `{agent_entry_gate_path(run_dir, round_id).resolve()}`",
                f"- Mission: `{(run_dir / 'mission.json').resolve()}`",
                f"- Runtime health: `{(run_dir / 'runtime' / 'runtime_health.json').resolve()}`",
                f"- Investigation board: `{(run_dir / 'board' / 'investigation_board.json').resolve()}`",
                "",
                "Use these generated runtime artifacts as the role contract surface.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "overview_path": str(overview_path.resolve()),
        "runtime_context_path": str(runtime_context_path.resolve()),
        "role_surface_path": str(role_surface_path.resolve()),
    }


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
    workspace_contexts: list[dict[str, str]] = []
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
            context_paths = write_role_workspace_context(
                workspace,
                run_dir=run_dir_path,
                run_id=run_id,
                round_id=round_id,
                role=role,
                agent_entry_gate=gate_payload,
            )
            workspace_contexts.append({"role": role, "workspace": str(workspace), **context_paths})
        agent_name = role_agent_name(agent_name_prefix or run_id, role)
        identity = role_identity(role)
        add_command = shell_command(
            [
                "openclaw",
                "agents",
                "add",
                agent_name,
                "--workspace",
                str(workspace),
                "--non-interactive",
                "--json",
            ]
        )
        identity_command = shell_command(
            [
                "openclaw",
                "agents",
                "set-identity",
                "--agent",
                agent_name,
                "--name",
                identity,
                "--json",
            ]
        )
        registrations.append(
            {
                "role": role,
                "agent_name": agent_name,
                "identity": identity,
                "workspace": str(workspace),
                "registration_command": add_command,
                "identity_command": identity_command,
                "setup_command": f"{add_command} && {identity_command}",
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
        "workspace_contexts": workspace_contexts,
        "register_all_command": " && ".join(item["setup_command"] for item in registrations),
        "output_path": str(output_path.resolve()),
    }
    write_json(output_path, payload)
    return payload


__all__ = [
    "materialize_openclaw_agent_registration_plan",
    "role_agent_name",
    "role_identity",
    "write_role_workspace_context",
]
