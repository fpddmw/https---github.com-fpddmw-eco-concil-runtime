"""OpenClaw adapter helpers for the eco-council controller."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.cli_invocation import runtime_module_command
from eco_council_runtime.controller.constants import (
    AGENT_ID_SAFE,
    MAX_OPENCLAW_INLINE_MESSAGE_CHARS,
    OPENCLAW_AGENT_GUIDE_FILENAME,
    ROLES,
)
from eco_council_runtime.controller.io import load_json_if_exists, maybe_text, run_json_command, write_text
from eco_council_runtime.controller.paths import (
    mission_path,
    openclaw_runtime_root,
    outbox_message_path,
    session_prompt_path,
    supervisor_current_step_path,
    supervisor_dir,
    supervisor_outbox_dir,
)
from eco_council_runtime.external_skills import sync_openclaw_managed_skills
from eco_council_runtime.layout import PROJECT_DIR


def openclaw_cli_env(run_dir: Path) -> dict[str, str]:
    runtime_root = openclaw_runtime_root(run_dir)
    state_dir = runtime_root / "state"
    cache_dir = runtime_root / "cache"
    config_path = runtime_root / "openclaw.json"
    runtime_root.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["OPENCLAW_STATE_DIR"] = str(state_dir)
    env["OPENCLAW_CONFIG_PATH"] = str(config_path)
    env["XDG_CACHE_HOME"] = str(cache_dir)
    return env


def normalize_agent_prefix(value: str) -> str:
    text = AGENT_ID_SAFE.sub("-", value.strip().lower()).strip("-")
    return text or "eco-council"


def openclaw_workspace_root(run_dir: Path, state: dict[str, Any]) -> Path:
    configured = maybe_text(state.get("openclaw", {}).get("workspace_root"))
    if configured:
        return Path(configured).expanduser().resolve()
    return supervisor_dir(run_dir) / "openclaw-workspaces"


def openclaw_skill_runtime_section(state: dict[str, Any]) -> dict[str, Any]:
    openclaw_section = state.setdefault("openclaw", {})
    return openclaw_section.setdefault("skill_runtime", {})


def installed_openclaw_skills(state: dict[str, Any]) -> list[str]:
    skill_runtime = openclaw_skill_runtime_section(state)
    values = skill_runtime.get("projected_skills")
    if not isinstance(values, list):
        return []
    return [maybe_text(item) for item in values if maybe_text(item)]


def ensure_openclaw_config(
    run_dir: Path,
    state: dict[str, Any],
    *,
    workspace_root_text: str = "",
    skills_root_text: str = "",
) -> dict[str, Any]:
    openclaw_section = state.setdefault("openclaw", {})
    prefix = normalize_agent_prefix(maybe_text(openclaw_section.get("agent_prefix")) or run_dir.name)
    openclaw_section["agent_prefix"] = prefix
    workspace_root = (
        Path(workspace_root_text).expanduser().resolve()
        if workspace_root_text
        else openclaw_workspace_root(run_dir, state)
    )
    openclaw_section["workspace_root"] = str(workspace_root)
    agents = openclaw_section.setdefault("agents", {})
    for role in ROLES:
        role_info = agents.setdefault(role, {})
        role_info["id"] = maybe_text(role_info.get("id")) or f"{prefix}-{role}"
        workspace = (
            Path(maybe_text(role_info.get("workspace"))).expanduser().resolve()
            if maybe_text(role_info.get("workspace"))
            else (workspace_root / role).resolve()
        )
        role_info["workspace"] = str(workspace)
        role_info["guide_path"] = str((workspace / OPENCLAW_AGENT_GUIDE_FILENAME).resolve())
    skill_runtime = openclaw_skill_runtime_section(state)
    configured_skills_root = (
        maybe_text(skills_root_text)
        or maybe_text(skill_runtime.get("skills_root"))
        or os.environ.get("ECO_COUNCIL_SKILLS_ROOT", "")
    )
    if configured_skills_root:
        skill_runtime["skills_root"] = str(Path(configured_skills_root).expanduser().resolve())
    skill_runtime.setdefault("managed_skills_dir", "")
    skill_runtime.setdefault("manifest_path", "")
    skill_runtime.setdefault("projected_skills", [])
    skill_runtime.setdefault("recognized_skills", [])
    skill_runtime.setdefault("projection_signature", "")
    skill_runtime.setdefault("projected_at_utc", "")
    return openclaw_section


def ensure_openclaw_skill_runtime(
    run_dir: Path,
    *,
    state: dict[str, Any],
    mission: dict[str, Any] | None = None,
    skills_root_text: str = "",
) -> dict[str, Any]:
    ensure_openclaw_config(run_dir, state, skills_root_text=skills_root_text)
    if mission is None:
        mission_payload = load_json_if_exists(mission_path(run_dir))
        mission = mission_payload if isinstance(mission_payload, dict) else {}
    skill_runtime = openclaw_skill_runtime_section(state)
    sync_result = sync_openclaw_managed_skills(
        openclaw_env=openclaw_cli_env(run_dir),
        mission=mission,
        skills_root_text=maybe_text(skill_runtime.get("skills_root")) or skills_root_text,
    )
    skill_runtime.update(sync_result)
    return skill_runtime


def role_display_name(role: str) -> str:
    return {
        "moderator": "Moderator",
        "sociologist": "Sociologist",
        "environmentalist": "Environmentalist",
    }[role]


def agent_workspace_path(state: dict[str, Any], role: str) -> Path:
    workspace_text = maybe_text(state.get("openclaw", {}).get("agents", {}).get(role, {}).get("workspace"))
    if not workspace_text:
        raise ValueError(f"Missing OpenClaw workspace for role={role}")
    return Path(workspace_text).expanduser().resolve()


def agent_command_guide_path(*, state: dict[str, Any], role: str) -> Path:
    workspace = agent_workspace_path(state, role)
    return workspace / OPENCLAW_AGENT_GUIDE_FILENAME


def supervisor_status_command(run_dir: Path) -> str:
    return runtime_module_command("supervisor", "status", "--run-dir", run_dir, "--pretty")


def installed_skill_guide_lines(state: dict[str, Any]) -> list[str]:
    skill_runtime = openclaw_skill_runtime_section(state)
    projected_skills = installed_openclaw_skills(state)
    if not projected_skills:
        return [
            "OpenClaw-managed eco-council skills: not yet projected.",
            "If a required eco-council skill is missing, ask the human to rerun `provision-openclaw-agents` with the correct detached skills root.",
        ]
    managed_dir = maybe_text(skill_runtime.get("managed_skills_dir"))
    lines = [
        "OpenClaw-managed eco-council skills installed for this isolated profile:",
        f"- {', '.join(projected_skills)}",
    ]
    if managed_dir:
        lines.append(f"- Managed skills dir: {managed_dir}")
    return lines


def openclaw_agent_guide_text(*, run_dir: Path, state: dict[str, Any], role: str) -> str:
    run_dir = run_dir.expanduser().resolve()
    skill_runtime = openclaw_skill_runtime_section(state)
    skills_root = maybe_text(skill_runtime.get("skills_root"))
    skills_root_args = ["--skills-root", skills_root] if skills_root else []
    status_command = supervisor_status_command(run_dir)
    continue_command = runtime_module_command(
        "supervisor",
        "continue-run",
        "--run-dir",
        run_dir,
        "--yes",
        *skills_root_args,
        "--pretty",
    )
    run_agent_command = runtime_module_command(
        "supervisor",
        "run-agent-step",
        "--run-dir",
        run_dir,
        "--role",
        role,
        "--yes",
        *skills_root_args,
        "--pretty",
    )
    provision_command = runtime_module_command(
        "supervisor",
        "provision-openclaw-agents",
        "--run-dir",
        run_dir,
        "--yes",
        *skills_root_args,
        "--pretty",
    )
    summarize_command = runtime_module_command(
        "supervisor",
        "summarize-run",
        "--run-dir",
        run_dir,
        "--lang",
        "zh",
        "--pretty",
    )
    init_command = (
        runtime_module_command(
            "supervisor",
            "init-run",
            "--run-dir",
            "NEW_RUN_DIR",
            "--mission-input",
            "MISSION_JSON",
        )
        + " [--skills-root SKILLS_ROOT] --yes --pretty"
    )
    return "\n".join(
        [
            "# OpenClaw Agent Guide",
            "",
            f"Run directory: {run_dir}",
            f"Role: {role}",
            "",
            "The supervisor owns stage transitions, shell stages, and JSON imports.",
            "Role agents own only the single JSON artifact requested by the current turn.",
            "",
            *installed_skill_guide_lines(state),
            "",
            "Run workspace artifacts to trust first:",
            f"- Current step checklist: {supervisor_current_step_path(run_dir)}",
            f"- Session prompt for this role: {session_prompt_path(run_dir, role)}",
            f"- Supervisor outbox directory: {supervisor_outbox_dir(run_dir)}",
            "",
            "Command inventory:",
            f"- `{status_command}`",
            "  Purpose: inspect current round, current stage, prompt paths, and recommended next command.",
            f"- `{continue_command}`",
            "  Purpose: advance one supervisor-owned shell stage such as prepare-round, execute-fetch-plan, run-data-plane, run-matching-adjudication, promote-all, or advance-round. Human/supervisor only.",
            f"- `{run_agent_command}`",
            "  Purpose: supervisor wrapper that sends the current turn to an OpenClaw agent and imports the validated JSON reply. Do not call this from inside the agent already handling the turn.",
            "- `python3 ... import-task-review ...` / `import-source-selection ...` / `import-data-readiness ...` / `import-matching-authorization ...` / `import-matching-adjudication ...` / `import-investigation-review ...` / `import-report ...` / `import-decision ...` / `import-fetch-execution ...`",
            "  Purpose: import canonical JSON after manual edits or external fetch execution. Human/supervisor only.",
            f"- `{provision_command}`",
            "  Purpose: create or repair the three fixed OpenClaw agents and workspace support files. Human/supervisor only.",
            f"- `{summarize_command}`",
            "  Purpose: render a human-readable meeting record for audit. Usually human-only.",
            f"- `{init_command}`",
            "  Purpose: bootstrap a brand-new run and provision agents. Human/supervisor only.",
            "- Validation commands printed inside the active prompt or packet",
            "  Purpose: check that the JSON artifact you just edited matches the required eco-council schema. Safe to run when the prompt explicitly asks for validation.",
            "",
            "Policy note:",
            "- Mission `policy_profile` expands into the active caps and source-governance envelope shown in the current packet.",
            "- If the envelope is insufficient, return `override_requests` inside the requested JSON artifact. Those requests are advisory only until an upstream human/supervisor edits mission.json.",
            "",
            "Never do the following unless the human explicitly changes your role to supervisor operator:",
            "- Do not call `continue-run`, `run-agent-step`, or any `import-*` command from inside a normal role turn.",
            "- Do not run raw fetch shell commands during task-review, source-selection, data-readiness, matching-authorization, matching-adjudication, investigation-review, report, or decision turns.",
            "- Do not mutate files other than the target JSON artifact named by the current turn prompt.",
            "- Do not invent fetch results, readiness reports, matching outputs, evidence cards, or reports outside the current stage contract.",
        ]
    )


def write_openclaw_workspace_files(*, run_dir: Path, state: dict[str, Any], role: str, agent_id: str) -> None:
    workspace = agent_workspace_path(state, role)
    workspace.mkdir(parents=True, exist_ok=True)
    write_text(workspace / "IDENTITY.md", identity_text(role=role, agent_id=agent_id))
    write_text(agent_command_guide_path(state=state, role=role), openclaw_agent_guide_text(run_dir=run_dir, state=state, role=role))


def session_prompt_text(*, run_dir: Path, state: dict[str, Any], role: str, agent_id: str) -> str:
    header = [
        f"You are the fixed {role_display_name(role)} agent for this eco-council workflow.",
        f"OpenClaw agent id: {agent_id}",
        "",
        "Role rules:",
    ]
    if role == "moderator":
        rules = [
            "1. Stay in role for the full run.",
            "2. Only work on the JSON file/object explicitly requested by the supervisor.",
            "3. For task review turns, return only a JSON list of round-task objects.",
            "4. For matching-authorization turns, return only one JSON object shaped like matching-authorization.",
            "5. For matching-adjudication turns, return only one JSON object shaped like matching-adjudication.",
            "6. For investigation-review turns, return only one JSON object shaped like investigation-review.",
            "7. For decision turns, return only one JSON object shaped like council-decision.",
            "8. Never add markdown, prose, or code fences.",
            "9. Use the projected OpenClaw-managed eco-council skills in this profile; if one is missing, stop and ask the human to reprovision agents.",
            "10. If compact historical-case context is provided, use it only to prioritize work and avoid redundant fetch requests; never treat it as current-round evidence.",
            "11. Use packet causal_focus summaries to prioritize evidence needs, claim focus, and leg coverage in tasks or decisions; do not prescribe exact source skills or source-family layer upgrades.",
            "12. If policy caps block necessary next steps, use the decision override_requests field to ask an upstream human/bot for envelope changes instead of applying them yourself.",
        ]
    else:
        rules = [
            "1. Stay in role for the full run.",
            "2. Only work on the source-selection packet, data-readiness packet, or report packet explicitly requested by the supervisor.",
            "3. For source-selection turns, return only one JSON object shaped like source-selection.",
            "4. For data-readiness turns, return only one JSON object shaped like data-readiness-report.",
            "5. For report turns, return only one JSON object shaped like expert-report.",
            "6. Never add markdown, prose, or code fences.",
            "7. Do not invent new raw data fetch results in readiness or report stages.",
            "8. Use the projected OpenClaw-managed eco-council skills in this profile; if one is missing, stop and ask the human to reprovision agents.",
            "9. `recommended_next_actions` must be a list of objects with `assigned_role`, `objective`, and `reason`; use [] when there are no recommendations.",
            "10. Treat moderator tasks as evidence needs only. Use packet causal_focus, governance, family layers, and anchors to decide exact source skills.",
            "11. Never self-apply profile or governance changes. If the current envelope is insufficient, keep work inside bounds and use override_requests so an upstream human/bot can decide.",
        ]
    command_notes = [
        "",
        *installed_skill_guide_lines(state),
        "",
        "Supervisor command boundaries:",
        f"- Command guide: {agent_command_guide_path(state=state, role=role)}",
        f"- Safe read-only status command: {supervisor_status_command(run_dir)}",
        "- Use validation commands from the active prompt or packet when they are explicitly requested.",
        "- Do not call continue-run, run-agent-step, init-run, provision-openclaw-agents, or any import-* command unless the human explicitly asks you to act as the supervisor operator.",
        "- Raw fetch shell execution stays under supervisor control. Your role turns return JSON only.",
    ]
    return "\n".join(header + rules + command_notes)


def role_prompt_outbox_text(*, role: str, round_id: str, prompt_path: Path, history_path: Path | None = None) -> str:
    lines = [
        f"This is your current eco-council turn for {round_id}.",
        "",
        "Read and follow this run workspace prompt file exactly:",
        str(prompt_path),
        "",
        "If this agent cannot access that workspace path, stop and ask the human to repair workspace binding or reprovision the agent.",
        "Return only JSON.",
    ]
    if role == "moderator" and history_path is not None and history_path.exists():
        lines.extend(
            [
                "",
                "Also review this compact historical-case context before answering:",
                str(history_path),
                "Use it only as planning guidance. Current-round evidence remains primary.",
            ]
        )
    if role == "moderator":
        lines.insert(0, "Use your moderator session rules.")
    else:
        lines.insert(0, f"Use your {role} session rules.")
    return "\n".join(lines)


def maybe_compact_openclaw_message(
    *,
    inline_message: str,
    session_text: str,
    role: str,
    round_id: str,
    prompt_path: Path,
    history_path: Path | None = None,
) -> str:
    if len(inline_message) <= MAX_OPENCLAW_INLINE_MESSAGE_CHARS:
        return inline_message
    return "\n\n".join(
        [
            session_text,
            role_prompt_outbox_text(
                role=role,
                round_id=round_id,
                prompt_path=prompt_path,
                history_path=history_path,
            ),
        ]
    )


def existing_openclaw_agents(run_dir: Path) -> dict[str, dict[str, Any]]:
    payload = run_json_command(
        ["openclaw", "agents", "list", "--json"],
        cwd=PROJECT_DIR,
        env=openclaw_cli_env(run_dir),
    )
    if not isinstance(payload, list):
        raise ValueError("Unexpected openclaw agents list payload.")
    output: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        agent_id = maybe_text(item.get("id"))
        if agent_id:
            output[agent_id] = item
    return output


def identity_text(*, role: str, agent_id: str) -> str:
    values = {
        "moderator": {
            "name": "Eco Council Moderator",
            "creature": "procedural council chair",
            "vibe": "skeptical, structured, concise",
            "emoji": "gavel",
        },
        "sociologist": {
            "name": "Eco Council Sociologist",
            "creature": "public-opinion analyst",
            "vibe": "evidence-led, careful, restrained",
            "emoji": "speech",
        },
        "environmentalist": {
            "name": "Eco Council Environmentalist",
            "creature": "physical-signal analyst",
            "vibe": "technical, methodical, cautious",
            "emoji": "globe",
        },
    }[role]
    return "\n".join(
        [
            "# IDENTITY.md - Who Am I?",
            "",
            f"- **Name:** {values['name']}",
            f"- **Creature:** {values['creature']}",
            f"- **Vibe:** {values['vibe']}",
            f"- **Emoji:** {values['emoji']}",
            f"- **Guide:** {OPENCLAW_AGENT_GUIDE_FILENAME}",
            "- **Avatar:**",
            "",
            f"Agent id: {agent_id}",
        ]
    )


def ensure_openclaw_agent(
    run_dir: Path,
    *,
    role: str,
    state: dict[str, Any],
    mission: dict[str, Any] | None = None,
    sync_skills: bool = True,
    skills_root_text: str = "",
) -> dict[str, Any]:
    openclaw_section = ensure_openclaw_config(run_dir, state, skills_root_text=skills_root_text)
    if sync_skills:
        ensure_openclaw_skill_runtime(run_dir, state=state, mission=mission, skills_root_text=skills_root_text)
    agents = openclaw_section.setdefault("agents", {})
    role_info = agents.setdefault(role, {})
    agent_id = maybe_text(role_info.get("id"))
    if not agent_id:
        raise ValueError(f"Missing configured agent id for role {role}")
    write_openclaw_workspace_files(run_dir=run_dir, state=state, role=role, agent_id=agent_id)
    workspace = agent_workspace_path(state, role)

    env = openclaw_cli_env(run_dir)
    current_agents = existing_openclaw_agents(run_dir)
    if agent_id not in current_agents:
        run_json_command(
            [
                "openclaw",
                "agents",
                "add",
                agent_id,
                "--workspace",
                str(workspace),
                "--non-interactive",
                "--json",
            ],
            cwd=PROJECT_DIR,
            env=env,
        )
    run_json_command(
        [
            "openclaw",
            "agents",
            "set-identity",
            "--agent",
            agent_id,
            "--workspace",
            str(workspace),
            "--from-identity",
            "--json",
        ],
        cwd=PROJECT_DIR,
        env=env,
    )
    role_info["workspace"] = str(workspace)
    role_info["guide_path"] = str(agent_command_guide_path(state=state, role=role))
    return {
        "role": role,
        "agent_id": agent_id,
        "workspace": str(workspace),
        "guide_path": maybe_text(role_info.get("guide_path")),
    }


def provision_openclaw_agents_for_run(
    run_dir: Path,
    *,
    state: dict[str, Any],
    workspace_root_text: str,
    skills_root_text: str,
    assume_yes: bool,
    approval_callback: Callable[[str, bool], bool],
    require_approval: bool = False,
    mission: dict[str, Any] | None = None,
) -> dict[str, Any]:
    openclaw_section = ensure_openclaw_config(
        run_dir,
        state,
        workspace_root_text=workspace_root_text,
        skills_root_text=skills_root_text,
    )
    approved = approval_callback(
        "About to create or reuse three OpenClaw isolated agents for moderator/sociologist/environmentalist.",
        assume_yes,
    )
    if not approved:
        if require_approval:
            raise ValueError(
                "OpenClaw agent provisioning was declined. Re-run init-run with --yes or pass --no-provision-openclaw to skip agent creation."
            )
        return {
            "approved": False,
            "workspace_root": maybe_text(openclaw_section.get("workspace_root")),
            "created_agents": [],
        }
    skill_runtime = ensure_openclaw_skill_runtime(
        run_dir,
        state=state,
        mission=mission,
        skills_root_text=skills_root_text,
    )
    created = [
        ensure_openclaw_agent(
            run_dir,
            role=role,
            state=state,
            mission=mission,
            sync_skills=False,
            skills_root_text=skills_root_text,
        )
        for role in ROLES
    ]
    return {
        "approved": True,
        "workspace_root": maybe_text(openclaw_section.get("workspace_root")),
        "created_agents": created,
        "skill_runtime": skill_runtime,
    }
