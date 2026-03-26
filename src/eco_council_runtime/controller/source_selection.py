"""Source-selection packet and prompt builders for supervisor stages."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.controller.constants import DEFAULT_SCHEMA_VERSION
from eco_council_runtime.controller.io import load_json_if_exists, maybe_text, read_json, write_json, write_text
from eco_council_runtime.controller.paths import (
    investigation_plan_path,
    mission_path,
    source_selection_packet_path,
    source_selection_path,
    source_selection_prompt_path,
    tasks_path,
)
from eco_council_runtime.controller.policy import (
    allowed_sources_for_role,
    effective_constraints,
    load_override_requests,
    policy_profile_summary,
    resolve_schema_version,
    role_evidence_requirements,
    role_family_memory,
    role_source_governance,
)
from eco_council_runtime.layout import CONTRACT_SCRIPT_PATH

SCHEMA_VERSION = resolve_schema_version(DEFAULT_SCHEMA_VERSION)


def _tasks_for_role(tasks: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [task for task in tasks if maybe_text(task.get("assigned_role")) == role]


def build_source_selection_packet(run_dir: Path, round_id: str, role: str) -> Path:
    mission_payload = read_json(mission_path(run_dir))
    if not isinstance(mission_payload, dict):
        raise ValueError(f"Mission payload is not a JSON object: {mission_path(run_dir)}")
    task_payload = read_json(tasks_path(run_dir, round_id))
    tasks = task_payload if isinstance(task_payload, list) else []
    role_tasks = [item for item in _tasks_for_role(tasks, role) if isinstance(item, dict)]
    governance = role_source_governance(mission_payload, role)
    profile = policy_profile_summary(mission_payload)
    packet = {
        "schema_version": SCHEMA_VERSION,
        "packet_kind": "eco-council-source-selection-packet",
        "run_id": maybe_text(mission_payload.get("run_id")),
        "round_id": round_id,
        "agent_role": role,
        "mission": mission_payload,
        "investigation_plan": load_json_if_exists(investigation_plan_path(run_dir, round_id)),
        "policy_profile": profile,
        "effective_constraints": effective_constraints(mission_payload),
        "tasks": role_tasks,
        "evidence_requirements": role_evidence_requirements(role_tasks),
        "allowed_sources": allowed_sources_for_role(mission_payload, role),
        "governance": governance,
        "family_memory": role_family_memory(run_dir, round_id, role, mission_payload),
        "current_source_selection": load_json_if_exists(source_selection_path(run_dir, round_id, role)),
        "existing_override_requests": load_override_requests(run_dir, round_id, role),
    }
    target = source_selection_packet_path(run_dir, round_id, role)
    write_json(target, packet, pretty=True)
    return target


def render_source_selection_prompt(run_dir: Path, round_id: str, role: str) -> Path:
    packet_path = build_source_selection_packet(run_dir, round_id, role)
    target_path = source_selection_path(run_dir, round_id, role)
    validate_command = (
        "python3 "
        + str(CONTRACT_SCRIPT_PATH)
        + " validate --kind source-selection --input "
        + str(target_path)
        + " --pretty"
    )
    lines = [
        "Use the eco-council runtime contract validation command below.",
        f"Open source-selection packet at: {packet_path}",
        f"Write the canonical source-selection object at: {target_path}",
        "",
        "Review whether your role needs any raw-data fetch sources before prepare-round.",
        "Treat moderator tasks as evidence-need statements only. Do not treat them as source commands.",
        "Use packet.governance as the authority boundary: only upstream-approved or policy-auto layers may be selected.",
        "Use packet.investigation_plan as causal-chain context. If the round needs source, mechanism, impact, or public-interpretation coverage beyond the mission region, explain that need in family_plans or override_requests rather than ignoring it.",
        "Requirements:",
        "1. Return exactly one valid source-selection JSON object.",
        "2. Keep run_id, round_id, agent_role, task_ids, and allowed_sources aligned with the packet unless the packet itself is stale.",
        "3. selected_sources must be a subset of allowed_sources.",
        "4. Include one source_decisions entry for every allowed source with selected=true or selected=false and one concrete reason.",
        "4a. In each source_decisions item, use the exact key name source_skill (not source).",
        "5. status must be exactly one of: complete, pending, blocked. For a finished selection, use complete (not completed).",
        "6. If no raw fetch is needed, keep selected_sources as [] and explain why in summary.",
        "7. Fill family_plans explicitly. Include one family_plans entry for every governed family in packet.governance.families and one layer_plans entry for every governed layer.",
        "7a. Each family_plans entry must include selected and reason. Use the exact key name reason, not justification.",
        "7b. Each layer_plans entry must include selected and reason. Use the exact key name reason, not justification.",
        "7c. Put layer_plans inside each family_plans item. Do not return a top-level layer_plans field.",
        "8. Every layer_plans entry must include anchor_mode and anchor_refs. Use anchor_mode=none and anchor_refs=[] for L1 layers and any unanchored or unselected layer.",
        "9. For each selected L2 layer, provide a non-empty anchor_refs list and a non-none anchor_mode.",
        "10. Use authorization_basis=entry-layer for L1 entry layers, policy-auto for auto-selectable non-entry layers, and upstream-approval for packet.governance.approved_layers decisions.",
        "11. Do not invent moderator overrides or self-apply envelope changes. If governance or caps are insufficient, keep selection inside the current envelope and use override_requests with one or more explicit policy requests.",
        "12. Each override_requests item must stay aligned with the current run_id, round_id, agent_role, and request_origin_kind=source-selection. Use target_path only for the profile-governed fields listed in packet.policy_profile.overrideable_paths.",
        "13. If no override is needed, keep override_requests as [].",
        "",
        "After editing, validate with:",
        validate_command,
        "",
        "Return only the final JSON object.",
    ]
    output_path = source_selection_prompt_path(run_dir, round_id, role)
    write_text(output_path, "\n".join(lines))
    return output_path
