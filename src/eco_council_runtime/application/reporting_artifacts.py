"""Artifact assembly, prompt rendering, and draft promotion for reporting flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import atomic_write_text_file, load_json_if_exists, utc_now_iso, write_json
from eco_council_runtime.application.reporting_drafts import (
    build_data_readiness_draft,
    build_decision_draft_from_state,
    build_expert_report_draft_from_state,
    build_investigation_review_draft_from_state,
    report_is_placeholder,
)
from eco_council_runtime.application.reporting_state import (
    collect_round_state,
    load_dict_if_exists,
    load_override_requests,
    matching_executed_for_state,
    mission_constraints,
    mission_policy_profile,
    mission_run_id,
    state_auditable_submissions,
    state_current_submissions,
)
from eco_council_runtime.application.reporting_views import (
    build_claim_candidate_pool_summary,
    build_observation_candidate_pool_summary,
    compact_claim_candidate_for_curation,
    compact_evidence_card,
    compact_isolated_entry,
    compact_observation_candidate_for_curation,
    compact_remand_entry,
    load_context_or_fallback_from_state,
)
from eco_council_runtime.cli_invocation import runtime_module_command
from eco_council_runtime.controller.audit_chain import record_decision_phase_receipt
from eco_council_runtime.controller.paths import (
    claim_curation_draft_path,
    claim_curation_packet_path,
    claim_curation_path,
    claim_curation_prompt_path,
    claim_submissions_path,
    data_readiness_draft_path,
    data_readiness_packet_path,
    data_readiness_prompt_path,
    data_readiness_report_path,
    decision_draft_path,
    decision_packet_path,
    decision_prompt_path,
    decision_target_path,
    investigation_plan_path,
    investigation_review_draft_path,
    investigation_review_packet_path,
    investigation_review_path,
    investigation_review_prompt_path,
    matching_adjudication_draft_path,
    matching_adjudication_packet_path,
    matching_adjudication_path,
    matching_adjudication_prompt_path,
    matching_authorization_draft_path,
    matching_authorization_packet_path,
    matching_authorization_path,
    matching_authorization_prompt_path,
    matching_candidate_set_path,
    observation_curation_draft_path,
    observation_curation_packet_path,
    observation_curation_path,
    observation_curation_prompt_path,
    observation_submissions_path,
    report_draft_path,
    report_packet_path,
    report_prompt_path,
    report_target_path,
)
from eco_council_runtime.domain.contract_bridge import (
    contract_call,
    effective_matching_authorization,
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings
from eco_council_runtime.drafts import (
    can_replace_existing_exact,
    can_replace_existing_report,
    decision_prompt_text,
    load_draft_payload,
    promote_draft,
    report_prompt_text,
)
from eco_council_runtime.planning import combine_recommendations

SCHEMA_VERSION = resolve_schema_version("1.0.0")
REPORT_ROLES = ("sociologist", "environmentalist")
READINESS_ROLES = ("sociologist", "environmentalist")
PROMOTABLE_REPORT_ROLES = ("sociologist", "environmentalist", "historian")


def write_text(path: Path, text: str) -> None:
    atomic_write_text_file(path, text)


def load_report_for_decision(run_dir: Path, round_id: str, role: str, *, prefer_drafts: bool) -> tuple[dict[str, Any] | None, str]:
    final_report = load_json_if_exists(report_target_path(run_dir, round_id, role))
    if not isinstance(final_report, dict):
        final_report = None
    draft_report = load_json_if_exists(report_draft_path(run_dir, round_id, role))
    if not isinstance(draft_report, dict):
        draft_report = None
    if prefer_drafts and draft_report is not None:
        return draft_report, "draft"
    if final_report is not None:
        return final_report, "final"
    if draft_report is not None:
        return draft_report, "draft"
    return None, "missing"


def build_report_instructions(role: str) -> list[str]:
    instructions = [
        "Return one JSON object only, shaped like expert-report.",
        "Treat `context` as a compact summary layer first; only rely on `canonical_paths` when the summary is insufficient.",
        "Start with `context.causal_focus` as the role-prioritized causal summary before re-reading the full `investigation_plan`.",
        "Use `investigation_plan` to describe which causal legs are supported, unresolved, or still isolated.",
        "Use only claim_ids, observation_ids, and evidence_ids already present in the packet context.",
        "Do not invent coordinates, timestamps, or raw-source facts outside the packet.",
        "If evidence remains partial or mixed, keep status as needs-more-evidence.",
        "Keep each finding traceable to specific canonical objects.",
        "If you include recommended_next_actions, each item must be an object with assigned_role, objective, and reason.",
        "If policy caps or source-governance boundaries are insufficient, keep work inside the current envelope and use override_requests instead of self-applying mission changes.",
    ]
    if role == "sociologist":
        instructions.append("Emphasize claim phrasing, public narrative concentration, and what still needs corroboration.")
    else:
        instructions.append("Emphasize metric interpretation, provenance limits, and what is or is not physically supported.")
    return instructions


def build_report_packet(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    draft_report: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == role]
    existing_report = load_json_if_exists(report_target_path(run_dir, round_id, role))
    if not isinstance(existing_report, dict):
        existing_report = None
    return {
        "packet_kind": "expert-report-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, round_id)),
        "investigation_review": load_dict_if_exists(investigation_review_path(run_dir, round_id)),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": role,
        },
        "role": role,
        "task_scope": relevant_tasks,
        "context": context,
        "instructions": build_report_instructions(role),
        "existing_override_requests": load_override_requests(run_dir, round_id, role),
        "validation": {
            "kind": "expert-report",
            "target_report_path": str(report_target_path(run_dir, round_id, role)),
            "draft_report_path": str(report_draft_path(run_dir, round_id, role)),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "expert-report",
                "--input",
                report_draft_path(run_dir, round_id, role),
            ),
        },
        "existing_report": existing_report,
        "draft_report": draft_report,
    }


def build_claim_curation_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    candidates = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    status = "pending" if candidates else "blocked"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "curation_id": f"claim-curation-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "sociologist",
        "status": status,
        "summary": (
            f"Pending agent curation across {len(candidates)} claim candidates for this round."
            if candidates
            else "No claim candidates were available for curation."
        ),
        "curated_claims": [],
        "rejected_candidate_ids": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }
    validate_payload("claim-curation", payload)
    return payload


def build_observation_curation_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    candidates = state.get("observation_candidates_current", []) if isinstance(state.get("observation_candidates_current"), list) else []
    status = "pending" if candidates else "blocked"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "curation_id": f"observation-curation-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "environmentalist",
        "status": status,
        "summary": (
            f"Pending agent curation across {len(candidates)} observation candidates for this round."
            if candidates
            else "No observation candidates were available for curation."
        ),
        "curated_observations": [],
        "rejected_candidate_ids": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }
    validate_payload("observation-curation", payload)
    return payload


def build_claim_curation_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like claim-curation.",
        "Review the full candidate public-claim pool before deciding what enters the auditable library.",
        "Treat draft_curation as a blank scaffold, not as a recommended shortlist.",
        "Use candidate_pool.summary to preserve claim-type, channel, source-skill, and time coverage instead of selecting only the most repeated narratives.",
        "Use context.causal_focus as the role-prioritized causal summary before falling back to the full investigation_plan.",
        "Use investigation_plan to preserve causal-leg coverage; do not over-select one dominant narrative if it leaves other required legs unrepresented.",
        "You may merge multiple candidate_claim_ids into one curated claim when they express the same public narrative.",
        "Use only claim_ids and candidate_claim_ids already present in the packet.",
        "Prefer rejected_candidate_ids for discarded items; reserve worth_storing=false for rare edge cases you still want explicitly recorded.",
        "Keep summaries, statements, and meaning fields grounded in the packet candidate pool only.",
        "Every curated_claims item must include a non-empty meaning string and an integer priority from 1 to 5.",
        "Do not invent raw-source facts outside the packet.",
        "If the current envelope blocks the candidate diversity you need, use override_requests instead of silently expanding scope.",
    ]


def build_observation_curation_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like observation-curation.",
        "Review the full candidate physical-observation pool before deciding what enters the auditable library.",
        "Treat draft_curation as a blank scaffold, not as a recommended shortlist.",
        "Use candidate_pool.summary to preserve metric-family, source-skill, time, and spatial coverage instead of retaining only repeated atomic observations.",
        "Use context.causal_focus as the role-prioritized causal summary before falling back to the full investigation_plan.",
        "Use investigation_plan to preserve causal-leg coverage; composite observations may represent source, mechanism, impact, or contextual legs differently.",
        "You may keep observations atomic or combine multiple candidate_observation_ids into one composite observation.",
        "Composite observations must explicitly fill candidate_observation_ids, source_skills, metric_bundle, evidence_role, and component_roles.",
        "Use candidate statistics and distribution_summary when deciding whether one candidate is representative enough or whether multiple candidates should be combined.",
        "If you combine heterogeneous metrics or units, do not invent rolled-up numeric summaries; only provide composite value/statistics when you can defend the metric and unit.",
        "Use evidence_role and component_roles to distinguish primary, contextual, contradictory, or mixed parts of the observation.",
        "Do not let context-only weather background stand in for direct support unless the packet evidence itself justifies it.",
        "Use only candidate observation ids and candidate claim context already present in the packet.",
        "If the current envelope blocks necessary corroboration, use override_requests instead of silently expanding scope.",
    ]


def build_claim_curation_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    state: dict[str, Any],
    draft_curation: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == "sociologist"]
    existing_curation = load_dict_if_exists(claim_curation_path(run_dir, round_id))
    candidates = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    return {
        "packet_kind": "claim-curation-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, round_id)),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "sociologist",
        },
        "role": "sociologist",
        "task_scope": relevant_tasks,
        "context": context,
        "candidate_pool": {
            "candidate_count": len(candidates),
            "summary": build_claim_candidate_pool_summary(candidates),
            "claim_candidates": [compact_claim_candidate_for_curation(item) for item in candidates],
        },
        "instructions": build_claim_curation_instructions(),
        "existing_override_requests": load_override_requests(run_dir, round_id, "sociologist"),
        "existing_curation": existing_curation,
        "draft_curation": draft_curation,
        "validation": {
            "kind": "claim-curation",
            "target_curation_path": str(claim_curation_path(run_dir, round_id)),
            "draft_curation_path": str(claim_curation_draft_path(run_dir, round_id)),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "claim-curation",
                "--input",
                claim_curation_draft_path(run_dir, round_id),
            ),
        },
    }


def build_observation_curation_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    state: dict[str, Any],
    draft_curation: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == "environmentalist"]
    existing_curation = load_dict_if_exists(observation_curation_path(run_dir, round_id))
    observation_candidates = (
        state.get("observation_candidates_current", [])
        if isinstance(state.get("observation_candidates_current"), list)
        else []
    )
    claim_candidates = state.get("claim_candidates_current", []) if isinstance(state.get("claim_candidates_current"), list) else []
    return {
        "packet_kind": "observation-curation-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, round_id)),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "environmentalist",
        },
        "role": "environmentalist",
        "task_scope": relevant_tasks,
        "context": context,
        "candidate_pool": {
            "claim_candidate_count": len(claim_candidates),
            "observation_candidate_count": len(observation_candidates),
            "summary": build_observation_candidate_pool_summary(observation_candidates),
            "claim_candidates": [compact_claim_candidate_for_curation(item) for item in claim_candidates],
            "observation_candidates": [compact_observation_candidate_for_curation(item) for item in observation_candidates],
        },
        "instructions": build_observation_curation_instructions(),
        "existing_override_requests": load_override_requests(run_dir, round_id, "environmentalist"),
        "existing_curation": existing_curation,
        "draft_curation": draft_curation,
        "validation": {
            "kind": "observation-curation",
            "target_curation_path": str(observation_curation_path(run_dir, round_id)),
            "draft_curation_path": str(observation_curation_draft_path(run_dir, round_id)),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "observation-curation",
                "--input",
                observation_curation_draft_path(run_dir, round_id),
            ),
        },
    }


def claim_curation_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the sociologist for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope`, `context` (especially `context.causal_focus`), `investigation_plan`, and `candidate_pool` before editing.",
        "3. Use `draft_curation` only as a scaffold; the final curated set must be chosen from the full candidate pool.",
        "4. Return only one JSON object shaped like claim-curation.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "5a. Every curated_claims item must include `meaning` and integer `priority` fields.",
        "6. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_curation_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def observation_curation_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the environmentalist for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope`, `context` (especially `context.causal_focus`), `investigation_plan`, and `candidate_pool` before editing.",
        "3. Use `draft_curation` only as a scaffold; the final curated set must be chosen from the full candidate pool.",
        "4. Return only one JSON object shaped like observation-curation.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_curation_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def build_data_readiness_instructions(role: str) -> list[str]:
    instructions = [
        "Return one JSON object only, shaped like data-readiness-report.",
        "Judge whether the auditable submission library available in this round is sufficiently representative for a later matching pass.",
        "Start with context.causal_focus as the role-prioritized causal summary before re-reading the full investigation_plan.",
        "Use investigation_plan to judge whether required causal legs are actually represented, not just whether counts look non-zero.",
        "Use submission_ids, claim_ids, and observation_ids already present in the packet context only.",
        "Do not invent raw-source facts outside the packet.",
        "If the compact representation is not sufficiently representative, use readiness_status=needs-more-data.",
        "If no auditable submissions are available, use readiness_status=blocked.",
        "Before the first matching pass, zero evidence cards is expected and must not be treated as a readiness failure by itself.",
        "Keep findings and recommendations traceable to the packet context.",
        "If the current mission envelope blocks the missing preparation you need, keep the report inside bounds and use override_requests for upstream review.",
    ]
    if role == "sociologist":
        instructions.append("Focus on narrative concentration, channel diversity, attribution clarity, and whether the compact claim set is representative.")
    else:
        instructions.append("Focus on metric coverage, provenance limits, and whether the compact observation set is representative enough for matching.")
    return instructions


def build_data_readiness_packet(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    context: dict[str, Any],
    draft_report: dict[str, Any],
) -> dict[str, Any]:
    relevant_tasks = [task for task in tasks if maybe_text(task.get("assigned_role")) == role]
    existing_report = load_dict_if_exists(data_readiness_report_path(run_dir, round_id, role))
    return {
        "packet_kind": "data-readiness-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, round_id)),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": role,
        },
        "role": role,
        "task_scope": relevant_tasks,
        "context": context,
        "instructions": build_data_readiness_instructions(role),
        "existing_override_requests": load_override_requests(run_dir, round_id, role),
        "validation": {
            "kind": "data-readiness-report",
            "target_report_path": str(data_readiness_report_path(run_dir, round_id, role)),
            "draft_report_path": str(data_readiness_draft_path(run_dir, round_id, role)),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "data-readiness-report",
                "--input",
                data_readiness_draft_path(run_dir, round_id, role),
            ),
        },
        "existing_report": existing_report,
        "draft_report": draft_report,
    }


def data_readiness_prompt_text(*, role: str, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the {role} for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope`, `context` (especially `context.causal_focus`), and `investigation_plan` before editing.",
        "3. Start from `draft_report` inside the packet.",
        "4. Return only one JSON object shaped like data-readiness-report.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. Keep `override_requests` as [] unless the current mission envelope itself is insufficient.",
        "7. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_report_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def build_matching_authorization_draft(
    *,
    mission: dict[str, Any],
    round_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    readiness_reports = state.get("readiness_reports", {}) if isinstance(state.get("readiness_reports"), dict) else {}
    reports = {role: report for role, report in readiness_reports.items() if isinstance(report, dict) and report}
    statuses = {role: maybe_text(report.get("readiness_status")) for role, report in reports.items()}
    final_round = bool(contract_call("is_final_allowed_round", mission, round_id))
    if any(status == "blocked" for status in statuses.values()):
        base_status = "not-authorized"
    elif set(reports) == set(READINESS_ROLES) and all(bool(report.get("sufficient_for_matching")) for report in reports.values()):
        base_status = "authorized"
    else:
        base_status = "deferred"
    if base_status == "authorized":
        summary = "Both data roles report that the current auditable submissions are sufficiently prepared for a matching pass."
        rationale = "Matching should proceed before requesting broader collection because both sides judge the current compact evidence library to be representative enough."
    elif base_status == "not-authorized":
        summary = "At least one data role reports a blocked readiness state, so matching is not yet authorized under normal readiness rules."
        rationale = "Matching is not normally reasonable when one side lacks auditable submissions or is structurally blocked."
    else:
        summary = "Current data readiness is incomplete or mixed, so matching is deferred pending more preparation under normal readiness rules."
        rationale = "Matching should usually wait until both roles either report sufficiency or the upstream operator explicitly changes the collection boundary."
    claim_ids = [
        maybe_text(item.get("claim_id"))
        for item in state.get("claims_active", [])
        if isinstance(item, dict) and maybe_text(item.get("claim_id"))
    ]
    observation_ids = [
        maybe_text(item.get("observation_id"))
        for item in state.get("observations_active", [])
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    ]
    referenced_readiness_ids = [
        maybe_text(report.get("readiness_id"))
        for report in reports.values()
        if maybe_text(report.get("readiness_id"))
    ]
    open_questions: list[str] = []
    recommendations = combine_recommendations(reports=list(reports.values()), missing_types=[])
    for report in reports.values():
        open_questions.extend(maybe_text(item) for item in report.get("open_questions", []) if maybe_text(item))
    if final_round and base_status != "authorized":
        open_questions.append("This is the final allowed round. Which isolated or remand outcomes remain acceptable if direct matches stay limited?")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "authorization_id": f"matchauth-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "moderator",
        "authorization_status": base_status,
        "moderator_requested_status": base_status,
        "authorization_basis": (
            "readiness-ready"
            if base_status == "authorized"
            else "readiness-blocked"
            if base_status == "not-authorized"
            else "readiness-deferred"
        ),
        "summary": truncate_text(summary, 400),
        "rationale": truncate_text(rationale, 500),
        "moderator_override": False,
        "allow_isolated_evidence": base_status == "authorized" or final_round,
        "referenced_readiness_ids": referenced_readiness_ids,
        "claim_ids": unique_strings(claim_ids),
        "observation_ids": unique_strings(observation_ids),
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations[:4],
    }
    payload = effective_matching_authorization(mission=mission, round_id=round_id, authorization=payload)
    validate_payload("matching-authorization", payload)
    return payload


def build_matching_authorization_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like matching-authorization.",
        "Start with context.causal_focus as the compact moderator view of the causal chain before re-reading the full investigation_plan.",
        "Authorize matching based on the auditable submission libraries and readiness reports, not on the presence of pre-existing evidence cards.",
        "If this is the final allowed round, the council must still run one terminal matching/adjudication pass and may end with matched, isolated, or remand evidence.",
        "Use only claim_ids and observation_ids already present in the packet.",
        "Do not invent new evidence or prescribe exact source skills.",
        "If authorization is deferred or denied, keep recommended_next_actions limited to evidence needs rather than collection commands.",
        "Do not use moderator_override to bypass mission policy_profile or source-governance boundaries. Upstream envelope changes must stay in override_requests on source-selection, readiness, report, or decision objects instead.",
        "allow_isolated_evidence should stay true unless the packet gives a specific reason to require strict remand-only handling.",
    ]


def build_matching_authorization_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    context: dict[str, Any],
    state: dict[str, Any],
    draft_authorization: dict[str, Any],
) -> dict[str, Any]:
    return {
        "packet_kind": "matching-authorization-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, round_id)),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "moderator",
        },
        "context": context,
        "readiness_reports": state.get("readiness_reports", {}),
        "pending_override_requests": load_override_requests(run_dir, round_id),
        "instructions": [
            "Return one JSON object only, shaped like matching-authorization.",
            "Authorize matching based on auditable submissions and readiness reports. Zero evidence cards before the first match is expected.",
            "If this is the final allowed round, the output must still permit one terminal matching/adjudication pass.",
            "Use the packet context and referenced readiness reports only; do not invent raw data or future rounds.",
            "Use `context.causal_focus` as the compact moderator view of the causal chain before re-reading the full investigation_plan.",
            "Use investigation_plan as causal-chain context. Matching may be partial by leg, and isolated evidence is valid when some legs remain unresolved.",
            "If readiness is incomplete or blocked, use authorization_status=deferred or not-authorized and recommend the next data-preparation actions instead.",
            "Keep claim_ids and observation_ids restricted to canonical ids already present in the packet.",
            "Pending override requests are advisory context only; they do not authorize envelope changes inside matching-authorization.",
        ],
        "validation": {
            "kind": "matching-authorization",
            "target_authorization_path": str(matching_authorization_path(run_dir, round_id)),
            "draft_authorization_path": str(matching_authorization_draft_path(run_dir, round_id)),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "matching-authorization",
                "--input",
                matching_authorization_draft_path(run_dir, round_id),
            ),
            "promote_command": runtime_module_command(
                "reporting",
                "promote-matching-authorization-draft",
                "--run-dir",
                run_dir,
                "--round-id",
                round_id,
            ),
        },
        "existing_authorization": load_dict_if_exists(matching_authorization_path(run_dir, round_id)),
        "draft_authorization": draft_authorization,
    }


def matching_authorization_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the moderator for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `context` (especially `context.causal_focus`), `investigation_plan`, and `readiness_reports` before editing.",
        "3. Start from `draft_authorization` inside the packet.",
        "4. Return only one JSON object shaped like matching-authorization.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. Do not use moderator_override to bypass mission policy_profile or source-governance boundaries.",
        "7. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_authorization_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Promotion command:",
        maybe_text(validation.get("promote_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def build_matching_adjudication_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like matching-adjudication.",
        "Treat packet.candidate_set and draft_adjudication as rule-nominated inputs, not as a binding final result.",
        "Start with context.causal_focus as the compact moderator view of the causal chain before re-reading the full investigation_plan.",
        "Use investigation_plan to evaluate whether matched, isolated, and remand evidence sufficiently cover the causal legs under review.",
        "You may merge or prune nominated observation clusters as long as all claim_ids and observation_ids stay within the authorized packet scope.",
        "Use isolated_entries for acceptable but unmatched evidence; use remand_entries for evidence that still needs targeted follow-up.",
        "Keep matching_result, evidence_cards, isolated_entries, remand_entries, and evidence_adjudication mutually consistent.",
        "matching_result.result_status must be exactly one of: matched, partial, unmatched. Do not use complete or completed.",
        "Do not invent raw facts, new source artifacts, or ids outside the packet candidate set and current round context.",
        "If allow_isolated_evidence is false, leave unmatched items in remand_entries instead of isolated_entries.",
        "Use recommended_next_actions only for concrete evidence needs; do not prescribe exact source skills or self-apply policy changes.",
    ]


def build_matching_adjudication_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    context: dict[str, Any],
    state: dict[str, Any],
    candidate_set: dict[str, Any],
    draft_adjudication: dict[str, Any],
) -> dict[str, Any]:
    return {
        "packet_kind": "matching-adjudication-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, round_id)),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "moderator",
        },
        "context": context,
        "readiness_reports": state.get("readiness_reports", {}),
        "matching_authorization": state.get("matching_authorization", {}),
        "candidate_set": candidate_set,
        "pending_override_requests": load_override_requests(run_dir, round_id),
        "instructions": build_matching_adjudication_instructions(),
        "validation": {
            "kind": "matching-adjudication",
            "target_adjudication_path": str(matching_adjudication_path(run_dir, round_id)),
            "draft_adjudication_path": str(matching_adjudication_draft_path(run_dir, round_id)),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "matching-adjudication",
                "--input",
                matching_adjudication_draft_path(run_dir, round_id),
            ),
            "promote_command": runtime_module_command(
                "reporting",
                "promote-matching-adjudication-draft",
                "--run-dir",
                run_dir,
                "--round-id",
                round_id,
            ),
        },
        "existing_adjudication": load_dict_if_exists(matching_adjudication_path(run_dir, round_id)),
        "draft_adjudication": draft_adjudication,
    }


def matching_adjudication_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the moderator for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `context` (especially `context.causal_focus`), `investigation_plan`, `readiness_reports`, `matching_authorization`, and `candidate_set` before editing.",
        "3. Start from `draft_adjudication` inside the packet.",
        "4. Return only one JSON object shaped like matching-adjudication.",
        "5. Keep `schema_version`, `run_id`, `round_id`, `agent_role`, and `authorization_id` consistent with the packet.",
        "6. Only use claim ids and observation ids already present in the authorized candidate set or draft.",
        "6a. Set matching_result.result_status to exactly one of matched, partial, unmatched.",
        "7. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_adjudication_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Promotion command:",
        maybe_text(validation.get("promote_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def build_investigation_review_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like investigation-review.",
        "Treat the packet draft as a rule-prepared starting point, not as a binding final answer.",
        "Start with context.causal_focus as the compact moderator view of the causal chain before re-reading the full investigation_plan.",
        "Use investigation_plan to audit each hypothesis and each causal leg explicitly.",
        "Judge whether the current matching is reasonable enough for expert reporting or whether remands and isolated evidence still need more explanation.",
        "Keep matched_card_ids, isolated_entry_ids, remand_ids, and evidence_refs restricted to canonical ids already present in the packet.",
        "If a leg remains unresolved, say so explicitly instead of forcing a match.",
        "Use recommended_next_actions only for concrete evidence needs; do not prescribe exact source skills or self-apply policy changes.",
    ]


def build_investigation_review_packet(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    context: dict[str, Any],
    state: dict[str, Any],
    draft_review: dict[str, Any],
) -> dict[str, Any]:
    return {
        "packet_kind": "investigation-review-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(mission),
        "effective_constraints": mission_constraints(mission),
        "investigation_plan": state.get("investigation_plan", {}),
        "run": {
            "run_id": mission_run_id(mission),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "role": "moderator",
        },
        "context": context,
        "matching_authorization": state.get("matching_authorization", {}),
        "matching_adjudication": state.get("matching_adjudication", {}),
        "matching_result": state.get("matching_result", {}),
        "evidence_adjudication": state.get("evidence_adjudication", {}),
        "cards_active": [compact_evidence_card(item) for item in state.get("cards_active", [])[:12]],
        "isolated_active": [compact_isolated_entry(item) for item in state.get("isolated_active", [])[:12]],
        "remands_open": [compact_remand_entry(item) for item in state.get("remands_open", [])[:12]],
        "pending_override_requests": load_override_requests(run_dir, round_id),
        "instructions": build_investigation_review_instructions(),
        "validation": {
            "kind": "investigation-review",
            "target_review_path": str(investigation_review_path(run_dir, round_id)),
            "draft_review_path": str(investigation_review_draft_path(run_dir, round_id)),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "investigation-review",
                "--input",
                investigation_review_draft_path(run_dir, round_id),
            ),
            "promote_command": runtime_module_command(
                "reporting",
                "promote-investigation-review-draft",
                "--run-dir",
                run_dir,
                "--round-id",
                round_id,
            ),
        },
        "existing_review": load_dict_if_exists(investigation_review_path(run_dir, round_id)),
        "draft_review": draft_review,
    }


def investigation_review_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the moderator for eco-council run {maybe_text(run.get('run_id'))} round {maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `context` (especially `context.causal_focus`), `investigation_plan`, `matching_result`, `evidence_adjudication`, `cards_active`, `isolated_active`, and `remands_open` before editing.",
        "3. Start from `draft_review` inside the packet.",
        "4. Return only one JSON object shaped like investigation-review.",
        "5. Keep `schema_version`, `run_id`, `round_id`, `agent_role`, `authorization_id`, and `matching_result_id` consistent with the packet.",
        "6. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        maybe_text(validation.get("draft_review_path")),
        "",
        "Validation command:",
        maybe_text(validation.get("validate_command")),
        "",
        "Promotion command:",
        maybe_text(validation.get("promote_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def build_decision_packet_from_state(
    *,
    run_dir: Path,
    state: dict[str, Any],
    next_round_id: str,
    moderator_context: dict[str, Any],
    reports: dict[str, dict[str, Any] | None],
    report_sources: dict[str, str],
    draft_decision: dict[str, Any],
    proposed_next_round_tasks: list[dict[str, Any]],
    missing_evidence_types: list[str],
) -> dict[str, Any]:
    return {
        "packet_kind": "council-decision-packet",
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "policy_profile": mission_policy_profile(state["mission"]),
        "effective_constraints": mission_constraints(state["mission"]),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, state["round_id"])),
        "run": {
            "run_id": mission_run_id(state["mission"]),
            "round_id": state["round_id"],
            "next_round_id": next_round_id,
            "topic": maybe_text(state["mission"].get("topic")),
            "objective": maybe_text(state["mission"].get("objective")),
        },
        "round_context": moderator_context,
        "readiness_reports": state.get("readiness_reports", {}),
        "matching_authorization": state.get("matching_authorization", {}),
        "matching_adjudication": state.get("matching_adjudication", {}),
        "matching_result": state.get("matching_result", {}),
        "evidence_adjudication": state.get("evidence_adjudication", {}),
        "investigation_review": state.get("investigation_review", {}),
        "reports": reports,
        "report_sources": report_sources,
        "missing_evidence_types": missing_evidence_types,
        "proposed_next_round_tasks": proposed_next_round_tasks,
        "pending_override_requests": load_override_requests(run_dir, state["round_id"]),
        "instructions": [
            "Return one JSON object only, shaped like council-decision.",
            "Base the decision on readiness reports, matching authorization, matching/adjudication artifacts, and expert-report content, not on raw fetch artifacts.",
            "Treat `round_context` as a compact summary layer first and consult `canonical_paths` only if a summary detail is insufficient.",
            "Use `round_context.causal_focus` as the moderator's compact causal summary before re-reading the full investigation_plan.",
            "Use `investigation_plan` to judge whether source, mechanism, impact, and public-interpretation legs are adequately covered or still need follow-up.",
            "Treat `investigation_review` as the moderator's explicit causal-leg audit before closure or another round.",
            "If another round is required, add new round-task objects for next_round_id instead of editing current tasks in place.",
            "Respect mission constraints such as max_rounds and max_tasks_per_round.",
            "Use anchor_refs and evidence gaps to define follow-up scope; do not prescribe concrete source skills inside moderator tasks.",
            "If mission constraints block a necessary follow-up round or task envelope, keep the decision inside the current boundary and use override_requests for upstream review.",
            "Keep final_brief empty unless the council is complete or blocked.",
        ],
        "validation": {
            "kind": "council-decision",
            "target_decision_path": str(decision_target_path(run_dir, state["round_id"])),
            "draft_decision_path": str(decision_draft_path(run_dir, state["round_id"])),
            "validate_command": runtime_module_command(
                "contract",
                "validate",
                "--kind",
                "council-decision",
                "--input",
                decision_draft_path(run_dir, state["round_id"]),
            ),
        },
        "draft_decision": draft_decision,
    }


def load_report_draft_payload(run_dir: Path, round_id: str, role: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=report_draft_path(run_dir, round_id, role),
        label=f"{role} report draft",
        round_error_label="Report draft",
        expected_round_id=round_id,
        expected_role=role,
        role_error_label="Report draft",
        kind="expert-report",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_decision_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=decision_draft_path(run_dir, round_id),
        label="moderator decision draft",
        round_error_label="Decision draft",
        expected_round_id=round_id,
        expected_role=None,
        role_error_label=None,
        kind="council-decision",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_matching_authorization_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=matching_authorization_draft_path(run_dir, round_id),
        label="moderator matching-authorization draft",
        round_error_label="Matching-authorization draft",
        expected_round_id=round_id,
        expected_role="moderator",
        role_error_label="Matching-authorization draft",
        kind="matching-authorization",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_matching_adjudication_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=matching_adjudication_draft_path(run_dir, round_id),
        label="moderator matching-adjudication draft",
        round_error_label="Matching-adjudication draft",
        expected_round_id=round_id,
        expected_role="moderator",
        role_error_label="Matching-adjudication draft",
        kind="matching-adjudication",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_investigation_review_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=investigation_review_draft_path(run_dir, round_id),
        label="moderator investigation-review draft",
        round_error_label="Investigation-review draft",
        expected_round_id=round_id,
        expected_role="moderator",
        role_error_label="Investigation-review draft",
        kind="investigation-review",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def promote_report_draft(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_report_draft_payload(run_dir, round_id, role, draft_path_text)
    result = promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=report_target_path(run_dir, round_id, role),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical report",
        overwrite_error_message="Refusing to overwrite non-placeholder canonical report without --allow-overwrite",
        can_replace_existing=lambda existing_payload, new_payload: can_replace_existing_report(
            existing_payload,
            new_payload,
            report_is_placeholder=report_is_placeholder,
        ),
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )
    return {
        **result,
        "role": role,
    }


def promote_decision_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_decision_draft_payload(run_dir, round_id, draft_path_text)
    result = promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=decision_target_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical decision",
        overwrite_error_message="Refusing to overwrite canonical decision without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )
    record_decision_phase_receipt(
        run_dir=run_dir,
        round_id=round_id,
        decision_payload=payload,
    )
    return result


def promote_matching_authorization_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_matching_authorization_draft_payload(run_dir, round_id, draft_path_text)
    return promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=matching_authorization_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical matching-authorization",
        overwrite_error_message="Refusing to overwrite canonical matching-authorization without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )


def promote_matching_adjudication_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_matching_adjudication_draft_payload(run_dir, round_id, draft_path_text)
    return promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=matching_adjudication_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical matching-adjudication",
        overwrite_error_message="Refusing to overwrite canonical matching-adjudication without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )


def promote_investigation_review_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_investigation_review_draft_payload(run_dir, round_id, draft_path_text)
    return promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=investigation_review_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical investigation-review",
        overwrite_error_message="Refusing to overwrite canonical investigation-review without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )


def curation_status_complete(curation: dict[str, Any]) -> bool:
    status = maybe_text(curation.get("status"))
    return status in {"complete", "blocked"}


def curations_materialized_for_round(*, run_dir: Path, round_id: str, state: dict[str, Any]) -> bool:
    claim_curation = state.get("claim_curation", {}) if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = state.get("observation_curation", {}) if isinstance(state.get("observation_curation"), dict) else {}
    if not curation_status_complete(claim_curation) or not curation_status_complete(observation_curation):
        return False
    required_paths = (
        claim_curation_path(run_dir, round_id),
        observation_curation_path(run_dir, round_id),
        claim_submissions_path(run_dir, round_id),
        observation_submissions_path(run_dir, round_id),
    )
    if not all(path.exists() for path in required_paths):
        return False
    latest_curation_mtime = max(
        claim_curation_path(run_dir, round_id).stat().st_mtime_ns,
        observation_curation_path(run_dir, round_id).stat().st_mtime_ns,
    )
    earliest_materialized_mtime = min(
        claim_submissions_path(run_dir, round_id).stat().st_mtime_ns,
        observation_submissions_path(run_dir, round_id).stat().st_mtime_ns,
    )
    return earliest_materialized_mtime >= latest_curation_mtime


def curation_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    mission = state["mission"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []

    sociologist_context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="sociologist")
    claim_draft = build_claim_curation_draft(
        mission=mission,
        round_id=round_id,
        state=state,
    )
    claim_packet = build_claim_curation_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        tasks=tasks,
        context=sociologist_context,
        state=state,
        draft_curation=claim_draft,
    )
    claim_packet_file = claim_curation_packet_path(run_dir, round_id)
    claim_draft_file = claim_curation_draft_path(run_dir, round_id)
    write_json(claim_packet_file, claim_packet, pretty=pretty)
    write_json(claim_draft_file, claim_draft, pretty=pretty)

    environmentalist_context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="environmentalist")
    observation_draft = build_observation_curation_draft(
        mission=mission,
        round_id=round_id,
        state=state,
    )
    observation_packet = build_observation_curation_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        tasks=tasks,
        context=environmentalist_context,
        state=state,
        draft_curation=observation_draft,
    )
    observation_packet_file = observation_curation_packet_path(run_dir, round_id)
    observation_draft_file = observation_curation_draft_path(run_dir, round_id)
    write_json(observation_packet_file, observation_packet, pretty=pretty)
    write_json(observation_draft_file, observation_draft, pretty=pretty)

    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "claim_candidate_count": len(state.get("claim_candidates_current", [])),
        "observation_candidate_count": len(state.get("observation_candidates_current", [])),
        "outputs": {
            "sociologist": {
                "packet_path": str(claim_packet_file),
                "draft_path": str(claim_draft_file),
            },
            "environmentalist": {
                "packet_path": str(observation_packet_file),
                "draft_path": str(observation_draft_file),
            },
        },
    }


def render_openclaw_prompts(
    *,
    run_dir: Path,
    round_id: str,
) -> dict[str, Any]:
    outputs: dict[str, str] = {}
    claim_packet_path_file = claim_curation_packet_path(run_dir, round_id)
    claim_packet = load_json_if_exists(claim_packet_path_file)
    if isinstance(claim_packet, dict):
        prompt_path = claim_curation_prompt_path(run_dir, round_id)
        write_text(prompt_path, claim_curation_prompt_text(packet_path=claim_packet_path_file, packet=claim_packet))
        outputs["sociologist_claim_curation"] = str(prompt_path)
    observation_packet_path_file = observation_curation_packet_path(run_dir, round_id)
    observation_packet = load_json_if_exists(observation_packet_path_file)
    if isinstance(observation_packet, dict):
        prompt_path = observation_curation_prompt_path(run_dir, round_id)
        write_text(
            prompt_path,
            observation_curation_prompt_text(packet_path=observation_packet_path_file, packet=observation_packet),
        )
        outputs["environmentalist_observation_curation"] = str(prompt_path)
    for role in READINESS_ROLES:
        packet_path = data_readiness_packet_path(run_dir, round_id, role)
        packet = load_json_if_exists(packet_path)
        if isinstance(packet, dict):
            prompt_path = data_readiness_prompt_path(run_dir, round_id, role)
            write_text(prompt_path, data_readiness_prompt_text(role=role, packet_path=packet_path, packet=packet))
            outputs[f"{role}_data_readiness"] = str(prompt_path)
    auth_packet_path = matching_authorization_packet_path(run_dir, round_id)
    auth_packet = load_json_if_exists(auth_packet_path)
    if isinstance(auth_packet, dict):
        auth_prompt_path = matching_authorization_prompt_path(run_dir, round_id)
        write_text(auth_prompt_path, matching_authorization_prompt_text(packet_path=auth_packet_path, packet=auth_packet))
        outputs["moderator_matching_authorization"] = str(auth_prompt_path)
    adjudication_packet_path = matching_adjudication_packet_path(run_dir, round_id)
    adjudication_packet = load_json_if_exists(adjudication_packet_path)
    if isinstance(adjudication_packet, dict):
        adjudication_prompt = matching_adjudication_prompt_path(run_dir, round_id)
        write_text(
            adjudication_prompt,
            matching_adjudication_prompt_text(packet_path=adjudication_packet_path, packet=adjudication_packet),
        )
        outputs["moderator_matching_adjudication"] = str(adjudication_prompt)
    review_packet_path_file = investigation_review_packet_path(run_dir, round_id)
    review_packet = load_json_if_exists(review_packet_path_file)
    if isinstance(review_packet, dict):
        review_prompt = investigation_review_prompt_path(run_dir, round_id)
        write_text(
            review_prompt,
            investigation_review_prompt_text(packet_path=review_packet_path_file, packet=review_packet),
        )
        outputs["moderator_investigation_review"] = str(review_prompt)
    for role in REPORT_ROLES:
        packet_path = report_packet_path(run_dir, round_id, role)
        packet = load_json_if_exists(packet_path)
        if isinstance(packet, dict):
            prompt_path = report_prompt_path(run_dir, round_id, role)
            write_text(prompt_path, report_prompt_text(role=role, packet_path=packet_path, packet=packet))
            outputs[f"{role}_report"] = str(prompt_path)
    packet_path = decision_packet_path(run_dir, round_id)
    packet = load_json_if_exists(packet_path)
    if isinstance(packet, dict):
        moderator_prompt_path = decision_prompt_path(run_dir, round_id)
        write_text(moderator_prompt_path, decision_prompt_text(packet_path=packet_path, packet=packet))
        outputs["moderator_decision"] = str(moderator_prompt_path)
    if not outputs:
        raise ValueError(
            f"No curation, readiness, authorization, matching-adjudication, investigation-review, report, or decision packets exist for {round_id}."
        )
    return outputs


def data_readiness_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    if not curations_materialized_for_round(run_dir=run_dir, round_id=round_id, state=state):
        raise ValueError(
            "Data-readiness packets require completed claim/observation curation plus refreshed "
            "materialized submissions. Run normalize materialize-curations after both curation payloads are imported."
        )
    mission = state["mission"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    max_findings = mission_constraints(mission).get("max_claims_per_round", 4)
    outputs: dict[str, dict[str, str]] = {}
    for role in READINESS_ROLES:
        context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role=role)
        draft_report = build_data_readiness_draft(
            mission=mission,
            round_id=round_id,
            role=role,
            state=state,
            max_findings=max_findings,
        )
        packet = build_data_readiness_packet(
            run_dir=run_dir,
            round_id=round_id,
            role=role,
            mission=mission,
            tasks=tasks,
            context=context,
            draft_report=draft_report,
        )
        packet_path = data_readiness_packet_path(run_dir, round_id, role)
        draft_path = data_readiness_draft_path(run_dir, round_id, role)
        write_json(packet_path, packet, pretty=pretty)
        write_json(draft_path, draft_report, pretty=pretty)
        outputs[role] = {
            "packet_path": str(packet_path),
            "draft_path": str(draft_path),
        }
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "claim_submission_count": len(state_auditable_submissions(state, "sociologist")),
        "observation_submission_count": len(state_auditable_submissions(state, "environmentalist")),
        "claim_submission_current_count": len(state_current_submissions(state, "sociologist")),
        "observation_submission_current_count": len(state_current_submissions(state, "environmentalist")),
        "outputs": outputs,
    }


def matching_authorization_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    mission = state["mission"]
    context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="moderator")
    draft_authorization = build_matching_authorization_draft(
        mission=mission,
        round_id=round_id,
        state=state,
    )
    packet = build_matching_authorization_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        context=context,
        state=state,
        draft_authorization=draft_authorization,
    )
    packet_path = matching_authorization_packet_path(run_dir, round_id)
    draft_path = matching_authorization_draft_path(run_dir, round_id)
    write_json(packet_path, packet, pretty=pretty)
    write_json(draft_path, draft_authorization, pretty=pretty)
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "matching_authorization_packet_path": str(packet_path),
        "matching_authorization_draft_path": str(draft_path),
    }


def matching_adjudication_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    authorization = state.get("matching_authorization", {}) if isinstance(state.get("matching_authorization"), dict) else {}
    if maybe_text(authorization.get("authorization_status")) != "authorized":
        raise ValueError("Matching-adjudication packets require canonical matching_authorization.json with authorization_status=authorized.")
    authorization_id = maybe_text(authorization.get("authorization_id"))
    candidate_set = load_dict_if_exists(matching_candidate_set_path(run_dir, round_id))
    if not isinstance(candidate_set, dict):
        raise ValueError(
            "Matching candidate set is missing. Run normalize prepare-matching-adjudication after authorization before building the moderator adjudication packet."
        )
    if authorization_id and maybe_text(candidate_set.get("authorization_id")) != authorization_id:
        raise ValueError("Matching candidate set authorization_id does not match matching_authorization.json. Regenerate it.")
    draft_adjudication = load_dict_if_exists(matching_adjudication_draft_path(run_dir, round_id))
    if not isinstance(draft_adjudication, dict):
        raise ValueError(
            "Matching adjudication draft is missing. Run normalize prepare-matching-adjudication after authorization before building the moderator adjudication packet."
        )
    validate_payload("matching-adjudication", draft_adjudication)
    if authorization_id and maybe_text(draft_adjudication.get("authorization_id")) != authorization_id:
        raise ValueError("Matching adjudication draft authorization_id does not match matching_authorization.json. Regenerate it.")
    mission = state["mission"]
    context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="moderator")
    packet = build_matching_adjudication_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        context=context,
        state=state,
        candidate_set=candidate_set,
        draft_adjudication=draft_adjudication,
    )
    packet_path = matching_adjudication_packet_path(run_dir, round_id)
    write_json(packet_path, packet, pretty=pretty)
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "matching_candidate_set_path": str(matching_candidate_set_path(run_dir, round_id)),
        "matching_adjudication_packet_path": str(packet_path),
        "matching_adjudication_draft_path": str(matching_adjudication_draft_path(run_dir, round_id)),
    }


def investigation_review_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    if not matching_executed_for_state(state):
        raise ValueError(
            "Investigation-review packets require completed matching/adjudication artifacts. "
            "Run matching materialization first."
        )
    mission = state["mission"]
    context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="moderator")
    draft_review = build_investigation_review_draft_from_state(state)
    packet = build_investigation_review_packet(
        run_dir=run_dir,
        round_id=round_id,
        mission=mission,
        context=context,
        state=state,
        draft_review=draft_review,
    )
    packet_path = investigation_review_packet_path(run_dir, round_id)
    draft_path = investigation_review_draft_path(run_dir, round_id)
    write_json(packet_path, packet, pretty=pretty)
    write_json(draft_path, draft_review, pretty=pretty)
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "investigation_review_packet_path": str(packet_path),
        "investigation_review_draft_path": str(draft_path),
    }


def report_artifacts(*, run_dir: Path, round_id: str, pretty: bool) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    if not matching_executed_for_state(state):
        raise ValueError(
            "Expert report packets require completed matching/adjudication artifacts. "
            "Use build-data-readiness-packets or build-decision-packet before matching, and build-report-packets only after run-matching-adjudication."
        )
    if not isinstance(state.get("investigation_review"), dict) or not state.get("investigation_review"):
        raise ValueError(
            "Expert report packets require canonical moderator investigation_review.json. "
            "Normally run-matching-adjudication auto-materializes this review; otherwise build/promote it before generating expert-report packets."
        )
    mission = state["mission"]
    tasks = state.get("tasks", []) if isinstance(state.get("tasks"), list) else []
    max_findings = mission_constraints(mission).get("max_claims_per_round", 4)
    outputs: dict[str, dict[str, str]] = {}
    for role in REPORT_ROLES:
        context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role=role)
        draft_report = build_expert_report_draft_from_state(state=state, role=role, max_findings=max_findings)
        packet = build_report_packet(
            run_dir=run_dir,
            round_id=round_id,
            role=role,
            mission=mission,
            tasks=tasks,
            context=context,
            draft_report=draft_report,
        )
        packet_path = report_packet_path(run_dir, round_id, role)
        draft_path = report_draft_path(run_dir, round_id, role)
        write_json(packet_path, packet, pretty=pretty)
        write_json(draft_path, draft_report, pretty=pretty)
        outputs[role] = {"report_packet_path": str(packet_path), "report_draft_path": str(draft_path)}
    return {
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "claim_count": len(state.get("claims", [])),
        "observation_count": len(state.get("observations", [])),
        "evidence_count": len(state.get("cards_active", [])),
        "outputs": outputs,
    }


def decision_artifacts(
    *,
    run_dir: Path,
    round_id: str,
    next_round_id: str,
    pretty: bool,
    prefer_draft_reports: bool,
) -> dict[str, Any]:
    state = collect_round_state(run_dir, round_id)
    reports: dict[str, dict[str, Any] | None] = {}
    report_sources: dict[str, str] = {}
    for role in REPORT_ROLES:
        report, source = load_report_for_decision(run_dir, round_id, role, prefer_drafts=prefer_draft_reports)
        reports[role] = report
        report_sources[role] = source
    moderator_context = load_context_or_fallback_from_state(run_dir=run_dir, state=state, role="moderator")
    draft_decision, next_round_tasks, missing_types = build_decision_draft_from_state(
        run_dir=run_dir,
        state=state,
        next_round_id=next_round_id,
        reports=reports,
        report_sources=report_sources,
    )
    packet = build_decision_packet_from_state(
        run_dir=run_dir,
        state=state,
        next_round_id=next_round_id,
        moderator_context=moderator_context,
        reports=reports,
        report_sources=report_sources,
        draft_decision=draft_decision,
        proposed_next_round_tasks=next_round_tasks,
        missing_evidence_types=missing_types,
    )
    packet_path = decision_packet_path(run_dir, round_id)
    draft_path = decision_draft_path(run_dir, round_id)
    write_json(packet_path, packet, pretty=pretty)
    write_json(draft_path, draft_decision, pretty=pretty)
    return {
        "run_id": mission_run_id(state["mission"]),
        "round_id": round_id,
        "next_round_id": next_round_id,
        "decision_packet_path": str(packet_path),
        "decision_draft_path": str(draft_path),
        "report_sources": report_sources,
        "missing_evidence_types": missing_types,
        "next_round_task_count": len(next_round_tasks),
    }
