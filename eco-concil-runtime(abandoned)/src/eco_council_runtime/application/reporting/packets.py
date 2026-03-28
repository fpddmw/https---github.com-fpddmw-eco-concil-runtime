"""Packet and draft builders for reporting artifact workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists, utc_now_iso
from eco_council_runtime.application.reporting.artifact_support import READINESS_ROLES, REPORT_ROLES
from eco_council_runtime.application.reporting_state import (
    load_dict_if_exists,
    load_override_requests,
    mission_constraints,
    mission_policy_profile,
    mission_run_id,
)
from eco_council_runtime.application.reporting_views import (
    build_claim_candidate_pool_summary,
    build_observation_candidate_pool_summary,
    compact_claim_candidate_for_curation,
    compact_evidence_card,
    compact_isolated_entry,
    compact_observation_candidate_for_curation,
    compact_remand_entry,
)
from eco_council_runtime.cli_invocation import runtime_module_command
from eco_council_runtime.controller.paths import (
    claim_curation_draft_path,
    claim_curation_path,
    data_readiness_draft_path,
    data_readiness_report_path,
    decision_draft_path,
    decision_target_path,
    investigation_actions_path,
    investigation_plan_path,
    investigation_review_draft_path,
    investigation_review_path,
    matching_adjudication_draft_path,
    matching_adjudication_path,
    matching_authorization_draft_path,
    matching_authorization_path,
    observation_curation_draft_path,
    observation_curation_path,
    report_draft_path,
    report_target_path,
)
from eco_council_runtime.domain.contract_bridge import (
    contract_call,
    effective_matching_authorization,
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings
from eco_council_runtime.planning import combine_recommendations

SCHEMA_VERSION = resolve_schema_version("1.0.0")


def build_report_instructions(role: str) -> list[str]:
    instructions = [
        "Return one JSON object only, shaped like expert-report.",
        "Treat `context` as a compact summary layer first; only rely on `canonical_paths` when the summary is insufficient.",
        "Start with `context.causal_focus` as the role-prioritized causal summary before re-reading the full `investigation_plan`.",
        "Use `investigation_actions` as the bounded persisted follow-up queue before inventing new next-step ideas from scratch.",
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
        "investigation_actions": load_dict_if_exists(investigation_actions_path(run_dir, round_id)),
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


def build_investigation_review_instructions() -> list[str]:
    return [
        "Return one JSON object only, shaped like investigation-review.",
        "Treat the packet draft as a rule-prepared starting point, not as a binding final answer.",
        "Start with context.causal_focus as the compact moderator view of the causal chain before re-reading the full investigation_plan.",
        "Use investigation_state as the persisted causal-status snapshot before falling back to raw canonical artifacts.",
        "Use investigation_actions as the persisted ranked next-action queue before drafting new follow-up recommendations.",
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
        "investigation_state": state.get("investigation_state", {}),
        "investigation_actions": state.get("investigation_actions", {}),
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
        "investigation_state": state.get("investigation_state", {}),
        "investigation_actions": state.get("investigation_actions", {}),
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
            "Use `investigation_state` as the persisted causal-status summary before manually re-inferring leg coverage from lower-level artifacts.",
            "Use `investigation_actions` as the persisted ranked action queue before recombining ad hoc next-step recommendations.",
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


__all__ = [
    "READINESS_ROLES",
    "REPORT_ROLES",
    "build_claim_curation_draft",
    "build_claim_curation_instructions",
    "build_claim_curation_packet",
    "build_data_readiness_instructions",
    "build_data_readiness_packet",
    "build_decision_packet_from_state",
    "build_investigation_review_instructions",
    "build_investigation_review_packet",
    "build_matching_adjudication_instructions",
    "build_matching_adjudication_packet",
    "build_matching_authorization_draft",
    "build_matching_authorization_instructions",
    "build_matching_authorization_packet",
    "build_observation_curation_draft",
    "build_observation_curation_instructions",
    "build_observation_curation_packet",
    "build_report_instructions",
    "build_report_packet",
]
