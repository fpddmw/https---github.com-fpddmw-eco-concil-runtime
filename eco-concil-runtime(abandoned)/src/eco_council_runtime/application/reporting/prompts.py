"""Prompt rendering for reporting artifact packets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists
from eco_council_runtime.application.reporting.artifact_support import READINESS_ROLES, REPORT_ROLES, write_text
from eco_council_runtime.controller.paths import (
    claim_curation_packet_path,
    claim_curation_prompt_path,
    data_readiness_packet_path,
    data_readiness_prompt_path,
    decision_packet_path,
    decision_prompt_path,
    investigation_review_packet_path,
    investigation_review_prompt_path,
    matching_adjudication_packet_path,
    matching_adjudication_prompt_path,
    matching_authorization_packet_path,
    matching_authorization_prompt_path,
    observation_curation_packet_path,
    observation_curation_prompt_path,
    report_packet_path,
    report_prompt_path,
)
from eco_council_runtime.domain.text import maybe_text
from eco_council_runtime.drafts import decision_prompt_text, report_prompt_text


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
        "2a. Read `investigation_state` first as the persisted causal-status summary, then use raw artifacts only to resolve gaps or ambiguities.",
        "2b. Read `investigation_actions` as the bounded ranked follow-up queue before inventing new recommended_next_actions.",
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


__all__ = [
    "claim_curation_prompt_text",
    "data_readiness_prompt_text",
    "investigation_review_prompt_text",
    "matching_adjudication_prompt_text",
    "matching_authorization_prompt_text",
    "observation_curation_prompt_text",
    "render_openclaw_prompts",
]
