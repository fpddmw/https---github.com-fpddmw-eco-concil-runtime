"""State assembly and phase/context helpers for reporting workflows."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.adapters.filesystem import (
    load_canonical_list,
    load_json_if_exists,
    stable_hash,
    stable_json,
)
from eco_council_runtime.adapters.run_paths import discover_round_ids, load_mission
from eco_council_runtime.controller.paths import (
    cards_active_path,
    claim_candidates_path,
    claim_curation_path,
    claim_submissions_path,
    claims_active_path,
    data_readiness_report_path,
    evidence_adjudication_path,
    investigation_plan_path,
    investigation_review_path,
    isolated_active_path,
    matching_adjudication_path,
    matching_authorization_path,
    matching_result_path,
    observation_candidates_path,
    observation_curation_path,
    observation_submissions_path,
    observations_active_path,
    override_requests_path,
    remands_open_path,
    shared_claims_path,
    shared_evidence_path,
    shared_observations_path,
    tasks_path,
)
from eco_council_runtime.domain.contract_bridge import contract_call, effective_matching_authorization
from eco_council_runtime.domain.rounds import parse_round_components
from eco_council_runtime.domain.text import maybe_text

PathBuilder = Callable[[Path, str], Path]
ObservationHydrator = Callable[[list[dict[str, Any]], list[dict[str, Any]]], list[dict[str, Any]]]


def mission_run_id(mission: dict[str, Any]) -> str:
    run_id = mission.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("mission.json missing run_id.")
    return run_id


def mission_constraints(mission: dict[str, Any]) -> dict[str, int]:
    values = contract_call("effective_constraints", mission)
    if isinstance(values, dict):
        return {key: int(value) for key, value in values.items() if isinstance(value, int) and value > 0}
    constraints = mission.get("constraints")
    if not isinstance(constraints, dict):
        return {}
    result: dict[str, int] = {}
    for key in ("max_rounds", "max_claims_per_round", "max_tasks_per_round", "claim_target_per_round", "claim_hard_cap_per_round"):
        value = constraints.get(key)
        if isinstance(value, int) and value > 0:
            result[key] = value
    return result


def mission_policy_profile(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("policy_profile_summary", mission)
    if isinstance(value, dict):
        return value
    return {}


def round_ids_through(run_dir: Path, round_id: str) -> list[str]:
    current = parse_round_components(round_id)
    if current is None:
        return [item for item in discover_round_ids(run_dir) if item <= round_id]
    prefix, number, _width = current
    selected: list[str] = []
    for item in discover_round_ids(run_dir):
        components = parse_round_components(item)
        if components is None:
            continue
        item_prefix, item_number, _item_width = components
        if item_prefix == prefix and item_number <= number:
            selected.append(item)
    return selected


def observation_signature_payload(observation: dict[str, Any]) -> dict[str, Any]:
    provenance = observation.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    return {
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "observation_mode": maybe_text(observation.get("observation_mode")),
        "evidence_role": maybe_text(observation.get("evidence_role")),
        "hypothesis_id": maybe_text(observation.get("hypothesis_id")),
        "leg_id": maybe_text(observation.get("leg_id")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": observation.get("statistics"),
        "distribution_summary": observation.get("distribution_summary"),
        "time_window": observation.get("time_window"),
        "place_scope": observation.get("place_scope"),
        "source_skills": sorted(maybe_text(item) for item in observation.get("source_skills", []) if maybe_text(item)),
        "metric_bundle": sorted(maybe_text(item) for item in observation.get("metric_bundle", []) if maybe_text(item)),
        "candidate_observation_ids": sorted(
            maybe_text(item) for item in observation.get("candidate_observation_ids", []) if maybe_text(item)
        ),
        "provenance_refs": sorted(
            stable_hash(stable_json(item))
            for item in observation.get("provenance_refs", [])
            if isinstance(item, dict)
        ),
        "component_roles": observation.get("component_roles"),
        "quality_flags": sorted(
            maybe_text(item) for item in observation.get("quality_flags", []) if maybe_text(item)
        ),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }


def shared_observation_id(observation: dict[str, Any]) -> str:
    signature = stable_hash(stable_json(observation_signature_payload(observation)))
    return f"obs-{signature[:12]}"


def materialize_shared_observation(observation: dict[str, Any]) -> dict[str, Any]:
    item = dict(observation)
    item["observation_id"] = shared_observation_id(observation)
    return item


def observation_submission_id(observation_id: str) -> str:
    return f"obssub-{maybe_text(observation_id)}"


def effective_shared_observations(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    merged_by_signature: dict[str, dict[str, Any]] = {}
    ordered_signatures: list[str] = []
    for observed_round_id in round_ids_through(run_dir, round_id):
        for observation in load_canonical_list(shared_observations_path(run_dir, observed_round_id)):
            signature_payload = observation_signature_payload(observation)
            signature = stable_hash(stable_json(signature_payload))
            if signature not in merged_by_signature:
                ordered_signatures.append(signature)
            merged_by_signature[signature] = materialize_shared_observation(observation)
    return [merged_by_signature[signature] for signature in ordered_signatures]


def effective_shared_claims(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    merged_by_id: dict[str, dict[str, Any]] = {}
    ordered_ids: list[str] = []
    for observed_round_id in round_ids_through(run_dir, round_id):
        for claim in load_canonical_list(shared_claims_path(run_dir, observed_round_id)):
            claim_id = maybe_text(claim.get("claim_id"))
            if not claim_id:
                continue
            if claim_id not in merged_by_id:
                ordered_ids.append(claim_id)
            merged_by_id[claim_id] = dict(claim)
    return [merged_by_id[claim_id] for claim_id in ordered_ids]


def active_library_list(run_dir: Path, round_id: str, path_fn: PathBuilder) -> list[dict[str, Any]]:
    current_path = path_fn(run_dir, round_id)
    if current_path.exists():
        current = load_canonical_list(current_path)
        if current:
            return current
    prior_rounds = round_ids_through(run_dir, round_id)
    if prior_rounds and prior_rounds[-1] == round_id:
        prior_rounds = prior_rounds[:-1]
    for observed_round_id in reversed(prior_rounds):
        current = load_canonical_list(path_fn(run_dir, observed_round_id))
        if current:
            return current
    return []


def load_dict_if_exists(path: Path) -> dict[str, Any]:
    payload = load_json_if_exists(path)
    if isinstance(payload, dict):
        return payload
    return {}


def load_override_requests(run_dir: Path, round_id: str, role: str | None = None) -> list[dict[str, Any]]:
    roles = (role,) if role else ("moderator", "sociologist", "environmentalist", "historian")
    output: list[dict[str, Any]] = []
    for role_name in roles:
        payload = load_json_if_exists(override_requests_path(run_dir, round_id, role_name))
        if not isinstance(payload, list):
            continue
        output.extend(item for item in payload if isinstance(item, dict))
    output.sort(key=lambda item: maybe_text(item.get("request_id")))
    return output


def matching_executed_for_state(state: dict[str, Any]) -> bool:
    return bool(
        state.get("matching_adjudication")
        or state.get("matching_result")
        or state.get("evidence_adjudication")
        or state.get("cards_active")
        or state.get("isolated_active")
        or state.get("remands_open")
    )


def compact_recommendation(action: Any) -> dict[str, Any] | None:
    if not isinstance(action, dict):
        return None
    assigned_role = maybe_text(action.get("assigned_role"))
    objective = maybe_text(action.get("objective"))
    reason = maybe_text(action.get("reason"))
    if not assigned_role or not objective or not reason:
        return None
    return {
        "assigned_role": assigned_role,
        "objective": objective,
        "reason": reason,
    }


def compact_matching_authorization_summary(authorization: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(authorization, dict):
        return {}
    return {
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "authorization_status": maybe_text(authorization.get("authorization_status")),
        "authorization_basis": maybe_text(authorization.get("authorization_basis")),
        "summary": maybe_text(authorization.get("summary")),
        "rationale": maybe_text(authorization.get("rationale")),
        "allow_isolated_evidence": bool(authorization.get("allow_isolated_evidence")),
        "claim_count": len(authorization.get("claim_ids", [])) if isinstance(authorization.get("claim_ids"), list) else 0,
        "observation_count": len(authorization.get("observation_ids", [])) if isinstance(authorization.get("observation_ids"), list) else 0,
        "open_questions": [maybe_text(item) for item in authorization.get("open_questions", []) if maybe_text(item)][:6],
    }


def compact_matching_result_summary(result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    matched_pairs = result.get("matched_pairs", [])
    return {
        "result_id": maybe_text(result.get("result_id")),
        "result_status": maybe_text(result.get("result_status")),
        "summary": maybe_text(result.get("summary")),
        "matched_pair_count": len(matched_pairs) if isinstance(matched_pairs, list) else 0,
        "matched_claim_count": len(result.get("matched_claim_ids", [])) if isinstance(result.get("matched_claim_ids"), list) else 0,
        "matched_observation_count": (
            len(result.get("matched_observation_ids", []))
            if isinstance(result.get("matched_observation_ids"), list)
            else 0
        ),
        "unmatched_claim_ids": [maybe_text(item) for item in result.get("unmatched_claim_ids", []) if maybe_text(item)][:8],
        "unmatched_observation_ids": [
            maybe_text(item)
            for item in result.get("unmatched_observation_ids", [])
            if maybe_text(item)
        ][:8],
    }


def compact_matching_adjudication_summary(adjudication: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(adjudication, dict):
        return {}
    recommendations = [
        compact_recommendation(item)
        for item in adjudication.get("recommended_next_actions", [])
        if isinstance(adjudication.get("recommended_next_actions"), list)
    ]
    return {
        "adjudication_id": maybe_text(adjudication.get("adjudication_id")),
        "candidate_set_id": maybe_text(adjudication.get("candidate_set_id")),
        "summary": maybe_text(adjudication.get("summary")),
        "rationale": maybe_text(adjudication.get("rationale")),
        "open_questions": [maybe_text(item) for item in adjudication.get("open_questions", []) if maybe_text(item)][:6],
        "recommended_next_actions": [item for item in recommendations if item][:4],
    }


def compact_evidence_adjudication_summary(adjudication: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(adjudication, dict):
        return {}
    recommendations = [
        compact_recommendation(item)
        for item in adjudication.get("recommended_next_actions", [])
        if isinstance(adjudication.get("recommended_next_actions"), list)
    ]
    return {
        "adjudication_id": maybe_text(adjudication.get("adjudication_id")),
        "adjudication_status": maybe_text(adjudication.get("adjudication_status")),
        "summary": maybe_text(adjudication.get("summary")),
        "matching_reasonable": bool(adjudication.get("matching_reasonable")),
        "needs_additional_data": bool(adjudication.get("needs_additional_data")),
        "open_questions": [maybe_text(item) for item in adjudication.get("open_questions", []) if maybe_text(item)][:6],
        "recommended_next_actions": [item for item in recommendations if item][:4],
    }


def compact_investigation_review_summary(review: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(review, dict):
        return {}
    recommendations = [
        compact_recommendation(item)
        for item in review.get("recommended_next_actions", [])
        if isinstance(review.get("recommended_next_actions"), list)
    ]
    return {
        "review_id": maybe_text(review.get("review_id")),
        "review_status": maybe_text(review.get("review_status")),
        "summary": maybe_text(review.get("summary")),
        "matching_reasonable": bool(review.get("matching_reasonable")),
        "needs_additional_data": bool(review.get("needs_additional_data")),
        "hypothesis_count": len(review.get("hypothesis_reviews", []))
        if isinstance(review.get("hypothesis_reviews"), list)
        else 0,
        "open_questions": [maybe_text(item) for item in review.get("open_questions", []) if maybe_text(item)][:6],
        "recommended_next_actions": [item for item in recommendations if item][:4],
    }


def augment_context_with_matching_state(*, run_dir: Path, state: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(context)
    round_id = state["round_id"]
    canonical_paths = merged.get("canonical_paths")
    if not isinstance(canonical_paths, dict):
        canonical_paths = {}
    canonical_paths.update(
        {
            "matching_authorization": str(matching_authorization_path(run_dir, round_id)),
            "matching_adjudication": str(matching_adjudication_path(run_dir, round_id)),
            "matching_result": str(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": str(evidence_adjudication_path(run_dir, round_id)),
            "investigation_review": str(investigation_review_path(run_dir, round_id)),
        }
    )
    merged["canonical_paths"] = canonical_paths
    phase_state = merged.get("phase_state")
    if not isinstance(phase_state, dict):
        phase_state = {}
    phase_state.update(phase_state_from_round_state(state))
    merged["phase_state"] = phase_state
    merged["matching"] = {
        "authorization": compact_matching_authorization_summary(
            state.get("matching_authorization", {}) if isinstance(state.get("matching_authorization"), dict) else {}
        ),
        "result": compact_matching_result_summary(
            state.get("matching_result", {}) if isinstance(state.get("matching_result"), dict) else {}
        ),
        "adjudication": compact_matching_adjudication_summary(
            state.get("matching_adjudication", {}) if isinstance(state.get("matching_adjudication"), dict) else {}
        ),
        "evidence_adjudication": compact_evidence_adjudication_summary(
            state.get("evidence_adjudication", {}) if isinstance(state.get("evidence_adjudication"), dict) else {}
        ),
        "investigation_review": compact_investigation_review_summary(
            state.get("investigation_review", {}) if isinstance(state.get("investigation_review"), dict) else {}
        ),
    }
    return merged


def phase_state_from_round_state(state: dict[str, Any]) -> dict[str, Any]:
    readiness_reports = state.get("readiness_reports", {}) if isinstance(state.get("readiness_reports"), dict) else {}
    readiness_statuses = {
        role: maybe_text(report.get("readiness_status"))
        for role, report in readiness_reports.items()
        if isinstance(report, dict) and report
    }
    claim_curation = state.get("claim_curation") if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = state.get("observation_curation") if isinstance(state.get("observation_curation"), dict) else {}
    authorization = state.get("matching_authorization", {}) if isinstance(state.get("matching_authorization"), dict) else {}
    moderator_adjudication = state.get("matching_adjudication", {}) if isinstance(state.get("matching_adjudication"), dict) else {}
    result = state.get("matching_result", {}) if isinstance(state.get("matching_result"), dict) else {}
    adjudication = state.get("evidence_adjudication", {}) if isinstance(state.get("evidence_adjudication"), dict) else {}
    investigation_review = state.get("investigation_review", {}) if isinstance(state.get("investigation_review"), dict) else {}
    return {
        "claim_curation_status": maybe_text(claim_curation.get("status")),
        "observation_curation_status": maybe_text(observation_curation.get("status")),
        "readiness_statuses": readiness_statuses,
        "readiness_received_roles": sorted(readiness_statuses),
        "matching_authorization_status": maybe_text(authorization.get("authorization_status")),
        "matching_authorization_basis": maybe_text(authorization.get("authorization_basis")),
        "matching_adjudication_id": maybe_text(moderator_adjudication.get("adjudication_id")),
        "matching_candidate_set_id": maybe_text(moderator_adjudication.get("candidate_set_id")),
        "matching_result_status": maybe_text(result.get("result_status")),
        "adjudication_status": maybe_text(adjudication.get("adjudication_status")),
        "investigation_review_id": maybe_text(investigation_review.get("review_id")),
        "investigation_review_status": maybe_text(investigation_review.get("review_status")),
        "matching_executed": matching_executed_for_state(state),
    }


def observation_match_key(item: dict[str, Any], *, include_investigation_tags: bool = True) -> str:
    provenance = item.get("provenance") if isinstance(item.get("provenance"), dict) else {}
    payload = {
        "source_skill": maybe_text(item.get("source_skill")),
        "metric": maybe_text(item.get("metric")),
        "aggregation": maybe_text(item.get("aggregation")),
        "observation_mode": maybe_text(item.get("observation_mode")),
        "evidence_role": maybe_text(item.get("evidence_role")),
        "value": item.get("value"),
        "unit": maybe_text(item.get("unit")),
        "time_window": item.get("time_window"),
        "place_scope": item.get("place_scope"),
        "source_skills": sorted(maybe_text(value) for value in item.get("source_skills", []) if maybe_text(value)),
        "metric_bundle": sorted(maybe_text(value) for value in item.get("metric_bundle", []) if maybe_text(value)),
        "candidate_observation_ids": sorted(
            maybe_text(value) for value in item.get("candidate_observation_ids", []) if maybe_text(value)
        ),
        "quality_flags": sorted(maybe_text(flag) for flag in item.get("quality_flags", []) if maybe_text(flag)),
        "provenance": {
            "source_skill": maybe_text(provenance.get("source_skill")),
            "record_locator": maybe_text(provenance.get("record_locator")),
            "external_id": maybe_text(provenance.get("external_id")),
            "sha256": maybe_text(provenance.get("sha256")),
        },
    }
    if include_investigation_tags:
        payload["hypothesis_id"] = maybe_text(item.get("hypothesis_id"))
        payload["leg_id"] = maybe_text(item.get("leg_id"))
        payload["component_roles"] = item.get("component_roles")
    return stable_hash(stable_json(payload))


def hydrate_observation_submissions_with_observations(
    submissions: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    observations_by_id = {
        maybe_text(item.get("observation_id")): item
        for item in observations
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    observations_by_key = {
        observation_match_key(item): item
        for item in observations
        if isinstance(item, dict)
    }
    observations_by_legacy_key = {
        observation_match_key(item, include_investigation_tags=False): item
        for item in observations
        if isinstance(item, dict)
    }
    hydrated: list[dict[str, Any]] = []
    for submission in submissions:
        if not isinstance(submission, dict):
            continue
        item = dict(submission)
        observation = observations_by_id.get(maybe_text(item.get("observation_id")))
        if observation is None:
            observation = observations_by_key.get(observation_match_key(item))
        if observation is None:
            observation = observations_by_legacy_key.get(
                observation_match_key(item, include_investigation_tags=False)
            )
        if observation is not None:
            canonical_observation_id = maybe_text(observation.get("observation_id"))
            if canonical_observation_id and maybe_text(item.get("observation_id")) != canonical_observation_id:
                item["observation_id"] = canonical_observation_id
                item["submission_id"] = observation_submission_id(canonical_observation_id)
            if not isinstance(item.get("statistics"), dict) and isinstance(observation.get("statistics"), dict):
                item["statistics"] = observation.get("statistics")
            if not isinstance(item.get("distribution_summary"), dict) and isinstance(observation.get("distribution_summary"), dict):
                item["distribution_summary"] = observation.get("distribution_summary")
            if not isinstance(item.get("time_window"), dict) and isinstance(observation.get("time_window"), dict):
                item["time_window"] = observation.get("time_window")
            if not maybe_text(item.get("unit")) and maybe_text(observation.get("unit")):
                item["unit"] = maybe_text(observation.get("unit"))
            if not maybe_text(item.get("hypothesis_id")) and maybe_text(observation.get("hypothesis_id")):
                item["hypothesis_id"] = maybe_text(observation.get("hypothesis_id"))
            if not maybe_text(item.get("leg_id")) and maybe_text(observation.get("leg_id")):
                item["leg_id"] = maybe_text(observation.get("leg_id"))
            if not isinstance(item.get("component_roles"), list) and isinstance(observation.get("component_roles"), list):
                item["component_roles"] = copy.deepcopy(observation.get("component_roles"))
        hydrated.append(item)
    return hydrated


def collect_round_state(
    run_dir: Path,
    round_id: str,
    *,
    hydrate_observations: ObservationHydrator = hydrate_observation_submissions_with_observations,
) -> dict[str, Any]:
    mission = load_mission(run_dir)
    observations = effective_shared_observations(run_dir, round_id)
    claims = effective_shared_claims(run_dir, round_id)
    claim_submissions_current = load_canonical_list(claim_submissions_path(run_dir, round_id))
    observation_submissions_current = hydrate_observations(
        load_canonical_list(observation_submissions_path(run_dir, round_id)),
        observations,
    )
    claims_active = active_library_list(run_dir, round_id, claims_active_path)
    observations_active = hydrate_observations(
        active_library_list(run_dir, round_id, observations_active_path),
        observations,
    )
    matching_authorization = effective_matching_authorization(
        mission=mission,
        round_id=round_id,
        authorization=load_dict_if_exists(matching_authorization_path(run_dir, round_id)),
    )
    state = {
        "mission": mission,
        "round_id": round_id,
        "tasks": load_canonical_list(tasks_path(run_dir, round_id)),
        "claims": claims,
        "observations": observations,
        "claim_candidates_current": load_canonical_list(claim_candidates_path(run_dir, round_id)),
        "observation_candidates_current": load_canonical_list(observation_candidates_path(run_dir, round_id)),
        "claim_curation": load_dict_if_exists(claim_curation_path(run_dir, round_id)),
        "observation_curation": load_dict_if_exists(observation_curation_path(run_dir, round_id)),
        "evidence_cards": load_canonical_list(shared_evidence_path(run_dir, round_id)),
        "claim_submissions_current": claim_submissions_current,
        "observation_submissions_current": observation_submissions_current,
        "claim_submissions_auditable": claims_active or claim_submissions_current,
        "observation_submissions_auditable": observations_active or observation_submissions_current,
        "claims_active": claims_active,
        "observations_active": observations_active,
        "cards_active": active_library_list(run_dir, round_id, cards_active_path),
        "isolated_active": active_library_list(run_dir, round_id, isolated_active_path),
        "remands_open": active_library_list(run_dir, round_id, remands_open_path),
        "investigation_plan": load_dict_if_exists(investigation_plan_path(run_dir, round_id)),
        "readiness_reports": {
            "sociologist": load_dict_if_exists(data_readiness_report_path(run_dir, round_id, "sociologist")),
            "environmentalist": load_dict_if_exists(data_readiness_report_path(run_dir, round_id, "environmentalist")),
        },
        "matching_authorization": matching_authorization,
        "matching_adjudication": load_dict_if_exists(matching_adjudication_path(run_dir, round_id)),
        "matching_result": load_dict_if_exists(matching_result_path(run_dir, round_id)),
        "evidence_adjudication": load_dict_if_exists(evidence_adjudication_path(run_dir, round_id)),
        "investigation_review": load_dict_if_exists(investigation_review_path(run_dir, round_id)),
    }
    state["phase_state"] = phase_state_from_round_state(state)
    return state


def state_current_submissions(state: dict[str, Any], role: str) -> list[dict[str, Any]]:
    if role == "sociologist":
        current = state.get("claim_submissions_current", [])
    else:
        current = state.get("observation_submissions_current", [])
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    return []


def state_auditable_submissions(state: dict[str, Any], role: str) -> list[dict[str, Any]]:
    if role == "sociologist":
        auditable = state.get("claim_submissions_auditable", [])
        current = state.get("claim_submissions_current", [])
    else:
        auditable = state.get("observation_submissions_auditable", [])
        current = state.get("observation_submissions_current", [])
    if isinstance(auditable, list) and auditable:
        return [item for item in auditable if isinstance(item, dict)]
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    return []


def state_submissions(state: dict[str, Any], role: str) -> list[dict[str, Any]]:
    submissions = state_auditable_submissions(state, role)
    if submissions:
        return submissions
    if role == "sociologist":
        active = state.get("claims_active", [])
    else:
        active = state.get("observations_active", [])
    if isinstance(active, list):
        return [item for item in active if isinstance(item, dict)]
    return []


__all__ = [
    "active_library_list",
    "augment_context_with_matching_state",
    "collect_round_state",
    "compact_evidence_adjudication_summary",
    "compact_investigation_review_summary",
    "compact_matching_adjudication_summary",
    "compact_matching_authorization_summary",
    "compact_matching_result_summary",
    "compact_recommendation",
    "effective_shared_claims",
    "effective_shared_observations",
    "hydrate_observation_submissions_with_observations",
    "load_dict_if_exists",
    "load_override_requests",
    "matching_executed_for_state",
    "materialize_shared_observation",
    "mission_constraints",
    "mission_policy_profile",
    "mission_run_id",
    "observation_match_key",
    "observation_signature_payload",
    "observation_submission_id",
    "phase_state_from_round_state",
    "round_ids_through",
    "shared_observation_id",
    "state_auditable_submissions",
    "state_current_submissions",
    "state_submissions",
]
