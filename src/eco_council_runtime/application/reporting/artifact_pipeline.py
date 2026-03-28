"""Artifact-building pipelines for reporting workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists, write_json
from eco_council_runtime.application.investigation.review import build_investigation_review_draft_from_state
from eco_council_runtime.application.reporting.artifact_support import READINESS_ROLES, REPORT_ROLES, load_report_for_decision
from eco_council_runtime.application.reporting.council_decision import build_decision_draft_from_state
from eco_council_runtime.application.reporting.expert_reports import build_expert_report_draft_from_state
from eco_council_runtime.application.reporting.packets import (
    build_claim_curation_draft,
    build_claim_curation_packet,
    build_data_readiness_packet,
    build_decision_packet_from_state,
    build_investigation_review_packet,
    build_matching_adjudication_packet,
    build_matching_authorization_draft,
    build_matching_authorization_packet,
    build_observation_curation_draft,
    build_observation_curation_packet,
    build_report_packet,
)
from eco_council_runtime.application.reporting.readiness import build_data_readiness_draft
from eco_council_runtime.application.reporting_state import (
    collect_round_state,
    load_dict_if_exists,
    matching_executed_for_state,
    mission_constraints,
    mission_run_id,
    state_auditable_submissions,
    state_current_submissions,
)
from eco_council_runtime.application.reporting_views import load_context_or_fallback_from_state
from eco_council_runtime.controller.paths import (
    claim_curation_draft_path,
    claim_curation_packet_path,
    claim_curation_path,
    claim_submissions_path,
    data_readiness_draft_path,
    data_readiness_packet_path,
    decision_draft_path,
    decision_packet_path,
    investigation_review_draft_path,
    investigation_review_packet_path,
    investigation_review_path,
    matching_adjudication_draft_path,
    matching_adjudication_packet_path,
    matching_authorization_draft_path,
    matching_authorization_packet_path,
    matching_candidate_set_path,
    observation_curation_draft_path,
    observation_curation_packet_path,
    observation_curation_path,
    observation_submissions_path,
    report_draft_path,
    report_packet_path,
)
from eco_council_runtime.domain.contract_bridge import validate_payload_or_raise as validate_payload
from eco_council_runtime.domain.text import maybe_text


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


__all__ = [
    "curation_artifacts",
    "curation_status_complete",
    "curations_materialized_for_round",
    "data_readiness_artifacts",
    "decision_artifacts",
    "investigation_review_artifacts",
    "matching_adjudication_artifacts",
    "matching_authorization_artifacts",
    "report_artifacts",
]
