"""Filesystem path helpers for the eco-council controller."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.controller.constants import ROUND_DIR_PATTERN, ROUND_ID_INPUT_PATTERN
from eco_council_runtime.controller.io import load_json_if_exists, maybe_text


def require_round_id(value: str) -> str:
    text = maybe_text(value)
    match = ROUND_ID_INPUT_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError(f"Invalid round id: {value!r}. Expected round-001 or round_001 style.")
    return f"round-{match.group(1)}"


def round_dir_name(round_id: str) -> str:
    text = maybe_text(round_id)
    match = ROUND_ID_INPUT_PATTERN.fullmatch(text)
    if match is not None:
        return f"round_{match.group(1)}"
    # Preserve legacy callers that pass through already-derived round-like labels.
    return text.replace("-", "_")


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_dir_name(round_id)


def discover_round_ids(run_dir: Path) -> list[str]:
    round_ids: list[str] = []
    if not run_dir.exists():
        return round_ids
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        match = ROUND_DIR_PATTERN.fullmatch(child.name)
        if match is None:
            continue
        round_ids.append(f"round-{match.group(1)}")
    round_ids.sort()
    return round_ids


def latest_round_id(run_dir: Path) -> str:
    round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}")
    return round_ids[-1]


def next_round_id(round_id: str) -> str:
    normalized = require_round_id(round_id)
    number = int(normalized.split("-")[1])
    return f"round-{number + 1:03d}"


def tasks_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "tasks.json"


def mission_path(run_dir: Path) -> Path:
    return run_dir / "mission.json"


def load_mission(run_dir: Path) -> dict[str, Any]:
    mission_payload = load_json_if_exists(mission_path(run_dir))
    if not isinstance(mission_payload, dict):
        raise ValueError(f"Mission payload is not a JSON object: {mission_path(run_dir)}")
    return mission_payload


def current_run_id(run_dir: Path) -> str:
    return maybe_text(load_mission(run_dir).get("run_id"))


def prior_round_ids(run_dir: Path, round_id: str) -> list[str]:
    round_ids = discover_round_ids(run_dir)
    if round_id not in round_ids:
        return round_ids
    current_index = round_ids.index(round_id)
    return round_ids[:current_index]


def task_review_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_task_review_prompt.txt"


def fetch_plan_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch_plan.json"


def fetch_execution_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch_execution.json"


def data_plane_execution_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "data_plane_execution.json"


def matching_execution_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_adjudication_execution.json"


def fetch_lock_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch.lock"


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "source_selection.json"


def source_selection_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_source_selection_prompt.txt"


def source_selection_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "source_selection_packet.json"


def role_normalized_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "normalized"


def default_context_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived"


def role_context_path(run_dir: Path, round_id: str, role: str) -> Path:
    return default_context_dir(run_dir, round_id, role) / f"context_{role}.json"


def claim_candidates_path(run_dir: Path, round_id: str) -> Path:
    return role_normalized_dir(run_dir, round_id, "sociologist") / "claim_candidates.json"


def observation_candidates_path(run_dir: Path, round_id: str) -> Path:
    return role_normalized_dir(run_dir, round_id, "environmentalist") / "observation_candidates.json"


def investigation_plan_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "investigation_plan.json"


def investigation_state_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "investigation_state.json"


def investigation_actions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "investigation_actions.json"


def claim_curation_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "claim_curation.json"


def observation_curation_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "observation_curation.json"


def claim_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "claim_submissions.json"


def observation_submissions_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "observation_submissions.json"


def claim_curation_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "derived" / "claim_curation_packet.json"


def observation_curation_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "derived" / "observation_curation_packet.json"


def claim_curation_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "derived" / "claim_curation_draft.json"


def observation_curation_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "derived" / "observation_curation_draft.json"


def claim_curation_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "derived" / "openclaw_claim_curation_prompt.txt"


def observation_curation_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "derived" / "openclaw_observation_curation_prompt.txt"


def override_requests_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "override_requests.json"


def report_draft_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"{role}_report_draft.json"


def data_readiness_report_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "data_readiness_report.json"


def data_readiness_draft_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"{role}_data_readiness_draft.json"


def data_readiness_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "data_readiness_packet.json"


def data_readiness_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_data_readiness_prompt.txt"


def report_target_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / f"{role}_report.json"


def report_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_report_prompt.txt"


def report_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "report_packet.json"


def decision_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "council_decision_draft.json"


def moderator_derived_dir(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived"


def matching_authorization_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "matching_authorization.json"


def matching_authorization_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_authorization_draft.json"


def matching_authorization_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_authorization_packet.json"


def matching_authorization_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_matching_authorization_prompt.txt"


def matching_candidate_set_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_candidate_set.json"


def matching_adjudication_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "matching_adjudication.json"


def matching_adjudication_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_adjudication_draft.json"


def matching_adjudication_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "matching_adjudication_packet.json"


def matching_adjudication_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_matching_adjudication_prompt.txt"


def matching_result_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "matching_result.json"


def evidence_adjudication_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence_adjudication.json"


def investigation_review_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "investigation_review.json"


def investigation_review_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "investigation_review_draft.json"


def investigation_review_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "investigation_review_packet.json"


def investigation_review_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_investigation_review_prompt.txt"


def decision_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_decision_prompt.txt"


def decision_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "decision_packet.json"


def decision_target_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "council_decision.json"


def reporting_handoff_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_reporting_handoff.json"


def shared_claims_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "claims.json"


def shared_observations_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "observations.json"


def shared_evidence_cards_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence_cards.json"


def shared_evidence_path(run_dir: Path, round_id: str) -> Path:
    return shared_evidence_cards_path(run_dir, round_id)


def evidence_library_dir(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "evidence-library"


def evidence_library_ledger_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "ledger.jsonl"


def audit_chain_dir(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "audit-chain"


def audit_chain_ledger_path(run_dir: Path, round_id: str) -> Path:
    return audit_chain_dir(run_dir, round_id) / "receipts.jsonl"


def audit_chain_objects_dir(run_dir: Path, round_id: str) -> Path:
    return audit_chain_dir(run_dir, round_id) / "objects"


def library_context_path(run_dir: Path, round_id: str, role: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / f"context_{role}.json"


def claims_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "claims_active.json"


def observations_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "observations_active.json"


def cards_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "cards_active.json"


def isolated_active_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "isolated_active.json"


def remands_open_path(run_dir: Path, round_id: str) -> Path:
    return evidence_library_dir(run_dir, round_id) / "remands_open.json"


def public_signals_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "sociologist" / "normalized" / "public_signals.jsonl"


def environment_signals_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "environmentalist" / "normalized" / "environment_signals.jsonl"


def supervisor_dir(run_dir: Path) -> Path:
    return run_dir / "supervisor"


def supervisor_state_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "state.json"


def supervisor_state_lock_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "state.lock"


def supervisor_sessions_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "sessions"


def supervisor_outbox_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "outbox"


def supervisor_responses_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "responses"


def supervisor_current_step_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "CURRENT_STEP.txt"


def reports_dir(run_dir: Path) -> Path:
    return run_dir / "reports"


def supervisor_context_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "context"


def history_context_path(run_dir: Path, round_id: str) -> Path:
    return supervisor_context_dir(run_dir) / f"{round_id}_historical_cases.txt"


def history_retrieval_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "shared" / "history_retrieval.json"


def response_base_path(run_dir: Path, round_id: str, role: str, kind: str) -> Path:
    safe_kind = kind.replace("-", "_")
    return supervisor_responses_dir(run_dir) / f"{round_id}_{role}_{safe_kind}"


def openclaw_runtime_root(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "openclaw-runtime"


def session_prompt_path(run_dir: Path, role: str) -> Path:
    return supervisor_sessions_dir(run_dir) / f"{role}_session_prompt.txt"


def outbox_message_path(run_dir: Path, name: str) -> Path:
    return supervisor_outbox_dir(run_dir) / f"{name}.txt"
