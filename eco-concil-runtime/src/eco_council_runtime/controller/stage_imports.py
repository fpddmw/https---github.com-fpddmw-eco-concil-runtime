"""Canonical import handlers for supervisor stage artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.application.investigation import materialize_investigation_bundle
from eco_council_runtime.controller.agent_ingest import (
    ensure_claim_curation_matches,
    ensure_data_readiness_matches,
    ensure_decision_matches,
    ensure_investigation_review_matches,
    ensure_matching_adjudication_matches,
    ensure_matching_authorization_matches,
    ensure_observation_curation_matches,
    ensure_report_matches,
    ensure_source_selection_matches,
    ensure_source_selection_respects_packet,
    ensure_task_review_matches,
    normalize_matching_authorization_payload,
    persist_override_requests_for_role,
)
from eco_council_runtime.controller.audit_chain import (
    record_fetch_phase_receipt,
    record_import_receipt,
    record_normalize_phase_receipt,
)
from eco_council_runtime.controller.constants import (
    CURATION_ROLES,
    READINESS_ROLES,
    REPORT_ROLES,
    SOURCE_SELECTION_ROLES,
    STAGE_AWAITING_DATA_READINESS,
    STAGE_AWAITING_DECISION,
    STAGE_AWAITING_EVIDENCE_CURATION,
    STAGE_AWAITING_INVESTIGATION_REVIEW,
    STAGE_AWAITING_MATCHING_ADJUDICATION,
    STAGE_AWAITING_MATCHING_AUTHORIZATION,
    STAGE_AWAITING_REPORTS,
    STAGE_AWAITING_SOURCE_SELECTION,
    STAGE_READY_DATA_PLANE,
    STAGE_READY_MATCHING_ADJUDICATION,
    STAGE_READY_PREPARE,
    STAGE_READY_PROMOTE,
)
from eco_council_runtime.controller.execution_artifacts import (
    ensure_data_plane_execution_matches,
    ensure_fetch_execution_matches,
)
from eco_council_runtime.controller.io import load_json_if_exists, maybe_text, write_json
from eco_council_runtime.controller.paths import (
    claim_submissions_path,
    claim_curation_path,
    claims_active_path,
    current_run_id,
    data_plane_execution_path,
    data_readiness_report_path,
    decision_draft_path,
    evidence_adjudication_path,
    fetch_execution_path,
    investigation_actions_path,
    investigation_review_path,
    investigation_state_path,
    load_mission,
    matching_adjudication_path,
    matching_authorization_path,
    matching_result_path,
    observation_submissions_path,
    observation_curation_path,
    observations_active_path,
    report_draft_path,
    shared_claims_path,
    shared_observations_path,
    source_selection_path,
    tasks_path,
)
from eco_council_runtime.controller.policy import effective_matching_authorization_payload

StateSaver = Callable[[Path, dict[str, Any]], None]
StatusBuilder = Callable[[Path, dict[str, Any]], dict[str, Any]]
RoundArtifactBuilder = Callable[[Path, str], None]
SourceSelectionPacketBuilder = Callable[[Path, str, str], Path]
SignalCorpusImporter = Callable[[Path, dict[str, Any], str], dict[str, Any] | None]


def _reset_imports_after_task_review() -> dict[str, Any]:
    return {
        "task_review_received": True,
        "source_selection_roles_received": [],
        "curation_roles_received": [],
        "data_readiness_roles_received": [],
        "matching_authorization_received": False,
        "matching_adjudication_received": False,
        "investigation_review_received": False,
        "report_roles_received": [],
        "decision_received": False,
    }


def _state_result(
    *,
    imported_kind: str,
    target_path: Path,
    source_path: Path,
    run_dir: Path,
    state: dict[str, Any],
    status_builder: StatusBuilder,
    role: str = "",
    derived_artifact_specs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    record_import_receipt(
        run_dir=run_dir,
        round_id=maybe_text(state.get("current_round_id")),
        imported_kind=imported_kind,
        source_path=source_path,
        target_path=target_path,
        role=role,
        stage_after_import=maybe_text(state.get("stage")),
        derived_artifact_specs=derived_artifact_specs,
    )
    result = {
        "imported_kind": imported_kind,
        "input_path": str(source_path),
        "target_path": str(target_path),
        "state": status_builder(run_dir, state),
    }
    if role:
        result["role"] = role
    return result


def import_task_review_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_task_review_matches(payload, round_id=round_id)
    target = tasks_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    state["stage"] = STAGE_AWAITING_SOURCE_SELECTION
    state["imports"] = _reset_imports_after_task_review()
    save_state(run_dir, state)
    return _state_result(
        imported_kind="round-task",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
    )


def import_source_selection_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    source_selection_packet_builder: SourceSelectionPacketBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_source_selection_matches(payload, round_id=round_id, role=role)
    ensure_source_selection_respects_packet(
        run_dir=run_dir,
        round_id=round_id,
        role=role,
        payload=payload,
        build_packet=source_selection_packet_builder,
    )
    target = source_selection_path(run_dir, round_id, role)
    write_json(target, payload, pretty=True)
    persist_override_requests_for_role(
        run_dir=run_dir,
        round_id=round_id,
        role=role,
        origin_kind="source-selection",
        payload=payload,
    )

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    received = {maybe_text(item) for item in imports.get("source_selection_roles_received", []) if maybe_text(item)}
    received.add(role)
    imports["source_selection_roles_received"] = sorted(received)
    state["imports"] = imports
    state["stage"] = STAGE_READY_PREPARE if received == set(SOURCE_SELECTION_ROLES) else STAGE_AWAITING_SOURCE_SELECTION
    save_state(run_dir, state)
    return _state_result(
        imported_kind="source-selection",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
        role=role,
    )


def _investigation_state_artifact_spec(run_dir: Path, round_id: str) -> dict[str, Any]:
    return {
        "path": investigation_state_path(run_dir, round_id),
        "label": "investigation-state",
        "artifact_kind": "derived-state",
        "required_current": True,
    }


def _investigation_actions_artifact_spec(run_dir: Path, round_id: str) -> dict[str, Any]:
    return {
        "path": investigation_actions_path(run_dir, round_id),
        "label": "investigation-actions",
        "artifact_kind": "derived-state",
        "required_current": True,
    }


def _curation_derived_artifact_specs(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    return [
        {
            "path": claim_submissions_path(run_dir, round_id),
            "label": "claim-submissions",
            "artifact_kind": "canonical",
            "required_current": True,
        },
        {
            "path": observation_submissions_path(run_dir, round_id),
            "label": "observation-submissions",
            "artifact_kind": "canonical",
            "required_current": True,
        },
        {
            "path": shared_claims_path(run_dir, round_id),
            "label": "shared-claims",
            "artifact_kind": "canonical",
            "required_current": True,
        },
        {
            "path": shared_observations_path(run_dir, round_id),
            "label": "shared-observations",
            "artifact_kind": "canonical",
            "required_current": True,
        },
        {
            "path": claims_active_path(run_dir, round_id),
            "label": "claims-active",
            "artifact_kind": "library-view",
            "required_current": True,
        },
        {
            "path": observations_active_path(run_dir, round_id),
            "label": "observations-active",
            "artifact_kind": "library-view",
            "required_current": True,
        },
        _investigation_state_artifact_spec(run_dir, round_id),
        _investigation_actions_artifact_spec(run_dir, round_id),
    ]


def _complete_curation_import(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    payload: Any,
    source_path: Path,
    target: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    materialize_curations: RoundArtifactBuilder,
    build_data_readiness_artifacts: RoundArtifactBuilder,
    imported_kind: str,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    derived_artifact_specs: list[dict[str, Any]] | None = None
    write_json(target, payload, pretty=True)
    persist_override_requests_for_role(
        run_dir=run_dir,
        round_id=round_id,
        role=role,
        origin_kind=imported_kind,
        payload=payload,
    )

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    received = {maybe_text(item) for item in imports.get("curation_roles_received", []) if maybe_text(item)}
    received.add(role)
    imports["curation_roles_received"] = sorted(received)
    state["imports"] = imports
    if received == set(CURATION_ROLES):
        imports["data_readiness_roles_received"] = []
        imports["matching_authorization_received"] = False
        imports["matching_adjudication_received"] = False
        imports["investigation_review_received"] = False
        imports["report_roles_received"] = []
        imports["decision_received"] = False
        materialize_curations(run_dir, round_id)
        materialize_investigation_bundle(run_dir, round_id, pretty=True)
        build_data_readiness_artifacts(run_dir, round_id)
        derived_artifact_specs = _curation_derived_artifact_specs(run_dir, round_id)
        state["stage"] = STAGE_AWAITING_DATA_READINESS
    else:
        state["stage"] = STAGE_AWAITING_EVIDENCE_CURATION
    save_state(run_dir, state)
    return _state_result(
        imported_kind=imported_kind,
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
        derived_artifact_specs=derived_artifact_specs,
    )


def import_claim_curation_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    materialize_curations: RoundArtifactBuilder,
    build_data_readiness_artifacts: RoundArtifactBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_claim_curation_matches(payload, round_id=round_id)
    return _complete_curation_import(
        run_dir=run_dir,
        state=state,
        role="sociologist",
        payload=payload,
        source_path=source_path,
        target=claim_curation_path(run_dir, round_id),
        save_state=save_state,
        status_builder=status_builder,
        materialize_curations=materialize_curations,
        build_data_readiness_artifacts=build_data_readiness_artifacts,
        imported_kind="claim-curation",
    )


def import_observation_curation_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    materialize_curations: RoundArtifactBuilder,
    build_data_readiness_artifacts: RoundArtifactBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_observation_curation_matches(payload, round_id=round_id)
    return _complete_curation_import(
        run_dir=run_dir,
        state=state,
        role="environmentalist",
        payload=payload,
        source_path=source_path,
        target=observation_curation_path(run_dir, round_id),
        save_state=save_state,
        status_builder=status_builder,
        materialize_curations=materialize_curations,
        build_data_readiness_artifacts=build_data_readiness_artifacts,
        imported_kind="observation-curation",
    )


def import_report_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    build_decision_artifacts: RoundArtifactBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_report_matches(payload, round_id=round_id, role=role)
    target = report_draft_path(run_dir, round_id, role)
    write_json(target, payload, pretty=True)
    persist_override_requests_for_role(
        run_dir=run_dir,
        round_id=round_id,
        role=role,
        origin_kind="expert-report",
        payload=payload,
    )

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    received = {maybe_text(item) for item in imports.get("report_roles_received", []) if maybe_text(item)}
    received.add(role)
    imports["report_roles_received"] = sorted(received)
    state["imports"] = imports
    if received == set(REPORT_ROLES):
        build_decision_artifacts(run_dir, round_id)
        state["stage"] = STAGE_AWAITING_DECISION
    else:
        state["stage"] = STAGE_AWAITING_REPORTS
    save_state(run_dir, state)
    return _state_result(
        imported_kind="expert-report",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
        role=role,
    )


def import_decision_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_decision_matches(payload, round_id=round_id)
    target = decision_draft_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    persist_override_requests_for_role(
        run_dir=run_dir,
        round_id=round_id,
        role="moderator",
        origin_kind="council-decision",
        payload=payload,
    )

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    imports["decision_received"] = True
    state["imports"] = imports
    state["stage"] = STAGE_READY_PROMOTE
    save_state(run_dir, state)
    return _state_result(
        imported_kind="council-decision",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
    )


def import_data_readiness_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    build_matching_authorization_artifacts: RoundArtifactBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_data_readiness_matches(payload, round_id=round_id, role=role)
    target = data_readiness_report_path(run_dir, round_id, role)
    write_json(target, payload, pretty=True)
    persist_override_requests_for_role(
        run_dir=run_dir,
        round_id=round_id,
        role=role,
        origin_kind="data-readiness-report",
        payload=payload,
    )

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    received = {maybe_text(item) for item in imports.get("data_readiness_roles_received", []) if maybe_text(item)}
    received.add(role)
    imports["data_readiness_roles_received"] = sorted(received)
    imports["matching_authorization_received"] = False
    imports["matching_adjudication_received"] = False
    imports["investigation_review_received"] = False
    imports["report_roles_received"] = []
    imports["decision_received"] = False
    state["imports"] = imports
    if received == set(READINESS_ROLES):
        build_matching_authorization_artifacts(run_dir, round_id)
        state["stage"] = STAGE_AWAITING_MATCHING_AUTHORIZATION
    else:
        state["stage"] = STAGE_AWAITING_DATA_READINESS
    save_state(run_dir, state)
    return _state_result(
        imported_kind="data-readiness-report",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
        role=role,
    )


def import_matching_authorization_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    build_matching_adjudication_artifacts: RoundArtifactBuilder,
    build_decision_artifacts: RoundArtifactBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = normalize_matching_authorization_payload(payload)
    ensure_matching_authorization_matches(payload, round_id=round_id, expected_run_id=current_run_id(run_dir))
    payload = effective_matching_authorization_payload(
        mission=load_mission(run_dir),
        round_id=round_id,
        payload=payload,
    )
    target = matching_authorization_path(run_dir, round_id)
    write_json(target, payload, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    imports["matching_authorization_received"] = True
    imports["matching_adjudication_received"] = False
    imports["investigation_review_received"] = False
    imports["report_roles_received"] = []
    imports["decision_received"] = False
    state["imports"] = imports
    if maybe_text(payload.get("authorization_status")) == "authorized":
        build_matching_adjudication_artifacts(run_dir, round_id)
        state["stage"] = STAGE_AWAITING_MATCHING_ADJUDICATION
    else:
        build_decision_artifacts(run_dir, round_id)
        state["stage"] = STAGE_AWAITING_DECISION
    save_state(run_dir, state)
    return _state_result(
        imported_kind="matching-authorization",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
    )


def import_matching_adjudication_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_matching_adjudication_matches(payload, round_id=round_id, expected_run_id=current_run_id(run_dir))
    authorization_payload = load_json_if_exists(matching_authorization_path(run_dir, round_id))
    authorization_payload = effective_matching_authorization_payload(
        mission=load_mission(run_dir),
        round_id=round_id,
        payload=authorization_payload if isinstance(authorization_payload, dict) else {},
    )
    if maybe_text(authorization_payload.get("authorization_status")) != "authorized":
        raise ValueError("Matching-adjudication import requires canonical matching_authorization.json with authorization_status=authorized.")
    if maybe_text(payload.get("authorization_id")) != maybe_text(authorization_payload.get("authorization_id")):
        raise ValueError("Matching-adjudication authorization_id does not match matching_authorization.json.")
    target = matching_adjudication_path(run_dir, round_id)
    write_json(target, payload, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    imports["matching_authorization_received"] = True
    imports["matching_adjudication_received"] = True
    imports["investigation_review_received"] = False
    imports["report_roles_received"] = []
    imports["decision_received"] = False
    state["imports"] = imports
    state["stage"] = STAGE_READY_MATCHING_ADJUDICATION
    save_state(run_dir, state)
    return _state_result(
        imported_kind="matching-adjudication",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
    )


def import_investigation_review_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    build_report_artifacts: RoundArtifactBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_investigation_review_matches(payload, round_id=round_id, expected_run_id=current_run_id(run_dir))
    matching_result = load_json_if_exists(matching_result_path(run_dir, round_id))
    if not isinstance(matching_result, dict):
        raise ValueError("Investigation-review import requires canonical shared matching_result.json.")
    evidence_adjudication = load_json_if_exists(evidence_adjudication_path(run_dir, round_id))
    if not isinstance(evidence_adjudication, dict):
        raise ValueError("Investigation-review import requires canonical shared evidence_adjudication.json.")
    if maybe_text(payload.get("matching_result_id")) != maybe_text(matching_result.get("result_id")):
        raise ValueError("Investigation-review matching_result_id does not match shared matching_result.json.")
    target = investigation_review_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    materialize_investigation_bundle(run_dir, round_id, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    imports["matching_authorization_received"] = True
    imports["matching_adjudication_received"] = True
    imports["investigation_review_received"] = True
    imports["report_roles_received"] = []
    imports["decision_received"] = False
    state["imports"] = imports
    build_report_artifacts(run_dir, round_id)
    state["stage"] = STAGE_AWAITING_REPORTS
    save_state(run_dir, state)
    return _state_result(
        imported_kind="investigation-review",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
        derived_artifact_specs=[
            _investigation_state_artifact_spec(run_dir, round_id),
            _investigation_actions_artifact_spec(run_dir, round_id),
        ],
    )


def import_fetch_execution_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_fetch_execution_matches(payload, run_dir=run_dir, round_id=round_id, source_path=source_path)
    target = fetch_execution_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    state["fetch_execution"] = "external-import"
    state["stage"] = STAGE_READY_DATA_PLANE
    save_state(run_dir, state)
    result = _state_result(
        imported_kind="fetch-execution",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
    )
    record_fetch_phase_receipt(run_dir=run_dir, round_id=round_id, payload=payload)
    return result


def import_data_plane_execution_payload(
    *,
    run_dir: Path,
    state: dict[str, Any],
    payload: Any,
    source_path: Path,
    save_state: StateSaver,
    status_builder: StatusBuilder,
    signal_corpus_importer: SignalCorpusImporter | None = None,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_data_plane_execution_matches(payload, run_dir=run_dir, round_id=round_id, source_path=source_path)
    target = data_plane_execution_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    signal_corpus_import = signal_corpus_importer(run_dir, state, round_id) if signal_corpus_importer is not None else None
    state["stage"] = STAGE_AWAITING_EVIDENCE_CURATION
    state["imports"] = {
        "task_review_received": True,
        "source_selection_roles_received": list(SOURCE_SELECTION_ROLES),
        "curation_roles_received": [],
        "data_readiness_roles_received": [],
        "matching_authorization_received": False,
        "matching_adjudication_received": False,
        "investigation_review_received": False,
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    result = _state_result(
        imported_kind="data-plane-execution",
        target_path=target,
        source_path=source_path,
        run_dir=run_dir,
        state=state,
        status_builder=status_builder,
    )
    record_normalize_phase_receipt(run_dir=run_dir, round_id=round_id, payload=payload)
    if signal_corpus_import is not None:
        result["signal_corpus_import"] = signal_corpus_import
    return result
