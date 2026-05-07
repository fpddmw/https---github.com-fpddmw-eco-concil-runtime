from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane import load_governed_execution_control_state
from eco_council_runtime.kernel.execution.executor import SkillExecutionError, maybe_text, new_runtime_event_id, utc_now_iso
from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.operator.surfaces import (
    build_reporting_surface,
    load_council_decision_wrapper,
    load_final_publication_wrapper,
    load_report_basis_freeze_wrapper,
    load_reporting_handoff_wrapper,
    load_supervisor_state_wrapper,
)
from eco_council_runtime.kernel.core.paths import (
    controller_state_path,
    history_bootstrap_state_path,
    round_close_state_path,
    supervisor_state_path,
)

ARCHIVE_SIGNAL_SKILL_NAME = "archive-signal-corpus"
ARCHIVE_CASE_SKILL_NAME = "archive-case-library"
HISTORY_BOOTSTRAP_SKILL_NAME = "materialize-history-context"
ARCHIVE_FAILURE_POLICIES = ("block", "warn")

def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results

def round_artifact_paths(run_dir: Path, round_id: str) -> dict[str, str]:
    return {
        "controller_state_path": str(controller_state_path(run_dir, round_id).resolve()),
        "supervisor_state_path": str(supervisor_state_path(run_dir, round_id).resolve()),
        "round_close_state_path": str(round_close_state_path(run_dir, round_id).resolve()),
        "history_bootstrap_state_path": str(history_bootstrap_state_path(run_dir, round_id).resolve()),
        "signal_archive_output_path": str((run_dir / "archive" / f"signal_corpus_import_{round_id}.json").resolve()),
        "case_archive_output_path": str((run_dir / "archive" / f"case_library_import_{round_id}.json").resolve()),
        "signal_archive_db_path": str((run_dir / ".." / "archives" / "eco_signal_corpus.sqlite").resolve()),
        "case_archive_db_path": str((run_dir / ".." / "archives" / "eco_case_library.sqlite").resolve()),
        "case_query_path": str((run_dir / "archive" / f"case_library_query_{round_id}.json").resolve()),
        "signal_query_path": str((run_dir / "archive" / f"signal_corpus_query_{round_id}.json").resolve()),
        "history_retrieval_path": str((run_dir / "investigation" / f"history_retrieval_{round_id}.json").resolve()),
        "history_context_path": str((run_dir / "investigation" / f"history_context_{round_id}.md").resolve()),
        "report_basis_freeze_path": str((run_dir / "report_basis" / f"frozen_report_basis_{round_id}.json").resolve()),
        "reporting_handoff_path": str((run_dir / "reporting" / f"reporting_handoff_{round_id}.json").resolve()),
        "council_decision_draft_path": str((run_dir / "reporting" / f"council_decision_draft_{round_id}.json").resolve()),
        "council_decision_path": str((run_dir / "reporting" / f"council_decision_{round_id}.json").resolve()),
        "final_publication_path": str((run_dir / "reporting" / f"final_publication_{round_id}.json").resolve()),
    }

def selected_decision_artifact(
    run_dir: Path,
    round_id: str,
    control_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(control_state, dict):
        decision = (
            control_state.get("decision", {})
            if isinstance(control_state.get("decision"), dict)
            else {}
        )
        if decision:
            return decision
        draft = (
            control_state.get("decision_draft", {})
            if isinstance(control_state.get("decision_draft"), dict)
            else {}
        )
        if draft:
            return draft
    decision = load_json_if_exists(run_dir / "reporting" / f"council_decision_{round_id}.json")
    if isinstance(decision, dict):
        return decision
    draft = load_json_if_exists(run_dir / "reporting" / f"council_decision_draft_{round_id}.json")
    if isinstance(draft, dict):
        return draft
    return {}

def infer_publication_status(final_publication: dict[str, Any], decision: dict[str, Any]) -> tuple[str, str]:
    publication_status = maybe_text(final_publication.get("publication_status"))
    publication_posture = maybe_text(final_publication.get("publication_posture"))
    if publication_status or publication_posture:
        return publication_status, publication_posture
    readiness = maybe_text(decision.get("publication_readiness"))
    if readiness == "ready":
        return "ready-for-release", "release"
    if readiness:
        return "hold-release", "withhold"
    return "", ""

def infer_close_posture(
    *,
    reporting_ready: bool,
    publication_posture: str,
    publication_materialized: bool,
    supervisor_status: str,
    reporting_handoff_status: str,
) -> str:
    if publication_materialized and publication_posture == "release":
        return "published-release"
    if publication_materialized and publication_posture == "withhold":
        return "published-withhold"
    if reporting_ready:
        return "reporting-ready-unpublished"
    if (
        supervisor_status == "hold-investigation-open"
        or reporting_handoff_status == "investigation-open"
    ):
        return "investigation-hold"
    return "post-round-pending"

def round_terminal_state(
    run_dir: Path,
    run_id: str,
    round_id: str,
    artifacts: dict[str, str],
) -> dict[str, Any]:
    control_state = load_governed_execution_control_state(run_dir, run_id=run_id, round_id=round_id)
    controller = (
        control_state.get("controller", {})
        if isinstance(control_state.get("controller"), dict)
        else {}
    ) or load_json_if_exists(Path(artifacts["controller_state_path"])) or {}
    supervisor_context = load_supervisor_state_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        supervisor_state_path=artifacts["supervisor_state_path"],
    )
    supervisor = (
        supervisor_context.get("payload")
        if isinstance(supervisor_context.get("payload"), dict)
        else {}
    )
    report_basis_context = load_report_basis_freeze_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        report_basis_path=artifacts["report_basis_freeze_path"],
    )
    report_basis = (
        report_basis_context.get("payload")
        if isinstance(report_basis_context.get("payload"), dict)
        else {}
    )
    handoff_context = load_reporting_handoff_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        reporting_handoff_path=artifacts["reporting_handoff_path"],
    )
    handoff = (
        handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else {}
    )
    decision_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="canonical",
        decision_path=artifacts["council_decision_path"],
    )
    decision_draft_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
        decision_path=artifacts["council_decision_draft_path"],
    )
    decision = (
        decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else {}
    ) or (
        decision_draft_context.get("payload")
        if isinstance(decision_draft_context.get("payload"), dict)
        else {}
    )
    final_publication_context = load_final_publication_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        output_path=artifacts["final_publication_path"],
    )
    final_publication = (
        final_publication_context.get("payload")
        if isinstance(final_publication_context.get("payload"), dict)
        else {}
    )
    controller_status = maybe_text(controller.get("controller_status"))
    supervisor_status = maybe_text(supervisor.get("supervisor_status"))
    reporting_surface = build_reporting_surface(
        supervisor_payload=supervisor,
        handoff_payload=handoff,
        decision_draft_payload=decision_draft_context.get("payload")
        if isinstance(decision_draft_context.get("payload"), dict)
        else {},
        decision_payload=decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else {},
        final_publication_payload=final_publication,
    )
    report_basis_status = (
        maybe_text(final_publication.get("report_basis_status"))
        or maybe_text(reporting_surface.get("report_basis_status"))
        or maybe_text(report_basis.get("report_basis_status"))
        or maybe_text(supervisor.get("report_basis_status"))
    )
    readiness_status = (
        maybe_text(reporting_surface.get("readiness_status"))
        or maybe_text(supervisor.get("readiness_status"))
        or maybe_text(controller.get("readiness_status"))
    )
    publication_status, publication_posture = infer_publication_status(final_publication, decision)
    block_close = False
    block_reason = ""
    block_message = ""
    if not supervisor:
        block_close = True
        block_reason = "missing-supervisor-state"
        block_message = "Round close requires a supervisor state record so the terminal posture is explicit."
    elif supervisor_status == "controller-failed" or controller_status == "failed":
        block_close = True
        block_reason = "controller-failed"
        block_message = "Round close is blocked because the governed-execution controller did not finish successfully."
    close_posture = infer_close_posture(
        reporting_ready=bool(reporting_surface.get("reporting_ready")),
        publication_posture=publication_posture,
        publication_materialized=bool(final_publication),
        supervisor_status=supervisor_status,
        reporting_handoff_status=maybe_text(reporting_surface.get("handoff_status")),
    )
    return {
        "controller": controller,
        "supervisor": supervisor,
        "report_basis": report_basis,
        "handoff": handoff,
        "decision": decision,
        "final_publication": final_publication,
        "controller_status": controller_status or "missing",
        "supervisor_status": supervisor_status or "missing",
        "readiness_status": readiness_status or "unknown",
        "report_basis_status": report_basis_status or "unknown",
        "reporting_ready": bool(reporting_surface.get("reporting_ready")),
        "reporting_blockers": (
            reporting_surface.get("reporting_blockers", [])
            if isinstance(reporting_surface.get("reporting_blockers"), list)
            else []
        ),
        "reporting_handoff_status": maybe_text(
            reporting_surface.get("handoff_status")
        ),
        "reporting_surface_source": maybe_text(
            reporting_surface.get("surface_source")
        ),
        "publication_status": publication_status or "unpublished",
        "publication_posture": publication_posture or "unpublished",
        "close_posture": close_posture,
        "block_close": block_close,
        "block_reason": block_reason,
        "block_message": block_message,
    }

def close_step_blueprints(artifacts: dict[str, str]) -> list[dict[str, Any]]:
    return [
        {
            "stage": "archive-signal-corpus",
            "skill_name": ARCHIVE_SIGNAL_SKILL_NAME,
            "expected_output_path": artifacts["signal_archive_output_path"],
            "operator_summary": "Freeze the normalized signal plane into the shared cross-run signal corpus.",
        },
        {
            "stage": "archive-case-library",
            "skill_name": ARCHIVE_CASE_SKILL_NAME,
            "expected_output_path": artifacts["case_archive_output_path"],
            "operator_summary": "Freeze the round or published case state into the shared case library.",
        },
    ]

def history_step_blueprint(artifacts: dict[str, str]) -> dict[str, Any]:
    return {
        "stage": "history-context-bootstrap",
        "skill_name": HISTORY_BOOTSTRAP_SKILL_NAME,
        "expected_output_path": artifacts["history_retrieval_path"],
        "operator_summary": "Query archived cases and signals, then materialize one retrieval-ready history context bundle.",
    }

def step_index(steps: list[dict[str, Any]], stage_name: str) -> int:
    for index, item in enumerate(steps):
        if maybe_text(item.get("stage")) == stage_name:
            return index
    raise ValueError(f"Missing post-round stage: {stage_name}")

def refresh_round_close_payload(payload: dict[str, Any]) -> dict[str, Any]:
    steps = payload.get("steps", []) if isinstance(payload.get("steps"), list) else []
    completed_stage_names = [maybe_text(step.get("stage")) for step in steps if maybe_text(step.get("status")) == "completed"]
    failed_stage_names = [maybe_text(step.get("stage")) for step in steps if maybe_text(step.get("status")) == "failed"]
    pending_stage_names = [maybe_text(step.get("stage")) for step in steps if maybe_text(step.get("status")) not in {"completed", "failed"}]
    current_stage = ""
    for step in steps:
        if maybe_text(step.get("status")) == "running":
            current_stage = maybe_text(step.get("stage"))
            break
    if not current_stage:
        current_stage = failed_stage_names[0] if failed_stage_names else (pending_stage_names[0] if pending_stage_names else "")
    payload["generated_at_utc"] = utc_now_iso()
    payload["completed_stage_names"] = completed_stage_names
    payload["pending_stage_names"] = pending_stage_names
    payload["failed_stage"] = failed_stage_names[0] if failed_stage_names else ""
    payload["current_stage"] = current_stage
    payload["history_bootstrap_recommended"] = maybe_text(payload.get("close_status")) in {"completed", "completed-with-warnings"}
    return payload

def refresh_history_bootstrap_payload(payload: dict[str, Any]) -> dict[str, Any]:
    steps = payload.get("steps", []) if isinstance(payload.get("steps"), list) else []
    failed_stage = ""
    current_stage = ""
    for step in steps:
        status = maybe_text(step.get("status"))
        if status == "failed" and not failed_stage:
            failed_stage = maybe_text(step.get("stage"))
        if status == "running" and not current_stage:
            current_stage = maybe_text(step.get("stage"))
    if not current_stage:
        if failed_stage:
            current_stage = failed_stage
        else:
            for step in steps:
                if maybe_text(step.get("status")) != "completed":
                    current_stage = maybe_text(step.get("stage"))
                    break
    payload["generated_at_utc"] = utc_now_iso()
    payload["failed_stage"] = failed_stage
    payload["current_stage"] = current_stage
    return payload

def persist_round_close_state(run_dir: Path, round_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    payload["artifacts"] = round_artifact_paths(run_dir, round_id)
    write_json(round_close_state_path(run_dir, round_id), refresh_round_close_payload(payload))
    return payload

def persist_history_bootstrap_state(run_dir: Path, round_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    payload["artifacts"] = round_artifact_paths(run_dir, round_id)
    write_json(history_bootstrap_state_path(run_dir, round_id), refresh_history_bootstrap_payload(payload))
    return payload

def summarized_skill_step(blueprint: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    summary = result.get("summary", {}) if isinstance(result.get("summary"), dict) else {}
    event = result.get("event", {}) if isinstance(result.get("event"), dict) else {}
    skill_payload = result.get("skill_payload", {}) if isinstance(result.get("skill_payload"), dict) else {}
    payload_summary = skill_payload.get("summary", {}) if isinstance(skill_payload.get("summary"), dict) else {}
    return {
        **blueprint,
        "status": maybe_text(event.get("status")) or "completed",
        "event_id": maybe_text(summary.get("event_id")),
        "receipt_id": maybe_text(summary.get("receipt_id")),
        "started_at_utc": maybe_text(event.get("started_at_utc")),
        "completed_at_utc": maybe_text(event.get("completed_at_utc")) or utc_now_iso(),
        "artifact_path": maybe_text(payload_summary.get("output_path")) or maybe_text(blueprint.get("expected_output_path")),
        "artifact_refs": skill_payload.get("artifact_refs", []) if isinstance(skill_payload.get("artifact_refs"), list) else [],
        "canonical_ids": skill_payload.get("canonical_ids", []) if isinstance(skill_payload.get("canonical_ids"), list) else [],
        "warnings": skill_payload.get("warnings", []) if isinstance(skill_payload.get("warnings"), list) else [],
    }

def failed_skill_step(blueprint: dict[str, Any], exc: SkillExecutionError) -> dict[str, Any]:
    payload = exc.payload if isinstance(exc.payload, dict) else {}
    failure = payload.get("failure", {}) if isinstance(payload.get("failure"), dict) else {}
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    return {
        **blueprint,
        "status": "failed",
        "event_id": maybe_text(summary.get("event_id")),
        "receipt_id": maybe_text(payload.get("receipt_id")),
        "started_at_utc": "",
        "completed_at_utc": utc_now_iso(),
        "artifact_path": maybe_text(blueprint.get("expected_output_path")),
        "artifact_refs": [],
        "canonical_ids": [],
        "warnings": payload.get("warnings", []) if isinstance(payload.get("warnings"), list) else [],
        "failure": failure or {"message": payload.get("message", str(exc))},
    }

def round_close_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "round-close", started_at, completed_at, payload.get("close_status")),
        "event_type": "round-close",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": payload.get("close_status"),
        "contract_mode": payload.get("contract_mode"),
        "execution_policy": payload.get("execution_policy", {}),
        "archive_failure_policy": payload.get("archive_failure_policy"),
        "archive_status": payload.get("archive_status"),
        "close_posture": payload.get("close_posture"),
        "publication_status": payload.get("publication_status"),
        "report_basis_status": payload.get("report_basis_status"),
        "reporting_ready": payload.get("reporting_ready"),
        "reporting_handoff_status": payload.get("reporting_handoff_status"),
        "reporting_blockers": payload.get("reporting_blockers", []),
        "failed_stage": payload.get("failed_stage"),
        "round_close_path": payload.get("artifacts", {}).get("round_close_state_path", "")
        if isinstance(payload.get("artifacts"), dict)
        else "",
    }

def history_bootstrap_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "history-bootstrap", started_at, completed_at, payload.get("bootstrap_status")),
        "event_type": "history-bootstrap",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": payload.get("bootstrap_status"),
        "contract_mode": payload.get("contract_mode"),
        "execution_policy": payload.get("execution_policy", {}),
        "selected_case_count": payload.get("selected_case_count", 0),
        "selected_signal_count": payload.get("selected_signal_count", 0),
        "history_bootstrap_path": payload.get("artifacts", {}).get("history_bootstrap_state_path", "")
        if isinstance(payload.get("artifacts"), dict)
        else "",
    }
