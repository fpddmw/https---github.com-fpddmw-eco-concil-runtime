from __future__ import annotations

from pathlib import Path
from typing import Any

from .deliberation_plane import load_phase2_control_state
from .executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill, utc_now_iso
from .ledger import append_ledger_event
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists, write_json
from .paths import (
    controller_state_path,
    ensure_runtime_dirs,
    history_bootstrap_state_path,
    round_close_state_path,
    supervisor_state_path,
)
from .registry import write_registry

ARCHIVE_SIGNAL_SKILL_NAME = "eco-archive-signal-corpus"
ARCHIVE_CASE_SKILL_NAME = "eco-archive-case-library"
HISTORY_BOOTSTRAP_SKILL_NAME = "eco-materialize-history-context"
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
        "promotion_basis_path": str((run_dir / "promotion" / f"promoted_evidence_basis_{round_id}.json").resolve()),
        "reporting_handoff_path": str((run_dir / "reporting" / f"reporting_handoff_{round_id}.json").resolve()),
        "council_decision_draft_path": str((run_dir / "reporting" / f"council_decision_draft_{round_id}.json").resolve()),
        "council_decision_path": str((run_dir / "reporting" / f"council_decision_{round_id}.json").resolve()),
        "final_publication_path": str((run_dir / "reporting" / f"final_publication_{round_id}.json").resolve()),
    }


def selected_decision_artifact(run_dir: Path, round_id: str) -> dict[str, Any]:
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


def infer_close_posture(*, promotion_status: str, publication_posture: str, publication_materialized: bool, supervisor_status: str) -> str:
    if publication_materialized and publication_posture == "release":
        return "published-release"
    if publication_materialized and publication_posture == "withhold":
        return "published-withhold"
    if promotion_status == "promoted":
        return "promoted-unpublished"
    if supervisor_status == "hold-investigation-open":
        return "investigation-hold"
    return "post-round-pending"


def round_terminal_state(run_dir: Path, round_id: str, artifacts: dict[str, str]) -> dict[str, Any]:
    control_state = load_phase2_control_state(run_dir, round_id=round_id)
    controller = load_json_if_exists(Path(artifacts["controller_state_path"])) or (
        control_state.get("controller", {})
        if isinstance(control_state.get("controller"), dict)
        else {}
    )
    supervisor = load_json_if_exists(Path(artifacts["supervisor_state_path"])) or (
        control_state.get("supervisor", {})
        if isinstance(control_state.get("supervisor"), dict)
        else {}
    )
    promotion = load_json_if_exists(Path(artifacts["promotion_basis_path"])) or {}
    handoff = load_json_if_exists(Path(artifacts["reporting_handoff_path"])) or {}
    decision = selected_decision_artifact(run_dir, round_id)
    final_publication = load_json_if_exists(Path(artifacts["final_publication_path"])) or {}
    controller_status = maybe_text(controller.get("controller_status"))
    supervisor_status = maybe_text(supervisor.get("supervisor_status"))
    promotion_status = maybe_text(final_publication.get("promotion_status")) or maybe_text(promotion.get("promotion_status")) or maybe_text(supervisor.get("promotion_status"))
    readiness_status = maybe_text(supervisor.get("readiness_status")) or maybe_text(controller.get("readiness_status"))
    publication_status, publication_posture = infer_publication_status(final_publication, decision)
    block_close = False
    block_reason = ""
    block_message = ""
    if not supervisor:
        block_close = True
        block_reason = "missing-supervisor-state"
        block_message = "Round close requires a supervisor artifact so the terminal posture is explicit."
    elif supervisor_status == "controller-failed" or controller_status == "failed":
        block_close = True
        block_reason = "controller-failed"
        block_message = "Round close is blocked because the phase-2 controller did not finish successfully."
    close_posture = infer_close_posture(
        promotion_status=promotion_status,
        publication_posture=publication_posture,
        publication_materialized=bool(final_publication),
        supervisor_status=supervisor_status,
    )
    return {
        "controller": controller,
        "supervisor": supervisor,
        "promotion": promotion,
        "handoff": handoff,
        "decision": decision,
        "final_publication": final_publication,
        "controller_status": controller_status or "missing",
        "supervisor_status": supervisor_status or "missing",
        "readiness_status": readiness_status or "unknown",
        "promotion_status": promotion_status or "unknown",
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
        "promotion_status": payload.get("promotion_status"),
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


def close_round(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    return close_round_with_contract_mode(run_dir, run_id=run_id, round_id=round_id, contract_mode="warn")


def close_round_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
    archive_failure_policy: str = "block",
) -> dict[str, Any]:
    if archive_failure_policy not in ARCHIVE_FAILURE_POLICIES:
        raise ValueError(f"Unsupported archive_failure_policy: {archive_failure_policy}")
    ensure_runtime_dirs(run_dir)
    write_registry(run_dir)
    init_run_manifest(run_dir, run_id)
    init_round_cursor(run_dir, run_id)
    artifacts = round_artifact_paths(run_dir, round_id)
    existing = load_json_if_exists(round_close_state_path(run_dir, round_id)) or {}
    if maybe_text(existing.get("close_status")) in {"completed", "completed-with-warnings"}:
        return {
            "status": "completed",
            "summary": {
                "run_id": run_id,
                "round_id": round_id,
                "round_close_path": artifacts["round_close_state_path"],
                "close_status": existing.get("close_status", ""),
                "archive_status": existing.get("archive_status", ""),
                "close_posture": existing.get("close_posture", ""),
            },
            "round_close": existing,
        }

    execution_policy = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects or [],
    }
    execution_kwargs = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects,
    }
    started_at = utc_now_iso()
    terminal_state = round_terminal_state(run_dir, round_id, artifacts)
    payload = {
        "schema_version": "runtime-round-close-v1",
        "generated_at_utc": started_at,
        "started_at_utc": started_at,
        "run_id": run_id,
        "round_id": round_id,
        "contract_mode": contract_mode,
        "execution_policy": execution_policy,
        "archive_failure_policy": archive_failure_policy,
        "archive_compaction_policy": "replace-per-run-snapshot",
        "close_status": "running",
        "archive_status": "pending",
        "close_posture": terminal_state["close_posture"],
        "controller_status": terminal_state["controller_status"],
        "supervisor_status": terminal_state["supervisor_status"],
        "readiness_status": terminal_state["readiness_status"],
        "promotion_status": terminal_state["promotion_status"],
        "publication_status": terminal_state["publication_status"],
        "publication_posture": terminal_state["publication_posture"],
        "recommended_next_skills": [],
        "warnings": [],
        "failure": {},
        "steps": [{**step, "status": "pending", "artifact_refs": [], "canonical_ids": [], "warnings": []} for step in close_step_blueprints(artifacts)],
        "artifacts": artifacts,
    }
    persist_round_close_state(run_dir, round_id, payload)

    if terminal_state["block_close"]:
        payload["close_status"] = "blocked"
        payload["archive_status"] = "blocked"
        payload["failure"] = {
            "error_code": terminal_state["block_reason"] or "round-close-blocked",
            "message": terminal_state["block_message"] or "Round close is blocked.",
            "retryable": False,
            "recovery_hints": ["Finish the supervisor phase successfully before closing the round."],
        }
        persist_round_close_state(run_dir, round_id, payload)
        append_ledger_event(
            run_dir,
            round_close_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                payload=payload,
            ),
        )
        raise SkillExecutionError(
            payload["failure"]["message"],
            {
                "status": "failed",
                "summary": {
                    "run_id": run_id,
                    "round_id": round_id,
                    "round_close_path": artifacts["round_close_state_path"],
                    "close_status": payload["close_status"],
                },
                "message": payload["failure"]["message"],
                "failure": payload["failure"],
                "round_close": payload,
            },
        )

    step_failures: list[dict[str, Any]] = []
    for blueprint in close_step_blueprints(artifacts):
        pos = step_index(payload["steps"], maybe_text(blueprint.get("stage")))
        payload["steps"][pos]["status"] = "running"
        payload["steps"][pos]["started_at_utc"] = utc_now_iso()
        persist_round_close_state(run_dir, round_id, payload)
        try:
            result = run_skill(
                run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name=maybe_text(blueprint.get("skill_name")),
                skill_args=[],
                contract_mode=contract_mode,
                **execution_kwargs,
            )
            payload["steps"][pos] = summarized_skill_step(blueprint, result)
        except SkillExecutionError as exc:
            payload["steps"][pos] = failed_skill_step(blueprint, exc)
            failure = {
                "stage": maybe_text(blueprint.get("stage")),
                "skill_name": maybe_text(blueprint.get("skill_name")),
                "message": exc.payload.get("message", str(exc)),
                "failure": exc.payload.get("failure", {}) if isinstance(exc.payload.get("failure"), dict) else {},
            }
            step_failures.append(failure)
            payload["warnings"].append(
                {"code": "archive-step-failed", "message": f"{failure['stage']} failed: {failure['message']}"}
            )
            if archive_failure_policy == "block":
                payload["close_status"] = "failed"
                payload["archive_status"] = "failed"
                payload["failure"] = {
                    "error_code": "archive-step-failed",
                    "message": failure["message"],
                    "retryable": bool(failure["failure"].get("retryable")) if isinstance(failure["failure"], dict) else False,
                    "stage": failure["stage"],
                    "skill_name": failure["skill_name"],
                    "recovery_hints": [
                        f"Re-run close-round after fixing {failure['skill_name']}.",
                        "Inspect the archive output paths and runtime ledger before retrying.",
                    ],
                }
                persist_round_close_state(run_dir, round_id, payload)
                append_ledger_event(
                    run_dir,
                    round_close_event(
                        run_id=run_id,
                        round_id=round_id,
                        started_at=started_at,
                        completed_at=utc_now_iso(),
                        payload=payload,
                    ),
                )
                raise SkillExecutionError(
                    payload["failure"]["message"],
                    {
                        "status": "failed",
                        "summary": {
                            "run_id": run_id,
                            "round_id": round_id,
                            "round_close_path": artifacts["round_close_state_path"],
                            "close_status": payload["close_status"],
                            "failed_stage": failure["stage"],
                        },
                        "message": payload["failure"]["message"],
                        "failure": payload["failure"],
                        "round_close": payload,
                    },
                )
        persist_round_close_state(run_dir, round_id, payload)

    payload["close_status"] = "completed-with-warnings" if step_failures else "completed"
    payload["archive_status"] = "degraded" if step_failures else "completed"
    payload["completed_at_utc"] = utc_now_iso()
    payload["recommended_next_skills"] = (
        unique_texts([failure["skill_name"] for failure in step_failures]) if step_failures else ["eco-materialize-history-context"]
    )
    persist_round_close_state(run_dir, round_id, payload)
    append_ledger_event(
        run_dir,
        round_close_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=maybe_text(payload.get("completed_at_utc")) or utc_now_iso(),
            payload=payload,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "round_close_path": artifacts["round_close_state_path"],
            "close_status": payload["close_status"],
            "archive_status": payload["archive_status"],
            "close_posture": payload["close_posture"],
        },
        "round_close": payload,
    }


def bootstrap_history_context(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    return bootstrap_history_context_with_contract_mode(run_dir, run_id=run_id, round_id=round_id, contract_mode="warn")


def bootstrap_history_context_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    write_registry(run_dir)
    init_run_manifest(run_dir, run_id)
    init_round_cursor(run_dir, run_id)
    artifacts = round_artifact_paths(run_dir, round_id)
    existing = load_json_if_exists(history_bootstrap_state_path(run_dir, round_id)) or {}
    if maybe_text(existing.get("bootstrap_status")) == "completed":
        return {
            "status": "completed",
            "summary": {
                "run_id": run_id,
                "round_id": round_id,
                "history_bootstrap_path": artifacts["history_bootstrap_state_path"],
                "bootstrap_status": existing.get("bootstrap_status", ""),
                "selected_case_count": int(existing.get("selected_case_count") or 0),
                "selected_signal_count": int(existing.get("selected_signal_count") or 0),
            },
            "history_bootstrap": existing,
        }

    close_state = load_json_if_exists(round_close_state_path(run_dir, round_id)) or {}
    execution_policy = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects or [],
    }
    execution_kwargs = {
        "timeout_seconds": timeout_seconds,
        "retry_budget": retry_budget,
        "retry_backoff_ms": retry_backoff_ms,
        "allow_side_effects": allow_side_effects,
    }
    started_at = utc_now_iso()
    blueprint = history_step_blueprint(artifacts)
    payload = {
        "schema_version": "runtime-history-bootstrap-v1",
        "generated_at_utc": started_at,
        "started_at_utc": started_at,
        "run_id": run_id,
        "round_id": round_id,
        "contract_mode": contract_mode,
        "execution_policy": execution_policy,
        "bootstrap_status": "running",
        "bootstrap_mode": "archive-query",
        "close_status": maybe_text(close_state.get("close_status")),
        "archive_status": maybe_text(close_state.get("archive_status")),
        "selected_case_count": 0,
        "selected_signal_count": 0,
        "recommended_next_skills": [],
        "warnings": [],
        "failure": {},
        "steps": [{**blueprint, "status": "pending", "artifact_refs": [], "canonical_ids": [], "warnings": []}],
        "artifacts": artifacts,
    }
    persist_history_bootstrap_state(run_dir, round_id, payload)
    payload["steps"][0]["status"] = "running"
    payload["steps"][0]["started_at_utc"] = utc_now_iso()
    persist_history_bootstrap_state(run_dir, round_id, payload)

    try:
        result = run_skill(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=HISTORY_BOOTSTRAP_SKILL_NAME,
            skill_args=[],
            contract_mode=contract_mode,
            **execution_kwargs,
        )
    except SkillExecutionError as exc:
        payload["steps"][0] = failed_skill_step(blueprint, exc)
        payload["bootstrap_status"] = "failed"
        payload["failure"] = {
            "error_code": "history-bootstrap-failed",
            "message": exc.payload.get("message", str(exc)),
            "retryable": bool(exc.payload.get("failure", {}).get("retryable")) if isinstance(exc.payload.get("failure"), dict) else False,
            "stage": maybe_text(blueprint.get("stage")),
            "recovery_hints": [
                "Inspect archive databases and query artifacts before retrying history bootstrap.",
                "Re-run bootstrap-history-context after restoring archive accessibility.",
            ],
        }
        persist_history_bootstrap_state(run_dir, round_id, payload)
        append_ledger_event(
            run_dir,
            history_bootstrap_event(
                run_id=run_id,
                round_id=round_id,
                started_at=started_at,
                completed_at=utc_now_iso(),
                payload=payload,
            ),
        )
        raise SkillExecutionError(
            payload["failure"]["message"],
            {
                "status": "failed",
                "summary": {
                    "run_id": run_id,
                    "round_id": round_id,
                    "history_bootstrap_path": artifacts["history_bootstrap_state_path"],
                    "bootstrap_status": payload["bootstrap_status"],
                },
                "message": payload["failure"]["message"],
                "failure": payload["failure"],
                "history_bootstrap": payload,
            },
        )

    payload["steps"][0] = summarized_skill_step(blueprint, result)
    skill_payload = result.get("skill_payload", {}) if isinstance(result.get("skill_payload"), dict) else {}
    payload_summary = skill_payload.get("summary", {}) if isinstance(skill_payload.get("summary"), dict) else {}
    payload["bootstrap_status"] = "completed"
    payload["selected_case_count"] = int(payload_summary.get("selected_case_count") or 0)
    payload["selected_signal_count"] = int(payload_summary.get("selected_signal_count") or 0)
    payload["recommended_next_skills"] = (
        skill_payload.get("board_handoff", {}).get("suggested_next_skills", [])
        if isinstance(skill_payload.get("board_handoff"), dict)
        and isinstance(skill_payload.get("board_handoff", {}).get("suggested_next_skills"), list)
        else []
    )
    payload["warnings"] = skill_payload.get("warnings", []) if isinstance(skill_payload.get("warnings"), list) else []
    payload["completed_at_utc"] = utc_now_iso()
    persist_history_bootstrap_state(run_dir, round_id, payload)
    append_ledger_event(
        run_dir,
        history_bootstrap_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=maybe_text(payload.get("completed_at_utc")) or utc_now_iso(),
            payload=payload,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "history_bootstrap_path": artifacts["history_bootstrap_state_path"],
            "bootstrap_status": payload["bootstrap_status"],
            "selected_case_count": payload["selected_case_count"],
            "selected_signal_count": payload["selected_signal_count"],
        },
        "history_bootstrap": payload,
    }
