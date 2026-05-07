from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.executor import SkillExecutionError, maybe_text, run_skill, utc_now_iso
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import init_round_cursor, init_run_manifest, load_json_if_exists
from eco_council_runtime.kernel.core.paths import ensure_runtime_dirs, round_close_state_path
from eco_council_runtime.kernel.core.registry import write_registry
from eco_council_runtime.kernel.governance.transition_requests import (
    TRANSITION_KIND_CLOSE_ROUND,
    mark_transition_request_committed,
    request_payload_option,
    resolve_transition_request_for_execution,
)
from eco_council_runtime.kernel.archive.post_round.common import (
    ARCHIVE_FAILURE_POLICIES,
    close_step_blueprints,
    failed_skill_step,
    persist_round_close_state,
    round_artifact_paths,
    round_close_event,
    round_terminal_state,
    step_index,
    summarized_skill_step,
    unique_texts,
)

def close_round(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    transition_request_id: str,
) -> dict[str, Any]:
    return close_round_with_contract_mode(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_request_id=transition_request_id,
        actor_role="runtime-operator",
        contract_mode="warn",
    )

def close_round_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    transition_request_id: str,
    actor_role: str = "runtime-operator",
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
    transition_request = resolve_transition_request_for_execution(
        run_dir,
        request_id=transition_request_id,
        transition_kind=TRANSITION_KIND_CLOSE_ROUND,
        run_id=run_id,
        round_id=round_id,
    )
    requested_archive_failure_policy = maybe_text(
        request_payload_option(
            transition_request,
            "archive_failure_policy",
            "",
        )
    )
    if (
        archive_failure_policy == "block"
        and requested_archive_failure_policy in ARCHIVE_FAILURE_POLICIES
    ):
        archive_failure_policy = requested_archive_failure_policy
    artifacts = round_artifact_paths(run_dir, round_id)
    existing = load_json_if_exists(round_close_state_path(run_dir, round_id)) or {}
    if maybe_text(existing.get("close_status")) in {"completed", "completed-with-warnings"}:
        mark_transition_request_committed(
            run_dir,
            request_id=maybe_text(transition_request.get("request_id")),
            committed_by_role=actor_role,
            committed_object_kind="round-close",
            committed_object_id=round_id,
        )
        return {
            "status": "completed",
            "summary": {
                "run_id": run_id,
                "round_id": round_id,
                "round_close_path": artifacts["round_close_state_path"],
                "close_status": existing.get("close_status", ""),
                "archive_status": existing.get("archive_status", ""),
                "close_posture": existing.get("close_posture", ""),
                "transition_request_id": maybe_text(
                    transition_request.get("request_id")
                ),
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
    terminal_state = round_terminal_state(run_dir, run_id, round_id, artifacts)
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
        "report_basis_status": terminal_state["report_basis_status"],
        "reporting_ready": terminal_state["reporting_ready"],
        "reporting_blockers": terminal_state["reporting_blockers"],
        "reporting_handoff_status": terminal_state["reporting_handoff_status"],
        "reporting_surface_source": terminal_state["reporting_surface_source"],
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
                actor_role=actor_role,
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
        unique_texts([failure["skill_name"] for failure in step_failures]) if step_failures else ["materialize-history-context"]
    )
    persist_round_close_state(run_dir, round_id, payload)
    mark_transition_request_committed(
        run_dir,
        request_id=maybe_text(transition_request.get("request_id")),
        committed_by_role=actor_role,
        committed_object_kind="round-close",
        committed_object_id=round_id,
    )
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
            "transition_request_id": maybe_text(
                transition_request.get("request_id")
            ),
        },
        "round_close": payload,
    }
