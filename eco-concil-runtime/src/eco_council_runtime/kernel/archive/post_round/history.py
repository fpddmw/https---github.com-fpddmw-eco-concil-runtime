from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.executor import SkillExecutionError, maybe_text, run_skill, utc_now_iso
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import init_round_cursor, init_run_manifest, load_json_if_exists
from eco_council_runtime.kernel.core.paths import ensure_runtime_dirs, history_bootstrap_state_path, round_close_state_path
from eco_council_runtime.kernel.core.registry import write_registry
from eco_council_runtime.kernel.archive.post_round.common import (
    HISTORY_BOOTSTRAP_SKILL_NAME,
    failed_skill_step,
    history_bootstrap_event,
    history_step_blueprint,
    persist_history_bootstrap_state,
    round_artifact_paths,
    summarized_skill_step,
)

def bootstrap_history_context(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    return bootstrap_history_context_with_contract_mode(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        actor_role="runtime-operator",
        contract_mode="warn",
    )

def bootstrap_history_context_with_contract_mode(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    actor_role: str = "runtime-operator",
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
            actor_role=actor_role,
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
