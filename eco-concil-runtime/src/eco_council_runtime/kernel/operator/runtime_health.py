from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.ledger import load_ledger_tail
from eco_council_runtime.kernel.core.locking import runtime_lock_state_payload
from eco_council_runtime.kernel.core.manifest import write_json
from eco_council_runtime.kernel.core.paths import ensure_runtime_dirs, runtime_health_path
from eco_council_runtime.kernel.operator.admission_policy import load_admission_policy
from eco_council_runtime.kernel.operator.dead_letters import load_dead_letters
from eco_council_runtime.kernel.operator.operations_common import DEFAULT_HEALTH_SCHEMA, maybe_text, utc_now_iso


SUPERSEDING_SUCCESS_STATUSES = {"completed", "completed-with-warnings"}


def event_supersession_key(event: dict[str, Any]) -> tuple[str, str, str, str, str]:
    operation_id = (
        maybe_text(event.get("skill_name"))
        or maybe_text(event.get("round_close_path"))
        or maybe_text(event.get("transition_request_id"))
        or maybe_text(event.get("event_type"))
    )
    return (
        maybe_text(event.get("run_id")),
        maybe_text(event.get("round_id")),
        maybe_text(event.get("event_type")),
        operation_id,
        maybe_text(event.get("actor_role")),
    )


def runtime_health_payload(run_dir: Path, *, round_id: str = "") -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    policy = load_admission_policy(run_dir)
    alert_policy = policy.get("alert_policy", {}) if isinstance(policy.get("alert_policy"), dict) else {}
    runtime_lock = runtime_lock_state_payload(run_dir)
    events = load_ledger_tail(run_dir, 1_000_000)
    filtered_events = [
        event for event in events if isinstance(event, dict) and (not round_id or maybe_text(event.get("round_id")) == round_id)
    ]
    dead_letters = load_dead_letters(run_dir, round_id=round_id, limit=50)
    closed_dead_letter_ids = {
        maybe_text(item.get("dead_letter_id"))
        for item in dead_letters
        if maybe_text(item.get("dead_letter_id")) and maybe_text(item.get("resolution_status")) == "closed"
    }
    unresolved_events = [
        event
        for event in filtered_events
        if maybe_text(event.get("dead_letter_id")) not in closed_dead_letter_ids
    ]
    completed_keys_after: set[tuple[str, str, str, str, str]] = set()
    superseded_blocked_event_ids: set[str] = set()
    for event in reversed(filtered_events):
        key = event_supersession_key(event)
        if not key[3]:
            continue
        status = maybe_text(event.get("status"))
        if status in SUPERSEDING_SUCCESS_STATUSES:
            completed_keys_after.add(key)
        elif status == "blocked" and key in completed_keys_after:
            event_id = maybe_text(event.get("event_id"))
            if event_id:
                superseded_blocked_event_ids.add(event_id)
    failed_events = [event for event in unresolved_events if maybe_text(event.get("status")) == "failed"]
    blocked_events = [
        event
        for event in unresolved_events
        if maybe_text(event.get("status")) == "blocked"
        and maybe_text(event.get("event_id")) not in superseded_blocked_event_ids
        and not (
            maybe_text(event.get("event_type")) == "skill-preflight"
            and not maybe_text(event.get("dead_letter_id"))
        )
    ]
    degraded_events = [
        event for event in filtered_events if maybe_text(event.get("status")) in {"completed-with-warnings", "degraded"}
    ]
    receipt_conflict_events = [
        event
        for event in unresolved_events
        if isinstance(event.get("failure"), dict)
        and maybe_text(event["failure"].get("error_code"))
        == "receipt-payload-hash-conflict"
    ]
    recovered_events = [event for event in filtered_events if bool(event.get("recovered_after_retry"))]
    open_dead_letters = [item for item in dead_letters if maybe_text(item.get("resolution_status")) != "closed"]
    alerts: list[dict[str, Any]] = []
    failed_threshold = int(alert_policy.get("failed_event_threshold") or 1)
    blocked_threshold = int(alert_policy.get("blocked_event_threshold") or 1)
    degraded_threshold = int(alert_policy.get("degraded_event_threshold") or 1)
    dead_letter_threshold = int(alert_policy.get("dead_letter_threshold") or 1)
    if len(failed_events) >= failed_threshold:
        alerts.append(
            {
                "severity": "critical",
                "code": "failed-events-present",
                "message": f"{len(failed_events)} failed runtime events are present.",
            }
        )
    if len(blocked_events) >= blocked_threshold:
        alerts.append(
            {
                "severity": "critical",
                "code": "blocked-events-present",
                "message": f"{len(blocked_events)} blocked runtime events are present.",
            }
        )
    if len(open_dead_letters) >= dead_letter_threshold:
        alerts.append(
            {
                "severity": "critical",
                "code": "open-dead-letters-present",
                "message": f"{len(open_dead_letters)} dead letters still require operator review.",
            }
        )
    if receipt_conflict_events:
        alerts.append(
            {
                "severity": "critical",
                "code": "receipt-conflicts-present",
                "message": f"{len(receipt_conflict_events)} runtime receipt conflicts require operator review.",
            }
        )
    if maybe_text(runtime_lock.get("lock_state")) == "stale":
        alerts.append(
            {
                "severity": "critical",
                "code": "stale-runtime-lock",
                "message": "Runtime execution lock state is stale; verify no execution is still active before clearing it.",
            }
        )
    if len(degraded_events) >= degraded_threshold:
        alerts.append(
            {
                "severity": "warning",
                "code": "degraded-events-present",
                "message": f"{len(degraded_events)} degraded runtime events are present.",
            }
        )
    if recovered_events:
        alerts.append(
            {
                "severity": "warning",
                "code": "retry-recoveries-observed",
                "message": f"{len(recovered_events)} runtime events only completed after retry.",
            }
        )
    alert_status = "green"
    if any(maybe_text(item.get("severity")) == "critical" for item in alerts):
        alert_status = "red"
    elif alerts:
        alert_status = "yellow"
    return {
        "schema_version": DEFAULT_HEALTH_SCHEMA,
        "generated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "round_id": round_id,
        "permission_profile": maybe_text(policy.get("permission_profile")) or "standard",
        "alert_status": alert_status,
        "summary": {
            "event_count": len(filtered_events),
            "failed_event_count": len(failed_events),
            "blocked_event_count": len(blocked_events),
            "degraded_event_count": len(degraded_events),
            "receipt_conflict_count": len(receipt_conflict_events),
            "recovered_after_retry_count": len(recovered_events),
            "open_dead_letter_count": len(open_dead_letters),
            "runtime_lock_state": maybe_text(runtime_lock.get("lock_state")),
        },
        "alerts": alerts,
        "runtime_lock": runtime_lock,
        "latest_failed_events": [
            {
                "event_type": maybe_text(item.get("event_type")),
                "skill_name": maybe_text(item.get("skill_name")),
                "failed_stage": maybe_text(item.get("failed_stage")),
                "status": maybe_text(item.get("status")),
            }
            for item in failed_events[-5:]
        ],
        "latest_blocked_events": [
            {
                "event_type": maybe_text(item.get("event_type")),
                "skill_name": maybe_text(item.get("skill_name")),
                "status": maybe_text(item.get("status")),
            }
            for item in blocked_events[-5:]
        ],
        "latest_receipt_conflicts": [
            {
                "event_id": maybe_text(item.get("event_id")),
                "event_type": maybe_text(item.get("event_type")),
                "skill_name": maybe_text(item.get("skill_name")),
                "receipt_id": maybe_text(item.get("receipt_id")),
                "receipt_path": maybe_text(item.get("receipt_path")),
                "payload_hash": maybe_text(item.get("payload_hash")),
                "previous_payload_hash": maybe_text(
                    item.get("receipt_write", {}).get("previous_payload_hash")
                ),
                "dead_letter_id": maybe_text(item.get("dead_letter_id")),
            }
            for item in receipt_conflict_events[-5:]
        ],
        "open_dead_letters": open_dead_letters[:10],
    }


def materialize_runtime_health(run_dir: Path, *, round_id: str = "") -> dict[str, Any]:
    payload = runtime_health_payload(run_dir, round_id=round_id)
    write_json(runtime_health_path(run_dir), payload)
    return payload


def refresh_runtime_surfaces(run_dir: Path, *, round_id: str = "") -> dict[str, Any]:
    from eco_council_runtime.kernel.operator.runbook import materialize_operator_runbook
    health = materialize_runtime_health(run_dir)
    runbook_path = materialize_operator_runbook(run_dir, round_id=round_id)
    return {
        "runtime_health_path": str(runtime_health_path(run_dir).resolve()),
        "runtime_health": health,
        "operator_runbook_path": runbook_path,
    }


__all__ = (
    "runtime_health_payload",
    "materialize_runtime_health",
    "refresh_runtime_surfaces",
)
