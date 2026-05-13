from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.planes.deliberation_plane import (
    connect_db as connect_deliberation_db,
    maybe_text,
)
from eco_council_runtime.kernel.governance.role_contracts import (
    ROLE_MODERATOR,
    ROLE_RUNTIME_OPERATOR,
    normalize_actor_role,
)

OBJECT_KIND_TRANSITION_REQUEST = "transition-request"
OBJECT_KIND_TRANSITION_APPROVAL = "transition-approval"
OBJECT_KIND_TRANSITION_REJECTION = "transition-rejection"

REQUEST_STATUS_PENDING = "pending-operator-confirmation"
REQUEST_STATUS_APPROVED = "approved"
REQUEST_STATUS_REJECTED = "rejected"
REQUEST_STATUS_COMMITTED = "committed"

DECISION_STATUS_APPROVED = "approved"
DECISION_STATUS_REJECTED = "rejected"

TRANSITION_KIND_OPEN_INVESTIGATION_ROUND = "open-investigation-round"
TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND = "open-report-writing-round"
TRANSITION_KIND_FREEZE_REPORT_BASIS = "freeze-report-basis"
TRANSITION_KIND_CLOSE_ROUND = "close-round"

TRANSITION_KIND_ALIASES = {
    "open-round": TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    "open-investigation-round": TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    "open-follow-up-round": TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    "open-report-writing-round": TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
    "open-reporting-round": TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
    "open-report-round": TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
    "freeze-report-basis": TRANSITION_KIND_FREEZE_REPORT_BASIS,
    "report-basis-freeze": TRANSITION_KIND_FREEZE_REPORT_BASIS,
    "close": TRANSITION_KIND_CLOSE_ROUND,
    "close-round": TRANSITION_KIND_CLOSE_ROUND,
}

TRANSITION_KIND_SPECS: dict[str, dict[str, str]] = {
    TRANSITION_KIND_OPEN_INVESTIGATION_ROUND: {
        "requested_surface": "skill",
        "requested_action": "open-follow-up-round",
        "requested_command_name": "open-investigation-round",
    },
    TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND: {
        "requested_surface": "skill",
        "requested_action": "open-report-writing-round",
        "requested_command_name": "open-report-writing-round",
    },
    TRANSITION_KIND_FREEZE_REPORT_BASIS: {
        "requested_surface": "skill",
        "requested_action": "freeze-report-basis",
        "requested_command_name": "freeze-report-basis",
    },
    TRANSITION_KIND_CLOSE_ROUND: {
        "requested_surface": "kernel-command",
        "requested_action": "archive-close-round",
        "requested_command_name": "close-round",
    },
}



def require_actor_role(
    actor_role: Any,
    *,
    expected_role: str,
    action_name: str,
) -> str:
    raw_role = maybe_text(actor_role)
    resolved_role = normalize_actor_role(raw_role) or raw_role
    if resolved_role != maybe_text(expected_role):
        raise ValueError(
            f"{action_name} requires actor role `{expected_role}`, got `{raw_role or '<empty>'}`."
        )
    return resolved_role


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


def list_dicts(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def stable_hash(*parts: Any) -> str:
    import hashlib

    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def connect_db(run_dir: str | Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    return connect_deliberation_db(resolve_run_dir(run_dir), db_path)


def normalize_transition_kind(transition_kind: Any) -> str:
    text = maybe_text(transition_kind)
    if not text:
        return ""
    return TRANSITION_KIND_ALIASES.get(text, text)


def transition_kind_spec(transition_kind: Any) -> dict[str, str]:
    normalized = normalize_transition_kind(transition_kind)
    spec = TRANSITION_KIND_SPECS.get(normalized)
    if not isinstance(spec, dict):
        supported = ", ".join(sorted(TRANSITION_KIND_SPECS))
        raise ValueError(
            f"Unsupported transition kind: {maybe_text(transition_kind) or '<empty>'}. "
            f"Supported kinds: {supported}."
        )
    return spec


def transition_request_id(
    *,
    run_id: str,
    round_id: str,
    transition_kind: str,
    target_round_id: str,
    created_at_utc: str,
) -> str:
    return (
        "transition-request-"
        + stable_hash(
            "transition-request",
            run_id,
            round_id,
            transition_kind,
            target_round_id,
            created_at_utc,
        )[:12]
    )


def transition_approval_id(
    *,
    request_id: str,
    approved_at_utc: str,
) -> str:
    return "transition-approval-" + stable_hash(
        "transition-approval",
        request_id,
        approved_at_utc,
    )[:12]


def transition_rejection_id(
    *,
    request_id: str,
    rejected_at_utc: str,
) -> str:
    return "transition-rejection-" + stable_hash(
        "transition-rejection",
        request_id,
        rejected_at_utc,
    )[:12]


def request_payload_option(
    request: dict[str, Any],
    key: str,
    default: Any = None,
) -> Any:
    request_payload = (
        request.get("request_payload", {})
        if isinstance(request.get("request_payload"), dict)
        else {}
    )
    return request_payload.get(key, default)


__all__ = (
    "OBJECT_KIND_TRANSITION_REQUEST",
    "OBJECT_KIND_TRANSITION_APPROVAL",
    "OBJECT_KIND_TRANSITION_REJECTION",
    "REQUEST_STATUS_PENDING",
    "REQUEST_STATUS_APPROVED",
    "REQUEST_STATUS_REJECTED",
    "REQUEST_STATUS_COMMITTED",
    "DECISION_STATUS_APPROVED",
    "DECISION_STATUS_REJECTED",
    "TRANSITION_KIND_OPEN_INVESTIGATION_ROUND",
    "TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND",
    "TRANSITION_KIND_FREEZE_REPORT_BASIS",
    "TRANSITION_KIND_CLOSE_ROUND",
    "TRANSITION_KIND_ALIASES",
    "TRANSITION_KIND_SPECS",
    "ROLE_MODERATOR",
    "ROLE_RUNTIME_OPERATOR",
    "maybe_text",
    "normalize_actor_role",
    "require_actor_role",
    "unique_texts",
    "list_dicts",
    "utc_now_iso",
    "stable_hash",
    "json_text",
    "resolve_run_dir",
    "connect_db",
    "normalize_transition_kind",
    "transition_kind_spec",
    "transition_request_id",
    "transition_approval_id",
    "transition_rejection_id",
    "request_payload_option",
)
