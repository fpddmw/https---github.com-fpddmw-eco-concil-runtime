"""Helpers for packet editor prompts and draft promotion flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

JsonLoader = Callable[[Path], Any | None]
JsonWriter = Callable[[Path, Any, bool], None]
PayloadValidator = Callable[[str, Any], None]
ReplacementPolicy = Callable[[dict[str, Any] | None, dict[str, Any]], bool]
PlaceholderPredicate = Callable[[dict[str, Any] | None], bool]


def _normalize_space(value: str) -> str:
    return " ".join(str(value).split())


def _maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return _normalize_space(str(value))


def load_required_object(path: Path, *, label: str, load_json_if_exists: JsonLoader) -> dict[str, Any]:
    payload = load_json_if_exists(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} is missing or not a JSON object: {path}")
    return payload


def load_draft_payload(
    *,
    draft_path_text: str,
    default_path: Path,
    label: str,
    round_error_label: str,
    expected_round_id: str,
    expected_role: str | None,
    role_error_label: str | None,
    kind: str,
    load_json_if_exists: JsonLoader,
    validate_payload: PayloadValidator,
) -> tuple[Path, dict[str, Any]]:
    draft_path = Path(draft_path_text).expanduser().resolve() if draft_path_text else default_path
    payload = load_required_object(draft_path, label=label, load_json_if_exists=load_json_if_exists)
    if _maybe_text(payload.get("round_id")) != expected_round_id:
        raise ValueError(f"{round_error_label} round mismatch: expected {expected_round_id}, got {payload.get('round_id')!r}")
    if expected_role is not None and _maybe_text(payload.get("agent_role")) != expected_role:
        role_label = role_error_label or round_error_label
        raise ValueError(f"{role_label} role mismatch: expected {expected_role}, got {payload.get('agent_role')!r}")
    validate_payload(kind, payload)
    return draft_path, payload


def can_replace_existing_report(
    existing_payload: dict[str, Any] | None,
    new_payload: dict[str, Any],
    *,
    report_is_placeholder: PlaceholderPredicate,
) -> bool:
    if existing_payload is None:
        return True
    if existing_payload == new_payload:
        return True
    return report_is_placeholder(existing_payload)


def can_replace_existing_exact(existing_payload: dict[str, Any] | None, new_payload: dict[str, Any]) -> bool:
    if existing_payload is None:
        return True
    return existing_payload == new_payload


def promote_draft(
    *,
    draft_path: Path,
    payload: dict[str, Any],
    target_path: Path,
    pretty: bool,
    allow_overwrite: bool,
    existing_label: str,
    overwrite_error_message: str,
    can_replace_existing: ReplacementPolicy,
    load_json_if_exists: JsonLoader,
    write_json: JsonWriter,
) -> dict[str, Any]:
    existing_payload = load_json_if_exists(target_path)
    if existing_payload is not None and not isinstance(existing_payload, dict):
        raise ValueError(f"Existing {existing_label} is not a JSON object: {target_path}")
    if not allow_overwrite and not can_replace_existing(existing_payload, payload):
        raise ValueError(f"{overwrite_error_message}: {target_path}")
    write_json(target_path, payload, pretty)
    return {
        "draft_path": str(draft_path),
        "target_path": str(target_path),
        "overwrote_existing": existing_payload is not None and existing_payload != payload,
    }


def report_prompt_text(*, role: str, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the {role} for eco-council run {_maybe_text(run.get('run_id'))} round {_maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `task_scope`, `context` (especially `context.causal_focus`), `investigation_plan`, and `investigation_review` before editing.",
        "3. Start from `draft_report` inside the packet.",
        "4. Return only one JSON object shaped like expert-report.",
        "5. Keep `schema_version`, `run_id`, `round_id`, and `agent_role` consistent with the packet.",
        "6. `recommended_next_actions` must be a list of objects with `assigned_role`, `objective`, and `reason`; do not emit strings there.",
        "7. Keep `override_requests` as [] unless the current mission envelope itself is insufficient.",
        "8. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        _maybe_text(validation.get("draft_report_path")),
        "",
        "Validation command:",
        _maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


def decision_prompt_text(*, packet_path: Path, packet: dict[str, Any]) -> str:
    run = packet.get("run", {}) if isinstance(packet.get("run"), dict) else {}
    validation = packet.get("validation", {}) if isinstance(packet.get("validation"), dict) else {}
    lines = [
        "Use the eco-council runtime reporting packet and validation commands below.",
        f"You are the moderator for eco-council run {_maybe_text(run.get('run_id'))} round {_maybe_text(run.get('round_id'))}.",
        "",
        "Open and read this packet JSON first:",
        str(packet_path),
        "",
        "Then follow these rules:",
        "1. Treat packet `instructions` as binding.",
        "2. Review `round_context` (especially `round_context.causal_focus`), `investigation_plan`, `investigation_review`, `readiness_reports`, `matching_authorization`, `matching_adjudication`, `matching_result`, `evidence_adjudication`, `reports`, and `proposed_next_round_tasks` before editing.",
        "3. Start from `draft_decision` inside the packet.",
        "4. If another round is needed, make sure each task adds at least one new evidence requirement or materially different claim focus; leave concrete source-family and layer choice to the expert source-selection stage.",
        "5. Return only one JSON object shaped like council-decision.",
        "6. Keep `schema_version`, `run_id`, and `round_id` consistent with the packet.",
        "7. Use `override_requests` only for upstream mission-envelope changes such as max_rounds or max_tasks_per_round; do not self-apply them.",
        "8. Do not return markdown, prose, code fences, or extra commentary.",
        "",
        "If you persist the result locally, write it to:",
        _maybe_text(validation.get("draft_decision_path")),
        "",
        "Validation command:",
        _maybe_text(validation.get("validate_command")),
        "",
        "Return only JSON.",
        "",
    ]
    return "\n".join(lines)


__all__ = [
    "can_replace_existing_exact",
    "can_replace_existing_report",
    "decision_prompt_text",
    "load_draft_payload",
    "load_required_object",
    "promote_draft",
    "report_prompt_text",
]
