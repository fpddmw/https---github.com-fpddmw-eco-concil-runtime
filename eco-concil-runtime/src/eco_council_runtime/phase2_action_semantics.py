from __future__ import annotations

from typing import Any

from .phase2_fallback_common import maybe_text

LEGACY_NON_BLOCKING_ACTION_KINDS = {
    "prepare-report-basis-freeze",
}


def maybe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    text = maybe_text(value).lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


def action_is_readiness_blocker(
    action: dict[str, Any],
    *,
    default: bool = True,
) -> bool:
    explicit = maybe_bool(action.get("readiness_blocker"))
    if explicit is not None:
        return explicit
    action_kind = maybe_text(action.get("action_kind"))
    if action_kind in LEGACY_NON_BLOCKING_ACTION_KINDS:
        return False
    return default


__all__ = [
    "LEGACY_NON_BLOCKING_ACTION_KINDS",
    "action_is_readiness_blocker",
    "maybe_bool",
]
