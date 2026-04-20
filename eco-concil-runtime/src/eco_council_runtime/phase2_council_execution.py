from __future__ import annotations

from typing import Any

from .phase2_fallback_common import maybe_text
from .phase2_proposal_actions import action_signature

COUNCIL_EXECUTION_MODE_PROPOSAL_AUTHORITATIVE = "proposal-authoritative"
COUNCIL_EXECUTION_MODE_PROPOSAL_AUGMENTED = "proposal-augmented"
COUNCIL_EXECUTION_MODE_FALLBACK_ONLY = "fallback-only"
VALID_COUNCIL_EXECUTION_MODES = {
    COUNCIL_EXECUTION_MODE_PROPOSAL_AUTHORITATIVE,
    COUNCIL_EXECUTION_MODE_PROPOSAL_AUGMENTED,
    COUNCIL_EXECUTION_MODE_FALLBACK_ONLY,
}


def normalize_council_execution_mode(value: Any) -> str:
    normalized = maybe_text(value)
    if normalized in VALID_COUNCIL_EXECUTION_MODES:
        return normalized
    return COUNCIL_EXECUTION_MODE_PROPOSAL_AUTHORITATIVE


def council_execution_uses_fallback_only(council_execution_mode: Any) -> bool:
    return (
        normalize_council_execution_mode(council_execution_mode)
        == COUNCIL_EXECUTION_MODE_FALLBACK_ONLY
    )


def council_inputs_present(
    *,
    proposal_actions: list[dict[str, Any]] | None = None,
    readiness_opinions: list[dict[str, Any]] | None = None,
) -> bool:
    return bool(
        [
            item
            for item in (proposal_actions or [])
            if isinstance(item, dict)
        ]
        or [
            item
            for item in (readiness_opinions or [])
            if isinstance(item, dict)
        ]
    )


def _normalized_actions(actions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [dict(action) for action in (actions or []) if isinstance(action, dict)]


def _collect_actions_with_origin(
    action_groups: list[tuple[str, list[dict[str, Any]]]],
    *,
    max_actions: int | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    merged: list[dict[str, Any]] = []
    origin_counts: dict[str, int] = {}
    seen_signatures: set[str] = set()
    limit = max_actions if isinstance(max_actions, int) and max_actions > 0 else None
    for origin, actions in action_groups:
        for action in actions:
            signature = action_signature(action)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            merged.append(dict(action))
            origin_counts[origin] = origin_counts.get(origin, 0) + 1
            if limit is not None and len(merged) >= limit:
                return merged, origin_counts
    return merged, origin_counts


def deduped_actions(actions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    merged, _ = _collect_actions_with_origin(
        [("selected", _normalized_actions(actions))],
        max_actions=None,
    )
    return merged


def deduped_action_count(actions: list[dict[str, Any]] | None) -> int:
    return len(deduped_actions(actions))


def resolve_council_action_queue(
    proposal_actions: list[dict[str, Any]] | None,
    fallback_actions: list[dict[str, Any]] | None,
    *,
    council_execution_mode: Any,
    max_actions: int | None = None,
) -> dict[str, Any]:
    mode = normalize_council_execution_mode(council_execution_mode)
    normalized_proposals = _normalized_actions(proposal_actions)
    normalized_fallback = _normalized_actions(fallback_actions)
    observed_proposal_count = deduped_action_count(normalized_proposals)
    observed_fallback_count = deduped_action_count(normalized_fallback)

    if mode == COUNCIL_EXECUTION_MODE_FALLBACK_ONLY:
        selected_actions, included_counts = _collect_actions_with_origin(
            [("fallback", normalized_fallback)],
            max_actions=max_actions,
        )
        resolution = COUNCIL_EXECUTION_MODE_FALLBACK_ONLY
    elif not normalized_proposals:
        selected_actions, included_counts = _collect_actions_with_origin(
            [("fallback", normalized_fallback)],
            max_actions=max_actions,
        )
        resolution = "fallback-when-no-proposal"
    elif mode == COUNCIL_EXECUTION_MODE_PROPOSAL_AUGMENTED:
        selected_actions, included_counts = _collect_actions_with_origin(
            [
                ("proposal", normalized_proposals),
                ("fallback", normalized_fallback),
            ],
            max_actions=max_actions,
        )
        resolution = COUNCIL_EXECUTION_MODE_PROPOSAL_AUGMENTED
    else:
        selected_actions, included_counts = _collect_actions_with_origin(
            [("proposal", normalized_proposals)],
            max_actions=max_actions,
        )
        resolution = COUNCIL_EXECUTION_MODE_PROPOSAL_AUTHORITATIVE

    included_proposal_count = int(included_counts.get("proposal") or 0)
    included_fallback_count = int(included_counts.get("fallback") or 0)
    return {
        "council_execution_mode": mode,
        "resolution": resolution,
        "proposal_authority_active": bool(normalized_proposals)
        and mode != COUNCIL_EXECUTION_MODE_FALLBACK_ONLY,
        "proposal_present": bool(normalized_proposals),
        "selected_actions": selected_actions,
        "observed_proposal_action_count": observed_proposal_count,
        "observed_fallback_action_count": observed_fallback_count,
        "included_proposal_action_count": included_proposal_count,
        "included_fallback_action_count": included_fallback_count,
        "suppressed_fallback_action_count": max(
            0,
            observed_fallback_count - included_fallback_count,
        ),
    }


def council_execution_uses_direct_queue(
    council_execution_mode: Any,
    *,
    proposal_actions: list[dict[str, Any]] | None = None,
    readiness_opinions: list[dict[str, Any]] | None = None,
) -> bool:
    if council_execution_uses_fallback_only(council_execution_mode):
        return False
    return council_inputs_present(
        proposal_actions=proposal_actions,
        readiness_opinions=readiness_opinions,
    )


__all__ = [
    "COUNCIL_EXECUTION_MODE_FALLBACK_ONLY",
    "COUNCIL_EXECUTION_MODE_PROPOSAL_AUGMENTED",
    "COUNCIL_EXECUTION_MODE_PROPOSAL_AUTHORITATIVE",
    "VALID_COUNCIL_EXECUTION_MODES",
    "council_execution_uses_direct_queue",
    "council_execution_uses_fallback_only",
    "council_inputs_present",
    "deduped_action_count",
    "deduped_actions",
    "normalize_council_execution_mode",
    "resolve_council_action_queue",
]
