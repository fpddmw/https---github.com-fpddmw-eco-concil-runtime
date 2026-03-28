"""Recommendation-selection helpers for reporting workflows."""

from __future__ import annotations

from typing import Any

from eco_council_runtime.domain.text import maybe_text
from eco_council_runtime.planning import combine_recommendations, recommendation_key


def _normalized_recommendation(recommendation: dict[str, Any]) -> dict[str, Any] | None:
    normalized = {
        "assigned_role": maybe_text(recommendation.get("assigned_role")),
        "objective": maybe_text(recommendation.get("objective")),
        "reason": maybe_text(recommendation.get("reason")),
    }
    if not all(normalized.values()):
        return None
    return normalized


def prioritized_recommendations_from_state(
    *,
    state: dict[str, Any],
    reports: list[dict[str, Any]],
    missing_types: list[str],
    limit: int | None = 4,
) -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    investigation_actions = state.get("investigation_actions") if isinstance(state.get("investigation_actions"), dict) else {}
    ranked_actions = investigation_actions.get("ranked_actions") if isinstance(investigation_actions.get("ranked_actions"), list) else []
    if ranked_actions:
        from eco_council_runtime.application.investigation.actions import (
            recommendations_from_investigation_actions,
        )

        recommendation_limit = len(ranked_actions)
        if isinstance(limit, int):
            recommendation_limit = max(limit, recommendation_limit)
        combined.extend(
            recommendations_from_investigation_actions(
                investigation_actions,
                limit=recommendation_limit,
            )
        )
    combined.extend(combine_recommendations(reports=reports, missing_types=missing_types))

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for recommendation in combined:
        if not isinstance(recommendation, dict):
            continue
        normalized = _normalized_recommendation(recommendation)
        if normalized is None:
            continue
        key = recommendation_key(normalized)
        if not key[0] or not key[1]:
            continue
        deduped.setdefault(key, normalized)
    recommendations = list(deduped.values())
    if isinstance(limit, int):
        return recommendations[:limit]
    return recommendations


__all__ = ["prioritized_recommendations_from_state"]
