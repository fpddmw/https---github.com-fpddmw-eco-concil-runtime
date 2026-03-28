"""Application-level normalize view and summary builders."""

from __future__ import annotations

import copy
from collections import Counter
from typing import Any, Callable

from eco_council_runtime.adapters.filesystem import utc_now_iso
from eco_council_runtime.domain.normalize_semantics import (
    claim_priority_metric_families,
    compact_claim_scope,
    maybe_number,
    observation_metric_family,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings

IdEmitter = Callable[[str, int], str]
PayloadValidator = Callable[[str, dict[str, Any]], Any]


def _default_emit_row_id(prefix: str, index: int) -> str:
    return f"{prefix}-{index:03d}"


def _noop_validate_payload(_kind: str, _payload: dict[str, Any]) -> None:
    return None


def maybe_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if not value.is_integer():
            return None
        return int(value) if value >= 0 else None
    if isinstance(value, str) and value.strip():
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def compact_count_items(value: Any, *, limit: int = 6) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("value"))
        count = maybe_nonnegative_int(item.get("count"))
        if not label or count is None or count <= 0:
            continue
        items.append({"value": label, "count": count})
        if len(items) >= limit:
            break
    return items


def compact_distribution_summary(distribution_obj: Any, *, limit: int = 6) -> dict[str, Any] | None:
    if not isinstance(distribution_obj, dict):
        return None
    compacted: dict[str, Any] = {}
    signal_count = maybe_nonnegative_int(distribution_obj.get("signal_count"))
    if signal_count is not None:
        compacted["signal_count"] = signal_count
    for field_name in ("distinct_day_count", "distinct_source_skill_count", "distinct_point_count"):
        count = maybe_nonnegative_int(distribution_obj.get(field_name))
        if count is not None:
            compacted[field_name] = count
    for field_name in ("time_bucket_counts", "source_skill_counts", "metric_counts", "point_bucket_counts"):
        items = compact_count_items(distribution_obj.get(field_name), limit=limit)
        if items:
            compacted[field_name] = items
    return compacted or None


def compact_statistics(statistics_obj: Any) -> dict[str, Any] | None:
    if not isinstance(statistics_obj, dict):
        return None
    compacted: dict[str, Any] = {}
    sample_count = maybe_nonnegative_int(statistics_obj.get("sample_count"))
    if sample_count is not None:
        compacted["sample_count"] = sample_count
    for key in ("min", "p05", "p25", "mean", "median", "p75", "p95", "max", "stddev"):
        value = maybe_number(statistics_obj.get(key))
        compacted[key] = round(value, 3) if value is not None else None
    if all(value is None for key, value in compacted.items() if key != "sample_count") and "sample_count" not in compacted:
        return None
    return compacted


def compact_task(task: dict[str, Any]) -> dict[str, Any]:
    inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
    evidence_requirements = inputs.get("evidence_requirements") if isinstance(inputs.get("evidence_requirements"), list) else []
    return {
        "task_id": maybe_text(task.get("task_id")),
        "assigned_role": maybe_text(task.get("assigned_role")),
        "objective": truncate_text(maybe_text(task.get("objective")), 180),
        "status": maybe_text(task.get("status")),
        "evidence_requirements": [
            maybe_text(item.get("requirement_type"))
            for item in evidence_requirements
            if isinstance(item, dict) and maybe_text(item.get("requirement_type"))
        ][:3],
    }


def claim_source_skills(claim: dict[str, Any]) -> list[str]:
    refs = claim.get("public_refs")
    if not isinstance(refs, list):
        return []
    return sorted(
        {
            maybe_text(ref.get("source_skill"))
            for ref in refs
            if isinstance(ref, dict) and maybe_text(ref.get("source_skill"))
        }
    )


def compact_claim(claim: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "claim_id": maybe_text(claim.get("claim_id")),
        "claim_type": maybe_text(claim.get("claim_type")),
        "summary": truncate_text(maybe_text(claim.get("summary")), 180),
        "priority": claim.get("priority"),
        "needs_physical_validation": bool(claim.get("needs_physical_validation")),
        "public_source_skills": claim_source_skills(claim),
    }
    hypothesis_id = maybe_text(claim.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(claim.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    compact_scope = compact_claim_scope(claim.get("claim_scope"))
    if compact_scope:
        payload["claim_scope"] = compact_scope
    return payload


def compact_observation(observation: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "observation_id": maybe_text(observation.get("observation_id")),
        "source_skill": maybe_text(observation.get("source_skill")),
        "metric": maybe_text(observation.get("metric")),
        "metric_family": observation_metric_family(observation.get("metric")),
        "aggregation": maybe_text(observation.get("aggregation")),
        "observation_mode": maybe_text(observation.get("observation_mode")),
        "evidence_role": maybe_text(observation.get("evidence_role")),
        "value": observation.get("value"),
        "unit": maybe_text(observation.get("unit")),
        "statistics": compact_statistics(observation.get("statistics")),
        "distribution_summary": compact_distribution_summary(observation.get("distribution_summary")),
        "time_window": observation.get("time_window"),
        "source_skills": [maybe_text(item) for item in observation.get("source_skills", []) if maybe_text(item)][:4],
        "metric_bundle": [maybe_text(item) for item in observation.get("metric_bundle", []) if maybe_text(item)][:6],
        "quality_flags": [maybe_text(item) for item in observation.get("quality_flags", []) if maybe_text(item)][:4],
    }
    hypothesis_id = maybe_text(observation.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(observation.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    return payload


def compact_evidence_card(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "evidence_id": maybe_text(card.get("evidence_id")),
        "claim_id": maybe_text(card.get("claim_id")),
        "verdict": maybe_text(card.get("verdict")),
        "confidence": maybe_text(card.get("confidence")),
        "summary": truncate_text(maybe_text(card.get("summary")), 220),
        "observation_ids": [maybe_text(item) for item in card.get("observation_ids", []) if maybe_text(item)][:6],
        "gaps": [truncate_text(maybe_text(item), 120) for item in card.get("gaps", []) if maybe_text(item)][:3],
    }


def compact_claim_submission(submission: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "submission_id": maybe_text(submission.get("submission_id")),
        "claim_id": maybe_text(submission.get("claim_id")),
        "claim_type": maybe_text(submission.get("claim_type")),
        "summary": truncate_text(maybe_text(submission.get("summary")), 180),
        "meaning": truncate_text(maybe_text(submission.get("meaning")), 200),
        "worth_storing": bool(submission.get("worth_storing")),
        "source_signal_count": submission.get("source_signal_count"),
        "candidate_claim_ids": [maybe_text(item) for item in submission.get("candidate_claim_ids", []) if maybe_text(item)][:6],
        "selection_reason": truncate_text(maybe_text(submission.get("selection_reason")), 160),
    }
    hypothesis_id = maybe_text(submission.get("hypothesis_id"))
    if hypothesis_id:
        payload["hypothesis_id"] = hypothesis_id
    leg_id = maybe_text(submission.get("leg_id"))
    if leg_id:
        payload["leg_id"] = leg_id
    compact_scope = compact_claim_scope(submission.get("claim_scope"))
    if compact_scope:
        payload["claim_scope"] = compact_scope
    return payload


def compact_observation_submission(submission: dict[str, Any]) -> dict[str, Any]:
    return {
        "submission_id": maybe_text(submission.get("submission_id")),
        "observation_id": maybe_text(submission.get("observation_id")),
        "metric": maybe_text(submission.get("metric")),
        "metric_family": observation_metric_family(submission.get("metric")),
        "source_skill": maybe_text(submission.get("source_skill")),
        "aggregation": maybe_text(submission.get("aggregation")),
        "observation_mode": maybe_text(submission.get("observation_mode")),
        "evidence_role": maybe_text(submission.get("evidence_role")),
        "value": submission.get("value"),
        "unit": maybe_text(submission.get("unit")),
        "statistics": compact_statistics(submission.get("statistics")),
        "distribution_summary": compact_distribution_summary(submission.get("distribution_summary")),
        "time_window": submission.get("time_window"),
        "meaning": truncate_text(maybe_text(submission.get("meaning")), 200),
        "worth_storing": bool(submission.get("worth_storing")),
        "source_skills": [maybe_text(item) for item in submission.get("source_skills", []) if maybe_text(item)][:4],
        "metric_bundle": [maybe_text(item) for item in submission.get("metric_bundle", []) if maybe_text(item)][:6],
        "candidate_observation_ids": [
            maybe_text(item) for item in submission.get("candidate_observation_ids", []) if maybe_text(item)
        ][:6],
        "selection_reason": truncate_text(maybe_text(submission.get("selection_reason")), 160),
    }


def compact_isolated_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "isolated_id": maybe_text(item.get("isolated_id")),
        "entity_kind": maybe_text(item.get("entity_kind")),
        "entity_id": maybe_text(item.get("entity_id")),
        "summary": truncate_text(maybe_text(item.get("summary")), 200),
        "reason": truncate_text(maybe_text(item.get("reason")), 160),
    }


def compact_remand_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "remand_id": maybe_text(item.get("remand_id")),
        "entity_kind": maybe_text(item.get("entity_kind")),
        "entity_id": maybe_text(item.get("entity_id")),
        "summary": truncate_text(maybe_text(item.get("summary")), 200),
        "reasons": [truncate_text(maybe_text(reason), 120) for reason in item.get("reasons", []) if maybe_text(reason)][:3],
    }


def observation_severity(observation: dict[str, Any]) -> float:
    statistics_obj = compact_statistics(observation.get("statistics"))
    if isinstance(statistics_obj, dict):
        for key in ("max", "p95", "mean", "min"):
            value = maybe_number(statistics_obj.get(key))
            if value is not None:
                return value
    value = maybe_number(observation.get("value"))
    return value if value is not None else 0.0


def observation_family_priority_index(observation: dict[str, Any], family_order: list[str]) -> int:
    family = observation_metric_family(observation.get("metric"))
    if family in family_order:
        return family_order.index(family)
    return len(family_order)


def representative_observation_order(observations: list[dict[str, Any]], claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    family_order = claim_priority_metric_families(claims)
    candidates = sorted(
        observations,
        key=lambda item: (
            observation_family_priority_index(item, family_order),
            -observation_severity(item),
            maybe_text(item.get("source_skill")),
            maybe_text(item.get("metric")),
            maybe_text(item.get("observation_id")),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def take_first(predicate: Any) -> None:
        for candidate in candidates:
            observation_id = maybe_text(candidate.get("observation_id"))
            if not observation_id or observation_id in seen_ids or not predicate(candidate):
                continue
            selected.append(candidate)
            seen_ids.add(observation_id)
            return

    for family in family_order:
        take_first(lambda item, family=family: observation_metric_family(item.get("metric")) == family)
    for source_skill in unique_strings([maybe_text(item.get("source_skill")) for item in candidates]):
        take_first(lambda item, source_skill=source_skill: maybe_text(item.get("source_skill")) == source_skill)
    for metric in unique_strings([maybe_text(item.get("metric")) for item in candidates]):
        take_first(lambda item, metric=metric: maybe_text(item.get("metric")) == metric)
    for candidate in candidates:
        observation_id = maybe_text(candidate.get("observation_id"))
        if not observation_id or observation_id in seen_ids:
            continue
        selected.append(candidate)
        seen_ids.add(observation_id)
    return selected


def ordered_context_observations(
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    claims: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    by_id = {maybe_text(item.get("observation_id")): item for item in observations}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for card in evidence_cards:
        ids = card.get("observation_ids")
        if not isinstance(ids, list):
            continue
        for observation_id in ids:
            key = maybe_text(observation_id)
            if not key or key in seen or key not in by_id:
                continue
            ordered.append(by_id[key])
            seen.add(key)
    remaining = [
        observation
        for observation in representative_observation_order(observations, claims or [])
        if maybe_text(observation.get("observation_id")) not in seen
    ]
    for observation in remaining:
        key = maybe_text(observation.get("observation_id"))
        if not key or key in seen:
            continue
        ordered.append(observation)
        seen.add(key)
    return ordered


def representative_claim_submissions(submissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        submissions,
        key=lambda item: (
            -int(item.get("source_signal_count") or 0),
            maybe_text(item.get("claim_type")),
            maybe_text(item.get("submission_id")),
        ),
    )


def claims_from_submissions(
    submissions: list[dict[str, Any]],
    *,
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> list[dict[str, Any]]:
    emit = emit_row_id or _default_emit_row_id
    validate = validate_payload or _noop_validate_payload
    claims: list[dict[str, Any]] = []
    for index, submission in enumerate(submissions, start=1):
        claim = {
            "schema_version": schema_version,
            "claim_id": maybe_text(submission.get("claim_id")) or emit("claim", index),
            "run_id": maybe_text(submission.get("run_id")),
            "round_id": maybe_text(submission.get("round_id")),
            "agent_role": "sociologist",
            "claim_type": maybe_text(submission.get("claim_type")),
            "status": "selected",
            "summary": maybe_text(submission.get("summary")),
            "statement": maybe_text(submission.get("statement")),
            "priority": int(submission.get("priority") or 1),
            "needs_physical_validation": bool(submission.get("needs_physical_validation")),
            "time_window": copy.deepcopy(submission.get("time_window")) if isinstance(submission.get("time_window"), dict) else submission.get("time_window"),
            "place_scope": copy.deepcopy(submission.get("place_scope")) if isinstance(submission.get("place_scope"), dict) else submission.get("place_scope"),
            "public_refs": submission.get("public_refs", []),
        }
        if isinstance(submission.get("claim_scope"), dict):
            claim["claim_scope"] = copy.deepcopy(submission.get("claim_scope"))
        hypothesis_id = maybe_text(submission.get("hypothesis_id"))
        if hypothesis_id:
            claim["hypothesis_id"] = hypothesis_id
        leg_id = maybe_text(submission.get("leg_id"))
        if leg_id:
            claim["leg_id"] = leg_id
        candidate_claim_ids = submission.get("candidate_claim_ids")
        if isinstance(candidate_claim_ids, list) and candidate_claim_ids:
            claim["candidate_claim_ids"] = [maybe_text(item) for item in candidate_claim_ids if maybe_text(item)]
        selection_reason = maybe_text(submission.get("selection_reason"))
        if selection_reason:
            claim["selection_reason"] = selection_reason
        validate("claim", claim)
        claims.append(claim)
    return claims


def observations_from_submissions(
    submissions: list[dict[str, Any]],
    *,
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> list[dict[str, Any]]:
    emit = emit_row_id or _default_emit_row_id
    validate = validate_payload or _noop_validate_payload
    observations: list[dict[str, Any]] = []
    for index, submission in enumerate(submissions, start=1):
        observation = {
            "schema_version": schema_version,
            "observation_id": maybe_text(submission.get("observation_id")) or emit("obs", index),
            "run_id": maybe_text(submission.get("run_id")),
            "round_id": maybe_text(submission.get("round_id")),
            "agent_role": "environmentalist",
            "source_skill": maybe_text(submission.get("source_skill")),
            "metric": maybe_text(submission.get("metric")),
            "aggregation": maybe_text(submission.get("aggregation")),
            "value": submission.get("value"),
            "unit": maybe_text(submission.get("unit")),
            "statistics": submission.get("statistics"),
            "time_window": submission.get("time_window"),
            "place_scope": submission.get("place_scope"),
            "quality_flags": submission.get("quality_flags", []),
            "provenance": submission.get("provenance"),
        }
        for field_name in (
            "observation_mode",
            "evidence_role",
            "hypothesis_id",
            "leg_id",
            "source_skills",
            "metric_bundle",
            "candidate_observation_ids",
            "provenance_refs",
            "component_roles",
            "distribution_summary",
        ):
            value = submission.get(field_name)
            if value is not None:
                observation[field_name] = value
        selection_reason = maybe_text(submission.get("selection_reason"))
        if selection_reason:
            observation["selection_reason"] = selection_reason
        validate("observation", observation)
        observations.append(observation)
    return observations


def representative_observation_submissions(
    submissions: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    *,
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> list[dict[str, Any]]:
    observations = observations_from_submissions(
        submissions,
        schema_version=schema_version,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )
    ordered_observations = representative_observation_order(observations, claims)
    by_id = {maybe_text(item.get("observation_id")): item for item in submissions}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for observation in ordered_observations:
        observation_id = maybe_text(observation.get("observation_id"))
        submission = by_id.get(observation_id)
        if observation_id and observation_id not in seen and submission is not None:
            ordered.append(submission)
            seen.add(observation_id)
    for submission in submissions:
        observation_id = maybe_text(submission.get("observation_id"))
        if not observation_id or observation_id in seen:
            continue
        ordered.append(submission)
        seen.add(observation_id)
    return ordered


def build_public_signal_summary(
    signals: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    *,
    generated_at_utc: str | None = None,
    max_context_claims: int = 4,
) -> dict[str, Any]:
    return {
        "generated_at_utc": generated_at_utc or utc_now_iso(),
        "signal_count": len(signals),
        "claim_count": len(claims),
        "source_skill_counts": dict(Counter(maybe_text(item.get("source_skill")) for item in signals)),
        "signal_kind_counts": dict(Counter(maybe_text(item.get("signal_kind")) for item in signals)),
        "top_signals": [
            {
                "signal_id": maybe_text(item.get("signal_id")),
                "source_skill": maybe_text(item.get("source_skill")),
                "title": truncate_text(maybe_text(item.get("title")), 120),
                "published_at_utc": maybe_text(item.get("published_at_utc")),
            }
            for item in signals[:5]
        ],
        "claims": [compact_claim(item) for item in claims[:max_context_claims]],
    }


def build_environment_signal_summary(
    signals: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    *,
    generated_at_utc: str | None = None,
    max_context_observations: int = 8,
) -> dict[str, Any]:
    return {
        "generated_at_utc": generated_at_utc or utc_now_iso(),
        "signal_count": len(signals),
        "observation_count": len(observations),
        "source_skill_counts": dict(Counter(maybe_text(item.get("source_skill")) for item in signals)),
        "metric_counts": dict(Counter(maybe_text(item.get("metric")) for item in signals)),
        "top_observations": [
            compact_observation(item)
            for item in representative_observation_order(observations, [])[:max_context_observations]
        ],
    }


__all__ = [
    "build_environment_signal_summary",
    "build_public_signal_summary",
    "claim_source_skills",
    "claims_from_submissions",
    "compact_claim",
    "compact_claim_submission",
    "compact_count_items",
    "compact_distribution_summary",
    "compact_evidence_card",
    "compact_isolated_entry",
    "compact_observation",
    "compact_observation_submission",
    "compact_remand_entry",
    "compact_statistics",
    "compact_task",
    "maybe_nonnegative_int",
    "observation_family_priority_index",
    "observation_severity",
    "observations_from_submissions",
    "ordered_context_observations",
    "representative_claim_submissions",
    "representative_observation_order",
    "representative_observation_submissions",
]
