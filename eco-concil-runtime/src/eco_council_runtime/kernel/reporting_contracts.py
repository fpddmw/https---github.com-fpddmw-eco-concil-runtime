from __future__ import annotations

from typing import Any

from .investigation_planning import (  # noqa: F401
    d1_contract_fields,
    maybe_text,
    normalize_d1_observed_inputs,
)

EXPLICIT_REPORTING_INPUT_NAMES = (
    "readiness",
    "promotion",
    "supervisor_state",
    "reporting_handoff",
    "decision",
    "expert_report_draft",
    "sociologist_report",
    "environmentalist_report",
)

MERGED_TEXT_FIELDS = (
    "board_state_source",
    "coverage_source",
    "db_path",
    "readiness_source",
    "board_brief_source",
    "next_actions_source",
    "promotion_source",
    "supervisor_state_source",
    "reporting_handoff_source",
    "decision_source",
    "expert_report_draft_source",
    "sociologist_report_source",
    "environmentalist_report_source",
)


def normalize_reporting_observed_inputs(
    observed_inputs: dict[str, Any] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    source: dict[str, Any] = {}
    if isinstance(observed_inputs, dict):
        source.update(observed_inputs)
    source.update(overrides)
    normalized = normalize_d1_observed_inputs(source)
    for input_name in EXPLICIT_REPORTING_INPUT_NAMES:
        artifact_key = f"{input_name}_artifact_present"
        present_key = f"{input_name}_present"
        if artifact_key not in source and present_key not in source:
            continue
        normalized[artifact_key] = bool(
            source.get(artifact_key, source.get(present_key, False))
        )
        normalized[present_key] = bool(source.get(present_key, False))
    return normalized


def _merged_text_field(
    primary: dict[str, Any] | None,
    fallback: dict[str, Any] | None,
    field_name: str,
) -> str:
    primary_text = maybe_text(primary.get(field_name)) if isinstance(primary, dict) else ""
    if primary_text:
        return primary_text
    return maybe_text(fallback.get(field_name)) if isinstance(fallback, dict) else ""


def _merged_sync_field(
    primary: dict[str, Any] | None,
    fallback: dict[str, Any] | None,
    field_name: str,
) -> dict[str, Any]:
    if isinstance(primary, dict) and isinstance(primary.get(field_name), dict):
        return primary[field_name]
    if isinstance(fallback, dict) and isinstance(fallback.get(field_name), dict):
        return fallback[field_name]
    return {}


def _merged_observed_inputs(
    primary: dict[str, Any] | None,
    fallback: dict[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if isinstance(fallback, dict) and isinstance(fallback.get("observed_inputs"), dict):
        merged.update(fallback["observed_inputs"])
    if isinstance(primary, dict) and isinstance(primary.get("observed_inputs"), dict):
        merged.update(primary["observed_inputs"])
    return merged


def reporting_contract_fields(
    *,
    board_state_source: Any = "",
    coverage_source: Any = "",
    db_path: Any = "",
    deliberation_sync: dict[str, Any] | None = None,
    analysis_sync: dict[str, Any] | None = None,
    readiness_source: Any = "",
    board_brief_source: Any = "",
    next_actions_source: Any = "",
    promotion_source: Any = "",
    supervisor_state_source: Any = "",
    reporting_handoff_source: Any = "",
    decision_source: Any = "",
    expert_report_draft_source: Any = "",
    sociologist_report_source: Any = "",
    environmentalist_report_source: Any = "",
    observed_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = d1_contract_fields(
        board_state_source=board_state_source,
        coverage_source=coverage_source,
        db_path=db_path,
        deliberation_sync=deliberation_sync,
        analysis_sync=analysis_sync,
        observed_inputs=observed_inputs,
    )
    optional_sources = {
        "readiness_source": readiness_source,
        "board_brief_source": board_brief_source,
        "next_actions_source": next_actions_source,
        "promotion_source": promotion_source,
        "supervisor_state_source": supervisor_state_source,
        "reporting_handoff_source": reporting_handoff_source,
        "decision_source": decision_source,
        "expert_report_draft_source": expert_report_draft_source,
        "sociologist_report_source": sociologist_report_source,
        "environmentalist_report_source": environmentalist_report_source,
    }
    for field_name, value in optional_sources.items():
        text = maybe_text(value)
        if text:
            payload[field_name] = text
    payload["observed_inputs"] = normalize_reporting_observed_inputs(observed_inputs)
    return payload


def reporting_contract_fields_from_payload(
    payload: dict[str, Any] | None,
    *,
    fallback_payload: dict[str, Any] | None = None,
    observed_inputs_overrides: dict[str, Any] | None = None,
    field_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    primary = payload if isinstance(payload, dict) else {}
    fallback = fallback_payload if isinstance(fallback_payload, dict) else {}
    observed_inputs = _merged_observed_inputs(primary, fallback)
    if isinstance(observed_inputs_overrides, dict):
        observed_inputs = {**observed_inputs, **observed_inputs_overrides}
    merged_fields: dict[str, Any] = {
        field_name: _merged_text_field(primary, fallback, field_name)
        for field_name in MERGED_TEXT_FIELDS
    }
    merged_fields["deliberation_sync"] = _merged_sync_field(
        primary,
        fallback,
        "deliberation_sync",
    )
    merged_fields["analysis_sync"] = _merged_sync_field(
        primary,
        fallback,
        "analysis_sync",
    )
    if isinstance(field_overrides, dict):
        merged_fields.update(field_overrides)
    return reporting_contract_fields(
        board_state_source=merged_fields.get("board_state_source"),
        coverage_source=merged_fields.get("coverage_source"),
        db_path=merged_fields.get("db_path"),
        deliberation_sync=merged_fields.get("deliberation_sync"),
        analysis_sync=merged_fields.get("analysis_sync"),
        readiness_source=merged_fields.get("readiness_source"),
        board_brief_source=merged_fields.get("board_brief_source"),
        next_actions_source=merged_fields.get("next_actions_source"),
        promotion_source=merged_fields.get("promotion_source"),
        supervisor_state_source=merged_fields.get("supervisor_state_source"),
        reporting_handoff_source=merged_fields.get("reporting_handoff_source"),
        decision_source=merged_fields.get("decision_source"),
        expert_report_draft_source=merged_fields.get("expert_report_draft_source"),
        sociologist_report_source=merged_fields.get("sociologist_report_source"),
        environmentalist_report_source=merged_fields.get(
            "environmentalist_report_source"
        ),
        observed_inputs=observed_inputs,
    )
