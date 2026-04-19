from __future__ import annotations

from typing import Any

from .phase2_fallback_common import (
    ARTIFACT_FALLBACK_PREFIXES,
    EXPLICIT_D1_INPUT_KEYS,
    OBSERVED_INPUT_PREFIXES,
    maybe_text,
)


def normalize_d1_observed_inputs(
    observed_inputs: dict[str, Any] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    source: dict[str, Any] = {}
    if isinstance(observed_inputs, dict):
        source.update(observed_inputs)
    source.update(overrides)
    normalized = {
        key: value for key, value in source.items() if key not in EXPLICIT_D1_INPUT_KEYS
    }
    for prefix in OBSERVED_INPUT_PREFIXES:
        present_key = f"{prefix}_present"
        artifact_key = f"{prefix}_artifact_present"
        present_value = bool(source.get(present_key, False))
        if prefix in ARTIFACT_FALLBACK_PREFIXES:
            artifact_value = bool(source.get(artifact_key, present_value))
        else:
            artifact_value = bool(source.get(artifact_key, False))
        normalized[artifact_key] = artifact_value
        normalized[present_key] = present_value
    return normalized


def d1_contract_fields(
    *,
    board_state_source: Any = "",
    coverage_source: Any = "",
    db_path: Any = "",
    deliberation_sync: dict[str, Any] | None = None,
    analysis_sync: dict[str, Any] | None = None,
    observed_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "board_state_source": maybe_text(board_state_source) or "missing-board",
        "coverage_source": maybe_text(coverage_source) or "missing-coverage",
        "db_path": maybe_text(db_path),
        "deliberation_sync": deliberation_sync if isinstance(deliberation_sync, dict) else {},
        "analysis_sync": analysis_sync if isinstance(analysis_sync, dict) else {},
        "observed_inputs": normalize_d1_observed_inputs(observed_inputs),
    }


def d1_contract_fields_from_payload(
    payload: dict[str, Any] | None,
    *,
    observed_inputs_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    observed_inputs = (
        source.get("observed_inputs")
        if isinstance(source.get("observed_inputs"), dict)
        else {}
    )
    if isinstance(observed_inputs_overrides, dict):
        observed_inputs = {**observed_inputs, **observed_inputs_overrides}
    return d1_contract_fields(
        board_state_source=source.get("board_state_source"),
        coverage_source=source.get("coverage_source"),
        db_path=source.get("db_path"),
        deliberation_sync=(
            source.get("deliberation_sync")
            if isinstance(source.get("deliberation_sync"), dict)
            else {}
        ),
        analysis_sync=(
            source.get("analysis_sync")
            if isinstance(source.get("analysis_sync"), dict)
            else {}
        ),
        observed_inputs=observed_inputs,
    )
