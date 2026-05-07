from __future__ import annotations

import json
from typing import Any

from .support import (
    artifact_ref,
    dict_items,
    helper_metadata,
    list_items,
    maybe_text,
    query_signals,
    relation_environment_class,
    relation_signal_role,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    stable_hash,
    text_terms,
    unique_texts,
    unique_values,
    utc_now_iso,
    write_json,
)


__all__ = (
    "STRUCTURED_VERIFICATION_SCOPE_FIELDS",
    "build_structured_verification_scope",
    "normalized_scope_list",
    "normalized_scope_value",
    "parse_json_object_text",
    "required_scope_fields",
    "run_review_fact_check_evidence_scope",
    "scope_value_present",
)


STRUCTURED_VERIFICATION_SCOPE_FIELDS = (
    "verification_question",
    "receptor_scope",
    "candidate_source_scope",
    "study_period",
    "evidence_window",
    "lag_window",
    "spatial_rule",
    "required_source_roles",
    "required_target_roles",
    "required_context_classes",
    "excluded_inferences",
)


def scope_value_present(value: Any) -> bool:
    if isinstance(value, dict):
        return any(scope_value_present(item) for item in value.values())
    if isinstance(value, list):
        return any(scope_value_present(item) for item in value)
    return bool(maybe_text(value))


def parse_json_object_text(value: Any) -> dict[str, Any]:
    text = maybe_text(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def normalized_scope_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            maybe_text(key): normalized_scope_value(raw_value)
            for key, raw_value in value.items()
            if maybe_text(key) and scope_value_present(raw_value)
        }
    if isinstance(value, list):
        return [
            normalized_scope_value(item)
            for item in value
            if scope_value_present(item)
        ]
    parsed_object = parse_json_object_text(value)
    if parsed_object:
        return normalized_scope_value(parsed_object)
    return maybe_text(value)


def normalized_scope_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return unique_texts(value)
    text = maybe_text(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = None
    if isinstance(parsed, list):
        return unique_texts(parsed)
    return unique_texts([part.strip() for part in text.split(",")])


def required_scope_fields(payload: dict[str, Any]) -> list[str]:
    return [
        field
        for field in STRUCTURED_VERIFICATION_SCOPE_FIELDS
        if not scope_value_present(payload.get(field))
    ]


def build_structured_verification_scope(
    *,
    verification_scope: dict[str, Any] | None,
    verification_question: str,
    receptor_scope: str,
    candidate_source_scope: str,
    study_period: str,
    evidence_window: str,
    lag_window: str,
    spatial_rule: str,
    required_source_roles: list[str] | None,
    required_target_roles: list[str] | None,
    required_context_classes: list[str] | None,
    excluded_inferences: list[str] | None,
    geographic_scope: str,
    lag_assumptions: str,
    metric_requirements: str,
    source_requirements: str,
) -> dict[str, Any]:
    raw_scope = dict_items(verification_scope)

    def fill_missing(field_name: str, value: Any) -> None:
        if not scope_value_present(raw_scope.get(field_name)) and scope_value_present(value):
            raw_scope[field_name] = value

    fill_missing("verification_question", verification_question)
    fill_missing(
        "receptor_scope",
        normalized_scope_value(receptor_scope)
        or {
            "geographic_scope": maybe_text(geographic_scope),
            "metric_requirements": maybe_text(metric_requirements),
        },
    )
    fill_missing(
        "candidate_source_scope",
        normalized_scope_value(candidate_source_scope)
        or {"source_requirements": maybe_text(source_requirements)},
    )
    fill_missing("study_period", normalized_scope_value(study_period))
    fill_missing("evidence_window", normalized_scope_value(evidence_window))
    fill_missing(
        "lag_window",
        normalized_scope_value(lag_window) or maybe_text(lag_assumptions),
    )
    fill_missing(
        "spatial_rule",
        normalized_scope_value(spatial_rule) or maybe_text(geographic_scope),
    )
    fill_missing("required_source_roles", normalized_scope_list(required_source_roles or []))
    fill_missing("required_target_roles", normalized_scope_list(required_target_roles or []))
    fill_missing(
        "required_context_classes",
        normalized_scope_list(required_context_classes or []),
    )
    fill_missing("excluded_inferences", normalized_scope_list(excluded_inferences or []))

    return {
        "verification_question": maybe_text(raw_scope.get("verification_question")),
        "receptor_scope": normalized_scope_value(raw_scope.get("receptor_scope")),
        "candidate_source_scope": normalized_scope_value(raw_scope.get("candidate_source_scope")),
        "study_period": normalized_scope_value(raw_scope.get("study_period")),
        "evidence_window": normalized_scope_value(raw_scope.get("evidence_window")),
        "lag_window": normalized_scope_value(raw_scope.get("lag_window")),
        "spatial_rule": normalized_scope_value(raw_scope.get("spatial_rule")),
        "required_source_roles": normalized_scope_list(raw_scope.get("required_source_roles")),
        "required_target_roles": normalized_scope_list(raw_scope.get("required_target_roles")),
        "required_context_classes": normalized_scope_list(raw_scope.get("required_context_classes")),
        "excluded_inferences": normalized_scope_list(raw_scope.get("excluded_inferences")),
    }


def run_review_fact_check_evidence_scope(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    verification_scope: dict[str, Any] | None = None,
    verification_question: str = "",
    receptor_scope: str = "",
    candidate_source_scope: str = "",
    lag_window: str = "",
    spatial_rule: str = "",
    required_source_roles: list[str] | None = None,
    required_target_roles: list[str] | None = None,
    required_context_classes: list[str] | None = None,
    excluded_inferences: list[str] | None = None,
    geographic_scope: str = "",
    study_period: str = "",
    evidence_window: str = "",
    lag_assumptions: str = "",
    metric_requirements: str = "",
    source_requirements: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "review-fact-check-evidence-scope"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"fact_check_evidence_scope_review_{round_id}.json")
    scope = build_structured_verification_scope(
        verification_scope=verification_scope,
        verification_question=verification_question,
        receptor_scope=receptor_scope,
        candidate_source_scope=candidate_source_scope,
        study_period=study_period,
        evidence_window=evidence_window,
        lag_window=lag_window,
        spatial_rule=spatial_rule,
        required_source_roles=required_source_roles,
        required_target_roles=required_target_roles,
        required_context_classes=required_context_classes,
        excluded_inferences=excluded_inferences,
        geographic_scope=geographic_scope,
        lag_assumptions=lag_assumptions,
        metric_requirements=metric_requirements,
        source_requirements=source_requirements,
    )
    missing_fields = required_scope_fields(scope)
    signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="environment", limit=limit)
    metric_terms = set(
        text_terms(
            json.dumps(scope["receptor_scope"], ensure_ascii=True, sort_keys=True),
            min_len=2,
            limit=20,
        )
    )
    source_terms = set(
        text_terms(
            json.dumps(scope["candidate_source_scope"], ensure_ascii=True, sort_keys=True),
            min_len=2,
            limit=20,
        )
    )
    source_roles = set(scope["required_source_roles"])
    target_roles = set(scope["required_target_roles"])
    context_classes = set(scope["required_context_classes"])
    metric_matches = [
        signal
        for signal in signals
        if not metric_terms or maybe_text(signal.get("metric")).casefold() in metric_terms
    ]
    source_matches = [
        signal
        for signal in signals
        if not source_terms or maybe_text(signal.get("source_skill")).casefold() in source_terms
    ]
    source_role_matches = [
        signal for signal in signals if relation_signal_role(signal) in source_roles
    ]
    target_role_matches = [
        signal for signal in signals if relation_signal_role(signal) in target_roles
    ]
    context_class_matches = [
        signal
        for signal in signals
        if relation_environment_class(signal) in context_classes
    ]
    scope_status = "scope-required" if missing_fields else "scope-reviewed-with-caveats"
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["structured-verification-scope-required", "db-environment-signal-scope-review"],
        caveats=[
            "This helper checks evidence scope coverage only and never outputs factual outcome labels.",
            "A finding or evidence bundle must cite this review before report use.",
        ],
    )
    review_id = "factscope-" + stable_hash(run_id, round_id, json.dumps(scope, sort_keys=True))[:12]
    review = {
        "review_id": review_id,
        "run_id": run_id,
        "round_id": round_id,
        "helper_governance": metadata,
        "verification_scope": scope,
        "scope": scope,
        "scope_status": scope_status,
        "missing_required_fields": missing_fields,
        "environment_signal_count": len(signals),
        "scope_coverage_notes": [
            {
                "dimension": "receptor-scope",
                "status": "scope-required" if missing_fields else "descriptive-match-count",
                "summary": f"{len(metric_matches)} environment signals match requested metric terms descriptively.",
            },
            {
                "dimension": "candidate-source-scope",
                "status": "scope-required" if missing_fields else "descriptive-match-count",
                "summary": f"{len(source_matches)} environment signals match requested source terms descriptively.",
            },
            {
                "dimension": "required-source-roles",
                "status": "scope-required" if missing_fields else "descriptive-match-count",
                "summary": f"{len(source_role_matches)} environment signals match required source roles descriptively.",
            },
            {
                "dimension": "required-target-roles",
                "status": "scope-required" if missing_fields else "descriptive-match-count",
                "summary": f"{len(target_role_matches)} environment signals match required target roles descriptively.",
            },
            {
                "dimension": "required-context-classes",
                "status": "scope-required" if missing_fields else "descriptive-match-count",
                "summary": f"{len(context_class_matches)} environment signals match required context classes descriptively.",
            },
            {
                "dimension": "time-place-scope",
                "status": "requires-human-review",
                "summary": "Study period, evidence window, lag window, and spatial rule are recorded for audit; this helper does not decide factual consistency.",
            },
        ],
        "disabled_judgement_surface": "factual-outcome-labels-disabled",
        "evidence_refs": unique_values([ref for signal in signals for ref in list_items(signal.get("evidence_refs"))]),
        "lineage": [maybe_text(signal.get("signal_id")) for signal in signals],
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
    }
    warnings = [
        {
            "code": "scope-required",
            "message": (
                "Structured verification_scope is missing required fields: "
                + ", ".join(missing_fields)
                + "."
            ),
        }
    ] if missing_fields else []
    payload = {
        "schema_version": "optional-analysis-fact-check-evidence-scope-review-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": scope_status,
        "review": review,
        "observed_inputs": {"db_path": db_path, "environment_signal_count": len(signals)},
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": payload["status"],
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "review_id": review_id,
            "scope_status": review["scope_status"],
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "factscope-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "factscope-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.review")],
        "canonical_ids": [review_id] if not missing_fields else [],
        "warnings": warnings,
        "review": review,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.review",
            candidate_ids=[review_id] if not missing_fields else [],
            gap_hints=[f"Missing explicit scope fields: {', '.join(missing_fields)}"] if missing_fields else [],
        ),
    }
