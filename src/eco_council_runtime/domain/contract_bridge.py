"""Shared bridge helpers from runtime flows into the contract module."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.domain.text import maybe_text


def load_contract_module() -> Any | None:
    try:
        from eco_council_runtime import contract as contract_module
    except Exception:
        return None
    return contract_module


CONTRACT_MODULE = load_contract_module()


def resolve_schema_version(default_version: str) -> str:
    if CONTRACT_MODULE is not None and hasattr(CONTRACT_MODULE, "SCHEMA_VERSION"):
        return maybe_text(getattr(CONTRACT_MODULE, "SCHEMA_VERSION")) or default_version
    return default_version


def contract_call(name: str, *args: Any, **kwargs: Any) -> Any | None:
    if CONTRACT_MODULE is None or not hasattr(CONTRACT_MODULE, name):
        return None
    helper = getattr(CONTRACT_MODULE, name)
    return helper(*args, **kwargs)


def effective_constraints(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("effective_constraints", mission)
    if isinstance(value, dict):
        return value
    constraints = mission.get("constraints")
    return constraints if isinstance(constraints, dict) else {}


def policy_profile_summary(mission: dict[str, Any]) -> dict[str, Any]:
    value = contract_call("policy_profile_summary", mission)
    if isinstance(value, dict):
        return value
    return {}


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    values = contract_call("allowed_sources_for_role", mission, role)
    if isinstance(values, list):
        return sorted({maybe_text(item) for item in values if maybe_text(item)})
    return []


def effective_matching_authorization(*, mission: dict[str, Any], round_id: str, authorization: Any) -> dict[str, Any]:
    if not isinstance(authorization, dict):
        return {}
    value = contract_call("apply_matching_authorization_policy", mission, round_id, authorization)
    if isinstance(value, dict):
        return value
    return dict(authorization)


def validate_payload_or_raise(kind: str, payload: Any) -> None:
    result = contract_call("validate_payload", kind, payload)
    if not isinstance(result, dict):
        return
    validation = result.get("validation")
    if isinstance(validation, dict) and validation.get("ok"):
        return
    issues: list[str] = []
    issue_list = validation.get("issues", []) if isinstance(validation, dict) else []
    for issue in issue_list[:5]:
        if isinstance(issue, dict):
            issues.append(f"{issue.get('path')}: {issue.get('message')}")
    detail = "; ".join(issues) if issues else "unknown validation failure"
    raise ValueError(f"Generated invalid {kind}: {detail}")


def validate_bundle(run_dir: Path) -> dict[str, Any] | None:
    value = contract_call("validate_bundle", run_dir)
    if isinstance(value, dict):
        return value
    return None
