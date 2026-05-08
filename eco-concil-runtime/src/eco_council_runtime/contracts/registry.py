from __future__ import annotations

from typing import Any

from .analysis import ANALYSIS_CONTRACTS
from .types import CanonicalContract, maybe_text
from .deliberation import DELIBERATION_CONTRACTS
from .reporting import REPORTING_CONTRACTS
from .runtime import RUNTIME_CONTRACTS
from .signal import SIGNAL_CONTRACTS


def optional_field_is_present(payload: dict[str, Any], field_name: str) -> bool:
    if field_name not in payload:
        return False
    value = payload.get(field_name)
    if value is None:
        return False
    if isinstance(value, str) and not maybe_text(value):
        return False
    return True


CANONICAL_CONTRACTS: dict[str, CanonicalContract] = {
    **SIGNAL_CONTRACTS,
    **ANALYSIS_CONTRACTS,
    **DELIBERATION_CONTRACTS,
    **RUNTIME_CONTRACTS,
    **REPORTING_CONTRACTS,
}


def canonical_contract(object_kind: str) -> CanonicalContract:
    contract = CANONICAL_CONTRACTS.get(maybe_text(object_kind))
    if contract is None:
        raise ValueError(f"Unknown canonical object kind: {object_kind!r}")
    return contract


def canonical_contract_kinds(*, plane: str = "") -> list[str]:
    normalized_plane = maybe_text(plane)
    return sorted(
        object_kind
        for object_kind, contract in CANONICAL_CONTRACTS.items()
        if not normalized_plane or contract.plane == normalized_plane
    )


def canonical_contracts_for_plane(*, plane: str = "") -> list[dict[str, Any]]:
    return [
        canonical_contract(object_kind).as_dict()
        for object_kind in canonical_contract_kinds(plane=plane)
    ]


def validate_canonical_payload(
    object_kind: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    contract = canonical_contract(object_kind)
    normalized = dict(payload)
    normalized["schema_version"] = (
        maybe_text(normalized.get("schema_version")) or contract.schema_version
    )

    missing_text_fields = [
        field_name
        for field_name in contract.required_text_fields
        if not maybe_text(normalized.get(field_name))
    ]
    invalid_list_fields = [
        field_name
        for field_name in contract.required_list_fields
        if not isinstance(normalized.get(field_name), list)
    ]
    invalid_dict_fields = [
        field_name
        for field_name in contract.required_dict_fields
        if not isinstance(normalized.get(field_name), dict)
    ]
    invalid_number_fields = [
        field_name
        for field_name in contract.required_number_fields
        if not isinstance(normalized.get(field_name), (int, float))
        or isinstance(normalized.get(field_name), bool)
    ]
    invalid_optional_list_fields = [
        field_name
        for field_name in contract.optional_list_fields
        if optional_field_is_present(normalized, field_name)
        and not isinstance(normalized.get(field_name), list)
    ]
    invalid_optional_dict_fields = [
        field_name
        for field_name in contract.optional_dict_fields
        if optional_field_is_present(normalized, field_name)
        and not isinstance(normalized.get(field_name), dict)
    ]
    invalid_optional_number_fields = [
        field_name
        for field_name in contract.optional_number_fields
        if optional_field_is_present(normalized, field_name)
        and (
            not isinstance(normalized.get(field_name), (int, float))
            or isinstance(normalized.get(field_name), bool)
        )
    ]
    invalid_optional_bool_fields = [
        field_name
        for field_name in contract.optional_bool_fields
        if optional_field_is_present(normalized, field_name)
        and not isinstance(normalized.get(field_name), bool)
    ]
    empty_list_fields = [
        field_name
        for field_name in contract.required_non_empty_list_fields
        if isinstance(normalized.get(field_name), list) and not normalized.get(field_name)
    ]
    empty_dict_fields = [
        field_name
        for field_name in contract.required_non_empty_dict_fields
        if isinstance(normalized.get(field_name), dict) and not normalized.get(field_name)
    ]
    if (
        missing_text_fields
        or invalid_list_fields
        or invalid_dict_fields
        or invalid_number_fields
        or invalid_optional_list_fields
        or invalid_optional_dict_fields
        or invalid_optional_number_fields
        or invalid_optional_bool_fields
        or empty_list_fields
        or empty_dict_fields
    ):
        problems: list[str] = []
        if missing_text_fields:
            problems.append(
                "missing text fields: " + ", ".join(sorted(missing_text_fields))
            )
        if invalid_list_fields:
            problems.append(
                "list fields required: " + ", ".join(sorted(invalid_list_fields))
            )
        if invalid_dict_fields:
            problems.append(
                "dict fields required: " + ", ".join(sorted(invalid_dict_fields))
            )
        if invalid_number_fields:
            problems.append(
                "number fields required: " + ", ".join(sorted(invalid_number_fields))
            )
        if invalid_optional_list_fields:
            problems.append(
                "optional list fields must be lists when present: "
                + ", ".join(sorted(invalid_optional_list_fields))
            )
        if invalid_optional_dict_fields:
            problems.append(
                "optional dict fields must be dicts when present: "
                + ", ".join(sorted(invalid_optional_dict_fields))
            )
        if invalid_optional_number_fields:
            problems.append(
                "optional number fields must be numbers when present: "
                + ", ".join(sorted(invalid_optional_number_fields))
            )
        if invalid_optional_bool_fields:
            problems.append(
                "optional bool fields must be bools when present: "
                + ", ".join(sorted(invalid_optional_bool_fields))
            )
        if empty_list_fields:
            problems.append(
                "non-empty list fields required: "
                + ", ".join(sorted(empty_list_fields))
            )
        if empty_dict_fields:
            problems.append(
                "non-empty dict fields required: "
                + ", ".join(sorted(empty_dict_fields))
            )
        raise ValueError(
            f"Invalid canonical payload for {object_kind}: " + "; ".join(problems)
        )
    return normalized


__all__ = (
    "CANONICAL_CONTRACTS",
    "canonical_contract",
    "canonical_contract_kinds",
    "canonical_contracts_for_plane",
    "validate_canonical_payload",
)
