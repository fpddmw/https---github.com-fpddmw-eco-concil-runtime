from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


PLANE_SIGNAL = "signal"
PLANE_ANALYSIS = "analysis"
PLANE_DELIBERATION = "deliberation"
PLANE_REPORTING = "reporting"
PLANE_RUNTIME = "runtime"


@dataclass(frozen=True)
class CanonicalContract:
    object_kind: str
    plane: str
    schema_version: str
    id_field: str
    required_text_fields: tuple[str, ...]
    required_list_fields: tuple[str, ...]
    required_dict_fields: tuple[str, ...]
    required_number_fields: tuple[str, ...]
    required_non_empty_list_fields: tuple[str, ...] = ()
    required_non_empty_dict_fields: tuple[str, ...] = ()
    item_level_query: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _contract(
    object_kind: str,
    *,
    plane: str,
    schema_version: str,
    id_field: str,
    required_text_fields: tuple[str, ...],
    required_list_fields: tuple[str, ...] = ("evidence_refs", "lineage"),
    required_dict_fields: tuple[str, ...] = ("provenance",),
    required_number_fields: tuple[str, ...] = (),
    required_non_empty_list_fields: tuple[str, ...] = (),
    required_non_empty_dict_fields: tuple[str, ...] = (),
    item_level_query: bool = True,
) -> CanonicalContract:
    return CanonicalContract(
        object_kind=object_kind,
        plane=plane,
        schema_version=schema_version,
        id_field=id_field,
        required_text_fields=("run_id", "round_id", id_field, *required_text_fields),
        required_list_fields=required_list_fields,
        required_dict_fields=required_dict_fields,
        required_number_fields=required_number_fields,
        required_non_empty_list_fields=required_non_empty_list_fields,
        required_non_empty_dict_fields=required_non_empty_dict_fields,
        item_level_query=item_level_query,
    )


__all__ = (
    "CanonicalContract",
    "PLANE_SIGNAL",
    "PLANE_ANALYSIS",
    "PLANE_DELIBERATION",
    "PLANE_REPORTING",
    "PLANE_RUNTIME",
    "maybe_text",
    "_contract",
)
