from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


PLANE_SIGNAL = "signal"
PLANE_ANALYSIS = "analysis"
PLANE_DELIBERATION = "deliberation"
PLANE_REPORTING = "reporting"
PLANE_RUNTIME = "runtime"


@dataclass(frozen=True)
class ContractFieldGroup:
    group_id: str
    description: str
    text_fields: tuple[str, ...] = ()
    list_fields: tuple[str, ...] = ()
    dict_fields: tuple[str, ...] = ()
    number_fields: tuple[str, ...] = ()
    bool_fields: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


FIELD_GROUP_OBJECT_IDENTITY = "object-identity"
FIELD_GROUP_EVIDENCE_LINEAGE = "evidence-lineage"
FIELD_GROUP_PROVENANCE = "provenance"
FIELD_GROUP_GOVERNANCE_TARGET = "governance-target"
FIELD_GROUP_BASIS_LINKAGE = "basis-linkage"
FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE = "challenger-constraint-state"
FIELD_GROUP_REPORT_CLAIM_LINKAGE = "report-claim-linkage"

CONTRACT_FIELD_GROUPS: dict[str, ContractFieldGroup] = {
    FIELD_GROUP_OBJECT_IDENTITY: ContractFieldGroup(
        FIELD_GROUP_OBJECT_IDENTITY,
        "Minimal object identity fields shared by canonical records.",
        text_fields=("run_id", "round_id"),
    ),
    FIELD_GROUP_EVIDENCE_LINEAGE: ContractFieldGroup(
        FIELD_GROUP_EVIDENCE_LINEAGE,
        "Structural evidence and lineage references without claim-support semantics.",
        list_fields=("evidence_refs", "lineage"),
    ),
    FIELD_GROUP_PROVENANCE: ContractFieldGroup(
        FIELD_GROUP_PROVENANCE,
        "Machine-readable provenance container.",
        dict_fields=("provenance",),
    ),
    FIELD_GROUP_GOVERNANCE_TARGET: ContractFieldGroup(
        FIELD_GROUP_GOVERNANCE_TARGET,
        "Explicit object target references used by deliberation records.",
        text_fields=("target_kind", "target_id"),
        dict_fields=("target",),
    ),
    FIELD_GROUP_BASIS_LINKAGE: ContractFieldGroup(
        FIELD_GROUP_BASIS_LINKAGE,
        "Explicit basis/evidence selections; no ranking or sufficiency judgement.",
        list_fields=(
            "basis_object_ids",
            "selected_basis_object_ids",
            "selected_evidence_refs",
        ),
    ),
    FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE: ContractFieldGroup(
        FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE,
        "Structural challenger constraint state and disposition propagation.",
        list_fields=(
            "challenger_constraint_ids",
            "unresolved_challenger_constraint_ids",
            "challenger_constraints",
            "unresolved_challenger_constraints",
            "basis_use_constraints",
            "required_followup_evidence",
        ),
        number_fields=(
            "challenger_constraint_count",
            "unresolved_challenger_constraint_count",
        ),
    ),
    FIELD_GROUP_REPORT_CLAIM_LINKAGE: ContractFieldGroup(
        FIELD_GROUP_REPORT_CLAIM_LINKAGE,
        "Explicit report claim and lead-basis linkage fields supplied by agents.",
        text_fields=("claim_id", "claim_text", "basis_use"),
        list_fields=(
            "claim_constraint_ids",
            "explicit_report_claim_objects",
            "report_claim_structural_violations",
            "explicit_lead_basis_objects",
            "lead_basis_constraint_violations",
        ),
        dict_fields=("report_claim_structure",),
        number_fields=(
            "explicit_report_claim_count",
            "report_claim_structural_violation_count",
            "explicit_lead_basis_count",
            "lead_basis_constraint_violation_count",
        ),
        bool_fields=("lead_basis",),
    ),
}


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
    optional_text_fields: tuple[str, ...] = ()
    optional_list_fields: tuple[str, ...] = ()
    optional_dict_fields: tuple[str, ...] = ()
    optional_number_fields: tuple[str, ...] = ()
    optional_bool_fields: tuple[str, ...] = ()
    field_groups: tuple[str, ...] = ()
    item_level_query: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def unique_fields(values: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    fields: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        fields.append(text)
    return tuple(fields)


def contract_field_group(group_id: str) -> ContractFieldGroup:
    normalized_group_id = maybe_text(group_id)
    group = CONTRACT_FIELD_GROUPS.get(normalized_group_id)
    if group is None:
        raise ValueError(f"Unknown contract field group: {group_id!r}")
    return group


def contract_field_groups() -> list[dict[str, Any]]:
    return [
        CONTRACT_FIELD_GROUPS[group_id].as_dict()
        for group_id in sorted(CONTRACT_FIELD_GROUPS)
    ]


def inferred_field_groups(
    *,
    required_text_fields: tuple[str, ...],
    required_list_fields: tuple[str, ...],
    required_dict_fields: tuple[str, ...],
    optional_text_fields: tuple[str, ...],
    optional_list_fields: tuple[str, ...],
    optional_dict_fields: tuple[str, ...],
    optional_number_fields: tuple[str, ...],
    optional_bool_fields: tuple[str, ...],
) -> tuple[str, ...]:
    text_fields = set(required_text_fields) | set(optional_text_fields)
    list_fields = set(required_list_fields) | set(optional_list_fields)
    dict_fields = set(required_dict_fields) | set(optional_dict_fields)
    number_fields = set(optional_number_fields)
    bool_fields = set(optional_bool_fields)

    group_ids = [FIELD_GROUP_OBJECT_IDENTITY]
    if {"evidence_refs", "lineage"}.issubset(list_fields):
        group_ids.append(FIELD_GROUP_EVIDENCE_LINEAGE)
    if "provenance" in dict_fields:
        group_ids.append(FIELD_GROUP_PROVENANCE)
    if {"target_kind", "target_id"}.issubset(text_fields) and "target" in dict_fields:
        group_ids.append(FIELD_GROUP_GOVERNANCE_TARGET)
    if {
        "basis_object_ids",
        "selected_basis_object_ids",
        "selected_evidence_refs",
    }.intersection(list_fields):
        group_ids.append(FIELD_GROUP_BASIS_LINKAGE)
    if {
        "challenger_constraints",
        "unresolved_challenger_constraints",
        "basis_use_constraints",
        "required_followup_evidence",
    }.intersection(list_fields) or {
        "challenger_constraint_count",
        "unresolved_challenger_constraint_count",
    }.intersection(number_fields):
        group_ids.append(FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE)
    if {
        "claim_id",
        "claim_text",
        "basis_use",
    }.intersection(text_fields) or {
        "claim_constraint_ids",
        "explicit_report_claim_objects",
        "report_claim_structural_violations",
        "explicit_lead_basis_objects",
        "lead_basis_constraint_violations",
    }.intersection(list_fields) or "lead_basis" in bool_fields:
        group_ids.append(FIELD_GROUP_REPORT_CLAIM_LINKAGE)
    return unique_fields(tuple(group_ids))


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
    optional_text_fields: tuple[str, ...] = (),
    optional_list_fields: tuple[str, ...] = (),
    optional_dict_fields: tuple[str, ...] = (),
    optional_number_fields: tuple[str, ...] = (),
    optional_bool_fields: tuple[str, ...] = (),
    field_groups: tuple[str, ...] = (),
    item_level_query: bool = True,
) -> CanonicalContract:
    normalized_required_text_fields = unique_fields(
        ("run_id", "round_id", id_field, *required_text_fields)
    )
    normalized_required_list_fields = unique_fields(required_list_fields)
    normalized_required_dict_fields = unique_fields(required_dict_fields)
    normalized_optional_text_fields = unique_fields(optional_text_fields)
    normalized_optional_list_fields = unique_fields(optional_list_fields)
    normalized_optional_dict_fields = unique_fields(optional_dict_fields)
    normalized_optional_number_fields = unique_fields(optional_number_fields)
    normalized_optional_bool_fields = unique_fields(optional_bool_fields)
    normalized_field_groups = unique_fields(
        (
            *inferred_field_groups(
                required_text_fields=normalized_required_text_fields,
                required_list_fields=normalized_required_list_fields,
                required_dict_fields=normalized_required_dict_fields,
                optional_text_fields=normalized_optional_text_fields,
                optional_list_fields=normalized_optional_list_fields,
                optional_dict_fields=normalized_optional_dict_fields,
                optional_number_fields=normalized_optional_number_fields,
                optional_bool_fields=normalized_optional_bool_fields,
            ),
            *field_groups,
        )
    )
    for group_id in normalized_field_groups:
        contract_field_group(group_id)
    return CanonicalContract(
        object_kind=object_kind,
        plane=plane,
        schema_version=schema_version,
        id_field=id_field,
        required_text_fields=normalized_required_text_fields,
        required_list_fields=normalized_required_list_fields,
        required_dict_fields=normalized_required_dict_fields,
        required_number_fields=unique_fields(required_number_fields),
        required_non_empty_list_fields=unique_fields(required_non_empty_list_fields),
        required_non_empty_dict_fields=unique_fields(required_non_empty_dict_fields),
        optional_text_fields=normalized_optional_text_fields,
        optional_list_fields=normalized_optional_list_fields,
        optional_dict_fields=normalized_optional_dict_fields,
        optional_number_fields=normalized_optional_number_fields,
        optional_bool_fields=normalized_optional_bool_fields,
        field_groups=normalized_field_groups,
        item_level_query=item_level_query,
    )


__all__ = (
    "ContractFieldGroup",
    "CanonicalContract",
    "CONTRACT_FIELD_GROUPS",
    "FIELD_GROUP_OBJECT_IDENTITY",
    "FIELD_GROUP_EVIDENCE_LINEAGE",
    "FIELD_GROUP_PROVENANCE",
    "FIELD_GROUP_GOVERNANCE_TARGET",
    "FIELD_GROUP_BASIS_LINKAGE",
    "FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE",
    "FIELD_GROUP_REPORT_CLAIM_LINKAGE",
    "PLANE_SIGNAL",
    "PLANE_ANALYSIS",
    "PLANE_DELIBERATION",
    "PLANE_REPORTING",
    "PLANE_RUNTIME",
    "contract_field_group",
    "contract_field_groups",
    "maybe_text",
    "_contract",
)
